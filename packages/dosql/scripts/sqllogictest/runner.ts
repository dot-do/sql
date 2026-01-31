#!/usr/bin/env npx tsx
/**
 * SQLLogicTest Runner for DoSQL
 *
 * Runs sqllogictest test files against the DoSQL engine.
 * Optimized for memory efficiency to handle large test suites.
 *
 * Usage:
 *   npx tsx packages/dosql/scripts/sqllogictest/runner.ts [options]
 *
 * Options:
 *   --limit=N         Run only first N tests
 *   --file=PATH       Run specific test file
 *   --dir=PATH        Run all .test files in directory
 *   --folder=PATH     Alias for --dir (for compatibility)
 *   --verbose         Show detailed output
 *   --continue        Continue on error (don't stop at first failure)
 *   --batch-size=N    Process tests in batches of N (default: 500)
 *   --split-files=N   Split large files into chunks of N tests (default: 1500)
 *   --category=X      Run only tests in category X
 *   --skip-category=X Skip tests in category X
 *
 * Examples:
 *   npx tsx runner.ts --limit=1000
 *   npx tsx runner.ts --file=select1.test --verbose
 *   npx tsx runner.ts --dir=./tests --continue
 *   npx tsx runner.ts --folder=tests/sqlite-full --batch-size=200
 *
 * Memory optimization:
 *   NODE_OPTIONS="--max-old-space-size=4096" npx tsx runner.ts --folder=tests/sqlite-full
 *
 * Note: Large test files are automatically split to prevent memory exhaustion.
 * The split-files option controls chunk size (default 1500 tests per chunk).
 */

import * as fs from 'fs';
import * as path from 'path';
import * as readline from 'readline';
import {
  parseTestFile,
  shouldSkip,
  formatValue,
  sortValues,
  hashValues,
  type Record,
  type StatementRecord,
  type QueryRecord,
  type HashThresholdRecord,
  type HaltRecord,
  type ResultType,
  type SortMode,
} from './parser.js';

// =============================================================================
// DOSQL ENGINE INTERFACE
// =============================================================================

// Import DoSQL engine
import { Database } from '../../src/database.js';

/**
 * Test database wrapper
 * Manages database lifecycle with proper cleanup for memory efficiency
 */
class TestDatabase {
  private db: Database | null = null;

  constructor() {
    this.createNewDatabase();
  }

  private createNewDatabase(): void {
    this.db = new Database(':memory:');
  }

  private ensureOpen(): Database {
    if (!this.db) {
      this.createNewDatabase();
    }
    return this.db!;
  }

  /**
   * Execute a SQL statement (no result expected)
   */
  execute(sql: string): { success: boolean; error?: string } {
    try {
      const db = this.ensureOpen();
      // Try to detect if it's a query or statement
      const normalized = sql.trim().toUpperCase();

      if (
        normalized.startsWith('SELECT') ||
        normalized.startsWith('VALUES') ||
        normalized.startsWith('WITH')
      ) {
        // It's a query, but we're treating as statement
        db.prepare(sql).all();
      } else {
        db.exec(sql);
      }

      return { success: true };
    } catch (e) {
      return {
        success: false,
        error: e instanceof Error ? e.message : String(e),
      };
    }
  }

  /**
   * Execute a query and return results
   */
  query(sql: string): { success: boolean; rows?: unknown[][]; error?: string } {
    try {
      const db = this.ensureOpen();
      const stmt = db.prepare(sql);
      const results = stmt.all();

      // Convert to array of arrays
      const rows = results.map(row => Object.values(row as object));

      return { success: true, rows };
    } catch (e) {
      return {
        success: false,
        error: e instanceof Error ? e.message : String(e),
      };
    }
  }

  /**
   * Reset the database - fully destroy old instance and create new
   */
  reset(): void {
    this.destroy();
    this.createNewDatabase();
    tryGC();
  }

  /**
   * Destroy the database connection completely
   */
  destroy(): void {
    if (this.db) {
      this.db.close();
      this.db = null;
    }
  }
}

// =============================================================================
// RESULT TYPES
// =============================================================================

interface TestResult {
  passed: boolean;
  record: Record;
  error?: string;
  expected?: string[];
  actual?: string[];
  expectedHash?: string;
  actualHash?: string;
  executionTime: number;
}

/**
 * Lightweight failure info for memory-efficient storage
 * We only store the essential info needed for reporting, not full TestResult
 */
interface FailureInfo {
  lineNumber: number;
  sql: string;
  error?: string;
  recordType: 'statement' | 'query';
}

interface RunSummary {
  totalTests: number;
  passed: number;
  failed: number;
  skipped: number;
  errors: number;
  executionTime: number;
  categoryResults: Map<string, { passed: number; failed: number; skipped: number }>;
  /** Lightweight failure info - only stores essential data to save memory */
  failedTests: FailureInfo[];
}

/**
 * Configuration for memory management
 */
const MEMORY_CONFIG = {
  /** Number of tests to process before cleanup */
  BATCH_SIZE: 500,
  /** Maximum number of failures to store (to prevent memory exhaustion on many failures) */
  MAX_FAILURES_STORED: 100,
  /** Whether to attempt garbage collection hints */
  ENABLE_GC_HINTS: true,
  /** Maximum tests per file chunk to prevent memory exhaustion */
  MAX_TESTS_PER_CHUNK: 1500,
};

/**
 * Attempt to trigger garbage collection if available
 * Run with --expose-gc flag to enable: node --expose-gc script.js
 */
function tryGC(): void {
  if (MEMORY_CONFIG.ENABLE_GC_HINTS && typeof global.gc === 'function') {
    global.gc();
  }
}

// =============================================================================
// STREAMING PARSER
// =============================================================================

/**
 * Streaming parser that yields records one at a time to minimize memory usage.
 * This is critical for large test files like select4.test (48K lines).
 */
async function* streamParseTestFile(filePath: string): AsyncGenerator<Record> {
  const fileStream = fs.createReadStream(filePath, { encoding: 'utf-8' });
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  let lineNumber = 0;
  let skipif: string[] = [];
  let onlyif: string[] = [];

  // State machine for parsing multi-line records
  let currentRecord: {
    type: 'statement' | 'query';
    startLine: number;
    header: string;
    sqlLines: string[];
    expectedValues: string[];
    foundSeparator: boolean;
    expectedHash?: string;
  } | null = null;

  for await (const line of rl) {
    lineNumber++;
    const trimmed = line.trim();

    // Skip empty lines and comments (unless collecting SQL/results)
    if (trimmed === '' || trimmed.startsWith('#')) {
      if (currentRecord) {
        // Empty line ends current record
        const record = finalizeRecord(currentRecord, skipif, onlyif);
        if (record) {
          yield record;
        }
        currentRecord = null;
        skipif = [];
        onlyif = [];
      }
      continue;
    }

    // Handle skipif/onlyif
    if (trimmed.startsWith('skipif ')) {
      skipif.push(trimmed.slice(7).trim());
      continue;
    }

    if (trimmed.startsWith('onlyif ')) {
      onlyif.push(trimmed.slice(7).trim());
      continue;
    }

    // Handle halt (respect onlyif/skipif conditions)
    if (trimmed === 'halt') {
      // Only halt if conditions are met:
      // - If onlyif is set, only halt for that db (we run as 'sqlite')
      // - If skipif is set, don't halt for that db
      const shouldHalt =
        (onlyif.length === 0 || onlyif.includes('sqlite')) &&
        !skipif.includes('sqlite');

      if (shouldHalt) {
        yield { type: 'halt', lineNumber } as HaltRecord;
        break;
      }
      // Clear conditions and continue
      skipif = [];
      onlyif = [];
      continue;
    }

    // Handle hash-threshold
    if (trimmed.startsWith('hash-threshold ')) {
      const threshold = parseInt(trimmed.slice(15).trim(), 10);
      yield { type: 'hash-threshold', threshold, lineNumber } as HashThresholdRecord;
      continue;
    }

    // Handle statement start
    if (trimmed.startsWith('statement ')) {
      // Finalize any previous record
      if (currentRecord) {
        const record = finalizeRecord(currentRecord, skipif, onlyif);
        if (record) {
          yield record;
        }
        skipif = [];
        onlyif = [];
      }

      currentRecord = {
        type: 'statement',
        startLine: lineNumber,
        header: trimmed,
        sqlLines: [],
        expectedValues: [],
        foundSeparator: false,
      };
      continue;
    }

    // Handle query start
    if (trimmed.startsWith('query ')) {
      // Finalize any previous record
      if (currentRecord) {
        const record = finalizeRecord(currentRecord, skipif, onlyif);
        if (record) {
          yield record;
        }
        skipif = [];
        onlyif = [];
      }

      currentRecord = {
        type: 'query',
        startLine: lineNumber,
        header: trimmed,
        sqlLines: [],
        expectedValues: [],
        foundSeparator: false,
      };
      continue;
    }

    // Collecting SQL or expected values for current record
    if (currentRecord) {
      if (trimmed.startsWith('----')) {
        currentRecord.foundSeparator = true;
        continue;
      }

      if (currentRecord.foundSeparator) {
        // Collecting expected values
        const hashMatch = trimmed.match(/^(\d+)\s+values?\s+hashing\s+to\s+([a-f0-9]{32})$/i);
        if (hashMatch) {
          currentRecord.expectedHash = hashMatch[2];
        } else {
          currentRecord.expectedValues.push(trimmed);
        }
      } else {
        // Collecting SQL
        if (!trimmed.startsWith('#')) {
          currentRecord.sqlLines.push(line);
        }
      }
    }
  }

  // Finalize last record if any
  if (currentRecord) {
    const record = finalizeRecord(currentRecord, skipif, onlyif);
    if (record) {
      yield record;
    }
  }
}

/**
 * Finalize a parsed record from accumulated lines
 */
function finalizeRecord(
  current: {
    type: 'statement' | 'query';
    startLine: number;
    header: string;
    sqlLines: string[];
    expectedValues: string[];
    foundSeparator: boolean;
    expectedHash?: string;
  },
  skipif: string[],
  onlyif: string[]
): StatementRecord | QueryRecord | null {
  let sql = current.sqlLines.join('\n').trim();
  if (sql.endsWith(';')) {
    sql = sql.slice(0, -1).trim();
  }

  if (current.type === 'statement') {
    const match = current.header.match(/^statement\s+(ok|error)$/i);
    if (!match) return null;

    return {
      type: 'statement',
      expectedResult: match[1].toLowerCase() as 'ok' | 'error',
      sql,
      lineNumber: current.startLine,
      ...(skipif.length > 0 && { skipif }),
      ...(onlyif.length > 0 && { onlyif }),
    };
  }

  if (current.type === 'query') {
    const match = current.header.match(/^query\s+([TIR]+)(?:\s+(nosort|rowsort|valuesort))?(?:\s+(.+))?$/i);
    if (!match) return null;

    const columnTypes = match[1].toUpperCase().split('') as ResultType[];
    const sortMode = (match[2]?.toLowerCase() || 'nosort') as SortMode;
    const label = match[3]?.trim();

    return {
      type: 'query',
      columnTypes,
      sortMode,
      ...(label && { label }),
      sql,
      expectedValues: current.expectedValues,
      ...(current.expectedHash && { expectedHash: current.expectedHash }),
      lineNumber: current.startLine,
      ...(skipif.length > 0 && { skipif }),
      ...(onlyif.length > 0 && { onlyif }),
    };
  }

  return null;
}

// =============================================================================
// RUNNER
// =============================================================================

class SqlLogicTestRunner {
  private db: TestDatabase;
  private verbose: boolean;
  private continueOnError: boolean;
  private limit: number;
  private batchSize: number;
  private splitFiles: number;
  private testCount: number = 0;
  private batchTestCount: number = 0;
  private chunkTestCount: number = 0;

  constructor(options: {
    verbose?: boolean;
    continueOnError?: boolean;
    limit?: number;
    batchSize?: number;
    splitFiles?: number;
  } = {}) {
    this.db = new TestDatabase();
    this.verbose = options.verbose ?? false;
    this.continueOnError = options.continueOnError ?? true;
    this.limit = options.limit ?? Infinity;
    this.batchSize = options.batchSize ?? MEMORY_CONFIG.BATCH_SIZE;
    this.splitFiles = options.splitFiles ?? MEMORY_CONFIG.MAX_TESTS_PER_CHUNK;
  }

  /**
   * Cleanup between batches to free memory
   */
  private cleanupBatch(): void {
    this.batchTestCount = 0;
    tryGC();
  }

  /**
   * Reset database to reclaim memory (for large files)
   * Note: This will lose schema state, so should only be used between files
   */
  resetDatabase(): void {
    this.db.reset();
    this.chunkTestCount = 0;
  }

  /**
   * Get current memory usage in MB
   */
  private getMemoryUsageMB(): number {
    const usage = process.memoryUsage();
    return Math.round(usage.heapUsed / 1024 / 1024);
  }

  /**
   * Check if we're approaching memory limits
   */
  private checkMemoryPressure(): boolean {
    const memMB = this.getMemoryUsageMB();
    return memMB > 3000; // Warn above 3GB
  }

  /**
   * Extract lightweight failure info from a test result
   */
  private extractFailureInfo(result: TestResult): FailureInfo | null {
    const record = result.record;
    if (record.type !== 'statement' && record.type !== 'query') {
      return null;
    }
    return {
      lineNumber: record.lineNumber,
      sql: record.sql.slice(0, 200), // Truncate SQL to save memory
      error: result.error?.slice(0, 200),
      recordType: record.type,
    };
  }

  /**
   * Run tests from a file
   * Uses streaming approach - processes tests one at a time without accumulating
   */
  async runFile(filePath: string): Promise<RunSummary> {
    const startTime = performance.now();
    const categoryResults = new Map<string, { passed: number; failed: number; skipped: number }>();
    // Only store lightweight failure info, limited count
    const failedTests: FailureInfo[] = [];

    let passed = 0;
    let failed = 0;
    let skipped = 0;
    let errorCount = 0;
    let shouldStop = false;

    // Determine category from file path
    const category = this.getCategory(filePath);
    if (!categoryResults.has(category)) {
      categoryResults.set(category, { passed: 0, failed: 0, skipped: 0 });
    }
    const catStats = categoryResults.get(category)!;

    // Use streaming parser to process records one at a time
    for await (const record of streamParseTestFile(filePath)) {
      // Check limit
      if (this.testCount >= this.limit || shouldStop) {
        break;
      }

      // Handle halt
      if (record.type === 'halt') {
        if (this.verbose) {
          console.log('Halt record encountered, stopping');
        }
        break;
      }

      // Skip control records
      if (record.type === 'hash-threshold') {
        continue;
      }

      // Check skipif/onlyif
      if (record.type === 'statement' || record.type === 'query') {
        if (shouldSkip(record, 'dosql')) {
          skipped++;
          catStats.skipped++;
          continue;
        }
      }

      this.testCount++;
      this.batchTestCount++;
      this.chunkTestCount++;

      // Run the test - result is processed immediately, not accumulated
      const result = await this.runRecord(record);

      if (result.passed) {
        passed++;
        catStats.passed++;
        if (this.verbose) {
          this.printProgress('.');
        }
      } else {
        failed++;
        catStats.failed++;

        // Only store limited failure info to prevent memory exhaustion
        if (failedTests.length < MEMORY_CONFIG.MAX_FAILURES_STORED) {
          const failureInfo = this.extractFailureInfo(result);
          if (failureInfo) {
            failedTests.push(failureInfo);
          }
        }

        if (this.verbose) {
          this.printProgress('F');
          console.log(`\nFailed at line ${record.lineNumber}:`);
          if (record.type === 'statement' || record.type === 'query') {
            console.log(`  SQL: ${record.sql.slice(0, 100)}${record.sql.length > 100 ? '...' : ''}`);
          }
          if (result.error) {
            console.log(`  Error: ${result.error}`);
          }
          if (result.expected && result.actual) {
            console.log(`  Expected (first 5): ${result.expected.slice(0, 5).join(', ')}`);
            console.log(`  Actual (first 5): ${result.actual.slice(0, 5).join(', ')}`);
          }
          if (result.expectedHash && result.actualHash) {
            console.log(`  Expected hash: ${result.expectedHash}`);
            console.log(`  Actual hash:   ${result.actualHash}`);
            console.log(`  Values count:  ${result.actual?.length ?? 0}`);
          }
        }

        if (!this.continueOnError) {
          shouldStop = true;
        }
      }

      // Batch cleanup - free memory periodically
      if (this.batchTestCount >= this.batchSize) {
        this.cleanupBatch();
      }

      // Progress indicator with memory info
      if (!this.verbose && this.testCount % 100 === 0) {
        const memMB = this.getMemoryUsageMB();
        process.stdout.write(`\rProgress: ${this.testCount} tests (${memMB}MB)...`);

        // Warn if memory pressure is high
        if (this.checkMemoryPressure() && this.testCount % 500 === 0) {
          console.log(`\n  [WARNING] High memory usage (${memMB}MB). Consider using --limit or --split-files.`);
        }
      }
    }

    const endTime = performance.now();

    // Final cleanup after file
    this.cleanupBatch();

    return {
      totalTests: this.testCount,
      passed,
      failed,
      skipped,
      errors: errorCount,
      executionTime: endTime - startTime,
      categoryResults,
      failedTests,
    };
  }

  /**
   * Run tests from a directory
   * Uses streaming merge to avoid accumulating all summaries in memory
   */
  async runDirectory(dirPath: string): Promise<RunSummary> {
    const files = this.findTestFiles(dirPath);

    console.log(`Found ${files.length} test files in ${dirPath}`);

    // Track global test count across files
    let globalTestCount = 0;

    // Use a running summary instead of accumulating all summaries
    const runningSummary: RunSummary = {
      totalTests: 0,
      passed: 0,
      failed: 0,
      skipped: 0,
      errors: 0,
      executionTime: 0,
      categoryResults: new Map(),
      failedTests: [],
    };

    for (const file of files) {
      if (globalTestCount >= this.limit) {
        break;
      }

      console.log(`\nRunning ${path.basename(file)}...`);
      this.db.reset(); // Fresh database for each file
      this.testCount = 0; // Reset per-file count
      this.batchTestCount = 0; // Reset batch count

      const summary = await this.runFile(file);

      // Merge into running summary immediately (no accumulation)
      runningSummary.totalTests += summary.totalTests;
      runningSummary.passed += summary.passed;
      runningSummary.failed += summary.failed;
      runningSummary.skipped += summary.skipped;
      runningSummary.errors += summary.errors;
      runningSummary.executionTime += summary.executionTime;

      // Merge category results
      for (const [cat, stats] of summary.categoryResults) {
        const existing = runningSummary.categoryResults.get(cat) || { passed: 0, failed: 0, skipped: 0 };
        existing.passed += stats.passed;
        existing.failed += stats.failed;
        existing.skipped += stats.skipped;
        runningSummary.categoryResults.set(cat, existing);
      }

      // Only keep limited failures
      if (runningSummary.failedTests.length < MEMORY_CONFIG.MAX_FAILURES_STORED) {
        const remaining = MEMORY_CONFIG.MAX_FAILURES_STORED - runningSummary.failedTests.length;
        runningSummary.failedTests.push(...summary.failedTests.slice(0, remaining));
      }

      globalTestCount += summary.passed + summary.failed;

      // Aggressive cleanup between files
      tryGC();
    }

    return runningSummary;
  }

  /**
   * Run a single record
   */
  private async runRecord(record: Record): Promise<TestResult> {
    const startTime = performance.now();

    switch (record.type) {
      case 'statement':
        return this.runStatement(record, startTime);
      case 'query':
        return this.runQuery(record, startTime);
      default:
        return {
          passed: true,
          record,
          executionTime: performance.now() - startTime,
        };
    }
  }

  /**
   * Run a statement record
   */
  private runStatement(record: StatementRecord, startTime: number): TestResult {
    const result = this.db.execute(record.sql);
    const executionTime = performance.now() - startTime;

    const passed =
      (record.expectedResult === 'ok' && result.success) ||
      (record.expectedResult === 'error' && !result.success);

    return {
      passed,
      record,
      error: result.success ? undefined : result.error,
      executionTime,
    };
  }

  /**
   * Run a query record
   * Memory optimized: doesn't retain large arrays in the result
   */
  private async runQuery(record: QueryRecord, startTime: number): Promise<TestResult> {
    const result = this.db.query(record.sql);
    const executionTime = performance.now() - startTime;

    // If query failed
    if (!result.success) {
      return {
        passed: false,
        record,
        error: result.error,
        executionTime,
      };
    }

    // Format actual values
    const actualValues: string[] = [];
    const rows = result.rows || [];
    for (let r = 0; r < rows.length; r++) {
      const row = rows[r];
      for (let i = 0; i < row.length; i++) {
        const type = record.columnTypes[i] || 'T';
        actualValues.push(formatValue(row[i], type));
      }
    }
    // Clear reference to rows immediately to allow GC
    result.rows = undefined;

    // Sort values according to sort mode
    const sortedActual = sortValues(actualValues, record.sortMode, record.columnTypes.length);

    // If expectedHash is provided and expectedValues is empty, use hash comparison
    if (record.expectedHash && record.expectedValues.length === 0) {
      const actualHash = await hashValues(sortedActual);
      const passed = actualHash.toLowerCase() === record.expectedHash.toLowerCase();

      // Don't store large actual arrays in results - only store for debugging if verbose
      return {
        passed,
        record,
        // Only store first few values for debugging, not all
        actual: this.verbose ? sortedActual.slice(0, 10) : undefined,
        expectedHash: record.expectedHash,
        actualHash,
        executionTime,
      };
    }

    // Fall back to value comparison
    const sortedExpected = sortValues(record.expectedValues, record.sortMode, record.columnTypes.length);
    const passed = this.compareResults(sortedExpected, sortedActual);

    // Don't store large arrays in results unless debugging
    return {
      passed,
      record,
      // Only store for debugging, truncated
      expected: this.verbose && !passed ? sortedExpected.slice(0, 10) : undefined,
      actual: this.verbose && !passed ? sortedActual.slice(0, 10) : undefined,
      executionTime,
    };
  }

  /**
   * Compare expected and actual results
   */
  private compareResults(expected: string[], actual: string[]): boolean {
    if (expected.length !== actual.length) {
      return false;
    }

    for (let i = 0; i < expected.length; i++) {
      if (expected[i] !== actual[i]) {
        return false;
      }
    }

    return true;
  }

  /**
   * Find all .test files in a directory
   */
  private findTestFiles(dirPath: string): string[] {
    const files: string[] = [];

    const entries = fs.readdirSync(dirPath, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = path.join(dirPath, entry.name);

      if (entry.isDirectory()) {
        files.push(...this.findTestFiles(fullPath));
      } else if (entry.isFile() && entry.name.endsWith('.test')) {
        files.push(fullPath);
      }
    }

    return files.sort();
  }

  /**
   * Get category from file path
   */
  private getCategory(filePath: string): string {
    const basename = path.basename(filePath, '.test');
    // Extract category prefix (e.g., "select" from "select1.test")
    const match = basename.match(/^([a-z]+)/);
    return match ? match[1] : 'other';
  }


  /**
   * Print progress character
   */
  private printProgress(char: string): void {
    process.stdout.write(char);
  }
}

// =============================================================================
// CLI
// =============================================================================

function printUsage(): void {
  console.log(`
SQLLogicTest Runner for DoSQL

Usage:
  npx tsx runner.ts [options]

Options:
  --limit=N         Run only first N tests per file (or total with --file)
  --file=PATH       Run specific test file
  --dir=PATH        Run all .test files in directory
  --folder=PATH     Alias for --dir (for compatibility)
  --batch-size=N    Process tests in batches of N for GC hints (default: ${MEMORY_CONFIG.BATCH_SIZE})
  --split-files=N   Max tests per file chunk (default: ${MEMORY_CONFIG.MAX_TESTS_PER_CHUNK})
  --verbose         Show detailed output
  --continue        Continue on error (don't stop at first failure)
  --stop-on-error   Stop at first failure
  --help            Show this help

Memory Management:
  The runner is optimized to stay within memory limits by:
  - Streaming records instead of loading entire file into memory
  - Processing tests in batches with periodic GC hints
  - Storing only limited failure information (max ${MEMORY_CONFIG.MAX_FAILURES_STORED})
  - Showing memory usage in progress indicator
  - Warning when memory pressure is high (>3GB)

  Note: Large test files (>2000 tests) may exhaust memory due to DoSQL engine
  internal storage growth. Use --limit to process files in parts.

Examples:
  npx tsx runner.ts --limit=1000
  npx tsx runner.ts --file=tests/select1.test --verbose
  npx tsx runner.ts --dir=./tests --continue
  npx tsx runner.ts --folder=tests/sqlite-full --batch-size=200

  # Run with memory limit (recommended for large test suites):
  NODE_OPTIONS="--max-old-space-size=4096" npx tsx runner.ts --folder=tests/sqlite-full

  # Run large files in parts to avoid memory exhaustion:
  NODE_OPTIONS="--max-old-space-size=4096" npx tsx runner.ts --file=tests/sqlite-full/select4.test --limit=1500
`);
}

function parseArgs(args: string[]): {
  limit: number;
  file?: string;
  dir?: string;
  verbose: boolean;
  continueOnError: boolean;
  batchSize: number;
  splitFiles: number;
  help: boolean;
} {
  const result = {
    limit: Infinity,
    file: undefined as string | undefined,
    dir: undefined as string | undefined,
    verbose: false,
    continueOnError: true,
    batchSize: MEMORY_CONFIG.BATCH_SIZE,
    splitFiles: MEMORY_CONFIG.MAX_TESTS_PER_CHUNK,
    help: false,
  };

  for (const arg of args) {
    if (arg.startsWith('--limit=')) {
      result.limit = parseInt(arg.slice(8), 10);
    } else if (arg.startsWith('--file=')) {
      result.file = arg.slice(7);
    } else if (arg.startsWith('--dir=')) {
      result.dir = arg.slice(6);
    } else if (arg.startsWith('--folder=')) {
      // Alias for --dir
      result.dir = arg.slice(9);
    } else if (arg.startsWith('--batch-size=')) {
      result.batchSize = parseInt(arg.slice(13), 10);
    } else if (arg.startsWith('--split-files=')) {
      result.splitFiles = parseInt(arg.slice(14), 10);
    } else if (arg === '--verbose' || arg === '-v') {
      result.verbose = true;
    } else if (arg === '--continue') {
      result.continueOnError = true;
    } else if (arg === '--stop-on-error') {
      result.continueOnError = false;
    } else if (arg === '--help' || arg === '-h') {
      result.help = true;
    }
  }

  return result;
}

function printSummary(summary: RunSummary): void {
  console.log('\n');
  console.log('='.repeat(60));
  console.log('SQLLOGICTEST RESULTS');
  console.log('='.repeat(60));
  console.log();

  const passRate = summary.totalTests > 0
    ? ((summary.passed / summary.totalTests) * 100).toFixed(2)
    : '0.00';

  console.log(`Total Tests:    ${summary.totalTests}`);
  console.log(`Passed:         ${summary.passed} (${passRate}%)`);
  console.log(`Failed:         ${summary.failed}`);
  console.log(`Skipped:        ${summary.skipped}`);
  console.log(`Execution Time: ${(summary.executionTime / 1000).toFixed(2)}s`);
  console.log();

  // Category breakdown
  if (summary.categoryResults.size > 0) {
    console.log('Results by Category:');
    console.log('-'.repeat(50));

    const categories = Array.from(summary.categoryResults.entries()).sort((a, b) =>
      a[0].localeCompare(b[0])
    );

    for (const [category, stats] of categories) {
      const total = stats.passed + stats.failed;
      const rate = total > 0 ? ((stats.passed / total) * 100).toFixed(1) : 'N/A';
      console.log(
        `  ${category.padEnd(15)} ${String(stats.passed).padStart(5)} / ${String(total).padStart(5)} passed (${rate}%)`
      );
    }
    console.log();
  }

  // Failed tests summary
  if (summary.failedTests.length > 0) {
    console.log('Failed Tests (first 10):');
    console.log('-'.repeat(50));

    for (const failure of summary.failedTests.slice(0, 10)) {
      console.log(`  Line ${failure.lineNumber}: ${failure.sql.slice(0, 60)}...`);
      if (failure.error) {
        console.log(`    Error: ${failure.error.slice(0, 80)}`);
      }
    }

    if (summary.failedTests.length > 10) {
      console.log(`  ... and ${summary.failedTests.length - 10} more stored`);
    }
    if (summary.failed > MEMORY_CONFIG.MAX_FAILURES_STORED) {
      console.log(`  (${summary.failed - MEMORY_CONFIG.MAX_FAILURES_STORED} additional failures not stored to save memory)`);
    }
    console.log();
  }

  console.log('='.repeat(60));
}

async function main(): Promise<void> {
  const args = parseArgs(process.argv.slice(2));

  if (args.help) {
    printUsage();
    process.exit(0);
  }

  // Determine what to run
  const scriptDir = path.dirname(new URL(import.meta.url).pathname);
  const defaultTestDir = path.join(scriptDir, 'tests');

  let targetPath: string;
  let isDirectory: boolean;

  if (args.file) {
    targetPath = path.resolve(args.file);
    isDirectory = false;
  } else if (args.dir) {
    targetPath = path.resolve(args.dir);
    isDirectory = true;
  } else {
    // Default: look for tests directory
    if (fs.existsSync(defaultTestDir)) {
      targetPath = defaultTestDir;
      isDirectory = true;
    } else {
      console.error('No test file or directory specified.');
      console.error(`Default test directory not found: ${defaultTestDir}`);
      console.error('Use --file=PATH or --dir=PATH to specify tests.');
      console.error('');
      console.error('To get started, download sqllogictest test files:');
      console.error('  mkdir -p packages/dosql/scripts/sqllogictest/tests');
      console.error('  # Download from https://www.sqlite.org/sqllogictest/');
      printUsage();
      process.exit(1);
    }
  }

  // Verify path exists
  if (!fs.existsSync(targetPath)) {
    console.error(`Path not found: ${targetPath}`);
    process.exit(1);
  }

  // Create runner
  const runner = new SqlLogicTestRunner({
    verbose: args.verbose,
    continueOnError: args.continueOnError,
    limit: args.limit,
    batchSize: args.batchSize,
    splitFiles: args.splitFiles,
  });

  console.log('SQLLogicTest Runner for DoSQL');
  console.log('='.repeat(60));
  console.log(`Target: ${targetPath}`);
  console.log(`Limit: ${args.limit === Infinity ? 'none' : args.limit}`);
  console.log(`Batch size: ${args.batchSize}`);
  console.log(`Split files: ${args.splitFiles} tests max per chunk`);
  console.log(`Verbose: ${args.verbose}`);
  console.log(`Continue on error: ${args.continueOnError}`);
  console.log(`Max failures stored: ${MEMORY_CONFIG.MAX_FAILURES_STORED}`);
  console.log();

  // Run tests
  let summary: RunSummary;

  if (isDirectory) {
    summary = await runner.runDirectory(targetPath);
  } else {
    summary = await runner.runFile(targetPath);
  }

  // Print results
  printSummary(summary);

  // Exit code based on results
  process.exit(summary.failed > 0 ? 1 : 0);
}

// Run
main().catch(e => {
  console.error('Fatal error:', e);
  process.exit(1);
});
