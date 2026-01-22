#!/usr/bin/env npx tsx
/**
 * SQLLogicTest Runner for DoSQL
 *
 * Runs sqllogictest test files against the DoSQL engine.
 *
 * Usage:
 *   npx tsx packages/dosql/scripts/sqllogictest/runner.ts [options]
 *
 * Options:
 *   --limit=N       Run only first N tests
 *   --file=PATH     Run specific test file
 *   --dir=PATH      Run all .test files in directory
 *   --verbose       Show detailed output
 *   --continue      Continue on error (don't stop at first failure)
 *   --category=X    Run only tests in category X
 *   --skip-category=X Skip tests in category X
 *
 * Examples:
 *   npx tsx runner.ts --limit=1000
 *   npx tsx runner.ts --file=select1.test --verbose
 *   npx tsx runner.ts --dir=./tests --continue
 */

import * as fs from 'fs';
import * as path from 'path';
import {
  parseTestFile,
  shouldSkip,
  formatValue,
  sortValues,
  type Record,
  type StatementRecord,
  type QueryRecord,
  type ResultType,
} from './parser.js';

// =============================================================================
// DOSQL ENGINE INTERFACE
// =============================================================================

// Import DoSQL engine
import { Database } from '../../src/database.js';

/**
 * Test database wrapper
 */
class TestDatabase {
  private db: Database;

  constructor() {
    this.db = new Database(':memory:');
  }

  /**
   * Execute a SQL statement (no result expected)
   */
  execute(sql: string): { success: boolean; error?: string } {
    try {
      // Try to detect if it's a query or statement
      const normalized = sql.trim().toUpperCase();

      if (
        normalized.startsWith('SELECT') ||
        normalized.startsWith('VALUES') ||
        normalized.startsWith('WITH')
      ) {
        // It's a query, but we're treating as statement
        this.db.prepare(sql).all();
      } else {
        this.db.exec(sql);
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
      const stmt = this.db.prepare(sql);
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
   * Reset the database
   */
  reset(): void {
    this.db.close();
    this.db = new Database(':memory:');
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
  executionTime: number;
}

interface RunSummary {
  totalTests: number;
  passed: number;
  failed: number;
  skipped: number;
  errors: number;
  executionTime: number;
  categoryResults: Map<string, { passed: number; failed: number; skipped: number }>;
  failedTests: TestResult[];
}

// =============================================================================
// RUNNER
// =============================================================================

class SqlLogicTestRunner {
  private db: TestDatabase;
  private verbose: boolean;
  private continueOnError: boolean;
  private limit: number;
  private testCount: number = 0;
  private results: TestResult[] = [];

  constructor(options: {
    verbose?: boolean;
    continueOnError?: boolean;
    limit?: number;
  } = {}) {
    this.db = new TestDatabase();
    this.verbose = options.verbose ?? false;
    this.continueOnError = options.continueOnError ?? true;
    this.limit = options.limit ?? Infinity;
  }

  /**
   * Run tests from a file
   */
  async runFile(filePath: string): Promise<RunSummary> {
    const content = fs.readFileSync(filePath, 'utf-8');
    const { records, errors, stats } = parseTestFile(content);

    if (errors.length > 0 && this.verbose) {
      console.error(`Parse errors in ${filePath}:`);
      for (const error of errors) {
        console.error(`  Line ${error.line}: ${error.message}`);
      }
    }

    const startTime = performance.now();
    const categoryResults = new Map<string, { passed: number; failed: number; skipped: number }>();
    const failedTests: TestResult[] = [];

    let passed = 0;
    let failed = 0;
    let skipped = 0;
    let errorCount = 0;

    // Determine category from file path
    const category = this.getCategory(filePath);
    if (!categoryResults.has(category)) {
      categoryResults.set(category, { passed: 0, failed: 0, skipped: 0 });
    }
    const catStats = categoryResults.get(category)!;

    for (const record of records) {
      // Check limit
      if (this.testCount >= this.limit) {
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

      // Run the test
      const result = this.runRecord(record);
      this.results.push(result);

      if (result.passed) {
        passed++;
        catStats.passed++;
        if (this.verbose) {
          this.printProgress('.');
        }
      } else {
        failed++;
        catStats.failed++;
        failedTests.push(result);

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
        }

        if (!this.continueOnError) {
          break;
        }
      }

      // Progress indicator
      if (!this.verbose && this.testCount % 100 === 0) {
        process.stdout.write(`\rProgress: ${this.testCount} tests...`);
      }
    }

    const endTime = performance.now();

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
   */
  async runDirectory(dirPath: string): Promise<RunSummary> {
    const files = this.findTestFiles(dirPath);
    const allSummaries: RunSummary[] = [];

    console.log(`Found ${files.length} test files in ${dirPath}`);

    // Track global test count across files
    let globalTestCount = 0;

    for (const file of files) {
      if (globalTestCount >= this.limit) {
        break;
      }

      console.log(`\nRunning ${path.basename(file)}...`);
      this.db.reset(); // Fresh database for each file
      this.testCount = 0; // Reset per-file count
      this.results = []; // Reset results

      const summary = await this.runFile(file);
      allSummaries.push(summary);
      globalTestCount += summary.passed + summary.failed;
    }

    // Merge summaries
    return this.mergeSummaries(allSummaries);
  }

  /**
   * Run a single record
   */
  private runRecord(record: Record): TestResult {
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
   */
  private runQuery(record: QueryRecord, startTime: number): TestResult {
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
    for (const row of result.rows || []) {
      for (let i = 0; i < row.length; i++) {
        const type = record.columnTypes[i] || 'T';
        actualValues.push(formatValue(row[i], type));
      }
    }

    // Sort values according to sort mode
    const sortedActual = sortValues(actualValues, record.sortMode, record.columnTypes.length);
    const sortedExpected = sortValues(record.expectedValues, record.sortMode, record.columnTypes.length);

    // Compare results
    const passed = this.compareResults(sortedExpected, sortedActual);

    return {
      passed,
      record,
      expected: sortedExpected,
      actual: sortedActual,
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
   * Merge multiple summaries
   */
  private mergeSummaries(summaries: RunSummary[]): RunSummary {
    const merged: RunSummary = {
      totalTests: 0,
      passed: 0,
      failed: 0,
      skipped: 0,
      errors: 0,
      executionTime: 0,
      categoryResults: new Map(),
      failedTests: [],
    };

    for (const summary of summaries) {
      merged.totalTests += summary.totalTests;
      merged.passed += summary.passed;
      merged.failed += summary.failed;
      merged.skipped += summary.skipped;
      merged.errors += summary.errors;
      merged.executionTime += summary.executionTime;
      merged.failedTests.push(...summary.failedTests);

      for (const [cat, stats] of summary.categoryResults) {
        const existing = merged.categoryResults.get(cat) || { passed: 0, failed: 0, skipped: 0 };
        existing.passed += stats.passed;
        existing.failed += stats.failed;
        existing.skipped += stats.skipped;
        merged.categoryResults.set(cat, existing);
      }
    }

    return merged;
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
  --limit=N         Run only first N tests
  --file=PATH       Run specific test file
  --dir=PATH        Run all .test files in directory
  --verbose         Show detailed output
  --continue        Continue on error (don't stop at first failure)
  --help            Show this help

Examples:
  npx tsx runner.ts --limit=1000
  npx tsx runner.ts --file=tests/select1.test --verbose
  npx tsx runner.ts --dir=./tests --continue
`);
}

function parseArgs(args: string[]): {
  limit: number;
  file?: string;
  dir?: string;
  verbose: boolean;
  continueOnError: boolean;
  help: boolean;
} {
  const result = {
    limit: Infinity,
    file: undefined as string | undefined,
    dir: undefined as string | undefined,
    verbose: false,
    continueOnError: true,
    help: false,
  };

  for (const arg of args) {
    if (arg.startsWith('--limit=')) {
      result.limit = parseInt(arg.slice(8), 10);
    } else if (arg.startsWith('--file=')) {
      result.file = arg.slice(7);
    } else if (arg.startsWith('--dir=')) {
      result.dir = arg.slice(6);
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

    for (const result of summary.failedTests.slice(0, 10)) {
      const record = result.record;
      if (record.type === 'statement' || record.type === 'query') {
        console.log(`  Line ${record.lineNumber}: ${record.sql.slice(0, 60)}...`);
        if (result.error) {
          console.log(`    Error: ${result.error.slice(0, 80)}`);
        }
      }
    }

    if (summary.failedTests.length > 10) {
      console.log(`  ... and ${summary.failedTests.length - 10} more`);
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
  });

  console.log('SQLLogicTest Runner for DoSQL');
  console.log('='.repeat(60));
  console.log(`Target: ${targetPath}`);
  console.log(`Limit: ${args.limit === Infinity ? 'none' : args.limit}`);
  console.log(`Verbose: ${args.verbose}`);
  console.log(`Continue on error: ${args.continueOnError}`);
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
