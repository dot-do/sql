/**
 * SQLLogicTest Parser
 *
 * Parses .test files from the SQLite sqllogictest suite.
 * Reference: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
 *
 * Record types:
 * - statement ok/error
 * - query <type-string> <sort-mode> <label>
 * - hash-threshold <n>
 * - halt
 * - skipif/onlyif <db>
 */

// =============================================================================
// TYPES
// =============================================================================

/**
 * Result type character for query columns
 * - T = text
 * - I = integer
 * - R = real (floating point)
 */
export type ResultType = 'T' | 'I' | 'R';

/**
 * Sort mode for query results
 * - nosort: exact order (use with ORDER BY or single row)
 * - rowsort: sort rows as text (strcmp)
 * - valuesort: sort individual values
 */
export type SortMode = 'nosort' | 'rowsort' | 'valuesort';

/**
 * Statement record - execute SQL with expected ok/error
 */
export interface StatementRecord {
  type: 'statement';
  expectedResult: 'ok' | 'error';
  sql: string;
  lineNumber: number;
  skipif?: string[];
  onlyif?: string[];
}

/**
 * Query record - execute SQL and verify results
 */
export interface QueryRecord {
  type: 'query';
  columnTypes: ResultType[];
  sortMode: SortMode;
  label?: string;
  sql: string;
  expectedValues: string[];
  expectedHash?: string;
  lineNumber: number;
  skipif?: string[];
  onlyif?: string[];
}

/**
 * Hash-threshold control record
 */
export interface HashThresholdRecord {
  type: 'hash-threshold';
  threshold: number;
  lineNumber: number;
}

/**
 * Halt record (stops processing)
 */
export interface HaltRecord {
  type: 'halt';
  lineNumber: number;
}

/**
 * All possible record types
 */
export type Record = StatementRecord | QueryRecord | HashThresholdRecord | HaltRecord;

/**
 * Parse result
 */
export interface ParseResult {
  records: Record[];
  errors: ParseError[];
  stats: {
    statements: number;
    queries: number;
    totalLines: number;
  };
}

/**
 * Parse error
 */
export interface ParseError {
  line: number;
  message: string;
  content?: string;
}

// =============================================================================
// PARSER
// =============================================================================

/**
 * Parse a sqllogictest file content
 */
export function parseTestFile(content: string): ParseResult {
  const lines = content.split('\n');
  const records: Record[] = [];
  const errors: ParseError[] = [];

  let i = 0;
  let statements = 0;
  let queries = 0;

  // Current conditional context
  let skipif: string[] = [];
  let onlyif: string[] = [];

  while (i < lines.length) {
    const line = lines[i].trim();
    const lineNumber = i + 1;

    // Skip empty lines and comments
    if (line === '' || line.startsWith('#')) {
      i++;
      continue;
    }

    // Handle skipif/onlyif
    if (line.startsWith('skipif ')) {
      skipif.push(line.slice(7).trim());
      i++;
      continue;
    }

    if (line.startsWith('onlyif ')) {
      onlyif.push(line.slice(7).trim());
      i++;
      continue;
    }

    // Handle halt (respect onlyif/skipif conditions)
    if (line === 'halt') {
      // Only halt if conditions are met:
      // - If onlyif is set, only halt for that db (we run as 'sqlite')
      // - If skipif is set, don't halt for that db
      const shouldHalt =
        (onlyif.length === 0 || onlyif.includes('sqlite')) &&
        !skipif.includes('sqlite');

      if (shouldHalt) {
        records.push({ type: 'halt', lineNumber });
        break;
      }
      // Clear conditions and continue
      skipif = [];
      onlyif = [];
      i++;
      continue;
    }

    // Handle hash-threshold
    if (line.startsWith('hash-threshold ')) {
      const threshold = parseInt(line.slice(15).trim(), 10);
      records.push({ type: 'hash-threshold', threshold, lineNumber });
      i++;
      continue;
    }

    // Handle statement
    if (line.startsWith('statement ')) {
      const result = parseStatement(lines, i, skipif, onlyif);
      if (result.record) {
        records.push(result.record);
        statements++;
      }
      if (result.error) {
        errors.push(result.error);
      }
      i = result.nextLine;
      skipif = [];
      onlyif = [];
      continue;
    }

    // Handle query
    if (line.startsWith('query ')) {
      const result = parseQuery(lines, i, skipif, onlyif);
      if (result.record) {
        records.push(result.record);
        queries++;
      }
      if (result.error) {
        errors.push(result.error);
      }
      i = result.nextLine;
      skipif = [];
      onlyif = [];
      continue;
    }

    // Unknown line - skip but maybe warn
    i++;
  }

  return {
    records,
    errors,
    stats: {
      statements,
      queries,
      totalLines: lines.length,
    },
  };
}

/**
 * Parse a statement record
 */
function parseStatement(
  lines: string[],
  startLine: number,
  skipif: string[],
  onlyif: string[]
): { record?: StatementRecord; error?: ParseError; nextLine: number } {
  const lineNumber = startLine + 1;
  const header = lines[startLine].trim();

  // Parse "statement ok" or "statement error"
  const match = header.match(/^statement\s+(ok|error)$/i);
  if (!match) {
    return {
      error: {
        line: lineNumber,
        message: `Invalid statement header: expected "statement ok" or "statement error"`,
        content: header,
      },
      nextLine: startLine + 1,
    };
  }

  const expectedResult = match[1].toLowerCase() as 'ok' | 'error';

  // Collect SQL lines until blank line or new record
  const sqlLines: string[] = [];
  let i = startLine + 1;

  while (i < lines.length) {
    const line = lines[i];
    const trimmed = line.trim();

    // Stop at blank line
    if (trimmed === '') {
      break;
    }

    // Stop at new record indicators
    if (
      trimmed.startsWith('statement ') ||
      trimmed.startsWith('query ') ||
      trimmed.startsWith('hash-threshold ') ||
      trimmed.startsWith('skipif ') ||
      trimmed.startsWith('onlyif ') ||
      trimmed === 'halt'
    ) {
      break;
    }

    // Skip comment lines in SQL
    if (!trimmed.startsWith('#')) {
      sqlLines.push(line);
    }

    i++;
  }

  // Remove trailing semicolons (spec says SQL should not include terminator)
  let sql = sqlLines.join('\n').trim();
  if (sql.endsWith(';')) {
    sql = sql.slice(0, -1).trim();
  }

  return {
    record: {
      type: 'statement',
      expectedResult,
      sql,
      lineNumber,
      ...(skipif.length > 0 && { skipif }),
      ...(onlyif.length > 0 && { onlyif }),
    },
    nextLine: i,
  };
}

/**
 * Parse a query record
 */
function parseQuery(
  lines: string[],
  startLine: number,
  skipif: string[],
  onlyif: string[]
): { record?: QueryRecord; error?: ParseError; nextLine: number } {
  const lineNumber = startLine + 1;
  const header = lines[startLine].trim();

  // Parse "query <types> [sortmode] [label]"
  const match = header.match(/^query\s+([TIR]+)(?:\s+(nosort|rowsort|valuesort))?(?:\s+(.+))?$/i);
  if (!match) {
    return {
      error: {
        line: lineNumber,
        message: `Invalid query header: expected "query <types> [sortmode] [label]"`,
        content: header,
      },
      nextLine: startLine + 1,
    };
  }

  const columnTypes = match[1].toUpperCase().split('') as ResultType[];
  const sortMode = (match[2]?.toLowerCase() || 'nosort') as SortMode;
  const label = match[3]?.trim();

  // Collect SQL lines until separator (----)
  const sqlLines: string[] = [];
  let i = startLine + 1;
  let foundSeparator = false;

  while (i < lines.length) {
    const line = lines[i];
    const trimmed = line.trim();

    // Found separator
    if (trimmed.startsWith('----')) {
      foundSeparator = true;
      i++;
      break;
    }

    // Stop at blank line (prototype script without results)
    if (trimmed === '') {
      break;
    }

    // Stop at new record indicators
    if (
      trimmed.startsWith('statement ') ||
      trimmed.startsWith('query ') ||
      trimmed.startsWith('hash-threshold ') ||
      trimmed.startsWith('skipif ') ||
      trimmed.startsWith('onlyif ') ||
      trimmed === 'halt'
    ) {
      break;
    }

    // Skip comment lines in SQL
    if (!trimmed.startsWith('#')) {
      sqlLines.push(line);
    }

    i++;
  }

  let sql = sqlLines.join('\n').trim();
  if (sql.endsWith(';')) {
    sql = sql.slice(0, -1).trim();
  }

  // Collect expected values if separator was found
  const expectedValues: string[] = [];
  let expectedHash: string | undefined;

  if (foundSeparator) {
    while (i < lines.length) {
      const line = lines[i];
      const trimmed = line.trim();

      // Stop at blank line
      if (trimmed === '') {
        i++;
        break;
      }

      // Stop at new record indicators
      if (
        trimmed.startsWith('statement ') ||
        trimmed.startsWith('query ') ||
        trimmed.startsWith('hash-threshold ') ||
        trimmed.startsWith('skipif ') ||
        trimmed.startsWith('onlyif ') ||
        trimmed === 'halt'
      ) {
        break;
      }

      // Check if this looks like a hash (MD5 is 32 hex chars, format: "N values hashing to HASH")
      const hashMatch = trimmed.match(/^(\d+)\s+values?\s+hashing\s+to\s+([a-f0-9]{32})$/i);
      if (hashMatch) {
        expectedHash = hashMatch[2];
        i++;
        continue;
      }

      // Regular value
      expectedValues.push(trimmed);
      i++;
    }
  }

  return {
    record: {
      type: 'query',
      columnTypes,
      sortMode,
      ...(label && { label }),
      sql,
      expectedValues,
      ...(expectedHash && { expectedHash }),
      lineNumber,
      ...(skipif.length > 0 && { skipif }),
      ...(onlyif.length > 0 && { onlyif }),
    },
    nextLine: i,
  };
}

/**
 * Check if a record should be skipped for a given database
 */
export function shouldSkip(record: StatementRecord | QueryRecord, dbName: string): boolean {
  // If onlyif is specified, skip unless we match
  if (record.onlyif && record.onlyif.length > 0) {
    if (!record.onlyif.includes(dbName)) {
      return true;
    }
  }

  // If skipif is specified, skip if we match
  if (record.skipif && record.skipif.length > 0) {
    if (record.skipif.includes(dbName)) {
      return true;
    }
  }

  return false;
}

/**
 * Format a value for comparison (following sqllogictest spec)
 * - Integer: printf("%d")
 * - Float: printf("%.3f")
 * - NULL: "NULL"
 * - Empty string: "(empty)"
 */
export function formatValue(value: unknown, type: ResultType): string {
  if (value === null || value === undefined) {
    return 'NULL';
  }

  if (typeof value === 'string') {
    if (value === '') {
      return '(empty)';
    }
    // Replace control characters with @
    return value.replace(/[\x00-\x1f]/g, '@');
  }

  if (typeof value === 'number') {
    if (type === 'I') {
      return Math.floor(value).toString();
    }
    if (type === 'R') {
      return value.toFixed(3);
    }
  }

  if (typeof value === 'bigint') {
    return value.toString();
  }

  return String(value);
}

/**
 * Sort values according to sort mode
 */
export function sortValues(values: string[], sortMode: SortMode, columnCount: number): string[] {
  if (sortMode === 'nosort') {
    return values;
  }

  if (sortMode === 'valuesort') {
    return [...values].sort();
  }

  if (sortMode === 'rowsort') {
    // Group values into rows, sort rows, then flatten
    const rows: string[][] = [];
    for (let i = 0; i < values.length; i += columnCount) {
      rows.push(values.slice(i, i + columnCount));
    }

    rows.sort((a, b) => {
      for (let i = 0; i < a.length; i++) {
        const cmp = a[i].localeCompare(b[i]);
        if (cmp !== 0) return cmp;
      }
      return 0;
    });

    return rows.flat();
  }

  return values;
}

/**
 * Compute MD5 hash of values (for hash comparison)
 *
 * SQLLogicTest uses MD5 hashes to verify query results.
 * The format joins values with newlines and computes the hash.
 */
export async function hashValues(values: string[]): Promise<string> {
  // SQLLogicTest hash format: values joined with newlines, plus a trailing newline
  const text = values.join('\n') + '\n';

  // Use Node.js crypto module (MD5 not available in Web Crypto API)
  const { createHash } = await import('crypto');
  return createHash('md5').update(text).digest('hex');
}
