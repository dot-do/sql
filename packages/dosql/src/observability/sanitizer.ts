/**
 * SQL Sanitizer for Safe Tracing
 *
 * Sanitizes SQL statements to remove sensitive data before
 * including in trace spans and logs.
 */

import type { SQLSanitizer, StatementType } from './types.js';

/**
 * SQL sanitizer implementation for safe tracing and logging.
 *
 * Removes or masks sensitive data from SQL statements before they are
 * included in trace spans, logs, or metrics. This prevents accidental
 * exposure of passwords, tokens, PII, and other sensitive values.
 *
 * Sanitization includes:
 * - Replacing string literals with '?'
 * - Masking long numeric values (potential tokens/IDs)
 * - Masking hex strings (potential cryptographic values)
 * - Normalizing whitespace
 *
 * @example
 * ```typescript
 * const sanitizer = new SQLSanitizerImpl();
 *
 * // String literals are replaced
 * sanitizer.sanitize("SELECT * FROM users WHERE password = 'secret123'");
 * // Returns: "SELECT * FROM users WHERE password = '?'"
 *
 * // Extract statement type
 * sanitizer.extractStatementType("INSERT INTO users ..."); // Returns: 'INSERT'
 *
 * // Extract table names
 * sanitizer.extractTableNames("SELECT * FROM users JOIN orders ON ...");
 * // Returns: ['users', 'orders']
 * ```
 */
export class SQLSanitizerImpl implements SQLSanitizer {
  /**
   * Sanitizes a SQL statement by replacing sensitive values with placeholders.
   *
   * This method removes potentially sensitive data from SQL statements:
   * - String literals are replaced with '?'
   * - Long numeric values (10+ digits) are replaced with '?'
   * - Hex strings (16+ chars) are replaced with '?'
   * - Whitespace is normalized
   *
   * @param sql - The SQL statement to sanitize
   * @param _params - Optional query parameters (reserved for future use)
   * @returns The sanitized SQL statement safe for logging/tracing
   *
   * @example
   * ```typescript
   * sanitize("SELECT * FROM users WHERE email = 'user@example.com'")
   * // Returns: "SELECT * FROM users WHERE email = '?'"
   *
   * sanitize("SELECT * FROM tokens WHERE token = 0x1234567890abcdef1234")
   * // Returns: "SELECT * FROM tokens WHERE token = ?"
   * ```
   */
  sanitize(sql: string, _params?: unknown[]): string {
    // Replace string literals with '?'
    let sanitized = sql.replace(/'([^'\\]|\\.)*'/g, "'?'");

    // Replace numeric literals that look like sensitive data (long numbers)
    // Keep short numbers as they're likely IDs or counts
    sanitized = sanitized.replace(/\b\d{10,}\b/g, '?');

    // Replace hex strings that might be tokens
    sanitized = sanitized.replace(/\b0x[0-9a-fA-F]{16,}\b/g, '?');

    // Normalize whitespace
    sanitized = sanitized.replace(/\s+/g, ' ').trim();

    return sanitized;
  }

  /**
   * Extracts the SQL statement type from a query string.
   *
   * Identifies the primary operation type by examining the first keyword
   * of the normalized SQL statement.
   *
   * @param sql - The SQL statement to analyze
   * @returns The statement type: 'SELECT', 'INSERT', 'UPDATE', 'DELETE', or 'OTHER'
   *
   * @example
   * ```typescript
   * extractStatementType("SELECT * FROM users") // Returns: 'SELECT'
   * extractStatementType("  insert into users...") // Returns: 'INSERT'
   * extractStatementType("CREATE TABLE foo") // Returns: 'OTHER'
   * ```
   */
  extractStatementType(sql: string): StatementType {
    const normalized = sql.trim().toUpperCase();

    if (normalized.startsWith('SELECT')) {
      return 'SELECT';
    }
    if (normalized.startsWith('INSERT')) {
      return 'INSERT';
    }
    if (normalized.startsWith('UPDATE')) {
      return 'UPDATE';
    }
    if (normalized.startsWith('DELETE')) {
      return 'DELETE';
    }

    return 'OTHER';
  }

  /**
   * Extracts table names from a SQL statement.
   *
   * Parses the SQL statement to find all referenced table names from:
   * - FROM clauses (including multiple tables)
   * - JOIN clauses
   * - INSERT INTO targets
   * - UPDATE targets
   * - DELETE FROM targets
   *
   * Table name delimiters (backticks, quotes, brackets) are stripped from results.
   *
   * @param sql - The SQL statement to analyze
   * @returns Array of unique table names found in the statement
   *
   * @example
   * ```typescript
   * extractTableNames("SELECT * FROM users u JOIN orders o ON u.id = o.user_id")
   * // Returns: ['users', 'orders']
   *
   * extractTableNames("INSERT INTO `my_table` (col) VALUES (1)")
   * // Returns: ['my_table']
   * ```
   */
  extractTableNames(sql: string): string[] {
    const tables: string[] = [];
    const normalized = sql.replace(/\s+/g, ' ').trim();

    // Match FROM clause tables
    const fromMatch = normalized.match(/\bFROM\s+([^\s,;()]+(?:\s*(?:AS\s+)?\w+)?(?:\s*,\s*[^\s,;()]+(?:\s*(?:AS\s+)?\w+)?)*)/i);
    if (fromMatch) {
      const fromClause = fromMatch[1];
      const tableMatches = fromClause.split(/\s*,\s*/);
      for (const tm of tableMatches) {
        const tableName = tm.split(/\s+/)[0].replace(/[`"[\]]/g, '');
        if (tableName && !tables.includes(tableName)) {
          tables.push(tableName);
        }
      }
    }

    // Match JOIN clause tables
    const joinMatches = Array.from(normalized.matchAll(/\bJOIN\s+([^\s]+)/gi));
    for (const match of joinMatches) {
      const tableName = match[1].replace(/[`"[\]]/g, '');
      if (tableName && !tables.includes(tableName)) {
        tables.push(tableName);
      }
    }

    // Match INSERT INTO
    const insertMatch = normalized.match(/\bINSERT\s+INTO\s+([^\s(]+)/i);
    if (insertMatch) {
      const tableName = insertMatch[1].replace(/[`"[\]]/g, '');
      if (tableName && !tables.includes(tableName)) {
        tables.push(tableName);
      }
    }

    // Match UPDATE
    const updateMatch = normalized.match(/\bUPDATE\s+([^\s]+)/i);
    if (updateMatch) {
      const tableName = updateMatch[1].replace(/[`"[\]]/g, '');
      if (tableName && !tables.includes(tableName)) {
        tables.push(tableName);
      }
    }

    // Match DELETE FROM
    const deleteMatch = normalized.match(/\bDELETE\s+FROM\s+([^\s]+)/i);
    if (deleteMatch) {
      const tableName = deleteMatch[1].replace(/[`"[\]]/g, '');
      if (tableName && !tables.includes(tableName)) {
        tables.push(tableName);
      }
    }

    return tables;
  }
}

/**
 * Creates a SQL sanitizer instance for safe tracing and logging.
 *
 * The sanitizer removes sensitive data from SQL statements before they
 * are included in trace spans, logs, or metrics to prevent accidental
 * exposure of passwords, tokens, and PII.
 *
 * @returns A new SQLSanitizer instance
 *
 * @example
 * ```typescript
 * const sanitizer = createSQLSanitizer();
 *
 * const safeSql = sanitizer.sanitize(
 *   "SELECT * FROM users WHERE api_key = 'sk_live_abc123'"
 * );
 * // safeSql: "SELECT * FROM users WHERE api_key = '?'"
 *
 * const statementType = sanitizer.extractStatementType(sql); // 'SELECT'
 * const tables = sanitizer.extractTableNames(sql); // ['users']
 * ```
 */
export function createSQLSanitizer(): SQLSanitizer {
  return new SQLSanitizerImpl();
}
