/**
 * SQL Sanitizer for Safe Tracing
 *
 * Sanitizes SQL statements to remove sensitive data before
 * including in trace spans and logs.
 */

import type { SQLSanitizer, StatementType } from './types.js';

/**
 * SQL sanitizer implementation
 */
export class SQLSanitizerImpl implements SQLSanitizer {
  /**
   * Sanitize SQL statement by replacing parameter values
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
   * Extract the SQL statement type
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
   * Extract table names from SQL statement
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
 * Create a SQL sanitizer instance
 */
export function createSQLSanitizer(): SQLSanitizer {
  return new SQLSanitizerImpl();
}
