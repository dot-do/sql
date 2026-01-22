/**
 * Mock Query Executor for Testing
 *
 * This module provides test utilities for the RPC layer.
 * These should ONLY be used in test files, never in production code.
 *
 * @packageDocumentation
 */

import type {
  QueryExecutor,
  ExecuteOptions,
  ExecuteResult,
  TransactionOptions,
} from '../../rpc/server.js';

import type { ColumnType, TableSchema } from '../../rpc/types.js';

// =============================================================================
// SQL Tokenizer (FOR TESTING ONLY)
// =============================================================================

/**
 * Token types for SQL tokenization
 * @internal FOR TESTING ONLY
 */
type SqlTokenType = 'keyword' | 'identifier' | 'string' | 'number' | 'operator' | 'punctuation' | 'whitespace' | 'comment' | 'parameter';

/**
 * SQL Token
 * @internal FOR TESTING ONLY
 */
interface SqlToken {
  type: SqlTokenType;
  value: string;
  /** Original position in input string */
  position: number;
}

/**
 * Simple SQL tokenizer that respects string literal boundaries
 *
 * FOR TESTING ONLY - This is not a production-grade SQL parser.
 * It handles:
 * - Single and double quoted strings
 * - Escaped quotes ('' and \')
 * - Line comments (--)
 * - Block comments (/* ... *\/)
 * - Basic SQL keywords and identifiers
 *
 * @internal FOR TESTING ONLY
 */
export function tokenizeSql(sql: string): SqlToken[] {
  const tokens: SqlToken[] = [];
  let i = 0;
  const len = sql.length;

  while (i < len) {
    const char = sql[i];
    const remaining = sql.slice(i);

    // Skip null bytes (potential injection attempt)
    if (char === '\x00') {
      i++;
      continue;
    }

    // Whitespace
    if (/\s/.test(char)) {
      let value = char;
      let j = i + 1;
      while (j < len && /\s/.test(sql[j])) {
        value += sql[j];
        j++;
      }
      tokens.push({ type: 'whitespace', value, position: i });
      i = j;
      continue;
    }

    // Line comment (--)
    if (remaining.startsWith('--')) {
      let value = '--';
      let j = i + 2;
      while (j < len && sql[j] !== '\n') {
        value += sql[j];
        j++;
      }
      tokens.push({ type: 'comment', value, position: i });
      i = j;
      continue;
    }

    // Block comment (/* ... */)
    if (remaining.startsWith('/*')) {
      let value = '/*';
      let j = i + 2;
      while (j < len - 1) {
        if (sql[j] === '*' && sql[j + 1] === '/') {
          value += '*/';
          j += 2;
          break;
        }
        value += sql[j];
        j++;
      }
      // Handle unclosed comment - consume rest
      if (j >= len - 1 && !value.endsWith('*/')) {
        value += sql.slice(j);
        j = len;
      }
      tokens.push({ type: 'comment', value, position: i });
      i = j;
      continue;
    }

    // Single-quoted string
    if (char === "'") {
      let value = "'";
      let j = i + 1;
      while (j < len) {
        const c = sql[j];
        // Handle escape sequences
        if (c === "'" && j + 1 < len && sql[j + 1] === "'") {
          // SQL standard: '' escapes to single '
          value += "''";
          j += 2;
          continue;
        }
        if (c === '\\' && j + 1 < len) {
          // Backslash escape (non-standard but common)
          value += c + sql[j + 1];
          j += 2;
          continue;
        }
        if (c === "'") {
          value += "'";
          j++;
          break;
        }
        value += c;
        j++;
      }
      tokens.push({ type: 'string', value, position: i });
      i = j;
      continue;
    }

    // Double-quoted string/identifier
    if (char === '"') {
      let value = '"';
      let j = i + 1;
      while (j < len) {
        const c = sql[j];
        // Handle escape sequences
        if (c === '"' && j + 1 < len && sql[j + 1] === '"') {
          // SQL standard: "" escapes to single "
          value += '""';
          j += 2;
          continue;
        }
        if (c === '\\' && j + 1 < len) {
          // Backslash escape
          value += c + sql[j + 1];
          j += 2;
          continue;
        }
        if (c === '"') {
          value += '"';
          j++;
          break;
        }
        value += c;
        j++;
      }
      tokens.push({ type: 'string', value, position: i });
      i = j;
      continue;
    }

    // Parameter markers ($1, $2, etc.)
    if (char === '$' && /\d/.test(sql[i + 1] || '')) {
      let value = '$';
      let j = i + 1;
      while (j < len && /\d/.test(sql[j])) {
        value += sql[j];
        j++;
      }
      tokens.push({ type: 'parameter', value, position: i });
      i = j;
      continue;
    }

    // Named parameter (:name)
    if (char === ':' && /[a-zA-Z_]/.test(sql[i + 1] || '')) {
      let value = ':';
      let j = i + 1;
      while (j < len && /[a-zA-Z0-9_]/.test(sql[j])) {
        value += sql[j];
        j++;
      }
      tokens.push({ type: 'parameter', value, position: i });
      i = j;
      continue;
    }

    // Positional parameter (?)
    if (char === '?') {
      tokens.push({ type: 'parameter', value: '?', position: i });
      i++;
      continue;
    }

    // Numbers
    if (/\d/.test(char)) {
      let value = char;
      let j = i + 1;
      while (j < len && /[\d.]/.test(sql[j])) {
        value += sql[j];
        j++;
      }
      tokens.push({ type: 'number', value, position: i });
      i = j;
      continue;
    }

    // Identifiers and keywords
    if (/[a-zA-Z_]/.test(char)) {
      let value = char;
      let j = i + 1;
      while (j < len && /[a-zA-Z0-9_]/.test(sql[j])) {
        value += sql[j];
        j++;
      }
      const upper = value.toUpperCase();
      const keywords = [
        'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET',
        'DELETE', 'CREATE', 'TABLE', 'DROP', 'ALTER', 'INDEX', 'JOIN', 'LEFT',
        'RIGHT', 'INNER', 'OUTER', 'ON', 'AND', 'OR', 'NOT', 'IN', 'LIKE',
        'BETWEEN', 'IS', 'NULL', 'TRUE', 'FALSE', 'AS', 'ORDER', 'BY', 'ASC',
        'DESC', 'LIMIT', 'OFFSET', 'UNION', 'ALL', 'DISTINCT', 'GROUP', 'HAVING',
        'BEGIN', 'COMMIT', 'ROLLBACK', 'TRANSACTION',
      ];
      const type: SqlTokenType = keywords.includes(upper) ? 'keyword' : 'identifier';
      tokens.push({ type, value, position: i });
      i = j;
      continue;
    }

    // Semicolon (statement separator)
    if (char === ';') {
      tokens.push({ type: 'punctuation', value: ';', position: i });
      i++;
      continue;
    }

    // Other operators and punctuation
    const operators = ['(', ')', ',', '.', '=', '<', '>', '!', '+', '-', '*', '/', '%'];
    if (operators.includes(char)) {
      // Handle multi-character operators
      let value = char;
      if ((char === '<' || char === '>' || char === '!' || char === '=') && sql[i + 1] === '=') {
        value += '=';
        i++;
      } else if (char === '<' && sql[i + 1] === '>') {
        value += '>';
        i++;
      }
      tokens.push({ type: 'operator', value, position: i });
      i++;
      continue;
    }

    // Unknown character - skip it
    i++;
  }

  return tokens;
}

/**
 * Check if SQL contains multiple statements (semicolon outside of strings/comments)
 * @internal FOR TESTING ONLY
 */
export function hasMultipleStatements(tokens: SqlToken[]): boolean {
  const significantTokens = tokens.filter(t => t.type !== 'whitespace' && t.type !== 'comment');
  const semicolonIndex = significantTokens.findIndex(t => t.type === 'punctuation' && t.value === ';');

  if (semicolonIndex === -1) return false;

  // Check if there's anything meaningful after the semicolon
  const afterSemicolon = significantTokens.slice(semicolonIndex + 1);
  return afterSemicolon.some(t => t.type !== 'whitespace' && t.type !== 'comment');
}

/**
 * Extract table name from SELECT statement tokens
 * @internal FOR TESTING ONLY
 */
export function extractTableFromSelect(tokens: SqlToken[]): string | null {
  const significantTokens = tokens.filter(t => t.type !== 'whitespace' && t.type !== 'comment');

  for (let i = 0; i < significantTokens.length; i++) {
    const token = significantTokens[i];
    if (token.type === 'keyword' && token.value.toUpperCase() === 'FROM') {
      const nextToken = significantTokens[i + 1];
      if (nextToken && nextToken.type === 'identifier') {
        return nextToken.value.toLowerCase();
      }
      // Check for subquery
      if (nextToken && nextToken.type === 'operator' && nextToken.value === '(') {
        return null; // Subquery not supported
      }
    }
  }
  return null;
}

/**
 * Extract table name from CREATE TABLE statement tokens
 * @internal FOR TESTING ONLY
 */
export function extractTableFromCreateTable(tokens: SqlToken[]): string | null {
  const significantTokens = tokens.filter(t => t.type !== 'whitespace' && t.type !== 'comment');

  for (let i = 0; i < significantTokens.length - 1; i++) {
    const token = significantTokens[i];
    if (token.type === 'keyword' && token.value.toUpperCase() === 'TABLE') {
      const nextToken = significantTokens[i + 1];
      if (nextToken && nextToken.type === 'identifier') {
        return nextToken.value.toLowerCase();
      }
    }
  }
  return null;
}

/**
 * Get the first keyword from tokens to determine statement type
 * @internal FOR TESTING ONLY
 */
export function getStatementType(tokens: SqlToken[]): string | null {
  const firstKeyword = tokens.find(t => t.type === 'keyword');
  return firstKeyword?.value.toUpperCase() || null;
}

/**
 * Check if tokens contain a UNION keyword (outside of strings)
 * @internal FOR TESTING ONLY
 */
export function hasUnionKeyword(tokens: SqlToken[]): boolean {
  return tokens.some(t => t.type === 'keyword' && t.value.toUpperCase() === 'UNION');
}

/**
 * Check if query has comma in FROM clause (implicit join)
 * @internal FOR TESTING ONLY
 */
export function hasImplicitJoin(tokens: SqlToken[]): boolean {
  const significantTokens = tokens.filter(t => t.type !== 'whitespace' && t.type !== 'comment');

  let inFromClause = false;
  for (const token of significantTokens) {
    if (token.type === 'keyword') {
      const upper = token.value.toUpperCase();
      if (upper === 'FROM') {
        inFromClause = true;
        continue;
      }
      // End of FROM clause
      if (inFromClause && ['WHERE', 'ORDER', 'GROUP', 'HAVING', 'LIMIT', 'OFFSET', 'UNION'].includes(upper)) {
        inFromClause = false;
      }
    }
    if (inFromClause && token.type === 'operator' && token.value === ',') {
      return true;
    }
  }
  return false;
}

// =============================================================================
// Mock Query Executor (FOR TESTING ONLY)
// =============================================================================

/**
 * Mock query executor for testing
 *
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * !!!                         FOR TESTING ONLY                             !!!
 * !!!                                                                       !!!
 * !!! This mock executor uses proper tokenization to parse SQL, but it is   !!!
 * !!! NOT a complete SQL implementation. It exists solely to test the RPC   !!!
 * !!! layer without depending on a real database.                           !!!
 * !!!                                                                       !!!
 * !!! DO NOT use this in production!                                        !!!
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 *
 * Provides a simple in-memory implementation for testing the RPC layer
 * without a real database.
 */
export class MockQueryExecutor implements QueryExecutor {
  #lsn = 0n;
  #tables: Map<string, { columns: string[]; columnTypes: ColumnType[]; rows: unknown[][] }> = new Map();
  #transactions: Map<string, { active: boolean }> = new Map();

  async execute(
    sql: string,
    _params?: unknown[],
    options?: ExecuteOptions
  ): Promise<ExecuteResult> {
    this.#lsn++;

    // =========================================================================
    // FOR TESTING ONLY: Proper SQL tokenization to avoid injection issues
    // =========================================================================

    // Tokenize the SQL to properly handle strings, comments, etc.
    const tokens = tokenizeSql(sql);

    // Reject multi-statement queries (injection prevention)
    if (hasMultipleStatements(tokens)) {
      throw new Error('Multi-statement queries are not supported');
    }

    // Reject UNION queries (common injection vector)
    if (hasUnionKeyword(tokens)) {
      throw new Error('UNION queries are not supported in mock executor');
    }

    // Check for implicit JOINs (comma in FROM clause)
    if (hasImplicitJoin(tokens)) {
      throw new Error('Implicit joins (comma syntax) are not supported');
    }

    const statementType = getStatementType(tokens);

    if (statementType === 'SELECT') {
      // Check for subqueries in FROM
      const fromIndex = tokens.findIndex(t => t.type === 'keyword' && t.value.toUpperCase() === 'FROM');
      if (fromIndex >= 0) {
        const afterFrom = tokens.slice(fromIndex + 1).filter(t => t.type !== 'whitespace');
        if (afterFrom[0]?.type === 'operator' && afterFrom[0]?.value === '(') {
          throw new Error('subqueries not supported');
        }
      }

      const tableName = extractTableFromSelect(tokens);

      if (tableName && this.#tables.has(tableName)) {
        const table = this.#tables.get(tableName)!;
        let rows = table.rows;

        // Apply limit/offset
        if (options?.offset !== undefined) {
          rows = rows.slice(options.offset);
        }
        if (options?.limit !== undefined) {
          rows = rows.slice(0, options.limit);
        }

        return {
          columns: table.columns,
          columnTypes: table.columnTypes,
          rows,
          rowCount: rows.length,
          lsn: this.#lsn,
        };
      }

      // Return empty result for unknown tables
      return {
        columns: [],
        columnTypes: [],
        rows: [],
        rowCount: 0,
        lsn: this.#lsn,
      };
    }

    if (statementType === 'INSERT') {
      // Mock insert
      return {
        columns: [],
        columnTypes: [],
        rows: [],
        rowCount: 1,
        lsn: this.#lsn,
      };
    }

    if (statementType === 'CREATE') {
      // Mock table creation
      const tableName = extractTableFromCreateTable(tokens);
      if (tableName) {
        this.#tables.set(tableName, {
          columns: ['id'],
          columnTypes: ['number'],
          rows: [],
        });
      }
      return {
        columns: [],
        columnTypes: [],
        rows: [],
        rowCount: 0,
        lsn: this.#lsn,
      };
    }

    // For unsupported statements, throw a consistent error
    throw new Error(`Unsupported SQL: ${sql}`);
  }

  getCurrentLSN(): bigint {
    return this.#lsn;
  }

  async getSchema(tables?: string[]): Promise<TableSchema[]> {
    const result: TableSchema[] = [];

    for (const [name, data] of this.#tables) {
      if (tables && !tables.includes(name)) continue;

      result.push({
        name,
        columns: data.columns.map((col, i) => ({
          name: col,
          type: data.columnTypes[i],
          nullable: true,
        })),
        primaryKey: ['id'],
      });
    }

    return result;
  }

  async beginTransaction(_options?: TransactionOptions): Promise<string> {
    const txId = `tx_${Date.now()}_${Math.random().toString(36).slice(2)}`;
    this.#transactions.set(txId, { active: true });
    return txId;
  }

  async commit(txId: string): Promise<void> {
    const tx = this.#transactions.get(txId);
    if (!tx) throw new Error(`Transaction ${txId} not found`);
    if (!tx.active) throw new Error(`Transaction ${txId} already completed`);
    tx.active = false;
  }

  async rollback(txId: string, _savepoint?: string): Promise<void> {
    const tx = this.#transactions.get(txId);
    if (!tx) throw new Error(`Transaction ${txId} not found`);
    if (!tx.active) throw new Error(`Transaction ${txId} already completed`);
    tx.active = false;
  }

  // Test helper methods
  addTable(name: string, columns: string[], columnTypes: ColumnType[], rows: unknown[][] = []) {
    this.#tables.set(name.toLowerCase(), { columns, columnTypes, rows });
  }
}
