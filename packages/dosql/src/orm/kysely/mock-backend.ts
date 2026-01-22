/**
 * DoSQL Mock Backend for Testing
 *
 * A simple in-memory backend implementation for testing the Kysely integration.
 * This is NOT a mock - it actually parses and executes SQL queries against
 * in-memory tables.
 */

import type { DoSQLBackend } from './types.js';
import type { SqlValue, Row, QueryResult } from '../../engine/types.js';

// =============================================================================
// SIMPLE SQL PARSER
// =============================================================================

interface ParsedSelect {
  type: 'SELECT';
  columns: string[] | '*';
  table: string;
  joins: Array<{
    type: 'INNER' | 'LEFT' | 'RIGHT';
    table: string;
    condition: { left: string; right: string };
  }>;
  where?: { column: string; op: string; value: SqlValue };
  orderBy?: { column: string; direction: 'ASC' | 'DESC' };
  limit?: number;
  offset?: number;
}

interface ParsedInsert {
  type: 'INSERT';
  table: string;
  columns: string[];
  values: SqlValue[];
}

interface ParsedUpdate {
  type: 'UPDATE';
  table: string;
  set: Record<string, SqlValue>;
  where?: { column: string; op: string; value: SqlValue };
}

interface ParsedDelete {
  type: 'DELETE';
  table: string;
  where?: { column: string; op: string; value: SqlValue };
}

type ParsedQuery = ParsedSelect | ParsedInsert | ParsedUpdate | ParsedDelete;

/**
 * Simple SQL parser for basic queries.
 * Supports SELECT, INSERT, UPDATE, DELETE with basic WHERE clauses.
 */
function parseSQL(sql: string, parameters: SqlValue[] = []): ParsedQuery {
  const normalizedSql = sql.trim().replace(/\s+/g, ' ');

  // Handle SELECT
  if (/^SELECT/i.test(normalizedSql)) {
    return parseSelect(normalizedSql, parameters);
  }

  // Handle INSERT
  if (/^INSERT/i.test(normalizedSql)) {
    return parseInsert(normalizedSql, parameters);
  }

  // Handle UPDATE
  if (/^UPDATE/i.test(normalizedSql)) {
    return parseUpdate(normalizedSql, parameters);
  }

  // Handle DELETE
  if (/^DELETE/i.test(normalizedSql)) {
    return parseDelete(normalizedSql, parameters);
  }

  throw new Error(`Unsupported SQL: ${sql}`);
}

/**
 * Remove quotes from an identifier (handles both " and `)
 */
function unquoteIdentifier(s: string): string {
  return s.replace(/^["'`]|["'`]$/g, '').trim();
}

/**
 * Parse a column reference like "table"."column" or just "column"
 */
function parseColumnRef(s: string): { table?: string; column: string } {
  const cleaned = s.trim();
  // Match "table"."column" or table.column
  const dotMatch = cleaned.match(/^(?:["'`]?(\w+)["'`]?\.)?["'`]?(\w+)["'`]?$/);
  if (dotMatch) {
    return {
      table: dotMatch[1] || undefined,
      column: dotMatch[2],
    };
  }
  return { column: unquoteIdentifier(cleaned) };
}

function parseSelect(sql: string, parameters: SqlValue[]): ParsedSelect {
  const result: ParsedSelect = {
    type: 'SELECT',
    columns: '*',
    table: '',
    joins: [],
  };

  // Extract columns - handle quoted identifiers
  const columnsMatch = sql.match(/select\s+(.*?)\s+from/i);
  if (columnsMatch) {
    const colsStr = columnsMatch[1].trim();
    if (colsStr === '*') {
      result.columns = '*';
    } else {
      // Split on commas but not inside quotes
      const columns: string[] = [];
      let current = '';
      let inQuote = false;
      let quoteChar = '';

      for (let i = 0; i < colsStr.length; i++) {
        const char = colsStr[i];
        if ((char === '"' || char === "'") && !inQuote) {
          inQuote = true;
          quoteChar = char;
          current += char;
        } else if (char === quoteChar && inQuote) {
          inQuote = false;
          current += char;
        } else if (char === ',' && !inQuote) {
          columns.push(current.trim());
          current = '';
        } else {
          current += char;
        }
      }
      if (current.trim()) {
        columns.push(current.trim());
      }

      result.columns = columns.map((c) => {
        const col = c.trim();
        // Handle "column" as "alias" or column as alias
        const asMatch = col.match(/^(.+?)\s+as\s+["'`]?(\w+)["'`]?$/i);
        if (asMatch) {
          return unquoteIdentifier(asMatch[2]); // Return alias
        }
        // Handle "table"."column" or "column"
        const parsed = parseColumnRef(col);
        return parsed.column;
      });
    }
  }

  // Extract table - handle quoted identifiers
  const tableMatch = sql.match(/from\s+["'`]?(\w+)["'`]?/i);
  if (tableMatch) {
    result.table = tableMatch[1];
  }

  // Extract JOINs - handle quoted identifiers
  const joinRegex = /(inner|left|right)?\s*join\s+["'`]?(\w+)["'`]?\s+on\s+["'`]?(\w+)["'`]?\.["'`]?(\w+)["'`]?\s*=\s*["'`]?(\w+)["'`]?\.["'`]?(\w+)["'`]?/gi;
  let joinMatch;
  while ((joinMatch = joinRegex.exec(sql)) !== null) {
    result.joins.push({
      type: (joinMatch[1]?.toUpperCase() || 'INNER') as 'INNER' | 'LEFT' | 'RIGHT',
      table: joinMatch[2],
      condition: {
        left: `${joinMatch[3]}.${joinMatch[4]}`,
        right: `${joinMatch[5]}.${joinMatch[6]}`,
      },
    });
  }

  // Extract WHERE - handle quoted identifiers
  const whereMatch = sql.match(/where\s+(?:["'`]?(\w+)["'`]?\.)?["'`]?(\w+)["'`]?\s*(=|<>|!=|<|>|<=|>=|like)\s*(\?|\$\d+|'[^']*'|\d+)/i);
  if (whereMatch) {
    let value: SqlValue = whereMatch[4];
    if (value === '?' || /^\$\d+$/.test(String(value))) {
      const paramIndex = value === '?' ? 0 : parseInt(String(value).slice(1)) - 1;
      value = parameters[paramIndex];
    } else if (typeof value === 'string' && value.startsWith("'")) {
      value = value.slice(1, -1);
    } else if (/^\d+$/.test(String(value))) {
      value = parseInt(String(value), 10);
    }
    result.where = {
      column: whereMatch[2], // Just the column name without table prefix
      op: whereMatch[3].toUpperCase(),
      value,
    };
  }

  // Extract ORDER BY - handle quoted identifiers
  const orderMatch = sql.match(/order\s+by\s+(?:["'`]?\w+["'`]?\.)?["'`]?(\w+)["'`]?\s*(asc|desc)?/i);
  if (orderMatch) {
    result.orderBy = {
      column: orderMatch[1],
      direction: (orderMatch[2]?.toUpperCase() || 'ASC') as 'ASC' | 'DESC',
    };
  }

  // Extract LIMIT - Kysely generates "limit ?" with parameter
  const limitMatch = sql.match(/limit\s+(\d+|\?)/i);
  if (limitMatch) {
    if (limitMatch[1] === '?') {
      // Find the limit parameter (it's after WHERE params if any)
      const whereParamCount = (sql.match(/where.*?\?/i) ? 1 : 0);
      const limitParam = parameters[whereParamCount];
      result.limit = typeof limitParam === 'number' ? limitParam : parseInt(String(limitParam), 10);
    } else {
      result.limit = parseInt(limitMatch[1], 10);
    }
  }

  // Extract OFFSET - Kysely generates "offset ?" with parameter
  const offsetMatch = sql.match(/offset\s+(\d+|\?)/i);
  if (offsetMatch) {
    if (offsetMatch[1] === '?') {
      // Find the offset parameter (it's after WHERE and LIMIT params)
      const whereParamCount = (sql.match(/where.*?\?/i) ? 1 : 0);
      const limitParamCount = (sql.match(/limit\s+\?/i) ? 1 : 0);
      const offsetParam = parameters[whereParamCount + limitParamCount];
      result.offset = typeof offsetParam === 'number' ? offsetParam : parseInt(String(offsetParam), 10);
    } else {
      result.offset = parseInt(offsetMatch[1], 10);
    }
  }

  return result;
}

function parseInsert(sql: string, parameters: SqlValue[]): ParsedInsert {
  const result: ParsedInsert = {
    type: 'INSERT',
    table: '',
    columns: [],
    values: [],
  };

  // Extract table
  const tableMatch = sql.match(/INSERT\s+INTO\s+["`]?([\w]+)["`]?/i);
  if (tableMatch) {
    result.table = tableMatch[1];
  }

  // Extract columns
  const columnsMatch = sql.match(/\(([^)]+)\)\s+VALUES/i);
  if (columnsMatch) {
    result.columns = columnsMatch[1].split(',').map((c) => c.trim().replace(/["`]/g, ''));
  }

  // Extract values
  const valuesMatch = sql.match(/VALUES\s*\(([^)]+)\)/i);
  if (valuesMatch) {
    const valuesStr = valuesMatch[1];
    const valueParts = valuesStr.split(',');
    let paramIndex = 0;
    result.values = valueParts.map((v) => {
      const trimmed = v.trim();
      if (trimmed === '?' || /^\$\d+$/.test(trimmed)) {
        const idx = trimmed === '?' ? paramIndex++ : parseInt(trimmed.slice(1)) - 1;
        return parameters[idx];
      }
      if (trimmed.startsWith("'") || trimmed.startsWith('"')) {
        return trimmed.slice(1, -1);
      }
      if (/^\d+$/.test(trimmed)) {
        return parseInt(trimmed, 10);
      }
      if (trimmed.toLowerCase() === 'null') {
        return null;
      }
      if (trimmed.toLowerCase() === 'true') {
        return true;
      }
      if (trimmed.toLowerCase() === 'false') {
        return false;
      }
      return trimmed;
    });
  }

  return result;
}

function parseUpdate(sql: string, parameters: SqlValue[]): ParsedUpdate {
  const result: ParsedUpdate = {
    type: 'UPDATE',
    table: '',
    set: {},
  };

  // Extract table
  const tableMatch = sql.match(/UPDATE\s+["`]?([\w]+)["`]?/i);
  if (tableMatch) {
    result.table = tableMatch[1];
  }

  // Extract SET clause
  const setMatch = sql.match(/SET\s+(.+?)(?:\s+WHERE|$)/i);
  if (setMatch) {
    const setParts = setMatch[1].split(',');
    let paramIndex = 0;
    for (const part of setParts) {
      const [col, val] = part.split('=').map((s) => s.trim());
      const colName = col.replace(/["`]/g, '');
      let value: SqlValue = val;
      if (val === '?' || /^\$\d+$/.test(val)) {
        const idx = val === '?' ? paramIndex++ : parseInt(val.slice(1)) - 1;
        value = parameters[idx];
      } else if (val.startsWith("'") || val.startsWith('"')) {
        value = val.slice(1, -1);
      } else if (/^\d+$/.test(val)) {
        value = parseInt(val, 10);
      }
      result.set[colName] = value;
    }
  }

  // Extract WHERE
  const whereMatch = sql.match(/WHERE\s+["`]?([\w]+)["`]?\s*(=|<>|!=)\s*(\?|\$\d+|'[^']*'|"[^"]*"|\d+)/i);
  if (whereMatch) {
    let value: SqlValue = whereMatch[3];
    if (value === '?' || /^\$\d+$/.test(String(value))) {
      const paramIndex = value === '?' ? Object.keys(result.set).length : parseInt(String(value).slice(1)) - 1;
      value = parameters[paramIndex];
    } else if (typeof value === 'string' && (value.startsWith("'") || value.startsWith('"'))) {
      value = value.slice(1, -1);
    } else if (/^\d+$/.test(String(value))) {
      value = parseInt(String(value), 10);
    }
    result.where = {
      column: whereMatch[1],
      op: whereMatch[2].toUpperCase(),
      value,
    };
  }

  return result;
}

function parseDelete(sql: string, parameters: SqlValue[]): ParsedDelete {
  const result: ParsedDelete = {
    type: 'DELETE',
    table: '',
  };

  // Extract table
  const tableMatch = sql.match(/DELETE\s+FROM\s+["`]?([\w]+)["`]?/i);
  if (tableMatch) {
    result.table = tableMatch[1];
  }

  // Extract WHERE
  const whereMatch = sql.match(/WHERE\s+["`]?([\w]+)["`]?\s*(=|<>|!=)\s*(\?|\$\d+|'[^']*'|"[^"]*"|\d+)/i);
  if (whereMatch) {
    let value: SqlValue = whereMatch[3];
    if (value === '?' || /^\$\d+$/.test(String(value))) {
      const paramIndex = value === '?' ? 0 : parseInt(String(value).slice(1)) - 1;
      value = parameters[paramIndex];
    } else if (typeof value === 'string' && (value.startsWith("'") || value.startsWith('"'))) {
      value = value.slice(1, -1);
    } else if (/^\d+$/.test(String(value))) {
      value = parseInt(String(value), 10);
    }
    result.where = {
      column: whereMatch[1],
      op: whereMatch[2].toUpperCase(),
      value,
    };
  }

  return result;
}

// =============================================================================
// IN-MEMORY STORAGE
// =============================================================================

interface TableData {
  rows: Row[];
  autoIncrement: number;
}

/**
 * Simple in-memory storage for testing.
 */
export class InMemoryStorage {
  private tables = new Map<string, TableData>();

  /**
   * Create a table.
   */
  createTable(name: string): void {
    if (!this.tables.has(name)) {
      this.tables.set(name, { rows: [], autoIncrement: 1 });
    }
  }

  /**
   * Get table data.
   */
  getTable(name: string): TableData {
    let table = this.tables.get(name);
    if (!table) {
      table = { rows: [], autoIncrement: 1 };
      this.tables.set(name, table);
    }
    return table;
  }

  /**
   * Clear all tables.
   */
  clear(): void {
    this.tables.clear();
  }

  /**
   * Seed data for testing.
   */
  seed(tableName: string, rows: Row[]): void {
    const table = this.getTable(tableName);
    table.rows = [...rows];
    // Update auto-increment based on max id
    const maxId = rows.reduce((max, row) => {
      const id = typeof row.id === 'number' ? row.id : 0;
      return Math.max(max, id);
    }, 0);
    table.autoIncrement = maxId + 1;
  }
}

// =============================================================================
// MOCK BACKEND
// =============================================================================

/**
 * In-memory DoSQL backend for testing.
 * Actually executes queries against in-memory tables.
 */
export class MockDoSQLBackend implements DoSQLBackend {
  private storage = new InMemoryStorage();
  private inTransaction = false;
  private transactionSnapshot: Map<string, Row[]> | null = null;

  /**
   * Execute a SQL query.
   */
  async execute<T = Row>(sql: string, parameters: SqlValue[] = []): Promise<QueryResult<T>> {
    const startTime = performance.now();

    // Handle transaction control
    if (/^\s*BEGIN/i.test(sql)) {
      await this.beginTransaction();
      return { rows: [] as T[], rowsAffected: 0 };
    }
    if (/^\s*COMMIT/i.test(sql)) {
      await this.commit();
      return { rows: [] as T[], rowsAffected: 0 };
    }
    if (/^\s*ROLLBACK/i.test(sql)) {
      await this.rollback();
      return { rows: [] as T[], rowsAffected: 0 };
    }

    const parsed = parseSQL(sql, parameters);
    let rows: T[] = [];
    let rowsAffected = 0;

    switch (parsed.type) {
      case 'SELECT':
        rows = this.executeSelect(parsed) as T[];
        break;
      case 'INSERT':
        rows = this.executeInsert(parsed) as T[];
        rowsAffected = rows.length;
        break;
      case 'UPDATE':
        rowsAffected = this.executeUpdate(parsed);
        break;
      case 'DELETE':
        rowsAffected = this.executeDelete(parsed);
        break;
    }

    const endTime = performance.now();

    return {
      rows,
      rowsAffected,
      stats: {
        planningTime: 0,
        executionTime: endTime - startTime,
        rowsScanned: rows.length,
        rowsReturned: rows.length,
      },
    };
  }

  /**
   * Execute a query and return rows only.
   */
  async query<T = Row>(sql: string, parameters: SqlValue[] = []): Promise<T[]> {
    const result = await this.execute<T>(sql, parameters);
    return result.rows;
  }

  /**
   * Execute a query and return a single row.
   */
  async queryOne<T = Row>(sql: string, parameters: SqlValue[] = []): Promise<T | null> {
    const rows = await this.query<T>(sql, parameters);
    return rows[0] || null;
  }

  /**
   * Begin a transaction.
   */
  async beginTransaction(): Promise<void> {
    if (this.inTransaction) {
      throw new Error('Transaction already in progress');
    }
    // Snapshot current state
    this.transactionSnapshot = new Map();
    for (const [name, table] of (this.storage as any).tables) {
      this.transactionSnapshot.set(name, [...table.rows]);
    }
    this.inTransaction = true;
  }

  /**
   * Commit the transaction.
   */
  async commit(): Promise<void> {
    if (!this.inTransaction) {
      throw new Error('No transaction in progress');
    }
    this.transactionSnapshot = null;
    this.inTransaction = false;
  }

  /**
   * Rollback the transaction.
   */
  async rollback(): Promise<void> {
    if (!this.inTransaction) {
      throw new Error('No transaction in progress');
    }
    // Restore snapshot
    if (this.transactionSnapshot) {
      for (const [name, rows] of this.transactionSnapshot) {
        const table = this.storage.getTable(name);
        table.rows = rows;
      }
    }
    this.transactionSnapshot = null;
    this.inTransaction = false;
  }

  /**
   * Close the backend.
   */
  async close(): Promise<void> {
    this.storage.clear();
  }

  /**
   * Seed data for testing.
   */
  seed(tableName: string, rows: Row[]): void {
    this.storage.seed(tableName, rows);
  }

  /**
   * Clear all data.
   */
  clear(): void {
    this.storage.clear();
  }

  // ==========================================================================
  // PRIVATE EXECUTION METHODS
  // ==========================================================================

  private executeSelect(parsed: ParsedSelect): Row[] {
    let rows = [...this.storage.getTable(parsed.table).rows];

    // Handle JOINs
    for (const join of parsed.joins) {
      const joinTableName = join.table;
      const mainTableName = parsed.table;
      const joinTableRows = this.storage.getTable(joinTableName).rows;
      const newRows: Row[] = [];

      // Parse the join condition to determine which column belongs to which table
      // Format: "table1.column1" = "table2.column2"
      const leftParts = join.condition.left.split('.');
      const rightParts = join.condition.right.split('.');

      const leftTable = leftParts.length > 1 ? leftParts[0] : null;
      const leftCol = leftParts[leftParts.length - 1];
      const rightTable = rightParts.length > 1 ? rightParts[0] : null;
      const rightCol = rightParts[rightParts.length - 1];

      for (const mainRow of rows) {
        for (const joinRow of joinTableRows) {
          // Determine which value to use based on the table names
          let leftVal: unknown;
          let rightVal: unknown;

          // Left side of condition
          if (leftTable === joinTableName) {
            leftVal = joinRow[leftCol];
          } else {
            leftVal = mainRow[leftCol];
          }

          // Right side of condition
          if (rightTable === joinTableName) {
            rightVal = joinRow[rightCol];
          } else {
            rightVal = mainRow[rightCol];
          }

          if (leftVal === rightVal) {
            // Merge rows, keeping main table values for conflicts (except we want to preserve all)
            newRows.push({ ...mainRow, ...joinRow });
          }
        }
      }

      if (join.type === 'INNER') {
        rows = newRows;
      } else if (join.type === 'LEFT') {
        // Keep original rows with null for unmatched
        rows = newRows.length > 0 ? newRows : rows;
      }
    }

    // Apply WHERE filter
    if (parsed.where) {
      rows = rows.filter((row) => {
        const col = parsed.where!.column;
        const val = row[col];
        let target = parsed.where!.value;
        const op = parsed.where!.op;

        // Handle boolean coercion (SQLite uses 0/1 for booleans)
        // Kysely sends boolean values as 1/0
        let normalizedVal = val;
        let normalizedTarget = target;

        // Normalize booleans to numbers for comparison
        if (typeof val === 'boolean') {
          normalizedVal = val ? 1 : 0;
        }
        if (typeof target === 'boolean') {
          normalizedTarget = target ? 1 : 0;
        }
        // Also handle the reverse - if one is a number 0/1 and the other is boolean
        if (typeof normalizedVal === 'number' && (normalizedVal === 0 || normalizedVal === 1)) {
          if (typeof normalizedTarget === 'number' && (normalizedTarget === 0 || normalizedTarget === 1)) {
            // Both are 0/1 numbers, compare as numbers
          }
        }

        // For boolean comparisons, also check truthiness equivalence
        const isBooleanComparison =
          (typeof val === 'boolean' || val === 0 || val === 1) &&
          (typeof target === 'boolean' || target === 0 || target === 1);

        let result: boolean;
        switch (op) {
          case '=':
            if (isBooleanComparison) {
              // Compare as booleans
              const boolVal = val === true || val === 1;
              const boolTarget = target === true || target === 1;
              result = boolVal === boolTarget;
            } else {
              result = val === target;
            }
            break;
          case '<>':
          case '!=':
            if (isBooleanComparison) {
              const boolVal = val === true || val === 1;
              const boolTarget = target === true || target === 1;
              result = boolVal !== boolTarget;
            } else {
              result = val !== target;
            }
            break;
          case '<':
            result = (val as number) < (target as number);
            break;
          case '>':
            result = (val as number) > (target as number);
            break;
          case '<=':
            result = (val as number) <= (target as number);
            break;
          case '>=':
            result = (val as number) >= (target as number);
            break;
          case 'LIKE':
            // Simple LIKE implementation
            const pattern = String(target).replace(/%/g, '.*').replace(/_/g, '.');
            result = new RegExp(`^${pattern}$`, 'i').test(String(val));
            break;
          default:
            result = true;
        }

        return result;
      });
    }

    // Apply ORDER BY
    if (parsed.orderBy) {
      const col = parsed.orderBy.column;
      const dir = parsed.orderBy.direction;
      rows.sort((a, b) => {
        const aVal = a[col];
        const bVal = b[col];
        if (aVal === bVal) return 0;
        if (aVal === null) return 1;
        if (bVal === null) return -1;
        const cmp = aVal < bVal ? -1 : 1;
        return dir === 'DESC' ? -cmp : cmp;
      });
    }

    // Apply OFFSET
    if (parsed.offset) {
      rows = rows.slice(parsed.offset);
    }

    // Apply LIMIT
    if (parsed.limit) {
      rows = rows.slice(0, parsed.limit);
    }

    // Apply column projection
    if (parsed.columns !== '*') {
      rows = rows.map((row) => {
        const projected: Row = {};
        for (const col of parsed.columns as string[]) {
          if (col in row) {
            projected[col] = row[col];
          }
        }
        return projected;
      });
    }

    return rows;
  }

  private executeInsert(parsed: ParsedInsert): Row[] {
    const table = this.storage.getTable(parsed.table);
    const row: Row = {};

    for (let i = 0; i < parsed.columns.length; i++) {
      row[parsed.columns[i]] = parsed.values[i];
    }

    // Auto-generate ID if not provided
    if (!('id' in row)) {
      row.id = table.autoIncrement++;
    } else if (typeof row.id === 'number' && row.id >= table.autoIncrement) {
      table.autoIncrement = row.id + 1;
    }

    table.rows.push(row);
    return [row];
  }

  private executeUpdate(parsed: ParsedUpdate): number {
    const table = this.storage.getTable(parsed.table);
    let affected = 0;

    for (const row of table.rows) {
      if (parsed.where) {
        const col = parsed.where.column;
        const val = row[col];
        const target = parsed.where.value;

        if (parsed.where.op === '=' && val !== target) continue;
        if ((parsed.where.op === '<>' || parsed.where.op === '!=') && val === target) continue;
      }

      // Apply updates
      for (const [key, value] of Object.entries(parsed.set)) {
        row[key] = value;
      }
      affected++;
    }

    return affected;
  }

  private executeDelete(parsed: ParsedDelete): number {
    const table = this.storage.getTable(parsed.table);
    const initialLength = table.rows.length;

    if (parsed.where) {
      table.rows = table.rows.filter((row) => {
        const col = parsed.where!.column;
        const val = row[col];
        const target = parsed.where!.value;

        if (parsed.where!.op === '=') return val !== target;
        if (parsed.where!.op === '<>' || parsed.where!.op === '!=') return val === target;
        return true;
      });
    } else {
      table.rows = [];
    }

    return initialLength - table.rows.length;
  }
}

/**
 * Create a mock DoSQL backend for testing.
 */
export function createMockBackend(): MockDoSQLBackend {
  return new MockDoSQLBackend();
}
