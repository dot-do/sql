/**
 * Prepared Statement Implementation for DoSQL
 *
 * Provides a better-sqlite3/D1 compatible prepared statement API.
 */

import type {
  Statement,
  SqlValue,
  BindParameters,
  RunResult,
  ColumnInfo,
  StatementOptions,
} from './types.js';
import {
  parseParameters,
  bindParameters,
  validateParameters,
  type ParsedParameters,
} from './binding.js';
import {
  StatementError,
  StatementErrorCode,
  createFinalizedStatementError,
  createTableNotFoundError,
  createUnsupportedSqlError,
} from '../errors/index.js';

// Re-export StatementError for backwards compatibility
export { StatementError, StatementErrorCode } from '../errors/index.js';

// =============================================================================
// EXECUTION CONTEXT
// =============================================================================

/**
 * Interface for the underlying SQL execution engine
 */
export interface ExecutionEngine {
  /**
   * Execute a SQL statement and return results
   */
  execute(sql: string, params: SqlValue[]): ExecutionResult;

  /**
   * Execute a SQL statement for write operations
   */
  run(sql: string, params: SqlValue[]): RunResult;

  /**
   * Get column metadata for a query
   */
  getColumns(sql: string): ColumnInfo[];
}

/**
 * Result from query execution
 */
export interface ExecutionResult {
  /**
   * Array of result rows
   */
  rows: Record<string, SqlValue>[];

  /**
   * Column metadata
   */
  columns: ColumnInfo[];

  /**
   * For INSERT/UPDATE/DELETE
   */
  changes?: number;

  /**
   * For INSERT
   */
  lastInsertRowid?: number | bigint;
}

// =============================================================================
// PREPARED STATEMENT IMPLEMENTATION
// =============================================================================

/**
 * Prepared statement implementation
 *
 * @template T - Row type for SELECT queries
 * @template P - Parameter types
 */
export class PreparedStatement<T = unknown, P extends BindParameters = BindParameters>
  implements Statement<T, P>
{
  /** The original SQL string */
  readonly source: string;

  /** Whether this is a read-only statement */
  readonly reader: boolean;

  /** Parsed parameter information */
  private readonly parsed: ParsedParameters;

  /** Execution engine reference */
  private readonly engine: ExecutionEngine;

  /** Pre-bound parameters */
  private boundParams: SqlValue[] | null = null;

  /** Statement options */
  private options: StatementOptions = {};

  /** Cached column metadata */
  private cachedColumns: ColumnInfo[] | null = null;

  /** Whether the statement has been finalized */
  private _finalized = false;

  constructor(sql: string, engine: ExecutionEngine) {
    this.source = sql;
    this.engine = engine;
    this.parsed = parseParameters(sql);
    this.reader = this.isReadStatement(sql);
  }

  /**
   * Check if statement has been finalized
   */
  get finalized(): boolean {
    return this._finalized;
  }

  /**
   * Determine if SQL is a read statement
   */
  private isReadStatement(sql: string): boolean {
    const normalized = sql.trim().toUpperCase();
    return (
      normalized.startsWith('SELECT') ||
      normalized.startsWith('PRAGMA') ||
      normalized.startsWith('EXPLAIN') ||
      normalized.startsWith('WITH')
    );
  }

  /**
   * Check if statement can be used
   */
  private checkUsable(): void {
    if (this._finalized) {
      throw createFinalizedStatementError(this.source);
    }
  }

  /**
   * Get parameters to use for execution
   */
  private getParams(...params: unknown[]): SqlValue[] {
    // If no params provided and we have bound params, use those
    if (params.length === 0 && this.boundParams !== null) {
      return this.boundParams;
    }

    // Bind the provided parameters
    if (params.length > 0) {
      validateParameters(this.parsed, ...params);
      return bindParameters(this.parsed, ...params);
    }

    // No parameters needed
    if (this.parsed.tokens.length === 0) {
      return [];
    }

    throw new StatementError(
      StatementErrorCode.EXECUTION_ERROR,
      `Expected ${this.parsed.tokens.length} parameters, got 0`,
      this.source
    );
  }

  /**
   * Transform result rows based on options
   */
  private transformRow(row: Record<string, SqlValue>): T {
    if (this.options.raw) {
      return Object.values(row) as unknown as T;
    }

    if (this.options.pluck) {
      const keys = Object.keys(row);
      return row[keys[0]] as unknown as T;
    }

    if (this.options.expand) {
      // Expand table.column into nested objects
      const expanded: Record<string, Record<string, SqlValue>> = {};
      for (const [key, value] of Object.entries(row)) {
        const parts = key.split('.');
        if (parts.length === 2) {
          const [table, column] = parts;
          if (!expanded[table]) {
            expanded[table] = {};
          }
          expanded[table][column] = value;
        } else {
          if (!expanded['']) {
            expanded[''] = {};
          }
          expanded[''][key] = value;
        }
      }
      // Flatten single-table results
      const tables = Object.keys(expanded);
      if (tables.length === 1 && tables[0] === '') {
        return expanded[''] as unknown as T;
      }
      return expanded as unknown as T;
    }

    // Handle safe integers
    if (this.options.safeIntegers) {
      for (const [key, value] of Object.entries(row)) {
        if (typeof value === 'number' && Number.isInteger(value)) {
          row[key] = BigInt(value);
        }
      }
    }

    return row as unknown as T;
  }

  // ==========================================================================
  // PUBLIC API
  // ==========================================================================

  /**
   * Bind parameters to the statement
   */
  bind(...params: P extends any[] ? P : [P]): this {
    this.checkUsable();
    validateParameters(this.parsed, ...params);
    this.boundParams = bindParameters(this.parsed, ...params);
    return this;
  }

  /**
   * Execute the statement and return run result
   */
  run(...params: P extends any[] ? P : [P]): RunResult {
    this.checkUsable();
    const values = this.getParams(...params);
    return this.engine.run(this.parsed.normalizedSql, values);
  }

  /**
   * Execute and return the first row
   */
  get(...params: P extends any[] ? P : [P]): T | undefined {
    this.checkUsable();
    const values = this.getParams(...params);
    const result = this.engine.execute(this.parsed.normalizedSql, values);

    if (result.rows.length === 0) {
      return undefined;
    }

    return this.transformRow(result.rows[0]);
  }

  /**
   * Execute and return all rows
   */
  all(...params: P extends any[] ? P : [P]): T[] {
    this.checkUsable();
    const values = this.getParams(...params);
    const result = this.engine.execute(this.parsed.normalizedSql, values);
    return result.rows.map(row => this.transformRow(row));
  }

  /**
   * Execute and return an iterator
   */
  *iterate(...params: P extends any[] ? P : [P]): IterableIterator<T> {
    this.checkUsable();
    const values = this.getParams(...params);
    const result = this.engine.execute(this.parsed.normalizedSql, values);

    for (const row of result.rows) {
      yield this.transformRow(row);
    }
  }

  /**
   * Get column metadata
   */
  columns(): ColumnInfo[] {
    this.checkUsable();

    if (this.cachedColumns) {
      return this.cachedColumns;
    }

    this.cachedColumns = this.engine.getColumns(this.source);
    return this.cachedColumns;
  }

  /**
   * Release resources
   */
  finalize(): void {
    if (this._finalized) {
      return;
    }

    this._finalized = true;
    this.boundParams = null;
    this.cachedColumns = null;
  }

  /**
   * Configure safeIntegers mode
   */
  safeIntegers(enabled = true): this {
    this.options.safeIntegers = enabled;
    return this;
  }

  /**
   * Configure expand mode
   */
  expand(enabled = true): this {
    this.options.expand = enabled;
    return this;
  }

  /**
   * Configure pluck mode
   */
  pluck(enabled = true): this {
    this.options.pluck = enabled;
    return this;
  }

  /**
   * Configure raw mode
   */
  raw(enabled = true): this {
    this.options.raw = enabled;
    return this;
  }
}

// =============================================================================
// IN-MEMORY ENGINE (for testing)
// =============================================================================

/**
 * Simple in-memory storage for testing
 */
export interface InMemoryTable {
  name: string;
  columns: ColumnInfo[];
  rows: Record<string, SqlValue>[];
  autoIncrement: number;
}

/**
 * In-memory database storage
 */
export interface InMemoryStorage {
  tables: Map<string, InMemoryTable>;
  lastInsertRowid: number | bigint;
}

/**
 * Create an empty in-memory storage
 */
export function createInMemoryStorage(): InMemoryStorage {
  return {
    tables: new Map(),
    lastInsertRowid: 0,
  };
}

/**
 * Simple in-memory execution engine for testing
 */
export class InMemoryEngine implements ExecutionEngine {
  private storage: InMemoryStorage;

  constructor(storage?: InMemoryStorage) {
    this.storage = storage ?? createInMemoryStorage();
  }

  /**
   * Get the underlying storage
   */
  getStorage(): InMemoryStorage {
    return this.storage;
  }

  /**
   * Execute a query
   */
  execute(sql: string, params: SqlValue[]): ExecutionResult {
    const normalized = sql.trim().toUpperCase();

    // Handle CREATE TABLE
    if (normalized.startsWith('CREATE TABLE')) {
      return this.executeCreateTable(sql);
    }

    // Handle INSERT
    if (normalized.startsWith('INSERT')) {
      return this.executeInsert(sql, params);
    }

    // Handle SELECT
    if (normalized.startsWith('SELECT')) {
      return this.executeSelect(sql, params);
    }

    // Handle UPDATE
    if (normalized.startsWith('UPDATE')) {
      return this.executeUpdate(sql, params);
    }

    // Handle DELETE
    if (normalized.startsWith('DELETE')) {
      return this.executeDelete(sql, params);
    }

    throw createUnsupportedSqlError(sql);
  }

  /**
   * Execute a write operation
   */
  run(sql: string, params: SqlValue[]): RunResult {
    const result = this.execute(sql, params);
    return {
      changes: result.changes ?? 0,
      lastInsertRowid: result.lastInsertRowid ?? 0,
    };
  }

  /**
   * Get column metadata
   */
  getColumns(sql: string): ColumnInfo[] {
    // Parse table name from SELECT
    const match = sql.match(/FROM\s+(\w+)/i);
    if (match) {
      const tableName = match[1];
      const table = this.storage.tables.get(tableName);
      if (table) {
        return table.columns;
      }
    }
    return [];
  }

  // ==========================================================================
  // PRIVATE EXECUTION METHODS
  // ==========================================================================

  private executeCreateTable(sql: string): ExecutionResult {
    // Simple CREATE TABLE parsing
    const match = sql.match(
      /CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s*\(([\s\S]+)\)/i
    );

    if (!match) {
      throw new StatementError(StatementErrorCode.INVALID_SQL, 'Invalid CREATE TABLE syntax', sql);
    }

    const tableName = match[1];
    const columnDefs = match[2];

    // Parse columns
    const columns: ColumnInfo[] = [];
    const columnParts = columnDefs.split(',').map(c => c.trim());

    for (const part of columnParts) {
      const colMatch = part.match(/^(\w+)\s+(\w+)/);
      if (colMatch) {
        columns.push({
          name: colMatch[1],
          column: colMatch[1],
          table: tableName,
          database: 'main',
          type: colMatch[2].toUpperCase(),
        });
      }
    }

    const table: InMemoryTable = {
      name: tableName,
      columns,
      rows: [],
      autoIncrement: 1,
    };

    this.storage.tables.set(tableName, table);

    return { rows: [], columns: [] };
  }

  private executeInsert(sql: string, params: SqlValue[]): ExecutionResult {
    // Simple INSERT parsing
    const match = sql.match(
      /INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/i
    );

    if (!match) {
      throw new StatementError(StatementErrorCode.INVALID_SQL, 'Invalid INSERT syntax', sql);
    }

    const tableName = match[1];
    const columnNames = match[2].split(',').map(c => c.trim());
    const valuePlaceholders = match[3].split(',').map(v => v.trim());

    const table = this.storage.tables.get(tableName);
    if (!table) {
      throw createTableNotFoundError(tableName, sql);
    }

    // Build row from params
    const row: Record<string, SqlValue> = {};
    let paramIndex = 0;

    for (let i = 0; i < columnNames.length; i++) {
      const colName = columnNames[i];
      const placeholder = valuePlaceholders[i];

      if (placeholder === '?') {
        row[colName] = params[paramIndex++];
      } else {
        // Literal value - try to parse
        if (placeholder.startsWith("'") && placeholder.endsWith("'")) {
          row[colName] = placeholder.slice(1, -1);
        } else if (!isNaN(Number(placeholder))) {
          row[colName] = Number(placeholder);
        } else if (placeholder.toUpperCase() === 'NULL') {
          row[colName] = null;
        } else {
          row[colName] = placeholder;
        }
      }
    }

    // Handle auto-increment ID
    const idCol = table.columns.find(c => c.type === 'INTEGER' && c.name.toLowerCase() === 'id');
    if (idCol && row[idCol.name] === undefined) {
      row[idCol.name] = table.autoIncrement++;
    }

    table.rows.push(row);
    this.storage.lastInsertRowid = row['id'] ?? table.rows.length;

    return {
      rows: [],
      columns: [],
      changes: 1,
      lastInsertRowid: this.storage.lastInsertRowid,
    };
  }

  private executeSelect(sql: string, params: SqlValue[]): ExecutionResult {
    // Simple SELECT parsing
    const match = sql.match(
      /SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+(.+?))?(?:\s+LIMIT\s+(\d+))?$/i
    );

    if (!match) {
      throw new StatementError(StatementErrorCode.INVALID_SQL, 'Invalid SELECT syntax', sql);
    }

    const columnList = match[1];
    const tableName = match[2];
    const whereClause = match[3];
    const orderBy = match[4];
    const limit = match[5] ? parseInt(match[5], 10) : undefined;

    const table = this.storage.tables.get(tableName);
    if (!table) {
      throw createTableNotFoundError(tableName, sql);
    }

    // Filter rows
    let rows = [...table.rows];

    if (whereClause) {
      const result = this.filterRows(rows, whereClause, params, 0);
      rows = result.filtered;
    }

    // Order rows
    if (orderBy) {
      rows = this.orderRows(rows, orderBy);
    }

    // Limit rows
    if (limit !== undefined) {
      rows = rows.slice(0, limit);
    }

    // Project columns
    const selectAll = columnList.trim() === '*';
    if (!selectAll) {
      const colNames = columnList.split(',').map(c => c.trim());
      rows = rows.map(row => {
        const projected: Record<string, SqlValue> = {};
        for (const col of colNames) {
          // Handle aliases
          const aliasMatch = col.match(/^(.+?)\s+AS\s+(\w+)$/i);
          if (aliasMatch) {
            const [, expr, alias] = aliasMatch;
            projected[alias] = row[expr.trim()] ?? null;
          } else {
            projected[col] = row[col] ?? null;
          }
        }
        return projected;
      });
    }

    return {
      rows,
      columns: selectAll ? table.columns : [],
    };
  }

  private executeUpdate(sql: string, params: SqlValue[]): ExecutionResult {
    // Simple UPDATE parsing
    const match = sql.match(
      /UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$/i
    );

    if (!match) {
      throw new StatementError(StatementErrorCode.INVALID_SQL, 'Invalid UPDATE syntax', sql);
    }

    const tableName = match[1];
    const setClause = match[2];
    const whereClause = match[3];

    const table = this.storage.tables.get(tableName);
    if (!table) {
      throw createTableNotFoundError(tableName, sql);
    }

    // Parse SET clause - handle expressions like "col = col + ?"
    const assignments = setClause.split(',').map(a => {
      const eqIndex = a.indexOf('=');
      if (eqIndex === -1) {
        throw new StatementError(StatementErrorCode.INVALID_SQL, `Invalid SET clause: ${a}`, sql);
      }
      const col = a.slice(0, eqIndex).trim();
      const expr = a.slice(eqIndex + 1).trim();
      return { col, expr };
    });

    // Count parameters used in SET clause
    const setParamCount = assignments.reduce((count, a) => {
      return count + (a.expr.match(/\?/g) || []).length;
    }, 0);

    // Find rows to update (WHERE params come after SET params)
    let rowsToUpdate = table.rows;
    if (whereClause) {
      const result = this.filterRows(table.rows, whereClause, params, setParamCount);
      rowsToUpdate = result.filtered;
    }

    // Track param index for SET values
    let paramIndex = 0;

    // Apply updates
    let changes = 0;
    for (const row of rowsToUpdate) {
      for (const { col, expr } of assignments) {
        // Evaluate expression
        const value = this.evaluateSetExpression(expr, row, params, paramIndex);
        paramIndex += (expr.match(/\?/g) || []).length;
        row[col] = value;
      }
      // Reset param index for next row (but actually we need to track differently)
      paramIndex = 0;
      changes++;
    }

    return {
      rows: [],
      columns: [],
      changes,
    };
  }

  /**
   * Evaluate a SET expression (handles column references and arithmetic)
   */
  private evaluateSetExpression(
    expr: string,
    row: Record<string, SqlValue>,
    params: SqlValue[],
    startParamIndex: number
  ): SqlValue {
    // Simple parameter
    if (expr === '?') {
      return params[startParamIndex];
    }

    // String literal
    if (expr.startsWith("'") && expr.endsWith("'")) {
      return expr.slice(1, -1);
    }

    // NULL
    if (expr.toUpperCase() === 'NULL') {
      return null;
    }

    // Number literal
    if (/^-?\d+(\.\d+)?$/.test(expr)) {
      return Number(expr);
    }

    // Column reference (just column name)
    if (/^\w+$/.test(expr)) {
      return row[expr] ?? null;
    }

    // Arithmetic expression: column +/- value
    const arithMatch = expr.match(/^(\w+)\s*([\+\-\*\/])\s*(.+)$/);
    if (arithMatch) {
      const [, leftCol, op, rightExpr] = arithMatch;
      const leftVal = Number(row[leftCol] ?? 0);
      let rightVal: number;

      if (rightExpr.trim() === '?') {
        rightVal = Number(params[startParamIndex]);
      } else {
        rightVal = Number(rightExpr);
      }

      switch (op) {
        case '+': return leftVal + rightVal;
        case '-': return leftVal - rightVal;
        case '*': return leftVal * rightVal;
        case '/': return rightVal !== 0 ? leftVal / rightVal : null;
        default: return leftVal;
      }
    }

    // Fallback - try as column reference
    return row[expr] ?? null;
  }

  private executeDelete(sql: string, params: SqlValue[]): ExecutionResult {
    // Simple DELETE parsing
    const match = sql.match(
      /DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?$/i
    );

    if (!match) {
      throw new StatementError(StatementErrorCode.INVALID_SQL, 'Invalid DELETE syntax', sql);
    }

    const tableName = match[1];
    const whereClause = match[2];

    const table = this.storage.tables.get(tableName);
    if (!table) {
      throw createTableNotFoundError(tableName, sql);
    }

    // Find rows to delete
    let rowsToDelete: Set<Record<string, SqlValue>>;
    if (whereClause) {
      const result = this.filterRows(table.rows, whereClause, params, 0);
      rowsToDelete = new Set(result.filtered);
    } else {
      rowsToDelete = new Set(table.rows);
    }

    const originalLength = table.rows.length;
    table.rows = table.rows.filter(row => !rowsToDelete.has(row));

    return {
      rows: [],
      columns: [],
      changes: originalLength - table.rows.length,
    };
  }

  private filterRows(
    rows: Record<string, SqlValue>[],
    whereClause: string,
    params: SqlValue[],
    startParamIndex = 0
  ): { filtered: Record<string, SqlValue>[]; paramIndex: number } {
    // Simple WHERE parsing (supports col = ?, col > ?, etc.)
    const conditions = whereClause.split(/\s+AND\s+/i);

    // First, parse all conditions to determine parameter values
    const parsedConditions: Array<{
      col: string;
      op: string;
      val: SqlValue;
    }> = [];

    let paramIndex = startParamIndex;
    for (const cond of conditions) {
      const match = cond.match(/^(\w+)\s*(=|!=|<>|>|<|>=|<=|LIKE|IS)\s*(.+)$/i);
      if (!match) continue;

      const [, col, op, valStr] = match;
      let val: SqlValue;

      if (valStr.trim() === '?') {
        val = params[paramIndex++];
      } else if (valStr.startsWith("'") && valStr.endsWith("'")) {
        val = valStr.slice(1, -1);
      } else if (!isNaN(Number(valStr))) {
        val = Number(valStr);
      } else if (valStr.toUpperCase() === 'NULL') {
        val = null;
      } else {
        val = valStr;
      }

      parsedConditions.push({ col, op: op.toUpperCase(), val });
    }

    const filtered = rows.filter(row => {
      for (const { col, op, val } of parsedConditions) {
        const rowVal = row[col];

        switch (op) {
          case '=':
            if (rowVal !== val) return false;
            break;
          case '!=':
          case '<>':
            if (rowVal === val) return false;
            break;
          case '>':
            if (!(Number(rowVal) > Number(val))) return false;
            break;
          case '<':
            if (!(Number(rowVal) < Number(val))) return false;
            break;
          case '>=':
            if (!(Number(rowVal) >= Number(val))) return false;
            break;
          case '<=':
            if (!(Number(rowVal) <= Number(val))) return false;
            break;
          case 'IS':
            if (val === null && rowVal !== null) return false;
            break;
          case 'LIKE':
            if (typeof rowVal === 'string' && typeof val === 'string') {
              const regex = new RegExp(
                '^' + val.replace(/%/g, '.*').replace(/_/g, '.') + '$',
                'i'
              );
              if (!regex.test(rowVal)) return false;
            }
            break;
        }
      }
      return true;
    });

    return { filtered, paramIndex };
  }

  private orderRows(
    rows: Record<string, SqlValue>[],
    orderBy: string
  ): Record<string, SqlValue>[] {
    const parts = orderBy.split(',').map(p => {
      const match = p.trim().match(/^(\w+)(?:\s+(ASC|DESC))?$/i);
      if (match) {
        return { col: match[1], desc: match[2]?.toUpperCase() === 'DESC' };
      }
      return null;
    }).filter((p): p is { col: string; desc: boolean } => p !== null);

    return [...rows].sort((a, b) => {
      for (const { col, desc } of parts) {
        const aVal = a[col];
        const bVal = b[col];

        if (aVal === bVal) continue;
        if (aVal === null) return desc ? -1 : 1;
        if (bVal === null) return desc ? 1 : -1;

        const cmp = aVal < bVal ? -1 : 1;
        return desc ? -cmp : cmp;
      }
      return 0;
    });
  }
}

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

/**
 * Create a prepared statement
 *
 * @param sql - SQL string
 * @param engine - Execution engine
 * @returns Prepared statement
 */
export function createStatement<T = unknown, P extends BindParameters = BindParameters>(
  sql: string,
  engine: ExecutionEngine
): Statement<T, P> {
  return new PreparedStatement<T, P>(sql, engine);
}
