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
import {
  evaluateWhereCondition,
  type WhereEvaluatorDeps,
} from './where-evaluator.js';
import {
  evaluateCaseExpr,
  splitSelectColumns,
  containsCaseExpression,
  findKeywordOutsideCaseAndStrings,
  evaluateExprWithSubqueries,
  containsSubqueryExpr,
  type ExprEvalContext,
  type SubqueryEvaluator,
} from './case-expr.js';

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
 * Index column definition with sort order
 */
export interface IndexColumn {
  name: string;
  order: 'ASC' | 'DESC';
}

/**
 * In-memory index definition
 */
export interface InMemoryIndex {
  name: string;
  tableName: string;
  columns: IndexColumn[];
  unique: boolean;
}

/**
 * In-memory database storage
 */
export interface InMemoryStorage {
  tables: Map<string, InMemoryTable>;
  indexes: Map<string, InMemoryIndex>;
  lastInsertRowid: number | bigint;
}

/**
 * Create an empty in-memory storage
 */
export function createInMemoryStorage(): InMemoryStorage {
  return {
    tables: new Map(),
    indexes: new Map(),
    lastInsertRowid: 0,
  };
}

/**
 * Simple in-memory execution engine for testing
 */
export class InMemoryEngine implements ExecutionEngine {
  private storage: InMemoryStorage;
  /**
   * Cache for scalar subquery results to avoid re-executing identical subqueries.
   * Key is the SQL string (after substitution for correlated subqueries).
   * This dramatically improves performance for queries like:
   * SELECT * FROM products WHERE unit_price > (SELECT AVG(unit_price) FROM products)
   * which would otherwise execute the subquery for every row.
   */
  private scalarSubqueryCache: Map<string, SqlValue> = new Map();

  constructor(storage?: InMemoryStorage) {
    this.storage = storage ?? createInMemoryStorage();
  }

  /**
   * Clear the scalar subquery cache.
   * Call this between queries to avoid stale results.
   */
  clearSubqueryCache(): void {
    this.scalarSubqueryCache.clear();
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

    // Handle CREATE INDEX (includes CREATE UNIQUE INDEX)
    if (normalized.startsWith('CREATE INDEX') || normalized.startsWith('CREATE UNIQUE INDEX')) {
      return this.executeCreateIndex(sql);
    }

    // Handle DROP INDEX
    if (normalized.startsWith('DROP INDEX')) {
      return this.executeDropIndex(sql);
    }

    // Handle INSERT
    if (normalized.startsWith('INSERT')) {
      return this.executeInsert(sql, params);
    }

    // Handle EXPLAIN
    if (normalized.startsWith('EXPLAIN')) {
      return this.executeSelect(sql, params);
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

    // Handle ANALYZE (no-op for in-memory engine)
    if (normalized.startsWith('ANALYZE')) {
      return { rows: [], columns: [], changes: 0, lastInsertRowid: 0 };
    }

    // Handle BEGIN/COMMIT/ROLLBACK (no-op for in-memory engine)
    if (normalized.startsWith('BEGIN') || normalized.startsWith('COMMIT') || normalized.startsWith('ROLLBACK')) {
      return { rows: [], columns: [], changes: 0, lastInsertRowid: 0 };
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
    // Simple CREATE TABLE parsing - supports both quoted and unquoted identifiers
    const match = sql.match(
      /CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:"([^"]+)"|(\w+))\s*\(([\s\S]+)\)/i
    );

    if (!match) {
      throw new StatementError(StatementErrorCode.INVALID_SQL, 'Invalid CREATE TABLE syntax', sql);
    }

    // Table name is either in group 1 (quoted) or group 2 (unquoted)
    // Column definitions are in group 3
    const tableName = match[1] || match[2];
    const columnDefs = match[3];

    // Parse columns
    const columns: ColumnInfo[] = [];
    const columnParts = columnDefs.split(',').map(c => c.trim());

    for (const part of columnParts) {
      const colMatch = part.match(/^(\w+)\s+(\w+)/);
      if (colMatch) {
        const col: ColumnInfo & { primaryKey?: boolean; autoIncrement?: boolean; defaultValue?: SqlValue } = {
          name: colMatch[1],
          column: colMatch[1],
          table: tableName,
          database: 'main',
          type: colMatch[2].toUpperCase(),
        };

        // Check for PRIMARY KEY
        if (/PRIMARY\s+KEY/i.test(part)) {
          col.primaryKey = true;
        }

        // Check for AUTOINCREMENT
        if (/AUTOINCREMENT/i.test(part)) {
          col.autoIncrement = true;
        }

        // Check for DEFAULT value
        const defaultMatch = part.match(/DEFAULT\s+(-?\d+(?:\.\d+)?|'[^']*'|NULL)/i);
        if (defaultMatch) {
          const defVal = defaultMatch[1];
          if (defVal.toUpperCase() === 'NULL') {
            col.defaultValue = null;
          } else if (defVal.startsWith("'") && defVal.endsWith("'")) {
            col.defaultValue = defVal.slice(1, -1);
          } else {
            col.defaultValue = Number(defVal);
          }
        }

        columns.push(col);
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

  private executeCreateIndex(sql: string): ExecutionResult {
    // Parse CREATE [UNIQUE] INDEX [IF NOT EXISTS] indexName ON tableName (col1 [ASC|DESC], col2 [ASC|DESC], ...)
    const match = sql.match(
      /CREATE\s+(UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s+ON\s+(\w+)\s*\(([^)]+)\)/i
    );

    if (!match) {
      throw new StatementError(StatementErrorCode.INVALID_SQL, 'Invalid CREATE INDEX syntax', sql);
    }

    const isUnique = !!match[1];
    const indexName = match[2];
    const tableName = match[3];
    const columnList = match[4];
    const ifNotExists = /IF\s+NOT\s+EXISTS/i.test(sql);

    // Validate table exists
    const table = this.storage.tables.get(tableName);
    if (!table) {
      throw createTableNotFoundError(tableName, sql);
    }

    // Check if index already exists
    if (this.storage.indexes.has(indexName)) {
      if (ifNotExists) {
        // Silently ignore - index already exists
        return { rows: [], columns: [] };
      }
      throw new StatementError(
        StatementErrorCode.EXECUTION_ERROR,
        `index ${indexName} already exists`,
        sql
      );
    }

    // Parse column definitions with sort order
    const columns: IndexColumn[] = [];
    const columnDefs = columnList.split(',').map(c => c.trim());

    for (const colDef of columnDefs) {
      const colMatch = colDef.match(/^(\w+)(?:\s+(ASC|DESC))?$/i);
      if (!colMatch) {
        throw new StatementError(
          StatementErrorCode.INVALID_SQL,
          `Invalid column definition in CREATE INDEX: ${colDef}`,
          sql
        );
      }

      const colName = colMatch[1];
      const order = (colMatch[2]?.toUpperCase() as 'ASC' | 'DESC') || 'ASC';

      // Validate column exists in table
      const columnExists = table.columns.some(c => c.name === colName);
      if (!columnExists) {
        throw new StatementError(
          StatementErrorCode.COLUMN_NOT_FOUND,
          `no such column: ${colName}`,
          sql
        );
      }

      columns.push({ name: colName, order });
    }

    // Create the index
    const index: InMemoryIndex = {
      name: indexName,
      tableName,
      columns,
      unique: isUnique,
    };

    this.storage.indexes.set(indexName, index);

    return { rows: [], columns: [] };
  }

  private executeDropIndex(sql: string): ExecutionResult {
    // Parse DROP INDEX [IF EXISTS] indexName
    const match = sql.match(/DROP\s+INDEX\s+(?:IF\s+EXISTS\s+)?(\w+)/i);

    if (!match) {
      throw new StatementError(StatementErrorCode.INVALID_SQL, 'Invalid DROP INDEX syntax', sql);
    }

    const indexName = match[1];
    const ifExists = /IF\s+EXISTS/i.test(sql);

    // Check if index exists
    if (!this.storage.indexes.has(indexName)) {
      if (ifExists) {
        // Silently ignore - index doesn't exist
        return { rows: [], columns: [] };
      }
      throw new StatementError(
        StatementErrorCode.EXECUTION_ERROR,
        `no such index: ${indexName}`,
        sql
      );
    }

    // Drop the index
    this.storage.indexes.delete(indexName);

    return { rows: [], columns: [] };
  }

  private executeInsert(sql: string, params: SqlValue[]): ExecutionResult {
    // Parse the INSERT statement to extract table name, optional column list, and value tuples
    const parsed = this.parseInsertStatement(sql);
    if (!parsed) {
      throw new StatementError(StatementErrorCode.INVALID_SQL, 'Invalid INSERT syntax', sql);
    }

    const { tableName, columnNames: explicitColumns, valueTuples } = parsed;

    const table = this.storage.tables.get(tableName);
    if (!table) {
      throw createTableNotFoundError(tableName, sql);
    }

    // Determine column names
    const columnNames = explicitColumns ?? table.columns.map(c => c.name);

    // Get default values from CREATE TABLE (parse from column definitions)
    const columnDefaults = this.getColumnDefaults(table);

    let totalChanges = 0;
    let paramIndex = 0;

    for (const valuePlaceholders of valueTuples) {
      // Validate value count matches column count
      if (valuePlaceholders.length !== columnNames.length) {
        throw new StatementError(
          StatementErrorCode.INVALID_SQL,
          `INSERT has ${valuePlaceholders.length} values but table ${tableName} has ${columnNames.length} columns`,
          sql
        );
      }

      // Build row from params
      const row: Record<string, SqlValue> = {};

      for (let i = 0; i < columnNames.length; i++) {
        const colName = columnNames[i];
        const placeholder = valuePlaceholders[i];

        if (placeholder === '?') {
          row[colName] = params[paramIndex++];
        } else if (placeholder.startsWith(':')) {
          row[colName] = params[paramIndex++];
        } else if (placeholder.toUpperCase() === 'DEFAULT') {
          // Use column default value
          row[colName] = columnDefaults.get(colName) ?? null;
        } else {
          // Try to evaluate as expression (handles arithmetic, || concat, function calls)
          row[colName] = this.evaluateInsertExpression(placeholder, params, paramIndex);
          // Count any ? params consumed in expression
          const qCount = (placeholder.match(/\?/g) || []).length;
          paramIndex += qCount;
        }
      }

      // Handle auto-increment for INTEGER PRIMARY KEY columns
      // When NULL is inserted into an INTEGER PRIMARY KEY, auto-generate an ID
      const pkCol = this.findPrimaryKeyColumn(table);
      if (pkCol && (row[pkCol] === null || row[pkCol] === undefined)) {
        row[pkCol] = table.autoIncrement++;
      }

      // Enforce unique index constraints
      this.checkUniqueConstraints(tableName, row, sql);

      table.rows.push(row);
      this.storage.lastInsertRowid = row['id'] ?? row[pkCol ?? ''] ?? table.rows.length;
      totalChanges++;
    }

    return {
      rows: [],
      columns: [],
      changes: totalChanges,
      lastInsertRowid: this.storage.lastInsertRowid,
    };
  }

  /**
   * Parse an INSERT statement, supporting:
   * - INSERT INTO t (cols) VALUES (v1), (v2), ...
   * - INSERT INTO t VALUES (v1), (v2), ...
   * - Quoted table names
   */
  private parseInsertStatement(sql: string): {
    tableName: string;
    columnNames: string[] | null;
    valueTuples: string[][];
  } | null {
    // Match: INSERT INTO table_name
    const headerMatch = sql.match(/^INSERT\s+INTO\s+(?:"([^"]+)"|(\w+))\s*/i);
    if (!headerMatch) return null;

    const tableName = headerMatch[1] || headerMatch[2];
    let rest = sql.slice(headerMatch[0].length);

    // Check for optional column list
    let columnNames: string[] | null = null;
    if (rest.startsWith('(')) {
      // Find closing paren for column list (before VALUES keyword)
      const upperRest = rest.toUpperCase();
      const valuesIdx = upperRest.indexOf('VALUES');
      if (valuesIdx === -1) return null;

      const colListStr = rest.slice(0, valuesIdx).trim();
      // colListStr should be "(col1, col2, ...)"
      const colMatch = colListStr.match(/^\(([^)]+)\)\s*$/);
      if (colMatch) {
        columnNames = colMatch[1].split(',').map(c => c.trim());
        rest = rest.slice(valuesIdx);
      }
    }

    // Now rest should start with VALUES
    const valuesMatch = rest.match(/^VALUES\s*/i);
    if (!valuesMatch) return null;
    rest = rest.slice(valuesMatch[0].length);

    // Parse multiple value tuples: (v1, v2), (v3, v4), ...
    const valueTuples: string[][] = [];
    let pos = 0;
    while (pos < rest.length) {
      // Skip whitespace and commas between tuples
      while (pos < rest.length && (/\s/.test(rest[pos]) || rest[pos] === ',')) pos++;
      if (pos >= rest.length) break;

      if (rest[pos] !== '(') break;
      pos++; // skip '('

      // Find matching close paren, respecting strings and nested parens
      let depth = 1;
      let tupleStart = pos;
      let inQuote = false;
      let quoteChar = '';

      while (pos < rest.length && depth > 0) {
        const ch = rest[pos];
        if (!inQuote) {
          if (ch === "'" || ch === '"') {
            inQuote = true;
            quoteChar = ch;
          } else if (ch === '(') {
            depth++;
          } else if (ch === ')') {
            depth--;
            if (depth === 0) break;
          }
        } else {
          if (ch === quoteChar) {
            if (pos + 1 < rest.length && rest[pos + 1] === quoteChar) {
              pos++; // skip escaped quote
            } else {
              inQuote = false;
            }
          }
        }
        pos++;
      }

      const tupleContent = rest.slice(tupleStart, pos);
      pos++; // skip closing ')'

      const values = this.parseInsertValues(tupleContent);
      valueTuples.push(values);
    }

    if (valueTuples.length === 0) return null;

    return { tableName, columnNames, valueTuples };
  }

  /**
   * Find the PRIMARY KEY column name for a table
   */
  /**
   * Execute a pragma table function: SELECT * FROM pragma_xxx('arg')
   */
  private executePragmaTableFunction(pragmaName: string, arg: string): ExecutionResult {
    switch (pragmaName.toLowerCase()) {
      case 'index_info': {
        const index = this.storage.indexes.get(arg);
        if (!index) {
          return { rows: [], columns: [] };
        }
        const rows = index.columns.map((col, i) => ({
          seqno: i as SqlValue,
          cid: i as SqlValue,
          name: col.name as SqlValue,
        }));
        return { rows, columns: [] };
      }
      case 'index_list': {
        const indexes: Record<string, SqlValue>[] = [];
        let seq = 0;
        for (const index of this.storage.indexes.values()) {
          if (index.tableName === arg) {
            indexes.push({
              seq: seq++ as SqlValue,
              name: index.name as SqlValue,
              unique: (index.unique ? 1 : 0) as SqlValue,
              origin: 'c' as SqlValue,
              partial: 0 as SqlValue,
            });
          }
        }
        return { rows: indexes, columns: [] };
      }
      default:
        return { rows: [], columns: [] };
    }
  }

  /**
   * Execute EXPLAIN QUERY PLAN
   */
  private executeExplainQueryPlan(sql: string): ExecutionResult {
    // Simple explain that shows index usage when applicable
    const selectMatch = sql.match(/SELECT\s+.*?\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?/i);
    if (!selectMatch) {
      return { rows: [{ id: 0, parent: 0, notused: 0, detail: 'SCAN' as SqlValue }], columns: [] };
    }

    const tableName = selectMatch[1];
    const whereClause = selectMatch[2];

    // Check if any index could be used for the WHERE clause
    if (whereClause) {
      // Extract column names from simple equality conditions
      const conditions = whereClause.split(/\s+AND\s+/i);
      for (const cond of conditions) {
        const colMatch = cond.match(/^(\w+)\s*=/);
        if (colMatch) {
          const colName = colMatch[1];
          // Check if any index on this table covers this column
          for (const index of this.storage.indexes.values()) {
            if (index.tableName === tableName && index.columns[0]?.name === colName) {
              return {
                rows: [{
                  id: 0 as SqlValue,
                  parent: 0 as SqlValue,
                  notused: 0 as SqlValue,
                  detail: `SEARCH ${tableName} USING INDEX ${index.name} (${colName}=?)` as SqlValue,
                }],
                columns: [],
              };
            }
          }
        }
      }
    }

    return {
      rows: [{
        id: 0 as SqlValue,
        parent: 0 as SqlValue,
        notused: 0 as SqlValue,
        detail: `SCAN ${tableName}` as SqlValue,
      }],
      columns: [],
    };
  }

  /**
   * Check unique index constraints for a new row
   */
  private checkUniqueConstraints(tableName: string, row: Record<string, SqlValue>, sql: string): void {
    const table = this.storage.tables.get(tableName);
    if (!table) return;

    for (const index of this.storage.indexes.values()) {
      if (index.tableName !== tableName || !index.unique) continue;

      // Build the key for the new row from index columns
      const newKeyValues = index.columns.map(c => row[c.name]);

      // Check if any existing row has the same key values
      for (const existingRow of table.rows) {
        const existingKeyValues = index.columns.map(c => existingRow[c.name]);

        // NULL values don't violate uniqueness
        if (newKeyValues.some(v => v === null || v === undefined)) continue;
        if (existingKeyValues.some(v => v === null || v === undefined)) continue;

        // Compare all key columns
        const matches = newKeyValues.every((v, i) => v === existingKeyValues[i]);
        if (matches) {
          throw new StatementError(
            StatementErrorCode.CONSTRAINT_VIOLATION,
            `UNIQUE constraint failed: ${index.name}`,
            sql
          );
        }
      }
    }
  }

  private findPrimaryKeyColumn(table: InMemoryTable): string | null {
    // Check column metadata - look for INTEGER PRIMARY KEY pattern
    // We store this info from CREATE TABLE parsing
    for (const col of table.columns) {
      if ((col as { primaryKey?: boolean }).primaryKey) {
        return col.name;
      }
    }
    // Fallback: check for 'id' column
    const idCol = table.columns.find(c => c.type === 'INTEGER' && c.name.toLowerCase() === 'id');
    return idCol ? idCol.name : null;
  }

  /**
   * Get default values for table columns
   */
  private getColumnDefaults(table: InMemoryTable): Map<string, SqlValue> {
    const defaults = new Map<string, SqlValue>();
    for (const col of table.columns) {
      const def = (col as { defaultValue?: SqlValue }).defaultValue;
      if (def !== undefined) {
        defaults.set(col.name, def);
      }
    }
    return defaults;
  }

  /**
   * Evaluate an INSERT expression (handles literals, arithmetic, || concatenation, function calls)
   */
  private evaluateInsertExpression(expr: string, params: SqlValue[], _paramIndex: number): SqlValue {
    const trimmed = expr.trim();

    // Simple literal values (fast path)
    if (trimmed === '?') return params[_paramIndex];
    if (trimmed.toUpperCase() === 'NULL') return null;
    if (trimmed.toUpperCase().startsWith("X'") && trimmed.endsWith("'")) {
      const hex = trimmed.slice(2, -1);
      const bytes = new Uint8Array(hex.length / 2);
      for (let i = 0; i < hex.length; i += 2) {
        bytes[i / 2] = parseInt(hex.substr(i, 2), 16);
      }
      return bytes;
    }
    if (/^-?\d+(\.\d+)?$/.test(trimmed)) return Number(trimmed);

    // Check for string concatenation (||) before single string literal
    if (trimmed.includes('||')) {
      return this.evaluateStringConcat(trimmed);
    }

    // Single string literal (must check after || to avoid 'a' || 'b' being treated as one string)
    if (this.isSingleStringLiteral(trimmed)) {
      return trimmed.slice(1, -1).replace(/''/g, "'");
    }

    // Check for function calls or arithmetic expressions
    if (/[+\-*/%]/.test(trimmed) || /\w+\s*\(/.test(trimmed)) {
      return evaluateCaseExpr(trimmed, {}, params, { value: _paramIndex });
    }

    // Fallback
    return trimmed;
  }

  /**
   * Check if a string is a single string literal (not containing || or other operators)
   */
  private isSingleStringLiteral(s: string): boolean {
    if (!s.startsWith("'") || !s.endsWith("'")) return false;
    // Walk through the string, tracking quote state
    let i = 1; // skip opening quote
    while (i < s.length) {
      if (s[i] === "'") {
        if (i + 1 < s.length && s[i + 1] === "'") {
          i += 2; // escaped quote
          continue;
        }
        // End of string - should be at the last character
        return i === s.length - 1;
      }
      i++;
    }
    return false;
  }

  /**
   * Evaluate string concatenation with || operator
   */
  private evaluateStringConcat(expr: string): SqlValue {
    // Split on || outside of quotes
    const parts: string[] = [];
    let current = '';
    let inQuote = false;

    for (let i = 0; i < expr.length; i++) {
      const ch = expr[i];
      if (!inQuote && ch === "'" ) {
        inQuote = true;
        current += ch;
      } else if (inQuote && ch === "'") {
        if (i + 1 < expr.length && expr[i + 1] === "'") {
          current += "''";
          i++;
        } else {
          inQuote = false;
          current += ch;
        }
      } else if (!inQuote && ch === '|' && i + 1 < expr.length && expr[i + 1] === '|') {
        parts.push(current.trim());
        current = '';
        i++; // skip second |
      } else {
        current += ch;
      }
    }
    if (current.trim()) parts.push(current.trim());

    const evaluatedParts = parts.map(p => {
      const t = p.trim();
      if (t.startsWith("'") && t.endsWith("'")) return t.slice(1, -1).replace(/''/g, "'");
      if (/^-?\d+(\.\d+)?$/.test(t)) return String(Number(t));
      return t;
    });

    return evaluatedParts.join('');
  }

  /**
   * Parse VALUES clause, handling quoted strings with commas
   */
  private parseInsertValues(valuesStr: string): string[] {
    const values: string[] = [];
    let current = '';
    let inQuote = false;
    let quoteChar = '';
    let parenDepth = 0;

    for (let i = 0; i < valuesStr.length; i++) {
      const char = valuesStr[i];

      if (!inQuote) {
        if (char === "'" || char === '"') {
          inQuote = true;
          quoteChar = char;
          current += char;
        } else if (char === '(') {
          parenDepth++;
          current += char;
        } else if (char === ')') {
          parenDepth--;
          current += char;
        } else if (char === ',' && parenDepth === 0) {
          values.push(current.trim());
          current = '';
        } else {
          current += char;
        }
      } else {
        current += char;
        if (char === quoteChar) {
          // Check for escaped quote
          if (i + 1 < valuesStr.length && valuesStr[i + 1] === quoteChar) {
            current += valuesStr[i + 1];
            i++;
          } else {
            inQuote = false;
          }
        }
      }
    }

    if (current.trim()) {
      values.push(current.trim());
    }

    return values;
  }

  /**
   * Parse a literal value from INSERT VALUES clause
   */
  private parseInsertLiteralValue(placeholder: string): SqlValue {
    const trimmed = placeholder.trim();

    // String literal - handle escaped quotes ('' -> ')
    if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
      return trimmed.slice(1, -1).replace(/''/g, "'");
    }

    // BLOB literal (X'...')
    if (trimmed.toUpperCase().startsWith("X'") && trimmed.endsWith("'")) {
      // Return as Uint8Array for BLOB data
      const hex = trimmed.slice(2, -1);
      const bytes = new Uint8Array(hex.length / 2);
      for (let i = 0; i < hex.length; i += 2) {
        bytes[i / 2] = parseInt(hex.substr(i, 2), 16);
      }
      return bytes;
    }

    // NULL
    if (trimmed.toUpperCase() === 'NULL') {
      return null;
    }

    // Number (integer or float)
    if (/^-?\d+(\.\d+)?$/.test(trimmed)) {
      return Number(trimmed);
    }

    // Fallback - return as-is
    return trimmed;
  }

  private executeSelect(sql: string, params: SqlValue[]): ExecutionResult {
    // Clear the subquery cache at the start of each top-level SELECT
    // This ensures we don't use stale cached results from previous queries
    // while still benefiting from caching within a single query execution
    this.scalarSubqueryCache.clear();

    // Handle pragma table functions: SELECT * FROM pragma_xxx('arg')
    const pragmaMatch = sql.match(/SELECT\s+\*\s+FROM\s+pragma_(\w+)\s*\(\s*'([^']+)'\s*\)/i);
    if (pragmaMatch) {
      return this.executePragmaTableFunction(pragmaMatch[1], pragmaMatch[2]);
    }

    // Handle EXPLAIN QUERY PLAN
    const explainMatch = sql.match(/^EXPLAIN\s+QUERY\s+PLAN\s+(.*)/i);
    if (explainMatch) {
      return this.executeExplainQueryPlan(explainMatch[1]);
    }

    // Parse SELECT with CASE expression support
    const parsed = this.parseSelectStatement(sql);
    if (!parsed) {
      throw new StatementError(StatementErrorCode.INVALID_SQL, 'Invalid SELECT syntax', sql);
    }

    const { columnList, tableName, tableRefs, whereClause, groupBy, havingClause, orderBy, limit } = parsed;

    // Validate all tables exist and build alias map
    const aliasToTable: Map<string, InMemoryTable> = new Map();
    for (const ref of tableRefs) {
      const table = this.storage.tables.get(ref.tableName);
      if (!table) {
        throw createTableNotFoundError(ref.tableName, sql);
      }
      aliasToTable.set(ref.alias, table);
    }

    // Build rows: if no tables (FROM-less), single table, or multiple tables
    let rows: Record<string, SqlValue>[];

    if (tableRefs.length === 0) {
      // FROM-less SELECT (e.g., SELECT 1, SELECT (SELECT ...))
      rows = [{}];
    } else if (tableRefs.length === 1) {
      // Single table query - prefix columns with alias for consistent resolution
      const ref = tableRefs[0];
      const table = aliasToTable.get(ref.alias)!;
      rows = table.rows.map(row => {
        const prefixed: Record<string, SqlValue> = {};
        for (const [col, val] of Object.entries(row)) {
          // Store both prefixed and unprefixed for backward compatibility
          prefixed[`${ref.alias}.${col}`] = val;
          prefixed[col] = val;
        }
        return prefixed;
      });
    } else {
      // Multiple tables - build Cartesian product
      rows = this.buildCrossProduct(tableRefs, aliasToTable);
    }

    // Filter rows - check for subquery in WHERE
    if (whereClause) {
      if (this.containsSubquery(whereClause)) {
        rows = this.filterWithSubquery(rows, whereClause, params, tableName);
      } else {
        rows = this.filterRowsWithAliases(rows, whereClause, params, aliasToTable);
      }
    }

    // Project columns
    const selectAll = columnList.trim() === '*';
    if (!selectAll) {
      // Use splitSelectColumns to properly handle CASE expressions
      const colDefs = splitSelectColumns(columnList);

      // Check if any column is an aggregate function
      const hasAggregate = colDefs.some(col => {
        const aliasInfo = this.parseColumnAlias(col);
        return this.isAggregateFunction(aliasInfo.expr);
      });

      // Handle GROUP BY
      if (groupBy && groupBy.length > 0) {
        // Group rows by the GROUP BY columns
        const groups = new Map<string, Record<string, SqlValue>[]>();

        for (const row of rows) {
          const groupKey = groupBy.map(col => {
            const val = row[col] ?? row[col.split('.')[1]] ?? null;
            return JSON.stringify(val);
          }).join('|');

          if (!groups.has(groupKey)) {
            groups.set(groupKey, []);
          }
          groups.get(groupKey)!.push(row);
        }

        // For each group, compute aggregates
        const resultRows: Record<string, SqlValue>[] = [];

        for (const [, groupRows] of groups) {
          // Apply HAVING filter if present
          if (havingClause) {
            if (!this.evaluateHavingClause(havingClause, groupRows, params)) {
              continue; // Skip this group
            }
          }

          const resultRow: Record<string, SqlValue> = {};

          for (const col of colDefs) {
            const aliasInfo = this.parseColumnAlias(col);
            const expr = aliasInfo.expr;
            const alias = aliasInfo.alias;

            if (this.isAggregateFunction(expr)) {
              resultRow[alias] = this.evaluateAggregate(expr, groupRows, params);
            } else {
              // Non-aggregate column in GROUP BY query - take value from first row
              const resolvedCol = this.resolveColumnName(expr.trim());
              resultRow[alias] = groupRows.length > 0 ? (groupRows[0][resolvedCol] ?? groupRows[0][resolvedCol.split('.')[1]] ?? null) : null;
            }
          }

          resultRows.push(resultRow);
        }

        rows = resultRows;
      } else if (hasAggregate) {
        // Compute aggregates over all rows and return single row
        const aggregateRow: Record<string, SqlValue> = {};
        for (const col of colDefs) {
          const aliasInfo = this.parseColumnAlias(col);
          const expr = aliasInfo.expr;
          const alias = aliasInfo.alias;

          if (this.isAggregateFunction(expr)) {
            aggregateRow[alias] = this.evaluateAggregate(expr, rows, params);
          } else {
            // For non-aggregate columns in aggregate query, resolve and take first row value
            const resolvedCol = this.resolveColumnName(expr.trim());
            aggregateRow[alias] = rows.length > 0 ? (rows[0][resolvedCol] ?? null) : null;
          }
        }
        rows = [aggregateRow];
      } else {
        // For non-aggregate queries, sort BEFORE projection so ORDER BY can access
        // columns not in the SELECT list (e.g., SELECT id, name FROM users ORDER BY age)
        if (orderBy) {
          rows = this.orderRowsWithAliases(rows, orderBy, colDefs, params);
        }

        rows = rows.map(row => {
          const projected: Record<string, SqlValue> = {};
          const pIdx = { value: 0 };

          for (const col of colDefs) {
            // Parse alias: find AS outside of CASE...END blocks
            const aliasInfo = this.parseColumnAlias(col);
            const expr = aliasInfo.expr;
            let outputAlias = aliasInfo.alias;

            // For alias.column pattern, extract just the column name for output
            if (/^\w+\.\w+$/.test(expr.trim())) {
              const parts = expr.trim().split('.');
              // Use just the column name as output key if no explicit AS alias
              if (aliasInfo.alias === expr.trim()) {
                outputAlias = parts[1];
              }
            }

            // Check if expression is a scalar subquery
            if (this.isScalarSubquery(expr)) {
              let subquerySql = expr.slice(1, -1).trim(); // Remove outer parens

              // Check if this is a correlated subquery referencing outer row
              // We need to detect references like "t1.b" where t1 is the outer table
              const isCorrelated = this.isCorrelatedSubqueryForSelect(subquerySql, row);

              if (isCorrelated) {
                // Substitute outer row references with actual values
                subquerySql = this.substituteOuterReferences(subquerySql, row);
              }

              const subqueryResult = this.execute(subquerySql, params);

              // Scalar subquery must return at most one row
              if (subqueryResult.rows.length > 1) {
                throw new StatementError(
                  StatementErrorCode.INVALID_SQL,
                  'Scalar subquery must return a single row, but returned more than one row',
                  expr
                );
              }

              let value: SqlValue = null;
              if (subqueryResult.rows.length > 0) {
                const firstRow = subqueryResult.rows[0];
                const keys = Object.keys(firstRow);
                if (keys.length > 0) {
                  value = firstRow[keys[0]];
                }
              }
              // Only set if not already present (first column wins in collision)
              if (!(outputAlias in projected)) {
                projected[outputAlias] = value;
              }
            } else if (containsCaseExpression(expr) || this.isArithmeticOrFunctionExpression(expr)) {
              // Evaluate CASE or arithmetic expression with subquery support
              // Only set if not already present (first column wins in collision)
              if (!(outputAlias in projected)) {
                if (containsSubqueryExpr(expr)) {
                  projected[outputAlias] = evaluateExprWithSubqueries(expr, row, {
                    params,
                    pIdx,
                    evaluateSubquery: (sql, p, outerRow) => this.evaluateScalarSubquery(sql, p, outerRow ?? row),
                  });
                } else {
                  projected[outputAlias] = evaluateCaseExpr(expr, row, params, pIdx);
                }
              }
            } else {
              // Resolve column reference (could be alias.col or just col)
              const resolvedCol = this.resolveColumnName(expr.trim());
              // Only set if not already present (first column wins in collision)
              if (!(outputAlias in projected)) {
                projected[outputAlias] = row[resolvedCol] ?? null;
              }
            }
          }
          return projected;
        });
      }
    } else {
      // SELECT * - return all columns without alias prefixes
      const firstTable = aliasToTable.get(tableRefs[0].alias);
      if (firstTable) {
        rows = rows.map(row => {
          const unprefixed: Record<string, SqlValue> = {};
          for (const colInfo of firstTable.columns) {
            unprefixed[colInfo.name] = row[colInfo.name] ?? row[`${tableRefs[0].alias}.${colInfo.name}`] ?? null;
          }
          return unprefixed;
        });
      }

      // For SELECT *, sort after unprefixing (ORDER BY can reference any column)
      if (orderBy) {
        rows = this.orderRowsWithAliases(rows, orderBy);
      }
    }

    // Order rows for aggregate queries (groupBy or hasAggregate cases)
    // Non-aggregate and SELECT * queries are already sorted above
    if (orderBy && !selectAll) {
      const colDefs = splitSelectColumns(columnList);
      const hasAggregate = colDefs.some(col => {
        const aliasInfo = this.parseColumnAlias(col);
        return this.isAggregateFunction(aliasInfo.expr);
      });
      // Only sort here for aggregate queries (groupBy or hasAggregate)
      if ((groupBy && groupBy.length > 0) || hasAggregate) {
        rows = this.orderRowsWithAliases(rows, orderBy, colDefs, params);
      }
    }

    // Limit rows
    if (limit !== undefined) {
      rows = rows.slice(0, limit);
    }

    return {
      rows,
      columns: selectAll ? (aliasToTable.get(tableRefs[0].alias)?.columns ?? []) : [],
    };
  }

  /**
   * Resolve a column reference: if it's alias.column, return that; otherwise try as-is
   */
  private resolveColumnName(expr: string): string {
    // If it's already alias.column format, return as-is
    if (/^\w+\.\w+$/.test(expr)) {
      return expr;
    }
    // Simple column name
    return expr;
  }

  /**
   * Build a Cartesian (cross) product of multiple table references
   */
  private buildCrossProduct(
    tableRefs: Array<{ tableName: string; alias: string }>,
    aliasToTable: Map<string, InMemoryTable>
  ): Record<string, SqlValue>[] {
    let result: Record<string, SqlValue>[] = [{}];

    for (const ref of tableRefs) {
      const table = aliasToTable.get(ref.alias)!;
      const newResult: Record<string, SqlValue>[] = [];

      for (const existingRow of result) {
        for (const tableRow of table.rows) {
          const combined: Record<string, SqlValue> = { ...existingRow };
          // Add columns with alias prefix
          for (const [col, val] of Object.entries(tableRow)) {
            combined[`${ref.alias}.${col}`] = val;
          }
          newResult.push(combined);
        }
      }

      result = newResult;
    }

    return result;
  }

  /**
   * Filter rows with alias-aware column resolution
   * Uses the new evaluateWhereCondition for complex WHERE clause support
   */
  private filterRowsWithAliases(
    rows: Record<string, SqlValue>[],
    whereClause: string,
    params: SqlValue[],
    aliasToTable: Map<string, InMemoryTable>
  ): Record<string, SqlValue>[] {
    const deps: WhereEvaluatorDeps = {
      parseValueList: (valueList, params, startParamIndex) => this.parseValueList(valueList, params, startParamIndex),
      valuesEqual: (a, b) => this.valuesEqual(a, b),
      getColumnValue: (row, colRef) => this.getColumnValue(row, colRef),
    };

    return rows.filter(row => {
      const pIdx = { value: 0 };
      return evaluateWhereCondition(whereClause, row, params, pIdx, deps);
    });
  }

  /**
   * Get column value from row, handling alias.column notation
   */
  private getColumnValue(row: Record<string, SqlValue>, colRef: string): SqlValue {
    // Try exact match first (works for alias.column)
    if (colRef in row) {
      return row[colRef];
    }
    // If it's alias.col format, try just the column name
    if (/^\w+\.\w+$/.test(colRef)) {
      const colName = colRef.split('.')[1];
      if (colName in row) {
        return row[colName];
      }
    }
    return null;
  }

  /**
   * Order rows with alias-aware column resolution
   * Supports CASE expressions, simple column references, numeric column positions, and subqueries in ORDER BY
   * @param rows The rows to sort
   * @param orderBy The ORDER BY clause string
   * @param selectColumns Optional array of SELECT column expressions for resolving numeric references
   * @param params Optional query parameters for subquery evaluation
   */
  private orderRowsWithAliases(
    rows: Record<string, SqlValue>[],
    orderBy: string,
    selectColumns?: string[],
    params?: SqlValue[]
  ): Record<string, SqlValue>[] {
    // Split ORDER BY terms respecting CASE...END blocks
    const orderTerms = this.splitOrderByTerms(orderBy);

    const parts: Array<{ expr: string; desc: boolean; isExpr: boolean; hasSubquery: boolean }> = [];
    for (const term of orderTerms) {
      const trimmed = term.trim();
      // Check for trailing ASC/DESC
      let desc = false;
      let exprPart = trimmed;
      const ascDescMatch = trimmed.match(/^([\s\S]+?)\s+(ASC|DESC)\s*$/i);
      if (ascDescMatch) {
        exprPart = ascDescMatch[1].trim();
        desc = ascDescMatch[2].toUpperCase() === 'DESC';
      }

      // Resolve numeric column references (e.g., ORDER BY 1)
      const numericMatch = exprPart.match(/^(\d+)$/);
      if (numericMatch && selectColumns) {
        const colIndex = parseInt(numericMatch[1], 10) - 1; // 1-indexed to 0-indexed
        if (colIndex >= 0 && colIndex < selectColumns.length) {
          // Get the expression part (without alias)
          const aliasInfo = this.parseColumnAlias(selectColumns[colIndex]);
          exprPart = aliasInfo.expr;
        }
      }

      const isExpr = containsCaseExpression(exprPart) || /[+\-*/%()]/.test(exprPart);
      const hasSubquery = containsSubqueryExpr(exprPart);
      parts.push({ expr: exprPart, desc, isExpr, hasSubquery });
    }

    return [...rows].sort((a, b) => {
      for (const { expr, desc, isExpr, hasSubquery } of parts) {
        let aVal: SqlValue;
        let bVal: SqlValue;

        if (isExpr) {
          // Evaluate expression for each row
          const pIdx1 = { value: 0 };
          const pIdx2 = { value: 0 };

          if (hasSubquery) {
            // Use evaluateExprWithSubqueries for expressions containing subqueries
            aVal = evaluateExprWithSubqueries(expr, a, {
              params: params ?? [],
              pIdx: pIdx1,
              evaluateSubquery: (sql, p, outerRow) => this.evaluateScalarSubquery(sql, p, outerRow ?? a),
            });
            bVal = evaluateExprWithSubqueries(expr, b, {
              params: params ?? [],
              pIdx: pIdx2,
              evaluateSubquery: (sql, p, outerRow) => this.evaluateScalarSubquery(sql, p, outerRow ?? b),
            });
          } else {
            aVal = evaluateCaseExpr(expr, a, params ?? [], pIdx1);
            bVal = evaluateCaseExpr(expr, b, params ?? [], pIdx2);
          }
        } else {
          aVal = this.getColumnValue(a, expr);
          bVal = this.getColumnValue(b, expr);
        }

        if (aVal === bVal) continue;
        if (aVal === null) return desc ? -1 : 1;
        if (bVal === null) return desc ? 1 : -1;

        const cmp = aVal < bVal ? -1 : 1;
        return desc ? -cmp : cmp;
      }
      return 0;
    });
  }

  /**
   * Split ORDER BY terms respecting CASE...END blocks
   */
  private splitOrderByTerms(orderBy: string): string[] {
    const terms: string[] = [];
    let current = '';
    let caseDepth = 0;
    let inString = false;
    let parenDepth = 0;
    const upper = orderBy.toUpperCase();

    for (let i = 0; i < orderBy.length; i++) {
      const ch = orderBy[i];

      if (ch === "'" && !inString) { inString = true; current += ch; continue; }
      if (ch === "'" && inString) {
        if (orderBy[i + 1] === "'") { current += "''"; i++; continue; }
        inString = false; current += ch; continue;
      }
      if (inString) { current += ch; continue; }

      if (ch === '(') { parenDepth++; current += ch; continue; }
      if (ch === ')') { parenDepth--; current += ch; continue; }

      if (upper.slice(i).startsWith('CASE') && (i + 4 >= orderBy.length || !/\w/.test(orderBy[i + 4]))) {
        caseDepth++; current += orderBy.slice(i, i + 4); i += 3; continue;
      }
      if (upper.slice(i).startsWith('END') && (i + 3 >= orderBy.length || !/\w/.test(orderBy[i + 3]))) {
        if (caseDepth > 0) caseDepth--;
        current += orderBy.slice(i, i + 3); i += 2; continue;
      }

      if (ch === ',' && caseDepth === 0 && parenDepth === 0) {
        terms.push(current.trim());
        current = '';
        continue;
      }

      current += ch;
    }

    if (current.trim()) terms.push(current.trim());
    return terms;
  }

  // ==========================================================================
  // SUBQUERY SUPPORT
  // ==========================================================================

  /**
   * Evaluate a scalar subquery, returning its single value or null.
   * Throws if the subquery returns more than one row.
   * Uses caching to avoid re-executing identical subqueries.
   */
  private evaluateScalarSubquery(sql: string, params: SqlValue[], outerRow?: Record<string, SqlValue>): SqlValue {
    let subquerySql = sql;

    // Handle correlated subquery references
    if (outerRow) {
      const isCorrelated = this.isCorrelatedSubqueryForSelect(subquerySql, outerRow);
      if (isCorrelated) {
        subquerySql = this.substituteOuterReferences(subquerySql, outerRow);
      }
    }

    // Create cache key including params for complete uniqueness
    const cacheKey = subquerySql + '|' + JSON.stringify(params);

    // Check cache first - this is the key optimization for subquery performance
    if (this.scalarSubqueryCache.has(cacheKey)) {
      return this.scalarSubqueryCache.get(cacheKey)!;
    }

    const result = this.execute(subquerySql, params);

    if (result.rows.length > 1) {
      throw new StatementError(
        StatementErrorCode.INVALID_SQL,
        'Scalar subquery must return a single row, but returned more than one row',
        sql
      );
    }

    let value: SqlValue = null;
    if (result.rows.length > 0) {
      const firstRow = result.rows[0];
      const keys = Object.keys(firstRow);
      if (keys.length > 0) {
        value = firstRow[keys[0]];
      }
    }

    // Cache the result for future use
    this.scalarSubqueryCache.set(cacheKey, value);

    return value;
  }

  /**
   * Check if SQL contains a subquery
   */
  private containsSubquery(sql: string): boolean {
    return /\(\s*SELECT\b/i.test(sql);
  }

  /**
   * Check if expression is a scalar subquery: (SELECT ...)
   */
  private isScalarSubquery(expr: string): boolean {
    const trimmed = expr.trim();
    return trimmed.startsWith('(') && trimmed.endsWith(')') &&
           /^\(\s*SELECT\b/i.test(trimmed);
  }

  /**
   * Check if expression contains arithmetic/comparison operators or function calls
   */
  private isArithmeticOrFunctionExpression(expr: string): boolean {
    const trimmed = expr.trim();
    // Check for arithmetic operators: + - * / %
    // Check for comparison operators: < > <= >= = != <>
    // Check for function calls: identifier followed by parenthesis like abs(...)
    // Check for numeric literals
    // Check for string literals
    return /[+\-*/%<>=!]/.test(trimmed) ||
           /\w+\s*\(/.test(trimmed) ||
           /^-?\d+(\.\d+)?$/.test(trimmed) ||
           (trimmed.startsWith("'") && trimmed.endsWith("'"));
  }

  /**
   * Filter rows with subquery support in WHERE clause
   */
  private filterWithSubquery(
    rows: Record<string, SqlValue>[],
    whereClause: string,
    params: SqlValue[],
    tableName: string
  ): Record<string, SqlValue>[] {
    const trimmed = whereClause.trim();

    // Check for EXISTS - needs correlated subquery support
    const existsMatch = this.parseExistsCondition(trimmed);
    if (existsMatch) {
      // Check if subquery references outer tables (correlated)
      const isCorrelated = this.isCorrelatedSubquery(existsMatch.subquery, rows);

      if (isCorrelated && rows.length > 0) {
        // For correlated EXISTS, evaluate subquery for each outer row with caching
        return rows.filter(outerRow => {
          const substitutedSubquery = this.substituteOuterReferences(existsMatch.subquery, outerRow);
          const cacheKey = 'EXISTS:' + substitutedSubquery + '|' + JSON.stringify(params);

          let exists: boolean;
          if (this.scalarSubqueryCache.has(cacheKey)) {
            exists = this.scalarSubqueryCache.get(cacheKey) as unknown as boolean;
          } else {
            const subqueryResult = this.execute(substitutedSubquery, params);
            exists = subqueryResult.rows.length > 0;
            this.scalarSubqueryCache.set(cacheKey, exists as unknown as SqlValue);
          }

          return existsMatch.isNot ? !exists : exists;
        });
      } else {
        // Non-correlated EXISTS - evaluate once
        const subqueryResult = this.execute(existsMatch.subquery, params);
        const exists = subqueryResult.rows.length > 0;
        return existsMatch.isNot ? (exists ? [] : rows) : (exists ? rows : []);
      }
    }

    // Check for IN with subquery
    const inMatch = this.parseInSubquery(trimmed);
    if (inMatch) {
      const subqueryResult = this.execute(inMatch.subquery, params);
      const subqueryValues = subqueryResult.rows.map(r => {
        const keys = Object.keys(r);
        return keys.length > 0 ? r[keys[0]] : null;
      });

      // Handle empty subquery result
      if (subqueryValues.length === 0) {
        // x IN () = FALSE for all x, x NOT IN () = TRUE for all x
        return inMatch.isNot ? rows : [];
      }

      const values = new Set(subqueryValues);
      const hasNullInSubquery = subqueryValues.some(v => v === null);

      return rows.filter(row => {
        const col = inMatch.column.includes('.')
          ? inMatch.column.split('.')[1]
          : inMatch.column;
        const val = row[col];

        // Handle NULL column values
        if (val === null) {
          // NULL IN (...) or NULL NOT IN (...) returns NULL (falsy)
          return false;
        }

        // Check if value matches any in the set
        const matches = values.has(val);

        if (inMatch.isNot) {
          // NOT IN: if NULL is in subquery result and value not found, result is NULL
          if (!matches && hasNullInSubquery) {
            return false;
          }
          return !matches;
        } else {
          return matches;
        }
      });
    }

    // Check for compound OR with subqueries (must check before AND to handle precedence)
    const orParts = this.splitOrOutsideSubqueries(trimmed);
    if (orParts.length > 1) {
      // For OR, we need to find rows matching ANY of the conditions
      const matchedRows = new Set<Record<string, SqlValue>>();
      for (const part of orParts) {
        let partMatches: Record<string, SqlValue>[];
        if (this.containsSubquery(part)) {
          partMatches = this.filterWithSubquery(rows, part, params, tableName);
        } else {
          // Use simple filtering for non-subquery parts
          const deps: WhereEvaluatorDeps = {
            parseValueList: (valueList, params, startParamIndex) => this.parseValueList(valueList, params, startParamIndex),
            valuesEqual: (a, b) => this.valuesEqual(a, b),
            getColumnValue: (row, colRef) => this.getColumnValue(row, colRef),
          };
          partMatches = rows.filter(row => {
            const pIdx = { value: 0 };
            return evaluateWhereCondition(part, row, params, pIdx, deps);
          });
        }
        for (const row of partMatches) {
          matchedRows.add(row);
        }
      }
      return Array.from(matchedRows);
    }

    // Check for compound AND with multiple subqueries
    const andParts = this.splitAndOutsideSubqueries(trimmed);
    if (andParts.length > 1) {
      let result = rows;
      for (const part of andParts) {
        if (this.containsSubquery(part)) {
          result = this.filterWithSubquery(result, part, params, tableName);
        } else {
          // Use simple filtering for non-subquery parts
          const deps: WhereEvaluatorDeps = {
            parseValueList: (valueList, params, startParamIndex) => this.parseValueList(valueList, params, startParamIndex),
            valuesEqual: (a, b) => this.valuesEqual(a, b),
            getColumnValue: (row, colRef) => this.getColumnValue(row, colRef),
          };
          result = result.filter(row => {
            const pIdx = { value: 0 };
            return evaluateWhereCondition(part, row, params, pIdx, deps);
          });
        }
      }
      return result;
    }

    // Check for comparison with scalar subquery: col > (SELECT ...)
    const scalarCompMatch = this.parseScalarComparison(trimmed);
    if (scalarCompMatch) {
      // Check if it's a correlated subquery
      const isCorrelated = this.isCorrelatedSubquery(scalarCompMatch.subquery, rows);

      if (isCorrelated && rows.length > 0) {
        // For correlated scalar comparison, evaluate for each row with caching
        return rows.filter(outerRow => {
          // Use evaluateScalarSubquery which has built-in caching
          const scalarValue = this.evaluateScalarSubquery(scalarCompMatch.subquery, params, outerRow);
          const col = scalarCompMatch.column.includes('.')
            ? scalarCompMatch.column.split('.')[1]
            : scalarCompMatch.column;
          const rowVal = outerRow[col];
          return this.compareValuesForSubquery(rowVal, scalarValue, scalarCompMatch.op);
        });
      } else {
        // Non-correlated scalar comparison - use caching
        const scalarValue = this.evaluateScalarSubquery(scalarCompMatch.subquery, params);

        return rows.filter(row => {
          const col = scalarCompMatch.column.includes('.')
            ? scalarCompMatch.column.split('.')[1]
            : scalarCompMatch.column;
          const rowVal = row[col];
          return this.compareValuesForSubquery(rowVal, scalarValue, scalarCompMatch.op);
        });
      }
    }

    // Fallback to simple filtering
    const result = this.filterRows(rows, whereClause, params, 0);
    return result.filtered;
  }

  /**
   * Check if a subquery references columns from the outer query (correlated)
   */
  private isCorrelatedSubquery(subquerySql: string, outerRows: Record<string, SqlValue>[]): boolean {
    if (outerRows.length === 0) return false;

    // Get column names from first outer row (includes alias.col format)
    const outerRow = outerRows[0];
    const outerColumns = Object.keys(outerRow);

    // Look for references in the subquery that match outer columns
    // Match patterns like: alias.column where alias.column exists in outer row
    for (const col of outerColumns) {
      if (col.includes('.')) {
        // Check if subquery contains this alias.column reference
        const regex = new RegExp(`\\b${this.escapeRegex(col)}\\b`, 'i');
        if (regex.test(subquerySql)) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Check if a scalar subquery in SELECT references columns from the current outer row (correlated).
   * This version takes a single row instead of an array of rows.
   */
  private isCorrelatedSubqueryForSelect(subquerySql: string, outerRow: Record<string, SqlValue>): boolean {
    const outerColumns = Object.keys(outerRow);

    // Look for references in the subquery that match outer columns
    // Match patterns like: alias.column where alias.column exists in outer row
    for (const col of outerColumns) {
      if (col.includes('.')) {
        // Check if subquery contains this alias.column reference
        const regex = new RegExp(`\\b${this.escapeRegex(col)}\\b`, 'i');
        if (regex.test(subquerySql)) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Substitute outer row references in a subquery with actual values
   */
  private substituteOuterReferences(subquerySql: string, outerRow: Record<string, SqlValue>): string {
    let result = subquerySql;

    // Sort columns by length (longest first) to avoid partial replacements
    const columns = Object.keys(outerRow).sort((a, b) => b.length - a.length);

    for (const col of columns) {
      if (col.includes('.')) {
        const value = outerRow[col];
        const regex = new RegExp(`\\b${this.escapeRegex(col)}\\b`, 'gi');

        // Format value for SQL
        let sqlValue: string;
        if (value === null) {
          sqlValue = 'NULL';
        } else if (typeof value === 'string') {
          sqlValue = `'${value.replace(/'/g, "''")}'`;
        } else {
          sqlValue = String(value);
        }

        result = result.replace(regex, sqlValue);
      }
    }

    return result;
  }

  /**
   * Escape special regex characters in a string
   */
  private escapeRegex(str: string): string {
    return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  /**
   * Split WHERE clause by OR, respecting parentheses and strings
   */
  private splitOrOutsideSubqueries(where: string): string[] {
    const parts: string[] = [];
    let current = '';
    let depth = 0;
    let inStr = false;
    const upper = where.toUpperCase();

    for (let i = 0; i < where.length; i++) {
      const ch = where[i];
      if (ch === "'" && !inStr) { inStr = true; current += ch; continue; }
      if (ch === "'" && inStr) {
        if (where[i + 1] === "'") { current += "''"; i++; continue; }
        inStr = false; current += ch; continue;
      }
      if (inStr) { current += ch; continue; }
      if (ch === '(') { depth++; current += ch; continue; }
      if (ch === ')') { depth--; current += ch; continue; }

      if (depth === 0 && upper.slice(i).startsWith(' OR ')) {
        parts.push(current.trim());
        current = '';
        i += 3; // skip ' OR '
        continue;
      }
      current += ch;
    }
    if (current.trim()) parts.push(current.trim());
    return parts;
  }

  /**
   * Split WHERE clause by AND, respecting parentheses, strings, and BETWEEN...AND
   */
  private splitAndOutsideSubqueries(where: string): string[] {
    const parts: string[] = [];
    let current = '';
    let depth = 0;
    let inStr = false;
    let inBetween = false;
    const upper = where.toUpperCase();

    for (let i = 0; i < where.length; i++) {
      const ch = where[i];
      if (ch === "'" && !inStr) { inStr = true; current += ch; continue; }
      if (ch === "'" && inStr) {
        if (where[i + 1] === "'") { current += "''"; i++; continue; }
        inStr = false; current += ch; continue;
      }
      if (inStr) { current += ch; continue; }
      if (ch === '(') { depth++; current += ch; continue; }
      if (ch === ')') { depth--; current += ch; continue; }

      // Check for BETWEEN keyword at depth 0
      if (depth === 0 && upper.slice(i).startsWith(' BETWEEN ')) {
        inBetween = true;
        current += where.slice(i, i + 9);
        i += 8;
        continue;
      }

      if (depth === 0 && upper.slice(i).startsWith(' AND ')) {
        // If we're inside a BETWEEN...AND, this AND is part of BETWEEN
        if (inBetween) {
          inBetween = false; // Consume the BETWEEN...AND
          current += where.slice(i, i + 5);
          i += 4;
          continue;
        }
        // Otherwise, this is a logical AND - split here
        parts.push(current.trim());
        current = '';
        i += 4; // skip ' AND '
        continue;
      }
      current += ch;
    }
    if (current.trim()) parts.push(current.trim());
    return parts;
  }

  /**
   * Parse EXISTS condition
   */
  private parseExistsCondition(whereClause: string): { isNot: boolean; subquery: string } | null {
    // NOT EXISTS
    const notExistsMatch = whereClause.match(/^\s*NOT\s+EXISTS\s*\(\s*(SELECT[\s\S]+)\s*\)\s*$/i);
    if (notExistsMatch) {
      return { isNot: true, subquery: notExistsMatch[1].trim() };
    }

    // EXISTS
    const existsMatch = whereClause.match(/^\s*EXISTS\s*\(\s*(SELECT[\s\S]+)\s*\)\s*$/i);
    if (existsMatch) {
      return { isNot: false, subquery: existsMatch[1].trim() };
    }

    return null;
  }

  /**
   * Parse IN with subquery
   */
  private parseInSubquery(whereClause: string): { isNot: boolean; column: string; subquery: string } | null {
    // NOT IN
    const notInMatch = whereClause.match(/^(\w+(?:\.\w+)?)\s+NOT\s+IN\s*\(\s*(SELECT[\s\S]+)\s*\)$/i);
    if (notInMatch) {
      return { isNot: true, column: notInMatch[1], subquery: notInMatch[2].trim() };
    }

    // IN
    const inMatch = whereClause.match(/^(\w+(?:\.\w+)?)\s+IN\s*\(\s*(SELECT[\s\S]+)\s*\)$/i);
    if (inMatch) {
      return { isNot: false, column: inMatch[1], subquery: inMatch[2].trim() };
    }

    return null;
  }

  /**
   * Parse scalar comparison with subquery: col > (SELECT ...)
   */
  private parseScalarComparison(whereClause: string): { column: string; op: string; subquery: string } | null {
    const match = whereClause.match(/^(\w+(?:\.\w+)?)\s*(>=|<=|<>|!=|>|<|=)\s*\(\s*(SELECT[\s\S]+)\s*\)$/i);
    if (match) {
      return { column: match[1], op: match[2], subquery: match[3].trim() };
    }
    return null;
  }

  /**
   * Compare two values with given operator for subquery evaluation
   */
  private compareValuesForSubquery(left: SqlValue, right: SqlValue, op: string): boolean {
    if (left === null || right === null) return false;

    const numLeft = Number(left);
    const numRight = Number(right);

    switch (op) {
      case '=':
        return left === right || (!isNaN(numLeft) && !isNaN(numRight) && numLeft === numRight);
      case '!=':
      case '<>':
        return left !== right && (isNaN(numLeft) || isNaN(numRight) || numLeft !== numRight);
      case '>': return numLeft > numRight;
      case '<': return numLeft < numRight;
      case '>=': return numLeft >= numRight;
      case '<=': return numLeft <= numRight;
      default: return false;
    }
  }

  /**
   * Evaluate a HAVING clause against a group of rows
   */
  private evaluateHavingClause(
    havingClause: string,
    groupRows: Record<string, SqlValue>[],
    params: SqlValue[]
  ): boolean {
    // Parse HAVING condition: aggregate_fn op value
    // e.g., count(*) > 1, sum(amount) >= 100
    const trimmed = havingClause.trim();

    // Match: aggregate(arg) op value
    const match = trimmed.match(/^(.+?)\s*(>=|<=|<>|!=|>|<|=)\s*(.+)$/);
    if (match) {
      const leftExpr = match[1].trim();
      const op = match[2];
      const rightStr = match[3].trim();

      let leftVal: SqlValue;
      if (this.isAggregateFunction(leftExpr)) {
        leftVal = this.evaluateAggregate(leftExpr, groupRows, params);
      } else {
        leftVal = groupRows.length > 0 ? (groupRows[0][leftExpr] ?? null) : null;
      }

      let rightVal: SqlValue;
      if (/^-?\d+(\.\d+)?$/.test(rightStr)) {
        rightVal = Number(rightStr);
      } else if (rightStr.startsWith("'") && rightStr.endsWith("'")) {
        rightVal = rightStr.slice(1, -1);
      } else {
        rightVal = rightStr;
      }

      if (leftVal === null || rightVal === null) return false;
      const numL = Number(leftVal);
      const numR = Number(rightVal);

      switch (op) {
        case '=': return numL === numR;
        case '<>': case '!=': return numL !== numR;
        case '>': return numL > numR;
        case '<': return numL < numR;
        case '>=': return numL >= numR;
        case '<=': return numL <= numR;
        default: return false;
      }
    }

    return true; // No condition matched, pass through
  }

  /**
   * Check if expression is an aggregate function
   */
  private isAggregateFunction(expr: string): boolean {
    const trimmed = expr.trim().toUpperCase();
    return /^(COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT|TOTAL)\s*\(/i.test(trimmed);
  }

  /**
   * Evaluate an aggregate function over a set of rows
   */
  private evaluateAggregate(expr: string, rows: Record<string, SqlValue>[], params: SqlValue[] = []): SqlValue {
    // Match aggregate function with optional DISTINCT
    const match = expr.trim().match(/^(COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT|TOTAL)\s*\(\s*(DISTINCT\s+)?(.+?)\s*\)$/i);
    if (!match) return null;

    const [, fn, distinctMod, argWithSeparator] = match;
    const fnUpper = fn.toUpperCase();
    const isDistinct = !!distinctMod;

    // For GROUP_CONCAT, check for separator: GROUP_CONCAT(col, 'sep')
    let argTrimmed = argWithSeparator.trim();
    let separator = ','; // default separator

    if (fnUpper === 'GROUP_CONCAT') {
      // Check for custom separator
      const sepMatch = argTrimmed.match(/^(.+?),\s*'([^']*)'$/);
      if (sepMatch) {
        argTrimmed = sepMatch[1].trim();
        separator = sepMatch[2];
      }
    }

    // COUNT(*)
    if (fnUpper === 'COUNT' && argTrimmed === '*') {
      return rows.length;
    }

    // Extract column values - evaluate expressions if needed
    const pIdx = { value: 0 };
    const values: SqlValue[] = rows.map(row => {
      // Check if it's a simple column reference
      if (/^\w+$/.test(argTrimmed)) {
        return row[argTrimmed] ?? null;
      }
      // Check for table.column format
      if (/^\w+\.\w+$/.test(argTrimmed)) {
        return row[argTrimmed.split('.')[1]] ?? row[argTrimmed] ?? null;
      }
      // Otherwise evaluate as expression (arithmetic, CASE, etc.)
      return evaluateCaseExpr(argTrimmed, row, params, pIdx);
    });

    // Handle DISTINCT
    let processedValues = values;
    if (isDistinct) {
      const seen = new Set<string>();
      processedValues = values.filter(v => {
        const key = JSON.stringify(v);
        if (seen.has(key)) return false;
        seen.add(key);
        return true;
      });
    }

    // Filter out NULLs for most aggregates (except COUNT(*))
    const nonNullValues = processedValues.filter(v => v !== null);

    switch (fnUpper) {
      case 'COUNT':
        return nonNullValues.length;

      case 'SUM': {
        if (nonNullValues.length === 0) return null;
        return nonNullValues.reduce((sum, v) => sum + Number(v), 0);
      }

      case 'TOTAL': {
        // TOTAL is like SUM but returns 0.0 for empty set instead of NULL
        if (nonNullValues.length === 0) return 0.0;
        return nonNullValues.reduce((sum, v) => sum + Number(v), 0);
      }

      case 'AVG': {
        if (nonNullValues.length === 0) return null;
        const sum = nonNullValues.reduce((s, v) => s + Number(v), 0);
        return sum / nonNullValues.length;
      }

      case 'MIN': {
        if (nonNullValues.length === 0) return null;
        return nonNullValues.reduce((min, v) => {
          const numV = Number(v);
          const numMin = Number(min);
          if (!isNaN(numV) && !isNaN(numMin)) {
            return numV < numMin ? v : min;
          }
          return String(v) < String(min) ? v : min;
        });
      }

      case 'MAX': {
        if (nonNullValues.length === 0) return null;
        return nonNullValues.reduce((max, v) => {
          const numV = Number(v);
          const numMax = Number(max);
          if (!isNaN(numV) && !isNaN(numMax)) {
            return numV > numMax ? v : max;
          }
          return String(v) > String(max) ? v : max;
        });
      }

      case 'GROUP_CONCAT': {
        if (nonNullValues.length === 0) return null;
        return nonNullValues.map(v => String(v)).join(separator);
      }

      default:
        return null;
    }
  }

  /**
   * Parse a table reference: table AS alias, table alias, or just table
   * Supports both quoted ("table") and unquoted (table) table names
   */
  private parseTableReference(ref: string): { tableName: string; alias: string } {
    const trimmed = ref.trim();

    // Match: "tableName" AS alias (quoted table with explicit alias)
    const quotedAsMatch = trimmed.match(/^"([^"]+)"\s+AS\s+(\w+)$/i);
    if (quotedAsMatch) {
      return { tableName: quotedAsMatch[1], alias: quotedAsMatch[2] };
    }

    // Match: "tableName" alias (quoted table with implicit alias)
    const quotedImplicitMatch = trimmed.match(/^"([^"]+)"\s+(\w+)$/);
    if (quotedImplicitMatch) {
      const possibleAlias = quotedImplicitMatch[2].toUpperCase();
      const keywords = ['WHERE', 'ORDER', 'LIMIT', 'GROUP', 'HAVING', 'JOIN', 'ON', 'AND', 'OR'];
      if (!keywords.includes(possibleAlias)) {
        return { tableName: quotedImplicitMatch[1], alias: quotedImplicitMatch[2] };
      }
    }

    // Match: "tableName" (just quoted table name)
    const quotedSimpleMatch = trimmed.match(/^"([^"]+)"$/);
    if (quotedSimpleMatch) {
      return { tableName: quotedSimpleMatch[1], alias: quotedSimpleMatch[1] };
    }

    // Match: tableName AS alias (unquoted table with explicit alias)
    const asMatch = trimmed.match(/^(\w+)\s+AS\s+(\w+)$/i);
    if (asMatch) {
      return { tableName: asMatch[1], alias: asMatch[2] };
    }
    // Match: tableName alias (unquoted table with implicit alias, no AS keyword)
    const implicitMatch = trimmed.match(/^(\w+)\s+(\w+)$/);
    if (implicitMatch) {
      // Make sure the second word is not a keyword
      const possibleAlias = implicitMatch[2].toUpperCase();
      const keywords = ['WHERE', 'ORDER', 'LIMIT', 'GROUP', 'HAVING', 'JOIN', 'ON', 'AND', 'OR'];
      if (!keywords.includes(possibleAlias)) {
        return { tableName: implicitMatch[1], alias: implicitMatch[2] };
      }
    }
    // Just table name, use table name as alias too
    const simpleMatch = trimmed.match(/^(\w+)$/);
    if (simpleMatch) {
      return { tableName: simpleMatch[1], alias: simpleMatch[1] };
    }
    return { tableName: trimmed, alias: trimmed };
  }

  /**
   * Parse a SELECT statement with CASE expression support and table aliases
   */
  private parseSelectStatement(sql: string): {
    columnList: string;
    tableName: string;
    tableRefs: Array<{ tableName: string; alias: string }>;
    whereClause?: string;
    groupBy?: string[];
    havingClause?: string;
    orderBy?: string;
    limit?: number;
  } | null {
    const upper = sql.toUpperCase();
    if (!upper.trimStart().startsWith('SELECT')) return null;

    // Find SELECT start
    const selectStart = upper.indexOf('SELECT') + 6;

    // Find FROM using CASE-aware search
    const fromPos = findKeywordOutsideCaseAndStrings(sql, selectStart, 'FROM');
    if (fromPos === -1) {
      // Handle FROM-less SELECT (e.g., SELECT 1, SELECT (SELECT ...))
      const columnList = sql.slice(selectStart).trim();
      return {
        columnList,
        tableName: '',
        tableRefs: [],
        whereClause: undefined,
        groupBy: undefined,
        havingClause: undefined,
        orderBy: undefined,
        limit: undefined,
      };
    }

    const columnList = sql.slice(selectStart, fromPos).trim();

    // Find WHERE, GROUP BY, HAVING, ORDER BY, LIMIT using CASE-aware search
    const afterFromStart = fromPos + 4;
    const wherePos = findKeywordOutsideCaseAndStrings(sql, afterFromStart, 'WHERE');
    const groupPos = findKeywordOutsideCaseAndStrings(sql, afterFromStart, 'GROUP');
    const havingPos = findKeywordOutsideCaseAndStrings(sql, afterFromStart, 'HAVING');
    const orderPos = findKeywordOutsideCaseAndStrings(sql, afterFromStart, 'ORDER');
    const limitPos = findKeywordOutsideCaseAndStrings(sql, afterFromStart, 'LIMIT');

    // Determine where the FROM clause ends
    let fromClauseEnd = sql.length;
    if (wherePos !== -1) fromClauseEnd = Math.min(fromClauseEnd, wherePos);
    if (groupPos !== -1) fromClauseEnd = Math.min(fromClauseEnd, groupPos);
    if (havingPos !== -1) fromClauseEnd = Math.min(fromClauseEnd, havingPos);
    if (orderPos !== -1) fromClauseEnd = Math.min(fromClauseEnd, orderPos);
    if (limitPos !== -1) fromClauseEnd = Math.min(fromClauseEnd, limitPos);

    const fromClause = sql.slice(afterFromStart, fromClauseEnd).trim();

    // Split by comma to get individual table references
    const tableRefStrings = fromClause.split(',').map(s => s.trim());
    const tableRefs: Array<{ tableName: string; alias: string }> = [];

    for (const refStr of tableRefStrings) {
      if (refStr) {
        tableRefs.push(this.parseTableReference(refStr));
      }
    }

    if (tableRefs.length === 0) return null;

    // For backwards compatibility, tableName is the first table
    const tableName = tableRefs[0].tableName;

    let whereClause: string | undefined;
    let groupBy: string[] | undefined;
    let havingClause: string | undefined;
    let orderBy: string | undefined;
    let limit: number | undefined;

    // Extract WHERE clause
    if (wherePos !== -1) {
      const whereStart = wherePos + 5; // "WHERE".length
      let whereEnd = sql.length;
      if (groupPos !== -1 && groupPos > wherePos) whereEnd = Math.min(whereEnd, groupPos);
      if (havingPos !== -1 && havingPos > wherePos) whereEnd = Math.min(whereEnd, havingPos);
      if (orderPos !== -1 && orderPos > wherePos) whereEnd = Math.min(whereEnd, orderPos);
      if (limitPos !== -1 && limitPos > wherePos) whereEnd = Math.min(whereEnd, limitPos);
      whereClause = sql.slice(whereStart, whereEnd).trim();
    }

    // Extract GROUP BY clause
    if (groupPos !== -1) {
      const groupMatch = sql.slice(groupPos).match(/^GROUP\s+BY\s+(.+?)(?:\s+HAVING\s+|\s+ORDER\s+|\s+LIMIT\s+|\s*$)/i);
      if (groupMatch) {
        groupBy = groupMatch[1].split(',').map(c => c.trim());
      }
    }

    // Extract HAVING clause
    if (havingPos !== -1) {
      const havingStart = havingPos + 6; // "HAVING".length
      let havingEnd = sql.length;
      if (orderPos !== -1 && orderPos > havingPos) havingEnd = Math.min(havingEnd, orderPos);
      if (limitPos !== -1 && limitPos > havingPos) havingEnd = Math.min(havingEnd, limitPos);
      havingClause = sql.slice(havingStart, havingEnd).trim();
    }

    // Extract ORDER BY clause
    if (orderPos !== -1) {
      const orderMatch = sql.slice(orderPos).match(/^ORDER\s+BY\s+(.+?)(?:\s+LIMIT\s+|\s*$)/i);
      if (orderMatch) {
        orderBy = orderMatch[1].trim();
      }
    }

    // Extract LIMIT
    if (limitPos !== -1) {
      const limitMatch = sql.slice(limitPos).match(/^LIMIT\s+(\d+)/i);
      if (limitMatch) {
        limit = parseInt(limitMatch[1], 10);
      }
    }

    return { columnList, tableName, tableRefs, whereClause, groupBy, havingClause, orderBy, limit };
  }

  /**
   * Parse column expression and alias, respecting CASE...END blocks
   */
  private parseColumnAlias(col: string): { expr: string; alias: string } {
    // Find AS keyword outside CASE...END blocks
    // We need to search for ' AS ' pattern manually since findKeywordOutsideCaseAndStrings
    // expects just the keyword
    const asPos = this.findAsKeyword(col);

    if (asPos !== -1) {
      const expr = col.slice(0, asPos).trim();
      // Find where 'AS' actually starts (skip leading whitespace)
      const afterExpr = col.slice(asPos);
      const asMatch = afterExpr.match(/^\s+AS\s+(\w+)/i);
      if (asMatch) {
        return { expr, alias: asMatch[1] };
      }
    }

    // No AS found - use expression as alias (or extract simple column name)
    const trimmed = col.trim();
    if (/^\w+$/.test(trimmed)) {
      return { expr: trimmed, alias: trimmed };
    }

    // For complex expressions without alias, generate a default name
    return { expr: trimmed, alias: trimmed };
  }

  /**
   * Find AS keyword position outside CASE...END blocks, parentheses, and string literals
   */
  private findAsKeyword(col: string): number {
    let caseDepth = 0;
    let parenDepth = 0;
    let inString = false;
    const upper = col.toUpperCase();

    for (let i = 0; i < col.length; i++) {
      const ch = col[i];

      // Handle string literals
      if (ch === "'" && !inString) { inString = true; continue; }
      if (ch === "'" && inString) {
        if (col[i + 1] === "'") { i++; continue; }
        inString = false; continue;
      }
      if (inString) continue;

      // Track parentheses depth
      if (ch === '(') { parenDepth++; continue; }
      if (ch === ')') { parenDepth--; continue; }

      // Track CASE depth
      if (upper.slice(i).startsWith('CASE') && (i + 4 >= col.length || !/\w/.test(col[i + 4]))) {
        caseDepth++; i += 3; continue;
      }
      if (upper.slice(i).startsWith('END') && (i + 3 >= col.length || !/\w/.test(col[i + 3]))) {
        if (caseDepth > 0) caseDepth--;
        i += 2; continue;
      }

      // Look for ' AS ' pattern outside CASE blocks and parentheses (subqueries)
      if (caseDepth === 0 && parenDepth === 0 && upper.slice(i).match(/^\s+AS\s+/)) {
        return i;
      }
    }
    return -1;
  }

  private executeUpdate(sql: string, params: SqlValue[]): ExecutionResult {
    // Simple UPDATE parsing - support both quoted ("table") and unquoted (table) table names
    const match = sql.match(
      /UPDATE\s+(?:"([^"]+)"|(\w+))\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$/i
    );

    if (!match) {
      throw new StatementError(StatementErrorCode.INVALID_SQL, 'Invalid UPDATE syntax', sql);
    }

    // Table name is either in group 1 (quoted) or group 2 (unquoted)
    const tableName = match[1] || match[2];
    const setClause = match[3];
    const whereClause = match[4];

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
    // Simple DELETE parsing - support both quoted ("table") and unquoted (table) table names
    const match = sql.match(
      /DELETE\s+FROM\s+(?:"([^"]+)"|(\w+))(?:\s+WHERE\s+(.+))?$/i
    );

    if (!match) {
      throw new StatementError(StatementErrorCode.INVALID_SQL, 'Invalid DELETE syntax', sql);
    }

    // Table name is either in group 1 (quoted) or group 2 (unquoted)
    const tableName = match[1] || match[2];
    const whereClause = match[3];

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
    // Simple WHERE parsing (supports col = ?, col > ?, col NOT IN (values), etc.)
    const conditions = whereClause.split(/\s+AND\s+/i);

    // First, parse all conditions to determine parameter values
    type ParsedCondition =
      | { type: 'simple'; col: string; op: string; val: SqlValue }
      | { type: 'not_in'; col: string; values: SqlValue[]; hasNull: boolean };
    const parsedConditions: ParsedCondition[] = [];

    let paramIndex = startParamIndex;
    for (const cond of conditions) {
      // Check for literal NOT IN pattern (e.g., 1 NOT IN (2, 3), NULL NOT IN ())
      // Now includes NULL as a valid literal
      const literalNotInMatch = cond.match(
        /^(-?\d+(?:\.\d+)?|'[^']*'|NULL)\s+NOT\s+IN\s*\(([^)]*)\)$/i
      );
      if (literalNotInMatch) {
        const [, literalStr, valueList] = literalNotInMatch;
        // Parse the literal value, handling NULL, strings, and numbers
        let literalValue: SqlValue;
        if (literalStr.toUpperCase() === 'NULL') {
          literalValue = null;
        } else if (literalStr.startsWith("'")) {
          literalValue = literalStr.slice(1, -1);
        } else {
          literalValue = Number(literalStr);
        }
        const values: SqlValue[] = [];
        let hasNull = false;
        if (valueList.trim()) {
          const items = this.parseValueList(valueList, params, paramIndex);
          for (const item of items.values) {
            if (item === null) hasNull = true;
            values.push(item);
          }
          paramIndex = items.paramIndex;
        }
        // For literal NOT IN, evaluate immediately and skip if false
        // Per SQL standard (R-52275-55503): When the right operand is an empty set,
        // NOT IN returns TRUE regardless of the left operand (even if NULL)
        let conditionResult: boolean;
        if (values.length === 0) {
          // Empty list: NOT IN () is always TRUE, even for NULL
          conditionResult = true;
        } else if (literalValue === null || hasNull) {
          // NULL NOT IN non-empty list, or list contains NULL: result is NULL (falsy)
          conditionResult = false;
        } else {
          conditionResult = !values.some(v => this.valuesEqual(v, literalValue));
        }
        if (!conditionResult) {
          return { filtered: [], paramIndex };
        }
        continue;
      }

      // Check for NOT IN pattern: column NOT IN (value1, value2, ...)
      const notInMatch = cond.match(/^(\w+)\s+NOT\s+IN\s*\(([^)]*)\)$/i);
      if (notInMatch) {
        const [, col, valueList] = notInMatch;
        const values: SqlValue[] = [];
        let hasNull = false;
        if (valueList.trim()) {
          const items = this.parseValueList(valueList, params, paramIndex);
          for (const item of items.values) {
            if (item === null) hasNull = true;
            values.push(item);
          }
          paramIndex = items.paramIndex;
        }
        parsedConditions.push({ type: 'not_in', col, values, hasNull });
        continue;
      }

      // Standard comparison operators
      const match = cond.match(/^(\w+)\s*(>=|<=|<>|!=|>|<|=|LIKE|IS)\s*(.+)$/i);
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

      parsedConditions.push({ type: 'simple', col, op: op.toUpperCase(), val });
    }

    const filtered = rows.filter(row => {
      for (const cond of parsedConditions) {
        if (cond.type === 'not_in') {
          const rowVal = row[cond.col];
          // Per SQL standard (R-52275-55503): Empty list: NOT IN () is always TRUE
          if (cond.values.length === 0) {
            continue; // TRUE, proceed to next condition
          }
          // NULL NOT IN non-empty list returns NULL (falsy)
          if (rowVal === null) return false;
          // If list contains NULL, result is NULL (falsy) when value not found
          if (cond.hasNull) {
            // Check if value is in non-null values
            const nonNullValues = cond.values.filter(v => v !== null);
            if (nonNullValues.some(v => this.valuesEqual(v, rowVal))) {
              return false;
            }
            // Value not found but NULL in list, result is NULL (falsy)
            return false;
          }
          // Standard NOT IN check
          if (cond.values.some(v => this.valuesEqual(v, rowVal))) {
            return false;
          }
          continue;
        }

        // Handle simple conditions
        const { col, op, val } = cond;
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

  private parseValueList(
    valueList: string,
    params: SqlValue[],
    startParamIndex: number
  ): { values: SqlValue[]; paramIndex: number } {
    const values: SqlValue[] = [];
    let paramIndex = startParamIndex;

    // Parse comma-separated values, handling quoted strings
    const items: string[] = [];
    let current = '';
    let inQuote = false;
    let quoteChar = '';

    for (let i = 0; i < valueList.length; i++) {
      const char = valueList[i];
      if (!inQuote && (char === "'" || char === '"')) {
        inQuote = true;
        quoteChar = char;
        current += char;
      } else if (inQuote && char === quoteChar) {
        // Check for escaped quote
        if (i + 1 < valueList.length && valueList[i + 1] === quoteChar) {
          current += char + quoteChar;
          i++;
        } else {
          inQuote = false;
          current += char;
        }
      } else if (!inQuote && char === ',') {
        items.push(current.trim());
        current = '';
      } else {
        current += char;
      }
    }
    if (current.trim()) {
      items.push(current.trim());
    }

    for (const item of items) {
      if (item === '?') {
        values.push(params[paramIndex++]);
      } else if (
        (item.startsWith("'") && item.endsWith("'")) ||
        (item.startsWith('"') && item.endsWith('"'))
      ) {
        values.push(
          item
            .slice(1, -1)
            .replace(/''/g, "'")
            .replace(/""/g, '"')
        );
      } else if (item.toUpperCase() === 'NULL') {
        values.push(null);
      } else if (!isNaN(Number(item))) {
        values.push(Number(item));
      } else {
        values.push(item);
      }
    }

    return { values, paramIndex };
  }

  private valuesEqual(a: SqlValue, b: SqlValue): boolean {
    if (a === b) return true;
    if (a === null || b === null) return false;
    // Handle number comparisons with type coercion
    if (typeof a === 'number' && typeof b === 'number') {
      return a === b;
    }
    if (typeof a === 'number' || typeof b === 'number') {
      const numA = Number(a);
      const numB = Number(b);
      if (!isNaN(numA) && !isNaN(numB)) {
        return numA === numB;
      }
    }
    return String(a) === String(b);
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
