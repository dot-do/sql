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
  evaluateCaseExpr,
  splitSelectColumns,
  containsCaseExpression,
  findKeywordOutsideCaseAndStrings,
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
    // Try matching INSERT with explicit column list first
    // Support both quoted ("table") and unquoted (table) table names
    let match = sql.match(
      /INSERT\s+INTO\s+(?:"([^"]+)"|(\w+))\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/i
    );

    let tableName: string;
    let columnNames: string[];
    let valuePlaceholders: string[];

    if (match) {
      // INSERT INTO t (cols) VALUES (vals)
      // Table name is either in group 1 (quoted) or group 2 (unquoted)
      tableName = match[1] || match[2];
      columnNames = match[3].split(',').map(c => c.trim());
      valuePlaceholders = this.parseInsertValues(match[4]);
    } else {
      // Try matching INSERT without column list: INSERT INTO t VALUES (vals)
      // Support both quoted ("table") and unquoted (table) table names
      const noColMatch = sql.match(
        /INSERT\s+INTO\s+(?:"([^"]+)"|(\w+))\s+VALUES\s*\(([^)]+)\)/i
      );

      if (!noColMatch) {
        throw new StatementError(StatementErrorCode.INVALID_SQL, 'Invalid INSERT syntax', sql);
      }

      // Table name is either in group 1 (quoted) or group 2 (unquoted)
      tableName = noColMatch[1] || noColMatch[2];
      valuePlaceholders = this.parseInsertValues(noColMatch[3]);

      // Get column names from table schema
      const table = this.storage.tables.get(tableName);
      if (!table) {
        throw createTableNotFoundError(tableName, sql);
      }

      columnNames = table.columns.map(c => c.name);

      // Validate value count matches column count
      if (valuePlaceholders.length !== columnNames.length) {
        throw new StatementError(
          StatementErrorCode.INVALID_SQL,
          `INSERT has ${valuePlaceholders.length} values but table ${tableName} has ${columnNames.length} columns`,
          sql
        );
      }
    }

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
      } else if (placeholder.startsWith(':')) {
        // Named parameter - look up in params object
        // For named params, the binding module converts them to positional
        row[colName] = params[paramIndex++];
      } else {
        // Literal value - try to parse
        row[colName] = this.parseInsertLiteralValue(placeholder);
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

    // String literal
    if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
      return trimmed.slice(1, -1);
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
    // Parse SELECT with CASE expression support
    const parsed = this.parseSelectStatement(sql);
    if (!parsed) {
      throw new StatementError(StatementErrorCode.INVALID_SQL, 'Invalid SELECT syntax', sql);
    }

    const { columnList, tableName, tableRefs, whereClause, orderBy, limit } = parsed;

    // Validate all tables exist and build alias map
    const aliasToTable: Map<string, InMemoryTable> = new Map();
    for (const ref of tableRefs) {
      const table = this.storage.tables.get(ref.tableName);
      if (!table) {
        throw createTableNotFoundError(ref.tableName, sql);
      }
      aliasToTable.set(ref.alias, table);
    }

    // Build rows: if single table, use directly; if multiple, compute cross product
    let rows: Record<string, SqlValue>[];

    if (tableRefs.length === 1) {
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

    // Order rows
    if (orderBy) {
      rows = this.orderRowsWithAliases(rows, orderBy);
    }

    // Limit rows
    if (limit !== undefined) {
      rows = rows.slice(0, limit);
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

      if (hasAggregate) {
        // Compute aggregates over all rows and return single row
        const aggregateRow: Record<string, SqlValue> = {};
        for (const col of colDefs) {
          const aliasInfo = this.parseColumnAlias(col);
          const expr = aliasInfo.expr;
          const alias = aliasInfo.alias;

          if (this.isAggregateFunction(expr)) {
            aggregateRow[alias] = this.evaluateAggregate(expr, rows);
          } else {
            // For non-aggregate columns in aggregate query, resolve and take first row value
            const resolvedCol = this.resolveColumnName(expr.trim());
            aggregateRow[alias] = rows.length > 0 ? (rows[0][resolvedCol] ?? null) : null;
          }
        }
        rows = [aggregateRow];
      } else {
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
              const subquerySql = expr.slice(1, -1).trim(); // Remove outer parens
              const subqueryResult = this.execute(subquerySql, params);

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
              // Evaluate CASE or arithmetic expression (+ - * / % unary, functions like abs())
              // Only set if not already present (first column wins in collision)
              if (!(outputAlias in projected)) {
                projected[outputAlias] = evaluateCaseExpr(expr, row, params, pIdx);
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
   */
  private filterRowsWithAliases(
    rows: Record<string, SqlValue>[],
    whereClause: string,
    params: SqlValue[],
    aliasToTable: Map<string, InMemoryTable>
  ): Record<string, SqlValue>[] {
    // Split by AND (simple implementation)
    const conditions = whereClause.split(/\s+AND\s+/i);

    let paramIndex = 0;

    // Parse conditions
    type ParsedCondition =
      | { type: 'comparison'; leftCol: string; op: string; rightVal: SqlValue | { col: string } }
      | { type: 'not_in'; col: string; values: SqlValue[]; hasNull: boolean }
      | { type: 'literal_not_in'; result: boolean };
    const parsedConditions: ParsedCondition[] = [];

    for (const cond of conditions) {
      // Check for literal NOT IN pattern (e.g., 1.0 NOT IN (2.0))
      const literalNotInMatch = cond.match(
        /^(-?\d+(?:\.\d+)?|'[^']*')\s+NOT\s+IN\s*\(([^)]*)\)$/i
      );
      if (literalNotInMatch) {
        const [, literalStr, valueList] = literalNotInMatch;
        const literalValue: SqlValue = literalStr.startsWith("'")
          ? literalStr.slice(1, -1)
          : Number(literalStr);
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
        // For literal NOT IN, evaluate the condition once
        const conditionResult = hasNull
          ? false
          : values.length === 0
            ? true
            : !values.some(v => this.valuesEqual(v, literalValue));
        parsedConditions.push({ type: 'literal_not_in', result: conditionResult });
        continue;
      }

      // Check for NOT IN pattern: column NOT IN (value1, value2, ...)
      const notInMatch = cond.match(/^(\w+(?:\.\w+)?)\s+NOT\s+IN\s*\(([^)]*)\)$/i);
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

      // Standard comparison: col op val OR col op col (for joins)
      const match = cond.match(/^(\w+(?:\.\w+)?)\s*(>=|<=|<>|!=|>|<|=|LIKE|IS)\s*(.+)$/i);
      if (!match) continue;

      const [, leftCol, op, rightStr] = match;
      const rightTrimmed = rightStr.trim();

      // Check if right side is another column reference (alias.column)
      // Must start with a letter to be a column reference (not a number like 2.5)
      if (/^[a-zA-Z_]\w*\.[a-zA-Z_]\w*$/.test(rightTrimmed)) {
        parsedConditions.push({
          type: 'comparison',
          leftCol,
          op: op.toUpperCase(),
          rightVal: { col: rightTrimmed }
        });
      } else {
        // Parse as value
        let val: SqlValue;
        if (rightTrimmed === '?') {
          val = params[paramIndex++];
        } else if (rightTrimmed.startsWith("'") && rightTrimmed.endsWith("'")) {
          val = rightTrimmed.slice(1, -1);
        } else if (!isNaN(Number(rightTrimmed))) {
          val = Number(rightTrimmed);
        } else if (rightTrimmed.toUpperCase() === 'NULL') {
          val = null;
        } else {
          val = rightTrimmed;
        }
        parsedConditions.push({
          type: 'comparison',
          leftCol,
          op: op.toUpperCase(),
          rightVal: val
        });
      }
    }

    return rows.filter(row => {
      for (const cond of parsedConditions) {
        // Handle literal NOT IN - if condition is false, exclude all rows
        if (cond.type === 'literal_not_in') {
          if (!cond.result) return false;
          continue;
        }

        if (cond.type === 'not_in') {
          const rowVal = this.getColumnValue(row, cond.col);
          if (rowVal === null) return false;
          if (cond.hasNull) return false;
          if (cond.values.some(v => this.valuesEqual(v, rowVal))) return false;
          continue;
        }

        // comparison
        const leftVal = this.getColumnValue(row, cond.leftCol);
        const rightVal = (cond.rightVal !== null && typeof cond.rightVal === 'object' && 'col' in cond.rightVal)
          ? this.getColumnValue(row, cond.rightVal.col)
          : cond.rightVal;

        switch (cond.op) {
          case '=':
            if (!this.valuesEqual(leftVal, rightVal)) return false;
            break;
          case '!=':
          case '<>':
            if (this.valuesEqual(leftVal, rightVal)) return false;
            break;
          case '>':
            // NULL comparisons should return false (exclude the row)
            if (leftVal === null || rightVal === null) return false;
            if (!(Number(leftVal) > Number(rightVal))) return false;
            break;
          case '<':
            if (leftVal === null || rightVal === null) return false;
            if (!(Number(leftVal) < Number(rightVal))) return false;
            break;
          case '>=':
            if (leftVal === null || rightVal === null) return false;
            if (!(Number(leftVal) >= Number(rightVal))) return false;
            break;
          case '<=':
            if (leftVal === null || rightVal === null) return false;
            if (!(Number(leftVal) <= Number(rightVal))) return false;
            break;
          case 'IS':
            if (rightVal === null && leftVal !== null) return false;
            break;
          case 'LIKE':
            if (typeof leftVal === 'string' && typeof rightVal === 'string') {
              const regex = new RegExp(
                '^' + rightVal.replace(/%/g, '.*').replace(/_/g, '.') + '$',
                'i'
              );
              if (!regex.test(leftVal)) return false;
            }
            break;
        }
      }
      return true;
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
   */
  private orderRowsWithAliases(
    rows: Record<string, SqlValue>[],
    orderBy: string
  ): Record<string, SqlValue>[] {
    const parts = orderBy.split(',').map(p => {
      const match = p.trim().match(/^(\w+(?:\.\w+)?)(?:\s+(ASC|DESC))?$/i);
      if (match) {
        return { col: match[1], desc: match[2]?.toUpperCase() === 'DESC' };
      }
      return null;
    }).filter((p): p is { col: string; desc: boolean } => p !== null);

    return [...rows].sort((a, b) => {
      for (const { col, desc } of parts) {
        const aVal = this.getColumnValue(a, col);
        const bVal = this.getColumnValue(b, col);

        if (aVal === bVal) continue;
        if (aVal === null) return desc ? -1 : 1;
        if (bVal === null) return desc ? 1 : -1;

        const cmp = aVal < bVal ? -1 : 1;
        return desc ? -cmp : cmp;
      }
      return 0;
    });
  }

  // ==========================================================================
  // SUBQUERY SUPPORT
  // ==========================================================================

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
   * Check if expression contains arithmetic operators or function calls
   */
  private isArithmeticOrFunctionExpression(expr: string): boolean {
    // Check for arithmetic operators: + - * / %
    // Check for function calls: identifier followed by parenthesis like abs(...)
    return /[+\-*/%]/.test(expr) || /\w+\s*\(/.test(expr);
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

    // Check for EXISTS
    const existsMatch = this.parseExistsCondition(trimmed);
    if (existsMatch) {
      const subqueryResult = this.execute(existsMatch.subquery, params);
      const exists = subqueryResult.rows.length > 0;
      return existsMatch.isNot ? (exists ? [] : rows) : (exists ? rows : []);
    }

    // Check for IN with subquery
    const inMatch = this.parseInSubquery(trimmed);
    if (inMatch) {
      const subqueryResult = this.execute(inMatch.subquery, params);
      const values = new Set(subqueryResult.rows.map(r => {
        const keys = Object.keys(r);
        return keys.length > 0 ? r[keys[0]] : null;
      }));

      return rows.filter(row => {
        const col = inMatch.column.includes('.')
          ? inMatch.column.split('.')[1]
          : inMatch.column;
        const val = row[col];
        const matches = values.has(val);
        return inMatch.isNot ? !matches : matches;
      });
    }

    // Check for comparison with scalar subquery: col > (SELECT ...)
    const scalarCompMatch = this.parseScalarComparison(trimmed);
    if (scalarCompMatch) {
      const subqueryResult = this.execute(scalarCompMatch.subquery, params);
      let scalarValue: SqlValue = null;
      if (subqueryResult.rows.length > 0) {
        const firstRow = subqueryResult.rows[0];
        const keys = Object.keys(firstRow);
        if (keys.length > 0) {
          scalarValue = firstRow[keys[0]];
        }
      }

      return rows.filter(row => {
        const col = scalarCompMatch.column.includes('.')
          ? scalarCompMatch.column.split('.')[1]
          : scalarCompMatch.column;
        const rowVal = row[col];
        return this.compareValuesForSubquery(rowVal, scalarValue, scalarCompMatch.op);
      });
    }

    // Fallback to simple filtering
    const result = this.filterRows(rows, whereClause, params, 0);
    return result.filtered;
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
   * Check if expression is an aggregate function
   */
  private isAggregateFunction(expr: string): boolean {
    const trimmed = expr.trim().toUpperCase();
    return /^(COUNT|SUM|AVG|MIN|MAX)\s*\(/i.test(trimmed);
  }

  /**
   * Evaluate an aggregate function over a set of rows
   */
  private evaluateAggregate(expr: string, rows: Record<string, SqlValue>[]): SqlValue {
    const match = expr.trim().match(/^(COUNT|SUM|AVG|MIN|MAX)\s*\(\s*(.+?)\s*\)$/i);
    if (!match) return null;

    const [, fn, arg] = match;
    const fnUpper = fn.toUpperCase();
    const argTrimmed = arg.trim();

    // COUNT(*)
    if (fnUpper === 'COUNT' && argTrimmed === '*') {
      return rows.length;
    }

    // Extract column values
    const values: SqlValue[] = rows.map(row => {
      if (argTrimmed.includes('.')) {
        return row[argTrimmed.split('.')[1]] ?? row[argTrimmed] ?? null;
      }
      return row[argTrimmed] ?? null;
    });

    // Filter out NULLs for most aggregates (except COUNT(*))
    const nonNullValues = values.filter(v => v !== null);

    switch (fnUpper) {
      case 'COUNT':
        return nonNullValues.length;

      case 'SUM': {
        if (nonNullValues.length === 0) return null;
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
    orderBy?: string;
    limit?: number;
  } | null {
    const upper = sql.toUpperCase();
    if (!upper.trimStart().startsWith('SELECT')) return null;

    // Find SELECT start
    const selectStart = upper.indexOf('SELECT') + 6;

    // Find FROM using CASE-aware search
    const fromPos = findKeywordOutsideCaseAndStrings(sql, selectStart, 'FROM');
    if (fromPos === -1) return null;

    const columnList = sql.slice(selectStart, fromPos).trim();

    // Find WHERE, ORDER BY, LIMIT using CASE-aware search
    const afterFromStart = fromPos + 4;
    const wherePos = findKeywordOutsideCaseAndStrings(sql, afterFromStart, 'WHERE');
    const orderPos = findKeywordOutsideCaseAndStrings(sql, afterFromStart, 'ORDER');
    const limitPos = findKeywordOutsideCaseAndStrings(sql, afterFromStart, 'LIMIT');

    // Determine where the FROM clause ends
    let fromClauseEnd = sql.length;
    if (wherePos !== -1) fromClauseEnd = Math.min(fromClauseEnd, wherePos);
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
    let orderBy: string | undefined;
    let limit: number | undefined;

    // Extract WHERE clause
    if (wherePos !== -1) {
      const whereStart = wherePos + 5; // "WHERE".length
      let whereEnd = sql.length;
      if (orderPos !== -1 && orderPos > wherePos) whereEnd = Math.min(whereEnd, orderPos);
      if (limitPos !== -1 && limitPos > wherePos) whereEnd = Math.min(whereEnd, limitPos);
      whereClause = sql.slice(whereStart, whereEnd).trim();
    }

    // Extract ORDER BY clause
    if (orderPos !== -1) {
      const orderMatch = sql.slice(orderPos).match(/^ORDER\s+BY\s+(.+?)(?:\s+LIMIT\s+|$)/i);
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

    return { columnList, tableName, tableRefs, whereClause, orderBy, limit };
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
   * Find AS keyword position outside CASE...END blocks and string literals
   */
  private findAsKeyword(col: string): number {
    let caseDepth = 0;
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

      // Track CASE depth
      if (upper.slice(i).startsWith('CASE') && (i + 4 >= col.length || !/\w/.test(col[i + 4]))) {
        caseDepth++; i += 3; continue;
      }
      if (upper.slice(i).startsWith('END') && (i + 3 >= col.length || !/\w/.test(col[i + 3]))) {
        if (caseDepth > 0) caseDepth--;
        i += 2; continue;
      }

      // Look for ' AS ' pattern outside CASE blocks
      if (caseDepth === 0 && upper.slice(i).match(/^\s+AS\s+/)) {
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
      // Check for literal NOT IN pattern (e.g., 1 NOT IN (2, 3))
      const literalNotInMatch = cond.match(
        /^(-?\d+(?:\.\d+)?|'[^']*')\s+NOT\s+IN\s*\(([^)]*)\)$/i
      );
      if (literalNotInMatch) {
        const [, literalStr, valueList] = literalNotInMatch;
        const literalValue: SqlValue = literalStr.startsWith("'")
          ? literalStr.slice(1, -1)
          : Number(literalStr);
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
        const conditionResult = hasNull
          ? false
          : values.length === 0
            ? true
            : !values.some(v => this.valuesEqual(v, literalValue));
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
          // NULL NOT IN (...) always returns NULL (falsy)
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
