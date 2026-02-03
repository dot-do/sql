/**
 * Standalone Query Executor
 *
 * A SQL query executor that can operate independently of Durable Object infrastructure.
 * This module extracts the core query execution logic to enable:
 * - Easier testing without DO infrastructure
 * - Reusable execution logic
 * - Embedding in non-Worker environments
 *
 * @example Basic usage with in-memory storage
 * ```typescript
 * import { StandaloneExecutor, createMemoryStorage, createMemorySchemaManager } from 'dosql/execution';
 *
 * const storage = createMemoryStorage();
 * const schema = createMemorySchemaManager(storage);
 * const executor = new StandaloneExecutor({ storage, schema });
 *
 * // Create a table
 * await executor.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
 *
 * // Insert data
 * await executor.execute("INSERT INTO users (name) VALUES ('Alice')");
 *
 * // Query data
 * const result = await executor.execute('SELECT * FROM users');
 * console.log(result.rows); // [{ id: 1, name: 'Alice' }]
 * ```
 *
 * @packageDocumentation
 */

import type {
  KVStorage,
  SchemaProvider,
  SchemaManager,
  WALWriter,
  QueryResult,
  ExecutorConfig,
  TableSchema,
} from './types.js';

import {
  StatementError,
  StatementErrorCode,
  createTableNotFoundError,
  createUnsupportedSqlError,
} from '../errors/index.js';

// =============================================================================
// Helper Functions (copied from worker modules to avoid circular deps)
// =============================================================================

/**
 * Safely escape a value for embedding in a SQL string.
 */
function escapeSqlValue(value: unknown): string {
  if (value === null || value === undefined) {
    return 'NULL';
  }
  if (typeof value === 'number') {
    if (!isFinite(value)) {
      return 'NULL';
    }
    return String(value);
  }
  if (typeof value === 'boolean') {
    return value ? '1' : '0';
  }
  const str = String(value);
  return "'" + str.replace(/'/g, "''") + "'";
}

/**
 * Substitute named parameters in SQL
 */
function substituteParams(
  sql: string,
  params: Record<string, unknown>
): string {
  let result = '';
  let i = 0;

  while (i < sql.length) {
    // Handle string literals
    if (sql[i] === "'") {
      result += "'";
      i++;
      while (i < sql.length) {
        if (sql[i] === "'" && sql[i + 1] === "'") {
          result += "''";
          i += 2;
        } else if (sql[i] === "'") {
          result += "'";
          i++;
          break;
        } else {
          result += sql[i];
          i++;
        }
      }
      continue;
    }

    // Handle named parameters
    if (sql[i] === ':') {
      const paramMatch = sql.substring(i).match(/^:(\w+)/);
      if (paramMatch) {
        const paramName = paramMatch[1]!;
        if (paramName in params) {
          result += escapeSqlValue(params[paramName]);
          i += paramMatch[0].length;
          continue;
        }
      }
    }

    result += sql[i];
    i++;
  }

  return result;
}

/**
 * Parse a SQL value token
 */
function parseSqlValue(trimmed: string): unknown {
  if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
    return trimmed.slice(1, -1).replace(/''/g, "'");
  }
  const num = Number(trimmed);
  if (!isNaN(num) && trimmed !== '') return num;
  if (trimmed.toUpperCase() === 'NULL') return null;
  return trimmed;
}

/**
 * Parse RETURNING clause
 */
interface ReturningColumn {
  expression: string | '*';
  alias?: string;
}

interface ParsedReturning {
  columns: ReturningColumn[];
}

function parseReturningClause(sql: string): ParsedReturning | null {
  const match = sql.match(/\s+RETURNING\s+(.+?)(?:;?\s*$)/i);
  if (!match) return null;

  // match[1] is guaranteed to exist when match is not null (captured group)
  const columnsStr = (match[1] ?? '').trim();
  if (columnsStr === '*') {
    return { columns: [{ expression: '*' }] };
  }

  const columns: ReturningColumn[] = [];
  const parts = splitColumns(columnsStr);

  for (const part of parts) {
    const trimmed = part.trim();
    if (!trimmed) continue;

    const asMatch = trimmed.match(/^(.+?)\s+AS\s+(\w+)$/i);
    if (asMatch) {
      columns.push({
        expression: asMatch[1]!.trim(),
        alias: asMatch[2]!,
      });
    } else {
      columns.push({ expression: trimmed });
    }
  }

  return { columns };
}

function splitColumns(str: string): string[] {
  const result: string[] = [];
  let current = '';
  let depth = 0;

  for (const char of str) {
    if (char === '(') {
      depth++;
      current += char;
    } else if (char === ')') {
      depth--;
      current += char;
    } else if (char === ',' && depth === 0) {
      result.push(current);
      current = '';
    } else {
      current += char;
    }
  }

  if (current) {
    result.push(current);
  }

  return result;
}

function stripReturningClause(sql: string): string {
  return sql.replace(/\s+RETURNING\s+.+?(?:;?\s*$)/i, '');
}

/**
 * Evaluate an expression against a row
 */
function evaluateExpression(
  expr: string,
  row: Record<string, unknown>,
  _schemaColumns: string[]
): unknown {
  const trimmed = expr.trim();

  // String literals
  if (
    (trimmed.startsWith("'") && trimmed.endsWith("'")) ||
    (trimmed.startsWith('"') && trimmed.endsWith('"'))
  ) {
    return trimmed.slice(1, -1);
  }

  // Numeric literals
  const num = Number(trimmed);
  if (!isNaN(num) && trimmed !== '') {
    return num;
  }

  // NULL
  if (trimmed.toUpperCase() === 'NULL') {
    return null;
  }

  // rowid variants
  const lower = trimmed.toLowerCase();
  if (lower === 'rowid' || lower === '_rowid_' || lower === 'oid') {
    return row.id ?? row.rowid ?? row._rowid_;
  }

  // Column reference
  if (/^[\w.]+$/.test(trimmed)) {
    const colName = trimmed.includes('.') ? trimmed.split('.').pop()! : trimmed;
    return row[colName];
  }

  // Arithmetic
  const arithmeticMatch = trimmed.match(/^([\w.]+)\s*([+\-*/])\s*([\w.]+)$/);
  if (arithmeticMatch) {
    const left = evaluateExpression(arithmeticMatch[1]!, row, _schemaColumns);
    const op = arithmeticMatch[2]!;
    const right = evaluateExpression(arithmeticMatch[3]!, row, _schemaColumns);

    const leftNum = Number(left);
    const rightNum = Number(right);

    if (!isNaN(leftNum) && !isNaN(rightNum)) {
      switch (op) {
        case '+':
          return leftNum + rightNum;
        case '-':
          return leftNum - rightNum;
        case '*':
          return leftNum * rightNum;
        case '/':
          return rightNum !== 0 ? leftNum / rightNum : null;
      }
    }
    return null;
  }

  return row[trimmed];
}

/**
 * Apply RETURNING clause to rows
 */
function applyReturning(
  returning: ParsedReturning,
  rows: Record<string, unknown>[],
  schemaColumns: string[]
): Record<string, unknown>[] {
  return rows.map((row) => {
    const result: Record<string, unknown> = {};

    for (const col of returning.columns) {
      if (col.expression === '*') {
        for (const schemaCol of schemaColumns) {
          result[schemaCol] = row[schemaCol];
        }
        if ('id' in row && !schemaColumns.includes('id')) {
          result.id = row.id;
        }
      } else {
        const value = evaluateExpression(col.expression, row, schemaColumns);
        const outputName =
          col.alias ||
          (col.expression.includes('.')
            ? col.expression.split('.').pop()!
            : col.expression);
        result[outputName] = value;
      }
    }

    return result;
  });
}

// =============================================================================
// Type Guards
// =============================================================================

function isSchemaManager(
  provider: SchemaProvider | SchemaManager
): provider is SchemaManager {
  return (
    'createTable' in provider &&
    'dropTable' in provider &&
    'getNextId' in provider
  );
}

// =============================================================================
// Standalone Executor
// =============================================================================

/**
 * Standalone SQL query executor.
 *
 * Executes SQL queries against a pluggable storage backend without requiring
 * Durable Object infrastructure. Supports CREATE TABLE, INSERT, SELECT,
 * UPDATE, DELETE, REPLACE, and DROP TABLE statements.
 */
export class StandaloneExecutor {
  private storage: KVStorage;
  private schema: SchemaProvider | SchemaManager;
  private wal?: WALWriter;

  constructor(config: ExecutorConfig) {
    this.storage = config.storage;
    this.schema = config.schema;
    this.wal = config.wal;
  }

  /**
   * Execute a SQL statement
   */
  async execute(
    sql: string,
    params?: Record<string, unknown>
  ): Promise<QueryResult> {
    // Substitute parameters
    if (params) {
      sql = substituteParams(sql, params);
    }

    const normalized = sql.trim().toUpperCase();

    if (normalized.startsWith('CREATE TABLE')) {
      return this.executeCreateTable(sql);
    }

    if (normalized.startsWith('INSERT')) {
      return this.executeInsert(sql);
    }

    if (normalized.startsWith('SELECT')) {
      return this.executeSelect(sql);
    }

    if (normalized.startsWith('UPDATE')) {
      return this.executeUpdate(sql);
    }

    if (normalized.startsWith('DELETE')) {
      return this.executeDelete(sql);
    }

    if (normalized.startsWith('REPLACE')) {
      return this.executeReplace(sql);
    }

    if (normalized.startsWith('DROP TABLE')) {
      return this.executeDropTable(sql);
    }

    throw createUnsupportedSqlError(sql);
  }

  // ===========================================================================
  // CREATE TABLE
  // ===========================================================================

  private async executeCreateTable(sql: string): Promise<QueryResult> {
    if (!isSchemaManager(this.schema)) {
      throw new StatementError(
        StatementErrorCode.UNSUPPORTED,
        'Schema manager does not support table creation',
        sql
      );
    }

    const match = sql.match(/CREATE\s+TABLE\s+(\w+)\s*\(([\s\S]+)\)/i);
    if (!match) {
      throw new StatementError(
        StatementErrorCode.INVALID_SQL,
        'Invalid CREATE TABLE syntax',
        sql
      );
    }

    const tableName = match[1]!;
    const columnDefs = match[2]!;

    // Check if table already exists
    if (this.schema.getSchema(tableName)) {
      throw new StatementError(
        StatementErrorCode.TABLE_EXISTS,
        `Table ${tableName} already exists`,
        sql
      );
    }

    const columns: { name: string; type: string; defaultValue?: string }[] = [];
    let primaryKey = 'id';

    const parts = columnDefs.split(',').map((p) => p.trim());
    for (const part of parts) {
      const pkMatch = part.match(/PRIMARY\s+KEY\s*\((\w+)\)/i);
      if (pkMatch) {
        primaryKey = pkMatch[1]!;
        continue;
      }

      const colMatch = part.match(
        /(\w+)\s+(\w+)(?:\s+PRIMARY\s+KEY)?(?:\s+DEFAULT\s+(\w+|'[^']*'))?/i
      );
      if (colMatch) {
        const column: { name: string; type: string; defaultValue?: string } = {
          name: colMatch[1]!,
          type: colMatch[2]!,
        };
        if (colMatch[3]) {
          column.defaultValue = colMatch[3]!;
        }
        columns.push(column);

        if (/\bPRIMARY\s+KEY\b/i.test(part)) {
          primaryKey = column.name;
        }
      }
    }

    const tableSchema: TableSchema = { name: tableName, columns, primaryKey };
    await this.schema.createTable(tableSchema);

    return { rows: [], rowsAffected: 0 };
  }

  // ===========================================================================
  // DROP TABLE
  // ===========================================================================

  private async executeDropTable(sql: string): Promise<QueryResult> {
    if (!isSchemaManager(this.schema)) {
      throw new StatementError(
        StatementErrorCode.UNSUPPORTED,
        'Schema manager does not support table deletion',
        sql
      );
    }

    const match = sql.match(/DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(\w+)/i);
    if (!match) {
      throw new StatementError(
        StatementErrorCode.INVALID_SQL,
        'Invalid DROP TABLE syntax',
        sql
      );
    }

    const tableName = match[1]!;
    const ifExists = /IF\s+EXISTS/i.test(sql);

    const schema = this.schema.getSchema(tableName);
    if (!schema) {
      if (ifExists) {
        return { rows: [], rowsAffected: 0 };
      }
      throw createTableNotFoundError(tableName, sql);
    }

    // Delete all rows
    const prefix = `${tableName}:`;
    const keysToDelete: string[] = [];

    for await (const [key] of this.storage.range(prefix, prefix + '\uffff')) {
      keysToDelete.push(key);
    }

    for (const key of keysToDelete) {
      await this.storage.delete(key);
    }

    await this.schema.dropTable(tableName);

    // WAL entry
    if (this.wal) {
      await this.wal.append(
        {
          timestamp: Date.now(),
          txnId: `txn_${Date.now()}`,
          op: 'DELETE',
          table: tableName,
          before: new TextEncoder().encode(
            JSON.stringify({ _dropped: true, rowCount: keysToDelete.length })
          ),
        },
        { sync: true }
      );
    }

    return { rows: [], rowsAffected: keysToDelete.length };
  }

  // ===========================================================================
  // INSERT
  // ===========================================================================

  private async executeInsert(sql: string): Promise<QueryResult> {
    const returning = parseReturningClause(sql);
    const sqlWithoutReturning = returning ? stripReturningClause(sql) : sql;

    // Check for INSERT ... SELECT
    const insertSelectMatch = sqlWithoutReturning.match(
      /INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*SELECT\s+(.+)/i
    );
    if (insertSelectMatch) {
      return this.executeInsertSelect(sql, returning, insertSelectMatch);
    }

    const hasOnConflict = /\sON\s+CONFLICT\s/i.test(sqlWithoutReturning);

    const multiValueMatch = sqlWithoutReturning.match(
      /INSERT\s+INTO\s+(\w+)\s*(?:AS\s+\w+\s*)?\(([^)]+)\)\s*VALUES\s*(.+)$/i
    );

    if (!multiValueMatch) {
      throw new StatementError(
        StatementErrorCode.INVALID_SQL,
        'Invalid INSERT syntax',
        sql
      );
    }

    const tableName = multiValueMatch[1]!;
    const columnsStr = multiValueMatch[2]!;
    const columns = columnsStr.split(',').map((c) => c.trim());

    const schema = this.schema.getSchema(tableName);
    if (!schema) {
      throw createTableNotFoundError(tableName, sql);
    }

    const schemaColumns = schema.columns.map((c) => c.name);
    const insertedRows: Record<string, unknown>[] = [];
    let valuesStr = multiValueMatch[3]!;

    const onConflictIdx = valuesStr.toUpperCase().indexOf(' ON CONFLICT');
    if (onConflictIdx !== -1) {
      valuesStr = valuesStr.substring(0, onConflictIdx);
    }

    const valueMatches = valuesStr.matchAll(/\(([^)]+)\)/g);

    for (const valueMatch of valueMatches) {
      const values = valueMatch[1]!
        .split(',')
        .map((v) => parseSqlValue(v.trim()));

      const row: Record<string, unknown> = {};
      columns.forEach((col, i) => {
        row[col] = values[i];
      });

      // Apply DEFAULT values
      if (isSchemaManager(this.schema)) {
        for (const col of schema.columns) {
          if (!(col.name in row) && col.defaultValue) {
            row[col.name] = this.schema.evaluateDefaultValue(col.defaultValue);
          }
        }
      }

      // Auto-generate ID
      if (
        isSchemaManager(this.schema) &&
        (row[schema.primaryKey] === undefined || row[schema.primaryKey] === null)
      ) {
        const nextId = await this.schema.getNextId(tableName);
        row[schema.primaryKey] = nextId;
      }

      // Handle ON CONFLICT
      if (hasOnConflict) {
        const doNothingMatch = sqlWithoutReturning.match(
          /ON\s+CONFLICT\s+DO\s+NOTHING/i
        );
        const doUpdateMatch = sqlWithoutReturning.match(
          /ON\s+CONFLICT\s*(?:\([^)]+\))?\s*DO\s+UPDATE\s+SET\s+(.+?)(?:WHERE|$)/i
        );

        const key = `${tableName}:${String(row[schema.primaryKey])}`;
        const existingRow = await this.storage.get(key);

        if (existingRow) {
          if (doNothingMatch) {
            continue;
          } else if (doUpdateMatch) {
            const setClauseStr = doUpdateMatch[1]!;
            const setMatches = setClauseStr.matchAll(
              /(\w+)\s*=\s*(\w+(?:\s*[+\-*/]\s*\w+)?)/g
            );
            const updatedRow = { ...existingRow };

            for (const setMatch of setMatches) {
              const setCol = setMatch[1]!;
              const setExpr = setMatch[2]!;
              updatedRow[setCol] = evaluateExpression(
                setExpr,
                existingRow,
                schemaColumns
              );
            }

            await this.storage.set(key, updatedRow);
            insertedRows.push(updatedRow);
            continue;
          }
        }
      }

      const pkValue = row[schema.primaryKey];
      if (pkValue === undefined) {
        throw new StatementError(
          StatementErrorCode.CONSTRAINT_VIOLATION,
          `Primary key ${schema.primaryKey} is required`,
          sql
        );
      }

      const key = `${tableName}:${String(pkValue)}`;
      await this.storage.set(key, row);

      if (isSchemaManager(this.schema)) {
        this.schema.updateMaxIdCache(tableName, pkValue);
      }

      // WAL entry
      if (this.wal) {
        await this.wal.append(
          {
            timestamp: Date.now(),
            txnId: `txn_${Date.now()}`,
            op: 'INSERT',
            table: tableName,
            after: new TextEncoder().encode(JSON.stringify(row)),
          },
          { sync: true }
        );
      }

      insertedRows.push(row);
    }

    const resultRows = returning
      ? applyReturning(returning, insertedRows, schemaColumns)
      : [];

    return { rows: resultRows, rowsAffected: insertedRows.length };
  }

  private async executeInsertSelect(
    sql: string,
    returning: ParsedReturning | null,
    match: RegExpMatchArray
  ): Promise<QueryResult> {
    const tableName = match[1]!;
    const columnsStr = match[2]!;
    const selectPart = match[3]!;

    const columns = columnsStr.split(',').map((c) => c.trim());

    const schema = this.schema.getSchema(tableName);
    if (!schema) {
      throw createTableNotFoundError(tableName, sql);
    }

    const schemaColumns = schema.columns.map((c) => c.name);

    // Execute SELECT
    const selectSql = `SELECT ${selectPart}`;
    const selectResult = await this.executeSelect(selectSql);

    const insertedRows: Record<string, unknown>[] = [];

    for (const sourceRow of selectResult.rows) {
      const row: Record<string, unknown> = {};
      columns.forEach((col, i) => {
        const sourceKeys = Object.keys(sourceRow);
        if (i < sourceKeys.length) {
          row[col] = sourceRow[sourceKeys[i]!];
        }
      });

      // Apply DEFAULT values
      if (isSchemaManager(this.schema)) {
        for (const col of schema.columns) {
          if (!(col.name in row) && col.defaultValue) {
            row[col.name] = this.schema.evaluateDefaultValue(col.defaultValue);
          }
        }
      }

      // Auto-generate ID
      if (
        isSchemaManager(this.schema) &&
        (row[schema.primaryKey] === undefined || row[schema.primaryKey] === null)
      ) {
        const nextId = await this.schema.getNextId(tableName);
        row[schema.primaryKey] = nextId;
      }

      const pkValue = row[schema.primaryKey];
      const key = `${tableName}:${String(pkValue)}`;

      await this.storage.set(key, row);

      if (isSchemaManager(this.schema)) {
        this.schema.updateMaxIdCache(tableName, pkValue);
      }

      // WAL entry
      if (this.wal) {
        await this.wal.append(
          {
            timestamp: Date.now(),
            txnId: `txn_${Date.now()}`,
            op: 'INSERT',
            table: tableName,
            after: new TextEncoder().encode(JSON.stringify(row)),
          },
          { sync: true }
        );
      }

      insertedRows.push(row);
    }

    const resultRows = returning
      ? applyReturning(returning, insertedRows, schemaColumns)
      : [];

    return { rows: resultRows, rowsAffected: insertedRows.length };
  }

  // ===========================================================================
  // SELECT
  // ===========================================================================

  async executeSelect(sql: string): Promise<QueryResult> {
    const match = sql.match(
      /SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?/i
    );
    if (!match) {
      throw new StatementError(
        StatementErrorCode.INVALID_SQL,
        'Invalid SELECT syntax',
        sql
      );
    }

    const _selectCols = match[1]!;
    const tableName = match[2]!;
    const whereClause = match[3];

    const schema = this.schema.getSchema(tableName);
    if (!schema) {
      throw createTableNotFoundError(tableName, sql);
    }

    const schemaColumns = schema.columns.map((c) => c.name);
    const rows: Record<string, unknown>[] = [];
    const prefix = `${tableName}:`;

    for await (const [_key, value] of this.storage.range(
      prefix,
      prefix + '\uffff'
    )) {
      if (whereClause) {
        if (!this.evaluateWhereClause(whereClause, value, schemaColumns)) {
          continue;
        }
      }
      rows.push(value);
    }

    return { rows, rowsAffected: 0 };
  }

  // ===========================================================================
  // UPDATE
  // ===========================================================================

  private async executeUpdate(sql: string): Promise<QueryResult> {
    const returning = parseReturningClause(sql);
    const sqlWithoutReturning = returning ? stripReturningClause(sql) : sql;

    const orderByMatch = sqlWithoutReturning.match(
      /ORDER\s+BY\s+(\w+)\s+(ASC|DESC)?/i
    );
    const limitMatch = sqlWithoutReturning.match(/LIMIT\s+(\d+)/i);

    const match = sqlWithoutReturning.match(
      /UPDATE\s+(\w+)(?:\s+AS\s+\w+)?\s+SET\s+(.+?)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY|\s+LIMIT|$)/i
    );
    if (!match) {
      throw new StatementError(
        StatementErrorCode.INVALID_SQL,
        'Invalid UPDATE syntax',
        sql
      );
    }

    const tableName = match[1]!;
    let setClauseStr = match[2]!;
    const whereClause = match[3];

    const schema = this.schema.getSchema(tableName);
    if (!schema) {
      throw createTableNotFoundError(tableName, sql);
    }

    const schemaColumns = schema.columns.map((c) => c.name);

    const setUpdates: Array<{ col: string; expr: string }> = [];
    setClauseStr = setClauseStr.replace(/\s+(WHERE|ORDER|LIMIT).*/i, '');

    const setClauses = setClauseStr.split(',');
    for (const clause of setClauses) {
      const setMatch = clause.trim().match(/^([\w.]+)\s*=\s*(.+)$/);
      if (setMatch) {
        const colName = setMatch[1]!.includes('.')
          ? setMatch[1]!.split('.').pop()!
          : setMatch[1]!;
        setUpdates.push({
          col: colName,
          expr: setMatch[2]!.trim(),
        });
      }
    }

    if (setUpdates.length === 0) {
      throw new StatementError(
        StatementErrorCode.INVALID_SQL,
        'Invalid SET clause',
        sql
      );
    }

    const prefix = `${tableName}:`;
    const matchingRows: Array<{
      key: string;
      value: Record<string, unknown>;
    }> = [];

    for await (const [key, value] of this.storage.range(
      prefix,
      prefix + '\uffff'
    )) {
      if (whereClause) {
        if (!this.evaluateWhereClause(whereClause, value, schemaColumns)) {
          continue;
        }
      }
      matchingRows.push({ key, value: { ...value } });
    }

    // Apply ORDER BY
    if (orderByMatch) {
      const orderCol = orderByMatch[1]!;
      const orderDir = (orderByMatch[2] || 'ASC').toUpperCase();
      matchingRows.sort((a, b) => {
        const aVal = a.value[orderCol];
        const bVal = b.value[orderCol];
        if (aVal === bVal) return 0;
        if (aVal === null) return orderDir === 'ASC' ? 1 : -1;
        if (bVal === null) return orderDir === 'ASC' ? -1 : 1;
        const cmp = (aVal as number) < (bVal as number) ? -1 : 1;
        return orderDir === 'DESC' ? -cmp : cmp;
      });
    }

    // Apply LIMIT
    if (limitMatch) {
      const limit = parseInt(limitMatch[1]!, 10);
      matchingRows.splice(limit);
    }

    const updatedRows: Record<string, unknown>[] = [];

    for (const { key, value } of matchingRows) {
      const before = { ...value };

      for (const update of setUpdates) {
        const newValue = evaluateExpression(update.expr, value, schemaColumns);
        value[update.col] = newValue;
      }

      await this.storage.set(key, value);
      updatedRows.push(value);

      // WAL entry
      if (this.wal) {
        await this.wal.append(
          {
            timestamp: Date.now(),
            txnId: `txn_${Date.now()}`,
            op: 'UPDATE',
            table: tableName,
            before: new TextEncoder().encode(JSON.stringify(before)),
            after: new TextEncoder().encode(JSON.stringify(value)),
          },
          { sync: true }
        );
      }
    }

    const resultRows = returning
      ? applyReturning(returning, updatedRows, schemaColumns)
      : [];

    return { rows: resultRows, rowsAffected: updatedRows.length };
  }

  // ===========================================================================
  // DELETE
  // ===========================================================================

  private async executeDelete(sql: string): Promise<QueryResult> {
    const returning = parseReturningClause(sql);
    const sqlWithoutReturning = returning ? stripReturningClause(sql) : sql;

    const orderByMatch = sqlWithoutReturning.match(
      /ORDER\s+BY\s+(\w+)\s+(ASC|DESC)?/i
    );
    const limitMatch = sqlWithoutReturning.match(/LIMIT\s+(\d+)/i);

    const match = sqlWithoutReturning.match(
      /DELETE\s+FROM\s+(\w+)(?:\s+AS\s+\w+)?(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY|\s+LIMIT|$)/i
    );
    if (!match) {
      throw new StatementError(
        StatementErrorCode.INVALID_SQL,
        'Invalid DELETE syntax',
        sql
      );
    }

    const tableName = match[1]!;
    const whereClause = match[2];

    const schema = this.schema.getSchema(tableName);
    if (!schema) {
      throw createTableNotFoundError(tableName, sql);
    }

    const schemaColumns = schema.columns.map((c) => c.name);

    const prefix = `${tableName}:`;
    const matchingRows: Array<{
      key: string;
      value: Record<string, unknown>;
    }> = [];

    for await (const [key, value] of this.storage.range(
      prefix,
      prefix + '\uffff'
    )) {
      if (whereClause) {
        if (!this.evaluateWhereClause(whereClause, value, schemaColumns)) {
          continue;
        }
      }
      matchingRows.push({ key, value: { ...value } });
    }

    // Apply ORDER BY
    if (orderByMatch) {
      const orderCol = orderByMatch[1]!;
      const orderDir = (orderByMatch[2] || 'ASC').toUpperCase();
      matchingRows.sort((a, b) => {
        const aVal = a.value[orderCol];
        const bVal = b.value[orderCol];
        if (aVal === bVal) return 0;
        if (aVal === null) return orderDir === 'ASC' ? 1 : -1;
        if (bVal === null) return orderDir === 'ASC' ? -1 : 1;
        const cmp = (aVal as number) < (bVal as number) ? -1 : 1;
        return orderDir === 'DESC' ? -cmp : cmp;
      });
    }

    // Apply LIMIT
    if (limitMatch) {
      const limit = parseInt(limitMatch[1]!, 10);
      matchingRows.splice(limit);
    }

    const deletedRows = matchingRows.map((r) => r.value);

    for (const { key, value } of matchingRows) {
      await this.storage.delete(key);

      // WAL entry
      if (this.wal) {
        await this.wal.append(
          {
            timestamp: Date.now(),
            txnId: `txn_${Date.now()}`,
            op: 'DELETE',
            table: tableName,
            before: new TextEncoder().encode(JSON.stringify(value)),
          },
          { sync: true }
        );
      }
    }

    const resultRows = returning
      ? applyReturning(returning, deletedRows, schemaColumns)
      : [];

    return { rows: resultRows, rowsAffected: deletedRows.length };
  }

  // ===========================================================================
  // REPLACE
  // ===========================================================================

  private async executeReplace(sql: string): Promise<QueryResult> {
    const returning = parseReturningClause(sql);
    const sqlWithoutReturning = returning ? stripReturningClause(sql) : sql;

    const match = sqlWithoutReturning.match(
      /REPLACE\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*(.+)$/i
    );
    if (!match) {
      throw new StatementError(
        StatementErrorCode.INVALID_SQL,
        'Invalid REPLACE syntax',
        sql
      );
    }

    const tableName = match[1]!;
    const columnsStr = match[2]!;
    const columns = columnsStr.split(',').map((c) => c.trim());

    const schema = this.schema.getSchema(tableName);
    if (!schema) {
      throw createTableNotFoundError(tableName, sql);
    }

    const schemaColumns = schema.columns.map((c) => c.name);
    const insertedRows: Record<string, unknown>[] = [];
    const valuesStr = match[3]!;

    const valueMatches = valuesStr.matchAll(/\(([^)]+)\)/g);

    for (const valueMatch of valueMatches) {
      const values = valueMatch[1]!
        .split(',')
        .map((v) => parseSqlValue(v.trim()));

      const row: Record<string, unknown> = {};
      columns.forEach((col, i) => {
        row[col] = values[i];
      });

      // Auto-generate ID
      if (
        isSchemaManager(this.schema) &&
        (row[schema.primaryKey] === undefined || row[schema.primaryKey] === null)
      ) {
        const nextId = await this.schema.getNextId(tableName);
        row[schema.primaryKey] = nextId;
      }

      const pkValue = row[schema.primaryKey];
      const key = `${tableName}:${String(pkValue)}`;

      // Check and delete existing
      const existingRow = await this.storage.get(key);
      if (existingRow) {
        if (this.wal) {
          await this.wal.append(
            {
              timestamp: Date.now(),
              txnId: `txn_${Date.now()}`,
              op: 'DELETE',
              table: tableName,
              before: new TextEncoder().encode(JSON.stringify(existingRow)),
            },
            { sync: true }
          );
        }
      }

      await this.storage.set(key, row);

      if (isSchemaManager(this.schema)) {
        this.schema.updateMaxIdCache(tableName, pkValue);
      }

      // WAL entry
      if (this.wal) {
        await this.wal.append(
          {
            timestamp: Date.now(),
            txnId: `txn_${Date.now()}`,
            op: 'INSERT',
            table: tableName,
            after: new TextEncoder().encode(JSON.stringify(row)),
          },
          { sync: true }
        );
      }

      insertedRows.push(row);
    }

    const resultRows = returning
      ? applyReturning(returning, insertedRows, schemaColumns)
      : [];

    return { rows: resultRows, rowsAffected: insertedRows.length };
  }

  // ===========================================================================
  // WHERE Clause Evaluation
  // ===========================================================================

  private evaluateWhereClause(
    whereClause: string,
    row: Record<string, unknown>,
    schemaColumns: string[]
  ): boolean {
    const trimmed = whereClause.trim();

    // Handle parentheses
    if (trimmed.startsWith('(') && trimmed.endsWith(')')) {
      let depth = 0;
      let isOuter = true;
      for (let i = 0; i < trimmed.length; i++) {
        if (trimmed[i] === '(') depth++;
        else if (trimmed[i] === ')') depth--;
        if (depth === 0 && i < trimmed.length - 1) {
          isOuter = false;
          break;
        }
      }
      if (isOuter) {
        return this.evaluateWhereClause(
          trimmed.slice(1, -1),
          row,
          schemaColumns
        );
      }
    }

    // OR
    const orParts = this.splitByKeyword(trimmed, 'OR');
    if (orParts.length > 1) {
      return orParts.some((part) =>
        this.evaluateWhereClause(part, row, schemaColumns)
      );
    }

    // AND
    const andParts = this.splitByKeyword(trimmed, 'AND');
    if (andParts.length > 1) {
      return andParts.every((part) =>
        this.evaluateWhereClause(part, row, schemaColumns)
      );
    }

    // IN clause
    const inMatch = trimmed.match(/(\w+)\s+IN\s*\(([^)]+)\)/i);
    if (inMatch) {
      const col = inMatch[1]!;
      const values = inMatch[2]!.split(',').map((v) => {
        const val = v.trim();
        if (val.startsWith("'") && val.endsWith("'")) {
          return val.slice(1, -1);
        }
        const num = Number(val);
        return !isNaN(num) ? num : val;
      });
      const rowVal = row[col];
      return values.some((v) => v == rowVal);
    }

    // Comparison operators
    const compMatch = trimmed.match(/(\w+)\s*(=|!=|<>|<|<=|>|>=)\s*('?[^']*'?)/);
    if (compMatch) {
      const col = compMatch[1]!;
      const op = compMatch[2]!;
      let val: unknown = compMatch[3]!.trim();

      if (typeof val === 'string') {
        val = parseSqlValue(val);
      }

      const rowVal = row[col];

      switch (op) {
        case '=':
          return rowVal == val;
        case '!=':
        case '<>':
          return rowVal != val;
        case '<':
          return Number(rowVal) < Number(val);
        case '<=':
          return Number(rowVal) <= Number(val);
        case '>':
          return Number(rowVal) > Number(val);
        case '>=':
          return Number(rowVal) >= Number(val);
      }
    }

    // Simple equality fallback
    const simpleMatch = trimmed.match(/(\w+)\s*=\s*'?([^']+)'?/);
    if (simpleMatch) {
      const filterCol = simpleMatch[1]!;
      const filterVal = simpleMatch[2]!;
      return String(row[filterCol]) === filterVal;
    }

    return true;
  }

  private splitByKeyword(str: string, keyword: string): string[] {
    const result: string[] = [];
    let depth = 0;
    const keywordRegex = new RegExp(`\\s+${keyword}\\s+`, 'gi');
    let lastIndex = 0;

    const positions: number[] = [];
    let match;
    while ((match = keywordRegex.exec(str)) !== null) {
      positions.push(match.index);
    }

    if (positions.length === 0) {
      return [str.trim()];
    }

    for (let i = 0; i < str.length; i++) {
      if (str[i] === '(') depth++;
      else if (str[i] === ')') depth--;

      if (depth === 0 && positions.includes(i)) {
        result.push(str.substring(lastIndex, i).trim());
        lastIndex = i + keyword.length + 2;
      }
    }

    if (lastIndex < str.length) {
      result.push(str.substring(lastIndex).trim());
    }

    return result.length > 0 ? result : [str.trim()];
  }
}

/**
 * Create a standalone executor with the given configuration
 */
export function createStandaloneExecutor(config: ExecutorConfig): StandaloneExecutor {
  return new StandaloneExecutor(config);
}
