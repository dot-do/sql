/**
 * DoSQL Durable Object Database
 *
 * A minimal Durable Object that uses DoSQL storage primitives:
 * - B-tree for row-oriented storage
 * - WAL for durability
 * - FSX backend for DO storage abstraction
 *
 * NOTE: This file uses direct imports to avoid pulling in heavy dependencies
 * like ai-evaluate, capnweb, iceberg-js that are not compatible with Workers.
 */

import { DurableObject } from 'cloudflare:workers';

// Direct imports to avoid transitive dependencies
import { createBTree, StringKeyCodec, JsonValueCodec, type BTree } from '../btree/index.js';
import { createDOBackend, type DOStorageBackend } from '../fsx/index.js';
import { createWALWriter, type WALWriter } from '../wal/index.js';

// =============================================================================
// RETURNING Clause Types and Helpers
// =============================================================================

interface ReturningColumn {
  expression: string | '*';
  alias?: string;
}

interface ParsedReturning {
  columns: ReturningColumn[];
}

/**
 * Parse RETURNING clause from SQL string
 */
function parseReturningClause(sql: string): ParsedReturning | null {
  const returningMatch = sql.match(/\s+RETURNING\s+(.+?)(?:;?\s*$)/i);
  if (!returningMatch) return null;

  const columnsStr = returningMatch[1].trim();
  const columns: ReturningColumn[] = [];

  // Handle RETURNING *
  if (columnsStr === '*') {
    return { columns: [{ expression: '*' }] };
  }

  // Parse column expressions with optional aliases
  // This is a simplified parser - handles basic cases:
  // - column_name
  // - column_name AS alias
  // - expression AS alias
  // - function(args) AS alias
  const parts = splitColumns(columnsStr);

  for (const part of parts) {
    const trimmed = part.trim();
    if (!trimmed) continue;

    // Check for AS alias
    const asMatch = trimmed.match(/^(.+?)\s+AS\s+(\w+)$/i);
    if (asMatch) {
      columns.push({
        expression: asMatch[1].trim(),
        alias: asMatch[2],
      });
    } else {
      columns.push({ expression: trimmed });
    }
  }

  return { columns };
}

/**
 * Split column list by commas, respecting parentheses
 */
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

/**
 * Evaluate an expression against a row
 */
function evaluateExpression(
  expr: string,
  row: Record<string, unknown>,
  schemaColumns: string[]
): unknown {
  const trimmed = expr.trim();

  // Handle string literals
  if ((trimmed.startsWith("'") && trimmed.endsWith("'")) ||
      (trimmed.startsWith('"') && trimmed.endsWith('"'))) {
    return trimmed.slice(1, -1);
  }

  // Handle numeric literals
  const num = Number(trimmed);
  if (!isNaN(num) && trimmed !== '') {
    return num;
  }

  // Handle NULL
  if (trimmed.toUpperCase() === 'NULL') {
    return null;
  }

  // Handle rowid/_rowid_/oid
  const lowerExpr = trimmed.toLowerCase();
  if (lowerExpr === 'rowid' || lowerExpr === '_rowid_' || lowerExpr === 'oid') {
    return row.id ?? row.rowid ?? row._rowid_;
  }

  // Handle column reference (possibly qualified: table.column)
  if (/^[\w.]+$/.test(trimmed)) {
    const colName = trimmed.includes('.') ? trimmed.split('.').pop()! : trimmed;
    return row[colName];
  }

  // Handle arithmetic expressions (e.g., price * quantity, price * 1.1)
  const arithmeticMatch = trimmed.match(/^([\w.]+)\s*([+\-*/])\s*([\w.]+)$/);
  if (arithmeticMatch) {
    const left = evaluateExpression(arithmeticMatch[1], row, schemaColumns);
    const op = arithmeticMatch[2];
    const right = evaluateExpression(arithmeticMatch[3], row, schemaColumns);

    const leftNum = Number(left);
    const rightNum = Number(right);

    if (!isNaN(leftNum) && !isNaN(rightNum)) {
      switch (op) {
        case '+': return leftNum + rightNum;
        case '-': return leftNum - rightNum;
        case '*': return leftNum * rightNum;
        case '/': return rightNum !== 0 ? leftNum / rightNum : null;
      }
    }
    return null;
  }

  // Handle string concatenation (col1 || ' ' || col2)
  if (trimmed.includes('||')) {
    const parts = trimmed.split('||').map((p) => p.trim());
    return parts.map((p) => {
      const val = evaluateExpression(p, row, schemaColumns);
      return val === null ? '' : String(val);
    }).join('');
  }

  // Handle function calls
  const funcMatch = trimmed.match(/^(\w+)\s*\((.+)\)$/);
  if (funcMatch) {
    const funcName = funcMatch[1].toUpperCase();
    const argsStr = funcMatch[2];

    // Parse function arguments
    const args = splitColumns(argsStr).map((a) =>
      evaluateExpression(a.trim(), row, schemaColumns)
    );

    return evaluateFunction(funcName, args, row, schemaColumns);
  }

  // Default: treat as column name
  return row[trimmed];
}

/**
 * Evaluate a SQL function
 */
function evaluateFunction(
  name: string,
  args: unknown[],
  row: Record<string, unknown>,
  schemaColumns: string[]
): unknown {
  switch (name) {
    case 'UPPER':
      return typeof args[0] === 'string' ? args[0].toUpperCase() : args[0];

    case 'LOWER':
      return typeof args[0] === 'string' ? args[0].toLowerCase() : args[0];

    case 'LENGTH':
      return typeof args[0] === 'string' ? args[0].length : null;

    case 'COALESCE':
      return args.find((a) => a !== null && a !== undefined) ?? null;

    case 'IFNULL':
      return args[0] !== null && args[0] !== undefined ? args[0] : args[1];

    case 'NULLIF':
      return args[0] === args[1] ? null : args[0];

    case 'SUBSTR':
    case 'SUBSTRING':
      if (typeof args[0] === 'string') {
        const start = Number(args[1]) - 1; // SQL is 1-indexed
        const len = args[2] !== undefined ? Number(args[2]) : undefined;
        return len !== undefined
          ? args[0].substring(start, start + len)
          : args[0].substring(start);
      }
      return null;

    case 'TRIM':
      return typeof args[0] === 'string' ? args[0].trim() : args[0];

    case 'ABS':
      return typeof args[0] === 'number' ? Math.abs(args[0]) : null;

    case 'ROUND':
      if (typeof args[0] === 'number') {
        const decimals = args[1] !== undefined ? Number(args[1]) : 0;
        const factor = Math.pow(10, decimals);
        return Math.round(args[0] * factor) / factor;
      }
      return null;

    case 'DATETIME':
      // Simplified: just return the value as-is or format date
      if (args[0] instanceof Date) {
        return args[0].toISOString().replace('T', ' ').split('.')[0];
      }
      return String(args[0]);

    case 'DATE':
      if (args[0] instanceof Date) {
        return args[0].toISOString().split('T')[0];
      }
      return String(args[0]);

    case 'TIME':
      if (args[0] instanceof Date) {
        return args[0].toISOString().split('T')[1].split('.')[0];
      }
      return String(args[0]);

    // Aggregate functions in RETURNING context should error
    case 'SUM':
    case 'COUNT':
    case 'AVG':
    case 'MIN':
    case 'MAX':
      throw new Error(`Aggregate function ${name} is not allowed in RETURNING clause`);

    default:
      // Unknown function - try to evaluate nested expressions
      return null;
  }
}

/**
 * Apply RETURNING clause to rows
 */
function applyReturning(
  returning: ParsedReturning,
  rows: Record<string, unknown>[],
  schemaColumns: string[]
): Record<string, unknown>[] {
  // Validate column references before processing
  for (const col of returning.columns) {
    if (col.expression !== '*') {
      validateColumnReference(col.expression, schemaColumns);
    }
  }

  return rows.map((row) => {
    const result: Record<string, unknown> = {};

    for (const col of returning.columns) {
      if (col.expression === '*') {
        // Return all columns
        for (const schemaCol of schemaColumns) {
          result[schemaCol] = row[schemaCol];
        }
        // Also include id/rowid if present
        if ('id' in row && !schemaColumns.includes('id')) {
          result.id = row.id;
        }
      } else {
        const value = evaluateExpression(col.expression, row, schemaColumns);
        const outputName = col.alias || getExpressionName(col.expression);
        result[outputName] = value;
      }
    }

    return result;
  });
}

/**
 * Validate column references in an expression
 * Throws error if non-existent column is referenced
 */
function validateColumnReference(expr: string, schemaColumns: string[]): void {
  const trimmed = expr.trim();

  // Skip literals (numbers, strings)
  if (!isNaN(Number(trimmed))) return;
  if ((trimmed.startsWith("'") && trimmed.endsWith("'")) ||
      (trimmed.startsWith('"') && trimmed.endsWith('"'))) return;
  if (trimmed.toUpperCase() === 'NULL') return;

  // Skip rowid variants
  const lower = trimmed.toLowerCase();
  if (lower === 'rowid' || lower === '_rowid_' || lower === 'oid') return;

  // Handle function calls - validate arguments
  const funcMatch = trimmed.match(/^(\w+)\s*\((.+)\)$/);
  if (funcMatch) {
    const argsStr = funcMatch[2];
    const args = splitColumns(argsStr);
    for (const arg of args) {
      validateColumnReference(arg.trim(), schemaColumns);
    }
    return;
  }

  // Handle arithmetic expressions
  const arithmeticMatch = trimmed.match(/^([\w.]+)\s*[+\-*/]\s*([\w.]+)$/);
  if (arithmeticMatch) {
    validateColumnReference(arithmeticMatch[1], schemaColumns);
    validateColumnReference(arithmeticMatch[2], schemaColumns);
    return;
  }

  // Handle string concatenation
  if (trimmed.includes('||')) {
    const parts = trimmed.split('||').map((p) => p.trim());
    for (const part of parts) {
      validateColumnReference(part, schemaColumns);
    }
    return;
  }

  // Plain column reference (possibly qualified)
  if (/^[\w.]+$/.test(trimmed)) {
    const colName = trimmed.includes('.') ? trimmed.split('.').pop()! : trimmed;

    // Check if column exists in schema (or is id/primary key)
    if (!schemaColumns.includes(colName) && colName !== 'id') {
      throw new Error(`no such column: ${colName}`);
    }
  }
}

/**
 * Get output column name for an expression
 */
function getExpressionName(expr: string): string {
  // For column references, return column name
  if (/^\w+$/.test(expr)) {
    return expr;
  }
  // For qualified references (table.column), return column name
  if (expr.includes('.') && /^[\w.]+$/.test(expr)) {
    return expr.split('.').pop()!;
  }
  // For expressions without alias, return the expression as-is
  return expr;
}

/**
 * Extract SQL without RETURNING clause
 */
function stripReturningClause(sql: string): string {
  return sql.replace(/\s+RETURNING\s+.+?(?:;?\s*$)/i, '');
}

// =============================================================================
// Types
// =============================================================================

export interface Env {
  DOSQL_DB: DurableObjectNamespace;
}

export interface QueryRequest {
  sql: string;
  params?: Record<string, unknown>;
}

export interface QueryResponse {
  success: boolean;
  rows?: Record<string, unknown>[];
  error?: string;
  stats?: {
    rowsAffected: number;
    executionTimeMs: number;
  };
}

export interface TableSchema {
  name: string;
  columns: { name: string; type: string; defaultValue?: string }[];
  primaryKey: string;
}

// =============================================================================
// DoSQL Database Durable Object
// =============================================================================

export class DoSQLDatabase extends DurableObject {
  private fsx: DOStorageBackend;
  private btree: BTree<string, Record<string, unknown>> | null = null;
  private wal: WALWriter | null = null;
  private initialized = false;
  private tables = new Map<string, TableSchema>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    // Create FSX backend using DO storage
    this.fsx = createDOBackend(ctx.storage);
  }

  /**
   * Initialize B-tree and WAL on first use
   */
  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return;

    // Create B-tree for data storage
    this.btree = createBTree(this.fsx, StringKeyCodec, JsonValueCodec);
    await this.btree.init();

    // Create WAL writer
    this.wal = createWALWriter(this.fsx);

    // Load table schemas from storage
    const schemaData = await this.fsx.read('_meta/schemas');
    if (schemaData) {
      const schemas = JSON.parse(new TextDecoder().decode(schemaData)) as TableSchema[];
      for (const schema of schemas) {
        this.tables.set(schema.name, schema);
      }
    }

    this.initialized = true;
  }

  /**
   * HTTP API handler
   */
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      await this.ensureInitialized();

      // Route requests
      if (path === '/query' && request.method === 'POST') {
        return this.handleQuery(request);
      }

      if (path === '/execute' && request.method === 'POST') {
        return this.handleExecute(request);
      }

      if (path === '/tables' && request.method === 'GET') {
        return this.handleListTables();
      }

      if (path === '/health' && request.method === 'GET') {
        return new Response(JSON.stringify({ status: 'ok', initialized: this.initialized }), {
          headers: { 'Content-Type': 'application/json' },
        });
      }

      // Not found
      return new Response(JSON.stringify({ error: 'Not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' },
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      return new Response(JSON.stringify({ error: message }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      });
    }
  }

  /**
   * Handle SELECT queries
   */
  private async handleQuery(request: Request): Promise<Response> {
    const body = (await request.json()) as QueryRequest;
    const startTime = performance.now();

    try {
      const result = await this.executeSQL(body.sql, body.params);
      const endTime = performance.now();

      const response: QueryResponse = {
        success: true,
        rows: result.rows,
        stats: {
          rowsAffected: result.rows.length,
          executionTimeMs: endTime - startTime,
        },
      };

      return new Response(JSON.stringify(response), {
        headers: { 'Content-Type': 'application/json' },
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      return new Response(JSON.stringify({ success: false, error: message }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }
  }

  /**
   * Handle INSERT/UPDATE/DELETE mutations
   */
  private async handleExecute(request: Request): Promise<Response> {
    const body = (await request.json()) as QueryRequest;
    const startTime = performance.now();

    try {
      const result = await this.executeSQL(body.sql, body.params);
      const endTime = performance.now();

      const response: QueryResponse = {
        success: true,
        rows: result.rows,
        stats: {
          rowsAffected: result.rowsAffected,
          executionTimeMs: endTime - startTime,
        },
      };

      return new Response(JSON.stringify(response), {
        headers: { 'Content-Type': 'application/json' },
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      return new Response(JSON.stringify({ success: false, error: message }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }
  }

  /**
   * List all tables
   */
  private async handleListTables(): Promise<Response> {
    const tables = Array.from(this.tables.values());
    return new Response(JSON.stringify({ tables }), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  /**
   * Simple SQL execution
   * This is a minimal parser for demonstration; real implementation would use full parser
   */
  private async executeSQL(
    sql: string,
    _params?: Record<string, unknown>
  ): Promise<{ rows: Record<string, unknown>[]; rowsAffected: number }> {
    const normalized = sql.trim().toUpperCase();

    if (!this.btree) {
      throw new Error('Database not initialized');
    }

    // CREATE TABLE
    if (normalized.startsWith('CREATE TABLE')) {
      return this.executeCreateTable(sql);
    }

    // INSERT
    if (normalized.startsWith('INSERT')) {
      return this.executeInsert(sql);
    }

    // SELECT
    if (normalized.startsWith('SELECT')) {
      return this.executeSelect(sql);
    }

    // UPDATE
    if (normalized.startsWith('UPDATE')) {
      return this.executeUpdate(sql);
    }

    // DELETE
    if (normalized.startsWith('DELETE')) {
      return this.executeDelete(sql);
    }

    // REPLACE
    if (normalized.startsWith('REPLACE')) {
      return this.executeReplace(sql);
    }

    throw new Error(`Unsupported SQL: ${sql.substring(0, 50)}...`);
  }

  /**
   * CREATE TABLE implementation
   */
  private async executeCreateTable(
    sql: string
  ): Promise<{ rows: Record<string, unknown>[]; rowsAffected: number }> {
    // Simple parser: CREATE TABLE name (col1 TYPE, col2 TYPE, PRIMARY KEY (col))
    const match = sql.match(/CREATE\s+TABLE\s+(\w+)\s*\(([\s\S]+)\)/i);
    if (!match) {
      throw new Error('Invalid CREATE TABLE syntax');
    }

    const tableName = match[1];
    const columnDefs = match[2];

    // Parse columns
    const columns: { name: string; type: string; defaultValue?: string }[] = [];
    let primaryKey = 'id';

    const parts = columnDefs.split(',').map((p) => p.trim());
    for (const part of parts) {
      // Check for standalone PRIMARY KEY constraint
      const pkMatch = part.match(/PRIMARY\s+KEY\s*\((\w+)\)/i);
      if (pkMatch) {
        primaryKey = pkMatch[1];
        continue;
      }

      // Parse column with optional DEFAULT and inline PRIMARY KEY
      // Format: column_name TYPE [PRIMARY KEY] [DEFAULT value]
      const colMatch = part.match(/(\w+)\s+(\w+)(?:\s+PRIMARY\s+KEY)?(?:\s+DEFAULT\s+(\w+|'[^']*'))?/i);
      if (colMatch) {
        const column: { name: string; type: string; defaultValue?: string } = {
          name: colMatch[1],
          type: colMatch[2],
        };
        if (colMatch[3]) {
          column.defaultValue = colMatch[3];
        }
        columns.push(column);

        // Check if this column has inline PRIMARY KEY
        if (/\bPRIMARY\s+KEY\b/i.test(part)) {
          primaryKey = column.name;
        }
      }
    }

    const schema: TableSchema = { name: tableName, columns, primaryKey };
    this.tables.set(tableName, schema);

    // Persist schemas
    await this.persistSchemas();

    return { rows: [], rowsAffected: 0 };
  }

  /**
   * INSERT implementation with RETURNING support
   */
  private async executeInsert(
    sql: string
  ): Promise<{ rows: Record<string, unknown>[]; rowsAffected: number }> {
    // Parse RETURNING clause first
    const returning = parseReturningClause(sql);
    const sqlWithoutReturning = returning ? stripReturningClause(sql) : sql;

    // Check if this is INSERT ... SELECT
    const insertSelectMatch = sqlWithoutReturning.match(
      /INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*SELECT\s+(.+)/i
    );
    if (insertSelectMatch) {
      return this.executeInsertSelect(sql, returning, insertSelectMatch);
    }

    // Check if ON CONFLICT is present
    const hasOnConflict = /\sON\s+CONFLICT\s/i.test(sqlWithoutReturning);

    // Simple parser: INSERT INTO table (cols) VALUES (vals), (vals2), ...
    // Also supports INSERT INTO table (cols) VALUES (vals)
    const multiValueMatch = sqlWithoutReturning.match(
      /INSERT\s+INTO\s+(\w+)\s*(?:AS\s+\w+\s*)?\(([^)]+)\)\s*VALUES\s*(.+)$/i
    );

    if (!multiValueMatch) {
      throw new Error('Invalid INSERT syntax');
    }

    const tableName = multiValueMatch[1];
    const columnsStr = multiValueMatch[2];
    const columns = columnsStr.split(',').map((c) => c.trim());

    const schema = this.tables.get(tableName);
    if (!schema) {
      throw new Error(`Table not found: ${tableName}`);
    }

    // Get schema columns for RETURNING *
    const schemaColumns = schema.columns.map((c) => c.name);

    // Parse all value rows
    const insertedRows: Record<string, unknown>[] = [];
    let valuesStr = multiValueMatch ? multiValueMatch[3] : onConflictMatch![3];

    // Remove ON CONFLICT clause from values string if present
    const onConflictIdx = valuesStr.toUpperCase().indexOf(' ON CONFLICT');
    if (onConflictIdx !== -1) {
      valuesStr = valuesStr.substring(0, onConflictIdx);
    }

    // Parse multiple value tuples: (val1, val2), (val3, val4), ...
    const valueMatches = valuesStr.matchAll(/\(([^)]+)\)/g);

    for (const valueMatch of valueMatches) {
      const values = valueMatch[1].split(',').map((v) => {
        const trimmed = v.trim();
        // Parse string literals
        if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
          return trimmed.slice(1, -1);
        }
        // Parse numbers
        const num = Number(trimmed);
        if (!isNaN(num) && trimmed !== '') return num;
        // Parse null
        if (trimmed.toUpperCase() === 'NULL') return null;
        return trimmed;
      });

      // Build row object
      const row: Record<string, unknown> = {};
      columns.forEach((col, i) => {
        row[col] = values[i];
      });

      // Apply DEFAULT values for columns not explicitly provided
      for (const col of schema.columns) {
        if (!(col.name in row) && col.defaultValue) {
          row[col.name] = this.evaluateDefaultValue(col.defaultValue);
        }
      }

      // Auto-generate ID if not provided and schema has an INTEGER PRIMARY KEY
      if (row[schema.primaryKey] === undefined || row[schema.primaryKey] === null) {
        // Generate next ID
        const nextId = await this.getNextId(tableName);
        row[schema.primaryKey] = nextId;
      }

      // Handle ON CONFLICT if present
      if (hasOnConflict) {
        const doNothingMatch = sqlWithoutReturning.match(/ON\s+CONFLICT\s+DO\s+NOTHING/i);
        const doUpdateMatch = sqlWithoutReturning.match(/ON\s+CONFLICT\s*(?:\([^)]+\))?\s*DO\s+UPDATE\s+SET\s+(.+?)(?:WHERE|$)/i);

        const key = `${tableName}:${String(row[schema.primaryKey])}`;
        const existingRow = await this.btree!.get(key);

        if (existingRow) {
          if (doNothingMatch) {
            // DO NOTHING - skip this row, don't add to insertedRows
            continue;
          } else if (doUpdateMatch) {
            // DO UPDATE - update the existing row
            const setClauseStr = doUpdateMatch[1];
            const setMatches = setClauseStr.matchAll(/(\w+)\s*=\s*(\w+(?:\s*[+\-*/]\s*\w+)?)/g);
            const updatedRow = { ...existingRow };

            for (const setMatch of setMatches) {
              const setCol = setMatch[1];
              const setExpr = setMatch[2];
              updatedRow[setCol] = evaluateExpression(setExpr, existingRow, schemaColumns);
            }

            await this.btree!.set(key, updatedRow);
            insertedRows.push(updatedRow);
            continue;
          }
        }
      }

      // Get primary key value
      const pkValue = row[schema.primaryKey];
      if (pkValue === undefined) {
        throw new Error(`Primary key ${schema.primaryKey} is required`);
      }

      // Write to B-tree
      const key = `${tableName}:${String(pkValue)}`;
      await this.btree!.set(key, row);

      // Write WAL entry
      if (this.wal) {
        await this.wal.append({
          timestamp: Date.now(),
          txnId: `txn_${Date.now()}`,
          op: 'INSERT',
          table: tableName,
          after: new TextEncoder().encode(JSON.stringify(row)),
        });
      }

      insertedRows.push(row);
    }

    if (this.wal) {
      await this.wal.flush();
    }

    // Apply RETURNING clause if present
    const resultRows = returning
      ? applyReturning(returning, insertedRows, schemaColumns)
      : [];

    return { rows: resultRows, rowsAffected: insertedRows.length };
  }

  /**
   * INSERT ... SELECT implementation with RETURNING support
   */
  private async executeInsertSelect(
    sql: string,
    returning: ParsedReturning | null,
    match: RegExpMatchArray
  ): Promise<{ rows: Record<string, unknown>[]; rowsAffected: number }> {
    const tableName = match[1];
    const columnsStr = match[2];
    const selectPart = match[3];

    const columns = columnsStr.split(',').map((c) => c.trim());

    const schema = this.tables.get(tableName);
    if (!schema) {
      throw new Error(`Table not found: ${tableName}`);
    }

    // Get schema columns for RETURNING *
    const schemaColumns = schema.columns.map((c) => c.name);

    // Execute the SELECT to get source rows
    const selectSql = `SELECT ${selectPart}`;
    const selectResult = await this.executeSelect(selectSql);

    // Insert each selected row
    const insertedRows: Record<string, unknown>[] = [];

    for (const sourceRow of selectResult.rows) {
      // Build row object from selected columns
      const row: Record<string, unknown> = {};
      columns.forEach((col, i) => {
        // Map selected columns to insert columns
        const sourceKeys = Object.keys(sourceRow);
        if (i < sourceKeys.length) {
          row[col] = sourceRow[sourceKeys[i]];
        }
      });

      // Apply DEFAULT values for columns not explicitly provided
      for (const col of schema.columns) {
        if (!(col.name in row) && col.defaultValue) {
          row[col.name] = this.evaluateDefaultValue(col.defaultValue);
        }
      }

      // Auto-generate ID if not provided
      if (row[schema.primaryKey] === undefined || row[schema.primaryKey] === null) {
        const nextId = await this.getNextId(tableName);
        row[schema.primaryKey] = nextId;
      }

      // Get primary key value
      const pkValue = row[schema.primaryKey];
      const key = `${tableName}:${String(pkValue)}`;

      // Write to B-tree
      await this.btree!.set(key, row);

      // Write WAL entry
      if (this.wal) {
        await this.wal.append({
          timestamp: Date.now(),
          txnId: `txn_${Date.now()}`,
          op: 'INSERT',
          table: tableName,
          after: new TextEncoder().encode(JSON.stringify(row)),
        });
      }

      insertedRows.push(row);
    }

    if (this.wal) {
      await this.wal.flush();
    }

    // Apply RETURNING clause if present
    const resultRows = returning
      ? applyReturning(returning, insertedRows, schemaColumns)
      : [];

    return { rows: resultRows, rowsAffected: insertedRows.length };
  }

  /**
   * Get next auto-increment ID for a table
   */
  private async getNextId(tableName: string): Promise<number> {
    let maxId = 0;
    const prefix = `${tableName}:`;

    for await (const [_key, value] of this.btree!.range(prefix, prefix + '\uffff')) {
      const id = Number(value.id);
      if (!isNaN(id) && id > maxId) {
        maxId = id;
      }
    }

    return maxId + 1;
  }

  /**
   * SELECT implementation
   */
  private async executeSelect(
    sql: string
  ): Promise<{ rows: Record<string, unknown>[]; rowsAffected: number }> {
    // Simple parser: SELECT * FROM table [WHERE col = value]
    const match = sql.match(/SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?/i);
    if (!match) {
      throw new Error('Invalid SELECT syntax');
    }

    const _selectCols = match[1];
    const tableName = match[2];
    const whereClause = match[3];

    const schema = this.tables.get(tableName);
    if (!schema) {
      throw new Error(`Table not found: ${tableName}`);
    }

    const rows: Record<string, unknown>[] = [];
    const prefix = `${tableName}:`;

    // Scan B-tree for matching rows
    for await (const [_key, value] of this.btree!.range(prefix, prefix + '\uffff')) {
      // Apply WHERE filter if present
      if (whereClause) {
        const filterMatch = whereClause.match(/(\w+)\s*=\s*'?([^']+)'?/);
        if (filterMatch) {
          const filterCol = filterMatch[1];
          const filterVal = filterMatch[2];
          if (String(value[filterCol]) !== filterVal) {
            continue;
          }
        }
      }
      rows.push(value);
    }

    return { rows, rowsAffected: 0 };
  }

  /**
   * UPDATE implementation with RETURNING support
   */
  private async executeUpdate(
    sql: string
  ): Promise<{ rows: Record<string, unknown>[]; rowsAffected: number }> {
    // Parse RETURNING clause first
    const returning = parseReturningClause(sql);
    const sqlWithoutReturning = returning ? stripReturningClause(sql) : sql;

    // Parse ORDER BY and LIMIT if present (before WHERE clause parsing)
    const orderByMatch = sqlWithoutReturning.match(/ORDER\s+BY\s+(\w+)\s+(ASC|DESC)?/i);
    const limitMatch = sqlWithoutReturning.match(/LIMIT\s+(\d+)/i);

    // Simple parser: UPDATE table SET col = value [WHERE col = value]
    // Also supports UPDATE table AS alias SET col = value WHERE col = value
    const match = sqlWithoutReturning.match(
      /UPDATE\s+(\w+)(?:\s+AS\s+\w+)?\s+SET\s+(.+?)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY|\s+LIMIT|$)/i
    );
    if (!match) {
      throw new Error('Invalid UPDATE syntax');
    }

    const tableName = match[1];
    let setClauseStr = match[2];
    const whereClause = match[3];

    const schema = this.tables.get(tableName);
    if (!schema) {
      throw new Error(`Table not found: ${tableName}`);
    }

    // Get schema columns for RETURNING *
    const schemaColumns = schema.columns.map((c) => c.name);

    // Parse multiple SET clauses (col1 = val1, col2 = val2)
    const setUpdates: Array<{ col: string; expr: string }> = [];

    // Remove any trailing WHERE/ORDER BY/LIMIT from SET clause
    setClauseStr = setClauseStr.replace(/\s+(WHERE|ORDER|LIMIT).*/i, '');

    const setClauses = setClauseStr.split(',');
    for (const clause of setClauses) {
      const setMatch = clause.trim().match(/^([\w.]+)\s*=\s*(.+)$/);
      if (setMatch) {
        const colName = setMatch[1].includes('.') ? setMatch[1].split('.').pop()! : setMatch[1];
        setUpdates.push({
          col: colName,
          expr: setMatch[2].trim(),
        });
      }
    }

    if (setUpdates.length === 0) {
      throw new Error('Invalid SET clause');
    }

    // Collect matching rows
    const prefix = `${tableName}:`;
    const matchingRows: Array<{ key: string; value: Record<string, unknown> }> = [];

    for await (const [key, value] of this.btree!.range(prefix, prefix + '\uffff')) {
      // Apply WHERE filter if present
      if (whereClause) {
        if (!this.evaluateWhereClause(whereClause, value, schemaColumns)) {
          continue;
        }
      }
      matchingRows.push({ key, value: { ...value } });
    }

    // Apply ORDER BY if present
    if (orderByMatch) {
      const orderCol = orderByMatch[1];
      const orderDir = (orderByMatch[2] || 'ASC').toUpperCase();
      matchingRows.sort((a, b) => {
        const aVal = a.value[orderCol];
        const bVal = b.value[orderCol];
        if (aVal === bVal) return 0;
        if (aVal === null) return orderDir === 'ASC' ? 1 : -1;
        if (bVal === null) return orderDir === 'ASC' ? -1 : 1;
        const cmp = aVal < bVal ? -1 : 1;
        return orderDir === 'DESC' ? -cmp : cmp;
      });
    }

    // Apply LIMIT if present
    if (limitMatch) {
      const limit = parseInt(limitMatch[1], 10);
      matchingRows.splice(limit);
    }

    // Update matching rows
    const updatedRows: Record<string, unknown>[] = [];

    for (const { key, value } of matchingRows) {
      const before = { ...value };

      // Apply SET updates
      for (const update of setUpdates) {
        const newValue = evaluateExpression(update.expr, value, schemaColumns);
        value[update.col] = newValue;
      }

      await this.btree!.set(key, value);
      updatedRows.push(value);

      // Write WAL entry
      if (this.wal) {
        await this.wal.append({
          timestamp: Date.now(),
          txnId: `txn_${Date.now()}`,
          op: 'UPDATE',
          table: tableName,
          before: new TextEncoder().encode(JSON.stringify(before)),
          after: new TextEncoder().encode(JSON.stringify(value)),
        });
      }
    }

    if (this.wal) {
      await this.wal.flush();
    }

    // Apply RETURNING clause if present
    const resultRows = returning
      ? applyReturning(returning, updatedRows, schemaColumns)
      : [];

    return { rows: resultRows, rowsAffected: updatedRows.length };
  }

  /**
   * Evaluate a WHERE clause against a row
   */
  private evaluateWhereClause(
    whereClause: string,
    row: Record<string, unknown>,
    schemaColumns: string[]
  ): boolean {
    const trimmed = whereClause.trim();

    // Handle parenthesized expressions first
    if (trimmed.startsWith('(') && trimmed.endsWith(')')) {
      // Check if it's a balanced outer parentheses
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
        return this.evaluateWhereClause(trimmed.slice(1, -1), row, schemaColumns);
      }
    }

    // Split by OR (lowest precedence), respecting parentheses
    const orParts = this.splitByKeyword(trimmed, 'OR');
    if (orParts.length > 1) {
      return orParts.some((part) => this.evaluateWhereClause(part, row, schemaColumns));
    }

    // Split by AND (higher precedence than OR), respecting parentheses
    const andParts = this.splitByKeyword(trimmed, 'AND');
    if (andParts.length > 1) {
      return andParts.every((part) => this.evaluateWhereClause(part, row, schemaColumns));
    }

    // Handle IN clause
    const inMatch = trimmed.match(/(\w+)\s+IN\s*\(([^)]+)\)/i);
    if (inMatch) {
      const col = inMatch[1];
      const values = inMatch[2].split(',').map((v) => {
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

    // Handle comparison operators
    const compMatch = trimmed.match(/(\w+)\s*(=|!=|<>|<|<=|>|>=)\s*('?[^']*'?)/);
    if (compMatch) {
      const col = compMatch[1];
      const op = compMatch[2];
      let val: unknown = compMatch[3].trim();

      // Parse value
      if (typeof val === 'string') {
        if (val.startsWith("'") && val.endsWith("'")) {
          val = val.slice(1, -1);
        } else {
          const num = Number(val);
          if (!isNaN(num)) val = num;
        }
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

    // Simple equality check (fallback)
    const simpleMatch = trimmed.match(/(\w+)\s*=\s*'?([^']+)'?/);
    if (simpleMatch) {
      const filterCol = simpleMatch[1];
      const filterVal = simpleMatch[2];
      return String(row[filterCol]) === filterVal;
    }

    return true;
  }

  /**
   * Split a string by keyword, respecting parentheses
   */
  private splitByKeyword(str: string, keyword: string): string[] {
    const result: string[] = [];
    let current = '';
    let depth = 0;
    const keywordRegex = new RegExp(`\\s+${keyword}\\s+`, 'gi');
    let lastIndex = 0;

    // Find all keyword positions
    const positions: number[] = [];
    let match;
    while ((match = keywordRegex.exec(str)) !== null) {
      positions.push(match.index);
    }

    if (positions.length === 0) {
      return [str.trim()];
    }

    // Check depth at each keyword position
    for (let i = 0; i < str.length; i++) {
      if (str[i] === '(') depth++;
      else if (str[i] === ')') depth--;

      if (depth === 0 && positions.includes(i)) {
        result.push(str.substring(lastIndex, i).trim());
        lastIndex = i + keyword.length + 2; // Skip keyword and surrounding spaces
      }
    }

    // Add remaining part
    if (lastIndex < str.length) {
      result.push(str.substring(lastIndex).trim());
    }

    return result.length > 0 ? result : [str.trim()];
  }

  /**
   * Evaluate a DEFAULT value expression
   */
  private evaluateDefaultValue(defaultValue: string): unknown {
    const upper = defaultValue.toUpperCase();

    // Handle CURRENT_TIMESTAMP and similar date/time functions
    if (upper === 'CURRENT_TIMESTAMP' || upper === 'DATETIME()' || upper === "DATETIME('NOW')") {
      return new Date().toISOString().replace('T', ' ').split('.')[0];
    }
    if (upper === 'CURRENT_DATE' || upper === 'DATE()' || upper === "DATE('NOW')") {
      return new Date().toISOString().split('T')[0];
    }
    if (upper === 'CURRENT_TIME' || upper === 'TIME()' || upper === "TIME('NOW')") {
      return new Date().toISOString().split('T')[1].split('.')[0];
    }

    // Handle NULL
    if (upper === 'NULL') {
      return null;
    }

    // Handle string literals
    if (defaultValue.startsWith("'") && defaultValue.endsWith("'")) {
      return defaultValue.slice(1, -1);
    }

    // Handle numbers
    const num = Number(defaultValue);
    if (!isNaN(num)) {
      return num;
    }

    // Return as-is for other cases
    return defaultValue;
  }

  /**
   * DELETE implementation with RETURNING support
   */
  private async executeDelete(
    sql: string
  ): Promise<{ rows: Record<string, unknown>[]; rowsAffected: number }> {
    // Parse RETURNING clause first
    const returning = parseReturningClause(sql);
    const sqlWithoutReturning = returning ? stripReturningClause(sql) : sql;

    // Parse ORDER BY and LIMIT if present
    const orderByMatch = sqlWithoutReturning.match(/ORDER\s+BY\s+(\w+)\s+(ASC|DESC)?/i);
    const limitMatch = sqlWithoutReturning.match(/LIMIT\s+(\d+)/i);

    // Simple parser: DELETE FROM table [AS alias] [WHERE col = value]
    const match = sqlWithoutReturning.match(
      /DELETE\s+FROM\s+(\w+)(?:\s+AS\s+\w+)?(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY|\s+LIMIT|$)/i
    );
    if (!match) {
      throw new Error('Invalid DELETE syntax');
    }

    const tableName = match[1];
    const whereClause = match[2];

    const schema = this.tables.get(tableName);
    if (!schema) {
      throw new Error(`Table not found: ${tableName}`);
    }

    // Get schema columns for RETURNING *
    const schemaColumns = schema.columns.map((c) => c.name);

    // Collect matching rows
    const prefix = `${tableName}:`;
    const matchingRows: Array<{ key: string; value: Record<string, unknown> }> = [];

    for await (const [key, value] of this.btree!.range(prefix, prefix + '\uffff')) {
      // Apply WHERE filter if present
      if (whereClause) {
        if (!this.evaluateWhereClause(whereClause, value, schemaColumns)) {
          continue;
        }
      }
      matchingRows.push({ key, value: { ...value } });
    }

    // Apply ORDER BY if present
    if (orderByMatch) {
      const orderCol = orderByMatch[1];
      const orderDir = (orderByMatch[2] || 'ASC').toUpperCase();
      matchingRows.sort((a, b) => {
        const aVal = a.value[orderCol];
        const bVal = b.value[orderCol];
        if (aVal === bVal) return 0;
        if (aVal === null) return orderDir === 'ASC' ? 1 : -1;
        if (bVal === null) return orderDir === 'ASC' ? -1 : 1;
        const cmp = aVal < bVal ? -1 : 1;
        return orderDir === 'DESC' ? -cmp : cmp;
      });
    }

    // Apply LIMIT if present
    if (limitMatch) {
      const limit = parseInt(limitMatch[1], 10);
      matchingRows.splice(limit);
    }

    // Capture rows for RETURNING before deletion
    const deletedRows = matchingRows.map((r) => r.value);

    // Delete the rows
    for (const { key, value } of matchingRows) {
      await this.btree!.delete(key);

      // Write WAL entry
      if (this.wal) {
        await this.wal.append({
          timestamp: Date.now(),
          txnId: `txn_${Date.now()}`,
          op: 'DELETE',
          table: tableName,
          before: new TextEncoder().encode(JSON.stringify(value)),
        });
      }
    }

    if (this.wal) {
      await this.wal.flush();
    }

    // Apply RETURNING clause if present
    const resultRows = returning
      ? applyReturning(returning, deletedRows, schemaColumns)
      : [];

    return { rows: resultRows, rowsAffected: deletedRows.length };
  }

  /**
   * REPLACE implementation with RETURNING support
   * REPLACE is INSERT OR REPLACE - it deletes conflicting rows and inserts new ones
   */
  private async executeReplace(
    sql: string
  ): Promise<{ rows: Record<string, unknown>[]; rowsAffected: number }> {
    // Parse RETURNING clause first
    const returning = parseReturningClause(sql);
    const sqlWithoutReturning = returning ? stripReturningClause(sql) : sql;

    // Simple parser: REPLACE INTO table (cols) VALUES (vals)
    const match = sqlWithoutReturning.match(
      /REPLACE\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*(.+)$/i
    );
    if (!match) {
      throw new Error('Invalid REPLACE syntax');
    }

    const tableName = match[1];
    const columnsStr = match[2];
    const columns = columnsStr.split(',').map((c) => c.trim());

    const schema = this.tables.get(tableName);
    if (!schema) {
      throw new Error(`Table not found: ${tableName}`);
    }

    // Get schema columns for RETURNING *
    const schemaColumns = schema.columns.map((c) => c.name);

    // Parse all value rows
    const insertedRows: Record<string, unknown>[] = [];
    const valuesStr = match[3];

    // Parse multiple value tuples: (val1, val2), (val3, val4), ...
    const valueMatches = valuesStr.matchAll(/\(([^)]+)\)/g);

    for (const valueMatch of valueMatches) {
      const values = valueMatch[1].split(',').map((v) => {
        const trimmed = v.trim();
        // Parse string literals
        if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
          return trimmed.slice(1, -1);
        }
        // Parse numbers
        const num = Number(trimmed);
        if (!isNaN(num) && trimmed !== '') return num;
        // Parse null
        if (trimmed.toUpperCase() === 'NULL') return null;
        return trimmed;
      });

      // Build row object
      const row: Record<string, unknown> = {};
      columns.forEach((col, i) => {
        row[col] = values[i];
      });

      // Auto-generate ID if not provided
      if (row[schema.primaryKey] === undefined || row[schema.primaryKey] === null) {
        const nextId = await this.getNextId(tableName);
        row[schema.primaryKey] = nextId;
      }

      const pkValue = row[schema.primaryKey];
      const key = `${tableName}:${String(pkValue)}`;

      // Check if row exists and delete it first (REPLACE behavior)
      const existingRow = await this.btree!.get(key);
      if (existingRow) {
        // Write WAL entry for delete
        if (this.wal) {
          await this.wal.append({
            timestamp: Date.now(),
            txnId: `txn_${Date.now()}`,
            op: 'DELETE',
            table: tableName,
            before: new TextEncoder().encode(JSON.stringify(existingRow)),
          });
        }
      }

      // Write to B-tree
      await this.btree!.set(key, row);

      // Write WAL entry for insert
      if (this.wal) {
        await this.wal.append({
          timestamp: Date.now(),
          txnId: `txn_${Date.now()}`,
          op: 'INSERT',
          table: tableName,
          after: new TextEncoder().encode(JSON.stringify(row)),
        });
      }

      insertedRows.push(row);
    }

    if (this.wal) {
      await this.wal.flush();
    }

    // Apply RETURNING clause if present
    const resultRows = returning
      ? applyReturning(returning, insertedRows, schemaColumns)
      : [];

    return { rows: resultRows, rowsAffected: insertedRows.length };
  }

  /**
   * Persist table schemas to storage
   */
  private async persistSchemas(): Promise<void> {
    const schemas = Array.from(this.tables.values());
    const data = new TextEncoder().encode(JSON.stringify(schemas));
    await this.fsx.write('_meta/schemas', data);
  }

  /**
   * Alarm handler for periodic tasks
   */
  async alarm(): Promise<void> {
    // Perform periodic maintenance
    await this.ensureInitialized();

    // Compact storage
    const stats = await this.fsx.getStats();
    console.log(`DoSQL alarm: ${stats.fileCount} files, ${stats.totalSize} bytes`);

    // Reschedule alarm for next hour
    const nextAlarm = Date.now() + 60 * 60 * 1000;
    this.ctx.storage.setAlarm(nextAlarm);
  }
}
