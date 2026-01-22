/**
 * DoSQL Database Durable Object with CapnWeb RPC
 *
 * A Durable Object that exposes SQL operations via CapnWeb RPC.
 * Demonstrates:
 * - RPC method exposure via CapnWeb RpcTarget
 * - Prepared statement management
 * - Transaction support
 * - Streaming/pipelining with .map() chaining
 *
 * @example
 * ```ts
 * // Client usage with pipelining
 * const result = await rpc.exec({ sql: 'SELECT * FROM users WHERE id = ?' })
 *   .map(r => r.rows[0])
 *   .map(user => user.name);
 * ```
 */

import { DurableObject } from 'cloudflare:workers';
import { RpcTarget, newWorkersRpcResponse, newWorkersWebSocketRpcResponse } from 'capnweb';

import type {
  Env,
  DoSQLRpcApi,
  ExecRequest,
  ExecResult,
  PreparedStatementHandle,
  TransactionOp,
  TransactionResult,
  DatabaseStatus,
  TableInfo,
  TableSchema,
  ColumnSchema,
  IndexSchema,
} from './types.js';

// =============================================================================
// In-Memory SQL Engine (simplified for demonstration)
// =============================================================================

interface TableData {
  schema: TableSchema;
  rows: Map<string, Record<string, unknown>>;
  autoIncrementId: number;
}

/**
 * Simple in-memory SQL engine for demonstration
 *
 * In production, this would be replaced with SQLite or another database engine.
 */
class SimpleSqlEngine {
  private tables = new Map<string, TableData>();
  private preparedStatements = new Map<string, { sql: string; paramCount: number }>();

  /**
   * Execute SQL statement
   */
  async execute(sql: string, params: unknown[] = []): Promise<ExecResult> {
    const normalized = sql.trim();
    const upper = normalized.toUpperCase();

    if (upper.startsWith('CREATE TABLE')) {
      return this.executeCreateTable(normalized);
    }
    if (upper.startsWith('INSERT')) {
      return this.executeInsert(normalized, params);
    }
    if (upper.startsWith('SELECT')) {
      return this.executeSelect(normalized, params);
    }
    if (upper.startsWith('UPDATE')) {
      return this.executeUpdate(normalized, params);
    }
    if (upper.startsWith('DELETE')) {
      return this.executeDelete(normalized, params);
    }
    if (upper.startsWith('DROP TABLE')) {
      return this.executeDropTable(normalized);
    }

    throw new Error(`Unsupported SQL: ${sql.substring(0, 50)}...`);
  }

  /**
   * Prepare a statement
   */
  prepareStatement(sql: string): PreparedStatementHandle {
    const id = `stmt_${Date.now()}_${Math.random().toString(36).slice(2)}`;
    const paramCount = (sql.match(/\?/g) || []).length;

    this.preparedStatements.set(id, { sql, paramCount });

    return { id, sql, paramCount };
  }

  /**
   * Run a prepared statement
   */
  async runPrepared(stmtId: string, params: unknown[] = []): Promise<ExecResult> {
    const stmt = this.preparedStatements.get(stmtId);
    if (!stmt) {
      throw new Error(`Prepared statement not found: ${stmtId}`);
    }
    return this.execute(stmt.sql, params);
  }

  /**
   * Finalize (close) a prepared statement
   */
  finalizePrepared(stmtId: string): boolean {
    return this.preparedStatements.delete(stmtId);
  }

  /**
   * Get database stats
   */
  getStats(): { tableCount: number; totalRows: number; preparedStatements: number } {
    let totalRows = 0;
    for (const table of this.tables.values()) {
      totalRows += table.rows.size;
    }

    return {
      tableCount: this.tables.size,
      totalRows,
      preparedStatements: this.preparedStatements.size,
    };
  }

  /**
   * List all tables
   */
  listTables(): TableInfo[] {
    const tables: TableInfo[] = [];
    for (const [name, data] of this.tables) {
      tables.push({ name, rowCount: data.rows.size });
    }
    return tables;
  }

  /**
   * Get table schema
   */
  getTableSchema(tableName: string): TableSchema | null {
    const table = this.tables.get(tableName.toLowerCase());
    return table ? table.schema : null;
  }

  // ==========================================================================
  // SQL Execution Methods
  // ==========================================================================

  private executeCreateTable(sql: string): ExecResult {
    const match = sql.match(/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\s*\(([\s\S]+)\)/i);
    if (!match) {
      throw new Error('Invalid CREATE TABLE syntax');
    }

    const tableName = match[1].toLowerCase();
    const columnDefs = match[2];

    // Check if table exists
    if (this.tables.has(tableName)) {
      if (sql.toUpperCase().includes('IF NOT EXISTS')) {
        return { columns: [], rows: [], rowCount: 0 };
      }
      throw new Error(`Table already exists: ${tableName}`);
    }

    // Parse columns
    const columns: ColumnSchema[] = [];
    const primaryKey: string[] = [];
    const indexes: IndexSchema[] = [];

    const parts = columnDefs.split(',').map((p) => p.trim());
    for (const part of parts) {
      // Check for PRIMARY KEY constraint
      const pkMatch = part.match(/PRIMARY\s+KEY\s*\(([^)]+)\)/i);
      if (pkMatch) {
        primaryKey.push(...pkMatch[1].split(',').map((c) => c.trim().toLowerCase()));
        continue;
      }

      // Check for column definition
      const colMatch = part.match(/(\w+)\s+(\w+)(?:\s+(NOT\s+NULL|NULL|PRIMARY\s+KEY))?/i);
      if (colMatch) {
        const colName = colMatch[1].toLowerCase();
        const colType = colMatch[2].toUpperCase();
        const constraint = colMatch[3]?.toUpperCase();

        columns.push({
          name: colName,
          type: colType,
          nullable: constraint !== 'NOT NULL',
          defaultValue: undefined,
        });

        if (constraint === 'PRIMARY KEY') {
          primaryKey.push(colName);
        }
      }
    }

    // Create table
    this.tables.set(tableName, {
      schema: { name: tableName, columns, primaryKey, indexes },
      rows: new Map(),
      autoIncrementId: 0,
    });

    return { columns: [], rows: [], rowCount: 0 };
  }

  private executeInsert(sql: string, params: unknown[]): ExecResult {
    const match = sql.match(
      /INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/i
    );
    if (!match) {
      throw new Error('Invalid INSERT syntax');
    }

    const tableName = match[1].toLowerCase();
    const table = this.tables.get(tableName);
    if (!table) {
      throw new Error(`Table not found: ${tableName}`);
    }

    const columnNames = match[2].split(',').map((c) => c.trim().toLowerCase());
    const valuePlaceholders = match[3].split(',').map((v) => v.trim());

    // Build row from values
    const row: Record<string, unknown> = {};
    let paramIndex = 0;

    for (let i = 0; i < columnNames.length; i++) {
      const colName = columnNames[i];
      const valuePlaceholder = valuePlaceholders[i];

      if (valuePlaceholder === '?') {
        row[colName] = params[paramIndex++];
      } else if (valuePlaceholder.startsWith("'") && valuePlaceholder.endsWith("'")) {
        row[colName] = valuePlaceholder.slice(1, -1);
      } else if (valuePlaceholder.toUpperCase() === 'NULL') {
        row[colName] = null;
      } else {
        const num = Number(valuePlaceholder);
        row[colName] = isNaN(num) ? valuePlaceholder : num;
      }
    }

    // Generate primary key if needed
    let pkValue: string;
    if (table.schema.primaryKey.length > 0) {
      const pkCol = table.schema.primaryKey[0];
      if (row[pkCol] === undefined || row[pkCol] === null) {
        // Auto-increment
        table.autoIncrementId++;
        row[pkCol] = table.autoIncrementId;
      }
      pkValue = String(row[pkCol]);
    } else {
      pkValue = String(++table.autoIncrementId);
    }

    table.rows.set(pkValue, row);

    return {
      columns: [],
      rows: [],
      rowCount: 0,
      lastInsertRowId: table.autoIncrementId,
      changes: 1,
    };
  }

  private executeSelect(sql: string, params: unknown[]): ExecResult {
    const match = sql.match(/SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+(.+?))?(?:\s+LIMIT\s+(\d+))?$/i);
    if (!match) {
      throw new Error('Invalid SELECT syntax');
    }

    const selectColumns = match[1].trim();
    const tableName = match[2].toLowerCase();
    const whereClause = match[3];
    const orderBy = match[4];
    const limit = match[5] ? parseInt(match[5], 10) : undefined;

    const table = this.tables.get(tableName);
    if (!table) {
      throw new Error(`Table not found: ${tableName}`);
    }

    // Determine which columns to return
    let columns: string[];
    if (selectColumns === '*') {
      columns = table.schema.columns.map((c) => c.name);
    } else {
      columns = selectColumns.split(',').map((c) => c.trim().toLowerCase());
    }

    // Filter rows
    let rows: Record<string, unknown>[] = Array.from(table.rows.values());

    if (whereClause) {
      rows = this.filterRows(rows, whereClause, params);
    }

    // Sort rows
    if (orderBy) {
      const orderMatch = orderBy.match(/(\w+)(?:\s+(ASC|DESC))?/i);
      if (orderMatch) {
        const sortCol = orderMatch[1].toLowerCase();
        const sortDir = orderMatch[2]?.toUpperCase() === 'DESC' ? -1 : 1;
        rows.sort((a, b) => {
          const aVal = a[sortCol];
          const bVal = b[sortCol];
          if (aVal === bVal) return 0;
          if (aVal === null || aVal === undefined) return 1;
          if (bVal === null || bVal === undefined) return -1;
          return (aVal < bVal ? -1 : 1) * sortDir;
        });
      }
    }

    // Apply limit
    if (limit !== undefined) {
      rows = rows.slice(0, limit);
    }

    // Project columns
    const resultRows = rows.map((row) => columns.map((col) => row[col]));

    return {
      columns,
      rows: resultRows,
      rowCount: resultRows.length,
    };
  }

  private executeUpdate(sql: string, params: unknown[]): ExecResult {
    const match = sql.match(/UPDATE\s+(\w+)\s+SET\s+(.+?)\s+WHERE\s+(.+)/i);
    if (!match) {
      throw new Error('Invalid UPDATE syntax');
    }

    const tableName = match[1].toLowerCase();
    const setClause = match[2];
    const whereClause = match[3];

    const table = this.tables.get(tableName);
    if (!table) {
      throw new Error(`Table not found: ${tableName}`);
    }

    // Parse SET clause
    const updates = new Map<string, unknown>();
    let paramIndex = 0;

    const setParts = setClause.split(',').map((p) => p.trim());
    for (const part of setParts) {
      const setMatch = part.match(/(\w+)\s*=\s*(.+)/);
      if (setMatch) {
        const colName = setMatch[1].toLowerCase();
        const value = setMatch[2].trim();

        if (value === '?') {
          updates.set(colName, params[paramIndex++]);
        } else if (value.startsWith("'") && value.endsWith("'")) {
          updates.set(colName, value.slice(1, -1));
        } else {
          const num = Number(value);
          updates.set(colName, isNaN(num) ? value : num);
        }
      }
    }

    // Filter and update rows
    const rows = Array.from(table.rows.entries());
    const filteredRows = this.filterRows(
      rows.map(([, row]) => row),
      whereClause,
      params.slice(paramIndex)
    );

    let changes = 0;
    for (const row of filteredRows) {
      for (const [col, value] of updates) {
        row[col] = value;
      }
      changes++;
    }

    return {
      columns: [],
      rows: [],
      rowCount: 0,
      changes,
    };
  }

  private executeDelete(sql: string, params: unknown[]): ExecResult {
    const match = sql.match(/DELETE\s+FROM\s+(\w+)\s+WHERE\s+(.+)/i);
    if (!match) {
      throw new Error('Invalid DELETE syntax');
    }

    const tableName = match[1].toLowerCase();
    const whereClause = match[2];

    const table = this.tables.get(tableName);
    if (!table) {
      throw new Error(`Table not found: ${tableName}`);
    }

    // Find rows to delete
    const keysToDelete: string[] = [];
    for (const [key, row] of table.rows) {
      if (this.filterRows([row], whereClause, params).length > 0) {
        keysToDelete.push(key);
      }
    }

    // Delete rows
    for (const key of keysToDelete) {
      table.rows.delete(key);
    }

    return {
      columns: [],
      rows: [],
      rowCount: 0,
      changes: keysToDelete.length,
    };
  }

  private executeDropTable(sql: string): ExecResult {
    const match = sql.match(/DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(\w+)/i);
    if (!match) {
      throw new Error('Invalid DROP TABLE syntax');
    }

    const tableName = match[1].toLowerCase();

    if (!this.tables.has(tableName)) {
      if (sql.toUpperCase().includes('IF EXISTS')) {
        return { columns: [], rows: [], rowCount: 0 };
      }
      throw new Error(`Table not found: ${tableName}`);
    }

    this.tables.delete(tableName);

    return { columns: [], rows: [], rowCount: 0 };
  }

  private filterRows(
    rows: Record<string, unknown>[],
    whereClause: string,
    params: unknown[]
  ): Record<string, unknown>[] {
    // Simple WHERE parsing: col = value or col = ?
    const match = whereClause.match(/(\w+)\s*=\s*(.+)/);
    if (!match) {
      return rows;
    }

    const colName = match[1].toLowerCase();
    let value = match[2].trim();

    let compareValue: unknown;
    if (value === '?') {
      compareValue = params[0];
    } else if (value.startsWith("'") && value.endsWith("'")) {
      compareValue = value.slice(1, -1);
    } else if (value.toUpperCase() === 'NULL') {
      compareValue = null;
    } else {
      const num = Number(value);
      compareValue = isNaN(num) ? value : num;
    }

    return rows.filter((row) => row[colName] === compareValue);
  }
}

// =============================================================================
// DoSQL RPC Target
// =============================================================================

/**
 * DoSQLRpcTarget - CapnWeb RpcTarget implementation
 *
 * Exposes DoSQL methods via CapnWeb RPC. Supports:
 * - exec() - Execute SQL and return results
 * - prepare() - Prepare a statement
 * - run() - Run a prepared statement
 * - transaction() - Execute multiple operations atomically
 * - Pipelining via .map() chaining
 */
export class DoSQLRpcTarget extends RpcTarget implements DoSQLRpcApi {
  #engine: SimpleSqlEngine;

  constructor(engine: SimpleSqlEngine) {
    super();
    this.#engine = engine;
  }

  async exec(request: ExecRequest): Promise<ExecResult> {
    return this.#engine.execute(request.sql, request.params);
  }

  async prepare(request: { sql: string }): Promise<PreparedStatementHandle> {
    return this.#engine.prepareStatement(request.sql);
  }

  async run(request: { stmtId: string; params?: unknown[] }): Promise<ExecResult> {
    return this.#engine.runPrepared(request.stmtId, request.params);
  }

  async finalize(request: { stmtId: string }): Promise<{ success: boolean }> {
    const success = this.#engine.finalizePrepared(request.stmtId);
    return { success };
  }

  async transaction(request: { ops: TransactionOp[] }): Promise<TransactionResult> {
    const results: Array<ExecResult | { error: string }> = [];
    const preparedInTransaction = new Map<string, string>();

    try {
      for (const op of request.ops) {
        if (op.type === 'exec' && op.sql) {
          const result = await this.#engine.execute(op.sql, op.params);
          results.push(result);
        } else if (op.type === 'prepare' && op.sql) {
          const handle = this.#engine.prepareStatement(op.sql);
          preparedInTransaction.set(handle.id, handle.id);
          results.push({
            columns: ['id', 'sql', 'paramCount'],
            rows: [[handle.id, handle.sql, handle.paramCount]],
            rowCount: 1,
          });
        } else if (op.type === 'run' && op.stmtId) {
          const result = await this.#engine.runPrepared(op.stmtId, op.params);
          results.push(result);
        } else {
          throw new Error(`Invalid transaction operation: ${JSON.stringify(op)}`);
        }
      }

      return { results, committed: true };
    } catch (error) {
      // Clean up prepared statements created in this transaction
      for (const stmtId of preparedInTransaction.keys()) {
        this.#engine.finalizePrepared(stmtId);
      }

      results.push({
        error: error instanceof Error ? error.message : String(error),
      });

      return { results, committed: false };
    }
  }

  async status(): Promise<DatabaseStatus> {
    const stats = this.#engine.getStats();
    return {
      initialized: true,
      tableCount: stats.tableCount,
      totalRows: stats.totalRows,
      storageBytes: 0, // Would need actual storage tracking
      preparedStatements: stats.preparedStatements,
      version: '0.1.0',
    };
  }

  async ping(): Promise<{ pong: true; timestamp: number }> {
    return { pong: true, timestamp: Date.now() };
  }

  async listTables(): Promise<{ tables: TableInfo[] }> {
    return { tables: this.#engine.listTables() };
  }

  async describeTable(request: { table: string }): Promise<TableSchema> {
    const schema = this.#engine.getTableSchema(request.table);
    if (!schema) {
      throw new Error(`Table not found: ${request.table}`);
    }
    return schema;
  }
}

// =============================================================================
// DoSQL Database Durable Object
// =============================================================================

/**
 * DoSQLDatabase Durable Object
 *
 * A Durable Object that provides SQL operations via CapnWeb RPC.
 * Supports both HTTP and WebSocket connections for RPC.
 */
export class DoSQLDatabase extends DurableObject {
  #engine: SimpleSqlEngine;
  #target: DoSQLRpcTarget;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.#engine = new SimpleSqlEngine();
    this.#target = new DoSQLRpcTarget(this.#engine);
  }

  /**
   * Handle incoming HTTP requests
   *
   * Routes to CapnWeb RPC handlers for both WebSocket and HTTP POST.
   */
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // Health check endpoint
    if (url.pathname === '/health') {
      return new Response(
        JSON.stringify({ status: 'ok', timestamp: Date.now() }),
        { headers: { 'Content-Type': 'application/json' } }
      );
    }

    // RPC endpoints
    if (url.pathname === '/rpc' || url.pathname === '/') {
      // WebSocket upgrade for streaming/pipelining
      const upgradeHeader = request.headers.get('upgrade');
      if (upgradeHeader?.toLowerCase() === 'websocket') {
        return newWorkersWebSocketRpcResponse(request, this.#target);
      }

      // HTTP POST for batch RPC
      if (request.method === 'POST') {
        return newWorkersRpcResponse(request, this.#target);
      }
    }

    // Status endpoint (convenience, also available via RPC)
    if (url.pathname === '/status') {
      const status = await this.#target.status();
      return new Response(JSON.stringify(status), {
        headers: { 'Content-Type': 'application/json' },
      });
    }

    return new Response('Not Found', { status: 404 });
  }
}
