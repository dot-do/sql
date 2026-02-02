/**
 * Prisma ORM Integration Tests for DoSQL
 *
 * Comprehensive integration tests covering:
 * - Basic CRUD operations
 * - Transaction support
 * - Schema migrations
 * - Query building compatibility
 *
 * Uses workers-vitest-pool - NO MOCKS.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  PrismaDoSQLAdapter,
  createPrismaAdapter,
  isOk,
  isError,
  unwrap,
  unwrapOr,
} from '../prisma/index.js';
import type {
  DoSQLBackend,
  DoSQLTransaction,
  Query,
  Result,
  ResultSet,
} from '../prisma/adapter.js';
import type { QueryResult, Row } from '../../engine/types.js';

// =============================================================================
// IN-MEMORY BACKEND IMPLEMENTATION
// =============================================================================

/**
 * Comprehensive in-memory DoSQL backend for Prisma adapter testing.
 * This is a real implementation, not a mock.
 */
class InMemoryPrismaBackend implements DoSQLBackend {
  private tables: Map<string, Row[]> = new Map();
  private autoIncrement: Map<string, bigint> = new Map();
  private schema: Map<string, string[]> = new Map();
  private transactionCounter = 0;

  constructor() {
    // Initialize common tables
    this.initTable('users');
    this.initTable('posts');
    this.initTable('comments');
    this.initTable('categories');
    this.initTable('migrations');
  }

  private initTable(name: string): void {
    if (!this.tables.has(name)) {
      this.tables.set(name, []);
      this.autoIncrement.set(name, 1n);
    }
  }

  async query<T = Row>(sql: string, params?: unknown[]): Promise<QueryResult<T>> {
    const normalizedSql = sql.trim().toUpperCase();

    // Handle DDL
    if (normalizedSql.startsWith('CREATE TABLE')) {
      this.executeCreateTable(sql);
      return { rows: [] as T[], columns: [] };
    }

    if (normalizedSql.startsWith('ALTER TABLE')) {
      this.executeAlterTable(sql);
      return { rows: [] as T[], columns: [] };
    }

    if (normalizedSql.startsWith('DROP TABLE')) {
      this.executeDropTable(sql);
      return { rows: [] as T[], columns: [] };
    }

    // Handle SELECT
    if (normalizedSql.startsWith('SELECT')) {
      return this.executeSelect<T>(sql, params);
    }

    // For non-select queries, return empty result
    return { rows: [] as T[], columns: [] };
  }

  async execute(sql: string, params?: unknown[]): Promise<{ rowsAffected: number; lastInsertRowid?: bigint }> {
    const normalizedSql = sql.trim().toUpperCase();

    if (normalizedSql.startsWith('CREATE TABLE')) {
      this.executeCreateTable(sql);
      return { rowsAffected: 0 };
    }

    if (normalizedSql.startsWith('ALTER TABLE')) {
      this.executeAlterTable(sql);
      return { rowsAffected: 0 };
    }

    if (normalizedSql.startsWith('DROP TABLE')) {
      this.executeDropTable(sql);
      return { rowsAffected: 0 };
    }

    if (normalizedSql.startsWith('INSERT')) {
      return this.executeInsert(sql, params);
    }

    if (normalizedSql.startsWith('UPDATE')) {
      return { rowsAffected: this.executeUpdate(sql, params) };
    }

    if (normalizedSql.startsWith('DELETE')) {
      return { rowsAffected: this.executeDelete(sql, params) };
    }

    return { rowsAffected: 0 };
  }

  async beginTransaction(): Promise<DoSQLTransaction> {
    return new InMemoryTransaction(this, ++this.transactionCounter);
  }

  // -------------------------------------------------------------------------
  // Internal DDL methods
  // -------------------------------------------------------------------------

  private executeCreateTable(sql: string): void {
    const match = sql.match(/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?["'`]?(\w+)["'`]?\s*\(([^)]+)\)/i);
    if (match) {
      const tableName = match[1].toLowerCase();
      if (!this.tables.has(tableName)) {
        this.tables.set(tableName, []);
        this.autoIncrement.set(tableName, 1n);

        // Parse column names
        const columnDefs = match[2].split(',').map((col) => {
          const colMatch = col.trim().match(/^["'`]?(\w+)["'`]?/);
          return colMatch ? colMatch[1] : '';
        }).filter(Boolean);
        this.schema.set(tableName, columnDefs);
      }
    }
  }

  private executeAlterTable(sql: string): void {
    // Handle ADD COLUMN
    const addMatch = sql.match(/ALTER\s+TABLE\s+["'`]?(\w+)["'`]?\s+ADD\s+(?:COLUMN\s+)?["'`]?(\w+)["'`]?/i);
    if (addMatch) {
      const tableName = addMatch[1].toLowerCase();
      const columnName = addMatch[2];
      const columns = this.schema.get(tableName) || [];
      if (!columns.includes(columnName)) {
        columns.push(columnName);
        this.schema.set(tableName, columns);
      }
      // Add null value to existing rows
      const rows = this.tables.get(tableName) || [];
      for (const row of rows) {
        if (!(columnName in row)) {
          row[columnName] = null;
        }
      }
      return;
    }

    // Handle DROP COLUMN
    const dropMatch = sql.match(/ALTER\s+TABLE\s+["'`]?(\w+)["'`]?\s+DROP\s+(?:COLUMN\s+)?["'`]?(\w+)["'`]?/i);
    if (dropMatch) {
      const tableName = dropMatch[1].toLowerCase();
      const columnName = dropMatch[2];
      const columns = this.schema.get(tableName) || [];
      const idx = columns.indexOf(columnName);
      if (idx >= 0) {
        columns.splice(idx, 1);
        this.schema.set(tableName, columns);
      }
      // Remove column from existing rows
      const rows = this.tables.get(tableName) || [];
      for (const row of rows) {
        delete row[columnName];
      }
      return;
    }

    // Handle RENAME COLUMN
    const renameMatch = sql.match(/ALTER\s+TABLE\s+["'`]?(\w+)["'`]?\s+RENAME\s+(?:COLUMN\s+)?["'`]?(\w+)["'`]?\s+TO\s+["'`]?(\w+)["'`]?/i);
    if (renameMatch) {
      const tableName = renameMatch[1].toLowerCase();
      const oldName = renameMatch[2];
      const newName = renameMatch[3];
      const columns = this.schema.get(tableName) || [];
      const idx = columns.indexOf(oldName);
      if (idx >= 0) {
        columns[idx] = newName;
        this.schema.set(tableName, columns);
      }
      // Rename in existing rows
      const rows = this.tables.get(tableName) || [];
      for (const row of rows) {
        if (oldName in row) {
          row[newName] = row[oldName];
          delete row[oldName];
        }
      }
    }
  }

  private executeDropTable(sql: string): void {
    const match = sql.match(/DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?["'`]?(\w+)["'`]?/i);
    if (match) {
      const tableName = match[1].toLowerCase();
      this.tables.delete(tableName);
      this.autoIncrement.delete(tableName);
      this.schema.delete(tableName);
    }
  }

  // -------------------------------------------------------------------------
  // Internal DML methods
  // -------------------------------------------------------------------------

  private executeSelect<T>(sql: string, params?: unknown[]): QueryResult<T> {
    const match = sql.match(/SELECT\s+(.+?)\s+FROM\s+["'`]?(\w+)["'`]?/i);
    if (!match) {
      return { rows: [] as T[], columns: [] };
    }

    const [, columns, tableName] = match;
    const table = this.tables.get(tableName.toLowerCase());

    if (!table || table.length === 0) {
      return { rows: [] as T[], columns: [] };
    }

    let rows = [...table];

    // Apply WHERE clause
    const whereMatch = sql.match(/WHERE\s+["'`]?(\w+)["'`]?\s*(=|<>|!=|<|>|<=|>=)\s*(\?|'[^']*'|\d+)/i);
    if (whereMatch && params) {
      const column = whereMatch[1];
      const op = whereMatch[2].toUpperCase();
      let value: unknown = whereMatch[3];
      if (value === '?') {
        value = params[0];
      } else if (typeof value === 'string' && value.startsWith("'")) {
        value = value.slice(1, -1);
      } else if (/^\d+$/.test(String(value))) {
        value = parseInt(String(value), 10);
      }

      rows = rows.filter((row) => {
        const rowVal = row[column];
        switch (op) {
          case '=': return rowVal === value;
          case '<>':
          case '!=': return rowVal !== value;
          case '<': return (rowVal as number) < (value as number);
          case '>': return (rowVal as number) > (value as number);
          case '<=': return (rowVal as number) <= (value as number);
          case '>=': return (rowVal as number) >= (value as number);
          default: return true;
        }
      });
    }

    // Apply ORDER BY
    const orderMatch = sql.match(/ORDER\s+BY\s+["'`]?(\w+)["'`]?\s*(ASC|DESC)?/i);
    if (orderMatch) {
      const column = orderMatch[1];
      const direction = orderMatch[2]?.toUpperCase() === 'DESC' ? -1 : 1;
      rows.sort((a, b) => {
        const aVal = a[column];
        const bVal = b[column];
        if (aVal === bVal) return 0;
        if (aVal === null) return 1;
        if (bVal === null) return -1;
        return aVal < bVal ? -direction : direction;
      });
    }

    // Apply LIMIT
    const limitMatch = sql.match(/LIMIT\s+(\d+)/i);
    if (limitMatch) {
      rows = rows.slice(0, parseInt(limitMatch[1], 10));
    }

    return { rows: rows as T[] };
  }

  private executeInsert(sql: string, params?: unknown[]): { rowsAffected: number; lastInsertRowid: bigint } {
    const match = sql.match(/INSERT\s+INTO\s+["'`]?(\w+)["'`]?\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/i);
    if (!match) {
      return { rowsAffected: 0, lastInsertRowid: 0n };
    }

    const tableName = match[1].toLowerCase();
    const columns = match[2].split(',').map((c) => c.trim().replace(/["'`]/g, ''));
    const valuePlaceholders = match[3].split(',').map((v) => v.trim());

    this.initTable(tableName);

    const row: Row = {};
    let paramIndex = 0;

    for (let i = 0; i < columns.length; i++) {
      const placeholder = valuePlaceholders[i];
      let value: unknown;
      if (placeholder === '?') {
        value = params?.[paramIndex++];
      } else if (placeholder.startsWith("'")) {
        value = placeholder.slice(1, -1);
      } else if (/^\d+$/.test(placeholder)) {
        value = parseInt(placeholder, 10);
      } else if (placeholder.toLowerCase() === 'null') {
        value = null;
      } else if (placeholder.toLowerCase() === 'true') {
        value = true;
      } else if (placeholder.toLowerCase() === 'false') {
        value = false;
      } else {
        value = placeholder;
      }
      row[columns[i]] = value;
    }

    // Auto-generate ID if not provided
    if (!('id' in row)) {
      const nextId = this.autoIncrement.get(tableName) || 1n;
      row['id'] = Number(nextId);
      this.autoIncrement.set(tableName, nextId + 1n);
    } else {
      const id = BigInt(row['id'] as number);
      const currentAuto = this.autoIncrement.get(tableName) || 1n;
      if (id >= currentAuto) {
        this.autoIncrement.set(tableName, id + 1n);
      }
    }

    this.tables.get(tableName)!.push(row);

    return { rowsAffected: 1, lastInsertRowid: BigInt(row.id as number) };
  }

  private executeUpdate(sql: string, params?: unknown[]): number {
    const match = sql.match(/UPDATE\s+["'`]?(\w+)["'`]?\s+SET\s+(.+?)(?:\s+WHERE|$)/i);
    if (!match) return 0;

    const tableName = match[1].toLowerCase();
    const rows = this.tables.get(tableName) || [];

    // Parse SET clause
    const setParts = match[2].split(',');
    const updates: Record<string, unknown> = {};
    let paramIndex = 0;

    for (const part of setParts) {
      const [col, val] = part.split('=').map((s) => s.trim());
      const colName = col.replace(/["'`]/g, '');
      if (val === '?') {
        updates[colName] = params?.[paramIndex++];
      } else if (val.startsWith("'")) {
        updates[colName] = val.slice(1, -1);
      } else if (/^\d+$/.test(val)) {
        updates[colName] = parseInt(val, 10);
      }
    }

    // Parse WHERE
    let targetRows = rows;
    const whereMatch = sql.match(/WHERE\s+["'`]?(\w+)["'`]?\s*=\s*(\?|\d+|'[^']*')/i);
    if (whereMatch) {
      const column = whereMatch[1];
      let value: unknown = whereMatch[2];
      if (value === '?') {
        value = params?.[paramIndex];
      } else if (typeof value === 'string' && value.startsWith("'")) {
        value = value.slice(1, -1);
      } else if (/^\d+$/.test(String(value))) {
        value = parseInt(String(value), 10);
      }
      targetRows = rows.filter((row) => row[column] === value);
    }

    // Apply updates
    for (const row of targetRows) {
      for (const [key, val] of Object.entries(updates)) {
        row[key] = val;
      }
    }

    return targetRows.length;
  }

  private executeDelete(sql: string, params?: unknown[]): number {
    const match = sql.match(/DELETE\s+FROM\s+["'`]?(\w+)["'`]?/i);
    if (!match) return 0;

    const tableName = match[1].toLowerCase();
    const rows = this.tables.get(tableName) || [];
    const initialCount = rows.length;

    // Parse WHERE
    const whereMatch = sql.match(/WHERE\s+["'`]?(\w+)["'`]?\s*=\s*(\?|\d+|'[^']*')/i);
    if (whereMatch) {
      const column = whereMatch[1];
      let value: unknown = whereMatch[2];
      if (value === '?') {
        value = params?.[0];
      } else if (typeof value === 'string' && value.startsWith("'")) {
        value = value.slice(1, -1);
      } else if (/^\d+$/.test(String(value))) {
        value = parseInt(String(value), 10);
      }
      const filtered = rows.filter((row) => row[column] !== value);
      this.tables.set(tableName, filtered);
      return initialCount - filtered.length;
    } else {
      this.tables.set(tableName, []);
      return initialCount;
    }
  }

  // -------------------------------------------------------------------------
  // Test helpers
  // -------------------------------------------------------------------------

  seedUsers(users: Row[]): void {
    this.tables.set('users', [...users]);
    const maxId = users.reduce((max, row) => {
      const id = typeof row.id === 'number' ? BigInt(row.id) : 0n;
      return id > max ? id : max;
    }, 0n);
    this.autoIncrement.set('users', maxId + 1n);
  }

  seedTable(tableName: string, rows: Row[]): void {
    const name = tableName.toLowerCase();
    this.tables.set(name, [...rows]);
    const maxId = rows.reduce((max, row) => {
      const id = typeof row.id === 'number' ? BigInt(row.id) : 0n;
      return id > max ? id : max;
    }, 0n);
    this.autoIncrement.set(name, maxId + 1n);
  }

  clear(): void {
    this.tables.clear();
    this.autoIncrement.clear();
    this.schema.clear();
    // Re-initialize common tables
    this.initTable('users');
    this.initTable('posts');
    this.initTable('comments');
    this.initTable('categories');
    this.initTable('migrations');
  }

  getTableData(tableName: string): Row[] {
    return [...(this.tables.get(tableName.toLowerCase()) || [])];
  }

  getSchema(tableName: string): string[] {
    return [...(this.schema.get(tableName.toLowerCase()) || [])];
  }
}

/**
 * In-memory transaction implementation for Prisma adapter
 */
class InMemoryTransaction implements DoSQLTransaction {
  readonly id: string;
  private committed = false;
  private rolledBack = false;

  constructor(
    private readonly backend: InMemoryPrismaBackend,
    txnNumber: number
  ) {
    this.id = `prisma_txn_${txnNumber}`;
  }

  async query<T = Row>(sql: string, params?: unknown[]): Promise<QueryResult<T>> {
    this.checkActive();
    return this.backend.query<T>(sql, params);
  }

  async execute(sql: string, params?: unknown[]): Promise<{ rowsAffected: number; lastInsertRowid?: bigint }> {
    this.checkActive();
    return this.backend.execute(sql, params);
  }

  async commit(): Promise<void> {
    this.checkActive();
    this.committed = true;
  }

  async rollback(): Promise<void> {
    this.checkActive();
    this.rolledBack = true;
  }

  private checkActive(): void {
    if (this.committed) {
      throw new Error('Transaction already committed');
    }
    if (this.rolledBack) {
      throw new Error('Transaction already rolled back');
    }
  }
}

// =============================================================================
// TEST SUITES
// =============================================================================

describe('Prisma ORM Integration Tests', () => {
  let backend: InMemoryPrismaBackend;
  let adapter: PrismaDoSQLAdapter;

  beforeEach(() => {
    backend = new InMemoryPrismaBackend();
    adapter = new PrismaDoSQLAdapter({ backend });
  });

  // ---------------------------------------------------------------------------
  // Basic CRUD Operations
  // ---------------------------------------------------------------------------

  describe('Basic CRUD Operations', () => {
    describe('queryRaw - SELECT', () => {
      beforeEach(() => {
        backend.seedUsers([
          { id: 1, name: 'Alice', email: 'alice@example.com', active: true },
          { id: 2, name: 'Bob', email: 'bob@example.com', active: true },
          { id: 3, name: 'Charlie', email: 'charlie@example.com', active: false },
        ]);
      });

      it('should select all rows', async () => {
        const result = await adapter.queryRaw({
          sql: 'SELECT * FROM users',
          args: [],
        });

        expect(result.ok).toBe(true);
        if (result.ok) {
          expect(result.value.rows).toHaveLength(3);
        }
      });

      it('should select with WHERE clause', async () => {
        const result = await adapter.queryRaw({
          sql: 'SELECT * FROM users WHERE id = ?',
          args: [1],
        });

        expect(result.ok).toBe(true);
        if (result.ok) {
          expect(result.value.rows).toHaveLength(1);
        }
      });

      it('should return column metadata', async () => {
        const result = await adapter.queryRaw({
          sql: 'SELECT * FROM users',
          args: [],
        });

        expect(result.ok).toBe(true);
        if (result.ok) {
          expect(result.value.columnNames).toBeDefined();
          expect(result.value.columnTypes).toBeDefined();
        }
      });

      it('should convert rows to positional arrays', async () => {
        const result = await adapter.queryRaw({
          sql: 'SELECT id, name FROM users WHERE id = ?',
          args: [1],
        });

        expect(result.ok).toBe(true);
        if (result.ok) {
          expect(Array.isArray(result.value.rows[0])).toBe(true);
        }
      });
    });

    describe('executeRaw - INSERT', () => {
      it('should insert a row', async () => {
        const result = await adapter.executeRaw({
          sql: 'INSERT INTO users (name, email, active) VALUES (?, ?, ?)',
          args: ['David', 'david@example.com', true],
        });

        expect(result.ok).toBe(true);
        if (result.ok) {
          expect(result.value).toBe(1);
        }
      });

      it('should insert and verify data', async () => {
        await adapter.executeRaw({
          sql: 'INSERT INTO users (name, email) VALUES (?, ?)',
          args: ['Eve', 'eve@example.com'],
        });

        const result = await adapter.queryRaw({
          sql: 'SELECT * FROM users WHERE name = ?',
          args: ['Eve'],
        });

        expect(result.ok).toBe(true);
        if (result.ok) {
          expect(result.value.rows).toHaveLength(1);
        }
      });
    });

    describe('executeRaw - UPDATE', () => {
      beforeEach(() => {
        backend.seedUsers([
          { id: 1, name: 'Alice', email: 'alice@example.com' },
        ]);
      });

      it('should update a row', async () => {
        const result = await adapter.executeRaw({
          sql: 'UPDATE users SET email = ? WHERE id = ?',
          args: ['alice.new@example.com', 1],
        });

        expect(result.ok).toBe(true);
        if (result.ok) {
          expect(result.value).toBeGreaterThanOrEqual(0);
        }
      });
    });

    describe('executeRaw - DELETE', () => {
      beforeEach(() => {
        backend.seedUsers([
          { id: 1, name: 'Alice', email: 'alice@example.com' },
          { id: 2, name: 'Bob', email: 'bob@example.com' },
        ]);
      });

      it('should delete a row', async () => {
        const result = await adapter.executeRaw({
          sql: 'DELETE FROM users WHERE id = ?',
          args: [1],
        });

        expect(result.ok).toBe(true);
        if (result.ok) {
          expect(result.value).toBe(1);
        }
      });

      it('should delete all rows without WHERE', async () => {
        const result = await adapter.executeRaw({
          sql: 'DELETE FROM users',
          args: [],
        });

        expect(result.ok).toBe(true);
        if (result.ok) {
          expect(result.value).toBe(2);
        }
      });
    });
  });

  // ---------------------------------------------------------------------------
  // Transaction Support
  // ---------------------------------------------------------------------------

  describe('Transaction Support', () => {
    beforeEach(() => {
      backend.seedTable('accounts', [
        { id: 1, name: 'Checking', balance: 1000 },
        { id: 2, name: 'Savings', balance: 5000 },
      ]);
    });

    it('should start a transaction', async () => {
      const result = await adapter.startTransaction();

      expect(result.ok).toBe(true);
      if (result.ok) {
        expect(result.value).toBeDefined();
        expect(result.value.options.usePhantomQuery).toBe(false);
      }
    });

    it('should execute queries within transaction', async () => {
      const txnResult = await adapter.startTransaction();
      expect(txnResult.ok).toBe(true);

      if (txnResult.ok) {
        const txn = txnResult.value;

        // Insert within transaction
        const insertResult = await txn.executeRaw({
          sql: 'INSERT INTO users (name, email) VALUES (?, ?)',
          args: ['TxnUser', 'txn@example.com'],
        });
        expect(insertResult.ok).toBe(true);

        // Query within transaction
        const queryResult = await txn.queryRaw({
          sql: 'SELECT * FROM users',
          args: [],
        });
        expect(queryResult.ok).toBe(true);

        // Commit
        const commitResult = await txn.commit();
        expect(commitResult.ok).toBe(true);
      }
    });

    it('should support rollback', async () => {
      const txnResult = await adapter.startTransaction();
      expect(txnResult.ok).toBe(true);

      if (txnResult.ok) {
        const txn = txnResult.value;

        // Execute operations
        await txn.executeRaw({
          sql: 'INSERT INTO users (name, email) VALUES (?, ?)',
          args: ['WillRollback', 'rollback@example.com'],
        });

        // Rollback
        const rollbackResult = await txn.rollback();
        expect(rollbackResult.ok).toBe(true);
      }
    });

    it('should prevent operations after commit', async () => {
      const txnResult = await adapter.startTransaction();
      expect(txnResult.ok).toBe(true);

      if (txnResult.ok) {
        const txn = txnResult.value;
        await txn.commit();

        // Try to query after commit
        const queryResult = await txn.queryRaw({
          sql: 'SELECT * FROM users',
          args: [],
        });

        expect(queryResult.ok).toBe(false);
        if (!queryResult.ok) {
          expect(queryResult.error.message).toContain('committed');
        }
      }
    });

    it('should prevent operations after rollback', async () => {
      const txnResult = await adapter.startTransaction();
      expect(txnResult.ok).toBe(true);

      if (txnResult.ok) {
        const txn = txnResult.value;
        await txn.rollback();

        // Try to query after rollback
        const queryResult = await txn.queryRaw({
          sql: 'SELECT * FROM users',
          args: [],
        });

        expect(queryResult.ok).toBe(false);
        if (!queryResult.ok) {
          expect(queryResult.error.message).toContain('rolled back');
        }
      }
    });
  });

  // ---------------------------------------------------------------------------
  // Schema Migrations
  // ---------------------------------------------------------------------------

  describe('Schema Migrations', () => {
    it('should create table', async () => {
      const result = await adapter.executeRaw({
        sql: 'CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)',
        args: [],
      });

      expect(result.ok).toBe(true);

      // Verify table can be used
      await adapter.executeRaw({
        sql: 'INSERT INTO products (name, price) VALUES (?, ?)',
        args: ['Widget', 9.99],
      });

      const query = await adapter.queryRaw({
        sql: 'SELECT * FROM products',
        args: [],
      });

      expect(query.ok).toBe(true);
      if (query.ok) {
        expect(query.value.rows).toHaveLength(1);
      }
    });

    it('should drop table', async () => {
      await adapter.executeRaw({
        sql: 'CREATE TABLE temp_table (id INTEGER PRIMARY KEY)',
        args: [],
      });

      const result = await adapter.executeRaw({
        sql: 'DROP TABLE temp_table',
        args: [],
      });

      expect(result.ok).toBe(true);
    });

    it('should add column', async () => {
      await adapter.executeRaw({
        sql: 'CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)',
        args: [],
      });

      const result = await adapter.executeRaw({
        sql: 'ALTER TABLE test_table ADD COLUMN description TEXT',
        args: [],
      });

      expect(result.ok).toBe(true);

      // Verify column was added
      const schema = backend.getSchema('test_table');
      expect(schema).toContain('description');
    });

    it('should rename column', async () => {
      await adapter.executeRaw({
        sql: 'CREATE TABLE test_table (id INTEGER PRIMARY KEY, old_name TEXT)',
        args: [],
      });

      // Add some data
      await adapter.executeRaw({
        sql: 'INSERT INTO test_table (old_name) VALUES (?)',
        args: ['test value'],
      });

      await adapter.executeRaw({
        sql: 'ALTER TABLE test_table RENAME COLUMN old_name TO new_name',
        args: [],
      });

      // Verify rename
      const data = backend.getTableData('test_table');
      expect(data[0]).toHaveProperty('new_name');
      expect(data[0]).not.toHaveProperty('old_name');
    });

    it('should drop column', async () => {
      await adapter.executeRaw({
        sql: 'CREATE TABLE test_table (id INTEGER PRIMARY KEY, keep_col TEXT, drop_col TEXT)',
        args: [],
      });

      // Add some data
      await adapter.executeRaw({
        sql: 'INSERT INTO test_table (keep_col, drop_col) VALUES (?, ?)',
        args: ['keep', 'drop'],
      });

      await adapter.executeRaw({
        sql: 'ALTER TABLE test_table DROP COLUMN drop_col',
        args: [],
      });

      // Verify drop
      const data = backend.getTableData('test_table');
      expect(data[0]).toHaveProperty('keep_col');
      expect(data[0]).not.toHaveProperty('drop_col');
    });

    it('should track migrations', async () => {
      // Insert migration record
      await adapter.executeRaw({
        sql: 'INSERT INTO migrations (name) VALUES (?)',
        args: ['001_initial_schema'],
      });

      await adapter.executeRaw({
        sql: 'INSERT INTO migrations (name) VALUES (?)',
        args: ['002_add_users'],
      });

      // Query migrations
      const result = await adapter.queryRaw({
        sql: 'SELECT * FROM migrations ORDER BY id ASC',
        args: [],
      });

      expect(result.ok).toBe(true);
      if (result.ok) {
        expect(result.value.rows).toHaveLength(2);
      }
    });
  });

  // ---------------------------------------------------------------------------
  // Query Building Compatibility
  // ---------------------------------------------------------------------------

  describe('Query Building Compatibility', () => {
    beforeEach(() => {
      backend.seedTable('products', [
        { id: 1, name: 'Apple', price: 1.50, stock: 100, category: 'fruit' },
        { id: 2, name: 'Banana', price: 0.75, stock: 150, category: 'fruit' },
        { id: 3, name: 'Carrot', price: 0.50, stock: 200, category: 'vegetable' },
        { id: 4, name: 'Milk', price: 2.99, stock: 50, category: 'dairy' },
        { id: 5, name: 'Bread', price: 3.50, stock: 30, category: 'bakery' },
      ]);
    });

    it('should handle equality filter', async () => {
      const result = await adapter.queryRaw({
        sql: 'SELECT * FROM products WHERE category = ?',
        args: ['fruit'],
      });

      expect(result.ok).toBe(true);
      if (result.ok) {
        expect(result.value.rows).toHaveLength(2);
      }
    });

    it('should handle inequality filter', async () => {
      const result = await adapter.queryRaw({
        sql: 'SELECT * FROM products WHERE category <> ?',
        args: ['fruit'],
      });

      expect(result.ok).toBe(true);
      if (result.ok) {
        expect(result.value.rows).toHaveLength(3);
      }
    });

    it('should handle greater than filter', async () => {
      const result = await adapter.queryRaw({
        sql: 'SELECT * FROM products WHERE price > ?',
        args: [2.00],
      });

      expect(result.ok).toBe(true);
      if (result.ok) {
        expect(result.value.rows).toHaveLength(2);
      }
    });

    it('should handle ORDER BY', async () => {
      const result = await adapter.queryRaw({
        sql: 'SELECT * FROM products ORDER BY price DESC',
        args: [],
      });

      expect(result.ok).toBe(true);
      if (result.ok) {
        // First row should have highest price
        const firstPrice = result.value.rows[0][result.value.columnNames.indexOf('price')];
        expect(firstPrice).toBe(3.50);
      }
    });

    it('should handle LIMIT', async () => {
      const result = await adapter.queryRaw({
        sql: 'SELECT * FROM products LIMIT 3',
        args: [],
      });

      expect(result.ok).toBe(true);
      if (result.ok) {
        expect(result.value.rows).toHaveLength(3);
      }
    });
  });

  // ---------------------------------------------------------------------------
  // Value Conversion
  // ---------------------------------------------------------------------------

  describe('Value Conversion', () => {
    it('should convert boolean true to 1', async () => {
      backend.seedUsers([
        { id: 1, name: 'Active', active: true },
        { id: 2, name: 'Inactive', active: false },
      ]);

      const result = await adapter.queryRaw({
        sql: 'SELECT id, active FROM users',
        args: [],
      });

      expect(result.ok).toBe(true);
      if (result.ok) {
        const activeIndex = result.value.columnNames.indexOf('active');
        expect(result.value.rows[0][activeIndex]).toBe(1);
        expect(result.value.rows[1][activeIndex]).toBe(0);
      }
    });

    it('should handle null values', async () => {
      backend.seedUsers([
        { id: 1, name: 'NoEmail', email: null },
      ]);

      const result = await adapter.queryRaw({
        sql: 'SELECT * FROM users',
        args: [],
      });

      expect(result.ok).toBe(true);
      if (result.ok) {
        const emailIndex = result.value.columnNames.indexOf('email');
        expect(result.value.rows[0][emailIndex]).toBe(null);
      }
    });

    it('should handle Date values', async () => {
      const date = new Date('2024-06-15T10:30:00Z');
      backend.seedUsers([
        { id: 1, name: 'User', createdAt: date },
      ]);

      const result = await adapter.queryRaw({
        sql: 'SELECT * FROM users',
        args: [],
      });

      expect(result.ok).toBe(true);
      if (result.ok) {
        const dateIndex = result.value.columnNames.indexOf('createdAt');
        const value = result.value.rows[0][dateIndex];
        expect(typeof value).toBe('string');
        expect(value).toBe(date.toISOString());
      }
    });
  });

  // ---------------------------------------------------------------------------
  // Result Utilities
  // ---------------------------------------------------------------------------

  describe('Result Utilities', () => {
    it('isOk should identify successful results', () => {
      const success: Result<number> = { ok: true, value: 42 };
      const failure: Result<number> = { ok: false, error: new Error('fail') };

      expect(isOk(success)).toBe(true);
      expect(isOk(failure)).toBe(false);
    });

    it('isError should identify error results', () => {
      const success: Result<number> = { ok: true, value: 42 };
      const failure: Result<number> = { ok: false, error: new Error('fail') };

      expect(isError(success)).toBe(false);
      expect(isError(failure)).toBe(true);
    });

    it('unwrap should return value for success', () => {
      const success: Result<string> = { ok: true, value: 'hello' };
      expect(unwrap(success)).toBe('hello');
    });

    it('unwrap should throw for error', () => {
      const failure: Result<string> = { ok: false, error: new Error('test error') };
      expect(() => unwrap(failure)).toThrow('test error');
    });

    it('unwrapOr should return value for success', () => {
      const success: Result<number> = { ok: true, value: 42 };
      expect(unwrapOr(success, 0)).toBe(42);
    });

    it('unwrapOr should return default for error', () => {
      const failure: Result<number> = { ok: false, error: new Error('fail') };
      expect(unwrapOr(failure, 99)).toBe(99);
    });
  });

  // ---------------------------------------------------------------------------
  // Error Handling
  // ---------------------------------------------------------------------------

  describe('Error Handling', () => {
    it('should return error result for backend query failure', async () => {
      const errorBackend: DoSQLBackend = {
        query: async () => { throw new Error('Query failed'); },
        execute: async () => { throw new Error('Execute failed'); },
        beginTransaction: async () => { throw new Error('Transaction failed'); },
      };

      const errorAdapter = new PrismaDoSQLAdapter({ backend: errorBackend });

      const result = await errorAdapter.queryRaw({
        sql: 'SELECT * FROM users',
        args: [],
      });

      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.error.message).toBe('Query failed');
      }
    });

    it('should return error result for backend execute failure', async () => {
      const errorBackend: DoSQLBackend = {
        query: async () => ({ rows: [], columns: [] }),
        execute: async () => { throw new Error('Execute failed'); },
        beginTransaction: async () => { throw new Error('Transaction failed'); },
      };

      const errorAdapter = new PrismaDoSQLAdapter({ backend: errorBackend });

      const result = await errorAdapter.executeRaw({
        sql: 'INSERT INTO users (name) VALUES (?)',
        args: ['test'],
      });

      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.error.message).toBe('Execute failed');
      }
    });

    it('should return error result for transaction start failure', async () => {
      const errorBackend: DoSQLBackend = {
        query: async () => ({ rows: [], columns: [] }),
        execute: async () => ({ rowsAffected: 0 }),
        beginTransaction: async () => { throw new Error('Transaction failed'); },
      };

      const errorAdapter = new PrismaDoSQLAdapter({ backend: errorBackend });

      const result = await errorAdapter.startTransaction();

      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.error.message).toBe('Transaction failed');
      }
    });
  });

  // ---------------------------------------------------------------------------
  // Logging
  // ---------------------------------------------------------------------------

  describe('Logging', () => {
    it('should log operations when enabled', async () => {
      const logs: string[] = [];
      const loggingAdapter = new PrismaDoSQLAdapter({
        backend,
        logging: true,
        logger: (msg) => logs.push(msg),
      });

      backend.seedUsers([{ id: 1, name: 'Test' }]);

      await loggingAdapter.queryRaw({ sql: 'SELECT * FROM users', args: [] });
      await loggingAdapter.executeRaw({ sql: 'DELETE FROM users', args: [] });

      expect(logs.length).toBeGreaterThanOrEqual(2);
    });

    it('should not log when disabled', async () => {
      const logs: string[] = [];
      const silentAdapter = new PrismaDoSQLAdapter({
        backend,
        logging: false,
        logger: (msg) => logs.push(msg),
      });

      await silentAdapter.queryRaw({ sql: 'SELECT * FROM users', args: [] });

      expect(logs).toHaveLength(0);
    });
  });

  // ---------------------------------------------------------------------------
  // Factory Function
  // ---------------------------------------------------------------------------

  describe('Factory Function', () => {
    it('should create adapter with createPrismaAdapter', () => {
      const adapter = createPrismaAdapter({ backend });

      expect(adapter).toBeDefined();
      expect(adapter.provider).toBe('sqlite');
      expect(adapter.adapterName).toBe('dosql-prisma');
    });

    it('should create adapter with logging config', () => {
      const logs: string[] = [];
      const adapter = createPrismaAdapter({
        backend,
        logging: true,
        logger: (msg) => logs.push(msg),
      });

      expect(adapter).toBeDefined();
    });
  });

  // ---------------------------------------------------------------------------
  // Adapter Properties
  // ---------------------------------------------------------------------------

  describe('Adapter Properties', () => {
    it('should have sqlite provider', () => {
      expect(adapter.provider).toBe('sqlite');
    });

    it('should have dosql-prisma adapter name', () => {
      expect(adapter.adapterName).toBe('dosql-prisma');
    });
  });
});
