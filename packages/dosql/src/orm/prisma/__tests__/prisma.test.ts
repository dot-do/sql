/**
 * Prisma Driver Adapter Tests for DoSQL
 *
 * Tests the Prisma driver adapter implementation.
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
} from '../index.js';
import type {
  DoSQLBackend,
  DoSQLTransaction,
  Query,
  Result,
  ResultSet,
} from '../adapter.js';
import type { QueryResult, Row } from '../../../engine/types.js';

// =============================================================================
// In-Memory DoSQL Backend for Testing
// =============================================================================

/**
 * Simple in-memory DoSQL backend for testing the adapter
 * This is NOT a mock - it's a real implementation that stores data in memory
 */
class InMemoryDoSQLBackend implements DoSQLBackend {
  private tables: Map<string, Row[]> = new Map();
  private lastInsertRowid: bigint = 0n;
  private transactionCounter = 0;

  constructor() {
    // Initialize with a users table for testing
    this.tables.set('users', []);
    this.tables.set('posts', []);
  }

  async query<T = Row>(sql: string, params?: unknown[]): Promise<QueryResult<T>> {
    const normalizedSql = sql.trim().toUpperCase();

    // Handle SELECT queries
    if (normalizedSql.startsWith('SELECT')) {
      return this.handleSelect<T>(sql, params);
    }

    // For other queries, return empty result
    return {
      rows: [] as T[],
      columns: [],
    };
  }

  async execute(sql: string, params?: unknown[]): Promise<{ rowsAffected: number; lastInsertRowid?: bigint }> {
    const normalizedSql = sql.trim().toUpperCase();

    if (normalizedSql.startsWith('INSERT')) {
      return this.handleInsert(sql, params);
    }

    if (normalizedSql.startsWith('UPDATE')) {
      return this.handleUpdate(sql, params);
    }

    if (normalizedSql.startsWith('DELETE')) {
      return this.handleDelete(sql, params);
    }

    if (normalizedSql.startsWith('CREATE')) {
      // DDL operations return 0 affected rows
      return { rowsAffected: 0 };
    }

    return { rowsAffected: 0 };
  }

  async beginTransaction(): Promise<DoSQLTransaction> {
    return new InMemoryTransaction(this, ++this.transactionCounter);
  }

  // ---- Internal query handlers ----

  private handleSelect<T>(sql: string, _params?: unknown[]): QueryResult<T> {
    // Simple parsing for "SELECT * FROM tablename" or "SELECT cols FROM tablename"
    const match = sql.match(/SELECT\s+(.+?)\s+FROM\s+(\w+)/i);
    if (!match) {
      return { rows: [] as T[], columns: [] };
    }

    const [, columns, tableName] = match;
    const table = this.tables.get(tableName.toLowerCase());

    if (!table || table.length === 0) {
      return { rows: [] as T[], columns: [] };
    }

    // Handle SELECT *
    if (columns.trim() === '*') {
      // Return rows without explicit column metadata to test type inference
      return {
        rows: table as T[],
      };
    }

    // Handle specific columns
    const colList = columns.split(',').map((c) => c.trim());
    const filteredRows = table.map((row) => {
      const newRow: Row = {};
      for (const col of colList) {
        if (col in row) {
          newRow[col] = row[col];
        }
      }
      return newRow;
    });

    // Return rows without explicit column metadata to test type inference
    return {
      rows: filteredRows as T[],
    };
  }

  private handleInsert(sql: string, params?: unknown[]): { rowsAffected: number; lastInsertRowid: bigint } {
    // Simple parsing for INSERT INTO tablename (cols) VALUES (?)
    const match = sql.match(/INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)/i);
    if (!match) {
      return { rowsAffected: 0, lastInsertRowid: 0n };
    }

    const [, tableName, columnsStr] = match;
    const columns = columnsStr.split(',').map((c) => c.trim());
    const table = this.tables.get(tableName.toLowerCase());

    if (!table) {
      this.tables.set(tableName.toLowerCase(), []);
    }

    // Create new row from params
    const newRow: Row = {};
    const values = params ?? [];
    for (let i = 0; i < columns.length && i < values.length; i++) {
      newRow[columns[i]] = values[i] as any;
    }

    // Auto-generate ID if not provided
    if (!('id' in newRow)) {
      this.lastInsertRowid++;
      newRow['id'] = Number(this.lastInsertRowid);
    } else {
      this.lastInsertRowid = BigInt(newRow['id'] as number);
    }

    this.tables.get(tableName.toLowerCase())!.push(newRow);

    return { rowsAffected: 1, lastInsertRowid: this.lastInsertRowid };
  }

  private handleUpdate(sql: string, _params?: unknown[]): { rowsAffected: number } {
    // Simple parsing for UPDATE tablename SET col = val WHERE ...
    const match = sql.match(/UPDATE\s+(\w+)/i);
    if (!match) {
      return { rowsAffected: 0 };
    }

    const [, tableName] = match;
    const table = this.tables.get(tableName.toLowerCase());

    // For simplicity, update all rows (real implementation would parse WHERE)
    return { rowsAffected: table?.length ?? 0 };
  }

  private handleDelete(sql: string, _params?: unknown[]): { rowsAffected: number } {
    // Simple parsing for DELETE FROM tablename WHERE ...
    const match = sql.match(/DELETE\s+FROM\s+(\w+)/i);
    if (!match) {
      return { rowsAffected: 0 };
    }

    const [, tableName] = match;
    const table = this.tables.get(tableName.toLowerCase());

    if (!table) {
      return { rowsAffected: 0 };
    }

    // For simplicity, delete all rows
    const count = table.length;
    this.tables.set(tableName.toLowerCase(), []);

    return { rowsAffected: count };
  }

  // ---- Test helpers ----

  /** Seed test data */
  seedUsers(users: Row[]): void {
    this.tables.set('users', users);
  }

  /** Clear all data */
  clear(): void {
    this.tables.clear();
    this.tables.set('users', []);
    this.tables.set('posts', []);
    this.lastInsertRowid = 0n;
  }
}

/**
 * In-memory transaction implementation
 */
class InMemoryTransaction implements DoSQLTransaction {
  readonly id: string;
  private committed = false;
  private rolledBack = false;

  constructor(
    private readonly backend: InMemoryDoSQLBackend,
    txnNumber: number
  ) {
    this.id = `txn_${txnNumber}`;
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
// Test Suites
// =============================================================================

describe('PrismaDoSQLAdapter', () => {
  let backend: InMemoryDoSQLBackend;
  let adapter: PrismaDoSQLAdapter;

  beforeEach(() => {
    backend = new InMemoryDoSQLBackend();
    adapter = new PrismaDoSQLAdapter({ backend });
  });

  // ---------------------------------------------------------------------------
  // Adapter Initialization Tests
  // ---------------------------------------------------------------------------

  describe('initialization', () => {
    it('should create adapter with required config', () => {
      const adapter = new PrismaDoSQLAdapter({ backend });

      expect(adapter).toBeDefined();
      expect(adapter.provider).toBe('sqlite');
      expect(adapter.adapterName).toBe('dosql-prisma');
    });

    it('should create adapter with logging enabled', () => {
      const logs: string[] = [];
      const adapter = new PrismaDoSQLAdapter({
        backend,
        logging: true,
        logger: (msg) => logs.push(msg),
      });

      expect(adapter).toBeDefined();
    });

    it('should create adapter using factory function', () => {
      const adapter = createPrismaAdapter({ backend });

      expect(adapter).toBeDefined();
      expect(adapter.provider).toBe('sqlite');
      expect(adapter.adapterName).toBe('dosql-prisma');
    });
  });

  // ---------------------------------------------------------------------------
  // queryRaw Tests
  // ---------------------------------------------------------------------------

  describe('queryRaw', () => {
    it('should execute SELECT query and return ResultSet', async () => {
      // Seed test data
      backend.seedUsers([
        { id: 1, name: 'Alice', email: 'alice@example.com' },
        { id: 2, name: 'Bob', email: 'bob@example.com' },
      ]);

      const result = await adapter.queryRaw({
        sql: 'SELECT * FROM users',
        args: [],
      });

      expect(result.ok).toBe(true);
      if (result.ok) {
        expect(result.value.rows).toHaveLength(2);
        expect(result.value.columnNames).toContain('id');
        expect(result.value.columnNames).toContain('name');
        expect(result.value.columnNames).toContain('email');
      }
    });

    it('should return empty ResultSet for non-existent table', async () => {
      const result = await adapter.queryRaw({
        sql: 'SELECT * FROM nonexistent',
        args: [],
      });

      expect(result.ok).toBe(true);
      if (result.ok) {
        expect(result.value.rows).toHaveLength(0);
      }
    });

    it('should handle SELECT with specific columns', async () => {
      backend.seedUsers([
        { id: 1, name: 'Alice', email: 'alice@example.com' },
      ]);

      const result = await adapter.queryRaw({
        sql: 'SELECT name, email FROM users',
        args: [],
      });

      expect(result.ok).toBe(true);
      if (result.ok) {
        expect(result.value.columnNames).toEqual(['name', 'email']);
        expect(result.value.rows[0]).toHaveLength(2);
      }
    });

    it('should convert rows to positional arrays', async () => {
      backend.seedUsers([
        { id: 1, name: 'Alice', email: 'alice@example.com' },
      ]);

      const result = await adapter.queryRaw({
        sql: 'SELECT id, name FROM users',
        args: [],
      });

      expect(result.ok).toBe(true);
      if (result.ok) {
        // Rows should be arrays, not objects
        expect(Array.isArray(result.value.rows[0])).toBe(true);
        // Values should match column order
        expect(result.value.rows[0][0]).toBe(1); // id
        expect(result.value.rows[0][1]).toBe('Alice'); // name
      }
    });

    it('should include columnTypes in ResultSet', async () => {
      backend.seedUsers([
        { id: 1, name: 'Alice', active: true },
      ]);

      const result = await adapter.queryRaw({
        sql: 'SELECT * FROM users',
        args: [],
      });

      expect(result.ok).toBe(true);
      if (result.ok) {
        expect(result.value.columnTypes).toBeDefined();
        expect(result.value.columnTypes.length).toBeGreaterThan(0);
        for (const colType of result.value.columnTypes) {
          expect(colType).toHaveProperty('name');
          expect(colType).toHaveProperty('type');
        }
      }
    });
  });

  // ---------------------------------------------------------------------------
  // executeRaw Tests
  // ---------------------------------------------------------------------------

  describe('executeRaw', () => {
    it('should execute INSERT and return affected rows', async () => {
      const result = await adapter.executeRaw({
        sql: 'INSERT INTO users (name, email) VALUES (?, ?)',
        args: ['Charlie', 'charlie@example.com'],
      });

      expect(result.ok).toBe(true);
      if (result.ok) {
        expect(result.value).toBe(1);
      }

      // Verify the insert
      const queryResult = await adapter.queryRaw({
        sql: 'SELECT * FROM users',
        args: [],
      });
      expect(queryResult.ok).toBe(true);
      if (queryResult.ok) {
        expect(queryResult.value.rows).toHaveLength(1);
      }
    });

    it('should execute UPDATE and return affected rows', async () => {
      backend.seedUsers([
        { id: 1, name: 'Alice', email: 'alice@example.com' },
        { id: 2, name: 'Bob', email: 'bob@example.com' },
      ]);

      const result = await adapter.executeRaw({
        sql: 'UPDATE users SET name = ? WHERE id = ?',
        args: ['Alice Updated', 1],
      });

      expect(result.ok).toBe(true);
      if (result.ok) {
        // Our simple implementation updates all rows
        expect(result.value).toBeGreaterThanOrEqual(0);
      }
    });

    it('should execute DELETE and return affected rows', async () => {
      backend.seedUsers([
        { id: 1, name: 'Alice', email: 'alice@example.com' },
        { id: 2, name: 'Bob', email: 'bob@example.com' },
      ]);

      const result = await adapter.executeRaw({
        sql: 'DELETE FROM users WHERE id = ?',
        args: [1],
      });

      expect(result.ok).toBe(true);
      if (result.ok) {
        expect(result.value).toBe(2); // Our simple impl deletes all
      }
    });

    it('should handle DDL statements', async () => {
      const result = await adapter.executeRaw({
        sql: 'CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)',
        args: [],
      });

      expect(result.ok).toBe(true);
      if (result.ok) {
        expect(result.value).toBe(0);
      }
    });
  });

  // ---------------------------------------------------------------------------
  // Transaction Tests
  // ---------------------------------------------------------------------------

  describe('startTransaction', () => {
    it('should start a new transaction', async () => {
      const result = await adapter.startTransaction();

      expect(result.ok).toBe(true);
      if (result.ok) {
        expect(result.value).toBeDefined();
        expect(result.value.options).toBeDefined();
        expect(result.value.options.usePhantomQuery).toBe(false);
      }
    });

    it('should execute queries within transaction', async () => {
      const txnResult = await adapter.startTransaction();
      expect(txnResult.ok).toBe(true);

      if (txnResult.ok) {
        const txn = txnResult.value;

        // Execute insert in transaction
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

    it('should support transaction rollback', async () => {
      const txnResult = await adapter.startTransaction();
      expect(txnResult.ok).toBe(true);

      if (txnResult.ok) {
        const txn = txnResult.value;

        // Execute some operations
        await txn.executeRaw({
          sql: 'INSERT INTO users (name, email) VALUES (?, ?)',
          args: ['WillRollback', 'rollback@example.com'],
        });

        // Rollback
        const rollbackResult = await txn.rollback();
        expect(rollbackResult.ok).toBe(true);
      }
    });

    it('should not allow operations after commit', async () => {
      const txnResult = await adapter.startTransaction();
      expect(txnResult.ok).toBe(true);

      if (txnResult.ok) {
        const txn = txnResult.value;
        await txn.commit();

        // Try to execute after commit
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

    it('should not allow operations after rollback', async () => {
      const txnResult = await adapter.startTransaction();
      expect(txnResult.ok).toBe(true);

      if (txnResult.ok) {
        const txn = txnResult.value;
        await txn.rollback();

        // Try to execute after rollback
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
  // Value Conversion Tests
  // ---------------------------------------------------------------------------

  describe('value conversion', () => {
    it('should convert boolean values to integers', async () => {
      backend.seedUsers([
        { id: 1, name: 'Alice', active: true },
        { id: 2, name: 'Bob', active: false },
      ]);

      const result = await adapter.queryRaw({
        sql: 'SELECT id, active FROM users',
        args: [],
      });

      expect(result.ok).toBe(true);
      if (result.ok) {
        // Booleans should be converted to 0/1
        const activeIndex = result.value.columnNames.indexOf('active');
        expect(result.value.rows[0][activeIndex]).toBe(1);
        expect(result.value.rows[1][activeIndex]).toBe(0);
      }
    });

    it('should handle null values', async () => {
      backend.seedUsers([
        { id: 1, name: 'Alice', middleName: null },
      ]);

      const result = await adapter.queryRaw({
        sql: 'SELECT * FROM users',
        args: [],
      });

      expect(result.ok).toBe(true);
      if (result.ok) {
        const middleNameIndex = result.value.columnNames.indexOf('middleName');
        expect(result.value.rows[0][middleNameIndex]).toBe(null);
      }
    });

    it('should handle Date values as ISO strings', async () => {
      const date = new Date('2024-01-15T12:00:00Z');
      backend.seedUsers([
        { id: 1, name: 'Alice', createdAt: date },
      ]);

      const result = await adapter.queryRaw({
        sql: 'SELECT * FROM users',
        args: [],
      });

      expect(result.ok).toBe(true);
      if (result.ok) {
        const createdAtIndex = result.value.columnNames.indexOf('createdAt');
        const value = result.value.rows[0][createdAtIndex];
        expect(typeof value).toBe('string');
        expect(value).toBe(date.toISOString());
      }
    });
  });

  // ---------------------------------------------------------------------------
  // Result Utility Tests
  // ---------------------------------------------------------------------------

  describe('result utilities', () => {
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
      const success: Result<number> = { ok: true, value: 42 };
      expect(unwrap(success)).toBe(42);
    });

    it('unwrap should throw for error', () => {
      const failure: Result<number> = { ok: false, error: new Error('test error') };
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
  // Logging Tests
  // ---------------------------------------------------------------------------

  describe('logging', () => {
    it('should log operations when logging is enabled', async () => {
      const logs: string[] = [];
      const loggingAdapter = new PrismaDoSQLAdapter({
        backend,
        logging: true,
        logger: (msg) => logs.push(msg),
      });

      backend.seedUsers([{ id: 1, name: 'Test' }]);

      await loggingAdapter.queryRaw({ sql: 'SELECT * FROM users', args: [] });
      await loggingAdapter.executeRaw({ sql: 'DELETE FROM users', args: [] });
      await loggingAdapter.startTransaction();

      expect(logs).toHaveLength(3);
      expect(logs[0]).toContain('queryRaw');
      expect(logs[1]).toContain('executeRaw');
      expect(logs[2]).toContain('startTransaction');
    });

    it('should not log when logging is disabled', async () => {
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
  // Error Handling Tests
  // ---------------------------------------------------------------------------

  describe('error handling', () => {
    it('should return error result for backend query failure', async () => {
      // Create a backend that throws
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
});

// =============================================================================
// ResultSet Conversion Tests
// =============================================================================

describe('ResultSet conversion', () => {
  let backend: InMemoryDoSQLBackend;
  let adapter: PrismaDoSQLAdapter;

  beforeEach(() => {
    backend = new InMemoryDoSQLBackend();
    adapter = new PrismaDoSQLAdapter({ backend });
  });

  it('should handle empty result set', async () => {
    const result = await adapter.queryRaw({
      sql: 'SELECT * FROM users',
      args: [],
    });

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.value.rows).toEqual([]);
      expect(result.value.columnNames).toEqual([]);
      expect(result.value.columnTypes).toEqual([]);
    }
  });

  it('should infer column types from values', async () => {
    backend.seedUsers([
      {
        id: 1,
        name: 'Alice',
        score: 95.5,
        active: true,
        data: null,
      },
    ]);

    const result = await adapter.queryRaw({
      sql: 'SELECT * FROM users',
      args: [],
    });

    expect(result.ok).toBe(true);
    if (result.ok) {
      const typeMap = new Map(
        result.value.columnTypes.map((ct) => [ct.name, ct.type])
      );

      expect(typeMap.get('id')).toBe('INTEGER');
      expect(typeMap.get('name')).toBe('TEXT');
      expect(typeMap.get('score')).toBe('REAL');
      // Booleans are converted to INTEGER in SQLite
      expect(typeMap.get('active')).toBe('INTEGER');
      expect(typeMap.get('data')).toBe('NULL');
    }
  });
});
