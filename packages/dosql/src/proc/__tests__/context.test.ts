/**
 * Database Context Tests
 *
 * Additional tests for the procedure context module including:
 * - Table accessor operations
 * - SQL executor functionality
 * - Transaction management
 * - Query options handling
 *
 * Issue: sql-ntht - Stored Procedure Module Tests
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  createInMemoryAdapter,
  createTableAccessor,
  createSqlFunction,
  createTransactionFunction,
  createInMemorySqlExecutor,
  createInMemoryTransactionManager,
  createDatabaseContext,
  type StorageAdapter,
  type SqlExecutor,
  type TransactionManager,
} from '../context.js';
import type { DatabaseSchema, TableAccessor } from '../types.js';

// =============================================================================
// TEST TYPES
// =============================================================================

interface TestRecord {
  id: number;
  name: string;
  value: number;
  active: boolean;
}

interface UserRecord {
  id: number;
  name: string;
  email: string;
  age: number;
}

interface OrderRecord {
  id: number;
  userId: number;
  total: number;
  status: string;
}

// =============================================================================
// IN-MEMORY ADAPTER TESTS
// =============================================================================

describe('createInMemoryAdapter', () => {
  describe('initialization', () => {
    it('should create empty adapter', async () => {
      const adapter = createInMemoryAdapter<TestRecord>([]);

      const count = await adapter.count();
      expect(count).toBe(0);
    });

    it('should initialize with seed data', async () => {
      const adapter = createInMemoryAdapter<TestRecord>([
        { id: 1, name: 'a', value: 10, active: true },
        { id: 2, name: 'b', value: 20, active: false },
      ]);

      const count = await adapter.count();
      expect(count).toBe(2);
    });

    it('should track ID counter from initial data', async () => {
      const adapter = createInMemoryAdapter<TestRecord>([
        { id: 5, name: 'a', value: 10, active: true },
        { id: 10, name: 'b', value: 20, active: false },
      ]);

      const newRecord = await adapter.insert({ name: 'c', value: 30, active: true });
      expect(newRecord.id).toBe(11);
    });
  });

  describe('get', () => {
    let adapter: StorageAdapter<TestRecord>;

    beforeEach(() => {
      adapter = createInMemoryAdapter<TestRecord>([
        { id: 1, name: 'first', value: 100, active: true },
        { id: 2, name: 'second', value: 200, active: false },
      ]);
    });

    it('should get record by numeric id', async () => {
      const record = await adapter.get(1);

      expect(record).toBeDefined();
      expect(record?.name).toBe('first');
    });

    it('should return undefined for non-existent id', async () => {
      const record = await adapter.get(999);
      expect(record).toBeUndefined();
    });
  });

  describe('query', () => {
    let adapter: StorageAdapter<TestRecord>;

    beforeEach(() => {
      adapter = createInMemoryAdapter<TestRecord>([
        { id: 1, name: 'a', value: 10, active: true },
        { id: 2, name: 'b', value: 20, active: true },
        { id: 3, name: 'c', value: 30, active: false },
        { id: 4, name: 'd', value: 40, active: false },
      ]);
    });

    it('should query with empty filter (all records)', async () => {
      const results = await adapter.query({});
      expect(results).toHaveLength(4);
    });

    it('should query with single field filter', async () => {
      const results = await adapter.query({ active: true });
      expect(results).toHaveLength(2);
      expect(results.every(r => r.active)).toBe(true);
    });

    it('should query with multiple field filter', async () => {
      const results = await adapter.query({ active: true, name: 'a' });
      expect(results).toHaveLength(1);
      expect(results[0].name).toBe('a');
    });

    it('should return empty array for no matches', async () => {
      const results = await adapter.query({ name: 'nonexistent' });
      expect(results).toHaveLength(0);
    });

    it('should apply limit option', async () => {
      const results = await adapter.query({}, { limit: 2 });
      expect(results).toHaveLength(2);
    });

    it('should apply offset option', async () => {
      const results = await adapter.query({}, { offset: 2 });
      expect(results).toHaveLength(2);
    });

    it('should apply orderBy option', async () => {
      const results = await adapter.query({}, { orderBy: 'value', orderDirection: 'desc' });
      expect(results[0].value).toBe(40);
      expect(results[3].value).toBe(10);
    });

    it('should combine query options', async () => {
      const results = await adapter.query(
        {},
        { orderBy: 'value', orderDirection: 'asc', offset: 1, limit: 2 }
      );
      expect(results).toHaveLength(2);
      expect(results[0].value).toBe(20);
      expect(results[1].value).toBe(30);
    });
  });

  describe('queryWithPredicate', () => {
    let adapter: StorageAdapter<TestRecord>;

    beforeEach(() => {
      adapter = createInMemoryAdapter<TestRecord>([
        { id: 1, name: 'a', value: 10, active: true },
        { id: 2, name: 'b', value: 25, active: true },
        { id: 3, name: 'c', value: 30, active: false },
        { id: 4, name: 'd', value: 50, active: false },
      ]);
    });

    it('should filter with predicate function', async () => {
      const results = await adapter.queryWithPredicate(r => r.value > 20);
      expect(results).toHaveLength(3);
      expect(results.every(r => r.value > 20)).toBe(true);
    });

    it('should support complex predicates', async () => {
      const results = await adapter.queryWithPredicate(
        r => r.active && r.value > 15
      );
      expect(results).toHaveLength(1);
      expect(results[0].name).toBe('b');
    });

    it('should apply query options with predicate', async () => {
      const results = await adapter.queryWithPredicate(
        r => r.value > 10,
        { limit: 2, orderBy: 'value', orderDirection: 'desc' }
      );
      expect(results).toHaveLength(2);
      expect(results[0].value).toBe(50);
    });
  });

  describe('count', () => {
    let adapter: StorageAdapter<TestRecord>;

    beforeEach(() => {
      adapter = createInMemoryAdapter<TestRecord>([
        { id: 1, name: 'a', value: 10, active: true },
        { id: 2, name: 'b', value: 20, active: true },
        { id: 3, name: 'c', value: 30, active: false },
      ]);
    });

    it('should count all records', async () => {
      const count = await adapter.count();
      expect(count).toBe(3);
    });

    it('should count with filter', async () => {
      const count = await adapter.count({ active: true });
      expect(count).toBe(2);
    });

    it('should return 0 for no matches', async () => {
      const count = await adapter.count({ name: 'nonexistent' });
      expect(count).toBe(0);
    });
  });

  describe('countWithPredicate', () => {
    let adapter: StorageAdapter<TestRecord>;

    beforeEach(() => {
      adapter = createInMemoryAdapter<TestRecord>([
        { id: 1, name: 'a', value: 10, active: true },
        { id: 2, name: 'b', value: 25, active: true },
        { id: 3, name: 'c', value: 30, active: false },
      ]);
    });

    it('should count with predicate', async () => {
      const count = await adapter.countWithPredicate(r => r.value >= 25);
      expect(count).toBe(2);
    });

    it('should count all when predicate undefined', async () => {
      const count = await adapter.countWithPredicate();
      expect(count).toBe(3);
    });
  });

  describe('insert', () => {
    it('should insert and auto-generate id', async () => {
      const adapter = createInMemoryAdapter<TestRecord>([]);

      const record = await adapter.insert({ name: 'new', value: 100, active: true });

      expect(record.id).toBe(1);
      expect(record.name).toBe('new');

      const count = await adapter.count();
      expect(count).toBe(1);
    });

    it('should auto-increment id', async () => {
      const adapter = createInMemoryAdapter<TestRecord>([]);

      const r1 = await adapter.insert({ name: 'a', value: 1, active: true });
      const r2 = await adapter.insert({ name: 'b', value: 2, active: true });
      const r3 = await adapter.insert({ name: 'c', value: 3, active: true });

      expect(r1.id).toBe(1);
      expect(r2.id).toBe(2);
      expect(r3.id).toBe(3);
    });
  });

  describe('update', () => {
    let adapter: StorageAdapter<TestRecord>;

    beforeEach(() => {
      adapter = createInMemoryAdapter<TestRecord>([
        { id: 1, name: 'a', value: 10, active: true },
        { id: 2, name: 'b', value: 20, active: true },
        { id: 3, name: 'c', value: 30, active: false },
      ]);
    });

    it('should update matching records', async () => {
      const updated = await adapter.update({ active: true }, { value: 99 });

      expect(updated).toBe(2);

      const results = await adapter.query({ active: true });
      expect(results.every(r => r.value === 99)).toBe(true);
    });

    it('should update single record by id', async () => {
      const updated = await adapter.update({ id: 1 }, { name: 'updated' });

      expect(updated).toBe(1);

      const record = await adapter.get(1);
      expect(record?.name).toBe('updated');
    });

    it('should return 0 for no matches', async () => {
      const updated = await adapter.update({ name: 'nonexistent' }, { value: 0 });
      expect(updated).toBe(0);
    });

    it('should preserve non-updated fields', async () => {
      await adapter.update({ id: 1 }, { name: 'updated' });

      const record = await adapter.get(1);
      expect(record?.name).toBe('updated');
      expect(record?.value).toBe(10); // Preserved
      expect(record?.active).toBe(true); // Preserved
    });
  });

  describe('updateWithPredicate', () => {
    let adapter: StorageAdapter<TestRecord>;

    beforeEach(() => {
      adapter = createInMemoryAdapter<TestRecord>([
        { id: 1, name: 'a', value: 10, active: true },
        { id: 2, name: 'b', value: 25, active: true },
        { id: 3, name: 'c', value: 30, active: false },
      ]);
    });

    it('should update with predicate', async () => {
      const updated = await adapter.updateWithPredicate(
        r => r.value > 20,
        { active: true }
      );

      expect(updated).toBe(2);

      const all = await adapter.query({});
      expect(all.every(r => r.active)).toBe(true);
    });
  });

  describe('delete', () => {
    let adapter: StorageAdapter<TestRecord>;

    beforeEach(() => {
      adapter = createInMemoryAdapter<TestRecord>([
        { id: 1, name: 'a', value: 10, active: true },
        { id: 2, name: 'b', value: 20, active: true },
        { id: 3, name: 'c', value: 30, active: false },
      ]);
    });

    it('should delete matching records', async () => {
      const deleted = await adapter.delete({ active: true });

      expect(deleted).toBe(2);

      const count = await adapter.count();
      expect(count).toBe(1);
    });

    it('should delete single record by id', async () => {
      const deleted = await adapter.delete({ id: 2 });

      expect(deleted).toBe(1);

      const record = await adapter.get(2);
      expect(record).toBeUndefined();
    });

    it('should return 0 for no matches', async () => {
      const deleted = await adapter.delete({ name: 'nonexistent' });
      expect(deleted).toBe(0);
    });
  });

  describe('deleteWithPredicate', () => {
    let adapter: StorageAdapter<TestRecord>;

    beforeEach(() => {
      adapter = createInMemoryAdapter<TestRecord>([
        { id: 1, name: 'a', value: 10, active: true },
        { id: 2, name: 'b', value: 25, active: true },
        { id: 3, name: 'c', value: 30, active: false },
      ]);
    });

    it('should delete with predicate', async () => {
      const deleted = await adapter.deleteWithPredicate(r => r.value < 30);

      expect(deleted).toBe(2);

      const remaining = await adapter.query({});
      expect(remaining).toHaveLength(1);
      expect(remaining[0].value).toBe(30);
    });
  });
});

// =============================================================================
// TABLE ACCESSOR TESTS
// =============================================================================

describe('createTableAccessor', () => {
  let adapter: StorageAdapter<UserRecord>;
  let accessor: TableAccessor<UserRecord>;

  beforeEach(() => {
    adapter = createInMemoryAdapter<UserRecord>([
      { id: 1, name: 'Alice', email: 'alice@test.com', age: 25 },
      { id: 2, name: 'Bob', email: 'bob@test.com', age: 30 },
      { id: 3, name: 'Charlie', email: 'charlie@test.com', age: 35 },
    ]);
    accessor = createTableAccessor(adapter);
  });

  describe('get', () => {
    it('should get record by key', async () => {
      const user = await accessor.get(1);
      expect(user?.name).toBe('Alice');
    });

    it('should return undefined for non-existent key', async () => {
      const user = await accessor.get(999);
      expect(user).toBeUndefined();
    });
  });

  describe('where', () => {
    it('should filter with partial object', async () => {
      const users = await accessor.where({ age: 30 });
      expect(users).toHaveLength(1);
      expect(users[0].name).toBe('Bob');
    });

    it('should filter with predicate function', async () => {
      const users = await accessor.where(u => u.age >= 30);
      expect(users).toHaveLength(2);
    });

    it('should apply query options', async () => {
      const users = await accessor.where({}, { limit: 2 });
      expect(users).toHaveLength(2);
    });
  });

  describe('all', () => {
    it('should return all records', async () => {
      const users = await accessor.all();
      expect(users).toHaveLength(3);
    });

    it('should apply options', async () => {
      const users = await accessor.all({ orderBy: 'age', orderDirection: 'desc' });
      expect(users[0].name).toBe('Charlie');
    });
  });

  describe('count', () => {
    it('should count all', async () => {
      const count = await accessor.count();
      expect(count).toBe(3);
    });

    it('should count with filter', async () => {
      const count = await accessor.count({ age: 30 });
      expect(count).toBe(1);
    });

    it('should count with predicate', async () => {
      const count = await accessor.count(u => u.age > 25);
      expect(count).toBe(2);
    });
  });

  describe('insert', () => {
    it('should insert new record', async () => {
      const user = await accessor.insert({
        name: 'Dave',
        email: 'dave@test.com',
        age: 40,
      });

      expect(user.id).toBeDefined();
      expect(user.name).toBe('Dave');

      const count = await accessor.count();
      expect(count).toBe(4);
    });
  });

  describe('update', () => {
    it('should update with filter', async () => {
      const count = await accessor.update({ id: 1 }, { age: 26 });
      expect(count).toBe(1);

      const user = await accessor.get(1);
      expect(user?.age).toBe(26);
    });

    it('should update with predicate', async () => {
      const count = await accessor.update(u => u.age >= 30, { email: 'updated@test.com' });
      expect(count).toBe(2);
    });
  });

  describe('delete', () => {
    it('should delete with filter', async () => {
      const count = await accessor.delete({ id: 2 });
      expect(count).toBe(1);

      const user = await accessor.get(2);
      expect(user).toBeUndefined();
    });

    it('should delete with predicate', async () => {
      const count = await accessor.delete(u => u.age < 30);
      expect(count).toBe(1);

      const remaining = await accessor.all();
      expect(remaining).toHaveLength(2);
    });
  });
});

// =============================================================================
// SQL FUNCTION TESTS
// =============================================================================

describe('createSqlFunction', () => {
  it('should create SQL template function', async () => {
    const executor: SqlExecutor = {
      async execute(sql, params) {
        return [{ sql, params }];
      },
    };

    const sql = createSqlFunction(executor);
    const results = await sql<{ sql: string; params: unknown[] }>`SELECT * FROM users WHERE id = ${1}`;

    expect(results[0].sql).toBe('SELECT * FROM users WHERE id = $1');
    expect(results[0].params).toEqual([1]);
  });

  it('should handle multiple parameters', async () => {
    const executor: SqlExecutor = {
      async execute(sql, params) {
        return [{ sql, params }];
      },
    };

    const sql = createSqlFunction(executor);
    const name = 'Alice';
    const age = 25;
    const results = await sql<{ sql: string; params: unknown[] }>`
      SELECT * FROM users WHERE name = ${name} AND age > ${age}
    `;

    expect(results[0].sql).toContain('$1');
    expect(results[0].sql).toContain('$2');
    expect(results[0].params).toEqual([name, age]);
  });

  it('should handle no parameters', async () => {
    const executor: SqlExecutor = {
      async execute(sql, params) {
        return [{ sql, params }];
      },
    };

    const sql = createSqlFunction(executor);
    const results = await sql<{ sql: string; params: unknown[] }>`SELECT COUNT(*) FROM users`;

    expect(results[0].sql).toBe('SELECT COUNT(*) FROM users');
    expect(results[0].params).toEqual([]);
  });
});

// =============================================================================
// IN-MEMORY SQL EXECUTOR TESTS
// =============================================================================

describe('createInMemorySqlExecutor', () => {
  it('should execute basic SELECT', async () => {
    const usersAdapter = createInMemoryAdapter<UserRecord>([
      { id: 1, name: 'Alice', email: 'alice@test.com', age: 25 },
    ]);

    const executor = createInMemorySqlExecutor(new Map([['users', usersAdapter]]));
    const results = await executor.execute('SELECT * FROM users', []);

    expect(results).toHaveLength(1);
  });

  it('should return empty for unsupported queries', async () => {
    const executor = createInMemorySqlExecutor(new Map());
    const results = await executor.execute('INSERT INTO users VALUES (1)', []);

    expect(results).toHaveLength(0);
  });

  it('should return empty for unknown table', async () => {
    const executor = createInMemorySqlExecutor(new Map());
    const results = await executor.execute('SELECT * FROM nonexistent', []);

    expect(results).toHaveLength(0);
  });
});

// =============================================================================
// TRANSACTION MANAGER TESTS
// =============================================================================

describe('createInMemoryTransactionManager', () => {
  let manager: TransactionManager;

  beforeEach(() => {
    manager = createInMemoryTransactionManager();
  });

  it('should begin transaction and return id', async () => {
    const txId = await manager.begin();

    expect(txId).toBeDefined();
    expect(txId).toMatch(/^tx_\d+$/);
  });

  it('should generate unique transaction ids', async () => {
    const tx1 = await manager.begin();
    const tx2 = await manager.begin();
    const tx3 = await manager.begin();

    expect(tx1).not.toBe(tx2);
    expect(tx2).not.toBe(tx3);
  });

  it('should commit transaction', async () => {
    const txId = await manager.begin();

    // Should not throw
    await manager.commit(txId);
  });

  it('should rollback transaction', async () => {
    const txId = await manager.begin();

    // Should not throw
    await manager.rollback(txId);
  });

  it('should execute SQL within transaction', async () => {
    const txId = await manager.begin();

    const results = await manager.execute(txId, 'SELECT 1', []);

    expect(results).toBeDefined();
    expect(Array.isArray(results)).toBe(true);
  });
});

// =============================================================================
// TRANSACTION FUNCTION TESTS
// =============================================================================

describe('createTransactionFunction', () => {
  interface TestDB extends DatabaseSchema {
    users: { id: 'number'; name: 'string' };
    orders: { id: 'number'; userId: 'number' };
  }

  it('should execute callback within transaction', async () => {
    const userAdapter = createInMemoryAdapter<{ id: number; name: string }>([]);
    const orderAdapter = createInMemoryAdapter<{ id: number; userId: number }>([]);

    const tableAdapters = new Map<string, StorageAdapter<Record<string, unknown>>>();
    tableAdapters.set('users', userAdapter as unknown as StorageAdapter<Record<string, unknown>>);
    tableAdapters.set('orders', orderAdapter as unknown as StorageAdapter<Record<string, unknown>>);

    const manager = createInMemoryTransactionManager();
    const transaction = createTransactionFunction<TestDB>(manager, tableAdapters);

    let callbackExecuted = false;

    await transaction(async (tx) => {
      callbackExecuted = true;
      expect(tx.tables).toBeDefined();
      expect(tx.sql).toBeDefined();
      expect(tx.commit).toBeDefined();
      expect(tx.rollback).toBeDefined();
    });

    expect(callbackExecuted).toBe(true);
  });

  it('should return callback result', async () => {
    const tableAdapters = new Map<string, StorageAdapter<Record<string, unknown>>>();
    const manager = createInMemoryTransactionManager();
    const transaction = createTransactionFunction<TestDB>(manager, tableAdapters);

    const result = await transaction(async () => {
      return 'transaction result';
    });

    expect(result).toBe('transaction result');
  });

  it('should rollback on error', async () => {
    const tableAdapters = new Map<string, StorageAdapter<Record<string, unknown>>>();
    const manager = createInMemoryTransactionManager();
    const transaction = createTransactionFunction<TestDB>(manager, tableAdapters);

    await expect(
      transaction(async () => {
        throw new Error('Transaction error');
      })
    ).rejects.toThrow('Transaction error');
  });
});

// =============================================================================
// DATABASE CONTEXT TESTS
// =============================================================================

describe('createDatabaseContext', () => {
  interface TestDB extends DatabaseSchema {
    users: { id: 'number'; name: 'string'; email: 'string' };
    orders: { id: 'number'; userId: 'number'; total: 'number' };
  }

  type UserRow = { id: number; name: string; email: string };
  type OrderRow = { id: number; userId: number; total: number };

  it('should create context with all components', () => {
    const userAdapter = createInMemoryAdapter<UserRow>([]);
    const orderAdapter = createInMemoryAdapter<OrderRow>([]);

    const tableMap = new Map<string, ReturnType<typeof createInMemoryAdapter>>();
    tableMap.set('users', userAdapter);
    tableMap.set('orders', orderAdapter);

    const db = createDatabaseContext<TestDB>({
      adapters: {
        users: userAdapter,
        orders: orderAdapter,
      } as any,
      sqlExecutor: createInMemorySqlExecutor(tableMap),
      transactionManager: createInMemoryTransactionManager(),
    });

    expect(db.tables).toBeDefined();
    expect(db.tables.users).toBeDefined();
    expect(db.tables.orders).toBeDefined();
    expect(db.sql).toBeDefined();
    expect(db.transaction).toBeDefined();
  });

  it('should provide working table accessors', async () => {
    const userAdapter = createInMemoryAdapter<UserRow>([
      { id: 1, name: 'Test', email: 'test@test.com' },
    ]);
    const orderAdapter = createInMemoryAdapter<OrderRow>([]);

    const tableMap = new Map<string, ReturnType<typeof createInMemoryAdapter>>();
    tableMap.set('users', userAdapter);
    tableMap.set('orders', orderAdapter);

    const db = createDatabaseContext<TestDB>({
      adapters: {
        users: userAdapter,
        orders: orderAdapter,
      } as any,
      sqlExecutor: createInMemorySqlExecutor(tableMap),
      transactionManager: createInMemoryTransactionManager(),
    });

    const user = await db.tables.users.get(1);
    expect(user?.name).toBe('Test');
  });

  it('should provide working SQL function', async () => {
    const userAdapter = createInMemoryAdapter<UserRow>([
      { id: 1, name: 'Test', email: 'test@test.com' },
    ]);
    const orderAdapter = createInMemoryAdapter<OrderRow>([]);

    const tableMap = new Map<string, ReturnType<typeof createInMemoryAdapter>>();
    tableMap.set('users', userAdapter);
    tableMap.set('orders', orderAdapter);

    const db = createDatabaseContext<TestDB>({
      adapters: {
        users: userAdapter,
        orders: orderAdapter,
      } as any,
      sqlExecutor: createInMemorySqlExecutor(tableMap),
      transactionManager: createInMemoryTransactionManager(),
    });

    const results = await db.sql`SELECT * FROM users`;
    expect(results).toHaveLength(1);
  });

  it('should provide working transaction function', async () => {
    const userAdapter = createInMemoryAdapter<UserRow>([]);
    const orderAdapter = createInMemoryAdapter<OrderRow>([]);

    const tableMap = new Map<string, ReturnType<typeof createInMemoryAdapter>>();
    tableMap.set('users', userAdapter);
    tableMap.set('orders', orderAdapter);

    const db = createDatabaseContext<TestDB>({
      adapters: {
        users: userAdapter,
        orders: orderAdapter,
      } as any,
      sqlExecutor: createInMemorySqlExecutor(tableMap),
      transactionManager: createInMemoryTransactionManager(),
    });

    const result = await db.transaction(async (tx) => {
      return 'from transaction';
    });

    expect(result).toBe('from transaction');
  });
});
