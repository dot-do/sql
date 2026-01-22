/**
 * Tests for DoSQL Secondary Index Support
 *
 * Tests:
 * - Index creation and lookup
 * - Composite index queries
 * - Optimizer choosing index vs scan
 * - EXPLAIN output verification
 */

import { describe, it, expect, beforeEach } from 'vitest';

import type { FSXBackend } from '../fsx/types.js';
import type { Row, Schema, Predicate, QueryPlan } from '../engine/types.js';

// Import types
import {
  type IndexDefinition,
  type IndexEntry,
  type IndexScanRange,
  createIndex,
  createCompositeIndex,
  pointLookup,
  rangeScan,
  isIndexCovering,
  countLeadingEqualities,
} from './types.js';

// Import secondary index implementation
import {
  SecondaryIndex,
  IndexManager,
  createSecondaryIndex,
  createIndexManager,
} from './secondary.js';

// Import optimizer
import {
  QueryOptimizer,
  createOptimizer,
  selectIndex,
  analyzePredicate,
  estimateJoinCost,
  suggestJoinOrder,
  DEFAULT_COST_MODEL,
} from './optimizer.js';

// Import explain
import {
  explain,
  ExplainGenerator,
  createExplainGenerator,
} from './explain.js';

// =============================================================================
// MOCK FSX BACKEND
// =============================================================================

/**
 * Simple in-memory FSX backend for testing
 */
class MockFSXBackend implements FSXBackend {
  private storage = new Map<string, Uint8Array>();

  async read(key: string): Promise<Uint8Array | null> {
    return this.storage.get(key) ?? null;
  }

  async write(key: string, data: Uint8Array): Promise<void> {
    this.storage.set(key, data);
  }

  async delete(key: string): Promise<boolean> {
    return this.storage.delete(key);
  }

  async list(prefix: string): Promise<string[]> {
    return Array.from(this.storage.keys()).filter(k => k.startsWith(prefix));
  }

  async exists(key: string): Promise<boolean> {
    return this.storage.has(key);
  }

  clear(): void {
    this.storage.clear();
  }
}

// =============================================================================
// INDEX TYPES TESTS
// =============================================================================

describe('Index Types', () => {
  describe('createIndex', () => {
    it('should create a simple single-column index', () => {
      const index = createIndex('idx_users_email', 'users', 'email');

      expect(index.name).toBe('idx_users_email');
      expect(index.table).toBe('users');
      expect(index.columns).toHaveLength(1);
      expect(index.columns[0].name).toBe('email');
      expect(index.columns[0].direction).toBe('asc');
      expect(index.unique).toBe(false);
    });

    it('should create a unique index', () => {
      const index = createIndex('idx_users_email', 'users', 'email', { unique: true });

      expect(index.unique).toBe(true);
    });

    it('should create an index with descending order', () => {
      const index = createIndex('idx_orders_date', 'orders', 'created_at', { direction: 'desc' });

      expect(index.columns[0].direction).toBe('desc');
    });

    it('should create an index with INCLUDE columns', () => {
      const index = createIndex('idx_users_email', 'users', 'email', {
        include: ['name', 'status'],
      });

      expect(index.include).toEqual(['name', 'status']);
    });
  });

  describe('createCompositeIndex', () => {
    it('should create a composite index from column names', () => {
      const index = createCompositeIndex('idx_orders_user_date', 'orders', [
        'user_id',
        'created_at',
      ]);

      expect(index.columns).toHaveLength(2);
      expect(index.columns[0].name).toBe('user_id');
      expect(index.columns[1].name).toBe('created_at');
    });

    it('should create a composite index with mixed directions', () => {
      const index = createCompositeIndex('idx_orders_user_date', 'orders', [
        { name: 'user_id', direction: 'asc' },
        { name: 'created_at', direction: 'desc' },
      ]);

      expect(index.columns[0].direction).toBe('asc');
      expect(index.columns[1].direction).toBe('desc');
    });
  });

  describe('pointLookup', () => {
    it('should create a point lookup range', () => {
      const range = pointLookup([1, 'test']);

      expect(range.start?.key).toEqual([1, 'test']);
      expect(range.start?.inclusive).toBe(true);
      expect(range.end?.key).toEqual([1, 'test']);
      expect(range.end?.inclusive).toBe(true);
    });
  });

  describe('rangeScan', () => {
    it('should create a range scan with bounds', () => {
      const range = rangeScan(
        { key: [10], inclusive: true },
        { key: [20], inclusive: false }
      );

      expect(range.start?.key).toEqual([10]);
      expect(range.start?.inclusive).toBe(true);
      expect(range.end?.key).toEqual([20]);
      expect(range.end?.inclusive).toBe(false);
    });

    it('should create an open-ended range', () => {
      const range = rangeScan({ key: [10] });

      expect(range.start?.key).toEqual([10]);
      expect(range.end).toBeUndefined();
    });
  });

  describe('isIndexCovering', () => {
    it('should detect a covering index', () => {
      const index = createIndex('idx', 'users', 'email', { include: ['name'] });
      const info = isIndexCovering(index, ['email', 'name']);

      expect(info.isCovering).toBe(true);
      expect(info.missingColumns).toHaveLength(0);
    });

    it('should detect a non-covering index', () => {
      const index = createIndex('idx', 'users', 'email');
      const info = isIndexCovering(index, ['email', 'name', 'status']);

      expect(info.isCovering).toBe(false);
      expect(info.missingColumns).toContain('name');
      expect(info.missingColumns).toContain('status');
    });
  });

  describe('countLeadingEqualities', () => {
    it('should count consecutive leading equalities', () => {
      const index = createCompositeIndex('idx', 'orders', ['user_id', 'status', 'date']);
      const predicates = [
        { columnIndex: 0, column: 'user_id', operator: 'eq' as const, value: 1, isEquality: true },
        { columnIndex: 1, column: 'status', operator: 'eq' as const, value: 'active', isEquality: true },
        { columnIndex: 2, column: 'date', operator: 'gt' as const, value: new Date(), isEquality: false },
      ];

      const count = countLeadingEqualities(predicates, index.columns);
      expect(count).toBe(2);
    });

    it('should return 0 for no leading equality', () => {
      const index = createCompositeIndex('idx', 'orders', ['user_id', 'status']);
      const predicates = [
        { columnIndex: 0, column: 'user_id', operator: 'gt' as const, value: 1, isEquality: false },
      ];

      const count = countLeadingEqualities(predicates, index.columns);
      expect(count).toBe(0);
    });
  });
});

// =============================================================================
// SECONDARY INDEX TESTS
// =============================================================================

describe('SecondaryIndex', () => {
  let fsx: MockFSXBackend;
  let index: SecondaryIndex;

  beforeEach(async () => {
    fsx = new MockFSXBackend();
    const definition = createIndex('idx_users_email', 'users', 'email', { unique: true });
    index = createSecondaryIndex(fsx, definition);
    await index.init();
  });

  describe('insert', () => {
    it('should insert an entry into the index', async () => {
      const result = await index.insert(['alice@example.com'], 1);

      expect(result).toBeNull(); // No violation

      const rowIds = await index.lookup(['alice@example.com']);
      expect(rowIds).toEqual([1]);
    });

    it('should detect unique constraint violations', async () => {
      await index.insert(['alice@example.com'], 1);
      const result = await index.insert(['alice@example.com'], 2);

      expect(result).not.toBeNull();
      expect(result?.existingRowId).toBe(1);
      expect(result?.newRowId).toBe(2);
    });
  });

  describe('delete', () => {
    it('should delete an entry from the index', async () => {
      await index.insert(['alice@example.com'], 1);
      const deleted = await index.delete(['alice@example.com'], 1);

      expect(deleted).toBe(true);

      const rowIds = await index.lookup(['alice@example.com']);
      expect(rowIds).toHaveLength(0);
    });

    it('should return false when deleting non-existent entry', async () => {
      const deleted = await index.delete(['notfound@example.com'], 1);
      expect(deleted).toBe(false);
    });
  });

  describe('update', () => {
    it('should update an index entry when key changes', async () => {
      await index.insert(['alice@example.com'], 1);
      const result = await index.update(
        ['alice@example.com'],
        ['alice.new@example.com'],
        1
      );

      expect(result).toBeNull();

      const oldLookup = await index.lookup(['alice@example.com']);
      expect(oldLookup).toHaveLength(0);

      const newLookup = await index.lookup(['alice.new@example.com']);
      expect(newLookup).toEqual([1]);
    });
  });

  describe('scan', () => {
    beforeEach(async () => {
      await index.insert(['alice@example.com'], 1);
      await index.insert(['bob@example.com'], 2);
      await index.insert(['charlie@example.com'], 3);
    });

    it('should scan all entries', async () => {
      const entries: IndexEntry[] = [];
      for await (const entry of index.scan({})) {
        entries.push(entry);
      }

      expect(entries).toHaveLength(3);
    });

    it('should scan with limit', async () => {
      const entries: IndexEntry[] = [];
      for await (const entry of index.scan({ limit: 2 })) {
        entries.push(entry);
      }

      expect(entries).toHaveLength(2);
    });

    it('should scan a range', async () => {
      const entries: IndexEntry[] = [];
      for await (const entry of index.scan({
        start: { key: ['alice@example.com'], inclusive: true },
        end: { key: ['charlie@example.com'], inclusive: false },
      })) {
        entries.push(entry);
      }

      // Should include alice and bob, but not charlie
      expect(entries.length).toBeGreaterThanOrEqual(1);
    });
  });
});

describe('Non-unique SecondaryIndex', () => {
  let fsx: MockFSXBackend;
  let index: SecondaryIndex;

  beforeEach(async () => {
    fsx = new MockFSXBackend();
    const definition = createIndex('idx_orders_status', 'orders', 'status', { unique: false });
    index = createSecondaryIndex(fsx, definition);
    await index.init();
  });

  it('should allow multiple row IDs per key', async () => {
    await index.insert(['pending'], 1);
    await index.insert(['pending'], 2);
    await index.insert(['pending'], 3);

    const rowIds = await index.lookup(['pending']);
    expect(rowIds).toHaveLength(3);
    expect(rowIds).toContain(1);
    expect(rowIds).toContain(2);
    expect(rowIds).toContain(3);
  });

  it('should delete only the specified row ID', async () => {
    await index.insert(['pending'], 1);
    await index.insert(['pending'], 2);

    await index.delete(['pending'], 1);

    const rowIds = await index.lookup(['pending']);
    expect(rowIds).toEqual([2]);
  });
});

describe('Composite SecondaryIndex', () => {
  let fsx: MockFSXBackend;
  let index: SecondaryIndex;

  beforeEach(async () => {
    fsx = new MockFSXBackend();
    const definition = createCompositeIndex('idx_orders_user_status', 'orders', [
      'user_id',
      'status',
    ]);
    index = createSecondaryIndex(fsx, definition);
    await index.init();
  });

  it('should insert and lookup composite keys', async () => {
    await index.insert([1, 'pending'], 100);
    await index.insert([1, 'completed'], 101);
    await index.insert([2, 'pending'], 102);

    const rowIds1 = await index.lookup([1, 'pending']);
    expect(rowIds1).toEqual([100]);

    const rowIds2 = await index.lookup([1, 'completed']);
    expect(rowIds2).toEqual([101]);
  });

  it('should support prefix scans', async () => {
    await index.insert([1, 'pending'], 100);
    await index.insert([1, 'completed'], 101);
    await index.insert([2, 'pending'], 102);

    // Scan all entries for user_id = 1
    const entries: IndexEntry[] = [];
    for await (const entry of index.scan({
      start: { key: [1], inclusive: true },
    })) {
      if (entry.key[0] !== 1) break;
      entries.push(entry);
    }

    expect(entries).toHaveLength(2);
  });

  it('should extract keys from rows', () => {
    const row: Row = { id: 100, user_id: 1, status: 'pending', amount: 99.99 };
    const key = index.extractKey(row);

    expect(key).toEqual([1, 'pending']);
  });
});

// =============================================================================
// INDEX MANAGER TESTS
// =============================================================================

describe('IndexManager', () => {
  let fsx: MockFSXBackend;
  let manager: IndexManager;

  beforeEach(() => {
    fsx = new MockFSXBackend();
    manager = createIndexManager(fsx);
  });

  it('should create and track indexes', async () => {
    const definition = createIndex('idx_users_email', 'users', 'email');
    await manager.createIndex(definition);

    const index = manager.getIndex('users', 'idx_users_email');
    expect(index).toBeDefined();
  });

  it('should get all indexes for a table', async () => {
    await manager.createIndex(createIndex('idx1', 'users', 'email'));
    await manager.createIndex(createIndex('idx2', 'users', 'name'));

    const indexes = manager.getTableIndexes('users');
    expect(indexes).toHaveLength(2);
  });

  it('should maintain indexes on insert', async () => {
    await manager.createIndex(createIndex('idx_users_email', 'users', 'email', { unique: true }));

    const result = await manager.onInsert('users', 1, { email: 'alice@example.com' });
    expect(result.inserted).toBe(1);
    expect(result.violations).toHaveLength(0);

    const index = manager.getIndex('users', 'idx_users_email')!;
    const rowIds = await index.lookup(['alice@example.com']);
    expect(rowIds).toEqual([1]);
  });

  it('should maintain indexes on delete', async () => {
    await manager.createIndex(createIndex('idx_users_email', 'users', 'email'));

    await manager.onInsert('users', 1, { email: 'alice@example.com' });
    await manager.onDelete('users', 1, { email: 'alice@example.com' });

    const index = manager.getIndex('users', 'idx_users_email')!;
    const rowIds = await index.lookup(['alice@example.com']);
    expect(rowIds).toHaveLength(0);
  });

  it('should maintain indexes on update', async () => {
    await manager.createIndex(createIndex('idx_users_email', 'users', 'email'));

    await manager.onInsert('users', 1, { email: 'alice@example.com' });
    await manager.onUpdate(
      'users',
      1,
      { email: 'alice@example.com' },
      { email: 'alice.new@example.com' }
    );

    const index = manager.getIndex('users', 'idx_users_email')!;

    const oldLookup = await index.lookup(['alice@example.com']);
    expect(oldLookup).toHaveLength(0);

    const newLookup = await index.lookup(['alice.new@example.com']);
    expect(newLookup).toEqual([1]);
  });

  it('should drop indexes', async () => {
    await manager.createIndex(createIndex('idx_users_email', 'users', 'email'));
    const dropped = await manager.dropIndex('users', 'idx_users_email');

    expect(dropped).toBe(true);
    expect(manager.getIndex('users', 'idx_users_email')).toBeUndefined();
  });
});

// =============================================================================
// OPTIMIZER TESTS
// =============================================================================

describe('Query Optimizer', () => {
  describe('analyzePredicate', () => {
    const index = createCompositeIndex('idx', 'orders', ['user_id', 'status', 'date']);

    it('should extract indexable predicates', () => {
      const predicate: Predicate = {
        type: 'comparison',
        op: 'eq',
        left: { type: 'columnRef', column: 'user_id' },
        right: { type: 'literal', value: 1, dataType: 'number' },
      };

      const analysis = analyzePredicate(predicate, index);

      expect(analysis.indexPredicates).toHaveLength(1);
      expect(analysis.indexPredicates[0].column).toBe('user_id');
      expect(analysis.indexPredicates[0].isEquality).toBe(true);
      expect(analysis.filterPredicates).toHaveLength(0);
    });

    it('should separate non-indexable predicates', () => {
      const predicate: Predicate = {
        type: 'logical',
        op: 'and',
        operands: [
          {
            type: 'comparison',
            op: 'eq',
            left: { type: 'columnRef', column: 'user_id' },
            right: { type: 'literal', value: 1, dataType: 'number' },
          },
          {
            type: 'comparison',
            op: 'like',
            left: { type: 'columnRef', column: 'description' }, // Not in index
            right: { type: 'literal', value: '%test%', dataType: 'string' },
          },
        ],
      };

      const analysis = analyzePredicate(predicate, index);

      expect(analysis.indexPredicates).toHaveLength(1);
      expect(analysis.filterPredicates).toHaveLength(1);
      expect(analysis.isFullyIndexable).toBe(false);
    });
  });

  describe('selectIndex', () => {
    const indexes = [
      createIndex('idx_email', 'users', 'email', { unique: true }),
      createCompositeIndex('idx_name_status', 'users', ['name', 'status']),
    ];

    it('should choose table scan when no predicate', () => {
      const result = selectIndex(undefined, {
        indexes,
        requiredColumns: ['id', 'email', 'name'],
      });

      expect(result.accessMethod).toBe('tableScan');
    });

    it('should choose index when predicate matches', () => {
      const predicate: Predicate = {
        type: 'comparison',
        op: 'eq',
        left: { type: 'columnRef', column: 'email' },
        right: { type: 'literal', value: 'alice@example.com', dataType: 'string' },
      };

      const result = selectIndex(predicate, {
        indexes,
        requiredColumns: ['id', 'email'],
        tableStats: { rowCount: 10000, avgRowSize: 100 },
      });

      // Should choose index scan (point lookup is very efficient)
      expect(result.accessMethod).not.toBe('tableScan');
      expect(result.selectedIndex?.name).toBe('idx_email');
    });

    it('should prefer covering index', () => {
      const coveringIndex = createIndex('idx_email_covering', 'users', 'email', {
        include: ['name'],
      });

      const predicate: Predicate = {
        type: 'comparison',
        op: 'eq',
        left: { type: 'columnRef', column: 'email' },
        right: { type: 'literal', value: 'alice@example.com', dataType: 'string' },
      };

      const result = selectIndex(predicate, {
        indexes: [coveringIndex],
        requiredColumns: ['email', 'name'],
        tableStats: { rowCount: 10000, avgRowSize: 100 },
      });

      expect(result.accessMethod).toBe('indexOnlyScan');
    });
  });

  describe('estimateJoinCost', () => {
    it('should estimate inner join cost', () => {
      const estimate = estimateJoinCost(1000, 100, 'inner', true);

      expect(estimate.resultRows).toBeGreaterThan(0);
      expect(estimate.cost).toBeGreaterThan(0);
    });

    it('should estimate cross join cost', () => {
      const estimate = estimateJoinCost(100, 100, 'cross', false);

      expect(estimate.resultRows).toBe(10000); // 100 * 100
    });

    it('should estimate left join preserving left rows', () => {
      const estimate = estimateJoinCost(1000, 100, 'left', true);

      expect(estimate.resultRows).toBe(1000);
    });
  });

  describe('suggestJoinOrder', () => {
    it('should start with smallest table', () => {
      const tables = [
        { name: 'orders', rowCount: 10000 },
        { name: 'users', rowCount: 100 },
        { name: 'products', rowCount: 500 },
      ];

      const joins = [
        { left: 'orders', right: 'users' },
        { left: 'orders', right: 'products' },
      ];

      const order = suggestJoinOrder(tables, joins);

      expect(order[0]).toBe('users'); // Smallest table first
    });
  });

  describe('QueryOptimizer', () => {
    it('should optimize a scan plan to use index', () => {
      const schema: Schema = {
        tables: new Map([
          ['users', {
            name: 'users',
            columns: [
              { name: 'id', type: 'number', nullable: false, primaryKey: true },
              { name: 'email', type: 'string', nullable: false },
            ],
            primaryKey: ['id'],
          }],
        ]),
      };

      const indexes = new Map([
        ['users', [createIndex('idx_email', 'users', 'email', { unique: true })]],
      ]);

      const optimizer = createOptimizer({
        schema,
        indexes,
        tableStats: new Map([['users', { rowCount: 10000, avgRowSize: 100 }]]),
      });

      const scanPlan: QueryPlan = {
        id: 1,
        type: 'scan',
        table: 'users',
        source: 'btree',
        columns: ['id', 'email'],
        predicate: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'email' },
          right: { type: 'literal', value: 'test@example.com', dataType: 'string' },
        },
      };

      const optimized = optimizer.optimize(scanPlan);

      // Should convert to index lookup
      expect(optimized.type).toBe('indexLookup');
    });

    it('should estimate plan cost', () => {
      const schema: Schema = { tables: new Map() };
      const optimizer = createOptimizer({ schema, indexes: new Map() });

      const plan: QueryPlan = {
        id: 1,
        type: 'scan',
        table: 'users',
        source: 'btree',
        columns: ['*'],
      };

      const cost = optimizer.estimateCost(plan);

      expect(cost.totalCost).toBeGreaterThan(0);
      expect(cost.estimatedRows).toBeGreaterThan(0);
    });
  });
});

// =============================================================================
// EXPLAIN TESTS
// =============================================================================

describe('EXPLAIN', () => {
  describe('explain()', () => {
    it('should generate text explain for scan', () => {
      const plan: QueryPlan = {
        id: 1,
        type: 'scan',
        table: 'users',
        source: 'btree',
        columns: ['id', 'name', 'email'],
      };

      const output = explain(plan, { format: 'text', costs: false });

      expect(output).toContain('Sequential Scan');
      expect(output).toContain('users');
    });

    it('should generate text explain for index lookup', () => {
      const plan: QueryPlan = {
        id: 1,
        type: 'indexLookup',
        table: 'users',
        index: 'idx_users_email',
        lookupKey: [{ type: 'literal', value: 'test@example.com', dataType: 'string' }],
        columns: ['id', 'email'],
      };

      const output = explain(plan, { format: 'text', costs: false });

      expect(output).toContain('Index Scan');
      expect(output).toContain('idx_users_email');
    });

    it('should generate text explain for filter', () => {
      const plan: QueryPlan = {
        id: 2,
        type: 'filter',
        input: {
          id: 1,
          type: 'scan',
          table: 'users',
          source: 'btree',
          columns: ['*'],
        },
        predicate: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', column: 'status' },
          right: { type: 'literal', value: 'active', dataType: 'string' },
        },
      };

      const output = explain(plan, { format: 'text', costs: false });

      expect(output).toContain('Filter');
      expect(output).toContain('status');
    });

    it('should generate text explain for join', () => {
      const plan: QueryPlan = {
        id: 3,
        type: 'join',
        joinType: 'inner',
        algorithm: 'hash',
        left: {
          id: 1,
          type: 'scan',
          table: 'users',
          source: 'btree',
          columns: ['*'],
        },
        right: {
          id: 2,
          type: 'scan',
          table: 'orders',
          source: 'btree',
          columns: ['*'],
        },
        condition: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', table: 'users', column: 'id' },
          right: { type: 'columnRef', table: 'orders', column: 'user_id' },
        },
      };

      const output = explain(plan, { format: 'text', costs: false });

      expect(output).toContain('Hash Join');
      expect(output).toContain('Inner');
    });

    it('should generate JSON explain', () => {
      const plan: QueryPlan = {
        id: 1,
        type: 'scan',
        table: 'users',
        source: 'btree',
        columns: ['*'],
      };

      const output = explain(plan, { format: 'json' });
      const parsed = JSON.parse(output);

      expect(parsed.nodeType).toBe('Seq Scan');
      expect(parsed.relationName).toBe('users');
    });

    it('should generate tree explain', () => {
      // Use a join plan with two children to test both '`--' and '|--'
      const plan: QueryPlan = {
        id: 3,
        type: 'join',
        joinType: 'inner',
        algorithm: 'hash',
        left: {
          id: 1,
          type: 'scan',
          table: 'users',
          source: 'btree',
          columns: ['*'],
        },
        right: {
          id: 2,
          type: 'scan',
          table: 'orders',
          source: 'btree',
          columns: ['*'],
        },
        condition: {
          type: 'comparison',
          op: 'eq',
          left: { type: 'columnRef', table: 'users', column: 'id' },
          right: { type: 'columnRef', table: 'orders', column: 'user_id' },
        },
      };

      const output = explain(plan, { format: 'tree' });

      expect(output).toContain('`--'); // Last child connector
      expect(output).toContain('|--'); // First child connector
    });
  });

  describe('ExplainGenerator', () => {
    it('should use default options', () => {
      const generator = createExplainGenerator();

      const plan: QueryPlan = {
        id: 1,
        type: 'scan',
        table: 'users',
        source: 'btree',
        columns: ['*'],
      };

      const output = generator.explain(plan);
      expect(output).toBeTruthy();
    });

    it('should show cost estimates when optimizer provided', () => {
      const schema: Schema = {
        tables: new Map([
          ['users', {
            name: 'users',
            columns: [{ name: 'id', type: 'number', nullable: false }],
          }],
        ]),
      };

      const optimizer = createOptimizer({
        schema,
        indexes: new Map(),
        tableStats: new Map([['users', { rowCount: 1000, avgRowSize: 50 }]]),
      });

      const generator = createExplainGenerator({
        costs: true,
        rowEstimates: true,
        optimizer,
      });

      const plan: QueryPlan = {
        id: 1,
        type: 'scan',
        table: 'users',
        source: 'btree',
        columns: ['*'],
      };

      const output = generator.explain(plan);

      expect(output).toContain('Cost:');
      expect(output).toContain('Rows:');
    });

    it('should show verbose output', () => {
      const generator = createExplainGenerator({
        verbose: true,
        costs: false,
      });

      const plan: QueryPlan = {
        id: 1,
        type: 'scan',
        table: 'users',
        source: 'btree',
        columns: ['id', 'name', 'email'],
      };

      const output = generator.explain(plan);

      expect(output).toContain('Output:');
      expect(output).toContain('id');
      expect(output).toContain('name');
      expect(output).toContain('email');
    });
  });

  describe('Complex query plans', () => {
    it('should explain aggregate with group by', () => {
      const plan: QueryPlan = {
        id: 2,
        type: 'aggregate',
        input: {
          id: 1,
          type: 'scan',
          table: 'orders',
          source: 'columnar',
          columns: ['*'],
        },
        groupBy: [{ type: 'columnRef', column: 'status' }],
        aggregates: [
          {
            expr: { type: 'aggregate', function: 'count', arg: '*' },
            alias: 'count',
          },
          {
            expr: { type: 'aggregate', function: 'sum', arg: { type: 'columnRef', column: 'amount' } },
            alias: 'total',
          },
        ],
      };

      const output = explain(plan, { format: 'text', costs: false });

      expect(output).toContain('HashAggregate');
      expect(output).toContain('Group By');
    });

    it('should explain sort with order by', () => {
      const plan: QueryPlan = {
        id: 2,
        type: 'sort',
        input: {
          id: 1,
          type: 'scan',
          table: 'users',
          source: 'btree',
          columns: ['*'],
        },
        orderBy: [
          { expr: { type: 'columnRef', column: 'name' }, direction: 'asc' },
          { expr: { type: 'columnRef', column: 'created_at' }, direction: 'desc' },
        ],
      };

      const output = explain(plan, { format: 'text', costs: false });

      expect(output).toContain('Sort');
      expect(output).toContain('ASC');
      expect(output).toContain('DESC');
    });

    it('should explain limit with offset', () => {
      const plan: QueryPlan = {
        id: 2,
        type: 'limit',
        input: {
          id: 1,
          type: 'scan',
          table: 'users',
          source: 'btree',
          columns: ['*'],
        },
        limit: 10,
        offset: 20,
      };

      const output = explain(plan, { format: 'text', costs: false });

      expect(output).toContain('Limit');
      expect(output).toContain('10');
      expect(output).toContain('OFFSET');
      expect(output).toContain('20');
    });

    it('should explain union', () => {
      const plan: QueryPlan = {
        id: 3,
        type: 'union',
        all: false,
        inputs: [
          {
            id: 1,
            type: 'scan',
            table: 'users',
            source: 'btree',
            columns: ['id', 'name'],
          },
          {
            id: 2,
            type: 'scan',
            table: 'admins',
            source: 'btree',
            columns: ['id', 'name'],
          },
        ],
      };

      const output = explain(plan, { format: 'text', costs: false });

      expect(output).toContain('UNION');
    });
  });
});
