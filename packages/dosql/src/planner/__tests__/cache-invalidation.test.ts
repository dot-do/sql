/**
 * DoSQL Query Plan Cache - Schema Invalidation Tests
 *
 * TDD tests for plan cache invalidation on schema changes:
 * - Cache entry invalidated after ALTER TABLE
 * - Cache entry invalidated after CREATE INDEX (plan may change to use index)
 * - Cache entry invalidated after DROP TABLE
 * - Cache survives unrelated DDL (different table)
 *
 * Issue: sql-yxog - Add plan cache schema invalidation
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  createQueryPlanCache,
  computePlanCacheKey,
  normalizeQueryForCache,
  processDDL,
  notifyIndexChange,
  type QueryPlanCacheImpl,
} from '../cache.js';

// =============================================================================
// TEST HELPERS
// =============================================================================

/**
 * Create a mock plan for testing
 */
function mockPlan(table: string, columns?: string[]): { table: string; plan: string; tables?: string[] } {
  return {
    table,
    plan: `SeqScan on ${table}`,
    tables: columns ? undefined : [table],
  };
}

/**
 * Create a join plan referencing multiple tables
 */
function mockJoinPlan(tables: string[]): { tables: string[]; plan: string } {
  return {
    tables,
    plan: `HashJoin on ${tables.join(', ')}`,
  };
}

/**
 * Helper to create a normalized cache key from SQL
 */
function cacheKey(sql: string): string {
  return computePlanCacheKey(normalizeQueryForCache(sql));
}

// =============================================================================
// SCHEMA VERSION TRACKING
// =============================================================================

describe('Schema Version Tracking', () => {
  let cache: QueryPlanCacheImpl;

  beforeEach(() => {
    cache = createQueryPlanCache({
      maxSize: 100,
      trackTableDependencies: true,
    });
  });

  describe('Basic schema version functionality', () => {
    it('should store schema version with cached plan', () => {
      const sql = 'SELECT * FROM users WHERE id = 1';
      const queryHash = cacheKey(sql);
      const plan = mockPlan('users');

      cache.set(queryHash, plan, 5); // Schema version 5

      const cached = cache.get(queryHash);
      expect(cached).toBeDefined();
      expect(cached!.schemaVersion).toBe(5);
    });

    it('should identify stale plans when schema version increases', () => {
      const sql = 'SELECT * FROM users WHERE id = 1';
      const queryHash = cacheKey(sql);
      const plan = mockPlan('users');

      cache.set(queryHash, plan, 5);

      // Plan cached at version 5, current schema is version 7
      expect(cache.isPlanStale(queryHash, 7)).toBe(true);
      expect(cache.isPlanStale(queryHash, 5)).toBe(false);
      expect(cache.isPlanStale(queryHash, 4)).toBe(false);
    });

    it('should update schema version when plan is refreshed', () => {
      const sql = 'SELECT * FROM users WHERE id = 1';
      const queryHash = cacheKey(sql);
      const plan = mockPlan('users');

      cache.set(queryHash, plan, 5);
      expect(cache.get(queryHash)!.schemaVersion).toBe(5);

      // Update plan at new schema version
      cache.set(queryHash, plan, 8);
      expect(cache.get(queryHash)!.schemaVersion).toBe(8);
    });
  });
});

// =============================================================================
// ALTER TABLE INVALIDATION
// =============================================================================

describe('Cache Invalidation on ALTER TABLE', () => {
  let cache: QueryPlanCacheImpl;

  beforeEach(() => {
    cache = createQueryPlanCache({
      maxSize: 100,
      trackTableDependencies: true,
    });
  });

  it('should invalidate plan when ALTER TABLE ADD COLUMN is executed', () => {
    const sql = 'SELECT * FROM users WHERE id = 1';
    const queryHash = cacheKey(sql);

    // Cache a plan for users table
    cache.set(queryHash, mockPlan('users'), 1);
    expect(cache.has(queryHash)).toBe(true);

    // Execute ALTER TABLE
    processDDL(cache, 'ALTER TABLE users ADD COLUMN email TEXT');

    // Plan should be invalidated
    expect(cache.has(queryHash)).toBe(false);
  });

  it('should invalidate plan when ALTER TABLE DROP COLUMN is executed', () => {
    const sql = 'SELECT name, email FROM users';
    const queryHash = cacheKey(sql);

    cache.set(queryHash, mockPlan('users'), 1);
    expect(cache.has(queryHash)).toBe(true);

    processDDL(cache, 'ALTER TABLE users DROP COLUMN email');

    expect(cache.has(queryHash)).toBe(false);
  });

  it('should invalidate plan when ALTER TABLE RENAME COLUMN is executed', () => {
    const sql = 'SELECT name FROM users';
    const queryHash = cacheKey(sql);

    cache.set(queryHash, mockPlan('users'), 1);
    expect(cache.has(queryHash)).toBe(true);

    processDDL(cache, 'ALTER TABLE users RENAME COLUMN name TO full_name');

    expect(cache.has(queryHash)).toBe(false);
  });

  it('should invalidate plan when ALTER TABLE RENAME is executed', () => {
    const sql = 'SELECT * FROM users';
    const queryHash = cacheKey(sql);

    cache.set(queryHash, mockPlan('users'), 1);
    expect(cache.has(queryHash)).toBe(true);

    processDDL(cache, 'ALTER TABLE users RENAME TO members');

    expect(cache.has(queryHash)).toBe(false);
  });

  it('should not invalidate plans for unrelated tables', () => {
    const usersQuery = 'SELECT * FROM users';
    const ordersQuery = 'SELECT * FROM orders';
    const usersHash = cacheKey(usersQuery);
    const ordersHash = cacheKey(ordersQuery);

    cache.set(usersHash, mockPlan('users'), 1);
    cache.set(ordersHash, mockPlan('orders'), 1);

    expect(cache.has(usersHash)).toBe(true);
    expect(cache.has(ordersHash)).toBe(true);

    // ALTER TABLE on users should not affect orders
    processDDL(cache, 'ALTER TABLE users ADD COLUMN status TEXT');

    expect(cache.has(usersHash)).toBe(false);
    expect(cache.has(ordersHash)).toBe(true); // Still cached
  });
});

// =============================================================================
// CREATE INDEX INVALIDATION
// =============================================================================

describe('Cache Invalidation on CREATE INDEX', () => {
  let cache: QueryPlanCacheImpl;

  beforeEach(() => {
    cache = createQueryPlanCache({
      maxSize: 100,
      trackTableDependencies: true,
    });
  });

  it('should invalidate plan when CREATE INDEX is executed (plan may use new index)', () => {
    const sql = 'SELECT * FROM users WHERE email = ?';
    const queryHash = cacheKey(sql);

    // Cache a plan that uses SeqScan
    cache.set(queryHash, { table: 'users', plan: 'SeqScan' }, 1);
    expect(cache.has(queryHash)).toBe(true);

    // Create index that could change the plan to IndexScan
    processDDL(cache, 'CREATE INDEX idx_users_email ON users(email)');

    // Plan should be invalidated so optimizer can consider the new index
    expect(cache.has(queryHash)).toBe(false);
  });

  it('should invalidate plan when CREATE UNIQUE INDEX is executed', () => {
    const sql = 'SELECT * FROM users WHERE id = ?';
    const queryHash = cacheKey(sql);

    cache.set(queryHash, mockPlan('users'), 1);
    expect(cache.has(queryHash)).toBe(true);

    processDDL(cache, 'CREATE UNIQUE INDEX idx_users_id ON users(id)');

    expect(cache.has(queryHash)).toBe(false);
  });

  it('should invalidate plan when composite CREATE INDEX is executed', () => {
    const sql = 'SELECT * FROM orders WHERE user_id = ? AND status = ?';
    const queryHash = cacheKey(sql);

    cache.set(queryHash, mockPlan('orders'), 1);
    expect(cache.has(queryHash)).toBe(true);

    processDDL(cache, 'CREATE INDEX idx_orders_user_status ON orders(user_id, status)');

    expect(cache.has(queryHash)).toBe(false);
  });

  it('should use notifyIndexChange for explicit index notifications', () => {
    const sql = 'SELECT * FROM products WHERE category = ?';
    const queryHash = cacheKey(sql);

    cache.set(queryHash, { table: 'products', plan: 'SeqScan' }, 1);
    expect(cache.has(queryHash)).toBe(true);

    notifyIndexChange(cache, {
      type: 'CREATE_INDEX',
      table: 'products',
      columns: ['category'],
    });

    expect(cache.has(queryHash)).toBe(false);
  });

  it('should not invalidate plans for unrelated tables on CREATE INDEX', () => {
    const usersQuery = 'SELECT * FROM users WHERE email = ?';
    const ordersQuery = 'SELECT * FROM orders WHERE user_id = ?';
    const usersHash = cacheKey(usersQuery);
    const ordersHash = cacheKey(ordersQuery);

    cache.set(usersHash, mockPlan('users'), 1);
    cache.set(ordersHash, mockPlan('orders'), 1);

    // Create index on users - should not affect orders
    processDDL(cache, 'CREATE INDEX idx_users_email ON users(email)');

    expect(cache.has(usersHash)).toBe(false);
    expect(cache.has(ordersHash)).toBe(true);
  });
});

// =============================================================================
// DROP TABLE INVALIDATION
// =============================================================================

describe('Cache Invalidation on DROP TABLE', () => {
  let cache: QueryPlanCacheImpl;

  beforeEach(() => {
    cache = createQueryPlanCache({
      maxSize: 100,
      trackTableDependencies: true,
    });
  });

  it('should invalidate plan when DROP TABLE is executed', () => {
    const sql = 'SELECT * FROM temp_data';
    const queryHash = cacheKey(sql);

    cache.set(queryHash, mockPlan('temp_data'), 1);
    expect(cache.has(queryHash)).toBe(true);

    processDDL(cache, 'DROP TABLE temp_data');

    expect(cache.has(queryHash)).toBe(false);
  });

  it('should invalidate plan when DROP TABLE IF EXISTS is executed', () => {
    const sql = 'SELECT * FROM staging';
    const queryHash = cacheKey(sql);

    cache.set(queryHash, mockPlan('staging'), 1);
    expect(cache.has(queryHash)).toBe(true);

    processDDL(cache, 'DROP TABLE IF EXISTS staging');

    expect(cache.has(queryHash)).toBe(false);
  });

  it('should not invalidate plans for other tables on DROP TABLE', () => {
    const tempQuery = 'SELECT * FROM temp_data';
    const usersQuery = 'SELECT * FROM users';
    const tempHash = cacheKey(tempQuery);
    const usersHash = cacheKey(usersQuery);

    cache.set(tempHash, mockPlan('temp_data'), 1);
    cache.set(usersHash, mockPlan('users'), 1);

    processDDL(cache, 'DROP TABLE temp_data');

    expect(cache.has(tempHash)).toBe(false);
    expect(cache.has(usersHash)).toBe(true);
  });
});

// =============================================================================
// DROP INDEX INVALIDATION
// =============================================================================

describe('Cache Invalidation on DROP INDEX', () => {
  let cache: QueryPlanCacheImpl;

  beforeEach(() => {
    cache = createQueryPlanCache({
      maxSize: 100,
      trackTableDependencies: true,
    });
  });

  it('should invalidate plan when DROP INDEX is executed', () => {
    const sql = 'SELECT * FROM users WHERE email = ?';
    const queryHash = cacheKey(sql);

    // Cache a plan that might be using the index
    cache.set(queryHash, { table: 'users', plan: 'IndexScan on idx_users_email' }, 1);
    expect(cache.has(queryHash)).toBe(true);

    // Drop the index - plan needs to be re-optimized
    processDDL(cache, 'DROP INDEX idx_users_email ON users');

    expect(cache.has(queryHash)).toBe(false);
  });

  it('should use notifyIndexChange for DROP_INDEX notifications', () => {
    const sql = 'SELECT * FROM products WHERE sku = ?';
    const queryHash = cacheKey(sql);

    cache.set(queryHash, { table: 'products', plan: 'IndexScan' }, 1);
    expect(cache.has(queryHash)).toBe(true);

    notifyIndexChange(cache, {
      type: 'DROP_INDEX',
      table: 'products',
      columns: ['sku'],
    });

    expect(cache.has(queryHash)).toBe(false);
  });
});

// =============================================================================
// MULTI-TABLE QUERY INVALIDATION
// =============================================================================

describe('Cache Invalidation for Multi-Table Queries', () => {
  let cache: QueryPlanCacheImpl;

  beforeEach(() => {
    cache = createQueryPlanCache({
      maxSize: 100,
      trackTableDependencies: true,
    });
  });

  it('should invalidate join plan when any referenced table is altered', () => {
    const sql = 'SELECT * FROM users JOIN orders ON users.id = orders.user_id';
    const queryHash = cacheKey(sql);

    cache.set(queryHash, mockJoinPlan(['users', 'orders']), 1);
    expect(cache.has(queryHash)).toBe(true);

    // Altering orders should invalidate the join plan
    processDDL(cache, 'ALTER TABLE orders ADD COLUMN discount REAL');

    expect(cache.has(queryHash)).toBe(false);
  });

  it('should invalidate join plan when either table gets new index', () => {
    const sql = 'SELECT * FROM users JOIN orders ON users.id = orders.user_id';
    const queryHash = cacheKey(sql);

    cache.set(queryHash, mockJoinPlan(['users', 'orders']), 1);
    expect(cache.has(queryHash)).toBe(true);

    // Creating index on users could change join strategy
    processDDL(cache, 'CREATE INDEX idx_users_id ON users(id)');

    expect(cache.has(queryHash)).toBe(false);
  });

  it('should not invalidate join plan when unrelated table is altered', () => {
    const joinQuery = 'SELECT * FROM users JOIN orders ON users.id = orders.user_id';
    const joinHash = cacheKey(joinQuery);

    cache.set(joinHash, mockJoinPlan(['users', 'orders']), 1);
    expect(cache.has(joinHash)).toBe(true);

    // Altering products should not affect users-orders join
    processDDL(cache, 'ALTER TABLE products ADD COLUMN weight REAL');

    expect(cache.has(joinHash)).toBe(true);
  });
});

// =============================================================================
// CACHE STATISTICS
// =============================================================================

describe('Cache Statistics for Invalidation', () => {
  let cache: QueryPlanCacheImpl;

  beforeEach(() => {
    cache = createQueryPlanCache({
      maxSize: 100,
      trackTableDependencies: true,
    });
  });

  it('should track number of invalidations in stats', () => {
    // Cache multiple plans
    cache.set(cacheKey('SELECT * FROM users'), mockPlan('users'), 1);
    cache.set(cacheKey('SELECT name FROM users'), mockPlan('users'), 1);
    cache.set(cacheKey('SELECT * FROM orders'), mockPlan('orders'), 1);

    const statsBefore = cache.getStats();
    expect(statsBefore.size).toBe(3);

    // Invalidate users table (2 plans)
    const invalidated = cache.invalidateTable('users');

    expect(invalidated).toBe(2);
    expect(cache.size()).toBe(1);
  });

  it('should return 0 when invalidating table with no cached plans', () => {
    cache.set(cacheKey('SELECT * FROM users'), mockPlan('users'), 1);

    const invalidated = cache.invalidateTable('nonexistent_table');

    expect(invalidated).toBe(0);
    expect(cache.size()).toBe(1);
  });
});

// =============================================================================
// INTEGRATION WITH TABLE DEPENDENCY TRACKING
// =============================================================================

describe('Table Dependency Tracking Integration', () => {
  let cache: QueryPlanCacheImpl;

  beforeEach(() => {
    cache = createQueryPlanCache({
      maxSize: 100,
      trackTableDependencies: true,
    });
  });

  it('should extract tables from plan and track dependencies', () => {
    const plan = {
      table: 'users',
      children: [
        { table: 'orders' },
        { table: 'products' },
      ],
    };

    const queryHash = cacheKey('SELECT * FROM users, orders, products');
    cache.set(queryHash, plan, 1);

    // Invalidating any of the tables should remove the plan
    expect(cache.has(queryHash)).toBe(true);

    cache.invalidateTable('products');
    expect(cache.has(queryHash)).toBe(false);
  });

  it('should handle plans with tables array property', () => {
    const plan = {
      tables: ['users', 'orders'],
      type: 'HashJoin',
    };

    const queryHash = cacheKey('SELECT * FROM users JOIN orders');
    cache.set(queryHash, plan, 1);

    expect(cache.has(queryHash)).toBe(true);

    cache.invalidateTable('orders');
    expect(cache.has(queryHash)).toBe(false);
  });
});

// =============================================================================
// EDGE CASES
// =============================================================================

describe('Edge Cases for Cache Invalidation', () => {
  let cache: QueryPlanCacheImpl;

  beforeEach(() => {
    cache = createQueryPlanCache({
      maxSize: 100,
      trackTableDependencies: true,
    });
  });

  it('should handle case-insensitive table names in DDL', () => {
    const sql = 'SELECT * FROM Users';
    const queryHash = cacheKey(sql);

    cache.set(queryHash, mockPlan('Users'), 1);
    expect(cache.has(queryHash)).toBe(true);

    // DDL with different case
    processDDL(cache, 'ALTER TABLE users ADD COLUMN status TEXT');

    expect(cache.has(queryHash)).toBe(false);
  });

  it('should handle CREATE INDEX with IF NOT EXISTS', () => {
    const sql = 'SELECT * FROM users WHERE email = ?';
    const queryHash = cacheKey(sql);

    cache.set(queryHash, mockPlan('users'), 1);
    expect(cache.has(queryHash)).toBe(true);

    processDDL(cache, 'CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)');

    expect(cache.has(queryHash)).toBe(false);
  });

  it('should handle DROP TABLE with CASCADE', () => {
    const sql = 'SELECT * FROM temp_data';
    const queryHash = cacheKey(sql);

    cache.set(queryHash, mockPlan('temp_data'), 1);
    expect(cache.has(queryHash)).toBe(true);

    processDDL(cache, 'DROP TABLE temp_data CASCADE');

    expect(cache.has(queryHash)).toBe(false);
  });

  it('should ignore unrecognized DDL statements', () => {
    const sql = 'SELECT * FROM users';
    const queryHash = cacheKey(sql);

    cache.set(queryHash, mockPlan('users'), 1);
    expect(cache.has(queryHash)).toBe(true);

    // This should not crash or invalidate anything
    processDDL(cache, 'CREATE EXTENSION pgcrypto');

    expect(cache.has(queryHash)).toBe(true);
  });

  it('should handle empty cache gracefully', () => {
    expect(() => cache.invalidateTable('users')).not.toThrow();
    expect(() => processDDL(cache, 'ALTER TABLE users ADD COLUMN x TEXT')).not.toThrow();
    expect(cache.size()).toBe(0);
  });
});
