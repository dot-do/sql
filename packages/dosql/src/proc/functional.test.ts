/**
 * Functional Stored Procedure Tests
 *
 * Tests for the functional procedure API including:
 * - Type inference tests (compile-time)
 * - Runtime tests for procedure execution
 * - Context builder tests
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  defineProcedures,
  defineProcedure,
  defineProceduresWithMiddleware,
  withValidation,
  withRetry,
  type FunctionalContext,
  type FunctionalDb,
  type Proc,
  type NoParamsProc,
  type InferProcedureCall,
  type ProcedureDef,
} from './functional.js';
import {
  createFunctionalDb,
  createInMemoryFunctionalDb,
  functionalDb,
  sql,
  isSqlExpression,
} from './context-builder.js';
import {
  createInMemoryAdapter,
  createInMemorySqlExecutor,
  createInMemoryTransactionManager,
} from './context.js';
import type { DatabaseSchema } from './types.js';

// =============================================================================
// TEST DATABASE SCHEMA
// =============================================================================

interface TestDB extends DatabaseSchema {
  users: {
    id: 'number';
    name: 'string';
    email: 'string';
    active: 'boolean';
  };
  orders: {
    id: 'number';
    userId: 'number';
    totalAmount: 'number';
    status: 'string';
  };
  accounts: {
    id: 'string';
    balance: 'number';
    ownerId: 'number';
  };
}

// Type helper for assertions
type Expect<T extends true> = T;
type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2) ? true : false;

// =============================================================================
// TYPE-LEVEL TESTS
// =============================================================================

describe('Functional Procedure Types', () => {
  // These tests verify compile-time type inference

  it('should infer return types correctly', () => {
    // Setup
    type TestProc = ProcedureDef<[string], number, TestDB>;

    // Verify the callable type strips context and wraps in Promise
    type CallableType = InferProcedureCall<TestProc, TestDB>;

    // CallableType should be: (arg: string) => Promise<number>
    type _TestCallable = Expect<Equal<
      CallableType,
      (arg: string) => Promise<number>
    >>;

    // Runtime check to satisfy test runner
    expect(true).toBe(true);
  });

  it('should infer multiple params correctly', () => {
    type TwoParamProc = ProcedureDef<[string, number], boolean, TestDB>;
    type CallableType = InferProcedureCall<TwoParamProc, TestDB>;

    type _TestTwoParams = Expect<Equal<
      CallableType,
      (arg1: string, arg2: number) => Promise<boolean>
    >>;

    expect(true).toBe(true);
  });

  it('should infer no-params procedures correctly', () => {
    type NoParamProc = NoParamsProc<string[], TestDB>;
    type CallableType = InferProcedureCall<NoParamProc, TestDB>;

    type _TestNoParams = Expect<Equal<
      CallableType,
      () => Promise<string[]>
    >>;

    expect(true).toBe(true);
  });

  it('should preserve complex return types', () => {
    type ComplexReturn = { success: boolean; data: { id: number; items: string[] } };
    type ComplexProc = ProcedureDef<[number], ComplexReturn, TestDB>;
    type CallableType = InferProcedureCall<ComplexProc, TestDB>;

    type _TestComplex = Expect<Equal<
      CallableType,
      (arg: number) => Promise<ComplexReturn>
    >>;

    expect(true).toBe(true);
  });
});

// =============================================================================
// CONTEXT BUILDER TESTS
// =============================================================================

describe('Context Builder', () => {
  describe('createInMemoryFunctionalDb', () => {
    it('should create db with typed table accessors', async () => {
      const db = createInMemoryFunctionalDb<TestDB>({
        users: [
          { id: 1, name: 'Alice', email: 'alice@example.com', active: true },
          { id: 2, name: 'Bob', email: 'bob@example.com', active: false },
        ],
        orders: [
          { id: 1, userId: 1, totalAmount: 100, status: 'completed' },
          { id: 2, userId: 1, totalAmount: 200, status: 'pending' },
        ],
        accounts: [],
      });

      // Test table accessor methods
      const allUsers = await db.users.all();
      expect(allUsers).toHaveLength(2);
      expect(allUsers[0].name).toBe('Alice');

      const alice = await db.users.get(1);
      expect(alice?.email).toBe('alice@example.com');

      const activeUsers = await db.users.where({ active: true });
      expect(activeUsers).toHaveLength(1);
      expect(activeUsers[0].name).toBe('Alice');
    });

    it('should support CRUD operations', async () => {
      const db = createInMemoryFunctionalDb<TestDB>({
        users: [],
        orders: [],
        accounts: [],
      });

      // Insert
      const newUser = await db.users.insert({
        name: 'Charlie',
        email: 'charlie@example.com',
        active: true,
      });
      expect(newUser.id).toBeDefined();
      expect(newUser.name).toBe('Charlie');

      // Update
      const updated = await db.users.update({ id: newUser.id }, { active: false });
      expect(updated).toBe(1);

      // Verify update
      const charlie = await db.users.get(newUser.id);
      expect(charlie?.active).toBe(false);

      // Delete
      const deleted = await db.users.delete({ id: newUser.id });
      expect(deleted).toBe(1);

      // Verify delete
      const count = await db.users.count();
      expect(count).toBe(0);
    });

    it('should have sql function', async () => {
      const db = createInMemoryFunctionalDb<TestDB>({
        users: [{ id: 1, name: 'Test', email: 'test@test.com', active: true }],
        orders: [],
        accounts: [],
      });

      expect(db.sql).toBeDefined();
      expect(typeof db.sql).toBe('function');
    });

    it('should have transaction function', async () => {
      const db = createInMemoryFunctionalDb<TestDB>({
        users: [],
        orders: [],
        accounts: [],
      });

      expect(db.transaction).toBeDefined();
      expect(typeof db.transaction).toBe('function');
    });
  });

  describe('functionalDb builder', () => {
    it('should build db with fluent API', () => {
      const userAdapter = createInMemoryAdapter([
        { id: 1, name: 'Test', email: 'test@test.com', active: true },
      ]);
      const orderAdapter = createInMemoryAdapter<{ id: number; userId: number; totalAmount: number; status: string }>([]);
      const accountAdapter = createInMemoryAdapter<{ id: string; balance: number; ownerId: number }>([]);

      const tableMap = new Map<string, ReturnType<typeof createInMemoryAdapter>>();
      tableMap.set('users', userAdapter);
      tableMap.set('orders', orderAdapter);
      tableMap.set('accounts', accountAdapter);

      const db = functionalDb<TestDB>()
        .table('users', userAdapter as any)
        .table('orders', orderAdapter as any)
        .table('accounts', accountAdapter as any)
        .sql(createInMemorySqlExecutor(tableMap as any))
        .transactions(createInMemoryTransactionManager())
        .build();

      expect(db.users).toBeDefined();
      expect(db.orders).toBeDefined();
      expect(db.sql).toBeDefined();
      expect(db.transaction).toBeDefined();
    });

    it('should throw if required components missing', () => {
      expect(() => functionalDb<TestDB>().build()).toThrow('SQL executor');

      const builder = functionalDb<TestDB>()
        .sql(createInMemorySqlExecutor(new Map()));

      expect(() => builder.build()).toThrow('Transaction manager');
    });
  });

  describe('sql template helper', () => {
    it('should create SQL expression object', () => {
      const expr = sql`balance - ${100}`;

      expect(expr.__sql).toBe(true);
      expect(expr.query).toBe('balance - $1');
      expect(expr.params).toEqual([100]);
    });

    it('should handle multiple parameters', () => {
      const expr = sql`balance >= ${50} AND balance <= ${1000}`;

      expect(expr.query).toBe('balance >= $1 AND balance <= $2');
      expect(expr.params).toEqual([50, 1000]);
    });

    it('should be detected by isSqlExpression', () => {
      const expr = sql`test`;
      const notExpr = { query: 'test', params: [] };

      expect(isSqlExpression(expr)).toBe(true);
      expect(isSqlExpression(notExpr)).toBe(false);
      expect(isSqlExpression('string')).toBe(false);
      expect(isSqlExpression(null)).toBe(false);
    });
  });
});

// =============================================================================
// DEFINE PROCEDURES TESTS
// =============================================================================

describe('defineProcedures', () => {
  let db: FunctionalDb<TestDB>;

  beforeEach(() => {
    db = createInMemoryFunctionalDb<TestDB>({
      users: [
        { id: 1, name: 'Alice', email: 'alice@example.com', active: true },
        { id: 2, name: 'Bob', email: 'bob@example.com', active: true },
        { id: 3, name: 'Charlie', email: 'charlie@example.com', active: false },
      ],
      orders: [
        { id: 1, userId: 1, totalAmount: 100, status: 'completed' },
        { id: 2, userId: 1, totalAmount: 200, status: 'completed' },
        { id: 3, userId: 2, totalAmount: 150, status: 'pending' },
      ],
      accounts: [
        { id: 'acc-1', balance: 1000, ownerId: 1 },
        { id: 'acc-2', balance: 500, ownerId: 2 },
      ],
    });
  });

  it('should define and call procedure with single param', async () => {
    const procedures = defineProcedures(
      {
        getUser: async (id: number, { db }) => {
          return db.users.get(id);
        },
      },
      { db }
    );

    const user = await procedures.getUser(1);

    expect(user).toBeDefined();
    expect(user?.name).toBe('Alice');
    expect(user?.email).toBe('alice@example.com');
  });

  it('should define and call procedure with multiple params', async () => {
    const procedures = defineProcedures(
      {
        calculateTotalForUser: async (userId: number, includeStatus: string, { db }) => {
          const orders = await db.orders.where({ userId, status: includeStatus });
          return orders.reduce((sum, o) => sum + o.totalAmount, 0);
        },
      },
      { db }
    );

    const total = await procedures.calculateTotalForUser(1, 'completed');

    expect(total).toBe(300); // 100 + 200
  });

  it('should define and call procedure with no params', async () => {
    const procedures = defineProcedures(
      {
        getStats: async ({ db }) => {
          const userCount = await db.users.count();
          const orderCount = await db.orders.count();
          return { userCount, orderCount };
        },
      },
      { db }
    );

    const stats = await procedures.getStats();

    expect(stats.userCount).toBe(3);
    expect(stats.orderCount).toBe(3);
  });

  it('should preserve return type inference', async () => {
    const procedures = defineProcedures(
      {
        getComplexResult: async (id: number, { db }) => {
          const user = await db.users.get(id);
          const orders = await db.orders.where({ userId: id });
          return {
            user,
            orders,
            totalOrders: orders.length,
            metadata: {
              processedAt: new Date().toISOString(),
              version: 1,
            },
          };
        },
      },
      { db }
    );

    const result = await procedures.getComplexResult(1);

    expect(result.user?.name).toBe('Alice');
    expect(result.orders).toHaveLength(2);
    expect(result.totalOrders).toBe(2);
    expect(result.metadata.version).toBe(1);
  });

  it('should define multiple procedures at once', async () => {
    const procedures = defineProcedures(
      {
        getUser: async (id: number, { db }) => db.users.get(id),
        countUsers: async ({ db }) => db.users.count(),
        getActiveUsers: async ({ db }) => db.users.where({ active: true }),
        getUserOrders: async (userId: number, { db }) => db.orders.where({ userId }),
      },
      { db }
    );

    const user = await procedures.getUser(1);
    const count = await procedures.countUsers();
    const activeUsers = await procedures.getActiveUsers();
    const orders = await procedures.getUserOrders(1);

    expect(user?.name).toBe('Alice');
    expect(count).toBe(3);
    expect(activeUsers).toHaveLength(2);
    expect(orders).toHaveLength(2);
  });

  it('should support sync procedures', async () => {
    const procedures = defineProcedures(
      {
        // Sync function (still called with await, returns Promise)
        addNumbers: (a: number, b: number, { db }) => a + b,
      },
      { db }
    );

    const result = await procedures.addNumbers(5, 3);

    expect(result).toBe(8);
  });

  it('should pass environment to context', async () => {
    const procedures = defineProcedures(
      {
        getEnvValue: async (key: string, { env }) => {
          return env?.[key] ?? 'not found';
        },
      },
      {
        db,
        env: { API_KEY: 'secret-123', DEBUG: 'true' },
      }
    );

    const apiKey = await procedures.getEnvValue('API_KEY');
    const missing = await procedures.getEnvValue('MISSING');

    expect(apiKey).toBe('secret-123');
    expect(missing).toBe('not found');
  });
});

// =============================================================================
// SINGLE PROCEDURE DEFINITION TESTS
// =============================================================================

describe('defineProcedure', () => {
  let db: FunctionalDb<TestDB>;

  beforeEach(() => {
    db = createInMemoryFunctionalDb<TestDB>({
      users: [{ id: 1, name: 'Alice', email: 'alice@example.com', active: true }],
      orders: [],
      accounts: [],
    });
  });

  it('should define a single procedure', async () => {
    const getUser = defineProcedure<TestDB>()(
      'getUser',
      async (id: number, { db }) => db.users.get(id),
      { db }
    );

    const user = await getUser(1);

    expect(user?.name).toBe('Alice');
  });
});

// =============================================================================
// MIDDLEWARE TESTS
// =============================================================================

describe('defineProceduresWithMiddleware', () => {
  let db: FunctionalDb<TestDB>;
  let logs: string[];

  beforeEach(() => {
    db = createInMemoryFunctionalDb<TestDB>({
      users: [{ id: 1, name: 'Alice', email: 'alice@example.com', active: true }],
      orders: [],
      accounts: [],
    });
    logs = [];
  });

  it('should apply middleware in order', async () => {
    const procedures = defineProceduresWithMiddleware(
      {
        getUser: async (id: number, { db }) => {
          logs.push('handler');
          return db.users.get(id);
        },
      },
      {
        db,
        middleware: [
          async (ctx, next) => {
            logs.push('middleware1-before');
            const result = await next();
            logs.push('middleware1-after');
            return result;
          },
          async (ctx, next) => {
            logs.push('middleware2-before');
            const result = await next();
            logs.push('middleware2-after');
            return result;
          },
        ],
      }
    );

    await procedures.getUser(1);

    expect(logs).toEqual([
      'middleware1-before',
      'middleware2-before',
      'handler',
      'middleware2-after',
      'middleware1-after',
    ]);
  });

  it('should work without middleware', async () => {
    const procedures = defineProceduresWithMiddleware(
      {
        getUser: async (id: number, { db }) => db.users.get(id),
      },
      { db }
    );

    const user = await procedures.getUser(1);

    expect(user?.name).toBe('Alice');
  });

  it('should support timing middleware', async () => {
    let duration = 0;

    const procedures = defineProceduresWithMiddleware(
      {
        slowOp: async (delay: number, { db }) => {
          await new Promise(resolve => setTimeout(resolve, delay));
          return 'done';
        },
      },
      {
        db,
        middleware: [
          async (ctx, next) => {
            const start = Date.now();
            const result = await next();
            duration = Date.now() - start;
            return result;
          },
        ],
      }
    );

    await procedures.slowOp(50);

    expect(duration).toBeGreaterThanOrEqual(45); // Allow some variance
  });
});

// =============================================================================
// VALIDATION TESTS
// =============================================================================

describe('withValidation', () => {
  let db: FunctionalDb<TestDB>;

  beforeEach(() => {
    db = createInMemoryFunctionalDb<TestDB>({
      users: [{ id: 1, name: 'Alice', email: 'alice@example.com', active: true }],
      orders: [],
      accounts: [],
    });
  });

  it('should validate arguments', async () => {
    const validatePositive = (v: unknown): v is number =>
      typeof v === 'number' && v > 0;

    const validated = withValidation(
      async (id: number, { db }) => db.users.get(id),
      [validatePositive]
    );

    const ctx: FunctionalContext<TestDB> = { db };

    // Valid call
    const user = await validated(1, ctx);
    expect(user?.name).toBe('Alice');

    // Invalid call
    await expect(validated(-1, ctx)).rejects.toThrow('Validation failed');
  });

  it('should validate multiple arguments', async () => {
    const isString = (v: unknown): v is string => typeof v === 'string';
    const isPositive = (v: unknown): v is number => typeof v === 'number' && v > 0;

    const validated = withValidation(
      async (name: string, age: number, { db }) => ({ name, age }),
      [isString, isPositive]
    );

    const ctx: FunctionalContext<TestDB> = { db };

    // Valid
    const result = await validated('Alice', 25, ctx);
    expect(result).toEqual({ name: 'Alice', age: 25 });

    // Invalid first arg
    await expect(validated(123 as any, 25, ctx)).rejects.toThrow('index 0');

    // Invalid second arg
    await expect(validated('Bob', -5, ctx)).rejects.toThrow('index 1');
  });
});

// =============================================================================
// RETRY TESTS
// =============================================================================

describe('withRetry', () => {
  let db: FunctionalDb<TestDB>;

  beforeEach(() => {
    db = createInMemoryFunctionalDb<TestDB>({
      users: [],
      orders: [],
      accounts: [],
    });
  });

  it('should retry on failure', async () => {
    let attempts = 0;

    const flaky = withRetry(
      async (threshold: number, { db }) => {
        attempts++;
        if (attempts < threshold) {
          throw new Error('Transient error');
        }
        return 'success';
      },
      { maxAttempts: 5, delayMs: 10 }
    );

    const ctx: FunctionalContext<TestDB> = { db };
    const result = await flaky(3, ctx);

    expect(result).toBe('success');
    expect(attempts).toBe(3);
  });

  it('should give up after max attempts', async () => {
    let attempts = 0;

    const alwaysFails = withRetry(
      async (arg: string, { db }) => {
        attempts++;
        throw new Error('Permanent error');
      },
      { maxAttempts: 3, delayMs: 10 }
    );

    const ctx: FunctionalContext<TestDB> = { db };

    await expect(alwaysFails('test', ctx)).rejects.toThrow('Permanent error');
    expect(attempts).toBe(3);
  });

  it('should respect isRetryable predicate', async () => {
    let attempts = 0;

    const selectiveRetry = withRetry(
      async (shouldFail: boolean, { db }) => {
        attempts++;
        if (shouldFail) {
          throw new Error('NON_RETRYABLE');
        }
        return 'success';
      },
      {
        maxAttempts: 3,
        delayMs: 10,
        isRetryable: (error) =>
          error instanceof Error && !error.message.includes('NON_RETRYABLE'),
      }
    );

    const ctx: FunctionalContext<TestDB> = { db };

    await expect(selectiveRetry(true, ctx)).rejects.toThrow('NON_RETRYABLE');
    expect(attempts).toBe(1); // Should not retry
  });
});

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

describe('Integration Tests', () => {
  it('should handle complex procedure workflow', async () => {
    const db = createInMemoryFunctionalDb<TestDB>({
      users: [
        { id: 1, name: 'Alice', email: 'alice@example.com', active: true },
        { id: 2, name: 'Bob', email: 'bob@example.com', active: true },
      ],
      orders: [
        { id: 1, userId: 1, totalAmount: 100, status: 'completed' },
        { id: 2, userId: 1, totalAmount: 200, status: 'completed' },
        { id: 3, userId: 2, totalAmount: 50, status: 'pending' },
      ],
      accounts: [
        { id: 'acc-1', balance: 1000, ownerId: 1 },
        { id: 'acc-2', balance: 500, ownerId: 2 },
      ],
    });

    const procedures = defineProcedures(
      {
        // Get user with their orders summary
        getUserSummary: async (userId: number, { db }) => {
          const user = await db.users.get(userId);
          if (!user) return null;

          const orders = await db.orders.where({ userId });
          const totalSpent = orders.reduce((sum, o) => sum + o.totalAmount, 0);
          const completedOrders = orders.filter(o => o.status === 'completed').length;

          return {
            user,
            orderCount: orders.length,
            completedOrders,
            totalSpent,
          };
        },

        // Create an order for a user
        createOrder: async (userId: number, amount: number, { db }) => {
          const user = await db.users.get(userId);
          if (!user) {
            throw new Error('User not found');
          }

          return db.orders.insert({
            userId,
            totalAmount: amount,
            status: 'pending',
          });
        },

        // Get top spenders
        getTopSpenders: async (limit: number, { db }) => {
          const users = await db.users.all();
          const orders = await db.orders.all();

          const userTotals = users.map(user => {
            const userOrders = orders.filter(o => o.userId === user.id);
            const total = userOrders.reduce((sum, o) => sum + o.totalAmount, 0);
            return { user, total };
          });

          return userTotals
            .sort((a, b) => b.total - a.total)
            .slice(0, limit);
        },
      },
      { db }
    );

    // Test getUserSummary
    const aliceSummary = await procedures.getUserSummary(1);
    expect(aliceSummary?.user.name).toBe('Alice');
    expect(aliceSummary?.orderCount).toBe(2);
    expect(aliceSummary?.completedOrders).toBe(2);
    expect(aliceSummary?.totalSpent).toBe(300);

    const bobSummary = await procedures.getUserSummary(2);
    expect(bobSummary?.totalSpent).toBe(50);

    const nonExistent = await procedures.getUserSummary(999);
    expect(nonExistent).toBeNull();

    // Test createOrder
    const newOrder = await procedures.createOrder(1, 75);
    expect(newOrder.totalAmount).toBe(75);
    expect(newOrder.status).toBe('pending');

    // Test getTopSpenders
    const topSpenders = await procedures.getTopSpenders(2);
    expect(topSpenders).toHaveLength(2);
    expect(topSpenders[0].user.name).toBe('Alice');
    expect(topSpenders[0].total).toBe(375); // 300 + 75 from new order
  });

  it('should work with type assertions in IDE', async () => {
    const db = createInMemoryFunctionalDb<TestDB>({
      users: [{ id: 1, name: 'Test', email: 'test@test.com', active: true }],
      orders: [],
      accounts: [],
    });

    // This test mainly verifies that types work correctly
    // If this compiles, types are working
    const procedures = defineProcedures(
      {
        // Return type should be inferred as Promise<{ id: number; name: string; ... } | undefined>
        getUser: async (id: number, { db }) => db.users.get(id),

        // Return type should be inferred as Promise<number>
        countOrders: async (userId: number, { db }) => {
          const orders = await db.orders.where({ userId });
          return orders.length;
        },

        // Return type should be inferred as Promise<{ name: string; count: number }[]>
        aggregate: async ({ db }) => {
          const users = await db.users.all();
          return users.map(u => ({ name: u.name, count: 1 }));
        },
      },
      { db }
    );

    // Type narrowing should work
    const user = await procedures.getUser(1);
    if (user) {
      // user should be typed with name, email, etc.
      expect(typeof user.name).toBe('string');
      expect(typeof user.email).toBe('string');
    }

    // Return type should be number
    const count: number = await procedures.countOrders(1);
    expect(typeof count).toBe('number');

    // Return type should be array with name and count
    const agg = await procedures.aggregate();
    expect(Array.isArray(agg)).toBe(true);
    if (agg.length > 0) {
      expect(typeof agg[0].name).toBe('string');
      expect(typeof agg[0].count).toBe('number');
    }
  });
});

// =============================================================================
// COMPILE-TIME TYPE TESTS
// =============================================================================

// These are compile-time only tests - if the file compiles, they pass

describe('Compile-time Type Tests', () => {
  it('should enforce correct param types', () => {
    // This test verifies that TypeScript correctly enforces types
    // The actual assertions are compile-time - if this compiles, it passes

    type TestProc = Proc<[string, number], boolean, TestDB>;

    // This should be the correct callable signature
    type Expected = (arg1: string, arg2: number) => Promise<boolean>;
    type Actual = InferProcedureCall<TestProc, TestDB>;

    // Type-level assertion
    type _Test = Expect<Equal<Actual, Expected>>;

    expect(true).toBe(true);
  });

  it('should infer context type correctly', () => {
    // FunctionalContext should have db with typed tables
    type Ctx = FunctionalContext<TestDB>;

    // db.users should be a TableAccessor
    type UsersAccessor = Ctx['db']['users'];

    // The accessor should have get, where, all, etc.
    type _HasGet = UsersAccessor['get'];
    type _HasWhere = UsersAccessor['where'];
    type _HasAll = UsersAccessor['all'];

    expect(true).toBe(true);
  });
});

// =============================================================================
// EDGE CASES
// =============================================================================

describe('Edge Cases', () => {
  let db: FunctionalDb<TestDB>;

  beforeEach(() => {
    db = createInMemoryFunctionalDb<TestDB>({
      users: [],
      orders: [],
      accounts: [],
    });
  });

  it('should handle empty tables', async () => {
    const procedures = defineProcedures(
      {
        getAll: async ({ db }) => db.users.all(),
        count: async ({ db }) => db.users.count(),
      },
      { db }
    );

    const all = await procedures.getAll();
    const count = await procedures.count();

    expect(all).toEqual([]);
    expect(count).toBe(0);
  });

  it('should handle procedure that throws', async () => {
    const procedures = defineProcedures(
      {
        willThrow: async (msg: string, { db }) => {
          throw new Error(msg);
        },
      },
      { db }
    );

    await expect(procedures.willThrow('test error')).rejects.toThrow('test error');
  });

  it('should handle optional params via union types', async () => {
    const procedures = defineProcedures(
      {
        searchUsers: async (query: string | undefined, { db }) => {
          if (!query) {
            return db.users.all();
          }
          return db.users.where(u => u.name.includes(query));
        },
      },
      { db }
    );

    // Both calls should work
    await procedures.searchUsers('alice');
    await procedures.searchUsers(undefined);
  });

  it('should handle procedures returning primitives', async () => {
    const procedures = defineProcedures(
      {
        getString: async ({ db }) => 'hello',
        getNumber: async ({ db }) => 42,
        getBoolean: async ({ db }) => true,
        getNull: async ({ db }) => null,
      },
      { db }
    );

    expect(await procedures.getString()).toBe('hello');
    expect(await procedures.getNumber()).toBe(42);
    expect(await procedures.getBoolean()).toBe(true);
    expect(await procedures.getNull()).toBeNull();
  });
});
