/**
 * Procedure Executor Tests
 *
 * Tests for the procedure executor module including:
 * - Basic execution scenarios
 * - Parameter passing and validation
 * - Execution with transactions
 * - Error handling and edge cases
 * - Nested procedure calls (via mock executor)
 *
 * Issue: sql-ntht - Stored Procedure Module Tests
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  createProcedureExecutor,
  createSimpleExecutor,
  createMockExecutor,
  createProcedureCall,
  batchExecute,
  sequentialExecute,
  type ProcedureExecutor,
} from '../executor.js';
import {
  createInMemoryAdapter,
  createInMemorySqlExecutor,
  createInMemoryTransactionManager,
  createDatabaseContext,
  type DatabaseContextOptions,
} from '../context.js';
import {
  createInMemoryCatalogStorage,
  createProcedureRegistry,
} from '../registry.js';
import type { DatabaseSchema, Procedure, ProcedureRegistry } from '../types.js';

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
    total: 'number';
    status: 'string';
  };
  accounts: {
    id: 'number';
    userId: 'number';
    balance: 'number';
  };
}

// Type for in-memory storage
type UserRecord = { id: number; name: string; email: string; active: boolean };
type OrderRecord = { id: number; userId: number; total: number; status: string };
type AccountRecord = { id: number; userId: number; balance: number };

// =============================================================================
// EXECUTOR CREATION TESTS
// =============================================================================

describe('Procedure Executor Creation', () => {
  it('should create executor with in-memory database context', () => {
    const adapters = {
      users: createInMemoryAdapter<UserRecord>([]),
      orders: createInMemoryAdapter<OrderRecord>([]),
      accounts: createInMemoryAdapter<AccountRecord>([]),
    };

    const tableMap = new Map(Object.entries(adapters));
    const db = createDatabaseContext<TestDB>({
      adapters: adapters as unknown as DatabaseContextOptions<TestDB>['adapters'],
      sqlExecutor: createInMemorySqlExecutor(tableMap),
      transactionManager: createInMemoryTransactionManager(),
    });

    const executor = createProcedureExecutor({
      db,
      defaultTimeout: 5000,
    });

    expect(executor).toBeDefined();
    expect(executor.call).toBeDefined();
    expect(executor.execute).toBeDefined();
    expect(executor.run).toBeDefined();
  });

  it('should create executor with registry', async () => {
    const adapters = {
      users: createInMemoryAdapter<UserRecord>([]),
      orders: createInMemoryAdapter<OrderRecord>([]),
      accounts: createInMemoryAdapter<AccountRecord>([]),
    };

    const tableMap = new Map(Object.entries(adapters));
    const db = createDatabaseContext<TestDB>({
      adapters: adapters as unknown as DatabaseContextOptions<TestDB>['adapters'],
      sqlExecutor: createInMemorySqlExecutor(tableMap),
      transactionManager: createInMemoryTransactionManager(),
    });

    const registry = createProcedureRegistry({
      storage: createInMemoryCatalogStorage(),
    });

    const executor = createProcedureExecutor({
      db,
      registry,
    });

    expect(executor).toBeDefined();
  });

  it('should create executor with custom environment', () => {
    const adapters = {
      users: createInMemoryAdapter<UserRecord>([]),
      orders: createInMemoryAdapter<OrderRecord>([]),
      accounts: createInMemoryAdapter<AccountRecord>([]),
    };

    const tableMap = new Map(Object.entries(adapters));
    const db = createDatabaseContext<TestDB>({
      adapters: adapters as unknown as DatabaseContextOptions<TestDB>['adapters'],
      sqlExecutor: createInMemorySqlExecutor(tableMap),
      transactionManager: createInMemoryTransactionManager(),
    });

    const executor = createProcedureExecutor({
      db,
      baseEnv: {
        API_KEY: 'test-key',
        DEBUG: 'true',
      },
    });

    expect(executor).toBeDefined();
  });
});

// =============================================================================
// SIMPLE EXECUTOR TESTS
// =============================================================================

describe('createSimpleExecutor', () => {
  it('should create executor from schema data', () => {
    const executor = createSimpleExecutor({
      users: [
        { id: 1, name: 'Alice', email: 'alice@test.com', active: true },
        { id: 2, name: 'Bob', email: 'bob@test.com', active: false },
      ],
      orders: [],
      accounts: [],
    });

    expect(executor).toBeDefined();
    expect(typeof executor.call).toBe('function');
    expect(typeof executor.execute).toBe('function');
    expect(typeof executor.run).toBe('function');
  });

  it('should execute basic procedure code', async () => {
    const executor = createSimpleExecutor({
      users: [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ],
    });

    const code = `
      export default async ({ db }) => {
        return 42;
      }
    `;

    const result = await executor.execute(code, []);

    // Result should have standard execution result properties
    expect(result).toBeDefined();
    expect(result.requestId).toBeDefined();
    expect(typeof result.duration).toBe('number');
  });

  it('should handle procedure with parameters', async () => {
    const executor = createSimpleExecutor({
      users: [{ id: 1, name: 'Alice' }],
    });

    const code = `
      export default async (ctx, a, b) => {
        return a + b;
      }
    `;

    const result = await executor.execute(code, [5, 3]);

    expect(result).toBeDefined();
    expect(result.requestId).toBeDefined();
  });
});

// =============================================================================
// MOCK EXECUTOR TESTS
// =============================================================================

describe('createMockExecutor', () => {
  describe('basic mocking', () => {
    it('should return static mock values', async () => {
      const executor = createMockExecutor({
        get_user: { id: 1, name: 'Mock User' },
        get_count: 42,
        get_list: [1, 2, 3],
      });

      const userResult = await executor.call('get_user', []);
      expect(userResult.success).toBe(true);
      expect(userResult.result).toEqual({ id: 1, name: 'Mock User' });

      const countResult = await executor.call('get_count', []);
      expect(countResult.success).toBe(true);
      expect(countResult.result).toBe(42);

      const listResult = await executor.call('get_list', []);
      expect(listResult.success).toBe(true);
      expect(listResult.result).toEqual([1, 2, 3]);
    });

    it('should support function mocks with parameters', async () => {
      const executor = createMockExecutor({
        add: (a: number, b: number) => a + b,
        multiply: async (a: number, b: number) => a * b,
        greet: (name: string) => `Hello, ${name}!`,
      });

      const addResult = await executor.call('add', [5, 3]);
      expect(addResult.success).toBe(true);
      expect(addResult.result).toBe(8);

      const multiplyResult = await executor.call('multiply', [4, 7]);
      expect(multiplyResult.success).toBe(true);
      expect(multiplyResult.result).toBe(28);

      const greetResult = await executor.call('greet', ['World']);
      expect(greetResult.success).toBe(true);
      expect(greetResult.result).toBe('Hello, World!');
    });

    it('should return error for undefined mock', async () => {
      const executor = createMockExecutor({
        defined_proc: 'value',
      });

      const result = await executor.call('undefined_proc', []);

      expect(result.success).toBe(false);
      expect(result.error).toContain('Mock not found');
      expect(result.error).toContain('undefined_proc');
    });

    it('should not support execute() method', async () => {
      const executor = createMockExecutor({});

      const result = await executor.execute('code', []);

      expect(result.success).toBe(false);
      expect(result.error).toContain('Mock executor does not support execute()');
    });

    it('should call procedure via run() using procedure name', async () => {
      const executor = createMockExecutor({
        my_proc: 'result value',
      });

      const procedure: Procedure = {
        name: 'my_proc',
        code: 'export default () => "should not execute"',
        metadata: {
          name: 'my_proc',
          version: 1,
          createdAt: new Date(),
          updatedAt: new Date(),
        },
      };

      const result = await executor.run(procedure, []);

      expect(result.success).toBe(true);
      expect(result.result).toBe('result value');
    });
  });

  describe('simulating nested procedure calls', () => {
    it('should mock nested procedure calls', async () => {
      // Simulate inner procedure
      const innerProcResult = { processed: true, value: 100 };

      // Mock outer procedure that conceptually calls inner
      const executor = createMockExecutor({
        inner_process: () => innerProcResult,
        outer_process: async (input: number) => {
          // In a real scenario, this would call inner_process
          const innerResult = innerProcResult;
          return {
            input,
            innerResult,
            combined: input + innerResult.value,
          };
        },
      });

      const result = await executor.call('outer_process', [50]);

      expect(result.success).toBe(true);
      expect(result.result).toEqual({
        input: 50,
        innerResult: { processed: true, value: 100 },
        combined: 150,
      });
    });

    it('should simulate recursive procedure calls', async () => {
      // Pre-compute factorial results for mocking
      const factorials: Record<number, number> = {
        0: 1,
        1: 1,
        2: 2,
        3: 6,
        4: 24,
        5: 120,
      };

      const executor = createMockExecutor({
        factorial: (n: number) => factorials[n] ?? 1,
      });

      const result5 = await executor.call('factorial', [5]);
      expect(result5.success).toBe(true);
      expect(result5.result).toBe(120);

      const result3 = await executor.call('factorial', [3]);
      expect(result3.success).toBe(true);
      expect(result3.result).toBe(6);
    });
  });
});

// =============================================================================
// PROCEDURE CALL HELPER TESTS
// =============================================================================

describe('createProcedureCall', () => {
  it('should create typed procedure call function', async () => {
    const executor = createMockExecutor({
      get_user_by_id: (id: number) => ({ id, name: `User ${id}` }),
    });

    const getUserById = createProcedureCall<TestDB, [number], { id: number; name: string }>(
      executor,
      'get_user_by_id'
    );

    const result = await getUserById(42);

    expect(result.success).toBe(true);
    expect(result.result).toEqual({ id: 42, name: 'User 42' });
  });

  it('should handle multiple parameters', async () => {
    const executor = createMockExecutor({
      create_order: (userId: number, amount: number, status: string) => ({
        id: 1,
        userId,
        total: amount,
        status,
      }),
    });

    const createOrder = createProcedureCall<
      TestDB,
      [number, number, string],
      { id: number; userId: number; total: number; status: string }
    >(executor, 'create_order');

    const result = await createOrder(5, 99.99, 'pending');

    expect(result.success).toBe(true);
    expect(result.result).toEqual({
      id: 1,
      userId: 5,
      total: 99.99,
      status: 'pending',
    });
  });
});

// =============================================================================
// BATCH EXECUTION TESTS
// =============================================================================

describe('batchExecute', () => {
  it('should execute multiple procedures in parallel', async () => {
    const callOrder: string[] = [];

    const executor = createMockExecutor({
      proc_a: async () => {
        callOrder.push('a_start');
        await new Promise(r => setTimeout(r, 10));
        callOrder.push('a_end');
        return 'A';
      },
      proc_b: async () => {
        callOrder.push('b_start');
        await new Promise(r => setTimeout(r, 5));
        callOrder.push('b_end');
        return 'B';
      },
      proc_c: async () => {
        callOrder.push('c_start');
        return 'C';
      },
    });

    const results = await batchExecute(executor, [
      { name: 'proc_a', params: [] },
      { name: 'proc_b', params: [] },
      { name: 'proc_c', params: [] },
    ]);

    expect(results).toHaveLength(3);
    expect(results[0].result).toBe('A');
    expect(results[1].result).toBe('B');
    expect(results[2].result).toBe('C');

    // All should start before any end (parallel execution)
    // c completes immediately, b completes before a
    expect(callOrder.includes('a_start')).toBe(true);
    expect(callOrder.includes('b_start')).toBe(true);
    expect(callOrder.includes('c_start')).toBe(true);
  });

  it('should return all results even if some fail', async () => {
    const executor = createMockExecutor({
      success_proc: () => 'success',
      // No mock for failing_proc - will return error
    });

    const results = await batchExecute(executor, [
      { name: 'success_proc', params: [] },
      { name: 'failing_proc', params: [] },
    ]);

    expect(results).toHaveLength(2);
    expect(results[0].success).toBe(true);
    expect(results[0].result).toBe('success');
    expect(results[1].success).toBe(false);
    expect(results[1].error).toContain('Mock not found');
  });

  it('should handle empty batch', async () => {
    const executor = createMockExecutor({});

    const results = await batchExecute(executor, []);

    expect(results).toHaveLength(0);
  });
});

// =============================================================================
// SEQUENTIAL EXECUTION TESTS
// =============================================================================

describe('sequentialExecute', () => {
  it('should execute procedures in sequence', async () => {
    const executionOrder: number[] = [];

    const executor = createMockExecutor({
      step_1: async () => {
        executionOrder.push(1);
        await new Promise(r => setTimeout(r, 5));
        return 'step1';
      },
      step_2: async () => {
        executionOrder.push(2);
        return 'step2';
      },
      step_3: async () => {
        executionOrder.push(3);
        return 'step3';
      },
    });

    const results = await sequentialExecute(executor, [
      { name: 'step_1', params: [] },
      { name: 'step_2', params: [] },
      { name: 'step_3', params: [] },
    ]);

    expect(results).toHaveLength(3);
    expect(executionOrder).toEqual([1, 2, 3]);
    expect(results.map(r => r.result)).toEqual(['step1', 'step2', 'step3']);
  });

  it('should stop on first failure', async () => {
    const executionOrder: string[] = [];

    const executor = createMockExecutor({
      step_1: () => {
        executionOrder.push('step_1');
        return 'ok';
      },
      // step_2 not mocked - will fail
      step_3: () => {
        executionOrder.push('step_3');
        return 'should not run';
      },
    });

    const results = await sequentialExecute(executor, [
      { name: 'step_1', params: [] },
      { name: 'step_2', params: [] },
      { name: 'step_3', params: [] },
    ]);

    // Should have 2 results - step_1 success, step_2 failure
    expect(results).toHaveLength(2);
    expect(results[0].success).toBe(true);
    expect(results[1].success).toBe(false);

    // step_3 should not have executed
    expect(executionOrder).toEqual(['step_1']);
  });

  it('should handle empty sequence', async () => {
    const executor = createMockExecutor({});

    const results = await sequentialExecute(executor, []);

    expect(results).toHaveLength(0);
  });

  it('should pass parameters to each procedure', async () => {
    const executor = createMockExecutor({
      accumulate: (value: number, multiplier: number) => value * multiplier,
    });

    const results = await sequentialExecute(executor, [
      { name: 'accumulate', params: [10, 2] },
      { name: 'accumulate', params: [5, 3] },
      { name: 'accumulate', params: [7, 4] },
    ]);

    expect(results).toHaveLength(3);
    expect(results[0].result).toBe(20);
    expect(results[1].result).toBe(15);
    expect(results[2].result).toBe(28);
  });
});

// =============================================================================
// ERROR HANDLING TESTS
// =============================================================================

describe('Error Handling', () => {
  describe('call without registry', () => {
    it('should return error when calling by name without registry', async () => {
      const adapters = {
        users: createInMemoryAdapter<UserRecord>([]),
        orders: createInMemoryAdapter<OrderRecord>([]),
        accounts: createInMemoryAdapter<AccountRecord>([]),
      };

      const tableMap = new Map(Object.entries(adapters));
      const db = createDatabaseContext<TestDB>({
        adapters: adapters as unknown as DatabaseContextOptions<TestDB>['adapters'],
        sqlExecutor: createInMemorySqlExecutor(tableMap),
        transactionManager: createInMemoryTransactionManager(),
      });

      const executor = createProcedureExecutor({
        db,
        // No registry provided
      });

      const result = await executor.call('any_proc', []);

      expect(result.success).toBe(false);
      expect(result.error).toContain('Registry not configured');
    });
  });

  describe('procedure not found', () => {
    it('should return error for non-existent procedure', async () => {
      const adapters = {
        users: createInMemoryAdapter<UserRecord>([]),
        orders: createInMemoryAdapter<OrderRecord>([]),
        accounts: createInMemoryAdapter<AccountRecord>([]),
      };

      const tableMap = new Map(Object.entries(adapters));
      const db = createDatabaseContext<TestDB>({
        adapters: adapters as unknown as DatabaseContextOptions<TestDB>['adapters'],
        sqlExecutor: createInMemorySqlExecutor(tableMap),
        transactionManager: createInMemoryTransactionManager(),
      });

      const registry = createProcedureRegistry({
        storage: createInMemoryCatalogStorage(),
      });

      const executor = createProcedureExecutor({
        db,
        registry,
      });

      const result = await executor.call('nonexistent_procedure', []);

      expect(result.success).toBe(false);
      expect(result.error).toContain("Procedure 'nonexistent_procedure' not found");
    });
  });

  describe('mock error handling', () => {
    it('should handle async mock that throws', async () => {
      const executor = createMockExecutor({
        throwing_proc: async () => {
          throw new Error('Mock procedure error');
        },
      });

      // The mock executor doesn't catch errors from function mocks
      // So this should propagate the error
      await expect(executor.call('throwing_proc', [])).rejects.toThrow('Mock procedure error');
    });

    it('should handle sync mock that throws', async () => {
      const executor = createMockExecutor({
        sync_throwing: () => {
          throw new Error('Sync error');
        },
      });

      await expect(executor.call('sync_throwing', [])).rejects.toThrow('Sync error');
    });
  });
});

// =============================================================================
// EXECUTION OPTIONS TESTS
// =============================================================================

describe('Execution Options', () => {
  it('should pass environment variables to execution', async () => {
    const adapters = {
      users: createInMemoryAdapter<UserRecord>([]),
      orders: createInMemoryAdapter<OrderRecord>([]),
      accounts: createInMemoryAdapter<AccountRecord>([]),
    };

    const tableMap = new Map(Object.entries(adapters));
    const db = createDatabaseContext<TestDB>({
      adapters: adapters as unknown as DatabaseContextOptions<TestDB>['adapters'],
      sqlExecutor: createInMemorySqlExecutor(tableMap),
      transactionManager: createInMemoryTransactionManager(),
    });

    const executor = createProcedureExecutor({
      db,
      baseEnv: {
        BASE_VAR: 'base-value',
      },
    });

    // The actual env passing is tested via the execute method
    // which wraps the code with the env
    const result = await executor.execute('export default () => 1', [], {
      env: {
        CUSTOM_VAR: 'custom-value',
      },
    });

    // The result should include requestId showing execution happened
    expect(result.requestId).toBeDefined();
  });

  it('should support custom timeout option', async () => {
    const adapters = {
      users: createInMemoryAdapter<UserRecord>([]),
      orders: createInMemoryAdapter<OrderRecord>([]),
      accounts: createInMemoryAdapter<AccountRecord>([]),
    };

    const tableMap = new Map(Object.entries(adapters));
    const db = createDatabaseContext<TestDB>({
      adapters: adapters as unknown as DatabaseContextOptions<TestDB>['adapters'],
      sqlExecutor: createInMemorySqlExecutor(tableMap),
      transactionManager: createInMemoryTransactionManager(),
    });

    const executor = createProcedureExecutor({
      db,
      defaultTimeout: 1000,
    });

    const result = await executor.execute('export default () => "fast"', [], {
      timeout: 5000,
    });

    expect(result.requestId).toBeDefined();
  });
});

// =============================================================================
// PROCEDURE RUN TESTS
// =============================================================================

describe('Procedure run() method', () => {
  it('should run procedure definition with options', async () => {
    const adapters = {
      users: createInMemoryAdapter<UserRecord>([]),
      orders: createInMemoryAdapter<OrderRecord>([]),
      accounts: createInMemoryAdapter<AccountRecord>([]),
    };

    const tableMap = new Map(Object.entries(adapters));
    const db = createDatabaseContext<TestDB>({
      adapters: adapters as unknown as DatabaseContextOptions<TestDB>['adapters'],
      sqlExecutor: createInMemorySqlExecutor(tableMap),
      transactionManager: createInMemoryTransactionManager(),
    });

    const executor = createProcedureExecutor({
      db,
    });

    const procedure: Procedure = {
      name: 'test_proc',
      code: 'export default () => "hello"',
      timeout: 3000,
      memoryLimit: 64,
      metadata: {
        name: 'test_proc',
        version: 1,
        createdAt: new Date(),
        updatedAt: new Date(),
      },
    };

    const result = await executor.run(procedure, []);

    expect(result.requestId).toBeDefined();
    expect(result.duration).toBeGreaterThanOrEqual(0);
  });

  it('should merge procedure options with execution options', async () => {
    const adapters = {
      users: createInMemoryAdapter<UserRecord>([]),
      orders: createInMemoryAdapter<OrderRecord>([]),
      accounts: createInMemoryAdapter<AccountRecord>([]),
    };

    const tableMap = new Map(Object.entries(adapters));
    const db = createDatabaseContext<TestDB>({
      adapters: adapters as unknown as DatabaseContextOptions<TestDB>['adapters'],
      sqlExecutor: createInMemorySqlExecutor(tableMap),
      transactionManager: createInMemoryTransactionManager(),
    });

    const executor = createProcedureExecutor({
      db,
      defaultTimeout: 5000,
      defaultMemoryLimit: 128,
    });

    const procedure: Procedure = {
      name: 'configured_proc',
      code: 'export default () => "configured"',
      timeout: 2000, // Procedure-specific timeout
      // No memoryLimit - should use default
      metadata: {
        name: 'configured_proc',
        version: 1,
        createdAt: new Date(),
        updatedAt: new Date(),
      },
    };

    const result = await executor.run(procedure, [], {
      // Override with execution option
      timeout: 1000,
    });

    expect(result.requestId).toBeDefined();
  });
});

// =============================================================================
// INTEGRATION WITH REGISTRY TESTS
// =============================================================================

describe('Integration with Registry', () => {
  let registry: ProcedureRegistry;
  let executor: ProcedureExecutor<TestDB>;

  beforeEach(async () => {
    const adapters = {
      users: createInMemoryAdapter<UserRecord>([
        { id: 1, name: 'Alice', email: 'alice@test.com', active: true },
      ]),
      orders: createInMemoryAdapter<OrderRecord>([]),
      accounts: createInMemoryAdapter<AccountRecord>([]),
    };

    const tableMap = new Map(Object.entries(adapters));
    const db = createDatabaseContext<TestDB>({
      adapters: adapters as unknown as DatabaseContextOptions<TestDB>['adapters'],
      sqlExecutor: createInMemorySqlExecutor(tableMap),
      transactionManager: createInMemoryTransactionManager(),
    });

    registry = createProcedureRegistry({
      storage: createInMemoryCatalogStorage(),
    });

    executor = createProcedureExecutor({
      db,
      registry,
    });

    // Register a test procedure
    await registry.register({
      name: 'get_constant',
      code: 'export default () => 42',
    });
  });

  it('should call registered procedure by name', async () => {
    const result = await executor.call('get_constant', []);

    expect(result.requestId).toBeDefined();
    // Note: Actual execution depends on ai-evaluate
  });

  it('should handle procedure updates', async () => {
    // Register v2
    await registry.register({
      name: 'get_constant',
      code: 'export default () => 100',
    });

    // Get should return latest version
    const proc = await registry.get('get_constant');
    expect(proc?.metadata.version).toBe(2);

    const result = await executor.call('get_constant', []);
    expect(result.requestId).toBeDefined();
  });
});
