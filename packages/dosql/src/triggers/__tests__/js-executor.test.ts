/**
 * JavaScript Trigger Executor Tests
 *
 * Tests for JavaScript/TypeScript trigger execution with:
 * - BEFORE/AFTER trigger execution
 * - Row modification in BEFORE triggers
 * - Error handling and rejection
 * - Timeout handling
 * - Recursion depth limits
 * - Conditional triggers
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  createJSTriggerExecutor,
  buildTriggerContext,
  buildMeta,
  executeDirectly,
} from '../js-executor.js';
import { createTriggerRegistry } from '../registry.js';
import type {
  TriggerRegistry,
  TriggerDefinition,
  TriggerContext,
  TriggerExecutor,
  TriggerConfig,
} from '../types.js';
import { TriggerError, TriggerErrorCode } from '../types.js';
import type { DatabaseContext } from '../../proc/types.js';

// =============================================================================
// Test Data and Helpers
// =============================================================================

interface UserRow {
  id: number;
  name: string;
  email: string;
  status: string;
  created_at?: string;
}

function createMockDatabaseContext(): DatabaseContext {
  return {
    tables: {},
    execute: vi.fn().mockResolvedValue([]),
    query: vi.fn().mockResolvedValue([]),
    get: vi.fn().mockResolvedValue(null),
    insert: vi.fn().mockResolvedValue(undefined),
    update: vi.fn().mockResolvedValue(0),
    delete: vi.fn().mockResolvedValue(0),
  } as unknown as DatabaseContext;
}

function createTestTrigger(
  name: string,
  handler: (ctx: TriggerContext<UserRow>) => void | UserRow | Promise<void | UserRow>,
  options: Partial<TriggerDefinition<UserRow>> = {}
): TriggerDefinition<UserRow> {
  return {
    name,
    table: options.table ?? 'users',
    timing: options.timing ?? 'before',
    events: options.events ?? ['insert'],
    handler: handler as any,
    priority: options.priority,
    enabled: options.enabled,
    condition: options.condition,
  };
}

// =============================================================================
// Context Building Tests
// =============================================================================

describe('Trigger Context Building', () => {
  const db = createMockDatabaseContext();

  describe('buildTriggerContext()', () => {
    it('should build context for INSERT', () => {
      const meta = buildMeta('test_trigger', 'req_123', 0);
      const ctx = buildTriggerContext<UserRow>(
        'users',
        'before',
        'insert',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' },
        db,
        meta
      );

      expect(ctx.table).toBe('users');
      expect(ctx.timing).toBe('before');
      expect(ctx.event).toBe('insert');
      expect(ctx.old).toBeUndefined();
      expect(ctx.new).toEqual({ id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' });
      expect(ctx.db).toBe(db);
      expect(ctx.meta.triggerName).toBe('test_trigger');
      expect(ctx.meta.requestId).toBe('req_123');
      expect(ctx.meta.depth).toBe(0);
    });

    it('should build context for UPDATE', () => {
      const meta = buildMeta('test_trigger', 'req_123', 0);
      const ctx = buildTriggerContext<UserRow>(
        'users',
        'before',
        'update',
        { id: 1, name: 'Alice', email: 'old@example.com', status: 'active' },
        { id: 1, name: 'Alice', email: 'new@example.com', status: 'active' },
        db,
        meta,
        'txn_456'
      );

      expect(ctx.event).toBe('update');
      expect(ctx.old?.email).toBe('old@example.com');
      expect(ctx.new?.email).toBe('new@example.com');
      expect(ctx.txnId).toBe('txn_456');
    });

    it('should build context for DELETE', () => {
      const meta = buildMeta('test_trigger', 'req_123', 0);
      const ctx = buildTriggerContext<UserRow>(
        'users',
        'before',
        'delete',
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' },
        undefined,
        db,
        meta
      );

      expect(ctx.event).toBe('delete');
      expect(ctx.old).toBeDefined();
      expect(ctx.new).toBeUndefined();
    });
  });

  describe('buildMeta()', () => {
    it('should build execution metadata', () => {
      const meta = buildMeta('audit_trigger', 'request_abc', 2);

      expect(meta.triggerName).toBe('audit_trigger');
      expect(meta.requestId).toBe('request_abc');
      expect(meta.depth).toBe(2);
      expect(meta.timestamp).toBeInstanceOf(Date);
    });
  });
});

// =============================================================================
// BEFORE Trigger Execution Tests
// =============================================================================

describe('BEFORE Trigger Execution', () => {
  let registry: TriggerRegistry;
  let executor: TriggerExecutor;
  let db: DatabaseContext;

  beforeEach(() => {
    registry = createTriggerRegistry();
    db = createMockDatabaseContext();
    executor = createJSTriggerExecutor({ registry, db });
  });

  describe('Basic Execution', () => {
    it('should execute BEFORE INSERT trigger', async () => {
      const handler = vi.fn();
      registry.register(createTestTrigger('before_insert', handler));

      const result = await executor.executeBefore<UserRow>(
        'users',
        'insert',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'pending' }
      );

      expect(result.proceed).toBe(true);
      expect(handler).toHaveBeenCalledTimes(1);
      expect(result.executions).toHaveLength(1);
      expect(result.executions[0].success).toBe(true);
    });

    it('should execute BEFORE UPDATE trigger', async () => {
      const handler = vi.fn();
      registry.register(createTestTrigger('before_update', handler, {
        timing: 'before',
        events: ['update'],
      }));

      const result = await executor.executeBefore<UserRow>(
        'users',
        'update',
        { id: 1, name: 'Alice', email: 'old@example.com', status: 'active' },
        { id: 1, name: 'Alice', email: 'new@example.com', status: 'active' }
      );

      expect(result.proceed).toBe(true);
      expect(handler).toHaveBeenCalled();
    });

    it('should execute BEFORE DELETE trigger', async () => {
      const handler = vi.fn();
      registry.register(createTestTrigger('before_delete', handler, {
        timing: 'before',
        events: ['delete'],
      }));

      const result = await executor.executeBefore<UserRow>(
        'users',
        'delete',
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' },
        undefined
      );

      expect(result.proceed).toBe(true);
      expect(handler).toHaveBeenCalled();
    });
  });

  describe('Row Modification', () => {
    it('should allow BEFORE trigger to modify the row', async () => {
      registry.register(createTestTrigger('add_timestamp', (ctx) => {
        return {
          ...ctx.new!,
          created_at: '2024-01-01T00:00:00Z',
        };
      }));

      const result = await executor.executeBefore<UserRow>(
        'users',
        'insert',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      expect(result.proceed).toBe(true);
      expect(result.row?.created_at).toBe('2024-01-01T00:00:00Z');
      expect(result.executions[0].modified).toBe(true);
    });

    it('should pass modified row to subsequent triggers', async () => {
      registry.register(createTestTrigger('first_trigger', (ctx) => {
        return { ...ctx.new!, name: 'Modified' };
      }, { priority: 10 }));

      registry.register(createTestTrigger('second_trigger', (ctx) => {
        return { ...ctx.new!, email: `${ctx.new!.name}@example.com` };
      }, { priority: 20 }));

      const result = await executor.executeBefore<UserRow>(
        'users',
        'insert',
        undefined,
        { id: 1, name: 'Original', email: 'original@example.com', status: 'active' }
      );

      expect(result.row?.name).toBe('Modified');
      expect(result.row?.email).toBe('Modified@example.com');
    });
  });

  describe('Operation Rejection', () => {
    it('should reject operation when trigger throws', async () => {
      registry.register(createTestTrigger('validator', () => {
        throw new Error('Validation failed: invalid email');
      }));

      const result = await executor.executeBefore<UserRow>(
        'users',
        'insert',
        undefined,
        { id: 1, name: 'Alice', email: 'invalid', status: 'active' }
      );

      expect(result.proceed).toBe(false);
      expect(result.error).toBeInstanceOf(TriggerError);
      expect(result.error?.message).toContain('rejected the operation');
    });

    it('should stop execution on first failure', async () => {
      const secondHandler = vi.fn();

      registry.register(createTestTrigger('failing_trigger', () => {
        throw new Error('First trigger failed');
      }, { priority: 10 }));

      registry.register(createTestTrigger('second_trigger', secondHandler, { priority: 20 }));

      const result = await executor.executeBefore<UserRow>(
        'users',
        'insert',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      expect(result.proceed).toBe(false);
      expect(secondHandler).not.toHaveBeenCalled();
    });
  });

  describe('Conditional Triggers', () => {
    it('should skip trigger when function condition returns false', async () => {
      const handler = vi.fn();
      registry.register(createTestTrigger('conditional', handler, {
        condition: (ctx) => ctx.new?.status === 'vip',
      }));

      const result = await executor.executeBefore<UserRow>(
        'users',
        'insert',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'regular' }
      );

      expect(result.proceed).toBe(true);
      expect(handler).not.toHaveBeenCalled();
      expect(result.executions).toHaveLength(0);
    });

    it('should execute trigger when function condition returns true', async () => {
      const handler = vi.fn();
      registry.register(createTestTrigger('conditional', handler, {
        condition: (ctx) => ctx.new?.status === 'vip',
      }));

      const result = await executor.executeBefore<UserRow>(
        'users',
        'insert',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'vip' }
      );

      expect(handler).toHaveBeenCalled();
    });
  });

  describe('Disabled Triggers', () => {
    it('should skip disabled triggers by default', async () => {
      const handler = vi.fn();
      registry.register(createTestTrigger('disabled_trigger', handler, { enabled: false }));

      const result = await executor.executeBefore<UserRow>(
        'users',
        'insert',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      expect(handler).not.toHaveBeenCalled();
      expect(result.executions).toHaveLength(0);
    });

    it('should execute disabled triggers when skipDisabled is false', async () => {
      const handler = vi.fn();
      registry.register(createTestTrigger('disabled_trigger', handler, { enabled: false }));

      const result = await executor.executeBefore<UserRow>(
        'users',
        'insert',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' },
        { skipDisabled: false }
      );

      expect(handler).toHaveBeenCalled();
    });
  });

  describe('No Triggers', () => {
    it('should proceed when no triggers exist', async () => {
      const result = await executor.executeBefore<UserRow>(
        'users',
        'insert',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      expect(result.proceed).toBe(true);
      expect(result.row).toEqual({ id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' });
      expect(result.executions).toHaveLength(0);
    });

    it('should proceed when no triggers match table', async () => {
      registry.register(createTestTrigger('other_table_trigger', vi.fn(), { table: 'orders' }));

      const result = await executor.executeBefore<UserRow>(
        'users',
        'insert',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      expect(result.proceed).toBe(true);
      expect(result.executions).toHaveLength(0);
    });
  });
});

// =============================================================================
// AFTER Trigger Execution Tests
// =============================================================================

describe('AFTER Trigger Execution', () => {
  let registry: TriggerRegistry;
  let executor: TriggerExecutor;
  let db: DatabaseContext;

  beforeEach(() => {
    registry = createTriggerRegistry();
    db = createMockDatabaseContext();
    executor = createJSTriggerExecutor({ registry, db });
  });

  describe('Basic Execution', () => {
    it('should execute AFTER INSERT trigger', async () => {
      const handler = vi.fn();
      registry.register(createTestTrigger('after_insert', handler, {
        timing: 'after',
        events: ['insert'],
      }));

      const result = await executor.executeAfter<UserRow>(
        'users',
        'insert',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      expect(result.success).toBe(true);
      expect(handler).toHaveBeenCalled();
      expect(result.executions).toHaveLength(1);
    });

    it('should execute AFTER UPDATE trigger', async () => {
      const handler = vi.fn();
      registry.register(createTestTrigger('after_update', handler, {
        timing: 'after',
        events: ['update'],
      }));

      const result = await executor.executeAfter<UserRow>(
        'users',
        'update',
        { id: 1, name: 'Alice', email: 'old@example.com', status: 'active' },
        { id: 1, name: 'Alice', email: 'new@example.com', status: 'active' }
      );

      expect(result.success).toBe(true);
      expect(handler).toHaveBeenCalled();
    });

    it('should execute AFTER DELETE trigger', async () => {
      const handler = vi.fn();
      registry.register(createTestTrigger('after_delete', handler, {
        timing: 'after',
        events: ['delete'],
      }));

      const result = await executor.executeAfter<UserRow>(
        'users',
        'delete',
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' },
        undefined
      );

      expect(result.success).toBe(true);
      expect(handler).toHaveBeenCalled();
    });
  });

  describe('Error Handling', () => {
    it('should continue execution after trigger error', async () => {
      const secondHandler = vi.fn();

      registry.register(createTestTrigger('failing_trigger', () => {
        throw new Error('Trigger failed');
      }, { timing: 'after', priority: 10 }));

      registry.register(createTestTrigger('second_trigger', secondHandler, {
        timing: 'after',
        priority: 20,
      }));

      const result = await executor.executeAfter<UserRow>(
        'users',
        'insert',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      expect(result.success).toBe(false);
      expect(result.errors).toHaveLength(1);
      expect(secondHandler).toHaveBeenCalled();
    });

    it('should collect all errors', async () => {
      registry.register(createTestTrigger('failing_1', () => {
        throw new Error('Error 1');
      }, { timing: 'after', priority: 10 }));

      registry.register(createTestTrigger('failing_2', () => {
        throw new Error('Error 2');
      }, { timing: 'after', priority: 20 }));

      const result = await executor.executeAfter<UserRow>(
        'users',
        'insert',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      expect(result.success).toBe(false);
      expect(result.errors).toHaveLength(2);
    });

    it('should call error handler when provided', async () => {
      const onAfterError = vi.fn();
      executor = createJSTriggerExecutor({ registry, db, onAfterError });

      registry.register(createTestTrigger('failing_trigger', () => {
        throw new Error('Trigger failed');
      }, { timing: 'after' }));

      await executor.executeAfter<UserRow>(
        'users',
        'insert',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      expect(onAfterError).toHaveBeenCalledWith('failing_trigger', expect.any(Error));
    });
  });

  describe('Return Value Ignored', () => {
    it('should ignore return value from AFTER triggers', async () => {
      registry.register(createTestTrigger('after_with_return', (ctx) => {
        return { ...ctx.new!, name: 'Modified' };
      }, { timing: 'after' }));

      const result = await executor.executeAfter<UserRow>(
        'users',
        'insert',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      expect(result.success).toBe(true);
      // Return value is ignored for AFTER triggers
    });
  });
});

// =============================================================================
// executeAll() Tests
// =============================================================================

describe('executeAll() - Combined Trigger Execution', () => {
  let registry: TriggerRegistry;
  let executor: TriggerExecutor;
  let db: DatabaseContext;

  beforeEach(() => {
    registry = createTriggerRegistry();
    db = createMockDatabaseContext();
    executor = createJSTriggerExecutor({ registry, db });
  });

  it('should execute BEFORE triggers, operation, and AFTER triggers in order', async () => {
    const callOrder: string[] = [];

    registry.register(createTestTrigger('before_trigger', () => {
      callOrder.push('before');
    }, { timing: 'before' }));

    registry.register(createTestTrigger('after_trigger', () => {
      callOrder.push('after');
    }, { timing: 'after' }));

    const operation = vi.fn().mockImplementation(async (row) => {
      callOrder.push('operation');
      return row;
    });

    await executor.executeAll<UserRow>(
      'users',
      'insert',
      undefined,
      { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' },
      operation
    );

    expect(callOrder).toEqual(['before', 'operation', 'after']);
  });

  it('should pass modified row from BEFORE triggers to operation', async () => {
    registry.register(createTestTrigger('modifier', (ctx) => {
      return { ...ctx.new!, name: 'Modified' };
    }, { timing: 'before' }));

    const operation = vi.fn().mockImplementation(async (row) => row);

    const result = await executor.executeAll<UserRow>(
      'users',
      'insert',
      undefined,
      { id: 1, name: 'Original', email: 'alice@example.com', status: 'active' },
      operation
    );

    expect(operation).toHaveBeenCalledWith(expect.objectContaining({ name: 'Modified' }));
  });

  it('should not execute operation when BEFORE trigger rejects', async () => {
    registry.register(createTestTrigger('rejector', () => {
      throw new Error('Rejected');
    }, { timing: 'before' }));

    const operation = vi.fn();

    const result = await executor.executeAll<UserRow>(
      'users',
      'insert',
      undefined,
      { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' },
      operation
    );

    expect(result.before.proceed).toBe(false);
    expect(operation).not.toHaveBeenCalled();
  });

  it('should not execute AFTER triggers when BEFORE rejects', async () => {
    const afterHandler = vi.fn();

    registry.register(createTestTrigger('rejector', () => {
      throw new Error('Rejected');
    }, { timing: 'before' }));

    registry.register(createTestTrigger('after_trigger', afterHandler, { timing: 'after' }));

    await executor.executeAll<UserRow>(
      'users',
      'insert',
      undefined,
      { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' },
      async () => undefined
    );

    expect(afterHandler).not.toHaveBeenCalled();
  });
});

// =============================================================================
// Recursion Depth Tests
// =============================================================================

describe('Recursion Depth Limits', () => {
  let registry: TriggerRegistry;
  let db: DatabaseContext;

  beforeEach(() => {
    registry = createTriggerRegistry();
    db = createMockDatabaseContext();
  });

  it('should reject when max depth is exceeded', async () => {
    const executor = createJSTriggerExecutor({ registry, db, maxDepth: 3 });

    registry.register(createTestTrigger('test_trigger', vi.fn()));

    const result = await executor.executeBefore<UserRow>(
      'users',
      'insert',
      undefined,
      { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' },
      { currentDepth: 3 }
    );

    expect(result.proceed).toBe(false);
    expect(result.error?.code).toBe(TriggerErrorCode.MAX_DEPTH_EXCEEDED);
  });

  it('should respect custom maxDepth in options', async () => {
    const executor = createJSTriggerExecutor({ registry, db, maxDepth: 10 });

    registry.register(createTestTrigger('test_trigger', vi.fn()));

    const result = await executor.executeBefore<UserRow>(
      'users',
      'insert',
      undefined,
      { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' },
      { currentDepth: 5, maxDepth: 5 }
    );

    expect(result.proceed).toBe(false);
    expect(result.error?.code).toBe(TriggerErrorCode.MAX_DEPTH_EXCEEDED);
  });

  it('should reject AFTER triggers when max depth exceeded', async () => {
    const executor = createJSTriggerExecutor({ registry, db, maxDepth: 2 });

    registry.register(createTestTrigger('test_trigger', vi.fn(), { timing: 'after' }));

    const result = await executor.executeAfter<UserRow>(
      'users',
      'insert',
      undefined,
      { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' },
      { currentDepth: 2 }
    );

    expect(result.success).toBe(false);
    expect(result.errors[0].triggerName).toBe('depth_check');
  });
});

// =============================================================================
// Timeout Tests
// =============================================================================

describe('Trigger Timeout', () => {
  let registry: TriggerRegistry;
  let db: DatabaseContext;
  let executor: TriggerExecutor;

  beforeEach(() => {
    registry = createTriggerRegistry();
    db = createMockDatabaseContext();
    executor = createJSTriggerExecutor({ registry, db, defaultTimeout: 100 });
  });

  it('should timeout slow triggers', async () => {
    registry.register(createTestTrigger('slow_trigger', async () => {
      await new Promise(resolve => setTimeout(resolve, 500));
    }));

    const result = await executor.executeBefore<UserRow>(
      'users',
      'insert',
      undefined,
      { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
    );

    expect(result.proceed).toBe(false);
    expect(result.error?.message).toContain('timed out');
  });

  it('should respect custom timeout in options', async () => {
    registry.register(createTestTrigger('slow_trigger', async () => {
      await new Promise(resolve => setTimeout(resolve, 100));
    }));

    const result = await executor.executeBefore<UserRow>(
      'users',
      'insert',
      undefined,
      { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' },
      { timeout: 200 }
    );

    expect(result.proceed).toBe(true);
  });
});

// =============================================================================
// Direct Execution Tests
// =============================================================================

describe('executeDirectly()', () => {
  it('should execute trigger handler directly', async () => {
    const handler = vi.fn().mockReturnValue({ id: 1, name: 'Modified' });
    const trigger: TriggerConfig = {
      name: 'test',
      table: 'users',
      timing: 'before',
      events: ['insert'],
      handler,
      version: 1,
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    const ctx = buildTriggerContext(
      'users',
      'before',
      'insert',
      undefined,
      { id: 1, name: 'Original' },
      createMockDatabaseContext(),
      buildMeta('test', 'req_1', 0)
    );

    const { result, modified } = await executeDirectly(trigger, ctx, 5000);

    expect(handler).toHaveBeenCalled();
    expect(result).toEqual({ id: 1, name: 'Modified' });
    expect(modified).toBe(true);
  });

  it('should handle async handlers', async () => {
    const handler = vi.fn().mockResolvedValue({ id: 1, name: 'Async Modified' });
    const trigger: TriggerConfig = {
      name: 'test',
      table: 'users',
      timing: 'before',
      events: ['insert'],
      handler,
      version: 1,
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    const ctx = buildTriggerContext(
      'users',
      'before',
      'insert',
      undefined,
      { id: 1, name: 'Original' },
      createMockDatabaseContext(),
      buildMeta('test', 'req_1', 0)
    );

    const { result } = await executeDirectly(trigger, ctx, 5000);
    expect(result).toEqual({ id: 1, name: 'Async Modified' });
  });

  it('should propagate errors from handler', async () => {
    const handler = vi.fn().mockRejectedValue(new Error('Handler error'));
    const trigger: TriggerConfig = {
      name: 'test',
      table: 'users',
      timing: 'before',
      events: ['insert'],
      handler,
      version: 1,
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    const ctx = buildTriggerContext(
      'users',
      'before',
      'insert',
      undefined,
      { id: 1, name: 'Original' },
      createMockDatabaseContext(),
      buildMeta('test', 'req_1', 0)
    );

    await expect(executeDirectly(trigger, ctx, 5000)).rejects.toThrow('Handler error');
  });
});

// =============================================================================
// Execution Metadata Tests
// =============================================================================

describe('Execution Metadata', () => {
  let registry: TriggerRegistry;
  let executor: TriggerExecutor;
  let db: DatabaseContext;

  beforeEach(() => {
    registry = createTriggerRegistry();
    db = createMockDatabaseContext();
    executor = createJSTriggerExecutor({ registry, db });
  });

  it('should track execution duration', async () => {
    registry.register(createTestTrigger('timed_trigger', async () => {
      await new Promise(resolve => setTimeout(resolve, 50));
    }));

    const result = await executor.executeBefore<UserRow>(
      'users',
      'insert',
      undefined,
      { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
    );

    expect(result.executions[0].duration).toBeGreaterThanOrEqual(40);
  });

  it('should record execution success/failure', async () => {
    registry.register(createTestTrigger('successful', vi.fn(), { priority: 10 }));
    registry.register(createTestTrigger('failing', () => {
      throw new Error('Failed');
    }, { priority: 20 }));

    const result = await executor.executeBefore<UserRow>(
      'users',
      'insert',
      undefined,
      { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
    );

    expect(result.executions).toHaveLength(2);
    expect(result.executions[0].success).toBe(true);
    expect(result.executions[1].success).toBe(false);
    expect(result.executions[1].error).toContain('Failed');
  });

  it('should use provided requestId', async () => {
    let receivedRequestId: string | undefined;
    registry.register(createTestTrigger('tracking_trigger', (ctx) => {
      receivedRequestId = ctx.meta.requestId;
    }));

    await executor.executeBefore<UserRow>(
      'users',
      'insert',
      undefined,
      { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' },
      { requestId: 'custom_request_123' }
    );

    expect(receivedRequestId).toBe('custom_request_123');
  });
});
