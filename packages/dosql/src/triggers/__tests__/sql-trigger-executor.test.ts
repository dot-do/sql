/**
 * SQL Trigger Executor Tests
 *
 * Tests for SQL trigger execution with:
 * - BEFORE/AFTER trigger execution
 * - WHEN clause evaluation
 * - UPDATE OF column detection
 * - RAISE function handling (IGNORE, ROLLBACK, ABORT, FAIL)
 * - SQL trigger body execution
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  createSQLTriggerExecutor,
  parseRaiseFunction,
  parseRaiseFunctions,
  evaluateRaiseCondition,
  didColumnsChange,
  type SQLTriggerExecutor,
} from '../sql-trigger-executor.js';
import { createTriggerRegistry } from '../registry.js';
import type {
  TriggerRegistry,
  ParsedSQLTrigger,
  SQLTriggerDefinition,
} from '../types.js';
import { TriggerError, TriggerErrorCode } from '../types.js';
import type { DatabaseContext } from '../../proc/types.js';

// =============================================================================
// Test Data and Helpers
// =============================================================================

interface TestRow {
  id: number;
  name: string;
  email: string;
  status: string;
  amount?: number;
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

function createSQLTrigger(
  name: string,
  options: Partial<ParsedSQLTrigger> = {}
): ParsedSQLTrigger {
  return {
    name,
    table: options.table ?? 'users',
    timing: options.timing ?? 'BEFORE',
    event: options.event ?? 'INSERT',
    events: options.events ?? [options.event ?? 'INSERT'],
    body: options.body ?? 'SELECT 1',
    forEachRow: options.forEachRow ?? true,
    ifNotExists: options.ifNotExists ?? false,
    temporary: options.temporary ?? false,
    referencesNew: options.referencesNew ?? true,
    referencesOld: options.referencesOld ?? false,
    statementCount: options.statementCount ?? 1,
    rawSql: options.rawSql ?? `CREATE TRIGGER ${name} ${options.timing ?? 'BEFORE'} ${options.event ?? 'INSERT'} ON ${options.table ?? 'users'} BEGIN ${options.body ?? 'SELECT 1'}; END`,
    columns: options.columns,
    whenClause: options.whenClause,
  };
}

// =============================================================================
// RAISE Function Parsing Tests
// =============================================================================

describe('RAISE Function Parsing', () => {
  describe('parseRaiseFunction()', () => {
    it('should parse RAISE(IGNORE)', () => {
      const result = parseRaiseFunction('RAISE(IGNORE)');
      expect(result).toEqual({ type: 'IGNORE', message: undefined });
    });

    it('should parse RAISE(ROLLBACK, message)', () => {
      const result = parseRaiseFunction("RAISE(ROLLBACK, 'Transaction aborted')");
      expect(result).toEqual({ type: 'ROLLBACK', message: 'Transaction aborted' });
    });

    it('should parse RAISE(ABORT, message)', () => {
      const result = parseRaiseFunction("RAISE(ABORT, 'Operation aborted')");
      expect(result).toEqual({ type: 'ABORT', message: 'Operation aborted' });
    });

    it('should parse RAISE(FAIL, message)', () => {
      const result = parseRaiseFunction('RAISE(FAIL, "Validation failed")');
      expect(result).toEqual({ type: 'FAIL', message: 'Validation failed' });
    });

    it('should handle case-insensitive keywords', () => {
      const result = parseRaiseFunction("raise(abort, 'error')");
      expect(result).toEqual({ type: 'ABORT', message: 'error' });
    });

    it('should return null for no RAISE function', () => {
      const result = parseRaiseFunction('SELECT 1');
      expect(result).toBeNull();
    });

    it('should return first RAISE for multiple RAISE functions', () => {
      const result = parseRaiseFunction("RAISE(IGNORE); RAISE(ABORT, 'error')");
      expect(result).toEqual({ type: 'IGNORE', message: undefined });
    });
  });

  describe('parseRaiseFunctions()', () => {
    it('should parse multiple RAISE functions', () => {
      const body = `
        SELECT CASE WHEN NEW.status = 'deleted' THEN RAISE(IGNORE) END;
        SELECT CASE WHEN NEW.amount < 0 THEN RAISE(ABORT, 'Negative amount') END;
      `;

      const results = parseRaiseFunctions(body);
      expect(results).toHaveLength(2);
      expect(results[0]).toEqual({ type: 'IGNORE', message: undefined });
      expect(results[1]).toEqual({ type: 'ABORT', message: 'Negative amount' });
    });

    it('should return empty array for no RAISE functions', () => {
      const results = parseRaiseFunctions('SELECT 1; UPDATE stats SET count = count + 1');
      expect(results).toHaveLength(0);
    });
  });

  describe('evaluateRaiseCondition()', () => {
    it('should return RAISE result when condition matches', () => {
      const body = "SELECT CASE WHEN NEW.status = 'blocked' THEN RAISE(ABORT, 'User is blocked') END";
      const result = evaluateRaiseCondition(body, undefined, { status: 'blocked' });
      expect(result).toEqual({ type: 'ABORT', message: 'User is blocked' });
    });

    it('should return null when condition does not match', () => {
      const body = "SELECT CASE WHEN NEW.status = 'blocked' THEN RAISE(ABORT, 'User is blocked') END";
      const result = evaluateRaiseCondition(body, undefined, { status: 'active' });
      expect(result).toBeNull();
    });

    it('should handle RAISE without condition', () => {
      const body = "RAISE(FAIL, 'Always fail')";
      const result = evaluateRaiseCondition(body, undefined, { status: 'any' });
      expect(result).toEqual({ type: 'FAIL', message: 'Always fail' });
    });

    it('should handle OLD references in condition', () => {
      const body = "SELECT CASE WHEN OLD.status = 'active' THEN RAISE(ABORT, 'Cannot modify active') END";
      const result = evaluateRaiseCondition(body, { status: 'active' }, { status: 'deleted' });
      expect(result).toEqual({ type: 'ABORT', message: 'Cannot modify active' });
    });

    it('should evaluate numeric conditions', () => {
      const body = "SELECT CASE WHEN NEW.amount < 0 THEN RAISE(FAIL, 'Invalid amount') END";
      const result = evaluateRaiseCondition(body, undefined, { amount: -10 });
      expect(result).toEqual({ type: 'FAIL', message: 'Invalid amount' });
    });
  });
});

// =============================================================================
// Column Change Detection Tests
// =============================================================================

describe('Column Change Detection', () => {
  describe('didColumnsChange()', () => {
    it('should return true when specified column changed', () => {
      const oldRow = { id: 1, name: 'Alice', email: 'old@example.com' };
      const newRow = { id: 1, name: 'Alice', email: 'new@example.com' };

      expect(didColumnsChange(['email'], oldRow, newRow)).toBe(true);
    });

    it('should return false when specified column did not change', () => {
      const oldRow = { id: 1, name: 'Alice', email: 'same@example.com' };
      const newRow = { id: 1, name: 'Bob', email: 'same@example.com' };

      expect(didColumnsChange(['email'], oldRow, newRow)).toBe(false);
    });

    it('should check multiple columns', () => {
      const oldRow = { id: 1, name: 'Alice', email: 'old@example.com', status: 'active' };
      const newRow = { id: 1, name: 'Alice', email: 'new@example.com', status: 'active' };

      expect(didColumnsChange(['name', 'status'], oldRow, newRow)).toBe(false);
      expect(didColumnsChange(['email', 'status'], oldRow, newRow)).toBe(true);
    });

    it('should return true for empty/undefined columns (fire for any change)', () => {
      const oldRow = { id: 1, name: 'Alice' };
      const newRow = { id: 1, name: 'Bob' };

      expect(didColumnsChange(undefined, oldRow, newRow)).toBe(true);
      expect(didColumnsChange([], oldRow, newRow)).toBe(true);
    });

    it('should return true when oldRow is undefined', () => {
      expect(didColumnsChange(['email'], undefined, { email: 'new@example.com' })).toBe(true);
    });

    it('should return true when newRow is undefined', () => {
      expect(didColumnsChange(['email'], { email: 'old@example.com' }, undefined)).toBe(true);
    });
  });
});

// =============================================================================
// SQL Trigger Executor Tests
// =============================================================================

describe('SQL Trigger Executor', () => {
  let registry: TriggerRegistry;
  let db: DatabaseContext;
  let executor: SQLTriggerExecutor;

  beforeEach(() => {
    registry = createTriggerRegistry();
    db = createMockDatabaseContext();
    executor = createSQLTriggerExecutor({ registry, db });
  });

  describe('BEFORE Trigger Execution', () => {
    it('should execute BEFORE INSERT SQL trigger', async () => {
      registry.register(createSQLTrigger('before_insert_users', {
        timing: 'BEFORE',
        event: 'INSERT',
        body: 'SELECT 1', // Simple body that doesn't raise
      }));

      const result = await executor.executeBefore<TestRow>(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      expect(result.proceed).toBe(true);
    });

    it('should execute BEFORE UPDATE SQL trigger', async () => {
      registry.register(createSQLTrigger('before_update_users', {
        timing: 'BEFORE',
        event: 'UPDATE',
        events: ['UPDATE'],
        body: 'SELECT 1',
      }));

      const result = await executor.executeBefore<TestRow>(
        'users',
        'UPDATE',
        { id: 1, name: 'Alice', email: 'old@example.com', status: 'active' },
        { id: 1, name: 'Alice', email: 'new@example.com', status: 'active' }
      );

      expect(result.proceed).toBe(true);
    });

    it('should execute BEFORE DELETE SQL trigger', async () => {
      registry.register(createSQLTrigger('before_delete_users', {
        timing: 'BEFORE',
        event: 'DELETE',
        events: ['DELETE'],
        body: 'SELECT 1',
      }));

      const result = await executor.executeBefore<TestRow>(
        'users',
        'DELETE',
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' },
        undefined
      );

      expect(result.proceed).toBe(true);
    });

    it('should reject when RAISE(ABORT) condition is met', async () => {
      registry.register(createSQLTrigger('validate_status', {
        timing: 'BEFORE',
        event: 'INSERT',
        body: "SELECT CASE WHEN NEW.status = 'blocked' THEN RAISE(ABORT, 'Cannot insert blocked user') END",
      }));

      const result = await executor.executeBefore<TestRow>(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'blocked' }
      );

      expect(result.proceed).toBe(false);
      expect(result.error?.message).toContain('Cannot insert blocked user');
    });

    it('should proceed when RAISE(IGNORE) is triggered', async () => {
      registry.register(createSQLTrigger('ignore_duplicates', {
        timing: 'BEFORE',
        event: 'INSERT',
        body: "SELECT CASE WHEN NEW.status = 'duplicate' THEN RAISE(IGNORE) END",
      }));

      const result = await executor.executeBefore<TestRow>(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'duplicate' }
      );

      expect(result.proceed).toBe(true);
    });
  });

  describe('AFTER Trigger Execution', () => {
    it('should execute AFTER INSERT SQL trigger', async () => {
      registry.register(createSQLTrigger('after_insert_audit', {
        timing: 'AFTER',
        event: 'INSERT',
        body: 'INSERT INTO audit_log (action) VALUES ("insert")',
      }));

      const result = await executor.executeAfter<TestRow>(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      expect(result.success).toBe(true);
    });

    it('should execute AFTER UPDATE SQL trigger', async () => {
      registry.register(createSQLTrigger('after_update_audit', {
        timing: 'AFTER',
        event: 'UPDATE',
        events: ['UPDATE'],
        body: 'INSERT INTO audit_log (action) VALUES ("update")',
      }));

      const result = await executor.executeAfter<TestRow>(
        'users',
        'UPDATE',
        { id: 1, name: 'Alice', email: 'old@example.com', status: 'active' },
        { id: 1, name: 'Alice', email: 'new@example.com', status: 'active' }
      );

      expect(result.success).toBe(true);
    });

    it('should execute AFTER DELETE SQL trigger', async () => {
      registry.register(createSQLTrigger('after_delete_audit', {
        timing: 'AFTER',
        event: 'DELETE',
        events: ['DELETE'],
        body: 'INSERT INTO audit_log (action) VALUES ("delete")',
      }));

      const result = await executor.executeAfter<TestRow>(
        'users',
        'DELETE',
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' },
        undefined
      );

      expect(result.success).toBe(true);
    });

    it('should collect errors from failing AFTER triggers', async () => {
      registry.register(createSQLTrigger('failing_after', {
        timing: 'AFTER',
        event: 'INSERT',
        body: "RAISE(FAIL, 'After trigger error')",
      }));

      const result = await executor.executeAfter<TestRow>(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      expect(result.success).toBe(false);
      expect(result.errors).toHaveLength(1);
    });
  });

  describe('WHEN Clause Evaluation', () => {
    it('should skip trigger when WHEN clause is false', async () => {
      registry.register(createSQLTrigger('conditional_trigger', {
        timing: 'BEFORE',
        event: 'INSERT',
        whenClause: "NEW.status = 'vip'",
        body: "RAISE(ABORT, 'VIP validation')",
      }));

      const result = await executor.executeBefore<TestRow>(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'regular' }
      );

      expect(result.proceed).toBe(true);
      // Trigger should be skipped
    });

    it('should execute trigger when WHEN clause is true', async () => {
      registry.register(createSQLTrigger('conditional_trigger', {
        timing: 'BEFORE',
        event: 'INSERT',
        whenClause: "NEW.status = 'vip'",
        body: "RAISE(ABORT, 'VIP validation failed')",
      }));

      const result = await executor.executeBefore<TestRow>(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'vip' }
      );

      expect(result.proceed).toBe(false);
    });

    it('should handle complex WHEN clauses', async () => {
      registry.register(createSQLTrigger('complex_when', {
        timing: 'BEFORE',
        event: 'UPDATE',
        events: ['UPDATE'],
        whenClause: "OLD.status = 'active' AND NEW.status = 'deleted'",
        body: "RAISE(ABORT, 'Cannot delete active user directly')",
      }));

      const result = await executor.executeBefore<TestRow>(
        'users',
        'UPDATE',
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' },
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'deleted' }
      );

      expect(result.proceed).toBe(false);
    });
  });

  describe('UPDATE OF Columns', () => {
    it('should fire trigger when specified column changes', async () => {
      registry.register(createSQLTrigger('email_change_trigger', {
        timing: 'BEFORE',
        event: 'UPDATE',
        events: ['UPDATE'],
        columns: ['email'],
        body: "RAISE(ABORT, 'Email changed')",
      }));

      const result = await executor.executeBefore<TestRow>(
        'users',
        'UPDATE',
        { id: 1, name: 'Alice', email: 'old@example.com', status: 'active' },
        { id: 1, name: 'Alice', email: 'new@example.com', status: 'active' }
      );

      expect(result.proceed).toBe(false);
    });

    it('should not fire trigger when specified column does not change', async () => {
      registry.register(createSQLTrigger('email_change_trigger', {
        timing: 'BEFORE',
        event: 'UPDATE',
        events: ['UPDATE'],
        columns: ['email'],
        body: "RAISE(ABORT, 'Email changed')",
      }));

      const result = await executor.executeBefore<TestRow>(
        'users',
        'UPDATE',
        { id: 1, name: 'Alice', email: 'same@example.com', status: 'active' },
        { id: 1, name: 'Bob', email: 'same@example.com', status: 'active' }
      );

      expect(result.proceed).toBe(true);
    });

    it('should check multiple columns', async () => {
      registry.register(createSQLTrigger('critical_fields_trigger', {
        timing: 'BEFORE',
        event: 'UPDATE',
        events: ['UPDATE'],
        columns: ['email', 'status'],
        body: "RAISE(ABORT, 'Critical field changed')",
      }));

      // Neither email nor status changed
      const result1 = await executor.executeBefore<TestRow>(
        'users',
        'UPDATE',
        { id: 1, name: 'Alice', email: 'same@example.com', status: 'active' },
        { id: 1, name: 'Bob', email: 'same@example.com', status: 'active' }
      );
      expect(result1.proceed).toBe(true);

      // Status changed
      const result2 = await executor.executeBefore<TestRow>(
        'users',
        'UPDATE',
        { id: 1, name: 'Alice', email: 'same@example.com', status: 'active' },
        { id: 1, name: 'Alice', email: 'same@example.com', status: 'inactive' }
      );
      expect(result2.proceed).toBe(false);
    });
  });

  describe('Recursion Depth', () => {
    it('should reject when max depth is exceeded', async () => {
      registry.register(createSQLTrigger('test_trigger', {
        timing: 'BEFORE',
        event: 'INSERT',
        body: 'SELECT 1',
      }));

      const result = await executor.executeBefore<TestRow>(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' },
        { currentDepth: 10, maxDepth: 10 }
      );

      expect(result.proceed).toBe(false);
      expect(result.error?.code).toBe(TriggerErrorCode.MAX_DEPTH_EXCEEDED);
    });
  });

  describe('Disabled Triggers', () => {
    it('should skip disabled SQL triggers', async () => {
      const trigger = createSQLTrigger('disabled_trigger', {
        timing: 'BEFORE',
        event: 'INSERT',
        body: "RAISE(ABORT, 'Should not execute')",
      });

      // Register and then disable
      registry.register(trigger);
      registry.disable('disabled_trigger');

      const result = await executor.executeBefore<TestRow>(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      expect(result.proceed).toBe(true);
    });
  });

  describe('SQL Executor Integration', () => {
    it('should call sqlExecutor when provided', async () => {
      const sqlExecutor = vi.fn().mockResolvedValue([]);
      executor = createSQLTriggerExecutor({ registry, db, sqlExecutor });

      registry.register(createSQLTrigger('with_executor', {
        timing: 'BEFORE',
        event: 'INSERT',
        body: 'INSERT INTO audit VALUES (NEW.id)',
      }));

      await executor.executeBefore<TestRow>(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      expect(sqlExecutor).toHaveBeenCalledWith(
        'INSERT INTO audit VALUES (NEW.id)',
        expect.objectContaining({
          'NEW.id': 1,
          'NEW.name': 'Alice',
          'NEW.email': 'alice@example.com',
          'NEW.status': 'active',
        })
      );
    });

    it('should handle sqlExecutor errors', async () => {
      const sqlExecutor = vi.fn().mockRejectedValue(new Error('SQL error'));
      executor = createSQLTriggerExecutor({ registry, db, sqlExecutor });

      registry.register(createSQLTrigger('failing_executor', {
        timing: 'BEFORE',
        event: 'INSERT',
        body: 'INSERT INTO audit VALUES (NEW.id)',
      }));

      const result = await executor.executeBefore<TestRow>(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      expect(result.proceed).toBe(false);
      expect(result.error?.message).toContain('SQL error');
    });
  });

  describe('Mixed JavaScript and SQL Triggers', () => {
    it('should execute both JavaScript and SQL triggers', async () => {
      const jsHandler = vi.fn();

      // Register JavaScript trigger
      registry.register({
        name: 'js_trigger',
        table: 'users',
        timing: 'before',
        events: ['insert'],
        handler: jsHandler,
        priority: 10,
      });

      // Register SQL trigger
      registry.register(createSQLTrigger('sql_trigger', {
        timing: 'BEFORE',
        event: 'INSERT',
        body: 'SELECT 1',
      }));

      const result = await executor.executeBefore<TestRow>(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      expect(result.proceed).toBe(true);
      expect(jsHandler).toHaveBeenCalled();
    });
  });

  describe('Execution Metadata', () => {
    it('should track SQL trigger execution details', async () => {
      registry.register(createSQLTrigger('tracked_trigger', {
        timing: 'BEFORE',
        event: 'INSERT',
        body: 'SELECT 1',
      }));

      const result = await executor.executeBefore<TestRow>(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      // The SQL trigger execution should be recorded
      const sqlExecution = result.executions.find(e => e.triggerName === 'tracked_trigger');
      expect(sqlExecution).toBeDefined();
      expect(sqlExecution?.timing).toBe('before');
      expect(sqlExecution?.event).toBe('insert');
      expect(sqlExecution?.success).toBe(true);
      expect(typeof sqlExecution?.duration).toBe('number');
    });
  });
});

// =============================================================================
// RAISE Type Behavior Tests
// =============================================================================

describe('RAISE Type Behaviors', () => {
  let registry: TriggerRegistry;
  let db: DatabaseContext;
  let executor: SQLTriggerExecutor;

  beforeEach(() => {
    registry = createTriggerRegistry();
    db = createMockDatabaseContext();
    executor = createSQLTriggerExecutor({ registry, db });
  });

  describe('RAISE(IGNORE)', () => {
    it('should silently skip the operation', async () => {
      registry.register(createSQLTrigger('ignore_trigger', {
        timing: 'BEFORE',
        event: 'INSERT',
        body: 'RAISE(IGNORE)',
      }));

      const result = await executor.executeBefore<TestRow>(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      // IGNORE means proceed silently
      expect(result.proceed).toBe(true);
    });
  });

  describe('RAISE(ABORT)', () => {
    it('should abort with error message', async () => {
      registry.register(createSQLTrigger('abort_trigger', {
        timing: 'BEFORE',
        event: 'INSERT',
        body: "RAISE(ABORT, 'Operation aborted by trigger')",
      }));

      const result = await executor.executeBefore<TestRow>(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      expect(result.proceed).toBe(false);
      expect(result.error?.message).toContain('Operation aborted by trigger');
    });
  });

  describe('RAISE(FAIL)', () => {
    it('should fail with error message', async () => {
      registry.register(createSQLTrigger('fail_trigger', {
        timing: 'BEFORE',
        event: 'INSERT',
        body: "RAISE(FAIL, 'Validation failed')",
      }));

      const result = await executor.executeBefore<TestRow>(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      expect(result.proceed).toBe(false);
      expect(result.error?.message).toContain('Validation failed');
    });
  });

  describe('RAISE(ROLLBACK)', () => {
    it('should rollback with error message', async () => {
      registry.register(createSQLTrigger('rollback_trigger', {
        timing: 'BEFORE',
        event: 'INSERT',
        body: "RAISE(ROLLBACK, 'Transaction rolled back')",
      }));

      const result = await executor.executeBefore<TestRow>(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Alice', email: 'alice@example.com', status: 'active' }
      );

      expect(result.proceed).toBe(false);
      expect(result.error?.message).toContain('Transaction rolled back');
    });
  });
});
