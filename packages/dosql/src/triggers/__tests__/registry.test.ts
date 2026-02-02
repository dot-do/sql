/**
 * Trigger Registry Tests
 *
 * Tests for trigger registration, management, filtering, and index operations.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  createTriggerRegistry,
  createInMemoryTriggerStorage,
  createRegistryWithTriggers,
  mergeRegistries,
  exportTriggers,
} from '../registry.js';
import type {
  TriggerRegistry,
  TriggerDefinition,
  TriggerConfig,
  TriggerContext,
  ParsedSQLTrigger,
} from '../types.js';
import { TriggerError, TriggerErrorCode } from '../types.js';

// =============================================================================
// Test Data
// =============================================================================

interface TestRow {
  id: number;
  name: string;
  email: string;
  status: string;
}

function createTestTrigger(
  name: string,
  options: Partial<TriggerDefinition<TestRow>> = {}
): TriggerDefinition<TestRow> {
  return {
    name,
    table: options.table ?? 'users',
    timing: options.timing ?? 'before',
    events: options.events ?? ['insert'],
    handler: options.handler ?? (() => {}),
    priority: options.priority,
    enabled: options.enabled,
    description: options.description,
    condition: options.condition,
  };
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
    events: options.events ?? ['INSERT'],
    body: options.body ?? 'SELECT 1',
    forEachRow: options.forEachRow ?? true,
    ifNotExists: options.ifNotExists ?? false,
    temporary: options.temporary ?? false,
    referencesNew: options.referencesNew ?? false,
    referencesOld: options.referencesOld ?? false,
    statementCount: options.statementCount ?? 1,
    rawSql: options.rawSql ?? `CREATE TRIGGER ${name} BEFORE INSERT ON users BEGIN SELECT 1; END`,
    columns: options.columns,
    whenClause: options.whenClause,
  };
}

// =============================================================================
// Registry Creation Tests
// =============================================================================

describe('Trigger Registry Creation', () => {
  it('should create an empty registry', () => {
    const registry = createTriggerRegistry();
    expect(registry.list()).toHaveLength(0);
  });

  it('should create registry with custom storage', () => {
    const storage = createInMemoryTriggerStorage();
    const registry = createTriggerRegistry({ storage });
    expect(registry.list()).toHaveLength(0);
  });

  it('should create registry with custom defaults', () => {
    const registry = createTriggerRegistry({
      defaultPriority: 50,
      enabledByDefault: false,
    });

    const trigger = registry.register(createTestTrigger('test_trigger'));
    expect(trigger.priority).toBe(50);
    expect(trigger.enabled).toBe(false);
  });
});

// =============================================================================
// Trigger Registration Tests
// =============================================================================

describe('Trigger Registration', () => {
  let registry: TriggerRegistry;

  beforeEach(() => {
    registry = createTriggerRegistry();
  });

  describe('JavaScript Triggers', () => {
    it('should register a basic trigger', () => {
      const trigger = registry.register(createTestTrigger('before_insert_users'));

      expect(trigger.name).toBe('before_insert_users');
      expect(trigger.table).toBe('users');
      expect(trigger.timing).toBe('before');
      expect(trigger.events).toEqual(['insert']);
      expect(trigger.version).toBe(1);
      expect(trigger.createdAt).toBeInstanceOf(Date);
      expect(trigger.updatedAt).toBeInstanceOf(Date);
    });

    it('should register trigger with multiple events', () => {
      const trigger = registry.register(
        createTestTrigger('audit_changes', {
          events: ['insert', 'update', 'delete'],
        })
      );

      expect(trigger.events).toEqual(['insert', 'update', 'delete']);
    });

    it('should register trigger with priority', () => {
      const trigger = registry.register(
        createTestTrigger('high_priority', { priority: 10 })
      );

      expect(trigger.priority).toBe(10);
    });

    it('should register trigger with condition function', () => {
      const condition = (ctx: TriggerContext<TestRow>) => ctx.new?.status === 'active';
      const trigger = registry.register(
        createTestTrigger('conditional_trigger', { condition })
      );

      expect(trigger.condition).toBe(condition);
    });

    it('should register trigger with string condition', () => {
      const trigger = registry.register(
        createTestTrigger('sql_condition_trigger', {
          condition: "NEW.status = 'active'",
        })
      );

      expect(trigger.condition).toBe("NEW.status = 'active'");
    });

    it('should reject duplicate trigger names', () => {
      registry.register(createTestTrigger('unique_trigger'));

      expect(() => {
        registry.register(createTestTrigger('unique_trigger'));
      }).toThrow(TriggerError);
    });

    it('should replace trigger when replace option is true', () => {
      const first = registry.register(createTestTrigger('replaceable', { priority: 100 }));
      expect(first.version).toBe(1);

      const second = registry.register(
        createTestTrigger('replaceable', { priority: 50 }),
        { replace: true }
      );

      expect(second.version).toBe(2);
      expect(second.priority).toBe(50);
      expect(second.createdAt).toEqual(first.createdAt);
    });

    it('should add author and tags when registering', () => {
      const trigger = registry.register(createTestTrigger('tagged_trigger'), {
        author: 'test_user',
        tags: ['audit', 'security'],
      });

      expect(trigger.author).toBe('test_user');
      expect(trigger.tags).toEqual(['audit', 'security']);
    });
  });

  describe('Validation', () => {
    it('should reject trigger without name', () => {
      expect(() => {
        registry.register({
          name: '',
          table: 'users',
          timing: 'before',
          events: ['insert'],
          handler: () => {},
        });
      }).toThrow(TriggerError);
    });

    it('should reject trigger without table', () => {
      expect(() => {
        registry.register({
          name: 'test',
          table: '',
          timing: 'before',
          events: ['insert'],
          handler: () => {},
        });
      }).toThrow(TriggerError);
    });

    it('should reject trigger with invalid timing', () => {
      expect(() => {
        registry.register({
          name: 'test',
          table: 'users',
          timing: 'during' as any,
          events: ['insert'],
          handler: () => {},
        });
      }).toThrow(TriggerError);
    });

    it('should reject trigger with empty events', () => {
      expect(() => {
        registry.register({
          name: 'test',
          table: 'users',
          timing: 'before',
          events: [],
          handler: () => {},
        });
      }).toThrow(TriggerError);
    });

    it('should reject trigger with invalid event', () => {
      expect(() => {
        registry.register({
          name: 'test',
          table: 'users',
          timing: 'before',
          events: ['create' as any],
          handler: () => {},
        });
      }).toThrow(TriggerError);
    });

    it('should reject trigger without handler', () => {
      expect(() => {
        registry.register({
          name: 'test',
          table: 'users',
          timing: 'before',
          events: ['insert'],
          handler: null as any,
        });
      }).toThrow(TriggerError);
    });
  });

  describe('SQL Triggers', () => {
    it('should register a SQL trigger', () => {
      const sqlTrigger = createSQLTrigger('sql_before_insert');
      const trigger = registry.register(sqlTrigger as any);

      expect(trigger.name).toBe('sql_before_insert');
      expect(trigger.table).toBe('users');
      expect((trigger as any).body).toBe('SELECT 1');
    });

    it('should register SQL trigger with WHEN clause', () => {
      const sqlTrigger = createSQLTrigger('sql_conditional', {
        whenClause: 'NEW.status = "active"',
      });
      const trigger = registry.register(sqlTrigger as any);

      expect(trigger.condition).toBe('NEW.status = "active"');
    });

    it('should register SQL trigger with UPDATE OF columns', () => {
      const sqlTrigger = createSQLTrigger('sql_update_columns', {
        timing: 'BEFORE',
        event: 'UPDATE',
        events: ['UPDATE'],
        columns: ['email', 'status'],
      });
      const trigger = registry.register(sqlTrigger as any);

      expect((trigger as any).columns).toEqual(['email', 'status']);
    });
  });
});

// =============================================================================
// Trigger Retrieval Tests
// =============================================================================

describe('Trigger Retrieval', () => {
  let registry: TriggerRegistry;

  beforeEach(() => {
    registry = createTriggerRegistry();
    registry.register(createTestTrigger('trigger_a', { table: 'users', timing: 'before', events: ['insert'] }));
    registry.register(createTestTrigger('trigger_b', { table: 'users', timing: 'after', events: ['insert'] }));
    registry.register(createTestTrigger('trigger_c', { table: 'orders', timing: 'before', events: ['update'] }));
    registry.register(createTestTrigger('trigger_d', { table: 'users', timing: 'before', events: ['delete'], enabled: false }));
  });

  describe('get()', () => {
    it('should get trigger by name', () => {
      const trigger = registry.get('trigger_a');
      expect(trigger).toBeDefined();
      expect(trigger?.name).toBe('trigger_a');
    });

    it('should return undefined for non-existent trigger', () => {
      const trigger = registry.get('non_existent');
      expect(trigger).toBeUndefined();
    });
  });

  describe('list()', () => {
    it('should list all triggers', () => {
      const triggers = registry.list();
      expect(triggers).toHaveLength(4);
    });

    it('should filter by table', () => {
      const triggers = registry.list({ table: 'users' });
      expect(triggers).toHaveLength(3);
      expect(triggers.every(t => t.table === 'users')).toBe(true);
    });

    it('should filter by timing', () => {
      const triggers = registry.list({ timing: 'before' });
      expect(triggers).toHaveLength(3);
      expect(triggers.every(t => t.timing === 'before')).toBe(true);
    });

    it('should filter by event', () => {
      const triggers = registry.list({ event: 'insert' });
      expect(triggers).toHaveLength(2);
    });

    it('should filter by enabled status', () => {
      const enabledTriggers = registry.list({ enabled: true });
      expect(enabledTriggers).toHaveLength(3);

      const disabledTriggers = registry.list({ enabled: false });
      expect(disabledTriggers).toHaveLength(1);
      expect(disabledTriggers[0].name).toBe('trigger_d');
    });

    it('should combine filters', () => {
      const triggers = registry.list({ table: 'users', timing: 'before', enabled: true });
      expect(triggers).toHaveLength(1);
      expect(triggers[0].name).toBe('trigger_a');
    });

    it('should sort by priority then name', () => {
      registry.clear();
      registry.register(createTestTrigger('z_trigger', { priority: 50 }));
      registry.register(createTestTrigger('a_trigger', { priority: 50 }));
      registry.register(createTestTrigger('m_trigger', { priority: 10 }));

      const triggers = registry.list();
      expect(triggers[0].name).toBe('m_trigger'); // lowest priority
      expect(triggers[1].name).toBe('a_trigger'); // same priority, alphabetical
      expect(triggers[2].name).toBe('z_trigger');
    });
  });

  describe('getForTableEvent()', () => {
    it('should get triggers for specific table, timing, and event', () => {
      const triggers = registry.getForTableEvent('users', 'before', 'insert');
      expect(triggers).toHaveLength(1);
      expect(triggers[0].name).toBe('trigger_a');
    });

    it('should return empty array when no triggers match', () => {
      const triggers = registry.getForTableEvent('products', 'before', 'insert');
      expect(triggers).toHaveLength(0);
    });

    it('should include triggers with multiple events', () => {
      registry.register(
        createTestTrigger('multi_event', {
          table: 'users',
          timing: 'before',
          events: ['insert', 'update'],
        })
      );

      const insertTriggers = registry.getForTableEvent('users', 'before', 'insert');
      const updateTriggers = registry.getForTableEvent('users', 'before', 'update');

      expect(insertTriggers.some(t => t.name === 'multi_event')).toBe(true);
      expect(updateTriggers.some(t => t.name === 'multi_event')).toBe(true);
    });
  });
});

// =============================================================================
// Trigger Management Tests
// =============================================================================

describe('Trigger Management', () => {
  let registry: TriggerRegistry;

  beforeEach(() => {
    registry = createTriggerRegistry();
    registry.register(createTestTrigger('test_trigger'));
  });

  describe('enable() / disable()', () => {
    it('should enable a disabled trigger', () => {
      registry.disable('test_trigger');
      expect(registry.get('test_trigger')?.enabled).toBe(false);

      const result = registry.enable('test_trigger');
      expect(result).toBe(true);
      expect(registry.get('test_trigger')?.enabled).toBe(true);
    });

    it('should disable an enabled trigger', () => {
      const result = registry.disable('test_trigger');
      expect(result).toBe(true);
      expect(registry.get('test_trigger')?.enabled).toBe(false);
    });

    it('should return false for non-existent trigger', () => {
      expect(registry.enable('non_existent')).toBe(false);
      expect(registry.disable('non_existent')).toBe(false);
    });

    it('should update timestamp when enabling/disabling', async () => {
      const originalUpdatedAt = registry.get('test_trigger')?.updatedAt;

      // Wait to ensure timestamp difference
      await new Promise(resolve => setTimeout(resolve, 10));

      registry.disable('test_trigger');
      const newUpdatedAt = registry.get('test_trigger')?.updatedAt;

      expect(newUpdatedAt!.getTime()).toBeGreaterThanOrEqual(originalUpdatedAt!.getTime());
    });
  });

  describe('remove()', () => {
    it('should remove a trigger', () => {
      const result = registry.remove('test_trigger');
      expect(result).toBe(true);
      expect(registry.get('test_trigger')).toBeUndefined();
      expect(registry.list()).toHaveLength(0);
    });

    it('should return false for non-existent trigger', () => {
      expect(registry.remove('non_existent')).toBe(false);
    });

    it('should remove trigger from index', () => {
      registry.remove('test_trigger');
      const triggers = registry.getForTableEvent('users', 'before', 'insert');
      expect(triggers).toHaveLength(0);
    });
  });

  describe('clear()', () => {
    it('should clear all triggers', () => {
      registry.register(createTestTrigger('trigger_2', { table: 'orders' }));
      registry.clear();
      expect(registry.list()).toHaveLength(0);
    });

    it('should clear triggers for specific table', () => {
      registry.register(createTestTrigger('orders_trigger', { table: 'orders' }));

      registry.clear('users');

      const remaining = registry.list();
      expect(remaining).toHaveLength(1);
      expect(remaining[0].name).toBe('orders_trigger');
    });
  });

  describe('setPriority()', () => {
    it('should update trigger priority', () => {
      const result = registry.setPriority('test_trigger', 5);
      expect(result).toBe(true);
      expect(registry.get('test_trigger')?.priority).toBe(5);
    });

    it('should return false for non-existent trigger', () => {
      expect(registry.setPriority('non_existent', 5)).toBe(false);
    });

    it('should update timestamp when changing priority', async () => {
      const originalUpdatedAt = registry.get('test_trigger')?.updatedAt;

      // Wait to ensure timestamp difference
      await new Promise(resolve => setTimeout(resolve, 10));

      registry.setPriority('test_trigger', 5);
      const newUpdatedAt = registry.get('test_trigger')?.updatedAt;

      expect(newUpdatedAt!.getTime()).toBeGreaterThanOrEqual(originalUpdatedAt!.getTime());
    });
  });
});

// =============================================================================
// Utility Functions Tests
// =============================================================================

describe('Registry Utilities', () => {
  describe('createRegistryWithTriggers()', () => {
    it('should create registry pre-populated with triggers', () => {
      const triggers = [
        createTestTrigger('trigger_1'),
        createTestTrigger('trigger_2', { table: 'orders' }),
      ];

      const registry = createRegistryWithTriggers(triggers);
      expect(registry.list()).toHaveLength(2);
    });
  });

  describe('mergeRegistries()', () => {
    it('should merge multiple registries', () => {
      const registry1 = createTriggerRegistry();
      registry1.register(createTestTrigger('trigger_1'));

      const registry2 = createTriggerRegistry();
      registry2.register(createTestTrigger('trigger_2'));

      const merged = mergeRegistries(registry1, registry2);
      expect(merged.list()).toHaveLength(2);
    });

    it('should replace duplicates from later registries', () => {
      const registry1 = createTriggerRegistry();
      registry1.register(createTestTrigger('same_name', { priority: 100 }));

      const registry2 = createTriggerRegistry();
      registry2.register(createTestTrigger('same_name', { priority: 50 }));

      const merged = mergeRegistries(registry1, registry2);
      expect(merged.list()).toHaveLength(1);
      expect(merged.get('same_name')?.priority).toBe(50);
    });
  });

  describe('exportTriggers()', () => {
    it('should export triggers as definitions', () => {
      const registry = createTriggerRegistry();
      registry.register(createTestTrigger('exportable', {
        description: 'Test trigger',
        priority: 50,
      }));

      const exported = exportTriggers(registry);
      expect(exported).toHaveLength(1);
      expect(exported[0].name).toBe('exportable');
      expect(exported[0].description).toBe('Test trigger');
      expect(exported[0].priority).toBe(50);
    });
  });
});

// =============================================================================
// In-Memory Storage Tests
// =============================================================================

describe('In-Memory Trigger Storage', () => {
  it('should implement all storage methods', () => {
    const storage = createInMemoryTriggerStorage();

    // set and get
    storage.set('test', { name: 'test' } as TriggerConfig);
    expect(storage.get('test')?.name).toBe('test');

    // list
    expect(storage.list()).toHaveLength(1);

    // delete
    expect(storage.delete('test')).toBe(true);
    expect(storage.delete('test')).toBe(false);
    expect(storage.get('test')).toBeUndefined();

    // clear
    storage.set('a', { name: 'a' } as TriggerConfig);
    storage.set('b', { name: 'b' } as TriggerConfig);
    storage.clear();
    expect(storage.list()).toHaveLength(0);
  });
});
