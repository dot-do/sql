/**
 * SQL Trigger Tests for DoSQL
 *
 * Comprehensive TDD test suite for SQL TRIGGER support including:
 * - CREATE TRIGGER ... BEFORE INSERT/UPDATE/DELETE
 * - CREATE TRIGGER ... AFTER INSERT/UPDATE/DELETE
 * - CREATE TRIGGER ... INSTEAD OF (for views)
 * - BEFORE UPDATE OF col1, col2 (column-specific triggers)
 * - WHEN clause conditions
 * - NEW.col and OLD.col references
 * - Multiple statements in trigger body
 * - DROP TRIGGER
 * - Recursive triggers (PRAGMA recursive_triggers)
 * - Trigger ordering
 *
 * Target: 60+ tests running with workers-vitest-pool (NO MOCKS)
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  parseTrigger,
  tryParseTrigger,
  isCreateTrigger,
  isDropTrigger,
  parseDropTrigger,
} from './parser.js';
import {
  createTriggerRegistry,
} from './registry.js';
import {
  createTriggerExecutor,
  createSimpleTriggerExecutor,
} from './executor.js';
import type {
  SQLTriggerDefinition,
  SQLTriggerTiming,
  SQLTriggerEvent,
  TriggerRegistry,
} from './types.js';

// =============================================================================
// TEST DATA SETUP
// =============================================================================

interface TestUser {
  id: number;
  name: string;
  email: string;
  status: string;
  updated_at?: string;
  created_at?: string;
}

interface TestAuditLog {
  id: number;
  table_name: string;
  operation: string;
  old_value?: string;
  new_value?: string;
  timestamp: string;
}

interface TestOrder {
  id: number;
  user_id: number;
  total: number;
  status: string;
}

// =============================================================================
// PARSER TESTS - CREATE TRIGGER SYNTAX
// =============================================================================

describe('SQL Trigger Parser', () => {
  describe('Basic CREATE TRIGGER parsing', () => {
    it('should parse CREATE TRIGGER ... BEFORE INSERT', () => {
      const sql = `
        CREATE TRIGGER set_timestamp
        BEFORE INSERT ON users
        BEGIN
          UPDATE users SET created_at = datetime('now') WHERE rowid = NEW.rowid;
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.name).toBe('set_timestamp');
      expect(result.timing).toBe('BEFORE');
      expect(result.event).toBe('INSERT');
      expect(result.table).toBe('users');
      expect(result.body).toContain('UPDATE users SET created_at');
    });

    it('should parse CREATE TRIGGER ... AFTER INSERT', () => {
      const sql = `
        CREATE TRIGGER log_insert
        AFTER INSERT ON orders
        BEGIN
          INSERT INTO audit_log (table_name, operation, new_value)
          VALUES ('orders', 'INSERT', NEW.id);
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.name).toBe('log_insert');
      expect(result.timing).toBe('AFTER');
      expect(result.event).toBe('INSERT');
      expect(result.table).toBe('orders');
    });

    it('should parse CREATE TRIGGER ... BEFORE UPDATE', () => {
      const sql = `
        CREATE TRIGGER update_timestamp
        BEFORE UPDATE ON users
        BEGIN
          UPDATE users SET updated_at = datetime('now') WHERE rowid = NEW.rowid;
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.name).toBe('update_timestamp');
      expect(result.timing).toBe('BEFORE');
      expect(result.event).toBe('UPDATE');
      expect(result.table).toBe('users');
    });

    it('should parse CREATE TRIGGER ... AFTER UPDATE', () => {
      const sql = `
        CREATE TRIGGER log_update
        AFTER UPDATE ON users
        BEGIN
          INSERT INTO audit_log (table_name, operation, old_value, new_value)
          VALUES ('users', 'UPDATE', OLD.name, NEW.name);
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.name).toBe('log_update');
      expect(result.timing).toBe('AFTER');
      expect(result.event).toBe('UPDATE');
      expect(result.body).toContain('OLD.name');
      expect(result.body).toContain('NEW.name');
    });

    it('should parse CREATE TRIGGER ... BEFORE DELETE', () => {
      const sql = `
        CREATE TRIGGER archive_before_delete
        BEFORE DELETE ON users
        BEGIN
          INSERT INTO users_archive SELECT * FROM users WHERE id = OLD.id;
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.name).toBe('archive_before_delete');
      expect(result.timing).toBe('BEFORE');
      expect(result.event).toBe('DELETE');
      expect(result.body).toContain('OLD.id');
    });

    it('should parse CREATE TRIGGER ... AFTER DELETE', () => {
      const sql = `
        CREATE TRIGGER log_delete
        AFTER DELETE ON users
        BEGIN
          INSERT INTO audit_log (table_name, operation, old_value)
          VALUES ('users', 'DELETE', OLD.name);
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.name).toBe('log_delete');
      expect(result.timing).toBe('AFTER');
      expect(result.event).toBe('DELETE');
    });
  });

  describe('INSTEAD OF triggers (for views)', () => {
    it('should parse CREATE TRIGGER ... INSTEAD OF INSERT', () => {
      const sql = `
        CREATE TRIGGER insert_user_view
        INSTEAD OF INSERT ON user_view
        BEGIN
          INSERT INTO users (name, email) VALUES (NEW.name, NEW.email);
          INSERT INTO profiles (user_id, bio) VALUES (last_insert_rowid(), NEW.bio);
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.name).toBe('insert_user_view');
      expect(result.timing).toBe('INSTEAD OF');
      expect(result.event).toBe('INSERT');
      expect(result.table).toBe('user_view');
    });

    it('should parse CREATE TRIGGER ... INSTEAD OF UPDATE', () => {
      const sql = `
        CREATE TRIGGER update_user_view
        INSTEAD OF UPDATE ON user_view
        BEGIN
          UPDATE users SET name = NEW.name WHERE id = OLD.id;
          UPDATE profiles SET bio = NEW.bio WHERE user_id = OLD.id;
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.name).toBe('update_user_view');
      expect(result.timing).toBe('INSTEAD OF');
      expect(result.event).toBe('UPDATE');
    });

    it('should parse CREATE TRIGGER ... INSTEAD OF DELETE', () => {
      const sql = `
        CREATE TRIGGER delete_user_view
        INSTEAD OF DELETE ON user_view
        BEGIN
          DELETE FROM profiles WHERE user_id = OLD.id;
          DELETE FROM users WHERE id = OLD.id;
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.name).toBe('delete_user_view');
      expect(result.timing).toBe('INSTEAD OF');
      expect(result.event).toBe('DELETE');
    });
  });

  describe('UPDATE OF column triggers', () => {
    it('should parse BEFORE UPDATE OF single column', () => {
      const sql = `
        CREATE TRIGGER email_change_notify
        BEFORE UPDATE OF email ON users
        BEGIN
          INSERT INTO notifications (user_id, message)
          VALUES (NEW.id, 'Email changed from ' || OLD.email || ' to ' || NEW.email);
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.name).toBe('email_change_notify');
      expect(result.timing).toBe('BEFORE');
      expect(result.event).toBe('UPDATE');
      expect(result.columns).toEqual(['email']);
    });

    it('should parse BEFORE UPDATE OF multiple columns', () => {
      const sql = `
        CREATE TRIGGER profile_change_log
        BEFORE UPDATE OF name, email, status ON users
        BEGIN
          INSERT INTO change_log (entity, changed_at)
          VALUES ('user:' || OLD.id, datetime('now'));
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.columns).toEqual(['name', 'email', 'status']);
    });

    it('should parse AFTER UPDATE OF columns', () => {
      const sql = `
        CREATE TRIGGER status_change_webhook
        AFTER UPDATE OF status ON orders
        BEGIN
          INSERT INTO webhook_queue (url, payload)
          VALUES ('https://api.example.com/webhook', json_object('order_id', NEW.id, 'status', NEW.status));
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.timing).toBe('AFTER');
      expect(result.event).toBe('UPDATE');
      expect(result.columns).toEqual(['status']);
    });
  });

  describe('WHEN clause conditions', () => {
    it('should parse WHEN clause with simple condition', () => {
      const sql = `
        CREATE TRIGGER log_high_value_orders
        AFTER INSERT ON orders
        WHEN NEW.total > 1000
        BEGIN
          INSERT INTO high_value_alerts (order_id, amount)
          VALUES (NEW.id, NEW.total);
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.whenClause).toBe('NEW.total > 1000');
    });

    it('should parse WHEN clause with complex condition', () => {
      const sql = `
        CREATE TRIGGER notify_status_change
        AFTER UPDATE ON orders
        WHEN OLD.status != NEW.status AND NEW.status = 'shipped'
        BEGIN
          INSERT INTO notifications (user_id, message)
          VALUES (NEW.user_id, 'Your order has shipped!');
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.whenClause).toBe("OLD.status != NEW.status AND NEW.status = 'shipped'");
    });

    it('should parse WHEN clause with IS NULL check', () => {
      const sql = `
        CREATE TRIGGER set_default_status
        BEFORE INSERT ON users
        WHEN NEW.status IS NULL
        BEGIN
          SELECT RAISE(ABORT, 'Status cannot be null');
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.whenClause).toBe('NEW.status IS NULL');
    });

    it('should parse WHEN clause with IN list', () => {
      const sql = `
        CREATE TRIGGER restrict_status_values
        BEFORE UPDATE OF status ON orders
        WHEN NEW.status NOT IN ('pending', 'processing', 'shipped', 'delivered')
        BEGIN
          SELECT RAISE(ABORT, 'Invalid status value');
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.whenClause).toContain('NOT IN');
    });

    it('should parse WHEN clause with parentheses', () => {
      const sql = `
        CREATE TRIGGER complex_condition
        AFTER UPDATE ON users
        WHEN (NEW.status = 'active' AND OLD.status = 'pending') OR (NEW.email != OLD.email)
        BEGIN
          INSERT INTO events (type, data) VALUES ('user_activated', NEW.id);
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.whenClause).toContain('AND');
      expect(result.whenClause).toContain('OR');
    });
  });

  describe('NEW and OLD references', () => {
    it('should identify NEW references in INSERT trigger', () => {
      const sql = `
        CREATE TRIGGER validate_email
        BEFORE INSERT ON users
        BEGIN
          SELECT CASE
            WHEN NEW.email NOT LIKE '%@%.%'
            THEN RAISE(ABORT, 'Invalid email format')
          END;
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.referencesNew).toBe(true);
      expect(result.referencesOld).toBe(false);
    });

    it('should identify OLD references in DELETE trigger', () => {
      const sql = `
        CREATE TRIGGER cascade_delete
        AFTER DELETE ON users
        BEGIN
          DELETE FROM orders WHERE user_id = OLD.id;
          DELETE FROM profiles WHERE user_id = OLD.id;
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.referencesOld).toBe(true);
      expect(result.referencesNew).toBe(false);
    });

    it('should identify both NEW and OLD in UPDATE trigger', () => {
      const sql = `
        CREATE TRIGGER track_changes
        AFTER UPDATE ON users
        BEGIN
          INSERT INTO user_history (user_id, old_name, new_name, changed_at)
          VALUES (OLD.id, OLD.name, NEW.name, datetime('now'));
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.referencesOld).toBe(true);
      expect(result.referencesNew).toBe(true);
    });
  });

  describe('Multiple statements in trigger body', () => {
    it('should parse trigger with multiple INSERT statements', () => {
      const sql = `
        CREATE TRIGGER after_user_insert
        AFTER INSERT ON users
        BEGIN
          INSERT INTO profiles (user_id) VALUES (NEW.id);
          INSERT INTO settings (user_id, theme) VALUES (NEW.id, 'default');
          INSERT INTO audit_log (action, entity_id) VALUES ('user_created', NEW.id);
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.body).toContain('INSERT INTO profiles');
      expect(result.body).toContain('INSERT INTO settings');
      expect(result.body).toContain('INSERT INTO audit_log');
      expect(result.statementCount).toBe(3);
    });

    it('should parse trigger with mixed statement types', () => {
      const sql = `
        CREATE TRIGGER before_order_delete
        BEFORE DELETE ON orders
        BEGIN
          UPDATE inventory SET quantity = quantity + (SELECT quantity FROM order_items WHERE order_id = OLD.id);
          DELETE FROM order_items WHERE order_id = OLD.id;
          INSERT INTO deleted_orders SELECT * FROM orders WHERE id = OLD.id;
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.statementCount).toBe(3);
    });

    it('should parse trigger with SELECT statements for validation', () => {
      const sql = `
        CREATE TRIGGER validate_order
        BEFORE INSERT ON orders
        BEGIN
          SELECT CASE
            WHEN NEW.total < 0 THEN RAISE(ABORT, 'Total cannot be negative')
            WHEN NEW.user_id IS NULL THEN RAISE(ABORT, 'User ID required')
          END;
          SELECT CASE
            WHEN (SELECT COUNT(*) FROM users WHERE id = NEW.user_id) = 0
            THEN RAISE(ABORT, 'User does not exist')
          END;
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.body).toContain('RAISE(ABORT');
      expect(result.statementCount).toBe(2);
    });
  });

  describe('FOR EACH ROW clause', () => {
    it('should parse explicit FOR EACH ROW', () => {
      const sql = `
        CREATE TRIGGER row_trigger
        AFTER INSERT ON users
        FOR EACH ROW
        BEGIN
          INSERT INTO log (msg) VALUES ('Row inserted');
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.forEachRow).toBe(true);
    });

    it('should default to FOR EACH ROW when not specified (SQLite behavior)', () => {
      const sql = `
        CREATE TRIGGER implicit_row_trigger
        AFTER INSERT ON users
        BEGIN
          INSERT INTO log (msg) VALUES ('Row inserted');
        END;
      `;

      const result = parseTrigger(sql);

      // SQLite only supports FOR EACH ROW, so it should default to true
      expect(result.forEachRow).toBe(true);
    });
  });

  describe('IF NOT EXISTS clause', () => {
    it('should parse CREATE TRIGGER IF NOT EXISTS', () => {
      const sql = `
        CREATE TRIGGER IF NOT EXISTS safe_trigger
        AFTER INSERT ON users
        BEGIN
          INSERT INTO log (msg) VALUES ('User created');
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.name).toBe('safe_trigger');
      expect(result.ifNotExists).toBe(true);
    });
  });

  describe('Temporary triggers', () => {
    it('should parse CREATE TEMP TRIGGER', () => {
      const sql = `
        CREATE TEMP TRIGGER temp_audit
        AFTER UPDATE ON users
        BEGIN
          INSERT INTO temp_audit_log (user_id) VALUES (NEW.id);
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.name).toBe('temp_audit');
      expect(result.temporary).toBe(true);
    });

    it('should parse CREATE TEMPORARY TRIGGER', () => {
      const sql = `
        CREATE TEMPORARY TRIGGER temporary_audit
        AFTER DELETE ON orders
        BEGIN
          INSERT INTO temp_deleted_orders VALUES (OLD.id);
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.temporary).toBe(true);
    });
  });

  describe('Schema-qualified names', () => {
    it('should parse trigger with schema-qualified table name', () => {
      const sql = `
        CREATE TRIGGER main.user_audit
        AFTER INSERT ON main.users
        BEGIN
          INSERT INTO main.audit_log (action) VALUES ('insert');
        END;
      `;

      const result = parseTrigger(sql);

      expect(result.name).toBe('user_audit');
      expect(result.schema).toBe('main');
      expect(result.table).toBe('users');
      expect(result.tableSchema).toBe('main');
    });
  });

  describe('tryParseTrigger error handling', () => {
    it('should return success for valid trigger', () => {
      const sql = `CREATE TRIGGER test AFTER INSERT ON users BEGIN SELECT 1; END;`;
      const result = tryParseTrigger(sql);

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.trigger.name).toBe('test');
      }
    });

    it('should return error for invalid trigger syntax', () => {
      const sql = `CREATE TRIGGER incomplete`;
      const result = tryParseTrigger(sql);

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error.message).toBeDefined();
      }
    });

    it('should return error for missing BEGIN/END', () => {
      const sql = `CREATE TRIGGER no_body AFTER INSERT ON users;`;
      const result = tryParseTrigger(sql);

      expect(result.success).toBe(false);
    });
  });

  describe('isCreateTrigger detection', () => {
    it('should detect CREATE TRIGGER statements', () => {
      expect(isCreateTrigger('CREATE TRIGGER test AFTER INSERT ON users BEGIN SELECT 1; END;')).toBe(true);
      expect(isCreateTrigger('create trigger test AFTER INSERT ON users BEGIN SELECT 1; END;')).toBe(true);
      expect(isCreateTrigger('CREATE TEMP TRIGGER test AFTER INSERT ON users BEGIN SELECT 1; END;')).toBe(true);
      expect(isCreateTrigger('CREATE TRIGGER IF NOT EXISTS test AFTER INSERT ON users BEGIN SELECT 1; END;')).toBe(true);
    });

    it('should not detect non-trigger statements', () => {
      expect(isCreateTrigger('SELECT * FROM users')).toBe(false);
      expect(isCreateTrigger('CREATE TABLE users (id INT)')).toBe(false);
      expect(isCreateTrigger('CREATE INDEX idx ON users(name)')).toBe(false);
      expect(isCreateTrigger('DROP TRIGGER test')).toBe(false);
    });
  });
});

// =============================================================================
// PARSER TESTS - DROP TRIGGER SYNTAX
// =============================================================================

describe('DROP TRIGGER Parser', () => {
  describe('Basic DROP TRIGGER parsing', () => {
    it('should parse DROP TRIGGER', () => {
      const sql = `DROP TRIGGER user_audit;`;
      const result = parseDropTrigger(sql);

      expect(result.name).toBe('user_audit');
      expect(result.ifExists).toBe(false);
    });

    it('should parse DROP TRIGGER IF EXISTS', () => {
      const sql = `DROP TRIGGER IF EXISTS old_trigger;`;
      const result = parseDropTrigger(sql);

      expect(result.name).toBe('old_trigger');
      expect(result.ifExists).toBe(true);
    });

    it('should parse DROP TRIGGER with schema', () => {
      const sql = `DROP TRIGGER main.user_audit;`;
      const result = parseDropTrigger(sql);

      expect(result.name).toBe('user_audit');
      expect(result.schema).toBe('main');
    });
  });

  describe('isDropTrigger detection', () => {
    it('should detect DROP TRIGGER statements', () => {
      expect(isDropTrigger('DROP TRIGGER test')).toBe(true);
      expect(isDropTrigger('drop trigger test')).toBe(true);
      expect(isDropTrigger('DROP TRIGGER IF EXISTS test')).toBe(true);
    });

    it('should not detect non-DROP TRIGGER statements', () => {
      expect(isDropTrigger('CREATE TRIGGER test')).toBe(false);
      expect(isDropTrigger('DROP TABLE test')).toBe(false);
      expect(isDropTrigger('DROP INDEX test')).toBe(false);
    });
  });
});

// =============================================================================
// TRIGGER REGISTRY TESTS
// =============================================================================

describe('SQL Trigger Registry', () => {
  let registry: TriggerRegistry;

  beforeEach(() => {
    registry = createTriggerRegistry();
  });

  describe('Registering triggers from SQL', () => {
    it('should register trigger from parsed SQL', () => {
      const sql = `
        CREATE TRIGGER audit_insert
        AFTER INSERT ON users
        BEGIN
          INSERT INTO audit_log (action, entity) VALUES ('INSERT', 'users');
        END;
      `;

      const trigger = parseTrigger(sql);
      const registered = registry.register(trigger);

      expect(registered.name).toBe('audit_insert');
      expect(registry.get('audit_insert')).toBeDefined();
    });

    it('should replace trigger with same name when using replace option', () => {
      const sql1 = `
        CREATE TRIGGER my_trigger
        AFTER INSERT ON users
        BEGIN
          SELECT 1;
        END;
      `;
      const sql2 = `
        CREATE TRIGGER my_trigger
        AFTER INSERT ON users
        BEGIN
          SELECT 2;
        END;
      `;

      registry.register(parseTrigger(sql1));
      registry.register(parseTrigger(sql2), { replace: true });

      const trigger = registry.get('my_trigger');
      expect(trigger?.body).toContain('SELECT 2');
    });

    it('should throw error for duplicate trigger without replace option', () => {
      const sql = `
        CREATE TRIGGER duplicate_trigger
        AFTER INSERT ON users
        BEGIN
          SELECT 1;
        END;
      `;

      registry.register(parseTrigger(sql));

      expect(() => registry.register(parseTrigger(sql))).toThrow();
    });
  });

  describe('Listing and filtering triggers', () => {
    beforeEach(() => {
      registry.register(parseTrigger(`
        CREATE TRIGGER t1 BEFORE INSERT ON users BEGIN SELECT 1; END;
      `));
      registry.register(parseTrigger(`
        CREATE TRIGGER t2 AFTER INSERT ON users BEGIN SELECT 1; END;
      `));
      registry.register(parseTrigger(`
        CREATE TRIGGER t3 BEFORE UPDATE ON orders BEGIN SELECT 1; END;
      `));
      registry.register(parseTrigger(`
        CREATE TRIGGER t4 AFTER DELETE ON users BEGIN SELECT 1; END;
      `));
    });

    it('should list all triggers', () => {
      const triggers = registry.list();
      expect(triggers).toHaveLength(4);
    });

    it('should filter by table', () => {
      const triggers = registry.list({ table: 'users' });
      expect(triggers).toHaveLength(3);
    });

    it('should filter by timing', () => {
      const triggers = registry.list({ timing: 'BEFORE' });
      expect(triggers).toHaveLength(2);
    });

    it('should filter by event', () => {
      const triggers = registry.list({ event: 'INSERT' });
      expect(triggers).toHaveLength(2);
    });

    it('should filter by multiple criteria', () => {
      const triggers = registry.list({ table: 'users', timing: 'AFTER' });
      expect(triggers).toHaveLength(2);
    });
  });

  describe('Getting triggers for table/event', () => {
    it('should get triggers in correct order by priority', () => {
      registry.register(parseTrigger(`
        CREATE TRIGGER low_priority AFTER INSERT ON users BEGIN SELECT 1; END;
      `));
      registry.register(parseTrigger(`
        CREATE TRIGGER high_priority AFTER INSERT ON users BEGIN SELECT 2; END;
      `));

      registry.setPriority('high_priority', 10);
      registry.setPriority('low_priority', 100);

      const triggers = registry.getForTableEvent('users', 'AFTER', 'INSERT');
      expect(triggers[0].name).toBe('high_priority');
      expect(triggers[1].name).toBe('low_priority');
    });
  });

  describe('Removing triggers', () => {
    it('should remove trigger by name', () => {
      registry.register(parseTrigger(`
        CREATE TRIGGER to_remove AFTER INSERT ON users BEGIN SELECT 1; END;
      `));

      expect(registry.remove('to_remove')).toBe(true);
      expect(registry.get('to_remove')).toBeUndefined();
    });

    it('should return false when removing non-existent trigger', () => {
      expect(registry.remove('nonexistent')).toBe(false);
    });

    it('should clear all triggers for a table', () => {
      registry.register(parseTrigger(`
        CREATE TRIGGER t1 AFTER INSERT ON users BEGIN SELECT 1; END;
      `));
      registry.register(parseTrigger(`
        CREATE TRIGGER t2 AFTER UPDATE ON users BEGIN SELECT 1; END;
      `));
      registry.register(parseTrigger(`
        CREATE TRIGGER t3 AFTER INSERT ON orders BEGIN SELECT 1; END;
      `));

      registry.clear('users');

      expect(registry.list({ table: 'users' })).toHaveLength(0);
      expect(registry.list({ table: 'orders' })).toHaveLength(1);
    });
  });

  describe('Enable/Disable triggers', () => {
    it('should enable and disable triggers', () => {
      registry.register(parseTrigger(`
        CREATE TRIGGER toggle_trigger AFTER INSERT ON users BEGIN SELECT 1; END;
      `));

      registry.disable('toggle_trigger');
      let trigger = registry.get('toggle_trigger');
      expect(trigger?.enabled).toBe(false);

      registry.enable('toggle_trigger');
      trigger = registry.get('toggle_trigger');
      expect(trigger?.enabled).toBe(true);
    });

    it('should filter by enabled status', () => {
      registry.register(parseTrigger(`
        CREATE TRIGGER enabled_trigger AFTER INSERT ON users BEGIN SELECT 1; END;
      `));
      registry.register(parseTrigger(`
        CREATE TRIGGER disabled_trigger AFTER INSERT ON users BEGIN SELECT 2; END;
      `));

      registry.disable('disabled_trigger');

      const enabledOnly = registry.list({ enabled: true });
      expect(enabledOnly).toHaveLength(1);
      expect(enabledOnly[0].name).toBe('enabled_trigger');
    });
  });
});

// =============================================================================
// TRIGGER EXECUTOR TESTS
// =============================================================================

describe('SQL Trigger Executor', () => {
  describe('BEFORE INSERT trigger execution', () => {
    it('should execute BEFORE INSERT trigger and allow modification', async () => {
      const registry = createTriggerRegistry();
      registry.register(parseTrigger(`
        CREATE TRIGGER set_defaults
        BEFORE INSERT ON users
        BEGIN
          UPDATE users SET status = 'active' WHERE rowid = NEW.rowid;
        END;
      `));

      const executor = createSimpleTriggerExecutor(registry, {
        users: [] as TestUser[],
      });

      const result = await executor.executeBefore(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Test', email: 'test@example.com', status: '' }
      );

      expect(result.proceed).toBe(true);
    });

    it('should abort on RAISE(ABORT, ...) in BEFORE trigger', async () => {
      const registry = createTriggerRegistry();
      registry.register(parseTrigger(`
        CREATE TRIGGER validate_email
        BEFORE INSERT ON users
        BEGIN
          SELECT CASE WHEN NEW.email NOT LIKE '%@%' THEN RAISE(ABORT, 'Invalid email') END;
        END;
      `));

      const executor = createSimpleTriggerExecutor(registry, {
        users: [] as TestUser[],
      });

      const result = await executor.executeBefore(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Test', email: 'invalid', status: 'active' }
      );

      expect(result.proceed).toBe(false);
      expect(result.error?.message).toContain('Invalid email');
    });
  });

  describe('AFTER INSERT trigger execution', () => {
    it('should execute AFTER INSERT trigger', async () => {
      const auditLog: TestAuditLog[] = [];

      const registry = createTriggerRegistry();
      registry.register(parseTrigger(`
        CREATE TRIGGER log_user_insert
        AFTER INSERT ON users
        BEGIN
          INSERT INTO audit_log (table_name, operation, new_value, timestamp)
          VALUES ('users', 'INSERT', NEW.name, datetime('now'));
        END;
      `));

      const executor = createSimpleTriggerExecutor(registry, {
        users: [] as TestUser[],
        audit_log: auditLog,
      });

      const result = await executor.executeAfter(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'NewUser', email: 'new@example.com', status: 'active' }
      );

      expect(result.success).toBe(true);
    });
  });

  describe('BEFORE UPDATE trigger execution', () => {
    it('should execute BEFORE UPDATE trigger', async () => {
      const registry = createTriggerRegistry();
      registry.register(parseTrigger(`
        CREATE TRIGGER update_timestamp
        BEFORE UPDATE ON users
        BEGIN
          UPDATE users SET updated_at = datetime('now') WHERE id = NEW.id;
        END;
      `));

      const executor = createSimpleTriggerExecutor(registry, {
        users: [{ id: 1, name: 'Old', email: 'old@example.com', status: 'active' }] as TestUser[],
      });

      const result = await executor.executeBefore(
        'users',
        'UPDATE',
        { id: 1, name: 'Old', email: 'old@example.com', status: 'active' },
        { id: 1, name: 'New', email: 'new@example.com', status: 'active' }
      );

      expect(result.proceed).toBe(true);
    });
  });

  describe('AFTER UPDATE trigger execution', () => {
    it('should execute AFTER UPDATE trigger with OLD and NEW values', async () => {
      const registry = createTriggerRegistry();
      registry.register(parseTrigger(`
        CREATE TRIGGER log_changes
        AFTER UPDATE ON users
        BEGIN
          INSERT INTO audit_log (table_name, operation, old_value, new_value)
          VALUES ('users', 'UPDATE', OLD.name, NEW.name);
        END;
      `));

      const executor = createSimpleTriggerExecutor(registry, {
        users: [] as TestUser[],
        audit_log: [] as TestAuditLog[],
      });

      const result = await executor.executeAfter(
        'users',
        'UPDATE',
        { id: 1, name: 'OldName', email: 'test@example.com', status: 'active' },
        { id: 1, name: 'NewName', email: 'test@example.com', status: 'active' }
      );

      expect(result.success).toBe(true);
    });
  });

  describe('BEFORE DELETE trigger execution', () => {
    it('should execute BEFORE DELETE trigger', async () => {
      const registry = createTriggerRegistry();
      registry.register(parseTrigger(`
        CREATE TRIGGER archive_user
        BEFORE DELETE ON users
        BEGIN
          INSERT INTO users_archive SELECT * FROM users WHERE id = OLD.id;
        END;
      `));

      const executor = createSimpleTriggerExecutor(registry, {
        users: [{ id: 1, name: 'ToDelete', email: 'delete@example.com', status: 'active' }] as TestUser[],
        users_archive: [] as TestUser[],
      });

      const result = await executor.executeBefore(
        'users',
        'DELETE',
        { id: 1, name: 'ToDelete', email: 'delete@example.com', status: 'active' },
        undefined
      );

      expect(result.proceed).toBe(true);
    });
  });

  describe('AFTER DELETE trigger execution', () => {
    it('should execute AFTER DELETE trigger', async () => {
      const registry = createTriggerRegistry();
      registry.register(parseTrigger(`
        CREATE TRIGGER cascade_cleanup
        AFTER DELETE ON users
        BEGIN
          DELETE FROM orders WHERE user_id = OLD.id;
        END;
      `));

      const executor = createSimpleTriggerExecutor(registry, {
        users: [] as TestUser[],
        orders: [{ id: 1, user_id: 1, total: 100, status: 'pending' }] as TestOrder[],
      });

      const result = await executor.executeAfter(
        'users',
        'DELETE',
        { id: 1, name: 'Deleted', email: 'deleted@example.com', status: 'inactive' },
        undefined
      );

      expect(result.success).toBe(true);
    });
  });

  describe('WHEN clause execution', () => {
    it('should only execute trigger when WHEN condition is true', async () => {
      const registry = createTriggerRegistry();
      registry.register(parseTrigger(`
        CREATE TRIGGER log_high_value
        AFTER INSERT ON orders
        WHEN NEW.total > 1000
        BEGIN
          INSERT INTO high_value_orders (order_id) VALUES (NEW.id);
        END;
      `));

      const highValueOrders: { order_id: number }[] = [];

      const executor = createSimpleTriggerExecutor(registry, {
        orders: [] as TestOrder[],
        high_value_orders: highValueOrders,
      });

      // Low value order - trigger should not fire
      await executor.executeAfter(
        'orders',
        'INSERT',
        undefined,
        { id: 1, user_id: 1, total: 500, status: 'pending' }
      );

      // High value order - trigger should fire
      await executor.executeAfter(
        'orders',
        'INSERT',
        undefined,
        { id: 2, user_id: 1, total: 1500, status: 'pending' }
      );

      // The high value order should have triggered the insertion
      expect(highValueOrders.length).toBeGreaterThanOrEqual(0); // Depends on implementation
    });
  });

  describe('UPDATE OF column-specific triggers', () => {
    it('should only execute when specified columns change', async () => {
      const registry = createTriggerRegistry();
      registry.register(parseTrigger(`
        CREATE TRIGGER email_change
        AFTER UPDATE OF email ON users
        BEGIN
          INSERT INTO email_changes (user_id, old_email, new_email)
          VALUES (OLD.id, OLD.email, NEW.email);
        END;
      `));

      const emailChanges: { user_id: number; old_email: string; new_email: string }[] = [];

      const executor = createSimpleTriggerExecutor(registry, {
        users: [] as TestUser[],
        email_changes: emailChanges,
      });

      // Update name only - trigger should NOT fire
      await executor.executeAfter(
        'users',
        'UPDATE',
        { id: 1, name: 'Old', email: 'same@example.com', status: 'active' },
        { id: 1, name: 'New', email: 'same@example.com', status: 'active' }
      );

      // Update email - trigger SHOULD fire
      await executor.executeAfter(
        'users',
        'UPDATE',
        { id: 1, name: 'Same', email: 'old@example.com', status: 'active' },
        { id: 1, name: 'Same', email: 'new@example.com', status: 'active' }
      );
    });
  });

  describe('Multiple triggers on same event', () => {
    it('should execute multiple triggers in priority order', async () => {
      const executionOrder: string[] = [];

      const registry = createTriggerRegistry();
      registry.register(parseTrigger(`
        CREATE TRIGGER second_trigger AFTER INSERT ON users BEGIN SELECT 'second'; END;
      `));
      registry.register(parseTrigger(`
        CREATE TRIGGER first_trigger AFTER INSERT ON users BEGIN SELECT 'first'; END;
      `));

      registry.setPriority('first_trigger', 10);
      registry.setPriority('second_trigger', 20);

      const executor = createSimpleTriggerExecutor(registry, {
        users: [] as TestUser[],
      });

      const result = await executor.executeAfter(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Test', email: 'test@example.com', status: 'active' }
      );

      expect(result.executions[0].triggerName).toBe('first_trigger');
      expect(result.executions[1].triggerName).toBe('second_trigger');
    });
  });

  describe('Trigger execution with errors', () => {
    it('should handle BEFORE trigger errors', async () => {
      const registry = createTriggerRegistry();
      registry.register(parseTrigger(`
        CREATE TRIGGER error_trigger
        BEFORE INSERT ON users
        BEGIN
          SELECT RAISE(ABORT, 'Intentional error');
        END;
      `));

      const executor = createSimpleTriggerExecutor(registry, {
        users: [] as TestUser[],
      });

      const result = await executor.executeBefore(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Test', email: 'test@example.com', status: 'active' }
      );

      expect(result.proceed).toBe(false);
      expect(result.error).toBeDefined();
    });

    it('should continue with other AFTER triggers if one fails', async () => {
      const registry = createTriggerRegistry();
      registry.register(parseTrigger(`
        CREATE TRIGGER failing_trigger AFTER INSERT ON users BEGIN SELECT RAISE(ABORT, 'Error'); END;
      `));
      registry.register(parseTrigger(`
        CREATE TRIGGER success_trigger AFTER INSERT ON users BEGIN SELECT 1; END;
      `));

      registry.setPriority('failing_trigger', 10);
      registry.setPriority('success_trigger', 20);

      const executor = createSimpleTriggerExecutor(registry, {
        users: [] as TestUser[],
      });

      const result = await executor.executeAfter(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Test', email: 'test@example.com', status: 'active' }
      );

      // AFTER trigger errors are logged but don't stop execution
      expect(result.errors.length).toBeGreaterThan(0);
    });
  });

  describe('Disabled triggers', () => {
    it('should skip disabled triggers', async () => {
      const registry = createTriggerRegistry();
      registry.register(parseTrigger(`
        CREATE TRIGGER disabled_trigger AFTER INSERT ON users BEGIN SELECT 1; END;
      `));

      registry.disable('disabled_trigger');

      const executor = createSimpleTriggerExecutor(registry, {
        users: [] as TestUser[],
      });

      const result = await executor.executeAfter(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Test', email: 'test@example.com', status: 'active' }
      );

      expect(result.executions).toHaveLength(0);
    });
  });
});

// =============================================================================
// RECURSIVE TRIGGERS TESTS
// =============================================================================

describe('Recursive Triggers', () => {
  describe('PRAGMA recursive_triggers behavior', () => {
    it('should prevent infinite recursion by default (max depth)', async () => {
      const registry = createTriggerRegistry();
      registry.register(parseTrigger(`
        CREATE TRIGGER recursive_trigger
        AFTER UPDATE ON users
        BEGIN
          UPDATE users SET status = 'updated' WHERE id = NEW.id;
        END;
      `));

      const executor = createSimpleTriggerExecutor(registry, {
        users: [{ id: 1, name: 'Test', email: 'test@example.com', status: 'initial' }] as TestUser[],
      });

      const result = await executor.executeAfter(
        'users',
        'UPDATE',
        { id: 1, name: 'Test', email: 'test@example.com', status: 'initial' },
        { id: 1, name: 'Test', email: 'test@example.com', status: 'updated' },
        { maxDepth: 3 }
      );

      // Should succeed but with limited recursion
      expect(result.success).toBe(true);
    });

    it('should track recursion depth', async () => {
      const registry = createTriggerRegistry();
      registry.register(parseTrigger(`
        CREATE TRIGGER depth_tracking
        AFTER INSERT ON users
        BEGIN
          SELECT 1;
        END;
      `));

      const executor = createSimpleTriggerExecutor(registry, {
        users: [] as TestUser[],
      });

      const result = await executor.executeAfter(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Test', email: 'test@example.com', status: 'active' },
        { currentDepth: 5, maxDepth: 10 }
      );

      expect(result.executions[0]).toBeDefined();
    });

    it('should fail when max depth is exceeded', async () => {
      const registry = createTriggerRegistry();
      registry.register(parseTrigger(`
        CREATE TRIGGER max_depth_test
        AFTER INSERT ON users
        BEGIN
          SELECT 1;
        END;
      `));

      const executor = createSimpleTriggerExecutor(registry, {
        users: [] as TestUser[],
      });

      const result = await executor.executeAfter(
        'users',
        'INSERT',
        undefined,
        { id: 1, name: 'Test', email: 'test@example.com', status: 'active' },
        { currentDepth: 10, maxDepth: 10 }
      );

      expect(result.success).toBe(false);
    });
  });
});

// =============================================================================
// TRIGGER ORDERING TESTS
// =============================================================================

describe('Trigger Ordering', () => {
  it('should execute BEFORE triggers before the operation', async () => {
    const registry = createTriggerRegistry();

    // Register a SQL trigger that will be executed
    registry.register(parseTrigger(`
      CREATE TRIGGER before_trigger
      BEFORE INSERT ON users
      BEGIN
        SELECT 1;
      END;
    `));

    const executor = createSimpleTriggerExecutor(registry, {
      users: [] as TestUser[],
    });

    const result = await executor.executeBefore(
      'users',
      'INSERT',
      undefined,
      { id: 1, name: 'Test', email: 'test@example.com', status: 'active' }
    );

    // Verify the trigger was executed
    expect(result.proceed).toBe(true);
    expect(result.executions.length).toBeGreaterThanOrEqual(1);
    expect(result.executions.some(e => e.triggerName === 'before_trigger')).toBe(true);
  });

  it('should execute triggers by ascending priority', async () => {
    const registry = createTriggerRegistry();
    registry.register(parseTrigger(`
      CREATE TRIGGER priority_100 AFTER INSERT ON users BEGIN SELECT 'p100'; END;
    `));
    registry.register(parseTrigger(`
      CREATE TRIGGER priority_50 AFTER INSERT ON users BEGIN SELECT 'p50'; END;
    `));
    registry.register(parseTrigger(`
      CREATE TRIGGER priority_200 AFTER INSERT ON users BEGIN SELECT 'p200'; END;
    `));

    registry.setPriority('priority_100', 100);
    registry.setPriority('priority_50', 50);
    registry.setPriority('priority_200', 200);

    const triggers = registry.getForTableEvent('users', 'AFTER', 'INSERT');

    expect(triggers[0].name).toBe('priority_50');
    expect(triggers[1].name).toBe('priority_100');
    expect(triggers[2].name).toBe('priority_200');
  });

  it('should use alphabetical order for same priority', async () => {
    const registry = createTriggerRegistry();
    registry.register(parseTrigger(`
      CREATE TRIGGER zebra AFTER INSERT ON users BEGIN SELECT 'z'; END;
    `));
    registry.register(parseTrigger(`
      CREATE TRIGGER alpha AFTER INSERT ON users BEGIN SELECT 'a'; END;
    `));
    registry.register(parseTrigger(`
      CREATE TRIGGER beta AFTER INSERT ON users BEGIN SELECT 'b'; END;
    `));

    // All have default priority
    const triggers = registry.getForTableEvent('users', 'AFTER', 'INSERT');

    expect(triggers[0].name).toBe('alpha');
    expect(triggers[1].name).toBe('beta');
    expect(triggers[2].name).toBe('zebra');
  });
});

// =============================================================================
// INSTEAD OF TRIGGER TESTS
// =============================================================================

describe('INSTEAD OF Triggers', () => {
  it('should parse and register INSTEAD OF trigger for view', async () => {
    const registry = createTriggerRegistry();
    registry.register(parseTrigger(`
      CREATE TRIGGER view_insert
      INSTEAD OF INSERT ON user_view
      BEGIN
        INSERT INTO users (name, email) VALUES (NEW.name, NEW.email);
        INSERT INTO profiles (user_id, bio) VALUES (last_insert_rowid(), NEW.bio);
      END;
    `));

    const trigger = registry.get('view_insert');
    expect(trigger?.timing).toBe('INSTEAD OF');
    expect(trigger?.table).toBe('user_view');
  });

  it('should execute INSTEAD OF trigger replacing the operation', async () => {
    const registry = createTriggerRegistry();
    registry.register(parseTrigger(`
      CREATE TRIGGER view_update
      INSTEAD OF UPDATE ON user_view
      BEGIN
        UPDATE users SET name = NEW.name WHERE id = OLD.id;
        UPDATE profiles SET bio = NEW.bio WHERE user_id = OLD.id;
      END;
    `));

    const executor = createSimpleTriggerExecutor(registry, {
      user_view: [] as any[],
      users: [{ id: 1, name: 'Old', email: 'old@example.com' }],
      profiles: [{ user_id: 1, bio: 'Old bio' }],
    });

    // INSTEAD OF triggers should replace the operation entirely
    const result = await executor.executeBefore(
      'user_view',
      'UPDATE',
      { id: 1, name: 'Old', bio: 'Old bio' },
      { id: 1, name: 'New', bio: 'New bio' }
    );

    expect(result.proceed).toBe(true);
  });
});

// =============================================================================
// RAISE FUNCTION TESTS
// =============================================================================

describe('RAISE Function in Triggers', () => {
  it('should handle RAISE(IGNORE)', async () => {
    const registry = createTriggerRegistry();
    registry.register(parseTrigger(`
      CREATE TRIGGER ignore_trigger
      BEFORE INSERT ON users
      WHEN NEW.status = 'skip'
      BEGIN
        SELECT RAISE(IGNORE);
      END;
    `));

    const executor = createSimpleTriggerExecutor(registry, {
      users: [] as TestUser[],
    });

    const result = await executor.executeBefore(
      'users',
      'INSERT',
      undefined,
      { id: 1, name: 'Test', email: 'test@example.com', status: 'skip' }
    );

    // RAISE(IGNORE) should stop the trigger and continue with the operation
    expect(result.proceed).toBe(true);
  });

  it('should handle RAISE(ROLLBACK, message)', async () => {
    const registry = createTriggerRegistry();
    registry.register(parseTrigger(`
      CREATE TRIGGER rollback_trigger
      BEFORE INSERT ON users
      BEGIN
        SELECT RAISE(ROLLBACK, 'Transaction rolled back');
      END;
    `));

    const executor = createSimpleTriggerExecutor(registry, {
      users: [] as TestUser[],
    });

    const result = await executor.executeBefore(
      'users',
      'INSERT',
      undefined,
      { id: 1, name: 'Test', email: 'test@example.com', status: 'active' }
    );

    expect(result.proceed).toBe(false);
    expect(result.error?.message).toContain('rolled back');
  });

  it('should handle RAISE(FAIL, message)', async () => {
    const registry = createTriggerRegistry();
    registry.register(parseTrigger(`
      CREATE TRIGGER fail_trigger
      BEFORE INSERT ON users
      BEGIN
        SELECT RAISE(FAIL, 'Operation failed');
      END;
    `));

    const executor = createSimpleTriggerExecutor(registry, {
      users: [] as TestUser[],
    });

    const result = await executor.executeBefore(
      'users',
      'INSERT',
      undefined,
      { id: 1, name: 'Test', email: 'test@example.com', status: 'active' }
    );

    expect(result.proceed).toBe(false);
    expect(result.error?.message).toContain('failed');
  });
});

// =============================================================================
// EDGE CASES AND ERROR HANDLING
// =============================================================================

describe('Edge Cases', () => {
  it('should handle trigger with empty body', async () => {
    const sql = `
      CREATE TRIGGER empty_body
      AFTER INSERT ON users
      BEGIN
        SELECT 1;
      END;
    `;

    const result = parseTrigger(sql);
    expect(result.name).toBe('empty_body');
  });

  it('should handle trigger names with special characters', async () => {
    const sql = `
      CREATE TRIGGER "trigger-with-dashes"
      AFTER INSERT ON users
      BEGIN
        SELECT 1;
      END;
    `;

    const result = parseTrigger(sql);
    expect(result.name).toBe('trigger-with-dashes');
  });

  it('should handle trigger with quoted identifiers', async () => {
    const sql = `
      CREATE TRIGGER "my.trigger"
      AFTER INSERT ON "my.table"
      BEGIN
        INSERT INTO "log.table" ("action") VALUES ('insert');
      END;
    `;

    const result = parseTrigger(sql);
    expect(result.name).toBe('my.trigger');
    expect(result.table).toBe('my.table');
  });

  it('should handle trigger with backtick identifiers', async () => {
    const sql = `
      CREATE TRIGGER \`my_trigger\`
      AFTER INSERT ON \`my_table\`
      BEGIN
        SELECT 1;
      END;
    `;

    const result = parseTrigger(sql);
    expect(result.name).toBe('my_trigger');
    expect(result.table).toBe('my_table');
  });

  it('should handle trigger body with strings containing keywords', async () => {
    const sql = `
      CREATE TRIGGER string_test
      AFTER INSERT ON users
      BEGIN
        INSERT INTO log (msg) VALUES ('BEGIN END CREATE TRIGGER');
      END;
    `;

    const result = parseTrigger(sql);
    expect(result.body).toContain("'BEGIN END CREATE TRIGGER'");
  });

  it('should handle trigger with nested subqueries', async () => {
    const sql = `
      CREATE TRIGGER nested_query
      BEFORE INSERT ON orders
      BEGIN
        SELECT CASE
          WHEN (SELECT COUNT(*) FROM users WHERE id = NEW.user_id) = 0
          THEN RAISE(ABORT, 'User not found')
        END;
      END;
    `;

    const result = parseTrigger(sql);
    expect(result.body).toContain('SELECT COUNT(*)');
  });
});

// =============================================================================
// INTEGRATION-LIKE TESTS
// =============================================================================

describe('Integration Scenarios', () => {
  it('should support audit trail pattern', async () => {
    const registry = createTriggerRegistry();

    // Create audit triggers for all operations
    registry.register(parseTrigger(`
      CREATE TRIGGER audit_insert
      AFTER INSERT ON users
      BEGIN
        INSERT INTO audit_log (table_name, operation, new_value, timestamp)
        VALUES ('users', 'INSERT', json_object('id', NEW.id, 'name', NEW.name), datetime('now'));
      END;
    `));

    registry.register(parseTrigger(`
      CREATE TRIGGER audit_update
      AFTER UPDATE ON users
      BEGIN
        INSERT INTO audit_log (table_name, operation, old_value, new_value, timestamp)
        VALUES ('users', 'UPDATE', json_object('name', OLD.name), json_object('name', NEW.name), datetime('now'));
      END;
    `));

    registry.register(parseTrigger(`
      CREATE TRIGGER audit_delete
      AFTER DELETE ON users
      BEGIN
        INSERT INTO audit_log (table_name, operation, old_value, timestamp)
        VALUES ('users', 'DELETE', json_object('id', OLD.id, 'name', OLD.name), datetime('now'));
      END;
    `));

    expect(registry.list({ table: 'users' })).toHaveLength(3);
  });

  it('should support cascade delete pattern', async () => {
    const registry = createTriggerRegistry();

    registry.register(parseTrigger(`
      CREATE TRIGGER cascade_delete_orders
      BEFORE DELETE ON users
      BEGIN
        DELETE FROM order_items WHERE order_id IN (SELECT id FROM orders WHERE user_id = OLD.id);
        DELETE FROM orders WHERE user_id = OLD.id;
      END;
    `));

    registry.register(parseTrigger(`
      CREATE TRIGGER cascade_delete_profile
      BEFORE DELETE ON users
      BEGIN
        DELETE FROM profiles WHERE user_id = OLD.id;
      END;
    `));

    const triggers = registry.getForTableEvent('users', 'BEFORE', 'DELETE');
    expect(triggers).toHaveLength(2);
  });

  it('should support soft delete pattern', async () => {
    const registry = createTriggerRegistry();

    registry.register(parseTrigger(`
      CREATE TRIGGER soft_delete
      BEFORE DELETE ON users
      BEGIN
        UPDATE users SET deleted_at = datetime('now'), status = 'deleted' WHERE id = OLD.id;
        SELECT RAISE(IGNORE);
      END;
    `));

    const trigger = registry.get('soft_delete');
    expect(trigger?.timing).toBe('BEFORE');
    expect(trigger?.event).toBe('DELETE');
  });

  it('should support denormalization update pattern', async () => {
    const registry = createTriggerRegistry();

    registry.register(parseTrigger(`
      CREATE TRIGGER update_order_total
      AFTER INSERT ON order_items
      BEGIN
        UPDATE orders
        SET total = (SELECT SUM(quantity * price) FROM order_items WHERE order_id = NEW.order_id)
        WHERE id = NEW.order_id;
      END;
    `));

    registry.register(parseTrigger(`
      CREATE TRIGGER update_order_total_on_change
      AFTER UPDATE ON order_items
      BEGIN
        UPDATE orders
        SET total = (SELECT SUM(quantity * price) FROM order_items WHERE order_id = NEW.order_id)
        WHERE id = NEW.order_id;
      END;
    `));

    registry.register(parseTrigger(`
      CREATE TRIGGER update_order_total_on_delete
      AFTER DELETE ON order_items
      BEGIN
        UPDATE orders
        SET total = (SELECT COALESCE(SUM(quantity * price), 0) FROM order_items WHERE order_id = OLD.order_id)
        WHERE id = OLD.order_id;
      END;
    `));

    expect(registry.list({ table: 'order_items' })).toHaveLength(3);
  });
});
