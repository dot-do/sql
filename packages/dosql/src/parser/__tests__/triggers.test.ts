/**
 * DoSQL CREATE TRIGGER Support Tests - RED Phase TDD
 *
 * Tests documenting CREATE TRIGGER syntax support gaps in the DoSQL parser.
 * These tests use the `it.fails()` pattern to document features that are
 * not yet implemented, serving as a specification for future development.
 *
 * Issue: sql-zhy.15 - [RED] CREATE TRIGGER Support
 *
 * SQLite Trigger Syntax Reference:
 * https://www.sqlite.org/lang_createtrigger.html
 *
 * CREATE TRIGGER [IF NOT EXISTS] schema_name.trigger_name
 * [BEFORE | AFTER | INSTEAD OF]
 * [DELETE | INSERT | UPDATE [OF column_list]]
 * ON table_name
 * [FOR EACH ROW]
 * [WHEN expr]
 * BEGIN
 *   trigger_body;
 * END;
 *
 * @packageDocumentation
 */

import { describe, it, expect } from 'vitest';
import { parseDDL, isParseSuccess, isParseError } from '../ddl.js';
import type { DDLStatement } from '../ddl-types.js';

// =============================================================================
// TYPE DEFINITIONS FOR TRIGGER SUPPORT
// =============================================================================

/**
 * Expected trigger timing type
 */
type TriggerTiming = 'BEFORE' | 'AFTER' | 'INSTEAD OF';

/**
 * Expected trigger event type
 */
type TriggerEvent = 'INSERT' | 'UPDATE' | 'DELETE';

/**
 * Expected CREATE TRIGGER statement structure
 * This is the expected AST structure when trigger support is implemented
 */
interface CreateTriggerStatement {
  type: 'CREATE TRIGGER';
  name: string;
  schema?: string;
  ifNotExists?: boolean;
  temporary?: boolean;
  timing: TriggerTiming;
  event: TriggerEvent;
  columns?: string[]; // For UPDATE OF column_list
  table: string;
  forEachRow?: boolean;
  when?: string; // WHEN condition expression
  body: string[]; // Trigger body statements
}

/**
 * Expected DROP TRIGGER statement structure
 */
interface DropTriggerStatement {
  type: 'DROP TRIGGER';
  name: string;
  schema?: string;
  ifExists?: boolean;
}

/**
 * Type guard for CREATE TRIGGER statement
 */
function isCreateTriggerStatement(stmt: DDLStatement): stmt is CreateTriggerStatement & DDLStatement {
  return (stmt as any).type === 'CREATE TRIGGER';
}

/**
 * Type guard for DROP TRIGGER statement
 */
function isDropTriggerStatement(stmt: DDLStatement): stmt is DropTriggerStatement & DDLStatement {
  return (stmt as any).type === 'DROP TRIGGER';
}

// =============================================================================
// BASIC BEFORE TRIGGERS (DOCUMENTED GAPS)
// =============================================================================

describe('CREATE TRIGGER - BEFORE Triggers', () => {
  describe('Gap: Basic BEFORE triggers are not supported', () => {
    it.fails('should parse basic BEFORE UPDATE trigger', () => {
      const sql = `
        CREATE TRIGGER update_timestamp
        BEFORE UPDATE ON users
        FOR EACH ROW
        BEGIN
          UPDATE users SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      if (!isCreateTriggerStatement(result.statement)) return;

      expect(result.statement.name).toBe('update_timestamp');
      expect(result.statement.timing).toBe('BEFORE');
      expect(result.statement.event).toBe('UPDATE');
      expect(result.statement.table).toBe('users');
      expect(result.statement.forEachRow).toBe(true);
      expect(result.statement.body).toHaveLength(1);
    });

    it.fails('should parse BEFORE INSERT trigger', () => {
      const sql = `
        CREATE TRIGGER set_defaults
        BEFORE INSERT ON products
        FOR EACH ROW
        BEGIN
          INSERT INTO audit_log (action, table_name) VALUES ('INSERT', 'products');
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      if (!isCreateTriggerStatement(result.statement)) return;

      expect(result.statement.timing).toBe('BEFORE');
      expect(result.statement.event).toBe('INSERT');
    });

    it.fails('should parse BEFORE DELETE trigger', () => {
      const sql = `
        CREATE TRIGGER prevent_delete
        BEFORE DELETE ON critical_data
        FOR EACH ROW
        BEGIN
          SELECT RAISE(ABORT, 'Cannot delete critical data');
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      if (!isCreateTriggerStatement(result.statement)) return;

      expect(result.statement.timing).toBe('BEFORE');
      expect(result.statement.event).toBe('DELETE');
    });
  });
});

// =============================================================================
// AFTER TRIGGERS WITH CONDITIONS (DOCUMENTED GAPS)
// =============================================================================

describe('CREATE TRIGGER - AFTER Triggers with Conditions', () => {
  describe('Gap: AFTER triggers with WHEN clause are not supported', () => {
    it.fails('should parse AFTER DELETE trigger with WHEN condition', () => {
      const sql = `
        CREATE TRIGGER audit_delete
        AFTER DELETE ON orders
        WHEN OLD.status = 'completed'
        BEGIN
          INSERT INTO audit_log (action, table_name, record_id) VALUES ('DELETE', 'orders', OLD.id);
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      if (!isCreateTriggerStatement(result.statement)) return;

      expect(result.statement.name).toBe('audit_delete');
      expect(result.statement.timing).toBe('AFTER');
      expect(result.statement.event).toBe('DELETE');
      expect(result.statement.table).toBe('orders');
      expect(result.statement.when).toContain("OLD.status = 'completed'");
    });

    it.fails('should parse AFTER UPDATE trigger with WHEN condition', () => {
      const sql = `
        CREATE TRIGGER log_price_change
        AFTER UPDATE ON products
        WHEN NEW.price != OLD.price
        BEGIN
          INSERT INTO price_history (product_id, old_price, new_price, changed_at)
          VALUES (NEW.id, OLD.price, NEW.price, CURRENT_TIMESTAMP);
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      if (!isCreateTriggerStatement(result.statement)) return;

      expect(result.statement.timing).toBe('AFTER');
      expect(result.statement.event).toBe('UPDATE');
      expect(result.statement.when).toContain('NEW.price != OLD.price');
    });

    it.fails('should parse AFTER INSERT trigger without FOR EACH ROW', () => {
      const sql = `
        CREATE TRIGGER increment_count
        AFTER INSERT ON items
        BEGIN
          UPDATE counters SET count = count + 1 WHERE name = 'items';
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      if (!isCreateTriggerStatement(result.statement)) return;

      expect(result.statement.timing).toBe('AFTER');
      expect(result.statement.event).toBe('INSERT');
      // FOR EACH ROW is optional and defaults to true in SQLite
      expect(result.statement.forEachRow).toBe(true);
    });
  });
});

// =============================================================================
// INSTEAD OF TRIGGERS FOR VIEWS (DOCUMENTED GAPS)
// =============================================================================

describe('CREATE TRIGGER - INSTEAD OF Triggers for Views', () => {
  describe('Gap: INSTEAD OF triggers for views are not supported', () => {
    it.fails('should parse INSTEAD OF INSERT trigger on view', () => {
      const sql = `
        CREATE TRIGGER insert_user_view
        INSTEAD OF INSERT ON user_details_view
        FOR EACH ROW
        BEGIN
          INSERT INTO users (name, email) VALUES (NEW.name, NEW.email);
          INSERT INTO profiles (user_id, bio) VALUES (last_insert_rowid(), NEW.bio);
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      if (!isCreateTriggerStatement(result.statement)) return;

      expect(result.statement.name).toBe('insert_user_view');
      expect(result.statement.timing).toBe('INSTEAD OF');
      expect(result.statement.event).toBe('INSERT');
      expect(result.statement.body).toHaveLength(2);
    });

    it.fails('should parse INSTEAD OF UPDATE trigger on view', () => {
      const sql = `
        CREATE TRIGGER update_user_view
        INSTEAD OF UPDATE ON user_details_view
        FOR EACH ROW
        BEGIN
          UPDATE users SET name = NEW.name, email = NEW.email WHERE id = OLD.id;
          UPDATE profiles SET bio = NEW.bio WHERE user_id = OLD.id;
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      if (!isCreateTriggerStatement(result.statement)) return;

      expect(result.statement.timing).toBe('INSTEAD OF');
      expect(result.statement.event).toBe('UPDATE');
    });

    it.fails('should parse INSTEAD OF DELETE trigger on view', () => {
      const sql = `
        CREATE TRIGGER delete_user_view
        INSTEAD OF DELETE ON user_details_view
        FOR EACH ROW
        BEGIN
          DELETE FROM profiles WHERE user_id = OLD.id;
          DELETE FROM users WHERE id = OLD.id;
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      if (!isCreateTriggerStatement(result.statement)) return;

      expect(result.statement.timing).toBe('INSTEAD OF');
      expect(result.statement.event).toBe('DELETE');
    });
  });
});

// =============================================================================
// TRIGGERS WITH MULTIPLE STATEMENTS (DOCUMENTED GAPS)
// =============================================================================

describe('CREATE TRIGGER - Multiple Statements', () => {
  describe('Gap: Triggers with multiple statements in body are not supported', () => {
    it.fails('should parse trigger with multiple INSERT statements', () => {
      const sql = `
        CREATE TRIGGER on_user_create
        AFTER INSERT ON users
        FOR EACH ROW
        BEGIN
          INSERT INTO user_settings (user_id, theme) VALUES (NEW.id, 'default');
          INSERT INTO user_preferences (user_id, notifications) VALUES (NEW.id, 1);
          INSERT INTO audit_log (action, user_id) VALUES ('USER_CREATED', NEW.id);
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      if (!isCreateTriggerStatement(result.statement)) return;

      expect(result.statement.body).toHaveLength(3);
    });

    it.fails('should parse trigger with mixed DML statements', () => {
      const sql = `
        CREATE TRIGGER cleanup_on_delete
        AFTER DELETE ON parent_table
        FOR EACH ROW
        BEGIN
          DELETE FROM child_table1 WHERE parent_id = OLD.id;
          DELETE FROM child_table2 WHERE parent_id = OLD.id;
          UPDATE summary_table SET count = count - 1 WHERE type = OLD.type;
          INSERT INTO deleted_records (table_name, record_id) VALUES ('parent_table', OLD.id);
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      if (!isCreateTriggerStatement(result.statement)) return;

      expect(result.statement.body).toHaveLength(4);
    });

    it.fails('should parse trigger with SELECT statements', () => {
      const sql = `
        CREATE TRIGGER validate_insert
        BEFORE INSERT ON orders
        FOR EACH ROW
        BEGIN
          SELECT CASE WHEN NEW.amount < 0 THEN RAISE(ABORT, 'Amount cannot be negative') END;
          SELECT CASE WHEN NEW.user_id IS NULL THEN RAISE(ABORT, 'User ID required') END;
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      if (!isCreateTriggerStatement(result.statement)) return;

      expect(result.statement.body).toHaveLength(2);
    });
  });
});

// =============================================================================
// DROP TRIGGER SYNTAX (DOCUMENTED GAPS)
// =============================================================================

describe('DROP TRIGGER', () => {
  describe('Gap: DROP TRIGGER syntax is not supported', () => {
    it.fails('should parse simple DROP TRIGGER', () => {
      const sql = 'DROP TRIGGER update_timestamp';
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isDropTriggerStatement(result.statement)).toBe(true);
      if (!isDropTriggerStatement(result.statement)) return;

      expect(result.statement.name).toBe('update_timestamp');
    });

    it.fails('should parse DROP TRIGGER IF EXISTS', () => {
      const sql = 'DROP TRIGGER IF EXISTS update_timestamp';
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isDropTriggerStatement(result.statement)).toBe(true);
      if (!isDropTriggerStatement(result.statement)) return;

      expect(result.statement.ifExists).toBe(true);
      expect(result.statement.name).toBe('update_timestamp');
    });

    it.fails('should parse schema-qualified DROP TRIGGER', () => {
      const sql = 'DROP TRIGGER main.update_timestamp';
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isDropTriggerStatement(result.statement)).toBe(true);
      if (!isDropTriggerStatement(result.statement)) return;

      expect(result.statement.schema).toBe('main');
      expect(result.statement.name).toBe('update_timestamp');
    });
  });
});

// =============================================================================
// RAISE() FUNCTION IN TRIGGERS (DOCUMENTED GAPS)
// =============================================================================

describe('CREATE TRIGGER - RAISE() Function', () => {
  describe('Gap: RAISE() function in triggers is not supported', () => {
    it.fails('should parse RAISE(IGNORE)', () => {
      const sql = `
        CREATE TRIGGER skip_invalid
        BEFORE INSERT ON data
        WHEN NEW.value < 0
        BEGIN
          SELECT RAISE(IGNORE);
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      expect(result.statement.body[0]).toContain('RAISE(IGNORE)');
    });

    it.fails('should parse RAISE(ROLLBACK, message)', () => {
      const sql = `
        CREATE TRIGGER prevent_update
        BEFORE UPDATE ON locked_table
        BEGIN
          SELECT RAISE(ROLLBACK, 'This table is locked and cannot be modified');
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      expect(result.statement.body[0]).toContain('RAISE(ROLLBACK');
    });

    it.fails('should parse RAISE(ABORT, message)', () => {
      const sql = `
        CREATE TRIGGER validate_email
        BEFORE INSERT ON users
        WHEN NEW.email NOT LIKE '%@%'
        BEGIN
          SELECT RAISE(ABORT, 'Invalid email format');
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
    });

    it.fails('should parse RAISE(FAIL, message)', () => {
      const sql = `
        CREATE TRIGGER check_constraint
        BEFORE UPDATE ON products
        WHEN NEW.price < NEW.cost
        BEGIN
          SELECT RAISE(FAIL, 'Price cannot be less than cost');
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
    });
  });
});

// =============================================================================
// NEW/OLD REFERENCES VALIDATION (DOCUMENTED GAPS)
// =============================================================================

describe('CREATE TRIGGER - NEW/OLD References', () => {
  describe('Gap: NEW/OLD reference validation is not supported', () => {
    it.fails('should allow NEW reference in INSERT trigger', () => {
      const sql = `
        CREATE TRIGGER log_insert
        AFTER INSERT ON items
        FOR EACH ROW
        BEGIN
          INSERT INTO item_log (item_id, item_name, action)
          VALUES (NEW.id, NEW.name, 'INSERTED');
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      // NEW should be valid in INSERT trigger body
      expect(result.statement.body[0]).toContain('NEW.');
    });

    it.fails('should allow OLD reference in DELETE trigger', () => {
      const sql = `
        CREATE TRIGGER log_delete
        AFTER DELETE ON items
        FOR EACH ROW
        BEGIN
          INSERT INTO item_log (item_id, item_name, action)
          VALUES (OLD.id, OLD.name, 'DELETED');
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      // OLD should be valid in DELETE trigger body
      expect(result.statement.body[0]).toContain('OLD.');
    });

    it.fails('should allow both NEW and OLD in UPDATE trigger', () => {
      const sql = `
        CREATE TRIGGER log_update
        AFTER UPDATE ON items
        FOR EACH ROW
        BEGIN
          INSERT INTO item_log (item_id, old_name, new_name, action)
          VALUES (NEW.id, OLD.name, NEW.name, 'UPDATED');
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      // Both NEW and OLD should be valid in UPDATE trigger
      expect(result.statement.body[0]).toContain('NEW.');
      expect(result.statement.body[0]).toContain('OLD.');
    });

    it.fails('should allow NEW and OLD in WHEN clause', () => {
      const sql = `
        CREATE TRIGGER conditional_update
        AFTER UPDATE ON orders
        WHEN OLD.status != NEW.status AND NEW.status = 'shipped'
        BEGIN
          INSERT INTO shipment_log (order_id) VALUES (NEW.id);
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      // NEW and OLD should be valid in WHEN clause
      expect(result.statement.when).toContain('OLD.status');
      expect(result.statement.when).toContain('NEW.status');
    });
  });
});

// =============================================================================
// UPDATE OF COLUMN LIST (DOCUMENTED GAPS)
// =============================================================================

describe('CREATE TRIGGER - UPDATE OF column_list', () => {
  describe('Gap: UPDATE OF column_list syntax is not supported', () => {
    it.fails('should parse UPDATE OF single column', () => {
      const sql = `
        CREATE TRIGGER log_price_change
        AFTER UPDATE OF price ON products
        FOR EACH ROW
        BEGIN
          INSERT INTO price_history (product_id, old_price, new_price)
          VALUES (NEW.id, OLD.price, NEW.price);
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      if (!isCreateTriggerStatement(result.statement)) return;

      expect(result.statement.event).toBe('UPDATE');
      expect(result.statement.columns).toEqual(['price']);
    });

    it.fails('should parse UPDATE OF multiple columns', () => {
      const sql = `
        CREATE TRIGGER log_inventory_change
        AFTER UPDATE OF quantity, reserved_quantity ON inventory
        FOR EACH ROW
        BEGIN
          INSERT INTO inventory_log (item_id, old_qty, new_qty)
          VALUES (NEW.id, OLD.quantity, NEW.quantity);
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      if (!isCreateTriggerStatement(result.statement)) return;

      expect(result.statement.event).toBe('UPDATE');
      expect(result.statement.columns).toEqual(['quantity', 'reserved_quantity']);
    });
  });
});

// =============================================================================
// TRIGGER OPTIONS AND MODIFIERS (DOCUMENTED GAPS)
// =============================================================================

describe('CREATE TRIGGER - Options and Modifiers', () => {
  describe('Gap: Trigger options are not supported', () => {
    it.fails('should parse CREATE TRIGGER IF NOT EXISTS', () => {
      const sql = `
        CREATE TRIGGER IF NOT EXISTS update_timestamp
        AFTER UPDATE ON users
        FOR EACH ROW
        BEGIN
          UPDATE users SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      if (!isCreateTriggerStatement(result.statement)) return;

      expect(result.statement.ifNotExists).toBe(true);
    });

    it.fails('should parse CREATE TEMPORARY TRIGGER', () => {
      const sql = `
        CREATE TEMPORARY TRIGGER temp_audit
        AFTER INSERT ON temp_table
        FOR EACH ROW
        BEGIN
          SELECT 1;
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      if (!isCreateTriggerStatement(result.statement)) return;

      expect(result.statement.temporary).toBe(true);
    });

    it.fails('should parse schema-qualified CREATE TRIGGER', () => {
      const sql = `
        CREATE TRIGGER main.update_timestamp
        AFTER UPDATE ON users
        FOR EACH ROW
        BEGIN
          UPDATE users SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      if (!isCreateTriggerStatement(result.statement)) return;

      expect(result.statement.schema).toBe('main');
      expect(result.statement.name).toBe('update_timestamp');
    });
  });
});

// =============================================================================
// EXPECTED CURRENT BEHAVIOR (REGULAR TESTS)
// =============================================================================

describe('CREATE TRIGGER - Current Parser Behavior', () => {
  describe('Current behavior: Parser rejects trigger syntax', () => {
    it('should return error for CREATE TRIGGER (not implemented)', () => {
      const sql = `
        CREATE TRIGGER update_timestamp
        BEFORE UPDATE ON users
        FOR EACH ROW
        BEGIN
          UPDATE users SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;
      `;
      const result = parseDDL(sql);

      // Currently, CREATE TRIGGER is not a recognized DDL statement
      expect(isParseError(result)).toBe(true);
      if (isParseError(result)) {
        // The parser should return an error for unknown statement types
        expect(result.error).toBeTruthy();
      }
    });

    it('should return error for DROP TRIGGER (not implemented)', () => {
      const sql = 'DROP TRIGGER update_timestamp';
      const result = parseDDL(sql);

      expect(isParseError(result)).toBe(true);
      if (isParseError(result)) {
        expect(result.error).toBeTruthy();
      }
    });

    it('should still parse valid DDL statements correctly', () => {
      // Verify that the parser still works for supported DDL statements
      const sql = 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)';
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.type).toBe('CREATE TABLE');
    });
  });
});

// =============================================================================
// COMPLEX TRIGGER SCENARIOS (DOCUMENTED GAPS)
// =============================================================================

describe('CREATE TRIGGER - Complex Scenarios', () => {
  describe('Gap: Complex trigger patterns are not supported', () => {
    it.fails('should parse trigger with nested CASE expression in body', () => {
      const sql = `
        CREATE TRIGGER categorize_order
        AFTER INSERT ON orders
        FOR EACH ROW
        BEGIN
          UPDATE orders SET category =
            CASE
              WHEN NEW.amount < 100 THEN 'small'
              WHEN NEW.amount < 1000 THEN 'medium'
              ELSE 'large'
            END
          WHERE id = NEW.id;
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
    });

    it.fails('should parse trigger with subquery in body', () => {
      const sql = `
        CREATE TRIGGER update_order_total
        AFTER INSERT ON order_items
        FOR EACH ROW
        BEGIN
          UPDATE orders SET total = (
            SELECT SUM(quantity * price) FROM order_items WHERE order_id = NEW.order_id
          ) WHERE id = NEW.order_id;
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
    });

    it.fails('should parse trigger with complex WHEN condition', () => {
      const sql = `
        CREATE TRIGGER audit_sensitive_changes
        AFTER UPDATE ON employees
        WHEN (OLD.salary != NEW.salary OR OLD.department != NEW.department)
          AND NEW.is_active = 1
        BEGIN
          INSERT INTO hr_audit (employee_id, change_type, old_value, new_value, changed_at)
          VALUES (
            NEW.id,
            CASE WHEN OLD.salary != NEW.salary THEN 'salary' ELSE 'department' END,
            CASE WHEN OLD.salary != NEW.salary THEN OLD.salary ELSE OLD.department END,
            CASE WHEN OLD.salary != NEW.salary THEN NEW.salary ELSE NEW.department END,
            CURRENT_TIMESTAMP
          );
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      if (!isCreateTriggerStatement(result.statement)) return;

      expect(result.statement.when).toBeTruthy();
    });

    it.fails('should parse trigger referencing another table', () => {
      const sql = `
        CREATE TRIGGER sync_inventory
        AFTER UPDATE OF quantity ON warehouse_stock
        FOR EACH ROW
        WHEN NEW.quantity < (SELECT min_stock FROM products WHERE id = NEW.product_id)
        BEGIN
          INSERT INTO reorder_alerts (product_id, current_quantity, min_quantity, created_at)
          SELECT NEW.product_id, NEW.quantity, min_stock, CURRENT_TIMESTAMP
          FROM products WHERE id = NEW.product_id;
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
    });
  });
});

// =============================================================================
// ERROR HANDLING FOR INVALID TRIGGER SYNTAX (WHEN IMPLEMENTED)
// =============================================================================

describe('CREATE TRIGGER - Error Handling (Future Implementation)', () => {
  describe('Gap: Semantic validation for invalid trigger usage', () => {
    // These tests document the expected semantic validation that should occur
    // when trigger parsing is implemented. The parser should eventually either:
    // 1. Parse successfully and provide semantic validation errors, or
    // 2. Provide helpful error messages during parsing

    it.fails('should validate that OLD reference is invalid in INSERT triggers', () => {
      // OLD is not valid in INSERT triggers - only NEW is available
      // When implemented, this should either:
      // - Fail to parse with a helpful message, OR
      // - Parse successfully but include a validation warning/error
      const sql = `
        CREATE TRIGGER invalid_trigger
        AFTER INSERT ON items
        FOR EACH ROW
        BEGIN
          INSERT INTO log (value) VALUES (OLD.id);
        END;
      `;
      const result = parseDDL(sql);

      // Expected: Should parse the trigger and detect invalid OLD reference
      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      // Semantic validation should flag OLD usage in INSERT trigger
    });

    it.fails('should validate that NEW reference is invalid in DELETE triggers', () => {
      // NEW is not valid in DELETE triggers - only OLD is available
      const sql = `
        CREATE TRIGGER invalid_trigger
        AFTER DELETE ON items
        FOR EACH ROW
        BEGIN
          INSERT INTO log (value) VALUES (NEW.id);
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      // Semantic validation should flag NEW usage in DELETE trigger
    });

    it.fails('should validate that INSTEAD OF can only be used on views', () => {
      // INSTEAD OF can only be used on views, not tables
      // Parser should either reject this or validation should flag it
      const sql = `
        CREATE TRIGGER invalid_trigger
        INSTEAD OF INSERT ON regular_table
        FOR EACH ROW
        BEGIN
          SELECT 1;
        END;
      `;
      const result = parseDDL(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(isCreateTriggerStatement(result.statement)).toBe(true);
      // Note: Full validation would require schema information to verify
      // that 'regular_table' is a table not a view
    });
  });
});
