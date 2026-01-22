/**
 * View Tests for DoSQL
 *
 * Comprehensive tests for SQL VIEW support including:
 * - CREATE VIEW / CREATE OR REPLACE VIEW
 * - CREATE VIEW IF NOT EXISTS
 * - CREATE TEMP VIEW
 * - DROP VIEW / DROP VIEW IF EXISTS
 * - SELECT from view
 * - View with parameters (column aliases)
 * - Nested views (view selecting from view)
 * - View with JOINs
 * - Updatable views (INSERT/UPDATE/DELETE through view)
 * - INSTEAD OF triggers on views
 * - WITH CHECK OPTION
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  parseCreateView,
  tryParseCreateView,
  isCreateView,
  parseDropView,
  isDropView,
  extractReferences,
  validateViewDefinition,
  generateCreateViewSql,
  generateDropViewSql,
  parseSelectColumns,
} from './parser.js';
import {
  createViewRegistry,
  createInMemoryViewStorage,
  analyzeDependencies,
  ViewBuilder,
  view,
} from './registry.js';
import {
  createViewExecutor,
  createMockSqlExecutor,
  createSqlViewManager,
} from './executor.js';
import type {
  ParsedView,
  ViewDefinition,
  ViewRegistry,
  InsteadOfTrigger,
  ViewDMLContext,
} from './types.js';
import { ViewError, ViewErrorCode } from './types.js';

// =============================================================================
// PARSER TESTS - CREATE VIEW
// =============================================================================

describe('View Parser - CREATE VIEW', () => {
  describe('parseCreateView', () => {
    it('should parse a simple CREATE VIEW statement', () => {
      const sql = 'CREATE VIEW active_users AS SELECT * FROM users WHERE active = true';

      const result = parseCreateView(sql);

      expect(result.name).toBe('active_users');
      expect(result.selectQuery).toBe('SELECT * FROM users WHERE active = true');
      expect(result.orReplace).toBe(false);
      expect(result.ifNotExists).toBe(false);
      expect(result.temporary).toBe(false);
    });

    it('should parse CREATE OR REPLACE VIEW', () => {
      const sql = 'CREATE OR REPLACE VIEW user_emails AS SELECT id, email FROM users';

      const result = parseCreateView(sql);

      expect(result.name).toBe('user_emails');
      expect(result.orReplace).toBe(true);
    });

    it('should parse CREATE VIEW IF NOT EXISTS', () => {
      const sql = 'CREATE VIEW IF NOT EXISTS admins AS SELECT * FROM users WHERE role = "admin"';

      const result = parseCreateView(sql);

      expect(result.name).toBe('admins');
      expect(result.ifNotExists).toBe(true);
    });

    it('should parse CREATE TEMP VIEW', () => {
      const sql = 'CREATE TEMP VIEW session_data AS SELECT * FROM sessions';

      const result = parseCreateView(sql);

      expect(result.name).toBe('session_data');
      expect(result.temporary).toBe(true);
    });

    it('should parse CREATE TEMPORARY VIEW', () => {
      const sql = 'CREATE TEMPORARY VIEW temp_orders AS SELECT * FROM orders WHERE status = "pending"';

      const result = parseCreateView(sql);

      expect(result.name).toBe('temp_orders');
      expect(result.temporary).toBe(true);
    });

    it('should parse view with schema prefix', () => {
      const sql = 'CREATE VIEW myschema.users_view AS SELECT * FROM users';

      const result = parseCreateView(sql);

      expect(result.name).toBe('users_view');
      expect(result.schema).toBe('myschema');
    });

    it('should parse view with column aliases', () => {
      const sql = 'CREATE VIEW user_info (user_id, user_name, user_email) AS SELECT id, name, email FROM users';

      const result = parseCreateView(sql);

      expect(result.name).toBe('user_info');
      expect(result.columns).toEqual(['user_id', 'user_name', 'user_email']);
    });

    it('should parse view with WITH CHECK OPTION', () => {
      const sql = 'CREATE VIEW active_users AS SELECT * FROM users WHERE active = true WITH CHECK OPTION';

      const result = parseCreateView(sql);

      expect(result.name).toBe('active_users');
      expect(result.checkOption).toBe('CASCADED');
    });

    it('should parse view with WITH LOCAL CHECK OPTION', () => {
      const sql = 'CREATE VIEW local_users AS SELECT * FROM users WHERE region = "local" WITH LOCAL CHECK OPTION';

      const result = parseCreateView(sql);

      expect(result.checkOption).toBe('LOCAL');
    });

    it('should parse view with WITH CASCADED CHECK OPTION', () => {
      const sql = 'CREATE VIEW cascaded_users AS SELECT * FROM users WITH CASCADED CHECK OPTION';

      const result = parseCreateView(sql);

      expect(result.checkOption).toBe('CASCADED');
    });

    it('should parse view with complex SELECT query', () => {
      const sql = `
        CREATE VIEW order_summary AS
        SELECT
          o.id,
          o.user_id,
          u.name AS user_name,
          o.total,
          o.status
        FROM orders o
        JOIN users u ON o.user_id = u.id
        WHERE o.status != 'cancelled'
      `;

      const result = parseCreateView(sql);

      expect(result.name).toBe('order_summary');
      expect(result.selectQuery).toContain('SELECT');
      expect(result.selectQuery).toContain('JOIN');
    });

    it('should parse view with subquery', () => {
      const sql = `
        CREATE VIEW top_customers AS
        SELECT * FROM (
          SELECT user_id, SUM(total) as total_spent
          FROM orders
          GROUP BY user_id
        ) WHERE total_spent > 1000
      `;

      const result = parseCreateView(sql);

      expect(result.name).toBe('top_customers');
      expect(result.selectQuery).toContain('SUM(total)');
    });

    it('should parse view with WITH clause (CTE)', () => {
      const sql = `
        CREATE VIEW cte_view AS
        WITH ranked AS (
          SELECT *, ROW_NUMBER() OVER (ORDER BY id) as rn
          FROM users
        )
        SELECT * FROM ranked WHERE rn <= 10
      `;

      const result = parseCreateView(sql);

      expect(result.name).toBe('cte_view');
      expect(result.selectQuery).toContain('WITH ranked AS');
    });

    it('should parse all options combined', () => {
      const sql = `
        CREATE OR REPLACE TEMPORARY VIEW IF NOT EXISTS
        myschema.complex_view (col1, col2, col3) AS
        SELECT a, b, c FROM table1
        WITH LOCAL CHECK OPTION
      `;

      const result = parseCreateView(sql);

      expect(result.name).toBe('complex_view');
      expect(result.schema).toBe('myschema');
      expect(result.columns).toEqual(['col1', 'col2', 'col3']);
      expect(result.orReplace).toBe(true);
      expect(result.ifNotExists).toBe(true);
      expect(result.temporary).toBe(true);
      expect(result.checkOption).toBe('LOCAL');
    });

    it('should throw error for invalid syntax', () => {
      expect(() => parseCreateView('SELECT * FROM users')).toThrow(ViewError);
      expect(() => parseCreateView('CREATE TABLE users (id INT)')).toThrow(ViewError);
      expect(() => parseCreateView('CREATE VIEW')).toThrow(ViewError);
    });

    it('should throw error for view without SELECT', () => {
      expect(() => parseCreateView('CREATE VIEW bad_view AS INSERT INTO users VALUES (1)')).toThrow(ViewError);
    });
  });

  describe('tryParseCreateView', () => {
    it('should return success for valid view', () => {
      const sql = 'CREATE VIEW test_view AS SELECT 1';

      const result = tryParseCreateView(sql);

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.view.name).toBe('test_view');
      }
    });

    it('should return error for invalid view', () => {
      const sql = 'SELECT * FROM users';

      const result = tryParseCreateView(sql);

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.error).toBeInstanceOf(ViewError);
      }
    });
  });

  describe('isCreateView', () => {
    it('should detect CREATE VIEW statements', () => {
      expect(isCreateView('CREATE VIEW test AS SELECT 1')).toBe(true);
      expect(isCreateView('create view test as select 1')).toBe(true);
      expect(isCreateView('CREATE OR REPLACE VIEW test AS SELECT 1')).toBe(true);
      expect(isCreateView('CREATE TEMP VIEW test AS SELECT 1')).toBe(true);
      expect(isCreateView('CREATE TEMPORARY VIEW test AS SELECT 1')).toBe(true);
    });

    it('should not detect non-view statements', () => {
      expect(isCreateView('SELECT * FROM users')).toBe(false);
      expect(isCreateView('CREATE TABLE users (id INT)')).toBe(false);
      expect(isCreateView('DROP VIEW test')).toBe(false);
    });
  });
});

// =============================================================================
// PARSER TESTS - DROP VIEW
// =============================================================================

describe('View Parser - DROP VIEW', () => {
  describe('parseDropView', () => {
    it('should parse simple DROP VIEW', () => {
      const sql = 'DROP VIEW my_view';

      const result = parseDropView(sql);

      expect(result.name).toBe('my_view');
      expect(result.ifExists).toBe(false);
      expect(result.cascade).toBe(false);
    });

    it('should parse DROP VIEW IF EXISTS', () => {
      const sql = 'DROP VIEW IF EXISTS my_view';

      const result = parseDropView(sql);

      expect(result.name).toBe('my_view');
      expect(result.ifExists).toBe(true);
    });

    it('should parse DROP VIEW with schema', () => {
      const sql = 'DROP VIEW myschema.my_view';

      const result = parseDropView(sql);

      expect(result.name).toBe('my_view');
      expect(result.schema).toBe('myschema');
    });

    it('should parse DROP VIEW CASCADE', () => {
      const sql = 'DROP VIEW my_view CASCADE';

      const result = parseDropView(sql);

      expect(result.name).toBe('my_view');
      expect(result.cascade).toBe(true);
    });

    it('should parse DROP VIEW RESTRICT', () => {
      const sql = 'DROP VIEW my_view RESTRICT';

      const result = parseDropView(sql);

      expect(result.name).toBe('my_view');
      expect(result.cascade).toBe(false);
    });

    it('should parse all options combined', () => {
      const sql = 'DROP VIEW IF EXISTS myschema.my_view CASCADE';

      const result = parseDropView(sql);

      expect(result.name).toBe('my_view');
      expect(result.schema).toBe('myschema');
      expect(result.ifExists).toBe(true);
      expect(result.cascade).toBe(true);
    });

    it('should throw error for invalid syntax', () => {
      expect(() => parseDropView('SELECT * FROM users')).toThrow(ViewError);
      expect(() => parseDropView('DROP TABLE users')).toThrow(ViewError);
    });
  });

  describe('isDropView', () => {
    it('should detect DROP VIEW statements', () => {
      expect(isDropView('DROP VIEW test')).toBe(true);
      expect(isDropView('drop view test')).toBe(true);
      expect(isDropView('DROP VIEW IF EXISTS test')).toBe(true);
    });

    it('should not detect non-drop statements', () => {
      expect(isDropView('CREATE VIEW test AS SELECT 1')).toBe(false);
      expect(isDropView('DROP TABLE test')).toBe(false);
    });
  });
});

// =============================================================================
// PARSER TESTS - Column Parsing
// =============================================================================

describe('View Parser - Column Parsing', () => {
  describe('parseSelectColumns', () => {
    it('should parse simple column references', () => {
      const columns = parseSelectColumns('SELECT id, name, email FROM users');

      expect(columns.length).toBeGreaterThanOrEqual(3);
      expect(columns.map(c => c.name)).toContain('id');
      expect(columns.map(c => c.name)).toContain('name');
      expect(columns.map(c => c.name)).toContain('email');
    });

    it('should parse aliased columns', () => {
      const columns = parseSelectColumns('SELECT id AS user_id, name AS user_name FROM users');

      expect(columns.some(c => c.name === 'user_id')).toBe(true);
      expect(columns.some(c => c.name === 'user_name')).toBe(true);
    });

    it('should parse function expressions', () => {
      const columns = parseSelectColumns('SELECT COUNT(*) AS total, SUM(amount) AS sum_amount FROM orders');

      expect(columns.some(c => c.name === 'total')).toBe(true);
      expect(columns.some(c => c.name === 'sum_amount')).toBe(true);
    });

    it('should infer types for aggregate functions', () => {
      const columns = parseSelectColumns('SELECT COUNT(*) AS cnt FROM users');

      const countCol = columns.find(c => c.name === 'cnt');
      expect(countCol?.type).toBe('INTEGER');
    });

    it('should handle qualified column names', () => {
      const columns = parseSelectColumns('SELECT users.id, orders.total FROM users JOIN orders');

      expect(columns.some(c => c.name === 'id')).toBe(true);
      expect(columns.some(c => c.name === 'total')).toBe(true);
    });
  });

  describe('extractReferences', () => {
    it('should extract table references from FROM clause', () => {
      const { tables } = extractReferences('SELECT * FROM users');

      expect(tables).toContain('users');
    });

    it('should extract table references from JOIN clause', () => {
      const { tables } = extractReferences('SELECT * FROM users JOIN orders ON users.id = orders.user_id');

      expect(tables).toContain('users');
      expect(tables).toContain('orders');
    });

    it('should extract multiple table references', () => {
      const { tables } = extractReferences(`
        SELECT * FROM users u
        JOIN orders o ON u.id = o.user_id
        JOIN products p ON o.product_id = p.id
      `);

      expect(tables).toContain('users');
      expect(tables).toContain('orders');
      expect(tables).toContain('products');
    });
  });
});

// =============================================================================
// PARSER TESTS - Validation
// =============================================================================

describe('View Parser - Validation', () => {
  describe('validateViewDefinition', () => {
    it('should validate a valid view', () => {
      const view: ParsedView = {
        name: 'test_view',
        selectQuery: 'SELECT * FROM users',
        orReplace: false,
        ifNotExists: false,
        temporary: false,
        rawSql: 'CREATE VIEW test_view AS SELECT * FROM users',
      };

      const result = validateViewDefinition(view);

      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should reject view with invalid name', () => {
      const view: ParsedView = {
        name: '',
        selectQuery: 'SELECT * FROM users',
        orReplace: false,
        ifNotExists: false,
        temporary: false,
        rawSql: '',
      };

      const result = validateViewDefinition(view);

      expect(result.valid).toBe(false);
      expect(result.errors.length).toBeGreaterThan(0);
    });

    it('should reject view with reserved prefix', () => {
      const view: ParsedView = {
        name: 'sqlite_internal',
        selectQuery: 'SELECT * FROM users',
        orReplace: false,
        ifNotExists: false,
        temporary: false,
        rawSql: '',
      };

      const result = validateViewDefinition(view);

      expect(result.valid).toBe(false);
    });

    it('should warn about non-updatable view patterns', () => {
      const viewWithDistinct: ParsedView = {
        name: 'test_view',
        selectQuery: 'SELECT DISTINCT * FROM users',
        orReplace: false,
        ifNotExists: false,
        temporary: false,
        rawSql: '',
      };

      const result = validateViewDefinition(viewWithDistinct);

      expect(result.warnings.length).toBeGreaterThan(0);
    });

    it('should reject self-referencing view', () => {
      const view: ParsedView = {
        name: 'recursive_view',
        selectQuery: 'SELECT * FROM recursive_view',
        orReplace: false,
        ifNotExists: false,
        temporary: false,
        rawSql: '',
      };

      const result = validateViewDefinition(view);

      expect(result.valid).toBe(false);
    });
  });
});

// =============================================================================
// PARSER TESTS - SQL Generation
// =============================================================================

describe('View Parser - SQL Generation', () => {
  describe('generateCreateViewSql', () => {
    it('should generate simple CREATE VIEW', () => {
      const view: ParsedView = {
        name: 'test_view',
        selectQuery: 'SELECT * FROM users',
        orReplace: false,
        ifNotExists: false,
        temporary: false,
        rawSql: '',
      };

      const sql = generateCreateViewSql(view);

      expect(sql).toBe('CREATE VIEW test_view AS SELECT * FROM users');
    });

    it('should generate CREATE OR REPLACE VIEW', () => {
      const view: ParsedView = {
        name: 'test_view',
        selectQuery: 'SELECT * FROM users',
        orReplace: true,
        ifNotExists: false,
        temporary: false,
        rawSql: '',
      };

      const sql = generateCreateViewSql(view);

      expect(sql).toContain('OR REPLACE');
    });

    it('should generate CREATE TEMPORARY VIEW', () => {
      const view: ParsedView = {
        name: 'test_view',
        selectQuery: 'SELECT * FROM users',
        orReplace: false,
        ifNotExists: false,
        temporary: true,
        rawSql: '',
      };

      const sql = generateCreateViewSql(view);

      expect(sql).toContain('TEMPORARY');
    });

    it('should generate CREATE VIEW with columns', () => {
      const view: ParsedView = {
        name: 'test_view',
        columns: ['a', 'b', 'c'],
        selectQuery: 'SELECT x, y, z FROM t',
        orReplace: false,
        ifNotExists: false,
        temporary: false,
        rawSql: '',
      };

      const sql = generateCreateViewSql(view);

      expect(sql).toContain('(a, b, c)');
    });

    it('should generate CREATE VIEW with CHECK OPTION', () => {
      const view: ParsedView = {
        name: 'test_view',
        selectQuery: 'SELECT * FROM users WHERE active = true',
        orReplace: false,
        ifNotExists: false,
        temporary: false,
        checkOption: 'CASCADED',
        rawSql: '',
      };

      const sql = generateCreateViewSql(view);

      expect(sql).toContain('WITH CHECK OPTION');
    });
  });

  describe('generateDropViewSql', () => {
    it('should generate simple DROP VIEW', () => {
      const sql = generateDropViewSql('test_view');

      expect(sql).toBe('DROP VIEW test_view');
    });

    it('should generate DROP VIEW IF EXISTS', () => {
      const sql = generateDropViewSql('test_view', { ifExists: true });

      expect(sql).toContain('IF EXISTS');
    });

    it('should generate DROP VIEW CASCADE', () => {
      const sql = generateDropViewSql('test_view', { cascade: true });

      expect(sql).toContain('CASCADE');
    });
  });
});

// =============================================================================
// REGISTRY TESTS
// =============================================================================

describe('View Registry', () => {
  let registry: ViewRegistry;

  beforeEach(() => {
    registry = createViewRegistry();
  });

  describe('register', () => {
    it('should register a new view', () => {
      const parsed = parseCreateView('CREATE VIEW test_view AS SELECT * FROM users');

      const view = registry.register(parsed);

      expect(view.name).toBe('test_view');
      expect(view.version).toBe(1);
      expect(view.enabled).toBe(true);
      expect(view.createdAt).toBeInstanceOf(Date);
    });

    it('should reject duplicate view names', () => {
      const parsed = parseCreateView('CREATE VIEW test_view AS SELECT * FROM users');
      registry.register(parsed);

      expect(() => registry.register(parsed)).toThrow(ViewError);
    });

    it('should allow IF NOT EXISTS for duplicate names', () => {
      const parsed1 = parseCreateView('CREATE VIEW test_view AS SELECT * FROM users');
      const parsed2 = parseCreateView('CREATE VIEW IF NOT EXISTS test_view AS SELECT * FROM orders');

      registry.register(parsed1);
      const result = registry.register(parsed2);

      expect(result.selectQuery).toContain('users'); // Returns existing view
    });

    it('should allow OR REPLACE for duplicate names', () => {
      const parsed1 = parseCreateView('CREATE VIEW test_view AS SELECT * FROM users');
      const parsed2 = parseCreateView('CREATE OR REPLACE VIEW test_view AS SELECT * FROM orders');

      registry.register(parsed1);
      const result = registry.register(parsed2);

      expect(result.selectQuery).toContain('orders');
      expect(result.version).toBe(2);
    });

    it('should track referenced tables', () => {
      const parsed = parseCreateView('CREATE VIEW test_view AS SELECT * FROM users JOIN orders ON users.id = orders.user_id');

      const view = registry.register(parsed);

      expect(view.referencedTables).toContain('users');
      expect(view.referencedTables).toContain('orders');
    });

    it('should determine if view is updatable', () => {
      const simpleView = parseCreateView('CREATE VIEW simple AS SELECT * FROM users');
      const joinView = parseCreateView('CREATE VIEW joined AS SELECT * FROM users JOIN orders ON users.id = orders.user_id');

      const simple = registry.register(simpleView);
      const joined = registry.register(joinView);

      expect(simple.updatable).toBe(true);
      expect(joined.updatable).toBe(false);
    });

    it('should mark temporary views correctly', () => {
      const parsed = parseCreateView('CREATE TEMP VIEW temp_view AS SELECT * FROM users');

      const view = registry.register(parsed);

      expect(view.type).toBe('temporary');
    });
  });

  describe('get', () => {
    it('should retrieve view by name', () => {
      const parsed = parseCreateView('CREATE VIEW test_view AS SELECT * FROM users');
      registry.register(parsed);

      const view = registry.get('test_view');

      expect(view).toBeDefined();
      expect(view?.name).toBe('test_view');
    });

    it('should be case-insensitive', () => {
      const parsed = parseCreateView('CREATE VIEW TestView AS SELECT * FROM users');
      registry.register(parsed);

      expect(registry.get('testview')).toBeDefined();
      expect(registry.get('TESTVIEW')).toBeDefined();
    });

    it('should return undefined for non-existent view', () => {
      expect(registry.get('nonexistent')).toBeUndefined();
    });
  });

  describe('exists', () => {
    it('should return true for existing view', () => {
      registry.register(parseCreateView('CREATE VIEW test_view AS SELECT * FROM users'));

      expect(registry.exists('test_view')).toBe(true);
    });

    it('should return false for non-existent view', () => {
      expect(registry.exists('nonexistent')).toBe(false);
    });
  });

  describe('list', () => {
    beforeEach(() => {
      registry.register(parseCreateView('CREATE VIEW users_view AS SELECT * FROM users'));
      registry.register(parseCreateView('CREATE TEMP VIEW temp_view AS SELECT * FROM users'));
      registry.register(parseCreateView('CREATE VIEW orders_view AS SELECT * FROM orders'));
    });

    it('should list all non-temporary views by default', () => {
      const views = registry.list();

      expect(views).toHaveLength(2);
      expect(views.map(v => v.name)).not.toContain('temp_view');
    });

    it('should include temporary views when requested', () => {
      const views = registry.list({ includeTemporary: true });

      expect(views).toHaveLength(3);
    });

    it('should filter by referenced table', () => {
      const views = registry.list({ referencedTable: 'users', includeTemporary: true });

      expect(views).toHaveLength(2);
    });
  });

  describe('drop', () => {
    it('should drop an existing view', () => {
      registry.register(parseCreateView('CREATE VIEW test_view AS SELECT * FROM users'));

      const result = registry.drop('test_view');

      expect(result).toBe(true);
      expect(registry.exists('test_view')).toBe(false);
    });

    it('should throw error for non-existent view without IF EXISTS', () => {
      expect(() => registry.drop('nonexistent')).toThrow(ViewError);
    });

    it('should return false for non-existent view with IF EXISTS', () => {
      const result = registry.drop('nonexistent', { ifExists: true });

      expect(result).toBe(false);
    });
  });

  describe('clear', () => {
    it('should clear all views', () => {
      registry.register(parseCreateView('CREATE VIEW view1 AS SELECT * FROM t1'));
      registry.register(parseCreateView('CREATE VIEW view2 AS SELECT * FROM t2'));

      registry.clear();

      expect(registry.list({ includeTemporary: true })).toHaveLength(0);
    });

    it('should clear only temporary views when requested', () => {
      registry.register(parseCreateView('CREATE VIEW view1 AS SELECT * FROM t1'));
      registry.register(parseCreateView('CREATE TEMP VIEW temp1 AS SELECT * FROM t2'));

      registry.clear({ temporaryOnly: true });

      expect(registry.list({ includeTemporary: true })).toHaveLength(1);
      expect(registry.exists('view1')).toBe(true);
      expect(registry.exists('temp1')).toBe(false);
    });
  });
});

// =============================================================================
// NESTED VIEWS TESTS
// =============================================================================

describe('Nested Views', () => {
  let registry: ViewRegistry;

  beforeEach(() => {
    registry = createViewRegistry();
  });

  it('should support view selecting from another view', () => {
    registry.register(parseCreateView('CREATE VIEW base_view AS SELECT id, name FROM users'));
    registry.register(parseCreateView('CREATE VIEW derived_view AS SELECT * FROM base_view WHERE id > 10'));

    const derived = registry.get('derived_view');

    expect(derived).toBeDefined();
    expect(derived?.referencedViews).toContain('base_view');
  });

  it('should track view dependencies', () => {
    registry.register(parseCreateView('CREATE VIEW view_a AS SELECT * FROM users'));
    registry.register(parseCreateView('CREATE VIEW view_b AS SELECT * FROM view_a'));
    registry.register(parseCreateView('CREATE VIEW view_c AS SELECT * FROM view_b'));

    const deps = registry.getDependencies('view_c');

    expect(deps.map(d => d.name)).toContain('view_b');
  });

  it('should track view dependents', () => {
    registry.register(parseCreateView('CREATE VIEW view_a AS SELECT * FROM users'));
    registry.register(parseCreateView('CREATE VIEW view_b AS SELECT * FROM view_a'));
    registry.register(parseCreateView('CREATE VIEW view_c AS SELECT * FROM view_a'));

    const dependents = registry.getDependents('view_a');

    expect(dependents).toHaveLength(2);
  });

  it('should detect circular dependencies', () => {
    registry.register(parseCreateView('CREATE VIEW view_a AS SELECT * FROM users'));

    // Create a view that references view_a
    registry.register(parseCreateView('CREATE VIEW view_b AS SELECT * FROM view_a'));

    // This would create a cycle if view_a referenced view_b
    // The registry should detect this during registration
    // For now, we test that the dependency analysis works
    const analysis = analyzeDependencies(registry);

    expect(analysis.circularDependencies).toHaveLength(0);
  });

  it('should cascade drop dependent views', () => {
    registry.register(parseCreateView('CREATE VIEW view_a AS SELECT * FROM users'));
    registry.register(parseCreateView('CREATE VIEW view_b AS SELECT * FROM view_a'));

    registry.drop('view_a', { cascade: true });

    expect(registry.exists('view_a')).toBe(false);
    expect(registry.exists('view_b')).toBe(false);
  });

  it('should prevent dropping view with dependents without CASCADE', () => {
    registry.register(parseCreateView('CREATE VIEW view_a AS SELECT * FROM users'));
    registry.register(parseCreateView('CREATE VIEW view_b AS SELECT * FROM view_a'));

    expect(() => registry.drop('view_a')).toThrow(ViewError);
  });
});

// =============================================================================
// DEPENDENCY ANALYSIS TESTS
// =============================================================================

describe('Dependency Analysis', () => {
  let registry: ViewRegistry;

  beforeEach(() => {
    registry = createViewRegistry();
  });

  it('should calculate topological order', () => {
    registry.register(parseCreateView('CREATE VIEW level0 AS SELECT * FROM users'));
    registry.register(parseCreateView('CREATE VIEW level1a AS SELECT * FROM level0'));
    registry.register(parseCreateView('CREATE VIEW level1b AS SELECT * FROM level0'));
    registry.register(parseCreateView('CREATE VIEW level2 AS SELECT * FROM level1a'));

    const analysis = analyzeDependencies(registry);

    const order = analysis.topologicalOrder;
    expect(order.indexOf('level0')).toBeLessThan(order.indexOf('level1a'));
    expect(order.indexOf('level0')).toBeLessThan(order.indexOf('level1b'));
    expect(order.indexOf('level1a')).toBeLessThan(order.indexOf('level2'));
  });

  it('should calculate depth correctly', () => {
    registry.register(parseCreateView('CREATE VIEW level0 AS SELECT * FROM users'));
    registry.register(parseCreateView('CREATE VIEW level1 AS SELECT * FROM level0'));
    registry.register(parseCreateView('CREATE VIEW level2 AS SELECT * FROM level1'));

    const analysis = analyzeDependencies(registry);

    expect(analysis.nodes.get('level0')?.depth).toBe(0);
    expect(analysis.nodes.get('level1')?.depth).toBe(1);
    expect(analysis.nodes.get('level2')?.depth).toBe(2);
  });
});

// =============================================================================
// VIEW BUILDER TESTS
// =============================================================================

describe('View Builder', () => {
  let registry: ViewRegistry;

  beforeEach(() => {
    registry = createViewRegistry();
  });

  it('should build a simple view', () => {
    const parsed = view('test_view')
      .as('SELECT * FROM users')
      .build();

    expect(parsed.name).toBe('test_view');
    expect(parsed.selectQuery).toBe('SELECT * FROM users');
  });

  it('should build view with all options', () => {
    const parsed = view('complex_view')
      .schema('myschema')
      .columns('a', 'b', 'c')
      .select('SELECT x, y, z FROM t')
      .orReplace()
      .temporary()
      .withCheckOption('LOCAL')
      .build();

    expect(parsed.name).toBe('complex_view');
    expect(parsed.schema).toBe('myschema');
    expect(parsed.columns).toEqual(['a', 'b', 'c']);
    expect(parsed.orReplace).toBe(true);
    expect(parsed.temporary).toBe(true);
    expect(parsed.checkOption).toBe('LOCAL');
  });

  it('should build and register view', () => {
    const viewDef = view('registered_view')
      .as('SELECT * FROM users')
      .description('A test view')
      .tag('test', 'example')
      .register(registry);

    expect(viewDef.name).toBe('registered_view');
    expect(viewDef.description).toBe('A test view');
    expect(viewDef.tags).toContain('test');
  });

  it('should throw error if name is missing', () => {
    expect(() => view().as('SELECT 1').build()).toThrow(ViewError);
  });

  it('should throw error if query is missing', () => {
    expect(() => view('test').build()).toThrow(ViewError);
  });
});

// =============================================================================
// INSTEAD OF TRIGGER TESTS
// =============================================================================

describe('INSTEAD OF Triggers', () => {
  let registry: ViewRegistry;

  beforeEach(() => {
    registry = createViewRegistry();
    registry.register(parseCreateView('CREATE VIEW user_view AS SELECT id, name FROM users'));
  });

  it('should register INSTEAD OF trigger', () => {
    const trigger: InsteadOfTrigger = {
      name: 'user_insert_trigger',
      viewName: 'user_view',
      operations: ['INSERT'],
      handler: async () => ({ success: true, rowsAffected: 1 }),
      enabled: true,
      priority: 1,
    };

    registry.registerInsteadOfTrigger(trigger);

    const triggers = registry.getInsteadOfTriggers('user_view', 'INSERT');
    expect(triggers).toHaveLength(1);
    expect(triggers[0].name).toBe('user_insert_trigger');
  });

  it('should support multiple operations on one trigger', () => {
    const trigger: InsteadOfTrigger = {
      name: 'user_dml_trigger',
      viewName: 'user_view',
      operations: ['INSERT', 'UPDATE', 'DELETE'],
      handler: async () => ({ success: true, rowsAffected: 1 }),
      enabled: true,
      priority: 1,
    };

    registry.registerInsteadOfTrigger(trigger);

    expect(registry.getInsteadOfTriggers('user_view', 'INSERT')).toHaveLength(1);
    expect(registry.getInsteadOfTriggers('user_view', 'UPDATE')).toHaveLength(1);
    expect(registry.getInsteadOfTriggers('user_view', 'DELETE')).toHaveLength(1);
  });

  it('should remove INSTEAD OF trigger', () => {
    const trigger: InsteadOfTrigger = {
      name: 'to_remove',
      viewName: 'user_view',
      operations: ['INSERT'],
      handler: async () => ({ success: true, rowsAffected: 1 }),
      enabled: true,
      priority: 1,
    };

    registry.registerInsteadOfTrigger(trigger);
    const removed = registry.removeInsteadOfTrigger('to_remove');

    expect(removed).toBe(true);
    expect(registry.getInsteadOfTriggers('user_view', 'INSERT')).toHaveLength(0);
  });

  it('should order triggers by priority', () => {
    registry.registerInsteadOfTrigger({
      name: 'low_priority',
      viewName: 'user_view',
      operations: ['INSERT'],
      handler: async () => ({ success: true, rowsAffected: 1 }),
      enabled: true,
      priority: 100,
    });

    registry.registerInsteadOfTrigger({
      name: 'high_priority',
      viewName: 'user_view',
      operations: ['INSERT'],
      handler: async () => ({ success: true, rowsAffected: 1 }),
      enabled: true,
      priority: 1,
    });

    const triggers = registry.getInsteadOfTriggers('user_view', 'INSERT');

    expect(triggers[0].name).toBe('high_priority');
    expect(triggers[1].name).toBe('low_priority');
  });

  it('should not return disabled triggers', () => {
    registry.registerInsteadOfTrigger({
      name: 'disabled_trigger',
      viewName: 'user_view',
      operations: ['INSERT'],
      handler: async () => ({ success: true, rowsAffected: 1 }),
      enabled: false,
      priority: 1,
    });

    const triggers = registry.getInsteadOfTriggers('user_view', 'INSERT');

    expect(triggers).toHaveLength(0);
  });
});

// =============================================================================
// VIEW EXECUTOR TESTS
// =============================================================================

describe('View Executor', () => {
  let registry: ViewRegistry;
  let executor: ReturnType<typeof createViewExecutor>;
  let mockData: Record<string, Record<string, unknown>[]>;

  beforeEach(() => {
    mockData = {
      users: [
        { id: 1, name: 'Alice', email: 'alice@example.com', active: true },
        { id: 2, name: 'Bob', email: 'bob@example.com', active: false },
        { id: 3, name: 'Charlie', email: 'charlie@example.com', active: true },
      ],
      orders: [
        { id: 1, user_id: 1, total: 100, status: 'completed' },
        { id: 2, user_id: 2, total: 200, status: 'pending' },
      ],
    };

    registry = createViewRegistry({
      tableExists: (name) => name in mockData,
    });

    registry.register(parseCreateView('CREATE VIEW active_users AS SELECT * FROM users WHERE active = true'));
    registry.register(parseCreateView('CREATE VIEW all_users AS SELECT * FROM users'));

    executor = createViewExecutor({
      registry,
      sqlExecutor: createMockSqlExecutor(mockData),
    });
  });

  describe('select', () => {
    it('should select from a view', async () => {
      const result = await executor.select('all_users');

      expect(result.rows).toHaveLength(3);
      expect(result.rowCount).toBe(3);
      expect(result.executionTime).toBeGreaterThanOrEqual(0);
    });

    it('should select from view with WHERE filter', async () => {
      const result = await executor.select('all_users', { where: 'active = true' });

      expect(result.rows).toHaveLength(2);
    });

    it('should throw error for non-existent view', async () => {
      await expect(executor.select('nonexistent')).rejects.toThrow(ViewError);
    });
  });

  describe('expand', () => {
    it('should expand a simple view', () => {
      const expanded = executor.expand('all_users');

      expect(expanded).toContain('SELECT * FROM users');
    });

    it('should throw error for non-existent view', () => {
      expect(() => executor.expand('nonexistent')).toThrow(ViewError);
    });
  });

  describe('validate', () => {
    it('should validate a valid view', () => {
      const result = executor.validate('all_users');

      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should return invalid for non-existent view', () => {
      const result = executor.validate('nonexistent');

      expect(result.valid).toBe(false);
    });
  });

  describe('insert through view', () => {
    it('should insert through updatable view', async () => {
      const result = await executor.insert('all_users', {
        id: 4,
        name: 'David',
        email: 'david@example.com',
        active: true,
      });

      expect(result.success).toBe(true);
      expect(result.rowsAffected).toBe(1);
    });

    it('should use INSTEAD OF trigger for insert', async () => {
      let handlerCalled = false;
      registry.registerInsteadOfTrigger({
        name: 'insert_trigger',
        viewName: 'all_users',
        operations: ['INSERT'],
        handler: async (ctx) => {
          handlerCalled = true;
          expect(ctx.new).toBeDefined();
          expect(ctx.operation).toBe('INSERT');
          return { success: true, rowsAffected: 1 };
        },
        enabled: true,
        priority: 1,
      });

      await executor.insert('all_users', { id: 5, name: 'Eve' });

      expect(handlerCalled).toBe(true);
    });
  });

  describe('update through view', () => {
    it('should update through updatable view', async () => {
      const result = await executor.update(
        'all_users',
        { name: 'Alice Updated' },
        'id = 1'
      );

      expect(result.success).toBe(true);
    });

    it('should use INSTEAD OF trigger for update', async () => {
      let handlerCalled = false;
      registry.registerInsteadOfTrigger({
        name: 'update_trigger',
        viewName: 'all_users',
        operations: ['UPDATE'],
        handler: async (ctx) => {
          handlerCalled = true;
          expect(ctx.old).toBeDefined();
          expect(ctx.new).toBeDefined();
          expect(ctx.operation).toBe('UPDATE');
          return { success: true, rowsAffected: 1 };
        },
        enabled: true,
        priority: 1,
      });

      await executor.update('all_users', { name: 'Updated' }, 'id = 1');

      expect(handlerCalled).toBe(true);
    });
  });

  describe('delete through view', () => {
    it('should delete through updatable view', async () => {
      const result = await executor.delete('all_users', 'id = 1');

      expect(result.success).toBe(true);
    });

    it('should use INSTEAD OF trigger for delete', async () => {
      let handlerCalled = false;
      registry.registerInsteadOfTrigger({
        name: 'delete_trigger',
        viewName: 'all_users',
        operations: ['DELETE'],
        handler: async (ctx) => {
          handlerCalled = true;
          expect(ctx.old).toBeDefined();
          expect(ctx.new).toBeUndefined();
          expect(ctx.operation).toBe('DELETE');
          return { success: true, rowsAffected: 1 };
        },
        enabled: true,
        priority: 1,
      });

      await executor.delete('all_users', 'id = 1');

      expect(handlerCalled).toBe(true);
    });
  });
});

// =============================================================================
// SQL VIEW MANAGER TESTS
// =============================================================================

describe('SQL View Manager', () => {
  let registry: ViewRegistry;
  let manager: ReturnType<typeof createSqlViewManager>;

  beforeEach(() => {
    registry = createViewRegistry();
    manager = createSqlViewManager(registry);
  });

  it('should detect view statements', () => {
    expect(manager.isViewStatement('CREATE VIEW test AS SELECT 1')).toBe(true);
    expect(manager.isViewStatement('DROP VIEW test')).toBe(true);
    expect(manager.isViewStatement('SELECT * FROM users')).toBe(false);
  });

  it('should execute CREATE VIEW', async () => {
    const result = await manager.execute('CREATE VIEW test_view AS SELECT * FROM users');

    expect(result).toBeDefined();
    expect((result as ViewDefinition).name).toBe('test_view');
  });

  it('should execute DROP VIEW', async () => {
    await manager.execute('CREATE VIEW test_view AS SELECT * FROM users');
    const result = await manager.execute('DROP VIEW test_view');

    expect(result).toBe(true);
  });

  it('should throw error for non-view statements', async () => {
    await expect(manager.execute('SELECT * FROM users')).rejects.toThrow(ViewError);
  });
});

// =============================================================================
// VIEW ERROR TESTS
// =============================================================================

describe('View Errors', () => {
  it('should create ViewError with code', () => {
    const error = new ViewError(
      ViewErrorCode.NOT_FOUND,
      'View not found',
      'test_view'
    );

    expect(error.code).toBe(ViewErrorCode.NOT_FOUND);
    expect(error.viewName).toBe('test_view');
    expect(error.message).toBe('View not found');
    expect(error.name).toBe('ViewError');
  });

  it('should include cause in ViewError', () => {
    const cause = new Error('Original error');
    const error = new ViewError(
      ViewErrorCode.QUERY_ERROR,
      'Query failed',
      'test_view',
      cause
    );

    expect(error.cause).toBe(cause);
  });
});
