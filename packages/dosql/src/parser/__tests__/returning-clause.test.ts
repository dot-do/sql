/**
 * DoSQL RETURNING Clause Parser Tests - RED Phase TDD
 *
 * These tests document EXPECTED SQLite-compatible RETURNING clause behavior
 * that is NOT YET FULLY IMPLEMENTED.
 *
 * TDD Workflow:
 * 1. RED: Tests fail (current state)
 * 2. GREEN: Implement features to make tests pass
 * 3. REFACTOR: Clean up implementation
 *
 * SQLite RETURNING Clause Reference (added in SQLite 3.35.0):
 * - Syntax: INSERT/UPDATE/DELETE ... RETURNING expr [AS alias], ...
 * - RETURNING * returns all columns
 * - Expressions can include functions, arithmetic, string operations
 * - Subqueries in RETURNING are supported
 * - CASE expressions in RETURNING are supported
 *
 * Issue Reference: sql-zhy.5
 *
 * @packageDocumentation
 */

import { describe, it, expect } from 'vitest';
import {
  parseDML,
  parseInsert,
  parseUpdate,
  parseDelete,
  parseReplace,
} from '../dml.js';
import type {
  InsertStatement,
  UpdateStatement,
  DeleteStatement,
  ReplaceStatement,
  LiteralExpression,
  ColumnReference,
  FunctionCall,
  BinaryExpression,
  UnaryExpression,
  SubqueryExpression,
  CaseExpression,
  ReturningClause,
  ReturningColumn,
} from '../dml-types.js';
import { isParseSuccess, isParseError } from '../dml-types.js';

// =============================================================================
// INSERT ... RETURNING * Tests
// =============================================================================

describe('RETURNING Clause: INSERT ... RETURNING *', () => {
  it('should parse INSERT with RETURNING * and mark wildcard for schema expansion', () => {
    // Gap: Parser should track metadata about wildcard expansion requirements
    const result = parseInsert(
      "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com') RETURNING *"
    );

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.returning).toBeDefined();
    expect(result.statement.returning?.columns[0].expression).toBe('*');
    // This property doesn't exist yet - documenting expected feature
    // When implemented, wildcard should have metadata for schema-aware expansion
    expect((result.statement.returning as any).requiresWildcardExpansion).toBe(true);
  });

  it('should parse INSERT DEFAULT VALUES with RETURNING * (gap: DEFAULT VALUES source)', () => {
    // Gap: DEFAULT VALUES source type combined with RETURNING
    const result = parseInsert('INSERT INTO logs DEFAULT VALUES RETURNING *');

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.source.type).toBe('insert_default');
    expect(result.statement.returning?.columns[0].expression).toBe('*');
  });
});

// =============================================================================
// INSERT ... RETURNING column1, column2 Tests
// =============================================================================

describe('RETURNING Clause: INSERT ... RETURNING column1, column2', () => {
  it('should parse RETURNING with backtick-quoted identifiers (gap: hyphenated names)', () => {
    // Gap: Backtick-quoted identifiers with special characters in RETURNING
    const result = parseInsert(
      "INSERT INTO `my-table` (`col-1`, `col-2`) VALUES (1, 2) RETURNING `col-1`, `col-2`"
    );

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect((cols[0].expression as ColumnReference).name).toBe('col-1');
    expect((cols[1].expression as ColumnReference).name).toBe('col-2');
  });

  it('should parse RETURNING with bracket-quoted identifiers (gap: SQL Server style)', () => {
    // Gap: SQL Server style [brackets] in RETURNING columns
    const result = parseInsert(
      'INSERT INTO [my-table] ([col-1], [col-2]) VALUES (1, 2) RETURNING [col-1], [col-2]'
    );

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect((cols[0].expression as ColumnReference).name).toBe('col-1');
  });
});

// =============================================================================
// INSERT ... RETURNING id AS new_id Tests
// =============================================================================

describe('RETURNING Clause: INSERT ... RETURNING id AS new_id', () => {
  it('should parse RETURNING with quoted aliases containing hyphens (gap: special char aliases)', () => {
    // Gap: Quoted aliases with hyphens/special characters
    const result = parseInsert(
      'INSERT INTO users (name) VALUES (\'Test\') RETURNING id AS "new-id", name AS "user-name"'
    );

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect(cols[0].alias).toBe('new-id');
    expect(cols[1].alias).toBe('user-name');
  });

  it('should parse RETURNING with implicit alias (gap: alias without AS)', () => {
    // Gap: SQLite allows alias without AS keyword
    const result = parseInsert(
      "INSERT INTO users (name) VALUES ('Test') RETURNING id new_id, name user_name"
    );

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect(cols[0].alias).toBe('new_id');
    expect(cols[1].alias).toBe('user_name');
  });
});

// =============================================================================
// UPDATE ... RETURNING * Tests
// =============================================================================

describe('RETURNING Clause: UPDATE ... RETURNING *', () => {
  it('should parse UPDATE FROM join with RETURNING * (gap: FROM clause)', () => {
    // Gap: UPDATE ... FROM ... RETURNING syntax
    const result = parseUpdate(`
      UPDATE inventory
      SET quantity = inventory.quantity - orders.qty
      FROM orders
      WHERE inventory.product_id = orders.product_id
      RETURNING *
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.from).toBeDefined();
    expect(result.statement.returning?.columns[0].expression).toBe('*');
  });

  it('should parse UPDATE with datetime function in SET and RETURNING (gap: datetime in SET)', () => {
    // Gap: datetime('now') function in SET clause combined with RETURNING
    const result = parseUpdate(`
      UPDATE stats
      SET count = count + 1,
          updated_at = datetime('now')
      WHERE id = 1
      RETURNING id, count, updated_at
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.returning?.columns).toHaveLength(3);
  });
});

// =============================================================================
// UPDATE ... RETURNING column reference patterns
// =============================================================================

describe('RETURNING Clause: UPDATE column reference patterns', () => {
  it('should parse RETURNING with table alias qualified columns (gap: alias.column)', () => {
    // Gap: Table alias qualification in RETURNING (u.id, u.name)
    const result = parseUpdate(`
      UPDATE users AS u
      SET u.name = 'New Name'
      WHERE u.id = 1
      RETURNING u.id, u.name, u.updated_at
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect((cols[0].expression as ColumnReference).table).toBe('u');
    expect((cols[0].expression as ColumnReference).name).toBe('id');
  });

  it('should parse RETURNING with schema.table.column (gap: three-part names)', () => {
    // Gap: Three-part qualified column names in RETURNING
    const result = parseUpdate(`
      UPDATE main.users
      SET name = 'Test'
      WHERE id = 1
      RETURNING main.users.id, main.users.name
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    // Should handle schema.table.column format
    const cols = result.statement.returning?.columns!;
    expect(cols.length).toBeGreaterThan(0);
  });
});

// =============================================================================
// DELETE ... RETURNING * Tests
// =============================================================================

describe('RETURNING Clause: DELETE ... RETURNING *', () => {
  it('should parse DELETE with INDEXED BY and RETURNING (gap: INDEXED BY hint)', () => {
    // Gap: INDEXED BY clause combined with RETURNING
    const result = parseDelete(`
      DELETE FROM logs INDEXED BY idx_logs_timestamp
      WHERE timestamp < datetime('now', '-30 days')
      RETURNING *
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.returning).toBeDefined();
  });

  it('should parse DELETE with NOT INDEXED and RETURNING (gap: NOT INDEXED hint)', () => {
    // Gap: NOT INDEXED clause combined with RETURNING
    const result = parseDelete(`
      DELETE FROM cache NOT INDEXED
      WHERE expired = 1
      RETURNING id, key
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.returning?.columns).toHaveLength(2);
  });
});

// =============================================================================
// RETURNING with expressions Tests
// =============================================================================

describe('RETURNING Clause: Expressions', () => {
  it('should parse RETURNING with complex nested arithmetic (gap: operator precedence)', () => {
    // Gap: Complex arithmetic expressions with correct precedence
    const result = parseInsert(`
      INSERT INTO calculations (a, b, c) VALUES (10, 20, 30)
      RETURNING
        id,
        a + b + c AS sum,
        a * b * c AS product,
        (a + b) / c AS ratio,
        a % b AS remainder,
        -a AS negated
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect(cols).toHaveLength(6);
    expect(cols[1].alias).toBe('sum');
    expect(cols[2].alias).toBe('product');
  });

  it('should parse RETURNING with bitwise operators (gap: &, |, <<, >>, ~)', () => {
    // Gap: Bitwise operators in RETURNING expressions
    const result = parseInsert(`
      INSERT INTO flags (value) VALUES (255)
      RETURNING
        id,
        value & 15 AS low_nibble,
        value | 256 AS with_bit,
        value << 2 AS shifted_left,
        value >> 2 AS shifted_right,
        ~value AS inverted
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect(cols).toHaveLength(6);
  });

  it('should parse RETURNING with IN list expression (gap: IN value list)', () => {
    // Gap: IN (value_list) expression in RETURNING
    const result = parseInsert(`
      INSERT INTO items (category) VALUES ('electronics')
      RETURNING
        id,
        category,
        category IN ('electronics', 'appliances', 'tech') AS is_tech
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect(cols[2].alias).toBe('is_tech');
    expect((cols[2].expression as BinaryExpression).operator).toBe('IN');
  });

  it('should parse RETURNING with BETWEEN expression (gap: BETWEEN...AND)', () => {
    // Gap: BETWEEN ... AND expression in RETURNING
    const result = parseInsert(`
      INSERT INTO scores (value) VALUES (75)
      RETURNING
        id,
        value,
        value BETWEEN 70 AND 80 AS is_passing
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect(cols[2].alias).toBe('is_passing');
    expect((cols[2].expression as BinaryExpression).operator).toBe('BETWEEN');
  });

  it('should parse RETURNING with LIKE pattern (gap: LIKE operator)', () => {
    // Gap: LIKE pattern expression in RETURNING
    const result = parseInsert(`
      INSERT INTO files (name) VALUES ('document.pdf')
      RETURNING
        id,
        name,
        name LIKE '%.pdf' AS is_pdf
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect(cols[2].alias).toBe('is_pdf');
    expect((cols[2].expression as BinaryExpression).operator).toBe('LIKE');
  });

  it('should parse RETURNING with GLOB pattern (gap: GLOB operator)', () => {
    // Gap: GLOB pattern expression in RETURNING
    const result = parseInsert(`
      INSERT INTO paths (path) VALUES ('/home/user/file.txt')
      RETURNING
        id,
        path,
        path GLOB '*/user/*' AS is_user_file
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect(cols[2].alias).toBe('is_user_file');
    expect((cols[2].expression as BinaryExpression).operator).toBe('GLOB');
  });

  it('should parse RETURNING with IS NULL / IS NOT NULL (gap: IS operator)', () => {
    // Gap: IS NULL and IS NOT NULL expressions in RETURNING
    const result = parseInsert(`
      INSERT INTO data (value, optional) VALUES (100, NULL)
      RETURNING
        id,
        value IS NOT NULL AS has_value,
        optional IS NULL AS no_optional
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect(cols[1].alias).toBe('has_value');
    expect(cols[2].alias).toBe('no_optional');
  });

  it('should parse RETURNING with deeply nested parentheses (gap: paren nesting)', () => {
    // Gap: Deeply nested parentheses in RETURNING
    const result = parseInsert(`
      INSERT INTO calc (a, b, c) VALUES (1, 2, 3)
      RETURNING
        id,
        ((a + b) * (c + 1)) / ((a * b) + 1) AS complex_calc
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.returning?.columns).toHaveLength(2);
    expect(result.statement.returning?.columns[1].alias).toBe('complex_calc');
  });

  it('should parse RETURNING with unary operators on columns (gap: unary +/-)', () => {
    // Gap: Unary +/- operators in RETURNING
    const result = parseInsert(`
      INSERT INTO amounts (value) VALUES (100)
      RETURNING id, value, -value AS negated, +value AS explicit_positive
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect(cols[2].alias).toBe('negated');
    expect((cols[2].expression as UnaryExpression).operator).toBe('-');
    expect(cols[3].alias).toBe('explicit_positive');
  });

  it('should parse RETURNING with NOT boolean operator (gap: NOT unary)', () => {
    // Gap: NOT unary operator in RETURNING
    const result = parseInsert(`
      INSERT INTO flags (active) VALUES (1)
      RETURNING id, active, NOT active AS inactive
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect(cols[2].alias).toBe('inactive');
    expect((cols[2].expression as UnaryExpression).operator).toBe('NOT');
  });
});

// =============================================================================
// RETURNING with subqueries Tests
// =============================================================================

describe('RETURNING Clause: Subqueries', () => {
  it('should parse RETURNING with scalar subquery (gap: subquery expression)', () => {
    // Gap: Scalar subquery in RETURNING
    const result = parseInsert(`
      INSERT INTO orders (user_id, product_id) VALUES (1, 100)
      RETURNING
        id,
        user_id,
        (SELECT name FROM users WHERE users.id = orders.user_id) AS user_name
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect((cols[2].expression as SubqueryExpression).type).toBe('subquery');
    expect(cols[2].alias).toBe('user_name');
  });

  it('should parse RETURNING with IN subquery (gap: IN + subquery)', () => {
    // Gap: IN (subquery) in RETURNING
    const result = parseInsert(`
      INSERT INTO products (category_id, name) VALUES (5, 'Widget')
      RETURNING
        id,
        name,
        category_id IN (SELECT id FROM active_categories) AS in_active_category
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect(cols[2].alias).toBe('in_active_category');
  });

  it('should parse RETURNING with correlated subquery (gap: correlation)', () => {
    // Gap: Correlated subqueries in RETURNING
    const result = parseUpdate(`
      UPDATE orders
      SET status = 'shipped'
      WHERE id = 1
      RETURNING
        id,
        (SELECT COUNT(*) FROM order_items WHERE order_items.order_id = orders.id) AS item_count,
        (SELECT SUM(price) FROM order_items WHERE order_items.order_id = orders.id) AS total
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect(cols[1].alias).toBe('item_count');
    expect(cols[2].alias).toBe('total');
  });
});

// =============================================================================
// Batch operations Tests
// =============================================================================

describe('RETURNING Clause: Batch Operations', () => {
  it('should parse multi-row INSERT with RETURNING expressions (gap: batch + expressions)', () => {
    // Gap: Multi-value INSERT with computed RETURNING expressions
    const result = parseInsert(`
      INSERT INTO events (name, priority)
      VALUES ('Event A', 1), ('Event B', 2), ('Event C', 3)
      RETURNING id, name, priority, id * priority AS weighted_id
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.returning?.columns).toHaveLength(4);
    expect(result.statement.returning?.columns[3].alias).toBe('weighted_id');
    if (result.statement.source.type === 'values_list') {
      expect(result.statement.source.rows).toHaveLength(3);
    }
  });

  it('should parse INSERT SELECT with RETURNING functions (gap: INSERT SELECT + RETURNING)', () => {
    // Gap: INSERT ... SELECT with functions in RETURNING
    const result = parseInsert(`
      INSERT INTO archive (user_id, name, archived_at)
      SELECT id, name, datetime('now')
      FROM users
      WHERE active = 0
      RETURNING id AS archive_id, user_id, name, LENGTH(name) AS name_length
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.source.type).toBe('insert_select');
    expect(result.statement.returning?.columns).toHaveLength(4);
    expect(result.statement.returning?.columns[3].alias).toBe('name_length');
  });

  it('should parse UPDATE with complex WHERE and RETURNING (gap: complex WHERE)', () => {
    // Gap: Complex UPDATE with function-heavy RETURNING
    const result = parseUpdate(`
      UPDATE tasks
      SET status = 'completed',
          completed_at = datetime('now')
      WHERE status = 'in_progress' AND due_date < date('now')
      RETURNING id, title, completed_at, julianday(completed_at) - julianday(created_at) AS days_to_complete
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.returning?.columns).toHaveLength(4);
    expect(result.statement.returning?.columns[3].alias).toBe('days_to_complete');
  });

  it('should parse DELETE with datetime WHERE and RETURNING (gap: datetime in WHERE)', () => {
    // Gap: DELETE with datetime functions in WHERE and RETURNING
    const result = parseDelete(`
      DELETE FROM notifications
      WHERE read = 1 AND created_at < datetime('now', '-7 days')
      RETURNING id, user_id, message, created_at
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.returning?.columns).toHaveLength(4);
  });
});

// =============================================================================
// Edge Cases Tests
// =============================================================================

describe('RETURNING Clause: Edge Cases', () => {
  it('should parse RETURNING with TYPEOF function (gap: TYPEOF)', () => {
    // Gap: TYPEOF() in RETURNING
    const result = parseInsert(`
      INSERT INTO mixed (value) VALUES (42)
      RETURNING id, value, TYPEOF(value) AS value_type
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect((cols[2].expression as FunctionCall).name.toUpperCase()).toBe('TYPEOF');
  });

  it('should parse RETURNING with IIF function (gap: IIF ternary)', () => {
    // Gap: IIF(condition, true_val, false_val) in RETURNING
    const result = parseInsert(`
      INSERT INTO scores (points) VALUES (85)
      RETURNING
        id,
        points,
        IIF(points >= 70, 'Pass', 'Fail') AS result
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect((cols[2].expression as FunctionCall).name.toUpperCase()).toBe('IIF');
  });

  it('should parse RETURNING with NULLIF function (gap: NULLIF)', () => {
    // Gap: NULLIF(a, b) in RETURNING
    const result = parseInsert(`
      INSERT INTO data (code) VALUES ('N/A')
      RETURNING id, code, NULLIF(code, 'N/A') AS nullable_code
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect((cols[2].expression as FunctionCall).name.toUpperCase()).toBe('NULLIF');
  });

  it('should parse RETURNING with IFNULL function (gap: IFNULL)', () => {
    // Gap: IFNULL(a, b) in RETURNING
    const result = parseInsert(`
      INSERT INTO settings (value, fallback) VALUES (NULL, 'default')
      RETURNING
        id,
        IFNULL(value, fallback) AS effective_value,
        COALESCE(value, fallback, 'none') AS final_value
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect(cols[1].alias).toBe('effective_value');
    expect(cols[2].alias).toBe('final_value');
  });
});

// =============================================================================
// REPLACE Statement Tests
// =============================================================================

describe('RETURNING Clause: REPLACE Statement', () => {
  it('should parse REPLACE with multiple RETURNING expressions (gap: REPLACE expressions)', () => {
    // Gap: Complex expressions in REPLACE RETURNING
    const result = parseReplace(`
      REPLACE INTO counters (id, name, count)
      VALUES (1, 'page_views', 100)
      RETURNING
        id,
        name,
        count,
        count * 2 AS doubled,
        UPPER(name) AS upper_name
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.returning?.columns).toHaveLength(5);
  });

  it('should parse REPLACE with subquery in RETURNING (gap: REPLACE + subquery)', () => {
    // Gap: Subquery in REPLACE RETURNING
    const result = parseReplace(`
      REPLACE INTO user_settings (user_id, setting, value)
      VALUES (1, 'theme', 'dark')
      RETURNING
        user_id,
        setting,
        value,
        (SELECT name FROM users WHERE id = user_settings.user_id) AS user_name
    `);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.returning?.columns).toHaveLength(4);
  });
});

// =============================================================================
// Error Validation Tests - Features that SHOULD produce errors
// =============================================================================

describe('RETURNING Clause: Error Validation', () => {
  it('should reject window functions in RETURNING (gap: window function validation)', () => {
    // Window functions are NOT allowed in RETURNING per SQL standard
    const result = parseInsert(`
      INSERT INTO data (value) VALUES (100)
      RETURNING id, value, ROW_NUMBER() OVER () AS row_num
    `);

    // This should produce a parse error - window functions not allowed
    expect(isParseError(result)).toBe(true);
  });

  it('should reject aggregate functions in RETURNING (gap: aggregate validation)', () => {
    // Aggregate functions without GROUP BY context are NOT allowed in RETURNING
    const result = parseInsert(`
      INSERT INTO data (value) VALUES (100)
      RETURNING id, SUM(value) AS total
    `);

    // This should produce a parse error - aggregates not allowed
    expect(isParseError(result)).toBe(true);
  });
});
