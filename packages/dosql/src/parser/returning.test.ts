/**
 * DoSQL RETURNING Clause Tests
 *
 * Comprehensive TDD tests for RETURNING clause support in INSERT, UPDATE, DELETE statements.
 * Tests cover parsing, AST generation, and execution result verification.
 *
 * Target: 40+ tests covering all RETURNING clause variations
 */

import { describe, it, expect } from 'vitest';
import {
  parseDML,
  parseInsert,
  parseUpdate,
  parseDelete,
  parseReplace,
} from './dml.js';
import type {
  InsertStatement,
  UpdateStatement,
  DeleteStatement,
  ReplaceStatement,
  LiteralExpression,
  ColumnReference,
  FunctionCall,
  BinaryExpression,
  ReturningClause,
  ReturningColumn,
} from './dml-types.js';
import {
  isParseSuccess,
  isParseError,
} from './dml-types.js';

// =============================================================================
// INSERT ... RETURNING Tests
// =============================================================================

describe('INSERT RETURNING Clause', () => {
  describe('RETURNING *', () => {
    it('should parse INSERT with RETURNING * (return all columns)', () => {
      const result = parseInsert("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com') RETURNING *");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning).toBeDefined();
      expect(result.statement.returning?.columns).toHaveLength(1);
      expect(result.statement.returning?.columns[0].expression).toBe('*');
    });

    it('should parse INSERT ... VALUES with RETURNING * and trailing semicolon', () => {
      const result = parseInsert("INSERT INTO products (name, price) VALUES ('Widget', 19.99) RETURNING *;");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning?.columns[0].expression).toBe('*');
      expect(result.remaining).toBe('');
    });

    it('should parse multi-row INSERT with RETURNING *', () => {
      const result = parseInsert(`
        INSERT INTO items (name, qty) VALUES
        ('Item A', 10),
        ('Item B', 20),
        ('Item C', 30)
        RETURNING *
      `);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning?.columns[0].expression).toBe('*');
      if (result.statement.source.type === 'values_list') {
        expect(result.statement.source.rows).toHaveLength(3);
      }
    });
  });

  describe('RETURNING specific columns', () => {
    it('should parse INSERT with RETURNING single column', () => {
      const result = parseInsert("INSERT INTO users (name) VALUES ('Bob') RETURNING id");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning?.columns).toHaveLength(1);
      const col = result.statement.returning?.columns[0];
      expect(col?.expression).not.toBe('*');
      expect((col?.expression as ColumnReference).type).toBe('column');
      expect((col?.expression as ColumnReference).name).toBe('id');
    });

    it('should parse INSERT with RETURNING multiple columns', () => {
      const result = parseInsert("INSERT INTO users (name, email) VALUES ('Alice', 'a@b.com') RETURNING id, created_at, name");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning?.columns).toHaveLength(3);

      const cols = result.statement.returning?.columns!;
      expect((cols[0].expression as ColumnReference).name).toBe('id');
      expect((cols[1].expression as ColumnReference).name).toBe('created_at');
      expect((cols[2].expression as ColumnReference).name).toBe('name');
    });

    it('should parse INSERT with RETURNING qualified column names', () => {
      const result = parseInsert("INSERT INTO users (name) VALUES ('Test') RETURNING users.id, users.created_at");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const cols = result.statement.returning?.columns!;
      expect((cols[0].expression as ColumnReference).table).toBe('users');
      expect((cols[0].expression as ColumnReference).name).toBe('id');
    });
  });

  describe('RETURNING with aliases', () => {
    it('should parse INSERT with RETURNING column AS alias', () => {
      const result = parseInsert("INSERT INTO users (name) VALUES ('Test') RETURNING id AS user_id");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning?.columns[0].alias).toBe('user_id');
    });

    it('should parse INSERT with RETURNING multiple aliased columns', () => {
      const result = parseInsert(`
        INSERT INTO orders (user_id, total)
        VALUES (1, 99.99)
        RETURNING id AS order_id, created_at AS placed_at, total AS order_total
      `);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const cols = result.statement.returning?.columns!;
      expect(cols).toHaveLength(3);
      expect(cols[0].alias).toBe('order_id');
      expect(cols[1].alias).toBe('placed_at');
      expect(cols[2].alias).toBe('order_total');
    });

    it('should parse INSERT with RETURNING mixed aliased and non-aliased columns', () => {
      const result = parseInsert("INSERT INTO t (a) VALUES (1) RETURNING id, name AS full_name, email");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const cols = result.statement.returning?.columns!;
      expect(cols[0].alias).toBeUndefined();
      expect(cols[1].alias).toBe('full_name');
      expect(cols[2].alias).toBeUndefined();
    });
  });

  describe('RETURNING with expressions', () => {
    it('should parse INSERT with RETURNING function call', () => {
      const result = parseInsert("INSERT INTO users (name) VALUES ('Test') RETURNING id, UPPER(name)");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const cols = result.statement.returning?.columns!;
      expect(cols).toHaveLength(2);
      expect((cols[1].expression as FunctionCall).type).toBe('function');
      expect((cols[1].expression as FunctionCall).name).toBe('UPPER');
    });

    it('should parse INSERT with RETURNING function call with alias', () => {
      const result = parseInsert("INSERT INTO users (name) VALUES ('Test') RETURNING id, LOWER(email) AS normalized_email");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const cols = result.statement.returning?.columns!;
      expect((cols[1].expression as FunctionCall).name).toBe('LOWER');
      expect(cols[1].alias).toBe('normalized_email');
    });

    it('should parse INSERT with RETURNING arithmetic expression', () => {
      const result = parseInsert("INSERT INTO products (price) VALUES (100) RETURNING id, price * 1.1 AS price_with_tax");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const cols = result.statement.returning?.columns!;
      expect((cols[1].expression as BinaryExpression).type).toBe('binary');
      expect((cols[1].expression as BinaryExpression).operator).toBe('*');
      expect(cols[1].alias).toBe('price_with_tax');
    });

    it('should parse INSERT with RETURNING string concatenation', () => {
      const result = parseInsert("INSERT INTO users (first_name, last_name) VALUES ('John', 'Doe') RETURNING id, first_name || ' ' || last_name AS full_name");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const cols = result.statement.returning?.columns!;
      expect(cols[1].alias).toBe('full_name');
      expect((cols[1].expression as BinaryExpression).operator).toBe('||');
    });

    it('should parse INSERT with RETURNING COALESCE function', () => {
      const result = parseInsert("INSERT INTO users (name, nickname) VALUES ('Test', NULL) RETURNING id, COALESCE(nickname, name) AS display_name");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const cols = result.statement.returning?.columns!;
      expect((cols[1].expression as FunctionCall).name).toBe('COALESCE');
      expect((cols[1].expression as FunctionCall).args).toHaveLength(2);
    });

    it('should parse INSERT with RETURNING datetime function', () => {
      const result = parseInsert("INSERT INTO logs (message) VALUES ('test') RETURNING id, created_at, DATETIME(created_at) AS formatted_time");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const cols = result.statement.returning?.columns!;
      expect(cols).toHaveLength(3);
      expect((cols[2].expression as FunctionCall).name).toBe('DATETIME');
    });
  });

  describe('INSERT with ON CONFLICT and RETURNING', () => {
    it('should parse INSERT with ON CONFLICT DO NOTHING and RETURNING', () => {
      const result = parseInsert("INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT DO NOTHING RETURNING *");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.onConflict).toBeDefined();
      expect(result.statement.onConflict?.action.type).toBe('do_nothing');
      expect(result.statement.returning?.columns[0].expression).toBe('*');
    });

    it('should parse INSERT with ON CONFLICT DO UPDATE and RETURNING', () => {
      const result = parseInsert(`
        INSERT INTO counters (id, count) VALUES (1, 1)
        ON CONFLICT (id) DO UPDATE SET count = count + 1
        RETURNING id, count AS new_count
      `);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.onConflict?.action.type).toBe('do_update');
      expect(result.statement.returning?.columns).toHaveLength(2);
      expect(result.statement.returning?.columns[1].alias).toBe('new_count');
    });
  });

  describe('INSERT ... SELECT with RETURNING', () => {
    it('should parse INSERT SELECT with RETURNING', () => {
      const result = parseInsert("INSERT INTO archive SELECT * FROM users WHERE inactive = TRUE RETURNING id");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.source.type).toBe('insert_select');
      expect(result.statement.returning?.columns).toHaveLength(1);
    });
  });

  describe('INSERT OR conflict variants with RETURNING', () => {
    it('should parse INSERT OR REPLACE with RETURNING', () => {
      const result = parseInsert("INSERT OR REPLACE INTO settings (key, value) VALUES ('theme', 'dark') RETURNING *");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.conflict?.action).toBe('REPLACE');
      expect(result.statement.returning).toBeDefined();
    });

    it('should parse INSERT OR IGNORE with RETURNING', () => {
      const result = parseInsert("INSERT OR IGNORE INTO unique_items (code) VALUES ('ABC123') RETURNING id, code");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.conflict?.action).toBe('IGNORE');
      expect(result.statement.returning?.columns).toHaveLength(2);
    });
  });
});

// =============================================================================
// UPDATE ... RETURNING Tests
// =============================================================================

describe('UPDATE RETURNING Clause', () => {
  describe('Basic UPDATE RETURNING', () => {
    it('should parse UPDATE with RETURNING *', () => {
      const result = parseUpdate("UPDATE users SET active = TRUE WHERE id = 1 RETURNING *");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning).toBeDefined();
      expect(result.statement.returning?.columns[0].expression).toBe('*');
    });

    it('should parse UPDATE with RETURNING specific columns', () => {
      const result = parseUpdate("UPDATE products SET price = 29.99 WHERE id = 5 RETURNING id, name, price");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning?.columns).toHaveLength(3);
    });

    it('should parse UPDATE with RETURNING aliased columns', () => {
      const result = parseUpdate("UPDATE users SET name = 'New Name' WHERE id = 1 RETURNING id AS user_id, name AS updated_name");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const cols = result.statement.returning?.columns!;
      expect(cols[0].alias).toBe('user_id');
      expect(cols[1].alias).toBe('updated_name');
    });
  });

  describe('UPDATE RETURNING with expressions', () => {
    it('should parse UPDATE with RETURNING computed expression', () => {
      const result = parseUpdate("UPDATE inventory SET quantity = quantity - 5 WHERE id = 1 RETURNING id, quantity, quantity * unit_price AS total_value");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const cols = result.statement.returning?.columns!;
      expect(cols).toHaveLength(3);
      expect(cols[2].alias).toBe('total_value');
    });

    it('should parse UPDATE with RETURNING function', () => {
      const result = parseUpdate("UPDATE users SET updated_at = NOW() WHERE id = 1 RETURNING id, DATETIME(updated_at) AS update_time");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const cols = result.statement.returning?.columns!;
      expect((cols[1].expression as FunctionCall).name).toBe('DATETIME');
    });
  });

  describe('UPDATE with ORDER BY, LIMIT and RETURNING', () => {
    it('should parse UPDATE with LIMIT and RETURNING', () => {
      const result = parseUpdate("UPDATE jobs SET status = 'processing' WHERE status = 'pending' LIMIT 10 RETURNING id, status");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.limit).toBeDefined();
      expect(result.statement.returning?.columns).toHaveLength(2);
    });

    it('should parse UPDATE with ORDER BY, LIMIT and RETURNING', () => {
      const result = parseUpdate("UPDATE tasks SET processed = TRUE ORDER BY priority DESC LIMIT 5 RETURNING id, priority, processed");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.orderBy).toBeDefined();
      expect(result.statement.limit).toBeDefined();
      expect(result.statement.returning?.columns).toHaveLength(3);
    });
  });

  describe('UPDATE multiple rows RETURNING', () => {
    it('should parse UPDATE without WHERE (all rows) with RETURNING', () => {
      const result = parseUpdate("UPDATE products SET in_stock = FALSE RETURNING id, name, in_stock");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.where).toBeUndefined();
      expect(result.statement.returning?.columns).toHaveLength(3);
    });

    it('should parse UPDATE with IN clause and RETURNING', () => {
      const result = parseUpdate("UPDATE items SET status = 'archived' WHERE id IN (1, 2, 3, 4, 5) RETURNING id, status");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning?.columns).toHaveLength(2);
    });
  });
});

// =============================================================================
// DELETE ... RETURNING Tests
// =============================================================================

describe('DELETE RETURNING Clause', () => {
  describe('Basic DELETE RETURNING', () => {
    it('should parse DELETE with RETURNING *', () => {
      const result = parseDelete("DELETE FROM users WHERE id = 1 RETURNING *");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning).toBeDefined();
      expect(result.statement.returning?.columns[0].expression).toBe('*');
    });

    it('should parse DELETE with RETURNING specific columns', () => {
      const result = parseDelete("DELETE FROM logs WHERE created_at < '2020-01-01' RETURNING id, message, created_at");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning?.columns).toHaveLength(3);
    });

    it('should parse DELETE with RETURNING aliased columns', () => {
      const result = parseDelete("DELETE FROM sessions WHERE expired = TRUE RETURNING id AS session_id, user_id AS affected_user");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const cols = result.statement.returning?.columns!;
      expect(cols[0].alias).toBe('session_id');
      expect(cols[1].alias).toBe('affected_user');
    });
  });

  describe('DELETE RETURNING with expressions', () => {
    it('should parse DELETE with RETURNING computed value', () => {
      const result = parseDelete("DELETE FROM cart_items WHERE cart_id = 1 RETURNING id, quantity, unit_price, quantity * unit_price AS line_total");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const cols = result.statement.returning?.columns!;
      expect(cols).toHaveLength(4);
      expect(cols[3].alias).toBe('line_total');
    });

    it('should parse DELETE with RETURNING string functions', () => {
      const result = parseDelete("DELETE FROM temp_data WHERE age > 30 RETURNING id, UPPER(name) AS name_upper");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const cols = result.statement.returning?.columns!;
      expect((cols[1].expression as FunctionCall).name).toBe('UPPER');
    });
  });

  describe('DELETE with ORDER BY, LIMIT and RETURNING', () => {
    it('should parse DELETE with LIMIT and RETURNING', () => {
      const result = parseDelete("DELETE FROM logs LIMIT 1000 RETURNING id, timestamp");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.limit).toBeDefined();
      expect(result.statement.returning?.columns).toHaveLength(2);
    });

    it('should parse DELETE with ORDER BY and LIMIT and RETURNING', () => {
      const result = parseDelete("DELETE FROM queue ORDER BY priority ASC LIMIT 100 RETURNING id, job_type, priority");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.orderBy).toBeDefined();
      expect(result.statement.limit).toBeDefined();
      expect(result.statement.returning?.columns).toHaveLength(3);
    });

    it('should parse DELETE with LIMIT OFFSET and RETURNING', () => {
      const result = parseDelete("DELETE FROM archive LIMIT 50 OFFSET 100 RETURNING *");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.limit?.offset).toBeDefined();
      expect(result.statement.returning).toBeDefined();
    });
  });

  describe('DELETE multiple rows RETURNING', () => {
    it('should parse DELETE without WHERE (all rows) with RETURNING', () => {
      const result = parseDelete("DELETE FROM temp_table RETURNING *");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.where).toBeUndefined();
      expect(result.statement.returning).toBeDefined();
    });

    it('should parse DELETE with complex WHERE and RETURNING', () => {
      const result = parseDelete("DELETE FROM orders WHERE status = 'cancelled' AND created_at < '2023-01-01' RETURNING id, user_id, total_amount");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning?.columns).toHaveLength(3);
    });
  });
});

// =============================================================================
// REPLACE ... RETURNING Tests
// =============================================================================

describe('REPLACE RETURNING Clause', () => {
  it('should parse REPLACE with RETURNING *', () => {
    const result = parseReplace("REPLACE INTO config (key, value) VALUES ('setting', 'value') RETURNING *");

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.returning).toBeDefined();
    expect(result.statement.returning?.columns[0].expression).toBe('*');
  });

  it('should parse REPLACE with RETURNING specific columns', () => {
    const result = parseReplace("REPLACE INTO settings (name, value) VALUES ('theme', 'dark') RETURNING rowid, name, value");

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.returning?.columns).toHaveLength(3);
  });

  it('should parse REPLACE with RETURNING aliased columns', () => {
    const result = parseReplace("REPLACE INTO counters (id, count) VALUES (1, 100) RETURNING id AS counter_id, count AS new_count");

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect(cols[0].alias).toBe('counter_id');
    expect(cols[1].alias).toBe('new_count');
  });
});

// =============================================================================
// Edge Cases and Error Handling
// =============================================================================

describe('RETURNING Clause Edge Cases', () => {
  describe('Case insensitivity', () => {
    it('should parse RETURNING in lowercase', () => {
      const result = parseInsert("INSERT INTO t (a) VALUES (1) returning id");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning).toBeDefined();
    });

    it('should parse RETURNING in mixed case', () => {
      const result = parseInsert("INSERT INTO t (a) VALUES (1) Returning id");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning).toBeDefined();
    });

    it('should parse AS in lowercase for aliases', () => {
      const result = parseInsert("INSERT INTO t (a) VALUES (1) RETURNING id as myid");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning?.columns[0].alias).toBe('myid');
    });
  });

  describe('Whitespace handling', () => {
    it('should handle extra whitespace around RETURNING', () => {
      const result = parseInsert("INSERT INTO t (a) VALUES (1)    RETURNING    id  ,  name");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning?.columns).toHaveLength(2);
    });

    it('should handle newlines before RETURNING', () => {
      const result = parseInsert(`
        INSERT INTO t (a) VALUES (1)
        RETURNING id, name
      `);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning?.columns).toHaveLength(2);
    });
  });

  describe('Complex expressions in RETURNING', () => {
    it('should parse RETURNING with nested function calls', () => {
      const result = parseInsert("INSERT INTO t (a) VALUES (1) RETURNING id, COALESCE(UPPER(name), 'UNKNOWN') AS display_name");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const expr = result.statement.returning?.columns[1].expression as FunctionCall;
      expect(expr.name).toBe('COALESCE');
    });

    it('should parse RETURNING with CASE expression placeholder', () => {
      // Note: Full CASE support may require additional parser work
      const result = parseInsert("INSERT INTO t (a) VALUES (1) RETURNING id, status");

      expect(isParseSuccess(result)).toBe(true);
    });

    it('should parse RETURNING with multiple arithmetic operations', () => {
      const result = parseInsert("INSERT INTO t (price, qty, tax) VALUES (100, 5, 0.1) RETURNING id, price * qty * (1 + tax) AS total");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning?.columns[1].alias).toBe('total');
    });
  });

  describe('RETURNING without modification', () => {
    it('should allow INSERT with only RETURNING (no ON CONFLICT modifies nothing)', () => {
      const result = parseInsert("INSERT INTO t (id) VALUES (1) ON CONFLICT DO NOTHING RETURNING *");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.onConflict?.action.type).toBe('do_nothing');
      expect(result.statement.returning).toBeDefined();
    });
  });

  describe('Statement type detection with RETURNING', () => {
    it('should correctly identify INSERT with RETURNING via parseDML', () => {
      const result = parseDML("INSERT INTO t (a) VALUES (1) RETURNING *");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.type).toBe('insert');
      expect((result.statement as InsertStatement).returning).toBeDefined();
    });

    it('should correctly identify UPDATE with RETURNING via parseDML', () => {
      const result = parseDML("UPDATE t SET a = 1 RETURNING *");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.type).toBe('update');
      expect((result.statement as UpdateStatement).returning).toBeDefined();
    });

    it('should correctly identify DELETE with RETURNING via parseDML', () => {
      const result = parseDML("DELETE FROM t WHERE id = 1 RETURNING *");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.type).toBe('delete');
      expect((result.statement as DeleteStatement).returning).toBeDefined();
    });
  });
});

// =============================================================================
// RETURNING Clause Structure Validation
// =============================================================================

describe('RETURNING Clause AST Structure', () => {
  it('should create proper ReturningClause structure', () => {
    const result = parseInsert("INSERT INTO t (a) VALUES (1) RETURNING id, name AS n, UPPER(code)");

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const returning = result.statement.returning!;

    // Verify structure
    expect(returning.type).toBe('returning');
    expect(Array.isArray(returning.columns)).toBe(true);
    expect(returning.columns).toHaveLength(3);

    // First column: simple column reference
    const col1 = returning.columns[0];
    expect(col1.expression).not.toBe('*');
    expect((col1.expression as ColumnReference).type).toBe('column');
    expect(col1.alias).toBeUndefined();

    // Second column: column with alias
    const col2 = returning.columns[1];
    expect((col2.expression as ColumnReference).name).toBe('name');
    expect(col2.alias).toBe('n');

    // Third column: function call
    const col3 = returning.columns[2];
    expect((col3.expression as FunctionCall).type).toBe('function');
    expect((col3.expression as FunctionCall).name).toBe('UPPER');
  });

  it('should preserve expression types in RETURNING columns', () => {
    const result = parseInsert("INSERT INTO t (a) VALUES (1) RETURNING 'literal' AS lit, 42 AS num, a + b AS sum");

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;

    // String literal
    expect((cols[0].expression as LiteralExpression).type).toBe('literal');
    expect((cols[0].expression as LiteralExpression).value).toBe('literal');

    // Numeric literal
    expect((cols[1].expression as LiteralExpression).type).toBe('literal');
    expect((cols[1].expression as LiteralExpression).value).toBe(42);

    // Binary expression
    expect((cols[2].expression as BinaryExpression).type).toBe('binary');
  });
});

// =============================================================================
// RETURNING with Table Aliases
// =============================================================================

describe('RETURNING with Table Aliases', () => {
  it('should parse INSERT with table alias and RETURNING', () => {
    const result = parseInsert("INSERT INTO users AS u (name) VALUES ('Test') RETURNING u.id, u.name");

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.alias).toBe('u');
    expect(result.statement.returning).toBeDefined();
  });

  it('should parse UPDATE with table alias and RETURNING', () => {
    const result = parseUpdate("UPDATE products AS p SET p.price = 100 WHERE p.id = 1 RETURNING p.id, p.price");

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.alias).toBe('p');
    expect(result.statement.returning?.columns).toHaveLength(2);
  });

  it('should parse DELETE with table alias and RETURNING', () => {
    const result = parseDelete("DELETE FROM orders AS o WHERE o.status = 'cancelled' RETURNING o.id, o.total");

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.alias).toBe('o');
    expect(result.statement.returning?.columns).toHaveLength(2);
  });
});

// =============================================================================
// Special SQLite RETURNING Features
// =============================================================================

describe('SQLite-specific RETURNING Features', () => {
  it('should parse RETURNING with rowid', () => {
    const result = parseInsert("INSERT INTO t (a) VALUES (1) RETURNING rowid, a");

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect((cols[0].expression as ColumnReference).name).toBe('rowid');
  });

  it('should parse RETURNING with _rowid_', () => {
    const result = parseInsert("INSERT INTO t (a) VALUES (1) RETURNING _rowid_, a");

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect((cols[0].expression as ColumnReference).name).toBe('_rowid_');
  });

  it('should parse RETURNING with oid', () => {
    const result = parseInsert("INSERT INTO t (a) VALUES (1) RETURNING oid, a");

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const cols = result.statement.returning?.columns!;
    expect((cols[0].expression as ColumnReference).name).toBe('oid');
  });
});
