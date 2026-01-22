/**
 * DoSQL RETURNING Clause Execution Tests - RED Phase TDD
 *
 * Comprehensive TDD tests for RETURNING clause execution in INSERT, UPDATE, DELETE statements.
 * These tests verify SQLite-compatible RETURNING behavior when executed against real
 * Durable Object SQLStorage.
 *
 * TDD NOTE: Tests marked with `it.fails` document EXPECTED behavior that is NOT YET IMPLEMENTED.
 * These represent the RED phase of TDD - they should fail until the feature is implemented.
 * Once implemented (GREEN phase), convert `it.fails` to `it`.
 *
 * SQLite RETURNING Reference:
 * - Added in SQLite 3.35.0 (2021-03-12)
 * - Returns data from rows that are modified by INSERT, UPDATE, DELETE
 * - Syntax: INSERT/UPDATE/DELETE ... RETURNING expr1, expr2, ... [AS alias]
 * - RETURNING * returns all columns of the modified row
 * - Expressions can include functions, arithmetic, string operations
 *
 * Uses workers-vitest-pool for real DO stubs (NO MOCKS).
 *
 * Issue Reference: pocs-5sa2
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  getUniqueDoSqlStub,
  executeSQL,
  querySQL,
} from '../../__tests__/do/setup.js';

// =============================================================================
// TEST: INSERT with RETURNING
// =============================================================================

describe('RETURNING Clause Execution: INSERT', () => {
  /**
   * Test: INSERT with RETURNING * returns all columns
   *
   * Expected SQLite behavior:
   * - RETURNING * returns all column values of the inserted row
   * - Includes auto-generated values (e.g., ROWID, autoincrement)
   * - Column order matches table definition
   *
   * Gap: RETURNING clause not executed by DO SQLStorage
   */
  it('INSERT with RETURNING * returns all columns', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP)'
    );

    const result = await executeSQL(
      stub,
      "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com') RETURNING *"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toBeDefined();
    expect(result.rows).toHaveLength(1);

    const row = result.rows![0];
    expect(row.id).toBeDefined(); // Auto-generated primary key
    expect(row.name).toBe('Alice');
    expect(row.email).toBe('alice@example.com');
    expect(row.created_at).toBeDefined(); // DEFAULT value
  });

  /**
   * Test: INSERT with RETURNING specific columns
   *
   * Expected SQLite behavior:
   * - Only specified columns are returned
   * - Column order matches RETURNING clause order
   *
   * Gap: RETURNING clause not executed
   */
  it('INSERT with RETURNING specific columns', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)'
    );

    const result = await executeSQL(
      stub,
      "INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com') RETURNING id, name"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);

    const row = result.rows![0];
    expect(row.id).toBeDefined();
    expect(row.name).toBe('Bob');
    expect(row.email).toBeUndefined(); // Not in RETURNING clause
  });

  /**
   * Test: INSERT with RETURNING computed expression
   *
   * Expected SQLite behavior:
   * - Expressions are evaluated on the inserted row
   * - Can use column values in expressions
   *
   * Gap: Expression evaluation in RETURNING not implemented
   */
  it('INSERT with RETURNING computed expression', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, quantity INTEGER)'
    );

    const result = await executeSQL(
      stub,
      "INSERT INTO products (name, price, quantity) VALUES ('Widget', 19.99, 10) RETURNING id, name, price * quantity AS total_value"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);

    const row = result.rows![0];
    expect(row.total_value).toBeCloseTo(199.9);
  });

  /**
   * Test: Batch INSERT with RETURNING (multiple rows)
   *
   * Expected SQLite behavior:
   * - Returns one result row per inserted row
   * - Order matches insertion order
   *
   * Gap: Multi-row INSERT RETURNING not implemented
   */
  it('INSERT batch with RETURNING returns all inserted rows', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)'
    );

    const result = await executeSQL(
      stub,
      `INSERT INTO items (name) VALUES ('Item A'), ('Item B'), ('Item C') RETURNING *`
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(3);

    expect(result.rows![0].name).toBe('Item A');
    expect(result.rows![1].name).toBe('Item B');
    expect(result.rows![2].name).toBe('Item C');

    // IDs should be auto-generated and sequential
    expect(result.rows![0].id as number).toBeLessThan(result.rows![1].id as number);
    expect(result.rows![1].id as number).toBeLessThan(result.rows![2].id as number);
  });

  /**
   * Test: INSERT ON CONFLICT with RETURNING
   *
   * Expected SQLite behavior:
   * - Returns the row after conflict resolution
   * - DO UPDATE: returns updated values
   * - DO NOTHING: returns nothing (no row modified)
   *
   * Gap: ON CONFLICT with RETURNING not implemented
   */
  it('INSERT ON CONFLICT DO UPDATE with RETURNING shows updated values', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE counters (id INTEGER PRIMARY KEY, count INTEGER DEFAULT 0)'
    );

    // Initial insert
    await executeSQL(
      stub,
      'INSERT INTO counters (id, count) VALUES (1, 1)'
    );

    // Upsert with RETURNING
    const result = await executeSQL(
      stub,
      'INSERT INTO counters (id, count) VALUES (1, 1) ON CONFLICT (id) DO UPDATE SET count = count + 1 RETURNING id, count'
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].count).toBe(2); // Should be incremented
  });

  /**
   * Test: INSERT ON CONFLICT DO NOTHING with RETURNING
   *
   * Expected SQLite behavior:
   * - DO NOTHING means no row is modified
   * - RETURNING should return empty result set
   *
   * Gap: ON CONFLICT DO NOTHING + RETURNING interaction not implemented
   */
  it('INSERT ON CONFLICT DO NOTHING with RETURNING returns empty when conflict', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE unique_items (code TEXT PRIMARY KEY, name TEXT)'
    );

    // Initial insert
    await executeSQL(
      stub,
      "INSERT INTO unique_items (code, name) VALUES ('ABC', 'First')"
    );

    // Conflict with DO NOTHING
    const result = await executeSQL(
      stub,
      "INSERT INTO unique_items (code, name) VALUES ('ABC', 'Second') ON CONFLICT DO NOTHING RETURNING *"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(0); // No row modified, so nothing returned
  });

  /**
   * Test: RETURNING with column aliases
   *
   * Expected SQLite behavior:
   * - AS keyword creates column alias in result
   * - Result uses alias as column name
   *
   * Gap: Alias handling in RETURNING not implemented
   */
  it('INSERT with RETURNING column aliases', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL)'
    );

    const result = await executeSQL(
      stub,
      "INSERT INTO orders (user_id, total) VALUES (42, 99.99) RETURNING id AS order_id, user_id AS customer_id, total AS order_total"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);

    const row = result.rows![0];
    expect(row.order_id).toBeDefined();
    expect(row.customer_id).toBe(42);
    expect(row.order_total).toBeCloseTo(99.99);

    // Original column names should NOT be present
    expect(row.id).toBeUndefined();
    expect(row.user_id).toBeUndefined();
    expect(row.total).toBeUndefined();
  });

  /**
   * Test: RETURNING rowid/id for tables without explicit PRIMARY KEY
   *
   * Expected SQLite behavior:
   * - SQLite always has rowid unless WITHOUT ROWID
   * - Can return rowid even if not explicitly defined
   *
   * Gap: rowid handling in RETURNING not implemented
   */
  it('INSERT with RETURNING rowid', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE logs (message TEXT, timestamp TEXT DEFAULT CURRENT_TIMESTAMP)'
    );

    const result = await executeSQL(
      stub,
      "INSERT INTO logs (message) VALUES ('Test log entry') RETURNING rowid, message"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].rowid).toBeDefined();
    expect(typeof result.rows![0].rowid).toBe('number');
  });

  /**
   * Test: RETURNING with _rowid_ alias
   *
   * Expected SQLite behavior:
   * - _rowid_ is alias for rowid
   * - Should work identically to rowid
   *
   * Gap: _rowid_ handling not implemented
   */
  it('INSERT with RETURNING _rowid_', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE events (type TEXT, data TEXT)'
    );

    const result = await executeSQL(
      stub,
      "INSERT INTO events (type, data) VALUES ('click', 'button1') RETURNING _rowid_, type"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0]._rowid_).toBeDefined();
  });

  /**
   * Test: RETURNING with oid alias
   *
   * Expected SQLite behavior:
   * - oid is another alias for rowid
   * - Should work identically to rowid
   *
   * Gap: oid handling not implemented
   */
  it('INSERT with RETURNING oid', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE metrics (name TEXT, value REAL)'
    );

    const result = await executeSQL(
      stub,
      "INSERT INTO metrics (name, value) VALUES ('cpu_usage', 45.5) RETURNING oid, name, value"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].oid).toBeDefined();
  });
});

// =============================================================================
// TEST: UPDATE with RETURNING
// =============================================================================

describe('RETURNING Clause Execution: UPDATE', () => {
  /**
   * Test: UPDATE with RETURNING * returns all updated rows
   *
   * Expected SQLite behavior:
   * - Returns all columns of each updated row
   * - Values reflect post-update state
   *
   * Gap: RETURNING in UPDATE not implemented
   */
  it('UPDATE with RETURNING * returns updated rows', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, status TEXT)'
    );

    await executeSQL(stub, "INSERT INTO users (name, status) VALUES ('Alice', 'pending')");
    await executeSQL(stub, "INSERT INTO users (name, status) VALUES ('Bob', 'pending')");

    const result = await executeSQL(
      stub,
      "UPDATE users SET status = 'active' WHERE status = 'pending' RETURNING *"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(2);

    for (const row of result.rows!) {
      expect(row.status).toBe('active'); // New value
    }
  });

  /**
   * Test: UPDATE with RETURNING old and new values pattern
   *
   * Note: SQLite RETURNING only shows the NEW value after update.
   * To get old values, you'd need to use a different approach (subquery, trigger).
   * This test verifies RETURNING shows post-update values.
   *
   * Gap: RETURNING in UPDATE not implemented
   */
  it('UPDATE with RETURNING shows new values (not old)', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE inventory (id INTEGER PRIMARY KEY, product TEXT, quantity INTEGER)'
    );

    await executeSQL(stub, "INSERT INTO inventory (product, quantity) VALUES ('Widget', 100)");

    const result = await executeSQL(
      stub,
      "UPDATE inventory SET quantity = quantity - 10 WHERE product = 'Widget' RETURNING id, product, quantity"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].quantity).toBe(90); // New value after decrement
  });

  /**
   * Test: UPDATE with RETURNING computed expression
   *
   * Expected SQLite behavior:
   * - Can compute expressions using updated values
   *
   * Gap: Expression in UPDATE RETURNING not implemented
   */
  it('UPDATE with RETURNING computed expression', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE prices (id INTEGER PRIMARY KEY, item TEXT, price REAL, quantity INTEGER)'
    );

    await executeSQL(stub, "INSERT INTO prices (item, price, quantity) VALUES ('Gadget', 50.00, 5)");

    const result = await executeSQL(
      stub,
      "UPDATE prices SET price = price * 1.1 WHERE item = 'Gadget' RETURNING id, item, price, price * quantity AS new_total"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].price).toBeCloseTo(55.00); // 50 * 1.1
    expect(result.rows![0].new_total).toBeCloseTo(275.00); // 55 * 5
  });

  /**
   * Test: UPDATE with RETURNING when no rows match
   *
   * Expected SQLite behavior:
   * - No rows updated means no rows returned
   * - Operation should succeed with empty result set
   *
   * Gap: Empty result handling in RETURNING not implemented
   */
  it('UPDATE with RETURNING returns empty when no rows match', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT, done INTEGER)'
    );

    await executeSQL(stub, "INSERT INTO tasks (title, done) VALUES ('Task 1', 1)");

    const result = await executeSQL(
      stub,
      "UPDATE tasks SET done = 1 WHERE done = 0 RETURNING *"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(0);
  });

  /**
   * Test: UPDATE with LIMIT and RETURNING
   *
   * Expected SQLite behavior:
   * - LIMIT restricts number of rows updated
   * - RETURNING shows only the rows that were actually updated
   *
   * Gap: LIMIT + RETURNING interaction not implemented
   */
  it('UPDATE with LIMIT and RETURNING', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE jobs (id INTEGER PRIMARY KEY, status TEXT, priority INTEGER)'
    );

    await executeSQL(stub, "INSERT INTO jobs (status, priority) VALUES ('pending', 1)");
    await executeSQL(stub, "INSERT INTO jobs (status, priority) VALUES ('pending', 2)");
    await executeSQL(stub, "INSERT INTO jobs (status, priority) VALUES ('pending', 3)");

    const result = await executeSQL(
      stub,
      "UPDATE jobs SET status = 'processing' WHERE status = 'pending' ORDER BY priority DESC LIMIT 2 RETURNING *"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(2);

    // Verify the two highest priority jobs were updated
    const priorities = result.rows!.map((r) => r.priority);
    expect(priorities).toContain(3);
    expect(priorities).toContain(2);
    expect(priorities).not.toContain(1);
  });
});

// =============================================================================
// TEST: DELETE with RETURNING
// =============================================================================

describe('RETURNING Clause Execution: DELETE', () => {
  /**
   * Test: DELETE with RETURNING * returns deleted rows
   *
   * Expected SQLite behavior:
   * - Returns all columns of each deleted row
   * - Values are from BEFORE deletion
   * - Rows are actually deleted from table
   *
   * Gap: RETURNING in DELETE not implemented
   */
  it('DELETE with RETURNING * returns deleted rows', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE sessions (id INTEGER PRIMARY KEY, user_id INTEGER, expired INTEGER)'
    );

    await executeSQL(stub, 'INSERT INTO sessions (user_id, expired) VALUES (1, 1)');
    await executeSQL(stub, 'INSERT INTO sessions (user_id, expired) VALUES (2, 0)');
    await executeSQL(stub, 'INSERT INTO sessions (user_id, expired) VALUES (3, 1)');

    const result = await executeSQL(
      stub,
      'DELETE FROM sessions WHERE expired = 1 RETURNING *'
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(2);

    // Verify the deleted data is returned
    for (const row of result.rows!) {
      expect(row.expired).toBe(1);
    }

    // Verify rows are actually deleted
    const remaining = await querySQL(stub, 'SELECT * FROM sessions');
    expect(remaining.rows).toHaveLength(1);
    expect(remaining.rows![0].user_id).toBe(2);
  });

  /**
   * Test: DELETE with RETURNING specific columns
   *
   * Expected SQLite behavior:
   * - Only specified columns returned
   *
   * Gap: Column selection in DELETE RETURNING not implemented
   */
  it('DELETE with RETURNING specific columns', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE logs (id INTEGER PRIMARY KEY, level TEXT, message TEXT, timestamp TEXT)'
    );

    await executeSQL(stub, "INSERT INTO logs (level, message, timestamp) VALUES ('DEBUG', 'Test message', '2024-01-01')");

    const result = await executeSQL(
      stub,
      "DELETE FROM logs WHERE level = 'DEBUG' RETURNING id, message"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].id).toBeDefined();
    expect(result.rows![0].message).toBe('Test message');
    expect(result.rows![0].level).toBeUndefined(); // Not in RETURNING
    expect(result.rows![0].timestamp).toBeUndefined(); // Not in RETURNING
  });

  /**
   * Test: DELETE with RETURNING computed expression
   *
   * Expected SQLite behavior:
   * - Can compute expressions using deleted row values
   *
   * Gap: Expression in DELETE RETURNING not implemented
   */
  it('DELETE with RETURNING computed expression', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE cart_items (id INTEGER PRIMARY KEY, cart_id INTEGER, quantity INTEGER, unit_price REAL)'
    );

    await executeSQL(stub, 'INSERT INTO cart_items (cart_id, quantity, unit_price) VALUES (1, 3, 10.00)');
    await executeSQL(stub, 'INSERT INTO cart_items (cart_id, quantity, unit_price) VALUES (1, 2, 25.00)');

    const result = await executeSQL(
      stub,
      'DELETE FROM cart_items WHERE cart_id = 1 RETURNING id, quantity * unit_price AS line_total'
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(2);

    const totals = result.rows!.map((r) => r.line_total);
    expect(totals).toContain(30.00); // 3 * 10
    expect(totals).toContain(50.00); // 2 * 25
  });

  /**
   * Test: DELETE with LIMIT and RETURNING
   *
   * Expected SQLite behavior:
   * - LIMIT restricts number of rows deleted
   * - RETURNING shows only the rows that were actually deleted
   *
   * Gap: LIMIT + RETURNING in DELETE not implemented
   */
  it('DELETE with LIMIT and RETURNING', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE queue (id INTEGER PRIMARY KEY, job_type TEXT, priority INTEGER)'
    );

    await executeSQL(stub, "INSERT INTO queue (job_type, priority) VALUES ('email', 1)");
    await executeSQL(stub, "INSERT INTO queue (job_type, priority) VALUES ('email', 2)");
    await executeSQL(stub, "INSERT INTO queue (job_type, priority) VALUES ('email', 3)");
    await executeSQL(stub, "INSERT INTO queue (job_type, priority) VALUES ('sms', 4)");

    const result = await executeSQL(
      stub,
      "DELETE FROM queue WHERE job_type = 'email' ORDER BY priority ASC LIMIT 2 RETURNING *"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(2);

    // Should delete lowest priority first
    const priorities = result.rows!.map((r) => r.priority);
    expect(priorities).toContain(1);
    expect(priorities).toContain(2);
    expect(priorities).not.toContain(3);
  });

  /**
   * Test: DELETE all rows with RETURNING
   *
   * Expected SQLite behavior:
   * - DELETE without WHERE deletes all rows
   * - RETURNING should return all deleted rows
   *
   * Gap: DELETE all + RETURNING not implemented
   */
  it('DELETE all rows with RETURNING', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE temp_data (id INTEGER PRIMARY KEY, value TEXT)'
    );

    await executeSQL(stub, "INSERT INTO temp_data (value) VALUES ('A')");
    await executeSQL(stub, "INSERT INTO temp_data (value) VALUES ('B')");
    await executeSQL(stub, "INSERT INTO temp_data (value) VALUES ('C')");

    const result = await executeSQL(
      stub,
      'DELETE FROM temp_data RETURNING *'
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(3);

    // Verify table is empty
    const remaining = await querySQL(stub, 'SELECT * FROM temp_data');
    expect(remaining.rows).toHaveLength(0);
  });
});

// =============================================================================
// TEST: RETURNING with Functions
// =============================================================================

describe('RETURNING Clause Execution: Functions', () => {
  /**
   * Test: RETURNING with string functions (UPPER, LOWER)
   *
   * Gap: Function evaluation in RETURNING not implemented
   */
  it('INSERT with RETURNING UPPER() function', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)'
    );

    const result = await executeSQL(
      stub,
      "INSERT INTO users (name) VALUES ('alice') RETURNING id, UPPER(name) AS name_upper"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].name_upper).toBe('ALICE');
  });

  /**
   * Test: RETURNING with COALESCE function
   *
   * Gap: COALESCE in RETURNING not implemented
   */
  it('INSERT with RETURNING COALESCE() function', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE profiles (id INTEGER PRIMARY KEY, name TEXT, nickname TEXT)'
    );

    const result = await executeSQL(
      stub,
      "INSERT INTO profiles (name, nickname) VALUES ('Alice', NULL) RETURNING id, COALESCE(nickname, name) AS display_name"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].display_name).toBe('Alice');
  });

  /**
   * Test: RETURNING with LENGTH function
   *
   * Gap: LENGTH in RETURNING not implemented
   */
  it('INSERT with RETURNING LENGTH() function', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE documents (id INTEGER PRIMARY KEY, content TEXT)'
    );

    const result = await executeSQL(
      stub,
      "INSERT INTO documents (content) VALUES ('Hello World') RETURNING id, LENGTH(content) AS content_length"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].content_length).toBe(11);
  });

  /**
   * Test: RETURNING with DATETIME function
   *
   * Gap: DATETIME in RETURNING not implemented
   */
  it('INSERT with RETURNING DATETIME() function', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE events (id INTEGER PRIMARY KEY, occurred_at TEXT DEFAULT CURRENT_TIMESTAMP)'
    );

    const result = await executeSQL(
      stub,
      "INSERT INTO events (id) VALUES (1) RETURNING id, DATETIME(occurred_at) AS formatted_time"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].formatted_time).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
  });

  /**
   * Test: RETURNING with nested function calls
   *
   * Gap: Nested functions in RETURNING not implemented
   */
  it('INSERT with RETURNING nested function calls', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, nickname TEXT)'
    );

    const result = await executeSQL(
      stub,
      "INSERT INTO users (name, nickname) VALUES ('Alice Smith', NULL) RETURNING id, UPPER(COALESCE(nickname, SUBSTR(name, 1, 5))) AS short_name"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].short_name).toBe('ALICE');
  });
});

// =============================================================================
// TEST: RETURNING with String Concatenation
// =============================================================================

describe('RETURNING Clause Execution: String Operations', () => {
  /**
   * Test: RETURNING with || concatenation
   *
   * Gap: String concatenation in RETURNING not implemented
   */
  it('INSERT with RETURNING string concatenation', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE people (id INTEGER PRIMARY KEY, first_name TEXT, last_name TEXT)'
    );

    const result = await executeSQL(
      stub,
      "INSERT INTO people (first_name, last_name) VALUES ('John', 'Doe') RETURNING id, first_name || ' ' || last_name AS full_name"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].full_name).toBe('John Doe');
  });
});

// =============================================================================
// TEST: RETURNING Edge Cases and Error Handling
// =============================================================================

describe('RETURNING Clause Execution: Edge Cases', () => {
  /**
   * Test: RETURNING with aggregate function should error
   *
   * Expected SQLite behavior:
   * - Aggregate functions (SUM, COUNT, AVG) are NOT allowed in RETURNING
   * - Should produce a syntax/semantic error
   *
   * Gap: Error handling for invalid RETURNING expressions not implemented
   */
  it('INSERT with RETURNING aggregate should error', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE numbers (id INTEGER PRIMARY KEY, value INTEGER)'
    );

    const result = await executeSQL(
      stub,
      'INSERT INTO numbers (value) VALUES (10) RETURNING SUM(value)'
    );

    // This should fail because aggregates are not allowed in RETURNING
    expect(result.success).toBe(false);
    expect(result.error).toBeDefined();
    expect(result.error).toMatch(/aggregate|not allowed|invalid/i);
  });

  /**
   * Test: RETURNING with non-existent column should error
   *
   * Expected SQLite behavior:
   * - Reference to column not in table should error
   *
   * Gap: Column validation in RETURNING not implemented
   */
  it('INSERT with RETURNING non-existent column should error', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE simple (id INTEGER PRIMARY KEY, name TEXT)'
    );

    const result = await executeSQL(
      stub,
      "INSERT INTO simple (name) VALUES ('test') RETURNING id, nonexistent_column"
    );

    expect(result.success).toBe(false);
    expect(result.error).toBeDefined();
    expect(result.error).toMatch(/no such column|unknown column/i);
  });

  /**
   * Test: RETURNING with literal values
   *
   * Expected SQLite behavior:
   * - Can return literal values (strings, numbers)
   *
   * Gap: Literal handling in RETURNING not implemented
   */
  it('INSERT with RETURNING literal values', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)'
    );

    const result = await executeSQL(
      stub,
      "INSERT INTO data (value) VALUES ('test') RETURNING id, 'constant' AS const_val, 42 AS num_val"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].const_val).toBe('constant');
    expect(result.rows![0].num_val).toBe(42);
  });

  /**
   * Test: RETURNING case insensitivity
   *
   * Expected SQLite behavior:
   * - RETURNING keyword should be case-insensitive
   *
   * Gap: Case insensitivity in execution not verified
   */
  it('INSERT with lowercase returning should work', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)'
    );

    const result = await executeSQL(
      stub,
      "INSERT INTO test (name) VALUES ('test') returning id, name"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].id).toBeDefined();
  });

  /**
   * Test: RETURNING mixed case should work
   *
   * Gap: Mixed case handling not verified
   */
  it('INSERT with mixed case Returning should work', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE test2 (id INTEGER PRIMARY KEY, name TEXT)'
    );

    const result = await executeSQL(
      stub,
      "INSERT INTO test2 (name) VALUES ('test') Returning id, name"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
  });
});

// =============================================================================
// TEST: RETURNING with Table Aliases
// =============================================================================

describe('RETURNING Clause Execution: Table Aliases', () => {
  /**
   * Test: INSERT with table alias and RETURNING using alias prefix
   *
   * Expected SQLite behavior:
   * - Table alias can be used in RETURNING to qualify column names
   *
   * Gap: Table alias in RETURNING not implemented
   */
  it('INSERT with table alias in RETURNING', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)'
    );

    const result = await executeSQL(
      stub,
      "INSERT INTO users AS u (name, email) VALUES ('Test', 'test@example.com') RETURNING u.id, u.name"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].id).toBeDefined();
    expect(result.rows![0].name).toBe('Test');
  });

  /**
   * Test: UPDATE with table alias and RETURNING
   *
   * Gap: Table alias in UPDATE RETURNING not implemented
   */
  it('UPDATE with table alias in RETURNING', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)'
    );

    await executeSQL(stub, "INSERT INTO products (name, price) VALUES ('Widget', 10.00)");

    const result = await executeSQL(
      stub,
      "UPDATE products AS p SET p.price = 15.00 WHERE p.name = 'Widget' RETURNING p.id, p.price"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].price).toBeCloseTo(15.00);
  });

  /**
   * Test: DELETE with table alias and RETURNING
   *
   * Gap: Table alias in DELETE RETURNING not implemented
   */
  it('DELETE with table alias in RETURNING', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, total REAL)'
    );

    await executeSQL(stub, "INSERT INTO orders (status, total) VALUES ('cancelled', 99.99)");

    const result = await executeSQL(
      stub,
      "DELETE FROM orders AS o WHERE o.status = 'cancelled' RETURNING o.id, o.total"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].total).toBeCloseTo(99.99);
  });
});

// =============================================================================
// TEST: REPLACE with RETURNING
// =============================================================================

describe('RETURNING Clause Execution: REPLACE', () => {
  /**
   * Test: REPLACE with RETURNING *
   *
   * Expected SQLite behavior:
   * - REPLACE is INSERT OR REPLACE
   * - RETURNING shows the inserted/replaced row
   *
   * Gap: REPLACE with RETURNING not implemented
   */
  it('REPLACE with RETURNING * returns inserted/replaced row', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE config (key TEXT PRIMARY KEY, value TEXT)'
    );

    // Initial insert
    await executeSQL(stub, "INSERT INTO config (key, value) VALUES ('theme', 'light')");

    // REPLACE (should update existing)
    const result = await executeSQL(
      stub,
      "REPLACE INTO config (key, value) VALUES ('theme', 'dark') RETURNING *"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].key).toBe('theme');
    expect(result.rows![0].value).toBe('dark');
  });

  /**
   * Test: REPLACE new row with RETURNING
   *
   * Gap: REPLACE (new row) with RETURNING not implemented
   */
  it('REPLACE new row with RETURNING returns new row', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE settings (id INTEGER PRIMARY KEY, name TEXT, value TEXT)'
    );

    const result = await executeSQL(
      stub,
      "REPLACE INTO settings (name, value) VALUES ('new_setting', 'new_value') RETURNING *"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0].id).toBeDefined();
    expect(result.rows![0].name).toBe('new_setting');
  });
});

// =============================================================================
// TEST: INSERT SELECT with RETURNING
// =============================================================================

describe('RETURNING Clause Execution: INSERT SELECT', () => {
  /**
   * Test: INSERT ... SELECT with RETURNING
   *
   * Expected SQLite behavior:
   * - INSERT SELECT can have RETURNING
   * - Returns all inserted rows from SELECT
   *
   * Gap: INSERT SELECT with RETURNING not implemented
   */
  it('INSERT SELECT with RETURNING returns all inserted rows', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, active INTEGER)'
    );
    await executeSQL(
      stub,
      'CREATE TABLE archived_users (id INTEGER PRIMARY KEY, name TEXT, archived_at TEXT DEFAULT CURRENT_TIMESTAMP)'
    );

    await executeSQL(stub, "INSERT INTO users (name, active) VALUES ('Alice', 0)");
    await executeSQL(stub, "INSERT INTO users (name, active) VALUES ('Bob', 0)");
    await executeSQL(stub, "INSERT INTO users (name, active) VALUES ('Charlie', 1)");

    const result = await executeSQL(
      stub,
      'INSERT INTO archived_users (name) SELECT name FROM users WHERE active = 0 RETURNING *'
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(2);

    const names = result.rows!.map((r) => r.name);
    expect(names).toContain('Alice');
    expect(names).toContain('Bob');
    expect(names).not.toContain('Charlie');
  });
});

// =============================================================================
// TEST: RETURNING with Complex WHERE Clauses
// =============================================================================

describe('RETURNING Clause Execution: Complex WHERE', () => {
  /**
   * Test: UPDATE with IN clause and RETURNING
   *
   * Gap: Complex WHERE + RETURNING not implemented
   */
  it('UPDATE with IN clause and RETURNING', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE items (id INTEGER PRIMARY KEY, status TEXT)'
    );

    await executeSQL(stub, "INSERT INTO items (id, status) VALUES (1, 'pending')");
    await executeSQL(stub, "INSERT INTO items (id, status) VALUES (2, 'pending')");
    await executeSQL(stub, "INSERT INTO items (id, status) VALUES (3, 'pending')");
    await executeSQL(stub, "INSERT INTO items (id, status) VALUES (4, 'pending')");
    await executeSQL(stub, "INSERT INTO items (id, status) VALUES (5, 'pending')");

    const result = await executeSQL(
      stub,
      "UPDATE items SET status = 'processed' WHERE id IN (1, 3, 5) RETURNING id, status"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(3);

    const ids = result.rows!.map((r) => r.id);
    expect(ids).toContain(1);
    expect(ids).toContain(3);
    expect(ids).toContain(5);
    expect(ids).not.toContain(2);
    expect(ids).not.toContain(4);
  });

  /**
   * Test: DELETE with complex AND/OR WHERE and RETURNING
   *
   * Gap: Complex boolean WHERE + RETURNING not implemented
   */
  it('DELETE with complex WHERE and RETURNING', async () => {
    const stub = getUniqueDoSqlStub();

    await executeSQL(
      stub,
      'CREATE TABLE logs (id INTEGER PRIMARY KEY, level TEXT, age_days INTEGER)'
    );

    await executeSQL(stub, "INSERT INTO logs (level, age_days) VALUES ('DEBUG', 100)");
    await executeSQL(stub, "INSERT INTO logs (level, age_days) VALUES ('INFO', 100)");
    await executeSQL(stub, "INSERT INTO logs (level, age_days) VALUES ('DEBUG', 5)");
    await executeSQL(stub, "INSERT INTO logs (level, age_days) VALUES ('ERROR', 100)");

    const result = await executeSQL(
      stub,
      "DELETE FROM logs WHERE (level = 'DEBUG' AND age_days > 30) OR (level = 'INFO' AND age_days > 60) RETURNING *"
    );

    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(2);

    // Should delete: DEBUG with 100 days, INFO with 100 days
    // Should NOT delete: DEBUG with 5 days, ERROR with 100 days
  });
});
