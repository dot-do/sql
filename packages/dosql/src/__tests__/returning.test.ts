/**
 * DoSQL RETURNING Clause Tests
 *
 * Comprehensive tests for the RETURNING clause in INSERT, UPDATE, and DELETE statements.
 * Tests SQLite-compatible syntax including:
 * - RETURNING *
 * - RETURNING col1, col2
 * - RETURNING col AS alias
 * - RETURNING expressions
 * - RETURNING with functions
 *
 * @module __tests__/returning
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  DMLExecutor,
  InMemoryDMLStorage,
  createInMemoryDMLExecutor,
} from '../executor/dml-executor.js';
import { parseDML, parseInsert, parseUpdate, parseDelete } from '../parser/dml.js';
import { isParseSuccess } from '../parser/dml-types.js';
import {
  evaluateReturning,
  evaluateExpression,
  hasReturningClause,
  hasWildcard,
  expandWildcard,
  getReturningColumns,
  validateReturning,
  createWildcardReturning,
  createColumnsReturning,
  generateReturningSql,
} from '../parser/returning.js';

// =============================================================================
// TEST SETUP
// =============================================================================

let executor: DMLExecutor;
let storage: InMemoryDMLStorage;

beforeEach(() => {
  const ctx = createInMemoryDMLExecutor();
  executor = ctx.executor;
  storage = ctx.storage;
  storage.createTable('t1', ['id', 'name', 'value']);
});

// =============================================================================
// PARSER TESTS - INSERT RETURNING
// =============================================================================

describe('RETURNING Parser: INSERT', () => {
  it('parses INSERT with RETURNING *', () => {
    const result = parseInsert("INSERT INTO t1 VALUES (1, 'a') RETURNING *");

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.returning).toBeDefined();
      expect(result.statement.returning?.columns).toHaveLength(1);
      expect(result.statement.returning?.columns[0].expression).toBe('*');
    }
  });

  it('parses INSERT with RETURNING specific columns', () => {
    const result = parseInsert("INSERT INTO t1 (id, name) VALUES (1, 'a') RETURNING id, name");

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.returning).toBeDefined();
      expect(result.statement.returning?.columns).toHaveLength(2);

      const cols = result.statement.returning!.columns;
      expect(cols[0].expression).not.toBe('*');
      if (cols[0].expression !== '*') {
        expect(cols[0].expression.type).toBe('column');
      }
    }
  });

  it('parses INSERT with RETURNING aliased column', () => {
    const result = parseInsert("INSERT INTO t1 (id, name) VALUES (1, 'a') RETURNING id AS row_id");

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.returning).toBeDefined();
      expect(result.statement.returning?.columns[0].alias).toBe('row_id');
    }
  });

  it('parses INSERT with RETURNING expression', () => {
    const result = parseInsert("INSERT INTO t1 (id, name, value) VALUES (1, 'a', 10) RETURNING value + 5 AS increased");

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.returning).toBeDefined();
      const col = result.statement.returning!.columns[0];
      expect(col.alias).toBe('increased');
      if (col.expression !== '*') {
        expect(col.expression.type).toBe('binary');
      }
    }
  });

  it('parses INSERT with RETURNING function call', () => {
    const result = parseInsert("INSERT INTO t1 (id, name) VALUES (1, 'alice') RETURNING UPPER(name) AS upper_name");

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.returning).toBeDefined();
      const col = result.statement.returning!.columns[0];
      expect(col.alias).toBe('upper_name');
      if (col.expression !== '*') {
        expect(col.expression.type).toBe('function');
      }
    }
  });

  it('parses INSERT with multiple RETURNING columns and expressions', () => {
    const result = parseInsert(
      "INSERT INTO t1 (id, name, value) VALUES (1, 'a', 10) RETURNING id, name, value * 2 AS doubled"
    );

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.returning?.columns).toHaveLength(3);
    }
  });

  it('parses multi-row INSERT with RETURNING', () => {
    const result = parseInsert(
      "INSERT INTO t1 (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c') RETURNING *"
    );

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.source.type).toBe('values_list');
      expect(result.statement.returning).toBeDefined();
    }
  });

  it('parses INSERT without RETURNING', () => {
    const result = parseInsert("INSERT INTO t1 (id, name) VALUES (1, 'a')");

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.returning).toBeUndefined();
    }
  });
});

// =============================================================================
// PARSER TESTS - UPDATE RETURNING
// =============================================================================

describe('RETURNING Parser: UPDATE', () => {
  it('parses UPDATE with RETURNING *', () => {
    const result = parseUpdate("UPDATE t1 SET name = 'b' WHERE id = 1 RETURNING *");

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.returning).toBeDefined();
      expect(result.statement.returning?.columns[0].expression).toBe('*');
    }
  });

  it('parses UPDATE with RETURNING specific columns', () => {
    const result = parseUpdate("UPDATE t1 SET name = 'b' WHERE id = 1 RETURNING id, name");

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.returning?.columns).toHaveLength(2);
    }
  });

  it('parses UPDATE with RETURNING aliased column', () => {
    const result = parseUpdate("UPDATE t1 SET value = value + 1 RETURNING value AS new_value");

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.returning?.columns[0].alias).toBe('new_value');
    }
  });

  it('parses UPDATE with WHERE, ORDER BY, LIMIT and RETURNING', () => {
    const result = parseUpdate(
      "UPDATE t1 SET value = 0 WHERE value > 10 ORDER BY id LIMIT 5 RETURNING id, value"
    );

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.where).toBeDefined();
      expect(result.statement.orderBy).toBeDefined();
      expect(result.statement.limit).toBeDefined();
      expect(result.statement.returning).toBeDefined();
    }
  });

  it('parses UPDATE without RETURNING', () => {
    const result = parseUpdate("UPDATE t1 SET name = 'b' WHERE id = 1");

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.returning).toBeUndefined();
    }
  });
});

// =============================================================================
// PARSER TESTS - DELETE RETURNING
// =============================================================================

describe('RETURNING Parser: DELETE', () => {
  it('parses DELETE with RETURNING *', () => {
    const result = parseDelete('DELETE FROM t1 WHERE id = 1 RETURNING *');

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.returning).toBeDefined();
      expect(result.statement.returning?.columns[0].expression).toBe('*');
    }
  });

  it('parses DELETE with RETURNING specific columns', () => {
    const result = parseDelete('DELETE FROM t1 WHERE id = 1 RETURNING id, name');

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.returning?.columns).toHaveLength(2);
    }
  });

  it('parses DELETE with RETURNING id only', () => {
    const result = parseDelete('DELETE FROM t1 WHERE id = 1 RETURNING id');

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.returning?.columns).toHaveLength(1);
      const col = result.statement.returning!.columns[0];
      if (col.expression !== '*') {
        expect(col.expression.type).toBe('column');
        expect(col.expression.name).toBe('id');
      }
    }
  });

  it('parses DELETE with ORDER BY, LIMIT and RETURNING', () => {
    const result = parseDelete(
      'DELETE FROM t1 WHERE value > 5 ORDER BY id DESC LIMIT 10 RETURNING *'
    );

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.where).toBeDefined();
      expect(result.statement.orderBy).toBeDefined();
      expect(result.statement.limit).toBeDefined();
      expect(result.statement.returning).toBeDefined();
    }
  });

  it('parses DELETE without RETURNING', () => {
    const result = parseDelete('DELETE FROM t1 WHERE id = 1');

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.returning).toBeUndefined();
    }
  });
});

// =============================================================================
// EXECUTION TESTS - INSERT RETURNING
// =============================================================================

describe('RETURNING Execution: INSERT', () => {
  it('INSERT RETURNING * returns all inserted columns', async () => {
    const result = await executor.execute(
      "INSERT INTO t1 (name, value) VALUES ('alice', 100) RETURNING *"
    );

    expect(result.changes).toBe(1);
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toHaveProperty('id');
    expect(result.rows[0]).toHaveProperty('name', 'alice');
    expect(result.rows[0]).toHaveProperty('value', 100);
  });

  it('INSERT RETURNING id, name returns specific columns', async () => {
    const result = await executor.execute(
      "INSERT INTO t1 (name, value) VALUES ('bob', 200) RETURNING id, name"
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toHaveProperty('id');
    expect(result.rows[0]).toHaveProperty('name', 'bob');
    expect(result.columns).toEqual(['id', 'name']);
  });

  it('multi-row INSERT with RETURNING returns all rows', async () => {
    const result = await executor.execute(
      "INSERT INTO t1 (name, value) VALUES ('a', 1), ('b', 2), ('c', 3) RETURNING name, value"
    );

    expect(result.changes).toBe(3);
    expect(result.rows).toHaveLength(3);
    expect(result.rows[0]).toMatchObject({ name: 'a', value: 1 });
    expect(result.rows[1]).toMatchObject({ name: 'b', value: 2 });
    expect(result.rows[2]).toMatchObject({ name: 'c', value: 3 });
  });

  it('INSERT without RETURNING returns empty rows', async () => {
    const result = await executor.execute(
      "INSERT INTO t1 (name, value) VALUES ('test', 50)"
    );

    expect(result.changes).toBe(1);
    expect(result.rows).toEqual([]);
    expect(result.columns).toEqual([]);
  });

  it('INSERT RETURNING with alias renames column', async () => {
    const result = await executor.execute(
      "INSERT INTO t1 (name, value) VALUES ('test', 42) RETURNING name AS user_name"
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toHaveProperty('user_name', 'test');
    expect(result.columns).toContain('user_name');
  });
});

// =============================================================================
// EXECUTION TESTS - UPDATE RETURNING
// =============================================================================

describe('RETURNING Execution: UPDATE', () => {
  beforeEach(async () => {
    await executor.execute("INSERT INTO t1 (name, value) VALUES ('alice', 100)");
    await executor.execute("INSERT INTO t1 (name, value) VALUES ('bob', 200)");
    await executor.execute("INSERT INTO t1 (name, value) VALUES ('charlie', 300)");
  });

  it('UPDATE RETURNING * returns updated rows', async () => {
    const result = await executor.execute(
      "UPDATE t1 SET value = 999 WHERE name = 'alice' RETURNING *"
    );

    expect(result.changes).toBe(1);
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({ name: 'alice', value: 999 });
  });

  it('UPDATE multiple rows with RETURNING', async () => {
    const result = await executor.execute(
      'UPDATE t1 SET value = 0 WHERE value > 100 RETURNING name, value'
    );

    expect(result.changes).toBe(2);
    expect(result.rows).toHaveLength(2);
    for (const row of result.rows) {
      expect(row).toHaveProperty('value', 0);
    }
  });

  it('UPDATE with no matches returns empty rows', async () => {
    const result = await executor.execute(
      "UPDATE t1 SET value = 0 WHERE name = 'nonexistent' RETURNING *"
    );

    expect(result.changes).toBe(0);
    expect(result.rows).toEqual([]);
  });

  it('UPDATE without RETURNING returns empty rows', async () => {
    const result = await executor.execute(
      "UPDATE t1 SET value = 500 WHERE name = 'bob'"
    );

    expect(result.changes).toBe(1);
    expect(result.rows).toEqual([]);
  });

  it('UPDATE RETURNING reflects new values', async () => {
    const result = await executor.execute(
      "UPDATE t1 SET value = value + 50 WHERE name = 'alice' RETURNING value"
    );

    expect(result.rows[0]).toHaveProperty('value', 150);
  });
});

// =============================================================================
// EXECUTION TESTS - DELETE RETURNING
// =============================================================================

describe('RETURNING Execution: DELETE', () => {
  beforeEach(async () => {
    await executor.execute("INSERT INTO t1 (name, value) VALUES ('alice', 100)");
    await executor.execute("INSERT INTO t1 (name, value) VALUES ('bob', 200)");
    await executor.execute("INSERT INTO t1 (name, value) VALUES ('charlie', 300)");
  });

  it('DELETE RETURNING * returns deleted rows', async () => {
    const result = await executor.execute(
      "DELETE FROM t1 WHERE name = 'bob' RETURNING *"
    );

    expect(result.changes).toBe(1);
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toMatchObject({ name: 'bob', value: 200 });

    // Verify row was actually deleted
    const remaining = await storage.getAll('t1');
    expect(remaining).toHaveLength(2);
    expect(remaining.find(r => r.name === 'bob')).toBeUndefined();
  });

  it('DELETE RETURNING id returns only id of deleted rows', async () => {
    const result = await executor.execute(
      "DELETE FROM t1 WHERE name = 'alice' RETURNING id"
    );

    expect(result.rows).toHaveLength(1);
    expect(result.rows[0]).toHaveProperty('id');
    expect(result.columns).toEqual(['id']);
  });

  it('DELETE multiple rows with RETURNING', async () => {
    const result = await executor.execute(
      'DELETE FROM t1 WHERE value >= 200 RETURNING name'
    );

    expect(result.changes).toBe(2);
    expect(result.rows).toHaveLength(2);

    const names = result.rows.map((r: Record<string, unknown>) => r.name);
    expect(names).toContain('bob');
    expect(names).toContain('charlie');
  });

  it('DELETE all rows with RETURNING', async () => {
    const result = await executor.execute('DELETE FROM t1 RETURNING *');

    expect(result.changes).toBe(3);
    expect(result.rows).toHaveLength(3);

    const remaining = await storage.getAll('t1');
    expect(remaining).toHaveLength(0);
  });

  it('DELETE with no matches returns empty rows', async () => {
    const result = await executor.execute(
      'DELETE FROM t1 WHERE value > 1000 RETURNING *'
    );

    expect(result.changes).toBe(0);
    expect(result.rows).toEqual([]);
  });

  it('DELETE without RETURNING returns empty rows', async () => {
    const result = await executor.execute(
      "DELETE FROM t1 WHERE name = 'alice'"
    );

    expect(result.changes).toBe(1);
    expect(result.rows).toEqual([]);
  });
});

// =============================================================================
// RETURNING UTILITY FUNCTIONS
// =============================================================================

describe('RETURNING Utilities', () => {
  it('hasReturningClause detects RETURNING clause', () => {
    const withReturning = parseInsert("INSERT INTO t1 VALUES (1, 'a') RETURNING *");
    const withoutReturning = parseInsert("INSERT INTO t1 VALUES (1, 'a')");

    if (withReturning.success && withoutReturning.success) {
      expect(hasReturningClause(withReturning.statement)).toBe(true);
      expect(hasReturningClause(withoutReturning.statement)).toBe(false);
    }
  });

  it('hasWildcard detects * in RETURNING', () => {
    const wildcard = createWildcardReturning();
    const specific = createColumnsReturning(['id', 'name']);

    expect(hasWildcard(wildcard)).toBe(true);
    expect(hasWildcard(specific)).toBe(false);
  });

  it('expandWildcard expands * to schema columns', () => {
    const returning = createWildcardReturning();
    const schemaColumns = ['id', 'name', 'value'];

    const expanded = expandWildcard(returning, schemaColumns);

    expect(expanded).toHaveLength(3);
    for (let i = 0; i < expanded.length; i++) {
      const col = expanded[i];
      if (col.expression !== '*') {
        expect(col.expression.type).toBe('column');
        expect(col.expression.name).toBe(schemaColumns[i]);
      }
    }
  });

  it('getReturningColumns extracts column info', () => {
    const result = parseInsert(
      "INSERT INTO t1 (id, name) VALUES (1, 'a') RETURNING id, name AS alias"
    );

    if (result.success && result.statement.returning) {
      const cols = getReturningColumns(result.statement.returning);

      expect(cols).toHaveLength(2);
      expect(cols[0].outputName).toBe('id');
      expect(cols[0].isWildcard).toBe(false);
      expect(cols[1].outputName).toBe('alias');
      expect(cols[1].hasAlias).toBe(true);
    }
  });

  it('validateReturning validates columns against schema', () => {
    const returning = createColumnsReturning(['id', 'name', 'nonexistent']);
    const schemaColumns = ['id', 'name', 'value'];

    const validation = validateReturning(returning, schemaColumns);

    expect(validation.valid).toBe(false);
    expect(validation.errors).toContain("Column 'nonexistent' not found in schema");
  });

  it('validateReturning allows unknown columns with option', () => {
    const returning = createColumnsReturning(['id', 'unknown']);
    const schemaColumns = ['id', 'name'];

    const validation = validateReturning(returning, schemaColumns, {
      allowUnknownColumns: true,
    });

    expect(validation.valid).toBe(true);
    expect(validation.warnings).toContain("Column 'unknown' not found in schema");
  });

  it('generateReturningSql generates correct SQL', () => {
    const returning = createColumnsReturning(['id', 'name']);
    const sql = generateReturningSql(returning);

    expect(sql).toBe('RETURNING id, name');
  });

  it('generateReturningSql handles wildcard', () => {
    const returning = createWildcardReturning();
    const sql = generateReturningSql(returning);

    expect(sql).toBe('RETURNING *');
  });
});

// =============================================================================
// EXPRESSION EVALUATION IN RETURNING
// =============================================================================

describe('RETURNING Expression Evaluation', () => {
  it('evaluates literal expression', () => {
    const row = { id: 1, name: 'test' };
    const expr = { type: 'literal' as const, value: 42, raw: '42' };

    const result = evaluateExpression(expr, row);
    expect(result).toBe(42);
  });

  it('evaluates column reference', () => {
    const row = { id: 1, name: 'alice' };
    const expr = { type: 'column' as const, name: 'name' };

    const result = evaluateExpression(expr, row);
    expect(result).toBe('alice');
  });

  it('evaluates null expression', () => {
    const row = { id: 1 };
    const expr = { type: 'null' as const };

    const result = evaluateExpression(expr, row);
    expect(result).toBe(null);
  });

  it('evaluates binary arithmetic expression', () => {
    const row = { id: 1, value: 10 };
    const expr = {
      type: 'binary' as const,
      operator: '+' as const,
      left: { type: 'column' as const, name: 'value' },
      right: { type: 'literal' as const, value: 5, raw: '5' },
    };

    const result = evaluateExpression(expr, row);
    expect(result).toBe(15);
  });

  it('evaluates string concatenation', () => {
    const row = { first: 'Hello', second: 'World' };
    const expr = {
      type: 'binary' as const,
      operator: '||' as const,
      left: { type: 'column' as const, name: 'first' },
      right: { type: 'column' as const, name: 'second' },
    };

    const result = evaluateExpression(expr, row);
    expect(result).toBe('HelloWorld');
  });

  it('evaluates UPPER function', () => {
    const row = { name: 'alice' };
    const expr = {
      type: 'function' as const,
      name: 'UPPER',
      args: [{ type: 'column' as const, name: 'name' }],
    };

    const result = evaluateExpression(expr, row);
    expect(result).toBe('ALICE');
  });

  it('evaluates LOWER function', () => {
    const row = { name: 'ALICE' };
    const expr = {
      type: 'function' as const,
      name: 'LOWER',
      args: [{ type: 'column' as const, name: 'name' }],
    };

    const result = evaluateExpression(expr, row);
    expect(result).toBe('alice');
  });

  it('evaluates COALESCE function', () => {
    const row = { a: null, b: null, c: 'value' };
    const expr = {
      type: 'function' as const,
      name: 'COALESCE',
      args: [
        { type: 'column' as const, name: 'a' },
        { type: 'column' as const, name: 'b' },
        { type: 'column' as const, name: 'c' },
      ],
    };

    const result = evaluateExpression(expr, row);
    expect(result).toBe('value');
  });

  it('evaluates ABS function', () => {
    const row = { value: -42 };
    const expr = {
      type: 'function' as const,
      name: 'ABS',
      args: [{ type: 'column' as const, name: 'value' }],
    };

    const result = evaluateExpression(expr, row);
    expect(result).toBe(42);
  });

  it('evaluates LENGTH function', () => {
    const row = { name: 'hello' };
    const expr = {
      type: 'function' as const,
      name: 'LENGTH',
      args: [{ type: 'column' as const, name: 'name' }],
    };

    const result = evaluateExpression(expr, row);
    expect(result).toBe(5);
  });

  it('evaluates unary minus', () => {
    const row = { value: 10 };
    const expr = {
      type: 'unary' as const,
      operator: '-' as const,
      operand: { type: 'column' as const, name: 'value' },
    };

    const result = evaluateExpression(expr, row);
    expect(result).toBe(-10);
  });

  it('evaluates NOT operator', () => {
    const row = { flag: true };
    const expr = {
      type: 'unary' as const,
      operator: 'NOT' as const,
      operand: { type: 'column' as const, name: 'flag' },
    };

    const result = evaluateExpression(expr, row);
    expect(result).toBe(false);
  });
});

// =============================================================================
// FULL RETURNING EVALUATION
// =============================================================================

describe('RETURNING Full Evaluation', () => {
  it('evaluates RETURNING * against a row', () => {
    const returning = createWildcardReturning();
    const row = { id: 1, name: 'alice', value: 100 };
    const schemaColumns = ['id', 'name', 'value'];

    const result = evaluateReturning(returning, row, schemaColumns);

    expect(result).toEqual({ id: 1, name: 'alice', value: 100 });
  });

  it('evaluates RETURNING specific columns', () => {
    const returning = createColumnsReturning(['id', 'name']);
    const row = { id: 1, name: 'alice', value: 100 };

    const result = evaluateReturning(returning, row);

    expect(result).toEqual({ id: 1, name: 'alice' });
  });

  it('evaluates RETURNING with custom function evaluator', () => {
    const result = parseInsert(
      "INSERT INTO t1 (name) VALUES ('test') RETURNING CUSTOM_FUNC(name) AS custom"
    );

    if (result.success && result.statement.returning) {
      const row = { name: 'test' };

      const evaluated = evaluateReturning(result.statement.returning, row, ['name'], {
        functionEvaluator: (name, args) => {
          if (name === 'CUSTOM_FUNC') {
            return `custom_${args[0]}`;
          }
          return null;
        },
      });

      expect(evaluated).toHaveProperty('custom', 'custom_test');
    }
  });
});

// =============================================================================
// ERROR HANDLING
// =============================================================================

describe('RETURNING Error Handling', () => {
  it('rejects window functions in RETURNING', () => {
    const result = parseDML(
      "INSERT INTO t1 (name) VALUES ('a') RETURNING ROW_NUMBER() OVER () AS rn"
    );

    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error).toContain('Window functions are not allowed in RETURNING clause');
    }
  });

  it('rejects aggregate functions in RETURNING', () => {
    const result = parseDML(
      "INSERT INTO t1 (name) VALUES ('a') RETURNING COUNT(*) AS cnt"
    );

    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error).toContain('Aggregate functions are not allowed in RETURNING clause');
    }
  });
});

// =============================================================================
// SQLITE COMPATIBILITY
// =============================================================================

describe('RETURNING SQLite Compatibility', () => {
  it('supports lowercase returning keyword', () => {
    const result = parseInsert("INSERT INTO t1 (name) VALUES ('a') returning *");

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.returning).toBeDefined();
    }
  });

  it('supports mixed case RETURNING keyword', () => {
    const result = parseInsert("INSERT INTO t1 (name) VALUES ('a') Returning id");

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.returning).toBeDefined();
    }
  });

  it('handles RETURNING with ON CONFLICT clause', () => {
    const result = parseInsert(
      "INSERT INTO t1 (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = 'b' RETURNING *"
    );

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.onConflict).toBeDefined();
      expect(result.statement.returning).toBeDefined();
    }
  });

  it('handles INSERT OR REPLACE with RETURNING', () => {
    const result = parseInsert(
      "INSERT OR REPLACE INTO t1 (id, name) VALUES (1, 'a') RETURNING *"
    );

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.statement.conflict?.action).toBe('REPLACE');
      expect(result.statement.returning).toBeDefined();
    }
  });
});
