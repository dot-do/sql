/**
 * DoSQL DML Parser Tests
 *
 * Comprehensive tests for INSERT, UPDATE, DELETE, and REPLACE statement parsing.
 */

import { describe, it, expect } from 'vitest';
import {
  parseDML,
  parseInsert,
  parseUpdate,
  parseDelete,
  parseReplace,
  isDMLStatement,
  getDMLType,
} from './dml.js';
import type {
  InsertStatement,
  UpdateStatement,
  DeleteStatement,
  ReplaceStatement,
  LiteralExpression,
  ColumnReference,
  BinaryExpression,
  ParameterExpression,
  FunctionCall,
} from './dml-types.js';
import {
  isInsertStatement,
  isUpdateStatement,
  isDeleteStatement,
  isReplaceStatement,
  isParseSuccess,
  isParseError,
} from './dml-types.js';

// =============================================================================
// BASIC INSERT TESTS
// =============================================================================

describe('INSERT Statement Parsing', () => {
  describe('Basic INSERT', () => {
    it('should parse simple INSERT with VALUES', () => {
      const result = parseInsert("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.type).toBe('insert');
      expect(result.statement.table).toBe('users');
      expect(result.statement.columns).toEqual(['name', 'email']);
      expect(result.statement.source.type).toBe('values_list');

      if (result.statement.source.type === 'values_list') {
        expect(result.statement.source.rows).toHaveLength(1);
        expect(result.statement.source.rows[0].values).toHaveLength(2);

        const firstValue = result.statement.source.rows[0].values[0] as LiteralExpression;
        expect(firstValue.type).toBe('literal');
        expect(firstValue.value).toBe('Alice');
      }
    });

    it('should parse INSERT without column list', () => {
      const result = parseInsert("INSERT INTO users VALUES (1, 'Bob', 'bob@example.com')");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.table).toBe('users');
      expect(result.statement.columns).toBeUndefined();
      expect(result.statement.source.type).toBe('values_list');
    });

    it('should parse INSERT with numeric values', () => {
      const result = parseInsert('INSERT INTO orders (id, total, quantity) VALUES (1, 99.99, 5)');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      if (result.statement.source.type === 'values_list') {
        const values = result.statement.source.rows[0].values as LiteralExpression[];
        expect(values[0].value).toBe(1);
        expect(values[1].value).toBe(99.99);
        expect(values[2].value).toBe(5);
      }
    });

    it('should parse INSERT with NULL values', () => {
      const result = parseInsert("INSERT INTO users (name, email) VALUES ('Alice', NULL)");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      if (result.statement.source.type === 'values_list') {
        const secondValue = result.statement.source.rows[0].values[1];
        expect(secondValue.type).toBe('null');
      }
    });

    it('should parse INSERT with boolean values', () => {
      const result = parseInsert("INSERT INTO flags (name, active) VALUES ('feature', TRUE)");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      if (result.statement.source.type === 'values_list') {
        const secondValue = result.statement.source.rows[0].values[1] as LiteralExpression;
        expect(secondValue.value).toBe(true);
      }
    });

    it('should parse INSERT with DEFAULT keyword', () => {
      const result = parseInsert('INSERT INTO users (name, created_at) VALUES (\'Alice\', DEFAULT)');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      if (result.statement.source.type === 'values_list') {
        const secondValue = result.statement.source.rows[0].values[1];
        expect(secondValue.type).toBe('default');
      }
    });
  });

  describe('Multi-row INSERT', () => {
    it('should parse INSERT with multiple rows', () => {
      const result = parseInsert(`
        INSERT INTO users (name, email) VALUES
        ('Alice', 'alice@example.com'),
        ('Bob', 'bob@example.com'),
        ('Charlie', 'charlie@example.com')
      `);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      if (result.statement.source.type === 'values_list') {
        expect(result.statement.source.rows).toHaveLength(3);
      }
    });
  });

  describe('INSERT ... SELECT', () => {
    it('should parse INSERT from SELECT', () => {
      const result = parseInsert('INSERT INTO archive SELECT * FROM users WHERE active = FALSE');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.source.type).toBe('insert_select');
      if (result.statement.source.type === 'insert_select') {
        expect(result.statement.source.query).toContain('SELECT');
        expect(result.statement.source.query).toContain('FROM users');
      }
    });

    it('should parse INSERT with column list from SELECT', () => {
      const result = parseInsert('INSERT INTO archive (name, email) SELECT name, email FROM users');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.columns).toEqual(['name', 'email']);
      expect(result.statement.source.type).toBe('insert_select');
    });
  });

  describe('INSERT DEFAULT VALUES', () => {
    it('should parse INSERT with DEFAULT VALUES', () => {
      const result = parseInsert('INSERT INTO logs DEFAULT VALUES');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.source.type).toBe('insert_default');
    });
  });

  describe('Conflict Handling', () => {
    it('should parse INSERT OR REPLACE', () => {
      const result = parseInsert("INSERT OR REPLACE INTO users (id, name) VALUES (1, 'Alice')");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.conflict?.action).toBe('REPLACE');
    });

    it('should parse INSERT OR IGNORE', () => {
      const result = parseInsert("INSERT OR IGNORE INTO users (id, name) VALUES (1, 'Alice')");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.conflict?.action).toBe('IGNORE');
    });

    it('should parse INSERT OR ABORT', () => {
      const result = parseInsert("INSERT OR ABORT INTO users (id, name) VALUES (1, 'Alice')");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.conflict?.action).toBe('ABORT');
    });

    it('should parse INSERT OR ROLLBACK', () => {
      const result = parseInsert("INSERT OR ROLLBACK INTO users (id, name) VALUES (1, 'Alice')");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.conflict?.action).toBe('ROLLBACK');
    });

    it('should parse INSERT OR FAIL', () => {
      const result = parseInsert("INSERT OR FAIL INTO users (id, name) VALUES (1, 'Alice')");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.conflict?.action).toBe('FAIL');
    });
  });

  describe('ON CONFLICT (UPSERT)', () => {
    it('should parse INSERT with ON CONFLICT DO NOTHING', () => {
      const result = parseInsert("INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT DO NOTHING");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.onConflict?.action.type).toBe('do_nothing');
    });

    it('should parse INSERT with ON CONFLICT (column) DO NOTHING', () => {
      const result = parseInsert("INSERT INTO users (id, name) VALUES (1, 'Alice') ON CONFLICT (id) DO NOTHING");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.onConflict?.target?.columns).toEqual(['id']);
      expect(result.statement.onConflict?.action.type).toBe('do_nothing');
    });

    it('should parse INSERT with ON CONFLICT DO UPDATE', () => {
      const result = parseInsert(`
        INSERT INTO users (id, name) VALUES (1, 'Alice')
        ON CONFLICT (id) DO UPDATE SET name = excluded.name
      `);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.onConflict?.target?.columns).toEqual(['id']);
      expect(result.statement.onConflict?.action.type).toBe('do_update');

      if (result.statement.onConflict?.action.type === 'do_update') {
        expect(result.statement.onConflict.action.set).toHaveLength(1);
        expect(result.statement.onConflict.action.set[0].column).toBe('name');
      }
    });

    it('should parse INSERT with ON CONFLICT DO UPDATE with WHERE', () => {
      const result = parseInsert(`
        INSERT INTO users (id, name, active) VALUES (1, 'Alice', TRUE)
        ON CONFLICT (id) DO UPDATE SET name = excluded.name WHERE active = TRUE
      `);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      if (result.statement.onConflict?.action.type === 'do_update') {
        expect(result.statement.onConflict.action.where).toBeDefined();
      }
    });
  });

  describe('RETURNING clause', () => {
    it('should parse INSERT with RETURNING *', () => {
      const result = parseInsert("INSERT INTO users (name) VALUES ('Alice') RETURNING *");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning?.columns).toHaveLength(1);
      expect(result.statement.returning?.columns[0].expression).toBe('*');
    });

    it('should parse INSERT with RETURNING specific columns', () => {
      const result = parseInsert("INSERT INTO users (name) VALUES ('Alice') RETURNING id, created_at");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning?.columns).toHaveLength(2);
    });

    it('should parse INSERT with RETURNING and alias', () => {
      const result = parseInsert("INSERT INTO users (name) VALUES ('Alice') RETURNING id AS user_id");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning?.columns[0].alias).toBe('user_id');
    });
  });

  describe('Parameter placeholders', () => {
    it('should parse INSERT with ? placeholders', () => {
      const result = parseInsert('INSERT INTO users (name, email) VALUES (?, ?)');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      if (result.statement.source.type === 'values_list') {
        const values = result.statement.source.rows[0].values as ParameterExpression[];
        expect(values[0].type).toBe('parameter');
        expect(values[1].type).toBe('parameter');
      }
    });

    it('should parse INSERT with named placeholders', () => {
      const result = parseInsert('INSERT INTO users (name, email) VALUES (:name, :email)');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      if (result.statement.source.type === 'values_list') {
        const values = result.statement.source.rows[0].values as ParameterExpression[];
        expect(values[0].name).toBe('name');
        expect(values[1].name).toBe('email');
      }
    });

    it('should parse INSERT with $n placeholders', () => {
      const result = parseInsert('INSERT INTO users (name, email) VALUES ($1, $2)');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      if (result.statement.source.type === 'values_list') {
        const values = result.statement.source.rows[0].values as ParameterExpression[];
        expect(values[0].name).toBe(1);
        expect(values[1].name).toBe(2);
      }
    });
  });
});

// =============================================================================
// UPDATE TESTS
// =============================================================================

describe('UPDATE Statement Parsing', () => {
  describe('Basic UPDATE', () => {
    it('should parse simple UPDATE', () => {
      const result = parseUpdate("UPDATE users SET name = 'Alice' WHERE id = 1");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.type).toBe('update');
      expect(result.statement.table).toBe('users');
      expect(result.statement.set).toHaveLength(1);
      expect(result.statement.set[0].column).toBe('name');
      expect(result.statement.where).toBeDefined();
    });

    it('should parse UPDATE with multiple SET clauses', () => {
      const result = parseUpdate("UPDATE users SET name = 'Alice', email = 'alice@example.com' WHERE id = 1");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.set).toHaveLength(2);
      expect(result.statement.set[0].column).toBe('name');
      expect(result.statement.set[1].column).toBe('email');
    });

    it('should parse UPDATE without WHERE clause', () => {
      const result = parseUpdate("UPDATE users SET active = FALSE");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.where).toBeUndefined();
    });

    it('should parse UPDATE with numeric expressions', () => {
      const result = parseUpdate('UPDATE products SET price = price * 1.1 WHERE category = \'electronics\'');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const setValue = result.statement.set[0].value as BinaryExpression;
      expect(setValue.type).toBe('binary');
      expect(setValue.operator).toBe('*');
    });
  });

  describe('Complex WHERE clauses', () => {
    it('should parse UPDATE with AND condition', () => {
      const result = parseUpdate('UPDATE users SET active = TRUE WHERE role = \'admin\' AND verified = TRUE');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const where = result.statement.where?.condition as BinaryExpression;
      expect(where.operator).toBe('AND');
    });

    it('should parse UPDATE with OR condition', () => {
      const result = parseUpdate('UPDATE users SET active = FALSE WHERE banned = TRUE OR expired = TRUE');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const where = result.statement.where?.condition as BinaryExpression;
      expect(where.operator).toBe('OR');
    });

    it('should parse UPDATE with IN clause', () => {
      const result = parseUpdate('UPDATE users SET role = \'guest\' WHERE id IN (1, 2, 3)');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const where = result.statement.where?.condition as BinaryExpression;
      expect(where.operator).toBe('IN');
    });

    it('should parse UPDATE with BETWEEN clause', () => {
      const result = parseUpdate('UPDATE orders SET status = \'archived\' WHERE created_at BETWEEN \'2020-01-01\' AND \'2020-12-31\'');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const where = result.statement.where?.condition as BinaryExpression;
      expect(where.operator).toBe('BETWEEN');
    });

    it('should parse UPDATE with LIKE clause', () => {
      const result = parseUpdate("UPDATE users SET domain = 'gmail' WHERE email LIKE '%@gmail.com'");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const where = result.statement.where?.condition as BinaryExpression;
      expect(where.operator).toBe('LIKE');
    });

    it('should parse UPDATE with IS NULL', () => {
      const result = parseUpdate('UPDATE users SET email = \'unknown@example.com\' WHERE email IS NULL');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const where = result.statement.where?.condition as BinaryExpression;
      expect(where.operator).toBe('IS');
    });

    it('should parse UPDATE with IS NOT NULL', () => {
      const result = parseUpdate('UPDATE users SET verified = TRUE WHERE email IS NOT NULL');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const where = result.statement.where?.condition as BinaryExpression;
      expect(where.operator).toBe('IS NOT');
    });
  });

  describe('UPDATE with table alias', () => {
    it('should parse UPDATE with AS alias', () => {
      const result = parseUpdate('UPDATE users AS u SET u.name = \'Alice\' WHERE u.id = 1');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.alias).toBe('u');
    });
  });

  describe('UPDATE OR conflict handling', () => {
    it('should parse UPDATE OR REPLACE', () => {
      const result = parseUpdate('UPDATE OR REPLACE users SET name = \'Alice\' WHERE id = 1');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.conflict?.action).toBe('REPLACE');
    });

    it('should parse UPDATE OR IGNORE', () => {
      const result = parseUpdate('UPDATE OR IGNORE users SET name = \'Alice\' WHERE id = 1');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.conflict?.action).toBe('IGNORE');
    });
  });

  describe('UPDATE with ORDER BY and LIMIT', () => {
    it('should parse UPDATE with ORDER BY', () => {
      const result = parseUpdate('UPDATE users SET processed = TRUE ORDER BY created_at ASC');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.orderBy?.items).toHaveLength(1);
      expect(result.statement.orderBy?.items[0].direction).toBe('ASC');
    });

    it('should parse UPDATE with LIMIT', () => {
      const result = parseUpdate('UPDATE users SET processed = TRUE LIMIT 10');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const limitCount = result.statement.limit?.count as LiteralExpression;
      expect(limitCount.value).toBe(10);
    });

    it('should parse UPDATE with ORDER BY and LIMIT', () => {
      const result = parseUpdate('UPDATE users SET processed = TRUE ORDER BY created_at DESC LIMIT 100');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.orderBy).toBeDefined();
      expect(result.statement.limit).toBeDefined();
    });
  });

  describe('UPDATE with RETURNING', () => {
    it('should parse UPDATE with RETURNING', () => {
      const result = parseUpdate("UPDATE users SET name = 'Alice' WHERE id = 1 RETURNING *");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning).toBeDefined();
    });
  });

  describe('UPDATE with function calls', () => {
    it('should parse UPDATE with function in SET', () => {
      const result = parseUpdate('UPDATE users SET updated_at = NOW() WHERE id = 1');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const setValue = result.statement.set[0].value as FunctionCall;
      expect(setValue.type).toBe('function');
      expect(setValue.name).toBe('NOW');
    });

    it('should parse UPDATE with COALESCE', () => {
      const result = parseUpdate("UPDATE users SET name = COALESCE(new_name, name) WHERE id = 1");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const setValue = result.statement.set[0].value as FunctionCall;
      expect(setValue.name).toBe('COALESCE');
      expect(setValue.args).toHaveLength(2);
    });
  });
});

// =============================================================================
// DELETE TESTS
// =============================================================================

describe('DELETE Statement Parsing', () => {
  describe('Basic DELETE', () => {
    it('should parse simple DELETE', () => {
      const result = parseDelete('DELETE FROM users WHERE id = 1');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.type).toBe('delete');
      expect(result.statement.table).toBe('users');
      expect(result.statement.where).toBeDefined();
    });

    it('should parse DELETE without WHERE (delete all)', () => {
      const result = parseDelete('DELETE FROM temp_data');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.where).toBeUndefined();
    });
  });

  describe('DELETE with complex WHERE', () => {
    it('should parse DELETE with AND condition', () => {
      const result = parseDelete('DELETE FROM users WHERE active = FALSE AND last_login < \'2020-01-01\'');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const where = result.statement.where?.condition as BinaryExpression;
      expect(where.operator).toBe('AND');
    });

    it('should parse DELETE with subquery in WHERE', () => {
      const result = parseDelete('DELETE FROM orders WHERE user_id IN (SELECT id FROM users WHERE banned = TRUE)');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const where = result.statement.where?.condition as BinaryExpression;
      expect(where.operator).toBe('IN');
    });
  });

  describe('DELETE with table alias', () => {
    it('should parse DELETE with AS alias', () => {
      const result = parseDelete('DELETE FROM users AS u WHERE u.id = 1');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.alias).toBe('u');
    });
  });

  describe('DELETE with ORDER BY and LIMIT', () => {
    it('should parse DELETE with ORDER BY', () => {
      const result = parseDelete('DELETE FROM logs ORDER BY created_at ASC');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.orderBy?.items[0].direction).toBe('ASC');
    });

    it('should parse DELETE with LIMIT', () => {
      const result = parseDelete('DELETE FROM logs LIMIT 1000');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const limitCount = result.statement.limit?.count as LiteralExpression;
      expect(limitCount.value).toBe(1000);
    });

    it('should parse DELETE with LIMIT and OFFSET', () => {
      const result = parseDelete('DELETE FROM logs LIMIT 100 OFFSET 50');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.limit?.offset).toBeDefined();
    });
  });

  describe('DELETE with RETURNING', () => {
    it('should parse DELETE with RETURNING', () => {
      const result = parseDelete('DELETE FROM users WHERE id = 1 RETURNING *');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning?.columns[0].expression).toBe('*');
    });

    it('should parse DELETE with RETURNING specific columns', () => {
      const result = parseDelete('DELETE FROM users WHERE id = 1 RETURNING id, name');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning?.columns).toHaveLength(2);
    });
  });
});

// =============================================================================
// REPLACE TESTS
// =============================================================================

describe('REPLACE Statement Parsing', () => {
  describe('Basic REPLACE', () => {
    it('should parse simple REPLACE', () => {
      const result = parseReplace("REPLACE INTO users (id, name) VALUES (1, 'Alice')");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.type).toBe('replace');
      expect(result.statement.table).toBe('users');
      expect(result.statement.columns).toEqual(['id', 'name']);
    });

    it('should parse REPLACE without column list', () => {
      const result = parseReplace("REPLACE INTO users VALUES (1, 'Bob')");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.columns).toBeUndefined();
    });

    it('should parse REPLACE with multiple rows', () => {
      const result = parseReplace(`
        REPLACE INTO users (id, name) VALUES
        (1, 'Alice'),
        (2, 'Bob')
      `);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      if (result.statement.source.type === 'values_list') {
        expect(result.statement.source.rows).toHaveLength(2);
      }
    });
  });

  describe('REPLACE with SELECT', () => {
    it('should parse REPLACE with SELECT', () => {
      const result = parseReplace('REPLACE INTO archive SELECT * FROM users WHERE active = FALSE');

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.source.type).toBe('insert_select');
    });
  });

  describe('REPLACE with RETURNING', () => {
    it('should parse REPLACE with RETURNING', () => {
      const result = parseReplace("REPLACE INTO users (id, name) VALUES (1, 'Alice') RETURNING *");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.returning).toBeDefined();
    });
  });
});

// =============================================================================
// GENERAL DML PARSING TESTS
// =============================================================================

describe('General DML Parsing', () => {
  describe('parseDML function', () => {
    it('should detect and parse INSERT', () => {
      const result = parseDML("INSERT INTO users (name) VALUES ('test')");
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        expect(isInsertStatement(result.statement)).toBe(true);
      }
    });

    it('should detect and parse UPDATE', () => {
      const result = parseDML("UPDATE users SET name = 'test'");
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        expect(isUpdateStatement(result.statement)).toBe(true);
      }
    });

    it('should detect and parse DELETE', () => {
      const result = parseDML('DELETE FROM users WHERE id = 1');
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        expect(isDeleteStatement(result.statement)).toBe(true);
      }
    });

    it('should detect and parse REPLACE', () => {
      const result = parseDML("REPLACE INTO users (id, name) VALUES (1, 'test')");
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        expect(isReplaceStatement(result.statement)).toBe(true);
      }
    });

    it('should reject non-DML statements', () => {
      const result = parseDML('SELECT * FROM users');
      expect(isParseError(result)).toBe(true);
    });
  });

  describe('isDMLStatement function', () => {
    it('should identify INSERT statements', () => {
      expect(isDMLStatement('INSERT INTO users VALUES (1)')).toBe(true);
    });

    it('should identify UPDATE statements', () => {
      expect(isDMLStatement('UPDATE users SET x = 1')).toBe(true);
    });

    it('should identify DELETE statements', () => {
      expect(isDMLStatement('DELETE FROM users')).toBe(true);
    });

    it('should identify REPLACE statements', () => {
      expect(isDMLStatement('REPLACE INTO users VALUES (1)')).toBe(true);
    });

    it('should reject SELECT statements', () => {
      expect(isDMLStatement('SELECT * FROM users')).toBe(false);
    });

    it('should be case-insensitive', () => {
      expect(isDMLStatement('insert into users values (1)')).toBe(true);
      expect(isDMLStatement('update users set x = 1')).toBe(true);
      expect(isDMLStatement('delete from users')).toBe(true);
    });
  });

  describe('getDMLType function', () => {
    it('should return correct type for INSERT', () => {
      expect(getDMLType('INSERT INTO users VALUES (1)')).toBe('INSERT');
    });

    it('should return correct type for UPDATE', () => {
      expect(getDMLType('UPDATE users SET x = 1')).toBe('UPDATE');
    });

    it('should return correct type for DELETE', () => {
      expect(getDMLType('DELETE FROM users')).toBe('DELETE');
    });

    it('should return correct type for REPLACE', () => {
      expect(getDMLType('REPLACE INTO users VALUES (1)')).toBe('REPLACE');
    });

    it('should return null for non-DML', () => {
      expect(getDMLType('SELECT * FROM users')).toBeNull();
    });
  });
});

// =============================================================================
// EDGE CASES AND ERROR HANDLING
// =============================================================================

describe('Edge Cases and Error Handling', () => {
  describe('Whitespace handling', () => {
    it('should handle leading/trailing whitespace', () => {
      const result = parseInsert('   INSERT INTO users (name) VALUES (\'test\')   ');
      expect(isParseSuccess(result)).toBe(true);
    });

    it('should handle extra whitespace between tokens', () => {
      const result = parseInsert('INSERT   INTO   users   (name)   VALUES   (\'test\')');
      expect(isParseSuccess(result)).toBe(true);
    });

    it('should handle newlines', () => {
      const result = parseInsert(`
        INSERT INTO users
        (name, email)
        VALUES
        ('Alice', 'alice@example.com')
      `);
      expect(isParseSuccess(result)).toBe(true);
    });
  });

  describe('Quoted identifiers', () => {
    it('should handle double-quoted identifiers', () => {
      const result = parseInsert('INSERT INTO "user-table" ("column-name") VALUES (\'test\')');
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        expect(result.statement.table).toBe('user-table');
        expect(result.statement.columns).toEqual(['column-name']);
      }
    });

    it('should handle backtick-quoted identifiers', () => {
      const result = parseInsert('INSERT INTO `users` (`name`) VALUES (\'test\')');
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        expect(result.statement.table).toBe('users');
      }
    });

    it('should handle bracket-quoted identifiers', () => {
      const result = parseInsert('INSERT INTO [users] ([name]) VALUES (\'test\')');
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        expect(result.statement.table).toBe('users');
      }
    });
  });

  describe('String escape handling', () => {
    it('should handle escaped single quotes', () => {
      const result = parseInsert("INSERT INTO users (name) VALUES ('O''Brien')");
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result) && result.statement.source.type === 'values_list') {
        const value = result.statement.source.rows[0].values[0] as LiteralExpression;
        expect(value.value).toBe("O'Brien");
      }
    });
  });

  describe('Semicolon handling', () => {
    it('should handle trailing semicolon', () => {
      const result = parseInsert("INSERT INTO users (name) VALUES ('test');");
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        expect(result.remaining).toBe('');
      }
    });
  });

  describe('Parse errors', () => {
    it('should report error for missing INTO', () => {
      const result = parseInsert('INSERT users (name) VALUES (\'test\')');
      expect(isParseError(result)).toBe(true);
      if (isParseError(result)) {
        expect(result.error).toContain('INTO');
      }
    });

    it('should report error for missing VALUES or SELECT', () => {
      const result = parseInsert('INSERT INTO users (name)');
      expect(isParseError(result)).toBe(true);
    });

    it('should report error for missing SET in UPDATE', () => {
      const result = parseUpdate('UPDATE users name = \'test\'');
      expect(isParseError(result)).toBe(true);
      if (isParseError(result)) {
        expect(result.error).toContain('SET');
      }
    });

    it('should report error for missing FROM in DELETE', () => {
      const result = parseDelete('DELETE users WHERE id = 1');
      expect(isParseError(result)).toBe(true);
      if (isParseError(result)) {
        expect(result.error).toContain('FROM');
      }
    });
  });
});

// =============================================================================
// EXPRESSION TESTS
// =============================================================================

describe('Expression Parsing', () => {
  describe('Arithmetic expressions in SET', () => {
    it('should parse addition', () => {
      const result = parseUpdate('UPDATE products SET stock = stock + 10 WHERE id = 1');
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        const value = result.statement.set[0].value as BinaryExpression;
        expect(value.operator).toBe('+');
      }
    });

    it('should parse subtraction', () => {
      const result = parseUpdate('UPDATE products SET stock = stock - 1 WHERE id = 1');
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        const value = result.statement.set[0].value as BinaryExpression;
        expect(value.operator).toBe('-');
      }
    });

    it('should parse multiplication', () => {
      const result = parseUpdate('UPDATE products SET price = price * 2 WHERE id = 1');
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        const value = result.statement.set[0].value as BinaryExpression;
        expect(value.operator).toBe('*');
      }
    });

    it('should parse division', () => {
      const result = parseUpdate('UPDATE products SET price = price / 2 WHERE id = 1');
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        const value = result.statement.set[0].value as BinaryExpression;
        expect(value.operator).toBe('/');
      }
    });

    it('should parse modulo', () => {
      const result = parseUpdate('UPDATE items SET remainder = value % 10 WHERE id = 1');
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        const value = result.statement.set[0].value as BinaryExpression;
        expect(value.operator).toBe('%');
      }
    });
  });

  describe('String concatenation', () => {
    it('should parse string concatenation with ||', () => {
      const result = parseUpdate("UPDATE users SET full_name = first_name || ' ' || last_name WHERE id = 1");
      expect(isParseSuccess(result)).toBe(true);
    });
  });

  describe('Comparison operators in WHERE', () => {
    it('should parse equality', () => {
      const result = parseDelete('DELETE FROM users WHERE id = 1');
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        const cond = result.statement.where?.condition as BinaryExpression;
        expect(cond.operator).toBe('=');
      }
    });

    it('should parse inequality (!= and <>)', () => {
      let result = parseDelete('DELETE FROM users WHERE status != \'active\'');
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        const cond = result.statement.where?.condition as BinaryExpression;
        expect(cond.operator).toBe('!=');
      }

      result = parseDelete('DELETE FROM users WHERE status <> \'active\'');
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        const cond = result.statement.where?.condition as BinaryExpression;
        expect(cond.operator).toBe('<>');
      }
    });

    it('should parse less than and greater than', () => {
      let result = parseDelete('DELETE FROM orders WHERE amount < 100');
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        const cond = result.statement.where?.condition as BinaryExpression;
        expect(cond.operator).toBe('<');
      }

      result = parseDelete('DELETE FROM orders WHERE amount > 100');
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        const cond = result.statement.where?.condition as BinaryExpression;
        expect(cond.operator).toBe('>');
      }
    });

    it('should parse less than or equal and greater than or equal', () => {
      let result = parseDelete('DELETE FROM orders WHERE amount <= 100');
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        const cond = result.statement.where?.condition as BinaryExpression;
        expect(cond.operator).toBe('<=');
      }

      result = parseDelete('DELETE FROM orders WHERE amount >= 100');
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        const cond = result.statement.where?.condition as BinaryExpression;
        expect(cond.operator).toBe('>=');
      }
    });
  });

  describe('NOT operator', () => {
    it('should parse NOT in WHERE', () => {
      const result = parseDelete('DELETE FROM users WHERE NOT active');
      expect(isParseSuccess(result)).toBe(true);
    });

    it('should parse NOT LIKE', () => {
      const result = parseDelete("DELETE FROM users WHERE email NOT LIKE '%@spam.com'");
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        const cond = result.statement.where?.condition as BinaryExpression;
        expect(cond.operator).toBe('NOT LIKE');
      }
    });

    it('should parse NOT IN', () => {
      const result = parseDelete('DELETE FROM users WHERE id NOT IN (1, 2, 3)');
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        const cond = result.statement.where?.condition as BinaryExpression;
        expect(cond.operator).toBe('NOT IN');
      }
    });

    it('should parse NOT BETWEEN', () => {
      const result = parseDelete('DELETE FROM orders WHERE amount NOT BETWEEN 100 AND 200');
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        const cond = result.statement.where?.condition as BinaryExpression;
        expect(cond.operator).toBe('NOT BETWEEN');
      }
    });
  });

  describe('Function calls', () => {
    it('should parse function with no arguments', () => {
      const result = parseUpdate('UPDATE logs SET timestamp = NOW() WHERE id = 1');
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        const value = result.statement.set[0].value as FunctionCall;
        expect(value.type).toBe('function');
        expect(value.name).toBe('NOW');
        expect(value.args).toHaveLength(0);
      }
    });

    it('should parse function with multiple arguments', () => {
      const result = parseUpdate("UPDATE users SET name = CONCAT(first, ' ', last) WHERE id = 1");
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result)) {
        const value = result.statement.set[0].value as FunctionCall;
        expect(value.name).toBe('CONCAT');
        expect(value.args).toHaveLength(3);
      }
    });

    it('should parse aggregate function with DISTINCT', () => {
      // This would typically be in SELECT, but we test the parser
      const result = parseInsert('INSERT INTO stats (unique_count) VALUES (COUNT(DISTINCT user_id))');
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result) && result.statement.source.type === 'values_list') {
        const value = result.statement.source.rows[0].values[0] as FunctionCall;
        expect(value.distinct).toBe(true);
      }
    });

    it('should parse function with *', () => {
      const result = parseInsert('INSERT INTO stats (total) VALUES (COUNT(*))');
      expect(isParseSuccess(result)).toBe(true);
      if (isParseSuccess(result) && result.statement.source.type === 'values_list') {
        const value = result.statement.source.rows[0].values[0] as FunctionCall;
        expect(value.args).toHaveLength(1);
        expect((value.args[0] as ColumnReference).name).toBe('*');
      }
    });
  });
});
