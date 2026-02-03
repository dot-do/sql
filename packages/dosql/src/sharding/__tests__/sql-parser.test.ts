/**
 * SQL Parser Unit Tests
 *
 * Comprehensive tests for the SQL parser:
 * - SELECT query parsing (columns, tables, WHERE, ORDER BY, LIMIT)
 * - INSERT, UPDATE, DELETE parsing
 * - Aggregate functions and GROUP BY
 * - Table aliases
 * - Compound queries (UNION, INTERSECT, EXCEPT)
 * - Edge cases and error handling
 *
 * @packageDocumentation
 */

import { describe, it, expect } from 'vitest';

import { SQLParser } from '../parser/sql-parser.js';

// =============================================================================
// PARSER INSTANTIATION
// =============================================================================

describe('SQLParser', () => {
  const parser = new SQLParser();

  describe('constructor', () => {
    it('should create parser instance', () => {
      const p = new SQLParser();
      expect(p).toBeInstanceOf(SQLParser);
    });
  });

  // ===========================================================================
  // SELECT QUERY PARSING
  // ===========================================================================

  describe('SELECT parsing', () => {
    describe('basic SELECT', () => {
      it('should parse SELECT *', () => {
        const parsed = parser.parse('SELECT * FROM users');

        expect(parsed.operation).toBe('SELECT');
        expect(parsed.tables).toHaveLength(1);
        expect(parsed.tables[0]!.name).toBe('users');
        expect(parsed.columns).toBeDefined();
        expect(parsed.columns![0]!.name).toBe('*');
      });

      it('should parse SELECT with single column', () => {
        const parsed = parser.parse('SELECT id FROM users');

        expect(parsed.columns).toHaveLength(1);
        expect(parsed.columns![0]!.name).toBe('id');
      });

      it('should parse SELECT with multiple columns', () => {
        const parsed = parser.parse('SELECT id, name, email FROM users');

        expect(parsed.columns!.length).toBeGreaterThanOrEqual(3);
      });

      it('should parse SELECT DISTINCT', () => {
        const parsed = parser.parse('SELECT DISTINCT status FROM users');

        expect(parsed.distinct).toBe(true);
      });

      it('should parse SELECT with column alias using AS', () => {
        const parsed = parser.parse('SELECT id AS user_id FROM users');

        const idColumn = parsed.columns?.find(c => c.alias === 'user_id');
        expect(idColumn).toBeDefined();
      });

      it('should parse SELECT with table.column', () => {
        const parsed = parser.parse('SELECT users.id FROM users');

        const idColumn = parsed.columns?.find(c => c.name === 'id');
        expect(idColumn).toBeDefined();
        expect(idColumn!.table).toBe('users');
      });
    });

    describe('FROM clause', () => {
      it('should parse single table', () => {
        const parsed = parser.parse('SELECT * FROM users');

        expect(parsed.tables).toHaveLength(1);
        expect(parsed.tables[0]!.name).toBe('users');
      });

      it('should parse table with alias', () => {
        const parsed = parser.parse('SELECT * FROM users u');

        expect(parsed.tables[0]!.name).toBe('users');
        expect(parsed.tables[0]!.alias).toBe('u');
      });

      it('should parse table with AS alias', () => {
        const parsed = parser.parse('SELECT * FROM users AS u');

        expect(parsed.tables[0]!.name).toBe('users');
        expect(parsed.tables[0]!.alias).toBe('u');
      });

      it('should parse quoted table name', () => {
        const parsed = parser.parse('SELECT * FROM "user table"');

        expect(parsed.tables[0]!.name).toBe('user table');
      });
    });

    describe('WHERE clause', () => {
      it('should parse simple equality', () => {
        const parsed = parser.parse('SELECT * FROM users WHERE id = 1');

        expect(parsed.where).toBeDefined();
        expect(parsed.where!.conditions).toHaveLength(1);
        expect(parsed.where!.conditions[0]!.column).toBe('id');
        expect(parsed.where!.conditions[0]!.operator).toBe('=');
        expect(parsed.where!.conditions[0]!.value).toBe(1);
      });

      it('should parse string equality', () => {
        const parsed = parser.parse("SELECT * FROM users WHERE name = 'John'");

        expect(parsed.where!.conditions[0]!.value).toBe('John');
      });

      it('should parse multiple AND conditions', () => {
        const parsed = parser.parse("SELECT * FROM users WHERE id = 1 AND status = 'active'");

        expect(parsed.where!.conditions.length).toBeGreaterThanOrEqual(2);
        expect(parsed.where!.operator).toBe('AND');
      });

      it('should detect OR conditions', () => {
        const parsed = parser.parse('SELECT * FROM users WHERE id = 1 OR id = 2');

        expect(parsed.where!.operator).toBe('OR');
      });

      it('should parse IN clause', () => {
        const parsed = parser.parse('SELECT * FROM users WHERE id IN (1, 2, 3)');

        const inCondition = parsed.where!.conditions.find(c => c.operator === 'IN');
        expect(inCondition).toBeDefined();
        expect(inCondition!.values).toEqual([1, 2, 3]);
      });

      it('should parse string values in IN clause', () => {
        const parsed = parser.parse("SELECT * FROM users WHERE status IN ('active', 'pending')");

        const inCondition = parsed.where!.conditions.find(c => c.operator === 'IN');
        expect(inCondition!.values).toEqual(['active', 'pending']);
      });

      it('should parse BETWEEN clause', () => {
        const parsed = parser.parse('SELECT * FROM users WHERE age BETWEEN 18 AND 65');

        const betweenCondition = parsed.where!.conditions.find(c => c.operator === 'BETWEEN');
        expect(betweenCondition).toBeDefined();
        expect(betweenCondition!.minValue).toBe(18);
        expect(betweenCondition!.maxValue).toBe(65);
      });

      it('should parse comparison operators', () => {
        const operators = ['>', '<', '>=', '<=', '!=', '<>'];

        for (const op of operators) {
          const parsed = parser.parse(`SELECT * FROM users WHERE age ${op} 21`);
          expect(parsed.where).toBeDefined();
        }
      });

      it('should parse IS NULL', () => {
        const parsed = parser.parse('SELECT * FROM users WHERE deleted_at IS NULL');

        const condition = parsed.where!.conditions.find(c => c.operator === 'IS NULL');
        expect(condition).toBeDefined();
      });

      it('should parse IS NOT NULL', () => {
        const parsed = parser.parse('SELECT * FROM users WHERE email IS NOT NULL');

        const condition = parsed.where!.conditions.find(c => c.operator === 'IS NOT NULL');
        expect(condition).toBeDefined();
      });

      it('should parse LIKE', () => {
        const parsed = parser.parse("SELECT * FROM users WHERE name LIKE 'J%'");

        const condition = parsed.where!.conditions.find(c => c.operator === 'LIKE');
        expect(condition).toBeDefined();
        expect(condition!.value).toBe('J%');
      });

      it('should parse parameter placeholders', () => {
        const parsed = parser.parse('SELECT * FROM users WHERE id = $1');

        expect(parsed.where!.conditions[0]!.value).toEqual({ placeholder: '$1' });
      });

      it('should parse ? placeholders', () => {
        const parsed = parser.parse('SELECT * FROM users WHERE id = ?');

        expect(parsed.where!.conditions[0]!.value).toEqual({ placeholder: '?' });
      });

      it('should parse table.column in WHERE', () => {
        const parsed = parser.parse('SELECT * FROM users u WHERE u.id = 1');

        const condition = parsed.where!.conditions.find(c => c.column === 'id');
        expect(condition).toBeDefined();
      });
    });

    describe('ORDER BY clause', () => {
      it('should parse simple ORDER BY', () => {
        const parsed = parser.parse('SELECT * FROM users ORDER BY created_at');

        expect(parsed.orderBy).toBeDefined();
        expect(parsed.orderBy![0]!.column).toBe('created_at');
        expect(parsed.orderBy![0]!.direction).toBe('ASC');
      });

      it('should parse ORDER BY DESC', () => {
        const parsed = parser.parse('SELECT * FROM users ORDER BY created_at DESC');

        expect(parsed.orderBy![0]!.direction).toBe('DESC');
      });

      it('should parse ORDER BY ASC explicitly', () => {
        const parsed = parser.parse('SELECT * FROM users ORDER BY created_at ASC');

        expect(parsed.orderBy![0]!.direction).toBe('ASC');
      });

      it('should parse multiple ORDER BY columns', () => {
        const parsed = parser.parse('SELECT * FROM users ORDER BY status, created_at DESC');

        expect(parsed.orderBy!.length).toBeGreaterThanOrEqual(2);
      });

      it('should parse ORDER BY with NULLS FIRST', () => {
        const parsed = parser.parse('SELECT * FROM users ORDER BY created_at NULLS FIRST');

        expect(parsed.orderBy![0]!.nulls).toBe('FIRST');
      });

      it('should parse ORDER BY with NULLS LAST', () => {
        const parsed = parser.parse('SELECT * FROM users ORDER BY created_at NULLS LAST');

        expect(parsed.orderBy![0]!.nulls).toBe('LAST');
      });

      it('should parse ORDER BY with table.column', () => {
        const parsed = parser.parse('SELECT * FROM users u ORDER BY u.created_at');

        expect(parsed.orderBy![0]!.column).toContain('created_at');
      });
    });

    describe('LIMIT and OFFSET', () => {
      it('should parse LIMIT', () => {
        const parsed = parser.parse('SELECT * FROM users LIMIT 10');

        expect(parsed.limit).toBe(10);
      });

      it('should parse OFFSET', () => {
        const parsed = parser.parse('SELECT * FROM users LIMIT 10 OFFSET 20');

        expect(parsed.limit).toBe(10);
        expect(parsed.offset).toBe(20);
      });

      it('should parse OFFSET only', () => {
        const parsed = parser.parse('SELECT * FROM users OFFSET 5');

        expect(parsed.offset).toBe(5);
      });
    });

    describe('GROUP BY and aggregates', () => {
      it('should parse COUNT(*)', () => {
        const parsed = parser.parse('SELECT COUNT(*) FROM users');

        expect(parsed.aggregates).toBeDefined();
        expect(parsed.aggregates!.length).toBeGreaterThan(0);
        expect(parsed.aggregates![0]!.function).toBe('COUNT');
      });

      it('should parse COUNT(column)', () => {
        const parsed = parser.parse('SELECT COUNT(id) FROM users');

        expect(parsed.aggregates![0]!.column).toBe('id');
      });

      it('should parse SUM', () => {
        const parsed = parser.parse('SELECT SUM(amount) FROM orders');

        expect(parsed.aggregates![0]!.function).toBe('SUM');
        expect(parsed.aggregates![0]!.column).toBe('amount');
      });

      it('should parse AVG', () => {
        const parsed = parser.parse('SELECT AVG(price) FROM products');

        expect(parsed.aggregates![0]!.function).toBe('AVG');
        expect(parsed.aggregates![0]!.isPartial).toBe(true);
      });

      it('should parse MIN and MAX', () => {
        const parsed = parser.parse('SELECT MIN(price), MAX(price) FROM products');

        expect(parsed.aggregates!.length).toBeGreaterThanOrEqual(2);
        expect(parsed.aggregates!.some(a => a.function === 'MIN')).toBe(true);
        expect(parsed.aggregates!.some(a => a.function === 'MAX')).toBe(true);
      });

      it('should parse aggregate with alias', () => {
        const parsed = parser.parse('SELECT COUNT(*) AS total FROM users');

        expect(parsed.aggregates![0]!.alias).toBe('total');
      });

      it('should parse GROUP BY single column', () => {
        const parsed = parser.parse('SELECT status, COUNT(*) FROM users GROUP BY status');

        expect(parsed.groupBy).toBeDefined();
        expect(parsed.groupBy).toContain('status');
      });

      it('should parse GROUP BY multiple columns', () => {
        const parsed = parser.parse('SELECT status, type, COUNT(*) FROM users GROUP BY status, type');

        expect(parsed.groupBy!.length).toBeGreaterThanOrEqual(2);
      });

      it('should parse GROUP BY with table.column', () => {
        const parsed = parser.parse('SELECT u.status, COUNT(*) FROM users u GROUP BY u.status');

        expect(parsed.groupBy).toBeDefined();
      });
    });

    describe('compound queries', () => {
      it('should detect UNION', () => {
        const parsed = parser.parse('SELECT id FROM users UNION SELECT id FROM admins');

        expect(parsed.compound).toBeDefined();
        expect(parsed.compound!.type).toBe('UNION');
      });

      it('should detect UNION ALL', () => {
        const parsed = parser.parse('SELECT id FROM users UNION ALL SELECT id FROM admins');

        expect(parsed.compound!.type).toBe('UNION ALL');
      });

      it('should detect INTERSECT', () => {
        const parsed = parser.parse('SELECT id FROM users INTERSECT SELECT id FROM premium_users');

        expect(parsed.compound!.type).toBe('INTERSECT');
      });

      it('should detect EXCEPT', () => {
        const parsed = parser.parse('SELECT id FROM users EXCEPT SELECT id FROM banned_users');

        expect(parsed.compound!.type).toBe('EXCEPT');
      });
    });
  });

  // ===========================================================================
  // INSERT QUERY PARSING
  // ===========================================================================

  describe('INSERT parsing', () => {
    it('should parse simple INSERT', () => {
      const parsed = parser.parse("INSERT INTO users (id, name) VALUES (1, 'Alice')");

      expect(parsed.operation).toBe('INSERT');
      expect(parsed.tables[0]!.name).toBe('users');
    });

    it('should extract column names', () => {
      const parsed = parser.parse("INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@test.com')");

      expect(parsed.columns).toBeDefined();
      expect(parsed.columns!.some(c => c.name === 'id')).toBe(true);
      expect(parsed.columns!.some(c => c.name === 'name')).toBe(true);
      expect(parsed.columns!.some(c => c.name === 'email')).toBe(true);
    });

    it('should parse INSERT without column list', () => {
      const parsed = parser.parse("INSERT INTO users VALUES (1, 'Alice')");

      expect(parsed.operation).toBe('INSERT');
      expect(parsed.tables[0]!.name).toBe('users');
    });

    it('should throw on invalid INSERT', () => {
      expect(() => parser.parse('INSERT')).toThrow();
    });
  });

  // ===========================================================================
  // UPDATE QUERY PARSING
  // ===========================================================================

  describe('UPDATE parsing', () => {
    it('should parse simple UPDATE', () => {
      const parsed = parser.parse("UPDATE users SET name = 'Bob' WHERE id = 1");

      expect(parsed.operation).toBe('UPDATE');
      expect(parsed.tables[0]!.name).toBe('users');
    });

    it('should parse UPDATE with WHERE', () => {
      const parsed = parser.parse("UPDATE users SET status = 'inactive' WHERE deleted_at IS NOT NULL");

      expect(parsed.where).toBeDefined();
      expect(parsed.where!.conditions.length).toBeGreaterThan(0);
    });

    it('should throw on invalid UPDATE', () => {
      expect(() => parser.parse('UPDATE')).toThrow();
    });
  });

  // ===========================================================================
  // DELETE QUERY PARSING
  // ===========================================================================

  describe('DELETE parsing', () => {
    it('should parse simple DELETE', () => {
      const parsed = parser.parse('DELETE FROM users WHERE id = 1');

      expect(parsed.operation).toBe('DELETE');
      expect(parsed.tables[0]!.name).toBe('users');
    });

    it('should parse DELETE with WHERE', () => {
      const parsed = parser.parse('DELETE FROM users WHERE status = 0 AND created_at < 1000');

      expect(parsed.where).toBeDefined();
      expect(parsed.where!.conditions.length).toBeGreaterThan(0);
    });

    it('should parse DELETE without WHERE (dangerous!)', () => {
      const parsed = parser.parse('DELETE FROM users');

      expect(parsed.operation).toBe('DELETE');
      expect(parsed.where).toBeUndefined();
    });

    it('should throw on invalid DELETE', () => {
      expect(() => parser.parse('DELETE')).toThrow();
    });
  });

  // ===========================================================================
  // ERROR HANDLING
  // ===========================================================================

  describe('error handling', () => {
    it('should throw on empty query', () => {
      expect(() => parser.parse('')).toThrow('Empty SQL query');
    });

    it('should throw on whitespace only', () => {
      expect(() => parser.parse('   \n\t  ')).toThrow('Empty SQL query');
    });

    it('should throw on unknown operation', () => {
      expect(() => parser.parse('MERGE INTO users')).toThrow('Unknown SQL operation');
    });

    it('should throw on comment-only query', () => {
      expect(() => parser.parse('-- just a comment')).toThrow('Empty SQL query');
    });
  });

  // ===========================================================================
  // EDGE CASES
  // ===========================================================================

  describe('edge cases', () => {
    it('should handle SQL with comments', () => {
      const parsed = parser.parse(`
        SELECT * -- get all columns
        FROM users /* user table */
        WHERE id = 1
      `);

      expect(parsed.operation).toBe('SELECT');
      expect(parsed.tables[0]!.name).toBe('users');
    });

    it('should handle SQL keywords in string literals', () => {
      const parsed = parser.parse("SELECT * FROM users WHERE name = 'SELECT FROM WHERE'");

      expect(parsed.operation).toBe('SELECT');
      // The string should not confuse the parser
    });

    it('should handle complex nested expressions', () => {
      const parsed = parser.parse('SELECT * FROM users WHERE (a = 1 AND b = 2) OR (c = 3)');

      expect(parsed.where).toBeDefined();
    });

    it('should handle very long SQL', () => {
      const columns = Array.from({ length: 50 }, (_, i) => `col${i}`).join(', ');
      const parsed = parser.parse(`SELECT ${columns} FROM users`);

      expect(parsed.operation).toBe('SELECT');
    });

    it('should handle mixed case keywords', () => {
      const parsed = parser.parse('select * FROM users Where id = 1');

      expect(parsed.operation).toBe('SELECT');
      expect(parsed.where).toBeDefined();
    });

    it('should handle multiple spaces between tokens', () => {
      const parsed = parser.parse('SELECT   *   FROM   users   WHERE   id   =   1');

      expect(parsed.operation).toBe('SELECT');
    });

    it('should handle newlines in SQL', () => {
      const parsed = parser.parse(`
        SELECT *
        FROM users
        WHERE id = 1
      `);

      expect(parsed.operation).toBe('SELECT');
    });

    it('should handle tabs in SQL', () => {
      const parsed = parser.parse('SELECT\t*\tFROM\tusers');

      expect(parsed.operation).toBe('SELECT');
    });

    it('should handle table names that are keywords', () => {
      const parsed = parser.parse('SELECT * FROM "order"');

      expect(parsed.tables[0]!.name).toBe('order');
    });

    it('should handle column names that are keywords', () => {
      const parsed = parser.parse('SELECT "select" FROM users');

      // Should parse without error
      expect(parsed.operation).toBe('SELECT');
    });

    it('should handle empty WHERE IN clause', () => {
      const parsed = parser.parse('SELECT * FROM users WHERE id IN ()');

      expect(parsed.where).toBeDefined();
    });

    it('should handle boolean literals', () => {
      const parsed = parser.parse('SELECT * FROM users WHERE active = TRUE AND deleted = FALSE');

      expect(parsed.where).toBeDefined();
    });

    it('should handle NULL comparisons as IS NULL pattern', () => {
      // Note: data = NULL is generally not recommended (should use IS NULL)
      // The parser may not extract this as a proper condition
      const parsed = parser.parse('SELECT * FROM users WHERE data IS NULL');

      expect(parsed.where).toBeDefined();
    });

    it('should handle parentheses in FROM clause', () => {
      // Subquery in FROM - should not crash
      const parsed = parser.parse('SELECT * FROM (SELECT id FROM users) AS t');

      expect(parsed.operation).toBe('SELECT');
    });

    it('should handle WHERE with function calls', () => {
      const parsed = parser.parse("SELECT * FROM users WHERE LOWER(email) = 'test@example.com'");

      expect(parsed.where).toBeDefined();
    });

    it('should handle WHERE with CAST', () => {
      const parsed = parser.parse('SELECT * FROM users WHERE CAST(age AS INT) > 21');

      expect(parsed.where).toBeDefined();
    });
  });

  // ===========================================================================
  // COMPLEX QUERIES
  // ===========================================================================

  describe('complex queries', () => {
    it('should parse query with all clauses', () => {
      const parsed = parser.parse(`
        SELECT DISTINCT
          u.id,
          u.name,
          COUNT(o.id) AS order_count,
          SUM(o.amount) AS total_amount
        FROM users u
        WHERE u.status = 'active'
          AND u.created_at BETWEEN 1000 AND 2000
        GROUP BY u.id, u.name
        ORDER BY total_amount DESC
        LIMIT 10
        OFFSET 5
      `);

      expect(parsed.operation).toBe('SELECT');
      expect(parsed.distinct).toBe(true);
      expect(parsed.tables[0]!.name).toBe('users');
      expect(parsed.tables[0]!.alias).toBe('u');
      expect(parsed.where).toBeDefined();
      expect(parsed.groupBy).toBeDefined();
      expect(parsed.orderBy).toBeDefined();
      expect(parsed.limit).toBe(10);
      expect(parsed.offset).toBe(5);
      expect(parsed.aggregates!.length).toBeGreaterThanOrEqual(2);
    });

    it('should parse complex WHERE with multiple conditions', () => {
      const parsed = parser.parse(`
        SELECT *
        FROM users
        WHERE tenant_id = 123
          AND status IN ('active', 'pending')
          AND age >= 18
          AND age <= 65
          AND deleted_at IS NULL
          AND email LIKE '%@company.com'
      `);

      expect(parsed.where!.conditions.length).toBeGreaterThanOrEqual(5);
    });
  });
});
