/**
 * SQL Sanitizer Tests
 *
 * Tests for SQL statement sanitization, statement type extraction,
 * and table name extraction.
 */

import { describe, it, expect } from 'vitest';
import { SQLSanitizerImpl, createSQLSanitizer } from '../sanitizer.js';

describe('SQLSanitizer', () => {
  const sanitizer = new SQLSanitizerImpl();

  // ===========================================================================
  // sanitize()
  // ===========================================================================

  describe('sanitize', () => {
    it('replaces single-quoted string literals with placeholders', () => {
      const result = sanitizer.sanitize("SELECT * FROM users WHERE name = 'Alice'");
      expect(result).toBe("SELECT * FROM users WHERE name = '?'");
    });

    it('replaces multiple string literals', () => {
      const result = sanitizer.sanitize("SELECT * FROM users WHERE name = 'Alice' AND email = 'alice@example.com'");
      expect(result).toBe("SELECT * FROM users WHERE name = '?' AND email = '?'");
    });

    it('handles escaped quotes within strings', () => {
      const result = sanitizer.sanitize("SELECT * FROM users WHERE name = 'O\\'Brien'");
      expect(result).toBe("SELECT * FROM users WHERE name = '?'");
    });

    it('replaces long numeric literals (10+ digits)', () => {
      const result = sanitizer.sanitize('SELECT * FROM users WHERE ssn = 1234567890');
      expect(result).toBe('SELECT * FROM users WHERE ssn = ?');
    });

    it('preserves short numeric literals', () => {
      const result = sanitizer.sanitize('SELECT * FROM users WHERE id = 42');
      expect(result).toBe('SELECT * FROM users WHERE id = 42');
    });

    it('replaces long hex strings that may be tokens', () => {
      const result = sanitizer.sanitize('SELECT * FROM sessions WHERE token = 0xabcdef0123456789');
      expect(result).toBe('SELECT * FROM sessions WHERE token = ?');
    });

    it('normalizes whitespace', () => {
      const result = sanitizer.sanitize('SELECT  *   FROM    users\n  WHERE  id = 1');
      expect(result).toBe('SELECT * FROM users WHERE id = 1');
    });

    it('handles empty string', () => {
      const result = sanitizer.sanitize('');
      expect(result).toBe('');
    });

    it('handles INSERT with values', () => {
      const result = sanitizer.sanitize("INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com')");
      expect(result).toBe("INSERT INTO users (name, email) VALUES ('?', '?')");
    });
  });

  // ===========================================================================
  // extractStatementType()
  // ===========================================================================

  describe('extractStatementType', () => {
    it('identifies SELECT statements', () => {
      expect(sanitizer.extractStatementType('SELECT * FROM users')).toBe('SELECT');
    });

    it('identifies INSERT statements', () => {
      expect(sanitizer.extractStatementType('INSERT INTO users VALUES (1)')).toBe('INSERT');
    });

    it('identifies UPDATE statements', () => {
      expect(sanitizer.extractStatementType('UPDATE users SET name = ?')).toBe('UPDATE');
    });

    it('identifies DELETE statements', () => {
      expect(sanitizer.extractStatementType('DELETE FROM users WHERE id = 1')).toBe('DELETE');
    });

    it('returns OTHER for DDL statements', () => {
      expect(sanitizer.extractStatementType('CREATE TABLE users (id INT)')).toBe('OTHER');
    });

    it('handles leading whitespace', () => {
      expect(sanitizer.extractStatementType('  SELECT * FROM users')).toBe('SELECT');
    });

    it('is case-insensitive', () => {
      expect(sanitizer.extractStatementType('select * from users')).toBe('SELECT');
      expect(sanitizer.extractStatementType('Insert into users values (1)')).toBe('INSERT');
    });
  });

  // ===========================================================================
  // extractTableNames()
  // ===========================================================================

  describe('extractTableNames', () => {
    it('extracts table from SELECT ... FROM', () => {
      const tables = sanitizer.extractTableNames('SELECT * FROM users');
      expect(tables).toContain('users');
    });

    it('extracts table from INSERT INTO', () => {
      const tables = sanitizer.extractTableNames('INSERT INTO orders (id) VALUES (1)');
      expect(tables).toContain('orders');
    });

    it('extracts table from UPDATE', () => {
      const tables = sanitizer.extractTableNames('UPDATE products SET price = 10');
      expect(tables).toContain('products');
    });

    it('extracts table from DELETE FROM', () => {
      const tables = sanitizer.extractTableNames('DELETE FROM sessions WHERE expired = 1');
      expect(tables).toContain('sessions');
    });

    it('extracts tables from JOIN', () => {
      const tables = sanitizer.extractTableNames('SELECT * FROM users JOIN orders ON users.id = orders.user_id');
      expect(tables).toContain('users');
      expect(tables).toContain('orders');
    });

    it('extracts multiple tables from comma-separated FROM', () => {
      const tables = sanitizer.extractTableNames('SELECT * FROM users, orders');
      expect(tables).toContain('users');
      expect(tables).toContain('orders');
    });

    it('does not duplicate table names', () => {
      const tables = sanitizer.extractTableNames('SELECT * FROM users JOIN users ON 1=1');
      const userOccurrences = tables.filter((t) => t === 'users');
      expect(userOccurrences.length).toBe(1);
    });

    it('strips quoting from table names', () => {
      const tables = sanitizer.extractTableNames('SELECT * FROM `users`');
      expect(tables).toContain('users');
    });

    it('returns empty array for DDL without recognized clauses', () => {
      const tables = sanitizer.extractTableNames('BEGIN TRANSACTION');
      expect(tables).toEqual([]);
    });
  });

  // ===========================================================================
  // createSQLSanitizer factory
  // ===========================================================================

  describe('createSQLSanitizer', () => {
    it('returns a working SQLSanitizer instance', () => {
      const s = createSQLSanitizer();
      expect(s.sanitize("SELECT * FROM t WHERE x = 'val'")).toBe("SELECT * FROM t WHERE x = '?'");
      expect(s.extractStatementType('SELECT 1')).toBe('SELECT');
      expect(s.extractTableNames('SELECT * FROM foo')).toContain('foo');
    });
  });
});
