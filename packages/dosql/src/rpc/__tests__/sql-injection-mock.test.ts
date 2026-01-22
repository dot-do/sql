/**
 * SQL Injection Tests for MockQueryExecutor
 *
 * These tests verify that the MockQueryExecutor's tokenization-based SQL parsing
 * properly handles:
 * - Semicolons in string literals (not treated as statement separators)
 * - SQL comments (-- and block comments)
 * - Escaped quotes ('')
 * - Parameter markers inside string literals (not extracted)
 * - Multi-statement injection attempts (rejected)
 *
 * The MockQueryExecutor uses proper tokenization instead of naive regex patterns
 * to avoid SQL injection vulnerabilities in the mock test implementation.
 *
 * NOTE: The mock does NOT evaluate WHERE clauses - it returns all rows from
 * a table. The security tests verify that injection attempts are properly
 * tokenized and either rejected or treated as literal strings.
 *
 * @see /packages/dosql/src/rpc/server.ts:714-847
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { MockQueryExecutor } from '../../__tests__/utils/index.js';

describe('MockQueryExecutor SQL Injection Vulnerabilities', () => {
  let executor: MockQueryExecutor;

  beforeEach(() => {
    executor = new MockQueryExecutor();
    // Set up test tables
    executor.addTable('users', ['id', 'name', 'password', 'role'], ['number', 'string', 'string', 'string'], [
      [1, 'admin', 'secret123', 'admin'],
      [2, 'alice', 'password1', 'user'],
      [3, 'bob', 'password2', 'user'],
    ]);
    executor.addTable('secrets', ['id', 'data'], ['number', 'string'], [
      [1, 'classified_data'],
      [2, 'api_key_12345'],
    ]);
  });

  // ===========================================================================
  // Semicolon Injection via String Literals
  // ===========================================================================

  describe('Semicolon in String Literals', () => {
    it('should NOT execute multiple statements when semicolon is in string literal', async () => {
      // A user might legitimately want to insert a value containing a semicolon
      // The tokenizer should treat the semicolon as part of the string literal
      const sql = `INSERT INTO users (name) VALUES ('test; DROP TABLE users--')`;

      // Expected secure behavior: This should be treated as a single INSERT
      // with a string value containing "; DROP TABLE users--"
      const result = await executor.execute(sql);

      // Should succeed as single INSERT (semicolon inside string is not a statement separator)
      expect(result.rowCount).toBe(1);

      // Verify the users table still exists and has data
      const selectResult = await executor.execute('SELECT * FROM users');
      expect(selectResult.rows.length).toBeGreaterThan(0);
    });

    it('should treat semicolon inside single quotes as literal text', async () => {
      // Text like "Hello; world" is valid content
      // The tokenizer should recognize this as a single SELECT statement
      const sql = `SELECT * FROM users WHERE name = 'test; SELECT * FROM secrets'`;

      // Expected: Should execute as single SELECT (not two statements)
      // The mock returns all users since it doesn't evaluate WHERE clauses
      const result = await executor.execute(sql);

      // Mock returns all rows from users table - key point is no error/injection
      expect(result.columns).toContain('name');
      expect(result.rows.length).toBe(3); // All 3 users
    });

    it('should handle multiple semicolons in string literals safely', async () => {
      const sql = `INSERT INTO users (name) VALUES ('a;b;c;d;e')`;

      // Should treat all semicolons as part of the string value
      const result = await executor.execute(sql);
      expect(result.rowCount).toBe(1);
    });
  });

  // ===========================================================================
  // SQL Comment Injection
  // ===========================================================================

  describe('SQL Comment Injection', () => {
    it('should properly tokenize -- comment outside string literal', async () => {
      // The -- here is OUTSIDE the string literal (after closing quote)
      // This is a valid SQL comment that comments out the rest
      const sql = `SELECT * FROM users WHERE name = 'admin'--' AND role = 'user'`;

      // The tokenizer sees: SELECT * FROM users WHERE name = 'admin' [comment]
      // This is valid SQL - the mock returns all users (doesn't evaluate WHERE)
      const result = await executor.execute(sql);

      // Mock returns all rows from users table
      expect(result.columns).toContain('name');
      expect(result.rows.length).toBe(3);
    });

    it('should NOT interpret block comments when inside string literal', async () => {
      const sql = `SELECT * FROM users WHERE name = 'test/*'`;

      // The /* is inside the string, should be treated as literal
      // Mock returns all users (doesn't evaluate WHERE)
      const result = await executor.execute(sql);

      expect(result.columns).toContain('name');
      expect(result.rows.length).toBe(3);
    });

    it('should handle block comment injection attempts', async () => {
      // This SQL has an unclosed string followed by a block comment
      const sql = `SELECT * FROM users WHERE name = 'admin'/*' AND password = 'wrong'*/`;

      // Tokenizer sees: 'admin' then /* comment */
      // Mock returns all users (doesn't evaluate WHERE)
      const result = await executor.execute(sql);

      expect(result.columns).toContain('name');
      expect(result.rows.length).toBe(3);
    });

    it('should handle nested comment attempts safely', async () => {
      const sql = `SELECT * FROM users WHERE name = 'test /* /* nested */ */'`;

      // Everything between quotes is a string literal
      const result = await executor.execute(sql);

      expect(result.columns).toContain('name');
      expect(result.rows.length).toBe(3);
    });
  });

  // ===========================================================================
  // Escaped Quote Injection
  // ===========================================================================

  describe('Escaped Quote Injection', () => {
    it('should handle escaped single quotes (doubled) correctly', async () => {
      // Standard SQL escaping: '' represents a literal single quote
      const sql = `SELECT * FROM users WHERE name = 'O''Brien'`;

      // Expected: Tokenizer properly handles '' escape sequence
      const result = await executor.execute(sql);

      // Should handle escaped quote properly
      expect(result.columns).toContain('name');
    });

    it('should handle escaped quote injection attempt', async () => {
      // Injection attempt using escaped quotes
      const sql = `SELECT * FROM users WHERE name = 'test'' OR ''1''=''1'`;

      // The tokenizer should see this as a single string: test' OR '1'='1
      // Mock returns all users (doesn't evaluate WHERE)
      const result = await executor.execute(sql);

      // Key security check: this is parsed as a single statement
      expect(result.columns).toContain('name');
      expect(result.rows.length).toBe(3);
    });

    it('should handle backslash-escaped quotes (non-standard)', async () => {
      // Some databases support backslash escaping
      const sql = `SELECT * FROM users WHERE name = 'test\\'--injection'`;

      // Tokenizer handles backslash escapes
      const result = await executor.execute(sql);

      // Should execute as single SELECT
      expect(result.columns).toContain('name');
      expect(result.rows.length).toBe(3);
    });

    it('should handle mixed escaping attempts', async () => {
      const sql = `INSERT INTO users (name) VALUES ('O''Brien; DROP TABLE--')`;

      // Contains escaped quote AND injection attempt inside string
      // Should insert the literal string "O'Brien; DROP TABLE--"
      const result = await executor.execute(sql);
      expect(result.rowCount).toBe(1);

      // Verify users table still exists
      const verify = await executor.execute('SELECT * FROM users');
      expect(verify.rows.length).toBeGreaterThan(0);
    });
  });

  // ===========================================================================
  // Parameter Marker False Positives
  // ===========================================================================

  describe('Parameter Marker Extraction from String Literals', () => {
    it('should NOT treat $1 inside string literal as parameter marker', async () => {
      // The string "$1 discount" is valid content, not a parameter
      const sql = `SELECT * FROM users WHERE name = 'Price: $1.00'`;

      // Expected: Treat $1.00 as literal text, not parameter marker
      // Tokenizer puts this in a string token, not a parameter token
      const result = await executor.execute(sql, []); // No params provided

      // Should execute without error (no params needed)
      expect(result.columns).toBeDefined();
    });

    it('should NOT extract :param from inside string literals', async () => {
      // Named parameters like :name shouldn't be extracted from strings
      const sql = `SELECT * FROM users WHERE bio = 'Contact: :email@example.com'`;

      // Should treat :email as literal text
      const result = await executor.execute(sql, []);

      expect(result.columns).toBeDefined();
    });

    it('should NOT extract ? markers from inside string literals', async () => {
      // Question marks in strings are not parameters
      const sql = `SELECT * FROM users WHERE name = 'What? Really?'`;

      // Should not require 2 parameter values
      const result = await executor.execute(sql, []);

      expect(result.columns).toBeDefined();
    });

    it('should correctly count parameters outside string literals only', async () => {
      // Mix of real parameters and false positives
      const sql = `SELECT * FROM users WHERE id = $1 AND name != 'Price: $2.00'`;

      // Should only see $1 as real parameter, not $2 in string
      // Providing one param should be sufficient
      const result = await executor.execute(sql, [1]);

      expect(result.columns).toBeDefined();
    });
  });

  // ===========================================================================
  // Multi-Statement Injection Attempts
  // ===========================================================================

  describe('Multi-Statement Injection', () => {
    it('should reject multiple statements separated by semicolon', async () => {
      const sql = `SELECT * FROM users; DROP TABLE secrets`;

      // Multi-statement queries should be rejected
      await expect(executor.execute(sql)).rejects.toThrow('Multi-statement queries are not supported');
    });

    it('should reject batched UPDATE followed by SELECT', async () => {
      const sql = `UPDATE users SET role = 'admin'; SELECT * FROM users`;

      // Should not execute UPDATE portion
      await expect(executor.execute(sql)).rejects.toThrow('Multi-statement queries are not supported');
    });

    it('should reject INSERT followed by SELECT', async () => {
      const sql = `INSERT INTO users (name) VALUES ('test'); SELECT password FROM users WHERE role = 'admin'`;

      // Multi-statement queries should be rejected
      await expect(executor.execute(sql)).rejects.toThrow('Multi-statement queries are not supported');
    });

    it('should handle null byte injection attempts', async () => {
      // Null byte can terminate strings in some languages
      // The tokenizer skips null bytes, so this becomes:
      // SELECT * FROM users WHERE name = 'test'; DROP TABLE users--'
      const sql = `SELECT * FROM users WHERE name = 'test\x00'; DROP TABLE users--'`;

      // This should be rejected as multi-statement (null byte is skipped)
      await expect(executor.execute(sql)).rejects.toThrow('Multi-statement queries are not supported');
    });
  });

  // ===========================================================================
  // UNION-based Injection
  // ===========================================================================

  describe('UNION-based Injection', () => {
    it('should reject UNION injection attempts', async () => {
      // Classic UNION injection to read from another table
      const sql = `SELECT id, name FROM users WHERE name = 'x' UNION SELECT id, data FROM secrets--'`;

      // UNION queries are rejected by the mock
      await expect(executor.execute(sql)).rejects.toThrow('UNION queries are not supported');
    });

    it('should handle UNION in string literal safely', async () => {
      const sql = `SELECT * FROM users WHERE name = 'UNION SELECT * FROM secrets'`;

      // UNION in string should be literal, not SQL keyword
      // The tokenizer correctly identifies this as a string token
      const result = await executor.execute(sql);

      // Should execute as normal SELECT (UNION is inside string, not a keyword)
      // Returns users table data (mock doesn't evaluate WHERE)
      expect(result.rows.length).toBe(3);
    });
  });

  // ===========================================================================
  // Case Sensitivity and Encoding Attacks
  // ===========================================================================

  describe('Case Sensitivity and Encoding Attacks', () => {
    it('should handle mixed case SQL keywords in injection', async () => {
      // Attempt to bypass simple uppercase checks
      const sql = `SELECT * FROM users; DrOp TaBlE secrets`;

      // Should reject regardless of case (multi-statement)
      await expect(executor.execute(sql)).rejects.toThrow('Multi-statement queries are not supported');
    });

    it('should handle unicode lookalikes in SQL keywords', async () => {
      // Using unicode characters that look like ASCII
      // e.g., using fullwidth semicolon U+FF1B
      const sql = `SELECT * FROM users\uFF1B DROP TABLE secrets`;

      // Fullwidth semicolon is not a real semicolon, so this is treated as a single statement
      // The tokenizer skips the unknown unicode character
      const result = await executor.execute(sql);

      // Verify secrets table still exists (unicode semicolon didn't create multi-statement)
      const verify = await executor.execute('SELECT * FROM secrets');
      expect(verify.rows.length).toBe(2);
    });
  });

  // ===========================================================================
  // Table Name Extraction Vulnerabilities
  // ===========================================================================

  describe('Table Name Extraction Vulnerabilities', () => {
    it('should reject implicit joins (comma syntax)', async () => {
      // The regex /FROM\s+(\w+)/ is too naive - proper tokenization detects this
      const sql = `SELECT * FROM users, secrets WHERE users.id = secrets.id`;

      // Implicit joins are rejected for security
      await expect(executor.execute(sql)).rejects.toThrow('Implicit joins (comma syntax) are not supported');
    });

    it('should handle subquery in FROM clause', async () => {
      const sql = `SELECT * FROM (SELECT * FROM secrets) AS subq`;

      // Naive regex won't handle this - proper tokenization detects and rejects
      await expect(executor.execute(sql)).rejects.toThrow('subqueries not supported');
    });

    it('should reject table names with injection in identifier', async () => {
      // Attempt to inject via table name
      const sql = `SELECT * FROM users; DROP TABLE secrets; SELECT * FROM test`;

      // Should reject as multi-statement
      await expect(executor.execute(sql)).rejects.toThrow('Multi-statement queries are not supported');
    });
  });

  // ===========================================================================
  // Denial of Service via Malformed Input
  // ===========================================================================

  describe('Denial of Service via Malformed Input', () => {
    it('should handle extremely long SQL statements without hanging', async () => {
      // ReDoS prevention - tokenizer should not hang on long input
      const longString = 'a'.repeat(100000);
      const sql = `SELECT * FROM users WHERE name = '${longString}'`;

      const startTime = Date.now();
      try {
        await executor.execute(sql);
      } catch {
        // Expected to fail, but should fail fast
      }
      const elapsed = Date.now() - startTime;

      // Should complete in reasonable time (< 1 second)
      expect(elapsed).toBeLessThan(1000);
    });

    it('should handle deeply nested parentheses', async () => {
      const nested = '('.repeat(1000) + 'SELECT 1' + ')'.repeat(1000);
      const sql = `SELECT * FROM users WHERE id IN (${nested})`;

      const startTime = Date.now();
      try {
        await executor.execute(sql);
      } catch {
        // Expected to fail, but fast
      }
      const elapsed = Date.now() - startTime;

      expect(elapsed).toBeLessThan(1000);
    });
  });
});
