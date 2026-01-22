/**
 * SQL Injection Tests for MockQueryExecutor
 *
 * These tests document SQL injection vulnerabilities in the MockQueryExecutor's
 * regex-based SQL parsing. The naive regex patterns do not properly handle:
 * - Semicolons in string literals
 * - SQL comments (-- and /‌*...*‌/)
 * - Escaped quotes
 * - Parameter markers inside string literals (false positives)
 * - Multi-statement injection attempts
 *
 * Tests marked with `it.fails()` document expected SECURE behavior that is
 * NOT currently implemented. These tests SHOULD pass once proper SQL parsing
 * is implemented but currently fail because the MockQueryExecutor is vulnerable.
 *
 * @see /packages/dosql/src/rpc/server.ts:714-847
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { MockQueryExecutor } from '../index.js';

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
    it.fails('should NOT execute multiple statements when semicolon is in string literal', async () => {
      // A user might legitimately want to insert a value containing a semicolon
      // The naive parser would see this as two separate statements
      const sql = `INSERT INTO users (name) VALUES ('test; DROP TABLE users--')`;

      // Expected secure behavior: This should be treated as a single INSERT
      // with a string value containing "; DROP TABLE users--"
      // Current behavior: The regex parser may misinterpret this
      const result = await executor.execute(sql);

      // Should succeed as single INSERT
      expect(result.rowCount).toBe(1);

      // Verify the users table still exists and has data
      const selectResult = await executor.execute('SELECT * FROM users');
      expect(selectResult.rows.length).toBeGreaterThan(0);
    });

    it.fails('should treat semicolon inside single quotes as literal text', async () => {
      // Text like "Hello; world" is valid content
      const sql = `SELECT * FROM users WHERE name = 'test; SELECT * FROM secrets'`;

      // Expected: Should search for literal string "test; SELECT * FROM secrets"
      // Not execute a second query
      const result = await executor.execute(sql);

      // Should only return users matching that literal string (none in test data)
      expect(result.rows).toHaveLength(0);
    });

    it.fails('should handle multiple semicolons in string literals safely', async () => {
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
    it.fails('should NOT interpret -- as comment when inside string literal', async () => {
      // The string "test--comment" is valid data
      const sql = `SELECT * FROM users WHERE name = 'admin'--' AND role = 'user'`;

      // VULNERABLE: The -- could be used to comment out the rest of the query
      // Expected secure behavior: Treat '--' as part of string (though malformed SQL)
      // This should fail as invalid SQL, not execute with injected comment

      // If the parser is vulnerable, it might:
      // 1. Comment out " AND role = 'user'" allowing admin access bypass
      const result = await executor.execute(sql);

      // A secure parser would either:
      // - Throw syntax error (unclosed string)
      // - Or require the full WHERE clause to match
      expect(result.rows).toHaveLength(0); // No user named literally "admin'--'"
    });

    it.fails('should NOT interpret block comments when inside string literal', async () => {
      const sql = `SELECT * FROM users WHERE name = 'test/*'`;

      // Expected: Treat /* as literal characters in string
      // Vulnerable parsers might see this as start of block comment
      const result = await executor.execute(sql);

      // Should find no users with name literally "test/*"
      expect(result.rows).toHaveLength(0);
    });

    it.fails('should reject queries with comment-based injection attempts', async () => {
      // Classic comment injection to bypass password check
      const sql = `SELECT * FROM users WHERE name = 'admin'/*' AND password = 'wrong'*/`;

      // This attempts to comment out the password check
      // A secure parser should either reject or properly handle this

      const result = await executor.execute(sql);

      // Should NOT return admin user without proper password check
      expect(result.rows).toHaveLength(0);
    });

    it.fails('should handle nested comment attempts safely', async () => {
      const sql = `SELECT * FROM users WHERE name = 'test /* /* nested */ */'`;

      // Nested comments can confuse simple parsers
      const result = await executor.execute(sql);

      // Should either reject as syntax error or treat as literal string
      expect(result.rows).toHaveLength(0);
    });
  });

  // ===========================================================================
  // Escaped Quote Injection
  // ===========================================================================

  describe('Escaped Quote Injection', () => {
    it.fails('should handle escaped single quotes (doubled) correctly', async () => {
      // Standard SQL escaping: '' represents a literal single quote
      const sql = `SELECT * FROM users WHERE name = 'O''Brien'`;

      // Expected: Search for user named "O'Brien"
      const result = await executor.execute(sql);

      // Should handle escaped quote properly
      expect(result.columns).toContain('name');
    });

    it.fails('should NOT be fooled by escaped quote injection', async () => {
      // Injection attempt using escaped quotes
      const sql = `SELECT * FROM users WHERE name = 'test'' OR ''1''=''1'`;

      // This attempts to create always-true condition via quote escaping
      // Expected: Either reject or treat as literal string search
      const result = await executor.execute(sql);

      // Should NOT return all users
      expect(result.rows.length).toBeLessThan(3);
    });

    it.fails('should handle backslash-escaped quotes (non-standard)', async () => {
      // Some databases support backslash escaping
      const sql = `SELECT * FROM users WHERE name = 'test\\'--injection'`;

      // A robust parser should handle this without breaking
      const result = await executor.execute(sql);

      // Should search for literal string, not inject
      expect(result.rows).toHaveLength(0);
    });

    it.fails('should handle mixed escaping attempts', async () => {
      const sql = `INSERT INTO users (name) VALUES ('O''Brien; DROP TABLE--')`;

      // Contains escaped quote AND injection attempt
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
    it.fails('should NOT treat $1 inside string literal as parameter marker', async () => {
      // The string "$1 discount" is valid content, not a parameter
      const sql = `SELECT * FROM users WHERE name = 'Price: $1.00'`;

      // Expected: Treat $1.00 as literal text, not parameter marker
      // Vulnerable: Regex might extract $1 as parameter, causing binding mismatch
      const result = await executor.execute(sql, []); // No params provided

      // Should execute without error (no params needed)
      expect(result.columns).toBeDefined();
    });

    it.fails('should NOT extract :param from inside string literals', async () => {
      // Named parameters like :name shouldn't be extracted from strings
      const sql = `SELECT * FROM users WHERE bio = 'Contact: :email@example.com'`;

      // Should treat :email as literal text
      const result = await executor.execute(sql, []);

      expect(result.columns).toBeDefined();
    });

    it.fails('should NOT extract ? markers from inside string literals', async () => {
      // Question marks in strings are not parameters
      const sql = `SELECT * FROM users WHERE name = 'What? Really?'`;

      // Should not require 2 parameter values
      const result = await executor.execute(sql, []);

      expect(result.columns).toBeDefined();
    });

    it.fails('should correctly count parameters outside string literals only', async () => {
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
    it.fails('should reject or isolate multiple statements separated by semicolon', async () => {
      const sql = `SELECT * FROM users; DROP TABLE secrets`;

      // Expected secure behavior:
      // 1. Reject entirely (safest)
      // 2. Execute only first statement
      // 3. Never execute DROP

      // Should throw or return only SELECT results
      await expect(async () => {
        const result = await executor.execute(sql);
        // If it doesn't throw, verify DROP wasn't executed
        const secretsExist = await executor.execute('SELECT * FROM secrets');
        expect(secretsExist.rows.length).toBeGreaterThan(0);
      }).not.toThrow();
    });

    it.fails('should reject batched UPDATE followed by SELECT', async () => {
      const sql = `UPDATE users SET role = 'admin'; SELECT * FROM users`;

      // Should not execute UPDATE portion
      await expect(executor.execute(sql)).rejects.toThrow();
    });

    it.fails('should prevent INSERT followed by malicious SELECT', async () => {
      const sql = `INSERT INTO users (name) VALUES ('test'); SELECT password FROM users WHERE role = 'admin'`;

      // Should not leak admin password
      const result = await executor.execute(sql);

      // Should either throw or only return INSERT result
      expect(result.columns).not.toContain('password');
    });

    it.fails('should handle null byte injection attempts', async () => {
      // Null byte can terminate strings in some languages
      const sql = `SELECT * FROM users WHERE name = 'test\x00'; DROP TABLE users--'`;

      // Should handle null byte safely
      const result = await executor.execute(sql);

      // Verify users table still intact
      const verify = await executor.execute('SELECT * FROM users');
      expect(verify.rows.length).toBe(3);
    });
  });

  // ===========================================================================
  // UNION-based Injection
  // ===========================================================================

  describe('UNION-based Injection', () => {
    it.fails('should NOT allow UNION injection to access other tables', async () => {
      // Classic UNION injection to read from another table
      const sql = `SELECT id, name FROM users WHERE name = 'x' UNION SELECT id, data FROM secrets--'`;

      // Expected: Either reject or only return users data
      const result = await executor.execute(sql);

      // Should NOT contain secrets table data
      const containsSecrets = result.rows.some(
        (row) => row.includes('classified_data') || row.includes('api_key_12345')
      );
      expect(containsSecrets).toBe(false);
    });

    it.fails('should handle UNION in string literal safely', async () => {
      const sql = `SELECT * FROM users WHERE name = 'UNION SELECT * FROM secrets'`;

      // UNION in string should be literal, not SQL keyword
      const result = await executor.execute(sql);

      // Should search for literal string containing "UNION SELECT..."
      expect(result.rows).toHaveLength(0);
    });
  });

  // ===========================================================================
  // Case Sensitivity and Encoding Attacks
  // ===========================================================================

  describe('Case Sensitivity and Encoding Attacks', () => {
    it.fails('should handle mixed case SQL keywords in injection', async () => {
      // Attempt to bypass simple uppercase checks
      const sql = `SELECT * FROM users; DrOp TaBlE secrets`;

      // Should reject regardless of case
      await expect(executor.execute(sql)).rejects.toThrow();
    });

    it.fails('should handle unicode lookalikes in SQL keywords', async () => {
      // Using unicode characters that look like ASCII
      // e.g., using fullwidth semicolon U+FF1B
      const sql = `SELECT * FROM users\uFF1B DROP TABLE secrets`;

      // Should handle unicode safely
      const result = await executor.execute(sql);

      // Verify secrets table still exists
      const verify = await executor.execute('SELECT * FROM secrets');
      expect(verify.rows.length).toBe(2);
    });
  });

  // ===========================================================================
  // Table Name Extraction Vulnerabilities
  // ===========================================================================

  describe('Table Name Extraction Vulnerabilities', () => {
    it.fails('should properly extract table name from complex FROM clause', async () => {
      // The regex /FROM\s+(\w+)/ is too naive
      const sql = `SELECT * FROM users, secrets WHERE users.id = secrets.id`;

      // Current implementation only extracts first table name
      // Should either support JOINs or reject
      const result = await executor.execute(sql);

      // If supporting JOIN, should return joined data
      // If not supporting, should throw clear error
      expect(result.columns.length).toBeGreaterThan(0);
    });

    it.fails('should handle subquery in FROM clause', async () => {
      const sql = `SELECT * FROM (SELECT * FROM secrets) AS subq`;

      // Naive regex won't handle this
      // Should either support or clearly reject
      await expect(executor.execute(sql)).rejects.toThrow('subqueries not supported');
    });

    it.fails('should reject table names with injection in identifier', async () => {
      // Attempt to inject via table name
      const sql = `SELECT * FROM users; DROP TABLE secrets; SELECT * FROM test`;

      // Should not extract "users; DROP TABLE secrets; SELECT * FROM test" as table name
      await expect(executor.execute(sql)).rejects.toThrow();
    });
  });

  // ===========================================================================
  // Denial of Service via Malformed Input
  // ===========================================================================

  describe('Denial of Service via Malformed Input', () => {
    it.fails('should handle extremely long SQL statements without hanging', async () => {
      // ReDoS prevention - regex should not hang on long input
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

    it.fails('should handle deeply nested parentheses', async () => {
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
