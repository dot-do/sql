/**
 * SQL Injection Prevention Tests
 *
 * Tests that parameterized queries are properly handled and that
 * user input cannot break out of intended SQL value contexts.
 */

import { describe, it, expect } from 'vitest';
import { SELF } from 'cloudflare:test';

/**
 * Helper to execute SQL against a named DO instance
 */
async function execute(dbName: string, sql: string, params?: Record<string, unknown>) {
  const response = await SELF.fetch(`http://localhost/db/${dbName}/execute`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql, params }),
  });
  return response.json() as Promise<{ success: boolean; error?: string; rows?: Record<string, unknown>[]; stats?: { rowsAffected: number } }>;
}

async function query(dbName: string, sql: string, params?: Record<string, unknown>) {
  const response = await SELF.fetch(`http://localhost/db/${dbName}/query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql, params }),
  });
  return response.json() as Promise<{ success: boolean; error?: string; rows?: Record<string, unknown>[]; stats?: { rowsAffected: number } }>;
}

describe('SQL Injection Prevention', () => {
  describe('Parameter substitution', () => {
    it('should substitute named parameters in INSERT values', async () => {
      const db = 'inject-params-insert';
      await execute(db, 'CREATE TABLE users (id INTEGER, name TEXT, email TEXT, PRIMARY KEY (id))');
      const result = await execute(db, "INSERT INTO users (id, name, email) VALUES (:id, :name, :email)", {
        id: 1,
        name: "O'Brien",
        email: 'obrien@test.com',
      });
      expect(result.success).toBe(true);

      // Verify the data was stored correctly with the apostrophe
      const selectResult = await query(db, 'SELECT * FROM users WHERE id = 1');
      expect(selectResult.success).toBe(true);
      expect(selectResult.rows).toHaveLength(1);
      expect(selectResult.rows![0].name).toBe("O'Brien");
    });

    it('should substitute named parameters in WHERE clause', async () => {
      const db = 'inject-params-where';
      await execute(db, 'CREATE TABLE items (id INTEGER, title TEXT, PRIMARY KEY (id))');
      await execute(db, "INSERT INTO items (id, title) VALUES (1, 'safe item')");
      await execute(db, "INSERT INTO items (id, title) VALUES (2, 'other item')");

      // Use parameterized WHERE clause
      const result = await query(db, "SELECT * FROM items WHERE title = :title", {
        title: 'safe item',
      });
      expect(result.success).toBe(true);
      expect(result.rows).toHaveLength(1);
      expect(result.rows![0].title).toBe('safe item');
    });

    it('should substitute named parameters in UPDATE SET and WHERE', async () => {
      const db = 'inject-params-update';
      await execute(db, 'CREATE TABLE products (id INTEGER, name TEXT, price INTEGER, PRIMARY KEY (id))');
      await execute(db, "INSERT INTO products (id, name, price) VALUES (1, 'Widget', 100)");

      const result = await execute(db, "UPDATE products SET name = :name WHERE id = :id", {
        name: "Widget's Deluxe",
        id: 1,
      });
      expect(result.success).toBe(true);
    });
  });

  describe('Escaped single quotes in literals', () => {
    it('should handle SQL-escaped single quotes in INSERT values', async () => {
      const db = 'inject-escaped-insert';
      await execute(db, 'CREATE TABLE notes (id INTEGER, content TEXT, PRIMARY KEY (id))');
      // SQL standard: two single quotes '' represent a literal single quote
      const result = await execute(db, "INSERT INTO notes (id, content) VALUES (1, 'it''s a test')");
      expect(result.success).toBe(true);

      const selectResult = await query(db, 'SELECT * FROM notes WHERE id = 1');
      expect(selectResult.success).toBe(true);
      expect(selectResult.rows).toHaveLength(1);
      expect(selectResult.rows![0].content).toBe("it's a test");
    });
  });

  describe('Injection attack vectors', () => {
    it('should not allow value breakout via unescaped quotes in INSERT', async () => {
      const db = 'inject-breakout-insert';
      await execute(db, 'CREATE TABLE logs (id INTEGER, msg TEXT, PRIMARY KEY (id))');

      // Attempt injection via parameter - the value should be treated as data, not SQL
      const result = await execute(db, "INSERT INTO logs (id, msg) VALUES (:id, :msg)", {
        id: 1,
        msg: "'; DROP TABLE logs; --",
      });
      expect(result.success).toBe(true);

      // Table should still exist and contain the malicious string as data
      const selectResult = await query(db, 'SELECT * FROM logs WHERE id = 1');
      expect(selectResult.success).toBe(true);
      expect(selectResult.rows).toHaveLength(1);
      expect(selectResult.rows![0].msg).toBe("'; DROP TABLE logs; --");
    });

    it('should not allow WHERE clause manipulation via parameters', async () => {
      const db = 'inject-breakout-where';
      await execute(db, 'CREATE TABLE secrets (id INTEGER, data TEXT, PRIMARY KEY (id))');
      await execute(db, "INSERT INTO secrets (id, data) VALUES (1, 'secret1')");
      await execute(db, "INSERT INTO secrets (id, data) VALUES (2, 'secret2')");

      // Attempt tautology injection via parameter
      const result = await query(db, "SELECT * FROM secrets WHERE data = :data", {
        data: "' OR '1'='1",
      });
      expect(result.success).toBe(true);
      // Should return 0 rows (no row has that literal value), not all rows
      expect(result.rows).toHaveLength(0);
    });
  });
});
