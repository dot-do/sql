/**
 * Input Size Limits Tests
 *
 * Tests for HTTP endpoint input validation:
 * - Request body size limits
 * - SQL query string length limits
 * - Parameter count limits
 *
 * Uses @cloudflare/vitest-pool-workers for miniflare integration.
 */

import { describe, it, expect } from 'vitest';
import { SELF } from 'cloudflare:test';
import { MAX_REQUEST_BODY_SIZE, MAX_SQL_LENGTH, MAX_PARAM_COUNT } from './database.js';

describe('DoSQL Worker - Input Size Limits', () => {
  describe('Content-Length validation', () => {
    it('should reject requests with Content-Length exceeding MAX_REQUEST_BODY_SIZE on /query', async () => {
      const oversizeContentLength = MAX_REQUEST_BODY_SIZE + 1;

      const response = await SELF.fetch('http://localhost/db/limits-cl-query/query', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': String(oversizeContentLength),
        },
        body: JSON.stringify({ sql: 'SELECT 1' }),
      });

      expect(response.status).toBe(413);
      const body = await response.json();
      expect(body.success).toBe(false);
      expect(body.error).toContain('Request body too large');
    });

    it('should reject requests with Content-Length exceeding MAX_REQUEST_BODY_SIZE on /execute', async () => {
      const oversizeContentLength = MAX_REQUEST_BODY_SIZE + 1;

      const response = await SELF.fetch('http://localhost/db/limits-cl-exec/execute', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': String(oversizeContentLength),
        },
        body: JSON.stringify({ sql: 'SELECT 1' }),
      });

      expect(response.status).toBe(413);
      const body = await response.json();
      expect(body.success).toBe(false);
      expect(body.error).toContain('Request body too large');
    });

    it('should allow requests with Content-Length within limits', async () => {
      const smallBody = JSON.stringify({
        sql: 'CREATE TABLE cl_test (id INTEGER, name TEXT, PRIMARY KEY (id))',
      });

      const response = await SELF.fetch('http://localhost/db/limits-cl-ok/execute', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': String(smallBody.length),
        },
        body: smallBody,
      });

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.success).toBe(true);
    });
  });

  describe('SQL query length validation', () => {
    it('should reject oversized SQL queries with 413 status', async () => {
      // Since MAX_REQUEST_BODY_SIZE and MAX_SQL_LENGTH are both 1MB, a SQL string
      // exceeding 1MB will hit the body size check first (because the JSON wrapper
      // adds overhead). This test verifies that oversized SQL payloads are rejected
      // with HTTP 413 regardless of which limit triggers first.
      const oversizeSQL = 'SELECT ' + 'x'.repeat(MAX_SQL_LENGTH + 1);

      const response = await SELF.fetch('http://localhost/db/limits-sql-len/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: oversizeSQL }),
      });

      expect(response.status).toBe(413);
      const body = await response.json();
      expect(body.success).toBe(false);
      // Will be caught by either body size or SQL length check
      expect(body.error).toContain('too large');
    });

    it('should allow SQL queries within limits', async () => {
      const response = await SELF.fetch('http://localhost/db/limits-sql-ok/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'CREATE TABLE sql_ok (id INTEGER, name TEXT, PRIMARY KEY (id))',
        }),
      });

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.success).toBe(true);
    });
  });

  describe('Parameter count validation', () => {
    it('should reject requests with too many parameters', async () => {
      // Create params object exceeding MAX_PARAM_COUNT
      const params: Record<string, unknown> = {};
      for (let i = 0; i < MAX_PARAM_COUNT + 1; i++) {
        params[`param_${i}`] = `value_${i}`;
      }

      const response = await SELF.fetch('http://localhost/db/limits-params/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'SELECT * FROM test',
          params,
        }),
      });

      expect(response.status).toBe(413);
      const body = await response.json();
      expect(body.success).toBe(false);
      expect(body.error).toContain('Too many parameters');
      expect(body.error).toContain(String(MAX_PARAM_COUNT));
    });

    it('should allow requests with parameter count within limits', async () => {
      const params: Record<string, unknown> = {};
      for (let i = 0; i < 10; i++) {
        params[`param_${i}`] = `value_${i}`;
      }

      const response = await SELF.fetch('http://localhost/db/limits-params-ok/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'CREATE TABLE params_test (id INTEGER, name TEXT, PRIMARY KEY (id))',
          params,
        }),
      });

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.success).toBe(true);
    });
  });

  describe('Invalid body validation', () => {
    it('should reject non-JSON request bodies', async () => {
      const response = await SELF.fetch('http://localhost/db/limits-invalid-json/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: 'this is not json',
      });

      expect(response.status).toBe(400);
      const body = await response.json();
      expect(body.success).toBe(false);
      expect(body.error).toContain('Invalid JSON');
    });

    it('should reject requests without sql field', async () => {
      const response = await SELF.fetch('http://localhost/db/limits-no-sql/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: 'SELECT 1' }),
      });

      expect(response.status).toBe(400);
      const body = await response.json();
      expect(body.success).toBe(false);
      expect(body.error).toContain('Missing or invalid "sql" field');
    });

    it('should reject requests with non-string sql field', async () => {
      const response = await SELF.fetch('http://localhost/db/limits-bad-sql/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: 12345 }),
      });

      expect(response.status).toBe(400);
      const body = await response.json();
      expect(body.success).toBe(false);
      expect(body.error).toContain('Missing or invalid "sql" field');
    });
  });

  describe('Exported constants', () => {
    it('should have reasonable MAX_REQUEST_BODY_SIZE (1MB)', () => {
      expect(MAX_REQUEST_BODY_SIZE).toBe(1 * 1024 * 1024);
    });

    it('should have reasonable MAX_SQL_LENGTH (1MB)', () => {
      expect(MAX_SQL_LENGTH).toBe(1 * 1024 * 1024);
    });

    it('should have reasonable MAX_PARAM_COUNT (1000)', () => {
      expect(MAX_PARAM_COUNT).toBe(1000);
    });
  });
});
