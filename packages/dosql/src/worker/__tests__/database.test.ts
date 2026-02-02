/**
 * DoSQL Database Durable Object E2E Tests
 *
 * End-to-end tests for the DoSQLDatabase Durable Object worker entry point.
 * Tests database initialization, fetch handling, query execution, and error handling.
 *
 * Uses @cloudflare/vitest-pool-workers for miniflare integration.
 * NOTE: Tests use unique database names per test to avoid isolated storage issues.
 * See: https://developers.cloudflare.com/workers/testing/vitest-integration/known-issues/#isolated-storage
 */

import { describe, it, expect } from 'vitest';
import { SELF } from 'cloudflare:test';

// Helper functions for cleaner test code
async function execute(dbName: string, sql: string) {
  const response = await SELF.fetch(`http://localhost/db/${dbName}/execute`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql }),
  });
  return {
    status: response.status,
    body: await response.json() as {
      success: boolean;
      error?: string;
      rows?: Record<string, unknown>[];
      stats?: { rowsAffected: number; executionTimeMs: number };
    },
  };
}

async function query(dbName: string, sql: string, params?: Record<string, unknown>) {
  const response = await SELF.fetch(`http://localhost/db/${dbName}/query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql, params }),
  });
  return {
    status: response.status,
    body: await response.json() as {
      success: boolean;
      error?: string;
      rows?: Record<string, unknown>[];
      stats?: { rowsAffected: number; executionTimeMs: number };
    },
  };
}

describe('DoSQLDatabase - E2E Tests', () => {
  // ===========================================================================
  // Database Initialization
  // ===========================================================================
  describe('Database Initialization', () => {
    it('should initialize on first request', async () => {
      const response = await SELF.fetch('http://localhost/db/db-init-1/health');
      expect(response.status).toBe(200);

      const body = await response.json();
      expect(body.status).toBe('ok');
      expect(body.initialized).toBe(true);
    });

    it('should maintain initialization state across requests', async () => {
      // First request initializes
      const response1 = await SELF.fetch('http://localhost/db/db-init-persist-1/health');
      expect(response1.status).toBe(200);
      const body1 = await response1.json();
      expect(body1.initialized).toBe(true);

      // Second request should still be initialized
      const response2 = await SELF.fetch('http://localhost/db/db-init-persist-1/health');
      expect(response2.status).toBe(200);
      const body2 = await response2.json();
      expect(body2.initialized).toBe(true);
    });
  });

  // ===========================================================================
  // Fetch Handler Routing
  // ===========================================================================
  describe('Fetch Handler Routing', () => {
    it('should route to /health endpoint', async () => {
      const response = await SELF.fetch('http://localhost/db/db-route-health-1/health');
      expect(response.status).toBe(200);
      expect(response.headers.get('Content-Type')).toBe('application/json');
    });

    it('should route to /tables endpoint', async () => {
      const response = await SELF.fetch('http://localhost/db/db-route-tables-1/tables');
      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.tables).toBeDefined();
      expect(Array.isArray(body.tables)).toBe(true);
    });

    it('should route to /execute endpoint with POST', async () => {
      const { status, body } = await execute('db-route-execute-1', 'CREATE TABLE exec_test (id INTEGER, PRIMARY KEY (id))');
      expect(status).toBe(200);
      expect(body.success).toBe(true);
    });

    it('should return 404 for unknown paths', async () => {
      const response = await SELF.fetch('http://localhost/db/db-route-unknown-1/unknown');
      expect(response.status).toBe(404);
      const body = await response.json();
      expect(body.error).toBe('Not found');
    });

    it('should return 404 for wrong HTTP methods on /query', async () => {
      const response = await SELF.fetch('http://localhost/db/db-route-method-1/query', {
        method: 'GET',
      });
      expect(response.status).toBe(404);
    });

    it('should return 404 for wrong HTTP methods on /execute', async () => {
      const response = await SELF.fetch('http://localhost/db/db-route-method-2/execute', {
        method: 'GET',
      });
      expect(response.status).toBe(404);
    });
  });

  // ===========================================================================
  // Query Execution Through HTTP Endpoints
  // ===========================================================================
  describe('Query Execution', () => {
    it('should execute CREATE TABLE statement', async () => {
      const { status, body } = await execute('db-exec-create-1', 'CREATE TABLE users (id INTEGER, name TEXT, email TEXT, PRIMARY KEY (id))');
      expect(status).toBe(200);
      expect(body.success).toBe(true);
    });

    it('should execute INSERT statement and return affected rows', async () => {
      await execute('db-exec-insert-1', 'CREATE TABLE insert_test (id INTEGER, name TEXT, PRIMARY KEY (id))');
      const { status, body } = await execute('db-exec-insert-1', "INSERT INTO insert_test (id, name) VALUES (1, 'Alice')");
      expect(status).toBe(200);
      expect(body.success).toBe(true);
      expect(body.stats).toBeDefined();
      expect(body.stats!.rowsAffected).toBe(1);
    });

    it('should execute SELECT statement and return rows', async () => {
      await execute('db-exec-select-1', 'CREATE TABLE select_test (id INTEGER, name TEXT, PRIMARY KEY (id))');
      await execute('db-exec-select-1', "INSERT INTO select_test (id, name) VALUES (1, 'Bob')");
      const { status, body } = await query('db-exec-select-1', 'SELECT * FROM select_test');
      expect(status).toBe(200);
      expect(body.success).toBe(true);
      expect(body.rows).toBeDefined();
      expect(Array.isArray(body.rows)).toBe(true);
    });

    it('should return execution time stats', async () => {
      const { status, body } = await execute('db-exec-stats-1', 'CREATE TABLE stats_test (id INTEGER, PRIMARY KEY (id))');
      expect(status).toBe(200);
      expect(body.stats).toBeDefined();
      expect(body.stats!.executionTimeMs).toBeDefined();
      expect(typeof body.stats!.executionTimeMs).toBe('number');
      expect(body.stats!.executionTimeMs).toBeGreaterThanOrEqual(0);
    });
  });

  // ===========================================================================
  // Error Handling
  // ===========================================================================
  describe('Error Handling', () => {
    it('should return error for invalid SQL syntax', async () => {
      const { status, body } = await execute('db-error-syntax-1', 'THIS IS NOT VALID SQL');
      expect(status).toBe(400);
      expect(body.success).toBe(false);
      expect(body.error).toBeDefined();
    });

    it('should return error for non-existent table in SELECT', async () => {
      const { status, body } = await query('db-error-table-1', 'SELECT * FROM nonexistent_table_xyz');
      expect(status).toBe(400);
      expect(body.success).toBe(false);
      expect(body.error).toContain('Table not found');
    });

    it('should return error for non-existent table in INSERT', async () => {
      const { status, body } = await execute('db-error-insert-1', "INSERT INTO nonexistent_table_xyz (id) VALUES (1)");
      expect(status).toBe(400);
      expect(body.success).toBe(false);
    });

    it('should return error for duplicate table creation', async () => {
      await execute('db-error-dup-1', 'CREATE TABLE dup_test (id INTEGER, PRIMARY KEY (id))');
      const { status, body } = await execute('db-error-dup-1', 'CREATE TABLE dup_test (id INTEGER, PRIMARY KEY (id))');
      expect(status).toBe(400);
      expect(body.success).toBe(false);
      expect(body.error).toContain('already exists');
    });

    it('should return error for malformed JSON body', async () => {
      const response = await SELF.fetch('http://localhost/db/db-error-json-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{ invalid json }',
      });
      expect(response.status).toBe(400);
      const body = await response.json();
      expect(body.success).toBe(false);
      expect(body.error).toContain('Invalid JSON');
    });

    it('should return error for missing sql field', async () => {
      const response = await SELF.fetch('http://localhost/db/db-error-nosql-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: 'SELECT 1' }),
      });
      expect(response.status).toBe(400);
      const body = await response.json();
      expect(body.success).toBe(false);
      expect(body.error).toContain('Missing or invalid "sql" field');
    });
  });

  // ===========================================================================
  // Table Management
  // ===========================================================================
  describe('Table Management', () => {
    it('should list tables after creation', async () => {
      await execute('db-tables-list-1', 'CREATE TABLE listed_table (id INTEGER, PRIMARY KEY (id))');
      const response = await SELF.fetch('http://localhost/db/db-tables-list-1/tables');
      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.tables).toBeDefined();
      expect(Array.isArray(body.tables)).toBe(true);
      expect(body.tables.length).toBeGreaterThanOrEqual(1);
      const foundTable = body.tables.find((t: { name: string }) => t.name === 'listed_table');
      expect(foundTable).toBeDefined();
    });

    it('should return empty list when no tables exist', async () => {
      const response = await SELF.fetch('http://localhost/db/db-tables-empty-1/tables');
      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.tables).toBeDefined();
      expect(Array.isArray(body.tables)).toBe(true);
    });

    it('should include table schema in list', async () => {
      await execute('db-tables-schema-1', 'CREATE TABLE schema_test (id INTEGER, name TEXT, age INTEGER, PRIMARY KEY (id))');
      const response = await SELF.fetch('http://localhost/db/db-tables-schema-1/tables');
      expect(response.status).toBe(200);
      const body = await response.json();
      const table = body.tables.find((t: { name: string }) => t.name === 'schema_test');
      expect(table).toBeDefined();
      expect(table.columns).toBeDefined();
      expect(Array.isArray(table.columns)).toBe(true);
    });
  });

  // ===========================================================================
  // Edge Cases
  // ===========================================================================
  describe('Edge Cases', () => {
    it('should handle empty SELECT result', async () => {
      await execute('db-edge-empty-1', 'CREATE TABLE empty_test (id INTEGER, PRIMARY KEY (id))');
      const { status, body } = await query('db-edge-empty-1', 'SELECT * FROM empty_test');
      expect(status).toBe(200);
      expect(body.success).toBe(true);
      expect(body.rows).toBeDefined();
      expect(body.rows!.length).toBe(0);
    });

    it('should handle special characters in table names', async () => {
      const { status, body } = await execute('db-edge-special-1', 'CREATE TABLE "my-table_123" (id INTEGER, PRIMARY KEY (id))');
      expect(status).toBe(200);
      expect(body.success).toBe(true);
    });

    it('should handle whitespace-only SQL', async () => {
      const { status, body } = await execute('db-edge-whitespace-1', '   \n\t  ');
      expect(status).toBe(400);
      expect(body.success).toBe(false);
    });
  });

  // ===========================================================================
  // Parameters Support
  // ===========================================================================
  describe('Parameters Support', () => {
    it('should accept params in query request', async () => {
      await execute('db-params-query-1', 'CREATE TABLE params_test (id INTEGER, name TEXT, PRIMARY KEY (id))');
      await execute('db-params-query-1', "INSERT INTO params_test (id, name) VALUES (1, 'Alice')");
      const { status, body } = await query('db-params-query-1', 'SELECT * FROM params_test WHERE id = :id', { id: 1 });
      expect(status).toBe(200);
      expect(body.success).toBe(true);
    });
  });
});
