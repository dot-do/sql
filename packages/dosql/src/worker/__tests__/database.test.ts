/**
 * DoSQL Database Durable Object E2E Tests
 *
 * End-to-end tests for the DoSQLDatabase Durable Object worker entry point.
 * Tests database initialization, fetch handling, query execution, and error handling.
 *
 * Uses @cloudflare/vitest-pool-workers for miniflare integration.
 * NOTE: Tests use unique database names per test to avoid isolated storage issues.
 */

import { describe, it, expect } from 'vitest';
import { SELF } from 'cloudflare:test';

describe('DoSQLDatabase - E2E Tests', () => {
  // ===========================================================================
  // Database Initialization
  // ===========================================================================
  describe('Database Initialization', () => {
    it('should initialize on first request', async () => {
      const response = await SELF.fetch('http://localhost/db/init-test-1/health');
      expect(response.status).toBe(200);

      const body = await response.json();
      expect(body.status).toBe('ok');
      expect(body.initialized).toBe(true);
    });

    it('should maintain initialization state across requests', async () => {
      // First request initializes
      const response1 = await SELF.fetch('http://localhost/db/init-persist-1/health');
      expect(response1.status).toBe(200);
      const body1 = await response1.json();
      expect(body1.initialized).toBe(true);

      // Second request should still be initialized
      const response2 = await SELF.fetch('http://localhost/db/init-persist-1/health');
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
      const response = await SELF.fetch('http://localhost/db/route-health-1/health');
      expect(response.status).toBe(200);
      expect(response.headers.get('Content-Type')).toBe('application/json');
    });

    it('should route to /tables endpoint', async () => {
      const response = await SELF.fetch('http://localhost/db/route-tables-1/tables');
      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.tables).toBeDefined();
      expect(Array.isArray(body.tables)).toBe(true);
    });

    it('should route to /query endpoint with POST', async () => {
      // First create a table
      await SELF.fetch('http://localhost/db/route-query-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'CREATE TABLE route_test (id INTEGER, PRIMARY KEY (id))',
        }),
      });

      // Then query it
      const response = await SELF.fetch('http://localhost/db/route-query-1/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: 'SELECT * FROM route_test' }),
      });
      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.success).toBe(true);
    });

    it('should route to /execute endpoint with POST', async () => {
      const response = await SELF.fetch('http://localhost/db/route-execute-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'CREATE TABLE exec_test (id INTEGER, PRIMARY KEY (id))',
        }),
      });
      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.success).toBe(true);
    });

    it('should return 404 for unknown paths', async () => {
      const response = await SELF.fetch('http://localhost/db/route-unknown-1/unknown');
      expect(response.status).toBe(404);
      const body = await response.json();
      expect(body.error).toBe('Not found');
    });

    it('should return 404 for wrong HTTP methods on /query', async () => {
      const response = await SELF.fetch('http://localhost/db/route-method-1/query', {
        method: 'GET',
      });
      expect(response.status).toBe(404);
    });

    it('should return 404 for wrong HTTP methods on /execute', async () => {
      const response = await SELF.fetch('http://localhost/db/route-method-2/execute', {
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
      const response = await SELF.fetch('http://localhost/db/exec-create-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'CREATE TABLE users (id INTEGER, name TEXT, email TEXT, PRIMARY KEY (id))',
        }),
      });

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.success).toBe(true);
    });

    it('should execute INSERT statement and return affected rows', async () => {
      // Create table first
      await SELF.fetch('http://localhost/db/exec-insert-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'CREATE TABLE insert_test (id INTEGER, name TEXT, PRIMARY KEY (id))',
        }),
      });

      // Then insert
      const response = await SELF.fetch('http://localhost/db/exec-insert-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: "INSERT INTO insert_test (id, name) VALUES (1, 'Alice')",
        }),
      });

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.success).toBe(true);
      expect(body.stats).toBeDefined();
      expect(body.stats.rowsAffected).toBe(1);
    });

    it('should execute SELECT statement and return rows', async () => {
      // Create and populate table
      await SELF.fetch('http://localhost/db/exec-select-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'CREATE TABLE select_test (id INTEGER, name TEXT, PRIMARY KEY (id))',
        }),
      });

      await SELF.fetch('http://localhost/db/exec-select-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: "INSERT INTO select_test (id, name) VALUES (1, 'Bob')",
        }),
      });

      // Query
      const response = await SELF.fetch('http://localhost/db/exec-select-1/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'SELECT * FROM select_test',
        }),
      });

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.success).toBe(true);
      expect(body.rows).toBeDefined();
      expect(Array.isArray(body.rows)).toBe(true);
    });

    it('should return execution time stats', async () => {
      const response = await SELF.fetch('http://localhost/db/exec-stats-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'CREATE TABLE stats_test (id INTEGER, PRIMARY KEY (id))',
        }),
      });

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.stats).toBeDefined();
      expect(body.stats.executionTimeMs).toBeDefined();
      expect(typeof body.stats.executionTimeMs).toBe('number');
      expect(body.stats.executionTimeMs).toBeGreaterThanOrEqual(0);
    });
  });

  // ===========================================================================
  // Error Handling
  // ===========================================================================
  describe('Error Handling', () => {
    it('should return error for invalid SQL syntax', async () => {
      const response = await SELF.fetch('http://localhost/db/error-syntax-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'THIS IS NOT VALID SQL',
        }),
      });

      expect(response.status).toBe(400);
      const body = await response.json();
      expect(body.success).toBe(false);
      expect(body.error).toBeDefined();
    });

    it('should return error for non-existent table in SELECT', async () => {
      const response = await SELF.fetch('http://localhost/db/error-table-1/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'SELECT * FROM nonexistent_table_xyz',
        }),
      });

      expect(response.status).toBe(400);
      const body = await response.json();
      expect(body.success).toBe(false);
      expect(body.error).toContain('Table not found');
    });

    it('should return error for non-existent table in INSERT', async () => {
      const response = await SELF.fetch('http://localhost/db/error-insert-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: "INSERT INTO nonexistent_table_xyz (id) VALUES (1)",
        }),
      });

      expect(response.status).toBe(400);
      const body = await response.json();
      expect(body.success).toBe(false);
    });

    it('should return error for duplicate table creation', async () => {
      // Create table first time
      await SELF.fetch('http://localhost/db/error-dup-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'CREATE TABLE dup_test (id INTEGER, PRIMARY KEY (id))',
        }),
      });

      // Try to create same table again
      const response = await SELF.fetch('http://localhost/db/error-dup-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'CREATE TABLE dup_test (id INTEGER, PRIMARY KEY (id))',
        }),
      });

      expect(response.status).toBe(400);
      const body = await response.json();
      expect(body.success).toBe(false);
      expect(body.error).toContain('already exists');
    });

    it('should return error for missing request body', async () => {
      const response = await SELF.fetch('http://localhost/db/error-body-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '',
      });

      expect(response.status).toBe(400);
      const body = await response.json();
      expect(body.success).toBe(false);
    });

    it('should return error for malformed JSON body', async () => {
      const response = await SELF.fetch('http://localhost/db/error-json-1/execute', {
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
      const response = await SELF.fetch('http://localhost/db/error-nosql-1/execute', {
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
      // Create a table
      await SELF.fetch('http://localhost/db/tables-list-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'CREATE TABLE listed_table (id INTEGER, PRIMARY KEY (id))',
        }),
      });

      // List tables
      const response = await SELF.fetch('http://localhost/db/tables-list-1/tables');
      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.tables).toBeDefined();
      expect(Array.isArray(body.tables)).toBe(true);
      expect(body.tables.length).toBeGreaterThanOrEqual(1);

      // Find our table
      const foundTable = body.tables.find((t: { name: string }) => t.name === 'listed_table');
      expect(foundTable).toBeDefined();
    });

    it('should return empty list when no tables exist', async () => {
      const response = await SELF.fetch('http://localhost/db/tables-empty-1/tables');
      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.tables).toBeDefined();
      expect(Array.isArray(body.tables)).toBe(true);
    });

    it('should include table schema in list', async () => {
      // Create table with multiple columns
      await SELF.fetch('http://localhost/db/tables-schema-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'CREATE TABLE schema_test (id INTEGER, name TEXT, age INTEGER, PRIMARY KEY (id))',
        }),
      });

      // List tables
      const response = await SELF.fetch('http://localhost/db/tables-schema-1/tables');
      expect(response.status).toBe(200);
      const body = await response.json();
      const table = body.tables.find((t: { name: string }) => t.name === 'schema_test');
      expect(table).toBeDefined();
      expect(table.columns).toBeDefined();
      expect(Array.isArray(table.columns)).toBe(true);
    });
  });

  // ===========================================================================
  // Rate Limiting Integration
  // ===========================================================================
  describe('Rate Limiting Integration', () => {
    it('should allow normal request rate', async () => {
      // Make several requests within rate limit
      const requests = Array(5).fill(null).map((_, i) =>
        SELF.fetch(`http://localhost/db/rate-ok-${i}/health`)
      );

      const responses = await Promise.all(requests);
      for (const response of responses) {
        expect(response.status).toBe(200);
      }
    });

    // Note: Testing rate limiting exhaustion requires careful handling
    // due to shared state across tests. Skipping exhaustion test.
  });

  // ===========================================================================
  // Edge Cases
  // ===========================================================================
  describe('Edge Cases', () => {
    it('should handle empty SELECT result', async () => {
      // Create empty table
      await SELF.fetch('http://localhost/db/edge-empty-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'CREATE TABLE empty_test (id INTEGER, PRIMARY KEY (id))',
        }),
      });

      // Query empty table
      const response = await SELF.fetch('http://localhost/db/edge-empty-1/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'SELECT * FROM empty_test',
        }),
      });

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.success).toBe(true);
      expect(body.rows).toBeDefined();
      expect(body.rows.length).toBe(0);
    });

    it('should handle special characters in table names', async () => {
      const response = await SELF.fetch('http://localhost/db/edge-special-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'CREATE TABLE "my-table_123" (id INTEGER, PRIMARY KEY (id))',
        }),
      });

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.success).toBe(true);
    });

    it('should handle concurrent requests to same database', async () => {
      // Create table first
      await SELF.fetch('http://localhost/db/edge-concurrent-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'CREATE TABLE concurrent_test (id INTEGER, name TEXT, PRIMARY KEY (id))',
        }),
      });

      // Send concurrent insert requests
      const requests = Array(3).fill(null).map((_, i) =>
        SELF.fetch('http://localhost/db/edge-concurrent-1/execute', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            sql: `INSERT INTO concurrent_test (id, name) VALUES (${i + 1}, 'User${i + 1}')`,
          }),
        })
      );

      const responses = await Promise.all(requests);
      for (const response of responses) {
        expect(response.status).toBe(200);
      }
    });

    it('should handle whitespace-only SQL', async () => {
      const response = await SELF.fetch('http://localhost/db/edge-whitespace-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: '   \n\t  ',
        }),
      });

      expect(response.status).toBe(400);
      const body = await response.json();
      expect(body.success).toBe(false);
    });
  });

  // ===========================================================================
  // Parameters Support
  // ===========================================================================
  describe('Parameters Support', () => {
    it('should accept params in query request', async () => {
      // Create table
      await SELF.fetch('http://localhost/db/params-query-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'CREATE TABLE params_test (id INTEGER, name TEXT, PRIMARY KEY (id))',
        }),
      });

      // Insert data
      await SELF.fetch('http://localhost/db/params-query-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: "INSERT INTO params_test (id, name) VALUES (1, 'Alice')",
        }),
      });

      // Query with params
      const response = await SELF.fetch('http://localhost/db/params-query-1/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'SELECT * FROM params_test WHERE id = :id',
          params: { id: 1 },
        }),
      });

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.success).toBe(true);
    });

    it('should accept params in execute request', async () => {
      // Create table
      await SELF.fetch('http://localhost/db/params-exec-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'CREATE TABLE params_exec_test (id INTEGER, name TEXT, PRIMARY KEY (id))',
        }),
      });

      // Insert with params
      const response = await SELF.fetch('http://localhost/db/params-exec-1/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'INSERT INTO params_exec_test (id, name) VALUES (:id, :name)',
          params: { id: 1, name: 'Bob' },
        }),
      });

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.success).toBe(true);
    });
  });
});
