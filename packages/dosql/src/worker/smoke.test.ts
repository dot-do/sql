/**
 * DoSQL Worker Smoke Tests
 *
 * Basic functionality tests for the DoSQL Durable Object worker.
 * Uses @cloudflare/vitest-pool-workers for miniflare integration.
 *
 * NOTE: Due to known issues with isolated storage in @cloudflare/vitest-pool-workers,
 * tests with multiple DO storage operations fail with "Failed to pop isolated storage".
 * See: https://developers.cloudflare.com/workers/testing/vitest-integration/known-issues/#isolated-storage
 *
 * For full integration testing, use `wrangler dev` with manual curl/httpie testing.
 */

import { describe, it, expect } from 'vitest';
import { SELF } from 'cloudflare:test';

describe('DoSQL Worker - Smoke Tests', () => {
  // Worker endpoint tests - these don't use DO storage directly
  describe('Worker Entry Point', () => {
    it('should respond to health check', async () => {
      const response = await SELF.fetch('http://localhost/health');

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body).toMatchObject({
        status: 'ok',
        service: 'dosql-test',
      });
    });

    it('should return API documentation', async () => {
      const response = await SELF.fetch('http://localhost/api');

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.endpoints).toBeDefined();
      expect(body.example).toBeDefined();
    });

    it('should return 404 for unknown routes', async () => {
      const response = await SELF.fetch('http://localhost/unknown');

      expect(response.status).toBe(404);
    });
  });

  // DO health check - simple single operation
  describe('DoSQLDatabase Basic Operations', () => {
    it('should respond to health check via worker routing', async () => {
      const response = await SELF.fetch('http://localhost/db/smoke-health/health');
      expect(response.status).toBe(200);

      const body = await response.json();
      expect(body).toMatchObject({
        status: 'ok',
        initialized: true,
      });
    });

    it('should create a table', async () => {
      const response = await SELF.fetch('http://localhost/db/smoke-create/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'CREATE TABLE smoke_users (id INTEGER, name TEXT, PRIMARY KEY (id))',
        }),
      });

      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body.success).toBe(true);
    });

    it('should return error for invalid SQL', async () => {
      const response = await SELF.fetch('http://localhost/db/smoke-error/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'INVALID SQL STATEMENT',
        }),
      });

      expect(response.status).toBe(400);
      const body = await response.json();
      expect(body.success).toBe(false);
      expect(body.error).toBeDefined();
    });

    it('should return error for non-existent table', async () => {
      const response = await SELF.fetch('http://localhost/db/smoke-notable/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'SELECT * FROM nonexistent_table',
        }),
      });

      expect(response.status).toBe(400);
      const body = await response.json();
      expect(body.success).toBe(false);
      expect(body.error).toContain('Table not found');
    });
  });

  // Skip this test due to known vitest-pool-workers isolated storage issue
  // Full CRUD testing should be done via wrangler dev or actual deployment
  describe.skip('Full CRUD Workflow (requires wrangler dev)', () => {
    it('should perform complete CRUD operations', async () => {
      // This test is skipped due to vitest-pool-workers limitation
      // Run manually with: wrangler dev, then use curl/httpie
    });
  });
});
