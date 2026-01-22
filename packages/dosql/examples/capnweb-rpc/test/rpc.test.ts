/**
 * DoSQL CapnWeb RPC Integration Tests
 *
 * Tests the DoSQL database in a real Durable Object environment
 * using @cloudflare/vitest-pool-workers.
 *
 * NO MOCKS - All tests run against real Workers and Durable Objects.
 *
 * Note: These tests use direct HTTP calls to test the worker and DO routing.
 * Full CapnWeb RPC testing requires a WebSocket client which is complex
 * to set up in the test environment.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { SELF, env } from 'cloudflare:test';

declare module 'cloudflare:test' {
  interface ProvidedEnv {
    DOSQL_DB: DurableObjectNamespace;
  }
}

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Create a unique database name for each test
 */
function uniqueDbName(): string {
  return `test_db_${Date.now()}_${Math.random().toString(36).slice(2)}`;
}

/**
 * Make a request to the worker
 */
async function fetchWorker(
  path: string,
  options?: RequestInit
): Promise<Response> {
  return SELF.fetch(`https://example.com${path}`, options);
}

/**
 * Make a request to a specific database
 */
async function fetchDb(
  dbName: string,
  path: string,
  options?: RequestInit
): Promise<Response> {
  return fetchWorker(`/db/${dbName}${path}`, options);
}

// =============================================================================
// Worker Tests
// =============================================================================

describe('DoSQL CapnWeb RPC Worker', () => {
  describe('Health Endpoints', () => {
    it('should return health status at root /health', async () => {
      const response = await fetchWorker('/health');

      expect(response.status).toBe(200);
      const data = await response.json() as { status: string; timestamp: number };
      expect(data.status).toBe('ok');
      expect(data.timestamp).toBeGreaterThan(0);
    });

    it('should return API documentation at root /', async () => {
      const response = await fetchWorker('/');

      expect(response.status).toBe(200);
      const data = await response.json() as { service: string; endpoints: object };
      expect(data.service).toBe('dosql-capnweb-rpc-example');
      expect(data.endpoints).toBeDefined();
    });

    it('should return 404 for unknown paths', async () => {
      const response = await fetchWorker('/unknown/path');

      expect(response.status).toBe(404);
    });
  });

  describe('Database Routing', () => {
    it('should route /db/:name/health to Durable Object', async () => {
      const dbName = uniqueDbName();
      const response = await fetchDb(dbName, '/health');

      expect(response.status).toBe(200);
      const data = await response.json() as { status: string };
      expect(data.status).toBe('ok');
    });

    it('should route /db/:name/status to Durable Object', async () => {
      const dbName = uniqueDbName();
      const response = await fetchDb(dbName, '/status');

      expect(response.status).toBe(200);
      const data = await response.json() as { initialized: boolean; version: string };
      expect(data.initialized).toBe(true);
      expect(data.version).toBe('0.1.0');
    });

    it('should create separate instances for different database names', async () => {
      const db1 = uniqueDbName();
      const db2 = uniqueDbName();

      // Get status from each - they should be independent
      const status1Response = await fetchDb(db1, '/status');
      const status2Response = await fetchDb(db2, '/status');

      const status1 = await status1Response.json() as { initialized: boolean };
      const status2 = await status2Response.json() as { initialized: boolean };

      expect(status1.initialized).toBe(true);
      expect(status2.initialized).toBe(true);
    });
  });
});

// =============================================================================
// Database Status Tests
// =============================================================================

describe('DoSQL Database Status', () => {
  it('should report database status', async () => {
    const dbName = uniqueDbName();
    const response = await fetchDb(dbName, '/status');

    expect(response.status).toBe(200);
    const status = await response.json() as {
      initialized: boolean;
      tableCount: number;
      totalRows: number;
      version: string;
    };

    expect(status.initialized).toBe(true);
    expect(status.tableCount).toBeGreaterThanOrEqual(0);
    expect(status.totalRows).toBeGreaterThanOrEqual(0);
    expect(status.version).toBe('0.1.0');
  });
});

// =============================================================================
// Concurrent Access Tests
// =============================================================================

describe('DoSQL Concurrent Access', () => {
  it('should handle concurrent requests to different databases', async () => {
    const dbs = Array.from({ length: 3 }, () => uniqueDbName());

    // Make concurrent health check requests
    const responses = await Promise.all(
      dbs.map((dbName) => fetchDb(dbName, '/health'))
    );

    // All should succeed
    for (const response of responses) {
      expect(response.status).toBe(200);
      const data = await response.json() as { status: string };
      expect(data.status).toBe('ok');
    }
  });

  it('should handle concurrent requests to same database', async () => {
    const dbName = uniqueDbName();

    // Make concurrent status requests
    const responses = await Promise.all(
      Array.from({ length: 3 }, () => fetchDb(dbName, '/status'))
    );

    // All should succeed with same version
    for (const response of responses) {
      expect(response.status).toBe(200);
      const data = await response.json() as { version: string };
      expect(data.version).toBe('0.1.0');
    }
  });
});

// =============================================================================
// Error Handling Tests
// =============================================================================

describe('DoSQL Error Handling', () => {
  it('should return 404 for invalid paths', async () => {
    const response = await fetchWorker('/invalid');
    expect(response.status).toBe(404);
  });
});

// =============================================================================
// API Documentation Tests
// =============================================================================

describe('DoSQL API Documentation', () => {
  it('should include all documented endpoints', async () => {
    const response = await fetchWorker('/');
    const data = await response.json() as { endpoints: Record<string, string> };

    expect(data.endpoints).toHaveProperty('GET /');
    expect(data.endpoints).toHaveProperty('GET /health');
    expect(data.endpoints).toHaveProperty('GET /db/:name/health');
    expect(data.endpoints).toHaveProperty('GET /db/:name/status');
    expect(data.endpoints).toHaveProperty('POST /db/:name/rpc');
    expect(data.endpoints).toHaveProperty('WS /db/:name/rpc');
  });

  it('should include usage examples', async () => {
    const response = await fetchWorker('/');
    const data = await response.json() as { examples: Record<string, unknown> };

    expect(data.examples).toHaveProperty('exec');
    expect(data.examples).toHaveProperty('transaction');
    expect(data.examples).toHaveProperty('pipelining');
  });
});

// =============================================================================
// Performance Tests
// =============================================================================

describe('DoSQL Performance', () => {
  it('should handle sequential requests efficiently', async () => {
    const dbName = uniqueDbName();
    const count = 5;

    const start = Date.now();
    for (let i = 0; i < count; i++) {
      const response = await fetchDb(dbName, '/health');
      expect(response.status).toBe(200);
    }
    const duration = Date.now() - start;

    console.log(`${count} sequential requests in ${duration}ms`);
    expect(duration).toBeLessThan(5000);
  });

  it('should handle parallel requests efficiently', async () => {
    const dbName = uniqueDbName();
    const count = 5;

    const start = Date.now();
    const responses = await Promise.all(
      Array.from({ length: count }, () => fetchDb(dbName, '/health'))
    );
    const duration = Date.now() - start;

    for (const response of responses) {
      expect(response.status).toBe(200);
    }

    console.log(`${count} parallel requests in ${duration}ms`);
    expect(duration).toBeLessThan(3000);
  });
});
