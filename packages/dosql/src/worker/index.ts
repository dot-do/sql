/**
 * DoSQL Test Worker
 *
 * A minimal Cloudflare Worker that exposes the DoSQL Durable Object.
 * Used for testing bundle size and basic functionality.
 */

import { DoSQLDatabase, type Env } from './database.js';

// Re-export the Durable Object class
export { DoSQLDatabase };

// Export TestBranchDO for branch testing
export { TestBranchDO } from '../branch/branch.test.js';

// Export BenchmarkTestDO for benchmark adapter testing
export { BenchmarkTestDO } from '../benchmarks/adapters/__tests__/do-sqlite.test.js';

// Export PerformanceBenchmarkDO for performance benchmark testing
export { PerformanceBenchmarkDO } from '../benchmarks/__tests__/performance.test.js';

// Export PerformanceRegressionDO for TDD RED phase performance regression tests
export { PerformanceRegressionDO } from '../__tests__/performance.test.js';

// =============================================================================
// Worker Fetch Handler
// =============================================================================

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    // Health check endpoint
    if (path === '/' || path === '/health') {
      return new Response(
        JSON.stringify({
          status: 'ok',
          service: 'dosql-test',
          version: '0.1.0',
        }),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    }

    // Route to Durable Object
    // Pattern: /db/:name/... -> DoSQLDatabase
    const dbMatch = path.match(/^\/db\/([^/]+)(\/.*)?$/);
    if (dbMatch) {
      const dbName = dbMatch[1];
      const subPath = dbMatch[2] || '/';

      // Get or create DO instance by name
      const id = env.DOSQL_DB.idFromName(dbName);
      const stub = env.DOSQL_DB.get(id);

      // Forward request to DO
      const doUrl = new URL(request.url);
      doUrl.pathname = subPath;

      return stub.fetch(
        new Request(doUrl.toString(), {
          method: request.method,
          headers: request.headers,
          body: request.body,
        })
      );
    }

    // API documentation
    if (path === '/api') {
      return new Response(
        JSON.stringify({
          endpoints: {
            'GET /': 'Health check',
            'GET /health': 'Health check',
            'GET /db/:name/health': 'Database health check',
            'GET /db/:name/tables': 'List tables',
            'POST /db/:name/query': 'Execute SELECT query',
            'POST /db/:name/execute': 'Execute INSERT/UPDATE/DELETE',
          },
          example: {
            createTable: {
              method: 'POST',
              url: '/db/mydb/execute',
              body: {
                sql: "CREATE TABLE users (id INTEGER, name TEXT, email TEXT, PRIMARY KEY (id))",
              },
            },
            insert: {
              method: 'POST',
              url: '/db/mydb/execute',
              body: {
                sql: "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')",
              },
            },
            select: {
              method: 'POST',
              url: '/db/mydb/query',
              body: {
                sql: 'SELECT * FROM users WHERE name = \'Alice\'',
              },
            },
          },
        }),
        {
          headers: { 'Content-Type': 'application/json' },
        }
      );
    }

    // Not found
    return new Response(
      JSON.stringify({
        error: 'Not found',
        availableEndpoints: ['/', '/health', '/api', '/db/:name/*'],
      }),
      {
        status: 404,
        headers: { 'Content-Type': 'application/json' },
      }
    );
  },
};
