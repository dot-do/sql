/**
 * DoSQL CapnWeb RPC Example Worker
 *
 * A Cloudflare Worker that exposes a DoSQL database via CapnWeb RPC.
 *
 * Features:
 * - CapnWeb RPC over WebSocket for streaming and pipelining
 * - CapnWeb RPC over HTTP POST for simple requests
 * - Full SQL support via Durable Objects
 * - .map() chaining for client-side pipelining
 *
 * @example
 * ```ts
 * // Connect via WebSocket for streaming
 * const rpc = newWebSocketRpcSession<DoSQLRpcApi>('wss://example.com/db/mydb/rpc');
 *
 * // Execute SQL with pipelining
 * const userName = await rpc.exec({ sql: 'SELECT * FROM users WHERE id = ?', params: [1] })
 *   .map(result => result.rows[0])
 *   .map(row => row[1]); // Get name column
 *
 * // Transaction example
 * const txResult = await rpc.transaction({
 *   ops: [
 *     { type: 'exec', sql: 'INSERT INTO orders (user_id, total) VALUES (?, ?)', params: [1, 99.99] },
 *     { type: 'exec', sql: 'UPDATE users SET order_count = order_count + 1 WHERE id = ?', params: [1] },
 *   ],
 * });
 * ```
 */

import { DoSQLDatabase } from './database.js';
import type { Env } from './types.js';

// Re-export the Durable Object class
export { DoSQLDatabase };

// Export types for clients
export type { DoSQLRpcApi, ExecRequest, ExecResult, TransactionOp, TransactionResult } from './types.js';

// =============================================================================
// Worker Fetch Handler
// =============================================================================

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    // API documentation at root
    if (path === '/' && request.method === 'GET') {
      return new Response(
        JSON.stringify({
          service: 'dosql-capnweb-rpc-example',
          version: '0.1.0',
          description: 'DoSQL with CapnWeb RPC in Durable Objects',
          endpoints: {
            'GET /': 'This documentation',
            'GET /health': 'Health check',
            'GET /db/:name/health': 'Database health check',
            'GET /db/:name/status': 'Database status',
            'POST /db/:name/rpc': 'CapnWeb RPC (HTTP batch)',
            'WS /db/:name/rpc': 'CapnWeb RPC (WebSocket, supports pipelining)',
          },
          examples: {
            exec: {
              description: 'Execute SQL',
              method: 'POST',
              url: '/db/mydb/rpc',
              body: {
                method: 'exec',
                params: [{ sql: "SELECT * FROM users WHERE id = ?", params: [1] }],
              },
            },
            transaction: {
              description: 'Execute transaction',
              method: 'POST',
              url: '/db/mydb/rpc',
              body: {
                method: 'transaction',
                params: [{
                  ops: [
                    { type: 'exec', sql: "INSERT INTO logs (msg) VALUES (?)", params: ['hello'] },
                    { type: 'exec', sql: "UPDATE counters SET value = value + 1" },
                  ],
                }],
              },
            },
            pipelining: {
              description: 'Client-side pipelining with .map()',
              code: `
const rpc = newWebSocketRpcSession<DoSQLRpcApi>('wss://example.com/db/mydb/rpc');

// Pipeline: exec -> map -> map
const userName = await rpc.exec({ sql: 'SELECT name FROM users WHERE id = ?', params: [1] })
  .map(result => result.rows[0])
  .map(row => row[0]);
              `.trim(),
            },
          },
        }, null, 2),
        { headers: { 'Content-Type': 'application/json' } }
      );
    }

    // Health check endpoint
    if (path === '/health') {
      return new Response(
        JSON.stringify({ status: 'ok', timestamp: Date.now() }),
        { headers: { 'Content-Type': 'application/json' } }
      );
    }

    // Route to Durable Object: /db/:name/...
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

    // Not found
    return new Response(
      JSON.stringify({
        error: 'Not Found',
        availableEndpoints: ['/', '/health', '/db/:name/*'],
      }),
      {
        status: 404,
        headers: { 'Content-Type': 'application/json' },
      }
    );
  },
};
