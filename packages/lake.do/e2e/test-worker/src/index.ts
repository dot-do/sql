/**
 * DoLake E2E Test Worker
 *
 * Minimal Cloudflare Worker for end-to-end testing of the lake.do client.
 * Proxies requests to the DoLake Durable Object and provides test-specific endpoints.
 *
 * Endpoints:
 * - GET /health - Worker health check
 * - POST /cdc - Write CDC events (proxied to DoLake DO)
 * - POST /query - Execute SQL query (proxied to DoLake DO)
 * - /lake/:name/* - Lakehouse-specific routes (proxied to DoLake DO)
 * - /ws/:name - WebSocket connection (proxied to DoLake DO)
 *
 * @packageDocumentation
 */

import { DoLake, type DoLakeEnv } from '@dotdo/dolake';

// Re-export DoLake for wrangler to detect the Durable Object class
export { DoLake };

/**
 * Environment bindings for the E2E test worker
 */
export interface Env extends DoLakeEnv {
  /** DoLake Durable Object namespace */
  DOLAKE: DurableObjectNamespace;
  /** R2 bucket for lakehouse data */
  LAKEHOUSE_BUCKET: R2Bucket;
  /** Environment identifier */
  ENVIRONMENT: string;
  /** Log level */
  LOG_LEVEL: string;
}

/**
 * Health check response
 */
interface HealthResponse {
  status: 'ok' | 'degraded' | 'error';
  service: string;
  version: string;
  environment: string;
  timestamp: number;
}

/**
 * Error response
 */
interface ErrorResponse {
  error: string;
  code: string;
  details?: unknown;
}

/**
 * Extract lakehouse name from URL path
 */
function extractLakehouseName(pathname: string): string | null {
  // Match /lake/:name/* pattern
  const lakeMatch = pathname.match(/^\/lake\/([^/]+)/);
  if (lakeMatch) {
    return lakeMatch[1];
  }

  // Match /ws/:name pattern
  const wsMatch = pathname.match(/^\/ws\/([^/]+)/);
  if (wsMatch) {
    return wsMatch[1];
  }

  return null;
}

/**
 * Get or create a DoLake Durable Object stub
 */
function getLakehouseStub(env: Env, lakehouseName: string): DurableObjectStub {
  const id = env.DOLAKE.idFromName(lakehouseName);
  return env.DOLAKE.get(id);
}

/**
 * Create a JSON response
 */
function jsonResponse<T>(data: T, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    },
  });
}

/**
 * Create an error response
 */
function errorResponse(message: string, code: string, status = 500, details?: unknown): Response {
  const body: ErrorResponse = { error: message, code, details };
  return jsonResponse(body, status);
}

/**
 * Handle CORS preflight requests
 */
function handleCors(): Response {
  return new Response(null, {
    status: 204,
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
      'Access-Control-Max-Age': '86400',
    },
  });
}

/**
 * Worker fetch handler
 */
async function handleFetch(request: Request, env: Env): Promise<Response> {
  const url = new URL(request.url);
  const { pathname } = url;
  const method = request.method;

  // Handle CORS preflight
  if (method === 'OPTIONS') {
    return handleCors();
  }

  try {
    // Health check endpoint
    if (pathname === '/health' && method === 'GET') {
      const response: HealthResponse = {
        status: 'ok',
        service: 'dolake',
        version: '0.1.0-e2e',
        environment: env.ENVIRONMENT || 'e2e-test',
        timestamp: Date.now(),
      };
      return jsonResponse(response);
    }

    // Root path - basic info
    if (pathname === '/' && method === 'GET') {
      return jsonResponse({
        service: 'dolake-e2e-test',
        version: '0.1.0-e2e',
        endpoints: {
          health: 'GET /health',
          lake: '/lake/:name/*',
          websocket: '/ws/:name',
        },
      });
    }

    // Lakehouse routes - proxy to Durable Object
    const lakehouseName = extractLakehouseName(pathname);
    if (lakehouseName) {
      const stub = getLakehouseStub(env, lakehouseName);

      // For WebSocket upgrade requests
      if (request.headers.get('Upgrade') === 'websocket') {
        return stub.fetch(request);
      }

      // Proxy the request to the Durable Object
      // Rewrite the URL to remove the /lake/:name prefix for DO routing
      const doUrl = new URL(request.url);
      doUrl.pathname = pathname.replace(/^\/lake\/[^/]+/, '') || '/';

      const doRequest = new Request(doUrl.toString(), {
        method: request.method,
        headers: request.headers,
        body: request.body,
      });

      return stub.fetch(doRequest);
    }

    // CDC write endpoint (legacy - requires lakehouse name in body)
    if (pathname === '/cdc' && method === 'POST') {
      try {
        const body = await request.json() as { lakehouse?: string; events?: unknown[] };
        const lakehouse = body.lakehouse || 'default';
        const stub = getLakehouseStub(env, lakehouse);

        // Forward to the DO's CDC endpoint
        const cdcRequest = new Request(`${url.origin}/cdc/write`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ events: body.events }),
        });

        return stub.fetch(cdcRequest);
      } catch (error) {
        return errorResponse(
          'Failed to process CDC request',
          'CDC_ERROR',
          400,
          error instanceof Error ? error.message : String(error)
        );
      }
    }

    // Query endpoint (legacy - requires lakehouse name in body)
    if (pathname === '/query' && method === 'POST') {
      try {
        const body = await request.json() as { lakehouse?: string; sql?: string; asOf?: string };
        const lakehouse = body.lakehouse || 'default';
        const stub = getLakehouseStub(env, lakehouse);

        // Forward to the DO's query endpoint
        const queryRequest = new Request(`${url.origin}/query`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ sql: body.sql, asOf: body.asOf }),
        });

        return stub.fetch(queryRequest);
      } catch (error) {
        return errorResponse(
          'Failed to process query request',
          'QUERY_ERROR',
          400,
          error instanceof Error ? error.message : String(error)
        );
      }
    }

    // 404 for unknown routes
    return errorResponse('Not found', 'NOT_FOUND', 404, { pathname });

  } catch (error) {
    console.error('[DoLake E2E Worker] Error:', error);
    return errorResponse(
      'Internal server error',
      'INTERNAL_ERROR',
      500,
      error instanceof Error ? error.message : String(error)
    );
  }
}

/**
 * Default export for Cloudflare Workers
 */
export default {
  fetch: handleFetch,
} satisfies ExportedHandler<Env>;
