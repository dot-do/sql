/**
 * DoSQL Router
 *
 * Complete Hono app combining auth, RPC, and REST endpoints for DoSQL.
 * Provides a batteries-included setup for exposing DoSQL Durable Objects.
 *
 * @packageDocumentation
 */

import type { AuthConfig } from './auth.js'
import type { RPCConfig, RPCRequest, RPCResponse } from './rpc.js'

// =============================================================================
// Types (declared inline to avoid requiring hono install for types)
// =============================================================================

interface HonoContext {
  req: {
    raw: Request
    method: string
    path: string
    header(name: string): string | undefined
    json<T>(): Promise<T>
    query(name: string): string | undefined
  }
  env: Record<string, unknown>
  json(data: unknown, status?: number): Response
  text(data: string, status?: number): Response
  set(key: string, value: unknown): void
  get(key: string): unknown
}

type MiddlewareHandler = (c: HonoContext, next: () => Promise<void>) => Promise<void | Response>

interface HonoApp {
  post(path: string, ...handlers: MiddlewareHandler[]): HonoApp
  get(path: string, ...handlers: MiddlewareHandler[]): HonoApp
  all(path: string, ...handlers: MiddlewareHandler[]): HonoApp
  use(...handlers: MiddlewareHandler[]): HonoApp
  use(path: string, ...handlers: MiddlewareHandler[]): HonoApp
  route(path: string, app: HonoApp): HonoApp
  fetch(request: Request, env?: unknown, ctx?: unknown): Promise<Response>
}

// =============================================================================
// Router Configuration
// =============================================================================

/**
 * Configuration for the complete DoSQL router
 */
export interface DoSQLRouterConfig {
  /** Auth configuration (oauth.do integration) */
  auth?: AuthConfig
  /** RPC configuration (rpc.do integration) */
  rpc?: RPCConfig
  /** Enable REST API endpoints (POST /query, /exec, /transaction) */
  rest?: boolean
  /** Enable WebSocket upgrade for streaming RPC */
  websocket?: boolean
  /** Enable health check endpoint (GET /health) */
  health?: boolean
}

// =============================================================================
// REST Endpoint Types
// =============================================================================

interface RestQueryRequest {
  sql: string
  params?: unknown[]
  db?: string
}

interface RestExecRequest {
  sql: string
  params?: unknown[]
  db?: string
}

interface RestTransactionRequest {
  ops: Array<{
    type: 'exec' | 'prepare' | 'run'
    sql?: string
    params?: unknown[]
    stmtId?: string
  }>
  db?: string
}

// =============================================================================
// Helper: Get DO Stub
// =============================================================================

function getDOStub(
  c: HonoContext,
  config: RPCConfig,
  dbName?: string
): DurableObjectStub | null {
  const namespace = c.env[config.doBinding] as DurableObjectNamespace | undefined
  if (!namespace) return null

  let doId: DurableObjectId

  if (config.resolveId) {
    const resolved = config.resolveId(c)
    if (typeof resolved === 'string') {
      doId = namespace.idFromName(resolved)
    } else {
      doId = resolved
    }
  } else {
    const name = dbName ?? c.req.query('db') ?? 'default'
    doId = namespace.idFromName(name)
  }

  return namespace.get(doId)
}

// =============================================================================
// Helper: Forward RPC to DO
// =============================================================================

async function forwardToDO(
  stub: DurableObjectStub,
  method: string,
  params: unknown[]
): Promise<{ result?: unknown; error?: { code: number; message: string; data?: unknown } }> {
  try {
    const response = await stub.fetch(
      new Request('https://do-internal/rpc', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ method, params }),
      })
    )

    if (!response.ok) {
      const text = await response.text()
      return { error: { code: -32603, message: `DO error: ${response.status}`, data: text } }
    }

    const result = await response.json()
    return { result }
  } catch (error) {
    return {
      error: {
        code: -32603,
        message: error instanceof Error ? error.message : 'Internal error',
      },
    }
  }
}

// =============================================================================
// Router Factory
// =============================================================================

/**
 * Create a complete DoSQL Hono router with auth, RPC, and REST
 *
 * Provides a fully-configured Hono app with:
 * - GET /health - Health check endpoint
 * - POST /rpc - rpc.do JSON-RPC 2.0 protocol handler
 * - GET /rpc - WebSocket upgrade for streaming RPC
 * - GET /rpc/methods - List available RPC methods
 * - POST /query - REST SQL query endpoint
 * - POST /exec - REST SQL exec endpoint
 * - POST /transaction - REST transaction endpoint
 *
 * All endpoints (except /health) are protected by auth middleware.
 *
 * @param config - Router configuration
 * @returns Hono app instance
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono'
 * import { createDoSQLRouter } from 'dosql/middleware'
 *
 * const app = new Hono()
 *
 * app.route('/db', createDoSQLRouter({
 *   auth: { publicPaths: ['/health'] },
 *   rpc: { doBinding: 'SQL_DO' },
 *   rest: true,
 *   websocket: true,
 *   health: true,
 * }))
 *
 * export default app
 * ```
 */
export function createDoSQLRouter(config: DoSQLRouterConfig): HonoApp {
  const {
    auth: authConfig,
    rpc: rpcConfig = { doBinding: 'SQL_DO' },
    rest = true,
    websocket = true,
    health = true,
  } = config

  // We dynamically import Hono and the auth/rpc modules at runtime
  let appPromise: Promise<HonoApp> | null = null

  async function buildApp(): Promise<HonoApp> {
    const { Hono } = await import('hono')
    const app = new Hono() as unknown as HonoApp

    // =========================================================================
    // Auth Middleware
    // =========================================================================

    if (authConfig) {
      const { authMiddleware } = await import('./auth.js')
      app.use(authMiddleware(authConfig) as MiddlewareHandler)
    }

    // =========================================================================
    // Health Check
    // =========================================================================

    if (health) {
      app.get('/health', (async (c: HonoContext, _next: () => Promise<void>) => {
        // Optionally ping the DO to check connectivity
        const stub = getDOStub(c, rpcConfig)
        let doStatus: 'ok' | 'unavailable' = 'unavailable'

        if (stub) {
          try {
            const pingResult = await forwardToDO(stub, 'ping', [])
            if (pingResult.result) {
              doStatus = 'ok'
            }
          } catch {
            // DO unavailable
          }
        }

        return c.json({
          status: 'ok',
          service: 'dosql',
          durable_object: doStatus,
          timestamp: Date.now(),
        })
      }) as MiddlewareHandler)
    }

    // =========================================================================
    // RPC Endpoint (rpc.do protocol)
    // =========================================================================

    // POST /rpc - JSON-RPC 2.0 handler
    app.post('/rpc', (async (c: HonoContext, _next: () => Promise<void>) => {
      const stub = getDOStub(c, rpcConfig)
      if (!stub) {
        return c.json({
          jsonrpc: '2.0',
          error: { code: -32001, message: 'Durable Object not found' },
          id: null,
        } satisfies RPCResponse, 500)
      }

      let body: unknown
      try {
        body = await c.req.json()
      } catch {
        return c.json({
          jsonrpc: '2.0',
          error: { code: -32700, message: 'Parse error' },
          id: null,
        } satisfies RPCResponse, 400)
      }

      const allowedMethods = rpcConfig.allowedMethods ?? [
        'exec', 'query', 'prepare', 'run', 'finalize',
        'transaction', 'status', 'ping', 'listTables', 'describeTable',
      ]
      const maxBatchSize = rpcConfig.maxBatchSize ?? 100
      const timeoutMs = rpcConfig.timeoutMs ?? 30000

      // Batch request
      if (Array.isArray(body)) {
        const batch = body as RPCRequest[]
        if (batch.length > maxBatchSize) {
          return c.json({
            jsonrpc: '2.0',
            error: { code: -32005, message: 'Batch request too large', data: { max: maxBatchSize } },
            id: null,
          } satisfies RPCResponse, 400)
        }

        const responses: RPCResponse[] = await Promise.all(
          batch.map(async (req): Promise<RPCResponse> => {
            if (!allowedMethods.includes(req.method)) {
              return {
                jsonrpc: '2.0',
                error: { code: -32601, message: 'Method not found', data: { method: req.method } },
                id: req.id,
              }
            }
            const params = Array.isArray(req.params) ? req.params : req.params ? [req.params] : []
            const doResult = await forwardToDO(stub, req.method, params)
            if (doResult.error) {
              return { jsonrpc: '2.0', error: doResult.error, id: req.id }
            }
            return { jsonrpc: '2.0', result: doResult.result, id: req.id }
          })
        )
        return c.json(responses)
      }

      // Single request
      const request = body as RPCRequest
      if (!request.jsonrpc || !request.method || request.id === undefined) {
        return c.json({
          jsonrpc: '2.0',
          error: { code: -32600, message: 'Invalid Request' },
          id: request?.id ?? null,
        } satisfies RPCResponse, 400)
      }

      if (!allowedMethods.includes(request.method)) {
        return c.json({
          jsonrpc: '2.0',
          error: { code: -32601, message: 'Method not found', data: { method: request.method } },
          id: request.id,
        } satisfies RPCResponse)
      }

      const params = Array.isArray(request.params) ? request.params : request.params ? [request.params] : []
      const doResult = await forwardToDO(stub, request.method, params)
      if (doResult.error) {
        return c.json({ jsonrpc: '2.0', error: doResult.error, id: request.id } satisfies RPCResponse)
      }
      return c.json({ jsonrpc: '2.0', result: doResult.result, id: request.id } satisfies RPCResponse)
    }) as MiddlewareHandler)

    // GET /rpc - WebSocket upgrade
    if (websocket) {
      app.get('/rpc', (async (c: HonoContext, _next: () => Promise<void>) => {
        const upgradeHeader = c.req.header('upgrade')
        if (upgradeHeader?.toLowerCase() !== 'websocket') {
          return c.json({
            protocol: 'rpc.do',
            version: '2.0',
            transport: ['http-post', 'websocket'],
            methods_endpoint: '/rpc/methods',
          })
        }

        const stub = getDOStub(c, rpcConfig)
        if (!stub) {
          return c.json({ error: 'Durable Object not found' }, 500)
        }

        // Forward WebSocket upgrade to the DO
        return stub.fetch(c.req.raw) as unknown as void
      }) as MiddlewareHandler)
    }

    // GET /rpc/methods - List available methods
    app.get('/rpc/methods', (async (c: HonoContext, _next: () => Promise<void>) => {
      const allowedMethods = rpcConfig.allowedMethods ?? [
        'exec', 'query', 'prepare', 'run', 'finalize',
        'transaction', 'status', 'ping', 'listTables', 'describeTable',
      ]
      return c.json({
        methods: allowedMethods,
        protocol: 'json-rpc-2.0',
        transport: websocket ? ['http', 'websocket'] : ['http'],
      })
    }) as MiddlewareHandler)

    // =========================================================================
    // REST Endpoints
    // =========================================================================

    if (rest) {
      // POST /query - Execute a SELECT query
      app.post('/query', (async (c: HonoContext, _next: () => Promise<void>) => {
        let body: RestQueryRequest
        try {
          body = await c.req.json<RestQueryRequest>()
        } catch {
          return c.json({ error: 'Invalid JSON body' }, 400)
        }

        if (!body.sql) {
          return c.json({ error: 'Missing required field: sql' }, 400)
        }

        const stub = getDOStub(c, rpcConfig, body.db)
        if (!stub) {
          return c.json({ error: 'Database not available' }, 500)
        }

        const result = await forwardToDO(stub, 'exec', [{ sql: body.sql, params: body.params }])
        if (result.error) {
          return c.json({ error: result.error.message, details: result.error.data }, 500)
        }
        return c.json(result.result)
      }) as MiddlewareHandler)

      // POST /exec - Execute an INSERT/UPDATE/DELETE
      app.post('/exec', (async (c: HonoContext, _next: () => Promise<void>) => {
        let body: RestExecRequest
        try {
          body = await c.req.json<RestExecRequest>()
        } catch {
          return c.json({ error: 'Invalid JSON body' }, 400)
        }

        if (!body.sql) {
          return c.json({ error: 'Missing required field: sql' }, 400)
        }

        const stub = getDOStub(c, rpcConfig, body.db)
        if (!stub) {
          return c.json({ error: 'Database not available' }, 500)
        }

        const result = await forwardToDO(stub, 'exec', [{ sql: body.sql, params: body.params }])
        if (result.error) {
          return c.json({ error: result.error.message, details: result.error.data }, 500)
        }
        return c.json(result.result)
      }) as MiddlewareHandler)

      // POST /transaction - Execute multiple operations atomically
      app.post('/transaction', (async (c: HonoContext, _next: () => Promise<void>) => {
        let body: RestTransactionRequest
        try {
          body = await c.req.json<RestTransactionRequest>()
        } catch {
          return c.json({ error: 'Invalid JSON body' }, 400)
        }

        if (!body.ops || !Array.isArray(body.ops)) {
          return c.json({ error: 'Missing required field: ops (array)' }, 400)
        }

        const stub = getDOStub(c, rpcConfig, body.db)
        if (!stub) {
          return c.json({ error: 'Database not available' }, 500)
        }

        const result = await forwardToDO(stub, 'transaction', [{ ops: body.ops }])
        if (result.error) {
          return c.json({ error: result.error.message, details: result.error.data }, 500)
        }
        return c.json(result.result)
      }) as MiddlewareHandler)
    }

    return app
  }

  // Return a lazy-loading proxy
  const proxy: HonoApp = {
    post: () => proxy,
    get: () => proxy,
    all: () => proxy,
    use: () => proxy,
    route: () => proxy,
    async fetch(request: Request, env?: unknown, ctx?: unknown): Promise<Response> {
      if (!appPromise) {
        appPromise = buildApp()
      }
      const app = await appPromise
      return app.fetch(request, env, ctx)
    },
  }

  return proxy
}
