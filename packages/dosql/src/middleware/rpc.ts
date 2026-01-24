/**
 * DoSQL RPC Handler
 *
 * Exposes DoSQL Durable Objects via the rpc.do protocol.
 * Supports both HTTP POST (JSON-RPC style) and WebSocket upgrade for streaming.
 *
 * @packageDocumentation
 */

// =============================================================================
// Types (declared inline to avoid requiring hono install for types)
// =============================================================================

/** Minimal Hono types */
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
  use(path: string, ...handlers: MiddlewareHandler[]): HonoApp
  fetch(request: Request, env?: unknown, ctx?: unknown): Promise<Response>
}

// =============================================================================
// RPC Protocol Types (rpc.do compatible)
// =============================================================================

/**
 * rpc.do JSON-RPC request format
 */
export interface RPCRequest {
  /** JSON-RPC version (always "2.0") */
  jsonrpc: '2.0'
  /** Method to invoke on the DO */
  method: string
  /** Parameters for the method */
  params?: unknown[] | Record<string, unknown>
  /** Request ID for correlation */
  id: string | number
}

/**
 * rpc.do JSON-RPC response format
 */
export interface RPCResponse {
  /** JSON-RPC version */
  jsonrpc: '2.0'
  /** Result on success */
  result?: unknown
  /** Error on failure */
  error?: {
    code: number
    message: string
    data?: unknown
  }
  /** Correlation ID from request */
  id: string | number
}

/**
 * rpc.do batch request (array of RPCRequest)
 */
export type RPCBatchRequest = RPCRequest[]

/**
 * rpc.do batch response (array of RPCResponse)
 */
export type RPCBatchResponse = RPCResponse[]

// =============================================================================
// RPC Configuration
// =============================================================================

/**
 * RPC handler configuration
 */
export interface RPCConfig {
  /** Durable Object namespace binding name in the Worker env */
  doBinding: string
  /** How to determine the DO instance ID from the request */
  resolveId?: (c: HonoContext) => string | DurableObjectId
  /** Allowed RPC methods (default: all DoSQL methods) */
  allowedMethods?: string[]
  /** Maximum batch size for batch requests (default: 100) */
  maxBatchSize?: number
  /** Request timeout in milliseconds (default: 30000) */
  timeoutMs?: number
}

// =============================================================================
// Default DoSQL Methods
// =============================================================================

const DEFAULT_ALLOWED_METHODS = [
  'exec',
  'query',
  'prepare',
  'run',
  'finalize',
  'transaction',
  'status',
  'ping',
  'listTables',
  'describeTable',
] as const

// =============================================================================
// Error Codes (JSON-RPC 2.0 standard + rpc.do extensions)
// =============================================================================

const RPC_ERRORS = {
  PARSE_ERROR: { code: -32700, message: 'Parse error' },
  INVALID_REQUEST: { code: -32600, message: 'Invalid Request' },
  METHOD_NOT_FOUND: { code: -32601, message: 'Method not found' },
  INVALID_PARAMS: { code: -32602, message: 'Invalid params' },
  INTERNAL_ERROR: { code: -32603, message: 'Internal error' },
  // rpc.do extensions
  DO_NOT_FOUND: { code: -32001, message: 'Durable Object not found' },
  DO_UNAVAILABLE: { code: -32002, message: 'Durable Object unavailable' },
  TIMEOUT: { code: -32003, message: 'Request timeout' },
  UNAUTHORIZED: { code: -32004, message: 'Unauthorized' },
  BATCH_TOO_LARGE: { code: -32005, message: 'Batch request too large' },
} as const

// =============================================================================
// Helper: Resolve DO Stub
// =============================================================================

function resolveDOStub(
  c: HonoContext,
  config: RPCConfig
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
    // Default: use 'db' query param, path segment, or 'default'
    const dbName = c.req.query('db') ?? 'default'
    doId = namespace.idFromName(dbName)
  }

  return namespace.get(doId)
}

// =============================================================================
// Helper: Execute Single RPC Call on DO
// =============================================================================

async function executeRPCCall(
  stub: DurableObjectStub,
  request: RPCRequest,
  allowedMethods: string[],
  timeoutMs: number
): Promise<RPCResponse> {
  // Validate method
  if (!allowedMethods.includes(request.method)) {
    return {
      jsonrpc: '2.0',
      error: { ...RPC_ERRORS.METHOD_NOT_FOUND, data: { method: request.method } },
      id: request.id,
    }
  }

  try {
    // Forward to DO via fetch with rpc.do protocol
    const controller = new AbortController()
    const timeout = setTimeout(() => controller.abort(), timeoutMs)

    try {
      const doResponse = await stub.fetch(
        new Request('https://do-internal/rpc', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            method: request.method,
            params: Array.isArray(request.params)
              ? request.params
              : request.params
                ? [request.params]
                : [],
          }),
          signal: controller.signal,
        })
      )

      clearTimeout(timeout)

      if (!doResponse.ok) {
        const errorText = await doResponse.text()
        return {
          jsonrpc: '2.0',
          error: {
            code: RPC_ERRORS.INTERNAL_ERROR.code,
            message: `DO error: ${doResponse.status}`,
            data: { status: doResponse.status, body: errorText },
          },
          id: request.id,
        }
      }

      const result = await doResponse.json()
      return {
        jsonrpc: '2.0',
        result,
        id: request.id,
      }
    } finally {
      clearTimeout(timeout)
    }
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') {
      return {
        jsonrpc: '2.0',
        error: { ...RPC_ERRORS.TIMEOUT, data: { timeoutMs } },
        id: request.id,
      }
    }
    return {
      jsonrpc: '2.0',
      error: {
        code: RPC_ERRORS.INTERNAL_ERROR.code,
        message: error instanceof Error ? error.message : 'Unknown error',
      },
      id: request.id,
    }
  }
}

// =============================================================================
// RPC Handler (Hono sub-app)
// =============================================================================

/**
 * Create a Hono sub-app that handles rpc.do protocol requests
 *
 * Routes:
 * - POST /rpc - JSON-RPC 2.0 request (single or batch)
 * - GET /rpc - WebSocket upgrade for streaming RPC
 * - GET /rpc/methods - List available methods
 *
 * @param config - RPC configuration
 * @returns Hono app instance
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono'
 * import { rpcHandler } from 'dosql/middleware'
 *
 * const app = new Hono()
 * const rpc = rpcHandler({ doBinding: 'SQL_DO' })
 * app.route('/api', rpc)
 * ```
 */
export function rpcHandler(config: RPCConfig): HonoApp {
  const allowedMethods = config.allowedMethods ?? [...DEFAULT_ALLOWED_METHODS]
  const maxBatchSize = config.maxBatchSize ?? 100
  const timeoutMs = config.timeoutMs ?? 30000

  // Dynamically import Hono to keep it optional
  let createApp: (() => Promise<HonoApp>) | null = null

  async function getApp(): Promise<HonoApp> {
    if (!createApp) {
      createApp = async () => {
        const { Hono } = await import('hono')
        const app = new Hono() as unknown as HonoApp

        // POST /rpc - JSON-RPC handler
        app.post('/rpc', (async (c: HonoContext, _next: () => Promise<void>) => {
          const stub = resolveDOStub(c, config)
          if (!stub) {
            return c.json({
              jsonrpc: '2.0',
              error: RPC_ERRORS.DO_NOT_FOUND,
              id: null,
            } satisfies RPCResponse, 500)
          }

          let body: unknown
          try {
            body = await c.req.json()
          } catch {
            return c.json({
              jsonrpc: '2.0',
              error: RPC_ERRORS.PARSE_ERROR,
              id: null,
            } satisfies RPCResponse, 400)
          }

          // Batch request
          if (Array.isArray(body)) {
            const batch = body as RPCBatchRequest
            if (batch.length > maxBatchSize) {
              return c.json({
                jsonrpc: '2.0',
                error: { ...RPC_ERRORS.BATCH_TOO_LARGE, data: { max: maxBatchSize, received: batch.length } },
                id: null,
              } satisfies RPCResponse, 400)
            }

            const responses = await Promise.all(
              batch.map((req) => executeRPCCall(stub, req, allowedMethods, timeoutMs))
            )
            return c.json(responses)
          }

          // Single request
          const request = body as RPCRequest
          if (!request.jsonrpc || !request.method || request.id === undefined) {
            return c.json({
              jsonrpc: '2.0',
              error: RPC_ERRORS.INVALID_REQUEST,
              id: request?.id ?? null,
            } satisfies RPCResponse, 400)
          }

          const response = await executeRPCCall(stub, request, allowedMethods, timeoutMs)
          return c.json(response)
        }) as MiddlewareHandler)

        // GET /rpc - WebSocket upgrade for streaming
        app.get('/rpc', (async (c: HonoContext, _next: () => Promise<void>) => {
          const upgradeHeader = c.req.header('upgrade')
          if (upgradeHeader?.toLowerCase() !== 'websocket') {
            return c.json({ error: 'WebSocket upgrade required' }, 426)
          }

          const stub = resolveDOStub(c, config)
          if (!stub) {
            return c.json({
              jsonrpc: '2.0',
              error: RPC_ERRORS.DO_NOT_FOUND,
              id: null,
            } satisfies RPCResponse, 500)
          }

          // Forward WebSocket upgrade to the DO
          return stub.fetch(c.req.raw)
        }) as MiddlewareHandler)

        // GET /rpc/methods - List available methods
        app.get('/rpc/methods', (async (c: HonoContext, _next: () => Promise<void>) => {
          return c.json({
            methods: allowedMethods,
            protocol: 'json-rpc-2.0',
            transport: ['http', 'websocket'],
          })
        }) as MiddlewareHandler)

        return app
      }
    }

    return createApp()
  }

  // Return a proxy that lazily initializes the Hono app
  const proxy: HonoApp = {
    post: () => proxy,
    get: () => proxy,
    all: () => proxy,
    use: () => proxy,
    async fetch(request: Request, env?: unknown, ctx?: unknown): Promise<Response> {
      const app = await getApp()
      return app.fetch(request, env, ctx)
    },
  }

  return proxy
}

// =============================================================================
// Middleware Version
// =============================================================================

/**
 * DoSQL RPC middleware that can be mounted on any Hono path
 *
 * Exposes DoSQL operations via the rpc.do JSON-RPC 2.0 protocol.
 * Handles both single and batch requests, plus WebSocket upgrade.
 *
 * @param config - RPC configuration
 * @returns Hono middleware handler
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono'
 * import { exposeDoSQL } from 'dosql/middleware'
 *
 * const app = new Hono()
 *
 * // Mount RPC on /db path
 * app.all('/db/*', exposeDoSQL({
 *   doBinding: 'SQL_DO',
 *   resolveId: (c) => c.req.query('name') ?? 'default',
 * }))
 * ```
 */
export function exposeDoSQL(config: RPCConfig): MiddlewareHandler {
  const allowedMethods = config.allowedMethods ?? [...DEFAULT_ALLOWED_METHODS]
  const maxBatchSize = config.maxBatchSize ?? 100
  const timeoutMs = config.timeoutMs ?? 30000

  return async (c, _next) => {
    const stub = resolveDOStub(c, config)
    if (!stub) {
      return c.json({
        jsonrpc: '2.0',
        error: RPC_ERRORS.DO_NOT_FOUND,
        id: null,
      } satisfies RPCResponse, 500)
    }

    // WebSocket upgrade
    const upgradeHeader = c.req.header('upgrade')
    if (upgradeHeader?.toLowerCase() === 'websocket') {
      return stub.fetch(c.req.raw) as unknown as void
    }

    // Only accept POST for RPC
    if (c.req.method !== 'POST') {
      return c.json({ error: 'Method not allowed. Use POST for RPC calls.' }, 405)
    }

    let body: unknown
    try {
      body = await c.req.json()
    } catch {
      return c.json({
        jsonrpc: '2.0',
        error: RPC_ERRORS.PARSE_ERROR,
        id: null,
      } satisfies RPCResponse, 400)
    }

    // Batch request
    if (Array.isArray(body)) {
      const batch = body as RPCBatchRequest
      if (batch.length > maxBatchSize) {
        return c.json({
          jsonrpc: '2.0',
          error: { ...RPC_ERRORS.BATCH_TOO_LARGE, data: { max: maxBatchSize, received: batch.length } },
          id: null,
        } satisfies RPCResponse, 400)
      }

      const responses = await Promise.all(
        batch.map((req) => executeRPCCall(stub, req, allowedMethods, timeoutMs))
      )
      return c.json(responses)
    }

    // Single request
    const request = body as RPCRequest
    if (!request.jsonrpc || !request.method || request.id === undefined) {
      return c.json({
        jsonrpc: '2.0',
        error: RPC_ERRORS.INVALID_REQUEST,
        id: request?.id ?? null,
      } satisfies RPCResponse, 400)
    }

    const response = await executeRPCCall(stub, request, allowedMethods, timeoutMs)
    return c.json(response)
  }
}
