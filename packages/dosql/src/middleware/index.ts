/**
 * DoSQL Middleware
 *
 * Hono middleware for exposing DoSQL Durable Objects with:
 * - Authentication via oauth.do
 * - RPC protocol via rpc.do (JSON-RPC 2.0)
 * - REST API endpoints for SQL operations
 * - WebSocket support for streaming
 *
 * @packageDocumentation
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono'
 * import { createDoSQLRouter, authMiddleware, exposeDoSQL } from 'dosql/middleware'
 *
 * // Option 1: Complete router with all features
 * const app = new Hono()
 * app.route('/db', createDoSQLRouter({
 *   auth: { publicPaths: ['/health'] },
 *   rpc: { doBinding: 'SQL_DO' },
 *   rest: true,
 *   websocket: true,
 *   health: true,
 * }))
 *
 * // Option 2: Compose individual middleware
 * const app2 = new Hono()
 * app2.use('*', authMiddleware({ publicPaths: ['/health'] }))
 * app2.all('/rpc/*', exposeDoSQL({ doBinding: 'SQL_DO' }))
 *
 * export default app
 * ```
 */

// Auth middleware
export { authMiddleware, getUser } from './auth.js'
export type { AuthConfig, AuthUser } from './auth.js'

// RPC handler
export { rpcHandler, exposeDoSQL } from './rpc.js'
export type { RPCConfig, RPCRequest, RPCResponse, RPCBatchRequest, RPCBatchResponse } from './rpc.js'

// Router
export { createDoSQLRouter } from './router.js'
export type { DoSQLRouterConfig } from './router.js'
