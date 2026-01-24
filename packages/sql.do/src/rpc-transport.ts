/**
 * sql.do/rpc - RPC transport integration using rpc.do
 *
 * Provides alternative transports beyond the default CapnWeb WebSocket transport.
 * Supports HTTP, WebSocket, and Cloudflare Worker binding transports via rpc.do.
 *
 * Users install rpc.do separately to use this module:
 * ```bash
 * npm install rpc.do
 * ```
 *
 * @example
 * ```typescript
 * import { createRPCClient } from 'sql.do/rpc';
 *
 * // Create an RPC proxy for SQL operations over HTTP
 * const rpc = await createRPCClient({
 *   url: 'https://sql.example.com/rpc',
 *   transport: 'http',
 *   auth: 'your-token',
 * });
 *
 * const result = await rpc.query('SELECT * FROM users');
 * ```
 *
 * @packageDocumentation
 * @module sql.do/rpc
 */

/**
 * Transport type for the RPC client.
 *
 * - `'http'` - Standard HTTP/HTTPS requests (default)
 * - `'ws'` - WebSocket persistent connection
 * - `'binding'` - Cloudflare Worker service binding (zero-latency)
 *
 * @public
 * @since 0.2.0
 */
export type RPCTransportType = 'http' | 'ws' | 'binding';

/**
 * Options for creating an RPC client.
 *
 * @public
 * @since 0.2.0
 */
export interface RPCClientOptions {
  /** RPC endpoint URL (or binding name for 'binding' transport) */
  url: string;
  /** Transport type to use */
  transport?: RPCTransportType;
  /** Authentication token */
  auth?: string;
}

/**
 * RPC proxy result type.
 *
 * @description Represents a typed RPC result with success/error discrimination.
 *
 * @public
 * @since 0.2.0
 */
export interface RPCResult<T = unknown> {
  /** Whether the call succeeded */
  ok: boolean;
  /** Result value on success */
  data?: T;
  /** Error details on failure */
  error?: { code: string; message: string; details?: unknown };
}

/**
 * Creates an RPC client proxy using rpc.do transports.
 *
 * @description Creates a typed RPC proxy that routes SQL operations through
 * the specified transport. This provides an alternative to the default CapnWeb
 * WebSocket transport used by the main sql.do client.
 *
 * Use cases:
 * - HTTP transport for serverless/stateless environments
 * - WebSocket transport for real-time bidirectional communication
 * - Binding transport for zero-latency Worker-to-Worker calls
 *
 * Requires `rpc.do` to be installed as a peer dependency.
 *
 * @typeParam T - The shape of the RPC interface (methods available on the proxy)
 * @param options - RPC client configuration
 * @returns A typed RPC proxy object with the specified interface
 * @throws {Error} When rpc.do is not installed or transport creation fails
 *
 * @example
 * ```typescript
 * import { createRPCClient } from 'sql.do/rpc';
 *
 * interface SQLRPCInterface {
 *   query(sql: string, params?: unknown[]): Promise<{ rows: unknown[] }>;
 *   exec(sql: string, params?: unknown[]): Promise<{ rowsAffected: number }>;
 * }
 *
 * // HTTP transport (default)
 * const httpClient = await createRPCClient<SQLRPCInterface>({
 *   url: 'https://sql.example.com/rpc',
 *   auth: 'my-token',
 * });
 * const result = await httpClient.query('SELECT 1');
 *
 * // WebSocket transport for persistent connections
 * const wsClient = await createRPCClient<SQLRPCInterface>({
 *   url: 'wss://sql.example.com/rpc',
 *   transport: 'ws',
 *   auth: 'my-token',
 * });
 *
 * // Binding transport for Worker-to-Worker
 * const bindingClient = await createRPCClient<SQLRPCInterface>({
 *   url: 'SQL_SERVICE',
 *   transport: 'binding',
 * });
 * ```
 *
 * @public
 * @since 0.2.0
 */
export async function createRPCClient<T = Record<string, (...args: unknown[]) => Promise<unknown>>>(
  options: RPCClientOptions
): Promise<T> {
  const { RPC } = await import('rpc.do');

  let transport: unknown;
  if (options.transport === 'ws') {
    const { ws } = await import('rpc.do/transports');
    transport = ws(options.url, options.auth);
  } else if (options.transport === 'binding') {
    const { binding } = await import('rpc.do/transports');
    transport = binding(options.url as unknown);
  } else {
    const { http } = await import('rpc.do/transports');
    transport = http(options.url, options.auth);
  }

  return RPC<T>(transport);
}
