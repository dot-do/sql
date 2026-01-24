/**
 * lake.do/rpc - RPC transport integration using rpc.do
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
 * import { createRPCClient } from 'lake.do/rpc';
 *
 * // Create an RPC proxy for lakehouse operations over HTTP
 * const rpc = await createRPCClient({
 *   url: 'https://lake.example.com/rpc',
 *   transport: 'http',
 *   auth: 'your-token',
 * });
 *
 * const result = await rpc.query('SELECT * FROM orders');
 * ```
 *
 * @packageDocumentation
 * @module lake.do/rpc
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
 * @description Creates a typed RPC proxy that routes lakehouse operations through
 * the specified transport. This provides an alternative to the default CapnWeb
 * WebSocket transport used by the main lake.do client.
 *
 * Use cases:
 * - HTTP transport for serverless/stateless environments
 * - WebSocket transport for real-time CDC streaming
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
 * import { createRPCClient } from 'lake.do/rpc';
 *
 * interface LakeRPCInterface {
 *   query(sql: string, options?: unknown): Promise<{ rows: unknown[]; bytesScanned: number }>;
 *   getMetadata(table: string): Promise<unknown>;
 *   listPartitions(table: string): Promise<unknown[]>;
 *   listSnapshots(table: string): Promise<unknown[]>;
 *   subscribe(options: unknown): Promise<void>;
 * }
 *
 * // HTTP transport (default)
 * const httpClient = await createRPCClient<LakeRPCInterface>({
 *   url: 'https://lake.example.com/rpc',
 *   auth: 'my-token',
 * });
 * const result = await httpClient.query('SELECT * FROM orders');
 *
 * // WebSocket transport for CDC streaming
 * const wsClient = await createRPCClient<LakeRPCInterface>({
 *   url: 'wss://lake.example.com/rpc',
 *   transport: 'ws',
 *   auth: 'my-token',
 * });
 *
 * // Binding transport for Worker-to-Worker
 * const bindingClient = await createRPCClient<LakeRPCInterface>({
 *   url: 'LAKE_SERVICE',
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
