/**
 * Hibernating DoSQL Database
 *
 * A Durable Object implementation that supports WebSocket hibernation
 * for 95% cost reduction on idle connections.
 *
 * This extends the base DoSQLDatabase with hibernation support using
 * Cloudflare's WebSocket hibernation API.
 *
 * @packageDocumentation
 */

import { DurableObject } from 'cloudflare:workers';
import { createBTree, StringKeyCodec, JsonValueCodec, type BTree } from '../btree/index.js';
import { createDOBackend, type DOStorageBackend } from '../fsx/index.js';
import { createWALWriter, type WALWriter } from '../wal/index.js';
import {
  HibernatingDurableObject,
  type WebSocketSessionState,
  type WebSocketTag,
  type RPCMessage,
  type RPCResponse,
} from './hibernation.js';

// =============================================================================
// Types
// =============================================================================

export interface Env {
  DOSQL_DB: DurableObjectNamespace;
}

export interface QueryRequest {
  sql: string;
  params?: Record<string, unknown>;
}

export interface QueryResponse {
  success: boolean;
  rows?: Record<string, unknown>[];
  error?: string;
  stats?: {
    rowsAffected: number;
    executionTimeMs: number;
  };
}

export interface TableSchema {
  name: string;
  columns: { name: string; type: string; defaultValue?: string }[];
  primaryKey: string;
}

// =============================================================================
// RPC Method Types
// =============================================================================

type RPCMethods = {
  query: { sql: string; params?: unknown[] };
  exec: { sql: string; params?: unknown[] };
  beginTransaction: { isolationLevel?: string; readOnly?: boolean };
  commit: { txId: string };
  rollback: { txId: string };
  ping: Record<string, never>;
  getSchema: { table: string };
};

// =============================================================================
// Hibernating DoSQL Database
// =============================================================================

/**
 * Hibernation-aware DoSQL Database Durable Object.
 *
 * Features:
 * - WebSocket hibernation for ~95% cost reduction
 * - Persistent WebSocket connections across hibernation
 * - Session state preserved in WebSocket attachments
 * - RPC-based query interface
 * - Connection tagging for management
 *
 * @example
 * ```typescript
 * // In wrangler.toml
 * [[durable_objects]]
 * name = "DOSQL_DB"
 * class_name = "HibernatingDoSQLDatabase"
 *
 * // Client connects via WebSocket
 * const ws = new WebSocket('wss://worker/db/mydb/ws');
 * ws.send(JSON.stringify({ id: '1', method: 'query', params: { sql: 'SELECT 1' } }));
 * ```
 */
export class HibernatingDoSQLDatabase extends HibernatingDurableObject {
  private fsx: DOStorageBackend;
  private btree: BTree<string, Record<string, unknown>> | null = null;
  private wal: WALWriter | null = null;
  private initialized = false;
  private tables = new Map<string, TableSchema>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.fsx = createDOBackend(ctx.storage);
  }

  // ===========================================================================
  // Initialization
  // ===========================================================================

  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return;

    this.btree = createBTree(this.fsx, StringKeyCodec, JsonValueCodec);
    await this.btree.init();

    this.wal = createWALWriter(this.fsx);

    const schemaData = await this.fsx.read('_meta/schemas');
    if (schemaData) {
      const schemas = JSON.parse(new TextDecoder().decode(schemaData)) as TableSchema[];
      for (const schema of schemas) {
        this.tables.set(schema.name, schema);
      }
    }

    this.initialized = true;
  }

  // ===========================================================================
  // HTTP/WebSocket Handler
  // ===========================================================================

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      // Handle WebSocket upgrade
      if (request.headers.get('Upgrade') === 'websocket') {
        await this.ensureInitialized();
        return this.handleWebSocketUpgrade(request);
      }

      await this.ensureInitialized();

      // HTTP API routes (for backward compatibility)
      if (path === '/query' && request.method === 'POST') {
        return this.handleHttpQuery(request);
      }

      if (path === '/execute' && request.method === 'POST') {
        return this.handleHttpExecute(request);
      }

      if (path === '/tables' && request.method === 'GET') {
        return this.handleListTables();
      }

      if (path === '/health' && request.method === 'GET') {
        const stats = this.getHibernationStats();
        return new Response(JSON.stringify({
          status: 'ok',
          initialized: this.initialized,
          hibernation: stats,
          connections: this.getWebSockets().length,
        }), {
          headers: { 'Content-Type': 'application/json' },
        });
      }

      if (path === '/stats' && request.method === 'GET') {
        return this.handleGetStats();
      }

      return new Response(JSON.stringify({ error: 'Not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' },
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      return new Response(JSON.stringify({ error: message }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      });
    }
  }

  // ===========================================================================
  // WebSocket Message Handling (Hibernation API)
  // ===========================================================================

  /**
   * Handle incoming WebSocket messages.
   * This is called by the hibernation API when the DO wakes up.
   */
  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    // Call parent to update stats and metrics
    await super.webSocketMessage(ws, message);

    try {
      await this.ensureInitialized();

      const data = typeof message === 'string'
        ? message
        : new TextDecoder().decode(message);

      const rpc: RPCMessage = JSON.parse(data);

      // Track pending request
      const session = this.getSessionState(ws);
      if (session) {
        this.updateSessionState(ws, {
          pendingRequests: [...session.pendingRequests, rpc.id],
        });
      }

      // Route to handler
      const response = await this.handleRPCMethod(rpc, ws);
      this.sendResponse(ws, response);

      // Update query count
      if (session && (rpc.method === 'query' || rpc.method === 'exec')) {
        this.updateSessionState(ws, {
          metrics: {
            ...session.metrics,
            totalQueries: session.metrics.totalQueries + 1,
          },
        });
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      // Try to extract id from message for error response
      try {
        const rpc = JSON.parse(typeof message === 'string' ? message : new TextDecoder().decode(message));
        this.sendResponse(ws, {
          id: rpc.id ?? 'unknown',
          error: { code: -32603, message: errorMessage },
        });
      } catch {
        // Can't parse message, just log
        console.error('[HibernatingDoSQL] Message handling error:', errorMessage);
      }
    }

    // Allow hibernation after processing
    this.scheduleHibernation();
  }

  /**
   * Handle WebSocket close.
   * Clean up any resources associated with the connection.
   */
  async webSocketClose(
    ws: WebSocket,
    code: number,
    reason: string,
    wasClean: boolean
  ): Promise<void> {
    await super.webSocketClose(ws, code, reason, wasClean);

    const session = this.getSessionState(ws);
    if (session?.transaction) {
      // Rollback any active transaction
      try {
        console.log(`[HibernatingDoSQL] Rolling back orphaned transaction: ${session.transaction.txId}`);
        // In a real implementation, this would rollback the transaction
      } catch (e) {
        console.error('[HibernatingDoSQL] Error rolling back transaction:', e);
      }
    }

    this.scheduleHibernation();
  }

  /**
   * Handle WebSocket errors.
   */
  async webSocketError(ws: WebSocket, error: unknown): Promise<void> {
    await super.webSocketError(ws, error);
    this.scheduleHibernation();
  }

  // ===========================================================================
  // RPC Method Routing
  // ===========================================================================

  private async handleRPCMethod(rpc: RPCMessage, ws: WebSocket): Promise<RPCResponse> {
    const { id, method, params } = rpc;

    try {
      switch (method) {
        case 'ping':
          return { id, result: { pong: true, timestamp: Date.now() } };

        case 'query': {
          const p = params as RPCMethods['query'];
          const result = await this.executeQuery(p.sql, p.params);
          return { id, result };
        }

        case 'exec': {
          const p = params as RPCMethods['exec'];
          const result = await this.executeQuery(p.sql, p.params);
          return { id, result };
        }

        case 'beginTransaction': {
          const p = params as RPCMethods['beginTransaction'];
          const txId = crypto.randomUUID();
          const session = this.getSessionState(ws);
          if (session) {
            this.updateSessionState(ws, {
              transaction: {
                txId,
                startedAt: Date.now(),
                timeout: 30000,
              },
            });
          }
          return {
            id,
            result: {
              id: txId,
              isolationLevel: p.isolationLevel ?? 'SERIALIZABLE',
              readOnly: p.readOnly ?? false,
              startedAt: new Date().toISOString(),
              snapshotLSN: '0',
            },
          };
        }

        case 'commit': {
          const p = params as RPCMethods['commit'];
          const session = this.getSessionState(ws);
          if (session?.transaction?.txId !== p.txId) {
            throw new Error(`Transaction not found: ${p.txId}`);
          }
          this.updateSessionState(ws, { transaction: undefined });
          return { id, result: { lsn: '1' } };
        }

        case 'rollback': {
          const p = params as RPCMethods['rollback'];
          const session = this.getSessionState(ws);
          if (session?.transaction?.txId !== p.txId) {
            throw new Error(`Transaction not found: ${p.txId}`);
          }
          this.updateSessionState(ws, { transaction: undefined });
          return { id, result: {} };
        }

        case 'getSchema': {
          const p = params as RPCMethods['getSchema'];
          const schema = this.tables.get(p.table);
          return { id, result: schema ?? null };
        }

        default:
          return {
            id,
            error: { code: -32601, message: `Method not found: ${method}` },
          };
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      return { id, error: { code: -32603, message } };
    }
  }

  // ===========================================================================
  // Query Execution
  // ===========================================================================

  private async executeQuery(
    sql: string,
    params?: unknown[]
  ): Promise<{ rows: unknown[]; rowsAffected: number; executionTimeMs: number }> {
    const startTime = performance.now();

    // This is a simplified implementation
    // In production, this would use the full SQL parser and B-tree operations
    const normalized = sql.trim().toUpperCase();

    let rows: unknown[] = [];
    let rowsAffected = 0;

    if (normalized.startsWith('SELECT')) {
      // Mock SELECT response
      rows = [{ result: 1 }];
    } else if (normalized.startsWith('INSERT') || normalized.startsWith('UPDATE') || normalized.startsWith('DELETE')) {
      rowsAffected = 1;
    }

    const executionTimeMs = performance.now() - startTime;

    return { rows, rowsAffected, executionTimeMs };
  }

  // ===========================================================================
  // HTTP Handlers (backward compatibility)
  // ===========================================================================

  private async handleHttpQuery(request: Request): Promise<Response> {
    const body = (await request.json()) as QueryRequest;
    const result = await this.executeQuery(body.sql);

    return new Response(JSON.stringify({
      success: true,
      rows: result.rows,
      stats: {
        rowsAffected: result.rows.length,
        executionTimeMs: result.executionTimeMs,
      },
    }), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  private async handleHttpExecute(request: Request): Promise<Response> {
    const body = (await request.json()) as QueryRequest;
    const result = await this.executeQuery(body.sql);

    return new Response(JSON.stringify({
      success: true,
      stats: {
        rowsAffected: result.rowsAffected,
        executionTimeMs: result.executionTimeMs,
      },
    }), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  private async handleListTables(): Promise<Response> {
    const tables = Array.from(this.tables.values());
    return new Response(JSON.stringify({ tables }), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  private async handleGetStats(): Promise<Response> {
    const hibernation = this.getHibernationStats();
    const connections = this.getWebSockets();

    const connectionStats = connections.map(ws => {
      const session = this.getSessionState(ws);
      return {
        sessionId: session?.sessionId,
        connectedAt: session?.connectedAt,
        lastActivity: session?.lastActivity,
        hasTransaction: !!session?.transaction,
        metrics: session?.metrics,
      };
    });

    return new Response(JSON.stringify({
      hibernation,
      connections: connectionStats,
      tableCount: this.tables.size,
    }), {
      headers: { 'Content-Type': 'application/json' },
    });
  }
}
