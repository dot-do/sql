/**
 * @dotdo/sql.do - CapnWeb RPC Client for DoSQL
 */

import type {
  SQLClient,
  SQLValue,
  QueryResult,
  QueryOptions,
  PreparedStatement,
  TransactionOptions,
  TransactionState,
  TransactionId,
  LSN,
  TableSchema,
  RPCRequest,
  RPCResponse,
  RPCError,
  StatementHash,
  IdempotencyConfig,
} from './types.js';
import { createTransactionId, createLSN, createStatementHash, DEFAULT_IDEMPOTENCY_CONFIG } from './types.js';

// =============================================================================
// Idempotency Key Generation
// =============================================================================

/**
 * Generate a random string of specified length using crypto
 */
function generateRandomString(length: number): string {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  const array = new Uint8Array(length);
  crypto.getRandomValues(array);
  return Array.from(array, (byte) => chars[byte % chars.length]).join('');
}

/**
 * Generate SHA-256 hash of input string and return first n characters
 */
async function hashString(input: string, length: number): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(input);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  const hashHex = hashArray.map((b) => b.toString(16).padStart(2, '0')).join('');
  return hashHex.slice(0, length);
}

/**
 * Generate an idempotency key with format: {timestamp}-{random}-{hash}
 * - timestamp: Unix milliseconds
 * - random: 8-char random string
 * - hash: First 8 chars of SHA-256 of SQL + params
 */
export async function generateIdempotencyKey(
  sql: string,
  params?: SQLValue[],
  prefix?: string
): Promise<string> {
  const timestamp = Date.now();
  const random = generateRandomString(8);
  const hashInput = sql + JSON.stringify(params ?? []);
  const hash = await hashString(hashInput, 8);

  const key = `${timestamp}-${random}-${hash}`;
  return prefix ? `${prefix}-${key}` : key;
}

/**
 * Check if a SQL statement is a mutation (INSERT, UPDATE, DELETE)
 */
export function isMutationQuery(sql: string): boolean {
  const trimmed = sql.trim().toUpperCase();
  return (
    trimmed.startsWith('INSERT') ||
    trimmed.startsWith('UPDATE') ||
    trimmed.startsWith('DELETE')
  );
}

// =============================================================================
// Client Configuration
// =============================================================================

export interface SQLClientConfig {
  /** DoSQL endpoint URL */
  url: string;
  /** Authentication token */
  token?: string;
  /** Database name */
  database?: string;
  /** Request timeout in milliseconds */
  timeout?: number;
  /** Retry configuration */
  retry?: RetryConfig;
  /** Idempotency configuration */
  idempotency?: IdempotencyConfig;
}

export interface RetryConfig {
  maxRetries: number;
  baseDelayMs: number;
  maxDelayMs: number;
}

const DEFAULT_RETRY: RetryConfig = {
  maxRetries: 3,
  baseDelayMs: 100,
  maxDelayMs: 5000,
};

// =============================================================================
// CapnWeb RPC Client
// =============================================================================

export class DoSQLClient implements SQLClient {
  private readonly config: Required<Omit<SQLClientConfig, 'token' | 'database'>> & {
    token?: string;
    database?: string;
  };
  private requestId = 0;
  private ws: WebSocket | null = null;
  private pendingRequests = new Map<string, {
    resolve: (value: unknown) => void;
    reject: (error: Error) => void;
    timeout: ReturnType<typeof setTimeout>;
    idempotencyKey?: string;
  }>();
  /** Map of request content hash to idempotency key for retry consistency */
  private idempotencyKeyCache = new Map<string, string>();

  constructor(config: SQLClientConfig) {
    this.config = {
      ...config,
      timeout: config.timeout ?? 30000,
      retry: config.retry ?? DEFAULT_RETRY,
      idempotency: config.idempotency ?? DEFAULT_IDEMPOTENCY_CONFIG,
    };
  }

  /**
   * Get or generate an idempotency key for a mutation request.
   * Reuses the same key for retries of the same request.
   */
  async getIdempotencyKey(sql: string, params?: SQLValue[]): Promise<string | undefined> {
    if (!this.config.idempotency.enabled || !isMutationQuery(sql)) {
      return undefined;
    }

    // Create a cache key based on the SQL and params
    const cacheKey = sql + JSON.stringify(params ?? []);

    // Check if we already have a key for this request (retry scenario)
    let key = this.idempotencyKeyCache.get(cacheKey);
    if (!key) {
      key = await generateIdempotencyKey(sql, params, this.config.idempotency.keyPrefix);
      this.idempotencyKeyCache.set(cacheKey, key);
    }

    return key;
  }

  /**
   * Clear the idempotency key for a request after success or final failure
   */
  clearIdempotencyKey(sql: string, params?: SQLValue[]): void {
    const cacheKey = sql + JSON.stringify(params ?? []);
    this.idempotencyKeyCache.delete(cacheKey);
  }

  // ===========================================================================
  // Connection Management
  // ===========================================================================

  private async ensureConnection(): Promise<WebSocket> {
    if (this.ws && this.ws.readyState === WebSocket.READY_STATE_OPEN) {
      return this.ws;
    }

    return new Promise((resolve, reject) => {
      const wsUrl = this.config.url.replace(/^http/, 'ws');
      this.ws = new WebSocket(wsUrl);

      this.ws.addEventListener('open', () => {
        resolve(this.ws!);
      });

      this.ws.addEventListener('error', (event: Event) => {
        reject(new Error(`WebSocket error: ${event}`));
      });

      this.ws.addEventListener('close', () => {
        this.ws = null;
        // Reject all pending requests
        for (const [id, pending] of this.pendingRequests) {
          clearTimeout(pending.timeout);
          pending.reject(new Error('Connection closed'));
          this.pendingRequests.delete(id);
        }
      });

      this.ws.addEventListener('message', (event: MessageEvent) => {
        this.handleMessage(event.data as string | ArrayBuffer);
      });
    });
  }

  private handleMessage(data: string | ArrayBuffer): void {
    try {
      const message = typeof data === 'string' ? data : new TextDecoder().decode(data);
      const response: RPCResponse = JSON.parse(message);

      const pending = this.pendingRequests.get(response.id);
      if (pending) {
        clearTimeout(pending.timeout);
        this.pendingRequests.delete(response.id);

        if (response.error) {
          pending.reject(new SQLError(response.error));
        } else {
          pending.resolve(response.result);
        }
      }
    } catch (error) {
      console.error('Failed to parse RPC response:', error);
    }
  }

  private async rpc<T>(method: string, params: unknown): Promise<T> {
    const ws = await this.ensureConnection();
    const id = `${++this.requestId}`;

    const request: RPCRequest = {
      id,
      method: method as RPCRequest['method'],
      params,
    };

    return new Promise<T>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.pendingRequests.delete(id);
        reject(new Error(`Request timeout: ${method}`));
      }, this.config.timeout);

      this.pendingRequests.set(id, {
        resolve: resolve as (value: unknown) => void,
        reject,
        timeout,
      });

      ws.send(JSON.stringify(request));
    });
  }

  // ===========================================================================
  // SQLClient Implementation
  // ===========================================================================

  async exec(sql: string, params?: SQLValue[], options?: QueryOptions): Promise<QueryResult> {
    const idempotencyKey = await this.getIdempotencyKey(sql, params);

    try {
      const result = await this.rpc<QueryResult>('exec', {
        sql,
        params: params ?? [],
        idempotencyKey,
        ...options,
      });

      // Clear idempotency key on success
      if (idempotencyKey) {
        this.clearIdempotencyKey(sql, params);
      }

      return result;
    } catch (error) {
      // For non-retryable errors, clear the idempotency key
      if (error instanceof SQLError && !this.isRetryableError(error)) {
        this.clearIdempotencyKey(sql, params);
      }
      throw error;
    }
  }

  /**
   * Check if an error is retryable (connection issues, timeouts, etc.)
   */
  private isRetryableError(error: SQLError): boolean {
    const retryableCodes = [
      'TIMEOUT',
      'CONNECTION_CLOSED',
      'NETWORK_ERROR',
      'UNAVAILABLE',
      'RESOURCE_EXHAUSTED',
    ];
    return retryableCodes.includes(error.code);
  }

  async query<T = Record<string, SQLValue>>(
    sql: string,
    params?: SQLValue[],
    options?: QueryOptions
  ): Promise<QueryResult<T>> {
    return this.rpc<QueryResult<T>>('query', {
      sql,
      params: params ?? [],
      ...options,
    });
  }

  async prepare(sql: string): Promise<PreparedStatement> {
    const result = await this.rpc<{ hash: string }>('prepare', { sql });
    return {
      sql,
      hash: createStatementHash(result.hash),
    };
  }

  async execute<T = Record<string, SQLValue>>(
    statement: PreparedStatement,
    params?: SQLValue[],
    options?: QueryOptions
  ): Promise<QueryResult<T>> {
    return this.rpc<QueryResult<T>>('execute', {
      hash: statement.hash,
      params: params ?? [],
      ...options,
    });
  }

  async beginTransaction(options?: TransactionOptions): Promise<TransactionState> {
    const result = await this.rpc<{
      id: string;
      isolationLevel: TransactionState['isolationLevel'];
      readOnly: boolean;
      startedAt: string;
      snapshotLSN: string;
    }>('beginTransaction', options ?? {});

    return {
      id: createTransactionId(result.id),
      isolationLevel: result.isolationLevel,
      readOnly: result.readOnly,
      startedAt: new Date(result.startedAt),
      snapshotLSN: createLSN(BigInt(result.snapshotLSN)),
    };
  }

  async commit(transactionId: TransactionId): Promise<LSN> {
    const result = await this.rpc<{ lsn: string }>('commit', { transactionId });
    return createLSN(BigInt(result.lsn));
  }

  async rollback(transactionId: TransactionId): Promise<void> {
    await this.rpc<void>('rollback', { transactionId });
  }

  async getSchema(tableName: string): Promise<TableSchema | null> {
    return this.rpc<TableSchema | null>('getSchema', { tableName });
  }

  async ping(): Promise<{ latency: number }> {
    const start = performance.now();
    await this.rpc<void>('ping', {});
    return { latency: performance.now() - start };
  }

  async close(): Promise<void> {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  // ===========================================================================
  // Convenience Methods
  // ===========================================================================

  /**
   * Execute a function within a transaction
   */
  async transaction<T>(
    fn: (tx: TransactionContext) => Promise<T>,
    options?: TransactionOptions
  ): Promise<T> {
    const state = await this.beginTransaction(options);
    const tx = new TransactionContext(this, state);

    try {
      const result = await fn(tx);
      await this.commit(state.id);
      return result;
    } catch (error) {
      await this.rollback(state.id);
      throw error;
    }
  }

  /**
   * Execute multiple statements in a batch
   */
  async batch(statements: Array<{ sql: string; params?: SQLValue[] }>): Promise<QueryResult[]> {
    return this.rpc<QueryResult[]>('batch', { statements });
  }
}

// =============================================================================
// Transaction Context
// =============================================================================

export class TransactionContext {
  constructor(
    private readonly client: DoSQLClient,
    private readonly state: TransactionState
  ) {}

  get transactionId(): TransactionId {
    return this.state.id;
  }

  async exec(sql: string, params?: SQLValue[]): Promise<QueryResult> {
    return this.client.exec(sql, params, { transactionId: this.state.id });
  }

  async query<T = Record<string, SQLValue>>(
    sql: string,
    params?: SQLValue[]
  ): Promise<QueryResult<T>> {
    return this.client.query<T>(sql, params, { transactionId: this.state.id });
  }
}

// =============================================================================
// Error Types
// =============================================================================

export class SQLError extends Error {
  readonly code: string;
  readonly details?: unknown;

  constructor(error: RPCError) {
    super(error.message);
    this.name = 'SQLError';
    this.code = error.code;
    this.details = error.details;
  }
}

// =============================================================================
// Factory Function
// =============================================================================

export function createSQLClient(config: SQLClientConfig): SQLClient {
  return new DoSQLClient(config);
}
