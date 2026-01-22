/**
 * DoSQL Worker Query Engine
 *
 * A read-only query engine designed for Cloudflare Workers.
 * Executes queries against R2 storage or cached data.
 * Write operations are rejected or routed to a Durable Object.
 *
 * @example
 * ```typescript
 * import { WorkerQueryEngine } from './worker-engine.js';
 *
 * // Create engine with R2 bucket
 * const engine = new WorkerQueryEngine({
 *   storage: env.MY_R2_BUCKET,
 * });
 *
 * // Execute read queries
 * const result = await engine.query('SELECT * FROM users WHERE id = ?', [1]);
 *
 * // Writes are rejected in read-only mode
 * try {
 *   await engine.query('INSERT INTO users (name) VALUES (?)', ['Alice']);
 * } catch (error) {
 *   // ReadOnlyError: Cannot execute INSERT in read-only mode
 * }
 *
 * // Route writes through a DO stub
 * const engineWithDO = new WorkerQueryEngine({
 *   storage: env.MY_R2_BUCKET,
 *   doStub: env.DOSQL_DB.get(id),
 * });
 *
 * const writeResult = await engineWithDO.write('INSERT INTO users (name) VALUES (?)', ['Alice']);
 * ```
 *
 * @packageDocumentation
 */

import type { R2Bucket } from '../r2-index/types.js';
import type { QueryResult, Row, SqlValue, ExecutionStats } from './types.js';
import { QueryMode, ModeEnforcer, ReadOnlyError, isWriteOperation } from './modes.js';

// =============================================================================
// TYPES
// =============================================================================

/**
 * R2 Index Reader interface for reading from R2-based indexes.
 * This is a simplified interface for the R2 single-file index format.
 */
export interface R2IndexReader {
  /**
   * Execute a query against the R2 index.
   *
   * @param sql - The SQL query to execute
   * @param params - Query parameters
   * @returns Query result with rows and metadata
   */
  query<T = Row>(sql: string, params?: SqlValue[]): Promise<QueryResult<T>>;

  /**
   * Get the schema for one or more tables.
   *
   * @param tables - Table names to get schema for (empty = all tables)
   * @returns Schema information
   */
  getSchema(tables?: string[]): Promise<TableSchemaInfo[]>;

  /**
   * Close the reader and release resources.
   */
  close(): Promise<void>;
}

/**
 * Table schema information returned by the reader.
 */
export interface TableSchemaInfo {
  name: string;
  columns: { name: string; type: string; nullable: boolean }[];
  primaryKey: string[];
  rowCount?: bigint;
}

/**
 * Configuration for the WorkerQueryEngine.
 */
export interface WorkerEngineConfig {
  /**
   * Storage backend - either an R2 bucket or an R2 index reader.
   * The engine will use this for executing read queries.
   */
  storage: R2Bucket | R2IndexReader;

  /**
   * Optional Durable Object stub for routing writes.
   * If not provided, write operations will throw ReadOnlyError.
   */
  doStub?: DurableObjectStub;

  /**
   * Optional cache configuration for query results.
   */
  cache?: {
    /** Enable caching */
    enabled: boolean;
    /** Cache TTL in milliseconds */
    ttlMs?: number;
    /** Maximum cache entries */
    maxEntries?: number;
  };

  /**
   * Optional timeout for queries in milliseconds.
   */
  timeoutMs?: number;
}

/**
 * Write result returned when routing to a Durable Object.
 */
export interface WriteResult {
  /** Whether the write succeeded */
  success: boolean;

  /** Number of rows affected */
  rowsAffected: number;

  /** Last insert ID (if applicable) */
  lastInsertId?: bigint;

  /** Execution statistics */
  stats?: ExecutionStats;

  /** Error message if failed */
  error?: string;
}

/**
 * Durable Object stub interface (matches Cloudflare Workers)
 */
export interface DurableObjectStub {
  fetch(request: Request): Promise<Response>;
  id: { toString(): string };
}

// =============================================================================
// WORKER QUERY ENGINE
// =============================================================================

/**
 * Read-only query engine for Cloudflare Workers.
 *
 * This engine executes queries against R2 storage or cached data.
 * Write operations are either rejected (if no DO stub is configured)
 * or routed to a Durable Object for execution.
 *
 * Key features:
 * - Mode enforcement: Prevents accidental writes in read-only context
 * - R2 integration: Direct queries against R2-stored data
 * - DO routing: Optional routing of writes to a Durable Object
 * - Caching: Optional query result caching
 */
export class WorkerQueryEngine {
  /** Mode enforcer for read-only mode */
  private readonly enforcer = new ModeEnforcer(QueryMode.READ_ONLY);

  /** Configuration */
  private readonly config: WorkerEngineConfig;

  /** Query result cache */
  private readonly cache: Map<string, { result: QueryResult; expiresAt: number }> = new Map();

  /**
   * Create a new WorkerQueryEngine.
   *
   * @param config - Engine configuration
   */
  constructor(config: WorkerEngineConfig) {
    this.config = config;
  }

  /**
   * Get the current query mode.
   */
  getMode(): QueryMode {
    return this.enforcer.getMode();
  }

  /**
   * Check if a Durable Object stub is configured for writes.
   */
  hasDoStub(): boolean {
    return this.config.doStub !== undefined;
  }

  /**
   * Execute a read query.
   *
   * @param sql - The SQL query to execute
   * @param params - Query parameters
   * @returns Query result with rows and metadata
   * @throws ReadOnlyError if the query is a write operation
   */
  async query<T = Row>(sql: string, params?: SqlValue[]): Promise<QueryResult<T>> {
    // Enforce read-only mode
    this.enforcer.enforce(sql);

    // Check cache first
    if (this.config.cache?.enabled) {
      const cached = this.getCachedResult<T>(sql, params);
      if (cached) {
        return cached;
      }
    }

    const startTime = performance.now();

    // Execute query based on storage type
    const result = await this.executeQuery<T>(sql, params);

    // Add execution stats
    const endTime = performance.now();
    result.stats = {
      ...result.stats,
      executionTime: endTime - startTime,
      planningTime: 0,
      rowsScanned: result.rows.length,
      rowsReturned: result.rows.length,
    };

    // Cache result if enabled
    if (this.config.cache?.enabled) {
      this.cacheResult(sql, params, result);
    }

    return result;
  }

  /**
   * Execute a write operation by routing to the Durable Object.
   *
   * @param sql - The SQL statement to execute
   * @param params - Query parameters
   * @returns Write result from the Durable Object
   * @throws Error if no DO stub is configured
   */
  async write(sql: string, params?: SqlValue[]): Promise<WriteResult> {
    if (!this.config.doStub) {
      throw new Error(
        'No Durable Object stub configured for writes. ' +
        'Either configure a DO stub or execute writes directly on the Durable Object.'
      );
    }

    // Validate that this is actually a write operation
    if (!isWriteOperation(sql)) {
      throw new Error(
        `Expected a write operation but got: ${sql.substring(0, 50)}...`
      );
    }

    // Route to Durable Object via RPC
    const response = await this.config.doStub.fetch(
      new Request('https://internal/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql, params }),
      })
    );

    if (!response.ok) {
      const error = await response.text();
      return {
        success: false,
        rowsAffected: 0,
        error: error || `DO request failed with status ${response.status}`,
      };
    }

    const result = await response.json() as WriteResult;
    return result;
  }

  /**
   * Execute a query that might be either a read or write.
   * Reads are executed locally, writes are routed to the DO.
   *
   * @param sql - The SQL statement to execute
   * @param params - Query parameters
   * @returns Query or write result
   */
  async execute<T = Row>(
    sql: string,
    params?: SqlValue[]
  ): Promise<QueryResult<T> | WriteResult> {
    if (isWriteOperation(sql)) {
      return this.write(sql, params);
    }
    return this.query<T>(sql, params);
  }

  /**
   * Get the schema for one or more tables.
   *
   * @param tables - Table names (empty = all tables)
   * @returns Schema information
   */
  async getSchema(tables?: string[]): Promise<TableSchemaInfo[]> {
    const storage = this.config.storage;

    // If storage is an R2IndexReader, use its getSchema method
    if (this.isR2IndexReader(storage)) {
      return storage.getSchema(tables);
    }

    // For raw R2Bucket, we need to read index files
    // This is a simplified implementation
    return [];
  }

  /**
   * Clear the query result cache.
   */
  clearCache(): void {
    this.cache.clear();
  }

  /**
   * Get cache statistics.
   */
  getCacheStats(): { size: number; maxSize: number } {
    return {
      size: this.cache.size,
      maxSize: this.config.cache?.maxEntries ?? 0,
    };
  }

  // ===========================================================================
  // PRIVATE METHODS
  // ===========================================================================

  /**
   * Execute a query against the storage backend.
   */
  private async executeQuery<T = Row>(
    sql: string,
    params?: SqlValue[]
  ): Promise<QueryResult<T>> {
    const storage = this.config.storage;

    // If storage is an R2IndexReader, use its query method
    if (this.isR2IndexReader(storage)) {
      return storage.query<T>(sql, params);
    }

    // For raw R2Bucket, we need to implement query execution
    // This is a placeholder that returns empty results
    // Real implementation would parse SQL and read from R2
    return {
      rows: [] as T[],
      columns: [],
      stats: {
        planningTime: 0,
        executionTime: 0,
        rowsScanned: 0,
        rowsReturned: 0,
      },
    };
  }

  /**
   * Type guard for R2IndexReader.
   */
  private isR2IndexReader(storage: R2Bucket | R2IndexReader): storage is R2IndexReader {
    return 'query' in storage && typeof storage.query === 'function';
  }

  /**
   * Generate a cache key for a query.
   */
  private getCacheKey(sql: string, params?: SqlValue[]): string {
    return JSON.stringify({ sql, params: params ?? [] });
  }

  /**
   * Get a cached query result.
   */
  private getCachedResult<T>(sql: string, params?: SqlValue[]): QueryResult<T> | null {
    const key = this.getCacheKey(sql, params);
    const cached = this.cache.get(key);

    if (!cached) {
      return null;
    }

    if (cached.expiresAt < Date.now()) {
      this.cache.delete(key);
      return null;
    }

    return cached.result as QueryResult<T>;
  }

  /**
   * Cache a query result.
   */
  private cacheResult<T>(
    sql: string,
    params: SqlValue[] | undefined,
    result: QueryResult<T>
  ): void {
    const key = this.getCacheKey(sql, params);
    const ttlMs = this.config.cache?.ttlMs ?? 60000;
    const maxEntries = this.config.cache?.maxEntries ?? 100;

    // Evict oldest entries if cache is full
    if (this.cache.size >= maxEntries) {
      const oldestKey = this.cache.keys().next().value;
      if (oldestKey) {
        this.cache.delete(oldestKey);
      }
    }

    this.cache.set(key, {
      result: result as QueryResult,
      expiresAt: Date.now() + ttlMs,
    });
  }
}

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

/**
 * Create a WorkerQueryEngine with the given configuration.
 *
 * @param config - Engine configuration
 * @returns A new WorkerQueryEngine instance
 */
export function createWorkerEngine(config: WorkerEngineConfig): WorkerQueryEngine {
  return new WorkerQueryEngine(config);
}
