/**
 * DoSQL Durable Object Database
 *
 * A minimal Durable Object that uses DoSQL storage primitives:
 * - B-tree for row-oriented storage
 * - WAL for durability
 * - FSX backend for DO storage abstraction
 *
 * NOTE: This file uses direct imports to avoid pulling in heavy dependencies
 * like ai-evaluate, capnweb, iceberg-js that are not compatible with Workers.
 *
 * This is a thin coordinator class. The actual logic is in:
 * - returning.ts       - RETURNING clause parsing and evaluation
 * - sql-params.ts      - Parameter substitution and SQL value parsing
 * - input-validation.ts - Request body size limits and validation
 * - schema-manager.ts  - Table schema CRUD, auto-increment, defaults
 * - query-executor.ts  - SQL routing and DML execution
 * - request-handler.ts - HTTP request routing and response formatting
 */

import { DurableObject } from 'cloudflare:workers';
import { createLogger } from '../logging/index.js';

const logger = createLogger({ defaultContext: { module: 'database' } });

// Direct imports to avoid transitive dependencies
import { createBTree, StringKeyCodec, JsonValueCodec, type BTree } from '../btree/index.js';
import { createDOBackend, type DOStorageBackend } from '../fsx/index.js';
import { createWALWriter, type WALWriter } from '../wal/index.js';
import { RateLimiter } from './rate-limiter.js';
import { createDefaultAuthConfig, type AuthConfig } from './auth.js';

// Extracted modules
import { SchemaManager } from './schema-manager.js';
import { QueryExecutor } from './query-executor.js';
import { RequestHandler } from './request-handler.js';

// Re-export types for backward compatibility
export type { TableSchema } from './schema-manager.js';
export type { QueryRequest, QueryResponse } from './request-handler.js';
export { MAX_REQUEST_BODY_SIZE, MAX_SQL_LENGTH, MAX_PARAM_COUNT } from './input-validation.js';

// =============================================================================
// Types
// =============================================================================

export interface Env {
  DOSQL_DB: DurableObjectNamespace;
}

// =============================================================================
// DoSQL Database Durable Object
// =============================================================================

export class DoSQLDatabase extends DurableObject {
  private fsx: DOStorageBackend;
  private btree: BTree<string, Record<string, unknown>> | null = null;
  private wal: WALWriter | null = null;
  private initialized = false;
  private tables = new Map<string, { name: string; columns: { name: string; type: string; defaultValue?: string }[]; primaryKey: string }>();
  private maxIdCache = new Map<string, number>();
  private rateLimiter: RateLimiter;
  private authConfig: AuthConfig;

  // Extracted modules
  private schemaManager: SchemaManager;
  private queryExecutor: QueryExecutor;
  private requestHandler: RequestHandler;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    // Create FSX backend using DO storage
    this.fsx = createDOBackend(ctx.storage);
    // Initialize rate limiter with default config
    this.rateLimiter = new RateLimiter();
    // Initialize auth config (disabled by default for backward compatibility)
    this.authConfig = createDefaultAuthConfig();

    // Initialize extracted modules
    this.schemaManager = new SchemaManager(this.tables, this.maxIdCache, this.fsx);
    this.queryExecutor = new QueryExecutor(
      this.schemaManager,
      () => this.btree!,
      () => this.wal,
    );
    this.requestHandler = new RequestHandler(
      this.queryExecutor,
      this.schemaManager,
      () => this.initialized,
      this.rateLimiter,
      () => this.authConfig,
    );
  }

  /**
   * Configure authentication for this database instance.
   * Call this to enable auth with bearer tokens and/or API keys.
   */
  setAuthConfig(config: AuthConfig): void {
    this.authConfig = config;
  }

  /**
   * Initialize B-tree and WAL on first use
   */
  private async ensureInitialized(): Promise<void> {
    if (this.initialized) return;

    // Create B-tree for data storage
    this.btree = createBTree(this.fsx, StringKeyCodec, JsonValueCodec);
    await this.btree.init();

    // Create WAL writer
    this.wal = createWALWriter(this.fsx);

    // Load table schemas from storage
    await this.schemaManager.loadSchemas();

    this.initialized = true;
  }

  /**
   * HTTP API handler
   */
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      await this.ensureInitialized();

      return this.requestHandler.handleRequest(request, path);
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      return new Response(JSON.stringify({ error: message }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      });
    }
  }

  /**
   * Alarm handler for periodic tasks
   */
  async alarm(): Promise<void> {
    // Perform periodic maintenance
    await this.ensureInitialized();

    // Compact storage
    const stats = await this.fsx.getStats();
    logger.info('DoSQL alarm', { fileCount: stats.fileCount, totalSize: stats.totalSize });

    // Reschedule alarm for next hour
    const nextAlarm = Date.now() + 60 * 60 * 1000;
    this.ctx.storage.setAlarm(nextAlarm);
  }
}
