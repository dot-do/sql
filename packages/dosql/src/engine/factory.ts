/**
 * DoSQL Query Engine Factory
 *
 * Factory function for creating query engines based on execution context.
 * Automatically selects the appropriate engine type based on the configuration.
 *
 * @example
 * ```typescript
 * import { createQueryEngine } from './factory.js';
 *
 * // Create read-only engine for Worker
 * const workerEngine = createQueryEngine({
 *   mode: 'read-only',
 *   storage: env.MY_R2_BUCKET,
 * });
 *
 * // Create read-write engine for Durable Object
 * const doEngine = createQueryEngine({
 *   mode: 'read-write',
 *   storage: ctx.storage,
 * });
 *
 * // Create read-only engine with DO stub for routing writes
 * const hybridEngine = createQueryEngine({
 *   mode: 'read-only',
 *   storage: env.MY_R2_BUCKET,
 *   doStub: env.DOSQL_DB.get(id),
 * });
 * ```
 *
 * @packageDocumentation
 */

import type { R2Bucket } from '../r2-index/types.js';
import { WorkerQueryEngine, type R2IndexReader, type DurableObjectStub } from './worker-engine.js';
import { DOQueryEngine, type DurableObjectStorage, type CDCPublisher } from './do-engine.js';
import { QueryMode } from './modes.js';
import type { WALWriter } from '../wal/index.js';

// =============================================================================
// TYPES
// =============================================================================

/**
 * Common options for all query engines.
 */
interface BaseEngineOptions {
  /**
   * Optional timeout for queries in milliseconds.
   */
  timeoutMs?: number;
}

/**
 * Options for creating a read-only query engine.
 */
export interface ReadOnlyEngineOptions extends BaseEngineOptions {
  /** Engine mode - must be 'read-only' */
  mode: 'read-only';

  /** Storage backend - R2 bucket or R2 index reader */
  storage: R2Bucket | R2IndexReader;

  /** Optional DO stub for routing writes */
  doStub?: DurableObjectStub;

  /** Optional cache configuration */
  cache?: {
    enabled: boolean;
    ttlMs?: number;
    maxEntries?: number;
  };
}

/**
 * Options for creating a read-write query engine.
 */
export interface ReadWriteEngineOptions extends BaseEngineOptions {
  /** Engine mode - must be 'read-write' */
  mode: 'read-write';

  /** Storage backend - Durable Object storage */
  storage: DurableObjectStorage;

  /** Optional WAL writer for durability */
  wal?: WALWriter;

  /** Optional CDC publisher for change streaming */
  cdc?: CDCPublisher;

  /** Optional transaction timeout in milliseconds */
  transactionTimeoutMs?: number;

  /** Optional maximum concurrent transactions */
  maxConcurrentTransactions?: number;
}

/**
 * Union type for all engine options.
 */
export type QueryEngineOptions = ReadOnlyEngineOptions | ReadWriteEngineOptions;

/**
 * Union type for all query engines.
 */
export type QueryEngine = WorkerQueryEngine | DOQueryEngine;

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

/**
 * Create a query engine based on the specified mode and configuration.
 *
 * In 'read-only' mode, creates a WorkerQueryEngine for fast queries against
 * R2/cached data. Write operations are rejected unless a DO stub is provided
 * for routing.
 *
 * In 'read-write' mode, creates a DOQueryEngine for full CRUD operations with
 * ACID transactions.
 *
 * @param options - Engine options including mode and storage
 * @returns A WorkerQueryEngine or DOQueryEngine based on the mode
 *
 * @example
 * ```typescript
 * // Worker context - read-only
 * export default {
 *   async fetch(request: Request, env: Env) {
 *     const engine = createQueryEngine({
 *       mode: 'read-only',
 *       storage: env.MY_R2_BUCKET,
 *     });
 *
 *     const users = await engine.query('SELECT * FROM users');
 *     return new Response(JSON.stringify(users));
 *   }
 * };
 *
 * // Durable Object context - read-write
 * export class MyDO extends DurableObject {
 *   private engine: DOQueryEngine;
 *
 *   constructor(ctx: DurableObjectState, env: Env) {
 *     super(ctx, env);
 *     this.engine = createQueryEngine({
 *       mode: 'read-write',
 *       storage: ctx.storage,
 *     }) as DOQueryEngine;
 *   }
 *
 *   async fetch(request: Request) {
 *     await this.engine.execute('INSERT INTO users (name) VALUES (?)', ['Alice']);
 *     return new Response('OK');
 *   }
 * }
 * ```
 */
export function createQueryEngine(options: ReadOnlyEngineOptions): WorkerQueryEngine;
export function createQueryEngine(options: ReadWriteEngineOptions): DOQueryEngine;
export function createQueryEngine(options: QueryEngineOptions): QueryEngine;
export function createQueryEngine(options: QueryEngineOptions): QueryEngine {
  if (options.mode === 'read-only') {
    return new WorkerQueryEngine({
      storage: options.storage,
      doStub: options.doStub,
      cache: options.cache,
      timeoutMs: options.timeoutMs,
    });
  }

  return new DOQueryEngine({
    storage: options.storage,
    wal: options.wal,
    cdc: options.cdc,
    transactionTimeoutMs: options.transactionTimeoutMs,
    maxConcurrentTransactions: options.maxConcurrentTransactions,
  });
}

// =============================================================================
// TYPE GUARDS
// =============================================================================

/**
 * Check if an engine is a WorkerQueryEngine.
 */
export function isWorkerEngine(engine: QueryEngine): engine is WorkerQueryEngine {
  return engine instanceof WorkerQueryEngine;
}

/**
 * Check if an engine is a DOQueryEngine.
 */
export function isDOEngine(engine: QueryEngine): engine is DOQueryEngine {
  return engine instanceof DOQueryEngine;
}

/**
 * Check if options are for read-only mode.
 */
export function isReadOnlyOptions(options: QueryEngineOptions): options is ReadOnlyEngineOptions {
  return options.mode === 'read-only';
}

/**
 * Check if options are for read-write mode.
 */
export function isReadWriteOptions(options: QueryEngineOptions): options is ReadWriteEngineOptions {
  return options.mode === 'read-write';
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Create a read-only engine for Worker context.
 * Shorthand for createQueryEngine({ mode: 'read-only', ... }).
 *
 * @param storage - R2 bucket or R2 index reader
 * @param options - Additional options
 * @returns A WorkerQueryEngine
 */
export function createReadOnlyEngine(
  storage: R2Bucket | R2IndexReader,
  options?: Omit<ReadOnlyEngineOptions, 'mode' | 'storage'>
): WorkerQueryEngine {
  return createQueryEngine({
    mode: 'read-only',
    storage,
    ...options,
  });
}

/**
 * Create a read-write engine for Durable Object context.
 * Shorthand for createQueryEngine({ mode: 'read-write', ... }).
 *
 * @param storage - Durable Object storage
 * @param options - Additional options
 * @returns A DOQueryEngine
 */
export function createReadWriteEngine(
  storage: DurableObjectStorage,
  options?: Omit<ReadWriteEngineOptions, 'mode' | 'storage'>
): DOQueryEngine {
  return createQueryEngine({
    mode: 'read-write',
    storage,
    ...options,
  });
}

// =============================================================================
// RE-EXPORTS
// =============================================================================

// Re-export mode types
export { QueryMode } from './modes.js';
export { ReadOnlyError, isWriteOperation, ModeEnforcer } from './modes.js';

// Re-export engine classes
export { WorkerQueryEngine } from './worker-engine.js';
export { DOQueryEngine } from './do-engine.js';

// Re-export engine types
export type { WorkerEngineConfig, WriteResult as WorkerWriteResult, R2IndexReader, DurableObjectStub } from './worker-engine.js';
export type { DOEngineConfig, Transaction, WriteResult as DOWriteResult, DurableObjectStorage, CDCPublisher, ChangeEvent } from './do-engine.js';
