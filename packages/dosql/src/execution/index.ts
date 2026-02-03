/**
 * Standalone Query Execution Module
 *
 * Provides a SQL query executor that can operate independently of
 * Durable Object infrastructure. This enables:
 *
 * - Easier testing without DO infrastructure
 * - Reusable execution logic
 * - Embedding in non-Worker environments
 *
 * @example Basic usage
 * ```typescript
 * import {
 *   StandaloneExecutor,
 *   createMemoryStorage,
 *   createMemorySchemaManager
 * } from 'dosql/execution';
 *
 * // Create in-memory storage
 * const storage = createMemoryStorage();
 * const schema = createMemorySchemaManager(storage);
 *
 * // Create executor
 * const executor = new StandaloneExecutor({ storage, schema });
 *
 * // Execute SQL
 * await executor.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
 * await executor.execute("INSERT INTO users (name) VALUES ('Alice')");
 * const result = await executor.execute('SELECT * FROM users');
 * ```
 *
 * @packageDocumentation
 */

// Types
export type {
  KVStorage,
  ColumnDefinition,
  TableSchema,
  SchemaProvider,
  SchemaManager,
  WALEntry,
  WALWriter,
  QueryResult,
  ExecutorConfig,
} from './types.js';

// Memory storage
export { MemoryStorage, createMemoryStorage } from './memory-storage.js';

// Memory schema manager
export { MemorySchemaManager, createMemorySchemaManager } from './memory-schema.js';

// Executor
export { StandaloneExecutor, createStandaloneExecutor } from './executor.js';
