/**
 * @dotdo/lake.do - Client SDK for DoLake
 *
 * Lakehouse client for Cloudflare Workers using CapnWeb RPC.
 *
 * @example
 * ```typescript
 * import { createLakeClient } from '@dotdo/lake.do';
 *
 * const client = createLakeClient({
 *   url: 'https://lake.example.com',
 *   token: 'your-token',
 * });
 *
 * // Query lakehouse data
 * const result = await client.query<{ date: string; total: number }>(
 *   'SELECT date, SUM(amount) as total FROM orders GROUP BY date'
 * );
 *
 * // Subscribe to CDC stream
 * for await (const batch of client.subscribe({ tables: ['orders'] })) {
 *   console.log(`Received ${batch.events.length} events`);
 *   for (const event of batch.events) {
 *     console.log(event.operation, event.table, event.after);
 *   }
 * }
 *
 * // Time travel query
 * const historical = await client.query(
 *   'SELECT * FROM inventory',
 *   { asOf: new Date('2024-01-01') }
 * );
 *
 * await client.close();
 * ```
 */

// Client
export { DoLakeClient, LakeError, createLakeClient } from './client.js';
export type { LakeClientConfig, RetryConfig } from './client.js';

// Types
export type {
  // Branded types
  CDCEventId,
  PartitionKey,
  ParquetFileId,
  SnapshotId,
  CompactionJobId,
  // CDC types
  CDCStreamOptions,
  CDCBatch,
  CDCStreamState,
  // Query types
  LakeQueryOptions,
  LakeQueryResult,
  // Partition types
  PartitionStrategy,
  PartitionConfig,
  PartitionInfo,
  // Compaction types
  CompactionConfig,
  CompactionJob,
  // Snapshot types
  Snapshot,
  TimeTravelOptions,
  // Schema types
  LakeColumnType,
  LakeColumn,
  LakeSchema,
  TableMetadata,
  // RPC types
  LakeRPCMethod,
  LakeRPCRequest,
  LakeRPCResponse,
  LakeRPCError,
  // Client interface
  LakeClient,
  // Metrics
  LakeMetrics,
} from './types.js';

// Brand constructors
export {
  createCDCEventId,
  createPartitionKey,
  createParquetFileId,
  createSnapshotId,
  createCompactionJobId,
} from './types.js';

// Re-export common types from sql.do
export type { TransactionId, LSN, SQLValue, CDCOperation, CDCEvent } from './types.js';
