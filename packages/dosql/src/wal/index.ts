/**
 * WAL (Write-Ahead Log) Module for DoSQL
 *
 * Provides durability through a write-ahead log with:
 * - Segment-based storage aligned with DO/R2 limits
 * - CRC32 checksums for integrity verification
 * - Checkpoint and recovery support
 * - Async iterator interface for reading
 *
 * @example
 * ```typescript
 * import { createWALWriter, createWALReader, createCheckpointManager } from 'dosql/wal';
 *
 * // Create writer
 * const writer = createWALWriter(backend);
 *
 * // Write entries
 * await writer.append({
 *   timestamp: Date.now(),
 *   txnId: 'txn_1',
 *   op: 'INSERT',
 *   table: 'users',
 *   after: new TextEncoder().encode(JSON.stringify({ id: 1, name: 'Alice' }))
 * });
 *
 * // Flush to storage
 * await writer.flush();
 *
 * // Read entries
 * const reader = createWALReader(backend);
 * for await (const entry of reader.iterate({ fromLSN: 0n })) {
 *   console.log(entry);
 * }
 * ```
 *
 * @packageDocumentation
 */

// Types
export {
  // Core types
  type WALEntry,
  type WALSegment,
  type WALOperation,
  type WALConfig,
  DEFAULT_WAL_CONFIG,

  // Writer types
  type WALWriter,
  type AppendOptions,
  type AppendResult,

  // Reader types
  type WALReader,
  type ReadOptions,

  // Checkpoint types
  type Checkpoint,
  type CheckpointManager,
  type RecoveryState,

  // Encoder types
  type WALEncoder,
  type WALStorage,

  // Error types
  WALError,
  WALErrorCode,
} from './types.js';

// Writer
export {
  createWALWriter,
  DefaultWALEncoder,
  crc32,
  WALTransaction,
  createTransaction,
  generateTxnId,
  // HLC support
  type WALWriterHLC,
  type WALWriterHLCConfig,
} from './writer.js';

// Reader
export {
  createWALReader,
  tailWAL,
  readWALBatched,
  reconstructTransactions,
  type TailOptions,
  type BatchReadOptions,
  type ReconstructedTransaction,
} from './reader.js';

// Checkpoint
export {
  createCheckpointManager,
  performRecovery,
  needsRecovery,
  createAutoCheckpointer,
  type FullRecoveryOptions,
  type AutoCheckpointOptions,
} from './checkpoint.js';

// Retention - Core exports
export {
  createWALRetentionManager,
  DEFAULT_RETENTION_POLICY,
  RETENTION_PRESETS,
  RetentionError,
  RetentionErrorCode,
  type WALRetentionManager,
  type RetentionPolicy,
  type ActiveReader,
  type RetentionCheckResult,
  type RetentionCleanupResult,
  type StorageStats,
  type EntryStats,
  type SegmentEntryStats,
  type FragmentationInfo,
  type MergeResult,
  type CompactResult,
  type ThrottleConfig,
  type RetentionMetrics,
  type CleanupRecord,
  type HealthCheckResult,
  type DynamicPolicyResult,
  type ReplicationStatus,
  type RegionReplicationStatus,
  type CheckpointInfo,
  type LowActivityWindow,
  type DynamicPolicyConfig,
  type CDCIntegrationConfig,
  type MetricsReporter,
  type RetentionMetric,
  type RetentionWarning,
  type CleanupProgressEvent,
  type PolicyDecision,
  // New types for WAL retention features
  type ExpiredEntry,
  type WALStats,
  type TruncateResult,
  type CompactWALResult,
  type ForceCleanupOptions,
  type ForceCleanupResult,
  type CleanupLatencyHistogram,
  type GrowthStats,
  // Extended manager type
  type ExtendedWALRetentionManager,
  // Checkpoint integration
  type CheckpointManagerForRetention,
  // Policy utilities
  parseSizeString,
  formatSizeString,
  validatePolicy,
  resolvePolicy,
  isInTimeWindow,
  getNextCronTime,
  // Scheduler utilities
  createRetentionScheduler,
  type RetentionScheduler,
  // Metrics utilities
  createMetricsCollector,
  createConsoleMetricsReporter,
  createNoopMetricsReporter,
  createBufferedMetricsReporter,
  performHealthCheck,
  type MetricsCollector,
} from './retention.js';
