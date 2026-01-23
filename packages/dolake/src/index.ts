/**
 * @dotdo/dolake
 *
 * DoLake - Lakehouse worker for CDC streaming to Iceberg/Parquet on R2
 *
 * Architecture:
 * ```
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │                          DoSQL Instances (Shards)                        │
 * │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐       │
 * │  │ Shard 1 │  │ Shard 2 │  │ Shard 3 │  │ Shard N │  │  ...    │       │
 * │  │ (JSON)  │  │ (JSON)  │  │ (JSON)  │  │ (JSON)  │  │         │       │
 * │  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘       │
 * │       │            │            │            │            │            │
 * │       │  WebSocket │ Hibernation│ (95% cost │discount)   │            │
 * │       ▼            ▼            ▼            ▼            ▼            │
 * │  ┌───────────────────────────────────────────────────────────────┐     │
 * │  │                        DoLake (Aggregator)                     │     │
 * │  │  - Receives CDC batches via WebSocket                          │     │
 * │  │  - Buffers by table/partition                                  │     │
 * │  │  - Writes Parquet to R2                                        │     │
 * │  │  - Maintains Iceberg metadata                                  │     │
 * │  │  - Exposes REST Catalog API                                    │     │
 * │  └───────────────────────────────────────────────────────────────┘     │
 * │                              │                                         │
 * │                              ▼                                         │
 * │                        ┌──────────┐                                    │
 * │                        │    R2    │                                    │
 * │                        │ (Iceberg)│                                    │
 * │                        └──────────┘                                    │
 * │                              │                                         │
 * │                              ▼                                         │
 * │  ┌───────────────────────────────────────────────────────────────┐     │
 * │  │              External Query Engines                            │     │
 * │  │   Spark  │  DuckDB  │  Trino  │  Flink  │  DataFusion         │     │
 * │  └───────────────────────────────────────────────────────────────┘     │
 * └─────────────────────────────────────────────────────────────────────────┘
 * ```
 *
 * @example Basic Usage
 * ```typescript
 * // wrangler.jsonc
 * {
 *   "durable_objects": {
 *     "bindings": [{
 *       "name": "DOLAKE",
 *       "class_name": "DoLake"
 *     }]
 *   },
 *   "r2_buckets": [{
 *     "binding": "LAKEHOUSE_BUCKET",
 *     "bucket_name": "my-lakehouse"
 *   }]
 * }
 *
 * // worker.ts
 * import { DoLake } from '@dotdo/dolake';
 *
 * export { DoLake };
 *
 * export default {
 *   async fetch(request: Request, env: Env) {
 *     const id = env.DOLAKE.idFromName('lakehouse');
 *     const stub = env.DOLAKE.get(id);
 *     return stub.fetch(request);
 *   }
 * };
 * ```
 *
 * @packageDocumentation
 */

// =============================================================================
// DoLake Durable Object
// =============================================================================

export { DoLake, type DoLakeEnv } from './dolake.js';
export { DoLake as default } from './dolake.js';

// =============================================================================
// Types
// =============================================================================

export {
  // CDC Event Types
  type CDCEvent,
  type CDCOperation,
  CDCOperationCode,

  // RPC Message Types
  type RpcMessage,
  type RpcMessageType,
  type CDCBatchMessage,
  type ConnectMessage,
  type HeartbeatMessage,
  type FlushRequestMessage,
  type AckMessage,
  type NackMessage,
  type StatusMessage,
  type AckStatus,
  type AckDetails,
  type NackReason,
  type RateLimitInfo,

  // Buffer Types
  type BufferedBatch,
  type BufferStats,
  type DoLakeState,

  // Flush Types
  type FlushResult,
  type FlushTrigger,

  // Configuration
  type DoLakeConfig,
  DEFAULT_DOLAKE_CONFIG,

  // Client Capabilities
  type ClientCapabilities,
  DEFAULT_CLIENT_CAPABILITIES,

  // WebSocket Attachment
  type WebSocketAttachment,
  CapabilityFlags,
  encodeCapabilities,
  decodeCapabilities,

  // Iceberg Types
  type IcebergSchema,
  type IcebergField,
  type IcebergPartitionSpec,
  type IcebergPartitionField,
  type IcebergSortOrder,
  type IcebergSortField,
  type IcebergSnapshot,
  type IcebergTableMetadata,
  type ManifestFile,
  type DataFile,
  type SnapshotSummary,
  type NamespaceIdentifier,
  type NamespaceProperties,
  type TableIdentifier,

  // Schema Versioning
  CURRENT_SCHEMA_VERSION,
  MIN_SUPPORTED_VERSION,
  SCHEMA_VERSION_HISTORY,
  BREAKING_CHANGES,

  // Errors
  DoLakeError,
  ConnectionError,
  BufferOverflowError,
  FlushError,
  ParquetWriteError,
  IcebergError,
  VersionMismatchError,

  // Type guards
  isCDCBatchMessage,
  isAckMessage,
  isNackMessage,
  isConnectMessage,
  isHeartbeatMessage,
  isFlushRequestMessage,

  // Utilities
  generateBatchId,
  generateCorrelationId,
  generateSnapshotId,
  generateUUID,
} from './types.js';

// =============================================================================
// Rate Limiting
// =============================================================================

export {
  RateLimiter,
  type RateLimitConfig,
  type RateLimitResult,
  type RateLimitMetrics,
  type TokenBucket,
  type ConnectionState,
  type IpState,
  DEFAULT_RATE_LIMIT_CONFIG,
} from './rate-limiter.js';

// =============================================================================
// Buffer Management
// =============================================================================

export {
  CDCBufferManager,
  type SourceConnectionState,
  type PartitionBuffer,
  type BufferSnapshot,
  type DedupConfig,
  type DedupStats,
  DEFAULT_DEDUP_CONFIG,
} from './buffer.js';

// =============================================================================
// Parquet Writing
// =============================================================================

export {
  writeParquet,
  writePartitionToParquet,
  inferSchemaFromEvents,
  eventsToRows,
  createDataFile,
  type ParquetWriteConfig,
  type ParquetWriteResult,
  DEFAULT_PARQUET_CONFIG,
} from './parquet.js';

// =============================================================================
// Iceberg Metadata
// =============================================================================

export {
  // Storage Interface
  type IcebergStorage,
  type CommitRequirement,
  R2IcebergStorage,

  // Path utilities
  metadataFilePath,
  manifestListPath,
  manifestFilePath,
  dataFilePath,
  partitionToPath,

  // Schema builders
  createSchema,
  addSchemaField,

  // Partition spec builders
  createUnpartitionedSpec,
  createIdentityPartitionSpec,
  createDatePartitionSpec,

  // Sort order builders
  createUnsortedOrder,
  createSortOrder,

  // Snapshot builders
  createAppendSnapshot,
  createManifestFile,

  // Table metadata
  createTableMetadata,
  addSnapshot,
  addSchema,
} from './iceberg.js';

// =============================================================================
// REST Catalog API
// =============================================================================

export {
  RestCatalogHandler,
  createRestCatalog,
  type RestCatalogConfig,
  type CatalogConfig,
  type ListNamespacesResponse,
  type CreateNamespaceRequest,
  type CreateNamespaceResponse,
  type GetNamespaceResponse,
  type UpdateNamespacePropertiesRequest,
  type UpdateNamespacePropertiesResponse,
  type ListTablesResponse,
  type CreateTableRequest,
  type LoadTableResponse,
  type CommitTableRequest,
  type CommitTableResponse,
  type TableUpdate,
  type ErrorResponse,
} from './catalog.js';

// =============================================================================
// Compaction
// =============================================================================

export {
  CompactionManager,
  CompactionError,
  type CompactionConfig,
  type CompactionCandidate,
  type CompactionResult,
  type CompactionMetrics,
  type FileInfo,
  type SpaceSavings,
  type AtomicCommitPreparation,
  DEFAULT_COMPACTION_CONFIG,
} from './compaction.js';

// =============================================================================
// Zod Schemas for Message Validation
// =============================================================================

export {
  // CDC Event Schemas
  CDCEventSchema,
  CDCOperationSchema,
  type ValidatedCDCEvent,

  // Client Capabilities Schema
  ClientCapabilitiesSchema,
  type ValidatedClientCapabilities,

  // Message Schemas
  CDCBatchMessageSchema,
  ConnectMessageSchema,
  HeartbeatMessageSchema,
  FlushRequestMessageSchema,
  DisconnectMessageSchema,
  AckMessageSchema,
  NackMessageSchema,
  StatusMessageSchema,

  // Discriminated Unions
  ClientRpcMessageSchema,
  RpcMessageSchema,

  // Validated Types
  type ValidatedCDCBatchMessage,
  type ValidatedConnectMessage,
  type ValidatedHeartbeatMessage,
  type ValidatedFlushRequestMessage,
  type ValidatedDisconnectMessage,
  type ValidatedAckMessage,
  type ValidatedNackMessage,
  type ValidatedStatusMessage,
  type ValidatedClientRpcMessage,
  type ValidatedRpcMessage,

  // Validation Functions
  validateClientMessage,
  validateRpcMessage,
  isValidClientMessage,
  isValidRpcMessage,
  validateCDCBatchMessage,
  validateConnectMessage,
  validateHeartbeatMessage,
  validateFlushRequestMessage,

  // Error Class
  MessageValidationError,

  // Supporting Schemas
  AckStatusSchema,
  AckDetailsSchema,
  NackReasonSchema,
  FlushReasonSchema,
  DoLakeStateSchema,
  BufferStatsSchema,
} from './schemas.js';

// =============================================================================
// Partitioning
// =============================================================================

export {
  PartitionManager,
  computePartitionIdentifier,
  partitionKeyToString,
  createDayPartitionSpec,
  createHourPartitionSpec,
  createBucketPartitionSpec,
  createCompositePartitionSpec,
  prunePartitions,
  parseWhereClause,
  calculatePartitionStats,
  calculateBucketDistribution,
  yearTransform,
  monthTransform,
  dayTransform,
  hourTransform,
  bucketTransform,
  hashValue,
  type PartitionTransform,
  type PartitionValue,
  type PartitionKey,
  type PartitionIdentifier,
  type PartitionStats,
  type PartitionMetadata,
  type PartitionPredicate,
  type PartitionManagerConfig,
  type PartitionListResponse,
  DEFAULT_PARTITION_MANAGER_CONFIG,
} from './partitioning.js';

// =============================================================================
// Query Engine
// =============================================================================

export {
  QueryEngine,
  canSkipFileByStats,
  executeSelect,
  parseDateRange,
  generateDatePartitions,
  type QueryRequest,
  type QueryResult,
  type QueryPlanResult,
  type PartialAggregationResult,
  type QueryRoutingResult,
  type ColumnStats,
  type FileStats,
  type QueryEngineConfig,
  DEFAULT_QUERY_ENGINE_CONFIG,
} from './query-engine.js';

// =============================================================================
// Scalability
// =============================================================================

export {
  ParallelWriteManager,
  PartitionCompactionManager,
  PartitionRebalancer,
  LargeFileHandler,
  HorizontalScalingManager,
  MemoryEfficientProcessor,
  type ScalingConfig,
  type PartitionWriteConfig,
  type PartitionWriteResult,
  type ParallelWriteResult,
  type PartitionCompactionRequest,
  type PartitionCompactionResult,
  type AutoCompactionResult,
  type RebalanceAction,
  type RebalanceRecommendation,
  type PartitionAnalysis,
  type SplitExecutionResult,
  type LargeFileWriteRequest,
  type LargeFileWriteResult,
  type RangeReadRequest,
  type RangeReadResult,
  type MemoryStats,
  type ScalingStatus,
  type DORoutingResult,
  DEFAULT_SCALING_CONFIG,
} from './scalability.js';

// =============================================================================
// Analytics Events (P2 Durability)
// =============================================================================

export {
  AnalyticsEventBuffer,
  AnalyticsEventHandler,
  inferAnalyticsSchema,
  createDatePartition,
  type AnalyticsEvent,
  type AnalyticsEventBatch,
  type AnalyticsDurabilityConfig,
  type AnalyticsSchema,
  type AnalyticsSchemaField,
  type DatePartition,
  type PersistResult,
  type RecoveryResult,
  type BatchHandleResult,
  type AnalyticsMetrics,
  type BufferConfig,
  DEFAULT_ANALYTICS_CONFIG,
  P2_DURABILITY_CONFIG,
} from './analytics-events.js';

// =============================================================================
// Durability Tiers
// =============================================================================

export {
  // Types
  DurabilityTier,
  type WriteResult,
  type DurabilityConfig,
  type R2Storage,
  type KVStorage,
  type VFSStorage,
  DEFAULT_DURABILITY_CONFIG,

  // Classification
  classifyEvent,
  classifyEvents,
  isTableInTier,
  getTablesInTier,

  // WriteBuffer (Single-responsibility class for buffering writes)
  WriteBuffer,
  type PendingEvent,
  type WriteBufferConfig,
  DEFAULT_WRITE_BUFFER_CONFIG,

  // FlushStrategy (Single-responsibility class for flush timing and policies)
  FlushStrategy,
  type FlushStrategyConfig,
  type FlushDecision,
  type RetryContext,
  DEFAULT_FLUSH_STRATEGY_CONFIG,

  // PersistenceManager (Single-responsibility class for storage persistence)
  PersistenceManager,
  type PersistenceManagerConfig,
  type WriteOperationResult,
  type TierMetrics,
  type P0Metrics,
  type P1Metrics,
  type P2Metrics,
  type P3Metrics,
  type AllTierMetrics,
  DEFAULT_PERSISTENCE_MANAGER_CONFIG,

  // DurabilityWriter (Facade composing the above classes)
  DurabilityWriter,
  defaultDurabilityWriter,
} from './durability.js';

// =============================================================================
// KV Fallback Storage
// =============================================================================

export {
  KVFallbackStorage,
  type KVFallbackConfig,
  type ExtendedKVStorage,
  type FallbackWriteResult,
  type RecoveryResult as KVRecoveryResult,
  type BulkRecoveryResult,
  type StoredBatch,
  type PendingBatchInfo,
  type RecoveryMetrics as KVRecoveryMetrics,
  type ExpirationWarnings,
  type FallbackStatus,
  type StartupRecoveryResult,
  type AlarmHandlerResult,
  type ChunkInfo,
  DEFAULT_KV_FALLBACK_CONFIG,
} from './kv-fallback.js';

// =============================================================================
// VFS Fallback Storage
// =============================================================================

export {
  VFSFallbackStorage,
  VFSStorageError,
  type VFSFallbackConfig,
  type VFSStorageStats,
  type VFSWriteOptions,
  type VFSWriteResult,
  type VFSBatchWriteResult,
  type BufferRotationResult,
  type RecoveryResult as VFSRecoveryResult,
  type AlarmResult as VFSAlarmResult,
  type VFSBackend,
  DEFAULT_VFS_FALLBACK_CONFIG,
} from './vfs-fallback.js';

// =============================================================================
// Business Events (Stripe Webhooks with P0 Durability)
// =============================================================================

export {
  StripeWebhookHandler,
  verifyStripeSignature,
  normalizeStripeEventToCDC,
  type StripeEvent,
  type NormalizedCDCEvent,
  type StripeWebhookConfig,
  type StripeWebhookResult,
  type SignatureVerificationResult,
  type ExtendedKVStorage as BusinessKVStorage,
  DEFAULT_STRIPE_WEBHOOK_CONFIG,
} from './business-events.js';

// =============================================================================
// Tail Worker CDC Streaming
// =============================================================================

export {
  TailWorkerCDCStreamer,
  createTailWorkerCDCStreamer,
  type TraceItem,
  type TailWorkerBatch,
  type TraceCDCEvent,
  type TailWorkerConfig,
  type ShardBackpressureStatus,
  type TransformResult,
  type BatchSendResult,
  type TailWorkerLoadMetrics,
  type DurabilityTier as TailWorkerDurabilityTier,
  DEFAULT_TAIL_WORKER_CONFIG,
} from './tail-worker.js';

// =============================================================================
// Circuit Breaker
// =============================================================================

export {
  // Classes
  R2CircuitBreaker,
  CircuitBreakerManager,

  // Factory functions
  createCircuitBreaker,
  createCircuitBreakerManager,

  // Types
  CircuitState,
  type CircuitBreakerConfig,
  type CircuitMetrics,
  type CircuitProtectedResult,

  // Constants
  DEFAULT_CIRCUIT_BREAKER_CONFIG,
} from './circuit-breaker.js';

// =============================================================================
// Serialization (BigInt Support)
// =============================================================================

export {
  // Core serialization functions
  serialize,
  deserialize,
  serializeMessage,
  deserializeMessage,

  // Replacer/reviver functions
  bigintReplacer,
  bigintReviver,
  createReplacer,
  createReviver,

  // Iceberg-specific serialization
  serializeIcebergMetadata,
  deserializeIcebergMetadata,

  // Branded types
  type SnapshotId,
  type SequenceNumber,
  type TimestampMs,
  snapshotId,
  sequenceNumber,
  timestampMs,

  // Type guards and validators
  isBigInt,
  isSerializedBigInt,
  isNumericBigIntString,
  isIcebergBigIntField,
  isValidSnapshotId,
  isValidSequenceNumber,
  isValidTimestamp,

  // Parsers
  parseSnapshotId,
  parseSequenceNumber,

  // Constants
  BIGINT_MARKER,
  ICEBERG_BIGINT_FIELDS,

  // Options type
  type IcebergSerializationOptions,
} from './serialization.js';

// =============================================================================
// WebSocket Handler (Modular)
// =============================================================================

export {
  WebSocketHandler,
  restoreFromHibernation,
  setupWebSocketAutoResponse,
  type ExtendedWebSocketAttachment,
  type FlushTriggerCallback,
  type FlushCallback,
  type WebSocketHandlerConfig,
  type WebSocketHandlerDeps,
} from './websocket-handler.js';

// =============================================================================
// State Machine (Modular)
// =============================================================================

export {
  DoLakeStateMachine,
  createStatePersistence,
  handleAlarm,
  scheduleAlarm,
  createRecoveryHandler,
  type StateTransition,
  type StateChangeEvent,
  type StatePersistence,
  type ScheduledCompaction,
  type StateMachineConfig,
  type AlarmHandlerConfig,
  type AlarmHandlerDeps,
  type RecoveryResult as StateMachineRecoveryResult,
  DEFAULT_STATE_MACHINE_CONFIG,
} from './state-machine.js';

// =============================================================================
// Catalog Router (Modular)
// =============================================================================

export {
  CatalogRouter,
  type CatalogRouterDeps,
  type CatalogRouterConfig,
} from './catalog-router.js';

// =============================================================================
// Flush Manager (Modular)
// =============================================================================

export {
  FlushManager,
  recoverFromFallback,
  estimateFlushTime,
  determineOptimalFlushTrigger,
  groupEventsByTable,
  groupEventsByPartition,
  type PartitionBuffer,
  type TableFlushResult,
  type FlushManagerDeps,
  type FlushManagerConfig,
} from './flush-manager.js';

// =============================================================================
// Cache Invalidation
// =============================================================================

export {
  CacheInvalidator,
  type CacheInvalidationConfig,
  type CacheInvalidationResult,
  type CacheEntryStatus,
  type CacheMetrics,
  type PartitionCacheStatus,
  type ReplicaStatus,
  type ReplicaConfig,
  type TableTTLConfig,
  DEFAULT_CACHE_INVALIDATION_CONFIG,
} from './cache-invalidation.js';
