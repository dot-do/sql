/**
 * DoLake Module
 *
 * Provides DO-resident caching and coherence for Iceberg lakehouse metadata.
 *
 * @module dolake
 */

export {
  // Main classes
  MetadataCache,
  CacheCoherenceManager,

  // Helper functions
  processSchemaChangeEvent,
  processPartitionSpecChange,
  processTableDrop,
  estimateMemoryUsage,
  createUnifiedCacheManager,

  // Types
  type IcebergMetadata,
  type MetadataCacheConfig,
  type MetadataCacheEntry,
  type CacheStats,
  type LatencyStats,
  type SchemaChangeEvent,
  type PartitionSpecChange,
  type StorageInterface,
  type CacheOptions,
  type CoherenceManagerConfig,
  type CoherenceMessage,
  type Conflict,
  type CoherenceHealth,
  type UnifiedCacheManagerConfig,
  type InvalidationStrategy,
} from './metadata-cache.js';

export {
  CacheInvalidator,
  type CacheInvalidatorConfig,
  type CDCEvent,
  type CacheEntryStatus,
} from './cache-invalidation.js';
