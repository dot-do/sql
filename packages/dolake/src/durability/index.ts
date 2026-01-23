/**
 * Durability Module
 *
 * This module provides durability tier-based event persistence with
 * support for different storage backends and failure handling strategies.
 *
 * The module is organized into single-responsibility classes:
 * - WriteBuffer: Handles buffering of writes
 * - FlushStrategy: Handles flush timing and policies
 * - PersistenceManager: Handles actual persistence to storage
 * - DurabilityWriter: Facade that composes the above
 *
 * Classification functions are also provided for categorizing events
 * into the appropriate durability tier.
 */

// =============================================================================
// Types
// =============================================================================

export {
  DurabilityTier,
  type WriteResult,
  type DurabilityConfig,
  type R2Storage,
  type KVStorage,
  type VFSStorage,
  DEFAULT_DURABILITY_CONFIG,
} from './types.js';

// =============================================================================
// Classification
// =============================================================================

export {
  classifyEvent,
  classifyEvents,
  isTableInTier,
  getTablesInTier,
} from './classification.js';

// =============================================================================
// WriteBuffer
// =============================================================================

export {
  WriteBuffer,
  type PendingEvent,
  type WriteBufferConfig,
  DEFAULT_WRITE_BUFFER_CONFIG,
} from './write-buffer.js';

// =============================================================================
// FlushStrategy
// =============================================================================

export {
  FlushStrategy,
  type FlushStrategyConfig,
  type FlushDecision,
  type RetryContext,
  DEFAULT_FLUSH_STRATEGY_CONFIG,
} from './flush-strategy.js';

// =============================================================================
// PersistenceManager
// =============================================================================

export {
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
} from './persistence-manager.js';

// =============================================================================
// DurabilityWriter (Facade)
// =============================================================================

export {
  DurabilityWriter,
  defaultDurabilityWriter,
} from './durability-writer.js';
