/**
 * DoLake Durability Tiers
 *
 * Implements durability classification and write behavior for events.
 * Different events need different durability guarantees:
 *
 * P0 Critical (Stripe webhooks, payments)
 * - Always write to R2 AND KV
 * - Retry until confirmed
 * - Dead letter queue on permanent failure
 *
 * P1 Important (user actions, signups)
 * - Write to R2, fallback to KV on failure
 * - Retry 3x with exponential backoff
 *
 * P2 Standard (analytics, telemetry)
 * - Write to R2 only
 * - Fallback to DO fsx VFS on R2 failure
 * - Retry from VFS on next flush
 *
 * P3 Best-effort (anonymous visits)
 * - Write to R2
 * - Drop on failure (acceptable loss)
 *
 * REFACTORED: This module has been split into smaller single-responsibility classes:
 * - WriteBuffer: Handles buffering of writes
 * - FlushStrategy: Handles flush timing and policies
 * - PersistenceManager: Handles actual persistence to storage
 * - DurabilityWriter: Facade that composes the above
 *
 * This file re-exports all components for backward compatibility.
 */

// =============================================================================
// Re-export all types and classes from the durability module
// =============================================================================

// Types
export {
  DurabilityTier,
  type WriteResult,
  type DurabilityConfig,
  type R2Storage,
  type KVStorage,
  type VFSStorage,
  DEFAULT_DURABILITY_CONFIG,
} from './durability/types.js';

// Classification
export {
  classifyEvent,
  classifyEvents,
  isTableInTier,
  getTablesInTier,
} from './durability/classification.js';

// WriteBuffer
export {
  WriteBuffer,
  type PendingEvent,
  type WriteBufferConfig,
  DEFAULT_WRITE_BUFFER_CONFIG,
} from './durability/write-buffer.js';

// FlushStrategy
export {
  FlushStrategy,
  type FlushStrategyConfig,
  type FlushDecision,
  type RetryContext,
  DEFAULT_FLUSH_STRATEGY_CONFIG,
} from './durability/flush-strategy.js';

// PersistenceManager
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
} from './durability/persistence-manager.js';

// DurabilityWriter (Facade)
export {
  DurabilityWriter,
  defaultDurabilityWriter,
} from './durability/durability-writer.js';
