/**
 * WAL Retention Manager for DoSQL
 *
 * This file re-exports all retention functionality from the modular implementation
 * in the retention/ directory for backward compatibility.
 *
 * The implementation has been split into smaller, focused modules:
 * - retention/policy.ts - Retention policy definitions and parsing
 * - retention/scheduler.ts - Retention job scheduling
 * - retention/executor.ts - Retention job execution
 * - retention/metrics.ts - Retention metrics and monitoring
 * - retention/index.ts - Main exports and facade
 *
 * For new code, you can import directly from 'dosql/wal/retention':
 * ```typescript
 * import { createWALRetentionManager } from 'dosql/wal/retention';
 * ```
 *
 * Or from this file for backward compatibility:
 * ```typescript
 * import { createWALRetentionManager } from 'dosql/wal/retention.js';
 * ```
 *
 * @packageDocumentation
 */

// Re-export everything from the modular implementation
export * from './retention/index.js';

// Also export the CheckpointManagerForRetention interface for convenience
export type { CheckpointManagerForRetention } from './retention/executor.js';
