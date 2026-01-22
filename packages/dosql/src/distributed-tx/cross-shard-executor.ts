/**
 * DoSQL Cross-Shard Executor
 *
 * Provides a high-level interface for executing distributed transactions
 * across multiple shards with automatic 2PC handling.
 *
 * @packageDocumentation
 */

import type { ShardId } from '../sharding/types.js';
import { IsolationLevel } from '../transaction/types.js';
import type { DistributedTransactionCoordinator } from './coordinator.js';
import type { CrossShardExecutionResult } from './types.js';

// =============================================================================
// CROSS-SHARD EXECUTOR INTERFACE
// =============================================================================

/**
 * Cross-shard executor interface
 */
export interface CrossShardExecutor {
  /** Execute operations atomically across shards */
  executeInTransaction<T>(
    shards: ShardId[],
    operations: Array<{ shard: ShardId; sql: string; params?: unknown[] }>,
    options?: { timeout?: number; isolationLevel?: string }
  ): Promise<CrossShardExecutionResult<T>>;
}

// =============================================================================
// CROSS-SHARD EXECUTOR IMPLEMENTATION
// =============================================================================

/**
 * Creates a cross-shard executor
 */
export function createCrossShardExecutor(
  coordinator: DistributedTransactionCoordinator
): CrossShardExecutor {
  return {
    async executeInTransaction<T>(
      shards: ShardId[],
      operations: Array<{ shard: ShardId; sql: string; params?: unknown[] }>,
      options?: { timeout?: number; isolationLevel?: string }
    ): Promise<CrossShardExecutionResult<T>> {
      const results: T[] = [];

      try {
        // Determine isolation level
        let isolationLevel = IsolationLevel.SERIALIZABLE;
        if (options?.isolationLevel) {
          const level = options.isolationLevel.toUpperCase();
          if (level === 'SERIALIZABLE') isolationLevel = IsolationLevel.SERIALIZABLE;
          else if (level === 'SNAPSHOT') isolationLevel = IsolationLevel.SNAPSHOT;
          else if (level === 'REPEATABLE_READ') isolationLevel = IsolationLevel.REPEATABLE_READ;
          else if (level === 'READ_COMMITTED') isolationLevel = IsolationLevel.READ_COMMITTED;
        }

        // Begin distributed transaction
        await coordinator.begin(shards, {
          ...(options?.timeout !== undefined && { timeout: options.timeout }),
          isolationLevel,
        });

        // Execute all operations
        for (const op of operations) {
          const result = await coordinator.execute(op.shard, op.sql, op.params);
          results.push(result as T);
        }

        // Prepare phase
        const votes = await coordinator.prepare();

        // Check if all voted YES
        let allYes = true;
        for (const vote of votes.values()) {
          if (vote !== 'YES') {
            allYes = false;
            break;
          }
        }

        if (!allYes) {
          // Rollback
          await coordinator.rollback();
          return {
            success: false,
            results: [],
            error: new Error('Transaction aborted: one or more participants voted NO'),
          };
        }

        // Commit phase
        await coordinator.commit();

        return {
          success: true,
          results,
        };
      } catch (error) {
        // Try to rollback
        try {
          await coordinator.rollback();
        } catch {
          // Ignore rollback errors
        }

        return {
          success: false,
          results: [],
          error: error instanceof Error ? error : new Error(String(error)),
        };
      }
    },
  };
}
