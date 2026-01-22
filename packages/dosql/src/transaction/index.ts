/**
 * Transaction Module for DoSQL
 *
 * Provides SQLite-compatible transaction semantics:
 * - BEGIN/COMMIT/ROLLBACK
 * - Savepoints for nested transactions
 * - Multiple isolation levels
 * - MVCC for snapshot isolation
 * - Lock management
 *
 * @example
 * ```typescript
 * import {
 *   createTransactionManager,
 *   executeInTransaction,
 *   IsolationLevel
 * } from '@dotdo/dosql/transaction';
 *
 * const manager = createTransactionManager({ walWriter });
 *
 * // Simple transaction
 * await manager.begin();
 * manager.logOperation({ op: 'INSERT', table: 'users', afterValue: data });
 * await manager.commit();
 *
 * // Using wrapper
 * const result = await executeInTransaction(manager, async (ctx) => {
 *   // Operations here
 *   return { success: true };
 * });
 * ```
 *
 * @packageDocumentation
 */

// Types
export {
  // State enums
  TransactionState,
  TransactionMode,
  IsolationLevel,
  LockType,

  // Error types
  TransactionError,
  TransactionErrorCode,

  // Core types
  type Savepoint,
  type SavepointStack,
  type TransactionLog,
  type TransactionLogEntry,
  type TransactionLogOperation,
  type TransactionContext,
  type TransactionOptions,
  type TransactionManager,
  type ApplyFunction,

  // MVCC types
  type Snapshot,
  type RowVersion,

  // Lock types
  type LockRequest,
  type LockResult,
  type HeldLock,

  // Stats
  type TransactionStats,

  // Timeout types
  type TransactionTimeoutConfig,
  type DurableObjectState,
  type DurableObjectStorage,
  type LongRunningTransactionLog,
  type WarningLevel,
  DEFAULT_TIMEOUT_CONFIG,

  // Factory functions
  createTransactionLog,
  createSavepointStack,
} from './types.js';

// Manager
export {
  createTransactionManager,
  executeInTransaction,
  executeWithSavepoint,
  executeReadOnly,
  createAutoCommitManager,
  type TransactionManagerOptions,
  type TransactionResult,
  type ExtendedTransactionManager,
} from './manager.js';

// Isolation
export {
  createLockManager,
  createMVCCStore,
  createIsolationEnforcer,
  createSnapshot,
  isVersionVisible,
  type LockManager,
  type LockManagerOptions,
  type MVCCStore,
  type IsolationEnforcer,
  type IsolationEnforcerOptions,
} from './isolation.js';

// Timeout enforcement
export {
  createTimeoutEnforcer,
  checkTransactionTimeout,
  type TransactionTimeoutEnforcer,
  type TimeoutEnforcerOptions,
} from './timeout.js';
