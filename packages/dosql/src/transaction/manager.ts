/**
 * Transaction Manager for DoSQL
 *
 * Implements SQLite-compatible transaction semantics with:
 * - BEGIN/COMMIT/ROLLBACK
 * - Savepoints for nested transactions
 * - Integration with WAL for durability
 * - Lock management for isolation
 *
 * @packageDocumentation
 */

import type { WALWriter, LSN, TransactionId } from '../wal/types.js';
import { createLSN, createTransactionId } from '../wal/types.js';
import { generateTxnId } from '../wal/writer.js';
import { createLogger, type StructuredLogger, type LogSink, NoOpSink } from '../logging/index.js';
import {
  type TransactionManager,
  type TransactionContext,
  type TransactionOptions,
  type TransactionLog,
  type TransactionLogEntry,
  type Savepoint,
  type SavepointStack,
  type ApplyFunction,
  type HeldLock,
  type TransactionTimeoutConfig,
  type DurableObjectState,
  type LongRunningTransactionLog,
  TransactionState,
  TransactionMode,
  IsolationLevel,
  LockType,
  TransactionError,
  TransactionErrorCode,
  createTransactionLog,
  createSavepointStack,
  DEFAULT_TIMEOUT_CONFIG,
} from './types.js';
import {
  createTimeoutEnforcer,
  checkTransactionTimeout,
  type TransactionTimeoutEnforcer,
} from './timeout.js';
import type { LockManager } from './isolation.js';

// =============================================================================
// Module-level Logger for Helper Functions
// =============================================================================

/**
 * Module-level logger for helper functions (executeInTransaction, executeWithSavepoint)
 * that don't have access to the transaction manager's logger instance.
 */
const moduleLogger = createLogger({
  defaultContext: { component: 'TransactionHelpers' },
});

// =============================================================================
// Transaction Manager Implementation
// =============================================================================

/**
 * Options for creating the transaction manager
 */
export interface TransactionManagerOptions {
  /** WAL writer for durability */
  walWriter?: WALWriter;
  /** Default isolation level */
  defaultIsolationLevel?: IsolationLevel;
  /** Default lock timeout in milliseconds */
  defaultLockTimeout?: number;
  /** Enable MVCC for snapshot isolation */
  enableMVCC?: boolean;
  /** Durable Object state for alarm-based timeout enforcement */
  doState?: DurableObjectState;
  /** Timeout configuration */
  timeoutConfig?: Partial<TransactionTimeoutConfig>;
  /** Maximum number of timeout extensions allowed */
  maxExtensions?: number;
  /** Callback for timeout warnings */
  onTimeoutWarning?: (txnId: TransactionId, remainingMs: number) => void;
  /** Callback for transaction timeout */
  onTimeout?: (txnId: TransactionId) => void;
  /** Callback for long-running transaction logs */
  onLongRunningTransaction?: (log: LongRunningTransactionLog) => void;
  /** Whether to track queries for logging */
  trackQueries?: boolean;
  /** I/O operation timeout in milliseconds */
  ioTimeoutMs?: number;
  /** Lock manager for isolation */
  lockManager?: LockManager;
  /** Structured logger instance */
  logger?: StructuredLogger;
  /** Custom log sink for structured logging */
  logSink?: LogSink;
}

/**
 * Creates a new transaction manager
 *
 * @example
 * ```typescript
 * const manager = createTransactionManager({
 *   walWriter: walWriter,
 *   defaultIsolationLevel: IsolationLevel.SERIALIZABLE
 * });
 *
 * // Begin transaction
 * await manager.begin();
 *
 * // Perform operations...
 * manager.logOperation({ op: 'INSERT', table: 'users', afterValue: data });
 *
 * // Commit
 * await manager.commit();
 * ```
 */
/**
 * Extended transaction manager interface with timeout support
 */
export interface ExtendedTransactionManager extends TransactionManager {
  /** Handle DO alarm for timeout enforcement */
  handleAlarm(): Promise<void>;
  /** Check if DO alarm support is enabled */
  hasAlarmSupport(): boolean;
  /** Get timeout configuration */
  getTimeoutConfig(): TransactionTimeoutConfig;
  /** Request timeout extension for current transaction */
  requestExtension(txnId: TransactionId, additionalMs: number): boolean;
  /** Get remaining time for current transaction */
  getRemainingTime(txnId: TransactionId): number;
  /** Get I/O timeout in milliseconds */
  getIoTimeoutMs(): number;
  /** Execute operation with I/O timeout */
  executeWithIoTimeout<T>(operation: () => Promise<T>): Promise<T>;
}

export function createTransactionManager(
  options: TransactionManagerOptions = {}
): ExtendedTransactionManager {
  const {
    walWriter,
    defaultIsolationLevel = IsolationLevel.SERIALIZABLE,
    defaultLockTimeout = 5000,
    enableMVCC = false,
    doState,
    timeoutConfig,
    maxExtensions,
    onTimeoutWarning,
    onTimeout,
    onLongRunningTransaction,
    trackQueries,
    ioTimeoutMs,
    lockManager,
    logger: providedLogger,
    logSink,
  } = options;

  // Create structured logger for transaction manager
  const logger: StructuredLogger = providedLogger ?? createLogger({
    sink: logSink,
    defaultContext: { component: 'TransactionManager' },
  });

  // Current transaction state
  let currentContext: TransactionContext | null = null;
  let applyFn: ApplyFunction | null = null;

  // Active transactions tracking (for MVCC)
  const activeTransactions = new Set<TransactionId>();

  // Merge timeout config with defaults
  const effectiveTimeoutConfig: TransactionTimeoutConfig = {
    ...DEFAULT_TIMEOUT_CONFIG,
    ...timeoutConfig,
  };

  // Create timeout enforcer
  const timeoutEnforcer = createTimeoutEnforcer({
    config: effectiveTimeoutConfig,
    doState,
    maxExtensions,
    onTimeoutWarning,
    onTimeout,
    onLongRunningTransaction,
    trackQueries,
    ioTimeoutMs,
  });

  // Set rollback function for timeout enforcer
  timeoutEnforcer.setRollbackFn(async (txnId: TransactionId) => {
    if (currentContext && currentContext.txnId === txnId) {
      await executeRollback(currentContext, { suppressErrors: true });
    }
  });

  /**
   * Options for executeRollback
   */
  interface ExecuteRollbackOptions {
    /**
     * Savepoint name to rollback to. If provided, only changes after this
     * savepoint will be reverted. If not provided, the entire transaction
     * is rolled back.
     */
    toSavepoint?: string;

    /**
     * If true, skip writing ROLLBACK to WAL. Used for savepoint rollback
     * which doesn't require WAL persistence.
     */
    skipWAL?: boolean;

    /**
     * If true, errors during undo operations will be logged but not thrown.
     * Used for timeout-triggered rollback where we must ensure cleanup happens.
     */
    suppressErrors?: boolean;

    /**
     * If true, skip clearing the transaction context (used for savepoint rollback
     * where the transaction remains active).
     */
    keepTransaction?: boolean;
  }

  /**
   * Unified rollback implementation
   *
   * This method handles all rollback scenarios:
   * - Full transaction rollback (public rollback() method)
   * - Rollback to savepoint (rollbackTo() method)
   * - Timeout-triggered rollback (performRollback helper)
   *
   * The method ensures consistent behavior across all paths:
   * 1. Undo operations are applied in reverse order
   * 2. Locks are always released (even if undo fails)
   * 3. WAL is updated (unless skipWAL is set)
   * 4. Transaction state is cleaned up
   *
   * @param context - The transaction context to rollback
   * @param options - Rollback options
   * @throws TransactionError if rollback fails and suppressErrors is false
   */
  async function executeRollback(
    context: TransactionContext,
    options: ExecuteRollbackOptions = {}
  ): Promise<void> {
    const {
      toSavepoint,
      skipWAL = false,
      suppressErrors = false,
      keepTransaction = false,
    } = options;

    let undoError: Error | null = null;
    let startIndex = 0;
    let endIndex = context.log.entries.length;
    let savepointFound: { savepoint: Savepoint; index: number } | null = null;

    // If rolling back to a savepoint, find it and adjust indices
    if (toSavepoint) {
      savepointFound = findSavepoint(context.savepoints, toSavepoint);
      if (!savepointFound) {
        throw new TransactionError(
          TransactionErrorCode.SAVEPOINT_NOT_FOUND,
          `Savepoint '${toSavepoint}' not found`,
          { txnId: context.txnId }
        );
      }
      startIndex = savepointFound.savepoint.logSnapshotIndex;
    }

    // Step 1: Apply undo operations (in reverse order)
    const hasUndoableEntries = context.log.entries
      .slice(startIndex, endIndex)
      .some((e) => e.op === 'INSERT' || e.op === 'UPDATE' || e.op === 'DELETE');

    if (hasUndoableEntries && applyFn) {
      try {
        await applyRollback(context.log, startIndex, endIndex);
      } catch (error) {
        undoError = error instanceof Error ? error : new Error(String(error));
        // Log rollback failure with structured context
        logger.error(
          'Failed to apply rollback operations',
          undoError,
          {
            txnId: context.txnId,
            operation: 'ROLLBACK',
            startIndex,
            endIndex,
            toSavepoint,
            suppressErrors,
          }
        );
      }
    } else if (hasUndoableEntries && !applyFn && !suppressErrors) {
      undoError = new TransactionError(
        TransactionErrorCode.ROLLBACK_FAILED,
        'No apply function set for rollback operations',
        { txnId: context.txnId }
      );
    }

    // Step 2: Always release locks (even if undo failed)
    // This is critical for preventing lock leaks
    if (lockManager && !keepTransaction) {
      lockManager.releaseAll(context.txnId);
    }

    // Step 3: Write to WAL (unless skipWAL is set)
    if (!skipWAL) {
      try {
        await writeToWAL('ROLLBACK', context.txnId);
      } catch (error) {
        // Log but don't fail - rollback already applied in memory
        logger.error(
          'Failed to write ROLLBACK to WAL',
          error instanceof Error ? error : new Error(String(error)),
          {
            txnId: context.txnId,
            operation: 'WAL_WRITE',
            walOp: 'ROLLBACK',
            toSavepoint,
          }
        );
      }
    }

    // Step 4: Update transaction state
    if (toSavepoint && savepointFound) {
      // For savepoint rollback: truncate log and remove nested savepoints
      context.log.entries = context.log.entries.slice(0, startIndex);
      context.log.currentSequence = startIndex;

      // Remove savepoints after this one (but keep this one)
      context.savepoints.savepoints = context.savepoints.savepoints.slice(
        0,
        savepointFound.index + 1
      );
    }

    // Step 5: If undo failed, keep transaction active (data not rolled back)
    if (undoError && !suppressErrors) {
      throw new TransactionError(
        TransactionErrorCode.ROLLBACK_FAILED,
        toSavepoint
          ? `Failed to rollback to savepoint '${toSavepoint}'`
          : 'Failed to apply rollback operations',
        { txnId: context.txnId, cause: undoError }
      );
    }

    // Step 6: Clean up transaction context (unless keepTransaction)
    if (!keepTransaction) {
      activeTransactions.delete(context.txnId);
      timeoutEnforcer.unregisterTransaction(context.txnId);
      if (currentContext === context) {
        currentContext = null;
      }
    }
  }

  /**
   * Create a new transaction context
   */
  function createContext(txnOptions: TransactionOptions): TransactionContext {
    const txnId: TransactionId = txnOptions.txnId ?? createTransactionId(generateTxnId());
    const now = Date.now();

    return {
      txnId,
      state: TransactionState.ACTIVE,
      mode: txnOptions.mode ?? TransactionMode.DEFERRED,
      isolationLevel: txnOptions.isolationLevel ?? defaultIsolationLevel,
      savepoints: createSavepointStack(),
      log: createTransactionLog(txnId),
      locks: [],
      startedAt: now,
      readOnly: txnOptions.readOnly ?? false,
      autoCommit: false,
      snapshot:
        txnOptions.isolationLevel === IsolationLevel.SNAPSHOT || enableMVCC
          ? {
              txnId,
              snapshotLsn: walWriter?.getCurrentLSN() ?? createLSN(0n),
              timestamp: now,
              activeTransactions: new Set(activeTransactions),
            }
          : undefined,
    };
  }

  /**
   * Write transaction control entry to WAL
   */
  async function writeToWAL(
    op: 'BEGIN' | 'COMMIT' | 'ROLLBACK',
    txnId: TransactionId
  ): Promise<LSN> {
    if (!walWriter) {
      return createLSN(0n);
    }

    const result = await walWriter.append(
      {
        timestamp: Date.now(),
        txnId,
        op,
        table: '',
      },
      { sync: op === 'COMMIT' || op === 'ROLLBACK' }
    );

    return result.lsn;
  }

  /**
   * Apply rollback for entries from endIndex (exclusive) to startIndex (inclusive)
   */
  async function applyRollback(
    log: TransactionLog,
    startIndex: number,
    endIndex: number
  ): Promise<void> {
    if (!applyFn) {
      throw new TransactionError(
        TransactionErrorCode.ROLLBACK_FAILED,
        'No apply function set for rollback operations',
        log.txnId
      );
    }

    // Apply in reverse order for proper undo
    for (let i = endIndex - 1; i >= startIndex; i--) {
      const entry = log.entries[i];

      // Skip savepoint-related entries
      if (
        entry.op === 'SAVEPOINT' ||
        entry.op === 'RELEASE' ||
        entry.op === 'ROLLBACK_TO'
      ) {
        continue;
      }

      // Determine undo operation
      switch (entry.op) {
        case 'INSERT':
          // Undo INSERT by DELETE
          await applyFn('DELETE', entry.table, entry.key, entry.beforeValue);
          break;
        case 'UPDATE':
          // Undo UPDATE by restoring previous value
          await applyFn('UPDATE', entry.table, entry.key, entry.beforeValue);
          break;
        case 'DELETE':
          // Undo DELETE by INSERT
          await applyFn('INSERT', entry.table, entry.key, entry.beforeValue);
          break;
      }
    }
  }

  /**
   * Find savepoint by name
   */
  function findSavepoint(
    stack: SavepointStack,
    name: string
  ): { savepoint: Savepoint; index: number } | null {
    const index = stack.savepoints.findIndex((sp) => sp.name === name);
    if (index === -1) {
      return null;
    }
    return { savepoint: stack.savepoints[index], index };
  }

  // Public interface
  const manager: ExtendedTransactionManager = {
    async begin(options: TransactionOptions = {}): Promise<TransactionContext> {
      if (currentContext !== null) {
        throw new TransactionError(
          TransactionErrorCode.TRANSACTION_ALREADY_ACTIVE,
          `Transaction ${currentContext.txnId} is already active`,
          currentContext.txnId
        );
      }

      const context = createContext(options);
      currentContext = context;
      activeTransactions.add(context.txnId);

      // Write BEGIN to WAL
      try {
        await writeToWAL('BEGIN', context.txnId);
      } catch (error) {
        currentContext = null;
        activeTransactions.delete(context.txnId);
        throw new TransactionError(
          TransactionErrorCode.WAL_FAILURE,
          'Failed to write BEGIN to WAL',
          context.txnId,
          error instanceof Error ? error : undefined
        );
      }

      // Register with timeout enforcer
      const timeoutMs = options.timeoutMs ?? effectiveTimeoutConfig.defaultTimeoutMs;
      timeoutEnforcer.registerTransaction(context.txnId, timeoutMs, context);

      // Set held locks getter for logging
      if (lockManager) {
        timeoutEnforcer.setHeldLocksGetter(context.txnId, () =>
          lockManager.getHeldLocks(context.txnId)
        );
      }

      return context;
    },

    async commit(): Promise<LSN> {
      if (!currentContext) {
        throw new TransactionError(
          TransactionErrorCode.NO_ACTIVE_TRANSACTION,
          'No active transaction to commit'
        );
      }

      const context = currentContext;

      // Check for timeout before commit
      checkTransactionTimeout(context, timeoutEnforcer);

      // Write COMMIT to WAL
      let commitLsn: LSN;
      try {
        commitLsn = await writeToWAL('COMMIT', context.txnId);
      } catch (error) {
        throw new TransactionError(
          TransactionErrorCode.WAL_FAILURE,
          'Failed to write COMMIT to WAL',
          context.txnId,
          error instanceof Error ? error : undefined
        );
      }

      // Release locks if lock manager is available
      if (lockManager) {
        lockManager.releaseAll(context.txnId);
      }

      // Unregister from timeout enforcer
      timeoutEnforcer.unregisterTransaction(context.txnId);

      // Clear state
      activeTransactions.delete(context.txnId);
      currentContext = null;

      return commitLsn;
    },

    async rollback(): Promise<void> {
      if (!currentContext) {
        throw new TransactionError(
          TransactionErrorCode.NO_ACTIVE_TRANSACTION,
          'No active transaction to rollback'
        );
      }

      await executeRollback(currentContext);
    },

    async savepoint(name: string): Promise<Savepoint> {
      if (!currentContext) {
        throw new TransactionError(
          TransactionErrorCode.NO_ACTIVE_TRANSACTION,
          'No active transaction for savepoint'
        );
      }

      const context = currentContext;

      // Check for duplicate savepoint name
      if (findSavepoint(context.savepoints, name)) {
        throw new TransactionError(
          TransactionErrorCode.DUPLICATE_SAVEPOINT,
          `Savepoint '${name}' already exists`,
          context.txnId
        );
      }

      const savepoint: Savepoint = {
        name,
        lsn: walWriter?.getCurrentLSN() ?? createLSN(0n),
        timestamp: Date.now(),
        logSnapshotIndex: context.log.entries.length,
        depth: context.savepoints.savepoints.length,
      };

      // Push to stack
      context.savepoints.savepoints.push(savepoint);
      context.savepoints.maxDepth = Math.max(
        context.savepoints.maxDepth,
        savepoint.depth + 1
      );

      // Update state if first savepoint
      if (context.state === TransactionState.ACTIVE) {
        context.state = TransactionState.SAVEPOINT;
      }

      // Log the savepoint operation
      manager.logOperation({
        op: 'SAVEPOINT',
        table: '',
        savepointName: name,
      });

      // Update logSnapshotIndex to include the SAVEPOINT log entry itself
      // so rollbackTo preserves the SAVEPOINT entry in the log
      savepoint.logSnapshotIndex = context.log.entries.length;

      return savepoint;
    },

    async release(name: string): Promise<void> {
      if (!currentContext) {
        throw new TransactionError(
          TransactionErrorCode.NO_ACTIVE_TRANSACTION,
          'No active transaction for release'
        );
      }

      const context = currentContext;
      const found = findSavepoint(context.savepoints, name);

      if (!found) {
        throw new TransactionError(
          TransactionErrorCode.SAVEPOINT_NOT_FOUND,
          `Savepoint '${name}' not found`,
          context.txnId
        );
      }

      // Remove this savepoint and all nested ones
      context.savepoints.savepoints = context.savepoints.savepoints.slice(
        0,
        found.index
      );

      // Update state if no more savepoints
      if (context.savepoints.savepoints.length === 0) {
        context.state = TransactionState.ACTIVE;
      }

      // Log the release operation
      manager.logOperation({
        op: 'RELEASE',
        table: '',
        savepointName: name,
      });
    },

    async rollbackTo(name: string): Promise<void> {
      if (!currentContext) {
        throw new TransactionError(
          TransactionErrorCode.NO_ACTIVE_TRANSACTION,
          'No active transaction for rollback to savepoint'
        );
      }

      const context = currentContext;
      const found = findSavepoint(context.savepoints, name);

      if (!found) {
        throw new TransactionError(
          TransactionErrorCode.SAVEPOINT_NOT_FOUND,
          `Savepoint '${name}' not found`,
          context.txnId
        );
      }

      // Apply rollback for operations after the savepoint
      if (applyFn && context.log.entries.length > found.savepoint.logSnapshotIndex) {
        try {
          await applyRollback(
            context.log,
            found.savepoint.logSnapshotIndex,
            context.log.entries.length
          );
        } catch (error) {
          throw new TransactionError(
            TransactionErrorCode.ROLLBACK_FAILED,
            `Failed to rollback to savepoint '${name}'`,
            context.txnId,
            error instanceof Error ? error : undefined
          );
        }
      }

      // Truncate log to savepoint
      context.log.entries = context.log.entries.slice(
        0,
        found.savepoint.logSnapshotIndex
      );
      context.log.currentSequence = found.savepoint.logSnapshotIndex;

      // Remove savepoints after this one (but keep this one)
      context.savepoints.savepoints = context.savepoints.savepoints.slice(
        0,
        found.index + 1
      );

      // Log the rollback_to operation
      manager.logOperation({
        op: 'ROLLBACK_TO',
        table: '',
        savepointName: name,
      });
    },

    getContext(): TransactionContext | null {
      return currentContext;
    },

    isActive(): boolean {
      return currentContext !== null;
    },

    getState(): TransactionState {
      return currentContext?.state ?? TransactionState.NONE;
    },

    logOperation(
      entry: Omit<TransactionLogEntry, 'sequence' | 'timestamp'>
    ): void {
      if (!currentContext) {
        throw new TransactionError(
          TransactionErrorCode.NO_ACTIVE_TRANSACTION,
          'No active transaction for logging operation'
        );
      }

      // Check for timeout before operation
      checkTransactionTimeout(currentContext, timeoutEnforcer);

      if (
        currentContext.readOnly &&
        (entry.op === 'INSERT' || entry.op === 'UPDATE' || entry.op === 'DELETE')
      ) {
        throw new TransactionError(
          TransactionErrorCode.READ_ONLY_VIOLATION,
          'Cannot modify data in read-only transaction',
          currentContext.txnId
        );
      }

      const logEntry: TransactionLogEntry = {
        ...entry,
        sequence: currentContext.log.currentSequence++,
        timestamp: Date.now(),
      };

      currentContext.log.entries.push(logEntry);

      // Track operation for long-running transaction logging
      timeoutEnforcer.trackOperation(currentContext.txnId, logEntry);
    },

    setApplyFunction(fn: ApplyFunction): void {
      applyFn = fn;
    },

    // Extended methods for timeout support
    async handleAlarm(): Promise<void> {
      await timeoutEnforcer.handleAlarm();
    },

    hasAlarmSupport(): boolean {
      return timeoutEnforcer.hasAlarmSupport();
    },

    getTimeoutConfig(): TransactionTimeoutConfig {
      return timeoutEnforcer.getTimeoutConfig();
    },

    requestExtension(txnId: TransactionId, additionalMs: number): boolean {
      return timeoutEnforcer.requestExtension(txnId, additionalMs);
    },

    getRemainingTime(txnId: TransactionId): number {
      return timeoutEnforcer.getRemainingTime(txnId);
    },

    getIoTimeoutMs(): number {
      return timeoutEnforcer.getIoTimeoutMs();
    },

    executeWithIoTimeout<T>(operation: () => Promise<T>): Promise<T> {
      return timeoutEnforcer.executeWithIoTimeout(operation);
    },
  };

  return manager;
}

// =============================================================================
// Transaction Wrapper for Execute Function Pattern
// =============================================================================

/**
 * Result type for transaction execution
 */
export interface TransactionResult<T> {
  /** Whether transaction was committed */
  committed: boolean;
  /** Result value (if committed) */
  value?: T;
  /** Error (if rolled back) */
  error?: Error;
  /** Commit LSN (if committed) */
  commitLsn?: bigint;
}

/**
 * Execute a function within a transaction
 *
 * @example
 * ```typescript
 * const result = await executeInTransaction(manager, async (ctx) => {
 *   // Operations within transaction
 *   return { success: true };
 * });
 *
 * if (result.committed) {
 *   console.log('Committed at LSN:', result.commitLsn);
 * }
 * ```
 */
export async function executeInTransaction<T>(
  manager: TransactionManager,
  fn: (context: TransactionContext) => Promise<T>,
  options?: TransactionOptions
): Promise<TransactionResult<T>> {
  const context = await manager.begin(options);

  try {
    const value = await fn(context);
    const commitLsn = await manager.commit();

    return {
      committed: true,
      value,
      commitLsn,
    };
  } catch (error) {
    try {
      await manager.rollback();
    } catch (rollbackError) {
      // Log rollback failure but return original error
      moduleLogger.error(
        'Rollback failed',
        rollbackError instanceof Error ? rollbackError : new Error(String(rollbackError)),
        {
          txnId: context.txnId,
          operation: 'ROLLBACK',
          originalError: error instanceof Error ? error.message : String(error),
        }
      );
    }

    return {
      committed: false,
      error: error instanceof Error ? error : new Error(String(error)),
    };
  }
}

// =============================================================================
// Savepoint Wrapper for Nested Transaction Pattern
// =============================================================================

/**
 * Execute a function within a savepoint (nested transaction)
 *
 * @example
 * ```typescript
 * await executeInTransaction(manager, async (ctx) => {
 *   // Outer operations
 *
 *   const inner = await executeWithSavepoint(manager, 'sp1', async () => {
 *     // Inner operations - can be rolled back independently
 *     return { nested: true };
 *   });
 *
 *   if (!inner.committed) {
 *     // Inner failed but outer can continue
 *   }
 *
 *   return { outer: true };
 * });
 * ```
 */
export async function executeWithSavepoint<T>(
  manager: TransactionManager,
  name: string,
  fn: () => Promise<T>
): Promise<TransactionResult<T>> {
  const savepoint = await manager.savepoint(name);

  try {
    const value = await fn();
    await manager.release(name);

    return {
      committed: true,
      value,
    };
  } catch (error) {
    try {
      await manager.rollbackTo(name);
    } catch (rollbackError) {
      console.error('Rollback to savepoint failed:', rollbackError);
    }

    return {
      committed: false,
      error: error instanceof Error ? error : new Error(String(error)),
    };
  }
}

// =============================================================================
// Auto-Commit Transaction Manager Wrapper
// =============================================================================

/**
 * Wrapper that provides auto-commit behavior (each operation is its own transaction)
 */
export function createAutoCommitManager(
  baseManager: TransactionManager
): TransactionManager {
  let autoCommitEnabled = true;

  const wrapper: TransactionManager = {
    async begin(options?: TransactionOptions): Promise<TransactionContext> {
      const context = await baseManager.begin(options);
      autoCommitEnabled = false;
      return { ...context, autoCommit: true };
    },

    async commit(): Promise<LSN> {
      const result = await baseManager.commit();
      autoCommitEnabled = true;
      return result;
    },

    async rollback(): Promise<void> {
      await baseManager.rollback();
      autoCommitEnabled = true;
    },

    async savepoint(name: string): Promise<Savepoint> {
      return baseManager.savepoint(name);
    },

    async release(name: string): Promise<void> {
      return baseManager.release(name);
    },

    async rollbackTo(name: string): Promise<void> {
      return baseManager.rollbackTo(name);
    },

    getContext(): TransactionContext | null {
      return baseManager.getContext();
    },

    isActive(): boolean {
      return baseManager.isActive();
    },

    getState(): TransactionState {
      return baseManager.getState();
    },

    logOperation(entry: Omit<TransactionLogEntry, 'sequence' | 'timestamp'>): void {
      baseManager.logOperation(entry);
    },

    setApplyFunction(fn: ApplyFunction): void {
      baseManager.setApplyFunction(fn);
    },
  };

  return wrapper;
}

// =============================================================================
// Read-Only Transaction Helper
// =============================================================================

/**
 * Execute a read-only transaction
 */
export async function executeReadOnly<T>(
  manager: TransactionManager,
  fn: (context: TransactionContext) => Promise<T>
): Promise<T> {
  const context = await manager.begin({ readOnly: true });

  try {
    const result = await fn(context);
    await manager.commit();
    return result;
  } catch (error) {
    await manager.rollback();
    throw error;
  }
}
