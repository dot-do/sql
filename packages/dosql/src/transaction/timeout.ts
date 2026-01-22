/**
 * Transaction Timeout Enforcement for DoSQL
 *
 * Implements server-side timeout enforcement using Durable Object alarms:
 * - Alarm scheduling when transactions begin
 * - Automatic rollback on timeout
 * - Grace period handling with warnings
 * - Long-running transaction logging
 *
 * @packageDocumentation
 */

import {
  type TransactionId,
  type TransactionContext,
  type TransactionTimeoutConfig,
  type DurableObjectState,
  type LongRunningTransactionLog,
  type WarningLevel,
  type HeldLock,
  type TransactionLogEntry,
  TransactionState,
  TransactionError,
  TransactionErrorCode,
  DEFAULT_TIMEOUT_CONFIG,
} from './types.js';

// =============================================================================
// Timeout Enforcer Types
// =============================================================================

/**
 * Options for creating a transaction timeout enforcer
 */
export interface TimeoutEnforcerOptions {
  /** Timeout configuration */
  config?: Partial<TransactionTimeoutConfig>;
  /** Durable Object state for alarm scheduling */
  doState?: DurableObjectState;
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
}

/**
 * Internal tracking state for a transaction
 */
interface TransactionTimeoutState {
  /** Transaction ID */
  txnId: TransactionId;
  /** When the transaction started */
  startedAt: number;
  /** When the transaction expires */
  expiresAt: number;
  /** Number of extensions granted */
  extensionCount: number;
  /** Whether warning has been emitted */
  warningEmitted: boolean;
  /** Whether long-running warning has been emitted */
  longRunningWarningEmitted: boolean;
  /** Timer ID for warning threshold */
  warningTimerId?: ReturnType<typeof setTimeout>;
  /** Timer ID for grace period warning */
  gracePeriodTimerId?: ReturnType<typeof setTimeout>;
  /** Timer ID for timeout */
  timeoutTimerId?: ReturnType<typeof setTimeout>;
  /** Operations logged (if tracking enabled) */
  operations?: Array<{ op: string; table: string; timestamp: number }>;
  /** Whether transaction is read-only */
  readOnly: boolean;
  /** Isolation level */
  isolationLevel: string;
  /** Held locks getter */
  getHeldLocks?: () => HeldLock[];
}

/**
 * Transaction timeout enforcer interface
 */
export interface TransactionTimeoutEnforcer {
  /** Register a transaction for timeout tracking */
  registerTransaction(
    txnId: TransactionId,
    timeoutMs: number,
    context: TransactionContext
  ): void;
  /** Unregister a transaction (on commit/rollback) */
  unregisterTransaction(txnId: TransactionId): void;
  /** Check if transaction is timed out */
  isTimedOut(txnId: TransactionId): boolean;
  /** Get remaining time for transaction */
  getRemainingTime(txnId: TransactionId): number;
  /** Request timeout extension */
  requestExtension(txnId: TransactionId, additionalMs: number): boolean;
  /** Handle alarm trigger (for DO alarm integration) */
  handleAlarm(): Promise<void>;
  /** Get timeout configuration */
  getTimeoutConfig(): TransactionTimeoutConfig;
  /** Check if DO alarm support is enabled */
  hasAlarmSupport(): boolean;
  /** Get I/O timeout in milliseconds */
  getIoTimeoutMs(): number;
  /** Execute operation with I/O timeout */
  executeWithIoTimeout<T>(operation: () => Promise<T>): Promise<T>;
  /** Track an operation for logging */
  trackOperation(txnId: TransactionId, entry: TransactionLogEntry): void;
  /** Set lock manager for held locks tracking */
  setHeldLocksGetter(
    txnId: TransactionId,
    getter: () => HeldLock[]
  ): void;
  /** Set rollback function */
  setRollbackFn(fn: (txnId: TransactionId) => Promise<void>): void;
}

// =============================================================================
// Timeout Enforcer Implementation
// =============================================================================

/**
 * Create a transaction timeout enforcer
 */
export function createTimeoutEnforcer(
  options: TimeoutEnforcerOptions = {}
): TransactionTimeoutEnforcer {
  const {
    config: partialConfig,
    doState,
    maxExtensions = 3,
    onTimeoutWarning,
    onTimeout,
    onLongRunningTransaction,
    trackQueries = false,
    ioTimeoutMs = 30000,
  } = options;

  // Merge with default config
  const config: TransactionTimeoutConfig = {
    ...DEFAULT_TIMEOUT_CONFIG,
    ...partialConfig,
  };

  // Transaction states
  const transactions = new Map<TransactionId, TransactionTimeoutState>();

  // Rollback function (set by manager)
  let rollbackFn: ((txnId: TransactionId) => Promise<void>) | null = null;

  /**
   * Clear all timers for a transaction
   */
  function clearTimers(state: TransactionTimeoutState): void {
    if (state.warningTimerId) {
      clearTimeout(state.warningTimerId);
      state.warningTimerId = undefined;
    }
    if (state.gracePeriodTimerId) {
      clearTimeout(state.gracePeriodTimerId);
      state.gracePeriodTimerId = undefined;
    }
    if (state.timeoutTimerId) {
      clearTimeout(state.timeoutTimerId);
      state.timeoutTimerId = undefined;
    }
  }

  /**
   * Setup timers for a transaction
   */
  function setupTimers(state: TransactionTimeoutState): void {
    const now = Date.now();
    const remaining = state.expiresAt - now;

    // Warning threshold timer (long-running transaction warning)
    if (!state.longRunningWarningEmitted) {
      const warningTime = config.warningThresholdMs - (now - state.startedAt);
      if (warningTime > 0) {
        state.warningTimerId = setTimeout(() => {
          emitLongRunningWarning(state);
        }, warningTime);
      } else if (now - state.startedAt >= config.warningThresholdMs) {
        // Already past threshold
        emitLongRunningWarning(state);
      }
    }

    // Grace period warning timer
    if (!state.warningEmitted && remaining > config.gracePeriodMs) {
      state.gracePeriodTimerId = setTimeout(() => {
        emitTimeoutWarning(state);
      }, remaining - config.gracePeriodMs);
    }

    // Timeout timer
    if (remaining > 0) {
      state.timeoutTimerId = setTimeout(() => {
        handleTimeout(state.txnId);
      }, remaining);
    }
  }

  /**
   * Emit long-running transaction warning
   */
  function emitLongRunningWarning(state: TransactionTimeoutState): void {
    if (state.longRunningWarningEmitted) return;
    state.longRunningWarningEmitted = true;

    if (onLongRunningTransaction) {
      const durationMs = Date.now() - state.startedAt;
      const warningLevel: WarningLevel =
        durationMs > config.maxTimeoutMs * 0.8 ? 'critical' : 'warning';

      const log: LongRunningTransactionLog = {
        txnId: state.txnId,
        durationMs,
        startedAt: state.startedAt,
        readOnly: state.readOnly,
        isolationLevel: state.isolationLevel,
        operationCount: state.operations?.length ?? 0,
        warningLevel,
        operations: state.operations?.map((op) => ({
          op: op.op as any,
          table: op.table,
          timestamp: op.timestamp,
        })),
        heldLocks: state.getHeldLocks?.(),
      };

      onLongRunningTransaction(log);
    }
  }

  /**
   * Emit timeout warning
   */
  function emitTimeoutWarning(state: TransactionTimeoutState): void {
    if (state.warningEmitted) return;
    state.warningEmitted = true;

    if (onTimeoutWarning) {
      const remaining = state.expiresAt - Date.now();
      onTimeoutWarning(state.txnId, remaining);
    }
  }

  /**
   * Handle transaction timeout
   */
  async function handleTimeout(txnId: TransactionId): Promise<void> {
    const state = transactions.get(txnId);
    if (!state) return;

    // Emit timeout callback
    if (onTimeout) {
      onTimeout(txnId);
    }

    // Perform rollback
    if (rollbackFn) {
      try {
        await rollbackFn(txnId);
      } catch (error) {
        // Log but don't throw - timeout handling should not fail
        console.error(`Failed to rollback timed-out transaction ${txnId}:`, error);
      }
    }

    // Clean up
    clearTimers(state);
    transactions.delete(txnId);

    // Cancel DO alarm if set
    if (doState) {
      try {
        await doState.storage.deleteAlarm();
      } catch {
        // Ignore alarm deletion errors
      }
    }
  }

  const enforcer: TransactionTimeoutEnforcer = {
    registerTransaction(
      txnId: TransactionId,
      timeoutMs: number,
      context: TransactionContext
    ): void {
      // Apply timeout limits
      const effectiveTimeout = Math.min(
        Math.max(timeoutMs, 0),
        config.maxTimeoutMs
      );

      const now = Date.now();
      const expiresAt = now + effectiveTimeout;

      const state: TransactionTimeoutState = {
        txnId,
        startedAt: now,
        expiresAt,
        extensionCount: 0,
        warningEmitted: false,
        longRunningWarningEmitted: false,
        operations: trackQueries ? [] : undefined,
        readOnly: context.readOnly,
        isolationLevel: context.isolationLevel,
      };

      transactions.set(txnId, state);

      // Update context with expiry
      context.expiresAt = expiresAt;
      context.extensionCount = 0;

      // Set DO alarm if available
      if (doState) {
        doState.storage.setAlarm(expiresAt).catch(() => {
          // Ignore alarm setting errors
        });
      }

      // Setup timers
      setupTimers(state);
    },

    unregisterTransaction(txnId: TransactionId): void {
      const state = transactions.get(txnId);
      if (!state) return;

      clearTimers(state);
      transactions.delete(txnId);

      // Cancel DO alarm
      if (doState) {
        doState.storage.deleteAlarm().catch(() => {
          // Ignore alarm deletion errors
        });
      }
    },

    isTimedOut(txnId: TransactionId): boolean {
      const state = transactions.get(txnId);
      if (!state) return false;
      return Date.now() >= state.expiresAt;
    },

    getRemainingTime(txnId: TransactionId): number {
      const state = transactions.get(txnId);
      if (!state) return 0;
      return Math.max(0, state.expiresAt - Date.now());
    },

    requestExtension(txnId: TransactionId, additionalMs: number): boolean {
      const state = transactions.get(txnId);
      if (!state) return false;

      // Check extension limit
      if (state.extensionCount >= maxExtensions) {
        return false;
      }

      // Calculate new expiry
      const now = Date.now();
      const currentRemaining = Math.max(0, state.expiresAt - now);
      const newExpiry = now + currentRemaining + additionalMs;

      // Cap at maxTimeoutMs from original start
      const maxAllowedExpiry = state.startedAt + config.maxTimeoutMs;
      state.expiresAt = Math.min(newExpiry, maxAllowedExpiry);
      state.extensionCount++;
      state.warningEmitted = false; // Reset warning for new timeout

      // Clear and reset timers
      clearTimers(state);
      setupTimers(state);

      // Update DO alarm if available
      if (doState) {
        doState.storage.setAlarm(state.expiresAt).catch(() => {
          // Ignore alarm setting errors
        });
      }

      return true;
    },

    async handleAlarm(): Promise<void> {
      // Find and handle any timed-out transactions
      const now = Date.now();
      for (const [txnId, state] of transactions) {
        if (now >= state.expiresAt) {
          await handleTimeout(txnId);
          break; // Handle one at a time
        }
      }
    },

    getTimeoutConfig(): TransactionTimeoutConfig {
      return { ...config };
    },

    hasAlarmSupport(): boolean {
      return doState !== undefined;
    },

    getIoTimeoutMs(): number {
      return ioTimeoutMs;
    },

    async executeWithIoTimeout<T>(operation: () => Promise<T>): Promise<T> {
      return new Promise<T>((resolve, reject) => {
        const timeoutId = setTimeout(() => {
          reject(
            new TransactionError(
              TransactionErrorCode.IO_TIMEOUT,
              `I/O operation timed out after ${ioTimeoutMs}ms`
            )
          );
        }, ioTimeoutMs);

        operation()
          .then((result) => {
            clearTimeout(timeoutId);
            resolve(result);
          })
          .catch((error) => {
            clearTimeout(timeoutId);
            reject(error);
          });
      });
    },

    trackOperation(txnId: TransactionId, entry: TransactionLogEntry): void {
      const state = transactions.get(txnId);
      if (!state || !state.operations) return;

      state.operations.push({
        op: entry.op,
        table: entry.table,
        timestamp: entry.timestamp,
      });
    },

    setHeldLocksGetter(
      txnId: TransactionId,
      getter: () => HeldLock[]
    ): void {
      const state = transactions.get(txnId);
      if (state) {
        state.getHeldLocks = getter;
      }
    },

    setRollbackFn(fn: (txnId: TransactionId) => Promise<void>): void {
      rollbackFn = fn;
    },
  };

  return enforcer;
}

// =============================================================================
// Timeout-Aware Transaction Checker
// =============================================================================

/**
 * Check if a transaction has timed out and throw if so
 */
export function checkTransactionTimeout(
  context: TransactionContext | null,
  enforcer?: TransactionTimeoutEnforcer
): void {
  if (!context) return;

  // Check via enforcer if available
  if (enforcer?.isTimedOut(context.txnId)) {
    throw new TransactionError(
      TransactionErrorCode.TIMEOUT,
      `Transaction ${context.txnId} has timed out`,
      context.txnId
    );
  }

  // Fallback to context expiry check
  if (context.expiresAt && Date.now() >= context.expiresAt) {
    throw new TransactionError(
      TransactionErrorCode.TIMEOUT,
      `Transaction ${context.txnId} has timed out`,
      context.txnId
    );
  }
}
