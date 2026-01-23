/**
 * Transaction Types for DoSQL
 *
 * SQLite-compatible transaction semantics including:
 * - Transaction states (NONE, ACTIVE, SAVEPOINT)
 * - Transaction modes (DEFERRED, IMMEDIATE, EXCLUSIVE)
 * - Savepoint stack for nested transactions
 * - Transaction log for rollback support
 *
 * @packageDocumentation
 */

import type { WALEntry, WALWriter, WALReader } from '../wal/types.js';
import type { LSN, TransactionId } from '../engine/types.js';
import {
  DoSQLError,
  ErrorCategory,
  registerErrorClass,
  type ErrorContext,
  type SerializedError,
} from '../errors/base.js';

// Re-export branded types for convenience
export type { LSN, TransactionId } from '../engine/types.js';
export { createLSN, createTransactionId } from '../engine/types.js';

// =============================================================================
// Transaction State
// =============================================================================

/**
 * Transaction state machine states
 */
export enum TransactionState {
  /** No active transaction */
  NONE = 'NONE',
  /** Transaction is active */
  ACTIVE = 'ACTIVE',
  /** Inside a savepoint (nested transaction) */
  SAVEPOINT = 'SAVEPOINT',
}

/**
 * SQLite-compatible transaction modes
 *
 * - DEFERRED: Locks are acquired only when needed (default)
 * - IMMEDIATE: Acquires write lock immediately
 * - EXCLUSIVE: Acquires exclusive lock, preventing all other access
 */
export enum TransactionMode {
  /** Default mode - locks acquired when needed */
  DEFERRED = 'DEFERRED',
  /** Acquire write lock immediately */
  IMMEDIATE = 'IMMEDIATE',
  /** Acquire exclusive lock for all access */
  EXCLUSIVE = 'EXCLUSIVE',
}

// =============================================================================
// Isolation Levels
// =============================================================================

/**
 * Transaction isolation levels
 *
 * SQLite uses SERIALIZABLE by default, but we support additional
 * levels for Durable Object context optimization.
 */
export enum IsolationLevel {
  /** Dirty reads allowed - lowest isolation */
  READ_UNCOMMITTED = 'READ_UNCOMMITTED',
  /** Only committed data visible */
  READ_COMMITTED = 'READ_COMMITTED',
  /** Repeatable reads within transaction */
  REPEATABLE_READ = 'REPEATABLE_READ',
  /** Snapshot isolation using MVCC */
  SNAPSHOT = 'SNAPSHOT',
  /** Full serialization - highest isolation (SQLite default) */
  SERIALIZABLE = 'SERIALIZABLE',
}

// =============================================================================
// Savepoint Types
// =============================================================================

/**
 * A savepoint marker for nested transaction support
 */
export interface Savepoint {
  /** Unique savepoint name */
  name: string;
  /** LSN at savepoint creation (branded type) */
  lsn: LSN;
  /** Timestamp when savepoint was created */
  timestamp: number;
  /** Snapshot of transaction log entries at savepoint */
  logSnapshotIndex: number;
  /** Depth in savepoint stack (0 = first savepoint) */
  depth: number;
}

/**
 * Stack of savepoints for nested transaction support
 */
export interface SavepointStack {
  /** Ordered list of savepoints (most recent last) */
  savepoints: Savepoint[];
  /** Maximum depth reached */
  maxDepth: number;
}

/**
 * Create an empty savepoint stack
 */
export function createSavepointStack(): SavepointStack {
  return {
    savepoints: [],
    maxDepth: 0,
  };
}

// =============================================================================
// Transaction Log Types
// =============================================================================

/**
 * Types of operations in the transaction log
 */
export type TransactionLogOperation =
  | 'INSERT'
  | 'UPDATE'
  | 'DELETE'
  | 'SAVEPOINT'
  | 'RELEASE'
  | 'ROLLBACK_TO';

/**
 * A single entry in the transaction log for rollback support
 */
export interface TransactionLogEntry {
  /** Operation type */
  op: TransactionLogOperation;
  /** Target table name */
  table: string;
  /** Primary key of affected row */
  key?: Uint8Array;
  /** Value before the operation (for undo) */
  beforeValue?: Uint8Array;
  /** Value after the operation (for redo) */
  afterValue?: Uint8Array;
  /** Savepoint name (for savepoint operations) */
  savepointName?: string;
  /** LSN from WAL (links to durability layer, branded type) */
  walLsn?: LSN;
  /** Timestamp of the operation */
  timestamp: number;
  /** Sequence within transaction */
  sequence: number;
}

/**
 * Transaction log for supporting rollback
 */
export interface TransactionLog {
  /** Transaction ID (branded type) */
  txnId: TransactionId;
  /** All logged operations in order */
  entries: TransactionLogEntry[];
  /** Current sequence number */
  currentSequence: number;
  /** Start timestamp */
  startedAt: number;
}

/**
 * Create an empty transaction log
 */
export function createTransactionLog(txnId: TransactionId): TransactionLog {
  return {
    txnId,
    entries: [],
    currentSequence: 0,
    startedAt: Date.now(),
  };
}

// =============================================================================
// MVCC Types (for Snapshot Isolation)
// =============================================================================

/**
 * Version info for MVCC
 */
export interface RowVersion {
  /** Transaction ID that created this version (branded type) */
  createdBy: TransactionId;
  /** Transaction ID that deleted this version (or null if active, branded type) */
  deletedBy?: TransactionId;
  /** LSN when created (branded type) */
  createdLsn: LSN;
  /** LSN when deleted (if applicable, branded type) */
  deletedLsn?: LSN;
  /** The actual row data */
  data: Uint8Array;
}

/**
 * Snapshot for MVCC reads
 */
export interface Snapshot {
  /** Transaction ID that owns this snapshot (branded type) */
  txnId: TransactionId;
  /** LSN at snapshot creation (branded type) */
  snapshotLsn: LSN;
  /** Timestamp of snapshot */
  timestamp: number;
  /** Transaction IDs that were active when snapshot was taken (branded type) */
  activeTransactions: Set<TransactionId>;
}

// =============================================================================
// Lock Types
// =============================================================================

/**
 * Lock types for transaction isolation
 */
export enum LockType {
  /** Shared lock for reading */
  SHARED = 'SHARED',
  /** Reserved lock (preparing to write) */
  RESERVED = 'RESERVED',
  /** Pending lock (waiting for shared locks to release) */
  PENDING = 'PENDING',
  /** Exclusive lock for writing */
  EXCLUSIVE = 'EXCLUSIVE',
}

/**
 * Lock request for the lock manager
 */
export interface LockRequest {
  /** Transaction requesting the lock (branded type) */
  txnId: TransactionId;
  /** Type of lock requested */
  lockType: LockType;
  /** Resource being locked (table name or '*' for database) */
  resource: string;
  /** Request timestamp */
  timestamp: number;
  /** Request timeout in ms */
  timeout?: number;
}

/**
 * Result of a lock acquisition attempt
 */
export interface LockResult {
  /** Whether lock was acquired */
  acquired: boolean;
  /** Lock type that was granted (may differ from requested) */
  grantedType?: LockType;
  /** If not acquired, the blocking transaction (branded type) */
  blockedBy?: TransactionId;
  /** Wait time in ms if lock was acquired after waiting */
  waitTime?: number;
}

/**
 * Current lock held by a transaction
 */
export interface HeldLock {
  /** Transaction holding the lock (branded type) */
  txnId: TransactionId;
  /** Type of lock held */
  lockType: LockType;
  /** Resource that is locked */
  resource: string;
  /** When lock was acquired */
  acquiredAt: number;
}

// =============================================================================
// Transaction Context
// =============================================================================

/**
 * Complete transaction context
 */
export interface TransactionContext {
  /** Unique transaction ID (branded type) */
  txnId: TransactionId;
  /** Current transaction state */
  state: TransactionState;
  /** Transaction mode */
  mode: TransactionMode;
  /** Isolation level */
  isolationLevel: IsolationLevel;
  /** Savepoint stack */
  savepoints: SavepointStack;
  /** Transaction log for rollback */
  log: TransactionLog;
  /** MVCC snapshot (for snapshot isolation) */
  snapshot?: Snapshot;
  /** Currently held locks */
  locks: HeldLock[];
  /** Transaction start time */
  startedAt: number;
  /** Read-only flag */
  readOnly: boolean;
  /** Auto-commit flag (transaction ends after each statement) */
  autoCommit: boolean;
  /** Timestamp when transaction expires (for timeout enforcement) */
  expiresAt?: number;
  /** Number of timeout extensions granted */
  extensionCount?: number;
}

/**
 * Options for creating a new transaction
 */
export interface TransactionOptions {
  /** Transaction mode (default: DEFERRED) */
  mode?: TransactionMode;
  /** Isolation level (default: SERIALIZABLE) */
  isolationLevel?: IsolationLevel;
  /** Whether transaction is read-only */
  readOnly?: boolean;
  /** Lock timeout in milliseconds */
  lockTimeout?: number;
  /** Custom transaction ID (auto-generated if not provided, branded type) */
  txnId?: TransactionId;
  /** Transaction timeout in milliseconds */
  timeoutMs?: number;
}

// =============================================================================
// Error Types
// =============================================================================

/**
 * Transaction-specific error codes
 *
 * Each error code includes recovery hints to help diagnose and resolve issues.
 */
export enum TransactionErrorCode {
  /**
   * No transaction is currently active.
   *
   * @description Thrown when attempting to commit, rollback, or perform
   * transaction-specific operations without an active transaction.
   *
   * @recovery
   * - Call `begin()` to start a new transaction before performing operations
   * - Check if a previous transaction was already committed or rolled back
   * - Verify that auto-commit mode is disabled if you expect manual transaction control
   */
  NO_ACTIVE_TRANSACTION = 'TXN_NO_ACTIVE',

  /**
   * A transaction is already in progress.
   *
   * @description Thrown when attempting to begin a new transaction while
   * another transaction is already active on the same connection.
   *
   * @recovery
   * - Commit or rollback the existing transaction before starting a new one
   * - Use savepoints for nested transaction semantics instead of nested BEGIN
   * - Check for unfinished transactions from previous operations
   */
  TRANSACTION_ALREADY_ACTIVE = 'TXN_ALREADY_ACTIVE',

  /**
   * The specified savepoint does not exist.
   *
   * @description Thrown when attempting to release or rollback to a savepoint
   * that was never created or has already been released.
   *
   * @recovery
   * - Verify the savepoint name is spelled correctly (names are case-sensitive)
   * - Ensure the savepoint was created in the current transaction
   * - Check if the savepoint was already released or rolled back
   * - List active savepoints to confirm available names
   */
  SAVEPOINT_NOT_FOUND = 'TXN_SAVEPOINT_NOT_FOUND',

  /**
   * A savepoint with the same name already exists.
   *
   * @description Thrown when attempting to create a savepoint with a name
   * that is already in use within the current transaction.
   *
   * @recovery
   * - Use a unique name for the new savepoint
   * - Release the existing savepoint first if you want to reuse the name
   * - Consider using auto-generated savepoint names (e.g., with timestamps)
   */
  DUPLICATE_SAVEPOINT = 'TXN_DUPLICATE_SAVEPOINT',

  /**
   * Failed to acquire the requested lock.
   *
   * @description Thrown when a lock cannot be acquired due to conflicts
   * with other transactions holding incompatible locks.
   *
   * @recovery
   * - Retry the operation after a brief delay
   * - Use a lower isolation level if consistency requirements allow
   * - Reduce the scope of locked resources
   * - Consider using IMMEDIATE mode to acquire locks upfront
   * - Check for long-running transactions that may be holding locks
   */
  LOCK_FAILED = 'TXN_LOCK_FAILED',

  /**
   * Lock acquisition timed out.
   *
   * @description Thrown when the lock wait time exceeds the configured
   * timeout threshold.
   *
   * @recovery
   * - Increase the lock timeout via `lockTimeout` in TransactionOptions
   * - Retry the operation with exponential backoff
   * - Investigate and resolve blocking transactions
   * - Consider breaking large transactions into smaller units
   * - Use `NOWAIT` semantics to fail fast if immediate lock is required
   */
  LOCK_TIMEOUT = 'TXN_LOCK_TIMEOUT',

  /**
   * A deadlock was detected between transactions.
   *
   * @description Thrown when two or more transactions are waiting for
   * locks held by each other, creating a circular dependency.
   *
   * @recovery
   * - Retry the entire transaction from the beginning
   * - Access resources in a consistent order across all transactions
   * - Use shorter transactions to reduce deadlock probability
   * - Consider using EXCLUSIVE mode to prevent concurrent access
   * - Implement application-level retry logic with backoff
   */
  DEADLOCK = 'TXN_DEADLOCK',

  /**
   * Serialization failure due to MVCC conflict.
   *
   * @description Thrown when concurrent transactions modify the same data
   * in a way that violates serializable isolation guarantees.
   *
   * @recovery
   * - Retry the entire transaction from the beginning
   * - Use a lower isolation level (e.g., READ_COMMITTED) if acceptable
   * - Reduce transaction duration to minimize conflict windows
   * - Partition data access patterns to avoid overlapping writes
   * - Implement optimistic concurrency control at the application level
   */
  SERIALIZATION_FAILURE = 'TXN_SERIALIZATION_FAILURE',

  /**
   * The transaction was aborted.
   *
   * @description Thrown when a transaction is forcefully terminated,
   * either by the system or due to an unrecoverable error.
   *
   * @recovery
   * - Start a new transaction and retry the operations
   * - Check for system-level issues (memory, disk space, etc.)
   * - Review transaction logs for the underlying cause
   * - Ensure proper error handling in transaction callbacks
   */
  ABORTED = 'TXN_ABORTED',

  /**
   * Attempted to write in a read-only transaction.
   *
   * @description Thrown when INSERT, UPDATE, DELETE, or other write
   * operations are attempted within a read-only transaction.
   *
   * @recovery
   * - Start a new read-write transaction for write operations
   * - Remove the `readOnly: true` option from TransactionOptions
   * - Verify the transaction mode matches the intended operations
   * - Use separate transactions for read and write workloads
   */
  READ_ONLY_VIOLATION = 'TXN_READ_ONLY_VIOLATION',

  /**
   * Invalid state transition attempted.
   *
   * @description Thrown when an operation is attempted that is not valid
   * for the current transaction state (e.g., commit after rollback).
   *
   * @recovery
   * - Check the current transaction state before operations
   * - Ensure proper sequencing of transaction lifecycle methods
   * - Do not reuse a transaction context after commit or rollback
   * - Review the transaction state machine: NONE -> ACTIVE -> (SAVEPOINT) -> NONE
   */
  INVALID_STATE = 'TXN_INVALID_STATE',

  /**
   * Write-ahead log (WAL) operation failed.
   *
   * @description Thrown when the transaction cannot write to or read from
   * the WAL, potentially compromising durability guarantees.
   *
   * @recovery
   * - Check disk space and I/O availability
   * - Verify WAL file permissions and accessibility
   * - Inspect WAL configuration and file system health
   * - Consider a system restart if WAL is corrupted
   * - Contact support if the issue persists
   */
  WAL_FAILURE = 'TXN_WAL_FAILURE',

  /**
   * Failed to rollback the transaction.
   *
   * @description Thrown when the rollback operation itself encounters
   * an error, leaving the database in a potentially inconsistent state.
   *
   * @recovery
   * - This is a critical error - investigate immediately
   * - Check WAL integrity and available disk space
   * - Review system logs for underlying I/O or memory errors
   * - A database recovery procedure may be required
   * - Consider restoring from backup if data integrity is compromised
   */
  ROLLBACK_FAILED = 'TXN_ROLLBACK_FAILED',

  /**
   * Transaction exceeded the maximum allowed duration.
   *
   * @description Thrown when a transaction runs longer than the configured
   * timeout period, triggering automatic termination.
   *
   * @recovery
   * - Increase `timeoutMs` in TransactionOptions if longer duration is needed
   * - Break large transactions into smaller, faster units
   * - Optimize slow queries within the transaction
   * - Use batch processing for bulk operations
   * - Check for external blocking factors (network, locks)
   */
  TIMEOUT = 'TXN_TIMEOUT',

  /**
   * I/O operation timed out.
   *
   * @description Thrown when a read or write operation to storage exceeds
   * the allowed time, typically due to system resource constraints.
   *
   * @recovery
   * - Retry the operation after a brief delay
   * - Check storage system health and responsiveness
   * - Verify network connectivity for remote storage
   * - Reduce I/O load by batching or throttling requests
   * - Increase I/O timeout thresholds if storage latency is expected
   */
  IO_TIMEOUT = 'IO_TIMEOUT',
}

/**
 * Error class for transaction operations
 *
 * Extends DoSQLError to provide consistent error handling with:
 * - Machine-readable error codes
 * - Error categories for API layer handling
 * - Recovery hints for developers
 * - Serialization support for RPC
 *
 * @example
 * ```typescript
 * try {
 *   await tx.commit();
 * } catch (error) {
 *   if (error instanceof TransactionError) {
 *     if (error.isRetryable()) {
 *       // Retry the transaction
 *     }
 *     console.log(error.code);        // 'TXN_ABORTED'
 *     console.log(error.txnId);       // Transaction ID
 *     console.log(error.recoveryHint); // How to fix
 *   }
 * }
 * ```
 */
export class TransactionError extends DoSQLError {
  readonly code: TransactionErrorCode;
  readonly category: ErrorCategory;
  readonly txnId?: TransactionId;

  constructor(
    code: TransactionErrorCode,
    message: string,
    options?: {
      cause?: Error;
      context?: ErrorContext;
      txnId?: TransactionId;
    }
  ) {
    super(message, { cause: options?.cause, context: options?.context });
    this.name = 'TransactionError';
    this.code = code;
    this.txnId = options?.txnId;

    // Set category based on error code
    this.category = this.determineCategory();

    // Include transaction ID in context
    if (this.txnId) {
      this.context = { ...this.context, transactionId: this.txnId };
    }

    // Set recovery hints based on error code
    this.setRecoveryHint();
  }

  private determineCategory(): ErrorCategory {
    switch (this.code) {
      case TransactionErrorCode.DEADLOCK:
      case TransactionErrorCode.SERIALIZATION_FAILURE:
      case TransactionErrorCode.ABORTED:
        return ErrorCategory.CONFLICT;
      case TransactionErrorCode.TIMEOUT:
      case TransactionErrorCode.LOCK_TIMEOUT:
        return ErrorCategory.TIMEOUT;
      case TransactionErrorCode.NO_ACTIVE_TRANSACTION:
      case TransactionErrorCode.TRANSACTION_ALREADY_ACTIVE:
      case TransactionErrorCode.SAVEPOINT_NOT_FOUND:
      case TransactionErrorCode.READ_ONLY_VIOLATION:
        return ErrorCategory.VALIDATION;
      case TransactionErrorCode.WAL_FAILURE:
      case TransactionErrorCode.ROLLBACK_FAILED:
        return ErrorCategory.INTERNAL;
      default:
        return ErrorCategory.EXECUTION;
    }
  }

  private setRecoveryHint(): void {
    switch (this.code) {
      case TransactionErrorCode.NO_ACTIVE_TRANSACTION:
        this.recoveryHint = 'Call beginTransaction() before performing transaction operations';
        break;
      case TransactionErrorCode.TRANSACTION_ALREADY_ACTIVE:
        this.recoveryHint = 'Commit or rollback the existing transaction before starting a new one, or use savepoints';
        break;
      case TransactionErrorCode.ABORTED:
        this.recoveryHint = 'Retry the entire transaction with exponential backoff';
        break;
      case TransactionErrorCode.DEADLOCK:
        this.recoveryHint = 'Acquire locks in a consistent order across all transactions to prevent deadlocks';
        break;
      case TransactionErrorCode.SERIALIZATION_FAILURE:
        this.recoveryHint = 'Retry the transaction - this is expected under SERIALIZABLE isolation';
        break;
      case TransactionErrorCode.LOCK_TIMEOUT:
        this.recoveryHint = 'Increase lock timeout or reduce transaction duration';
        break;
      case TransactionErrorCode.TIMEOUT:
        this.recoveryHint = 'Increase transaction timeout or break into smaller transactions';
        break;
      case TransactionErrorCode.SAVEPOINT_NOT_FOUND:
        this.recoveryHint = 'Verify savepoint name and ensure it was created in the current transaction';
        break;
      case TransactionErrorCode.READ_ONLY_VIOLATION:
        this.recoveryHint = 'Use a read-write transaction for write operations';
        break;
      case TransactionErrorCode.WAL_FAILURE:
        this.recoveryHint = 'Check disk space and I/O availability';
        break;
      case TransactionErrorCode.ROLLBACK_FAILED:
        this.recoveryHint = 'Critical error - investigate immediately and consider database recovery';
        break;
    }
  }

  /**
   * Check if this error is retryable
   */
  isRetryable(): boolean {
    return [
      TransactionErrorCode.ABORTED,
      TransactionErrorCode.DEADLOCK,
      TransactionErrorCode.SERIALIZATION_FAILURE,
      TransactionErrorCode.LOCK_TIMEOUT,
      TransactionErrorCode.TIMEOUT,
      TransactionErrorCode.WAL_FAILURE,
    ].includes(this.code);
  }

  /**
   * Get a user-friendly error message
   */
  toUserMessage(): string {
    switch (this.code) {
      case TransactionErrorCode.NO_ACTIVE_TRANSACTION:
        return 'No active transaction. Please start a transaction first.';
      case TransactionErrorCode.TRANSACTION_ALREADY_ACTIVE:
        return 'A transaction is already in progress. Complete it before starting a new one.';
      case TransactionErrorCode.ABORTED:
        return 'The transaction was aborted due to a conflict. Please retry.';
      case TransactionErrorCode.DEADLOCK:
        return 'A deadlock was detected. Please retry the transaction.';
      case TransactionErrorCode.SERIALIZATION_FAILURE:
        return 'The transaction failed due to concurrent modifications. Please retry.';
      case TransactionErrorCode.LOCK_TIMEOUT:
        return 'Failed to acquire a lock within the timeout period. Please retry.';
      case TransactionErrorCode.TIMEOUT:
        return 'The transaction timed out. Please retry with a shorter transaction or increased timeout.';
      case TransactionErrorCode.SAVEPOINT_NOT_FOUND:
        return 'The specified savepoint does not exist.';
      case TransactionErrorCode.READ_ONLY_VIOLATION:
        return 'Cannot perform write operations in a read-only transaction.';
      case TransactionErrorCode.WAL_FAILURE:
        return 'A write-ahead log error occurred. Please retry.';
      case TransactionErrorCode.ROLLBACK_FAILED:
        return 'Failed to rollback the transaction. Database integrity may be affected.';
      default:
        return this.message;
    }
  }

  /**
   * Deserialize from JSON
   */
  static fromJSON(json: SerializedError): TransactionError {
    return new TransactionError(
      json.code as TransactionErrorCode,
      json.message,
      {
        context: json.context,
        txnId: json.context?.transactionId as TransactionId | undefined,
      }
    );
  }
}

// Register for deserialization
registerErrorClass('TransactionError', TransactionError);

// =============================================================================
// Transaction Manager Interface
// =============================================================================

/**
 * Apply function for rolling back or applying operations
 */
export type ApplyFunction = (
  op: TransactionLogOperation,
  table: string,
  key: Uint8Array | undefined,
  value: Uint8Array | undefined
) => Promise<void>;

/**
 * Transaction manager interface
 */
export interface TransactionManager {
  /**
   * Begin a new transaction
   * @param options Transaction options
   * @returns Transaction context
   */
  begin(options?: TransactionOptions): Promise<TransactionContext>;

  /**
   * Commit the current transaction
   * @returns Commit LSN (branded type)
   */
  commit(): Promise<LSN>;

  /**
   * Rollback the current transaction
   */
  rollback(): Promise<void>;

  /**
   * Create a savepoint
   * @param name Savepoint name
   */
  savepoint(name: string): Promise<Savepoint>;

  /**
   * Release a savepoint (merge changes into parent transaction)
   * @param name Savepoint name
   */
  release(name: string): Promise<void>;

  /**
   * Rollback to a savepoint
   * @param name Savepoint name
   */
  rollbackTo(name: string): Promise<void>;

  /**
   * Get current transaction context (or null if none active)
   */
  getContext(): TransactionContext | null;

  /**
   * Check if a transaction is active
   */
  isActive(): boolean;

  /**
   * Get current transaction state
   */
  getState(): TransactionState;

  /**
   * Log an operation for rollback support
   * @param entry Log entry
   */
  logOperation(entry: Omit<TransactionLogEntry, 'sequence' | 'timestamp'>): void;

  /**
   * Set the apply function for rollback operations
   * @param fn Apply function
   */
  setApplyFunction(fn: ApplyFunction): void;
}

// =============================================================================
// Statistics and Monitoring
// =============================================================================

/**
 * Transaction statistics
 */
export interface TransactionStats {
  /** Total transactions started */
  totalStarted: number;
  /** Total transactions committed */
  totalCommitted: number;
  /** Total transactions rolled back */
  totalRolledBack: number;
  /** Total savepoints created */
  totalSavepoints: number;
  /** Total lock waits */
  totalLockWaits: number;
  /** Total deadlocks detected */
  totalDeadlocks: number;
  /** Average transaction duration in ms */
  avgDurationMs: number;
  /** Currently active transactions */
  activeCount: number;
}

// =============================================================================
// Transaction Timeout Configuration
// =============================================================================

/**
 * Configuration for transaction timeout enforcement
 */
export interface TransactionTimeoutConfig {
  /** Default timeout for transactions in milliseconds */
  defaultTimeoutMs: number;
  /** Maximum allowed timeout in milliseconds */
  maxTimeoutMs: number;
  /** Grace period before hard timeout in milliseconds */
  gracePeriodMs: number;
  /** Threshold for warning logs in milliseconds */
  warningThresholdMs: number;
}

/**
 * Default timeout configuration
 */
export const DEFAULT_TIMEOUT_CONFIG: TransactionTimeoutConfig = {
  defaultTimeoutMs: 30000,
  maxTimeoutMs: 120000,
  gracePeriodMs: 5000,
  warningThresholdMs: 25000,
};

/**
 * Warning level for long-running transaction logs
 */
export type WarningLevel = 'warning' | 'critical';

/**
 * Structured log entry for long-running transactions
 */
export interface LongRunningTransactionLog {
  /** Transaction ID */
  txnId: TransactionId;
  /** Duration in milliseconds */
  durationMs: number;
  /** Transaction start timestamp */
  startedAt: number;
  /** Whether transaction is read-only */
  readOnly: boolean;
  /** Transaction isolation level */
  isolationLevel: string;
  /** Number of operations logged */
  operationCount: number;
  /** Warning severity level */
  warningLevel: WarningLevel;
  /** Operations in the transaction (if tracked) */
  operations?: Array<{
    op: TransactionLogOperation;
    table: string;
    timestamp: number;
  }>;
  /** Locks held by the transaction */
  heldLocks?: HeldLock[];
}

/**
 * Durable Object storage interface for alarm scheduling
 */
export interface DurableObjectStorage {
  /** Set an alarm for a specific time */
  setAlarm(scheduledTime: number | Date): Promise<void>;
  /** Delete the current alarm */
  deleteAlarm(): Promise<void>;
  /** Get current alarm time */
  getAlarm(): Promise<number | null>;
}

/**
 * Durable Object state with storage
 */
export interface DurableObjectState {
  /** Storage interface for alarms */
  storage: DurableObjectStorage;
}
