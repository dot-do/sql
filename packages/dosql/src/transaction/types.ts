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
}

// =============================================================================
// Error Types
// =============================================================================

/**
 * Transaction-specific error codes
 */
export enum TransactionErrorCode {
  /** No transaction is active */
  NO_ACTIVE_TRANSACTION = 'TXN_NO_ACTIVE',
  /** Transaction already active */
  TRANSACTION_ALREADY_ACTIVE = 'TXN_ALREADY_ACTIVE',
  /** Savepoint not found */
  SAVEPOINT_NOT_FOUND = 'TXN_SAVEPOINT_NOT_FOUND',
  /** Duplicate savepoint name */
  DUPLICATE_SAVEPOINT = 'TXN_DUPLICATE_SAVEPOINT',
  /** Lock acquisition failed */
  LOCK_FAILED = 'TXN_LOCK_FAILED',
  /** Lock timeout */
  LOCK_TIMEOUT = 'TXN_LOCK_TIMEOUT',
  /** Deadlock detected */
  DEADLOCK = 'TXN_DEADLOCK',
  /** Serialization failure (MVCC conflict) */
  SERIALIZATION_FAILURE = 'TXN_SERIALIZATION_FAILURE',
  /** Transaction aborted */
  ABORTED = 'TXN_ABORTED',
  /** Read-only violation */
  READ_ONLY_VIOLATION = 'TXN_READ_ONLY_VIOLATION',
  /** Invalid state transition */
  INVALID_STATE = 'TXN_INVALID_STATE',
  /** WAL integration failure */
  WAL_FAILURE = 'TXN_WAL_FAILURE',
  /** Rollback failed */
  ROLLBACK_FAILED = 'TXN_ROLLBACK_FAILED',
}

/**
 * Custom error class for transaction operations
 */
export class TransactionError extends Error {
  constructor(
    public readonly code: TransactionErrorCode,
    message: string,
    public readonly txnId?: TransactionId,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = 'TransactionError';
  }
}

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
