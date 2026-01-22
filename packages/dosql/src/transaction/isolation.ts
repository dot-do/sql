/**
 * Transaction Isolation for DoSQL
 *
 * Implements isolation levels and lock management:
 * - Read uncommitted, read committed, serializable
 * - Snapshot isolation for MVCC
 * - Lock management for Durable Object context
 *
 * @packageDocumentation
 */

import {
  type Snapshot,
  type RowVersion,
  type LockRequest,
  type LockResult,
  type HeldLock,
  type TransactionContext,
  type TransactionId,
  type LSN,
  IsolationLevel,
  LockType,
  TransactionError,
  TransactionErrorCode,
  createLSN,
  createTransactionId,
} from './types.js';

import {
  DeadlockDetector,
  DeadlockError,
  WaitForGraph,
  type DeadlockDetectorOptions,
  type DeadlockInfo,
  type DeadlockStats,
  type VictimSelectionPolicy,
  type DeadlockPreventionScheme,
} from '../database/deadlock-detector.js';

// =============================================================================
// Lock Manager
// =============================================================================

/**
 * Lock manager options
 */
export interface LockManagerOptions {
  /** Default lock timeout in milliseconds */
  defaultTimeout?: number;
  /** Enable deadlock detection */
  detectDeadlocks?: boolean;
  /** Maximum lock wait queue size */
  maxWaitQueueSize?: number;
  /** Victim selection policy for deadlock resolution */
  victimSelection?: VictimSelectionPolicy;
  /** Deadlock prevention scheme (if set, detection may be bypassed) */
  deadlockPrevention?: DeadlockPreventionScheme;
  /** Detection interval in ms for periodic detection */
  deadlockDetectionInterval?: number;
  /** Timeout for deadlock detection specifically */
  deadlockTimeout?: number;
  /** Callback when deadlock is detected */
  onDeadlock?: (info: DeadlockInfo) => void;
}

/**
 * Internal lock state
 */
interface LockState {
  /** Current lock holder(s) */
  holders: Map<TransactionId, LockType>;
  /** Waiting requests in order */
  waitQueue: Array<{
    request: LockRequest;
    resolve: (result: LockResult) => void;
    reject: (error: Error) => void;
    timeoutId?: ReturnType<typeof setTimeout>;
  }>;
}

/**
 * Lock manager for transaction isolation
 */
export interface LockManager {
  /**
   * Acquire a lock
   * @param request Lock request
   * @returns Lock result
   */
  acquire(request: LockRequest): Promise<LockResult>;

  /**
   * Release all locks held by a transaction
   * @param txnId Transaction ID
   */
  releaseAll(txnId: TransactionId): void;

  /**
   * Release a specific lock
   * @param txnId Transaction ID
   * @param resource Resource to unlock
   */
  release(txnId: TransactionId, resource: string): void;

  /**
   * Get all locks held by a transaction
   * @param txnId Transaction ID
   */
  getHeldLocks(txnId: TransactionId): HeldLock[];

  /**
   * Check if a lock can be acquired without blocking
   * @param request Lock request
   */
  canAcquire(request: LockRequest): boolean;

  /**
   * Get lock state for debugging
   */
  getState(): Map<string, { holders: TransactionId[]; waiters: TransactionId[] }>;

  /**
   * Get the wait-for graph for deadlock analysis
   */
  getWaitForGraph(): WaitForGraph;

  /**
   * Get deadlock statistics
   */
  getDeadlockStats(): DeadlockStats;

  /**
   * Get deadlock history
   */
  getDeadlockHistory(): DeadlockInfo[];

  /**
   * Set transaction cost for victim selection
   * @param txnId Transaction ID
   * @param cost Cost value (higher = more work done)
   */
  setTransactionCost(txnId: TransactionId, cost: number): void;

  /**
   * Mark a transaction as read-only
   * @param txnId Transaction ID
   * @param readOnly Whether the transaction is read-only
   */
  markReadOnly(txnId: TransactionId, readOnly: boolean): void;
}

/**
 * Create a lock manager
 */
export function createLockManager(
  options: LockManagerOptions = {}
): LockManager {
  const {
    defaultTimeout = 5000,
    detectDeadlocks = true,
    maxWaitQueueSize = 100,
    victimSelection = 'youngest',
    deadlockPrevention,
    deadlockDetectionInterval,
    deadlockTimeout,
    onDeadlock,
  } = options;

  // Lock state per resource
  const locks = new Map<string, LockState>();

  // Transaction -> held locks mapping for quick cleanup
  const txnLocks = new Map<TransactionId, Set<string>>();

  // Track lock acquisition times for each transaction
  const txnLockTimes = new Map<TransactionId, Map<string, number>>();

  // Create deadlock detector with options
  const deadlockDetector = new DeadlockDetector({
    enabled: detectDeadlocks,
    victimSelection,
    deadlockPrevention,
    deadlockDetectionInterval,
    deadlockTimeout,
    onDeadlock,
  });

  /**
   * Get or create lock state for a resource
   */
  function getLockState(resource: string): LockState {
    let state = locks.get(resource);
    if (!state) {
      state = {
        holders: new Map(),
        waitQueue: [],
      };
      locks.set(resource, state);
    }
    return state;
  }

  /**
   * Check if locks are compatible
   */
  function areLocksCompatible(a: LockType, b: LockType): boolean {
    // Lock compatibility matrix (SQLite-style with IX extension)
    // SHARED can coexist with SHARED and RESERVED
    // RESERVED can coexist with SHARED and RESERVED (like Intent Exclusive IX locks)
    // Everything else requires exclusive access
    if (a === LockType.SHARED && b === LockType.SHARED) {
      return true;
    }
    if (a === LockType.SHARED && b === LockType.RESERVED) {
      return true;
    }
    if (a === LockType.RESERVED && b === LockType.SHARED) {
      return true;
    }
    // RESERVED locks are compatible with each other (like IX locks)
    if (a === LockType.RESERVED && b === LockType.RESERVED) {
      return true;
    }
    return false;
  }

  /**
   * Check if a transaction can acquire a lock
   */
  function canAcquireImmediate(
    state: LockState,
    txnId: TransactionId,
    requestedType: LockType
  ): boolean {
    // If no holders, can acquire
    if (state.holders.size === 0) {
      return true;
    }

    // If this transaction already holds a lock
    const existingLock = state.holders.get(txnId);
    if (existingLock !== undefined) {
      // Can upgrade from SHARED to higher
      if (requestedType === LockType.SHARED) {
        return true;
      }
      // For upgrade to EXCLUSIVE, need to be the only holder
      if (requestedType === LockType.EXCLUSIVE && state.holders.size === 1) {
        return true;
      }
      // For RESERVED, can acquire if holding SHARED and others only have SHARED
      if (requestedType === LockType.RESERVED) {
        for (const [holderId, heldType] of state.holders) {
          if (holderId !== txnId && heldType !== LockType.SHARED) {
            return false;
          }
        }
        return true;
      }
      // Lock upgrade with multiple holders - need to wait
      return false;
    }

    // Check compatibility with existing holders
    for (const [_, heldType] of state.holders) {
      if (!areLocksCompatible(heldType, requestedType)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Determine the wait reason for the wait-for graph
   */
  function getWaitReason(
    state: LockState,
    txnId: TransactionId,
    requestedType: LockType
  ): 'exclusive' | 'shared' | 'lockUpgrade' {
    const existingLock = state.holders.get(txnId);
    if (existingLock !== undefined) {
      // Already has a lock, so this is an upgrade
      return 'lockUpgrade';
    }
    return requestedType === LockType.SHARED ? 'shared' : 'exclusive';
  }

  /**
   * Process wait queue after a lock release
   */
  function processWaitQueue(resource: string): void {
    const state = locks.get(resource);
    if (!state || state.waitQueue.length === 0) return;

    // Try to grant locks to waiting requests
    const granted: number[] = [];

    for (let i = 0; i < state.waitQueue.length; i++) {
      const waiter = state.waitQueue[i];

      if (canAcquireImmediate(state, waiter.request.txnId, waiter.request.lockType)) {
        // Grant the lock
        state.holders.set(waiter.request.txnId, waiter.request.lockType);

        // Track for transaction
        let txnHeld = txnLocks.get(waiter.request.txnId);
        if (!txnHeld) {
          txnHeld = new Set();
          txnLocks.set(waiter.request.txnId, txnHeld);
        }
        txnHeld.add(resource);

        // Track acquisition time
        let txnTimes = txnLockTimes.get(waiter.request.txnId);
        if (!txnTimes) {
          txnTimes = new Map();
          txnLockTimes.set(waiter.request.txnId, txnTimes);
        }
        txnTimes.set(resource, Date.now());

        // Remove wait edges from deadlock detector
        for (const [holderTxn] of state.holders) {
          if (holderTxn !== waiter.request.txnId) {
            deadlockDetector.removeWait(waiter.request.txnId, holderTxn, resource);
          }
        }

        // Clear timeout
        if (waiter.timeoutId) {
          clearTimeout(waiter.timeoutId);
        }

        // Resolve promise
        waiter.resolve({
          acquired: true,
          grantedType: waiter.request.lockType,
        });

        granted.push(i);

        // For exclusive locks, stop processing queue
        if (
          waiter.request.lockType !== LockType.SHARED &&
          waiter.request.lockType !== LockType.RESERVED
        ) {
          break;
        }
      }
    }

    // Remove granted requests from queue (in reverse to maintain indices)
    for (let i = granted.length - 1; i >= 0; i--) {
      state.waitQueue.splice(granted[i], 1);
    }
  }

  const manager: LockManager = {
    async acquire(request: LockRequest): Promise<LockResult> {
      const state = getLockState(request.resource);

      // Register transaction with deadlock detector
      deadlockDetector.registerTransaction(request.txnId);

      // Check lock ordering prevention (must check even for immediate acquisitions)
      if (deadlockPrevention === 'lockOrdering') {
        const orderingError = deadlockDetector.checkPrevention(
          request.txnId,
          '', // No specific holder for ordering check
          request.resource
        );
        if (orderingError && orderingError.code === TransactionErrorCode.LOCK_FAILED) {
          throw orderingError;
        }
      }

      // Check for immediate acquisition
      if (canAcquireImmediate(state, request.txnId, request.lockType)) {
        state.holders.set(request.txnId, request.lockType);

        // Track for transaction
        let txnHeld = txnLocks.get(request.txnId);
        if (!txnHeld) {
          txnHeld = new Set();
          txnLocks.set(request.txnId, txnHeld);
        }
        txnHeld.add(request.resource);

        // Track acquisition time
        let txnTimes = txnLockTimes.get(request.txnId);
        if (!txnTimes) {
          txnTimes = new Map();
          txnLockTimes.set(request.txnId, txnTimes);
        }
        txnTimes.set(request.resource, Date.now());

        // Record lock acquisition for lock ordering
        deadlockDetector.recordLockAcquisition(request.txnId, request.resource);

        return {
          acquired: true,
          grantedType: request.lockType,
          waitTime: 0,
        };
      }

      // Check deadlock prevention before adding wait edges
      if (deadlockPrevention) {
        for (const [holderTxn] of state.holders) {
          if (holderTxn !== request.txnId) {
            const preventionError = deadlockDetector.checkPrevention(
              request.txnId,
              holderTxn,
              request.resource
            );
            if (preventionError) {
              // Check if this is wound-wait (older wounds younger)
              if ((preventionError as any).woundTarget) {
                // Release the holder's locks and grant to requester
                manager.releaseAll((preventionError as any).woundTarget);

                // Now can acquire immediately
                state.holders.set(request.txnId, request.lockType);
                let txnHeld = txnLocks.get(request.txnId);
                if (!txnHeld) {
                  txnHeld = new Set();
                  txnLocks.set(request.txnId, txnHeld);
                }
                txnHeld.add(request.resource);

                return {
                  acquired: true,
                  grantedType: request.lockType,
                  waitTime: 0,
                };
              }
              throw preventionError;
            }
          }
        }
      }

      // Add wait edges to the deadlock detector graph
      const waitReason = getWaitReason(state, request.txnId, request.lockType);
      for (const [holderTxn] of state.holders) {
        if (holderTxn !== request.txnId) {
          deadlockDetector.addWait(
            request.txnId,
            holderTxn,
            request.resource,
            request.lockType,
            waitReason
          );
        }
      }

      // Check for deadlock using the enhanced detector
      if (detectDeadlocks) {
        const deadlockInfo = deadlockDetector.checkDeadlock(request.txnId);
        if (deadlockInfo) {
          // Cast victimTxnId from external DeadlockInfo to TransactionId
          const victimTxnId = deadlockInfo.victimTxnId as TransactionId;

          // Remove wait edges since we're aborting
          deadlockDetector.removeWait(request.txnId);

          // Auto-release victim's locks
          manager.releaseAll(victimTxnId);

          // If the victim is NOT the requesting transaction, notify the victim in the wait queue
          if (victimTxnId !== request.txnId) {
            // Find and reject the victim's pending request
            for (const [resource, lockState] of locks) {
              const victimWaiter = lockState.waitQueue.find(
                (w) => w.request.txnId === victimTxnId
              );
              if (victimWaiter) {
                // Remove from queue
                const idx = lockState.waitQueue.indexOf(victimWaiter);
                if (idx !== -1) {
                  lockState.waitQueue.splice(idx, 1);
                }
                // Clear timeout
                if (victimWaiter.timeoutId) {
                  clearTimeout(victimWaiter.timeoutId);
                }
                // Reject with deadlock error
                victimWaiter.reject(new DeadlockError(deadlockInfo, victimTxnId));
                break;
              }
            }
            // Remove victim's wait edges
            deadlockDetector.removeWait(victimTxnId);
          }

          throw new DeadlockError(deadlockInfo, request.txnId);
        }
      }

      // Check wait queue size
      if (state.waitQueue.length >= maxWaitQueueSize) {
        deadlockDetector.removeWait(request.txnId);
        return {
          acquired: false,
          blockedBy: [...state.holders.keys()][0],
        };
      }

      // Need to wait
      const timeout = request.timeout ?? defaultTimeout;
      const startTime = Date.now();

      return new Promise((resolve, reject) => {
        const waiter = {
          request,
          resolve: (result: LockResult) => {
            result.waitTime = Date.now() - startTime;
            resolve(result);
          },
          reject,
          timeoutId: undefined as ReturnType<typeof setTimeout> | undefined,
        };

        // Set timeout
        if (timeout > 0 && timeout < Infinity) {
          waiter.timeoutId = setTimeout(() => {
            // Remove from queue
            const idx = state.waitQueue.indexOf(waiter);
            if (idx !== -1) {
              state.waitQueue.splice(idx, 1);
            }

            // Remove wait edges
            deadlockDetector.removeWait(request.txnId);

            reject(
              new TransactionError(
                TransactionErrorCode.LOCK_TIMEOUT,
                `Lock timeout after ${timeout}ms waiting for ${request.resource}`,
                request.txnId
              )
            );
          }, timeout);
        }

        state.waitQueue.push(waiter);
      });
    },

    releaseAll(txnId: TransactionId): void {
      const held = txnLocks.get(txnId);
      if (!held) return;

      // Make a copy since we're modifying during iteration
      const resources = [...held];
      for (const resource of resources) {
        manager.release(txnId, resource);
      }

      txnLocks.delete(txnId);
      txnLockTimes.delete(txnId);

      // Cleanup deadlock detector
      deadlockDetector.unregisterTransaction(txnId);
    },

    release(txnId: TransactionId, resource: string): void {
      const state = locks.get(resource);
      if (!state) return;

      state.holders.delete(txnId);

      // Update transaction tracking
      const txnHeld = txnLocks.get(txnId);
      if (txnHeld) {
        txnHeld.delete(resource);
        if (txnHeld.size === 0) {
          txnLocks.delete(txnId);
        }
      }

      // Update lock times tracking
      const txnTimes = txnLockTimes.get(txnId);
      if (txnTimes) {
        txnTimes.delete(resource);
        if (txnTimes.size === 0) {
          txnLockTimes.delete(txnId);
        }
      }

      // Remove wait edges that were waiting on this transaction for this resource
      for (const waiter of state.waitQueue) {
        deadlockDetector.removeWait(waiter.request.txnId, txnId, resource);
      }

      // Clean up empty lock state
      if (state.holders.size === 0 && state.waitQueue.length === 0) {
        locks.delete(resource);
      } else {
        // Process waiters
        processWaitQueue(resource);
      }
    },

    getHeldLocks(txnId: TransactionId): HeldLock[] {
      const held: HeldLock[] = [];
      const resources = txnLocks.get(txnId);
      const times = txnLockTimes.get(txnId);

      if (resources) {
        for (const resource of resources) {
          const state = locks.get(resource);
          if (state) {
            const lockType = state.holders.get(txnId);
            if (lockType !== undefined) {
              held.push({
                txnId,
                lockType,
                resource,
                acquiredAt: times?.get(resource) ?? Date.now(),
              });
            }
          }
        }
      }

      return held;
    },

    canAcquire(request: LockRequest): boolean {
      const state = getLockState(request.resource);
      return canAcquireImmediate(state, request.txnId, request.lockType);
    },

    getState(): Map<string, { holders: TransactionId[]; waiters: TransactionId[] }> {
      const result = new Map<string, { holders: TransactionId[]; waiters: TransactionId[] }>();

      for (const [resource, state] of locks) {
        result.set(resource, {
          holders: [...state.holders.keys()],
          waiters: state.waitQueue.map((w) => w.request.txnId),
        });
      }

      return result;
    },

    getWaitForGraph(): WaitForGraph {
      return deadlockDetector.getWaitForGraph();
    },

    getDeadlockStats(): DeadlockStats {
      return deadlockDetector.getDeadlockStats();
    },

    getDeadlockHistory(): DeadlockInfo[] {
      return deadlockDetector.getDeadlockHistory();
    },

    setTransactionCost(txnId: TransactionId, cost: number): void {
      deadlockDetector.setTransactionCost(txnId, cost);
    },

    markReadOnly(txnId: TransactionId, readOnly: boolean): void {
      deadlockDetector.markReadOnly(txnId, readOnly);
    },
  };

  return manager;
}

// =============================================================================
// MVCC Snapshot Manager
// =============================================================================

/**
 * MVCC version store for snapshot isolation
 */
export interface MVCCStore {
  /**
   * Get visible version of a row for a snapshot
   * @param table Table name
   * @param key Row key
   * @param snapshot Snapshot to use for visibility
   */
  getVisible(
    table: string,
    key: Uint8Array,
    snapshot: Snapshot
  ): RowVersion | null;

  /**
   * Add a new version
   * @param table Table name
   * @param key Row key
   * @param version New version
   */
  addVersion(table: string, key: Uint8Array, version: RowVersion): void;

  /**
   * Mark a version as deleted
   * @param table Table name
   * @param key Row key
   * @param deletedBy Transaction ID that deleted
   * @param deletedLsn LSN of deletion
   */
  markDeleted(
    table: string,
    key: Uint8Array,
    deletedBy: TransactionId,
    deletedLsn: LSN
  ): void;

  /**
   * Clean up old versions that are no longer needed
   * @param oldestActiveLsn Oldest LSN that might be needed
   */
  vacuum(oldestActiveLsn: LSN): number;
}

/**
 * Key for version lookup (combined table + key)
 */
function makeVersionKey(table: string, key: Uint8Array): string {
  // Simple approach - in production would use more efficient encoding
  const keyHex = Array.from(key)
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
  return `${table}:${keyHex}`;
}

/**
 * Create an MVCC store
 */
export function createMVCCStore(): MVCCStore {
  // Map from composite key to list of versions (newest first)
  const versions = new Map<string, RowVersion[]>();

  return {
    getVisible(
      table: string,
      key: Uint8Array,
      snapshot: Snapshot
    ): RowVersion | null {
      const versionKey = makeVersionKey(table, key);
      const rowVersions = versions.get(versionKey);

      if (!rowVersions || rowVersions.length === 0) {
        return null;
      }

      // Find the newest version visible to this snapshot
      for (const version of rowVersions) {
        // Version must have been created before or at snapshot time
        if (version.createdLsn > snapshot.snapshotLsn) {
          continue;
        }

        // Version creator must not be in active transactions at snapshot time
        // (unless it's our own transaction)
        if (
          version.createdBy !== snapshot.txnId &&
          snapshot.activeTransactions.has(version.createdBy)
        ) {
          continue;
        }

        // If version was deleted, check visibility of deletion
        if (version.deletedBy) {
          // If deletion happened after snapshot, this version is still visible
          if (
            version.deletedLsn !== undefined &&
            version.deletedLsn > snapshot.snapshotLsn
          ) {
            return version;
          }

          // If deleter was active at snapshot time, deletion not visible
          if (snapshot.activeTransactions.has(version.deletedBy)) {
            return version;
          }

          // Deletion is visible, continue to older versions
          continue;
        }

        return version;
      }

      return null;
    },

    addVersion(table: string, key: Uint8Array, version: RowVersion): void {
      const versionKey = makeVersionKey(table, key);
      let rowVersions = versions.get(versionKey);

      if (!rowVersions) {
        rowVersions = [];
        versions.set(versionKey, rowVersions);
      }

      // Insert at beginning (newest first)
      rowVersions.unshift(version);
    },

    markDeleted(
      table: string,
      key: Uint8Array,
      deletedBy: TransactionId,
      deletedLsn: LSN
    ): void {
      const versionKey = makeVersionKey(table, key);
      const rowVersions = versions.get(versionKey);

      if (!rowVersions || rowVersions.length === 0) {
        return;
      }

      // Mark the newest undeleted version
      for (const version of rowVersions) {
        if (!version.deletedBy) {
          version.deletedBy = deletedBy;
          version.deletedLsn = deletedLsn;
          break;
        }
      }
    },

    vacuum(oldestActiveLsn: LSN): number {
      let cleaned = 0;

      for (const [key, rowVersions] of versions) {
        // Keep only versions that might be needed
        const filtered = rowVersions.filter((v) => {
          // Keep if created after oldest active
          if (v.createdLsn >= oldestActiveLsn) {
            return true;
          }

          // Keep if not deleted or deleted after oldest active
          if (!v.deletedBy || (v.deletedLsn && v.deletedLsn >= oldestActiveLsn)) {
            return true;
          }

          cleaned++;
          return false;
        });

        if (filtered.length === 0) {
          versions.delete(key);
        } else if (filtered.length !== rowVersions.length) {
          versions.set(key, filtered);
        }
      }

      return cleaned;
    },
  };
}

// =============================================================================
// Isolation Level Enforcer
// =============================================================================

/**
 * Configuration for isolation enforcement
 */
export interface IsolationEnforcerOptions {
  /** Lock manager instance */
  lockManager: LockManager;
  /** MVCC store (for snapshot isolation) */
  mvccStore?: MVCCStore;
}

/**
 * Isolation level enforcer
 */
export interface IsolationEnforcer {
  /**
   * Prepare for a read operation
   * @param context Transaction context
   * @param table Table being read
   * @param key Optional specific key being read
   */
  prepareRead(
    context: TransactionContext,
    table: string,
    key?: Uint8Array
  ): Promise<void>;

  /**
   * Prepare for a write operation
   * @param context Transaction context
   * @param table Table being written
   * @param key Key being written
   */
  prepareWrite(
    context: TransactionContext,
    table: string,
    key: Uint8Array
  ): Promise<void>;

  /**
   * Validate that a write does not violate isolation
   * @param context Transaction context
   * @param table Table being written
   * @param key Key being written
   */
  validateWrite(
    context: TransactionContext,
    table: string,
    key: Uint8Array
  ): void;

  /**
   * Called when transaction commits
   * @param context Transaction context
   */
  onCommit(context: TransactionContext): void;

  /**
   * Called when transaction rolls back
   * @param context Transaction context
   */
  onRollback(context: TransactionContext): void;
}

/**
 * Create an isolation enforcer
 */
export function createIsolationEnforcer(
  options: IsolationEnforcerOptions
): IsolationEnforcer {
  const { lockManager, mvccStore } = options;

  // Track read sets for serializable validation
  const readSets = new Map<TransactionId, Set<string>>();
  const writeSets = new Map<TransactionId, Set<string>>();

  function getKeyString(key: Uint8Array): string {
    return Array.from(key)
      .map((b) => b.toString(16).padStart(2, '0'))
      .join('');
  }

  return {
    async prepareRead(
      context: TransactionContext,
      table: string,
      key?: Uint8Array
    ): Promise<void> {
      const resource = key ? `${table}:${getKeyString(key)}` : table;

      switch (context.isolationLevel) {
        case IsolationLevel.READ_UNCOMMITTED:
          // No locking needed
          break;

        case IsolationLevel.READ_COMMITTED:
          // Short-term shared lock (released after read)
          await lockManager.acquire({
            txnId: context.txnId,
            lockType: LockType.SHARED,
            resource,
            timestamp: Date.now(),
          });
          // Immediately release for read committed
          lockManager.release(context.txnId, resource);
          break;

        case IsolationLevel.REPEATABLE_READ:
        case IsolationLevel.SERIALIZABLE:
          // Hold shared lock until end of transaction
          const result = await lockManager.acquire({
            txnId: context.txnId,
            lockType: LockType.SHARED,
            resource,
            timestamp: Date.now(),
          });
          if (result.acquired) {
            context.locks.push({
              txnId: context.txnId,
              lockType: LockType.SHARED,
              resource,
              acquiredAt: Date.now(),
            });
          }

          // Track read set for serializable
          if (context.isolationLevel === IsolationLevel.SERIALIZABLE) {
            let readSet = readSets.get(context.txnId);
            if (!readSet) {
              readSet = new Set();
              readSets.set(context.txnId, readSet);
            }
            readSet.add(resource);
          }
          break;

        case IsolationLevel.SNAPSHOT:
          // No locking for reads - use MVCC snapshot
          break;
      }
    },

    async prepareWrite(
      context: TransactionContext,
      table: string,
      key: Uint8Array
    ): Promise<void> {
      const resource = `${table}:${getKeyString(key)}`;

      // All isolation levels need exclusive lock for writes
      const result = await lockManager.acquire({
        txnId: context.txnId,
        lockType: LockType.EXCLUSIVE,
        resource,
        timestamp: Date.now(),
      });

      if (result.acquired) {
        context.locks.push({
          txnId: context.txnId,
          lockType: LockType.EXCLUSIVE,
          resource,
          acquiredAt: Date.now(),
        });
      }

      // Track write set
      let writeSet = writeSets.get(context.txnId);
      if (!writeSet) {
        writeSet = new Set();
        writeSets.set(context.txnId, writeSet);
      }
      writeSet.add(resource);
    },

    validateWrite(
      context: TransactionContext,
      table: string,
      key: Uint8Array
    ): void {
      if (context.isolationLevel !== IsolationLevel.SERIALIZABLE) {
        return;
      }

      const resource = `${table}:${getKeyString(key)}`;

      // For serializable, check if any concurrent transaction read this resource
      for (const [txnId, readSet] of readSets) {
        if (txnId !== context.txnId && readSet.has(resource)) {
          throw new TransactionError(
            TransactionErrorCode.SERIALIZATION_FAILURE,
            `Serialization conflict: transaction ${txnId} read resource ${resource}`,
            context.txnId
          );
        }
      }
    },

    onCommit(context: TransactionContext): void {
      // Release all locks
      lockManager.releaseAll(context.txnId);

      // Clear tracking sets
      readSets.delete(context.txnId);
      writeSets.delete(context.txnId);
    },

    onRollback(context: TransactionContext): void {
      // Release all locks
      lockManager.releaseAll(context.txnId);

      // Clear tracking sets
      readSets.delete(context.txnId);
      writeSets.delete(context.txnId);
    },
  };
}

// =============================================================================
// Visibility Functions for MVCC
// =============================================================================

/**
 * Check if a version is visible to a snapshot
 */
export function isVersionVisible(
  version: RowVersion,
  snapshot: Snapshot
): boolean {
  // Version must have been created before or at snapshot time
  if (version.createdLsn > snapshot.snapshotLsn) {
    return false;
  }

  // Version creator must not be in active transactions at snapshot time
  // (unless it's our own transaction)
  if (
    version.createdBy !== snapshot.txnId &&
    snapshot.activeTransactions.has(version.createdBy)
  ) {
    return false;
  }

  // If version was deleted, check visibility of deletion
  if (version.deletedBy) {
    // If deletion happened after snapshot, this version is still visible
    if (
      version.deletedLsn !== undefined &&
      version.deletedLsn > snapshot.snapshotLsn
    ) {
      return true;
    }

    // If deleter was active at snapshot time, deletion not visible
    if (snapshot.activeTransactions.has(version.deletedBy)) {
      return true;
    }

    // Deletion is visible, version is not
    return false;
  }

  return true;
}

/**
 * Create a snapshot for a transaction
 */
export function createSnapshot(
  txnId: TransactionId,
  currentLsn: LSN,
  activeTransactions: Set<TransactionId>
): Snapshot {
  return {
    txnId,
    snapshotLsn: currentLsn,
    timestamp: Date.now(),
    activeTransactions: new Set(activeTransactions),
  };
}
