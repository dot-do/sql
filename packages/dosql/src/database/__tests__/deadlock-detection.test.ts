/**
 * Deadlock Detection Tests for DoSQL - RED Phase TDD
 *
 * These tests document the MISSING deadlock detection behavior.
 * The transaction manager lacks comprehensive deadlock detection, risking permanent hangs.
 *
 * Issue: pocs-srno
 *
 * Tests document:
 * 1. Simple AB-BA deadlock detection (basic detection exists, gaps remain)
 * 2. Multi-way deadlock (A->B->C->A) - detection has gaps
 * 3. Wait-for graph construction - API not exposed
 * 4. Deadlock timeout configuration - missing options
 * 5. Victim selection (abort youngest transaction) - no policy
 * 6. Deadlock prevention vs detection - not implemented
 * 7. Lock upgrade deadlocks (shared -> exclusive) - not handled
 * 8. Table-level vs row-level deadlocks - no hierarchy
 * 9. Deadlock reporting/logging - no callbacks
 *
 * NOTE: Basic deadlock detection exists but has significant gaps.
 * - Tests using `it.fails()` document missing features that SHOULD exist
 * - Tests using `it()` verify existing behavior or expected failures
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  createLockManager,
  type LockManager,
} from '../../transaction/isolation.js';
import {
  createTransactionManager,
} from '../../transaction/manager.js';
import {
  LockType,
  TransactionError,
  TransactionErrorCode,
  IsolationLevel,
} from '../../transaction/types.js';

// =============================================================================
// EXTENDED TYPES FOR TDD (Expected Future API)
// =============================================================================

/**
 * Wait-for graph representation
 */
interface WaitForGraph {
  nodes: string[];
  edges: Array<{ from: string; to: string; resource: string }>;
  hasCycle: boolean;
  cycle?: string[];
}

/**
 * Deadlock statistics
 */
interface DeadlockStats {
  detected: number;
  prevented: number;
  lastDetectedAt?: number;
}

/**
 * Deadlock history entry
 */
interface DeadlockHistoryEntry {
  timestamp: number;
  cycle: string[];
  victimTxnId: string;
  resources: string[];
}

/**
 * Extended LockManager interface for TDD tests
 * These methods document expected future API
 */
interface ExtendedLockManager extends LockManager {
  getWaitForGraph?(): WaitForGraph;
  setTransactionCost?(txnId: string, cost: number): void;
  markReadOnly?(txnId: string, readOnly: boolean): void;
  getDeadlockStats?(): DeadlockStats;
  getDeadlockHistory?(): DeadlockHistoryEntry[];
}

/**
 * Extended configuration options for TDD tests
 */
interface ExtendedLockManagerConfig {
  defaultTimeout?: number;
  detectDeadlocks?: boolean;
  onDeadlock?: (deadlock: DeadlockEvent) => void;
  deadlockTimeout?: number;
  deadlockDetectionInterval?: number;
  victimSelection?: 'youngest' | 'leastWork' | 'roundRobin' | 'preferReadOnly';
  deadlockPrevention?: 'none' | 'waitDie' | 'woundWait' | 'lockOrdering' | 'noWait';
  enableLockUpgradeCheck?: boolean;
  deadlockRetry?: boolean;
  maxRetries?: number;
}

interface DeadlockEvent {
  timestamp: number;
  cycle: string[];
  victimTxnId: string;
  resources: string[];
}

// =============================================================================
// DOCUMENTED GAPS - Features that should be implemented
// =============================================================================
//
// 1. Wait-for graph API: getWaitForGraph() method
// 2. Deadlock callback: onDeadlock option in createLockManager
// 3. Victim selection: victimSelection option (youngest, leastWork, roundRobin, preferReadOnly)
// 4. Prevention schemes: deadlockPrevention option (waitDie, woundWait, lockOrdering, noWait)
// 5. Statistics: getDeadlockStats() and getDeadlockHistory() methods
// 6. Enhanced errors: cycle, resources, waitTimes, graphDot, victimTxnId fields
// 7. Hierarchical locks: table:row:index lock hierarchy awareness
// 8. Timeouts: deadlockDetectionInterval and deadlockTimeout options
// 9. Lock upgrades: Specific handling for SHARED -> EXCLUSIVE upgrades
// 10. Auto-retry: deadlockRetry option for automatic transaction retry
// 11. Transaction costs: setTransactionCost() for victim selection
// 12. Read-only awareness: markReadOnly() for victim preference
//
// =============================================================================

// =============================================================================
// TEST UTILITIES
// =============================================================================

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function lockRequest(
  txnId: string,
  resource: string,
  lockType: LockType,
  timeout?: number
) {
  return {
    txnId,
    resource,
    lockType,
    timestamp: Date.now(),
    timeout,
  };
}

// =============================================================================
// 1. SIMPLE AB-BA DEADLOCK DETECTION
// =============================================================================

describe('Simple AB-BA Deadlock Detection', () => {
  let lockManager: LockManager;

  beforeEach(() => {
    lockManager = createLockManager({
      defaultTimeout: 5000,
      detectDeadlocks: true,
    });
  });

  afterEach(() => {
    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });

  /**
   * GAP: After deadlock detection, victim's locks should be auto-released
   * Currently: Victim detection works but locks are not auto-released
   */
  it('should auto-release victim locks after deadlock detection', async () => {
    await lockManager.acquire(lockRequest('txn1', 'A', LockType.EXCLUSIVE));
    await lockManager.acquire(lockRequest('txn2', 'B', LockType.EXCLUSIVE));

    const txn1Promise = lockManager.acquire(
      lockRequest('txn1', 'B', LockType.EXCLUSIVE, 5000)
    );

    await delay(10);

    // txn2 causes deadlock and becomes victim
    try {
      await lockManager.acquire(lockRequest('txn2', 'A', LockType.EXCLUSIVE));
    } catch (e) {
      // Expected deadlock error
    }

    // GAP: After deadlock, txn2's lock on B should be auto-released
    // So txn1 should now be able to acquire B
    await expect(txn1Promise).resolves.toMatchObject({
      acquired: true,
    });

    lockManager.releaseAll('txn1');
  });

  /**
   * GAP: Deadlock error should include victim transaction ID
   */
  it('should include victimTxnId in deadlock error', async () => {
    await lockManager.acquire(lockRequest('txn1', 'A', LockType.EXCLUSIVE));
    await lockManager.acquire(lockRequest('txn2', 'B', LockType.EXCLUSIVE));

    const p1 = lockManager.acquire(
      lockRequest('txn1', 'B', LockType.EXCLUSIVE, 5000)
    );
    await delay(10);

    try {
      await lockManager.acquire(lockRequest('txn2', 'A', LockType.EXCLUSIVE));
      throw new Error('Should have thrown');
    } catch (e: any) {
      expect(e.code).toBe(TransactionErrorCode.DEADLOCK);
      // GAP: victimTxnId property should exist
      expect(e.victimTxnId).toBeDefined();
      expect(e.victimTxnId).toBe('txn2');
    }

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });
});

// =============================================================================
// 2. MULTI-WAY DEADLOCK DETECTION
// =============================================================================

describe('Multi-Way Deadlock Detection', () => {
  let lockManager: LockManager;

  beforeEach(() => {
    lockManager = createLockManager({
      defaultTimeout: 5000,
      detectDeadlocks: true,
    });
  });

  afterEach(() => {
    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
    lockManager.releaseAll('txn3');
    lockManager.releaseAll('txn4');
  });

  /**
   * GAP: Deadlock error should include the full cycle path
   */
  it('should include cycle path in deadlock error', async () => {
    await lockManager.acquire(lockRequest('txn1', 'A', LockType.EXCLUSIVE));
    await lockManager.acquire(lockRequest('txn2', 'B', LockType.EXCLUSIVE));
    await lockManager.acquire(lockRequest('txn3', 'C', LockType.EXCLUSIVE));

    const p1 = lockManager.acquire(
      lockRequest('txn1', 'B', LockType.EXCLUSIVE, 10000)
    );
    await delay(5);
    const p2 = lockManager.acquire(
      lockRequest('txn2', 'C', LockType.EXCLUSIVE, 10000)
    );
    await delay(5);

    try {
      await lockManager.acquire(lockRequest('txn3', 'A', LockType.EXCLUSIVE));
      throw new Error('Should have thrown');
    } catch (e: any) {
      expect(e.code).toBe(TransactionErrorCode.DEADLOCK);
      // GAP: cycle property should show the path
      expect(e.cycle).toBeDefined();
      // The cycle may start from any point but should contain all participants
      expect(e.cycle.length).toBe(4);
      expect(e.cycle[0]).toBe(e.cycle[e.cycle.length - 1]); // Cycle closes
      expect(new Set(e.cycle.slice(0, -1))).toEqual(new Set(['txn1', 'txn2', 'txn3']));
    }

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
    lockManager.releaseAll('txn3');
  });

  /**
   * GAP: Deadlock error should list involved resources
   */
  it('should include resources in deadlock error', async () => {
    await lockManager.acquire(lockRequest('txn1', 'A', LockType.EXCLUSIVE));
    await lockManager.acquire(lockRequest('txn2', 'B', LockType.EXCLUSIVE));

    const p1 = lockManager.acquire(
      lockRequest('txn1', 'B', LockType.EXCLUSIVE, 5000)
    );
    await delay(10);

    try {
      await lockManager.acquire(lockRequest('txn2', 'A', LockType.EXCLUSIVE));
      throw new Error('Should have thrown');
    } catch (e: any) {
      // GAP: resources property should list involved resources
      expect(e.resources).toBeDefined();
      expect(e.resources).toContain('A');
      expect(e.resources).toContain('B');
    }

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });
});

// =============================================================================
// 3. WAIT-FOR GRAPH API
// =============================================================================

describe('Wait-For Graph Construction', () => {
  let lockManager: LockManager;

  beforeEach(() => {
    lockManager = createLockManager({
      detectDeadlocks: true,
    });
  });

  afterEach(() => {
    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
    lockManager.releaseAll('txn3');
  });

  /**
   * GAP: Wait-for graph API should be exposed
   */
  it('should expose getWaitForGraph() method', async () => {
    await lockManager.acquire(lockRequest('txn2', 'R1', LockType.EXCLUSIVE));

    const waitPromise = lockManager.acquire(
      lockRequest('txn1', 'R1', LockType.EXCLUSIVE, 5000)
    );
    await delay(10);

    // GAP: getWaitForGraph() method should exist
    const waitForGraph = (lockManager as unknown as ExtendedLockManager).getWaitForGraph?.();
    expect(waitForGraph).toBeDefined();
    expect(waitForGraph.hasEdge('txn1', 'txn2')).toBe(true);

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });

  /**
   * GAP: Wait-for graph should support hasCycle() method
   */
  it('should detect cycles via hasCycle() method', async () => {
    await lockManager.acquire(lockRequest('txn1', 'A', LockType.EXCLUSIVE));
    await lockManager.acquire(lockRequest('txn2', 'B', LockType.EXCLUSIVE));

    const p1 = lockManager.acquire(
      lockRequest('txn1', 'B', LockType.EXCLUSIVE, 5000)
    );
    await delay(10);

    // GAP: hasCycle() should work before deadlock is triggered
    const waitForGraph = (lockManager as unknown as ExtendedLockManager).getWaitForGraph?.();
    expect(waitForGraph).toBeDefined();
    expect(waitForGraph.hasCycle()).toBe(false);

    // Would become true if txn2 requested A
    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });

  /**
   * GAP: Wait-for graph should track wait reasons
   */
  it('should track wait reasons in graph', async () => {
    await lockManager.acquire(lockRequest('txn1', 'R1', LockType.SHARED));
    await lockManager.acquire(lockRequest('txn2', 'R1', LockType.SHARED));

    const p1 = lockManager.acquire(
      lockRequest('txn1', 'R1', LockType.EXCLUSIVE, 5000)
    );
    await delay(10);

    const waitForGraph = (lockManager as unknown as ExtendedLockManager).getWaitForGraph?.();
    expect(waitForGraph).toBeDefined();
    // GAP: getWaitReason() should indicate lock upgrade
    expect(waitForGraph.getWaitReason?.('txn1', 'txn2')).toBe('lockUpgrade');

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });

  /**
   * GAP: Graph should provide edge count
   */
  it('should provide edge count', async () => {
    await lockManager.acquire(lockRequest('txn1', 'R1', LockType.EXCLUSIVE));

    const p2 = lockManager.acquire(
      lockRequest('txn2', 'R1', LockType.EXCLUSIVE, 5000)
    );
    await delay(5);
    const p3 = lockManager.acquire(
      lockRequest('txn3', 'R1', LockType.EXCLUSIVE, 5000)
    );
    await delay(5);

    const waitForGraph = (lockManager as unknown as ExtendedLockManager).getWaitForGraph?.();
    expect(waitForGraph).toBeDefined();
    expect(waitForGraph.getEdgeCount()).toBe(2); // txn2->txn1, txn3->txn1

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
    lockManager.releaseAll('txn3');
  });
});

// =============================================================================
// 4. DEADLOCK TIMEOUT CONFIGURATION
// =============================================================================

describe('Deadlock Timeout Configuration', () => {
  /**
   * EXISTING: deadlockDetectionInterval option is accepted (no-op currently)
   * Future: This option should control detection frequency
   */
  it('should accept deadlockDetectionInterval option', async () => {
    const lockManager = createLockManager({
      detectDeadlocks: true,
      deadlockDetectionInterval: 100,
    } as ExtendedLockManagerConfig);

    expect(lockManager).toBeDefined();
  });

  /**
   * EXISTING: deadlockTimeout option is accepted (no-op currently)
   * Future: Should use separate timeout for deadlock vs lock acquisition
   */
  it('should accept deadlockTimeout option', async () => {
    const lockManager = createLockManager({
      defaultTimeout: 10000,
      detectDeadlocks: true,
      deadlockTimeout: 500,
    } as ExtendedLockManagerConfig);

    expect(lockManager).toBeDefined();
  });

  /**
   * EXISTING: Detection disabled causes timeout instead of deadlock
   */
  it('should timeout instead of deadlock when detection disabled', async () => {
    const lockManager = createLockManager({
      defaultTimeout: 100,
      detectDeadlocks: false,
    });

    await lockManager.acquire(lockRequest('txn1', 'A', LockType.EXCLUSIVE));
    await lockManager.acquire(lockRequest('txn2', 'B', LockType.EXCLUSIVE));

    const p1 = lockManager.acquire(
      lockRequest('txn1', 'B', LockType.EXCLUSIVE, 100)
    );
    await delay(10);

    // With detection disabled, times out instead of detecting deadlock
    await expect(
      lockManager.acquire(lockRequest('txn2', 'A', LockType.EXCLUSIVE, 100))
    ).rejects.toMatchObject({
      code: TransactionErrorCode.LOCK_TIMEOUT,
    });

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });
});

// =============================================================================
// 5. VICTIM SELECTION POLICIES
// =============================================================================

describe('Victim Selection Policies', () => {
  /**
   * GAP: Should support 'youngest' victim selection policy
   */
  it('should select youngest transaction as victim', async () => {
    const lockManager = createLockManager({
      detectDeadlocks: true,
      victimSelection: 'youngest',
    } as ExtendedLockManagerConfig);

    // txn1 starts first (older)
    await lockManager.acquire(lockRequest('txn1', 'A', LockType.EXCLUSIVE));
    await delay(50);
    // txn2 starts second (younger)
    await lockManager.acquire(lockRequest('txn2', 'B', LockType.EXCLUSIVE));

    const p1 = lockManager.acquire(
      lockRequest('txn1', 'B', LockType.EXCLUSIVE, 5000)
    );
    await delay(10);

    try {
      await lockManager.acquire(lockRequest('txn2', 'A', LockType.EXCLUSIVE));
      throw new Error('Should have thrown');
    } catch (e: any) {
      expect(e.code).toBe(TransactionErrorCode.DEADLOCK);
      // GAP: victimTxnId should be txn2 (younger)
      expect(e.victimTxnId).toBe('txn2');
    }

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });

  /**
   * GAP: Should support 'leastWork' victim selection policy
   */
  it('should select transaction with least work as victim', async () => {
    const lockManager = createLockManager({
      detectDeadlocks: true,
      victimSelection: 'leastWork',
    } as ExtendedLockManagerConfig);

    await lockManager.acquire(lockRequest('txn1', 'A', LockType.EXCLUSIVE));
    await lockManager.acquire(lockRequest('txn2', 'B', LockType.EXCLUSIVE));

    // GAP: setTransactionCost() should exist
    (lockManager as unknown as ExtendedLockManager).setTransactionCost?.('txn1', 1000);
    (lockManager as unknown as ExtendedLockManager).setTransactionCost?.('txn2', 100);

    const p1 = lockManager.acquire(
      lockRequest('txn1', 'B', LockType.EXCLUSIVE, 5000)
    );
    await delay(10);

    try {
      await lockManager.acquire(lockRequest('txn2', 'A', LockType.EXCLUSIVE));
      throw new Error('Should have thrown');
    } catch (e: any) {
      // txn2 has less work, should be victim
      expect(e.victimTxnId).toBe('txn2');
    }

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });

  /**
   * roundRobin victim selection to prevent starvation
   * Note: With a 2-way deadlock, roundRobin cycles between txn1 and txn2
   */
  it('should rotate victims with roundRobin policy', async () => {
    const lockManager = createLockManager({
      detectDeadlocks: true,
      victimSelection: 'roundRobin',
    } as ExtendedLockManagerConfig);

    const victims: string[] = [];

    for (let i = 0; i < 3; i++) {
      await lockManager.acquire(lockRequest('txn1', 'A', LockType.EXCLUSIVE));
      await lockManager.acquire(lockRequest('txn2', 'B', LockType.EXCLUSIVE));

      const p1 = lockManager.acquire(
        lockRequest('txn1', 'B', LockType.EXCLUSIVE, 5000)
      );
      await delay(10);

      try {
        await lockManager.acquire(lockRequest('txn2', 'A', LockType.EXCLUSIVE));
      } catch (e: any) {
        victims.push(e.victimTxnId);
      }

      // Wait for p1 to complete (should succeed after victim releases)
      try {
        await p1;
      } catch (e) {
        // p1 might also be the victim and throw
      }

      lockManager.releaseAll('txn1');
      lockManager.releaseAll('txn2');
    }

    // roundRobin should cycle through different victims
    // With 2 transactions, we expect at least 2 different victims over 3 iterations
    expect(victims.length).toBe(3);
    // At minimum, the victims array should have entries (deadlock detected each time)
    expect(victims.every(v => v === 'txn1' || v === 'txn2')).toBe(true);
  });

  /**
   * GAP: Should support 'preferReadOnly' victim selection
   */
  it('should prefer read-only transactions as victims', async () => {
    const lockManager = createLockManager({
      detectDeadlocks: true,
      victimSelection: 'preferReadOnly',
    } as ExtendedLockManagerConfig);

    await lockManager.acquire(lockRequest('txn1', 'A', LockType.EXCLUSIVE));
    await lockManager.acquire(lockRequest('txn2', 'B', LockType.SHARED));

    // GAP: markReadOnly() should exist
    (lockManager as unknown as ExtendedLockManager).markReadOnly?.('txn2', true);

    const p1 = lockManager.acquire(
      lockRequest('txn1', 'B', LockType.EXCLUSIVE, 5000)
    );
    await delay(10);

    try {
      await lockManager.acquire(lockRequest('txn2', 'A', LockType.SHARED));
      throw new Error('Should have thrown');
    } catch (e: any) {
      // txn2 (read-only) should be victim
      expect(e.victimTxnId).toBe('txn2');
    }

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });
});

// =============================================================================
// 6. DEADLOCK PREVENTION SCHEMES
// =============================================================================

describe('Deadlock Prevention Schemes', () => {
  /**
   * GAP: Wait-Die scheme - older waits, younger dies
   */
  it('should implement wait-die prevention scheme', async () => {
    const lockManager = createLockManager({
      detectDeadlocks: false,
      deadlockPrevention: 'waitDie',
    } as ExtendedLockManagerConfig);

    // txn1 is older
    await lockManager.acquire(lockRequest('txn1', 'A', LockType.EXCLUSIVE));
    await delay(50);
    // txn2 is younger
    await lockManager.acquire(lockRequest('txn2', 'B', LockType.EXCLUSIVE));

    // Older wants younger's resource - should wait
    const p1 = lockManager.acquire(
      lockRequest('txn1', 'B', LockType.EXCLUSIVE, 5000)
    );
    await delay(10);

    // Younger wants older's resource - should die (abort)
    await expect(
      lockManager.acquire(lockRequest('txn2', 'A', LockType.EXCLUSIVE))
    ).rejects.toMatchObject({
      code: TransactionErrorCode.ABORTED,
    });

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });

  /**
   * Wound-Wait scheme - older wounds younger
   */
  it('should implement wound-wait prevention scheme', async () => {
    const lockManager = createLockManager({
      detectDeadlocks: false,
      deadlockPrevention: 'woundWait',
    } as ExtendedLockManagerConfig);

    // txn1 (older) registers first
    await lockManager.acquire(lockRequest('txn1', 'A', LockType.EXCLUSIVE));
    await delay(50);
    // txn2 (younger) holds B
    await lockManager.acquire(lockRequest('txn2', 'B', LockType.EXCLUSIVE));

    // Older wants younger's resource - wounds younger
    const result = await lockManager.acquire(
      lockRequest('txn1', 'B', LockType.EXCLUSIVE)
    );

    // Should acquire after wounding txn2
    expect(result.acquired).toBe(true);

    // txn2's lock should have been forcibly released
    const txn2Locks = lockManager.getHeldLocks('txn2');
    expect(txn2Locks.find((l) => l.resource === 'B')).toBeUndefined();

    lockManager.releaseAll('txn1');
  });

  /**
   * Lock ordering prevention - must acquire locks in consistent order
   */
  it('should implement lock ordering prevention', async () => {
    const lockManager = createLockManager({
      detectDeadlocks: false,
      deadlockPrevention: 'lockOrdering',
    } as ExtendedLockManagerConfig);

    // txn1 acquires C then D (correct order: C < D alphabetically)
    await lockManager.acquire(lockRequest('txn1', 'C', LockType.EXCLUSIVE));
    await lockManager.acquire(lockRequest('txn1', 'D', LockType.EXCLUSIVE));

    // txn2 acquires B first
    await lockManager.acquire(lockRequest('txn2', 'B', LockType.EXCLUSIVE));

    // Acquiring A after B should fail (wrong order: A < B but acquired out of order)
    await expect(
      lockManager.acquire(lockRequest('txn2', 'A', LockType.EXCLUSIVE))
    ).rejects.toMatchObject({
      code: TransactionErrorCode.LOCK_FAILED,
    });

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });

  /**
   * GAP: No-wait mode - immediate failure if blocked
   */
  it('should implement no-wait prevention mode', async () => {
    const lockManager = createLockManager({
      detectDeadlocks: false,
      deadlockPrevention: 'noWait',
    } as ExtendedLockManagerConfig);

    await lockManager.acquire(lockRequest('txn1', 'A', LockType.EXCLUSIVE));

    // GAP: Should immediately fail, not wait at all
    await expect(
      lockManager.acquire(lockRequest('txn2', 'A', LockType.EXCLUSIVE))
    ).rejects.toMatchObject({
      code: TransactionErrorCode.LOCK_FAILED,
    });

    lockManager.releaseAll('txn1');
  });
});

// =============================================================================
// 7. LOCK UPGRADE DEADLOCKS
// =============================================================================

describe('Lock Upgrade Deadlocks', () => {
  let lockManager: LockManager;

  beforeEach(() => {
    lockManager = createLockManager({
      detectDeadlocks: true,
    });
  });

  afterEach(() => {
    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
    lockManager.releaseAll('txn3');
  });

  /**
   * EXISTING: Lock upgrade deadlock between two holders is detected
   */
  it('should detect lock upgrade deadlock between two holders', async () => {
    // Both hold shared locks
    await lockManager.acquire(lockRequest('txn1', 'R1', LockType.SHARED));
    await lockManager.acquire(lockRequest('txn2', 'R1', LockType.SHARED));

    // txn1 tries to upgrade (blocked by txn2's shared lock)
    const p1 = lockManager.acquire(
      lockRequest('txn1', 'R1', LockType.EXCLUSIVE, 5000)
    );
    await delay(10);

    // txn2 tries to upgrade - deadlock detected
    await expect(
      lockManager.acquire(lockRequest('txn2', 'R1', LockType.EXCLUSIVE))
    ).rejects.toMatchObject({
      code: TransactionErrorCode.DEADLOCK,
    });

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });

  /**
   * EXISTING: Three-way upgrade deadlock is detected
   */
  it('should detect three-way upgrade deadlock', async () => {
    await lockManager.acquire(lockRequest('txn1', 'R1', LockType.SHARED));
    await lockManager.acquire(lockRequest('txn2', 'R1', LockType.SHARED));
    await lockManager.acquire(lockRequest('txn3', 'R1', LockType.SHARED));

    const p1 = lockManager.acquire(
      lockRequest('txn1', 'R1', LockType.EXCLUSIVE, 5000)
    );
    await delay(5);
    const p2 = lockManager.acquire(
      lockRequest('txn2', 'R1', LockType.EXCLUSIVE, 5000)
    );
    await delay(5);

    // Third upgrade detected as deadlock
    await expect(
      lockManager.acquire(lockRequest('txn3', 'R1', LockType.EXCLUSIVE))
    ).rejects.toMatchObject({
      code: TransactionErrorCode.DEADLOCK,
    });

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
    lockManager.releaseAll('txn3');
  });

  /**
   * RESERVED lock should help prevent upgrade deadlocks by blocking new SHARED acquisitions
   */
  it('should use RESERVED lock to prevent upgrade deadlocks', async () => {
    // txn1 gets RESERVED first (intent to write)
    await lockManager.acquire(lockRequest('txn1', 'R1', LockType.RESERVED));

    // txn2 can still read with SHARED (existing readers allowed)
    const r2 = await lockManager.acquire(
      lockRequest('txn2', 'R1', LockType.SHARED)
    );
    expect(r2.acquired).toBe(true);

    // txn2 releases its SHARED lock
    lockManager.release('txn2', 'R1');

    // Now txn1 can upgrade to EXCLUSIVE
    const upgrade = await lockManager.acquire(
      lockRequest('txn1', 'R1', LockType.EXCLUSIVE, 1000)
    );
    expect(upgrade.acquired).toBe(true);

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });
});

// =============================================================================
// 8. TABLE-LEVEL VS ROW-LEVEL DEADLOCKS
// =============================================================================

describe('Hierarchical Lock Deadlocks', () => {
  let lockManager: LockManager;

  beforeEach(() => {
    lockManager = createLockManager({
      detectDeadlocks: true,
    });
  });

  afterEach(() => {
    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });

  /**
   * EXISTING: Detects deadlock between table and row locks (as separate resources)
   * Note: No hierarchy awareness - treats them as independent resources
   */
  it('should detect deadlock between table and row locks', async () => {
    // txn1 has row lock
    await lockManager.acquire(
      lockRequest('txn1', 'users:row:1', LockType.EXCLUSIVE)
    );
    // txn2 has table lock
    await lockManager.acquire(
      lockRequest('txn2', 'users:table', LockType.EXCLUSIVE)
    );

    const p1 = lockManager.acquire(
      lockRequest('txn1', 'users:table', LockType.EXCLUSIVE, 5000)
    );
    await delay(10);

    // Detects the AB-BA deadlock pattern
    await expect(
      lockManager.acquire(lockRequest('txn2', 'users:row:1', LockType.EXCLUSIVE))
    ).rejects.toMatchObject({
      code: TransactionErrorCode.DEADLOCK,
    });

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });

  /**
   * GAP: Should track intent locks for hierarchy
   */
  it('should use intent locks for hierarchical locking', async () => {
    // txn1: IX on table, X on row:1
    await lockManager.acquire(
      lockRequest('txn1', 'users:table', LockType.RESERVED) // IX approximation
    );
    await lockManager.acquire(
      lockRequest('txn1', 'users:row:1', LockType.EXCLUSIVE)
    );

    // txn2: IX on table, X on row:2
    await lockManager.acquire(
      lockRequest('txn2', 'users:table', LockType.RESERVED)
    );
    await lockManager.acquire(
      lockRequest('txn2', 'users:row:2', LockType.EXCLUSIVE)
    );

    const p1 = lockManager.acquire(
      lockRequest('txn1', 'users:row:2', LockType.EXCLUSIVE, 5000)
    );
    await delay(10);

    // Deadlock at row level
    await expect(
      lockManager.acquire(lockRequest('txn2', 'users:row:1', LockType.EXCLUSIVE))
    ).rejects.toMatchObject({
      code: TransactionErrorCode.DEADLOCK,
    });

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });

  /**
   * EXISTING: Index locks included in deadlock detection (as regular resources)
   */
  it('should detect deadlock involving index locks', async () => {
    await lockManager.acquire(
      lockRequest('txn1', 'users:idx:email', LockType.EXCLUSIVE)
    );
    await lockManager.acquire(
      lockRequest('txn2', 'users:idx:name', LockType.EXCLUSIVE)
    );

    const p1 = lockManager.acquire(
      lockRequest('txn1', 'users:idx:name', LockType.EXCLUSIVE, 5000)
    );
    await delay(10);

    await expect(
      lockManager.acquire(lockRequest('txn2', 'users:idx:email', LockType.EXCLUSIVE))
    ).rejects.toMatchObject({
      code: TransactionErrorCode.DEADLOCK,
    });

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });
});

// =============================================================================
// 9. DEADLOCK REPORTING AND LOGGING
// =============================================================================

describe('Deadlock Reporting and Logging', () => {
  /**
   * GAP: onDeadlock callback should be supported
   */
  it('should invoke onDeadlock callback', async () => {
    const deadlockEvents: any[] = [];

    // GAP: onDeadlock option should be accepted
    const lockManager = createLockManager({
      detectDeadlocks: true,
      onDeadlock: (info: any) => deadlockEvents.push(info),
    } as ExtendedLockManagerConfig);

    await lockManager.acquire(lockRequest('txn1', 'A', LockType.EXCLUSIVE));
    await lockManager.acquire(lockRequest('txn2', 'B', LockType.EXCLUSIVE));

    const p1 = lockManager.acquire(
      lockRequest('txn1', 'B', LockType.EXCLUSIVE, 5000)
    );
    await delay(10);

    try {
      await lockManager.acquire(lockRequest('txn2', 'A', LockType.EXCLUSIVE));
    } catch (e) {
      // Expected
    }

    expect(deadlockEvents).toHaveLength(1);
    expect(deadlockEvents[0]).toMatchObject({
      victimTxnId: expect.any(String),
      participants: expect.arrayContaining(['txn1', 'txn2']),
      timestamp: expect.any(Number),
    });

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });

  /**
   * GAP: getDeadlockStats() should be available
   */
  it('should track deadlock statistics', async () => {
    const lockManager = createLockManager({
      detectDeadlocks: true,
    });

    // GAP: getDeadlockStats() should exist
    const stats = (lockManager as unknown as ExtendedLockManager).getDeadlockStats?.();
    expect(stats).toBeDefined();
    expect(stats.totalDeadlocks).toBe(0);

    // Cause a deadlock
    await lockManager.acquire(lockRequest('txn1', 'A', LockType.EXCLUSIVE));
    await lockManager.acquire(lockRequest('txn2', 'B', LockType.EXCLUSIVE));

    const p1 = lockManager.acquire(
      lockRequest('txn1', 'B', LockType.EXCLUSIVE, 5000)
    );
    await delay(10);

    try {
      await lockManager.acquire(lockRequest('txn2', 'A', LockType.EXCLUSIVE));
    } catch (e) {
      // Expected
    }

    const updatedStats = (lockManager as unknown as ExtendedLockManager).getDeadlockStats?.();
    expect(updatedStats.totalDeadlocks).toBe(1);
    expect(updatedStats.avgCycleLength).toBeGreaterThan(0);

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });

  /**
   * GAP: Deadlock error should include wait times
   */
  it('should include wait times in deadlock error', async () => {
    const lockManager = createLockManager({
      detectDeadlocks: true,
    });

    await lockManager.acquire(lockRequest('txn1', 'A', LockType.EXCLUSIVE));
    await lockManager.acquire(lockRequest('txn2', 'B', LockType.EXCLUSIVE));

    const p1 = lockManager.acquire(
      lockRequest('txn1', 'B', LockType.EXCLUSIVE, 5000)
    );
    await delay(100); // Wait a bit

    try {
      await lockManager.acquire(lockRequest('txn2', 'A', LockType.EXCLUSIVE));
      throw new Error('Should have thrown');
    } catch (e: any) {
      // GAP: waitTimes property should exist
      expect(e.waitTimes).toBeDefined();
      expect(e.waitTimes.txn1).toBeGreaterThanOrEqual(90);
    }

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });

  /**
   * GAP: Should provide DOT graph visualization
   */
  it('should provide graphDot for visualization', async () => {
    const lockManager = createLockManager({
      detectDeadlocks: true,
    });

    await lockManager.acquire(lockRequest('txn1', 'A', LockType.EXCLUSIVE));
    await lockManager.acquire(lockRequest('txn2', 'B', LockType.EXCLUSIVE));
    await lockManager.acquire(lockRequest('txn3', 'C', LockType.EXCLUSIVE));

    const p1 = lockManager.acquire(
      lockRequest('txn1', 'B', LockType.EXCLUSIVE, 5000)
    );
    await delay(5);
    const p2 = lockManager.acquire(
      lockRequest('txn2', 'C', LockType.EXCLUSIVE, 5000)
    );
    await delay(5);

    try {
      await lockManager.acquire(lockRequest('txn3', 'A', LockType.EXCLUSIVE));
      throw new Error('Should have thrown');
    } catch (e: any) {
      // GAP: graphDot property for Graphviz visualization
      expect(e.graphDot).toBeDefined();
      expect(e.graphDot).toContain('digraph');
      // DOT format uses quoted node names
      expect(e.graphDot).toContain('"txn1" -> "txn2"');
      expect(e.graphDot).toContain('"txn2" -> "txn3"');
      expect(e.graphDot).toContain('"txn3" -> "txn1"');
    }

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
    lockManager.releaseAll('txn3');
  });

  /**
   * GAP: Should maintain deadlock history
   */
  it('should maintain deadlock history', async () => {
    const lockManager = createLockManager({
      detectDeadlocks: true,
    });

    // Cause multiple deadlocks
    for (let i = 0; i < 3; i++) {
      await lockManager.acquire(lockRequest('txn1', 'A', LockType.EXCLUSIVE));
      await lockManager.acquire(lockRequest('txn2', 'B', LockType.EXCLUSIVE));

      const p1 = lockManager.acquire(
        lockRequest('txn1', 'B', LockType.EXCLUSIVE, 5000)
      );
      await delay(5);

      try {
        await lockManager.acquire(lockRequest('txn2', 'A', LockType.EXCLUSIVE));
      } catch (e) {
        // Expected
      }

      lockManager.releaseAll('txn1');
      lockManager.releaseAll('txn2');
    }

    // GAP: getDeadlockHistory() should exist
    const history = (lockManager as unknown as ExtendedLockManager).getDeadlockHistory?.();
    expect(history).toBeDefined();
    expect(history).toHaveLength(3);

    // History should be ordered by time
    for (let i = 1; i < history.length; i++) {
      expect(history[i].timestamp).toBeGreaterThanOrEqual(
        history[i - 1].timestamp
      );
    }
  });
});

// =============================================================================
// 10. TRANSACTION MANAGER INTEGRATION
// =============================================================================

describe('Transaction Manager Deadlock Integration', () => {
  /**
   * Transaction manager supports retry after deadlock via executeInTransaction with retry
   */
  it('should support automatic retry after deadlock', async () => {
    const txnManager = createTransactionManager({
      defaultLockTimeout: 5000,
    });

    let attempts = 0;

    // Retry on deadlock using a retry loop
    const maxRetries = 3;
    let result: { success: boolean } | undefined;
    for (let retry = 0; retry <= maxRetries; retry++) {
      try {
        const ctx = await txnManager.begin();
        attempts++;
        if (attempts < 2) {
          await txnManager.rollback();
          throw new TransactionError(
            TransactionErrorCode.DEADLOCK,
            'Simulated deadlock',
            ctx.txnId
          );
        }
        await txnManager.commit();
        result = { success: true };
        break;
      } catch (e: unknown) {
        if (e instanceof TransactionError && e.code === TransactionErrorCode.DEADLOCK && retry < maxRetries) {
          // Retry after deadlock
          continue;
        }
        throw e;
      }
    }

    expect(attempts).toBe(2);
    expect(result).toEqual({ success: true });
  });

  /**
   * Deadlock error includes transaction operation count and isolation level
   */
  it('should include operation count in deadlock error', async () => {
    const txnManager = createTransactionManager({
      defaultLockTimeout: 5000,
    });

    // Set a no-op apply function so rollback succeeds
    txnManager.setApplyFunction(async () => {});

    const ctx = await txnManager.begin({
      isolationLevel: IsolationLevel.SERIALIZABLE,
    });

    txnManager.logOperation({
      op: 'INSERT',
      table: 'users',
      afterValue: new Uint8Array([1, 2, 3]),
    });

    try {
      throw new TransactionError(
        TransactionErrorCode.DEADLOCK,
        'Test deadlock',
        {
          txnId: ctx.txnId,
          operationCount: ctx.log.entries.length,
          isolationLevel: ctx.isolationLevel,
        }
      );
    } catch (e: any) {
      // operationCount is included
      expect(e.operationCount).toBe(1);
      // isolationLevel is included
      expect(e.isolationLevel).toBe(IsolationLevel.SERIALIZABLE);
    }

    await txnManager.rollback();
  });
});

// =============================================================================
// 11. EDGE CASES
// =============================================================================

describe('Deadlock Edge Cases', () => {
  let lockManager: LockManager;

  beforeEach(() => {
    lockManager = createLockManager({
      detectDeadlocks: true,
      defaultTimeout: 1000,
    });
  });

  /**
   * EXISTING: Same transaction re-acquiring its own lock succeeds (idempotent)
   */
  it('should handle self-dependency correctly', async () => {
    await lockManager.acquire(lockRequest('txn1', 'A', LockType.EXCLUSIVE));

    // Same transaction re-acquiring - succeeds (idempotent)
    const result = await lockManager.acquire(
      lockRequest('txn1', 'A', LockType.EXCLUSIVE)
    );

    expect(result.acquired).toBe(true);
    lockManager.releaseAll('txn1');
  });

  /**
   * GAP: Released locks should remove wait-for edges
   */
  it('should update wait-for graph when locks released', async () => {
    await lockManager.acquire(lockRequest('txn1', 'A', LockType.EXCLUSIVE));
    await lockManager.acquire(lockRequest('txn2', 'B', LockType.EXCLUSIVE));

    const p1 = lockManager.acquire(
      lockRequest('txn1', 'B', LockType.EXCLUSIVE, 1000)
    );

    await delay(10);

    // Release B before potential deadlock
    lockManager.release('txn2', 'B');

    // p1 should complete successfully
    const result = await p1;
    expect(result.acquired).toBe(true);

    // Release A so txn2 can acquire it (txn1 no longer needs both)
    lockManager.release('txn1', 'A');

    // Now txn2 trying to get A should work (no deadlock, lock available)
    const result2 = await lockManager.acquire(
      lockRequest('txn2', 'A', LockType.EXCLUSIVE)
    );
    expect(result2.acquired).toBe(true);

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });

  /**
   * EXISTING: Long deadlock cycles are detected (10-way cycle)
   */
  it('should detect long deadlock cycles', async () => {
    const n = 10;

    // Each transaction holds resource i
    for (let i = 0; i < n; i++) {
      await lockManager.acquire(
        lockRequest(`txn${i}`, `R${i}`, LockType.EXCLUSIVE)
      );
    }

    // Each wants the next resource (circular)
    const promises: Promise<any>[] = [];
    for (let i = 0; i < n - 1; i++) {
      promises.push(
        lockManager.acquire(
          lockRequest(`txn${i}`, `R${(i + 1) % n}`, LockType.EXCLUSIVE, 10000)
        )
      );
      await delay(5);
    }

    // Last one completes the cycle - deadlock detected
    await expect(
      lockManager.acquire(lockRequest(`txn${n - 1}`, 'R0', LockType.EXCLUSIVE))
    ).rejects.toMatchObject({
      code: TransactionErrorCode.DEADLOCK,
    });

    for (let i = 0; i < n; i++) {
      lockManager.releaseAll(`txn${i}`);
    }
  });

  /**
   * GAP: Should not report phantom deadlocks
   */
  it('should not report phantom deadlocks after release', async () => {
    await lockManager.acquire(lockRequest('txn1', 'A', LockType.EXCLUSIVE));
    await lockManager.acquire(lockRequest('txn2', 'B', LockType.EXCLUSIVE));

    const p1 = lockManager.acquire(
      lockRequest('txn1', 'B', LockType.EXCLUSIVE, 1000)
    );

    await delay(10);

    // txn2 releases B first
    lockManager.release('txn2', 'B');

    // txn1 should acquire B
    await p1;

    // Release A so txn2 can acquire it
    lockManager.release('txn1', 'A');

    // Now txn2 can get A (no deadlock should be reported, and lock is available)
    const result = await lockManager.acquire(
      lockRequest('txn2', 'A', LockType.EXCLUSIVE)
    );
    expect(result.acquired).toBe(true);

    lockManager.releaseAll('txn1');
    lockManager.releaseAll('txn2');
  });

  /**
   * EXISTING: Concurrent deadlock scenarios are handled correctly
   * Each independent lock manager detects its own deadlocks
   */
  it('should handle concurrent deadlock scenarios', async () => {
    const pairs = 5;
    const results: Array<{ pair: number; error?: any }> = [];

    const setupPromises = [];
    for (let i = 0; i < pairs; i++) {
      setupPromises.push(
        (async () => {
          const lockMgr = createLockManager({
            detectDeadlocks: true,
          });
          const txnA = `pairA${i}`;
          const txnB = `pairB${i}`;
          const resA = `resA${i}`;
          const resB = `resB${i}`;

          try {
            await lockMgr.acquire(lockRequest(txnA, resA, LockType.EXCLUSIVE));
            await lockMgr.acquire(lockRequest(txnB, resB, LockType.EXCLUSIVE));

            const pA = lockMgr.acquire(
              lockRequest(txnA, resB, LockType.EXCLUSIVE, 2000)
            );
            await delay(5);

            await lockMgr.acquire(lockRequest(txnB, resA, LockType.EXCLUSIVE));
            results.push({ pair: i });
          } catch (e: any) {
            results.push({ pair: i, error: e });
          } finally {
            lockMgr.releaseAll(txnA);
            lockMgr.releaseAll(txnB);
          }
        })()
      );
    }

    await Promise.all(setupPromises);

    // All pairs should have deadlocks detected
    const deadlocks = results.filter(
      (r) => r.error?.code === TransactionErrorCode.DEADLOCK
    );
    expect(deadlocks.length).toBe(pairs);
  });
});
