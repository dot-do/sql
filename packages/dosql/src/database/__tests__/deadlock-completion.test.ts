/**
 * Deadlock Detection Completion Tests - RED Phase TDD
 *
 * Issue: sql-zhy.7 - Deadlock Detection Completion
 *
 * These tests document MISSING behavior in the deadlock detector that should exist.
 * Tests use `it.fails()` pattern to document expected behavior not yet implemented.
 *
 * Focus areas:
 * 1. 3-way deadlock detection (A->B->C->A)
 * 2. 4+ way deadlock cycles
 * 3. Victim selection policies (youngest, oldest, least work)
 * 4. Deadlock timeout configuration
 * 5. Automatic victim abort and retry
 * 6. Deadlock statistics and reporting
 * 7. Prevention vs detection modes
 * 8. Lock upgrade deadlocks
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  DeadlockDetector,
  WaitForGraph,
  DeadlockError,
  createDeadlockDetector,
  type DeadlockInfo,
  type VictimSelectionPolicy,
  type DeadlockPreventionScheme,
} from '../deadlock-detector.js';
import { LockType, TransactionErrorCode } from '../../transaction/types.js';

// =============================================================================
// TEST UTILITIES
// =============================================================================

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// =============================================================================
// 1. THREE-WAY DEADLOCK DETECTION (A->B->C->A)
// =============================================================================

describe('Three-Way Deadlock Detection (A->B->C->A)', () => {
  let detector: DeadlockDetector;

  beforeEach(() => {
    detector = createDeadlockDetector({
      enabled: true,
      victimSelection: 'youngest',
    });
  });

  afterEach(() => {
    detector.clear();
  });

  /**
   * GAP: Detector should detect 3-way cycle and return correct cycle path
   */
  it('should detect 3-way deadlock cycle A->B->C->A', () => {
    // Setup 3-way cycle: txn1 waits for txn2, txn2 waits for txn3, txn3 waits for txn1
    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');
    detector.registerTransaction('txn3');

    detector.addWait('txn1', 'txn2', 'resourceA', LockType.EXCLUSIVE);
    detector.addWait('txn2', 'txn3', 'resourceB', LockType.EXCLUSIVE);
    detector.addWait('txn3', 'txn1', 'resourceC', LockType.EXCLUSIVE);

    const deadlock = detector.checkDeadlock('txn3');

    expect(deadlock).not.toBeNull();
    expect(deadlock!.cycle.length).toBe(4); // Cycle closes: [txn3, txn1, txn2, txn3]
    expect(deadlock!.participants).toContain('txn1');
    expect(deadlock!.participants).toContain('txn2');
    expect(deadlock!.participants).toContain('txn3');
  });

  /**
   * GAP: 3-way deadlock should include all 3 resources in the cycle
   */
  it('should include all 3 resources in 3-way deadlock', () => {
    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');
    detector.registerTransaction('txn3');

    detector.addWait('txn1', 'txn2', 'table:users', LockType.EXCLUSIVE);
    detector.addWait('txn2', 'txn3', 'table:orders', LockType.EXCLUSIVE);
    detector.addWait('txn3', 'txn1', 'table:products', LockType.EXCLUSIVE);

    const deadlock = detector.checkDeadlock('txn3');

    expect(deadlock).not.toBeNull();
    expect(deadlock!.resources).toContain('table:users');
    expect(deadlock!.resources).toContain('table:orders');
    expect(deadlock!.resources).toContain('table:products');
  });

  /**
   * GAP: 3-way deadlock should select correct victim based on policy
   */
  it('should select youngest victim in 3-way deadlock', async () => {
    const detector = createDeadlockDetector({
      enabled: true,
      victimSelection: 'youngest',
    });

    // txn1 is oldest
    detector.registerTransaction('txn1');
    await delay(10);
    // txn2 is middle
    detector.registerTransaction('txn2');
    await delay(10);
    // txn3 is youngest
    detector.registerTransaction('txn3');

    detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn2', 'txn3', 'B', LockType.EXCLUSIVE);
    detector.addWait('txn3', 'txn1', 'C', LockType.EXCLUSIVE);

    const deadlock = detector.checkDeadlock('txn3');

    expect(deadlock).not.toBeNull();
    expect(deadlock!.victimTxnId).toBe('txn3'); // Youngest should be victim
  });
});

// =============================================================================
// 2. FOUR+ WAY DEADLOCK CYCLES
// =============================================================================

describe('Four+ Way Deadlock Cycles', () => {
  let detector: DeadlockDetector;

  beforeEach(() => {
    detector = createDeadlockDetector({
      enabled: true,
      victimSelection: 'youngest',
    });
  });

  afterEach(() => {
    detector.clear();
  });

  /**
   * GAP: Detector should detect 4-way deadlock cycle
   */
  it('should detect 4-way deadlock cycle', () => {
    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');
    detector.registerTransaction('txn3');
    detector.registerTransaction('txn4');

    detector.addWait('txn1', 'txn2', 'R1', LockType.EXCLUSIVE);
    detector.addWait('txn2', 'txn3', 'R2', LockType.EXCLUSIVE);
    detector.addWait('txn3', 'txn4', 'R3', LockType.EXCLUSIVE);
    detector.addWait('txn4', 'txn1', 'R4', LockType.EXCLUSIVE);

    const deadlock = detector.checkDeadlock('txn4');

    expect(deadlock).not.toBeNull();
    expect(deadlock!.cycle.length).toBe(5); // 4 nodes + closing
    expect(deadlock!.participants.length).toBe(4);
  });

  /**
   * GAP: Detector should detect 5-way deadlock cycle
   */
  it('should detect 5-way deadlock cycle', () => {
    for (let i = 1; i <= 5; i++) {
      detector.registerTransaction(`txn${i}`);
    }

    detector.addWait('txn1', 'txn2', 'R1', LockType.EXCLUSIVE);
    detector.addWait('txn2', 'txn3', 'R2', LockType.EXCLUSIVE);
    detector.addWait('txn3', 'txn4', 'R3', LockType.EXCLUSIVE);
    detector.addWait('txn4', 'txn5', 'R4', LockType.EXCLUSIVE);
    detector.addWait('txn5', 'txn1', 'R5', LockType.EXCLUSIVE);

    const deadlock = detector.checkDeadlock('txn5');

    expect(deadlock).not.toBeNull();
    expect(deadlock!.participants.length).toBe(5);
  });

  /**
   * GAP: Should detect cycle even with non-participating transactions in graph
   */
  it('should detect cycle with bystander transactions in graph', () => {
    // Bystanders that don't form a cycle
    detector.registerTransaction('bystander1');
    detector.registerTransaction('bystander2');

    // Actual cycle participants
    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');
    detector.registerTransaction('txn3');
    detector.registerTransaction('txn4');

    // Bystander waits (no cycle)
    detector.addWait('bystander1', 'txn1', 'X', LockType.SHARED);
    detector.addWait('bystander2', 'txn2', 'Y', LockType.SHARED);

    // Actual cycle
    detector.addWait('txn1', 'txn2', 'R1', LockType.EXCLUSIVE);
    detector.addWait('txn2', 'txn3', 'R2', LockType.EXCLUSIVE);
    detector.addWait('txn3', 'txn4', 'R3', LockType.EXCLUSIVE);
    detector.addWait('txn4', 'txn1', 'R4', LockType.EXCLUSIVE);

    const deadlock = detector.checkDeadlock('txn4');

    expect(deadlock).not.toBeNull();
    // Bystanders should NOT be in the cycle
    expect(deadlock!.participants).not.toContain('bystander1');
    expect(deadlock!.participants).not.toContain('bystander2');
    expect(deadlock!.participants.length).toBe(4);
  });

  /**
   * GAP: Should handle very long cycles (10+ transactions)
   */
  it('should detect 10-way deadlock cycle', () => {
    const n = 10;
    for (let i = 0; i < n; i++) {
      detector.registerTransaction(`txn${i}`);
    }

    // Create circular wait: txn0 -> txn1 -> ... -> txn9 -> txn0
    for (let i = 0; i < n; i++) {
      const next = (i + 1) % n;
      detector.addWait(`txn${i}`, `txn${next}`, `R${i}`, LockType.EXCLUSIVE);
    }

    const deadlock = detector.checkDeadlock(`txn${n - 1}`);

    expect(deadlock).not.toBeNull();
    expect(deadlock!.participants.length).toBe(n);
  });
});

// =============================================================================
// 3. VICTIM SELECTION POLICIES
// =============================================================================

describe('Victim Selection Policies', () => {
  /**
   * GAP: 'youngest' policy should select transaction with latest start time
   */
  it('should select youngest transaction as victim', async () => {
    const detector = createDeadlockDetector({
      enabled: true,
      victimSelection: 'youngest',
    });

    // Register in order with delays to establish age
    detector.registerTransaction('txn_old');
    await delay(20);
    detector.registerTransaction('txn_mid');
    await delay(20);
    detector.registerTransaction('txn_young');

    detector.addWait('txn_old', 'txn_mid', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn_mid', 'txn_young', 'B', LockType.EXCLUSIVE);
    detector.addWait('txn_young', 'txn_old', 'C', LockType.EXCLUSIVE);

    const deadlock = detector.checkDeadlock('txn_young');

    expect(deadlock).not.toBeNull();
    expect(deadlock!.victimTxnId).toBe('txn_young');
  });

  /**
   * GAP: 'leastWork' policy should select transaction with lowest cost
   */
  it('should select transaction with least work as victim', () => {
    const detector = createDeadlockDetector({
      enabled: true,
      victimSelection: 'leastWork',
    });

    detector.registerTransaction('txn_highcost');
    detector.registerTransaction('txn_midcost');
    detector.registerTransaction('txn_lowcost');

    detector.setTransactionCost('txn_highcost', 1000);
    detector.setTransactionCost('txn_midcost', 500);
    detector.setTransactionCost('txn_lowcost', 10);

    detector.addWait('txn_highcost', 'txn_midcost', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn_midcost', 'txn_lowcost', 'B', LockType.EXCLUSIVE);
    detector.addWait('txn_lowcost', 'txn_highcost', 'C', LockType.EXCLUSIVE);

    const deadlock = detector.checkDeadlock('txn_lowcost');

    expect(deadlock).not.toBeNull();
    expect(deadlock!.victimTxnId).toBe('txn_lowcost'); // Least work = victim
  });

  /**
   * GAP: 'roundRobin' policy should cycle through victims
   */
  it('should rotate victims with roundRobin policy', () => {
    const detector = createDeadlockDetector({
      enabled: true,
      victimSelection: 'roundRobin',
    });

    const victims: string[] = [];

    // Trigger multiple deadlocks and track victims
    for (let i = 0; i < 4; i++) {
      detector.clear();
      detector.registerTransaction('txn1');
      detector.registerTransaction('txn2');

      detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE);
      detector.addWait('txn2', 'txn1', 'B', LockType.EXCLUSIVE);

      const deadlock = detector.checkDeadlock('txn2');
      if (deadlock) {
        victims.push(deadlock.victimTxnId);
      }
    }

    expect(victims.length).toBe(4);
    // Round-robin should cycle between txn1 and txn2
    expect(victims.filter((v) => v === 'txn1').length).toBeGreaterThan(0);
    expect(victims.filter((v) => v === 'txn2').length).toBeGreaterThan(0);
  });

  /**
   * GAP: 'preferReadOnly' policy should prefer read-only transactions as victims
   */
  it('should prefer read-only transactions as victims', () => {
    const detector = createDeadlockDetector({
      enabled: true,
      victimSelection: 'preferReadOnly',
    });

    detector.registerTransaction('txn_writer');
    detector.registerTransaction('txn_reader');

    detector.markReadOnly('txn_reader', true);
    detector.markReadOnly('txn_writer', false);

    detector.addWait('txn_writer', 'txn_reader', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn_reader', 'txn_writer', 'B', LockType.SHARED);

    const deadlock = detector.checkDeadlock('txn_reader');

    expect(deadlock).not.toBeNull();
    expect(deadlock!.victimTxnId).toBe('txn_reader'); // Read-only preferred as victim
  });

  /**
   * When no read-only in cycle, 'preferReadOnly' should fall back to youngest
   * This now passes - implementation correctly falls back
   */
  it('should fall back to youngest when no read-only transactions', async () => {
    const detector = createDeadlockDetector({
      enabled: true,
      victimSelection: 'preferReadOnly',
    });

    // txn_old is registered first
    detector.registerTransaction('txn_old');
    await delay(10);
    // txn_young is registered second
    detector.registerTransaction('txn_young');

    // Both are writers
    detector.markReadOnly('txn_old', false);
    detector.markReadOnly('txn_young', false);

    detector.addWait('txn_old', 'txn_young', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn_young', 'txn_old', 'B', LockType.EXCLUSIVE);

    const deadlock = detector.checkDeadlock('txn_young');

    expect(deadlock).not.toBeNull();
    // Falls back to youngest selection
    expect(deadlock!.victimTxnId).toBe('txn_young');
  });
});

// =============================================================================
// 4. DEADLOCK TIMEOUT CONFIGURATION
// =============================================================================

describe('Deadlock Timeout Configuration', () => {
  /**
   * GAP: Detector should accept deadlockTimeout option
   */
  it('should accept deadlockTimeout configuration', () => {
    const detector = createDeadlockDetector({
      enabled: true,
      deadlockTimeout: 500,
    });

    expect(detector).toBeDefined();
  });

  /**
   * GAP: Detector should accept deadlockDetectionInterval option
   */
  it('should accept deadlockDetectionInterval configuration', () => {
    const detector = createDeadlockDetector({
      enabled: true,
      deadlockDetectionInterval: 100,
    });

    expect(detector).toBeDefined();
  });

  /**
   * GAP: Long-waiting transactions should be flagged in deadlock info
   */
  it.fails('should flag transactions waiting longer than timeout', async () => {
    const detector = createDeadlockDetector({
      enabled: true,
      deadlockTimeout: 50,
    });

    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');

    detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE);

    // Wait longer than timeout
    await delay(100);

    detector.addWait('txn2', 'txn1', 'B', LockType.EXCLUSIVE);

    const deadlock = detector.checkDeadlock('txn2');

    expect(deadlock).not.toBeNull();
    // GAP: Should indicate which transactions exceeded timeout
    expect((deadlock as any).exceededTimeout).toBeDefined();
    expect((deadlock as any).exceededTimeout).toContain('txn1');
  });

  /**
   * GAP: Detection should be disabled when enabled=false
   */
  it('should not detect deadlocks when disabled', () => {
    const detector = createDeadlockDetector({
      enabled: false,
    });

    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');

    detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn2', 'txn1', 'B', LockType.EXCLUSIVE);

    const deadlock = detector.checkDeadlock('txn2');

    expect(deadlock).toBeNull(); // Detection disabled
  });
});

// =============================================================================
// 5. AUTOMATIC VICTIM ABORT AND RETRY
// =============================================================================

describe('Automatic Victim Abort and Retry', () => {
  /**
   * GAP: DeadlockError should have all necessary context
   */
  it('should create DeadlockError with full context', () => {
    const info: DeadlockInfo = {
      cycle: ['txn1', 'txn2', 'txn1'],
      resources: ['A', 'B'],
      victimTxnId: 'txn2',
      waitTimes: { txn1: 100, txn2: 50 },
      timestamp: Date.now(),
      graphDot: 'digraph {}',
      participants: ['txn1', 'txn2'],
    };

    const error = new DeadlockError(info, 'txn2');

    expect(error.code).toBe(TransactionErrorCode.DEADLOCK);
    expect(error.victimTxnId).toBe('txn2');
    expect(error.cycle).toEqual(['txn1', 'txn2', 'txn1']);
    expect(error.resources).toContain('A');
    expect(error.resources).toContain('B');
    expect(error.waitTimes).toEqual({ txn1: 100, txn2: 50 });
    expect(error.graphDot).toBe('digraph {}');
    expect(error.participants).toContain('txn1');
    expect(error.participants).toContain('txn2');
  });

  /**
   * GAP: onDeadlock callback should be invoked when deadlock detected
   */
  it('should invoke onDeadlock callback', () => {
    const deadlockEvents: DeadlockInfo[] = [];

    const detector = createDeadlockDetector({
      enabled: true,
      onDeadlock: (info) => deadlockEvents.push(info),
    });

    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');

    detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn2', 'txn1', 'B', LockType.EXCLUSIVE);

    detector.checkDeadlock('txn2');

    expect(deadlockEvents.length).toBe(1);
    expect(deadlockEvents[0].victimTxnId).toBeDefined();
  });

  /**
   * GAP: Removing victim should clear wait-for graph edges
   */
  it('should clear victim edges after deadlock resolution', () => {
    const detector = createDeadlockDetector({ enabled: true });
    const graph = detector.getWaitForGraph();

    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');

    detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn2', 'txn1', 'B', LockType.EXCLUSIVE);

    expect(graph.getEdgeCount()).toBe(2);

    const deadlock = detector.checkDeadlock('txn2');
    expect(deadlock).not.toBeNull();

    // Simulate victim abort - remove victim from graph
    detector.unregisterTransaction(deadlock!.victimTxnId);

    // Graph should no longer have a cycle
    expect(graph.hasCycle()).toBe(false);
  });

  /**
   * GAP: Should support automatic retry configuration
   */
  it.fails('should support automatic retry after deadlock abort', async () => {
    const detector = createDeadlockDetector({
      enabled: true,
      // GAP: autoRetry option should exist
      autoRetry: {
        enabled: true,
        maxRetries: 3,
        baseBackoffMs: 10,
      },
    } as any);

    // GAP: getRetryConfig() should return retry settings
    const retryConfig = (detector as any).getRetryConfig?.();
    expect(retryConfig).toBeDefined();
    expect(retryConfig.enabled).toBe(true);
    expect(retryConfig.maxRetries).toBe(3);
  });
});

// =============================================================================
// 6. DEADLOCK STATISTICS AND REPORTING
// =============================================================================

describe('Deadlock Statistics and Reporting', () => {
  /**
   * GAP: Stats should persist across clear() - currently clear() resets stats
   * This documents the gap that stats are lost when graph is cleared
   */
  it.fails('should persist total deadlocks across clear() calls', () => {
    const detector = createDeadlockDetector({ enabled: true });

    // Cause 3 deadlocks with clear between them
    for (let i = 0; i < 3; i++) {
      detector.registerTransaction('txn1');
      detector.registerTransaction('txn2');

      detector.addWait('txn1', 'txn2', `A${i}`, LockType.EXCLUSIVE);
      detector.addWait('txn2', 'txn1', `B${i}`, LockType.EXCLUSIVE);

      detector.checkDeadlock('txn2');
      detector.clear(); // GAP: This resets stats!
    }

    const stats = detector.getDeadlockStats();

    // GAP: Stats should persist even after clear()
    expect(stats.totalDeadlocks).toBe(3);
  });

  /**
   * Should track total deadlocks WITHOUT clearing between
   */
  it('should track total deadlocks in statistics (without clear)', () => {
    const detector = createDeadlockDetector({ enabled: true });

    // Cause a deadlock
    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');
    detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn2', 'txn1', 'B', LockType.EXCLUSIVE);
    detector.checkDeadlock('txn2');

    const stats = detector.getDeadlockStats();

    expect(stats.totalDeadlocks).toBe(1);
  });

  /**
   * GAP: Stats should persist - currently clear() resets average cycle length
   */
  it.fails('should track average cycle length across clear() calls', () => {
    const detector = createDeadlockDetector({ enabled: true });

    // 2-way deadlock
    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');
    detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn2', 'txn1', 'B', LockType.EXCLUSIVE);
    detector.checkDeadlock('txn2');
    detector.clear(); // GAP: Resets stats

    // 3-way deadlock
    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');
    detector.registerTransaction('txn3');
    detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn2', 'txn3', 'B', LockType.EXCLUSIVE);
    detector.addWait('txn3', 'txn1', 'C', LockType.EXCLUSIVE);
    detector.checkDeadlock('txn3');

    const stats = detector.getDeadlockStats();

    // GAP: Should track both deadlocks
    expect(stats.totalDeadlocks).toBe(2);
    expect(stats.avgCycleLength).toBe(2.5); // (2 + 3) / 2
  });

  /**
   * GAP: Should track victims aborted by policy
   */
  it('should track victims aborted by policy', () => {
    const detector = createDeadlockDetector({
      enabled: true,
      victimSelection: 'youngest',
    });

    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');
    detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn2', 'txn1', 'B', LockType.EXCLUSIVE);
    detector.checkDeadlock('txn2');

    const stats = detector.getDeadlockStats();

    expect(stats.victimsAborted.youngest).toBe(1);
    expect(stats.victimsAborted.leastWork).toBe(0);
  });

  /**
   * GAP: History should persist across clear() - currently clear() resets history
   */
  it.fails('should maintain deadlock history across clear() calls', () => {
    const detector = createDeadlockDetector({
      enabled: true,
      maxHistorySize: 100,
    });

    // Cause multiple deadlocks with clear between
    for (let i = 0; i < 5; i++) {
      detector.registerTransaction('txn1');
      detector.registerTransaction('txn2');
      detector.addWait('txn1', 'txn2', `A${i}`, LockType.EXCLUSIVE);
      detector.addWait('txn2', 'txn1', `B${i}`, LockType.EXCLUSIVE);
      detector.checkDeadlock('txn2');
      detector.clear(); // GAP: This resets history!
    }

    const history = detector.getDeadlockHistory();

    // GAP: History should persist even after clear()
    expect(history.length).toBe(5);
    // History should be ordered by timestamp
    for (let i = 1; i < history.length; i++) {
      expect(history[i].timestamp).toBeGreaterThanOrEqual(history[i - 1].timestamp);
    }
  });

  /**
   * Should maintain history without clear() between deadlocks
   */
  it('should maintain deadlock history (without clear)', () => {
    const detector = createDeadlockDetector({
      enabled: true,
      maxHistorySize: 100,
    });

    // Cause single deadlock
    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');
    detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn2', 'txn1', 'B', LockType.EXCLUSIVE);
    detector.checkDeadlock('txn2');

    const history = detector.getDeadlockHistory();

    expect(history.length).toBe(1);
    expect(history[0].cycle).toBeDefined();
  });

  /**
   * GAP: maxHistorySize should persist across clear() calls
   */
  it.fails('should respect maxHistorySize limit across clear() calls', () => {
    const detector = createDeadlockDetector({
      enabled: true,
      maxHistorySize: 3,
    });

    // Cause 5 deadlocks with clear between
    for (let i = 0; i < 5; i++) {
      detector.registerTransaction('txn1');
      detector.registerTransaction('txn2');
      detector.addWait('txn1', 'txn2', `A${i}`, LockType.EXCLUSIVE);
      detector.addWait('txn2', 'txn1', `B${i}`, LockType.EXCLUSIVE);
      detector.checkDeadlock('txn2');
      detector.clear(); // GAP: Resets history
    }

    const history = detector.getDeadlockHistory();

    // GAP: Should keep last 3 entries
    expect(history.length).toBe(3);
  });

  /**
   * GAP: Should provide DOT graph visualization
   */
  it('should provide DOT graph in deadlock info', () => {
    const detector = createDeadlockDetector({ enabled: true });

    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');
    detector.registerTransaction('txn3');

    detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn2', 'txn3', 'B', LockType.EXCLUSIVE);
    detector.addWait('txn3', 'txn1', 'C', LockType.EXCLUSIVE);

    const deadlock = detector.checkDeadlock('txn3');

    expect(deadlock).not.toBeNull();
    expect(deadlock!.graphDot).toContain('digraph');
    expect(deadlock!.graphDot).toContain('"txn1"');
    expect(deadlock!.graphDot).toContain('"txn2"');
    expect(deadlock!.graphDot).toContain('"txn3"');
  });
});

// =============================================================================
// 7. PREVENTION VS DETECTION MODES
// =============================================================================

describe('Prevention vs Detection Modes', () => {
  /**
   * GAP: Wait-Die scheme - older waits, younger dies
   */
  it('should implement wait-die prevention scheme', () => {
    const detector = createDeadlockDetector({
      enabled: true,
      deadlockPrevention: 'waitDie',
    });

    // txn_old is older (registered first)
    detector.registerTransaction('txn_old');
    // txn_young is younger
    detector.registerTransaction('txn_young');

    // Younger wants older's resource - should die
    const error = detector.checkPrevention('txn_young', 'txn_old', 'R1');

    expect(error).not.toBeNull();
    expect(error!.code).toBe(TransactionErrorCode.ABORTED);
  });

  /**
   * Wait-Die should allow older to wait for younger
   * Implementation is correct - older can wait for younger
   */
  it('should allow older to wait for younger in wait-die', async () => {
    const detector = createDeadlockDetector({
      enabled: true,
      deadlockPrevention: 'waitDie',
    });

    // txn_old is registered first (older timestamp)
    detector.registerTransaction('txn_old');
    await delay(10); // Ensure time difference
    // txn_young is registered second (younger timestamp)
    detector.registerTransaction('txn_young');

    // In Wait-Die: older waits for younger, younger dies when waiting for older
    // Older (txn_old) wants younger's (txn_young) resource - should be allowed to wait
    const error = detector.checkPrevention('txn_old', 'txn_young', 'R1');

    expect(error).toBeNull(); // Older should be allowed to wait
  });

  /**
   * Wound-Wait scheme - older wounds younger
   * Implementation is correct - returns error targeting holder
   */
  it('should implement wound-wait prevention scheme', async () => {
    const detector = createDeadlockDetector({
      enabled: true,
      deadlockPrevention: 'woundWait',
    });

    // txn_old is registered first (older timestamp)
    detector.registerTransaction('txn_old');
    await delay(10);
    // txn_young is registered second (younger timestamp)
    detector.registerTransaction('txn_young');

    // In Wound-Wait: older wounds younger (forces younger to abort)
    // Older wants younger's resource - should wound younger
    const error = detector.checkPrevention('txn_old', 'txn_young', 'R1');

    expect(error).not.toBeNull();
    // Wound-wait returns error targeting the holder (younger)
    expect((error as any).woundTarget).toBe('txn_young');
  });

  /**
   * GAP: Wound-Wait should allow younger to wait for older
   * Currently not documented as a distinct test
   */
  it('should allow younger to wait for older in wound-wait', async () => {
    const detector = createDeadlockDetector({
      enabled: true,
      deadlockPrevention: 'woundWait',
    });

    detector.registerTransaction('txn_old');
    await delay(10);
    detector.registerTransaction('txn_young');

    // In Wound-Wait: younger waits for older
    const error = detector.checkPrevention('txn_young', 'txn_old', 'R1');

    expect(error).toBeNull(); // Younger allowed to wait
  });

  /**
   * GAP: No-Wait mode - immediate failure
   */
  it('should implement no-wait prevention mode', () => {
    const detector = createDeadlockDetector({
      enabled: true,
      deadlockPrevention: 'noWait',
    });

    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');

    // Any wait should fail immediately
    const error = detector.checkPrevention('txn1', 'txn2', 'R1');

    expect(error).not.toBeNull();
    expect(error!.code).toBe(TransactionErrorCode.LOCK_FAILED);
  });

  /**
   * GAP: Lock ordering prevention - enforce acquisition order
   */
  it('should implement lock ordering prevention', () => {
    const detector = createDeadlockDetector({
      enabled: true,
      deadlockPrevention: 'lockOrdering',
    });

    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');

    // Record that txn1 has lock on 'B'
    detector.recordLockAcquisition('txn1', 'B');

    // Trying to acquire 'A' after 'B' violates ordering (A < B alphabetically)
    const error = detector.checkPrevention('txn1', 'txn2', 'A');

    expect(error).not.toBeNull();
    expect(error!.code).toBe(TransactionErrorCode.LOCK_FAILED);
  });

  /**
   * GAP: Should track prevention activations in stats
   */
  it('should track prevention activations', () => {
    const detector = createDeadlockDetector({
      enabled: true,
      deadlockPrevention: 'noWait',
    });

    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');

    detector.checkPrevention('txn1', 'txn2', 'R1');
    detector.checkPrevention('txn1', 'txn2', 'R2');

    const stats = detector.getDeadlockStats();

    expect(stats.preventionActivations).toBe(2);
  });
});

// =============================================================================
// 8. LOCK UPGRADE DEADLOCKS
// =============================================================================

describe('Lock Upgrade Deadlocks', () => {
  let detector: DeadlockDetector;

  beforeEach(() => {
    detector = createDeadlockDetector({ enabled: true });
  });

  afterEach(() => {
    detector.clear();
  });

  /**
   * GAP: Should detect upgrade deadlock with 'lockUpgrade' reason
   */
  it('should detect lock upgrade deadlock between two holders', () => {
    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');

    // Both hold shared, both want to upgrade
    detector.addWait('txn1', 'txn2', 'R1', LockType.EXCLUSIVE, 'lockUpgrade');
    detector.addWait('txn2', 'txn1', 'R1', LockType.EXCLUSIVE, 'lockUpgrade');

    const deadlock = detector.checkDeadlock('txn2');

    expect(deadlock).not.toBeNull();
    expect(deadlock!.resources).toContain('R1');
  });

  /**
   * GAP: Wait-for graph should track lockUpgrade reason
   */
  it('should track lockUpgrade wait reason in graph', () => {
    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');

    detector.addWait('txn1', 'txn2', 'R1', LockType.EXCLUSIVE, 'lockUpgrade');

    const graph = detector.getWaitForGraph();
    const reason = graph.getWaitReason('txn1', 'txn2');

    expect(reason).toBe('lockUpgrade');
  });

  /**
   * GAP: Should distinguish upgrade deadlocks in statistics
   */
  it.fails('should track upgrade deadlocks separately in stats', () => {
    const detector = createDeadlockDetector({ enabled: true });

    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');

    detector.addWait('txn1', 'txn2', 'R1', LockType.EXCLUSIVE, 'lockUpgrade');
    detector.addWait('txn2', 'txn1', 'R1', LockType.EXCLUSIVE, 'lockUpgrade');

    detector.checkDeadlock('txn2');

    const stats = detector.getDeadlockStats();

    // GAP: Should track upgrade deadlocks separately
    expect((stats as any).upgradeDeadlocks).toBe(1);
  });

  /**
   * GAP: Three-way upgrade deadlock on same resource
   */
  it('should detect three-way upgrade deadlock', () => {
    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');
    detector.registerTransaction('txn3');

    // All three hold shared on R1, all want to upgrade
    detector.addWait('txn1', 'txn2', 'R1', LockType.EXCLUSIVE, 'lockUpgrade');
    detector.addWait('txn2', 'txn3', 'R1', LockType.EXCLUSIVE, 'lockUpgrade');
    detector.addWait('txn3', 'txn1', 'R1', LockType.EXCLUSIVE, 'lockUpgrade');

    const deadlock = detector.checkDeadlock('txn3');

    expect(deadlock).not.toBeNull();
    expect(deadlock!.participants.length).toBe(3);
  });
});

// =============================================================================
// 9. WAIT-FOR GRAPH API
// =============================================================================

describe('Wait-For Graph API', () => {
  let graph: WaitForGraph;

  beforeEach(() => {
    graph = new WaitForGraph();
  });

  /**
   * Basic edge management
   */
  it('should add and check edges', () => {
    graph.addEdge({
      from: 'txn1',
      to: 'txn2',
      resource: 'R1',
      reason: 'exclusive',
      requestedLockType: LockType.EXCLUSIVE,
    });

    expect(graph.hasEdge('txn1', 'txn2')).toBe(true);
    expect(graph.hasEdge('txn2', 'txn1')).toBe(false);
  });

  /**
   * Cycle detection
   */
  it('should detect cycles correctly', () => {
    graph.addEdge({
      from: 'txn1',
      to: 'txn2',
      resource: 'R1',
      reason: 'exclusive',
      requestedLockType: LockType.EXCLUSIVE,
    });
    graph.addEdge({
      from: 'txn2',
      to: 'txn1',
      resource: 'R2',
      reason: 'exclusive',
      requestedLockType: LockType.EXCLUSIVE,
    });

    expect(graph.hasCycle()).toBe(true);
    const cycle = graph.findCycle();
    expect(cycle).not.toBeNull();
    expect(cycle!.length).toBeGreaterThanOrEqual(2);
  });

  /**
   * Edge removal
   */
  it('should remove edges correctly', () => {
    graph.addEdge({
      from: 'txn1',
      to: 'txn2',
      resource: 'R1',
      reason: 'exclusive',
      requestedLockType: LockType.EXCLUSIVE,
    });

    expect(graph.hasEdge('txn1', 'txn2')).toBe(true);

    graph.removeEdge('txn1', 'txn2');

    expect(graph.hasEdge('txn1', 'txn2')).toBe(false);
  });

  /**
   * Transaction removal
   */
  it('should remove all edges for a transaction', () => {
    graph.addEdge({
      from: 'txn1',
      to: 'txn2',
      resource: 'R1',
      reason: 'exclusive',
      requestedLockType: LockType.EXCLUSIVE,
    });
    graph.addEdge({
      from: 'txn3',
      to: 'txn1',
      resource: 'R2',
      reason: 'shared',
      requestedLockType: LockType.SHARED,
    });

    expect(graph.getEdgeCount()).toBe(2);

    graph.removeTransaction('txn1');

    expect(graph.getEdgeCount()).toBe(0);
  });

  /**
   * DOT graph generation
   */
  it('should generate valid DOT graph', () => {
    graph.addEdge({
      from: 'txn1',
      to: 'txn2',
      resource: 'R1',
      reason: 'exclusive',
      requestedLockType: LockType.EXCLUSIVE,
    });

    const dot = graph.toDot();

    expect(dot).toContain('digraph WaitFor');
    expect(dot).toContain('"txn1" -> "txn2"');
    expect(dot).toContain('R1');
  });

  /**
   * GAP: getResourcesInCycle() doesn't return all resources in cycle
   * The cycle path doesn't always match edge order, missing some resources
   */
  it.fails('should get ALL resources involved in cycle', () => {
    graph.addEdge({
      from: 'txn1',
      to: 'txn2',
      resource: 'tableA',
      reason: 'exclusive',
      requestedLockType: LockType.EXCLUSIVE,
    });
    graph.addEdge({
      from: 'txn2',
      to: 'txn1',
      resource: 'tableB',
      reason: 'exclusive',
      requestedLockType: LockType.EXCLUSIVE,
    });

    const cycle = graph.findCycle();
    expect(cycle).not.toBeNull();

    const resources = graph.getResourcesInCycle(cycle!);
    // GAP: Only returns one resource due to cycle path order
    expect(resources).toContain('tableA');
    expect(resources).toContain('tableB');
  });

  /**
   * Single resource in cycle should work
   */
  it('should get single resource when cycle has same resource', () => {
    graph.addEdge({
      from: 'txn1',
      to: 'txn2',
      resource: 'sharedResource',
      reason: 'lockUpgrade',
      requestedLockType: LockType.EXCLUSIVE,
    });
    graph.addEdge({
      from: 'txn2',
      to: 'txn1',
      resource: 'sharedResource',
      reason: 'lockUpgrade',
      requestedLockType: LockType.EXCLUSIVE,
    });

    const cycle = graph.findCycle();
    expect(cycle).not.toBeNull();

    const resources = graph.getResourcesInCycle(cycle!);
    expect(resources).toContain('sharedResource');
  });
});

// =============================================================================
// 10. ADDITIONAL GAPS - MISSING FEATURES
// =============================================================================

describe('Additional Missing Features', () => {
  /**
   * GAP: clearGraph() without clearing stats/history
   * Current clear() resets everything including stats
   * Need explicit clearGraph() method that preserves stats
   */
  it.fails('should have clearGraph() that preserves stats and history', () => {
    const detector = createDeadlockDetector({ enabled: true });

    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');
    detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn2', 'txn1', 'B', LockType.EXCLUSIVE);
    detector.checkDeadlock('txn2');

    // GAP: Need explicit clearGraph() method (not undefined optional chaining)
    const clearGraphFn = (detector as any).clearGraph;
    expect(clearGraphFn).toBeDefined(); // This should fail - method doesn't exist
    clearGraphFn.call(detector);

    const stats = detector.getDeadlockStats();
    const history = detector.getDeadlockHistory();

    // Stats and history should persist after clearGraph()
    expect(stats.totalDeadlocks).toBe(1);
    expect(history.length).toBe(1);
  });

  /**
   * GAP: Export deadlock report as JSON
   */
  it.fails('should export deadlock report as JSON', () => {
    const detector = createDeadlockDetector({ enabled: true });

    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');
    detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn2', 'txn1', 'B', LockType.EXCLUSIVE);
    detector.checkDeadlock('txn2');

    // GAP: exportReport() should exist
    const report = (detector as any).exportReport?.();
    expect(report).toBeDefined();
    expect(typeof report).toBe('string');
    const parsed = JSON.parse(report);
    expect(parsed.totalDeadlocks).toBe(1);
    expect(parsed.history).toBeDefined();
  });

  /**
   * GAP: Deadlock prediction / early warning
   */
  it.fails('should predict potential deadlocks before they occur', () => {
    const detector = createDeadlockDetector({ enabled: true });

    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');

    // txn1 holds A, wants B
    detector.addWait('txn1', 'txn2', 'B', LockType.EXCLUSIVE);

    // GAP: predictDeadlock() should warn about potential cycle
    const prediction = (detector as any).predictDeadlock?.('txn2', 'txn1', 'A');
    expect(prediction).toBeDefined();
    expect(prediction.wouldCauseDeadlock).toBe(true);
  });

  /**
   * GAP: Deadlock probability scoring
   */
  it.fails('should calculate deadlock probability for transaction', () => {
    const detector = createDeadlockDetector({ enabled: true });

    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');
    detector.registerTransaction('txn3');

    // Complex wait graph
    detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn2', 'txn3', 'B', LockType.EXCLUSIVE);

    // GAP: getDeadlockProbability() should exist
    const probability = (detector as any).getDeadlockProbability?.('txn3');
    expect(probability).toBeDefined();
    expect(probability).toBeGreaterThanOrEqual(0);
    expect(probability).toBeLessThanOrEqual(1);
  });

  /**
   * GAP: Transaction priority for victim selection
   */
  it.fails('should support transaction priority in victim selection', () => {
    const detector = createDeadlockDetector({
      enabled: true,
      victimSelection: 'priority' as any, // GAP: 'priority' policy not implemented
    });

    detector.registerTransaction('txn_high');
    detector.registerTransaction('txn_low');

    // GAP: setTransactionPriority() should exist
    (detector as any).setTransactionPriority?.('txn_high', 100);
    (detector as any).setTransactionPriority?.('txn_low', 1);

    detector.addWait('txn_high', 'txn_low', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn_low', 'txn_high', 'B', LockType.EXCLUSIVE);

    const deadlock = detector.checkDeadlock('txn_low');

    expect(deadlock).not.toBeNull();
    // Low priority should be victim
    expect(deadlock!.victimTxnId).toBe('txn_low');
  });

  /**
   * GAP: Deadlock throttling to prevent cascade
   */
  it.fails('should throttle deadlock detection under high contention', () => {
    const detector = createDeadlockDetector({
      enabled: true,
      // GAP: throttling options should exist
      throttle: {
        maxDetectionsPerSecond: 10,
        cooldownMs: 100,
      },
    } as any);

    // GAP: getThrottleState() should exist
    const throttleState = (detector as any).getThrottleState?.();
    expect(throttleState).toBeDefined();
    expect(throttleState.isThrottled).toBe(false);
  });

  /**
   * GAP: Async deadlock detection with timeout
   */
  it.fails('should support async deadlock detection with timeout', async () => {
    const detector = createDeadlockDetector({ enabled: true });

    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');
    detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn2', 'txn1', 'B', LockType.EXCLUSIVE);

    // GAP: checkDeadlockAsync() should exist as a method
    const checkDeadlockAsyncFn = (detector as any).checkDeadlockAsync;
    expect(checkDeadlockAsyncFn).toBeDefined(); // This should fail

    const deadlock = await checkDeadlockAsyncFn.call(detector, 'txn2', { timeout: 100 });
    expect(deadlock).not.toBeNull();
  });
});

// =============================================================================
// 11. EDGE CASES AND ROBUSTNESS
// =============================================================================

describe('Edge Cases and Robustness', () => {
  let detector: DeadlockDetector;

  beforeEach(() => {
    detector = createDeadlockDetector({ enabled: true });
  });

  afterEach(() => {
    detector.clear();
  });

  /**
   * No deadlock when no cycle
   */
  it('should not detect deadlock when no cycle exists', () => {
    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');
    detector.registerTransaction('txn3');

    // Chain: txn1 -> txn2 -> txn3 (no cycle)
    detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn2', 'txn3', 'B', LockType.EXCLUSIVE);

    const deadlock = detector.checkDeadlock('txn2');

    expect(deadlock).toBeNull();
  });

  /**
   * Empty graph
   */
  it('should handle empty graph gracefully', () => {
    const deadlock = detector.checkDeadlock('nonexistent');

    expect(deadlock).toBeNull();
  });

  /**
   * Self-loop (transaction waiting on itself - invalid)
   */
  it('should handle self-loop in graph', () => {
    detector.registerTransaction('txn1');

    // This shouldn't happen in practice, but should be handled
    detector.addWait('txn1', 'txn1', 'A', LockType.EXCLUSIVE);

    // Self-loop is a cycle of length 1
    const graph = detector.getWaitForGraph();
    expect(graph.hasCycle()).toBe(true);
  });

  /**
   * Duplicate edges should be idempotent
   */
  it('should handle duplicate edge additions', () => {
    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');

    detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE); // Duplicate

    const graph = detector.getWaitForGraph();
    expect(graph.getEdgeCount()).toBe(1); // Should not create duplicate
  });

  /**
   * Multiple edges between same transactions for different resources
   */
  it('should allow multiple edges for different resources', () => {
    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');

    detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn1', 'txn2', 'B', LockType.SHARED);

    const graph = detector.getWaitForGraph();
    expect(graph.getEdgeCount()).toBe(2);
  });

  /**
   * Clear should reset all state
   */
  it('should reset all state on clear', () => {
    detector.registerTransaction('txn1');
    detector.registerTransaction('txn2');

    detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE);
    detector.addWait('txn2', 'txn1', 'B', LockType.EXCLUSIVE);

    detector.checkDeadlock('txn2'); // Creates history entry

    detector.clear();

    const graph = detector.getWaitForGraph();
    const stats = detector.getDeadlockStats();

    expect(graph.getEdgeCount()).toBe(0);
    expect(stats.totalDeadlocks).toBe(0);
  });
});
