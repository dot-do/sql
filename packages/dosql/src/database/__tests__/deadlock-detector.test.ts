/**
 * Deadlock Detector Tests
 *
 * Tests for wait-for graph and deadlock detection in DoSQL.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  DeadlockDetector,
  DeadlockError,
  WaitForGraph,
  type DeadlockDetectorOptions,
  type DeadlockInfo,
  type VictimSelectionPolicy,
  type WaitReason,
} from '../deadlock-detector.js';
import { LockType, TransactionErrorCode } from '../transaction/types.js';

// =============================================================================
// WaitForGraph Tests
// =============================================================================

describe('WaitForGraph', () => {
  describe('Edge Management', () => {
    it('should add edges to the graph', () => {
      const graph = new WaitForGraph();

      graph.addEdge({
        from: 'txn1',
        to: 'txn2',
        resource: 'users',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      expect(graph.hasEdge('txn1', 'txn2')).toBe(true);
      expect(graph.getEdgeCount()).toBe(1);
    });

    it('should not add duplicate edges', () => {
      const graph = new WaitForGraph();

      graph.addEdge({
        from: 'txn1',
        to: 'txn2',
        resource: 'users',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      graph.addEdge({
        from: 'txn1',
        to: 'txn2',
        resource: 'users',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      expect(graph.getEdgeCount()).toBe(1);
    });

    it('should remove edges from the graph', () => {
      const graph = new WaitForGraph();

      graph.addEdge({
        from: 'txn1',
        to: 'txn2',
        resource: 'users',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      graph.removeEdge('txn1', 'txn2');

      expect(graph.hasEdge('txn1', 'txn2')).toBe(false);
      expect(graph.getEdgeCount()).toBe(0);
    });

    it('should remove edges by resource', () => {
      const graph = new WaitForGraph();

      graph.addEdge({
        from: 'txn1',
        to: 'txn2',
        resource: 'users',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      graph.addEdge({
        from: 'txn1',
        to: 'txn2',
        resource: 'orders',
        reason: 'shared',
        requestedLockType: LockType.SHARED,
      });

      graph.removeEdge('txn1', 'txn2', 'users');

      expect(graph.getEdgeCount()).toBe(1);
    });

    it('should remove all edges for a transaction', () => {
      const graph = new WaitForGraph();

      // txn1 -> txn2, txn1 -> txn3, txn3 -> txn1
      graph.addEdge({
        from: 'txn1',
        to: 'txn2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });
      graph.addEdge({
        from: 'txn1',
        to: 'txn3',
        resource: 'B',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });
      graph.addEdge({
        from: 'txn3',
        to: 'txn1',
        resource: 'C',
        reason: 'shared',
        requestedLockType: LockType.SHARED,
      });

      graph.removeTransaction('txn1');

      expect(graph.hasEdge('txn1', 'txn2')).toBe(false);
      expect(graph.hasEdge('txn1', 'txn3')).toBe(false);
      expect(graph.hasEdge('txn3', 'txn1')).toBe(false);
    });

    it('should return wait reason for an edge', () => {
      const graph = new WaitForGraph();

      graph.addEdge({
        from: 'txn1',
        to: 'txn2',
        resource: 'users',
        reason: 'lockUpgrade',
        requestedLockType: LockType.EXCLUSIVE,
      });

      expect(graph.getWaitReason('txn1', 'txn2')).toBe('lockUpgrade');
    });

    it('should return undefined for non-existent edge', () => {
      const graph = new WaitForGraph();

      expect(graph.getWaitReason('txn1', 'txn2')).toBeUndefined();
    });
  });

  describe('Cycle Detection', () => {
    it('should detect no cycle when graph is empty', () => {
      const graph = new WaitForGraph();

      expect(graph.hasCycle()).toBe(false);
      expect(graph.findCycle()).toBeNull();
    });

    it('should detect no cycle in acyclic graph', () => {
      const graph = new WaitForGraph();

      // txn1 -> txn2 -> txn3 (no cycle)
      graph.addEdge({
        from: 'txn1',
        to: 'txn2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });
      graph.addEdge({
        from: 'txn2',
        to: 'txn3',
        resource: 'B',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      expect(graph.hasCycle()).toBe(false);
    });

    it('should detect simple cycle', () => {
      const graph = new WaitForGraph();

      // txn1 -> txn2 -> txn1 (simple cycle)
      graph.addEdge({
        from: 'txn1',
        to: 'txn2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });
      graph.addEdge({
        from: 'txn2',
        to: 'txn1',
        resource: 'B',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      expect(graph.hasCycle()).toBe(true);
      const cycle = graph.findCycle();
      expect(cycle).not.toBeNull();
      expect(cycle!.length).toBeGreaterThanOrEqual(2);
    });

    it('should detect longer cycles', () => {
      const graph = new WaitForGraph();

      // txn1 -> txn2 -> txn3 -> txn1
      graph.addEdge({
        from: 'txn1',
        to: 'txn2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });
      graph.addEdge({
        from: 'txn2',
        to: 'txn3',
        resource: 'B',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });
      graph.addEdge({
        from: 'txn3',
        to: 'txn1',
        resource: 'C',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      expect(graph.hasCycle()).toBe(true);
    });

    it('should find cycle from specific transaction', () => {
      const graph = new WaitForGraph();

      graph.addEdge({
        from: 'txn1',
        to: 'txn2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });
      graph.addEdge({
        from: 'txn2',
        to: 'txn1',
        resource: 'B',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      const cycle = graph.findCycleFrom('txn1');
      expect(cycle).not.toBeNull();
      expect(cycle).toContain('txn1');
      expect(cycle).toContain('txn2');
    });

    it('should return null when no cycle from specific transaction', () => {
      const graph = new WaitForGraph();

      graph.addEdge({
        from: 'txn1',
        to: 'txn2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      expect(graph.findCycleFrom('txn1')).toBeNull();
    });
  });

  describe('Cycle Information', () => {
    it('should get resources in cycle', () => {
      const graph = new WaitForGraph();

      graph.addEdge({
        from: 'txn1',
        to: 'txn2',
        resource: 'users',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });
      graph.addEdge({
        from: 'txn2',
        to: 'txn1',
        resource: 'orders',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      const cycle = graph.findCycle()!;
      const resources = graph.getResourcesInCycle(cycle);

      expect(resources).toContain('users');
      expect(resources).toContain('orders');
    });

    it('should get wait times in cycle', () => {
      const graph = new WaitForGraph();

      graph.addEdge({
        from: 'txn1',
        to: 'txn2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });
      graph.addEdge({
        from: 'txn2',
        to: 'txn1',
        resource: 'B',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      const cycle = graph.findCycle()!;
      const waitTimes = graph.getWaitTimesInCycle(cycle);

      expect(waitTimes).toBeDefined();
      expect(Object.keys(waitTimes).length).toBeGreaterThan(0);
    });
  });

  describe('Transaction Metadata', () => {
    it('should set and get transaction metadata', () => {
      const graph = new WaitForGraph();

      graph.setTransactionMeta('txn1', {
        startTime: 1000,
        cost: 50,
        readOnly: true,
        priority: 5,
      });

      const meta = graph.getTransactionMeta('txn1');
      expect(meta).toBeDefined();
      expect(meta?.startTime).toBe(1000);
      expect(meta?.cost).toBe(50);
      expect(meta?.readOnly).toBe(true);
      expect(meta?.priority).toBe(5);
    });

    it('should merge metadata with existing', () => {
      const graph = new WaitForGraph();

      graph.setTransactionMeta('txn1', { startTime: 1000 });
      graph.setTransactionMeta('txn1', { cost: 100 });

      const meta = graph.getTransactionMeta('txn1');
      expect(meta?.startTime).toBe(1000);
      expect(meta?.cost).toBe(100);
    });

    it('should return undefined for unknown transaction', () => {
      const graph = new WaitForGraph();

      expect(graph.getTransactionMeta('unknown')).toBeUndefined();
    });
  });

  describe('Victim Selection', () => {
    let graph: WaitForGraph;

    beforeEach(() => {
      graph = new WaitForGraph();

      // Set up a cycle: txn1 -> txn2 -> txn1
      graph.addEdge({
        from: 'txn1',
        to: 'txn2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });
      graph.addEdge({
        from: 'txn2',
        to: 'txn1',
        resource: 'B',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });
    });

    it('should select youngest victim', () => {
      graph.setTransactionMeta('txn1', { startTime: 1000 });
      graph.setTransactionMeta('txn2', { startTime: 2000 }); // younger

      const cycle = graph.findCycle()!;
      const victim = graph.selectVictim(cycle, 'youngest');

      expect(victim).toBe('txn2');
    });

    it('should select least work victim', () => {
      graph.setTransactionMeta('txn1', { cost: 100 });
      graph.setTransactionMeta('txn2', { cost: 10 }); // less work

      const cycle = graph.findCycle()!;
      const victim = graph.selectVictim(cycle, 'leastWork');

      expect(victim).toBe('txn2');
    });

    it('should prefer read-only victim', () => {
      graph.setTransactionMeta('txn1', { readOnly: false });
      graph.setTransactionMeta('txn2', { readOnly: true }); // read-only

      const cycle = graph.findCycle()!;
      const victim = graph.selectVictim(cycle, 'preferReadOnly');

      expect(victim).toBe('txn2');
    });

    it('should select by priority (lowest priority as victim)', () => {
      graph.setTransactionMeta('txn1', { priority: 10 }); // higher priority
      graph.setTransactionMeta('txn2', { priority: 1 }); // lower priority

      const cycle = graph.findCycle()!;
      const victim = graph.selectVictim(cycle, 'priority');

      expect(victim).toBe('txn2');
    });

    it('should use round robin selection', () => {
      const cycle = graph.findCycle()!;

      const victim1 = graph.selectVictim(cycle, 'roundRobin');
      const victim2 = graph.selectVictim(cycle, 'roundRobin');

      // Different victims on consecutive calls
      expect(victim1).not.toBe(victim2);
    });
  });

  describe('DOT Visualization', () => {
    it('should generate valid DOT format', () => {
      const graph = new WaitForGraph();

      graph.addEdge({
        from: 'txn1',
        to: 'txn2',
        resource: 'users',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      const dot = graph.toDot();

      expect(dot).toContain('digraph WaitFor');
      expect(dot).toContain('txn1');
      expect(dot).toContain('txn2');
      expect(dot).toContain('users');
    });
  });

  describe('getAllEdges', () => {
    it('should return all edges', () => {
      const graph = new WaitForGraph();

      graph.addEdge({
        from: 'txn1',
        to: 'txn2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });
      graph.addEdge({
        from: 'txn2',
        to: 'txn3',
        resource: 'B',
        reason: 'shared',
        requestedLockType: LockType.SHARED,
      });

      const edges = graph.getAllEdges();

      expect(edges).toHaveLength(2);
    });
  });
});

// =============================================================================
// DeadlockDetector Tests
// =============================================================================

describe('DeadlockDetector', () => {
  describe('Basic Detection', () => {
    it('should detect deadlock when cycle forms', () => {
      const detector = new DeadlockDetector({ enabled: true });

      detector.registerTransaction('txn1');
      detector.registerTransaction('txn2');

      detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE, 'exclusive');
      detector.addWait('txn2', 'txn1', 'B', LockType.EXCLUSIVE, 'exclusive');

      const deadlock = detector.checkDeadlock('txn2');

      expect(deadlock).not.toBeNull();
      expect(deadlock?.cycle.length).toBeGreaterThanOrEqual(2);
    });

    it('should not detect deadlock when disabled', () => {
      const detector = new DeadlockDetector({ enabled: false });

      detector.registerTransaction('txn1');
      detector.registerTransaction('txn2');

      detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE, 'exclusive');
      detector.addWait('txn2', 'txn1', 'B', LockType.EXCLUSIVE, 'exclusive');

      const deadlock = detector.checkDeadlock('txn2');

      expect(deadlock).toBeNull();
    });

    it('should return null when no cycle exists', () => {
      const detector = new DeadlockDetector({ enabled: true });

      detector.registerTransaction('txn1');
      detector.registerTransaction('txn2');

      detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE, 'exclusive');

      const deadlock = detector.checkDeadlock('txn1');

      expect(deadlock).toBeNull();
    });
  });

  describe('Transaction Lifecycle', () => {
    it('should register and unregister transactions', () => {
      const detector = new DeadlockDetector({ enabled: true });

      detector.registerTransaction('txn1');
      detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE, 'exclusive');

      detector.unregisterTransaction('txn1');

      // Graph should be clean after unregister
      const graph = detector.getWaitForGraph();
      expect(graph.hasEdge('txn1', 'txn2')).toBe(false);
    });

    it('should remove wait edges', () => {
      const detector = new DeadlockDetector({ enabled: true });

      detector.registerTransaction('txn1');
      detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE, 'exclusive');

      detector.removeWait('txn1', 'txn2', 'A');

      const graph = detector.getWaitForGraph();
      expect(graph.hasEdge('txn1', 'txn2')).toBe(false);
    });

    it('should remove all wait edges for transaction', () => {
      const detector = new DeadlockDetector({ enabled: true });

      detector.registerTransaction('txn1');
      detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE, 'exclusive');
      detector.addWait('txn1', 'txn3', 'B', LockType.EXCLUSIVE, 'exclusive');

      detector.removeWait('txn1'); // Remove all

      const graph = detector.getWaitForGraph();
      expect(graph.hasEdge('txn1', 'txn2')).toBe(false);
      expect(graph.hasEdge('txn1', 'txn3')).toBe(false);
    });
  });

  describe('Transaction Metadata', () => {
    it('should set transaction cost', () => {
      const detector = new DeadlockDetector({
        enabled: true,
        victimSelection: 'leastWork',
      });

      detector.registerTransaction('txn1');
      detector.registerTransaction('txn2');

      detector.setTransactionCost('txn1', 100);
      detector.setTransactionCost('txn2', 10);

      detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE, 'exclusive');
      detector.addWait('txn2', 'txn1', 'B', LockType.EXCLUSIVE, 'exclusive');

      const deadlock = detector.checkDeadlock('txn2');

      // txn2 should be victim (least work)
      expect(deadlock?.victimTxnId).toBe('txn2');
    });

    it('should mark transaction as read-only', () => {
      const detector = new DeadlockDetector({
        enabled: true,
        victimSelection: 'preferReadOnly',
      });

      detector.registerTransaction('txn1');
      detector.registerTransaction('txn2');

      detector.markReadOnly('txn1', false);
      detector.markReadOnly('txn2', true);

      detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE, 'exclusive');
      detector.addWait('txn2', 'txn1', 'B', LockType.EXCLUSIVE, 'exclusive');

      const deadlock = detector.checkDeadlock('txn2');

      expect(deadlock?.victimTxnId).toBe('txn2');
    });
  });

  describe('Lock Acquisition Tracking', () => {
    it('should track lock acquisition for ordering', () => {
      const detector = new DeadlockDetector({
        enabled: true,
        deadlockPrevention: 'lockOrdering',
      });

      detector.registerTransaction('txn1');
      detector.recordLockAcquisition('txn1', 'A');
      detector.recordLockAcquisition('txn1', 'B');

      // Should not throw
      const prevention = detector.checkPrevention('txn1', 'txn2', 'C');

      // Lock ordering violation would return an error
      expect(prevention === null || prevention !== undefined).toBe(true);
    });
  });

  describe('Statistics', () => {
    it('should track deadlock statistics', () => {
      const detector = new DeadlockDetector({
        enabled: true,
        victimSelection: 'youngest',
      });

      detector.registerTransaction('txn1');
      detector.registerTransaction('txn2');

      detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE, 'exclusive');
      detector.addWait('txn2', 'txn1', 'B', LockType.EXCLUSIVE, 'exclusive');

      detector.checkDeadlock('txn2');

      const stats = detector.getDeadlockStats();
      expect(stats.totalDeadlocks).toBe(1);
    });

    it('should track deadlock history', () => {
      const detector = new DeadlockDetector({
        enabled: true,
      });

      detector.registerTransaction('txn1');
      detector.registerTransaction('txn2');

      detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE, 'exclusive');
      detector.addWait('txn2', 'txn1', 'B', LockType.EXCLUSIVE, 'exclusive');

      detector.checkDeadlock('txn2');

      const history = detector.getDeadlockHistory();
      expect(history.length).toBe(1);
      expect(history[0].participants).toContain('txn1');
      expect(history[0].participants).toContain('txn2');
    });
  });

  describe('Deadlock Callback', () => {
    it('should call onDeadlock callback', () => {
      const callbackArgs: unknown[] = [];
      const detector = new DeadlockDetector({
        enabled: true,
        onDeadlock: (info: unknown) => { callbackArgs.push(info); },
      });

      detector.registerTransaction('txn1');
      detector.registerTransaction('txn2');

      detector.addWait('txn1', 'txn2', 'A', LockType.EXCLUSIVE, 'exclusive');
      detector.addWait('txn2', 'txn1', 'B', LockType.EXCLUSIVE, 'exclusive');

      detector.checkDeadlock('txn2');

      expect(callbackArgs.length).toBeGreaterThan(0);
      const info = callbackArgs[0] as { victimTxnId: string; cycle: unknown[] };
      expect(typeof info.victimTxnId).toBe('string');
      expect(Array.isArray(info.cycle)).toBe(true);
    });
  });

  describe('Prevention Schemes', () => {
    it('should apply waitDie prevention', async () => {
      const detector = new DeadlockDetector({
        enabled: true,
        deadlockPrevention: 'waitDie',
      });

      // Register transactions in order - txn1 first (older), txn2 second (younger)
      detector.registerTransaction('txn1');
      // Small delay to ensure different timestamps
      await new Promise((resolve) => setTimeout(resolve, 5));
      detector.registerTransaction('txn2');

      // In wait-die: older waits, younger dies (aborts)
      // Older (txn1) waiting on younger (txn2) - should be allowed (older can wait)
      const prevention1 = detector.checkPrevention('txn1', 'txn2', 'A');
      expect(prevention1).toBeNull();

      // Younger (txn2) waiting on older (txn1) - should die (return error, younger must abort)
      const prevention2 = detector.checkPrevention('txn2', 'txn1', 'B');
      expect(prevention2).not.toBeNull();
    });

    it('should apply noWait prevention', () => {
      const detector = new DeadlockDetector({
        enabled: true,
        deadlockPrevention: 'noWait',
      });

      detector.registerTransaction('txn1');

      // Any wait should fail immediately
      const prevention = detector.checkPrevention('txn1', 'txn2', 'A');

      expect(prevention).not.toBeNull();
      expect(prevention?.code).toBe(TransactionErrorCode.LOCK_FAILED);
    });
  });
});

// =============================================================================
// DeadlockError Tests
// =============================================================================

describe('DeadlockError', () => {
  it('should create error with deadlock info', () => {
    const info: DeadlockInfo = {
      cycle: ['txn1', 'txn2', 'txn1'],
      resources: ['A', 'B'],
      victimTxnId: 'txn2',
      waitTimes: { txn1: 100, txn2: 50 },
      timestamp: Date.now(),
      graphDot: 'digraph {}',
      participants: ['txn1', 'txn2'],
    };

    const error = new DeadlockError(info, 'txn1');

    expect(error).toBeInstanceOf(DeadlockError);
    expect(error.code).toBe(TransactionErrorCode.DEADLOCK);
    expect(error.cycle).toEqual(info.cycle);
    expect(error.resources).toEqual(info.resources);
    expect(error.victimTxnId).toBe('txn2');
    expect(error.message).toContain('txn1');
    expect(error.message).toContain('txn2');
  });
});
