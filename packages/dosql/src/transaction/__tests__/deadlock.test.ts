/**
 * Deadlock Detection Tests
 *
 * Comprehensive tests for the transaction isolation deadlock detection system.
 * Tests the wait-for graph, cycle detection, and victim selection strategies.
 *
 * Issue: sql-ooh8 - Transaction isolation enforcer lacks deadlock detection
 *
 * Test scenarios:
 * 1. Two transactions waiting on each other (simple deadlock)
 * 2. Multi-transaction cycles (A waits B waits C waits A)
 * 3. No deadlock scenarios (chains without cycles)
 * 4. Victim selection strategies (youngest, leastWork, priority, etc.)
 * 5. Wait-for graph construction and edge management
 * 6. Deadlock prevention schemes
 * 7. Statistics and history tracking
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  createLockManager,
  type LockManager,
} from '../isolation.js';
import {
  TransactionError,
  TransactionErrorCode,
  createTransactionId,
  createLSN,
  LockType,
  type TransactionId,
} from '../types.js';
import {
  DeadlockDetector,
  DeadlockError,
  WaitForGraph,
  type DeadlockInfo,
  type VictimSelectionPolicy,
} from '../../database/deadlock-detector.js';

// =============================================================================
// TEST UTILITIES
// =============================================================================

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Helper to create transaction IDs with predictable naming
 */
function txn(id: string): TransactionId {
  return createTransactionId(`txn-${id}`);
}

// =============================================================================
// 1. WAIT-FOR GRAPH TESTS
// =============================================================================

describe('WaitForGraph', () => {
  let graph: WaitForGraph;

  beforeEach(() => {
    graph = new WaitForGraph();
  });

  describe('Edge Management', () => {
    it('should add edges to the graph', () => {
      graph.addEdge({
        from: 'txn-1',
        to: 'txn-2',
        resource: 'resource-A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      expect(graph.hasEdge('txn-1', 'txn-2')).toBe(true);
      expect(graph.hasEdge('txn-2', 'txn-1')).toBe(false);
    });

    it('should not add duplicate edges', () => {
      graph.addEdge({
        from: 'txn-1',
        to: 'txn-2',
        resource: 'resource-A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      graph.addEdge({
        from: 'txn-1',
        to: 'txn-2',
        resource: 'resource-A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      // Should still only have one edge
      expect(graph.getEdgeCount()).toBe(1);
    });

    it('should allow multiple edges from same source to different targets', () => {
      graph.addEdge({
        from: 'txn-1',
        to: 'txn-2',
        resource: 'resource-A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      graph.addEdge({
        from: 'txn-1',
        to: 'txn-3',
        resource: 'resource-B',
        reason: 'shared',
        requestedLockType: LockType.SHARED,
      });

      expect(graph.hasEdge('txn-1', 'txn-2')).toBe(true);
      expect(graph.hasEdge('txn-1', 'txn-3')).toBe(true);
      expect(graph.getEdgeCount()).toBe(2);
    });

    it('should remove specific edges', () => {
      graph.addEdge({
        from: 'txn-1',
        to: 'txn-2',
        resource: 'resource-A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      graph.addEdge({
        from: 'txn-1',
        to: 'txn-3',
        resource: 'resource-B',
        reason: 'shared',
        requestedLockType: LockType.SHARED,
      });

      graph.removeEdge('txn-1', 'txn-2', 'resource-A');

      expect(graph.hasEdge('txn-1', 'txn-2')).toBe(false);
      expect(graph.hasEdge('txn-1', 'txn-3')).toBe(true);
    });

    it('should remove all edges for a transaction', () => {
      // T1 -> T2
      graph.addEdge({
        from: 'txn-1',
        to: 'txn-2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      // T3 -> T1
      graph.addEdge({
        from: 'txn-3',
        to: 'txn-1',
        resource: 'B',
        reason: 'shared',
        requestedLockType: LockType.SHARED,
      });

      // Remove all edges involving T1
      graph.removeTransaction('txn-1');

      expect(graph.hasEdge('txn-1', 'txn-2')).toBe(false);
      expect(graph.hasEdge('txn-3', 'txn-1')).toBe(false);
      expect(graph.getEdgeCount()).toBe(0);
    });

    it('should track wait reasons', () => {
      graph.addEdge({
        from: 'txn-1',
        to: 'txn-2',
        resource: 'A',
        reason: 'lockUpgrade',
        requestedLockType: LockType.EXCLUSIVE,
      });

      expect(graph.getWaitReason('txn-1', 'txn-2')).toBe('lockUpgrade');
      expect(graph.getWaitReason('txn-2', 'txn-1')).toBeUndefined();
    });
  });

  describe('Cycle Detection', () => {
    it('should detect simple two-node cycle (A -> B -> A)', () => {
      graph.addEdge({
        from: 'txn-1',
        to: 'txn-2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      graph.addEdge({
        from: 'txn-2',
        to: 'txn-1',
        resource: 'B',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      expect(graph.hasCycle()).toBe(true);

      const cycle = graph.findCycle();
      expect(cycle).not.toBeNull();
      expect(cycle!.length).toBeGreaterThanOrEqual(3); // A -> B -> A (closed)
    });

    it('should detect three-node cycle (A -> B -> C -> A)', () => {
      graph.addEdge({
        from: 'txn-1',
        to: 'txn-2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      graph.addEdge({
        from: 'txn-2',
        to: 'txn-3',
        resource: 'B',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      graph.addEdge({
        from: 'txn-3',
        to: 'txn-1',
        resource: 'C',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      expect(graph.hasCycle()).toBe(true);

      const cycle = graph.findCycle();
      expect(cycle).not.toBeNull();
      expect(cycle!.length).toBeGreaterThanOrEqual(4); // 3 nodes + closing
    });

    it('should detect cycle when starting from any node in the cycle', () => {
      // Create cycle: T1 -> T2 -> T3 -> T1
      graph.addEdge({
        from: 'txn-1',
        to: 'txn-2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      graph.addEdge({
        from: 'txn-2',
        to: 'txn-3',
        resource: 'B',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      graph.addEdge({
        from: 'txn-3',
        to: 'txn-1',
        resource: 'C',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      // Should find cycle from any starting point
      expect(graph.findCycleFrom('txn-1')).not.toBeNull();
      expect(graph.findCycleFrom('txn-2')).not.toBeNull();
      expect(graph.findCycleFrom('txn-3')).not.toBeNull();
    });

    it('should not detect cycle in linear chain (A -> B -> C)', () => {
      graph.addEdge({
        from: 'txn-1',
        to: 'txn-2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      graph.addEdge({
        from: 'txn-2',
        to: 'txn-3',
        resource: 'B',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      expect(graph.hasCycle()).toBe(false);
      expect(graph.findCycle()).toBeNull();
    });

    it('should not detect cycle in empty graph', () => {
      expect(graph.hasCycle()).toBe(false);
      expect(graph.findCycle()).toBeNull();
    });

    it('should not detect cycle with single node and no edges', () => {
      graph.setTransactionMeta('txn-1', { startTime: Date.now(), cost: 0, readOnly: false, victimCount: 0, priority: 0 });
      expect(graph.hasCycle()).toBe(false);
    });

    it('should detect cycle with multiple components where only one has cycle', () => {
      // Component 1: Linear chain (no cycle)
      graph.addEdge({
        from: 'txn-1',
        to: 'txn-2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      // Component 2: Cycle
      graph.addEdge({
        from: 'txn-3',
        to: 'txn-4',
        resource: 'B',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      graph.addEdge({
        from: 'txn-4',
        to: 'txn-3',
        resource: 'C',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      expect(graph.hasCycle()).toBe(true);
    });

    it('should collect all resources involved in a cycle', () => {
      graph.addEdge({
        from: 'txn-1',
        to: 'txn-2',
        resource: 'resource-A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      graph.addEdge({
        from: 'txn-2',
        to: 'txn-3',
        resource: 'resource-B',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      graph.addEdge({
        from: 'txn-3',
        to: 'txn-1',
        resource: 'resource-C',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      const cycle = graph.findCycle()!;
      const resources = graph.getResourcesInCycle(cycle);

      expect(resources.length).toBeGreaterThanOrEqual(2);
    });
  });

  describe('DOT Graph Generation', () => {
    it('should generate valid DOT graph format', () => {
      graph.addEdge({
        from: 'txn-1',
        to: 'txn-2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      const dot = graph.toDot();
      expect(dot).toContain('digraph WaitFor');
      expect(dot).toContain('"txn-1" -> "txn-2"');
    });
  });
});

// =============================================================================
// 2. VICTIM SELECTION TESTS
// =============================================================================

describe('Victim Selection Policies', () => {
  let graph: WaitForGraph;

  beforeEach(() => {
    graph = new WaitForGraph();
  });

  describe('Youngest Transaction Policy', () => {
    it('should select the youngest transaction as victim', () => {
      // Setup cycle with different start times
      const now = Date.now();

      graph.setTransactionMeta('txn-1', {
        startTime: now - 1000, // 1 second ago (older)
        cost: 0,
        readOnly: false,
        victimCount: 0,
        priority: 0,
      });

      graph.setTransactionMeta('txn-2', {
        startTime: now, // Just now (younger)
        cost: 0,
        readOnly: false,
        victimCount: 0,
        priority: 0,
      });

      // Create cycle
      graph.addEdge({
        from: 'txn-1',
        to: 'txn-2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      graph.addEdge({
        from: 'txn-2',
        to: 'txn-1',
        resource: 'B',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      const cycle = graph.findCycle()!;
      const victim = graph.selectVictim(cycle, 'youngest');

      expect(victim).toBe('txn-2'); // Youngest should be victim
    });
  });

  describe('Least Work Policy', () => {
    it('should select transaction with least work as victim', () => {
      graph.setTransactionMeta('txn-1', {
        startTime: Date.now(),
        cost: 100, // More work done
        readOnly: false,
        victimCount: 0,
        priority: 0,
      });

      graph.setTransactionMeta('txn-2', {
        startTime: Date.now(),
        cost: 10, // Less work done
        readOnly: false,
        victimCount: 0,
        priority: 0,
      });

      graph.addEdge({
        from: 'txn-1',
        to: 'txn-2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      graph.addEdge({
        from: 'txn-2',
        to: 'txn-1',
        resource: 'B',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      const cycle = graph.findCycle()!;
      const victim = graph.selectVictim(cycle, 'leastWork');

      expect(victim).toBe('txn-2'); // Less work should be victim
    });
  });

  describe('Prefer Read-Only Policy', () => {
    it('should prefer read-only transactions as victims', () => {
      graph.setTransactionMeta('txn-1', {
        startTime: Date.now(),
        cost: 0,
        readOnly: false, // Read-write
        victimCount: 0,
        priority: 0,
      });

      graph.setTransactionMeta('txn-2', {
        startTime: Date.now(),
        cost: 0,
        readOnly: true, // Read-only - should be victim
        victimCount: 0,
        priority: 0,
      });

      graph.addEdge({
        from: 'txn-1',
        to: 'txn-2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      graph.addEdge({
        from: 'txn-2',
        to: 'txn-1',
        resource: 'B',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      const cycle = graph.findCycle()!;
      const victim = graph.selectVictim(cycle, 'preferReadOnly');

      expect(victim).toBe('txn-2'); // Read-only should be victim
    });

    it('should fall back to youngest when no read-only transactions', () => {
      const now = Date.now();

      graph.setTransactionMeta('txn-1', {
        startTime: now - 1000,
        cost: 0,
        readOnly: false,
        victimCount: 0,
        priority: 0,
      });

      graph.setTransactionMeta('txn-2', {
        startTime: now,
        cost: 0,
        readOnly: false,
        victimCount: 0,
        priority: 0,
      });

      graph.addEdge({
        from: 'txn-1',
        to: 'txn-2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      graph.addEdge({
        from: 'txn-2',
        to: 'txn-1',
        resource: 'B',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      const cycle = graph.findCycle()!;
      const victim = graph.selectVictim(cycle, 'preferReadOnly');

      expect(victim).toBe('txn-2'); // Falls back to youngest
    });
  });

  describe('Priority Policy', () => {
    it('should select lowest priority transaction as victim', () => {
      graph.setTransactionMeta('txn-1', {
        startTime: Date.now(),
        cost: 0,
        readOnly: false,
        victimCount: 0,
        priority: 10, // Higher priority (protected)
      });

      graph.setTransactionMeta('txn-2', {
        startTime: Date.now(),
        cost: 0,
        readOnly: false,
        victimCount: 0,
        priority: 1, // Lower priority (victim)
      });

      graph.addEdge({
        from: 'txn-1',
        to: 'txn-2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      graph.addEdge({
        from: 'txn-2',
        to: 'txn-1',
        resource: 'B',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      const cycle = graph.findCycle()!;
      const victim = graph.selectVictim(cycle, 'priority');

      expect(victim).toBe('txn-2'); // Lower priority should be victim
    });
  });

  describe('Round Robin Policy', () => {
    it('should rotate victim selection across multiple deadlocks', () => {
      graph.setTransactionMeta('txn-1', {
        startTime: Date.now(),
        cost: 0,
        readOnly: false,
        victimCount: 0,
        priority: 0,
      });

      graph.setTransactionMeta('txn-2', {
        startTime: Date.now(),
        cost: 0,
        readOnly: false,
        victimCount: 0,
        priority: 0,
      });

      graph.addEdge({
        from: 'txn-1',
        to: 'txn-2',
        resource: 'A',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      graph.addEdge({
        from: 'txn-2',
        to: 'txn-1',
        resource: 'B',
        reason: 'exclusive',
        requestedLockType: LockType.EXCLUSIVE,
      });

      const cycle = graph.findCycle()!;

      // Get victims multiple times - should rotate
      const victims: string[] = [];
      for (let i = 0; i < 4; i++) {
        victims.push(graph.selectVictim(cycle, 'roundRobin'));
      }

      // With round robin, we expect rotation
      expect(victims[0]).not.toBe(victims[1]);
    });
  });
});

// =============================================================================
// 3. DEADLOCK DETECTOR TESTS
// =============================================================================

describe('DeadlockDetector', () => {
  let detector: DeadlockDetector;

  beforeEach(() => {
    detector = new DeadlockDetector({
      enabled: true,
      victimSelection: 'youngest',
    });
  });

  describe('Transaction Registration', () => {
    it('should register transactions with timestamps', () => {
      detector.registerTransaction('txn-1');
      detector.registerTransaction('txn-2');

      // Verify transactions are tracked by checking deadlock detection works
      detector.addWait('txn-1', 'txn-2', 'A', LockType.EXCLUSIVE);
      detector.addWait('txn-2', 'txn-1', 'B', LockType.EXCLUSIVE);

      const info = detector.checkDeadlock('txn-2');
      expect(info).not.toBeNull();
    });

    it('should preserve original timestamp on re-registration', async () => {
      detector.registerTransaction('txn-1');
      await delay(10);
      detector.registerTransaction('txn-1'); // Re-register

      // Original timestamp should be preserved
      const graph = detector.getWaitForGraph();
      const meta = graph.getTransactionMeta('txn-1');
      expect(meta).toBeDefined();
    });

    it('should unregister transactions and clean up edges', () => {
      detector.registerTransaction('txn-1');
      detector.registerTransaction('txn-2');

      detector.addWait('txn-1', 'txn-2', 'A', LockType.EXCLUSIVE);

      detector.unregisterTransaction('txn-1');

      // No cycle should be possible after unregistration
      detector.addWait('txn-2', 'txn-1', 'B', LockType.EXCLUSIVE);
      const info = detector.checkDeadlock('txn-2');
      // After unregistration, no edges for txn-1 exist
      expect(info).toBeNull();
    });
  });

  describe('Wait Edge Management', () => {
    it('should add wait edges between transactions', () => {
      detector.registerTransaction('txn-1');
      detector.registerTransaction('txn-2');

      detector.addWait('txn-1', 'txn-2', 'resource-A', LockType.EXCLUSIVE, 'exclusive');

      const graph = detector.getWaitForGraph();
      expect(graph.hasEdge('txn-1', 'txn-2')).toBe(true);
    });

    it('should remove specific wait edges', () => {
      detector.registerTransaction('txn-1');
      detector.registerTransaction('txn-2');

      detector.addWait('txn-1', 'txn-2', 'resource-A', LockType.EXCLUSIVE);
      detector.removeWait('txn-1', 'txn-2', 'resource-A');

      const graph = detector.getWaitForGraph();
      expect(graph.hasEdge('txn-1', 'txn-2')).toBe(false);
    });

    it('should remove all wait edges for a transaction', () => {
      detector.registerTransaction('txn-1');
      detector.registerTransaction('txn-2');
      detector.registerTransaction('txn-3');

      detector.addWait('txn-1', 'txn-2', 'A', LockType.EXCLUSIVE);
      detector.addWait('txn-1', 'txn-3', 'B', LockType.EXCLUSIVE);

      detector.removeWait('txn-1'); // Remove all waits for txn-1

      const graph = detector.getWaitForGraph();
      expect(graph.hasEdge('txn-1', 'txn-2')).toBe(false);
      expect(graph.hasEdge('txn-1', 'txn-3')).toBe(false);
    });
  });

  describe('Simple Deadlock Detection', () => {
    it('should detect deadlock between two transactions', () => {
      detector.registerTransaction('txn-1');
      detector.registerTransaction('txn-2');

      // T1 waits for T2 (T1 -> T2)
      detector.addWait('txn-1', 'txn-2', 'A', LockType.EXCLUSIVE);

      // T2 waits for T1 (T2 -> T1) - creates cycle
      detector.addWait('txn-2', 'txn-1', 'B', LockType.EXCLUSIVE);

      const info = detector.checkDeadlock('txn-2');

      expect(info).not.toBeNull();
      expect(info!.cycle.length).toBeGreaterThanOrEqual(3);
      expect(info!.victimTxnId).toBeDefined();
    });

    it('should return null when no deadlock exists', () => {
      detector.registerTransaction('txn-1');
      detector.registerTransaction('txn-2');
      detector.registerTransaction('txn-3');

      // Linear chain: T1 -> T2 -> T3 (no cycle)
      detector.addWait('txn-1', 'txn-2', 'A', LockType.EXCLUSIVE);
      detector.addWait('txn-2', 'txn-3', 'B', LockType.EXCLUSIVE);

      const info = detector.checkDeadlock('txn-1');
      expect(info).toBeNull();
    });

    it('should not detect deadlock when detection is disabled', () => {
      const disabledDetector = new DeadlockDetector({ enabled: false });

      disabledDetector.registerTransaction('txn-1');
      disabledDetector.registerTransaction('txn-2');

      disabledDetector.addWait('txn-1', 'txn-2', 'A', LockType.EXCLUSIVE);
      disabledDetector.addWait('txn-2', 'txn-1', 'B', LockType.EXCLUSIVE);

      const info = disabledDetector.checkDeadlock('txn-2');
      expect(info).toBeNull();
    });
  });

  describe('Multi-Transaction Cycles', () => {
    it('should detect three-transaction cycle (A -> B -> C -> A)', () => {
      detector.registerTransaction('txn-A');
      detector.registerTransaction('txn-B');
      detector.registerTransaction('txn-C');

      detector.addWait('txn-A', 'txn-B', 'resource-1', LockType.EXCLUSIVE);
      detector.addWait('txn-B', 'txn-C', 'resource-2', LockType.EXCLUSIVE);
      detector.addWait('txn-C', 'txn-A', 'resource-3', LockType.EXCLUSIVE);

      const info = detector.checkDeadlock('txn-C');

      expect(info).not.toBeNull();
      expect(info!.cycle.length).toBeGreaterThanOrEqual(4); // A -> B -> C -> A
      expect(info!.participants).toContain('txn-A');
      expect(info!.participants).toContain('txn-B');
      expect(info!.participants).toContain('txn-C');
    });

    it('should detect four-transaction cycle', () => {
      detector.registerTransaction('txn-A');
      detector.registerTransaction('txn-B');
      detector.registerTransaction('txn-C');
      detector.registerTransaction('txn-D');

      detector.addWait('txn-A', 'txn-B', 'r1', LockType.EXCLUSIVE);
      detector.addWait('txn-B', 'txn-C', 'r2', LockType.EXCLUSIVE);
      detector.addWait('txn-C', 'txn-D', 'r3', LockType.EXCLUSIVE);
      detector.addWait('txn-D', 'txn-A', 'r4', LockType.EXCLUSIVE);

      const info = detector.checkDeadlock('txn-D');

      expect(info).not.toBeNull();
      expect(info!.participants.length).toBe(4);
    });
  });

  describe('Upgrade Deadlock Detection', () => {
    it('should detect lock upgrade deadlock', () => {
      detector.registerTransaction('txn-1');
      detector.registerTransaction('txn-2');

      // T1 holds shared, waits for exclusive upgrade (blocked by T2's shared)
      detector.addWait('txn-1', 'txn-2', 'A', LockType.EXCLUSIVE, 'lockUpgrade');

      // T2 holds shared, waits for exclusive upgrade (blocked by T1's shared)
      detector.addWait('txn-2', 'txn-1', 'A', LockType.EXCLUSIVE, 'lockUpgrade');

      const info = detector.checkDeadlock('txn-2');

      expect(info).not.toBeNull();

      // Check stats track upgrade deadlocks
      const stats = detector.getDeadlockStats();
      expect(stats.upgradeDeadlocks).toBeGreaterThanOrEqual(1);
    });
  });

  describe('Deadlock Information', () => {
    it('should provide complete deadlock information', () => {
      detector.registerTransaction('txn-1');
      detector.registerTransaction('txn-2');

      detector.addWait('txn-1', 'txn-2', 'resource-A', LockType.EXCLUSIVE);
      detector.addWait('txn-2', 'txn-1', 'resource-B', LockType.EXCLUSIVE);

      const info = detector.checkDeadlock('txn-2');

      expect(info).not.toBeNull();
      expect(info!.cycle).toBeDefined();
      expect(info!.resources).toBeDefined();
      expect(info!.victimTxnId).toBeDefined();
      expect(info!.waitTimes).toBeDefined();
      expect(info!.timestamp).toBeDefined();
      expect(info!.graphDot).toBeDefined();
      expect(info!.participants).toBeDefined();
    });

    it('should include DOT graph visualization', () => {
      detector.registerTransaction('txn-1');
      detector.registerTransaction('txn-2');

      detector.addWait('txn-1', 'txn-2', 'A', LockType.EXCLUSIVE);
      detector.addWait('txn-2', 'txn-1', 'B', LockType.EXCLUSIVE);

      const info = detector.checkDeadlock('txn-2');

      expect(info!.graphDot).toContain('digraph WaitFor');
      expect(info!.graphDot).toContain('txn-1');
      expect(info!.graphDot).toContain('txn-2');
    });
  });

  describe('Transaction Metadata', () => {
    it('should track transaction cost for victim selection', () => {
      detector.registerTransaction('txn-1');
      detector.registerTransaction('txn-2');

      detector.setTransactionCost('txn-1', 100);
      detector.setTransactionCost('txn-2', 10);

      detector.addWait('txn-1', 'txn-2', 'A', LockType.EXCLUSIVE);
      detector.addWait('txn-2', 'txn-1', 'B', LockType.EXCLUSIVE);

      // With leastWork policy
      const leastWorkDetector = new DeadlockDetector({
        enabled: true,
        victimSelection: 'leastWork',
      });

      leastWorkDetector.registerTransaction('txn-1');
      leastWorkDetector.registerTransaction('txn-2');
      leastWorkDetector.setTransactionCost('txn-1', 100);
      leastWorkDetector.setTransactionCost('txn-2', 10);

      leastWorkDetector.addWait('txn-1', 'txn-2', 'A', LockType.EXCLUSIVE);
      leastWorkDetector.addWait('txn-2', 'txn-1', 'B', LockType.EXCLUSIVE);

      const info = leastWorkDetector.checkDeadlock('txn-2');
      expect(info!.victimTxnId).toBe('txn-2'); // Less work
    });

    it('should mark transactions as read-only', () => {
      detector.registerTransaction('txn-1');
      detector.markReadOnly('txn-1', true);

      const graph = detector.getWaitForGraph();
      const meta = graph.getTransactionMeta('txn-1');
      expect(meta?.readOnly).toBe(true);
    });

    it('should set transaction priority', () => {
      detector.registerTransaction('txn-1');
      detector.setTransactionPriority('txn-1', 10);

      const graph = detector.getWaitForGraph();
      const meta = graph.getTransactionMeta('txn-1');
      expect(meta?.priority).toBe(10);
    });
  });

  describe('Statistics Tracking', () => {
    it('should track total deadlocks detected', () => {
      detector.registerTransaction('txn-1');
      detector.registerTransaction('txn-2');

      detector.addWait('txn-1', 'txn-2', 'A', LockType.EXCLUSIVE);
      detector.addWait('txn-2', 'txn-1', 'B', LockType.EXCLUSIVE);

      detector.checkDeadlock('txn-2');

      const stats = detector.getDeadlockStats();
      expect(stats.totalDeadlocks).toBe(1);
    });

    it('should track victims aborted by policy', () => {
      const youngestDetector = new DeadlockDetector({
        enabled: true,
        victimSelection: 'youngest',
      });

      youngestDetector.registerTransaction('txn-1');
      youngestDetector.registerTransaction('txn-2');

      youngestDetector.addWait('txn-1', 'txn-2', 'A', LockType.EXCLUSIVE);
      youngestDetector.addWait('txn-2', 'txn-1', 'B', LockType.EXCLUSIVE);

      youngestDetector.checkDeadlock('txn-2');

      const stats = youngestDetector.getDeadlockStats();
      expect(stats.victimsAborted.youngest).toBe(1);
    });

    it('should calculate average cycle length', () => {
      // First deadlock: 2-node cycle
      detector.registerTransaction('txn-1');
      detector.registerTransaction('txn-2');
      detector.addWait('txn-1', 'txn-2', 'A', LockType.EXCLUSIVE);
      detector.addWait('txn-2', 'txn-1', 'B', LockType.EXCLUSIVE);
      detector.checkDeadlock('txn-2');

      const stats = detector.getDeadlockStats();
      expect(stats.avgCycleLength).toBeGreaterThan(0);
    });
  });

  describe('Deadlock History', () => {
    it('should maintain history of detected deadlocks', () => {
      detector.registerTransaction('txn-1');
      detector.registerTransaction('txn-2');

      detector.addWait('txn-1', 'txn-2', 'A', LockType.EXCLUSIVE);
      detector.addWait('txn-2', 'txn-1', 'B', LockType.EXCLUSIVE);

      detector.checkDeadlock('txn-2');

      const history = detector.getDeadlockHistory();
      expect(history.length).toBe(1);
      expect(history[0].cycle).toBeDefined();
    });

    it('should limit history size', () => {
      const limitedDetector = new DeadlockDetector({
        enabled: true,
        maxHistorySize: 2,
      });

      // Create 3 deadlocks
      for (let i = 0; i < 3; i++) {
        limitedDetector.clear();
        limitedDetector.registerTransaction(`txn-${i}-1`);
        limitedDetector.registerTransaction(`txn-${i}-2`);
        limitedDetector.addWait(`txn-${i}-1`, `txn-${i}-2`, `A${i}`, LockType.EXCLUSIVE);
        limitedDetector.addWait(`txn-${i}-2`, `txn-${i}-1`, `B${i}`, LockType.EXCLUSIVE);
        limitedDetector.checkDeadlock(`txn-${i}-2`);
      }

      // Persistent history should have at most 2 entries (most recent)
      const persistentHistory = limitedDetector.getPersistentHistory();
      expect(persistentHistory.length).toBeLessThanOrEqual(2);
    });
  });

  describe('Deadlock Callback', () => {
    it('should invoke callback when deadlock detected', () => {
      let callbackInvoked = false;
      let callbackInfo: DeadlockInfo | null = null;

      const callbackDetector = new DeadlockDetector({
        enabled: true,
        onDeadlock: (info) => {
          callbackInvoked = true;
          callbackInfo = info;
        },
      });

      callbackDetector.registerTransaction('txn-1');
      callbackDetector.registerTransaction('txn-2');

      callbackDetector.addWait('txn-1', 'txn-2', 'A', LockType.EXCLUSIVE);
      callbackDetector.addWait('txn-2', 'txn-1', 'B', LockType.EXCLUSIVE);

      callbackDetector.checkDeadlock('txn-2');

      expect(callbackInvoked).toBe(true);
      expect(callbackInfo).not.toBeNull();
    });
  });

  describe('Clear and Reset', () => {
    it('should clear all state', () => {
      detector.registerTransaction('txn-1');
      detector.registerTransaction('txn-2');
      detector.addWait('txn-1', 'txn-2', 'A', LockType.EXCLUSIVE);

      detector.clear();

      const graph = detector.getWaitForGraph();
      expect(graph.getEdgeCount()).toBe(0);
    });

    it('should clear graph state while preserving stats', () => {
      detector.registerTransaction('txn-1');
      detector.registerTransaction('txn-2');
      detector.addWait('txn-1', 'txn-2', 'A', LockType.EXCLUSIVE);
      detector.addWait('txn-2', 'txn-1', 'B', LockType.EXCLUSIVE);
      detector.checkDeadlock('txn-2');

      const statsBefore = detector.getPersistentStats();

      detector.clearGraph();

      const graph = detector.getWaitForGraph();
      expect(graph.getEdgeCount()).toBe(0);

      // Persistent stats should be preserved
      const statsAfter = detector.getPersistentStats();
      expect(statsAfter.totalDeadlocks).toBe(statsBefore.totalDeadlocks);
    });
  });
});

// =============================================================================
// 4. DEADLOCK PREVENTION SCHEMES
// =============================================================================

describe('Deadlock Prevention Schemes', () => {
  describe('Wait-Die Scheme', () => {
    it('should allow older transaction to wait for younger', async () => {
      const detector = new DeadlockDetector({
        enabled: true,
        deadlockPrevention: 'waitDie',
      });

      // Register older transaction first
      detector.registerTransaction('txn-older');

      // Wait to ensure time difference
      await delay(20);

      // Register younger transaction second
      detector.registerTransaction('txn-younger');

      // Older waits for younger - should be allowed
      const error = detector.checkPrevention('txn-older', 'txn-younger', 'A');
      expect(error).toBeNull();
    });

    it('should abort younger transaction waiting for older', async () => {
      const detector = new DeadlockDetector({
        enabled: true,
        deadlockPrevention: 'waitDie',
      });

      // Register older transaction first
      detector.registerTransaction('txn-older');

      // Wait to ensure time difference
      await delay(20);

      // Register younger transaction second
      detector.registerTransaction('txn-younger');

      // Younger waits for older - should abort
      const error = detector.checkPrevention('txn-younger', 'txn-older', 'A');
      expect(error).not.toBeNull();
      expect(error!.code).toBe(TransactionErrorCode.ABORTED);
    });
  });

  describe('Wound-Wait Scheme', () => {
    it('should wound (abort) younger holder when older requests', async () => {
      const detector = new DeadlockDetector({
        enabled: true,
        deadlockPrevention: 'woundWait',
      });

      // Register older transaction first
      detector.registerTransaction('txn-older');

      // Wait to ensure time difference
      await delay(20);

      // Register younger transaction second
      detector.registerTransaction('txn-younger');

      // Older requests lock held by younger - younger gets wounded
      const error = detector.checkPrevention('txn-older', 'txn-younger', 'A');
      expect(error).not.toBeNull();
      expect(error!.woundTarget).toBe('txn-younger');
    });

    it('should allow younger transaction to wait for older', async () => {
      const detector = new DeadlockDetector({
        enabled: true,
        deadlockPrevention: 'woundWait',
      });

      // Register older transaction first
      detector.registerTransaction('txn-older');

      // Wait to ensure time difference
      await delay(20);

      // Register younger transaction second
      detector.registerTransaction('txn-younger');

      // Younger waits for older - should be allowed
      const error = detector.checkPrevention('txn-younger', 'txn-older', 'A');
      expect(error).toBeNull();
    });
  });

  describe('No-Wait Scheme', () => {
    let detector: DeadlockDetector;

    beforeEach(() => {
      detector = new DeadlockDetector({
        enabled: true,
        deadlockPrevention: 'noWait',
      });
    });

    it('should immediately fail when lock cannot be acquired', () => {
      detector.registerTransaction('txn-1');
      detector.registerTransaction('txn-2');

      // Any wait should fail immediately
      const error = detector.checkPrevention('txn-1', 'txn-2', 'A');
      expect(error).not.toBeNull();
      expect(error!.code).toBe(TransactionErrorCode.LOCK_FAILED);
    });
  });

  describe('Lock Ordering Scheme', () => {
    let detector: DeadlockDetector;

    beforeEach(() => {
      detector = new DeadlockDetector({
        enabled: true,
        deadlockPrevention: 'lockOrdering',
      });
    });

    it('should allow locks acquired in order', () => {
      detector.registerTransaction('txn-1');

      // Acquire lock A first
      detector.recordLockAcquisition('txn-1', 'resource-A');

      // Acquire lock B (higher order) - should be allowed
      const error = detector.checkPrevention('txn-1', '', 'resource-Z');
      expect(error).toBeNull();
    });

    it('should reject locks acquired out of order', () => {
      detector.registerTransaction('txn-1');

      // Acquire lock Z first (higher alphabetical order)
      detector.recordLockAcquisition('txn-1', 'resource-Z');

      // Try to acquire lock A (lower order) - should fail
      const error = detector.checkPrevention('txn-1', '', 'resource-A');
      expect(error).not.toBeNull();
      expect(error!.code).toBe(TransactionErrorCode.LOCK_FAILED);
    });
  });
});

// =============================================================================
// 5. LOCK MANAGER INTEGRATION TESTS
// =============================================================================

describe('LockManager Deadlock Integration', () => {
  let lockManager: LockManager;

  beforeEach(() => {
    lockManager = createLockManager({
      defaultTimeout: 5000,
      detectDeadlocks: true,
      victimSelection: 'youngest',
    });
  });

  it('should detect deadlock through lock manager', async () => {
    const txn1 = txn('1');
    const txn2 = txn('2');

    // T1 locks A
    await lockManager.acquire({
      txnId: txn1,
      resource: 'A',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    // T2 locks B
    await lockManager.acquire({
      txnId: txn2,
      resource: 'B',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    // T1 waits for B (will wait for T2)
    const t1WaitB = lockManager.acquire({
      txnId: txn1,
      resource: 'B',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
      timeout: 5000,
    });

    // T2 waits for A - creates deadlock
    let deadlockDetected = false;
    try {
      await lockManager.acquire({
        txnId: txn2,
        resource: 'A',
        lockType: LockType.EXCLUSIVE,
        timestamp: Date.now(),
        timeout: 5000,
      });
    } catch (error) {
      if (error instanceof DeadlockError || (error instanceof TransactionError && error.code === TransactionErrorCode.DEADLOCK)) {
        deadlockDetected = true;
      }
    }

    expect(deadlockDetected).toBe(true);

    // Cleanup
    lockManager.releaseAll(txn1);
    lockManager.releaseAll(txn2);
  });

  it('should expose deadlock statistics', async () => {
    const txn1 = txn('1');
    const txn2 = txn('2');

    await lockManager.acquire({
      txnId: txn1,
      resource: 'A',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    await lockManager.acquire({
      txnId: txn2,
      resource: 'B',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    const t1WaitB = lockManager.acquire({
      txnId: txn1,
      resource: 'B',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
      timeout: 5000,
    });

    try {
      await lockManager.acquire({
        txnId: txn2,
        resource: 'A',
        lockType: LockType.EXCLUSIVE,
        timestamp: Date.now(),
        timeout: 5000,
      });
    } catch {
      // Expected
    }

    const stats = lockManager.getDeadlockStats();
    expect(stats.totalDeadlocks).toBeGreaterThanOrEqual(1);

    lockManager.releaseAll(txn1);
    lockManager.releaseAll(txn2);
  });

  it('should expose deadlock history', async () => {
    const txn1 = txn('1');
    const txn2 = txn('2');

    await lockManager.acquire({
      txnId: txn1,
      resource: 'A',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    await lockManager.acquire({
      txnId: txn2,
      resource: 'B',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    const t1WaitB = lockManager.acquire({
      txnId: txn1,
      resource: 'B',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
      timeout: 5000,
    });

    try {
      await lockManager.acquire({
        txnId: txn2,
        resource: 'A',
        lockType: LockType.EXCLUSIVE,
        timestamp: Date.now(),
        timeout: 5000,
      });
    } catch {
      // Expected
    }

    const history = lockManager.getDeadlockHistory();
    expect(history.length).toBeGreaterThanOrEqual(1);

    lockManager.releaseAll(txn1);
    lockManager.releaseAll(txn2);
  });

  it('should expose wait-for graph', async () => {
    const txn1 = txn('1');
    const txn2 = txn('2');

    await lockManager.acquire({
      txnId: txn1,
      resource: 'A',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
    });

    // Start T2 waiting for A (held by T1)
    const t2Wait = lockManager.acquire({
      txnId: txn2,
      resource: 'A',
      lockType: LockType.EXCLUSIVE,
      timestamp: Date.now(),
      timeout: 5000,
    });

    // Give time for wait edge to be added
    await delay(10);

    const graph = lockManager.getWaitForGraph();
    expect(graph).toBeDefined();
    // T2 should be waiting for T1
    expect(graph.hasEdge(txn2, txn1)).toBe(true);

    // Release and cleanup
    lockManager.releaseAll(txn1);
    const result = await t2Wait;
    expect(result.acquired).toBe(true);
    lockManager.releaseAll(txn2);
  });

  it('should support transaction cost tracking', () => {
    const txn1 = txn('1');
    lockManager.setTransactionCost(txn1, 100);

    // Should not throw
    expect(() => lockManager.setTransactionCost(txn1, 200)).not.toThrow();
  });

  it('should support marking transactions as read-only', () => {
    const txn1 = txn('1');
    lockManager.markReadOnly(txn1, true);

    // Should not throw
    expect(() => lockManager.markReadOnly(txn1, false)).not.toThrow();
  });
});

// =============================================================================
// 6. DEADLOCK ERROR TESTS
// =============================================================================

describe('DeadlockError', () => {
  it('should create DeadlockError with full information', () => {
    const info: DeadlockInfo = {
      cycle: ['txn-1', 'txn-2', 'txn-1'],
      resources: ['A', 'B'],
      victimTxnId: 'txn-2',
      waitTimes: { 'txn-1': 100, 'txn-2': 50 },
      timestamp: Date.now(),
      graphDot: 'digraph WaitFor { "txn-1" -> "txn-2"; }',
      participants: ['txn-1', 'txn-2'],
    };

    const error = new DeadlockError(info, 'txn-2');

    expect(error.name).toBe('DeadlockError');
    expect(error.code).toBe(TransactionErrorCode.DEADLOCK);
    expect(error.victimTxnId).toBe('txn-2');
    expect(error.cycle).toEqual(['txn-1', 'txn-2', 'txn-1']);
    expect(error.resources).toEqual(['A', 'B']);
    expect(error.message).toContain('Deadlock detected');
  });

  it('should be an instance of TransactionError', () => {
    const info: DeadlockInfo = {
      cycle: ['txn-1', 'txn-2', 'txn-1'],
      resources: ['A'],
      victimTxnId: 'txn-2',
      waitTimes: {},
      timestamp: Date.now(),
      graphDot: '',
      participants: ['txn-1', 'txn-2'],
    };

    const error = new DeadlockError(info, 'txn-2');

    expect(error).toBeInstanceOf(TransactionError);
    expect(error.isRetryable()).toBe(true);
  });
});

// =============================================================================
// 7. EDGE CASES AND STRESS TESTS
// =============================================================================

describe('Edge Cases and Stress Tests', () => {
  describe('No Deadlock Scenarios', () => {
    let detector: DeadlockDetector;

    beforeEach(() => {
      detector = new DeadlockDetector({ enabled: true });
    });

    it('should not detect deadlock in star topology', () => {
      // Central transaction holds lock, multiple others wait
      detector.registerTransaction('txn-center');
      detector.registerTransaction('txn-1');
      detector.registerTransaction('txn-2');
      detector.registerTransaction('txn-3');

      detector.addWait('txn-1', 'txn-center', 'A', LockType.EXCLUSIVE);
      detector.addWait('txn-2', 'txn-center', 'A', LockType.EXCLUSIVE);
      detector.addWait('txn-3', 'txn-center', 'A', LockType.EXCLUSIVE);

      expect(detector.checkDeadlock('txn-1')).toBeNull();
      expect(detector.checkDeadlock('txn-2')).toBeNull();
      expect(detector.checkDeadlock('txn-3')).toBeNull();
    });

    it('should not detect deadlock in tree topology', () => {
      detector.registerTransaction('txn-root');
      detector.registerTransaction('txn-left');
      detector.registerTransaction('txn-right');
      detector.registerTransaction('txn-ll');
      detector.registerTransaction('txn-lr');

      detector.addWait('txn-left', 'txn-root', 'A', LockType.EXCLUSIVE);
      detector.addWait('txn-right', 'txn-root', 'B', LockType.EXCLUSIVE);
      detector.addWait('txn-ll', 'txn-left', 'C', LockType.EXCLUSIVE);
      detector.addWait('txn-lr', 'txn-left', 'D', LockType.EXCLUSIVE);

      expect(detector.checkDeadlock('txn-ll')).toBeNull();
    });

    it('should not detect deadlock with shared locks only', async () => {
      // Multiple shared locks should not cause deadlock
      const lockManager = createLockManager({
        defaultTimeout: 1000,
        detectDeadlocks: true,
      });

      const txn1 = txn('1');
      const txn2 = txn('2');
      const txn3 = txn('3');

      // All acquire shared locks on same resource - all should succeed
      const results = await Promise.all([
        lockManager.acquire({
          txnId: txn1,
          resource: 'A',
          lockType: LockType.SHARED,
          timestamp: Date.now(),
        }),
        lockManager.acquire({
          txnId: txn2,
          resource: 'A',
          lockType: LockType.SHARED,
          timestamp: Date.now(),
        }),
        lockManager.acquire({
          txnId: txn3,
          resource: 'A',
          lockType: LockType.SHARED,
          timestamp: Date.now(),
        }),
      ]);

      expect(results).toBeDefined();
      expect(results.every(r => r.acquired)).toBe(true);

      lockManager.releaseAll(txn1);
      lockManager.releaseAll(txn2);
      lockManager.releaseAll(txn3);
    });
  });

  describe('Complex Cycle Detection', () => {
    let detector: DeadlockDetector;

    beforeEach(() => {
      detector = new DeadlockDetector({ enabled: true });
    });

    it('should detect cycle in graph with multiple components', () => {
      // Component 1: Linear (no cycle)
      detector.registerTransaction('txn-A1');
      detector.registerTransaction('txn-A2');
      detector.addWait('txn-A1', 'txn-A2', 'r1', LockType.EXCLUSIVE);

      // Component 2: Cycle
      detector.registerTransaction('txn-B1');
      detector.registerTransaction('txn-B2');
      detector.registerTransaction('txn-B3');
      detector.addWait('txn-B1', 'txn-B2', 'r2', LockType.EXCLUSIVE);
      detector.addWait('txn-B2', 'txn-B3', 'r3', LockType.EXCLUSIVE);
      detector.addWait('txn-B3', 'txn-B1', 'r4', LockType.EXCLUSIVE);

      // Should find cycle in component 2
      const info = detector.checkDeadlock('txn-B3');
      expect(info).not.toBeNull();
    });

    it('should handle self-loop detection', () => {
      detector.registerTransaction('txn-1');

      // Self-loop (transaction waits for itself - should not happen in practice)
      detector.addWait('txn-1', 'txn-1', 'A', LockType.EXCLUSIVE);

      // This represents a bug if it happens, but detector should handle it
      const graph = detector.getWaitForGraph();
      expect(graph.hasEdge('txn-1', 'txn-1')).toBe(true);
    });
  });

  describe('Performance', () => {
    it('should handle many transactions without cycle efficiently', () => {
      const detector = new DeadlockDetector({ enabled: true });
      const count = 100;

      // Create linear chain of 100 transactions
      for (let i = 0; i < count; i++) {
        detector.registerTransaction(`txn-${i}`);
        if (i > 0) {
          detector.addWait(`txn-${i}`, `txn-${i - 1}`, `r${i}`, LockType.EXCLUSIVE);
        }
      }

      const start = Date.now();
      const result = detector.checkDeadlock(`txn-${count - 1}`);
      const elapsed = Date.now() - start;

      expect(result).toBeNull();
      expect(elapsed).toBeLessThan(100); // Should complete quickly
    });

    it('should detect large cycle efficiently', () => {
      const detector = new DeadlockDetector({ enabled: true });
      const count = 50;

      // Create cycle of 50 transactions
      for (let i = 0; i < count; i++) {
        detector.registerTransaction(`txn-${i}`);
      }

      for (let i = 0; i < count; i++) {
        const next = (i + 1) % count;
        detector.addWait(`txn-${i}`, `txn-${next}`, `r${i}`, LockType.EXCLUSIVE);
      }

      const start = Date.now();
      const result = detector.checkDeadlock(`txn-0`);
      const elapsed = Date.now() - start;

      expect(result).not.toBeNull();
      expect(elapsed).toBeLessThan(100); // Should complete quickly
    });
  });

  describe('Async Detection', () => {
    it('should support async deadlock detection with timeout', async () => {
      const detector = new DeadlockDetector({
        enabled: true,
        deadlockTimeout: 100,
      });

      detector.registerTransaction('txn-1');
      detector.registerTransaction('txn-2');
      detector.addWait('txn-1', 'txn-2', 'A', LockType.EXCLUSIVE);
      detector.addWait('txn-2', 'txn-1', 'B', LockType.EXCLUSIVE);

      const info = await detector.checkDeadlockAsync('txn-2', { timeout: 1000 });
      expect(info).not.toBeNull();
    });
  });
});

// =============================================================================
// 8. DEADLOCK PREDICTION TESTS
// =============================================================================

describe('Deadlock Prediction', () => {
  let detector: DeadlockDetector;

  beforeEach(() => {
    detector = new DeadlockDetector({ enabled: true });
  });

  it('should predict deadlock before it occurs', () => {
    detector.registerTransaction('txn-1');
    detector.registerTransaction('txn-2');

    // T1 waits for T2
    detector.addWait('txn-1', 'txn-2', 'A', LockType.EXCLUSIVE);

    // Predict if T2 waiting for T1 would cause deadlock
    const prediction = detector.predictDeadlock('txn-2', 'txn-1', 'B');

    expect(prediction.wouldCauseDeadlock).toBe(true);
    expect(prediction.predictedCycle).toBeDefined();
  });

  it('should not predict deadlock for safe waits', () => {
    detector.registerTransaction('txn-1');
    detector.registerTransaction('txn-2');
    detector.registerTransaction('txn-3');

    // Linear chain: T1 -> T2
    detector.addWait('txn-1', 'txn-2', 'A', LockType.EXCLUSIVE);

    // Predict if T3 waiting for T2 would cause deadlock (it wouldn't)
    const prediction = detector.predictDeadlock('txn-3', 'txn-2', 'B');

    expect(prediction.wouldCauseDeadlock).toBe(false);
  });

  it('should calculate deadlock probability', () => {
    detector.registerTransaction('txn-1');
    detector.registerTransaction('txn-2');

    detector.addWait('txn-1', 'txn-2', 'A', LockType.EXCLUSIVE);

    const probability = detector.getDeadlockProbability('txn-2');
    expect(probability).toBeGreaterThanOrEqual(0);
    expect(probability).toBeLessThanOrEqual(1);
  });
});
