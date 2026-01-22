/**
 * Concurrent Query Planning Tests (RED Phase TDD)
 *
 * These tests document the race condition in global planNodeIdCounter.
 *
 * PROBLEM STATEMENT:
 * ===================
 * The query planner uses a global mutable counter (`planNodeIdCounter`) to generate
 * unique IDs for plan nodes. This creates several concurrency issues:
 *
 * 1. RACE CONDITION: When multiple queries are planned simultaneously in a Durable
 *    Object (or across Workers), they can receive duplicate plan node IDs because
 *    the counter increment (++planNodeIdCounter) is not atomic.
 *
 * 2. ID COLLISION: In distributed scenarios (multiple DO instances), each instance
 *    has its own copy of the counter starting at 0, leading to ID collisions across
 *    instances.
 *
 * 3. NON-DETERMINISM: Tests cannot reliably predict plan node IDs because the
 *    counter state depends on previous test runs and execution order.
 *
 * AFFECTED CODE:
 * ==============
 * - packages/dosql/src/engine/types.ts:815-825 (nextPlanId, resetPlanIds)
 * - packages/dosql/src/planner/types.ts:501-509 (nextPlanNodeId, resetPlanNodeIds)
 *
 * CURRENT IMPLEMENTATION:
 * =======================
 *   let planNodeIdCounter = 0;
 *   export function nextPlanId(): number {
 *     return ++planNodeIdCounter;
 *   }
 *
 * The ++planNodeIdCounter operation is NOT atomic in JavaScript. While JS is
 * single-threaded, the Workers runtime can interleave async operations. When
 * query planning involves async operations (e.g., fetching statistics), the
 * counter can be read by multiple concurrent planners before any increment,
 * resulting in duplicate IDs.
 *
 * SOLUTION REQUIREMENTS:
 * ======================
 * 1. Request-scoped ID generation (each planning context gets isolated IDs)
 * 2. Or: Use UUIDs/ULIDs instead of sequential integers
 * 3. Or: Include request/context identifier in the plan node ID
 * 4. Tests should be deterministic without requiring global state reset
 *
 * @see https://developers.cloudflare.com/durable-objects/
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  nextPlanNodeId,
  resetPlanNodeIds,
  createScanNode,
  createJoinNode,
  createFilterNode,
  createSortNode,
  createLimitNode,
  createAggregateNode,
  type PhysicalPlanNode,
  type ScanNode,
} from '../index.js';
import { nextPlanId, resetPlanIds, col } from '../../engine/types.js';

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Simulate planning a simple query that creates multiple plan nodes.
 * Returns the plan node IDs generated during planning.
 */
function planSimpleQuery(): number[] {
  const ids: number[] = [];

  // Simulates: SELECT id, name FROM users WHERE status = 'active' ORDER BY name LIMIT 10
  const scanNode = createScanNode('users', ['id', 'name', 'status']);
  ids.push(scanNode.id);

  const filterNode = createFilterNode(scanNode, {
    type: 'comparison',
    op: 'eq',
    left: { type: 'columnRef', column: 'status' },
    right: { type: 'literal', value: 'active', dataType: 'string' },
  });
  ids.push(filterNode.id);

  const sortNode = createSortNode(filterNode, [
    { expr: { type: 'columnRef', column: 'name' }, direction: 'asc' },
  ]);
  ids.push(sortNode.id);

  const limitNode = createLimitNode(sortNode, 10);
  ids.push(limitNode.id);

  return ids;
}

/**
 * Simulate planning a complex join query.
 */
function planJoinQuery(): number[] {
  const ids: number[] = [];

  const usersScan = createScanNode('users', ['id', 'name']);
  ids.push(usersScan.id);

  const ordersScan = createScanNode('orders', ['id', 'user_id', 'amount']);
  ids.push(ordersScan.id);

  const joinNode = createJoinNode(usersScan, ordersScan, 'inner', 'hash', {
    type: 'comparison',
    op: 'eq',
    left: { type: 'columnRef', table: 'users', column: 'id' },
    right: { type: 'columnRef', table: 'orders', column: 'user_id' },
  });
  ids.push(joinNode.id);

  return ids;
}

/**
 * Collect all plan node IDs from a plan tree.
 */
function collectPlanIds(node: PhysicalPlanNode): number[] {
  const ids = [node.id];
  for (const child of node.children) {
    ids.push(...collectPlanIds(child));
  }
  return ids;
}

// =============================================================================
// TEST SUITE: CONCURRENT PLANNING RACE CONDITIONS
// =============================================================================

describe('Concurrent Query Planning (Race Condition Tests)', () => {
  /**
   * Reset global state before each test.
   * NOTE: This is itself a code smell - tests should not depend on global state.
   */
  beforeEach(() => {
    resetPlanNodeIds();
    resetPlanIds();
  });

  // ===========================================================================
  // SCENARIO 1: Two simultaneous queries should get unique IDs
  // ===========================================================================
  describe('Scenario 1: Two concurrent queries', () => {
    /**
     * This test documents the issue with global counter sharing.
     *
     * While this specific test passes due to JS single-threading,
     * the underlying issue is that queries share a global ID namespace.
     * In a distributed DO environment, this creates real problems.
     */
    it('should produce unique plan node IDs when planned simultaneously', async () => {
      // Plan two queries "concurrently"
      const [query1Ids, query2Ids] = await Promise.all([
        Promise.resolve(planSimpleQuery()),
        Promise.resolve(planSimpleQuery()),
      ]);

      // Combine all IDs from both queries
      const allIds = [...query1Ids, ...query2Ids];
      const uniqueIds = new Set(allIds);

      // With current implementation, IDs ARE unique because JS is single-threaded
      // But they share the global sequence, which is problematic
      expect(uniqueIds.size).toBe(allIds.length);

      // However, the IDs are NOT isolated per query
      // Query 1 gets [1,2,3,4], Query 2 gets [5,6,7,8]
      // This means queries depend on execution order
      const query1Set = new Set(query1Ids);
      const query2Set = new Set(query2Ids);
      const intersection = [...query1Set].filter(id => query2Set.has(id));
      expect(intersection).toHaveLength(0);
    });

    /**
     * This test FAILS because queries share the global ID sequence.
     * Each query should have its own ID sequence starting from 1.
     */
    it.fails('each query should have independent ID sequences starting from 1', async () => {
      // Plan two queries
      const query1Ids = planSimpleQuery(); // Gets [1,2,3,4]
      const query2Ids = planSimpleQuery(); // Gets [5,6,7,8] - NOT [1,2,3,4]

      // EXPECTED: Each query has its own sequence starting at 1
      // ACTUAL: Queries share the global sequence
      expect(query1Ids[0]).toBe(1);
      expect(query2Ids[0]).toBe(1); // FAILS: query2Ids[0] is 5, not 1
    });

    /**
     * Test that each query's plan tree has internally consistent IDs.
     */
    it('should have consistent IDs within a single query plan', () => {
      const ids = planSimpleQuery();

      // All IDs within a single plan should be unique
      const uniqueIds = new Set(ids);
      expect(uniqueIds.size).toBe(ids.length);

      // IDs should be sequential (with current implementation)
      // This is a property we might want to maintain for debugging
      for (let i = 1; i < ids.length; i++) {
        expect(ids[i]).toBeGreaterThan(ids[i - 1]);
      }
    });
  });

  // ===========================================================================
  // SCENARIO 2: 10 concurrent queries all get unique IDs
  // ===========================================================================
  describe('Scenario 2: Ten concurrent queries', () => {
    /**
     * This test passes due to JS single-threading, but documents the shared sequence issue.
     */
    it('should produce unique IDs across 10 concurrent query plans (shared sequence)', async () => {
      const concurrentPlans = 10;

      // Plan 10 queries concurrently
      const allQueryIds = await Promise.all(
        Array.from({ length: concurrentPlans }, () =>
          Promise.resolve(planJoinQuery())
        )
      );

      // Flatten all IDs
      const allIds = allQueryIds.flat();
      const uniqueIds = new Set(allIds);

      // Each join query creates 3 nodes, so 10 queries = 30 unique IDs expected
      expect(allIds).toHaveLength(concurrentPlans * 3);
      expect(uniqueIds.size).toBe(allIds.length);
    });

    /**
     * This test FAILS because queries share a global sequence instead of having
     * isolated ID spaces.
     */
    it.fails('each of 10 concurrent queries should have IDs starting at 1', async () => {
      const concurrentPlans = 10;

      const allQueryIds = await Promise.all(
        Array.from({ length: concurrentPlans }, () =>
          Promise.resolve(planJoinQuery())
        )
      );

      // EXPECTED: Each query has [1, 2, 3] (isolated sequences)
      // ACTUAL: Query 1 gets [1,2,3], Query 2 gets [4,5,6], etc.
      for (const queryIds of allQueryIds) {
        expect(queryIds[0]).toBe(1);
      }
    });

    /**
     * Verify no query shares IDs with another query - passes because of global sequence.
     */
    it('should have no ID overlap between queries (but IDs are not isolated)', async () => {
      const concurrentPlans = 10;

      const allQueryIds = await Promise.all(
        Array.from({ length: concurrentPlans }, () =>
          Promise.resolve(planSimpleQuery())
        )
      );

      // Check each pair of queries for overlapping IDs
      for (let i = 0; i < allQueryIds.length; i++) {
        const setI = new Set(allQueryIds[i]);
        for (let j = i + 1; j < allQueryIds.length; j++) {
          const setJ = new Set(allQueryIds[j]);
          const overlap = [...setI].filter(id => setJ.has(id));
          expect(overlap).toHaveLength(0);
        }
      }
    });
  });

  // ===========================================================================
  // SCENARIO 3: Rapid sequential planning doesn't share IDs
  // ===========================================================================
  describe('Scenario 3: Rapid sequential planning', () => {
    /**
     * Even without true concurrency, rapid sequential planning should
     * produce unique IDs. This test passes with current implementation
     * but documents the requirement.
     */
    it('should produce unique IDs for sequential rapid planning', () => {
      const allIds: number[] = [];

      // Rapidly plan 100 queries sequentially
      for (let i = 0; i < 100; i++) {
        allIds.push(...planSimpleQuery());
      }

      const uniqueIds = new Set(allIds);
      expect(uniqueIds.size).toBe(allIds.length);
    });

    /**
     * After resetting, IDs should start from 1 again.
     * This behavior is problematic if any references to old IDs still exist.
     */
    it.fails('should not reuse IDs after reset if old plans are still referenced', () => {
      // Plan first query
      const firstPlanIds = planSimpleQuery();
      const firstPlanIdSet = new Set(firstPlanIds);

      // Reset counter (simulating a new request context that resets state)
      resetPlanNodeIds();

      // Plan second query - this will reuse IDs!
      const secondPlanIds = planSimpleQuery();
      const secondPlanIdSet = new Set(secondPlanIds);

      // EXPECTED: No overlap even after reset (if proper isolation)
      // ACTUAL: Full overlap because counter restarts at 0
      const overlap = [...firstPlanIdSet].filter(id => secondPlanIdSet.has(id));
      expect(overlap).toHaveLength(0);
    });
  });

  // ===========================================================================
  // SCENARIO 4: Plan IDs are consistent within a single query
  // ===========================================================================
  describe('Scenario 4: Internal plan consistency', () => {
    /**
     * All nodes in a single plan tree should have unique IDs.
     */
    it('should have unique IDs for all nodes in a plan tree', () => {
      const scan = createScanNode('users', ['id', 'name']);
      const filter = createFilterNode(scan, {
        type: 'comparison',
        op: 'eq',
        left: { type: 'columnRef', column: 'status' },
        right: { type: 'literal', value: 'active', dataType: 'string' },
      });
      const sort = createSortNode(filter, [
        { expr: { type: 'columnRef', column: 'name' }, direction: 'asc' },
      ]);
      const limit = createLimitNode(sort, 10);

      const allIds = collectPlanIds(limit);
      const uniqueIds = new Set(allIds);

      expect(uniqueIds.size).toBe(allIds.length);
    });

    /**
     * Child node IDs should be different from parent IDs.
     */
    it('should have child IDs different from parent IDs', () => {
      const leftScan = createScanNode('users', ['id']);
      const rightScan = createScanNode('orders', ['user_id']);
      const join = createJoinNode(leftScan, rightScan, 'inner', 'hash');

      expect(join.id).not.toBe(leftScan.id);
      expect(join.id).not.toBe(rightScan.id);
      expect(leftScan.id).not.toBe(rightScan.id);
    });
  });

  // ===========================================================================
  // SCENARIO 5: Different query contexts don't share ID sequences
  // ===========================================================================
  describe('Scenario 5: Query context isolation', () => {
    /**
     * This test FAILS because there's no concept of query context.
     * All queries share the same global counter.
     * With proper isolation, each query should have IDs starting at 1.
     */
    it.fails('should have isolated ID sequences per query context', () => {
      // Simulate two separate "request contexts"
      // In a proper implementation, each context would have its own ID generator

      // Plan queries in each "context"
      // Currently, there's no way to pass context to the planner
      const ids1 = planSimpleQuery();  // Gets [1, 2, 3, 4]
      const ids2 = planSimpleQuery();  // Gets [5, 6, 7, 8] - should be [1, 2, 3, 4]

      // EXPECTED: Both queries should start at 1 (isolated sequences)
      // ACTUAL: ids1[0] = 1, ids2[0] = 5 (shared sequence)
      expect(ids1[0]).toBe(1);
      expect(ids2[0]).toBe(1); // FAILS: ids2[0] is 5
    });

    /**
     * Plans from different contexts should be independently identifiable.
     */
    it.fails('should include context identifier in plan node IDs', () => {
      const scan = createScanNode('users', ['id']);

      // EXPECTED: Plan node ID should include some form of context/request identifier
      // e.g., "ctx_abc123_1" or { context: 'abc123', seq: 1 }
      // ACTUAL: Just a plain number

      // This would require a different ID format
      expect(typeof scan.id).not.toBe('number');
    });
  });

  // ===========================================================================
  // SCENARIO 6: ID sequence is predictable for deterministic testing
  // ===========================================================================
  describe('Scenario 6: Deterministic testing', () => {
    /**
     * After reset, the first ID should be 1.
     */
    it('should start at 1 after reset', () => {
      resetPlanNodeIds();
      const id = nextPlanNodeId();
      expect(id).toBe(1);
    });

    /**
     * IDs should increment sequentially.
     */
    it('should increment sequentially', () => {
      resetPlanNodeIds();
      expect(nextPlanNodeId()).toBe(1);
      expect(nextPlanNodeId()).toBe(2);
      expect(nextPlanNodeId()).toBe(3);
    });
  });
});

// =============================================================================
// SCENARIO 6b: Tests that show test isolation issues
// This describe block intentionally does NOT reset the counter in beforeEach
// to demonstrate the problem with global state.
// =============================================================================
describe('Scenario 6b: Test isolation issues (no beforeEach reset)', () => {
  /**
   * This test FAILS because test isolation requires manual reset.
   * Tests should be deterministic without depending on execution order.
   *
   * NOTE: This test's success/failure depends on test execution order,
   * which is exactly the problem we're documenting.
   */
  it.fails('should be deterministic without requiring explicit reset', () => {
    // Don't call resetPlanNodeIds() - simulate a test running in isolation

    // EXPECTED: First ID should be 1 in a properly isolated test
    // ACTUAL: ID depends on how many IDs were generated in previous tests
    const scan = createScanNode('users', ['id']);
    expect(scan.id).toBe(1);
  });

  /**
   * This test FAILS because we cannot predict the starting ID.
   */
  it.fails('should have predictable starting ID without reset', () => {
    // This test runs without knowing what ID we'll get
    const ids = planSimpleQuery();

    // EXPECTED: IDs should be [1, 2, 3, 4] (isolated test)
    // ACTUAL: IDs depend on test execution order
    expect(ids).toEqual([1, 2, 3, 4]);
  });

  /**
   * Demonstrates that tests must use beforeEach reset for determinism.
   */
  it('requires explicit reset for deterministic testing (current workaround)', () => {
    // This is the workaround: explicitly reset before each test
    resetPlanNodeIds();

    const ids = planSimpleQuery();
    expect(ids).toEqual([1, 2, 3, 4]);
  });
});

// =============================================================================
// SCENARIOS 7-10: Additional concurrent planning tests
// These have their own beforeEach reset
// =============================================================================
describe('Concurrent Query Planning (Additional Scenarios)', () => {
  beforeEach(() => {
    resetPlanNodeIds();
    resetPlanIds();
  });

  // ===========================================================================
  // SCENARIO 7: ID overflow handling
  // ===========================================================================
  describe('Scenario 7: ID overflow handling', () => {
    /**
     * This test documents expected behavior when approaching MAX_SAFE_INTEGER.
     * While unlikely in practice, the implementation should handle this gracefully.
     */
    it.fails('should handle approaching MAX_SAFE_INTEGER', () => {
      // Skip this test in normal runs - it's for documentation
      // In a real implementation, we'd need to handle overflow

      // The current implementation will eventually exceed MAX_SAFE_INTEGER
      // after 9,007,199,254,740,991 plan nodes are created
      // At that point, IDs will lose precision

      // A proper implementation should:
      // 1. Detect when approaching the limit
      // 2. Either throw an error or use a different ID format (e.g., bigint, string)

      // Simulate setting counter near the limit
      // This would require access to the internal counter, which we don't have
      expect(true).toBe(false); // Placeholder - test documents the requirement
    });

    /**
     * IDs should remain within safe integer range.
     */
    it('should produce IDs within safe integer range', () => {
      const ids: number[] = [];
      for (let i = 0; i < 1000; i++) {
        ids.push(nextPlanNodeId());
      }

      for (const id of ids) {
        expect(id).toBeLessThanOrEqual(Number.MAX_SAFE_INTEGER);
        expect(Number.isSafeInteger(id)).toBe(true);
      }
    });
  });

  // ===========================================================================
  // SCENARIO 8: ID uniqueness across DO instances (distributed scenario)
  // ===========================================================================
  describe('Scenario 8: Distributed DO instances', () => {
    /**
     * This test FAILS because each DO instance has its own counter starting at 0.
     * In a distributed system, this leads to ID collisions across instances.
     */
    it.fails('should produce globally unique IDs across DO instances', async () => {
      // Simulate two DO instances (each with their own global counter)
      // In reality, each DO has isolated memory, so counters start at 0

      // Instance 1
      resetPlanNodeIds();
      const instance1Ids = planSimpleQuery();

      // Instance 2 (simulated by resetting)
      resetPlanNodeIds();
      const instance2Ids = planSimpleQuery();

      // EXPECTED: IDs should be globally unique (include instance identifier)
      // ACTUAL: Both instances produce [1, 2, 3, 4]
      const allIds = [...instance1Ids, ...instance2Ids];
      const uniqueIds = new Set(allIds);

      expect(uniqueIds.size).toBe(allIds.length);
    });

    /**
     * Plan node IDs should include DO instance identifier for global uniqueness.
     */
    it.fails('should include DO instance identifier in plan IDs', () => {
      const scan = createScanNode('users', ['id']);

      // EXPECTED: ID format like "DO_abc123:1" or { doId: 'abc123', seq: 1 }
      // ACTUAL: Just a plain number

      // In a distributed system, we need:
      // - DO instance ID (from DurableObjectId.toString())
      // - Request ID (from request headers or generated)
      // - Sequence number (local counter)

      expect(typeof scan.id).toBe('string');
    });

    /**
     * Plans from different DO instances should be mergeable without ID conflicts.
     */
    it.fails('should allow merging plans from different DO instances', () => {
      // Use case: A coordinator DO gathers sub-plans from shard DOs
      // and needs to combine them into a single execution plan

      // Simulate shard 1 plan
      resetPlanNodeIds();
      const shard1Plan = createScanNode('users_shard1', ['id']);

      // Simulate shard 2 plan
      resetPlanNodeIds();
      const shard2Plan = createScanNode('users_shard2', ['id']);

      // Both have id = 1 with current implementation
      // This would cause issues when combining into a merge plan

      expect(shard1Plan.id).not.toBe(shard2Plan.id);
    });
  });

  // ===========================================================================
  // SCENARIO 9: Async planning with interleaving
  // ===========================================================================
  describe('Scenario 9: Async planning interleaving', () => {
    /**
     * Simulates async planning where statistics are fetched.
     * This creates opportunities for counter race conditions.
     */
    async function planWithAsyncStats(): Promise<number[]> {
      const ids: number[] = [];

      // Read counter before "async" operation
      const scan = createScanNode('users', ['id']);
      ids.push(scan.id);

      // Simulate async statistics fetch
      await new Promise(resolve => setTimeout(resolve, 0));

      // Continue planning after async gap
      const filter = createFilterNode(scan, {
        type: 'comparison',
        op: 'eq',
        left: { type: 'columnRef', column: 'id' },
        right: { type: 'literal', value: 1, dataType: 'number' },
      });
      ids.push(filter.id);

      return ids;
    }

    /**
     * This test documents async behavior - IDs are unique but interleaved.
     */
    it('should produce unique IDs with async planning (interleaved order)', async () => {
      // Run multiple async plannings concurrently
      const results = await Promise.all([
        planWithAsyncStats(),
        planWithAsyncStats(),
        planWithAsyncStats(),
      ]);

      const allIds = results.flat();
      const uniqueIds = new Set(allIds);

      // IDs are unique due to atomic ++ in single-threaded JS
      expect(uniqueIds.size).toBe(allIds.length);

      // However, the IDs are interleaved: [1, 2, 3, 4, 5, 6] across all plans
      // Not [1,2], [3,4], [5,6] per plan
    });

    /**
     * This test FAILS because async plans don't have isolated ID sequences.
     */
    it.fails('each async plan should have sequential IDs within itself', async () => {
      const results = await Promise.all([
        planWithAsyncStats(),
        planWithAsyncStats(),
        planWithAsyncStats(),
      ]);

      // Check that each plan has sequential IDs
      // EXPECTED: Each plan has [1, 2] (isolated)
      // ACTUAL: Plans interleave, e.g., [[1, 4], [2, 5], [3, 6]]
      for (const planIds of results) {
        expect(planIds[1] - planIds[0]).toBe(1);
      }
    });
  });

  // ===========================================================================
  // SCENARIO 10: Request-scoped ID generation
  // ===========================================================================
  describe('Scenario 10: Request-scoped ID generation (desired behavior)', () => {
    /**
     * These tests document the DESIRED behavior with request-scoped IDs.
     * They all FAIL with the current global counter implementation.
     */

    it.fails('should support creating an isolated planning context', () => {
      // EXPECTED API:
      // const ctx1 = createPlanningContext();
      // const ctx2 = createPlanningContext();
      // ctx1.nextId() and ctx2.nextId() are independent

      // ACTUAL: No such API exists
      expect(typeof createPlanNodeId).toBe('function');
    });

    it.fails('should support factory function with context parameter', () => {
      // EXPECTED API:
      // const ctx = createPlanningContext();
      // const scan = createScanNode('users', ['id'], { context: ctx });
      // scan.id is scoped to ctx

      // ACTUAL: createScanNode uses global nextPlanNodeId()
      const scan = createScanNode('users', ['id']);
      expect(scan).toHaveProperty('contextId');
    });

    it.fails('should reset context without affecting other contexts', () => {
      // EXPECTED:
      // const ctx1 = createPlanningContext();
      // const ctx2 = createPlanningContext();
      // ctx1.reset() should not affect ctx2's counter

      // ACTUAL: resetPlanNodeIds() resets the global counter
      expect(true).toBe(false);
    });
  });
});

// =============================================================================
// ADDITIONAL TEST: Verify global counter issues
// =============================================================================

describe('Global Counter Issues', () => {
  beforeEach(() => {
    resetPlanNodeIds();
  });

  /**
   * This test documents that while synchronous counter access is safe,
   * the shared global state is still problematic for isolation.
   */
  it('synchronous counter access produces unique IDs', async () => {
    const iterations = 1000;
    const concurrency = 10;

    const generateIds = async (): Promise<number[]> => {
      const ids: number[] = [];
      for (let i = 0; i < iterations / concurrency; i++) {
        ids.push(nextPlanNodeId());
        // Small delay to allow interleaving
        if (i % 10 === 0) {
          await new Promise(resolve => setImmediate(resolve));
        }
      }
      return ids;
    };

    const allResults = await Promise.all(
      Array.from({ length: concurrency }, generateIds)
    );

    const allIds = allResults.flat();
    const uniqueIds = new Set(allIds);

    // In JS single-threaded environment, IDs are unique
    expect(uniqueIds.size).toBe(iterations);
    expect(allIds.length).toBe(iterations);
  });

  /**
   * This test FAILS: IDs are interleaved across concurrent tasks,
   * not sequential within each task.
   */
  it.fails('should produce sequential IDs within each concurrent task', async () => {
    const tasksCount = 5;
    const idsPerTask = 10;

    const generateIds = async (): Promise<number[]> => {
      const ids: number[] = [];
      for (let i = 0; i < idsPerTask; i++) {
        ids.push(nextPlanNodeId());
        await new Promise(resolve => setImmediate(resolve));
      }
      return ids;
    };

    const allResults = await Promise.all(
      Array.from({ length: tasksCount }, generateIds)
    );

    // EXPECTED: Each task has sequential IDs [1-10], [11-20], etc.
    // ACTUAL: IDs are interleaved, e.g., [[1,6,11,...], [2,7,12,...], ...]
    for (const taskIds of allResults) {
      for (let i = 1; i < taskIds.length; i++) {
        expect(taskIds[i] - taskIds[i - 1]).toBe(1);
      }
    }
  });

  /**
   * Verify that the read-modify-write of ++counter is not atomic.
   */
  it('documents that ++ is read-modify-write (but safe in single-threaded JS)', () => {
    // This test documents that ++ is read-modify-write
    // In a tight loop without async, JS single-threading prevents races
    // But with async operations, the gap between read and write can be exploited

    let counter = 0;

    // Synchronous: no race possible
    const syncIds = Array.from({ length: 100 }, () => ++counter);
    expect(new Set(syncIds).size).toBe(100);

    // The issue arises when:
    // 1. Task A reads counter (value: 100)
    // 2. Task A yields (e.g., await)
    // 3. Task B reads counter (value: 100)
    // 4. Task B increments and writes (value: 101)
    // 5. Task A resumes and increments based on old read (value: 101 again!)
    // Both tasks got 101, causing a duplicate

    // In pure JS, ++ is atomic enough, but in Workers with shared isolates
    // or with explicit counter state capture before await, races can occur
    expect(true).toBe(true);
  });

  /**
   * This test FAILS: Demonstrates the reset problem.
   */
  it.fails('reset should not cause ID reuse if old plans exist', () => {
    const plan1 = createScanNode('users', ['id']);
    const id1 = plan1.id;

    // Another component resets the counter (simulating new request)
    resetPlanNodeIds();

    const plan2 = createScanNode('orders', ['id']);
    const id2 = plan2.id;

    // EXPECTED: IDs are globally unique even after reset
    // ACTUAL: id1 === id2 === 1
    expect(id1).not.toBe(id2);
  });
});

// Placeholder for the expected API (to make TypeScript happy in failing tests)
declare function createPlanningContext(): unknown;
declare function createPlanNodeId(): number;
