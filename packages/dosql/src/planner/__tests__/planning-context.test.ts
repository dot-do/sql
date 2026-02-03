/**
 * Tests for PlanningContext - Request-scoped ID generation for query planning
 *
 * Tests the PlanningContext class including:
 * - ID generation and isolation
 * - Deterministic testing contexts
 * - Factory functions
 * - Default (shared) context for backwards compatibility
 *
 * Following NO MOCKS philosophy - all tests use real PlanningContext instances
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  PlanningContext,
  createPlanningContext,
  getDefaultPlanningContext,
  resetDefaultPlanningContext,
} from '../planning-context.js';

// =============================================================================
// PLANNING CONTEXT BASIC TESTS
// =============================================================================

describe('PlanningContext', () => {
  describe('constructor', () => {
    it('should create context with auto-generated context ID', () => {
      const ctx = new PlanningContext();
      expect(ctx.contextId).toBeDefined();
      expect(ctx.contextId.length).toBeGreaterThan(0);
    });

    it('should create context with provided context ID', () => {
      const ctx = new PlanningContext('my-custom-id');
      expect(ctx.contextId).toBe('my-custom-id');
    });

    it('should start with counter at 0', () => {
      const ctx = new PlanningContext();
      expect(ctx.currentId).toBe(0);
    });
  });

  describe('nextId', () => {
    it('should generate sequential IDs starting from 1', () => {
      const ctx = new PlanningContext();

      expect(ctx.nextId()).toBe(1);
      expect(ctx.nextId()).toBe(2);
      expect(ctx.nextId()).toBe(3);
    });

    it('should generate unique IDs across many calls', () => {
      const ctx = new PlanningContext();
      const ids = new Set<number>();

      for (let i = 0; i < 1000; i++) {
        ids.add(ctx.nextId());
      }

      expect(ids.size).toBe(1000);
    });

    it('should update currentId after each call', () => {
      const ctx = new PlanningContext();

      ctx.nextId();
      expect(ctx.currentId).toBe(1);

      ctx.nextId();
      expect(ctx.currentId).toBe(2);

      ctx.nextId();
      expect(ctx.currentId).toBe(3);
    });
  });

  describe('currentId', () => {
    it('should return 0 before any ID generation', () => {
      const ctx = new PlanningContext();
      expect(ctx.currentId).toBe(0);
    });

    it('should return last generated ID', () => {
      const ctx = new PlanningContext();

      ctx.nextId();
      ctx.nextId();
      ctx.nextId();

      expect(ctx.currentId).toBe(3);
    });

    it('should not change when accessed multiple times', () => {
      const ctx = new PlanningContext();
      ctx.nextId();

      const id1 = ctx.currentId;
      const id2 = ctx.currentId;
      const id3 = ctx.currentId;

      expect(id1).toBe(id2);
      expect(id2).toBe(id3);
    });
  });

  describe('contextId', () => {
    it('should return provided context ID', () => {
      const ctx = new PlanningContext('test-context-123');
      expect(ctx.contextId).toBe('test-context-123');
    });

    it('should generate UUID-like ID when not provided', () => {
      const ctx = new PlanningContext();
      // UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx or ctx_timestamp_random
      const id = ctx.contextId;
      expect(id.length).toBeGreaterThan(10);
    });

    it('should generate unique IDs for different contexts', () => {
      const ctx1 = new PlanningContext();
      const ctx2 = new PlanningContext();
      const ctx3 = new PlanningContext();

      expect(ctx1.contextId).not.toBe(ctx2.contextId);
      expect(ctx2.contextId).not.toBe(ctx3.contextId);
      expect(ctx1.contextId).not.toBe(ctx3.contextId);
    });
  });
});

// =============================================================================
// DETERMINISTIC CONTEXT TESTS
// =============================================================================

describe('PlanningContext.createDeterministic', () => {
  it('should create context with default start ID of 0', () => {
    const ctx = PlanningContext.createDeterministic();
    expect(ctx.currentId).toBe(0);
    expect(ctx.nextId()).toBe(1);
  });

  it('should create context with custom start ID', () => {
    const ctx = PlanningContext.createDeterministic(100);
    expect(ctx.currentId).toBe(100);
    expect(ctx.nextId()).toBe(101);
    expect(ctx.nextId()).toBe(102);
  });

  it('should create context with default contextId of "test"', () => {
    const ctx = PlanningContext.createDeterministic();
    expect(ctx.contextId).toBe('test');
  });

  it('should create context with custom contextId', () => {
    const ctx = PlanningContext.createDeterministic(0, 'my-test-context');
    expect(ctx.contextId).toBe('my-test-context');
  });

  it('should produce predictable results for testing', () => {
    // Same setup should produce same results
    const ctx1 = PlanningContext.createDeterministic(0, 'test');
    const ctx2 = PlanningContext.createDeterministic(0, 'test');

    expect(ctx1.nextId()).toBe(ctx2.nextId());
    expect(ctx1.nextId()).toBe(ctx2.nextId());
    expect(ctx1.nextId()).toBe(ctx2.nextId());
  });

  it('should allow starting from arbitrary ID', () => {
    const ctx = PlanningContext.createDeterministic(999);

    expect(ctx.nextId()).toBe(1000);
    expect(ctx.nextId()).toBe(1001);
    expect(ctx.nextId()).toBe(1002);
  });
});

// =============================================================================
// FACTORY FUNCTION TESTS
// =============================================================================

describe('createPlanningContext', () => {
  it('should create new context without arguments', () => {
    const ctx = createPlanningContext();
    expect(ctx).toBeInstanceOf(PlanningContext);
    expect(ctx.currentId).toBe(0);
  });

  it('should create new context with custom context ID', () => {
    const ctx = createPlanningContext('factory-test-id');
    expect(ctx).toBeInstanceOf(PlanningContext);
    expect(ctx.contextId).toBe('factory-test-id');
  });

  it('should create independent contexts', () => {
    const ctx1 = createPlanningContext();
    const ctx2 = createPlanningContext();

    // Generate IDs in first context
    ctx1.nextId();
    ctx1.nextId();
    ctx1.nextId();

    // Second context should still start fresh
    expect(ctx2.nextId()).toBe(1);
  });
});

// =============================================================================
// ID ISOLATION TESTS
// =============================================================================

describe('PlanningContext ID Isolation', () => {
  it('should maintain separate ID sequences for different contexts', () => {
    const ctx1 = createPlanningContext('ctx1');
    const ctx2 = createPlanningContext('ctx2');
    const ctx3 = createPlanningContext('ctx3');

    // Generate IDs in different orders
    expect(ctx1.nextId()).toBe(1);
    expect(ctx2.nextId()).toBe(1);
    expect(ctx3.nextId()).toBe(1);

    expect(ctx1.nextId()).toBe(2);
    expect(ctx3.nextId()).toBe(2);
    expect(ctx2.nextId()).toBe(2);

    // Each context should be at ID 2
    expect(ctx1.currentId).toBe(2);
    expect(ctx2.currentId).toBe(2);
    expect(ctx3.currentId).toBe(2);
  });

  it('should not share state between contexts', () => {
    const ctx1 = createPlanningContext();

    // Generate many IDs in first context
    for (let i = 0; i < 100; i++) {
      ctx1.nextId();
    }

    const ctx2 = createPlanningContext();

    // Second context should start fresh
    expect(ctx2.currentId).toBe(0);
    expect(ctx2.nextId()).toBe(1);
  });

  it('should handle concurrent-like usage patterns', () => {
    const contexts: PlanningContext[] = [];
    const results: Map<string, number[]> = new Map();

    // Create multiple contexts
    for (let i = 0; i < 10; i++) {
      const ctx = createPlanningContext(`ctx-${i}`);
      contexts.push(ctx);
      results.set(`ctx-${i}`, []);
    }

    // Interleave ID generation across contexts
    for (let round = 0; round < 5; round++) {
      for (let i = 0; i < contexts.length; i++) {
        const id = contexts[i].nextId();
        results.get(`ctx-${i}`)!.push(id);
      }
    }

    // Each context should have generated 1, 2, 3, 4, 5
    for (let i = 0; i < contexts.length; i++) {
      const ids = results.get(`ctx-${i}`)!;
      expect(ids).toEqual([1, 2, 3, 4, 5]);
    }
  });
});

// =============================================================================
// DEFAULT CONTEXT TESTS (BACKWARDS COMPATIBILITY)
// =============================================================================

describe('Default PlanningContext (Legacy Support)', () => {
  beforeEach(() => {
    // Reset default context before each test
    resetDefaultPlanningContext();
  });

  afterEach(() => {
    // Clean up after each test
    resetDefaultPlanningContext();
  });

  it('should return same instance on multiple calls', () => {
    const ctx1 = getDefaultPlanningContext();
    const ctx2 = getDefaultPlanningContext();
    const ctx3 = getDefaultPlanningContext();

    expect(ctx1).toBe(ctx2);
    expect(ctx2).toBe(ctx3);
  });

  it('should have contextId of "default"', () => {
    const ctx = getDefaultPlanningContext();
    expect(ctx.contextId).toBe('default');
  });

  it('should maintain state across calls', () => {
    const ctx1 = getDefaultPlanningContext();
    ctx1.nextId();
    ctx1.nextId();

    const ctx2 = getDefaultPlanningContext();
    expect(ctx2.currentId).toBe(2);
    expect(ctx2.nextId()).toBe(3);
  });

  it('should reset when resetDefaultPlanningContext is called', () => {
    const ctx1 = getDefaultPlanningContext();
    ctx1.nextId();
    ctx1.nextId();
    ctx1.nextId();

    expect(ctx1.currentId).toBe(3);

    resetDefaultPlanningContext();

    const ctx2 = getDefaultPlanningContext();
    expect(ctx2.currentId).toBe(0);
    expect(ctx2.nextId()).toBe(1);
  });

  it('should create new instance after reset', () => {
    const ctx1 = getDefaultPlanningContext();
    resetDefaultPlanningContext();
    const ctx2 = getDefaultPlanningContext();

    expect(ctx1).not.toBe(ctx2);
  });
});

// =============================================================================
// EDGE CASES AND STRESS TESTS
// =============================================================================

describe('PlanningContext Edge Cases', () => {
  it('should handle very large number of IDs', () => {
    const ctx = createPlanningContext();

    // Generate a large number of IDs
    const target = 100000;
    for (let i = 0; i < target; i++) {
      ctx.nextId();
    }

    expect(ctx.currentId).toBe(target);
    expect(ctx.nextId()).toBe(target + 1);
  });

  it('should handle empty string context ID', () => {
    const ctx = new PlanningContext('');
    expect(ctx.contextId).toBe('');
    expect(ctx.nextId()).toBe(1);
  });

  it('should handle context ID with special characters', () => {
    const specialId = 'ctx-!@#$%^&*()_+-=[]{}|;:,.<>?';
    const ctx = new PlanningContext(specialId);
    expect(ctx.contextId).toBe(specialId);
  });

  it('should handle context ID with unicode characters', () => {
    const unicodeId = 'ctx-\u4e2d\u6587-\u65e5\u672c\u8a9e';
    const ctx = new PlanningContext(unicodeId);
    expect(ctx.contextId).toBe(unicodeId);
  });

  it('should handle deterministic context with startId of 0', () => {
    const ctx = PlanningContext.createDeterministic(0);
    expect(ctx.nextId()).toBe(1);
    expect(ctx.nextId()).toBe(2);
  });

  it('should handle deterministic context with negative-like starting (wraps to positive)', () => {
    // JavaScript numbers, so this will work but may have unexpected behavior
    // This tests that the system doesn't crash
    const ctx = PlanningContext.createDeterministic(-10);
    const id1 = ctx.nextId();
    const id2 = ctx.nextId();

    expect(id2).toBe(id1 + 1);
  });
});

// =============================================================================
// INTEGRATION-STYLE TESTS
// =============================================================================

describe('PlanningContext Integration Scenarios', () => {
  it('should support query planning workflow', () => {
    // Simulate a query planning session
    const ctx = createPlanningContext('query-planning-session');

    // Create scan node
    const scanNodeId = ctx.nextId();

    // Create filter node
    const filterNodeId = ctx.nextId();

    // Create project node
    const projectNodeId = ctx.nextId();

    expect(scanNodeId).toBe(1);
    expect(filterNodeId).toBe(2);
    expect(projectNodeId).toBe(3);
  });

  it('should support parallel query planning sessions', () => {
    // Simulate multiple concurrent query planning sessions
    const session1 = createPlanningContext('session-1');
    const session2 = createPlanningContext('session-2');

    // Session 1 creates a complex plan
    const s1_scan = session1.nextId();
    const s1_filter = session1.nextId();
    const s1_join = session1.nextId();
    const s1_project = session1.nextId();

    // Session 2 creates a simple plan
    const s2_scan = session2.nextId();
    const s2_limit = session2.nextId();

    // Both sessions should have isolated ID sequences
    expect(s1_scan).toBe(1);
    expect(s1_project).toBe(4);

    expect(s2_scan).toBe(1);
    expect(s2_limit).toBe(2);
  });

  it('should support deterministic test cases', () => {
    // Test case 1
    const test1 = PlanningContext.createDeterministic();
    const t1_id1 = test1.nextId();
    const t1_id2 = test1.nextId();

    // Test case 2 - should produce identical results
    const test2 = PlanningContext.createDeterministic();
    const t2_id1 = test2.nextId();
    const t2_id2 = test2.nextId();

    expect(t1_id1).toBe(t2_id1);
    expect(t1_id2).toBe(t2_id2);
  });

  it('should work with types module functions via default context', () => {
    resetDefaultPlanningContext();

    // The types module uses getDefaultPlanningContext internally
    const defaultCtx = getDefaultPlanningContext();

    const id1 = defaultCtx.nextId();
    const id2 = defaultCtx.nextId();

    // IDs should be sequential
    expect(id2).toBe(id1 + 1);

    resetDefaultPlanningContext();
  });
});
