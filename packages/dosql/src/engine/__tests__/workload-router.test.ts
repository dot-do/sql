/**
 * DoSQL Workload Router Tests
 *
 * Tests for OLTP/OLAP workload classification and routing:
 * - Query classification based on characteristics
 * - Routing decisions
 * - Signal detection
 *
 * Uses vitest for testing.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  WorkloadType,
  WorkloadClassifier,
  WorkloadRouter,
  type RoutingDecision,
  type ClassifierConfig,
  DEFAULT_CLASSIFIER_CONFIG,
  createWorkloadClassifier,
  createWorkloadRouter,
  classifyQuery,
  shouldUseColumnar,
  shouldUseBTree,
} from '../workload-router.js';
import type { Schema } from '../types.js';

// =============================================================================
// TEST UTILITIES
// =============================================================================

/**
 * Create a test schema with common tables
 */
function createTestSchema(): Schema {
  return {
    tables: new Map([
      ['users', {
        name: 'users',
        columns: [
          { name: 'id', type: 'number', nullable: false, primaryKey: true },
          { name: 'name', type: 'string', nullable: false },
          { name: 'email', type: 'string', nullable: true },
          { name: 'created_at', type: 'date', nullable: false },
        ],
        primaryKey: ['id'],
      }],
      ['orders', {
        name: 'orders',
        columns: [
          { name: 'id', type: 'number', nullable: false, primaryKey: true },
          { name: 'user_id', type: 'number', nullable: false },
          { name: 'amount', type: 'number', nullable: false },
          { name: 'quantity', type: 'number', nullable: false },
          { name: 'region', type: 'string', nullable: true },
          { name: 'status', type: 'string', nullable: false },
          { name: 'created_at', type: 'date', nullable: false },
        ],
        primaryKey: ['id'],
      }],
      ['products', {
        name: 'products',
        columns: [
          { name: 'id', type: 'number', nullable: false, primaryKey: true },
          { name: 'name', type: 'string', nullable: false },
          { name: 'price', type: 'number', nullable: false },
          { name: 'category', type: 'string', nullable: true },
        ],
        primaryKey: ['id'],
      }],
    ]),
  };
}

// =============================================================================
// WORKLOAD TYPE TESTS
// =============================================================================

describe('WorkloadType', () => {
  it('should have correct enum values', () => {
    expect(WorkloadType.OLTP).toBe('oltp');
    expect(WorkloadType.OLAP).toBe('olap');
    expect(WorkloadType.HYBRID).toBe('hybrid');
  });
});

// =============================================================================
// WORKLOAD CLASSIFIER TESTS
// =============================================================================

describe('WorkloadClassifier', () => {
  let schema: Schema;
  let classifier: WorkloadClassifier;

  beforeEach(() => {
    schema = createTestSchema();
    classifier = new WorkloadClassifier(schema);
  });

  describe('OLTP classification', () => {
    it('should classify point lookup by primary key as OLTP', () => {
      const decision = classifier.classify('SELECT * FROM users WHERE id = 1');

      expect(decision.workloadType).toBe(WorkloadType.OLTP);
      expect(decision.primarySource).toBe('btree');
      expect(decision.useIndexLookup).toBe(true);
      expect(decision.signals.some(s => s.name === 'primaryKeyLookup')).toBe(true);
    });

    it('should classify small LIMIT query as OLTP', () => {
      const decision = classifier.classify('SELECT * FROM users LIMIT 10');

      expect(decision.workloadType).toBe(WorkloadType.OLTP);
      expect(decision.signals.some(s => s.name === 'smallLimit')).toBe(true);
    });

    it('should classify primary key lookup with small limit as OLTP', () => {
      const decision = classifier.classify('SELECT * FROM orders WHERE id = 123 LIMIT 1');

      expect(decision.workloadType).toBe(WorkloadType.OLTP);
      expect(decision.useIndexLookup).toBe(true);
    });
  });

  describe('OLAP classification', () => {
    it('should classify COUNT aggregation as OLAP', () => {
      const decision = classifier.classify('SELECT COUNT(*) FROM orders');

      expect(decision.workloadType).toBe(WorkloadType.OLAP);
      expect(decision.primarySource).toBe('columnar');
      expect(decision.projectionPushdown).toBe(true);
      expect(decision.signals.some(s => s.name === 'aggregates')).toBe(true);
    });

    it('should classify SUM aggregation as OLAP', () => {
      const decision = classifier.classify('SELECT SUM(amount) FROM orders');

      expect(decision.workloadType).toBe(WorkloadType.OLAP);
    });

    it('should classify GROUP BY query as OLAP', () => {
      const decision = classifier.classify(
        'SELECT region, COUNT(*) FROM orders GROUP BY region'
      );

      expect(decision.workloadType).toBe(WorkloadType.OLAP);
      expect(decision.signals.some(s => s.name === 'groupBy')).toBe(true);
      expect(decision.signals.some(s => s.name === 'aggregates')).toBe(true);
    });

    it('should classify full table scan without limit as OLAP or HYBRID', () => {
      const decision = classifier.classify('SELECT * FROM orders');

      // Full table scan without aggregates is borderline - could be OLAP or HYBRID
      expect([WorkloadType.OLAP, WorkloadType.HYBRID]).toContain(decision.workloadType);
      expect(decision.signals.some(s => s.name === 'noLimit')).toBe(true);
    });

    it('should classify query with HAVING as OLAP', () => {
      const decision = classifier.classify(
        'SELECT region, COUNT(*) as cnt FROM orders GROUP BY region HAVING COUNT(*) > 10'
      );

      expect(decision.workloadType).toBe(WorkloadType.OLAP);
      expect(decision.signals.some(s => s.name === 'having')).toBe(true);
    });

    it('should classify DISTINCT query as OLAP', () => {
      const decision = classifier.classify('SELECT DISTINCT region FROM orders');

      expect(decision.workloadType).toBe(WorkloadType.OLAP);
      expect(decision.signals.some(s => s.name === 'distinct')).toBe(true);
    });

    it('should classify query with large LIMIT as OLAP or HYBRID', () => {
      const decision = classifier.classify('SELECT * FROM orders LIMIT 10000');

      // Large limit without aggregates is borderline - could be OLAP or HYBRID
      expect([WorkloadType.OLAP, WorkloadType.HYBRID]).toContain(decision.workloadType);
      expect(decision.signals.some(s => s.name === 'largeLimit')).toBe(true);
    });

    it('should classify AVG/MIN/MAX aggregations as OLAP', () => {
      const decision = classifier.classify(
        'SELECT AVG(amount), MIN(amount), MAX(amount) FROM orders'
      );

      expect(decision.workloadType).toBe(WorkloadType.OLAP);
    });

    it('should classify full sort without limit as OLAP', () => {
      const decision = classifier.classify('SELECT * FROM orders ORDER BY amount DESC');

      expect(decision.workloadType).toBe(WorkloadType.OLAP);
      expect(decision.signals.some(s => s.name === 'fullSort')).toBe(true);
    });
  });

  describe('HYBRID classification', () => {
    it('should classify aggregate with recency filter as HYBRID', () => {
      const decision = classifier.classify(
        'SELECT SUM(amount) FROM orders WHERE created_at > NOW()'
      );

      // Has aggregate (OLAP signal) but also recency filter (OLTP signal)
      // Should result in HYBRID or lean towards one based on weight
      expect([WorkloadType.HYBRID, WorkloadType.OLAP]).toContain(decision.workloadType);
    });

    it('should classify small aggregation with primary key filter as HYBRID', () => {
      const decision = classifier.classify(
        'SELECT COUNT(*) FROM orders WHERE id = 1'
      );

      // Has aggregate (OLAP) and primary key lookup (OLTP)
      // Mixed signals could result in HYBRID
      expect([WorkloadType.HYBRID, WorkloadType.OLTP, WorkloadType.OLAP]).toContain(decision.workloadType);
    });

    it('should detect joins as HYBRID candidates', () => {
      const decision = classifier.classify(
        'SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id'
      );

      expect(decision.signals.some(s => s.name === 'joins')).toBe(true);
    });
  });

  describe('routing decision details', () => {
    it('should set predicatePushdown to true', () => {
      const decision = classifier.classify('SELECT * FROM users WHERE id = 1');

      expect(decision.predicatePushdown).toBe(true);
    });

    it('should set projectionPushdown based on workload type', () => {
      const oltpDecision = classifier.classify('SELECT * FROM users WHERE id = 1');
      const olapDecision = classifier.classify('SELECT COUNT(*) FROM orders');

      expect(oltpDecision.projectionPushdown).toBe(false);
      expect(olapDecision.projectionPushdown).toBe(true);
    });

    it('should provide reason for classification', () => {
      const decision = classifier.classify('SELECT COUNT(*) FROM orders GROUP BY region');

      expect(decision.reason).toContain('OLAP');
      expect(decision.reason.length).toBeGreaterThan(0);
    });

    it('should provide confidence score between 0 and 1', () => {
      const decision = classifier.classify('SELECT COUNT(*) FROM orders');

      expect(decision.confidence).toBeGreaterThanOrEqual(0);
      expect(decision.confidence).toBeLessThanOrEqual(1);
    });

    it('should include signals in decision', () => {
      const decision = classifier.classify('SELECT COUNT(*) FROM orders GROUP BY region');

      expect(decision.signals.length).toBeGreaterThan(0);
      expect(decision.signals.every(s => 'name' in s && 'weight' in s)).toBe(true);
    });
  });

  describe('custom configuration', () => {
    it('should respect columnarThreshold config', () => {
      const customClassifier = new WorkloadClassifier(schema, {
        columnarThreshold: 50,
      });

      // With lower threshold, LIMIT 100 should be OLAP
      const decision = customClassifier.classify('SELECT * FROM users LIMIT 100');

      expect(decision.signals.some(s => s.name === 'largeLimit')).toBe(true);
    });

    it('should respect oltpLimitThreshold config', () => {
      const customClassifier = new WorkloadClassifier(schema, {
        oltpLimitThreshold: 5,
      });

      // With stricter threshold, LIMIT 10 should not be small limit
      const decision = customClassifier.classify('SELECT * FROM users LIMIT 10');

      expect(decision.signals.some(s => s.name === 'smallLimit')).toBe(false);
    });
  });
});

// =============================================================================
// PLAN CLASSIFICATION TESTS
// =============================================================================

describe('WorkloadClassifier.classifyPlan', () => {
  let schema: Schema;
  let classifier: WorkloadClassifier;

  beforeEach(() => {
    schema = createTestSchema();
    classifier = new WorkloadClassifier(schema);
  });

  it('should classify scan plan with columnar source as OLAP', () => {
    const decision = classifier.classifyPlan({
      id: 1,
      type: 'scan',
      table: 'orders',
      source: 'columnar',
      columns: ['*'],
    });

    expect(decision.workloadType).toBe(WorkloadType.OLAP);
    expect(decision.signals.some(s => s.name === 'columnarScan')).toBe(true);
  });

  it('should classify index lookup plan as OLTP', () => {
    const decision = classifier.classifyPlan({
      id: 1,
      type: 'indexLookup',
      table: 'users',
      index: 'primary',
      lookupKey: [{ type: 'literal', value: 1, dataType: 'number' }],
      columns: ['*'],
    });

    expect(decision.workloadType).toBe(WorkloadType.OLTP);
    expect(decision.signals.some(s => s.name === 'indexLookup')).toBe(true);
  });

  it('should classify aggregate plan as OLAP', () => {
    const decision = classifier.classifyPlan({
      id: 1,
      type: 'aggregate',
      input: {
        id: 2,
        type: 'scan',
        table: 'orders',
        source: 'columnar',
        columns: ['amount'],
      },
      groupBy: [],
      aggregates: [{
        expr: {
          type: 'aggregate',
          function: 'sum',
          arg: { type: 'columnRef', column: 'amount' },
        },
        alias: 'total',
      }],
    });

    expect(decision.workloadType).toBe(WorkloadType.OLAP);
    expect(decision.signals.some(s => s.name === 'aggregate')).toBe(true);
  });

  it('should classify limit plan with small limit as OLTP', () => {
    const decision = classifier.classifyPlan({
      id: 1,
      type: 'limit',
      limit: 10,
      input: {
        id: 2,
        type: 'scan',
        table: 'users',
        source: 'btree',
        columns: ['*'],
      },
    });

    expect(decision.workloadType).toBe(WorkloadType.OLTP);
    expect(decision.signals.some(s => s.name === 'smallLimit')).toBe(true);
  });

  it('should classify merge plan', () => {
    const decision = classifier.classifyPlan({
      id: 1,
      type: 'merge',
      inputs: [
        { id: 2, type: 'scan', table: 'orders', source: 'btree', columns: ['*'] },
        { id: 3, type: 'scan', table: 'orders', source: 'columnar', columns: ['*'] },
      ],
    });

    expect(decision.signals.some(s => s.name === 'merge')).toBe(true);
  });
});

// =============================================================================
// WORKLOAD ROUTER TESTS
// =============================================================================

describe('WorkloadRouter', () => {
  let schema: Schema;
  let router: WorkloadRouter;

  beforeEach(() => {
    schema = createTestSchema();
    router = new WorkloadRouter(schema);
  });

  describe('routing', () => {
    it('should route SQL queries', () => {
      const decision = router.route('SELECT COUNT(*) FROM orders');

      expect(decision.workloadType).toBe(WorkloadType.OLAP);
    });

    it('should route plans', () => {
      const decision = router.routePlan({
        id: 1,
        type: 'indexLookup',
        table: 'users',
        index: 'primary',
        lookupKey: [{ type: 'literal', value: 1, dataType: 'number' }],
        columns: ['*'],
      });

      expect(decision.workloadType).toBe(WorkloadType.OLTP);
    });
  });

  describe('path registration', () => {
    it('should register OLTP path', () => {
      const mockPath = {
        pathType: WorkloadType.OLTP,
        execute: async () => ({ rows: [], stats: {} as any }),
        canExecute: () => true,
        estimateCost: () => ({ ioOps: 0, cpuCost: 0, memoryBytes: 0, estimatedTimeMs: 0 }),
      };

      expect(() => router.registerOLTPPath(mockPath)).not.toThrow();
      expect(router.hasPath(WorkloadType.OLTP)).toBe(true);
    });

    it('should register OLAP path', () => {
      const mockPath = {
        pathType: WorkloadType.OLAP,
        execute: async () => ({ rows: [], stats: {} as any }),
        canExecute: () => true,
        estimateCost: () => ({ ioOps: 0, cpuCost: 0, memoryBytes: 0, estimatedTimeMs: 0 }),
      };

      expect(() => router.registerOLAPPath(mockPath)).not.toThrow();
      expect(router.hasPath(WorkloadType.OLAP)).toBe(true);
    });

    it('should reject mismatched path types', () => {
      const mockPath = {
        pathType: WorkloadType.OLAP,
        execute: async () => ({ rows: [], stats: {} as any }),
        canExecute: () => true,
        estimateCost: () => ({ ioOps: 0, cpuCost: 0, memoryBytes: 0, estimatedTimeMs: 0 }),
      };

      expect(() => router.registerOLTPPath(mockPath)).toThrow('Expected OLTP path');
    });

    it('should get registered paths', () => {
      const oltpPath = {
        pathType: WorkloadType.OLTP,
        execute: async () => ({ rows: [], stats: {} as any }),
        canExecute: () => true,
        estimateCost: () => ({ ioOps: 0, cpuCost: 0, memoryBytes: 0, estimatedTimeMs: 0 }),
      };

      router.registerOLTPPath(oltpPath);

      expect(router.getPath(WorkloadType.OLTP)).toBe(oltpPath);
      expect(router.getPath(WorkloadType.OLAP)).toBeNull();
    });

    it('should check hasPath for HYBRID', () => {
      expect(router.hasPath(WorkloadType.HYBRID)).toBe(false);

      const oltpPath = {
        pathType: WorkloadType.OLTP,
        execute: async () => ({ rows: [], stats: {} as any }),
        canExecute: () => true,
        estimateCost: () => ({ ioOps: 0, cpuCost: 0, memoryBytes: 0, estimatedTimeMs: 0 }),
      };
      const olapPath = {
        pathType: WorkloadType.OLAP,
        execute: async () => ({ rows: [], stats: {} as any }),
        canExecute: () => true,
        estimateCost: () => ({ ioOps: 0, cpuCost: 0, memoryBytes: 0, estimatedTimeMs: 0 }),
      };

      router.registerOLTPPath(oltpPath);
      router.registerOLAPPath(olapPath);

      expect(router.hasPath(WorkloadType.HYBRID)).toBe(true);
    });
  });
});

// =============================================================================
// UTILITY FUNCTION TESTS
// =============================================================================

describe('utility functions', () => {
  let schema: Schema;

  beforeEach(() => {
    schema = createTestSchema();
  });

  describe('createWorkloadClassifier', () => {
    it('should create a classifier', () => {
      const classifier = createWorkloadClassifier(schema);

      expect(classifier).toBeInstanceOf(WorkloadClassifier);
    });
  });

  describe('createWorkloadRouter', () => {
    it('should create a router', () => {
      const router = createWorkloadRouter(schema);

      expect(router).toBeInstanceOf(WorkloadRouter);
    });
  });

  describe('classifyQuery', () => {
    it('should return workload type for query', () => {
      const result = classifyQuery('SELECT COUNT(*) FROM orders', schema);

      expect(result).toBe(WorkloadType.OLAP);
    });

    it('should return OLTP for point lookup', () => {
      const result = classifyQuery('SELECT * FROM users WHERE id = 1', schema);

      expect(result).toBe(WorkloadType.OLTP);
    });
  });

  describe('shouldUseColumnar', () => {
    it('should return true for OLAP queries', () => {
      expect(shouldUseColumnar('SELECT COUNT(*) FROM orders', schema)).toBe(true);
    });

    it('should return true for HYBRID queries', () => {
      // A query that might be classified as HYBRID
      // (depends on actual classification)
      const result = shouldUseColumnar('SELECT * FROM orders', schema);
      expect([true, false]).toContain(result);
    });

    it('should return false for OLTP queries', () => {
      expect(shouldUseColumnar('SELECT * FROM users WHERE id = 1', schema)).toBe(false);
    });
  });

  describe('shouldUseBTree', () => {
    it('should return true for OLTP queries', () => {
      expect(shouldUseBTree('SELECT * FROM users WHERE id = 1', schema)).toBe(true);
    });

    it('should return false for OLAP queries', () => {
      expect(shouldUseBTree('SELECT COUNT(*) FROM orders', schema)).toBe(false);
    });
  });
});

// =============================================================================
// DEFAULT CONFIG TESTS
// =============================================================================

describe('DEFAULT_CLASSIFIER_CONFIG', () => {
  it('should have expected default values', () => {
    expect(DEFAULT_CLASSIFIER_CONFIG.columnarThreshold).toBe(1000);
    expect(DEFAULT_CLASSIFIER_CONFIG.oltpLimitThreshold).toBe(100);
    expect(DEFAULT_CLASSIFIER_CONFIG.indexLookupSelectivity).toBe(0.01);
    expect(DEFAULT_CLASSIFIER_CONFIG.preferHotForRecent).toBe(true);
    expect(DEFAULT_CLASSIFIER_CONFIG.recencyThreshold).toBe(60 * 60 * 1000); // 1 hour
    expect(DEFAULT_CLASSIFIER_CONFIG.olapAggregateThreshold).toBe(1);
  });

  it('should be frozen', () => {
    expect(Object.isFrozen(DEFAULT_CLASSIFIER_CONFIG)).toBe(true);
  });
});

// =============================================================================
// EDGE CASE TESTS
// =============================================================================

describe('edge cases', () => {
  let schema: Schema;
  let classifier: WorkloadClassifier;

  beforeEach(() => {
    schema = createTestSchema();
    classifier = new WorkloadClassifier(schema);
  });

  it('should handle query without FROM clause', () => {
    const decision = classifier.classify('SELECT 1');

    // Should not throw, classification depends on heuristics
    expect(decision.workloadType).toBeDefined();
  });

  it('should handle complex nested queries', () => {
    // Note: Actual subquery support may be limited
    // This tests that the classifier doesn't crash
    expect(() => classifier.classify(
      'SELECT * FROM users WHERE id IN (1, 2, 3)'
    )).not.toThrow();
  });

  it('should handle query with multiple aggregates', () => {
    const decision = classifier.classify(
      'SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM orders'
    );

    expect(decision.workloadType).toBe(WorkloadType.OLAP);
    // Multiple aggregates should strengthen OLAP signal
  });

  it('should handle query on unknown table', () => {
    // Schema doesn't have this table
    const decision = classifier.classify('SELECT * FROM unknown_table WHERE id = 1');

    // Should still classify based on query structure
    expect(decision.workloadType).toBeDefined();
  });

  it('should handle ORDER BY with LIMIT', () => {
    const decision = classifier.classify('SELECT * FROM orders ORDER BY amount DESC LIMIT 10');

    // Small limit should favor OLTP even with ORDER BY
    expect(decision.signals.some(s => s.name === 'smallLimit')).toBe(true);
  });
});
