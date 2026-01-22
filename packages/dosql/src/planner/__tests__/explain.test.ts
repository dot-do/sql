/**
 * DoSQL Planner - EXPLAIN Output Tests
 *
 * TDD tests for:
 * - formatJsonNode returns properly typed output (JsonExplainNode)
 * - All ExplainNode types are handled
 * - Nested nodes are properly formatted
 * - formatValue handles all SqlValue types
 *
 * Issue: pocs-akoc - Replace `any` in planner/explain.ts with JsonExplainNode interface
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  explain,
  explainAnalyze,
  ExplainGenerator,
  createExplainGenerator,
  summarizePlan,
  getTotalCost,
  getEstimatedRows,
  type ExplainNode,
  type ExplainOptions,
  type ExecutionStats,
  type JsonExplainNode,
} from '../explain.js';
import {
  createScanNode,
  createJoinNode,
  createFilterNode,
  createSortNode,
  createLimitNode,
  createAggregateNode,
  resetPlanNodeIds,
  type PhysicalPlanNode,
  type ScanNode,
  type JoinNode,
  type PlanCost,
} from '../types.js';
import type { Predicate, Expression } from '../../engine/types.js';

// =============================================================================
// TEST HELPERS
// =============================================================================

/**
 * Create a mock PlanCost for testing
 */
function mockCost(overrides?: Partial<PlanCost>): PlanCost {
  return {
    ioOps: 10,
    estimatedRows: 100,
    cpuCost: 0.5,
    memoryBytes: 1024,
    startupCost: 0.5,
    totalCost: 10.5,
    usedStatistics: true,
    confidence: 0.9,
    ...overrides,
  };
}

/**
 * Create a simple comparison predicate
 */
function comparisonPredicate(
  column: string,
  op: 'eq' | 'gt' | 'lt' | 'ge' | 'le' | 'ne',
  value: string | number | boolean | null
): Predicate {
  return {
    type: 'comparison',
    op,
    left: { type: 'columnRef', column },
    right: { type: 'literal', value, dataType: typeof value as 'string' | 'number' | 'boolean' | 'null' },
  };
}

/**
 * Create a column reference expression
 */
function colRef(column: string, table?: string): Expression {
  return { type: 'columnRef', column, table };
}

// =============================================================================
// JSON FORMAT OUTPUT TESTS
// =============================================================================

describe('explain.ts JSON format', () => {
  beforeEach(() => {
    resetPlanNodeIds();
  });

  describe('formatJsonNode return type', () => {
    it('should return a properly typed JsonExplainNode for a scan node', () => {
      const scan = createScanNode('users', ['id', 'name', 'email'], {
        accessMethod: 'seqScan',
        cost: mockCost(),
      });

      const json = explain(scan, { format: 'json' });
      const parsed = JSON.parse(json);

      // Verify structure matches JsonExplainNode
      expect(parsed).toBeInstanceOf(Array);
      expect(parsed.length).toBe(1);

      const plan = parsed[0].Plan as JsonExplainNode;
      expect(plan).toHaveProperty('Node Type');
      expect(plan['Node Type']).toBe('Seq Scan');
      expect(plan).toHaveProperty('Relation Name');
      expect(plan['Relation Name']).toBe('users');
    });

    it('should include cost information when costs option is true', () => {
      const scan = createScanNode('orders', ['id', 'amount'], {
        accessMethod: 'seqScan',
        cost: mockCost({ startupCost: 1.5, totalCost: 25.0 }),
      });

      const json = explain(scan, { format: 'json', costs: true });
      const parsed = JSON.parse(json);
      const plan = parsed[0].Plan as JsonExplainNode;

      expect(plan).toHaveProperty('Startup Cost');
      expect(plan['Startup Cost']).toBe(1.5);
      expect(plan).toHaveProperty('Total Cost');
      expect(plan['Total Cost']).toBe(25.0);
    });

    it('should include row estimates when rowEstimates option is true', () => {
      const scan = createScanNode('products', ['id', 'name'], {
        accessMethod: 'seqScan',
        cost: mockCost({ estimatedRows: 500 }),
      });

      const json = explain(scan, { format: 'json', rowEstimates: true });
      const parsed = JSON.parse(json);
      const plan = parsed[0].Plan as JsonExplainNode;

      expect(plan).toHaveProperty('Plan Rows');
      expect(plan['Plan Rows']).toBe(500);
    });
  });

  describe('nested nodes formatting', () => {
    it('should format child nodes in Plans array', () => {
      const scan = createScanNode('users', ['id', 'name'], {
        accessMethod: 'seqScan',
        cost: mockCost(),
      });

      const filter = createFilterNode(scan, comparisonPredicate('id', 'gt', 10), {
        cost: mockCost(),
      });

      const json = explain(filter, { format: 'json' });
      const parsed = JSON.parse(json);
      const plan = parsed[0].Plan as JsonExplainNode;

      expect(plan['Node Type']).toBe('Filter');
      expect(plan).toHaveProperty('Plans');
      expect(Array.isArray(plan.Plans)).toBe(true);
      expect(plan.Plans!.length).toBe(1);
      expect(plan.Plans![0]['Node Type']).toBe('Seq Scan');
    });

    it('should format deeply nested nodes correctly', () => {
      // Build: Limit -> Sort -> Filter -> Scan
      const scan = createScanNode('orders', ['id', 'amount', 'date'], {
        accessMethod: 'seqScan',
        cost: mockCost(),
      });

      const filter = createFilterNode(scan, comparisonPredicate('amount', 'gt', 100), {
        cost: mockCost(),
      });

      const sort = createSortNode(filter, [{ expr: colRef('date'), direction: 'desc' }], {
        cost: mockCost(),
      });

      const limit = createLimitNode(sort, 10, undefined, {
        cost: mockCost(),
      });

      const json = explain(limit, { format: 'json' });
      const parsed = JSON.parse(json);
      const plan = parsed[0].Plan as JsonExplainNode;

      // Verify the nested structure
      expect(plan['Node Type']).toBe('Limit');
      expect(plan.Plans![0]['Node Type']).toBe('Sort');
      expect(plan.Plans![0].Plans![0]['Node Type']).toBe('Filter');
      expect(plan.Plans![0].Plans![0].Plans![0]['Node Type']).toBe('Seq Scan');
    });

    it('should format join nodes with multiple children', () => {
      const leftScan = createScanNode('users', ['id', 'name'], {
        accessMethod: 'seqScan',
        cost: mockCost(),
      });

      const rightScan = createScanNode('orders', ['id', 'user_id', 'amount'], {
        accessMethod: 'seqScan',
        cost: mockCost(),
      });

      const join = createJoinNode(leftScan, rightScan, 'inner', 'hash', undefined, {
        cost: mockCost(),
      });

      const json = explain(join, { format: 'json' });
      const parsed = JSON.parse(json);
      const plan = parsed[0].Plan as JsonExplainNode;

      expect(plan['Node Type']).toBe('Hash Join');
      expect(plan.Plans).toHaveLength(2);
      expect(plan.Plans![0]['Node Type']).toBe('Seq Scan');
      expect(plan.Plans![0]['Relation Name']).toBe('users');
      expect(plan.Plans![1]['Node Type']).toBe('Seq Scan');
      expect(plan.Plans![1]['Relation Name']).toBe('orders');
    });
  });

  describe('all ExplainNode types', () => {
    it('should handle Index Scan nodes', () => {
      const scan = createScanNode('users', ['id', 'name'], {
        accessMethod: 'indexScan',
        indexName: 'idx_users_id',
        indexCondition: [{ column: 'id', operator: 'eq', value: 42, columnIndex: 0 }],
        cost: mockCost(),
      });

      const json = explain(scan, { format: 'json' });
      const parsed = JSON.parse(json);
      const plan = parsed[0].Plan as JsonExplainNode;

      expect(plan['Node Type']).toBe('Index Scan');
      expect(plan['Index Name']).toBe('idx_users_id');
      expect(plan['Index Cond']).toContain('id');
    });

    it('should handle Index Only Scan nodes', () => {
      const scan = createScanNode('users', ['id'], {
        accessMethod: 'indexOnlyScan',
        indexName: 'idx_users_pk',
        cost: mockCost(),
      });

      const json = explain(scan, { format: 'json' });
      const parsed = JSON.parse(json);
      const plan = parsed[0].Plan as JsonExplainNode;

      expect(plan['Node Type']).toBe('Index Only Scan');
      expect(plan['Index Name']).toBe('idx_users_pk');
    });

    it('should handle Hash Join nodes with hash condition', () => {
      const left = createScanNode('a', ['id'], { accessMethod: 'seqScan', cost: mockCost() });
      const right = createScanNode('b', ['id'], { accessMethod: 'seqScan', cost: mockCost() });

      const join = createJoinNode(left, right, 'inner', 'hash', comparisonPredicate('id', 'eq', 1), {
        cost: mockCost(),
      });

      const json = explain(join, { format: 'json' });
      const parsed = JSON.parse(json);
      const plan = parsed[0].Plan as JsonExplainNode;

      expect(plan['Node Type']).toBe('Hash Join');
      expect(plan).toHaveProperty('Hash Cond');
    });

    it('should handle Merge Join nodes with merge condition', () => {
      const left = createScanNode('a', ['id'], { accessMethod: 'seqScan', cost: mockCost() });
      const right = createScanNode('b', ['id'], { accessMethod: 'seqScan', cost: mockCost() });

      const join = createJoinNode(left, right, 'inner', 'merge', comparisonPredicate('id', 'eq', 1), {
        cost: mockCost(),
      });

      const json = explain(join, { format: 'json' });
      const parsed = JSON.parse(json);
      const plan = parsed[0].Plan as JsonExplainNode;

      expect(plan['Node Type']).toBe('Merge Join');
      expect(plan).toHaveProperty('Merge Cond');
    });

    it('should handle Nested Loop nodes with join condition', () => {
      const left = createScanNode('a', ['id'], { accessMethod: 'seqScan', cost: mockCost() });
      const right = createScanNode('b', ['id'], { accessMethod: 'seqScan', cost: mockCost() });

      const join = createJoinNode(left, right, 'inner', 'nestedLoop', comparisonPredicate('id', 'eq', 1), {
        cost: mockCost(),
      });

      const json = explain(join, { format: 'json' });
      const parsed = JSON.parse(json);
      const plan = parsed[0].Plan;

      expect(plan['Node Type']).toBe('Nested Loop');
    });

    it('should handle Aggregate nodes with group key', () => {
      const scan = createScanNode('orders', ['user_id', 'amount'], {
        accessMethod: 'seqScan',
        cost: mockCost(),
      });

      const agg = createAggregateNode(
        scan,
        [colRef('user_id')],
        [{ expr: { type: 'aggregate', function: 'sum', arg: colRef('amount') }, alias: 'total' }],
        {
          strategy: 'hashed',
          cost: mockCost(),
        }
      );

      const json = explain(agg, { format: 'json' });
      const parsed = JSON.parse(json);
      const plan = parsed[0].Plan as JsonExplainNode;

      expect(plan['Node Type']).toBe('HashAggregate');
      expect(plan).toHaveProperty('Group Key');
    });

    it('should handle Sort nodes with sort key', () => {
      const scan = createScanNode('users', ['id', 'name'], {
        accessMethod: 'seqScan',
        cost: mockCost(),
      });

      const sort = createSortNode(scan, [{ expr: colRef('name'), direction: 'asc' }], {
        cost: mockCost(),
      });

      const json = explain(sort, { format: 'json' });
      const parsed = JSON.parse(json);
      const plan = parsed[0].Plan as JsonExplainNode;

      expect(plan['Node Type']).toBe('Sort');
      expect(plan).toHaveProperty('Sort Key');
    });

    it('should handle Filter nodes with filter condition', () => {
      const scan = createScanNode('users', ['id', 'name'], {
        accessMethod: 'seqScan',
        cost: mockCost(),
      });

      const filter = createFilterNode(scan, comparisonPredicate('id', 'gt', 10), {
        cost: mockCost(),
      });

      const json = explain(filter, { format: 'json' });
      const parsed = JSON.parse(json);
      const plan = parsed[0].Plan as JsonExplainNode;

      expect(plan['Node Type']).toBe('Filter');
      expect(plan).toHaveProperty('Filter');
    });
  });
});

// =============================================================================
// EXPLAIN ANALYZE TESTS
// =============================================================================

describe('explainAnalyze JSON format', () => {
  beforeEach(() => {
    resetPlanNodeIds();
  });

  it('should include actual execution statistics', () => {
    const scan = createScanNode('users', ['id', 'name'], {
      accessMethod: 'seqScan',
      cost: mockCost(),
    });

    const stats: ExecutionStats = {
      actualRows: 150,
      startupTimeMs: 0.5,
      totalTimeMs: 5.2,
      loops: 1,
    };

    const json = explainAnalyze(scan, stats, { format: 'json' });
    const parsed = JSON.parse(json);
    const plan = parsed[0].Plan as JsonExplainNode;

    expect(plan).toHaveProperty('Actual Rows');
    expect(plan['Actual Rows']).toBe(150);
    expect(plan).toHaveProperty('Actual Startup Time');
    expect(plan['Actual Startup Time']).toBe(0.5);
    expect(plan).toHaveProperty('Actual Total Time');
    expect(plan['Actual Total Time']).toBe(5.2);
    expect(plan).toHaveProperty('Actual Loops');
    expect(plan['Actual Loops']).toBe(1);
  });

  it('should propagate execution stats to nested nodes', () => {
    const scan = createScanNode('users', ['id', 'name'], {
      accessMethod: 'seqScan',
      cost: mockCost(),
    });

    const filter = createFilterNode(scan, comparisonPredicate('id', 'gt', 10), {
      cost: mockCost(),
    });

    const stats: ExecutionStats = {
      actualRows: 50,
      startupTimeMs: 0.1,
      totalTimeMs: 2.0,
      loops: 1,
      children: [
        {
          actualRows: 100,
          startupTimeMs: 0.05,
          totalTimeMs: 1.5,
          loops: 1,
        },
      ],
    };

    const json = explainAnalyze(filter, stats, { format: 'json' });
    const parsed = JSON.parse(json);
    const plan = parsed[0].Plan as JsonExplainNode;

    expect(plan['Actual Rows']).toBe(50);
    expect(plan.Plans![0]['Actual Rows']).toBe(100);
  });
});

// =============================================================================
// OTHER FORMAT TESTS
// =============================================================================

describe('explain.ts other formats', () => {
  beforeEach(() => {
    resetPlanNodeIds();
  });

  it('should format as text (default)', () => {
    const scan = createScanNode('users', ['id', 'name'], {
      accessMethod: 'seqScan',
      cost: mockCost(),
    });

    const text = explain(scan, { format: 'text' });

    expect(text).toContain('Seq Scan');
    expect(text).toContain('users');
  });

  it('should format as YAML', () => {
    const scan = createScanNode('users', ['id', 'name'], {
      accessMethod: 'seqScan',
      cost: mockCost(),
    });

    const yaml = explain(scan, { format: 'yaml' });

    expect(yaml).toContain('Node Type:');
    expect(yaml).toContain('Seq Scan');
  });

  it('should format as tree', () => {
    const scan = createScanNode('users', ['id', 'name'], {
      accessMethod: 'seqScan',
      cost: mockCost(),
    });

    const tree = explain(scan, { format: 'tree' });

    expect(tree).toContain('Seq Scan');
    expect(tree).toContain('users');
  });
});

// =============================================================================
// EXPLAIN GENERATOR CLASS TESTS
// =============================================================================

describe('ExplainGenerator class', () => {
  beforeEach(() => {
    resetPlanNodeIds();
  });

  it('should create generator with default options', () => {
    const generator = createExplainGenerator();
    expect(generator).toBeInstanceOf(ExplainGenerator);
  });

  it('should create generator with custom options', () => {
    const generator = createExplainGenerator({ format: 'json', costs: false });
    const scan = createScanNode('users', ['id'], {
      accessMethod: 'seqScan',
      cost: mockCost(),
    });

    const json = generator.explain(scan);
    const parsed = JSON.parse(json);

    expect(parsed[0].Plan).not.toHaveProperty('Startup Cost');
  });

  it('should build explain nodes for custom formatting', () => {
    const generator = createExplainGenerator({ costs: true, rowEstimates: true });
    const scan = createScanNode('users', ['id', 'name'], {
      accessMethod: 'seqScan',
      cost: mockCost({ estimatedRows: 200 }),
    });

    const explainNode = generator.buildExplainNode(scan);

    expect(explainNode.nodeType).toBe('Seq Scan');
    expect(explainNode.relationName).toBe('users');
    expect(explainNode.rows).toBe(200);
  });
});

// =============================================================================
// UTILITY FUNCTION TESTS
// =============================================================================

describe('utility functions', () => {
  beforeEach(() => {
    resetPlanNodeIds();
  });

  describe('summarizePlan', () => {
    it('should summarize a simple scan', () => {
      const scan = createScanNode('users', ['id'], {
        accessMethod: 'seqScan',
        cost: mockCost(),
      });

      const summary = summarizePlan(scan);
      expect(summary).toContain('SeqScan(users)');
    });

    it('should summarize a plan with joins', () => {
      const left = createScanNode('users', ['id'], { accessMethod: 'seqScan', cost: mockCost() });
      const right = createScanNode('orders', ['id'], { accessMethod: 'seqScan', cost: mockCost() });
      const join = createJoinNode(left, right, 'inner', 'hash', undefined, { cost: mockCost() });

      const summary = summarizePlan(join);
      expect(summary).toContain('hashJoin');
    });
  });

  describe('getTotalCost', () => {
    it('should return the total cost of a plan', () => {
      const scan = createScanNode('users', ['id'], {
        accessMethod: 'seqScan',
        cost: mockCost({ totalCost: 42.5 }),
      });

      expect(getTotalCost(scan)).toBe(42.5);
    });
  });

  describe('getEstimatedRows', () => {
    it('should return the estimated rows of a plan', () => {
      const scan = createScanNode('users', ['id'], {
        accessMethod: 'seqScan',
        cost: mockCost({ estimatedRows: 1000 }),
      });

      expect(getEstimatedRows(scan)).toBe(1000);
    });
  });
});

// =============================================================================
// TYPE SAFETY TESTS
// =============================================================================

describe('type safety', () => {
  beforeEach(() => {
    resetPlanNodeIds();
  });

  it('JsonExplainNode should have all expected properties properly typed', () => {
    const scan = createScanNode('users', ['id', 'name'], {
      accessMethod: 'indexScan',
      indexName: 'idx_users_pk',
      cost: mockCost({ startupCost: 0.5, totalCost: 10.0, estimatedRows: 100 }),
    });

    const json = explain(scan, { format: 'json', costs: true, rowEstimates: true });
    const parsed = JSON.parse(json);
    const plan = parsed[0].Plan as JsonExplainNode;

    // These properties should be properly typed (not `any`)
    const nodeType: string = plan['Node Type'];
    const startupCost: number | undefined = plan['Startup Cost'];
    const totalCost: number | undefined = plan['Total Cost'];
    const planRows: number | undefined = plan['Plan Rows'];
    const relationName: string | undefined = plan['Relation Name'];
    const indexName: string | undefined = plan['Index Name'];
    const plans: JsonExplainNode[] | undefined = plan.Plans;

    expect(typeof nodeType).toBe('string');
    expect(typeof startupCost).toBe('number');
    expect(typeof totalCost).toBe('number');
    expect(typeof planRows).toBe('number');
    expect(typeof relationName).toBe('string');
    expect(typeof indexName).toBe('string');
    expect(plans).toBeUndefined(); // No children for a simple scan
  });
});
