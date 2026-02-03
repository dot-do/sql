/**
 * DoSQL EXPLAIN and EXPLAIN QUERY PLAN Tests
 *
 * Issue: sql-rl5o - Query Explain Plan Visualization API
 *
 * Tests for EXPLAIN and EXPLAIN QUERY PLAN parsing and output formatting.
 *
 * Features:
 * - EXPLAIN parsing for SELECT, INSERT, UPDATE, DELETE
 * - EXPLAIN QUERY PLAN syntax
 * - Human-readable output format
 * - Estimated cost information
 * - Index usage reporting
 * - Join strategy information
 * - Scan type information (full, index, covering)
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  parseSQL,
  detectStatementType,
  type ParseResult,
  type StatementType,
} from '../parser/unified.js';
import {
  explain,
  type ExplainOptions,
  type ExplainFormat,
} from '../planner/explain.js';
import {
  createScanNode,
  createJoinNode,
  createFilterNode,
  createSortNode,
  createLimitNode,
  resetPlanNodeIds,
  type PlanCost,
} from '../planner/types.js';
import type { Predicate } from '../engine/types.js';

// =============================================================================
// TEST HELPERS
// =============================================================================

/**
 * Create a fake PlanCost for testing.
 * This is a test data factory, not a mock with stubbed behavior.
 */
function fakeCost(overrides?: Partial<PlanCost>): PlanCost {
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

// =============================================================================
// EXPLAIN PARSING TESTS
// =============================================================================

describe('EXPLAIN Parsing', () => {
  describe('Statement Type Detection', () => {
    it('should detect EXPLAIN as statement type', () => {
      const type = detectStatementType('EXPLAIN SELECT * FROM users');
      expect(type).toBe('EXPLAIN');
    });

    it('should detect EXPLAIN case-insensitively', () => {
      const type = detectStatementType('explain SELECT * FROM users');
      expect(type).toBe('EXPLAIN');
    });

    it('should detect EXPLAIN QUERY PLAN', () => {
      const type = detectStatementType('EXPLAIN QUERY PLAN SELECT * FROM users');
      expect(type).toBe('EXPLAIN');
    });

    it('should detect EXPLAIN ANALYZE', () => {
      const type = detectStatementType('EXPLAIN ANALYZE SELECT * FROM users');
      expect(type).toBe('EXPLAIN');
    });
  });

  describe('EXPLAIN SELECT Parsing', () => {
    it('should parse EXPLAIN SELECT', () => {
      const result = parseSQL('EXPLAIN SELECT * FROM users');

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.statementType).toBe('EXPLAIN');
        expect(result.ast.type).toBe('explain');
        expect((result.ast as any).statement.type).toBe('select');
      }
    });

    it('should parse EXPLAIN SELECT with WHERE clause', () => {
      const result = parseSQL('EXPLAIN SELECT * FROM users WHERE id = 1');

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.statementType).toBe('EXPLAIN');
        expect(result.ast.type).toBe('explain');
      }
    });

    it('should parse EXPLAIN SELECT with JOIN', () => {
      const result = parseSQL('EXPLAIN SELECT * FROM users JOIN orders ON users.id = orders.user_id');

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.statementType).toBe('EXPLAIN');
      }
    });
  });

  describe('EXPLAIN QUERY PLAN Parsing', () => {
    it('should parse EXPLAIN QUERY PLAN SELECT', () => {
      const result = parseSQL('EXPLAIN QUERY PLAN SELECT * FROM users');

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.statementType).toBe('EXPLAIN');
        expect(result.ast.type).toBe('explain');
        expect((result.ast as any).queryPlan).toBe(true);
      }
    });

    it('should parse EXPLAIN QUERY PLAN with complex query', () => {
      const result = parseSQL(`
        EXPLAIN QUERY PLAN
        SELECT u.name, COUNT(o.id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.active = 1
        GROUP BY u.id
        ORDER BY order_count DESC
        LIMIT 10
      `);

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.statementType).toBe('EXPLAIN');
        expect((result.ast as any).queryPlan).toBe(true);
      }
    });
  });

  describe('EXPLAIN ANALYZE Parsing', () => {
    it('should parse EXPLAIN ANALYZE SELECT', () => {
      const result = parseSQL('EXPLAIN ANALYZE SELECT * FROM users');

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.statementType).toBe('EXPLAIN');
        expect((result.ast as any).analyze).toBe(true);
      }
    });
  });

  describe('EXPLAIN DML Parsing', () => {
    it('should parse EXPLAIN INSERT', () => {
      const result = parseSQL("EXPLAIN INSERT INTO users (name) VALUES ('Alice')");

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.statementType).toBe('EXPLAIN');
        expect((result.ast as any).statement.type).toBe('insert');
      }
    });

    it('should parse EXPLAIN UPDATE', () => {
      const result = parseSQL("EXPLAIN UPDATE users SET name = 'Bob' WHERE id = 1");

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.statementType).toBe('EXPLAIN');
        expect((result.ast as any).statement.type).toBe('update');
      }
    });

    it('should parse EXPLAIN DELETE', () => {
      const result = parseSQL('EXPLAIN DELETE FROM users WHERE id = 1');

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.statementType).toBe('EXPLAIN');
        expect((result.ast as any).statement.type).toBe('delete');
      }
    });
  });
});

// =============================================================================
// EXPLAIN OUTPUT FORMAT TESTS
// =============================================================================

describe('EXPLAIN Output Format', () => {
  beforeEach(() => {
    resetPlanNodeIds();
  });

  describe('SQLite-style Output', () => {
    it('should format simple scan plan', () => {
      const scan = createScanNode('users', ['id', 'name', 'email'], {
        accessMethod: 'seqScan',
        cost: fakeCost(),
      });

      const output = explain(scan, { format: 'text' });

      expect(output).toContain('Seq Scan');
      expect(output).toContain('users');
    });

    it('should format index scan plan', () => {
      const scan = createScanNode('users', ['id', 'name'], {
        accessMethod: 'indexScan',
        indexName: 'idx_users_id',
        indexCondition: [{ column: 'id', operator: 'eq', value: 1, columnIndex: 0 }],
        cost: fakeCost(),
      });

      const output = explain(scan, { format: 'text' });

      expect(output).toContain('Index Scan');
      expect(output).toContain('idx_users_id');
      expect(output).toContain('users');
    });

    it('should format covering index scan', () => {
      const scan = createScanNode('users', ['id'], {
        accessMethod: 'indexOnlyScan',
        indexName: 'idx_users_pk',
        cost: fakeCost(),
      });

      const output = explain(scan, { format: 'text' });

      expect(output).toContain('Index Only Scan');
      expect(output).toContain('idx_users_pk');
    });
  });

  describe('Cost Estimates', () => {
    it('should include estimated cost', () => {
      const scan = createScanNode('users', ['id'], {
        accessMethod: 'seqScan',
        cost: fakeCost({ startupCost: 0.5, totalCost: 25.0 }),
      });

      const output = explain(scan, { format: 'text', costs: true });

      expect(output).toContain('cost=');
      expect(output).toContain('0.50');
      expect(output).toContain('25.00');
    });

    it('should include row estimates', () => {
      const scan = createScanNode('users', ['id'], {
        accessMethod: 'seqScan',
        cost: fakeCost({ estimatedRows: 1000 }),
      });

      const output = explain(scan, { format: 'text', costs: true, rowEstimates: true });

      expect(output).toContain('rows=1000');
    });
  });

  describe('Index Usage', () => {
    it('should show index condition', () => {
      const scan = createScanNode('users', ['id', 'name'], {
        accessMethod: 'indexScan',
        indexName: 'idx_users_id',
        indexCondition: [{ column: 'id', operator: 'eq', value: 42, columnIndex: 0 }],
        cost: fakeCost(),
      });

      const output = explain(scan, { format: 'text' });

      expect(output).toContain('Index Cond');
      expect(output).toContain('id');
    });

    it('should show filter condition', () => {
      const scan = createScanNode('users', ['id', 'name'], {
        accessMethod: 'seqScan',
        filterCondition: comparisonPredicate('active', 'eq', true),
        cost: fakeCost(),
      });

      const output = explain(scan, { format: 'text' });

      expect(output).toContain('Filter');
    });
  });

  describe('Join Strategies', () => {
    it('should show hash join', () => {
      const left = createScanNode('users', ['id'], { accessMethod: 'seqScan', cost: fakeCost() });
      const right = createScanNode('orders', ['user_id'], { accessMethod: 'seqScan', cost: fakeCost() });
      const join = createJoinNode(left, right, 'inner', 'hash', comparisonPredicate('id', 'eq', 1), {
        cost: fakeCost(),
      });

      const output = explain(join, { format: 'text' });

      expect(output).toContain('Hash Join');
    });

    it('should show nested loop join', () => {
      const left = createScanNode('users', ['id'], { accessMethod: 'seqScan', cost: fakeCost() });
      const right = createScanNode('orders', ['user_id'], { accessMethod: 'seqScan', cost: fakeCost() });
      const join = createJoinNode(left, right, 'inner', 'nestedLoop', comparisonPredicate('id', 'eq', 1), {
        cost: fakeCost(),
      });

      const output = explain(join, { format: 'text' });

      expect(output).toContain('Nested Loop');
    });

    it('should show merge join', () => {
      const left = createScanNode('users', ['id'], { accessMethod: 'seqScan', cost: fakeCost() });
      const right = createScanNode('orders', ['user_id'], { accessMethod: 'seqScan', cost: fakeCost() });
      const join = createJoinNode(left, right, 'inner', 'merge', comparisonPredicate('id', 'eq', 1), {
        cost: fakeCost(),
      });

      const output = explain(join, { format: 'text' });

      expect(output).toContain('Merge Join');
    });

    it('should show LEFT JOIN', () => {
      const left = createScanNode('users', ['id'], { accessMethod: 'seqScan', cost: fakeCost() });
      const right = createScanNode('orders', ['user_id'], { accessMethod: 'seqScan', cost: fakeCost() });
      const join = createJoinNode(left, right, 'left', 'hash', undefined, {
        cost: fakeCost(),
      });

      const output = explain(join, { format: 'text' });

      expect(output).toContain('Hash Join');
      expect(output).toContain('LEFT JOIN');
    });
  });

  describe('Scan Types', () => {
    it('should identify full table scan', () => {
      const scan = createScanNode('users', ['id', 'name', 'email'], {
        accessMethod: 'seqScan',
        cost: fakeCost(),
      });

      const output = explain(scan, { format: 'text' });

      expect(output).toContain('Seq Scan');
    });

    it('should identify index scan', () => {
      const scan = createScanNode('users', ['id', 'name'], {
        accessMethod: 'indexScan',
        indexName: 'idx_users_id',
        cost: fakeCost(),
      });

      const output = explain(scan, { format: 'text' });

      expect(output).toContain('Index Scan');
    });

    it('should identify covering index scan', () => {
      const scan = createScanNode('users', ['id'], {
        accessMethod: 'indexOnlyScan',
        indexName: 'idx_users_pk',
        cost: fakeCost(),
      });

      const output = explain(scan, { format: 'text' });

      expect(output).toContain('Index Only Scan');
    });

    it('should identify bitmap index scan', () => {
      const scan = createScanNode('users', ['id', 'name'], {
        accessMethod: 'bitmapIndexScan',
        indexName: 'idx_users_active',
        cost: fakeCost(),
      });

      const output = explain(scan, { format: 'text' });

      expect(output).toContain('Bitmap Index Scan');
    });
  });
});

// =============================================================================
// EXPLAIN QUERY PLAN OUTPUT TESTS
// =============================================================================

describe('EXPLAIN QUERY PLAN Output', () => {
  beforeEach(() => {
    resetPlanNodeIds();
  });

  it('should produce tree format output', () => {
    const scan = createScanNode('t1', ['id'], {
      accessMethod: 'seqScan',
      cost: fakeCost(),
    });

    const output = explain(scan, { format: 'tree' });

    // Tree format uses ASCII characters
    expect(output).toMatch(/[`|]--.*/);
    expect(output).toContain('Seq Scan');
    expect(output).toContain('t1');
  });

  it('should show nested plan structure', () => {
    const scan = createScanNode('users', ['id', 'name'], {
      accessMethod: 'seqScan',
      cost: fakeCost(),
    });

    const filter = createFilterNode(scan, comparisonPredicate('id', 'gt', 10), {
      cost: fakeCost(),
    });

    const sort = createSortNode(filter, [{ expr: { type: 'columnRef', column: 'name' }, direction: 'asc' }], {
      cost: fakeCost(),
    });

    const limit = createLimitNode(sort, 10, undefined, {
      cost: fakeCost(),
    });

    const output = explain(limit, { format: 'text' });

    // Should show nested structure with arrows
    expect(output).toContain('Limit');
    expect(output).toContain('Sort');
    expect(output).toContain('Filter');
    expect(output).toContain('Seq Scan');
    expect(output).toContain('->');
  });

  it('should show index lookup in expected format', () => {
    const scan = createScanNode('t1', ['id'], {
      accessMethod: 'indexScan',
      indexName: 'idx_id',
      indexCondition: [{ column: 'id', operator: 'eq', value: 1, columnIndex: 0 }],
      cost: fakeCost(),
    });

    const output = explain(scan, { format: 'text' });

    // Expected format similar to SQLite
    expect(output).toContain('Index Scan');
    expect(output).toContain('idx_id');
  });
});

// =============================================================================
// EXPLAIN JSON FORMAT TESTS
// =============================================================================

describe('EXPLAIN JSON Format', () => {
  beforeEach(() => {
    resetPlanNodeIds();
  });

  it('should produce valid JSON output', () => {
    const scan = createScanNode('users', ['id', 'name'], {
      accessMethod: 'seqScan',
      cost: fakeCost(),
    });

    const output = explain(scan, { format: 'json' });

    // Should be valid JSON
    const parsed = JSON.parse(output);
    expect(parsed).toBeInstanceOf(Array);
    expect(parsed[0]).toHaveProperty('Plan');
  });

  it('should include all plan properties in JSON', () => {
    const scan = createScanNode('users', ['id', 'name'], {
      accessMethod: 'indexScan',
      indexName: 'idx_users_pk',
      cost: fakeCost({ startupCost: 0.5, totalCost: 10.0, estimatedRows: 100 }),
    });

    const output = explain(scan, { format: 'json', costs: true, rowEstimates: true });
    const parsed = JSON.parse(output);
    const plan = parsed[0].Plan;

    expect(plan['Node Type']).toBe('Index Scan');
    expect(plan['Relation Name']).toBe('users');
    expect(plan['Index Name']).toBe('idx_users_pk');
    expect(plan['Startup Cost']).toBe(0.5);
    expect(plan['Total Cost']).toBe(10.0);
    expect(plan['Plan Rows']).toBe(100);
  });

  it('should nest child plans in JSON', () => {
    const scan = createScanNode('users', ['id'], { accessMethod: 'seqScan', cost: fakeCost() });
    const filter = createFilterNode(scan, comparisonPredicate('id', 'gt', 10), { cost: fakeCost() });

    const output = explain(filter, { format: 'json' });
    const parsed = JSON.parse(output);
    const plan = parsed[0].Plan;

    expect(plan['Node Type']).toBe('Filter');
    expect(plan.Plans).toHaveLength(1);
    expect(plan.Plans[0]['Node Type']).toBe('Seq Scan');
  });
});

// =============================================================================
// EXPLAIN STATEMENT TYPE TESTS
// =============================================================================

describe('EXPLAIN Statement AST Types', () => {
  it('should have correct AST structure for EXPLAIN', () => {
    const result = parseSQL('EXPLAIN SELECT * FROM users');

    expect(result.success).toBe(true);
    if (result.success) {
      const ast = result.ast as any;
      expect(ast.type).toBe('explain');
      expect(ast.queryPlan).toBeFalsy();
      expect(ast.analyze).toBeFalsy();
      expect(ast.statement).toBeDefined();
    }
  });

  it('should have correct AST structure for EXPLAIN QUERY PLAN', () => {
    const result = parseSQL('EXPLAIN QUERY PLAN SELECT * FROM users');

    expect(result.success).toBe(true);
    if (result.success) {
      const ast = result.ast as any;
      expect(ast.type).toBe('explain');
      expect(ast.queryPlan).toBe(true);
      expect(ast.statement).toBeDefined();
    }
  });

  it('should have correct AST structure for EXPLAIN ANALYZE', () => {
    const result = parseSQL('EXPLAIN ANALYZE SELECT * FROM users');

    expect(result.success).toBe(true);
    if (result.success) {
      const ast = result.ast as any;
      expect(ast.type).toBe('explain');
      expect(ast.analyze).toBe(true);
      expect(ast.statement).toBeDefined();
    }
  });

  it('should preserve inner statement in AST', () => {
    const result = parseSQL('EXPLAIN SELECT id, name FROM users WHERE active = true ORDER BY name');

    expect(result.success).toBe(true);
    if (result.success) {
      const ast = result.ast as any;
      const stmt = ast.statement;

      expect(stmt.type).toBe('select');
      expect(stmt.columns).toHaveLength(2);
      expect(stmt.where).toBeDefined();
      expect(stmt.orderBy).toBeDefined();
    }
  });
});

// =============================================================================
// EXPLAIN FORMAT OPTIONS TESTS
// =============================================================================

describe('EXPLAIN Format Options', () => {
  beforeEach(() => {
    resetPlanNodeIds();
  });

  it('should support text format (default)', () => {
    const scan = createScanNode('users', ['id'], { accessMethod: 'seqScan', cost: fakeCost() });
    const output = explain(scan);

    expect(typeof output).toBe('string');
    expect(output).toContain('Seq Scan');
  });

  it('should support json format', () => {
    const scan = createScanNode('users', ['id'], { accessMethod: 'seqScan', cost: fakeCost() });
    const output = explain(scan, { format: 'json' });

    expect(() => JSON.parse(output)).not.toThrow();
  });

  it('should support yaml format', () => {
    const scan = createScanNode('users', ['id'], { accessMethod: 'seqScan', cost: fakeCost() });
    const output = explain(scan, { format: 'yaml' });

    expect(output).toContain('Node Type:');
    expect(output).toContain('Seq Scan');
  });

  it('should support tree format', () => {
    const scan = createScanNode('users', ['id'], { accessMethod: 'seqScan', cost: fakeCost() });
    const output = explain(scan, { format: 'tree' });

    expect(output).toMatch(/[`|]--.*/);
  });

  it('should toggle costs display', () => {
    const scan = createScanNode('users', ['id'], {
      accessMethod: 'seqScan',
      cost: fakeCost({ startupCost: 1.0, totalCost: 50.0 }),
    });

    const withCosts = explain(scan, { format: 'text', costs: true });
    const withoutCosts = explain(scan, { format: 'text', costs: false });

    expect(withCosts).toContain('cost=');
    expect(withoutCosts).not.toContain('cost=');
  });

  it('should toggle row estimates display', () => {
    const scan = createScanNode('users', ['id'], {
      accessMethod: 'seqScan',
      cost: fakeCost({ estimatedRows: 500 }),
    });

    const withRows = explain(scan, { format: 'text', costs: true, rowEstimates: true });
    const withoutRows = explain(scan, { format: 'text', costs: true, rowEstimates: false });

    expect(withRows).toContain('rows=500');
    expect(withoutRows).not.toContain('rows=');
  });
});
