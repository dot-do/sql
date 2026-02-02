/**
 * CTE (Common Table Expression) Operator Tests
 *
 * Comprehensive tests for SQL CTEs:
 * - Simple (non-recursive) CTEs
 * - Recursive CTEs with iterative fixed-point execution
 * - Multiple CTEs in WITH clause
 * - CTE with column aliases
 * - CTE references within queries
 * - Edge cases: empty results, limits, nested references
 *
 * Following the project's TDD with NO MOCKS philosophy.
 */

import { describe, it, expect, beforeEach } from 'vitest';

import {
  createCTEContext,
  CTEScanOperator,
  executeSimpleCTE,
  executeRecursiveCTE,
  executeWithClause,
  getCTERows,
  isCTEMaterialized,
  getCTEColumns,
  createMockQueryExecutor,
  isCTEPlan,
  isCTEScanPlan,
  type CTEExecutionContext,
  type MaterializedCTE,
  type CTEPlan,
  type CTEScanPlan,
} from '../cte.js';
import {
  type Row,
  type ExecutionContext,
  type Operator,
} from '../../types.js';
import { type CTEDefinition, type WithClause } from '../../../parser/cte-types.js';

// =============================================================================
// TEST HELPERS
// =============================================================================

/**
 * Create a minimal mock execution context
 */
function createMockExecutionContext(): ExecutionContext {
  return {
    schema: { tables: new Map() },
    btree: {
      get: async () => undefined,
      range: async function* () {},
      scan: async function* () {},
      set: async () => {},
      delete: async () => false,
      count: async () => 0,
    },
    columnar: {
      scan: async function* () {},
      count: async () => 0,
      sum: async () => null,
      minMax: async () => ({ min: null, max: null }),
    },
  };
}

/**
 * Collect all rows from an operator
 */
async function collectRows(operator: Operator, ctx?: ExecutionContext): Promise<Row[]> {
  const context = ctx ?? createMockExecutionContext();
  await operator.open(context);
  const results: Row[] = [];
  let row: Row | null;
  while ((row = await operator.next()) !== null) {
    results.push(row);
  }
  await operator.close();
  return results;
}

// =============================================================================
// CTE CONTEXT TESTS
// =============================================================================

describe('CTE Context', () => {
  it('should create CTE context with default limits', () => {
    const baseCtx = createMockExecutionContext();
    const cteCtx = createCTEContext(baseCtx);

    expect(cteCtx.materializedCTEs).toBeDefined();
    expect(cteCtx.materializedCTEs.size).toBe(0);
    expect(cteCtx.recursiveLimits.maxIterations).toBe(1000);
    expect(cteCtx.recursiveLimits.maxRows).toBe(100000);
  });

  it('should create CTE context with custom limits', () => {
    const baseCtx = createMockExecutionContext();
    const cteCtx = createCTEContext(baseCtx, {
      maxIterations: 500,
      maxRows: 50000,
    });

    expect(cteCtx.recursiveLimits.maxIterations).toBe(500);
    expect(cteCtx.recursiveLimits.maxRows).toBe(50000);
  });

  it('should preserve base context properties', () => {
    const baseCtx = createMockExecutionContext();
    baseCtx.transactionId = 'txn-123';
    const cteCtx = createCTEContext(baseCtx);

    expect(cteCtx.transactionId).toBe('txn-123');
    expect(cteCtx.schema).toBe(baseCtx.schema);
  });
});

// =============================================================================
// SIMPLE CTE TESTS
// =============================================================================

describe('CTE - Simple (Non-Recursive)', () => {
  it('should materialize simple CTE', async () => {
    const cte: CTEDefinition = {
      name: 'active_users',
      query: 'SELECT * FROM users WHERE status = "active"',
      recursive: false,
    };

    const tables = new Map<string, Row[]>();
    tables.set('users', [
      { id: 1, name: 'Alice', status: 'active' },
      { id: 2, name: 'Bob', status: 'inactive' },
      { id: 3, name: 'Charlie', status: 'active' },
    ]);

    // Create a query executor that filters for active users
    const queryExecutor = async (sql: string, ctx: ExecutionContext): Promise<Row[]> => {
      if (sql.toLowerCase().includes('active')) {
        return tables.get('users')!.filter(r => r.status === 'active');
      }
      return tables.get('users') || [];
    };

    const cteCtx = createCTEContext(createMockExecutionContext());
    const materialized = await executeSimpleCTE(cte, queryExecutor, cteCtx);

    expect(materialized.name).toBe('active_users');
    expect(materialized.rows).toHaveLength(2);
    expect(materialized.rows.map(r => r.name)).toContain('Alice');
    expect(materialized.rows.map(r => r.name)).toContain('Charlie');
  });

  it('should apply column aliases', async () => {
    const cte: CTEDefinition = {
      name: 'user_ids',
      columns: ['user_id', 'user_name'],
      query: 'SELECT id, name FROM users',
      recursive: false,
    };

    const queryExecutor = async (): Promise<Row[]> => [
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ];

    const cteCtx = createCTEContext(createMockExecutionContext());
    const materialized = await executeSimpleCTE(cte, queryExecutor, cteCtx);

    expect(materialized.columns).toEqual(['user_id', 'user_name']);
    expect(materialized.rows[0]).toHaveProperty('user_id');
    expect(materialized.rows[0]).toHaveProperty('user_name');
    expect(materialized.rows[0]).not.toHaveProperty('id');
    expect(materialized.rows[0]).not.toHaveProperty('name');
  });

  it('should handle empty CTE result', async () => {
    const cte: CTEDefinition = {
      name: 'empty_cte',
      query: 'SELECT * FROM nothing',
      recursive: false,
    };

    const queryExecutor = async (): Promise<Row[]> => [];

    const cteCtx = createCTEContext(createMockExecutionContext());
    const materialized = await executeSimpleCTE(cte, queryExecutor, cteCtx);

    expect(materialized.rows).toHaveLength(0);
    expect(materialized.columns).toEqual([]);
  });

  it('should throw error for column count mismatch', async () => {
    const cte: CTEDefinition = {
      name: 'mismatched',
      columns: ['a', 'b', 'c'], // 3 columns
      query: 'SELECT * FROM test',
      recursive: false,
    };

    const queryExecutor = async (): Promise<Row[]> => [
      { x: 1, y: 2 }, // Only 2 columns
    ];

    const cteCtx = createCTEContext(createMockExecutionContext());

    await expect(executeSimpleCTE(cte, queryExecutor, cteCtx))
      .rejects.toThrow('column count mismatch');
  });
});

// =============================================================================
// RECURSIVE CTE TESTS
// =============================================================================

describe('CTE - Recursive', () => {
  it('should execute recursive CTE for tree traversal', async () => {
    // Simulate: WITH RECURSIVE tree AS (
    //   SELECT id, parent_id, name FROM nodes WHERE parent_id IS NULL
    //   UNION ALL
    //   SELECT n.id, n.parent_id, n.name FROM nodes n JOIN tree t ON n.parent_id = t.id
    // )
    const cte: CTEDefinition = {
      name: 'tree',
      recursive: true,
      anchorQuery: 'SELECT * FROM nodes WHERE parent_id IS NULL',
      recursiveQuery: 'SELECT * FROM nodes n JOIN tree t ON n.parent_id = t.id',
      query: '', // Required by interface but not used in recursive
    };

    const nodes = [
      { id: 1, parent_id: null, name: 'root' },
      { id: 2, parent_id: 1, name: 'child1' },
      { id: 3, parent_id: 1, name: 'child2' },
      { id: 4, parent_id: 2, name: 'grandchild1' },
      { id: 5, parent_id: 3, name: 'grandchild2' },
    ];

    const queryExecutor = async (sql: string, ctx: ExecutionContext): Promise<Row[]> => {
      const cteCtx = ctx as CTEExecutionContext;

      if (sql.includes('parent_id IS NULL')) {
        // Anchor query: root nodes
        return nodes.filter(n => n.parent_id === null);
      }

      // Recursive query: join with tree CTE
      const tree = cteCtx.materializedCTEs?.get('tree');
      if (!tree) return [];

      // Find children of current tree nodes
      const parentIds = new Set(tree.rows.map(r => r.id));
      return nodes.filter(n => n.parent_id !== null && parentIds.has(n.parent_id));
    };

    const cteCtx = createCTEContext(createMockExecutionContext());
    const materialized = await executeRecursiveCTE(cte, queryExecutor, cteCtx);

    expect(materialized.rows).toHaveLength(5);
    expect(materialized.rows.map(r => r.name)).toContain('root');
    expect(materialized.rows.map(r => r.name)).toContain('child1');
    expect(materialized.rows.map(r => r.name)).toContain('grandchild1');
  });

  it('should execute recursive CTE for number sequence', async () => {
    // WITH RECURSIVE nums AS (
    //   SELECT 1 AS n
    //   UNION ALL
    //   SELECT n + 1 FROM nums WHERE n < 5
    // )
    const cte: CTEDefinition = {
      name: 'nums',
      columns: ['n'],
      recursive: true,
      anchorQuery: 'SELECT 1',
      recursiveQuery: 'SELECT n + 1 FROM nums WHERE n < 5',
      query: '',
    };

    let iteration = 0;
    const queryExecutor = async (sql: string, ctx: ExecutionContext): Promise<Row[]> => {
      const cteCtx = ctx as CTEExecutionContext;

      if (sql.includes('SELECT 1')) {
        return [{ n: 1 }];
      }

      // Get current nums from CTE
      const nums = cteCtx.materializedCTEs?.get('nums');
      if (!nums) return [];

      // Add 1 to each number that's less than 5
      return nums.rows
        .filter(r => (r.n as number) < 5)
        .map(r => ({ n: (r.n as number) + 1 }));
    };

    const cteCtx = createCTEContext(createMockExecutionContext());
    const materialized = await executeRecursiveCTE(cte, queryExecutor, cteCtx);

    expect(materialized.rows).toHaveLength(5);
    const numbers = materialized.rows.map(r => r.n).sort((a, b) => (a as number) - (b as number));
    expect(numbers).toEqual([1, 2, 3, 4, 5]);
  });

  it('should stop when no new rows are produced', async () => {
    const cte: CTEDefinition = {
      name: 'test',
      recursive: true,
      anchorQuery: 'SELECT 1',
      recursiveQuery: 'SELECT * FROM test',
      query: '',
    };

    let callCount = 0;
    const queryExecutor = async (sql: string, ctx: ExecutionContext): Promise<Row[]> => {
      callCount++;
      if (sql.includes('SELECT 1')) {
        return [{ value: 1 }];
      }
      // Return empty after first iteration
      return [];
    };

    const cteCtx = createCTEContext(createMockExecutionContext());
    const materialized = await executeRecursiveCTE(cte, queryExecutor, cteCtx);

    expect(materialized.rows).toHaveLength(1);
    // Should have stopped after one empty result
    expect(callCount).toBe(2); // anchor + one recursive call
  });

  it('should stop when only duplicate rows are produced', async () => {
    const cte: CTEDefinition = {
      name: 'test',
      recursive: true,
      anchorQuery: 'SELECT 1',
      recursiveQuery: 'SELECT * FROM test',
      query: '',
    };

    const queryExecutor = async (sql: string, ctx: ExecutionContext): Promise<Row[]> => {
      // Always return the same row
      return [{ value: 1 }];
    };

    const cteCtx = createCTEContext(createMockExecutionContext());
    const materialized = await executeRecursiveCTE(cte, queryExecutor, cteCtx);

    // Should deduplicate and stop
    expect(materialized.rows).toHaveLength(1);
  });

  it('should respect max iterations limit', async () => {
    const cte: CTEDefinition = {
      name: 'infinite',
      recursive: true,
      anchorQuery: 'SELECT 0',
      recursiveQuery: 'SELECT n + 1 FROM infinite',
      query: '',
    };

    let n = 0;
    const queryExecutor = async (sql: string): Promise<Row[]> => {
      if (sql.includes('SELECT 0')) {
        n = 0;
        return [{ n: 0 }];
      }
      // Always produce new rows
      return [{ n: ++n }];
    };

    const cteCtx = createCTEContext(createMockExecutionContext(), {
      maxIterations: 10,
    });

    await expect(executeRecursiveCTE(cte, queryExecutor, cteCtx))
      .rejects.toThrow('exceeded maximum iterations');
  });

  it('should respect max rows limit', async () => {
    const cte: CTEDefinition = {
      name: 'many_rows',
      recursive: true,
      anchorQuery: 'SELECT 1',
      recursiveQuery: 'SELECT * FROM many_rows',
      query: '',
    };

    let n = 1;
    const queryExecutor = async (sql: string): Promise<Row[]> => {
      if (sql.includes('SELECT 1')) {
        return [{ n: n++ }];
      }
      // Produce many new rows each iteration
      const rows: Row[] = [];
      for (let i = 0; i < 1000; i++) {
        rows.push({ n: n++ });
      }
      return rows;
    };

    const cteCtx = createCTEContext(createMockExecutionContext(), {
      maxRows: 100,
    });

    await expect(executeRecursiveCTE(cte, queryExecutor, cteCtx))
      .rejects.toThrow('exceeded maximum row limit');
  });

  it('should return empty result when anchor returns nothing', async () => {
    const cte: CTEDefinition = {
      name: 'empty_anchor',
      recursive: true,
      anchorQuery: 'SELECT * FROM empty',
      recursiveQuery: 'SELECT * FROM empty_anchor',
      query: '',
    };

    const queryExecutor = async (): Promise<Row[]> => [];

    const cteCtx = createCTEContext(createMockExecutionContext());
    const materialized = await executeRecursiveCTE(cte, queryExecutor, cteCtx);

    expect(materialized.rows).toHaveLength(0);
  });

  it('should throw error when anchor/recursive queries missing', async () => {
    const cte: CTEDefinition = {
      name: 'invalid',
      recursive: true,
      query: 'SELECT 1', // No anchor or recursive
    };

    const queryExecutor = async (): Promise<Row[]> => [];
    const cteCtx = createCTEContext(createMockExecutionContext());

    await expect(executeRecursiveCTE(cte, queryExecutor, cteCtx))
      .rejects.toThrow('must have both anchor and recursive queries');
  });
});

// =============================================================================
// WITH CLAUSE TESTS
// =============================================================================

describe('CTE - WITH Clause', () => {
  it('should execute multiple CTEs in order', async () => {
    const withClause: WithClause = {
      recursive: false,
      ctes: [
        {
          name: 'cte1',
          query: 'SELECT 1 AS a',
          recursive: false,
        },
        {
          name: 'cte2',
          query: 'SELECT 2 AS b',
          recursive: false,
        },
      ],
    };

    let cte1Executed = false;
    let cte2Executed = false;

    const queryExecutor = async (sql: string, ctx: ExecutionContext): Promise<Row[]> => {
      if (sql.includes('SELECT 1')) {
        cte1Executed = true;
        return [{ a: 1 }];
      }
      if (sql.includes('SELECT 2')) {
        // cte1 should already be executed
        expect(cte1Executed).toBe(true);
        cte2Executed = true;
        return [{ b: 2 }];
      }
      return [];
    };

    const baseCtx = createMockExecutionContext();
    const cteCtx = await executeWithClause(withClause, queryExecutor, baseCtx);

    expect(cte1Executed).toBe(true);
    expect(cte2Executed).toBe(true);
    expect(cteCtx.materializedCTEs.size).toBe(2);
    expect(isCTEMaterialized(cteCtx, 'cte1')).toBe(true);
    expect(isCTEMaterialized(cteCtx, 'cte2')).toBe(true);
  });

  it('should allow later CTEs to reference earlier CTEs', async () => {
    const withClause: WithClause = {
      recursive: false,
      ctes: [
        {
          name: 'numbers',
          query: 'SELECT 1 AS n UNION SELECT 2 AS n',
          recursive: false,
        },
        {
          name: 'doubled',
          query: 'SELECT n * 2 AS n FROM numbers',
          recursive: false,
        },
      ],
    };

    const queryExecutor = async (sql: string, ctx: ExecutionContext): Promise<Row[]> => {
      const cteCtx = ctx as CTEExecutionContext;

      if (sql.includes('SELECT 1 AS n')) {
        return [{ n: 1 }, { n: 2 }];
      }
      if (sql.includes('FROM numbers')) {
        const numbers = cteCtx.materializedCTEs?.get('numbers');
        return numbers?.rows.map(r => ({ n: (r.n as number) * 2 })) || [];
      }
      return [];
    };

    const baseCtx = createMockExecutionContext();
    const cteCtx = await executeWithClause(withClause, queryExecutor, baseCtx);

    const doubled = getCTERows(cteCtx, 'doubled');
    expect(doubled).toHaveLength(2);
    expect(doubled?.map(r => r.n).sort()).toEqual([2, 4]);
  });

  it('should handle mixed simple and recursive CTEs', async () => {
    const withClause: WithClause = {
      recursive: true,
      ctes: [
        {
          name: 'base',
          query: 'SELECT 1 AS seed',
          recursive: false,
        },
        {
          name: 'sequence',
          recursive: true,
          anchorQuery: 'SELECT seed AS n FROM base',
          recursiveQuery: 'SELECT n + 1 FROM sequence WHERE n < 3',
          query: '',
        },
      ],
    };

    const queryExecutor = async (sql: string, ctx: ExecutionContext): Promise<Row[]> => {
      const cteCtx = ctx as CTEExecutionContext;

      if (sql.includes('SELECT 1 AS seed')) {
        return [{ seed: 1 }];
      }
      if (sql.includes('FROM base')) {
        const base = cteCtx.materializedCTEs?.get('base');
        return base?.rows.map(r => ({ n: r.seed })) || [];
      }
      if (sql.includes('FROM sequence WHERE')) {
        const seq = cteCtx.materializedCTEs?.get('sequence');
        if (!seq) return [];
        return seq.rows
          .filter(r => (r.n as number) < 3)
          .map(r => ({ n: (r.n as number) + 1 }));
      }
      return [];
    };

    const baseCtx = createMockExecutionContext();
    const cteCtx = await executeWithClause(withClause, queryExecutor, baseCtx);

    expect(isCTEMaterialized(cteCtx, 'base')).toBe(true);
    expect(isCTEMaterialized(cteCtx, 'sequence')).toBe(true);

    const sequence = getCTERows(cteCtx, 'sequence');
    expect(sequence).toHaveLength(3);
    expect(sequence?.map(r => r.n).sort((a, b) => (a as number) - (b as number))).toEqual([1, 2, 3]);
  });
});

// =============================================================================
// CTE SCAN OPERATOR TESTS
// =============================================================================

describe('CTE Scan Operator', () => {
  it('should scan materialized CTE rows', async () => {
    const baseCtx = createMockExecutionContext();
    const cteCtx = createCTEContext(baseCtx);

    // Pre-materialize a CTE
    cteCtx.materializedCTEs.set('users', {
      name: 'users',
      rows: [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
      ],
      columns: ['id', 'name'],
    });

    const scanOperator = new CTEScanOperator('users');
    const results = await collectRows(scanOperator, cteCtx);

    expect(results).toHaveLength(2);
    expect(results[0]).toEqual({ id: 1, name: 'Alice' });
    expect(results[1]).toEqual({ id: 2, name: 'Bob' });
  });

  it('should apply alias to scanned rows', async () => {
    const baseCtx = createMockExecutionContext();
    const cteCtx = createCTEContext(baseCtx);

    cteCtx.materializedCTEs.set('data', {
      name: 'data',
      rows: [{ value: 100 }],
      columns: ['value'],
    });

    const scanOperator = new CTEScanOperator('data', 'd');
    const results = await collectRows(scanOperator, cteCtx);

    expect(results).toHaveLength(1);
    expect(results[0]).toHaveProperty('value', 100);
    expect(results[0]).toHaveProperty('d.value', 100);
  });

  it('should throw error for non-materialized CTE', async () => {
    const baseCtx = createMockExecutionContext();
    const cteCtx = createCTEContext(baseCtx);

    const scanOperator = new CTEScanOperator('nonexistent');

    await expect(collectRows(scanOperator, cteCtx))
      .rejects.toThrow('CTE \'nonexistent\' is not materialized');
  });

  it('should handle case-insensitive CTE names', async () => {
    const baseCtx = createMockExecutionContext();
    const cteCtx = createCTEContext(baseCtx);

    cteCtx.materializedCTEs.set('mydata', {
      name: 'MyData',
      rows: [{ x: 1 }],
      columns: ['x'],
    });

    const scanOperator = new CTEScanOperator('MYDATA');
    const results = await collectRows(scanOperator, cteCtx);

    expect(results).toHaveLength(1);
  });

  it('should return correct columns', async () => {
    const baseCtx = createMockExecutionContext();
    const cteCtx = createCTEContext(baseCtx);

    cteCtx.materializedCTEs.set('test', {
      name: 'test',
      rows: [{ a: 1, b: 2, c: 3 }],
      columns: ['a', 'b', 'c'],
    });

    const scanOperator = new CTEScanOperator('test');
    await scanOperator.open(cteCtx);

    expect(scanOperator.columns()).toEqual(['a', 'b', 'c']);
  });

  it('should handle empty CTE', async () => {
    const baseCtx = createMockExecutionContext();
    const cteCtx = createCTEContext(baseCtx);

    cteCtx.materializedCTEs.set('empty', {
      name: 'empty',
      rows: [],
      columns: ['id'],
    });

    const scanOperator = new CTEScanOperator('empty');
    const results = await collectRows(scanOperator, cteCtx);

    expect(results).toHaveLength(0);
  });
});

// =============================================================================
// UTILITY FUNCTION TESTS
// =============================================================================

describe('CTE Utility Functions', () => {
  describe('getCTERows', () => {
    it('should return rows for existing CTE', () => {
      const cteCtx = createCTEContext(createMockExecutionContext());
      cteCtx.materializedCTEs.set('test', {
        name: 'test',
        rows: [{ id: 1 }, { id: 2 }],
        columns: ['id'],
      });

      const rows = getCTERows(cteCtx, 'test');
      expect(rows).toHaveLength(2);
    });

    it('should return undefined for non-existent CTE', () => {
      const cteCtx = createCTEContext(createMockExecutionContext());
      expect(getCTERows(cteCtx, 'missing')).toBeUndefined();
    });

    it('should be case-insensitive', () => {
      const cteCtx = createCTEContext(createMockExecutionContext());
      cteCtx.materializedCTEs.set('mytest', {
        name: 'MyTest',
        rows: [{ x: 1 }],
        columns: ['x'],
      });

      expect(getCTERows(cteCtx, 'MYTEST')).toBeDefined();
      expect(getCTERows(cteCtx, 'mytest')).toBeDefined();
    });
  });

  describe('isCTEMaterialized', () => {
    it('should return true for materialized CTE', () => {
      const cteCtx = createCTEContext(createMockExecutionContext());
      cteCtx.materializedCTEs.set('exists', {
        name: 'exists',
        rows: [],
        columns: [],
      });

      expect(isCTEMaterialized(cteCtx, 'exists')).toBe(true);
    });

    it('should return false for non-materialized CTE', () => {
      const cteCtx = createCTEContext(createMockExecutionContext());
      expect(isCTEMaterialized(cteCtx, 'missing')).toBe(false);
    });
  });

  describe('getCTEColumns', () => {
    it('should return columns for existing CTE', () => {
      const cteCtx = createCTEContext(createMockExecutionContext());
      cteCtx.materializedCTEs.set('test', {
        name: 'test',
        rows: [],
        columns: ['id', 'name', 'status'],
      });

      const columns = getCTEColumns(cteCtx, 'test');
      expect(columns).toEqual(['id', 'name', 'status']);
    });

    it('should return undefined for non-existent CTE', () => {
      const cteCtx = createCTEContext(createMockExecutionContext());
      expect(getCTEColumns(cteCtx, 'missing')).toBeUndefined();
    });
  });

  describe('isCTEPlan', () => {
    it('should return true for CTE plan', () => {
      const plan: CTEPlan = {
        type: 'cte',
        id: 1,
        withClause: { recursive: false, ctes: [] },
        mainQuery: { type: 'scan', id: 2, table: 'test', source: 'btree', columns: [] },
      };

      expect(isCTEPlan(plan)).toBe(true);
    });

    it('should return false for other plan types', () => {
      expect(isCTEPlan({ type: 'scan', id: 1, table: 'test', source: 'btree', columns: [] })).toBe(false);
      expect(isCTEPlan({ type: 'filter' })).toBe(false);
      expect(isCTEPlan(null)).toBe(false);
      expect(isCTEPlan(undefined)).toBe(false);
    });
  });

  describe('isCTEScanPlan', () => {
    it('should return true for CTE scan plan', () => {
      const plan: CTEScanPlan = {
        type: 'cteScan',
        id: 1,
        cteName: 'test_cte',
        columns: ['id', 'name'],
      };

      expect(isCTEScanPlan(plan)).toBe(true);
    });

    it('should return false for other plan types', () => {
      expect(isCTEScanPlan({ type: 'scan' })).toBe(false);
      expect(isCTEScanPlan({ type: 'cte' })).toBe(false);
    });
  });
});

// =============================================================================
// MOCK QUERY EXECUTOR TESTS
// =============================================================================

describe('Mock Query Executor', () => {
  it('should return rows from tables', async () => {
    const tables = new Map<string, Row[]>();
    tables.set('users', [
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
    ]);

    const executor = createMockQueryExecutor(tables);
    const ctx = createMockExecutionContext();

    const results = await executor('SELECT * FROM users', ctx);
    expect(results).toHaveLength(2);
  });

  it('should return rows from materialized CTEs', async () => {
    const tables = new Map<string, Row[]>();
    const executor = createMockQueryExecutor(tables);

    const cteCtx = createCTEContext(createMockExecutionContext());
    cteCtx.materializedCTEs.set('mycte', {
      name: 'mycte',
      rows: [{ value: 42 }],
      columns: ['value'],
    });

    const results = await executor('SELECT * FROM mycte', cteCtx);
    expect(results).toHaveLength(1);
    expect(results[0].value).toBe(42);
  });

  it('should return empty array for unknown tables', async () => {
    const tables = new Map<string, Row[]>();
    const executor = createMockQueryExecutor(tables);
    const ctx = createMockExecutionContext();

    const results = await executor('SELECT * FROM nonexistent', ctx);
    expect(results).toHaveLength(0);
  });

  it('should be case-insensitive for table names', async () => {
    const tables = new Map<string, Row[]>();
    tables.set('users', [{ id: 1 }]);
    const executor = createMockQueryExecutor(tables);
    const ctx = createMockExecutionContext();

    const results = await executor('SELECT * FROM USERS', ctx);
    expect(results).toHaveLength(1);
  });
});

// =============================================================================
// EDGE CASES
// =============================================================================

describe('CTE - Edge Cases', () => {
  it('should handle CTE with special characters in name', async () => {
    const cteCtx = createCTEContext(createMockExecutionContext());
    cteCtx.materializedCTEs.set('my_cte_123', {
      name: 'my_cte_123',
      rows: [{ x: 1 }],
      columns: ['x'],
    });

    expect(isCTEMaterialized(cteCtx, 'my_cte_123')).toBe(true);
  });

  it('should handle large number of CTEs', async () => {
    const withClause: WithClause = {
      recursive: false,
      ctes: Array.from({ length: 100 }, (_, i) => ({
        name: `cte_${i}`,
        query: `SELECT ${i} AS value`,
        recursive: false,
      })),
    };

    let executionCount = 0;
    const queryExecutor = async (sql: string): Promise<Row[]> => {
      executionCount++;
      const match = sql.match(/SELECT (\d+)/);
      return match ? [{ value: parseInt(match[1]) }] : [];
    };

    const baseCtx = createMockExecutionContext();
    const cteCtx = await executeWithClause(withClause, queryExecutor, baseCtx);

    expect(executionCount).toBe(100);
    expect(cteCtx.materializedCTEs.size).toBe(100);
  });

  it('should handle CTE with null values', async () => {
    const cte: CTEDefinition = {
      name: 'with_nulls',
      query: 'SELECT 1',
      recursive: false,
    };

    const queryExecutor = async (): Promise<Row[]> => [
      { id: 1, value: null },
      { id: 2, value: 'test' },
      { id: 3, value: null },
    ];

    const cteCtx = createCTEContext(createMockExecutionContext());
    const materialized = await executeSimpleCTE(cte, queryExecutor, cteCtx);

    expect(materialized.rows).toHaveLength(3);
    expect(materialized.rows.filter(r => r.value === null)).toHaveLength(2);
  });

  it('should handle deeply nested recursive structure', async () => {
    // Simulate a deep tree: 1 -> 2 -> 3 -> 4 -> 5 -> ... -> 20
    const cte: CTEDefinition = {
      name: 'deep_tree',
      recursive: true,
      anchorQuery: 'SELECT 1 AS level, 1 AS id',
      recursiveQuery: 'SELECT level + 1, id + 1 FROM deep_tree WHERE level < 20',
      query: '',
    };

    const queryExecutor = async (sql: string, ctx: ExecutionContext): Promise<Row[]> => {
      const cteCtx = ctx as CTEExecutionContext;

      if (sql.includes('SELECT 1 AS level')) {
        return [{ level: 1, id: 1 }];
      }

      const tree = cteCtx.materializedCTEs?.get('deep_tree');
      if (!tree) return [];

      return tree.rows
        .filter(r => (r.level as number) < 20)
        .map(r => ({ level: (r.level as number) + 1, id: (r.id as number) + 1 }));
    };

    const cteCtx = createCTEContext(createMockExecutionContext());
    const materialized = await executeRecursiveCTE(cte, queryExecutor, cteCtx);

    expect(materialized.rows).toHaveLength(20);
    expect(Math.max(...materialized.rows.map(r => r.level as number))).toBe(20);
  });

  it('should preserve row order in CTE scan', async () => {
    const cteCtx = createCTEContext(createMockExecutionContext());
    cteCtx.materializedCTEs.set('ordered', {
      name: 'ordered',
      rows: [
        { id: 3 },
        { id: 1 },
        { id: 2 },
      ],
      columns: ['id'],
    });

    const scanOperator = new CTEScanOperator('ordered');
    const results = await collectRows(scanOperator, cteCtx);

    // Order should be preserved
    expect(results.map(r => r.id)).toEqual([3, 1, 2]);
  });
});
