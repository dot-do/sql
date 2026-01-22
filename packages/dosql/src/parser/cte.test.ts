/**
 * CTE (Common Table Expression) Parser Tests
 *
 * Tests for parsing and executing CTEs including:
 * - Simple CTEs
 * - Multiple CTEs
 * - Recursive CTEs (tree traversal, Fibonacci, etc.)
 * - CTEs in subqueries
 * - Column aliasing
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  parseCTE,
  hasWithClause,
  extractWithClause,
  getCTENames,
  findCTE,
  validateCTEReferences,
  expandCTEsInline,
} from './cte.js';
import {
  createCTE,
  createRecursiveCTE,
  createWithClause,
  createCTEScope,
  isRecursiveCTE,
  hasRecursiveCTE,
  isCTEParseSuccess,
  isCTEParseError,
} from './cte-types.js';
import {
  executeWithClause,
  executeSimpleCTE,
  executeRecursiveCTE,
  createCTEContext,
  CTEScanOperator,
  getCTERows,
  isCTEMaterialized,
  createMockQueryExecutor,
  type CTEExecutionContext,
  type MaterializedCTE,
} from '../engine/operators/cte.js';
import type { ExecutionContext, Row } from '../engine/types.js';

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Create a minimal execution context for testing
 */
function createTestContext(): ExecutionContext {
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

// =============================================================================
// SIMPLE CTE PARSING TESTS
// =============================================================================

describe('CTE Parser - Simple CTEs', () => {
  it('should parse a simple CTE', () => {
    const sql = `
      WITH active_users AS (
        SELECT id, name FROM users WHERE active = 1
      )
      SELECT * FROM active_users
    `;

    const result = parseCTE(sql);
    expect(result.success).toBe(true);

    if (isCTEParseSuccess(result)) {
      expect(result.with.ctes.length).toBe(1);
      expect(result.with.ctes[0].name).toBe('active_users');
      expect(result.with.ctes[0].recursive).toBe(false);
      expect(result.mainQuery).toContain('SELECT * FROM active_users');
    }
  });

  it('should parse CTE with column aliases', () => {
    const sql = `
      WITH user_stats(user_id, total_orders, avg_amount) AS (
        SELECT user_id, COUNT(*), AVG(amount) FROM orders GROUP BY user_id
      )
      SELECT * FROM user_stats WHERE total_orders > 10
    `;

    const result = parseCTE(sql);
    expect(result.success).toBe(true);

    if (isCTEParseSuccess(result)) {
      expect(result.with.ctes[0].columns).toEqual(['user_id', 'total_orders', 'avg_amount']);
    }
  });

  it('should parse multiple CTEs', () => {
    const sql = `
      WITH
        dept_employees AS (
          SELECT * FROM employees WHERE dept_id = 5
        ),
        managers AS (
          SELECT * FROM employees WHERE is_manager = 1
        ),
        high_performers AS (
          SELECT * FROM employees WHERE performance_score > 4
        )
      SELECT * FROM dept_employees
      WHERE id IN (SELECT id FROM managers)
      AND id IN (SELECT id FROM high_performers)
    `;

    const result = parseCTE(sql);
    expect(result.success).toBe(true);

    if (isCTEParseSuccess(result)) {
      expect(result.with.ctes.length).toBe(3);
      expect(getCTENames(result.with)).toEqual(['dept_employees', 'managers', 'high_performers']);
    }
  });

  it('should handle nested parentheses in CTE query', () => {
    const sql = `
      WITH complex_cte AS (
        SELECT id, (SELECT COUNT(*) FROM orders WHERE user_id = u.id) as order_count
        FROM users u
        WHERE EXISTS (SELECT 1 FROM subscriptions WHERE user_id = u.id)
      )
      SELECT * FROM complex_cte
    `;

    const result = parseCTE(sql);
    expect(result.success).toBe(true);

    if (isCTEParseSuccess(result)) {
      expect(result.with.ctes[0].query).toContain('SELECT id');
      expect(result.with.ctes[0].query).toContain('SELECT COUNT(*)');
    }
  });

  it('should handle string literals with parentheses', () => {
    const sql = `
      WITH messages AS (
        SELECT id, 'Hello (World)' as greeting FROM users
      )
      SELECT * FROM messages
    `;

    const result = parseCTE(sql);
    expect(result.success).toBe(true);

    if (isCTEParseSuccess(result)) {
      expect(result.with.ctes[0].query).toContain("'Hello (World)'");
    }
  });
});

// =============================================================================
// RECURSIVE CTE PARSING TESTS
// =============================================================================

describe('CTE Parser - Recursive CTEs', () => {
  it('should parse recursive CTE with RECURSIVE keyword', () => {
    const sql = `
      WITH RECURSIVE ancestors AS (
        SELECT id, parent_id, name FROM employees WHERE id = 5
        UNION ALL
        SELECT e.id, e.parent_id, e.name FROM employees e
        JOIN ancestors a ON e.id = a.parent_id
      )
      SELECT * FROM ancestors
    `;

    const result = parseCTE(sql);
    expect(result.success).toBe(true);

    if (isCTEParseSuccess(result)) {
      expect(result.with.recursive).toBe(true);
      expect(result.with.ctes[0].recursive).toBe(true);
      expect(result.with.ctes[0].anchorQuery).toBeDefined();
      expect(result.with.ctes[0].recursiveQuery).toBeDefined();
    }
  });

  it('should identify non-self-referencing CTE as non-recursive even with RECURSIVE keyword', () => {
    const sql = `
      WITH RECURSIVE simple AS (
        SELECT 1 as num
      )
      SELECT * FROM simple
    `;

    const result = parseCTE(sql);
    expect(result.success).toBe(true);

    if (isCTEParseSuccess(result)) {
      // The WITH clause has RECURSIVE, but this CTE doesn't self-reference
      expect(result.with.recursive).toBe(true);
      expect(result.with.ctes[0].recursive).toBe(false);
    }
  });

  it('should parse recursive CTE for tree traversal', () => {
    const sql = `
      WITH RECURSIVE org_chart(id, manager_id, name, level) AS (
        -- Anchor: Start with the CEO (no manager)
        SELECT id, manager_id, name, 0 FROM employees WHERE manager_id IS NULL
        UNION ALL
        -- Recursive: Join employees to their managers
        SELECT e.id, e.manager_id, e.name, oc.level + 1
        FROM employees e
        JOIN org_chart oc ON e.manager_id = oc.id
      )
      SELECT * FROM org_chart ORDER BY level
    `;

    const result = parseCTE(sql);
    expect(result.success).toBe(true);

    if (isCTEParseSuccess(result)) {
      expect(result.with.ctes[0].columns).toEqual(['id', 'manager_id', 'name', 'level']);
      expect(result.with.ctes[0].recursive).toBe(true);
    }
  });

  it('should parse Fibonacci sequence recursive CTE', () => {
    const sql = `
      WITH RECURSIVE fibonacci(n, fib_n, fib_n1) AS (
        SELECT 1, 0, 1
        UNION ALL
        SELECT n + 1, fib_n1, fib_n + fib_n1
        FROM fibonacci
        WHERE n < 10
      )
      SELECT n, fib_n FROM fibonacci
    `;

    const result = parseCTE(sql);
    expect(result.success).toBe(true);

    if (isCTEParseSuccess(result)) {
      expect(result.with.ctes[0].name).toBe('fibonacci');
      expect(result.with.ctes[0].recursive).toBe(true);
    }
  });

  it('should parse recursive CTE for graph traversal', () => {
    const sql = `
      WITH RECURSIVE reachable(node_id, path, distance) AS (
        -- Start from source node
        SELECT id, ARRAY[id], 0
        FROM nodes
        WHERE id = 'A'
        UNION ALL
        -- Follow edges
        SELECT e.target_id, r.path || e.target_id, r.distance + e.weight
        FROM reachable r
        JOIN edges e ON r.node_id = e.source_id
        WHERE NOT e.target_id = ANY(r.path)  -- Prevent cycles
      )
      SELECT * FROM reachable WHERE node_id = 'Z'
    `;

    const result = parseCTE(sql);
    expect(result.success).toBe(true);

    if (isCTEParseSuccess(result)) {
      expect(result.with.ctes[0].recursive).toBe(true);
    }
  });
});

// =============================================================================
// CTE UTILITY FUNCTION TESTS
// =============================================================================

describe('CTE Utility Functions', () => {
  it('hasWithClause should detect WITH clauses', () => {
    expect(hasWithClause('WITH x AS (SELECT 1) SELECT * FROM x')).toBe(true);
    expect(hasWithClause('SELECT * FROM users')).toBe(false);
    expect(hasWithClause('  WITH x AS (SELECT 1) SELECT * FROM x')).toBe(true);
    expect(hasWithClause('WITHOUT')).toBe(false);
  });

  it('extractWithClause should separate WITH clause from main query', () => {
    const sql = 'WITH x AS (SELECT 1 as n) SELECT * FROM x';
    const { withClause, mainQuery } = extractWithClause(sql);

    expect(withClause).not.toBeNull();
    expect(withClause?.ctes.length).toBe(1);
    expect(mainQuery).toBe('SELECT * FROM x');
  });

  it('extractWithClause should return original SQL for non-CTE queries', () => {
    const sql = 'SELECT * FROM users';
    const { withClause, mainQuery } = extractWithClause(sql);

    expect(withClause).toBeNull();
    expect(mainQuery).toBe(sql);
  });

  it('findCTE should find CTE by name (case insensitive)', () => {
    const result = parseCTE(`
      WITH users_cte AS (SELECT * FROM users),
           orders_cte AS (SELECT * FROM orders)
      SELECT * FROM users_cte
    `);

    if (isCTEParseSuccess(result)) {
      const found = findCTE(result.with, 'USERS_CTE');
      expect(found).toBeDefined();
      expect(found?.name).toBe('users_cte');

      const notFound = findCTE(result.with, 'nonexistent');
      expect(notFound).toBeUndefined();
    }
  });

  it('validateCTEReferences should detect self-reference without RECURSIVE', () => {
    // Create a CTE that references itself but wasn't marked as recursive
    const cte = createCTE('self_ref', 'SELECT * FROM self_ref WHERE x = 1');
    const withClause = createWithClause([cte], false);

    const validation = validateCTEReferences(withClause);
    expect(validation.valid).toBe(false);
    expect(validation.errors.length).toBeGreaterThan(0);
  });

  it('expandCTEsInline should expand simple CTEs', () => {
    const sql = 'WITH x AS (SELECT 1 as n) SELECT * FROM x';
    const expanded = expandCTEsInline(sql);

    expect(expanded).toContain('(SELECT 1 as n)');
    expect(expanded).not.toContain('WITH');
  });
});

// =============================================================================
// CTE TYPE FUNCTIONS TESTS
// =============================================================================

describe('CTE Type Functions', () => {
  it('createCTE should create non-recursive CTE', () => {
    const cte = createCTE('my_cte', 'SELECT * FROM users', ['id', 'name']);

    expect(cte.name).toBe('my_cte');
    expect(cte.query).toBe('SELECT * FROM users');
    expect(cte.columns).toEqual(['id', 'name']);
    expect(cte.recursive).toBe(false);
  });

  it('createRecursiveCTE should create recursive CTE', () => {
    const cte = createRecursiveCTE(
      'tree',
      'SELECT id, parent_id FROM nodes WHERE parent_id IS NULL',
      'SELECT n.id, n.parent_id FROM nodes n JOIN tree t ON n.parent_id = t.id',
      ['id', 'parent_id']
    );

    expect(cte.name).toBe('tree');
    expect(cte.recursive).toBe(true);
    expect(cte.anchorQuery).toBeDefined();
    expect(cte.recursiveQuery).toBeDefined();
  });

  it('isRecursiveCTE should identify recursive CTEs', () => {
    const simpleCTE = createCTE('simple', 'SELECT 1');
    const recursiveCTE = createRecursiveCTE('rec', 'SELECT 1', 'SELECT * FROM rec');

    expect(isRecursiveCTE(simpleCTE)).toBe(false);
    expect(isRecursiveCTE(recursiveCTE)).toBe(true);
  });

  it('hasRecursiveCTE should check WITH clause for any recursive CTEs', () => {
    const nonRecursiveWith = createWithClause([
      createCTE('a', 'SELECT 1'),
      createCTE('b', 'SELECT 2'),
    ]);

    const recursiveWith = createWithClause([
      createCTE('a', 'SELECT 1'),
      createRecursiveCTE('b', 'SELECT 1', 'SELECT * FROM b'),
    ]);

    expect(hasRecursiveCTE(nonRecursiveWith)).toBe(false);
    expect(hasRecursiveCTE(recursiveWith)).toBe(true);
  });

  it('createCTEScope should create lowercase name map', () => {
    const withClause = createWithClause([
      createCTE('MyTable', 'SELECT 1'),
      createCTE('OTHER_TABLE', 'SELECT 2'),
    ]);

    const scope = createCTEScope(withClause);

    expect(scope.has('mytable')).toBe(true);
    expect(scope.has('other_table')).toBe(true);
    expect(scope.has('MyTable')).toBe(false); // Case sensitive lookup
  });
});

// =============================================================================
// CTE EXECUTION TESTS
// =============================================================================

describe('CTE Execution - Simple CTEs', () => {
  let baseCtx: ExecutionContext;

  beforeEach(() => {
    baseCtx = createTestContext();
  });

  it('should execute simple CTE and materialize results', async () => {
    const cte = createCTE('numbers', 'SELECT * FROM source_table');

    const mockRows = [
      { n: 1 }, { n: 2 }, { n: 3 },
    ];

    const mockExecutor = async (sql: string): Promise<Row[]> => {
      if (sql.includes('source_table')) {
        return mockRows;
      }
      return [];
    };

    const ctx = createCTEContext(baseCtx);
    const materialized = await executeSimpleCTE(cte, mockExecutor, ctx);

    expect(materialized.name).toBe('numbers');
    expect(materialized.rows).toEqual(mockRows);
  });

  it('should apply column aliases when materializing', async () => {
    const cte = createCTE('renamed', 'SELECT * FROM source', ['value', 'label']);

    const mockExecutor = async (): Promise<Row[]> => [
      { col1: 1, col2: 'a' },
      { col1: 2, col2: 'b' },
    ];

    const ctx = createCTEContext(baseCtx);
    const materialized = await executeSimpleCTE(cte, mockExecutor, ctx);

    expect(materialized.columns).toEqual(['value', 'label']);
    expect(materialized.rows[0]).toEqual({ value: 1, label: 'a' });
    expect(materialized.rows[1]).toEqual({ value: 2, label: 'b' });
  });

  it('should throw error for column count mismatch', async () => {
    const cte = createCTE('mismatched', 'SELECT * FROM source', ['a', 'b', 'c']);

    const mockExecutor = async (): Promise<Row[]> => [
      { x: 1, y: 2 }, // Only 2 columns, but CTE expects 3
    ];

    const ctx = createCTEContext(baseCtx);

    await expect(executeSimpleCTE(cte, mockExecutor, ctx)).rejects.toThrow(
      /column count mismatch/
    );
  });
});

describe('CTE Execution - Recursive CTEs', () => {
  let baseCtx: ExecutionContext;

  beforeEach(() => {
    baseCtx = createTestContext();
  });

  it('should execute recursive CTE for counting', async () => {
    // Generate numbers 1-5 recursively
    const cte = createRecursiveCTE(
      'counter',
      'SELECT 1 as n',
      'SELECT n + 1 FROM counter WHERE n < 5',
      ['n']
    );

    let iteration = 0;
    const mockExecutor = async (sql: string, ctx: ExecutionContext): Promise<Row[]> => {
      const cteCtx = ctx as CTEExecutionContext;

      if (sql.includes('SELECT 1')) {
        // Anchor query
        return [{ n: 1 }];
      }

      // Recursive query - get current working table and add 1
      const currentRows = cteCtx.materializedCTEs.get('counter')?.rows || [];
      const newRows: Row[] = [];

      for (const row of currentRows) {
        const n = row.n as number;
        if (n < 5) {
          newRows.push({ n: n + 1 });
        }
      }

      return newRows;
    };

    const ctx = createCTEContext(baseCtx);
    const materialized = await executeRecursiveCTE(cte, mockExecutor, ctx);

    expect(materialized.rows.length).toBe(5);
    expect(materialized.rows.map(r => r.n)).toEqual([1, 2, 3, 4, 5]);
  });

  it('should stop at fixed point (no new rows)', async () => {
    const cte = createRecursiveCTE(
      'limited',
      'SELECT 1 as n',
      'SELECT n FROM limited WHERE 1 = 0', // Always returns empty
      ['n']
    );

    const mockExecutor = async (sql: string): Promise<Row[]> => {
      if (sql.includes('SELECT 1')) {
        return [{ n: 1 }];
      }
      return []; // Recursive part returns nothing
    };

    const ctx = createCTEContext(baseCtx);
    const materialized = await executeRecursiveCTE(cte, mockExecutor, ctx);

    expect(materialized.rows.length).toBe(1);
    expect(materialized.rows[0].n).toBe(1);
  });

  it('should enforce iteration limit', async () => {
    const cte = createRecursiveCTE(
      'infinite',
      'SELECT 1 as n',
      'SELECT n + 1 FROM infinite', // No stopping condition
      ['n']
    );

    const mockExecutor = async (sql: string, ctx: ExecutionContext): Promise<Row[]> => {
      const cteCtx = ctx as CTEExecutionContext;

      if (sql.includes('SELECT 1')) {
        return [{ n: 1 }];
      }

      const currentRows = cteCtx.materializedCTEs.get('infinite')?.rows || [];
      return currentRows.map(row => ({ n: (row.n as number) + 1 }));
    };

    const ctx = createCTEContext(baseCtx, { maxIterations: 10 });

    await expect(executeRecursiveCTE(cte, mockExecutor, ctx)).rejects.toThrow(
      /exceeded maximum iterations/
    );
  });

  it('should enforce row limit', async () => {
    const cte = createRecursiveCTE(
      'growing',
      'SELECT 1 as n',
      'SELECT n + 1 FROM growing',
      ['n']
    );

    const mockExecutor = async (sql: string, ctx: ExecutionContext): Promise<Row[]> => {
      const cteCtx = ctx as CTEExecutionContext;

      if (sql.includes('SELECT 1')) {
        return [{ n: 1 }];
      }

      const currentRows = cteCtx.materializedCTEs.get('growing')?.rows || [];
      return currentRows.map(row => ({ n: (row.n as number) + 1 }));
    };

    const ctx = createCTEContext(baseCtx, { maxRows: 5, maxIterations: 100 });

    await expect(executeRecursiveCTE(cte, mockExecutor, ctx)).rejects.toThrow(
      /exceeded maximum row limit/
    );
  });

  it('should deduplicate rows in recursive CTE', async () => {
    const cte = createRecursiveCTE(
      'dedup',
      'SELECT 1 as n',
      'SELECT 1 as n FROM dedup', // Always returns same value
      ['n']
    );

    let anchorCalled = false;
    const mockExecutor = async (sql: string): Promise<Row[]> => {
      if (sql.includes('SELECT 1 as n') && !anchorCalled) {
        anchorCalled = true;
        return [{ n: 1 }];
      }
      return [{ n: 1 }]; // Recursive always returns { n: 1 }
    };

    const ctx = createCTEContext(baseCtx);
    const materialized = await executeRecursiveCTE(cte, mockExecutor, ctx);

    // Should have only 1 row because duplicates are filtered
    expect(materialized.rows.length).toBe(1);
  });
});

describe('CTE Execution - WITH Clause', () => {
  let baseCtx: ExecutionContext;

  beforeEach(() => {
    baseCtx = createTestContext();
  });

  it('should execute multiple CTEs in order', async () => {
    const withClause = createWithClause([
      createCTE('first', 'SELECT 1 as n'),
      createCTE('second', 'SELECT * FROM first'),
    ]);

    const executionOrder: string[] = [];

    const mockExecutor = async (sql: string, ctx: ExecutionContext): Promise<Row[]> => {
      const cteCtx = ctx as CTEExecutionContext;

      if (sql.includes('SELECT 1')) {
        executionOrder.push('first');
        return [{ n: 1 }];
      }
      if (sql.includes('FROM first')) {
        executionOrder.push('second');
        // second CTE references first, which should be materialized
        expect(cteCtx.materializedCTEs.has('first')).toBe(true);
        return cteCtx.materializedCTEs.get('first')!.rows;
      }
      return [];
    };

    const ctx = await executeWithClause(withClause, mockExecutor, baseCtx);

    expect(executionOrder).toEqual(['first', 'second']);
    expect(ctx.materializedCTEs.size).toBe(2);
    expect(getCTERows(ctx, 'first')).toEqual([{ n: 1 }]);
    expect(getCTERows(ctx, 'second')).toEqual([{ n: 1 }]);
  });

  it('should support CTE referencing previous CTE', async () => {
    const withClause = createWithClause([
      createCTE('numbers', 'SELECT 1 as n UNION ALL SELECT 2 UNION ALL SELECT 3'),
      createCTE('doubled', 'SELECT n * 2 as n FROM numbers'),
    ]);

    // Custom executor that handles both CTEs
    const enhancedExecutor = async (sql: string, ctx: ExecutionContext): Promise<Row[]> => {
      // First CTE: SELECT 1 as n UNION ALL SELECT 2 UNION ALL SELECT 3
      if (sql.includes('SELECT 1') && sql.includes('UNION ALL')) {
        return [{ n: 1 }, { n: 2 }, { n: 3 }];
      }
      // Second CTE: references 'numbers' CTE
      if (sql.includes('n * 2')) {
        const cteCtx = ctx as CTEExecutionContext;
        const numberRows = cteCtx.materializedCTEs.get('numbers')?.rows || [];
        return numberRows.map(r => ({ n: (r.n as number) * 2 }));
      }
      return [];
    };

    const ctx = await executeWithClause(withClause, enhancedExecutor, baseCtx);

    expect(getCTERows(ctx, 'doubled')).toEqual([{ n: 2 }, { n: 4 }, { n: 6 }]);
  });
});

describe('CTEScanOperator', () => {
  let cteCtx: CTEExecutionContext;

  beforeEach(() => {
    const baseCtx = createTestContext();
    cteCtx = createCTEContext(baseCtx);

    // Pre-materialize a CTE
    cteCtx.materializedCTEs.set('test_cte', {
      name: 'test_cte',
      rows: [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' },
        { id: 3, name: 'Charlie' },
      ],
      columns: ['id', 'name'],
    });
  });

  it('should scan all rows from materialized CTE', async () => {
    const operator = new CTEScanOperator('test_cte');
    await operator.open(cteCtx);

    const rows: Row[] = [];
    let row: Row | null;
    while ((row = await operator.next()) !== null) {
      rows.push(row);
    }

    await operator.close();

    expect(rows.length).toBe(3);
    expect(rows[0].name).toBe('Alice');
    expect(rows[2].name).toBe('Charlie');
  });

  it('should return correct columns', async () => {
    const operator = new CTEScanOperator('test_cte');
    await operator.open(cteCtx);

    expect(operator.columns()).toEqual(['id', 'name']);

    await operator.close();
  });

  it('should throw error for non-existent CTE', async () => {
    const operator = new CTEScanOperator('nonexistent');

    await expect(operator.open(cteCtx)).rejects.toThrow(/not materialized/);
  });

  it('should apply alias to rows', async () => {
    const operator = new CTEScanOperator('test_cte', 'aliased');
    await operator.open(cteCtx);

    const row = await operator.next();
    expect(row).not.toBeNull();

    // Should have both plain column and alias-prefixed column
    expect(row!.id).toBe(1);
    expect(row!['aliased.id']).toBe(1);

    await operator.close();
  });
});

// =============================================================================
// EDGE CASES AND ERROR HANDLING
// =============================================================================

describe('CTE Parser - Edge Cases', () => {
  it('should handle empty WITH clause query', () => {
    const sql = 'SELECT * FROM users';
    const result = parseCTE(sql);
    expect(result.success).toBe(false);
    expect(isCTEParseError(result)).toBe(true);
  });

  it('should handle malformed CTE (missing AS)', () => {
    const sql = 'WITH bad_cte (SELECT 1) SELECT * FROM bad_cte';
    const result = parseCTE(sql);
    expect(result.success).toBe(false);
  });

  it('should handle malformed CTE (missing parentheses)', () => {
    const sql = 'WITH bad_cte AS SELECT 1 SELECT * FROM bad_cte';
    const result = parseCTE(sql);
    expect(result.success).toBe(false);
  });

  it('should handle quoted identifiers', () => {
    const sql = `
      WITH "My CTE" AS (SELECT 1 as n)
      SELECT * FROM "My CTE"
    `;

    const result = parseCTE(sql);
    expect(result.success).toBe(true);

    if (isCTEParseSuccess(result)) {
      expect(result.with.ctes[0].name).toBe('My CTE');
    }
  });

  it('should handle SQL comments in CTE', () => {
    const sql = `
      -- This is a comment
      WITH /* inline comment */ commented AS (
        SELECT 1 as n -- trailing comment
      )
      SELECT * FROM commented
    `;

    const result = parseCTE(sql);
    expect(result.success).toBe(true);
  });

  it('should handle CTE with UNION in query', () => {
    const sql = `
      WITH combined AS (
        SELECT id, name FROM table1
        UNION ALL
        SELECT id, name FROM table2
      )
      SELECT * FROM combined
    `;

    const result = parseCTE(sql);
    expect(result.success).toBe(true);

    if (isCTEParseSuccess(result)) {
      // Non-recursive because it doesn't reference itself
      expect(result.with.ctes[0].recursive).toBe(false);
    }
  });
});

// =============================================================================
// REAL-WORLD SCENARIO TESTS
// =============================================================================

describe('CTE - Real-World Scenarios', () => {
  it('should parse organizational hierarchy query', () => {
    const sql = `
      WITH RECURSIVE org_hierarchy AS (
        -- Start with top-level employees (no manager)
        SELECT
          id,
          name,
          manager_id,
          1 as level,
          name as path
        FROM employees
        WHERE manager_id IS NULL

        UNION ALL

        -- Join with direct reports
        SELECT
          e.id,
          e.name,
          e.manager_id,
          oh.level + 1,
          oh.path || ' > ' || e.name
        FROM employees e
        INNER JOIN org_hierarchy oh ON e.manager_id = oh.id
      )
      SELECT * FROM org_hierarchy ORDER BY path
    `;

    const result = parseCTE(sql);
    expect(result.success).toBe(true);

    if (isCTEParseSuccess(result)) {
      expect(result.with.ctes[0].recursive).toBe(true);
      expect(result.with.ctes[0].anchorQuery).toContain('manager_id IS NULL');
      expect(result.with.ctes[0].recursiveQuery).toContain('INNER JOIN org_hierarchy');
    }
  });

  it('should parse bill of materials query', () => {
    const sql = `
      WITH RECURSIVE bom(part_id, component_id, quantity, level) AS (
        -- Base parts (leaf nodes)
        SELECT part_id, component_id, quantity, 1
        FROM parts_structure
        WHERE part_id = 'PRODUCT-001'

        UNION ALL

        -- Recursively get sub-components
        SELECT ps.part_id, ps.component_id, ps.quantity * b.quantity, b.level + 1
        FROM parts_structure ps
        JOIN bom b ON ps.part_id = b.component_id
        WHERE b.level < 10
      )
      SELECT
        component_id,
        SUM(quantity) as total_quantity
      FROM bom
      GROUP BY component_id
    `;

    const result = parseCTE(sql);
    expect(result.success).toBe(true);

    if (isCTEParseSuccess(result)) {
      expect(result.with.ctes[0].name).toBe('bom');
      expect(result.with.ctes[0].columns).toEqual(['part_id', 'component_id', 'quantity', 'level']);
    }
  });

  it('should parse cumulative aggregation query', () => {
    const sql = `
      WITH monthly_sales AS (
        SELECT
          DATE_TRUNC('month', sale_date) as month,
          SUM(amount) as total
        FROM sales
        GROUP BY DATE_TRUNC('month', sale_date)
      ),
      cumulative AS (
        SELECT
          month,
          total,
          SUM(total) OVER (ORDER BY month) as running_total
        FROM monthly_sales
      )
      SELECT * FROM cumulative ORDER BY month
    `;

    const result = parseCTE(sql);
    expect(result.success).toBe(true);

    if (isCTEParseSuccess(result)) {
      expect(result.with.ctes.length).toBe(2);
      expect(getCTENames(result.with)).toEqual(['monthly_sales', 'cumulative']);
    }
  });
});
