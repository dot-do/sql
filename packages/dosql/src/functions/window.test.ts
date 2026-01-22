/**
 * Comprehensive Tests for SQL Window Functions
 *
 * Tests all window function categories:
 * - Ranking functions (ROW_NUMBER, RANK, DENSE_RANK, NTILE, PERCENT_RANK, CUME_DIST)
 * - Value access functions (LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE)
 * - Aggregate window functions (SUM, AVG, COUNT, MIN, MAX)
 * - Frame specifications
 * - Partitioning
 * - Edge cases
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  // Types
  type WindowSpec,
  type WindowContext,
  type WindowFrame,
  type FrameBoundary,

  // Functions
  rowNumber,
  rank,
  denseRank,
  ntile,
  percentRank,
  cumeDist,
  lagColumn,
  leadColumn,
  firstValue,
  lastValue,
  nthValue,
  windowSum,
  windowAvg,
  windowCount,
  windowMin,
  windowMax,

  // Frame calculation
  calculateFrameBoundaries,
  getDefaultFrame,

  // Function registry
  windowFunctionSignatures,
  isWindowFunction,
} from './window.js';

import {
  // Parser types and functions
  parseWindowSpec,
  parseOverClause,
  parseWindowClause,
  resolveWindowSpec,
  validateWindowSpec,
  type WindowDefinition,
} from '../parser/window.js';

import {
  // Operator
  WindowOperator,
  createWindowOperator,
  containsWindowFunction,
  extractWindowFunctions,
  type WindowPlan,
  type WindowFunctionDef,
} from '../engine/operators/window.js';

// =============================================================================
// TEST UTILITIES
// =============================================================================

/**
 * Create a simple window context for testing
 */
function createWindowContext(
  rows: Record<string, any>[],
  currentIndex: number,
  options: {
    orderBy?: { column: string; direction: 'asc' | 'desc' }[];
    frame?: WindowFrame;
    partitionBy?: string[];
  } = {}
): WindowContext {
  const spec: WindowSpec = {
    orderBy: options.orderBy,
    partitionBy: options.partitionBy,
  };

  const frame = options.frame || getDefaultFrame(!!options.orderBy);
  const orderByColumns = options.orderBy?.map(o => o.column) || [];
  const { start, end } = calculateFrameBoundaries(rows, currentIndex, frame, orderByColumns);

  return {
    partitionRows: rows,
    currentIndex,
    frameStart: start,
    frameEnd: end,
    spec,
  };
}

/**
 * Sample employee data for testing
 */
const employeeData = [
  { id: 1, name: 'Alice', department: 'Engineering', salary: 90000, hire_date: '2020-01-15' },
  { id: 2, name: 'Bob', department: 'Engineering', salary: 85000, hire_date: '2019-03-01' },
  { id: 3, name: 'Carol', department: 'Engineering', salary: 85000, hire_date: '2021-06-01' },
  { id: 4, name: 'David', department: 'Sales', salary: 75000, hire_date: '2018-11-15' },
  { id: 5, name: 'Eve', department: 'Sales', salary: 80000, hire_date: '2020-07-01' },
  { id: 6, name: 'Frank', department: 'Sales', salary: 70000, hire_date: '2022-01-10' },
];

// =============================================================================
// RANKING FUNCTION TESTS
// =============================================================================

describe('Window Functions', () => {
  describe('ROW_NUMBER()', () => {
    it('assigns sequential numbers starting from 1', () => {
      const rows = employeeData;

      for (let i = 0; i < rows.length; i++) {
        const ctx = createWindowContext(rows, i);
        expect(rowNumber(ctx)).toBe(i + 1);
      }
    });

    it('resets within each partition', () => {
      const engineering = employeeData.filter(e => e.department === 'Engineering');
      const sales = employeeData.filter(e => e.department === 'Sales');

      // Engineering partition
      for (let i = 0; i < engineering.length; i++) {
        const ctx = createWindowContext(engineering, i);
        expect(rowNumber(ctx)).toBe(i + 1);
      }

      // Sales partition
      for (let i = 0; i < sales.length; i++) {
        const ctx = createWindowContext(sales, i);
        expect(rowNumber(ctx)).toBe(i + 1);
      }
    });

    it('handles single row partition', () => {
      const ctx = createWindowContext([{ id: 1 }], 0);
      expect(rowNumber(ctx)).toBe(1);
    });

    it('handles empty partition', () => {
      const ctx = createWindowContext([], 0);
      expect(rowNumber(ctx)).toBe(1); // Still returns 1 for index 0
    });
  });

  describe('RANK()', () => {
    it('assigns same rank to equal ORDER BY values', () => {
      // Sort by salary descending
      const rows = [...employeeData].sort((a, b) => b.salary - a.salary);
      const orderBy = [{ column: 'salary', direction: 'desc' as const }];

      // First row (Alice, 90000) should have rank 1
      expect(rank(createWindowContext(rows, 0, { orderBy }))).toBe(1);

      // Bob and Carol (85000) should both have rank 2
      expect(rank(createWindowContext(rows, 1, { orderBy }))).toBe(2);
      expect(rank(createWindowContext(rows, 2, { orderBy }))).toBe(2);

      // Eve (80000) should have rank 4 (with gap)
      expect(rank(createWindowContext(rows, 3, { orderBy }))).toBe(4);
    });

    it('handles all equal values', () => {
      const rows = [{ id: 1, val: 10 }, { id: 2, val: 10 }, { id: 3, val: 10 }];
      const orderBy = [{ column: 'val', direction: 'asc' as const }];

      for (let i = 0; i < rows.length; i++) {
        expect(rank(createWindowContext(rows, i, { orderBy }))).toBe(1);
      }
    });

    it('returns 1 without ORDER BY', () => {
      for (let i = 0; i < employeeData.length; i++) {
        expect(rank(createWindowContext(employeeData, i))).toBe(1);
      }
    });
  });

  describe('DENSE_RANK()', () => {
    it('assigns rank without gaps', () => {
      const rows = [...employeeData].sort((a, b) => b.salary - a.salary);
      const orderBy = [{ column: 'salary', direction: 'desc' as const }];

      // First row (Alice, 90000) should have dense_rank 1
      expect(denseRank(createWindowContext(rows, 0, { orderBy }))).toBe(1);

      // Bob and Carol (85000) should both have dense_rank 2
      expect(denseRank(createWindowContext(rows, 1, { orderBy }))).toBe(2);
      expect(denseRank(createWindowContext(rows, 2, { orderBy }))).toBe(2);

      // Eve (80000) should have dense_rank 3 (no gap)
      expect(denseRank(createWindowContext(rows, 3, { orderBy }))).toBe(3);
    });

    it('handles all unique values', () => {
      const rows = [{ id: 1, val: 1 }, { id: 2, val: 2 }, { id: 3, val: 3 }];
      const orderBy = [{ column: 'val', direction: 'asc' as const }];

      for (let i = 0; i < rows.length; i++) {
        expect(denseRank(createWindowContext(rows, i, { orderBy }))).toBe(i + 1);
      }
    });
  });

  describe('NTILE(n)', () => {
    it('divides rows into n buckets', () => {
      const rows = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].map(id => ({ id }));

      // NTILE(4) should create buckets: [1,2,3], [4,5,6], [7,8], [9,10]
      // First 2 buckets get 3 rows, last 2 get 2 rows (10 % 4 = 2 extra rows for first 2)
      expect(ntile(createWindowContext(rows, 0), 4)).toBe(1); // row 1 -> bucket 1
      expect(ntile(createWindowContext(rows, 2), 4)).toBe(1); // row 3 -> bucket 1
      expect(ntile(createWindowContext(rows, 3), 4)).toBe(2); // row 4 -> bucket 2
      expect(ntile(createWindowContext(rows, 5), 4)).toBe(2); // row 6 -> bucket 2
      expect(ntile(createWindowContext(rows, 6), 4)).toBe(3); // row 7 -> bucket 3
      expect(ntile(createWindowContext(rows, 8), 4)).toBe(4); // row 9 -> bucket 4
    });

    it('handles n larger than row count', () => {
      const rows = [{ id: 1 }, { id: 2 }, { id: 3 }];

      // NTILE(10) with 3 rows - each row gets its own bucket
      expect(ntile(createWindowContext(rows, 0), 10)).toBe(1);
      expect(ntile(createWindowContext(rows, 1), 10)).toBe(2);
      expect(ntile(createWindowContext(rows, 2), 10)).toBe(3);
    });

    it('handles n = 1', () => {
      const rows = [{ id: 1 }, { id: 2 }, { id: 3 }];

      for (let i = 0; i < rows.length; i++) {
        expect(ntile(createWindowContext(rows, i), 1)).toBe(1);
      }
    });

    it('returns null for invalid n', () => {
      const ctx = createWindowContext([{ id: 1 }], 0);
      expect(ntile(ctx, null)).toBe(null);
      expect(ntile(ctx, 0)).toBe(null);
      expect(ntile(ctx, -1)).toBe(null);
    });
  });

  describe('PERCENT_RANK()', () => {
    it('returns relative rank as percentage', () => {
      const rows = [...employeeData].sort((a, b) => a.salary - b.salary);
      const orderBy = [{ column: 'salary', direction: 'asc' as const }];

      // First row: (1-1)/(6-1) = 0
      expect(percentRank(createWindowContext(rows, 0, { orderBy }))).toBe(0);

      // Last row: (6-1)/(6-1) = 1
      expect(percentRank(createWindowContext(rows, 5, { orderBy }))).toBe(1);
    });

    it('returns 0 for single row partition', () => {
      const ctx = createWindowContext([{ id: 1 }], 0);
      expect(percentRank(ctx)).toBe(0);
    });
  });

  describe('CUME_DIST()', () => {
    it('returns cumulative distribution', () => {
      const rows = [{ val: 1 }, { val: 2 }, { val: 3 }, { val: 4 }];
      const orderBy = [{ column: 'val', direction: 'asc' as const }];

      // cumeDist = count of rows <= current / total rows
      expect(cumeDist(createWindowContext(rows, 0, { orderBy }))).toBe(0.25); // 1/4
      expect(cumeDist(createWindowContext(rows, 1, { orderBy }))).toBe(0.5);  // 2/4
      expect(cumeDist(createWindowContext(rows, 2, { orderBy }))).toBe(0.75); // 3/4
      expect(cumeDist(createWindowContext(rows, 3, { orderBy }))).toBe(1);    // 4/4
    });
  });
});

// =============================================================================
// VALUE ACCESS FUNCTION TESTS
// =============================================================================

describe('Value Access Functions', () => {
  describe('LAG()', () => {
    it('returns value from previous row', () => {
      const rows = [{ id: 1, val: 'a' }, { id: 2, val: 'b' }, { id: 3, val: 'c' }];

      expect(lagColumn(createWindowContext(rows, 1), 'val', 1, null)).toBe('a');
      expect(lagColumn(createWindowContext(rows, 2), 'val', 1, null)).toBe('b');
    });

    it('returns default for first row', () => {
      const rows = [{ id: 1, val: 'a' }, { id: 2, val: 'b' }];

      expect(lagColumn(createWindowContext(rows, 0), 'val', 1, 'default')).toBe('default');
      expect(lagColumn(createWindowContext(rows, 0), 'val', 1, null)).toBe(null);
    });

    it('supports custom offset', () => {
      const rows = [{ id: 1, val: 'a' }, { id: 2, val: 'b' }, { id: 3, val: 'c' }, { id: 4, val: 'd' }];

      expect(lagColumn(createWindowContext(rows, 2), 'val', 2, null)).toBe('a');
      expect(lagColumn(createWindowContext(rows, 3), 'val', 3, null)).toBe('a');
      expect(lagColumn(createWindowContext(rows, 3), 'val', 4, 'none')).toBe('none');
    });
  });

  describe('LEAD()', () => {
    it('returns value from following row', () => {
      const rows = [{ id: 1, val: 'a' }, { id: 2, val: 'b' }, { id: 3, val: 'c' }];

      expect(leadColumn(createWindowContext(rows, 0), 'val', 1, null)).toBe('b');
      expect(leadColumn(createWindowContext(rows, 1), 'val', 1, null)).toBe('c');
    });

    it('returns default for last row', () => {
      const rows = [{ id: 1, val: 'a' }, { id: 2, val: 'b' }];

      expect(leadColumn(createWindowContext(rows, 1), 'val', 1, 'default')).toBe('default');
      expect(leadColumn(createWindowContext(rows, 1), 'val', 1, null)).toBe(null);
    });

    it('supports custom offset', () => {
      const rows = [{ id: 1, val: 'a' }, { id: 2, val: 'b' }, { id: 3, val: 'c' }, { id: 4, val: 'd' }];

      expect(leadColumn(createWindowContext(rows, 0), 'val', 2, null)).toBe('c');
      expect(leadColumn(createWindowContext(rows, 0), 'val', 3, null)).toBe('d');
      expect(leadColumn(createWindowContext(rows, 1), 'val', 3, 'none')).toBe('none');
    });
  });

  describe('FIRST_VALUE()', () => {
    it('returns first value in frame', () => {
      const rows = [{ id: 1, val: 'a' }, { id: 2, val: 'b' }, { id: 3, val: 'c' }];

      // Default frame with ORDER BY: UNBOUNDED PRECEDING to CURRENT ROW
      const orderBy = [{ column: 'id', direction: 'asc' as const }];

      expect(firstValue(createWindowContext(rows, 0, { orderBy }), 'val')).toBe('a');
      expect(firstValue(createWindowContext(rows, 1, { orderBy }), 'val')).toBe('a');
      expect(firstValue(createWindowContext(rows, 2, { orderBy }), 'val')).toBe('a');
    });

    it('respects frame boundaries', () => {
      const rows = [{ id: 1, val: 'a' }, { id: 2, val: 'b' }, { id: 3, val: 'c' }, { id: 4, val: 'd' }];

      // Frame: 1 PRECEDING to CURRENT ROW
      const frame: WindowFrame = {
        mode: 'rows',
        start: { type: 'preceding', offset: 1 },
        end: { type: 'currentRow' },
      };

      const orderBy = [{ column: 'id', direction: 'asc' as const }];

      expect(firstValue(createWindowContext(rows, 0, { orderBy, frame }), 'val')).toBe('a');
      expect(firstValue(createWindowContext(rows, 1, { orderBy, frame }), 'val')).toBe('a');
      expect(firstValue(createWindowContext(rows, 2, { orderBy, frame }), 'val')).toBe('b');
      expect(firstValue(createWindowContext(rows, 3, { orderBy, frame }), 'val')).toBe('c');
    });
  });

  describe('LAST_VALUE()', () => {
    it('returns last value in frame', () => {
      const rows = [{ id: 1, val: 'a' }, { id: 2, val: 'b' }, { id: 3, val: 'c' }];

      // Use frame: UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING
      const frame: WindowFrame = {
        mode: 'rows',
        start: { type: 'unboundedPreceding' },
        end: { type: 'unboundedFollowing' },
      };

      const orderBy = [{ column: 'id', direction: 'asc' as const }];

      for (let i = 0; i < rows.length; i++) {
        expect(lastValue(createWindowContext(rows, i, { orderBy, frame }), 'val')).toBe('c');
      }
    });

    it('respects frame boundaries', () => {
      const rows = [{ id: 1, val: 'a' }, { id: 2, val: 'b' }, { id: 3, val: 'c' }, { id: 4, val: 'd' }];

      // Frame: CURRENT ROW to 1 FOLLOWING
      const frame: WindowFrame = {
        mode: 'rows',
        start: { type: 'currentRow' },
        end: { type: 'following', offset: 1 },
      };

      const orderBy = [{ column: 'id', direction: 'asc' as const }];

      expect(lastValue(createWindowContext(rows, 0, { orderBy, frame }), 'val')).toBe('b');
      expect(lastValue(createWindowContext(rows, 1, { orderBy, frame }), 'val')).toBe('c');
      expect(lastValue(createWindowContext(rows, 2, { orderBy, frame }), 'val')).toBe('d');
      expect(lastValue(createWindowContext(rows, 3, { orderBy, frame }), 'val')).toBe('d');
    });
  });

  describe('NTH_VALUE()', () => {
    it('returns nth value in frame', () => {
      const rows = [{ id: 1, val: 'a' }, { id: 2, val: 'b' }, { id: 3, val: 'c' }, { id: 4, val: 'd' }];

      const frame: WindowFrame = {
        mode: 'rows',
        start: { type: 'unboundedPreceding' },
        end: { type: 'unboundedFollowing' },
      };

      const ctx = (i: number) => createWindowContext(rows, i, { frame });

      expect(nthValue(ctx(0), 'val', 1)).toBe('a');
      expect(nthValue(ctx(0), 'val', 2)).toBe('b');
      expect(nthValue(ctx(0), 'val', 3)).toBe('c');
      expect(nthValue(ctx(0), 'val', 4)).toBe('d');
    });

    it('returns null for n out of frame', () => {
      const rows = [{ id: 1, val: 'a' }, { id: 2, val: 'b' }];
      const ctx = createWindowContext(rows, 0);

      expect(nthValue(ctx, 'val', 5)).toBe(null);
    });

    it('returns null for invalid n', () => {
      const ctx = createWindowContext([{ val: 'a' }], 0);

      expect(nthValue(ctx, 'val', 0)).toBe(null);
      expect(nthValue(ctx, 'val', -1)).toBe(null);
      expect(nthValue(ctx, 'val', null)).toBe(null);
    });
  });
});

// =============================================================================
// AGGREGATE WINDOW FUNCTION TESTS
// =============================================================================

describe('Aggregate Window Functions', () => {
  const numericRows = [
    { id: 1, val: 10 },
    { id: 2, val: 20 },
    { id: 3, val: 30 },
    { id: 4, val: 40 },
    { id: 5, val: 50 },
  ];

  describe('SUM() over window', () => {
    it('calculates sum within frame', () => {
      const frame: WindowFrame = {
        mode: 'rows',
        start: { type: 'unboundedPreceding' },
        end: { type: 'currentRow' },
      };

      const orderBy = [{ column: 'id', direction: 'asc' as const }];

      expect(windowSum(createWindowContext(numericRows, 0, { orderBy, frame }), 'val')).toBe(10);
      expect(windowSum(createWindowContext(numericRows, 1, { orderBy, frame }), 'val')).toBe(30);
      expect(windowSum(createWindowContext(numericRows, 2, { orderBy, frame }), 'val')).toBe(60);
      expect(windowSum(createWindowContext(numericRows, 4, { orderBy, frame }), 'val')).toBe(150);
    });

    it('handles sliding window', () => {
      const frame: WindowFrame = {
        mode: 'rows',
        start: { type: 'preceding', offset: 1 },
        end: { type: 'following', offset: 1 },
      };

      const orderBy = [{ column: 'id', direction: 'asc' as const }];

      expect(windowSum(createWindowContext(numericRows, 0, { orderBy, frame }), 'val')).toBe(30);  // 10+20
      expect(windowSum(createWindowContext(numericRows, 1, { orderBy, frame }), 'val')).toBe(60);  // 10+20+30
      expect(windowSum(createWindowContext(numericRows, 2, { orderBy, frame }), 'val')).toBe(90);  // 20+30+40
      expect(windowSum(createWindowContext(numericRows, 4, { orderBy, frame }), 'val')).toBe(90);  // 40+50
    });

    it('handles null values', () => {
      const rows = [
        { id: 1, val: 10 },
        { id: 2, val: null },
        { id: 3, val: 30 },
      ];

      const frame: WindowFrame = {
        mode: 'rows',
        start: { type: 'unboundedPreceding' },
        end: { type: 'unboundedFollowing' },
      };

      // NULL values should be ignored
      expect(windowSum(createWindowContext(rows, 0, { frame }), 'val')).toBe(40);
    });
  });

  describe('AVG() over window', () => {
    it('calculates average within frame', () => {
      const frame: WindowFrame = {
        mode: 'rows',
        start: { type: 'unboundedPreceding' },
        end: { type: 'unboundedFollowing' },
      };

      expect(windowAvg(createWindowContext(numericRows, 2, { frame }), 'val')).toBe(30);
    });

    it('handles null values', () => {
      const rows = [
        { id: 1, val: 10 },
        { id: 2, val: null },
        { id: 3, val: 30 },
      ];

      const frame: WindowFrame = {
        mode: 'rows',
        start: { type: 'unboundedPreceding' },
        end: { type: 'unboundedFollowing' },
      };

      // AVG of 10 and 30 (null ignored) = 20
      expect(windowAvg(createWindowContext(rows, 0, { frame }), 'val')).toBe(20);
    });
  });

  describe('COUNT() over window', () => {
    it('counts all rows in frame', () => {
      const frame: WindowFrame = {
        mode: 'rows',
        start: { type: 'unboundedPreceding' },
        end: { type: 'currentRow' },
      };

      const orderBy = [{ column: 'id', direction: 'asc' as const }];

      expect(windowCount(createWindowContext(numericRows, 0, { orderBy, frame }))).toBe(1);
      expect(windowCount(createWindowContext(numericRows, 2, { orderBy, frame }))).toBe(3);
      expect(windowCount(createWindowContext(numericRows, 4, { orderBy, frame }))).toBe(5);
    });

    it('counts non-null values for column', () => {
      const rows = [
        { id: 1, val: 10 },
        { id: 2, val: null },
        { id: 3, val: 30 },
      ];

      const frame: WindowFrame = {
        mode: 'rows',
        start: { type: 'unboundedPreceding' },
        end: { type: 'unboundedFollowing' },
      };

      expect(windowCount(createWindowContext(rows, 0, { frame }), 'val')).toBe(2);
      expect(windowCount(createWindowContext(rows, 0, { frame }))).toBe(3); // COUNT(*)
    });
  });

  describe('MIN() over window', () => {
    it('finds minimum within frame', () => {
      const frame: WindowFrame = {
        mode: 'rows',
        start: { type: 'unboundedPreceding' },
        end: { type: 'currentRow' },
      };

      const orderBy = [{ column: 'id', direction: 'asc' as const }];

      expect(windowMin(createWindowContext(numericRows, 0, { orderBy, frame }), 'val')).toBe(10);
      expect(windowMin(createWindowContext(numericRows, 2, { orderBy, frame }), 'val')).toBe(10);
      expect(windowMin(createWindowContext(numericRows, 4, { orderBy, frame }), 'val')).toBe(10);
    });
  });

  describe('MAX() over window', () => {
    it('finds maximum within frame', () => {
      const frame: WindowFrame = {
        mode: 'rows',
        start: { type: 'unboundedPreceding' },
        end: { type: 'currentRow' },
      };

      const orderBy = [{ column: 'id', direction: 'asc' as const }];

      expect(windowMax(createWindowContext(numericRows, 0, { orderBy, frame }), 'val')).toBe(10);
      expect(windowMax(createWindowContext(numericRows, 2, { orderBy, frame }), 'val')).toBe(30);
      expect(windowMax(createWindowContext(numericRows, 4, { orderBy, frame }), 'val')).toBe(50);
    });
  });
});

// =============================================================================
// FRAME SPECIFICATION TESTS
// =============================================================================

describe('Frame Specifications', () => {
  describe('calculateFrameBoundaries()', () => {
    const rows = [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }];

    it('calculates ROWS UNBOUNDED PRECEDING to CURRENT ROW', () => {
      const frame: WindowFrame = {
        mode: 'rows',
        start: { type: 'unboundedPreceding' },
        end: { type: 'currentRow' },
      };

      expect(calculateFrameBoundaries(rows, 0, frame)).toEqual({ start: 0, end: 0 });
      expect(calculateFrameBoundaries(rows, 2, frame)).toEqual({ start: 0, end: 2 });
      expect(calculateFrameBoundaries(rows, 4, frame)).toEqual({ start: 0, end: 4 });
    });

    it('calculates ROWS CURRENT ROW to UNBOUNDED FOLLOWING', () => {
      const frame: WindowFrame = {
        mode: 'rows',
        start: { type: 'currentRow' },
        end: { type: 'unboundedFollowing' },
      };

      expect(calculateFrameBoundaries(rows, 0, frame)).toEqual({ start: 0, end: 4 });
      expect(calculateFrameBoundaries(rows, 2, frame)).toEqual({ start: 2, end: 4 });
      expect(calculateFrameBoundaries(rows, 4, frame)).toEqual({ start: 4, end: 4 });
    });

    it('calculates ROWS n PRECEDING to n FOLLOWING', () => {
      const frame: WindowFrame = {
        mode: 'rows',
        start: { type: 'preceding', offset: 1 },
        end: { type: 'following', offset: 1 },
      };

      expect(calculateFrameBoundaries(rows, 0, frame)).toEqual({ start: 0, end: 1 });
      expect(calculateFrameBoundaries(rows, 2, frame)).toEqual({ start: 1, end: 3 });
      expect(calculateFrameBoundaries(rows, 4, frame)).toEqual({ start: 3, end: 4 });
    });

    it('calculates ROWS BETWEEN n PRECEDING AND n PRECEDING', () => {
      const frame: WindowFrame = {
        mode: 'rows',
        start: { type: 'preceding', offset: 3 },
        end: { type: 'preceding', offset: 1 },
      };

      expect(calculateFrameBoundaries(rows, 4, frame)).toEqual({ start: 1, end: 3 });
    });

    it('calculates ROWS BETWEEN n FOLLOWING AND n FOLLOWING', () => {
      const frame: WindowFrame = {
        mode: 'rows',
        start: { type: 'following', offset: 1 },
        end: { type: 'following', offset: 3 },
      };

      expect(calculateFrameBoundaries(rows, 0, frame)).toEqual({ start: 1, end: 3 });
    });
  });

  describe('getDefaultFrame()', () => {
    it('returns RANGE UNBOUNDED PRECEDING to CURRENT ROW with ORDER BY', () => {
      const frame = getDefaultFrame(true);
      expect(frame.mode).toBe('range');
      expect(frame.start.type).toBe('unboundedPreceding');
      expect(frame.end.type).toBe('currentRow');
    });

    it('returns RANGE UNBOUNDED PRECEDING to UNBOUNDED FOLLOWING without ORDER BY', () => {
      const frame = getDefaultFrame(false);
      expect(frame.mode).toBe('range');
      expect(frame.start.type).toBe('unboundedPreceding');
      expect(frame.end.type).toBe('unboundedFollowing');
    });
  });
});

// =============================================================================
// PARSER TESTS
// =============================================================================

describe('Window Parser', () => {
  describe('parseWindowSpec()', () => {
    it('parses PARTITION BY clause', () => {
      const result = parseWindowSpec({ input: '(PARTITION BY dept)', position: 0 });
      expect(result).not.toBeNull();
      expect(result!.spec.partitionBy).toHaveLength(1);
      expect((result!.spec.partitionBy![0] as any).column).toBe('dept');
    });

    it('parses ORDER BY clause', () => {
      const result = parseWindowSpec({ input: '(ORDER BY salary DESC)', position: 0 });
      expect(result).not.toBeNull();
      expect(result!.spec.orderBy).toHaveLength(1);
      expect((result!.spec.orderBy![0].expression as any).column).toBe('salary');
      expect(result!.spec.orderBy![0].direction).toBe('desc');
    });

    it('parses PARTITION BY and ORDER BY together', () => {
      const result = parseWindowSpec({ input: '(PARTITION BY dept ORDER BY salary)', position: 0 });
      expect(result).not.toBeNull();
      expect(result!.spec.partitionBy).toHaveLength(1);
      expect(result!.spec.orderBy).toHaveLength(1);
    });

    it('parses multiple columns in PARTITION BY', () => {
      const result = parseWindowSpec({ input: '(PARTITION BY dept, location)', position: 0 });
      expect(result).not.toBeNull();
      expect(result!.spec.partitionBy).toHaveLength(2);
    });

    it('parses multiple columns in ORDER BY', () => {
      const result = parseWindowSpec({ input: '(ORDER BY salary DESC, name ASC)', position: 0 });
      expect(result).not.toBeNull();
      expect(result!.spec.orderBy).toHaveLength(2);
      expect(result!.spec.orderBy![0].direction).toBe('desc');
      expect(result!.spec.orderBy![1].direction).toBe('asc');
    });

    it('parses NULLS FIRST/LAST', () => {
      const result = parseWindowSpec({ input: '(ORDER BY salary NULLS FIRST)', position: 0 });
      expect(result).not.toBeNull();
      expect(result!.spec.orderBy![0].nulls).toBe('first');
    });

    it('parses ROWS frame specification', () => {
      const result = parseWindowSpec({
        input: '(ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)',
        position: 0
      });
      expect(result).not.toBeNull();
      expect(result!.spec.frame).toBeDefined();
      expect(result!.spec.frame!.mode).toBe('rows');
      expect(result!.spec.frame!.start.type).toBe('preceding');
      expect(result!.spec.frame!.start.offset).toBe(1);
      expect(result!.spec.frame!.end.type).toBe('following');
      expect(result!.spec.frame!.end.offset).toBe(1);
    });

    it('parses RANGE frame specification', () => {
      const result = parseWindowSpec({
        input: '(ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)',
        position: 0
      });
      expect(result).not.toBeNull();
      expect(result!.spec.frame!.mode).toBe('range');
      expect(result!.spec.frame!.start.type).toBe('unboundedPreceding');
      expect(result!.spec.frame!.end.type).toBe('currentRow');
    });

    it('parses GROUPS frame specification', () => {
      const result = parseWindowSpec({
        input: '(ORDER BY id GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)',
        position: 0
      });
      expect(result).not.toBeNull();
      expect(result!.spec.frame!.mode).toBe('groups');
    });

    it('parses empty window spec', () => {
      const result = parseWindowSpec({ input: '()', position: 0 });
      expect(result).not.toBeNull();
      expect(result!.spec.partitionBy).toBeUndefined();
      expect(result!.spec.orderBy).toBeUndefined();
    });
  });

  describe('parseWindowClause()', () => {
    it('parses WINDOW clause with single definition', () => {
      const result = parseWindowClause({
        input: 'WINDOW w AS (PARTITION BY dept ORDER BY salary)',
        position: 0
      });
      expect(result).not.toBeNull();
      expect(result!.definitions).toHaveLength(1);
      expect(result!.definitions[0].name).toBe('w');
    });

    it('parses WINDOW clause with multiple definitions', () => {
      const result = parseWindowClause({
        input: 'WINDOW w1 AS (PARTITION BY dept), w2 AS (ORDER BY salary)',
        position: 0
      });
      expect(result).not.toBeNull();
      expect(result!.definitions).toHaveLength(2);
      expect(result!.definitions[0].name).toBe('w1');
      expect(result!.definitions[1].name).toBe('w2');
    });
  });

  describe('resolveWindowSpec()', () => {
    it('resolves named window reference', () => {
      const namedWindows = new Map<string, WindowSpec>([
        ['w', { partitionBy: [{ type: 'column', column: 'dept' }] }],
      ]);

      const resolved = resolveWindowSpec('w', namedWindows);
      expect(resolved.partitionBy).toHaveLength(1);
    });

    it('merges base window with inline spec', () => {
      const namedWindows = new Map<string, WindowSpec>([
        ['base', { partitionBy: [{ type: 'column', column: 'dept' }] }],
      ]);

      const spec: WindowSpec = {
        baseName: 'base',
        orderBy: [{ expression: { type: 'column', column: 'salary' }, direction: 'desc' }],
      };

      const resolved = resolveWindowSpec(spec, namedWindows);
      expect(resolved.partitionBy).toHaveLength(1);
      expect(resolved.orderBy).toHaveLength(1);
    });

    it('throws for unknown window name', () => {
      const namedWindows = new Map<string, WindowSpec>();

      expect(() => resolveWindowSpec('unknown', namedWindows)).toThrow('Unknown window name: unknown');
    });
  });

  describe('validateWindowSpec()', () => {
    it('accepts valid frame specification', () => {
      const spec: WindowSpec = {
        frame: {
          mode: 'rows',
          start: { type: 'unboundedPreceding' },
          end: { type: 'currentRow' },
        },
      };

      expect(() => validateWindowSpec(spec)).not.toThrow();
    });

    it('rejects UNBOUNDED FOLLOWING as start', () => {
      const spec: WindowSpec = {
        frame: {
          mode: 'rows',
          start: { type: 'unboundedFollowing' },
          end: { type: 'currentRow' },
        },
      };

      expect(() => validateWindowSpec(spec)).toThrow('UNBOUNDED FOLLOWING cannot be used as frame start');
    });

    it('rejects UNBOUNDED PRECEDING as end', () => {
      const spec: WindowSpec = {
        frame: {
          mode: 'rows',
          start: { type: 'currentRow' },
          end: { type: 'unboundedPreceding' },
        },
      };

      expect(() => validateWindowSpec(spec)).toThrow('UNBOUNDED PRECEDING cannot be used as frame end');
    });
  });
});

// =============================================================================
// FUNCTION REGISTRY TESTS
// =============================================================================

describe('Window Function Registry', () => {
  describe('windowFunctionSignatures', () => {
    it('includes all ranking functions', () => {
      expect(windowFunctionSignatures.row_number).toBeDefined();
      expect(windowFunctionSignatures.rank).toBeDefined();
      expect(windowFunctionSignatures.dense_rank).toBeDefined();
      expect(windowFunctionSignatures.ntile).toBeDefined();
      expect(windowFunctionSignatures.percent_rank).toBeDefined();
      expect(windowFunctionSignatures.cume_dist).toBeDefined();
    });

    it('includes all value access functions', () => {
      expect(windowFunctionSignatures.lag).toBeDefined();
      expect(windowFunctionSignatures.lead).toBeDefined();
      expect(windowFunctionSignatures.first_value).toBeDefined();
      expect(windowFunctionSignatures.last_value).toBeDefined();
      expect(windowFunctionSignatures.nth_value).toBeDefined();
    });

    it('has correct parameter definitions', () => {
      const lagSig = windowFunctionSignatures.lag;
      expect(lagSig.params).toHaveLength(3);
      expect(lagSig.params[0].name).toBe('expr');
      expect(lagSig.params[1].optional).toBe(true);
      expect(lagSig.params[2].optional).toBe(true);
    });
  });

  describe('isWindowFunction()', () => {
    it('identifies window functions', () => {
      expect(isWindowFunction('row_number')).toBe(true);
      expect(isWindowFunction('ROW_NUMBER')).toBe(true);
      expect(isWindowFunction('rank')).toBe(true);
      expect(isWindowFunction('lag')).toBe(true);
    });

    it('returns false for non-window functions', () => {
      expect(isWindowFunction('sum')).toBe(false);
      expect(isWindowFunction('upper')).toBe(false);
      expect(isWindowFunction('unknown')).toBe(false);
    });
  });
});

// =============================================================================
// EDGE CASE TESTS
// =============================================================================

describe('Edge Cases', () => {
  describe('Empty partitions', () => {
    it('handles empty input gracefully', () => {
      const ctx = createWindowContext([], 0);
      // Functions should not crash on empty input
      expect(() => rowNumber(ctx)).not.toThrow();
      expect(() => rank(ctx)).not.toThrow();
    });
  });

  describe('Single row partitions', () => {
    it('handles single row for all functions', () => {
      const rows = [{ id: 1, val: 100 }];
      const ctx = createWindowContext(rows, 0);

      expect(rowNumber(ctx)).toBe(1);
      expect(rank(ctx)).toBe(1);
      expect(denseRank(ctx)).toBe(1);
      expect(ntile(ctx, 4)).toBe(1);
      expect(percentRank(ctx)).toBe(0);
      expect(windowSum(ctx, 'val')).toBe(100);
      expect(windowAvg(ctx, 'val')).toBe(100);
      expect(windowCount(ctx)).toBe(1);
      expect(windowMin(ctx, 'val')).toBe(100);
      expect(windowMax(ctx, 'val')).toBe(100);
    });
  });

  describe('All NULL values', () => {
    it('handles all NULL values in aggregates', () => {
      const rows = [{ val: null }, { val: null }, { val: null }];

      const frame: WindowFrame = {
        mode: 'rows',
        start: { type: 'unboundedPreceding' },
        end: { type: 'unboundedFollowing' },
      };

      const ctx = createWindowContext(rows, 0, { frame });

      expect(windowSum(ctx, 'val')).toBe(null);
      expect(windowAvg(ctx, 'val')).toBe(null);
      expect(windowMin(ctx, 'val')).toBe(null);
      expect(windowMax(ctx, 'val')).toBe(null);
      expect(windowCount(ctx, 'val')).toBe(0);
      expect(windowCount(ctx)).toBe(3); // COUNT(*) counts all rows
    });
  });

  describe('Large offsets', () => {
    it('handles LAG with offset larger than partition', () => {
      const rows = [{ val: 'a' }, { val: 'b' }];
      const ctx = createWindowContext(rows, 1);

      expect(lagColumn(ctx, 'val', 10, 'default')).toBe('default');
    });

    it('handles LEAD with offset larger than partition', () => {
      const rows = [{ val: 'a' }, { val: 'b' }];
      const ctx = createWindowContext(rows, 0);

      expect(leadColumn(ctx, 'val', 10, 'default')).toBe('default');
    });

    it('handles NTH_VALUE with n larger than frame', () => {
      const rows = [{ val: 'a' }, { val: 'b' }];
      const ctx = createWindowContext(rows, 0);

      expect(nthValue(ctx, 'val', 100)).toBe(null);
    });
  });

  describe('Mixed data types', () => {
    it('handles mixed numeric types', () => {
      const rows = [{ val: 1 }, { val: 2.5 }, { val: 3 }];

      const frame: WindowFrame = {
        mode: 'rows',
        start: { type: 'unboundedPreceding' },
        end: { type: 'unboundedFollowing' },
      };

      const ctx = createWindowContext(rows, 0, { frame });

      expect(windowSum(ctx, 'val')).toBe(6.5);
    });
  });
});
