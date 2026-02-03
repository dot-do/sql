/**
 * Backpressure Tests for Engine Operators
 *
 * Tests for streaming backpressure mechanism:
 * - Pause/resume functionality
 * - Memory-based watermarks
 * - Backpressure controller coordination
 * - ScanOperator backpressure
 * - JoinOperator memory limits
 * - AggregateOperator streaming limits
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

import {
  type Row,
  type ExecutionContext,
  type Operator,
  type ScanPlan,
  type JoinPlan,
  type AggregatePlan,
  type BackpressureState,
  type WatermarkConfig,
  DEFAULT_WATERMARKS,
  col,
  lit,
} from '../../types.js';

import { ScanOperator } from '../scan.js';
import { JoinOperator } from '../join.js';
import { AggregateOperator } from '../aggregate.js';
import {
  DefaultBackpressureController,
  supportsBackpressure,
  createBackpressureIterator,
} from '../backpressure.js';

// =============================================================================
// TEST HELPERS
// =============================================================================

/**
 * Create a mock operator that produces rows from an array
 */
function createMockOperator(rows: Row[], columns?: string[]): Operator {
  let index = 0;
  const cols = columns ?? (rows.length > 0 ? Object.keys(rows[0]) : []);

  return {
    async open() {
      index = 0;
    },
    async next() {
      return index < rows.length ? rows[index++] : null;
    },
    async close() {},
    columns() {
      return cols;
    },
    async *[Symbol.asyncIterator]() {
      let row: Row | null;
      while ((row = await this.next()) !== null) {
        yield row;
      }
    },
  };
}

/**
 * Create a mock slow operator for testing backpressure timing
 */
function createSlowMockOperator(rows: Row[], delayMs: number = 10): Operator {
  let index = 0;
  const cols = rows.length > 0 ? Object.keys(rows[0]) : [];

  return {
    async open() {
      index = 0;
    },
    async next() {
      if (index >= rows.length) return null;
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      return rows[index++];
    },
    async close() {},
    columns() {
      return cols;
    },
    async *[Symbol.asyncIterator]() {
      let row: Row | null;
      while ((row = await this.next()) !== null) {
        yield row;
      }
    },
  };
}

/**
 * Generate test rows
 */
function generateRows(count: number, prefix = ''): Row[] {
  const rows: Row[] = [];
  for (let i = 0; i < count; i++) {
    rows.push({
      id: i,
      name: `${prefix}name_${i}`,
      value: i * 100,
      category: `cat_${i % 10}`,
    });
  }
  return rows;
}

/**
 * Collect all rows from an operator
 */
async function collectRows(operator: Operator, ctx: ExecutionContext): Promise<Row[]> {
  await operator.open(ctx);
  const results: Row[] = [];
  let row: Row | null;
  while ((row = await operator.next()) !== null) {
    results.push(row);
  }
  await operator.close();
  return results;
}

/**
 * Create a minimal mock execution context
 */
function createMockExecutionContext(rows?: Row[]): ExecutionContext {
  const tableRows = rows ?? [];

  return {
    schema: {
      tables: new Map([
        [
          'test_table',
          {
            name: 'test_table',
            columns: [
              { name: 'id', type: 'number', nullable: false },
              { name: 'name', type: 'string', nullable: false },
              { name: 'value', type: 'number', nullable: true },
              { name: 'category', type: 'string', nullable: true },
            ],
          },
        ],
      ]),
    },
    btree: {
      get: async () => undefined,
      range: async function* () {},
      scan: async function* () {
        for (const row of tableRows) {
          yield row;
        }
      },
      set: async () => {},
      delete: async () => false,
      count: async () => tableRows.length,
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
// BACKPRESSURE CONTROLLER TESTS
// =============================================================================

describe('DefaultBackpressureController', () => {
  let controller: DefaultBackpressureController;

  beforeEach(() => {
    controller = new DefaultBackpressureController();
  });

  describe('watermark configuration', () => {
    it('should use default watermarks', () => {
      const watermarks = controller.getWatermarks();
      expect(watermarks.highWatermark).toBe(DEFAULT_WATERMARKS.highWatermark);
      expect(watermarks.lowWatermark).toBe(DEFAULT_WATERMARKS.lowWatermark);
    });

    it('should allow custom watermarks', () => {
      const customWatermarks: WatermarkConfig = {
        highWatermark: 1024 * 1024, // 1MB
        lowWatermark: 512 * 1024, // 512KB
      };
      controller.setWatermarks(customWatermarks);

      const watermarks = controller.getWatermarks();
      expect(watermarks.highWatermark).toBe(1024 * 1024);
      expect(watermarks.lowWatermark).toBe(512 * 1024);
    });
  });

  describe('pause/resume requests', () => {
    it('should track pause requests', () => {
      const mockOperator = createMockOperator([]);

      expect(controller.shouldPause()).toBe(false);

      controller.requestPause(mockOperator);
      expect(controller.shouldPause()).toBe(true);

      controller.requestResume(mockOperator);
      expect(controller.shouldPause()).toBe(false);
    });

    it('should handle multiple pause requests', () => {
      const op1 = createMockOperator([]);
      const op2 = createMockOperator([]);

      controller.requestPause(op1);
      controller.requestPause(op2);
      expect(controller.shouldPause()).toBe(true);

      controller.requestResume(op1);
      expect(controller.shouldPause()).toBe(true); // op2 still paused

      controller.requestResume(op2);
      expect(controller.shouldPause()).toBe(false);
    });
  });

  describe('memory tracking', () => {
    it('should track memory usage from multiple operators', () => {
      const op1 = createMockOperator([]);
      const op2 = createMockOperator([]);

      controller.reportMemoryUsage(op1, 1000);
      expect(controller.getTotalMemoryUsage()).toBe(1000);

      controller.reportMemoryUsage(op2, 2000);
      expect(controller.getTotalMemoryUsage()).toBe(3000);
    });

    it('should update memory usage for existing operators', () => {
      const op = createMockOperator([]);

      controller.reportMemoryUsage(op, 1000);
      expect(controller.getTotalMemoryUsage()).toBe(1000);

      controller.reportMemoryUsage(op, 500);
      expect(controller.getTotalMemoryUsage()).toBe(500);
    });

    it('should trigger pause based on memory', () => {
      const customWatermarks: WatermarkConfig = {
        highWatermark: 1000,
        lowWatermark: 500,
      };
      controller.setWatermarks(customWatermarks);

      const op = createMockOperator([]);

      controller.reportMemoryUsage(op, 500);
      expect(controller.shouldPause()).toBe(false);

      controller.reportMemoryUsage(op, 1000);
      expect(controller.shouldPause()).toBe(true);

      controller.reportMemoryUsage(op, 400);
      expect(controller.shouldResume()).toBe(true);
    });
  });

  describe('cleanup', () => {
    it('should clear all state', () => {
      const op = createMockOperator([]);
      controller.requestPause(op);
      controller.reportMemoryUsage(op, 1000);

      controller.clear();

      expect(controller.shouldPause()).toBe(false);
      expect(controller.getTotalMemoryUsage()).toBe(0);
    });
  });
});

// =============================================================================
// SCAN OPERATOR BACKPRESSURE TESTS
// =============================================================================

describe('ScanOperator Backpressure', () => {
  describe('backpressure support detection', () => {
    it('should implement BackpressureOperator interface', () => {
      const plan: ScanPlan = {
        id: 1,
        type: 'scan',
        table: 'test_table',
        source: 'btree',
        columns: ['id', 'name'],
      };
      const ctx = createMockExecutionContext();
      const operator = new ScanOperator(plan, ctx);

      expect(supportsBackpressure(operator)).toBe(true);
    });
  });

  describe('pause and resume', () => {
    it('should start in flowing state', () => {
      const plan: ScanPlan = {
        id: 1,
        type: 'scan',
        table: 'test_table',
        source: 'btree',
        columns: ['id', 'name'],
      };
      const ctx = createMockExecutionContext();
      const operator = new ScanOperator(plan, ctx);

      expect(operator.getBackpressureState()).toBe('flowing');
      expect(operator.isPaused()).toBe(false);
    });

    it('should transition to paused state', async () => {
      const plan: ScanPlan = {
        id: 1,
        type: 'scan',
        table: 'test_table',
        source: 'btree',
        columns: ['id', 'name'],
      };
      const ctx = createMockExecutionContext();
      const operator = new ScanOperator(plan, ctx);

      await operator.pause();

      expect(operator.getBackpressureState()).toBe('paused');
      expect(operator.isPaused()).toBe(true);
    });

    it('should resume from paused state', async () => {
      const plan: ScanPlan = {
        id: 1,
        type: 'scan',
        table: 'test_table',
        source: 'btree',
        columns: ['id', 'name'],
      };
      const ctx = createMockExecutionContext();
      const operator = new ScanOperator(plan, ctx);

      await operator.pause();
      expect(operator.isPaused()).toBe(true);

      await operator.resume();
      expect(operator.isPaused()).toBe(false);
      expect(operator.getBackpressureState()).toBe('flowing');
    });

    it('should block next() while paused', async () => {
      const rows = generateRows(10);
      const plan: ScanPlan = {
        id: 1,
        type: 'scan',
        table: 'test_table',
        source: 'btree',
        columns: ['id', 'name', 'value', 'category'],
      };
      const ctx = createMockExecutionContext(rows);
      const operator = new ScanOperator(plan, ctx);

      await operator.open(ctx);

      // Get first row
      const row1 = await operator.next();
      expect(row1).not.toBeNull();

      // Pause the operator
      await operator.pause();

      // next() should block until resumed
      let nextResolved = false;
      const nextPromise = operator.next().then((row) => {
        nextResolved = true;
        return row;
      });

      // Wait a bit - next should still be blocked
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(nextResolved).toBe(false);

      // Resume - next should complete
      await operator.resume();
      const row2 = await nextPromise;
      expect(nextResolved).toBe(true);
      expect(row2).not.toBeNull();

      await operator.close();
    });
  });

  describe('backpressure statistics', () => {
    it('should track pause/resume counts', async () => {
      const plan: ScanPlan = {
        id: 1,
        type: 'scan',
        table: 'test_table',
        source: 'btree',
        columns: ['id'],
      };
      const ctx = createMockExecutionContext();
      const operator = new ScanOperator(plan, ctx);

      const initialStats = operator.getBackpressureStats();
      expect(initialStats.pauseCount).toBe(0);
      expect(initialStats.resumeCount).toBe(0);

      await operator.pause();
      await operator.resume();
      await operator.pause();
      await operator.resume();

      const finalStats = operator.getBackpressureStats();
      expect(finalStats.pauseCount).toBe(2);
      expect(finalStats.resumeCount).toBe(2);
    });
  });

  describe('backpressure callbacks', () => {
    it('should notify on state changes', async () => {
      const plan: ScanPlan = {
        id: 1,
        type: 'scan',
        table: 'test_table',
        source: 'btree',
        columns: ['id'],
      };
      const ctx = createMockExecutionContext();
      const operator = new ScanOperator(plan, ctx);

      const stateChanges: BackpressureState[] = [];
      operator.onBackpressure((state) => {
        stateChanges.push(state);
      });

      await operator.pause();
      await operator.resume();

      expect(stateChanges).toEqual(['paused', 'flowing']);
    });
  });

  describe('controller integration', () => {
    it('should report pause/resume to controller', async () => {
      const controller = new DefaultBackpressureController();
      const plan: ScanPlan = {
        id: 1,
        type: 'scan',
        table: 'test_table',
        source: 'btree',
        columns: ['id'],
      };
      const ctx = createMockExecutionContext();
      const operator = new ScanOperator(plan, ctx);
      operator.setController(controller);

      expect(controller.shouldPause()).toBe(false);

      await operator.pause();
      expect(controller.shouldPause()).toBe(true);

      await operator.resume();
      expect(controller.shouldPause()).toBe(false);
    });
  });
});

// =============================================================================
// JOIN OPERATOR BACKPRESSURE TESTS
// =============================================================================

describe('JoinOperator Backpressure', () => {
  describe('backpressure support', () => {
    it('should implement BackpressureOperator interface', () => {
      const leftRows = generateRows(5, 'left_');
      const rightRows = generateRows(5, 'right_');

      const plan: JoinPlan = {
        id: 1,
        type: 'join',
        joinType: 'inner',
        left: { id: 2, type: 'scan', table: 'left', source: 'btree', columns: ['id', 'name'] },
        right: { id: 3, type: 'scan', table: 'right', source: 'btree', columns: ['id', 'value'] },
        condition: {
          type: 'comparison',
          op: 'eq',
          left: col('id', 'left'),
          right: col('id', 'right'),
        },
      };

      const leftInput = createMockOperator(leftRows, ['id', 'name', 'value', 'category']);
      const rightInput = createMockOperator(rightRows, ['id', 'name', 'value', 'category']);
      const ctx = createMockExecutionContext();

      const operator = new JoinOperator(plan, leftInput, rightInput, ctx);

      expect(supportsBackpressure(operator)).toBe(true);
    });
  });

  describe('memory tracking during hash build', () => {
    it('should track memory usage during materialization', async () => {
      const leftRows = generateRows(100);
      const rightRows = generateRows(100);

      const plan: JoinPlan = {
        id: 1,
        type: 'join',
        joinType: 'inner',
        algorithm: 'hash',
        left: { id: 2, type: 'scan', table: 'left', source: 'btree', columns: ['id', 'name'] },
        right: { id: 3, type: 'scan', table: 'right', source: 'btree', columns: ['id', 'value'] },
        condition: {
          type: 'comparison',
          op: 'eq',
          left: col('id', 'left'),
          right: col('id', 'right'),
        },
      };

      const leftInput = createMockOperator(leftRows, ['id', 'name', 'value', 'category']);
      const rightInput = createMockOperator(rightRows, ['id', 'name', 'value', 'category']);
      const ctx = createMockExecutionContext();

      const operator = new JoinOperator(plan, leftInput, rightInput, ctx);
      await operator.open(ctx);

      const stats = operator.getBackpressureStats();
      expect(stats.memoryUsage).toBeGreaterThan(0);
      expect(stats.bufferedRows).toBe(100); // Right side materialized

      await operator.close();
    });

    it('should trigger draining state when memory limit exceeded', async () => {
      const rightRows = generateRows(1000);

      const plan: JoinPlan = {
        id: 1,
        type: 'join',
        joinType: 'inner',
        algorithm: 'hash',
        left: { id: 2, type: 'scan', table: 'left', source: 'btree', columns: ['id'] },
        right: { id: 3, type: 'scan', table: 'right', source: 'btree', columns: ['id'] },
        condition: {
          type: 'comparison',
          op: 'eq',
          left: col('id', 'left'),
          right: col('id', 'right'),
        },
      };

      const leftInput = createMockOperator([], ['id', 'name', 'value', 'category']);
      const rightInput = createMockOperator(rightRows, ['id', 'name', 'value', 'category']);
      const ctx = createMockExecutionContext();

      const operator = new JoinOperator(plan, leftInput, rightInput, ctx);

      // Set very low watermarks to trigger backpressure
      operator.setWatermarks({
        highWatermark: 1000, // 1KB
        lowWatermark: 500,
      });

      const stateChanges: BackpressureState[] = [];
      operator.onBackpressure((state) => {
        stateChanges.push(state);
      });

      await operator.open(ctx);

      // Should have triggered draining state
      expect(operator.isMemoryLimitExceeded()).toBe(true);
      expect(stateChanges).toContain('draining');

      await operator.close();
    });

    it('should report memory to controller', async () => {
      const controller = new DefaultBackpressureController();
      const rightRows = generateRows(50);

      const plan: JoinPlan = {
        id: 1,
        type: 'join',
        joinType: 'inner',
        algorithm: 'hash',
        left: { id: 2, type: 'scan', table: 'left', source: 'btree', columns: ['id'] },
        right: { id: 3, type: 'scan', table: 'right', source: 'btree', columns: ['id'] },
        condition: {
          type: 'comparison',
          op: 'eq',
          left: col('id', 'left'),
          right: col('id', 'right'),
        },
      };

      const leftInput = createMockOperator([], ['id', 'name', 'value', 'category']);
      const rightInput = createMockOperator(rightRows, ['id', 'name', 'value', 'category']);
      const ctx = createMockExecutionContext();

      const operator = new JoinOperator(plan, leftInput, rightInput, ctx);
      operator.setController(controller);

      await operator.open(ctx);

      expect(controller.getTotalMemoryUsage()).toBeGreaterThan(0);

      await operator.close();

      // Memory should be cleared after close
      expect(controller.getTotalMemoryUsage()).toBe(0);
    });
  });

  describe('pause and resume', () => {
    it('should support pause/resume operations', async () => {
      const plan: JoinPlan = {
        id: 1,
        type: 'join',
        joinType: 'inner',
        left: { id: 2, type: 'scan', table: 'left', source: 'btree', columns: ['id'] },
        right: { id: 3, type: 'scan', table: 'right', source: 'btree', columns: ['id'] },
      };

      const leftInput = createMockOperator([], ['id']);
      const rightInput = createMockOperator([], ['id']);
      const ctx = createMockExecutionContext();

      const operator = new JoinOperator(plan, leftInput, rightInput, ctx);

      expect(operator.isPaused()).toBe(false);

      await operator.pause();
      expect(operator.isPaused()).toBe(true);

      await operator.resume();
      expect(operator.isPaused()).toBe(false);
    });
  });
});

// =============================================================================
// AGGREGATE OPERATOR BACKPRESSURE TESTS
// =============================================================================

describe('AggregateOperator Backpressure', () => {
  describe('backpressure support', () => {
    it('should implement BackpressureOperator interface', () => {
      const plan: AggregatePlan = {
        id: 1,
        type: 'aggregate',
        input: { id: 2, type: 'scan', table: 'test', source: 'btree', columns: ['category'] },
        groupBy: [col('category')],
        aggregates: [
          {
            expr: { type: 'aggregate', function: 'count', arg: '*' },
            alias: 'count',
          },
        ],
      };

      const input = createMockOperator([], ['category']);
      const ctx = createMockExecutionContext();

      const operator = new AggregateOperator(plan, input, ctx);

      expect(supportsBackpressure(operator)).toBe(true);
    });
  });

  describe('group tracking', () => {
    it('should track number of groups', async () => {
      const rows = generateRows(100); // 10 unique categories (cat_0 to cat_9)

      const plan: AggregatePlan = {
        id: 1,
        type: 'aggregate',
        input: { id: 2, type: 'scan', table: 'test', source: 'btree', columns: ['category'] },
        groupBy: [col('category')],
        aggregates: [
          {
            expr: { type: 'aggregate', function: 'count', arg: '*' },
            alias: 'count',
          },
        ],
      };

      const input = createMockOperator(rows, ['id', 'name', 'value', 'category']);
      const ctx = createMockExecutionContext();

      const operator = new AggregateOperator(plan, input, ctx);
      await operator.open(ctx);

      expect(operator.getGroupCount()).toBe(10);
      expect(operator.getBackpressureStats().bufferedRows).toBe(100);

      await operator.close();
    });

    it('should track memory usage during aggregation', async () => {
      const rows = generateRows(100);

      const plan: AggregatePlan = {
        id: 1,
        type: 'aggregate',
        input: { id: 2, type: 'scan', table: 'test', source: 'btree', columns: ['category'] },
        groupBy: [col('category')],
        aggregates: [
          {
            expr: { type: 'aggregate', function: 'sum', arg: col('value') },
            alias: 'total',
          },
        ],
      };

      const input = createMockOperator(rows, ['id', 'name', 'value', 'category']);
      const ctx = createMockExecutionContext();

      const operator = new AggregateOperator(plan, input, ctx);
      await operator.open(ctx);

      const stats = operator.getBackpressureStats();
      expect(stats.memoryUsage).toBeGreaterThan(0);

      await operator.close();
    });
  });

  describe('group limits', () => {
    it('should trigger draining when group limit exceeded', async () => {
      // Create rows with many unique categories
      const rows: Row[] = [];
      for (let i = 0; i < 100; i++) {
        rows.push({ id: i, category: `unique_cat_${i}`, value: i });
      }

      const plan: AggregatePlan = {
        id: 1,
        type: 'aggregate',
        input: { id: 2, type: 'scan', table: 'test', source: 'btree', columns: ['category'] },
        groupBy: [col('category')],
        aggregates: [
          {
            expr: { type: 'aggregate', function: 'count', arg: '*' },
            alias: 'count',
          },
        ],
      };

      const input = createMockOperator(rows, ['id', 'category', 'value']);
      const ctx = createMockExecutionContext();

      const operator = new AggregateOperator(plan, input, ctx);
      operator.setMaxGroups(50); // Limit to 50 groups

      const stateChanges: BackpressureState[] = [];
      operator.onBackpressure((state) => {
        stateChanges.push(state);
      });

      await operator.open(ctx);

      expect(operator.isGroupLimitExceeded()).toBe(true);
      expect(stateChanges).toContain('draining');

      await operator.close();
    });

    it('should not trigger limit for small group counts', async () => {
      const rows = generateRows(100); // Only 10 unique categories

      const plan: AggregatePlan = {
        id: 1,
        type: 'aggregate',
        input: { id: 2, type: 'scan', table: 'test', source: 'btree', columns: ['category'] },
        groupBy: [col('category')],
        aggregates: [
          {
            expr: { type: 'aggregate', function: 'count', arg: '*' },
            alias: 'count',
          },
        ],
      };

      const input = createMockOperator(rows, ['id', 'name', 'value', 'category']);
      const ctx = createMockExecutionContext();

      const operator = new AggregateOperator(plan, input, ctx);
      // Default max groups is very high

      await operator.open(ctx);

      expect(operator.isGroupLimitExceeded()).toBe(false);
      expect(operator.getBackpressureState()).toBe('flowing');

      await operator.close();
    });
  });

  describe('memory watermarks', () => {
    it('should trigger draining when high watermark exceeded', async () => {
      // Create many groups with large keys
      const rows: Row[] = [];
      for (let i = 0; i < 1000; i++) {
        rows.push({
          id: i,
          category: `very_long_category_name_that_uses_memory_${i}`,
          value: i,
        });
      }

      const plan: AggregatePlan = {
        id: 1,
        type: 'aggregate',
        input: { id: 2, type: 'scan', table: 'test', source: 'btree', columns: ['category'] },
        groupBy: [col('category')],
        aggregates: [
          {
            expr: { type: 'aggregate', function: 'sum', arg: col('value') },
            alias: 'total',
          },
        ],
      };

      const input = createMockOperator(rows, ['id', 'category', 'value']);
      const ctx = createMockExecutionContext();

      const operator = new AggregateOperator(plan, input, ctx);
      operator.setWatermarks({
        highWatermark: 1000, // Very low to trigger
        lowWatermark: 500,
      });

      await operator.open(ctx);

      expect(operator.getBackpressureState()).toBe('draining');

      await operator.close();
    });
  });

  describe('pause and resume', () => {
    it('should support pause/resume during result iteration', async () => {
      const rows = generateRows(100);

      const plan: AggregatePlan = {
        id: 1,
        type: 'aggregate',
        input: { id: 2, type: 'scan', table: 'test', source: 'btree', columns: ['category'] },
        groupBy: [col('category')],
        aggregates: [
          {
            expr: { type: 'aggregate', function: 'count', arg: '*' },
            alias: 'count',
          },
        ],
      };

      const input = createMockOperator(rows, ['id', 'name', 'value', 'category']);
      const ctx = createMockExecutionContext();

      const operator = new AggregateOperator(plan, input, ctx);

      expect(operator.isPaused()).toBe(false);

      await operator.pause();
      expect(operator.isPaused()).toBe(true);

      await operator.resume();
      expect(operator.isPaused()).toBe(false);
    });
  });

  describe('controller integration', () => {
    it('should report memory to controller during aggregation', async () => {
      const controller = new DefaultBackpressureController();
      const rows = generateRows(100);

      const plan: AggregatePlan = {
        id: 1,
        type: 'aggregate',
        input: { id: 2, type: 'scan', table: 'test', source: 'btree', columns: ['category'] },
        groupBy: [col('category')],
        aggregates: [
          {
            expr: { type: 'aggregate', function: 'count', arg: '*' },
            alias: 'count',
          },
        ],
      };

      const input = createMockOperator(rows, ['id', 'name', 'value', 'category']);
      const ctx = createMockExecutionContext();

      const operator = new AggregateOperator(plan, input, ctx);
      operator.setController(controller);

      await operator.open(ctx);

      expect(controller.getTotalMemoryUsage()).toBeGreaterThan(0);

      await operator.close();

      // Memory should be cleared after close
      expect(controller.getTotalMemoryUsage()).toBe(0);
    });
  });
});

// =============================================================================
// BACKPRESSURE UTILITY TESTS
// =============================================================================

describe('Backpressure Utilities', () => {
  describe('supportsBackpressure', () => {
    it('should return true for operators with backpressure support', () => {
      const plan: ScanPlan = {
        id: 1,
        type: 'scan',
        table: 'test',
        source: 'btree',
        columns: ['id'],
      };
      const ctx = createMockExecutionContext();
      const operator = new ScanOperator(plan, ctx);

      expect(supportsBackpressure(operator)).toBe(true);
    });

    it('should return false for basic mock operators', () => {
      const mockOperator = createMockOperator([]);
      expect(supportsBackpressure(mockOperator)).toBe(false);
    });
  });

  describe('createBackpressureIterator', () => {
    it('should iterate through all rows', async () => {
      const rows = generateRows(10);
      const plan: ScanPlan = {
        id: 1,
        type: 'scan',
        table: 'test_table',
        source: 'btree',
        columns: ['id', 'name', 'value', 'category'],
      };
      const ctx = createMockExecutionContext(rows);
      const operator = new ScanOperator(plan, ctx);
      await operator.open(ctx);

      const collectedRows: Row[] = [];
      for await (const row of createBackpressureIterator(operator)) {
        collectedRows.push(row);
      }

      expect(collectedRows.length).toBe(10);
    });
  });
});

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

describe('Backpressure Integration', () => {
  describe('pipeline coordination', () => {
    it('should coordinate backpressure across multiple operators', async () => {
      const controller = new DefaultBackpressureController();

      // Create a simple pipeline: scan -> join
      const leftRows = generateRows(50, 'left_');
      const rightRows = generateRows(50, 'right_');

      const scanPlan: ScanPlan = {
        id: 1,
        type: 'scan',
        table: 'test_table',
        source: 'btree',
        columns: ['id', 'name', 'value', 'category'],
      };

      const joinPlan: JoinPlan = {
        id: 2,
        type: 'join',
        joinType: 'inner',
        algorithm: 'hash',
        left: { id: 3, type: 'scan', table: 'left', source: 'btree', columns: ['id'] },
        right: { id: 4, type: 'scan', table: 'right', source: 'btree', columns: ['id'] },
        condition: {
          type: 'comparison',
          op: 'eq',
          left: col('id', 'left'),
          right: col('id', 'right'),
        },
      };

      const ctx = createMockExecutionContext(leftRows);
      const scanOp = new ScanOperator(scanPlan, ctx);
      scanOp.setController(controller);

      const leftInput = createMockOperator(leftRows, ['id', 'name', 'value', 'category']);
      const rightInput = createMockOperator(rightRows, ['id', 'name', 'value', 'category']);
      const joinOp = new JoinOperator(joinPlan, leftInput, rightInput, ctx);
      joinOp.setController(controller);

      await joinOp.open(ctx);

      // Both operators should report memory
      expect(controller.getTotalMemoryUsage()).toBeGreaterThan(0);

      await joinOp.close();
    });

    it('should reset state when operators are closed', async () => {
      const controller = new DefaultBackpressureController();
      const rows = generateRows(100);

      const plan: AggregatePlan = {
        id: 1,
        type: 'aggregate',
        input: { id: 2, type: 'scan', table: 'test', source: 'btree', columns: ['category'] },
        groupBy: [col('category')],
        aggregates: [
          {
            expr: { type: 'aggregate', function: 'count', arg: '*' },
            alias: 'count',
          },
        ],
      };

      const input = createMockOperator(rows, ['id', 'name', 'value', 'category']);
      const ctx = createMockExecutionContext();

      const operator = new AggregateOperator(plan, input, ctx);
      operator.setController(controller);

      await operator.open(ctx);
      const memoryBefore = controller.getTotalMemoryUsage();
      expect(memoryBefore).toBeGreaterThan(0);

      await operator.close();

      expect(controller.getTotalMemoryUsage()).toBe(0);
      expect(operator.getBackpressureState()).toBe('flowing');
      expect(operator.getGroupCount()).toBe(0);
    });
  });

  describe('watermark configuration', () => {
    it('should allow customizing watermarks per operator', () => {
      const plan: ScanPlan = {
        id: 1,
        type: 'scan',
        table: 'test',
        source: 'btree',
        columns: ['id'],
      };
      const ctx = createMockExecutionContext();
      const operator = new ScanOperator(plan, ctx);

      const customWatermarks: WatermarkConfig = {
        highWatermark: 1024 * 1024,
        lowWatermark: 512 * 1024,
      };

      operator.setWatermarks(customWatermarks);

      // Verify it doesn't throw and operator still works
      expect(operator.isPaused()).toBe(false);
    });
  });
});
