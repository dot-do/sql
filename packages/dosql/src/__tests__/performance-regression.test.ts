/**
 * DoSQL Performance Regression Tests
 *
 * Issue: sql-io8b - Performance Regression Tests - Query latency, INSERT throughput baselines
 *
 * This test suite establishes performance baselines that serve as regression guards.
 * Tests will FAIL if performance regresses significantly below the established thresholds.
 *
 * Performance Baselines (regression thresholds):
 *
 * Query Latency:
 * - Simple SELECT P50: < 5ms
 * - Simple SELECT P95: < 15ms
 * - Simple SELECT P99: < 25ms
 * - Point lookup P50: < 3ms
 * - Point lookup P95: < 10ms
 * - Point lookup P99: < 20ms
 *
 * INSERT Throughput:
 * - Single INSERT: > 100 rows/second
 * - Batch INSERT (100 rows): > 500 rows/second effective
 * - INSERT P95 latency: < 20ms
 *
 * SELECT Throughput:
 * - Point queries: > 500 queries/second
 * - Range queries: > 100 queries/second
 *
 * Transaction Commit Latency:
 * - Single-op transaction P50: < 10ms
 * - Single-op transaction P95: < 30ms
 * - Multi-op transaction P95: < 50ms
 *
 * @packageDocumentation
 */

import { describe, it, expect } from 'vitest';
import { env, runInDurableObject } from 'cloudflare:test';
import { DurableObject } from 'cloudflare:workers';

// =============================================================================
// Test Environment Types
// =============================================================================

interface PerformanceRegressionEnv {
  PERFORMANCE_REGRESSION_DO: DurableObjectNamespace;
}

// =============================================================================
// Performance Measurement Utilities
// =============================================================================

/**
 * Latency statistics for performance measurements
 */
interface LatencyStats {
  min: number;
  max: number;
  avg: number;
  p50: number;
  p95: number;
  p99: number;
  samples: number[];
}

/**
 * Throughput statistics
 */
interface ThroughputStats {
  opsPerSecond: number;
  totalOps: number;
  totalTimeMs: number;
}

/**
 * Calculate latency percentiles from an array of durations
 */
function calculateLatencyStats(durations: number[]): LatencyStats {
  if (durations.length === 0) {
    return { min: 0, max: 0, avg: 0, p50: 0, p95: 0, p99: 0, samples: [] };
  }

  const sorted = [...durations].sort((a, b) => a - b);
  const sum = sorted.reduce((a, b) => a + b, 0);

  const percentile = (p: number) => {
    const index = Math.ceil((p / 100) * sorted.length) - 1;
    return sorted[Math.max(0, index)] || 0;
  };

  return {
    min: sorted[0],
    max: sorted[sorted.length - 1],
    avg: sum / sorted.length,
    p50: percentile(50),
    p95: percentile(95),
    p99: percentile(99),
    samples: sorted,
  };
}

/**
 * Measure execution time of an operation
 */
async function measureTime<T>(fn: () => Promise<T> | T): Promise<{ result: T; elapsed: number }> {
  const start = performance.now();
  const result = await fn();
  const elapsed = performance.now() - start;
  return { result, elapsed };
}

/**
 * Run a benchmark with warmup iterations
 */
async function runBenchmark<T>(
  fn: () => Promise<T> | T,
  options: {
    iterations: number;
    warmupIterations?: number;
  }
): Promise<{ latency: LatencyStats; results: T[] }> {
  const { iterations, warmupIterations = 5 } = options;

  // Warmup (not measured)
  for (let i = 0; i < warmupIterations; i++) {
    await fn();
  }

  // Measured runs
  const durations: number[] = [];
  const results: T[] = [];

  for (let i = 0; i < iterations; i++) {
    const { result, elapsed } = await measureTime(fn);
    durations.push(elapsed);
    results.push(result);
  }

  return {
    latency: calculateLatencyStats(durations),
    results,
  };
}

/**
 * Measure throughput over a duration
 */
async function measureThroughput<T>(
  fn: () => Promise<T> | T,
  options: {
    durationMs: number;
    minIterations?: number;
  }
): Promise<ThroughputStats> {
  const { durationMs, minIterations = 10 } = options;
  let totalOps = 0;

  const start = performance.now();
  while (performance.now() - start < durationMs || totalOps < minIterations) {
    await fn();
    totalOps++;
  }
  const totalTimeMs = performance.now() - start;

  return {
    opsPerSecond: (totalOps / totalTimeMs) * 1000,
    totalOps,
    totalTimeMs,
  };
}

// =============================================================================
// Test Counter for Unique DO Instances
// =============================================================================

let testCounter = 0;

function getUniqueStub(): DurableObjectStub {
  const typedEnv = env as unknown as PerformanceRegressionEnv;
  const id = typedEnv.PERFORMANCE_REGRESSION_DO.idFromName(
    `perf-regression-v2-${Date.now()}-${testCounter++}`
  );
  return typedEnv.PERFORMANCE_REGRESSION_DO.get(id);
}

// =============================================================================
// Performance Regression Test DO (reuse existing from performance.test.ts)
// =============================================================================

// Note: The PerformanceRegressionDO class is already defined in performance.test.ts
// and configured in vitest.config.ts. We'll use the same DO class here.

export class PerformanceRegressionDO extends DurableObject {
  private sql: SqlStorage;

  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
  }

  /**
   * Execute SQL and return timing
   */
  async execWithTiming(sql: string): Promise<{ rows: unknown[]; elapsed: number }> {
    const start = performance.now();
    const cursor = this.sql.exec(sql);
    const rows = [...cursor];
    const elapsed = performance.now() - start;
    return { rows, elapsed };
  }

  /**
   * Execute SQL without timing
   */
  exec(sql: string): unknown[] {
    return [...this.sql.exec(sql)];
  }

  /**
   * Setup test table with data
   */
  async setupTable(tableName: string, rowCount: number): Promise<void> {
    this.sql.exec(`DROP TABLE IF EXISTS ${tableName}`);
    this.sql.exec(`
      CREATE TABLE ${tableName} (
        id INTEGER PRIMARY KEY,
        name TEXT,
        value REAL,
        data TEXT,
        created_at INTEGER
      )
    `);

    // Insert in batches for efficiency
    const batchSize = 100;
    for (let i = 0; i < rowCount; i += batchSize) {
      const values: string[] = [];
      for (let j = i; j < Math.min(i + batchSize, rowCount); j++) {
        values.push(`(${j}, 'name_${j}', ${j * 1.5}, 'data_${j}', ${Date.now()})`);
      }
      this.sql.exec(`INSERT INTO ${tableName} (id, name, value, data, created_at) VALUES ${values.join(', ')}`);
    }
  }

  /**
   * Create secondary index
   */
  createIndex(tableName: string, column: string, indexName: string): void {
    this.sql.exec(`CREATE INDEX IF NOT EXISTS ${indexName} ON ${tableName}(${column})`);
  }
}

// =============================================================================
// QUERY LATENCY REGRESSION TESTS
// =============================================================================

describe('performance regression - Query Latency Baselines', () => {
  describe('Simple SELECT Latency', () => {
    it('should maintain P50 latency < 5ms for simple SELECT', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        await instance.setupTable('select_p50', 500);

        const { latency } = await runBenchmark(
          () => instance.execWithTiming('SELECT * FROM select_p50 LIMIT 10'),
          { iterations: 50, warmupIterations: 10 }
        );

        expect(latency.p50).toBeLessThan(5);
      });
    });

    it('should maintain P95 latency < 15ms for simple SELECT', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        await instance.setupTable('select_p95', 500);

        const { latency } = await runBenchmark(
          () => instance.execWithTiming('SELECT * FROM select_p95 LIMIT 10'),
          { iterations: 100, warmupIterations: 10 }
        );

        expect(latency.p95).toBeLessThan(15);
      });
    });

    it('should maintain P99 latency < 25ms for simple SELECT', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        await instance.setupTable('select_p99', 500);

        const { latency } = await runBenchmark(
          () => instance.execWithTiming('SELECT * FROM select_p99 LIMIT 10'),
          { iterations: 100, warmupIterations: 10 }
        );

        expect(latency.p99).toBeLessThan(25);
      });
    });
  });

  describe('Point Lookup Latency', () => {
    it('should maintain P50 latency < 3ms for point lookup by ID', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        await instance.setupTable('point_p50', 1000);

        const { latency } = await runBenchmark(
          () => instance.execWithTiming('SELECT * FROM point_p50 WHERE id = 500'),
          { iterations: 50, warmupIterations: 10 }
        );

        expect(latency.p50).toBeLessThan(3);
      });
    });

    it('should maintain P95 latency < 10ms for point lookup by ID', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        await instance.setupTable('point_p95', 1000);

        const { latency } = await runBenchmark(
          () => instance.execWithTiming('SELECT * FROM point_p95 WHERE id = 500'),
          { iterations: 100, warmupIterations: 10 }
        );

        expect(latency.p95).toBeLessThan(10);
      });
    });

    it('should maintain P99 latency < 20ms for point lookup by ID', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        await instance.setupTable('point_p99', 1000);

        const { latency } = await runBenchmark(
          () => instance.execWithTiming('SELECT * FROM point_p99 WHERE id = 500'),
          { iterations: 100, warmupIterations: 10 }
        );

        expect(latency.p99).toBeLessThan(20);
      });
    });
  });

  describe('Range Query Latency', () => {
    it('should maintain P95 latency < 20ms for indexed range query', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        await instance.setupTable('range_test', 1000);

        const { latency } = await runBenchmark(
          () => instance.execWithTiming('SELECT * FROM range_test WHERE id BETWEEN 400 AND 600'),
          { iterations: 50, warmupIterations: 10 }
        );

        expect(latency.p95).toBeLessThan(20);
      });
    });

    it('should maintain P95 latency < 30ms for secondary index range query', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        await instance.setupTable('range_secondary', 1000);
        instance.createIndex('range_secondary', 'value', 'idx_range_value');

        const { latency } = await runBenchmark(
          () => instance.execWithTiming('SELECT * FROM range_secondary WHERE value BETWEEN 500 AND 1000'),
          { iterations: 50, warmupIterations: 10 }
        );

        expect(latency.p95).toBeLessThan(30);
      });
    });
  });
});

// =============================================================================
// INSERT THROUGHPUT REGRESSION TESTS
// =============================================================================

describe('performance regression - INSERT Throughput Baselines', () => {
  describe('Single INSERT Throughput', () => {
    it('should achieve > 100 single INSERTs per second', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        instance.exec('CREATE TABLE insert_throughput (id INTEGER PRIMARY KEY, data TEXT)');

        let id = 1;
        const stats = await measureThroughput(
          () => instance.exec(`INSERT INTO insert_throughput (id, data) VALUES (${id++}, 'test_data')`),
          { durationMs: 2000, minIterations: 50 }
        );

        expect(stats.opsPerSecond).toBeGreaterThan(100);
      });
    });

    it('should maintain INSERT P95 latency < 20ms', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        instance.exec('CREATE TABLE insert_latency (id INTEGER PRIMARY KEY, data TEXT)');

        let id = 1;
        const { latency } = await runBenchmark(
          () => instance.execWithTiming(`INSERT INTO insert_latency (id, data) VALUES (${id++}, 'test')`),
          { iterations: 100, warmupIterations: 10 }
        );

        expect(latency.p95).toBeLessThan(20);
      });
    });

    it('should maintain INSERT P99 latency < 35ms', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        instance.exec('CREATE TABLE insert_p99 (id INTEGER PRIMARY KEY, data TEXT)');

        let id = 1;
        const { latency } = await runBenchmark(
          () => instance.execWithTiming(`INSERT INTO insert_p99 (id, data) VALUES (${id++}, 'test')`),
          { iterations: 100, warmupIterations: 10 }
        );

        expect(latency.p99).toBeLessThan(35);
      });
    });
  });

  describe('Batch INSERT Throughput', () => {
    it('should achieve > 500 effective rows/second for batch INSERT', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        instance.exec('CREATE TABLE batch_throughput (id INTEGER PRIMARY KEY, data TEXT)');

        let batchNum = 0;
        const batchSize = 100;

        const start = performance.now();
        let totalRows = 0;

        // Run for 2 seconds or at least 10 batches
        while (performance.now() - start < 2000 || totalRows < 500) {
          const values = Array.from(
            { length: batchSize },
            (_, i) => `(${batchNum * batchSize + i}, 'data_${i}')`
          ).join(', ');
          instance.exec(`INSERT INTO batch_throughput (id, data) VALUES ${values}`);
          batchNum++;
          totalRows += batchSize;
        }

        const totalTimeMs = performance.now() - start;
        const rowsPerSecond = (totalRows / totalTimeMs) * 1000;

        expect(rowsPerSecond).toBeGreaterThan(500);
      });
    });

    it('should complete 100-row batch INSERT in < 100ms', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        instance.exec('CREATE TABLE batch_latency (id INTEGER PRIMARY KEY, data TEXT)');

        let batchId = 0;
        const { latency } = await runBenchmark(
          () => {
            const base = batchId * 100;
            batchId++;
            const values = Array.from({ length: 100 }, (_, i) => `(${base + i}, 'data_${i}')`).join(', ');
            return instance.execWithTiming(`INSERT INTO batch_latency (id, data) VALUES ${values}`);
          },
          { iterations: 20, warmupIterations: 5 }
        );

        expect(latency.p95).toBeLessThan(100);
      });
    });
  });
});

// =============================================================================
// SELECT THROUGHPUT REGRESSION TESTS
// =============================================================================

describe('performance regression - SELECT Throughput Baselines', () => {
  describe('Point Query Throughput', () => {
    it('should achieve > 500 point queries per second', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        await instance.setupTable('point_throughput', 1000);

        const stats = await measureThroughput(
          () => instance.exec('SELECT * FROM point_throughput WHERE id = 500'),
          { durationMs: 2000, minIterations: 100 }
        );

        expect(stats.opsPerSecond).toBeGreaterThan(500);
      });
    });

    it('should maintain > 300 random point queries per second', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        await instance.setupTable('random_point', 1000);

        let queryCount = 0;
        const start = performance.now();

        while (performance.now() - start < 2000 || queryCount < 100) {
          const id = Math.floor(Math.random() * 1000);
          instance.exec(`SELECT * FROM random_point WHERE id = ${id}`);
          queryCount++;
        }

        const totalTimeMs = performance.now() - start;
        const qps = (queryCount / totalTimeMs) * 1000;

        expect(qps).toBeGreaterThan(300);
      });
    });
  });

  describe('Range Query Throughput', () => {
    it('should achieve > 100 range queries per second', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        await instance.setupTable('range_throughput', 1000);

        const stats = await measureThroughput(
          () => instance.exec('SELECT * FROM range_throughput WHERE id BETWEEN 400 AND 600'),
          { durationMs: 2000, minIterations: 50 }
        );

        expect(stats.opsPerSecond).toBeGreaterThan(100);
      });
    });
  });

  describe('Aggregate Query Throughput', () => {
    it('should achieve > 200 aggregate queries per second', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        await instance.setupTable('agg_throughput', 1000);

        const stats = await measureThroughput(
          () => instance.exec('SELECT COUNT(*), AVG(value), SUM(value) FROM agg_throughput'),
          { durationMs: 2000, minIterations: 50 }
        );

        expect(stats.opsPerSecond).toBeGreaterThan(200);
      });
    });
  });
});

// =============================================================================
// TRANSACTION COMMIT LATENCY REGRESSION TESTS
// =============================================================================

describe('performance regression - Transaction Commit Latency Baselines', () => {
  describe('Single-Operation Transaction', () => {
    it('should maintain P50 latency < 10ms for single UPDATE transaction', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        instance.exec('CREATE TABLE txn_single (id INTEGER PRIMARY KEY, value INTEGER)');
        instance.exec('INSERT INTO txn_single (id, value) VALUES (1, 100)');

        const { latency } = await runBenchmark(
          () => instance.execWithTiming('UPDATE txn_single SET value = value + 1 WHERE id = 1'),
          { iterations: 50, warmupIterations: 10 }
        );

        expect(latency.p50).toBeLessThan(10);
      });
    });

    it('should maintain P95 latency < 30ms for single UPDATE transaction', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        instance.exec('CREATE TABLE txn_p95 (id INTEGER PRIMARY KEY, value INTEGER)');
        instance.exec('INSERT INTO txn_p95 (id, value) VALUES (1, 100)');

        const { latency } = await runBenchmark(
          () => instance.execWithTiming('UPDATE txn_p95 SET value = value + 1 WHERE id = 1'),
          { iterations: 100, warmupIterations: 10 }
        );

        expect(latency.p95).toBeLessThan(30);
      });
    });
  });

  describe('Multi-Operation Transaction', () => {
    it('should maintain P95 latency < 50ms for multi-statement transaction', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        instance.exec('CREATE TABLE txn_multi_a (id INTEGER PRIMARY KEY, balance INTEGER)');
        instance.exec('CREATE TABLE txn_multi_b (id INTEGER PRIMARY KEY, balance INTEGER)');
        instance.exec('INSERT INTO txn_multi_a VALUES (1, 1000)');
        instance.exec('INSERT INTO txn_multi_b VALUES (1, 1000)');

        const { latency } = await runBenchmark(
          async () => {
            const start = performance.now();
            // Simulate a transfer transaction (read-modify-write pattern)
            instance.exec('UPDATE txn_multi_a SET balance = balance - 10 WHERE id = 1');
            instance.exec('UPDATE txn_multi_b SET balance = balance + 10 WHERE id = 1');
            return { rows: [], elapsed: performance.now() - start };
          },
          { iterations: 50, warmupIterations: 10 }
        );

        expect(latency.p95).toBeLessThan(50);
      });
    });

    it('should achieve > 50 transactions per second for multi-op workload', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        instance.exec('CREATE TABLE txn_tps_a (id INTEGER PRIMARY KEY, balance INTEGER)');
        instance.exec('CREATE TABLE txn_tps_b (id INTEGER PRIMARY KEY, balance INTEGER)');
        instance.exec('INSERT INTO txn_tps_a VALUES (1, 1000)');
        instance.exec('INSERT INTO txn_tps_b VALUES (1, 1000)');

        const stats = await measureThroughput(
          () => {
            instance.exec('UPDATE txn_tps_a SET balance = balance - 1 WHERE id = 1');
            instance.exec('UPDATE txn_tps_b SET balance = balance + 1 WHERE id = 1');
          },
          { durationMs: 2000, minIterations: 50 }
        );

        expect(stats.opsPerSecond).toBeGreaterThan(50);
      });
    });
  });

  describe('Read-Modify-Write Transaction', () => {
    it('should maintain P95 latency < 40ms for read-modify-write pattern', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
        instance.exec('CREATE TABLE rmw_test (id INTEGER PRIMARY KEY, counter INTEGER)');
        instance.exec('INSERT INTO rmw_test VALUES (1, 0)');

        const { latency } = await runBenchmark(
          async () => {
            const start = performance.now();
            // Read current value
            const rows = instance.exec('SELECT counter FROM rmw_test WHERE id = 1') as Array<{ counter: number }>;
            const current = rows[0]?.counter || 0;
            // Update with new value
            instance.exec(`UPDATE rmw_test SET counter = ${current + 1} WHERE id = 1`);
            return { rows: [], elapsed: performance.now() - start };
          },
          { iterations: 50, warmupIterations: 10 }
        );

        expect(latency.p95).toBeLessThan(40);
      });
    });
  });
});

// =============================================================================
// COMPREHENSIVE REGRESSION GUARD TESTS
// =============================================================================

describe('performance regression - Comprehensive Baseline Guards', () => {
  it('should not regress on mixed workload performance', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
      await instance.setupTable('mixed_workload', 500);

      let insertId = 1000;
      let queryCount = 0;
      let insertCount = 0;
      let updateCount = 0;

      const start = performance.now();

      // Run mixed workload for 3 seconds
      while (performance.now() - start < 3000) {
        const op = Math.random();

        if (op < 0.6) {
          // 60% reads
          instance.exec('SELECT * FROM mixed_workload WHERE id = 250');
          queryCount++;
        } else if (op < 0.85) {
          // 25% inserts
          instance.exec(`INSERT INTO mixed_workload (id, name, value, data, created_at) VALUES (${insertId++}, 'new', 100, 'data', ${Date.now()})`);
          insertCount++;
        } else {
          // 15% updates
          instance.exec('UPDATE mixed_workload SET value = value + 1 WHERE id = 100');
          updateCount++;
        }
      }

      const totalTimeMs = performance.now() - start;
      const totalOps = queryCount + insertCount + updateCount;
      const opsPerSecond = (totalOps / totalTimeMs) * 1000;

      // Mixed workload should achieve at least 200 ops/sec
      expect(opsPerSecond).toBeGreaterThan(200);
    });
  });

  it('should maintain consistent latency across table sizes', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
      // Test with small table (100 rows)
      await instance.setupTable('size_100', 100);
      const small = await runBenchmark(
        () => instance.execWithTiming('SELECT * FROM size_100 WHERE id = 50'),
        { iterations: 30, warmupIterations: 5 }
      );

      // Test with medium table (1000 rows)
      await instance.setupTable('size_1000', 1000);
      const medium = await runBenchmark(
        () => instance.execWithTiming('SELECT * FROM size_1000 WHERE id = 500'),
        { iterations: 30, warmupIterations: 5 }
      );

      // Point lookups should be efficient regardless of table size (B-tree index)
      // Medium table lookup should not be more than 5x slower than small
      expect(medium.latency.p95 / (small.latency.p95 || 0.1)).toBeLessThan(5);

      // Both should meet absolute baselines
      expect(small.latency.p95).toBeLessThan(10);
      expect(medium.latency.p95).toBeLessThan(15);
    });
  });

  it('should maintain performance under sustained load', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
      await instance.setupTable('sustained', 500);

      const windowStats: number[] = [];

      // Measure 3 one-second windows
      for (let window = 0; window < 3; window++) {
        let ops = 0;
        const windowStart = performance.now();

        while (performance.now() - windowStart < 1000) {
          instance.exec('SELECT * FROM sustained WHERE id = 250');
          ops++;
        }

        windowStats.push(ops);
      }

      // All windows should achieve at least 300 ops
      const minOps = Math.min(...windowStats);
      expect(minOps).toBeGreaterThan(300);

      // No significant degradation (last window within 80% of first)
      const degradation = windowStats[2] / windowStats[0];
      expect(degradation).toBeGreaterThan(0.8);
    });
  });

  it('should not regress on write-heavy workload', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: PerformanceRegressionDO) => {
      instance.exec('CREATE TABLE write_heavy (id INTEGER PRIMARY KEY, data TEXT, counter INTEGER)');

      let id = 1;
      let totalOps = 0;
      const start = performance.now();

      // Run write-heavy workload for 2 seconds
      while (performance.now() - start < 2000) {
        // 70% inserts, 30% updates
        if (Math.random() < 0.7 || id < 10) {
          instance.exec(`INSERT INTO write_heavy (id, data, counter) VALUES (${id++}, 'data', 0)`);
        } else {
          const updateId = Math.floor(Math.random() * (id - 1)) + 1;
          instance.exec(`UPDATE write_heavy SET counter = counter + 1 WHERE id = ${updateId}`);
        }
        totalOps++;
      }

      const totalTimeMs = performance.now() - start;
      const opsPerSecond = (totalOps / totalTimeMs) * 1000;

      // Write-heavy workload should achieve at least 100 ops/sec
      expect(opsPerSecond).toBeGreaterThan(100);
    });
  });
});
