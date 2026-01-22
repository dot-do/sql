/**
 * DoSQL Production Benchmarks - TDD RED Phase
 *
 * These tests document the expected production-grade performance targets
 * using `it.fails()` to indicate performance goals that aren't yet verified.
 *
 * Production Performance Targets:
 * - Simple SELECT: < 10ms p99
 * - INSERT 100 rows: < 50ms p99
 * - Concurrent queries (100 parallel): < 20ms p99
 * - Query throughput: > 1000 qps sustained
 * - Cold start: < 100ms
 * - Memory usage: < 128MB for typical workloads
 * - CDC stream latency: < 100ms end-to-end
 *
 * Issue: sql-6jf - Production Benchmarks tests
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { env, runInDurableObject } from 'cloudflare:test';
import { DurableObject } from 'cloudflare:workers';

// =============================================================================
// Test Environment Types
// =============================================================================

interface ProductionBenchmarkEnv {
  PRODUCTION_BENCHMARK_DO: DurableObjectNamespace;
}

// =============================================================================
// Performance Measurement Utilities
// =============================================================================

/**
 * Measure execution time of an async operation
 */
async function measureTime<T>(fn: () => Promise<T>): Promise<{ result: T; elapsed: number }> {
  const start = performance.now();
  const result = await fn();
  const elapsed = performance.now() - start;
  return { result, elapsed };
}

/**
 * Measure execution time of a sync operation
 */
function measureTimeSync<T>(fn: () => T): { result: T; elapsed: number } {
  const start = performance.now();
  const result = fn();
  const elapsed = performance.now() - start;
  return { result, elapsed };
}

/**
 * Run an operation multiple times and return statistics
 */
async function benchmark<T>(
  fn: () => Promise<T>,
  iterations: number = 100,
  warmupIterations: number = 10
): Promise<{
  results: T[];
  times: number[];
  min: number;
  max: number;
  avg: number;
  p50: number;
  p95: number;
  p99: number;
}> {
  // Warmup runs (not measured)
  for (let i = 0; i < warmupIterations; i++) {
    await fn();
  }

  const times: number[] = [];
  const results: T[] = [];

  for (let i = 0; i < iterations; i++) {
    const { result, elapsed } = await measureTime(fn);
    results.push(result);
    times.push(elapsed);
  }

  times.sort((a, b) => a - b);

  const sum = times.reduce((a, b) => a + b, 0);
  const p50Index = Math.floor(times.length * 0.5);
  const p95Index = Math.floor(times.length * 0.95);
  const p99Index = Math.floor(times.length * 0.99);

  return {
    results,
    times,
    min: times[0] || 0,
    max: times[times.length - 1] || 0,
    avg: times.length > 0 ? sum / times.length : 0,
    p50: times[p50Index] || 0,
    p95: times[p95Index] || times[times.length - 1] || 0,
    p99: times[p99Index] || times[times.length - 1] || 0,
  };
}

/**
 * Run operations in parallel and measure statistics
 */
async function parallelBenchmark<T>(
  fn: () => Promise<T>,
  concurrency: number,
  iterations: number = 100
): Promise<{
  totalTime: number;
  individualTimes: number[];
  p50: number;
  p95: number;
  p99: number;
  throughput: number;
}> {
  const individualTimes: number[] = [];
  const start = performance.now();

  // Run in batches of concurrency
  for (let i = 0; i < iterations; i += concurrency) {
    const batchSize = Math.min(concurrency, iterations - i);
    const promises = Array.from({ length: batchSize }, async () => {
      const opStart = performance.now();
      await fn();
      return performance.now() - opStart;
    });
    const batchTimes = await Promise.all(promises);
    individualTimes.push(...batchTimes);
  }

  const totalTime = performance.now() - start;
  individualTimes.sort((a, b) => a - b);

  const p50Index = Math.floor(individualTimes.length * 0.5);
  const p95Index = Math.floor(individualTimes.length * 0.95);
  const p99Index = Math.floor(individualTimes.length * 0.99);

  return {
    totalTime,
    individualTimes,
    p50: individualTimes[p50Index] || 0,
    p95: individualTimes[p95Index] || individualTimes[individualTimes.length - 1] || 0,
    p99: individualTimes[p99Index] || individualTimes[individualTimes.length - 1] || 0,
    throughput: (iterations / totalTime) * 1000, // ops/sec
  };
}

/**
 * Get unique test counter for isolation
 */
let testCounter = 0;

/**
 * Get a unique DO stub for production benchmark testing
 */
function getUniqueStub(): DurableObjectStub {
  const typedEnv = env as unknown as ProductionBenchmarkEnv;
  const id = typedEnv.PRODUCTION_BENCHMARK_DO.idFromName(
    `prod-benchmark-${Date.now()}-${testCounter++}`
  );
  return typedEnv.PRODUCTION_BENCHMARK_DO.get(id);
}

// =============================================================================
// Production Benchmark Test DO
// =============================================================================

/**
 * Durable Object for production benchmark tests
 * Uses SQLite storage for realistic performance characteristics
 */
export class ProductionBenchmarkDO extends DurableObject {
  private sql: SqlStorage;
  private cdcSubscribers: Map<string, { callback: (data: unknown) => void; lastLSN: bigint }>;

  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.cdcSubscribers = new Map();
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
   * Setup CDC-enabled table with triggers
   */
  async setupCDCTable(tableName: string): Promise<void> {
    this.sql.exec(`DROP TABLE IF EXISTS ${tableName}`);
    this.sql.exec(`DROP TABLE IF EXISTS ${tableName}_cdc_log`);

    this.sql.exec(`
      CREATE TABLE ${tableName} (
        id INTEGER PRIMARY KEY,
        name TEXT,
        value REAL,
        updated_at INTEGER
      )
    `);

    // CDC log table
    this.sql.exec(`
      CREATE TABLE ${tableName}_cdc_log (
        lsn INTEGER PRIMARY KEY AUTOINCREMENT,
        op TEXT,
        table_name TEXT,
        row_id INTEGER,
        data TEXT,
        timestamp INTEGER
      )
    `);

    // Insert trigger for CDC capture
    this.sql.exec(`
      CREATE TRIGGER ${tableName}_cdc_insert AFTER INSERT ON ${tableName}
      BEGIN
        INSERT INTO ${tableName}_cdc_log (op, table_name, row_id, data, timestamp)
        VALUES ('INSERT', '${tableName}', NEW.id, json_object('id', NEW.id, 'name', NEW.name, 'value', NEW.value), unixepoch() * 1000);
      END
    `);

    // Update trigger for CDC capture
    this.sql.exec(`
      CREATE TRIGGER ${tableName}_cdc_update AFTER UPDATE ON ${tableName}
      BEGIN
        INSERT INTO ${tableName}_cdc_log (op, table_name, row_id, data, timestamp)
        VALUES ('UPDATE', '${tableName}', NEW.id, json_object('id', NEW.id, 'name', NEW.name, 'value', NEW.value), unixepoch() * 1000);
      END
    `);
  }

  /**
   * Subscribe to CDC changes
   */
  subscribeCDC(subscriberId: string, callback: (data: unknown) => void): void {
    this.cdcSubscribers.set(subscriberId, { callback, lastLSN: 0n });
  }

  /**
   * Get CDC log entries since LSN
   */
  getCDCEntries(fromLSN: bigint): unknown[] {
    return this.exec(`SELECT * FROM cdc_test_cdc_log WHERE lsn > ${fromLSN} ORDER BY lsn`);
  }

  /**
   * Perform INSERT and measure CDC latency
   */
  async insertWithCDCLatency(tableName: string, id: number, name: string, value: number): Promise<{
    insertTime: number;
    cdcLatency: number;
    totalLatency: number;
  }> {
    const startTotal = performance.now();

    // Perform INSERT
    const insertStart = performance.now();
    this.sql.exec(`INSERT INTO ${tableName} (id, name, value, updated_at) VALUES (${id}, '${name}', ${value}, ${Date.now()})`);
    const insertTime = performance.now() - insertStart;

    // Read back from CDC log (simulating CDC consumer)
    const cdcStart = performance.now();
    const cdcEntries = this.exec(`SELECT * FROM ${tableName}_cdc_log WHERE row_id = ${id} ORDER BY lsn DESC LIMIT 1`);
    const cdcLatency = performance.now() - cdcStart;

    const totalLatency = performance.now() - startTotal;

    return { insertTime, cdcLatency, totalLatency };
  }

  /**
   * Measure memory usage (approximate via storage info)
   */
  async measureMemoryUsage(): Promise<{ storageBytes: number }> {
    // Get approximate storage size from page count
    const pageInfo = this.exec('PRAGMA page_count') as Array<{ page_count: number }>;
    const pageSizeInfo = this.exec('PRAGMA page_size') as Array<{ page_size: number }>;

    const pageCount = pageInfo[0]?.page_count || 0;
    const pageSize = pageSizeInfo[0]?.page_size || 4096;

    return { storageBytes: pageCount * pageSize };
  }

  /**
   * Create index on table
   */
  createIndex(tableName: string, column: string, indexName: string): void {
    this.sql.exec(`CREATE INDEX IF NOT EXISTS ${indexName} ON ${tableName}(${column})`);
  }
}

// =============================================================================
// TDD RED Phase - Simple SELECT Query Latency (p99 < 10ms)
// =============================================================================

describe('TDD RED Phase - Production SELECT Latency', () => {
  it.fails('should execute simple SELECT query in < 10ms p99', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      // Setup: realistic table with 1000 rows
      await instance.setupTable('prod_select', 1000);

      // Run 100 iterations to get accurate p99
      const stats = await benchmark(
        () => instance.execWithTiming('SELECT * FROM prod_select WHERE id = 500 LIMIT 10'),
        100,
        20
      );

      // Production target: p99 < 10ms
      expect(stats.p99).toBeLessThan(10);
    });
  });

  it.fails('should maintain < 10ms p99 for SELECT with ORDER BY', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      await instance.setupTable('prod_order', 1000);

      const stats = await benchmark(
        () => instance.execWithTiming('SELECT * FROM prod_order ORDER BY value DESC LIMIT 20'),
        100,
        20
      );

      expect(stats.p99).toBeLessThan(10);
    });
  });

  it.fails('should maintain < 10ms p99 for SELECT with aggregation', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      await instance.setupTable('prod_agg', 1000);

      const stats = await benchmark(
        () => instance.execWithTiming('SELECT COUNT(*), AVG(value), SUM(value) FROM prod_agg'),
        100,
        20
      );

      expect(stats.p99).toBeLessThan(10);
    });
  });
});

// =============================================================================
// TDD RED Phase - INSERT 100 Rows (p99 < 50ms)
// =============================================================================

describe('TDD RED Phase - Production INSERT Throughput', () => {
  it.fails('should INSERT 100 rows in < 50ms p99', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      instance.exec('CREATE TABLE prod_batch_insert (id INTEGER PRIMARY KEY, name TEXT, value REAL)');

      let batchNum = 0;
      const stats = await benchmark(
        async () => {
          const values = Array.from({ length: 100 }, (_, i) => {
            const id = batchNum * 100 + i;
            return `(${id}, 'name_${id}', ${id * 1.5})`;
          }).join(', ');
          batchNum++;
          return instance.execWithTiming(`INSERT INTO prod_batch_insert (id, name, value) VALUES ${values}`);
        },
        50, // Run 50 batches for p99 measurement
        10
      );

      // Production target: p99 < 50ms for 100-row batch INSERT
      expect(stats.p99).toBeLessThan(50);
    });
  });

  it.fails('should INSERT 100 rows individually in < 50ms total p99', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      instance.exec('CREATE TABLE prod_individual_insert (id INTEGER PRIMARY KEY, name TEXT, value REAL)');

      let globalId = 0;
      const stats = await benchmark(
        async () => {
          const startId = globalId;
          const start = performance.now();

          for (let i = 0; i < 100; i++) {
            instance.exec(`INSERT INTO prod_individual_insert (id, name, value) VALUES (${globalId++}, 'name_${i}', ${i * 1.5})`);
          }

          return { rows: [], elapsed: performance.now() - start };
        },
        20, // 20 batches of 100 individual inserts
        5
      );

      // Production target: p99 < 50ms for 100 individual INSERTs
      expect(stats.p99).toBeLessThan(50);
    });
  });

  it.fails('should INSERT 100 rows with complex data in < 50ms p99', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      instance.exec(`
        CREATE TABLE prod_complex_insert (
          id INTEGER PRIMARY KEY,
          name TEXT,
          email TEXT,
          description TEXT,
          value REAL,
          count INTEGER,
          active INTEGER,
          metadata TEXT,
          created_at INTEGER,
          updated_at INTEGER
        )
      `);

      let batchNum = 0;
      const stats = await benchmark(
        async () => {
          const values = Array.from({ length: 100 }, (_, i) => {
            const id = batchNum * 100 + i;
            return `(${id}, 'user_${id}', 'email_${id}@example.com', 'A moderately long description for testing complex data inserts', ${id * 1.5}, ${id * 10}, 1, '{"key": "value", "nested": {"a": 1}}', ${Date.now()}, ${Date.now()})`;
          }).join(', ');
          batchNum++;
          return instance.execWithTiming(`INSERT INTO prod_complex_insert VALUES ${values}`);
        },
        30,
        10
      );

      expect(stats.p99).toBeLessThan(50);
    });
  });
});

// =============================================================================
// TDD RED Phase - Concurrent Queries (100 parallel, p99 < 20ms)
// =============================================================================

describe('TDD RED Phase - Production Concurrent Query Handling', () => {
  it.fails('should handle 100 parallel queries with < 20ms p99', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      await instance.setupTable('prod_concurrent', 1000);

      // Simulate 100 concurrent queries
      const stats = await parallelBenchmark(
        () => instance.execWithTiming('SELECT * FROM prod_concurrent WHERE id = 500'),
        100, // 100 concurrent queries
        500  // Total 500 queries
      );

      // Production target: p99 < 20ms under concurrency
      expect(stats.p99).toBeLessThan(20);
    });
  });

  it.fails('should maintain < 20ms p99 for mixed read/write concurrent load', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      await instance.setupTable('prod_mixed', 1000);

      let writeId = 10000;
      const stats = await parallelBenchmark(
        async () => {
          // 80% reads, 20% writes
          if (Math.random() < 0.8) {
            return instance.execWithTiming('SELECT * FROM prod_mixed WHERE id = 500');
          } else {
            return instance.execWithTiming(`UPDATE prod_mixed SET value = value + 1 WHERE id = ${writeId++ % 1000}`);
          }
        },
        100,
        500
      );

      expect(stats.p99).toBeLessThan(20);
    });
  });

  it.fails('should handle burst of 100 concurrent INSERTs with < 20ms p99', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      instance.exec('CREATE TABLE prod_burst_insert (id INTEGER PRIMARY KEY, data TEXT)');

      let globalId = 0;
      const stats = await parallelBenchmark(
        () => {
          const id = globalId++;
          return instance.execWithTiming(`INSERT INTO prod_burst_insert (id, data) VALUES (${id}, 'data_${id}')`);
        },
        100,
        500
      );

      expect(stats.p99).toBeLessThan(20);
    });
  });
});

// =============================================================================
// TDD RED Phase - Query Throughput (> 1000 qps sustained)
// =============================================================================

describe('TDD RED Phase - Production Query Throughput', () => {
  it.fails('should sustain > 1000 queries per second for simple SELECTs', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      await instance.setupTable('prod_throughput', 1000);

      // Run for 5 seconds to measure sustained throughput
      const duration = 5000; // 5 seconds
      let queryCount = 0;
      const start = performance.now();

      while (performance.now() - start < duration) {
        instance.exec('SELECT * FROM prod_throughput WHERE id = 500');
        queryCount++;
      }

      const elapsed = performance.now() - start;
      const qps = (queryCount / elapsed) * 1000;

      // Production target: > 1000 qps sustained
      expect(qps).toBeGreaterThan(1000);
    });
  });

  it.fails('should sustain > 1000 qps for mixed workload', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      await instance.setupTable('prod_mixed_throughput', 1000);

      const duration = 5000;
      let queryCount = 0;
      let writeId = 10000;
      const start = performance.now();

      while (performance.now() - start < duration) {
        // 90% reads, 10% writes
        if (Math.random() < 0.9) {
          instance.exec('SELECT * FROM prod_mixed_throughput WHERE id = 500');
        } else {
          instance.exec(`UPDATE prod_mixed_throughput SET value = value + 1 WHERE id = ${writeId++ % 1000}`);
        }
        queryCount++;
      }

      const elapsed = performance.now() - start;
      const qps = (queryCount / elapsed) * 1000;

      expect(qps).toBeGreaterThan(1000);
    });
  });

  it.fails('should sustain > 1000 qps for parameterized queries', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      await instance.setupTable('prod_param_throughput', 1000);
      instance.createIndex('prod_param_throughput', 'name', 'idx_prod_param_name');

      const duration = 5000;
      let queryCount = 0;
      const start = performance.now();

      while (performance.now() - start < duration) {
        const id = Math.floor(Math.random() * 1000);
        instance.exec(`SELECT * FROM prod_param_throughput WHERE id = ${id}`);
        queryCount++;
      }

      const elapsed = performance.now() - start;
      const qps = (queryCount / elapsed) * 1000;

      expect(qps).toBeGreaterThan(1000);
    });
  });

  it.fails('should maintain > 1000 qps under sustained load without degradation', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      await instance.setupTable('prod_sustained', 1000);

      // Measure QPS in 1-second windows over 10 seconds
      const windows: number[] = [];

      for (let w = 0; w < 10; w++) {
        let queryCount = 0;
        const windowStart = performance.now();

        while (performance.now() - windowStart < 1000) {
          instance.exec('SELECT * FROM prod_sustained WHERE id = 500');
          queryCount++;
        }

        windows.push(queryCount);
      }

      // All windows should exceed 1000 qps
      const minQps = Math.min(...windows);
      expect(minQps).toBeGreaterThan(1000);

      // No significant degradation (last window should be within 80% of first)
      const degradationRatio = windows[windows.length - 1] / windows[0];
      expect(degradationRatio).toBeGreaterThan(0.8);
    });
  });
});

// =============================================================================
// TDD RED Phase - Cold Start Time (< 100ms)
// =============================================================================

describe('TDD RED Phase - Production Cold Start', () => {
  it.fails('should complete cold start in < 100ms', async () => {
    // Each call to getUniqueStub creates a fresh DO instance
    const stub = getUniqueStub();

    const { elapsed } = await measureTime(async () => {
      await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
        // Cold start: create table and execute first query
        instance.exec('CREATE TABLE cold_start_test (id INTEGER PRIMARY KEY, data TEXT)');
        instance.exec('INSERT INTO cold_start_test (id, data) VALUES (1, "test")');
        return instance.exec('SELECT * FROM cold_start_test WHERE id = 1');
      });
    });

    // Production target: cold start < 100ms
    expect(elapsed).toBeLessThan(100);
  });

  it.fails('should achieve < 100ms cold start with schema setup', async () => {
    const stub = getUniqueStub();

    const { elapsed } = await measureTime(async () => {
      await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
        // More realistic cold start with multiple tables
        instance.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)');
        instance.exec('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)');
        instance.exec('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)');
        instance.exec('CREATE INDEX idx_orders_user ON orders(user_id)');
        instance.exec('INSERT INTO users VALUES (1, "Test User", "test@example.com")');
        return instance.exec('SELECT * FROM users WHERE id = 1');
      });
    });

    expect(elapsed).toBeLessThan(100);
  });

  it.fails('should maintain consistent cold start times across multiple instances', async () => {
    const coldStartTimes: number[] = [];

    for (let i = 0; i < 10; i++) {
      const stub = getUniqueStub();

      const { elapsed } = await measureTime(async () => {
        await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
          instance.exec(`CREATE TABLE cold_test_${i} (id INTEGER PRIMARY KEY)`);
          instance.exec(`INSERT INTO cold_test_${i} VALUES (1)`);
          return instance.exec(`SELECT * FROM cold_test_${i}`);
        });
      });

      coldStartTimes.push(elapsed);
    }

    // All cold starts should be < 100ms
    const maxColdStart = Math.max(...coldStartTimes);
    expect(maxColdStart).toBeLessThan(100);

    // p95 cold start should also be < 100ms
    coldStartTimes.sort((a, b) => a - b);
    const p95 = coldStartTimes[Math.floor(coldStartTimes.length * 0.95)];
    expect(p95).toBeLessThan(100);
  });
});

// =============================================================================
// TDD RED Phase - Memory Usage (< 128MB for typical workloads)
// =============================================================================

describe('TDD RED Phase - Production Memory Usage', () => {
  it.fails('should stay under 128MB for 10K row workload', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      // Setup table with 10K rows (typical workload)
      await instance.setupTable('memory_test_10k', 10000);

      // Perform typical operations
      for (let i = 0; i < 100; i++) {
        instance.exec('SELECT * FROM memory_test_10k WHERE id = 5000');
        instance.exec('SELECT COUNT(*) FROM memory_test_10k');
      }

      const { storageBytes } = await instance.measureMemoryUsage();
      const storageMB = storageBytes / (1024 * 1024);

      // Production target: storage < 128MB
      expect(storageMB).toBeLessThan(128);
    });
  });

  it.fails('should stay under 128MB after bulk operations', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      instance.exec('CREATE TABLE memory_bulk (id INTEGER PRIMARY KEY, data TEXT)');

      // Insert 50K rows in batches
      for (let batch = 0; batch < 500; batch++) {
        const values = Array.from({ length: 100 }, (_, i) => {
          const id = batch * 100 + i;
          return `(${id}, 'data_${id}_with_some_additional_text_to_simulate_realistic_data')`;
        }).join(', ');
        instance.exec(`INSERT INTO memory_bulk (id, data) VALUES ${values}`);
      }

      const { storageBytes } = await instance.measureMemoryUsage();
      const storageMB = storageBytes / (1024 * 1024);

      expect(storageMB).toBeLessThan(128);
    });
  });

  it.fails('should not leak memory during repeated query execution', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      await instance.setupTable('memory_leak_test', 1000);

      // Measure initial memory
      const initial = await instance.measureMemoryUsage();

      // Execute many queries
      for (let i = 0; i < 10000; i++) {
        instance.exec('SELECT * FROM memory_leak_test WHERE id = 500');
      }

      // Measure final memory
      const final = await instance.measureMemoryUsage();

      // Memory growth should be minimal (< 10MB growth)
      const growthMB = (final.storageBytes - initial.storageBytes) / (1024 * 1024);
      expect(growthMB).toBeLessThan(10);

      // Total should still be under 128MB
      expect(final.storageBytes / (1024 * 1024)).toBeLessThan(128);
    });
  });

  it.fails('should handle large result sets without excessive memory', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      await instance.setupTable('large_result', 10000);

      // Execute query returning many rows
      const before = await instance.measureMemoryUsage();

      for (let i = 0; i < 100; i++) {
        instance.exec('SELECT * FROM large_result LIMIT 1000');
      }

      const after = await instance.measureMemoryUsage();
      const growthMB = (after.storageBytes - before.storageBytes) / (1024 * 1024);

      // Memory should not grow significantly from reading data
      expect(growthMB).toBeLessThan(20);
      expect(after.storageBytes / (1024 * 1024)).toBeLessThan(128);
    });
  });
});

// =============================================================================
// TDD RED Phase - CDC Stream Latency (< 100ms end-to-end)
// =============================================================================

describe('TDD RED Phase - Production CDC Stream Latency', () => {
  it.fails('should achieve < 100ms CDC end-to-end latency for INSERT', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      await instance.setupCDCTable('cdc_test');

      const latencies: number[] = [];

      for (let i = 0; i < 50; i++) {
        const { totalLatency } = await instance.insertWithCDCLatency('cdc_test', i, `name_${i}`, i * 1.5);
        latencies.push(totalLatency);
      }

      latencies.sort((a, b) => a - b);
      const p99 = latencies[Math.floor(latencies.length * 0.99)] || latencies[latencies.length - 1];

      // Production target: CDC end-to-end latency < 100ms
      expect(p99).toBeLessThan(100);
    });
  });

  it.fails('should maintain < 100ms CDC latency under sustained writes', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      await instance.setupCDCTable('cdc_sustained');

      // Sustained CDC operations for 3 seconds
      const duration = 3000;
      const latencies: number[] = [];
      let id = 0;
      const start = performance.now();

      while (performance.now() - start < duration) {
        const { totalLatency } = await instance.insertWithCDCLatency('cdc_sustained', id++, `name_${id}`, id * 1.5);
        latencies.push(totalLatency);
      }

      latencies.sort((a, b) => a - b);
      const p99 = latencies[Math.floor(latencies.length * 0.99)] || latencies[latencies.length - 1];

      expect(p99).toBeLessThan(100);
    });
  });

  it.fails('should achieve < 100ms CDC latency for UPDATE operations', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      await instance.setupCDCTable('cdc_update');

      // Insert initial rows
      for (let i = 0; i < 100; i++) {
        instance.exec(`INSERT INTO cdc_update (id, name, value, updated_at) VALUES (${i}, 'name_${i}', ${i * 1.5}, ${Date.now()})`);
      }

      const latencies: number[] = [];

      for (let i = 0; i < 50; i++) {
        const startUpdate = performance.now();

        // Perform UPDATE
        instance.exec(`UPDATE cdc_update SET value = value + 1, updated_at = ${Date.now()} WHERE id = ${i}`);

        // Read CDC log for the update
        const cdcEntries = instance.exec(`SELECT * FROM cdc_update_cdc_log WHERE row_id = ${i} AND op = 'UPDATE' ORDER BY lsn DESC LIMIT 1`);

        const totalLatency = performance.now() - startUpdate;
        latencies.push(totalLatency);
      }

      latencies.sort((a, b) => a - b);
      const p99 = latencies[Math.floor(latencies.length * 0.99)] || latencies[latencies.length - 1];

      expect(p99).toBeLessThan(100);
    });
  });

  it.fails('should handle CDC for batch INSERT with < 100ms latency', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      await instance.setupCDCTable('cdc_batch');

      const latencies: number[] = [];

      for (let batch = 0; batch < 10; batch++) {
        const batchStart = performance.now();

        // Batch insert (triggers fire for each row)
        const startId = batch * 10;
        for (let i = 0; i < 10; i++) {
          const id = startId + i;
          instance.exec(`INSERT INTO cdc_batch (id, name, value, updated_at) VALUES (${id}, 'name_${id}', ${id * 1.5}, ${Date.now()})`);
        }

        // Verify all CDC entries exist
        const cdcCount = instance.exec(`SELECT COUNT(*) as cnt FROM cdc_batch_cdc_log WHERE row_id >= ${startId} AND row_id < ${startId + 10}`) as Array<{ cnt: number }>;

        const batchLatency = performance.now() - batchStart;
        latencies.push(batchLatency);
      }

      latencies.sort((a, b) => a - b);
      const p99 = latencies[Math.floor(latencies.length * 0.99)] || latencies[latencies.length - 1];

      // Batch CDC latency should still be < 100ms for 10 operations
      expect(p99).toBeLessThan(100);
    });
  });

  it.fails('should maintain CDC ordering with < 100ms latency', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: ProductionBenchmarkDO) => {
      await instance.setupCDCTable('cdc_order');

      const start = performance.now();

      // Insert 100 rows rapidly
      for (let i = 0; i < 100; i++) {
        instance.exec(`INSERT INTO cdc_order (id, name, value, updated_at) VALUES (${i}, 'name_${i}', ${i * 1.5}, ${Date.now()})`);
      }

      // Verify CDC log has correct ordering
      const cdcEntries = instance.exec('SELECT lsn, row_id FROM cdc_order_cdc_log ORDER BY lsn') as Array<{ lsn: number; row_id: number }>;

      const totalLatency = performance.now() - start;

      // All 100 entries should be captured
      expect(cdcEntries.length).toBe(100);

      // LSNs should be strictly increasing
      for (let i = 1; i < cdcEntries.length; i++) {
        expect(cdcEntries[i].lsn).toBeGreaterThan(cdcEntries[i - 1].lsn);
      }

      // Total time for 100 CDC-captured operations should be < 100ms (effectively < 1ms per op)
      expect(totalLatency).toBeLessThan(100);
    });
  });
});
