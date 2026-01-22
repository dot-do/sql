/**
 * DoSQL Performance Benchmark Runner
 *
 * Orchestrates performance benchmarks with latency metrics (P50, P95, P99).
 * Includes tests for:
 * - Query latency benchmarks (point queries, range queries)
 * - Write latency (INSERT, UPDATE, DELETE)
 * - Cold start timing
 * - Concurrent access (1, 5, 10, 20 clients)
 *
 * @packageDocumentation
 */

import {
  type BenchmarkAdapter,
  type BenchmarkOperation,
  type LatencyStats,
  type ColdStartMetrics,
  type TableSchemaConfig,
  calculateLatencyStats,
  generateTestRow,
  DEFAULT_BENCHMARK_CONFIG,
} from './types.js';

// =============================================================================
// Performance Benchmark Types
// =============================================================================

/**
 * Configuration for performance benchmarks
 */
export interface PerformanceBenchmarkConfig {
  /** Number of iterations for each benchmark */
  iterations: number;
  /** Number of warmup iterations (not counted in metrics) */
  warmupIterations: number;
  /** Row count for data setup */
  rowCount: number;
  /** Concurrency levels to test */
  concurrencyLevels: number[];
  /** Enable cold start measurement */
  measureColdStart: boolean;
  /** Table schema for benchmarks */
  schema: TableSchemaConfig;
}

/**
 * Default performance benchmark configuration
 */
export const DEFAULT_PERFORMANCE_CONFIG: PerformanceBenchmarkConfig = {
  iterations: 100,
  warmupIterations: 10,
  rowCount: 1000,
  concurrencyLevels: [1, 5, 10, 20],
  measureColdStart: true,
  schema: DEFAULT_BENCHMARK_CONFIG.schema!,
};

/**
 * Performance baseline thresholds in milliseconds
 *
 * These are the maximum acceptable latencies for each operation type.
 * Tests will fail if P95 latency exceeds these thresholds.
 */
export interface PerformanceBaselines {
  /** Point query P95 threshold (ms) */
  pointQueryP95: number;
  /** Range query P95 threshold (ms) */
  rangeQueryP95: number;
  /** Insert P95 threshold (ms) */
  insertP95: number;
  /** Update P95 threshold (ms) */
  updateP95: number;
  /** Delete P95 threshold (ms) */
  deleteP95: number;
  /** Batch insert P95 threshold (ms) */
  batchInsertP95: number;
  /** Cold start threshold (ms) */
  coldStart: number;
}

/**
 * Default performance baselines
 *
 * Point queries should complete in < 5ms at P95
 * Cold start should complete in < 50ms
 */
export const DEFAULT_BASELINES: PerformanceBaselines = {
  pointQueryP95: 5, // Point queries < 5ms P95
  rangeQueryP95: 10, // Range queries < 10ms P95
  insertP95: 10, // Insert < 10ms P95
  updateP95: 10, // Update < 10ms P95
  deleteP95: 10, // Delete < 10ms P95
  batchInsertP95: 50, // Batch insert (100 rows) < 50ms P95
  coldStart: 50, // Cold start < 50ms
};

/**
 * Result of a performance benchmark run
 */
export interface PerformanceBenchmarkResult {
  /** Operation type */
  operation: string;
  /** Latency statistics */
  latency: LatencyStats;
  /** Number of successful operations */
  successCount: number;
  /** Number of failed operations */
  errorCount: number;
  /** Operations per second */
  throughput: number;
  /** Concurrency level used */
  concurrency: number;
  /** Whether baseline was met */
  meetsBaseline: boolean;
  /** Baseline threshold used */
  baselineThreshold: number;
}

/**
 * Full performance benchmark report
 */
export interface PerformanceBenchmarkReport {
  /** Timestamp of the benchmark run */
  timestamp: string;
  /** Configuration used */
  config: PerformanceBenchmarkConfig;
  /** Baselines used */
  baselines: PerformanceBaselines;
  /** Query latency results */
  queryLatency: {
    pointQuery: PerformanceBenchmarkResult;
    rangeQuery: PerformanceBenchmarkResult;
  };
  /** Write latency results */
  writeLatency: {
    insert: PerformanceBenchmarkResult;
    update: PerformanceBenchmarkResult;
    delete: PerformanceBenchmarkResult;
    batchInsert: PerformanceBenchmarkResult;
  };
  /** Cold start metrics */
  coldStart: ColdStartMetrics & { meetsBaseline: boolean };
  /** Concurrent access results */
  concurrentAccess: PerformanceBenchmarkResult[];
  /** Overall pass/fail status */
  passed: boolean;
  /** Summary of failed baselines */
  failedBaselines: string[];
}

// =============================================================================
// Benchmark Runner Implementation
// =============================================================================

/**
 * Performance Benchmark Runner
 *
 * Executes performance benchmarks and collects latency metrics.
 */
export class PerformanceBenchmarkRunner {
  private adapter: BenchmarkAdapter;
  private config: PerformanceBenchmarkConfig;
  private baselines: PerformanceBaselines;

  constructor(
    adapter: BenchmarkAdapter,
    config: Partial<PerformanceBenchmarkConfig> = {},
    baselines: Partial<PerformanceBaselines> = {}
  ) {
    this.adapter = adapter;
    this.config = { ...DEFAULT_PERFORMANCE_CONFIG, ...config };
    this.baselines = { ...DEFAULT_BASELINES, ...baselines };
  }

  /**
   * Run all performance benchmarks
   */
  async runAll(): Promise<PerformanceBenchmarkReport> {
    const failedBaselines: string[] = [];

    // Initialize adapter
    await this.adapter.initialize();

    // Setup test data
    await this.setupTestData();

    // Run query latency benchmarks
    const pointQuery = await this.runPointQueryBenchmark();
    if (!pointQuery.meetsBaseline) {
      failedBaselines.push(`Point query P95 (${pointQuery.latency.p95.toFixed(2)}ms) exceeds baseline (${this.baselines.pointQueryP95}ms)`);
    }

    const rangeQuery = await this.runRangeQueryBenchmark();
    if (!rangeQuery.meetsBaseline) {
      failedBaselines.push(`Range query P95 (${rangeQuery.latency.p95.toFixed(2)}ms) exceeds baseline (${this.baselines.rangeQueryP95}ms)`);
    }

    // Run write latency benchmarks
    const insert = await this.runInsertBenchmark();
    if (!insert.meetsBaseline) {
      failedBaselines.push(`Insert P95 (${insert.latency.p95.toFixed(2)}ms) exceeds baseline (${this.baselines.insertP95}ms)`);
    }

    const update = await this.runUpdateBenchmark();
    if (!update.meetsBaseline) {
      failedBaselines.push(`Update P95 (${update.latency.p95.toFixed(2)}ms) exceeds baseline (${this.baselines.updateP95}ms)`);
    }

    const deleteBench = await this.runDeleteBenchmark();
    if (!deleteBench.meetsBaseline) {
      failedBaselines.push(`Delete P95 (${deleteBench.latency.p95.toFixed(2)}ms) exceeds baseline (${this.baselines.deleteP95}ms)`);
    }

    const batchInsert = await this.runBatchInsertBenchmark();
    if (!batchInsert.meetsBaseline) {
      failedBaselines.push(`Batch insert P95 (${batchInsert.latency.p95.toFixed(2)}ms) exceeds baseline (${this.baselines.batchInsertP95}ms)`);
    }

    // Run cold start measurement
    const coldStart = await this.runColdStartBenchmark();
    const coldStartMeetsBaseline = coldStart.timeToFirstQuery <= this.baselines.coldStart;
    if (!coldStartMeetsBaseline) {
      failedBaselines.push(`Cold start (${coldStart.timeToFirstQuery.toFixed(2)}ms) exceeds baseline (${this.baselines.coldStart}ms)`);
    }

    // Run concurrent access benchmarks
    const concurrentAccess = await this.runConcurrentAccessBenchmarks();

    // Cleanup
    await this.adapter.cleanup();

    return {
      timestamp: new Date().toISOString(),
      config: this.config,
      baselines: this.baselines,
      queryLatency: {
        pointQuery,
        rangeQuery,
      },
      writeLatency: {
        insert,
        update,
        delete: deleteBench,
        batchInsert,
      },
      coldStart: {
        ...coldStart,
        meetsBaseline: coldStartMeetsBaseline,
      },
      concurrentAccess,
      passed: failedBaselines.length === 0,
      failedBaselines,
    };
  }

  // ===========================================================================
  // Test Data Setup
  // ===========================================================================

  /**
   * Setup test data for benchmarks
   */
  private async setupTestData(): Promise<void> {
    // Drop existing table if any
    await this.adapter.dropTable(this.config.schema.tableName);

    // Create table
    await this.adapter.createTable(this.config.schema);

    // Insert test data
    const rows: Record<string, unknown>[] = [];
    for (let i = 1; i <= this.config.rowCount; i++) {
      rows.push(generateTestRow(i, this.config.schema));
    }

    // Batch insert in chunks of 100
    const chunkSize = 100;
    for (let i = 0; i < rows.length; i += chunkSize) {
      const chunk = rows.slice(i, i + chunkSize);
      await this.adapter.insertBatch(this.config.schema.tableName, chunk);
    }
  }

  // ===========================================================================
  // Query Latency Benchmarks
  // ===========================================================================

  /**
   * Run point query benchmark (SELECT by primary key)
   */
  async runPointQueryBenchmark(): Promise<PerformanceBenchmarkResult> {
    const durations: number[] = [];
    let successCount = 0;
    let errorCount = 0;

    // Warmup
    for (let i = 0; i < this.config.warmupIterations; i++) {
      const id = Math.floor(Math.random() * this.config.rowCount) + 1;
      await this.adapter.read(this.config.schema.tableName, 'id', id);
    }

    // Benchmark iterations
    for (let i = 0; i < this.config.iterations; i++) {
      const id = Math.floor(Math.random() * this.config.rowCount) + 1;
      const result = await this.adapter.read(this.config.schema.tableName, 'id', id);

      if (result.success) {
        durations.push(result.durationMs);
        successCount++;
      } else {
        errorCount++;
      }
    }

    const latency = calculateLatencyStats(durations);
    const totalDuration = durations.reduce((a, b) => a + b, 0);
    const throughput = (successCount / totalDuration) * 1000;

    return {
      operation: 'point-query',
      latency,
      successCount,
      errorCount,
      throughput,
      concurrency: 1,
      meetsBaseline: latency.p95 <= this.baselines.pointQueryP95,
      baselineThreshold: this.baselines.pointQueryP95,
    };
  }

  /**
   * Run range query benchmark (SELECT with WHERE clause)
   */
  async runRangeQueryBenchmark(): Promise<PerformanceBenchmarkResult> {
    const durations: number[] = [];
    let successCount = 0;
    let errorCount = 0;

    // Warmup
    for (let i = 0; i < this.config.warmupIterations; i++) {
      await this.adapter.readMany(
        this.config.schema.tableName,
        'id BETWEEN ? AND ?',
        { start: 1, end: 100 }
      );
    }

    // Benchmark iterations
    for (let i = 0; i < this.config.iterations; i++) {
      const start = Math.floor(Math.random() * (this.config.rowCount - 100)) + 1;
      const result = await this.adapter.readMany(
        this.config.schema.tableName,
        'id BETWEEN ? AND ?',
        { start, end: start + 99 }
      );

      if (result.success) {
        durations.push(result.durationMs);
        successCount++;
      } else {
        errorCount++;
      }
    }

    const latency = calculateLatencyStats(durations);
    const totalDuration = durations.reduce((a, b) => a + b, 0);
    const throughput = (successCount / totalDuration) * 1000;

    return {
      operation: 'range-query',
      latency,
      successCount,
      errorCount,
      throughput,
      concurrency: 1,
      meetsBaseline: latency.p95 <= this.baselines.rangeQueryP95,
      baselineThreshold: this.baselines.rangeQueryP95,
    };
  }

  // ===========================================================================
  // Write Latency Benchmarks
  // ===========================================================================

  /**
   * Run INSERT benchmark
   */
  async runInsertBenchmark(): Promise<PerformanceBenchmarkResult> {
    const durations: number[] = [];
    let successCount = 0;
    let errorCount = 0;

    // Start ID after existing data
    const startId = this.config.rowCount + 1;

    // Warmup
    for (let i = 0; i < this.config.warmupIterations; i++) {
      const row = generateTestRow(startId + i, this.config.schema);
      await this.adapter.insert(this.config.schema.tableName, row);
    }

    // Benchmark iterations
    for (let i = 0; i < this.config.iterations; i++) {
      const row = generateTestRow(startId + this.config.warmupIterations + i, this.config.schema);
      const result = await this.adapter.insert(this.config.schema.tableName, row);

      if (result.success) {
        durations.push(result.durationMs);
        successCount++;
      } else {
        errorCount++;
      }
    }

    const latency = calculateLatencyStats(durations);
    const totalDuration = durations.reduce((a, b) => a + b, 0);
    const throughput = (successCount / totalDuration) * 1000;

    return {
      operation: 'insert',
      latency,
      successCount,
      errorCount,
      throughput,
      concurrency: 1,
      meetsBaseline: latency.p95 <= this.baselines.insertP95,
      baselineThreshold: this.baselines.insertP95,
    };
  }

  /**
   * Run UPDATE benchmark
   */
  async runUpdateBenchmark(): Promise<PerformanceBenchmarkResult> {
    const durations: number[] = [];
    let successCount = 0;
    let errorCount = 0;

    // Warmup
    for (let i = 0; i < this.config.warmupIterations; i++) {
      const id = Math.floor(Math.random() * this.config.rowCount) + 1;
      await this.adapter.update(
        this.config.schema.tableName,
        'id',
        id,
        { value: Math.random() * 1000 }
      );
    }

    // Benchmark iterations
    for (let i = 0; i < this.config.iterations; i++) {
      const id = Math.floor(Math.random() * this.config.rowCount) + 1;
      const result = await this.adapter.update(
        this.config.schema.tableName,
        'id',
        id,
        { value: Math.random() * 1000, name: `updated-${i}` }
      );

      if (result.success) {
        durations.push(result.durationMs);
        successCount++;
      } else {
        errorCount++;
      }
    }

    const latency = calculateLatencyStats(durations);
    const totalDuration = durations.reduce((a, b) => a + b, 0);
    const throughput = (successCount / totalDuration) * 1000;

    return {
      operation: 'update',
      latency,
      successCount,
      errorCount,
      throughput,
      concurrency: 1,
      meetsBaseline: latency.p95 <= this.baselines.updateP95,
      baselineThreshold: this.baselines.updateP95,
    };
  }

  /**
   * Run DELETE benchmark
   */
  async runDeleteBenchmark(): Promise<PerformanceBenchmarkResult> {
    const durations: number[] = [];
    let successCount = 0;
    let errorCount = 0;

    // Insert rows to delete
    const deleteStartId = this.config.rowCount + 10000;
    const rowsToDelete: Record<string, unknown>[] = [];
    for (let i = 0; i < this.config.warmupIterations + this.config.iterations; i++) {
      rowsToDelete.push(generateTestRow(deleteStartId + i, this.config.schema));
    }
    await this.adapter.insertBatch(this.config.schema.tableName, rowsToDelete);

    // Warmup
    for (let i = 0; i < this.config.warmupIterations; i++) {
      await this.adapter.delete(this.config.schema.tableName, 'id', deleteStartId + i);
    }

    // Benchmark iterations
    for (let i = 0; i < this.config.iterations; i++) {
      const result = await this.adapter.delete(
        this.config.schema.tableName,
        'id',
        deleteStartId + this.config.warmupIterations + i
      );

      if (result.success) {
        durations.push(result.durationMs);
        successCount++;
      } else {
        errorCount++;
      }
    }

    const latency = calculateLatencyStats(durations);
    const totalDuration = durations.reduce((a, b) => a + b, 0);
    const throughput = (successCount / totalDuration) * 1000;

    return {
      operation: 'delete',
      latency,
      successCount,
      errorCount,
      throughput,
      concurrency: 1,
      meetsBaseline: latency.p95 <= this.baselines.deleteP95,
      baselineThreshold: this.baselines.deleteP95,
    };
  }

  /**
   * Run batch INSERT benchmark (100 rows per batch)
   */
  async runBatchInsertBenchmark(): Promise<PerformanceBenchmarkResult> {
    const durations: number[] = [];
    let successCount = 0;
    let errorCount = 0;
    const batchSize = 100;

    // Start ID after existing data
    let currentId = this.config.rowCount + 20000;

    // Warmup
    for (let i = 0; i < this.config.warmupIterations; i++) {
      const rows: Record<string, unknown>[] = [];
      for (let j = 0; j < batchSize; j++) {
        rows.push(generateTestRow(currentId++, this.config.schema));
      }
      await this.adapter.insertBatch(this.config.schema.tableName, rows);
    }

    // Benchmark iterations
    for (let i = 0; i < this.config.iterations; i++) {
      const rows: Record<string, unknown>[] = [];
      for (let j = 0; j < batchSize; j++) {
        rows.push(generateTestRow(currentId++, this.config.schema));
      }

      const result = await this.adapter.insertBatch(this.config.schema.tableName, rows);

      if (result.success) {
        durations.push(result.durationMs);
        successCount++;
      } else {
        errorCount++;
      }
    }

    const latency = calculateLatencyStats(durations);
    const totalDuration = durations.reduce((a, b) => a + b, 0);
    const throughput = (successCount / totalDuration) * 1000;

    return {
      operation: 'batch-insert',
      latency,
      successCount,
      errorCount,
      throughput,
      concurrency: 1,
      meetsBaseline: latency.p95 <= this.baselines.batchInsertP95,
      baselineThreshold: this.baselines.batchInsertP95,
    };
  }

  // ===========================================================================
  // Cold Start Benchmark
  // ===========================================================================

  /**
   * Run cold start benchmark
   */
  async runColdStartBenchmark(): Promise<ColdStartMetrics> {
    return this.adapter.measureColdStart();
  }

  // ===========================================================================
  // Concurrent Access Benchmarks
  // ===========================================================================

  /**
   * Run concurrent access benchmarks at different concurrency levels
   */
  async runConcurrentAccessBenchmarks(): Promise<PerformanceBenchmarkResult[]> {
    const results: PerformanceBenchmarkResult[] = [];

    for (const concurrency of this.config.concurrencyLevels) {
      const result = await this.runConcurrentPointQueries(concurrency);
      results.push(result);
    }

    return results;
  }

  /**
   * Run concurrent point queries at a specific concurrency level
   */
  private async runConcurrentPointQueries(
    concurrency: number
  ): Promise<PerformanceBenchmarkResult> {
    const durations: number[] = [];
    let successCount = 0;
    let errorCount = 0;

    // Number of batches of concurrent operations
    const batches = Math.ceil(this.config.iterations / concurrency);

    for (let batch = 0; batch < batches; batch++) {
      const promises: Promise<BenchmarkOperation>[] = [];

      for (let i = 0; i < concurrency; i++) {
        const id = Math.floor(Math.random() * this.config.rowCount) + 1;
        promises.push(
          this.adapter.read(this.config.schema.tableName, 'id', id)
        );
      }

      const results = await Promise.all(promises);

      for (const result of results) {
        if (result.success) {
          durations.push(result.durationMs);
          successCount++;
        } else {
          errorCount++;
        }
      }
    }

    const latency = calculateLatencyStats(durations);
    const totalDuration = durations.reduce((a, b) => a + b, 0);
    const throughput = (successCount / (totalDuration / concurrency)) * 1000;

    return {
      operation: `concurrent-point-query-${concurrency}`,
      latency,
      successCount,
      errorCount,
      throughput,
      concurrency,
      // Concurrent queries should still meet point query baseline
      meetsBaseline: latency.p95 <= this.baselines.pointQueryP95 * 2, // Allow 2x for concurrency
      baselineThreshold: this.baselines.pointQueryP95 * 2,
    };
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a new performance benchmark runner
 */
export function createPerformanceBenchmarkRunner(
  adapter: BenchmarkAdapter,
  config?: Partial<PerformanceBenchmarkConfig>,
  baselines?: Partial<PerformanceBaselines>
): PerformanceBenchmarkRunner {
  return new PerformanceBenchmarkRunner(adapter, config, baselines);
}
