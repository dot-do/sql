/**
 * Production Benchmark Harness for DoSQL
 *
 * Core benchmark execution engine that runs scenarios and collects metrics.
 * Designed to run on real Cloudflare Workers in production.
 */

import {
  type LatencyMetrics,
  type ThroughputMetrics,
  type ColdStartMetrics,
  type MemoryMetrics,
  type ScenarioConfig,
  type ScenarioResult,
  type ScenarioBaseline,
  type BenchmarkConfig,
  type BenchmarkBaselines,
  type BenchmarkReport,
  type BenchmarkSummary,
  type ReportMetadata,
  type RuntimeInfo,
  DEFAULT_BASELINES,
  createTimer,
  stopTimer,
  calculateLatencyMetrics,
  calculateThroughputMetrics,
} from './types.js';

// =============================================================================
// Benchmark Adapter Interface
// =============================================================================

/**
 * Interface for database adapters that can be benchmarked
 */
export interface BenchmarkableAdapter {
  /** Initialize the adapter */
  initialize(): Promise<void>;
  /** Clean up resources */
  cleanup(): Promise<void>;
  /** Execute a simple SELECT query */
  select(tableName: string, id: number): Promise<{ durationMs: number; success: boolean; error?: string }>;
  /** Execute a SELECT with range */
  selectRange(tableName: string, startId: number, endId: number): Promise<{ durationMs: number; success: boolean; error?: string; rowCount: number }>;
  /** Execute an INSERT */
  insert(tableName: string, row: Record<string, unknown>): Promise<{ durationMs: number; success: boolean; error?: string }>;
  /** Execute a batch INSERT */
  insertBatch(tableName: string, rows: Record<string, unknown>[]): Promise<{ durationMs: number; success: boolean; error?: string }>;
  /** Execute an UPDATE */
  update(tableName: string, id: number, updates: Record<string, unknown>): Promise<{ durationMs: number; success: boolean; error?: string }>;
  /** Execute a DELETE */
  delete(tableName: string, id: number): Promise<{ durationMs: number; success: boolean; error?: string }>;
  /** Create table for benchmarks */
  createTable(tableName: string): Promise<void>;
  /** Drop table */
  dropTable(tableName: string): Promise<void>;
  /** Seed data */
  seedData(tableName: string, rowCount: number): Promise<void>;
  /** Measure cold start */
  measureColdStart(): Promise<ColdStartMetrics>;
  /** Get memory usage (if available) */
  getMemoryUsage?(): MemoryMetrics;
}

// =============================================================================
// Benchmark Harness
// =============================================================================

/**
 * Production Benchmark Harness
 *
 * Orchestrates benchmark execution and metrics collection.
 */
export class BenchmarkHarness {
  private adapter: BenchmarkableAdapter;
  private config: BenchmarkConfig;
  private results: ScenarioResult[] = [];
  private coldStart?: ColdStartMetrics;
  private startTime: number = 0;

  constructor(adapter: BenchmarkableAdapter, config: Partial<BenchmarkConfig> = {}) {
    this.adapter = adapter;
    this.config = {
      name: config.name ?? 'DoSQL Production Benchmark',
      environment: config.environment ?? 'local',
      workerUrl: config.workerUrl,
      scenarios: config.scenarios ?? this.getDefaultScenarios(),
      baselines: { ...DEFAULT_BASELINES, ...config.baselines },
      measureColdStart: config.measureColdStart ?? true,
      measureMemory: config.measureMemory ?? true,
      measureCDC: config.measureCDC ?? false,
      outputFormat: config.outputFormat ?? 'json',
      outputPath: config.outputPath,
      verbose: config.verbose ?? false,
    };
  }

  /**
   * Get default scenarios
   */
  private getDefaultScenarios(): ScenarioConfig[] {
    return [
      {
        type: 'simple-select',
        name: 'Simple SELECT',
        description: 'Point query by primary key',
        iterations: 100,
        warmupIterations: 10,
        rowCount: 1000,
      },
      {
        type: 'insert-single',
        name: 'Single INSERT',
        description: 'Insert single row',
        iterations: 100,
        warmupIterations: 10,
        rowCount: 0,
      },
      {
        type: 'insert-batch',
        name: 'Batch INSERT (100 rows)',
        description: 'Insert 100 rows in a batch',
        iterations: 50,
        warmupIterations: 5,
        rowCount: 0,
        batchSize: 100,
      },
      {
        type: 'concurrent-read',
        name: 'Concurrent Reads (10x)',
        description: '10 concurrent SELECT queries',
        iterations: 50,
        warmupIterations: 5,
        rowCount: 1000,
        concurrency: 10,
      },
    ];
  }

  /**
   * Run all benchmarks
   */
  async runAll(): Promise<BenchmarkReport> {
    this.startTime = Date.now();
    this.results = [];

    // Initialize adapter
    await this.adapter.initialize();

    this.log('Starting benchmark suite:', this.config.name);

    // Measure cold start if enabled
    if (this.config.measureColdStart) {
      this.log('Measuring cold start...');
      this.coldStart = await this.adapter.measureColdStart();
      this.log(`Cold start: ${this.coldStart.totalMs.toFixed(2)}ms`);
    }

    // Run each scenario
    for (const scenario of this.config.scenarios) {
      this.log(`Running scenario: ${scenario.name}`);
      const result = await this.runScenario(scenario);
      this.results.push(result);
      this.log(`  P95: ${result.latency.p95.toFixed(2)}ms, Throughput: ${result.throughput.queriesPerSecond.toFixed(2)} ops/sec`);
    }

    // Cleanup
    await this.adapter.cleanup();

    // Generate report
    return this.generateReport();
  }

  /**
   * Run a single scenario
   */
  async runScenario(scenario: ScenarioConfig): Promise<ScenarioResult> {
    const tableName = `benchmark_${scenario.type.replace(/-/g, '_')}`;
    const timings: number[] = [];
    const errors: string[] = [];
    let successCount = 0;
    let errorCount = 0;

    // Get memory at start
    const memoryStart = this.config.measureMemory && this.adapter.getMemoryUsage
      ? this.adapter.getMemoryUsage()
      : undefined;

    const startedAt = new Date().toISOString();

    // Setup: Create table and seed data
    await this.adapter.dropTable(tableName);
    await this.adapter.createTable(tableName);
    if (scenario.rowCount > 0) {
      await this.adapter.seedData(tableName, scenario.rowCount);
    }

    // Warmup
    for (let i = 0; i < scenario.warmupIterations; i++) {
      await this.executeScenarioOperation(scenario, tableName, i);
    }

    // Run iterations
    for (let i = 0; i < scenario.iterations; i++) {
      const result = await this.executeScenarioOperation(scenario, tableName, i + scenario.rowCount + scenario.warmupIterations);

      if (result.success) {
        timings.push(result.durationMs);
        successCount++;
      } else {
        errorCount++;
        if (result.error) {
          errors.push(result.error);
        }
      }
    }

    const completedAt = new Date().toISOString();

    // Get memory at end
    const memoryEnd = this.config.measureMemory && this.adapter.getMemoryUsage
      ? this.adapter.getMemoryUsage()
      : undefined;

    // Calculate metrics
    const latency = calculateLatencyMetrics(timings);
    const throughput = calculateThroughputMetrics(timings, successCount, errorCount);

    // Get baseline for this scenario
    const baseline = this.getBaselineForScenario(scenario);

    // Check if baseline is met
    const meetsBaseline =
      latency.p95 <= baseline.p95Ms &&
      throughput.errorRate <= baseline.maxErrorRate &&
      (baseline.minThroughput === undefined || throughput.queriesPerSecond >= baseline.minThroughput);

    // Build memory metrics
    const memory: MemoryMetrics | undefined = memoryStart && memoryEnd
      ? {
          heapUsedStart: memoryStart.heapUsedStart,
          heapUsedEnd: memoryEnd.heapUsedEnd,
          heapTotal: memoryEnd.heapTotal,
          external: memoryEnd.external,
          rss: memoryEnd.rss,
          memoryDelta: memoryEnd.heapUsedEnd - memoryStart.heapUsedStart,
          peakMemory: Math.max(memoryStart.peakMemory, memoryEnd.peakMemory),
        }
      : undefined;

    return {
      config: scenario,
      latency,
      throughput,
      memory,
      meetsBaseline,
      baseline,
      startedAt,
      completedAt,
      rawTimings: timings,
      errors: errors.slice(0, 10), // Limit to first 10 errors
    };
  }

  /**
   * Execute a single operation for a scenario
   */
  private async executeScenarioOperation(
    scenario: ScenarioConfig,
    tableName: string,
    iteration: number
  ): Promise<{ durationMs: number; success: boolean; error?: string }> {
    switch (scenario.type) {
      case 'simple-select': {
        const id = (iteration % (scenario.rowCount || 1000)) + 1;
        return this.adapter.select(tableName, id);
      }

      case 'insert-single': {
        const row = this.generateRow(iteration);
        return this.adapter.insert(tableName, row);
      }

      case 'insert-batch': {
        const batchSize = scenario.batchSize ?? 100;
        const rows = Array.from({ length: batchSize }, (_, i) =>
          this.generateRow(iteration * batchSize + i)
        );
        return this.adapter.insertBatch(tableName, rows);
      }

      case 'concurrent-read': {
        const concurrency = scenario.concurrency ?? 10;
        const rowCount = scenario.rowCount || 1000;
        const timer = createTimer();

        const promises = Array.from({ length: concurrency }, (_, i) => {
          const id = ((iteration * concurrency + i) % rowCount) + 1;
          return this.adapter.select(tableName, id);
        });

        const results = await Promise.all(promises);
        const durationMs = stopTimer(timer);

        const success = results.every((r) => r.success);
        const error = results.find((r) => !r.success)?.error;

        return { durationMs, success, error };
      }

      case 'concurrent-write': {
        const concurrency = scenario.concurrency ?? 5;
        const timer = createTimer();

        const promises = Array.from({ length: concurrency }, (_, i) => {
          const row = this.generateRow(iteration * concurrency + i);
          return this.adapter.insert(tableName, row);
        });

        const results = await Promise.all(promises);
        const durationMs = stopTimer(timer);

        const success = results.every((r) => r.success);
        const error = results.find((r) => !r.success)?.error;

        return { durationMs, success, error };
      }

      case 'mixed-workload': {
        // 70% reads, 30% writes
        const isRead = Math.random() < 0.7;
        if (isRead) {
          const id = (iteration % (scenario.rowCount || 1000)) + 1;
          return this.adapter.select(tableName, id);
        } else {
          const row = this.generateRow(iteration);
          return this.adapter.insert(tableName, row);
        }
      }

      default:
        return { durationMs: 0, success: false, error: `Unknown scenario type: ${scenario.type}` };
    }
  }

  /**
   * Generate a test row
   */
  private generateRow(id: number): Record<string, unknown> {
    return {
      id,
      name: `test-row-${id}`,
      value: Math.random() * 1000,
      data: `data-${id}-${Date.now()}`,
      created_at: Date.now(),
    };
  }

  /**
   * Get baseline for a scenario
   */
  private getBaselineForScenario(scenario: ScenarioConfig): ScenarioBaseline {
    const baselines = this.config.baselines;

    switch (scenario.type) {
      case 'simple-select':
        return {
          p95Ms: baselines.simpleSelectP95,
          p99Ms: baselines.simpleSelectP95 * 2,
          maxErrorRate: baselines.maxErrorRate,
          minThroughput: 100,
        };

      case 'insert-single':
        return {
          p95Ms: baselines.insertP95,
          p99Ms: baselines.insertP95 * 2,
          maxErrorRate: baselines.maxErrorRate,
        };

      case 'insert-batch':
        return {
          p95Ms: baselines.batchInsertP95,
          p99Ms: baselines.batchInsertP95 * 2,
          maxErrorRate: baselines.maxErrorRate,
        };

      case 'concurrent-read':
        return {
          p95Ms: baselines.concurrentReadP95,
          p99Ms: baselines.concurrentReadP95 * 2,
          maxErrorRate: baselines.maxErrorRate,
        };

      case 'concurrent-write':
        return {
          p95Ms: baselines.insertP95 * 2, // Allow 2x for concurrent writes
          p99Ms: baselines.insertP95 * 4,
          maxErrorRate: baselines.maxErrorRate,
        };

      case 'mixed-workload':
        return {
          p95Ms: baselines.simpleSelectP95 * 2, // Weighted average
          p99Ms: baselines.simpleSelectP95 * 4,
          maxErrorRate: baselines.maxErrorRate,
        };

      default:
        return {
          p95Ms: 100, // Default fallback
          p99Ms: 200,
          maxErrorRate: baselines.maxErrorRate,
        };
    }
  }

  /**
   * Generate the benchmark report
   */
  private generateReport(): BenchmarkReport {
    const endTime = Date.now();
    const durationSeconds = (endTime - this.startTime) / 1000;

    const failedBaselines: string[] = [];

    // Check cold start baseline
    const coldStartMeetsBaseline = this.coldStart
      ? this.coldStart.totalMs <= this.config.baselines.coldStartMs
      : true;

    if (this.coldStart && !coldStartMeetsBaseline) {
      failedBaselines.push(
        `Cold start (${this.coldStart.totalMs.toFixed(2)}ms) exceeds baseline (${this.config.baselines.coldStartMs}ms)`
      );
    }

    // Check scenario baselines
    for (const result of this.results) {
      if (!result.meetsBaseline) {
        failedBaselines.push(
          `${result.config.name} P95 (${result.latency.p95.toFixed(2)}ms) exceeds baseline (${result.baseline.p95Ms}ms)`
        );
      }
    }

    // Calculate summary
    const summary = this.calculateSummary();

    // Build runtime info
    const runtime = this.detectRuntime();

    return {
      metadata: {
        name: this.config.name,
        timestamp: new Date().toISOString(),
        durationSeconds,
        environment: this.config.environment,
        workerUrl: this.config.workerUrl,
        runtime,
      },
      coldStart: this.coldStart
        ? { ...this.coldStart, meetsBaseline: coldStartMeetsBaseline }
        : undefined,
      scenarios: this.results,
      passed: failedBaselines.length === 0,
      failedBaselines,
      summary,
    };
  }

  /**
   * Calculate benchmark summary
   */
  private calculateSummary(): BenchmarkSummary {
    const passedScenarios = this.results.filter((r) => r.meetsBaseline).length;
    const failedScenarios = this.results.length - passedScenarios;

    const totalOperations = this.results.reduce(
      (sum, r) => sum + r.throughput.totalOperations,
      0
    );
    const totalErrors = this.results.reduce(
      (sum, r) => sum + r.throughput.errorCount,
      0
    );

    const p95Values = this.results.map((r) => r.latency.p95);
    const averageP95Ms =
      p95Values.length > 0
        ? p95Values.reduce((a, b) => a + b, 0) / p95Values.length
        : 0;

    const throughputValues = this.results.map((r) => r.throughput.queriesPerSecond);
    const averageThroughput =
      throughputValues.length > 0
        ? throughputValues.reduce((a, b) => a + b, 0) / throughputValues.length
        : 0;

    return {
      totalScenarios: this.results.length,
      passedScenarios,
      failedScenarios,
      totalOperations,
      totalErrors,
      errorRate: totalOperations > 0 ? totalErrors / totalOperations : 0,
      averageP95Ms,
      averageThroughput,
    };
  }

  /**
   * Detect runtime environment
   */
  private detectRuntime(): RuntimeInfo {
    // Check if we're in Workers environment
    if (typeof globalThis !== 'undefined' && 'caches' in globalThis) {
      return {
        type: 'workers',
        platform: 'cloudflare',
      };
    }

    // Check if we're in Node.js
    if (typeof process !== 'undefined' && process.version) {
      return {
        type: 'node',
        version: process.version,
        platform: process.platform,
      };
    }

    // Default to miniflare (test environment)
    return {
      type: 'miniflare',
    };
  }

  /**
   * Log message if verbose mode is enabled
   */
  private log(...args: unknown[]): void {
    if (this.config.verbose) {
      console.log('[Benchmark]', ...args);
    }
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a new benchmark harness
 */
export function createBenchmarkHarness(
  adapter: BenchmarkableAdapter,
  config?: Partial<BenchmarkConfig>
): BenchmarkHarness {
  return new BenchmarkHarness(adapter, config);
}
