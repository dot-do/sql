/**
 * Production Benchmark Types for DoSQL
 *
 * Type definitions for the production benchmark harness that measures
 * real-world performance on Cloudflare Workers.
 */

// =============================================================================
// Metric Types
// =============================================================================

/**
 * Latency statistics with percentiles
 */
export interface LatencyMetrics {
  /** Minimum latency in milliseconds */
  min: number;
  /** Maximum latency in milliseconds */
  max: number;
  /** Mean latency in milliseconds */
  mean: number;
  /** 50th percentile (median) latency in milliseconds */
  p50: number;
  /** 95th percentile latency in milliseconds */
  p95: number;
  /** 99th percentile latency in milliseconds */
  p99: number;
  /** Standard deviation in milliseconds */
  stdDev: number;
  /** Sample count */
  count: number;
}

/**
 * Throughput statistics
 */
export interface ThroughputMetrics {
  /** Operations per second */
  queriesPerSecond: number;
  /** Total operations completed */
  totalOperations: number;
  /** Total duration in milliseconds */
  totalDurationMs: number;
  /** Successful operations */
  successCount: number;
  /** Failed operations */
  errorCount: number;
  /** Error rate (0-1) */
  errorRate: number;
}

/**
 * Cold start metrics
 */
export interface ColdStartMetrics {
  /** Time to first query in milliseconds */
  timeToFirstQueryMs: number;
  /** Time for DO initialization in milliseconds */
  initializationMs: number;
  /** Time for connection establishment in milliseconds */
  connectionMs: number;
  /** Total cold start time in milliseconds */
  totalMs: number;
}

/**
 * Memory usage metrics
 */
export interface MemoryMetrics {
  /** Heap used in bytes (at start) */
  heapUsedStart: number;
  /** Heap used in bytes (at end) */
  heapUsedEnd: number;
  /** Heap total in bytes */
  heapTotal: number;
  /** External memory in bytes */
  external: number;
  /** RSS (Resident Set Size) in bytes - if available */
  rss?: number;
  /** Memory delta (end - start) in bytes */
  memoryDelta: number;
  /** Peak memory usage in bytes */
  peakMemory: number;
}

/**
 * CDC stream latency metrics
 */
export interface CDCLatencyMetrics {
  /** Write to CDC event latency in milliseconds */
  writeToEventLatency: LatencyMetrics;
  /** End-to-end latency from write to consumer receipt */
  endToEndLatency: LatencyMetrics;
  /** Events processed per second */
  eventsPerSecond: number;
  /** Total events captured */
  totalEvents: number;
  /** Events lost/missed */
  lostEvents: number;
  /** Loss rate (0-1) */
  lossRate: number;
}

// =============================================================================
// Scenario Types
// =============================================================================

/**
 * Benchmark scenario type
 */
export type ScenarioType =
  | 'simple-select'
  | 'insert-single'
  | 'insert-batch'
  | 'concurrent-read'
  | 'concurrent-write'
  | 'mixed-workload'
  | 'cdc-streaming';

/**
 * Benchmark scenario configuration
 */
export interface ScenarioConfig {
  /** Scenario type */
  type: ScenarioType;
  /** Human-readable name */
  name: string;
  /** Description */
  description: string;
  /** Number of iterations */
  iterations: number;
  /** Warmup iterations (not counted in metrics) */
  warmupIterations: number;
  /** Row count for data setup */
  rowCount: number;
  /** Concurrency level (for concurrent scenarios) */
  concurrency?: number;
  /** Batch size (for batch operations) */
  batchSize?: number;
  /** Duration in seconds (for time-based scenarios) */
  durationSeconds?: number;
  /** Custom parameters */
  params?: Record<string, unknown>;
}

/**
 * Scenario result
 */
export interface ScenarioResult {
  /** Scenario configuration */
  config: ScenarioConfig;
  /** Latency metrics */
  latency: LatencyMetrics;
  /** Throughput metrics */
  throughput: ThroughputMetrics;
  /** Memory metrics (if available) */
  memory?: MemoryMetrics;
  /** Whether baseline was met */
  meetsBaseline: boolean;
  /** Baseline threshold that was used */
  baseline: ScenarioBaseline;
  /** Start timestamp */
  startedAt: string;
  /** End timestamp */
  completedAt: string;
  /** Raw timings for detailed analysis */
  rawTimings: number[];
  /** Errors encountered */
  errors: string[];
}

/**
 * Scenario baseline thresholds
 */
export interface ScenarioBaseline {
  /** P95 latency threshold in milliseconds */
  p95Ms: number;
  /** P99 latency threshold in milliseconds */
  p99Ms: number;
  /** Maximum error rate (0-1) */
  maxErrorRate: number;
  /** Minimum throughput (ops/sec) */
  minThroughput?: number;
}

// =============================================================================
// Benchmark Configuration
// =============================================================================

/**
 * Full benchmark suite configuration
 */
export interface BenchmarkConfig {
  /** Suite name */
  name: string;
  /** Environment (production, staging, local) */
  environment: 'production' | 'staging' | 'local' | 'test';
  /** Worker URL for HTTP-based benchmarks */
  workerUrl?: string;
  /** Scenarios to run */
  scenarios: ScenarioConfig[];
  /** Global baselines */
  baselines: BenchmarkBaselines;
  /** Enable cold start measurement */
  measureColdStart: boolean;
  /** Enable memory measurement */
  measureMemory: boolean;
  /** Enable CDC latency measurement */
  measureCDC: boolean;
  /** Output format */
  outputFormat: 'json' | 'markdown' | 'csv' | 'console';
  /** Output file path (optional) */
  outputPath?: string;
  /** Verbose logging */
  verbose: boolean;
}

/**
 * Global benchmark baselines
 */
export interface BenchmarkBaselines {
  /** Simple SELECT P95 threshold (ms) */
  simpleSelectP95: number;
  /** INSERT P95 threshold (ms) */
  insertP95: number;
  /** Batch INSERT P95 threshold (ms) */
  batchInsertP95: number;
  /** Concurrent read P95 threshold (ms) */
  concurrentReadP95: number;
  /** Cold start threshold (ms) */
  coldStartMs: number;
  /** CDC event latency P95 threshold (ms) */
  cdcEventLatencyP95: number;
  /** Maximum error rate (0-1) */
  maxErrorRate: number;
}

/**
 * Default benchmark baselines
 */
export const DEFAULT_BASELINES: BenchmarkBaselines = {
  simpleSelectP95: 5, // Point queries < 5ms P95
  insertP95: 10, // Single INSERT < 10ms P95
  batchInsertP95: 50, // Batch INSERT (100 rows) < 50ms P95
  concurrentReadP95: 10, // Concurrent reads < 10ms P95
  coldStartMs: 50, // Cold start < 50ms
  cdcEventLatencyP95: 20, // CDC event latency < 20ms P95
  maxErrorRate: 0.01, // Max 1% error rate
};

// =============================================================================
// Benchmark Results
// =============================================================================

/**
 * Full benchmark report
 */
export interface BenchmarkReport {
  /** Report metadata */
  metadata: ReportMetadata;
  /** Cold start results (if measured) */
  coldStart?: ColdStartMetrics & { meetsBaseline: boolean };
  /** Scenario results */
  scenarios: ScenarioResult[];
  /** CDC results (if measured) */
  cdc?: CDCLatencyMetrics & { meetsBaseline: boolean };
  /** Overall pass/fail status */
  passed: boolean;
  /** Summary of failed baselines */
  failedBaselines: string[];
  /** Summary statistics */
  summary: BenchmarkSummary;
}

/**
 * Report metadata
 */
export interface ReportMetadata {
  /** Benchmark suite name */
  name: string;
  /** Timestamp */
  timestamp: string;
  /** Duration in seconds */
  durationSeconds: number;
  /** Environment */
  environment: string;
  /** Worker URL (if applicable) */
  workerUrl?: string;
  /** Git commit (if available) */
  gitCommit?: string;
  /** Git branch (if available) */
  gitBranch?: string;
  /** Runtime info */
  runtime: RuntimeInfo;
}

/**
 * Runtime information
 */
export interface RuntimeInfo {
  /** Runtime type */
  type: 'workers' | 'miniflare' | 'node';
  /** Runtime version */
  version?: string;
  /** Platform */
  platform?: string;
  /** Worker tier (if applicable) */
  workerTier?: 'standard' | 'unbound';
  /** Colo (if applicable) */
  colo?: string;
}

/**
 * Benchmark summary
 */
export interface BenchmarkSummary {
  /** Total scenarios run */
  totalScenarios: number;
  /** Scenarios passed */
  passedScenarios: number;
  /** Scenarios failed */
  failedScenarios: number;
  /** Total operations */
  totalOperations: number;
  /** Total errors */
  totalErrors: number;
  /** Overall error rate */
  errorRate: number;
  /** Average P95 across all scenarios */
  averageP95Ms: number;
  /** Average throughput across all scenarios */
  averageThroughput: number;
}

// =============================================================================
// Utility Types
// =============================================================================

/**
 * Timer for measuring durations
 */
export interface Timer {
  /** Start time */
  startTime: number;
  /** End time (set when stopped) */
  endTime?: number;
  /** Duration in milliseconds */
  durationMs: number;
}

/**
 * Create a new timer
 */
export function createTimer(): Timer {
  const startTime = performance.now();
  return {
    startTime,
    get durationMs() {
      return (this.endTime ?? performance.now()) - this.startTime;
    },
  };
}

/**
 * Stop a timer
 */
export function stopTimer(timer: Timer): number {
  timer.endTime = performance.now();
  return timer.durationMs;
}

// =============================================================================
// Statistics Utilities
// =============================================================================

/**
 * Calculate latency metrics from an array of durations
 */
export function calculateLatencyMetrics(durations: number[]): LatencyMetrics {
  if (durations.length === 0) {
    return {
      min: 0,
      max: 0,
      mean: 0,
      p50: 0,
      p95: 0,
      p99: 0,
      stdDev: 0,
      count: 0,
    };
  }

  const sorted = [...durations].sort((a, b) => a - b);
  const sum = sorted.reduce((acc, val) => acc + val, 0);
  const mean = sum / sorted.length;

  // Calculate standard deviation
  const squaredDiffs = sorted.map((val) => Math.pow(val - mean, 2));
  const avgSquaredDiff =
    squaredDiffs.reduce((acc, val) => acc + val, 0) / sorted.length;
  const stdDev = Math.sqrt(avgSquaredDiff);

  // Calculate percentiles
  const percentile = (arr: number[], p: number): number => {
    const index = Math.ceil((p / 100) * arr.length) - 1;
    return arr[Math.max(0, index)];
  };

  return {
    min: sorted[0],
    max: sorted[sorted.length - 1],
    mean,
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    stdDev,
    count: sorted.length,
  };
}

/**
 * Calculate throughput metrics
 */
export function calculateThroughputMetrics(
  durations: number[],
  successCount: number,
  errorCount: number
): ThroughputMetrics {
  const totalDurationMs = durations.reduce((acc, val) => acc + val, 0);
  const totalOperations = successCount + errorCount;
  const durationSeconds = totalDurationMs / 1000;

  return {
    queriesPerSecond:
      durationSeconds > 0 ? successCount / durationSeconds : 0,
    totalOperations,
    totalDurationMs,
    successCount,
    errorCount,
    errorRate: totalOperations > 0 ? errorCount / totalOperations : 0,
  };
}
