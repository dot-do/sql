/**
 * Benchmark Types for DoSQL vs D1/DO-SQLite Comparison
 *
 * Self-contained type definitions for the benchmark suite.
 */

// =============================================================================
// Metric Types
// =============================================================================

/**
 * Latency statistics for a benchmark operation
 */
export interface LatencyStats {
  /** Minimum latency in milliseconds */
  min: number;
  /** Maximum latency in milliseconds */
  max: number;
  /** Mean latency in milliseconds */
  mean: number;
  /** Median (p50) latency in milliseconds */
  median: number;
  /** 95th percentile latency in milliseconds */
  p95: number;
  /** 99th percentile latency in milliseconds */
  p99: number;
  /** Standard deviation in milliseconds */
  stdDev: number;
}

/**
 * Cold start timing metrics
 */
export interface ColdStartMetrics {
  /** Time to first query execution in milliseconds */
  timeToFirstQuery: number;
  /** Time for database initialization in milliseconds */
  initializationTime: number;
  /** Time for connection establishment in milliseconds */
  connectionTime: number;
}

/**
 * Storage usage metrics
 */
export interface StorageMetrics {
  /** Total bytes stored */
  totalBytes: number;
  /** Number of rows */
  rowCount: number;
  /** Number of tables */
  tableCount: number;
  /** Percentage of limit used (if applicable) */
  limitUtilization: number;
}

// =============================================================================
// Operation Types
// =============================================================================

/**
 * CRUD operation type
 */
export type CrudOperation = 'create' | 'read' | 'update' | 'delete';

/**
 * Benchmark operation with timing
 */
export interface BenchmarkOperation {
  /** Operation type */
  type: CrudOperation | 'batch' | 'transaction' | 'query';
  /** SQL statement or description */
  sql: string;
  /** Parameters for the operation */
  params?: Record<string, unknown>;
  /** Execution time in milliseconds */
  durationMs: number;
  /** Number of rows affected/returned */
  rowCount: number;
  /** Success status */
  success: boolean;
  /** Error message if failed */
  error?: string;
  /** Timestamp when operation started */
  startedAt: number;
}

// =============================================================================
// Configuration Types
// =============================================================================

/**
 * Column configuration
 */
export interface ColumnConfig {
  /** Column name */
  name: string;
  /** Column type */
  type: 'INTEGER' | 'TEXT' | 'REAL' | 'BLOB';
  /** Whether column is nullable */
  nullable?: boolean;
  /** Default value */
  defaultValue?: unknown;
}

/**
 * Table schema configuration for benchmarks
 */
export interface TableSchemaConfig {
  /** Table name */
  tableName: string;
  /** Column definitions */
  columns: ColumnConfig[];
  /** Primary key column name */
  primaryKey: string;
  /** Index columns */
  indexes?: string[];
}

/**
 * Default benchmark schema
 */
export const DEFAULT_BENCHMARK_SCHEMA: TableSchemaConfig = {
  tableName: 'benchmark_data',
  columns: [
    { name: 'id', type: 'INTEGER' },
    { name: 'name', type: 'TEXT' },
    { name: 'value', type: 'REAL' },
    { name: 'data', type: 'TEXT' },
    { name: 'created_at', type: 'INTEGER' },
  ],
  primaryKey: 'id',
  indexes: ['name', 'created_at'],
};

// =============================================================================
// Adapter Interface
// =============================================================================

/**
 * Benchmark adapter interface
 */
export interface BenchmarkAdapter {
  /** Unique adapter name */
  readonly name: string;

  /** Adapter version */
  readonly version: string;

  /** Initialize the adapter */
  initialize(): Promise<void>;

  /** Clean up resources */
  cleanup(): Promise<void>;

  /** Create a table */
  createTable(schema: TableSchemaConfig): Promise<void>;

  /** Drop a table */
  dropTable(tableName: string): Promise<void>;

  /** Insert a single row */
  insert(tableName: string, row: Record<string, unknown>): Promise<BenchmarkOperation>;

  /** Insert multiple rows */
  insertBatch(tableName: string, rows: Record<string, unknown>[]): Promise<BenchmarkOperation>;

  /** Read a single row */
  read(tableName: string, primaryKey: string, primaryKeyValue: unknown): Promise<BenchmarkOperation>;

  /** Read multiple rows */
  readMany(tableName: string, whereClause: string, params?: Record<string, unknown>): Promise<BenchmarkOperation>;

  /** Update a single row */
  update(tableName: string, primaryKey: string, primaryKeyValue: unknown, updates: Record<string, unknown>): Promise<BenchmarkOperation>;

  /** Delete a single row */
  delete(tableName: string, primaryKey: string, primaryKeyValue: unknown): Promise<BenchmarkOperation>;

  /** Execute a raw SQL query */
  query(sql: string, params?: Record<string, unknown>): Promise<BenchmarkOperation>;

  /** Execute multiple operations in a transaction */
  transaction(operations: Array<{
    type: 'insert' | 'update' | 'delete';
    tableName: string;
    data: Record<string, unknown>;
  }>): Promise<BenchmarkOperation>;

  /** Get current storage metrics */
  getStorageMetrics(): Promise<StorageMetrics>;

  /** Measure cold start time */
  measureColdStart(): Promise<ColdStartMetrics>;
}

// =============================================================================
// Scenario Types
// =============================================================================

/**
 * Scenario type identifiers
 */
export type ScenarioType =
  | 'simple-select'
  | 'insert'
  | 'transaction'
  | 'batch-insert'
  | 'range-query'
  | 'update'
  | 'delete';

/**
 * Scenario configuration
 */
export interface ScenarioConfig {
  /** Scenario type */
  type: ScenarioType;
  /** Human-readable name */
  name: string;
  /** Description of what this scenario tests */
  description: string;
  /** Number of iterations to run */
  iterations: number;
  /** Warmup iterations (not counted in metrics) */
  warmupIterations: number;
  /** Number of rows to seed for the scenario */
  rowCount: number;
  /** Batch size for batch operations */
  batchSize?: number;
  /** Custom parameters */
  params?: Record<string, unknown>;
}

/**
 * Scenario result with metrics
 */
export interface ScenarioResult {
  /** Scenario configuration used */
  config: ScenarioConfig;
  /** Adapter name that was benchmarked */
  adapter: string;
  /** Latency statistics */
  latency: LatencyStats;
  /** Throughput metrics */
  throughput: {
    opsPerSecond: number;
    totalOperations: number;
    successCount: number;
    errorCount: number;
    errorRate: number;
  };
  /** Raw timings for detailed analysis */
  rawTimings: number[];
  /** Start timestamp */
  startedAt: string;
  /** End timestamp */
  completedAt: string;
  /** Errors encountered */
  errors: string[];
}

/**
 * Scenario execution function signature
 */
export type ScenarioExecutor = (
  adapter: BenchmarkAdapter,
  config: ScenarioConfig
) => Promise<ScenarioResult>;

// =============================================================================
// Benchmark Suite Types
// =============================================================================

/**
 * Adapter type for comparison
 */
export type AdapterType = 'dosql' | 'd1' | 'do-sqlite';

/**
 * Comparison result between adapters for a scenario
 */
export interface AdapterComparison {
  /** Scenario that was compared */
  scenario: ScenarioType;
  /** Results by adapter */
  results: Record<AdapterType, ScenarioResult>;
  /** Winner (fastest adapter) */
  winner: AdapterType;
  /** Relative performance (ratio of slowest to fastest) */
  performanceRatio: number;
  /** P95 latency comparison */
  p95Comparison: Record<AdapterType, number>;
}

/**
 * Full benchmark report
 */
export interface BenchmarkReport {
  /** Report metadata */
  metadata: {
    name: string;
    timestamp: string;
    durationSeconds: number;
    runtime: 'workers' | 'node' | 'miniflare';
  };
  /** Scenario results by adapter */
  results: Record<AdapterType, ScenarioResult[]>;
  /** Comparisons between adapters */
  comparisons: AdapterComparison[];
  /** Summary statistics */
  summary: {
    totalScenarios: number;
    adaptersCompared: AdapterType[];
    overallWinner: AdapterType | null;
    winsByAdapter: Record<AdapterType, number>;
  };
}

// =============================================================================
// CLI Types
// =============================================================================

/**
 * CLI options
 */
export interface CLIOptions {
  /** Adapter to benchmark */
  adapter: AdapterType;
  /** Scenario to run */
  scenario: ScenarioType;
  /** Number of iterations */
  iterations: number;
  /** Number of warmup iterations */
  warmup: number;
  /** Output format */
  format: 'json' | 'console';
  /** Verbose output */
  verbose: boolean;
}

/**
 * CLI output format
 */
export interface CLIOutput {
  /** Adapter used */
  adapter: AdapterType;
  /** Scenario run */
  scenario: ScenarioType;
  /** Latency stats */
  latency: LatencyStats;
  /** Throughput */
  throughput: {
    opsPerSecond: number;
    successRate: number;
  };
  /** Timestamp */
  timestamp: string;
  /** Duration in seconds */
  durationSeconds: number;
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Calculate latency stats from an array of durations
 */
export function calculateLatencyStats(durations: number[]): LatencyStats {
  if (durations.length === 0) {
    return {
      min: 0,
      max: 0,
      mean: 0,
      median: 0,
      p95: 0,
      p99: 0,
      stdDev: 0,
    };
  }

  const sorted = [...durations].sort((a, b) => a - b);
  const sum = sorted.reduce((acc, val) => acc + val, 0);
  const mean = sum / sorted.length;

  // Calculate standard deviation
  const squaredDiffs = sorted.map((val) => Math.pow(val - mean, 2));
  const avgSquaredDiff = squaredDiffs.reduce((acc, val) => acc + val, 0) / sorted.length;
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
    median: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    stdDev,
  };
}

/**
 * Generate test data for benchmarks
 */
export function generateTestRow(
  id: number,
  schema: TableSchemaConfig
): Record<string, unknown> {
  const row: Record<string, unknown> = {};

  for (const col of schema.columns) {
    if (col.name === schema.primaryKey) {
      row[col.name] = id;
    } else {
      switch (col.type) {
        case 'INTEGER':
          row[col.name] = Math.floor(Math.random() * 1000000);
          break;
        case 'REAL':
          row[col.name] = Math.random() * 1000;
          break;
        case 'TEXT':
          row[col.name] = `value_${id}_${col.name}_${Date.now()}`;
          break;
        case 'BLOB':
          row[col.name] = new Uint8Array([id % 256, (id >> 8) % 256]).buffer;
          break;
      }
    }
  }

  return row;
}

// =============================================================================
// Default Configurations
// =============================================================================

/**
 * Default scenario configurations
 */
export const DEFAULT_SCENARIOS: ScenarioConfig[] = [
  {
    type: 'simple-select',
    name: 'Simple SELECT',
    description: 'Point query by primary key',
    iterations: 100,
    warmupIterations: 10,
    rowCount: 1000,
  },
  {
    type: 'insert',
    name: 'INSERT Single Row',
    description: 'Insert a single row',
    iterations: 100,
    warmupIterations: 10,
    rowCount: 0,
  },
  {
    type: 'transaction',
    name: 'Transaction (Multi-Statement)',
    description: 'Execute multiple statements in a transaction',
    iterations: 50,
    warmupIterations: 5,
    rowCount: 100,
  },
];
