/**
 * DoSQL Benchmark Types
 *
 * Type definitions for the benchmark adapter framework.
 * Used for comparing DoSQL against DO SQLite and other storage backends.
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
 * Throughput statistics
 */
export interface ThroughputStats {
  /** Operations per second */
  opsPerSecond: number;
  /** Rows per second (for bulk operations) */
  rowsPerSecond: number;
  /** Bytes per second (for data transfer) */
  bytesPerSecond: number;
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
 * DO SQLite specific billing metrics
 */
export interface DOBillingMetrics {
  /** Estimated row operations (reads + writes) */
  estimatedRowOps: number;
  /** Estimated storage bytes used */
  estimatedStorageBytes: number;
  /** Estimated cost in USD (based on DO pricing) */
  estimatedCostUSD: number;
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
  /** Percentage of 128MB limit used (for DO SQLite) */
  limitUtilization: number;
}

/**
 * Complete metrics for a benchmark run
 */
export interface BenchmarkMetrics {
  /** Operation latency stats */
  latency: LatencyStats;
  /** Throughput stats */
  throughput: ThroughputStats;
  /** Cold start metrics (if measured) */
  coldStart?: ColdStartMetrics;
  /** DO billing metrics (for DO SQLite) */
  billing?: DOBillingMetrics;
  /** Storage metrics */
  storage?: StorageMetrics;
  /** Total duration in milliseconds */
  totalDurationMs: number;
  /** Number of operations performed */
  operationCount: number;
  /** Number of errors encountered */
  errorCount: number;
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
 * Benchmark configuration
 */
export interface BenchmarkConfig {
  /** Number of iterations per operation type */
  iterations: number;
  /** Warmup iterations (not counted in metrics) */
  warmupIterations: number;
  /** Number of rows to use for tests */
  rowCount: number;
  /** Enable cold start measurement */
  measureColdStart: boolean;
  /** Enable billing estimation (DO SQLite specific) */
  measureBilling: boolean;
  /** Concurrency level for parallel operations */
  concurrency: number;
  /** Random seed for reproducibility */
  seed?: number;
  /** Custom table schema for tests */
  schema?: TableSchemaConfig;
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
 * Default benchmark configuration
 */
export const DEFAULT_BENCHMARK_CONFIG: BenchmarkConfig = {
  iterations: 100,
  warmupIterations: 10,
  rowCount: 1000,
  measureColdStart: true,
  measureBilling: true,
  concurrency: 1,
  schema: {
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
  },
};

// =============================================================================
// Result Types
// =============================================================================

/**
 * Result of a single benchmark suite
 */
export interface BenchmarkSuiteResult {
  /** Suite name */
  name: string;
  /** Adapter name (e.g., 'do-sqlite', 'dosql') */
  adapter: string;
  /** Start timestamp */
  startedAt: number;
  /** End timestamp */
  completedAt: number;
  /** Configuration used */
  config: BenchmarkConfig;
  /** Results by operation type */
  results: Record<CrudOperation, BenchmarkMetrics>;
  /** Query benchmark results */
  queryResults?: BenchmarkMetrics;
  /** Transaction benchmark results */
  transactionResults?: BenchmarkMetrics;
  /** Raw operation data (for detailed analysis) */
  operations: BenchmarkOperation[];
  /** Environment info */
  environment: EnvironmentInfo;
}

/**
 * Environment information for the benchmark run
 */
export interface EnvironmentInfo {
  /** Runtime environment */
  runtime: 'workers' | 'node' | 'miniflare';
  /** Adapter version */
  adapterVersion: string;
  /** Platform info */
  platform?: string;
  /** Worker tier (for Cloudflare Workers) */
  workerTier?: 'standard' | 'unbound';
  /** Colo location (for Cloudflare Workers) */
  colo?: string;
}

// =============================================================================
// Adapter Interface
// =============================================================================

/**
 * Benchmark adapter interface
 *
 * Implementations should handle the specifics of each storage backend
 * while exposing a consistent interface for benchmarking.
 */
export interface BenchmarkAdapter {
  /** Unique adapter name */
  readonly name: string;

  /** Adapter version */
  readonly version: string;

  /**
   * Initialize the adapter and prepare for benchmarking
   */
  initialize(): Promise<void>;

  /**
   * Clean up resources after benchmarking
   */
  cleanup(): Promise<void>;

  /**
   * Create a table with the given schema
   */
  createTable(schema: TableSchemaConfig): Promise<void>;

  /**
   * Drop a table if it exists
   */
  dropTable(tableName: string): Promise<void>;

  /**
   * Insert a single row
   */
  insert(
    tableName: string,
    row: Record<string, unknown>
  ): Promise<BenchmarkOperation>;

  /**
   * Insert multiple rows in a batch
   */
  insertBatch(
    tableName: string,
    rows: Record<string, unknown>[]
  ): Promise<BenchmarkOperation>;

  /**
   * Read a single row by primary key
   */
  read(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown
  ): Promise<BenchmarkOperation>;

  /**
   * Read multiple rows with a WHERE clause
   */
  readMany(
    tableName: string,
    whereClause: string,
    params?: Record<string, unknown>
  ): Promise<BenchmarkOperation>;

  /**
   * Update a single row
   */
  update(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown,
    updates: Record<string, unknown>
  ): Promise<BenchmarkOperation>;

  /**
   * Delete a single row
   */
  delete(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown
  ): Promise<BenchmarkOperation>;

  /**
   * Execute a raw SQL query
   */
  query(sql: string, params?: Record<string, unknown>): Promise<BenchmarkOperation>;

  /**
   * Execute multiple operations in a transaction
   */
  transaction(
    operations: Array<{
      type: 'insert' | 'update' | 'delete';
      tableName: string;
      data: Record<string, unknown>;
    }>
  ): Promise<BenchmarkOperation>;

  /**
   * Get current storage metrics
   */
  getStorageMetrics(): Promise<StorageMetrics>;

  /**
   * Measure cold start time
   * Returns time to first successful query after fresh initialization
   */
  measureColdStart(): Promise<ColdStartMetrics>;

  /**
   * Get billing metrics (DO SQLite specific)
   */
  getBillingMetrics?(): Promise<DOBillingMetrics>;
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
 * Calculate throughput stats
 */
export function calculateThroughputStats(
  operationCount: number,
  totalDurationMs: number,
  totalRows: number,
  totalBytes: number
): ThroughputStats {
  const durationSeconds = totalDurationMs / 1000;
  return {
    opsPerSecond: operationCount / durationSeconds,
    rowsPerSecond: totalRows / durationSeconds,
    bytesPerSecond: totalBytes / durationSeconds,
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

/**
 * DO SQLite pricing constants (as of 2026)
 * See: https://developers.cloudflare.com/durable-objects/platform/pricing/
 */
export const DO_SQLITE_PRICING = {
  /** Price per million row reads */
  readPricePerMillion: 0.001,
  /** Price per million row writes */
  writePricePerMillion: 1.0,
  /** Price per GB-month storage */
  storagePricePerGBMonth: 0.20,
  /** Maximum database size in bytes (128MB) */
  maxDatabaseSize: 128 * 1024 * 1024,
};

/**
 * Estimate DO SQLite billing from operations
 */
export function estimateDOBilling(
  readOps: number,
  writeOps: number,
  storageBytes: number
): DOBillingMetrics {
  const readCost = (readOps / 1_000_000) * DO_SQLITE_PRICING.readPricePerMillion;
  const writeCost = (writeOps / 1_000_000) * DO_SQLITE_PRICING.writePricePerMillion;
  const storageCost =
    (storageBytes / (1024 * 1024 * 1024)) * DO_SQLITE_PRICING.storagePricePerGBMonth;

  return {
    estimatedRowOps: readOps + writeOps,
    estimatedStorageBytes: storageBytes,
    estimatedCostUSD: readCost + writeCost + storageCost,
  };
}
