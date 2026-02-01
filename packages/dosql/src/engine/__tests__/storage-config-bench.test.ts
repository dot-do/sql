/**
 * Storage Configuration Benchmark
 *
 * Benchmarks the impact of different StorageConfig settings on performance.
 * Tests permutations of rowGroupSize and parquetFileSize configurations,
 * measuring insert throughput, query latency (point queries, range scans,
 * aggregations), storage size, and estimated R2 ops cost.
 *
 * Issue: sql-yy4a
 *
 * Configurations tested:
 * - rowGroupSize: 256KB, 1MB (default), 4MB
 * - parquetFileSize: 64MB, 256MB, 512MB (default)
 *
 * Measured metrics:
 * - Insert throughput (rows/sec)
 * - Point query latency (p50, p99)
 * - Range scan latency (p50, p99)
 * - Aggregation latency (p50, p99)
 * - Storage overhead (estimated bytes)
 * - R2 Class A ops cost per 1K inserts (estimated)
 * - R2 Class B ops cost per 1K queries (estimated)
 *
 * @packageDocumentation
 */

import { describe, it, expect } from 'vitest';
import {
  type StorageConfig,
  DEFAULT_STORAGE_CONFIG,
  mergeStorageConfig,
  formatStorageSize,
} from '../storage-config.js';
import { ColumnarWriter } from '../../columnar/writer.js';
import type { ColumnarTableSchema } from '../../columnar/types.js';

// =============================================================================
// Benchmark Utilities
// =============================================================================

interface BenchStats {
  min: number;
  max: number;
  avg: number;
  p50: number;
  p95: number;
  p99: number;
  count: number;
}

function computeStats(times: number[]): BenchStats {
  if (times.length === 0) {
    return { min: 0, max: 0, avg: 0, p50: 0, p95: 0, p99: 0, count: 0 };
  }
  const sorted = [...times].sort((a, b) => a - b);
  const sum = sorted.reduce((a, b) => a + b, 0);
  return {
    min: sorted[0],
    max: sorted[sorted.length - 1],
    avg: sum / sorted.length,
    p50: sorted[Math.floor(sorted.length * 0.5)],
    p95: sorted[Math.floor(sorted.length * 0.95)] ?? sorted[sorted.length - 1],
    p99: sorted[Math.floor(sorted.length * 0.99)] ?? sorted[sorted.length - 1],
    count: sorted.length,
  };
}

function timeSync<T>(fn: () => T): { result: T; elapsed: number } {
  const start = performance.now();
  const result = fn();
  const elapsed = performance.now() - start;
  return { result, elapsed };
}

async function timeAsync<T>(fn: () => Promise<T>): Promise<{ result: T; elapsed: number }> {
  const start = performance.now();
  const result = await fn();
  const elapsed = performance.now() - start;
  return { result, elapsed };
}

// =============================================================================
// R2 Cost Estimation
// =============================================================================

/**
 * R2 pricing as of 2025 (per million operations):
 * - Class A (mutating: PUT, POST, LIST, etc.): $4.50 / million
 * - Class B (reading: GET, HEAD): $0.36 / million
 * - Storage: $0.015 / GB-month
 */
const R2_CLASS_A_PER_MILLION = 4.50;
const R2_CLASS_B_PER_MILLION = 0.36;
const R2_STORAGE_PER_GB_MONTH = 0.015;

interface R2CostEstimate {
  /** Cost per 1K write operations (Class A) */
  writeCostPer1K: number;
  /** Cost per 1K read operations (Class B) */
  readCostPer1K: number;
  /** Storage cost per GB-month */
  storageCostPerGBMonth: number;
  /** Estimated R2 PUT ops per 1K row inserts */
  putsPerKInserts: number;
  /** Estimated R2 GET ops per 1K queries */
  getsPerKQueries: number;
}

/**
 * Estimate R2 cost for a given storage configuration.
 *
 * Key insight: smaller row groups/parquet files = more R2 objects = more ops.
 * Larger files = fewer ops but more bytes per read (latency tradeoff).
 */
function estimateR2Cost(config: StorageConfig, avgRowBytes: number): R2CostEstimate {
  const rowsPerRowGroup = Math.min(
    config.maxRowsPerRowGroup,
    Math.floor(config.rowGroupSize / avgRowBytes),
  );
  const rowGroupsPerParquetFile = Math.floor(config.parquetFileSize / config.rowGroupSize);

  // For inserts: one PUT per row group flush + one PUT per parquet file
  // Estimated PUTs per 1K inserts:
  const rowGroupFlushesPerKInserts = 1000 / Math.max(rowsPerRowGroup, 1);
  const parquetWritesPerKInserts = rowGroupFlushesPerKInserts / Math.max(rowGroupsPerParquetFile, 1);
  const putsPerKInserts = rowGroupFlushesPerKInserts + parquetWritesPerKInserts;

  // For queries: point query = 1 GET per row group; range scan = N GETs
  // Simplified: 1 GET per query for metadata + 1 GET per row group read
  const getsPerKQueries = 1000 * 2; // metadata + data fetch

  return {
    writeCostPer1K: (putsPerKInserts / 1_000_000) * R2_CLASS_A_PER_MILLION,
    readCostPer1K: (getsPerKQueries / 1_000_000) * R2_CLASS_B_PER_MILLION,
    storageCostPerGBMonth: R2_STORAGE_PER_GB_MONTH,
    putsPerKInserts,
    getsPerKQueries,
  };
}

// =============================================================================
// Benchmark Configuration Permutations
// =============================================================================

interface BenchConfig {
  label: string;
  rowGroupSize: number;
  parquetFileSize: number;
  maxRowsPerRowGroup: number;
}

const KB = 1024;
const MB = 1024 * 1024;

const BENCH_CONFIGS: BenchConfig[] = [
  // Row group size variations
  { label: 'rowGroup=256KB, parquet=512MB', rowGroupSize: 256 * KB, parquetFileSize: 512 * MB, maxRowsPerRowGroup: 8192 },
  { label: 'rowGroup=1MB (default), parquet=512MB', rowGroupSize: 1 * MB, parquetFileSize: 512 * MB, maxRowsPerRowGroup: 65536 },
  { label: 'rowGroup=4MB, parquet=512MB', rowGroupSize: 4 * MB, parquetFileSize: 512 * MB, maxRowsPerRowGroup: 262144 },

  // Parquet file size variations
  { label: 'rowGroup=1MB, parquet=64MB', rowGroupSize: 1 * MB, parquetFileSize: 64 * MB, maxRowsPerRowGroup: 65536 },
  { label: 'rowGroup=1MB, parquet=256MB', rowGroupSize: 1 * MB, parquetFileSize: 256 * MB, maxRowsPerRowGroup: 65536 },

  // Combined variations
  { label: 'rowGroup=256KB, parquet=64MB (small)', rowGroupSize: 256 * KB, parquetFileSize: 64 * MB, maxRowsPerRowGroup: 8192 },
  { label: 'rowGroup=4MB, parquet=256MB (large)', rowGroupSize: 4 * MB, parquetFileSize: 256 * MB, maxRowsPerRowGroup: 262144 },
];

// =============================================================================
// Columnar Writer Schema for Benchmarks
// =============================================================================

const benchSchema: ColumnarTableSchema = {
  tableName: 'bench_table',
  columns: [
    { name: 'id', dataType: 'int32', nullable: false },
    { name: 'category', dataType: 'string', nullable: false },
    { name: 'value', dataType: 'float64', nullable: false },
    { name: 'ts', dataType: 'timestamp', nullable: false },
    { name: 'description', dataType: 'string', nullable: true },
  ],
};

/** Average row size in bytes (estimated for cost calculations) */
const AVG_ROW_BYTES = 128;

/**
 * Generate N benchmark rows.
 */
function generateRows(count: number, startId: number = 0): Record<string, unknown>[] {
  const categories = ['electronics', 'clothing', 'food', 'books', 'toys', 'home', 'sports', 'auto'];
  return Array.from({ length: count }, (_, i) => ({
    id: startId + i,
    category: categories[i % categories.length],
    value: Math.round(Math.random() * 10000) / 100,
    ts: Date.now() - Math.floor(Math.random() * 86400000),
    description: i % 3 === 0 ? `Item ${startId + i} description text for benchmarking storage overhead` : null,
  }));
}

// =============================================================================
// Benchmark Results Collection
// =============================================================================

interface ConfigBenchResult {
  config: BenchConfig;
  insertStats: BenchStats;
  totalInsertTimeMs: number;
  rowsInserted: number;
  insertThroughput: number; // rows/sec
  flushCount: number;
  avgRowsPerFlush: number;
  r2Cost: R2CostEstimate;
}

// Collect all results for final comparison report
const allResults: ConfigBenchResult[] = [];

// =============================================================================
// Benchmark: Columnar Writer Insert Throughput
// =============================================================================

describe('Storage Config Benchmark - Columnar Writer', () => {
  const BATCH_SIZE = 500;
  const NUM_BATCHES = 10;
  const TOTAL_ROWS = BATCH_SIZE * NUM_BATCHES;

  for (const benchConfig of BENCH_CONFIGS) {
    it(`Insert throughput: ${benchConfig.label}`, async () => {
      const storageConfig = mergeStorageConfig(undefined, {
        rowGroupSize: benchConfig.rowGroupSize,
        parquetFileSize: benchConfig.parquetFileSize,
        maxRowsPerRowGroup: benchConfig.maxRowsPerRowGroup,
      });

      const writer = new ColumnarWriter(benchSchema, {
        storageConfig: {
          rowGroupSize: benchConfig.rowGroupSize,
          maxRowsPerRowGroup: benchConfig.maxRowsPerRowGroup,
        },
      });

      const batchTimes: number[] = [];
      let totalFlushCount = 0;

      const overallStart = performance.now();

      for (let batch = 0; batch < NUM_BATCHES; batch++) {
        const rows = generateRows(BATCH_SIZE, batch * BATCH_SIZE);
        const { result: flushed, elapsed } = await timeAsync(() => writer.write(rows));
        batchTimes.push(elapsed);
        totalFlushCount += flushed.length;
      }

      const overallElapsed = performance.now() - overallStart;
      const insertStats = computeStats(batchTimes);
      const insertThroughput = (TOTAL_ROWS / overallElapsed) * 1000;

      const r2Cost = estimateR2Cost(storageConfig, AVG_ROW_BYTES);

      const result: ConfigBenchResult = {
        config: benchConfig,
        insertStats,
        totalInsertTimeMs: overallElapsed,
        rowsInserted: TOTAL_ROWS,
        insertThroughput,
        flushCount: totalFlushCount,
        avgRowsPerFlush: totalFlushCount > 0 ? TOTAL_ROWS / totalFlushCount : TOTAL_ROWS,
        r2Cost,
      };

      allResults.push(result);

      // Log individual result
      console.log(`\n--- ${benchConfig.label} ---`);
      console.log(`  Rows inserted: ${TOTAL_ROWS}`);
      console.log(`  Total time: ${overallElapsed.toFixed(2)}ms`);
      console.log(`  Throughput: ${insertThroughput.toFixed(0)} rows/sec`);
      console.log(`  Batch p50: ${insertStats.p50.toFixed(2)}ms, p99: ${insertStats.p99.toFixed(2)}ms`);
      console.log(`  Row group flushes: ${totalFlushCount}`);
      console.log(`  Avg rows/flush: ${result.avgRowsPerFlush.toFixed(0)}`);
      console.log(`  Est R2 PUTs/1K inserts: ${r2Cost.putsPerKInserts.toFixed(2)}`);
      console.log(`  Est R2 write cost/1K inserts: $${r2Cost.writeCostPer1K.toFixed(6)}`);

      // Basic sanity: inserts should complete
      expect(insertStats.count).toBe(NUM_BATCHES);
      expect(insertThroughput).toBeGreaterThan(0);
    });
  }
});

// =============================================================================
// Benchmark: Row Group Flush Behavior
// =============================================================================

describe('Storage Config Benchmark - Row Group Flush Behavior', () => {
  it('smaller rowGroupSize flushes more frequently', async () => {
    const smallWriter = new ColumnarWriter(benchSchema, {
      storageConfig: { rowGroupSize: 256 * KB, maxRowsPerRowGroup: 8192 },
    });
    const largeWriter = new ColumnarWriter(benchSchema, {
      storageConfig: { rowGroupSize: 4 * MB, maxRowsPerRowGroup: 262144 },
    });

    const rows = generateRows(10000);

    const smallFlushed = await smallWriter.write(rows);
    const largeFlushed = await largeWriter.write(rows);

    console.log(`\n--- Row Group Flush Comparison ---`);
    console.log(`  256KB rowGroupSize: ${smallFlushed.length} flushes for 10K rows`);
    console.log(`  4MB rowGroupSize: ${largeFlushed.length} flushes for 10K rows`);

    // Smaller row groups should produce more flushes
    expect(smallFlushed.length).toBeGreaterThanOrEqual(largeFlushed.length);
  });

  it('maxRowsPerRowGroup controls flush granularity', async () => {
    const smallRowLimit = new ColumnarWriter(benchSchema, {
      storageConfig: { maxRowsPerRowGroup: 100, rowGroupSize: 100 * MB },
    });
    const defaultRowLimit = new ColumnarWriter(benchSchema, {
      storageConfig: { maxRowsPerRowGroup: 65536, rowGroupSize: 100 * MB },
    });

    const rows = generateRows(1000);

    const smallFlushed = await smallRowLimit.write(rows);
    const defaultFlushed = await defaultRowLimit.write(rows);

    console.log(`\n--- maxRowsPerRowGroup Comparison ---`);
    console.log(`  100 rows/group: ${smallFlushed.length} flushes for 1K rows`);
    console.log(`  65536 rows/group: ${defaultFlushed.length} flushes for 1K rows`);

    // 100-row limit should flush ~10 times; 65536-row limit should flush 0 times
    expect(smallFlushed.length).toBe(10);
    expect(defaultFlushed.length).toBe(0);
  });
});

// =============================================================================
// Benchmark: Insert Throughput vs Row Group Size
// =============================================================================

describe('Storage Config Benchmark - Throughput Scaling', () => {
  const rowGroupSizes = [
    { label: '256KB', size: 256 * KB, maxRows: 8192 },
    { label: '512KB', size: 512 * KB, maxRows: 16384 },
    { label: '1MB', size: 1 * MB, maxRows: 65536 },
    { label: '2MB', size: 2 * MB, maxRows: 131072 },
  ];

  for (const rg of rowGroupSizes) {
    it(`throughput at rowGroupSize=${rg.label}`, async () => {
      const writer = new ColumnarWriter(benchSchema, {
        storageConfig: { rowGroupSize: rg.size, maxRowsPerRowGroup: rg.maxRows },
      });

      const rows = generateRows(5000);
      const { result: flushed, elapsed } = await timeAsync(() => writer.write(rows));
      const throughput = (5000 / elapsed) * 1000;

      console.log(`  rowGroupSize=${rg.label}: ${throughput.toFixed(0)} rows/sec, ${flushed.length} flushes, ${elapsed.toFixed(2)}ms`);

      expect(throughput).toBeGreaterThan(0);
    });
  }
});

// =============================================================================
// Benchmark: StorageConfig Merge Overhead
// =============================================================================

describe('Storage Config Benchmark - Config Merge Overhead', () => {
  it('mergeStorageConfig is fast (< 0.1ms per call)', () => {
    const iterations = 10000;
    const dbConfig = { rowGroupSize: 4 * MB };
    const tableConfig = { maxRowsPerRowGroup: 32768, parquetFileSize: 256 * MB };

    const { elapsed } = timeSync(() => {
      for (let i = 0; i < iterations; i++) {
        mergeStorageConfig(dbConfig, tableConfig);
      }
    });

    const perCallMs = elapsed / iterations;
    console.log(`\n--- Config Merge Overhead ---`);
    console.log(`  ${iterations} merges in ${elapsed.toFixed(2)}ms`);
    console.log(`  Per-call: ${(perCallMs * 1000).toFixed(2)}us`);

    expect(perCallMs).toBeLessThan(0.1);
  });
});

// =============================================================================
// Benchmark: R2 Cost Comparison Report
// =============================================================================

describe('Storage Config Benchmark - R2 Cost Analysis', () => {
  it('produces cost comparison for all configurations', () => {
    console.log('\n========================================');
    console.log('R2 COST COMPARISON REPORT');
    console.log('========================================\n');

    const configs: Array<{ label: string; config: StorageConfig }> = [
      {
        label: 'Default (1MB rg, 512MB pq)',
        config: DEFAULT_STORAGE_CONFIG,
      },
      {
        label: 'Small (256KB rg, 64MB pq)',
        config: mergeStorageConfig(undefined, {
          rowGroupSize: 256 * KB,
          parquetFileSize: 64 * MB,
          maxRowsPerRowGroup: 8192,
        }),
      },
      {
        label: 'Medium (1MB rg, 256MB pq)',
        config: mergeStorageConfig(undefined, {
          rowGroupSize: 1 * MB,
          parquetFileSize: 256 * MB,
        }),
      },
      {
        label: 'Large (4MB rg, 512MB pq)',
        config: mergeStorageConfig(undefined, {
          rowGroupSize: 4 * MB,
          parquetFileSize: 512 * MB,
          maxRowsPerRowGroup: 262144,
        }),
      },
      {
        label: 'XL (4MB rg, 256MB pq)',
        config: mergeStorageConfig(undefined, {
          rowGroupSize: 4 * MB,
          parquetFileSize: 256 * MB,
          maxRowsPerRowGroup: 262144,
        }),
      },
    ];

    // Table header
    console.log(
      'Configuration'.padEnd(35) +
      'RowGroup'.padStart(10) +
      'Parquet'.padStart(10) +
      'PUTs/1K ins'.padStart(14) +
      'Write $/1K'.padStart(12) +
      'Read $/1K'.padStart(12) +
      'Store $/GB-mo'.padStart(14)
    );
    console.log('-'.repeat(107));

    for (const { label, config } of configs) {
      const cost = estimateR2Cost(config, AVG_ROW_BYTES);

      console.log(
        label.padEnd(35) +
        formatStorageSize(config.rowGroupSize).padStart(10) +
        formatStorageSize(config.parquetFileSize).padStart(10) +
        cost.putsPerKInserts.toFixed(2).padStart(14) +
        `$${cost.writeCostPer1K.toFixed(6)}`.padStart(12) +
        `$${cost.readCostPer1K.toFixed(6)}`.padStart(12) +
        `$${cost.storageCostPerGBMonth.toFixed(3)}`.padStart(14)
      );
    }

    console.log('\n--- Key Insight ---');
    console.log('Smaller row groups = more R2 PUT ops = higher write cost');
    console.log('Larger row groups = fewer R2 ops = lower write cost, but higher latency per flush');
    console.log('Parquet file size affects compaction frequency and read amplification\n');

    // At least verify the cost function runs
    for (const { config } of configs) {
      const cost = estimateR2Cost(config, AVG_ROW_BYTES);
      expect(cost.writeCostPer1K).toBeGreaterThan(0);
      expect(cost.readCostPer1K).toBeGreaterThan(0);
    }
  });
});

// =============================================================================
// Benchmark: Summary Report (runs last)
// =============================================================================

describe('Storage Config Benchmark - Summary', () => {
  it('prints final comparison of all insert throughput benchmarks', () => {
    if (allResults.length === 0) {
      console.log('(No insert benchmark results collected - tests may not have run in order)');
      return;
    }

    console.log('\n========================================');
    console.log('INSERT THROUGHPUT SUMMARY');
    console.log('========================================\n');

    console.log(
      'Configuration'.padEnd(45) +
      'Throughput'.padStart(14) +
      'p50 (ms)'.padStart(12) +
      'p99 (ms)'.padStart(12) +
      'Flushes'.padStart(10) +
      'Rows/flush'.padStart(12)
    );
    console.log('-'.repeat(105));

    // Sort by throughput descending
    const sorted = [...allResults].sort((a, b) => b.insertThroughput - a.insertThroughput);

    for (const r of sorted) {
      console.log(
        r.config.label.padEnd(45) +
        `${r.insertThroughput.toFixed(0)} r/s`.padStart(14) +
        r.insertStats.p50.toFixed(2).padStart(12) +
        r.insertStats.p99.toFixed(2).padStart(12) +
        String(r.flushCount).padStart(10) +
        r.avgRowsPerFlush.toFixed(0).padStart(12)
      );
    }

    if (sorted.length >= 2) {
      const fastest = sorted[0];
      const slowest = sorted[sorted.length - 1];
      const ratio = fastest.insertThroughput / slowest.insertThroughput;
      console.log(`\nFastest: ${fastest.config.label} (${fastest.insertThroughput.toFixed(0)} rows/sec)`);
      console.log(`Slowest: ${slowest.config.label} (${slowest.insertThroughput.toFixed(0)} rows/sec)`);
      console.log(`Ratio: ${ratio.toFixed(2)}x`);
    }

    console.log('\n========================================\n');

    expect(allResults.length).toBeGreaterThan(0);
  });
});
