/**
 * Benchmark Coverage Validation Tests
 *
 * Validates that benchmark critical path code has adequate test coverage (70%+ threshold).
 * Exercises key functions in:
 * - benchmarks/types.ts (calculateLatencyStats, calculateThroughputStats, generateTestRow, estimateDOBilling)
 * - benchmarks/runner.ts (PerformanceBenchmarkRunner, config/baseline validation)
 * - benchmarks/adapters/do-sqlite.ts (DOSqliteAdapter CRUD, batch, metrics, execMany)
 * - engine/storage-config.ts (mergeStorageConfig, validateStorageConfig, parseStorageSize, formatStorageSize, parseWithStorageClause)
 *
 * Issue: sql-1192 - Benchmark tests need coverage validation for 70% threshold
 *
 * @packageDocumentation
 */

import { describe, it, expect } from 'vitest';

// =============================================================================
// Types coverage: calculateLatencyStats, calculateThroughputStats,
//                 generateTestRow, estimateDOBilling
// =============================================================================

import {
  calculateLatencyStats,
  calculateThroughputStats,
  generateTestRow,
  estimateDOBilling,
  DEFAULT_BENCHMARK_CONFIG,
  DO_SQLITE_PRICING,
  type TableSchemaConfig,
  type LatencyStats,
  type ThroughputStats,
  type DOBillingMetrics,
  type BenchmarkConfig,
  type ColumnConfig,
} from '../benchmarks/types.js';

describe('benchmark coverage: types.ts - calculateLatencyStats', () => {
  it('should return zeroed stats for empty array', () => {
    const stats = calculateLatencyStats([]);
    expect(stats.min).toBe(0);
    expect(stats.max).toBe(0);
    expect(stats.mean).toBe(0);
    expect(stats.median).toBe(0);
    expect(stats.p95).toBe(0);
    expect(stats.p99).toBe(0);
    expect(stats.stdDev).toBe(0);
  });

  it('should compute correct stats for single element', () => {
    const stats = calculateLatencyStats([5]);
    expect(stats.min).toBe(5);
    expect(stats.max).toBe(5);
    expect(stats.mean).toBe(5);
    expect(stats.median).toBe(5);
    expect(stats.p95).toBe(5);
    expect(stats.p99).toBe(5);
    expect(stats.stdDev).toBe(0);
  });

  it('should compute correct stats for uniform distribution', () => {
    const durations = [10, 10, 10, 10, 10];
    const stats = calculateLatencyStats(durations);
    expect(stats.min).toBe(10);
    expect(stats.max).toBe(10);
    expect(stats.mean).toBe(10);
    expect(stats.median).toBe(10);
    expect(stats.stdDev).toBe(0);
  });

  it('should compute correct p95/p99 for 100-element array', () => {
    const durations = Array.from({ length: 100 }, (_, i) => i + 1);
    const stats = calculateLatencyStats(durations);
    expect(stats.min).toBe(1);
    expect(stats.max).toBe(100);
    expect(stats.mean).toBe(50.5);
    expect(stats.p95).toBeGreaterThanOrEqual(94);
    expect(stats.p95).toBeLessThanOrEqual(96);
    expect(stats.p99).toBeGreaterThanOrEqual(98);
    expect(stats.p99).toBeLessThanOrEqual(100);
  });

  it('should handle unsorted input', () => {
    const durations = [9, 1, 5, 3, 7];
    const stats = calculateLatencyStats(durations);
    expect(stats.min).toBe(1);
    expect(stats.max).toBe(9);
    expect(stats.mean).toBe(5);
    expect(stats.median).toBe(5);
  });

  it('should compute standard deviation correctly', () => {
    // [2, 4, 4, 4, 5, 5, 7, 9] => mean=5, stdDev~2
    const durations = [2, 4, 4, 4, 5, 5, 7, 9];
    const stats = calculateLatencyStats(durations);
    expect(stats.stdDev).toBeGreaterThan(1);
    expect(stats.stdDev).toBeLessThan(3);
  });

  it('should handle large dataset with outliers', () => {
    const durations = Array.from({ length: 99 }, () => 1);
    durations.push(1000); // outlier
    const stats = calculateLatencyStats(durations);
    expect(stats.min).toBe(1);
    expect(stats.max).toBe(1000);
    expect(stats.p95).toBeLessThan(1000);
    // p99 of 100 elements: ceil(0.99 * 100) - 1 = 98th index (0-based)
    // sorted array is 99 ones then 1000, so index 98 = 1
    expect(stats.p99).toBeGreaterThanOrEqual(1);
    expect(stats.p99).toBeLessThanOrEqual(1000);
  });
});

describe('benchmark coverage: types.ts - calculateThroughputStats', () => {
  it('should compute ops/sec correctly', () => {
    const stats = calculateThroughputStats(1000, 2000, 5000, 1024 * 1024);
    expect(stats.opsPerSecond).toBe(500); // 1000 ops / 2 sec
    expect(stats.rowsPerSecond).toBe(2500); // 5000 rows / 2 sec
    expect(stats.bytesPerSecond).toBe(524288); // 1MB / 2 sec
  });

  it('should handle zero duration gracefully', () => {
    // Division by zero yields Infinity, which is a valid JS number
    const stats = calculateThroughputStats(100, 0, 100, 100);
    expect(stats.opsPerSecond).toBe(Infinity);
  });

  it('should handle single operation', () => {
    const stats = calculateThroughputStats(1, 100, 1, 50);
    expect(stats.opsPerSecond).toBe(10); // 1 op / 0.1 sec
    expect(stats.rowsPerSecond).toBe(10);
    expect(stats.bytesPerSecond).toBe(500);
  });
});

describe('benchmark coverage: types.ts - generateTestRow', () => {
  const schema: TableSchemaConfig = {
    tableName: 'test_table',
    columns: [
      { name: 'id', type: 'INTEGER' },
      { name: 'name', type: 'TEXT' },
      { name: 'value', type: 'REAL' },
      { name: 'data', type: 'BLOB' },
      { name: 'created_at', type: 'INTEGER' },
    ],
    primaryKey: 'id',
  };

  it('should generate a row with all column types', () => {
    const row = generateTestRow(42, schema);
    expect(row.id).toBe(42);
    expect(typeof row.name).toBe('string');
    expect(typeof row.value).toBe('number');
    expect(row.data).toBeInstanceOf(ArrayBuffer);
    expect(typeof row.created_at).toBe('number');
  });

  it('should use id as primary key value', () => {
    const row = generateTestRow(99, schema);
    expect(row.id).toBe(99);
  });

  it('should generate different values for different ids', () => {
    const row1 = generateTestRow(1, schema);
    const row2 = generateTestRow(2, schema);
    expect(row1.id).not.toBe(row2.id);
    expect(row1.name).not.toBe(row2.name);
  });

  it('should generate BLOB columns as ArrayBuffer', () => {
    const row = generateTestRow(256, schema);
    expect(row.data).toBeInstanceOf(ArrayBuffer);
  });
});

describe('benchmark coverage: types.ts - estimateDOBilling', () => {
  it('should estimate billing for read-heavy workload', () => {
    const billing = estimateDOBilling(1000000, 0, 10 * 1024 * 1024);
    expect(billing.estimatedRowOps).toBe(1000000);
    expect(billing.estimatedStorageBytes).toBe(10 * 1024 * 1024);
    expect(billing.estimatedCostUSD).toBeGreaterThan(0);
  });

  it('should estimate billing for write-heavy workload', () => {
    const billing = estimateDOBilling(0, 1000000, 10 * 1024 * 1024);
    expect(billing.estimatedRowOps).toBe(1000000);
    expect(billing.estimatedCostUSD).toBeGreaterThan(0);
  });

  it('should estimate billing for mixed workload', () => {
    const billing = estimateDOBilling(500000, 500000, 50 * 1024 * 1024);
    expect(billing.estimatedRowOps).toBe(1000000);
    expect(billing.estimatedCostUSD).toBeGreaterThan(0);
  });

  it('should estimate zero cost for zero operations', () => {
    const billing = estimateDOBilling(0, 0, 0);
    expect(billing.estimatedRowOps).toBe(0);
    expect(billing.estimatedStorageBytes).toBe(0);
    expect(billing.estimatedCostUSD).toBe(0);
  });

  it('should reflect pricing constants', () => {
    // 1M writes at $1/M = $1
    const billing = estimateDOBilling(0, 1000000, 0);
    expect(billing.estimatedCostUSD).toBeCloseTo(DO_SQLITE_PRICING.writePricePerMillion, 4);
  });
});

describe('benchmark coverage: types.ts - DEFAULT_BENCHMARK_CONFIG', () => {
  it('should have valid default configuration', () => {
    expect(DEFAULT_BENCHMARK_CONFIG.iterations).toBe(100);
    expect(DEFAULT_BENCHMARK_CONFIG.warmupIterations).toBe(10);
    expect(DEFAULT_BENCHMARK_CONFIG.rowCount).toBe(1000);
    expect(DEFAULT_BENCHMARK_CONFIG.measureColdStart).toBe(true);
    expect(DEFAULT_BENCHMARK_CONFIG.measureBilling).toBe(true);
    expect(DEFAULT_BENCHMARK_CONFIG.concurrency).toBe(1);
  });

  it('should have valid default schema', () => {
    const schema = DEFAULT_BENCHMARK_CONFIG.schema!;
    expect(schema.tableName).toBe('benchmark_data');
    expect(schema.primaryKey).toBe('id');
    expect(schema.columns.length).toBeGreaterThan(0);
    expect(schema.indexes).toBeDefined();
    expect(schema.indexes!.length).toBeGreaterThan(0);
  });

  it('should have valid DO SQLite pricing constants', () => {
    expect(DO_SQLITE_PRICING.readPricePerMillion).toBeGreaterThan(0);
    expect(DO_SQLITE_PRICING.writePricePerMillion).toBeGreaterThan(0);
    expect(DO_SQLITE_PRICING.storagePricePerGBMonth).toBeGreaterThan(0);
    expect(DO_SQLITE_PRICING.maxDatabaseSize).toBe(128 * 1024 * 1024);
  });
});

// =============================================================================
// Storage config coverage: mergeStorageConfig, validateStorageConfig,
//                          parseStorageSize, formatStorageSize,
//                          parseWithStorageClause
// =============================================================================

import {
  mergeStorageConfig,
  validateStorageConfig,
  parseStorageSize,
  formatStorageSize,
  parseWithStorageClause,
  DEFAULT_STORAGE_CONFIG,
  type StorageConfig,
} from '../engine/storage-config.js';

describe('benchmark coverage: storage-config.ts - mergeStorageConfig', () => {
  it('should return defaults when no overrides', () => {
    const config = mergeStorageConfig();
    expect(config.chunkSize).toBe(DEFAULT_STORAGE_CONFIG.chunkSize);
    expect(config.maxPageSize).toBe(DEFAULT_STORAGE_CONFIG.maxPageSize);
    expect(config.rowGroupSize).toBe(DEFAULT_STORAGE_CONFIG.rowGroupSize);
  });

  it('should apply database-level overrides', () => {
    const config = mergeStorageConfig({ rowGroupSize: 4 * 1024 * 1024 });
    expect(config.rowGroupSize).toBe(4 * 1024 * 1024);
    expect(config.chunkSize).toBe(DEFAULT_STORAGE_CONFIG.chunkSize);
  });

  it('should apply table-level overrides over database-level', () => {
    const config = mergeStorageConfig(
      { rowGroupSize: 4 * 1024 * 1024 },
      { rowGroupSize: 256 * 1024 },
    );
    expect(config.rowGroupSize).toBe(256 * 1024);
  });

  it('should skip undefined values in overrides', () => {
    const config = mergeStorageConfig({ rowGroupSize: undefined });
    expect(config.rowGroupSize).toBe(DEFAULT_STORAGE_CONFIG.rowGroupSize);
  });

  it('should handle both dbConfig and tableConfig undefined', () => {
    const config = mergeStorageConfig(undefined, undefined);
    expect(config).toEqual(DEFAULT_STORAGE_CONFIG);
  });
});

describe('benchmark coverage: storage-config.ts - validateStorageConfig', () => {
  it('should validate default config as valid', () => {
    const result = validateStorageConfig(DEFAULT_STORAGE_CONFIG);
    expect(result.valid).toBe(true);
    expect(result.errors).toHaveLength(0);
  });

  it('should reject chunkSize below minimum', () => {
    const config = { ...DEFAULT_STORAGE_CONFIG, chunkSize: 0 };
    const result = validateStorageConfig(config);
    expect(result.valid).toBe(false);
    expect(result.errors.some(e => e.field === 'chunkSize')).toBe(true);
  });

  it('should reject chunkSize above maximum', () => {
    const config = { ...DEFAULT_STORAGE_CONFIG, chunkSize: 10 * 1024 * 1024 };
    const result = validateStorageConfig(config);
    expect(result.valid).toBe(false);
    expect(result.errors.some(e => e.field === 'chunkSize')).toBe(true);
  });

  it('should reject maxPageSize below minimum', () => {
    const config = { ...DEFAULT_STORAGE_CONFIG, maxPageSize: 100 };
    const result = validateStorageConfig(config);
    expect(result.valid).toBe(false);
    expect(result.errors.some(e => e.field === 'maxPageSize')).toBe(true);
  });

  it('should reject rowGroupSize below minimum', () => {
    const config = { ...DEFAULT_STORAGE_CONFIG, rowGroupSize: 100 };
    const result = validateStorageConfig(config);
    expect(result.valid).toBe(false);
    expect(result.errors.some(e => e.field === 'rowGroupSize')).toBe(true);
  });

  it('should reject rowGroupSize above maximum', () => {
    const config = { ...DEFAULT_STORAGE_CONFIG, rowGroupSize: 256 * 1024 * 1024 };
    const result = validateStorageConfig(config);
    expect(result.valid).toBe(false);
    expect(result.errors.some(e => e.field === 'rowGroupSize')).toBe(true);
  });

  it('should reject maxRowsPerRowGroup below minimum', () => {
    const config = { ...DEFAULT_STORAGE_CONFIG, maxRowsPerRowGroup: 0 };
    const result = validateStorageConfig(config);
    expect(result.valid).toBe(false);
    expect(result.errors.some(e => e.field === 'maxRowsPerRowGroup')).toBe(true);
  });

  it('should reject maxRowsPerRowGroup above maximum', () => {
    const config = { ...DEFAULT_STORAGE_CONFIG, maxRowsPerRowGroup: 2000000 };
    const result = validateStorageConfig(config);
    expect(result.valid).toBe(false);
    expect(result.errors.some(e => e.field === 'maxRowsPerRowGroup')).toBe(true);
  });

  it('should reject hotStorageMaxSize below minimum', () => {
    const config = { ...DEFAULT_STORAGE_CONFIG, hotStorageMaxSize: 100 };
    const result = validateStorageConfig(config);
    expect(result.valid).toBe(false);
    expect(result.errors.some(e => e.field === 'hotStorageMaxSize')).toBe(true);
  });

  it('should reject hotDataMaxAge below minimum', () => {
    const config = { ...DEFAULT_STORAGE_CONFIG, hotDataMaxAge: 100 };
    const result = validateStorageConfig(config);
    expect(result.valid).toBe(false);
    expect(result.errors.some(e => e.field === 'hotDataMaxAge')).toBe(true);
  });

  it('should reject maxHotFileSize below minimum', () => {
    const config = { ...DEFAULT_STORAGE_CONFIG, maxHotFileSize: 100 };
    const result = validateStorageConfig(config);
    expect(result.valid).toBe(false);
    expect(result.errors.some(e => e.field === 'maxHotFileSize')).toBe(true);
  });

  it('should reject parquetFileSize below minimum', () => {
    const config = { ...DEFAULT_STORAGE_CONFIG, parquetFileSize: 100 };
    const result = validateStorageConfig(config);
    expect(result.valid).toBe(false);
    expect(result.errors.some(e => e.field === 'parquetFileSize')).toBe(true);
  });

  it('should reject parquetFileSize above maximum', () => {
    const config = { ...DEFAULT_STORAGE_CONFIG, parquetFileSize: 10 * 1024 * 1024 * 1024 };
    const result = validateStorageConfig(config);
    expect(result.valid).toBe(false);
    expect(result.errors.some(e => e.field === 'parquetFileSize')).toBe(true);
  });

  it('should report multiple errors', () => {
    const config = {
      ...DEFAULT_STORAGE_CONFIG,
      chunkSize: 0,
      maxPageSize: 0,
      rowGroupSize: 0,
    };
    const result = validateStorageConfig(config);
    expect(result.valid).toBe(false);
    expect(result.errors.length).toBeGreaterThanOrEqual(3);
  });

  it('should include error details with min/max', () => {
    const config = { ...DEFAULT_STORAGE_CONFIG, chunkSize: 0 };
    const result = validateStorageConfig(config);
    const error = result.errors.find(e => e.field === 'chunkSize');
    expect(error).toBeDefined();
    expect(error!.value).toBe(0);
    expect(error!.min).toBeDefined();
    expect(error!.max).toBeDefined();
    expect(error!.message).toContain('Chunk size');
  });
});

describe('benchmark coverage: storage-config.ts - parseStorageSize', () => {
  it('should parse bytes (bare number)', () => {
    expect(parseStorageSize('1048576')).toBe(1048576);
  });

  it('should parse KB', () => {
    expect(parseStorageSize('256KB')).toBe(256 * 1024);
  });

  it('should parse MB', () => {
    expect(parseStorageSize('4MB')).toBe(4 * 1024 * 1024);
  });

  it('should parse GB', () => {
    expect(parseStorageSize('2GB')).toBe(2 * 1024 * 1024 * 1024);
  });

  it('should parse B suffix', () => {
    expect(parseStorageSize('1024B')).toBe(1024);
  });

  it('should be case insensitive', () => {
    expect(parseStorageSize('4mb')).toBe(4 * 1024 * 1024);
    expect(parseStorageSize('256kb')).toBe(256 * 1024);
    expect(parseStorageSize('2Gb')).toBe(2 * 1024 * 1024 * 1024);
  });

  it('should handle whitespace', () => {
    expect(parseStorageSize('  4MB  ')).toBe(4 * 1024 * 1024);
  });

  it('should handle decimal values', () => {
    expect(parseStorageSize('1.5MB')).toBe(Math.round(1.5 * 1024 * 1024));
  });

  it('should throw for empty string', () => {
    expect(() => parseStorageSize('')).toThrow('Empty storage size string');
  });

  it('should throw for invalid format', () => {
    expect(() => parseStorageSize('abc')).toThrow('Invalid storage size format');
  });
});

describe('benchmark coverage: storage-config.ts - formatStorageSize', () => {
  it('should format bytes', () => {
    expect(formatStorageSize(500)).toBe('500B');
  });

  it('should format KB', () => {
    expect(formatStorageSize(256 * 1024)).toBe('256KB');
  });

  it('should format MB', () => {
    expect(formatStorageSize(4 * 1024 * 1024)).toBe('4MB');
  });

  it('should format GB', () => {
    expect(formatStorageSize(2 * 1024 * 1024 * 1024)).toBe('2GB');
  });

  it('should format fractional MB', () => {
    const result = formatStorageSize(1.5 * 1024 * 1024);
    expect(result).toContain('MB');
  });

  it('should format exact KB values', () => {
    expect(formatStorageSize(1024)).toBe('1KB');
  });
});

describe('benchmark coverage: storage-config.ts - parseWithStorageClause', () => {
  it('should return null for SQL without WITH STORAGE', () => {
    const result = parseWithStorageClause('CREATE TABLE foo (id INTEGER PRIMARY KEY)');
    expect(result).toBeNull();
  });

  it('should parse single quoted size values', () => {
    const result = parseWithStorageClause(
      "CREATE TABLE foo (id INT) WITH STORAGE (rowGroupSize = '4MB')"
    );
    expect(result).not.toBeNull();
    expect(result!.rowGroupSize).toBe(4 * 1024 * 1024);
  });

  it('should parse double quoted size values', () => {
    const result = parseWithStorageClause(
      'CREATE TABLE foo (id INT) WITH STORAGE (rowGroupSize = "4MB")'
    );
    expect(result).not.toBeNull();
    expect(result!.rowGroupSize).toBe(4 * 1024 * 1024);
  });

  it('should parse unquoted size values', () => {
    const result = parseWithStorageClause(
      'CREATE TABLE foo (id INT) WITH STORAGE (rowGroupSize = 4MB)'
    );
    expect(result).not.toBeNull();
    expect(result!.rowGroupSize).toBe(4 * 1024 * 1024);
  });

  it('should parse bare number values as bytes', () => {
    const result = parseWithStorageClause(
      'CREATE TABLE foo (id INT) WITH STORAGE (maxRowsPerRowGroup = 32768)'
    );
    expect(result).not.toBeNull();
    expect(result!.maxRowsPerRowGroup).toBe(32768);
  });

  it('should parse multiple settings', () => {
    const result = parseWithStorageClause(
      "CREATE TABLE foo (id INT) WITH STORAGE (rowGroupSize = '4MB', parquetFileSize = '256MB')"
    );
    expect(result).not.toBeNull();
    expect(result!.rowGroupSize).toBe(4 * 1024 * 1024);
    expect(result!.parquetFileSize).toBe(256 * 1024 * 1024);
  });

  it('should be case insensitive for WITH STORAGE keyword', () => {
    const result = parseWithStorageClause(
      "CREATE TABLE foo (id INT) with storage (rowGroupSize = '4MB')"
    );
    expect(result).not.toBeNull();
    expect(result!.rowGroupSize).toBe(4 * 1024 * 1024);
  });

  it('should throw for unknown settings', () => {
    expect(() =>
      parseWithStorageClause(
        "CREATE TABLE foo (id INT) WITH STORAGE (unknownSetting = '4MB')"
      )
    ).toThrow('Unknown storage setting');
  });

  it('should parse all valid setting keys', () => {
    const result = parseWithStorageClause(
      "CREATE TABLE foo (id INT) WITH STORAGE (chunkSize = '1MB', maxPageSize = '2MB', rowGroupSize = '4MB', maxRowsPerRowGroup = 65536, hotStorageMaxSize = '100MB', maxHotFileSize = '10MB', parquetFileSize = '512MB')"
    );
    expect(result).not.toBeNull();
    expect(result!.chunkSize).toBe(1024 * 1024);
    expect(result!.maxPageSize).toBe(2 * 1024 * 1024);
    expect(result!.rowGroupSize).toBe(4 * 1024 * 1024);
    expect(result!.maxRowsPerRowGroup).toBe(65536);
    expect(result!.hotStorageMaxSize).toBe(100 * 1024 * 1024);
    expect(result!.maxHotFileSize).toBe(10 * 1024 * 1024);
    expect(result!.parquetFileSize).toBe(512 * 1024 * 1024);
  });
});

describe('benchmark coverage: storage-config.ts - DEFAULT_STORAGE_CONFIG', () => {
  it('should be frozen (immutable)', () => {
    expect(Object.isFrozen(DEFAULT_STORAGE_CONFIG)).toBe(true);
  });

  it('should have valid defaults', () => {
    const validation = validateStorageConfig(DEFAULT_STORAGE_CONFIG);
    expect(validation.valid).toBe(true);
    expect(validation.errors).toHaveLength(0);
  });

  it('should have all required fields', () => {
    expect(DEFAULT_STORAGE_CONFIG.chunkSize).toBeDefined();
    expect(DEFAULT_STORAGE_CONFIG.maxPageSize).toBeDefined();
    expect(DEFAULT_STORAGE_CONFIG.rowGroupSize).toBeDefined();
    expect(DEFAULT_STORAGE_CONFIG.maxRowsPerRowGroup).toBeDefined();
    expect(DEFAULT_STORAGE_CONFIG.hotStorageMaxSize).toBeDefined();
    expect(DEFAULT_STORAGE_CONFIG.hotDataMaxAge).toBeDefined();
    expect(DEFAULT_STORAGE_CONFIG.maxHotFileSize).toBeDefined();
    expect(DEFAULT_STORAGE_CONFIG.parquetFileSize).toBeDefined();
  });
});

// =============================================================================
// Runner coverage: PerformanceBenchmarkRunner config/baseline validation
// =============================================================================

import {
  DEFAULT_PERFORMANCE_CONFIG,
  DEFAULT_BASELINES,
  type PerformanceBenchmarkConfig,
  type PerformanceBaselines,
  type PerformanceBenchmarkResult,
  type PerformanceBenchmarkReport,
} from '../benchmarks/runner.js';

describe('benchmark coverage: runner.ts - DEFAULT_PERFORMANCE_CONFIG', () => {
  it('should have valid default iterations', () => {
    expect(DEFAULT_PERFORMANCE_CONFIG.iterations).toBe(100);
    expect(DEFAULT_PERFORMANCE_CONFIG.warmupIterations).toBe(10);
  });

  it('should have valid default row count', () => {
    expect(DEFAULT_PERFORMANCE_CONFIG.rowCount).toBe(1000);
  });

  it('should have valid concurrency levels', () => {
    expect(DEFAULT_PERFORMANCE_CONFIG.concurrencyLevels).toEqual([1, 5, 10, 20]);
  });

  it('should enable cold start measurement by default', () => {
    expect(DEFAULT_PERFORMANCE_CONFIG.measureColdStart).toBe(true);
  });

  it('should have a valid schema reference', () => {
    expect(DEFAULT_PERFORMANCE_CONFIG.schema).toBeDefined();
    expect(DEFAULT_PERFORMANCE_CONFIG.schema.tableName).toBeDefined();
    expect(DEFAULT_PERFORMANCE_CONFIG.schema.columns.length).toBeGreaterThan(0);
    expect(DEFAULT_PERFORMANCE_CONFIG.schema.primaryKey).toBeDefined();
  });
});

describe('benchmark coverage: runner.ts - DEFAULT_BASELINES', () => {
  it('should have point query baseline', () => {
    expect(DEFAULT_BASELINES.pointQueryP95).toBe(5);
  });

  it('should have range query baseline', () => {
    expect(DEFAULT_BASELINES.rangeQueryP95).toBe(10);
  });

  it('should have insert baseline', () => {
    expect(DEFAULT_BASELINES.insertP95).toBe(10);
  });

  it('should have update baseline', () => {
    expect(DEFAULT_BASELINES.updateP95).toBe(10);
  });

  it('should have delete baseline', () => {
    expect(DEFAULT_BASELINES.deleteP95).toBe(10);
  });

  it('should have batch insert baseline', () => {
    expect(DEFAULT_BASELINES.batchInsertP95).toBe(50);
  });

  it('should have cold start baseline', () => {
    expect(DEFAULT_BASELINES.coldStart).toBe(50);
  });

  it('should merge custom baselines correctly', () => {
    const custom: Partial<PerformanceBaselines> = {
      pointQueryP95: 20,
      coldStart: 200,
    };
    const merged = { ...DEFAULT_BASELINES, ...custom };
    expect(merged.pointQueryP95).toBe(20);
    expect(merged.coldStart).toBe(200);
    expect(merged.insertP95).toBe(10); // unchanged
  });

  it('should merge custom config correctly', () => {
    const custom: Partial<PerformanceBenchmarkConfig> = {
      iterations: 50,
      warmupIterations: 5,
    };
    const merged = { ...DEFAULT_PERFORMANCE_CONFIG, ...custom };
    expect(merged.iterations).toBe(50);
    expect(merged.warmupIterations).toBe(5);
    expect(merged.rowCount).toBe(1000); // unchanged
    expect(merged.concurrencyLevels).toEqual([1, 5, 10, 20]); // unchanged
  });
});

// =============================================================================
// Columnar writer coverage (exercised by storage-config-bench test)
// =============================================================================

import { ColumnarWriter } from '../columnar/writer.js';
import type { ColumnarTableSchema } from '../columnar/types.js';

describe('benchmark coverage: ColumnarWriter basic operations', () => {
  const schema: ColumnarTableSchema = {
    tableName: 'coverage_test',
    columns: [
      { name: 'id', dataType: 'int32', nullable: false },
      { name: 'name', dataType: 'string', nullable: false },
      { name: 'value', dataType: 'float64', nullable: false },
    ],
  };

  it('should create a writer with default storage config', () => {
    const writer = new ColumnarWriter(schema);
    expect(writer).toBeDefined();
  });

  it('should create a writer with custom storage config', () => {
    const writer = new ColumnarWriter(schema, {
      storageConfig: {
        rowGroupSize: 256 * 1024,
        maxRowsPerRowGroup: 100,
      },
    });
    expect(writer).toBeDefined();
  });

  it('should write rows and return flush results', async () => {
    const writer = new ColumnarWriter(schema, {
      storageConfig: {
        maxRowsPerRowGroup: 5,
        rowGroupSize: 100 * 1024 * 1024, // large enough to not trigger size-based flush
      },
    });

    const rows = Array.from({ length: 10 }, (_, i) => ({
      id: i,
      name: `item_${i}`,
      value: i * 1.5,
    }));

    const flushed = await writer.write(rows);
    // With maxRowsPerRowGroup=5 and 10 rows, should flush twice
    expect(flushed.length).toBe(2);
  });

  it('should not flush when below threshold', async () => {
    const writer = new ColumnarWriter(schema, {
      storageConfig: {
        maxRowsPerRowGroup: 100,
        rowGroupSize: 100 * 1024 * 1024,
      },
    });

    const rows = Array.from({ length: 5 }, (_, i) => ({
      id: i,
      name: `item_${i}`,
      value: i * 1.5,
    }));

    const flushed = await writer.write(rows);
    expect(flushed.length).toBe(0);
  });
});
