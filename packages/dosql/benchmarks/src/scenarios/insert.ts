/**
 * INSERT Scenarios
 *
 * Benchmarks single-row and bulk INSERT operations.
 * Tests write latency for the most common write patterns.
 */

import type {
  BenchmarkAdapter,
  ScenarioConfig,
  ScenarioResult,
  ScenarioExecutor,
} from '../types.js';
import {
  calculateLatencyStats,
  generateTestRow,
  DEFAULT_BENCHMARK_SCHEMA,
} from '../types.js';

/**
 * Default configuration for single INSERT scenario
 */
export const INSERT_CONFIG: ScenarioConfig = {
  type: 'insert',
  name: 'INSERT Single Row',
  description: 'Insert a single row (INSERT INTO table VALUES (...))',
  iterations: 100,
  warmupIterations: 10,
  rowCount: 0,
};

/**
 * Default configuration for bulk INSERT scenario (1000 rows)
 */
export const BULK_INSERT_CONFIG: ScenarioConfig = {
  type: 'bulk-insert',
  name: 'Bulk INSERT (1000 rows)',
  description: 'Insert 1000 rows in a single batch operation',
  iterations: 20,
  warmupIterations: 3,
  rowCount: 0,
  batchSize: 1000,
};

/**
 * Execute the single INSERT scenario
 *
 * This scenario:
 * 1. Creates a test table
 * 2. Runs warmup inserts
 * 3. Measures latency for single-row inserts
 */
export const executeInsert: ScenarioExecutor = async (
  adapter: BenchmarkAdapter,
  config: ScenarioConfig
): Promise<ScenarioResult> => {
  const schema = DEFAULT_BENCHMARK_SCHEMA;
  const tableName = `benchmark_insert_${Date.now()}`;
  const timings: number[] = [];
  const errors: string[] = [];
  let successCount = 0;
  let errorCount = 0;

  const startedAt = new Date().toISOString();

  // Setup: Create table
  try {
    await adapter.dropTable(tableName);
  } catch {
    // Ignore if table doesn't exist
  }

  await adapter.createTable({
    ...schema,
    tableName,
  });

  let currentId = 1;

  // Warmup phase
  for (let i = 0; i < config.warmupIterations; i++) {
    const row = generateTestRow(currentId++, { ...schema, tableName });
    await adapter.insert(tableName, row);
  }

  // Benchmark phase
  for (let i = 0; i < config.iterations; i++) {
    const row = generateTestRow(currentId++, { ...schema, tableName });
    const result = await adapter.insert(tableName, row);

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

  // Cleanup
  try {
    await adapter.dropTable(tableName);
  } catch {
    // Ignore cleanup errors
  }

  const completedAt = new Date().toISOString();

  // Calculate metrics
  const latency = calculateLatencyStats(timings);
  const totalDurationMs = timings.reduce((a, b) => a + b, 0);
  const opsPerSecond = totalDurationMs > 0 ? (successCount / totalDurationMs) * 1000 : 0;

  return {
    config,
    adapter: adapter.name,
    latency,
    throughput: {
      opsPerSecond,
      totalOperations: successCount + errorCount,
      successCount,
      errorCount,
      errorRate: errorCount / (successCount + errorCount) || 0,
    },
    rawTimings: timings,
    startedAt,
    completedAt,
    errors: errors.slice(0, 10),
  };
};

/**
 * Execute the bulk INSERT scenario (1000 rows per operation)
 *
 * This scenario:
 * 1. Creates a test table
 * 2. Runs warmup batch inserts
 * 3. Measures latency for 1000-row batch inserts
 */
export const executeBulkInsert: ScenarioExecutor = async (
  adapter: BenchmarkAdapter,
  config: ScenarioConfig
): Promise<ScenarioResult> => {
  const schema = DEFAULT_BENCHMARK_SCHEMA;
  const tableName = `benchmark_bulk_insert_${Date.now()}`;
  const batchSize = config.batchSize ?? 1000;
  const timings: number[] = [];
  const errors: string[] = [];
  let successCount = 0;
  let errorCount = 0;

  const startedAt = new Date().toISOString();

  // Setup: Create table
  try {
    await adapter.dropTable(tableName);
  } catch {
    // Ignore if table doesn't exist
  }

  await adapter.createTable({
    ...schema,
    tableName,
  });

  let currentId = 1;

  // Warmup phase
  for (let i = 0; i < config.warmupIterations; i++) {
    const rows: Record<string, unknown>[] = [];
    for (let j = 0; j < batchSize; j++) {
      rows.push(generateTestRow(currentId++, { ...schema, tableName }));
    }
    await adapter.insertBatch(tableName, rows);

    // Drop and recreate table between iterations to keep size manageable
    try {
      await adapter.dropTable(tableName);
    } catch {
      // Ignore
    }
    await adapter.createTable({
      ...schema,
      tableName,
    });
    currentId = 1;
  }

  // Benchmark phase
  for (let i = 0; i < config.iterations; i++) {
    const rows: Record<string, unknown>[] = [];
    for (let j = 0; j < batchSize; j++) {
      rows.push(generateTestRow(currentId++, { ...schema, tableName }));
    }

    const result = await adapter.insertBatch(tableName, rows);

    if (result.success) {
      timings.push(result.durationMs);
      successCount++;
    } else {
      errorCount++;
      if (result.error) {
        errors.push(result.error);
      }
    }

    // Reset for next iteration
    try {
      await adapter.dropTable(tableName);
    } catch {
      // Ignore
    }
    await adapter.createTable({
      ...schema,
      tableName,
    });
    currentId = 1;
  }

  // Final cleanup
  try {
    await adapter.dropTable(tableName);
  } catch {
    // Ignore cleanup errors
  }

  const completedAt = new Date().toISOString();

  // Calculate metrics
  const latency = calculateLatencyStats(timings);
  const totalDurationMs = timings.reduce((a, b) => a + b, 0);
  const opsPerSecond = totalDurationMs > 0 ? (successCount / totalDurationMs) * 1000 : 0;

  return {
    config,
    adapter: adapter.name,
    latency,
    throughput: {
      opsPerSecond,
      totalOperations: successCount + errorCount,
      successCount,
      errorCount,
      errorRate: errorCount / (successCount + errorCount) || 0,
    },
    rawTimings: timings,
    startedAt,
    completedAt,
    errors: errors.slice(0, 10),
  };
};

/**
 * Create an INSERT scenario with custom config
 */
export function createInsertScenario(
  overrides: Partial<ScenarioConfig> = {}
): ScenarioConfig {
  return {
    ...INSERT_CONFIG,
    ...overrides,
  };
}

/**
 * Create a bulk INSERT scenario with custom config
 */
export function createBulkInsertScenario(
  overrides: Partial<ScenarioConfig> = {}
): ScenarioConfig {
  return {
    ...BULK_INSERT_CONFIG,
    ...overrides,
  };
}
