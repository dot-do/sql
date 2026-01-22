/**
 * INSERT Scenario
 *
 * Benchmarks single-row INSERT operations.
 * Tests write latency for the most common write pattern.
 */

import {
  type BenchmarkAdapter,
  type ScenarioConfig,
  type ScenarioResult,
  type ScenarioExecutor,
  calculateLatencyStats,
  generateTestRow,
  DEFAULT_BENCHMARK_SCHEMA,
} from '../types.js';

/**
 * Default configuration for INSERT scenario
 */
export const INSERT_CONFIG: ScenarioConfig = {
  type: 'insert',
  name: 'INSERT Single Row',
  description: 'Insert a single row (INSERT INTO table VALUES (...))',
  iterations: 100,
  warmupIterations: 10,
  rowCount: 0, // No seeding needed for INSERT benchmark
};

/**
 * Default configuration for batch INSERT scenario
 */
export const BATCH_INSERT_CONFIG: ScenarioConfig = {
  type: 'batch-insert',
  name: 'Batch INSERT',
  description: 'Insert multiple rows in a single transaction',
  iterations: 50,
  warmupIterations: 5,
  rowCount: 0,
  batchSize: 100,
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
  const tableName = `benchmark_${config.type.replace(/-/g, '_')}`;
  const timings: number[] = [];
  const errors: string[] = [];
  let successCount = 0;
  let errorCount = 0;

  const startedAt = new Date().toISOString();

  // Setup: Create table
  await adapter.dropTable(tableName);
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

  const completedAt = new Date().toISOString();

  // Calculate metrics
  const latency = calculateLatencyStats(timings);
  const totalDurationMs = timings.reduce((a, b) => a + b, 0);
  const opsPerSecond = (successCount / totalDurationMs) * 1000;

  return {
    config,
    adapter: adapter.name,
    latency,
    throughput: {
      opsPerSecond,
      totalOperations: successCount + errorCount,
      successCount,
      errorCount,
      errorRate: errorCount / (successCount + errorCount),
    },
    rawTimings: timings,
    startedAt,
    completedAt,
    errors: errors.slice(0, 10),
  };
};

/**
 * Execute the batch INSERT scenario
 *
 * This scenario:
 * 1. Creates a test table
 * 2. Runs warmup batch inserts
 * 3. Measures latency for batch inserts
 */
export const executeBatchInsert: ScenarioExecutor = async (
  adapter: BenchmarkAdapter,
  config: ScenarioConfig
): Promise<ScenarioResult> => {
  const schema = DEFAULT_BENCHMARK_SCHEMA;
  const tableName = `benchmark_${config.type.replace(/-/g, '_')}`;
  const batchSize = config.batchSize ?? 100;
  const timings: number[] = [];
  const errors: string[] = [];
  let successCount = 0;
  let errorCount = 0;

  const startedAt = new Date().toISOString();

  // Setup: Create table
  await adapter.dropTable(tableName);
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
  }

  const completedAt = new Date().toISOString();

  // Calculate metrics
  const latency = calculateLatencyStats(timings);
  const totalDurationMs = timings.reduce((a, b) => a + b, 0);
  const opsPerSecond = (successCount / totalDurationMs) * 1000;

  return {
    config,
    adapter: adapter.name,
    latency,
    throughput: {
      opsPerSecond,
      totalOperations: successCount + errorCount,
      successCount,
      errorCount,
      errorRate: errorCount / (successCount + errorCount),
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
 * Create a batch INSERT scenario with custom config
 */
export function createBatchInsertScenario(
  overrides: Partial<ScenarioConfig> = {}
): ScenarioConfig {
  return {
    ...BATCH_INSERT_CONFIG,
    ...overrides,
  };
}
