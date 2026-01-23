/**
 * Simple SELECT Scenario
 *
 * Benchmarks point queries by primary key.
 * This is the most common read pattern and tests raw query latency.
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
 * Default configuration for simple SELECT scenario
 */
export const SIMPLE_SELECT_CONFIG: ScenarioConfig = {
  type: 'simple-select',
  name: 'Simple SELECT',
  description: 'Point query by primary key (SELECT * FROM table WHERE id = ?)',
  iterations: 100,
  warmupIterations: 10,
  rowCount: 1000,
};

/**
 * Execute the simple SELECT scenario
 *
 * This scenario:
 * 1. Creates a test table
 * 2. Seeds it with the configured number of rows
 * 3. Runs warmup queries
 * 4. Measures latency for point queries
 */
export const executeSimpleSelect: ScenarioExecutor = async (
  adapter: BenchmarkAdapter,
  config: ScenarioConfig
): Promise<ScenarioResult> => {
  const schema = DEFAULT_BENCHMARK_SCHEMA;
  const tableName = `benchmark_${config.type.replace(/-/g, '_')}_${Date.now()}`;
  const timings: number[] = [];
  const errors: string[] = [];
  let successCount = 0;
  let errorCount = 0;

  const startedAt = new Date().toISOString();

  // Setup: Create table and seed data
  try {
    await adapter.dropTable(tableName);
  } catch {
    // Ignore if table doesn't exist
  }

  await adapter.createTable({
    ...schema,
    tableName,
  });

  // Seed test data
  const rows: Record<string, unknown>[] = [];
  for (let i = 1; i <= config.rowCount; i++) {
    rows.push(generateTestRow(i, { ...schema, tableName }));
  }

  // Batch insert in chunks of 100
  const chunkSize = 100;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    await adapter.insertBatch(tableName, chunk);
  }

  // Warmup phase
  for (let i = 0; i < config.warmupIterations; i++) {
    const id = Math.floor(Math.random() * config.rowCount) + 1;
    await adapter.read(tableName, 'id', id);
  }

  // Benchmark phase
  for (let i = 0; i < config.iterations; i++) {
    const id = Math.floor(Math.random() * config.rowCount) + 1;
    const result = await adapter.read(tableName, 'id', id);

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
 * Create a simple SELECT scenario with custom config
 */
export function createSimpleSelectScenario(
  overrides: Partial<ScenarioConfig> = {}
): ScenarioConfig {
  return {
    ...SIMPLE_SELECT_CONFIG,
    ...overrides,
  };
}
