/**
 * UPDATE Scenario
 *
 * Benchmarks single-row UPDATE operations.
 * Tests write latency for update patterns.
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
 * Default configuration for UPDATE scenario
 */
export const UPDATE_CONFIG: ScenarioConfig = {
  type: 'update',
  name: 'UPDATE Single Row',
  description: 'Update a single row by primary key (UPDATE table SET ... WHERE id = ?)',
  iterations: 100,
  warmupIterations: 10,
  rowCount: 1000,
};

/**
 * Execute the UPDATE scenario
 *
 * This scenario:
 * 1. Creates a test table
 * 2. Seeds it with rows
 * 3. Runs warmup updates
 * 4. Measures latency for single-row updates
 */
export const executeUpdate: ScenarioExecutor = async (
  adapter: BenchmarkAdapter,
  config: ScenarioConfig
): Promise<ScenarioResult> => {
  const schema = DEFAULT_BENCHMARK_SCHEMA;
  const tableName = `benchmark_update_${Date.now()}`;
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
    await adapter.update(tableName, 'id', id, {
      name: `updated_warmup_${i}`,
      value: Math.random() * 1000,
    });
  }

  // Benchmark phase
  for (let i = 0; i < config.iterations; i++) {
    const id = Math.floor(Math.random() * config.rowCount) + 1;
    const result = await adapter.update(tableName, 'id', id, {
      name: `updated_${i}_${Date.now()}`,
      value: Math.random() * 1000,
    });

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
 * Create an UPDATE scenario with custom config
 */
export function createUpdateScenario(
  overrides: Partial<ScenarioConfig> = {}
): ScenarioConfig {
  return {
    ...UPDATE_CONFIG,
    ...overrides,
  };
}
