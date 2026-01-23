/**
 * DELETE Scenario
 *
 * Benchmarks single-row DELETE operations.
 * Tests write latency for delete patterns.
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
 * Default configuration for DELETE scenario
 */
export const DELETE_CONFIG: ScenarioConfig = {
  type: 'delete',
  name: 'DELETE Single Row',
  description: 'Delete a single row by primary key (DELETE FROM table WHERE id = ?)',
  iterations: 100,
  warmupIterations: 10,
  rowCount: 1000,
};

/**
 * Execute the DELETE scenario
 *
 * This scenario:
 * 1. Creates a test table
 * 2. Seeds it with enough rows for all iterations
 * 3. Runs warmup deletes
 * 4. Measures latency for single-row deletes
 */
export const executeDelete: ScenarioExecutor = async (
  adapter: BenchmarkAdapter,
  config: ScenarioConfig
): Promise<ScenarioResult> => {
  const schema = DEFAULT_BENCHMARK_SCHEMA;
  const tableName = `benchmark_delete_${Date.now()}`;
  const timings: number[] = [];
  const errors: string[] = [];
  let successCount = 0;
  let errorCount = 0;

  const startedAt = new Date().toISOString();

  // Setup: Create table and seed data
  // We need enough rows for warmup + iterations
  const totalNeeded = config.warmupIterations + config.iterations + 100; // Buffer

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
  for (let i = 1; i <= totalNeeded; i++) {
    rows.push(generateTestRow(i, { ...schema, tableName }));
  }

  // Batch insert in chunks of 100
  const chunkSize = 100;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    await adapter.insertBatch(tableName, chunk);
  }

  let deleteId = 1;

  // Warmup phase
  for (let i = 0; i < config.warmupIterations; i++) {
    await adapter.delete(tableName, 'id', deleteId++);
  }

  // Benchmark phase
  for (let i = 0; i < config.iterations; i++) {
    const result = await adapter.delete(tableName, 'id', deleteId++);

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
 * Create a DELETE scenario with custom config
 */
export function createDeleteScenario(
  overrides: Partial<ScenarioConfig> = {}
): ScenarioConfig {
  return {
    ...DELETE_CONFIG,
    ...overrides,
  };
}
