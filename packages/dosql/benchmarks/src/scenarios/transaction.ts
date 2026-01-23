/**
 * Transaction Scenario
 *
 * Benchmarks multi-statement transactions.
 * Tests transaction throughput with mixed operations.
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
 * Default configuration for transaction scenario
 */
export const TRANSACTION_CONFIG: ScenarioConfig = {
  type: 'transaction',
  name: 'Transaction (Multi-Statement)',
  description: 'Execute multiple statements in a transaction (INSERT + UPDATE + DELETE)',
  iterations: 50,
  warmupIterations: 5,
  rowCount: 100,
};

/**
 * Execute the transaction scenario
 *
 * This scenario:
 * 1. Creates a test table
 * 2. Seeds it with initial data
 * 3. Runs warmup transactions
 * 4. Measures latency for multi-statement transactions
 *
 * Each transaction performs:
 * - 1 INSERT
 * - 1 UPDATE
 * - 1 DELETE
 */
export const executeTransaction: ScenarioExecutor = async (
  adapter: BenchmarkAdapter,
  config: ScenarioConfig
): Promise<ScenarioResult> => {
  const schema = DEFAULT_BENCHMARK_SCHEMA;
  const tableName = `benchmark_transaction_${Date.now()}`;
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

  let nextId = config.rowCount + 1;
  let deleteId = 1;

  // Warmup phase
  for (let i = 0; i < config.warmupIterations; i++) {
    const updateId = Math.floor(Math.random() * (config.rowCount - deleteId)) + deleteId + 1;

    await adapter.transaction([
      {
        type: 'insert',
        tableName,
        data: generateTestRow(nextId++, { ...schema, tableName }),
      },
      {
        type: 'update',
        tableName,
        data: {
          id: updateId,
          name: `updated_warmup_${i}`,
          value: Math.random() * 1000,
        },
      },
      {
        type: 'delete',
        tableName,
        data: { id: deleteId++ },
      },
    ]);
  }

  // Benchmark phase
  for (let i = 0; i < config.iterations; i++) {
    // Make sure we have valid IDs to work with
    const validIds = [];
    for (let j = deleteId; j < nextId; j++) {
      validIds.push(j);
    }

    if (validIds.length < 2) {
      // Need to add more rows
      for (let j = 0; j < 10; j++) {
        const row = generateTestRow(nextId++, { ...schema, tableName });
        await adapter.insert(tableName, row);
      }
      continue;
    }

    const updateId = validIds[Math.floor(Math.random() * (validIds.length - 1)) + 1];

    const result = await adapter.transaction([
      {
        type: 'insert',
        tableName,
        data: generateTestRow(nextId++, { ...schema, tableName }),
      },
      {
        type: 'update',
        tableName,
        data: {
          id: updateId,
          name: `updated_${i}_${Date.now()}`,
          value: Math.random() * 1000,
        },
      },
      {
        type: 'delete',
        tableName,
        data: { id: deleteId++ },
      },
    ]);

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
 * Create a transaction scenario with custom config
 */
export function createTransactionScenario(
  overrides: Partial<ScenarioConfig> = {}
): ScenarioConfig {
  return {
    ...TRANSACTION_CONFIG,
    ...overrides,
  };
}
