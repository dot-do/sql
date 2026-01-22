/**
 * Transaction Scenario
 *
 * Benchmarks multi-statement transactions.
 * Tests atomic operations combining reads and writes.
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
 * Default configuration for transaction scenario
 */
export const TRANSACTION_CONFIG: ScenarioConfig = {
  type: 'transaction',
  name: 'Transaction (Multi-Statement)',
  description:
    'Execute multiple statements in a transaction (INSERT, UPDATE, DELETE)',
  iterations: 50,
  warmupIterations: 5,
  rowCount: 100, // Seed some initial data for updates/deletes
  params: {
    operationsPerTransaction: 3, // 1 INSERT, 1 UPDATE, 1 DELETE
  },
};

/**
 * Execute the transaction scenario
 *
 * This scenario:
 * 1. Creates a test table
 * 2. Seeds initial data for updates/deletes
 * 3. Runs warmup transactions
 * 4. Measures latency for multi-statement transactions
 *
 * Each transaction includes:
 * - 1 INSERT (new row)
 * - 1 UPDATE (random existing row)
 * - 1 DELETE (specific row marked for deletion)
 */
export const executeTransaction: ScenarioExecutor = async (
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

  // Seed initial data for updates/deletes
  const seedRows: Record<string, unknown>[] = [];
  for (let i = 1; i <= config.rowCount; i++) {
    seedRows.push(generateTestRow(i, { ...schema, tableName }));
  }

  // Batch insert seed data
  const chunkSize = 100;
  for (let i = 0; i < seedRows.length; i += chunkSize) {
    const chunk = seedRows.slice(i, i + chunkSize);
    await adapter.insertBatch(tableName, chunk);
  }

  // Track IDs for inserts, updates, and deletes
  let nextInsertId = config.rowCount + 1;
  let deleteId = 1; // Start deleting from ID 1

  // Warmup phase
  for (let i = 0; i < config.warmupIterations; i++) {
    const updateId = Math.floor(Math.random() * (nextInsertId - 1)) + 1;

    await adapter.transaction([
      {
        type: 'insert',
        tableName,
        data: generateTestRow(nextInsertId++, { ...schema, tableName }),
      },
      {
        type: 'update',
        tableName,
        data: {
          id: updateId,
          name: `updated-warmup-${i}`,
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
    // Pick a random existing row for update (avoiding deleted ones)
    const updateId = Math.floor(Math.random() * (nextInsertId - deleteId)) + deleteId;

    const result = await adapter.transaction([
      {
        type: 'insert',
        tableName,
        data: generateTestRow(nextInsertId++, { ...schema, tableName }),
      },
      {
        type: 'update',
        tableName,
        data: {
          id: updateId,
          name: `updated-bench-${i}`,
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
