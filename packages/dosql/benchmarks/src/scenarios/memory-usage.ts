/**
 * Memory Usage Scenario
 *
 * Benchmarks memory consumption during database operations.
 * Tests memory efficiency across different adapters.
 */

import type {
  BenchmarkAdapter,
  ScenarioConfig,
  ScenarioResult,
  ScenarioExecutor,
  MemoryMetrics,
} from '../types.js';
import {
  calculateLatencyStats,
  generateTestRow,
  DEFAULT_BENCHMARK_SCHEMA,
  getMemoryUsage,
} from '../types.js';

/**
 * Default configuration for memory usage scenario
 */
export const MEMORY_USAGE_CONFIG: ScenarioConfig = {
  type: 'memory-usage',
  name: 'Memory Usage',
  description: 'Memory consumption during bulk operations',
  iterations: 10,
  warmupIterations: 2,
  rowCount: 10000, // Large dataset to see memory impact
};

/**
 * Execute the memory usage scenario
 *
 * This scenario:
 * 1. Measures baseline memory
 * 2. Performs bulk inserts
 * 3. Measures memory after inserts
 * 4. Performs bulk reads
 * 5. Measures peak memory usage
 */
export const executeMemoryUsage: ScenarioExecutor = async (
  adapter: BenchmarkAdapter,
  config: ScenarioConfig
): Promise<ScenarioResult> => {
  const schema = DEFAULT_BENCHMARK_SCHEMA;
  const tableName = `benchmark_memory_${Date.now()}`;
  const timings: number[] = [];
  const errors: string[] = [];
  let successCount = 0;
  let errorCount = 0;

  const startedAt = new Date().toISOString();

  // Track memory metrics
  let peakMemory = 0;
  const memoryReadings: MemoryMetrics[] = [];

  const recordMemory = () => {
    const usage = getMemoryUsage();
    if (usage) {
      memoryReadings.push(usage);
      if (usage.heapUsed > peakMemory) {
        peakMemory = usage.heapUsed;
      }
    }
  };

  // Baseline memory
  recordMemory();

  // Setup: Create table
  try {
    await adapter.dropTable(tableName);
  } catch {
    // Ignore
  }

  await adapter.createTable({
    ...schema,
    tableName,
  });

  recordMemory();

  // Generate all rows first to isolate DB memory from row generation
  const allRows: Record<string, unknown>[] = [];
  for (let i = 1; i <= config.rowCount; i++) {
    allRows.push(generateTestRow(i, { ...schema, tableName }));
  }

  recordMemory();

  // Warmup phase - small batch inserts
  for (let i = 0; i < config.warmupIterations; i++) {
    const start = i * 100;
    const chunk = allRows.slice(start, start + 100);
    if (chunk.length > 0) {
      await adapter.insertBatch(tableName, chunk);
    }
  }

  recordMemory();

  // Clear warmup data
  try {
    await adapter.dropTable(tableName);
  } catch {
    // Ignore
  }
  await adapter.createTable({
    ...schema,
    tableName,
  });

  // Force GC if available (Node.js with --expose-gc)
  if (typeof global !== 'undefined' && typeof (global as unknown as { gc?: () => void }).gc === 'function') {
    (global as unknown as { gc: () => void }).gc();
  }

  recordMemory();

  // Benchmark phase - measure memory during bulk operations
  for (let i = 0; i < config.iterations; i++) {
    const iterStart = performance.now();

    try {
      // Reset table for each iteration
      if (i > 0) {
        try {
          await adapter.dropTable(tableName);
        } catch {
          // Ignore
        }
        await adapter.createTable({
          ...schema,
          tableName,
        });
      }

      // Insert all rows in batches
      const batchSize = 1000;
      for (let j = 0; j < allRows.length; j += batchSize) {
        const chunk = allRows.slice(j, j + batchSize);
        await adapter.insertBatch(tableName, chunk);
        recordMemory();
      }

      // Read all rows to test memory during result materialization
      for (let j = 0; j < 10; j++) {
        const start = Math.floor(Math.random() * (config.rowCount - 100)) + 1;
        await adapter.readMany(tableName, 'id BETWEEN :start AND :end', {
          start,
          end: start + 100,
        });
        recordMemory();
      }

      const iterEnd = performance.now();
      timings.push(iterEnd - iterStart);
      successCount++;
    } catch (e) {
      errorCount++;
      if (e instanceof Error) {
        errors.push(e.message);
      }
    }

    recordMemory();
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

  // Calculate memory statistics
  let memory: MemoryMetrics | undefined;
  if (memoryReadings.length > 0) {
    const lastReading = memoryReadings[memoryReadings.length - 1];
    memory = {
      heapUsed: lastReading.heapUsed,
      heapTotal: lastReading.heapTotal,
      external: lastReading.external,
      rss: lastReading.rss,
      peakUsage: peakMemory,
    };

    // Add memory info to errors for visibility
    const baselineMemory = memoryReadings[0]?.heapUsed ?? 0;
    const memoryGrowth = peakMemory - baselineMemory;
    errors.unshift(
      `Memory: baseline=${formatBytes(baselineMemory)}, peak=${formatBytes(peakMemory)}, growth=${formatBytes(memoryGrowth)}`
    );
  }

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
    memory,
    rawTimings: timings,
    startedAt,
    completedAt,
    errors: errors.slice(0, 10),
  };
};

/**
 * Format bytes to human-readable string
 */
function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)}MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)}GB`;
}

/**
 * Create a memory usage scenario with custom config
 */
export function createMemoryUsageScenario(
  overrides: Partial<ScenarioConfig> = {}
): ScenarioConfig {
  return {
    ...MEMORY_USAGE_CONFIG,
    ...overrides,
  };
}
