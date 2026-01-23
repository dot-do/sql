/**
 * Cold Start Scenario
 *
 * Benchmarks database initialization and first query time.
 * Tests cold start performance across different adapters.
 */

import type {
  BenchmarkAdapter,
  ScenarioConfig,
  ScenarioResult,
  ScenarioExecutor,
} from '../types.js';
import { calculateLatencyStats, DEFAULT_BENCHMARK_SCHEMA } from '../types.js';

/**
 * Default configuration for cold start scenario
 */
export const COLD_START_CONFIG: ScenarioConfig = {
  type: 'cold-start',
  name: 'Cold Start Time',
  description: 'Time to first query after initialization',
  iterations: 10,
  warmupIterations: 0, // No warmup for cold start benchmarks
  rowCount: 0,
};

/**
 * Execute the cold start scenario
 *
 * This scenario:
 * 1. Measures adapter initialization time
 * 2. Creates a table
 * 3. Executes first query
 * 4. Repeats for statistical significance
 */
export const executeColdStart: ScenarioExecutor = async (
  adapter: BenchmarkAdapter,
  config: ScenarioConfig
): Promise<ScenarioResult> => {
  const schema = DEFAULT_BENCHMARK_SCHEMA;
  const tableName = `benchmark_coldstart_${Date.now()}`;
  const timings: number[] = [];
  const errors: string[] = [];
  let successCount = 0;
  let errorCount = 0;

  const startedAt = new Date().toISOString();

  // For cold start, we measure the entire cycle:
  // cleanup -> initialize -> create table -> first query

  for (let i = 0; i < config.iterations; i++) {
    const iterStart = performance.now();

    try {
      // Ensure we're starting fresh
      await adapter.cleanup();

      // Measure initialization
      const initStart = performance.now();
      await adapter.initialize();
      const initEnd = performance.now();

      // Create table
      const tableStart = performance.now();
      try {
        await adapter.dropTable(tableName);
      } catch {
        // Ignore
      }
      await adapter.createTable({
        ...schema,
        tableName,
      });
      const tableEnd = performance.now();

      // First query (simple INSERT)
      const queryStart = performance.now();
      await adapter.insert(tableName, {
        id: 1,
        name: 'cold_start_test',
        value: 123.45,
        data: 'test data',
        created_at: Date.now(),
      });
      const queryEnd = performance.now();

      // Total time
      const totalTime = performance.now() - iterStart;
      timings.push(totalTime);
      successCount++;

      // Log breakdown if verbose
      const initTime = initEnd - initStart;
      const tableTime = tableEnd - tableStart;
      const queryTime = queryEnd - queryStart;

      // Store breakdown in the first successful iteration for reference
      if (i === 0) {
        errors.push(
          `Breakdown: init=${initTime.toFixed(2)}ms, table=${tableTime.toFixed(2)}ms, query=${queryTime.toFixed(2)}ms`
        );
      }

      // Cleanup for next iteration
      try {
        await adapter.dropTable(tableName);
      } catch {
        // Ignore
      }
    } catch (e) {
      errorCount++;
      if (e instanceof Error) {
        errors.push(e.message);
      }
    }
  }

  const completedAt = new Date().toISOString();

  // Calculate metrics
  const latency = calculateLatencyStats(timings);
  const totalDurationMs = timings.reduce((a, b) => a + b, 0);
  const opsPerSecond = totalDurationMs > 0 ? (successCount / totalDurationMs) * 1000 : 0;

  // Get cold start metrics from adapter
  let coldStart;
  try {
    coldStart = await adapter.measureColdStart();
  } catch {
    // Ignore if not available
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
    coldStart,
    rawTimings: timings,
    startedAt,
    completedAt,
    errors: errors.slice(0, 10),
  };
};

/**
 * Create a cold start scenario with custom config
 */
export function createColdStartScenario(
  overrides: Partial<ScenarioConfig> = {}
): ScenarioConfig {
  return {
    ...COLD_START_CONFIG,
    ...overrides,
  };
}
