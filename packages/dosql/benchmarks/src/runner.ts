/**
 * Benchmark Runner
 *
 * Core benchmark runner that executes scenarios and generates reports.
 */

import type {
  BenchmarkAdapter,
  ScenarioConfig,
  ScenarioResult,
  AdapterType,
  BenchmarkReport,
  AdapterComparison,
} from './types.js';
import { getScenarioExecutor } from './scenarios/index.js';

// =============================================================================
// Benchmark Runner Class
// =============================================================================

export interface BenchmarkRunnerOptions {
  /** Enable verbose logging */
  verbose?: boolean;
  /** Quick mode (reduced iterations) */
  quick?: boolean;
}

/**
 * Benchmark runner for executing scenarios against adapters
 */
export class BenchmarkRunner {
  private verbose: boolean;
  private quick: boolean;

  constructor(options: BenchmarkRunnerOptions = {}) {
    this.verbose = options.verbose ?? false;
    this.quick = options.quick ?? false;
  }

  /**
   * Run a single scenario with a single adapter
   */
  async runScenario(
    adapter: BenchmarkAdapter,
    config: ScenarioConfig
  ): Promise<ScenarioResult> {
    this.log(`Running scenario: ${config.name} with adapter: ${adapter.name}`);

    // Apply quick mode overrides
    const finalConfig = this.quick
      ? {
          ...config,
          iterations: Math.min(config.iterations, 20),
          warmupIterations: Math.min(config.warmupIterations, 5),
          rowCount: Math.min(config.rowCount, 100),
        }
      : config;

    // Initialize adapter
    await adapter.initialize();

    try {
      // Get the executor for this scenario type
      const executor = getScenarioExecutor(finalConfig.type);

      // Execute the scenario
      const result = await executor(adapter, finalConfig);

      this.log(
        `  P95: ${result.latency.p95.toFixed(2)}ms, ` +
          `Throughput: ${result.throughput.opsPerSecond.toFixed(2)} ops/sec`
      );

      return result;
    } finally {
      // Cleanup
      await adapter.cleanup();
    }
  }

  /**
   * Run multiple scenarios with a single adapter
   */
  async runScenarios(
    adapter: BenchmarkAdapter,
    scenarios: ScenarioConfig[]
  ): Promise<ScenarioResult[]> {
    const results: ScenarioResult[] = [];

    for (const config of scenarios) {
      try {
        const result = await this.runScenario(adapter, config);
        results.push(result);
      } catch (error) {
        this.log(`  Error in scenario ${config.name}: ${error}`);
        // Continue with other scenarios
      }
    }

    return results;
  }

  /**
   * Run comparison benchmark across multiple adapters
   */
  async runComparison(
    adapters: BenchmarkAdapter[],
    scenarios: ScenarioConfig[]
  ): Promise<BenchmarkReport> {
    const startTime = Date.now();
    const results: Partial<Record<AdapterType, ScenarioResult[]>> = {};
    const comparisons: AdapterComparison[] = [];

    // Run all scenarios for each adapter
    for (const adapter of adapters) {
      this.log(`\nBenchmarking adapter: ${adapter.name}`);
      results[adapter.name as AdapterType] = [];

      for (const config of scenarios) {
        try {
          const result = await this.runScenario(adapter, config);
          results[adapter.name as AdapterType]!.push(result);
        } catch (error) {
          this.log(`  Error: ${error}`);
        }
      }
    }

    // Generate comparisons
    for (const scenario of scenarios) {
      const scenarioResults: Partial<Record<AdapterType, ScenarioResult>> = {};
      const p95Comparison: Partial<Record<AdapterType, number>> = {};

      for (const adapter of adapters) {
        const adapterResults = results[adapter.name as AdapterType];
        const scenarioResult = adapterResults?.find(
          (r) => r.config.type === scenario.type
        );
        if (scenarioResult) {
          scenarioResults[adapter.name as AdapterType] = scenarioResult;
          p95Comparison[adapter.name as AdapterType] = scenarioResult.latency.p95;
        }
      }

      // Find the winner (lowest P95 latency)
      let winner: AdapterType = adapters[0].name as AdapterType;
      let minP95 = Infinity;
      let maxP95 = 0;

      for (const [adapterName, p95] of Object.entries(p95Comparison)) {
        if (p95 < minP95) {
          minP95 = p95;
          winner = adapterName as AdapterType;
        }
        if (p95 > maxP95) {
          maxP95 = p95;
        }
      }

      comparisons.push({
        scenario: scenario.type,
        results: scenarioResults,
        winner,
        performanceRatio: minP95 > 0 ? maxP95 / minP95 : 1,
        p95Comparison,
      });
    }

    // Calculate overall winner
    const winsByAdapter: Partial<Record<AdapterType, number>> = {};
    for (const adapter of adapters) {
      winsByAdapter[adapter.name as AdapterType] = 0;
    }
    for (const comparison of comparisons) {
      winsByAdapter[comparison.winner] = (winsByAdapter[comparison.winner] ?? 0) + 1;
    }

    let overallWinner: AdapterType | null = null;
    let maxWins = 0;
    for (const [adapter, wins] of Object.entries(winsByAdapter)) {
      if (wins && wins > maxWins) {
        maxWins = wins;
        overallWinner = adapter as AdapterType;
      }
    }

    const durationSeconds = (Date.now() - startTime) / 1000;

    return {
      metadata: {
        name: 'SQLite Implementation Comparison Benchmark',
        timestamp: new Date().toISOString(),
        durationSeconds,
        runtime: this.detectRuntime(),
        nodeVersion: typeof process !== 'undefined' ? process.version : undefined,
        platform: typeof process !== 'undefined' ? process.platform : undefined,
      },
      results,
      comparisons,
      summary: {
        totalScenarios: scenarios.length,
        adaptersCompared: adapters.map((a) => a.name as AdapterType),
        overallWinner,
        winsByAdapter,
      },
    };
  }

  /**
   * Detect runtime environment
   */
  private detectRuntime(): 'workers' | 'node' | 'miniflare' {
    if (typeof globalThis !== 'undefined' && 'caches' in globalThis) {
      return 'workers';
    }
    if (typeof process !== 'undefined' && process.version) {
      return 'node';
    }
    return 'miniflare';
  }

  /**
   * Log message if verbose mode is enabled
   */
  private log(...args: unknown[]): void {
    if (this.verbose) {
      console.log('[Benchmark]', ...args);
    }
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a new benchmark runner
 */
export function createBenchmarkRunner(
  options?: BenchmarkRunnerOptions
): BenchmarkRunner {
  return new BenchmarkRunner(options);
}
