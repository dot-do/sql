/**
 * Benchmark Runner
 *
 * CLI-compatible benchmark runner that executes scenarios and outputs JSON results.
 */

import {
  type BenchmarkAdapter,
  type ScenarioConfig,
  type ScenarioResult,
  type AdapterType,
  type ScenarioType,
  type CLIOptions,
  type CLIOutput,
  type BenchmarkReport,
  type AdapterComparison,
  DEFAULT_SCENARIOS,
} from './types.js';

import { getScenarioExecutor, getDefaultScenarioConfig } from './scenarios/index.js';
import { DoSQLAdapter } from './adapters/dosql.js';

// =============================================================================
// Benchmark Runner Class
// =============================================================================

/**
 * Benchmark runner for executing scenarios against adapters
 */
export class BenchmarkRunner {
  private verbose: boolean;

  constructor(options: { verbose?: boolean } = {}) {
    this.verbose = options.verbose ?? false;
  }

  /**
   * Run a single scenario with a single adapter
   */
  async runScenario(
    adapter: BenchmarkAdapter,
    config: ScenarioConfig
  ): Promise<ScenarioResult> {
    this.log(`Running scenario: ${config.name} with adapter: ${adapter.name}`);

    // Initialize adapter
    await adapter.initialize();

    try {
      // Get the executor for this scenario type
      const executor = getScenarioExecutor(config.type);

      // Execute the scenario
      const result = await executor(adapter, config);

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
   * Run all default scenarios with an adapter
   */
  async runAllScenarios(adapter: BenchmarkAdapter): Promise<ScenarioResult[]> {
    const results: ScenarioResult[] = [];

    for (const config of DEFAULT_SCENARIOS) {
      const result = await this.runScenario(adapter, config);
      results.push(result);
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
    const results: Record<AdapterType, ScenarioResult[]> = {} as Record<
      AdapterType,
      ScenarioResult[]
    >;
    const comparisons: AdapterComparison[] = [];

    // Run all scenarios for each adapter
    for (const adapter of adapters) {
      this.log(`\nBenchmarking adapter: ${adapter.name}`);
      results[adapter.name as AdapterType] = [];

      for (const config of scenarios) {
        const result = await this.runScenario(adapter, config);
        results[adapter.name as AdapterType].push(result);
      }
    }

    // Generate comparisons
    for (const scenario of scenarios) {
      const scenarioResults: Record<AdapterType, ScenarioResult> = {} as Record<
        AdapterType,
        ScenarioResult
      >;
      const p95Comparison: Record<AdapterType, number> = {} as Record<
        AdapterType,
        number
      >;

      for (const adapter of adapters) {
        const adapterResults = results[adapter.name as AdapterType];
        const scenarioResult = adapterResults.find(
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
        performanceRatio: maxP95 / minP95,
        p95Comparison,
      });
    }

    // Calculate overall winner
    const winsByAdapter: Record<AdapterType, number> = {} as Record<
      AdapterType,
      number
    >;
    for (const adapter of adapters) {
      winsByAdapter[adapter.name as AdapterType] = 0;
    }
    for (const comparison of comparisons) {
      winsByAdapter[comparison.winner]++;
    }

    let overallWinner: AdapterType | null = null;
    let maxWins = 0;
    for (const [adapter, wins] of Object.entries(winsByAdapter)) {
      if (wins > maxWins) {
        maxWins = wins;
        overallWinner = adapter as AdapterType;
      }
    }

    const durationSeconds = (Date.now() - startTime) / 1000;

    return {
      metadata: {
        name: 'DoSQL vs D1/DO-SQLite Benchmark',
        timestamp: new Date().toISOString(),
        durationSeconds,
        runtime: this.detectRuntime(),
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
// CLI Runner
// =============================================================================

/**
 * Run benchmark from CLI options
 */
export async function runCLI(options: CLIOptions): Promise<CLIOutput> {
  const runner = new BenchmarkRunner({ verbose: options.verbose });

  // Create adapter based on type
  let adapter: BenchmarkAdapter;

  switch (options.adapter) {
    case 'dosql':
      adapter = new DoSQLAdapter();
      break;
    case 'd1':
    case 'do-sqlite':
      throw new Error(
        `Adapter '${options.adapter}' requires runtime bindings and cannot be used from CLI directly`
      );
    default:
      throw new Error(`Unknown adapter: ${options.adapter}`);
  }

  // Get scenario config
  const baseConfig = getDefaultScenarioConfig(options.scenario);
  const config: ScenarioConfig = {
    ...baseConfig,
    iterations: options.iterations,
    warmupIterations: options.warmup,
  };

  const startTime = Date.now();

  // Run the scenario
  const result = await runner.runScenario(adapter, config);

  const durationSeconds = (Date.now() - startTime) / 1000;

  // Format output
  const output: CLIOutput = {
    adapter: options.adapter,
    scenario: options.scenario,
    latency: {
      min: result.latency.min,
      max: result.latency.max,
      mean: result.latency.mean,
      median: result.latency.median,
      p95: result.latency.p95,
      p99: result.latency.p99,
      stdDev: result.latency.stdDev,
    },
    throughput: {
      opsPerSecond: result.throughput.opsPerSecond,
      successRate:
        result.throughput.successCount / result.throughput.totalOperations,
    },
    timestamp: new Date().toISOString(),
    durationSeconds,
  };

  return output;
}

/**
 * Parse CLI arguments
 */
export function parseCLIArgs(args: string[]): CLIOptions {
  const options: CLIOptions = {
    adapter: 'dosql',
    scenario: 'simple-select',
    iterations: 100,
    warmup: 10,
    format: 'json',
    verbose: false,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];

    switch (arg) {
      case '--adapter':
      case '-a':
        options.adapter = args[++i] as AdapterType;
        break;
      case '--scenario':
      case '-s':
        options.scenario = args[++i] as ScenarioType;
        break;
      case '--iterations':
      case '-i':
        options.iterations = parseInt(args[++i], 10);
        break;
      case '--warmup':
      case '-w':
        options.warmup = parseInt(args[++i], 10);
        break;
      case '--format':
      case '-f':
        options.format = args[++i] as 'json' | 'console';
        break;
      case '--verbose':
      case '-v':
        options.verbose = true;
        break;
      case '--help':
      case '-h':
        console.log(`
DoSQL Benchmark CLI

Usage: npx tsx benchmark/src/cli.ts [options]

Options:
  -a, --adapter <name>     Adapter to benchmark (dosql, d1, do-sqlite)
  -s, --scenario <name>    Scenario to run (simple-select, insert, transaction)
  -i, --iterations <n>     Number of iterations (default: 100)
  -w, --warmup <n>         Number of warmup iterations (default: 10)
  -f, --format <format>    Output format: json or console (default: json)
  -v, --verbose            Enable verbose logging
  -h, --help               Show this help message

Examples:
  npx tsx benchmark/src/cli.ts -a dosql -s simple-select -i 1000
  npx tsx benchmark/src/cli.ts -a dosql -s insert -f console -v
`);
        process.exit(0);
    }
  }

  return options;
}

/**
 * Main CLI entry point
 */
export async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const options = parseCLIArgs(args);

  try {
    const result = await runCLI(options);

    if (options.format === 'json') {
      console.log(JSON.stringify(result, null, 2));
    } else {
      console.log('\n=== Benchmark Results ===\n');
      console.log(`Adapter:   ${result.adapter}`);
      console.log(`Scenario:  ${result.scenario}`);
      console.log(`Duration:  ${result.durationSeconds.toFixed(2)}s`);
      console.log('\nLatency (ms):');
      console.log(`  Min:    ${result.latency.min.toFixed(3)}`);
      console.log(`  Max:    ${result.latency.max.toFixed(3)}`);
      console.log(`  Mean:   ${result.latency.mean.toFixed(3)}`);
      console.log(`  Median: ${result.latency.median.toFixed(3)}`);
      console.log(`  P95:    ${result.latency.p95.toFixed(3)}`);
      console.log(`  P99:    ${result.latency.p99.toFixed(3)}`);
      console.log(`  StdDev: ${result.latency.stdDev.toFixed(3)}`);
      console.log('\nThroughput:');
      console.log(`  Ops/sec:      ${result.throughput.opsPerSecond.toFixed(2)}`);
      console.log(
        `  Success Rate: ${(result.throughput.successRate * 100).toFixed(2)}%`
      );
    }
  } catch (error) {
    console.error(
      'Benchmark failed:',
      error instanceof Error ? error.message : error
    );
    process.exit(1);
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a new benchmark runner
 */
export function createBenchmarkRunner(options?: {
  verbose?: boolean;
}): BenchmarkRunner {
  return new BenchmarkRunner(options);
}
