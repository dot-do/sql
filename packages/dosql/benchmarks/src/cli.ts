#!/usr/bin/env node
/**
 * DoSQL Benchmark CLI
 *
 * Command-line interface for running SQLite implementation benchmarks.
 */

import type { AdapterType, ScenarioType, CLIOptions, BenchmarkReport } from './types.js';
import { DEFAULT_SCENARIOS, QUICK_SCENARIO_OVERRIDES } from './types.js';
import { BenchmarkRunner } from './runner.js';
import {
  createAdapter,
  getAvailableAdapters,
  ADAPTER_DISPLAY_NAMES,
} from './adapters/index.js';
import {
  getQuickScenarios,
  getComprehensiveScenarios,
  SCENARIO_DISPLAY_NAMES,
  getDefaultScenarioConfig,
} from './scenarios/index.js';
import { generateMarkdownReport, serializeResults } from './report.js';

// =============================================================================
// CLI Argument Parsing
// =============================================================================

function parseArgs(args: string[]): CLIOptions {
  const options: CLIOptions = {
    adapters: ['dosql', 'better-sqlite3', 'libsql'],
    scenarios: getQuickScenarios(),
    iterations: 100,
    warmup: 10,
    format: 'console',
    verbose: false,
    quick: false,
  };

  for (let i = 0; i < args.length; i++) {
    const arg = args[i];

    switch (arg) {
      case '--adapters':
      case '-a':
        options.adapters = args[++i].split(',') as AdapterType[];
        break;
      case '--scenarios':
      case '-s':
        options.scenarios = args[++i].split(',') as ScenarioType[];
        break;
      case '--all':
        options.scenarios = getComprehensiveScenarios();
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
        options.format = args[++i] as 'json' | 'console' | 'markdown';
        break;
      case '--output':
      case '-o':
        options.output = args[++i];
        break;
      case '--verbose':
      case '-v':
        options.verbose = true;
        break;
      case '--quick':
      case '-q':
        options.quick = true;
        break;
      case '--turso-url':
        options.tursoUrl = args[++i];
        break;
      case '--turso-token':
        options.tursoToken = args[++i];
        break;
      case '--help':
      case '-h':
        printHelp();
        process.exit(0);
      case '--list':
      case '-l':
        printAvailable();
        process.exit(0);
    }
  }

  // Load Turso config from env if not provided
  if (!options.tursoUrl) {
    options.tursoUrl = process.env.TURSO_DATABASE_URL;
  }
  if (!options.tursoToken) {
    options.tursoToken = process.env.TURSO_AUTH_TOKEN;
  }

  return options;
}

function printHelp(): void {
  console.log(`
DoSQL Benchmark CLI - SQLite Implementation Comparison

Usage: npx tsx benchmarks/src/cli.ts [options]

Options:
  -a, --adapters <list>    Comma-separated adapters (default: dosql,better-sqlite3,libsql)
  -s, --scenarios <list>   Comma-separated scenarios (default: quick set)
  --all                    Run all comprehensive scenarios
  -i, --iterations <n>     Number of iterations (default: 100)
  -w, --warmup <n>         Warmup iterations (default: 10)
  -f, --format <type>      Output: json, console, markdown (default: console)
  -o, --output <file>      Write results to file
  -v, --verbose            Enable verbose logging
  -q, --quick              Quick mode (reduced iterations)
  --turso-url <url>        Turso database URL
  --turso-token <token>    Turso auth token
  -l, --list               List available adapters and scenarios
  -h, --help               Show this help

Adapters:
  dosql           DoSQL in-memory implementation
  better-sqlite3  Native SQLite (synchronous)
  libsql          LibSQL embedded mode
  turso           Turso edge SQLite (requires URL/token)
  d1              Cloudflare D1 (Workers only)
  do-sqlite       DO SQLite (Workers only)

Scenarios:
  simple-select   Point query by primary key
  insert          Single row INSERT
  bulk-insert     1000 row INSERT
  update          Single row UPDATE
  delete          Single row DELETE
  transaction     Multi-statement transaction
  complex-join    JOIN with aggregation
  cold-start      Initialization time

Examples:
  # Quick benchmark of local adapters
  npm run benchmark

  # Full benchmark with all scenarios
  npm run benchmark:all

  # Benchmark specific adapters
  npm run benchmark -- -a dosql,libsql -s insert,update

  # Output to JSON file
  npm run benchmark -- -f json -o results.json

  # Benchmark with Turso
  npm run benchmark -- -a turso --turso-url libsql://... --turso-token ...

Environment Variables:
  TURSO_DATABASE_URL   Turso database URL
  TURSO_AUTH_TOKEN     Turso authentication token
`);
}

function printAvailable(): void {
  console.log('\nAvailable Adapters:');
  for (const [key, name] of Object.entries(ADAPTER_DISPLAY_NAMES)) {
    console.log(`  ${key.padEnd(16)} ${name}`);
  }

  console.log('\nAvailable Scenarios:');
  for (const [key, name] of Object.entries(SCENARIO_DISPLAY_NAMES)) {
    console.log(`  ${key.padEnd(16)} ${name}`);
  }

  console.log('\nQuick Scenarios:', getQuickScenarios().join(', '));
  console.log('Comprehensive:', getComprehensiveScenarios().join(', '));
}

// =============================================================================
// Output Formatting
// =============================================================================

function formatConsoleOutput(report: BenchmarkReport): void {
  console.log('\n' + '='.repeat(70));
  console.log('  SQLite Implementation Benchmark Results');
  console.log('='.repeat(70));
  console.log(`  Timestamp: ${report.metadata.timestamp}`);
  console.log(`  Runtime: ${report.metadata.runtime}`);
  console.log(`  Duration: ${report.metadata.durationSeconds.toFixed(2)}s`);
  console.log('='.repeat(70));

  // Summary table header
  const adapters = report.summary.adaptersCompared;
  console.log('\n  P95 Latency Comparison (ms):');
  console.log('  ' + '-'.repeat(68));

  const headerRow = ['Scenario'.padEnd(18)];
  for (const adapter of adapters) {
    headerRow.push(adapter.padStart(12));
  }
  headerRow.push('Winner'.padStart(14));
  console.log('  ' + headerRow.join(' | '));
  console.log('  ' + '-'.repeat(68));

  // Data rows
  for (const comp of report.comparisons) {
    const row = [comp.scenario.padEnd(18)];
    for (const adapter of adapters) {
      const p95 = comp.p95Comparison[adapter];
      row.push(p95 !== undefined ? p95.toFixed(3).padStart(12) : 'N/A'.padStart(12));
    }
    row.push(comp.winner.padStart(14));
    console.log('  ' + row.join(' | '));
  }

  console.log('  ' + '-'.repeat(68));

  // Winner summary
  console.log('\n  Wins by Adapter:');
  for (const [adapter, wins] of Object.entries(report.summary.winsByAdapter)) {
    console.log(`    ${adapter}: ${wins} scenario(s)`);
  }

  if (report.summary.overallWinner) {
    console.log(`\n  Overall Winner: ${report.summary.overallWinner}`);
  }

  console.log('\n' + '='.repeat(70) + '\n');
}

// =============================================================================
// Main Entry Point
// =============================================================================

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const options = parseArgs(args);

  console.log('DoSQL Benchmark Suite');
  console.log('---------------------\n');

  if (options.verbose) {
    console.log('Options:', options);
  }

  // Create adapters
  const adapters = [];
  for (const adapterType of options.adapters) {
    const adapter = await createAdapter(adapterType, {
      turso:
        options.tursoUrl && options.tursoToken
          ? { url: options.tursoUrl, authToken: options.tursoToken }
          : undefined,
    });

    if (adapter) {
      adapters.push(adapter);
      console.log(`  [+] ${ADAPTER_DISPLAY_NAMES[adapterType] || adapterType}`);
    } else {
      console.log(`  [-] ${adapterType} (not available)`);
    }
  }

  if (adapters.length === 0) {
    console.error('\nNo adapters available. Exiting.');
    process.exit(1);
  }

  // Build scenario configs
  const scenarios = options.scenarios.map((type) => {
    const config = getDefaultScenarioConfig(type);
    return {
      ...config,
      iterations: options.quick
        ? Math.min(config.iterations, QUICK_SCENARIO_OVERRIDES.iterations ?? 20)
        : options.iterations,
      warmupIterations: options.quick
        ? Math.min(config.warmupIterations, QUICK_SCENARIO_OVERRIDES.warmupIterations ?? 5)
        : options.warmup,
    };
  });

  console.log(`\nRunning ${scenarios.length} scenario(s) across ${adapters.length} adapter(s)...\n`);

  // Run benchmarks
  const runner = new BenchmarkRunner({
    verbose: options.verbose,
    quick: options.quick,
  });

  const report = await runner.runComparison(adapters, scenarios);

  // Output results
  switch (options.format) {
    case 'json': {
      const json = serializeResults(report);
      if (options.output) {
        const fs = await import('fs');
        fs.writeFileSync(options.output, json);
        console.log(`Results written to ${options.output}`);
      } else {
        console.log(json);
      }
      break;
    }
    case 'markdown': {
      const markdown = generateMarkdownReport(report);
      if (options.output) {
        const fs = await import('fs');
        fs.writeFileSync(options.output, markdown);
        console.log(`Report written to ${options.output}`);
      } else {
        console.log(markdown);
      }
      break;
    }
    case 'console':
    default:
      formatConsoleOutput(report);
      break;
  }

  // Exit with error if there were failures
  const hasErrors = Object.values(report.results).some((results) =>
    results?.some((r) => r.errors.length > 0)
  );
  if (hasErrors && options.verbose) {
    console.log('Some scenarios had errors. Check verbose output for details.');
  }
}

// Run CLI
main().catch((error) => {
  console.error('Benchmark failed:', error);
  process.exit(1);
});
