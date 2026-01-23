#!/usr/bin/env npx tsx
/**
 * Generate Benchmark Report
 *
 * Runs benchmarks and generates docs/BENCHMARKS.md
 */

import { writeFileSync, mkdirSync, existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { BenchmarkRunner } from './runner.js';
import { DoSQLAdapter } from './adapters/dosql.js';
import { generateMarkdownReport, serializeResults } from './report.js';
import { DEFAULT_SCENARIOS } from './types.js';
import type { BenchmarkReport, AdapterType, ScenarioResult, AdapterComparison } from './types.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

async function main() {
  console.log('DoSQL Benchmark Report Generator\n');
  console.log('='.repeat(50));

  const runner = new BenchmarkRunner({ verbose: true });
  const adapter = new DoSQLAdapter();

  const startTime = Date.now();

  // Run all scenarios
  console.log('\nRunning benchmark scenarios...\n');
  const results = await runner.runAllScenarios(adapter);

  const durationSeconds = (Date.now() - startTime) / 1000;

  // Build report
  const report: BenchmarkReport = {
    metadata: {
      name: 'DoSQL Performance Benchmark',
      timestamp: new Date().toISOString(),
      durationSeconds,
      runtime: 'node',
    },
    results: {
      dosql: results,
    } as Record<AdapterType, ScenarioResult[]>,
    comparisons: results.map((result) => ({
      scenario: result.config.type,
      results: { dosql: result } as Record<AdapterType, ScenarioResult>,
      winner: 'dosql' as AdapterType,
      performanceRatio: 1,
      p95Comparison: { dosql: result.latency.p95 } as Record<AdapterType, number>,
    })) as AdapterComparison[],
    summary: {
      totalScenarios: results.length,
      adaptersCompared: ['dosql'] as AdapterType[],
      overallWinner: 'dosql' as AdapterType,
      winsByAdapter: { dosql: results.length } as Record<AdapterType, number>,
    },
  };

  // Generate markdown report
  const markdown = generateMarkdownReport(report, {
    includeHistograms: true,
    includeMethodology: true,
    includeReproduction: true,
  });

  // Ensure docs directory exists
  const docsDir = join(__dirname, '..', '..', 'docs');
  if (!existsSync(docsDir)) {
    mkdirSync(docsDir, { recursive: true });
  }

  // Write markdown report
  const markdownPath = join(docsDir, 'BENCHMARKS.md');
  writeFileSync(markdownPath, markdown, 'utf-8');
  console.log(`\nMarkdown report written to: ${markdownPath}`);

  // Write JSON results
  const jsonPath = join(docsDir, 'benchmark-results.json');
  writeFileSync(jsonPath, serializeResults(report), 'utf-8');
  console.log(`JSON results written to: ${jsonPath}`);

  // Print summary
  console.log('\n' + '='.repeat(50));
  console.log('BENCHMARK SUMMARY');
  console.log('='.repeat(50));

  for (const result of results) {
    console.log(`\n${result.config.name}:`);
    console.log(`  P50 (Median): ${result.latency.median.toFixed(3)} ms`);
    console.log(`  P95: ${result.latency.p95.toFixed(3)} ms`);
    console.log(`  P99: ${result.latency.p99.toFixed(3)} ms`);
    console.log(`  Throughput: ${result.throughput.opsPerSecond.toFixed(2)} ops/sec`);
  }

  console.log('\n' + '='.repeat(50));
  console.log(`Total duration: ${durationSeconds.toFixed(2)}s`);
  console.log('='.repeat(50));
}

main().catch((error) => {
  console.error('Error generating report:', error);
  process.exit(1);
});
