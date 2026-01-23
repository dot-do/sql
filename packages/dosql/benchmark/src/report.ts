/**
 * Benchmark Report Generator
 *
 * Generates markdown reports from benchmark results with latency metrics,
 * comparison tables, and methodology documentation.
 */

import type {
  BenchmarkReport,
  ScenarioResult,
  LatencyStats,
  AdapterType,
  AdapterComparison,
} from './types.js';

// =============================================================================
// Report Configuration
// =============================================================================

export interface ReportConfig {
  /** Include raw timing data */
  includeRawData?: boolean;
  /** Include ASCII histograms */
  includeHistograms?: boolean;
  /** Include methodology section */
  includeMethodology?: boolean;
  /** Include reproduction instructions */
  includeReproduction?: boolean;
  /** Target file path for results */
  outputPath?: string;
}

const DEFAULT_CONFIG: Required<ReportConfig> = {
  includeRawData: false,
  includeHistograms: true,
  includeMethodology: true,
  includeReproduction: true,
  outputPath: 'docs/BENCHMARKS.md',
};

// =============================================================================
// Report Generator
// =============================================================================

/**
 * Generate a markdown benchmark report
 */
export function generateMarkdownReport(
  report: BenchmarkReport,
  config: ReportConfig = {}
): string {
  const cfg = { ...DEFAULT_CONFIG, ...config };
  const sections: string[] = [];

  // Header
  sections.push(generateHeader(report));

  // Executive Summary
  sections.push(generateExecutiveSummary(report));

  // Results Summary Table
  sections.push(generateSummaryTable(report));

  // Latency Comparison
  sections.push(generateLatencyComparison(report));

  // Per-Scenario Results
  sections.push(generateScenarioResults(report, cfg));

  // Comparison Details
  if (report.comparisons.length > 0) {
    sections.push(generateComparisonDetails(report));
  }

  // Histograms
  if (cfg.includeHistograms) {
    sections.push(generateHistograms(report));
  }

  // Methodology
  if (cfg.includeMethodology) {
    sections.push(generateMethodology());
  }

  // Reproduction Instructions
  if (cfg.includeReproduction) {
    sections.push(generateReproductionInstructions());
  }

  // Footer
  sections.push(generateFooter(report));

  return sections.join('\n\n');
}

// =============================================================================
// Report Sections
// =============================================================================

function generateHeader(report: BenchmarkReport): string {
  return `# Performance Benchmarks

> Generated: ${report.metadata.timestamp}
> Runtime: ${report.metadata.runtime}
> Duration: ${report.metadata.durationSeconds.toFixed(2)}s

This document contains performance benchmarks comparing DoSQL against D1 and raw DO SQLite.`;
}

function generateExecutiveSummary(report: BenchmarkReport): string {
  const { summary } = report;

  let winnerText = '';
  if (summary.overallWinner) {
    const winCount = summary.winsByAdapter[summary.overallWinner];
    winnerText = `**${summary.overallWinner.toUpperCase()}** won ${winCount} of ${summary.totalScenarios} scenarios.`;
  } else {
    winnerText = 'No clear winner - results vary by scenario.';
  }

  const adaptersStr = summary.adaptersCompared.join(', ');

  return `## Executive Summary

- **Adapters Tested:** ${adaptersStr}
- **Total Scenarios:** ${summary.totalScenarios}
- **Overall Winner:** ${winnerText}

### Wins by Adapter

| Adapter | Wins |
|---------|------|
${Object.entries(summary.winsByAdapter)
  .map(([adapter, wins]) => `| ${adapter} | ${wins} |`)
  .join('\n')}`;
}

function generateSummaryTable(report: BenchmarkReport): string {
  const adapters = Object.keys(report.results) as AdapterType[];

  if (adapters.length === 0) {
    return '## Summary\n\nNo results available.';
  }

  // Build header row
  const headerRow = `| Scenario | ${adapters.map((a) => `${a} P95 (ms)`).join(' | ')} | Winner |`;
  const separatorRow = `|----------|${adapters.map(() => ':-----------:').join('|')}|:------:|`;

  // Build data rows from comparisons
  const dataRows = report.comparisons.map((comp) => {
    const p95Values = adapters.map((adapter) => {
      const p95 = comp.p95Comparison[adapter];
      return p95 !== undefined ? p95.toFixed(3) : 'N/A';
    });
    return `| ${comp.scenario} | ${p95Values.join(' | ')} | ${comp.winner} |`;
  });

  return `## Summary Results

${headerRow}
${separatorRow}
${dataRows.join('\n')}`;
}

function generateLatencyComparison(report: BenchmarkReport): string {
  const adapters = Object.keys(report.results) as AdapterType[];

  if (adapters.length === 0) {
    return '';
  }

  const rows: string[] = [];

  // Get all scenarios
  const scenarios = new Set<string>();
  for (const results of Object.values(report.results)) {
    for (const result of results) {
      scenarios.add(result.config.type);
    }
  }

  for (const scenario of scenarios) {
    rows.push(`### ${scenario}`);
    rows.push('');
    rows.push('| Metric | ' + adapters.join(' | ') + ' |');
    rows.push('|--------|' + adapters.map(() => ':------:').join('|') + '|');

    const metrics: (keyof LatencyStats)[] = ['min', 'median', 'mean', 'p95', 'p99', 'max', 'stdDev'];

    for (const metric of metrics) {
      const values = adapters.map((adapter) => {
        const result = report.results[adapter]?.find((r) => r.config.type === scenario);
        if (!result) return 'N/A';
        const value = result.latency[metric];
        return value.toFixed(3);
      });
      rows.push(`| ${metric} | ${values.join(' | ')} |`);
    }

    rows.push('');
  }

  return `## Latency Comparison (ms)

${rows.join('\n')}`;
}

function generateScenarioResults(report: BenchmarkReport, _config: Required<ReportConfig>): string {
  const sections: string[] = ['## Detailed Scenario Results'];

  for (const [adapter, results] of Object.entries(report.results)) {
    sections.push(`### ${adapter.toUpperCase()}`);

    for (const result of results) {
      sections.push(`#### ${result.config.name}`);
      sections.push('');
      sections.push(`- **Description:** ${result.config.description}`);
      sections.push(`- **Iterations:** ${result.config.iterations} (warmup: ${result.config.warmupIterations})`);
      sections.push(`- **Row Count:** ${result.config.rowCount}`);
      sections.push('');
      sections.push('**Latency:**');
      sections.push(`- P50 (Median): ${result.latency.median.toFixed(3)} ms`);
      sections.push(`- P95: ${result.latency.p95.toFixed(3)} ms`);
      sections.push(`- P99: ${result.latency.p99.toFixed(3)} ms`);
      sections.push(`- Min: ${result.latency.min.toFixed(3)} ms`);
      sections.push(`- Max: ${result.latency.max.toFixed(3)} ms`);
      sections.push(`- Mean: ${result.latency.mean.toFixed(3)} ms`);
      sections.push(`- StdDev: ${result.latency.stdDev.toFixed(3)} ms`);
      sections.push('');
      sections.push('**Throughput:**');
      sections.push(`- Ops/sec: ${result.throughput.opsPerSecond.toFixed(2)}`);
      sections.push(`- Total Operations: ${result.throughput.totalOperations}`);
      sections.push(`- Success Rate: ${((result.throughput.successCount / result.throughput.totalOperations) * 100).toFixed(2)}%`);
      sections.push('');
    }
  }

  return sections.join('\n');
}

function generateComparisonDetails(report: BenchmarkReport): string {
  const sections: string[] = ['## Comparison Analysis'];

  for (const comp of report.comparisons) {
    sections.push(`### ${comp.scenario}`);
    sections.push('');
    sections.push(`- **Winner:** ${comp.winner}`);
    sections.push(`- **Performance Ratio:** ${comp.performanceRatio.toFixed(2)}x`);
    sections.push('');
    sections.push('| Adapter | P95 Latency (ms) |');
    sections.push('|---------|:----------------:|');

    for (const [adapter, p95] of Object.entries(comp.p95Comparison)) {
      const isWinner = adapter === comp.winner ? ' **' : '';
      sections.push(`| ${adapter}${isWinner} | ${p95.toFixed(3)}${isWinner} |`);
    }

    sections.push('');
  }

  return sections.join('\n');
}

function generateHistograms(report: BenchmarkReport): string {
  const sections: string[] = ['## Latency Distributions'];

  for (const [adapter, results] of Object.entries(report.results)) {
    for (const result of results) {
      if (result.rawTimings.length === 0) continue;

      sections.push(`### ${adapter} - ${result.config.name}`);
      sections.push('');
      sections.push('```');
      sections.push(generateAsciiHistogram(result.rawTimings));
      sections.push('```');
      sections.push('');
    }
  }

  return sections.join('\n');
}

function generateAsciiHistogram(timings: number[]): string {
  if (timings.length === 0) return 'No data';

  const sorted = [...timings].sort((a, b) => a - b);
  const min = sorted[0];
  const max = sorted[sorted.length - 1];

  // Create buckets
  const bucketCount = 10;
  const bucketSize = (max - min) / bucketCount || 1;
  const buckets = new Array(bucketCount).fill(0);

  for (const timing of sorted) {
    const bucketIndex = Math.min(Math.floor((timing - min) / bucketSize), bucketCount - 1);
    buckets[bucketIndex]++;
  }

  const maxCount = Math.max(...buckets);
  const lines: string[] = [];

  for (let i = 0; i < bucketCount; i++) {
    const bucketStart = min + i * bucketSize;
    const bucketEnd = min + (i + 1) * bucketSize;
    const barLength = Math.round((buckets[i] / maxCount) * 40);
    const bar = '#'.repeat(barLength);
    lines.push(`${bucketStart.toFixed(2).padStart(8)} - ${bucketEnd.toFixed(2).padEnd(8)} | ${bar} (${buckets[i]})`);
  }

  return lines.join('\n');
}

function generateMethodology(): string {
  return `## Methodology

### Test Environment
- **Runtime:** Cloudflare Workers / Node.js
- **Database:** SQLite (via Durable Objects / D1)

### Measurement Approach
1. **Warmup:** Each scenario begins with warmup iterations that are not counted in results
2. **Timing:** Uses \`performance.now()\` for high-resolution timing
3. **Iterations:** Multiple iterations per scenario for statistical significance
4. **Metrics:** P50 (median), P95, P99 latencies along with min/max/mean/stddev

### Scenarios
- **Simple SELECT:** Point query by primary key
- **INSERT:** Single row insertion
- **Transaction:** Multiple operations in a transaction
- **Batch INSERT:** Bulk data insertion

### Percentile Calculation
Percentiles are calculated using the rank method:
- Sort all timings
- P(n) = timing at position ceil(n/100 * count)

### Statistical Notes
- Standard deviation indicates timing consistency
- High P99/P95 ratio suggests occasional slow queries
- Compare mean vs median to detect skew`;
}

function generateReproductionInstructions(): string {
  return `## Reproduction

### Prerequisites
\`\`\`bash
npm install
\`\`\`

### Run Benchmarks

#### Single Adapter
\`\`\`bash
# DoSQL benchmark
npm run bench:dosql

# All scenarios
npm run bench:all
\`\`\`

#### Comparison Suite
\`\`\`bash
# Run comparison (requires D1 and DO bindings)
npm run bench:compare
\`\`\`

### CLI Options
\`\`\`
-a, --adapter <name>     Adapter: dosql, d1, do-sqlite
-s, --scenario <name>    Scenario: simple-select, insert, transaction
-i, --iterations <n>     Number of iterations (default: 100)
-w, --warmup <n>         Warmup iterations (default: 10)
-f, --format <format>    Output: json, console, markdown
-v, --verbose            Enable verbose logging
\`\`\`

### Environment Variables
\`\`\`bash
BENCH_ITERATIONS=1000     # Override iteration count
BENCH_WARMUP=100          # Override warmup count
BENCH_OUTPUT=results.json # Save results to file
\`\`\``;
}

function generateFooter(report: BenchmarkReport): string {
  return `---

*Benchmark completed at ${report.metadata.timestamp}*

*Generated by DoSQL Benchmark Suite v1.0.0*`;
}

// =============================================================================
// Result Persistence
// =============================================================================

/**
 * Save benchmark results to JSON file
 */
export function serializeResults(report: BenchmarkReport): string {
  return JSON.stringify(report, null, 2);
}

/**
 * Load benchmark results from JSON
 */
export function deserializeResults(json: string): BenchmarkReport {
  return JSON.parse(json) as BenchmarkReport;
}

/**
 * Compare two benchmark reports and generate a diff
 */
export function compareReports(
  baseline: BenchmarkReport,
  current: BenchmarkReport
): ReportComparison {
  const regressions: ScenarioRegression[] = [];
  const improvements: ScenarioImprovement[] = [];

  for (const comparison of current.comparisons) {
    const baselineComp = baseline.comparisons.find(
      (c) => c.scenario === comparison.scenario
    );
    if (!baselineComp) continue;

    for (const adapter of Object.keys(comparison.p95Comparison) as AdapterType[]) {
      const currentP95 = comparison.p95Comparison[adapter];
      const baselineP95 = baselineComp.p95Comparison[adapter];

      if (currentP95 === undefined || baselineP95 === undefined) continue;

      const changePercent = ((currentP95 - baselineP95) / baselineP95) * 100;

      if (changePercent > 10) {
        // >10% slower = regression
        regressions.push({
          scenario: comparison.scenario,
          adapter,
          baselineP95,
          currentP95,
          changePercent,
        });
      } else if (changePercent < -10) {
        // >10% faster = improvement
        improvements.push({
          scenario: comparison.scenario,
          adapter,
          baselineP95,
          currentP95,
          changePercent,
        });
      }
    }
  }

  return {
    baseline: baseline.metadata.timestamp,
    current: current.metadata.timestamp,
    regressions,
    improvements,
    hasRegressions: regressions.length > 0,
  };
}

export interface ScenarioRegression {
  scenario: string;
  adapter: AdapterType;
  baselineP95: number;
  currentP95: number;
  changePercent: number;
}

export interface ScenarioImprovement {
  scenario: string;
  adapter: AdapterType;
  baselineP95: number;
  currentP95: number;
  changePercent: number;
}

export interface ReportComparison {
  baseline: string;
  current: string;
  regressions: ScenarioRegression[];
  improvements: ScenarioImprovement[];
  hasRegressions: boolean;
}

/**
 * Generate a regression report for CI
 */
export function generateRegressionReport(comparison: ReportComparison): string {
  const lines: string[] = [];

  lines.push('# Performance Regression Report');
  lines.push('');
  lines.push(`Baseline: ${comparison.baseline}`);
  lines.push(`Current: ${comparison.current}`);
  lines.push('');

  if (comparison.hasRegressions) {
    lines.push('## Regressions Detected');
    lines.push('');
    lines.push('| Scenario | Adapter | Baseline P95 | Current P95 | Change |');
    lines.push('|----------|---------|:------------:|:-----------:|:------:|');

    for (const reg of comparison.regressions) {
      lines.push(
        `| ${reg.scenario} | ${reg.adapter} | ${reg.baselineP95.toFixed(3)}ms | ${reg.currentP95.toFixed(3)}ms | +${reg.changePercent.toFixed(1)}% |`
      );
    }
    lines.push('');
  }

  if (comparison.improvements.length > 0) {
    lines.push('## Improvements');
    lines.push('');
    lines.push('| Scenario | Adapter | Baseline P95 | Current P95 | Change |');
    lines.push('|----------|---------|:------------:|:-----------:|:------:|');

    for (const imp of comparison.improvements) {
      lines.push(
        `| ${imp.scenario} | ${imp.adapter} | ${imp.baselineP95.toFixed(3)}ms | ${imp.currentP95.toFixed(3)}ms | ${imp.changePercent.toFixed(1)}% |`
      );
    }
    lines.push('');
  }

  if (!comparison.hasRegressions && comparison.improvements.length === 0) {
    lines.push('No significant changes detected.');
  }

  return lines.join('\n');
}

// =============================================================================
// CI Integration
// =============================================================================

/**
 * Format results for GitHub Actions step summary
 */
export function formatForGitHubSummary(report: BenchmarkReport): string {
  const lines: string[] = [];

  lines.push('## Benchmark Results');
  lines.push('');
  lines.push(`| Metric | Value |`);
  lines.push(`|--------|-------|`);
  lines.push(`| Runtime | ${report.metadata.runtime} |`);
  lines.push(`| Duration | ${report.metadata.durationSeconds.toFixed(2)}s |`);
  lines.push(`| Scenarios | ${report.summary.totalScenarios} |`);
  lines.push(`| Winner | ${report.summary.overallWinner || 'N/A'} |`);
  lines.push('');

  // Quick comparison table
  const adapters = Object.keys(report.results) as AdapterType[];
  if (adapters.length > 0) {
    lines.push('### P95 Latency Comparison');
    lines.push('');
    lines.push(`| Scenario | ${adapters.join(' | ')} |`);
    lines.push(`|----------|${adapters.map(() => ':---:').join('|')}|`);

    for (const comp of report.comparisons) {
      const values = adapters.map((a) => {
        const p95 = comp.p95Comparison[a];
        const isWinner = a === comp.winner ? '**' : '';
        return p95 !== undefined ? `${isWinner}${p95.toFixed(3)}ms${isWinner}` : 'N/A';
      });
      lines.push(`| ${comp.scenario} | ${values.join(' | ')} |`);
    }
  }

  return lines.join('\n');
}

/**
 * Determine CI exit code based on regression detection
 */
export function getCIExitCode(
  comparison: ReportComparison,
  options: { allowRegressions?: boolean; maxRegressionPercent?: number } = {}
): number {
  if (options.allowRegressions) {
    return 0;
  }

  const maxRegression = options.maxRegressionPercent ?? 20;

  const criticalRegressions = comparison.regressions.filter(
    (r) => r.changePercent > maxRegression
  );

  if (criticalRegressions.length > 0) {
    return 1;
  }

  return 0;
}
