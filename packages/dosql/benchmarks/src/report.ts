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
} from './types.js';
import { ADAPTER_DISPLAY_NAMES, ADAPTER_DESCRIPTIONS } from './adapters/index.js';
import { SCENARIO_DISPLAY_NAMES } from './scenarios/index.js';

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
  sections.push(generateScenarioResults(report));

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
  return `# SQLite Implementation Benchmark Results

> **Generated:** ${report.metadata.timestamp}
> **Runtime:** ${report.metadata.runtime}${report.metadata.nodeVersion ? ` (${report.metadata.nodeVersion})` : ''}
> **Platform:** ${report.metadata.platform || 'Unknown'}
> **Duration:** ${report.metadata.durationSeconds.toFixed(2)}s

This document contains performance benchmarks comparing DoSQL against other SQLite implementations:
- Native SQLite (better-sqlite3)
- LibSQL (local embedded)
- Turso (edge SQLite via HTTP)
- Cloudflare D1
- Durable Object SQLite`;
}

function generateExecutiveSummary(report: BenchmarkReport): string {
  const { summary } = report;

  let winnerText = '';
  if (summary.overallWinner) {
    const winCount = summary.winsByAdapter[summary.overallWinner] ?? 0;
    const displayName = ADAPTER_DISPLAY_NAMES[summary.overallWinner] || summary.overallWinner;
    winnerText = `**${displayName}** won ${winCount} of ${summary.totalScenarios} scenarios.`;
  } else {
    winnerText = 'No clear winner - results vary by scenario.';
  }

  const adaptersStr = summary.adaptersCompared
    .map((a) => ADAPTER_DISPLAY_NAMES[a] || a)
    .join(', ');

  return `## Executive Summary

- **Adapters Tested:** ${adaptersStr}
- **Total Scenarios:** ${summary.totalScenarios}
- **Overall Winner:** ${winnerText}

### Wins by Adapter

| Adapter | Wins | Description |
|---------|:----:|-------------|
${Object.entries(summary.winsByAdapter)
  .map(([adapter, wins]) => {
    const displayName = ADAPTER_DISPLAY_NAMES[adapter as AdapterType] || adapter;
    const description = ADAPTER_DESCRIPTIONS[adapter as AdapterType] || '';
    return `| ${displayName} | ${wins} | ${description} |`;
  })
  .join('\n')}`;
}

function generateSummaryTable(report: BenchmarkReport): string {
  const adapters = Object.keys(report.results) as AdapterType[];

  if (adapters.length === 0) {
    return '## Summary\n\nNo results available.';
  }

  // Build header row
  const headerRow = `| Scenario | ${adapters.map((a) => `${ADAPTER_DISPLAY_NAMES[a] || a} (ms)`).join(' | ')} | Winner |`;
  const separatorRow = `|:---------|${adapters.map(() => ':----------:').join('|')}|:------:|`;

  // Build data rows from comparisons
  const dataRows = report.comparisons.map((comp) => {
    const scenarioName = SCENARIO_DISPLAY_NAMES[comp.scenario] || comp.scenario;
    const p95Values = adapters.map((adapter) => {
      const p95 = comp.p95Comparison[adapter];
      const isWinner = adapter === comp.winner;
      const value = p95 !== undefined ? p95.toFixed(3) : 'N/A';
      return isWinner ? `**${value}**` : value;
    });
    const winnerName = ADAPTER_DISPLAY_NAMES[comp.winner] || comp.winner;
    return `| ${scenarioName} | ${p95Values.join(' | ')} | ${winnerName} |`;
  });

  return `## Summary Results (P95 Latency)

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
    if (results) {
      for (const result of results) {
        scenarios.add(result.config.type);
      }
    }
  }

  for (const scenario of scenarios) {
    const scenarioName = SCENARIO_DISPLAY_NAMES[scenario as keyof typeof SCENARIO_DISPLAY_NAMES] || scenario;
    rows.push(`### ${scenarioName}`);
    rows.push('');
    rows.push(
      '| Metric | ' +
        adapters.map((a) => ADAPTER_DISPLAY_NAMES[a] || a).join(' | ') +
        ' |'
    );
    rows.push('|:-------|' + adapters.map(() => ':------:').join('|') + '|');

    const metrics: (keyof LatencyStats)[] = [
      'min',
      'median',
      'mean',
      'p95',
      'p99',
      'max',
      'stdDev',
    ];

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

  return `## Detailed Latency Comparison (ms)

${rows.join('\n')}`;
}

function generateScenarioResults(report: BenchmarkReport): string {
  const sections: string[] = ['## Per-Adapter Results'];

  for (const [adapter, results] of Object.entries(report.results)) {
    if (!results) continue;

    const displayName = ADAPTER_DISPLAY_NAMES[adapter as AdapterType] || adapter;
    sections.push(`### ${displayName}`);

    for (const result of results) {
      const scenarioName =
        SCENARIO_DISPLAY_NAMES[result.config.type as keyof typeof SCENARIO_DISPLAY_NAMES] ||
        result.config.name;
      sections.push(`#### ${scenarioName}`);
      sections.push('');
      sections.push(`- **Description:** ${result.config.description}`);
      sections.push(
        `- **Iterations:** ${result.config.iterations} (warmup: ${result.config.warmupIterations})`
      );
      sections.push(`- **Row Count:** ${result.config.rowCount}`);
      sections.push('');
      sections.push('**Latency (ms):**');
      sections.push(`- P50 (Median): ${result.latency.median.toFixed(3)}`);
      sections.push(`- P95: ${result.latency.p95.toFixed(3)}`);
      sections.push(`- P99: ${result.latency.p99.toFixed(3)}`);
      sections.push(`- Min: ${result.latency.min.toFixed(3)}`);
      sections.push(`- Max: ${result.latency.max.toFixed(3)}`);
      sections.push(`- Mean: ${result.latency.mean.toFixed(3)}`);
      sections.push(`- StdDev: ${result.latency.stdDev.toFixed(3)}`);
      sections.push('');
      sections.push('**Throughput:**');
      sections.push(`- Ops/sec: ${result.throughput.opsPerSecond.toFixed(2)}`);
      sections.push(`- Total Operations: ${result.throughput.totalOperations}`);
      sections.push(
        `- Success Rate: ${((result.throughput.successCount / result.throughput.totalOperations) * 100).toFixed(2)}%`
      );

      if (result.memory) {
        sections.push('');
        sections.push('**Memory:**');
        sections.push(`- Heap Used: ${formatBytes(result.memory.heapUsed)}`);
        sections.push(`- Peak: ${formatBytes(result.memory.peakUsage)}`);
      }

      if (result.coldStart) {
        sections.push('');
        sections.push('**Cold Start:**');
        sections.push(`- Time to First Query: ${result.coldStart.timeToFirstQuery.toFixed(2)}ms`);
        sections.push(`- Initialization: ${result.coldStart.initializationTime.toFixed(2)}ms`);
      }

      sections.push('');
    }
  }

  return sections.join('\n');
}

function generateComparisonDetails(report: BenchmarkReport): string {
  const sections: string[] = ['## Performance Analysis'];

  for (const comp of report.comparisons) {
    const scenarioName =
      SCENARIO_DISPLAY_NAMES[comp.scenario as keyof typeof SCENARIO_DISPLAY_NAMES] || comp.scenario;
    const winnerName = ADAPTER_DISPLAY_NAMES[comp.winner] || comp.winner;

    sections.push(`### ${scenarioName}`);
    sections.push('');
    sections.push(`- **Winner:** ${winnerName}`);
    sections.push(`- **Performance Ratio:** ${comp.performanceRatio.toFixed(2)}x`);
    sections.push('');
    sections.push('| Adapter | P95 Latency (ms) |');
    sections.push('|:--------|:----------------:|');

    for (const [adapter, p95] of Object.entries(comp.p95Comparison)) {
      const displayName = ADAPTER_DISPLAY_NAMES[adapter as AdapterType] || adapter;
      const isWinner = adapter === comp.winner;
      const formatted = isWinner ? `**${p95!.toFixed(3)}**` : p95!.toFixed(3);
      sections.push(`| ${displayName} | ${formatted} |`);
    }

    sections.push('');
  }

  return sections.join('\n');
}

function generateHistograms(report: BenchmarkReport): string {
  const sections: string[] = ['## Latency Distributions'];

  for (const [adapter, results] of Object.entries(report.results)) {
    if (!results) continue;

    for (const result of results) {
      if (result.rawTimings.length === 0) continue;

      const displayName = ADAPTER_DISPLAY_NAMES[adapter as AdapterType] || adapter;
      const scenarioName =
        SCENARIO_DISPLAY_NAMES[result.config.type as keyof typeof SCENARIO_DISPLAY_NAMES] ||
        result.config.name;

      sections.push(`### ${displayName} - ${scenarioName}`);
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
    const bucketIndex = Math.min(
      Math.floor((timing - min) / bucketSize),
      bucketCount - 1
    );
    buckets[bucketIndex]++;
  }

  const maxCount = Math.max(...buckets);
  const lines: string[] = [];

  for (let i = 0; i < bucketCount; i++) {
    const bucketStart = min + i * bucketSize;
    const bucketEnd = min + (i + 1) * bucketSize;
    const barLength = Math.round((buckets[i] / maxCount) * 30);
    const bar = '#'.repeat(barLength);
    lines.push(
      `${bucketStart.toFixed(2).padStart(8)} - ${bucketEnd.toFixed(2).padEnd(8)} | ${bar} (${buckets[i]})`
    );
  }

  return lines.join('\n');
}

function generateMethodology(): string {
  return `## Methodology

### Test Environment
- **Runtime:** Node.js / Cloudflare Workers
- **Database:** SQLite-based implementations

### Measurement Approach
1. **Warmup:** Each scenario begins with warmup iterations (not counted)
2. **Timing:** Uses \`performance.now()\` for high-resolution timing
3. **Iterations:** Multiple iterations per scenario for statistical significance
4. **Metrics:** P50, P95, P99 latencies along with min/max/mean/stddev

### Scenarios Tested
- **Simple SELECT:** Point query by primary key
- **INSERT:** Single row insertion
- **Bulk INSERT:** 1000-row batch insertion
- **UPDATE:** Single row update
- **DELETE:** Single row deletion
- **Transaction:** Multiple operations in a transaction
- **Complex JOIN:** Multi-table JOIN with aggregation
- **Cold Start:** Time to first query after initialization

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
cd packages/dosql
npm install
\`\`\`

### Run Benchmarks

#### Quick Benchmark (4 scenarios)
\`\`\`bash
npm run benchmark
\`\`\`

#### Full Benchmark Suite
\`\`\`bash
npm run benchmark:all
\`\`\`

#### Specific Adapters/Scenarios
\`\`\`bash
npm run benchmark -- -a dosql,libsql -s insert,update,transaction
\`\`\`

#### Output Formats
\`\`\`bash
# JSON output
npm run benchmark -- -f json -o results.json

# Markdown report
npm run benchmark -- -f markdown -o BENCHMARKS.md
\`\`\`

#### With Turso
\`\`\`bash
npm run benchmark -- -a turso \\
  --turso-url libsql://your-db.turso.io \\
  --turso-token your-token
\`\`\`

### CLI Options
\`\`\`
-a, --adapters <list>    Comma-separated adapters
-s, --scenarios <list>   Comma-separated scenarios
--all                    Run all scenarios
-i, --iterations <n>     Number of iterations
-w, --warmup <n>         Warmup iterations
-f, --format <type>      Output: json, console, markdown
-o, --output <file>      Write results to file
-v, --verbose            Enable verbose logging
-q, --quick              Quick mode (reduced iterations)
-l, --list               List available adapters/scenarios
\`\`\`

### Environment Variables
\`\`\`bash
TURSO_DATABASE_URL=libsql://...
TURSO_AUTH_TOKEN=...
\`\`\``;
}

function generateFooter(report: BenchmarkReport): string {
  return `---

*Benchmark completed at ${report.metadata.timestamp}*

*Generated by DoSQL Benchmark Suite v1.0.0*`;
}

// =============================================================================
// Utility Functions
// =============================================================================

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)}KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)}MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)}GB`;
}

// =============================================================================
// Result Serialization
// =============================================================================

/**
 * Save benchmark results to JSON
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
