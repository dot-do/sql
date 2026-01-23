/**
 * Report Generator Tests
 *
 * Tests for the markdown report generator and CI integration.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  generateMarkdownReport,
  serializeResults,
  deserializeResults,
  compareReports,
  generateRegressionReport,
  formatForGitHubSummary,
  getCIExitCode,
} from '../report.js';
import type { BenchmarkReport, ScenarioResult, LatencyStats } from '../types.js';

// =============================================================================
// Test Fixtures
// =============================================================================

function createLatencyStats(base: number): LatencyStats {
  return {
    min: base * 0.5,
    max: base * 2,
    mean: base,
    median: base * 0.9,
    p95: base * 1.5,
    p99: base * 1.8,
    stdDev: base * 0.2,
  };
}

function createScenarioResult(
  scenarioType: string,
  adapter: string,
  baseLatency: number
): ScenarioResult {
  return {
    config: {
      type: scenarioType as 'simple-select',
      name: `${scenarioType} Test`,
      description: `Test ${scenarioType}`,
      iterations: 100,
      warmupIterations: 10,
      rowCount: 1000,
    },
    adapter,
    latency: createLatencyStats(baseLatency),
    throughput: {
      opsPerSecond: 1000 / baseLatency,
      totalOperations: 100,
      successCount: 100,
      errorCount: 0,
      errorRate: 0,
    },
    rawTimings: Array.from({ length: 100 }, (_, i) => baseLatency * (0.8 + Math.random() * 0.4)),
    startedAt: '2024-01-01T00:00:00.000Z',
    completedAt: '2024-01-01T00:01:00.000Z',
    errors: [],
  };
}

function createReport(adapters: string[], scenarios: string[]): BenchmarkReport {
  const results: Record<string, ScenarioResult[]> = {};
  const comparisons = [];

  for (const adapter of adapters) {
    results[adapter] = scenarios.map((scenario, i) =>
      createScenarioResult(scenario, adapter, 1 + i * 0.5 + (adapter === 'dosql' ? 0 : 0.2))
    );
  }

  for (const scenario of scenarios) {
    const p95Comparison: Record<string, number> = {};
    let minP95 = Infinity;
    let winner = adapters[0];

    for (const adapter of adapters) {
      const result = results[adapter].find((r) => r.config.type === scenario);
      if (result) {
        p95Comparison[adapter] = result.latency.p95;
        if (result.latency.p95 < minP95) {
          minP95 = result.latency.p95;
          winner = adapter;
        }
      }
    }

    comparisons.push({
      scenario,
      results: {} as Record<string, ScenarioResult>,
      winner,
      performanceRatio: Math.max(...Object.values(p95Comparison)) / minP95,
      p95Comparison: p95Comparison as Record<'dosql' | 'd1' | 'do-sqlite', number>,
    });
  }

  const winsByAdapter: Record<string, number> = {};
  for (const adapter of adapters) {
    winsByAdapter[adapter] = comparisons.filter((c) => c.winner === adapter).length;
  }

  return {
    metadata: {
      name: 'Test Benchmark',
      timestamp: '2024-01-01T00:00:00.000Z',
      durationSeconds: 60,
      runtime: 'node',
    },
    results: results as Record<'dosql' | 'd1' | 'do-sqlite', ScenarioResult[]>,
    comparisons: comparisons as BenchmarkReport['comparisons'],
    summary: {
      totalScenarios: scenarios.length,
      adaptersCompared: adapters as ('dosql' | 'd1' | 'do-sqlite')[],
      overallWinner: Object.entries(winsByAdapter).sort((a, b) => b[1] - a[1])[0]?.[0] as 'dosql' | null,
      winsByAdapter: winsByAdapter as Record<'dosql' | 'd1' | 'do-sqlite', number>,
    },
  };
}

// =============================================================================
// Report Generation Tests
// =============================================================================

describe('generateMarkdownReport', () => {
  it('should generate markdown report with all sections', () => {
    const report = createReport(['dosql', 'd1'], ['simple-select', 'insert']);
    const markdown = generateMarkdownReport(report);

    expect(markdown).toContain('# Performance Benchmarks');
    expect(markdown).toContain('## Executive Summary');
    expect(markdown).toContain('## Summary Results');
    expect(markdown).toContain('## Latency Comparison');
    expect(markdown).toContain('## Detailed Scenario Results');
    expect(markdown).toContain('## Methodology');
    expect(markdown).toContain('## Reproduction');
  });

  it('should include comparison tables', () => {
    const report = createReport(['dosql', 'd1'], ['simple-select']);
    const markdown = generateMarkdownReport(report);

    expect(markdown).toContain('| Scenario |');
    expect(markdown).toContain('dosql');
    expect(markdown).toContain('d1');
  });

  it('should include latency metrics', () => {
    const report = createReport(['dosql'], ['simple-select']);
    const markdown = generateMarkdownReport(report);

    expect(markdown).toContain('P50');
    expect(markdown).toContain('P95');
    expect(markdown).toContain('P99');
    expect(markdown).toContain('Min');
    expect(markdown).toContain('Max');
  });

  it('should include histogram when configured', () => {
    const report = createReport(['dosql'], ['simple-select']);
    const markdown = generateMarkdownReport(report, { includeHistograms: true });

    expect(markdown).toContain('## Latency Distributions');
  });

  it('should respect configuration options', () => {
    const report = createReport(['dosql'], ['simple-select']);

    const withMethodology = generateMarkdownReport(report, { includeMethodology: true });
    expect(withMethodology).toContain('## Methodology');

    const withoutMethodology = generateMarkdownReport(report, { includeMethodology: false });
    expect(withoutMethodology).not.toContain('## Methodology');
  });
});

// =============================================================================
// Serialization Tests
// =============================================================================

describe('Result Serialization', () => {
  it('should serialize results to JSON', () => {
    const report = createReport(['dosql'], ['simple-select']);
    const json = serializeResults(report);

    expect(typeof json).toBe('string');
    expect(() => JSON.parse(json)).not.toThrow();
  });

  it('should deserialize results from JSON', () => {
    const original = createReport(['dosql'], ['simple-select']);
    const json = serializeResults(original);
    const restored = deserializeResults(json);

    expect(restored.metadata.name).toBe(original.metadata.name);
    expect(restored.summary.totalScenarios).toBe(original.summary.totalScenarios);
  });

  it('should preserve all data through round-trip', () => {
    const original = createReport(['dosql', 'd1'], ['simple-select', 'insert']);
    const restored = deserializeResults(serializeResults(original));

    expect(restored.results.dosql.length).toBe(original.results.dosql.length);
    expect(restored.comparisons.length).toBe(original.comparisons.length);
  });
});

// =============================================================================
// Report Comparison Tests
// =============================================================================

describe('compareReports', () => {
  it('should detect regressions', () => {
    const baseline = createReport(['dosql'], ['simple-select']);
    const current = createReport(['dosql'], ['simple-select']);

    // Make current 50% slower
    current.results.dosql[0].latency.p95 *= 1.5;
    current.comparisons[0].p95Comparison.dosql *= 1.5;

    const comparison = compareReports(baseline, current);

    expect(comparison.hasRegressions).toBe(true);
    expect(comparison.regressions.length).toBeGreaterThan(0);
  });

  it('should detect improvements', () => {
    const baseline = createReport(['dosql'], ['simple-select']);
    const current = createReport(['dosql'], ['simple-select']);

    // Make current 50% faster
    current.results.dosql[0].latency.p95 *= 0.5;
    current.comparisons[0].p95Comparison.dosql *= 0.5;

    const comparison = compareReports(baseline, current);

    expect(comparison.improvements.length).toBeGreaterThan(0);
  });

  it('should ignore small changes', () => {
    const baseline = createReport(['dosql'], ['simple-select']);
    const current = createReport(['dosql'], ['simple-select']);

    // Make current 5% slower (below threshold)
    current.results.dosql[0].latency.p95 *= 1.05;
    current.comparisons[0].p95Comparison.dosql *= 1.05;

    const comparison = compareReports(baseline, current);

    expect(comparison.regressions.length).toBe(0);
    expect(comparison.improvements.length).toBe(0);
  });
});

// =============================================================================
// Regression Report Tests
// =============================================================================

describe('generateRegressionReport', () => {
  it('should generate report for regressions', () => {
    const baseline = createReport(['dosql'], ['simple-select']);
    const current = createReport(['dosql'], ['simple-select']);
    current.comparisons[0].p95Comparison.dosql *= 1.5;

    const comparison = compareReports(baseline, current);
    const report = generateRegressionReport(comparison);

    expect(report).toContain('# Performance Regression Report');
    expect(report).toContain('Regressions Detected');
  });

  it('should show no changes when stable', () => {
    const baseline = createReport(['dosql'], ['simple-select']);
    const current = createReport(['dosql'], ['simple-select']);

    const comparison = compareReports(baseline, current);
    const report = generateRegressionReport(comparison);

    expect(report).toContain('No significant changes detected');
  });
});

// =============================================================================
// GitHub Actions Integration Tests
// =============================================================================

describe('formatForGitHubSummary', () => {
  it('should generate valid markdown for GitHub summary', () => {
    const report = createReport(['dosql', 'd1'], ['simple-select']);
    const summary = formatForGitHubSummary(report);

    expect(summary).toContain('## Benchmark Results');
    expect(summary).toContain('| Metric | Value |');
    expect(summary).toContain('P95 Latency Comparison');
  });

  it('should highlight winner in comparison', () => {
    const report = createReport(['dosql', 'd1'], ['simple-select']);
    const summary = formatForGitHubSummary(report);

    // Winner should be highlighted with **
    expect(summary).toMatch(/\*\*\d+\.\d+ms\*\*/);
  });
});

describe('getCIExitCode', () => {
  it('should return 0 when no regressions', () => {
    const comparison = {
      baseline: '2024-01-01',
      current: '2024-01-02',
      regressions: [],
      improvements: [],
      hasRegressions: false,
    };

    expect(getCIExitCode(comparison)).toBe(0);
  });

  it('should return 1 when critical regressions exist', () => {
    const comparison = {
      baseline: '2024-01-01',
      current: '2024-01-02',
      regressions: [
        {
          scenario: 'simple-select',
          adapter: 'dosql' as const,
          baselineP95: 1,
          currentP95: 1.5,
          changePercent: 50, // 50% regression
        },
      ],
      improvements: [],
      hasRegressions: true,
    };

    expect(getCIExitCode(comparison)).toBe(1);
  });

  it('should respect allowRegressions option', () => {
    const comparison = {
      baseline: '2024-01-01',
      current: '2024-01-02',
      regressions: [
        {
          scenario: 'simple-select',
          adapter: 'dosql' as const,
          baselineP95: 1,
          currentP95: 1.5,
          changePercent: 50,
        },
      ],
      improvements: [],
      hasRegressions: true,
    };

    expect(getCIExitCode(comparison, { allowRegressions: true })).toBe(0);
  });

  it('should respect maxRegressionPercent option', () => {
    const comparison = {
      baseline: '2024-01-01',
      current: '2024-01-02',
      regressions: [
        {
          scenario: 'simple-select',
          adapter: 'dosql' as const,
          baselineP95: 1,
          currentP95: 1.15,
          changePercent: 15, // 15% regression
        },
      ],
      improvements: [],
      hasRegressions: true,
    };

    // 15% is below 20% threshold, should pass
    expect(getCIExitCode(comparison, { maxRegressionPercent: 20 })).toBe(0);

    // 15% is above 10% threshold, should fail
    expect(getCIExitCode(comparison, { maxRegressionPercent: 10 })).toBe(1);
  });
});
