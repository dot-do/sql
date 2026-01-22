/**
 * Benchmark Runner Tests
 *
 * Tests for the benchmark runner and scenarios.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  BenchmarkRunner,
  createBenchmarkRunner,
  parseCLIArgs,
} from '../runner.js';
import { DoSQLAdapter } from '../adapters/dosql.js';
import {
  createSimpleSelectScenario,
  createInsertScenario,
  createTransactionScenario,
} from '../scenarios/index.js';
import type { ScenarioResult, CLIOptions } from '../types.js';

describe('BenchmarkRunner', () => {
  let adapter: DoSQLAdapter;
  let runner: BenchmarkRunner;

  beforeEach(() => {
    adapter = new DoSQLAdapter();
    runner = createBenchmarkRunner({ verbose: false });
  });

  afterEach(async () => {
    // Cleanup is handled in runScenario
  });

  describe('runScenario', () => {
    it('should run simple-select scenario successfully', async () => {
      const config = createSimpleSelectScenario({
        iterations: 10,
        warmupIterations: 2,
        rowCount: 100,
      });

      const result = await runner.runScenario(adapter, config);

      expect(result).toBeDefined();
      expect(result.adapter).toBe('dosql');
      expect(result.config.type).toBe('simple-select');
      expect(result.latency).toBeDefined();
      expect(result.latency.min).toBeGreaterThanOrEqual(0);
      expect(result.latency.max).toBeGreaterThan(0);
      expect(result.latency.p95).toBeGreaterThan(0);
      expect(result.throughput).toBeDefined();
      expect(result.throughput.successCount).toBe(10);
      expect(result.throughput.errorCount).toBe(0);
    });

    it('should run insert scenario successfully', async () => {
      const config = createInsertScenario({
        iterations: 10,
        warmupIterations: 2,
      });

      const result = await runner.runScenario(adapter, config);

      expect(result).toBeDefined();
      expect(result.adapter).toBe('dosql');
      expect(result.config.type).toBe('insert');
      expect(result.throughput.successCount).toBe(10);
      expect(result.throughput.errorCount).toBe(0);
    });

    it('should run transaction scenario successfully', async () => {
      const config = createTransactionScenario({
        iterations: 5,
        warmupIterations: 2,
        rowCount: 50, // Need more seed rows to avoid running out of IDs to delete
      });

      const result = await runner.runScenario(adapter, config);

      expect(result).toBeDefined();
      expect(result.adapter).toBe('dosql');
      expect(result.config.type).toBe('transaction');
      expect(result.throughput.successCount).toBe(5);
      expect(result.throughput.errorCount).toBe(0);
    });
  });

  describe('runAllScenarios', () => {
    it('should run all default scenarios', async () => {
      const results = await runner.runAllScenarios(adapter);

      expect(results).toBeDefined();
      expect(results.length).toBe(3); // simple-select, insert, transaction

      for (const result of results) {
        expect(result.adapter).toBe('dosql');
        expect(result.throughput.successCount).toBeGreaterThan(0);
      }
    });
  });
});

describe('parseCLIArgs', () => {
  it('should parse default options', () => {
    const options = parseCLIArgs([]);

    expect(options.adapter).toBe('dosql');
    expect(options.scenario).toBe('simple-select');
    expect(options.iterations).toBe(100);
    expect(options.warmup).toBe(10);
    expect(options.format).toBe('json');
    expect(options.verbose).toBe(false);
  });

  it('should parse adapter option', () => {
    const options = parseCLIArgs(['-a', 'dosql']);
    expect(options.adapter).toBe('dosql');

    const options2 = parseCLIArgs(['--adapter', 'd1']);
    expect(options2.adapter).toBe('d1');
  });

  it('should parse scenario option', () => {
    const options = parseCLIArgs(['-s', 'insert']);
    expect(options.scenario).toBe('insert');

    const options2 = parseCLIArgs(['--scenario', 'transaction']);
    expect(options2.scenario).toBe('transaction');
  });

  it('should parse iterations option', () => {
    const options = parseCLIArgs(['-i', '500']);
    expect(options.iterations).toBe(500);

    const options2 = parseCLIArgs(['--iterations', '1000']);
    expect(options2.iterations).toBe(1000);
  });

  it('should parse warmup option', () => {
    const options = parseCLIArgs(['-w', '20']);
    expect(options.warmup).toBe(20);

    const options2 = parseCLIArgs(['--warmup', '50']);
    expect(options2.warmup).toBe(50);
  });

  it('should parse format option', () => {
    const options = parseCLIArgs(['-f', 'console']);
    expect(options.format).toBe('console');

    const options2 = parseCLIArgs(['--format', 'json']);
    expect(options2.format).toBe('json');
  });

  it('should parse verbose option', () => {
    const options = parseCLIArgs(['-v']);
    expect(options.verbose).toBe(true);

    const options2 = parseCLIArgs(['--verbose']);
    expect(options2.verbose).toBe(true);
  });

  it('should parse multiple options', () => {
    const options = parseCLIArgs([
      '-a', 'dosql',
      '-s', 'insert',
      '-i', '200',
      '-w', '20',
      '-f', 'console',
      '-v',
    ]);

    expect(options.adapter).toBe('dosql');
    expect(options.scenario).toBe('insert');
    expect(options.iterations).toBe(200);
    expect(options.warmup).toBe(20);
    expect(options.format).toBe('console');
    expect(options.verbose).toBe(true);
  });
});

describe('ScenarioResult', () => {
  let adapter: DoSQLAdapter;
  let runner: BenchmarkRunner;

  beforeEach(() => {
    adapter = new DoSQLAdapter();
    runner = createBenchmarkRunner({ verbose: false });
  });

  it('should calculate correct latency statistics', async () => {
    const config = createSimpleSelectScenario({
      iterations: 20,
      warmupIterations: 5,
      rowCount: 100,
    });

    const result = await runner.runScenario(adapter, config);

    // Verify latency stats make sense
    expect(result.latency.min).toBeLessThanOrEqual(result.latency.median);
    expect(result.latency.median).toBeLessThanOrEqual(result.latency.p95);
    expect(result.latency.p95).toBeLessThanOrEqual(result.latency.p99);
    expect(result.latency.p99).toBeLessThanOrEqual(result.latency.max);
    expect(result.latency.mean).toBeGreaterThan(0);
    expect(result.latency.stdDev).toBeGreaterThanOrEqual(0);
  });

  it('should have correct timestamps', async () => {
    const config = createSimpleSelectScenario({
      iterations: 5,
      warmupIterations: 2,
      rowCount: 50,
    });

    const beforeRun = new Date().toISOString();
    const result = await runner.runScenario(adapter, config);
    const afterRun = new Date().toISOString();

    expect(result.startedAt).toBeDefined();
    expect(result.completedAt).toBeDefined();
    expect(result.startedAt >= beforeRun).toBe(true);
    expect(result.completedAt <= afterRun).toBe(true);
    expect(result.startedAt <= result.completedAt).toBe(true);
  });

  it('should have correct raw timings count', async () => {
    const iterations = 15;
    const config = createSimpleSelectScenario({
      iterations,
      warmupIterations: 3,
      rowCount: 50,
    });

    const result = await runner.runScenario(adapter, config);

    expect(result.rawTimings.length).toBe(iterations);
    expect(result.throughput.totalOperations).toBe(iterations);
  });
});

describe('Scenario Edge Cases', () => {
  let adapter: DoSQLAdapter;
  let runner: BenchmarkRunner;

  beforeEach(() => {
    adapter = new DoSQLAdapter();
    runner = createBenchmarkRunner({ verbose: false });
  });

  it('should handle minimum iterations', async () => {
    const config = createSimpleSelectScenario({
      iterations: 1,
      warmupIterations: 1,
      rowCount: 10,
    });

    const result = await runner.runScenario(adapter, config);

    expect(result.throughput.successCount).toBe(1);
    expect(result.rawTimings.length).toBe(1);
  });

  it('should handle zero warmup iterations', async () => {
    const config = createInsertScenario({
      iterations: 5,
      warmupIterations: 0,
    });

    const result = await runner.runScenario(adapter, config);

    expect(result.throughput.successCount).toBe(5);
  });

  it('should handle large row counts', async () => {
    const config = createSimpleSelectScenario({
      iterations: 5,
      warmupIterations: 2,
      rowCount: 5000,
    });

    const result = await runner.runScenario(adapter, config);

    expect(result.throughput.successCount).toBe(5);
    expect(result.throughput.errorCount).toBe(0);
  });
});
