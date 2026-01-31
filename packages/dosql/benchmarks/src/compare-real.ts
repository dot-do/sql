#!/usr/bin/env npx tsx
/**
 * Real Performance Benchmark Comparison
 *
 * Compares DoSQL (REAL engine) vs better-sqlite3 vs LibSQL
 * with realistic workloads and accurate latency metrics.
 *
 * Usage:
 *   npx tsx benchmarks/src/compare-real.ts
 *   npx tsx benchmarks/src/compare-real.ts --quick
 *   npx tsx benchmarks/src/compare-real.ts --verbose
 */

import { DoSQLRealAdapter } from './adapters/dosql-real.js';
import { BetterSQLite3Adapter } from './adapters/better-sqlite3.js';
import { LibSQLAdapter } from './adapters/libsql.js';
import type { BenchmarkAdapter, LatencyStats } from './types.js';
import { calculateLatencyStats } from './types.js';

// =============================================================================
// Configuration
// =============================================================================

interface BenchmarkConfig {
  iterations: number;
  warmupIterations: number;
  rowCount: number;
  verbose: boolean;
}

const DEFAULT_CONFIG: BenchmarkConfig = {
  iterations: 1000,
  warmupIterations: 100,
  rowCount: 10000,
  verbose: false,
};

const QUICK_CONFIG: BenchmarkConfig = {
  iterations: 100,
  warmupIterations: 10,
  rowCount: 1000,
  verbose: false,
};

// =============================================================================
// Benchmark Scenarios
// =============================================================================

interface BenchmarkResult {
  name: string;
  adapter: string;
  latency: LatencyStats;
  opsPerSecond: number;
  errors: number;
}

/**
 * Simple SELECT by primary key
 */
// Counter for unique table names
let tableCounter = 0;
function getUniqueTableName(prefix: string): string {
  return `${prefix}_${Date.now()}_${++tableCounter}`;
}

/**
 * Simple SELECT by primary key
 */
async function benchmarkSelectByPK(
  adapter: BenchmarkAdapter,
  config: BenchmarkConfig
): Promise<BenchmarkResult> {
  const tableName = getUniqueTableName('bench_select_pk');
  const timings: number[] = [];
  let errors = 0;

  // Setup
  await adapter.createTable({
    tableName,
    columns: [
      { name: 'id', type: 'INTEGER' },
      { name: 'name', type: 'TEXT' },
      { name: 'value', type: 'REAL' },
      { name: 'data', type: 'TEXT' },
    ],
    primaryKey: 'id',
  });

  // Seed data
  const rows = Array.from({ length: config.rowCount }, (_, i) => ({
    id: i + 1,
    name: `name_${i + 1}`,
    value: Math.random() * 1000,
    data: `data_${i + 1}_${Date.now()}`,
  }));
  await adapter.insertBatch(tableName, rows);

  // Warmup
  for (let i = 0; i < config.warmupIterations; i++) {
    const id = Math.floor(Math.random() * config.rowCount) + 1;
    await adapter.read(tableName, 'id', id);
  }

  // Benchmark
  for (let i = 0; i < config.iterations; i++) {
    const id = Math.floor(Math.random() * config.rowCount) + 1;
    const result = await adapter.read(tableName, 'id', id);
    if (result.success) {
      timings.push(result.durationMs);
    } else {
      errors++;
    }
  }

  // Cleanup
  await adapter.dropTable(tableName);

  const latency = calculateLatencyStats(timings);
  const totalMs = timings.reduce((a, b) => a + b, 0);

  return {
    name: 'SELECT by PK',
    adapter: adapter.name,
    latency,
    opsPerSecond: totalMs > 0 ? (timings.length / totalMs) * 1000 : 0,
    errors,
  };
}

/**
 * INSERT single row
 */
async function benchmarkInsertSingle(
  adapter: BenchmarkAdapter,
  config: BenchmarkConfig
): Promise<BenchmarkResult> {
  const tableName = getUniqueTableName('bench_insert_single');
  const timings: number[] = [];
  let errors = 0;

  // Setup
  await adapter.createTable({
    tableName,
    columns: [
      { name: 'id', type: 'INTEGER' },
      { name: 'name', type: 'TEXT' },
      { name: 'value', type: 'REAL' },
    ],
    primaryKey: 'id',
  });

  // Warmup
  for (let i = 0; i < config.warmupIterations; i++) {
    await adapter.insert(tableName, {
      id: i + 1,
      name: `warmup_${i}`,
      value: Math.random() * 1000,
    });
  }

  // Clear and restart
  await adapter.dropTable(tableName);
  await adapter.createTable({
    tableName,
    columns: [
      { name: 'id', type: 'INTEGER' },
      { name: 'name', type: 'TEXT' },
      { name: 'value', type: 'REAL' },
    ],
    primaryKey: 'id',
  });

  // Benchmark
  for (let i = 0; i < config.iterations; i++) {
    const result = await adapter.insert(tableName, {
      id: i + 1,
      name: `row_${i}`,
      value: Math.random() * 1000,
    });
    if (result.success) {
      timings.push(result.durationMs);
    } else {
      errors++;
    }
  }

  // Cleanup
  await adapter.dropTable(tableName);

  const latency = calculateLatencyStats(timings);
  const totalMs = timings.reduce((a, b) => a + b, 0);

  return {
    name: 'INSERT single',
    adapter: adapter.name,
    latency,
    opsPerSecond: totalMs > 0 ? (timings.length / totalMs) * 1000 : 0,
    errors,
  };
}

/**
 * UPDATE with WHERE clause
 */
async function benchmarkUpdateWhere(
  adapter: BenchmarkAdapter,
  config: BenchmarkConfig
): Promise<BenchmarkResult> {
  const tableName = getUniqueTableName('bench_update_where');
  const timings: number[] = [];
  let errors = 0;

  // Setup
  await adapter.createTable({
    tableName,
    columns: [
      { name: 'id', type: 'INTEGER' },
      { name: 'name', type: 'TEXT' },
      { name: 'value', type: 'REAL' },
      { name: 'counter', type: 'INTEGER' },
    ],
    primaryKey: 'id',
    indexes: ['name'],
  });

  // Seed data
  const rows = Array.from({ length: config.rowCount }, (_, i) => ({
    id: i + 1,
    name: `name_${i + 1}`,
    value: Math.random() * 1000,
    counter: 0,
  }));
  await adapter.insertBatch(tableName, rows);

  // Warmup
  for (let i = 0; i < config.warmupIterations; i++) {
    const id = Math.floor(Math.random() * config.rowCount) + 1;
    await adapter.update(tableName, 'id', id, { counter: i });
  }

  // Benchmark
  for (let i = 0; i < config.iterations; i++) {
    const id = Math.floor(Math.random() * config.rowCount) + 1;
    const result = await adapter.update(tableName, 'id', id, {
      value: Math.random() * 1000,
      counter: i,
    });
    if (result.success) {
      timings.push(result.durationMs);
    } else {
      errors++;
    }
  }

  // Cleanup
  await adapter.dropTable(tableName);

  const latency = calculateLatencyStats(timings);
  const totalMs = timings.reduce((a, b) => a + b, 0);

  return {
    name: 'UPDATE by PK',
    adapter: adapter.name,
    latency,
    opsPerSecond: totalMs > 0 ? (timings.length / totalMs) * 1000 : 0,
    errors,
  };
}

/**
 * SELECT with JOIN
 */
async function benchmarkSelectJoin(
  adapter: BenchmarkAdapter,
  config: BenchmarkConfig
): Promise<BenchmarkResult> {
  const usersTable = getUniqueTableName('users');
  const ordersTable = getUniqueTableName('orders');
  const timings: number[] = [];
  let errors = 0;

  // Setup tables
  await adapter.createTable({
    tableName: usersTable,
    columns: [
      { name: 'id', type: 'INTEGER' },
      { name: 'name', type: 'TEXT' },
      { name: 'email', type: 'TEXT' },
    ],
    primaryKey: 'id',
  });

  await adapter.createTable({
    tableName: ordersTable,
    columns: [
      { name: 'id', type: 'INTEGER' },
      { name: 'user_id', type: 'INTEGER' },
      { name: 'total', type: 'REAL' },
      { name: 'status', type: 'TEXT' },
    ],
    primaryKey: 'id',
    indexes: ['user_id'],
  });

  // Seed data
  const users = Array.from({ length: Math.min(config.rowCount, 1000) }, (_, i) => ({
    id: i + 1,
    name: `User ${i + 1}`,
    email: `user${i + 1}@example.com`,
  }));
  await adapter.insertBatch(usersTable, users);

  const orders = Array.from({ length: config.rowCount }, (_, i) => ({
    id: i + 1,
    user_id: (i % 1000) + 1,
    total: Math.random() * 1000,
    status: ['pending', 'shipped', 'delivered'][i % 3],
  }));
  await adapter.insertBatch(ordersTable, orders);

  // Build the JOIN query (single line to avoid parser issues)
  const joinQuery = `SELECT ${usersTable}.name, ${ordersTable}.total, ${ordersTable}.status FROM ${usersTable} JOIN ${ordersTable} ON ${usersTable}.id = ${ordersTable}.user_id WHERE ${usersTable}.id = :userId`;

  // Warmup
  for (let i = 0; i < Math.min(config.warmupIterations, 50); i++) {
    const userId = Math.floor(Math.random() * Math.min(config.rowCount, 1000)) + 1;
    await adapter.query(joinQuery, { userId });
  }

  // Benchmark
  const joinIterations = Math.min(config.iterations, 500); // JOINs are slower
  for (let i = 0; i < joinIterations; i++) {
    const userId = Math.floor(Math.random() * Math.min(config.rowCount, 1000)) + 1;
    const result = await adapter.query(joinQuery, { userId });
    if (result.success) {
      timings.push(result.durationMs);
    } else {
      errors++;
      if (config.verbose && errors <= 3) {
        console.log(`    JOIN error: ${result.error}`);
      }
    }
  }

  // Cleanup
  await adapter.dropTable(ordersTable);
  await adapter.dropTable(usersTable);

  const latency = calculateLatencyStats(timings);
  const totalMs = timings.reduce((a, b) => a + b, 0);

  return {
    name: 'SELECT with JOIN',
    adapter: adapter.name,
    latency,
    opsPerSecond: totalMs > 0 ? (timings.length / totalMs) * 1000 : 0,
    errors,
  };
}

/**
 * Transaction with multiple statements
 */
async function benchmarkTransaction(
  adapter: BenchmarkAdapter,
  config: BenchmarkConfig
): Promise<BenchmarkResult> {
  const tableName = getUniqueTableName('bench_transaction');
  const timings: number[] = [];
  let errors = 0;

  // Setup
  await adapter.createTable({
    tableName,
    columns: [
      { name: 'id', type: 'INTEGER' },
      { name: 'name', type: 'TEXT' },
      { name: 'balance', type: 'REAL' },
    ],
    primaryKey: 'id',
  });

  // Seed initial data
  const rows = Array.from({ length: 100 }, (_, i) => ({
    id: i + 1,
    name: `account_${i + 1}`,
    balance: 1000.0,
  }));
  await adapter.insertBatch(tableName, rows);

  // Warmup
  for (let i = 0; i < Math.min(config.warmupIterations, 20); i++) {
    await adapter.transaction([
      { type: 'update', tableName, data: { id: 1, balance: 999 } },
      { type: 'update', tableName, data: { id: 2, balance: 1001 } },
    ]);
  }

  // Reset balances
  for (let i = 1; i <= 100; i++) {
    await adapter.update(tableName, 'id', i, { balance: 1000.0 });
  }

  // Benchmark
  const txnIterations = Math.min(config.iterations, 200);
  for (let i = 0; i < txnIterations; i++) {
    const from = (i % 100) + 1;
    const to = ((i + 1) % 100) + 1;
    const amount = Math.random() * 10;

    const result = await adapter.transaction([
      { type: 'update', tableName, data: { id: from, balance: 1000 - amount } },
      { type: 'update', tableName, data: { id: to, balance: 1000 + amount } },
      { type: 'insert', tableName, data: { id: 100 + i + 1, name: `txn_${i}`, balance: 0 } },
    ]);
    if (result.success) {
      timings.push(result.durationMs);
    } else {
      errors++;
    }
  }

  // Cleanup
  await adapter.dropTable(tableName);

  const latency = calculateLatencyStats(timings);
  const totalMs = timings.reduce((a, b) => a + b, 0);

  return {
    name: 'Transaction (3 ops)',
    adapter: adapter.name,
    latency,
    opsPerSecond: totalMs > 0 ? (timings.length / totalMs) * 1000 : 0,
    errors,
  };
}

/**
 * SELECT with aggregate (COUNT, SUM, AVG)
 */
async function benchmarkAggregate(
  adapter: BenchmarkAdapter,
  config: BenchmarkConfig
): Promise<BenchmarkResult> {
  const tableName = getUniqueTableName('bench_aggregate');
  const timings: number[] = [];
  let errors = 0;

  // Setup
  await adapter.createTable({
    tableName,
    columns: [
      { name: 'id', type: 'INTEGER' },
      { name: 'category', type: 'TEXT' },
      { name: 'amount', type: 'REAL' },
    ],
    primaryKey: 'id',
    indexes: ['category'],
  });

  // Seed data
  const categories = ['A', 'B', 'C', 'D', 'E'];
  const rows = Array.from({ length: config.rowCount }, (_, i) => ({
    id: i + 1,
    category: categories[i % categories.length],
    amount: Math.random() * 1000,
  }));
  await adapter.insertBatch(tableName, rows);

  // Warmup
  for (let i = 0; i < config.warmupIterations; i++) {
    const cat = categories[i % categories.length];
    await adapter.query(
      `SELECT category, COUNT(*) as cnt, SUM(amount) as total, AVG(amount) as avg
       FROM ${tableName}
       WHERE category = :category
       GROUP BY category`,
      { category: cat }
    );
  }

  // Benchmark
  for (let i = 0; i < config.iterations; i++) {
    const cat = categories[i % categories.length];
    const result = await adapter.query(
      `SELECT category, COUNT(*) as cnt, SUM(amount) as total, AVG(amount) as avg
       FROM ${tableName}
       WHERE category = :category
       GROUP BY category`,
      { category: cat }
    );
    if (result.success) {
      timings.push(result.durationMs);
    } else {
      errors++;
    }
  }

  // Cleanup
  await adapter.dropTable(tableName);

  const latency = calculateLatencyStats(timings);
  const totalMs = timings.reduce((a, b) => a + b, 0);

  return {
    name: 'SELECT with aggregates',
    adapter: adapter.name,
    latency,
    opsPerSecond: totalMs > 0 ? (timings.length / totalMs) * 1000 : 0,
    errors,
  };
}

// =============================================================================
// Report Generation
// =============================================================================

function formatLatency(value: number): string {
  if (value < 0.001) {
    return `${(value * 1000000).toFixed(0)}ns`;
  }
  if (value < 1) {
    return `${(value * 1000).toFixed(2)}us`;
  }
  return `${value.toFixed(3)}ms`;
}

function printResultsTable(results: BenchmarkResult[][]): void {
  // Group by scenario
  const scenariosSet = new Set(results.flat().map((r) => r.name));
  const scenarios = Array.from(scenariosSet);
  const adapters = Array.from(new Set(results.flat().map((r) => r.adapter)));

  console.log('\n' + '='.repeat(100));
  console.log('  BENCHMARK RESULTS: DoSQL (Real Engine) vs better-sqlite3 vs LibSQL');
  console.log('='.repeat(100));

  // Header
  const header = ['Scenario'.padEnd(22)];
  for (const adapter of adapters) {
    header.push(adapter.padStart(16));
  }
  header.push('Winner'.padStart(14));
  console.log('\n  ' + header.join(' | '));
  console.log('  ' + '-'.repeat(96));

  // P50 (median) latency comparison
  console.log('\n  P50 (median) Latency:');
  for (const scenario of scenarios) {
    const row = [scenario.padEnd(22)];
    let minLatency = Infinity;
    let winner = '';

    for (const adapter of adapters) {
      const result = results.flat().find((r) => r.name === scenario && r.adapter === adapter);
      if (result && result.opsPerSecond > 0) {
        row.push(formatLatency(result.latency.median).padStart(16));
        if (result.latency.median < minLatency && result.latency.median > 0) {
          minLatency = result.latency.median;
          winner = adapter;
        }
      } else {
        row.push('N/A'.padStart(16));
      }
    }
    row.push(winner.padStart(14));
    console.log('  ' + row.join(' | '));
  }

  // P95 latency comparison
  console.log('\n  P95 Latency:');
  for (const scenario of scenarios) {
    const row = [scenario.padEnd(22)];
    let minLatency = Infinity;
    let winner = '';

    for (const adapter of adapters) {
      const result = results.flat().find((r) => r.name === scenario && r.adapter === adapter);
      if (result && result.opsPerSecond > 0) {
        row.push(formatLatency(result.latency.p95).padStart(16));
        if (result.latency.p95 < minLatency && result.latency.p95 > 0) {
          minLatency = result.latency.p95;
          winner = adapter;
        }
      } else {
        row.push('N/A'.padStart(16));
      }
    }
    row.push(winner.padStart(14));
    console.log('  ' + row.join(' | '));
  }

  // P99 latency comparison
  console.log('\n  P99 Latency:');
  for (const scenario of scenarios) {
    const row = [scenario.padEnd(22)];
    let minLatency = Infinity;
    let winner = '';

    for (const adapter of adapters) {
      const result = results.flat().find((r) => r.name === scenario && r.adapter === adapter);
      if (result && result.opsPerSecond > 0) {
        row.push(formatLatency(result.latency.p99).padStart(16));
        if (result.latency.p99 < minLatency && result.latency.p99 > 0) {
          minLatency = result.latency.p99;
          winner = adapter;
        }
      } else {
        row.push('N/A'.padStart(16));
      }
    }
    row.push(winner.padStart(14));
    console.log('  ' + row.join(' | '));
  }

  // Min/Max latency
  console.log('\n  Latency Range (min-max):');
  for (const scenario of scenarios) {
    const row = [scenario.padEnd(22)];

    for (const adapter of adapters) {
      const result = results.flat().find((r) => r.name === scenario && r.adapter === adapter);
      if (result && result.opsPerSecond > 0) {
        row.push(`${formatLatency(result.latency.min)}-${formatLatency(result.latency.max)}`.padStart(16));
      } else {
        row.push('N/A'.padStart(16));
      }
    }
    row.push('');
    console.log('  ' + row.join(' | '));
  }

  // Throughput
  console.log('\n  Throughput (ops/sec):');
  for (const scenario of scenarios) {
    const row = [scenario.padEnd(22)];
    let maxOps = 0;
    let winner = '';

    for (const adapter of adapters) {
      const result = results.flat().find((r) => r.name === scenario && r.adapter === adapter);
      if (result) {
        row.push(result.opsPerSecond.toFixed(0).padStart(16));
        if (result.opsPerSecond > maxOps) {
          maxOps = result.opsPerSecond;
          winner = adapter;
        }
      } else {
        row.push('N/A'.padStart(16));
      }
    }
    row.push(winner.padStart(14));
    console.log('  ' + row.join(' | '));
  }

  console.log('\n' + '='.repeat(100));

  // Summary
  const wins: Record<string, number> = {};
  for (const adapter of adapters) {
    wins[adapter] = 0;
  }

  for (const scenario of scenarios) {
    let minLatency = Infinity;
    let winner = '';
    for (const adapter of adapters) {
      const result = results.flat().find((r) => r.name === scenario && r.adapter === adapter);
      // Only count results with actual ops (not 0 ops/sec which means all errors)
      if (result && result.opsPerSecond > 0 && result.latency.p95 < minLatency && result.latency.p95 > 0) {
        minLatency = result.latency.p95;
        winner = adapter;
      }
    }
    if (winner) {
      wins[winner]++;
    }
  }

  console.log('\n  Summary (by P95 latency wins):');
  for (const [adapter, count] of Object.entries(wins).sort((a, b) => b[1] - a[1])) {
    console.log(`    ${adapter}: ${count} scenario(s)`);
  }

  console.log('\n' + '='.repeat(100) + '\n');
}

// =============================================================================
// Main
// =============================================================================

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const isQuick = args.includes('--quick') || args.includes('-q');
  const isVerbose = args.includes('--verbose') || args.includes('-v');

  const config: BenchmarkConfig = isQuick
    ? { ...QUICK_CONFIG, verbose: isVerbose }
    : { ...DEFAULT_CONFIG, verbose: isVerbose };

  console.log('='.repeat(70));
  console.log('  DoSQL Real Engine Performance Benchmark');
  console.log('='.repeat(70));
  console.log(`  Mode: ${isQuick ? 'Quick' : 'Full'}`);
  console.log(`  Iterations: ${config.iterations}`);
  console.log(`  Warmup: ${config.warmupIterations}`);
  console.log(`  Row count: ${config.rowCount}`);
  console.log('='.repeat(70));

  // Create adapters
  const adapters: BenchmarkAdapter[] = [
    new DoSQLRealAdapter({ verbose: isVerbose }),
    new BetterSQLite3Adapter(),
    new LibSQLAdapter(),
  ];

  console.log('\nAdapters:');
  for (const adapter of adapters) {
    console.log(`  [+] ${adapter.name} v${adapter.version}`);
  }

  // Run benchmarks
  const allResults: BenchmarkResult[][] = [];

  const benchmarks = [
    { name: 'SELECT by PK', fn: benchmarkSelectByPK },
    { name: 'INSERT single', fn: benchmarkInsertSingle },
    { name: 'UPDATE by PK', fn: benchmarkUpdateWhere },
    { name: 'SELECT with JOIN', fn: benchmarkSelectJoin },
    { name: 'Transaction (3 ops)', fn: benchmarkTransaction },
    { name: 'SELECT with aggregates', fn: benchmarkAggregate },
  ];

  for (const benchmark of benchmarks) {
    console.log(`\nRunning: ${benchmark.name}`);
    const results: BenchmarkResult[] = [];

    for (const adapter of adapters) {
      process.stdout.write(`  ${adapter.name}... `);
      try {
        await adapter.initialize();
        const result = await benchmark.fn(adapter, config);
        results.push(result);
        console.log(`P95: ${formatLatency(result.latency.p95)}, ${result.opsPerSecond.toFixed(0)} ops/sec`);
      } catch (error) {
        console.log(`ERROR: ${error}`);
      } finally {
        await adapter.cleanup();
      }
    }

    allResults.push(results);
  }

  // Print comparison table
  printResultsTable(allResults);
}

main().catch((error) => {
  console.error('Benchmark failed:', error);
  process.exit(1);
});
