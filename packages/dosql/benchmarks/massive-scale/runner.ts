#!/usr/bin/env npx tsx
/**
 * Massive-Scale Benchmark Runner
 *
 * Run benchmarks against loaded datasets:
 * - IMDB: Point queries, joins, aggregations
 * - Wiktionary: Dictionary lookups, full-text patterns
 * - Common Crawl: Graph traversals, degree distributions
 *
 * Also compares Cache API vs Sharded DO performance:
 * - Cache API: Edge-local (~1ms), eventually consistent
 * - Sharded DOs: Single location, strongly consistent, full SQL
 *
 * Usage:
 *   npx tsx runner.ts --dataset=imdb --queries=1000
 *   npx tsx runner.ts --all --concurrency=10
 *   npx tsx runner.ts --cache-comparison --dataset=imdb
 */

import {
  BENCHMARK_QUERIES,
  BenchmarkConfig,
  DEFAULT_BENCHMARK_CONFIG,
  IMDB_SHARD_CONFIG,
  WIKTIONARY_SHARD_CONFIG,
  CRAWL_GRAPH_SHARD_CONFIG,
} from './schema.js';

// =============================================================================
// Types
// =============================================================================

interface BenchmarkResult {
  name: string;
  dataset: string;
  queryType: string;
  totalQueries: number;
  successfulQueries: number;
  failedQueries: number;
  totalTimeMs: number;
  latencies: {
    p50: number;
    p95: number;
    p99: number;
    min: number;
    max: number;
    avg: number;
  };
  throughput: number; // queries per second
  rowsReturned: number;
}

interface ShardedDatabase {
  query(shardId: string, sql: string, params?: unknown[]): Promise<{ rows: unknown[]; latencyMs: number }>;
  queryAll(sql: string, params?: unknown[]): Promise<{ rows: unknown[]; latencyMs: number }>;
}

// =============================================================================
// Stats Calculation
// =============================================================================

function calculatePercentile(sortedArray: number[], percentile: number): number {
  if (sortedArray.length === 0) return 0;
  const index = Math.ceil((percentile / 100) * sortedArray.length) - 1;
  return sortedArray[Math.max(0, index)];
}

function calculateLatencyStats(latencies: number[]): BenchmarkResult['latencies'] {
  if (latencies.length === 0) {
    return { p50: 0, p95: 0, p99: 0, min: 0, max: 0, avg: 0 };
  }

  const sorted = [...latencies].sort((a, b) => a - b);
  const sum = latencies.reduce((a, b) => a + b, 0);

  return {
    p50: calculatePercentile(sorted, 50),
    p95: calculatePercentile(sorted, 95),
    p99: calculatePercentile(sorted, 99),
    min: sorted[0],
    max: sorted[sorted.length - 1],
    avg: sum / latencies.length,
  };
}

// =============================================================================
// Hash Router (for shard selection)
// =============================================================================

function fnv1a(input: string): number {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function getShardForKey(key: string, shardCount: number): string {
  return `shard_${fnv1a(key) % shardCount}`;
}

// =============================================================================
// IMDB Benchmarks
// =============================================================================

async function benchmarkIMDB(
  db: ShardedDatabase,
  config: BenchmarkConfig
): Promise<BenchmarkResult[]> {
  const results: BenchmarkResult[] = [];

  // Sample data for queries
  const sampleTconsts = [
    'tt0000001', 'tt0000002', 'tt0000003', 'tt0000009', 'tt0000012',
    'tt0111161', 'tt0468569', 'tt0071562', 'tt0050083', 'tt0108052',
  ];
  const sampleYears = [2020, 2021, 2022, 2023, 2024];

  // Benchmark: Point Query (getTitle)
  {
    const latencies: number[] = [];
    let rowsReturned = 0;
    let successful = 0;
    let failed = 0;
    const startTime = Date.now();

    for (let i = 0; i < config.benchmarkQueries; i++) {
      const tconst = sampleTconsts[i % sampleTconsts.length];
      const shardId = getShardForKey(tconst, IMDB_SHARD_CONFIG.shardCount);

      try {
        const result = await db.query(shardId, BENCHMARK_QUERIES.imdb.getTitle, [tconst]);
        latencies.push(result.latencyMs);
        rowsReturned += result.rows.length;
        successful++;
      } catch {
        failed++;
      }
    }

    results.push({
      name: 'imdb_point_query',
      dataset: 'imdb',
      queryType: 'point',
      totalQueries: config.benchmarkQueries,
      successfulQueries: successful,
      failedQueries: failed,
      totalTimeMs: Date.now() - startTime,
      latencies: calculateLatencyStats(latencies),
      throughput: (successful / ((Date.now() - startTime) / 1000)),
      rowsReturned,
    });
  }

  // Benchmark: Range Query (moviesInYear)
  {
    const latencies: number[] = [];
    let rowsReturned = 0;
    let successful = 0;
    let failed = 0;
    const startTime = Date.now();

    for (let i = 0; i < config.benchmarkQueries; i++) {
      const year = sampleYears[i % sampleYears.length];

      try {
        // Scatter query across all shards
        const result = await db.queryAll(BENCHMARK_QUERIES.imdb.moviesInYear, [year, 'movie']);
        latencies.push(result.latencyMs);
        rowsReturned += result.rows.length;
        successful++;
      } catch {
        failed++;
      }
    }

    results.push({
      name: 'imdb_range_query',
      dataset: 'imdb',
      queryType: 'range',
      totalQueries: config.benchmarkQueries,
      successfulQueries: successful,
      failedQueries: failed,
      totalTimeMs: Date.now() - startTime,
      latencies: calculateLatencyStats(latencies),
      throughput: (successful / ((Date.now() - startTime) / 1000)),
      rowsReturned,
    });
  }

  // Benchmark: Join Query (movieWithRating)
  {
    const latencies: number[] = [];
    let rowsReturned = 0;
    let successful = 0;
    let failed = 0;
    const startTime = Date.now();

    for (let i = 0; i < config.benchmarkQueries; i++) {
      const tconst = sampleTconsts[i % sampleTconsts.length];
      const shardId = getShardForKey(tconst, IMDB_SHARD_CONFIG.shardCount);

      try {
        const result = await db.query(shardId, BENCHMARK_QUERIES.imdb.movieWithRating, [tconst]);
        latencies.push(result.latencyMs);
        rowsReturned += result.rows.length;
        successful++;
      } catch {
        failed++;
      }
    }

    results.push({
      name: 'imdb_join_query',
      dataset: 'imdb',
      queryType: 'join',
      totalQueries: config.benchmarkQueries,
      successfulQueries: successful,
      failedQueries: failed,
      totalTimeMs: Date.now() - startTime,
      latencies: calculateLatencyStats(latencies),
      throughput: (successful / ((Date.now() - startTime) / 1000)),
      rowsReturned,
    });
  }

  // Benchmark: Aggregation (avgRatingByGenre)
  {
    const latencies: number[] = [];
    let rowsReturned = 0;
    let successful = 0;
    let failed = 0;
    const startTime = Date.now();

    for (let i = 0; i < Math.min(config.benchmarkQueries, 100); i++) {
      const startYear = 2010 + (i % 10);
      const endYear = startYear + 5;

      try {
        const result = await db.queryAll(BENCHMARK_QUERIES.imdb.avgRatingByGenre, [startYear, endYear]);
        latencies.push(result.latencyMs);
        rowsReturned += result.rows.length;
        successful++;
      } catch {
        failed++;
      }
    }

    results.push({
      name: 'imdb_aggregation',
      dataset: 'imdb',
      queryType: 'aggregation',
      totalQueries: Math.min(config.benchmarkQueries, 100),
      successfulQueries: successful,
      failedQueries: failed,
      totalTimeMs: Date.now() - startTime,
      latencies: calculateLatencyStats(latencies),
      throughput: (successful / ((Date.now() - startTime) / 1000)),
      rowsReturned,
    });
  }

  return results;
}

// =============================================================================
// Wiktionary Benchmarks
// =============================================================================

async function benchmarkWiktionary(
  db: ShardedDatabase,
  config: BenchmarkConfig
): Promise<BenchmarkResult[]> {
  const results: BenchmarkResult[] = [];

  // Sample words
  const sampleWords = [
    'hello', 'world', 'computer', 'database', 'algorithm',
    'language', 'dictionary', 'etymology', 'syntax', 'grammar',
  ];
  const sampleLangCodes = ['en', 'de', 'fr', 'es', 'ja'];

  // Benchmark: Word Lookup
  {
    const latencies: number[] = [];
    let rowsReturned = 0;
    let successful = 0;
    let failed = 0;
    const startTime = Date.now();

    for (let i = 0; i < config.benchmarkQueries; i++) {
      const word = sampleWords[i % sampleWords.length];
      const langCode = sampleLangCodes[i % sampleLangCodes.length];
      const shardId = getShardForKey(langCode, WIKTIONARY_SHARD_CONFIG.shardCount);

      try {
        const result = await db.query(shardId, BENCHMARK_QUERIES.wiktionary.lookupWord, [word, langCode]);
        latencies.push(result.latencyMs);
        rowsReturned += result.rows.length;
        successful++;
      } catch {
        failed++;
      }
    }

    results.push({
      name: 'wiktionary_lookup',
      dataset: 'wiktionary',
      queryType: 'point',
      totalQueries: config.benchmarkQueries,
      successfulQueries: successful,
      failedQueries: failed,
      totalTimeMs: Date.now() - startTime,
      latencies: calculateLatencyStats(latencies),
      throughput: (successful / ((Date.now() - startTime) / 1000)),
      rowsReturned,
    });
  }

  // Benchmark: Get Definitions (join)
  {
    const latencies: number[] = [];
    let rowsReturned = 0;
    let successful = 0;
    let failed = 0;
    const startTime = Date.now();

    for (let i = 0; i < config.benchmarkQueries; i++) {
      const word = sampleWords[i % sampleWords.length];
      const langCode = sampleLangCodes[i % sampleLangCodes.length];
      const shardId = getShardForKey(langCode, WIKTIONARY_SHARD_CONFIG.shardCount);

      try {
        const result = await db.query(shardId, BENCHMARK_QUERIES.wiktionary.getDefinitions, [word, langCode]);
        latencies.push(result.latencyMs);
        rowsReturned += result.rows.length;
        successful++;
      } catch {
        failed++;
      }
    }

    results.push({
      name: 'wiktionary_definitions',
      dataset: 'wiktionary',
      queryType: 'join',
      totalQueries: config.benchmarkQueries,
      successfulQueries: successful,
      failedQueries: failed,
      totalTimeMs: Date.now() - startTime,
      latencies: calculateLatencyStats(latencies),
      throughput: (successful / ((Date.now() - startTime) / 1000)),
      rowsReturned,
    });
  }

  // Benchmark: Count by Language (aggregation)
  {
    const latencies: number[] = [];
    let rowsReturned = 0;
    let successful = 0;
    let failed = 0;
    const startTime = Date.now();

    for (let i = 0; i < Math.min(config.benchmarkQueries, 50); i++) {
      try {
        const result = await db.queryAll(BENCHMARK_QUERIES.wiktionary.countByLanguage, []);
        latencies.push(result.latencyMs);
        rowsReturned += result.rows.length;
        successful++;
      } catch {
        failed++;
      }
    }

    results.push({
      name: 'wiktionary_aggregation',
      dataset: 'wiktionary',
      queryType: 'aggregation',
      totalQueries: Math.min(config.benchmarkQueries, 50),
      successfulQueries: successful,
      failedQueries: failed,
      totalTimeMs: Date.now() - startTime,
      latencies: calculateLatencyStats(latencies),
      throughput: (successful / ((Date.now() - startTime) / 1000)),
      rowsReturned,
    });
  }

  return results;
}

// =============================================================================
// Common Crawl Graph Benchmarks
// =============================================================================

async function benchmarkCrawlGraph(
  db: ShardedDatabase,
  config: BenchmarkConfig
): Promise<BenchmarkResult[]> {
  const results: BenchmarkResult[] = [];

  // Sample node IDs (distributed across range)
  const sampleNodeIds = Array.from({ length: 100 }, (_, i) =>
    Math.floor((i / 100) * 309_200_000)
  );

  // Benchmark: Get Outlinks
  {
    const latencies: number[] = [];
    let rowsReturned = 0;
    let successful = 0;
    let failed = 0;
    const startTime = Date.now();

    for (let i = 0; i < config.benchmarkQueries; i++) {
      const nodeId = sampleNodeIds[i % sampleNodeIds.length];
      const shardIndex = Math.floor(nodeId / (309_200_000 / CRAWL_GRAPH_SHARD_CONFIG.shardCount));
      const shardId = `shard_${shardIndex}`;

      try {
        const result = await db.query(shardId, BENCHMARK_QUERIES.crawl_graph.getOutlinks, [nodeId]);
        latencies.push(result.latencyMs);
        rowsReturned += result.rows.length;
        successful++;
      } catch {
        failed++;
      }
    }

    results.push({
      name: 'crawl_graph_outlinks',
      dataset: 'crawl_graph',
      queryType: 'point',
      totalQueries: config.benchmarkQueries,
      successfulQueries: successful,
      failedQueries: failed,
      totalTimeMs: Date.now() - startTime,
      latencies: calculateLatencyStats(latencies),
      throughput: (successful / ((Date.now() - startTime) / 1000)),
      rowsReturned,
    });
  }

  // Benchmark: Get Backlinks (scatter across shards)
  {
    const latencies: number[] = [];
    let rowsReturned = 0;
    let successful = 0;
    let failed = 0;
    const startTime = Date.now();

    for (let i = 0; i < Math.min(config.benchmarkQueries, 200); i++) {
      const nodeId = sampleNodeIds[i % sampleNodeIds.length];

      try {
        const result = await db.queryAll(BENCHMARK_QUERIES.crawl_graph.getBacklinks, [nodeId]);
        latencies.push(result.latencyMs);
        rowsReturned += result.rows.length;
        successful++;
      } catch {
        failed++;
      }
    }

    results.push({
      name: 'crawl_graph_backlinks',
      dataset: 'crawl_graph',
      queryType: 'scatter',
      totalQueries: Math.min(config.benchmarkQueries, 200),
      successfulQueries: successful,
      failedQueries: failed,
      totalTimeMs: Date.now() - startTime,
      latencies: calculateLatencyStats(latencies),
      throughput: (successful / ((Date.now() - startTime) / 1000)),
      rowsReturned,
    });
  }

  // Benchmark: 2-Hop Traversal
  {
    const latencies: number[] = [];
    let rowsReturned = 0;
    let successful = 0;
    let failed = 0;
    const startTime = Date.now();

    for (let i = 0; i < Math.min(config.benchmarkQueries, 100); i++) {
      const nodeId = sampleNodeIds[i % sampleNodeIds.length];
      const shardIndex = Math.floor(nodeId / (309_200_000 / CRAWL_GRAPH_SHARD_CONFIG.shardCount));
      const shardId = `shard_${shardIndex}`;

      try {
        const result = await db.query(shardId, BENCHMARK_QUERIES.crawl_graph.twoHop, [nodeId]);
        latencies.push(result.latencyMs);
        rowsReturned += result.rows.length;
        successful++;
      } catch {
        failed++;
      }
    }

    results.push({
      name: 'crawl_graph_2hop',
      dataset: 'crawl_graph',
      queryType: 'traversal',
      totalQueries: Math.min(config.benchmarkQueries, 100),
      successfulQueries: successful,
      failedQueries: failed,
      totalTimeMs: Date.now() - startTime,
      latencies: calculateLatencyStats(latencies),
      throughput: (successful / ((Date.now() - startTime) / 1000)),
      rowsReturned,
    });
  }

  return results;
}

// =============================================================================
// Cache API vs Sharded DO Comparison
// =============================================================================

interface CacheComparisonResult {
  dataset: string;
  queryType: string;
  iterations: number;
  cache: {
    p50: number;
    p95: number;
    p99: number;
    avg: number;
    hitRate: number;
  };
  do: {
    p50: number;
    p95: number;
    p99: number;
    avg: number;
  };
  speedup: {
    p50: number;
    avg: number;
  };
  recommendation: string;
}

async function runCacheComparison(
  endpoint: string,
  dataset: 'imdb' | 'wiktionary' | 'crawl_graph',
  config: BenchmarkConfig
): Promise<CacheComparisonResult[]> {
  const results: CacheComparisonResult[] = [];

  // Sample keys for each dataset
  const sampleKeys = {
    imdb: [
      'tt0111161', 'tt0068646', 'tt0071562', 'tt0468569', 'tt0050083',
      'tt0108052', 'tt0167260', 'tt0110912', 'tt0060196', 'tt0120737',
    ],
    wiktionary: [
      'hello', 'world', 'computer', 'database', 'algorithm',
      'language', 'dictionary', 'etymology', 'syntax', 'grammar',
    ],
    crawl_graph: [
      '0', '1000000', '10000000', '50000000', '100000000',
      '150000000', '200000000', '250000000', '300000000', '309000000',
    ],
  };

  const keys = sampleKeys[dataset];
  const iterations = Math.min(config.benchmarkQueries, 100);

  console.log(`\nRunning Cache API vs DO comparison for ${dataset}...`);
  console.log(`  Keys: ${keys.length}, Iterations: ${iterations}`);

  try {
    // Point query comparison
    const pointResponse = await fetch(`${endpoint}/cache/benchmark`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dataset, keys, iterations }),
    });

    const pointResult = await pointResponse.json() as {
      cache: { p50: number; p95: number; p99: number; avg: number; hitRate: number };
      do: { p50: number; p95: number; p99: number; avg: number };
      speedup: { p50: number; avg: number };
    };

    results.push({
      dataset,
      queryType: 'point',
      iterations,
      cache: pointResult.cache,
      do: pointResult.do,
      speedup: pointResult.speedup,
      recommendation: pointResult.cache.hitRate > 0.8 && pointResult.speedup.p50 > 5
        ? 'Use Cache API for read-heavy point queries with acceptable staleness'
        : 'Use Sharded DOs for consistency or low cache hit rates',
    });

    // Scatter query comparison (for aggregations)
    if (dataset === 'imdb') {
      const scatterResponse = await fetch(`${endpoint}/cache/benchmark/scatter`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          dataset,
          sql: 'SELECT COUNT(*) as cnt FROM title_basics WHERE startYear = 2023',
          cacheKey: 'movies_2023_count',
          iterations: Math.min(iterations, 50),
        }),
      });

      const scatterResult = await scatterResponse.json() as {
        cache: { p50: number; p95: number; p99: number; avg: number; hitRate: number };
        scatter: { p50: number; p95: number; p99: number; avg: number };
        speedup: { p50: number; avg: number };
        recommendation: string;
      };

      results.push({
        dataset,
        queryType: 'scatter',
        iterations: Math.min(iterations, 50),
        cache: scatterResult.cache,
        do: scatterResult.scatter,
        speedup: scatterResult.speedup,
        recommendation: scatterResult.recommendation,
      });
    }
  } catch (err) {
    console.error(`Cache comparison failed: ${err}`);
  }

  return results;
}

function generateCacheComparisonReport(results: CacheComparisonResult[]): void {
  console.log('\n' + '='.repeat(80));
  console.log('              CACHE API vs SHARDED DO COMPARISON');
  console.log('='.repeat(80));
  console.log('\n┌─────────────┬──────────┬─────────────────────────────┬─────────────────────────────┬──────────┐');
  console.log('│   Dataset   │   Type   │       Cache API (ms)        │       Sharded DO (ms)       │ Speedup  │');
  console.log('├─────────────┼──────────┼─────────────────────────────┼─────────────────────────────┼──────────┤');

  for (const r of results) {
    const cacheStats = `p50:${r.cache.p50.toFixed(1)} p95:${r.cache.p95.toFixed(1)}`;
    const doStats = `p50:${r.do.p50.toFixed(1)} p95:${r.do.p95.toFixed(1)}`;
    console.log(
      `│ ${r.dataset.padEnd(11)} │ ${r.queryType.padEnd(8)} │ ${cacheStats.padEnd(27)} │ ${doStats.padEnd(27)} │ ${r.speedup.p50.toFixed(1)}x`.padEnd(88) + '│'
    );
  }

  console.log('└─────────────┴──────────┴─────────────────────────────┴─────────────────────────────┴──────────┘');

  // Trade-offs summary
  console.log('\n┌───────────────────────────────────────────────────────────────────────────────┐');
  console.log('│                           TRADE-OFF ANALYSIS                                  │');
  console.log('├───────────────────────────────────────────────────────────────────────────────┤');
  console.log('│  Cache API                        │  Sharded DOs                              │');
  console.log('│  ✓ Edge-local (~1ms latency)      │  ✓ Single location consistency            │');
  console.log('│  ✓ Massive read scalability       │  ✓ Full SQL query support                 │');
  console.log('│  ✗ Eventually consistent          │  ✓ Strong consistency                     │');
  console.log('│  ✗ Key-value only (no queries)    │  ✓ Joins, aggregations, filters           │');
  console.log('│  ✗ TTL-based expiry               │  ✓ Durable persistent storage             │');
  console.log('├───────────────────────────────────────────────────────────────────────────────┤');
  console.log('│  RECOMMENDATION: Use hybrid approach                                          │');
  console.log('│  - Cache API for hot read paths (product pages, user profiles)               │');
  console.log('│  - Sharded DOs for writes & consistency-critical reads                       │');
  console.log('│  - Invalidate cache on DO writes for near-real-time consistency              │');
  console.log('└───────────────────────────────────────────────────────────────────────────────┘');

  // Recommendations per result
  console.log('\n[SPECIFIC RECOMMENDATIONS]');
  for (const r of results) {
    console.log(`  ${r.dataset}/${r.queryType}: ${r.recommendation}`);
  }
}

// =============================================================================
// Report Generator
// =============================================================================

function generateReport(results: BenchmarkResult[]): void {
  console.log('\n' + '='.repeat(80));
  console.log('                    MASSIVE-SCALE BENCHMARK RESULTS');
  console.log('='.repeat(80));

  // Group by dataset
  const byDataset = new Map<string, BenchmarkResult[]>();
  for (const r of results) {
    const list = byDataset.get(r.dataset) || [];
    list.push(r);
    byDataset.set(r.dataset, list);
  }

  for (const [dataset, datasetResults] of byDataset) {
    console.log(`\n[${dataset.toUpperCase()}]`);
    console.log('-'.repeat(80));
    console.log(
      'Benchmark'.padEnd(30) +
      'p50'.padStart(10) +
      'p95'.padStart(10) +
      'p99'.padStart(10) +
      'QPS'.padStart(12) +
      'Rows'.padStart(10)
    );
    console.log('-'.repeat(80));

    for (const r of datasetResults) {
      console.log(
        r.name.padEnd(30) +
        `${r.latencies.p50.toFixed(2)}ms`.padStart(10) +
        `${r.latencies.p95.toFixed(2)}ms`.padStart(10) +
        `${r.latencies.p99.toFixed(2)}ms`.padStart(10) +
        r.throughput.toFixed(0).padStart(12) +
        r.rowsReturned.toLocaleString().padStart(10)
      );
    }
  }

  // Summary
  console.log('\n' + '='.repeat(80));
  console.log('SUMMARY');
  console.log('='.repeat(80));

  const totalQueries = results.reduce((a, r) => a + r.totalQueries, 0);
  const totalSuccessful = results.reduce((a, r) => a + r.successfulQueries, 0);
  const totalFailed = results.reduce((a, r) => a + r.failedQueries, 0);
  const totalRows = results.reduce((a, r) => a + r.rowsReturned, 0);
  const avgP50 = results.reduce((a, r) => a + r.latencies.p50, 0) / results.length;
  const avgP95 = results.reduce((a, r) => a + r.latencies.p95, 0) / results.length;

  console.log(`Total Queries:    ${totalQueries.toLocaleString()}`);
  console.log(`Successful:       ${totalSuccessful.toLocaleString()} (${((totalSuccessful / totalQueries) * 100).toFixed(1)}%)`);
  console.log(`Failed:           ${totalFailed.toLocaleString()}`);
  console.log(`Total Rows:       ${totalRows.toLocaleString()}`);
  console.log(`Average p50:      ${avgP50.toFixed(2)}ms`);
  console.log(`Average p95:      ${avgP95.toFixed(2)}ms`);
}

// =============================================================================
// Main Entry Point
// =============================================================================

export async function runBenchmarks(
  db: ShardedDatabase,
  config: Partial<BenchmarkConfig> = {}
): Promise<BenchmarkResult[]> {
  const fullConfig: BenchmarkConfig = { ...DEFAULT_BENCHMARK_CONFIG, ...config };
  const allResults: BenchmarkResult[] = [];

  console.log('Starting massive-scale benchmarks...');
  console.log(`Config: ${JSON.stringify(fullConfig)}`);

  // Warmup
  console.log(`\nRunning ${fullConfig.warmupQueries} warmup queries...`);

  // Run benchmarks based on dataset
  if (fullConfig.dataset === 'imdb' || !config.dataset) {
    console.log('\n[IMDB Benchmarks]');
    allResults.push(...await benchmarkIMDB(db, fullConfig));
  }

  if (fullConfig.dataset === 'wiktionary' || !config.dataset) {
    console.log('\n[Wiktionary Benchmarks]');
    allResults.push(...await benchmarkWiktionary(db, fullConfig));
  }

  if (fullConfig.dataset === 'crawl_graph' || !config.dataset) {
    console.log('\n[Common Crawl Graph Benchmarks]');
    allResults.push(...await benchmarkCrawlGraph(db, fullConfig));
  }

  generateReport(allResults);

  return allResults;
}

// Export cache comparison for use as module
export { runCacheComparison, generateCacheComparisonReport };
export type { CacheComparisonResult };

// CLI entry point
if (import.meta.url === `file://${process.argv[1]}`) {
  const args = process.argv.slice(2);
  const config: Partial<BenchmarkConfig> = {};
  let endpoint = 'http://localhost:8787';
  let cacheComparison = false;

  for (const arg of args) {
    if (arg.startsWith('--dataset=')) {
      config.dataset = arg.split('=')[1] as BenchmarkConfig['dataset'];
    } else if (arg.startsWith('--queries=')) {
      config.benchmarkQueries = parseInt(arg.split('=')[1], 10);
    } else if (arg.startsWith('--concurrency=')) {
      config.concurrency = parseInt(arg.split('=')[1], 10);
    } else if (arg.startsWith('--endpoint=')) {
      endpoint = arg.split('=')[1];
    } else if (arg === '--cache-comparison') {
      cacheComparison = true;
    }
  }

  if (cacheComparison) {
    // Run Cache API vs DO comparison
    const dataset = (config.dataset || 'imdb') as 'imdb' | 'wiktionary' | 'crawl_graph';
    const fullConfig: BenchmarkConfig = { ...DEFAULT_BENCHMARK_CONFIG, ...config };

    console.log('Cache API vs Sharded DO Benchmark');
    console.log('='.repeat(60));
    console.log(`Endpoint: ${endpoint}`);
    console.log(`Dataset:  ${dataset}`);
    console.log('='.repeat(60));

    runCacheComparison(endpoint, dataset, fullConfig)
      .then(results => {
        generateCacheComparisonReport(results);
      })
      .catch(console.error);
  } else {
    // Run standard benchmarks with HTTP client
    const httpDb: ShardedDatabase = {
      async query(shardId, sql, params = []) {
        const shardIndex = parseInt(shardId.replace('shard_', ''), 10);
        const start = performance.now();

        const response = await fetch(`${endpoint}/query/${config.dataset || 'imdb'}/${shardIndex}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ sql, params }),
        });

        const result = await response.json() as { rows: unknown[]; latencyMs?: number };
        return {
          rows: result.rows || [],
          latencyMs: result.latencyMs || (performance.now() - start),
        };
      },

      async queryAll(sql, params = []) {
        const start = performance.now();

        const response = await fetch(`${endpoint}/scatter/${config.dataset || 'imdb'}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ sql, params }),
        });

        const result = await response.json() as { totalRows: number; shardResults: Array<{ rows?: unknown[] }> };
        const allRows = (result.shardResults || []).flatMap(r => r.rows || []);

        return {
          rows: allRows,
          latencyMs: performance.now() - start,
        };
      },
    };

    runBenchmarks(httpDb, config).catch(console.error);
  }
}
