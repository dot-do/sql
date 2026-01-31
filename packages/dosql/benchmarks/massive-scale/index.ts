/**
 * Massive-Scale Benchmark Suite
 *
 * Benchmarks DoSQL with datasets that exceed D1/DO-SQLite limits:
 *
 * | Dataset           | Size    | Rows/Entities    | Shards |
 * |-------------------|---------|------------------|--------|
 * | IMDB              | 5.5GB   | 100M rows        | 64     |
 * | Wiktionary        | 20GB    | ~1M entries      | 32     |
 * | Common Crawl      | 100GB+  | 309M nodes, 2.9B | 128    |
 *
 * ## Quick Start
 *
 * ```bash
 * # 1. Deploy the benchmark worker
 * cd packages/dosql/benchmarks/massive-scale
 * wrangler deploy
 *
 * # 2. Download and load data (with 100k row limit for testing)
 * npx tsx load-data.ts --download --dataset=imdb --limit=100000
 *
 * # 3. Run benchmarks
 * npx tsx runner.ts --dataset=imdb --queries=1000
 *
 * # 4. Run full benchmark (all datasets)
 * npx tsx runner.ts --all
 * ```
 *
 * ## Architecture
 *
 * ```
 * ┌─────────────────────────────────────────────────────────────┐
 * │                    Benchmark Worker                          │
 * │  ┌─────────────────────────────────────────────────────────┐ │
 * │  │                  Query Router                           │ │
 * │  │     fnv1a(key) % shard_count → target shard            │ │
 * │  └─────────────────────────────────────────────────────────┘ │
 * │                           │                                  │
 * │     ┌─────────────────────┼─────────────────────┐           │
 * │     │                     │                     │           │
 * │     ▼                     ▼                     ▼           │
 * │  ┌─────┐              ┌─────┐              ┌─────┐          │
 * │  │IMDB │              │WIKT │              │CRAWL│          │
 * │  │Shard│ x64          │Shard│ x32          │Shard│ x128     │
 * │  │ DO  │              │ DO  │              │ DO  │          │
 * │  └─────┘              └─────┘              └─────┘          │
 * │     │                     │                     │           │
 * │     └─────────────────────┴─────────────────────┘           │
 * │                           │                                  │
 * │                    ┌──────┴──────┐                          │
 * │                    │     R2      │                          │
 * │                    │  (overflow) │                          │
 * │                    └─────────────┘                          │
 * └─────────────────────────────────────────────────────────────┘
 * ```
 *
 * ## Benchmark Types
 *
 * ### IMDB (Relational)
 * - Point queries: Primary key lookups
 * - Range queries: Movies by year
 * - Joins: Titles with ratings
 * - Aggregations: Average rating by genre
 *
 * ### Wiktionary (Document)
 * - Dictionary lookups: Word by language
 * - Definition retrieval: Senses and glosses
 * - Full-text patterns: Etymology search
 * - Language statistics: Word counts
 *
 * ### Common Crawl (Graph)
 * - Outlink queries: Edges from a node
 * - Backlink queries: Edges to a node (scatter)
 * - 2-hop traversal: Friends of friends
 * - Degree distribution: Analytics
 *
 * ## Expected Results
 *
 * Based on DoSQL architecture and Cloudflare limits:
 *
 * | Metric            | IMDB      | Wiktionary | Crawl Graph |
 * |-------------------|-----------|------------|-------------|
 * | Point Query p50   | <5ms      | <5ms       | <5ms        |
 * | Range Query p50   | <20ms     | <20ms      | N/A         |
 * | Scatter Query p50 | <100ms    | <50ms      | <200ms      |
 * | Join Query p50    | <10ms     | <10ms      | <50ms       |
 * | Aggregation p50   | <200ms    | <100ms     | <500ms      |
 *
 * ## Comparison with Alternatives
 *
 * | System      | Max Size | Sharding    | Cold Storage |
 * |-------------|----------|-------------|--------------|
 * | D1          | 10GB     | No          | No           |
 * | DO SQLite   | ~1GB     | Manual      | No           |
 * | **DoSQL**   | **∞**    | **Built-in**| **R2/Parquet**|
 */

export { IMDB_SCHEMAS, WIKTIONARY_SCHEMAS, CRAWL_GRAPH_SCHEMAS } from './schema.js';
export { BENCHMARK_QUERIES, BenchmarkConfig, DEFAULT_BENCHMARK_CONFIG } from './schema.js';
export { loadIMDBTable, loadWiktionary, loadCrawlGraphVertices, loadCrawlGraphEdges } from './loader.js';
export { runBenchmarks } from './runner.js';
