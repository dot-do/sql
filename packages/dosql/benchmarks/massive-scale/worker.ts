/**
 * Massive-Scale Benchmark Worker
 *
 * Cloudflare Worker that deploys sharded DoSQL instances for:
 * - IMDB (64 shards)
 * - Wiktionary (32 shards)
 * - Common Crawl Graph (128 shards)
 *
 * Also benchmarks Cache API vs Sharded DOs:
 * - Cache API: Edge-local (~1ms), eventually consistent, key-value only
 * - Sharded DOs: Single location, strongly consistent, full SQL
 *
 * Endpoints:
 *   POST /load/:dataset       - Load data from uploaded file
 *   POST /query/:dataset      - Execute benchmark queries
 *   GET  /stats/:dataset      - Get shard statistics
 *   GET  /health              - Health check
 *
 * Cache API Endpoints:
 *   POST /cache/put/:dataset  - Store records in Cache API
 *   GET  /cache/get/:dataset/:key - Get record from Cache API
 *   POST /cache/populate/:dataset - Bulk populate cache from DOs
 *   POST /cache/benchmark     - Compare Cache API vs DO performance
 */

import { DurableObject } from 'cloudflare:workers';

// =============================================================================
// Environment Types
// =============================================================================

interface Env {
  IMDB_SHARD: DurableObjectNamespace<IMDBShardDO>;
  WIKTIONARY_SHARD: DurableObjectNamespace<WiktionaryShardDO>;
  CRAWL_GRAPH_SHARD: DurableObjectNamespace<CrawlGraphShardDO>;
  BENCHMARK_R2: R2Bucket;
}

// =============================================================================
// Shard Configuration
// =============================================================================

const SHARD_CONFIGS = {
  imdb: {
    count: 64,
    namespace: 'IMDB_SHARD',
  },
  wiktionary: {
    count: 32,
    namespace: 'WIKTIONARY_SHARD',
  },
  crawl_graph: {
    count: 128,
    namespace: 'CRAWL_GRAPH_SHARD',
  },
};

// =============================================================================
// Base Shard Durable Object
// =============================================================================

abstract class BaseShardDO extends DurableObject {
  protected db: any; // DoSQL Database instance

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.initDatabase();
  }

  protected abstract initDatabase(): void;
  protected abstract getSchemas(): string[];

  async setup(): Promise<{ success: boolean; tables: string[] }> {
    const schemas = this.getSchemas();
    const tables: string[] = [];

    for (const schema of schemas) {
      try {
        this.db.exec(schema);
        // Extract table name from CREATE TABLE statement
        const match = schema.match(/CREATE TABLE (?:IF NOT EXISTS )?(\w+)/i);
        if (match) {
          tables.push(match[1]);
        }
      } catch (err) {
        console.error('Schema error:', err);
      }
    }

    return { success: true, tables };
  }

  async query(sql: string, params: unknown[] = []): Promise<{ rows: unknown[]; latencyMs: number }> {
    const start = performance.now();
    try {
      const stmt = this.db.prepare(sql);
      const rows = stmt.all(...params);
      return { rows, latencyMs: performance.now() - start };
    } catch (err) {
      console.error('Query error:', sql, err);
      throw err;
    }
  }

  async exec(sql: string): Promise<void> {
    this.db.exec(sql);
  }

  async batch(statements: Array<{ sql: string; params: unknown[] }>): Promise<void> {
    this.db.exec('BEGIN TRANSACTION');
    try {
      for (const stmt of statements) {
        this.db.prepare(stmt.sql).run(...stmt.params);
      }
      this.db.exec('COMMIT');
    } catch (err) {
      this.db.exec('ROLLBACK');
      throw err;
    }
  }

  async getStats(): Promise<{ rowCounts: Record<string, number>; storageBytes: number }> {
    const schemas = this.getSchemas();
    const rowCounts: Record<string, number> = {};

    for (const schema of schemas) {
      const match = schema.match(/CREATE TABLE (?:IF NOT EXISTS )?(\w+)/i);
      if (match) {
        const tableName = match[1];
        try {
          const result = this.db.prepare(`SELECT COUNT(*) as count FROM ${tableName}`).get() as { count: number };
          rowCounts[tableName] = result.count;
        } catch {
          rowCounts[tableName] = 0;
        }
      }
    }

    // Approximate storage from DO storage
    const storageBytes = 0; // Would need actual storage API

    return { rowCounts, storageBytes };
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      if (path === '/setup') {
        const result = await this.setup();
        return Response.json(result);
      }

      if (path === '/query' && request.method === 'POST') {
        const { sql, params = [] } = await request.json() as { sql: string; params?: unknown[] };
        const result = await this.query(sql, params);
        return Response.json(result);
      }

      if (path === '/exec' && request.method === 'POST') {
        const { sql } = await request.json() as { sql: string };
        await this.exec(sql);
        return Response.json({ success: true });
      }

      if (path === '/batch' && request.method === 'POST') {
        const { statements } = await request.json() as { statements: Array<{ sql: string; params: unknown[] }> };
        await this.batch(statements);
        return Response.json({ success: true, count: statements.length });
      }

      if (path === '/stats') {
        const stats = await this.getStats();
        return Response.json(stats);
      }

      return new Response('Not Found', { status: 404 });
    } catch (err) {
      return Response.json({ error: String(err) }, { status: 500 });
    }
  }
}

// =============================================================================
// IMDB Shard DO
// =============================================================================

export class IMDBShardDO extends BaseShardDO {
  protected initDatabase(): void {
    // Use native DO SQLite
    this.db = this.ctx.storage.sql;
  }

  protected getSchemas(): string[] {
    return [
      `CREATE TABLE IF NOT EXISTS title_basics (
        tconst TEXT PRIMARY KEY,
        titleType TEXT,
        primaryTitle TEXT,
        originalTitle TEXT,
        isAdult INTEGER,
        startYear INTEGER,
        endYear INTEGER,
        runtimeMinutes INTEGER,
        genres TEXT
      )`,
      `CREATE TABLE IF NOT EXISTS name_basics (
        nconst TEXT PRIMARY KEY,
        primaryName TEXT,
        birthYear INTEGER,
        deathYear INTEGER,
        primaryProfession TEXT,
        knownForTitles TEXT
      )`,
      `CREATE TABLE IF NOT EXISTS title_ratings (
        tconst TEXT PRIMARY KEY,
        averageRating REAL,
        numVotes INTEGER
      )`,
      `CREATE TABLE IF NOT EXISTS title_principals (
        tconst TEXT,
        ordering INTEGER,
        nconst TEXT,
        category TEXT,
        job TEXT,
        characters TEXT,
        PRIMARY KEY (tconst, ordering)
      )`,
      `CREATE TABLE IF NOT EXISTS title_crew (
        tconst TEXT PRIMARY KEY,
        directors TEXT,
        writers TEXT
      )`,
    ];
  }
}

// =============================================================================
// Wiktionary Shard DO
// =============================================================================

export class WiktionaryShardDO extends BaseShardDO {
  protected initDatabase(): void {
    this.db = this.ctx.storage.sql;
  }

  protected getSchemas(): string[] {
    return [
      `CREATE TABLE IF NOT EXISTS words (
        id INTEGER PRIMARY KEY,
        word TEXT NOT NULL,
        lang_code TEXT NOT NULL,
        pos TEXT,
        etymology TEXT
      )`,
      `CREATE TABLE IF NOT EXISTS senses (
        id INTEGER PRIMARY KEY,
        word_id INTEGER NOT NULL,
        gloss TEXT NOT NULL,
        tags TEXT,
        categories TEXT
      )`,
      `CREATE TABLE IF NOT EXISTS forms (
        id INTEGER PRIMARY KEY,
        word_id INTEGER NOT NULL,
        form TEXT NOT NULL,
        tags TEXT
      )`,
      `CREATE INDEX IF NOT EXISTS idx_words_word_lang ON words(word, lang_code)`,
      `CREATE INDEX IF NOT EXISTS idx_senses_word_id ON senses(word_id)`,
      `CREATE INDEX IF NOT EXISTS idx_forms_word_id ON forms(word_id)`,
    ];
  }
}

// =============================================================================
// Common Crawl Graph Shard DO
// =============================================================================

export class CrawlGraphShardDO extends BaseShardDO {
  protected initDatabase(): void {
    this.db = this.ctx.storage.sql;
  }

  protected getSchemas(): string[] {
    return [
      `CREATE TABLE IF NOT EXISTS vertices (
        node_id INTEGER PRIMARY KEY,
        domain TEXT NOT NULL,
        reverse_domain TEXT NOT NULL
      )`,
      `CREATE TABLE IF NOT EXISTS edges (
        source_id INTEGER NOT NULL,
        dest_id INTEGER NOT NULL,
        PRIMARY KEY (source_id, dest_id)
      )`,
      `CREATE INDEX IF NOT EXISTS idx_vertices_domain ON vertices(domain)`,
      `CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_id)`,
      `CREATE INDEX IF NOT EXISTS idx_edges_dest ON edges(dest_id)`,
    ];
  }
}

// =============================================================================
// Router Functions
// =============================================================================

function fnv1a(input: string): number {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function getShardId(key: string | number, shardCount: number): number {
  return fnv1a(String(key)) % shardCount;
}

function getShardStub(
  env: Env,
  dataset: keyof typeof SHARD_CONFIGS,
  shardIndex: number
): DurableObjectStub {
  const config = SHARD_CONFIGS[dataset];
  const namespace = env[config.namespace as keyof Env] as DurableObjectNamespace;
  const id = namespace.idFromName(`${dataset}_shard_${shardIndex}`);
  return namespace.get(id);
}

// =============================================================================
// Main Worker
// =============================================================================

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      // Health check
      if (path === '/health') {
        return Response.json({ status: 'ok', timestamp: Date.now() });
      }

      // Setup all shards for a dataset
      if (path.startsWith('/setup/')) {
        const dataset = path.split('/')[2] as keyof typeof SHARD_CONFIGS;
        if (!SHARD_CONFIGS[dataset]) {
          return Response.json({ error: 'Unknown dataset' }, { status: 400 });
        }

        const config = SHARD_CONFIGS[dataset];
        const results: Array<{ shard: number; result: unknown }> = [];

        // Setup shards in parallel (batches of 10)
        for (let i = 0; i < config.count; i += 10) {
          const batch = [];
          for (let j = i; j < Math.min(i + 10, config.count); j++) {
            const stub = getShardStub(env, dataset, j);
            batch.push(
              stub.fetch(new Request('http://shard/setup'))
                .then(r => r.json())
                .then(result => ({ shard: j, result }))
            );
          }
          results.push(...await Promise.all(batch));
        }

        return Response.json({
          dataset,
          shardCount: config.count,
          results,
        });
      }

      // Query a specific shard
      if (path.startsWith('/query/') && request.method === 'POST') {
        const parts = path.split('/');
        const dataset = parts[2] as keyof typeof SHARD_CONFIGS;
        const shardKey = parts[3] || '0';

        if (!SHARD_CONFIGS[dataset]) {
          return Response.json({ error: 'Unknown dataset' }, { status: 400 });
        }

        const config = SHARD_CONFIGS[dataset];
        const shardIndex = getShardId(shardKey, config.count);
        const stub = getShardStub(env, dataset, shardIndex);

        const body = await request.json();
        const response = await stub.fetch(new Request('http://shard/query', {
          method: 'POST',
          body: JSON.stringify(body),
          headers: { 'Content-Type': 'application/json' },
        }));

        const result = await response.json();
        return Response.json({ shardIndex, ...result as object });
      }

      // Scatter query across all shards
      if (path.startsWith('/scatter/') && request.method === 'POST') {
        const dataset = path.split('/')[2] as keyof typeof SHARD_CONFIGS;

        if (!SHARD_CONFIGS[dataset]) {
          return Response.json({ error: 'Unknown dataset' }, { status: 400 });
        }

        const config = SHARD_CONFIGS[dataset];
        const body = await request.text();
        const start = performance.now();

        // Query all shards in parallel
        const promises = [];
        for (let i = 0; i < config.count; i++) {
          const stub = getShardStub(env, dataset, i);
          promises.push(
            stub.fetch(new Request('http://shard/query', {
              method: 'POST',
              body,
              headers: { 'Content-Type': 'application/json' },
            }))
              .then(r => r.json())
              .then(r => ({ shard: i, ...(r as object) }))
              .catch(err => ({ shard: i, error: String(err) }))
          );
        }

        const results = await Promise.all(promises);
        const allRows = results.flatMap(r => (r as any).rows || []);

        return Response.json({
          totalShards: config.count,
          totalRows: allRows.length,
          latencyMs: performance.now() - start,
          shardResults: results,
        });
      }

      // Batch insert to specific shard
      if (path.startsWith('/batch/') && request.method === 'POST') {
        const parts = path.split('/');
        const dataset = parts[2] as keyof typeof SHARD_CONFIGS;
        const shardKey = parts[3] || '0';

        if (!SHARD_CONFIGS[dataset]) {
          return Response.json({ error: 'Unknown dataset' }, { status: 400 });
        }

        const config = SHARD_CONFIGS[dataset];
        const shardIndex = getShardId(shardKey, config.count);
        const stub = getShardStub(env, dataset, shardIndex);

        const body = await request.text();
        const response = await stub.fetch(new Request('http://shard/batch', {
          method: 'POST',
          body,
          headers: { 'Content-Type': 'application/json' },
        }));

        const result = await response.json();
        return Response.json({ shardIndex, ...result as object });
      }

      // Get stats from all shards
      if (path.startsWith('/stats/')) {
        const dataset = path.split('/')[2] as keyof typeof SHARD_CONFIGS;

        if (!SHARD_CONFIGS[dataset]) {
          return Response.json({ error: 'Unknown dataset' }, { status: 400 });
        }

        const config = SHARD_CONFIGS[dataset];
        const promises = [];

        for (let i = 0; i < config.count; i++) {
          const stub = getShardStub(env, dataset, i);
          promises.push(
            stub.fetch(new Request('http://shard/stats'))
              .then(r => r.json())
              .then(stats => ({ shard: i, ...stats as object }))
              .catch(err => ({ shard: i, error: String(err) }))
          );
        }

        const shardStats = await Promise.all(promises);

        // Aggregate totals
        const totals: Record<string, number> = {};
        for (const s of shardStats) {
          const stats = s as { rowCounts?: Record<string, number> };
          if (stats.rowCounts) {
            for (const [table, count] of Object.entries(stats.rowCounts)) {
              totals[table] = (totals[table] || 0) + count;
            }
          }
        }

        return Response.json({
          dataset,
          shardCount: config.count,
          totalRowCounts: totals,
          shardStats,
        });
      }

      // ==========================================================================
      // Cache API Endpoints - Compare edge cache vs sharded DOs
      // ==========================================================================

      // Get the cache instance for benchmarks
      const cache = await caches.open('dosql-benchmark');
      const cacheBaseUrl = 'https://dosql-cache.internal';

      // Store a record in Cache API
      if (path.startsWith('/cache/put/') && request.method === 'POST') {
        const parts = path.split('/');
        const dataset = parts[3];
        const body = await request.json() as { key: string; value: unknown; ttl?: number };
        const { key, value, ttl = 3600 } = body;

        const cacheKey = `${cacheBaseUrl}/${dataset}/${key}`;
        const cacheResponse = new Response(JSON.stringify(value), {
          headers: {
            'Content-Type': 'application/json',
            'Cache-Control': `max-age=${ttl}`,
          },
        });

        await cache.put(cacheKey, cacheResponse);
        return Response.json({ success: true, key, cached: true });
      }

      // Get a record from Cache API
      if (path.startsWith('/cache/get/')) {
        const parts = path.split('/');
        const dataset = parts[3];
        const key = parts[4];
        const start = performance.now();

        const cacheKey = `${cacheBaseUrl}/${dataset}/${key}`;
        const cached = await cache.match(cacheKey);

        if (cached) {
          const value = await cached.json();
          return Response.json({
            hit: true,
            key,
            value,
            latencyMs: performance.now() - start,
            source: 'cache',
          });
        }

        return Response.json({
          hit: false,
          key,
          latencyMs: performance.now() - start,
          source: 'cache',
        });
      }

      // Bulk populate cache from DO data
      if (path.startsWith('/cache/populate/') && request.method === 'POST') {
        const dataset = path.split('/')[3] as keyof typeof SHARD_CONFIGS;
        if (!SHARD_CONFIGS[dataset]) {
          return Response.json({ error: 'Unknown dataset' }, { status: 400 });
        }

        const body = await request.json() as { keys: string[]; ttl?: number };
        const { keys, ttl = 3600 } = body;
        const config = SHARD_CONFIGS[dataset];
        const start = performance.now();

        // Group keys by shard
        const shardGroups = new Map<number, string[]>();
        for (const key of keys) {
          const shardIndex = getShardId(key, config.count);
          if (!shardGroups.has(shardIndex)) {
            shardGroups.set(shardIndex, []);
          }
          shardGroups.get(shardIndex)!.push(key);
        }

        // Fetch from DOs and populate cache
        let populated = 0;
        const errors: string[] = [];

        for (const [shardIndex, shardKeys] of shardGroups) {
          const stub = getShardStub(env, dataset, shardIndex);

          // Build SQL to fetch all keys for this shard
          const placeholders = shardKeys.map(() => '?').join(',');
          const sql = dataset === 'imdb'
            ? `SELECT * FROM title_basics WHERE tconst IN (${placeholders})`
            : dataset === 'wiktionary'
            ? `SELECT * FROM words WHERE word IN (${placeholders})`
            : `SELECT * FROM vertices WHERE node_id IN (${placeholders})`;

          try {
            const response = await stub.fetch(new Request('http://shard/query', {
              method: 'POST',
              body: JSON.stringify({ sql, params: shardKeys }),
              headers: { 'Content-Type': 'application/json' },
            }));

            const result = await response.json() as { rows: Array<Record<string, unknown>> };

            // Cache each row
            for (const row of result.rows || []) {
              const rowKey = dataset === 'imdb'
                ? (row.tconst as string)
                : dataset === 'wiktionary'
                ? (row.word as string)
                : String(row.node_id);

              const cacheKey = `${cacheBaseUrl}/${dataset}/${rowKey}`;
              const cacheResponse = new Response(JSON.stringify(row), {
                headers: {
                  'Content-Type': 'application/json',
                  'Cache-Control': `max-age=${ttl}`,
                },
              });
              await cache.put(cacheKey, cacheResponse);
              populated++;
            }
          } catch (err) {
            errors.push(`Shard ${shardIndex}: ${String(err)}`);
          }
        }

        return Response.json({
          populated,
          requested: keys.length,
          latencyMs: performance.now() - start,
          errors: errors.length > 0 ? errors : undefined,
        });
      }

      // Benchmark: Compare Cache API vs Sharded DO performance
      if (path === '/cache/benchmark' && request.method === 'POST') {
        const body = await request.json() as {
          dataset: keyof typeof SHARD_CONFIGS;
          keys: string[];
          iterations?: number;
        };
        const { dataset, keys, iterations = 100 } = body;

        if (!SHARD_CONFIGS[dataset]) {
          return Response.json({ error: 'Unknown dataset' }, { status: 400 });
        }

        const config = SHARD_CONFIGS[dataset];
        const results = {
          cache: { latencies: [] as number[], hits: 0, misses: 0 },
          do: { latencies: [] as number[] },
        };

        // Warm up: populate cache with these keys first
        const warmupKeys = keys.slice(0, Math.min(keys.length, 10));
        for (const key of warmupKeys) {
          const shardIndex = getShardId(key, config.count);
          const stub = getShardStub(env, dataset, shardIndex);

          const sql = dataset === 'imdb'
            ? 'SELECT * FROM title_basics WHERE tconst = ?'
            : dataset === 'wiktionary'
            ? 'SELECT * FROM words WHERE word = ?'
            : 'SELECT * FROM vertices WHERE node_id = ?';

          try {
            const response = await stub.fetch(new Request('http://shard/query', {
              method: 'POST',
              body: JSON.stringify({ sql, params: [key] }),
              headers: { 'Content-Type': 'application/json' },
            }));
            const result = await response.json() as { rows: unknown[] };
            if (result.rows?.[0]) {
              const cacheKey = `${cacheBaseUrl}/${dataset}/${key}`;
              await cache.put(cacheKey, new Response(JSON.stringify(result.rows[0]), {
                headers: { 'Content-Type': 'application/json', 'Cache-Control': 'max-age=3600' },
              }));
            }
          } catch {
            // Ignore warmup errors
          }
        }

        // Run benchmark iterations
        for (let i = 0; i < iterations; i++) {
          const key = keys[i % keys.length];

          // Test Cache API
          const cacheStart = performance.now();
          const cacheKey = `${cacheBaseUrl}/${dataset}/${key}`;
          const cached = await cache.match(cacheKey);
          const cacheLatency = performance.now() - cacheStart;
          results.cache.latencies.push(cacheLatency);
          if (cached) {
            results.cache.hits++;
          } else {
            results.cache.misses++;
          }

          // Test DO query
          const doStart = performance.now();
          const shardIndex = getShardId(key, config.count);
          const stub = getShardStub(env, dataset, shardIndex);

          const sql = dataset === 'imdb'
            ? 'SELECT * FROM title_basics WHERE tconst = ?'
            : dataset === 'wiktionary'
            ? 'SELECT * FROM words WHERE word = ?'
            : 'SELECT * FROM vertices WHERE node_id = ?';

          await stub.fetch(new Request('http://shard/query', {
            method: 'POST',
            body: JSON.stringify({ sql, params: [key] }),
            headers: { 'Content-Type': 'application/json' },
          }));
          const doLatency = performance.now() - doStart;
          results.do.latencies.push(doLatency);
        }

        // Calculate statistics
        const calcStats = (latencies: number[]) => {
          const sorted = [...latencies].sort((a, b) => a - b);
          return {
            p50: sorted[Math.floor(sorted.length * 0.5)],
            p95: sorted[Math.floor(sorted.length * 0.95)],
            p99: sorted[Math.floor(sorted.length * 0.99)],
            avg: latencies.reduce((a, b) => a + b, 0) / latencies.length,
            min: Math.min(...latencies),
            max: Math.max(...latencies),
          };
        };

        return Response.json({
          dataset,
          iterations,
          keysUsed: keys.length,
          cache: {
            ...calcStats(results.cache.latencies),
            hitRate: results.cache.hits / (results.cache.hits + results.cache.misses),
            hits: results.cache.hits,
            misses: results.cache.misses,
          },
          do: calcStats(results.do.latencies),
          speedup: {
            p50: calcStats(results.do.latencies).p50 / calcStats(results.cache.latencies).p50,
            avg: calcStats(results.do.latencies).avg / calcStats(results.cache.latencies).avg,
          },
          tradeoffs: {
            cache: ['Edge-local (~1ms)', 'Eventually consistent', 'Key-value only', 'TTL-based expiry'],
            do: ['Single location', 'Strongly consistent', 'Full SQL queries', 'Durable storage'],
          },
        });
      }

      // Benchmark: Full comparison with scatter queries
      if (path === '/cache/benchmark/scatter' && request.method === 'POST') {
        const body = await request.json() as {
          dataset: keyof typeof SHARD_CONFIGS;
          sql: string;
          cacheKey: string;
          iterations?: number;
        };
        const { dataset, sql, cacheKey, iterations = 50 } = body;

        if (!SHARD_CONFIGS[dataset]) {
          return Response.json({ error: 'Unknown dataset' }, { status: 400 });
        }

        const config = SHARD_CONFIGS[dataset];
        const results = {
          cache: { latencies: [] as number[], hits: 0, misses: 0 },
          scatter: { latencies: [] as number[] },
        };

        // First iteration: run scatter query and cache result
        const scatterStart = performance.now();
        const scatterPromises = [];
        for (let i = 0; i < config.count; i++) {
          const stub = getShardStub(env, dataset, i);
          scatterPromises.push(
            stub.fetch(new Request('http://shard/query', {
              method: 'POST',
              body: JSON.stringify({ sql, params: [] }),
              headers: { 'Content-Type': 'application/json' },
            }))
              .then(r => r.json())
              .then(r => (r as { rows: unknown[] }).rows || [])
              .catch(() => [])
          );
        }
        const scatterResults = await Promise.all(scatterPromises);
        const allRows = scatterResults.flat();
        const firstScatterLatency = performance.now() - scatterStart;

        // Cache the aggregated result
        const fullCacheKey = `${cacheBaseUrl}/${dataset}/scatter/${cacheKey}`;
        await cache.put(fullCacheKey, new Response(JSON.stringify(allRows), {
          headers: { 'Content-Type': 'application/json', 'Cache-Control': 'max-age=60' },
        }));

        // Run benchmark iterations
        for (let i = 0; i < iterations; i++) {
          // Test Cache API (cached scatter result)
          const cacheStart = performance.now();
          const cached = await cache.match(fullCacheKey);
          const cacheLatency = performance.now() - cacheStart;
          results.cache.latencies.push(cacheLatency);
          if (cached) {
            results.cache.hits++;
          } else {
            results.cache.misses++;
          }

          // Test scatter query (every 10th iteration to avoid overload)
          if (i % 10 === 0) {
            const doStart = performance.now();
            const promises = [];
            for (let j = 0; j < config.count; j++) {
              const stub = getShardStub(env, dataset, j);
              promises.push(
                stub.fetch(new Request('http://shard/query', {
                  method: 'POST',
                  body: JSON.stringify({ sql, params: [] }),
                  headers: { 'Content-Type': 'application/json' },
                }))
              );
            }
            await Promise.all(promises);
            results.scatter.latencies.push(performance.now() - doStart);
          }
        }

        // Add initial scatter time
        results.scatter.latencies.unshift(firstScatterLatency);

        const calcStats = (latencies: number[]) => {
          if (latencies.length === 0) return { p50: 0, p95: 0, p99: 0, avg: 0, min: 0, max: 0 };
          const sorted = [...latencies].sort((a, b) => a - b);
          return {
            p50: sorted[Math.floor(sorted.length * 0.5)],
            p95: sorted[Math.floor(sorted.length * 0.95)],
            p99: sorted[Math.floor(sorted.length * 0.99)],
            avg: latencies.reduce((a, b) => a + b, 0) / latencies.length,
            min: Math.min(...latencies),
            max: Math.max(...latencies),
          };
        };

        return Response.json({
          dataset,
          iterations,
          shardCount: config.count,
          sql,
          cache: {
            ...calcStats(results.cache.latencies),
            hitRate: results.cache.hits / (results.cache.hits + results.cache.misses),
          },
          scatter: calcStats(results.scatter.latencies),
          speedup: {
            p50: calcStats(results.scatter.latencies).p50 / Math.max(calcStats(results.cache.latencies).p50, 0.1),
            avg: calcStats(results.scatter.latencies).avg / Math.max(calcStats(results.cache.latencies).avg, 0.1),
          },
          recommendation: calcStats(results.cache.latencies).p50 < 5
            ? 'Cache API is significantly faster for read-heavy workloads with acceptable staleness'
            : 'Consider hybrid approach: cache for hot data, DO for consistency-critical queries',
        });
      }

      return new Response('Not Found', { status: 404 });
    } catch (err) {
      return Response.json({ error: String(err) }, { status: 500 });
    }
  },
};
