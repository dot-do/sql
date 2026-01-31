/**
 * Massive-Scale Benchmark Worker
 *
 * Cloudflare Worker that deploys sharded DoSQL instances for:
 * - IMDB (64 shards)
 * - Wiktionary (32 shards)
 * - Common Crawl Graph (128 shards)
 *
 * Endpoints:
 *   POST /load/:dataset      - Load data from uploaded file
 *   POST /query/:dataset     - Execute benchmark queries
 *   GET  /stats/:dataset     - Get shard statistics
 *   GET  /health             - Health check
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

      return new Response('Not Found', { status: 404 });
    } catch (err) {
      return Response.json({ error: String(err) }, { status: 500 });
    }
  },
};
