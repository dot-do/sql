/**
 * Massive-Scale Data Loader
 *
 * Streaming loader for massive datasets:
 * - IMDB TSV files (gzip compressed)
 * - Wiktionary JSONL (gzip compressed)
 * - Common Crawl graph (gzip text)
 *
 * Features:
 * - Streaming decompression (no full file in memory)
 * - Batched inserts (1000 rows per transaction)
 * - Shard-aware routing
 * - Progress tracking
 */

import { createReadStream } from 'node:fs';
import { createGunzip } from 'node:zlib';
import { createInterface } from 'node:readline';
import { pipeline } from 'node:stream/promises';
import { Writable } from 'node:stream';
import {
  IMDB_SCHEMAS,
  IMDB_SHARD_CONFIG,
  WIKTIONARY_SCHEMAS,
  WIKTIONARY_SHARD_CONFIG,
  CRAWL_GRAPH_SCHEMAS,
  CRAWL_GRAPH_SHARD_CONFIG,
  DATASET_URLS,
} from './schema.js';

// =============================================================================
// Types
// =============================================================================

interface LoaderStats {
  rowsLoaded: number;
  bytesProcessed: number;
  errors: number;
  startTime: number;
  lastReportTime: number;
}

interface ShardRouter {
  getShard(key: string | number): string;
  getAllShards(): string[];
}

interface ShardedDatabase {
  exec(shardId: string, sql: string, params?: unknown[]): Promise<void>;
  run(shardId: string, sql: string, params?: unknown[]): Promise<void>;
  batch(shardId: string, statements: Array<{ sql: string; params: unknown[] }>): Promise<void>;
}

// =============================================================================
// Hash Function (FNV-1a for shard routing)
// =============================================================================

function fnv1a(input: string): number {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function createHashRouter(shardCount: number): ShardRouter {
  const shardIds = Array.from({ length: shardCount }, (_, i) => `shard_${i}`);
  return {
    getShard(key: string | number): string {
      const hash = fnv1a(String(key));
      return shardIds[hash % shardCount];
    },
    getAllShards(): string[] {
      return shardIds;
    },
  };
}

function createRangeRouter(boundaries: Array<{ min: number; max: number | null }>): ShardRouter {
  const shardIds = boundaries.map((_, i) => `shard_${i}`);
  return {
    getShard(key: string | number): string {
      const numKey = typeof key === 'number' ? key : parseInt(key, 10);
      for (let i = 0; i < boundaries.length; i++) {
        const b = boundaries[i];
        if (numKey >= b.min && (b.max === null || numKey < b.max)) {
          return shardIds[i];
        }
      }
      return shardIds[shardIds.length - 1];
    },
    getAllShards(): string[] {
      return shardIds;
    },
  };
}

// =============================================================================
// Progress Reporter
// =============================================================================

function reportProgress(name: string, stats: LoaderStats, final = false): void {
  const elapsed = (Date.now() - stats.startTime) / 1000;
  const rowsPerSec = Math.round(stats.rowsLoaded / elapsed);
  const mbProcessed = (stats.bytesProcessed / 1024 / 1024).toFixed(2);

  console.log(
    `[${name}] ${final ? 'DONE' : 'Progress'}: ${stats.rowsLoaded.toLocaleString()} rows, ` +
    `${mbProcessed} MB, ${rowsPerSec.toLocaleString()} rows/sec, ${stats.errors} errors`
  );
}

// =============================================================================
// IMDB Loader (TSV)
// =============================================================================

interface IMDBRow {
  [key: string]: string;
}

function parseTSVLine(line: string, headers: string[]): IMDBRow {
  const values = line.split('\t');
  const row: IMDBRow = {};
  for (let i = 0; i < headers.length; i++) {
    row[headers[i]] = values[i] === '\\N' ? '' : values[i];
  }
  return row;
}

export async function loadIMDBTable(
  tableName: keyof typeof IMDB_SCHEMAS,
  filePath: string,
  db: ShardedDatabase,
  options: { batchSize?: number; limit?: number } = {}
): Promise<LoaderStats> {
  const { batchSize = 1000, limit } = options;
  const router = createHashRouter(IMDB_SHARD_CONFIG.shardCount);
  const stats: LoaderStats = {
    rowsLoaded: 0,
    bytesProcessed: 0,
    errors: 0,
    startTime: Date.now(),
    lastReportTime: Date.now(),
  };

  // Create tables on all shards
  const schema = IMDB_SCHEMAS[tableName];
  for (const shardId of router.getAllShards()) {
    await db.exec(shardId, schema);
  }

  // Prepare insert statement based on table
  const insertSQL = getIMDBInsertSQL(tableName);

  // Batch buffers per shard
  const shardBatches = new Map<string, Array<{ sql: string; params: unknown[] }>>();
  for (const shardId of router.getAllShards()) {
    shardBatches.set(shardId, []);
  }

  // Stream and process
  const gunzip = createGunzip();
  const fileStream = createReadStream(filePath);
  const rl = createInterface({ input: pipeline(fileStream, gunzip) as any });

  let headers: string[] | null = null;
  let lineNumber = 0;

  for await (const line of rl) {
    lineNumber++;
    stats.bytesProcessed += line.length + 1;

    // First line is header
    if (!headers) {
      headers = line.split('\t');
      continue;
    }

    // Check limit
    if (limit && stats.rowsLoaded >= limit) break;

    try {
      const row = parseTSVLine(line, headers);
      const shardKey = row['tconst'] || row['nconst'] || row['id'] || String(lineNumber);
      const shardId = router.getShard(shardKey);
      const params = getIMDBParams(tableName, row);

      shardBatches.get(shardId)!.push({ sql: insertSQL, params });
      stats.rowsLoaded++;

      // Flush batch if full
      const batch = shardBatches.get(shardId)!;
      if (batch.length >= batchSize) {
        await db.batch(shardId, batch);
        shardBatches.set(shardId, []);
      }

      // Progress report every 10 seconds
      if (Date.now() - stats.lastReportTime > 10000) {
        reportProgress(`IMDB:${tableName}`, stats);
        stats.lastReportTime = Date.now();
      }
    } catch (err) {
      stats.errors++;
      if (stats.errors <= 5) {
        console.error(`Error on line ${lineNumber}:`, err);
      }
    }
  }

  // Flush remaining batches
  for (const [shardId, batch] of shardBatches) {
    if (batch.length > 0) {
      await db.batch(shardId, batch);
    }
  }

  reportProgress(`IMDB:${tableName}`, stats, true);
  return stats;
}

function getIMDBInsertSQL(tableName: string): string {
  switch (tableName) {
    case 'title_basics':
      return 'INSERT INTO title_basics (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)';
    case 'name_basics':
      return 'INSERT INTO name_basics (nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles) VALUES (?, ?, ?, ?, ?, ?)';
    case 'title_ratings':
      return 'INSERT INTO title_ratings (tconst, averageRating, numVotes) VALUES (?, ?, ?)';
    case 'title_principals':
      return 'INSERT INTO title_principals (tconst, ordering, nconst, category, job, characters) VALUES (?, ?, ?, ?, ?, ?)';
    case 'title_crew':
      return 'INSERT INTO title_crew (tconst, directors, writers) VALUES (?, ?, ?)';
    default:
      throw new Error(`Unknown table: ${tableName}`);
  }
}

function getIMDBParams(tableName: string, row: IMDBRow): unknown[] {
  switch (tableName) {
    case 'title_basics':
      return [
        row.tconst,
        row.titleType,
        row.primaryTitle,
        row.originalTitle,
        row.isAdult === '1' ? 1 : 0,
        row.startYear ? parseInt(row.startYear, 10) : null,
        row.endYear ? parseInt(row.endYear, 10) : null,
        row.runtimeMinutes ? parseInt(row.runtimeMinutes, 10) : null,
        row.genres,
      ];
    case 'name_basics':
      return [
        row.nconst,
        row.primaryName,
        row.birthYear ? parseInt(row.birthYear, 10) : null,
        row.deathYear ? parseInt(row.deathYear, 10) : null,
        row.primaryProfession,
        row.knownForTitles,
      ];
    case 'title_ratings':
      return [
        row.tconst,
        parseFloat(row.averageRating),
        parseInt(row.numVotes, 10),
      ];
    case 'title_principals':
      return [
        row.tconst,
        parseInt(row.ordering, 10),
        row.nconst,
        row.category,
        row.job,
        row.characters,
      ];
    case 'title_crew':
      return [row.tconst, row.directors, row.writers];
    default:
      throw new Error(`Unknown table: ${tableName}`);
  }
}

// =============================================================================
// Wiktionary Loader (JSONL)
// =============================================================================

interface WiktionaryEntry {
  word: string;
  lang_code: string;
  pos?: string;
  etymology_text?: string;
  senses?: Array<{
    glosses?: string[];
    tags?: string[];
    categories?: string[];
  }>;
  forms?: Array<{
    form: string;
    tags?: string[];
  }>;
}

export async function loadWiktionary(
  filePath: string,
  db: ShardedDatabase,
  options: { batchSize?: number; limit?: number } = {}
): Promise<LoaderStats> {
  const { batchSize = 1000, limit } = options;
  const router = createHashRouter(WIKTIONARY_SHARD_CONFIG.shardCount);
  const stats: LoaderStats = {
    rowsLoaded: 0,
    bytesProcessed: 0,
    errors: 0,
    startTime: Date.now(),
    lastReportTime: Date.now(),
  };

  // Create tables on all shards
  for (const shardId of router.getAllShards()) {
    await db.exec(shardId, WIKTIONARY_SCHEMAS.words);
    await db.exec(shardId, WIKTIONARY_SCHEMAS.senses);
    await db.exec(shardId, WIKTIONARY_SCHEMAS.forms);
  }

  // Word ID counters per shard
  const wordIdCounters = new Map<string, number>();
  for (const shardId of router.getAllShards()) {
    wordIdCounters.set(shardId, 0);
  }

  // Batch buffers per shard
  const shardBatches = new Map<string, Array<{ sql: string; params: unknown[] }>>();
  for (const shardId of router.getAllShards()) {
    shardBatches.set(shardId, []);
  }

  // Stream and process
  const gunzip = createGunzip();
  const fileStream = createReadStream(filePath);
  const rl = createInterface({ input: pipeline(fileStream, gunzip) as any });

  for await (const line of rl) {
    stats.bytesProcessed += line.length + 1;
    if (limit && stats.rowsLoaded >= limit) break;

    try {
      const entry: WiktionaryEntry = JSON.parse(line);
      const shardId = router.getShard(entry.lang_code);
      const batch = shardBatches.get(shardId)!;

      // Get next word ID for this shard
      const wordId = wordIdCounters.get(shardId)!;
      wordIdCounters.set(shardId, wordId + 1);

      // Insert word
      batch.push({
        sql: 'INSERT INTO words (id, word, lang_code, pos, etymology) VALUES (?, ?, ?, ?, ?)',
        params: [wordId, entry.word, entry.lang_code, entry.pos || null, entry.etymology_text || null],
      });

      // Insert senses
      if (entry.senses) {
        for (const sense of entry.senses) {
          const gloss = sense.glosses?.join('; ') || '';
          batch.push({
            sql: 'INSERT INTO senses (word_id, gloss, tags, categories) VALUES (?, ?, ?, ?)',
            params: [
              wordId,
              gloss,
              sense.tags?.join(',') || null,
              sense.categories?.join(',') || null,
            ],
          });
        }
      }

      // Insert forms
      if (entry.forms) {
        for (const form of entry.forms) {
          batch.push({
            sql: 'INSERT INTO forms (word_id, form, tags) VALUES (?, ?, ?)',
            params: [wordId, form.form, form.tags?.join(',') || null],
          });
        }
      }

      stats.rowsLoaded++;

      // Flush batch if full
      if (batch.length >= batchSize) {
        await db.batch(shardId, batch);
        shardBatches.set(shardId, []);
      }

      // Progress report
      if (Date.now() - stats.lastReportTime > 10000) {
        reportProgress('Wiktionary', stats);
        stats.lastReportTime = Date.now();
      }
    } catch (err) {
      stats.errors++;
      if (stats.errors <= 5) {
        console.error('Wiktionary parse error:', err);
      }
    }
  }

  // Flush remaining
  for (const [shardId, batch] of shardBatches) {
    if (batch.length > 0) {
      await db.batch(shardId, batch);
    }
  }

  reportProgress('Wiktionary', stats, true);
  return stats;
}

// =============================================================================
// Common Crawl Graph Loader
// =============================================================================

export async function loadCrawlGraphVertices(
  filePath: string,
  db: ShardedDatabase,
  options: { batchSize?: number; limit?: number } = {}
): Promise<LoaderStats> {
  const { batchSize = 1000, limit } = options;
  const router = createRangeRouter(CRAWL_GRAPH_SHARD_CONFIG.boundaries);
  const stats: LoaderStats = {
    rowsLoaded: 0,
    bytesProcessed: 0,
    errors: 0,
    startTime: Date.now(),
    lastReportTime: Date.now(),
  };

  // Create tables on all shards
  for (const shardId of router.getAllShards()) {
    await db.exec(shardId, CRAWL_GRAPH_SCHEMAS.vertices);
  }

  // Batch buffers
  const shardBatches = new Map<string, Array<{ sql: string; params: unknown[] }>>();
  for (const shardId of router.getAllShards()) {
    shardBatches.set(shardId, []);
  }

  // Stream and process
  const gunzip = createGunzip();
  const fileStream = createReadStream(filePath);
  const rl = createInterface({ input: pipeline(fileStream, gunzip) as any });

  for await (const line of rl) {
    stats.bytesProcessed += line.length + 1;
    if (limit && stats.rowsLoaded >= limit) break;

    try {
      // Format: node_id TAB reversed_domain
      const parts = line.split('\t');
      if (parts.length >= 2) {
        const nodeId = parseInt(parts[0], 10);
        const reverseDomain = parts[1];
        const domain = reverseDomain.split('.').reverse().join('.');

        const shardId = router.getShard(nodeId);
        const batch = shardBatches.get(shardId)!;

        batch.push({
          sql: 'INSERT INTO vertices (node_id, domain, reverse_domain) VALUES (?, ?, ?)',
          params: [nodeId, domain, reverseDomain],
        });

        stats.rowsLoaded++;

        // Flush batch
        if (batch.length >= batchSize) {
          await db.batch(shardId, batch);
          shardBatches.set(shardId, []);
        }

        // Progress
        if (Date.now() - stats.lastReportTime > 10000) {
          reportProgress('CrawlGraph:vertices', stats);
          stats.lastReportTime = Date.now();
        }
      }
    } catch (err) {
      stats.errors++;
      if (stats.errors <= 5) {
        console.error('Vertex parse error:', err);
      }
    }
  }

  // Flush remaining
  for (const [shardId, batch] of shardBatches) {
    if (batch.length > 0) {
      await db.batch(shardId, batch);
    }
  }

  reportProgress('CrawlGraph:vertices', stats, true);
  return stats;
}

export async function loadCrawlGraphEdges(
  filePath: string,
  db: ShardedDatabase,
  options: { batchSize?: number; limit?: number } = {}
): Promise<LoaderStats> {
  const { batchSize = 1000, limit } = options;
  const router = createRangeRouter(CRAWL_GRAPH_SHARD_CONFIG.boundaries);
  const stats: LoaderStats = {
    rowsLoaded: 0,
    bytesProcessed: 0,
    errors: 0,
    startTime: Date.now(),
    lastReportTime: Date.now(),
  };

  // Create tables on all shards
  for (const shardId of router.getAllShards()) {
    await db.exec(shardId, CRAWL_GRAPH_SCHEMAS.edges);
  }

  // Batch buffers
  const shardBatches = new Map<string, Array<{ sql: string; params: unknown[] }>>();
  for (const shardId of router.getAllShards()) {
    shardBatches.set(shardId, []);
  }

  // Stream and process
  const gunzip = createGunzip();
  const fileStream = createReadStream(filePath);
  const rl = createInterface({ input: pipeline(fileStream, gunzip) as any });

  for await (const line of rl) {
    stats.bytesProcessed += line.length + 1;
    if (limit && stats.rowsLoaded >= limit) break;

    try {
      // Format: source_id TAB dest_id
      const parts = line.split('\t');
      if (parts.length >= 2) {
        const sourceId = parseInt(parts[0], 10);
        const destId = parseInt(parts[1], 10);

        // Route by source for outlink locality
        const shardId = router.getShard(sourceId);
        const batch = shardBatches.get(shardId)!;

        batch.push({
          sql: 'INSERT INTO edges (source_id, dest_id) VALUES (?, ?)',
          params: [sourceId, destId],
        });

        stats.rowsLoaded++;

        // Flush batch
        if (batch.length >= batchSize) {
          await db.batch(shardId, batch);
          shardBatches.set(shardId, []);
        }

        // Progress
        if (Date.now() - stats.lastReportTime > 10000) {
          reportProgress('CrawlGraph:edges', stats);
          stats.lastReportTime = Date.now();
        }
      }
    } catch (err) {
      stats.errors++;
      if (stats.errors <= 5) {
        console.error('Edge parse error:', err);
      }
    }
  }

  // Flush remaining
  for (const [shardId, batch] of shardBatches) {
    if (batch.length > 0) {
      await db.batch(shardId, batch);
    }
  }

  reportProgress('CrawlGraph:edges', stats, true);
  return stats;
}

// =============================================================================
// Download Helper
// =============================================================================

export async function downloadDataset(url: string, destPath: string): Promise<void> {
  const { spawn } = await import('node:child_process');
  return new Promise((resolve, reject) => {
    const curl = spawn('curl', ['-L', '-o', destPath, '--progress-bar', url]);
    curl.on('close', (code) => {
      if (code === 0) resolve();
      else reject(new Error(`curl exited with code ${code}`));
    });
  });
}

// =============================================================================
// Main Entry Point
// =============================================================================

export interface LoadAllResult {
  imdb: LoaderStats[];
  wiktionary: LoaderStats;
  crawlGraph: {
    vertices: LoaderStats;
    edges: LoaderStats;
  };
}

export async function loadAll(
  db: ShardedDatabase,
  dataPaths: {
    imdb?: {
      title_basics?: string;
      name_basics?: string;
      title_ratings?: string;
      title_principals?: string;
      title_crew?: string;
    };
    wiktionary?: string;
    crawlGraph?: {
      vertices?: string;
      edges?: string;
    };
  },
  options: { batchSize?: number; limit?: number } = {}
): Promise<LoadAllResult> {
  const result: LoadAllResult = {
    imdb: [],
    wiktionary: { rowsLoaded: 0, bytesProcessed: 0, errors: 0, startTime: 0, lastReportTime: 0 },
    crawlGraph: {
      vertices: { rowsLoaded: 0, bytesProcessed: 0, errors: 0, startTime: 0, lastReportTime: 0 },
      edges: { rowsLoaded: 0, bytesProcessed: 0, errors: 0, startTime: 0, lastReportTime: 0 },
    },
  };

  // Load IMDB tables in parallel
  if (dataPaths.imdb) {
    const imdbPromises: Promise<LoaderStats>[] = [];
    for (const [tableName, path] of Object.entries(dataPaths.imdb)) {
      if (path) {
        imdbPromises.push(
          loadIMDBTable(tableName as keyof typeof IMDB_SCHEMAS, path, db, options)
        );
      }
    }
    result.imdb = await Promise.all(imdbPromises);
  }

  // Load Wiktionary
  if (dataPaths.wiktionary) {
    result.wiktionary = await loadWiktionary(dataPaths.wiktionary, db, options);
  }

  // Load Common Crawl Graph (vertices first, then edges)
  if (dataPaths.crawlGraph) {
    if (dataPaths.crawlGraph.vertices) {
      result.crawlGraph.vertices = await loadCrawlGraphVertices(
        dataPaths.crawlGraph.vertices,
        db,
        options
      );
    }
    if (dataPaths.crawlGraph.edges) {
      result.crawlGraph.edges = await loadCrawlGraphEdges(
        dataPaths.crawlGraph.edges,
        db,
        options
      );
    }
  }

  return result;
}
