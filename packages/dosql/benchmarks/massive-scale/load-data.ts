#!/usr/bin/env npx tsx
/**
 * Data Loader CLI for Massive-Scale Benchmarks
 *
 * Downloads and loads data into the deployed Worker:
 *
 * Usage:
 *   npx tsx load-data.ts --dataset=imdb --endpoint=https://dosql-massive-benchmark.workers.dev
 *   npx tsx load-data.ts --dataset=all --limit=100000
 *
 * Options:
 *   --dataset    Dataset to load: imdb, wiktionary, crawl_graph, or all
 *   --endpoint   Worker endpoint URL
 *   --limit      Limit rows per table (for testing)
 *   --download   Download data files first
 *   --data-dir   Directory for downloaded data files
 */

import { createReadStream, existsSync, mkdirSync } from 'node:fs';
import { createGunzip } from 'node:zlib';
import { createInterface } from 'node:readline';
import { spawn } from 'node:child_process';
import { pipeline } from 'node:stream/promises';

// =============================================================================
// Configuration
// =============================================================================

const DATASET_URLS = {
  imdb: {
    title_basics: 'https://datasets.imdbws.com/title.basics.tsv.gz',
    name_basics: 'https://datasets.imdbws.com/name.basics.tsv.gz',
    title_ratings: 'https://datasets.imdbws.com/title.ratings.tsv.gz',
    title_principals: 'https://datasets.imdbws.com/title.principals.tsv.gz',
    title_crew: 'https://datasets.imdbws.com/title.crew.tsv.gz',
  },
  wiktionary: {
    english: 'https://kaikki.org/dictionary/raw-wiktextract-data.json.gz',
  },
  crawl_graph: {
    // Using smaller sample for testing
    vertices_sample: 'https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2024-10/host/vertices.txt.gz',
    edges_sample: 'https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2024-10/host/edges.txt.gz',
  },
};

const SHARD_COUNTS = {
  imdb: 64,
  wiktionary: 32,
  crawl_graph: 128,
};

// =============================================================================
// Hash Function for Shard Routing
// =============================================================================

function fnv1a(input: string): number {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

function getShardKey(key: string, shardCount: number): string {
  return String(fnv1a(key) % shardCount);
}

// =============================================================================
// Download Helper
// =============================================================================

async function downloadFile(url: string, destPath: string): Promise<void> {
  console.log(`Downloading ${url}...`);
  return new Promise((resolve, reject) => {
    const curl = spawn('curl', ['-L', '-o', destPath, '--progress-bar', url]);
    curl.stderr.pipe(process.stderr);
    curl.on('close', (code) => {
      if (code === 0) {
        console.log(`Downloaded: ${destPath}`);
        resolve();
      } else {
        reject(new Error(`curl exited with code ${code}`));
      }
    });
  });
}

// =============================================================================
// Batch Sender
// =============================================================================

interface BatchResult {
  success: boolean;
  error?: string;
  count?: number;
}

async function sendBatch(
  endpoint: string,
  dataset: string,
  shardKey: string,
  statements: Array<{ sql: string; params: unknown[] }>
): Promise<BatchResult> {
  try {
    const response = await fetch(`${endpoint}/batch/${dataset}/${shardKey}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ statements }),
    });

    const result = await response.json() as { success?: boolean; error?: string; count?: number };
    return { success: result.success || false, count: result.count, error: result.error };
  } catch (err) {
    return { success: false, error: String(err) };
  }
}

// =============================================================================
// IMDB Loader
// =============================================================================

async function loadIMDBTable(
  endpoint: string,
  tableName: string,
  filePath: string,
  options: { limit?: number; batchSize?: number }
): Promise<{ rows: number; errors: number }> {
  const { limit, batchSize = 500 } = options;

  console.log(`Loading IMDB ${tableName} from ${filePath}...`);

  // Batch buffers per shard
  const shardBatches = new Map<string, Array<{ sql: string; params: unknown[] }>>();
  for (let i = 0; i < SHARD_COUNTS.imdb; i++) {
    shardBatches.set(String(i), []);
  }

  const insertSQL = getIMDBInsertSQL(tableName);
  let headers: string[] | null = null;
  let rows = 0;
  let errors = 0;

  const gunzip = createGunzip();
  const fileStream = createReadStream(filePath);
  const rl = createInterface({ input: fileStream.pipe(gunzip) });

  for await (const line of rl) {
    if (!headers) {
      headers = line.split('\t');
      continue;
    }

    if (limit && rows >= limit) break;

    try {
      const values = line.split('\t');
      const row: Record<string, string> = {};
      for (let i = 0; i < headers.length; i++) {
        row[headers[i]] = values[i] === '\\N' ? '' : values[i];
      }

      const shardKeyValue = row['tconst'] || row['nconst'] || String(rows);
      const shardIndex = getShardKey(shardKeyValue, SHARD_COUNTS.imdb);
      const params = getIMDBParams(tableName, row);

      shardBatches.get(shardIndex)!.push({ sql: insertSQL, params });
      rows++;

      // Flush batches
      for (const [shardKey, batch] of shardBatches) {
        if (batch.length >= batchSize) {
          const result = await sendBatch(endpoint, 'imdb', shardKey, batch);
          if (!result.success) {
            errors++;
            console.error(`Batch error on shard ${shardKey}:`, result.error);
          }
          shardBatches.set(shardKey, []);
        }
      }

      // Progress
      if (rows % 10000 === 0) {
        console.log(`  ${tableName}: ${rows.toLocaleString()} rows loaded, ${errors} errors`);
      }
    } catch (err) {
      errors++;
    }
  }

  // Flush remaining
  for (const [shardKey, batch] of shardBatches) {
    if (batch.length > 0) {
      const result = await sendBatch(endpoint, 'imdb', shardKey, batch);
      if (!result.success) errors++;
    }
  }

  console.log(`  ${tableName}: DONE - ${rows.toLocaleString()} rows, ${errors} errors`);
  return { rows, errors };
}

function getIMDBInsertSQL(tableName: string): string {
  switch (tableName) {
    case 'title_basics':
      return 'INSERT OR REPLACE INTO title_basics (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)';
    case 'name_basics':
      return 'INSERT OR REPLACE INTO name_basics (nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles) VALUES (?, ?, ?, ?, ?, ?)';
    case 'title_ratings':
      return 'INSERT OR REPLACE INTO title_ratings (tconst, averageRating, numVotes) VALUES (?, ?, ?)';
    case 'title_principals':
      return 'INSERT OR REPLACE INTO title_principals (tconst, ordering, nconst, category, job, characters) VALUES (?, ?, ?, ?, ?, ?)';
    case 'title_crew':
      return 'INSERT OR REPLACE INTO title_crew (tconst, directors, writers) VALUES (?, ?, ?)';
    default:
      throw new Error(`Unknown IMDB table: ${tableName}`);
  }
}

function getIMDBParams(tableName: string, row: Record<string, string>): unknown[] {
  switch (tableName) {
    case 'title_basics':
      return [
        row.tconst, row.titleType, row.primaryTitle, row.originalTitle,
        row.isAdult === '1' ? 1 : 0,
        row.startYear ? parseInt(row.startYear, 10) : null,
        row.endYear ? parseInt(row.endYear, 10) : null,
        row.runtimeMinutes ? parseInt(row.runtimeMinutes, 10) : null,
        row.genres,
      ];
    case 'name_basics':
      return [
        row.nconst, row.primaryName,
        row.birthYear ? parseInt(row.birthYear, 10) : null,
        row.deathYear ? parseInt(row.deathYear, 10) : null,
        row.primaryProfession, row.knownForTitles,
      ];
    case 'title_ratings':
      return [row.tconst, parseFloat(row.averageRating), parseInt(row.numVotes, 10)];
    case 'title_principals':
      return [row.tconst, parseInt(row.ordering, 10), row.nconst, row.category, row.job, row.characters];
    case 'title_crew':
      return [row.tconst, row.directors, row.writers];
    default:
      throw new Error(`Unknown IMDB table: ${tableName}`);
  }
}

// =============================================================================
// Main Entry Point
// =============================================================================

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const config = {
    dataset: 'imdb' as 'imdb' | 'wiktionary' | 'crawl_graph' | 'all',
    endpoint: 'http://localhost:8787',
    limit: undefined as number | undefined,
    download: false,
    dataDir: './data',
  };

  // Parse args
  for (const arg of args) {
    if (arg.startsWith('--dataset=')) {
      config.dataset = arg.split('=')[1] as typeof config.dataset;
    } else if (arg.startsWith('--endpoint=')) {
      config.endpoint = arg.split('=')[1];
    } else if (arg.startsWith('--limit=')) {
      config.limit = parseInt(arg.split('=')[1], 10);
    } else if (arg === '--download') {
      config.download = true;
    } else if (arg.startsWith('--data-dir=')) {
      config.dataDir = arg.split('=')[1];
    }
  }

  console.log('Massive-Scale Data Loader');
  console.log('='.repeat(60));
  console.log(`Dataset:  ${config.dataset}`);
  console.log(`Endpoint: ${config.endpoint}`);
  console.log(`Limit:    ${config.limit || 'none'}`);
  console.log('='.repeat(60));

  // Ensure data directory exists
  if (!existsSync(config.dataDir)) {
    mkdirSync(config.dataDir, { recursive: true });
  }

  // Download if requested
  if (config.download) {
    console.log('\nDownloading datasets...');

    if (config.dataset === 'imdb' || config.dataset === 'all') {
      for (const [table, url] of Object.entries(DATASET_URLS.imdb)) {
        const destPath = `${config.dataDir}/imdb_${table}.tsv.gz`;
        if (!existsSync(destPath)) {
          await downloadFile(url, destPath);
        } else {
          console.log(`Skipping ${table} (already exists)`);
        }
      }
    }

    if (config.dataset === 'wiktionary' || config.dataset === 'all') {
      const destPath = `${config.dataDir}/wiktionary.json.gz`;
      if (!existsSync(destPath)) {
        await downloadFile(DATASET_URLS.wiktionary.english, destPath);
      }
    }

    if (config.dataset === 'crawl_graph' || config.dataset === 'all') {
      for (const [name, url] of Object.entries(DATASET_URLS.crawl_graph)) {
        const destPath = `${config.dataDir}/crawl_${name}.txt.gz`;
        if (!existsSync(destPath)) {
          await downloadFile(url, destPath);
        }
      }
    }
  }

  // Setup shards
  console.log('\nSetting up shards...');
  const datasets = config.dataset === 'all'
    ? ['imdb', 'wiktionary', 'crawl_graph']
    : [config.dataset];

  for (const dataset of datasets) {
    console.log(`Setting up ${dataset}...`);
    const response = await fetch(`${config.endpoint}/setup/${dataset}`);
    const result = await response.json();
    console.log(`  ${(result as any).shardCount} shards initialized`);
  }

  // Load data
  console.log('\nLoading data...');
  const totalStats = { rows: 0, errors: 0 };

  if (config.dataset === 'imdb' || config.dataset === 'all') {
    for (const table of ['title_basics', 'title_ratings', 'title_principals'] as const) {
      const filePath = `${config.dataDir}/imdb_${table}.tsv.gz`;
      if (existsSync(filePath)) {
        const stats = await loadIMDBTable(config.endpoint, table, filePath, { limit: config.limit });
        totalStats.rows += stats.rows;
        totalStats.errors += stats.errors;
      } else {
        console.log(`Skipping ${table} (file not found)`);
      }
    }
  }

  // Summary
  console.log('\n' + '='.repeat(60));
  console.log('LOAD COMPLETE');
  console.log('='.repeat(60));
  console.log(`Total Rows:   ${totalStats.rows.toLocaleString()}`);
  console.log(`Total Errors: ${totalStats.errors}`);

  // Get final stats
  for (const dataset of datasets) {
    console.log(`\n[${dataset.toUpperCase()} Stats]`);
    const response = await fetch(`${config.endpoint}/stats/${dataset}`);
    const stats = await response.json() as { totalRowCounts: Record<string, number> };
    for (const [table, count] of Object.entries(stats.totalRowCounts)) {
      console.log(`  ${table}: ${count.toLocaleString()} rows`);
    }
  }
}

main().catch(console.error);
