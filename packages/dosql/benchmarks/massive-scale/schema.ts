/**
 * Massive-Scale Benchmark Schemas
 *
 * Schemas and sharding strategies for:
 * - IMDB (5.5GB, 100M rows) - Relational workload
 * - Wiktionary (20GB JSONL) - Document workload
 * - Common Crawl Host Graph (2.9B edges) - Graph workload
 */

// =============================================================================
// IMDB Schema (Relational)
// =============================================================================

export const IMDB_SCHEMAS = {
  title_basics: `
    CREATE TABLE title_basics (
      tconst TEXT PRIMARY KEY,
      titleType TEXT,
      primaryTitle TEXT,
      originalTitle TEXT,
      isAdult INTEGER,
      startYear INTEGER,
      endYear INTEGER,
      runtimeMinutes INTEGER,
      genres TEXT
    )
  `,

  name_basics: `
    CREATE TABLE name_basics (
      nconst TEXT PRIMARY KEY,
      primaryName TEXT,
      birthYear INTEGER,
      deathYear INTEGER,
      primaryProfession TEXT,
      knownForTitles TEXT
    )
  `,

  title_ratings: `
    CREATE TABLE title_ratings (
      tconst TEXT PRIMARY KEY,
      averageRating REAL,
      numVotes INTEGER
    )
  `,

  title_principals: `
    CREATE TABLE title_principals (
      tconst TEXT,
      ordering INTEGER,
      nconst TEXT,
      category TEXT,
      job TEXT,
      characters TEXT,
      PRIMARY KEY (tconst, ordering)
    )
  `,

  title_crew: `
    CREATE TABLE title_crew (
      tconst TEXT PRIMARY KEY,
      directors TEXT,
      writers TEXT
    )
  `,
};

// IMDB Sharding Strategy: Hash on tconst (title ID)
export const IMDB_SHARD_CONFIG = {
  vindex: 'hash' as const,
  shardKey: 'tconst',
  shardCount: 64, // ~100MB per shard for 5.5GB total
};

// =============================================================================
// Wiktionary Schema (Document)
// =============================================================================

export const WIKTIONARY_SCHEMAS = {
  words: `
    CREATE TABLE words (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      word TEXT NOT NULL,
      lang_code TEXT NOT NULL,
      pos TEXT,
      etymology TEXT
    )
  `,

  senses: `
    CREATE TABLE senses (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      word_id INTEGER NOT NULL,
      gloss TEXT NOT NULL,
      tags TEXT,
      categories TEXT
    )
  `,

  forms: `
    CREATE TABLE forms (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      word_id INTEGER NOT NULL,
      form TEXT NOT NULL,
      tags TEXT
    )
  `,
};

// Wiktionary Sharding: Hash on lang_code for language locality
export const WIKTIONARY_SHARD_CONFIG = {
  vindex: 'hash' as const,
  shardKey: 'lang_code',
  shardCount: 32, // ~600MB per shard for 20GB total
};

// =============================================================================
// Common Crawl Host Graph Schema (Graph)
// =============================================================================

export const CRAWL_GRAPH_SCHEMAS = {
  vertices: `
    CREATE TABLE vertices (
      node_id INTEGER PRIMARY KEY,
      domain TEXT NOT NULL,
      reverse_domain TEXT NOT NULL
    )
  `,

  edges: `
    CREATE TABLE edges (
      source_id INTEGER NOT NULL,
      dest_id INTEGER NOT NULL,
      PRIMARY KEY (source_id, dest_id)
    )
  `,

  // Adjacency list for efficient graph traversal
  adjacency: `
    CREATE TABLE adjacency (
      node_id INTEGER NOT NULL,
      neighbors TEXT NOT NULL,
      out_degree INTEGER NOT NULL,
      PRIMARY KEY (node_id)
    )
  `,
};

// Graph Sharding: Range on node_id for locality
export const CRAWL_GRAPH_SHARD_CONFIG = {
  vindex: 'range' as const,
  shardKey: 'source_id',
  shardCount: 128, // ~800MB per shard for 100GB+
  boundaries: Array.from({ length: 128 }, (_, i) => ({
    min: Math.floor(i * 309_200_000 / 128),
    max: i === 127 ? null : Math.floor((i + 1) * 309_200_000 / 128),
  })),
};

// =============================================================================
// Dataset Download URLs
// =============================================================================

export const DATASET_URLS = {
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
    // Feb-May 2025 release
    vertices_host: 'https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2025-feb-mar-apr-may/host/cc-main-2025-feb-mar-apr-may-host-vertices.paths.gz',
    edges_host: 'https://data.commoncrawl.org/projects/hyperlinkgraph/cc-main-2025-feb-mar-apr-may/host/cc-main-2025-feb-mar-apr-may-host-edges.paths.gz',
  },
};

// =============================================================================
// Benchmark Query Templates
// =============================================================================

export const BENCHMARK_QUERIES = {
  imdb: {
    // Point lookup
    getTitle: 'SELECT * FROM title_basics WHERE tconst = ?',
    // Range scan
    moviesInYear: 'SELECT * FROM title_basics WHERE startYear = ? AND titleType = ?',
    // Join
    movieWithRating: `
      SELECT t.*, r.averageRating, r.numVotes
      FROM title_basics t
      JOIN title_ratings r ON t.tconst = r.tconst
      WHERE t.tconst = ?
    `,
    // Aggregation
    avgRatingByGenre: `
      SELECT genres, AVG(averageRating) as avg_rating, COUNT(*) as count
      FROM title_basics t
      JOIN title_ratings r ON t.tconst = r.tconst
      WHERE startYear >= ? AND startYear <= ?
      GROUP BY genres
      ORDER BY avg_rating DESC
      LIMIT 20
    `,
    // Complex analytics
    topActors: `
      SELECT n.primaryName, COUNT(*) as movies, AVG(r.averageRating) as avg_rating
      FROM title_principals p
      JOIN name_basics n ON p.nconst = n.nconst
      JOIN title_ratings r ON p.tconst = r.tconst
      WHERE p.category = 'actor' OR p.category = 'actress'
      GROUP BY n.nconst
      HAVING COUNT(*) >= ?
      ORDER BY avg_rating DESC
      LIMIT ?
    `,
  },

  wiktionary: {
    // Dictionary lookup
    lookupWord: 'SELECT * FROM words WHERE word = ? AND lang_code = ?',
    // Find definitions
    getDefinitions: `
      SELECT w.word, s.gloss, s.tags
      FROM words w
      JOIN senses s ON w.id = s.word_id
      WHERE w.word = ? AND w.lang_code = ?
    `,
    // Word forms
    getForms: `
      SELECT w.word, f.form, f.tags
      FROM words w
      JOIN forms f ON w.id = f.word_id
      WHERE w.word = ? AND w.lang_code = ?
    `,
    // Count by language
    countByLanguage: `
      SELECT lang_code, COUNT(*) as word_count
      FROM words
      GROUP BY lang_code
      ORDER BY word_count DESC
    `,
  },

  crawl_graph: {
    // Get domain info
    getDomain: 'SELECT * FROM vertices WHERE domain = ?',
    // Outgoing links
    getOutlinks: 'SELECT dest_id FROM edges WHERE source_id = ?',
    // Incoming links (backlinks)
    getBacklinks: 'SELECT source_id FROM edges WHERE dest_id = ?',
    // Degree distribution
    outDegree: 'SELECT out_degree, COUNT(*) FROM adjacency GROUP BY out_degree ORDER BY out_degree',
    // BFS traversal (2-hop)
    twoHop: `
      SELECT DISTINCT e2.dest_id
      FROM edges e1
      JOIN edges e2 ON e1.dest_id = e2.source_id
      WHERE e1.source_id = ?
      LIMIT 1000
    `,
  },
};

// =============================================================================
// Benchmark Configuration
// =============================================================================

export interface BenchmarkConfig {
  dataset: 'imdb' | 'wiktionary' | 'crawl_graph';
  warmupQueries: number;
  benchmarkQueries: number;
  concurrency: number;
  collectStats: boolean;
}

export const DEFAULT_BENCHMARK_CONFIG: BenchmarkConfig = {
  dataset: 'imdb',
  warmupQueries: 100,
  benchmarkQueries: 1000,
  concurrency: 10,
  collectStats: true,
};
