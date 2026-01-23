# Vector Search Guide

DoSQL provides native vector search capabilities for AI/ML applications, enabling semantic search, recommendations, and RAG (Retrieval Augmented Generation) patterns directly within your database on Cloudflare Workers.

## Table of Contents

- [Introduction](#introduction)
  - [Why Vector Search?](#why-vector-search)
  - [Key Capabilities](#key-capabilities)
  - [When to Use Vector Search](#when-to-use-vector-search)
- [Core Concepts](#core-concepts)
  - [Embeddings and Vectors](#embeddings-and-vectors)
  - [Distance Metrics](#distance-metrics)
  - [HNSW Algorithm](#hnsw-algorithm)
- [Creating Vector Columns](#creating-vector-columns)
  - [Schema Design](#schema-design)
  - [Vector Types and Precision](#vector-types-and-precision)
- [Building HNSW Indexes](#building-hnsw-indexes)
  - [SQL Index Creation](#sql-index-creation)
  - [TypeScript Index API](#typescript-index-api)
  - [Parameter Tuning](#parameter-tuning)
- [Querying Vectors](#querying-vectors)
  - [Basic Similarity Search](#basic-similarity-search)
  - [Hybrid Search (Vector + Filters)](#hybrid-search-vector--filters)
  - [Pagination and Thresholds](#pagination-and-thresholds)
- [Embedding Integration](#embedding-integration)
  - [OpenAI Embeddings](#openai-embeddings)
  - [Cloudflare Workers AI](#cloudflare-workers-ai)
  - [Batch Processing](#batch-processing)
  - [Choosing an Embedding Model](#choosing-an-embedding-model)
- [Performance Optimization](#performance-optimization)
  - [Index Tuning for Your Dataset](#index-tuning-for-your-dataset)
  - [Quantization for Memory Efficiency](#quantization-for-memory-efficiency)
  - [Query Optimization Strategies](#query-optimization-strategies)
  - [Batch Operations](#batch-operations)
- [Practical Examples](#practical-examples)
  - [Example: Semantic Search](#example-semantic-search)
  - [Example: Recommendation System](#example-recommendation-system)
  - [Example: RAG Pipeline](#example-rag-pipeline)
- [Troubleshooting](#troubleshooting)
- [Next Steps](#next-steps)

---

## Introduction

### Why Vector Search?

Traditional keyword search fails when users express the same concept differently. A user searching for "affordable laptops" will not find documents containing "budget notebooks" or "cheap portable computers" unless you manually create synonym mappings.

Vector search solves this by representing text, images, and other data as high-dimensional vectors (embeddings) that capture semantic meaning. Similar concepts cluster together in vector space, enabling:

- **Semantic understanding**: "king - man + woman = queen" relationships
- **Cross-modal search**: Find images matching text descriptions
- **Fuzzy matching**: Surface relevant results even with typos or paraphrasing
- **Language independence**: Match concepts across languages (with multilingual models)

### Key Capabilities

DoSQL provides a complete vector search solution optimized for edge deployment:

| Feature | Description |
|---------|-------------|
| **Native vector storage** | Store vectors as BLOB columns with automatic serialization |
| **HNSW indexing** | Approximate nearest neighbor search with configurable precision/speed tradeoffs |
| **Multiple distance metrics** | Cosine, Euclidean (L2), dot product, and Hamming distance |
| **Hybrid queries** | Combine vector similarity with traditional SQL filters |
| **Quantization** | Compress vectors 4-8x for memory efficiency |
| **Edge-native** | Sub-millisecond queries running in Cloudflare Workers |

### When to Use Vector Search

Vector search is ideal for:

- **Semantic document search** - Find relevant documents regardless of exact keyword matches
- **Product recommendations** - Suggest similar items based on description embeddings
- **RAG applications** - Retrieve relevant context for LLM-based Q&A systems
- **Image similarity** - Find visually similar images using vision model embeddings
- **Anomaly detection** - Identify outliers that are distant from normal patterns
- **Deduplication** - Find near-duplicate content using similarity thresholds

---

## Core Concepts

### Embeddings and Vectors

An **embedding** is a numerical representation of data (text, images, audio) as a fixed-length array of floating-point numbers. Embedding models are trained so that semantically similar inputs produce vectors that are close together in the embedding space.

```typescript
// A text embedding from OpenAI (1536 dimensions)
const embedding = new Float32Array([
  0.0231, -0.0142, 0.0089, ..., 0.0156  // 1536 values
]);

// Similar texts produce similar vectors
const similarText = await getEmbedding("machine learning tutorial");
const verySimilar = await getEmbedding("ML learning guide");  // Close to similarText
const different = await getEmbedding("chocolate cake recipe"); // Far from similarText
```

**Dimensionality** refers to the number of elements in the vector. Higher dimensions can capture more nuance but require more storage and computation:

| Model | Dimensions | Storage per Vector |
|-------|------------|-------------------|
| all-MiniLM-L6-v2 | 384 | 1.5 KB (F32) |
| BGE-large | 1024 | 4 KB (F32) |
| OpenAI text-embedding-3-small | 1536 | 6 KB (F32) |
| OpenAI text-embedding-3-large | 3072 | 12 KB (F32) |

### Distance Metrics

Distance metrics measure how far apart two vectors are. DoSQL supports four metrics:

#### Cosine Distance

Measures the angle between vectors, ignoring magnitude. Best for text embeddings where document length varies.

```sql
-- Cosine distance: 0 = identical direction, 2 = opposite direction
SELECT id, title,
       vector_distance_cos(embedding, :query_vector) as distance
FROM documents
ORDER BY distance ASC
LIMIT 10;
```

- **Range**: [0, 2]
- **0** = identical direction (most similar)
- **1** = perpendicular (90 degrees)
- **2** = opposite direction (least similar)
- **Best for**: Text embeddings, normalized vectors

#### Euclidean Distance (L2)

Measures straight-line distance in vector space. Sensitive to vector magnitude.

```sql
-- Euclidean distance: 0 = identical, larger = more different
SELECT id, title,
       vector_distance_l2(embedding, :query_vector) as distance
FROM documents
ORDER BY distance ASC
LIMIT 10;
```

- **Range**: [0, infinity)
- **0** = identical vectors
- **Best for**: Image embeddings, already-normalized vectors

#### Dot Product

For normalized vectors, negative dot product serves as a distance metric. Fast to compute.

```sql
-- Dot product distance: more negative = more similar
SELECT id, title,
       vector_distance_dot(embedding, :query_vector) as distance
FROM documents
ORDER BY distance ASC
LIMIT 10;
```

- **Range**: (-infinity, infinity)
- **More negative** = more similar (for normalized vectors)
- **Best for**: When vectors are already normalized, maximum performance

### HNSW Algorithm

HNSW (Hierarchical Navigable Small World) is a graph-based algorithm for approximate nearest neighbor (ANN) search. It provides excellent query performance with tunable precision.

**How it works:**

1. **Build phase**: Constructs a multi-layer graph where higher layers contain fewer, more spaced-out nodes
2. **Search phase**: Starts at the top layer, greedily navigates toward the query vector, then descends to lower layers for refinement

**Key parameters:**

| Parameter | Effect | Trade-off |
|-----------|--------|-----------|
| `M` | Connections per node | Higher = better recall, more memory |
| `efConstruction` | Build-time search width | Higher = better index quality, slower build |
| `efSearch` | Query-time search width | Higher = better recall, slower queries |

---

## Creating Vector Columns

### Schema Design

Vectors are stored as BLOB columns in DoSQL. The recommended pattern is to store the vector alongside the source data it represents:

```sql
-- .do/migrations/001_create_documents.sql
CREATE TABLE documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  content TEXT NOT NULL,
  embedding BLOB NOT NULL,
  category TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Index for vector similarity search
CREATE INDEX idx_documents_embedding ON documents
USING VECTOR (embedding)
WITH (
  dimensions = 1536,
  metric = 'cosine',
  m = 16,
  ef_construction = 200
);

-- Traditional index for filtering
CREATE INDEX idx_documents_category ON documents(category);
```

**Schema design tips:**

1. **Keep source data** - Store the original text/data alongside embeddings for retrieval
2. **Add filter columns** - Include columns you will filter on (category, date, user_id)
3. **Consider chunking** - For long documents, split into chunks with separate embeddings
4. **Track metadata** - Store embedding model version to handle future migrations

### Vector Types and Precision

DoSQL supports multiple vector precision types for different use cases:

| Type | Bytes/Element | Precision | Use Case |
|------|---------------|-----------|----------|
| `F64` | 8 | Highest | Scientific computing, maximum accuracy |
| `F32` | 4 | Standard | Default for ML embeddings |
| `F16` | 2 | Reduced | Memory-constrained environments |
| `BF16` | 2 | Neural-optimized | Neural network inference |
| `I8` | 1 | Quantized | 4x memory savings with ~1% accuracy loss |
| `F1BIT` | 0.125 | Binary | 32x savings, specialized models only |

```typescript
import { VectorColumn, VectorType, DistanceMetric } from '@dotdo/dosql/vector';

// Standard F32 column (recommended default)
const standardColumn = new VectorColumn({
  columnDef: {
    name: 'embedding',
    dimensions: 1536,
    type: VectorType.F32,
    nullable: false,
    distanceMetric: DistanceMetric.Cosine,
  },
});

// Quantized I8 column for memory efficiency
const quantizedColumn = new VectorColumn({
  columnDef: {
    name: 'embedding',
    dimensions: 1536,
    type: VectorType.F32,
    distanceMetric: DistanceMetric.Cosine,
  },
  quantization: {
    targetType: VectorType.I8,  // 4x memory reduction
  },
});
```

---

## Building HNSW Indexes

### SQL Index Creation

Create an HNSW index on a vector column for fast similarity search:

```sql
-- Create HNSW vector index
CREATE INDEX idx_products_embedding ON products
USING VECTOR (embedding)
WITH (
  dimensions = 1536,       -- Must match your embedding model
  metric = 'cosine',       -- or 'l2', 'dot', 'hamming'
  m = 16,                  -- Connections per node (8-64)
  ef_construction = 200    -- Build quality (64-512)
);
```

### TypeScript Index API

For programmatic control over index creation and management:

```typescript
import { HnswIndex, DistanceMetric } from '@dotdo/dosql/vector';

// Create index with configuration
const index = new HnswIndex({
  M: 16,                           // Connections per node
  efConstruction: 200,             // Build-time quality
  efSearch: 100,                   // Default query-time quality
  distanceMetric: DistanceMetric.Cosine,
  seed: 42,                        // Reproducible builds (optional)
});

// Insert vectors with their row IDs
index.insert(1n, new Float32Array([0.1, 0.2, 0.3, ...]));
index.insert(2n, new Float32Array([0.4, 0.5, 0.6, ...]));

// Search for k nearest neighbors
const results = index.search(
  new Float32Array([0.15, 0.25, 0.35, ...]),
  10,     // k: number of results
  150,    // efSearch: search quality (optional override)
);

// Results include rowId, distance, and similarity score
for (const result of results) {
  console.log(`Row ${result.rowId}: distance=${result.distance}, score=${result.score}`);
}

// Get index statistics
const stats = index.getStats();
console.log({
  nodeCount: stats.nodeCount,
  dimensions: stats.dimensions,
  maxLevel: stats.maxLevel,
  avgConnections: stats.avgConnectionsLevel0,
  memoryUsageBytes: stats.memoryUsageBytes,
});
```

### Parameter Tuning

HNSW parameters control the trade-off between recall (accuracy), speed, and memory:

| Goal | Adjust | Impact |
|------|--------|--------|
| Better recall | Increase `M`, `efConstruction`, `efSearch` | More memory, slower build/query |
| Faster queries | Decrease `efSearch` | Lower recall |
| Faster index build | Decrease `efConstruction` | Lower quality index |
| Less memory | Decrease `M` | Lower recall |

**Recommended settings by dataset size:**

| Dataset Size | M | efConstruction | efSearch | Notes |
|--------------|---|----------------|----------|-------|
| < 10K | 8 | 100 | 50 | Small dataset, fast builds |
| 10K - 100K | 16 | 200 | 100 | Balanced default |
| 100K - 1M | 24 | 300 | 150 | Large dataset |
| > 1M | 32 | 400 | 200 | Very large, consider sharding |

```typescript
// High recall configuration (slower, more accurate)
const highRecallIndex = new HnswIndex({
  M: 32,
  efConstruction: 400,
  efSearch: 300,
  distanceMetric: DistanceMetric.Cosine,
});

// Fast query configuration (faster, less accurate)
const fastIndex = new HnswIndex({
  M: 12,
  efConstruction: 100,
  efSearch: 50,
  distanceMetric: DistanceMetric.Cosine,
});
```

---

## Querying Vectors

### Basic Similarity Search

The fundamental vector search query finds the k most similar vectors:

```sql
-- Find 10 most similar documents to the query embedding
SELECT id, title, content,
       vector_distance_cos(embedding, :query_embedding) as distance
FROM documents
ORDER BY distance ASC
LIMIT 10;
```

In TypeScript:

```typescript
import { DB } from '@dotdo/dosql';

const db = await DB('semantic-search');

// Generate embedding for query text
const queryEmbedding = await getEmbedding('machine learning tutorial');

// Search for similar documents
const results = await db.query(`
  SELECT id, title, content,
         vector_distance_cos(embedding, ?) as distance
  FROM documents
  ORDER BY distance ASC
  LIMIT 10
`, [new Uint8Array(queryEmbedding.buffer)]);

for (const doc of results) {
  console.log(`${doc.title} (distance: ${doc.distance.toFixed(4)})`);
}
```

### Hybrid Search (Vector + Filters)

Combine semantic similarity with traditional SQL filters for precise results:

```sql
-- Find similar documents in a specific category, published recently
SELECT id, title, content,
       vector_distance_cos(embedding, :query_embedding) as distance
FROM documents
WHERE category = 'technology'
  AND published_at > '2024-01-01'
ORDER BY distance ASC
LIMIT 10;
```

```sql
-- Multi-table hybrid search with JOIN
SELECT d.id, d.title, d.content, u.name as author,
       vector_distance_cos(d.embedding, :query) as distance
FROM documents d
JOIN users u ON d.author_id = u.id
WHERE d.status = 'published'
  AND d.language = 'en'
  AND u.verified = true
ORDER BY distance ASC
LIMIT 10;
```

**Pre-filter pattern for large datasets:**

```typescript
import { hybridSearch, VectorColumn } from '@dotdo/dosql/vector';

// First, get candidate IDs from scalar query (uses B-tree index)
const categoryIds = await db.query(
  'SELECT id FROM products WHERE category = ?',
  ['electronics']
);
const filterIds = new Set(categoryIds.map(r => BigInt(r.id)));

// Then, search only within those candidates
const results = hybridSearch(vectorColumn, queryVector, {
  filterIds,           // Only search these IDs
  k: 10,               // Number of results
  oversampleFactor: 2, // Fetch 2x, then filter to k
});
```

**Weighted hybrid search (combine keyword + semantic):**

```sql
-- Combine BM25 keyword relevance with vector similarity
SELECT id, title, content,
  (0.4 * bm25_score(content, :keyword_query)) +
  (0.6 * (1 - vector_distance_cos(embedding, :vector_query))) as combined_score
FROM documents
WHERE content MATCH :keyword_query
ORDER BY combined_score DESC
LIMIT 10;
```

### Pagination and Thresholds

**Pagination:**

```sql
-- Page 3, 20 items per page
SELECT id, title,
       vector_distance_cos(embedding, :query_embedding) as distance
FROM documents
ORDER BY distance ASC
LIMIT 20 OFFSET 40;
```

**Distance threshold:**

```sql
-- Only return results within a similarity threshold
SELECT id, title, content,
       vector_distance_cos(embedding, :query_embedding) as distance
FROM documents
WHERE vector_distance_cos(embedding, :query_embedding) < 0.5
ORDER BY distance ASC
LIMIT 100;
```

---

## Embedding Integration

### OpenAI Embeddings

OpenAI provides high-quality text embeddings through their API:

```typescript
import { DB } from '@dotdo/dosql';
import OpenAI from 'openai';

const openai = new OpenAI();
const db = await DB('embeddings');

async function getEmbedding(text: string): Promise<Float32Array> {
  const response = await openai.embeddings.create({
    model: 'text-embedding-3-small',  // 1536 dimensions
    input: text,
  });
  return new Float32Array(response.data[0].embedding);
}

// Insert document with embedding
async function insertDocument(title: string, content: string) {
  const text = `${title}\n\n${content}`;
  const embedding = await getEmbedding(text);

  await db.run(
    'INSERT INTO documents (title, content, embedding) VALUES (?, ?, ?)',
    [title, content, new Uint8Array(embedding.buffer)]
  );
}

// Search for similar documents
async function searchDocuments(query: string, limit = 10) {
  const queryEmbedding = await getEmbedding(query);

  return db.query(`
    SELECT id, title, content,
           1 - vector_distance_cos(embedding, ?) as similarity
    FROM documents
    ORDER BY similarity DESC
    LIMIT ?
  `, [new Uint8Array(queryEmbedding.buffer), limit]);
}
```

### Cloudflare Workers AI

Use Cloudflare's built-in AI for embeddings without external API calls:

```typescript
interface Env {
  AI: Ai;
  DOSQL_DB: DurableObjectNamespace;
}

async function getEmbedding(env: Env, text: string): Promise<Float32Array> {
  const response = await env.AI.run('@cf/baai/bge-base-en-v1.5', {
    text: [text],
  });
  return new Float32Array(response.data[0]);
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const { query } = await request.json();

    // Generate query embedding using Workers AI
    const queryEmbedding = await getEmbedding(env, query);

    // Search documents
    const db = await getDB(env);
    const results = await db.query(`
      SELECT id, title, content,
             vector_distance_cos(embedding, ?) as distance
      FROM documents
      ORDER BY distance ASC
      LIMIT 10
    `, [new Uint8Array(queryEmbedding.buffer)]);

    return Response.json(results);
  },
};
```

### Batch Processing

Process multiple embeddings efficiently:

```typescript
async function batchEmbed(texts: string[]): Promise<Float32Array[]> {
  // OpenAI supports up to 2048 texts per request
  const response = await openai.embeddings.create({
    model: 'text-embedding-3-small',
    input: texts,
  });

  return response.data.map(d => new Float32Array(d.embedding));
}

// Batch insert documents
async function batchInsertDocuments(docs: Array<{ title: string; content: string }>) {
  const texts = docs.map(d => `${d.title}\n\n${d.content}`);
  const embeddings = await batchEmbed(texts);

  await db.transaction(async (tx) => {
    for (let i = 0; i < docs.length; i++) {
      await tx.run(
        'INSERT INTO documents (title, content, embedding) VALUES (?, ?, ?)',
        [docs[i].title, docs[i].content, new Uint8Array(embeddings[i].buffer)]
      );
    }
  });
}

// Process large datasets in chunks
async function indexLargeDataset(documents: Document[]) {
  const BATCH_SIZE = 100;

  for (let i = 0; i < documents.length; i += BATCH_SIZE) {
    const batch = documents.slice(i, i + BATCH_SIZE);
    await batchInsertDocuments(batch);
    console.log(`Indexed ${Math.min(i + BATCH_SIZE, documents.length)}/${documents.length}`);
  }
}
```

### Choosing an Embedding Model

| Model | Dimensions | Strengths | Cost |
|-------|------------|-----------|------|
| **OpenAI text-embedding-3-small** | 1536 | High quality, well-tested | $0.02/1M tokens |
| **OpenAI text-embedding-3-large** | 3072 | Highest quality | $0.13/1M tokens |
| **Cohere embed-v3** | 1024 | Excellent multilingual | $0.10/1M tokens |
| **Cloudflare BGE-base-en-v1.5** | 768 | Free on Workers AI | Free |
| **all-MiniLM-L6-v2** | 384 | Fast, lightweight | Open source |

**Selection criteria:**

1. **Quality requirements**: For production RAG, use OpenAI or Cohere
2. **Cost sensitivity**: Use Workers AI (free) or open-source models
3. **Latency constraints**: Smaller dimensions = faster queries
4. **Language support**: Cohere excels at multilingual
5. **Privacy**: Self-hosted models avoid sending data externally

---

## Performance Optimization

### Index Tuning for Your Dataset

Start with recommended defaults and adjust based on your recall requirements:

```typescript
// Step 1: Create index with balanced defaults
const index = new HnswIndex({
  M: 16,
  efConstruction: 200,
  efSearch: 100,
  distanceMetric: DistanceMetric.Cosine,
});

// Step 2: Build index with your data
for (const [id, vector] of yourData) {
  index.insert(id, vector);
}

// Step 3: Evaluate recall with test queries
function evaluateRecall(testQueries: TestQuery[]): number {
  let correct = 0;
  let total = 0;

  for (const { query, expectedIds } of testQueries) {
    const results = index.search(query, expectedIds.length);
    const foundIds = new Set(results.map(r => r.rowId));

    for (const expected of expectedIds) {
      if (foundIds.has(expected)) correct++;
      total++;
    }
  }

  return correct / total;
}

// Step 4: Adjust efSearch based on recall requirements
// Lower efSearch = faster but less accurate
// Higher efSearch = slower but more accurate
const recall = evaluateRecall(testQueries);
console.log(`Recall@10: ${(recall * 100).toFixed(1)}%`);
```

### Quantization for Memory Efficiency

Quantization reduces vector storage by 4-8x with minimal accuracy loss:

```typescript
import { VectorColumn, VectorType } from '@dotdo/dosql/vector';

// Enable quantization to reduce memory usage
const column = new VectorColumn({
  columnDef: {
    name: 'embedding',
    dimensions: 1536,
    type: VectorType.F32,  // Input type
    distanceMetric: DistanceMetric.Cosine,
  },
  quantization: {
    targetType: VectorType.I8,  // 4x memory reduction
  },
});

// Memory comparison
// 100K vectors at 1536 dimensions:
// - F32: 100,000 * 1536 * 4 bytes = 614 MB
// - I8:  100,000 * 1536 * 1 byte  = 154 MB

// Rebuild quantization codebook after bulk inserts
await column.batchSet(entries);
column.recomputeQuantization();
```

**Memory estimates:**

| Vectors | Dimensions | F32 Memory | I8 Memory |
|---------|------------|------------|-----------|
| 10K | 1536 | 58 MB | 15 MB |
| 100K | 1536 | 580 MB | 150 MB |
| 1M | 1536 | 5.8 GB | 1.5 GB |
| 10K | 384 | 15 MB | 4 MB |
| 100K | 384 | 150 MB | 38 MB |

### Query Optimization Strategies

**1. Adjust efSearch dynamically based on requirements:**

```typescript
// Fast queries for autocomplete (lower accuracy OK)
const fastResults = column.search(query, 10, 50);

// Standard queries for search results
const goodResults = column.search(query, 10, 100);

// High-quality queries for recommendations
const bestResults = column.search(query, 10, 300);
```

**2. Pre-filter to reduce search space:**

```typescript
// Filter first, then vector search only on filtered IDs
const filtered = hybridSearch(column, query, {
  filterIds: categoryIds,  // Only search within category
  k: 10,
});
```

**3. Choose the right distance metric:**

```typescript
// Cosine: Best for text (handles variable-length documents)
// L2: Best for images (often pre-normalized)
// Dot: Fastest when vectors are already normalized
```

**4. Consider dimension reduction for large embeddings:**

```typescript
// Many 1536-dim models work well truncated to 512-768 dims
// Test with your data before applying in production
const reducedEmbedding = embedding.slice(0, 768);
```

### Batch Operations

Bulk inserts are significantly faster than individual inserts:

```typescript
// Collect entries for batch insert
const entries: Array<[bigint, Float32Array]> = documents.map((doc, i) => [
  BigInt(i + 1),
  embeddings[i],
]);

// Batch insert (index rebuilds automatically)
column.batchSet(entries);

// For very large datasets, insert in chunks to manage memory
const CHUNK_SIZE = 10000;
for (let i = 0; i < entries.length; i += CHUNK_SIZE) {
  const chunk = entries.slice(i, i + CHUNK_SIZE);
  column.batchSet(chunk);
  console.log(`Inserted ${i + chunk.length}/${entries.length}`);
}
```

---

## Practical Examples

### Example: Semantic Search

A complete semantic search implementation for a documentation site.

**Schema:**

```sql
-- .do/migrations/001_create_docs.sql
CREATE TABLE docs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  slug TEXT UNIQUE NOT NULL,
  title TEXT NOT NULL,
  content TEXT NOT NULL,
  embedding BLOB NOT NULL,
  section TEXT,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_docs_section ON docs(section);
CREATE INDEX idx_docs_embedding ON docs
USING VECTOR (embedding)
WITH (dimensions = 1536, metric = 'cosine', m = 16);
```

**Ingestion:**

```typescript
import { DB } from '@dotdo/dosql';
import OpenAI from 'openai';

const openai = new OpenAI();
const db = await DB('docs');

interface Doc {
  slug: string;
  title: string;
  content: string;
  section: string;
}

async function indexDocument(doc: Doc): Promise<void> {
  // Create embedding from title and content
  const text = `${doc.title}\n\n${doc.content}`;
  const response = await openai.embeddings.create({
    model: 'text-embedding-3-small',
    input: text,
  });
  const embedding = new Float32Array(response.data[0].embedding);

  // Upsert document
  await db.run(`
    INSERT INTO docs (slug, title, content, section, embedding)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(slug) DO UPDATE SET
      title = excluded.title,
      content = excluded.content,
      section = excluded.section,
      embedding = excluded.embedding,
      updated_at = CURRENT_TIMESTAMP
  `, [doc.slug, doc.title, doc.content, doc.section, new Uint8Array(embedding.buffer)]);
}
```

**Search API:**

```typescript
interface SearchResult {
  slug: string;
  title: string;
  content: string;
  section: string;
  score: number;
  snippet: string;
}

async function searchDocs(query: string, options?: {
  section?: string;
  limit?: number;
}): Promise<SearchResult[]> {
  const { section, limit = 10 } = options ?? {};

  // Generate query embedding
  const response = await openai.embeddings.create({
    model: 'text-embedding-3-small',
    input: query,
  });
  const queryEmbedding = new Float32Array(response.data[0].embedding);

  // Build query with optional section filter
  let sql = `
    SELECT slug, title, content, section,
           1 - vector_distance_cos(embedding, ?) as score
    FROM docs
  `;
  const params: unknown[] = [new Uint8Array(queryEmbedding.buffer)];

  if (section) {
    sql += ' WHERE section = ?';
    params.push(section);
  }

  sql += ' ORDER BY score DESC LIMIT ?';
  params.push(limit);

  const results = await db.query(sql, params);

  // Generate snippets highlighting relevant content
  return results.map(r => ({
    ...r,
    snippet: generateSnippet(r.content, query),
  }));
}

function generateSnippet(content: string, query: string): string {
  const words = query.toLowerCase().split(/\s+/);
  const sentences = content.split(/[.!?]+/);

  for (const sentence of sentences) {
    if (words.some(w => sentence.toLowerCase().includes(w))) {
      return sentence.trim().slice(0, 200) + '...';
    }
  }

  return content.slice(0, 200) + '...';
}
```

### Example: Recommendation System

A product recommendation system combining content-based and collaborative filtering.

**Schema:**

```sql
-- .do/migrations/001_create_products.sql
CREATE TABLE products (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sku TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  description TEXT,
  category TEXT NOT NULL,
  price REAL NOT NULL,
  content_embedding BLOB NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_content ON products
USING VECTOR (content_embedding)
WITH (dimensions = 1536, metric = 'cosine', m = 16);

-- User interactions for collaborative filtering
CREATE TABLE interactions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  interaction_type TEXT NOT NULL, -- 'view', 'cart', 'purchase'
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (product_id) REFERENCES products(id)
);

CREATE INDEX idx_interactions_user ON interactions(user_id);
```

**Content-based recommendations:**

```typescript
async function getSimilarProducts(
  productId: number,
  limit = 10
): Promise<Product[]> {
  const product = await db.queryOne(
    'SELECT content_embedding FROM products WHERE id = ?',
    [productId]
  );

  if (!product) throw new Error('Product not found');

  return db.query(`
    SELECT id, sku, name, description, category, price,
           1 - vector_distance_cos(content_embedding, ?) as similarity
    FROM products
    WHERE id != ?
    ORDER BY similarity DESC
    LIMIT ?
  `, [product.content_embedding, productId, limit]);
}
```

**Personalized recommendations based on user history:**

```typescript
async function getPersonalizedRecommendations(
  userId: number,
  limit = 10
): Promise<Product[]> {
  // Get user's recent interactions
  const interactions = await db.query(`
    SELECT p.content_embedding, i.interaction_type
    FROM interactions i
    JOIN products p ON p.id = i.product_id
    WHERE i.user_id = ?
    ORDER BY i.created_at DESC
    LIMIT 20
  `, [userId]);

  if (interactions.length === 0) {
    return getPopularProducts(limit);
  }

  // Weight embeddings by interaction type
  const weights: Record<string, number> = {
    'purchase': 3.0,
    'cart': 2.0,
    'view': 1.0,
  };

  // Compute weighted average embedding as user profile
  const dims = 1536;
  const userProfile = new Float32Array(dims);
  let totalWeight = 0;

  for (const interaction of interactions) {
    const embedding = new Float32Array(
      interaction.content_embedding.buffer,
      interaction.content_embedding.byteOffset,
      dims
    );
    const weight = weights[interaction.interaction_type] || 1.0;

    for (let i = 0; i < dims; i++) {
      userProfile[i] += embedding[i] * weight;
    }
    totalWeight += weight;
  }

  // Normalize
  for (let i = 0; i < dims; i++) {
    userProfile[i] /= totalWeight;
  }

  // Exclude recently viewed products
  const recentIds = await db.query(
    'SELECT product_id FROM interactions WHERE user_id = ? ORDER BY created_at DESC LIMIT 50',
    [userId]
  );
  const excludeIds = recentIds.map(r => r.product_id);

  return db.query(`
    SELECT id, sku, name, description, category, price,
           1 - vector_distance_cos(content_embedding, ?) as score
    FROM products
    WHERE id NOT IN (${excludeIds.map(() => '?').join(',')})
    ORDER BY score DESC
    LIMIT ?
  `, [new Uint8Array(userProfile.buffer), ...excludeIds, limit]);
}
```

### Example: RAG Pipeline

A complete RAG (Retrieval Augmented Generation) implementation for Q&A.

**Schema:**

```sql
-- .do/migrations/001_create_knowledge_base.sql
CREATE TABLE documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  title TEXT,
  url TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE chunks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  document_id INTEGER NOT NULL,
  chunk_index INTEGER NOT NULL,
  content TEXT NOT NULL,
  embedding BLOB NOT NULL,
  token_count INTEGER,
  FOREIGN KEY (document_id) REFERENCES documents(id)
);

CREATE INDEX idx_chunks_document ON chunks(document_id);
CREATE INDEX idx_chunks_embedding ON chunks
USING VECTOR (embedding)
WITH (dimensions = 1536, metric = 'cosine', m = 16, ef_construction = 200);
```

**Document chunking and ingestion:**

```typescript
import OpenAI from 'openai';

const openai = new OpenAI();

function chunkDocument(content: string, options?: {
  chunkSize?: number;
  overlap?: number;
}): string[] {
  const { chunkSize = 500, overlap = 50 } = options ?? {};
  const chunks: string[] = [];

  // Split by paragraphs first
  const paragraphs = content.split(/\n\n+/);
  let currentChunk = '';

  for (const para of paragraphs) {
    if (currentChunk.length + para.length > chunkSize && currentChunk.length > 0) {
      chunks.push(currentChunk.trim());
      // Keep overlap from end of previous chunk
      const words = currentChunk.split(/\s+/);
      currentChunk = words.slice(-overlap).join(' ') + ' ' + para;
    } else {
      currentChunk += (currentChunk ? '\n\n' : '') + para;
    }
  }

  if (currentChunk.trim()) {
    chunks.push(currentChunk.trim());
  }

  return chunks;
}

async function ingestDocument(doc: {
  source: string;
  title: string;
  content: string;
  url?: string;
}): Promise<void> {
  // Insert document record
  const result = await db.run(
    'INSERT INTO documents (source, title, url) VALUES (?, ?, ?)',
    [doc.source, doc.title, doc.url]
  );
  const documentId = result.lastInsertRowId;

  // Chunk the content
  const chunks = chunkDocument(doc.content);

  // Batch embed chunks
  const response = await openai.embeddings.create({
    model: 'text-embedding-3-small',
    input: chunks,
  });

  // Insert chunks with embeddings
  await db.transaction(async (tx) => {
    for (let i = 0; i < chunks.length; i++) {
      const embedding = new Float32Array(response.data[i].embedding);
      await tx.run(
        'INSERT INTO chunks (document_id, chunk_index, content, embedding) VALUES (?, ?, ?, ?)',
        [documentId, i, chunks[i], new Uint8Array(embedding.buffer)]
      );
    }
  });
}
```

**Retrieval:**

```typescript
interface RetrievedChunk {
  id: number;
  content: string;
  documentTitle: string;
  documentUrl: string;
  score: number;
}

async function retrieveContext(
  query: string,
  options?: { topK?: number; minScore?: number }
): Promise<RetrievedChunk[]> {
  const { topK = 5, minScore = 0.7 } = options ?? {};

  // Generate query embedding
  const response = await openai.embeddings.create({
    model: 'text-embedding-3-small',
    input: query,
  });
  const queryEmbedding = new Float32Array(response.data[0].embedding);

  // Retrieve relevant chunks
  const chunks = await db.query(`
    SELECT c.id, c.content, d.title as document_title, d.url as document_url,
           1 - vector_distance_cos(c.embedding, ?) as score
    FROM chunks c
    JOIN documents d ON d.id = c.document_id
    ORDER BY score DESC
    LIMIT ?
  `, [new Uint8Array(queryEmbedding.buffer), topK * 2]);  // Over-fetch for filtering

  // Filter by minimum score
  return chunks
    .filter(c => c.score >= minScore)
    .slice(0, topK)
    .map(c => ({
      id: c.id,
      content: c.content,
      documentTitle: c.document_title,
      documentUrl: c.document_url,
      score: c.score,
    }));
}
```

**Generation with context:**

```typescript
interface RAGResponse {
  answer: string;
  sources: Array<{ title: string; url: string; relevance: number }>;
}

async function answerQuestion(question: string): Promise<RAGResponse> {
  // Retrieve relevant context
  const context = await retrieveContext(question, { topK: 5, minScore: 0.7 });

  if (context.length === 0) {
    return {
      answer: "I don't have enough information to answer that question.",
      sources: [],
    };
  }

  // Build context string with citations
  const contextText = context
    .map((c, i) => `[${i + 1}] ${c.content}`)
    .join('\n\n');

  // Generate answer with GPT-4
  const completion = await openai.chat.completions.create({
    model: 'gpt-4-turbo-preview',
    messages: [
      {
        role: 'system',
        content: `You are a helpful assistant that answers questions based on the provided context.
Use only the information from the context to answer. If the context doesn't contain
enough information, say so. Cite sources using [1], [2], etc.`,
      },
      {
        role: 'user',
        content: `Context:\n${contextText}\n\nQuestion: ${question}`,
      },
    ],
    temperature: 0.1,
    max_tokens: 500,
  });

  return {
    answer: completion.choices[0].message.content || '',
    sources: context.map(c => ({
      title: c.documentTitle,
      url: c.documentUrl,
      relevance: c.score,
    })),
  };
}
```

**Advanced: Hypothetical Document Embedding (HyDE):**

HyDE improves retrieval by generating a hypothetical answer first, then using that to search:

```typescript
async function hydeRetrieve(question: string): Promise<RetrievedChunk[]> {
  // Generate a hypothetical answer
  const completion = await openai.chat.completions.create({
    model: 'gpt-3.5-turbo',
    messages: [
      {
        role: 'system',
        content: 'Write a short paragraph that would answer the following question. ' +
                 'Write as if you are writing documentation.',
      },
      { role: 'user', content: question },
    ],
    temperature: 0.7,
    max_tokens: 200,
  });

  const hypotheticalAnswer = completion.choices[0].message.content || question;

  // Use the hypothetical answer for retrieval (better matches document style)
  return retrieveContext(hypotheticalAnswer, { topK: 5 });
}
```

---

## Troubleshooting

### Common Issues

**"Vector dimensions mismatch"**

The query vector dimensions must match the indexed vector dimensions.

```typescript
// Wrong: Using wrong embedding model
const wrongEmbedding = await getEmbedding768Dim(query);  // 768 dimensions
// Searching index with 1536 dimensions - ERROR

// Right: Use same model for query and index
const correctEmbedding = await getEmbedding1536Dim(query);  // 1536 dimensions
```

**"Index not found"**

The vector index may not exist or not be loaded.

```typescript
// Check if index exists
const indexes = await db.query(`
  SELECT name FROM sqlite_master
  WHERE type = 'index' AND sql LIKE '%USING VECTOR%'
`);

// Create index if missing
if (!indexes.find(i => i.name === 'idx_docs_embedding')) {
  await db.exec(`
    CREATE INDEX idx_docs_embedding ON docs
    USING VECTOR (embedding)
    WITH (dimensions = 1536, metric = 'cosine')
  `);
}
```

**Slow query performance**

- Check that the vector index exists and is being used
- Reduce `efSearch` for faster (but less accurate) queries
- Use pre-filtering to reduce the search space
- Consider quantization for memory-bound workloads

**Out of memory**

- Enable quantization (I8 reduces memory by 4x)
- Use smaller dimensions if model supports it
- Shard data across multiple Durable Objects
- Reduce `M` parameter (fewer connections = less memory)

---

## Next Steps

- [API Reference](./api-reference.md) - Complete function and type reference
- [Advanced Features](./advanced.md) - Time travel, branching, CDC streaming
- [Architecture](./architecture.md) - Understanding DoSQL internals
- [Getting Started](./getting-started.md) - Basic usage and setup
