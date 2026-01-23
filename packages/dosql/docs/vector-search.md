# Vector Search Guide

Build semantic search, recommendations, and RAG pipelines with DoSQL's native vector search capabilities. This guide covers everything you need to integrate AI/ML-powered features into your Cloudflare Workers applications.

## Why Vector Search?

Traditional keyword search fails when users express concepts differently. A search for "affordable laptops" will miss documents containing "budget notebooks" or "cheap portable computers" unless you manually create synonym mappings.

Vector search solves this by converting text, images, and other data into high-dimensional numerical representations (embeddings) that capture semantic meaning. Similar concepts cluster together in vector space, enabling:

- **Semantic understanding**: Find related content regardless of exact wording
- **Fuzzy matching**: Surface relevant results even with typos or paraphrasing
- **Cross-language search**: Match concepts across languages with multilingual models
- **Multimodal search**: Find images matching text descriptions

**DoSQL provides:**

| Feature | Description |
|---------|-------------|
| Native vector storage | Store vectors as BLOB columns with automatic serialization |
| HNSW indexing | Sub-millisecond approximate nearest neighbor search |
| Multiple distance metrics | Cosine, Euclidean (L2), dot product |
| Hybrid queries | Combine vector similarity with SQL filters |
| Quantization | 4x memory reduction with minimal accuracy loss |
| Edge-native | Low-latency queries running in Cloudflare Workers |

---

## Table of Contents

- [Core Concepts](#core-concepts)
  - [What Are Embeddings?](#what-are-embeddings)
  - [Distance Metrics](#distance-metrics)
  - [HNSW Algorithm](#hnsw-algorithm)
- [Creating Vector Indexes](#creating-vector-indexes)
  - [Schema Design](#schema-design)
  - [SQL Index Creation](#sql-index-creation)
  - [TypeScript Index API](#typescript-index-api)
  - [Parameter Tuning](#parameter-tuning)
- [Querying Vectors](#querying-vectors)
  - [Basic Similarity Search](#basic-similarity-search)
  - [Hybrid Search](#hybrid-search-vector--filters)
  - [Pagination and Thresholds](#pagination-and-thresholds)
- [Embedding Integration](#embedding-integration)
  - [OpenAI Embeddings](#openai-embeddings)
  - [Cloudflare Workers AI](#cloudflare-workers-ai)
  - [Batch Processing](#batch-processing)
  - [Model Selection](#choosing-an-embedding-model)
- [Performance Optimization](#performance-optimization)
  - [Index Tuning](#index-tuning-for-your-dataset)
  - [Quantization](#quantization-for-memory-efficiency)
  - [Query Strategies](#query-optimization-strategies)
- [Real-World Examples](#practical-examples)
  - [Semantic Search](#example-semantic-search)
  - [Recommendation System](#example-recommendation-system)
  - [RAG Pipeline](#example-rag-pipeline)
- [Troubleshooting](#troubleshooting)

---

## Core Concepts

### What Are Embeddings?

An **embedding** is a numerical representation of data (text, images, audio) as a fixed-length array of floating-point numbers. Embedding models are trained so that semantically similar inputs produce vectors that are close together in the embedding space.

```typescript
// A text embedding from OpenAI (1536 dimensions)
const embedding = new Float32Array([
  0.0231, -0.0142, 0.0089, ..., 0.0156  // 1536 values
]);

// Similar texts produce similar vectors
const query1 = await getEmbedding("machine learning tutorial");
const query2 = await getEmbedding("ML learning guide");        // Close to query1
const query3 = await getEmbedding("chocolate cake recipe");   // Far from query1
```

**Dimensionality** is the number of elements in the vector. Higher dimensions capture more nuance but require more storage:

| Model | Dimensions | Storage per Vector |
|-------|------------|-------------------|
| all-MiniLM-L6-v2 | 384 | 1.5 KB |
| BGE-base-en-v1.5 | 768 | 3 KB |
| OpenAI text-embedding-3-small | 1536 | 6 KB |
| OpenAI text-embedding-3-large | 3072 | 12 KB |

### Distance Metrics

Distance metrics measure how far apart two vectors are. DoSQL supports three metrics:

**Cosine Distance** (recommended for text)

Measures the angle between vectors, ignoring magnitude. Best for text embeddings where document length varies.

```sql
-- Range: [0, 2] where 0 = identical direction
SELECT id, title,
       vector_distance_cos(embedding, :query_vector) as distance
FROM documents
ORDER BY distance ASC
LIMIT 10;
```

**Euclidean Distance (L2)**

Measures straight-line distance in vector space. Sensitive to vector magnitude.

```sql
-- Range: [0, infinity) where 0 = identical
SELECT id, title,
       vector_distance_l2(embedding, :query_vector) as distance
FROM documents
ORDER BY distance ASC
LIMIT 10;
```

**Dot Product (Inner Product)**

For normalized vectors, negative dot product serves as a distance metric. Fastest to compute.

```sql
-- More negative = more similar (for normalized vectors)
SELECT id, title,
       vector_distance_dot(embedding, :query_vector) as distance
FROM documents
ORDER BY distance ASC
LIMIT 10;
```

**Note:** For dot product, ensure your vectors are normalized (unit length). If not, normalize them before storage:

```typescript
function normalize(vec: Float32Array): Float32Array {
  const norm = Math.sqrt(vec.reduce((sum, v) => sum + v * v, 0));
  return vec.map(v => v / norm);
}
```

### HNSW Algorithm

HNSW (Hierarchical Navigable Small World) is a graph-based algorithm for approximate nearest neighbor search. It provides excellent query performance with tunable precision.

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

## Creating Vector Indexes

### Schema Design

Store vectors as BLOB columns alongside the source data they represent:

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

-- Vector index for similarity search
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

**Schema design best practices:**

1. **Keep source data** - Store original text alongside embeddings for retrieval
2. **Add filter columns** - Include columns you will filter on (category, date, user_id)
3. **Chunk long documents** - Split into segments with separate embeddings
4. **Track model version** - Store embedding model info for future migrations

### SQL Index Creation

Create an HNSW index on a vector column:

```sql
CREATE INDEX idx_products_embedding ON products
USING VECTOR (embedding)
WITH (
  dimensions = 1536,       -- Must match your embedding model
  metric = 'cosine',       -- or 'l2', 'dot'
  m = 16,                  -- Connections per node (8-64)
  ef_construction = 200    -- Build quality (64-512)
);
```

### TypeScript Index API

For programmatic control over index creation:

```typescript
import { HnswIndex, DistanceMetric } from '@dotdo/dosql/vector';

// Create index with configuration
const index = new HnswIndex({
  M: 16,                           // Connections per node
  efConstruction: 200,             // Build-time quality
  efSearch: 100,                   // Default query-time quality
  distanceMetric: DistanceMetric.Cosine,
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
  console.log(`Row ${result.rowId}: distance=${result.distance}`);
}

// Get index statistics
const stats = index.getStats();
console.log({
  nodeCount: stats.nodeCount,
  dimensions: stats.dimensions,
  maxLevel: stats.maxLevel,
  memoryUsageBytes: stats.memoryUsageBytes,
});
```

### Parameter Tuning

HNSW parameters control the trade-off between recall (accuracy), speed, and memory:

| Goal | Adjust | Impact |
|------|--------|--------|
| Better recall | Increase `M`, `efConstruction`, `efSearch` | More memory, slower |
| Faster queries | Decrease `efSearch` | Lower recall |
| Faster builds | Decrease `efConstruction` | Lower quality index |
| Less memory | Decrease `M` | Lower recall |

**Recommended settings by dataset size:**

| Dataset Size | M | efConstruction | efSearch |
|--------------|---|----------------|----------|
| < 10K | 8 | 100 | 50 |
| 10K - 100K | 16 | 200 | 100 |
| 100K - 1M | 24 | 300 | 150 |
| > 1M | 32 | 400 | 200 |

```typescript
// High recall configuration
const highRecallIndex = new HnswIndex({
  M: 32,
  efConstruction: 400,
  efSearch: 300,
  distanceMetric: DistanceMetric.Cosine,
});

// Fast query configuration
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

Find the k most similar vectors to a query:

```sql
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

Combine semantic similarity with traditional SQL filters:

```sql
-- Filter by category and date
SELECT id, title, content,
       vector_distance_cos(embedding, :query_embedding) as distance
FROM documents
WHERE category = 'technology'
  AND published_at > '2024-01-01'
ORDER BY distance ASC
LIMIT 10;
```

```sql
-- Multi-table join with filters
SELECT d.id, d.title, d.content, u.name as author,
       vector_distance_cos(d.embedding, :query) as distance
FROM documents d
JOIN users u ON d.author_id = u.id
WHERE d.status = 'published'
  AND d.language = 'en'
ORDER BY distance ASC
LIMIT 10;
```

**Pre-filter pattern for large datasets:**

```typescript
import { hybridSearch } from '@dotdo/dosql/vector';

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

**Weighted hybrid search (keyword + semantic):**

```sql
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

1. **Quality**: For production RAG, use OpenAI or Cohere
2. **Cost**: Use Workers AI (free) or open-source models for budget-conscious projects
3. **Latency**: Smaller dimensions = faster queries
4. **Language support**: Cohere excels at multilingual content
5. **Privacy**: Self-hosted models avoid sending data externally

---

## Performance Optimization

### Index Tuning for Your Dataset

Start with defaults and adjust based on recall requirements:

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

const recall = evaluateRecall(testQueries);
console.log(`Recall@10: ${(recall * 100).toFixed(1)}%`);

// Step 4: Adjust efSearch until recall meets requirements
```

### Quantization for Memory Efficiency

Quantization reduces vector storage by 4x with minimal accuracy loss:

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
```

**Memory comparison:**

| Vectors | Dimensions | F32 Memory | I8 Memory |
|---------|------------|------------|-----------|
| 10K | 1536 | 58 MB | 15 MB |
| 100K | 1536 | 580 MB | 150 MB |
| 1M | 1536 | 5.8 GB | 1.5 GB |

### Query Optimization Strategies

**1. Adjust efSearch dynamically:**

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
const filtered = hybridSearch(column, query, {
  filterIds: categoryIds,
  k: 10,
});
```

**3. Choose the right distance metric:**

- **Cosine**: Best for text (handles variable-length documents)
- **L2**: Best for images (often pre-normalized)
- **Dot**: Fastest when vectors are already normalized

**4. Batch operations for bulk inserts:**

```typescript
const entries: Array<[bigint, Float32Array]> = documents.map((doc, i) => [
  BigInt(i + 1),
  embeddings[i],
]);

// Batch insert (much faster than individual inserts)
column.batchSet(entries);
```

---

## Practical Examples

### Example: Semantic Search

A complete semantic search implementation for documentation.

**Schema:**

```sql
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

**Search API:**

```typescript
interface SearchResult {
  slug: string;
  title: string;
  content: string;
  section: string;
  score: number;
}

async function searchDocs(query: string, options?: {
  section?: string;
  limit?: number;
}): Promise<SearchResult[]> {
  const { section, limit = 10 } = options ?? {};

  const response = await openai.embeddings.create({
    model: 'text-embedding-3-small',
    input: query,
  });
  const queryEmbedding = new Float32Array(response.data[0].embedding);

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

  return db.query(sql, params);
}
```

### Example: Recommendation System

Product recommendations combining content-based filtering.

**Schema:**

```sql
CREATE TABLE products (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sku TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  description TEXT,
  category TEXT NOT NULL,
  price REAL NOT NULL,
  content_embedding BLOB NOT NULL
);

CREATE INDEX idx_products_category ON products(category);
CREATE INDEX idx_products_content ON products
USING VECTOR (content_embedding)
WITH (dimensions = 1536, metric = 'cosine', m = 16);

CREATE TABLE interactions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  product_id INTEGER NOT NULL,
  interaction_type TEXT NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
```

**Content-based recommendations:**

```typescript
async function getSimilarProducts(productId: number, limit = 10) {
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

**Personalized recommendations:**

```typescript
async function getPersonalizedRecommendations(userId: number, limit = 10) {
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
  const weights = { 'purchase': 3.0, 'cart': 2.0, 'view': 1.0 };
  const dims = 1536;
  const userProfile = new Float32Array(dims);
  let totalWeight = 0;

  for (const interaction of interactions) {
    const embedding = new Float32Array(interaction.content_embedding.buffer);
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

  return db.query(`
    SELECT id, sku, name, description, category, price,
           1 - vector_distance_cos(content_embedding, ?) as score
    FROM products
    ORDER BY score DESC
    LIMIT ?
  `, [new Uint8Array(userProfile.buffer), limit]);
}
```

### Example: RAG Pipeline

A complete RAG (Retrieval Augmented Generation) implementation.

**Schema:**

```sql
CREATE TABLE documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  title TEXT,
  url TEXT
);

CREATE TABLE chunks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  document_id INTEGER NOT NULL,
  chunk_index INTEGER NOT NULL,
  content TEXT NOT NULL,
  embedding BLOB NOT NULL,
  FOREIGN KEY (document_id) REFERENCES documents(id)
);

CREATE INDEX idx_chunks_embedding ON chunks
USING VECTOR (embedding)
WITH (dimensions = 1536, metric = 'cosine', m = 16, ef_construction = 200);
```

**Document chunking:**

```typescript
function chunkDocument(content: string, chunkSize = 500, overlap = 50): string[] {
  const chunks: string[] = [];
  const paragraphs = content.split(/\n\n+/);
  let currentChunk = '';

  for (const para of paragraphs) {
    if (currentChunk.length + para.length > chunkSize && currentChunk.length > 0) {
      chunks.push(currentChunk.trim());
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
```

**Retrieval:**

```typescript
async function retrieveContext(query: string, topK = 5, minScore = 0.7) {
  const response = await openai.embeddings.create({
    model: 'text-embedding-3-small',
    input: query,
  });
  const queryEmbedding = new Float32Array(response.data[0].embedding);

  const chunks = await db.query(`
    SELECT c.id, c.content, d.title, d.url,
           1 - vector_distance_cos(c.embedding, ?) as score
    FROM chunks c
    JOIN documents d ON d.id = c.document_id
    ORDER BY score DESC
    LIMIT ?
  `, [new Uint8Array(queryEmbedding.buffer), topK * 2]);

  return chunks.filter(c => c.score >= minScore).slice(0, topK);
}
```

**Generation:**

```typescript
async function answerQuestion(question: string) {
  const context = await retrieveContext(question);

  if (context.length === 0) {
    return {
      answer: "I don't have enough information to answer that question.",
      sources: [],
    };
  }

  const contextText = context
    .map((c, i) => `[${i + 1}] ${c.content}`)
    .join('\n\n');

  const completion = await openai.chat.completions.create({
    model: 'gpt-4-turbo-preview',
    messages: [
      {
        role: 'system',
        content: `Answer questions based on the provided context. Cite sources using [1], [2], etc.`,
      },
      {
        role: 'user',
        content: `Context:\n${contextText}\n\nQuestion: ${question}`,
      },
    ],
    temperature: 0.1,
  });

  return {
    answer: completion.choices[0].message.content,
    sources: context.map(c => ({ title: c.title, url: c.url, score: c.score })),
  };
}
```

---

## Troubleshooting

### "Vector dimensions mismatch"

The query vector dimensions must match the indexed vector dimensions.

```typescript
// Wrong: Using wrong embedding model
const wrong = await get768DimEmbedding(query);  // 768 dims
// Searching 1536-dim index - ERROR

// Right: Use same model for queries and indexing
const correct = await get1536DimEmbedding(query);  // 1536 dims
```

### Slow query performance

1. Verify the vector index exists and is being used
2. Reduce `efSearch` for faster queries
3. Use pre-filtering to reduce search space
4. Consider quantization for memory-bound workloads

### Out of memory

1. Enable I8 quantization (4x memory reduction)
2. Use smaller dimensions if model supports it
3. Shard data across multiple Durable Objects
4. Reduce `M` parameter (fewer connections = less memory)

### Low recall (missing relevant results)

1. Increase `efSearch` for better query accuracy
2. Rebuild index with higher `efConstruction`
3. Verify your query embedding uses the same model as indexed embeddings
4. Check if distance threshold is too strict

---

## Next Steps

- [API Reference](./api-reference.md) - Complete function and type reference
- [Advanced Features](./advanced.md) - Time travel, branching, CDC streaming
- [Architecture](./architecture.md) - Understanding DoSQL internals
- [Getting Started](./getting-started.md) - Basic setup and usage
