# Vector Search Guide

DoSQL provides native vector search capabilities for AI/ML applications, enabling semantic search, recommendations, and RAG (Retrieval Augmented Generation) patterns directly within your SQL database.

## Table of Contents

- [Vector Search Overview](#vector-search-overview)
- [Creating Vector Columns](#creating-vector-columns)
- [Indexing Vectors (HNSW)](#indexing-vectors-hnsw)
- [Similarity Functions](#similarity-functions)
- [Querying Vectors with SQL](#querying-vectors-with-sql)
- [Hybrid Search (Vector + Filters)](#hybrid-search-vector--filters)
- [Embedding Generation Integration](#embedding-generation-integration)
- [Performance Tuning](#performance-tuning)
- [Example: Semantic Search](#example-semantic-search)
- [Example: Recommendation System](#example-recommendation-system)
- [Example: RAG (Retrieval Augmented Generation)](#example-rag-retrieval-augmented-generation)

---

## Vector Search Overview

Vector search enables finding similar items by comparing high-dimensional vector representations (embeddings) of data. DoSQL supports:

- **Native vector storage** - Store vectors as BLOB columns with automatic serialization
- **HNSW indexing** - Approximate nearest neighbor search with configurable precision/speed tradeoffs
- **Multiple distance metrics** - Cosine, Euclidean (L2), dot product, and Hamming
- **Hybrid queries** - Combine vector similarity with traditional SQL filters
- **Quantization** - Compress vectors for memory efficiency

### Supported Vector Types

| Type | Bytes per Element | Use Case |
|------|-------------------|----------|
| `F64` | 8 | Highest precision |
| `F32` | 4 | Standard ML vectors (default) |
| `F16` | 2 | Reduced precision |
| `BF16` | 2 | Neural network optimized |
| `I8` | 1 | Quantized vectors |
| `F1BIT` | 0.125 | Binary embeddings |

### Common Embedding Dimensions

| Model | Dimensions | Notes |
|-------|------------|-------|
| OpenAI text-embedding-3-small | 1536 | Default OpenAI model |
| OpenAI text-embedding-3-large | 3072 | Higher quality |
| Cohere embed-v3 | 1024 | Multilingual |
| BGE-large | 1024 | Open source |
| all-MiniLM-L6-v2 | 384 | Lightweight |

---

## Creating Vector Columns

### Basic Vector Column

Vectors are stored as BLOB columns. DoSQL provides helper functions to serialize and deserialize vectors:

```sql
-- Create table with vector column
CREATE TABLE documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  content TEXT NOT NULL,
  embedding BLOB,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
```

### Vector Column with Constraints

```sql
-- Create table with NOT NULL vector column
CREATE TABLE products (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  description TEXT,
  embedding BLOB NOT NULL,
  category TEXT,
  price REAL
);
```

### TypeScript Vector Column Definition

```typescript
import { DB } from '@dotdo/dosql';
import { VectorColumn, VectorType, DistanceMetric } from '@dotdo/dosql/vector';

// Define a vector column with options
const embeddingColumn = new VectorColumn({
  columnDef: {
    name: 'embedding',
    dimensions: 1536,
    type: VectorType.F32,
    nullable: false,
    distanceMetric: DistanceMetric.Cosine,
  },
  enableIndex: true,
  hnswM: 16,
  hnswEfConstruction: 200,
  hnswEfSearch: 100,
});
```

---

## Indexing Vectors (HNSW)

HNSW (Hierarchical Navigable Small World) is a graph-based algorithm for approximate nearest neighbor search. It provides excellent query performance with tunable precision.

### Creating an HNSW Index

```sql
-- Create HNSW vector index
CREATE INDEX idx_documents_embedding ON documents
USING VECTOR (embedding)
WITH (
  dimensions = 1536,
  metric = 'cosine',
  m = 16,
  ef_construction = 200
);
```

### HNSW Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `dimensions` | Required | Number of dimensions in vectors |
| `metric` | `cosine` | Distance metric: `cosine`, `l2`, `dot`, `hamming` |
| `m` | 16 | Max connections per node (8-64 typical) |
| `ef_construction` | 200 | Build-time search width (64-512 typical) |
| `ef_search` | 100 | Query-time search width (50-500 typical) |

### TypeScript HNSW Index

```typescript
import { HnswIndex, DistanceMetric } from '@dotdo/dosql/vector';

// Create index with configuration
const index = new HnswIndex({
  M: 16,                           // Connections per node
  efConstruction: 200,             // Build-time quality
  efSearch: 100,                   // Query-time quality
  distanceMetric: DistanceMetric.Cosine,
  seed: 42,                        // For reproducible builds
});

// Insert vectors
index.insert(1n, new Float32Array([0.1, 0.2, 0.3, ...]));
index.insert(2n, new Float32Array([0.4, 0.5, 0.6, ...]));

// Search for nearest neighbors
const results = index.search(
  new Float32Array([0.15, 0.25, 0.35, ...]),
  10,    // k: number of results
  150,   // efSearch: search quality (optional)
);

// Results include rowId, distance, and similarity score
for (const result of results) {
  console.log(`Row ${result.rowId}: distance=${result.distance}, score=${result.score}`);
}
```

### Index Statistics

```typescript
const stats = index.getStats();
console.log({
  nodeCount: stats.nodeCount,
  dimensions: stats.dimensions,
  maxLevel: stats.maxLevel,
  avgConnections: stats.avgConnectionsLevel0,
});
```

### Tuning HNSW Parameters

| Goal | Adjust | Trade-off |
|------|--------|-----------|
| Better recall | Increase `M`, `efConstruction`, `efSearch` | More memory, slower build/query |
| Faster queries | Decrease `efSearch` | Lower recall |
| Faster build | Decrease `efConstruction` | Lower quality index |
| Less memory | Decrease `M` | Lower recall |

**Recommended settings by dataset size:**

| Dataset Size | M | efConstruction | efSearch |
|--------------|---|----------------|----------|
| < 10K | 8 | 100 | 50 |
| 10K - 100K | 16 | 200 | 100 |
| 100K - 1M | 24 | 300 | 150 |
| > 1M | 32 | 400 | 200 |

---

## Similarity Functions

DoSQL provides SQL functions for computing vector distances and similarities.

### Cosine Distance

Measures the angle between vectors. Most common for text embeddings.

```sql
-- Cosine distance (0 = identical, 2 = opposite)
SELECT id, title,
       vector_distance_cos(embedding, :query_vector) as distance
FROM documents
ORDER BY distance ASC
LIMIT 10;
```

**Properties:**
- Range: [0, 2]
- 0 = identical direction
- 1 = orthogonal (90 degrees)
- 2 = opposite direction
- Normalized by magnitude (good for variable-length text)

### Euclidean Distance (L2)

Measures the straight-line distance between vectors.

```sql
-- Euclidean distance
SELECT id, title,
       vector_distance_l2(embedding, :query_vector) as distance
FROM documents
ORDER BY distance ASC
LIMIT 10;
```

**Properties:**
- Range: [0, infinity)
- 0 = identical vectors
- Sensitive to magnitude (better for normalized vectors)
- Better for image embeddings

### Dot Product

Measures similarity as the dot product (for normalized vectors, equivalent to cosine similarity).

```sql
-- Dot product distance (negative dot product, smaller = more similar)
SELECT id, title,
       vector_distance_dot(embedding, :query_vector) as distance
FROM documents
ORDER BY distance ASC
LIMIT 10;
```

**Properties:**
- Range: (-infinity, infinity)
- More negative = more similar (for normalized vectors)
- Fast computation
- Use with normalized vectors for best results

### Vector Operations

```sql
-- Create a vector from JSON array
SELECT vector('[0.1, 0.2, 0.3]') as v;

-- Extract vector as JSON array
SELECT vector_extract(embedding) FROM documents WHERE id = 1;

-- Get vector dimensions
SELECT vector_dims(embedding) FROM documents WHERE id = 1;

-- Get L2 norm (magnitude)
SELECT vector_norm(embedding) FROM documents WHERE id = 1;

-- Normalize vector to unit length
SELECT vector_normalize(embedding) FROM documents WHERE id = 1;

-- Vector arithmetic
SELECT vector_add(v1, v2) as sum,
       vector_sub(v1, v2) as diff,
       vector_scale(v1, 2.0) as scaled,
       vector_dot(v1, v2) as dot_product
FROM vector_pairs;
```

### Function Reference

| Function | Arguments | Returns | Description |
|----------|-----------|---------|-------------|
| `vector(input)` | JSON array or typed array | BLOB | Create vector |
| `vector_distance_cos(v1, v2)` | Two vectors | REAL | Cosine distance |
| `vector_distance_l2(v1, v2)` | Two vectors | REAL | Euclidean distance |
| `vector_distance_dot(v1, v2)` | Two vectors | REAL | Dot product distance |
| `vector_extract(v)` | Vector | TEXT | Convert to JSON array |
| `vector_dims(v)` | Vector | INTEGER | Number of dimensions |
| `vector_norm(v)` | Vector | REAL | L2 norm |
| `vector_normalize(v)` | Vector | BLOB | Unit vector |
| `vector_add(v1, v2)` | Two vectors | BLOB | Element-wise sum |
| `vector_sub(v1, v2)` | Two vectors | BLOB | Element-wise difference |
| `vector_scale(v, s)` | Vector, scalar | BLOB | Scaled vector |
| `vector_dot(v1, v2)` | Two vectors | REAL | Dot product |

---

## Querying Vectors with SQL

### Basic Similarity Search

```sql
-- Find 10 most similar documents
SELECT id, title, content,
       vector_distance_cos(embedding, :query_embedding) as distance
FROM documents
ORDER BY distance ASC
LIMIT 10;
```

### With TypeScript

```typescript
import { DB } from '@dotdo/dosql';

const db = await DB('semantic-search');

// Generate embedding for query
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

### With Distance Threshold

```sql
-- Find documents within distance threshold
SELECT id, title, content,
       vector_distance_cos(embedding, :query_embedding) as distance
FROM documents
WHERE vector_distance_cos(embedding, :query_embedding) < 0.5
ORDER BY distance ASC
LIMIT 100;
```

### Pagination

```sql
-- Paginated vector search
SELECT id, title,
       vector_distance_cos(embedding, :query_embedding) as distance
FROM documents
ORDER BY distance ASC
LIMIT 20 OFFSET 40;  -- Page 3, 20 items per page
```

---

## Hybrid Search (Vector + Filters)

Hybrid search combines semantic similarity with traditional SQL filters for more precise results.

### Filter Before Vector Search

```sql
-- Find similar documents in a specific category
SELECT id, title,
       vector_distance_cos(embedding, :query_embedding) as distance
FROM documents
WHERE category = 'technology'
  AND published_at > '2024-01-01'
ORDER BY distance ASC
LIMIT 10;
```

### Multi-Filter Hybrid Search

```sql
-- Complex filtering with vector search
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

### TypeScript Hybrid Search

```typescript
import { hybridSearch, VectorColumn } from '@dotdo/dosql/vector';

// Pre-filter row IDs from scalar query
const categoryIds = await db.query(
  'SELECT id FROM products WHERE category = ?',
  ['electronics']
);
const filterIds = new Set(categoryIds.map(r => BigInt(r.id)));

// Hybrid search with pre-filtering
const results = hybridSearch(vectorColumn, queryVector, {
  filterIds,
  k: 10,
  oversampleFactor: 2,  // Fetch 2x results, then filter
});
```

### Weighted Hybrid Search (BM25 + Vector)

```sql
-- Combine keyword relevance and semantic similarity
SELECT id, title, content,
  (0.4 * bm25_score(content, :keyword_query)) +
  (0.6 * (1 - vector_distance_cos(embedding, :vector_query))) as combined_score
FROM documents
WHERE content MATCH :keyword_query
ORDER BY combined_score DESC
LIMIT 10;
```

### Re-ranking Pattern

```typescript
// Step 1: Retrieve candidates with vector search (fast, approximate)
const candidates = await db.query(`
  SELECT id, title, content, embedding
  FROM documents
  ORDER BY vector_distance_cos(embedding, ?) ASC
  LIMIT 100
`, [queryEmbedding]);

// Step 2: Re-rank with exact scoring or LLM
const reranked = await rerank(query, candidates);

// Step 3: Return top results
return reranked.slice(0, 10);
```

---

## Embedding Generation Integration

### OpenAI Integration

```typescript
import { DB } from '@dotdo/dosql';
import OpenAI from 'openai';

const openai = new OpenAI();

async function getEmbedding(text: string): Promise<Float32Array> {
  const response = await openai.embeddings.create({
    model: 'text-embedding-3-small',
    input: text,
  });
  return new Float32Array(response.data[0].embedding);
}

// Insert document with embedding
async function insertDocument(title: string, content: string) {
  const embedding = await getEmbedding(`${title}\n\n${content}`);

  await db.run(
    'INSERT INTO documents (title, content, embedding) VALUES (?, ?, ?)',
    [title, content, new Uint8Array(embedding.buffer)]
  );
}
```

### Cloudflare Workers AI Integration

```typescript
import { DB } from '@dotdo/dosql';

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

    // Generate query embedding
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

### Batch Embedding

```typescript
async function batchEmbed(texts: string[]): Promise<Float32Array[]> {
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
```

---

## Performance Tuning

### Index Configuration

```typescript
// High recall, slower queries
const highRecallIndex = new HnswIndex({
  M: 32,
  efConstruction: 400,
  efSearch: 300,
  distanceMetric: DistanceMetric.Cosine,
});

// Fast queries, lower recall
const fastIndex = new HnswIndex({
  M: 12,
  efConstruction: 100,
  efSearch: 50,
  distanceMetric: DistanceMetric.Cosine,
});

// Balanced (recommended default)
const balancedIndex = new HnswIndex({
  M: 16,
  efConstruction: 200,
  efSearch: 100,
  distanceMetric: DistanceMetric.Cosine,
});
```

### Quantization for Memory Efficiency

```typescript
import { VectorColumn, VectorType } from '@dotdo/dosql/vector';

// Enable quantization to reduce memory usage
const column = new VectorColumn({
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

// Recompute quantization after bulk inserts
await column.batchSet(entries);
column.recomputeQuantization();
```

### Memory Estimates

| Vectors | Dimensions | F32 Memory | I8 Memory |
|---------|------------|------------|-----------|
| 10K | 1536 | 58 MB | 15 MB |
| 100K | 1536 | 580 MB | 150 MB |
| 1M | 1536 | 5.8 GB | 1.5 GB |
| 10K | 384 | 15 MB | 4 MB |
| 100K | 384 | 150 MB | 38 MB |

### Query Optimization

```typescript
// 1. Use efSearch parameter for speed/recall tradeoff
const fastResults = column.search(query, 10, 50);   // Fast, lower recall
const goodResults = column.search(query, 10, 100);  // Balanced
const bestResults = column.search(query, 10, 300);  // Slower, better recall

// 2. Pre-filter when possible to reduce search space
const filtered = hybridSearch(column, query, {
  filterIds: categoryIds,  // Search only within category
  k: 10,
});

// 3. Use appropriate distance metric
// - Cosine: text embeddings (direction matters, not magnitude)
// - L2: image embeddings (often pre-normalized)
// - Dot: when vectors are already normalized

// 4. Consider dimension reduction for large embeddings
// Many 1536-dim models work well truncated to 512-768 dims
```

### Batch Operations

```typescript
// Bulk insert is faster than individual inserts
const entries: Array<[bigint, Float32Array]> = documents.map((doc, i) => [
  BigInt(i + 1),
  embeddings[i],
]);

column.batchSet(entries);  // Automatically rebuilds index

// For very large datasets, insert in chunks
const CHUNK_SIZE = 10000;
for (let i = 0; i < entries.length; i += CHUNK_SIZE) {
  const chunk = entries.slice(i, i + CHUNK_SIZE);
  column.batchSet(chunk);
}
```

---

## Example: Semantic Search

A complete semantic search implementation for a documentation site.

### Schema

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

### Ingestion

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

async function indexAllDocs(docs: Doc[]): Promise<void> {
  // Batch embed for efficiency
  const texts = docs.map(d => `${d.title}\n\n${d.content}`);
  const response = await openai.embeddings.create({
    model: 'text-embedding-3-small',
    input: texts,
  });

  await db.transaction(async (tx) => {
    for (let i = 0; i < docs.length; i++) {
      const embedding = new Float32Array(response.data[i].embedding);
      await tx.run(`
        INSERT INTO docs (slug, title, content, section, embedding)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(slug) DO UPDATE SET
          title = excluded.title,
          content = excluded.content,
          section = excluded.section,
          embedding = excluded.embedding,
          updated_at = CURRENT_TIMESTAMP
      `, [docs[i].slug, docs[i].title, docs[i].content, docs[i].section,
          new Uint8Array(embedding.buffer)]);
    }
  });
}
```

### Search API

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
  const params: any[] = [new Uint8Array(queryEmbedding.buffer)];

  if (section) {
    sql += ' WHERE section = ?';
    params.push(section);
  }

  sql += ' ORDER BY score DESC LIMIT ?';
  params.push(limit);

  const results = await db.query(sql, params);

  // Generate snippets
  return results.map(r => ({
    ...r,
    snippet: generateSnippet(r.content, query),
  }));
}

function generateSnippet(content: string, query: string): string {
  // Simple snippet extraction around query terms
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

### Worker Endpoint

```typescript
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === '/search' && request.method === 'GET') {
      const query = url.searchParams.get('q');
      const section = url.searchParams.get('section') || undefined;

      if (!query) {
        return Response.json({ error: 'Query required' }, { status: 400 });
      }

      const results = await searchDocs(query, { section });
      return Response.json({ results });
    }

    return new Response('Not Found', { status: 404 });
  },
};
```

---

## Example: Recommendation System

A product recommendation system using collaborative filtering and content-based vectors.

### Schema

```sql
-- .do/migrations/001_create_products.sql
CREATE TABLE products (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sku TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  description TEXT,
  category TEXT NOT NULL,
  price REAL NOT NULL,
  -- Content-based embedding (from name + description)
  content_embedding BLOB NOT NULL,
  -- Collaborative filtering embedding (from user interactions)
  cf_embedding BLOB,
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
CREATE INDEX idx_interactions_product ON interactions(product_id);
```

### Content-Based Recommendations

```typescript
async function getContentRecommendations(
  productId: number,
  limit: number = 10
): Promise<Product[]> {
  // Get the product's embedding
  const product = await db.queryOne(
    'SELECT content_embedding FROM products WHERE id = ?',
    [productId]
  );

  if (!product) throw new Error('Product not found');

  // Find similar products (excluding the source product)
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

### Personalized Recommendations

```typescript
async function getPersonalizedRecommendations(
  userId: number,
  limit: number = 10
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
    // Cold start: return popular products
    return getPopularProducts(limit);
  }

  // Weight embeddings by interaction type
  const weights: Record<string, number> = {
    'purchase': 3.0,
    'cart': 2.0,
    'view': 1.0,
  };

  // Compute weighted average embedding
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

  // Find products similar to user profile
  const recentProductIds = await db.query(
    'SELECT product_id FROM interactions WHERE user_id = ? ORDER BY created_at DESC LIMIT 50',
    [userId]
  );
  const excludeIds = recentProductIds.map(r => r.product_id);

  return db.query(`
    SELECT id, sku, name, description, category, price,
           1 - vector_distance_cos(content_embedding, ?) as score
    FROM products
    WHERE id NOT IN (${excludeIds.map(() => '?').join(',')})
    ORDER BY score DESC
    LIMIT ?
  `, [new Uint8Array(userProfile.buffer), ...excludeIds, limit]);
}

async function getPopularProducts(limit: number): Promise<Product[]> {
  return db.query(`
    SELECT p.*, COUNT(i.id) as interaction_count
    FROM products p
    LEFT JOIN interactions i ON i.product_id = p.id
    GROUP BY p.id
    ORDER BY interaction_count DESC
    LIMIT ?
  `, [limit]);
}
```

### Category-Filtered Recommendations

```typescript
async function getCategoryRecommendations(
  productId: number,
  category: string,
  limit: number = 10
): Promise<Product[]> {
  const product = await db.queryOne(
    'SELECT content_embedding FROM products WHERE id = ?',
    [productId]
  );

  return db.query(`
    SELECT id, sku, name, description, price,
           1 - vector_distance_cos(content_embedding, ?) as similarity
    FROM products
    WHERE id != ? AND category = ?
    ORDER BY similarity DESC
    LIMIT ?
  `, [product.content_embedding, productId, category, limit]);
}
```

---

## Example: RAG (Retrieval Augmented Generation)

A complete RAG implementation for a Q&A system.

### Schema

```sql
-- .do/migrations/001_create_knowledge_base.sql
CREATE TABLE chunks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  document_id INTEGER NOT NULL,
  chunk_index INTEGER NOT NULL,
  content TEXT NOT NULL,
  embedding BLOB NOT NULL,
  metadata TEXT,  -- JSON metadata
  FOREIGN KEY (document_id) REFERENCES documents(id)
);

CREATE TABLE documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source TEXT NOT NULL,
  title TEXT,
  url TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_chunks_document ON chunks(document_id);
CREATE INDEX idx_chunks_embedding ON chunks
USING VECTOR (embedding)
WITH (dimensions = 1536, metric = 'cosine', m = 16, ef_construction = 200);
```

### Document Chunking and Ingestion

```typescript
import { DB } from '@dotdo/dosql';
import OpenAI from 'openai';

const openai = new OpenAI();
const db = await DB('knowledge-base');

interface Document {
  source: string;
  title: string;
  content: string;
  url?: string;
}

// Chunk document into overlapping segments
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

async function ingestDocument(doc: Document): Promise<void> {
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

### Retrieval

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
  options?: {
    topK?: number;
    minScore?: number;
  }
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

  // Filter by minimum score and limit
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

### Generation with Context

```typescript
interface RAGResponse {
  answer: string;
  sources: Array<{
    title: string;
    url: string;
    relevance: number;
  }>;
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

  // Build context string
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

### Complete RAG Worker

```typescript
import { DB } from '@dotdo/dosql';
import OpenAI from 'openai';

interface Env {
  DOSQL_DB: DurableObjectNamespace;
  OPENAI_API_KEY: string;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const openai = new OpenAI({ apiKey: env.OPENAI_API_KEY });
    const url = new URL(request.url);

    if (url.pathname === '/ask' && request.method === 'POST') {
      const { question } = await request.json();

      if (!question) {
        return Response.json({ error: 'Question required' }, { status: 400 });
      }

      const response = await answerQuestion(question);
      return Response.json(response);
    }

    if (url.pathname === '/ingest' && request.method === 'POST') {
      const document = await request.json();
      await ingestDocument(document);
      return Response.json({ success: true });
    }

    return new Response('Not Found', { status: 404 });
  },
};
```

### Advanced RAG: Hypothetical Document Embedding (HyDE)

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

  // Use the hypothetical answer for retrieval
  return retrieveContext(hypotheticalAnswer, { topK: 5 });
}
```

---

## Next Steps

- [Advanced Features](./advanced.md) - Time travel, branching, CDC streaming
- [API Reference](./api-reference.md) - Complete function and type reference
- [Architecture](./architecture.md) - Understanding DoSQL internals
- [Getting Started](./getting-started.md) - Basic usage and setup
