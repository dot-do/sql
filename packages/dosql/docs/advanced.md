# Advanced DoSQL Features

This guide covers advanced DoSQL features including vector search, time travel queries, branch/merge operations, CDC streaming, and CapnWeb RPC integration.

## Table of Contents

- [Vector Search](#vector-search)
- [Time Travel Queries](#time-travel-queries)
- [Branch and Merge](#branch-and-merge)
- [CDC Streaming to DoLake](#cdc-streaming-to-dolake)
- [CapnWeb RPC Integration](#capnweb-rpc-integration)
- [Virtual Tables](#virtual-tables)
- [Stored Procedures](#stored-procedures)
- [Sharding](#sharding)

---

## Vector Search

DoSQL supports vector similarity search for AI/ML applications.

### Creating a Vector Column

```sql
-- Create table with vector column
CREATE TABLE documents (
  id INTEGER PRIMARY KEY,
  content TEXT NOT NULL,
  embedding BLOB,  -- Store vectors as BLOB
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Create vector index (HNSW)
CREATE INDEX idx_documents_embedding ON documents
USING VECTOR (embedding)
WITH (
  dimensions = 1536,
  metric = 'cosine',
  m = 16,
  ef_construction = 200
);
```

### Inserting Vectors

```typescript
import { DB } from '@dotdo/dosql';

const db = await DB('embeddings');

// Insert document with embedding
const embedding = await getEmbedding('Hello, world!');  // Float32Array
await db.run(
  'INSERT INTO documents (content, embedding) VALUES (?, ?)',
  ['Hello, world!', new Uint8Array(embedding.buffer)]
);
```

### Vector Similarity Search

```typescript
// Find similar documents
const queryEmbedding = await getEmbedding('greeting');
const similar = await db.query(`
  SELECT id, content, vector_distance(embedding, ?) as distance
  FROM documents
  ORDER BY distance ASC
  LIMIT 10
`, [new Uint8Array(queryEmbedding.buffer)]);
```

### Hybrid Search (Text + Vector)

```typescript
// Combine keyword and semantic search
const results = await db.query(`
  SELECT id, content,
    (0.5 * bm25_score(content, :query)) +
    (0.5 * (1 - vector_distance(embedding, :embedding))) as score
  FROM documents
  WHERE content MATCH :query
  ORDER BY score DESC
  LIMIT 10
`, {
  query: 'greeting',
  embedding: new Uint8Array(queryEmbedding.buffer),
});
```

### Vector Search Options

```typescript
interface VectorIndexOptions {
  /** Number of dimensions */
  dimensions: number;

  /** Distance metric: 'cosine', 'euclidean', 'dot_product' */
  metric: 'cosine' | 'euclidean' | 'dot_product';

  /** HNSW M parameter (connections per node) */
  m?: number;

  /** HNSW ef_construction (build-time search width) */
  ef_construction?: number;

  /** HNSW ef_search (query-time search width) */
  ef_search?: number;
}
```

---

## Time Travel Queries

DoSQL supports querying data at any point in time using its WAL and snapshot system.

### Time Point Types

```typescript
import { lsn, timestamp, snapshot, branch, relative } from '@dotdo/dosql';

// By LSN (Log Sequence Number) - most precise
const point1 = lsn(12345n);

// By timestamp
const point2 = timestamp('2024-01-01T12:00:00Z');
const point3 = timestamp(new Date('2024-01-01'));

// By snapshot ID
const point4 = snapshot('main@5');

// By branch (defaults to HEAD)
const point5 = branch('feature-x');

// Relative (e.g., "5 versions ago")
const point6 = relative({ versionOffset: -5 });
```

### SQL Time Travel Syntax

```sql
-- Query at specific timestamp
SELECT * FROM users
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 12:00:00';

-- Query at specific LSN
SELECT * FROM users
FOR SYSTEM_TIME AS OF LSN 12345;

-- Query at specific snapshot
SELECT * FROM users
FOR SYSTEM_TIME AS OF SNAPSHOT 'main@5';

-- Query on specific branch
SELECT * FROM users
FOR SYSTEM_TIME AS OF BRANCH 'feature-x';

-- Query relative to current (5 versions ago)
SELECT * FROM users
FOR SYSTEM_TIME AS OF VERSION CURRENT - 5;
```

### Programmatic Time Travel

```typescript
import { DB } from '@dotdo/dosql';
import { createTimeTravelSession, timestamp } from '@dotdo/dosql/timetravel';

const db = await DB('analytics');

// Create a time travel session
const session = await createTimeTravelSession(db, {
  asOf: timestamp('2024-01-01T00:00:00Z'),
  scope: 'local',  // 'local' | 'branch' | 'global'
});

// All queries in session see data as of that time
const users = await session.query('SELECT * FROM users');
const orders = await session.query('SELECT * FROM orders');

// Close session
await session.close();
```

### Comparing Time Points

```typescript
import { createBranchHistoryManager, compareBranches } from '@dotdo/dosql/timetravel';

const manager = createBranchHistoryManager(deps);

// Get distance between two points
const distance = await manager.getDistance(
  timestamp('2024-01-01'),
  timestamp('2024-02-01'),
  'main'
);

// Compare branches
const comparison = await compareBranches(manager, 'feature-x', 'main');
console.log(`Feature is ${comparison.sourceAhead} commits ahead`);
console.log(`Feature is ${comparison.sourceBehind} commits behind`);
```

### Time Range Queries

```sql
-- Get history of a row
SELECT *, _dosql_version, _dosql_valid_from, _dosql_valid_to
FROM users
FOR SYSTEM_TIME BETWEEN '2024-01-01' AND '2024-02-01'
WHERE id = 42;

-- Get all changes in range
SELECT * FROM users
FOR SYSTEM_TIME FROM '2024-01-01' TO '2024-02-01';
```

---

## Branch and Merge

DoSQL supports git-like branching for databases, enabling safe experimentation and parallel development.

### Creating a Branch

```typescript
import { DB } from '@dotdo/dosql';
import { COWBackend, createCOWBackend } from '@dotdo/dosql/fsx';

const db = await DB('my-app');

// Create a branch from current state
await db.branch('feature-x');

// Create a branch from specific snapshot
await db.branch('experiment', { from: 'main@5' });

// Create a branch from specific LSN
await db.branch('rollback', { fromLSN: 12345n });
```

### Switching Branches

```typescript
// Switch to branch
await db.checkout('feature-x');

// Get current branch
const current = await db.currentBranch();
console.log(current); // 'feature-x'

// List all branches
const branches = await db.listBranches();
```

### Making Changes on Branch

```typescript
// On feature-x branch
await db.checkout('feature-x');

// Changes only affect this branch
await db.run('ALTER TABLE users ADD COLUMN role TEXT');
await db.run("INSERT INTO users (name, role) VALUES ('Admin', 'admin')");

// Main branch is unaffected
await db.checkout('main');
const columns = await db.query('PRAGMA table_info(users)');
// 'role' column doesn't exist here
```

### Merging Branches

```typescript
// Merge feature-x into main
await db.checkout('main');
const result = await db.merge('feature-x', {
  strategy: 'auto',  // 'auto' | 'ours' | 'theirs' | 'manual'
});

if (result.conflicts.length > 0) {
  console.log('Conflicts detected:', result.conflicts);
  // Handle conflicts manually
}
```

### Merge Strategies

```typescript
interface MergeOptions {
  /**
   * Merge strategy
   * - 'auto': Automatic merge, fail on conflicts
   * - 'ours': Keep our version on conflict
   * - 'theirs': Keep their version on conflict
   * - 'manual': Return conflicts without applying
   */
  strategy: 'auto' | 'ours' | 'theirs' | 'manual';

  /** Squash commits into single commit */
  squash?: boolean;

  /** Commit message */
  message?: string;
}
```

### Merge Conflict Resolution

```typescript
const result = await db.merge('feature-x', { strategy: 'manual' });

if (result.conflicts.length > 0) {
  for (const conflict of result.conflicts) {
    console.log(`Conflict in ${conflict.table} for key ${conflict.key}`);
    console.log('Main value:', conflict.main);
    console.log('Feature value:', conflict.branch);

    // Resolve manually
    await db.resolveConflict(conflict.id, conflict.branch); // Keep feature version
  }

  // Complete merge after resolving all conflicts
  await db.completeMerge();
}
```

### Branch Comparison

```typescript
import { compareBranches } from '@dotdo/dosql/timetravel';

const comparison = await compareBranches(manager, 'feature-x', 'main');

console.log({
  source: comparison.source,              // 'feature-x'
  target: comparison.target,              // 'main'
  sourceAhead: comparison.sourceAhead,    // commits unique to feature-x
  sourceBehind: comparison.sourceBehind,  // commits missing from feature-x
  diverged: comparison.diverged,          // both have unique commits
  canFastForward: comparison.canFastForward,
});
```

### Deleting Branches

```typescript
// Delete a merged branch
await db.deleteBranch('feature-x');

// Force delete unmerged branch
await db.deleteBranch('abandoned', { force: true });
```

---

## CDC Streaming to DoLake

Change Data Capture (CDC) enables real-time streaming of database changes to external systems.

### Basic CDC Subscription

```typescript
import { createCDC } from '@dotdo/dosql/cdc';

const cdc = createCDC(backend);

// Subscribe to all changes
for await (const entry of cdc.subscribe(0n)) {
  console.log('Operation:', entry.op);  // INSERT, UPDATE, DELETE
  console.log('Table:', entry.table);
  console.log('Data:', entry.after);
}
```

### Filtered Subscription

```typescript
import { createCDCSubscription } from '@dotdo/dosql/cdc';

const subscription = createCDCSubscription(backend, {
  fromLSN: 0n,
  filter: {
    tables: ['users', 'orders'],
    operations: ['INSERT', 'UPDATE'],
  },
});

for await (const event of subscription.iterate()) {
  if (event.type === 'insert') {
    console.log('New row:', event.data);
  } else if (event.type === 'update') {
    console.log('Changed:', event.before, '->', event.after);
  }
}
```

### Replication Slots

Replication slots provide durable position tracking for reliable consumption:

```typescript
import { createCDC } from '@dotdo/dosql/cdc';

const cdc = createCDC(backend);

// Create a replication slot
await cdc.slots.createSlot('my-consumer', 0n);

// Subscribe from slot position
const subscription = await cdc.slots.subscribeFromSlot('my-consumer');

for await (const event of subscription.iterate()) {
  // Process event
  await processEvent(event);

  // Acknowledge position (persisted)
  await cdc.slots.updateSlot('my-consumer', event.lsn);
}
```

### Streaming to Lakehouse

```typescript
import { createLakehouseStreamer } from '@dotdo/dosql/cdc';

const streamer = createLakehouseStreamer({
  cdc: cdcSubscription,
  lakehouse: {
    r2Bucket: env.DATA_BUCKET,
    prefix: 'cdc/',
    format: 'parquet',
  },
  batch: {
    maxSize: 10000,      // Max events per batch
    maxBytes: 10485760,  // 10MB max
    maxWaitMs: 5000,     // Flush every 5 seconds
  },
});

// Start streaming
await streamer.start();

// Monitor status
const status = await streamer.getStatus();
console.log('Processed LSN:', status.lastProcessedLSN);
console.log('Pending events:', status.pendingCount);

// Stop gracefully
await streamer.stop();
```

### CDC Event Types

```typescript
interface ChangeEvent {
  /** Change type */
  type: 'insert' | 'update' | 'delete';

  /** Table name */
  table: string;

  /** Log sequence number */
  lsn: bigint;

  /** Transaction ID */
  txnId: string;

  /** Timestamp */
  timestamp: Date;

  /** Row data before change (for update/delete) */
  before?: Record<string, unknown>;

  /** Row data after change (for insert/update) */
  after?: Record<string, unknown>;

  /** Primary key */
  key: Uint8Array;
}
```

---

## CapnWeb RPC Integration

DoSQL uses CapnWeb for efficient DO-to-DO communication and client queries.

### RPC Server Setup

```typescript
import { DoSQLTarget, handleDoSQLRequest } from '@dotdo/dosql/rpc';

export class DatabaseDO implements DurableObject {
  private db: Database;
  private target: DoSQLTarget;

  constructor(state: DurableObjectState, env: Env) {
    this.target = new DoSQLTarget(this);
  }

  async fetch(request: Request): Promise<Response> {
    // Handle DoSQL RPC requests
    if (isDoSQLRequest(request)) {
      return handleDoSQLRequest(request, this.target);
    }

    // Handle other requests
    return new Response('Not Found', { status: 404 });
  }

  // DoSQLTarget methods
  async query(sql: string, params?: unknown[]): Promise<unknown[]> {
    return this.db.query(sql, params);
  }

  async run(sql: string, params?: unknown[]): Promise<RunResult> {
    return this.db.run(sql, params);
  }
}
```

### RPC Client Usage

```typescript
import { createWebSocketClient, createHttpClient } from '@dotdo/dosql/rpc';

// WebSocket client (for long-lived connections)
const wsClient = await createWebSocketClient({
  url: 'wss://my-do.example.com/db',
  reconnect: true,
});

// HTTP batch client (for stateless requests)
const httpClient = createHttpClient({
  url: 'https://my-do.example.com/db',
  batch: true,
});

// Both clients have the same interface
const users = await wsClient.query('SELECT * FROM users');
await wsClient.run('INSERT INTO users (name) VALUES (?)', ['Alice']);
```

### RPC Transactions

```typescript
import { withTransaction } from '@dotdo/dosql/rpc';

await withTransaction(client, async (tx) => {
  await tx.run('INSERT INTO orders (user_id) VALUES (?)', [1]);
  await tx.run('UPDATE users SET order_count = order_count + 1 WHERE id = ?', [1]);
  // Automatically commits on success, rolls back on error
});
```

### Streaming Query Results

```typescript
// Stream large result sets
const stream = client.stream('SELECT * FROM large_table');

for await (const chunk of stream) {
  for (const row of chunk.rows) {
    await processRow(row);
  }
}
```

### CDC over RPC

```typescript
// Subscribe to CDC events via RPC
const subscription = await client.subscribeCDC({
  fromLSN: 0n,
  tables: ['users', 'orders'],
});

for await (const event of subscription) {
  console.log('Change:', event.type, event.table);
}
```

---

## Virtual Tables

Query external data sources directly using SQL.

### URL Sources

```sql
-- JSON API
SELECT * FROM 'https://api.example.com/users.json'
WHERE role = 'admin';

-- CSV file
SELECT * FROM 'https://data.gov/dataset.csv'
WITH (headers=true, delimiter=',');

-- Parquet from R2
SELECT * FROM 'r2://mybucket/data/sales.parquet'
WHERE year = 2024;

-- With authentication
SELECT * FROM 'https://api.example.com/private.json'
WITH (auth='bearer', token='secret');
```

### Programmatic Virtual Tables

```typescript
import { createVirtualTableRegistry, createURLVirtualTable } from '@dotdo/dosql/virtual';

const registry = createVirtualTableRegistry();

// Register a virtual table
registry.register('github_users', createURLVirtualTable({
  url: 'https://api.github.com/users',
  format: 'json',
  transform: (data) => data.map(u => ({
    id: u.id,
    login: u.login,
    avatar_url: u.avatar_url,
  })),
}));

// Query the virtual table
const users = await db.query('SELECT * FROM github_users LIMIT 10');
```

### Virtual Table Options

```typescript
interface URLSourceOptions {
  /** Source URL */
  url: string;

  /** Data format: 'json', 'csv', 'parquet', 'ndjson' */
  format: VirtualTableFormat;

  /** Authentication options */
  auth?: AuthOptions;

  /** Request headers */
  headers?: Record<string, string>;

  /** CSV-specific options */
  csv?: {
    delimiter?: string;
    headers?: boolean;
    quote?: string;
  };

  /** Cache options */
  cache?: {
    ttl: number;
    key?: string;
  };

  /** Transform function */
  transform?: (data: unknown) => unknown[];
}
```

---

## Stored Procedures

DoSQL supports ESM-based stored procedures for complex business logic.

### Defining a Procedure

```typescript
import { procedure, ProcedureContext } from '@dotdo/dosql/proc';

// Define a procedure
const createUser = procedure('create_user')
  .input({
    name: 'string',
    email: 'string',
    role: 'string?', // optional
  })
  .output({
    id: 'number',
    created: 'boolean',
  })
  .handler(async (ctx: ProcedureContext, input) => {
    const { db } = ctx;

    // Check if user exists
    const existing = await db.queryOne(
      'SELECT id FROM users WHERE email = ?',
      [input.email]
    );

    if (existing) {
      return { id: existing.id, created: false };
    }

    // Create user
    const result = await db.run(
      'INSERT INTO users (name, email, role) VALUES (?, ?, ?)',
      [input.name, input.email, input.role || 'user']
    );

    return { id: result.lastInsertRowId, created: true };
  });
```

### Registering and Executing

```typescript
import { createProcedureRegistry, createProcedureExecutor } from '@dotdo/dosql/proc';

// Create registry
const registry = createProcedureRegistry();
registry.register(createUser);

// Create executor
const executor = createProcedureExecutor(db, registry);

// Execute procedure
const result = await executor.execute('create_user', {
  name: 'Alice',
  email: 'alice@example.com',
});

console.log(result); // { id: 1, created: true }
```

### SQL Procedure Syntax

```sql
-- Call procedure
CALL create_user('Alice', 'alice@example.com');

-- Call with named parameters
CALL create_user(name='Alice', email='alice@example.com', role='admin');

-- Call in transaction
BEGIN;
CALL create_user('Bob', 'bob@example.com');
CALL send_welcome_email(:last_insert_id);
COMMIT;
```

---

## Sharding

DoSQL supports horizontal sharding across multiple Durable Objects.

### Shard Configuration

```typescript
import { createShardRouter, VindexType } from '@dotdo/dosql/sharding';

const router = createShardRouter({
  tables: {
    users: {
      // Shard by user ID using hash
      vindex: VindexType.HASH,
      column: 'id',
      shards: 16,  // Number of shards
    },
    orders: {
      // Shard by user_id to colocate with users
      vindex: VindexType.HASH,
      column: 'user_id',
      shards: 16,
    },
  },
});
```

### Executing Sharded Queries

```typescript
import { createShardExecutor } from '@dotdo/dosql/sharding';

const executor = createShardExecutor(router, {
  getDO: (shardId) => env.DOSQL_DB.get(env.DOSQL_DB.idFromName(`shard-${shardId}`)),
});

// Single-shard query (knows exact shard)
const user = await executor.query(
  'SELECT * FROM users WHERE id = ?',
  [42]
);

// Scatter-gather query (queries all shards)
const activeUsers = await executor.query(
  'SELECT * FROM users WHERE active = ?',
  [true]
);
```

### Vindex Types

| Type | Description | Use Case |
|------|-------------|----------|
| `HASH` | Consistent hash on column | Even distribution |
| `RANGE` | Range-based partitioning | Time-series data |
| `LOOKUP` | External lookup table | Custom routing |
| `REGION` | Geographic routing | Multi-region |

### Cross-Shard Transactions

```typescript
// Two-phase commit for cross-shard transactions
await executor.transaction(async (tx) => {
  // This may touch multiple shards
  await tx.run('UPDATE accounts SET balance = balance - 100 WHERE id = ?', [1]);
  await tx.run('UPDATE accounts SET balance = balance + 100 WHERE id = ?', [2]);
});
```

---

## Next Steps

- [Architecture](./architecture.md) - Understanding DoSQL internals
- [API Reference](./api-reference.md) - Complete API documentation
