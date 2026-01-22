# Migrating from Cloudflare D1 to DoSQL

This guide provides comprehensive documentation for migrating your application from Cloudflare D1 to DoSQL. It covers feature comparisons, migration strategies, code changes, and testing procedures.

---

## Table of Contents

- [Overview](#overview)
  - [Why Migrate](#why-migrate)
  - [Architecture Differences](#architecture-differences)
  - [When to Migrate vs Stay on D1](#when-to-migrate-vs-stay-on-d1)
- [Compatibility Matrix](#compatibility-matrix)
  - [SQL Dialect Differences](#sql-dialect-differences)
  - [Supported vs Unsupported D1 Features](#supported-vs-unsupported-d1-features)
  - [API Mapping Table](#api-mapping-table)
- [Step-by-Step Migration](#step-by-step-migration)
  - [Schema Export from D1](#schema-export-from-d1)
  - [Schema Import to DoSQL](#schema-import-to-dosql)
  - [Data Migration Strategies](#data-migration-strategies)
  - [Zero-Downtime Migration](#zero-downtime-migration)
- [Code Transformation Examples](#code-transformation-examples)
  - [D1 Binding Replacement](#d1-binding-replacement)
  - [Query Syntax Adjustments](#query-syntax-adjustments)
  - [Transaction Handling Differences](#transaction-handling-differences)
  - [Error Handling Changes](#error-handling-changes)
- [Client SDK Migration](#client-sdk-migration)
  - [From D1 Client to DoSQL Client](#from-d1-client-to-dosql-client)
  - [TypeScript Type Changes](#typescript-type-changes)
  - [Query Builder Compatibility](#query-builder-compatibility)
- [Testing Your Migration](#testing-your-migration)
  - [Parallel Running Both Databases](#parallel-running-both-databases)
  - [Data Consistency Verification](#data-consistency-verification)
  - [Performance Comparison](#performance-comparison)
- [Rollback Procedures](#rollback-procedures)
  - [How to Roll Back](#how-to-roll-back)
  - [Data Sync Considerations](#data-sync-considerations)

---

## Overview

### Why Migrate

DoSQL provides several advanced features that Cloudflare D1 does not offer:

| Feature | D1 | DoSQL | Impact |
|---------|-----|-------|--------|
| **Change Data Capture (CDC)** | No | Yes | Real-time streaming to lakehouse |
| **Time Travel** | No | Yes | Query data at any point in time |
| **Database Branching** | No | Yes | Git-like branching and merging |
| **Vector Search** | No | Yes | HNSW indexes, similarity search |
| **Multi-Tenant Isolation** | Limited | Full | Per-tenant Durable Objects |
| **Real-time Subscriptions** | No | Yes | CDC subscriptions via RPC |
| **Tiered Storage** | No | Yes | Hot (DO) + Cold (R2) tiers |
| **Global Distribution** | Regional | Per-request | Routing to any region |
| **Custom Stored Procedures** | No | Yes | ESM-based procedures |
| **Sharding** | No | Yes | Horizontal scaling across DOs |

**Key benefits of migrating to DoSQL:**

1. **CDC for Event-Driven Architectures** - Stream changes to data warehouses, Kafka, or other systems
2. **Time Travel for Auditing** - Query historical data, undo mistakes, audit changes
3. **Branching for Safe Deployments** - Test schema changes on branches before merging
4. **Multi-Tenant Isolation** - True isolation with per-tenant Durable Objects
5. **Smaller Bundle Size** - ~7KB vs ~500KB+ for WASM-based alternatives

### Architecture Differences

**Cloudflare D1:**

```
+-------------------------------------------------------------+
|                      Cloudflare D1                           |
+-------------------------------------------------------------+
|                                                               |
|  +-------------+    +-------------+    +-----------------+   |
|  |   Worker    |--->|  D1 Binding |--->|     SQLite      |   |
|  |   (Edge)    |    |   (Proxy)   |    |   (Regional)    |   |
|  +-------------+    +-------------+    +-----------------+   |
|                                                               |
|  Characteristics:                                             |
|  - Centralized database                                       |
|  - Regional replicas for reads                                |
|  - Single leader for writes                                   |
|  - Shared infrastructure across tenants                       |
|                                                               |
+-------------------------------------------------------------+
```

**DoSQL:**

```
+-------------------------------------------------------------+
|                         DoSQL                                |
+-------------------------------------------------------------+
|                                                               |
|  +-------------+    +-------------+    +-----------------+   |
|  |   Worker    |--->| DO Binding  |--->|    Durable      |   |
|  |   (Edge)    |    |   (Stub)    |    |    Object       |   |
|  +-------------+    +-------------+    +-----------------+   |
|                                               |               |
|                           +-------------------+------+        |
|                           |                   |      |        |
|                           v                   v      v        |
|                     +----------+        +--------+            |
|                     | DO Store |        |   R2   |            |
|                     |  (Hot)   |        | (Cold) |            |
|                     +----------+        +--------+            |
|                                                               |
|  Characteristics:                                             |
|  - Per-tenant Durable Objects                                 |
|  - Strong consistency within DO                               |
|  - Automatic geographic distribution                          |
|  - Tiered storage (hot/cold)                                  |
|                                                               |
+-------------------------------------------------------------+
```

### When to Migrate vs Stay on D1

**Stay on D1 if:**

- You have a simple, single-tenant application
- You don't need CDC, time travel, or branching
- Your data model is straightforward CRUD
- You want managed infrastructure with minimal configuration
- Your read patterns benefit from regional replicas
- You need full SQLite compatibility

**Migrate to DoSQL if:**

- You need change data capture for event-driven architectures
- You require time travel queries for auditing or debugging
- You want database branching for safe schema migrations
- You have multi-tenant SaaS with isolation requirements
- You need vector search for AI/ML applications
- You want real-time subscriptions to data changes
- You need tiered storage (hot/cold) for cost optimization
- You're hitting D1 limits (10GB per database, 25 databases)

---

## Compatibility Matrix

### SQL Dialect Differences

Both D1 and DoSQL use SQLite-compatible SQL, with some differences:

| Feature | D1 | DoSQL | Notes |
|---------|-----|-------|-------|
| `SELECT` | Yes | Yes | Fully compatible |
| `INSERT` | Yes | Yes | Fully compatible |
| `UPDATE` | Yes | Yes | Fully compatible |
| `DELETE` | Yes | Yes | Fully compatible |
| `CREATE TABLE` | Yes | Yes | Fully compatible |
| `CREATE INDEX` | Yes | Yes | Fully compatible |
| `ALTER TABLE` | Limited | Limited | Both have SQLite limitations |
| `DROP TABLE` | Yes | Yes | Fully compatible |
| `PRAGMA` | Limited | Extended | DoSQL adds custom PRAGMAs |
| `EXPLAIN` | Yes | Yes | Fully compatible |
| `VACUUM` | Yes | Yes | Fully compatible |
| `FOR SYSTEM_TIME` | No | Yes | DoSQL time travel syntax |
| `USING VECTOR` | No | Yes | DoSQL vector index syntax |
| `CALL` | No | Yes | DoSQL stored procedures |

**D1-specific features not in DoSQL:**

- `D1_LAST_BATCH_ID()` - D1 batch tracking (not needed in DoSQL)

**DoSQL-specific SQL extensions:**

```sql
-- Time travel queries
SELECT * FROM users FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 12:00:00';
SELECT * FROM users FOR SYSTEM_TIME AS OF LSN 12345;
SELECT * FROM users FOR SYSTEM_TIME AS OF BRANCH 'feature-x';

-- Vector index creation
CREATE INDEX idx_embedding ON documents USING VECTOR (embedding)
WITH (dimensions = 1536, metric = 'cosine');

-- Vector similarity search
SELECT *, vector_distance(embedding, ?) as distance
FROM documents
ORDER BY distance;

-- Custom PRAGMAs
PRAGMA dosql_wal_status;
PRAGMA dosql_branches;
PRAGMA dosql_current_lsn;
```

### Supported vs Unsupported D1 Features

| D1 Feature | DoSQL Support | Migration Notes |
|------------|---------------|-----------------|
| Prepared statements | Full | Same API |
| Parameterized queries | Full | Same syntax (`?` and `:name`) |
| Batch operations | Full | `db.batch()` supported |
| Transactions | Full | Enhanced with savepoints |
| JSON functions | Full | `json()`, `json_extract()`, etc. |
| Full-text search | Partial | Use vector search for semantic |
| Triggers | No | Use CDC subscriptions instead |
| Views | Yes | Standard SQLite views |
| CTEs (WITH) | Yes | Common Table Expressions work |
| Window functions | Yes | `ROW_NUMBER()`, `RANK()`, etc. |
| Foreign keys | Yes | `PRAGMA foreign_keys = ON` |

### API Mapping Table

| D1 Method | DoSQL Equivalent | Notes |
|-----------|------------------|-------|
| `db.prepare(sql)` | `db.prepare(sql)` | Same |
| `stmt.bind(...params)` | `stmt.bind(params)` | Array instead of spread |
| `stmt.all()` | `stmt.all()` | Same |
| `stmt.first()` | `stmt.get()` | Renamed |
| `stmt.run()` | `stmt.run()` | Same |
| `stmt.raw()` | `stmt.iterate()` | Different return format |
| `db.batch([...])` | `db.transaction(...)` | Use transactions |
| `db.exec(sql)` | `db.exec(sql)` | Same |
| `db.dump()` | Not supported | Use CDC export |

---

## Step-by-Step Migration

### Schema Export from D1

Export your D1 schema using Wrangler:

```bash
# List your D1 databases
npx wrangler d1 list

# Export schema (DDL only)
npx wrangler d1 export my-database --output=schema.sql --no-data

# Export full database (schema + data)
npx wrangler d1 export my-database --output=full-export.sql
```

Alternatively, query the schema programmatically:

```typescript
// D1 schema export
async function exportD1Schema(db: D1Database): Promise<string> {
  const tables = await db
    .prepare(`
      SELECT name, sql FROM sqlite_master
      WHERE type='table' AND name NOT LIKE 'sqlite_%'
    `)
    .all<{ name: string; sql: string }>();

  const indexes = await db
    .prepare(`
      SELECT name, sql FROM sqlite_master
      WHERE type='index' AND sql IS NOT NULL
    `)
    .all<{ name: string; sql: string }>();

  let schema = '-- Tables\n';
  for (const table of tables.results) {
    schema += `${table.sql};\n\n`;
  }

  schema += '-- Indexes\n';
  for (const index of indexes.results) {
    schema += `${index.sql};\n\n`;
  }

  return schema;
}
```

### Schema Import to DoSQL

**Option 1: Migration Files (Recommended)**

Create migration files in `.do/migrations/`:

```
your-project/
+-- .do/
|   +-- migrations/
|       +-- 001_initial_schema.sql    # Your D1 schema
|       +-- 002_add_dosql_features.sql # DoSQL enhancements
|       +-- ...
```

```sql
-- .do/migrations/001_initial_schema.sql
-- Imported from D1

CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT UNIQUE,
  active BOOLEAN DEFAULT true,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE posts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  title TEXT NOT NULL,
  body TEXT,
  published_at TEXT,
  FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX idx_posts_user_id ON posts(user_id);
CREATE INDEX idx_posts_published ON posts(published_at);
```

```sql
-- .do/migrations/002_add_dosql_features.sql
-- Optional: Add DoSQL-specific features

-- Add vector column for semantic search
ALTER TABLE posts ADD COLUMN embedding BLOB;

-- Create vector index
CREATE INDEX idx_posts_embedding ON posts
USING VECTOR (embedding)
WITH (dimensions = 1536, metric = 'cosine');
```

**Option 2: Inline Migrations**

```typescript
import { DB } from '@dotdo/dosql';

const d1Schema = `
  CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE
  );
`;

const db = await DB('my-tenant', {
  migrations: [
    { id: '001_d1_import', sql: d1Schema },
  ],
});
```

### Data Migration Strategies

**Strategy 1: Full Export/Import (Simple, Downtime Required)**

```typescript
// Step 1: Export all data from D1
async function exportD1Data(d1: D1Database): Promise<Map<string, unknown[]>> {
  const data = new Map<string, unknown[]>();

  // Get all tables
  const tables = await d1
    .prepare(`
      SELECT name FROM sqlite_master
      WHERE type='table'
      AND name NOT LIKE 'sqlite_%'
      AND name NOT LIKE '_cf_%'
    `)
    .all<{ name: string }>();

  // Export each table
  for (const table of tables.results) {
    const rows = await d1.prepare(`SELECT * FROM ${table.name}`).all();
    data.set(table.name, rows.results);
  }

  return data;
}

// Step 2: Import into DoSQL
async function importToDoSQL(
  dosql: Database,
  data: Map<string, unknown[]>
): Promise<void> {
  await dosql.transaction(async (tx) => {
    for (const [table, rows] of data.entries()) {
      for (const row of rows) {
        const columns = Object.keys(row);
        const placeholders = columns.map(() => '?').join(', ');
        const values = Object.values(row);

        await tx.run(
          `INSERT INTO ${table} (${columns.join(', ')}) VALUES (${placeholders})`,
          values
        );
      }
    }
  });
}

// Migration script
async function migrate(d1: D1Database, dosql: Database): Promise<void> {
  console.log('Exporting data from D1...');
  const data = await exportD1Data(d1);

  console.log(`Importing ${data.size} tables to DoSQL...`);
  await importToDoSQL(dosql, data);

  console.log('Migration complete!');
}
```

**Strategy 2: Incremental Sync (For Large Datasets)**

```typescript
interface SyncState {
  table: string;
  lastSyncedId: number;
  lastSyncedAt: string;
}

async function incrementalSync(
  d1: D1Database,
  dosql: Database,
  batchSize: number = 1000
): Promise<void> {
  // Get sync state
  let syncState = await dosql.queryOne<SyncState>(
    'SELECT * FROM _sync_state WHERE table_name = ?',
    ['users']
  );

  if (!syncState) {
    syncState = { table: 'users', lastSyncedId: 0, lastSyncedAt: '' };
  }

  // Fetch new records from D1
  const newRows = await d1
    .prepare(`
      SELECT * FROM users
      WHERE id > ?
      ORDER BY id ASC
      LIMIT ?
    `)
    .bind(syncState.lastSyncedId, batchSize)
    .all();

  if (newRows.results.length === 0) {
    console.log('No new records to sync');
    return;
  }

  // Insert into DoSQL
  await dosql.transaction(async (tx) => {
    for (const row of newRows.results) {
      await tx.run(
        `INSERT OR REPLACE INTO users (id, name, email, created_at)
         VALUES (?, ?, ?, ?)`,
        [row.id, row.name, row.email, row.created_at]
      );
    }

    // Update sync state
    const lastRow = newRows.results[newRows.results.length - 1];
    await tx.run(
      `INSERT OR REPLACE INTO _sync_state (table_name, last_synced_id, last_synced_at)
       VALUES (?, ?, datetime('now'))`,
      ['users', lastRow.id]
    );
  });

  console.log(`Synced ${newRows.results.length} records`);
}
```

**Strategy 3: Dual-Write During Transition**

```typescript
class DualWriteDatabase {
  constructor(
    private d1: D1Database,
    private dosql: Database,
    private primaryIsDoSQL: boolean = false
  ) {}

  async run(sql: string, params?: unknown[]): Promise<void> {
    if (this.primaryIsDoSQL) {
      // Write to DoSQL first, then D1
      await this.dosql.run(sql, params);
      try {
        await this.d1.prepare(sql).bind(...(params || [])).run();
      } catch (error) {
        console.error('D1 secondary write failed:', error);
        // Continue - DoSQL is primary
      }
    } else {
      // Write to D1 first, then DoSQL
      await this.d1.prepare(sql).bind(...(params || [])).run();
      try {
        await this.dosql.run(sql, params);
      } catch (error) {
        console.error('DoSQL secondary write failed:', error);
        // Continue - D1 is primary
      }
    }
  }

  async query<T>(sql: string, params?: unknown[]): Promise<T[]> {
    // Always read from primary
    if (this.primaryIsDoSQL) {
      return this.dosql.query<T>(sql, params);
    } else {
      const result = await this.d1.prepare(sql).bind(...(params || [])).all<T>();
      return result.results;
    }
  }

  // Switch primary after verification
  setPrimaryToDoSQL(): void {
    this.primaryIsDoSQL = true;
  }
}
```

### Zero-Downtime Migration

```
Phase 1: Preparation (No downtime)
+---------------------------------------------------------------+
|  1. Deploy DoSQL infrastructure                                |
|  2. Create schema in DoSQL                                     |
|  3. Set up dual-write with D1 as primary                       |
|  4. Start incremental data sync from D1 to DoSQL               |
+---------------------------------------------------------------+
                              |
                              v
Phase 2: Sync & Verify (No downtime)
+---------------------------------------------------------------+
|  5. Continue dual-write until DoSQL catches up                 |
|  6. Run data consistency verification                          |
|  7. Compare query results between D1 and DoSQL                 |
|  8. Monitor for discrepancies                                  |
+---------------------------------------------------------------+
                              |
                              v
Phase 3: Cutover (Brief read-only window)
+---------------------------------------------------------------+
|  9. Enable read-only mode briefly                              |
|  10. Final sync of any remaining records                       |
|  11. Switch primary to DoSQL                                   |
|  12. Resume normal operations                                  |
+---------------------------------------------------------------+
                              |
                              v
Phase 4: Cleanup (No downtime)
+---------------------------------------------------------------+
|  13. Continue dual-write to D1 for safety period               |
|  14. Monitor for issues                                        |
|  15. Remove D1 writes after confidence period                  |
|  16. Decommission D1 database                                  |
+---------------------------------------------------------------+
```

---

## Code Transformation Examples

### D1 Binding Replacement

**D1 (Before):**

```typescript
// wrangler.toml
// [[d1_databases]]
// binding = "DB"
// database_name = "my-database"
// database_id = "xxxx"

export interface Env {
  DB: D1Database;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const users = await env.DB
      .prepare('SELECT * FROM users WHERE active = ?')
      .bind(true)
      .all();

    return Response.json(users.results);
  },
};
```

**DoSQL (After):**

```typescript
// wrangler.toml
// [durable_objects]
// bindings = [
//   { name = "DOSQL_DB", class_name = "TenantDatabase" }
// ]

export interface Env {
  DOSQL_DB: DurableObjectNamespace;
}

export { TenantDatabase } from './database';

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Get or create DO for tenant
    const tenantId = getTenantId(request);
    const id = env.DOSQL_DB.idFromName(tenantId);
    const stub = env.DOSQL_DB.get(id);

    // Forward request to DO
    return stub.fetch(request);
  },
};

// database.ts
import { DB, Database } from '@dotdo/dosql';

export class TenantDatabase implements DurableObject {
  private db: Database | null = null;
  private state: DurableObjectState;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
  }

  private async getDB(): Promise<Database> {
    if (!this.db) {
      this.db = await DB('tenant', {
        migrations: { folder: '.do/migrations' },
        storage: { hot: this.state.storage },
      });
    }
    return this.db;
  }

  async fetch(request: Request): Promise<Response> {
    const db = await this.getDB();
    const url = new URL(request.url);

    if (url.pathname === '/users') {
      const users = await db.query(
        'SELECT * FROM users WHERE active = ?',
        [true]
      );
      return Response.json(users);
    }

    return new Response('Not Found', { status: 404 });
  }
}
```

### Query Syntax Adjustments

**Prepared Statements:**

```typescript
// D1
const stmt = env.DB.prepare('SELECT * FROM users WHERE id = ?');
const result = await stmt.bind(42).first();

// DoSQL
const stmt = db.prepare('SELECT * FROM users WHERE id = ?');
const result = await stmt.get([42]);  // Use array, not .bind()
```

**All Rows:**

```typescript
// D1
const result = await env.DB
  .prepare('SELECT * FROM users')
  .all();
const users = result.results;

// DoSQL
const users = await db.query('SELECT * FROM users');
// Returns array directly, no .results wrapper
```

**Batch Operations:**

```typescript
// D1
const results = await env.DB.batch([
  env.DB.prepare('INSERT INTO users (name) VALUES (?)').bind('Alice'),
  env.DB.prepare('INSERT INTO users (name) VALUES (?)').bind('Bob'),
]);

// DoSQL - Use transactions instead
await db.transaction(async (tx) => {
  await tx.run('INSERT INTO users (name) VALUES (?)', ['Alice']);
  await tx.run('INSERT INTO users (name) VALUES (?)', ['Bob']);
});
```

**Raw SQL Execution:**

```typescript
// D1
await env.DB.exec(`
  CREATE TABLE temp (id INTEGER);
  INSERT INTO temp VALUES (1);
`);

// DoSQL - Same
await db.exec(`
  CREATE TABLE temp (id INTEGER);
  INSERT INTO temp VALUES (1);
`);
```

### Transaction Handling Differences

**D1 Transactions:**

```typescript
// D1 uses batch() for atomic operations
const results = await env.DB.batch([
  env.DB.prepare('UPDATE accounts SET balance = balance - ? WHERE id = ?')
    .bind(100, 1),
  env.DB.prepare('UPDATE accounts SET balance = balance + ? WHERE id = ?')
    .bind(100, 2),
]);
```

**DoSQL Transactions:**

```typescript
// DoSQL uses proper transaction blocks
await db.transaction(async (tx) => {
  await tx.run(
    'UPDATE accounts SET balance = balance - ? WHERE id = ?',
    [100, 1]
  );
  await tx.run(
    'UPDATE accounts SET balance = balance + ? WHERE id = ?',
    [100, 2]
  );
  // Automatic commit on success, rollback on error
});

// With return value
const newBalance = await db.transaction(async (tx) => {
  await tx.run(
    'UPDATE accounts SET balance = balance - ? WHERE id = ?',
    [100, 1]
  );
  const result = await tx.queryOne<{ balance: number }>(
    'SELECT balance FROM accounts WHERE id = ?',
    [1]
  );
  return result?.balance;
});

// With savepoints (nested transactions)
await db.transaction(async (tx) => {
  await tx.run('INSERT INTO orders (user_id) VALUES (?)', [1]);

  await tx.savepoint('items', async (sp) => {
    await sp.run(
      'INSERT INTO order_items (order_id, product_id) VALUES (?, ?)',
      [1, 100]
    );
    // Can rollback just this savepoint without affecting outer transaction
  });
});

// Transaction modes
await db.transaction(async (tx) => { /* ... */ }, { mode: 'IMMEDIATE' });
await db.transaction(async (tx) => { /* ... */ }, { mode: 'EXCLUSIVE' });
```

### Error Handling Changes

**D1 Error Handling:**

```typescript
// D1
try {
  await env.DB
    .prepare('INSERT INTO users (email) VALUES (?)')
    .bind('duplicate@test.com')
    .run();
} catch (error) {
  if (error.message.includes('UNIQUE constraint failed')) {
    // Handle duplicate
  }
  throw error;
}
```

**DoSQL Error Handling:**

```typescript
import {
  TransactionError,
  TransactionErrorCode
} from '@dotdo/dosql/transaction';

// DoSQL with typed errors
try {
  await db.run(
    'INSERT INTO users (email) VALUES (?)',
    ['duplicate@test.com']
  );
} catch (error) {
  if (error instanceof TransactionError) {
    switch (error.code) {
      case TransactionErrorCode.CONSTRAINT_VIOLATION:
        // Handle constraint violation
        break;
      case TransactionErrorCode.DEADLOCK:
        // Handle deadlock - maybe retry
        break;
      case TransactionErrorCode.LOCK_TIMEOUT:
        // Handle timeout
        break;
    }
  }
  throw error;
}

// Transaction-specific error handling
try {
  await db.transaction(async (tx) => {
    await tx.run(
      'UPDATE accounts SET balance = balance - 100 WHERE id = ?',
      [1]
    );
    throw new Error('Intentional rollback');
  });
} catch (error) {
  // Transaction automatically rolled back
  console.log('Transaction rolled back:', error.message);
}
```

---

## Client SDK Migration

### From D1 Client to DoSQL Client

**D1 Worker Client:**

```typescript
// D1 - Direct binding in Worker
export interface Env {
  DB: D1Database;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const users = await env.DB
      .prepare('SELECT * FROM users LIMIT ?')
      .bind(10)
      .all();
    return Response.json(users.results);
  }
};
```

**DoSQL with RPC Client:**

```typescript
// DoSQL - Client connecting to DO via RPC
import { createHttpClient, createWebSocketClient } from '@dotdo/dosql/rpc';

// HTTP client (stateless, good for simple queries)
const httpClient = createHttpClient({
  url: 'https://my-worker.example.com/db',
});

// WebSocket client (persistent, good for subscriptions)
const wsClient = await createWebSocketClient({
  url: 'wss://my-worker.example.com/db',
  reconnect: true,
});

// Same query interface
const users = await httpClient.query('SELECT * FROM users LIMIT ?', [10]);

// Subscribe to changes (DoSQL-only feature)
const subscription = await wsClient.subscribeCDC({
  fromLSN: 0n,
  tables: ['users'],
});

for await (const event of subscription) {
  console.log('Change:', event.type, event.table, event.after);
}
```

### TypeScript Type Changes

**D1 Types:**

```typescript
// D1 result types
interface D1Result<T> {
  results: T[];
  success: boolean;
  meta: D1Meta;
}

interface D1Meta {
  duration: number;
  changes: number;
  last_row_id: number;
  rows_read: number;
  rows_written: number;
}

// Usage
const result: D1Result<User> = await env.DB
  .prepare('SELECT * FROM users')
  .all<User>();

const users: User[] = result.results;
const duration = result.meta.duration;
```

**DoSQL Types:**

```typescript
// DoSQL - simpler return types
interface RunResult {
  rowsAffected: number;
  lastInsertRowId: number | bigint;
}

// Usage - direct array return
const users: User[] = await db.query<User>('SELECT * FROM users');

// Run result
const result: RunResult = await db.run(
  'INSERT INTO users (name) VALUES (?)',
  ['Alice']
);
console.log(result.lastInsertRowId);

// Type-safe queries with schema
import { createDatabase, type DatabaseSchema } from '@dotdo/dosql';

interface MySchema extends DatabaseSchema {
  users: {
    id: 'number';
    name: 'string';
    email: 'string';
    active: 'boolean';
  };
}

const db = createDatabase<MySchema>();

// Compile-time type inference
const users = await db.sql`SELECT id, name FROM users`;
// users is typed as { id: number; name: string }[]
```

**Migration Helper Types:**

```typescript
// Helper to convert D1 code to DoSQL
type D1ToDoSQLResult<T> = T[];  // Remove D1Result wrapper

// Wrapper for gradual migration
class D1CompatibilityWrapper {
  constructor(private db: Database) {}

  prepare(sql: string) {
    return new D1CompatibleStatement(this.db.prepare(sql));
  }

  async batch<T>(statements: D1CompatibleStatement[]): Promise<D1Result<T>[]> {
    return this.db.transaction(async (tx) => {
      const results: D1Result<T>[] = [];
      for (const stmt of statements) {
        const result = await stmt.executeInTransaction(tx);
        results.push(result);
      }
      return results;
    });
  }
}
```

### Query Builder Compatibility

**Drizzle ORM (Works with Both):**

```typescript
// D1 with Drizzle
import { drizzle } from 'drizzle-orm/d1';
import { users } from './schema';

const d1Db = drizzle(env.DB);
const result = await d1Db
  .select()
  .from(users)
  .where(eq(users.active, true));

// DoSQL with Drizzle adapter
import { drizzle } from 'drizzle-orm/dosql';  // Custom adapter needed
import { users } from './schema';

const dosqlDb = drizzle(db);
const result = await dosqlDb
  .select()
  .from(users)
  .where(eq(users.active, true));
```

**Kysely (Works with Both):**

```typescript
// D1 with Kysely
import { Kysely } from 'kysely';
import { D1Dialect } from 'kysely-d1';

const d1Db = new Kysely<Database>({
  dialect: new D1Dialect({ database: env.DB }),
});

// DoSQL with Kysely
import { DoSQLDialect } from '@dotdo/dosql/kysely';

const dosqlDb = new Kysely<Database>({
  dialect: new DoSQLDialect({ database: db }),
});

// Same query syntax for both
const users = await db
  .selectFrom('users')
  .selectAll()
  .where('active', '=', true)
  .execute();
```

---

## Testing Your Migration

### Parallel Running Both Databases

```typescript
// Test harness for comparing D1 and DoSQL
class MigrationTestHarness {
  constructor(
    private d1: D1Database,
    private dosql: Database,
    private logger: Logger
  ) {}

  async compareQuery<T>(
    sql: string,
    params?: unknown[]
  ): Promise<{ match: boolean; d1: T[]; dosql: T[] }> {
    const d1Result = await this.d1
      .prepare(sql)
      .bind(...(params || []))
      .all<T>();
    const dosqlResult = await this.dosql.query<T>(sql, params);

    const d1Sorted = this.sortResults(d1Result.results);
    const dosqlSorted = this.sortResults(dosqlResult);

    const match = JSON.stringify(d1Sorted) === JSON.stringify(dosqlSorted);

    if (!match) {
      this.logger.warn('Query mismatch', {
        sql,
        params,
        d1Count: d1Result.results.length,
        dosqlCount: dosqlResult.length,
      });
    }

    return { match, d1: d1Result.results, dosql: dosqlResult };
  }

  async runComparisonSuite(): Promise<ComparisonReport> {
    const results: QueryComparison[] = [];

    // Test all tables
    const tables = ['users', 'posts', 'comments'];
    for (const table of tables) {
      const comparison = await this.compareQuery(`SELECT * FROM ${table}`);
      results.push({ table, ...comparison });
    }

    // Test specific queries
    const queries = [
      { sql: 'SELECT COUNT(*) as count FROM users', params: [] },
      { sql: 'SELECT * FROM users WHERE active = ?', params: [true] },
      {
        sql: `SELECT u.*, COUNT(p.id) as post_count
              FROM users u
              LEFT JOIN posts p ON p.user_id = u.id
              GROUP BY u.id`,
        params: []
      },
    ];

    for (const { sql, params } of queries) {
      const comparison = await this.compareQuery(sql, params);
      results.push({ sql, params, ...comparison });
    }

    return {
      totalQueries: results.length,
      matches: results.filter(r => r.match).length,
      mismatches: results.filter(r => !r.match),
    };
  }

  private sortResults<T>(results: T[]): T[] {
    return [...results].sort((a, b) =>
      JSON.stringify(a).localeCompare(JSON.stringify(b))
    );
  }
}
```

### Data Consistency Verification

```typescript
// Verify data consistency between D1 and DoSQL
async function verifyDataConsistency(
  d1: D1Database,
  dosql: Database
): Promise<ConsistencyReport> {
  const issues: ConsistencyIssue[] = [];

  // Get all tables
  const d1Tables = await d1
    .prepare(`
      SELECT name FROM sqlite_master
      WHERE type='table' AND name NOT LIKE 'sqlite_%'
    `)
    .all<{ name: string }>();

  for (const { name: table } of d1Tables.results) {
    // Compare row counts
    const d1Count = await d1
      .prepare(`SELECT COUNT(*) as count FROM ${table}`)
      .first<{ count: number }>();
    const dosqlCount = await dosql.queryOne<{ count: number }>(
      `SELECT COUNT(*) as count FROM ${table}`
    );

    if (d1Count?.count !== dosqlCount?.count) {
      issues.push({
        type: 'row_count_mismatch',
        table,
        d1Value: d1Count?.count,
        dosqlValue: dosqlCount?.count,
      });
    }

    // Compare checksums (for small tables)
    if ((d1Count?.count || 0) < 10000) {
      const d1Checksum = await computeTableChecksum(d1, table);
      const dosqlChecksum = await computeTableChecksumDoSQL(dosql, table);

      if (d1Checksum !== dosqlChecksum) {
        issues.push({
          type: 'checksum_mismatch',
          table,
          d1Value: d1Checksum,
          dosqlValue: dosqlChecksum,
        });
      }
    }
  }

  return {
    verified: issues.length === 0,
    issues,
    timestamp: new Date().toISOString(),
  };
}

async function computeTableChecksum(
  d1: D1Database,
  table: string
): Promise<string> {
  const rows = await d1
    .prepare(`SELECT * FROM ${table} ORDER BY rowid`)
    .all();
  return hashRows(rows.results);
}

async function computeTableChecksumDoSQL(
  dosql: Database,
  table: string
): Promise<string> {
  const rows = await dosql.query(`SELECT * FROM ${table} ORDER BY rowid`);
  return hashRows(rows);
}

function hashRows(rows: unknown[]): string {
  const str = JSON.stringify(rows);
  // Simple hash for comparison
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash;
  }
  return hash.toString(16);
}
```

### Performance Comparison

```typescript
// Benchmark comparison between D1 and DoSQL
interface BenchmarkResult {
  operation: string;
  d1: { avgMs: number; p95Ms: number; p99Ms: number };
  dosql: { avgMs: number; p95Ms: number; p99Ms: number };
  winner: 'd1' | 'dosql' | 'tie';
}

async function runPerformanceBenchmark(
  d1: D1Database,
  dosql: Database,
  iterations: number = 100
): Promise<BenchmarkResult[]> {
  const results: BenchmarkResult[] = [];

  // Benchmark: Simple SELECT
  results.push(await benchmarkOperation(
    'Simple SELECT',
    async () => {
      await d1.prepare('SELECT * FROM users WHERE id = ?').bind(1).first();
    },
    async () => {
      await dosql.queryOne('SELECT * FROM users WHERE id = ?', [1]);
    },
    iterations
  ));

  // Benchmark: Complex JOIN
  results.push(await benchmarkOperation(
    'Complex JOIN',
    async () => {
      await d1.prepare(`
        SELECT u.*, COUNT(p.id) as post_count
        FROM users u
        LEFT JOIN posts p ON p.user_id = u.id
        WHERE u.active = ?
        GROUP BY u.id
        LIMIT 10
      `).bind(true).all();
    },
    async () => {
      await dosql.query(`
        SELECT u.*, COUNT(p.id) as post_count
        FROM users u
        LEFT JOIN posts p ON p.user_id = u.id
        WHERE u.active = ?
        GROUP BY u.id
        LIMIT 10
      `, [true]);
    },
    iterations
  ));

  // Benchmark: INSERT
  results.push(await benchmarkOperation(
    'INSERT',
    async () => {
      await d1
        .prepare('INSERT INTO benchmark_test (value) VALUES (?)')
        .bind(Math.random())
        .run();
    },
    async () => {
      await dosql.run(
        'INSERT INTO benchmark_test (value) VALUES (?)',
        [Math.random()]
      );
    },
    iterations
  ));

  // Benchmark: Transaction
  results.push(await benchmarkOperation(
    'Transaction (3 operations)',
    async () => {
      await d1.batch([
        d1.prepare('INSERT INTO benchmark_test (value) VALUES (?)').bind(1),
        d1.prepare('INSERT INTO benchmark_test (value) VALUES (?)').bind(2),
        d1.prepare('INSERT INTO benchmark_test (value) VALUES (?)').bind(3),
      ]);
    },
    async () => {
      await dosql.transaction(async (tx) => {
        await tx.run('INSERT INTO benchmark_test (value) VALUES (?)', [1]);
        await tx.run('INSERT INTO benchmark_test (value) VALUES (?)', [2]);
        await tx.run('INSERT INTO benchmark_test (value) VALUES (?)', [3]);
      });
    },
    iterations
  ));

  return results;
}

async function benchmarkOperation(
  name: string,
  d1Fn: () => Promise<void>,
  dosqlFn: () => Promise<void>,
  iterations: number
): Promise<BenchmarkResult> {
  const d1Times: number[] = [];
  const dosqlTimes: number[] = [];

  for (let i = 0; i < iterations; i++) {
    // D1
    const d1Start = performance.now();
    await d1Fn();
    d1Times.push(performance.now() - d1Start);

    // DoSQL
    const dosqlStart = performance.now();
    await dosqlFn();
    dosqlTimes.push(performance.now() - dosqlStart);
  }

  const d1Stats = computeStats(d1Times);
  const dosqlStats = computeStats(dosqlTimes);

  return {
    operation: name,
    d1: d1Stats,
    dosql: dosqlStats,
    winner: d1Stats.avgMs < dosqlStats.avgMs * 0.95 ? 'd1' :
            dosqlStats.avgMs < d1Stats.avgMs * 0.95 ? 'dosql' : 'tie',
  };
}

function computeStats(
  times: number[]
): { avgMs: number; p95Ms: number; p99Ms: number } {
  const sorted = [...times].sort((a, b) => a - b);
  return {
    avgMs: times.reduce((a, b) => a + b, 0) / times.length,
    p95Ms: sorted[Math.floor(sorted.length * 0.95)],
    p99Ms: sorted[Math.floor(sorted.length * 0.99)],
  };
}
```

---

## Rollback Procedures

### How to Roll Back

If issues occur after migrating to DoSQL, you can roll back to D1:

**Step 1: Re-enable D1 Writes**

```typescript
// Immediately re-enable D1 as primary
class RollbackEnabledDatabase {
  private useD1: boolean = true;  // Rollback flag

  constructor(
    private d1: D1Database,
    private dosql: Database
  ) {}

  async rollbackToD1(): void {
    this.useD1 = true;
    console.log('Rolled back to D1');
  }

  async query<T>(sql: string, params?: unknown[]): Promise<T[]> {
    if (this.useD1) {
      const result = await this.d1
        .prepare(sql)
        .bind(...(params || []))
        .all<T>();
      return result.results;
    }
    return this.dosql.query<T>(sql, params);
  }

  async run(sql: string, params?: unknown[]): Promise<void> {
    if (this.useD1) {
      await this.d1.prepare(sql).bind(...(params || [])).run();
    }
    await this.dosql.run(sql, params);
  }
}
```

**Step 2: Sync DoSQL Changes Back to D1**

```typescript
// Replay CDC events to D1
import { createCDC } from '@dotdo/dosql/cdc';

async function syncDoSQLToD1(
  dosql: Database,
  d1: D1Database,
  fromLSN: bigint
): Promise<void> {
  const cdc = createCDC(dosql);

  for await (const event of cdc.subscribe(fromLSN)) {
    switch (event.type) {
      case 'insert':
        const columns = Object.keys(event.after!);
        const values = Object.values(event.after!);
        const placeholders = columns.map(() => '?').join(', ');
        await d1
          .prepare(`INSERT INTO ${event.table} (${columns.join(', ')}) VALUES (${placeholders})`)
          .bind(...values)
          .run();
        break;

      case 'update':
        const setClauses = Object.keys(event.after!)
          .map(col => `${col} = ?`)
          .join(', ');
        const updateValues = [...Object.values(event.after!), event.key];
        await d1
          .prepare(`UPDATE ${event.table} SET ${setClauses} WHERE rowid = ?`)
          .bind(...updateValues)
          .run();
        break;

      case 'delete':
        await d1
          .prepare(`DELETE FROM ${event.table} WHERE rowid = ?`)
          .bind(event.key)
          .run();
        break;
    }
  }
}
```

**Step 3: Verify D1 Data Integrity**

```typescript
// Verify D1 is up to date
async function verifyRollback(
  d1: D1Database,
  dosql: Database
): Promise<boolean> {
  const tables = ['users', 'posts', 'comments'];

  for (const table of tables) {
    const d1Count = await d1
      .prepare(`SELECT COUNT(*) as c FROM ${table}`)
      .first<{ c: number }>();
    const dosqlCount = await dosql.queryOne<{ c: number }>(
      `SELECT COUNT(*) as c FROM ${table}`
    );

    if (d1Count?.c !== dosqlCount?.c) {
      console.error(
        `Count mismatch in ${table}: D1=${d1Count?.c}, DoSQL=${dosqlCount?.c}`
      );
      return false;
    }
  }

  return true;
}
```

### Data Sync Considerations

**During Dual-Write Period:**

- Both databases receive all writes
- Reads can be from either (based on primary setting)
- CDC captures all DoSQL changes for replay if needed

**After Full Migration:**

- Keep D1 instance running (read-only) for safety period
- Use CDC to continuously sync to D1 as backup
- Set retention policy for how long to maintain D1 sync

**Rollback Timing:**

| Rollback Type | Method | Data Loss Risk |
|---------------|--------|----------------|
| Immediate | Switch primary flag | Minor (in-flight requests) |
| Graceful | Sync remaining CDC events, then switch | None |
| Full | Re-migrate all data from DoSQL to D1 | None |

**Data Loss Prevention:**

```typescript
// Track migration state for safe rollback
interface MigrationState {
  phase: 'dual_write' | 'dosql_primary' | 'dosql_only' | 'rolled_back';
  d1LastLSN: bigint;
  dosqlLastLSN: bigint;
  lastSyncAt: string;
  rollbackAvailable: boolean;
}

async function getMigrationState(kv: KVNamespace): Promise<MigrationState> {
  const state = await kv.get('migration_state', 'json');
  return state as MigrationState || {
    phase: 'dual_write',
    d1LastLSN: 0n,
    dosqlLastLSN: 0n,
    lastSyncAt: new Date().toISOString(),
    rollbackAvailable: true,
  };
}

async function canRollback(state: MigrationState): Promise<boolean> {
  // Rollback is safe if:
  // 1. Still in dual_write or dosql_primary phase
  // 2. D1 is less than X hours behind DoSQL
  // 3. No schema changes applied that D1 doesn't have

  if (state.phase === 'dosql_only') {
    return false;  // D1 is decommissioned
  }

  const hoursBehind =
    (Number(state.dosqlLastLSN - state.d1LastLSN)) / 3600;
  return hoursBehind < 24;  // Allow rollback if less than 24 hours behind
}
```

---

## Summary

Migrating from D1 to DoSQL provides access to advanced features like CDC, time travel, and branching, but requires careful planning:

1. **Assess your needs** - Only migrate if you need DoSQL's advanced features
2. **Plan the migration** - Choose between full export, incremental sync, or dual-write
3. **Update your code** - Replace D1 bindings with DO stubs, adjust query syntax
4. **Test thoroughly** - Run both databases in parallel and verify consistency
5. **Have a rollback plan** - Keep D1 available during the transition period

For questions or issues, refer to:

- [Migration Checklist](./CHECKLIST.md)
- [Testing Guide](./testing.md)
- [Rollback Procedures](./rollback.md)
- [DoSQL Getting Started](../../packages/dosql/docs/getting-started.md)
- [DoSQL API Reference](../../packages/dosql/docs/api-reference.md)
