# Migrating from Turso to DoSQL

This guide provides comprehensive documentation for migrating your application from Turso (libSQL) to DoSQL. It covers feature comparisons, migration strategies, code changes, and testing procedures.

## Table of Contents

- [Overview](#overview)
  - [Why Migrate](#why-migrate)
  - [Architecture Differences](#architecture-differences)
  - [When to Migrate vs Stay on Turso](#when-to-migrate-vs-stay-on-turso)
- [Compatibility Matrix](#compatibility-matrix)
  - [SQL Dialect Differences](#sql-dialect-differences)
  - [Feature Comparison Table](#feature-comparison-table)
  - [API Compatibility Table](#api-compatibility-table)
- [Step-by-Step Migration](#step-by-step-migration)
  - [Schema Export from Turso](#schema-export-from-turso)
  - [Schema Import to DoSQL](#schema-import-to-dosql)
  - [Data Migration Strategies](#data-migration-strategies)
  - [Zero-Downtime Migration](#zero-downtime-migration)
- [Code Changes Required](#code-changes-required)
  - [Client Library Replacement](#client-library-replacement)
  - [Protocol Differences: HTTP API vs CapnWeb RPC](#protocol-differences-http-api-vs-capnweb-rpc)
  - [Query Syntax Adjustments](#query-syntax-adjustments)
  - [Transaction Handling Differences](#transaction-handling-differences)
  - [Error Handling Changes](#error-handling-changes)
- [Embedded Replicas Migration](#embedded-replicas-migration)
  - [Turso Embedded Replicas vs DoSQL Durable Objects](#turso-embedded-replicas-vs-dosql-durable-objects)
  - [Sync Strategy Migration](#sync-strategy-migration)
  - [Offline-First Patterns](#offline-first-patterns)
- [Vector Search Migration](#vector-search-migration)
  - [Turso Vector vs DoSQL Vector](#turso-vector-vs-dosql-vector)
  - [Index Migration](#index-migration)
  - [Query Migration](#query-migration)
- [Global Replication Migration](#global-replication-migration)
  - [Turso Global vs DoSQL Durable Objects](#turso-global-vs-dosql-durable-objects)
  - [Routing Strategies](#routing-strategies)
- [Testing the Migration](#testing-the-migration)
  - [Parallel Running Both Databases](#parallel-running-both-databases)
  - [Data Consistency Verification](#data-consistency-verification)
  - [Performance Comparison](#performance-comparison)
- [Rollback Strategy](#rollback-strategy)
  - [How to Roll Back](#how-to-roll-back)
  - [Data Sync Considerations](#data-sync-considerations)
- [Common Gotchas](#common-gotchas)

---

## Overview

### Why Migrate

DoSQL provides several advanced features that Turso does not offer, along with native Cloudflare Workers integration:

| Feature | Turso | DoSQL |
|---------|-------|-------|
| **Bundle Size** | ~500KB (WASM) | **7KB** (native TS) |
| **Change Data Capture (CDC)** | No | Yes - Real-time streaming to lakehouse |
| **Time Travel** | Limited (PITR) | Yes - Query data at any point in time |
| **Database Branching** | No | Yes - Git-like branching and merging |
| **Vector Search** | Yes (libsql-vector) | Yes - HNSW indexes, similar API |
| **Multi-Tenant Isolation** | Database per tenant | Per-tenant Durable Objects |
| **Real-time Subscriptions** | No | Yes - CDC subscriptions via RPC |
| **Tiered Storage** | No | Yes - Hot (DO) + Cold (R2) tiers |
| **Compile-time Type Safety** | No | Yes - Zero runtime SQL errors |
| **Protocol** | HTTP API / WebSocket | CapnWeb RPC (WebSocket/HTTP batch) |
| **Global Distribution** | Edge replicas (limited control) | Per-request routing to any region |
| **Embedded Replicas** | Yes (libSQL) | DO-native with automatic sync |
| **Custom Stored Procedures** | No | Yes - ESM-based procedures |

**Key advantages of migrating to DoSQL:**

1. **50-500x Smaller Bundle**: Pure TypeScript implementation vs WASM
2. **Compile-time Type Safety**: Catch SQL errors before deployment
3. **CDC for Event-Driven Architectures**: Stream changes to data warehouses or other systems
4. **Time Travel for Auditing**: Query historical data, undo mistakes, audit changes
5. **Branching for Safe Deployments**: Test schema changes on branches before merging
6. **Native Cloudflare Integration**: Designed specifically for DO/R2 constraints
7. **No External Network Calls**: Data lives in Durable Objects, reducing latency

### Architecture Differences

**Turso:**
```
+-------------------------------------------------------------+
|                         Turso                                |
+-------------------------------------------------------------+
|  +-------------+    +---------------+    +-----------------+ |
|  |   Client    |--->|   HTTP API    |--->|  Primary DB     | |
|  | (@libsql/   |    | (libsql://)   |    |  (Regional)     | |
|  |   client)   |    +---------------+    +-----------------+ |
|  +-------------+                                |             |
|        |                                        v             |
|        |                              +-----------------+     |
|        +--- (Embedded Replica) ------>|  Edge Replicas  |     |
|                                       |  (Read-only)    |     |
|                                       +-----------------+     |
|                                                               |
|  - Centralized primary with edge replicas                     |
|  - HTTP/WebSocket protocol (Hrana)                            |
|  - Embedded replicas sync via libSQL                          |
|  - Global replication managed by Turso                        |
+-------------------------------------------------------------+
```

**DoSQL:**
```
+-------------------------------------------------------------+
|                        DoSQL                                 |
+-------------------------------------------------------------+
|  +-------------+    +---------------+    +-----------------+ |
|  |   Client    |--->|  DO Binding   |--->|    Durable      | |
|  |(@dotdo/sql) |    |   (Stub)      |    |    Object       | |
|  +-------------+    +---------------+    +-----------------+ |
|                                               |              |
|                           +-------------------+--------+     |
|                           |                   |        |     |
|                           v                   v        v     |
|                     +----------+        +--------+           |
|                     | DO Store |        |   R2   |           |
|                     |  (Hot)   |        | (Cold) |           |
|                     +----------+        +--------+           |
|                           |                   |              |
|                           v                   v              |
|                     +---------------------------------+      |
|                     |         DoLake CDC              |      |
|                     |  (Parquet + Iceberg + REST)     |      |
|                     +---------------------------------+      |
|                                                              |
|  - Per-tenant Durable Objects                                |
|  - CapnWeb RPC (WebSocket/HTTP batch)                        |
|  - Strong consistency within DO                              |
|  - Automatic geographic distribution via Cloudflare          |
|  - Tiered storage (hot/cold)                                 |
+-------------------------------------------------------------+
```

### When to Migrate vs Stay on Turso

**Stay on Turso if:**
- You need 100% SQLite compatibility (libSQL is a superset)
- You're using Turso's managed infrastructure and don't want to self-manage
- You rely heavily on Turso's global replication with specific region requirements
- Your application is not on Cloudflare Workers
- You need Turso-specific features like Platform API or scoped tokens
- You want a managed service with SLA guarantees

**Migrate to DoSQL if:**
- You're building on Cloudflare Workers and want native integration
- Bundle size is critical (7KB vs ~500KB WASM)
- You need compile-time SQL type safety
- You need change data capture for event-driven architectures
- You require time travel queries for auditing or debugging
- You want database branching for safe schema migrations
- You have multi-tenant SaaS with strong isolation requirements
- You need real-time subscriptions to data changes
- You want tiered storage (hot/cold) for cost optimization
- You're hitting Turso's pricing limits

---

## Compatibility Matrix

### SQL Dialect Differences

Both Turso (libSQL) and DoSQL support SQLite-compatible SQL, with some differences:

| Feature | Turso (libSQL) | DoSQL | Notes |
|---------|----------------|-------|-------|
| `SELECT` | Yes | Yes | Fully compatible |
| `INSERT` | Yes | Yes | Fully compatible |
| `UPDATE` | Yes | Yes | Fully compatible |
| `DELETE` | Yes | Yes | Fully compatible |
| `CREATE TABLE` | Yes | Yes | Fully compatible |
| `CREATE INDEX` | Yes | Yes | Fully compatible |
| `ALTER TABLE` | Yes | Limited | Both have SQLite limitations |
| `PRAGMA` | libSQL PRAGMAs | DoSQL PRAGMAs | Different extensions |
| `VECTOR` columns | Yes | Yes | Different syntax (see below) |
| `FOR SYSTEM_TIME` | No | Yes | DoSQL time travel syntax |
| `CALL` | No | Yes | DoSQL stored procedures |
| `ALTER COLUMN` (libSQL) | Yes | No | libSQL extension |
| `RANDOM ROWID` | Yes | No | libSQL extension |

**Turso/libSQL-specific features not in DoSQL:**
- `ALTER COLUMN` - libSQL allows column type changes
- `RANDOM ROWID` - Random primary key generation
- Platform tokens and scoped access
- Native libSQL protocol (Hrana)

**DoSQL-specific SQL extensions:**

```sql
-- Time travel queries
SELECT * FROM users FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 12:00:00';
SELECT * FROM users FOR SYSTEM_TIME AS OF LSN 12345;
SELECT * FROM users FOR SYSTEM_TIME AS OF BRANCH 'feature-x';

-- Vector index creation (DoSQL syntax)
CREATE INDEX idx_embedding ON documents USING VECTOR (embedding)
WITH (dimensions = 1536, metric = 'cosine');

-- Vector similarity search
SELECT *, vector_distance_cos(embedding, ?) as distance FROM documents ORDER BY distance;

-- Custom PRAGMAs
PRAGMA dosql_wal_status;
PRAGMA dosql_branches;
PRAGMA dosql_current_lsn;
```

### Feature Comparison Table

| Turso Feature | DoSQL Equivalent | Migration Notes |
|---------------|------------------|-----------------|
| `createClient()` | `createWebSocketClient()` / `createHttpClient()` | Different import path |
| `client.execute()` | `client.query()` / `client.run()` | Renamed, same semantics |
| `client.batch()` | `client.transaction()` | Use transactions instead |
| `client.transaction()` | `client.transaction()` | Similar API |
| `client.sync()` | Automatic (DO-based) | Sync is automatic in DOs |
| Embedded replicas | Durable Object per tenant | Different architecture |
| Global replication | DO geographic routing | Cloudflare-managed |
| Vector (`F32_BLOB`) | `VECTOR` type | Similar, different syntax |
| `vector_distance_cos()` | `vector_distance_cos()` | Same function name |
| HTTP API | CapnWeb RPC | Protocol change |
| Hrana protocol | CapnWeb binary | More efficient serialization |
| Auth tokens | DO binding authentication | Cloudflare auth model |

### API Compatibility Table

| @libsql/client Method | @dotdo/sql Equivalent | Notes |
|----------------------|----------------------|-------|
| `createClient(config)` | `createWebSocketClient(config)` | Different config shape |
| `client.execute(sql)` | `client.query(sql)` / `client.run(sql)` | `query` for SELECT, `run` for mutations |
| `client.execute({ sql, args })` | `client.query(sql, params)` | Params as second argument |
| `client.batch(statements, 'write')` | `client.transaction(async (tx) => {...})` | Callback-based transactions |
| `client.transaction('write')` | `client.transaction(async (tx) => {...})` | Same isolation level |
| `tx.execute(stmt)` | `tx.query(sql, params)` / `tx.run(sql, params)` | Inside transaction context |
| `tx.commit()` | Automatic on callback success | Implicit commit |
| `tx.rollback()` | Automatic on callback error | Implicit rollback |
| `client.sync()` | N/A (automatic) | DOs sync automatically |
| `client.close()` | `client.close()` | Same |
| `result.rows` | Direct array return | No `.rows` wrapper |
| `result.rowsAffected` | `result.rowsAffected` | Same |
| `result.lastInsertRowid` | `result.lastInsertRowId` | Slight naming difference |

---

## Step-by-Step Migration

### Schema Export from Turso

Export your Turso schema using the Turso CLI or programmatically:

**Option 1: Turso CLI**

```bash
# List your databases
turso db list

# Connect to database shell
turso db shell my-database

# In the shell, export schema
.schema > schema.sql
```

**Option 2: Programmatic Export**

```typescript
import { createClient } from '@libsql/client';

async function exportTursoSchema(client: ReturnType<typeof createClient>): Promise<string> {
  // Get all tables
  const tables = await client.execute(
    "SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'libsql_%'"
  );

  // Get all indexes
  const indexes = await client.execute(
    "SELECT name, sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL"
  );

  // Get all triggers (if any)
  const triggers = await client.execute(
    "SELECT name, sql FROM sqlite_master WHERE type='trigger'"
  );

  let schema = '-- Tables\n';
  for (const table of tables.rows) {
    schema += `${table.sql};\n\n`;
  }

  schema += '-- Indexes\n';
  for (const index of indexes.rows) {
    if (index.sql) {
      schema += `${index.sql};\n\n`;
    }
  }

  if (triggers.rows.length > 0) {
    schema += '-- Triggers (NOTE: DoSQL uses CDC instead of triggers)\n';
    for (const trigger of triggers.rows) {
      schema += `-- ${trigger.sql};\n`;
    }
  }

  return schema;
}

// Usage
const tursoClient = createClient({
  url: 'libsql://my-database.turso.io',
  authToken: process.env.TURSO_AUTH_TOKEN,
});

const schema = await exportTursoSchema(tursoClient);
console.log(schema);
```

### Schema Import to DoSQL

**Option 1: Migration Files (Recommended)**

Create migration files in `.do/migrations/`:

```
your-project/
+-- .do/
|   +-- migrations/
|       +-- 001_initial_schema.sql    # Your Turso schema
|       +-- 002_add_dosql_features.sql # DoSQL enhancements
|       +-- ...
```

```sql
-- .do/migrations/001_initial_schema.sql
-- Imported from Turso

CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT UNIQUE,
  active BOOLEAN DEFAULT true,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE documents (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  title TEXT NOT NULL,
  content TEXT,
  -- Vector column (converted from Turso F32_BLOB)
  embedding BLOB,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX idx_documents_user_id ON documents(user_id);
```

```sql
-- .do/migrations/002_add_dosql_features.sql
-- Optional: Add DoSQL-specific features

-- Create vector index (DoSQL syntax)
CREATE INDEX idx_documents_embedding ON documents
USING VECTOR (embedding)
WITH (dimensions = 1536, metric = 'cosine');
```

**Option 2: Inline Migrations**

```typescript
import { DB } from '@dotdo/dosql';

const tursoSchema = `
  CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE
  );
`;

const db = await DB('my-tenant', {
  migrations: [
    { id: '001_turso_import', sql: tursoSchema },
  ],
});
```

### Data Migration Strategies

**Strategy 1: Full Export/Import (Simple, Downtime Required)**

```typescript
import { createClient } from '@libsql/client';
import { DB, Database } from '@dotdo/dosql';

// Step 1: Export all data from Turso
async function exportTursoData(
  turso: ReturnType<typeof createClient>
): Promise<Map<string, Record<string, unknown>[]>> {
  const data = new Map<string, Record<string, unknown>[]>();

  // Get all tables
  const tables = await turso.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'libsql_%'"
  );

  // Export each table
  for (const tableRow of tables.rows) {
    const tableName = tableRow.name as string;
    const rows = await turso.execute(`SELECT * FROM "${tableName}"`);

    // Convert libSQL row format to plain objects
    const plainRows = rows.rows.map((row) => {
      const obj: Record<string, unknown> = {};
      for (let i = 0; i < rows.columns.length; i++) {
        obj[rows.columns[i]] = row[i];
      }
      return obj;
    });

    data.set(tableName, plainRows);
  }

  return data;
}

// Step 2: Import into DoSQL
async function importToDoSQL(
  dosql: Database,
  data: Map<string, Record<string, unknown>[]>
): Promise<void> {
  await dosql.transaction(async (tx) => {
    for (const [table, rows] of data.entries()) {
      for (const row of rows) {
        const columns = Object.keys(row);
        const placeholders = columns.map(() => '?').join(', ');
        const values = Object.values(row);

        await tx.run(
          `INSERT INTO "${table}" (${columns.map(c => `"${c}"`).join(', ')}) VALUES (${placeholders})`,
          values
        );
      }
    }
  });
}

// Migration script
async function migrate(): Promise<void> {
  const turso = createClient({
    url: 'libsql://my-database.turso.io',
    authToken: process.env.TURSO_AUTH_TOKEN,
  });

  const dosql = await DB('my-tenant');

  console.log('Exporting data from Turso...');
  const data = await exportTursoData(turso);

  console.log(`Importing ${data.size} tables to DoSQL...`);
  await importToDoSQL(dosql, data);

  console.log('Migration complete!');

  turso.close();
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
  turso: ReturnType<typeof createClient>,
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

  // Fetch new records from Turso
  const newRows = await turso.execute({
    sql: `
      SELECT * FROM users
      WHERE id > ?
      ORDER BY id ASC
      LIMIT ?
    `,
    args: [syncState.lastSyncedId, batchSize],
  });

  if (newRows.rows.length === 0) {
    console.log('No new records to sync');
    return;
  }

  // Insert into DoSQL
  await dosql.transaction(async (tx) => {
    for (const row of newRows.rows) {
      await tx.run(
        'INSERT OR REPLACE INTO users (id, name, email, created_at) VALUES (?, ?, ?, ?)',
        [row.id, row.name, row.email, row.created_at]
      );
    }

    // Update sync state
    const lastRow = newRows.rows[newRows.rows.length - 1];
    await tx.run(
      `INSERT OR REPLACE INTO _sync_state (table_name, last_synced_id, last_synced_at)
       VALUES (?, ?, datetime('now'))`,
      ['users', lastRow.id]
    );
  });

  console.log(`Synced ${newRows.rows.length} records`);
}
```

**Strategy 3: Dual-Write During Transition**

```typescript
class DualWriteDatabase {
  constructor(
    private turso: ReturnType<typeof createClient>,
    private dosql: Database,
    private primaryIsDoSQL: boolean = false
  ) {}

  async run(sql: string, params?: unknown[]): Promise<void> {
    if (this.primaryIsDoSQL) {
      // Write to DoSQL first, then Turso
      await this.dosql.run(sql, params);
      try {
        await this.turso.execute({ sql, args: params as InValue[] });
      } catch (error) {
        console.error('Turso secondary write failed:', error);
        // Continue - DoSQL is primary
      }
    } else {
      // Write to Turso first, then DoSQL
      await this.turso.execute({ sql, args: params as InValue[] });
      try {
        await this.dosql.run(sql, params);
      } catch (error) {
        console.error('DoSQL secondary write failed:', error);
        // Continue - Turso is primary
      }
    }
  }

  async query<T>(sql: string, params?: unknown[]): Promise<T[]> {
    // Always read from primary
    if (this.primaryIsDoSQL) {
      return this.dosql.query<T>(sql, params);
    } else {
      const result = await this.turso.execute({ sql, args: params as InValue[] });
      // Convert Turso result format to plain objects
      return result.rows.map((row) => {
        const obj: Record<string, unknown> = {};
        for (let i = 0; i < result.columns.length; i++) {
          obj[result.columns[i]] = row[i];
        }
        return obj as T;
      });
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
+-------------------------------------------------------------+
|  1. Deploy DoSQL infrastructure (Durable Objects)            |
|  2. Create schema in DoSQL (run migrations)                  |
|  3. Set up dual-write with Turso as primary                  |
|  4. Start incremental data sync from Turso to DoSQL          |
+-------------------------------------------------------------+
                            |
                            v
Phase 2: Sync & Verify (No downtime)
+-------------------------------------------------------------+
|  5. Continue dual-write until DoSQL catches up               |
|  6. Run data consistency verification                        |
|  7. Compare query results between Turso and DoSQL            |
|  8. Monitor for discrepancies                                |
+-------------------------------------------------------------+
                            |
                            v
Phase 3: Cutover (Brief read-only window)
+-------------------------------------------------------------+
|  9. Enable read-only mode briefly                            |
|  10. Final sync of any remaining records                     |
|  11. Switch primary to DoSQL                                 |
|  12. Resume normal operations                                |
+-------------------------------------------------------------+
                            |
                            v
Phase 4: Cleanup (No downtime)
+-------------------------------------------------------------+
|  13. Continue dual-write to Turso for safety period          |
|  14. Monitor for issues                                      |
|  15. Remove Turso writes after confidence period             |
|  16. Decommission Turso database                             |
+-------------------------------------------------------------+
```

---

## Code Changes Required

### Client Library Replacement

**Turso (Before):**

```typescript
// Install: npm install @libsql/client
import { createClient, InValue } from '@libsql/client';

const client = createClient({
  url: 'libsql://my-database.turso.io',
  authToken: process.env.TURSO_AUTH_TOKEN,
});

// Simple query
const users = await client.execute('SELECT * FROM users WHERE active = ?', [true]);
console.log(users.rows);

// Batch operations
const results = await client.batch([
  { sql: 'INSERT INTO users (name) VALUES (?)', args: ['Alice'] },
  { sql: 'INSERT INTO users (name) VALUES (?)', args: ['Bob'] },
], 'write');

// Transaction
const tx = await client.transaction('write');
try {
  await tx.execute({ sql: 'INSERT INTO orders (user_id) VALUES (?)', args: [1] });
  await tx.execute({ sql: 'UPDATE inventory SET stock = stock - 1 WHERE id = ?', args: [42] });
  await tx.commit();
} catch (error) {
  await tx.rollback();
  throw error;
}

client.close();
```

**DoSQL (After):**

```typescript
// Install: npm install @dotdo/sql.do @dotdo/dosql
import { createWebSocketClient } from '@dotdo/dosql/rpc';

const client = await createWebSocketClient({
  url: 'wss://my-worker.example.com/db',
  // Authentication handled by Cloudflare bindings
});

// Simple query - returns array directly (no .rows)
const users = await client.query<User>('SELECT * FROM users WHERE active = ?', [true]);
console.log(users); // Direct array

// Transaction-based batch operations
await client.transaction(async (tx) => {
  await tx.run('INSERT INTO users (name) VALUES (?)', ['Alice']);
  await tx.run('INSERT INTO users (name) VALUES (?)', ['Bob']);
  // Automatic commit on success
});

// Transaction with return value
const orderId = await client.transaction(async (tx) => {
  const result = await tx.run('INSERT INTO orders (user_id) VALUES (?)', [1]);
  await tx.run('UPDATE inventory SET stock = stock - 1 WHERE id = ?', [42]);
  return result.lastInsertRowId;
  // Automatic commit on success, rollback on error
});

client.close();
```

### Protocol Differences: HTTP API vs CapnWeb RPC

**Turso uses HTTP/WebSocket with Hrana protocol:**
- JSON-based messages over HTTP or WebSocket
- Request/response model
- Separate connection for each request (HTTP) or persistent (WebSocket)

**DoSQL uses CapnWeb RPC:**
- Binary serialization (more efficient than JSON)
- Promise pipelining for reduced round trips
- Native WebSocket with hibernation support
- HTTP batch mode for simple operations

```typescript
// Turso HTTP API example
const response = await fetch('https://my-database.turso.io/v2/pipeline', {
  method: 'POST',
  headers: {
    'Authorization': `Bearer ${authToken}`,
    'Content-Type': 'application/json',
  },
  body: JSON.stringify({
    requests: [
      { type: 'execute', stmt: { sql: 'SELECT * FROM users' } },
    ],
  }),
});

// DoSQL CapnWeb RPC (via client SDK)
import { createHttpClient } from '@dotdo/dosql/rpc';

const client = createHttpClient({
  url: 'https://my-worker.example.com/db',
});

// Automatic serialization and deserialization
const users = await client.query('SELECT * FROM users');
```

**Worker-side handling:**

```typescript
// wrangler.toml
// [durable_objects]
// bindings = [
//   { name = "DOSQL_DB", class_name = "TenantDatabase" }
// ]

import { DoSQLTarget, handleDoSQLRequest, isDoSQLRequest } from '@dotdo/dosql/rpc';
import { DB, Database } from '@dotdo/dosql';

export interface Env {
  DOSQL_DB: DurableObjectNamespace;
}

export { TenantDatabase } from './database';

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Get tenant ID from request
    const tenantId = getTenantId(request);
    const id = env.DOSQL_DB.idFromName(tenantId);
    const stub = env.DOSQL_DB.get(id);

    // Forward to Durable Object
    return stub.fetch(request);
  },
};

// database.ts
export class TenantDatabase implements DurableObject {
  private db: Database | null = null;
  private state: DurableObjectState;
  private rpcTarget: DoSQLTarget | null = null;

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
    // Handle RPC requests
    if (isDoSQLRequest(request)) {
      if (!this.rpcTarget) {
        const db = await this.getDB();
        this.rpcTarget = new DoSQLTarget(db);
      }
      return handleDoSQLRequest(request, this.rpcTarget);
    }

    // Handle REST requests
    const url = new URL(request.url);
    const db = await this.getDB();

    if (url.pathname === '/users') {
      const users = await db.query('SELECT * FROM users WHERE active = ?', [true]);
      return Response.json(users);
    }

    return new Response('Not Found', { status: 404 });
  }
}
```

### Query Syntax Adjustments

**Prepared Statements:**

```typescript
// Turso - using args object
const stmt = await client.execute({
  sql: 'SELECT * FROM users WHERE id = ?',
  args: [42],
});

// DoSQL - params as second argument
const user = await client.queryOne('SELECT * FROM users WHERE id = ?', [42]);
```

**Named Parameters:**

```typescript
// Turso - named params with $
const stmt = await client.execute({
  sql: 'SELECT * FROM users WHERE id = $id AND active = $active',
  args: { $id: 42, $active: true },
});

// DoSQL - named params with :
const user = await client.queryOne(
  'SELECT * FROM users WHERE id = :id AND active = :active',
  { id: 42, active: true }
);
```

**Batch Operations:**

```typescript
// Turso
const results = await client.batch([
  { sql: 'INSERT INTO users (name) VALUES (?)', args: ['Alice'] },
  { sql: 'INSERT INTO users (name) VALUES (?)', args: ['Bob'] },
], 'write');

// DoSQL - Use transactions instead
await client.transaction(async (tx) => {
  await tx.run('INSERT INTO users (name) VALUES (?)', ['Alice']);
  await tx.run('INSERT INTO users (name) VALUES (?)', ['Bob']);
});
```

**Interactive Transactions:**

```typescript
// Turso
const tx = await client.transaction('write');
try {
  const result = await tx.execute({ sql: 'INSERT INTO users (name) VALUES (?)', args: ['Alice'] });
  const userId = result.lastInsertRowid;
  await tx.execute({ sql: 'INSERT INTO profiles (user_id) VALUES (?)', args: [userId] });
  await tx.commit();
} catch {
  await tx.rollback();
}

// DoSQL - callback-based with automatic commit/rollback
const userId = await client.transaction(async (tx) => {
  const result = await tx.run('INSERT INTO users (name) VALUES (?)', ['Alice']);
  await tx.run('INSERT INTO profiles (user_id) VALUES (?)', [result.lastInsertRowId]);
  return result.lastInsertRowId;
  // Automatic commit on success, rollback on error
});
```

### Transaction Handling Differences

**Turso Transaction Model:**
- Explicit `transaction()` call returns transaction handle
- Manual `commit()` and `rollback()` calls
- Must remember to rollback in catch blocks
- `'write'` and `'read'` modes

**DoSQL Transaction Model:**
- Callback-based transactions
- Automatic commit on callback success
- Automatic rollback on callback error
- Supports savepoints for nested transactions

```typescript
// DoSQL transactions with savepoints
await client.transaction(async (tx) => {
  await tx.run('INSERT INTO orders (user_id) VALUES (?)', [1]);

  await tx.savepoint('items', async (sp) => {
    await sp.run('INSERT INTO order_items (order_id, product_id) VALUES (?, ?)', [1, 100]);
    // Can rollback just this savepoint without affecting outer transaction
    if (someCondition) {
      throw new Error('Rollback items only');
    }
  });

  await tx.run('UPDATE inventory SET stock = stock - 1 WHERE id = ?', [100]);
});

// DoSQL transaction modes
await client.transaction(async (tx) => { ... }, { mode: 'IMMEDIATE' });
await client.transaction(async (tx) => { ... }, { mode: 'EXCLUSIVE' });
```

### Error Handling Changes

**Turso Error Handling:**

```typescript
import { LibsqlError } from '@libsql/client';

try {
  await client.execute({
    sql: 'INSERT INTO users (email) VALUES (?)',
    args: ['duplicate@test.com'],
  });
} catch (error) {
  if (error instanceof LibsqlError) {
    if (error.code === 'SQLITE_CONSTRAINT_UNIQUE') {
      // Handle unique constraint violation
    }
  }
  throw error;
}
```

**DoSQL Error Handling:**

```typescript
import { TransactionError, TransactionErrorCode } from '@dotdo/dosql/transaction';

try {
  await client.run('INSERT INTO users (email) VALUES (?)', ['duplicate@test.com']);
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
```

---

## Embedded Replicas Migration

### Turso Embedded Replicas vs DoSQL Durable Objects

**Turso Embedded Replicas:**
- Local SQLite file synchronized with remote primary
- Read locally, write to remote primary
- Manual `sync()` calls for consistency
- Works in Node.js and local environments
- Requires libSQL driver with embedded support

**DoSQL Durable Objects:**
- Each tenant has its own Durable Object instance
- Cloudflare automatically routes to the same DO instance
- Strong consistency within the DO
- No manual sync - writes are immediately consistent
- Works natively in Cloudflare Workers

```typescript
// Turso Embedded Replica
import { createClient } from '@libsql/client';

const client = createClient({
  url: 'file:./local.db',  // Local SQLite file
  syncUrl: 'libsql://my-database.turso.io',  // Remote primary
  authToken: process.env.TURSO_AUTH_TOKEN,
  syncInterval: 60,  // Sync every 60 seconds
});

// Read locally (fast)
const users = await client.execute('SELECT * FROM users');

// Write goes to remote, then syncs back
await client.execute({ sql: 'INSERT INTO users (name) VALUES (?)', args: ['Alice'] });

// Manual sync for immediate consistency
await client.sync();

// DoSQL Durable Object (equivalent)
// No separate setup needed - DOs provide this natively

// In your Worker
export class TenantDatabase implements DurableObject {
  private db: Database | null = null;

  async fetch(request: Request): Promise<Response> {
    const db = await this.getDB();

    // All operations are local to the DO - no sync needed
    const users = await db.query('SELECT * FROM users');

    // Writes are immediately consistent within the DO
    await db.run('INSERT INTO users (name) VALUES (?)', ['Alice']);

    // No sync() needed - DO storage is always consistent
    return Response.json(users);
  }
}
```

### Sync Strategy Migration

**Turso Sync Patterns:**

```typescript
// Turso: Background sync with periodic intervals
const client = createClient({
  url: 'file:./local.db',
  syncUrl: 'libsql://my-database.turso.io',
  syncInterval: 60000,  // 60 seconds
});

// Or manual sync after important writes
await client.execute({ sql: 'INSERT INTO ...', args: [] });
await client.sync();  // Ensure sync to primary
```

**DoSQL: No Manual Sync Needed**

```typescript
// DoSQL: Durable Objects provide automatic consistency
// Each DO instance is the source of truth for its data

// For multi-region reads, use Cloudflare's smart routing
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const tenantId = getTenantId(request);

    // Cloudflare routes to the DO's colocated location
    // Write requests go to the DO's primary location
    // Read requests can be served from cache or DO

    const id = env.DOSQL_DB.idFromName(tenantId);
    const stub = env.DOSQL_DB.get(id);
    return stub.fetch(request);
  },
};
```

### Offline-First Patterns

If you were using Turso embedded replicas for offline-first functionality, consider these DoSQL alternatives:

**Option 1: Client-side caching with sync**

```typescript
// Client-side cache with periodic sync to DoSQL
class OfflineFirstClient {
  private cache: Map<string, unknown[]> = new Map();
  private pendingWrites: Array<{ sql: string; params: unknown[] }> = [];

  async query<T>(sql: string): Promise<T[]> {
    // Try cache first
    const cacheKey = sql;
    if (this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey) as T[];
    }

    // Fetch from DoSQL
    const result = await this.client.query<T>(sql);
    this.cache.set(cacheKey, result);
    return result;
  }

  async write(sql: string, params: unknown[]): Promise<void> {
    // Queue for later sync
    this.pendingWrites.push({ sql, params });

    // Try to sync immediately if online
    if (navigator.onLine) {
      await this.sync();
    }
  }

  async sync(): Promise<void> {
    if (this.pendingWrites.length === 0) return;

    await this.client.transaction(async (tx) => {
      for (const write of this.pendingWrites) {
        await tx.run(write.sql, write.params);
      }
    });

    this.pendingWrites = [];
    this.cache.clear();  // Invalidate cache after sync
  }
}
```

**Option 2: Use Cloudflare Workers as the offline boundary**

```typescript
// The Worker itself acts as the "local" layer
// Data is always stored in Durable Objects
// Use caching for read performance

export class TenantDatabase implements DurableObject {
  private cache = new Map<string, { data: unknown[]; timestamp: number }>();
  private readonly CACHE_TTL = 60000; // 1 minute

  async query<T>(sql: string, params?: unknown[]): Promise<T[]> {
    const cacheKey = JSON.stringify({ sql, params });
    const cached = this.cache.get(cacheKey);

    if (cached && Date.now() - cached.timestamp < this.CACHE_TTL) {
      return cached.data as T[];
    }

    const result = await this.db.query<T>(sql, params);
    this.cache.set(cacheKey, { data: result, timestamp: Date.now() });
    return result;
  }
}
```

---

## Vector Search Migration

### Turso Vector vs DoSQL Vector

Both Turso and DoSQL support vector search, but with different syntax:

| Feature | Turso (libsql-vector) | DoSQL |
|---------|----------------------|-------|
| Vector type | `F32_BLOB(dimensions)` | `VECTOR(dimensions)` / `BLOB` |
| Distance cosine | `vector_distance_cos(a, b)` | `vector_distance_cos(a, b)` |
| Distance L2 | `vector_distance_l2(a, b)` | `vector_distance_l2(a, b)` |
| Vector extract | `vector_extract(v)` | `vector_to_json(v)` |
| Index type | `libsql_vector_idx` | `HNSW` |
| Index creation | `CREATE INDEX ... USING libsql_vector_idx` | `CREATE INDEX ... USING VECTOR` |

### Index Migration

**Turso Vector Index:**

```sql
-- Turso: Create table with vector column
CREATE TABLE documents (
  id INTEGER PRIMARY KEY,
  content TEXT,
  embedding F32_BLOB(1536)  -- 1536 dimensions for OpenAI embeddings
);

-- Turso: Create vector index
CREATE INDEX idx_documents_embedding ON documents (
  libsql_vector_idx(embedding)
);
```

**DoSQL Vector Index:**

```sql
-- DoSQL: Create table with vector column
CREATE TABLE documents (
  id INTEGER PRIMARY KEY,
  content TEXT,
  embedding BLOB  -- Store as BLOB
);

-- DoSQL: Create vector index
CREATE INDEX idx_documents_embedding ON documents
USING VECTOR (embedding)
WITH (dimensions = 1536, metric = 'cosine');
```

### Query Migration

**Turso Vector Search:**

```typescript
// Turso: Vector similarity search
const embedding = new Float32Array([0.1, 0.2, ...]); // 1536 dimensions

const results = await client.execute({
  sql: `
    SELECT id, content, vector_distance_cos(embedding, ?) as distance
    FROM documents
    ORDER BY distance
    LIMIT 10
  `,
  args: [embedding],
});
```

**DoSQL Vector Search:**

```typescript
import { vector_distance_cos } from '@dotdo/dosql/vector';

// DoSQL: Vector similarity search
const embedding = new Float32Array([0.1, 0.2, ...]); // 1536 dimensions

// Option 1: SQL-based (for simple queries)
const results = await client.query(`
  SELECT id, content, vector_distance_cos(embedding, ?) as distance
  FROM documents
  ORDER BY distance
  LIMIT 10
`, [embedding]);

// Option 2: Using VectorColumn for advanced features
import { VectorColumn, hybridSearch } from '@dotdo/dosql/vector';

const vectorCol = new VectorColumn({
  dimensions: 1536,
  metric: 'cosine',
  hnswConfig: { M: 16, efConstruction: 200, efSearch: 100 },
});

// Hybrid search (vector + SQL filter)
const results = await hybridSearch(db, {
  vectorColumn: vectorCol,
  query: embedding,
  k: 10,
  filter: 'category = ?',
  filterParams: ['technology'],
});
```

**Vector Data Migration:**

```typescript
// Migrate vector data from Turso to DoSQL
async function migrateVectors(
  turso: ReturnType<typeof createClient>,
  dosql: Database
): Promise<void> {
  // Export vectors from Turso
  const documents = await turso.execute(`
    SELECT id, content, vector_extract(embedding) as embedding_json
    FROM documents
  `);

  // Import to DoSQL
  await dosql.transaction(async (tx) => {
    for (const doc of documents.rows) {
      // Parse the JSON array and convert to Float32Array
      const embeddingArray = JSON.parse(doc.embedding_json as string);
      const embedding = new Float32Array(embeddingArray);

      await tx.run(
        'INSERT INTO documents (id, content, embedding) VALUES (?, ?, ?)',
        [doc.id, doc.content, embedding]
      );
    }
  });
}
```

---

## Global Replication Migration

### Turso Global vs DoSQL Durable Objects

**Turso Global Replication:**
- Database replicated to multiple edge locations
- Automatic read routing to nearest replica
- Writes go to primary, replicated asynchronously
- You choose regions during database creation
- Read replicas are eventually consistent

**DoSQL Durable Objects:**
- Each DO instance has a "home" location
- Cloudflare routes requests to the DO's location
- Strong consistency (no eventual consistency concerns)
- DO location determined by first write
- Can use DO hints for location control

### Routing Strategies

**Turso: Automatic Global Routing**

```typescript
// Turso handles routing automatically
const client = createClient({
  url: 'libsql://my-database.turso.io',  // Routes to nearest edge
  authToken: process.env.TURSO_AUTH_TOKEN,
});
```

**DoSQL: Cloudflare-managed Routing**

```typescript
// DoSQL: DOs are automatically colocated with their first writer

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const tenantId = getTenantId(request);

    // Option 1: Standard DO routing (colocated with first writer)
    const id = env.DOSQL_DB.idFromName(tenantId);
    const stub = env.DOSQL_DB.get(id);

    // Option 2: Location hints for new DOs
    // Useful for multi-region deployments
    const locationHint = getLocationHint(request);  // e.g., 'enam', 'weur', 'apac'
    const idWithHint = env.DOSQL_DB.idFromName(tenantId);
    const stubWithHint = env.DOSQL_DB.get(idWithHint, { locationHint });

    return stub.fetch(request);
  },
};

function getLocationHint(request: Request): string {
  // Determine region based on user location or preference
  const cf = request.cf;
  if (!cf) return 'enam';

  const continent = cf.continent;
  switch (continent) {
    case 'NA': return 'enam';
    case 'EU': return 'weur';
    case 'AS': return 'apac';
    default: return 'enam';
  }
}
```

**Multi-Region Pattern for DoSQL:**

```typescript
// For global applications, use multiple DOs with regional sharding
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const tenantId = getTenantId(request);
    const region = getRegion(request);

    // Create region-specific DO ID
    const doId = `${tenantId}:${region}`;
    const id = env.DOSQL_DB.idFromName(doId);
    const stub = env.DOSQL_DB.get(id);

    // For writes that need global consistency, use a primary region
    if (isWriteRequest(request)) {
      const primaryDoId = `${tenantId}:primary`;
      const primaryId = env.DOSQL_DB.idFromName(primaryDoId);
      const primaryStub = env.DOSQL_DB.get(primaryId);
      return primaryStub.fetch(request);
    }

    return stub.fetch(request);
  },
};
```

---

## Testing the Migration

### Parallel Running Both Databases

```typescript
// Test harness for comparing Turso and DoSQL
class MigrationTestHarness {
  constructor(
    private turso: ReturnType<typeof createClient>,
    private dosql: Database,
    private logger: Logger
  ) {}

  async compareQuery<T>(
    sql: string,
    params?: unknown[]
  ): Promise<{ match: boolean; turso: T[]; dosql: T[] }> {
    // Query Turso
    const tursoResult = await this.turso.execute({
      sql,
      args: params as InValue[],
    });

    // Convert Turso result to plain objects
    const tursoRows = tursoResult.rows.map((row) => {
      const obj: Record<string, unknown> = {};
      for (let i = 0; i < tursoResult.columns.length; i++) {
        obj[tursoResult.columns[i]] = row[i];
      }
      return obj as T;
    });

    // Query DoSQL
    const dosqlRows = await this.dosql.query<T>(sql, params);

    // Compare results
    const tursoSorted = this.sortResults(tursoRows);
    const dosqlSorted = this.sortResults(dosqlRows);

    const match = JSON.stringify(tursoSorted) === JSON.stringify(dosqlSorted);

    if (!match) {
      this.logger.warn('Query mismatch', {
        sql,
        params,
        tursoCount: tursoRows.length,
        dosqlCount: dosqlRows.length,
      });
    }

    return { match, turso: tursoRows, dosql: dosqlRows };
  }

  async runComparisonSuite(): Promise<ComparisonReport> {
    const results: QueryComparison[] = [];

    // Test all tables
    const tables = ['users', 'documents', 'orders'];
    for (const table of tables) {
      const comparison = await this.compareQuery(`SELECT * FROM ${table}`);
      results.push({ table, ...comparison });
    }

    // Test specific queries
    const queries = [
      { sql: 'SELECT COUNT(*) as count FROM users', params: [] },
      { sql: 'SELECT * FROM users WHERE active = ?', params: [true] },
      { sql: 'SELECT u.*, COUNT(d.id) as doc_count FROM users u LEFT JOIN documents d ON d.user_id = u.id GROUP BY u.id', params: [] },
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
async function verifyDataConsistency(
  turso: ReturnType<typeof createClient>,
  dosql: Database
): Promise<ConsistencyReport> {
  const issues: ConsistencyIssue[] = [];

  // Get all tables from Turso
  const tursoTables = await turso.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'libsql_%'"
  );

  for (const tableRow of tursoTables.rows) {
    const table = tableRow.name as string;

    // Compare row counts
    const tursoCount = await turso.execute(`SELECT COUNT(*) as count FROM "${table}"`);
    const dosqlCount = await dosql.queryOne<{ count: number }>(`SELECT COUNT(*) as count FROM "${table}"`);

    if (Number(tursoCount.rows[0].count) !== dosqlCount?.count) {
      issues.push({
        type: 'row_count_mismatch',
        table,
        tursoValue: Number(tursoCount.rows[0].count),
        dosqlValue: dosqlCount?.count,
      });
    }

    // Compare checksums (for small tables)
    if (Number(tursoCount.rows[0].count) < 10000) {
      const tursoChecksum = await computeTableChecksum(turso, table);
      const dosqlChecksum = await computeTableChecksumDoSQL(dosql, table);

      if (tursoChecksum !== dosqlChecksum) {
        issues.push({
          type: 'checksum_mismatch',
          table,
          tursoValue: tursoChecksum,
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
```

### Performance Comparison

```typescript
interface BenchmarkResult {
  operation: string;
  turso: { avgMs: number; p95Ms: number; p99Ms: number };
  dosql: { avgMs: number; p95Ms: number; p99Ms: number };
  winner: 'turso' | 'dosql' | 'tie';
}

async function runPerformanceBenchmark(
  turso: ReturnType<typeof createClient>,
  dosql: Database,
  iterations: number = 100
): Promise<BenchmarkResult[]> {
  const results: BenchmarkResult[] = [];

  // Benchmark: Simple SELECT
  results.push(await benchmarkOperation(
    'Simple SELECT',
    async () => {
      await turso.execute({ sql: 'SELECT * FROM users WHERE id = ?', args: [1] });
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
      await turso.execute({
        sql: `
          SELECT u.*, COUNT(d.id) as doc_count
          FROM users u
          LEFT JOIN documents d ON d.user_id = u.id
          WHERE u.active = ?
          GROUP BY u.id
          LIMIT 10
        `,
        args: [true],
      });
    },
    async () => {
      await dosql.query(`
        SELECT u.*, COUNT(d.id) as doc_count
        FROM users u
        LEFT JOIN documents d ON d.user_id = u.id
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
      await turso.execute({ sql: 'INSERT INTO benchmark_test (value) VALUES (?)', args: [Math.random()] });
    },
    async () => {
      await dosql.run('INSERT INTO benchmark_test (value) VALUES (?)', [Math.random()]);
    },
    iterations
  ));

  // Benchmark: Transaction
  results.push(await benchmarkOperation(
    'Transaction (3 operations)',
    async () => {
      const tx = await turso.transaction('write');
      await tx.execute({ sql: 'INSERT INTO benchmark_test (value) VALUES (?)', args: [1] });
      await tx.execute({ sql: 'INSERT INTO benchmark_test (value) VALUES (?)', args: [2] });
      await tx.execute({ sql: 'INSERT INTO benchmark_test (value) VALUES (?)', args: [3] });
      await tx.commit();
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

  // Benchmark: Vector Search (if applicable)
  results.push(await benchmarkOperation(
    'Vector Search (k=10)',
    async () => {
      const queryVector = new Float32Array(1536).fill(0.1);
      await turso.execute({
        sql: 'SELECT id, vector_distance_cos(embedding, ?) as dist FROM documents ORDER BY dist LIMIT 10',
        args: [queryVector],
      });
    },
    async () => {
      const queryVector = new Float32Array(1536).fill(0.1);
      await dosql.query(
        'SELECT id, vector_distance_cos(embedding, ?) as dist FROM documents ORDER BY dist LIMIT 10',
        [queryVector]
      );
    },
    iterations
  ));

  return results;
}
```

---

## Rollback Strategy

### How to Roll Back

If issues occur after migrating to DoSQL, you can roll back to Turso:

**Step 1: Re-enable Turso Writes**

```typescript
class RollbackEnabledDatabase {
  private useTurso: boolean = true;  // Rollback flag

  constructor(
    private turso: ReturnType<typeof createClient>,
    private dosql: Database
  ) {}

  async rollbackToTurso(): void {
    this.useTurso = true;
    console.log('Rolled back to Turso');
  }

  async query<T>(sql: string, params?: unknown[]): Promise<T[]> {
    if (this.useTurso) {
      const result = await this.turso.execute({ sql, args: params as InValue[] });
      return result.rows.map((row) => {
        const obj: Record<string, unknown> = {};
        for (let i = 0; i < result.columns.length; i++) {
          obj[result.columns[i]] = row[i];
        }
        return obj as T;
      });
    }
    return this.dosql.query<T>(sql, params);
  }

  async run(sql: string, params?: unknown[]): Promise<void> {
    if (this.useTurso) {
      await this.turso.execute({ sql, args: params as InValue[] });
    }
    await this.dosql.run(sql, params);
  }
}
```

**Step 2: Sync DoSQL Changes Back to Turso**

```typescript
// Replay CDC events to Turso
import { createCDC } from '@dotdo/dosql/cdc';

async function syncDoSQLToTurso(
  dosql: Database,
  turso: ReturnType<typeof createClient>,
  fromLSN: bigint
): Promise<void> {
  const cdc = createCDC(dosql);

  for await (const event of cdc.subscribe(fromLSN)) {
    switch (event.type) {
      case 'insert':
        const columns = Object.keys(event.after!);
        const placeholders = columns.map((_, i) => `?${i + 1}`).join(', ');
        const values = Object.values(event.after!);
        await turso.execute({
          sql: `INSERT INTO "${event.table}" (${columns.map(c => `"${c}"`).join(', ')}) VALUES (${placeholders})`,
          args: values as InValue[],
        });
        break;

      case 'update':
        const setClauses = Object.keys(event.after!)
          .map((col, i) => `"${col}" = ?${i + 1}`)
          .join(', ');
        const updateValues = [...Object.values(event.after!), event.key];
        await turso.execute({
          sql: `UPDATE "${event.table}" SET ${setClauses} WHERE rowid = ?${updateValues.length}`,
          args: updateValues as InValue[],
        });
        break;

      case 'delete':
        await turso.execute({
          sql: `DELETE FROM "${event.table}" WHERE rowid = ?`,
          args: [event.key],
        });
        break;
    }
  }
}
```

### Data Sync Considerations

**During Dual-Write Period:**
- Both databases receive all writes
- Reads can be from either (based on primary setting)
- CDC captures all DoSQL changes for replay if needed

**After Full Migration:**
- Keep Turso database active (read-only) for safety period
- Use CDC to continuously sync to Turso as backup
- Set retention policy for how long to maintain Turso sync

**Rollback Timing:**
- Immediate rollback: Switch primary flag, minor data loss possible
- Graceful rollback: Sync remaining CDC events, then switch
- Full rollback: Re-migrate all data from DoSQL to Turso

---

## Common Gotchas

### 1. Result Format Differences

**Turso** returns results wrapped in a `rows` array with separate `columns`:
```typescript
// Turso
const result = await client.execute('SELECT * FROM users');
// result.rows = [['Alice', 1], ['Bob', 2]]
// result.columns = ['name', 'id']
```

**DoSQL** returns plain objects directly:
```typescript
// DoSQL
const users = await client.query<User>('SELECT * FROM users');
// users = [{ name: 'Alice', id: 1 }, { name: 'Bob', id: 2 }]
```

### 2. Parameter Binding Syntax

**Turso** uses `args` property:
```typescript
// Turso
await client.execute({ sql: 'SELECT * FROM users WHERE id = ?', args: [42] });
```

**DoSQL** uses second argument:
```typescript
// DoSQL
await client.query('SELECT * FROM users WHERE id = ?', [42]);
```

### 3. Transaction API

**Turso** returns a transaction handle:
```typescript
// Turso
const tx = await client.transaction('write');
await tx.execute(...);
await tx.commit();  // Manual commit required
```

**DoSQL** uses callback-based transactions:
```typescript
// DoSQL
await client.transaction(async (tx) => {
  await tx.run(...);
  // Automatic commit/rollback
});
```

### 4. Embedded Replicas vs Durable Objects

- Turso embedded replicas require explicit `sync()` calls
- DoSQL Durable Objects are always consistent - no sync needed
- Migration from offline-first patterns requires architectural changes

### 5. Vector Column Types

**Turso** uses `F32_BLOB(dimensions)`:
```sql
-- Turso
CREATE TABLE docs (embedding F32_BLOB(1536));
```

**DoSQL** uses `BLOB` with separate index:
```sql
-- DoSQL
CREATE TABLE docs (embedding BLOB);
CREATE INDEX idx ON docs USING VECTOR (embedding) WITH (dimensions = 1536);
```

### 6. Global Routing

- Turso automatically routes to nearest edge replica
- DoSQL routes to the DO's home location
- Use location hints when creating new DOs for region control

### 7. Authentication Model

**Turso** uses auth tokens:
```typescript
// Turso
createClient({ url: '...', authToken: 'xxx' });
```

**DoSQL** uses Cloudflare bindings:
```typescript
// DoSQL - authentication handled by Cloudflare
// No auth token needed in Worker code
const stub = env.DOSQL_DB.get(id);
```

### 8. Bundle Size Considerations

- Turso's `@libsql/client/web` is ~500KB (WASM-based)
- DoSQL's `@dotdo/dosql` is ~7KB (pure TypeScript)
- This can significantly impact cold start times on Cloudflare Workers

### 9. CDC vs Triggers

- Turso supports SQLite triggers
- DoSQL uses CDC subscriptions instead
- Migrate trigger logic to CDC event handlers

```typescript
// Instead of Turso triggers, use DoSQL CDC
const cdc = createCDC(db);
for await (const event of cdc.subscribe(0n)) {
  if (event.table === 'orders' && event.type === 'insert') {
    // Handle new order (equivalent to INSERT trigger)
    await sendOrderNotification(event.after);
  }
}
```

### 10. libSQL-specific Features Not Available

- `ALTER COLUMN` (libSQL extension) - must recreate table
- `RANDOM ROWID` - use application-generated UUIDs
- Platform API features - use Cloudflare dashboard/API instead

---

## Summary

Migrating from Turso to DoSQL provides access to advanced features like compile-time type safety, CDC, time travel, and branching, while significantly reducing bundle size. Key migration steps:

1. **Assess your needs** - Only migrate if you need DoSQL's unique features or are building on Cloudflare Workers
2. **Export your schema** - Use Turso CLI or programmatic export
3. **Plan data migration** - Choose between full export, incremental sync, or dual-write
4. **Update client code** - Replace `@libsql/client` with `@dotdo/sql.do`
5. **Migrate vectors** - Adjust syntax for vector columns and indexes
6. **Adapt replication strategy** - Move from edge replicas to Durable Objects
7. **Test thoroughly** - Run both databases in parallel and verify consistency
8. **Have a rollback plan** - Keep Turso available during the transition period

For questions or issues, refer to:
- [DoSQL Getting Started](../packages/dosql/docs/getting-started.md)
- [DoSQL API Reference](../packages/dosql/docs/api-reference.md)
- [DoSQL Architecture](../packages/dosql/docs/architecture.md)
