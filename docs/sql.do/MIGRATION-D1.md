# Migrating from D1 to DoSQL

A practical guide for developers migrating Cloudflare Workers applications from D1 to DoSQL.

## Table of Contents

- [Quick Reference Card](#quick-reference-card)
- [Should You Migrate?](#should-you-migrate)
- [Side-by-Side Comparison](#side-by-side-comparison)
- [Step-by-Step Migration Process](#step-by-step-migration-process)
- [API Compatibility Reference](#api-compatibility-reference)
- [Common Gotchas and Solutions](#common-gotchas-and-solutions)
- [Testing Your Migration](#testing-your-migration)
- [Rollback Strategy](#rollback-strategy)

---

## Quick Reference Card

Print this section or keep it handy during migration.

| D1 | DoSQL | Notes |
|----|-------|-------|
| `env.DB` | `env.DOSQL_DB.get(id)` | DoSQL uses Durable Objects |
| `.bind(...).all()` | `.all(...params)` | Parameters go directly to method |
| `.bind(...).first()` | `.get(...params)` | Method renamed |
| `.bind(...).run()` | `.run(...params)` | Same pattern |
| `db.batch([...])` | `db.transaction(() => {...})` | Explicit transactions |
| `result.results` | Direct array return | No wrapper object |
| `result.meta.last_row_id` | `result.lastInsertRowid` | Property renamed |
| `result.meta.changes` | `result.changes` | Property renamed |

---

## Should You Migrate?

### Migrate to DoSQL When You Need

| Feature | Description |
|---------|-------------|
| **Data Locality** | Each tenant gets their own database in a Durable Object, reducing latency |
| **Time Travel** | Query historical data with `FOR SYSTEM_TIME AS OF` |
| **Branching** | Create Git-like branches for testing and staging |
| **CDC Streaming** | Real-time change capture to lakehouse or external systems |
| **Virtual Tables** | Query R2, URLs, and APIs directly in SQL |
| **True Isolation** | Complete tenant isolation with separate DO instances |

### Stay with D1 When

| Scenario | Reason |
|----------|--------|
| Zero operational overhead required | D1 is fully managed |
| No natural tenant boundaries | D1 works better for shared databases |
| Need read replicas across regions | D1 has built-in replication |
| Team is unfamiliar with Durable Objects | Learning curve consideration |
| Simple app with low latency requirements | D1 may be simpler |

### Migration Complexity Assessment

| Your Application | Complexity | Estimated Time |
|------------------|------------|----------------|
| Simple CRUD app | Low | 1-2 hours |
| App with transactions | Low | 2-4 hours |
| App with batch operations | Medium | 4-8 hours |
| Multi-database app | Medium | 1-2 days |
| App with complex migrations | High | 2-5 days |

---

## Side-by-Side Comparison

### Database Connection

<table>
<tr>
<th width="50%">D1</th>
<th width="50%">DoSQL</th>
</tr>
<tr>
<td>

```typescript
// D1: Direct binding
export interface Env {
  DB: D1Database;
}

export default {
  async fetch(request: Request, env: Env) {
    // DB is immediately available
    const result = await env.DB
      .prepare('SELECT * FROM users')
      .all();

    return Response.json(result.results);
  }
};
```

</td>
<td>

```typescript
// DoSQL: Durable Object pattern
import { DB } from '@dotdo/dosql';

export interface Env {
  DOSQL_DB: DurableObjectNamespace;
}

export class DatabaseDO implements DurableObject {
  private db: Database | null = null;

  constructor(private state: DurableObjectState) {}

  private async getDB() {
    if (!this.db) {
      this.db = await DB('main', {
        storage: { hot: this.state.storage }
      });
    }
    return this.db;
  }

  async fetch(request: Request) {
    const db = await this.getDB();
    const users = db.prepare('SELECT * FROM users').all();
    return Response.json(users);
  }
}

export default {
  async fetch(request: Request, env: Env) {
    const id = env.DOSQL_DB.idFromName('default');
    const stub = env.DOSQL_DB.get(id);
    return stub.fetch(request);
  }
};
```

</td>
</tr>
</table>

### Simple SELECT Query

<table>
<tr>
<th width="50%">D1</th>
<th width="50%">DoSQL</th>
</tr>
<tr>
<td>

```typescript
// D1
const result = await db
  .prepare('SELECT * FROM users WHERE id = ?')
  .bind(42)
  .all();

// Access results
const users = result.results;
const meta = result.meta;
```

</td>
<td>

```typescript
// DoSQL
const users = db
  .prepare('SELECT * FROM users WHERE id = ?')
  .all(42);

// Results returned directly (no wrapper)
// No separate meta object
```

</td>
</tr>
</table>

### Get Single Row

<table>
<tr>
<th width="50%">D1</th>
<th width="50%">DoSQL</th>
</tr>
<tr>
<td>

```typescript
// D1: first() method
const user = await db
  .prepare('SELECT * FROM users WHERE id = ?')
  .bind(42)
  .first();

// Get specific column
const name = await db
  .prepare('SELECT name FROM users WHERE id = ?')
  .bind(42)
  .first('name');
```

</td>
<td>

```typescript
// DoSQL: get() method
const user = db
  .prepare('SELECT * FROM users WHERE id = ?')
  .get(42);

// Get specific column with pluck()
const name = db
  .prepare('SELECT name FROM users WHERE id = ?')
  .pluck()
  .get(42);
```

</td>
</tr>
</table>

### INSERT with Last ID

<table>
<tr>
<th width="50%">D1</th>
<th width="50%">DoSQL</th>
</tr>
<tr>
<td>

```typescript
// D1
const result = await db
  .prepare('INSERT INTO users (name, email) VALUES (?, ?)')
  .bind('Alice', 'alice@example.com')
  .run();

const userId = result.meta.last_row_id;
const rowsAffected = result.meta.changes;
```

</td>
<td>

```typescript
// DoSQL
const result = db
  .prepare('INSERT INTO users (name, email) VALUES (?, ?)')
  .run('Alice', 'alice@example.com');

const userId = result.lastInsertRowid;
const rowsAffected = result.changes;
```

</td>
</tr>
</table>

### Batch Operations vs Transactions

<table>
<tr>
<th width="50%">D1</th>
<th width="50%">DoSQL</th>
</tr>
<tr>
<td>

```typescript
// D1: Implicit transaction via batch
const results = await db.batch([
  db.prepare('INSERT INTO orders (user_id, total) VALUES (?, ?)')
    .bind(1, 99.99),
  db.prepare('INSERT INTO items (order_id, product) VALUES (?, ?)')
    .bind(1, 'Widget'),
  db.prepare('UPDATE inventory SET stock = stock - 1 WHERE id = ?')
    .bind(42),
]);

// Check all succeeded
const allSuccess = results.every(r => r.success);
```

</td>
<td>

```typescript
// DoSQL: Explicit transaction
const createOrder = db.transaction((
  userId: number,
  total: number,
  product: string,
  inventoryId: number
) => {
  const order = db.prepare(
    'INSERT INTO orders (user_id, total) VALUES (?, ?)'
  ).run(userId, total);

  db.prepare(
    'INSERT INTO items (order_id, product) VALUES (?, ?)'
  ).run(order.lastInsertRowid, product);

  db.prepare(
    'UPDATE inventory SET stock = stock - 1 WHERE id = ?'
  ).run(inventoryId);

  return { orderId: order.lastInsertRowid };
});

// Execute (auto-rollback on error)
const result = createOrder(1, 99.99, 'Widget', 42);
```

</td>
</tr>
</table>

### Parameter Binding Styles

<table>
<tr>
<th width="50%">D1</th>
<th width="50%">DoSQL</th>
</tr>
<tr>
<td>

```typescript
// D1: Positional with ?1, ?2
const users = await db
  .prepare('SELECT * FROM users WHERE role = ?1 AND active = ?2')
  .bind('admin', true)
  .all();

// Sequential ?
const posts = await db
  .prepare('SELECT * FROM posts WHERE user_id = ?')
  .bind(userId)
  .all();
```

</td>
<td>

```typescript
// DoSQL: Positional with ?
const users = db
  .prepare('SELECT * FROM users WHERE role = ? AND active = ?')
  .all('admin', true);

// Named parameters with :name, $name, or @name
const posts = db
  .prepare('SELECT * FROM posts WHERE user_id = :userId')
  .all({ userId });

// Also supports $name syntax
const comments = db
  .prepare('SELECT * FROM comments WHERE post_id = $postId')
  .all({ postId: 123 });
```

</td>
</tr>
</table>

### Error Handling

<table>
<tr>
<th width="50%">D1</th>
<th width="50%">DoSQL</th>
</tr>
<tr>
<td>

```typescript
// D1: Check success property
try {
  const result = await db
    .prepare('SELECT * FROM users')
    .all();

  if (!result.success) {
    console.error('Query failed');
  }
} catch (error) {
  console.error('D1 error:', error);
}
```

</td>
<td>

```typescript
// DoSQL: Typed error handling
import { DatabaseError } from '@dotdo/dosql';

try {
  const users = db.prepare('SELECT * FROM users').all();
} catch (error) {
  if (error instanceof DatabaseError) {
    switch (error.code) {
      case 'STMT_TABLE_NOT_FOUND':
        console.error('Table does not exist');
        break;
      case 'STMT_SYNTAX_ERROR':
        console.error('SQL syntax error:', error.message);
        break;
      case 'CONSTRAINT_UNIQUE':
        console.error('Unique constraint violation');
        break;
      default:
        console.error('Database error:', error.code);
    }
  }
}
```

</td>
</tr>
</table>

### TypeScript Generics

<table>
<tr>
<th width="50%">D1</th>
<th width="50%">DoSQL</th>
</tr>
<tr>
<td>

```typescript
// D1: Generic on method
interface User {
  id: number;
  name: string;
  email: string;
}

const user = await db
  .prepare('SELECT * FROM users WHERE id = ?')
  .bind(1)
  .first<User>();
// user is User | null
```

</td>
<td>

```typescript
// DoSQL: Generic on prepare
interface User {
  id: number;
  name: string;
  email: string;
}

const user = db
  .prepare<User>('SELECT * FROM users WHERE id = ?')
  .get(1);
// user is User | undefined

// With parameter types
const users = db
  .prepare<User, [string, boolean]>(
    'SELECT * FROM users WHERE role = ? AND active = ?'
  )
  .all('admin', true);
// users is User[]
```

</td>
</tr>
</table>

---

## Step-by-Step Migration Process

### Step 1: Install DoSQL

```bash
npm install @dotdo/dosql
```

### Step 2: Create Durable Object Database Class

Create `src/database.ts`:

```typescript
import { DB, type Database } from '@dotdo/dosql';

export interface Env {
  // Keep D1 binding during migration (optional)
  D1_DB?: D1Database;
  // Add DO namespace
  DOSQL_DB: DurableObjectNamespace;
  // Optional: R2 for cold storage
  DATA_BUCKET?: R2Bucket;
}

export class DatabaseDO implements DurableObject {
  private db: Database | null = null;
  private state: DurableObjectState;
  private env: Env;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  private async getDB(): Promise<Database> {
    if (!this.db) {
      this.db = await DB('main', {
        migrations: { folder: '.do/migrations' },
        storage: {
          hot: this.state.storage,
          cold: this.env.DATA_BUCKET, // Optional
        },
      });
    }
    return this.db;
  }

  async fetch(request: Request): Promise<Response> {
    const db = await this.getDB();
    const url = new URL(request.url);

    // Your route handlers here
    // See Step 7 for converting your routes

    return new Response('Not found', { status: 404 });
  }
}
```

### Step 3: Convert Migrations

D1 migrations are typically in `migrations/` or managed via Wrangler CLI. DoSQL uses `.do/migrations/`.

```bash
# Create DoSQL migrations directory
mkdir -p .do/migrations

# Copy your existing migrations
cp migrations/*.sql .do/migrations/

# Rename to follow DoSQL convention (NNN_name.sql)
# Example: 0001_create_users.sql -> 001_create_users.sql
```

DoSQL migration naming convention:
```
.do/migrations/
  001_create_users.sql
  002_add_posts.sql
  003_add_indexes.sql
```

### Step 4: Update Wrangler Configuration

Update `wrangler.jsonc`:

```jsonc
{
  "name": "my-app",
  "main": "src/index.ts",
  "compatibility_date": "2024-01-01",

  // Optional: Keep D1 during migration for dual-write
  "d1_databases": [
    {
      "binding": "D1_DB",
      "database_name": "my-db",
      "database_id": "your-database-id"
    }
  ],

  // Add Durable Objects
  "durable_objects": {
    "bindings": [
      { "name": "DOSQL_DB", "class_name": "DatabaseDO" }
    ]
  },
  "migrations": [
    { "tag": "v1", "new_classes": ["DatabaseDO"] }
  ],

  // Optional: R2 for cold storage
  "r2_buckets": [
    { "binding": "DATA_BUCKET", "bucket_name": "my-data-bucket" }
  ]
}
```

### Step 5: Create a Migration Wrapper (Optional)

During migration, you can create a wrapper that works with both D1 and DoSQL:

```typescript
// src/db-wrapper.ts
import type { Database } from '@dotdo/dosql';

interface QueryResult<T> {
  results: T[];
  changes: number;
  lastInsertRowid: number | bigint;
}

export class DBWrapper {
  constructor(
    private d1: D1Database | null,
    private dosql: Database | null,
    private useDoSQL: boolean = false
  ) {}

  async query<T>(sql: string, params: unknown[] = []): Promise<T[]> {
    if (this.useDoSQL && this.dosql) {
      return this.dosql.prepare(sql).all(...params) as T[];
    }

    if (this.d1) {
      const result = await this.d1.prepare(sql).bind(...params).all<T>();
      return result.results;
    }

    throw new Error('No database configured');
  }

  async get<T>(sql: string, params: unknown[] = []): Promise<T | undefined> {
    if (this.useDoSQL && this.dosql) {
      return this.dosql.prepare(sql).get(...params) as T | undefined;
    }

    if (this.d1) {
      const result = await this.d1.prepare(sql).bind(...params).first<T>();
      return result ?? undefined;
    }

    throw new Error('No database configured');
  }

  async run(
    sql: string,
    params: unknown[] = []
  ): Promise<{ changes: number; lastInsertRowid: number | bigint }> {
    if (this.useDoSQL && this.dosql) {
      const result = this.dosql.prepare(sql).run(...params);
      return { changes: result.changes, lastInsertRowid: result.lastInsertRowid };
    }

    if (this.d1) {
      const result = await this.d1.prepare(sql).bind(...params).run();
      return { changes: result.meta.changes, lastInsertRowid: result.meta.last_row_id };
    }

    throw new Error('No database configured');
  }
}
```

### Step 6: Migrate Data (If Needed)

If you have existing data in D1, migrate it to DoSQL:

```typescript
// src/migrate-data.ts
import type { D1Database } from '@cloudflare/workers-types';
import type { Database } from '@dotdo/dosql';

interface MigrationOptions {
  tables: string[];
  batchSize?: number;
  onProgress?: (table: string, current: number, total: number) => void;
}

export async function migrateD1ToDoSQL(
  d1: D1Database,
  dosql: Database,
  options: MigrationOptions
): Promise<void> {
  const { tables, batchSize = 1000, onProgress } = options;

  for (const table of tables) {
    // Get total count
    const countResult = await d1
      .prepare(`SELECT COUNT(*) as count FROM ${table}`)
      .first<{ count: number }>();
    const total = countResult?.count ?? 0;

    if (total === 0) {
      onProgress?.(table, 0, 0);
      continue;
    }

    // Get column info
    const columns = await d1.prepare(`PRAGMA table_info(${table})`).all();
    const columnNames = columns.results.map((c: any) => c.name).join(', ');
    const placeholders = columns.results.map(() => '?').join(', ');

    // Prepare insert statement
    const insertStmt = dosql.prepare(
      `INSERT INTO ${table} (${columnNames}) VALUES (${placeholders})`
    );

    // Create batch insert transaction
    const insertBatch = dosql.transaction((rows: unknown[][]) => {
      for (const row of rows) {
        insertStmt.run(...row);
      }
    });

    // Migrate in batches
    let offset = 0;
    while (offset < total) {
      const batch = await d1
        .prepare(`SELECT * FROM ${table} LIMIT ? OFFSET ?`)
        .bind(batchSize, offset)
        .all();

      const rows = batch.results.map((row: any) =>
        columns.results.map((c: any) => row[c.name])
      );

      insertBatch(rows);

      offset += batchSize;
      onProgress?.(table, Math.min(offset, total), total);
    }
  }
}

// Usage in a migration endpoint
export async function handleMigration(
  d1: D1Database,
  dosql: Database
): Promise<Response> {
  try {
    await migrateD1ToDoSQL(d1, dosql, {
      tables: ['users', 'posts', 'comments'],
      onProgress: (table, current, total) => {
        console.log(`Migrating ${table}: ${current}/${total}`);
      },
    });
    return new Response('Migration complete', { status: 200 });
  } catch (error) {
    return new Response(`Migration failed: ${error}`, { status: 500 });
  }
}
```

### Step 7: Convert Your Route Handlers

Transform each D1 endpoint to DoSQL:

**Before (D1):**

```typescript
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === '/users' && request.method === 'GET') {
      const result = await env.DB
        .prepare('SELECT * FROM users WHERE active = ?')
        .bind(true)
        .all();
      return Response.json(result.results);
    }

    if (url.pathname === '/users' && request.method === 'POST') {
      const { name, email } = await request.json();
      const result = await env.DB
        .prepare('INSERT INTO users (name, email) VALUES (?, ?)')
        .bind(name, email)
        .run();
      return Response.json({ id: result.meta.last_row_id }, { status: 201 });
    }

    return new Response('Not found', { status: 404 });
  },
};
```

**After (DoSQL):**

```typescript
import { DatabaseDO } from './database';

export { DatabaseDO };

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Route to Durable Object
    const id = env.DOSQL_DB.idFromName('default');
    const stub = env.DOSQL_DB.get(id);
    return stub.fetch(request);
  },
};

// In DatabaseDO class:
async fetch(request: Request): Promise<Response> {
  const db = await this.getDB();
  const url = new URL(request.url);

  if (url.pathname === '/users' && request.method === 'GET') {
    const users = db
      .prepare('SELECT * FROM users WHERE active = ?')
      .all(true);
    return Response.json(users);
  }

  if (url.pathname === '/users' && request.method === 'POST') {
    const { name, email } = await request.json();
    const result = db
      .prepare('INSERT INTO users (name, email) VALUES (?, ?)')
      .run(name, email);
    return Response.json({ id: result.lastInsertRowid }, { status: 201 });
  }

  return new Response('Not found', { status: 404 });
}
```

### Step 8: Validate and Test

```typescript
// Validation script
async function validateMigration(
  d1: D1Database,
  dosql: Database
): Promise<{ valid: boolean; errors: string[] }> {
  const tables = ['users', 'posts', 'comments'];
  const errors: string[] = [];

  for (const table of tables) {
    // Compare counts
    const d1Count = await d1
      .prepare(`SELECT COUNT(*) as c FROM ${table}`)
      .first<{ c: number }>();
    const dosqlCount = dosql
      .prepare(`SELECT COUNT(*) as c FROM ${table}`)
      .get() as { c: number };

    if (d1Count?.c !== dosqlCount?.c) {
      errors.push(
        `Count mismatch for ${table}: D1=${d1Count?.c}, DoSQL=${dosqlCount?.c}`
      );
    }

    // Compare sample data
    const d1Sample = await d1
      .prepare(`SELECT * FROM ${table} ORDER BY id LIMIT 100`)
      .all();
    const dosqlSample = dosql
      .prepare(`SELECT * FROM ${table} ORDER BY id LIMIT 100`)
      .all();

    if (JSON.stringify(d1Sample.results) !== JSON.stringify(dosqlSample)) {
      errors.push(`Data mismatch in ${table}`);
    }
  }

  return { valid: errors.length === 0, errors };
}
```

### Step 9: Switch Traffic

Once validated, switch all traffic to DoSQL:

```typescript
// Use environment variable to control routing
const USE_DOSQL = env.DOSQL_ENABLED === 'true';

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (USE_DOSQL) {
      const id = env.DOSQL_DB.idFromName('default');
      const stub = env.DOSQL_DB.get(id);
      return stub.fetch(request);
    }

    // Fallback to D1
    return handleD1Request(request, env.D1_DB);
  },
};
```

### Step 10: Remove D1

After confirming everything works:

1. Remove D1 binding from `wrangler.jsonc`
2. Remove D1-related code
3. Delete D1 database via Wrangler CLI

```bash
# Remove D1 database
wrangler d1 delete my-db
```

---

## API Compatibility Reference

### Method Mapping

| D1 Method | DoSQL Equivalent | Return Type |
|-----------|------------------|-------------|
| `db.prepare(sql)` | `db.prepare(sql)` | Statement |
| `stmt.bind(...params)` | `stmt.bind(...params)` | Statement (chainable) |
| `stmt.all()` | `stmt.all()` | `T[]` (direct) |
| `stmt.first()` | `stmt.get()` | `T \| undefined` |
| `stmt.first('column')` | `stmt.pluck().get()` | `Value \| undefined` |
| `stmt.run()` | `stmt.run()` | `RunResult` |
| `stmt.raw()` | `stmt.raw().all()` | `Value[][]` |
| `db.exec(sql)` | `db.exec(sql)` | `this` |
| `db.batch([...])` | `db.transaction(() => {...})` | `ReturnType<fn>` |
| `db.dump()` | Not supported | Use WAL export |

### Result Object Mapping

| D1 Property | DoSQL Property |
|-------------|----------------|
| `result.results` | Direct return (no wrapper) |
| `result.success` | Throws on error |
| `result.meta.duration` | Not available |
| `result.meta.changes` | `result.changes` |
| `result.meta.last_row_id` | `result.lastInsertRowid` |
| `result.meta.served_by` | Not available |

### SQL Feature Compatibility

| Feature | D1 | DoSQL |
|---------|-----|-------|
| Parameter binding `?` | Yes | Yes |
| Parameter binding `?1`, `?2` | Yes | Yes |
| Named parameters `:name` | No | Yes |
| Named parameters `$name` | No | Yes |
| Named parameters `@name` | No | Yes |
| JSON functions | Yes | Yes |
| FTS5 full-text search | Yes | Yes |
| Window functions | Yes | Yes |
| CTEs | Yes | Yes |
| Time travel queries | No | Yes |
| Virtual tables | No | Yes |

---

## Common Gotchas and Solutions

### Gotcha 1: Async vs Sync API

**Problem:** D1 is async everywhere, DoSQL is sync for prepared statements.

**D1 (async):**
```typescript
const users = await db.prepare('SELECT * FROM users').all();
```

**DoSQL (sync):**
```typescript
const users = db.prepare('SELECT * FROM users').all(); // No await!
```

**Solution:** Remove `await` from DoSQL prepared statement calls. Only `DB()` initialization is async.

---

### Gotcha 2: Result Wrapper Object

**Problem:** D1 returns `{ results, success, meta }`, DoSQL returns results directly.

**D1:**
```typescript
const result = await db.prepare('SELECT * FROM users').all();
const users = result.results; // Array is nested
const count = result.meta.changes;
```

**DoSQL:**
```typescript
const users = db.prepare('SELECT * FROM users').all(); // Direct array
// No meta object - use RunResult for mutations
```

**Solution:** Access the array directly. For mutation metadata, capture the `run()` result.

---

### Gotcha 3: first() vs get()

**Problem:** Method is renamed.

**D1:**
```typescript
const user = await db.prepare('SELECT * FROM users WHERE id = ?').bind(1).first();
```

**DoSQL:**
```typescript
const user = db.prepare('SELECT * FROM users WHERE id = ?').get(1);
```

**Solution:** Replace `.first()` with `.get()`.

---

### Gotcha 4: batch() vs transaction()

**Problem:** D1 `batch()` is implicit, DoSQL `transaction()` is explicit.

**D1:**
```typescript
const results = await db.batch([
  db.prepare('INSERT INTO a VALUES (?)').bind(1),
  db.prepare('INSERT INTO b VALUES (?)').bind(2),
]);
```

**DoSQL:**
```typescript
const insertBoth = db.transaction(() => {
  db.prepare('INSERT INTO a VALUES (?)').run(1);
  db.prepare('INSERT INTO b VALUES (?)').run(2);
});
insertBoth();
```

**Solution:** Wrap batch operations in `db.transaction()`.

---

### Gotcha 5: Binding Parameters

**Problem:** Parameters go to different places.

**D1:**
```typescript
db.prepare('SELECT * FROM users WHERE id = ?').bind(42).all();
```

**DoSQL:**
```typescript
db.prepare('SELECT * FROM users WHERE id = ?').all(42);
```

**Solution:** Pass parameters directly to `all()`, `get()`, or `run()`.

---

### Gotcha 6: Durable Object Routing

**Problem:** D1 is a direct binding, DoSQL requires DO routing.

**D1:**
```typescript
// Direct access
const result = await env.DB.prepare('...').all();
```

**DoSQL:**
```typescript
// Must route through DO
const id = env.DOSQL_DB.idFromName('tenant-id');
const stub = env.DOSQL_DB.get(id);
const response = await stub.fetch(new Request('http://internal/query'));
```

**Solution:** Create a Durable Object class and route requests to it.

---

### Gotcha 7: Cold Starts

**Problem:** DoSQL in Durable Objects has cold start overhead that D1 doesn't have.

**Solution:** Use Durable Object hibernation and keep database instance cached:

```typescript
export class DatabaseDO implements DurableObject {
  private db: Database | null = null;

  private async getDB() {
    if (!this.db) {
      // This runs on cold start
      this.db = await DB('main', {
        migrations: { folder: '.do/migrations' },
        storage: { hot: this.state.storage },
      });
    }
    return this.db;
  }

  async fetch(request: Request) {
    const db = await this.getDB();
    // DB stays warm across hibernation cycles
  }
}
```

---

### Gotcha 8: No Read Replicas

**Problem:** D1 has read replicas, DoSQL doesn't.

**Solution:** Use caching for read-heavy workloads:

```typescript
async fetch(request: Request) {
  const url = new URL(request.url);
  const cacheKey = new Request(`https://cache${url.pathname}${url.search}`);
  const cache = caches.default;

  // Check cache first
  let response = await cache.match(cacheKey);
  if (response) {
    return response;
  }

  // Query database
  const db = await this.getDB();
  const data = db.prepare('SELECT * FROM products').all();

  response = Response.json(data, {
    headers: { 'Cache-Control': 'max-age=60' },
  });

  // Store in cache
  await cache.put(cacheKey, response.clone());
  return response;
}
```

---

### Gotcha 9: Error Handling Differences

**Problem:** D1 uses `success` flag, DoSQL throws errors.

**D1:**
```typescript
const result = await db.prepare('...').all();
if (!result.success) {
  // Handle error
}
```

**DoSQL:**
```typescript
import { DatabaseError } from '@dotdo/dosql';

try {
  const result = db.prepare('...').all();
} catch (error) {
  if (error instanceof DatabaseError) {
    // Handle specific error codes
    console.log(error.code, error.message);
  }
}
```

**Solution:** Wrap DoSQL calls in try/catch and check error codes.

---

## Testing Your Migration

### Dual-Write Pattern

Write to both databases during migration:

```typescript
class DualWriteDB {
  constructor(
    private d1: D1Database,
    private dosql: Database,
    private mode: 'd1-primary' | 'dosql-primary' | 'dosql-only'
  ) {}

  async run(sql: string, params: unknown[]): Promise<void> {
    switch (this.mode) {
      case 'd1-primary':
        await this.d1.prepare(sql).bind(...params).run();
        this.dosql.prepare(sql).run(...params); // Fire and forget
        break;

      case 'dosql-primary':
        this.dosql.prepare(sql).run(...params);
        await this.d1.prepare(sql).bind(...params).run(); // Fire and forget
        break;

      case 'dosql-only':
        this.dosql.prepare(sql).run(...params);
        break;
    }
  }
}
```

### Shadow Testing

Compare results between D1 and DoSQL:

```typescript
async function shadowTest<T>(
  d1: D1Database,
  dosql: Database,
  sql: string,
  params: unknown[]
): Promise<{ match: boolean; d1: T[]; dosql: T[] }> {
  const d1Result = await d1.prepare(sql).bind(...params).all<T>();
  const dosqlResult = dosql.prepare(sql).all(...params) as T[];

  const match =
    JSON.stringify(d1Result.results) === JSON.stringify(dosqlResult);

  if (!match) {
    console.warn('Shadow test mismatch:', {
      sql,
      d1Count: d1Result.results.length,
      dosqlCount: dosqlResult.length,
    });
  }

  return {
    match,
    d1: d1Result.results,
    dosql: dosqlResult,
  };
}
```

### Migration Checklist

```markdown
## Pre-Migration
- [ ] Audit all D1 queries in codebase
- [ ] Identify batch operations that need transaction conversion
- [ ] Review migrations for compatibility
- [ ] Set up DoSQL Durable Object class
- [ ] Test migrations in development
- [ ] Estimate data volume and migration time

## Migration
- [ ] Deploy DO alongside D1
- [ ] Run data migration script
- [ ] Validate data integrity (counts, checksums)
- [ ] Enable dual-write mode
- [ ] Monitor for discrepancies
- [ ] Run shadow tests

## Post-Migration
- [ ] Switch reads to DoSQL
- [ ] Monitor latency and errors
- [ ] Disable D1 writes
- [ ] Final validation
- [ ] Remove D1 binding
- [ ] Clean up migration code
```

---

## Rollback Strategy

If migration fails, you need a quick rollback plan.

### Using Feature Flags

```typescript
// Control via environment variable
const USE_DOSQL = env.DOSQL_ENABLED === 'true';

async function getUsers(env: Env): Promise<User[]> {
  if (USE_DOSQL) {
    const id = env.DOSQL_DB.idFromName('default');
    const stub = env.DOSQL_DB.get(id);
    const response = await stub.fetch('http://internal/users');
    return response.json();
  }

  // Fallback to D1
  const result = await env.D1_DB.prepare('SELECT * FROM users').all<User>();
  return result.results;
}

// Quick rollback via environment variable
// wrangler secret put DOSQL_ENABLED --value false
```

### Rollback Steps

1. **Immediate:** Set `DOSQL_ENABLED=false` to route traffic back to D1
2. **If dual-write was enabled:** Data should be in sync
3. **If dual-write was not enabled:** You may need to migrate data back to D1
4. **Investigate:** Check logs and error patterns before retrying migration

---

## Next Steps

After successfully migrating, explore DoSQL's unique features:

- **[Time Travel](./advanced.md#time-travel)** - Query historical data
- **[Branching](./advanced.md#branching)** - Git-like database branches
- **[CDC Streaming](./advanced.md#cdc-streaming)** - Real-time change capture
- **[Virtual Tables](./advanced.md#virtual-tables)** - Query R2, URLs, APIs directly

For issues during migration, see [Troubleshooting](./TROUBLESHOOTING.md) or file an issue on GitHub.
