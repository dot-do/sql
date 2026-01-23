# Migrating from D1 to DoSQL

A comprehensive guide for developers migrating Cloudflare Workers applications from D1 to DoSQL.

## Table of Contents

- [Quick Reference Card](#quick-reference-card)
- [Should You Migrate?](#should-you-migrate)
- [Side-by-Side Comparison](#side-by-side-comparison)
  - [Database Connection](#database-connection)
  - [Query Methods](#query-methods)
  - [Parameter Binding](#parameter-binding)
  - [Transactions and Batching](#transactions-and-batching)
  - [Error Handling](#error-handling)
  - [TypeScript Generics](#typescript-generics)
- [Step-by-Step Migration Process](#step-by-step-migration-process)
- [API Compatibility Reference](#api-compatibility-reference)
- [Common Gotchas and Solutions](#common-gotchas-and-solutions)
- [Testing Your Migration](#testing-your-migration)
- [Rollback Strategy](#rollback-strategy)
- [Advanced Migration Patterns](#advanced-migration-patterns)

---

## Quick Reference Card

Print this section or keep it handy during migration.

### Method Mappings

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

### Async vs Sync

| Operation | D1 | DoSQL |
|-----------|-----|-------|
| Database initialization | N/A (binding) | `await DB(...)` (async) |
| Prepared statement execution | `await stmt.all()` | `stmt.all()` (sync) |
| Query execution | `await db.prepare(...).all()` | `db.prepare(...).all()` (sync) |
| Batch/Transaction | `await db.batch([...])` | `db.transaction(...)()` (sync) |

### Return Value Differences

| Operation | D1 Returns | DoSQL Returns |
|-----------|------------|---------------|
| `all()` / `all()` | `{ results: T[], success, meta }` | `T[]` directly |
| `first()` / `get()` | `T \| null` | `T \| undefined` |
| `run()` / `run()` | `{ meta: { changes, last_row_id } }` | `{ changes, lastInsertRowid }` |
| `first('column')` | Column value | Use `pluck().get()` |

---

## Should You Migrate?

### Migrate to DoSQL When You Need

| Feature | D1 | DoSQL | Description |
|---------|-----|-------|-------------|
| Data Locality | Shared regions | Per-tenant DO | Each tenant gets their own database, reducing latency |
| Time Travel | Not available | `FOR SYSTEM_TIME AS OF` | Query historical data at any point |
| Branching | Not available | Git-like branches | Create branches for testing and staging |
| CDC Streaming | Not available | Real-time events | Stream changes to lakehouse or external systems |
| Virtual Tables | Not available | R2, URLs, APIs | Query external data sources directly in SQL |
| True Isolation | Shared database | Separate DO instances | Complete tenant isolation |
| Synchronous API | Async only | Sync within DO | Simpler code flow, no promise chains |

### Stay with D1 When

| Scenario | Reason |
|----------|--------|
| Zero operational overhead required | D1 is fully managed with no DO complexity |
| No natural tenant boundaries | D1 works better for shared, multi-tenant databases |
| Need read replicas across regions | D1 has built-in global replication |
| Team is unfamiliar with Durable Objects | Significant learning curve for DO patterns |
| Simple app with low write volume | D1 may be simpler for basic use cases |
| Need Wrangler D1 CLI tooling | D1 has mature CLI for migrations, backups |

### Migration Complexity Assessment

| Your Application | Complexity | Estimated Time | Key Challenges |
|------------------|------------|----------------|----------------|
| Simple CRUD app | Low | 1-2 hours | API syntax changes |
| App with transactions | Low | 2-4 hours | batch() to transaction() |
| App with batch operations | Medium | 4-8 hours | Rewriting batch logic |
| Multi-database app | Medium | 1-2 days | DO routing patterns |
| App with complex migrations | High | 2-5 days | Migration format conversion |
| High-traffic production app | High | 1-2 weeks | Dual-write, validation, rollout |

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
// D1: Direct binding from wrangler.toml
export interface Env {
  DB: D1Database;
}

export default {
  async fetch(request: Request, env: Env) {
    // DB is immediately available as a binding
    const result = await env.DB
      .prepare('SELECT * FROM users')
      .all();

    return Response.json(result.results);
  }
};
```

**wrangler.toml:**
```toml
[[d1_databases]]
binding = "DB"
database_name = "my-db"
database_id = "xxx-xxx-xxx"
```

</td>
<td>

```typescript
// DoSQL: Durable Object pattern
import { Database, createDatabase } from '@dotdo/dosql';

export interface Env {
  DOSQL_DB: DurableObjectNamespace;
}

export class DatabaseDO implements DurableObject {
  private db: Database | null = null;

  constructor(private state: DurableObjectState) {}

  private getDB(): Database {
    if (!this.db) {
      this.db = createDatabase();
      // Run migrations, setup, etc.
    }
    return this.db;
  }

  async fetch(request: Request): Promise<Response> {
    const db = this.getDB();
    // Queries are synchronous within the DO
    const users = db.prepare('SELECT * FROM users').all();
    return Response.json(users);
  }
}

export default {
  async fetch(request: Request, env: Env) {
    // Route to the appropriate DO instance
    const id = env.DOSQL_DB.idFromName('default');
    const stub = env.DOSQL_DB.get(id);
    return stub.fetch(request);
  }
};
```

**wrangler.toml:**
```toml
[durable_objects]
bindings = [
  { name = "DOSQL_DB", class_name = "DatabaseDO" }
]

[[migrations]]
tag = "v1"
new_classes = ["DatabaseDO"]
```

</td>
</tr>
</table>

**Key Differences:**
- D1 provides a direct binding; DoSQL requires setting up a Durable Object class
- D1 database is shared; DoSQL gives you per-tenant isolation via DO naming
- D1 configuration is simpler; DoSQL requires DO migrations in wrangler.toml

---

### Query Methods

#### SELECT All Rows

<table>
<tr>
<th width="50%">D1</th>
<th width="50%">DoSQL</th>
</tr>
<tr>
<td>

```typescript
// D1: async, results wrapped in object
const result = await db
  .prepare('SELECT * FROM users WHERE active = ?')
  .bind(true)
  .all();

// Access the array via .results
const users = result.results;

// Access metadata
const duration = result.meta.duration;
const success = result.success;
```

</td>
<td>

```typescript
// DoSQL: sync, direct array return
const users = db
  .prepare('SELECT * FROM users WHERE active = ?')
  .all(true);

// users IS the array - no wrapper
// No separate metadata object
```

</td>
</tr>
</table>

#### SELECT Single Row

<table>
<tr>
<th width="50%">D1</th>
<th width="50%">DoSQL</th>
</tr>
<tr>
<td>

```typescript
// D1: first() method, returns null if not found
const user = await db
  .prepare('SELECT * FROM users WHERE id = ?')
  .bind(42)
  .first();
// user is T | null

// Get specific column value
const name = await db
  .prepare('SELECT name FROM users WHERE id = ?')
  .bind(42)
  .first('name');
// name is string | null
```

</td>
<td>

```typescript
// DoSQL: get() method, returns undefined if not found
const user = db
  .prepare('SELECT * FROM users WHERE id = ?')
  .get(42);
// user is T | undefined

// Get specific column value with pluck()
const name = db
  .prepare('SELECT name FROM users WHERE id = ?')
  .pluck()
  .get(42);
// name is string | undefined
```

</td>
</tr>
</table>

#### INSERT / UPDATE / DELETE

<table>
<tr>
<th width="50%">D1</th>
<th width="50%">DoSQL</th>
</tr>
<tr>
<td>

```typescript
// D1: run() returns wrapped result
const result = await db
  .prepare('INSERT INTO users (name, email) VALUES (?, ?)')
  .bind('Alice', 'alice@example.com')
  .run();

// Access via meta object
const userId = result.meta.last_row_id;
const rowsAffected = result.meta.changes;
const success = result.success;

// UPDATE example
const updateResult = await db
  .prepare('UPDATE users SET active = ? WHERE id = ?')
  .bind(false, 42)
  .run();
console.log(updateResult.meta.changes); // 1
```

</td>
<td>

```typescript
// DoSQL: run() returns direct result
const result = db
  .prepare('INSERT INTO users (name, email) VALUES (?, ?)')
  .run('Alice', 'alice@example.com');

// Access directly on result
const userId = result.lastInsertRowid;
const rowsAffected = result.changes;
// No success property - throws on error

// UPDATE example
const updateResult = db
  .prepare('UPDATE users SET active = ? WHERE id = ?')
  .run(false, 42);
console.log(updateResult.changes); // 1
```

</td>
</tr>
</table>

#### Raw Results (Arrays)

<table>
<tr>
<th width="50%">D1</th>
<th width="50%">DoSQL</th>
</tr>
<tr>
<td>

```typescript
// D1: raw() method
const result = await db
  .prepare('SELECT id, name FROM users')
  .raw();

// Returns array of arrays
// [[1, 'Alice'], [2, 'Bob']]
```

</td>
<td>

```typescript
// DoSQL: raw() then all()
const rows = db
  .prepare('SELECT id, name FROM users')
  .raw()
  .all();

// Returns array of arrays
// [[1, 'Alice'], [2, 'Bob']]

// Can also get column names
const stmt = db.prepare('SELECT id, name FROM users').raw();
const rows = stmt.all();
const columns = stmt.columns();
// columns = [{ name: 'id', ... }, { name: 'name', ... }]
```

</td>
</tr>
</table>

---

### Parameter Binding

<table>
<tr>
<th width="50%">D1</th>
<th width="50%">DoSQL</th>
</tr>
<tr>
<td>

```typescript
// D1: Bind separately with .bind()

// Sequential ? placeholders
const users = await db
  .prepare('SELECT * FROM users WHERE role = ? AND active = ?')
  .bind('admin', true)
  .all();

// Positional ?1, ?2 placeholders
const posts = await db
  .prepare('SELECT * FROM posts WHERE user_id = ?1 AND status = ?2')
  .bind(userId, 'published')
  .all();

// Named parameters NOT supported in D1
// This will NOT work:
// .bind({ userId: 1, status: 'active' })
```

</td>
<td>

```typescript
// DoSQL: Pass params directly to execution method

// Sequential ? placeholders
const users = db
  .prepare('SELECT * FROM users WHERE role = ? AND active = ?')
  .all('admin', true);

// Named parameters with :name
const posts = db
  .prepare('SELECT * FROM posts WHERE user_id = :userId AND status = :status')
  .all({ userId, status: 'published' });

// Named parameters with $name
const comments = db
  .prepare('SELECT * FROM comments WHERE post_id = $postId')
  .all({ postId: 123 });

// Named parameters with @name
const likes = db
  .prepare('SELECT * FROM likes WHERE user_id = @userId')
  .all({ userId: 42 });

// Can also use bind() for reuse
const stmt = db.prepare('SELECT * FROM users WHERE id = ?');
const user1 = stmt.bind(1).get();
const user2 = stmt.bind(2).get();
```

</td>
</tr>
</table>

**Key Differences:**
- D1 requires `.bind()` before execution; DoSQL allows params directly in `.all()`, `.get()`, `.run()`
- D1 only supports positional parameters; DoSQL supports named parameters (`:name`, `$name`, `@name`)
- DoSQL's named parameters make complex queries more readable

---

### Transactions and Batching

<table>
<tr>
<th width="50%">D1</th>
<th width="50%">DoSQL</th>
</tr>
<tr>
<td>

```typescript
// D1: Implicit transaction via batch()
// All statements run in a single transaction

const results = await db.batch([
  db.prepare('INSERT INTO orders (user_id, total) VALUES (?, ?)')
    .bind(1, 99.99),
  db.prepare('INSERT INTO order_items (order_id, product_id, qty) VALUES (?, ?, ?)')
    .bind(1, 42, 2),
  db.prepare('UPDATE inventory SET stock = stock - ? WHERE product_id = ?')
    .bind(2, 42),
]);

// Check results
const allSuccess = results.every(r => r.success);
const orderId = results[0].meta.last_row_id;

// Note: Cannot use results from one statement
// in another within the same batch
```

</td>
<td>

```typescript
// DoSQL: Explicit transaction() wrapper
// Full control over transaction logic

const createOrder = db.transaction((
  userId: number,
  total: number,
  productId: number,
  qty: number
) => {
  // Each statement can use results from previous
  const order = db.prepare(
    'INSERT INTO orders (user_id, total) VALUES (?, ?)'
  ).run(userId, total);

  const orderId = order.lastInsertRowid;

  db.prepare(
    'INSERT INTO order_items (order_id, product_id, qty) VALUES (?, ?, ?)'
  ).run(orderId, productId, qty);

  db.prepare(
    'UPDATE inventory SET stock = stock - ? WHERE product_id = ?'
  ).run(qty, productId);

  return { orderId, itemCount: 1 };
});

// Execute - auto-rollback on any error
const result = createOrder(1, 99.99, 42, 2);
console.log(result.orderId);

// Nested transactions use savepoints
const transferFunds = db.transaction((from, to, amount) => {
  db.prepare('UPDATE accounts SET balance = balance - ? WHERE id = ?')
    .run(amount, from);
  db.prepare('UPDATE accounts SET balance = balance + ? WHERE id = ?')
    .run(amount, to);
});
```

</td>
</tr>
</table>

**Key Differences:**
- D1 batch is a list of independent statements; DoSQL transaction is a function with full logic
- D1 cannot use one statement's result in another; DoSQL can chain results naturally
- DoSQL transactions support savepoints via `db.transaction.immediate()` and `db.transaction.exclusive()`

---

### Error Handling

<table>
<tr>
<th width="50%">D1</th>
<th width="50%">DoSQL</th>
</tr>
<tr>
<td>

```typescript
// D1: Check success property + try/catch

try {
  const result = await db
    .prepare('SELECT * FROM users')
    .all();

  if (!result.success) {
    // Query-level failure
    console.error('Query failed');
    console.error(result.error);
  }

  // Use results
  return result.results;
} catch (error) {
  // Network/binding errors
  console.error('D1 error:', error);
  throw error;
}
```

</td>
<td>

```typescript
// DoSQL: Typed exceptions with error codes

import { DatabaseError } from '@dotdo/dosql';

try {
  const users = db.prepare('SELECT * FROM users').all();
  return users;
} catch (error) {
  if (error instanceof DatabaseError) {
    // Strongly-typed error handling
    switch (error.code) {
      case 'SQLITE_ERROR':
        console.error('SQL error:', error.message);
        break;
      case 'SQLITE_CONSTRAINT_UNIQUE':
        console.error('Unique constraint violation');
        break;
      case 'SQLITE_CONSTRAINT_FOREIGNKEY':
        console.error('Foreign key violation');
        break;
      case 'SQLITE_BUSY':
        console.error('Database is locked');
        break;
      default:
        console.error(`Database error [${error.code}]:`, error.message);
    }
  }
  throw error;
}

// Transaction errors auto-rollback
const tx = db.transaction(() => {
  db.prepare('INSERT INTO users (email) VALUES (?)').run('dupe@example.com');
  // If this throws, the INSERT is rolled back
  throw new Error('Abort!');
});

try {
  tx();
} catch (error) {
  // Transaction was rolled back
}
```

</td>
</tr>
</table>

**Key Differences:**
- D1 uses `success` boolean; DoSQL throws exceptions
- DoSQL provides typed `DatabaseError` with SQLite error codes
- DoSQL transactions automatically rollback on any exception

---

### TypeScript Generics

<table>
<tr>
<th width="50%">D1</th>
<th width="50%">DoSQL</th>
</tr>
<tr>
<td>

```typescript
// D1: Generic on execution method

interface User {
  id: number;
  name: string;
  email: string;
}

// Type on first()
const user = await db
  .prepare('SELECT * FROM users WHERE id = ?')
  .bind(1)
  .first<User>();
// user is User | null

// Type on all()
const users = await db
  .prepare('SELECT * FROM users')
  .all<User>();
// users.results is User[]

// Type on raw()
const raw = await db
  .prepare('SELECT id, name FROM users')
  .raw<[number, string]>();
// raw is [number, string][]
```

</td>
<td>

```typescript
// DoSQL: Generic on prepare(), with param types

interface User {
  id: number;
  name: string;
  email: string;
}

// Type on prepare - inferred for all executions
const userStmt = db.prepare<User>('SELECT * FROM users WHERE id = ?');
const user = userStmt.get(1);
// user is User | undefined

const users = userStmt.all(1);
// users is User[]

// With parameter types for extra safety
const stmt = db.prepare<User, [number, boolean]>(
  'SELECT * FROM users WHERE id = ? AND active = ?'
);
const user = stmt.get(1, true);  // Params typed as [number, boolean]
// stmt.get('wrong', 'types');   // TypeScript error!

// Named parameter types
interface UserParams {
  userId: number;
  role: string;
}
const namedStmt = db.prepare<User, UserParams>(
  'SELECT * FROM users WHERE id = :userId AND role = :role'
);
const admin = namedStmt.get({ userId: 1, role: 'admin' });
```

</td>
</tr>
</table>

**Key Differences:**
- D1 types go on execution methods; DoSQL types go on `prepare()`
- DoSQL can type both result AND parameter types
- DoSQL's approach provides better reusability for prepared statements

---

## Step-by-Step Migration Process

### Step 1: Install DoSQL

```bash
npm install @dotdo/dosql
```

### Step 2: Create Durable Object Database Class

Create `src/database.ts`:

```typescript
import { Database, createDatabase } from '@dotdo/dosql';

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

  constructor(
    private state: DurableObjectState,
    private env: Env
  ) {}

  private getDB(): Database {
    if (!this.db) {
      this.db = createDatabase();
      this.runMigrations();
    }
    return this.db;
  }

  private runMigrations(): void {
    const db = this.db!;

    // Check current schema version
    db.exec(`
      CREATE TABLE IF NOT EXISTS __migrations (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        applied_at TEXT DEFAULT CURRENT_TIMESTAMP
      )
    `);

    // Run your migrations here
    // See Step 3 for migration patterns
  }

  async fetch(request: Request): Promise<Response> {
    const db = this.getDB();
    const url = new URL(request.url);

    // Your route handlers here
    // See Step 7 for converting your routes

    return new Response('Not found', { status: 404 });
  }
}
```

### Step 3: Convert Migrations

D1 migrations are typically in `migrations/` or managed via Wrangler CLI.

**D1 Migration:**
```sql
-- migrations/0001_create_users.sql
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT UNIQUE
);
```

**DoSQL Migration (embedded in DO):**
```typescript
private runMigrations(): void {
  const db = this.db!;

  // Migration tracking table
  db.exec(`
    CREATE TABLE IF NOT EXISTS __migrations (
      id INTEGER PRIMARY KEY,
      name TEXT UNIQUE NOT NULL,
      applied_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
  `);

  const migrations = [
    {
      name: '001_create_users',
      sql: `
        CREATE TABLE users (
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          email TEXT UNIQUE
        )
      `
    },
    {
      name: '002_add_posts',
      sql: `
        CREATE TABLE posts (
          id INTEGER PRIMARY KEY,
          user_id INTEGER NOT NULL REFERENCES users(id),
          title TEXT NOT NULL,
          body TEXT,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
      `
    },
  ];

  const applied = new Set(
    db.prepare('SELECT name FROM __migrations').all().map((r: any) => r.name)
  );

  for (const migration of migrations) {
    if (!applied.has(migration.name)) {
      db.transaction(() => {
        db.exec(migration.sql);
        db.prepare('INSERT INTO __migrations (name) VALUES (?)').run(migration.name);
      })();
      console.log(`Applied migration: ${migration.name}`);
    }
  }
}
```

### Step 4: Update Wrangler Configuration

Update `wrangler.toml`:

```toml
name = "my-app"
main = "src/index.ts"
compatibility_date = "2024-01-01"

# Optional: Keep D1 during migration for dual-write
[[d1_databases]]
binding = "D1_DB"
database_name = "my-db"
database_id = "your-database-id"

# Add Durable Objects
[durable_objects]
bindings = [
  { name = "DOSQL_DB", class_name = "DatabaseDO" }
]

[[migrations]]
tag = "v1"
new_classes = ["DatabaseDO"]

# Optional: R2 for cold storage
[[r2_buckets]]
binding = "DATA_BUCKET"
bucket_name = "my-data-bucket"
```

### Step 5: Create a Migration Wrapper (Optional)

During migration, you can create a wrapper that works with both D1 and DoSQL:

```typescript
// src/db-wrapper.ts
import type { Database } from '@dotdo/dosql';

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
      return {
        changes: result.meta.changes,
        lastInsertRowid: result.meta.last_row_id
      };
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
): Promise<{ tables: Record<string, number> }> {
  const { tables, batchSize = 1000, onProgress } = options;
  const result: Record<string, number> = {};

  for (const table of tables) {
    // Get total count
    const countResult = await d1
      .prepare(`SELECT COUNT(*) as count FROM ${table}`)
      .first<{ count: number }>();
    const total = countResult?.count ?? 0;
    result[table] = 0;

    if (total === 0) {
      onProgress?.(table, 0, 0);
      continue;
    }

    // Get column info
    const columnsResult = await d1.prepare(`PRAGMA table_info(${table})`).all();
    const columns = columnsResult.results as Array<{ name: string; type: string }>;
    const columnNames = columns.map(c => c.name).join(', ');
    const placeholders = columns.map(() => '?').join(', ');

    // Prepare insert statement
    const insertStmt = dosql.prepare(
      `INSERT OR REPLACE INTO ${table} (${columnNames}) VALUES (${placeholders})`
    );

    // Create batch insert transaction
    const insertBatch = dosql.transaction((rows: unknown[][]) => {
      for (const row of rows) {
        insertStmt.run(...row);
      }
      return rows.length;
    });

    // Migrate in batches
    let offset = 0;
    while (offset < total) {
      const batch = await d1
        .prepare(`SELECT * FROM ${table} LIMIT ? OFFSET ?`)
        .bind(batchSize, offset)
        .all();

      const rows = batch.results.map((row: Record<string, unknown>) =>
        columns.map(c => row[c.name])
      );

      const inserted = insertBatch(rows);
      result[table] += inserted;

      offset += batchSize;
      onProgress?.(table, Math.min(offset, total), total);
    }
  }

  return { tables: result };
}

// Usage in a migration endpoint
export async function handleMigration(
  d1: D1Database,
  dosql: Database
): Promise<Response> {
  try {
    const result = await migrateD1ToDoSQL(d1, dosql, {
      tables: ['users', 'posts', 'comments'],
      onProgress: (table, current, total) => {
        console.log(`Migrating ${table}: ${current}/${total}`);
      },
    });
    return Response.json({ success: true, ...result });
  } catch (error) {
    return Response.json(
      { success: false, error: String(error) },
      { status: 500 }
    );
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

    if (url.pathname.startsWith('/users/') && request.method === 'GET') {
      const id = url.pathname.split('/')[2];
      const user = await env.DB
        .prepare('SELECT * FROM users WHERE id = ?')
        .bind(id)
        .first();

      if (!user) {
        return Response.json({ error: 'Not found' }, { status: 404 });
      }
      return Response.json(user);
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
  const db = this.getDB();
  const url = new URL(request.url);

  if (url.pathname === '/users' && request.method === 'GET') {
    // Note: no await, direct array return
    const users = db
      .prepare('SELECT * FROM users WHERE active = ?')
      .all(true);
    return Response.json(users);
  }

  if (url.pathname === '/users' && request.method === 'POST') {
    const { name, email } = await request.json();
    // Note: no await, params in run(), lastInsertRowid property
    const result = db
      .prepare('INSERT INTO users (name, email) VALUES (?, ?)')
      .run(name, email);
    return Response.json({ id: Number(result.lastInsertRowid) }, { status: 201 });
  }

  if (url.pathname.startsWith('/users/') && request.method === 'GET') {
    const id = url.pathname.split('/')[2];
    // Note: no await, .get() instead of .first()
    const user = db
      .prepare('SELECT * FROM users WHERE id = ?')
      .get(Number(id));

    if (!user) {
      return Response.json({ error: 'Not found' }, { status: 404 });
    }
    return Response.json(user);
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
      .get() as { c: number } | undefined;

    if (d1Count?.c !== dosqlCount?.c) {
      errors.push(
        `Count mismatch for ${table}: D1=${d1Count?.c}, DoSQL=${dosqlCount?.c}`
      );
    }

    // Compare checksums (order-independent)
    const d1Checksum = await d1
      .prepare(`SELECT SUM(id) as s FROM ${table}`)
      .first<{ s: number }>();
    const dosqlChecksum = dosql
      .prepare(`SELECT SUM(id) as s FROM ${table}`)
      .get() as { s: number } | undefined;

    if (d1Checksum?.s !== dosqlChecksum?.s) {
      errors.push(`Checksum mismatch for ${table}`);
    }
  }

  return { valid: errors.length === 0, errors };
}
```

### Step 9: Switch Traffic

Once validated, switch all traffic to DoSQL:

```typescript
// Use environment variable to control routing
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const USE_DOSQL = env.DOSQL_ENABLED === 'true';

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

1. Remove D1 binding from `wrangler.toml`
2. Remove D1-related code
3. Delete D1 database via Wrangler CLI

```bash
# Remove D1 database
wrangler d1 delete my-db
```

---

## API Compatibility Reference

### Method Mapping

| D1 Method | DoSQL Equivalent | Notes |
|-----------|------------------|-------|
| `db.prepare(sql)` | `db.prepare(sql)` | Both return a statement |
| `stmt.bind(...params)` | `stmt.bind(...params)` | DoSQL: chainable, optional |
| `stmt.all()` | `stmt.all(...params)` | DoSQL: params optional in call |
| `stmt.first()` | `stmt.get(...params)` | Method renamed |
| `stmt.first('col')` | `stmt.pluck().get()` | Different pattern |
| `stmt.run()` | `stmt.run(...params)` | Same name |
| `stmt.raw()` | `stmt.raw().all()` | Need to call all() |
| `db.exec(sql)` | `db.exec(sql)` | Same |
| `db.batch([...])` | `db.transaction(fn)` | Different paradigm |
| `db.dump()` | Not supported | Use manual export |

### Result Object Mapping

| D1 Property | DoSQL Property | Notes |
|-------------|----------------|-------|
| `result.results` | Direct return | No wrapper in DoSQL |
| `result.success` | N/A | DoSQL throws on error |
| `result.meta.duration` | Not available | Use manual timing |
| `result.meta.changes` | `result.changes` | Direct property |
| `result.meta.last_row_id` | `result.lastInsertRowid` | Renamed, bigint type |
| `result.meta.served_by` | Not available | - |
| `result.meta.rows_read` | Not available | - |
| `result.meta.rows_written` | Not available | - |

### SQL Feature Compatibility

| Feature | D1 | DoSQL | Notes |
|---------|-----|-------|-------|
| Parameter `?` | Yes | Yes | Sequential positional |
| Parameter `?1`, `?2` | Yes | Yes | Explicit positional |
| Parameter `:name` | No | Yes | DoSQL only |
| Parameter `$name` | No | Yes | DoSQL only |
| Parameter `@name` | No | Yes | DoSQL only |
| JSON functions | Yes | Yes | `json()`, `json_extract()`, etc. |
| FTS5 full-text search | Yes | Yes | Full-text search |
| Window functions | Yes | Yes | `ROW_NUMBER()`, `RANK()`, etc. |
| CTEs | Yes | Yes | `WITH` clauses |
| UPSERT | Yes | Yes | `ON CONFLICT` |
| Savepoints | Yes | Yes | Nested transactions |
| Time travel queries | No | Yes | DoSQL only |
| Virtual tables | No | Yes | DoSQL only |
| User-defined functions | No | Yes | DoSQL only |

---

## Common Gotchas and Solutions

### Gotcha 1: Async vs Sync API

**Problem:** D1 is async everywhere, DoSQL is sync for prepared statements.

```typescript
// D1 (async)
const users = await db.prepare('SELECT * FROM users').all();

// DoSQL (sync) - WRONG: don't use await
const users = await db.prepare('SELECT * FROM users').all(); // Unnecessary await

// DoSQL (sync) - CORRECT
const users = db.prepare('SELECT * FROM users').all();
```

**Solution:** Remove `await` from DoSQL prepared statement calls. Only database initialization can be async if using `DB()` factory.

---

### Gotcha 2: Result Wrapper Object

**Problem:** D1 returns `{ results, success, meta }`, DoSQL returns results directly.

```typescript
// D1
const result = await db.prepare('SELECT * FROM users').all();
const users = result.results; // Array is nested

// DoSQL - WRONG: no .results property
const result = db.prepare('SELECT * FROM users').all();
const users = result.results; // undefined!

// DoSQL - CORRECT
const users = db.prepare('SELECT * FROM users').all(); // Direct array
```

**Solution:** Access the array directly. For mutation metadata, capture the `run()` result.

---

### Gotcha 3: first() vs get()

**Problem:** Method is renamed and return type differs.

```typescript
// D1
const user = await db.prepare('...').bind(1).first();
// Returns: T | null

// DoSQL
const user = db.prepare('...').get(1);
// Returns: T | undefined
```

**Solution:** Replace `.first()` with `.get()`. Check for `undefined` instead of `null`.

---

### Gotcha 4: batch() vs transaction()

**Problem:** D1 `batch()` is a statement list, DoSQL `transaction()` is a function.

```typescript
// D1
const results = await db.batch([
  db.prepare('INSERT INTO a VALUES (?)').bind(1),
  db.prepare('INSERT INTO b VALUES (?)').bind(2),
]);

// DoSQL - WRONG: no batch() method
const results = db.batch([...]); // Error!

// DoSQL - CORRECT: use transaction()
const insertBoth = db.transaction(() => {
  db.prepare('INSERT INTO a VALUES (?)').run(1);
  db.prepare('INSERT INTO b VALUES (?)').run(2);
});
insertBoth(); // Execute the transaction
```

**Solution:** Wrap batch operations in `db.transaction()`. Remember to call the returned function.

---

### Gotcha 5: Binding Parameters

**Problem:** Parameters go to different places.

```typescript
// D1
db.prepare('SELECT * FROM users WHERE id = ?').bind(42).all();

// DoSQL - Method 1: params in execution method
db.prepare('SELECT * FROM users WHERE id = ?').all(42);

// DoSQL - Method 2: bind() is also available
db.prepare('SELECT * FROM users WHERE id = ?').bind(42).all();
```

**Solution:** Pass parameters directly to `all()`, `get()`, or `run()`. Or use `bind()` if you prefer.

---

### Gotcha 6: Durable Object Routing

**Problem:** D1 is a direct binding, DoSQL requires DO routing.

```typescript
// D1: Direct access
const result = await env.DB.prepare('...').all();

// DoSQL: Must route through DO
const id = env.DOSQL_DB.idFromName('tenant-id');
const stub = env.DOSQL_DB.get(id);
const response = await stub.fetch(new Request('http://internal/query'));
```

**Solution:** Create a Durable Object class and route requests to it. This adds complexity but enables per-tenant isolation.

---

### Gotcha 7: Cold Starts

**Problem:** DoSQL in Durable Objects has cold start overhead.

**Solution:** Cache the database instance and use hibernation-aware patterns:

```typescript
export class DatabaseDO implements DurableObject {
  private db: Database | null = null;

  private getDB(): Database {
    if (!this.db) {
      // This runs on cold start only
      this.db = createDatabase();
      this.runMigrations();
    }
    return this.db;
  }

  async fetch(request: Request): Promise<Response> {
    const db = this.getDB();
    // DB stays warm across requests
    // ...
  }
}
```

---

### Gotcha 8: No Read Replicas

**Problem:** D1 has read replicas, DoSQL doesn't.

**Solution:** Use the Cache API for read-heavy workloads:

```typescript
async fetch(request: Request): Promise<Response> {
  const url = new URL(request.url);
  const cacheKey = new Request(`https://cache${url.pathname}${url.search}`);
  const cache = caches.default;

  // Check cache first for GET requests
  if (request.method === 'GET') {
    const cached = await cache.match(cacheKey);
    if (cached) {
      return cached;
    }
  }

  // Query database
  const db = this.getDB();
  const data = db.prepare('SELECT * FROM products WHERE active = ?').all(true);

  const response = Response.json(data, {
    headers: { 'Cache-Control': 'max-age=60' },
  });

  // Cache GET responses
  if (request.method === 'GET') {
    await cache.put(cacheKey, response.clone());
  }

  return response;
}
```

---

### Gotcha 9: Error Handling Differences

**Problem:** D1 uses `success` flag, DoSQL throws errors.

```typescript
// D1
const result = await db.prepare('...').all();
if (!result.success) {
  // Handle error via flag
}

// DoSQL
import { DatabaseError } from '@dotdo/dosql';

try {
  const result = db.prepare('...').all();
} catch (error) {
  if (error instanceof DatabaseError) {
    // Handle via typed exception
    console.log(error.code, error.message);
  }
}
```

**Solution:** Wrap DoSQL calls in try/catch and use typed error handling.

---

### Gotcha 10: lastInsertRowid is BigInt

**Problem:** D1's `last_row_id` is a number, DoSQL's `lastInsertRowid` is a bigint.

```typescript
// D1
const result = await db.prepare('INSERT INTO users (name) VALUES (?)').bind('Alice').run();
const id = result.meta.last_row_id; // number

// DoSQL
const result = db.prepare('INSERT INTO users (name) VALUES (?)').run('Alice');
const id = result.lastInsertRowid; // bigint

// Convert if needed for JSON serialization
const jsonSafeId = Number(result.lastInsertRowid);
```

**Solution:** Use `Number()` to convert if you need a regular number for JSON.

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
        // D1 is primary, write to DoSQL async
        await this.d1.prepare(sql).bind(...params).run();
        try {
          this.dosql.prepare(sql).run(...params);
        } catch (e) {
          console.error('DoSQL shadow write failed:', e);
        }
        break;

      case 'dosql-primary':
        // DoSQL is primary, write to D1 async
        this.dosql.prepare(sql).run(...params);
        try {
          await this.d1.prepare(sql).bind(...params).run();
        } catch (e) {
          console.error('D1 shadow write failed:', e);
        }
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
- [ ] Audit all D1 queries in codebase (grep for env.DB, .bind(, .first(, etc.)
- [ ] Identify batch operations that need transaction conversion
- [ ] Review migrations for compatibility
- [ ] Set up DoSQL Durable Object class
- [ ] Test migrations in development
- [ ] Estimate data volume and migration time
- [ ] Plan maintenance window if needed

## Migration
- [ ] Deploy DO alongside D1
- [ ] Run schema migrations in DoSQL
- [ ] Run data migration script
- [ ] Validate data integrity (counts, checksums)
- [ ] Enable dual-write mode
- [ ] Monitor for discrepancies
- [ ] Run shadow tests on read queries

## Cutover
- [ ] Switch reads to DoSQL
- [ ] Monitor latency and error rates
- [ ] Run validation queries
- [ ] Disable D1 writes (dual-write -> dosql-only)
- [ ] Final data validation

## Post-Migration
- [ ] Monitor for 24-48 hours
- [ ] Remove D1 binding from wrangler.toml
- [ ] Remove D1-related code
- [ ] Delete D1 database
- [ ] Clean up migration code
- [ ] Update documentation
```

---

## Rollback Strategy

If migration fails, you need a quick rollback plan.

### Using Feature Flags

```typescript
// Control via environment variable
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const USE_DOSQL = env.DOSQL_ENABLED === 'true';

    if (USE_DOSQL) {
      const id = env.DOSQL_DB.idFromName('default');
      const stub = env.DOSQL_DB.get(id);
      return stub.fetch(request);
    }

    // Fallback to D1
    return handleD1Request(request, env.D1_DB);
  },
};

// Quick rollback via environment variable
// wrangler secret put DOSQL_ENABLED false
```

### Rollback Steps

1. **Immediate:** Set `DOSQL_ENABLED=false` to route traffic back to D1
2. **If dual-write was enabled:** Data should be in sync
3. **If dual-write was not enabled:** You may need to migrate data back to D1:

```typescript
// Reverse migration (DoSQL -> D1)
async function rollbackToD1(
  dosql: Database,
  d1: D1Database,
  tables: string[]
): Promise<void> {
  for (const table of tables) {
    const rows = dosql.prepare(`SELECT * FROM ${table}`).all();

    if (rows.length === 0) continue;

    const columns = Object.keys(rows[0] as object);
    const placeholders = columns.map(() => '?').join(', ');

    for (const row of rows) {
      const values = columns.map(c => (row as Record<string, unknown>)[c]);
      await d1
        .prepare(`INSERT OR REPLACE INTO ${table} (${columns.join(', ')}) VALUES (${placeholders})`)
        .bind(...values)
        .run();
    }
  }
}
```

4. **Investigate:** Check logs and error patterns before retrying migration

---

## Advanced Migration Patterns

### Multi-Tenant Migration

If your D1 database serves multiple tenants, migrate to per-tenant DOs:

```typescript
// Extract tenant data from D1 and create separate DOs
async function migrateToMultiTenant(
  d1: D1Database,
  env: Env,
  tenantIds: string[]
): Promise<void> {
  for (const tenantId of tenantIds) {
    // Get tenant's data from D1
    const users = await d1
      .prepare('SELECT * FROM users WHERE tenant_id = ?')
      .bind(tenantId)
      .all();

    // Route to tenant-specific DO
    const id = env.DOSQL_DB.idFromName(tenantId);
    const stub = env.DOSQL_DB.get(id);

    // Send data to tenant's DO
    await stub.fetch(new Request('http://internal/import', {
      method: 'POST',
      body: JSON.stringify({ users: users.results }),
    }));
  }
}
```

### Gradual Rollout

Migrate a percentage of traffic:

```typescript
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const ROLLOUT_PERCENT = parseInt(env.DOSQL_ROLLOUT_PERCENT || '0', 10);

    // Use consistent hashing for user
    const userId = getUserId(request);
    const hash = simpleHash(userId);
    const useDoSQL = (hash % 100) < ROLLOUT_PERCENT;

    if (useDoSQL) {
      const id = env.DOSQL_DB.idFromName('default');
      const stub = env.DOSQL_DB.get(id);
      return stub.fetch(request);
    }

    return handleD1Request(request, env.D1_DB);
  },
};

function simpleHash(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) - hash) + str.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash);
}
```

---

## Next Steps

After successfully migrating, explore DoSQL's unique features:

- **[Time Travel](./advanced.md#time-travel)** - Query historical data with `FOR SYSTEM_TIME AS OF`
- **[Branching](./advanced.md#branching)** - Git-like database branches for testing
- **[CDC Streaming](./advanced.md#cdc-streaming)** - Real-time change capture to external systems
- **[Virtual Tables](./advanced.md#virtual-tables)** - Query R2, URLs, and APIs directly in SQL
- **[User-Defined Functions](./api-reference.md#user-defined-functions)** - Add custom SQL functions

For issues during migration, see [Troubleshooting](./TROUBLESHOOTING.md) or file an issue on GitHub.
