# Migrating from D1 to DoSQL

A comprehensive guide for Cloudflare Workers developers migrating from D1 to DoSQL.

## Table of Contents

- [Quick Reference Card](#quick-reference-card)
- [Should You Migrate?](#should-you-migrate)
- [Understanding the Architecture Difference](#understanding-the-architecture-difference)
- [Side-by-Side Comparison](#side-by-side-comparison)
  - [Database Connection](#database-connection)
  - [Query Methods](#query-methods)
  - [Parameter Binding](#parameter-binding)
  - [Transactions](#transactions)
  - [Error Handling](#error-handling)
  - [TypeScript Generics](#typescript-generics)
- [Step-by-Step Migration](#step-by-step-migration)
- [API Compatibility Reference](#api-compatibility-reference)
- [Common Migration Pitfalls](#common-migration-pitfalls)
- [Testing Your Migration](#testing-your-migration)
- [Rollback Strategy](#rollback-strategy)

---

## Quick Reference Card

Keep this reference handy during migration.

### Method Mappings

| D1 | DoSQL | Notes |
|----|-------|-------|
| `env.DB` | `env.DOSQL_DB.get(id)` | DoSQL uses Durable Objects |
| `.prepare(sql).bind(...).all()` | `db.query(sql, [...])` | High-level API |
| `.prepare(sql).bind(...).first()` | `db.queryOne(sql, [...])` | Returns `undefined` not `null` |
| `.prepare(sql).bind(...).run()` | `db.run(sql, [...])` | Same pattern |
| `db.batch([...])` | `db.transaction(async (tx) => {...})` | Explicit transactions |
| `result.results` | Direct array return | No wrapper object |
| `result.meta.last_row_id` | `result.lastInsertRowId` | Property renamed |
| `result.meta.changes` | `result.rowsAffected` | Property renamed |

### Return Value Differences

| Operation | D1 Returns | DoSQL Returns |
|-----------|------------|---------------|
| `all()` / `query()` | `{ results: T[], success, meta }` | `T[]` directly |
| `first()` / `queryOne()` | `T \| null` | `T \| undefined` |
| `run()` | `{ meta: { changes, last_row_id } }` | `{ rowsAffected, lastInsertRowId }` |
| `first('column')` | Column value | Not supported (use SQL) |

---

## Should You Migrate?

### Migrate to DoSQL When You Need

| Requirement | D1 | DoSQL | Description |
|-------------|-----|-------|-------------|
| Per-tenant isolation | Shared database | Per-tenant DO | Complete data isolation per customer |
| Data locality | Regional | Edge-local | Database lives where the user is |
| Time travel | Not available | Built-in | Query historical data at any point |
| Git-like branching | Not available | Built-in | Create branches for testing |
| CDC streaming | Not available | Built-in | Stream changes to external systems |
| Virtual tables | Not available | Built-in | Query R2, URLs, APIs in SQL |

### Stay with D1 When

| Scenario | Reason |
|----------|--------|
| Single shared database | D1 is simpler for shared multi-tenant |
| Global read replicas needed | D1 has built-in replication |
| Team unfamiliar with DOs | Durable Objects add complexity |
| Mature CLI tooling required | D1 has Wrangler migration commands |
| Low write volume, simple app | D1 may be sufficient |

### Migration Effort Estimate

| Application Type | Effort | Time Estimate |
|------------------|--------|---------------|
| Simple CRUD | Low | 1-2 hours |
| App with transactions | Low | 2-4 hours |
| Multi-database app | Medium | 1-2 days |
| Complex migrations setup | Medium | 2-4 days |
| High-traffic production | High | 1-2 weeks |

---

## Understanding the Architecture Difference

The fundamental difference between D1 and DoSQL is the deployment model:

**D1**: A managed SQLite service that you connect to via bindings. All requests go to a regional database.

**DoSQL**: A database engine that runs inside your Durable Object. Each DO instance is an isolated database.

```
D1 Architecture:
  Worker Request --> D1 Binding --> Regional D1 Database

DoSQL Architecture:
  Worker Request --> DO Stub --> Durable Object (contains DoSQL database)
```

This means:
- **D1**: One database shared by all requests
- **DoSQL**: Many databases, one per Durable Object instance

For multi-tenant apps, this is powerful: `env.DOSQL_DB.idFromName(tenantId)` gives each tenant their own isolated database.

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
import { DB } from '@dotdo/dosql';

export interface Env {
  DOSQL_DB: DurableObjectNamespace;
}

export class DatabaseDO implements DurableObject {
  private db: Awaited<ReturnType<typeof DB>> | null = null;

  constructor(private state: DurableObjectState) {}

  private async getDB() {
    if (!this.db) {
      this.db = await DB('mydb', {
        migrations: { folder: '.do/migrations' },
        storage: { hot: this.state.storage },
      });
    }
    return this.db;
  }

  async fetch(request: Request): Promise<Response> {
    const db = await this.getDB();
    const users = await db.query('SELECT * FROM users');
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
- D1 is a direct binding; DoSQL requires a Durable Object class
- D1 database is shared; DoSQL provides per-instance isolation
- DoSQL migrations are embedded in code, not managed by Wrangler

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
// D1: Results wrapped in object
const result = await db
  .prepare('SELECT * FROM users WHERE active = ?')
  .bind(true)
  .all();

// Access via .results
const users = result.results;

// Metadata available
console.log(result.meta.duration);
console.log(result.success);
```

</td>
<td>

```typescript
// DoSQL: Direct array return
const users = await db.query(
  'SELECT * FROM users WHERE active = ?',
  [true]
);

// users IS the array
// No wrapper, no metadata
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
// D1: first() returns null if not found
const user = await db
  .prepare('SELECT * FROM users WHERE id = ?')
  .bind(42)
  .first();
// user is T | null

// Get specific column
const name = await db
  .prepare('SELECT name FROM users WHERE id = ?')
  .bind(42)
  .first('name');
// name is string | null
```

</td>
<td>

```typescript
// DoSQL: queryOne() returns undefined
const user = await db.queryOne(
  'SELECT * FROM users WHERE id = ?',
  [42]
);
// user is T | undefined

// Get specific column - use SQL
const result = await db.queryOne<{ name: string }>(
  'SELECT name FROM users WHERE id = ?',
  [42]
);
const name = result?.name;
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
```

</td>
<td>

```typescript
// DoSQL: run() returns flat result
const result = await db.run(
  'INSERT INTO users (name, email) VALUES (?, ?)',
  ['Alice', 'alice@example.com']
);

// Access directly
const userId = result.lastInsertRowId;
const rowsAffected = result.rowsAffected;
// No success - throws on error
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
// D1: .bind() before execution
const users = await db
  .prepare('SELECT * FROM users WHERE role = ? AND active = ?')
  .bind('admin', true)
  .all();

// Positional ?1, ?2
const posts = await db
  .prepare('SELECT * FROM posts WHERE user_id = ?1')
  .bind(userId)
  .all();

// Named parameters NOT supported
```

</td>
<td>

```typescript
// DoSQL: Parameters in method call
const users = await db.query(
  'SELECT * FROM users WHERE role = ? AND active = ?',
  ['admin', true]
);

// Positional with array
const posts = await db.query(
  'SELECT * FROM posts WHERE user_id = ?',
  [userId]
);

// Both positional and named work
// See low-level API for named params
```

</td>
</tr>
</table>

---

### Transactions

<table>
<tr>
<th width="50%">D1</th>
<th width="50%">DoSQL</th>
</tr>
<tr>
<td>

```typescript
// D1: batch() for implicit transaction
const results = await db.batch([
  db.prepare('INSERT INTO orders (user_id, total) VALUES (?, ?)')
    .bind(1, 99.99),
  db.prepare('INSERT INTO order_items (order_id, product_id) VALUES (?, ?)')
    .bind(1, 42),
  db.prepare('UPDATE inventory SET stock = stock - 1 WHERE product_id = ?')
    .bind(42),
]);

// Cannot use result of one statement in another
// All succeed or all fail together
```

</td>
<td>

```typescript
// DoSQL: transaction() with callback
await db.transaction(async (tx) => {
  // Can use results from previous statements
  const order = await tx.run(
    'INSERT INTO orders (user_id, total) VALUES (?, ?)',
    [1, 99.99]
  );

  const orderId = order.lastInsertRowId;

  await tx.run(
    'INSERT INTO order_items (order_id, product_id) VALUES (?, ?)',
    [orderId, 42]
  );

  await tx.run(
    'UPDATE inventory SET stock = stock - 1 WHERE product_id = ?',
    [42]
  );
});
// Auto-rollback on any error
```

</td>
</tr>
</table>

**Key Differences:**
- D1 `batch()` is a list of independent statements
- DoSQL `transaction()` is a function with sequential logic
- DoSQL can use results from earlier statements in later ones

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
// D1: Check success + try/catch
try {
  const result = await db
    .prepare('SELECT * FROM users')
    .all();

  if (!result.success) {
    console.error('Query failed');
    console.error(result.error);
  }

  return result.results;
} catch (error) {
  // Network/binding errors
  console.error('D1 error:', error);
}
```

</td>
<td>

```typescript
// DoSQL: Exception-based
import { DoSQLError } from '@dotdo/dosql';

try {
  const users = await db.query('SELECT * FROM users');
  return users;
} catch (error) {
  if (error instanceof DoSQLError) {
    console.error(`[${error.code}] ${error.message}`);
    // error.code is a typed error code
  }
  throw error;
}

// Transactions auto-rollback on error
await db.transaction(async (tx) => {
  await tx.run('INSERT INTO users (email) VALUES (?)', ['test@example.com']);
  throw new Error('Abort!');
  // INSERT is automatically rolled back
});
```

</td>
</tr>
</table>

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

const user = await db
  .prepare('SELECT * FROM users WHERE id = ?')
  .bind(1)
  .first<User>();
// user is User | null

const users = await db
  .prepare('SELECT * FROM users')
  .all<User>();
// users.results is User[]
```

</td>
<td>

```typescript
// DoSQL: Generic on query methods
interface User {
  id: number;
  name: string;
  email: string;
}

const user = await db.queryOne<User>(
  'SELECT * FROM users WHERE id = ?',
  [1]
);
// user is User | undefined

const users = await db.query<User>(
  'SELECT * FROM users'
);
// users is User[]
```

</td>
</tr>
</table>

---

## Step-by-Step Migration

### Step 1: Install DoSQL

```bash
npm install @dotdo/dosql
```

### Step 2: Create the Durable Object Class

Create `src/database.ts`:

```typescript
import { DB } from '@dotdo/dosql';

export interface Env {
  // Keep D1 during migration (optional)
  D1_DB?: D1Database;
  // Add DoSQL namespace
  DOSQL_DB: DurableObjectNamespace;
}

export class DatabaseDO implements DurableObject {
  private db: Awaited<ReturnType<typeof DB>> | null = null;

  constructor(
    private state: DurableObjectState,
    private env: Env
  ) {}

  private async getDB() {
    if (!this.db) {
      this.db = await DB('app', {
        migrations: { folder: '.do/migrations' },
        storage: { hot: this.state.storage },
      });
    }
    return this.db;
  }

  async fetch(request: Request): Promise<Response> {
    const db = await this.getDB();
    const url = new URL(request.url);

    // Your route handlers here
    return new Response('Not found', { status: 404 });
  }
}
```

### Step 3: Create Migration Files

Create `.do/migrations/001_init.sql`:

```sql
-- Copy your D1 schema here
CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT UNIQUE,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE posts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  title TEXT NOT NULL,
  body TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (user_id) REFERENCES users(id)
);
```

### Step 4: Update wrangler.toml

```toml
name = "my-app"
main = "src/index.ts"
compatibility_date = "2024-12-01"

# Keep D1 during migration (optional)
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

# Include migration files
[rules]
  [[rules.glob]]
  pattern = ".do/migrations/*.sql"
  type = "Data"
```

### Step 5: Convert Your Routes

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
    const users = await db.query(
      'SELECT * FROM users WHERE active = ?',
      [true]
    );
    return Response.json(users);
  }

  if (url.pathname === '/users' && request.method === 'POST') {
    const { name, email } = await request.json();
    const result = await db.run(
      'INSERT INTO users (name, email) VALUES (?, ?)',
      [name, email]
    );
    return Response.json({ id: result.lastInsertRowId }, { status: 201 });
  }

  return new Response('Not found', { status: 404 });
}
```

### Step 6: Migrate Existing Data (Optional)

If you have data in D1, migrate it to DoSQL:

```typescript
async function migrateData(d1: D1Database, dosql: Database): Promise<void> {
  const tables = ['users', 'posts'];

  for (const table of tables) {
    // Get data from D1
    const result = await d1.prepare(`SELECT * FROM ${table}`).all();

    if (result.results.length === 0) continue;

    // Get columns
    const columns = Object.keys(result.results[0]);
    const placeholders = columns.map(() => '?').join(', ');

    // Insert into DoSQL
    await dosql.transaction(async (tx) => {
      for (const row of result.results) {
        const values = columns.map(c => row[c]);
        await tx.run(
          `INSERT INTO ${table} (${columns.join(', ')}) VALUES (${placeholders})`,
          values
        );
      }
    });

    console.log(`Migrated ${result.results.length} rows from ${table}`);
  }
}
```

### Step 7: Validate and Switch

```typescript
// Use environment variable for feature flag
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const USE_DOSQL = env.USE_DOSQL === 'true';

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

### Step 8: Remove D1

After confirming DoSQL works:

1. Remove D1 binding from `wrangler.toml`
2. Remove D1-related code
3. Delete D1 database: `wrangler d1 delete my-db`

---

## API Compatibility Reference

### Method Mapping

| D1 Method | DoSQL Equivalent | Notes |
|-----------|------------------|-------|
| `db.prepare(sql)` | Used internally | High-level API preferred |
| `stmt.bind(...params)` | Pass params to method | `db.query(sql, params)` |
| `stmt.all()` | `db.query()` | Direct array return |
| `stmt.first()` | `db.queryOne()` | Returns `undefined` not `null` |
| `stmt.first('col')` | Not supported | Use SQL column selection |
| `stmt.run()` | `db.run()` | Same purpose |
| `stmt.raw()` | Not in high-level API | Use low-level if needed |
| `db.exec(sql)` | Not exposed | Use `db.run()` |
| `db.batch([...])` | `db.transaction()` | Different paradigm |
| `db.dump()` | Not supported | Use manual export |

### Result Object Mapping

| D1 Property | DoSQL Property | Notes |
|-------------|----------------|-------|
| `result.results` | Direct return | No wrapper |
| `result.success` | N/A | Throws on error |
| `result.meta.duration` | Not available | Use manual timing |
| `result.meta.changes` | `result.rowsAffected` | Renamed |
| `result.meta.last_row_id` | `result.lastInsertRowId` | Renamed |

### SQL Feature Compatibility

| Feature | D1 | DoSQL |
|---------|-----|-------|
| Positional `?` | Yes | Yes |
| Positional `?1`, `?2` | Yes | Yes |
| Named `:name` | No | Yes (low-level API) |
| JSON functions | Yes | Yes |
| FTS5 full-text search | Yes | Yes |
| Window functions | Yes | Yes |
| CTEs | Yes | Yes |
| UPSERT | Yes | Yes |
| Time travel | No | Yes |
| Virtual tables | No | Yes |

---

## Common Migration Pitfalls

### Pitfall 1: Forgetting the Wrapper Object

**D1** returns `{ results, success, meta }`. **DoSQL** returns the array directly.

```typescript
// D1
const result = await db.prepare('SELECT * FROM users').all();
const users = result.results;  // Need .results

// DoSQL
const users = await db.query('SELECT * FROM users');
// users IS the array
```

### Pitfall 2: null vs undefined

**D1** `first()` returns `null`. **DoSQL** `queryOne()` returns `undefined`.

```typescript
// D1
const user = await db.prepare('...').first();
if (user === null) { /* not found */ }

// DoSQL
const user = await db.queryOne('...');
if (user === undefined) { /* not found */ }
// Or simply: if (!user)
```

### Pitfall 3: batch() Does Not Exist

**D1** uses `batch([])`. **DoSQL** uses `transaction()`.

```typescript
// D1
await db.batch([
  db.prepare('INSERT INTO a VALUES (?)').bind(1),
  db.prepare('INSERT INTO b VALUES (?)').bind(2),
]);

// DoSQL
await db.transaction(async (tx) => {
  await tx.run('INSERT INTO a VALUES (?)', [1]);
  await tx.run('INSERT INTO b VALUES (?)', [2]);
});
```

### Pitfall 4: Routing to Durable Object

D1 is a direct binding. DoSQL requires routing through the DO.

```typescript
// D1
const result = await env.DB.prepare('...').all();

// DoSQL - must route through DO
const id = env.DOSQL_DB.idFromName('tenant-id');
const stub = env.DOSQL_DB.get(id);
const response = await stub.fetch(new Request('http://internal/api'));
```

### Pitfall 5: Property Names Changed

| D1 | DoSQL |
|----|-------|
| `result.meta.last_row_id` | `result.lastInsertRowId` |
| `result.meta.changes` | `result.rowsAffected` |

---

## Testing Your Migration

### Dual-Write Pattern

Write to both databases during migration to keep them in sync:

```typescript
class DualWriteDB {
  constructor(
    private d1: D1Database,
    private dosql: Database,
    private primary: 'd1' | 'dosql'
  ) {}

  async run(sql: string, params: unknown[]): Promise<void> {
    if (this.primary === 'd1') {
      await this.d1.prepare(sql).bind(...params).run();
      try {
        await this.dosql.run(sql, params);
      } catch (e) {
        console.error('DoSQL shadow write failed:', e);
      }
    } else {
      await this.dosql.run(sql, params);
      try {
        await this.d1.prepare(sql).bind(...params).run();
      } catch (e) {
        console.error('D1 shadow write failed:', e);
      }
    }
  }
}
```

### Validation Script

Compare counts and checksums between databases:

```typescript
async function validateMigration(
  d1: D1Database,
  dosql: Database
): Promise<{ valid: boolean; errors: string[] }> {
  const tables = ['users', 'posts'];
  const errors: string[] = [];

  for (const table of tables) {
    const d1Count = await d1
      .prepare(`SELECT COUNT(*) as c FROM ${table}`)
      .first<{ c: number }>();

    const dosqlCount = await dosql.queryOne<{ c: number }>(
      `SELECT COUNT(*) as c FROM ${table}`
    );

    if (d1Count?.c !== dosqlCount?.c) {
      errors.push(`Count mismatch for ${table}: D1=${d1Count?.c}, DoSQL=${dosqlCount?.c}`);
    }
  }

  return { valid: errors.length === 0, errors };
}
```

### Migration Checklist

**Pre-Migration:**
- [ ] Audit all D1 queries (grep for `env.DB`, `.bind(`, `.first(`)
- [ ] Identify `batch()` calls that need transaction conversion
- [ ] Copy D1 schema to `.do/migrations/`
- [ ] Set up DoSQL Durable Object class
- [ ] Test migrations locally with `wrangler dev`

**Migration:**
- [ ] Deploy DO alongside D1
- [ ] Run data migration script
- [ ] Validate data integrity (counts, checksums)
- [ ] Enable dual-write mode
- [ ] Monitor for discrepancies

**Cutover:**
- [ ] Switch reads to DoSQL
- [ ] Monitor latency and error rates
- [ ] Disable D1 writes
- [ ] Final validation

**Post-Migration:**
- [ ] Monitor for 24-48 hours
- [ ] Remove D1 binding from wrangler.toml
- [ ] Remove D1-related code
- [ ] Delete D1 database

---

## Rollback Strategy

### Quick Rollback with Feature Flags

```typescript
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const USE_DOSQL = env.USE_DOSQL === 'true';

    if (USE_DOSQL) {
      const id = env.DOSQL_DB.idFromName('default');
      const stub = env.DOSQL_DB.get(id);
      return stub.fetch(request);
    }

    return handleD1Request(request, env.D1_DB);
  },
};

// Rollback: wrangler secret put USE_DOSQL false
```

### Rollback Steps

1. Set `USE_DOSQL=false` to route traffic back to D1
2. If dual-write was enabled, data should be in sync
3. Investigate issues in logs
4. Fix and retry migration

---

## Next Steps

After successfully migrating, explore DoSQL's unique features:

- **[Time Travel](./advanced.md#time-travel-queries)** - Query historical data
- **[Branching](./advanced.md#branching)** - Git-like database branches
- **[CDC Streaming](./advanced.md#cdc-streaming-patterns)** - Real-time change capture
- **[Virtual Tables](./advanced.md#virtual-tables)** - Query R2, URLs in SQL

For issues, see [Troubleshooting](./TROUBLESHOOTING.md) or file an issue on GitHub.
