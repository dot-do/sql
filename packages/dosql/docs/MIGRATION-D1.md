# Migrating from D1 to DoSQL

A practical guide for migrating Cloudflare Workers applications from D1 to DoSQL.

## Table of Contents

- [Overview](#overview)
- [API Differences](#api-differences)
- [Step-by-Step Migration](#step-by-step-migration)
- [Code Examples](#code-examples)
- [Gotchas and Limitations](#gotchas-and-limitations)
- [Testing Migration](#testing-migration)

---

## Overview

### Why Migrate from D1 to DoSQL?

| Reason | D1 | DoSQL | Benefit |
|--------|-----|-------|---------|
| **Data Locality** | Regional | Per-DO | Lower latency for tenant-specific data |
| **Time Travel** | No | Yes | Query historical data, audit trails |
| **Branching** | No | Yes | Git-like branches for testing/staging |
| **CDC Streaming** | No | Yes | Real-time change capture to lakehouse |
| **Virtual Tables** | No | Yes | Query R2, URLs, APIs directly in SQL |
| **Bundle Size** | N/A | 7KB | Minimal impact on Worker size |
| **Multi-tenancy** | Shared DB | DO per tenant | True isolation |

### When NOT to Migrate

- You need a managed service with zero operational overhead
- Your data doesn't have natural tenant boundaries
- You're using D1's read replicas across regions
- Your team is unfamiliar with Durable Objects

### Migration Complexity Assessment

| Scenario | Complexity | Estimated Time |
|----------|------------|----------------|
| Simple CRUD app | Low | 1-2 hours |
| App with transactions | Low | 2-4 hours |
| App with batch operations | Medium | 4-8 hours |
| Multi-database app | Medium | 1-2 days |
| App with complex migrations | High | 2-5 days |

---

## API Differences

### Core API Comparison

| Operation | D1 | DoSQL |
|-----------|-----|-------|
| Create database | `env.DB` (binding) | `await DB('name', options)` |
| Simple query | `db.prepare(sql).all()` | `db.prepare(sql).all()` |
| With params | `db.prepare(sql).bind(...).all()` | `db.prepare(sql).all(...params)` |
| First row | `db.prepare(sql).first()` | `db.prepare(sql).get()` |
| Execute DDL | `db.exec(sql)` | `db.exec(sql)` |
| Run mutation | `db.prepare(sql).run()` | `db.prepare(sql).run()` |
| Batch queries | `db.batch([...])` | Transaction wrapper |

### Detailed Method Mapping

```typescript
// D1 Method                      // DoSQL Equivalent
db.prepare(sql).all()            db.prepare(sql).all()
db.prepare(sql).first()          db.prepare(sql).get()
db.prepare(sql).first('column')  db.prepare(sql).pluck().get()
db.prepare(sql).run()            db.prepare(sql).run()
db.prepare(sql).raw()            db.prepare(sql).raw().all()
db.exec(sql)                     db.exec(sql)
db.batch([stmt1, stmt2])         db.transaction(() => { ... })()
db.dump()                        // Not supported (use WAL export)
```

### Query Syntax Differences

Both D1 and DoSQL use SQLite syntax. Key differences:

| Feature | D1 | DoSQL |
|---------|-----|-------|
| Parameter binding | `?1`, `?2` or `?` | `?`, `:name`, `$name`, `@name` |
| JSON functions | Full SQLite JSON1 | Full SQLite JSON1 |
| FTS5 | Supported | Supported |
| Window functions | Supported | Supported |
| CTEs | Supported | Supported |
| **Time travel** | Not available | `FOR SYSTEM_TIME AS OF` |
| **Virtual tables** | Not available | URL, R2, API sources |

### Transaction Handling Differences

**D1 Transactions:**
```typescript
// D1 uses batch for implicit transactions
const results = await db.batch([
  db.prepare('INSERT INTO users (name) VALUES (?)').bind('Alice'),
  db.prepare('INSERT INTO users (name) VALUES (?)').bind('Bob'),
]);
```

**DoSQL Transactions:**
```typescript
// DoSQL uses explicit transaction wrapper
const insertUsers = db.transaction((users: string[]) => {
  const stmt = db.prepare('INSERT INTO users (name) VALUES (?)');
  for (const name of users) {
    stmt.run(name);
  }
});

insertUsers(['Alice', 'Bob']);
```

### Return Type Differences

**D1 Results:**
```typescript
interface D1Result<T> {
  results: T[];           // Query results
  success: boolean;       // Operation success
  meta: {
    duration: number;     // Execution time
    changes: number;      // Rows affected
    last_row_id: number;  // Last insert ID
    served_by: string;    // Region info
  };
}
```

**DoSQL Results:**
```typescript
// For queries (all/get)
type QueryResult<T> = T[] | T | undefined;

// For mutations (run)
interface RunResult {
  changes: number;           // Rows affected
  lastInsertRowid: number;   // Last insert ID (or bigint)
}
```

---

## Step-by-Step Migration

### Step 1: Install DoSQL

```bash
npm install @dotdo/dosql
```

### Step 2: Create a Durable Object Database Class

```typescript
// src/database.ts
import { DB, type Database } from '@dotdo/dosql';

export interface Env {
  // Keep D1 binding during migration
  D1_DB: D1Database;
  // Add DO namespace
  DOSQL_DB: DurableObjectNamespace;
}

export class DatabaseDO implements DurableObject {
  private db: Database | null = null;
  private state: DurableObjectState;

  constructor(state: DurableObjectState) {
    this.state = state;
  }

  private async getDB(): Promise<Database> {
    if (!this.db) {
      this.db = await DB('main', {
        migrations: { folder: '.do/migrations' },
        storage: { hot: this.state.storage },
      });
    }
    return this.db;
  }

  async fetch(request: Request): Promise<Response> {
    const db = await this.getDB();
    // Handle requests...
  }
}
```

### Step 3: Convert D1 Migrations to DoSQL Format

**D1 migration location:** `migrations/` or via Wrangler CLI

**DoSQL migration location:** `.do/migrations/`

```bash
# Create migrations directory
mkdir -p .do/migrations

# Copy existing migrations
cp migrations/*.sql .do/migrations/
```

Rename migrations to follow DoSQL convention:
```
.do/migrations/
  001_create_users.sql
  002_add_posts.sql
  003_add_indexes.sql
```

### Step 4: Update Wrangler Configuration

```jsonc
// wrangler.jsonc
{
  "name": "my-app",
  "main": "src/index.ts",
  "compatibility_date": "2024-01-01",

  // Keep D1 during migration
  "d1_databases": [
    { "binding": "D1_DB", "database_name": "my-db", "database_id": "xxx" }
  ],

  // Add Durable Objects
  "durable_objects": {
    "bindings": [
      { "name": "DOSQL_DB", "class_name": "DatabaseDO" }
    ]
  },
  "migrations": [
    { "tag": "v1", "new_classes": ["DatabaseDO"] }
  ]
}
```

### Step 5: Create Migration Wrapper

During migration, create a wrapper that can switch between D1 and DoSQL:

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

  async query<T>(sql: string, params: unknown[] = []): Promise<QueryResult<T>> {
    if (this.useDoSQL && this.dosql) {
      const results = this.dosql.prepare(sql).all(...params) as T[];
      return { results, changes: 0, lastInsertRowid: 0 };
    }

    if (this.d1) {
      const result = await this.d1.prepare(sql).bind(...params).all<T>();
      return {
        results: result.results,
        changes: result.meta.changes,
        lastInsertRowid: result.meta.last_row_id,
      };
    }

    throw new Error('No database configured');
  }

  async run(sql: string, params: unknown[] = []): Promise<{ changes: number; lastInsertRowid: number | bigint }> {
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

  async exec(sql: string): Promise<void> {
    if (this.useDoSQL && this.dosql) {
      this.dosql.exec(sql);
      return;
    }

    if (this.d1) {
      await this.d1.exec(sql);
      return;
    }

    throw new Error('No database configured');
  }
}
```

### Step 6: Migrate Data

```typescript
// scripts/migrate-data.ts
import type { D1Database } from '@cloudflare/workers-types';
import type { Database } from '@dotdo/dosql';

interface MigrationOptions {
  tables: string[];
  batchSize?: number;
  onProgress?: (table: string, count: number, total: number) => void;
}

export async function migrateD1ToDoSQL(
  d1: D1Database,
  dosql: Database,
  options: MigrationOptions
): Promise<void> {
  const { tables, batchSize = 1000, onProgress } = options;

  for (const table of tables) {
    // Get total count
    const countResult = await d1.prepare(`SELECT COUNT(*) as count FROM ${table}`).first<{ count: number }>();
    const total = countResult?.count ?? 0;

    if (total === 0) {
      onProgress?.(table, 0, 0);
      continue;
    }

    // Get column names
    const columns = await d1.prepare(`PRAGMA table_info(${table})`).all();
    const columnNames = columns.results.map((c: any) => c.name).join(', ');
    const placeholders = columns.results.map(() => '?').join(', ');

    // Migrate in batches
    let offset = 0;
    const insertStmt = dosql.prepare(
      `INSERT INTO ${table} (${columnNames}) VALUES (${placeholders})`
    );

    const insertBatch = dosql.transaction((rows: unknown[][]) => {
      for (const row of rows) {
        insertStmt.run(...row);
      }
    });

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

// Usage in a Worker
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === '/migrate' && request.method === 'POST') {
      const doId = env.DOSQL_DB.idFromName('main');
      const doStub = env.DOSQL_DB.get(doId);

      // Trigger migration in DO
      return doStub.fetch(new Request('http://internal/migrate', {
        method: 'POST',
        body: JSON.stringify({ d1Binding: 'D1_DB' }),
      }));
    }

    return new Response('Not found', { status: 404 });
  },
};
```

### Step 7: Switch Application Code

Replace D1 calls with DoSQL:

```typescript
// Before (D1)
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const users = await env.D1_DB
      .prepare('SELECT * FROM users WHERE active = ?')
      .bind(true)
      .all();

    return Response.json(users.results);
  },
};

// After (DoSQL)
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const doId = env.DOSQL_DB.idFromName('default');
    const doStub = env.DOSQL_DB.get(doId);

    return doStub.fetch(new Request('http://internal/users', {
      method: 'GET',
    }));
  },
};

// In DatabaseDO class
async fetch(request: Request): Promise<Response> {
  const db = await this.getDB();
  const url = new URL(request.url);

  if (url.pathname === '/users' && request.method === 'GET') {
    const users = db.prepare('SELECT * FROM users WHERE active = ?').all(true);
    return Response.json(users);
  }

  return new Response('Not found', { status: 404 });
}
```

### Step 8: Validate and Cutover

```typescript
// Validation script
async function validateMigration(d1: D1Database, dosql: Database): Promise<boolean> {
  const tables = ['users', 'posts', 'comments'];

  for (const table of tables) {
    // Compare counts
    const d1Count = await d1.prepare(`SELECT COUNT(*) as c FROM ${table}`).first<{ c: number }>();
    const dosqlCount = dosql.prepare(`SELECT COUNT(*) as c FROM ${table}`).get() as { c: number };

    if (d1Count?.c !== dosqlCount?.c) {
      console.error(`Count mismatch for ${table}: D1=${d1Count?.c}, DoSQL=${dosqlCount?.c}`);
      return false;
    }

    // Compare checksums (sample)
    const d1Sample = await d1.prepare(`SELECT * FROM ${table} ORDER BY id LIMIT 100`).all();
    const dosqlSample = dosql.prepare(`SELECT * FROM ${table} ORDER BY id LIMIT 100`).all();

    if (JSON.stringify(d1Sample.results) !== JSON.stringify(dosqlSample)) {
      console.error(`Data mismatch for ${table}`);
      return false;
    }
  }

  return true;
}
```

---

## Code Examples

### Basic CRUD Operations

**D1 (Before):**
```typescript
// Create
const result = await db.prepare(
  'INSERT INTO users (name, email) VALUES (?, ?)'
).bind('Alice', 'alice@example.com').run();
const userId = result.meta.last_row_id;

// Read
const user = await db.prepare(
  'SELECT * FROM users WHERE id = ?'
).bind(userId).first();

// Update
await db.prepare(
  'UPDATE users SET name = ? WHERE id = ?'
).bind('Alicia', userId).run();

// Delete
await db.prepare(
  'DELETE FROM users WHERE id = ?'
).bind(userId).run();
```

**DoSQL (After):**
```typescript
// Create
const result = db.prepare(
  'INSERT INTO users (name, email) VALUES (?, ?)'
).run('Alice', 'alice@example.com');
const userId = result.lastInsertRowid;

// Read
const user = db.prepare(
  'SELECT * FROM users WHERE id = ?'
).get(userId);

// Update
db.prepare(
  'UPDATE users SET name = ? WHERE id = ?'
).run('Alicia', userId);

// Delete
db.prepare(
  'DELETE FROM users WHERE id = ?'
).run(userId);
```

### Batch Operations to Transactions

**D1 (Before):**
```typescript
// D1 batch - implicit transaction
const results = await db.batch([
  db.prepare('INSERT INTO orders (user_id, total) VALUES (?, ?)').bind(1, 99.99),
  db.prepare('INSERT INTO order_items (order_id, product_id, qty) VALUES (?, ?, ?)').bind(1, 100, 2),
  db.prepare('UPDATE inventory SET stock = stock - ? WHERE product_id = ?').bind(2, 100),
]);

// Check all succeeded
const allSuccess = results.every(r => r.success);
```

**DoSQL (After):**
```typescript
// DoSQL transaction - explicit
const createOrder = db.transaction((userId: number, total: number, items: OrderItem[]) => {
  const orderResult = db.prepare(
    'INSERT INTO orders (user_id, total) VALUES (?, ?)'
  ).run(userId, total);

  const orderId = orderResult.lastInsertRowid;

  const insertItem = db.prepare(
    'INSERT INTO order_items (order_id, product_id, qty) VALUES (?, ?, ?)'
  );
  const updateInventory = db.prepare(
    'UPDATE inventory SET stock = stock - ? WHERE product_id = ?'
  );

  for (const item of items) {
    insertItem.run(orderId, item.productId, item.qty);
    updateInventory.run(item.qty, item.productId);
  }

  return { orderId, itemCount: items.length };
});

// Execute - automatically commits or rolls back
const result = createOrder(1, 99.99, [{ productId: 100, qty: 2 }]);
```

### Parameterized Queries

**D1 (Before):**
```typescript
// Positional parameters
const users = await db.prepare(
  'SELECT * FROM users WHERE role = ?1 AND active = ?2'
).bind('admin', true).all();

// All positional
const posts = await db.prepare(
  'SELECT * FROM posts WHERE user_id = ? AND published = ?'
).bind(userId, true).all();
```

**DoSQL (After):**
```typescript
// Positional parameters
const users = db.prepare(
  'SELECT * FROM users WHERE role = ? AND active = ?'
).all('admin', true);

// Named parameters
const posts = db.prepare(
  'SELECT * FROM posts WHERE user_id = :userId AND published = :published'
).all({ userId, published: true });

// Alternative named syntax
const comments = db.prepare(
  'SELECT * FROM comments WHERE post_id = $postId'
).all({ postId: 123 });
```

### Error Handling

**D1 (Before):**
```typescript
try {
  const result = await db.prepare('SELECT * FROM nonexistent').all();
  if (!result.success) {
    console.error('Query failed');
  }
} catch (error) {
  console.error('D1 error:', error);
}
```

**DoSQL (After):**
```typescript
import { DatabaseError, DatabaseErrorCode } from '@dotdo/dosql';

try {
  const result = db.prepare('SELECT * FROM nonexistent').all();
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
        console.error('Database error:', error.code, error.message);
    }
  } else {
    throw error;
  }
}
```

### Typed Queries

**D1 (Before):**
```typescript
interface User {
  id: number;
  name: string;
  email: string;
}

// D1 typing via generic
const user = await db.prepare(
  'SELECT * FROM users WHERE id = ?'
).bind(1).first<User>();
```

**DoSQL (After):**
```typescript
interface User {
  id: number;
  name: string;
  email: string;
}

// DoSQL typing via prepare generic
const user = db.prepare<User>(
  'SELECT * FROM users WHERE id = ?'
).get(1);
// user is User | undefined

// With parameter types
const users = db.prepare<User, [string, boolean]>(
  'SELECT * FROM users WHERE role = ? AND active = ?'
).all('admin', true);
// users is User[]
```

---

## Gotchas and Limitations

### Features Not Yet Supported

| Feature | D1 | DoSQL | Workaround |
|---------|-----|-------|------------|
| `db.dump()` | Yes | No | Export via WAL reader |
| Read replicas | Yes | No | Use DO hibernation |
| Global distribution | Yes | No | Route to nearest region |
| REST API | Yes | No | Build custom API layer |
| Console UI | Yes | No | Build admin dashboard |

### Performance Considerations

1. **Cold starts**: DoSQL in Durable Objects has cold start overhead
   ```typescript
   // Mitigate with hibernation API
   async fetch(request: Request): Promise<Response> {
     // DB stays warm across hibernation
     const db = await this.getDB();
     // ...
   }
   ```

2. **No read replicas**: All reads go to single DO
   ```typescript
   // Mitigate with caching
   const cache = caches.default;
   const cacheKey = new Request(`https://cache/${cacheKeyString}`);

   let data = await cache.match(cacheKey);
   if (!data) {
     data = await doStub.fetch(request);
     await cache.put(cacheKey, data.clone());
   }
   ```

3. **Batch vs Transaction overhead**: Transactions have per-statement overhead
   ```typescript
   // For bulk inserts, use a single prepared statement
   const bulkInsert = db.transaction((rows: Row[]) => {
     const stmt = db.prepare('INSERT INTO t (a, b) VALUES (?, ?)');
     for (const row of rows) {
       stmt.run(row.a, row.b);
     }
   });
   ```

### Rollback Strategy

If migration fails, you can rollback:

```typescript
// 1. Keep D1 as source of truth during migration
// 2. Implement feature flag for gradual rollout
const USE_DOSQL = env.DOSQL_ENABLED === 'true';

async function getUsers(env: Env): Promise<User[]> {
  if (USE_DOSQL) {
    const doId = env.DOSQL_DB.idFromName('default');
    const stub = env.DOSQL_DB.get(doId);
    const response = await stub.fetch('http://internal/users');
    return response.json();
  }

  // Fallback to D1
  const result = await env.D1_DB.prepare('SELECT * FROM users').all<User>();
  return result.results;
}

// 3. Quick rollback via environment variable
// wrangler secret put DOSQL_ENABLED --value false
```

---

## Testing Migration

### Dual-Write Pattern

Write to both D1 and DoSQL during migration:

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
        // D1 first, then DoSQL (async, don't wait)
        await this.d1.prepare(sql).bind(...params).run();
        this.dosql.prepare(sql).run(...params);
        break;

      case 'dosql-primary':
        // DoSQL first, then D1 (async, don't wait)
        this.dosql.prepare(sql).run(...params);
        await this.d1.prepare(sql).bind(...params).run();
        break;

      case 'dosql-only':
        this.dosql.prepare(sql).run(...params);
        break;
    }
  }
}
```

### Shadow Testing

Compare D1 and DoSQL results in production:

```typescript
async function shadowTest<T>(
  d1: D1Database,
  dosql: Database,
  sql: string,
  params: unknown[]
): Promise<{ match: boolean; d1Result: T[]; dosqlResult: T[] }> {
  const [d1Result, dosqlResult] = await Promise.all([
    d1.prepare(sql).bind(...params).all<T>(),
    Promise.resolve(dosql.prepare(sql).all(...params) as T[]),
  ]);

  const match = JSON.stringify(d1Result.results) === JSON.stringify(dosqlResult);

  if (!match) {
    console.warn('Shadow test mismatch:', {
      sql,
      params,
      d1Count: d1Result.results.length,
      dosqlCount: dosqlResult.length,
    });
  }

  return {
    match,
    d1Result: d1Result.results,
    dosqlResult,
  };
}

// Usage
const { match, d1Result } = await shadowTest(
  env.D1_DB,
  dosql,
  'SELECT * FROM users WHERE active = ?',
  [true]
);

// Return D1 result but log mismatches
return Response.json(d1Result);
```

### Data Validation

```typescript
interface ValidationResult {
  table: string;
  d1Count: number;
  dosqlCount: number;
  sampleMatch: boolean;
  checksumMatch: boolean;
}

async function validateTable(
  d1: D1Database,
  dosql: Database,
  table: string
): Promise<ValidationResult> {
  // Count comparison
  const d1Count = (await d1.prepare(`SELECT COUNT(*) as c FROM ${table}`).first<{ c: number }>())?.c ?? 0;
  const dosqlCount = (dosql.prepare(`SELECT COUNT(*) as c FROM ${table}`).get() as { c: number })?.c ?? 0;

  // Sample comparison
  const d1Sample = await d1.prepare(`SELECT * FROM ${table} ORDER BY rowid LIMIT 100`).all();
  const dosqlSample = dosql.prepare(`SELECT * FROM ${table} ORDER BY rowid LIMIT 100`).all();
  const sampleMatch = JSON.stringify(d1Sample.results) === JSON.stringify(dosqlSample);

  // Checksum (for small tables)
  let checksumMatch = true;
  if (d1Count <= 10000) {
    const d1All = await d1.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all();
    const dosqlAll = dosql.prepare(`SELECT * FROM ${table} ORDER BY rowid`).all();
    checksumMatch = JSON.stringify(d1All.results) === JSON.stringify(dosqlAll);
  }

  return {
    table,
    d1Count,
    dosqlCount,
    sampleMatch,
    checksumMatch,
  };
}

// Run validation
async function runValidation(
  d1: D1Database,
  dosql: Database,
  tables: string[]
): Promise<void> {
  console.log('Starting validation...');

  for (const table of tables) {
    const result = await validateTable(d1, dosql, table);

    const status = result.d1Count === result.dosqlCount &&
                   result.sampleMatch &&
                   result.checksumMatch
                   ? 'PASS' : 'FAIL';

    console.log(`[${status}] ${table}:`, {
      counts: `D1=${result.d1Count}, DoSQL=${result.dosqlCount}`,
      sampleMatch: result.sampleMatch,
      checksumMatch: result.checksumMatch,
    });
  }
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

## Migration
- [ ] Deploy DO alongside D1
- [ ] Run data migration script
- [ ] Validate data integrity
- [ ] Enable dual-write mode
- [ ] Monitor for discrepancies

## Post-Migration
- [ ] Switch reads to DoSQL
- [ ] Monitor latency and errors
- [ ] Disable D1 writes
- [ ] Remove D1 binding
- [ ] Clean up migration code
```

---

## Next Steps

After migrating from D1, explore DoSQL's unique features:

- **[Time Travel](./advanced.md#time-travel)** - Query historical data
- **[Branching](./advanced.md#branching)** - Git-like database branches
- **[CDC Streaming](./advanced.md#cdc-streaming)** - Real-time change capture
- **[Virtual Tables](./advanced.md#virtual-tables)** - Query R2, URLs, APIs directly

For support, see [Troubleshooting](./TROUBLESHOOTING.md) or file an issue.
