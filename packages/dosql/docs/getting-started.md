# Getting Started with DoSQL

This guide covers installation, basic usage, CRUD operations, migrations, and deploying to Cloudflare Workers.

## Table of Contents

- [Installation](#installation)
- [Basic Usage with DB()](#basic-usage-with-db)
- [Creating Tables](#creating-tables)
- [CRUD Operations](#crud-operations)
- [Migrations](#migrations)
- [Deploying to Cloudflare Workers](#deploying-to-cloudflare-workers)

---

## Installation

```bash
npm install @dotdo/dosql
```

### Peer Dependencies

DoSQL has optional peer dependencies for Cloudflare Workers:

```bash
npm install @cloudflare/workers-types --save-dev
```

### TypeScript Configuration

Add to your `tsconfig.json`:

```json
{
  "compilerOptions": {
    "types": ["@cloudflare/workers-types"],
    "strict": true,
    "moduleResolution": "bundler"
  }
}
```

---

## Basic Usage with DB()

The `DB()` function is the primary interface for DoSQL. It creates or connects to a database with automatic migration support.

### Simple Database

```typescript
import { DB } from '@dotdo/dosql';

// Create a database with a name
const db = await DB('my-database');

// Execute queries
const result = await db.query('SELECT 1 + 1 as sum');
console.log(result); // [{ sum: 2 }]
```

### Database with Migrations

```typescript
import { DB } from '@dotdo/dosql';

const db = await DB('tenants', {
  migrations: { folder: '.do/migrations' },
  autoMigrate: true, // default: true
});
```

### Database with Inline Migrations

```typescript
import { DB } from '@dotdo/dosql';

const db = await DB('tenants', {
  migrations: [
    {
      id: '001_init',
      sql: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)',
    },
    {
      id: '002_add_email',
      sql: 'ALTER TABLE users ADD COLUMN email TEXT',
    },
  ],
});
```

### Database Options

```typescript
interface DBOptions {
  // Migration source: folder path, inline array, or async loader
  migrations?: MigrationSource;

  // Auto-apply migrations on first access (default: true)
  autoMigrate?: boolean;

  // Migration table name (default: '__dosql_migrations')
  migrationsTable?: string;

  // Storage tier configuration
  storage?: {
    hot: DurableObjectStorage;
    cold?: R2Bucket;
  };

  // Enable WAL for durability
  wal?: boolean;

  // Logging
  logger?: Logger;
}
```

---

## Creating Tables

### Basic Table Creation

```sql
-- .do/migrations/001_create_users.sql
CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT UNIQUE,
  active BOOLEAN DEFAULT true,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
```

### Table with Indexes

```sql
-- .do/migrations/002_create_posts.sql
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

### Supported Column Types

| SQL Type | TypeScript Type | Notes |
|----------|-----------------|-------|
| `INTEGER` | `number` | 64-bit signed integer |
| `REAL` | `number` | 64-bit floating point |
| `TEXT` | `string` | UTF-8 string |
| `BLOB` | `Uint8Array` | Binary data |
| `BOOLEAN` | `boolean` | Stored as 0/1 |
| `NULL` | `null` | Explicit null |

### Constraints

```sql
CREATE TABLE products (
  id INTEGER PRIMARY KEY,
  sku TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  price REAL CHECK (price >= 0),
  quantity INTEGER DEFAULT 0,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
```

---

## CRUD Operations

### Create (INSERT)

```typescript
// Single insert
await db.run(
  'INSERT INTO users (name, email) VALUES (?, ?)',
  ['Alice', 'alice@example.com']
);

// Get the last inserted ID
const result = await db.run(
  'INSERT INTO users (name, email) VALUES (?, ?)',
  ['Bob', 'bob@example.com']
);
console.log(result.lastInsertRowId); // 2

// Insert with named parameters
await db.run(
  'INSERT INTO users (name, email) VALUES (:name, :email)',
  { name: 'Carol', email: 'carol@example.com' }
);

// Bulk insert
await db.transaction(async (tx) => {
  for (const user of users) {
    await tx.run('INSERT INTO users (name, email) VALUES (?, ?)', [user.name, user.email]);
  }
});
```

### Read (SELECT)

```typescript
// Get all rows
const users = await db.query('SELECT * FROM users');

// Get with conditions
const activeUsers = await db.query(
  'SELECT * FROM users WHERE active = ?',
  [true]
);

// Get single row
const user = await db.queryOne('SELECT * FROM users WHERE id = ?', [1]);

// Get with LIMIT and OFFSET
const page = await db.query(
  'SELECT * FROM users ORDER BY id LIMIT ? OFFSET ?',
  [10, 20]
);

// Join tables
const postsWithUsers = await db.query(`
  SELECT p.*, u.name as author_name
  FROM posts p
  JOIN users u ON u.id = p.user_id
  WHERE p.published_at IS NOT NULL
`);

// Aggregate functions
const stats = await db.queryOne(`
  SELECT
    COUNT(*) as total,
    COUNT(CASE WHEN active THEN 1 END) as active,
    MAX(created_at) as newest
  FROM users
`);
```

### Update (UPDATE)

```typescript
// Update single row
await db.run('UPDATE users SET name = ? WHERE id = ?', ['Alicia', 1]);

// Update with multiple conditions
await db.run(
  'UPDATE users SET active = ? WHERE created_at < ? AND active = ?',
  [false, '2024-01-01', true]
);

// Update with expression
await db.run('UPDATE products SET quantity = quantity - ? WHERE id = ?', [1, 42]);

// Get affected row count
const result = await db.run('UPDATE users SET active = false WHERE active = true');
console.log(result.rowsAffected); // number of updated rows
```

### Delete (DELETE)

```typescript
// Delete single row
await db.run('DELETE FROM users WHERE id = ?', [1]);

// Delete with conditions
await db.run('DELETE FROM users WHERE active = ? AND created_at < ?', [false, '2023-01-01']);

// Delete all (use with caution)
await db.run('DELETE FROM users');

// Truncate equivalent (faster for large tables)
await db.run('DELETE FROM users');
await db.run('VACUUM'); // Reclaim space
```

### Prepared Statements

```typescript
// Create a prepared statement
const stmt = db.prepare('SELECT * FROM users WHERE active = ?');

// Execute multiple times with different parameters
const activeUsers = await stmt.all([true]);
const inactiveUsers = await stmt.all([false]);

// Get single row
const firstActive = await stmt.get([true]);

// Run for modifications
const insertStmt = db.prepare('INSERT INTO users (name) VALUES (?)');
await insertStmt.run(['Alice']);
await insertStmt.run(['Bob']);
```

---

## Migrations

### Migration File Convention

DoSQL uses a simple `.do/migrations/*.sql` convention:

```
your-project/
├── .do/
│   └── migrations/
│       ├── 001_create_users.sql
│       ├── 002_add_posts.sql
│       └── 003_add_indexes.sql
├── src/
└── package.json
```

### Migration File Format

```sql
-- .do/migrations/001_create_users.sql
-- Comments are supported

CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT UNIQUE
);

-- Multiple statements in one file
CREATE INDEX idx_users_email ON users(email);
```

### Naming Convention

- Format: `NNN_descriptive_name.sql`
- Prefix: 3-digit number for ordering (001, 002, etc.)
- Separator: Underscore
- Extension: `.sql`

### Migration Tracking

DoSQL tracks applied migrations in `__dosql_migrations`:

```sql
-- Created automatically
CREATE TABLE __dosql_migrations (
  id TEXT PRIMARY KEY,       -- Migration ID
  applied_at TEXT NOT NULL,  -- ISO timestamp
  checksum TEXT NOT NULL,    -- SHA-256 of SQL
  duration_ms INTEGER        -- Execution time
);
```

### Drizzle Kit Compatibility

DoSQL also supports Drizzle Kit migrations:

```typescript
// Load from Drizzle folder
const db = await DB('tenants', {
  migrations: { drizzle: './drizzle' },
});
```

### Manual Migration Control

```typescript
import { createMigrationRunner, createSchemaTracker } from '@dotdo/dosql/migrations';

// In a Durable Object
export class TenantDB {
  private tracker: SchemaTracker;
  private runner: MigrationRunner;

  constructor(state: DurableObjectState) {
    this.tracker = createSchemaTracker(state.storage);
    this.runner = createMigrationRunner(this.db);
  }

  async migrate(migrations: Migration[]) {
    const status = await this.runner.getStatus(migrations);

    if (status.needsMigration) {
      const result = await this.runner.migrate(migrations);
      console.log(`Applied ${result.applied.length} migrations`);
    }
  }
}
```

---

## Deploying to Cloudflare Workers

### Project Structure

```
your-project/
├── .do/
│   └── migrations/
│       └── 001_init.sql
├── src/
│   ├── index.ts        # Worker entry point
│   └── database.ts     # Database class
├── wrangler.jsonc
├── package.json
└── tsconfig.json
```

### Wrangler Configuration

```jsonc
// wrangler.jsonc
{
  "$schema": "node_modules/wrangler/config-schema.json",
  "name": "my-app",
  "main": "src/index.ts",
  "compatibility_date": "2024-01-01",

  // Durable Objects
  "durable_objects": {
    "bindings": [
      { "name": "DOSQL_DB", "class_name": "TenantDatabase" }
    ]
  },

  // DO migrations (not SQL migrations)
  "migrations": [
    { "tag": "v1", "new_classes": ["TenantDatabase"] }
  ],

  // Optional: R2 for cold storage
  "r2_buckets": [
    { "binding": "DATA_BUCKET", "bucket_name": "my-data-bucket" }
  ]
}
```

### Worker Implementation

```typescript
// src/index.ts
import { TenantDatabase } from './database';

export { TenantDatabase };

export interface Env {
  DOSQL_DB: DurableObjectNamespace;
  DATA_BUCKET?: R2Bucket;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const tenantId = url.searchParams.get('tenant') || 'default';

    // Get or create Durable Object for tenant
    const id = env.DOSQL_DB.idFromName(tenantId);
    const db = env.DOSQL_DB.get(id);

    // Forward request to DO
    return db.fetch(request);
  },
};
```

### Database Durable Object

```typescript
// src/database.ts
import { DB } from '@dotdo/dosql';

export class TenantDatabase implements DurableObject {
  private db: Database | null = null;
  private state: DurableObjectState;
  private env: Env;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;
  }

  private async getDB(): Promise<Database> {
    if (!this.db) {
      this.db = await DB('tenant', {
        migrations: { folder: '.do/migrations' },
        storage: {
          hot: this.state.storage,
          cold: this.env.DATA_BUCKET,
        },
      });
    }
    return this.db;
  }

  async fetch(request: Request): Promise<Response> {
    const db = await this.getDB();
    const url = new URL(request.url);

    if (url.pathname === '/users' && request.method === 'GET') {
      const users = await db.query('SELECT * FROM users');
      return Response.json(users);
    }

    if (url.pathname === '/users' && request.method === 'POST') {
      const body = await request.json() as { name: string; email: string };
      await db.run(
        'INSERT INTO users (name, email) VALUES (?, ?)',
        [body.name, body.email]
      );
      return Response.json({ success: true }, { status: 201 });
    }

    return new Response('Not Found', { status: 404 });
  }
}
```

### Deploying

```bash
# Deploy to Cloudflare
npx wrangler deploy

# Deploy to specific environment
npx wrangler deploy --env production

# Test locally
npx wrangler dev
```

### Environment Variables

```bash
# Set secrets
npx wrangler secret put DATABASE_KEY
```

### Monitoring

```bash
# View logs
npx wrangler tail

# View DO analytics
npx wrangler durable-objects list
```

---

## Next Steps

- [API Reference](./api-reference.md) - Complete API documentation
- [Advanced Features](./advanced.md) - Time travel, branching, CDC
- [Architecture](./architecture.md) - Understanding DoSQL internals
