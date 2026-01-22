# DoSQL Documentation

> Type-safe SQL for TypeScript with Cloudflare Durable Objects

DoSQL is a database engine purpose-built for Cloudflare Workers and Durable Objects. It provides type-safe SQL queries with compile-time validation, automatic schema migrations, and advanced features like time travel, branching, and CDC streaming to lakehouse storage.

## Quick Start

```bash
npm install @dotdo/dosql
```

```typescript
import { DB } from '@dotdo/dosql';

// Create a database with auto-migrations
const db = await DB('my-tenant', {
  migrations: { folder: '.do/migrations' },
});

// Type-safe queries
const users = await db.query('SELECT * FROM users WHERE active = ?', [true]);
```

## Key Features

| Feature | Description |
|---------|-------------|
| **Type-Safe SQL** | Compile-time query validation with TypeScript |
| **Zero Dependencies** | Pure TypeScript, ~7KB gzipped |
| **Auto Migrations** | `.do/migrations/*.sql` convention |
| **Time Travel** | Query data at any point in time |
| **Branch/Merge** | Git-like branching for databases |
| **CDC Streaming** | Real-time change capture to lakehouse |
| **Virtual Tables** | Query URLs, R2, and remote APIs directly |
| **CapnWeb RPC** | Efficient DO-to-DO communication |

## Documentation

| Document | Description |
|----------|-------------|
| [Getting Started](./getting-started.md) | Installation, basic usage, CRUD operations |
| [API Reference](./api-reference.md) | Complete API documentation |
| [Advanced Features](./advanced.md) | Time travel, branching, CDC, vector search |
| [Architecture](./architecture.md) | Storage tiers, bundle optimization, internals |

## Bundle Size

DoSQL is extremely compact, designed for Cloudflare's bundle limits:

| Bundle | Gzipped | CF Free (1MB) |
|--------|---------|---------------|
| Worker Bundle | **7.36 KB** | 0.7% |
| Full Library | **34.26 KB** | 3.3% |

## Platform Support

- Cloudflare Workers (primary)
- Cloudflare Durable Objects
- Cloudflare R2 (cold storage)
- Node.js (for testing)

## Example: Complete CRUD

```typescript
import { DB } from '@dotdo/dosql';

// Initialize database
const db = await DB('tenants', {
  migrations: [
    {
      id: '001_create_users',
      sql: `
        CREATE TABLE users (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          name TEXT NOT NULL,
          email TEXT UNIQUE,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
      `,
    },
  ],
});

// Create
await db.run('INSERT INTO users (name, email) VALUES (?, ?)', ['Alice', 'alice@example.com']);

// Read
const users = await db.query('SELECT * FROM users');

// Update
await db.run('UPDATE users SET name = ? WHERE id = ?', ['Alicia', 1]);

// Delete
await db.run('DELETE FROM users WHERE id = ?', [1]);

// Transaction
await db.transaction(async (tx) => {
  await tx.run('INSERT INTO users (name) VALUES (?)', ['Bob']);
  await tx.run('INSERT INTO users (name) VALUES (?)', ['Carol']);
});
```

## Example: Time Travel

```typescript
import { DB } from '@dotdo/dosql';

const db = await DB('analytics');

// Query current data
const current = await db.query('SELECT * FROM metrics');

// Query data as of 1 hour ago
const historical = await db.query(
  "SELECT * FROM metrics FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 10:00:00'"
);

// Query data at specific LSN
const snapshot = await db.query(
  'SELECT * FROM metrics FOR SYSTEM_TIME AS OF LSN 12345'
);
```

## Example: Virtual Tables

```typescript
import { DB } from '@dotdo/dosql';

const db = await DB('analytics');

// Query JSON API directly
const users = await db.query(`
  SELECT id, name
  FROM 'https://api.example.com/users.json'
  WHERE role = 'admin'
`);

// Query Parquet from R2
const sales = await db.query(`
  SELECT SUM(amount) as total
  FROM 'r2://mybucket/data/sales.parquet'
  WHERE year = 2024
`);

// Query CSV with options
const data = await db.query(`
  SELECT *
  FROM 'https://data.gov/dataset.csv'
  WITH (headers=true, delimiter=',')
`);
```

## License

MIT
