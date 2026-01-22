# DoSQL

[![npm version](https://img.shields.io/npm/v/@dotdo/dosql.svg)](https://www.npmjs.com/package/@dotdo/dosql)
[![Bundle Size](https://img.shields.io/badge/bundle-7.4KB_gzip-brightgreen)](./docs/architecture.md#bundle-size-optimization)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.3+-blue.svg)](https://www.typescriptlang.org/)
[![Cloudflare Workers](https://img.shields.io/badge/Cloudflare-Workers-orange.svg)](https://workers.cloudflare.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

> Type-safe SQL for TypeScript with Cloudflare Durable Objects

DoSQL is a database engine purpose-built for Cloudflare Workers and Durable Objects. It provides type-safe SQL queries with compile-time validation, automatic schema migrations, and advanced features like time travel, branching, and CDC streaming.

## Features

- **Type-Safe SQL** - Compile-time query validation with TypeScript
- **Zero Dependencies** - Pure TypeScript, ~7KB gzipped
- **Auto Migrations** - Simple `.do/migrations/*.sql` convention
- **Time Travel** - Query data at any point in time
- **Branch/Merge** - Git-like branching for databases
- **CDC Streaming** - Real-time change capture to lakehouse
- **Virtual Tables** - Query URLs, R2, and APIs directly
- **CapnWeb RPC** - Efficient DO-to-DO communication

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

// Transactions
await db.transaction(async (tx) => {
  await tx.run('INSERT INTO orders (user_id) VALUES (?)', [1]);
  await tx.run('UPDATE users SET order_count = order_count + 1 WHERE id = ?', [1]);
});
```

## Bundle Size

| Bundle | Gzipped | CF Free (1MB) |
|--------|---------|---------------|
| Worker | **7.36 KB** | 0.7% |
| Full Library | **34.26 KB** | 3.3% |

## Documentation

| Document | Description |
|----------|-------------|
| [Getting Started](./docs/getting-started.md) | Installation, usage, CRUD, migrations |
| [API Reference](./docs/api-reference.md) | Complete API documentation |
| [Advanced Features](./docs/advanced.md) | Time travel, branching, CDC, vector search |
| [Architecture](./docs/architecture.md) | Storage tiers, bundle optimization, internals |

## Example: Migrations

```
.do/
└── migrations/
    ├── 001_create_users.sql
    ├── 002_add_posts.sql
    └── 003_add_indexes.sql
```

```sql
-- .do/migrations/001_create_users.sql
CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT UNIQUE,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
```

## Example: Time Travel

```typescript
// Query current data
const current = await db.query('SELECT * FROM metrics');

// Query data as of 1 hour ago
const historical = await db.query(`
  SELECT * FROM metrics
  FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 10:00:00'
`);
```

## Example: Branching

```typescript
// Create a branch for testing
await db.branch('feature-x');
await db.checkout('feature-x');

// Make changes on branch
await db.run('ALTER TABLE users ADD COLUMN role TEXT');

// Merge back to main
await db.checkout('main');
await db.merge('feature-x');
```

## Example: Virtual Tables

```typescript
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
```

## Example: CDC Streaming

```typescript
import { createCDC } from '@dotdo/dosql/cdc';

const cdc = createCDC(backend);

for await (const event of cdc.subscribe(0n)) {
  console.log('Change:', event.op, event.table);
  await syncToLakehouse(event);
}
```

## Cloudflare Workers Deployment

```typescript
// src/database.ts
import { DB } from '@dotdo/dosql';

export class TenantDatabase implements DurableObject {
  private db: Database | null = null;

  constructor(private state: DurableObjectState, private env: Env) {}

  private async getDB(): Promise<Database> {
    if (!this.db) {
      this.db = await DB('tenant', {
        migrations: { folder: '.do/migrations' },
        storage: { hot: this.state.storage, cold: this.env.R2_BUCKET },
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

## Comparison

| Feature | DoSQL | SQLite-WASM | PGLite | DuckDB-WASM |
|---------|-------|-------------|--------|-------------|
| Bundle Size | **7KB** | ~500KB | ~3MB | ~4MB |
| Type Safety | Yes | No | No | No |
| Time Travel | Yes | No | No | No |
| Branching | Yes | No | No | No |
| CDC | Yes | No | No | No |
| Workers Native | Yes | Partial | No | Partial |

## Module Exports

```typescript
// Core
import { DB, createDatabase } from '@dotdo/dosql';

// Transactions
import { TransactionManager } from '@dotdo/dosql/transaction';

// WAL
import { createWALWriter, createWALReader } from '@dotdo/dosql/wal';

// CDC
import { createCDC, createCDCSubscription } from '@dotdo/dosql/cdc';

// FSX (Storage)
import { createDOBackend, createR2Backend, createTieredBackend } from '@dotdo/dosql/fsx';

// RPC
import { createWebSocketClient, DoSQLTarget } from '@dotdo/dosql/rpc';

// Sharding
import { createShardRouter, createShardExecutor } from '@dotdo/dosql/sharding';

// Procedures
import { procedure, createProcedureRegistry } from '@dotdo/dosql/proc';

// Virtual Tables
import { createVirtualTableRegistry } from '@dotdo/dosql/virtual';
```

## Requirements

- Cloudflare Workers / Durable Objects
- TypeScript 5.3+
- Node.js 18+ (for development)

## License

MIT

## Links

- [GitHub](https://github.com/dotdo/dosql)
- [npm](https://www.npmjs.com/package/@dotdo/dosql)
- [Documentation](./docs/README.md)
- [Changelog](./CHANGELOG.md)
