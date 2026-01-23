# DoSQL

> The database engine built for Cloudflare Durable Objects

[![npm version](https://img.shields.io/npm/v/@dotdo/dosql.svg)](https://www.npmjs.com/package/@dotdo/dosql)
[![Bundle Size](https://img.shields.io/badge/bundle-7.4KB_gzip-brightgreen)](./docs/architecture.md#bundle-size)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.3+-blue.svg)](https://www.typescriptlang.org/)
[![Cloudflare Workers](https://img.shields.io/badge/Cloudflare-Workers-orange.svg)](https://workers.cloudflare.com/)

---

**You're building stateful applications on Cloudflare.** You need a database that lives inside your Durable Objects, survives hibernation, and scales without managing infrastructure. DoSQL gives you exactly that: a type-safe SQL database engine purpose-built for the DO runtime.

```
Client --> Worker --> Durable Object (DoSQL) --> R2/Lakehouse
                            |
                      SQLite Storage
                      Hibernation-Safe
                      Auto-Compaction
```

## The Durable Object Database

DoSQL runs **inside** your Durable Object. Not as a separate service. Not as an external database. Your data lives where your compute lives.

```typescript
import { DurableObject } from 'cloudflare:workers';
import { DB, type Database } from '@dotdo/dosql';

export class UserDatabase extends DurableObject<Env> {
  private db: Database | null = null;

  private async getDB(): Promise<Database> {
    if (!this.db) {
      this.db = await DB('users', {
        migrations: { folder: '.do/migrations' },
        storage: {
          hot: this.ctx.storage,      // DO's built-in SQLite
          cold: this.env.R2_BUCKET,   // Overflow to R2
        },
      });
    }
    return this.db;
  }

  async createUser(name: string, email: string): Promise<User> {
    const db = await this.getDB();
    const result = await db.run(
      'INSERT INTO users (name, email) VALUES (?, ?) RETURNING *',
      [name, email]
    );
    return result.rows[0] as User;
  }

  async getUser(id: number): Promise<User | null> {
    const db = await this.getDB();
    const result = await db.query<User>(
      'SELECT * FROM users WHERE id = ?',
      [id]
    );
    return result.rows[0] ?? null;
  }
}
```

## Why DoSQL for Durable Objects?

| Feature | DoSQL | DO SQLite (raw) | D1 | Turso |
|---------|-------|-----------------|----|----|
| **Runs in DO** | Yes | Yes | No (external) | No (external) |
| **Hibernation-safe** | Yes | Manual | N/A | N/A |
| **Type-safe queries** | Yes | No | No | No |
| **Auto-migrations** | Yes | No | Yes | Yes |
| **Time travel** | Yes | No | No | Yes |
| **CDC streaming** | Yes | No | No | No |
| **Cross-DO queries** | Yes | No | No | No |
| **Lakehouse sync** | Yes | No | No | No |
| **Bundle size** | 7KB | 0KB | N/A | ~50KB |

D1 and Turso are great databases. But they're external services. Every query is a network hop. DoSQL eliminates that latency by running inside your Durable Object.

## Quick Start

### 1. Install

```bash
npm install @dotdo/dosql
```

### 2. Create your Durable Object

```typescript
// src/database.ts
import { DurableObject } from 'cloudflare:workers';
import { DB, type Database } from '@dotdo/dosql';

interface Env {
  R2_BUCKET: R2Bucket;
}

export class TenantDatabase extends DurableObject<Env> {
  private db: Database | null = null;

  private async getDB(): Promise<Database> {
    if (!this.db) {
      this.db = await DB('tenant', {
        migrations: { folder: '.do/migrations' },
        storage: {
          hot: this.ctx.storage,
          cold: this.env.R2_BUCKET,
        },
      });
    }
    return this.db;
  }

  async fetch(request: Request): Promise<Response> {
    const db = await this.getDB();
    const url = new URL(request.url);

    if (url.pathname === '/query' && request.method === 'POST') {
      const { sql, params } = await request.json();
      const result = await db.query(sql, params);
      return Response.json(result);
    }

    return new Response('Not found', { status: 404 });
  }
}
```

### 3. Configure wrangler.toml

```toml
name = "my-app"
compatibility_date = "2026-01-01"

[durable_objects]
bindings = [
  { name = "TENANT_DB", class_name = "TenantDatabase" }
]

[[migrations]]
tag = "v1"
new_classes = ["TenantDatabase"]

[[r2_buckets]]
binding = "R2_BUCKET"
bucket_name = "my-lakehouse"
```

### 4. Create migrations

```
.do/
└── migrations/
    ├── 001_create_users.sql
    └── 002_create_orders.sql
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

### 5. Route to your DO

```typescript
// src/index.ts
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const tenantId = url.searchParams.get('tenant') ?? 'default';

    // Each tenant gets their own DO instance with isolated data
    const id = env.TENANT_DB.idFromName(tenantId);
    const stub = env.TENANT_DB.get(id);

    return stub.fetch(request);
  }
};
```

## DO-Specific Features

### Hibernation Support

Durable Objects hibernate when idle. DoSQL handles this gracefully:

```typescript
export class HibernatingDatabase extends DurableObject<Env> {
  private db: Database | null = null;

  // Called when DO wakes from hibernation
  async webSocketMessage(ws: WebSocket, message: string) {
    // DB reconnects automatically on first use
    const db = await this.getDB();
    const result = await db.query(message);
    ws.send(JSON.stringify(result));
  }

  // State survives hibernation - SQLite is durable
  async webSocketClose(ws: WebSocket) {
    // Optional: flush pending writes before hibernate
    await this.db?.flush();
  }
}
```

### Alarm-Based Compaction

Schedule background maintenance without blocking requests:

```typescript
export class CompactingDatabase extends DurableObject<Env> {
  private db: Database | null = null;

  async alarm() {
    const db = await this.getDB();

    // Compact WAL, archive old data to R2
    await db.maintenance({
      compact: true,
      archiveOlderThan: '7d',
    });

    // Schedule next compaction
    await this.ctx.storage.setAlarm(Date.now() + 3600_000); // 1 hour
  }

  async fetch(request: Request): Promise<Response> {
    // Ensure alarm is scheduled
    const alarm = await this.ctx.storage.getAlarm();
    if (!alarm) {
      await this.ctx.storage.setAlarm(Date.now() + 3600_000);
    }

    // Handle request...
  }
}
```

### Cross-DO Queries

Query data across multiple Durable Objects:

```typescript
import { createShardRouter } from '@dotdo/dosql/sharding';

export class CoordinatorDatabase extends DurableObject<Env> {
  async aggregateUsers(): Promise<UserStats> {
    const router = createShardRouter(this.env.USER_SHARDS);

    // Fan-out query to all shards, combine results
    const result = await router.query(`
      SELECT
        COUNT(*) as total_users,
        SUM(CASE WHEN active THEN 1 ELSE 0 END) as active_users
      FROM users
    `);

    return result.rows[0];
  }
}
```

### CDC Streaming to Lakehouse

Capture every change and stream to R2/Iceberg:

```typescript
import { createCDC } from '@dotdo/dosql/cdc';

export class StreamingDatabase extends DurableObject<Env> {
  private cdc = createCDC();

  async insert(table: string, data: Record<string, unknown>) {
    const db = await this.getDB();

    await db.transaction(async (tx) => {
      await tx.run(`INSERT INTO ${table} VALUES (?)`, [data]);

      // CDC event captured in same transaction
      this.cdc.emit({
        op: 'INSERT',
        table,
        data,
        timestamp: Date.now(),
      });
    });
  }

  async alarm() {
    // Flush CDC events to R2 lakehouse
    const events = this.cdc.drain();
    if (events.length > 0) {
      await this.env.R2_BUCKET.put(
        `cdc/${Date.now()}.json`,
        JSON.stringify(events)
      );
    }
  }
}
```

### Tiered Storage (Hot/Cold)

Keep recent data in DO SQLite, archive historical data to R2:

```typescript
import { createTieredBackend } from '@dotdo/dosql/fsx';

export class TieredDatabase extends DurableObject<Env> {
  private async getDB(): Promise<Database> {
    const storage = createTieredBackend({
      hot: this.ctx.storage,           // Fast: DO SQLite
      warm: this.env.KV_CACHE,         // Medium: KV
      cold: this.env.R2_BUCKET,        // Archive: R2 Parquet
      hotThreshold: '24h',
      warmThreshold: '7d',
    });

    return DB('tiered', { storage });
  }
}
```

## Architecture

```
                    +-----------------+
                    |     Client      |
                    +--------+--------+
                             |
                             v
                    +--------+--------+
                    |     Worker      |
                    |  (Routing Only) |
                    +--------+--------+
                             |
           +-----------------+-----------------+
           |                 |                 |
           v                 v                 v
    +------+------+   +------+------+   +------+------+
    | Durable Obj |   | Durable Obj |   | Durable Obj |
    |  (Tenant A) |   |  (Tenant B) |   |  (Tenant C) |
    +------+------+   +------+------+   +------+------+
           |                 |                 |
           +-----------------+-----------------+
                             |
                    +--------+--------+
                    |       R2        |
                    |   (Lakehouse)   |
                    +-----------------+
```

Each Durable Object contains:
- **DoSQL Engine** - Query parsing, optimization, execution
- **SQLite Storage** - Cloudflare's native DO storage
- **WAL Buffer** - Write-ahead log for durability
- **CDC Capture** - Change tracking for streaming

## Connecting from Clients

While DoSQL runs in Durable Objects, clients connect through your Worker:

```typescript
// Client-side (browser, Node.js, etc.)
const response = await fetch('https://my-app.workers.dev/query', {
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify({
    tenant: 'acme-corp',
    sql: 'SELECT * FROM users WHERE active = ?',
    params: [true],
  }),
});

const { rows } = await response.json();
```

For real-time updates, use WebSocket connections:

```typescript
// Worker routes WebSocket to DO
export class RealtimeDatabase extends DurableObject<Env> {
  async fetch(request: Request): Promise<Response> {
    if (request.headers.get('Upgrade') === 'websocket') {
      const [client, server] = Object.values(new WebSocketPair());
      this.ctx.acceptWebSocket(server);
      return new Response(null, { status: 101, webSocket: client });
    }
    // ...
  }

  async webSocketMessage(ws: WebSocket, message: string) {
    const { sql, params } = JSON.parse(message);
    const db = await this.getDB();
    const result = await db.query(sql, params);
    ws.send(JSON.stringify(result));
  }
}
```

## Advanced Features

### Time Travel Queries

Query your data at any point in time:

```typescript
// Current state
const now = await db.query('SELECT * FROM inventory');

// State from 1 hour ago
const hourAgo = await db.query(`
  SELECT * FROM inventory
  FOR SYSTEM_TIME AS OF TIMESTAMP '2026-01-22 10:00:00'
`);

// Diff between two points
const changes = await db.query(`
  SELECT * FROM inventory
  FOR SYSTEM_TIME BETWEEN
    TIMESTAMP '2026-01-22 09:00:00' AND
    TIMESTAMP '2026-01-22 10:00:00'
`);
```

### Database Branching

Create isolated copies for testing or A/B scenarios:

```typescript
// Create a branch
await db.branch('experiment-pricing');
await db.checkout('experiment-pricing');

// Make changes on branch
await db.run('UPDATE products SET price = price * 1.1');

// Test the changes...
const results = await db.query('SELECT * FROM products');

// Merge back or discard
await db.checkout('main');
await db.merge('experiment-pricing'); // or db.deleteBranch()
```

### Virtual Tables

Query external data sources as SQL tables:

```typescript
// Query JSON API
const users = await db.query(`
  SELECT id, name
  FROM 'https://api.example.com/users.json'
  WHERE role = 'admin'
`);

// Query Parquet in R2
const sales = await db.query(`
  SELECT SUM(amount) as total
  FROM 'r2://mybucket/sales/2026/*.parquet'
  WHERE region = 'US'
`);

// Join DO data with external data
const enriched = await db.query(`
  SELECT u.*, o.total_orders
  FROM users u
  JOIN 'r2://analytics/order_counts.parquet' o ON u.id = o.user_id
`);
```

## Migrations

```
.do/
└── migrations/
    ├── 001_create_users.sql
    ├── 002_add_orders.sql
    └── 003_add_indexes.sql
```

Migrations run automatically on first DB access. Each migration runs once and is tracked.

## Bundle Size

DoSQL is designed for Workers' 1MB limit:

| Bundle | Gzipped | % of Free Tier |
|--------|---------|----------------|
| Core | **7.36 KB** | 0.7% |
| With CDC | **12.1 KB** | 1.2% |
| Full Library | **34.26 KB** | 3.3% |

## Module Exports

| Module | Use Case | Stability |
|--------|----------|-----------|
| `@dotdo/dosql` | Core DB operations | Beta |
| `@dotdo/dosql/fsx` | Storage backends | Experimental |
| `@dotdo/dosql/cdc` | Change data capture | Experimental |
| `@dotdo/dosql/sharding` | Cross-DO queries | Experimental |
| `@dotdo/dosql/wal` | Write-ahead log | Beta |
| `@dotdo/dosql/rpc` | DO-to-DO communication | Beta |
| `@dotdo/dosql/errors` | Error types | Stable |

## Requirements

- Cloudflare Workers with Durable Objects
- `compatibility_date` 2024-01-01 or later
- TypeScript 5.3+ (for type safety)
- R2 bucket (for lakehouse features)

## Status

> **Developer Preview** - APIs may change before 1.0

| Phase | Target | Status |
|-------|--------|--------|
| Developer Preview | Current | Active |
| Beta | Q2 2026 | Planned |
| GA | Q3 2026 | Target |

## Documentation

| Document | Description |
|----------|-------------|
| [Getting Started](./docs/getting-started.md) | Full setup guide |
| [API Reference](./docs/api-reference.md) | Complete API docs |
| [Advanced Features](./docs/advanced.md) | Time travel, branching, CDC |
| [Architecture](./docs/architecture.md) | Internals and optimization |

## License

MIT

## Links

- [GitHub](https://github.com/dotdo/sql)
- [npm](https://www.npmjs.com/package/@dotdo/dosql)
- [Cloudflare Durable Objects](https://developers.cloudflare.com/durable-objects/)
