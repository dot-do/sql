> ![Developer Preview](https://img.shields.io/badge/Status-Developer%20Preview-orange)
>
> **Developer Preview** - This package is under active development. APIs may change before the stable 1.0 release. Not recommended for production use without thorough testing.

# DoSQL

[![npm version](https://img.shields.io/npm/v/@dotdo/dosql.svg)](https://www.npmjs.com/package/@dotdo/dosql)
[![Bundle Size](https://img.shields.io/badge/bundle-7.4KB_gzip-brightgreen)](./docs/architecture.md#bundle-size)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.3+-blue.svg)](https://www.typescriptlang.org/)
[![Cloudflare Workers](https://img.shields.io/badge/Cloudflare-Workers-orange.svg)](https://workers.cloudflare.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> Type-safe SQL for TypeScript with Cloudflare Durable Objects

DoSQL is a database engine purpose-built for Cloudflare Workers and Durable Objects. It provides type-safe SQL queries with compile-time validation, automatic schema migrations, and advanced features like time travel, branching, and CDC streaming.

## Status

| Property | Value |
|----------|-------|
| Current version | 0.1.0 |
| Stability | Developer Preview |
| Breaking changes | Expected before 1.0 |
| Expected GA | Q3 2026 (Target) |

### Graduation Timeline

| Phase | Target | Description |
|-------|--------|-------------|
| Developer Preview | Current | Core APIs stabilizing, gathering feedback |
| Beta | Q2 2026 | API freeze for core features, focus on stability |
| Release Candidate | Q3 2026 | Production-ready, final testing |
| General Availability (1.0) | Q3 2026 | Stable release with LTS support |

**Note:** Timeline is subject to change based on community feedback and testing results.

## Stability

### Stable APIs

- Core database operations (`DB`, `query`, `run`, `transaction`)
- Basic SQL execution (SELECT, INSERT, UPDATE, DELETE)
- Schema migrations (`.do/migrations/*.sql` convention)
- Transaction management
- Error handling

### Experimental APIs

- Time travel queries (`FOR SYSTEM_TIME AS OF`)
- Database branching (`branch`, `checkout`, `merge`)
- CDC streaming to lakehouse
- Virtual tables (URL, R2, external APIs)
- CapnWeb RPC protocol
- Sharding and query routing
- Stored procedures

## Version Compatibility

| Dependency | Minimum | Tested |
|------------|---------|--------|
| Node.js | 20.0.0 | 22.21.1 |
| TypeScript | 5.3.0 | 5.9.3 |
| Cloudflare Workers (compatibility_date) | 2024-01-01 | 2026-01-01 |
| @cloudflare/workers-types | 4.0.0 | 4.20260122.0 |
| Wrangler | 4.0.0 | 4.59.3 |
| workerd | 1.20240101.0 | 1.20260116.0 |

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
interface User {
  id: number;
  name: string;
  active: boolean;
  order_count: number;
}

const users = await db.query<User>('SELECT * FROM users WHERE active = ?', [true]);

// Transactions
await db.transaction(async (tx: TransactionContext) => {
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
  FOR SYSTEM_TIME AS OF TIMESTAMP '2026-01-22 10:00:00'
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
import { createCDC, type CDCEvent } from '@dotdo/dosql/cdc';

const cdc = createCDC(backend);

for await (const event: CDCEvent of cdc.subscribe(0n)) {
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

## HTTP Endpoints

DoSQL exposes HTTP endpoints for health checks and database operations.

### Worker-Level Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Health check (alias for `/health`) |
| GET | `/health` | Health check |
| GET | `/api` | API documentation |
| ALL | `/db/:name/*` | Route to database Durable Object |

#### /health Endpoint

Returns the health status of the DoSQL worker.

**Request:**
```bash
curl https://your-worker.workers.dev/health
```

**Response:**
```json
{
  "status": "ok",
  "service": "dosql",
  "version": "0.1.0"
}
```

### Database-Level Endpoints

These endpoints are available within each database Durable Object (accessed via `/db/:name/...`).

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Database health check |
| GET | `/tables` | List all tables |
| POST | `/query` | Execute SELECT query |
| POST | `/execute` | Execute INSERT/UPDATE/DELETE |

#### /db/:name/health Endpoint

Returns the health status of a specific database instance.

**Request:**
```bash
curl https://your-worker.workers.dev/db/mydb/health
```

**Response:**
```json
{
  "status": "ok",
  "initialized": true
}
```

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | Always `"ok"` when healthy |
| `initialized` | boolean | Whether the database has been initialized |

#### /db/:name/tables Endpoint

Lists all tables in the database.

**Request:**
```bash
curl https://your-worker.workers.dev/db/mydb/tables
```

**Response:**
```json
{
  "tables": ["users", "posts", "comments"]
}
```

#### /db/:name/query Endpoint

Executes a SELECT query.

**Request:**
```bash
curl -X POST https://your-worker.workers.dev/db/mydb/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM users WHERE active = ?", "params": [true]}'
```

**Response:**
```json
{
  "success": true,
  "rows": [
    {"id": 1, "name": "Alice", "active": true},
    {"id": 2, "name": "Bob", "active": true}
  ],
  "stats": {
    "rowsAffected": 2,
    "executionTimeMs": 1.5
  }
}
```

#### /db/:name/execute Endpoint

Executes INSERT, UPDATE, or DELETE statements.

**Request:**
```bash
curl -X POST https://your-worker.workers.dev/db/mydb/execute \
  -H "Content-Type: application/json" \
  -d '{"sql": "INSERT INTO users (name, email) VALUES (?, ?)", "params": ["Charlie", "charlie@example.com"]}'
```

**Response:**
```json
{
  "success": true,
  "stats": {
    "rowsAffected": 1,
    "executionTimeMs": 2.3
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

### Stability Legend

- :green_circle: **Stable** - API is stable and unlikely to change
- :yellow_circle: **Beta** - API is mostly stable but may have minor changes
- :red_circle: **Experimental** - API may change significantly

| Module | Exports | Stability |
|--------|---------|-----------|
| `@dotdo/dosql` | `DB`, `createDatabase` | :yellow_circle: Beta |
| `@dotdo/dosql/transaction` | `TransactionManager` | :yellow_circle: Beta |
| `@dotdo/dosql/wal` | `createWALWriter`, `createWALReader` | :yellow_circle: Beta |
| `@dotdo/dosql/cdc` | `createCDC`, `createCDCSubscription` | :red_circle: Experimental |
| `@dotdo/dosql/rpc` | `createWebSocketClient`, `DoSQLTarget` | :yellow_circle: Beta |
| `@dotdo/dosql/errors` | `DoSQLError`, `DatabaseError`, `SQLSyntaxError` | :green_circle: Stable |
| `@dotdo/dosql/fsx` | `createDOBackend`, `createR2Backend`, `createTieredBackend` | :red_circle: Experimental |
| `@dotdo/dosql/sharding` | `createShardRouter`, `createShardExecutor` | :red_circle: Experimental |
| `@dotdo/dosql/proc` | `procedure`, `createProcedureRegistry` | :red_circle: Experimental |
| `@dotdo/dosql/virtual` | `createVirtualTableRegistry` | :red_circle: Experimental |
| `@dotdo/dosql/columnar` | `ColumnarWriter`, `ColumnarReader` | :red_circle: Experimental |
| `@dotdo/dosql/orm/drizzle` | `drizzleAdapter` | :yellow_circle: Beta |
| `@dotdo/dosql/orm/kysely` | `kyselyAdapter` | :yellow_circle: Beta |
| `@dotdo/dosql/orm/knex` | `knexAdapter` | :yellow_circle: Beta |
| `@dotdo/dosql/orm/prisma` | `prismaAdapter` | :yellow_circle: Beta |

### Import Examples

```typescript
// Core - Beta
import { DB, createDatabase } from '@dotdo/dosql';

// Transactions - Beta
import { TransactionManager } from '@dotdo/dosql/transaction';

// WAL - Beta
import { createWALWriter, createWALReader } from '@dotdo/dosql/wal';

// CDC - Experimental
import { createCDC, createCDCSubscription } from '@dotdo/dosql/cdc';

// RPC - Beta
import { createWebSocketClient, DoSQLTarget } from '@dotdo/dosql/rpc';

// Errors - Stable
import { DoSQLError, DatabaseError, SQLSyntaxError } from '@dotdo/dosql/errors';

// FSX (Storage) - Experimental
import { createDOBackend, createR2Backend, createTieredBackend } from '@dotdo/dosql/fsx';

// Sharding - Experimental
import { createShardRouter, createShardExecutor } from '@dotdo/dosql/sharding';

// Procedures - Experimental
import { procedure, createProcedureRegistry } from '@dotdo/dosql/proc';

// Virtual Tables - Experimental
import { createVirtualTableRegistry } from '@dotdo/dosql/virtual';

// Columnar (OLAP storage) - Experimental
import { ColumnarWriter, ColumnarReader } from '@dotdo/dosql/columnar';

// ORM Adapters - Beta
import { drizzleAdapter } from '@dotdo/dosql/orm/drizzle';
import { kyselyAdapter } from '@dotdo/dosql/orm/kysely';
import { knexAdapter } from '@dotdo/dosql/orm/knex';
import { prismaAdapter } from '@dotdo/dosql/orm/prisma';
```

## Troubleshooting

### Connection Errors

**Symptoms**: `ConnectionError` with code `CONNECTION_FAILED` or `CONNECTION_CLOSED`

| Error | Cause | Solution |
|-------|-------|----------|
| `Connection failed` | Network issue or invalid endpoint | Verify your Durable Object binding is correct in `wrangler.toml` |
| `WebSocket connection closed` | Connection dropped unexpectedly | Enable automatic reconnection with retry logic |
| `Connection timeout` | Server not responding | Increase timeout in configuration or check DO health |

```typescript
// Retry pattern for connection errors
import { ConnectionError } from '@dotdo/dosql/errors';

async function withRetry<T>(fn: () => Promise<T>, maxRetries = 3): Promise<T> {
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await fn();
    } catch (error) {
      if (error instanceof ConnectionError && i < maxRetries - 1) {
        await new Promise(r => setTimeout(r, 100 * Math.pow(2, i)));
        continue;
      }
      throw error;
    }
  }
  throw new Error('Max retries exceeded');
}
```

### Transaction Errors

**Symptoms**: Transaction operations fail or produce unexpected results

| Error Code | Cause | Solution |
|------------|-------|----------|
| `DEADLOCK_DETECTED` | Two transactions waiting on each other | Retry the transaction; use consistent lock ordering |
| `SERIALIZATION_FAILURE` | Concurrent modification conflict | Retry with exponential backoff |
| `TRANSACTION_TIMEOUT` | Transaction exceeded time limit | Break into smaller transactions or increase timeout |
| `TRANSACTION_ABORTED` | Transaction was rolled back | Check for constraint violations; retry if transient |

```typescript
// Handle transaction conflicts with retry
async function safeTransaction<T>(
  db: Database,
  fn: (tx: TransactionContext) => Promise<T>
): Promise<T> {
  const maxRetries = 3;
  for (let i = 0; i < maxRetries; i++) {
    try {
      return await db.transaction(fn);
    } catch (error) {
      if (error instanceof DoSQLError) {
        const retryable = ['DEADLOCK_DETECTED', 'SERIALIZATION_FAILURE'];
        if (retryable.includes(error.code) && i < maxRetries - 1) {
          await new Promise(r => setTimeout(r, 50 * Math.pow(2, i)));
          continue;
        }
      }
      throw error;
    }
  }
  throw new Error('Transaction failed after retries');
}
```

### SQL Syntax Errors

**Symptoms**: `SQLSyntaxError` or `UnexpectedTokenError`

| Error | Cause | Solution |
|-------|-------|----------|
| `Unexpected token` | Invalid SQL syntax | Check SQL statement for typos; use parameterized queries |
| `Missing keyword` | Incomplete SQL statement | Ensure all required clauses are present (e.g., `FROM` in `SELECT`) |
| `Invalid identifier` | Reserved word used as identifier | Quote identifiers with double quotes: `"table"` |

```typescript
// Common mistake: string interpolation (SQL injection risk!)
// BAD:
const bad = db.query(`SELECT * FROM users WHERE name = '${name}'`);

// GOOD: Use parameterized queries
const good = db.query('SELECT * FROM users WHERE name = ?', [name]);
```

### Common Configuration Mistakes

| Issue | Symptom | Fix |
|-------|---------|-----|
| Missing R2 binding | `R2_BUCKET is undefined` | Add `r2_buckets` to `wrangler.toml` |
| Wrong migrations path | Tables not created | Ensure `.do/migrations/*.sql` exists and path is correct |
| Missing DO binding | `DOSQL is undefined` | Add durable object binding to `wrangler.toml` |
| TypeScript version | Type errors | Upgrade to TypeScript 5.3+ |

```toml
# Example wrangler.toml configuration
[durable_objects]
bindings = [
  { name = "DOSQL", class_name = "TenantDatabase" }
]

[[r2_buckets]]
binding = "R2_BUCKET"
bucket_name = "my-bucket"
```

### Performance Issues

| Symptom | Cause | Solution |
|---------|-------|----------|
| Slow queries | Missing indexes | Add indexes on frequently queried columns |
| High memory usage | Large result sets | Use pagination with `LIMIT` and `OFFSET` |
| Transaction timeouts | Long-running operations | Break into smaller batches |

```typescript
// Paginated query pattern
async function* paginate<T>(db: Database, sql: string, pageSize = 1000) {
  let offset = 0;
  while (true) {
    const result = await db.query<T>(`${sql} LIMIT ? OFFSET ?`, [pageSize, offset]);
    if (result.rows.length === 0) break;
    yield result.rows;
    offset += pageSize;
    if (result.rows.length < pageSize) break;
  }
}
```

## Requirements

- Cloudflare Workers / Durable Objects (compatibility_date 2024-01-01+)
- TypeScript 5.3+
- Node.js 20+ (for development)

## License

MIT

## Changelog

See [CHANGELOG.md](./CHANGELOG.md) for a detailed history of changes.

## Links

- [GitHub](https://github.com/dotdo/sql)
- [npm](https://www.npmjs.com/package/@dotdo/dosql)
- [Documentation](./docs/README.md)
