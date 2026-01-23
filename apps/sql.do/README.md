# sql.do

**Your edge is only as fast as your slowest query.**

You deployed to Cloudflare. Your Workers run in 300+ cities. But every database call crawls back to `us-east-1`, and your 5ms edge becomes a 200ms round trip.

**DoSQL runs SQL where your code runs.**

```typescript
import { DB } from '@dotdo/dosql'

const db = await DB('acme', { migrations: '.do/migrations' })

const dashboard = await db.query<Metric>(`
  SELECT date, SUM(revenue) as total
  FROM orders WHERE tenant_id = ?
  GROUP BY date
`, [tenantId])
```

No proxy. No cache layer. A real database at the edge.

## The Problem

Edge developers face an impossible choice:

| Option | Tradeoff |
|--------|----------|
| **Centralized Postgres** | Full SQL, but 200ms latency kills UX |
| **Edge KV** | Fast, but no joins, no transactions, no queries |
| **Distributed SQL** | Complex ops, unpredictable bills, months to configure |

You should not have to choose between SQL and speed.

## The Solution

DoSQL embeds a complete SQL database inside Cloudflare Durable Objects. ACID transactions. Automatic migrations. Full type safety. Deploys with your Worker.

### Install

```bash
npm install @dotdo/dosql
```

### Define Schema

```sql
-- .do/migrations/001_init.sql
CREATE TABLE users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER REFERENCES users(id),
  total DECIMAL(10,2) NOT NULL,
  status TEXT DEFAULT 'pending'
);
```

### Deploy

```typescript
export class TenantDB implements DurableObject {
  private db: Database | null = null

  constructor(private state: DurableObjectState, private env: Env) {}

  async fetch(request: Request): Promise<Response> {
    this.db ??= await DB('tenant', {
      migrations: '.do/migrations',
      storage: { hot: this.state.storage, cold: this.env.R2 }
    })

    const users = await this.db.query<User>(
      'SELECT * FROM users WHERE active = ?', [true]
    )

    return Response.json(users)
  }
}
```

## Features

### Type-Safe Queries

Typos become compile errors, not production incidents.

```typescript
interface User {
  id: number
  email: string
  name: string
}

const users = await db.query<User>(
  'SELECT id, email, name FROM users WHERE active = ?', [true]
)

users[0].email  // string
users[0].emial  // TypeScript error
```

### ACID Transactions

Real transactions at the edge. No eventual consistency.

```typescript
await db.transaction(async (tx) => {
  await tx.run(
    'INSERT INTO orders (user_id, total) VALUES (?, ?)',
    [userId, amount]
  )
  await tx.run(
    'UPDATE users SET order_count = order_count + 1 WHERE id = ?',
    [userId]
  )
})
```

### Time Travel

Query any point in history. Built-in audit trail.

```typescript
// What did this table look like yesterday?
const yesterday = await db.query(`
  SELECT * FROM accounts
  FOR SYSTEM_TIME AS OF '2026-01-22T00:00:00Z'
`)

// Full history between two timestamps
const changes = await db.query(`
  SELECT * FROM accounts
  FOR SYSTEM_TIME BETWEEN '2026-01-01' AND '2026-01-23'
`)
```

### Database Branching

Test migrations without touching production.

```typescript
await db.branch('feature-pricing')
await db.checkout('feature-pricing')

await db.run('ALTER TABLE plans ADD COLUMN tier TEXT')
await db.run('UPDATE plans SET tier = "pro" WHERE price > 100')

// Validated? Merge it.
await db.checkout('main')
await db.merge('feature-pricing')
```

### Query Anything

APIs, Parquet files, remote endpoints. SQL is your interface.

```typescript
// Query a JSON API
const contributors = await db.query(`
  SELECT login, contributions
  FROM 'https://api.github.com/repos/cloudflare/workers-sdk/contributors'
  WHERE contributions > 100
`)

// Query Parquet in R2
const analytics = await db.query(`
  SELECT date, SUM(pageviews) as total
  FROM 'r2://analytics/*.parquet'
  WHERE date >= '2026-01-01'
  GROUP BY date
`)

// Join local tables with remote data
const enriched = await db.query(`
  SELECT u.name, o.total, r.rating
  FROM users u
  JOIN orders o ON u.id = o.user_id
  JOIN 'https://api.example.com/ratings.json' r ON u.id = r.user_id
`)
```

### Change Data Capture

Stream every mutation to your lakehouse.

```typescript
import { CDC } from '@dotdo/dosql/cdc'

for await (const change of CDC(db).subscribe()) {
  await lakehouse.append(change)
}
```

## Performance

| Metric | DoSQL | Centralized DB |
|--------|-------|----------------|
| Bundle | **7.4 KB** gzip | N/A |
| Query Latency | **< 1ms** | 50-200ms |
| Cold Start | **< 10ms** | N/A |
| Transactions | **ACID** | ACID |

## Start Now

```bash
npm install @dotdo/dosql
```

```typescript
import { DB } from '@dotdo/dosql'

const db = await DB('app')
await db.run('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
await db.run('INSERT INTO items (name) VALUES (?)', ['Edge SQL'])

const items = await db.query('SELECT * FROM items')
// [{ id: 1, name: 'Edge SQL' }]
```

Your users get speed. Your team gets type safety. You get simplicity.

---

## Documentation

| Guide | Description |
|-------|-------------|
| [Getting Started](https://sql.do/docs) | Install and run your first query |
| [Migrations](https://sql.do/docs/migrations) | Manage schema with SQL files |
| [Transactions](https://sql.do/docs/transactions) | ACID guarantees |
| [Time Travel](https://sql.do/docs/time-travel) | Query historical state |
| [Branching](https://sql.do/docs/branching) | Git-like workflows |
| [Virtual Tables](https://sql.do/docs/virtual-tables) | Query APIs with SQL |
| [CDC](https://sql.do/docs/cdc) | Stream changes |

## Requirements

- Cloudflare Workers with Durable Objects
- TypeScript 5.3+
- Node.js 20+ (development)

## License

MIT

---

[Get Started](https://sql.do/docs) | [GitHub](https://github.com/dotdo/sql) | [npm](https://www.npmjs.com/package/@dotdo/dosql)
