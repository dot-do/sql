# sql.do

**You deserve a database that keeps up with you.**

You have shipped code to the edge. Your Workers run in 300+ cities. Your users get sub-50ms responses worldwide. But every time you query your database, you watch that latency spike. The centralized database becomes the bottleneck. The edge becomes a lie.

**DoSQL changes that.**

```typescript
import { DB } from '@dotdo/dosql'

// Your database. At the edge. Type-safe.
const db = await DB('tenant-acme', {
  migrations: { folder: '.do/migrations' }
})

// Queries that run where your users are
const dashboard = await db.query<Metrics>(`
  SELECT date, SUM(revenue) as total
  FROM orders
  WHERE tenant_id = ?
  GROUP BY date
`, [tenantId])
```

## The Problem You Know Too Well

You have been here before. You chose the edge because latency matters. Because your users deserve fast. But traditional databases force a painful choice:

- **Centralized SQL**: Powerful queries, but every request crawls back to us-east-1
- **Edge KV stores**: Fast reads, but no joins, no transactions, no SQL
- **Distributed SQL**: Complex setup, operational nightmares, unpredictable costs

You are stuck choosing between the power of SQL and the speed of the edge.

## Your Path Forward

DoSQL is a complete SQL database that runs inside Cloudflare Durable Objects. Not a proxy. Not a cache. A real database engine with transactions, migrations, and type safety that deploys with your Worker.

### Step 1: Install

```bash
npm install @dotdo/dosql
```

### Step 2: Define Your Schema

```sql
-- .do/migrations/001_create_tables.sql
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

### Step 3: Ship

```typescript
export class TenantDatabase implements DurableObject {
  private db: Database | null = null

  constructor(private state: DurableObjectState, private env: Env) {}

  async fetch(request: Request): Promise<Response> {
    // Database initializes automatically with migrations
    this.db ??= await DB('tenant', {
      migrations: { folder: '.do/migrations' },
      storage: {
        hot: this.state.storage,
        cold: this.env.R2_BUCKET
      }
    })

    // Full SQL power at the edge
    const users = await this.db.query<User>(
      'SELECT * FROM users WHERE active = ?',
      [true]
    )

    return Response.json(users)
  }
}
```

## What You Get

### Type-Safe SQL

Your queries are validated at compile time. Typos become build errors, not runtime surprises.

```typescript
interface User {
  id: number
  email: string
  name: string
  active: boolean
}

// TypeScript knows this returns User[]
const users = await db.query<User>(
  'SELECT id, email, name, active FROM users WHERE active = ?',
  [true]
)

// IDE autocomplete works
users[0].email  // string
users[0].actve  // Error: Property 'actve' does not exist
```

### Transactions That Actually Work

ACID transactions at the edge. No compromises.

```typescript
await db.transaction(async (tx) => {
  const user = await tx.query<User>(
    'SELECT * FROM users WHERE id = ?',
    [userId]
  )

  await tx.run(
    'INSERT INTO orders (user_id, total) VALUES (?, ?)',
    [userId, total]
  )

  await tx.run(
    'UPDATE users SET order_count = order_count + 1 WHERE id = ?',
    [userId]
  )
})
// All or nothing. Isolation guaranteed.
```

### Time Travel

Made a mistake? Query the past.

```typescript
// Current data
const now = await db.query('SELECT * FROM accounts')

// What did this look like yesterday?
const yesterday = await db.query(`
  SELECT * FROM accounts
  FOR SYSTEM_TIME AS OF TIMESTAMP '2026-01-22 00:00:00'
`)

// Audit trail built in
const history = await db.query(`
  SELECT * FROM accounts
  FOR SYSTEM_TIME BETWEEN '2026-01-01' AND '2026-01-23'
`)
```

### Git-Style Branching

Test schema changes without fear.

```typescript
// Create a branch for your experiment
await db.branch('feature-new-pricing')
await db.checkout('feature-new-pricing')

// Make breaking changes safely
await db.run('ALTER TABLE plans ADD COLUMN tier TEXT')
await db.run('UPDATE plans SET tier = "pro" WHERE price > 100')

// It works? Merge it.
await db.checkout('main')
await db.merge('feature-new-pricing')

// It broke? Just delete the branch.
await db.deleteBranch('feature-new-pricing')
```

### Virtual Tables

Query anything with SQL. APIs, files, the web itself.

```typescript
// Query a JSON API like a table
const githubUsers = await db.query(`
  SELECT login, contributions
  FROM 'https://api.github.com/repos/cloudflare/workers-sdk/contributors'
  WHERE contributions > 100
`)

// Query Parquet files in R2
const analytics = await db.query(`
  SELECT date, SUM(pageviews) as total
  FROM 'r2://analytics/pageviews/*.parquet'
  WHERE date >= '2026-01-01'
  GROUP BY date
`)

// Join local data with remote APIs
const enriched = await db.query(`
  SELECT u.name, o.total, r.rating
  FROM users u
  JOIN orders o ON u.id = o.user_id
  JOIN 'https://reviews-api.example.com/ratings.json' r
    ON u.id = r.user_id
`)
```

### CDC Streaming

Stream changes to your data warehouse in real-time.

```typescript
import { createCDC } from '@dotdo/dosql/cdc'

const cdc = createCDC(db)

for await (const event of cdc.subscribe()) {
  // Every INSERT, UPDATE, DELETE as it happens
  console.log(event.op, event.table, event.data)

  // Stream to your lakehouse
  await lakehouse.append(event)
}
```

## The Numbers

| Metric | DoSQL | Traditional Edge |
|--------|-------|------------------|
| Bundle Size | **7.4 KB** gzipped | N/A |
| Query Latency | **< 1ms** local | 50-200ms roundtrip |
| Cold Start | **< 10ms** | Varies |
| Transactions | **Full ACID** | Eventually consistent |

## What Happens If You Do Nothing

The gap between your edge compute and your centralized database will only grow. Your users will feel it in every slow dashboard load, every laggy form submission, every timeout error. Your competitors who solve this will ship faster features with better UX.

## Start Today

```bash
npm install @dotdo/dosql
```

```typescript
import { DB } from '@dotdo/dosql'

// Five lines to a database at the edge
const db = await DB('my-app')
await db.run('CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)')
await db.run('INSERT INTO items (name) VALUES (?)', ['Hello, Edge'])
const items = await db.query('SELECT * FROM items')
console.log(items) // [{ id: 1, name: 'Hello, Edge' }]
```

Your users deserve fast. Your code deserves type safety. Your operations deserve simplicity.

**DoSQL gives you all three.**

---

## Documentation

| Guide | Description |
|-------|-------------|
| [Getting Started](https://sql.do/docs/getting-started) | Installation and first queries |
| [Migrations](https://sql.do/docs/migrations) | Schema management with SQL files |
| [Transactions](https://sql.do/docs/transactions) | ACID guarantees at the edge |
| [Time Travel](https://sql.do/docs/time-travel) | Query historical data |
| [Branching](https://sql.do/docs/branching) | Git-like database workflows |
| [Virtual Tables](https://sql.do/docs/virtual-tables) | Query APIs and files with SQL |
| [CDC Streaming](https://sql.do/docs/cdc) | Real-time change capture |

## Requirements

- Cloudflare Workers with Durable Objects
- TypeScript 5.3+
- Node.js 20+ (development)

## License

MIT

---

Built for developers who believe the edge should be more than a CDN.

[Get Started](https://sql.do/docs) | [GitHub](https://github.com/dotdo/sql) | [npm](https://www.npmjs.com/package/@dotdo/dosql)
