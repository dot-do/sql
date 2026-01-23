# sql.do

**Your database is the slowest part of your stack.**

You shipped to the edge. Your Workers run in 300+ cities. Your users expect sub-50ms responses. But every database query crawls back to a single region, and your edge advantage vanishes.

**DoSQL puts SQL where your code runs.**

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

## The Tradeoff That Should Not Exist

Every edge developer faces the same impossible choice:

- **Centralized SQL**: Full power, but 200ms+ roundtrips kill your UX
- **Edge KV**: Fast reads, but no joins, no transactions, no queries
- **Distributed SQL**: Complex ops, unpredictable costs, months to set up

You should not have to choose between SQL and speed.

## SQL at the Edge. For Real.

DoSQL is a complete SQL database inside Cloudflare Durable Objects. Not a proxy. Not a cache. A real database engine with ACID transactions, migrations, and type safety that deploys with your Worker.

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

### Type-Safe Queries

Typos become build errors, not 3am pages.

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

### Real Transactions

ACID at the edge. No eventual consistency. No compromises.

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
// All or nothing. Always.
```

### Time Travel

Deleted the wrong rows? Query the past.

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

### Database Branching

Test schema changes without touching production.

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

### Query Anything

APIs, Parquet files, the web. If it has data, SQL can query it.

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

### Change Data Capture

Stream every INSERT, UPDATE, DELETE to your lakehouse.

```typescript
import { createCDC } from '@dotdo/dosql/cdc'

const cdc = createCDC(db)

for await (const change of cdc.subscribe()) {
  console.log(change.op, change.table, change.data)
  await lakehouse.append(change)
}
```

## Performance

| Metric | DoSQL | Centralized DB |
|--------|-------|----------------|
| Bundle Size | **7.4 KB** gzipped | N/A |
| Query Latency | **< 1ms** | 50-200ms |
| Cold Start | **< 10ms** | N/A |
| Transactions | **Full ACID** | Full ACID |

## The Cost of Waiting

Every day you run centralized queries from the edge, your users feel it. Slow dashboards. Laggy forms. Timeout errors. Your competitors who solve this first will ship faster and win.

## Get Started in 60 Seconds

```bash
npm install @dotdo/dosql
```

```typescript
import { DB } from '@dotdo/dosql'

const db = await DB('my-app')
await db.run('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)')
await db.run('INSERT INTO items (name) VALUES (?)', ['Hello, Edge'])
const items = await db.query('SELECT * FROM items')
// [{ id: 1, name: 'Hello, Edge' }]
```

Fast for your users. Type-safe for your team. Simple to operate.

**Stop choosing. Start shipping.**

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

**Built for developers who refuse to compromise.**

[Get Started](https://sql.do/docs) | [View on GitHub](https://github.com/dotdo/sql) | [npm](https://www.npmjs.com/package/@dotdo/dosql)
