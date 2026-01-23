# lake.do

**Real-time Iceberg lakehouse on Cloudflare.**

---

## Your Analytics Are Always Late

Your CEO asks: "What happened in the last hour?"

You say: "I'll have that for you tomorrow."

The gap between your application and your analytics is costing you:

- Database queries timeout when analysts run them
- Dashboards are 24 hours stale by the time anyone sees them
- Setting up Kafka, Spark, and Airflow requires a dedicated team
- Every "quick" data request becomes a multi-day project

You need real-time analytics. Not another infrastructure team.

---

## lake.do Closes the Gap

lake.do runs where your application already lives: Cloudflare's edge. No clusters to manage. No pipelines to monitor. No vendors to negotiate with.

Every change flows from DoSQL to lake.do in real-time. Written as Parquet to R2. Wrapped in proper Iceberg metadata. Query with DuckDB, Spark, Trino, or the built-in engine.

```typescript
import { Lake } from 'lake.do'

const lake = new Lake({
  endpoint: 'https://lake.example.com',
  token: process.env.LAKE_TOKEN,
})

// Query data from seconds ago
const revenue = await lake.query(`
  SELECT
    date_trunc('hour', created_at) as hour,
    SUM(amount) as revenue,
    COUNT(*) as orders
  FROM orders
  WHERE created_at >= NOW() - INTERVAL '24 hours'
  GROUP BY 1
  ORDER BY 1
`)

console.log(`Last hour: $${revenue.rows[0].revenue}`)
```

---

## Three Steps to Real-Time Analytics

### 1. Stream Changes Automatically

CDC events flow from DoSQL without configuration. Every INSERT, UPDATE, DELETE. WebSocket connections hibernate when idle.

```typescript
// Subscribe to real-time changes
for await (const batch of lake.subscribe(['orders', 'customers'])) {
  console.log(`${batch.events.length} changes`)
}
```

### 2. Query Any Point in Time

Full history on every table. Query now. Query last Tuesday. Query the exact moment a bug was reported.

```typescript
// What did inventory look like last week?
const then = await lake.query(
  'SELECT * FROM inventory WHERE product_id = ?',
  ['SKU-123'],
  { asOf: '2025-01-14T00:00:00Z' }
)

const now = await lake.query(
  'SELECT * FROM inventory WHERE product_id = ?',
  ['SKU-123']
)

console.log('Then:', then.rows[0].quantity)
console.log('Now:', now.rows[0].quantity)
```

### 3. Use Your Existing Tools

Standard Iceberg format. Your stack already supports it.

```sql
-- DuckDB
SELECT * FROM iceberg_scan('r2://lakehouse/orders');

-- Spark
spark.table("lake.orders").groupBy("region").sum("revenue")

-- Trino
SELECT * FROM lake.orders WHERE created_at > DATE '2025-01-01';
```

---

## What You Avoid

| Traditional Lakehouse | lake.do |
|-----------------------|---------|
| Hours of latency | Seconds |
| Dedicated infrastructure team | Zero ops |
| Fixed capacity billing | Pay per query |
| Complex CDC pipelines | Automatic |
| Vendor lock-in | Standard Iceberg |

---

## Real Code for Real Problems

```typescript
import { Lake } from 'lake.do'

const lake = new Lake({
  endpoint: 'https://lake.example.com',
  token: process.env.LAKE_TOKEN,
})

// Real-time dashboard
async function getDashboard() {
  const [revenue, users, alerts] = await Promise.all([
    lake.query(`
      SELECT SUM(amount) as total
      FROM orders
      WHERE created_at >= NOW() - INTERVAL '1 hour'
    `),
    lake.query(`
      SELECT COUNT(DISTINCT user_id) as active
      FROM events
      WHERE timestamp >= NOW() - INTERVAL '1 hour'
    `),
    lake.query(`
      SELECT product_name, quantity
      FROM inventory
      WHERE quantity < reorder_point
    `),
  ])

  return {
    hourlyRevenue: revenue.rows[0].total,
    activeUsers: users.rows[0].active,
    lowStock: alerts.rows,
  }
}

// React to changes as they happen
async function syncDownstream() {
  for await (const batch of lake.subscribe(['orders'])) {
    for (const event of batch.events) {
      if (event.op === 'INSERT') {
        await notifyWarehouse(event.data)
        await updateSearchIndex(event.data)
      }
    }
  }
}

// Debug production issues with time travel
async function investigate(orderId: string, reportedAt: Date) {
  const snapshot = await lake.query(
    'SELECT * FROM orders WHERE id = ?',
    [orderId],
    { asOf: reportedAt }
  )
  console.log('State at time of report:', snapshot.rows[0])
}
```

---

## Start in 60 Seconds

```bash
npm install lake.do
```

```typescript
import { Lake } from 'lake.do'

const lake = new Lake({
  endpoint: 'https://your-lakehouse.workers.dev',
  token: process.env.LAKE_TOKEN,
})

const result = await lake.query('SELECT COUNT(*) as total FROM orders')
console.log(`Orders: ${result.rows[0].total}`)
```

You now have a lakehouse.

---

## Architecture

```
DoSQL (OLTP) --> lake.do --> R2 (Iceberg/Parquet)
                                    |
                      +-------------+-------------+
                      |             |             |
                   DuckDB        Spark         Trino
```

DoSQL handles transactions and emits CDC events. lake.do batches events, writes Parquet files, and manages Iceberg metadata. R2 stores standard Iceberg tables that any tool can read.

---

## Capabilities

- **Real-time CDC** - Seconds, not hours
- **Time Travel** - Query any historical snapshot
- **Standard Iceberg** - No vendor lock-in
- **Partition Pruning** - Fast scans on large tables
- **Auto Compaction** - Small files merged automatically
- **REST Catalog** - Standard API for external engines
- **WebSocket Hibernation** - Pay only when data flows
- **Edge Latency** - Compute where your users are

---

## Documentation

- [Getting Started](https://lake.do/docs)
- [CDC Streaming](https://lake.do/docs/cdc)
- [Time Travel](https://lake.do/docs/time-travel)
- [External Tools](https://lake.do/docs/tools)
- [API Reference](https://lake.do/docs/api)

---

## Stop Waiting for Yesterday's Data

Your data is flowing. Your analysts are waiting.

```bash
npm install lake.do
```

[Get Started](https://lake.do/docs) | [Examples](https://github.com/dotdo/lake.do/examples) | [Discord](https://discord.gg/dotdo)

---

*Real-time analytics. Zero ops. Standard Iceberg.*
