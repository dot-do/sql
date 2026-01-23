# lake.do

**Real-time Iceberg Lakehouse on Cloudflare.**

---

## Your Data Is Trapped

You built something great. Users are active. Data is flowing. But when your CEO asks "What happened in the last hour?" you have to say: "Check back tomorrow."

The gap between your application and your analytics is killing you:

- Your database chokes on analytical queries
- ETL pipelines mean your dashboards are always 24 hours stale
- Setting up Kafka, Spark, and Airflow feels like building a second company
- Every "quick" data request turns into a week-long project

**You need real-time analytics. Not another infrastructure team.**

---

## lake.do: Your Lakehouse, Zero Ops

lake.do runs where your application already lives: Cloudflare's global edge. No clusters. No provisioning. No ETL pipelines to babysit.

Every change in DoSQL flows to lake.do in real-time. Batched intelligently. Written as Parquet to R2. Wrapped in proper Iceberg metadata. Within seconds, your data is queryable by DuckDB, Spark, Trino, or our built-in engine.

```typescript
import { createLakeClient } from '@dotdo/lake.do'

const lake = createLakeClient({
  url: 'https://lake.example.com',
  token: process.env.LAKE_TOKEN,
})

// Analytical queries on real-time data
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

// Data from seconds ago, not hours
console.log(`Last hour revenue: $${revenue.rows[0].revenue}`)
```

---

## Three Steps to Real-Time Analytics

### 1. Stream Your Changes

CDC events flow from DoSQL automatically. Every INSERT, UPDATE, DELETE. WebSocket connections hibernate when idle (95% cost savings).

```typescript
// CDC flows automatically from DoSQL. Or subscribe manually:
for await (const batch of lake.subscribe({
  tables: ['orders', 'customers'],
  operations: ['INSERT', 'UPDATE'],
})) {
  console.log(`${batch.events.length} changes in real-time`)
}
```

### 2. Query Across Time

Full history on every table. Query now. Query last Tuesday. Query any snapshot.

```typescript
// What did inventory look like last week?
const historical = await lake.query(
  'SELECT * FROM inventory WHERE product_id = ?',
  { params: ['SKU-123'], asOf: new Date('2025-01-14T00:00:00Z') }
)

// Compare to now
const current = await lake.query(
  'SELECT * FROM inventory WHERE product_id = ?',
  { params: ['SKU-123'] }
)

console.log('Then:', historical.rows[0])
console.log('Now:', current.rows[0])
```

### 3. Connect Your Tools

Standard Iceberg. Your existing stack just works.

**DuckDB** (local analysis):
```sql
SELECT * FROM iceberg_scan('r2://my-lakehouse/warehouse/default/orders');
```

**Spark** (large-scale processing):
```python
spark.table("dolake.default.orders").groupBy("region").sum("revenue")
```

**Trino** (federated queries):
```sql
SELECT * FROM dolake.default.orders WHERE created_at > DATE '2025-01-01';
```

---

## What You're Avoiding

Without lake.do, you're stuck with:

- **Stale data**: Analysts always looking at yesterday
- **Infrastructure sprawl**: Kafka + Spark + S3 + Hive + Airflow = a team just for plumbing
- **Audit gaps**: No time travel means no audit trail, no point-in-time recovery
- **Cost overruns**: Paying for peak capacity you use 10% of the time

| Traditional Lakehouse | lake.do |
|----------------------|---------|
| Hours of latency | Seconds |
| Dedicated infra team | Zero ops |
| Fixed capacity costs | Pay-per-request |
| Complex CDC pipelines | Automatic |
| Vendor lock-in | Standard Iceberg |

---

## What Success Looks Like

```typescript
import { createLakeClient } from '@dotdo/lake.do'

const lake = createLakeClient({
  url: 'https://lake.example.com',
  token: process.env.LAKE_TOKEN,
})

// Real-time dashboard in 10 lines
async function getDashboardMetrics() {
  const [revenue, users, inventory] = await Promise.all([
    lake.query(`SELECT SUM(amount) as total FROM orders WHERE created_at >= NOW() - INTERVAL '1 hour'`),
    lake.query(`SELECT COUNT(DISTINCT user_id) as active FROM events WHERE date = CURRENT_DATE`),
    lake.query(`SELECT product_name, quantity FROM inventory WHERE quantity < reorder_point`),
  ])

  return {
    hourlyRevenue: revenue.rows[0].total,
    activeUsers: users.rows[0].active,
    lowStockItems: inventory.rows,
    dataAge: 'seconds',  // not hours
  }
}

// React to changes in real-time
async function syncDownstream() {
  for await (const batch of lake.subscribe({ tables: ['orders'] })) {
    for (const event of batch.events) {
      if (event.operation === 'INSERT') {
        await notifyWarehouse(event.after)
        await updateSearchIndex(event.after)
        await refreshCache(event.after.id)
      }
    }
  }
}

// Debug with time travel
async function investigateIssue(orderId: string, reportedAt: Date) {
  const snapshot = await lake.query(
    'SELECT * FROM orders WHERE id = ?',
    { params: [orderId], asOf: reportedAt }
  )
  console.log('State when bug was reported:', snapshot.rows[0])
}
```

---

## Get Started in 60 Seconds

```bash
npm install @dotdo/lake.do
```

```typescript
import { createLakeClient } from '@dotdo/lake.do'

const lake = createLakeClient({
  url: 'https://your-lakehouse.workers.dev',
  token: process.env.LAKE_TOKEN,
})

// Query
const result = await lake.query('SELECT COUNT(*) FROM orders')
console.log(`Total orders: ${result.rows[0].count}`)

// Subscribe to changes
for await (const batch of lake.subscribe({ tables: ['orders'] })) {
  console.log(`${batch.events.length} changes`)
}
```

That's it. You're running a lakehouse.

---

## How It Works

```
Your App --> DoSQL --> lake.do --> R2 (Iceberg/Parquet)
                                       |
                         +-------------+-------------+
                         |             |             |
                      DuckDB        Spark         Trino
```

**DoSQL**: OLTP. Every transaction emits CDC events.

**lake.do**: Batches events. Writes Parquet. Manages Iceberg metadata.

**R2**: Standard Iceberg tables. Query with any tool.

**Result**: Real-time data, full history, edge latency.

---

## Features

- **Real-time CDC**: Seconds, not hours
- **Iceberg Native**: No vendor lock-in
- **Time Travel**: Query any point in history
- **Partition Pruning**: Fast on large datasets
- **Auto Compaction**: Small files merged automatically
- **REST Catalog**: Standard API for external engines
- **WebSocket Hibernation**: 95% cost savings when idle
- **Edge Latency**: Data where your users are

---

## Documentation

- [Getting Started](https://lake.do/docs/getting-started)
- [CDC Streaming](https://lake.do/docs/cdc-streaming)
- [Time Travel](https://lake.do/docs/time-travel)
- [External Tools](https://lake.do/docs/external-tools)
- [API Reference](https://lake.do/docs/api)

---

## Stop Waiting. Start Querying.

Your data is flowing. Your analysts are waiting. The infrastructure excuse is over.

```bash
npm install @dotdo/lake.do
```

**[Get Started Now](https://lake.do/docs/getting-started)** | **[Examples](https://github.com/dotdo/lake.do/tree/main/examples)** | **[Discord](https://discord.gg/dotdo)**

---

*lake.do: Real-time analytics. Zero ops. Standard Iceberg.*
