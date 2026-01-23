# lake.do

**Real-time Iceberg Lakehouse on Cloudflare.**

---

## Your Data Deserves Better

You've built something great. Your application is growing, your users are active, and data is flowing. But now you're facing the analytics challenge:

- Your transactional database wasn't built for analytical queries
- Syncing data to a data warehouse means hours of latency
- Setting up Kafka, Spark, and a proper lakehouse feels like building a second company
- Your stakeholders want real-time dashboards, not yesterday's numbers

**You shouldn't have to choose between operational simplicity and analytical power.**

---

## Meet DoLake

DoLake is the lakehouse that runs where your application already lives: on Cloudflare's global edge. No clusters to manage. No infrastructure to provision. No ETL pipelines to babysit.

Your CDC events flow from DoSQL to DoLake in real-time. They're batched intelligently, written as Parquet files to R2, and wrapped in proper Iceberg metadata. Within seconds, your data is queryable by any tool that speaks Iceberg: DuckDB, Spark, Trino, or our built-in query engine.

```typescript
import { createLakeClient } from '@dotdo/lake.do';

// Connect to your lakehouse
const lake = createLakeClient({
  url: 'https://lake.example.com',
  token: process.env.LAKE_TOKEN,
});

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
`);

// Your dashboard shows data from seconds ago, not hours
console.log(`Last hour revenue: $${revenue.rows[0].revenue}`);
```

---

## The Plan

### Step 1: Stream Your Changes

DoLake receives CDC events from your DoSQL instances automatically. Every INSERT, UPDATE, and DELETE flows through WebSocket connections that cost 95% less when idle.

```typescript
// In your DoSQL worker - CDC events flow automatically
// No configuration needed. Just enable CDC on your tables.

// Or subscribe to changes from anywhere:
for await (const batch of lake.subscribe({
  tables: ['orders', 'customers'],
  operations: ['INSERT', 'UPDATE'],
})) {
  console.log(`${batch.events.length} changes in real-time`);
}
```

### Step 2: Query Across Time

Your data maintains full history. Query the present, the past, or any snapshot in between. Time travel isn't a luxury feature; it's built into every table.

```typescript
// What did inventory look like last Tuesday?
const historical = await lake.query(
  'SELECT * FROM inventory WHERE product_id = ?',
  {
    params: ['SKU-123'],
    asOf: new Date('2025-01-14T00:00:00Z')
  }
);

// Compare to current state
const current = await lake.query(
  'SELECT * FROM inventory WHERE product_id = ?',
  { params: ['SKU-123'] }
);

// Understand exactly what changed
console.log('Then:', historical.rows[0]);
console.log('Now:', current.rows[0]);
```

### Step 3: Connect Your Tools

DoLake speaks standard Iceberg. Your existing analytics stack just works.

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

## Avoid the Pitfalls

Without DoLake, you're looking at:

- **Stale data**: Traditional ETL means your analysts are always looking at yesterday
- **Infrastructure complexity**: Kafka + Spark + S3 + Hive Metastore + Airflow = a team just for plumbing
- **Compliance nightmares**: No time travel means no audit trail, no point-in-time recovery
- **Cost overruns**: Reserved capacity for peak load means paying for idle 90% of the time

DoLake eliminates these problems:

| Traditional Lakehouse | DoLake |
|----------------------|--------|
| Hours of latency | Seconds |
| Dedicated infrastructure team | Zero ops |
| Fixed capacity costs | Pay-per-request |
| Complex CDC pipelines | Automatic |
| Vendor lock-in | Standard Iceberg |

---

## Success Looks Like This

```typescript
import { createLakeClient } from '@dotdo/lake.do';

const lake = createLakeClient({
  url: 'https://lake.example.com',
  token: process.env.LAKE_TOKEN,
});

// Real-time analytics dashboard
async function getDashboardMetrics() {
  const [revenue, users, inventory] = await Promise.all([
    // Revenue in the last hour
    lake.query(`
      SELECT SUM(amount) as total
      FROM orders
      WHERE created_at >= NOW() - INTERVAL '1 hour'
    `),

    // Active users today
    lake.query(`
      SELECT COUNT(DISTINCT user_id) as active
      FROM events
      WHERE date = CURRENT_DATE
    `),

    // Low inventory alerts
    lake.query(`
      SELECT product_name, quantity
      FROM inventory
      WHERE quantity < reorder_point
    `),
  ]);

  return {
    hourlyRevenue: revenue.rows[0].total,
    activeUsers: users.rows[0].active,
    lowStockItems: inventory.rows,
    // Data freshness: seconds, not hours
    dataAge: 'real-time',
  };
}

// CDC-powered materialized views
async function syncToDownstream() {
  for await (const batch of lake.subscribe({ tables: ['orders'] })) {
    for (const event of batch.events) {
      if (event.operation === 'INSERT') {
        // Trigger downstream systems in real-time
        await notifyWarehouse(event.after);
        await updateSearchIndex(event.after);
        await refreshCache(event.after.id);
      }
    }
  }
}

// Time travel for debugging
async function investigateIssue(orderId: string, reportedAt: Date) {
  // What did the system see when the bug was reported?
  const snapshot = await lake.query(
    'SELECT * FROM orders WHERE id = ?',
    { params: [orderId], asOf: reportedAt }
  );

  console.log('State at time of report:', snapshot.rows[0]);

  // What changed since then?
  const history = await lake.query(`
    SELECT * FROM orders
    WHERE id = ?
    ORDER BY _commit_timestamp DESC
  `, { params: [orderId] });

  console.log('Full history:', history.rows);
}
```

---

## Get Started

### Install the SDK

```bash
npm install @dotdo/lake.do
```

### Connect and Query

```typescript
import { createLakeClient } from '@dotdo/lake.do';

const lake = createLakeClient({
  url: 'https://your-lakehouse.workers.dev',
  token: process.env.LAKE_TOKEN,
});

// You're ready. Start querying.
const result = await lake.query('SELECT COUNT(*) FROM orders');
console.log(`Total orders: ${result.rows[0].count}`);
```

### Enable CDC Streaming

```typescript
// Subscribe to real-time changes
for await (const batch of lake.subscribe({ tables: ['orders'] })) {
  console.log(`Received ${batch.events.length} changes`);
}
```

---

## The Architecture

```
Your Application
      |
      v
  +--------+     WebSocket      +--------+     Parquet     +-----+
  | DoSQL  | ----------------> | DoLake | --------------> |  R2  |
  +--------+   (CDC Events)    +--------+   (Iceberg)     +-----+
                                    |                         |
                                    |    REST Catalog API     |
                                    v                         v
                            +-------------+           +-------------+
                            |   DuckDB    |           |    Spark    |
                            +-------------+           +-------------+
```

**DoSQL** handles your OLTP workload. Every transaction generates CDC events.

**DoLake** aggregates those events, batches them intelligently, and writes Parquet files with Iceberg metadata.

**R2** stores your data as standard Iceberg tables, queryable by any compatible tool.

**Your analytics** run on real-time data, with full history, at edge latency.

---

## Features

- **Real-time CDC**: Changes flow in seconds, not hours
- **Iceberg Native**: Standard format, no vendor lock-in
- **Time Travel**: Query any point in history
- **Partition Pruning**: Fast queries on large datasets
- **Automatic Compaction**: Small files merged automatically
- **REST Catalog**: Standard API for external engines
- **WebSocket Hibernation**: 95% cost reduction on idle connections
- **Edge Latency**: Data lives where your users are

---

## Documentation

- [Getting Started](https://lake.do/docs/getting-started)
- [CDC Streaming](https://lake.do/docs/cdc-streaming)
- [Time Travel Queries](https://lake.do/docs/time-travel)
- [Connecting External Tools](https://lake.do/docs/external-tools)
- [API Reference](https://lake.do/docs/api)

---

## Start Building

Your data is already flowing. Your analysts are waiting. The infrastructure problem is solved.

```bash
npm install @dotdo/lake.do
```

**[Get Started](https://lake.do/docs/getting-started)** | **[View Examples](https://github.com/dotdo/lake.do/tree/main/examples)** | **[Join Discord](https://discord.gg/dotdo)**

---

*DoLake: Because your data should work as hard as you do.*
