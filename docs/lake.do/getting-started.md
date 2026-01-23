# Getting Started with DoLake

DoLake is a real-time Iceberg lakehouse that runs on Cloudflare's edge. Stream your database changes to a lakehouse in milliseconds, query across time, and connect your favorite analytics tools.

## What is DoLake?

- **Real-time CDC streaming** - Database changes flow to your lakehouse in milliseconds
- **Time travel built-in** - Query your data as it existed at any point in time
- **Open format** - Standard Apache Iceberg tables work with DuckDB, Spark, Trino
- **Zero infrastructure** - Runs on Cloudflare R2 and Durable Objects

## Quick Start

### 1. Install

```bash
npm install @dotdo/lake.do
```

### 2. Configure CDC Source

Connect your database to stream changes:

```typescript
import { DoLake } from '@dotdo/lake.do'

const lake = new DoLake({
  catalog: env.LAKE_CATALOG,
  warehouse: 'my-warehouse'
})

// Subscribe to CDC events from DoSQL
const subscription = await lake.subscribe('my-database', {
  tables: ['users', 'orders', 'events'],
  onEvent: async (event) => {
    console.log(`${event.operation} on ${event.table}`)
  }
})
```

### 3. Query Your Lakehouse

```typescript
// Query current data
const result = await lake.query(`
  SELECT
    date_trunc('day', created_at) as day,
    COUNT(*) as orders,
    SUM(total) as revenue
  FROM orders
  GROUP BY 1
  ORDER BY 1 DESC
  LIMIT 30
`)

// Time travel - query historical state
const snapshot = await lake.query(`
  SELECT * FROM users
  AS OF TIMESTAMP '2024-01-15T00:00:00Z'
`)
```

### 4. Connect Analytics Tools

DoLake produces standard Iceberg tables that work with any tool:

```python
# DuckDB
import duckdb
conn = duckdb.connect()
conn.execute("INSTALL iceberg; LOAD iceberg;")
result = conn.execute("""
  SELECT * FROM iceberg_scan('r2://my-bucket/warehouse/orders')
""").fetchall()
```

```sql
-- Spark
SELECT * FROM lake.orders
WHERE created_at >= '2024-01-01'
```

## Core Concepts

### CDC Streaming

DoLake captures every INSERT, UPDATE, and DELETE from your source database:

```typescript
// Events include before/after values
{
  operation: 'UPDATE',
  table: 'users',
  before: { id: 1, status: 'active' },
  after: { id: 1, status: 'premium' },
  timestamp: '2024-01-20T10:30:00Z',
  lsn: 1234567
}
```

### Iceberg Tables

Data is stored in Apache Iceberg format with automatic compaction:

- **Snapshots** - Every change creates a new snapshot for time travel
- **Partitioning** - Automatic partitioning by time or custom columns
- **Schema evolution** - Add columns without rewriting data

### Time Travel

Query any point in history:

```typescript
// By timestamp
await lake.query(`SELECT * FROM orders AS OF '2024-01-15'`)

// By snapshot ID
await lake.query(`SELECT * FROM orders VERSION AS OF 12345`)

// Diff between snapshots
await lake.diff('orders', { from: snapshot1, to: snapshot2 })
```

## Next Steps

- [Architecture Overview](./architecture.md) - How DoLake works under the hood
- [CDC Configuration](./cdc.md) - Advanced CDC streaming options
- [Iceberg Tables](./iceberg.md) - Working with Iceberg format
- [Analytics Integration](./analytics.md) - Connect DuckDB, Spark, Trino
- [API Reference](./api-reference.md) - Complete API documentation
