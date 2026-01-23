# CDC Streaming Configuration

Configure Change Data Capture streaming from your databases to DoLake.

## Supported Sources

| Source | Status | Notes |
|--------|--------|-------|
| DoSQL | ✅ Full Support | Native integration |
| PostgreSQL | ✅ Full Support | Logical replication |
| MySQL | 🔜 Coming Soon | Binlog streaming |
| SQLite | ✅ Full Support | Via DoSQL |

## Basic Configuration

### DoSQL Source

```typescript
import { DoLake } from 'lake.do'
import { DoSQL } from 'dosql'

// Create DoLake instance
const lake = new DoLake({
  catalog: env.LAKE_CATALOG,
  warehouse: 'analytics'
})

// Subscribe to DoSQL CDC
const subscription = await lake.subscribe(env.DOSQL_DB, {
  tables: ['users', 'orders', 'events'],
  startFrom: 'latest'
})
```

### PostgreSQL Source

```typescript
const subscription = await lake.subscribePostgres({
  connectionString: env.POSTGRES_URL,
  publication: 'my_publication',
  slot: 'dolake_slot',
  tables: ['public.users', 'public.orders']
})
```

## Subscription Options

```typescript
interface SubscribeOptions {
  // Tables to replicate
  tables: string[]

  // Where to start reading
  startFrom: 'latest' | 'earliest' | number  // LSN

  // Event handlers
  onEvent?: (event: CDCEvent) => Promise<void>
  onError?: (error: Error) => void
  onCheckpoint?: (lsn: number) => void

  // Batching
  batchSize?: number      // Events per batch (default: 1000)
  batchTimeoutMs?: number // Max wait time (default: 1000)

  // Filtering
  filter?: {
    operations?: ('INSERT' | 'UPDATE' | 'DELETE')[]
    columns?: string[]  // Only include these columns
  }

  // Transforms
  transform?: (event: CDCEvent) => CDCEvent | null
}
```

## Event Filtering

### By Operation Type

```typescript
await lake.subscribe(source, {
  tables: ['audit_log'],
  filter: {
    operations: ['INSERT']  // Only captures inserts
  }
})
```

### By Columns

```typescript
await lake.subscribe(source, {
  tables: ['users'],
  filter: {
    columns: ['id', 'email', 'updated_at']  // Exclude sensitive columns
  }
})
```

### Custom Transform

```typescript
await lake.subscribe(source, {
  tables: ['orders'],
  transform: (event) => {
    // Mask PII
    if (event.after?.credit_card) {
      event.after.credit_card = '****'
    }
    // Filter out test data
    if (event.after?.email?.endsWith('@test.com')) {
      return null  // Skip this event
    }
    return event
  }
})
```

## Partitioning Strategy

Configure how data is partitioned in the lakehouse:

```typescript
await lake.subscribe(source, {
  tables: ['events'],
  partitioning: {
    events: {
      by: ['days(created_at)', 'bucket(16, user_id)'],
      targetFileSize: '128MB'
    }
  }
})
```

### Partition Types

| Type | Syntax | Example |
|------|--------|---------|
| Identity | `column` | `country` |
| Year | `years(column)` | `years(created_at)` |
| Month | `months(column)` | `months(created_at)` |
| Day | `days(column)` | `days(created_at)` |
| Hour | `hours(column)` | `hours(created_at)` |
| Bucket | `bucket(n, column)` | `bucket(16, user_id)` |
| Truncate | `truncate(n, column)` | `truncate(10, zip_code)` |

## Error Handling

### Retry Configuration

```typescript
await lake.subscribe(source, {
  tables: ['orders'],
  retry: {
    maxAttempts: 5,
    initialDelayMs: 1000,
    maxDelayMs: 30000,
    backoffMultiplier: 2
  }
})
```

### Dead Letter Queue

```typescript
await lake.subscribe(source, {
  tables: ['orders'],
  deadLetterQueue: {
    enabled: true,
    table: 'cdc_failures',
    maxRetries: 3
  }
})
```

## Monitoring

### Subscription Status

```typescript
const status = await subscription.getStatus()

// Returns
{
  state: 'running',
  currentLsn: 123456789,
  lag: {
    events: 150,
    bytes: 52428,
    timeMs: 2500
  },
  throughput: {
    eventsPerSecond: 1250,
    bytesPerSecond: 524288
  }
}
```

### Metrics

```typescript
const metrics = await lake.getMetrics('cdc')

// Returns
{
  eventsReceived: 1000000,
  eventsProcessed: 999850,
  eventsFailed: 150,
  bytesProcessed: 1073741824,
  avgLatencyMs: 45,
  p99LatencyMs: 250
}
```

## Schema Evolution

DoLake automatically handles schema changes:

### Adding Columns

When source adds a new column, DoLake:
1. Detects the schema change
2. Updates Iceberg table schema
3. New data includes the column
4. Old data returns NULL for new column

### Removing Columns

When source removes a column:
1. Existing data preserved
2. New events won't include column
3. Queries still work on historical data

### Type Changes

Compatible changes are automatic. For breaking changes:

```typescript
await lake.subscribe(source, {
  tables: ['users'],
  schemaEvolution: {
    allowTypeWidening: true,    // int → bigint
    allowTypeNarrowing: false,  // Reject bigint → int
    onIncompatibleChange: 'fail' // or 'create_new_table'
  }
})
```

## Best Practices

1. **Start with specific tables** - Don't replicate everything
2. **Use appropriate partitioning** - Match your query patterns
3. **Monitor lag** - Set alerts for replication delays
4. **Handle failures** - Configure DLQ for problematic events
5. **Test schema changes** - Use staging environment first
