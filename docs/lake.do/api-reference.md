# DoLake API Reference

Complete API documentation for DoLake.

## DoLake Client

### Constructor

```typescript
import { DoLake } from 'lake.do'

const lake = new DoLake({
  catalog: env.LAKE_CATALOG,    // Durable Object binding
  warehouse: 'my-warehouse',     // Warehouse name
  namespace: 'default'           // Optional namespace
})
```

### Configuration Options

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `catalog` | `DurableObjectNamespace` | Yes | DO binding for catalog |
| `warehouse` | `string` | Yes | Warehouse identifier |
| `namespace` | `string` | No | Table namespace (default: 'default') |
| `defaultFormat` | `'parquet' \| 'orc'` | No | Data file format (default: 'parquet') |

## CDC Subscription

### subscribe()

Subscribe to CDC events from a source database.

```typescript
const subscription = await lake.subscribe(sourceDb, {
  tables: ['users', 'orders'],
  startFrom: 'latest',  // or LSN number
  onEvent: async (event) => {
    console.log(event)
  },
  onError: (error) => {
    console.error(error)
  }
})

// Later: stop subscription
await subscription.close()
```

### CDC Event Types

```typescript
interface CDCEvent {
  operation: 'INSERT' | 'UPDATE' | 'DELETE'
  table: string
  schema: string
  before: Record<string, unknown> | null  // null for INSERT
  after: Record<string, unknown> | null   // null for DELETE
  timestamp: string                        // ISO 8601
  lsn: number                             // Log sequence number
  transactionId: string
}
```

## Query API

### query()

Execute a SQL query against the lakehouse.

```typescript
const result = await lake.query<{ id: number; name: string }>(`
  SELECT id, name FROM users WHERE status = ?
`, ['active'])

// Result shape
{
  rows: [{ id: 1, name: 'Alice' }, ...],
  rowCount: 100,
  columns: ['id', 'name'],
  executionTimeMs: 45
}
```

### queryStream()

Stream large result sets.

```typescript
const stream = await lake.queryStream(`
  SELECT * FROM events WHERE date >= '2024-01-01'
`)

for await (const batch of stream) {
  // Process batch of rows
  console.log(`Received ${batch.length} rows`)
}
```

## Time Travel

### AS OF TIMESTAMP

Query data at a specific point in time.

```typescript
const historical = await lake.query(`
  SELECT * FROM users
  AS OF TIMESTAMP '2024-01-15T00:00:00Z'
`)
```

### AS OF VERSION

Query a specific snapshot version.

```typescript
const snapshot = await lake.query(`
  SELECT * FROM orders
  VERSION AS OF 12345
`)
```

### listSnapshots()

List available snapshots for a table.

```typescript
const snapshots = await lake.listSnapshots('users')

// Returns
[
  {
    snapshotId: 12345,
    timestamp: '2024-01-20T10:30:00Z',
    operation: 'append',
    summary: { 'added-records': '1000' }
  },
  // ...
]
```

### diff()

Compare two snapshots.

```typescript
const changes = await lake.diff('users', {
  from: 12340,
  to: 12345
})

// Returns
{
  added: 50,
  updated: 12,
  deleted: 3,
  changedFiles: ['part-00042.parquet']
}
```

## Table Management

### createTable()

Create a new Iceberg table.

```typescript
await lake.createTable('events', {
  columns: [
    { name: 'id', type: 'long', required: true },
    { name: 'type', type: 'string' },
    { name: 'payload', type: 'string' },
    { name: 'created_at', type: 'timestamp' }
  ],
  partitionBy: ['days(created_at)'],
  properties: {
    'write.target-file-size-bytes': '134217728'  // 128MB
  }
})
```

### alterTable()

Modify table schema.

```typescript
// Add column
await lake.alterTable('events', {
  addColumns: [
    { name: 'source', type: 'string' }
  ]
})

// Rename column
await lake.alterTable('events', {
  renameColumn: { from: 'type', to: 'event_type' }
})
```

### dropTable()

Drop a table.

```typescript
await lake.dropTable('events', { purge: true })
```

## Compaction

### compact()

Trigger manual compaction.

```typescript
await lake.compact('events', {
  targetFileSizeBytes: 134217728,  // 128MB
  minInputFiles: 5,
  maxConcurrentJobs: 4
})
```

### getCompactionStatus()

Check compaction progress.

```typescript
const status = await lake.getCompactionStatus('events')

// Returns
{
  running: true,
  progress: 0.75,
  filesProcessed: 150,
  filesRemaining: 50,
  bytesWritten: 1073741824
}
```

## Metadata

### getTableMetadata()

Get table metadata.

```typescript
const metadata = await lake.getTableMetadata('users')

// Returns
{
  tableId: 'abc123',
  location: 'r2://bucket/warehouse/users',
  schema: { ... },
  partitionSpec: { ... },
  currentSnapshotId: 12345,
  properties: { ... }
}
```

### getStatistics()

Get table statistics.

```typescript
const stats = await lake.getStatistics('users')

// Returns
{
  rowCount: 1000000,
  fileSizeBytes: 52428800,
  fileCount: 12,
  lastUpdated: '2024-01-20T10:30:00Z'
}
```

## Error Handling

```typescript
import { DoLakeError, SnapshotNotFoundError, TableNotFoundError } from 'lake.do'

try {
  await lake.query('SELECT * FROM nonexistent')
} catch (error) {
  if (error instanceof TableNotFoundError) {
    console.log(`Table not found: ${error.tableName}`)
  } else if (error instanceof SnapshotNotFoundError) {
    console.log(`Snapshot not found: ${error.snapshotId}`)
  } else if (error instanceof DoLakeError) {
    console.log(`DoLake error: ${error.message}`)
  }
}
```

## TypeScript Types

```typescript
import type {
  DoLakeConfig,
  CDCEvent,
  CDCSubscription,
  QueryResult,
  TableMetadata,
  Snapshot,
  CompactionStatus
} from 'lake.do'
```
