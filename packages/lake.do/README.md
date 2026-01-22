> **Developer Preview** - This package is under active development. APIs may change. Not recommended for production use.

# @dotdo/lake.do

Client SDK for DoLake - Lakehouse on Cloudflare Workers.

## Status

| Property | Value |
|----------|-------|
| Current version | 0.1.0-alpha |
| Stability | Experimental |
| Breaking changes | Expected before 1.0 |

## Stability

### Stable APIs

- Core query execution (`query`)
- Connection management (`createLakeClient`, `close`, `ping`)
- Basic CDC subscription

### Experimental APIs

- Time travel queries (`asOf` option)
- Partition management (`listPartitions`, `compact`)
- Snapshot management (`listSnapshots`)
- Compaction job tracking

## Version Compatibility

| Dependency | Version |
|------------|---------|
| Node.js | 18+ |
| TypeScript | 5.3+ |

## Installation

```bash
pnpm add @dotdo/lake.do
```

## Usage

### Query Lakehouse Data

```typescript
import { createLakeClient } from '@dotdo/lake.do';

const client = createLakeClient({
  url: 'https://lake.example.com',
  token: 'your-token',
});

// Analytical queries on Parquet data
const result = await client.query<{ date: string; revenue: number }>(
  'SELECT date, SUM(amount) as revenue FROM orders GROUP BY date ORDER BY date'
);

console.log(result.rows);
console.log(`Scanned ${result.bytesScanned} bytes from ${result.filesScanned} files`);
```

### CDC Streaming

```typescript
// Subscribe to change data capture events
for await (const batch of client.subscribe({
  tables: ['orders', 'customers'],
  operations: ['INSERT', 'UPDATE'],
})) {
  console.log(`Batch ${batch.sequenceNumber}: ${batch.events.length} events`);

  for (const event of batch.events) {
    switch (event.operation) {
      case 'INSERT':
        console.log('New row:', event.after);
        break;
      case 'UPDATE':
        console.log('Updated:', event.before, '->', event.after);
        break;
      case 'DELETE':
        console.log('Deleted:', event.before);
        break;
    }
  }
}
```

### Time Travel

```typescript
// Query data as of a specific point in time
const historicalData = await client.query(
  'SELECT * FROM inventory WHERE product_id = ?',
  { asOf: new Date('2024-01-01T00:00:00Z') }
);

// List available snapshots
const snapshots = await client.listSnapshots('orders');
for (const snapshot of snapshots) {
  console.log(`Snapshot ${snapshot.id} at ${snapshot.timestamp}`);
}
```

### Partition Management

```typescript
// List partitions
const partitions = await client.listPartitions('orders');
for (const partition of partitions) {
  console.log(`${partition.key}: ${partition.fileCount} files, ${partition.rowCount} rows`);
}

// Trigger compaction
const job = await client.compact(partitions[0].key, {
  targetFileSize: 128 * 1024 * 1024, // 128MB
  minFiles: 5,
});

// Check compaction status
const status = await client.getCompactionStatus(job.id);
console.log(`Compaction ${status.status}: ${status.bytesWritten} bytes written`);
```

## API

### `createLakeClient(config)`

Create a new lakehouse client instance.

```typescript
interface LakeClientConfig {
  url: string;        // DoLake endpoint URL
  token?: string;     // Authentication token
  timeout?: number;   // Request timeout (ms)
}
```

### Query Methods

- `query<T>(sql, options?)` - Execute analytical queries on Parquet data
- `subscribe(options?)` - Subscribe to CDC stream (async iterable)

### Metadata Methods

- `getMetadata(tableName)` - Get table metadata and schema
- `listPartitions(tableName)` - List table partitions
- `listSnapshots(tableName)` - List snapshots for time travel

### Compaction Methods

- `compact(partition, config?)` - Trigger partition compaction
- `getCompactionStatus(jobId)` - Get compaction job status

### Other Methods

- `ping()` - Check connection health
- `close()` - Close connection

## Types

Import types for type-safe usage:

```typescript
import type {
  CDCBatch,
  CDCEvent,
  LakeQueryResult,
  PartitionInfo,
  Snapshot,
  TableMetadata,
} from '@dotdo/lake.do';
```

## License

MIT
