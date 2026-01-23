> **Developer Preview** - This package is under active development. APIs may change. Not recommended for production use.

# lake.do

Client SDK for DoLake - Lakehouse on Cloudflare Workers.

## Status

| Property | Value |
|----------|-------|
| Current version | 0.1.0 |
| Stability | Experimental |
| Breaking changes | Expected before 1.0 |

## Stability

### Stability Legend

- :green_circle: **Stable** - API is stable and unlikely to change. Safe for production use.
- :yellow_circle: **Beta** - API is mostly stable but may have minor changes. Use with caution in production.
- :red_circle: **Experimental** - API is under active development and may change significantly. Not recommended for production.

### API Stability by Category

| API | Methods | Stability |
|-----|---------|-----------|
| Core query execution | `query` | :green_circle: Stable |
| Connection management | `createLakeClient`, `close`, `ping` | :green_circle: Stable |
| Basic CDC subscription | `subscribe` | :green_circle: Stable |
| CDC stream controller | `CDCStreamController` | :green_circle: Stable |
| CDC types | `CDCStreamOptions`, `CDCBatch`, `CDCEvent` | :red_circle: Experimental |
| Time travel queries | `asOf` option | :green_circle: Stable |
| Partition management | `listPartitions`, `compact` | :green_circle: Stable |
| Snapshot management | `listSnapshots` | :green_circle: Stable |
| Compaction types | `CompactionConfig`, `CompactionJob` | :red_circle: Experimental |
| Compaction job tracking | `getCompactionStatus` | :green_circle: Stable |

## Version Compatibility

| Dependency | Version |
|------------|---------|
| Node.js | 18+ |
| TypeScript | 5.3+ |

## Installation

```bash
pnpm add lake.do
```

## Usage

### Query Lakehouse Data

```typescript
import { createLakeClient } from 'lake.do';

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

// Parameterized queries for safe value interpolation
const filtered = await client.query<{ id: string; name: string }>(
  'SELECT id, name FROM customers WHERE region = ? AND status = ?',
  { params: ['us-west', 'active'] }
);
```

> **Note:** Always use parameterized queries (`?` placeholders with the `params` option) when incorporating user input or dynamic values. This prevents SQL injection and ensures proper value escaping.

### CDC Streaming

```typescript
import { CDCError, ConnectionError } from 'lake.do';

// Subscribe to change data capture events with error handling
async function consumeCDCStream() {
  let retryCount = 0;
  const maxRetries = 5;

  while (retryCount < maxRetries) {
    try {
      for await (const batch: CDCBatch of client.subscribe({
        tables: ['orders', 'customers'],
        operations: ['INSERT', 'UPDATE'],
      })) {
        // Reset retry count on successful batch
        retryCount = 0;

        console.log(`Batch ${batch.sequenceNumber}: ${batch.events.length} events`);

        for (const event: CDCEvent of batch.events) {
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
      // Stream ended normally
      break;
    } catch (error) {
      if (error instanceof ConnectionError) {
        retryCount++;
        const delay = Math.min(1000 * Math.pow(2, retryCount), 30000);
        console.error(`Connection lost, retrying in ${delay}ms (attempt ${retryCount}/${maxRetries})`);
        await new Promise(resolve => setTimeout(resolve, delay));
      } else if (error instanceof CDCError) {
        console.error(`CDC error [${error.code}]: ${error.message}`);
        // Handle specific CDC errors
        if (error.code === 'SEQUENCE_GAP') {
          console.warn('Sequence gap detected - some events may be missed');
          // Optionally reset to latest position
        }
        throw error; // Re-throw non-recoverable errors
      } else {
        throw error;
      }
    }
  }

  if (retryCount >= maxRetries) {
    throw new Error(`CDC stream failed after ${maxRetries} retries`);
  }
}

consumeCDCStream().catch(console.error);
```

### CDC Streaming with Backpressure Control

For high-throughput scenarios, use `CDCStreamController` directly for fine-grained control over queue bounds and backpressure:

```typescript
import { CDCStreamController, type BackpressureStrategy } from 'lake.do';

// Create a controller with backpressure configuration
const controller = new CDCStreamController({
  maxQueueSize: 100,                    // Buffer up to 100 batches (default: 1000)
  backpressureStrategy: 'drop-oldest',  // Strategy when queue is full
  highWaterMark: 80,                    // Emit event at 80% capacity
  lowWaterMark: 20,                     // Emit event when back to 20%
  onHighWaterMark: (event) => {
    console.warn('Queue filling up:', event.utilizationPercent + '%');
  },
  onLowWaterMark: (event) => {
    console.log('Queue recovered:', event.utilizationPercent + '%');
  },
});

// Push batches (from your CDC source)
const accepted: boolean = controller.push(batch);
if (!accepted) {
  console.warn('Queue full, batch rejected');
}

// Or use async push to wait for space (only with 'block' strategy)
await controller.pushAsync(batch);

// Consume batches via async iteration
for await (const batch: CDCBatch of controller) {
  await processBatch(batch);
}

// Monitor queue metrics
const metrics: CDCStreamMetrics = controller.getMetrics();
console.log(`Queue: ${metrics.currentDepth}/${metrics.maxDepth} (${metrics.utilizationPercent}%)`);
console.log(`Peak depth: ${metrics.peakDepth}, Dropped: ${metrics.droppedCount}`);

// Clean up
controller.close();
```

#### Backpressure Strategies

| Strategy | Behavior | Use Case |
|----------|----------|----------|
| `block` | `push()` returns `false` when full; `pushAsync()` waits | Guaranteed delivery, slower producers |
| `drop-oldest` | Removes oldest batch to make room | Real-time data where latest matters most |
| `drop-newest` | Rejects new batch without modifying queue | Preserve historical order |

#### CDCStreamController Options

```typescript
interface CDCStreamControllerOptions {
  /** Maximum batches to buffer (default: 1000, minimum: 1) */
  maxQueueSize?: number;
  /** Strategy when queue is full (default: 'block') */
  backpressureStrategy?: BackpressureStrategy;
  /** High water mark threshold percentage (0-100) */
  highWaterMark?: number;
  /** Low water mark threshold percentage (0-100) */
  lowWaterMark?: number;
  /** Callback when queue reaches high water mark */
  onHighWaterMark?: (event: WaterMarkEvent) => void;
  /** Callback when queue falls below low water mark */
  onLowWaterMark?: (event: WaterMarkEvent) => void;
}
```

#### Queue Metrics

```typescript
interface CDCStreamMetrics {
  currentDepth: number;      // Current buffered batches
  maxDepth: number;          // Configured max queue size
  peakDepth: number;         // Highest depth seen since reset
  droppedCount: number;      // Batches dropped due to backpressure
  totalPushed: number;       // Total batches pushed
  totalConsumed: number;     // Total batches consumed
  utilizationPercent: number; // Queue utilization (0-100)
}
```

#### Runtime Queue Management

```typescript
// Adjust queue size at runtime (excess batches dropped per strategy)
controller.setMaxQueueSize(50);

// Reset metrics for monitoring windows
controller.resetMetrics();
```

### Time Travel

```typescript
// Query data as of a specific point in time
const historicalData = await client.query(
  'SELECT * FROM inventory WHERE product_id = ?',
  { asOf: new Date('2025-01-01T00:00:00Z') }
);

// List available snapshots
const snapshots: Snapshot[] = await client.listSnapshots('orders');
for (const snapshot: Snapshot of snapshots) {
  console.log(`Snapshot ${snapshot.id} at ${snapshot.timestamp}`);
}
```

### Partition Management

```typescript
// List partitions
const partitions: PartitionInfo[] = await client.listPartitions('orders');
for (const partition: PartitionInfo of partitions) {
  console.log(`${partition.key}: ${partition.fileCount} files, ${partition.rowCount} rows`);
}

// Trigger compaction
const job: { id: string } = await client.compact(partitions[0].key, {
  targetFileSize: 128 * 1024 * 1024, // 128MB
  minFiles: 5,
});

// Check compaction status
const status: { status: string; bytesWritten: number } = await client.getCompactionStatus(job.id);
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
  retry?: RetryConfig; // Retry configuration for transient failures
}

interface RetryConfig {
  maxRetries: number;  // Maximum retry attempts (default: 3)
  baseDelayMs: number; // Base delay for exponential backoff (default: 100)
  maxDelayMs: number;  // Maximum delay cap (default: 5000)
}
```

#### Retry Configuration

The client automatically retries transient failures (timeouts, connection errors) using exponential backoff with jitter. Application-level errors (syntax errors, table not found) fail immediately without retry.

```typescript
const client = createLakeClient({
  url: 'https://lake.example.com',
  token: 'your-token',
  retry: {
    maxRetries: 5,      // Retry up to 5 times (6 total attempts)
    baseDelayMs: 200,   // Start with 200ms delay
    maxDelayMs: 10000,  // Cap delay at 10 seconds
  },
});
```

Default retry behavior:
- **maxRetries**: 3 (4 total attempts including the initial request)
- **baseDelayMs**: 100ms (delay doubles each retry: 100ms, 200ms, 400ms...)
- **maxDelayMs**: 5000ms (delays are capped at 5 seconds)

To disable retries:
```typescript
const client = createLakeClient({
  url: 'https://lake.example.com',
  retry: { maxRetries: 0, baseDelayMs: 0, maxDelayMs: 0 },
});
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

### CDC Stream Classes

- `CDCStreamController` - Controller for CDC stream with backpressure support
- `BoundedQueue` - Low-level bounded queue (for advanced usage)
- `MetricsTracker` - Queue metrics tracking (for advanced usage)

## Types

Import types for type-safe usage:

```typescript
import type {
  // Client configuration types
  LakeClientConfig,
  RetryConfig,
  LakeClient,

  // Query types
  LakeQueryOptions,
  LakeQueryResult,

  // CDC types (experimental)
  CDCStreamOptions,
  CDCBatch,
  CDCEvent,
  CDCOperation,
  CDCStreamState,
  ClientCDCOperation,
  ClientCapabilities,

  // CDC Stream controller types
  CDCStreamControllerOptions,
  CDCStreamMetrics,
  BackpressureStrategy,
  WaterMarkEvent,

  // Partition types
  PartitionStrategy,
  PartitionConfig,
  PartitionInfo,

  // Snapshot and time travel types
  Snapshot,
  TimeTravelOptions,

  // Schema types
  LakeColumnType,
  LakeColumn,
  LakeSchema,
  TableMetadata,

  // Compaction types (experimental)
  CompactionConfig,
  CompactionJob,

  // Metrics types (experimental)
  LakeMetrics,

  // RPC types (internal, for advanced usage)
  LakeRPCMethod,
  LakeRPCRequest,
  LakeRPCResponse,
  LakeRPCError,

  // Branded types for type-safe identifiers
  CDCEventId,
  PartitionKey,
  ParquetFileId,
  SnapshotId,
  CompactionJobId,

  // Re-exported types from sql.do
  TransactionId,
  LSN,
  SQLValue,
} from 'lake.do';
```

### Branded Type Factory Functions

The library provides branded types for type-safe identifiers. These prevent accidentally mixing up different ID types at compile time. Use the factory functions to create branded type instances from plain strings.

#### Available Factory Functions

| Function | Returns | Description |
|----------|---------|-------------|
| `createCDCEventId(id)` | `CDCEventId` | Creates a CDC event identifier |
| `createPartitionKey(key)` | `PartitionKey` | Creates a partition key (e.g., `date=2024-01-15`) |
| `createParquetFileId(id)` | `ParquetFileId` | Creates a Parquet file identifier |
| `createSnapshotId(id)` | `SnapshotId` | Creates a snapshot identifier for time travel |
| `createCompactionJobId(id)` | `CompactionJobId` | Creates a compaction job identifier |

#### Basic Usage

```typescript
import {
  createCDCEventId,
  createPartitionKey,
  createParquetFileId,
  createSnapshotId,
  createCompactionJobId,
} from 'lake.do';

// Create branded identifiers from plain strings
const partitionKey = createPartitionKey('date=2024-01-15');
const snapshotId = createSnapshotId('snap_789');
const jobId = createCompactionJobId('compact_456');
const fileId = createParquetFileId('data_00001.parquet');
const eventId = createCDCEventId('evt_123456');
```

#### Type Safety Example

Branded types prevent mixing up identifiers at compile time:

```typescript
import {
  createPartitionKey,
  createSnapshotId,
  type PartitionKey,
  type SnapshotId,
} from 'lake.do';

function processPartition(key: PartitionKey): void {
  // ...
}

function querySnapshot(id: SnapshotId): void {
  // ...
}

const partitionKey = createPartitionKey('date=2024-01-15');
const snapshotId = createSnapshotId('snap_789');

// Correct usage - compiles successfully
processPartition(partitionKey);
querySnapshot(snapshotId);

// Type errors - prevented at compile time
// processPartition(snapshotId);  // Error: SnapshotId is not assignable to PartitionKey
// querySnapshot(partitionKey);   // Error: PartitionKey is not assignable to SnapshotId
// processPartition('date=2024-01-15');  // Error: string is not assignable to PartitionKey
```

#### Using with Client Methods

```typescript
import { createLakeClient, createPartitionKey, createSnapshotId } from 'lake.do';

const client = createLakeClient({ url: 'https://lake.example.com' });

// Time travel query using a snapshot ID
const snapshotId = createSnapshotId('snap_789');
const historicalData = await client.query('SELECT * FROM orders', {
  asOf: snapshotId,
});

// Compact a specific partition
const partitionKey = createPartitionKey('date=2024-01-15');
const job = await client.compact(partitionKey, {
  targetFileSize: 128 * 1024 * 1024,
  minFiles: 5,
});

// Check compaction status using the job ID (already branded from compact())
const status = await client.getCompactionStatus(job.id);
console.log(`Compaction ${status.status}`);
```

#### Development Mode Validation

In development mode (`NODE_ENV !== 'production'`), the factory functions validate their inputs:

```typescript
import { createPartitionKey } from 'lake.do';

// These throw errors in development mode
createPartitionKey('');         // Error: PartitionKey cannot be empty
createPartitionKey('   ');      // Error: PartitionKey cannot be empty
createPartitionKey(123 as any); // Error: PartitionKey must be a string

// In production mode, validation is skipped for performance
```

#### Working with API Responses

When receiving data from API responses, use the factory functions to convert plain strings to branded types:

```typescript
import {
  createSnapshotId,
  createPartitionKey,
  type Snapshot,
  type PartitionInfo,
} from 'lake.do';

// API responses return branded types directly
const snapshots: Snapshot[] = await client.listSnapshots('orders');
const partitions: PartitionInfo[] = await client.listPartitions('orders');

// Access the branded IDs directly
for (const snapshot of snapshots) {
  console.log(`Snapshot ${snapshot.id} at ${snapshot.timestamp}`);
  // snapshot.id is already typed as SnapshotId
}

for (const partition of partitions) {
  if (partition.fileCount > 10) {
    // partition.key is already typed as PartitionKey
    await client.compact(partition.key);
  }
}

// When working with external data, use factory functions
const externalSnapshotId = createSnapshotId(response.snapshotId);
const externalPartitionKey = createPartitionKey(response.partitionKey);
```

### CDC Constants and Utilities

```typescript
import {
  // CDC operation codes for efficient binary encoding
  CDCOperationCode,

  // Default client capabilities
  DEFAULT_CLIENT_CAPABILITIES,
  DEFAULT_RETRY_CONFIG,

  // Type guards
  isServerCDCEvent,
  isClientCDCEvent,
  isDateTimestamp,
  isNumericTimestamp,
  isRetryConfig,

  // Converters
  serverToClientCDCEvent,
  clientToServerCDCEvent,
  createRetryConfig,
} from 'lake.do';
```

### Event Types

```typescript
import type {
  LakeClientEventType,    // 'connected' | 'disconnected' | 'reconnecting' | 'reconnected' | 'error'
  LakeClientEventHandler, // (event?: unknown) => void
} from 'lake.do';
```

## Troubleshooting

### Connection Errors

**Symptoms**: `ConnectionError` when connecting or executing queries

| Error Code | Cause | Solution |
|------------|-------|----------|
| `CONNECTION_ERROR` | Network issue or invalid URL | Verify URL; check network connectivity |
| `CONNECTION_CLOSED` | WebSocket connection dropped | Implement reconnection; check server health |
| `NOT_CONNECTED` | Operation attempted without connection | Call `connect()` or check `isConnected()` |
| `CONNECTION_TIMEOUT` | Connection attempt timed out | Increase timeout; verify endpoint accessibility |

```typescript
import { ConnectionError, createLakeClient } from 'lake.do';

const client = createLakeClient({
  url: 'https://lake.example.com',
  token: 'your-token',
  timeout: 60000,  // Increase timeout for slow connections
});

// Handle connection errors
try {
  const result = await client.query('SELECT * FROM orders');
} catch (error) {
  if (error instanceof ConnectionError) {
    if (error.code === 'CONNECTION_CLOSED') {
      // Attempt reconnection
      await client.connect();
    } else if (error.code === 'NOT_CONNECTED') {
      // Ensure connection before query
      await client.connect();
    }
  }
}
```

### Rate Limiting and Backpressure

**Symptoms**: Queries fail or CDC stream drops events

| Issue | Cause | Solution |
|-------|-------|----------|
| Queue full | CDC events arriving faster than processing | Increase `maxQueueSize`; use `drop-oldest` strategy |
| High water mark | Queue approaching capacity | Handle `onHighWaterMark` callback; slow down producers |
| Dropped events | Backpressure strategy discarding events | Use `block` strategy for guaranteed delivery |

```typescript
import { CDCStreamController } from 'lake.do';

// Configure backpressure handling
const controller = new CDCStreamController({
  maxQueueSize: 500,                    // Increase buffer size
  backpressureStrategy: 'drop-oldest',  // Or 'block' for guaranteed delivery
  highWaterMark: 70,                    // Warning at 70%
  lowWaterMark: 30,                     // Recovery at 30%

  onHighWaterMark: (event) => {
    console.warn(`Queue at ${event.utilizationPercent}% - slowing down`);
    // Signal producers to slow down
  },

  onLowWaterMark: (event) => {
    console.log(`Queue recovered to ${event.utilizationPercent}%`);
    // Resume normal processing
  },
});

// Monitor metrics
const metrics = controller.getMetrics();
console.log(`Queue: ${metrics.currentDepth}/${metrics.maxDepth}`);
console.log(`Dropped: ${metrics.droppedCount}`);
```

### Query Errors

**Symptoms**: `QueryError` or `LakeError` when executing queries

| Error Code | Cause | Solution |
|------------|-------|----------|
| `TABLE_NOT_FOUND` | Table does not exist | Verify table name; check namespace |
| `INVALID_SQL` | SQL syntax error | Check query syntax; use parameterized queries |
| `QUERY_TIMEOUT` | Query execution timed out | Simplify query; add partition filters |
| `PARTITION_NOT_FOUND` | Requested partition missing | Check partition key format |

```typescript
import { QueryError, LakeError } from 'lake.do';

try {
  const result = await client.query('SELECT * FROM nonexistent_table');
} catch (error) {
  if (error instanceof QueryError) {
    switch (error.code) {
      case 'TABLE_NOT_FOUND':
        console.error('Table not found - check table name');
        break;
      case 'INVALID_SQL':
        console.error('SQL syntax error:', error.message);
        break;
      case 'QUERY_TIMEOUT':
        console.error('Query timed out - add partition filters');
        break;
    }
  } else if (error instanceof LakeError) {
    console.error(`Lake error [${error.code}]: ${error.message}`);
  }
}
```

### Timeout Errors

**Symptoms**: `TimeoutError` on queries or operations

| Error | Cause | Solution |
|-------|-------|----------|
| `Request timeout: query` | Query execution exceeded limit | Increase timeout; optimize query |
| `Request timeout: compact` | Compaction taking too long | Reduce partition size; increase timeout |
| `Request timeout: listPartitions` | Too many partitions | Use filters; paginate results |

```typescript
import { TimeoutError, createLakeClient } from 'lake.do';

// Configure longer timeout for analytical queries
const client = createLakeClient({
  url: 'https://lake.example.com',
  timeout: 120000,  // 2 minutes
  retry: {
    maxRetries: 2,
    baseDelayMs: 500,
    maxDelayMs: 10000,
  },
});

try {
  const result = await client.query(`
    SELECT date, SUM(revenue) as total
    FROM orders
    WHERE date >= '2024-01-01'
    GROUP BY date
  `);
} catch (error) {
  if (error instanceof TimeoutError) {
    console.error(`${error.method} timed out - try adding filters`);
  }
}
```

### Common Configuration Mistakes

| Issue | Symptom | Fix |
|-------|---------|-----|
| Wrong URL | Connection fails | Use full URL including protocol (https://) |
| Missing token | `UNAUTHORIZED` error | Set `token` in configuration |
| Retry disabled | Transient failures not handled | Add `retry` configuration |
| Small timeout | Queries timeout frequently | Increase `timeout` for analytical workloads |

```typescript
// Complete configuration example
const client = createLakeClient({
  // Required
  url: 'https://lake.example.com',

  // Authentication
  token: process.env.LAKE_TOKEN,

  // Timeout (ms) - increase for large queries
  timeout: 120000,

  // Retry configuration
  retry: {
    maxRetries: 3,      // Retry up to 3 times
    baseDelayMs: 200,   // Start with 200ms delay
    maxDelayMs: 10000,  // Cap at 10 seconds
  },
});
```

### Performance Issues

| Symptom | Cause | Solution |
|---------|-------|----------|
| Slow queries | Full table scan | Add partition filters (date, region) |
| High latency | Many small files | Run compaction on affected partitions |
| Memory issues | Large result sets | Use streaming or pagination |
| CDC lag | Consumer too slow | Increase processing parallelism |

```typescript
// Optimize queries with partition pruning
const result = await client.query(`
  SELECT customer_id, SUM(amount) as total
  FROM orders
  WHERE dt >= '2024-01-01' AND dt < '2024-02-01'  -- Partition filter
  GROUP BY customer_id
`);

console.log(`Scanned ${result.filesScanned} files, ${result.bytesScanned} bytes`);

// Trigger compaction for slow partitions
const partitions = await client.listPartitions('orders');
for (const partition of partitions) {
  if (partition.fileCount > 100) {
    await client.compact(partition.key, {
      targetFileSize: 128 * 1024 * 1024,
      minFiles: 5,
    });
  }
}
```

## WebSocket Protocol

The lake.do client communicates with the DoLake server using a WebSocket-based RPC protocol. This section documents the protocol for developers implementing custom clients or debugging connections.

### Connection Establishment

The client converts HTTP(S) URLs to WebSocket URLs automatically:

```typescript
// Client configuration
const client = createLakeClient({
  url: 'https://lake.example.com',  // Converted to wss://lake.example.com
  token: 'your-token',
});

// Manual connection (optional - auto-connects on first operation)
await client.connect();
```

The WebSocket URL transformation:
- `https://` becomes `wss://`
- `http://` becomes `ws://`

### Message Format

All messages are JSON-encoded strings sent over the WebSocket connection.

#### Request Format

```typescript
interface LakeRPCRequest {
  /** Unique request ID for correlating responses */
  id: string;
  /** The RPC method to invoke */
  method: LakeRPCMethod;
  /** Method-specific parameters */
  params: unknown;
}

type LakeRPCMethod =
  | 'query'
  | 'subscribe'
  | 'unsubscribe'
  | 'getMetadata'
  | 'listPartitions'
  | 'getPartition'
  | 'compact'
  | 'getCompactionStatus'
  | 'listSnapshots'
  | 'getSnapshot'
  | 'ping';
```

#### Response Format

```typescript
interface LakeRPCResponse<T = unknown> {
  /** Request ID this response corresponds to */
  id: string;
  /** The result payload (present on success) */
  result?: T;
  /** Error details (present on failure) */
  error?: LakeRPCError;
}

interface LakeRPCError {
  /** Machine-readable error code */
  code: string;
  /** Human-readable error message */
  message: string;
  /** Additional error context */
  details?: unknown;
}
```

### Message Examples

#### Query Request/Response

```json
// Request
{
  "id": "1",
  "method": "query",
  "params": {
    "sql": "SELECT * FROM orders WHERE status = ?",
    "params": ["active"],
    "limit": 100
  }
}

// Success Response
{
  "id": "1",
  "result": {
    "rows": [{ "id": "ord_123", "status": "active", "amount": 99.99 }],
    "columns": ["id", "status", "amount"],
    "rowCount": 1,
    "bytesScanned": 1048576,
    "filesScanned": 3,
    "partitionsScanned": 1,
    "duration": 42
  }
}

// Error Response
{
  "id": "1",
  "error": {
    "code": "TABLE_NOT_FOUND",
    "message": "Table 'orders' does not exist"
  }
}
```

#### Subscribe Request

```json
// Request
{
  "id": "2",
  "method": "subscribe",
  "params": {
    "tables": ["orders", "inventory"],
    "operations": ["INSERT", "UPDATE"],
    "batchSize": 100,
    "batchTimeoutMs": 1000
  }
}

// Success Response
{
  "id": "2",
  "result": null
}
```

#### CDC Batch (Server Push)

After subscribing, the server pushes CDC batches without a request ID:

```json
{
  "type": "cdc_batch",
  "events": [
    {
      "table": "orders",
      "operation": "INSERT",
      "timestamp": "2025-01-15T10:30:00Z",
      "after": { "id": "ord_456", "status": "pending", "amount": 150.00 }
    },
    {
      "table": "orders",
      "operation": "UPDATE",
      "timestamp": "2025-01-15T10:30:01Z",
      "before": { "id": "ord_123", "status": "active" },
      "after": { "id": "ord_123", "status": "shipped" }
    }
  ],
  "sequenceNumber": 42,
  "timestamp": "2025-01-15T10:30:02Z",
  "sourceDoId": "do_abc123"
}
```

#### Ping Request

```json
// Request
{
  "id": "3",
  "method": "ping",
  "params": {}
}

// Response
{
  "id": "3",
  "result": null
}
```

#### Metadata Request

```json
// Request
{
  "id": "4",
  "method": "getMetadata",
  "params": { "tableName": "orders" }
}

// Response
{
  "id": "4",
  "result": {
    "tableId": "orders",
    "schema": {
      "schemaId": 1,
      "columns": [
        { "id": 1, "name": "id", "type": "string", "required": true },
        { "id": 2, "name": "amount", "type": "double", "required": true },
        { "id": 3, "name": "created_at", "type": "timestamptz", "required": true }
      ],
      "identifierFields": [1]
    },
    "partitionSpec": {
      "strategy": "time",
      "column": "created_at",
      "granularity": "daily"
    },
    "currentSnapshotId": "snap_789",
    "snapshots": [
      {
        "id": "snap_789",
        "timestamp": "2025-01-15T00:00:00Z",
        "summary": { "addedFiles": 5, "deletedFiles": 0, "addedRows": 1000, "deletedRows": 0 },
        "manifestList": "s3://bucket/manifests/snap_789.avro"
      }
    ],
    "properties": {}
  }
}
```

#### Compaction Request

```json
// Request
{
  "id": "5",
  "method": "compact",
  "params": {
    "partition": "date=2025-01-15",
    "config": {
      "targetFileSize": 134217728,
      "minFiles": 5,
      "maxFiles": 100
    }
  }
}

// Response
{
  "id": "5",
  "result": {
    "id": "compact_xyz",
    "partition": "date=2025-01-15",
    "status": "pending",
    "inputFiles": ["file_001.parquet", "file_002.parquet", "file_003.parquet"]
  }
}
```

### Connection Events

The client emits events for connection state changes:

```typescript
const client = createLakeClient({ url: 'https://lake.example.com' });

// Connection established
client.on('connected', () => {
  console.log('WebSocket connected');
});

// Connection lost
client.on('disconnected', () => {
  console.log('WebSocket disconnected');
});

// Reconnection in progress
client.on('reconnecting', (event) => {
  console.log(`Reconnecting, attempt ${event.attempt} in ${event.delay}ms`);
});

// Successfully reconnected
client.on('reconnected', (event) => {
  console.log(`Reconnected after ${event.attempts} attempts`);
});

// Error occurred
client.on('error', (error) => {
  console.error('WebSocket error:', error);
});
```

### Error Codes

Common error codes returned in `LakeRPCError`:

| Code | Description |
|------|-------------|
| `TABLE_NOT_FOUND` | The requested table does not exist |
| `PARTITION_NOT_FOUND` | The requested partition does not exist |
| `INVALID_SQL` | SQL syntax error or invalid query |
| `QUERY_TIMEOUT` | Query execution timed out |
| `UNAUTHORIZED` | Authentication failed or token expired |
| `CONNECTION_ERROR` | General connection failure |
| `CONNECTION_CLOSED` | WebSocket connection was closed unexpectedly |
| `CONNECTION_TIMEOUT` | Connection attempt timed out |
| `SEQUENCE_GAP` | Gap detected in CDC event sequence |

### Implementing a Custom Client

To implement a compatible client:

1. **Establish WebSocket connection** to the server URL (convert `https://` to `wss://`)
2. **Generate unique request IDs** for each RPC call (incrementing integers work well)
3. **Send JSON-encoded requests** with `id`, `method`, and `params`
4. **Match responses by ID** using the `id` field
5. **Handle CDC batches** separately (messages with `type: "cdc_batch"`)
6. **Implement timeout handling** for requests that don't receive responses
7. **Handle connection drops** with automatic reconnection

```typescript
// Minimal custom client example (pseudocode)
class MinimalLakeClient {
  private ws: WebSocket;
  private requestId = 0;
  private pending = new Map<string, { resolve, reject }>();

  async connect(url: string) {
    const wsUrl = url.replace(/^http/, 'ws');
    this.ws = new WebSocket(wsUrl);

    this.ws.onmessage = (event) => {
      const msg = JSON.parse(event.data);

      // Handle CDC batch
      if (msg.type === 'cdc_batch') {
        this.handleCDCBatch(msg);
        return;
      }

      // Handle RPC response
      const pending = this.pending.get(msg.id);
      if (pending) {
        this.pending.delete(msg.id);
        if (msg.error) {
          pending.reject(new Error(msg.error.message));
        } else {
          pending.resolve(msg.result);
        }
      }
    };
  }

  async rpc(method: string, params: unknown): Promise<unknown> {
    const id = String(++this.requestId);
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.ws.send(JSON.stringify({ id, method, params }));
    });
  }

  async query(sql: string) {
    return this.rpc('query', { sql });
  }
}
```

## License

MIT
