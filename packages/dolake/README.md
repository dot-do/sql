# @dotdo/dolake

> **Pre-release Software**: This is v0.1.0. APIs may change. Not recommended for production use without thorough testing.

DoLake is a high-performance Lakehouse Durable Object that receives CDC (Change Data Capture) events from DoSQL instances via WebSocket, batches them intelligently, and writes them as Parquet files to R2 with full Iceberg metadata support.

## Stability

### Stable APIs

- Core CDC event ingestion via WebSocket
- Parquet file writing to R2
- Basic Iceberg metadata support
- Rate limiting and backpressure signaling
- Health check and status endpoints

### Experimental APIs

- REST Catalog API (Iceberg spec - API may change)
- Automatic compaction
- Partition management and pruning
- Query routing and execution
- Zod schema validation (internal types may change)
- Binary protocol support

## Version Compatibility

| Dependency | Version |
|------------|---------|
| Node.js | 18+ |
| TypeScript | 5.3+ |
| Cloudflare Workers | 2024-01-01+ |
| @cloudflare/workers-types | 4.x |

## Features

- **WebSocket Hibernation**: 95% cost reduction on idle connections
- **CDC Streaming**: Real-time change data capture from multiple DoSQL shards
- **Parquet Writing**: Efficient columnar storage with configurable compression (snappy, gzip, zstd)
- **Iceberg Metadata**: Full Iceberg table format support (snapshots, manifests, schema evolution)
- **REST Catalog API**: Standard Iceberg REST Catalog for external query engines
- **Rate Limiting**: Token bucket algorithm with backpressure signaling
- **Deduplication**: Configurable deduplication window for exactly-once semantics
- **Automatic Compaction**: Background compaction of small files
- **Partition Management**: Time-based, identity, and bucket partitioning
- **Query Engine**: Built-in query routing and partition pruning
- **Zod Validation**: Runtime type validation for all incoming messages

## Architecture

```
+-------------------------------------------------------------------------+
|                       DoSQL Instances (Shards)                          |
|  +---------+  +---------+  +---------+  +---------+  +---------+        |
|  | Shard 1 |  | Shard 2 |  | Shard 3 |  | Shard N |  |   ...   |        |
|  |  (CDC)  |  |  (CDC)  |  |  (CDC)  |  |  (CDC)  |  |         |        |
|  +----+----+  +----+----+  +----+----+  +----+----+  +----+----+        |
|       |            |            |            |            |             |
|       |   WebSocket Hibernation (95% cost discount)      |             |
|       v            v            v            v            v             |
|  +------------------------------------------------------------------+   |
|  |                    DoLake (Aggregator DO)                        |   |
|  |                                                                  |   |
|  |  +-------------+  +---------------+  +------------------+        |   |
|  |  | Rate Limiter|  | CDC Buffer    |  | Parquet Writer   |        |   |
|  |  | (Token      |  | Manager       |  | (hyparquet)      |        |   |
|  |  |  Bucket)    |  | (Dedup)       |  |                  |        |   |
|  |  +-------------+  +---------------+  +------------------+        |   |
|  |                                                                  |   |
|  |  +-------------+  +---------------+  +------------------+        |   |
|  |  | Partition   |  | Compaction    |  | Query Engine     |        |   |
|  |  | Manager     |  | Manager       |  | (Routing/Pruning)|        |   |
|  |  +-------------+  +---------------+  +------------------+        |   |
|  +------------------------------------------------------------------+   |
|                                |                                        |
|                                v                                        |
|                          +----------+                                   |
|                          |    R2    |                                   |
|                          | (Iceberg)|                                   |
|                          +----------+                                   |
|                                |                                        |
|           +--------------------+--------------------+                   |
|           v                    v                    v                   |
|  +---------------+    +---------------+    +---------------+            |
|  |    Spark      |    |    DuckDB     |    |    Trino      |            |
|  +---------------+    +---------------+    +---------------+            |
+-------------------------------------------------------------------------+
```

### Data Flow

1. **CDC Events**: DoSQL shards emit CDC events on every INSERT, UPDATE, DELETE
2. **WebSocket Transport**: Events stream to DoLake via hibernating WebSocket connections
3. **Buffering**: Events are batched by table and partition with deduplication
4. **Flush Triggers**: Automatic flush on event count, byte size, or time threshold
5. **Parquet Writing**: Batched events are converted to Parquet format
6. **Iceberg Metadata**: Snapshots and manifests are updated atomically
7. **Query Access**: External engines query via REST Catalog or direct R2 access

## Installation

```bash
npm install @dotdo/dolake
```

## Quick Start

### 1. Configure wrangler.toml / wrangler.jsonc

```jsonc
// wrangler.jsonc
{
  "name": "my-lakehouse",
  "main": "src/worker.ts",
  "compatibility_date": "2024-01-01",
  "durable_objects": {
    "bindings": [
      {
        "name": "DOLAKE",
        "class_name": "DoLake"
      }
    ]
  },
  "r2_buckets": [
    {
      "binding": "LAKEHOUSE_BUCKET",
      "bucket_name": "my-lakehouse-data"
    }
  ],
  "migrations": [
    { "tag": "v1", "new_classes": ["DoLake"] }
  ]
}
```

### 2. Export the Durable Object

```typescript
// src/worker.ts
import { DoLake } from '@dotdo/dolake';

export { DoLake };

interface Env {
  DOLAKE: DurableObjectNamespace;
  LAKEHOUSE_BUCKET: R2Bucket;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // Route requests to DoLake
    if (url.pathname.startsWith('/lakehouse')) {
      const id = env.DOLAKE.idFromName('default');
      const stub = env.DOLAKE.get(id);
      return stub.fetch(request);
    }

    return new Response('Not Found', { status: 404 });
  },
};
```

### 3. Connect from DoSQL (CDC Producer)

```typescript
// In DoSQL or any CDC producer
const ws = new WebSocket('https://my-lakehouse.workers.dev/lakehouse', {
  headers: {
    'Upgrade': 'websocket',
    'X-Client-ID': 'dosql-shard-1',
    'X-Shard-Name': 'users-shard-1',
  },
});

ws.onopen = () => {
  // Send connect message
  ws.send(JSON.stringify({
    type: 'connect',
    timestamp: Date.now(),
    sourceDoId: 'dosql-shard-1',
    sourceShardName: 'users-shard-1',
    lastAckSequence: 0,
    protocolVersion: 1,
    capabilities: {
      binaryProtocol: false,
      compression: false,
      batching: true,
      maxBatchSize: 1000,
      maxMessageSize: 4194304,
    },
  }));
};

// Send CDC events
ws.send(JSON.stringify({
  type: 'cdc_batch',
  timestamp: Date.now(),
  sourceDoId: 'dosql-shard-1',
  events: [
    {
      sequence: 1,
      timestamp: Date.now(),
      operation: 'INSERT',
      table: 'users',
      rowId: 'user-123',
      after: { id: 'user-123', name: 'Alice', email: 'alice@example.com' },
    },
  ],
  sequenceNumber: 1,
  firstEventSequence: 1,
  lastEventSequence: 1,
  sizeBytes: 256,
  isRetry: false,
  retryCount: 0,
}));

// Handle acknowledgments
ws.onmessage = (event) => {
  const msg = JSON.parse(event.data);
  if (msg.type === 'ack') {
    console.log(`Batch ${msg.sequenceNumber} acknowledged: ${msg.status}`);
    // Check for backpressure
    if (msg.details?.suggestedDelayMs) {
      await new Promise(r => setTimeout(r, msg.details.suggestedDelayMs));
    }
  } else if (msg.type === 'nack') {
    console.error(`Batch ${msg.sequenceNumber} rejected: ${msg.reason}`);
    if (msg.shouldRetry) {
      // Retry after delay
      await new Promise(r => setTimeout(r, msg.retryDelayMs || 1000));
    }
  }
};
```

### 4. Query with External Engines

**DuckDB:**
```sql
INSTALL iceberg;
LOAD iceberg;

SELECT * FROM iceberg_scan(
  'r2://my-lakehouse/warehouse/default/users',
  allow_moved_paths = true
);
```

**Spark:**
```python
spark = SparkSession.builder \
    .config("spark.sql.catalog.dolake", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.dolake.type", "rest") \
    .config("spark.sql.catalog.dolake.uri", "https://your-worker.workers.dev/lakehouse/v1") \
    .getOrCreate()

df = spark.table("dolake.default.users")
```

**Trino:**
```sql
-- In catalog configuration
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://your-worker.workers.dev/lakehouse/v1

-- Query
SELECT * FROM dolake.default.users WHERE created_at > DATE '2024-01-01';
```

## API Reference

### DoLake Class

The main Durable Object class that handles CDC ingestion and lakehouse operations.

```typescript
import { DoLake, type DoLakeEnv } from '@dotdo/dolake';

export { DoLake };
```

#### Environment Bindings

```typescript
interface DoLakeEnv {
  /** R2 bucket for lakehouse data */
  LAKEHOUSE_BUCKET: R2Bucket;

  /** Optional KV for metadata caching */
  LAKEHOUSE_KV?: KVNamespace;
}
```

### HTTP Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/status` | Current DoLake status and buffer stats |
| GET | `/metrics` | Rate limiting and buffer metrics |
| POST | `/flush` | Trigger manual flush to R2 |
| PATCH | `/v1/config` | Update configuration at runtime |
| POST | `/v1/query` | Execute query |
| POST | `/v1/query/plan` | Get query execution plan |
| POST | `/v1/query/route` | Route query to partitions |
| GET | `/v1/scaling/status` | Scaling and memory status |
| GET | `/v1/memory-stats` | Memory usage statistics |
| GET | `/v1/write-stats` | Parquet write statistics |

### REST Catalog API (Iceberg Spec)

DoLake implements the standard Iceberg REST Catalog API:

| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/namespaces` | List namespaces |
| POST | `/v1/namespaces` | Create namespace |
| GET | `/v1/namespaces/{ns}` | Get namespace metadata |
| DELETE | `/v1/namespaces/{ns}` | Delete namespace |
| PATCH | `/v1/namespaces/{ns}/properties` | Update namespace properties |
| GET | `/v1/namespaces/{ns}/tables` | List tables in namespace |
| POST | `/v1/namespaces/{ns}/tables` | Create table |
| GET | `/v1/namespaces/{ns}/tables/{table}` | Load table metadata |
| DELETE | `/v1/namespaces/{ns}/tables/{table}` | Drop table |
| POST | `/v1/namespaces/{ns}/tables/{table}/commits` | Commit table changes |
| GET | `/v1/namespaces/{ns}/tables/{table}/partitions` | List partitions |
| GET | `/v1/namespaces/{ns}/tables/{table}/partition-stats` | Get partition statistics |

### Compaction API

| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/compaction/candidates` | List compaction candidates |
| POST | `/v1/compaction/run` | Trigger compaction |
| GET | `/v1/compaction/metrics` | Get compaction metrics |
| POST | `/v1/compaction/auto` | Run auto-compaction |

### Configuration

```typescript
interface DoLakeConfig {
  /** R2 bucket binding name */
  r2BucketName: string;                    // Default: 'lakehouse-data'

  /** Base path in R2 for Iceberg tables */
  r2BasePath: string;                      // Default: 'warehouse'

  /** Maximum events before flush */
  flushThresholdEvents: number;            // Default: 10000

  /** Maximum size in bytes before flush */
  flushThresholdBytes: number;             // Default: 32MB (33554432)

  /** Maximum age of buffer before flush (ms) */
  flushThresholdMs: number;                // Default: 60000 (1 minute)

  /** Interval for scheduled flushes (ms) */
  flushIntervalMs: number;                 // Default: 30000 (30 seconds)

  /** Maximum buffer size in bytes */
  maxBufferSize: number;                   // Default: 128MB (134217728)

  /** Enable local fallback storage */
  enableFallback: boolean;                 // Default: true

  /** Maximum fallback storage size */
  maxFallbackSize: number;                 // Default: 64MB (67108864)

  /** Enable deduplication */
  enableDeduplication: boolean;            // Default: true

  /** Deduplication window in ms */
  deduplicationWindowMs: number;           // Default: 300000 (5 minutes)

  /** Target Parquet row group size */
  parquetRowGroupSize: number;             // Default: 100000

  /** Parquet compression algorithm */
  parquetCompression: 'none' | 'snappy' | 'gzip' | 'zstd';  // Default: 'snappy'
}

// Access defaults
import { DEFAULT_DOLAKE_CONFIG } from '@dotdo/dolake';
```

## CDC Streaming

### WebSocket Message Types (Client to Server)

#### Connect Message

Sent when establishing a new connection:

```typescript
interface ConnectMessage {
  type: 'connect';
  timestamp: number;
  correlationId?: string;
  sourceDoId: string;
  sourceShardName?: string;
  lastAckSequence: number;      // For resumption after disconnect
  protocolVersion: number;       // Currently: 1
  capabilities: {
    binaryProtocol: boolean;
    compression: boolean;
    batching: boolean;
    maxBatchSize: number;
    maxMessageSize: number;
  };
}
```

#### CDC Batch Message

Sent to stream CDC events:

```typescript
interface CDCBatchMessage {
  type: 'cdc_batch';
  timestamp: number;
  correlationId?: string;
  sourceDoId: string;
  sourceShardName?: string;
  events: CDCEvent[];
  sequenceNumber: number;        // Batch sequence (monotonically increasing)
  firstEventSequence: number;    // First event's sequence in batch
  lastEventSequence: number;     // Last event's sequence in batch
  sizeBytes: number;             // Approximate batch size
  isRetry: boolean;
  retryCount: number;
}
```

#### Heartbeat Message

Sent periodically to keep connection alive:

```typescript
interface HeartbeatMessage {
  type: 'heartbeat';
  timestamp: number;
  sourceDoId: string;
  lastAckSequence: number;
  pendingEvents: number;
}
```

#### Flush Request Message

Request immediate flush:

```typescript
interface FlushRequestMessage {
  type: 'flush_request';
  timestamp: number;
  sourceDoId: string;
  reason: 'manual' | 'shutdown' | 'buffer_full' | 'time_threshold';
}
```

### WebSocket Message Types (Server to Client)

#### Ack Message

Acknowledgment of successful batch processing:

```typescript
interface AckMessage {
  type: 'ack';
  timestamp: number;
  correlationId?: string;
  sequenceNumber: number;
  status: 'ok' | 'buffered' | 'persisted' | 'duplicate' | 'fallback';
  batchId?: string;
  details?: {
    eventsProcessed: number;
    bufferUtilization: number;      // 0.0 - 1.0
    timeUntilFlush?: number;        // ms until next scheduled flush
    persistedPath?: string;         // R2 path if persisted
    remainingTokens?: number;       // Rate limit tokens remaining
    bucketCapacity?: number;        // Rate limit bucket capacity
    suggestedDelayMs?: number;      // Backpressure signal
    circuitBreakerState?: 'closed' | 'open' | 'half-open';
  };
  rateLimit?: {
    limit: number;
    remaining: number;
    resetAt: number;
  };
}
```

#### Nack Message

Negative acknowledgment (rejection):

```typescript
interface NackMessage {
  type: 'nack';
  timestamp: number;
  correlationId?: string;
  sequenceNumber: number;
  reason: NackReason;
  errorMessage: string;
  shouldRetry: boolean;
  retryDelayMs?: number;
  maxSize?: number;                 // For size violations
}

type NackReason =
  | 'buffer_full'           // Buffer at capacity
  | 'rate_limited'          // Rate limit exceeded
  | 'invalid_sequence'      // Sequence number issue
  | 'invalid_format'        // Message validation failed
  | 'internal_error'        // Server error
  | 'shutting_down'         // Server shutting down
  | 'payload_too_large'     // Payload exceeds limit
  | 'event_too_large'       // Single event too large
  | 'load_shedding'         // Under heavy load
  | 'connection_limit'      // Max connections per source
  | 'ip_limit';             // Max connections per IP
```

#### Status Message

Current server status:

```typescript
interface StatusMessage {
  type: 'status';
  timestamp: number;
  state: 'idle' | 'receiving' | 'flushing' | 'recovering' | 'error';
  buffer: {
    batchCount: number;
    eventCount: number;
    totalSizeBytes: number;
    utilization: number;
    oldestBatchTime?: number;
    newestBatchTime?: number;
  };
  connectedSources: number;
  lastFlushTime?: number;
  nextFlushTime?: number;
}
```

### CDC Event Format

```typescript
interface CDCEvent {
  /** Monotonically increasing sequence number */
  sequence: number;

  /** Unix timestamp in milliseconds */
  timestamp: number;

  /** Type of database operation */
  operation: 'INSERT' | 'UPDATE' | 'DELETE';

  /** Table name where the change occurred */
  table: string;

  /** Primary key or row identifier */
  rowId: string;

  /** Row data before the change (for UPDATE/DELETE) */
  before?: unknown;

  /** Row data after the change (for INSERT/UPDATE) */
  after?: unknown;

  /** Optional metadata */
  metadata?: Record<string, unknown>;
}
```

### Backpressure Handling

When buffer utilization exceeds the backpressure threshold (default 80%), the `suggestedDelayMs` field in ack messages indicates how long clients should wait:

```typescript
// Client-side backpressure handling
ws.onmessage = (event) => {
  const msg = JSON.parse(event.data);

  if (msg.type === 'ack' && msg.details?.suggestedDelayMs) {
    // Slow down sending
    await new Promise(r => setTimeout(r, msg.details.suggestedDelayMs));
  }

  if (msg.type === 'ack' && msg.details?.bufferUtilization > 0.7) {
    // Proactively reduce batch size
    batchSize = Math.max(100, batchSize * 0.8);
  }
};
```

## Rate Limiting

DoLake implements sophisticated rate limiting to protect against abuse and ensure fair resource usage.

### Configuration

```typescript
interface RateLimitConfig {
  /** Maximum new connections per second */
  connectionsPerSecond: number;            // Default: 20

  /** Maximum messages per connection per second */
  messagesPerSecond: number;               // Default: 100

  /** Token bucket capacity for burst handling */
  burstCapacity: number;                   // Default: 50

  /** Token refill rate per second */
  refillRate: number;                      // Default: 10

  /** Maximum payload size in bytes */
  maxPayloadSize: number;                  // Default: 4MB (4194304)

  /** Maximum individual event size in bytes */
  maxEventSize: number;                    // Default: 1MB (1048576)

  /** Maximum connections per source/client ID */
  maxConnectionsPerSource: number;         // Default: 5

  /** Maximum connections per IP */
  maxConnectionsPerIp: number;             // Default: 30

  /** Subnet-level rate limiting threshold (/24) */
  subnetRateLimitThreshold: number;        // Default: 100

  /** Whitelisted IPs (no rate limiting) */
  whitelistedIps: string[];                // Default: private ranges

  /** Buffer utilization threshold for backpressure */
  backpressureThreshold: number;           // Default: 0.8

  /** Base retry delay for exponential backoff (ms) */
  baseRetryDelayMs: number;                // Default: 100

  /** Max retry delay for exponential backoff (ms) */
  maxRetryDelayMs: number;                 // Default: 30000
}

// Access defaults
import { DEFAULT_RATE_LIMIT_CONFIG } from '@dotdo/dolake';
```

### Response Headers

Rate limit information is included in HTTP responses:

```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 45
X-RateLimit-Reset: 1704067200
Retry-After: 5
```

### Best Practices

1. **Respect Retry-After**: When receiving a 429 or nack with `retryDelayMs`, wait the specified time
2. **Monitor Buffer Utilization**: Reduce send rate when `bufferUtilization` exceeds 0.7
3. **Use Correlation IDs**: Include `correlationId` to track request/response pairs
4. **Handle Backpressure**: Implement backpressure handling based on `suggestedDelayMs`
5. **Batch Efficiently**: Aim for 100-1000 events per batch for optimal throughput
6. **Implement Exponential Backoff**: Double delay on consecutive rate limits

## Partitioning

DoLake supports multiple partitioning strategies:

```typescript
import {
  createDayPartitionSpec,
  createHourPartitionSpec,
  createBucketPartitionSpec,
  createCompositePartitionSpec,
} from '@dotdo/dolake';

// Day partitioning (e.g., dt=2024-01-15)
const daySpec = createDayPartitionSpec('timestamp', 1000);

// Hour partitioning (e.g., dt=2024-01-15/hr=14)
const hourSpec = createHourPartitionSpec('timestamp', 1001);

// Bucket partitioning (e.g., bucket=7)
const bucketSpec = createBucketPartitionSpec('user_id', 16, 1002);

// Composite partitioning
const compositeSpec = createCompositePartitionSpec([
  { type: 'day', sourceField: 'timestamp' },
  { type: 'bucket', sourceField: 'region', numBuckets: 4 },
]);
```

### Partition Pruning

Queries automatically prune partitions:

```typescript
const result = await fetch('/v1/query/plan', {
  method: 'POST',
  body: JSON.stringify({
    table: { namespace: ['default'], name: 'events' },
    columns: ['*'],
    where: "timestamp >= '2024-01-01' AND timestamp < '2024-02-01'",
  }),
});
// Only scans partitions dt=2024-01-01 through dt=2024-01-31
```

## Compaction

DoLake automatically compacts small Parquet files:

```typescript
interface CompactionConfig {
  /** Minimum files before compaction triggers */
  minFilesToCompact: number;               // Default: 5

  /** Maximum files to compact in one operation */
  maxFilesToCompact: number;               // Default: 100

  /** Target file size after compaction */
  targetFileSizeBytes: number;             // Default: 128MB

  /** Enable automatic compaction */
  autoCompact: boolean;                    // Default: true

  /** Compaction check interval (ms) */
  compactionIntervalMs: number;            // Default: 3600000 (1 hour)
}

// Trigger manual compaction
await fetch('/v1/compaction/run', {
  method: 'POST',
  body: JSON.stringify({
    namespace: 'default',
    table: 'users',
    partitions: ['dt=2024-01-15'],
  }),
});
```

## Zod Schema Validation

All messages are validated using Zod:

```typescript
import {
  validateClientMessage,
  validateCDCBatchMessage,
  MessageValidationError,
  CDCBatchMessageSchema,
} from '@dotdo/dolake';

// Validate any client message
try {
  const message = validateClientMessage(rawData);
  // message is typed based on discriminated union
} catch (error) {
  if (error instanceof MessageValidationError) {
    console.error('Validation failed:', error.getErrorDetails());
  }
}

// Validate specific message type
const batchMsg = validateCDCBatchMessage(data);

// Use schema directly
const result = CDCBatchMessageSchema.safeParse(data);
if (!result.success) {
  console.error(result.error.issues);
}
```

## Error Handling

DoLake provides typed error classes:

```typescript
import {
  DoLakeError,
  ConnectionError,
  BufferOverflowError,
  FlushError,
  ParquetWriteError,
  IcebergError,
} from '@dotdo/dolake';

try {
  // ... operation
} catch (error) {
  if (error instanceof BufferOverflowError) {
    // Buffer full - implement backpressure
    // error.retryable === true
  } else if (error instanceof FlushError) {
    // Check if fallback storage was used
    if (error.usedFallback) {
      console.warn('R2 write failed, using fallback storage');
    }
  } else if (error instanceof IcebergError) {
    // Metadata error - not retryable
    // error.retryable === false
  }
}
```

## Exports

```typescript
// Main class
export { DoLake, type DoLakeEnv } from '@dotdo/dolake';

// Types
export {
  type CDCEvent,
  type CDCBatchMessage,
  type ConnectMessage,
  type AckMessage,
  type NackMessage,
  type DoLakeConfig,
  type DoLakeState,
  type FlushResult,
  type BufferStats,
  // ... and many more
} from '@dotdo/dolake';

// Rate limiting
export {
  RateLimiter,
  type RateLimitConfig,
  type RateLimitResult,
  DEFAULT_RATE_LIMIT_CONFIG,
} from '@dotdo/dolake';

// Schemas
export {
  CDCBatchMessageSchema,
  ConnectMessageSchema,
  validateClientMessage,
  MessageValidationError,
} from '@dotdo/dolake';

// Iceberg utilities
export {
  R2IcebergStorage,
  createTableMetadata,
  createAppendSnapshot,
  createDayPartitionSpec,
} from '@dotdo/dolake';

// Compaction
export {
  CompactionManager,
  type CompactionConfig,
  DEFAULT_COMPACTION_CONFIG,
} from '@dotdo/dolake';
```

## Related Packages

- **[@dotdo/lake.do](https://www.npmjs.com/package/@dotdo/lake.do)** - Client SDK for connecting to DoLake
- **[@dotdo/sql.do](https://www.npmjs.com/package/@dotdo/sql.do)** - DoSQL with built-in CDC streaming
- **[@dotdo/shared-types](https://www.npmjs.com/package/@dotdo/shared-types)** - Shared type definitions

## Documentation

- [Architecture](./docs/ARCHITECTURE.md) - System design and data flow
- [DoSQL Integration](./docs/INTEGRATION.md) - How to connect DoSQL to DoLake
- [REST API Reference](./docs/API.md) - Complete Iceberg REST Catalog API

## Requirements

- Cloudflare Workers with Durable Objects
- R2 bucket for lakehouse storage
- TypeScript 5.3+

## License

MIT
