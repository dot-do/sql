> **Developer Preview** - This package is under active development. APIs may change. Not recommended for production use.

# dolake

DoLake is a high-performance Lakehouse Durable Object that receives CDC (Change Data Capture) events from DoSQL instances via WebSocket, batches them intelligently, and writes them as Parquet files to R2 with full Iceberg metadata support.

## Status

| Property | Value |
|----------|-------|
| Current version | 0.1.0 |
| Stability | Experimental |
| Breaking changes | Expected before 1.0 |

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
|       |   WebSocket Hibernation (95% cost discount)       |             |
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

### State Machine

DoLake uses a state machine to manage its lifecycle and ensure safe concurrent operations. The state machine validates all transitions and prevents invalid state changes.

#### States

| State | Description |
|-------|-------------|
| `idle` | No active operations. Ready to receive events or perform maintenance. |
| `receiving` | Actively receiving CDC events from connected DoSQL shards. |
| `flushing` | Writing buffered events to R2 as Parquet files. |
| `recovering` | Recovering from fallback storage after a failed flush. |
| `error` | An unrecoverable error occurred (terminal state, not shown in transitions). |

#### State Transition Diagram

```
                              ┌─────────────────────────────────────┐
                              │                                     │
                              ▼                                     │
                        ┌──────────┐                                │
           ┌───────────▶│   idle   │◀───────────┐                   │
           │            └──────────┘            │                   │
           │                 │                  │                   │
           │                 │ CDC event        │ flush complete    │
           │                 │ received         │                   │
           │                 ▼                  │                   │
           │           ┌───────────┐            │                   │
           │           │ receiving │────────────┘                   │
           │           └───────────┘                                │
           │                 │                                      │
           │                 │ threshold reached                    │
           │                 │ (events/size/time)                   │
           │                 ▼                                      │
           │           ┌───────────┐                                │
           └───────────│ flushing  │────────────────────────────────┘
                       └───────────┘        can receive during flush
                              │
                              │ flush failed,
                              │ fallback triggered
                              ▼
                       ┌───────────┐
                       │recovering │───────────▶ (back to idle/receiving)
                       └───────────┘
```

#### Valid Transitions

| From | To | Trigger |
|------|----|---------|
| `idle` | `receiving` | First CDC event arrives |
| `idle` | `flushing` | Manual flush requested or scheduled alarm fires |
| `idle` | `recovering` | Fallback events detected on startup |
| `receiving` | `idle` | Buffer empty, no pending events |
| `receiving` | `flushing` | Threshold reached (events, bytes, or time) |
| `flushing` | `idle` | Flush complete, buffer empty |
| `flushing` | `receiving` | Flush complete, new events arrived during flush |
| `recovering` | `idle` | Recovery complete, no pending events |
| `recovering` | `receiving` | Recovery complete, new events arrived |

#### State Machine API

```typescript
import { DoLakeStateMachine, type StateChangeEvent } from 'dolake';

const stateMachine = new DoLakeStateMachine({
  flushIntervalMs: 60_000,
  enableFallback: true,
});

// Get current state
const state = stateMachine.getState(); // 'idle' | 'receiving' | 'flushing' | 'recovering'

// Check if transition is valid before attempting
if (stateMachine.canTransitionTo('flushing')) {
  stateMachine.setState('flushing', 'threshold_events');
}

// Listen for state changes
stateMachine.addListener((event: StateChangeEvent) => {
  console.log(`State: ${event.previousState} -> ${event.currentState}`);
  console.log(`Trigger: ${event.trigger}`);
});

// Query helpers
stateMachine.isIdle();      // true when state === 'idle'
stateMachine.isActive();    // true when receiving or flushing
stateMachine.isRecovering(); // true when state === 'recovering'

// Force state for recovery scenarios (bypasses validation)
stateMachine.forceState('idle', 'recovery_failed');

// Get transition history
const history = stateMachine.getHistory();
```

## Installation

```bash
npm install dolake
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
import { DoLake } from 'dolake';

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

ws.onopen = (): void => {
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
ws.onmessage = (event: MessageEvent): void => {
  const msg: AckMessage | NackMessage = JSON.parse(event.data as string);
  if (msg.type === 'ack') {
    console.log(`Batch ${msg.sequenceNumber} acknowledged: ${msg.status}`);
    // Check for backpressure
    if (msg.details?.suggestedDelayMs) {
      setTimeout(() => {
        // Resume sending after delay
      }, msg.details.suggestedDelayMs);
    }
  } else if (msg.type === 'nack') {
    console.error(`Batch ${msg.sequenceNumber} rejected: ${msg.reason}`);
    if (msg.shouldRetry) {
      // Retry after delay
      setTimeout(() => {
        // Retry the batch
      }, msg.retryDelayMs || 1000);
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
import { DoLake, type DoLakeEnv } from 'dolake';

export { DoLake };
```

#### Environment Bindings

DoLake requires specific Cloudflare bindings to function. These are configured in your `wrangler.toml` or `wrangler.jsonc` file.

```typescript
interface DoLakeEnv {
  /** R2 bucket for lakehouse data */
  LAKEHOUSE_BUCKET: R2Bucket;

  /** Optional KV for metadata caching */
  LAKEHOUSE_KV?: KVNamespace;
}
```

##### LAKEHOUSE_BUCKET (Required)

The `LAKEHOUSE_BUCKET` binding connects DoLake to an R2 bucket where all lakehouse data is stored:

- **Parquet data files**: CDC events are batched and written as columnar Parquet files
- **Iceberg metadata**: Table metadata, snapshots, and manifest files following the Iceberg spec
- **Compacted files**: Merged Parquet files from the compaction process

**Configuration:**

```toml
# wrangler.toml
[[r2_buckets]]
binding = "LAKEHOUSE_BUCKET"
bucket_name = "my-lakehouse-data"
```

```jsonc
// wrangler.jsonc
{
  "r2_buckets": [
    {
      "binding": "LAKEHOUSE_BUCKET",
      "bucket_name": "my-lakehouse-data"
    }
  ]
}
```

**R2 Bucket Setup:**

1. Create the R2 bucket in your Cloudflare dashboard or via Wrangler:
   ```bash
   wrangler r2 bucket create my-lakehouse-data
   ```

2. The bucket name in your config must match the actual R2 bucket name.

3. DoLake automatically creates the directory structure:
   ```
   warehouse/
   ├── {namespace}/
   │   └── {table}/
   │       ├── metadata/
   │       │   ├── v1.metadata.json
   │       │   └── snap-{id}-{uuid}.avro
   │       └── data/
   │           └── {partition}/
   │               └── {file}.parquet
   ```

##### LAKEHOUSE_KV (Optional)

The `LAKEHOUSE_KV` binding provides a KV namespace for caching metadata, improving read performance for frequently accessed table metadata.

**What it caches:**

- Table metadata (schema, partition specs, properties)
- Namespace listings
- Snapshot references
- Partition statistics

**Configuration:**

```toml
# wrangler.toml
[[kv_namespaces]]
binding = "LAKEHOUSE_KV"
id = "your-kv-namespace-id"
```

```jsonc
// wrangler.jsonc
{
  "kv_namespaces": [
    {
      "binding": "LAKEHOUSE_KV",
      "id": "your-kv-namespace-id"
    }
  ]
}
```

**KV Namespace Setup:**

1. Create the KV namespace:
   ```bash
   wrangler kv:namespace create LAKEHOUSE_KV
   ```

2. Copy the returned namespace ID to your config.

3. For preview/development, you can also create a preview namespace:
   ```bash
   wrangler kv:namespace create LAKEHOUSE_KV --preview
   ```

**When to use LAKEHOUSE_KV:**

- High read throughput: Caching reduces R2 reads for metadata
- Multi-region access: KV provides lower latency for global reads
- Cost optimization: KV reads are cheaper than R2 for small metadata files

**When it's not needed:**

- Low traffic/development: R2 metadata reads are sufficient
- Write-heavy workloads: Cache invalidation overhead may not be worth it
- Single-region deployment: R2 latency is acceptable

##### Complete Configuration Example

```jsonc
// wrangler.jsonc - Full DoLake configuration
{
  "name": "my-lakehouse",
  "main": "src/worker.ts",
  "compatibility_date": "2024-01-01",

  // Durable Object binding
  "durable_objects": {
    "bindings": [
      {
        "name": "DOLAKE",
        "class_name": "DoLake"
      }
    ]
  },

  // R2 bucket for lakehouse data (REQUIRED)
  "r2_buckets": [
    {
      "binding": "LAKEHOUSE_BUCKET",
      "bucket_name": "my-lakehouse-data"
    }
  ],

  // KV namespace for metadata caching (OPTIONAL)
  "kv_namespaces": [
    {
      "binding": "LAKEHOUSE_KV",
      "id": "abc123def456..."
    }
  ],

  // DO migrations
  "migrations": [
    { "tag": "v1", "new_classes": ["DoLake"] }
  ]
}
```

```typescript
// src/worker.ts - Type-safe environment interface
import { DoLake, type DoLakeEnv } from 'dolake';

export { DoLake };

interface Env extends DoLakeEnv {
  DOLAKE: DurableObjectNamespace;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const id = env.DOLAKE.idFromName('default');
    const stub = env.DOLAKE.get(id);
    return stub.fetch(request);
  },
};
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

#### /metrics Endpoint - Prometheus Format

The `/metrics` endpoint returns metrics in [Prometheus exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/) (`text/plain`). These metrics can be scraped by Prometheus, Grafana Agent, or any compatible monitoring system.

**Example Response:**

```prometheus
# HELP dolake_buffer_events Number of events in buffer
# TYPE dolake_buffer_events gauge
dolake_buffer_events 1250

# HELP dolake_buffer_bytes Buffer size in bytes
# TYPE dolake_buffer_bytes gauge
dolake_buffer_bytes 524288

# HELP dolake_buffer_utilization Buffer utilization ratio
# TYPE dolake_buffer_utilization gauge
dolake_buffer_utilization 0.25

# HELP dolake_connected_sources Number of connected source DOs
# TYPE dolake_connected_sources gauge
dolake_connected_sources 3

# HELP dolake_dedup_checks Total deduplication checks
# TYPE dolake_dedup_checks counter
dolake_dedup_checks 15000

# HELP dolake_dedup_duplicates Total duplicates found
# TYPE dolake_dedup_duplicates counter
dolake_dedup_duplicates 42

# HELP dolake_rate_limited_total Total rate limited requests
# TYPE dolake_rate_limited_total counter
dolake_rate_limited_total 5

# HELP dolake_rate_limit_violations Total rate limit violations
# TYPE dolake_rate_limit_violations counter
dolake_rate_limit_violations 2

# HELP dolake_connections_rate_limited Connections rate limited
# TYPE dolake_connections_rate_limited counter
dolake_connections_rate_limited 1

# HELP dolake_messages_rate_limited Messages rate limited
# TYPE dolake_messages_rate_limited counter
dolake_messages_rate_limited 4

# HELP dolake_payload_rejections Payload size rejections
# TYPE dolake_payload_rejections counter
dolake_payload_rejections 0

# HELP dolake_preparse_size_rejections Pre-parse size rejections (before JSON.parse)
# TYPE dolake_preparse_size_rejections counter
dolake_preparse_size_rejections 0

# HELP dolake_event_size_rejections Event size rejections
# TYPE dolake_event_size_rejections counter
dolake_event_size_rejections 2

# HELP dolake_connections_closed_violations Connections closed for violations
# TYPE dolake_connections_closed_violations counter
dolake_connections_closed_violations 0

# HELP dolake_ip_rejections IP-based rejections
# TYPE dolake_ip_rejections counter
dolake_ip_rejections 1

# HELP dolake_load_shedding_events Load shedding events
# TYPE dolake_load_shedding_events counter
dolake_load_shedding_events 0

# HELP dolake_active_connections Currently active connections
# TYPE dolake_active_connections gauge
dolake_active_connections 3

# HELP dolake_peak_connections Peak connections
# TYPE dolake_peak_connections gauge
dolake_peak_connections 5
```

**Available Metrics:**

| Metric | Type | Description |
|--------|------|-------------|
| `dolake_buffer_events` | gauge | Number of CDC events currently in buffer |
| `dolake_buffer_bytes` | gauge | Current buffer size in bytes |
| `dolake_buffer_utilization` | gauge | Buffer utilization ratio (0.0 - 1.0) |
| `dolake_connected_sources` | gauge | Number of connected DoSQL source instances |
| `dolake_dedup_checks` | counter | Total deduplication checks performed |
| `dolake_dedup_duplicates` | counter | Total duplicate events detected and rejected |
| `dolake_rate_limited_total` | counter | Total requests rate limited (connections + messages) |
| `dolake_rate_limit_violations` | counter | Total rate limit violations (payload + event size) |
| `dolake_connections_rate_limited` | counter | Connections rejected due to rate limiting |
| `dolake_messages_rate_limited` | counter | Messages rejected due to rate limiting |
| `dolake_payload_rejections` | counter | Payloads rejected for exceeding size limit |
| `dolake_preparse_size_rejections` | counter | Pre-parse size rejections (before JSON.parse) |
| `dolake_event_size_rejections` | counter | Individual events rejected for exceeding size limit |
| `dolake_connections_closed_violations` | counter | Connections closed due to repeated violations |
| `dolake_ip_rejections` | counter | Requests rejected based on IP-based rate limiting |
| `dolake_load_shedding_events` | counter | Events dropped during load shedding |
| `dolake_active_connections` | gauge | Currently active WebSocket connections |
| `dolake_peak_connections` | gauge | Peak number of concurrent connections |

**Usage with Prometheus:**

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'dolake'
    scrape_interval: 15s
    static_configs:
      - targets: ['your-worker.workers.dev']
    metrics_path: '/lakehouse/metrics'
```

**Usage with Grafana Agent:**

```yaml
# agent.yaml
metrics:
  wal_directory: /tmp/wal
  configs:
    - name: dolake
      scrape_configs:
        - job_name: 'dolake'
          static_configs:
            - targets: ['your-worker.workers.dev']
          metrics_path: '/lakehouse/metrics'
```

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
import { DEFAULT_DOLAKE_CONFIG } from 'dolake';
```

## CDC Streaming

### HTTP-Based CDC Ingestion

For simple integrations or one-off CDC event ingestion, you can use the HTTP POST endpoint instead of WebSocket streaming:

```typescript
// POST /cdc - Write CDC events via HTTP
const response = await fetch('https://your-worker.workers.dev/cdc', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
  },
  body: JSON.stringify({
    lakehouse: 'default',  // Optional, defaults to 'default'
    events: [
      {
        sequence: 1,
        timestamp: Date.now(),
        operation: 'INSERT',
        table: 'users',
        rowId: 'user-123',
        after: { id: 'user-123', name: 'Alice', email: 'alice@example.com' },
      },
      {
        sequence: 2,
        timestamp: Date.now(),
        operation: 'UPDATE',
        table: 'users',
        rowId: 'user-456',
        before: { id: 'user-456', name: 'Bob', email: 'bob@old.com' },
        after: { id: 'user-456', name: 'Bob', email: 'bob@new.com' },
      },
      {
        sequence: 3,
        timestamp: Date.now(),
        operation: 'DELETE',
        table: 'users',
        rowId: 'user-789',
        before: { id: 'user-789', name: 'Charlie', email: 'charlie@example.com' },
      },
    ],
  }),
});

if (response.ok) {
  const result = await response.json();
  console.log('CDC events written:', result);
} else {
  const error = await response.json();
  console.error('Failed to write CDC events:', error);
}
```

**When to use HTTP vs WebSocket:**

| Use Case | Recommended Transport |
|----------|----------------------|
| High-throughput streaming from DoSQL shards | WebSocket (with backpressure) |
| One-off event ingestion | HTTP POST |
| External system integration (webhooks) | HTTP POST |
| Batch imports | HTTP POST or WebSocket |
| Real-time CDC with acknowledgments | WebSocket |
| Simple REST API integration | HTTP POST |

**HTTP CDC Request Format:**

```typescript
interface HTTPCDCRequest {
  /** Target lakehouse name (default: 'default') */
  lakehouse?: string;
  /** CDC events to write */
  events: CDCEvent[];
}
```

**HTTP CDC Response:**

```typescript
// Success response
interface HTTPCDCResponse {
  success: true;
  eventsProcessed: number;
  batchId?: string;
}

// Error response
interface HTTPCDCErrorResponse {
  error: string;
  code: string;
  details?: unknown;
}
```

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
  reason: FlushTrigger;
}

/**
 * Flush trigger reasons
 */
type FlushTrigger =
  | 'threshold_events'    // Event count threshold reached
  | 'threshold_size'      // Byte size threshold reached
  | 'threshold_time'      // Time interval threshold reached
  | 'manual'              // Manually triggered flush
  | 'shutdown'            // Graceful shutdown flush
  | 'memory_pressure';    // Memory pressure triggered flush
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

### NACK Reason Codes

When DoLake rejects a message, it returns a NACK with a `reason` code indicating why the rejection occurred. The following table documents all NACK reason codes, their meanings, and recommended handling strategies:

| Reason Code | Description | Retryable | Recommended Handling |
|-------------|-------------|-----------|---------------------|
| `buffer_full` | The server's internal buffer has reached capacity and cannot accept more events. | Yes | Wait for `retryDelayMs` (typically 1-5 seconds), then retry with exponential backoff. Consider reducing batch sizes. |
| `rate_limited` | The client has exceeded the configured rate limit (messages per second or tokens exhausted). | Yes | Respect the `retryDelayMs` value. Implement exponential backoff. Check `rateLimit.resetAt` in responses for when limits reset. |
| `invalid_sequence` | The batch sequence number is invalid (e.g., out of order, duplicate, or gap detected). | Depends | Check your sequence numbering logic. For duplicates (already processed), no retry needed. For gaps, may need to resync. |
| `invalid_format` | The message failed Zod schema validation (missing required fields, wrong types, etc.). | No | Fix the message format. Check `errorMessage` for specific validation errors. Review the schema requirements. |
| `internal_error` | An unexpected server-side error occurred during processing. | Yes | Retry with exponential backoff. If persistent, check server logs and contact support. |
| `shutting_down` | The server is gracefully shutting down and not accepting new messages. | Yes | Reconnect to a new instance. The client should attempt reconnection with backoff. |
| `payload_too_large` | The entire message payload exceeds `maxPayloadSize` (default: 4MB). | No | Split the batch into smaller chunks. The `maxSize` field in the NACK indicates the limit. |
| `event_too_large` | A single CDC event within the batch exceeds `maxEventSize` (default: 1MB). | No | Reduce the event size. Consider excluding large blob data or compressing payloads externally. |
| `load_shedding` | The server is under heavy load and temporarily rejecting requests to maintain stability. | Yes | Wait for `retryDelayMs` and retry. This is a transient condition during traffic spikes. |
| `connection_limit` | The client has exceeded `maxConnectionsPerSource` (default: 5 connections per sourceDoId). | No | Close existing connections before opening new ones. Review connection management logic. |
| `ip_limit` | The IP address or subnet has exceeded connection limits (`maxConnectionsPerIp` or `subnetRateLimitThreshold`). | No | Reduce connections from this IP/subnet. May indicate distributed client misconfiguration or abuse. |

#### Handling NACK Responses

```typescript
ws.onmessage = (event: MessageEvent): void => {
  const msg = JSON.parse(event.data as string);

  if (msg.type === 'nack') {
    const { reason, shouldRetry, retryDelayMs, errorMessage, maxSize } = msg;

    switch (reason) {
      case 'buffer_full':
      case 'rate_limited':
      case 'load_shedding':
        // Transient errors - retry with backoff
        if (shouldRetry && retryDelayMs) {
          setTimeout(() => retryBatch(msg.sequenceNumber), retryDelayMs);
        }
        break;

      case 'invalid_format':
        // Fix the message - don't retry the same payload
        console.error('Message validation failed:', errorMessage);
        // Log and skip, or fix the payload format
        break;

      case 'payload_too_large':
      case 'event_too_large':
        // Split the batch into smaller chunks
        console.error(`Size limit exceeded. Max allowed: ${maxSize} bytes`);
        splitAndRetryBatch(msg.sequenceNumber, maxSize);
        break;

      case 'connection_limit':
      case 'ip_limit':
        // Connection management issue
        console.error('Connection limit reached:', errorMessage);
        // Review your connection pooling strategy
        break;

      case 'shutting_down':
        // Reconnect to a new instance
        reconnectWithBackoff();
        break;

      case 'internal_error':
        // Retry with longer backoff
        setTimeout(() => retryBatch(msg.sequenceNumber), retryDelayMs || 5000);
        break;

      case 'invalid_sequence':
        // May need sequence resync
        if (shouldRetry) {
          resyncSequences();
        }
        break;
    }
  }
};
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
ws.onmessage = (event: MessageEvent): void => {
  const msg: AckMessage = JSON.parse(event.data as string);

  if (msg.type === 'ack' && msg.details?.suggestedDelayMs) {
    // Slow down sending - schedule next batch after delay
    setTimeout(() => {
      // Resume sending batches
    }, msg.details.suggestedDelayMs);
  }

  if (msg.type === 'ack' && msg.details?.bufferUtilization > 0.7) {
    // Proactively reduce batch size
    batchSize = Math.max(100, batchSize * 0.8);
  }
};
```

## Durability Tiers

DoLake implements a tiered durability system that classifies events based on their criticality and configures appropriate storage and retry behavior. This ensures that critical events like payments are never lost, while allowing more relaxed guarantees for high-volume analytics data.

### Tier Overview

| Tier | Name | Primary Storage | Fallback | Retry Behavior | Use Case |
|------|------|-----------------|----------|----------------|----------|
| P0 | Critical | R2 + KV | DLQ | Unlimited retries | Payments, Stripe webhooks |
| P1 | Important | R2 | KV | 3x exponential backoff | User actions, signups |
| P2 | Standard | R2 | VFS (DO Storage) | 3x then fallback | Analytics, telemetry |
| P3 | Best-effort | R2 | None (drop) | None | Anonymous visits |

### P2 Durability: Analytics Events

The P2 durability tier is specifically designed for **analytics events ingestion**. It provides a balance between durability and performance for high-volume event streams.

#### Configuration

```typescript
import {
  AnalyticsEventHandler,
  P2_DURABILITY_CONFIG,
} from 'dolake';

// P2 durability configuration
const config = {
  primaryStorage: 'r2',        // Cloudflare R2 object storage
  fallbackStorage: 'vfs',      // Durable Object storage (VFS)
  fallbackEnabled: true,       // Enable fallback on R2 failure
  retryAttempts: 3,            // Retry 3x before fallback
  retryDelayMs: 1000,          // 1 second delay between retries
  deduplicationEnabled: true,  // Deduplicate by eventId
  deduplicationWindowMs: 300_000, // 5 minute dedup window
};

const handler = new AnalyticsEventHandler(P2_DURABILITY_CONFIG);
```

#### Write Flow

The P2 durability tier follows this write flow:

```
Analytics Event Batch
        |
        v
+-------------------+
| Validate Batch    |
| - batchId present |
| - events array    |
+--------+----------+
         |
         v
+-------------------+
| Deduplicate       |
| (by eventId,      |
|  5-min window)    |
+--------+----------+
         |
         v
+-------------------+
| Partition by Date |
| year=YYYY/month=  |
| MM/day=DD         |
+--------+----------+
         |
         v
+-------------------+
| Write to R2       |
| (Primary Storage) |
+--------+----------+
         |
    success?
    /      \
   /        \
  v          v
SUCCESS    RETRY (x3)
  |        with 1s delay
  |             |
  |        success?
  |        /      \
  |       /        \
  |      v          v
  |   SUCCESS    FALLBACK
  |      |       to VFS
  |      |          |
  v      v          v
+-------------------+
| Return ACK        |
| usedFallback:     |
| true/false        |
+-------------------+
```

#### Client-Side Batching

The `AnalyticsEventBuffer` handles client-side batching with dual flush triggers:

```typescript
import { AnalyticsEventBuffer, type AnalyticsEvent } from 'dolake';

const buffer = new AnalyticsEventBuffer({
  maxBatchSize: 100,      // Flush at 100 events
  flushTimeoutMs: 5000,   // Or after 5 seconds
});

// Add events to buffer
const event: AnalyticsEvent = {
  eventId: crypto.randomUUID(),
  eventType: 'page_view',
  timestamp: Date.now(),
  sessionId: 'session-123',
  userId: 'user-456',
  payload: {
    page: '/home',
    referrer: 'https://google.com',
  },
};

const shouldFlush = buffer.add(event);

// Flush when threshold reached or timeout
if (shouldFlush || buffer.shouldFlushByTime()) {
  const batch = buffer.flush();
  // Send batch via WebSocket
  ws.send(buffer.serializeBatch(batch));
}
```

#### Date-Based Partitioning

Analytics events are automatically partitioned by date for efficient querying:

```typescript
// Events are stored with date partitioning
// analytics/year=2024/month=03/day=15/{uuid}.parquet

const handler = new AnalyticsEventHandler(P2_DURABILITY_CONFIG);

// Query partitions for a date range
const partitions = handler.getPartitionsForRange(
  new Date('2024-03-01'),
  new Date('2024-03-15')
);
// Returns: ['year=2024/month=03/day=01', 'year=2024/month=03/day=02', ...]
```

#### VFS Fallback and Recovery

When R2 is unavailable, events are stored in VFS (Durable Object storage) and recovered later:

```typescript
// Automatic recovery from VFS to R2
const result = await handler.recoverFromFallback(vfsData, {
  r2Bucket: env.LAKEHOUSE_BUCKET,
});

if (result.success) {
  console.log(`Recovered ${result.recoveredEvents} events to R2`);
}
```

#### Metrics and Monitoring

Track P2 durability metrics:

```typescript
const metrics = handler.getMetrics();
// {
//   batchesReceived: 150,
//   eventsReceived: 15000,
//   bytesReceived: 2500000,
//   r2Writes: 145,
//   vfsFallbacks: 5,      // Times VFS fallback was used
//   errors: 0,
// }
```

#### Status Endpoint

Check analytics durability status via HTTP:

```typescript
const response = await fetch('https://your-worker.workers.dev/analytics/status');
const status = await response.json();
// {
//   durabilityTier: 'P2',
//   primaryStorage: 'r2',
//   fallbackStorage: 'vfs',
//   eventsBuffered: 42,
// }
```

### Event Classification

Events are automatically classified into durability tiers based on table name, source, or explicit metadata:

```typescript
import { classifyEvent, DurabilityTier } from 'dolake';

// Classification by table name
classifyEvent({ table: 'payments', ... });      // P0 (Critical)
classifyEvent({ table: 'users', ... });         // P1 (Important)
classifyEvent({ table: 'analytics_events', ... }); // P2 (Standard)
classifyEvent({ table: 'anonymous_visits', ... }); // P3 (Best-effort)

// Explicit durability override
classifyEvent({
  table: 'custom_events',
  metadata: { durability: 'P0' },  // Force P0
  ...
});
```

### Durability Guarantees

| Tier | Guarantee | Latency | Throughput |
|------|-----------|---------|------------|
| P0 | Never lost, DLQ on failure | Higher (dual-write) | Lower |
| P1 | High durability, fallback available | Medium | Medium |
| P2 | Durable with VFS fallback | Low | High |
| P3 | Best-effort, acceptable loss | Lowest | Highest |

## Security

### Pre-Parse Size Validation

DoLake implements **pre-parse size validation** as a critical security feature to prevent Denial-of-Service (DoS) attacks via memory exhaustion.

#### How It Works

When a WebSocket message arrives, DoLake validates the raw message size **before** any JSON parsing occurs:

```typescript
// websocket-handler.ts - handleMessage()
const payloadSize = typeof message === 'string'
  ? new TextEncoder().encode(message).length
  : message.byteLength;

// Pre-parse size validation: reject oversized messages BEFORE JSON.parse
if (payloadSize > this.rateLimitConfig.maxPayloadSize) {
  this.handleOversizedPayload(ws, attachment, payloadSize);
  return; // Message rejected without parsing
}
```

This validation occurs at the entry point of message processing, before:
1. `JSON.parse()` is called
2. `TextDecoder.decode()` is called (for binary messages)
3. Any Zod schema validation runs
4. Memory is allocated for the parsed object

#### Why This Matters

Without pre-parse validation, an attacker could send a malicious payload like:

```json
{"type":"cdc_batch","events":[...millions of items...],"data":"x".repeat(100_000_000)}
```

If `JSON.parse()` runs first, the JavaScript runtime allocates memory for the entire parsed object before any size checks occur. This can:

1. **Exhaust Worker memory**: Cloudflare Workers have a 128MB memory limit. A single 50MB JSON payload could crash the Durable Object.
2. **Cause cascading failures**: Memory pressure affects all connections to the DO, not just the attacker's.
3. **Enable amplification attacks**: Deeply nested JSON structures can consume 10-100x more memory when parsed than their wire size.

#### Behavior on Oversized Payloads

When a payload exceeds `maxPayloadSize` (default: 4MB), DoLake:

1. **Immediately rejects** with a `payload_too_large` NACK (no parsing attempted)
2. **Tracks the violation** in rate limiter metrics
3. **Returns detailed error info** including actual size, max size, and human-readable message
4. **Closes repeat offenders** after multiple violations

Example NACK response:
```json
{
  "type": "nack",
  "sequenceNumber": 0,
  "reason": "payload_too_large",
  "errorMessage": "Payload size 5.00 MB exceeded limit of 4.00 MB",
  "shouldRetry": false,
  "maxSize": 4194304,
  "actualSize": 5242880,
  "receivedSize": 5242880
}
```

#### Configuration

Size limits are configured via `RateLimitConfig`:

```typescript
interface RateLimitConfig {
  /** Maximum payload size in bytes (default: 4MB) */
  maxPayloadSize: number;

  /** Maximum individual event size in bytes (default: 1MB) */
  maxEventSize: number;
}
```

#### Monitoring

Pre-parse rejections are tracked in Prometheus metrics:

```
# HELP dolake_preparse_size_rejections Pre-parse size rejections (before JSON.parse)
# TYPE dolake_preparse_size_rejections counter
dolake_preparse_size_rejections 42
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
import { DEFAULT_RATE_LIMIT_CONFIG } from 'dolake';
```

### Token Bucket Algorithm

DoLake uses the **token bucket algorithm** for rate limiting, which provides smooth rate control while allowing controlled bursts of traffic. This algorithm is particularly well-suited for CDC streaming where traffic patterns can be bursty.

#### How It Works

The token bucket algorithm uses a conceptual "bucket" that holds tokens:

```
                    Token Bucket
              +---------------------+
              |  o o o o o o o o    |  <- Tokens (capacity: 50)
              |  o o o o o o o o    |
              |  o o o o o o        |  <- Current tokens: 38
              |                     |
              |    | Refill         |  <- 10 tokens/second added
              |    v (10/sec)       |
              +---------------------+
                       |
                       v
              +---------------------+
              |   Message Queue     |
              |   [msg] [msg] [msg] |  <- Each message consumes 1 token
              +---------------------+
```

**Key Concepts:**

1. **Tokens**: Each token represents permission to send one message. Messages can only be sent if tokens are available.

2. **Capacity (burstCapacity)**: The maximum number of tokens the bucket can hold. This determines the maximum burst size - how many messages can be sent in rapid succession.

3. **Refill Rate (refillRate)**: Tokens are continuously added to the bucket at this rate (tokens per second). This determines the sustained throughput rate.

4. **Token Consumption**: Each message sent consumes one token from the bucket.

#### Algorithm Behavior

```
Time (seconds)    Tokens    Action
----------------------------------------------
0                 50        Initial state (bucket full)
0.0               49        Message sent (-1 token)
0.0               48        Message sent (-1 token)
...
0.0               0         Message sent (-1 token)
0.0               0         RATE LIMITED (no tokens!)
0.1               1         Refill (+1 token)
0.1               0         Message sent (-1 token)
0.2               1         Refill (+1 token)
...
5.0               50        Bucket full again (capped at capacity)
```

**Burst Handling:**

With the default configuration (`burstCapacity: 50`, `refillRate: 10`):

- **Burst**: You can send up to 50 messages instantly
- **Sustained Rate**: After the burst, you can send 10 messages/second
- **Recovery Time**: An empty bucket refills completely in 5 seconds (50 / 10 = 5s)

#### Token Bucket Implementation

DoLake maintains separate token buckets for different purposes:

| Bucket Type | Capacity | Refill Rate | Purpose |
|-------------|----------|-------------|---------|
| Message Bucket | 50 | 10/sec | Regular CDC batch messages |
| Heartbeat Bucket | 100 | 20/sec | Heartbeat/ping messages (more lenient) |

The heartbeat bucket is intentionally more lenient (2x capacity and refill rate) to ensure connection health checks are rarely rate-limited.

#### Refill Calculation

Tokens are refilled based on elapsed time since the last operation:

```typescript
// Simplified refill logic from rate-limiter.ts
const elapsed = (now - bucket.lastRefillTime) / 1000;  // seconds
const tokensToAdd = elapsed * bucket.refillRate;
bucket.tokens = Math.min(bucket.capacity, bucket.tokens + tokensToAdd);
```

This means:
- If 0.5 seconds have passed with `refillRate: 10`, 5 tokens are added
- Tokens never exceed the bucket capacity
- Fractional tokens are tracked for precision

#### Why Token Bucket?

Compared to other rate limiting algorithms:

| Algorithm | Pros | Cons |
|-----------|------|------|
| **Token Bucket** | Allows bursts, smooth rate limiting, simple | Requires tracking token state |
| Fixed Window | Simple to implement | Allows 2x burst at window boundaries |
| Sliding Window | Precise rate limiting | More memory/computation |
| Leaky Bucket | Strict rate enforcement | No burst tolerance |

Token bucket is ideal for CDC streaming because:
1. **Burst tolerance**: CDC events often arrive in bursts (e.g., batch imports)
2. **Smooth degradation**: When rate-limited, clients receive clear retry signals
3. **Fair resource allocation**: Each connection gets its own bucket
4. **Predictable recovery**: Clients know exactly when capacity will be available

#### Configuration Example

```typescript
import { RateLimiter, DEFAULT_RATE_LIMIT_CONFIG } from 'dolake';

// High-throughput configuration
const highThroughputLimiter = new RateLimiter({
  burstCapacity: 100,     // Allow bursts of 100 messages
  refillRate: 50,         // Sustained rate: 50 messages/second
  messagesPerSecond: 50,  // Match refill rate for consistency
});

// Strict rate limiting for public-facing endpoints
const strictLimiter = new RateLimiter({
  burstCapacity: 10,      // Small burst allowance
  refillRate: 5,          // Sustained rate: 5 messages/second
  messagesPerSecond: 5,
});

// Check rate limit status
const result = limiter.checkMessage(connectionId, 'cdc_batch', payloadSize);
if (!result.allowed) {
  console.log(`Rate limited. Remaining tokens: ${result.remainingTokens}`);
  console.log(`Bucket capacity: ${result.bucketCapacity}`);
  console.log(`Retry in: ${result.retryDelayMs}ms`);
}
```

#### Monitoring Token Usage

The rate limiter exposes token bucket state in responses:

```typescript
// In ack/nack messages
{
  "type": "ack",
  "details": {
    "remainingTokens": 42,      // Current tokens in bucket
    "bucketCapacity": 50,       // Maximum bucket capacity
    "suggestedDelayMs": 100     // Backpressure signal
  },
  "rateLimit": {
    "limit": 100,               // Configured messages per second
    "remaining": 42,            // Remaining in current window
    "resetAt": 1704067200       // Unix timestamp when limit resets
  }
}
```

### Response Headers

Rate limit information is included in HTTP responses:

```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 45
X-RateLimit-Reset: 1704067200
Retry-After: 5
```

### Whitelisted Networks

By default, connections from private IP ranges bypass rate limiting. This is appropriate for internal service-to-service communication where DoSQL shards connect to DoLake within the same network.

**Default Whitelisted Networks:**

| CIDR Range | Description |
|------------|-------------|
| `10.0.0.0/8` | RFC 1918 Class A private network (10.0.0.0 - 10.255.255.255) |
| `172.16.0.0/12` | RFC 1918 Class B private network (172.16.0.0 - 172.31.255.255) |
| `192.168.0.0/16` | RFC 1918 Class C private network (192.168.0.0 - 192.168.255.255) |
| `127.0.0.1` | Localhost (loopback address) |

**Rationale for Whitelisting Private Networks:**

1. **Internal Traffic**: DoSQL shards typically connect to DoLake within the same Cloudflare network or VPC. Rate limiting internal service traffic is unnecessary and can impede legitimate CDC streaming.

2. **Trust Boundary**: Private IP ranges (RFC 1918) are not routable on the public internet. Traffic from these addresses originates from within your infrastructure.

3. **Performance**: Skipping rate limit checks for trusted internal traffic reduces latency and processing overhead.

4. **Burst Handling**: Internal services may need to send large CDC batches during catch-up or recovery scenarios without being throttled.

**Custom Whitelist Configuration:**

```typescript
import { RateLimiter, DEFAULT_RATE_LIMIT_CONFIG } from 'dolake';

// Add additional trusted networks
const limiter = new RateLimiter({
  whitelistedIps: [
    ...DEFAULT_RATE_LIMIT_CONFIG.whitelistedIps,
    '100.64.0.0/10',    // CGNAT (Carrier-Grade NAT)
    '198.51.100.0/24',  // Custom internal range
  ],
});

// Or replace the default whitelist entirely
const strictLimiter = new RateLimiter({
  whitelistedIps: ['10.0.0.0/8'],  // Only allow Class A private
});
```

**Security Considerations:**

- Only whitelist IP ranges you control and trust
- In production, verify that your Cloudflare Workers receive accurate client IPs (check `CF-Connecting-IP` header handling)
- Consider whether external traffic could spoof private IP addresses in your deployment

### Load Levels

DoLake dynamically adjusts its behavior based on system load. The rate limiter tracks a **load level** that determines how aggressively the system protects itself from overload.

#### Load Level Calculation

The load level is calculated from the **utilization ratio**:

```
utilizationRatio = activeConnections / (connectionsPerSecond * 10)
```

With the default configuration (`connectionsPerSecond: 20`), this means:
- 200 active connections = 100% utilization
- 100 active connections = 50% utilization

#### Load Level Thresholds

| Load Level | Utilization Ratio | Degraded Mode | Description |
|------------|-------------------|---------------|-------------|
| `normal` | < 50% | No | System operating normally. All requests processed without restrictions. |
| `elevated` | 50% - 70% | No | System is busy but handling load. No special restrictions yet. |
| `high` | 70% - 90% | Yes | System under significant load. Degraded mode enabled for protection. |
| `critical` | > 90% | Yes | System at capacity. Load shedding active for non-priority traffic. |

#### System Behavior at Each Level

**Normal (< 50% utilization)**
- All CDC batches processed immediately
- Full burst capacity available
- No backpressure signaling

**Elevated (50% - 70% utilization)**
- All CDC batches still processed
- Backpressure signals may start appearing in ack messages
- Clients should monitor `bufferUtilization` and `suggestedDelayMs`

**High (70% - 90% utilization)**
- Degraded mode enabled (`isDegraded()` returns `true`)
- Stricter rate limiting enforced
- Clients should reduce send rate based on backpressure signals

**Critical (> 90% utilization)**
- **Load shedding active**: CDC batches from non-high-priority connections may be rejected
- NACK with `load_shedding` reason returned
- High-priority connections (with `X-Priority: high` header) continue processing
- Clients receive `retryDelayMs` indicating when to retry

#### Checking Load Level

```typescript
import { RateLimiter } from 'dolake';

const limiter = new RateLimiter();

// Get current load level
const loadLevel = limiter.getLoadLevel(); // 'normal' | 'elevated' | 'high' | 'critical'

// Check if in degraded mode
const isDegraded = limiter.isDegraded(); // true when high or critical
```

#### Handling Load Shedding

When load shedding occurs, clients receive a NACK:

```json
{
  "type": "nack",
  "reason": "load_shedding",
  "errorMessage": "System under heavy load",
  "shouldRetry": true,
  "retryDelayMs": 5000
}
```

**Recommended client behavior:**
1. Respect the `retryDelayMs` value before retrying
2. Reduce batch sizes during high load periods
3. Consider using the `X-Priority: high` header for critical traffic
4. Implement circuit breaker patterns to avoid overwhelming the system

```typescript
// High-priority connection example
const ws = new WebSocket('wss://dolake.example.com/lakehouse', {
  headers: {
    'X-Priority': 'high',
  },
});
```

### Subnet-Based Rate Limiting

DoLake implements subnet-level rate limiting to prevent abuse from distributed IP pools. Instead of only tracking individual IP addresses, the rate limiter aggregates connections at the subnet level:

- **IPv4**: `/24` subnet (e.g., `192.168.1.0/24` - 256 addresses)
- **IPv6**: `/64` subnet (e.g., `2001:db8:1234:5678::/64`)

#### Why Subnet Limiting?

Individual IP-based rate limiting can be easily bypassed by attackers who control large IP pools (e.g., botnets, cloud instances, or IPv6 address ranges). By tracking connections at the subnet level:

1. **Prevents IP rotation attacks**: An attacker rotating through IPs in the same `/24` block still hits the subnet limit
2. **Mitigates cloud abuse**: Cloud providers typically assign IPs from the same subnet to a single tenant
3. **Handles IPv6 effectively**: A single IPv6 allocation often includes billions of addresses (`/64` = 2^64 addresses), making per-IP limiting useless without subnet aggregation
4. **Reduces memory usage**: Tracking `/24` subnets requires far fewer entries than tracking every unique IP

#### Configuration

```typescript
const rateLimiter = new RateLimiter({
  // Per-IP connection limit
  maxConnectionsPerIp: 30,

  // Per-subnet connection limit (/24 for IPv4, /64 for IPv6)
  subnetRateLimitThreshold: 100,

  // Whitelist for trusted networks (bypasses all rate limiting)
  whitelistedIps: ['10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16'],
});
```

When a subnet exceeds `subnetRateLimitThreshold` connections, new connections from any IP in that subnet are rejected with an `ip_limit` error until existing connections close.

### Best Practices

1. **Respect Retry-After**: When receiving a 429 or nack with `retryDelayMs`, wait the specified time
2. **Monitor Buffer Utilization**: Reduce send rate when `bufferUtilization` exceeds 0.7
3. **Use Correlation IDs**: Include `correlationId` to track request/response pairs
4. **Handle Backpressure**: Implement backpressure handling based on `suggestedDelayMs`
5. **Batch Efficiently**: Aim for 100-1000 events per batch for optimal throughput
6. **Implement Exponential Backoff**: Double delay on consecutive rate limits

## Scalability

DoLake provides comprehensive scalability features for handling high-throughput CDC workloads across multiple Durable Object instances.

### ParallelWriteManager

Manages parallel writes to multiple partitions with throttling and batching support.

```typescript
import {
  ParallelWriteManager,
  type ScalingConfig,
  type ParallelWriteResult,
  DEFAULT_SCALING_CONFIG,
} from 'dolake';

// Create with custom configuration
const writeManager = new ParallelWriteManager({
  ...DEFAULT_SCALING_CONFIG,
  parallelPartitionWrites: true,
  maxParallelWriters: 4,
  maxPartitionWriteBytesPerSecond: 100 * 1024 * 1024, // 100MB/s
});

// Prepare partition data
const partitionData = new Map<string, CDCEvent[]>();
partitionData.set('dt=2024-01-15', events1);
partitionData.set('dt=2024-01-16', events2);

// Write to multiple partitions in parallel
const result: ParallelWriteResult = await writeManager.writeParallel(
  partitionData,
  async (partition, events) => {
    // Your write logic here
    return {
      partition,
      filesWritten: 1,
      bytesWritten: BigInt(events.length * 100),
      recordsWritten: BigInt(events.length),
      durationMs: 50,
      success: true,
    };
  }
);

console.log(`Wrote ${result.filesWritten} files across ${result.partitionsWritten} partitions`);
console.log(`Parallel writes used: ${result.parallelWritesUsed}`);
console.log(`Failed partitions: ${result.failedPartitions}`);

// Check throttling status
const throttleStatus = writeManager.getThrottlingStatus();
if (throttleStatus.throttlingActive) {
  console.log('Throttled partitions:', throttleStatus.throttledPartitions);
}
```

#### Configuration Options

```typescript
interface ScalingConfig {
  /** Scaling mode: single DO, partition-per-DO, or auto */
  scalingMode: 'single' | 'partition-per-do' | 'auto';  // Default: 'single'

  /** Minimum DO instances */
  minInstances: number;                    // Default: 1

  /** Maximum DO instances */
  maxInstances: number;                    // Default: 16

  /** Partitions per DO instance */
  partitionsPerInstance: number;           // Default: 4

  /** Enable parallel partition writes */
  parallelPartitionWrites: boolean;        // Default: true

  /** Maximum parallel writers */
  maxParallelWriters: number;              // Default: 4

  /** Maximum Parquet file size in bytes */
  maxParquetFileSize: number;              // Default: 512MB

  /** Use multipart upload for large files */
  useMultipartUpload: boolean;             // Default: true

  /** Maximum partition write throughput (bytes/sec) */
  maxPartitionWriteBytesPerSecond: number; // Default: 100MB/s
}
```

### HorizontalScalingManager

Manages horizontal scaling of Durable Object instances with consistent hashing for partition routing.

```typescript
import {
  HorizontalScalingManager,
  type ScalingStatus,
  DEFAULT_SCALING_CONFIG,
} from 'dolake';

const scalingManager = new HorizontalScalingManager({
  ...DEFAULT_SCALING_CONFIG,
  scalingMode: 'auto',
  minInstances: 2,
  maxInstances: 16,
  partitionsPerInstance: 4,
});

// Get scaling status
const status: ScalingStatus = scalingManager.getStatus(totalPartitions);
console.log(`Current instances: ${status.currentInstances}`);
console.log(`Recommendation: ${status.scalingRecommendation}`);

// Route requests to appropriate DO instance
const routing = scalingManager.routeToInstance('users', 'dt=2024-01-15');
console.log(`Target DO: ${routing.targetDoId}`);
console.log(`Instance index: ${routing.instanceIndex}`);

// Manual scaling
scalingManager.scaleTo(8);

// Auto-scale based on partition count
const newInstanceCount = scalingManager.autoScale(totalPartitions);
```

### AutoScalingManager

Extends HorizontalScalingManager with load-based automatic scaling based on buffer utilization and latency metrics.

```typescript
import {
  AutoScalingManager,
  type AutoScalingConfig,
  type LoadMetrics,
  type ScaleEvent,
} from 'dolake';

const autoScaler = new AutoScalingManager({
  minInstances: 2,
  maxInstances: 16,
  scaleUpBufferThreshold: 0.8,        // Scale up when buffer > 80%
  scaleUpLatencyThresholdMs: 5000,    // Scale up when latency > 5s
  scaleDownBufferThreshold: 0.2,      // Scale down when buffer < 20%
  scaleDownCooldownMs: 300000,        // Wait 5 min before scale-down
  scaleCooldownMs: 60000,             // 1 min between scaling operations
  drainTimeoutMs: 300000,             // 5 min drain timeout
});

// Record metrics from each shard
autoScaler.recordLoadMetrics('shard-0', {
  bufferUtilization: 0.85,
  avgLatencyMs: 100,
  p99LatencyMs: 500,
  requestsPerSecond: 1000,
  activeConnections: 50,
});

// Detect overload conditions
const overload = autoScaler.detectOverload();
if (overload.isOverloaded) {
  console.log(`Overload detected: ${overload.reason}`);
}

// Evaluate if scaling is needed
const scaleEvent = autoScaler.evaluateScaling();
if (scaleEvent) {
  console.log(`Scaling ${scaleEvent.type}: ${scaleEvent.reason}`);
  console.log(`From ${scaleEvent.fromInstances} to ${scaleEvent.toInstances} instances`);
}

// Subscribe to scale events
autoScaler.onScaleEvent((event: ScaleEvent) => {
  console.log(`Scale event: ${event.type}`);
  if (event.affectedShards) {
    console.log(`Affected shards: ${event.affectedShards.join(', ')}`);
  }
});

// Manual scale operations
await autoScaler.scaleUp(4);
await autoScaler.scaleDown(2);

// Graceful shard draining
await autoScaler.startDrain('shard-1');
// ... wait for drain to complete ...
await autoScaler.completeDrain('shard-1');

// Get shard health
const health = autoScaler.getShardHealth('shard-0');
console.log(`Shard status: ${health?.status}`); // 'healthy' | 'draining' | 'unhealthy'

// Get aggregate metrics across all shards
const aggregate = autoScaler.getAggregateMetrics();
console.log(`Avg buffer utilization: ${aggregate.bufferUtilization}`);
console.log(`Total RPS: ${aggregate.requestsPerSecond}`);
```

#### Auto-Scaling Behavior

| Condition | Action |
|-----------|--------|
| Buffer utilization > 80% | Scale up |
| Average latency > 5 seconds | Scale up |
| Buffer utilization < 20% for 5+ minutes | Scale down |
| Already at max instances | No scale-up |
| Already at min instances | No scale-down |
| Within cooldown period | No scaling |

#### Shard Health States

| State | Description |
|-------|-------------|
| `healthy` | Shard is accepting requests normally |
| `draining` | Shard is completing existing requests, not accepting new ones |
| `unhealthy` | Shard has buffer > 90% or latency > 10s |

### Additional Scalability Components

#### PartitionCompactionManager

Manages compaction of partition files to optimal sizes.

```typescript
import { PartitionCompactionManager } from 'dolake';

const compactionManager = new PartitionCompactionManager();
compactionManager.registerPartition('dt=2024-01-15', partitionMetadata);

const result = await compactionManager.compactPartition(
  { namespace: ['default'], tableName: 'events', partition: 'dt=2024-01-15' },
  files,
  async (filesToMerge) => mergedFiles
);
```

#### PartitionRebalancer

Analyzes partition distribution and recommends rebalancing actions.

```typescript
import { PartitionRebalancer } from 'dolake';

const rebalancer = new PartitionRebalancer();
rebalancer.registerPartitionSize('dt=2024-01-15', BigInt(1024 * 1024 * 1024), BigInt(1000000));

const analysis = rebalancer.analyzePartitions('events');
console.log(`Skew factor: ${analysis.skewFactor}`);
console.log(`Hot partitions: ${analysis.hotPartitions}`);

const recommendation = rebalancer.recommend('events', BigInt(512 * 1024 * 1024), 2.0);
for (const action of recommendation.actions) {
  console.log(`${action.type} ${action.partition}: ${action.reason}`);
}
```

#### LargeFileHandler

Handles large file operations with multipart upload and range reads.

```typescript
import { LargeFileHandler } from 'dolake';

const fileHandler = new LargeFileHandler();

// Write large file with multipart upload
const writeResult = await fileHandler.writeLargeFile(
  { tableName: 'events', targetSizeBytes: 1024 * 1024 * 1024, useMultipart: true },
  generateData,
  uploadPart
);

// Read specific row groups using range requests
const readResult = await fileHandler.readRowGroups(
  { filePath: 'path/to/file.parquet', rowGroupRange: { start: 0, end: 10 }, useRangeRequests: true },
  100, // total row groups
  BigInt(10 * 1024 * 1024), // row group size
  fetchRange
);
```

## Partitioning

DoLake supports multiple partitioning strategies:

```typescript
import {
  createDayPartitionSpec,
  createHourPartitionSpec,
  createBucketPartitionSpec,
  createCompositePartitionSpec,
} from 'dolake';

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

## Cache Invalidation

DoLake provides a comprehensive cache invalidation system for the R2 warm tier. The `CacheInvalidator` class handles automatic cache invalidation on CDC events (INSERT/UPDATE/DELETE), supports partial invalidation by partition, batches multiple invalidations efficiently, and can propagate invalidations across replicas.

### Configuration

```typescript
import {
  CacheInvalidator,
  type CacheInvalidationConfig,
  DEFAULT_CACHE_INVALIDATION_CONFIG,
} from 'dolake';

const invalidator = new CacheInvalidator({
  enabled: true,                    // Enable cache invalidation
  batchSize: 100,                   // Max batch size before flush
  batchDelayMs: 50,                 // Max delay before batch flush (ms)
  ttlMs: 300_000,                   // Default TTL for cache entries (5 minutes)
  propagateToReplicas: true,        // Propagate to replicas
  staleReadPrevention: true,        // Prevent stale reads during invalidation
  consistencyMode: 'strict',        // 'strict' | 'eventual'
  batchInvalidation: true,          // Enable batch invalidation
  deduplicateInvalidations: true,   // Deduplicate invalidation requests
  readYourWritesConsistency: true,  // Enable read-your-writes
  slidingExpiration: false,         // Enable sliding TTL expiration
  maxPropagationDelayMs: 5000,      // Max propagation delay for eventual consistency
});
```

### CDC Integration

The cache invalidator automatically processes CDC events and invalidates affected cache entries:

```typescript
// Process CDC events to trigger cache invalidation
await invalidator.processCDCEvents([
  {
    sequence: 1,
    timestamp: Date.now(),
    operation: 'UPDATE',
    table: 'users',
    rowId: 'user-123',
    before: { id: 'user-123', name: 'Alice' },
    after: { id: 'user-123', name: 'Alice Smith' },
  },
]);

// Flush pending invalidations manually if needed
const result = await invalidator.flushPendingInvalidations();
console.log(`Invalidated ${result.keysInvalidated.length} keys`);
console.log(`Notified ${result.replicasNotified} replicas`);
```

### Manual Invalidation

You can manually invalidate cache entries for specific tables and partitions:

```typescript
// Invalidate all entries for a table
await invalidator.invalidate('users');

// Invalidate specific partitions
await invalidator.invalidate('events', ['day=2024-01-15', 'day=2024-01-16']);

// Invalidate using wildcard patterns
await invalidator.invalidate('events', undefined, 'day=2024-01-*');
```

### Partition-Specific Invalidation

When CDC events contain partition information (e.g., date fields), only the affected partitions are invalidated:

```typescript
// Register cache entries for partitions
const key1 = invalidator.registerCacheEntry('events', 'day=2024-01-15');
const key2 = invalidator.registerCacheEntry('events', 'day=2024-01-16');

// After an UPDATE to day=2024-01-15, only that partition is invalidated
await invalidator.processCDCEvents([{
  sequence: 1,
  timestamp: Date.now(),
  operation: 'UPDATE',
  table: 'events',
  rowId: 'event-123',
  after: { id: 'event-123', day: '2024-01-15', value: 100 },
}]);

// Check partition status
const status = invalidator.getPartitionCacheStatus('events');
console.log('Invalidated partitions:', status.invalidatedPartitions);
console.log('Unchanged partitions:', status.unchangedPartitions);
```

### Per-Table TTL Configuration

Configure different TTL values for different tables based on their access patterns:

```typescript
invalidator.configureTableTTLs({
  hot_table: { ttlMs: 60_000 },      // 1 minute for frequently changing data
  cold_table: { ttlMs: 3_600_000 },  // 1 hour for stable data
  default: { ttlMs: 300_000 },       // 5 minutes default
});
```

### Replica Propagation

Configure invalidation propagation across replicas for distributed deployments:

```typescript
invalidator.configureReplication({
  enabled: true,
  replicas: ['replica-1', 'replica-2', 'replica-3'],
  invalidationPropagation: true,
  consistencyMode: 'strict',  // Wait for all replicas
  maxPropagationDelayMs: 5000,
});

// Check propagation status
const propStatus = invalidator.getInvalidationStatus();
console.log(`Replicas notified: ${propStatus.replicasNotified}`);
console.log(`Replicas confirmed: ${propStatus.replicasConfirmed}`);
console.log(`Failed replicas:`, propStatus.failedReplicas);
```

### Read-Your-Writes Consistency

Enable session-based consistency to ensure clients see their own writes:

```typescript
// Create a session
const session = invalidator.createSession();
const token = session.token;

// Record writes in the session
invalidator.recordSessionWrite(token, 'users', 1);

// Check if read can see the write
const canRead = invalidator.canReadOwnWrite(token, 'users');
if (!canRead) {
  // Bypass cache and read directly from storage
}
```

### Cache Metrics

Monitor cache performance with comprehensive metrics:

```typescript
const metrics = invalidator.getMetrics();
console.log(`Hit rate: ${(metrics.hitRate * 100).toFixed(1)}%`);
console.log(`Miss rate: ${(metrics.missRate * 100).toFixed(1)}%`);
console.log(`Stale reads blocked: ${metrics.staleReadsBlocked}`);
console.log(`Invalidations pending: ${metrics.invalidationsPending}`);
console.log(`Keys invalidated: ${metrics.keysInvalidated}`);
console.log(`Batch operations: ${metrics.batchInvalidationOps}`);
console.log(`Deduplicated requests: ${metrics.invalidationRequestsDeduplicated}`);
```

### HTTP Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/cache/metrics` | Get cache metrics |
| GET | `/v1/cache/entry/{table}` | Get cache entry status for a table |
| GET | `/v1/cache/partitions/{table}` | Get partition cache status |
| GET | `/v1/cache/queries/{table}` | Get query cache status |
| PUT | `/v1/cache/config` | Update cache configuration |
| PUT | `/v1/cache/config/tables` | Configure per-table TTLs |
| POST | `/v1/cache/invalidate` | Manually invalidate cache entries |
| GET | `/v1/cache/invalidation/status` | Get invalidation propagation status |
| GET | `/v1/cache/invalidation/pending` | Get pending invalidation count |
| PUT | `/v1/config/replication` | Configure replica settings |

### Best Practices

1. **Use batch invalidation**: Enable `batchInvalidation` to reduce overhead when processing many CDC events
2. **Configure appropriate TTLs**: Set shorter TTLs for frequently changing data and longer TTLs for stable data
3. **Enable deduplication**: Keep `deduplicateInvalidations` enabled to avoid redundant invalidations
4. **Monitor metrics**: Track hit rates and stale read counts to tune your cache configuration
5. **Use strict mode for critical data**: Set `consistencyMode: 'strict'` when data consistency is paramount
6. **Implement session tracking**: Use read-your-writes consistency for interactive applications

## Zod Schema Validation

All messages are validated using Zod:

```typescript
import {
  validateClientMessage,
  validateCDCBatchMessage,
  MessageValidationError,
  CDCBatchMessageSchema,
} from 'dolake';

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
} from 'dolake';

try {
  // ... operation
} catch (error) {
  if (error instanceof BufferOverflowError) {
    // Buffer full - implement backpressure
    // error.retryable === true
    // Access detailed context:
    console.log(`Buffer: ${error.currentSizeBytes}/${error.maxSizeBytes} bytes`);
    console.log(`Utilization: ${(error.utilization * 100).toFixed(1)}%`);
    console.log(`Attempted batch size: ${error.attemptedBatchSizeBytes} bytes`);
    // Wait for flush before retrying
    await new Promise(r => setTimeout(r, 1000));
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

### Stability Legend

- :green_circle: **Stable** - API is stable and unlikely to change
- :yellow_circle: **Beta** - API is mostly stable but may have minor changes
- :red_circle: **Experimental** - API may change significantly

| Category | Exports | Stability |
|----------|---------|-----------|
| Main class | `DoLake`, `DoLakeEnv` | :red_circle: Experimental |
| Types | `CDCEvent`, `CDCBatchMessage`, `ConnectMessage`, `AckMessage`, `NackMessage`, `DoLakeConfig`, `DoLakeState`, `FlushResult`, `BufferStats` | :red_circle: Experimental |
| Rate limiting | `RateLimiter`, `RateLimitConfig`, `RateLimitResult`, `DEFAULT_RATE_LIMIT_CONFIG` | :yellow_circle: Beta |
| Schemas | `CDCBatchMessageSchema`, `ConnectMessageSchema`, `validateClientMessage`, `MessageValidationError` | :yellow_circle: Beta |
| Iceberg utilities | `R2IcebergStorage`, `createTableMetadata`, `createAppendSnapshot`, `createDayPartitionSpec` | :red_circle: Experimental |
| Compaction | `CompactionManager`, `CompactionConfig`, `DEFAULT_COMPACTION_CONFIG` | :red_circle: Experimental |

### Import Examples

```typescript
// Main class - Experimental
export { DoLake, type DoLakeEnv } from 'dolake';

// Types - Experimental
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
} from 'dolake';

// Rate limiting - Beta
export {
  RateLimiter,
  type RateLimitConfig,
  type RateLimitResult,
  DEFAULT_RATE_LIMIT_CONFIG,
} from 'dolake';

// Schemas - Beta
export {
  CDCBatchMessageSchema,
  ConnectMessageSchema,
  validateClientMessage,
  MessageValidationError,
} from 'dolake';

// Iceberg utilities - Experimental
export {
  R2IcebergStorage,
  createTableMetadata,
  createAppendSnapshot,
  createDayPartitionSpec,
} from 'dolake';

// Compaction - Experimental
export {
  CompactionManager,
  type CompactionConfig,
  DEFAULT_COMPACTION_CONFIG,
} from 'dolake';
```

## Related Packages

- **[lake.do](https://www.npmjs.com/package/lake.do)** - Client SDK for connecting to DoLake
- **[sql.do](https://www.npmjs.com/package/sql.do)** - DoSQL with built-in CDC streaming
- **[@dotdo/sql-types](https://www.npmjs.com/package/@dotdo/sql-types)** - Shared type definitions

## Documentation

- [Architecture](./docs/ARCHITECTURE.md) - System design and data flow
- [DoSQL Integration](./docs/INTEGRATION.md) - How to connect DoSQL to DoLake
- [REST API Reference](./docs/API.md) - Complete Iceberg REST Catalog API

## Troubleshooting

### WebSocket Close Codes

DoLake uses standard WebSocket close codes (RFC 6455) and custom application codes to communicate connection closure reasons. Understanding these codes helps with debugging and implementing proper reconnection logic.

#### Standard Close Codes (RFC 6455)

| Code | Name | Description | When Used |
|------|------|-------------|-----------|
| 1000 | Normal Closure | Connection closed cleanly after completing its purpose | Graceful shutdown, scaling down, client disconnect |
| 1001 | Going Away | Endpoint is going away (server shutdown or browser navigation) | Server restart, deployment, browser tab close |
| 1002 | Protocol Error | Connection terminated due to protocol error | Invalid WebSocket frame format |
| 1003 | Unsupported Data | Received data type that cannot be accepted | Binary message when only text expected, or vice versa |
| 1006 | Abnormal Closure | Connection closed abnormally (no close frame received) | Network failure, process crash, timeout |
| 1007 | Invalid Payload | Message payload data was inconsistent with message type | Invalid UTF-8 in text message |
| 1008 | Policy Violation | Message violates server policy | Unknown connection, too many size violations, authentication failure |
| 1009 | Message Too Big | Message too large to process | Payload exceeds `maxPayloadSize` (default: 4MB) |
| 1011 | Unexpected Condition | Server encountered unexpected condition preventing request fulfillment | Internal server error |

#### Custom Close Codes (4000-4999)

Custom codes in the 4000-4999 range are application-specific:

| Code | Name | Description | When Used |
|------|------|-------------|-----------|
| 4000 | Heartbeat Timeout | Client failed to respond to heartbeats within timeout period | Connection idle too long without ping/pong |
| 4001 | Authentication Failed | Authentication credentials invalid or expired | Token validation failure, missing credentials |
| 4002 | Rate Limited | Connection closed due to excessive rate limit violations | Repeated `rate_limited` NACKs |
| 4003 | Buffer Overflow | Connection closed due to persistent buffer overflow | Repeated `buffer_full` NACKs without recovery |

#### Handling Close Codes

```typescript
ws.onclose = (event: CloseEvent): void => {
  const { code, reason, wasClean } = event;

  switch (code) {
    case 1000:
      // Normal closure - connection completed successfully
      console.log('Connection closed normally');
      break;

    case 1001:
      // Server going away - reconnect to new instance
      console.log('Server going away, reconnecting...');
      reconnectWithBackoff();
      break;

    case 1006:
      // Abnormal closure - network issue
      console.warn('Connection lost unexpectedly');
      reconnectWithBackoff();
      break;

    case 1008:
      // Policy violation - check reason for details
      console.error(`Policy violation: ${reason}`);
      if (reason === 'Unknown connection') {
        // Re-authenticate and reconnect
        refreshAuthAndReconnect();
      } else if (reason === 'Too many size violations') {
        // Reduce message sizes before reconnecting
        reduceBatchSize();
        reconnectWithBackoff();
      }
      break;

    case 1009:
      // Message too large - reduce payload size
      console.error('Message too large for server');
      reduceBatchSize();
      break;

    case 4000:
      // Heartbeat timeout - implement proper heartbeat handling
      console.warn('Heartbeat timeout - connection was idle');
      reconnectWithBackoff();
      break;

    default:
      console.error(`Connection closed: code=${code}, reason=${reason}`);
      if (code >= 4000 && code < 5000) {
        // Application-specific error
        handleApplicationError(code, reason);
      }
      break;
  }
};
```

#### Best Practices for Connection Management

1. **Implement heartbeat handling**: Send periodic ping messages to keep the connection alive and detect dead connections early.

2. **Handle 1008 carefully**: This code indicates policy violations. Check the `reason` field for specifics:
   - `"Unknown connection"`: The server doesn't recognize this connection (possibly after hibernation wake-up with stale state)
   - `"Too many size violations"`: Multiple oversized payloads were sent

3. **Reconnect with backoff**: For codes 1001, 1006, and 4000-4003, implement exponential backoff reconnection to avoid overwhelming the server.

4. **Monitor for 1009**: If you receive close code 1009, your messages are too large. Reduce batch sizes or split large events.

### Connection Errors

**Symptoms**: WebSocket connection fails or disconnects unexpectedly

| Error | Cause | Solution |
|-------|-------|----------|
| `CONNECTION_CLOSED` | WebSocket connection closed | Check network connectivity; implement reconnection logic |
| `CONNECTION_TIMEOUT` | Connection attempt timed out | Increase timeout; verify DoLake endpoint is accessible |
| `PROTOCOL_ERROR` | Invalid message format | Ensure message format matches the expected schema |

```typescript
// Reconnection pattern with exponential backoff
function connectWithRetry(url: string, maxRetries = 5): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    let attempt = 0;

    function tryConnect(): void {
      const ws = new WebSocket(url);

      ws.onopen = (): void => resolve(ws);
      ws.onerror = (): void => {
        if (attempt < maxRetries) {
          const delay = Math.min(100 * Math.pow(2, attempt), 30000);
          setTimeout(tryConnect, delay);
          attempt++;
        } else {
          reject(new Error('Max reconnection attempts exceeded'));
        }
      };
    }

    tryConnect();
  });
}
```

### Rate Limiting Errors

**Symptoms**: `nack` messages with `rate_limited` reason or HTTP 429 responses

| Error Reason | Cause | Solution |
|--------------|-------|----------|
| `rate_limited` | Too many messages per second | Implement backpressure; respect `Retry-After` header |
| `connection_limit` | Max connections per source exceeded | Reduce concurrent connections; use connection pooling |
| `ip_limit` | Max connections per IP/subnet exceeded | Contact support for whitelist; reduce connection count |
| `payload_too_large` | Message exceeds 4MB limit | Split into smaller batches |
| `event_too_large` | Single event exceeds 1MB | Reduce event payload size |

```typescript
// Handle rate limiting with backpressure
ws.onmessage = (event: MessageEvent): void => {
  const msg = JSON.parse(event.data as string);

  if (msg.type === 'ack' && msg.details?.suggestedDelayMs) {
    // Backpressure signal - slow down, schedule next send after delay
    setTimeout(() => {
      // Resume sending
    }, msg.details.suggestedDelayMs);
  }

  if (msg.type === 'nack' && msg.reason === 'rate_limited') {
    // Explicit rate limit - wait before retry
    const delay = msg.retryDelayMs || 1000;
    setTimeout(() => {
      // Retry the batch
    }, delay);
  }
};
```

### Buffer and Backpressure Errors

**Symptoms**: `buffer_full` or `load_shedding` nack reasons

| Error Reason | Cause | Solution |
|--------------|-------|----------|
| `buffer_full` | DoLake buffer at capacity | Wait for flush; reduce send rate |
| `load_shedding` | System under extreme load | Use high-priority header; reduce batch size |

```typescript
// Monitor buffer utilization
ws.onmessage = (event: MessageEvent): void => {
  const msg = JSON.parse(event.data as string);

  if (msg.type === 'ack') {
    const utilization = msg.details?.bufferUtilization || 0;

    if (utilization > 0.7) {
      // Proactively reduce batch size
      currentBatchSize = Math.max(100, currentBatchSize * 0.8);
    }

    if (utilization > 0.9) {
      // Pause sending until buffer clears
      pauseSending = true;
    }
  }
};
```

### Flush and Write Errors

**Symptoms**: `FlushError` or `ParquetWriteError`

| Error | Cause | Solution |
|-------|-------|----------|
| `FlushError` | Failed to write to R2 | Check R2 bucket binding; verify bucket exists |
| `ParquetWriteError` | Invalid data for Parquet format | Validate event data types match schema |
| `IcebergError` | Metadata update failed | Check R2 permissions; retry operation |

```typescript
// Check flush status
const status = await fetch('/status');
const { buffer, lastFlushTime, state } = await status.json();

if (state === 'error') {
  console.error('DoLake in error state, buffer:', buffer);
  // Trigger manual flush to recover
  await fetch('/flush', { method: 'POST' });
}
```

### Common Configuration Mistakes

| Issue | Symptom | Fix |
|-------|---------|-----|
| Missing R2 binding | `LAKEHOUSE_BUCKET is undefined` | Add `r2_buckets` to `wrangler.toml` |
| Wrong bucket name | `R2 object not found` | Verify `bucket_name` matches actual R2 bucket |
| Missing DO migration | `DoLake class not found` | Add DO migration tag in `wrangler.toml` |
| Invalid protocol version | `nack` with `invalid_format` | Use `protocolVersion: 1` in connect message |

```jsonc
// Correct wrangler.jsonc configuration
{
  "durable_objects": {
    "bindings": [
      { "name": "DOLAKE", "class_name": "DoLake" }
    ]
  },
  "r2_buckets": [
    { "binding": "LAKEHOUSE_BUCKET", "bucket_name": "my-lakehouse-data" }
  ],
  "migrations": [
    { "tag": "v1", "new_classes": ["DoLake"] }
  ]
}
```

### Performance Issues

| Symptom | Cause | Solution |
|---------|-------|----------|
| High flush latency | Large buffer, many small files | Increase `flushThresholdEvents`; reduce flush frequency |
| Memory pressure | Buffer too large | Reduce `maxBufferSize`; increase flush frequency |
| Slow queries | Too many small Parquet files | Enable auto-compaction; run manual compaction |

```typescript
// Optimal configuration for high-throughput CDC
const config = {
  flushThresholdEvents: 10000,    // Flush every 10K events
  flushThresholdBytes: 33554432,  // Or every 32MB
  flushThresholdMs: 60000,        // Or every minute
  parquetRowGroupSize: 100000,    // Optimize for query performance
  parquetCompression: 'zstd',     // Best compression ratio
};
```

## Migration Guide

This section covers migration steps for upgrading between DoLake versions.

### Migrating from 0.0.x to 0.1.x

#### Breaking Changes

| Change | 0.0.x | 0.1.x |
|--------|-------|-------|
| Message validation | No validation | Zod schema validation required |
| Rate limiting | Optional | Enabled by default |
| Protocol version | None | `protocolVersion: 1` required in connect |
| Event format | Flexible | Strict `CDCEvent` schema |
| Acknowledgments | Simple string | Typed `AckMessage`/`NackMessage` |

#### API Migration Steps

**1. Update Connect Messages**

The connect message now requires explicit protocol version and capabilities:

```typescript
// Before (0.0.x)
ws.send(JSON.stringify({
  type: 'connect',
  sourceId: 'shard-1',
}));

// After (0.1.x)
ws.send(JSON.stringify({
  type: 'connect',
  timestamp: Date.now(),
  sourceDoId: 'shard-1',
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
```

**2. Update CDC Batch Messages**

CDC batches now require sequence tracking and size metadata:

```typescript
// Before (0.0.x)
ws.send(JSON.stringify({
  type: 'batch',
  events: [...],
}));

// After (0.1.x)
ws.send(JSON.stringify({
  type: 'cdc_batch',
  timestamp: Date.now(),
  sourceDoId: 'shard-1',
  events: [...],
  sequenceNumber: batchSeq++,
  firstEventSequence: firstSeq,
  lastEventSequence: lastSeq,
  sizeBytes: calculateSize(events),
  isRetry: false,
  retryCount: 0,
}));
```

**3. Update CDC Event Format**

Events now require explicit typing:

```typescript
// Before (0.0.x)
const event = {
  op: 'insert',
  table: 'users',
  data: { id: 1, name: 'Alice' },
};

// After (0.1.x)
const event: CDCEvent = {
  sequence: seq++,
  timestamp: Date.now(),
  operation: 'INSERT',  // 'INSERT' | 'UPDATE' | 'DELETE'
  table: 'users',
  rowId: 'user-1',
  after: { id: 1, name: 'Alice' },
};
```

**4. Handle New Acknowledgment Format**

Acknowledgments now include detailed status and rate limit info:

```typescript
// Before (0.0.x)
ws.onmessage = (event) => {
  const msg = JSON.parse(event.data);
  if (msg.status === 'ok') {
    // Success
  }
};

// After (0.1.x)
ws.onmessage = (event) => {
  const msg = JSON.parse(event.data);
  if (msg.type === 'ack') {
    // Check status: 'ok' | 'buffered' | 'persisted' | 'duplicate' | 'fallback'
    if (msg.details?.suggestedDelayMs) {
      // Handle backpressure
    }
  } else if (msg.type === 'nack') {
    // Handle rejection with msg.reason and msg.shouldRetry
  }
};
```

**5. Import Path Changes**

Some imports have been reorganized:

```typescript
// Before (0.0.x)
import { DoLake } from 'dolake/do';
import { CDCEvent } from 'dolake/types';

// After (0.1.x)
import { DoLake, type CDCEvent, type DoLakeEnv } from 'dolake';
```

#### Configuration Changes

**1. New Default Configuration**

Configuration now uses explicit defaults via `DEFAULT_DOLAKE_CONFIG`:

```typescript
// Before (0.0.x) - implicit defaults
const config = {
  bufferSize: 1000,
  flushInterval: 30000,
};

// After (0.1.x) - explicit typed config
import { DEFAULT_DOLAKE_CONFIG, type DoLakeConfig } from 'dolake';

const config: Partial<DoLakeConfig> = {
  ...DEFAULT_DOLAKE_CONFIG,
  flushThresholdEvents: 10000,
  flushThresholdBytes: 33554432,
  flushThresholdMs: 60000,
};
```

**2. Rate Limiting Configuration**

Rate limiting is now enabled by default:

```typescript
// Before (0.0.x) - no rate limiting
// (nothing to configure)

// After (0.1.x) - configure rate limits
import { DEFAULT_RATE_LIMIT_CONFIG, type RateLimitConfig } from 'dolake';

const rateLimitConfig: Partial<RateLimitConfig> = {
  ...DEFAULT_RATE_LIMIT_CONFIG,
  messagesPerSecond: 100,
  burstCapacity: 50,
  maxPayloadSize: 4194304,
  backpressureThreshold: 0.8,
};
```

**3. Wrangler Configuration**

Update your `wrangler.toml` or `wrangler.jsonc`:

```diff
// wrangler.jsonc
{
  "durable_objects": {
    "bindings": [
-     { "name": "LAKEHOUSE", "class_name": "Lakehouse" }
+     { "name": "DOLAKE", "class_name": "DoLake" }
    ]
  },
  "r2_buckets": [
    { "binding": "LAKEHOUSE_BUCKET", "bucket_name": "my-lakehouse-data" }
  ],
  "migrations": [
-   { "tag": "v1", "new_classes": ["Lakehouse"] }
+   { "tag": "v1", "new_classes": ["DoLake"] },
+   { "tag": "v2", "renamed_classes": [{ "from": "Lakehouse", "to": "DoLake" }] }
  ]
}
```

#### Type Import Updates

Update your type imports for full TypeScript support:

```typescript
// Before (0.0.x)
// Types were loosely defined or not exported

// After (0.1.x) - comprehensive type exports
import {
  // Core types
  type CDCEvent,
  type CDCBatchMessage,
  type ConnectMessage,
  type AckMessage,
  type NackMessage,

  // Configuration types
  type DoLakeConfig,
  type RateLimitConfig,
  type ScalingConfig,
  type CompactionConfig,

  // Result types
  type FlushResult,
  type BufferStats,
  type ParallelWriteResult,

  // Schemas for validation
  CDCBatchMessageSchema,
  ConnectMessageSchema,
  validateClientMessage,
  MessageValidationError,
} from 'dolake';
```

### Migration Checklist

Use this checklist when migrating from 0.0.x to 0.1.x:

- [ ] Update `dolake` package to 0.1.x
- [ ] Update connect messages to include `protocolVersion: 1` and `capabilities`
- [ ] Update CDC batch messages to use `cdc_batch` type with sequence tracking
- [ ] Update CDC events to use strict `CDCEvent` format with `operation` enum
- [ ] Implement NACK handling for rate limiting and validation errors
- [ ] Implement backpressure handling using `suggestedDelayMs`
- [ ] Update wrangler.toml with DO migration tag
- [ ] Update import paths to use main package export
- [ ] Add TypeScript types for configuration and messages
- [ ] Test reconnection logic with new protocol version

### Future Migration Notes (0.1.x to 1.0.0)

The following changes are expected before the 1.0.0 stable release:

- **Binary protocol**: The `binaryProtocol` capability will be fully implemented
- **Compression**: Built-in message compression support
- **Schema evolution**: Automatic schema migration for CDC events
- **Multi-region**: Cross-region replication support

Monitor the [CHANGELOG](./CHANGELOG.md) for breaking changes in each release.

## Requirements

- Cloudflare Workers with Durable Objects
- R2 bucket for lakehouse storage
- TypeScript 5.3+

## License

MIT
