# DoLake

**DoLake** is a lakehouse Durable Object for Cloudflare Workers that receives CDC (Change Data Capture) streams from DoSQL instances and writes them to R2 as Parquet files with Iceberg metadata. This enables external query engines like Spark, DuckDB, Trino, and Flink to query your data using the standard Iceberg REST Catalog API.

## Features

- **CDC Streaming**: Receives CDC events from DoSQL shards via WebSocket with hibernation support (95% cost reduction)
- **Parquet Writing**: Converts CDC events to Parquet format with schema inference
- **Iceberg Metadata**: Maintains full Iceberg table metadata including snapshots, manifests, and schema evolution
- **REST Catalog API**: Exposes standard Iceberg REST Catalog for external query engines
- **Buffer Management**: Intelligent batching by table/partition with configurable flush thresholds
- **Deduplication**: Built-in deduplication to handle at-least-once delivery
- **Fallback Storage**: Local DO storage fallback when R2 writes fail
- **Metrics**: Prometheus-compatible metrics endpoint

## Installation

```bash
npm install @dotdo/dolake
```

## Quick Start

### 1. Configure Wrangler

Add DoLake to your `wrangler.jsonc`:

```jsonc
{
  "durable_objects": {
    "bindings": [
      { "name": "DOLAKE", "class_name": "DoLake" }
    ]
  },
  "r2_buckets": [
    { "binding": "LAKEHOUSE_BUCKET", "bucket_name": "my-lakehouse" }
  ],
  "migrations": [
    { "tag": "v1", "new_classes": ["DoLake"] }
  ]
}
```

### 2. Export the Durable Object

In your worker entry point:

```typescript
import { DoLake } from '@dotdo/dolake';

export { DoLake };

export default {
  async fetch(request: Request, env: Env) {
    const url = new URL(request.url);

    // Route requests to DoLake
    if (url.pathname.startsWith('/lakehouse')) {
      const id = env.DOLAKE.idFromName('default');
      const stub = env.DOLAKE.get(id);
      return stub.fetch(request);
    }

    return new Response('Not Found', { status: 404 });
  }
};
```

### 3. Connect DoSQL to DoLake

From your DoSQL instance, establish a WebSocket connection:

```typescript
// In DoSQL Durable Object
async connectToLakehouse(env: Env) {
  const dolakeId = env.DOLAKE.idFromName('default');
  const dolakeStub = env.DOLAKE.get(dolakeId);

  const response = await dolakeStub.fetch('https://dolake/ws', {
    headers: {
      'Upgrade': 'websocket',
      'X-Client-ID': this.ctx.id.toString(),
      'X-Shard-Name': 'users-shard-1',
    },
  });

  const ws = response.webSocket;
  ws.accept();

  // Send CDC events
  ws.send(JSON.stringify({
    type: 'cdc_batch',
    sourceDoId: this.ctx.id.toString(),
    events: cdcEvents,
    sequenceNumber: this.sequenceNumber++,
    timestamp: Date.now(),
  }));
}
```

### 4. Query with External Engines

Configure your query engine to use the REST Catalog:

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

## Configuration

DoLake can be configured via the `DoLakeConfig` interface:

```typescript
interface DoLakeConfig {
  // R2 Settings
  r2BucketName: string;           // Default: 'lakehouse-data'
  r2BasePath: string;             // Default: 'warehouse'

  // Flush Thresholds
  flushThresholdEvents: number;   // Default: 10,000
  flushThresholdBytes: number;    // Default: 32MB
  flushThresholdMs: number;       // Default: 60,000 (1 minute)
  flushIntervalMs: number;        // Default: 30,000 (30 seconds)

  // Buffer Settings
  maxBufferSize: number;          // Default: 128MB
  enableFallback: boolean;        // Default: true
  maxFallbackSize: number;        // Default: 64MB

  // Deduplication
  enableDeduplication: boolean;   // Default: true
  deduplicationWindowMs: number;  // Default: 300,000 (5 minutes)

  // Parquet Settings
  parquetRowGroupSize: number;    // Default: 100,000
  parquetCompression: 'none' | 'snappy' | 'gzip' | 'zstd';  // Default: 'snappy'
}
```

## Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/ws` | WebSocket | CDC streaming connection |
| `/v1/*` | Various | Iceberg REST Catalog API |
| `/status` | GET | Buffer and connection status |
| `/flush` | POST | Trigger manual flush |
| `/health` | GET | Health check |
| `/metrics` | GET | Prometheus metrics |

## CDC Event Format

```typescript
interface CDCEvent {
  sequence: number;        // Monotonically increasing sequence
  timestamp: number;       // Unix timestamp in milliseconds
  operation: 'INSERT' | 'UPDATE' | 'DELETE';
  table: string;           // Table name
  rowId: string;           // Primary key/row identifier
  before?: unknown;        // Previous row data (UPDATE/DELETE)
  after?: unknown;         // New row data (INSERT/UPDATE)
  metadata?: Record<string, unknown>;  // Optional metadata
}
```

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
