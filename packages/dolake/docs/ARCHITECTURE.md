# DoLake Architecture

This document describes the architecture and design of DoLake, a lakehouse component that bridges real-time CDC streams from DoSQL to the open Iceberg table format on Cloudflare R2.

## System Overview

```
+-------------------------------------------------------------------------+
|                        DoSQL Instances (Shards)                          |
|  +----------+  +----------+  +----------+  +----------+  +----------+   |
|  | Shard 1  |  | Shard 2  |  | Shard 3  |  | Shard N  |  |   ...    |   |
|  |  (JSON)  |  |  (JSON)  |  |  (JSON)  |  |  (JSON)  |  |          |   |
|  +----+-----+  +----+-----+  +----+-----+  +----+-----+  +----+-----+   |
|       |             |             |             |             |          |
|       | WebSocket   | Hibernation | (95% cost  discount)     |          |
|       v             v             v             v             v          |
|  +------------------------------------------------------------------+   |
|  |                       DoLake (Aggregator DO)                      |   |
|  |                                                                    |   |
|  |  +-------------------+  +-------------------+  +----------------+  |   |
|  |  | CDC Buffer Mgr    |  | Parquet Writer    |  | REST Catalog   |  |   |
|  |  | - Table buffers   |  | - Schema infer    |  | - Iceberg API  |  |   |
|  |  | - Deduplication   |  | - Column encode   |  | - Namespaces   |  |   |
|  |  | - Flush triggers  |  | - Statistics      |  | - Tables       |  |   |
|  |  +-------------------+  +-------------------+  +----------------+  |   |
|  |                                                                    |   |
|  |  +-------------------+  +-------------------+                      |   |
|  |  | Iceberg Metadata  |  | R2 Storage        |                      |   |
|  |  | - Snapshots       |  | - Data files      |                      |   |
|  |  | - Manifests       |  | - Metadata files  |                      |   |
|  |  | - Schema history  |  | - Manifest lists  |                      |   |
|  |  +-------------------+  +-------------------+                      |   |
|  +------------------------------------------------------------------+   |
|                                    |                                     |
|                                    v                                     |
|                              +-----------+                               |
|                              |    R2     |                               |
|                              | (Iceberg) |                               |
|                              +-----------+                               |
|                                    |                                     |
|                                    v                                     |
|  +------------------------------------------------------------------+   |
|  |                   External Query Engines                          |   |
|  |   Spark   |   DuckDB   |   Trino   |   Flink   |   DataFusion    |   |
|  +------------------------------------------------------------------+   |
+-------------------------------------------------------------------------+
```

## Core Components

### 1. DoLake Durable Object

The main Durable Object that orchestrates all lakehouse operations:

```
+-------------------------------------------------------------------+
|                         DoLake DO                                  |
|                                                                    |
|  State:                                                            |
|  +------------------+  +------------------+  +------------------+  |
|  | idle             |  | receiving        |  | flushing         |  |
|  | No active work   |  | Processing CDC   |  | Writing to R2    |  |
|  +------------------+  +------------------+  +------------------+  |
|          |                    |                    |               |
|          +--------------------+--------------------+               |
|                               |                                    |
|  Components:                  v                                    |
|  +------------------+  +------------------+  +------------------+  |
|  | CDCBufferManager |  | R2IcebergStorage |  | RestCatalogHndlr|  |
|  +------------------+  +------------------+  +------------------+  |
+-------------------------------------------------------------------+
```

### 2. CDC Buffer Manager

Manages buffering, batching, and deduplication of incoming CDC events:

```
                          CDC Events In
                               |
                               v
                    +---------------------+
                    | Deduplication Check |
                    | (batch:sourceId:seq)|
                    +----------+----------+
                               |
              duplicate?       |
             +--------+--------+--------+
             |                          |
             v                          v
       +----------+            +---------------+
       | Skip/ACK |            | Add to Batch  |
       +----------+            +-------+-------+
                                       |
                                       v
                    +---------------------+
                    | Distribute to       |
                    | Partition Buffers   |
                    +----------+----------+
                               |
        +----------+-----------+-----------+----------+
        |          |           |           |          |
        v          v           v           v          v
   +--------+ +--------+ +--------+ +--------+ +--------+
   | users  | | orders | | users  | | orders | | events |
   | dt=01  | | dt=01  | | dt=02  | | dt=02  | | (none) |
   +--------+ +--------+ +--------+ +--------+ +--------+
```

**Buffer Structure:**

```
PartitionBuffer {
  table: string           // e.g., "users"
  partitionKey: string    // e.g., "dt=2026-01-21"
  events: CDCEvent[]      // Buffered events
  sizeBytes: number       // Estimated size
  firstEventTime: number  // For age-based flush
  lastEventTime: number
}
```

### 3. Flush Pipeline

When flush triggers fire, data flows through the flush pipeline:

```
                    Flush Trigger
                    (events/size/time)
                          |
                          v
              +------------------------+
              | Get Partition Buffers  |
              +------------------------+
                          |
                          v
              +------------------------+
              | Group by Table         |
              +------------------------+
                          |
           +--------------+--------------+
           |              |              |
           v              v              v
    +------------+  +------------+  +------------+
    | Table: A   |  | Table: B   |  | Table: C   |
    +------------+  +------------+  +------------+
           |              |              |
           v              v              v
    +------------+  +------------+  +------------+
    | Infer/Get  |  | Infer/Get  |  | Infer/Get  |
    | Schema     |  | Schema     |  | Schema     |
    +------------+  +------------+  +------------+
           |              |              |
           v              v              v
    +------------+  +------------+  +------------+
    | Write      |  | Write      |  | Write      |
    | Parquet    |  | Parquet    |  | Parquet    |
    +------------+  +------------+  +------------+
           |              |              |
           v              v              v
    +------------+  +------------+  +------------+
    | Upload R2  |  | Upload R2  |  | Upload R2  |
    +------------+  +------------+  +------------+
           |              |              |
           +--------------+--------------+
                          |
                          v
              +------------------------+
              | Create Snapshot        |
              | Update Metadata        |
              +------------------------+
                          |
                          v
              +------------------------+
              | Clear Buffers          |
              | Mark Persisted         |
              +------------------------+
```

### 4. WebSocket Connection Management

DoLake uses WebSocket Hibernation for cost-efficient connection handling:

```
+--------------------+                    +--------------------+
|     DoSQL DO       |                    |     DoLake DO      |
|                    |                    |                    |
|  +-------------+   |    Upgrade         |   +-----------+    |
|  | CDC Stream  +---+------------------->+   | Accept    |    |
|  +-------------+   |                    |   | WebSocket |    |
|                    |                    |   +-----+-----+    |
|                    |                    |         |          |
|                    |                    |         v          |
|                    |                    |   +-----------+    |
|                    |    Attachment      |   | Store     |    |
|                    |    (sourceDoId,    |   | Attach-   |    |
|                    |     lastAckSeq,    |   | ment      |    |
|                    |     capabilities)  |   +-----------+    |
|                    |                    |         |          |
|  +-------------+   |    cdc_batch       |         v          |
|  | Send Batch  +---+------------------->+   +-----------+    |
|  +-------------+   |                    |   | Process   |    |
|                    |                    |   | Message   |    |
|                    |    ack/nack        |   +-----+-----+    |
|  +-------------+   |<-------------------+         |          |
|  | Handle ACK  |   |                    |         v          |
|  +-------------+   |                    |   +-----------+    |
|                    |                    |   | Hibernate |    |
|                    |    (idle period)   |   | (95% cost |    |
|                    |                    |   |  savings) |    |
|                    |                    |   +-----------+    |
+--------------------+                    +--------------------+
```

**WebSocket Attachment (survives hibernation):**

```typescript
interface WebSocketAttachment {
  sourceDoId: string;        // Connected DO's ID
  sourceShardName?: string;  // Human-readable name
  lastAckSequence: number;   // For resumption
  connectedAt: number;       // Connection timestamp
  protocolVersion: number;   // Protocol version
  capabilityFlags: number;   // Encoded capabilities
}
```

## Data Flow

### Write Path (CDC to Iceberg)

```
1. DoSQL generates CDC event
   |
   v
2. WebSocket sends cdc_batch message
   |
   v
3. DoLake receives and validates
   |
   v
4. Deduplication check (batch:source:seq)
   |
   v
5. Add to CDCBufferManager
   |
   v
6. Distribute to partition buffers
   |
   v
7. Check flush triggers
   |
   +---> Not triggered: Send ACK, wait
   |
   v (triggered)
8. Write Parquet files to R2
   |
   v
9. Create manifest entries
   |
   v
10. Create new snapshot
    |
    v
11. Update table metadata in R2
    |
    v
12. Clear buffers, mark persisted
```

### Read Path (Query Engine)

```
1. External engine connects to REST Catalog
   |
   v
2. GET /v1/config - Get catalog configuration
   |
   v
3. GET /v1/namespaces - List available namespaces
   |
   v
4. GET /v1/namespaces/{ns}/tables - List tables
   |
   v
5. GET /v1/namespaces/{ns}/tables/{table} - Load table metadata
   |
   v
6. Parse metadata for current snapshot
   |
   v
7. Read manifest list from R2
   |
   v
8. Read manifest files from R2
   |
   v
9. Read Parquet data files from R2
   |
   v
10. Execute query with predicate pushdown
```

## Storage Layout

DoLake uses the standard Iceberg storage layout in R2:

```
r2://lakehouse-bucket/
└── warehouse/                          # Base path
    ├── default/                        # Namespace
    │   ├── users/                      # Table
    │   │   ├── metadata/
    │   │   │   ├── v1.metadata.json    # Initial metadata
    │   │   │   ├── v2.metadata.json    # After first flush
    │   │   │   ├── v3.metadata.json    # After second flush
    │   │   │   ├── snap-{id}-manifest-list.avro
    │   │   │   └── {uuid}-manifest.avro
    │   │   └── data/
    │   │       ├── dt=2026-01-20/
    │   │       │   ├── {uuid}.parquet
    │   │       │   └── {uuid}.parquet
    │   │       └── dt=2026-01-21/
    │   │           └── {uuid}.parquet
    │   └── orders/                     # Another table
    │       ├── metadata/
    │       └── data/
    └── analytics/                      # Another namespace
        └── events/
            ├── metadata/
            └── data/
```

## Iceberg Metadata Structure

### Table Metadata

```json
{
  "format-version": 2,
  "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
  "location": "warehouse/default/users",
  "last-sequence-number": 5,
  "last-updated-ms": 1705840000000,
  "last-column-id": 8,
  "current-schema-id": 0,
  "schemas": [
    {
      "type": "struct",
      "schema-id": 0,
      "fields": [
        {"id": 1, "name": "_cdc_sequence", "type": "long", "required": true},
        {"id": 2, "name": "_cdc_timestamp", "type": "timestamptz", "required": true},
        {"id": 3, "name": "_cdc_operation", "type": "string", "required": true},
        {"id": 4, "name": "_cdc_row_id", "type": "string", "required": true},
        {"id": 5, "name": "id", "type": "string", "required": false},
        {"id": 6, "name": "name", "type": "string", "required": false},
        {"id": 7, "name": "email", "type": "string", "required": false},
        {"id": 8, "name": "created_at", "type": "timestamptz", "required": false}
      ]
    }
  ],
  "current-snapshot-id": 1705840000000000123,
  "snapshots": [...],
  "snapshot-log": [...]
}
```

### Snapshot

```json
{
  "snapshot-id": 1705840000000000123,
  "parent-snapshot-id": null,
  "sequence-number": 1,
  "timestamp-ms": 1705840000000,
  "manifest-list": "warehouse/default/users/metadata/snap-1705840000000000123-manifest-list.avro",
  "summary": {
    "operation": "append",
    "added-data-files": "3",
    "added-records": "15000",
    "added-files-size": "1048576"
  }
}
```

## Error Handling & Recovery

### Fallback Storage

When R2 writes fail, DoLake uses local DO storage as fallback:

```
             Normal Path              Fallback Path
                  |                        |
                  v                        v
         +---------------+        +---------------+
         | Write to R2   |        | Write to DO   |
         +-------+-------+        | Storage       |
                 |                +-------+-------+
                 |                        |
          success|              recovery  |
                 |                alarm   |
                 v                        v
         +---------------+        +---------------+
         | Update Meta   |        | Retry to R2   |
         +---------------+        +---------------+
```

### Recovery Flow

```
1. Alarm fires (scheduled)
   |
   v
2. Check for fallback events
   |
   v
3. Load events from DO storage
   |
   v
4. Retry flush to R2
   |
   +---> Success: Clear fallback storage
   |
   +---> Failure: Keep in fallback, schedule retry
```

## Message Protocol

### Client to DoLake Messages

| Type | Description | Fields |
|------|-------------|--------|
| `cdc_batch` | CDC events batch | sourceDoId, events, sequenceNumber |
| `connect` | Connection init | sourceDoId, capabilities, lastAckSequence |
| `heartbeat` | Keep-alive | sourceDoId, lastAckSequence |
| `flush_request` | Request flush | sourceDoId, reason |

### DoLake to Client Messages

| Type | Description | Fields |
|------|-------------|--------|
| `ack` | Batch accepted | sequenceNumber, status, details |
| `nack` | Batch rejected | sequenceNumber, reason, shouldRetry |
| `status` | Status update | state, buffer, connectedSources |
| `pong` | Heartbeat response | timestamp, serverTime |

### ACK Status Values

| Status | Description |
|--------|-------------|
| `ok` | Batch processed successfully |
| `buffered` | Batch added to buffer (high utilization) |
| `persisted` | Batch written to R2 |
| `duplicate` | Batch already processed |
| `fallback` | Batch in fallback storage |

### NACK Reasons

| Reason | Retry? | Description |
|--------|--------|-------------|
| `buffer_full` | Yes | Buffer at capacity |
| `rate_limited` | Yes | Too many requests |
| `invalid_sequence` | No | Sequence number issue |
| `invalid_format` | No | Malformed message |
| `internal_error` | Yes | Server-side error |
| `shutting_down` | Yes | DO is shutting down |

## Performance Considerations

### Buffer Sizing

```
Recommended Buffer Configuration:
+----------------------------+------------------+
| Workload                   | maxBufferSize    |
+----------------------------+------------------+
| Low volume (<1K events/s)  | 32 MB            |
| Medium (1K-10K events/s)   | 64 MB            |
| High (>10K events/s)       | 128 MB           |
+----------------------------+------------------+

Flush Thresholds:
+----------------------------+------------------+
| Threshold Type             | Recommended      |
+----------------------------+------------------+
| Event count                | 10,000           |
| Size                       | 32 MB            |
| Time                       | 60 seconds       |
+----------------------------+------------------+
```

### WebSocket Hibernation

DoLake uses WebSocket Hibernation to minimize costs during idle periods:

- Active connections: Full DO compute costs
- Hibernating connections: 95% cost reduction
- Automatic hibernation after message processing
- Instant wake-up on new messages

### Parquet Optimization

```
Parquet Configuration:
+----------------------------+------------------+
| Setting                    | Default          |
+----------------------------+------------------+
| Row group size             | 100,000 rows     |
| Page size                  | 1 MB             |
| Compression                | Snappy           |
| Dictionary encoding        | Enabled          |
| Statistics                 | Enabled          |
+----------------------------+------------------+
```

## Scaling

DoLake is designed for horizontal scaling through multiple instances:

```
                     +------------------+
                     |  Load Balancer   |
                     +--------+---------+
                              |
       +----------------------+----------------------+
       |                      |                      |
       v                      v                      v
+-------------+        +-------------+        +-------------+
| DoLake      |        | DoLake      |        | DoLake      |
| Instance A  |        | Instance B  |        | Instance C  |
| (users,     |        | (orders,    |        | (events,    |
|  products)  |        |  inventory) |        |  logs)      |
+------+------+        +------+------+        +------+------+
       |                      |                      |
       +----------------------+----------------------+
                              |
                              v
                    +-----------------+
                    |   R2 Bucket     |
                    | (Shared Store)  |
                    +-----------------+
```

**Partitioning Strategies:**

1. **By Table**: Each DoLake handles specific tables
2. **By Namespace**: Each DoLake handles a namespace
3. **By Shard Range**: Each DoLake handles specific DoSQL shards
