# DoLake REST API Reference

DoLake exposes two categories of endpoints:

1. **Management API** - For monitoring and controlling DoLake
2. **Iceberg REST Catalog API** - Standard Iceberg catalog interface for external query engines

## Base URL

All endpoints are relative to your DoLake worker URL:

```
https://your-worker.workers.dev/lakehouse
```

---

## Management API

### Health Check

Check if DoLake is running.

```
GET /health
```

**Response**

```
200 OK

OK
```

---

### Status

Get detailed status of DoLake including buffer state and connections.

```
GET /status
```

**Response**

```json
{
  "state": "receiving",
  "buffer": {
    "batchCount": 5,
    "eventCount": 450,
    "totalSizeBytes": 156832,
    "utilization": 0.0012,
    "oldestBatchTime": 1705840000000,
    "newestBatchTime": 1705840005000
  },
  "connectedSources": 3,
  "sourceStates": [
    {
      "id": "abc123",
      "sourceShardName": "users-shard-1",
      "lastReceivedSequence": 42,
      "lastAckedSequence": 41,
      "connectedAt": 1705830000000,
      "lastActivityAt": 1705840005000,
      "batchesReceived": 15,
      "eventsReceived": 1500
    }
  ],
  "dedupStats": {
    "totalChecks": 150,
    "duplicatesFound": 3,
    "entriesTracked": 147
  }
}
```

**Response Fields**

| Field | Type | Description |
|-------|------|-------------|
| `state` | string | Current state: `idle`, `receiving`, `flushing`, `recovering`, `error` |
| `buffer.batchCount` | number | Number of batches in buffer |
| `buffer.eventCount` | number | Total events in buffer |
| `buffer.totalSizeBytes` | number | Estimated buffer size in bytes |
| `buffer.utilization` | number | Buffer utilization ratio (0-1) |
| `connectedSources` | number | Number of connected DoSQL instances |
| `sourceStates` | array | State of each connected source |
| `dedupStats` | object | Deduplication statistics |

---

### Manual Flush

Trigger an immediate flush of the buffer to R2.

```
POST /flush
```

**Response**

```json
{
  "success": true,
  "batchesFlushed": 5,
  "eventsFlushed": 450,
  "bytesWritten": 156832,
  "paths": [
    "warehouse/default/users/data/dt=2026-01-21/abc123.parquet",
    "warehouse/default/orders/data/def456.parquet"
  ],
  "durationMs": 234,
  "usedFallback": false
}
```

**Response Fields**

| Field | Type | Description |
|-------|------|-------------|
| `success` | boolean | Whether flush completed successfully |
| `batchesFlushed` | number | Number of batches flushed |
| `eventsFlushed` | number | Total events written |
| `bytesWritten` | number | Bytes written to R2 |
| `paths` | string[] | Paths of written Parquet files |
| `durationMs` | number | Flush duration in milliseconds |
| `usedFallback` | boolean | Whether fallback storage was used |
| `error` | string | Error message if `success` is false |

---

### Metrics

Get Prometheus-compatible metrics.

```
GET /metrics
```

**Response**

```
Content-Type: text/plain

# HELP dolake_buffer_events Number of events in buffer
# TYPE dolake_buffer_events gauge
dolake_buffer_events 450

# HELP dolake_buffer_bytes Buffer size in bytes
# TYPE dolake_buffer_bytes gauge
dolake_buffer_bytes 156832

# HELP dolake_buffer_utilization Buffer utilization ratio
# TYPE dolake_buffer_utilization gauge
dolake_buffer_utilization 0.0012

# HELP dolake_connected_sources Number of connected source DOs
# TYPE dolake_connected_sources gauge
dolake_connected_sources 3

# HELP dolake_dedup_checks Total deduplication checks
# TYPE dolake_dedup_checks counter
dolake_dedup_checks 150

# HELP dolake_dedup_duplicates Total duplicates found
# TYPE dolake_dedup_duplicates counter
dolake_dedup_duplicates 3
```

---

## Iceberg REST Catalog API

DoLake implements the [Iceberg REST Catalog specification](https://iceberg.apache.org/spec/#rest-catalog). All catalog endpoints are prefixed with `/v1/`.

---

### Configuration

Get catalog configuration.

```
GET /v1/config
```

**Response**

```json
{
  "overrides": {},
  "defaults": {
    "warehouse": "r2://lakehouse-bucket/warehouse",
    "catalog-impl": "org.apache.iceberg.rest.RESTCatalog"
  }
}
```

---

## Namespace Operations

### List Namespaces

List all namespaces in the catalog.

```
GET /v1/namespaces
```

**Query Parameters**

| Parameter | Type | Description |
|-----------|------|-------------|
| `parent` | string | Optional parent namespace (URL encoded, levels separated by `%1F`) |

**Response**

```json
{
  "namespaces": [
    ["default"],
    ["analytics"],
    ["production", "users"]
  ]
}
```

---

### Create Namespace

Create a new namespace.

```
POST /v1/namespaces
```

**Request Body**

```json
{
  "namespace": ["production", "users"],
  "properties": {
    "owner": "data-team",
    "description": "Production user data"
  }
}
```

**Response**

```json
{
  "namespace": ["production", "users"],
  "properties": {
    "owner": "data-team",
    "description": "Production user data"
  }
}
```

**Error Responses**

| Status | Error | Description |
|--------|-------|-------------|
| 409 | AlreadyExistsError | Namespace already exists |

---

### Get Namespace

Get namespace properties.

```
GET /v1/namespaces/{namespace}
```

**Path Parameters**

| Parameter | Type | Description |
|-----------|------|-------------|
| `namespace` | string | URL encoded namespace (levels separated by `%1F`) |

**Example**

```
GET /v1/namespaces/production%1Fusers
```

**Response**

```json
{
  "namespace": ["production", "users"],
  "properties": {
    "owner": "data-team",
    "description": "Production user data"
  }
}
```

**Error Responses**

| Status | Error | Description |
|--------|-------|-------------|
| 404 | NoSuchNamespaceError | Namespace not found |

---

### Check Namespace Exists

Check if a namespace exists.

```
HEAD /v1/namespaces/{namespace}
```

**Response**

- `204 No Content` - Namespace exists
- `404 Not Found` - Namespace does not exist

---

### Update Namespace Properties

Update namespace properties.

```
POST /v1/namespaces/{namespace}/properties
```

**Request Body**

```json
{
  "updates": {
    "owner": "platform-team",
    "contact": "platform@example.com"
  },
  "removals": ["description"]
}
```

**Response**

```json
{
  "updated": ["owner", "contact"],
  "removed": ["description"],
  "missing": []
}
```

---

### Delete Namespace

Delete an empty namespace.

```
DELETE /v1/namespaces/{namespace}
```

**Response**

- `204 No Content` - Successfully deleted

**Error Responses**

| Status | Error | Description |
|--------|-------|-------------|
| 404 | NoSuchNamespaceError | Namespace not found |
| 409 | NamespaceNotEmptyError | Namespace contains tables |

---

## Table Operations

### List Tables

List all tables in a namespace.

```
GET /v1/namespaces/{namespace}/tables
```

**Response**

```json
{
  "identifiers": [
    {
      "namespace": ["default"],
      "name": "users"
    },
    {
      "namespace": ["default"],
      "name": "orders"
    }
  ]
}
```

---

### Create Table

Create a new Iceberg table.

```
POST /v1/namespaces/{namespace}/tables
```

**Request Body**

```json
{
  "name": "users",
  "schema": {
    "type": "struct",
    "schema-id": 0,
    "fields": [
      {"id": 1, "name": "id", "type": "string", "required": true},
      {"id": 2, "name": "name", "type": "string", "required": false},
      {"id": 3, "name": "email", "type": "string", "required": false},
      {"id": 4, "name": "created_at", "type": "timestamptz", "required": false}
    ],
    "identifier-field-ids": [1]
  },
  "partition-spec": {
    "spec-id": 0,
    "fields": [
      {
        "source-id": 4,
        "field-id": 1000,
        "name": "created_day",
        "transform": "day"
      }
    ]
  },
  "write-order": {
    "order-id": 0,
    "fields": [
      {
        "source-id": 4,
        "direction": "desc",
        "null-order": "nulls-last",
        "transform": "identity"
      }
    ]
  },
  "properties": {
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "snappy"
  }
}
```

**Response**

```json
{
  "metadata-location": "r2://lakehouse-bucket/warehouse/default/users/metadata/v1.metadata.json",
  "metadata": {
    "format-version": 2,
    "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
    "location": "warehouse/default/users",
    "last-sequence-number": 0,
    "last-updated-ms": 1705840000000,
    "schemas": [...],
    "partition-specs": [...],
    "sort-orders": [...],
    "current-snapshot-id": null,
    "snapshots": []
  }
}
```

**Error Responses**

| Status | Error | Description |
|--------|-------|-------------|
| 404 | NoSuchNamespaceError | Parent namespace not found |
| 409 | AlreadyExistsError | Table already exists |

---

### Load Table

Load table metadata.

```
GET /v1/namespaces/{namespace}/tables/{table}
```

**Response**

```json
{
  "metadata-location": "r2://lakehouse-bucket/warehouse/default/users/metadata/v3.metadata.json",
  "metadata": {
    "format-version": 2,
    "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
    "location": "warehouse/default/users",
    "last-sequence-number": 3,
    "last-updated-ms": 1705840300000,
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
    "partition-specs": [
      {"spec-id": 0, "fields": []}
    ],
    "sort-orders": [
      {"order-id": 0, "fields": []}
    ],
    "current-snapshot-id": 1705840300000000123,
    "snapshots": [
      {
        "snapshot-id": 1705840100000000001,
        "parent-snapshot-id": null,
        "sequence-number": 1,
        "timestamp-ms": 1705840100000,
        "manifest-list": "warehouse/default/users/metadata/snap-1705840100000000001-manifest-list.avro",
        "summary": {
          "operation": "append",
          "added-data-files": "2",
          "added-records": "5000"
        }
      }
    ],
    "snapshot-log": [...]
  }
}
```

**Error Responses**

| Status | Error | Description |
|--------|-------|-------------|
| 404 | NoSuchTableError | Table not found |

---

### Check Table Exists

Check if a table exists.

```
HEAD /v1/namespaces/{namespace}/tables/{table}
```

**Response**

- `204 No Content` - Table exists
- `404 Not Found` - Table does not exist

---

### Commit Table Updates

Commit changes to a table (add snapshots, update schema, etc.).

```
POST /v1/namespaces/{namespace}/tables/{table}
```

**Request Body**

```json
{
  "requirements": [
    {
      "type": "assert-ref-snapshot-id",
      "ref": "main",
      "snapshot-id": 1705840200000000002
    }
  ],
  "updates": [
    {
      "action": "add-snapshot",
      "snapshot": {
        "snapshot-id": 1705840300000000003,
        "parent-snapshot-id": 1705840200000000002,
        "sequence-number": 3,
        "timestamp-ms": 1705840300000,
        "manifest-list": "warehouse/default/users/metadata/snap-1705840300000000003-manifest-list.avro",
        "summary": {
          "operation": "append",
          "added-data-files": "1",
          "added-records": "1000"
        }
      }
    },
    {
      "action": "set-snapshot-ref",
      "ref-name": "main",
      "snapshot-id": 1705840300000000003,
      "type": "branch"
    }
  ]
}
```

**Update Actions**

| Action | Description |
|--------|-------------|
| `add-snapshot` | Add a new snapshot |
| `set-snapshot-ref` | Update a branch/tag reference |
| `set-properties` | Add/update table properties |
| `remove-properties` | Remove table properties |
| `add-schema` | Add a new schema version |
| `set-current-schema` | Set the current schema |
| `add-partition-spec` | Add a partition spec |
| `set-default-spec` | Set the default partition spec |
| `add-sort-order` | Add a sort order |
| `set-default-sort-order` | Set the default sort order |
| `set-location` | Update table location |

**Response**

```json
{
  "metadata-location": "r2://lakehouse-bucket/warehouse/default/users/metadata/v4.metadata.json",
  "metadata": {
    "format-version": 2,
    "table-uuid": "550e8400-e29b-41d4-a716-446655440000",
    "current-snapshot-id": 1705840300000000003,
    ...
  }
}
```

**Error Responses**

| Status | Error | Description |
|--------|-------|-------------|
| 404 | NoSuchTableError | Table not found |
| 409 | CommitFailedException | Requirement not met (optimistic concurrency conflict) |

---

### Delete Table

Delete a table.

```
DELETE /v1/namespaces/{namespace}/tables/{table}
```

**Query Parameters**

| Parameter | Type | Description |
|-----------|------|-------------|
| `purgeRequested` | boolean | If true, also delete data files |

**Response**

- `204 No Content` - Successfully deleted

**Error Responses**

| Status | Error | Description |
|--------|-------|-------------|
| 404 | NoSuchTableError | Table not found |

---

## WebSocket Protocol

DoLake accepts WebSocket connections for CDC streaming at the root path.

### Connection

```
GET /ws
Upgrade: websocket
X-Client-ID: <source-do-id>
X-Shard-Name: <optional-shard-name>
```

### Message Types

#### Client to Server

**Connect Message**

```json
{
  "type": "connect",
  "timestamp": 1705840000000,
  "sourceDoId": "abc123",
  "sourceShardName": "users-shard-1",
  "lastAckSequence": 42,
  "protocolVersion": 1,
  "capabilities": {
    "binaryProtocol": false,
    "compression": false,
    "batching": true,
    "maxBatchSize": 1000,
    "maxMessageSize": 4194304
  }
}
```

**CDC Batch Message**

```json
{
  "type": "cdc_batch",
  "timestamp": 1705840001000,
  "correlationId": "req-123",
  "sourceDoId": "abc123",
  "sourceShardName": "users-shard-1",
  "events": [
    {
      "sequence": 100,
      "timestamp": 1705840000500,
      "operation": "INSERT",
      "table": "users",
      "rowId": "user-1",
      "after": {
        "id": "user-1",
        "name": "John Doe",
        "email": "john@example.com"
      }
    }
  ],
  "sequenceNumber": 43,
  "firstEventSequence": 100,
  "lastEventSequence": 105,
  "sizeBytes": 1024,
  "isRetry": false,
  "retryCount": 0
}
```

**Heartbeat Message**

```json
{
  "type": "heartbeat",
  "timestamp": 1705840030000,
  "sourceDoId": "abc123",
  "lastAckSequence": 43,
  "pendingEvents": 0
}
```

**Flush Request Message**

```json
{
  "type": "flush_request",
  "timestamp": 1705840060000,
  "correlationId": "flush-1",
  "sourceDoId": "abc123",
  "reason": "manual"
}
```

#### Server to Client

**ACK Message**

```json
{
  "type": "ack",
  "timestamp": 1705840001100,
  "correlationId": "req-123",
  "sequenceNumber": 43,
  "status": "ok",
  "details": {
    "eventsProcessed": 6,
    "bufferUtilization": 0.15,
    "timeUntilFlush": 45000
  }
}
```

**ACK Status Values**

| Status | Description |
|--------|-------------|
| `ok` | Batch accepted and buffered |
| `buffered` | Batch buffered but buffer utilization is high |
| `persisted` | Batch already persisted to R2 |
| `duplicate` | Batch was a duplicate (already processed) |
| `fallback` | Batch stored in fallback storage |

**NACK Message**

```json
{
  "type": "nack",
  "timestamp": 1705840001100,
  "sequenceNumber": 43,
  "reason": "buffer_full",
  "errorMessage": "Buffer is full: 134217728 > 134217728",
  "shouldRetry": true,
  "retryDelayMs": 5000
}
```

**NACK Reason Values**

| Reason | Retry | Description |
|--------|-------|-------------|
| `buffer_full` | Yes | Buffer at maximum capacity |
| `rate_limited` | Yes | Too many requests |
| `invalid_sequence` | No | Invalid sequence number |
| `invalid_format` | No | Malformed message |
| `internal_error` | Yes | Server-side error |
| `shutting_down` | Yes | DoLake is shutting down |

**Status Message**

```json
{
  "type": "status",
  "timestamp": 1705840001100,
  "state": "receiving",
  "buffer": {
    "batchCount": 10,
    "eventCount": 1000,
    "totalSizeBytes": 524288,
    "utilization": 0.004
  },
  "connectedSources": 3,
  "lastFlushTime": 1705839960000,
  "nextFlushTime": 1705840020000
}
```

**Pong Message** (heartbeat response)

```json
{
  "type": "pong",
  "timestamp": 1705840030000,
  "serverTime": 1705840030050
}
```

**Flush Response**

```json
{
  "type": "flush_response",
  "timestamp": 1705840065000,
  "correlationId": "flush-1",
  "result": {
    "success": true,
    "batchesFlushed": 10,
    "eventsFlushed": 1000,
    "bytesWritten": 524288,
    "paths": ["..."],
    "durationMs": 234,
    "usedFallback": false
  }
}
```

---

## Error Responses

All error responses follow this format:

```json
{
  "error": {
    "message": "Namespace not found: production.users",
    "type": "NoSuchNamespaceError",
    "code": 404
  }
}
```

**Common Error Types**

| Type | Code | Description |
|------|------|-------------|
| `NoSuchNamespaceError` | 404 | Namespace does not exist |
| `NoSuchTableError` | 404 | Table does not exist |
| `AlreadyExistsError` | 409 | Resource already exists |
| `NamespaceNotEmptyError` | 409 | Namespace contains tables |
| `CommitFailedException` | 409 | Optimistic concurrency conflict |
| `InternalError` | 500 | Server-side error |
