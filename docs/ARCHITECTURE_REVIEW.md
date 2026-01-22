# DoSQL & DoLake Architecture Review

**Date**: 2026-01-22
**Reviewer**: Claude Code (Opus 4.5)
**Repository**: `/Users/nathanclevenger/projects/sql`
**Status**: Implementation Complete - 4 Core Packages

---

## Executive Summary

The `@dotdo/sql` monorepo implements a complete edge-native database stack for Cloudflare Workers with four packages:

| Package | Role | Dependencies |
|---------|------|--------------|
| **@dotdo/sql.do** | Client SDK for DoSQL | None |
| **@dotdo/lake.do** | Client SDK for DoLake | `@dotdo/sql.do` |
| **@dotdo/dosql** | Server-side DO (SQL engine) | `@dotdo/sql.do` |
| **@dotdo/dolake** | Server-side DO (Lakehouse) | `@dotdo/lake.do` |

The architecture follows a clean client/server separation with shared types and CapnWeb RPC for communication.

---

## Package Dependency Graph

```
                      ┌─────────────────────────────────────────┐
                      │          External Dependencies          │
                      │  ┌─────────────┐    ┌─────────────────┐ │
                      │  │  capnweb    │    │   hyparquet     │ │
                      │  │  (RPC)      │    │  (Parquet)      │ │
                      │  └──────┬──────┘    └────────┬────────┘ │
                      └─────────┼────────────────────┼──────────┘
                                │                    │
         ┌──────────────────────┴────────────────────┴──────────────────┐
         │                                                               │
         ▼                                                               ▼
┌─────────────────┐                                           ┌─────────────────┐
│  @dotdo/sql.do  │                                           │ @dotdo/lake.do  │
│  (Client SDK)   │ ◀──────────── depends on ─────────────────│  (Client SDK)   │
│                 │                                           │                 │
│ - SQLClient     │                                           │ - LakeClient    │
│ - Branded Types │                                           │ - CDC Types     │
│ - RPC Messages  │                                           │ - Query Types   │
└────────┬────────┘                                           └────────┬────────┘
         │                                                              │
         │ depends on                                                   │ depends on
         ▼                                                              ▼
┌─────────────────┐                                           ┌─────────────────┐
│  @dotdo/dosql   │                                           │  @dotdo/dolake  │
│  (Server DO)    │ ─────────── CDC Stream ────────────────▶  │  (Server DO)    │
│                 │              (WebSocket)                  │                 │
│ - SQL Parser    │                                           │ - CDC Buffer    │
│ - B-tree Index  │                                           │ - Parquet Write │
│ - WAL/MVCC      │                                           │ - Iceberg Meta  │
│ - Sharding      │                                           │ - REST Catalog  │
│ - CDC Producer  │                                           │ - Compaction    │
└─────────────────┘                                           └─────────────────┘
```

---

## Module Structure Analysis

### @dotdo/sql.do (Client SDK)

```
packages/sql.do/src/
├── index.ts       # Public exports
├── types.ts       # Branded types, RPC messages, schemas
└── client.ts      # DoSQLClient with CapnWeb WebSocket RPC
```

**Key Design Decisions:**
- Branded types (`TransactionId`, `LSN`, `ShardId`, `StatementHash`) prevent ID confusion
- WebSocket-based RPC with automatic reconnection and retry
- Transaction context pattern for transactional operations
- Prepared statement caching via statement hash

**Exports Analysis:**
```typescript
// Branded Types
TransactionId, LSN, StatementHash, ShardId

// Query Types
SQLValue, QueryResult, PreparedStatement, QueryOptions

// Transaction Types
IsolationLevel, TransactionOptions, TransactionState

// Schema Types
ColumnType, ColumnDefinition, TableSchema, IndexDefinition

// RPC Types
RPCMethod, RPCRequest, RPCResponse, RPCError

// Client Interface
SQLClient, DoSQLClient, TransactionContext, SQLError
```

### @dotdo/lake.do (Client SDK)

```
packages/lake.do/src/
├── index.ts       # Public exports + re-exports from sql.do
├── types.ts       # Lake-specific branded types, CDC streaming
└── client.ts      # DoLakeClient with CDC subscription support
```

**Key Design Decisions:**
- Re-exports common types from `@dotdo/sql.do` for consistency
- AsyncIterable-based CDC subscription for backpressure handling
- Parquet file and snapshot IDs as branded types
- Compaction job management through RPC

**Exports Analysis:**
```typescript
// Lake-specific Branded Types
CDCEventId, PartitionKey, ParquetFileId, SnapshotId, CompactionJobId

// CDC Streaming Types
CDCStreamOptions, CDCBatch, CDCStreamState

// Query Types
LakeQueryOptions, LakeQueryResult

// Partition Types
PartitionStrategy, PartitionConfig, PartitionInfo

// Compaction Types
CompactionConfig, CompactionJob

// Snapshot Types
Snapshot, TimeTravelOptions

// Re-exported from sql.do
TransactionId, LSN, SQLValue, CDCOperation, CDCEvent
```

### @dotdo/dosql (Server DO)

```
packages/dosql/src/
├── index.ts              # Main exports
├── parser.ts             # Type-safe SQL parser
├── aggregates.ts         # Aggregate function parsing
├── database.ts           # Database class (better-sqlite3 compatible)
├── join-parser.ts        # JOIN clause parsing
├── sharding/             # Distributed query execution
│   ├── index.ts          # ShardingClient exports
│   ├── types.ts          # VSchema, Vindex, routing types
│   ├── vindex.ts         # Hash/Range/Consistent hash vindexes
│   ├── router.ts         # Query parsing and routing
│   ├── executor.ts       # Distributed execution
│   └── replica.ts        # Replica selection and health
├── rpc/                  # CapnWeb RPC implementation
│   ├── index.ts          # RPC exports
│   ├── types.ts          # QueryRequest, StreamRequest, CDC types
│   ├── client.ts         # WebSocket/HTTP batch clients
│   └── server.ts         # DoSQLTarget for DO handling
├── wal/                  # Write-Ahead Log
│   ├── index.ts          # WAL exports
│   ├── types.ts          # WALEntry, WALSegment, Checkpoint
│   ├── writer.ts         # WAL writer with CRC32
│   ├── reader.ts         # WAL reader with tail/batch
│   ├── checkpoint.ts     # Checkpoint management
│   └── retention.ts      # WAL segment retention
├── cdc/                  # Change Data Capture
│   ├── index.ts          # CDC exports
│   ├── types.ts          # CDCEvent, Subscription, Stream
│   ├── stream.ts         # Streaming implementation
│   └── capture.ts        # WAL-to-CDC conversion
├── transaction/          # ACID transactions
│   ├── index.ts          # Transaction exports
│   ├── manager.ts        # Transaction lifecycle
│   ├── mvcc.ts           # Multi-version concurrency
│   ├── lock.ts           # Lock management
│   └── savepoint.ts      # Savepoint support
├── btree/                # B-tree index implementation
├── fsx/                  # Storage abstraction
├── proc/                 # Stored procedures (ESM)
├── migrations/           # Schema migrations
├── virtual/              # Virtual tables (URL sources)
└── orm/                  # ORM adapters
    ├── prisma/
    ├── kysely/
    ├── knex/
    └── drizzle/
```

**Key Features:**
- Type-safe SQL parsing with compile-time validation
- Sharding with Vitess-inspired VSchema and Vindexes
- MVCC transactions with 5 isolation levels
- Replication slot-based CDC for exactly-once delivery
- WAL with CRC32 checksums and segment management

### @dotdo/dolake (Server DO)

```
packages/dolake/src/
├── index.ts              # All exports
├── dolake.ts             # Main DoLake Durable Object (2500+ lines)
├── types.ts              # CDC, RPC, Iceberg types
├── schemas.ts            # Zod validation schemas
├── buffer.ts             # CDC event buffering
├── parquet.ts            # Parquet file writing (hyparquet)
├── iceberg.ts            # Iceberg metadata management
├── catalog.ts            # REST Catalog API (Spark/Trino compatible)
├── compaction.ts         # File compaction with space reclamation
├── partitioning.ts       # Partition management (day/hour/bucket)
├── query-engine.ts       # Partition-pruning query execution
├── rate-limiter.ts       # WebSocket connection rate limiting
└── scalability.ts        # Parallel writes, horizontal scaling
```

**Key Features:**
- WebSocket hibernation for 95% cost reduction on idle connections
- Smart buffering with deduplication and partitioning
- Iceberg table format with full manifest/snapshot management
- REST Catalog API compatible with Spark, Trino, DuckDB
- Adaptive compaction with space savings calculation

---

## RPC Protocol Design (CapnWeb)

### Client-to-Gateway Communication

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                           WebSocket RPC Protocol                               │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│   Client                                           DoSQL Gateway               │
│   ┌──────┐                                         ┌──────────────┐           │
│   │      │  ─────────── WebSocket Connect ────────▶│              │           │
│   │      │                                         │              │           │
│   │      │  ─────── JSON-RPC Request ─────────────▶│              │           │
│   │      │  {                                      │              │           │
│   │      │    "id": "1",                           │              │           │
│   │      │    "method": "query",                   │              │           │
│   │      │    "params": {                          │              │           │
│   │      │      "sql": "SELECT * FROM users",      │              │           │
│   │      │      "params": [],                      │              │           │
│   │      │      "transactionId": "tx_abc123"       │              │           │
│   │      │    }                                    │              │           │
│   │      │  }                                      │              │           │
│   │      │                                         │              │           │
│   │      │  ◀─────── JSON-RPC Response ────────────│              │           │
│   │      │  {                                      │              │           │
│   │      │    "id": "1",                           │              │           │
│   │      │    "result": {                          │              │           │
│   │      │      "rows": [...],                     │              │           │
│   │      │      "columns": [...],                  │              │           │
│   │      │      "duration": 42                     │              │           │
│   │      │    }                                    │              │           │
│   │      │  }                                      │              │           │
│   └──────┘                                         └──────────────┘           │
│                                                                               │
├───────────────────────────────────────────────────────────────────────────────┤
│                            RPC Methods                                         │
├───────────────────────────────────────────────────────────────────────────────┤
│  exec           - Execute SQL statement (INSERT, UPDATE, DELETE, DDL)         │
│  query          - Execute SQL query (SELECT)                                   │
│  prepare        - Prepare statement for repeated execution                     │
│  execute        - Execute prepared statement                                   │
│  beginTransaction - Start transaction with isolation level                     │
│  commit         - Commit transaction, returns LSN                              │
│  rollback       - Rollback transaction                                         │
│  getSchema      - Get table schema                                             │
│  ping           - Health check with latency measurement                        │
│  batch          - Execute multiple statements                                  │
└───────────────────────────────────────────────────────────────────────────────┘
```

### DoSQL-to-DoLake CDC Protocol

```
┌───────────────────────────────────────────────────────────────────────────────┐
│                         CDC WebSocket Protocol                                 │
├───────────────────────────────────────────────────────────────────────────────┤
│                                                                               │
│   DoSQL Shard                                      DoLake Aggregator           │
│   ┌──────────┐                                     ┌──────────────┐           │
│   │          │  ─────────── WebSocket Connect ────▶│              │           │
│   │          │              (with hibernation)     │              │           │
│   │          │                                     │              │           │
│   │          │  ─────── connect message ──────────▶│              │           │
│   │          │  {                                  │              │           │
│   │          │    "type": "connect",               │              │           │
│   │          │    "sourceDoId": "shard-1",         │              │           │
│   │          │    "protocolVersion": 1,            │              │           │
│   │          │    "lastAckSequence": 42,           │              │           │
│   │          │    "capabilities": {...}            │              │           │
│   │          │  }                                  │              │           │
│   │          │                                     │              │           │
│   │          │  ─────── cdc_batch message ────────▶│              │           │
│   │          │  {                                  │              │           │
│   │          │    "type": "cdc_batch",             │              │           │
│   │          │    "sequenceNumber": 43,            │              │           │
│   │          │    "events": [{                     │              │           │
│   │          │      "sequence": 1,                 │              │           │
│   │          │      "operation": "INSERT",         │              │           │
│   │          │      "table": "users",              │              │           │
│   │          │      "rowId": "u_123",              │              │           │
│   │          │      "after": {...}                 │              │           │
│   │          │    }]                               │              │           │
│   │          │  }                                  │              │           │
│   │          │                                     │              │           │
│   │          │  ◀─────── ack message ──────────────│              │           │
│   │          │  {                                  │              │           │
│   │          │    "type": "ack",                   │              │           │
│   │          │    "sequenceNumber": 43,            │              │           │
│   │          │    "status": "ok",                  │              │           │
│   │          │    "details": {                     │              │           │
│   │          │      "bufferUtilization": 0.3,      │              │           │
│   │          │      "timeUntilFlush": 5000         │              │           │
│   │          │    },                               │              │           │
│   │          │    "rateLimit": {                   │              │           │
│   │          │      "limit": 1000,                 │              │           │
│   │          │      "remaining": 950,              │              │           │
│   │          │      "resetAt": 1705920000          │              │           │
│   │          │    }                                │              │           │
│   │          │  }                                  │              │           │
│   │          │                                     │              │           │
│   │          │  ◀─────── nack message ─────────────│              │           │
│   │          │  {                                  │              │           │
│   │          │    "type": "nack",                  │              │           │
│   │          │    "reason": "buffer_full",         │              │           │
│   │          │    "shouldRetry": true,             │              │           │
│   │          │    "retryDelayMs": 5000             │              │           │
│   │          │  }                                  │              │           │
│   └──────────┘                                     └──────────────┘           │
│                                                                               │
├───────────────────────────────────────────────────────────────────────────────┤
│                            Message Types                                       │
├───────────────────────────────────────────────────────────────────────────────┤
│  Client → Server:                                                             │
│    connect        - Initiate connection with capabilities                      │
│    cdc_batch      - Batch of CDC events                                        │
│    heartbeat      - Keep connection alive, report pending events               │
│    flush_request  - Request immediate flush to R2                              │
│    disconnect     - Graceful disconnect                                        │
│                                                                               │
│  Server → Client:                                                             │
│    ack            - Acknowledge batch with status                              │
│    nack           - Reject batch with retry info                               │
│    status         - Buffer stats, connected sources                            │
│    pong           - Response to heartbeat                                      │
│    flush_response - Result of flush request                                    │
└───────────────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow Architecture

### Complete Write Path

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              WRITE PATH                                          │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────┐                                                                │
│  │   Client    │                                                                │
│  │ Application │                                                                │
│  └──────┬──────┘                                                                │
│         │  1. SQL INSERT/UPDATE/DELETE                                          │
│         ▼                                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                        DoSQL Gateway (Workers)                           │    │
│  │                                                                          │    │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐ │    │
│  │  │ SQL Parser  │─▶│   Planner   │─▶│  Optimizer  │─▶│  Shard Router   │ │    │
│  │  │             │  │             │  │             │  │ (VSchema-based) │ │    │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └────────┬────────┘ │    │
│  └───────────────────────────────────────────────────────────────┼──────────┘    │
│                                                                  │              │
│         ┌────────────────────────────────────────────────────────┘              │
│         │  2. Route to appropriate shard(s)                                     │
│         ▼                                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                        Shard DO (DoSQL)                                  │    │
│  │                                                                          │    │
│  │  ┌───────────────────────────────────────────────────────────────────┐  │    │
│  │  │ Transaction Manager                                                │  │    │
│  │  │ ┌─────────────────────────────────────────────────────────────┐   │  │    │
│  │  │ │ 3. Begin Transaction (acquire locks, create snapshot)       │   │  │    │
│  │  │ └─────────────────────────────────────────────────────────────┘   │  │    │
│  │  └───────────────────────────────────────────────────────────────────┘  │    │
│  │                           │                                              │    │
│  │                           ▼                                              │    │
│  │  ┌───────────────────────────────────────────────────────────────────┐  │    │
│  │  │ WAL Writer                                                         │  │    │
│  │  │ ┌─────────────────────────────────────────────────────────────┐   │  │    │
│  │  │ │ 4. Append WAL Entry                                          │   │  │    │
│  │  │ │    - Timestamp, TxnId, Op, Table, Before/After               │   │  │    │
│  │  │ │    - CRC32 checksum for integrity                            │   │  │    │
│  │  │ └─────────────────────────────────────────────────────────────┘   │  │    │
│  │  └───────────────────────────────────────────────────────────────────┘  │    │
│  │                           │                                              │    │
│  │                           ▼                                              │    │
│  │  ┌───────────────────────────────────────────────────────────────────┐  │    │
│  │  │ B-tree Index                                                       │  │    │
│  │  │ ┌─────────────────────────────────────────────────────────────┐   │  │    │
│  │  │ │ 5. Update Index                                              │   │  │    │
│  │  │ │    - Copy-on-write for MVCC                                  │   │  │    │
│  │  │ │    - Page splits/merges as needed                            │   │  │    │
│  │  │ └─────────────────────────────────────────────────────────────┘   │  │    │
│  │  └───────────────────────────────────────────────────────────────────┘  │    │
│  │                           │                                              │    │
│  │                           ▼                                              │    │
│  │  ┌───────────────────────────────────────────────────────────────────┐  │    │
│  │  │ CDC Producer                                                       │  │    │
│  │  │ ┌─────────────────────────────────────────────────────────────┐   │  │    │
│  │  │ │ 6. Generate CDC Event                                        │   │  │    │
│  │  │ │    - Sequence number, timestamp, operation                   │   │  │    │
│  │  │ │    - Before/after row data                                   │   │  │    │
│  │  │ │    - Batch for delivery                                      │   │  │    │
│  │  │ └─────────────────────────────────────────────────────────────┘   │  │    │
│  │  └───────────────────────────────────────────────────────────────────┘  │    │
│  │                           │                                              │    │
│  │                           ▼                                              │    │
│  │  ┌───────────────────────────────────────────────────────────────────┐  │    │
│  │  │ 7. Commit Transaction                                              │  │    │
│  │  │    - Release locks                                                 │  │    │
│  │  │    - Advance LSN                                                   │  │    │
│  │  │    - Return to client                                              │  │    │
│  │  └───────────────────────────────────────────────────────────────────┘  │    │
│  └──────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
│         │  8. WebSocket CDC Stream (hibernation-enabled)                        │
│         ▼                                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                        DoLake Aggregator                                 │    │
│  │                                                                          │    │
│  │  ┌───────────────────────────────────────────────────────────────────┐  │    │
│  │  │ 9. Buffer CDC Events                                               │  │    │
│  │  │    - Partition by table + partition key                            │  │    │
│  │  │    - Deduplicate using sequence numbers                            │  │    │
│  │  │    - Track per-source state                                        │  │    │
│  │  └───────────────────────────────────────────────────────────────────┘  │    │
│  │                           │                                              │    │
│  │                           ▼                                              │    │
│  │  ┌───────────────────────────────────────────────────────────────────┐  │    │
│  │  │ 10. Flush Trigger                                                  │  │    │
│  │  │     - Event count threshold (default 10,000)                       │  │    │
│  │  │     - Byte size threshold (default 32MB)                           │  │    │
│  │  │     - Time threshold (default 60s)                                 │  │    │
│  │  │     - Manual flush request                                         │  │    │
│  │  └───────────────────────────────────────────────────────────────────┘  │    │
│  │                           │                                              │    │
│  │                           ▼                                              │    │
│  │  ┌───────────────────────────────────────────────────────────────────┐  │    │
│  │  │ 11. Write Parquet Files                                            │  │    │
│  │  │     - Schema inference from CDC events                             │  │    │
│  │  │     - Columnar encoding                                            │  │    │
│  │  │     - Per-partition files                                          │  │    │
│  │  └───────────────────────────────────────────────────────────────────┘  │    │
│  │                           │                                              │    │
│  │                           ▼                                              │    │
│  │  ┌───────────────────────────────────────────────────────────────────┐  │    │
│  │  │ 12. Update Iceberg Metadata                                        │  │    │
│  │  │     - Create manifest file                                         │  │    │
│  │  │     - Create manifest list                                         │  │    │
│  │  │     - Create snapshot with summary                                 │  │    │
│  │  │     - Update table metadata                                        │  │    │
│  │  └───────────────────────────────────────────────────────────────────┘  │    │
│  └──────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
│         │  13. Write to R2                                                      │
│         ▼                                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                           R2 Lakehouse                                   │    │
│  │                                                                          │    │
│  │  /warehouse/                                                             │    │
│  │    └── default/                      (namespace)                         │    │
│  │        └── users/                    (table)                             │    │
│  │            ├── metadata/                                                 │    │
│  │            │   └── v1.metadata.json                                      │    │
│  │            ├── manifests/                                                │    │
│  │            │   └── snap-{id}.json                                        │    │
│  │            └── data/                                                     │    │
│  │                └── day=2024-01-22/                                       │    │
│  │                    └── {uuid}.parquet                                    │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Sharding Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           SHARDING ARCHITECTURE                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  VSchema Definition:                                                            │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  const vschema = createVSchema({                                         │    │
│  │    // Sharded table - distributed by tenant_id                           │    │
│  │    users: shardedTable('tenant_id', hashVindex()),                       │    │
│  │                                                                          │    │
│  │    // Reference table - replicated to all shards                         │    │
│  │    countries: referenceTable(),                                          │    │
│  │                                                                          │    │
│  │    // Unsharded table - lives on primary shard                           │    │
│  │    config: unshardedTable(),                                             │    │
│  │  }, [                                                                    │    │
│  │    shard('shard-0', 'user-do'),                                          │    │
│  │    shard('shard-1', 'user-do'),                                          │    │
│  │    shard('shard-2', 'user-do'),                                          │    │
│  │  ]);                                                                     │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
│  Query Routing:                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                                                                          │    │
│  │  Query: SELECT * FROM users WHERE tenant_id = 42                         │    │
│  │         └─────────────────────────────────────────────────────┐          │    │
│  │                                                               ▼          │    │
│  │  ┌─────────────┐     ┌─────────────┐     ┌─────────────────────────┐    │    │
│  │  │ SQL Parser  │ ──▶ │   Router    │ ──▶ │   Vindex.map(42)        │    │    │
│  │  │             │     │             │     │   = FNV1a(42) % 3       │    │    │
│  │  │             │     │             │     │   = shard-1             │    │    │
│  │  └─────────────┘     └─────────────┘     └───────────┬─────────────┘    │    │
│  │                                                       │                  │    │
│  │                          Single-shard query           ▼                  │    │
│  │                                               ┌─────────────┐            │    │
│  │                                               │  shard-1    │            │    │
│  │                                               │  (DO)       │            │    │
│  │                                               └─────────────┘            │    │
│  │                                                                          │    │
│  │  Query: SELECT * FROM users WHERE created_at > '2024-01-01'              │    │
│  │         └─────────────────────────────────────────────────────┐          │    │
│  │                                                               ▼          │    │
│  │  ┌─────────────┐     ┌─────────────┐     ┌─────────────────────────┐    │    │
│  │  │ SQL Parser  │ ──▶ │   Router    │ ──▶ │  No shard key found!    │    │    │
│  │  │             │     │             │     │  Scatter to all shards  │    │    │
│  │  └─────────────┘     └─────────────┘     └───────────┬─────────────┘    │    │
│  │                                                       │                  │    │
│  │                           Scatter-gather              ▼                  │    │
│  │                    ┌──────────────────────────────────────────┐          │    │
│  │                    │                                          │          │    │
│  │              ┌─────▼─────┐  ┌─────────────┐  ┌─────────────┐ │          │    │
│  │              │  shard-0  │  │  shard-1    │  │  shard-2    │ │          │    │
│  │              └─────┬─────┘  └──────┬──────┘  └──────┬──────┘ │          │    │
│  │                    │              │               │          │          │    │
│  │                    └──────────────┼───────────────┘          │          │    │
│  │                                   │                          │          │    │
│  │                            ┌──────▼──────┐                   │          │    │
│  │                            │   Merge     │ ◀─────────────────┘          │    │
│  │                            │   Results   │                              │    │
│  │                            └─────────────┘                              │    │
│  │                                                                          │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
│  Vindex Types:                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                                                                          │    │
│  │  hashVindex()          - FNV1a or xxHash for even distribution           │    │
│  │  consistentHashVindex() - Consistent hashing for minimal resharding      │    │
│  │  rangeVindex()         - Range-based for time-series data                │    │
│  │                                                                          │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Storage Tiering

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           STORAGE TIERING                                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  HOT TIER: Durable Object Storage                                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  Constraints:                                                            │    │
│  │    - 128KB per key-value entry                                           │    │
│  │    - 2MB per blob                                                        │    │
│  │    - 10GB total per DO (charged $0.20/GB-month)                          │    │
│  │                                                                          │    │
│  │  Used for:                                                               │    │
│  │    - Active B-tree pages (hot data)                                      │    │
│  │    - WAL segments (recent writes)                                        │    │
│  │    - Transaction state                                                   │    │
│  │    - Schema metadata                                                     │    │
│  │    - Index structures                                                    │    │
│  │                                                                          │    │
│  │  Storage Layout:                                                         │    │
│  │  ┌───────────────────────────────────────────────────────────────────┐  │    │
│  │  │ Key                        │ Value                                │  │    │
│  │  ├────────────────────────────┼──────────────────────────────────────┤  │    │
│  │  │ _meta/schema.json          │ Table schemas, column definitions    │  │    │
│  │  │ _meta/config.json          │ Database configuration               │  │    │
│  │  │ _wal/segment_00001         │ WAL segment (chunked if >2MB)        │  │    │
│  │  │ _wal/checkpoint            │ Last checkpoint LSN                  │  │    │
│  │  │ _btree/users/root          │ B-tree root page                     │  │    │
│  │  │ _btree/users/page_001      │ B-tree internal/leaf pages           │  │    │
│  │  │ _mvcc/versions/v_001       │ MVCC version chain                   │  │    │
│  │  │ _tx/active/tx_abc          │ Active transaction state             │  │    │
│  │  └───────────────────────────────────────────────────────────────────┘  │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
│  WARM TIER: DoLake Buffer (DO Storage)                                          │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  Used for:                                                               │    │
│  │    - CDC event batches pending flush                                     │    │
│  │    - Fallback when R2 writes fail                                        │    │
│  │    - Buffer snapshots for hibernation recovery                           │    │
│  │                                                                          │    │
│  │  Storage Layout:                                                         │    │
│  │  ┌───────────────────────────────────────────────────────────────────┐  │    │
│  │  │ Key                        │ Value                                │  │    │
│  │  ├────────────────────────────┼──────────────────────────────────────┤  │    │
│  │  │ buffer_snapshot            │ Serialized buffer state              │  │    │
│  │  │ fallback_events            │ Events when R2 fails                 │  │    │
│  │  │ scheduled_compaction       │ Pending compaction jobs              │  │    │
│  │  └───────────────────────────────────────────────────────────────────┘  │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
│  COLD TIER: R2 Lakehouse                                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  Constraints:                                                            │    │
│  │    - No object size limit (practical limit ~5TB)                         │    │
│  │    - $0.015/GB-month storage                                             │    │
│  │    - Free egress to Workers                                              │    │
│  │                                                                          │    │
│  │  Used for:                                                               │    │
│  │    - Historical data (Parquet files)                                     │    │
│  │    - Iceberg metadata (manifests, snapshots)                             │    │
│  │    - Archived WAL segments                                               │    │
│  │    - Compacted B-tree pages                                              │    │
│  │                                                                          │    │
│  │  Storage Layout (Iceberg Table Format):                                  │    │
│  │  ┌───────────────────────────────────────────────────────────────────┐  │    │
│  │  │ /warehouse/                                                        │  │    │
│  │  │   └── default/                     # Namespace                     │  │    │
│  │  │       └── users/                   # Table                         │  │    │
│  │  │           ├── metadata/                                            │  │    │
│  │  │           │   ├── v1.metadata.json                                 │  │    │
│  │  │           │   └── v2.metadata.json                                 │  │    │
│  │  │           ├── manifests/                                           │  │    │
│  │  │           │   ├── snap-001-m0.json                                 │  │    │
│  │  │           │   └── snap-002-m0.json                                 │  │    │
│  │  │           └── data/                                                │  │    │
│  │  │               ├── day=2024-01-21/                                  │  │    │
│  │  │               │   ├── 00001.parquet                                │  │    │
│  │  │               │   └── 00002.parquet                                │  │    │
│  │  │               └── day=2024-01-22/                                  │  │    │
│  │  │                   └── 00001.parquet                                │  │    │
│  │  └───────────────────────────────────────────────────────────────────┘  │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
│  Data Lifecycle:                                                                │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                                                                          │    │
│  │   WRITE                                                                  │    │
│  │   ┌────────┐     ┌────────┐     ┌────────┐     ┌────────┐               │    │
│  │   │  WAL   │ ──▶ │ B-tree │ ──▶ │  CDC   │ ──▶ │Parquet │               │    │
│  │   │  (DO)  │     │  (DO)  │     │ Buffer │     │  (R2)  │               │    │
│  │   └────────┘     └────────┘     │  (DO)  │     └────────┘               │    │
│  │      │              │           └────────┘         │                    │    │
│  │      │              │                              │                    │    │
│  │      │              │                              │                    │    │
│  │      ▼              ▼                              ▼                    │    │
│  │   ┌─────────────────────────────────────────────────────────────────┐   │    │
│  │   │                    CHECKPOINT / COMPACTION                       │   │    │
│  │   │                                                                  │   │    │
│  │   │  WAL Retention:        B-tree Compaction:      Parquet Compact:  │   │    │
│  │   │  - 7 days default      - Merge small pages     - Merge small     │   │    │
│  │   │  - Archive to R2       - Reclaim deletions       files           │   │    │
│  │   │  - CDC caught up       - Balance tree          - Re-sort data    │   │    │
│  │   └─────────────────────────────────────────────────────────────────┘   │    │
│  │                                                                          │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Client/Server Type Sharing Strategy

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        TYPE SHARING STRATEGY                                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  Pattern: Client packages export types that server packages depend on           │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                        @dotdo/sql.do                                     │    │
│  │                        (Canonical Types)                                 │    │
│  │                                                                          │    │
│  │  Branded Types:                                                          │    │
│  │  ┌─────────────────────────────────────────────────────────────────┐    │    │
│  │  │  type TransactionId = string & { readonly [Brand]: never };      │    │    │
│  │  │  type LSN = bigint & { readonly [Brand]: never };                │    │    │
│  │  │  type StatementHash = string & { readonly [Brand]: never };      │    │    │
│  │  │  type ShardId = string & { readonly [Brand]: never };            │    │    │
│  │  │                                                                  │    │    │
│  │  │  // Constructors for runtime creation                            │    │    │
│  │  │  function createTransactionId(id: string): TransactionId;        │    │    │
│  │  │  function createLSN(lsn: bigint): LSN;                           │    │    │
│  │  └─────────────────────────────────────────────────────────────────┘    │    │
│  │                                                                          │    │
│  │  RPC Types:                                                              │    │
│  │  ┌─────────────────────────────────────────────────────────────────┐    │    │
│  │  │  interface RPCRequest { id: string; method: RPCMethod; ... }     │    │    │
│  │  │  interface RPCResponse<T> { id: string; result?: T; error?: ... }│    │    │
│  │  │  interface RPCError { code: string; message: string; ... }       │    │    │
│  │  └─────────────────────────────────────────────────────────────────┘    │    │
│  │                                                                          │    │
│  │  Query Types:                                                            │    │
│  │  ┌─────────────────────────────────────────────────────────────────┐    │    │
│  │  │  interface QueryResult<T> { rows: T[]; columns: string[]; ... } │    │    │
│  │  │  interface QueryOptions { transactionId?: TransactionId; ... }   │    │    │
│  │  └─────────────────────────────────────────────────────────────────┘    │    │
│  └──────────────────────────────────┬──────────────────────────────────────┘    │
│                                     │                                           │
│                                     │ depends on                                │
│                                     ▼                                           │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                        @dotdo/dosql                                      │    │
│  │                        (Server Implementation)                           │    │
│  │                                                                          │    │
│  │  import type {                                                           │    │
│  │    TransactionId,                                                        │    │
│  │    LSN,                                                                  │    │
│  │    RPCRequest,                                                           │    │
│  │    RPCResponse,                                                          │    │
│  │    QueryResult,                                                          │    │
│  │  } from '@dotdo/sql.do';                                                 │    │
│  │                                                                          │    │
│  │  // Server uses same types for serialization/deserialization             │    │
│  │  // Ensures wire format compatibility                                    │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
│                                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                        @dotdo/lake.do                                    │    │
│  │                        (Extends sql.do types)                            │    │
│  │                                                                          │    │
│  │  // Re-export sql.do types for client convenience                        │    │
│  │  export type { TransactionId, LSN, SQLValue } from '@dotdo/sql.do';      │    │
│  │                                                                          │    │
│  │  // Add lake-specific branded types                                      │    │
│  │  type CDCEventId = string & { readonly [Brand]: never };                 │    │
│  │  type PartitionKey = string & { readonly [Brand]: never };               │    │
│  │  type ParquetFileId = string & { readonly [Brand]: never };              │    │
│  │  type SnapshotId = string & { readonly [Brand]: never };                 │    │
│  └──────────────────────────────────┬──────────────────────────────────────┘    │
│                                     │                                           │
│                                     │ depends on                                │
│                                     ▼                                           │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                        @dotdo/dolake                                     │    │
│  │                        (Server Implementation)                           │    │
│  │                                                                          │    │
│  │  import type {                                                           │    │
│  │    CDCEventId,                                                           │    │
│  │    PartitionKey,                                                         │    │
│  │    SnapshotId,                                                           │    │
│  │  } from '@dotdo/lake.do';                                                │    │
│  │                                                                          │    │
│  │  // Also imports from sql.do transitively                                │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
│  Benefits:                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  1. Single source of truth for types                                     │    │
│  │  2. Compile-time type safety across client/server boundary               │    │
│  │  3. Branded types prevent mixing different ID types                      │    │
│  │  4. Smaller client bundle (types are erased)                             │    │
│  │  5. Independent versioning of client and server                          │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Architectural Strengths

### 1. Clean Module Boundaries
- Client SDKs (`sql.do`, `lake.do`) are pure TypeScript with no runtime dependencies
- Server packages (`dosql`, `dolake`) contain all implementation complexity
- Shared types flow from client to server, not vice versa

### 2. Effective Type System Usage
- Branded types prevent ID confusion at compile time
- Interface segregation keeps APIs focused
- Generic constraints provide type inference for query results

### 3. Robust CDC Pipeline
- Sequence-based deduplication handles at-least-once delivery
- Backpressure via ack/nack with rate limit info
- Fallback to DO storage when R2 fails
- Hibernation for 95% cost reduction on idle connections

### 4. Scalable Sharding
- VSchema-based routing inspired by Vitess
- Multiple vindex types (hash, consistent hash, range)
- Reference table replication for small lookup tables
- Scatter-gather with merge for cross-shard queries

### 5. Storage Efficiency
- WAL with CRC32 checksums for durability
- B-tree with copy-on-write for MVCC
- Parquet columnar format for analytics
- Iceberg table format for time travel

---

## Areas for Improvement

### 1. DoLake DO Size
The `dolake.ts` file is over 2500 lines. Consider splitting into:
- `http-handlers.ts` - HTTP request routing
- `websocket-handlers.ts` - WebSocket message handling
- `flush.ts` - Flush operations to R2
- `compaction-handlers.ts` - Compaction API handlers

### 2. Test Coverage
Current test files focus on unit tests. Consider adding:
- Integration tests for full client-server flows
- E2E tests with real Durable Objects
- Performance benchmarks for critical paths

### 3. Error Handling
Some error paths could be more specific:
- Add error codes for different failure modes
- Include retry hints in error responses
- Add structured logging for debugging

### 4. Documentation
The inline documentation is good. Consider adding:
- API reference generated from TSDoc
- Architecture decision records (ADRs)
- Runbook for common operations

---

## Cloudflare Platform Alignment

| Constraint | Implementation |
|------------|---------------|
| **CPU (50ms/30s)** | Streaming execution, index-based queries |
| **Memory (128MB)** | Columnar format, projection pushdown |
| **Bundle (1MB)** | Tree-shakeable, no WASM in core |
| **DO Storage (128KB/2MB)** | Chunked blobs, hot/cold tiering |
| **Subrequests (1000)** | Batched RPC, connection reuse |

---

## Conclusion

The `@dotdo/sql` monorepo demonstrates a well-designed edge-native database architecture:

1. **Clear separation** between client SDKs and server implementations
2. **Type-safe** RPC with branded types and shared interfaces
3. **Scalable** through VSchema-based sharding and CDC aggregation
4. **Efficient** storage tiering from DO to R2 via Iceberg/Parquet
5. **Resilient** with fallback storage and at-least-once delivery

The architecture is production-ready with room for continued refinement in testing, documentation, and modularization.

---

*Generated by Claude Code (Opus 4.5) - 2026-01-22*
