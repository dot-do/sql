# DoSQL Architecture

This document describes the architecture of the DoSQL ecosystem, a type-safe SQL database system built natively for Cloudflare Workers and Durable Objects.

## Table of Contents

- [Package Overview](#package-overview)
- [Component Architecture](#component-architecture)
- [Data Flow](#data-flow)
- [Storage Tiers](#storage-tiers)
- [Sharding Architecture](#sharding-architecture)
- [RPC Protocol](#rpc-protocol)
- [Sequence Diagrams](#sequence-diagrams)
- [WebSocket Hibernation](#websocket-hibernation)

---

## Package Overview

The DoSQL ecosystem consists of five packages that work together to provide a complete database solution:

| Package | Description | Role |
|---------|-------------|------|
| `@dotdo/shared-types` | Canonical type definitions | Shared types for client/server compatibility |
| `@dotdo/sql.do` | Client SDK | CapnWeb RPC client for DoSQL |
| `@dotdo/dosql` | Server Durable Object | SQL engine, WAL, transactions, CDC |
| `@dotdo/lake.do` | Lake Client SDK | CapnWeb client for lakehouse queries and CDC streams |
| `@dotdo/dolake` | Lakehouse Durable Object | CDC aggregation, Parquet/Iceberg, R2 storage |

### Package Dependency Graph

```
@dotdo/shared-types          (canonical types, no deps)
         |
         +------------------------------------+
         |                                    |
         v                                    v
   @dotdo/sql.do                        @dotdo/lake.do
   (client SDK)                         (lake client SDK)
         |                                    |
         v                                    v
   @dotdo/dosql ----------------------> @dotdo/dolake
   (SQL engine DO)        CDC           (lakehouse DO)
```

---

## Component Architecture

### DoSQL Module Structure

The `@dotdo/dosql` package contains the following core modules:

```
src/
|-- parser/           # SQL parsing (AST, tokenizer, DDL/DML)
|-- planner/          # Query planning & optimization
|-- executor/         # Query execution engine
|-- btree/            # B-tree index implementation
|-- transaction/      # ACID transaction manager
|-- wal/              # Write-ahead log (writer, reader, checkpoint, retention)
|-- cdc/              # Change data capture (capture, stream, types)
|-- fsx/              # File system abstraction (DO, R2, tiered, COW)
|-- sharding/         # Distributed sharding (vindex, router, executor, replica)
|-- rpc/              # CapnWeb RPC (client, server, types)
|-- statement/        # Prepared statements & cache
|-- constraints/      # Foreign keys & validation
|-- triggers/         # Trigger definitions & execution
|-- migrations/       # Schema migrations & Drizzle compatibility
|-- branch/           # Git-like branching & merge
|-- lakehouse/        # CDC aggregation & partitioning
|-- columnar/         # Columnar encoding (chunk, reader, writer)
|-- vector/           # Vector search (HNSW, distance functions)
|-- fts/              # Full-text search
|-- functions/        # SQL functions
|-- aggregates.ts     # Aggregate functions (COUNT, SUM, AVG, etc.)
|-- pragma/           # PRAGMA commands
|-- index/            # Secondary indexes
|-- view/             # View definitions
|-- virtual/          # Virtual tables
|-- timetravel/       # Point-in-time queries
|-- replication/      # Cross-region replication
|-- compaction/       # Storage compaction
|-- collation/        # String collation
|-- schema/           # Schema management
|-- orm/              # ORM integration
|-- engine/           # Core engine types
|-- errors/           # Error definitions
```

### Component Diagram

```
+---------------------------------------------------------------------------------+
|                              Client Application                                  |
|  +-------------------------------------+  +---------------------------------+   |
|  |         @dotdo/sql.do               |  |         @dotdo/lake.do          |   |
|  |  - Type-safe SQL queries            |  |  - Lakehouse queries            |   |
|  |  - Prepared statements              |  |  - CDC stream subscriptions     |   |
|  |  - Transaction context              |  |  - Time travel queries          |   |
|  +-----------------+-------------------+  +-----------------+---------------+   |
+-------------------|----------------------------------------|-------------------+
                    |                                        |
                    | WebSocket/HTTP                         | WebSocket/HTTP
                    | (CapnWeb RPC)                          | (CapnWeb RPC)
                    |                                        |
+-------------------|----------------------------------------|-------------------+
|                   |         Cloudflare Workers             |                    |
|                   v                                        v                    |
|  +-------------------------------------+  +---------------------------------+   |
|  |         @dotdo/dosql                |  |         @dotdo/dolake           |   |
|  |     (Durable Object)                |  |     (Durable Object)            |   |
|  |                                     |  |                                 |   |
|  |  +-------------------------------+  |  |  +---------------------------+  |   |
|  |  | Parser (Type-level SQL)       |  |  |  | CDC Buffer Manager        |  |   |
|  |  | - AST generation              |  |  |  | - Multi-shard aggregation |  |   |
|  |  | - DDL/DML parsing             |  |  |  | - Deduplication           |  |   |
|  |  | - CTE & subquery support      |  |  |  | - Backpressure handling   |  |   |
|  |  +-------------------------------+  |  |  +---------------------------+  |   |
|  |  +-------------------------------+  |  |  +---------------------------+  |   |
|  |  | Query Planner & Executor      |  |  |  | Parquet Writer            |  |   |
|  |  | - Cost-based optimization     |  |  |  | - Columnar encoding       |  |   |
|  |  | - Join strategies             |  |  |  | - Compression             |  |   |
|  |  | - Index selection             |  |  |  | - Row group batching      |  |   |
|  |  +-------------------------------+  |  |  +---------------------------+  |   |
|  |  +-------------------------------+  |  |  +---------------------------+  |   |
|  |  | Transaction Manager           |<-|--|--| Iceberg Catalog           |  |   |
|  |  | - ACID guarantees             |  |  |  | - REST Catalog API        |  |   |
|  |  | - Savepoints, MVCC            |  |  |  | - Metadata management     |  |   |
|  |  | - Deadlock detection          |  |  |  | - Schema evolution        |  |   |
|  |  +-------------------------------+  |  |  +---------------------------+  |   |
|  |  +-------------------------------+  |  |  +---------------------------+  |   |
|  |  | WAL Writer                    |--|--|->| Query Engine              |  |   |
|  |  | - Durability                  |CDC|  |  | - Partition pruning       |  |   |
|  |  | - Checkpoint & recovery       |  |  |  | - Predicate pushdown      |  |   |
|  |  | - Retention management        |  |  |  | - Stats-based filtering   |  |   |
|  |  +-------------------------------+  |  |  +---------------------------+  |   |
|  |  +-------------------------------+  |  |  +---------------------------+  |   |
|  |  | Sharding Router               |  |  |  | Compaction Manager        |  |   |
|  |  | - VSchema routing             |  |  |  | - Small file merging      |  |   |
|  |  | - Distributed execution       |  |  |  | - Atomic commits          |  |   |
|  |  | - Replica selection           |  |  |  | - Space optimization      |  |   |
|  |  +-------------------------------+  |  |  +---------------------------+  |   |
|  |                 |                   |  |                 |               |   |
|  +-----------------|-+-----------------+  +-----------------+---------------+   |
|                    | |                                      |                   |
|                    v v                                      v                   |
|  +-------------------------------------+  +---------------------------------+   |
|  |        DO Storage (Hot)             |  |           R2 (Cold)             |   |
|  |  - 2MB blob chunks                  |  |  - Parquet data files           |   |
|  |  - Low latency (~1ms)               |  |  - Iceberg metadata             |   |
|  |  - B-tree pages, WAL segments       |  |  - Unlimited capacity           |   |
|  +-------------------------------------+  +---------------------------------+   |
+----------------------------------------------------------------------------------+
                                                              |
                                                              v
                                              +---------------------------------+
                                              |   External Query Engines        |
                                              |  Spark | DuckDB | Trino | Flink |
                                              |     (via Iceberg REST Catalog)  |
                                              +---------------------------------+
```

---

## Data Flow

### Query Execution Path

```
Client Request
      |
      v
+--------------------+
|  RPC Handler       |  Parse request, authenticate, route
+--------+-----------+
         |
         v
+--------------------+
|  SQL Parser        |  Tokenize -> Parse -> AST -> Validate
|  - parser/         |
+--------+-----------+
         |
         v
+--------------------+
|  Query Planner     |  AST -> Logical Plan -> Physical Plan
|  - planner/        |  (cost estimation, index selection)
+--------+-----------+
         |
         v
+--------------------+
|  Transaction       |  Acquire locks, create snapshot (MVCC)
|  Manager           |  Check isolation level
|  - transaction/    |
+--------+-----------+
         |
         v
+--------------------+
|  Executor          |  Execute operators (Scan, Filter,
|  - executor/       |  Project, Sort, Join, Aggregate)
+--------+-----------+
         |
         v
+--------------------+
|  FSX Backend       |  Read/write via tiered storage
|  - fsx/            |  (DO hot -> R2 warm/cold)
+--------+-----------+
         |
         v
   Query Result
```

### Write Path with WAL and CDC

```
INSERT/UPDATE/DELETE
         |
         v
+--------------------+
|  Transaction       |  Begin or use existing transaction
|  Manager           |
+--------+-----------+
         |
    +----+----+
    |         |
    v         v
+-------+  +-------+
|  WAL  |  | B-tree|  Write in parallel
| Write |  | Write |
| (wal/)|  |(btree)|
+---+---+  +---+---+
    |         |
    +----+----+
         |
         v
+--------------------+
|  Commit            |  Flush WAL, release locks
|  (transaction/)    |
+--------+-----------+
         |
         v
+--------------------+
|  CDC Capture       |  Emit change event from WAL entry
|  (cdc/capture.ts)  |
+--------+-----------+
         |
         v (async)
+--------------------+
|  CDC Stream        |  Buffer events, apply filters
|  (cdc/stream.ts)   |
+--------+-----------+
         |
         v (WebSocket)
   Lakehouse (DoLake)
```

### CDC Streaming to Lakehouse

```
+---------------------------------------------------------------------------------+
|                          DoSQL Instances (Shards)                                |
|  +---------+  +---------+  +---------+  +---------+  +---------+               |
|  | Shard 1 |  | Shard 2 |  | Shard 3 |  | Shard N |  |  ...    |               |
|  |  (WAL)  |  |  (WAL)  |  |  (WAL)  |  |  (WAL)  |  |         |               |
|  +----+----+  +----+----+  +----+----+  +----+----+  +----+----+               |
|       |            |            |            |            |                    |
|       |  WebSocket |  WebSocket |  WebSocket |  WebSocket |                    |
|       |  CDC Stream|  CDC Stream|  CDC Stream|  CDC Stream|                    |
|       v            v            v            v            v                    |
|  +-----------------------------------------------------------------------+     |
|  |                        DoLake (Aggregator)                             |     |
|  |                                                                        |     |
|  |  +------------------+  +------------------+  +------------------+      |     |
|  |  | CDC Buffer       |  | Deduplication    |  | Partitioning     |      |     |
|  |  | Manager          |  | (by LSN + shard) |  | Manager          |      |     |
|  |  | (buffer.ts)      |  |                  |  | (partitioning.ts)|      |     |
|  |  +--------+---------+  +--------+---------+  +--------+---------+      |     |
|  |           |                     |                     |                |     |
|  |           v                     v                     v                |     |
|  |  +---------------------------------------------------------------+    |     |
|  |  |                    Flush Pipeline                              |    |     |
|  |  |  Buffer -> Partition -> Parquet Write -> Iceberg Commit       |    |     |
|  |  +---------------------------------------------------------------+    |     |
|  |           |                                                            |     |
|  |           v                                                            |     |
|  |  +------------------+  +------------------+  +------------------+      |     |
|  |  | Parquet Writer   |  | Iceberg Catalog  |  | Query Engine     |      |     |
|  |  | (parquet.ts)     |  | (catalog.ts)     |  | (query-engine.ts)|      |     |
|  |  +------------------+  +------------------+  +------------------+      |     |
|  +-----------------------------------------------------------------------+     |
|                              |                                                  |
|                              v                                                  |
|                        +----------+                                             |
|                        |    R2    |  Parquet files + Iceberg metadata           |
|                        | (Iceberg)|                                             |
|                        +----------+                                             |
|                              |                                                  |
|                              v                                                  |
|  +-----------------------------------------------------------------------+     |
|  |              External Query Engines                                    |     |
|  |   Spark  |  DuckDB  |  Trino  |  Flink  |  DataFusion                 |     |
|  |           (via Iceberg REST Catalog - catalog.ts)                      |     |
|  +-----------------------------------------------------------------------+     |
+---------------------------------------------------------------------------------+
```

---

## Storage Tiers

DoSQL implements a tiered storage architecture optimized for Cloudflare's infrastructure:

### Storage Tier Diagram

```
+---------------------------------------------------------------------------------+
|                              Storage Tiers                                       |
+---------------------------------------------------------------------------------+
|                                                                                  |
|  +---------------------------+                                                   |
|  |     HOT TIER              |   Latency: ~1ms                                   |
|  |  (DO Storage - fsx/do-    |   Max Value: 2MB                                  |
|  |   backend.ts)             |   Consistency: Strong (single-leader)             |
|  |                           |                                                   |
|  |  Contents:                |   +-------------------------------------------+   |
|  |  - WAL segments           |   |  DurableObjectStorage                     |   |
|  |  - B-tree pages           |   |  +-------+  +-------+  +-------+         |   |
|  |  - Active indexes         |   |  | WAL   |  | B-tree|  | Meta  |         |   |
|  |  - Transaction state      |   |  | Segs  |  | Pages |  | Data  |         |   |
|  |  - Recent data            |   |  +-------+  +-------+  +-------+         |   |
|  +------------+--------------+   +-------------------------------------------+   |
|               |                                                                  |
|               | Migration (compaction/scheduler.ts)                              |
|               v                                                                  |
|  +---------------------------+                                                   |
|  |     WARM TIER             |   Latency: ~50-100ms                              |
|  |  (R2 with Cache - fsx/    |   Capacity: Unlimited                             |
|  |   r2-cache.ts)            |   Format: Binary pages                            |
|  |                           |                                                   |
|  |  Contents:                |   +-------------------------------------------+   |
|  |  - Overflow pages         |   |  R2 Bucket (with DO cache)                |   |
|  |  - Historical B-tree      |   |  +-------+  +-------+  +-------+         |   |
|  |  - Archive indexes        |   |  | Page  |  | Index |  | Blob  |         |   |
|  |  - Large BLOBs            |   |  | Files |  | Files |  | Data  |         |   |
|  +------------+--------------+   |  +-------+  +-------+  +-------+         |   |
|               |                  +-------------------------------------------+   |
|               | CDC Stream (cdc/stream.ts -> dolake)                             |
|               v                                                                  |
|  +---------------------------+                                                   |
|  |     COLD TIER             |   Latency: ~100-500ms                             |
|  |  (Parquet/Iceberg on R2)  |   Capacity: Unlimited                             |
|  |  (dolake/parquet.ts,      |   Format: Columnar (Parquet)                      |
|  |   dolake/iceberg.ts)      |   Metadata: Iceberg                               |
|  |                           |                                                   |
|  |  Contents:                |   +-------------------------------------------+   |
|  |  - Analytics data         |   |  R2 Bucket (Iceberg Table)                |   |
|  |  - Historical snapshots   |   |  +-------+  +-------+  +-------+         |   |
|  |  - Time travel data       |   |  |Parquet|  |Manifest|  | Meta  |         |   |
|  |  - CDC archive            |   |  | Files |  | Lists  |  | JSON  |         |   |
|  +---------------------------+   |  +-------+  +-------+  +-------+         |   |
|                                  +-------------------------------------------+   |
+---------------------------------------------------------------------------------+
```

### Tiered Storage Configuration

```typescript
// fsx/types.ts
interface TieredStorageConfig {
  hotTier: {
    maxSize: number;           // Max size in hot tier (default: 50MB)
    maxAge: number;            // Max age before migration (default: 1 hour)
  };
  warmTier: {
    cacheSize: number;         // Cache size for warm reads (default: 10MB)
    cacheTTL: number;          // Cache TTL (default: 5 minutes)
  };
  coldTier: {
    partitionBy: string[];     // Partition columns
    fileTargetSize: number;    // Target Parquet file size (default: 128MB)
  };
}
```

### Performance Expectations by Tier

Each storage tier has distinct latency and throughput characteristics optimized for its use case:

#### Hot Tier (Durable Object Storage)

| Metric | Expected Value | Notes |
|--------|----------------|-------|
| **Read Latency (p50)** | 0.5 - 1ms | Single key lookup |
| **Read Latency (p99)** | 2 - 5ms | Under normal load |
| **Write Latency (p50)** | 1 - 2ms | Single key write |
| **Write Latency (p99)** | 5 - 10ms | Includes WAL sync |
| **Throughput (reads)** | 10,000 - 50,000 ops/sec | Per DO instance |
| **Throughput (writes)** | 1,000 - 5,000 ops/sec | WAL-bound |
| **Max Value Size** | 2 MB | Cloudflare DO limit |
| **Max Keys** | ~10,000 | Practical limit per DO |
| **Consistency** | Strong | Single-leader guarantee |

**Best For**: Active transactions, hot working set, WAL segments, B-tree root/branch pages

#### Warm Tier (R2 with DO Cache)

| Metric | Expected Value | Notes |
|--------|----------------|-------|
| **Read Latency (cache hit, p50)** | 1 - 2ms | Served from DO cache |
| **Read Latency (cache hit, p99)** | 5 - 10ms | Under load |
| **Read Latency (cache miss, p50)** | 30 - 50ms | R2 fetch required |
| **Read Latency (cache miss, p99)** | 100 - 150ms | Cross-region or congestion |
| **Write Latency (p50)** | 20 - 40ms | Direct R2 write |
| **Write Latency (p99)** | 80 - 120ms | Under load |
| **Throughput (reads)** | 1,000 - 10,000 ops/sec | Cache-dependent |
| **Throughput (writes)** | 100 - 500 ops/sec | R2-bound |
| **Max Object Size** | 5 GB | R2 single object limit |
| **Consistency** | Eventual | Cache invalidation delay |

**Best For**: Overflow pages, historical B-tree nodes, large BLOBs, archived indexes

#### Cold Tier (Parquet/Iceberg on R2)

| Metric | Expected Value | Notes |
|--------|----------------|-------|
| **Read Latency (metadata, p50)** | 50 - 100ms | Manifest/metadata fetch |
| **Read Latency (data scan, p50)** | 100 - 300ms | Single Parquet file |
| **Read Latency (p99)** | 500 - 1000ms | Large scans, cold start |
| **Write Latency (flush, p50)** | 200 - 500ms | Parquet file write |
| **Write Latency (commit, p50)** | 100 - 200ms | Iceberg metadata update |
| **Scan Throughput** | 50 - 200 MB/sec | Parquet columnar reads |
| **Write Throughput** | 10 - 50 MB/sec | Batched CDC flush |
| **Target File Size** | 128 - 256 MB | Optimal Parquet files |
| **Compression Ratio** | 3x - 10x | Depends on data entropy |
| **Consistency** | Snapshot isolation | Iceberg ACID guarantees |

**Best For**: Analytics queries, historical data, time travel, CDC archive, compliance retention

#### Cross-Tier Summary

```
+------------------+---------------+----------------+------------------+
|     Metric       |   Hot (DO)    |  Warm (R2+$)   |  Cold (Iceberg)  |
+------------------+---------------+----------------+------------------+
| Read Latency p50 |    0.5-1ms    |    1-50ms      |    100-300ms     |
| Read Latency p99 |    2-5ms      |   100-150ms    |    500-1000ms    |
| Write Latency    |    1-2ms      |    20-40ms     |    200-500ms     |
| Throughput (r)   | 10K-50K ops/s |  1K-10K ops/s  |   50-200 MB/s    |
| Throughput (w)   |  1K-5K ops/s  |  100-500 ops/s |   10-50 MB/s     |
| Capacity         |    ~100MB     |   Unlimited    |    Unlimited     |
| Consistency      |    Strong     |    Eventual    |    Snapshot      |
+------------------+---------------+----------------+------------------+
```

#### Latency Optimization Guidelines

1. **Keep hot data hot**: Configure `hotTier.maxAge` to retain frequently accessed data
2. **Warm cache sizing**: Size `warmTier.cacheSize` to fit your working set
3. **Partition cold data**: Use time-based partitioning for efficient pruning
4. **Batch CDC writes**: Configure flush thresholds to balance latency vs throughput
5. **Prefetch on access patterns**: Use predictive loading for sequential scans

### Storage Flow

```
Write Request
      |
      v
+--------------------+
|   Hot (DO)         |  Immediate writes
|   - WAL segment    |  (wal/writer.ts)
|   - B-tree page    |  (btree/)
+--------+-----------+
         |
         | Async (compaction/scheduler.ts)
         v
+--------------------+
|   Warm (R2+Cache)  |  Overflow & archive
|   - Binary pages   |  (fsx/tiered.ts)
+--------+-----------+
         |
         | CDC Stream (cdc/ -> dolake)
         v
+--------------------+
|  Cold (R2/Iceberg) |  Batched Parquet writes
|   - Parquet files  |  (dolake/parquet.ts)
|   - Iceberg meta   |  (dolake/iceberg.ts)
+--------------------+
```

---

## Sharding Architecture

DoSQL provides native sharding inspired by Vitess, implemented in `src/sharding/`.

### Sharding Component Diagram

```
+---------------------------------------------------------------------------------+
|                           Sharding Architecture                                  |
+---------------------------------------------------------------------------------+
|                                                                                  |
|  +-----------------------------------------------------------------------+      |
|  |                        VSchema Configuration                          |      |
|  |                        (sharding/types.ts)                            |      |
|  |                                                                        |      |
|  |  +------------------+  +------------------+  +------------------+      |      |
|  |  | Table Configs    |  | Shard Configs    |  | Replica Configs  |      |      |
|  |  | - sharded        |  | - id (ShardId)   |  | - id             |      |      |
|  |  | - unsharded      |  | - doNamespace    |  | - role (primary/ |      |      |
|  |  | - reference      |  | - doId           |  |   replica/       |      |      |
|  |  |                  |  | - replicas[]     |  |   analytics)     |      |      |
|  |  |                  |  |                  |  | - region         |      |      |
|  |  |                  |  |                  |  | - weight         |      |      |
|  |  +------------------+  +------------------+  +------------------+      |      |
|  +-----------------------------------------------------------------------+      |
|                                      |                                           |
|                                      v                                           |
|  +-----------------------------------------------------------------------+      |
|  |                         Vindex Layer                                   |      |
|  |                         (sharding/vindex.ts)                           |      |
|  |                                                                        |      |
|  |  +------------------+  +------------------+  +------------------+      |      |
|  |  | Hash Vindex      |  | Consistent Hash  |  | Range Vindex     |      |      |
|  |  | - FNV-1a         |  | - Virtual nodes  |  | - Boundaries     |      |      |
|  |  | - xxhash         |  | - Rebalancing    |  | - Range queries  |      |      |
|  |  +------------------+  +------------------+  +------------------+      |      |
|  +-----------------------------------------------------------------------+      |
|                                      |                                           |
|                                      v                                           |
|  +-----------------------------------------------------------------------+      |
|  |                        Query Router                                    |      |
|  |                        (sharding/router.ts)                            |      |
|  |                                                                        |      |
|  |  +------------------+  +------------------+  +------------------+      |      |
|  |  | SQL Parser       |  | Shard Key        |  | Execution Plan   |      |      |
|  |  | - ParsedQuery    |  | Extractor        |  | Generator        |      |      |
|  |  | - TableReference |  | - WHERE clause   |  | - Single shard   |      |      |
|  |  | - WhereClause    |  | - JOIN keys      |  | - Scatter-gather |      |      |
|  |  +------------------+  +------------------+  +------------------+      |      |
|  +-----------------------------------------------------------------------+      |
|                                      |                                           |
|                                      v                                           |
|  +-----------------------------------------------------------------------+      |
|  |                     Distributed Executor                               |      |
|  |                     (sharding/executor.ts)                             |      |
|  |                                                                        |      |
|  |  +------------------------------------------------------------------+ |      |
|  |  |                    Execution Flow                                 | |      |
|  |  |                                                                   | |      |
|  |  |   Query --> Plan --> Route --> Execute (parallel) --> Merge      | |      |
|  |  |                        |              |                           | |      |
|  |  |                        v              v                           | |      |
|  |  |                   +--------+    +----------+                      | |      |
|  |  |                   |Replica |    | Result   |                      | |      |
|  |  |                   |Selector|    | Merger   |                      | |      |
|  |  |                   +--------+    | - Sort   |                      | |      |
|  |  |                        |        | - Limit  |                      | |      |
|  |  |                        v        | - Agg    |                      | |      |
|  |  |                   (replica.ts)  +----------+                      | |      |
|  |  +------------------------------------------------------------------+ |      |
|  +-----------------------------------------------------------------------+      |
|                                      |                                           |
|                                      v                                           |
|  +-----------------------------------------------------------------------+      |
|  |                          Shard DOs                                     |      |
|  |                                                                        |      |
|  |  +----------+  +----------+  +----------+  +----------+               |      |
|  |  | Shard 1  |  | Shard 2  |  | Shard 3  |  | Shard N  |               |      |
|  |  | +-------+|  | +-------+|  | +-------+|  | +-------+|               |      |
|  |  | |Primary||  | |Primary||  | |Primary||  | |Primary||               |      |
|  |  | +-------+|  | +-------+|  | +-------+|  | +-------+|               |      |
|  |  | +-------+|  | +-------+|  | +-------+|  | +-------+|               |      |
|  |  | |Replica||  | |Replica||  | |Replica||  | |Replica||               |      |
|  |  | +-------+|  | +-------+|  | +-------+|  | +-------+|               |      |
|  |  +----------+  +----------+  +----------+  +----------+               |      |
|  +-----------------------------------------------------------------------+      |
|                                                                                  |
+---------------------------------------------------------------------------------+
```

### Sharding Query Flow

```
SELECT * FROM users WHERE tenant_id = 123
                |
                v
+-----------------------------+
| SQL Parser                  |  Parse and analyze query
| (sharding/router.ts)        |
+-------------+---------------+
              |
              v
+-----------------------------+
| Shard Key Extraction        |  Extract: tenant_id = 123
| - WHERE clause analysis     |
| - IN list detection         |
+-------------+---------------+
              |
              v
+-----------------------------+
| Vindex Resolution           |  Hash(123) -> Shard 2
| (sharding/vindex.ts)        |
+-------------+---------------+
              |
              v
+-----------------------------+
| Execution Plan              |  Single-shard plan:
| - type: 'single'            |  Route to Shard 2 only
| - targetShards: [shard-2]   |
+-------------+---------------+
              |
              v
+-----------------------------+
| Replica Selection           |  Select best replica:
| (sharding/replica.ts)       |  - Health check
| - Nearest region            |  - Load balance
| - Primary/replica role      |
+-------------+---------------+
              |
              v
+-----------------------------+
| RPC Execution               |  Execute via ShardRPC
| (via CapnWeb)               |
+-------------+---------------+
              |
              v
         Query Result
```

---

## RPC Protocol

DoSQL uses CapnWeb for efficient RPC communication, implemented in `src/rpc/`.

### CapnWeb Protocol Diagram

```
+---------------------------------------------------------------------------------+
|                            CapnWeb RPC Protocol                                  |
|                            (rpc/types.ts, rpc/client.ts, rpc/server.ts)          |
+---------------------------------------------------------------------------------+
|                                                                                  |
|  Transport Layer                                                                 |
|  +-----------------------------------------------------------------------+      |
|  |  WebSocket (Primary)           |  HTTP Batch (Fallback)               |      |
|  |  - Persistent connection       |  - Stateless requests                |      |
|  |  - Bi-directional streaming    |  - Multiple queries per request      |      |
|  |  - Low latency                 |  - Better for serverless             |      |
|  |  - CDC subscriptions           |  - No persistent state               |      |
|  +-----------------------------------------------------------------------+      |
|                                                                                  |
|  Serialization                                                                   |
|  +-----------------------------------------------------------------------+      |
|  |  JSON with BigInt Support                                              |      |
|  |  - Branded types (TransactionId, LSN, ShardId, StatementHash)          |      |
|  |  - BigInt serialization via markers                                    |      |
|  |  - Uint8Array for binary data                                          |      |
|  +-----------------------------------------------------------------------+      |
|                                                                                  |
|  Message Types (from rpc/types.ts)                                               |
|  +-----------------------------------------------------------------------+      |
|  |  Request Types:                |  Response Types:                      |      |
|  |  - QueryRequest                |  - QueryResponse                      |      |
|  |  - StreamRequest               |  - StreamChunk / StreamComplete       |      |
|  |  - BeginTransactionRequest     |  - TransactionHandle                  |      |
|  |  - CommitRequest               |  - TransactionResult                  |      |
|  |  - RollbackRequest             |                                       |      |
|  |  - BatchRequest                |  - BatchResponse                      |      |
|  |  - CDCRequest                  |  - CDCEvent (streaming)               |      |
|  |  - SchemaRequest               |  - SchemaResponse                     |      |
|  +-----------------------------------------------------------------------+      |
|                                                                                  |
|  DoSQL API Methods (DoSQLAPI interface)                                          |
|  +-----------------------------------------------------------------------+      |
|  |  Query Operations:             |  Schema Operations:                   |      |
|  |  - query(QueryRequest)         |  - getSchema(SchemaRequest)           |      |
|  |  - queryStream(StreamRequest)  |                                       |      |
|  |                                |  Connection Operations:               |      |
|  |  Transaction Operations:       |  - ping()                             |      |
|  |  - beginTransaction()          |  - getStats()                         |      |
|  |  - commit(CommitRequest)       |                                       |      |
|  |  - rollback(RollbackRequest)   |  CDC Operations:                      |      |
|  |                                |  - subscribeCDC(CDCRequest)           |      |
|  |  Batch Operations:             |  - unsubscribeCDC(subscriptionId)     |      |
|  |  - batch(BatchRequest)         |                                       |      |
|  +-----------------------------------------------------------------------+      |
|                                                                                  |
|  Error Handling (RPCErrorCode enum)                                              |
|  +-----------------------------------------------------------------------+      |
|  |  General: UNKNOWN, INVALID_REQUEST, TIMEOUT, INTERNAL_ERROR            |      |
|  |  Query: SYNTAX_ERROR, TABLE_NOT_FOUND, COLUMN_NOT_FOUND, TYPE_MISMATCH |      |
|  |  Transaction: TRANSACTION_NOT_FOUND, DEADLOCK_DETECTED, SERIALIZATION  |      |
|  |  CDC: INVALID_LSN, SUBSCRIPTION_ERROR, BUFFER_OVERFLOW                 |      |
|  |  Auth: UNAUTHORIZED, FORBIDDEN                                         |      |
|  |  Resource: RESOURCE_EXHAUSTED, QUOTA_EXCEEDED                          |      |
|  +-----------------------------------------------------------------------+      |
|                                                                                  |
+---------------------------------------------------------------------------------+
```

### RPC Message Flow

```
Client                                                              Server (DO)
   |                                                                      |
   |  1. WebSocket Connect                                                |
   |--------------------------------------------------------------------->|
   |                                                                      |
   |  2. RPC Request (QueryRequest)                                       |
   |  { id: "req-1", method: "query",                                     |
   |    params: { sql: "SELECT ...", params: [...] } }                    |
   |--------------------------------------------------------------------->|
   |                                                                      |
   |  3. RPC Response (QueryResponse)                                     |
   |  { id: "req-1", result: { columns: [...], rows: [...], lsn: 42n } } |
   |<---------------------------------------------------------------------|
   |                                                                      |
   |  4. CDC Subscribe                                                    |
   |  { id: "req-2", method: "subscribeCDC",                              |
   |    params: { fromLSN: 42n, tables: ["users"] } }                     |
   |--------------------------------------------------------------------->|
   |                                                                      |
   |  5. CDC Events (streaming)                                           |
   |  { lsn: 43n, table: "users", operation: "INSERT", after: {...} }     |
   |<---------------------------------------------------------------------|
   |  { lsn: 44n, table: "users", operation: "UPDATE", before/after }     |
   |<---------------------------------------------------------------------|
   |  ...                                                                 |
```

---

## Sequence Diagrams

### Transaction Lifecycle

```
Client              DoSQL DO               WAL                B-tree
   |                    |                   |                    |
   | beginTransaction   |                   |                    |
   |------------------->|                   |                    |
   |                    | create TxContext  |                    |
   |                    |------------------>|                    |
   |                    |<------------------|                    |
   |  TransactionHandle |                   |                    |
   |<-------------------|                   |                    |
   |                    |                   |                    |
   | INSERT INTO users  |                   |                    |
   |------------------->|                   |                    |
   |                    | acquire lock      |                    |
   |                    |---------------------------------------->|
   |                    |                   |                    |
   |                    | append WAL entry  |                    |
   |                    |------------------>|                    |
   |                    |                   |                    |
   |                    | write B-tree page |                    |
   |                    |---------------------------------------->|
   |                    |<----------------------------------------|
   |    QueryResult     |                   |                    |
   |<-------------------|                   |                    |
   |                    |                   |                    |
   | commit             |                   |                    |
   |------------------->|                   |                    |
   |                    | flush WAL         |                    |
   |                    |------------------>|                    |
   |                    |<------------------|                    |
   |                    | release locks     |                    |
   |                    |---------------------------------------->|
   |                    |                   |                    |
   |                    | emit CDC event    |                    |
   |                    |------------------>| (to stream.ts)     |
   | TransactionResult  |                   |                    |
   |<-------------------|                   |                    |
```

### CDC Streaming to Lakehouse

```
DoSQL DO            CDC Module           DoLake DO           R2 Storage
   |                    |                    |                    |
   | WAL entry written  |                    |                    |
   |------------------->|                    |                    |
   |                    | capture change     |                    |
   |                    | (capture.ts)       |                    |
   |                    |                    |                    |
   |                    | WebSocket stream   |                    |
   |                    |------------------->|                    |
   |                    |                    | buffer event       |
   |                    |                    | (buffer.ts)        |
   |                    |                    |                    |
   |                    |                    | [batch threshold]  |
   |                    |                    |                    |
   |                    |                    | partition events   |
   |                    |                    | (partitioning.ts)  |
   |                    |                    |                    |
   |                    |                    | write Parquet      |
   |                    |                    | (parquet.ts)       |
   |                    |                    |------------------->|
   |                    |                    |                    |
   |                    |                    | update Iceberg     |
   |                    |                    | metadata           |
   |                    |                    | (iceberg.ts)       |
   |                    |                    |------------------->|
   |                    |                    |<-------------------|
   |                    |                    |                    |
   |                    | ACK (with LSN)     |                    |
   |                    |<-------------------|                    |
```

### Sharded Query Execution

```
Client          ShardingClient         Router           Executor         Shards
   |                  |                  |                  |               |
   | query(SQL)       |                  |                  |               |
   |----------------->|                  |                  |               |
   |                  | parse SQL        |                  |               |
   |                  |----------------->|                  |               |
   |                  |                  |                  |               |
   |                  | extract shard key|                  |               |
   |                  |<-----------------|                  |               |
   |                  |                  |                  |               |
   |                  | resolve vindex   |                  |               |
   |                  |----------------->|                  |               |
   |                  |<-----------------|                  |               |
   |                  |                  |                  |               |
   |                  | create plan      |                  |               |
   |                  |----------------->|                  |               |
   |                  |<-----------------|                  |               |
   |                  |                  |                  |               |
   |                  | execute(plan)    |                  |               |
   |                  |----------------------------------->|               |
   |                  |                  |                  |               |
   |                  |                  |                  | parallel RPC  |
   |                  |                  |                  |-------------->|
   |                  |                  |                  |<--------------|
   |                  |                  |                  |               |
   |                  |                  |                  | merge results |
   |                  |                  |                  | (sort, limit) |
   |                  |<-----------------------------------|               |
   |                  |                  |                  |               |
   | MergedResult     |                  |                  |               |
   |<-----------------|                  |                  |               |
```

### Query with Time Travel

```
Client              DoSQL DO             FSX Backend          Storage
   |                    |                    |                    |
   | query(SQL, asOf)   |                    |                    |
   |------------------->|                    |                    |
   |                    | parse SQL          |                    |
   |                    | detect asOf LSN    |                    |
   |                    |                    |                    |
   |                    | create MVCC        |                    |
   |                    | snapshot at LSN    |                    |
   |                    |                    |                    |
   |                    | read historical    |                    |
   |                    | pages              |                    |
   |                    |------------------->|                    |
   |                    |                    | check hot tier     |
   |                    |                    |------------------->|
   |                    |                    |                    |
   |                    |                    | [if not found]     |
   |                    |                    | check warm tier    |
   |                    |                    |------------------->|
   |                    |                    |<-------------------|
   |                    |<-------------------|                    |
   |                    |                    |                    |
   |                    | execute on         |                    |
   |                    | historical data    |                    |
   |                    |                    |                    |
   |    QueryResult     |                    |                    |
   |<-------------------|                    |                    |
```

---

## Bundle Size

DoSQL is designed for Cloudflare's constraints:

| Component | Gzipped Size |
|-----------|-------------|
| Core (B-tree, FSX, WAL) | ~7.4 KB |
| Sharding | ~6.8 KB |
| Procedures | ~5.5 KB |
| CDC | ~1.4 KB |
| **Full library** | **~34 KB** |

Compare to WASM alternatives:
- sql.js: ~500 KB
- PGLite: ~3 MB
- DuckDB-WASM: ~4 MB

---

## File Reference Summary

| Feature | Source Location |
|---------|-----------------|
| SQL Parser | `src/parser/` |
| Query Planner | `src/planner/` |
| Query Executor | `src/executor/` |
| B-tree Index | `src/btree/` |
| Transaction Manager | `src/transaction/` |
| WAL | `src/wal/` |
| CDC | `src/cdc/` |
| FSX Storage | `src/fsx/` |
| Sharding | `src/sharding/` |
| RPC (CapnWeb) | `src/rpc/` |
| Prepared Statements | `src/statement/` |
| Constraints | `src/constraints/` |
| Triggers | `src/triggers/` |
| Migrations | `src/migrations/` |
| Branching | `src/branch/` |
| Lakehouse | `src/lakehouse/` |
| Columnar | `src/columnar/` |
| Vector Search | `src/vector/` |
| Full-Text Search | `src/fts/` |
| Time Travel | `src/timetravel/` |
| Compaction | `src/compaction/` |
| Shared Types | `../shared-types/src/` |
| DoLake | `../dolake/src/` |

---

## WebSocket Hibernation

DoSQL uses Cloudflare's WebSocket Hibernation API to maintain persistent connections at minimal cost. When a Durable Object has no active requests, it can "hibernate" to reduce compute charges by up to 95%, while keeping WebSocket connections open.

### Hibernation Attachment Schema

The key to hibernation support is the WebSocket attachment - a serializable state object that persists across hibernation cycles. When a DO wakes from hibernation, all in-memory state is lost, but WebSocket attachments are automatically restored.

**DoSQL Session State Attachment:**

```typescript
/**
 * Session state attached to WebSocket for persistence across hibernation.
 * This state survives DO sleep/wake cycles.
 *
 * Defined in: packages/dosql/src/worker/hibernation.ts
 */
interface WebSocketSessionState {
  /** Unique session identifier (UUID) */
  sessionId: string;

  /** Client identifier for monitoring/debugging (optional) */
  clientId?: string;

  /** Database name being accessed (optional) */
  database?: string;

  /** Branch being accessed for git-like branching (optional) */
  branch?: string;

  /** Unix timestamp (ms) when the connection was established */
  connectedAt: number;

  /** Unix timestamp (ms) of last activity (message sent/received) */
  lastActivity: number;

  /** Array of pending RPC request IDs awaiting response */
  pendingRequests: string[];

  /** Active transaction state if any (optional) */
  transaction?: {
    /** Transaction ID */
    txId: string;
    /** Unix timestamp (ms) when transaction started */
    startedAt: number;
    /** Transaction timeout in milliseconds */
    timeout: number;
  };

  /** Prepared statement cache - array of [name, sql] pairs (optional) */
  preparedStatements?: [string, string][];

  /** Connection-level metrics */
  metrics: {
    /** Total number of queries executed on this connection */
    totalQueries: number;
    /** Total number of errors on this connection */
    totalErrors: number;
    /** Total bytes received on this connection */
    bytesReceived: number;
    /** Total bytes sent on this connection */
    bytesSent: number;
  };
}
```

### What Data is Preserved During Hibernation

| Field | Purpose | Restored On Wake |
|-------|---------|------------------|
| `sessionId` | Unique session tracking | Yes - for logging and correlation |
| `clientId` | Client identification | Yes - for monitoring |
| `database` | Current database context | Yes - for multi-tenant routing |
| `branch` | Git-like branch context | Yes - for branch operations |
| `connectedAt` | Connection age tracking | Yes - for idle timeout decisions |
| `lastActivity` | Activity tracking | Yes - for idle detection |
| `pendingRequests` | In-flight request IDs | Yes - for request resumption |
| `transaction` | Active transaction state | Yes - critical for ACID guarantees |
| `preparedStatements` | Statement cache | Yes - for prepared statement reuse |
| `metrics` | Connection statistics | Yes - for observability |

### WebSocket Tags

DoSQL uses WebSocket tags to enable efficient connection management:

```typescript
type WebSocketTag =
  | `client:${string}`    // Client identifier (e.g., "client:user-123")
  | `database:${string}`  // Database name (e.g., "database:myapp")
  | `branch:${string}`    // Branch name (e.g., "branch:feature-x")
  | `tx:${string}`        // Transaction ID (e.g., "tx:abc-123")
  | `session:${string}`   // Session ID (e.g., "session:uuid-here")
  | `notify:${string}`;   // Notification channel (e.g., "notify:orders")
```

Tags enable:
- Broadcasting to specific clients: `ctx.getWebSockets('database:myapp')`
- Transaction coordination: `ctx.getWebSockets('tx:abc-123')`
- Notification channels: `ctx.getWebSockets('notify:orders')`

### Hibernation Mixin

DoSQL provides a reusable `HibernationMixin` for adding hibernation support to any Durable Object:

```typescript
// Example usage
export class MyDatabase extends HibernationMixin(DurableObject) {
  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
    const session = this.getSessionState(ws);
    // Session is automatically restored from hibernation

    // Process message...
    const data = JSON.parse(message as string);

    // Update session metrics
    this.updateSessionState(ws, {
      metrics: {
        ...session.metrics,
        totalQueries: session.metrics.totalQueries + 1,
      },
    });

    // Send response
    this.sendResponse(ws, { id: data.id, result: 'ok' });

    // Allow hibernation after processing
    this.scheduleHibernation();
  }
}
```

### Attachment Lifecycle

1. **On WebSocket Accept**: Create initial session state
   ```typescript
   this.acceptWebSocket(ws, [`client:${clientId}`, `session:${sessionId}`], {
     clientId,
     database: params.get('database'),
     branch: params.get('branch'),
   });
   ```

2. **On Message Received**: Update session state
   ```typescript
   async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
     const session = this.getSessionState(ws);
     this.updateSessionState(ws, {
       lastActivity: Date.now(),
       metrics: {
         ...session.metrics,
         bytesReceived: session.metrics.bytesReceived + getMessageSize(message),
       },
     });
   }
   ```

3. **On Hibernation Wake**: Restore session state
   ```typescript
   // Automatic - getSessionState() returns the persisted state
   const session = this.getSessionState(ws);
   // session.sessionId, session.transaction, etc. are all restored
   ```

4. **On Connection Close**: Clean up transaction state
   ```typescript
   async webSocketClose(ws: WebSocket, code: number, reason: string) {
     const session = this.getSessionState(ws);
     if (session?.transaction) {
       // Rollback any active transaction
       await this.rollbackTransaction(session.transaction.txId);
     }
   }
   ```

### Hibernation Statistics

The `HibernationMixin` tracks hibernation statistics for monitoring:

```typescript
interface HibernationStats {
  /** Total number of hibernation cycles (sleeps) */
  totalSleeps: number;
  /** Total number of wake-ups from hibernation */
  totalWakes: number;
  /** Average sleep duration in milliseconds */
  averageSleepDuration: number;
  /** Total time spent sleeping in milliseconds */
  totalSleepTime: number;
  /** Estimated CPU time saved in milliseconds */
  cpuTimeSaved: number;
}
```

### Cost Savings

WebSocket Hibernation provides significant cost savings:

| State | Billing | Use Case |
|-------|---------|----------|
| Active | Full compute charges | Processing queries, transactions |
| Hibernating | 95% reduction | Idle connections waiting for messages |

For applications with many concurrent but idle connections (e.g., real-time dashboards, collaboration tools), hibernation can reduce Durable Object costs by 80-95%.

---

## Further Reading

- [Getting Started](./getting-started.md)
- [API Reference](./api-reference.md)
- [Advanced Features](./advanced.md)
