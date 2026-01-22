# DoSQL & DoLake Architecture Review

**Date**: 2026-01-22
**Reviewer**: Claude Code
**Repository**: `/Users/nathanclevenger/projects/sql`
**Status**: Early Stage - Package Structure Only

---

## Executive Summary

The `@dotdo/sql` monorepo is a **greenfield project** for building two complementary database products for Cloudflare Workers:

| Product | Purpose | Status |
|---------|---------|--------|
| **DoSQL** | SQLite-compatible SQL database engine for Durable Objects | POC Complete |
| **DoLake** | CDC-driven lakehouse for analytics on R2/Iceberg | POC Complete |

The monorepo has been initialized with the correct structure but contains no packages yet. Mature POC implementations exist in `/Users/nathanclevenger/projects/pocs/packages/{dosql,dolake}` ready for extraction and refinement.

---

## Current State Assessment

### Repository Structure

```
/Users/nathanclevenger/projects/sql/
├── .beads/              # Issue tracker database
├── .git/                # Git repository
├── AGENTS.md            # Agent instructions
├── CLAUDE.md            # Project context for Claude
├── README.md            # Project overview
├── package.json         # Root workspace config
├── pnpm-workspace.yaml  # pnpm workspace definition
└── packages/            # Empty - packages to be added
```

### Root package.json Analysis

```json
{
  "name": "@dotdo/sql",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": {
    "build": "pnpm -r build",
    "test": "pnpm -r test",
    "lint": "pnpm -r lint",
    "typecheck": "pnpm -r typecheck",
    "clean": "pnpm -r clean"
  },
  "engines": {
    "node": ">=20.0.0",
    "pnpm": ">=9.0.0"
  },
  "packageManager": "pnpm@9.15.0"
}
```

**Observations:**
- Uses pnpm workspaces (correct for monorepo)
- Modern Node.js requirement (>=20)
- Standard script conventions
- Missing: devDependencies for TypeScript, testing, linting

---

## POC Analysis

### DoSQL POC (`/Users/nathanclevenger/projects/pocs/packages/dosql`)

**Feature completeness**: Highly mature, production-ready POC

#### Module Structure

```
src/
├── parser/           # SQL parser with type-level inference
│   ├── case.ts       # Case expression parsing
│   ├── cte.ts        # Common Table Expressions
│   ├── ddl.ts        # CREATE/DROP/ALTER
│   ├── dml.ts        # INSERT/UPDATE/DELETE
│   ├── returning.ts  # RETURNING clause
│   ├── set-ops.ts    # UNION/INTERSECT/EXCEPT
│   ├── subquery.ts   # Subquery support
│   └── window.ts     # Window functions
├── btree/            # B-tree index implementation
├── wal/              # Write-ahead log
├── fsx/              # File system abstraction (DO, R2, Memory)
├── sharding/         # Horizontal scaling and query routing
├── transaction/      # ACID transactions with MVCC
├── cdc/              # Change Data Capture
├── planner/          # Query planner and optimizer
├── engine/           # Execution engine
├── branch/           # Database branching (git-like)
├── timetravel/       # Point-in-time queries
├── columnar/         # Columnar storage format
├── compaction/       # Compaction strategies
├── proc/             # Stored procedures (ESM modules)
├── functions/        # Built-in SQL functions
├── virtual/          # Virtual tables (URL sources)
├── migrations/       # Schema migration system
├── rpc/              # Durable Object RPC
├── replication/      # Cross-DO replication
├── fts/              # Full-text search
├── vector/           # Vector similarity search
├── view/             # Views and materialized views
├── lakehouse/        # Lakehouse integration
├── iceberg/          # Iceberg format support
└── orm/              # ORM adapters (Prisma, Kysely, Knex, Drizzle)
```

#### Key Features Implemented

| Feature | Status | Notes |
|---------|--------|-------|
| Type-safe SQL | Complete | Compile-time query validation |
| B-tree storage | Complete | DO-optimized with COW |
| WAL | Complete | Durable with CRC checksums |
| Transactions | Complete | ACID with savepoints |
| Sharding | Complete | Hash/range partitioning |
| CDC | Complete | WebSocket streaming |
| Time Travel | Complete | Point-in-time queries |
| Branching | Complete | Git-like database branches |
| Virtual Tables | Complete | Query URLs/R2 directly |
| Stored Procedures | Complete | ESM-based procedures |

#### Bundle Size (Critical for Workers)

| Bundle | Gzipped | % of 1MB limit |
|--------|---------|----------------|
| Worker (minimal) | 7.36 KB | 0.7% |
| Full library | 34.26 KB | 3.3% |

### DoLake POC (`/Users/nathanclevenger/projects/pocs/packages/dolake`)

**Feature completeness**: Well-structured POC with core functionality

#### Module Structure

```
src/
├── dolake.ts         # Main Durable Object (~80KB - needs splitting)
├── buffer.ts         # CDC event buffering
├── parquet.ts        # Parquet file writing
├── iceberg.ts        # Iceberg metadata management
├── catalog.ts        # REST Catalog API
├── compaction.ts     # File compaction
├── partitioning.ts   # Table partitioning
├── query-engine.ts   # Analytical queries
├── rate-limiter.ts   # WebSocket rate limiting
├── scalability.ts    # Scaling strategies
├── schemas.ts        # Zod validation schemas
└── types.ts          # TypeScript types
```

#### Key Features Implemented

| Feature | Status | Notes |
|---------|--------|-------|
| CDC WebSocket | Complete | With hibernation (95% cost reduction) |
| Parquet writing | Complete | Schema inference |
| Iceberg metadata | Complete | Full manifest management |
| REST Catalog | Complete | Spark/DuckDB compatible |
| Buffer management | Complete | Smart batching |
| Deduplication | Complete | At-least-once delivery |
| Fallback storage | Complete | DO storage when R2 fails |

---

## Recommended Target Architecture

### High-Level System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           CLIENT APPLICATIONS                                │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │  Web App (JS)   │  │   Mobile App    │  │    CLI Tool     │             │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘             │
└───────────┼────────────────────┼────────────────────┼───────────────────────┘
            │                    │                    │
            └────────────────────┼────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          CLOUDFLARE WORKERS                                  │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                         DOSQL GATEWAY                                  │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │  │
│  │  │ SQL Parser  │  │   Planner   │  │  Optimizer  │  │Shard Router │   │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘   │  │
│  └────────────────────────────────────┬──────────────────────────────────┘  │
│                                       │                                      │
│         ┌─────────────────────────────┼─────────────────────────────┐       │
│         │                             │                             │       │
│         ▼                             ▼                             ▼       │
│  ┌─────────────┐              ┌─────────────┐              ┌─────────────┐  │
│  │   Shard 0   │              │   Shard 1   │              │   Shard N   │  │
│  │  (DO+SQLite)│              │  (DO+SQLite)│              │  (DO+SQLite)│  │
│  │  ┌───────┐  │              │  ┌───────┐  │              │  ┌───────┐  │  │
│  │  │B-tree │  │              │  │B-tree │  │              │  │B-tree │  │  │
│  │  │ WAL   │  │              │  │ WAL   │  │              │  │ WAL   │  │  │
│  │  │ MVCC  │  │              │  │ MVCC  │  │              │  │ MVCC  │  │  │
│  │  └───────┘  │              │  └───────┘  │              │  └───────┘  │  │
│  └──────┬──────┘              └──────┬──────┘              └──────┬──────┘  │
│         │                            │                            │         │
│         └────────────────────────────┼────────────────────────────┘         │
│                                      │ CDC Events (WebSocket)               │
│                                      ▼                                      │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                           DOLAKE AGGREGATOR                            │  │
│  │  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────────────┐   │  │
│  │  │  Buffer   │  │ Deduper   │  │ Compactor │  │  Iceberg Writer   │   │  │
│  │  └───────────┘  └───────────┘  └───────────┘  └───────────────────┘   │  │
│  └────────────────────────────────────┬──────────────────────────────────┘  │
│                                       │                                      │
└───────────────────────────────────────┼──────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              R2 LAKEHOUSE                                    │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                        ICEBERG TABLE FORMAT                            │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │  │
│  │  │  Manifests  │  │  Snapshots  │  │   Parquet   │  │   Schemas   │   │  │
│  │  │   (JSON)    │  │   (JSON)    │  │   (Data)    │  │   (Avro)    │   │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘   │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         EXTERNAL QUERY ENGINES                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │   DuckDB    │  │    Spark    │  │    Trino    │  │    Flink    │        │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Package Organization

Based on the POC analysis and EvoDB patterns, I recommend the following package structure:

```
packages/
├── dosql/                      # @dotdo/dosql - Main database engine
│   ├── src/
│   │   ├── index.ts            # Public API
│   │   ├── database.ts         # Database class
│   │   ├── parser/             # SQL parser
│   │   ├── planner/            # Query planning
│   │   ├── engine/             # Execution
│   │   └── types.ts            # Core types
│   └── package.json
│
├── dosql-btree/                # @dotdo/dosql-btree - B-tree implementation
│   ├── src/
│   │   ├── index.ts
│   │   ├── node.ts             # Node structure
│   │   ├── page.ts             # Page management
│   │   └── codec.ts            # Key/value encoding
│   └── package.json
│
├── dosql-wal/                  # @dotdo/dosql-wal - Write-ahead log
│   ├── src/
│   │   ├── index.ts
│   │   ├── writer.ts           # WAL writer
│   │   ├── reader.ts           # WAL reader
│   │   └── checkpoint.ts       # Checkpointing
│   └── package.json
│
├── dosql-fsx/                  # @dotdo/dosql-fsx - Storage abstraction
│   ├── src/
│   │   ├── index.ts
│   │   ├── do-backend.ts       # Durable Object storage
│   │   ├── r2-backend.ts       # R2 storage
│   │   ├── memory-backend.ts   # In-memory (testing)
│   │   └── tiered-backend.ts   # Hot/cold tiering
│   └── package.json
│
├── dosql-transaction/          # @dotdo/dosql-transaction - ACID transactions
│   ├── src/
│   │   ├── index.ts
│   │   ├── manager.ts          # Transaction manager
│   │   ├── mvcc.ts             # Multi-version concurrency
│   │   ├── lock.ts             # Lock manager
│   │   └── savepoint.ts        # Savepoint support
│   └── package.json
│
├── dosql-sharding/             # @dotdo/dosql-sharding - Horizontal scaling
│   ├── src/
│   │   ├── index.ts
│   │   ├── router.ts           # Query routing
│   │   ├── executor.ts         # Distributed execution
│   │   └── partition.ts        # Partition strategies
│   └── package.json
│
├── dosql-cdc/                  # @dotdo/dosql-cdc - Change Data Capture
│   ├── src/
│   │   ├── index.ts
│   │   ├── producer.ts         # CDC event producer
│   │   ├── consumer.ts         # CDC event consumer
│   │   └── types.ts            # Event types
│   └── package.json
│
├── dosql-rpc/                  # @dotdo/dosql-rpc - DO-to-DO communication
│   ├── src/
│   │   ├── index.ts
│   │   ├── client.ts           # RPC client
│   │   ├── server.ts           # RPC server
│   │   └── protocol.ts         # Wire protocol
│   └── package.json
│
├── dolake/                     # @dotdo/dolake - Lakehouse aggregator
│   ├── src/
│   │   ├── index.ts
│   │   ├── dolake.ts           # Durable Object
│   │   ├── buffer.ts           # Event buffering
│   │   ├── compaction.ts       # File compaction
│   │   └── query.ts            # Query engine
│   └── package.json
│
├── dolake-parquet/             # @dotdo/dolake-parquet - Parquet operations
│   ├── src/
│   │   ├── index.ts
│   │   ├── writer.ts           # Parquet writer
│   │   ├── reader.ts           # Parquet reader
│   │   └── schema.ts           # Schema inference
│   └── package.json
│
├── dolake-iceberg/             # @dotdo/dolake-iceberg - Iceberg metadata
│   ├── src/
│   │   ├── index.ts
│   │   ├── manifest.ts         # Manifest management
│   │   ├── snapshot.ts         # Snapshot management
│   │   └── catalog.ts          # REST Catalog API
│   └── package.json
│
└── shared/                     # @dotdo/sql-shared - Common utilities
    ├── src/
    │   ├── index.ts
    │   ├── types.ts            # Shared types
    │   └── utils.ts            # Common utilities
    └── package.json
```

### Package Dependency Graph

```
                               @dotdo/sql-shared
                                      │
           ┌──────────────────────────┼──────────────────────────┐
           │                          │                          │
           ▼                          ▼                          ▼
    @dotdo/dosql-fsx          @dotdo/dosql-wal         @dotdo/dosql-btree
           │                          │                          │
           └──────────────────────────┼──────────────────────────┘
                                      │
                                      ▼
                          @dotdo/dosql-transaction
                                      │
           ┌──────────────────────────┼──────────────────────────┐
           │                          │                          │
           ▼                          ▼                          ▼
    @dotdo/dosql-cdc          @dotdo/dosql-rpc         @dotdo/dosql-sharding
           │                          │                          │
           └──────────────────────────┼──────────────────────────┘
                                      │
                                      ▼
                                @dotdo/dosql
                                      │
                                      │ CDC Events
                                      ▼
                     ┌────────────────┴────────────────┐
                     │                                 │
                     ▼                                 ▼
           @dotdo/dolake-parquet            @dotdo/dolake-iceberg
                     │                                 │
                     └─────────────┬───────────────────┘
                                   │
                                   ▼
                             @dotdo/dolake
```

---

## Data Flow Patterns

### OLTP Write Path (DoSQL)

```
1. CLIENT REQUEST
   │
   ▼
2. GATEWAY (Workers)
   │  - Parse SQL
   │  - Identify target shard(s)
   │  - Route to appropriate DO
   │
   ▼
3. SHARD DO (DoSQL)
   │  - Acquire transaction lock
   │  - Execute statement
   │  - Update B-tree
   │  - Append to WAL
   │  - Generate CDC event
   │
   ▼ WebSocket (hibernation)
   │
4. DOLAKE AGGREGATOR
   │  - Buffer CDC events
   │  - Deduplicate
   │  - Batch by table/partition
   │
   ▼ Threshold trigger
   │
5. R2 LAKEHOUSE
   │  - Write Parquet file
   │  - Update Iceberg manifest
   │  - Create snapshot
```

### OLAP Read Path (DoLake)

```
1. ANALYTICAL QUERY
   │
   ▼
2. DOLAKE QUERY ENGINE
   │  - Parse query
   │  - Load manifest from R2
   │  - Prune partitions (zone maps)
   │  - Plan execution
   │
   ▼ Parallel fetch
   │
3. R2 STORAGE
   │  - Read Parquet files
   │  - Column projection
   │  - Predicate pushdown
   │
   ▼
4. RESULT AGGREGATION
   │  - Merge partial results
   │  - Apply filters
   │  - Sort/aggregate
   │
   ▼
5. CLIENT RESPONSE
```

### Time Travel Query

```
1. QUERY WITH TIMESTAMP
   │  SELECT * FROM users
   │  FOR SYSTEM_TIME AS OF '2024-01-01 10:00:00'
   │
   ▼
2. DOLAKE SNAPSHOT LOOKUP
   │  - Find snapshot at timestamp
   │  - Resolve manifest chain
   │
   ▼
3. HISTORICAL DATA READ
   │  - Read from historical Parquet files
   │  - Apply schema at that point
   │
   ▼
4. RESULT (point-in-time view)
```

---

## Storage Architecture

### Hot Tier (Durable Object Storage)

```
DO Storage (2MB blob limit)
├── _meta/
│   ├── schema.json        # Table schemas
│   ├── branches.json      # Branch metadata
│   └── config.json        # Database config
├── _wal/
│   ├── segment_00001      # WAL segments
│   ├── segment_00002
│   └── checkpoint         # Last checkpoint LSN
├── _btree/
│   ├── users/             # Per-table B-tree
│   │   ├── root.page
│   │   ├── page_001
│   │   └── page_002
│   └── orders/
│       ├── root.page
│       └── ...
└── _snapshots/
    ├── main@001.json      # Snapshot metadata
    └── feature-x@001.json
```

### Cold Tier (R2 Lakehouse)

```
R2 Bucket
├── warehouse/
│   └── default/           # Namespace
│       └── users/         # Table
│           ├── metadata/
│           │   ├── v1.metadata.json
│           │   └── v2.metadata.json
│           ├── data/
│           │   ├── year=2024/
│           │   │   ├── month=01/
│           │   │   │   ├── 00001.parquet
│           │   │   │   └── 00002.parquet
│           │   │   └── month=02/
│           │   │       └── ...
│           │   └── year=2025/
│           │       └── ...
│           └── manifests/
│               ├── snap-001.avro
│               └── snap-002.avro
└── _dolake/
    ├── buffers/           # Fallback buffers
    └── state/             # Coordinator state
```

---

## API Design Recommendations

### DoSQL API

```typescript
// Core database interface
interface DoSQLDatabase {
  // Query execution
  query<T>(sql: string, params?: unknown[]): Promise<T[]>;
  run(sql: string, params?: unknown[]): Promise<RunResult>;
  exec(sql: string): Promise<void>;

  // Prepared statements
  prepare<T>(sql: string): PreparedStatement<T>;

  // Transactions
  transaction<T>(fn: (tx: Transaction) => Promise<T>): Promise<T>;

  // Branching
  branch(name: string): Promise<void>;
  checkout(name: string): Promise<void>;
  merge(source: string): Promise<MergeResult>;

  // Time travel
  at(timestamp: Date | string | bigint): DoSQLDatabase;

  // CDC
  subscribe(options?: CDCOptions): AsyncIterable<CDCEvent>;

  // Maintenance
  checkpoint(): Promise<void>;
  vacuum(): Promise<VacuumResult>;
}

// Usage
import { DoSQL } from '@dotdo/dosql';

export class TenantDB implements DurableObject {
  private db: DoSQLDatabase;

  constructor(state: DurableObjectState, env: Env) {
    this.db = DoSQL(state, {
      migrations: { folder: '.do/migrations' },
      storage: { cold: env.R2_BUCKET },
      cdc: { target: env.DOLAKE },
    });
  }

  async fetch(request: Request): Promise<Response> {
    const { sql, params } = await request.json();
    const result = await this.db.query(sql, params);
    return Response.json(result);
  }
}
```

### DoLake API

```typescript
// Lakehouse interface
interface DoLakeDatabase {
  // CDC ingestion
  ingest(events: CDCEvent[]): Promise<IngestResult>;

  // Query execution
  query<T>(sql: string): Promise<T[]>;
  scan(table: string, options?: ScanOptions): AsyncIterable<Row>;

  // Table management
  createTable(name: string, schema: TableSchema): Promise<void>;
  dropTable(name: string): Promise<void>;

  // Snapshot management
  snapshot(message?: string): Promise<SnapshotId>;
  listSnapshots(): Promise<Snapshot[]>;
  rollback(snapshotId: SnapshotId): Promise<void>;

  // Maintenance
  compact(options?: CompactOptions): Promise<CompactResult>;
  vacuum(options?: VacuumOptions): Promise<VacuumResult>;

  // Catalog
  catalog(): IcebergCatalog;
}

// Usage
import { DoLake } from '@dotdo/dolake';

export class LakehouseAggregator implements DurableObject {
  private lake: DoLakeDatabase;

  constructor(state: DurableObjectState, env: Env) {
    this.lake = DoLake(state, {
      bucket: env.R2_BUCKET,
      namespace: 'default',
      flush: {
        maxEvents: 10000,
        maxBytes: 32 * 1024 * 1024,
        maxAge: 60000,
      },
    });
  }

  async webSocketMessage(ws: WebSocket, message: string) {
    const { events } = JSON.parse(message);
    await this.lake.ingest(events);
  }
}
```

---

## Scalability Analysis

### DoSQL Scaling Dimensions

| Dimension | Strategy | Limit |
|-----------|----------|-------|
| **Tenants** | 1 DO per tenant | Unlimited |
| **Data per tenant** | Hot/cold tiering | DO: ~500MB, R2: Unlimited |
| **QPS per tenant** | Single DO | ~1000 QPS |
| **Cross-tenant queries** | Shard router | 10-100 shards typical |

### DoLake Scaling Dimensions

| Dimension | Strategy | Limit |
|-----------|----------|-------|
| **CDC throughput** | WebSocket hibernation | Thousands of producers |
| **Storage** | R2 + Parquet | Petabytes |
| **Query concurrency** | Workers parallelism | High |
| **Partitions** | Iceberg partitioning | Unlimited |

### Scaling Architecture

```
                     ┌─────────────────────────────────────────┐
                     │           GLOBAL SHARD ROUTER           │
                     │  (Consistent hashing + locality hints)  │
                     └────────────────────┬────────────────────┘
                                          │
         ┌────────────────────────────────┼────────────────────────────────┐
         │                                │                                │
         ▼                                ▼                                ▼
┌─────────────────┐              ┌─────────────────┐              ┌─────────────────┐
│  SHARD GROUP A  │              │  SHARD GROUP B  │              │  SHARD GROUP C  │
│  (US-WEST)      │              │  (EU-WEST)      │              │  (APAC)         │
│  ┌───────────┐  │              │  ┌───────────┐  │              │  ┌───────────┐  │
│  │ DO Shard 1│  │              │  │ DO Shard 1│  │              │  │ DO Shard 1│  │
│  │ DO Shard 2│  │              │  │ DO Shard 2│  │              │  │ DO Shard 2│  │
│  │ DO Shard N│  │              │  │ DO Shard N│  │              │  │ DO Shard N│  │
│  └───────────┘  │              │  └───────────┘  │              │  └───────────┘  │
└────────┬────────┘              └────────┬────────┘              └────────┬────────┘
         │                                │                                │
         │                    CDC Events  │                                │
         │                                │                                │
         ▼                                ▼                                ▼
┌─────────────────┐              ┌─────────────────┐              ┌─────────────────┐
│  DOLAKE (US)    │              │  DOLAKE (EU)    │              │  DOLAKE (APAC)  │
└────────┬────────┘              └────────┬────────┘              └────────┬────────┘
         │                                │                                │
         └────────────────────────────────┼────────────────────────────────┘
                                          │
                                          ▼
                              ┌─────────────────────┐
                              │   R2 (GLOBAL)       │
                              │  Unified Lakehouse  │
                              └─────────────────────┘
```

---

## Cloudflare Constraints Reference

| Resource | Workers | Durable Objects | Snippets |
|----------|---------|-----------------|----------|
| **CPU per request** | 50ms | 30s | 5ms |
| **Memory** | 128MB | 128MB | 32MB |
| **Bundle size** | 1MB | 1MB | Minimal |
| **Subrequests** | 1000 | Unlimited | 5 |
| **Storage** | - | 128KB entries | - |
| **Blob size** | - | 2MB | - |

### Optimization Strategies

1. **Bundle size**: Tree-shakeable modules, no WASM
2. **CPU**: Streaming execution, index-based queries
3. **Memory**: Columnar format, projection pushdown
4. **Storage**: Chunked blobs, hot/cold tiering
5. **Latency**: Location hints, cache-first reads

---

## Recommendations

### Immediate Actions

1. **Create base packages**
   - `@dotdo/sql-shared` for common types/utilities
   - `@dotdo/dosql-fsx` for storage abstraction
   - `@dotdo/dosql-btree` for B-tree implementation

2. **Set up build infrastructure**
   - TypeScript 5.4+ with strict mode
   - Vitest for testing (NOT vitest watch mode in parallel)
   - ESLint + Prettier
   - Changesets for versioning

3. **Extract from POCs**
   - Prioritize core modules (fsx, btree, wal)
   - Add comprehensive tests before extraction
   - Document public APIs

### Short-term Goals

1. **DoSQL v0.1.0-rc.1**
   - Core query execution
   - Transaction support
   - Basic migrations
   - DO storage backend

2. **DoLake v0.1.0-rc.1**
   - CDC ingestion
   - Parquet writing
   - Basic Iceberg metadata
   - REST Catalog endpoints

### Long-term Goals

1. **Production readiness**
   - Sharding support
   - Cross-region replication
   - Comprehensive observability

2. **Ecosystem integration**
   - ORM adapters (Drizzle, Prisma, Kysely)
   - GraphQL schema generation
   - EvoDB compatibility layer

---

## Comparison with EvoDB

| Aspect | DoSQL/DoLake | EvoDB |
|--------|--------------|-------|
| **Primary focus** | SQL compatibility | Schema evolution |
| **Query language** | SQL | Custom query builder |
| **Storage format** | B-tree + Parquet | Columnar JSON blocks |
| **Sharding** | Built-in | Via lakehouse |
| **Time travel** | WAL-based | Snapshot-based |
| **Target users** | SQL developers | Document DB users |

### Integration Opportunity

DoSQL and EvoDB can complement each other:
- DoSQL for OLTP with SQL compatibility
- EvoDB for schema-evolving document storage
- Shared DoLake for analytics

---

## Conclusion

The `@dotdo/sql` monorepo is well-positioned for development with:
- Mature POC implementations ready for extraction
- Clear architectural patterns from EvoDB
- Strong alignment with Cloudflare's platform constraints

The recommended architecture separates concerns appropriately while maintaining a minimal bundle footprint suitable for Workers deployment.

**Next Steps:**
1. Create initial packages with shared infrastructure
2. Extract and refine POC code into modular packages
3. Establish testing and CI/CD pipelines
4. Document public APIs with TypeDoc

---

*Generated by Claude Code - 2026-01-22*
