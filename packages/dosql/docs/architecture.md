# DoSQL Architecture

This document describes the architecture of the DoSQL ecosystem, a type-safe SQL database system built natively for Cloudflare Workers and Durable Objects.

## Table of Contents

- [Package Overview](#package-overview)
- [Architecture Diagram](#architecture-diagram)
- [Client SDK Integration](#client-sdk-integration)
- [Core Components](#core-components)
- [Data Flow](#data-flow)
- [Storage Tiers](#storage-tiers)

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
         │
         ├────────────────────────────────────────┐
         │                                        │
         ▼                                        ▼
   @dotdo/sql.do                            @dotdo/lake.do
   (client SDK)                             (lake client SDK)
         │                                        │
         ▼                                        ▼
   @dotdo/dosql ─────────────────────────► @dotdo/dolake
   (SQL engine DO)        CDC              (lakehouse DO)
```

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Client Application                                  │
│  ┌─────────────────────────────────────┐  ┌─────────────────────────────────┐   │
│  │         @dotdo/sql.do               │  │         @dotdo/lake.do          │   │
│  │  • Type-safe SQL queries            │  │  • Lakehouse queries            │   │
│  │  • Prepared statements              │  │  • CDC stream subscriptions     │   │
│  │  • Transaction context              │  │  • Time travel queries          │   │
│  └─────────────────┬───────────────────┘  └─────────────────┬───────────────┘   │
└────────────────────┼────────────────────────────────────────┼───────────────────┘
                     │                                        │
                     │ WebSocket/HTTP                         │ WebSocket/HTTP
                     │ (CapnWeb RPC)                          │ (CapnWeb RPC)
                     │                                        │
┌────────────────────┼────────────────────────────────────────┼───────────────────┐
│                    │         Cloudflare Workers             │                    │
│                    ▼                                        ▼                    │
│  ┌─────────────────────────────────────┐  ┌─────────────────────────────────┐   │
│  │         @dotdo/dosql                │  │         @dotdo/dolake           │   │
│  │     (Durable Object)                │  │     (Durable Object)            │   │
│  │                                     │  │                                 │   │
│  │  ┌───────────────────────────────┐  │  │  ┌───────────────────────────┐  │   │
│  │  │ Parser (Type-level SQL)       │  │  │  │ CDC Buffer Manager        │  │   │
│  │  │ • Compile-time validation     │  │  │  │ • Multi-shard aggregation │  │   │
│  │  │ • Result type inference       │  │  │  │ • Deduplication           │  │   │
│  │  └───────────────────────────────┘  │  │  └───────────────────────────┘  │   │
│  │  ┌───────────────────────────────┐  │  │  ┌───────────────────────────┐  │   │
│  │  │ Query Executor                │  │  │  │ Parquet Writer            │  │   │
│  │  │ • Plan optimization           │  │  │  │ • Columnar encoding       │  │   │
│  │  │ • Operator execution          │  │  │  │ • Compression             │  │   │
│  │  └───────────────────────────────┘  │  │  └───────────────────────────┘  │   │
│  │  ┌───────────────────────────────┐  │  │  ┌───────────────────────────┐  │   │
│  │  │ Transaction Manager           │◄─┼──┼──┤ Iceberg Catalog           │  │   │
│  │  │ • ACID guarantees             │  │  │  │ • REST API                │  │   │
│  │  │ • Savepoints, MVCC            │  │  │  │ • Metadata management     │  │   │
│  │  └───────────────────────────────┘  │  │  └───────────────────────────┘  │   │
│  │  ┌───────────────────────────────┐  │  │  ┌───────────────────────────┐  │   │
│  │  │ WAL Writer                    │──┼──┼─►│ Query Engine              │  │   │
│  │  │ • Durability                  │CDC│  │  │ • Partition pruning       │  │   │
│  │  │ • Recovery                    │  │  │  │ • Predicate pushdown      │  │   │
│  │  └───────────────────────────────┘  │  │  └───────────────────────────┘  │   │
│  │                 │                   │  │                 │               │   │
│  └─────────────────┼───────────────────┘  └─────────────────┼───────────────┘   │
│                    │                                        │                    │
│                    ▼                                        ▼                    │
│  ┌─────────────────────────────────────┐  ┌─────────────────────────────────┐   │
│  │        DO Storage (Hot)             │  │           R2 (Cold)             │   │
│  │  • 2MB blob chunks                  │  │  • Parquet data files           │   │
│  │  • Low latency (~1ms)               │  │  • Iceberg metadata             │   │
│  │  • B-tree pages, WAL segments       │  │  • Unlimited capacity           │   │
│  └─────────────────────────────────────┘  └─────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
                                                              │
                                                              ▼
                                              ┌─────────────────────────────────┐
                                              │   External Query Engines        │
                                              │  Spark │ DuckDB │ Trino │ Flink │
                                              │     (via Iceberg REST Catalog)  │
                                              └─────────────────────────────────┘
```

---

## Client SDK Integration

### sql.do Client

The `@dotdo/sql.do` package provides type-safe SQL access via CapnWeb RPC over WebSocket:

```typescript
import { createSQLClient } from '@dotdo/sql.do';

const client = createSQLClient({
  url: 'https://sql.example.com',
  token: 'your-token',
});

// Queries with automatic reconnection
const result = await client.query('SELECT * FROM users WHERE id = ?', [1]);

// Transactions with proper isolation
await client.transaction(async (tx) => {
  await tx.exec('INSERT INTO users (name) VALUES (?)', ['Alice']);
  await tx.exec('INSERT INTO logs (action) VALUES (?)', ['user_created']);
});
```

### lake.do Client

The `@dotdo/lake.do` package provides lakehouse access and CDC streaming:

```typescript
import { createLakeClient } from '@dotdo/lake.do';

const lake = createLakeClient({
  url: 'https://lake.example.com',
});

// Analytical queries on cold data
const result = await lake.query<{ date: string; total: number }>(
  'SELECT date, SUM(amount) as total FROM orders GROUP BY date'
);

// Subscribe to real-time CDC stream
for await (const batch of lake.subscribe({ tables: ['orders'] })) {
  for (const event of batch.events) {
    console.log(event.operation, event.table, event.after);
  }
}
```

### CapnWeb Protocol

Both clients use CapnWeb RPC for efficient communication:

```
┌─────────────────────────────────────────────────────────────────┐
│                       CapnWeb RPC Protocol                       │
├──────────────┬──────────────────────────────────────────────────┤
│ Transport    │ WebSocket (primary) or HTTP batch                 │
├──────────────┼──────────────────────────────────────────────────┤
│ Serialization│ JSON with BigInt support (via branded types)     │
├──────────────┼──────────────────────────────────────────────────┤
│ Methods      │ exec, query, prepare, execute, beginTransaction, │
│              │ commit, rollback, getSchema, ping                │
├──────────────┼──────────────────────────────────────────────────┤
│ Streaming    │ Async iterators for CDC and large result sets    │
├──────────────┼──────────────────────────────────────────────────┤
│ Pipelining   │ Multiple requests over single connection         │
└──────────────┴──────────────────────────────────────────────────┘
```

### Type Safety Across Boundaries

The `@dotdo/shared-types` package ensures type consistency:

```typescript
// Branded types prevent mixing incompatible IDs
type TransactionId = string & { readonly [TransactionIdBrand]: never };
type LSN = bigint & { readonly [LSNBrand]: never };
type StatementHash = string & { readonly [StatementHashBrand]: never };
type ShardId = string & { readonly [ShardIdBrand]: never };

// Unified CDC event type used by all packages
interface CDCEvent<T = unknown> {
  lsn: bigint | LSN;
  table: string;
  operation: CDCOperation;
  timestamp: Date | number;
  transactionId?: string | TransactionId;
  before?: T;
  after?: T;
}
```

---

## Core Components

### Parser (Type-Level SQL)

DoSQL parses SQL at both compile-time and runtime for type safety:

```typescript
// Type-level parsing infers result types from SQL strings
type Result = SQL<'SELECT id, name FROM users WHERE active = true', MySchema>;
// Result = { id: number; name: string }[]

// Runtime parsing validates and optimizes queries
const db = createDatabase(schema);
const result = db.query('SELECT id, name FROM users WHERE active = true');
```

The parser supports:
- SELECT with JOIN, WHERE, GROUP BY, ORDER BY, LIMIT
- INSERT, UPDATE, DELETE with RETURNING
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- Subqueries and CTEs
- Prepared statements with parameter binding

### Executor

The query executor transforms parsed SQL into execution plans:

```
┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐
│   SQL   │───►│ Parser  │───►│ Planner │───►│Executor │───►│ Result  │
└─────────┘    └─────────┘    └─────────┘    └─────────┘    └─────────┘
                   │              │              │
                   ▼              ▼              ▼
              ┌─────────┐   ┌─────────┐   ┌─────────┐
              │   AST   │   │  Plan   │   │Operators│
              └─────────┘   │  Tree   │   │ Scan    │
                            └─────────┘   │ Filter  │
                                          │ Project │
                                          │ Sort    │
                                          │ Join    │
                                          └─────────┘
```

### Transaction Manager

Implements SQLite-compatible transaction semantics:

```typescript
// Transaction states
enum TransactionState {
  NONE,
  ACTIVE,
  SAVEPOINT,
}

// Isolation levels
type IsolationLevel =
  | 'READ_UNCOMMITTED'
  | 'READ_COMMITTED'
  | 'REPEATABLE_READ'
  | 'SERIALIZABLE'
  | 'SNAPSHOT';

// Lock types for concurrency control
type LockType = 'SHARED' | 'RESERVED' | 'PENDING' | 'EXCLUSIVE';
```

Features:
- Savepoints for nested transactions
- MVCC for snapshot isolation
- Lock manager for serializable isolation
- Automatic rollback on errors

### Write-Ahead Log (WAL)

The WAL provides durability and enables CDC:

```
┌─────────────────────────────────────────────────────────────────┐
│                         WAL Segment                              │
├──────────────┬──────────────┬──────────────┬────────────────────┤
│   Header     │   Entry 1    │   Entry 2    │       ...          │
│  (magic,     │  (LSN, op,   │  (LSN, op,   │                    │
│   version)   │   data, CRC) │   data, CRC) │                    │
└──────────────┴──────────────┴──────────────┴────────────────────┘
```

WAL entry structure:
```typescript
interface WALEntry {
  lsn: bigint;              // Log Sequence Number
  timestamp: number;        // Unix timestamp
  txnId: string;            // Transaction ID
  op: WALOperation;         // INSERT, UPDATE, DELETE, BEGIN, COMMIT, ROLLBACK
  table: string;            // Target table
  key?: Uint8Array;         // Row key
  before?: Uint8Array;      // Data before operation
  after?: Uint8Array;       // Data after operation
  checksum: number;         // CRC32 for integrity
}
```

### Change Data Capture (CDC)

CDC streams database changes to the lakehouse:

```typescript
import { createCDC } from '@dotdo/dosql/cdc';

const cdc = createCDC(backend);

// Subscribe with filters
for await (const event of cdc.subscribeChanges(0n, {
  tables: ['users', 'orders'],
  operations: ['INSERT', 'UPDATE']
})) {
  // event.type: 'insert' | 'update' | 'delete'
  // event.data: row data
}

// Replication slots for durable position tracking
await cdc.slots.createSlot('lakehouse-sync', 0n);
const subscription = await cdc.slots.subscribeFromSlot('lakehouse-sync');
// ... process events ...
await cdc.slots.updateSlot('lakehouse-sync', lastProcessedLSN);
```

---

## Data Flow

### Query Execution Path

```
Client Request
      │
      ▼
┌─────────────────┐
│  RPC Handler    │  Parse request, authenticate
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  SQL Parser     │  Tokenize, parse, validate
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Query Planner  │  Optimize, create execution plan
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Transaction    │  Acquire locks, create snapshot
│  Manager        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Executor       │  Run operators against B-tree
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  FSX Backend    │  Read/write DO storage
└────────┬────────┘
         │
         ▼
   Query Result
```

### Write Path with WAL

```
INSERT/UPDATE/DELETE
         │
         ▼
┌─────────────────┐
│  Transaction    │  Begin or use existing
└────────┬────────┘
         │
    ┌────┴────┐
    │         │
    ▼         ▼
┌───────┐  ┌───────┐
│  WAL  │  │ B-tree│  Write in parallel
│ Write │  │ Write │
└───┬───┘  └───┬───┘
    │         │
    └────┬────┘
         │
         ▼
┌─────────────────┐
│  Commit         │  Flush WAL, release locks
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  CDC Capture    │  Emit change event
└────────┬────────┘
         │
         ▼
   Lakehouse (async)
```

### CDC to Lakehouse Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DoSQL Instances (Shards)                            │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐                         │
│  │ Shard 1 │  │ Shard 2 │  │ Shard 3 │  │ Shard N │                         │
│  │  (WAL)  │  │  (WAL)  │  │  (WAL)  │  │  (WAL)  │                         │
│  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘                         │
│       │            │            │            │                              │
│       │ WebSocket  │ WebSocket  │ WebSocket  │ WebSocket                    │
│       │ CDC Stream │ CDC Stream │ CDC Stream │ CDC Stream                   │
│       ▼            ▼            ▼            ▼                              │
│  ┌───────────────────────────────────────────────────────────────┐          │
│  │                      DoLake (Aggregator)                       │          │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │          │
│  │  │ CDC Buffer   │  │ Parquet      │  │ Iceberg      │         │          │
│  │  │ Manager      │──│ Writer       │──│ Catalog      │         │          │
│  │  │ (dedup,      │  │ (columnar,   │  │ (metadata,   │         │          │
│  │  │  partition)  │  │  compress)   │  │  snapshots)  │         │          │
│  │  └──────────────┘  └──────────────┘  └──────────────┘         │          │
│  └───────────────────────────────────────────────────────────────┘          │
│                              │                                              │
│                              ▼                                              │
│                        ┌──────────┐                                         │
│                        │    R2    │  Parquet files + Iceberg metadata       │
│                        └──────────┘                                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Storage Tiers

### Hot Tier (Durable Object Storage)

Primary storage for active data with strong consistency:

| Property | Value |
|----------|-------|
| Latency | ~1ms |
| Max value size | 2MB |
| Consistency | Strong (single-leader) |
| Use cases | Working set, WAL, indexes, recent data |

```typescript
const db = await DB('tenant', {
  storage: {
    hot: state.storage,  // DurableObjectStorage
  },
});
```

### Warm Tier (R2 with Caching)

For less frequently accessed data with optional caching:

| Property | Value |
|----------|-------|
| Latency | ~50-100ms |
| Capacity | Unlimited |
| Format | Binary pages |
| Use cases | Historical data, large tables |

### Cold Tier (Parquet/Iceberg on R2)

Analytical storage managed by DoLake:

| Property | Value |
|----------|-------|
| Latency | ~100-500ms |
| Capacity | Unlimited |
| Format | Parquet (columnar) |
| Metadata | Iceberg |
| Use cases | Analytics, data lake, external query engines |

```typescript
// DoLake handles cold tier automatically via CDC
// External engines can query via Iceberg REST Catalog
// Time travel queries access historical snapshots
const historical = await lake.query(
  'SELECT * FROM orders',
  { asOf: new Date('2024-01-01') }
);
```

### Storage Flow

```
Write Request
      │
      ▼
┌─────────────────┐
│   Hot (DO)      │  Immediate writes
│   • WAL segment │
│   • B-tree page │
└────────┬────────┘
         │
         │ CDC Stream (async)
         ▼
┌─────────────────┐
│  Cold (R2)      │  Batched Parquet writes
│   • Parquet     │
│   • Iceberg     │
└─────────────────┘
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

## Further Reading

- [Getting Started](./getting-started.md)
- [API Reference](./api-reference.md)
- [Advanced Features](./advanced.md)
