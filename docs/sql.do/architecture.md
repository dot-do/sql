# DoSQL Architecture Overview

This guide explains how DoSQL works under the hood. Whether you're evaluating DoSQL for your project, debugging an issue, or contributing to the codebase, this document will help you understand the key design decisions and how the pieces fit together.

## What is DoSQL?

DoSQL is a SQL database engine built from scratch for Cloudflare Workers. Unlike traditional databases that rely on WebAssembly ports (like SQLite-WASM or PGLite), DoSQL is written entirely in TypeScript and designed specifically for Cloudflare's Durable Objects platform.

**Key insight**: By building natively for Durable Objects instead of porting an existing database, DoSQL achieves a 7KB bundle size versus 500KB-4MB for WASM alternatives.

## High-Level Architecture

At its core, DoSQL follows a classic database architecture with some important adaptations for the edge computing environment:

```
                    Your Application
                          |
                          v
           +------------------------------+
           |         DoSQL Client         |
           |    (@dotdo/sql.do SDK)       |
           +------------------------------+
                          |
                   WebSocket/HTTP
                   (CapnWeb RPC)
                          |
                          v
+----------------------------------------------------------+
|                   Cloudflare Workers                      |
|  +----------------------------------------------------+  |
|  |                    DoSQL Engine                     |  |
|  |                 (Durable Object)                    |  |
|  |                                                     |  |
|  |  +-------+    +--------+    +----------+           |  |
|  |  |Parser | -> |Planner | -> |Executor  |           |  |
|  |  +-------+    +--------+    +----------+           |  |
|  |                                  |                  |  |
|  |            +---------------------+                  |  |
|  |            |                     |                  |  |
|  |            v                     v                  |  |
|  |      +---------+           +---------+             |  |
|  |      | B-tree  |           |Columnar |             |  |
|  |      |  (OLTP) |           | (OLAP)  |             |  |
|  |      +---------+           +---------+             |  |
|  |            |                     |                  |  |
|  |            +---------------------+                  |  |
|  |                      |                              |  |
|  |                      v                              |  |
|  |  +----------------------------------------------+  |  |
|  |  |              Storage Layer (FSX)              |  |  |
|  |  |   +----------+    +----------+    +-------+   |  |  |
|  |  |   |    DO    |    |    R2    |    |Tiered |   |  |  |
|  |  |   |  (Hot)   | -> | (Cold)   | <- |Manager|   |  |  |
|  |  |   +----------+    +----------+    +-------+   |  |  |
|  |  +----------------------------------------------+  |  |
|  +----------------------------------------------------+  |
+----------------------------------------------------------+
```

## How a Query Executes

When you run a SQL query, it flows through several stages. Let's trace a simple query:

```typescript
const users = await db.query('SELECT * FROM users WHERE active = ?', [true]);
```

```mermaid
sequenceDiagram
    participant App as Your App
    participant Client as DoSQL Client
    participant RPC as CapnWeb RPC
    participant Parser as SQL Parser
    participant Planner as Query Planner
    participant Executor as Executor
    participant BTree as B-tree
    participant Storage as DO Storage

    App->>Client: query('SELECT * FROM users...')
    Client->>RPC: WebSocket message
    RPC->>Parser: Parse SQL string
    Parser->>Parser: Tokenize -> AST
    Parser->>Planner: ParsedSelect AST
    Planner->>Planner: Analyze tables, columns
    Planner->>Planner: Choose scan strategy
    Planner->>Executor: QueryPlan
    Executor->>BTree: Table scan with predicate
    BTree->>Storage: Read pages
    Storage-->>BTree: Page data
    BTree-->>Executor: Matching rows
    Executor-->>RPC: QueryResult
    RPC-->>Client: Response
    Client-->>App: Typed rows
```

### Stage 1: Parsing

The parser transforms your SQL string into an Abstract Syntax Tree (AST):

```
SQL: "SELECT * FROM users WHERE active = ?"

         SelectStatement
              |
    +---------+---------+
    |         |         |
  columns   table     where
    |         |         |
   [*]     "users"    active = ?
```

**Key files**: `src/parser/unified.ts`, `src/parser/shared/tokenizer.ts`

### Stage 2: Planning

The planner analyzes the AST and decides how to execute the query efficiently:

1. **Table resolution**: Verifies tables exist, resolves schemas
2. **Column validation**: Checks all referenced columns are valid
3. **Index selection**: Determines if indexes can speed up the query
4. **Path selection**: Chooses between B-tree (OLTP) or columnar (OLAP) scan

```mermaid
flowchart TD
    A[AST] --> B{Query Type?}
    B -->|Point lookup| C[B-tree Index Scan]
    B -->|Range scan| D[B-tree Range Scan]
    B -->|Full table scan| E{Data Size?}
    E -->|Small| F[B-tree Table Scan]
    E -->|Large + Analytics| G[Columnar Scan]
    C --> H[Physical Plan]
    D --> H
    F --> H
    G --> H
```

**Key files**: `src/planner/index.ts`, `src/planner/stats.ts`

### Stage 3: Execution

The executor transforms the plan into a tree of operators that produce rows:

```
        ProjectOperator (SELECT columns)
              |
        FilterOperator (WHERE clause)
              |
        ScanOperator (read from storage)
```

Each operator follows a **pull-based** model:
- The top operator calls `next()` on its child
- The child produces rows on demand
- Rows flow upward through the tree

This approach is memory-efficient because it processes one row at a time rather than materializing entire result sets.

**Key files**: `src/engine/executor.ts`, `src/engine/operators/`

## Core Components

### B-tree Index

The B-tree is DoSQL's primary data structure for OLTP workloads. It's a B+ tree implementation optimized for Durable Object storage:

```mermaid
flowchart TD
    subgraph "B+ Tree Structure"
        R[Root Node<br/>keys: 50, 100]
        R --> I1[Internal<br/>keys: 25]
        R --> I2[Internal<br/>keys: 75]
        R --> I3[Internal<br/>keys: 125, 150]
        I1 --> L1[Leaf: 1-25]
        I1 --> L2[Leaf: 26-50]
        I2 --> L3[Leaf: 51-75]
        I2 --> L4[Leaf: 76-100]
        I3 --> L5[Leaf: 101-125]
        I3 --> L6[Leaf: 126-150]
        I3 --> L7[Leaf: 151+]
        L1 <--> L2 <--> L3 <--> L4 <--> L5 <--> L6 <--> L7
    end
```

**Why a custom B-tree?**
- Durable Object storage has a 2MB value limit
- Pages are sized to fit within this constraint
- Linked leaf nodes enable efficient range scans
- Custom key/value codecs support any data type

**Key files**: `src/btree/`

### Columnar Storage

For analytical queries over large datasets, DoSQL uses columnar storage:

```
Row-oriented (B-tree):        Column-oriented:
+----+------+-------+         id:    [1, 2, 3, 4, 5...]
| id | name | sales |         name:  [Alice, Bob, Carol...]
+----+------+-------+         sales: [100, 200, 150, 300...]
| 1  | Alice| 100   |
| 2  | Bob  | 200   |
| 3  | Carol| 150   |
```

Columnar storage is faster for analytics because:
- Queries only read needed columns
- Better compression (similar values together)
- Zone maps enable predicate pushdown (skip irrelevant data)

**Key files**: `src/columnar/`

### Storage Layer (FSX)

The FSX abstraction provides a unified interface over different storage backends:

```mermaid
flowchart TD
    subgraph "FSX Abstraction"
        A[FSXBackend Interface]
        A --> B[DOBackend<br/>Hot Tier]
        A --> C[R2Backend<br/>Cold Tier]
        A --> D[TieredBackend<br/>Auto-migration]
    end

    B --> E[Durable Object Storage<br/>~1ms latency<br/>2MB max value]
    C --> F[R2 Object Storage<br/>~50-100ms latency<br/>Unlimited capacity]
    D --> B
    D --> C
```

**Tiered Storage Flow**:

1. **Writes** go to hot tier (DO storage) for low latency
2. **Background migration** moves old/large data to cold tier (R2)
3. **Reads** check hot tier first, then cold tier

This gives you fast access to recent data while supporting unlimited storage capacity.

**Key files**: `src/fsx/`

### Write-Ahead Log (WAL)

The WAL ensures durability by recording all changes before they're applied:

```mermaid
sequenceDiagram
    participant App
    participant TxManager as Transaction Manager
    participant WAL
    participant BTree
    participant CDC

    App->>TxManager: BEGIN
    App->>TxManager: INSERT INTO users...
    TxManager->>WAL: Append entry (LSN=42)
    TxManager->>BTree: Write row
    App->>TxManager: COMMIT
    TxManager->>WAL: Append COMMIT marker
    TxManager->>WAL: Flush to storage
    WAL->>CDC: Emit change event
    TxManager-->>App: Success
```

**WAL Entry Format**:
```
+--------+--------+--------+--------+--------+--------+
| LSN    | TxnId  |  Op    | Table  | Key    | Value  |
| bigint | string | enum   | string | bytes  | bytes  |
+--------+--------+--------+--------+--------+--------+
```

The WAL enables:
- **Crash recovery**: Replay uncommitted transactions
- **Point-in-time recovery**: Restore to any LSN
- **CDC streaming**: Derive change events from WAL entries

**Key files**: `src/wal/`

### Transaction Manager

DoSQL provides full ACID transactions with savepoint support:

```mermaid
stateDiagram-v2
    [*] --> Active: BEGIN
    Active --> Active: Execute queries
    Active --> Savepoint: SAVEPOINT
    Savepoint --> Active: RELEASE
    Savepoint --> Active: ROLLBACK TO
    Active --> Committed: COMMIT
    Active --> RolledBack: ROLLBACK
    Committed --> [*]
    RolledBack --> [*]
```

**Isolation Levels**:
- **SERIALIZABLE** (default): Full isolation, no anomalies
- **SNAPSHOT**: Read from consistent snapshot, write conflicts detected

**Key files**: `src/transaction/`

## Change Data Capture (CDC)

CDC captures all database changes and streams them to consumers:

```mermaid
flowchart LR
    subgraph "DoSQL Instances"
        S1[Shard 1<br/>WAL]
        S2[Shard 2<br/>WAL]
        S3[Shard N<br/>WAL]
    end

    subgraph "CDC Pipeline"
        C1[Capture]
        C2[Filter]
        C3[Stream]
    end

    subgraph "Consumers"
        L[DoLake<br/>Lakehouse]
        A[Your App]
        E[External<br/>Systems]
    end

    S1 --> C1
    S2 --> C1
    S3 --> C1
    C1 --> C2 --> C3
    C3 --> L
    C3 --> A
    C3 --> E
```

**CDC Event Format**:
```typescript
interface ChangeEvent {
  type: 'insert' | 'update' | 'delete';
  table: string;
  lsn: bigint;          // Logical sequence number
  txnId: string;        // Transaction ID
  timestamp: Date;
  before?: Record<string, unknown>;  // Previous values (update/delete)
  after?: Record<string, unknown>;   // New values (insert/update)
}
```

**Key files**: `src/cdc/`

## Sharding Architecture

For large-scale deployments, DoSQL supports automatic sharding:

```mermaid
flowchart TD
    subgraph "Query Router"
        R[Router]
        V[Vindex<br/>Shard Key Lookup]
    end

    subgraph "Shards"
        S1[Shard 1<br/>tenant_id: 1-100]
        S2[Shard 2<br/>tenant_id: 101-200]
        S3[Shard N<br/>tenant_id: 201+]
    end

    Client --> R
    R --> V
    V --> S1
    V --> S2
    V --> S3
```

**Vindex Types**:

| Type | Use Case | How It Works |
|------|----------|--------------|
| HASH | Even distribution | Consistent hash of shard key |
| RANGE | Range queries | Key ranges mapped to shards |
| LOOKUP | Flexible routing | External lookup table |
| REGION | Geo-affinity | Geographic zone mapping |

**Query Routing**:

1. **Single-shard**: Query includes shard key, route to one shard
2. **Scatter-gather**: No shard key, query all shards and merge results

**Key files**: `src/sharding/`

## Storage Tiers

DoSQL implements a three-tier storage architecture:

```
+-------------------------------------------------------------------------+
|                              Storage Tiers                               |
+-------------------------------------------------------------------------+
|                                                                          |
|   HOT TIER (Durable Object Storage)                                     |
|   +------------------------------------------------------------------+  |
|   | Latency: ~1ms | Max Value: 2MB | Consistency: Strong             |  |
|   | Contents: WAL segments, B-tree pages, active indexes             |  |
|   +------------------------------------------------------------------+  |
|                                  |                                       |
|                                  | Migration (age/size threshold)        |
|                                  v                                       |
|   WARM TIER (R2 with Cache)                                             |
|   +------------------------------------------------------------------+  |
|   | Latency: ~50-100ms | Capacity: Unlimited | Format: Binary pages  |  |
|   | Contents: Overflow pages, archived indexes, large blobs          |  |
|   +------------------------------------------------------------------+  |
|                                  |                                       |
|                                  | CDC streaming                         |
|                                  v                                       |
|   COLD TIER (Parquet/Iceberg on R2)                                     |
|   +------------------------------------------------------------------+  |
|   | Latency: ~100-500ms | Capacity: Unlimited | Format: Columnar     |  |
|   | Contents: Analytics data, historical snapshots, CDC archive      |  |
|   +------------------------------------------------------------------+  |
|                                                                          |
+-------------------------------------------------------------------------+
```

**Performance Summary**:

| Tier | Read Latency (p50) | Write Latency | Capacity |
|------|-------------------|---------------|----------|
| Hot (DO) | 0.5-1ms | 1-2ms | ~100MB |
| Warm (R2+Cache) | 1-50ms | 20-40ms | Unlimited |
| Cold (Iceberg) | 100-300ms | 200-500ms | Unlimited |

## Why This Architecture Matters

### For Application Developers

1. **Predictable Performance**: The tiered storage ensures hot data stays fast
2. **Unlimited Scale**: Cold tier provides infinite storage for historical data
3. **Real-time CDC**: Build reactive applications with change streaming
4. **Type Safety**: TypeScript-native means better tooling and fewer bugs

### For Operations Teams

1. **No Infrastructure**: Runs entirely on Cloudflare's managed platform
2. **Auto-scaling**: Durable Objects scale automatically
3. **Global Distribution**: Deploy close to users worldwide
4. **Cost Efficiency**: Pay only for what you use, hibernate idle connections

### For Data Teams

1. **Iceberg Integration**: Query historical data with Spark, Trino, DuckDB
2. **CDC Streaming**: Build real-time data pipelines
3. **Time Travel**: Query data at any point in history
4. **Schema Evolution**: Iceberg handles schema changes gracefully

## Bundle Size Comparison

DoSQL's native TypeScript approach results in dramatically smaller bundles:

| Database | Bundle Size (gzipped) |
|----------|----------------------|
| DoSQL | **7 KB** |
| SQLite-WASM | ~500 KB |
| PGLite | ~3 MB |
| DuckDB-WASM | ~4 MB |

This matters because:
- Cloudflare Workers have a 1MB bundle limit (free tier)
- Smaller bundles mean faster cold starts
- Less code to load and parse

## Component Summary

| Component | Location | Responsibility |
|-----------|----------|----------------|
| SQL Parser | `src/parser/` | SQL string to AST |
| Query Planner | `src/planner/` | AST to physical plan |
| Executor | `src/engine/` | Execute plan, produce rows |
| B-tree | `src/btree/` | OLTP storage, indexes |
| Columnar | `src/columnar/` | OLAP storage, analytics |
| FSX | `src/fsx/` | Storage abstraction layer |
| WAL | `src/wal/` | Durability, recovery |
| Transactions | `src/transaction/` | ACID guarantees |
| CDC | `src/cdc/` | Change data capture |
| Sharding | `src/sharding/` | Distributed queries |
| RPC | `src/rpc/` | Client-server communication |

## Further Reading

- [Getting Started](./getting-started.md) - Installation and basic usage
- [API Reference](./api-reference.md) - Complete API documentation
- [Advanced Features](./advanced.md) - Time travel, branching, CDC, vector search
