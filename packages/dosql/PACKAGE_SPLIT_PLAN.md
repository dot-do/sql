# DoSQL Package Split Plan

## Executive Summary

The dosql package has grown to **436 TypeScript source files** across **48 modules**. This document outlines a plan to split it into **12 focused sub-packages** to improve maintainability, enable better tree-shaking, and reduce bundle sizes for consumers who only need specific functionality.

## Current State Analysis

### File Distribution by Module

| Module | Files | Description |
|--------|-------|-------------|
| `parser/` | 20 | SQL parsing (DDL, DML, CTE, subqueries, etc.) |
| `functions/` | 12 | SQL functions (date, json, string, math, aggregate, window, vector) |
| `proc/` | 10 | ESM stored procedures |
| `fsx/` | 9 | File system abstraction (DO, R2, tiered, COW) |
| `planner/` | 9 | Query planning and optimization |
| `wal/` | 8 | Write-ahead log |
| `migrations/` | 8 | Schema migrations |
| `sharding/` | 8 | Native sharding (Vitess-inspired) |
| `sources/` | 8 | URL table sources |
| `triggers/` | 7 | Database triggers |
| `errors/` | 7 | Error handling |
| `distributed-tx/` | 7 | Distributed transactions (2PC) |
| `engine/` | 7 | Query execution engine |
| `fts/` | 7 | Full-text search |
| `columnar/` | 6 | Columnar storage format |
| `btree/` | 6 | B-tree index implementation |
| `replication/` | 6 | Primary/replica replication |
| `schema/` | 6 | Schema inference and validation |
| `statement/` | 6 | Prepared statements |
| `timetravel/` | 6 | Time-travel queries |
| `transaction/` | 6 | Transaction management |
| `vector/` | 6 | Vector search (HNSW) |
| `view/` | 6 | View management |
| `attach/` | 5 | ATTACH DATABASE |
| `branch/` | 5 | Database branching |
| `cdc/` | 5 | Change data capture |
| `collation/` | 5 | Collation support |
| `compaction/` | 5 | Data compaction |
| `constraints/` | 5 | Constraint validation |
| `index/` | 5 | Index management |
| `lakehouse/` | 5 | Lakehouse integration |
| `observability/` | 5 | Tracing and metrics |
| `pragma/` | 5 | PRAGMA commands |
| `r2-index/` | 5 | R2-backed indexes |
| `rpc/` | 5 | CapnWeb RPC |
| `virtual/` | 5 | Virtual tables |
| `worker/` | 5 | Worker integration |
| `iceberg/` | 4 | Iceberg format spike |
| `orm/` | 16 | ORM adapters (prisma, kysely, knex, drizzle) |
| `utils/` | 9 | Shared utilities |
| `cli/` | 2 | CLI tools |
| `types/` | 2 | Core types |
| `database/` | 2 | Database core (tokenizer, deadlock) |
| `executor/` | 2 | Query executor |
| `logging/` | 1 | Logging |

### Current Export Structure

The package.json already defines these exports:
- `.` (main)
- `./rpc`
- `./wal`
- `./cdc`
- `./transaction`
- `./orm/prisma`
- `./orm/kysely`
- `./orm/knex`
- `./orm/drizzle`
- `./fsx`
- `./proc`
- `./e2e`

---

## Proposed Package Structure

### Tier 1: Core Packages (No Internal Dependencies)

#### 1. `@dotdo/dosql-core` - Core Types & Utilities
**Files: ~25**

```
src/core/
  types/           # Core types (SqlValue, branded types)
  errors/          # Error classes and codes
  utils/           # Shared utilities (crypto, encoding, math)
  hlc.ts           # Hybrid logical clock
  constants.ts     # Constants
```

**Exports:**
```json
{
  ".": { "types": "./dist/index.d.ts", "import": "./dist/index.js" },
  "./errors": { "types": "./dist/errors/index.d.ts", "import": "./dist/errors/index.js" },
  "./utils": { "types": "./dist/utils/index.d.ts", "import": "./dist/utils/index.js" }
}
```

---

#### 2. `@dotdo/dosql-parser` - SQL Parser
**Files: ~20**

```
src/parser/
  shared/          # Tokenizer, parser state
  ddl.ts           # CREATE TABLE, INDEX, etc.
  dml.ts           # SELECT, INSERT, UPDATE, DELETE
  cte.ts           # Common Table Expressions
  subquery.ts      # Subquery parsing
  set-ops.ts       # UNION, INTERSECT, EXCEPT
  returning.ts     # RETURNING clause
  case.ts          # CASE expressions
  window.ts        # Window functions
  virtual.ts       # Virtual table parsing
```

**Dependencies:** `@dotdo/dosql-core`

**Exports:**
```json
{
  ".": { "types": "./dist/index.d.ts", "import": "./dist/index.js" },
  "./ddl": { "types": "./dist/ddl/index.d.ts", "import": "./dist/ddl/index.js" },
  "./dml": { "types": "./dist/dml/index.d.ts", "import": "./dist/dml/index.js" }
}
```

---

### Tier 2: Storage Packages

#### 3. `@dotdo/dosql-fsx` - File System Abstraction
**Files: ~15**

```
src/fsx/
  types.ts         # FSXBackend interface
  do-backend.ts    # Durable Object storage
  r2-backend.ts    # R2 storage
  r2-errors.ts     # R2 error handling
  r2-cache.ts      # R2 caching layer
  tiered.ts        # Tiered storage (hot/cold)
  cow-backend.ts   # Copy-on-Write backend
  cow-types.ts     # COW types
  snapshot.ts      # Snapshot management
  merge.ts         # Merge operations
  gc.ts            # Garbage collection
```

**Dependencies:** `@dotdo/dosql-core`

**Exports:**
```json
{
  ".": { "types": "./dist/index.d.ts", "import": "./dist/index.js" },
  "./do": { "types": "./dist/do-backend.d.ts", "import": "./dist/do-backend.js" },
  "./r2": { "types": "./dist/r2-backend.d.ts", "import": "./dist/r2-backend.js" },
  "./tiered": { "types": "./dist/tiered.d.ts", "import": "./dist/tiered.js" },
  "./cow": { "types": "./dist/cow-backend.d.ts", "import": "./dist/cow-backend.js" }
}
```

---

#### 4. `@dotdo/dosql-wal` - Write-Ahead Log
**Files: ~10**

```
src/wal/
  types.ts         # WAL types (WALEntry, LSN, etc.)
  writer.ts        # WAL writer
  reader.ts        # WAL reader
  checkpoint.ts    # Checkpoint manager
  retention.ts     # WAL retention policies
  retention-types.ts
```

**Dependencies:** `@dotdo/dosql-core`, `@dotdo/dosql-fsx`

**Exports:**
```json
{
  ".": { "types": "./dist/index.d.ts", "import": "./dist/index.js" },
  "./writer": { "types": "./dist/writer.d.ts", "import": "./dist/writer.js" },
  "./reader": { "types": "./dist/reader.d.ts", "import": "./dist/reader.js" },
  "./retention": { "types": "./dist/retention.d.ts", "import": "./dist/retention.js" }
}
```

---

#### 5. `@dotdo/dosql-btree` - B-Tree Index
**Files: ~6**

```
src/btree/
  types.ts         # BTree types
  btree.ts         # BTree implementation
  page.ts          # Page management
  lru-cache.ts     # LRU cache for pages
```

**Dependencies:** `@dotdo/dosql-core`, `@dotdo/dosql-fsx`

---

### Tier 3: Transaction & CDC Packages

#### 6. `@dotdo/dosql-transaction` - Transaction Management
**Files: ~10**

```
src/transaction/
  types.ts         # Transaction types
  manager.ts       # Transaction manager
  isolation.ts     # Isolation levels (MVCC, locks)
  timeout.ts       # Timeout enforcement
```

**Plus from database/:**
```
  deadlock-detector.ts
```

**Dependencies:** `@dotdo/dosql-core`, `@dotdo/dosql-wal`

---

#### 7. `@dotdo/dosql-cdc` - Change Data Capture
**Files: ~8**

```
src/cdc/
  types.ts         # CDC types
  stream.ts        # CDC streaming
  capture.ts       # WAL capture
```

**Dependencies:** `@dotdo/dosql-core`, `@dotdo/dosql-wal`, `@dotdo/dosql-fsx`

---

### Tier 4: Query Engine Packages

#### 8. `@dotdo/dosql-engine` - Query Execution Engine
**Files: ~30**

```
src/engine/
  types.ts         # Engine types (QueryPlan, Predicate, etc.)
  executor.ts      # Query executor
  planner.ts       # Query planner
  factory.ts       # Engine factory
  modes.ts         # Execution modes
  do-engine.ts     # DO-specific engine
  worker-engine.ts # Worker engine
  operators/       # Query operators (scan, filter, join, etc.)

src/planner/       # Query planning
  cost.ts          # Cost estimation
  optimizer.ts     # Query optimization
  explain.ts       # EXPLAIN support
  stats.ts         # Statistics
  cache.ts         # Plan cache

src/executor/      # DML executor
  dml-executor.ts

src/functions/     # SQL functions
  date.ts, json.ts, string.ts, math.ts
  aggregate.ts, window.ts, vector.ts
  registry.ts
```

**Dependencies:** `@dotdo/dosql-core`, `@dotdo/dosql-parser`

---

#### 9. `@dotdo/dosql-rpc` - CapnWeb RPC
**Files: ~6**

```
src/rpc/
  types.ts         # RPC types
  client.ts        # RPC client
  server.ts        # RPC server (DoSQLTarget)
```

**Dependencies:** `@dotdo/dosql-core`, `capnweb`

---

### Tier 5: Feature Packages

#### 10. `@dotdo/dosql-sharding` - Native Sharding
**Files: ~10**

```
src/sharding/
  types.ts         # VSchema, Vindex types
  vindex.ts        # Vindex implementations
  router.ts        # Query router
  executor.ts      # Distributed executor
  replica.ts       # Replica management
  sql-tokenizer.ts # SQL tokenizer for sharding
```

**Dependencies:** `@dotdo/dosql-core`, `@dotdo/dosql-engine`, `@dotdo/dosql-rpc`

---

#### 11. `@dotdo/dosql-proc` - ESM Stored Procedures
**Files: ~12**

```
src/proc/
  types.ts         # Procedure types
  parser.ts        # CREATE PROCEDURE parser
  registry.ts      # Procedure registry
  executor.ts      # Procedure executor
  context.ts       # Database context
  context-builder.ts
  functional.ts    # Functional API

src/triggers/      # Database triggers
  types.ts, parser.ts, registry.ts
  executor.ts, js-executor.ts
  definition.ts
```

**Dependencies:** `@dotdo/dosql-core`, `@dotdo/dosql-parser`, `ai-evaluate`

---

#### 12. `@dotdo/dosql-orm` - ORM Adapters
**Files: ~16**

```
src/orm/
  prisma/          # Prisma adapter
  kysely/          # Kysely dialect
  knex/            # Knex client
  drizzle/         # Drizzle adapter
```

**Dependencies:** `@dotdo/dosql-core` + respective ORM packages as peer deps

---

### Tier 6: Main Package (Facade)

#### `@dotdo/dosql` - Main Package (Re-exports)
**Files: ~15**

The main package becomes a facade that re-exports from sub-packages:

```typescript
// index.ts
export * from '@dotdo/dosql-core';
export * from '@dotdo/dosql-parser';
export * from '@dotdo/dosql-fsx';
export * from '@dotdo/dosql-wal';
export * from '@dotdo/dosql-cdc';
export * from '@dotdo/dosql-transaction';
export * from '@dotdo/dosql-engine';
export * from '@dotdo/dosql-rpc';
// ... selective exports to avoid conflicts
```

Plus these modules stay in main:
```
src/
  database.ts      # Main Database class
  schema/          # Schema inference
  view/            # View management
  virtual/         # Virtual tables
  attach/          # ATTACH DATABASE
  branch/          # Database branching
  timetravel/      # Time-travel queries
  migrations/      # Schema migrations
  observability/   # Tracing/metrics
  collation/       # Collation support
  constraints/     # Constraint validation
  pragma/          # PRAGMA commands
  cli/             # CLI tools
  worker/          # Worker integration
  index/           # Index management
  fts/             # Full-text search
  vector/          # Vector search
  columnar/        # Columnar storage
  lakehouse/       # Lakehouse integration
  r2-index/        # R2-backed indexes
  replication/     # Replication
  distributed-tx/  # Distributed transactions
  compaction/      # Data compaction
  iceberg/         # Iceberg spike
  benchmarks/      # Benchmarks
```

---

## Dependency Graph

```
                              @dotdo/dosql-core
                                     |
                    +----------------+----------------+
                    |                |                |
              @dotdo/dosql-parser  @dotdo/dosql-fsx  (capnweb)
                    |                |                |
                    |      +---------+--------+       |
                    |      |                  |       |
                    |  @dotdo/dosql-wal  @dotdo/dosql-btree
                    |      |
              +-----+------+------+
              |                   |
    @dotdo/dosql-transaction  @dotdo/dosql-cdc
              |
              +------+------+
                     |
           @dotdo/dosql-engine
                     |
         +-----------+-----------+
         |           |           |
  @dotdo/dosql-rpc   |    @dotdo/dosql-proc
         |           |           |
         +-----------+-----------+
                     |
           @dotdo/dosql-sharding
                     |
              @dotdo/dosql (main facade)
                     |
              @dotdo/dosql-orm
```

---

## New Package.json Exports Structure

```json
{
  "name": "@dotdo/dosql",
  "exports": {
    ".": {
      "types": "./dist/index.d.ts",
      "import": "./dist/index.js"
    },
    "./core": {
      "types": "./node_modules/@dotdo/dosql-core/dist/index.d.ts",
      "import": "@dotdo/dosql-core"
    },
    "./parser": {
      "types": "./node_modules/@dotdo/dosql-parser/dist/index.d.ts",
      "import": "@dotdo/dosql-parser"
    },
    "./fsx": {
      "types": "./node_modules/@dotdo/dosql-fsx/dist/index.d.ts",
      "import": "@dotdo/dosql-fsx"
    },
    "./fsx/do": {
      "types": "./node_modules/@dotdo/dosql-fsx/dist/do-backend.d.ts",
      "import": "@dotdo/dosql-fsx/do"
    },
    "./fsx/r2": {
      "types": "./node_modules/@dotdo/dosql-fsx/dist/r2-backend.d.ts",
      "import": "@dotdo/dosql-fsx/r2"
    },
    "./wal": {
      "types": "./node_modules/@dotdo/dosql-wal/dist/index.d.ts",
      "import": "@dotdo/dosql-wal"
    },
    "./cdc": {
      "types": "./node_modules/@dotdo/dosql-cdc/dist/index.d.ts",
      "import": "@dotdo/dosql-cdc"
    },
    "./transaction": {
      "types": "./node_modules/@dotdo/dosql-transaction/dist/index.d.ts",
      "import": "@dotdo/dosql-transaction"
    },
    "./engine": {
      "types": "./node_modules/@dotdo/dosql-engine/dist/index.d.ts",
      "import": "@dotdo/dosql-engine"
    },
    "./rpc": {
      "types": "./node_modules/@dotdo/dosql-rpc/dist/index.d.ts",
      "import": "@dotdo/dosql-rpc"
    },
    "./sharding": {
      "types": "./node_modules/@dotdo/dosql-sharding/dist/index.d.ts",
      "import": "@dotdo/dosql-sharding"
    },
    "./proc": {
      "types": "./node_modules/@dotdo/dosql-proc/dist/index.d.ts",
      "import": "@dotdo/dosql-proc"
    },
    "./orm/prisma": {
      "types": "./node_modules/@dotdo/dosql-orm/dist/prisma/index.d.ts",
      "import": "@dotdo/dosql-orm/prisma"
    },
    "./orm/kysely": {
      "types": "./node_modules/@dotdo/dosql-orm/dist/kysely/index.d.ts",
      "import": "@dotdo/dosql-orm/kysely"
    },
    "./orm/knex": {
      "types": "./node_modules/@dotdo/dosql-orm/dist/knex/index.d.ts",
      "import": "@dotdo/dosql-orm/knex"
    },
    "./orm/drizzle": {
      "types": "./node_modules/@dotdo/dosql-orm/dist/drizzle/index.d.ts",
      "import": "@dotdo/dosql-orm/drizzle"
    },
    "./schema": {
      "types": "./dist/schema/index.d.ts",
      "import": "./dist/schema/index.js"
    },
    "./view": {
      "types": "./dist/view/index.d.ts",
      "import": "./dist/view/index.js"
    },
    "./virtual": {
      "types": "./dist/virtual/index.d.ts",
      "import": "./dist/virtual/index.js"
    },
    "./attach": {
      "types": "./dist/attach/index.d.ts",
      "import": "./dist/attach/index.js"
    },
    "./branch": {
      "types": "./dist/branch/index.d.ts",
      "import": "./dist/branch/index.js"
    },
    "./timetravel": {
      "types": "./dist/timetravel/index.d.ts",
      "import": "./dist/timetravel/index.js"
    },
    "./migrations": {
      "types": "./dist/migrations/index.d.ts",
      "import": "./dist/migrations/index.js"
    },
    "./observability": {
      "types": "./dist/observability/index.d.ts",
      "import": "./dist/observability/index.js"
    },
    "./fts": {
      "types": "./dist/fts/index.d.ts",
      "import": "./dist/fts/index.js"
    },
    "./vector": {
      "types": "./dist/vector/index.d.ts",
      "import": "./dist/vector/index.js"
    },
    "./columnar": {
      "types": "./dist/columnar/index.d.ts",
      "import": "./dist/columnar/index.js"
    },
    "./lakehouse": {
      "types": "./dist/lakehouse/index.d.ts",
      "import": "./dist/lakehouse/index.js"
    },
    "./replication": {
      "types": "./dist/replication/index.d.ts",
      "import": "./dist/replication/index.js"
    },
    "./distributed-tx": {
      "types": "./dist/distributed-tx/index.d.ts",
      "import": "./dist/distributed-tx/index.js"
    }
  }
}
```

---

## Implementation Plan

### Phase 1: Create Core Package (Week 1)
1. Create `packages/dosql-core/` directory structure
2. Move `errors/`, `utils/`, `types/`, `hlc.ts`, `constants.ts`
3. Update all internal imports to use `@dotdo/dosql-core`
4. Set up workspace linking

### Phase 2: Extract Storage Packages (Week 2)
1. Create `packages/dosql-fsx/`
2. Create `packages/dosql-wal/`
3. Create `packages/dosql-btree/`
4. Update imports

### Phase 3: Extract Transaction & CDC (Week 3)
1. Create `packages/dosql-transaction/`
2. Create `packages/dosql-cdc/`
3. Handle circular dependency between WAL retention and CDC (needs interface extraction)

### Phase 4: Extract Engine & RPC (Week 4)
1. Create `packages/dosql-engine/`
2. Create `packages/dosql-rpc/`
3. Move `parser/`, `planner/`, `functions/`, `executor/`, `engine/` to engine package

### Phase 5: Extract Feature Packages (Week 5)
1. Create `packages/dosql-sharding/`
2. Create `packages/dosql-proc/` (includes triggers)
3. Create `packages/dosql-orm/`

### Phase 6: Update Main Package (Week 6)
1. Convert main package to facade
2. Re-export from sub-packages
3. Keep remaining modules in main
4. Update all tests
5. Update documentation

---

## Migration Strategy

### For Internal Code
- Use path aliases during migration: `"@dotdo/dosql-core": ["./packages/dosql-core/src"]`
- Gradually update imports file-by-file
- Run tests after each module extraction

### For External Consumers
- Main package continues to work unchanged (facade pattern)
- New sub-package imports are opt-in for better tree-shaking
- Deprecation warnings for direct deep imports

---

## Bundle Size Impact

| Use Case | Current | After Split |
|----------|---------|-------------|
| Full import | ~800KB | ~800KB (same) |
| RPC client only | ~800KB | ~50KB |
| WAL only | ~800KB | ~30KB |
| CDC streaming | ~800KB | ~60KB |
| Sharding only | ~800KB | ~100KB |
| ORM adapter | ~800KB | ~20KB |

---

## Open Questions

1. **Monorepo vs Multi-repo**: Should sub-packages be in the same monorepo or separate repos?
   - Recommendation: Same monorepo using workspaces

2. **Version Strategy**: Should all packages share versions or version independently?
   - Recommendation: Share major/minor, independent patch versions

3. **Circular Dependency Resolution**: WAL retention depends on CDC types
   - Solution: Extract shared types to core, use interfaces

4. **Testing Strategy**: How to handle integration tests that span packages?
   - Recommendation: Keep integration tests in main package

---

## Conclusion

Splitting dosql into 12 focused packages will:
- Improve maintainability (smaller, focused codebases)
- Enable better tree-shaking (consumers only bundle what they use)
- Speed up CI (parallel builds, incremental testing)
- Clarify API boundaries (each package has clear responsibilities)
- Enable independent versioning (bug fixes don't require full releases)

The facade pattern ensures backward compatibility - existing imports continue to work while enabling new, more efficient import patterns.
