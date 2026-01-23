# DoSQL Package Splitting Plan

## Executive Summary

The `dosql` package has grown into a large monolith with 40+ source directories containing parser, engine, ORM integrations, sharding, and many other subsystems. This document proposes splitting it into focused packages to improve maintainability, enable independent versioning, and allow consumers to install only what they need.

## Current Package Analysis

### Size and Complexity

- **40+ top-level source directories** in `/packages/dosql/src/`
- **300+ TypeScript files** (source and tests)
- **External dependencies**: capnweb, drizzle-orm, kysely, knex, iceberg-js, @libsql/client
- **Exports 10+ sub-paths** via package.json exports

### Current Directory Structure

```
packages/dosql/src/
├── parser/          # SQL parsing (DDL, DML, CTE, Window, etc.)
├── engine/          # Query execution engine
├── planner/         # Query planning and optimization
├── btree/           # B-tree index implementation
├── columnar/        # Columnar storage
├── sharding/        # Vitess-inspired sharding
├── orm/             # ORM integrations
│   ├── drizzle/
│   ├── kysely/
│   ├── knex/
│   └── prisma/
├── rpc/             # CapnWeb RPC layer
├── transaction/     # Transaction management
├── wal/             # Write-ahead log
├── cdc/             # Change data capture
├── proc/            # Stored procedures
├── migrations/      # Schema migrations
├── functions/       # SQL functions
├── schema/          # Schema management
├── fts/             # Full-text search
├── vector/          # Vector search/HNSW
├── fsx/             # Filesystem abstraction
├── r2-index/        # R2 index for cold storage
├── lakehouse/       # Lakehouse integration
├── iceberg/         # Iceberg table format
├── branch/          # Database branching
├── timetravel/      # Time travel queries
├── replication/     # Replication support
├── compaction/      # Compaction scheduler
├── triggers/        # Trigger support
├── constraints/     # Constraint validation
├── collation/       # Collation support
├── view/            # View support
├── virtual/         # Virtual tables
├── pragma/          # PRAGMA commands
├── index/           # Secondary indexes
├── attach/          # ATTACH database
├── statement/       # Prepared statements
├── sources/         # External data sources
├── errors/          # Error types
├── types/           # Type utilities
├── benchmarks/      # Benchmark suite
└── worker/          # Worker runtime
```

## Proposed Package Split

### Tier 1: Core Foundation (High Priority)

#### 1. `dosql-core`
**Purpose**: Core types, parser, and engine with no ORM dependencies

**Includes**:
- `engine/types.ts` - Branded types (LSN, TransactionId, ShardId, etc.), SqlValue, Row, QueryResult
- `parser/` - Complete SQL parsing infrastructure
- `engine/` - Query execution engine (minus ORM specifics)
- `planner/` - Query planning and optimization
- `executor/` - DML executor
- `errors/` - Error types and codes
- `types/` - Type utilities (affinity, etc.)

**Dependencies**:
- None (standalone)

**Exports**:
```typescript
// Types
export type { SqlValue, Row, QueryResult, LSN, TransactionId, ShardId };
export { createLSN, createTransactionId, createShardId };

// Parser
export { parseSQL, parseDDL, parseDML };

// Engine
export { createQueryEngine, DOQueryEngine, WorkerQueryEngine };
export type { Engine, QueryPlan, Operator };
```

---

#### 2. `dosql-sharding`
**Purpose**: Vitess-inspired distributed query routing

**Includes**:
- `sharding/` - Complete sharding module
  - `types.ts` - VSchema, Vindex types
  - `vindex.ts` - Hash/Range vindexes
  - `router.ts` - Query router
  - `executor.ts` - Distributed executor
  - `replica.ts` - Replica selector

**Dependencies**:
- `dosql-core` (for engine types, ShardId)

**Exports**:
```typescript
// Types
export type { VSchema, VindexConfig, ShardConfig, RoutingDecision };

// Factory functions
export { createVSchema, hashVindex, rangeVindex, shardedTable };

// Router & Executor
export { createRouter, createExecutor, createShardingClient };
```

---

#### 3. `dosql-orm`
**Purpose**: ORM integrations (Drizzle, Kysely, Knex, Prisma)

**Includes**:
- `orm/drizzle/` - Drizzle adapter
- `orm/kysely/` - Kysely dialect
- `orm/knex/` - Knex client
- `orm/prisma/` - Prisma adapter

**Dependencies**:
- `dosql-core` (for SqlValue, Row, QueryResult)
- `drizzle-orm` (peerDep)
- `kysely` (peerDep)
- `knex` (peerDep)

**Exports**:
```typescript
// Drizzle
export { drizzle, DoSQLDrizzleDatabase } from './drizzle';

// Kysely
export { DoSQLDialect, createDoSQLKysely } from './kysely';

// Knex
export { createKnex, createInMemoryBackend } from './knex';

// Prisma
export { PrismaDoSQLAdapter, createPrismaAdapter } from './prisma';
```

**Note**: Could potentially split further into `dosql-drizzle`, `dosql-kysely`, etc. if consumers want even finer granularity.

---

### Tier 2: Extended Capabilities (Medium Priority)

#### 4. `dosql-storage`
**Purpose**: Storage layer abstractions

**Includes**:
- `btree/` - B-tree implementation
- `columnar/` - Columnar storage
- `fsx/` - Filesystem abstraction (R2, DO storage)
- `r2-index/` - R2 index reader/writer
- `compaction/` - Compaction scheduler

**Dependencies**:
- `dosql-core`

---

#### 5. `dosql-transaction`
**Purpose**: Transaction management and WAL

**Includes**:
- `transaction/` - Transaction manager, isolation levels
- `wal/` - Write-ahead log
- `branch/` - Database branching
- `timetravel/` - Time travel queries

**Dependencies**:
- `dosql-core`

---

#### 6. `dosql-rpc`
**Purpose**: CapnWeb RPC layer for DO communication

**Includes**:
- `rpc/` - Complete RPC module

**Dependencies**:
- `dosql-core`
- `capnweb`

---

#### 7. `dosql-cdc`
**Purpose**: Change data capture and streaming

**Includes**:
- `cdc/` - CDC capture and streaming

**Dependencies**:
- `dosql-core`
- `dosql-rpc` (optional)

---

### Tier 3: Specialized Features (Lower Priority)

#### 8. `dosql-search`
**Purpose**: Full-text and vector search

**Includes**:
- `fts/` - Full-text search
- `vector/` - Vector search, HNSW

**Dependencies**:
- `dosql-core`

---

#### 9. `dosql-proc`
**Purpose**: Stored procedures and migrations

**Includes**:
- `proc/` - Stored procedure support
- `migrations/` - Migration runner
- `triggers/` - Trigger support

**Dependencies**:
- `dosql-core`

---

#### 10. `dosql-lakehouse`
**Purpose**: Lakehouse and Iceberg integration

**Includes**:
- `lakehouse/` - Lakehouse aggregator
- `iceberg/` - Iceberg table format

**Dependencies**:
- `dosql-core`
- `dosql-storage`
- `iceberg-js`

---

## Dependency Graph

```
                    ┌─────────────────────┐
                    │   dosql-core │
                    └─────────┬───────────┘
                              │
          ┌───────────────────┼───────────────────┐
          │                   │                   │
          ▼                   ▼                   ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│ dosql-sharding  │ │   dosql-orm     │ │  dosql-storage  │
└─────────────────┘ └─────────────────┘ └────────┬────────┘
                                                 │
                              ┌──────────────────┼──────────────────┐
                              │                  │                  │
                              ▼                  ▼                  ▼
                    ┌─────────────────┐ ┌─────────────────┐ ┌──────────────┐
                    │ dosql-transaction│ │   dosql-rpc    │ │ dosql-search │
                    └─────────────────┘ └─────────────────┘ └──────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │    dosql-cdc    │
                    └─────────────────┘
```

## Implementation Strategy

### Phase 1: Core Extraction (Week 1)

1. **Create `dosql-core`**
   - Extract `engine/types.ts` (branded types, SqlValue, Row, etc.)
   - Move parser modules
   - Move basic engine infrastructure
   - Ensure no circular dependencies

2. **Update main `dosql` package**
   - Add `dosql-core` as dependency
   - Re-export core types for backward compatibility

### Phase 2: Sharding and ORM (Week 2)

3. **Create `dosql-sharding`**
   - Extract sharding module
   - Update imports to use `dosql-core`

4. **Create `dosql-orm`**
   - Extract ORM integrations
   - Make ORM libraries peer dependencies
   - Update imports

### Phase 3: Extended Modules (Week 3-4)

5. Extract remaining modules as needed:
   - Storage layer
   - Transaction/WAL
   - RPC
   - CDC
   - Search
   - Procedures
   - Lakehouse

### Phase 4: Deprecation (Week 5+)

6. **Deprecate monolithic package**
   - Mark main `dosql` as deprecated
   - Point users to new packages
   - Maintain backward compatibility layer

## Migration Guide for Consumers

### Before (Monolithic)

```typescript
import {
  createQueryEngine,
  createShardingClient,
  drizzle
} from 'dosql';
import { hashVindex } from 'dosql/sharding';
import { DoSQLDialect } from 'dosql/orm/kysely';
```

### After (Split Packages)

```typescript
// Core functionality
import { createQueryEngine } from 'dosql-core';

// Sharding
import { createShardingClient, hashVindex } from 'dosql-sharding';

// ORM integrations
import { drizzle } from 'dosql-orm/drizzle';
import { DoSQLDialect } from 'dosql-orm/kysely';
```

## Risk Assessment

### Low Risk
- ORM integrations (`dosql-orm`) - clearly isolated, minimal internal dependencies
- Sharding (`dosql-sharding`) - well-encapsulated module

### Medium Risk
- Core types extraction - many internal consumers, need careful interface design
- Storage layer - tight coupling with engine

### High Risk
- Transaction/WAL - complex interactions with multiple subsystems
- CDC - depends on transaction and RPC layers

## Recommendation

**Start with the low-risk, high-value split:**

1. **Immediate**: Create `dosql-orm` package
   - Clear boundaries (only imports `engine/types.ts`)
   - Reduces main package dependencies (drizzle-orm, kysely, knex become peer deps)
   - Users who don't need ORM don't need those deps

2. **Short-term**: Create `dosql-sharding` package
   - Self-contained module
   - Only imports `engine/types.ts` for ShardId

3. **Medium-term**: Create `dosql-core` with extracted types
   - Most complex due to many consumers
   - Should be done carefully with full test coverage

## Decision

Given the complexity and risk assessment, I recommend:

- **This task should NOT be implemented immediately** without more planning
- **Create this plan document** for future reference
- **Consider starting with `dosql-orm`** as a pilot split since it has the clearest boundaries

The full split is a multi-week effort that requires:
1. Comprehensive test coverage before refactoring
2. Careful API design for the split packages
3. Migration tooling or compatibility layer
4. Documentation updates
