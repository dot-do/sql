# DoSQL Architecture

This document covers the internal architecture of DoSQL, including storage tiers, bundle size optimization, and the FSX (File System Abstraction) layer.

## Table of Contents

- [Overview](#overview)
- [Storage Tiers](#storage-tiers)
- [FSX (File System Abstraction)](#fsx-file-system-abstraction)
- [B-tree Storage](#b-tree-storage)
- [Write-Ahead Log (WAL)](#write-ahead-log-wal)
- [Copy-on-Write (COW) for Branching](#copy-on-write-cow-for-branching)
- [Bundle Size Optimization](#bundle-size-optimization)
- [Transaction System](#transaction-system)
- [Query Execution](#query-execution)

---

## Overview

DoSQL is a database engine designed specifically for Cloudflare Workers and Durable Objects. Unlike traditional WASM-based SQL databases (SQLite-WASM, PGLite, DuckDB-WASM), DoSQL is built natively in TypeScript with custom data structures optimized for the Cloudflare platform.

### Design Principles

1. **Edge-Native**: Built for Cloudflare's constraints (1MB bundle, DO storage limits)
2. **Type-Safe**: Compile-time SQL validation with TypeScript
3. **Zero Dependencies**: No WASM, no Node.js APIs
4. **Git-Like**: Branching, time travel, and COW semantics
5. **Lakehouse Integration**: CDC streaming to R2/Iceberg

### Architecture Diagram

```
                                    ┌──────────────────────────────────────────┐
                                    │              DoSQL Client                 │
                                    │  (Type-Safe SQL, Prepared Statements)    │
                                    └────────────────────┬─────────────────────┘
                                                         │
                                                         ▼
                                    ┌──────────────────────────────────────────┐
                                    │            Query Executor                 │
                                    │  (Parser, Planner, Operators)            │
                                    └────────────────────┬─────────────────────┘
                                                         │
                         ┌───────────────────────────────┼───────────────────────────────┐
                         │                               │                               │
                         ▼                               ▼                               ▼
          ┌──────────────────────────┐    ┌──────────────────────────┐    ┌──────────────────────────┐
          │     Transaction Mgr      │    │      WAL Writer          │    │     Schema Tracker       │
          │  (ACID, Savepoints)      │    │  (Durability, Recovery)  │    │  (Migrations)            │
          └────────────┬─────────────┘    └────────────┬─────────────┘    └──────────────────────────┘
                       │                               │
                       └───────────────┬───────────────┘
                                       │
                                       ▼
                       ┌──────────────────────────────────┐
                       │           B-tree Layer           │
                       │  (Pages, Keys, Range Scans)      │
                       └───────────────┬──────────────────┘
                                       │
                                       ▼
                       ┌──────────────────────────────────┐
                       │        FSX (Storage Abstraction) │
                       │  (COW, Snapshots, Tiering)       │
                       └───────────────┬──────────────────┘
                                       │
                    ┌──────────────────┴──────────────────┐
                    │                                     │
                    ▼                                     ▼
    ┌─────────────────────────────┐       ┌─────────────────────────────┐
    │   DO Storage (Hot Tier)     │       │    R2 (Cold Tier)           │
    │   - 2MB blob chunks         │       │    - Parquet files          │
    │   - Low latency             │       │    - Archive storage        │
    │   - Single-tenant           │       │    - Lakehouse integration  │
    └─────────────────────────────┘       └─────────────────────────────┘
```

---

## Storage Tiers

DoSQL implements a tiered storage architecture optimized for Cloudflare's infrastructure.

### Hot Tier (Durable Object Storage)

The hot tier uses Cloudflare's Durable Object storage for frequently accessed data.

**Characteristics:**
- Low latency (~1ms)
- 2MB maximum value size
- Strong consistency (single-leader)
- 128KB entries per DO (recommended)

**Usage:**
- Active working set
- Recent data (configurable age threshold)
- WAL segments
- Index pages

```typescript
// Hot tier is used automatically for recent data
const db = await DB('tenant', {
  storage: {
    hot: state.storage,  // DurableObjectStorage
  },
});
```

### Cold Tier (R2)

The cold tier uses Cloudflare R2 for archival and large data.

**Characteristics:**
- Higher latency (~50-100ms)
- Unlimited storage
- Columnar format (Parquet)
- S3-compatible API

**Usage:**
- Historical data
- Large blobs
- Lakehouse integration
- Backup storage

```typescript
// Add R2 for cold storage
const db = await DB('tenant', {
  storage: {
    hot: state.storage,
    cold: env.R2_BUCKET,
  },
});
```

### Tiering Configuration

```typescript
interface TieredStorageConfig {
  /** Max age before data migrates to cold (default: 1 hour) */
  hotDataMaxAge: number;

  /** Max hot storage size before migration (default: 100MB) */
  hotStorageMaxSize: number;

  /** Auto-migrate cold data to R2 (default: true) */
  autoMigrate: boolean;

  /** Read from hot first (default: true) */
  readHotFirst: boolean;

  /** Cache R2 reads in hot storage (default: false) */
  cacheR2Reads: boolean;

  /** Max file size for hot storage (default: 10MB) */
  maxHotFileSize: number;
}
```

### Data Migration

```typescript
import { TieredStorageBackend } from '@dotdo/dosql/fsx';

const tiered = new TieredStorageBackend(hotBackend, coldBackend, config);

// Manual migration
const result = await tiered.migrateToCode({
  olderThan: Date.now() - 86400000,  // 24 hours ago
  maxBytes: 50 * 1024 * 1024,        // 50MB max
});

console.log(`Migrated ${result.migrated.length} files`);
console.log(`Transferred ${result.bytesTransferred} bytes`);
```

---

## FSX (File System Abstraction)

FSX provides a unified interface for storage backends with advanced features like COW and snapshots.

### Core Interface

```typescript
interface FSXBackend {
  /** Read file, optionally with byte range */
  read(path: string, range?: ByteRange): Promise<Uint8Array | null>;

  /** Write file */
  write(path: string, data: Uint8Array): Promise<void>;

  /** Delete file */
  delete(path: string): Promise<void>;

  /** List files with prefix */
  list(prefix: string): Promise<string[]>;

  /** Check if file exists */
  exists(path: string): Promise<boolean>;
}
```

### Backend Implementations

#### DO Backend

```typescript
import { createDOBackend } from '@dotdo/dosql/fsx';

// In a Durable Object
export class MyDO {
  private fsx: DOStorageBackend;

  constructor(state: DurableObjectState) {
    this.fsx = createDOBackend(state.storage);
  }
}
```

**Features:**
- Automatic chunking for files >2MB
- Transactional batch writes
- Key prefix namespacing

#### R2 Backend

```typescript
import { createR2Backend } from '@dotdo/dosql/fsx';

const fsx = createR2Backend(env.MY_BUCKET, {
  prefix: 'data/',  // Optional key prefix
});
```

**Features:**
- Multipart uploads for large files
- Range request support
- Metadata storage

#### Tiered Backend

```typescript
import { createTieredBackend } from '@dotdo/dosql/fsx';

const fsx = createTieredBackend(
  createDOBackend(state.storage),  // Hot tier
  createR2Backend(env.R2_BUCKET),  // Cold tier
  config
);
```

#### Memory Backend (Testing)

```typescript
import { createMemoryBackend } from '@dotdo/dosql/fsx';

const fsx = createMemoryBackend();
```

---

## B-tree Storage

DoSQL uses a B+tree for row storage, optimized for DO storage constraints.

### B-tree Configuration

```typescript
interface BTreeConfig {
  /** Maximum keys per node (default: 100) */
  maxKeys: number;

  /** Minimum keys per node (default: 50) */
  minKeys: number;

  /** Page size in bytes (default: 64KB) */
  pageSize: number;

  /** Key prefix for pages (default: '_btree/') */
  pagePrefix: string;
}
```

### Page Structure

```typescript
interface Page {
  /** Unique page identifier */
  id: number;

  /** Page type: INTERNAL or LEAF */
  type: PageType;

  /** Keys stored in this page */
  keys: Uint8Array[];

  /** Values (leaf) or child page IDs (internal) */
  values: Uint8Array[];  // Leaf only
  children: number[];     // Internal only

  /** Leaf page chain for range scans */
  nextLeaf: number;
  prevLeaf: number;
}
```

### Key/Value Codecs

```typescript
interface KeyCodec<K> {
  encode(key: K): Uint8Array;
  decode(data: Uint8Array): K;
  compare(a: K, b: K): number;
}

interface ValueCodec<V> {
  encode(value: V): Uint8Array;
  decode(data: Uint8Array): V;
}
```

### B-tree Operations

```typescript
import { createBTree } from '@dotdo/dosql/btree';

// Create B-tree
const tree = createBTree(fsx, keyCodec, valueCodec, {
  pagePrefix: 'users/',
});

await tree.init();

// CRUD operations
await tree.set(key, value);
const value = await tree.get(key);
await tree.delete(key);

// Range scan
for await (const [k, v] of tree.range(startKey, endKey)) {
  console.log(k, v);
}

// Full iteration
for await (const [k, v] of tree.entries()) {
  console.log(k, v);
}
```

---

## Write-Ahead Log (WAL)

The WAL provides durability and enables time travel queries.

### WAL Structure

```
┌─────────────────────────────────────────────────────────────────┐
│                         WAL Segment                              │
├──────────────┬──────────────┬──────────────┬────────────────────┤
│   Header     │   Entry 1    │   Entry 2    │       ...          │
│  (magic,     │  (LSN, op,   │  (LSN, op,   │                    │
│   version)   │   data, CRC) │   data, CRC) │                    │
└──────────────┴──────────────┴──────────────┴────────────────────┘
```

### WAL Entry Format

```typescript
interface WALEntry {
  /** Log Sequence Number (monotonically increasing) */
  lsn: bigint;

  /** Entry timestamp */
  timestamp: number;

  /** Transaction ID */
  txnId: string;

  /** Operation type */
  op: 'INSERT' | 'UPDATE' | 'DELETE' | 'BEGIN' | 'COMMIT' | 'ROLLBACK';

  /** Target table */
  table: string;

  /** Row key */
  key?: Uint8Array;

  /** Data before operation */
  before?: Uint8Array;

  /** Data after operation */
  after?: Uint8Array;

  /** CRC32 checksum */
  checksum: number;
}
```

### WAL Configuration

```typescript
interface WALConfig {
  /** Maximum segment size (default: 10MB) */
  maxSegmentSize: number;

  /** Segment prefix (default: '_wal/') */
  segmentPrefix: string;

  /** Sync mode: 'async' | 'sync' | 'batch' */
  syncMode: string;

  /** Batch flush interval (default: 100ms) */
  batchIntervalMs: number;

  /** Enable CRC verification (default: true) */
  verifyChecksums: boolean;
}
```

### WAL Operations

```typescript
import { createWALWriter, createWALReader } from '@dotdo/dosql/wal';

// Writer
const writer = createWALWriter(fsx, config);
await writer.append({
  timestamp: Date.now(),
  txnId: 'txn_1',
  op: 'INSERT',
  table: 'users',
  after: new TextEncoder().encode(JSON.stringify({ id: 1, name: 'Alice' })),
});
await writer.flush();

// Reader
const reader = createWALReader(fsx, config);
for await (const entry of reader.iterate({ fromLSN: 0n })) {
  console.log(entry.lsn, entry.op, entry.table);
}
```

### Checkpointing

```typescript
import { createCheckpointManager } from '@dotdo/dosql/wal';

const checkpointer = createCheckpointManager(fsx, {
  intervalMs: 60000,       // Checkpoint every minute
  walEntriesThreshold: 1000, // Or every 1000 entries
});

// Manual checkpoint
await checkpointer.checkpoint();

// Auto-checkpointing
await checkpointer.start();
```

---

## Copy-on-Write (COW) for Branching

DoSQL implements COW semantics for efficient branching and snapshots.

### COW Architecture

```
                         ┌──────────────────┐
                         │     main@1       │
                         │  (base snapshot) │
                         └────────┬─────────┘
                                  │
              ┌───────────────────┼───────────────────┐
              │                   │                   │
              ▼                   ▼                   ▼
       ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
       │   main@2     │   │  feature@1   │   │   test@1     │
       │  (delta)     │   │  (delta)     │   │  (delta)     │
       └──────┬───────┘   └──────────────┘   └──────────────┘
              │
              ▼
       ┌──────────────┐
       │   main@3     │
       │  (delta)     │
       └──────────────┘
```

### Blob References

```typescript
type BlobRefType =
  | 'direct'     // Data stored inline
  | 'content'    // Reference by content hash
  | 'snapshot';  // Reference to snapshot + path

interface BlobRef {
  type: BlobRefType;
  hash?: string;       // Content hash for deduplication
  snapshotId?: string; // Reference snapshot
  path?: string;       // Path within snapshot
}
```

### Snapshot Management

```typescript
import { COWBackend, createCOWBackend } from '@dotdo/dosql/fsx';

const cow = createCOWBackend(baseFsx, {
  defaultBranch: 'main',
  snapshotPrefix: '_snapshots/',
  blobPrefix: '_blobs/',
});

// Create snapshot
const snapshot = await cow.createSnapshot('main', 'Initial state');

// Create branch
await cow.createBranch('feature-x', { from: 'main' });

// Read from branch
const data = await cow.read('feature-x', 'data.json');

// Write to branch (creates COW copy)
await cow.write('feature-x', 'data.json', newData);
```

### Garbage Collection

```typescript
import { GarbageCollector } from '@dotdo/dosql/fsx';

const gc = new GarbageCollector(cow, {
  retainSnapshots: 10,    // Keep last 10 snapshots per branch
  retainDays: 30,         // Keep snapshots from last 30 days
  dryRun: false,
});

const result = await gc.collect();
console.log(`Freed ${result.bytesFreed} bytes`);
console.log(`Deleted ${result.blobsDeleted} blobs`);
```

---

## Bundle Size Optimization

DoSQL is designed to fit within Cloudflare's 1MB bundle limit.

### Bundle Breakdown

| Component | Gzipped | Description |
|-----------|---------|-------------|
| B-tree | 2.8 KB | Core data structure |
| FSX | 1.6 KB | Storage abstraction |
| WAL | 1.3 KB | Write-ahead log |
| Worker | 1.7 KB | DO entry point |
| **Total Worker** | **7.4 KB** | Minimal bundle |
| Sharding | 6.8 KB | Multi-DO support |
| Procedures | 5.5 KB | Stored procedures |
| CDC | 1.4 KB | Change data capture |
| Virtual Tables | 4.0 KB | URL sources |
| **Full Library** | **34.3 KB** | All features |

### Tree-Shaking

DoSQL is designed for optimal tree-shaking:

```typescript
// Only imports what you need
import { DB } from '@dotdo/dosql';           // Core only
import { createCDC } from '@dotdo/dosql/cdc'; // CDC if needed
```

### Build Configuration

```typescript
// esbuild.config.js
export default {
  entryPoints: ['src/index.ts'],
  bundle: true,
  minify: true,
  format: 'esm',
  target: 'es2022',
  platform: 'browser',
  external: ['cloudflare:*'],
  define: {
    'process.env.NODE_ENV': '"production"',
  },
};
```

### Bundle Analysis

```bash
# Analyze bundle composition
node scripts/bundle-analysis.mjs

# Check gzipped size
gzip -c dist/worker.min.js | wc -c
```

### Comparison with Alternatives

| Database | Gzipped Size | Notes |
|----------|-------------|-------|
| **DoSQL** | **7.4 KB** | Native TypeScript |
| sql.js | ~500 KB | SQLite in WASM |
| PGLite | ~3 MB | PostgreSQL in WASM |
| DuckDB-WASM | ~4 MB | DuckDB in WASM |

---

## Transaction System

DoSQL implements SQLite-compatible transaction semantics.

### Transaction States

```
                    ┌──────────┐
           BEGIN    │   NONE   │
          ┌─────────│          │◄────────────┐
          │         └──────────┘              │
          │                                   │ COMMIT/ROLLBACK
          ▼                                   │
     ┌──────────┐                        ┌──────────┐
     │  ACTIVE  │────────SAVEPOINT──────►│SAVEPOINT │
     │          │◄───────RELEASE─────────│          │
     └──────────┘                        └──────────┘
```

### Isolation Levels

| Level | Description | Use Case |
|-------|-------------|----------|
| `READ_UNCOMMITTED` | Dirty reads allowed | Lowest isolation |
| `READ_COMMITTED` | Only committed data | Default for reads |
| `REPEATABLE_READ` | Stable reads | MVCC-based |
| `SNAPSHOT` | Point-in-time view | Time travel |
| `SERIALIZABLE` | Full isolation | SQLite default |

### Lock Types

| Lock | Description |
|------|-------------|
| `SHARED` | Read lock (multiple allowed) |
| `RESERVED` | Intent to write |
| `PENDING` | Waiting for exclusive |
| `EXCLUSIVE` | Write lock (single) |

### Transaction Implementation

```typescript
interface TransactionContext {
  txnId: string;
  state: TransactionState;
  mode: 'DEFERRED' | 'IMMEDIATE' | 'EXCLUSIVE';
  isolationLevel: IsolationLevel;
  savepoints: SavepointStack;
  log: TransactionLog;
  snapshot?: Snapshot;
  locks: HeldLock[];
  readOnly: boolean;
}
```

---

## Query Execution

DoSQL parses and executes SQL at runtime with type inference.

### Query Pipeline

```
┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐
│  SQL    │───►│ Parser  │───►│Planner  │───►│Executor │───►│ Result  │
│ String  │    │         │    │         │    │         │    │         │
└─────────┘    └─────────┘    └─────────┘    └─────────┘    └─────────┘
                   │              │              │
                   ▼              ▼              ▼
              ┌─────────┐   ┌─────────┐   ┌─────────┐
              │   AST   │   │  Plan   │   │Operators│
              │         │   │  Tree   │   │         │
              └─────────┘   └─────────┘   └─────────┘
```

### Type-Level SQL Parsing

DoSQL uses TypeScript's type system for compile-time SQL validation:

```typescript
// Type-level parser infers result type from SQL string
type Result = SQL<'SELECT id, name FROM users WHERE active = true', MySchema>;
// Result = { id: number; name: string }[]
```

### Query Operators

| Operator | Description |
|----------|-------------|
| `Scan` | Full table scan |
| `IndexScan` | B-tree index lookup |
| `Filter` | Row-level predicate |
| `Project` | Column selection |
| `Sort` | ORDER BY |
| `Limit` | LIMIT/OFFSET |
| `Join` | Table join |
| `Aggregate` | GROUP BY |
| `HashAggregate` | Optimized aggregation |

---

## Summary

DoSQL's architecture is purpose-built for the Cloudflare Workers platform:

1. **Tiered Storage**: Hot (DO) + Cold (R2) for optimal cost/performance
2. **FSX Abstraction**: Unified interface with COW, snapshots, and tiering
3. **B-tree Storage**: Custom implementation sized for DO constraints
4. **WAL Durability**: Segment-based with CRC checksums
5. **COW Branching**: Git-like semantics for databases
6. **Minimal Bundle**: ~7KB gzipped for core functionality
7. **Type Safety**: Compile-time SQL validation

For more details, see:
- [Getting Started](./getting-started.md)
- [API Reference](./api-reference.md)
- [Advanced Features](./advanced.md)
