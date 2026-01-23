# SQLite Compatibility Research for DoSQL

This document presents research findings on SQLite test suites, Turso/libSQL compatibility approaches, and recommendations for DoSQL compatibility testing.

## Table of Contents

1. [SQLite Official Test Suite](#1-sqlite-official-test-suite)
2. [Turso/libSQL Compatibility Approach](#2-tursolibsql-compatibility-approach)
3. [Turso Extensions](#3-turso-extensions)
4. [Recommendations for DoSQL](#4-recommendations-for-dosql)
5. [Sources](#5-sources)

---

## 1. SQLite Official Test Suite

### 1.1 Overview

SQLite is one of the most thoroughly tested software projects in existence. The testing infrastructure includes multiple complementary test suites:

| Test Suite | Description | Availability |
|------------|-------------|--------------|
| **TCL Test Suite** | Primary test harness written in Tcl | Open source (sqlite.org/src) |
| **TH3** | Proprietary test harness achieving 100% branch coverage | Proprietary (licensed) |
| **SQL Logic Test (SLT)** | Cross-database SQL correctness tests | Open source (sqlite.org/sqllogictest) |
| **Fuzz Testing** | AFL, OSS-Fuzz, dbsqlfuzz | Internal |

### 1.2 TCL Test Suite Details

The TCL test suite is the publicly available, primary test infrastructure:

- **Size**: 1,390 test files totaling 23.2 MB
- **Test Cases**: 51,445 distinct test cases
- **Infrastructure**: 27.2 KSLOC of C code for the TCL interface
- **Parameterization**: Heavy parameterization expands to millions of actual test runs

#### Test Modes

| Mode | Test Cases | Duration | Purpose |
|------|------------|----------|---------|
| **veryquick** | ~304,700 | Minutes | Pre-checkin verification |
| **full** | Millions (expanded) | Hours | Complete validation |
| **coverage** | Full + analysis | Extended | gcov coverage measurement |

#### Test Categories

1. **Anomaly Testing**
   - Out-of-memory (OOM) simulations
   - I/O error simulations via modified VFS
   - Crash and power-loss scenarios
   - Compound failures combining multiple error types

2. **Fuzz Testing**
   - SQL fuzzing with AFL and Google OSS-Fuzz
   - Database file corruption testing
   - Boundary value analysis
   - JSONB corruption testing

3. **Regression Testing**
   - Tests added for every reported bug
   - Thousands of regression test cases accumulated over years

4. **Journal/WAL Tests**
   - Alternative OS backend monitoring disk I/O
   - Verification of write-ahead log behavior

5. **Boundary Tests**
   - Maximum column counts
   - Maximum SQL statement lengths
   - Integer limits
   - Edge cases at defined limits

### 1.3 TH3 (Test Harness #3)

TH3 is SQLite's proprietary test suite (not publicly available):

- **Test Cases**: 50,362 distinct test cases
- **Expanded Coverage**: ~2.4 million full-coverage instances
- **Coverage**: 100% branch coverage, 100% MC/DC coverage
- **Soak Test**: Over 2.5 billion tests before each release
- **Design**: Pure C, runs on embedded/specialized platforms

### 1.4 SQL Logic Test (sqllogictest)

The most practical test suite for compatibility testing:

- **Test Cases**: 7.2 million queries (1.12 GB of test data)
- **Purpose**: Cross-database SQL correctness verification
- **Databases Tested**: SQLite, PostgreSQL, MySQL, SQL Server, Oracle 10g
- **Format**: Engine-agnostic text files (.slt format)
- **Location**: https://sqlite.org/sqllogictest

#### Test File Format

```
# Comment lines start with #
statement ok
CREATE TABLE t1(a, b, c)

statement ok
INSERT INTO t1 VALUES(1, 2, 3)

query III rowsort
SELECT * FROM t1
----
1
2
3
```

- **Type strings**: `T` (text), `I` (integer), `R` (float)
- **Sort modes**: `nosort`, `rowsort`, `valuesort`
- **Result validation**: Direct comparison or MD5 hash for large results

#### Compatibility Pass Rates (Industry Examples)

| Database | Initial | Current | Notes |
|----------|---------|---------|-------|
| **Dolt** | 23.9% | 100% | Achieved 100% in early 2024 |
| **Doltgres** | 86% | 91.1% | As of late 2024 |

### 1.5 How to Access Tests

```bash
# TCL tests (requires full source tree)
fossil clone https://sqlite.org/src sqlite.fossil
fossil open sqlite.fossil

# SQL Logic Test suite
fossil clone https://sqlite.org/sqllogictest sqllogictest.fossil
fossil open sqllogictest.fossil
```

---

## 2. Turso/libSQL Compatibility Approach

### 2.1 Compatibility Philosophy

Turso/libSQL maintains three core compatibility pillars:

1. **API Compatibility**: 100% SQLite C API compatibility with optional extensions
2. **File Format Compatibility**: Can always read/write standard SQLite files
3. **Embedded Nature**: Maintains in-process execution capability

### 2.2 Testing Methodology

#### Deterministic Simulation Testing (DST)

Turso's primary testing strategy differs from SQLite's traditional approach:

- **Technique**: Deterministic Simulation Testing pioneered by FoundationDB
- **Partnership**: Antithesis autonomous testing platform
- **Approach**: Systematic exploration of failure scenarios and edge cases
- **Bug Bounty**: $1,000 for data corruption bugs their testing missed

#### Why DST Over Traditional Testing

| Traditional Testing | Deterministic Simulation Testing |
|---------------------|----------------------------------|
| Tests what you expect might break | Discovers failure modes not considered |
| Regression-focused | Proactive discovery |
| Sequential execution | Parallel exploration of state space |

### 2.3 libSQL Fork Approach

libSQL extends SQLite while maintaining compatibility:

```
SQLite Core
    |
    v
libSQL (fork)
    |
    +-- 100% SQLite API compatibility
    +-- Additional APIs for extensions
    +-- File format compatibility when extensions unused
```

### 2.4 Limbo: The Rust Rewrite (2025)

Turso announced a complete SQLite rewrite in Rust:

- **Goal**: Full language and file format compatibility
- **Safety**: Memory safety via Rust
- **Testing**: DST built-in from the start
- **Status**: Replaces libSQL as intended direction (libSQL remains production-ready)

---

## 3. Turso Extensions

### 3.1 Vector Search (Native)

Built-in vector similarity search without extensions:

#### Supported Vector Types

| Type | Format | Storage | Use Case |
|------|--------|---------|----------|
| `F64_BLOB` | 64-bit float | 8D+18 bytes | Maximum precision |
| `F32_BLOB` | 32-bit float | 4D bytes | **Recommended default** |
| `F16_BLOB` | 16-bit float (IEEE 754) | 2D+12 bytes | Balanced |
| `FB16_BLOB` | bfloat16 | 2D+12 bytes | Speed/accuracy trade-off |
| `F8_BLOB` | 8-bit compressed | D+14 bytes | Space efficient |
| `F1BIT_BLOB` | 1-bit quantized | ceil(D/8)+3 bytes | Extreme compression |

#### SQL Syntax

```sql
-- Create table with vector column
CREATE TABLE movies (
  title TEXT,
  year INT,
  embedding F32_BLOB(768)
);

-- Insert vectors
INSERT INTO movies VALUES
  ('Inception', 2010, vector32('[0.1, 0.2, ...]'));

-- Create DiskANN index
CREATE INDEX movies_idx ON movies(libsql_vector_idx(embedding));

-- Query with cosine distance
SELECT title, vector_distance_cos(embedding, vector32('[0.1, ...]')) AS distance
FROM movies
ORDER BY distance ASC
LIMIT 10;

-- Indexed ANN query
SELECT title, year
FROM vector_top_k('movies_idx', vector32('[0.1, ...]'), 10)
JOIN movies ON movies.rowid = id;
```

#### Distance Functions

- `vector_distance_cos`: Cosine distance (1 - cosine similarity), range 0-2
- `vector_distance_l2`: Euclidean distance (not for F1BIT_BLOB)

#### Performance Characteristics

- Full table scan: Good recall up to ~10,000 vectors
- DiskANN index: Approximate nearest neighbors for larger datasets
- Trade-off: Speed vs accuracy for large tables

### 3.2 HTTP/WebSocket API (Hrana Protocol)

#### Protocol Overview

- **Name**: Hrana (Czech for "edge")
- **Transport**: WebSocket or HTTP
- **Design**: Fewer roundtrips than Postgres wire protocol
- **Cloudflare Compatible**: Works where TCP is banned

#### Endpoints

```
POST /v2/pipeline  - Execute SQL operations
GET  /health       - Health check
GET  /version      - Server version
```

#### Key Features

- Connection pooling built-in
- Multiple SQL streams per connection
- Zero cold start impact
- Postgres wire protocol support (via sqld)

### 3.3 Edge Replication

#### Architecture

```
                    +-------------------+
                    |   Primary (Write) |
                    +-------------------+
                           |
           +---------------+---------------+
           |               |               |
           v               v               v
      +--------+      +--------+      +--------+
      | Replica |     | Replica |     | Replica |
      | (Read)  |     | (Read)  |     | (Read)  |
      +--------+      +--------+      +--------+
```

#### Characteristics

- **Model**: Pull-based (not Raft-based)
- **Consistency**: Eventual (1-50ms sync lag between regions)
- **Writes**: All go to primary, async replication to replicas
- **Strong Consistency**: Query primary directly when needed

### 3.4 Embedded Replicas

Local database copies within applications:

```javascript
// JavaScript SDK example
const db = createClient({
  url: 'libsql://my-db.turso.io',
  authToken: '...',
  syncUrl: 'libsql://my-db.turso.io',
  syncInterval: 60 // seconds
});

// Reads: Local (microseconds)
// Writes: Remote (network latency)
await db.sync(); // Manual sync
```

#### Benefits

- Zero network latency for reads
- Offline read capability
- Read-your-writes semantics
- Reduced bandwidth costs

#### Ideal Use Cases

- VPS/VM deployments
- Containerized applications (Fly.io, Railway)
- Edge computing environments
- Mobile/on-device applications

### 3.5 Offline Writes (Beta)

- Write to local WAL when offline
- Push changes when connectivity restored
- Pull remote changes to sync
- **Status**: Beta, not recommended for production

### 3.6 WebAssembly User-Defined Functions

```sql
-- Register WASM function
CREATE FUNCTION encrypt_data LANGUAGE wasm AS <wasm-blob>;

-- Use in queries
SELECT encrypt_data(sensitive_column) FROM users;
```

#### Features

- Dynamic function registration via SQL
- Rust to WASM compilation supported
- WASM triggers for automated workflows
- Multiple runtimes: Wasmtime, WasmEdge

### 3.7 Encryption at Rest

- Per-database encryption with unique keys
- BYOK (Bring Your Own Key) support
- Open source implementation
- Multi-tenant isolation via per-tenant keys

### 3.8 ALTER TABLE Extensions

```sql
-- libSQL extension: modify columns without table recreation
ALTER TABLE users ALTER COLUMN email SET NOT NULL;
ALTER TABLE users ALTER COLUMN age TYPE INTEGER;
```

### 3.9 Random ROWID

```sql
-- Pseudorandom row identifiers
CREATE TABLE items (
  id INTEGER PRIMARY KEY RANDOM ROWID,
  name TEXT
);
```

### 3.10 Virtual WAL (Write-Ahead Log)

Pluggable WAL implementation enabling:

- Custom storage backends
- Replication to remote storage
- Compression/encryption of WAL pages
- Distributed locking mechanisms

### 3.11 Multi-Tenant Database Architecture

- Database-per-tenant model
- Shared schema across tenant databases
- Per-tenant encryption keys
- Automatic schema migrations across all tenants
- No row-level security complexity

---

## 4. Recommendations for DoSQL

### 4.1 Test Suite Strategy

#### Recommended Test Hierarchy

```
Level 1: SQL Logic Test (sqllogictest)
    |
    +-- 7.2M queries, cross-database validated
    +-- Engine-agnostic format
    +-- Industry standard (used by Dolt, CockroachDB)
    |
Level 2: Custom TCL-derived Tests
    |
    +-- Boundary tests for DoSQL-specific limits
    +-- Edge runtime constraints (memory, CPU)
    +-- Cloudflare Workers compatibility
    |
Level 3: Extension-Specific Tests
    |
    +-- Vector search operations
    +-- HTTP/WebSocket protocol
    +-- Replication scenarios
```

#### Phase 1: Adopt sqllogictest (Priority: High)

**Why sqllogictest first:**

1. Publicly available and well-documented
2. 7.2 million queries with known-correct results
3. Used by major database projects for compatibility measurement
4. Engine-agnostic format works with any database
5. Provides clear pass/fail percentage metric

**Implementation:**

```bash
# Clone the test suite
fossil clone https://sqlite.org/sqllogictest sqllogictest.fossil

# Or use dolthub's git mirror
git clone https://github.com/dolthub/sqllogictest

# Or use Rust implementation
cargo install sqllogictest-rs
```

**Target Milestones:**

| Milestone | Pass Rate | Timeline |
|-----------|-----------|----------|
| Alpha | 50% | Initial |
| Beta | 90% | 3 months |
| Production | 99% | 6 months |
| Full Compatibility | 100% | 12 months |

#### Phase 2: Core SQLite Feature Tests

Focus areas derived from TCL test categories:

1. **SQL Syntax Coverage**
   - SELECT, INSERT, UPDATE, DELETE
   - JOINs (INNER, LEFT, RIGHT, CROSS)
   - Subqueries and CTEs
   - Window functions
   - Aggregate functions

2. **Type System**
   - Dynamic typing behavior
   - Type affinity rules
   - NULL handling
   - BLOB operations

3. **Indexing**
   - B-tree indexes
   - Covering indexes
   - Partial indexes
   - Expression indexes

4. **Transactions**
   - ACID compliance
   - Isolation levels
   - Savepoints
   - Rollback behavior

5. **Constraints**
   - PRIMARY KEY
   - UNIQUE
   - NOT NULL
   - CHECK constraints
   - FOREIGN KEY

#### Phase 3: Edge Runtime Tests

DoSQL-specific tests for Cloudflare Workers environment:

```typescript
// Example: Memory pressure tests
test('handles memory limits gracefully', async () => {
  const db = new DoSQL();
  // Insert data until approaching Worker memory limit
  // Verify graceful degradation
});

// Example: CPU time limits
test('query timeout within Worker limits', async () => {
  const db = new DoSQL();
  // Complex query that might exceed CPU time
  // Verify appropriate timeout behavior
});
```

### 4.2 Turso Features to Prioritize

#### Tier 1: Essential (Implement First)

| Feature | Rationale | Complexity |
|---------|-----------|------------|
| **Vector Search** | AI/ML applications are primary use case | Medium |
| **HTTP API** | Native to Cloudflare Workers | Low |
| **Per-tenant Databases** | Multi-tenancy is core DO pattern | Medium |

#### Tier 2: Important (Implement Second)

| Feature | Rationale | Complexity |
|---------|-----------|------------|
| **Embedded Replicas** | Edge performance optimization | High |
| **Encryption at Rest** | Security requirement | Medium |
| **WASM UDFs** | Extensibility without native code | High |

#### Tier 3: Consider Later

| Feature | Rationale | Complexity |
|---------|-----------|------------|
| **Offline Writes** | Complex sync semantics | Very High |
| **Virtual WAL** | Advanced replication scenarios | Very High |
| **ALTER TABLE Extensions** | Nice-to-have for DX | Low |

### 4.3 Compatibility Testing Infrastructure

#### Recommended CI Pipeline

```yaml
# .github/workflows/compatibility.yml
name: SQLite Compatibility

on: [push, pull_request]

jobs:
  sqllogictest:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Run sqllogictest suite
        run: |
          # Run subset for PRs, full suite nightly
          ./scripts/run-sqllogictest.sh --mode quick

      - name: Report pass rate
        run: |
          # Post pass rate to PR comment
          ./scripts/report-compatibility.sh

  nightly-full:
    runs-on: ubuntu-latest
    if: github.event_name == 'schedule'
    steps:
      - name: Full sqllogictest suite
        run: |
          ./scripts/run-sqllogictest.sh --mode full

      - name: Update compatibility badge
        run: |
          # Update README badge with pass rate
```

#### Test Organization

```
packages/dosql/
├── tests/
│   ├── unit/                    # Unit tests
│   ├── integration/             # Integration tests
│   └── compatibility/
│       ├── sqllogictest/        # sqllogictest runner
│       │   ├── runner.ts
│       │   └── results/
│       ├── sqlite-tcl/          # Adapted TCL tests
│       │   ├── boundary.test.ts
│       │   └── constraints.test.ts
│       └── extensions/          # Extension-specific tests
│           ├── vector.test.ts
│           └── http-api.test.ts
```

### 4.4 Metrics and Reporting

#### Key Metrics to Track

1. **sqllogictest Pass Rate**: Primary compatibility metric
2. **Feature Coverage**: Percentage of SQLite features supported
3. **Performance Benchmarks**: Query latency vs SQLite
4. **Edge-Specific Metrics**: Memory usage, cold start time

#### Compatibility Matrix

```markdown
| SQLite Feature | Status | Notes |
|----------------|--------|-------|
| SELECT | Full | - |
| INSERT | Full | - |
| UPDATE | Full | - |
| DELETE | Full | - |
| JOIN | Full | - |
| Subqueries | Full | - |
| Window Functions | Partial | Missing GROUPS |
| FTS5 | Not Planned | Use vector search |
| R-Tree | Not Planned | - |
```

### 4.5 Comparison with Alternatives

#### SQLite-Compatible Databases Landscape

| Solution | Approach | Consistency | DoSQL Relevance |
|----------|----------|-------------|-----------------|
| **libSQL** | Fork + extensions | Strong | High - features to adopt |
| **rqlite** | Raft clustering | Strong | Medium - HA patterns |
| **dqlite** | Embedded distributed | Strong | Low - Linux-only |
| **LiteFS** | FUSE replication | Strong | Medium - replication patterns |
| **Litestream** | WAL streaming | Eventual | Medium - backup patterns |

---

## 5. Sources

### SQLite Official

- [How SQLite Is Tested](https://sqlite.org/testing.html)
- [TH3: Test Harness #3](https://sqlite.org/th3.html)
- [SQL Logic Test Documentation](https://sqlite.org/sqllogictest/doc/trunk/about.wiki)
- [testrunner.tcl Script](https://sqlite.org/src/doc/trunk/doc/testrunner.md)

### Turso/libSQL

- [libSQL GitHub Repository](https://github.com/tursodatabase/libsql)
- [Turso AI & Embeddings Documentation](https://docs.turso.tech/features/ai-and-embeddings)
- [Embedded Replicas Introduction](https://docs.turso.tech/features/embedded-replicas/introduction)
- [libSQL Extensions Documentation](https://github.com/tursodatabase/libsql/blob/main/libsql-sqlite3/doc/libsql_extensions.md)
- [libSQL Remote Protocol Reference](https://docs.turso.tech/sdk/http/reference)
- [Turso Vector Search](https://turso.tech/vector)
- [Fully Open Source Encryption for SQLite](https://turso.tech/blog/fully-open-source-encryption-for-sqlite-b3858225)
- [Local-First SQLite with Turso](https://turso.tech/local-first)
- [Database Per Tenant Architectures](https://turso.tech/blog/database-per-tenant-architectures-get-production-friendly-improvements)
- [WebAssembly Functions for SQLite](https://turso.tech/blog/webassembly-functions-for-your-sqlite-compatible-database-7e1ad95a2aa7)

### Turso Limbo/Rewrite

- [Introducing Limbo: A Complete Rewrite of SQLite in Rust](https://turso.tech/blog/introducing-limbo-a-complete-rewrite-of-sqlite-in-rust)

### Industry Examples

- [Getting to One 9 of SQL Correctness in Dolt](https://www.dolthub.com/blog/2019-12-17-one-nine-of-sql-correctness/)
- [Hitting 99% Correctness for Dolt](https://www.dolthub.com/blog/2021-06-28-sql-correctness-99/)
- [Doltgres Correctness Update](https://www.dolthub.com/blog/2024-11-12-doltgres-correctness-update/)
- [Dolt Correctness Documentation](https://docs.dolthub.com/sql-reference/benchmarks/correctness)

### Alternative Solutions Comparison

- [LiteFS vs Litestream vs rqlite vs dqlite](https://onidel.com/blog/sqlite-replication-vps-2025)
- [Comparing Litestream, rqlite, and dqlite - Gcore](https://gcore.com/learning/comparing-litestream-rqlite-dqlite)
- [Litestream Alternatives](https://litestream.io/alternatives/)

### Tools and Implementations

- [sqllogictest-rs (Rust Implementation)](https://github.com/risinglightdb/sqllogictest-rs)
- [dolthub/sqllogictest (Git Mirror)](https://github.com/dolthub/sqllogictest)
- [sqld GitHub Repository](https://github.com/libsql/sqld)

---

## Appendix A: sqllogictest File Format Reference

```
# Statement that should succeed
statement ok
CREATE TABLE t1(a INTEGER, b TEXT, c REAL)

# Statement that should fail
statement error
CREATE TABLE t1(a INTEGER)

# Query with expected results
query III rowsort label-1
SELECT a, b, c FROM t1
----
1
hello
3.14

# Query with hash verification (for large results)
query I nosort
SELECT x FROM large_table
----
1000 values hashing to abc123def456
```

## Appendix B: Vector Search Quick Reference

```sql
-- Vector types (from most to least precise)
F64_BLOB(dims)    -- 64-bit float
F32_BLOB(dims)    -- 32-bit float (recommended)
F16_BLOB(dims)    -- 16-bit float
FB16_BLOB(dims)   -- bfloat16
F8_BLOB(dims)     -- 8-bit compressed
F1BIT_BLOB(dims)  -- 1-bit quantized

-- Conversion functions
vector64('[1.0, 2.0, 3.0]')
vector32('[1.0, 2.0, 3.0]')
vector16('[1.0, 2.0, 3.0]')
vectorb16('[1.0, 2.0, 3.0]')
vector8('[1.0, 2.0, 3.0]')
vector1bit('[1.0, 2.0, 3.0]')

-- Distance functions
vector_distance_cos(v1, v2)  -- Cosine distance
vector_distance_l2(v1, v2)   -- Euclidean distance

-- Index creation
CREATE INDEX idx ON table(libsql_vector_idx(column));

-- ANN query
SELECT * FROM vector_top_k('idx', query_vector, k) JOIN ...;
```
