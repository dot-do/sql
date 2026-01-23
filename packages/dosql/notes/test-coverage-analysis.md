# DoSQL Test Coverage Analysis

This document provides a comprehensive analysis of the existing test suite for DoSQL, identifying coverage gaps and priorities for achieving SQLite compatibility.

## Executive Summary

| Metric | Value |
|--------|-------|
| **Total Test Files** | 24 |
| **Total Test Cases** | ~1,650+ |
| **Feature Categories** | 14 |
| **Estimated Coverage** | ~60-70% of core SQLite features |
| **sqllogictest Target** | 7.2M queries |

---

## 1. Test Inventory by Category

### 1.1 Parser Tests

| Test File | Test Count | Description |
|-----------|------------|-------------|
| `parser/cte.test.ts` | ~80 | WITH clause, recursive CTEs, CTE chaining |
| `parser/ddl.test.ts` | ~100 | CREATE TABLE/INDEX, ALTER, DROP statements |
| `parser/dml.test.ts` | ~120 | INSERT/UPDATE/DELETE/REPLACE, UPSERT |
| `parser/set-ops.test.ts` | ~95 | UNION, INTERSECT, EXCEPT, compound operations |
| `parser/subquery.test.ts` | ~100 | Scalar, IN, EXISTS, correlated, derived tables |
| **Subtotal** | **~495** | |

**Coverage Assessment:**
- Strong: Basic DML, DDL, CTEs, set operations
- Good: Subquery parsing, correlated references
- Gap: RETURNING clause, complex expressions parsing

### 1.2 Type System Tests

| Test File | Test Count | Description |
|-----------|------------|-------------|
| `types.test.ts` | ~60 | Type-level SQL parser tests, schema inference |
| `aggregates.test.ts` | ~75 | Aggregate function type inference, GROUP BY |
| `join.test.ts` | ~80 | JOIN type inference, nullability for LEFT/RIGHT |
| **Subtotal** | **~215** | |

**Coverage Assessment:**
- Strong: Type-level parsing, compile-time inference
- Good: JOIN nullability, aggregate result types
- Gap: Runtime type coercion, SQLite affinity rules

### 1.3 Function Tests

| Test File | Test Count | Description |
|-----------|------------|-------------|
| `functions/functions.test.ts` | ~130 | String, math, date, aggregate, JSON functions |
| `functions/window.test.ts` | ~100 | Ranking, value access, aggregate window functions |
| **Subtotal** | **~230** | |

**Coverage Assessment:**
- Strong: Core string functions (length, substr, upper, lower, trim)
- Strong: Math functions (abs, round, pow, sqrt, trig)
- Strong: JSON functions (json_extract, json_array, json_object)
- Strong: Window functions (ROW_NUMBER, RANK, LAG, LEAD)
- Good: Date functions (date, time, datetime, strftime)
- Gap: printf edge cases, collation functions, typeof/coalesce edge cases

### 1.4 Index and Optimizer Tests

| Test File | Test Count | Description |
|-----------|------------|-------------|
| `index/index.test.ts` | ~100 | Index types, secondary index, composite, optimizer |
| **Subtotal** | **~100** | |

**Coverage Assessment:**
- Strong: B-tree index operations, point lookups, range scans
- Strong: Query optimizer (analyzePredicate, selectIndex, estimateJoinCost)
- Good: EXPLAIN plan generation (text, JSON, tree formats)
- Gap: Partial indexes, expression indexes, covering index optimization

### 1.5 Storage Tests

| Test File | Test Count | Description |
|-----------|------------|-------------|
| `btree/btree.test.ts` | ~70 | B-tree page serialization, codecs, operations |
| `fsx/fsx.test.ts` | ~60 | Memory, DO, R2, tiered storage backends |
| **Subtotal** | **~130** | |

**Coverage Assessment:**
- Strong: Page serialization/deserialization, binary search
- Strong: Key/value codecs (string, number, JSON, binary)
- Good: Multiple entries, sorted order, range queries
- Good: Tiered storage (hot/cold migration)
- Gap: Page splits under concurrent access, compaction

### 1.6 Transaction Tests

| Test File | Test Count | Description |
|-----------|------------|-------------|
| `transaction/transaction.test.ts` | ~95 | MVCC, lock manager, isolation, savepoints |
| **Subtotal** | **~95** | |

**Coverage Assessment:**
- Strong: BEGIN/COMMIT/ROLLBACK, transaction lifecycle
- Good: MVCC version visibility, snapshot isolation
- Good: Lock manager, conflict detection
- Good: Savepoints (SAVEPOINT, RELEASE, ROLLBACK TO)
- Gap: Serialization failure handling, deadlock resolution

### 1.7 Sharding Tests

| Test File | Test Count | Description |
|-----------|------------|-------------|
| `sharding/sharding.test.ts` | ~75 | Hash functions, Vindex, query routing |
| **Subtotal** | **~75** | |

**Coverage Assessment:**
- Strong: Hash-based sharding (xxHash, consistent hashing)
- Good: Vindex interface, scatter-gather
- Good: Query routing decisions
- Gap: Cross-shard transactions, resharding

### 1.8 Virtual Tables and Sources

| Test File | Test Count | Description |
|-----------|------------|-------------|
| `sources/sources.test.ts` | ~70 | Format detection, JSON/NDJSON/CSV parsers |
| `virtual/virtual.test.ts` | ~100 | Virtual tables, URL sources |
| **Subtotal** | **~170** | |

**Coverage Assessment:**
- Strong: URL table sources, format auto-detection
- Good: JSON, NDJSON, CSV parsing
- Good: Virtual table interface
- Gap: Parquet reading, streaming large files

### 1.9 PRAGMA Tests

| Test File | Test Count | Description |
|-----------|------------|-------------|
| `pragma/pragma.test.ts` | ~90 | SQLite PRAGMA support |
| **Subtotal** | **~90** | |

**Coverage Assessment:**
- Good: table_info, index_list, database_list
- Good: synchronous, journal_mode settings
- Gap: Many SQLite PRAGMAs not implemented

### 1.10 Stored Procedure Tests

| Test File | Test Count | Description |
|-----------|------------|-------------|
| `proc/proc.test.ts` | ~40 | ESM stored procedures |
| `proc/functional.test.ts` | ~50 | Functional procedure API |
| **Subtotal** | **~90** | |

**Coverage Assessment:**
- Good: JavaScript function registration
- Good: Functional API (map, filter, reduce patterns)
- Gap: Transaction support in procedures, error handling

### 1.11 RPC Tests

| Test File | Test Count | Description |
|-----------|------------|-------------|
| `rpc/rpc.test.ts` | ~25 | MockQueryExecutor, DoSQLTarget, batch queries |
| **Subtotal** | **~25** | |

**Coverage Assessment:**
- Basic: Query execution, batch operations
- Good: Transaction lifecycle (begin/commit/rollback)
- Gap: Streaming results, connection pooling

### 1.12 Iceberg Integration Tests

| Test File | Test Count | Description |
|-----------|------------|-------------|
| `iceberg/spike.test.ts` | ~35 | Schema conversion, REST API structure |
| **Subtotal** | **~35** | |

**Coverage Assessment:**
- Basic: DoSQL to Iceberg schema conversion
- Basic: REST API endpoint structure documentation
- Gap: Full Iceberg metadata read/write, snapshot management

---

## 2. Coverage Summary Table

| Feature Category | Test Files | Tests | Coverage | Critical Gaps |
|------------------|------------|-------|----------|---------------|
| **Parser** | 5 | ~495 | 80% | RETURNING, complex expressions |
| **Type System** | 3 | ~215 | 70% | Runtime coercion, affinity |
| **Functions** | 2 | ~230 | 75% | printf edge cases, collation |
| **Index/Optimizer** | 1 | ~100 | 60% | Partial/expression indexes |
| **Storage** | 2 | ~130 | 65% | Concurrent page splits |
| **Transactions** | 1 | ~95 | 70% | Deadlock resolution |
| **Sharding** | 1 | ~75 | 50% | Cross-shard transactions |
| **Virtual Tables** | 2 | ~170 | 60% | Parquet, streaming |
| **PRAGMA** | 1 | ~90 | 40% | Many PRAGMAs missing |
| **Stored Procedures** | 2 | ~90 | 55% | Tx support, error handling |
| **RPC** | 1 | ~25 | 30% | Streaming, pooling |
| **Iceberg** | 1 | ~35 | 20% | Full metadata support |
| **TOTAL** | **24** | **~1,650+** | **~60%** | |

---

## 3. SQLite Feature Comparison

### 3.1 Core SQL (DML)

| Feature | DoSQL Tests | SQLite | Gap |
|---------|-------------|--------|-----|
| SELECT * | Yes | Yes | None |
| SELECT columns | Yes | Yes | None |
| SELECT DISTINCT | Yes | Yes | None |
| WHERE clause | Yes | Yes | None |
| ORDER BY | Yes | Yes | NULLS FIRST/LAST edge cases |
| LIMIT/OFFSET | Yes | Yes | None |
| GROUP BY | Yes | Yes | None |
| HAVING | Yes | Yes | None |
| INSERT | Yes | Yes | None |
| INSERT OR REPLACE | Yes | Yes | None |
| INSERT OR IGNORE | Yes | Yes | None |
| UPDATE | Yes | Yes | None |
| DELETE | Yes | Yes | None |
| UPSERT (ON CONFLICT) | Yes | Yes | None |
| **RETURNING** | **No** | Yes | **High priority** |

### 3.2 JOINs

| Feature | DoSQL Tests | SQLite | Gap |
|---------|-------------|--------|-----|
| INNER JOIN | Yes | Yes | None |
| LEFT JOIN | Yes | Yes | None |
| RIGHT JOIN | Yes | Yes | None |
| CROSS JOIN | Yes | Yes | None |
| FULL OUTER JOIN | Yes | Yes | None |
| NATURAL JOIN | Partial | Yes | Need more tests |
| USING clause | Partial | Yes | Need more tests |
| Self-join | Yes | Yes | None |

### 3.3 Subqueries

| Feature | DoSQL Tests | SQLite | Gap |
|---------|-------------|--------|-----|
| Scalar subquery | Yes | Yes | None |
| IN subquery | Yes | Yes | None |
| NOT IN subquery | Yes | Yes | None |
| EXISTS subquery | Yes | Yes | None |
| NOT EXISTS | Yes | Yes | None |
| Correlated subquery | Yes | Yes | None |
| Derived tables (FROM) | Yes | Yes | None |
| ANY/ALL/SOME | Yes | Yes | None |
| LATERAL | Yes | Yes | None |

### 3.4 Set Operations

| Feature | DoSQL Tests | SQLite | Gap |
|---------|-------------|--------|-----|
| UNION | Yes | Yes | None |
| UNION ALL | Yes | Yes | None |
| INTERSECT | Yes | Yes | None |
| INTERSECT ALL | Yes | Yes | None |
| EXCEPT | Yes | Yes | None |
| EXCEPT ALL | Yes | Yes | None |
| MINUS (alias) | Yes | Yes | None |
| Compound ORDER BY | Yes | Yes | None |

### 3.5 DDL

| Feature | DoSQL Tests | SQLite | Gap |
|---------|-------------|--------|-----|
| CREATE TABLE | Yes | Yes | None |
| CREATE TABLE AS | Yes | Yes | None |
| CREATE INDEX | Yes | Yes | None |
| CREATE UNIQUE INDEX | Yes | Yes | None |
| DROP TABLE | Yes | Yes | None |
| DROP INDEX | Yes | Yes | None |
| ALTER TABLE ADD | Yes | Yes | None |
| ALTER TABLE DROP | Yes | Yes | None |
| ALTER TABLE RENAME | Yes | Yes | None |
| CREATE VIEW | Partial | Yes | Need tests |
| CREATE TRIGGER | **No** | Yes | **Medium priority** |
| CREATE VIRTUAL TABLE | Partial | Yes | FTS5/R-Tree not planned |

### 3.6 Window Functions

| Feature | DoSQL Tests | SQLite | Gap |
|---------|-------------|--------|-----|
| ROW_NUMBER | Yes | Yes | None |
| RANK | Yes | Yes | None |
| DENSE_RANK | Yes | Yes | None |
| NTILE | Yes | Yes | None |
| PERCENT_RANK | Yes | Yes | None |
| CUME_DIST | Yes | Yes | None |
| LAG | Yes | Yes | None |
| LEAD | Yes | Yes | None |
| FIRST_VALUE | Yes | Yes | None |
| LAST_VALUE | Yes | Yes | None |
| NTH_VALUE | Yes | Yes | None |
| ROWS frame | Yes | Yes | None |
| RANGE frame | Yes | Yes | None |
| GROUPS frame | Partial | Yes | Need more tests |
| Named windows | Partial | Yes | Need tests |

### 3.7 Common Table Expressions

| Feature | DoSQL Tests | SQLite | Gap |
|---------|-------------|--------|-----|
| Basic CTE | Yes | Yes | None |
| Multiple CTEs | Yes | Yes | None |
| Recursive CTE | Yes | Yes | None |
| CTE in subquery | Yes | Yes | None |
| CTE with column list | Partial | Yes | Need tests |
| Materialized hint | **No** | Yes | Low priority |

### 3.8 Built-in Functions

| Category | Tested | SQLite Total | Coverage |
|----------|--------|--------------|----------|
| String functions | 15+ | ~20 | 75% |
| Math functions | 20+ | ~25 | 80% |
| Date/time functions | 10+ | ~12 | 80% |
| Aggregate functions | 8+ | ~10 | 80% |
| JSON functions | 15+ | ~20 | 75% |
| Window functions | 11 | 11 | 100% |
| **Total** | **80+** | **~100** | **~80%** |

### 3.9 Transactions

| Feature | DoSQL Tests | SQLite | Gap |
|---------|-------------|--------|-----|
| BEGIN | Yes | Yes | None |
| COMMIT | Yes | Yes | None |
| ROLLBACK | Yes | Yes | None |
| SAVEPOINT | Yes | Yes | None |
| RELEASE | Yes | Yes | None |
| ROLLBACK TO | Yes | Yes | None |
| Deferred transactions | Partial | Yes | Need tests |
| Immediate transactions | Partial | Yes | Need tests |
| Exclusive transactions | Partial | Yes | Need tests |

### 3.10 Constraints

| Feature | DoSQL Tests | SQLite | Gap |
|---------|-------------|--------|-----|
| PRIMARY KEY | Yes | Yes | None |
| NOT NULL | Yes | Yes | None |
| UNIQUE | Yes | Yes | None |
| CHECK | Yes | Yes | None |
| FOREIGN KEY | Partial | Yes | Need enforcement tests |
| DEFAULT | Yes | Yes | None |
| COLLATE | Partial | Yes | Need tests |

---

## 4. sqllogictest Compatibility

### 4.1 Current Estimated Pass Rate

Based on the feature coverage analysis:

| Test Category | Estimated Pass Rate |
|---------------|---------------------|
| SELECT basics | 90%+ |
| JOIN operations | 85%+ |
| Subqueries | 80%+ |
| Set operations | 90%+ |
| Aggregates | 85%+ |
| Window functions | 85%+ |
| DDL operations | 75%+ |
| **Overall Estimate** | **~70-75%** |

### 4.2 Path to 100% Compatibility

| Milestone | Pass Rate | Missing Features |
|-----------|-----------|------------------|
| Current | ~70% | - |
| +RETURNING clause | +3% | 73% |
| +Missing functions | +5% | 78% |
| +Type coercion fixes | +5% | 83% |
| +TRIGGER support | +3% | 86% |
| +Edge case fixes | +7% | 93% |
| +Full conformance | +7% | 100% |

---

## 5. Prioritized Gap List

### 5.1 Critical Priority (Must Have)

| Gap | Impact | Effort |
|-----|--------|--------|
| **RETURNING clause** | Blocks common INSERT/UPDATE patterns | Medium |
| **Runtime type coercion** | SQLite affinity rules for correct results | High |
| **Deadlock detection** | Required for concurrent workloads | Medium |
| **printf format compliance** | Many queries use printf | Low |

### 5.2 High Priority (Should Have)

| Gap | Impact | Effort |
|-----|--------|--------|
| **TRIGGER support** | Common SQLite pattern | High |
| **Partial indexes** | Performance optimization | Medium |
| **Expression indexes** | Performance optimization | Medium |
| **Collation functions** | Sorting correctness | Medium |
| **Cross-shard transactions** | Distributed correctness | Very High |

### 5.3 Medium Priority (Nice to Have)

| Gap | Impact | Effort |
|-----|--------|--------|
| **VIEW creation/usage** | Schema organization | Medium |
| **More PRAGMA support** | Compatibility | Low |
| **Streaming query results** | Large result sets | Medium |
| **Connection pooling** | Performance | Medium |

### 5.4 Low Priority (Future)

| Gap | Impact | Effort |
|-----|--------|--------|
| **FTS5 full-text search** | Use vector search instead | N/A |
| **R-Tree spatial index** | Specialized use case | N/A |
| **ATTACH DATABASE** | Multi-database | High |

---

## 6. Recommended Test Additions

### 6.1 New Test Files Needed

| Test File | Purpose | Priority |
|-----------|---------|----------|
| `parser/returning.test.ts` | RETURNING clause parsing | Critical |
| `coercion/type-affinity.test.ts` | SQLite type affinity rules | Critical |
| `transaction/deadlock.test.ts` | Deadlock detection/resolution | Critical |
| `parser/trigger.test.ts` | CREATE/DROP TRIGGER | High |
| `index/partial.test.ts` | Partial and expression indexes | High |
| `view/view.test.ts` | CREATE/DROP VIEW | Medium |
| `compat/sqllogictest-runner.test.ts` | sqllogictest harness | High |

### 6.2 Existing Test File Expansions

| Test File | Additions Needed |
|-----------|------------------|
| `functions/functions.test.ts` | printf format strings, collation |
| `transaction/transaction.test.ts` | Immediate/exclusive transactions |
| `pragma/pragma.test.ts` | More SQLite PRAGMAs |
| `index/index.test.ts` | Partial indexes, covering queries |

---

## 7. sqllogictest Integration Plan

### 7.1 Infrastructure Setup

```typescript
// packages/dosql/tests/compat/sqllogictest-runner.ts

interface SQLLogicTest {
  type: 'statement' | 'query';
  sql: string;
  expected: 'ok' | 'error' | ResultSet;
  sortMode?: 'nosort' | 'rowsort' | 'valuesort';
}

class SQLLogicTestRunner {
  async runFile(path: string): Promise<TestResult> {
    // Parse .slt file
    // Execute against DoSQL
    // Compare results
  }

  async runSuite(dir: string): Promise<SuiteResult> {
    // Run all .slt files
    // Report pass/fail statistics
  }
}
```

### 7.2 CI Integration

```yaml
# .github/workflows/sqllogictest.yml
name: SQLite Compatibility

on: [push, pull_request]

jobs:
  quick-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run quick sqllogictest suite
        run: npm run test:sqllogictest -- --quick

  nightly-full:
    runs-on: ubuntu-latest
    if: github.event_name == 'schedule'
    steps:
      - name: Run full sqllogictest suite
        run: npm run test:sqllogictest -- --full
```

### 7.3 Target Milestones

| Milestone | Pass Rate | Timeline |
|-----------|-----------|----------|
| Alpha | 70% | Current |
| Beta | 85% | +2 months |
| RC | 95% | +4 months |
| 1.0 | 99% | +6 months |
| Full | 100% | +12 months |

---

## 8. Conclusion

DoSQL has a solid foundation with ~1,650+ tests covering core SQL functionality. The main gaps are:

1. **RETURNING clause** - Critical for modern INSERT/UPDATE patterns
2. **Runtime type coercion** - Required for SQLite compatibility
3. **Deadlock handling** - Needed for concurrent workloads
4. **TRIGGER support** - Common SQLite feature

With focused effort on these gaps and integration of sqllogictest, DoSQL can achieve 90%+ SQLite compatibility within 4-6 months.

---

## Appendix A: Test File Locations

```
packages/dosql/src/
├── aggregates.test.ts          # Aggregate type inference
├── join.test.ts                # JOIN type inference
├── types.test.ts               # Type-level parser tests
├── btree/
│   └── btree.test.ts           # B-tree operations
├── fsx/
│   └── fsx.test.ts             # Storage backends
├── functions/
│   ├── functions.test.ts       # Built-in functions
│   └── window.test.ts          # Window functions
├── iceberg/
│   └── spike.test.ts           # Iceberg integration
├── index/
│   └── index.test.ts           # Index and optimizer
├── parser/
│   ├── cte.test.ts             # CTEs
│   ├── ddl.test.ts             # DDL statements
│   ├── dml.test.ts             # DML statements
│   ├── set-ops.test.ts         # Set operations
│   └── subquery.test.ts        # Subqueries
├── pragma/
│   └── pragma.test.ts          # PRAGMA support
├── proc/
│   ├── proc.test.ts            # Stored procedures
│   └── functional.test.ts      # Functional API
├── rpc/
│   └── rpc.test.ts             # RPC layer
├── sharding/
│   └── sharding.test.ts        # Sharding
├── sources/
│   └── sources.test.ts         # Data sources
├── transaction/
│   └── transaction.test.ts     # Transactions
└── virtual/
    └── virtual.test.ts         # Virtual tables
```

## Appendix B: SQLite Test Suite Resources

| Resource | URL |
|----------|-----|
| sqllogictest | https://sqlite.org/sqllogictest |
| sqllogictest-rs (Rust) | https://github.com/risinglightdb/sqllogictest-rs |
| dolthub mirror | https://github.com/dolthub/sqllogictest |
| TCL test suite | https://sqlite.org/src/doc/trunk/doc/testrunner.md |
| SQLite testing docs | https://sqlite.org/testing.html |
