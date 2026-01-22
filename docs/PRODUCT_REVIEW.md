# Product Review: @dotdo/sql Monorepo

**Review Date:** 2026-01-22
**Product:** DoSQL + DoLake
**Repository:** /Users/nathanclevenger/projects/sql
**Reviewer:** Claude Opus 4.5

---

## Executive Summary

The @dotdo/sql monorepo represents an ambitious vision to build a distributed SQL database (DoSQL) and analytics lakehouse (DoLake) native to Cloudflare Workers and Durable Objects. The project is in its **early initialization phase** - the `packages/` directory is empty, with only configuration files and documentation in place.

**Current Status:** Vision documented, zero implementation

**Strategic Assessment:** The product vision addresses a real gap in the Cloudflare ecosystem - there is no first-class, horizontally-scalable SQL database built natively for Workers and Durable Objects. The timing is opportune given recent Cloudflare improvements to DO SQLite and the growing edge computing market.

---

## Product Vision Analysis

### What DoSQL + DoLake Aims to Be

Based on README.md and CLAUDE.md:

| Component | Purpose | Storage | Query Model |
|-----------|---------|---------|-------------|
| **DoSQL** | Transactional OLTP database | DO SQLite | SQLite-compatible SQL |
| **DoLake** | Analytical OLAP lakehouse | R2 Parquet | CDC streaming + analytics |

### Architecture Vision

```
+-------------------------------------------------------------------+
|                     Client Application                             |
+-------------------------------------------------------------------+
                              |
+-------------------------------------------------------------------+
|                      DoSQL Gateway                                 |
|  +-------------+  +-------------+  +---------------------+         |
|  | SQL Parser  |  |  Planner    |  |  Query Optimizer    |         |
+-------------------------------------------------------------------+
                              |
        +---------------------+---------------------+
        |                     |                     |
+-------v-------+     +-------v-------+     +-------v-------+
|   Shard DO    |     |   Shard DO    |     |   Shard DO    |
|  (SQLite)     |     |  (SQLite)     |     |  (SQLite)     |
+-------+-------+     +-------+-------+     +-------+-------+
        |                     |                     |
        |             CDC Events                    |
        |                     |                     |
+-------v---------------------v---------------------v-------+
|                      DoLake                                |
|  +-------------+  +-------------+  +-----------+           |
|  | CDC Ingest  |  |  Compaction |  |  R2 Store |           |
+------------------------------------------------------------+
```

### Planned Feature Set

| Feature | DoSQL | DoLake | Differentiation |
|---------|-------|--------|-----------------|
| SQLite-compatible SQL | Yes | N/A | Parse/plan layer on top of DO SQLite |
| Horizontal sharding | Yes | N/A | Automatic query routing across shards |
| JOINs, aggregates, CTEs | Yes | N/A | Full SQL expressiveness |
| Window functions | Yes | N/A | Advanced analytics |
| CDC streaming | Producer | Consumer | Real-time data pipelines |
| Parquet storage | N/A | Yes | Cost-effective cold storage |
| Time travel | N/A | Yes | Query historical data |
| Strong consistency | Yes | Eventual | DO transactional guarantees |

---

## Competitive Landscape

### Direct Competitors

| Product | Platform | Model | Strengths | Weaknesses |
|---------|----------|-------|-----------|------------|
| **Cloudflare D1** | Workers | SQLite | Native, free tier, global | Single region write, limited scale |
| **Turso** | Edge | libSQL | Edge replicas, branching | External dependency, cost |
| **PlanetScale** | Cloud | Vitess/MySQL | Proven sharding, serverless | Not edge-native, MySQL only |
| **Neon** | Cloud | PostgreSQL | Branching, serverless | Not edge-native, cold starts |
| **Supabase** | Cloud | PostgreSQL | Real-time, auth, storage | Centralized, not edge-native |

### Indirect Competitors

| Product | Platform | Model | Overlap with DoSQL/DoLake |
|---------|----------|-------|---------------------------|
| **ClickHouse Cloud** | Cloud | Analytics | OLAP workloads, CDC |
| **Snowflake** | Cloud | Data warehouse | Lakehouse pattern |
| **Databricks** | Cloud | Lakehouse | Iceberg/Parquet analytics |
| **Firebolt** | Cloud | Analytics | Real-time analytics |

### Competitive Positioning

**DoSQL + DoLake unique value proposition:**

1. **Edge-native** - Runs in Cloudflare Workers, not external cloud
2. **Durable Object-native** - Leverages DO SQLite for strong consistency
3. **Horizontally scalable** - Sharding built from the ground up
4. **Unified OLTP + OLAP** - CDC pipeline connects transactional to analytical
5. **Cost-optimized** - Uses R2 ($0.015/GB) for cold storage, DOs only for hot data

### Market Gap Analysis

| Capability | D1 | Turso | PlanetScale | DoSQL |
|------------|----|----|----|----|
| Native to CF Workers | Yes | No | No | **Yes** |
| Horizontal sharding | No | Limited | Yes | **Yes** |
| Cross-shard JOINs | N/A | No | Limited | **Planned** |
| CDC to analytics | No | No | No | **Yes** |
| Time travel | No | No | No | **Yes (DoLake)** |
| Hibernating WebSocket | No | No | No | **Planned** |

---

## Related Work in the Ecosystem

The @dotdo organization has significant prior art that should inform DoSQL/DoLake:

### evodb (Schema-Evolving Database)

| Feature | Implementation Status | Applicable to DoSQL |
|---------|----------------------|---------------------|
| Columnar JSON shredding | Implemented | Yes - storage optimization |
| Schema evolution | Implemented | Yes - flexible schemas |
| CDC pipeline (CapnWeb RPC) | Implemented | Yes - reuse for DoLake |
| Lakehouse on R2 | Implemented | Yes - direct reuse |
| Edge constraints handling | Implemented | Yes - Snippets patterns |

**Recommendation:** DoLake should build on @evodb/lakehouse and @evodb/writer rather than starting from scratch.

### graphdb (Cost-Optimized Graph Database)

| Feature | Implementation Status | Applicable to DoSQL |
|---------|----------------------|---------------------|
| Hibernating WebSocket | Implemented | Yes - 95% cost reduction |
| Bloom filter routing | Implemented | Yes - query routing |
| Shard DO architecture | Implemented | Yes - sharding patterns |
| R2 GraphCol format | Implemented | Partial - storage format |
| Benchmark framework | Implemented | Yes - performance testing |

**Recommendation:** DoSQL should adopt the hibernating WebSocket pattern and benchmark infrastructure from graphdb.

### vitess-do (Distributed Sharding)

| Feature | Implementation Status | Applicable to DoSQL |
|---------|----------------------|---------------------|
| VTGate query routing | In development | Direct alignment |
| VTTablet shard management | In development | Direct alignment |
| PGlite/SQLite backends | In development | SQLite only for DoSQL |
| Cross-shard transactions | In development | Required for DoSQL |

**Recommendation:** Evaluate whether DoSQL should be a rebrand/fork of vitess-do or a separate implementation.

### pocs (Proof of Concepts)

Extensive POC work has been done in /Users/nathanclevenger/projects/pocs:

| POC Package | Relevance to DoSQL/DoLake |
|-------------|---------------------------|
| mergetree | LSM-tree storage for DO - applicable |
| replacing-mergetree | Upsert semantics - applicable |
| storage-tiering | Hot/cold tiering - core to DoLake |
| cdc | Change data capture - core to DoLake |
| query-engine | DO-native SQL engine - applicable |
| edgeql | Federated SQL - applicable patterns |
| coordinator | Multi-file Iceberg scans - applicable |
| parquet | Parquet reading - core to DoLake |

**Recommendation:** The dosql and dolake packages referenced in pocs/TYPESCRIPT_REVIEW_DOSQL_DOLAKE.md appear to already exist as POCs. These should be migrated to @dotdo/sql rather than rebuilding.

---

## Cloudflare Workers Constraints

### Current Platform Limits

| Constraint | Workers | Snippets | Durable Objects |
|------------|---------|----------|-----------------|
| **CPU Time** | 50ms (free), 30s (paid) | 5ms | 30s/request |
| **Memory** | 128MB | 32MB | 128MB |
| **Subrequests** | 1000 | 5 | Unlimited |
| **Request size** | 100MB | 100MB | 100MB |
| **SQLite size** | N/A | N/A | 10GB |
| **DO storage** | N/A | N/A | 50GB |

### Constraint Implications

| Feature | Constraint | Mitigation |
|---------|------------|------------|
| Large JOINs | 50ms CPU limit in Workers | Execute in DOs (30s budget) |
| Cross-shard queries | 1000 subrequest limit | Batch shard requests, hibernating WS |
| Cold analytics | Memory limits | Streaming + R2 Parquet |
| Schema metadata | DO storage limits | Store schemas in R2, cache in DO |
| Index builds | CPU time | Incremental builds via alarms |

### Edge Constraint Strategy

Based on graphdb and evodb patterns:

```
+--------------------+     +---------------------+     +-------------------+
|   Snippets (FREE)  | --> |    Gateway Worker   | --> |   Shard DOs       |
|   5ms, 32KB        |     |    50ms budget      |     |   30s budget      |
+--------------------+     +---------------------+     +-------------------+
         |                          |                          |
    Bloom filter              Query routing              SQLite execution
    Query parsing             Aggregation                Transactions
    Shard selection           Caching                    CDC streaming
```

---

## Production Readiness Assessment

### Current State: 0% Complete

| Category | Status | Score |
|----------|--------|-------|
| Core database engine | Not started | 0/10 |
| Query parsing | Not started | 0/10 |
| Query planning | Not started | 0/10 |
| Transaction management | Not started | 0/10 |
| Sharding | Not started | 0/10 |
| CDC pipeline | Not started | 0/10 |
| Lakehouse storage | Not started | 0/10 |
| Testing infrastructure | Not started | 0/10 |
| Documentation | Skeleton only | 2/10 |
| CI/CD | Not started | 0/10 |

### Path to MVP (Minimum Viable Product)

**MVP Definition:** Single-shard SQL database with CDC to R2 Parquet

| Milestone | Scope | Effort |
|-----------|-------|--------|
| M1: Single-node DoSQL | CREATE/INSERT/SELECT/UPDATE/DELETE on single DO | 2 weeks |
| M2: Query parser | Full SQL parser with AST | 2 weeks |
| M3: Query planner | Cost-based optimization | 2 weeks |
| M4: Transactions | BEGIN/COMMIT/ROLLBACK, isolation | 1 week |
| M5: CDC export | Stream changes to DoLake | 1 week |
| M6: DoLake ingest | Buffer CDC, write Parquet to R2 | 2 weeks |
| M7: Time travel | Query Parquet snapshots | 1 week |

**MVP Total:** ~11 weeks

### Path to Production

| Phase | Scope | Effort |
|-------|-------|--------|
| Alpha | MVP + basic sharding | +4 weeks |
| Beta | Cross-shard queries, WebSocket API | +6 weeks |
| RC | Performance optimization, monitoring | +4 weeks |
| GA | Production hardening, docs | +4 weeks |

**Production Total:** ~29 weeks (~7 months)

---

## Documentation Needs

### Current Documentation

| Document | Status | Quality |
|----------|--------|---------|
| README.md | Exists | Good - clear vision, architecture diagram |
| CLAUDE.md | Exists | Excellent - detailed development guide |
| AGENTS.md | Exists | Good - agent workflow reference |
| API docs | Missing | N/A |
| Architecture docs | Missing | N/A |
| User guides | Missing | N/A |
| Migration guides | Missing | N/A |

### Required Documentation

| Category | Documents | Priority |
|----------|-----------|----------|
| **Architecture** | System design, sharding strategy, CDC flow | P0 |
| **API Reference** | SQL syntax, client API, WebSocket protocol | P0 |
| **Operations** | Deployment, monitoring, backup/restore | P1 |
| **Migration** | From D1, from Turso, from PlanetScale | P1 |
| **Tutorials** | Quick start, multi-tenant app, analytics | P2 |
| **Internals** | Query optimizer, transaction manager, WAL | P2 |

---

## Proposed Roadmap

### Phase 0: Foundation (Current - 2 weeks)

**Goal:** Migrate existing POC code, establish infrastructure

| Task | Source | Target | Status |
|------|--------|--------|--------|
| Migrate dosql POC | pocs/packages/dosql | sql/packages/dosql | Not started |
| Migrate dolake POC | pocs/packages/dolake | sql/packages/dolake | Not started |
| Set up CI/CD | - | .github/workflows | Not started |
| Add TypeScript config | - | tsconfig.json | Not started |
| Add Vitest config | - | vitest.config.ts | Not started |
| Create test fixtures | - | packages/test-utils | Not started |

### Phase 1: Single-Shard DoSQL (Weeks 3-6)

**Goal:** Working SQL database on single Durable Object

| Milestone | Description | Deliverable |
|-----------|-------------|-------------|
| M1.1 | Database DO scaffold | DoSQL class with SQLite access |
| M1.2 | DDL support | CREATE TABLE, DROP TABLE, ALTER TABLE |
| M1.3 | DML support | INSERT, UPDATE, DELETE, SELECT |
| M1.4 | Query parsing | Full SQL parser with error messages |
| M1.5 | Transactions | BEGIN, COMMIT, ROLLBACK, isolation levels |
| M1.6 | Indexes | CREATE INDEX, query optimizer integration |

### Phase 2: DoLake Foundation (Weeks 7-10)

**Goal:** CDC pipeline from DoSQL to R2 Parquet

| Milestone | Description | Deliverable |
|-----------|-------------|-------------|
| M2.1 | CDC event schema | Event types, serialization |
| M2.2 | CDC capture | Triggers on DoSQL writes |
| M2.3 | CDC transport | WebSocket or RPC to DoLake |
| M2.4 | Buffer manager | In-memory buffer with flush logic |
| M2.5 | Parquet writer | Write columnar files to R2 |
| M2.6 | Manifest manager | Track Parquet files, snapshots |

### Phase 3: Horizontal Sharding (Weeks 11-16)

**Goal:** Multi-shard DoSQL with query routing

| Milestone | Description | Deliverable |
|-----------|-------------|-------------|
| M3.1 | Shard key definition | VSchema-style configuration |
| M3.2 | Shard router | Route queries to correct shards |
| M3.3 | Scatter-gather | Fan-out queries, aggregate results |
| M3.4 | Cross-shard JOINs | Distributed join execution |
| M3.5 | Shard balancing | Split/merge shards |
| M3.6 | Global tables | Replicate lookup tables |

### Phase 4: Analytics (Weeks 17-22)

**Goal:** Query historical data in DoLake

| Milestone | Description | Deliverable |
|-----------|-------------|-------------|
| M4.1 | Parquet reader | Read columnar data from R2 |
| M4.2 | Time travel | Query at specific timestamp |
| M4.3 | Compaction | Merge small files, remove deletes |
| M4.4 | Partition pruning | Skip irrelevant partitions |
| M4.5 | Aggregate pushdown | Execute aggregates on Parquet |
| M4.6 | Hot/cold routing | Unified queries across DO + R2 |

### Phase 5: Production Readiness (Weeks 23-29)

**Goal:** Production-grade deployment

| Milestone | Description | Deliverable |
|-----------|-------------|-------------|
| M5.1 | Monitoring | Metrics, logging, tracing |
| M5.2 | Security | Auth, encryption, access control |
| M5.3 | Performance | Benchmarks, optimization |
| M5.4 | Documentation | API docs, guides, examples |
| M5.5 | Client SDKs | TypeScript, Python, Go |
| M5.6 | Release | npm publish, versioning |

---

## Risk Assessment

### Technical Risks

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| DO SQLite limitations | High | Medium | Fall back to custom B-tree if needed |
| Subrequest limits | High | High | Hibernating WebSocket, batching |
| Cross-shard consistency | High | Medium | 2PC or eventual consistency |
| Cold start latency | Medium | High | Edge caching, snippets routing |
| Parquet parsing overhead | Medium | Low | Streaming decompression |

### Business Risks

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| D1 feature parity | High | Medium | Differentiate on sharding, CDC |
| Turso competition | Medium | Medium | Focus on Cloudflare-native |
| Cloudflare platform changes | Medium | Low | Abstract storage layer |
| Adoption barriers | Medium | Medium | Migration guides, D1 compatibility |

### Operational Risks

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Data loss | Critical | Low | WAL + R2 backup, DO replication |
| Performance degradation | High | Medium | Auto-scaling, monitoring |
| Breaking changes | Medium | Medium | Semantic versioning, deprecation |

---

## Success Metrics

### Product Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Query latency p50 | < 20ms | Benchmark suite |
| Query latency p99 | < 100ms | Benchmark suite |
| Write throughput | > 10K ops/sec | Benchmark suite |
| CDC latency | < 1 second | Integration tests |
| Time travel query | < 500ms | Benchmark suite |

### Adoption Metrics

| Metric | Target (6 months) | Target (12 months) |
|--------|-------------------|-------------------|
| npm downloads/month | 1,000 | 10,000 |
| GitHub stars | 500 | 2,000 |
| Production deployments | 10 | 100 |
| Active contributors | 5 | 20 |

---

## Recommendations

### Immediate Actions (This Week)

1. **Migrate POC code** - Move dosql and dolake from pocs to sql monorepo
2. **Set up CI/CD** - GitHub Actions for build, test, lint
3. **Create test utilities** - Shared testing infrastructure
4. **Document architecture** - Write ARCHITECTURE.md with detailed design

### Short-term Actions (Next Month)

5. **Complete single-shard DoSQL** - Working database on single DO
6. **Implement CDC pipeline** - Stream changes to DoLake
7. **Write integration tests** - Full coverage of core paths
8. **Benchmark baseline** - Establish performance baselines

### Medium-term Actions (Next Quarter)

9. **Add horizontal sharding** - Multi-shard query routing
10. **Implement Parquet analytics** - Query historical data
11. **Create client SDK** - TypeScript client with connection pooling
12. **Write documentation** - User guides, API reference

### Long-term Vision (Next Year)

13. **Production hardening** - Security, monitoring, reliability
14. **Ecosystem integration** - D1 migration, Turso compatibility
15. **Enterprise features** - RBAC, audit logging, encryption
16. **Community building** - Open source, contributors, adoption

---

## Conclusion

DoSQL + DoLake represents a compelling vision for edge-native databases on Cloudflare. The combination of:

- **Transactional OLTP** (DoSQL) with strong consistency via Durable Objects
- **Analytical OLAP** (DoLake) with cost-effective R2 Parquet storage
- **Real-time CDC** pipeline connecting the two

...addresses a genuine gap in the Cloudflare ecosystem that neither D1 nor external databases like Turso fully fill.

**Key differentiators:**
1. Native to Cloudflare Workers and Durable Objects
2. Horizontal sharding built from the ground up
3. Unified transactional and analytical queries
4. CDC-driven lakehouse with time travel

**Critical path to success:**
1. Leverage existing POC work (dosql, dolake in pocs)
2. Reuse proven patterns from evodb and graphdb
3. Focus on single-shard MVP before sharding
4. Build community around Cloudflare Workers developers

The project is well-positioned but needs to move from vision to implementation. The detailed architecture in CLAUDE.md and the related work across the @dotdo ecosystem provide a strong foundation. Execution is now the priority.

---

*Generated by Claude Opus 4.5 on 2026-01-22*
