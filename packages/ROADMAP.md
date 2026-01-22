# DoSQL + DoLake Roadmap

This roadmap outlines the development milestones for the DoSQL and DoLake packages. It covers current status, planned features, and criteria for stable releases.

---

## Current Release: v0.1.0-alpha

**Status:** Developer Preview

### Core Features Working

- **CRUD Operations** - Full SQL support for SELECT, INSERT, UPDATE, DELETE
- **Transactions** - ACID-compliant transaction management with savepoints
- **CDC Streaming** - Real-time change data capture from DoSQL to DoLake
- **WebSocket RPC** - Efficient hibernating WebSocket connections for DO-to-DO communication
- **Parquet Writing** - Columnar storage with configurable compression (snappy, gzip, zstd)
- **Iceberg Metadata** - Full table format support (snapshots, manifests, schema evolution)
- **Rate Limiting** - Token bucket algorithm with backpressure signaling
- **Deduplication** - Configurable window for exactly-once semantics

### Known Limitations

See individual package READMEs for detailed limitations:

- [DoSQL README](./dosql/README.md) - Experimental APIs (time travel, branching, virtual tables)
- [DoLake README](./dolake/README.md) - Experimental APIs (REST Catalog, compaction, query routing)

---

## v0.1.0 Stable

**Target:** Production-ready release for early adopters

### Milestone Criteria

| Requirement | Status | Notes |
|-------------|--------|-------|
| All P0/P1 issues resolved | Pending | Critical bugs blocking production use |
| 80%+ test coverage | Pending | Unit, integration, and E2E tests |
| E2E tests passing on production | Pending | Real Cloudflare Workers environment |
| Documentation complete | Pending | Getting started, API reference, tutorials |
| No known critical bugs | Pending | Verified through production testing |
| Performance benchmarks | Pending | Latency and throughput documented |

### Deliverables

- [ ] Comprehensive API documentation for all public interfaces
- [ ] Migration guide from alpha to stable
- [ ] Performance benchmarks published (p50, p95, p99 latencies)
- [ ] Error handling improvements with typed error classes
- [ ] Validation for all user inputs using Zod schemas
- [ ] Monitoring and observability hooks (metrics, traces)

---

## v0.2.0

**Target:** Operational improvements for production workloads

### Planned Features

#### Connection Pooling
- Persistent connection management for high-throughput scenarios
- Automatic connection health checks and recycling
- Configurable pool sizes per shard/tenant

#### Automatic Failover
- Health monitoring for DoSQL and DoLake instances
- Automatic promotion of replicas on primary failure
- Graceful degradation with read-only fallback mode

#### Multi-Region Replication
- Cross-region data replication for disaster recovery
- Configurable replication lag thresholds
- Conflict resolution strategies for concurrent writes

#### Schema Migrations CLI
- `dosql migrate` command for applying migrations
- Migration status tracking and rollback support
- Dry-run mode for validating migrations
- Version compatibility checks

### Additional Improvements

- [ ] Query caching with configurable TTL
- [ ] Batch operations API for bulk inserts/updates
- [ ] Improved compaction strategies for DoLake
- [ ] Partition pruning optimization
- [ ] Memory pressure monitoring and auto-scaling hints

---

## v1.0.0

**Target:** Enterprise-ready stable release

### Criteria for 1.0

| Requirement | Description |
|-------------|-------------|
| 6+ months production usage | Proven stability across diverse workloads |
| Stable API with deprecation policy | Semantic versioning with 6-month deprecation notices |
| Performance benchmarks published | Verified against industry standards |
| Enterprise features | Security, compliance, and audit capabilities |

### Enterprise Features

#### Row-Level Security (RLS)
- Policy-based access control at the row level
- Tenant isolation enforcement
- Dynamic security predicates

#### Audit Logging
- Immutable audit trail for all data modifications
- Configurable retention policies
- Integration with external SIEM systems

#### Additional Enterprise Capabilities
- [ ] SOC 2 compliance documentation
- [ ] GDPR data handling features (right to deletion, export)
- [ ] Advanced encryption (customer-managed keys)
- [ ] SLA guarantees with defined uptime targets
- [ ] Priority support channels

### API Stability Guarantees

Starting with v1.0.0:

1. **Breaking changes** require major version bump (2.0.0, 3.0.0, etc.)
2. **Deprecated APIs** remain functional for minimum 2 minor versions
3. **Deprecation warnings** logged 6 months before removal
4. **Migration guides** provided for all breaking changes

---

## Future Roadmap (Post-1.0)

### v1.1.0 - Analytics Integration
- Native DuckDB query engine integration
- Materialized views with incremental refresh
- Time-series aggregation functions

### v1.2.0 - Advanced Replication
- Synchronous replication mode
- Multi-master write support
- Global transaction IDs

### v2.0.0 - Next Generation
- GraphQL API layer
- Real-time subscriptions
- Edge-native query optimizer

---

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](../CONTRIBUTING.md) for:

- Development setup instructions
- TDD workflow (Red-Green-Refactor)
- Testing guidelines with `workers-vitest-pool`
- Pull request process

### How to Help

1. **Report issues** - File bugs and feature requests on GitHub
2. **Write tests** - Help improve coverage with TDD-style tests
3. **Documentation** - Improve guides and examples
4. **Code contributions** - Pick up issues tagged `good-first-issue`

### Priority Areas for Contributions

- E2E test coverage for edge cases
- Performance benchmarks and optimization
- Documentation and tutorials
- Error message improvements
- TypeScript type refinements

---

## Version History

| Version | Date | Status |
|---------|------|--------|
| v0.1.0-alpha | 2024-01 | Current - Developer Preview |
| v0.1.0 | TBD | Planned - Stable Release |
| v0.2.0 | TBD | Planned - Operational Features |
| v1.0.0 | TBD | Planned - Enterprise Ready |

---

## Feedback

We actively collect feedback from early adopters. Please share your experience:

- **GitHub Discussions** - General questions and feature discussions
- **GitHub Issues** - Bug reports and specific feature requests
- **Discord** - Real-time community support

Your feedback directly influences this roadmap.
