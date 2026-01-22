# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-01-22

### Added

#### DoSQL Edge Database
- Edge SQL database powered by Durable Objects with full TypeScript type safety
- Support for SQLite-compatible SQL dialect optimized for edge execution
- Automatic schema inference and type generation
- Multi-region deployment with automatic data locality
- Connection pooling and session management

#### DoLake Lakehouse
- Lakehouse architecture with Apache Iceberg table format on Cloudflare R2
- Native Apache Parquet read/write support with column pruning and predicate pushdown
- Time travel queries with snapshot isolation
- Schema evolution with backward and forward compatibility
- Partition pruning for efficient large-scale queries
- Compaction and optimization workflows

#### Client SDKs
- `sql.do` - TypeScript SDK for DoSQL edge database interactions
- `lake.do` - TypeScript SDK for DoLake lakehouse queries
- Unified query builder with fluent API
- Streaming result sets for large query results
- Automatic retry with exponential backoff

#### CapnWeb RPC Protocol
- Binary RPC protocol for efficient client-server communication
- WebSocket-based transport with multiplexing support
- Request/response correlation with unique message IDs
- Bi-directional streaming for real-time updates
- Protocol versioning for backward compatibility

#### CDC Streaming
- Change Data Capture from Durable Objects to lakehouse
- Real-time event streaming with at-least-once delivery guarantees
- Configurable batch sizes and flush intervals
- Dead letter queue for failed events
- Schema registry integration for event evolution

#### ORM Integrations
- **Drizzle ORM** - Full integration with type-safe schema definitions
- **Kysely** - Query builder integration with dialect support
- **Knex** - Migration and query building support
- **Prisma** - Prisma Client integration with custom datasource

#### Virtual Tables
- Virtual table support for external URL data sources
- JSON, CSV, and Parquet URL sources
- Lazy loading with caching policies
- Schema inference from remote sources
- Join capabilities between virtual and physical tables

#### Vector Search
- Native vector embedding storage and indexing
- HNSW index support for approximate nearest neighbor search
- Cosine similarity, Euclidean distance, and dot product metrics
- Hybrid search combining vector and keyword queries
- Integration with popular embedding models

#### Transaction Support
- ACID transaction guarantees within Durable Object boundaries
- Optimistic concurrency control with version vectors
- Deadlock detection with automatic resolution
- Savepoints for partial rollback
- Distributed transaction coordination across DOs

#### Error Handling
- Comprehensive error hierarchy with typed exceptions
- `SqlSyntaxError` - Parse and syntax errors with position info
- `ConstraintViolationError` - Primary key, foreign key, unique violations
- `TransactionError` - Commit failures, deadlocks, timeouts
- `ConnectionError` - Network and transport failures
- `QueryTimeoutError` - Execution time limit exceeded
- Structured error codes for programmatic handling

#### SQL Injection Prevention
- SQL tokenizer with whitelist-based validation
- Parameterized query enforcement
- Automatic escaping of user inputs
- Query pattern analysis and blocking
- Audit logging of suspicious queries

### Security

#### SQL Injection Protection
- Multi-layer SQL tokenizer prevents injection attacks
- Lexical analysis validates query structure before execution
- Parameterized queries enforced at protocol level
- Known attack pattern detection and blocking

#### WebSocket Security
- Payload size validation with configurable limits (default: 1MB)
- Message rate limiting per connection
- Origin validation for cross-origin requests
- Automatic connection termination on policy violations

#### Rate Limiting
- Configurable rate limiting with token bucket algorithm
- Per-IP, per-user, and per-API-key thresholds
- Sliding window counters for burst protection
- Custom rate limit headers in responses
- Graceful degradation under load

### Changed
- N/A (initial release)

### Deprecated
- N/A (initial release)

### Removed
- N/A (initial release)

### Fixed
- N/A (initial release)

---

## [Unreleased]

### Added
- _Future features will be listed here_

### Changed
- _Future changes will be listed here_

### Deprecated
- _Deprecations will be listed here_

### Removed
- _Removals will be listed here_

### Fixed
- _Bug fixes will be listed here_

### Security
- _Security updates will be listed here_
