# Changelog

All notable changes to this package will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial release preparation

## [0.1.0] - 2026-01-22

### Added
- **DoLake Durable Object**: Main lakehouse worker for CDC streaming to Iceberg/Parquet on R2
- **WebSocket Hibernation**: 95% cost reduction on idle connections using Cloudflare hibernation API
- **CDC Streaming**: Real-time change data capture from multiple DoSQL shards via WebSocket
- **Parquet Writing**: Efficient columnar storage with configurable compression (snappy, gzip, zstd)
- **Iceberg Metadata**: Full Iceberg table format support including snapshots, manifests, and schema evolution
- **REST Catalog API**: Standard Iceberg REST Catalog for external query engines (Spark, Trino, DuckDB)
- **Rate Limiting**: Token bucket algorithm with per-connection and per-IP limits
  - Subnet-level rate limiting (/24 IPv4, /64 IPv6)
  - Configurable whitelisted networks for private IP ranges
  - Backpressure signaling via `suggestedDelayMs`
- **Deduplication**: Configurable deduplication window for exactly-once semantics
- **Automatic Compaction**: Background compaction of small Parquet files
- **Partition Management**: Time-based (day/hour), identity, and bucket partitioning strategies
- **Query Engine**: Built-in query routing and partition pruning
- **Zod Validation**: Runtime type validation for all incoming WebSocket messages
- **Cache Invalidation**: Automatic cache invalidation on CDC events
- **Analytics Events**: P2 durability support for analytics event handling
- **Scalability Components**:
  - `ParallelWriteManager` for concurrent Parquet writes
  - `HorizontalScalingManager` for multi-DO coordination
  - `PartitionCompactionManager` for partition-level compaction
  - `PartitionRebalancer` for load balancing across partitions
  - `LargeFileHandler` for handling oversized files
  - `MemoryEfficientProcessor` for streaming large datasets

### HTTP Endpoints
- `GET /health` - Health check endpoint
- `GET /status` - Current DoLake status and buffer statistics
- `GET /metrics` - Prometheus-format rate limiting and buffer metrics
- `POST /flush` - Trigger manual flush to R2
- `POST /cdc` - HTTP-based CDC ingestion endpoint

### WebSocket Protocol
- `connect` - Connection establishment with capability negotiation
- `cdc_batch` - Batch CDC event streaming
- `heartbeat` / `pong` - Keep-alive with auto-response
- `flush_request` / `flush_response` - On-demand flush coordination
- `ack` / `nack` - Reliable delivery with detailed status

### Security
- Pre-parse size validation to prevent memory exhaustion attacks
- IP-based rate limiting with subnet aggregation
- Configurable connection limits per source and per IP
- Load shedding under critical load conditions

### Dependencies
- `lake.do` - Client SDK for DoLake connections
- `hyparquet` - Parquet file writing
- `zod` - Runtime schema validation

[Unreleased]: https://github.com/dotdo/sql/compare/dolake-v0.1.0...HEAD
[0.1.0]: https://github.com/dotdo/sql/releases/tag/dolake-v0.1.0
