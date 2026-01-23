# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Nothing yet

## [0.1.0] - 2026-01-22

### Added
- Initial release of `lake.do` client SDK
- `createLakeClient` factory function for creating DoLake connections
- `DoLakeClient` class implementing CapnWeb RPC over WebSocket
- SQL query execution with `query<T>()` method
- CDC streaming subscription with `subscribe()` async iterable
- Time travel queries with `asOf` option (Date or SnapshotId)
- Partition management with `listPartitions()`
- Compaction operations with `compact()` and `getCompactionStatus()`
- Snapshot listing with `listSnapshots()`
- Table metadata retrieval with `getMetadata()`
- Connection health check with `ping()`
- Branded types for type-safe identifiers:
  - `CDCEventId`, `PartitionKey`, `ParquetFileId`, `SnapshotId`, `CompactionJobId`
- Factory functions for branded types:
  - `createCDCEventId`, `createPartitionKey`, `createParquetFileId`, etc.
- Type guards for branded types:
  - `isCDCEventId`, `isPartitionKey`, `isParquetFileId`, etc.
- Error types: `LakeError`, `ConnectionError`, `QueryError`
- Event emitter API: `on()`, `off()`, `once()` for connection events
- Retry configuration support
- CDCStreamController with backpressure support
- Experimental module exports via `lake.do/experimental`

### Dependencies
- Requires `sql.do` as peer dependency for shared types

[Unreleased]: https://github.com/dot-do/sql/compare/lake.do@0.1.0...HEAD
[0.1.0]: https://github.com/dot-do/sql/releases/tag/lake.do@0.1.0
