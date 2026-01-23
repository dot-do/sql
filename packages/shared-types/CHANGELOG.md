# Changelog

All notable changes to this package will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-01-22

### Added

- **Branded Types**: Type-safe branded types for `LSN`, `TransactionId`, `ShardId`, and `StatementHash`
  - Factory functions: `createLSN()`, `createTransactionId()`, `createShardId()`, `createStatementHash()`
  - Validation functions: `isValidLSN()`, `isValidTransactionId()`, `isValidShardId()`, `isValidStatementHash()`
  - Runtime validation tracking: `isValidatedLSN()`, `isValidatedTransactionId()`, `isValidatedShardId()`, `isValidatedStatementHash()`

- **LSN Utilities**: Comprehensive LSN manipulation functions
  - Serialization: `serializeLSN()`, `deserializeLSN()`, `lsnToNumber()`, `lsnToBytes()`, `bytesToLSN()`
  - Operations: `compareLSN()`, `incrementLSN()`, `lsnValue()`

- **SQL Value Types**: `SQLValue` union type for all valid SQL parameter and result values

- **Column Types**: Unified column type system
  - `ColumnType`, `SQLColumnType`, `JSColumnType`
  - Type converters: `sqlToJsType()`, `jsToSqlType()`
  - Type maps: `SQL_TO_JS_TYPE_MAP`, `JS_TO_SQL_TYPE_MAP`

- **Query Types**: Request and response types for SQL queries
  - `QueryRequest`, `QueryOptions`, `QueryResponse`, `QueryResult`
  - Converters: `responseToResult()`, `resultToResponse()`

- **CDC Types**: Change Data Capture event types
  - `CDCEvent`, `CDCOperation`, `ClientCDCOperation`, `CDCOperationCode`
  - Type guards: `isServerCDCEvent()`, `isClientCDCEvent()`
  - Converters: `serverToClientCDCEvent()`, `clientToServerCDCEvent()`

- **Transaction Types**: Transaction handling types
  - `IsolationLevel`, `ServerIsolationLevel`
  - `TransactionOptions`, `TransactionState`, `TransactionHandle`

- **RPC Types**: Remote procedure call types
  - `RPCMethod`, `RPCRequest`, `RPCResponse`, `RPCError`, `RPCErrorCode`

- **Schema Types**: Database schema definition types
  - `TableSchema`, `ColumnDefinition`, `IndexDefinition`, `ForeignKeyDefinition`

- **Sharding Types**: Sharding configuration types
  - `ShardConfig`, `ShardInfo`

- **Connection Types**: Client connection types
  - `ConnectionOptions`, `ConnectionStats`

- **Idempotency Types**: Idempotency configuration
  - `IdempotencyConfig`, `DEFAULT_IDEMPOTENCY_CONFIG`

- **Client Capabilities**: Client capability negotiation
  - `ClientCapabilities`, `DEFAULT_CLIENT_CAPABILITIES`

- **Result Pattern**: Standardized error handling
  - `Result`, `Success`, `Failure`, `ResultError`
  - Type guards: `isSuccess()`, `isFailure()`
  - Constructors: `success()`, `failure()`, `failureFromError()`
  - Utilities: `unwrap()`, `unwrapOr()`, `mapResult()`, `flatMapResult()`, `combineResults()`
  - Legacy support: `LegacyResult`, `isLegacySuccess()`, `isLegacyFailure()`, `fromLegacyResult()`

- **Retry Configuration**: Automatic retry behavior types
  - `RetryConfig`, `DEFAULT_RETRY_CONFIG`
  - `isRetryConfig()`, `createRetryConfig()`

- **Runtime Configuration** (from config.ts):
  - Wrapper cache: `setWrapperCacheConfig()`, `getWrapperCacheConfig()`, `WrapperCacheConfig`
  - Dev mode: `setDevMode()`, `isDevMode()`
  - Strict mode: `setStrictMode()`, `isStrictMode()`

- **Wrapper Cache Management**:
  - `getWrapperMapStats()`, `clearWrapperMaps()`
  - LRU eviction for bounded memory usage

- **Prepared Statements**: `PreparedStatement` interface

### Notes

- This is the initial stable release
- All APIs are considered experimental and may change before 1.0
- Designed for use with `sql.do`, `dosql`, `lake.do`, and `dolake`
