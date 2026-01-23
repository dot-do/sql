# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-01-22

### Added

- **Core Database Operations**
  - `DB` factory function for creating database instances
  - `createDatabase` for programmatic database creation
  - `query` method for type-safe SELECT queries with TypeScript inference
  - `run` method for executing INSERT, UPDATE, DELETE statements
  - `transaction` method with full ACID compliance

- **Schema Migrations**
  - Automatic migration discovery from `.do/migrations/*.sql` directory
  - Sequential migration execution with version tracking
  - Migration state persistence across restarts

- **Transaction Management**
  - Full transaction support with begin, commit, rollback
  - Savepoint support for nested transactions
  - Multiple isolation levels (READ COMMITTED, REPEATABLE READ, SERIALIZABLE)
  - MVCC (Multi-Version Concurrency Control) implementation
  - Lock manager with deadlock detection

- **Write-Ahead Log (WAL)**
  - `createWALWriter` and `createWALReader` for durable writes
  - WAL-based recovery for crash consistency
  - Configurable sync modes for performance tuning

- **Change Data Capture (CDC)** (Experimental)
  - `createCDC` for real-time change streaming
  - `createCDCSubscription` for subscribing to table changes
  - Lakehouse integration for analytics pipelines

- **Time Travel Queries** (Experimental)
  - `FOR SYSTEM_TIME AS OF` syntax support
  - Query historical data at any point in time
  - Automatic version retention management

- **Database Branching** (Experimental)
  - `branch` method for creating database branches
  - `checkout` for switching between branches
  - `merge` for combining branch changes
  - Git-like workflow for database development

- **Virtual Tables** (Experimental)
  - Query JSON/CSV from URLs directly in SQL
  - R2 bucket integration for Parquet/JSON files
  - External API virtual table support

- **CapnWeb RPC Protocol** (Experimental)
  - WebSocket-based DO-to-DO communication
  - `createWebSocketClient` for RPC clients
  - `DoSQLTarget` for routing requests

- **Sharding** (Experimental)
  - `createShardRouter` for query routing
  - `createShardExecutor` for distributed execution
  - Hash-based and range-based sharding strategies

- **Stored Procedures** (Experimental)
  - `procedure` builder for defining procedures
  - `createProcedureRegistry` for procedure management
  - ESM-based procedure execution

- **ORM Adapters**
  - Drizzle ORM adapter (`dosql/orm/drizzle`)
  - Kysely adapter (`dosql/orm/kysely`)
  - Knex adapter (`dosql/orm/knex`)
  - Prisma adapter (`dosql/orm/prisma`)

- **Storage Backends** (Experimental)
  - `createDOBackend` for Durable Object storage
  - `createR2Backend` for R2 cold storage
  - `createTieredBackend` for hot/cold tiering

- **Columnar Storage** (Experimental)
  - `ColumnarWriter` for OLAP-optimized writes
  - `ColumnarReader` for efficient analytical queries

- **Error Handling**
  - `DoSQLError` base error class
  - `DatabaseError` for database-level errors
  - `SQLSyntaxError` for parse errors
  - `ConnectionError` for network issues
  - `TransactionError` with specific error codes

- **HTTP API**
  - `/health` endpoint for health checks
  - `/tables` endpoint for schema introspection
  - `/query` endpoint for SELECT queries
  - `/execute` endpoint for mutations

- **Type Safety**
  - Compile-time SQL query validation
  - Automatic result type inference from SQL
  - Full TypeScript 5.3+ support

[Unreleased]: https://github.com/dotdo/sql/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/dotdo/sql/releases/tag/v0.1.0
