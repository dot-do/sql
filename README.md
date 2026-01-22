# @dotdo/sql

SQL database for Cloudflare Workers - DoSQL and DoLake.

## Overview

This monorepo contains the DoSQL and DoLake packages for building SQL databases on Cloudflare Workers using Durable Objects.

### Packages

| Package | Description |
|---------|-------------|
| `@dotdo/dosql` | SQL database engine with SQLite-compatible syntax |
| `@dotdo/dolake` | Lakehouse for CDC streaming and Parquet storage |

## Features

- **SQLite-compatible SQL** - Full SQL parser with SELECT, INSERT, UPDATE, DELETE, JOINs, aggregates, window functions, CTEs
- **Durable Objects** - Each database instance runs in a Durable Object for strong consistency
- **Sharding** - Horizontal scaling with automatic query routing
- **CDC Streaming** - Change Data Capture for real-time data pipelines
- **Lakehouse** - Parquet files on R2 for analytical queries
- **Time Travel** - Query historical data at any point in time

## Getting Started

```bash
# Install dependencies
pnpm install

# Build all packages
pnpm build

# Run tests
pnpm test
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Client Application                       │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│                      DoSQL Gateway                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │ SQL Parser  │  │  Planner    │  │  Query Optimizer    │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────┬───────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
┌───────▼───────┐ ┌───────▼───────┐ ┌───────▼───────┐
│   Shard DO    │ │   Shard DO    │ │   Shard DO    │
│  (SQLite)     │ │  (SQLite)     │ │  (SQLite)     │
└───────┬───────┘ └───────┬───────┘ └───────┬───────┘
        │                 │                 │
        │         CDC Events                │
        │                 │                 │
┌───────▼─────────────────▼─────────────────▼───────┐
│                      DoLake                        │
│  ┌─────────────┐  ┌─────────────┐  ┌───────────┐  │
│  │ CDC Ingest  │  │  Compaction │  │  R2 Store │  │
│  └─────────────┘  └─────────────┘  └───────────┘  │
└───────────────────────────────────────────────────┘
```

## Development

```bash
# Create a new issue
bd create --title="Add feature X" --type=feature --priority=1

# Find available work
bd ready

# Start working on an issue
bd update <id> --status=in_progress

# Complete work
bd close <id>
bd sync
git push
```

## License

MIT
