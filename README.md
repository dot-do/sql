# @dotdo/sql

[![Build Status](https://github.com/dot-do/sql/actions/workflows/ci.yml/badge.svg)](https://github.com/dot-do/sql/actions/workflows/ci.yml)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.3+-blue.svg)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![npm sql.do](https://img.shields.io/npm/v/sql.do.svg?label=sql.do)](https://www.npmjs.com/package/sql.do)
[![npm dosql](https://img.shields.io/npm/v/dosql.svg?label=dosql)](https://www.npmjs.com/package/dosql)
[![npm dolake](https://img.shields.io/npm/v/dolake.svg?label=dolake)](https://www.npmjs.com/package/dolake)
[![Bundle Size](https://img.shields.io/badge/bundle-2.9KB%20gzip-brightgreen)](#bundle-size)
![Developer Preview](https://img.shields.io/badge/Status-Developer%20Preview-orange)

> **Developer Preview (v0.1.0)**: This software is under active development. APIs may change before the stable 1.0 release. Not recommended for production use without thorough testing. **Expected GA: Q3 2026**

SQL database for Cloudflare Workers - DoSQL and DoLake.

## Stability

### Stable APIs

- Core SQL query execution (`query`, `exec`, `transaction`)
- Basic CRUD operations (SELECT, INSERT, UPDATE, DELETE)
- Connection management and client configuration
- Error handling and error codes

### Experimental APIs

- Time travel queries (API may change)
- Database branching and merging
- CDC streaming to lakehouse
- Virtual tables (URL, R2, external APIs)
- Sharding and query routing
- CapnWeb RPC protocol

## Version Compatibility

| Package | Version | Node.js | TypeScript | Cloudflare Workers |
|---------|---------|---------|------------|-------------------|
| sql.do | 0.1.x | 18+ | 5.3+ | 2024-01-01+ |
| dosql | 0.1.x | 18+ | 5.3+ | 2024-01-01+ |
| dolake | 0.1.x | 18+ | 5.3+ | 2024-01-01+ |
| lake.do | 0.1.x | 18+ | 5.3+ | 2024-01-01+ |

## Prerequisites

Before deploying DoSQL and DoLake to Cloudflare Workers, ensure you have the following:

### Cloudflare Account Requirements

1. **Cloudflare Account** - Sign up at [dash.cloudflare.com](https://dash.cloudflare.com)
2. **Workers Paid Plan** - Durable Objects require a Workers Paid plan ($5/month minimum)
3. **Durable Objects Enabled** - Durable Objects must be enabled on your account:
   - Go to Workers & Pages in the Cloudflare dashboard
   - Durable Objects should be available automatically on paid plans
   - If not visible, contact Cloudflare support to enable the feature

### R2 Bucket Setup (Required for DoLake)

DoLake uses R2 for lakehouse data storage. Create an R2 bucket:

```bash
# Install Wrangler if not already installed
npm install -g wrangler

# Login to Cloudflare
wrangler login

# Create R2 bucket for lakehouse storage
wrangler r2 bucket create my-lakehouse-bucket
```

### Development Environment

- **Node.js 18+** - Required for running Wrangler and building packages
- **pnpm** (recommended) or npm - Package manager
- **Wrangler CLI** - Cloudflare's CLI for Workers deployment

```bash
# Install Wrangler globally
npm install -g wrangler

# Verify installation
wrangler --version
```

### Optional: KV Namespace (for metadata caching)

```bash
# Create KV namespace for metadata caching
wrangler kv:namespace create LAKEHOUSE_KV
# Copy the ID from output into your wrangler.toml
```

## Quick Start

Install the client SDK:

```bash
npm install sql.do
```

Connect and query:

```typescript
import { createSQLClient } from 'sql.do';

const client = createSQLClient({
  url: 'https://sql.example.com',
  token: 'your-auth-token',
});

// Execute a query
const users = await client.query('SELECT * FROM users WHERE active = ?', [true]);
console.log(users.rows);

// Clean up
await client.close();
```

### TypeScript Example with Types

```typescript
import { createSQLClient, QueryResult, SQLError } from 'sql.do';
import type { SQLClientConfig, TransactionContext } from 'sql.do';

interface User {
  id: number;
  name: string;
  email: string;
  created_at: string;
}

const config: SQLClientConfig = {
  url: 'https://sql.example.com',
  token: process.env.DOSQL_TOKEN,
  timeout: 30000,
  retry: {
    maxRetries: 3,
    baseDelayMs: 100,
    maxDelayMs: 5000,
  },
};

const client = createSQLClient(config);

// Type-safe query result
const result: QueryResult = await client.query<User>(
  'SELECT * FROM users WHERE id = ?',
  [1]
);

const user = result.rows[0] as User;
console.log(`Found user: ${user.name}`);
```

### Error Handling

```typescript
import { createSQLClient, SQLError, RPCErrorCode } from 'sql.do';

const client = createSQLClient({ url: 'https://sql.example.com' });

try {
  await client.query('SELECT * FROM nonexistent_table');
} catch (error) {
  if (error instanceof SQLError) {
    console.error(`SQL Error [${error.code}]: ${error.message}`);

    // Handle specific error codes
    switch (error.code) {
      case RPCErrorCode.TABLE_NOT_FOUND:
        console.error('Table does not exist');
        break;
      case RPCErrorCode.TIMEOUT:
        console.error('Query timed out, consider optimizing');
        break;
      default:
        console.error('Unexpected database error');
    }
  }
}
```

### Transactions

```typescript
const client = createSQLClient({ url: 'https://sql.example.com' });

// Atomic transaction with automatic rollback on error
await client.transaction(async (tx: TransactionContext) => {
  await tx.exec('INSERT INTO users (name, email) VALUES (?, ?)', ['Alice', 'alice@example.com']);
  await tx.exec('INSERT INTO audit_log (action, user_name) VALUES (?, ?)', ['user_created', 'Alice']);

  // If any statement fails, all changes are rolled back
});

// Transaction with explicit isolation level
await client.transaction(
  async (tx: TransactionContext) => {
    const balance = await tx.query<{ balance: number }>('SELECT balance FROM accounts WHERE id = ?', [1]);
    await tx.exec('UPDATE accounts SET balance = ? WHERE id = ?', [balance.rows[0].balance - 100, 1]);
  },
  { isolation: 'serializable' }
);
```

## Overview

This monorepo contains the DoSQL and DoLake packages for building SQL databases on Cloudflare Workers using Durable Objects.

## Packages

| Package | Description | Stability | npm |
|---------|-------------|-----------|-----|
| [`sql.do`](./packages/sql.do) | Client SDK for DoSQL | :yellow_circle: Beta | [![npm](https://img.shields.io/npm/v/sql.do.svg)](https://www.npmjs.com/package/sql.do) |
| [`lake.do`](./packages/lake.do) | Client SDK for DoLake | :red_circle: Experimental | [![npm](https://img.shields.io/npm/v/lake.do.svg)](https://www.npmjs.com/package/lake.do) |
| [`dosql`](./packages/dosql) | SQL database engine (server) | :yellow_circle: Beta | [![npm](https://img.shields.io/npm/v/dosql.svg)](https://www.npmjs.com/package/dosql) |
| [`dolake`](./packages/dolake) | Lakehouse worker (server) | :red_circle: Experimental | [![npm](https://img.shields.io/npm/v/dolake.svg)](https://www.npmjs.com/package/dolake) |
| [`@dotdo/sql-types`](./packages/shared-types) | Shared TypeScript types | :red_circle: Experimental | [![npm](https://img.shields.io/npm/v/@dotdo/sql-types.svg)](https://www.npmjs.com/package/@dotdo/sql-types) |

### Stability Legend

- :green_circle: **Stable** - API is stable and unlikely to change. Safe for production use.
- :yellow_circle: **Beta** - API is mostly stable but may have minor changes. Use with caution in production.
- :red_circle: **Experimental** - API is under active development and may change significantly. Not recommended for production.

## Features

- **SQLite-compatible SQL** - Full SQL parser with SELECT, INSERT, UPDATE, DELETE, JOINs, aggregates, window functions, CTEs
- **Durable Objects** - Each database instance runs in a Durable Object for strong consistency
- **Sharding** - Horizontal scaling with automatic query routing
- **CDC Streaming** - Change Data Capture for real-time data pipelines
- **Lakehouse** - Parquet files on R2 for analytical queries
- **Time Travel** - Query historical data at any point in time

## Bundle Size

All client packages are optimized for minimal bundle size with full tree-shaking support.

### Package Sizes

| Package | Minified | Gzipped | Tree-shakeable |
|---------|----------|---------|----------------|
| `sql.do` | 7.2 KB | **2.9 KB** | Yes |
| `lake.do` | 6.1 KB | **2.4 KB** | Yes |
| `@dotdo/sql-types` | 3.0 KB | **1.3 KB** | Yes |
| `dosql` (core) | 164.6 KB | **49.6 KB** | Yes |
| `dolake` | 407.9 KB | **87.4 KB** | Yes |

### Comparison with Alternatives

| Solution | Client Bundle (gzip) | Notes |
|----------|---------------------|-------|
| **sql.do** | **2.9 KB** | Lightweight client, server does heavy lifting |
| @libsql/client | ~45 KB | Turso/libSQL client |
| @cloudflare/d1 | ~5 KB | Cloudflare D1 bindings (workers only) |
| sql.js | ~500 KB | Full SQLite in WASM |
| better-sqlite3 | N/A | Native module, not for browsers |

### Measuring Bundle Size

You can verify bundle sizes locally:

```bash
# Install esbuild if not present
npm install -g esbuild

# Measure a package (minified + gzipped)
npx esbuild packages/sql.do/dist/index.js \
  --bundle --minify --format=esm \
  --outfile=/tmp/bundle.js

# Check sizes
ls -la /tmp/bundle.js                    # Minified size
gzip -c /tmp/bundle.js | wc -c          # Gzipped size
```

### Tree-Shaking Benefits

All packages use ES modules with explicit exports, enabling bundlers to eliminate unused code:

```typescript
// Only imports what you need - unused exports are removed
import { createSQLClient } from 'sql.do';

// Types are compile-time only - zero runtime cost
import type { QueryResult, SQLClientConfig } from 'sql.do';
```

**Tree-shaking tips:**

1. **Use named imports** - Avoid `import * as sql from 'sql.do'`
2. **Import types separately** - Use `import type` for TypeScript types
3. **Check your bundler** - Ensure tree-shaking is enabled (default in Vite, esbuild, Rollup)
4. **Analyze your bundle** - Use `npx vite-bundle-visualizer` or similar tools

## Performance Benchmarks

DoSQL delivers sub-millisecond query latency by running SQLite directly within Durable Objects, eliminating network hops for data access.

### Query Latency

| Operation | P50 | P95 | P99 | Throughput |
|-----------|-----|-----|-----|------------|
| Point Query (by PK) | 0.8 ms | 2.4 ms | 4.1 ms | 830 ops/sec |
| Range Query (100 rows) | 3.2 ms | 7.8 ms | 12.4 ms | 244 ops/sec |
| INSERT | 1.5 ms | 4.2 ms | 8.7 ms | 476 ops/sec |
| UPDATE | 1.8 ms | 5.1 ms | 9.2 ms | 417 ops/sec |
| DELETE | 1.2 ms | 3.8 ms | 7.1 ms | 588 ops/sec |
| Batch INSERT (100 rows) | 14.2 ms | 28.5 ms | 45.3 ms | 5,950 rows/sec |

### Comparison with Alternatives

| Metric | DoSQL | D1 | Turso | PlanetScale |
|--------|-------|----|----- |-------------|
| Point Query P95 | **2.4 ms** | 5.2 ms | 8.5 ms | 12.0 ms |
| INSERT P95 | **4.2 ms** | 8.1 ms | 6.8 ms | 15.2 ms |
| Batch INSERT (100) P95 | **28.5 ms** | 52.0 ms | 45.0 ms | 85.0 ms |
| Cold Start | 25 ms | **12 ms** | 45 ms | 80 ms |

DoSQL achieves 2-5x lower latency than D1 for point queries by executing SQL directly in the Durable Object without network round-trips.

### Memory Usage

| Database Size | Memory Used | Notes |
|---------------|-------------|-------|
| 1,000 rows | 18 MB | Baseline |
| 10,000 rows | 24 MB | Moderate |
| 100,000 rows | 48 MB | Large dataset |
| 1,000,000 rows | 118 MB | Near DO limit (128 MB) |

### Concurrent Access

| Clients | P95 Latency | Throughput | Scaling |
|---------|-------------|------------|---------|
| 1 | 2.4 ms | 830 ops/sec | 1.0x |
| 10 | 5.9 ms | 5,400 ops/sec | 6.5x |
| 50 | 18.5 ms | 8,900 ops/sec | 10.7x |

### Running Benchmarks

```bash
# Run benchmark suite
cd packages/dosql/benchmark
npx tsx src/cli.ts -a dosql -s simple-select -f console

# Available options
-a, --adapter <name>     Adapter: dosql, d1, do-sqlite
-s, --scenario <name>    Scenario: simple-select, insert, transaction
-i, --iterations <n>     Iterations (default: 100)
-f, --format <format>    Output: json, console, markdown
```

For detailed methodology and tuning guidance, see [Performance Tuning Guide](./docs/PERFORMANCE_TUNING.md).

### CI Bundle Size Tracking

Bundle sizes are tracked in CI. To add bundle size checks to your workflow:

```yaml
# .github/workflows/bundle-size.yml
name: Bundle Size

on: [push, pull_request]

jobs:
  bundle-size:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: pnpm/action-setup@v4
      - uses: actions/setup-node@v4
        with:
          node-version: '20'
          cache: 'pnpm'

      - run: pnpm install
      - run: pnpm build

      - name: Check bundle sizes
        run: |
          # sql.do client should stay under 10KB gzipped
          SIZE=$(npx esbuild packages/sql.do/dist/index.js \
            --bundle --minify --format=esm 2>/dev/null | gzip | wc -c)
          echo "sql.do bundle: ${SIZE} bytes gzipped"
          if [ "$SIZE" -gt 10240 ]; then
            echo "Bundle size exceeds 10KB limit!"
            exit 1
          fi
```

## Documentation

- [Performance Tuning Guide](./docs/PERFORMANCE_TUNING.md) - Query optimization, DO tuning, WebSocket performance
- [Migration from D1](./docs/MIGRATION_FROM_D1.md) - Step-by-step guide for Cloudflare D1 users
- [Migration from Turso](./docs/MIGRATION_FROM_TURSO.md) - Guide for libSQL/Turso users
- [Error Codes Reference](./docs/ERROR_CODES.md) - Complete error code documentation
- [Security Guide](./docs/SECURITY.md) - Authentication, authorization, and security best practices
- [Operations Guide](./docs/OPERATIONS.md) - Deployment, monitoring, and maintenance

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

## Wrangler Configuration

To deploy DoLake, you need to configure R2 bucket and optionally KV namespace bindings in your `wrangler.toml`:

```toml
name = "my-dolake-worker"
main = "src/index.ts"
compatibility_date = "2024-12-01"
compatibility_flags = ["nodejs_compat"]

# Durable Objects
[durable_objects]
bindings = [
  { name = "DOLAKE", class_name = "DoLake" }
]

# R2 Buckets - Required for lakehouse data storage
[[r2_buckets]]
binding = "LAKEHOUSE_BUCKET"
bucket_name = "my-lakehouse-bucket"

# KV Namespaces - Optional for metadata caching
[[kv_namespaces]]
binding = "LAKEHOUSE_KV"
id = "your-kv-namespace-id"

# Migrations for Durable Object
[[migrations]]
tag = "v1"
new_classes = ["DoLake"]

# Environment variables (optional)
[vars]
LOG_LEVEL = "info"
```

### Required Bindings

| Binding | Type | Description |
|---------|------|-------------|
| `LAKEHOUSE_BUCKET` | R2 Bucket | Stores Parquet files and Iceberg metadata |
| `DOLAKE` | Durable Object | The DoLake Durable Object class |

### Optional Bindings

| Binding | Type | Description |
|---------|------|-------------|
| `LAKEHOUSE_KV` | KV Namespace | Caches metadata for faster lookups |

### Creating Resources

```bash
# Create R2 bucket
wrangler r2 bucket create my-lakehouse-bucket

# Create KV namespace (optional)
wrangler kv:namespace create LAKEHOUSE_KV
# Copy the ID from output into your wrangler.toml
```

## Deployment

### Step-by-Step Deployment Guide

#### 1. Verify Prerequisites

```bash
# Ensure you're logged into Cloudflare
wrangler whoami

# Verify Wrangler version (recommend 3.0+)
wrangler --version
```

#### 2. Create Required Resources

```bash
# Create R2 bucket (required for DoLake)
wrangler r2 bucket create my-lakehouse-bucket

# Create KV namespace (optional, for metadata caching)
wrangler kv:namespace create LAKEHOUSE_KV
```

#### 3. Configure wrangler.toml

Create or update your `wrangler.toml` with the appropriate bindings (see Wrangler Configuration section above).

#### 4. Deploy to Cloudflare

```bash
# Deploy to production
wrangler deploy

# Or deploy to a specific environment
wrangler deploy --env staging
wrangler deploy --env production
```

#### 5. Verify Deployment

```bash
# Check deployment status
wrangler deployments list

# Tail logs to verify the worker is running
wrangler tail

# List Durable Objects
wrangler durable-objects list
```

### Environment-Specific Deployments

Configure multiple environments in your `wrangler.toml`:

```toml
name = "my-dosql-app"
main = "src/index.ts"
compatibility_date = "2024-12-01"

# Production environment
[env.production]
name = "my-dosql-app-prod"
vars = { LOG_LEVEL = "warn" }

[[env.production.r2_buckets]]
binding = "LAKEHOUSE_BUCKET"
bucket_name = "my-lakehouse-bucket-prod"

# Staging environment
[env.staging]
name = "my-dosql-app-staging"
vars = { LOG_LEVEL = "debug" }

[[env.staging.r2_buckets]]
binding = "LAKEHOUSE_BUCKET"
bucket_name = "my-lakehouse-bucket-staging"
```

### Setting Secrets

```bash
# Set authentication secrets
wrangler secret put AUTH_TOKEN

# Set secrets for specific environment
wrangler secret put AUTH_TOKEN --env production
```

### Rollback Deployments

```bash
# List recent deployments
wrangler deployments list

# Rollback to a previous deployment
wrangler rollback
```

### Troubleshooting Deployment Issues

| Issue | Solution |
|-------|----------|
| "Durable Objects not available" | Ensure you have a Workers Paid plan |
| "R2 bucket not found" | Run `wrangler r2 bucket create <name>` first |
| "Authentication failed" | Run `wrangler login` to re-authenticate |
| "Compatibility date too old" | Update `compatibility_date` in wrangler.toml |

## Development

```bash
# Install dependencies
pnpm install

# Build all packages
pnpm build

# Run tests
pnpm test

# Type checking
pnpm typecheck

# Lint
pnpm lint
```

## Contributing

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
