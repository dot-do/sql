# @dotdo/sql

[![Build Status](https://github.com/dot-do/sql/actions/workflows/ci.yml/badge.svg)](https://github.com/dot-do/sql/actions/workflows/ci.yml)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.3+-blue.svg)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![npm @dotdo/sql.do](https://img.shields.io/npm/v/@dotdo/sql.do.svg?label=@dotdo/sql.do)](https://www.npmjs.com/package/@dotdo/sql.do)
[![npm @dotdo/dosql](https://img.shields.io/npm/v/@dotdo/dosql.svg?label=@dotdo/dosql)](https://www.npmjs.com/package/@dotdo/dosql)
[![npm @dotdo/dolake](https://img.shields.io/npm/v/@dotdo/dolake.svg?label=@dotdo/dolake)](https://www.npmjs.com/package/@dotdo/dolake)
[![Bundle Size](https://img.shields.io/badge/bundle-2.9KB%20gzip-brightgreen)](#bundle-size)

> **Pre-release Software**: This is v0.1.0. APIs may change. Not recommended for production use without thorough testing.

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
| @dotdo/sql.do | 0.1.x | 18+ | 5.3+ | 2024-01-01+ |
| @dotdo/dosql | 0.1.x | 18+ | 5.3+ | 2024-01-01+ |
| @dotdo/dolake | 0.1.x | 18+ | 5.3+ | 2024-01-01+ |
| @dotdo/lake.do | 0.1.x | 18+ | 5.3+ | 2024-01-01+ |

## Quick Start

Install the client SDK:

```bash
npm install @dotdo/sql.do
```

Connect and query:

```typescript
import { createSQLClient } from '@dotdo/sql.do';

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
import { createSQLClient, QueryResult, SQLError } from '@dotdo/sql.do';
import type { SQLClientConfig, TransactionContext } from '@dotdo/sql.do';

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
import { createSQLClient, SQLError, RPCErrorCode } from '@dotdo/sql.do';

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
await client.transaction(async (tx) => {
  await tx.exec('INSERT INTO users (name, email) VALUES (?, ?)', ['Alice', 'alice@example.com']);
  await tx.exec('INSERT INTO audit_log (action, user_name) VALUES (?, ?)', ['user_created', 'Alice']);

  // If any statement fails, all changes are rolled back
});

// Transaction with explicit isolation level
await client.transaction(
  async (tx) => {
    const balance = await tx.query('SELECT balance FROM accounts WHERE id = ?', [1]);
    await tx.exec('UPDATE accounts SET balance = ? WHERE id = ?', [balance.rows[0].balance - 100, 1]);
  },
  { isolation: 'serializable' }
);
```

## Overview

This monorepo contains the DoSQL and DoLake packages for building SQL databases on Cloudflare Workers using Durable Objects.

## Packages

| Package | Description | npm |
|---------|-------------|-----|
| [`@dotdo/sql.do`](./packages/sql.do) | Client SDK for DoSQL | [![npm](https://img.shields.io/npm/v/@dotdo/sql.do.svg)](https://www.npmjs.com/package/@dotdo/sql.do) |
| [`@dotdo/lake.do`](./packages/lake.do) | Client SDK for DoLake | [![npm](https://img.shields.io/npm/v/@dotdo/lake.do.svg)](https://www.npmjs.com/package/@dotdo/lake.do) |
| [`@dotdo/dosql`](./packages/dosql) | SQL database engine (server) | [![npm](https://img.shields.io/npm/v/@dotdo/dosql.svg)](https://www.npmjs.com/package/@dotdo/dosql) |
| [`@dotdo/dolake`](./packages/dolake) | Lakehouse worker (server) | [![npm](https://img.shields.io/npm/v/@dotdo/dolake.svg)](https://www.npmjs.com/package/@dotdo/dolake) |
| [`@dotdo/shared-types`](./packages/shared-types) | Shared TypeScript types | [![npm](https://img.shields.io/npm/v/@dotdo/shared-types.svg)](https://www.npmjs.com/package/@dotdo/shared-types) |

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
| `@dotdo/sql.do` | 7.2 KB | **2.9 KB** | Yes |
| `@dotdo/lake.do` | 6.1 KB | **2.4 KB** | Yes |
| `@dotdo/shared-types` | 3.0 KB | **1.3 KB** | Yes |
| `@dotdo/dosql` (core) | 164.6 KB | **49.6 KB** | Yes |
| `@dotdo/dolake` | 407.9 KB | **87.4 KB** | Yes |

### Comparison with Alternatives

| Solution | Client Bundle (gzip) | Notes |
|----------|---------------------|-------|
| **@dotdo/sql.do** | **2.9 KB** | Lightweight client, server does heavy lifting |
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
import { createSQLClient } from '@dotdo/sql.do';

// Types are compile-time only - zero runtime cost
import type { QueryResult, SQLClientConfig } from '@dotdo/sql.do';
```

**Tree-shaking tips:**

1. **Use named imports** - Avoid `import * as sql from '@dotdo/sql.do'`
2. **Import types separately** - Use `import type` for TypeScript types
3. **Check your bundler** - Ensure tree-shaking is enabled (default in Vite, esbuild, Rollup)
4. **Analyze your bundle** - Use `npx vite-bundle-visualizer` or similar tools

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
