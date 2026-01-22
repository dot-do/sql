# @dotdo/sql

[![Build Status](https://github.com/dot-do/sql/actions/workflows/ci.yml/badge.svg)](https://github.com/dot-do/sql/actions/workflows/ci.yml)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.3+-blue.svg)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![npm @dotdo/sql.do](https://img.shields.io/npm/v/@dotdo/sql.do.svg?label=@dotdo/sql.do)](https://www.npmjs.com/package/@dotdo/sql.do)
[![npm @dotdo/dosql](https://img.shields.io/npm/v/@dotdo/dosql.svg?label=@dotdo/dosql)](https://www.npmjs.com/package/@dotdo/dosql)
[![npm @dotdo/dolake](https://img.shields.io/npm/v/@dotdo/dolake.svg?label=@dotdo/dolake)](https://www.npmjs.com/package/@dotdo/dolake)

SQL database for Cloudflare Workers - DoSQL and DoLake.

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
