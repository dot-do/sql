# Contributing to DoSQL

Thank you for your interest in contributing to DoSQL. This guide covers the development workflow, testing practices, and code standards for this project.

## Table of Contents

- [Getting Started](#getting-started)
- [Testing Guide](#testing-guide)
  - [TDD Red-Green-Refactor Pattern](#tdd-red-green-refactor-pattern)
  - [Using it.fails() for RED Phase](#using-itfails-for-red-phase)
  - [Test File Organization](#test-file-organization)
  - [Vitest Configuration](#vitest-configuration)
  - [Workers-Vitest-Pool for Cloudflare Tests](#workers-vitest-pool-for-cloudflare-tests)
  - [Good Test Patterns](#good-test-patterns)
  - [Running Tests Locally](#running-tests-locally)
- [Code Style](#code-style)
- [Pull Requests](#pull-requests)

## Getting Started

```bash
# Clone the repository
git clone <repository-url>
cd sql

# Install dependencies
pnpm install

# Build all packages
pnpm build

# Run tests
pnpm test
```

## Testing Guide

This project follows **Test-Driven Development (TDD)** with a **NO MOCKS** philosophy. Tests run in actual Cloudflare Workers environments via `workers-vitest-pool`, using real Durable Objects and real SQLite.

### TDD Red-Green-Refactor Pattern

We follow the classic TDD cycle:

1. **RED Phase**: Write a failing test that documents expected behavior
2. **GREEN Phase**: Write the minimum code to make the test pass
3. **REFACTOR Phase**: Improve the code while keeping tests green

#### Why TDD?

- Tests document expected behavior before implementation
- Forces thinking about API design upfront
- Provides immediate feedback during development
- Creates a safety net for refactoring
- Documents requirements as executable specifications

### Using it.fails() for RED Phase

Vitest provides `it.fails()` to mark tests that are **expected to fail**. This is invaluable for the RED phase of TDD.

#### When to Use it.fails()

- Documenting expected behavior that is NOT YET IMPLEMENTED
- Writing specification tests before implementation
- Creating a backlog of work as failing tests
- Capturing bug reports as tests

#### Example: RED Phase Tests

```typescript
/**
 * Branded Types Enforcement TDD Tests (RED PHASE)
 *
 * These tests document the EXPECTED behavior for branded type enforcement
 * that is NOT YET IMPLEMENTED.
 *
 * Using it.fails() to mark these as expected failures until implementation.
 */

import { describe, it, expect } from 'vitest';

describe('LSN Factory Enforcement', () => {
  describe('createLSN validation', () => {
    // RED PHASE: Documents expected behavior
    it.fails('should throw error for negative bigint values', () => {
      // EXPECTED: createLSN(-1n) should throw 'LSN cannot be negative: -1'
      // ACTUAL: Currently just casts without validation
      expect(() => createLSN(-1n)).toThrow('LSN cannot be negative');
    });

    // GREEN: This test passes - basic functionality works
    it('should accept valid non-negative bigint values', () => {
      const lsn = createLSN(0n);
      expect(lsn).toBe(0n);
    });
  });
});
```

#### Converting RED to GREEN

When implementing the feature:

1. Remove `it.fails()` and change to regular `it()`
2. Implement the functionality
3. Verify the test passes
4. Refactor if needed

```typescript
// Before (RED): Test expected to fail
it.fails('should throw error for negative bigint values', () => {
  expect(() => createLSN(-1n)).toThrow('LSN cannot be negative');
});

// After (GREEN): Test now passes
it('should throw error for negative bigint values', () => {
  expect(() => createLSN(-1n)).toThrow('LSN cannot be negative');
});
```

### Test File Organization

Tests are co-located with source files or placed in `__tests__` directories:

```
packages/dosql/
├── src/
│   ├── parser/
│   │   ├── dml.ts
│   │   ├── dml.test.ts        # Co-located unit test
│   │   └── dml-types.ts
│   ├── sharding/
│   │   ├── index.ts
│   │   └── sharding.test.ts   # Co-located feature test
│   └── __tests__/
│       ├── aggregates.test.ts # Integration tests
│       └── join.test.ts
├── e2e/
│   └── __tests__/
│       ├── lakehouse-e2e.test.ts  # End-to-end tests
│       └── prod-e2e.test.ts
└── vitest.config.ts
```

#### Test Naming Conventions

- `*.test.ts` - All test files
- `unit.test.ts` - Pure unit tests (rare, we prefer integration)
- `*-e2e.test.ts` - End-to-end tests against deployed services

### Vitest Configuration

#### Standard Package Configuration (Workers Pool)

For packages running in Cloudflare Workers environment:

```typescript
// vitest.config.ts
import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config';

export default defineWorkersConfig({
  test: {
    include: ['src/**/*.test.ts'],
    // Disable isolated storage to avoid known issues with DO storage cleanup
    // See: https://developers.cloudflare.com/workers/testing/vitest-integration/known-issues/#isolated-storage
    isolatedStorage: false,
    poolOptions: {
      workers: {
        wrangler: { configPath: './wrangler.jsonc' },
        singleWorker: true,
        miniflare: {
          durableObjects: {
            DOSQL_DB: 'DoSQLDatabase',
            TEST_BRANCH_DO: 'TestBranchDO',
          },
          r2Buckets: ['TEST_R2_BUCKET'],
          d1Databases: ['TEST_D1'],
        },
      },
    },
  },
});
```

#### E2E Tests Configuration (Node Environment)

For tests against deployed services:

```typescript
// e2e/vitest.config.ts
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node',
    include: ['__tests__/**/*.test.ts'],
    testTimeout: 120_000,  // Longer timeout for network operations
    hookTimeout: 60_000,
    sequence: {
      concurrent: false,  // Run sequentially
    },
    pool: 'forks',
    poolOptions: {
      forks: {
        singleFork: true,  // Avoid parallel interference
      },
    },
    retry: 1,  // Retry flaky network tests
  },
});
```

#### Simple Node Environment Configuration

For packages without Workers dependencies:

```typescript
// vitest.config.ts
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    include: ['src/**/*.test.ts'],
  },
});
```

### Workers-Vitest-Pool for Cloudflare Tests

The `@cloudflare/vitest-pool-workers` package runs tests inside actual Cloudflare Workers runtime, not a mock.

#### Key Features

- **Real Durable Objects**: Tests use actual DO implementation
- **Real SQLite**: Tests use actual D1/DO SQLite
- **Real R2**: Tests can interact with R2 buckets
- **Real KV**: Tests can use Workers KV

#### Configuration Options

```typescript
poolOptions: {
  workers: {
    // Path to wrangler config
    wrangler: { configPath: './wrangler.jsonc' },

    // Use single worker for test isolation
    singleWorker: true,

    // Disable isolated storage (recommended for DO tests)
    isolatedStorage: false,

    // Miniflare configuration
    miniflare: {
      durableObjects: {
        // Direct class binding
        MY_DO: 'MyDurableObject',

        // With SQLite enabled
        SQLITE_DO: { className: 'SqliteDO', useSQLite: true },
      },
      r2Buckets: ['BUCKET_NAME'],
      d1Databases: ['DB_NAME'],
      kvNamespaces: ['KV_NAME'],
    },
  },
},
```

#### Accessing Bindings in Tests

```typescript
import { env } from 'cloudflare:test';
import { describe, it, expect } from 'vitest';

describe('Durable Object Tests', () => {
  it('should access DO binding', async () => {
    const id = env.DOSQL_DB.idFromName('test');
    const stub = env.DOSQL_DB.get(id);
    const response = await stub.fetch('http://localhost/query', {
      method: 'POST',
      body: JSON.stringify({ sql: 'SELECT 1' }),
    });
    expect(response.ok).toBe(true);
  });
});
```

### Good Test Patterns

#### Pattern 1: Comprehensive Parser Tests

```typescript
describe('INSERT Statement Parsing', () => {
  describe('Basic INSERT', () => {
    it('should parse simple INSERT with VALUES', () => {
      const result = parseInsert(
        "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')"
      );

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.type).toBe('insert');
      expect(result.statement.table).toBe('users');
      expect(result.statement.columns).toEqual(['name', 'email']);
    });

    it('should parse INSERT with NULL values', () => {
      const result = parseInsert("INSERT INTO users (name, email) VALUES ('Alice', NULL)");

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      if (result.statement.source.type === 'values_list') {
        const secondValue = result.statement.source.rows[0].values[1];
        expect(secondValue.type).toBe('null');
      }
    });
  });

  describe('Error Handling', () => {
    it('should report error for missing INTO', () => {
      const result = parseInsert('INSERT users (name) VALUES (\'test\')');
      expect(isParseError(result)).toBe(true);
      if (isParseError(result)) {
        expect(result.error).toContain('INTO');
      }
    });
  });
});
```

#### Pattern 2: Distribution Tests with Statistics

```typescript
describe('HashVindex', () => {
  it('distributes keys across all shards', () => {
    const vindex = new HashVindex(shards);

    const distribution = testDistribution(
      vindex,
      () => `user-${Math.random()}`,
      10000
    );

    // Check all shards received data
    for (const shardId of vindex.getAllShards()) {
      expect(distribution.get(shardId)).toBeGreaterThan(0);
    }

    // Check distribution is reasonably uniform
    const stats = distributionStats(distribution);
    expect(stats.stdDev / stats.mean).toBeLessThan(0.1); // CV < 10%
  });
});
```

#### Pattern 3: Async Tests with Real Resources

```typescript
describe('Scatter-gather execution', () => {
  it('executes query across all shards and merges results', async () => {
    const rpc = new MockShardRPC();
    rpc.setShardData('shard-1', ['id', 'name'], [[1, 'Alice'], [2, 'Bob']]);
    rpc.setShardData('shard-2', ['id', 'name'], [[3, 'Charlie'], [4, 'Diana']]);

    const selector = createReplicaSelector(shards);
    const executor = createExecutor(rpc, selector);
    const router = createRouter(vschema);

    const plan = router.createExecutionPlan('SELECT * FROM users');
    const result = await executor.execute(plan);

    expect(result.rows).toHaveLength(4);
    expect(result.contributingShards).toHaveLength(2);
  });
});
```

#### Pattern 4: Type-Level Tests

```typescript
describe('Type-Level Query Detection', () => {
  it('type definitions compile correctly', () => {
    // This test verifies the types compile - if it runs, types are correct
    type TestSchema = {
      users: {
        columns: { id: 'number'; tenant_id: 'number'; name: 'string' };
        shardKey: 'tenant_id';
      };
    };

    // Verify type inference works
    type QueryType = DetectQueryType<
      'SELECT * FROM users WHERE tenant_id = 1',
      TestSchema,
      'users'
    >;

    const _t: QueryType = 'single-shard';
    expect(true).toBe(true);
  });
});
```

### Running Tests Locally

#### Run All Tests

```bash
pnpm test
```

#### Run Specific Package Tests

```bash
pnpm --filter @dotdo/dosql test
pnpm --filter @dotdo/dolake test
```

#### Run Single Test File

```bash
cd packages/dosql
npx vitest run src/parser/dml.test.ts
```

#### Run Tests in Watch Mode (Development)

```bash
cd packages/dosql
npx vitest
```

**Warning**: Vitest/Vite consume significant memory. Guidelines:

1. Never run multiple vitest instances in parallel
2. Use `npx vitest run` (not watch mode) for CI
3. Kill orphans if needed: `pkill -9 -f vitest; pkill -9 -f vite`
4. For subagents: Run ONE test file at a time

#### Run E2E Tests

```bash
cd packages/dosql/e2e
npx vitest run
```

E2E tests require deployed services. Set environment variables:

```bash
export E2E_ENDPOINT="https://your-worker.workers.dev"
export E2E_AUTH_TOKEN="your-token"
```

## Code Style

- TypeScript strict mode
- Branded types for IDs (TransactionId, LSN, ShardId)
- Prefer `unknown` over `any`
- No unnecessary abstractions
- Prefer integration tests over unit tests

## Pull Requests

1. Write failing tests first (RED phase)
2. Implement feature to make tests pass (GREEN phase)
3. Refactor as needed (REFACTOR phase)
4. Ensure all tests pass: `pnpm test`
5. Create PR with clear description

### PR Checklist

- [ ] Tests written using TDD approach
- [ ] All tests pass
- [ ] No console.log statements left behind
- [ ] Types are properly exported
- [ ] Documentation updated if API changed
