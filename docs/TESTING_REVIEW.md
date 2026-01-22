# Testing Review: @dotdo/sql Monorepo

**Review Date**: 2026-01-22
**Reviewer**: Automated Testing Review
**Repository**: @dotdo/sql (DoSQL & DoLake)

## Executive Summary

The @dotdo/sql monorepo is a new project with the `packages/` directory currently empty. No test files exist yet. This review establishes the testing strategy and requirements for DoSQL (SQL database engine) and DoLake (lakehouse for CDC/analytics) following the project's NO MOCKS philosophy.

---

## Current State Analysis

### Repository Structure

```
/Users/nathanclevenger/projects/sql/
├── packages/           # EMPTY - no packages implemented yet
├── CLAUDE.md           # Project instructions and testing philosophy
├── README.md           # Project overview
├── package.json        # Monorepo root configuration
└── pnpm-workspace.yaml # Workspace definition
```

### Test Files Found

| Category | Count |
|----------|-------|
| `*.test.ts` files | 0 |
| `*.spec.ts` files | 0 |
| `vitest.config.ts` files | 0 |
| `__tests__/` directories | 0 |

### Current Coverage

**No tests exist** - the repository is in initial setup phase.

---

## Testing Philosophy (from CLAUDE.md)

The project mandates **TDD with NO MOCKS**:

> - Tests run in actual Cloudflare Workers environment via `workers-vitest-pool`
> - Use real Durable Objects, not mocks
> - Use real SQLite, not mocks
> - Integration tests over unit tests

This is a significant architectural decision that prioritizes:
1. **Production fidelity** - Tests run in the actual runtime environment
2. **Real behavior** - No mocking of Durable Objects or SQLite
3. **Integration focus** - Prefer testing complete flows over isolated units

---

## Recommended Testing Strategy

### Test Framework: workers-vitest-pool

Each package should use Cloudflare's `@cloudflare/vitest-pool-workers` for testing in the actual Workers runtime.

#### vitest.config.ts Template

```typescript
import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config';

export default defineWorkersConfig({
  test: {
    poolOptions: {
      workers: {
        wrangler: { configPath: './wrangler.jsonc' },
        miniflare: {
          durableObjects: {
            // Define DO bindings here
          },
        },
      },
    },
  },
});
```

### Test Organization by Package

#### @dotdo/dosql

```
packages/dosql/
├── src/
│   ├── parser/
│   │   └── __tests__/
│   │       ├── select.test.ts
│   │       ├── insert.test.ts
│   │       ├── update.test.ts
│   │       ├── delete.test.ts
│   │       ├── ddl.test.ts
│   │       ├── joins.test.ts
│   │       ├── aggregates.test.ts
│   │       ├── window-functions.test.ts
│   │       └── cte.test.ts
│   ├── planner/
│   │   └── __tests__/
│   │       ├── query-plan.test.ts
│   │       └── optimizer.test.ts
│   ├── engine/
│   │   └── __tests__/
│   │       ├── execution.test.ts
│   │       ├── transactions.test.ts
│   │       └── isolation.test.ts
│   ├── sharding/
│   │   └── __tests__/
│   │       ├── router.test.ts
│   │       ├── query-rewrite.test.ts
│   │       └── scatter-gather.test.ts
│   ├── btree/
│   │   └── __tests__/
│   │       ├── insert.test.ts
│   │       ├── search.test.ts
│   │       ├── delete.test.ts
│   │       └── range-scan.test.ts
│   ├── fsx/
│   │   └── __tests__/
│   │       ├── memory-fs.test.ts
│   │       ├── do-storage-fs.test.ts
│   │       └── r2-fs.test.ts
│   ├── wal/
│   │   └── __tests__/
│   │       ├── write.test.ts
│   │       ├── recovery.test.ts
│   │       └── checkpoint.test.ts
│   ├── transaction/
│   │   └── __tests__/
│   │       ├── begin-commit.test.ts
│   │       ├── rollback.test.ts
│   │       ├── isolation-levels.test.ts
│   │       └── deadlock.test.ts
│   └── database/
│       └── __tests__/
│           ├── create-table.test.ts
│           ├── locks.test.ts
│           └── concurrent.test.ts
├── tests/
│   └── integration/
│       ├── e2e-query.test.ts
│       ├── e2e-transactions.test.ts
│       ├── e2e-sharding.test.ts
│       └── e2e-recovery.test.ts
└── vitest.config.ts
```

#### @dotdo/dolake

```
packages/dolake/
├── src/
│   ├── __tests__/
│   │   ├── dolake.test.ts
│   │   ├── compaction.test.ts
│   │   ├── partitioning.test.ts
│   │   ├── query-engine.test.ts
│   │   ├── rate-limiter.test.ts
│   │   └── schemas.test.ts
├── tests/
│   └── integration/
│       ├── cdc-ingest.test.ts
│       ├── parquet-write.test.ts
│       ├── compaction-flow.test.ts
│       └── time-travel.test.ts
└── vitest.config.ts
```

---

## Test Patterns

### Pattern 1: Real Durable Object Tests

```typescript
// packages/dosql/src/database/__tests__/create-table.test.ts
import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect } from 'vitest';
import { DoSQLDatabase } from '../database';

describe('CREATE TABLE', () => {
  it('creates a table with columns', async () => {
    const result = await runInDurableObject(
      env.DOSQL_DATABASE.get(env.DOSQL_DATABASE.idFromName('test-db')),
      async (stub, state) => {
        const db = new DoSQLDatabase(state);
        return db.execute(`
          CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE
          )
        `);
      }
    );

    expect(result.success).toBe(true);
  });

  it('fails on duplicate table name', async () => {
    await runInDurableObject(
      env.DOSQL_DATABASE.get(env.DOSQL_DATABASE.idFromName('test-db-2')),
      async (stub, state) => {
        const db = new DoSQLDatabase(state);
        await db.execute('CREATE TABLE users (id INTEGER)');

        await expect(
          db.execute('CREATE TABLE users (id INTEGER)')
        ).rejects.toThrow(/already exists/);
      }
    );
  });
});
```

### Pattern 2: Real SQLite Tests

```typescript
// packages/dosql/src/engine/__tests__/execution.test.ts
import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect } from 'vitest';

describe('Query Execution', () => {
  it('executes SELECT with WHERE clause', async () => {
    await runInDurableObject(
      env.DOSQL_DATABASE.get(env.DOSQL_DATABASE.idFromName('query-test')),
      async (stub, state) => {
        const db = state.storage.sql;

        // Setup
        db.exec(`
          CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL);
          INSERT INTO products VALUES (1, 'Widget', 9.99);
          INSERT INTO products VALUES (2, 'Gadget', 19.99);
          INSERT INTO products VALUES (3, 'Gizmo', 14.99);
        `);

        // Test
        const results = db.exec(
          'SELECT * FROM products WHERE price > 10 ORDER BY price'
        ).toArray();

        expect(results).toEqual([
          { id: 3, name: 'Gizmo', price: 14.99 },
          { id: 2, name: 'Gadget', price: 19.99 },
        ]);
      }
    );
  });
});
```

### Pattern 3: CDC Integration Tests

```typescript
// packages/dolake/tests/integration/cdc-ingest.test.ts
import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect } from 'vitest';
import { DoLake } from '../../src/dolake';

describe('CDC Ingest', () => {
  it('captures INSERT events', async () => {
    await runInDurableObject(
      env.DOLAKE.get(env.DOLAKE.idFromName('cdc-test')),
      async (stub, state) => {
        const lake = new DoLake(state, env);

        // Simulate CDC event from DoSQL
        await lake.ingestCDCEvent({
          type: 'INSERT',
          table: 'users',
          data: { id: 1, name: 'Alice', email: 'alice@example.com' },
          timestamp: Date.now(),
          lsn: '0000000001',
        });

        const events = await lake.queryEvents('users', { limit: 10 });
        expect(events).toHaveLength(1);
        expect(events[0].type).toBe('INSERT');
      }
    );
  });
});
```

### Pattern 4: Transaction Isolation Tests

```typescript
// packages/dosql/src/transaction/__tests__/isolation-levels.test.ts
import { env, runInDurableObject } from 'cloudflare:test';
import { describe, it, expect } from 'vitest';

describe('Transaction Isolation', () => {
  it('SERIALIZABLE prevents phantom reads', async () => {
    await runInDurableObject(
      env.DOSQL_DATABASE.get(env.DOSQL_DATABASE.idFromName('isolation-test')),
      async (stub, state) => {
        const db = state.storage.sql;

        db.exec('CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)');
        db.exec('INSERT INTO accounts VALUES (1, 100)');

        // Start transaction 1
        const tx1 = db.exec('BEGIN TRANSACTION');
        const snapshot1 = db.exec('SELECT SUM(balance) as total FROM accounts').one();

        // Transaction 2 inserts a new row
        db.exec('INSERT INTO accounts VALUES (2, 200)');

        // Transaction 1 should still see the same total (no phantom)
        const snapshot2 = db.exec('SELECT SUM(balance) as total FROM accounts').one();

        // In SERIALIZABLE, both should be 100
        expect(snapshot1.total).toBe(100);
        expect(snapshot2.total).toBe(100);

        db.exec('COMMIT');
      }
    );
  });
});
```

---

## TDD Red-Green-Refactor Process

### Phase 1: Red (Write Failing Tests)

Before implementing any feature:

1. Create test file with descriptive test cases
2. Write tests that describe expected behavior
3. Run tests - they should FAIL (red)
4. Commit failing tests with message: `test(dosql): add failing tests for X`

### Phase 2: Green (Make Tests Pass)

Implement the minimum code to pass tests:

1. Write implementation code
2. Run tests until they PASS (green)
3. Commit with message: `feat(dosql): implement X`

### Phase 3: Refactor (Improve Code)

Clean up while maintaining green tests:

1. Refactor for clarity, performance, maintainability
2. Run tests after each change - must stay GREEN
3. Commit with message: `refactor(dosql): improve X`

---

## Anti-Patterns to Avoid

### 1. NO MOCKS

```typescript
// BAD - Do not mock Durable Objects
const mockState = { storage: { sql: { exec: vi.fn() } } };
const db = new DoSQLDatabase(mockState);

// GOOD - Use real Durable Object state
await runInDurableObject(env.DOSQL_DATABASE.get(id), async (stub, state) => {
  const db = new DoSQLDatabase(state);
  // ...
});
```

### 2. NO vi.mock() for Cloudflare APIs

```typescript
// BAD - Do not mock R2
vi.mock('@cloudflare/workers-types', () => ({
  R2Bucket: { put: vi.fn(), get: vi.fn() }
}));

// GOOD - Use real R2 in miniflare
await runInDurableObject(env.DOLAKE.get(id), async (stub, state) => {
  const bucket = env.LAKE_BUCKET;
  await bucket.put('test-key', 'test-data');
});
```

### 3. NO Fake Timers for Real Async

```typescript
// BAD - Fake timers break real async
vi.useFakeTimers();
await lake.scheduleCompaction();
vi.advanceTimersByTime(60000);

// GOOD - Test real behavior with reasonable timeouts
await lake.scheduleCompaction();
await vi.waitFor(() => {
  expect(lake.compactionStatus).toBe('completed');
}, { timeout: 10000 });
```

---

## Test Categories

### Unit Tests (Parser, Planner)

- Pure functions without side effects
- Still NO MOCKS - test actual parsing/planning logic
- Fast execution, no I/O

```typescript
// Pure function test - no mocking needed
describe('SQL Parser', () => {
  it('parses SELECT with JOIN', () => {
    const ast = parse('SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id');
    expect(ast.type).toBe('SELECT');
    expect(ast.joins).toHaveLength(1);
    expect(ast.joins[0].type).toBe('INNER');
  });
});
```

### Integration Tests (Engine, Sharding)

- Test component interactions
- Use real Durable Objects
- Test with actual SQLite

### E2E Tests (Complete Flows)

- Full request/response cycles
- Multi-DO communication
- CDC event flow from DoSQL to DoLake

---

## Coverage Requirements

### Minimum Coverage Targets

| Package | Line | Branch | Function |
|---------|------|--------|----------|
| @dotdo/dosql | 80% | 75% | 85% |
| @dotdo/dolake | 80% | 75% | 85% |

### Critical Path Coverage (100%)

These paths MUST have 100% coverage:

1. **Transaction commit/rollback**
2. **WAL write and recovery**
3. **SQL injection prevention**
4. **Data integrity constraints**
5. **Shard routing logic**
6. **CDC event serialization**

---

## CI/CD Integration

### GitHub Actions Workflow

```yaml
# .github/workflows/test.yml
name: Tests

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: pnpm/action-setup@v4
        with:
          version: 9
      - uses: actions/setup-node@v4
        with:
          node-version: 20
          cache: 'pnpm'

      - run: pnpm install
      - run: pnpm build
      - run: pnpm test -- --coverage

      - name: Upload coverage
        uses: codecov/codecov-action@v4
        with:
          fail_ci_if_error: true
```

---

## Test Utilities

### Recommended Helper Functions

```typescript
// packages/test-utils/src/index.ts
import { env, runInDurableObject } from 'cloudflare:test';

export async function withTestDatabase<T>(
  name: string,
  fn: (db: DurableObjectState['storage']['sql']) => Promise<T>
): Promise<T> {
  const id = env.DOSQL_DATABASE.idFromName(`test-${name}-${Date.now()}`);
  return runInDurableObject(env.DOSQL_DATABASE.get(id), async (stub, state) => {
    return fn(state.storage.sql);
  });
}

export async function seedTestData(
  db: DurableObjectState['storage']['sql'],
  schema: string,
  rows: Record<string, unknown>[]
): Promise<void> {
  db.exec(schema);
  for (const row of rows) {
    // Insert row...
  }
}

export function generateTestId(): string {
  return `test-${Date.now()}-${Math.random().toString(36).slice(2)}`;
}
```

---

## Review Checklist

Before merging any PR:

- [ ] All tests pass (`pnpm test`)
- [ ] No mocks used (grep for `vi.mock`, `vi.fn()`)
- [ ] Coverage thresholds met
- [ ] New features have tests (TDD)
- [ ] Integration tests for cross-component changes
- [ ] E2E tests for user-facing features

---

## Recommendations

### Immediate Actions

1. **Create package scaffolding** with vitest configuration
2. **Implement test utilities** package for common patterns
3. **Set up CI/CD** with test coverage reporting
4. **Document TDD workflow** in contributing guidelines

### Long-term Improvements

1. **Property-based testing** for SQL parser (fast-check)
2. **Chaos testing** for transaction recovery
3. **Load testing** for sharding performance
4. **Snapshot testing** for query plans

---

## Conclusion

The @dotdo/sql monorepo is in the initial setup phase with no tests yet. The testing philosophy is well-defined in CLAUDE.md: **TDD with NO MOCKS using workers-vitest-pool**. This review establishes the testing strategy, patterns, and requirements that should be followed as the codebase is developed.

Key principles:
- Use real Cloudflare Workers runtime (workers-vitest-pool)
- No mocking of Durable Objects or SQLite
- Prefer integration tests over isolated unit tests
- Follow TDD Red-Green-Refactor cycle
- Maintain high coverage on critical paths
