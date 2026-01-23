# TypeScript Review: DoSQL and DoLake Packages

**Review Date:** 2026-01-22
**Reviewer:** Claude Code (Opus 4.5)
**Packages Reviewed:**
- `packages/dosql/` - TypeScript-native SQLite rewrite for Cloudflare Workers
- `packages/dolake/` - Companion lakehouse for CDC streaming to Iceberg/Parquet on R2

---

## 1. Executive Summary

### Overall Assessment: **Strong** (8.5/10)

Both packages demonstrate excellent TypeScript practices with room for targeted improvements. The codebase exhibits:

**Strengths:**
- Comprehensive branded type system for domain primitives (LSN, TransactionId, ShardId, PageId)
- Well-structured discriminated unions for query plans and state machines
- Consistent error handling with typed error classes and error code enums
- Strong use of generics with proper constraints
- Strict mode enabled across both packages
- Template literal types for compile-time SQL analysis
- Conditional types for PRAGMA result mapping

**Areas for Improvement:**
- 200+ instances of `as any` type assertions (primarily in tests and type-unsafe integrations)
- Missing `noUncheckedIndexedAccess` compiler option
- `AggregateOptions` interface uses `any` for accumulator type
- Some circular import workarounds using `any`

**Risk Assessment:** Low - The `any` usage is largely contained to test files and integration boundaries where external APIs lack proper types.

---

## 2. TypeScript Configuration Analysis

### DoSQL (`packages/dosql/tsconfig.json`)

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "declaration": true,
    "declarationMap": true,
    "sourceMap": true,
    "outDir": "./dist",
    "rootDir": "./src",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true
  }
}
```

**Enabled Strict Checks:**
- `strict: true` (enables all strict type-checking options)
- `forceConsistentCasingInFileNames: true`

**Recommended Additions:**
| Option | Current | Recommended | Impact |
|--------|---------|-------------|--------|
| `noUncheckedIndexedAccess` | Not set | `true` | Prevents undefined access on arrays/objects |
| `exactOptionalPropertyTypes` | Not set | `true` | Stricter optional property handling |
| `noImplicitReturns` | Not set | `true` | Catches missing return statements |
| `noFallthroughCasesInSwitch` | Not set | `true` | Prevents switch fallthrough bugs |

### DoLake (`packages/dolake/tsconfig.json`)

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "lib": ["ES2022"],
    "strict": true,
    "types": ["@cloudflare/workers-types"]
  }
}
```

**Notes:**
- Uses `bundler` module resolution (appropriate for Workers)
- Includes Cloudflare Workers types
- Missing same additional strict options as DoSQL

---

## 3. Type Safety Assessment

### 3.1 Branded Types Implementation

**Location:** `/Users/nathanclevenger/projects/pocs/packages/dosql/src/engine/types.ts`

The codebase implements branded types using the recommended `unique symbol` pattern:

```typescript
/** Brand symbol for Log Sequence Number */
declare const LSNBrand: unique symbol;
export type LSN = bigint & { readonly [LSNBrand]: never };

/** Factory function with validation */
export function createLSN(value: bigint): LSN {
  if (value < 0n) {
    throw new Error(`LSN cannot be negative: ${value}`);
  }
  return value as LSN;
}
```

**Branded Types Implemented:**
| Type | Underlying | Purpose |
|------|-----------|---------|
| `LSN` | `bigint` | Log Sequence Numbers for WAL |
| `TransactionId` | `string` | Unique transaction identifiers |
| `ShardId` | `string` | Shard identifiers for sharding |
| `PageId` | `number` | B-tree page identifiers |

**Assessment:** Excellent implementation. Factory functions provide runtime validation while branded types provide compile-time safety.

### 3.2 Discriminated Unions

**Location:** `/Users/nathanclevenger/projects/pocs/packages/dosql/src/engine/types.ts`

Query plans use proper discriminated unions:

```typescript
export type QueryPlan =
  | ScanPlan
  | FilterPlan
  | ProjectPlan
  | JoinPlan
  | AggregationPlan
  | SortPlan
  | LimitPlan
  | UnionPlan
  | IntersectPlan
  | ExceptPlan
  | CTEPlan
  | CTEScanPlan;

export interface ScanPlan {
  type: 'scan';
  table: string;
  alias?: string;
}

export interface FilterPlan {
  type: 'filter';
  input: QueryPlan;
  predicate: Expression;
}
```

**State Machines:**

```typescript
// Transaction states
export enum TransactionState {
  NONE = 'NONE',
  ACTIVE = 'ACTIVE',
  SAVEPOINT = 'SAVEPOINT',
}

// Transaction modes
export enum TransactionMode {
  DEFERRED = 'DEFERRED',
  IMMEDIATE = 'IMMEDIATE',
  EXCLUSIVE = 'EXCLUSIVE',
}
```

**Assessment:** Well-structured discriminated unions enable exhaustive pattern matching and type narrowing.

### 3.3 Conditional Types

**Location:** `/Users/nathanclevenger/projects/pocs/packages/dosql/src/statement/types.ts`

PRAGMA results use conditional types for type-safe returns:

```typescript
export type PragmaResult<N extends PragmaName> =
  N extends 'journal_mode' ? string :
  N extends 'synchronous' ? number :
  N extends 'foreign_keys' ? number :
  N extends 'cache_size' ? number :
  N extends 'table_info' ? TableInfoRow[] :
  N extends 'index_list' ? IndexListRow[] :
  N extends 'database_list' ? DatabaseListRow[] :
  N extends 'compile_options' ? string[] :
  unknown;
```

**Assessment:** Good use of conditional types for API ergonomics. The `unknown` fallback is appropriate for unknown pragmas.

### 3.4 Template Literal Types

**Location:** `/Users/nathanclevenger/projects/pocs/packages/dosql/src/sharding/types.ts`

Compile-time query analysis using template literals:

```typescript
export type HasEqualityOnColumn<
  SQL extends string,
  Column extends string
> = Uppercase<SQL> extends `${string}WHERE${string}${Uppercase<Column>}${string}=${string}`
  ? true
  : false;

export type DetectQueryType<
  SQL extends string,
  Schema extends TypedDatabaseSchema,
  Table extends keyof Schema
> = Schema[Table]['shardKey'] extends string
  ? HasEqualityOnColumn<SQL, Schema[Table]['shardKey']> extends true
    ? 'single-shard'
    : 'scatter'
  : 'scatter';
```

**Assessment:** Innovative use of template literals for compile-time query routing detection. This enables zero-runtime-cost query classification.

---

## 4. Pattern Usage Analysis

### 4.1 Error Handling Pattern

The codebase consistently implements custom error classes with typed error codes:

```typescript
// Example from transaction/types.ts
export enum TransactionErrorCode {
  NO_ACTIVE_TRANSACTION = 'TXN_NO_ACTIVE',
  TRANSACTION_ALREADY_ACTIVE = 'TXN_ALREADY_ACTIVE',
  SAVEPOINT_NOT_FOUND = 'TXN_SAVEPOINT_NOT_FOUND',
  DEADLOCK = 'TXN_DEADLOCK',
  // ...
}

export class TransactionError extends Error {
  constructor(
    public readonly code: TransactionErrorCode,
    message: string,
    public readonly txnId?: TransactionId,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = 'TransactionError';
  }
}
```

**Error Classes Found (25 total):**
- `TransactionError`, `CDCError`, `BranchError`, `ConstraintError`
- `RetentionError`, `WALError`, `TriggerError`, `LakehouseError`
- `BindingError`, `StatementError`, `CompactionError`, `ShardExecutionError`
- `SyntaxError` (parser), `COWError`, `FSXError`, `DatabaseError`
- `ViewError`, `ReplicationError`, `TimeTravelError`, `DoSQLKyselyError`
- `CollationNotFoundError`, `CollationExistsError`, `BuiltinCollationError`
- `ReadOnlyError`

**Assessment:** Excellent error handling architecture with domain-specific errors and typed error codes.

### 4.2 Generic Constraints

**Location:** `/Users/nathanclevenger/projects/pocs/packages/dosql/src/statement/types.ts`

```typescript
export interface Statement<T = unknown, P extends BindParameters = BindParameters> {
  bind(...params: P extends any[] ? P : [P]): this;
  run(...params: P extends any[] ? P : [P]): RunResult;
  get(...params: P extends any[] ? P : [P]): T | undefined;
  all(...params: P extends any[] ? P : [P]): T[];
  iterate(...params: P extends any[] ? P : [P]): IterableIterator<T>;
}
```

**Location:** `/Users/nathanclevenger/projects/pocs/packages/dosql/src/sharding/types.ts`

```typescript
export interface VSchema<
  Tables extends Record<string, TableShardingConfig> = Record<string, TableShardingConfig>
> {
  tables: Tables;
  shards: ShardConfig[];
  defaultShard?: ShardId;
  settings?: VSchemaSettings;
}
```

**Assessment:** Generics are well-constrained with sensible defaults. The `Statement` interface uses conditional types for parameter spreading.

### 4.3 Type Guards

**Location:** `/Users/nathanclevenger/projects/pocs/packages/dosql/src/engine/types.ts`

```typescript
export function isLSN(value: unknown): value is LSN {
  return typeof value === 'bigint' && value >= 0n;
}

export function isTransactionId(value: unknown): value is TransactionId {
  return typeof value === 'string' && value.length > 0;
}

export function isShardId(value: unknown): value is ShardId {
  return typeof value === 'string' && value.length > 0;
}
```

**Assessment:** Type guards are implemented for all branded types, enabling safe runtime narrowing.

### 4.4 Readonly and Immutability

```typescript
// ByteRange tuple
export type ByteRange = readonly [start: number, end: number];

// Statement properties
export interface Statement<T, P> {
  readonly source: string;
  readonly reader: boolean;
  readonly finalized: boolean;
}

// Branded type pattern
export type LSN = bigint & { readonly [LSNBrand]: never };
```

**Assessment:** Good use of `readonly` for immutable data structures and branded type implementation.

---

## 5. Issues Found

### 5.1 Critical: `any` Type Usage in Public API

**File:** `/Users/nathanclevenger/projects/pocs/packages/dosql/src/statement/types.ts`
**Lines:** 557, 562, 567, 572

```typescript
// Current (ISSUE)
export interface AggregateOptions {
  step: (accumulator: any, ...values: SqlValue[]) => void;
  result: (accumulator: any) => SqlValue;
  start?: any;
  inverse?: (accumulator: any, ...values: SqlValue[]) => void;
}
```

**Suggested Improvement:**
```typescript
// Improved with generic accumulator type
export interface AggregateOptions<TAccumulator = unknown> {
  step: (accumulator: TAccumulator, ...values: SqlValue[]) => void;
  result: (accumulator: TAccumulator) => SqlValue;
  start?: TAccumulator;
  inverse?: (accumulator: TAccumulator, ...values: SqlValue[]) => void;
}
```

### 5.2 High: Transaction Function Type

**File:** `/Users/nathanclevenger/projects/pocs/packages/dosql/src/statement/types.ts`
**Lines:** 274, 482

```typescript
// Current (ISSUE)
export interface TransactionFunction<F extends (...args: any[]) => any> {
  (...args: Parameters<F>): ReturnType<F>;
}

transaction<F extends (...args: any[]) => any>(fn: F): TransactionFunction<F>;
```

**Suggested Improvement:**
```typescript
// Improved with explicit parameter types
export interface TransactionFunction<
  TArgs extends unknown[],
  TReturn
> {
  (...args: TArgs): TReturn;
  deferred(...args: TArgs): TReturn;
  immediate(...args: TArgs): TReturn;
  exclusive(...args: TArgs): TReturn;
}

transaction<TArgs extends unknown[], TReturn>(
  fn: (...args: TArgs) => TReturn
): TransactionFunction<TArgs, TReturn>;
```

### 5.3 Medium: Circular Import Workarounds

**File:** `/Users/nathanclevenger/projects/pocs/packages/dosql/src/engine/operators/window.ts`
**Line:** 69

```typescript
// Current (ISSUE)
export interface WindowPlan {
  type: 'window';
  input: any; // QueryPlan (avoid circular import)
  windowFunctions: WindowFunctionSpec[];
}
```

**Suggested Improvement:**
```typescript
// Option 1: Import type-only
import type { QueryPlan } from '../types.js';

export interface WindowPlan {
  type: 'window';
  input: QueryPlan;
  windowFunctions: WindowFunctionSpec[];
}

// Option 2: Separate base types file
// src/engine/base-types.ts - no imports
// src/engine/types.ts - imports base-types
```

### 5.4 Medium: Type Guards Using `any`

**File:** `/Users/nathanclevenger/projects/pocs/packages/dosql/src/engine/operators/cte.ts`
**Lines:** 410, 417

```typescript
// Current (ISSUE)
export function isCTEPlan(plan: any): plan is CTEPlan {
  return plan?.type === 'cte';
}

export function isCTEScanPlan(plan: any): plan is CTEScanPlan {
  return plan?.type === 'cteScan';
}
```

**Suggested Improvement:**
```typescript
// Improved with unknown and proper narrowing
export function isCTEPlan(plan: unknown): plan is CTEPlan {
  return (
    typeof plan === 'object' &&
    plan !== null &&
    'type' in plan &&
    (plan as { type: unknown }).type === 'cte'
  );
}
```

### 5.5 Low: Test Files with Excessive `as any`

**Summary:** 150+ instances of `as any` in test files

**Common Patterns:**
1. Accessing private/internal properties: `(writer as any).bufferRow(row)`
2. Mock implementations: `mockBucket as any`
3. Type narrowing shortcuts: `result.where as any`

**Recommendation:**
- Create test utility types for accessing internal properties
- Use proper mock typing with `jest.Mocked<T>` or Vitest equivalents
- Add proper type assertions instead of `as any`

### 5.6 Low: Planner/Optimizer Type Assertions

**File:** `/Users/nathanclevenger/projects/pocs/packages/dosql/src/planner/optimizer.ts`
**Lines:** 280, 470, 472, 494, 496, 523, 525, 552, 554

```typescript
// Current (ISSUE) - 9 instances
return this.optimizeScan(scanWithPredicate as any, alternatives, indexesConsidered);

// Pattern throughout:
} as any),
} as any;
```

**Root Cause:** Query plan type transformations during optimization require intermediate states that don't match final types.

**Suggested Improvement:**
```typescript
// Create intermediate plan types
interface PartialScanPlan extends Partial<ScanPlan> {
  type: 'scan';
  table: string;
}

// Use type assertions with comments
return this.optimizeScan(
  scanWithPredicate as ScanPlan, // Safe: predicate merged
  alternatives,
  indexesConsidered
);
```

---

## 6. Recommended Type Improvements

### 6.1 Enable Additional Compiler Options

Add to both `tsconfig.json` files:

```json
{
  "compilerOptions": {
    "noUncheckedIndexedAccess": true,
    "exactOptionalPropertyTypes": true,
    "noImplicitReturns": true,
    "noFallthroughCasesInSwitch": true
  }
}
```

**Impact:**
- `noUncheckedIndexedAccess`: Prevents undefined array/object access (breaking change, ~50 fixes needed)
- `exactOptionalPropertyTypes`: Stricter handling of `undefined` vs optional (minimal impact)
- `noImplicitReturns`: Catches missing returns (minimal impact)
- `noFallthroughCasesInSwitch`: Prevents switch bugs (minimal impact)

### 6.2 Generic Accumulator for Aggregates

```typescript
// src/statement/types.ts
export interface AggregateOptions<TAccumulator = unknown> {
  step: (accumulator: TAccumulator, ...values: SqlValue[]) => void;
  result: (accumulator: TAccumulator) => SqlValue;
  start?: TAccumulator;
  inverse?: (accumulator: TAccumulator, ...values: SqlValue[]) => void;
}

// Usage
db.aggregate<{ sum: number; count: number }>('avg_custom', {
  start: { sum: 0, count: 0 },
  step: (acc, value) => {
    acc.sum += value as number;
    acc.count++;
  },
  result: (acc) => acc.count > 0 ? acc.sum / acc.count : null,
});
```

### 6.3 Test Utility Types

```typescript
// src/__tests__/test-utils.ts

/**
 * Expose private members for testing
 */
export type Expose<T> = {
  [K in keyof T]: T[K];
} & Record<string, unknown>;

/**
 * Create strongly-typed test accessor
 */
export function testAccess<T>(obj: T): Expose<T> {
  return obj as Expose<T>;
}

// Usage in tests
const exposed = testAccess(writer);
exposed.bufferRow(row); // Now typed as unknown function
```

### 6.4 Query Plan Builder Types

```typescript
// src/engine/plan-builder.ts

interface PlanBuilder<T extends QueryPlan> {
  with<P extends Partial<T>>(additions: P): PlanBuilder<T & P>;
  build(): T;
}

function scan(table: string): PlanBuilder<ScanPlan> {
  return {
    with: (additions) => ({ ...baseBuilder, ...additions }),
    build: () => ({ type: 'scan', table }),
  };
}

// Usage in optimizer
const optimizedPlan = scan('users')
  .with({ predicate: extractedPredicate })
  .with({ indexHint: 'idx_users_email' })
  .build();
```

---

## 7. Priority Fixes

### P0 - Critical (Fix Immediately)

None - No type safety issues affect runtime behavior in production code.

### P1 - High (Fix This Sprint)

| Issue | File | Impact |
|-------|------|--------|
| Generic `AggregateOptions` | `statement/types.ts:553-573` | Public API type safety |
| Transaction function typing | `statement/types.ts:274,482` | Public API type inference |

### P2 - Medium (Fix This Quarter)

| Issue | File | Impact |
|-------|------|--------|
| Circular import workaround | `operators/window.ts:69` | Internal type safety |
| Type guards using `any` | `operators/cte.ts:410,417` | Runtime type narrowing |
| Optimizer type assertions | `planner/optimizer.ts` (9 locations) | Optimizer correctness |
| Enable `noUncheckedIndexedAccess` | `tsconfig.json` | Array/object access safety |

### P3 - Low (Backlog)

| Issue | File | Impact |
|-------|------|--------|
| Test file `as any` (150+ instances) | Various test files | Test type coverage |
| ORM adapter type assertions | `orm/*/` | Integration type safety |
| Parser test type narrowing | `parser/*.test.ts` | Test maintainability |

---

## Appendix A: Type Safety Metrics

| Metric | DoSQL | DoLake | Target |
|--------|-------|--------|--------|
| `strict` mode | Yes | Yes | Yes |
| `any` in production code | ~30 | 0 | 0 |
| `any` in test code | ~170 | 0 | <50 |
| `as any` assertions | 200+ | 0 | <20 |
| Branded types | 4 | 0 | - |
| Custom error classes | 25 | 4 | - |
| Type guards | 10+ | 2 | - |
| Conditional types | 5 | 0 | - |
| Template literal types | 2 | 0 | - |

## Appendix B: File Inventory

### DoSQL Type Definition Files

| File | Lines | Purpose |
|------|-------|---------|
| `engine/types.ts` | 800+ | Core types, branded types, query plans |
| `statement/types.ts` | 574 | Statement interface, bind parameters |
| `transaction/types.ts` | 482 | Transaction state machine, MVCC |
| `wal/types.ts` | 413 | Write-ahead log types |
| `cdc/types.ts` | 476 | Change data capture streaming |
| `sharding/types.ts` | 594 | VSchema, vindex configurations |
| `fsx/types.ts` | 240+ | Filesystem abstraction |
| `rpc/types.ts` | 300+ | RPC protocol types |

### DoLake Type Definition Files

| File | Lines | Purpose |
|------|-------|---------|
| `types.ts` | 200+ | Core lakehouse types |
| `buffer.ts` | 150+ | Buffer management |
| `parquet.ts` | 100+ | Parquet writer types |
| `iceberg.ts` | 150+ | Iceberg metadata types |

---

## Appendix C: Recommended Reading Order

For developers new to the codebase:

1. `src/engine/types.ts` - Core branded types and query plans
2. `src/statement/types.ts` - Statement API (better-sqlite3 compatible)
3. `src/database.ts` - Database class implementation
4. `src/transaction/types.ts` - Transaction state machine
5. `src/wal/types.ts` - WAL system types
6. `src/sharding/types.ts` - Distributed sharding (Vitess-inspired)
7. `src/cdc/types.ts` - CDC streaming to lakehouse

---

*This review was generated by Claude Code (Opus 4.5) as part of the DoSQL TypeScript audit.*
