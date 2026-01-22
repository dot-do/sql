# TypeScript Review: @dotdo/sql Monorepo

**Date**: 2026-01-22
**Reviewer**: TypeScript Architecture Review
**Status**: Initial Review (Pre-Implementation)

## Executive Summary

The `@dotdo/sql` monorepo is in its initial setup phase with no TypeScript source code yet implemented. This review documents the TypeScript requirements established in the project documentation and proposes a comprehensive type system design for the DoSQL (SQL database engine) and DoLake (lakehouse for CDC/analytics) packages.

## Current State Analysis

### Repository Structure

```
/Users/nathanclevenger/projects/sql/
├── package.json          # Root package (pnpm workspace)
├── pnpm-workspace.yaml   # Workspace configuration
├── packages/             # Empty - packages not yet created
├── README.md             # Project overview
├── CLAUDE.md             # Development guidelines
└── AGENTS.md             # Agent instructions
```

### Documented TypeScript Requirements

From `CLAUDE.md`:

| Requirement | Status | Notes |
|-------------|--------|-------|
| TypeScript strict mode | Required | Must be enabled in all tsconfig files |
| Branded types for IDs | Required | TransactionId, LSN explicitly mentioned |
| Prefer `unknown` over `any` | Required | Type safety mandate |
| No unnecessary abstractions | Required | Keep types lean |

---

## Proposed Type System Design

### 1. Root TypeScript Configuration

**File**: `/packages/tsconfig.base.json`

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "NodeNext",
    "moduleResolution": "NodeNext",
    "lib": ["ES2022"],
    "declaration": true,
    "declarationMap": true,
    "sourceMap": true,
    "strict": true,
    "noImplicitAny": true,
    "strictNullChecks": true,
    "strictFunctionTypes": true,
    "strictBindCallApply": true,
    "strictPropertyInitialization": true,
    "noImplicitThis": true,
    "useUnknownInCatchVariables": true,
    "alwaysStrict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "exactOptionalPropertyTypes": true,
    "noImplicitReturns": true,
    "noFallthroughCasesInSwitch": true,
    "noUncheckedIndexedAccess": true,
    "noImplicitOverride": true,
    "noPropertyAccessFromIndexSignature": true,
    "esModuleInterop": true,
    "forceConsistentCasingInFileNames": true,
    "isolatedModules": true,
    "verbatimModuleSyntax": true,
    "skipLibCheck": true
  }
}
```

**Key Settings Rationale**:

| Setting | Purpose |
|---------|---------|
| `strict: true` | Enables all strict mode family options |
| `noUncheckedIndexedAccess` | Adds `undefined` to index signatures, catches array/object access bugs |
| `exactOptionalPropertyTypes` | Distinguishes `prop?: T` from `prop: T \| undefined` |
| `useUnknownInCatchVariables` | `catch(e)` gives `unknown` not `any` |
| `verbatimModuleSyntax` | Enforces explicit `import type` for type-only imports |
| `noPropertyAccessFromIndexSignature` | Forces bracket notation for dynamic keys |

---

### 2. Branded Types System

Branded types prevent accidentally mixing semantically different values that share the same primitive type.

**File**: `packages/dosql/src/types/branded.ts`

```typescript
/**
 * Brand symbol for creating nominal types from structural types.
 * Uses a unique symbol to ensure brands cannot be accidentally satisfied.
 */
declare const brand: unique symbol;

/**
 * Creates a branded type from a base type.
 * @template T - The base type (e.g., string, number, bigint)
 * @template Brand - A unique string literal identifying the brand
 */
export type Branded<T, Brand extends string> = T & {
  readonly [brand]: Brand;
};

// ============================================================
// DoSQL Branded Types
// ============================================================

/** Unique identifier for a database transaction */
export type TransactionId = Branded<bigint, 'TransactionId'>;

/** Log Sequence Number - monotonic identifier for WAL entries */
export type LSN = Branded<bigint, 'LSN'>;

/** Unique identifier for a database shard */
export type ShardId = Branded<string, 'ShardId'>;

/** Unique identifier for a database table */
export type TableId = Branded<string, 'TableId'>;

/** Unique identifier for a row within a table */
export type RowId = Branded<bigint, 'RowId'>;

/** Unique identifier for a B-tree page */
export type PageId = Branded<number, 'PageId'>;

/** Unique identifier for a B-tree node */
export type NodeId = Branded<number, 'NodeId'>;

/** SQL statement hash for query plan caching */
export type StatementHash = Branded<string, 'StatementHash'>;

/** Snapshot isolation timestamp */
export type SnapshotId = Branded<bigint, 'SnapshotId'>;

// ============================================================
// DoLake Branded Types
// ============================================================

/** Unique identifier for a CDC event */
export type CDCEventId = Branded<string, 'CDCEventId'>;

/** Unique identifier for a Parquet file */
export type ParquetFileId = Branded<string, 'ParquetFileId'>;

/** Partition key for lakehouse data */
export type PartitionKey = Branded<string, 'PartitionKey'>;

/** Unique identifier for a compaction job */
export type CompactionJobId = Branded<string, 'CompactionJobId'>;

// ============================================================
// Brand Constructors (runtime validation)
// ============================================================

/**
 * Creates a TransactionId from a bigint.
 * @throws Error if value is negative
 */
export function createTransactionId(value: bigint): TransactionId {
  if (value < 0n) {
    throw new Error(`TransactionId must be non-negative: ${value}`);
  }
  return value as TransactionId;
}

/**
 * Creates an LSN from a bigint.
 * @throws Error if value is negative
 */
export function createLSN(value: bigint): LSN {
  if (value < 0n) {
    throw new Error(`LSN must be non-negative: ${value}`);
  }
  return value as LSN;
}

/**
 * Creates a ShardId from a string.
 * @throws Error if value is empty or invalid format
 */
export function createShardId(value: string): ShardId {
  if (!value || !/^shard-[a-z0-9-]+$/.test(value)) {
    throw new Error(`Invalid ShardId format: ${value}`);
  }
  return value as ShardId;
}

/**
 * Type guard to check if a value is a valid TransactionId
 */
export function isTransactionId(value: unknown): value is TransactionId {
  return typeof value === 'bigint' && value >= 0n;
}

/**
 * Type guard to check if a value is a valid LSN
 */
export function isLSN(value: unknown): value is LSN {
  return typeof value === 'bigint' && value >= 0n;
}
```

**Usage Example**:

```typescript
// This will NOT compile - prevents mixing IDs
function commitTransaction(txnId: TransactionId, lsn: LSN): void { /* ... */ }

const txn = createTransactionId(1n);
const lsn = createLSN(100n);

commitTransaction(txn, lsn);  // OK
commitTransaction(lsn, txn);  // ERROR: Argument of type 'LSN' is not assignable to parameter of type 'TransactionId'
```

---

### 3. Result Types (Error Handling)

Use discriminated unions instead of throwing exceptions for expected error cases.

**File**: `packages/dosql/src/types/result.ts`

```typescript
/**
 * Represents a successful operation result.
 */
export interface Ok<T> {
  readonly ok: true;
  readonly value: T;
}

/**
 * Represents a failed operation result.
 */
export interface Err<E> {
  readonly ok: false;
  readonly error: E;
}

/**
 * Result type for operations that can fail in expected ways.
 * Use this instead of throwing exceptions for recoverable errors.
 */
export type Result<T, E = Error> = Ok<T> | Err<E>;

/**
 * Creates a successful result.
 */
export function ok<T>(value: T): Ok<T> {
  return { ok: true, value };
}

/**
 * Creates a failed result.
 */
export function err<E>(error: E): Err<E> {
  return { ok: false, error };
}

/**
 * Unwraps a Result, throwing if it's an error.
 * Use sparingly - prefer pattern matching.
 */
export function unwrap<T, E>(result: Result<T, E>): T {
  if (result.ok) {
    return result.value;
  }
  throw result.error;
}

/**
 * Maps over a successful result.
 */
export function map<T, U, E>(
  result: Result<T, E>,
  fn: (value: T) => U
): Result<U, E> {
  if (result.ok) {
    return ok(fn(result.value));
  }
  return result;
}

/**
 * Chains Result-returning operations.
 */
export function flatMap<T, U, E>(
  result: Result<T, E>,
  fn: (value: T) => Result<U, E>
): Result<U, E> {
  if (result.ok) {
    return fn(result.value);
  }
  return result;
}
```

---

### 4. SQL AST Types

Strongly typed Abstract Syntax Tree for parsed SQL.

**File**: `packages/dosql/src/parser/ast.ts`

```typescript
import type { TableId, ShardId } from '../types/branded.js';

// ============================================================
// Base Types
// ============================================================

export type SqlValue =
  | { type: 'null' }
  | { type: 'boolean'; value: boolean }
  | { type: 'integer'; value: bigint }
  | { type: 'float'; value: number }
  | { type: 'text'; value: string }
  | { type: 'blob'; value: Uint8Array }
  | { type: 'parameter'; index: number };

export interface ColumnRef {
  table?: string;
  column: string;
  alias?: string;
}

// ============================================================
// Expressions
// ============================================================

export type Expression =
  | LiteralExpr
  | ColumnExpr
  | BinaryExpr
  | UnaryExpr
  | FunctionExpr
  | SubqueryExpr
  | CaseExpr
  | CastExpr
  | BetweenExpr
  | InExpr
  | IsNullExpr
  | ExistsExpr;

export interface LiteralExpr {
  type: 'literal';
  value: SqlValue;
}

export interface ColumnExpr {
  type: 'column';
  ref: ColumnRef;
}

export interface BinaryExpr {
  type: 'binary';
  operator: BinaryOperator;
  left: Expression;
  right: Expression;
}

export type BinaryOperator =
  | '=' | '<>' | '!=' | '<' | '<=' | '>' | '>='
  | '+' | '-' | '*' | '/' | '%'
  | 'AND' | 'OR'
  | 'LIKE' | 'GLOB' | 'REGEXP'
  | '||'  // string concatenation
  | '&' | '|' | '<<' | '>>';  // bitwise

export interface UnaryExpr {
  type: 'unary';
  operator: UnaryOperator;
  operand: Expression;
}

export type UnaryOperator = 'NOT' | '-' | '+' | '~';

export interface FunctionExpr {
  type: 'function';
  name: string;
  args: Expression[];
  distinct?: boolean;
  filter?: Expression;
  over?: WindowSpec;
}

export interface WindowSpec {
  partitionBy?: Expression[];
  orderBy?: OrderByItem[];
  frame?: WindowFrame;
}

export interface WindowFrame {
  type: 'ROWS' | 'RANGE' | 'GROUPS';
  start: FrameBound;
  end?: FrameBound;
}

export type FrameBound =
  | { type: 'UNBOUNDED_PRECEDING' }
  | { type: 'CURRENT_ROW' }
  | { type: 'UNBOUNDED_FOLLOWING' }
  | { type: 'PRECEDING'; value: number }
  | { type: 'FOLLOWING'; value: number };

export interface SubqueryExpr {
  type: 'subquery';
  query: SelectStatement;
}

export interface CaseExpr {
  type: 'case';
  operand?: Expression;
  when: Array<{ condition: Expression; result: Expression }>;
  else?: Expression;
}

export interface CastExpr {
  type: 'cast';
  operand: Expression;
  targetType: DataType;
}

export interface BetweenExpr {
  type: 'between';
  operand: Expression;
  low: Expression;
  high: Expression;
  not?: boolean;
}

export interface InExpr {
  type: 'in';
  operand: Expression;
  values: Expression[] | SelectStatement;
  not?: boolean;
}

export interface IsNullExpr {
  type: 'is_null';
  operand: Expression;
  not?: boolean;
}

export interface ExistsExpr {
  type: 'exists';
  subquery: SelectStatement;
  not?: boolean;
}

// ============================================================
// Data Types
// ============================================================

export type DataType =
  | { type: 'INTEGER' }
  | { type: 'REAL' }
  | { type: 'TEXT'; maxLength?: number }
  | { type: 'BLOB' }
  | { type: 'BOOLEAN' }
  | { type: 'TIMESTAMP' }
  | { type: 'DATE' }
  | { type: 'JSON' };

// ============================================================
// Statements
// ============================================================

export type Statement =
  | SelectStatement
  | InsertStatement
  | UpdateStatement
  | DeleteStatement
  | CreateTableStatement
  | DropTableStatement
  | CreateIndexStatement
  | DropIndexStatement
  | BeginStatement
  | CommitStatement
  | RollbackStatement;

// ============================================================
// SELECT Statement
// ============================================================

export interface SelectStatement {
  type: 'select';
  with?: CTEDefinition[];
  columns: SelectColumn[];
  from?: TableReference[];
  where?: Expression;
  groupBy?: Expression[];
  having?: Expression;
  orderBy?: OrderByItem[];
  limit?: Expression;
  offset?: Expression;
  distinct?: boolean;
  union?: {
    type: 'UNION' | 'UNION ALL' | 'INTERSECT' | 'EXCEPT';
    select: SelectStatement;
  };
}

export type SelectColumn =
  | { type: 'all' }
  | { type: 'table_all'; table: string }
  | { type: 'expression'; expr: Expression; alias?: string };

export interface CTEDefinition {
  name: string;
  columns?: string[];
  query: SelectStatement;
  recursive?: boolean;
}

export type TableReference =
  | TableRef
  | JoinRef
  | SubqueryRef;

export interface TableRef {
  type: 'table';
  name: string;
  alias?: string;
  schema?: string;
}

export interface JoinRef {
  type: 'join';
  joinType: JoinType;
  left: TableReference;
  right: TableReference;
  on?: Expression;
  using?: string[];
}

export type JoinType =
  | 'INNER'
  | 'LEFT'
  | 'RIGHT'
  | 'FULL'
  | 'CROSS'
  | 'NATURAL';

export interface SubqueryRef {
  type: 'subquery';
  query: SelectStatement;
  alias: string;
}

export interface OrderByItem {
  expr: Expression;
  direction?: 'ASC' | 'DESC';
  nulls?: 'FIRST' | 'LAST';
}

// ============================================================
// INSERT Statement
// ============================================================

export interface InsertStatement {
  type: 'insert';
  table: string;
  columns?: string[];
  values?: SqlValue[][];
  query?: SelectStatement;
  onConflict?: OnConflictClause;
  returning?: SelectColumn[];
}

export interface OnConflictClause {
  columns?: string[];
  action: 'NOTHING' | {
    type: 'UPDATE';
    set: Array<{ column: string; value: Expression }>;
    where?: Expression;
  };
}

// ============================================================
// UPDATE Statement
// ============================================================

export interface UpdateStatement {
  type: 'update';
  table: string;
  set: Array<{ column: string; value: Expression }>;
  from?: TableReference[];
  where?: Expression;
  returning?: SelectColumn[];
}

// ============================================================
// DELETE Statement
// ============================================================

export interface DeleteStatement {
  type: 'delete';
  table: string;
  where?: Expression;
  returning?: SelectColumn[];
}

// ============================================================
// DDL Statements
// ============================================================

export interface CreateTableStatement {
  type: 'create_table';
  table: string;
  columns: ColumnDefinition[];
  constraints?: TableConstraint[];
  ifNotExists?: boolean;
  as?: SelectStatement;
}

export interface ColumnDefinition {
  name: string;
  dataType: DataType;
  constraints?: ColumnConstraint[];
}

export type ColumnConstraint =
  | { type: 'PRIMARY_KEY'; autoIncrement?: boolean }
  | { type: 'NOT_NULL' }
  | { type: 'UNIQUE' }
  | { type: 'DEFAULT'; value: Expression }
  | { type: 'CHECK'; expr: Expression }
  | { type: 'REFERENCES'; table: string; column: string; onDelete?: ForeignKeyAction; onUpdate?: ForeignKeyAction };

export type ForeignKeyAction = 'CASCADE' | 'SET NULL' | 'SET DEFAULT' | 'RESTRICT' | 'NO ACTION';

export type TableConstraint =
  | { type: 'PRIMARY_KEY'; columns: string[] }
  | { type: 'UNIQUE'; columns: string[] }
  | { type: 'CHECK'; name?: string; expr: Expression }
  | { type: 'FOREIGN_KEY'; columns: string[]; references: { table: string; columns: string[] }; onDelete?: ForeignKeyAction; onUpdate?: ForeignKeyAction };

export interface DropTableStatement {
  type: 'drop_table';
  table: string;
  ifExists?: boolean;
}

export interface CreateIndexStatement {
  type: 'create_index';
  name: string;
  table: string;
  columns: Array<{ column: string; direction?: 'ASC' | 'DESC' }>;
  unique?: boolean;
  ifNotExists?: boolean;
  where?: Expression;
}

export interface DropIndexStatement {
  type: 'drop_index';
  name: string;
  ifExists?: boolean;
}

// ============================================================
// Transaction Statements
// ============================================================

export interface BeginStatement {
  type: 'begin';
  mode?: 'DEFERRED' | 'IMMEDIATE' | 'EXCLUSIVE';
}

export interface CommitStatement {
  type: 'commit';
}

export interface RollbackStatement {
  type: 'rollback';
  savepoint?: string;
}
```

---

### 5. Database Engine Types

**File**: `packages/dosql/src/engine/types.ts`

```typescript
import type { TransactionId, LSN, ShardId, RowId, TableId } from '../types/branded.js';
import type { Result } from '../types/result.js';
import type { Statement, SelectStatement, DataType } from '../parser/ast.js';

// ============================================================
// Query Execution Types
// ============================================================

export interface QueryResult {
  columns: ColumnMetadata[];
  rows: Row[];
  rowsAffected: number;
  lastInsertRowId?: RowId;
}

export interface ColumnMetadata {
  name: string;
  type: DataType;
  table?: string;
  nullable: boolean;
}

export type Row = ReadonlyArray<SqlRuntimeValue>;

export type SqlRuntimeValue =
  | null
  | boolean
  | bigint
  | number
  | string
  | Uint8Array;

// ============================================================
// Transaction Types
// ============================================================

export type IsolationLevel =
  | 'READ_UNCOMMITTED'
  | 'READ_COMMITTED'
  | 'REPEATABLE_READ'
  | 'SERIALIZABLE';

export interface Transaction {
  readonly id: TransactionId;
  readonly isolationLevel: IsolationLevel;
  readonly startLSN: LSN;
  readonly readOnly: boolean;
}

export interface TransactionState {
  readonly status: 'active' | 'committed' | 'aborted';
  readonly locksHeld: ReadonlySet<LockKey>;
  readonly undoLog: ReadonlyArray<UndoEntry>;
}

export type LockKey = `${TableId}:${RowId}` | `${TableId}:*`;

export interface UndoEntry {
  readonly lsn: LSN;
  readonly table: TableId;
  readonly rowId: RowId;
  readonly beforeImage: Row | null;
}

// ============================================================
// WAL Types
// ============================================================

export type WALEntry =
  | InsertWALEntry
  | UpdateWALEntry
  | DeleteWALEntry
  | CommitWALEntry
  | AbortWALEntry
  | CheckpointWALEntry;

export interface InsertWALEntry {
  readonly type: 'insert';
  readonly lsn: LSN;
  readonly txnId: TransactionId;
  readonly table: TableId;
  readonly rowId: RowId;
  readonly data: Row;
}

export interface UpdateWALEntry {
  readonly type: 'update';
  readonly lsn: LSN;
  readonly txnId: TransactionId;
  readonly table: TableId;
  readonly rowId: RowId;
  readonly beforeImage: Row;
  readonly afterImage: Row;
}

export interface DeleteWALEntry {
  readonly type: 'delete';
  readonly lsn: LSN;
  readonly txnId: TransactionId;
  readonly table: TableId;
  readonly rowId: RowId;
  readonly beforeImage: Row;
}

export interface CommitWALEntry {
  readonly type: 'commit';
  readonly lsn: LSN;
  readonly txnId: TransactionId;
}

export interface AbortWALEntry {
  readonly type: 'abort';
  readonly lsn: LSN;
  readonly txnId: TransactionId;
}

export interface CheckpointWALEntry {
  readonly type: 'checkpoint';
  readonly lsn: LSN;
  readonly activeTransactions: ReadonlyArray<TransactionId>;
}

// ============================================================
// Error Types
// ============================================================

export type DatabaseError =
  | { code: 'SYNTAX_ERROR'; message: string; position?: number }
  | { code: 'TABLE_NOT_FOUND'; table: string }
  | { code: 'COLUMN_NOT_FOUND'; column: string; table?: string }
  | { code: 'TYPE_MISMATCH'; expected: DataType; actual: DataType }
  | { code: 'CONSTRAINT_VIOLATION'; constraint: string; message: string }
  | { code: 'TRANSACTION_CONFLICT'; txnId: TransactionId }
  | { code: 'DEADLOCK_DETECTED'; txnId: TransactionId }
  | { code: 'LOCK_TIMEOUT'; resource: LockKey }
  | { code: 'READONLY_TRANSACTION' }
  | { code: 'INTERNAL_ERROR'; message: string };

// ============================================================
// Engine Interfaces
// ============================================================

export interface QueryEngine {
  execute(
    sql: string,
    params?: SqlRuntimeValue[]
  ): Promise<Result<QueryResult, DatabaseError>>;

  prepare(
    sql: string
  ): Result<PreparedStatement, DatabaseError>;
}

export interface PreparedStatement {
  readonly sql: string;
  readonly parameterCount: number;

  execute(
    params?: SqlRuntimeValue[]
  ): Promise<Result<QueryResult, DatabaseError>>;

  close(): void;
}

export interface TransactionManager {
  begin(
    isolationLevel?: IsolationLevel,
    readOnly?: boolean
  ): Promise<Result<Transaction, DatabaseError>>;

  commit(
    txnId: TransactionId
  ): Promise<Result<void, DatabaseError>>;

  rollback(
    txnId: TransactionId
  ): Promise<Result<void, DatabaseError>>;
}
```

---

### 6. DoLake CDC Types

**File**: `packages/dolake/src/types/cdc.ts`

```typescript
import type { TransactionId, LSN, TableId, RowId, CDCEventId, PartitionKey } from '@dotdo/dosql/types/branded.js';
import type { Row, SqlRuntimeValue } from '@dotdo/dosql/engine/types.js';

// ============================================================
// CDC Event Types
// ============================================================

export type CDCEvent =
  | CDCInsertEvent
  | CDCUpdateEvent
  | CDCDeleteEvent
  | CDCSchemaChangeEvent
  | CDCTruncateEvent;

export interface CDCEventBase {
  readonly id: CDCEventId;
  readonly lsn: LSN;
  readonly txnId: TransactionId;
  readonly timestamp: bigint;  // Unix timestamp in microseconds
  readonly table: TableId;
  readonly partitionKey?: PartitionKey;
}

export interface CDCInsertEvent extends CDCEventBase {
  readonly type: 'insert';
  readonly rowId: RowId;
  readonly after: Row;
}

export interface CDCUpdateEvent extends CDCEventBase {
  readonly type: 'update';
  readonly rowId: RowId;
  readonly before: Row;
  readonly after: Row;
  readonly changedColumns: ReadonlyArray<number>;  // Column indices that changed
}

export interface CDCDeleteEvent extends CDCEventBase {
  readonly type: 'delete';
  readonly rowId: RowId;
  readonly before: Row;
}

export interface CDCSchemaChangeEvent extends CDCEventBase {
  readonly type: 'schema_change';
  readonly changeType: 'CREATE_TABLE' | 'ALTER_TABLE' | 'DROP_TABLE';
  readonly ddl: string;
}

export interface CDCTruncateEvent extends CDCEventBase {
  readonly type: 'truncate';
}

// ============================================================
// CDC Stream Types
// ============================================================

export interface CDCStreamPosition {
  readonly lsn: LSN;
  readonly timestamp: bigint;
}

export interface CDCSubscription {
  readonly id: string;
  readonly tables: ReadonlyArray<TableId>;
  readonly position: CDCStreamPosition;
  readonly filter?: CDCFilter;
}

export interface CDCFilter {
  readonly tables?: ReadonlyArray<TableId>;
  readonly eventTypes?: ReadonlyArray<CDCEvent['type']>;
  readonly predicate?: string;  // SQL-like predicate expression
}

export interface CDCBatch {
  readonly events: ReadonlyArray<CDCEvent>;
  readonly startPosition: CDCStreamPosition;
  readonly endPosition: CDCStreamPosition;
  readonly isComplete: boolean;
}

// ============================================================
// Lakehouse Types
// ============================================================

export interface LakehousePartition {
  readonly key: PartitionKey;
  readonly table: TableId;
  readonly minLSN: LSN;
  readonly maxLSN: LSN;
  readonly rowCount: number;
  readonly byteSize: number;
  readonly parquetFiles: ReadonlyArray<ParquetFileMetadata>;
}

export interface ParquetFileMetadata {
  readonly path: string;  // R2 object key
  readonly rowCount: number;
  readonly byteSize: number;
  readonly minValues: Record<string, SqlRuntimeValue>;
  readonly maxValues: Record<string, SqlRuntimeValue>;
  readonly nullCounts: Record<string, number>;
}

export interface CompactionJob {
  readonly id: string;
  readonly partition: PartitionKey;
  readonly inputFiles: ReadonlyArray<string>;
  readonly status: 'pending' | 'running' | 'completed' | 'failed';
  readonly startedAt?: bigint;
  readonly completedAt?: bigint;
  readonly error?: string;
}

// ============================================================
// Time Travel Types
// ============================================================

export interface Snapshot {
  readonly id: string;
  readonly timestamp: bigint;
  readonly lsn: LSN;
  readonly manifest: SnapshotManifest;
}

export interface SnapshotManifest {
  readonly partitions: ReadonlyArray<LakehousePartition>;
  readonly schema: TableSchema;
}

export interface TableSchema {
  readonly columns: ReadonlyArray<{
    name: string;
    type: string;
    nullable: boolean;
  }>;
}
```

---

### 7. Utility Types

**File**: `packages/dosql/src/types/utils.ts`

```typescript
/**
 * Makes all properties deeply readonly.
 */
export type DeepReadonly<T> = T extends (infer U)[]
  ? ReadonlyArray<DeepReadonly<U>>
  : T extends Map<infer K, infer V>
  ? ReadonlyMap<DeepReadonly<K>, DeepReadonly<V>>
  : T extends Set<infer U>
  ? ReadonlySet<DeepReadonly<U>>
  : T extends object
  ? { readonly [K in keyof T]: DeepReadonly<T[K]> }
  : T;

/**
 * Makes specified keys required and non-nullable.
 */
export type RequiredNonNullable<T, K extends keyof T> = T & {
  [P in K]-?: NonNullable<T[P]>;
};

/**
 * Extracts the element type from an array or readonly array.
 */
export type ArrayElement<T> = T extends readonly (infer U)[] ? U : never;

/**
 * Creates a type where at least one of the specified keys is required.
 */
export type AtLeastOne<T, Keys extends keyof T = keyof T> = Pick<T, Exclude<keyof T, Keys>> &
  {
    [K in Keys]-?: Required<Pick<T, K>> & Partial<Pick<T, Exclude<Keys, K>>>;
  }[Keys];

/**
 * Creates a type where exactly one of the specified keys is present.
 */
export type ExactlyOne<T, Keys extends keyof T = keyof T> = Pick<T, Exclude<keyof T, Keys>> &
  {
    [K in Keys]-?: Required<Pick<T, K>> & Partial<Record<Exclude<Keys, K>, never>>;
  }[Keys];

/**
 * Asserts that a type is never (for exhaustiveness checking).
 */
export function assertNever(value: never, message?: string): never {
  throw new Error(message ?? `Unexpected value: ${JSON.stringify(value)}`);
}

/**
 * Type-safe object keys.
 */
export function typedKeys<T extends object>(obj: T): Array<keyof T> {
  return Object.keys(obj) as Array<keyof T>;
}

/**
 * Type-safe object entries.
 */
export function typedEntries<T extends object>(obj: T): Array<[keyof T, T[keyof T]]> {
  return Object.entries(obj) as Array<[keyof T, T[keyof T]]>;
}

/**
 * Narrows an array to a non-empty array.
 */
export type NonEmptyArray<T> = [T, ...T[]];

/**
 * Type guard for non-empty arrays.
 */
export function isNonEmpty<T>(arr: readonly T[]): arr is NonEmptyArray<T> {
  return arr.length > 0;
}

/**
 * Asserts a condition is true, narrowing the type.
 */
export function assert(condition: unknown, message?: string): asserts condition {
  if (!condition) {
    throw new Error(message ?? 'Assertion failed');
  }
}

/**
 * Asserts a value is defined (not null or undefined).
 */
export function assertDefined<T>(
  value: T,
  message?: string
): asserts value is NonNullable<T> {
  if (value === null || value === undefined) {
    throw new Error(message ?? 'Expected value to be defined');
  }
}
```

---

### 8. Type Export Strategy

Each package should have a centralized `types.ts` or `types/index.ts` that re-exports all public types.

**File**: `packages/dosql/src/types/index.ts`

```typescript
// Branded types
export type {
  TransactionId,
  LSN,
  ShardId,
  TableId,
  RowId,
  PageId,
  NodeId,
  StatementHash,
  SnapshotId,
} from './branded.js';

export {
  createTransactionId,
  createLSN,
  createShardId,
  isTransactionId,
  isLSN,
} from './branded.js';

// Result types
export type { Ok, Err, Result } from './result.js';
export { ok, err, unwrap, map, flatMap } from './result.js';

// Utility types
export type {
  DeepReadonly,
  RequiredNonNullable,
  ArrayElement,
  AtLeastOne,
  ExactlyOne,
  NonEmptyArray,
} from './utils.js';

export {
  assertNever,
  typedKeys,
  typedEntries,
  isNonEmpty,
  assert,
  assertDefined,
} from './utils.js';
```

**File**: `packages/dosql/src/index.ts`

```typescript
// Re-export all public types
export * from './types/index.js';

// Re-export parser types
export type { Statement, Expression, SelectStatement } from './parser/ast.js';

// Re-export engine types
export type {
  QueryResult,
  QueryEngine,
  PreparedStatement,
  Transaction,
  DatabaseError,
} from './engine/types.js';
```

---

## Guidelines & Best Practices

### 1. Use `unknown` Over `any`

```typescript
// BAD
function parseJson(input: string): any {
  return JSON.parse(input);
}

// GOOD
function parseJson(input: string): unknown {
  return JSON.parse(input);
}

// Use type guards to narrow unknown
function isQueryResult(value: unknown): value is QueryResult {
  return (
    typeof value === 'object' &&
    value !== null &&
    'columns' in value &&
    'rows' in value
  );
}
```

### 2. Exhaustiveness Checking

```typescript
function handleStatement(stmt: Statement): void {
  switch (stmt.type) {
    case 'select':
      handleSelect(stmt);
      break;
    case 'insert':
      handleInsert(stmt);
      break;
    case 'update':
      handleUpdate(stmt);
      break;
    case 'delete':
      handleDelete(stmt);
      break;
    // ... other cases
    default:
      // This ensures all cases are handled
      assertNever(stmt);
  }
}
```

### 3. Prefer Readonly Types

```typescript
// BAD - allows mutation
interface Transaction {
  id: TransactionId;
  status: string;
}

// GOOD - immutable
interface Transaction {
  readonly id: TransactionId;
  readonly status: 'active' | 'committed' | 'aborted';
}

// For collections, use ReadonlyArray, ReadonlyMap, ReadonlySet
function processEvents(events: ReadonlyArray<CDCEvent>): void {
  // events.push() would be a type error
}
```

### 4. Discriminated Unions for State

```typescript
// BAD - boolean flags and nullable fields
interface QueryState {
  isLoading: boolean;
  isError: boolean;
  data: QueryResult | null;
  error: Error | null;
}

// GOOD - discriminated union
type QueryState =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; data: QueryResult }
  | { status: 'error'; error: DatabaseError };
```

### 5. Generic Constraints

```typescript
// BAD - too loose
function findById<T>(items: T[], id: string): T | undefined {
  // @ts-expect-error - T might not have 'id'
  return items.find(item => item.id === id);
}

// GOOD - constrained generic
function findById<T extends { id: string }>(
  items: readonly T[],
  id: string
): T | undefined {
  return items.find(item => item.id === id);
}
```

---

## Recommendations

### Immediate Actions

1. **Create tsconfig.base.json** with strict settings as documented above
2. **Implement branded types** in `packages/dosql/src/types/branded.ts`
3. **Implement Result types** in `packages/dosql/src/types/result.ts`
4. **Enable verbatimModuleSyntax** to enforce `import type` declarations

### Code Review Checklist

When reviewing TypeScript code, verify:

- [ ] No use of `any` (use `unknown` with type guards)
- [ ] All public APIs use branded types for IDs
- [ ] Result types used for recoverable errors
- [ ] Discriminated unions for state (not boolean flags)
- [ ] `readonly` modifiers on immutable data
- [ ] Exhaustive switch statements with `assertNever`
- [ ] Type exports are explicit (not `export *`)
- [ ] Generic constraints are properly specified
- [ ] No type assertions (`as`) without justification

### Future Enhancements

1. **Zod Integration** - Runtime validation schemas that infer static types
2. **Effect-TS** - Consider for advanced error handling
3. **TypeScript ESLint** - Enable `@typescript-eslint/strict` ruleset
4. **API Documentation** - Use TSDoc comments for public APIs

---

## Appendix: Package-Specific Configurations

### DoSQL Package tsconfig.json

```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": {
    "outDir": "./dist",
    "rootDir": "./src",
    "types": ["@cloudflare/workers-types"]
  },
  "include": ["src/**/*.ts"],
  "exclude": ["node_modules", "dist", "**/*.test.ts"]
}
```

### DoLake Package tsconfig.json

```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": {
    "outDir": "./dist",
    "rootDir": "./src",
    "types": ["@cloudflare/workers-types"]
  },
  "include": ["src/**/*.ts"],
  "exclude": ["node_modules", "dist", "**/*.test.ts"],
  "references": [
    { "path": "../dosql" }
  ]
}
```

---

## Conclusion

The `@dotdo/sql` monorepo is well-positioned for a robust TypeScript implementation. The documented requirements (strict mode, branded types, `unknown` over `any`) align with modern TypeScript best practices. This review provides a comprehensive type system design that should be implemented as the packages are developed.

Key priorities:

1. **Start with branded types** - They form the foundation of type safety for database IDs
2. **Implement Result types early** - Establishes error handling patterns from the start
3. **Use strict tsconfig** - Catches issues at compile time rather than runtime
4. **Document type conventions** - Ensures consistency across contributors
