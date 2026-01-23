# DoSQL API Reference

Complete API documentation for DoSQL - a type-safe SQL database for Cloudflare Workers.

## Table of Contents

- [Module Exports](#module-exports)
- [Database Class](#database-class)
  - [Constructor](#constructor)
  - [Properties](#properties)
  - [Core Methods](#core-methods)
  - [Prepared Statements](#prepared-statements)
  - [Transactions](#transactions)
  - [Savepoints](#savepoints)
  - [Batch Operations](#batch-operations)
  - [PRAGMA Commands](#pragma-commands)
  - [User-Defined Functions](#user-defined-functions)
- [Statement Interface](#statement-interface)
  - [Statement Properties](#statement-properties)
  - [Statement Methods](#statement-methods)
  - [Statement Configuration](#statement-configuration)
- [Type System](#type-system)
  - [SQL Value Types](#sql-value-types)
  - [Parameter Binding](#parameter-binding)
  - [Result Types](#result-types)
- [Error Handling](#error-handling)
  - [Error Hierarchy](#error-hierarchy)
  - [Error Codes](#error-codes)
  - [Error Handling Patterns](#error-handling-patterns)
- [RPC API](#rpc-api)
  - [Client Creation](#client-creation)
  - [Query Operations](#query-operations)
  - [Streaming Queries](#streaming-queries)
  - [Transaction Operations](#transaction-operations)
  - [Schema Operations](#schema-operations)
  - [Connection Management](#connection-management)
  - [Server Implementation](#server-implementation)
- [WAL (Write-Ahead Log)](#wal-write-ahead-log)
  - [WAL Writer](#wal-writer)
  - [WAL Reader](#wal-reader)
  - [Checkpoint Management](#checkpoint-management)
  - [WAL Retention](#wal-retention)
- [CDC (Change Data Capture)](#cdc-change-data-capture)
  - [Subscriptions](#subscriptions)
  - [Change Events](#change-events)
  - [Replication Slots](#replication-slots)
  - [Lakehouse Streaming](#lakehouse-streaming)
- [Transaction Management](#transaction-management)
  - [Transaction Manager](#transaction-manager)
  - [Isolation Levels](#isolation-levels)
  - [MVCC Support](#mvcc-support)
- [Branching](#branching)
  - [Branch Manager](#branch-manager)
  - [Branch Operations](#branch-operations)
  - [Merge Operations](#merge-operations)
- [Migrations](#migrations)
  - [Migration Runner](#migration-runner)
  - [Schema Tracker](#schema-tracker)
  - [Drizzle Compatibility](#drizzle-compatibility)
- [Virtual Tables](#virtual-tables)
  - [URL Table Sources](#url-table-sources)
  - [R2 Sources](#r2-sources)
- [FSX (File System Abstraction)](#fsx-file-system-abstraction)
  - [Storage Backends](#storage-backends)
  - [Tiered Storage](#tiered-storage)
  - [Copy-on-Write Backend](#copy-on-write-backend)
- [Stored Procedures](#stored-procedures)
  - [Procedure Registry](#procedure-registry)
  - [Procedure Builder](#procedure-builder)
  - [Functional API](#functional-api)
- [Sharding](#sharding)
  - [VSchema Definition](#vschema-definition)
  - [Vindex Types](#vindex-types)
  - [Query Routing](#query-routing)
- [Columnar Storage](#columnar-storage)
  - [Encoding Types](#encoding-types)
  - [Automatic Encoding Selection](#automatic-encoding-selection)
  - [Column Statistics (Zone Maps)](#column-statistics-zone-maps)
  - [Writer Usage](#writer-usage)
  - [Reader Usage](#reader-usage)
  - [Data Types](#data-types)
  - [Predicate Operations](#predicate-operations)

---

## Module Exports

DoSQL provides multiple subpath exports for importing specific functionality:

```typescript
// Main entry point - types, factories, errors
import { createDatabase, createQuery, DoSQLError, IsolationLevel } from 'dosql';

// RPC client/server for remote database access
import { createWebSocketClient, createHttpClient, DoSQLTarget } from 'dosql/rpc';

// Write-Ahead Log for durability
import { createWALWriter, createWALReader, createCheckpointManager } from 'dosql/wal';

// Change Data Capture for real-time streaming
import { createCDC, createCDCSubscription, createReplicationSlotManager } from 'dosql/cdc';

// Transaction utilities
import { createTransactionManager, executeInTransaction, IsolationLevel } from 'dosql/transaction';

// FSX - File System Abstraction for tiered storage
import {
  createDOBackend,
  createR2Backend,
  createTieredBackend,
  createCOWBackend,
  MemoryFSXBackend,
} from 'dosql/fsx';

// ORM adapters
import { createPrismaAdapter } from 'dosql/orm/prisma';
import { createKyselyAdapter } from 'dosql/orm/kysely';
import { createKnexAdapter } from 'dosql/orm/knex';
import { createDrizzleAdapter } from 'dosql/orm/drizzle';

// Stored procedures
import {
  createProcedureRegistry,
  createProcedureExecutor,
  procedure,
  defineProcedures,
} from 'dosql/proc';
```

| Subpath | Description |
|---------|-------------|
| `dosql` | Main entry point with type-safe SQL parser, factories, and errors |
| `dosql/rpc` | RPC client/server for remote database operations |
| `dosql/wal` | Write-Ahead Log for durability and recovery |
| `dosql/cdc` | Change Data Capture for real-time change streaming |
| `dosql/transaction` | Transaction management utilities |
| `dosql/fsx` | File System Abstraction with tiered storage backends |
| `dosql/proc` | Stored procedures with ESM module support |
| `dosql/orm/prisma` | Prisma ORM adapter |
| `dosql/orm/kysely` | Kysely query builder adapter |
| `dosql/orm/knex` | Knex.js query builder adapter |
| `dosql/orm/drizzle` | Drizzle ORM adapter |

---

## Database Class

The `Database` class provides a better-sqlite3/D1 compatible API for SQL operations.

### Constructor

```typescript
constructor(filename?: string, options?: DatabaseOptions)
```

Creates a new database instance.

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `filename` | `string` | `':memory:'` | Database filename. Use `':memory:'` for in-memory database |
| `options` | `DatabaseOptions` | `{}` | Configuration options |

#### DatabaseOptions

```typescript
interface DatabaseOptions {
  /** Whether to open database in read-only mode */
  readonly?: boolean;

  /** Whether to require the database file to exist */
  fileMustExist?: boolean;

  /** Timeout for acquiring locks (milliseconds) */
  timeout?: number;

  /** Verbose logging function for debugging */
  verbose?: (message?: unknown, ...params: unknown[]) => void;

  /** Maximum size of the statement cache */
  statementCacheSize?: number;
}
```

#### Examples

```typescript
import { createDatabase } from 'dosql';

// In-memory database (default)
const db = createDatabase();

// Named in-memory database
const db = createDatabase(':memory:');

// With options
const db = createDatabase(':memory:', {
  readonly: false,
  timeout: 10000,
  statementCacheSize: 200,
});

// With verbose logging
const db = createDatabase(':memory:', {
  verbose: console.log,
});
```

### Properties

```typescript
class Database {
  /** Database filename (or ':memory:') */
  readonly name: string;

  /** Whether the database is read-only */
  readonly readonly: boolean;

  /** Whether the database connection is open */
  readonly open: boolean;

  /** Whether currently in a transaction */
  readonly inTransaction: boolean;
}
```

---

### Core Methods

#### prepare()

Prepare a SQL statement for execution.

```typescript
prepare<T = unknown, P extends BindParameters = BindParameters>(
  sql: string
): Statement<T, P>
```

##### Type Parameters

| Parameter | Description |
|-----------|-------------|
| `T` | Expected row type for SELECT queries |
| `P` | Parameter types for binding |

##### Examples

```typescript
// Simple SELECT
const stmt = db.prepare('SELECT * FROM users');
const users = stmt.all();

// With positional parameters
const stmt = db.prepare<{ id: number; name: string }>(
  'SELECT id, name FROM users WHERE id = ?'
);
const user = stmt.get(42);

// With named parameters
const stmt = db.prepare<{ id: number; name: string }, { userId: number }>(
  'SELECT id, name FROM users WHERE id = :userId'
);
const user = stmt.get({ userId: 42 });

// INSERT statement
const insertStmt = db.prepare(
  'INSERT INTO users (name, email) VALUES (?, ?)'
);
const result = insertStmt.run('Alice', 'alice@example.com');
console.log(result.lastInsertRowid); // 1
```

---

#### exec()

Execute one or more SQL statements. Does not return results.

```typescript
exec(sql: string): this
```

##### Examples

```typescript
// Create table
db.exec(`
  CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  )
`);

// Multiple statements
db.exec(`
  CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
  CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);
  CREATE INDEX idx_posts_user ON posts(user_id);
`);

// Chaining
db.exec('CREATE TABLE a (id INTEGER)')
  .exec('CREATE TABLE b (id INTEGER)')
  .exec('CREATE TABLE c (id INTEGER)');
```

---

#### close()

Close the database connection and release resources.

```typescript
close(): this
```

---

### Prepared Statements

#### Statement Properties

```typescript
interface Statement<T, P> {
  /** The original SQL string */
  readonly source: string;

  /** Whether the statement is read-only (SELECT, etc.) */
  readonly reader: boolean;

  /** Whether the statement has been finalized */
  readonly finalized: boolean;
}
```

#### Statement Methods

##### bind()

Bind parameters to the statement (chainable).

```typescript
bind(...params: P extends any[] ? P : [P]): this
```

##### run()

Execute the statement and return modification results.

```typescript
run(...params: P extends any[] ? P : [P]): RunResult

interface RunResult {
  /** Number of rows affected by the statement */
  changes: number;

  /** Row ID of the last inserted row (for INSERT statements) */
  lastInsertRowid: number | bigint;
}
```

##### get()

Execute the statement and return the first row.

```typescript
get(...params: P extends any[] ? P : [P]): T | undefined
```

##### all()

Execute the statement and return all matching rows.

```typescript
all(...params: P extends any[] ? P : [P]): T[]
```

##### iterate()

Execute the statement and return an iterator over rows.

```typescript
iterate(...params: P extends any[] ? P : [P]): IterableIterator<T>
```

##### columns()

Get column metadata for the result set.

```typescript
columns(): ColumnInfo[]

interface ColumnInfo {
  /** Column name as defined in the query/table */
  name: string;
  /** Original column name (before AS alias) */
  column: string | null;
  /** Table name the column belongs to (null for expressions) */
  table: string | null;
  /** Database name (always 'main' for single-db) */
  database: string | null;
  /** Declared type from schema (e.g., 'INTEGER', 'TEXT') */
  type: string | null;
}
```

##### finalize()

Release resources associated with the statement.

```typescript
finalize(): void
```

#### Statement Configuration

```typescript
// safeIntegers - Use BigInt for INTEGER columns
const stmt = db.prepare('SELECT id FROM users').safeIntegers();
const ids = stmt.pluck().all();
// ids is bigint[]

// pluck - Return only the first column value
const stmt = db.prepare('SELECT name FROM users').pluck();
const names = stmt.all();
// names is string[] instead of { name: string }[]

// raw - Return arrays instead of objects
const stmt = db.prepare('SELECT id, name FROM users').raw();
const rows = stmt.all();
// rows is [number, string][] instead of { id: number, name: string }[]

// expand - Expand nested objects (for JOINs with table prefixes)
const stmt = db.prepare(`
  SELECT users.id, users.name, posts.title
  FROM users
  JOIN posts ON posts.user_id = users.id
`).expand();
const rows = stmt.all();
// rows have structure: { users: { id, name }, posts: { title } }
```

---

### Transactions

#### transaction()

Create a transaction wrapper function.

```typescript
transaction<F extends (...args: any[]) => any>(fn: F): TransactionFunction<F>

interface TransactionFunction<F> {
  (...args: Parameters<F>): ReturnType<F>;
  deferred(...args: Parameters<F>): ReturnType<F>;
  immediate(...args: Parameters<F>): ReturnType<F>;
  exclusive(...args: Parameters<F>): ReturnType<F>;
}
```

##### Transaction Modes

| Mode | Description |
|------|-------------|
| `deferred` | Locks acquired when needed (default) |
| `immediate` | Acquires write lock immediately |
| `exclusive` | Full exclusive access to database |

##### Examples

```typescript
// Basic transaction
const transfer = db.transaction((from: number, to: number, amount: number) => {
  const fromBalance = db.prepare('SELECT balance FROM accounts WHERE id = ?').pluck().get(from);
  if (fromBalance < amount) {
    throw new Error('Insufficient funds');
  }
  db.prepare('UPDATE accounts SET balance = balance - ? WHERE id = ?').run(amount, from);
  db.prepare('UPDATE accounts SET balance = balance + ? WHERE id = ?').run(amount, to);
  return { success: true };
});

// Execute the transaction
const result = transfer(1, 2, 100);

// Using different modes
const bulkInsert = db.transaction((items: Item[]) => {
  const insert = db.prepare('INSERT INTO items (name, value) VALUES (?, ?)');
  for (const item of items) {
    insert.run(item.name, item.value);
  }
});

bulkInsert(items);             // Deferred mode (default)
bulkInsert.immediate(items);   // Gets write lock immediately
bulkInsert.exclusive(items);   // Full exclusive access
```

---

### Savepoints

Savepoints allow partial rollback within a transaction.

#### savepoint()

```typescript
savepoint(name: string): this
```

#### release()

```typescript
release(name: string): this
```

#### rollback()

```typescript
rollback(name?: string): this
```

##### Examples

```typescript
// Basic savepoint usage
db.savepoint('sp1');
db.prepare('INSERT INTO users (name) VALUES (?)').run('Alice');
db.release('sp1'); // Commits the savepoint

// Rollback to savepoint
db.savepoint('sp1');
db.prepare('INSERT INTO users (name) VALUES (?)').run('Alice');
db.rollback('sp1'); // Undoes the insert

// Try-catch pattern with savepoints
const processItems = db.transaction((items: Item[]) => {
  for (const item of items) {
    db.savepoint('item');
    try {
      processItem(item);
      db.release('item');
    } catch {
      db.rollback('item');
    }
  }
});
```

##### Savepoint Constraints

- Maximum nesting depth: 32
- Names must be unique among active savepoints
- Must be released/rolled back in LIFO order
- Only valid within an active transaction

---

### Batch Operations

Execute multiple queries in a single request with configurable execution semantics.

#### batch()

```typescript
batch(request: BatchRequest): Promise<BatchResponse>

interface BatchRequest {
  /** Array of queries to execute */
  queries: QueryRequest[];
  /** Whether to execute in a single transaction */
  atomic?: boolean;
  /** Whether to continue on error */
  continueOnError?: boolean;
  /** Branch for all queries */
  branch?: string;
}

interface BatchResponse {
  /** Results for each query (in order) */
  results: Array<QueryResponse | BatchError>;
  /** Number of successful queries */
  successCount: number;
  /** Number of failed queries */
  errorCount: number;
  /** Total execution time */
  executionTimeMs: number;
  /** Final LSN after batch */
  lsn: bigint;
}
```

#### Atomic vs ContinueOnError

| `atomic` | `continueOnError` | Behavior | Use Case |
|----------|-------------------|----------|----------|
| `false` | `false` | Stop on first error, no rollback | Fast-fail, partial results acceptable |
| `false` | `true` | Continue on errors, no rollback | Best-effort execution |
| `true` | `false` | Stop on first error, rollback all | All-or-nothing consistency |
| `true` | `true` | Continue on errors, rollback all on any error | Validate all, rollback if any fail |

---

### PRAGMA Commands

#### pragma()

```typescript
pragma<N extends PragmaName>(name: N, value?: SqlValue): PragmaResult<N>

type PragmaName =
  | 'journal_mode'
  | 'synchronous'
  | 'foreign_keys'
  | 'cache_size'
  | 'page_size'
  | 'auto_vacuum'
  | 'busy_timeout'
  | 'wal_checkpoint'
  | 'table_info'
  | 'index_list'
  | 'database_list'
  | 'compile_options'
  | 'user_version'
  | 'application_id'
  | 'integrity_check'
  | 'quick_check'
  | string;
```

##### Examples

```typescript
// Get/set journal mode
const mode = db.pragma('journal_mode');

// Enable foreign keys
db.pragma('foreign_keys', 1);

// Get table info
const columns = db.pragma('table_info', 'users');

// Integrity check
const result = db.pragma('integrity_check');
```

---

### User-Defined Functions

#### function()

Register a user-defined scalar function.

```typescript
function(name: string, fn: (...args: SqlValue[]) => SqlValue): this
```

##### Examples

```typescript
// Simple function
db.function('double', (x) => Number(x) * 2);
const result = db.prepare('SELECT double(5)').pluck().get(); // 10

// String function
db.function('reverse', (str) => {
  return String(str).split('').reverse().join('');
});

// Multi-argument function
db.function('clamp', (value, min, max) => {
  const v = Number(value);
  return Math.max(Number(min), Math.min(Number(max), v));
});
```

#### aggregate()

Register a user-defined aggregate function.

```typescript
aggregate<TAccumulator = unknown>(
  name: string,
  options: AggregateOptions<TAccumulator>
): this

interface AggregateOptions<TAccumulator> {
  /** Called for each row to update the accumulator state */
  step: (accumulator: TAccumulator, ...values: SqlValue[]) => void;

  /** Called to finalize and return the aggregate result */
  result: (accumulator: TAccumulator) => SqlValue;

  /** Initial accumulator value */
  start?: TAccumulator;

  /** Optional inverse function for window functions */
  inverse?: (accumulator: TAccumulator, ...values: SqlValue[]) => void;
}
```

##### Examples

```typescript
// Custom sum
db.aggregate<number>('my_sum', {
  start: 0,
  step: (acc, value) => acc + Number(value),
  result: (acc) => acc,
});

// Median
db.aggregate<number[]>('median', {
  start: [],
  step: (acc, value) => {
    if (value !== null) acc.push(Number(value));
  },
  result: (acc) => {
    if (acc.length === 0) return null;
    acc.sort((a, b) => a - b);
    const mid = Math.floor(acc.length / 2);
    return acc.length % 2 === 0
      ? (acc[mid - 1] + acc[mid]) / 2
      : acc[mid];
  },
});
```

---

## Type System

### SQL Value Types

```typescript
type SqlValue =
  | string      // TEXT
  | number      // INTEGER, REAL
  | bigint      // INTEGER (64-bit)
  | boolean     // INTEGER (0 or 1)
  | null        // NULL
  | Uint8Array  // BLOB
  | Date;       // TEXT (ISO 8601 format)
```

### Parameter Binding

```typescript
type NamedParameters = Record<string, SqlValue>;
type PositionalParameters = SqlValue[];
type BindParameters = NamedParameters | PositionalParameters;
```

#### Examples

```typescript
// Positional parameters with ?
db.prepare('SELECT * FROM users WHERE id = ? AND active = ?').all(42, true);

// Positional parameters with ?NNN
db.prepare('SELECT * FROM users WHERE id = ?1 AND role = ?2').all(42, 'admin');

// Named parameters with :name
db.prepare('SELECT * FROM users WHERE id = :id').get({ id: 42 });

// Named parameters with @name
db.prepare('SELECT * FROM users WHERE id = @id').get({ id: 42 });

// Named parameters with $name
db.prepare('SELECT * FROM users WHERE id = $id').get({ id: 42 });
```

### Result Types

```typescript
interface RunResult {
  /** Number of rows affected by the statement */
  changes: number;

  /** Row ID of the last inserted row (for INSERT statements) */
  lastInsertRowid: number | bigint;
}

interface ColumnInfo {
  name: string;
  column: string | null;
  table: string | null;
  database: string | null;
  type: string | null;
}
```

---

## Error Handling

### Error Hierarchy

All DoSQL errors extend the base `DoSQLError` class.

```typescript
abstract class DoSQLError extends Error {
  /** Machine-readable error code */
  abstract readonly code: string;

  /** Error category for consistent handling */
  abstract readonly category: ErrorCategory;

  /** Timestamp when error occurred */
  readonly timestamp: number;

  /** Error context */
  context?: ErrorContext;

  /** Recovery hint for developers */
  recoveryHint?: string;

  /** Check if this error is retryable */
  isRetryable(): boolean;

  /** Get a user-friendly error message */
  toUserMessage(): string;

  /** Serialize error for RPC/API responses */
  toJSON(): SerializedError;

  /** Format error for structured logging */
  toLogEntry(): ErrorLogEntry;

  /** Create error with additional context */
  withContext(context: ErrorContext): this;

  /** Set recovery hint */
  withRecoveryHint(hint: string): this;
}

enum ErrorCategory {
  CONNECTION = 'CONNECTION',
  EXECUTION = 'EXECUTION',
  VALIDATION = 'VALIDATION',
  RESOURCE = 'RESOURCE',
  CONFLICT = 'CONFLICT',
  TIMEOUT = 'TIMEOUT',
  INTERNAL = 'INTERNAL',
}
```

### Error Codes

#### DatabaseErrorCode

```typescript
enum DatabaseErrorCode {
  CLOSED = 'DB_CLOSED',
  READ_ONLY = 'DB_READ_ONLY',
  NOT_FOUND = 'DB_NOT_FOUND',
  CONSTRAINT_VIOLATION = 'DB_CONSTRAINT',
  QUERY_ERROR = 'DB_QUERY_ERROR',
  CONNECTION_FAILED = 'DB_CONNECTION_FAILED',
  CONFIG_ERROR = 'DB_CONFIG_ERROR',
  TIMEOUT = 'DB_TIMEOUT',
  INTERNAL = 'DB_INTERNAL',
}
```

#### StatementErrorCode

```typescript
enum StatementErrorCode {
  FINALIZED = 'STMT_FINALIZED',
  SYNTAX_ERROR = 'STMT_SYNTAX',
  EXECUTION_ERROR = 'STMT_EXECUTION',
  TABLE_NOT_FOUND = 'STMT_TABLE_NOT_FOUND',
  COLUMN_NOT_FOUND = 'STMT_COLUMN_NOT_FOUND',
  INVALID_SQL = 'STMT_INVALID_SQL',
  UNSUPPORTED = 'STMT_UNSUPPORTED',
}
```

#### BindingErrorCode

```typescript
enum BindingErrorCode {
  MISSING_PARAM = 'BIND_MISSING_PARAM',
  TYPE_MISMATCH = 'BIND_TYPE_MISMATCH',
  INVALID_TYPE = 'BIND_INVALID_TYPE',
  COUNT_MISMATCH = 'BIND_COUNT_MISMATCH',
  NAMED_EXPECTED = 'BIND_NAMED_EXPECTED',
}
```

### Error Handling Patterns

```typescript
import {
  DoSQLError,
  DatabaseError,
  StatementError,
  BindingError,
  ErrorCategory,
} from 'dosql';

// Basic error handling
try {
  db.prepare('SELECT * FROM nonexistent').all();
} catch (error) {
  if (error instanceof DoSQLError) {
    console.log('Code:', error.code);
    console.log('Category:', error.category);
    console.log('Retryable:', error.isRetryable());
  }
}

// Handle by category
try {
  await someOperation();
} catch (error) {
  if (error instanceof DoSQLError) {
    switch (error.category) {
      case ErrorCategory.CONNECTION:
        console.log('Connection issue - retry later');
        break;
      case ErrorCategory.VALIDATION:
        console.log('Invalid input:', error.toUserMessage());
        break;
      case ErrorCategory.TIMEOUT:
        console.log('Operation timed out');
        break;
    }
  }
}

// Retry pattern
async function executeWithRetry<T>(
  fn: () => Promise<T>,
  maxRetries = 3
): Promise<T> {
  let lastError: Error | undefined;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error as Error;
      if (error instanceof DoSQLError && !error.isRetryable()) {
        throw error;
      }
      await new Promise(r => setTimeout(r, 100 * Math.pow(2, attempt)));
    }
  }

  throw lastError;
}
```

---

## RPC API

The RPC module provides client/server communication for remote database access using CapnWeb.

### Client Creation

#### createWebSocketClient()

Create a DoSQL client using WebSocket transport (recommended for streaming and CDC).

```typescript
import { createWebSocketClient, type ConnectionOptions } from 'dosql/rpc';

interface ConnectionOptions {
  /** WebSocket URL for the RPC endpoint */
  url: string;
  /** Default branch for all queries */
  defaultBranch?: string;
  /** Connection timeout in milliseconds */
  connectTimeoutMs?: number;
  /** Query timeout in milliseconds */
  queryTimeoutMs?: number;
  /** Auto-reconnect on disconnect */
  autoReconnect?: boolean;
  /** Max reconnect attempts */
  maxReconnectAttempts?: number;
  /** Delay between reconnect attempts in ms */
  reconnectDelayMs?: number;
}

const client = createWebSocketClient({
  url: 'wss://dosql.example.com/rpc',
  defaultBranch: 'main',
  autoReconnect: true,
});
```

#### createHttpClient()

Create a DoSQL client using HTTP batch transport (for stateless requests).

```typescript
import { createHttpClient } from 'dosql/rpc';

const client = createHttpClient({
  url: 'https://dosql.example.com/rpc',
  defaultBranch: 'main',
});
```

### Query Operations

```typescript
interface QueryRequest {
  /** SQL query string */
  sql: string;
  /** Positional parameters */
  params?: unknown[];
  /** Named parameters */
  namedParams?: Record<string, unknown>;
  /** Branch for query isolation */
  branch?: string;
  /** Time travel LSN */
  asOf?: bigint;
  /** Query timeout in ms */
  timeoutMs?: number;
  /** Max rows to return */
  limit?: number;
  /** Offset for pagination */
  offset?: number;
}

interface QueryResponse {
  /** Column names */
  columns: string[];
  /** Column types */
  columnTypes: ColumnType[];
  /** Result rows */
  rows: unknown[][];
  /** Number of rows */
  rowCount: number;
  /** Current LSN */
  lsn: bigint;
  /** Execution time in ms */
  executionTimeMs: number;
  /** Whether more rows exist */
  hasMore: boolean;
}

const result = await client.query({
  sql: 'SELECT id, name FROM users WHERE active = $1',
  params: [true],
  limit: 100,
});
```

### Streaming Queries

```typescript
interface StreamRequest {
  sql: string;
  params?: unknown[];
  chunkSize?: number;
  maxRows?: number;
  branch?: string;
}

interface StreamChunk {
  chunkIndex: number;
  rows: unknown[][];
  rowCount: number;
  isLast: boolean;
  totalRowsSoFar: number;
}

for await (const chunk of client.queryStream({
  sql: 'SELECT * FROM events WHERE timestamp > $1',
  params: ['2024-01-01'],
  chunkSize: 1000,
})) {
  processRows(chunk.rows);
}
```

### Transaction Operations

```typescript
import { withTransaction } from 'dosql/rpc';

interface BeginTransactionRequest {
  isolation?: 'READ_COMMITTED' | 'REPEATABLE_READ' | 'SERIALIZABLE';
  timeoutMs?: number;
  branch?: string;
  readOnly?: boolean;
}

// Using the convenience wrapper
const result = await withTransaction(client, async (tx) => {
  await tx.query({ sql: 'INSERT INTO users (name) VALUES ($1)', params: ['Alice'] });
  await tx.query({ sql: 'INSERT INTO logs (action) VALUES ($1)', params: ['user_created'] });
  return { success: true };
});
```

### Schema Operations

```typescript
interface SchemaRequest {
  tables?: string[];
  branch?: string;
  includeIndexes?: boolean;
  includeForeignKeys?: boolean;
}

interface SchemaResponse {
  tables: TableSchema[];
  version: number;
  lastModifiedLSN: bigint;
}

const schema = await client.getSchema({
  tables: ['users', 'posts'],
  includeIndexes: true,
});
```

### Connection Management

```typescript
interface ConnectionStats {
  connected: boolean;
  connectionId?: string;
  branch?: string;
  currentLSN?: bigint;
  latencyMs?: number;
  messagesSent: number;
  messagesReceived: number;
  reconnectCount: number;
}

const pong = await client.ping();
const stats = client.getConnectionStats();
client.close();
```

### Server Implementation

```typescript
import { DoSQLTarget, handleDoSQLRequest, type QueryExecutor } from 'dosql/rpc';

export class DoSQLDurableObject implements DurableObject {
  private target: DoSQLTarget;

  constructor(ctx: DurableObjectState, env: Env) {
    const executor = new MyQueryExecutor(ctx);
    this.target = new DoSQLTarget(executor, undefined, {
      streamTTLMs: 30 * 60 * 1000,
      maxConcurrentStreams: 100,
      onScheduleAlarm: (delayMs) => ctx.storage.setAlarm(Date.now() + delayMs),
    });
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === '/rpc') {
      return handleDoSQLRequest(request, this.target);
    }
    return new Response('Not Found', { status: 404 });
  }
}
```

---

## WAL (Write-Ahead Log)

The WAL module provides durability through a write-ahead log.

### WAL Writer

```typescript
import {
  createWALWriter,
  WALTransaction,
  createTransaction,
  generateTxnId,
  type WALWriter,
  type WALEntry,
  type WALConfig,
} from 'dosql/wal';

interface WALConfig {
  /** Maximum segment size in bytes */
  maxSegmentSize?: number;
  /** Flush interval in ms */
  flushIntervalMs?: number;
  /** Enable CRC32 checksums */
  enableChecksums?: boolean;
}

interface WALEntry {
  timestamp: number;
  txnId: string;
  op: 'INSERT' | 'UPDATE' | 'DELETE' | 'TRUNCATE' | 'DDL';
  table: string;
  before?: Uint8Array;
  after?: Uint8Array;
  pk?: Uint8Array;
  branch?: string;
}

const writer = createWALWriter(backend, {
  maxSegmentSize: 2 * 1024 * 1024,
  enableChecksums: true,
});

const result = await writer.append({
  timestamp: Date.now(),
  txnId: generateTxnId(),
  op: 'INSERT',
  table: 'users',
  after: new TextEncoder().encode(JSON.stringify({ id: 1, name: 'Alice' })),
});

await writer.flush();
```

### WAL Reader

```typescript
import {
  createWALReader,
  tailWAL,
  readWALBatched,
  reconstructTransactions,
} from 'dosql/wal';

const reader = createWALReader(backend);

// Read all entries from LSN
for await (const entry of reader.iterate({ fromLSN: 0n })) {
  console.log(entry.op, entry.table);
}

// Read in batches
for await (const batch of readWALBatched(reader, { batchSize: 100 })) {
  processBatch(batch);
}

// Tail the WAL (follow new entries)
for await (const entry of tailWAL(reader, { fromLSN: lastLSN })) {
  handleNewEntry(entry);
}
```

### Checkpoint Management

```typescript
import {
  createCheckpointManager,
  performRecovery,
  needsRecovery,
  createAutoCheckpointer,
} from 'dosql/wal';

const checkpointManager = createCheckpointManager(storage, writer);

const checkpoint = await checkpointManager.createCheckpoint();

if (await needsRecovery(storage)) {
  const state = await performRecovery(storage, writer, reader);
  console.log(`Recovered ${state.entriesReplayed} entries`);
}

const autoCheckpointer = createAutoCheckpointer(checkpointManager, {
  intervalMs: 60000,
  maxEntries: 10000,
  maxSizeBytes: 50 * 1024 * 1024,
});
autoCheckpointer.start();
```

### WAL Retention

```typescript
import {
  createWALRetentionManager,
  RETENTION_PRESETS,
  type RetentionPolicy,
} from 'dosql/wal';

interface RetentionPolicy {
  maxAgeMs?: number;
  maxEntries?: number;
  maxSizeBytes?: number;
  minEntriesToKeep?: number;
  cleanupSchedule?: string;
}

const retention = createWALRetentionManager(backend, writer, {
  policy: {
    maxAgeMs: 7 * 24 * 60 * 60 * 1000,
    maxSizeBytes: 1024 * 1024 * 1024,
    minEntriesToKeep: 1000,
  },
});

const check = await retention.checkRetention();
const result = await retention.cleanup();

// Use presets
const retention = createWALRetentionManager(backend, writer, {
  policy: RETENTION_PRESETS.production,
});
```

---

## CDC (Change Data Capture)

The CDC module provides real-time streaming of database changes.

### Subscriptions

```typescript
import {
  createCDC,
  createCDCSubscription,
  subscribeTable,
  subscribeBatched,
  type CDCFilter,
} from 'dosql/cdc';

interface CDCFilter {
  tables?: string[];
  operations?: ('INSERT' | 'UPDATE' | 'DELETE' | 'TRUNCATE')[];
  predicate?: (entry: WALEntry) => boolean;
}

const cdc = createCDC(backend);
const subscription = createCDCSubscription(backend);

// Subscribe to all changes
for await (const entry of subscription.subscribe(0n)) {
  console.log('Change:', entry.op, entry.table);
}

// Subscribe with filters
for await (const event of subscription.subscribeChanges(0n, {
  tables: ['users'],
  operations: ['INSERT', 'UPDATE']
}, JSON.parse)) {
  if (event.type === 'insert') {
    console.log('New user:', event.data);
  }
}

// Batched subscription for throughput
for await (const batch of subscribeBatched(cdc, { batchSize: 100 })) {
  await processBatch(batch);
}
```

### Change Events

```typescript
interface ChangeEvent<T = unknown> {
  type: 'insert' | 'update' | 'delete' | 'truncate';
  table: string;
  lsn: bigint;
  timestamp: number;
  txnId: string;
  data?: T;
  oldData?: T;
  pk?: unknown;
  branch?: string;
}
```

### Replication Slots

```typescript
import {
  createReplicationSlotManager,
  type ReplicationSlot,
} from 'dosql/cdc';

interface ReplicationSlot {
  name: string;
  confirmedLSN: bigint;
  createdAt: number;
  lastActiveAt: number;
  metadata?: Record<string, unknown>;
}

const slots = createReplicationSlotManager(storage);

await slots.createSlot('my-consumer', 0n, {
  metadata: { version: '1.0' },
});

const subscription = await slots.subscribeFromSlot('my-consumer');
for await (const entry of subscription) {
  await processEntry(entry);
  await slots.updateSlot('my-consumer', entry.lsn);
}
```

### Lakehouse Streaming

```typescript
import {
  createLakehouseStreamer,
  type LakehouseStreamConfig,
} from 'dosql/cdc';

interface LakehouseStreamConfig {
  targetUrl: string;
  batchSize: number;
  flushIntervalMs: number;
  retry: RetryConfig;
  checkpointIntervalMs: number;
}

const streamer = createLakehouseStreamer(cdc, {
  targetUrl: 'iceberg://my-catalog/database/table',
  batchSize: 1000,
  flushIntervalMs: 5000,
});

await streamer.start({ fromLSN: 0n });
const status = streamer.getStatus();
await streamer.stop();
```

---

## Transaction Management

The transaction module provides SQLite-compatible transaction semantics with MVCC support.

### Transaction Manager

```typescript
import {
  createTransactionManager,
  executeInTransaction,
  executeWithSavepoint,
  executeReadOnly,
  type TransactionManagerOptions,
} from 'dosql/transaction';

interface TransactionManagerOptions {
  walWriter?: WALWriter;
  lockManager?: LockManager;
  mvccStore?: MVCCStore;
  defaultIsolation?: IsolationLevel;
  defaultTimeout?: number;
}

const manager = createTransactionManager({ walWriter });

// Execute in transaction
const result = await executeInTransaction(manager, async (ctx) => {
  ctx.logOperation({ op: 'INSERT', table: 'users', afterValue: data });
  return { success: true };
});

// Execute with savepoint
await executeWithSavepoint(manager, 'sp1', async (ctx) => {
  // Operations that can be rolled back independently
});

// Read-only transaction
const data = await executeReadOnly(manager, async (ctx) => {
  return ctx.query('SELECT * FROM users');
});
```

### Isolation Levels

```typescript
enum IsolationLevel {
  READ_UNCOMMITTED = 'READ_UNCOMMITTED',
  READ_COMMITTED = 'READ_COMMITTED',
  REPEATABLE_READ = 'REPEATABLE_READ',
  SERIALIZABLE = 'SERIALIZABLE',
}
```

### MVCC Support

```typescript
import {
  createLockManager,
  createMVCCStore,
  createIsolationEnforcer,
  createSnapshot,
  isVersionVisible,
  type LockManager,
  type MVCCStore,
} from 'dosql/transaction';

const lockManager = createLockManager({
  deadlockDetection: true,
  lockTimeoutMs: 5000,
});

const mvccStore = createMVCCStore();

const isolationEnforcer = createIsolationEnforcer({
  lockManager,
  mvccStore,
  level: IsolationLevel.SERIALIZABLE,
});
```

---

## Branching

DoSQL provides git-like branching for database versioning.

### Branch Manager

```typescript
import {
  createBranchManager,
  type BranchManager,
  type BranchMetadata,
} from 'dosql'; // Re-exported from main package

interface BranchMetadata {
  name: string;
  parent?: string;
  createdAtLSN: bigint;
  createdAt: number;
  headLSN: bigint;
  author?: AuthorInfo;
  description?: string;
}

const branchManager = createBranchManager(storage, {
  defaultBranch: 'main',
  protectedBranches: ['main', 'production'],
});
```

### Branch Operations

```typescript
// Create a new branch
const branch = await branchManager.createBranch({
  name: 'feature/new-users',
  fromBranch: 'main',
  author: { name: 'Alice', email: 'alice@example.com' },
});

// List branches
const branches = await branchManager.listBranches();

// Checkout a branch
await branchManager.checkout('feature/new-users');

// Compare branches
const diff = await branchManager.compare('main', 'feature/new-users');

// Delete a branch
await branchManager.deleteBranch('feature/new-users');
```

### Merge Operations

```typescript
type MergeStrategy = 'fast-forward' | 'merge' | 'squash' | 'rebase';

interface MergeResult {
  success: boolean;
  commitId?: string;
  conflicts?: MergeConflict[];
  strategy: MergeStrategy;
}

const result = await branchManager.merge({
  source: 'feature/new-users',
  target: 'main',
  strategy: 'merge',
  message: 'Merge feature/new-users into main',
});
```

---

## Migrations

### Migration Runner

```typescript
import {
  createMigrationRunner,
  createMigrations,
  type Migration,
} from 'dosql'; // Re-exported from main package

interface Migration {
  id: string;
  sql: string;
  down?: string;
  description?: string;
  checksum?: string;
}

const migrations = createMigrations([
  {
    id: '20240101000000_init',
    sql: `
      CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
      )
    `,
    down: 'DROP TABLE users',
  },
]);

const runner = createMigrationRunner(db, storage, {
  table: '_migrations',
  dryRun: false,
});

await runner.up();
await runner.down(1);
const status = await runner.status();
```

### Schema Tracker

```typescript
import {
  createSchemaTracker,
  initializeWithMigrations,
} from 'dosql';

const tracker = createSchemaTracker(storage, {
  snapshotInterval: 10,
});

const status = await initializeWithMigrations({
  migrations,
  storage: ctx.storage,
  db: database,
  autoMigrate: true,
});
```

### Drizzle Compatibility

```typescript
import { loadDrizzleMigrations, toDoSqlMigration } from 'dosql';

const migrations = await loadDrizzleMigrations('./drizzle', {
  validateChecksums: true,
});
```

---

## Virtual Tables

### URL Table Sources

Query data directly from URLs using ClickHouse-style syntax.

```typescript
import {
  createResolver,
  resolveSource,
  UrlTableSource,
  createUrlSource,
  type Format,
} from 'dosql';

type Format = 'json' | 'ndjson' | 'csv' | 'parquet';

const resolver = createResolver({ fetch: globalThis.fetch });

// Query JSON from URL
const source = await resolveSource(
  "SELECT * FROM 'https://api.example.com/users.json'",
  resolver
);

for await (const row of source.scan()) {
  console.log(row);
}

// Create URL source directly
const urlSource = createUrlSource('https://api.example.com/data.csv', {
  format: 'csv',
  headers: { Authorization: 'Bearer token' },
});
```

### R2 Sources

```typescript
import {
  createR2Source,
  parseR2Uri,
  listR2Objects,
} from 'dosql';

const source = createR2Source(r2Bucket, 'data/users.parquet');

for await (const row of source.scan({
  columns: ['id', 'name', 'email'],
  filter: { column: 'active', op: '=', value: true },
})) {
  console.log(row);
}

const { bucket, key } = parseR2Uri('r2://mybucket/path/to/file.parquet');
```

---

## FSX (File System Abstraction)

### Storage Backends

```typescript
import {
  createDOBackend,
  createR2Backend,
  createMemoryBackend,
  type FSXBackend,
} from 'dosql/fsx';

interface FSXBackend {
  read(path: string, range?: [number, number]): Promise<Uint8Array | null>;
  write(path: string, data: Uint8Array): Promise<void>;
  delete(path: string): Promise<void>;
  list(prefix: string): Promise<string[]>;
  exists(path: string): Promise<boolean>;
}

const doBackend = createDOBackend(ctx.storage);
const r2Backend = createR2Backend(env.MY_BUCKET, { keyPrefix: 'data/' });
const memoryBackend = createMemoryBackend();
```

### Tiered Storage

```typescript
import {
  createTieredBackend,
  StorageTier,
  type TieredStorageConfig,
} from 'dosql/fsx';

enum StorageTier {
  HOT = 'hot',
  COLD = 'cold',
  BOTH = 'both',
}

interface TieredStorageConfig {
  hotDataMaxAge: number;
  hotStorageMaxSize: number;
  autoMigrate: boolean;
  readHotFirst: boolean;
  cacheR2Reads: boolean;
  maxHotFileSize: number;
}

const tiered = createTieredBackend(hot, cold, {
  hotDataMaxAge: 30 * 60 * 1000,
  hotStorageMaxSize: 50 * 1024 * 1024,
  autoMigrate: true,
});

await tiered.write('data.bin', myData);
const data = await tiered.read('data.bin');

await tiered.migrateToR2({ olderThan: 60 * 60 * 1000 });
await tiered.promoteToHot(['important-data.bin']);
```

### Copy-on-Write Backend

```typescript
import {
  createCOWBackend,
  type Snapshot,
  type MergeResult,
} from 'dosql/fsx';

const cow = createCOWBackend(underlying, {
  enableSnapshots: true,
  gcIntervalMs: 60000,
});

const snapshot1 = await cow.createSnapshot('v1.0');
await cow.write('data.bin', newData);
const snapshot2 = await cow.createSnapshot('v1.1');

await cow.restore(snapshot1);
await cow.createBranch('feature', { fromSnapshot: snapshot1 });
await cow.checkout('feature');

const result = await cow.merge({ source: 'feature', target: 'main' });
const gcResult = await cow.gc({ dryRun: false });
```

---

## Stored Procedures

DoSQL supports ESM-based stored procedures with PL/pgSQL-like semantics.

### Procedure Registry

```typescript
import {
  createProcedureRegistry,
  createProcedureExecutor,
  createInMemoryCatalogStorage,
  parseProcedure,
} from 'dosql/proc';

const registry = createProcedureRegistry({
  storage: createInMemoryCatalogStorage(),
});

// Register via SQL
const parsed = parseProcedure(`
  CREATE PROCEDURE calculate_total AS MODULE $$
    export default async ({ db }, userId) => {
      const orders = await db.orders.where({ userId });
      return orders.reduce((sum, o) => sum + o.total, 0);
    }
  $$;
`);
await registry.register({
  name: parsed.name,
  code: parsed.code,
});

const executor = createProcedureExecutor({ db, registry });
const result = await executor.call('calculate_total', [userId]);
```

### Procedure Builder

```typescript
import { procedure, ProcedureBuilder } from 'dosql/proc';

const createUser = procedure('createUser')
  .input<{ name: string; email: string }>()
  .output<{ id: number; created: boolean }>()
  .handler(async (ctx, input) => {
    const result = await ctx.db.sql`
      INSERT INTO users (name, email)
      VALUES (${input.name}, ${input.email})
    `;
    return {
      id: Number(result.lastInsertRowid),
      created: true,
    };
  })
  .build();

await registry.register(createUser);
```

### Functional API

```typescript
import {
  defineProcedures,
  defineProcedure,
  withValidation,
  withRetry,
} from 'dosql/proc';

const procs = defineProcedures(dbContext, {
  getUser: defineProcedure(async (ctx, userId: number) => {
    return ctx.db.users.find(userId);
  }),

  updateEmail: defineProcedure(
    withValidation(
      { userId: 'number', email: 'string' },
      withRetry(
        { maxAttempts: 3 },
        async (ctx, userId: number, email: string) => {
          return ctx.db.users.update(userId, { email });
        }
      )
    )
  ),
});

const user = await procs.getUser(123);
```

---

## Sharding

DoSQL provides Vitess-inspired native sharding with real SQL parsing and cost-based query routing.

### VSchema Definition

```typescript
import {
  createVSchema,
  shardedTable,
  referenceTable,
  hashVindex,
  consistentHashVindex,
  rangeVindex,
  shard,
  replica,
  createShardId,
} from 'dosql';

const vschema = createVSchema({
  users: shardedTable('tenant_id', hashVindex()),
  orders: shardedTable('order_id', consistentHashVindex(150)),
  countries: referenceTable(),
}, [
  shard(createShardId('shard-1'), 'user-do', {
    replicas: [
      replica('replica-1a', 'user-do-replica', 'replica', { region: 'us-west' }),
    ],
  }),
  shard(createShardId('shard-2'), 'user-do'),
]);
```

### Vindex Types

```typescript
// Hash Vindex - even distribution
hashVindex()

// Consistent Hash - better for rebalancing
consistentHashVindex(virtualNodes?: number)

// Range Vindex - for range queries
rangeVindex(ranges: RangeConfig[])
```

### Query Routing

```typescript
import {
  createShardingClient,
  createRouter,
  createExecutor,
  type ShardingClient,
} from 'dosql';

const client = createShardingClient({
  vschema,
  rpc: myShardRPC,
  currentRegion: request.cf?.colo,
  executor: {
    maxParallelShards: 16,
    defaultTimeoutMs: 5000,
  },
});

// Automatically routed to correct shard
const result = await client.query(
  'SELECT * FROM users WHERE tenant_id = $1',
  [123]
);

// Stream large results across shards
for await (const row of client.queryStream('SELECT * FROM users')) {
  processRow(row);
}
```

---

## Columnar Storage

DoSQL includes a columnar OLAP storage engine optimized for analytics queries.

### Encoding Types

```typescript
type Encoding = 'raw' | 'dict' | 'rle' | 'delta' | 'bitpack';
```

| Encoding | Best For |
|----------|----------|
| `raw` | Random/high-entropy data, floats |
| `dict` | Low-cardinality strings |
| `rle` | Consecutive repeated values |
| `delta` | Sorted/sequential integers |
| `bitpack` | Small integers with limited range |

### Automatic Encoding Selection

```typescript
import { analyzeForEncoding, type EncodingAnalysis } from 'dosql';

const values = ['active', 'active', 'inactive', 'active'];
const analysis = analyzeForEncoding(values, 'string');

// {
//   recommendedEncoding: 'dict',
//   estimatedSize: 45,
//   cardinality: 2,
// }
```

### Column Statistics (Zone Maps)

```typescript
interface ColumnStats {
  min: number | bigint | string | null;
  max: number | bigint | string | null;
  nullCount: number;
  distinctCount?: number;
  sum?: number | bigint;
}
```

### Writer Usage

```typescript
import {
  ColumnarWriter,
  writeColumnar,
  inferSchema,
  type ColumnarTableSchema,
} from 'dosql';

const schema: ColumnarTableSchema = {
  tableName: 'users',
  columns: [
    { name: 'id', dataType: 'int64', nullable: false },
    { name: 'name', dataType: 'string', nullable: false },
    { name: 'age', dataType: 'int32', nullable: true },
  ],
  primaryKey: ['id'],
};

const writer = new ColumnarWriter(schema, {
  targetRowsPerGroup: 65536,
  forceEncoding: new Map([['status', 'dict']]),
}, fsx);

await writer.write(rows);
await writer.finalize();
```

### Reader Usage

```typescript
import {
  ColumnarReader,
  mightMatchPredicates,
  type Predicate,
} from 'dosql';

const reader = new ColumnarReader(fsx, {
  enablePredicatePushdown: true,
  parallelScan: 4,
});

const result = await reader.read({
  table: 'users',
  projection: { columns: ['id', 'name'] },
  predicates: [
    { column: 'age', op: 'ge', value: 18 },
    { column: 'age', op: 'lt', value: 65 },
  ],
  limit: 100,
});
```

### Data Types

```typescript
type ColumnDataType =
  | 'int8' | 'int16' | 'int32' | 'int64'
  | 'uint8' | 'uint16' | 'uint32' | 'uint64'
  | 'float32' | 'float64'
  | 'boolean'
  | 'string'
  | 'bytes'
  | 'timestamp';
```

### Predicate Operations

```typescript
type PredicateOp = 'eq' | 'ne' | 'lt' | 'le' | 'gt' | 'ge' | 'in' | 'between';

const predicates: Predicate[] = [
  { column: 'status', op: 'eq', value: 'active' },
  { column: 'age', op: 'ge', value: 18 },
  { column: 'price', op: 'between', value: 10, value2: 100 },
  { column: 'category', op: 'in', value: ['electronics', 'clothing'] },
];
```

---

## Related Guides

- [Getting Started](./getting-started.md) - Quick start guide for DoSQL
- [Advanced Features](./advanced.md) - Time travel, branching, CDC
- [Architecture](./architecture.md) - Understanding DoSQL internals
- [Troubleshooting](./TROUBLESHOOTING.md) - Common issues and solutions
