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
- [WAL (Write-Ahead Log)](#wal-write-ahead-log)
  - [WAL Writer](#wal-writer)
  - [WAL Reader](#wal-reader)
  - [Checkpoint Management](#checkpoint-management)
- [CDC (Change Data Capture)](#cdc-change-data-capture)
  - [Subscriptions](#subscriptions)
  - [Replication Slots](#replication-slots)
- [Virtual Tables](#virtual-tables)
  - [URL Table Sources](#url-table-sources)
  - [R2 Sources](#r2-sources)
- [Advanced Features](#advanced-features)
  - [FSX Tiered Storage](#fsx-tiered-storage)
  - [Sharding](#sharding)
  - [Stored Procedures](#stored-procedures)
- [Columnar Storage](#columnar-storage)
  - [Encoding Types](#encoding-types)
  - [Automatic Encoding Selection](#automatic-encoding-selection)
  - [Column Statistics (Zone Maps)](#column-statistics-zone-maps)
  - [Writer Usage](#writer-usage)
  - [Reader Usage](#reader-usage)
  - [Data Types](#data-types)
  - [Predicate Operations](#predicate-operations)
  - [Constants](#constants)

---

## Module Exports

DoSQL provides multiple subpath exports for importing specific functionality:

```typescript
// Main entry point - Database, Statement, types, errors
import { Database, createDatabase, DatabaseError } from '@dotdo/dosql';

// RPC client/server for remote database access
import { createRPCClient, createRPCServer } from '@dotdo/dosql/rpc';

// Write-Ahead Log for durability
import { createWALWriter, createWALReader } from '@dotdo/dosql/wal';

// Change Data Capture for real-time streaming
import { createCDC, createCDCSubscription } from '@dotdo/dosql/cdc';

// Transaction utilities
import { TransactionManager } from '@dotdo/dosql/transaction';

// FSX - File System Abstraction for tiered storage
import {
  createDOBackend,
  createR2Backend,
  createTieredBackend,
  createCOWBackend,
  MemoryFSXBackend,
} from '@dotdo/dosql/fsx';

// ORM adapters
import { createPrismaAdapter } from '@dotdo/dosql/orm/prisma';
import { createKyselyAdapter } from '@dotdo/dosql/orm/kysely';
import { createKnexAdapter } from '@dotdo/dosql/orm/knex';
import { createDrizzleAdapter } from '@dotdo/dosql/orm/drizzle';
```

| Subpath | Description |
|---------|-------------|
| `@dotdo/dosql` | Main entry point with Database class, types, and errors |
| `@dotdo/dosql/rpc` | RPC client/server for remote database operations |
| `@dotdo/dosql/wal` | Write-Ahead Log for durability and recovery |
| `@dotdo/dosql/cdc` | Change Data Capture for real-time change streaming |
| `@dotdo/dosql/transaction` | Transaction management utilities |
| `@dotdo/dosql/fsx` | File System Abstraction with tiered storage backends |
| `@dotdo/dosql/orm/prisma` | Prisma ORM adapter |
| `@dotdo/dosql/orm/kysely` | Kysely query builder adapter |
| `@dotdo/dosql/orm/knex` | Knex.js query builder adapter |
| `@dotdo/dosql/orm/drizzle` | Drizzle ORM adapter |

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
  /**
   * Whether to open database in read-only mode
   * @default false
   */
  readonly?: boolean;

  /**
   * Whether to require the database file to exist
   * @default false
   */
  fileMustExist?: boolean;

  /**
   * Timeout for acquiring locks (milliseconds)
   * @default 5000
   */
  timeout?: number;

  /**
   * Verbose logging function for debugging
   * @default undefined
   */
  verbose?: (message?: unknown, ...params: unknown[]) => void;

  /**
   * Maximum size of the statement cache
   * @default 100
   */
  statementCacheSize?: number;
}
```

#### Examples

```typescript
import { Database, createDatabase } from '@dotdo/dosql';

// Example 1: In-memory database (default)
const db = new Database();

// Example 2: Using factory function
const db = createDatabase();

// Example 3: Named in-memory database
const db = new Database(':memory:');

// Example 4: With options
const db = new Database(':memory:', {
  readonly: false,
  timeout: 10000,
  statementCacheSize: 200,
});

// Example 5: With verbose logging
const db = new Database(':memory:', {
  verbose: console.log,
});

// Example 6: Read-only mode
const db = new Database(':memory:', {
  readonly: true,
});
```

### Properties

```typescript
class Database {
  /**
   * Database filename (or ':memory:')
   */
  readonly name: string;

  /**
   * Whether the database is read-only
   */
  readonly readonly: boolean;

  /**
   * Whether the database connection is open
   */
  readonly open: boolean;

  /**
   * Whether currently in a transaction
   */
  readonly inTransaction: boolean;
}
```

#### Example: Checking Database State

```typescript
const db = new Database();

console.log(db.name);           // ':memory:'
console.log(db.readonly);       // false
console.log(db.open);           // true
console.log(db.inTransaction);  // false

// After closing
db.close();
console.log(db.open);           // false
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

##### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `sql` | `string` | SQL statement to prepare |

##### Returns

`Statement<T, P>` - A prepared statement object

##### Examples

```typescript
// Example 1: Simple SELECT
const stmt = db.prepare('SELECT * FROM users');
const users = stmt.all();

// Example 2: With positional parameters
const stmt = db.prepare<{ id: number; name: string }>(
  'SELECT id, name FROM users WHERE id = ?'
);
const user = stmt.get(42);
// user is typed as { id: number; name: string } | undefined

// Example 3: With named parameters
const stmt = db.prepare<{ id: number; name: string }, { userId: number }>(
  'SELECT id, name FROM users WHERE id = :userId'
);
const user = stmt.get({ userId: 42 });

// Example 4: INSERT statement
const insertStmt = db.prepare(
  'INSERT INTO users (name, email) VALUES (?, ?)'
);
const result = insertStmt.run('Alice', 'alice@example.com');
console.log(result.lastInsertRowid); // 1

// Example 5: UPDATE statement
const updateStmt = db.prepare(
  'UPDATE users SET active = ? WHERE id = ?'
);
const result = updateStmt.run(true, 42);
console.log(result.changes); // 1

// Example 6: Reusing prepared statements
const stmt = db.prepare('SELECT * FROM users WHERE role = ?');
const admins = stmt.all('admin');
const users = stmt.all('user');
const guests = stmt.all('guest');
```

##### Error Handling

```typescript
import { DatabaseError, DatabaseErrorCode } from '@dotdo/dosql';

try {
  const stmt = db.prepare('SELECT * FROM nonexistent_table');
  stmt.all();
} catch (error) {
  if (error instanceof DatabaseError) {
    console.log(error.code); // 'STMT_TABLE_NOT_FOUND'
    console.log(error.message);
  }
}
```

---

#### exec()

Execute one or more SQL statements. Does not return results.

```typescript
exec(sql: string): this
```

##### Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `sql` | `string` | SQL string (may contain multiple statements) |

##### Returns

`this` - The database instance (for chaining)

##### Examples

```typescript
// Example 1: Create table
db.exec(`
  CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  )
`);

// Example 2: Multiple statements
db.exec(`
  CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
  CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT);
  CREATE INDEX idx_posts_user ON posts(user_id);
`);

// Example 3: Chaining
db.exec('CREATE TABLE a (id INTEGER)')
  .exec('CREATE TABLE b (id INTEGER)')
  .exec('CREATE TABLE c (id INTEGER)');

// Example 4: Schema migration script
db.exec(`
  -- Enable foreign keys
  PRAGMA foreign_keys = ON;

  -- Create users table
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL
  );

  -- Create posts table with foreign key
  CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    body TEXT,
    FOREIGN KEY (user_id) REFERENCES users(id)
  );
`);

// Example 5: Handling semicolons in string literals (supported)
db.exec(`
  INSERT INTO users (name) VALUES ('John; Doe');
  INSERT INTO users (name) VALUES ('Jane "O''Brien"');
`);
```

##### Error Handling

```typescript
import { DatabaseError, ReadOnlyError } from '@dotdo/dosql';

// Read-only database error
const readOnlyDb = new Database(':memory:', { readonly: true });
try {
  readOnlyDb.exec('CREATE TABLE test (id INTEGER)');
} catch (error) {
  if (error instanceof ReadOnlyError) {
    console.log('Cannot modify read-only database');
  }
}

// Closed database error
db.close();
try {
  db.exec('SELECT 1');
} catch (error) {
  if (error instanceof DatabaseError && error.code === 'DB_CLOSED') {
    console.log('Database is closed');
  }
}
```

---

#### close()

Close the database connection and release resources.

```typescript
close(): this
```

##### Returns

`this` - The database instance

##### Examples

```typescript
// Example 1: Basic usage
const db = new Database();
// ... use database
db.close();

// Example 2: Try-finally pattern
const db = new Database();
try {
  db.exec('CREATE TABLE test (id INTEGER)');
  db.prepare('INSERT INTO test VALUES (?)').run(1);
} finally {
  db.close();
}

// Example 3: Checking if closed
const db = new Database();
console.log(db.open); // true
db.close();
console.log(db.open); // false

// Example 4: Safe to call multiple times
db.close();
db.close(); // No error
```

---

### Prepared Statements

The `Statement` interface provides methods for executing prepared SQL statements.

#### Statement Properties

```typescript
interface Statement<T, P> {
  /**
   * The original SQL string
   */
  readonly source: string;

  /**
   * Whether the statement is read-only (SELECT, etc.)
   */
  readonly reader: boolean;

  /**
   * Whether the statement has been finalized
   */
  readonly finalized: boolean;
}
```

#### Statement Methods

##### bind()

Bind parameters to the statement (chainable).

```typescript
bind(...params: P extends any[] ? P : [P]): this
```

```typescript
// Example 1: Positional parameters
const stmt = db.prepare('SELECT * FROM users WHERE id = ? AND active = ?');
const boundStmt = stmt.bind(42, true);
const user = boundStmt.get();

// Example 2: Named parameters
const stmt = db.prepare('SELECT * FROM users WHERE id = :id');
const user = stmt.bind({ id: 42 }).get();

// Example 3: Chaining with configuration
const stmt = db.prepare('SELECT id FROM users WHERE active = ?');
const ids = stmt.bind(true).pluck().all();
// ids is number[]
```

##### run()

Execute the statement and return modification results.

```typescript
run(...params: P extends any[] ? P : [P]): RunResult

interface RunResult {
  /**
   * Number of rows affected by the statement
   */
  changes: number;

  /**
   * Row ID of the last inserted row (for INSERT statements)
   * Returns 0 if no rows were inserted
   */
  lastInsertRowid: number | bigint;
}
```

```typescript
// Example 1: INSERT
const stmt = db.prepare('INSERT INTO users (name, email) VALUES (?, ?)');
const result = stmt.run('Alice', 'alice@example.com');
console.log(result.changes);        // 1
console.log(result.lastInsertRowid); // 1

// Example 2: UPDATE
const stmt = db.prepare('UPDATE users SET active = ? WHERE role = ?');
const result = stmt.run(false, 'guest');
console.log(result.changes); // Number of rows updated

// Example 3: DELETE
const stmt = db.prepare('DELETE FROM users WHERE active = ?');
const result = stmt.run(false);
console.log(result.changes); // Number of rows deleted

// Example 4: Batch inserts
const insert = db.prepare('INSERT INTO users (name) VALUES (?)');
const names = ['Alice', 'Bob', 'Carol'];
let totalInserted = 0;
for (const name of names) {
  const { changes } = insert.run(name);
  totalInserted += changes;
}
console.log(`Inserted ${totalInserted} users`);
```

##### get()

Execute the statement and return the first row.

```typescript
get(...params: P extends any[] ? P : [P]): T | undefined
```

```typescript
// Example 1: Basic usage
const stmt = db.prepare('SELECT * FROM users WHERE id = ?');
const user = stmt.get(42);
if (user) {
  console.log(user.name);
}

// Example 2: Typed result
interface User {
  id: number;
  name: string;
  email: string;
}
const stmt = db.prepare<User>('SELECT * FROM users WHERE id = ?');
const user = stmt.get(42);
// user is User | undefined

// Example 3: With named parameters
const stmt = db.prepare('SELECT * FROM users WHERE email = :email');
const user = stmt.get({ email: 'alice@example.com' });

// Example 4: Aggregate query
const stmt = db.prepare('SELECT COUNT(*) as count FROM users WHERE active = ?');
const result = stmt.get(true);
console.log(result?.count); // e.g., 42
```

##### all()

Execute the statement and return all matching rows.

```typescript
all(...params: P extends any[] ? P : [P]): T[]
```

```typescript
// Example 1: All rows
const stmt = db.prepare('SELECT * FROM users');
const users = stmt.all();

// Example 2: Filtered rows
const stmt = db.prepare('SELECT * FROM users WHERE active = ?');
const activeUsers = stmt.all(true);

// Example 3: Typed results
interface User {
  id: number;
  name: string;
}
const stmt = db.prepare<User>('SELECT id, name FROM users WHERE role = ?');
const admins = stmt.all('admin');
// admins is User[]

// Example 4: Complex query
const stmt = db.prepare(`
  SELECT u.id, u.name, COUNT(p.id) as post_count
  FROM users u
  LEFT JOIN posts p ON p.user_id = u.id
  GROUP BY u.id
  HAVING post_count > ?
`);
const prolificUsers = stmt.all(10);
```

##### iterate()

Execute the statement and return an iterator over rows.

```typescript
iterate(...params: P extends any[] ? P : [P]): IterableIterator<T>
```

```typescript
// Example 1: Basic iteration
const stmt = db.prepare('SELECT * FROM users');
for (const user of stmt.iterate()) {
  console.log(user.name);
}

// Example 2: Early termination
const stmt = db.prepare('SELECT * FROM large_table');
for (const row of stmt.iterate()) {
  if (row.id > 100) break; // Stop early
  processRow(row);
}

// Example 3: With parameters
const stmt = db.prepare('SELECT * FROM logs WHERE level = ?');
for (const log of stmt.iterate('ERROR')) {
  console.error(log.message);
}

// Example 4: Spread into array
const stmt = db.prepare('SELECT id FROM users LIMIT 10');
const ids = [...stmt.pluck().iterate()];
// ids is number[]
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

```typescript
// Example 1: Basic column info
const stmt = db.prepare('SELECT id, name FROM users');
const cols = stmt.columns();
console.log(cols);
// [
//   { name: 'id', column: 'id', table: 'users', database: 'main', type: 'INTEGER' },
//   { name: 'name', column: 'name', table: 'users', database: 'main', type: 'TEXT' }
// ]

// Example 2: With aliases
const stmt = db.prepare('SELECT id AS user_id, name AS user_name FROM users');
const cols = stmt.columns();
console.log(cols[0].name);   // 'user_id'
console.log(cols[0].column); // 'id'

// Example 3: Expression columns
const stmt = db.prepare('SELECT COUNT(*) as total FROM users');
const cols = stmt.columns();
console.log(cols[0].name);  // 'total'
console.log(cols[0].table); // null (expression, not from table)
```

##### finalize()

Release resources associated with the statement.

```typescript
finalize(): void
```

```typescript
// Example 1: Manual finalization
const stmt = db.prepare('SELECT * FROM users');
const users = stmt.all();
stmt.finalize();

// Example 2: Check finalized state
const stmt = db.prepare('SELECT 1');
console.log(stmt.finalized); // false
stmt.finalize();
console.log(stmt.finalized); // true

// Attempting to use a finalized statement throws an error
try {
  stmt.all();
} catch (error) {
  console.log(error.code); // 'STMT_FINALIZED'
}
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

// Combining options
const stmt = db.prepare('SELECT id FROM users WHERE active = ?')
  .safeIntegers()
  .pluck();
const ids = stmt.all(true);
// ids is bigint[]
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
// Example 1: Basic transaction
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

// Example 2: Transaction with return value
const createUserWithProfile = db.transaction((name: string, bio: string) => {
  const { lastInsertRowid: userId } = db.prepare(
    'INSERT INTO users (name) VALUES (?)'
  ).run(name);

  db.prepare(
    'INSERT INTO profiles (user_id, bio) VALUES (?, ?)'
  ).run(userId, bio);

  return userId;
});

const newUserId = createUserWithProfile('Alice', 'Software engineer');

// Example 3: Using different modes
const bulkInsert = db.transaction((items: Item[]) => {
  const insert = db.prepare('INSERT INTO items (name, value) VALUES (?, ?)');
  for (const item of items) {
    insert.run(item.name, item.value);
  }
});

// Deferred mode (default)
bulkInsert(items);

// Immediate mode - gets write lock immediately
bulkInsert.immediate(items);

// Exclusive mode - full exclusive access
bulkInsert.exclusive(items);

// Example 4: Automatic rollback on error
const failingTransaction = db.transaction(() => {
  db.prepare('INSERT INTO users (name) VALUES (?)').run('Alice');
  throw new Error('Oops!');
  // Transaction automatically rolls back
});

try {
  failingTransaction();
} catch (error) {
  // Error is re-thrown, but transaction is rolled back
  console.log('Transaction failed:', error.message);
}

// Example 5: Nested function calls
const createOrder = db.transaction((userId: number, items: OrderItem[]) => {
  const { lastInsertRowid: orderId } = db.prepare(
    'INSERT INTO orders (user_id) VALUES (?)'
  ).run(userId);

  const insertItem = db.prepare(
    'INSERT INTO order_items (order_id, product_id, quantity) VALUES (?, ?, ?)'
  );

  for (const item of items) {
    insertItem.run(orderId, item.productId, item.quantity);
  }

  // Update inventory
  const updateStock = db.prepare(
    'UPDATE products SET stock = stock - ? WHERE id = ?'
  );
  for (const item of items) {
    updateStock.run(item.quantity, item.productId);
  }

  return { orderId, itemCount: items.length };
});
```

---

### Savepoints

Savepoints allow partial rollback within a transaction.

#### savepoint()

Begin a savepoint.

```typescript
savepoint(name: string): this
```

#### release()

Release (commit) a savepoint.

```typescript
release(name: string): this
```

#### rollback()

Rollback to a savepoint or the transaction.

```typescript
rollback(name?: string): this
```

##### Examples

```typescript
// Example 1: Basic savepoint usage
db.savepoint('sp1');
db.prepare('INSERT INTO users (name) VALUES (?)').run('Alice');
db.release('sp1'); // Commits the savepoint

// Example 2: Rollback to savepoint
db.savepoint('sp1');
db.prepare('INSERT INTO users (name) VALUES (?)').run('Alice');
db.rollback('sp1'); // Undoes the insert

// Example 3: Nested savepoints
const complexOperation = db.transaction(() => {
  // Main operation
  db.prepare('INSERT INTO orders (user_id) VALUES (?)').run(1);

  db.savepoint('items');
  try {
    db.prepare('INSERT INTO order_items (order_id, product_id) VALUES (?, ?)').run(1, 100);
    // If this fails, we can rollback just the items
  } catch (error) {
    db.rollback('items');
    // Handle partial failure
  }
  db.release('items');
});

// Example 4: Try-catch pattern with savepoints
const importData = db.transaction((rows: DataRow[]) => {
  let successCount = 0;
  let errorCount = 0;

  for (const row of rows) {
    db.savepoint('row');
    try {
      db.prepare('INSERT INTO data (value) VALUES (?)').run(row.value);
      db.release('row');
      successCount++;
    } catch (error) {
      db.rollback('row');
      errorCount++;
    }
  }

  return { successCount, errorCount };
});
```

#### Savepoint Nesting Limitations

While savepoints support nesting, there are important limitations and constraints to be aware of:

##### Maximum Nesting Depth

```typescript
// DoSQL enforces a maximum savepoint nesting depth of 32
// Exceeding this limit throws a DatabaseError

// This will eventually fail:
for (let i = 0; i < 50; i++) {
  db.savepoint(`sp_${i}`); // Throws at i=32
}
```

##### Name Uniqueness Within Active Savepoints

```typescript
// Savepoint names must be unique among currently active (unreleased) savepoints
// Reusing an active savepoint name throws an error

db.savepoint('sp1');
db.savepoint('sp2');
db.savepoint('sp1'); // ERROR: Savepoint 'sp1' already exists

// However, after releasing a savepoint, its name can be reused:
db.savepoint('sp1');
db.release('sp1');
db.savepoint('sp1'); // OK - sp1 was released
```

##### Savepoint Ordering Constraints

```typescript
// Savepoints must be released or rolled back in LIFO (stack) order
// You cannot release or rollback an inner savepoint before an outer one

db.savepoint('outer');
db.savepoint('inner');

// INCORRECT - cannot release outer before inner
db.release('outer'); // ERROR: Cannot release savepoint with active nested savepoints

// CORRECT - release in reverse order
db.release('inner');
db.release('outer');
```

##### Rollback Behavior with Nested Savepoints

```typescript
// Rolling back to a savepoint also releases all savepoints created after it
db.savepoint('sp1');
db.prepare('INSERT INTO t VALUES (1)').run();
db.savepoint('sp2');
db.prepare('INSERT INTO t VALUES (2)').run();
db.savepoint('sp3');
db.prepare('INSERT INTO t VALUES (3)').run();

// Rolling back to sp1 also implicitly releases sp2 and sp3
db.rollback('sp1');

// After rollback, sp2 and sp3 no longer exist
db.release('sp2'); // ERROR: Savepoint 'sp2' does not exist
```

##### Transaction Context Required

```typescript
// Savepoints are only valid within an active transaction
// Using savepoints outside a transaction throws an error

// INCORRECT - no active transaction
db.savepoint('sp1'); // ERROR: Cannot create savepoint outside transaction

// CORRECT - within transaction
const doWork = db.transaction(() => {
  db.savepoint('sp1'); // OK
  // ... work ...
  db.release('sp1');
});
doWork();
```

##### Memory Considerations

```typescript
// Each active savepoint consumes memory for tracking state
// Deep nesting or many parallel savepoints can impact performance

// Best practice: Keep nesting shallow and release savepoints promptly
const processItems = db.transaction((items: Item[]) => {
  for (const item of items) {
    db.savepoint('item');
    try {
      processItem(item);
      db.release('item'); // Release immediately after success
    } catch {
      db.rollback('item');
    }
    // Savepoint is always released/rolled back before next iteration
  }
});
```

##### Savepoints and DDL Statements

```typescript
// Some DDL statements (CREATE TABLE, DROP TABLE, etc.) may implicitly
// commit the transaction in certain SQL implementations.
// In DoSQL, DDL within savepoints follows SQLite semantics:

const migration = db.transaction(() => {
  db.savepoint('schema_change');
  try {
    db.exec('ALTER TABLE users ADD COLUMN status TEXT');
    db.release('schema_change');
  } catch (error) {
    db.rollback('schema_change');
    // Schema change is rolled back
    throw error;
  }
});
```

---

### Batch Operations

Execute multiple queries in a single request with configurable execution semantics.

#### batch()

Execute multiple queries with control over atomicity and error handling.

```typescript
batch(request: BatchRequest): Promise<BatchResponse>

interface BatchRequest {
  /** Array of queries to execute */
  queries: QueryRequest[];
  /** Whether to execute in a single transaction (default: false) */
  atomic?: boolean;
  /** Whether to continue on error (default: false) */
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

interface BatchError {
  /** Index of the failed query */
  index: number;
  /** Error message */
  error: string;
  /** Error code */
  code?: string;
}
```

#### Atomic vs ContinueOnError Tradeoffs

The `atomic` and `continueOnError` options control how the batch handles failures. Understanding their interaction is critical for choosing the right behavior for your use case.

##### Option Combinations

| `atomic` | `continueOnError` | Behavior | Use Case |
|----------|-------------------|----------|----------|
| `false` | `false` | Stop on first error, no rollback | Fast-fail, partial results acceptable |
| `false` | `true` | Continue on errors, no rollback | Best-effort execution, collect all results |
| `true` | `false` | Stop on first error, rollback all | All-or-nothing consistency |
| `true` | `true` | Continue on errors, rollback all on any error | Validate all queries, rollback if any fail |

##### Atomic Mode (`atomic: true`)

When `atomic` is enabled, all queries execute within a single database transaction:

- **All succeed together**: If all queries complete successfully, changes are committed atomically
- **All fail together**: If any query fails, all changes are rolled back - the database remains unchanged
- **Isolation**: Other concurrent operations see either all changes or none

```typescript
// Example: Transfer money between accounts (all-or-nothing)
const result = await rpc.batch({
  queries: [
    { sql: 'UPDATE accounts SET balance = balance - ? WHERE id = ?', params: [100, 1] },
    { sql: 'UPDATE accounts SET balance = balance + ? WHERE id = ?', params: [100, 2] },
    { sql: 'INSERT INTO transfers (from_id, to_id, amount) VALUES (?, ?, ?)', params: [1, 2, 100] },
  ],
  atomic: true,
});

// If any query fails, no money moves and no transfer is recorded
```

**When to use atomic mode:**
- Financial transactions (transfers, payments)
- Multi-table inserts with foreign key relationships
- Any operation where partial completion would leave data inconsistent
- Schema migrations that must apply completely or not at all

**Tradeoffs of atomic mode:**
- Holds locks longer (entire batch duration)
- Higher latency due to transaction overhead
- May increase lock contention under high concurrency
- Single failure causes complete rollback (potentially expensive re-work)

##### Continue On Error Mode (`continueOnError: true`)

When `continueOnError` is enabled, execution proceeds through all queries regardless of failures:

```typescript
// Example: Import data with best-effort semantics
const result = await rpc.batch({
  queries: [
    { sql: 'INSERT INTO users (id, name) VALUES (?, ?)', params: [1, 'Alice'] },
    { sql: 'INSERT INTO users (id, name) VALUES (?, ?)', params: [1, 'Bob'] }, // Duplicate key!
    { sql: 'INSERT INTO users (id, name) VALUES (?, ?)', params: [3, 'Carol'] },
  ],
  continueOnError: true,
});

// result.successCount = 2 (Alice and Carol)
// result.errorCount = 1 (Bob failed due to duplicate key)
// Both Alice and Carol are inserted
```

**When to use continueOnError:**
- Bulk data imports where some failures are expected
- Idempotent operations (INSERT OR IGNORE patterns)
- Collecting validation errors across all queries
- Processing independent operations in a single request

**Tradeoffs of continueOnError:**
- May leave database in partially updated state
- Requires checking each result for errors
- Harder to reason about final state if some operations failed
- Not suitable for dependent operations

##### Combining Atomic + ContinueOnError

When both options are enabled, the batch executes all queries but rolls back if any fail:

```typescript
// Example: Validate an entire batch before committing
const result = await rpc.batch({
  queries: [
    { sql: 'INSERT INTO orders (id, user_id) VALUES (?, ?)', params: [1, 100] },
    { sql: 'INSERT INTO order_items (order_id, product_id) VALUES (?, ?)', params: [1, 50] },
    { sql: 'UPDATE inventory SET qty = qty - 1 WHERE product_id = ?', params: [50] },
  ],
  atomic: true,
  continueOnError: true,
});

// All queries execute, but if ANY fail, ALL are rolled back
// Useful for dry-run validation or detecting all errors at once
```

**When to use both:**
- Validating complex multi-statement operations
- Detecting all constraint violations in one pass
- Testing migrations before applying them
- Pre-flight checks that need full error reporting

##### Default Behavior (`atomic: false`, `continueOnError: false`)

Without either option, the batch stops on the first error with no transaction wrapper:

```typescript
// Example: Sequential operations, stop on first failure
const result = await rpc.batch({
  queries: [
    { sql: 'INSERT INTO users (name) VALUES (?)', params: ['Alice'] },
    { sql: 'INVALID SQL HERE' }, // Error!
    { sql: 'INSERT INTO users (name) VALUES (?)', params: ['Carol'] }, // Never executed
  ],
});

// result.successCount = 1 (Alice)
// result.errorCount = 1 (invalid SQL)
// Carol's insert is skipped - results array has placeholder for it
```

**When to use defaults:**
- Simple sequential operations where order matters
- When you want to fail fast without overhead
- Operations that are easy to retry from the point of failure

##### Performance Considerations

| Mode | Transaction Overhead | Lock Duration | Error Recovery |
|------|---------------------|---------------|----------------|
| Default | None | Per-query | Retry from failure point |
| `atomic` only | Full | Entire batch | Retry entire batch |
| `continueOnError` only | None | Per-query | Check each result |
| Both | Full | Entire batch | Retry entire batch |

##### Examples

```typescript
// Example 1: Atomic batch for data consistency
const transfer = await rpc.batch({
  queries: [
    { sql: 'UPDATE accounts SET balance = balance - 100 WHERE id = 1' },
    { sql: 'UPDATE accounts SET balance = balance + 100 WHERE id = 2' },
  ],
  atomic: true,
});

// Example 2: Best-effort bulk insert
const bulkInsert = await rpc.batch({
  queries: users.map(user => ({
    sql: 'INSERT OR IGNORE INTO users (email, name) VALUES (?, ?)',
    params: [user.email, user.name],
  })),
  continueOnError: true,
});
console.log(`Inserted ${bulkInsert.successCount} of ${users.length} users`);

// Example 3: Validate before commit
const validation = await rpc.batch({
  queries: migrationStatements,
  atomic: true,
  continueOnError: true,
});
if (validation.errorCount > 0) {
  console.log('Migration would fail:', validation.results.filter(r => 'error' in r));
  // All changes are already rolled back
}

// Example 4: Process results with error handling
const result = await rpc.batch({
  queries: operations,
  continueOnError: true,
});

for (let i = 0; i < result.results.length; i++) {
  const r = result.results[i];
  if ('error' in r) {
    console.error(`Query ${i} failed: ${r.error}`);
  } else {
    console.log(`Query ${i} affected ${r.changes} rows`);
  }
}
```

---

### PRAGMA Commands

#### pragma()

Execute a PRAGMA statement.

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
// Example 1: Get/set journal mode
const mode = db.pragma('journal_mode');
console.log(mode); // 'memory'

// Example 2: Enable foreign keys
db.pragma('foreign_keys', 1);
const enabled = db.pragma('foreign_keys');
console.log(enabled); // 1

// Example 3: Set cache size
db.pragma('cache_size', -64000); // 64MB (negative = KB)

// Example 4: Get table info
interface TableInfoRow {
  cid: number;
  name: string;
  type: string;
  notnull: number;
  dflt_value: string | null;
  pk: number;
}
const columns = db.pragma('table_info', 'users') as TableInfoRow[];
for (const col of columns) {
  console.log(`${col.name}: ${col.type}`);
}

// Example 5: Integrity check
const result = db.pragma('integrity_check');
console.log(result); // ['ok'] if database is healthy

// Example 6: Get database list
const databases = db.pragma('database_list');
console.log(databases);
// [{ seq: 0, name: 'main', file: ':memory:' }]

// Example 7: Application version tracking
db.pragma('user_version', 3); // Set schema version
const version = db.pragma('user_version');
console.log(version); // 3

// Example 8: Get compile options
const options = db.pragma('compile_options');
console.log(options); // ['ENABLE_FTS5', 'ENABLE_JSON1', ...]
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
// Example 1: Simple function
db.function('double', (x) => Number(x) * 2);
const result = db.prepare('SELECT double(5)').pluck().get();
console.log(result); // 10

// Example 2: String function
db.function('reverse', (str) => {
  return String(str).split('').reverse().join('');
});
const reversed = db.prepare("SELECT reverse('hello')").pluck().get();
console.log(reversed); // 'olleh'

// Example 3: Multi-argument function
db.function('clamp', (value, min, max) => {
  const v = Number(value);
  const lo = Number(min);
  const hi = Number(max);
  return Math.max(lo, Math.min(hi, v));
});
const clamped = db.prepare('SELECT clamp(150, 0, 100)').pluck().get();
console.log(clamped); // 100

// Example 4: Using in queries
db.function('is_valid_email', (email) => {
  const pattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return pattern.test(String(email)) ? 1 : 0;
});
const validUsers = db.prepare(`
  SELECT * FROM users WHERE is_valid_email(email) = 1
`).all();

// Example 5: JSON processing
db.function('json_extract_name', (jsonStr) => {
  try {
    const obj = JSON.parse(String(jsonStr));
    return obj.name ?? null;
  } catch {
    return null;
  }
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
  /**
   * Called for each row to update the accumulator state
   */
  step: (accumulator: TAccumulator, ...values: SqlValue[]) => void;

  /**
   * Called to finalize and return the aggregate result
   */
  result: (accumulator: TAccumulator) => SqlValue;

  /**
   * Initial accumulator value
   */
  start?: TAccumulator;

  /**
   * Optional inverse function for window functions
   */
  inverse?: (accumulator: TAccumulator, ...values: SqlValue[]) => void;
}
```

##### Examples

```typescript
// Example 1: Custom sum
db.aggregate<number>('my_sum', {
  start: 0,
  step: (acc, value) => acc + Number(value),
  result: (acc) => acc,
});
const total = db.prepare('SELECT my_sum(amount) FROM orders').pluck().get();

// Example 2: String concatenation
db.aggregate<string[]>('string_agg', {
  start: [],
  step: (acc, value, separator = ',') => {
    acc.push(String(value));
  },
  result: (acc) => acc.join(','),
});
const names = db.prepare('SELECT string_agg(name) FROM users').pluck().get();

// Example 3: Min/Max range
interface MinMax {
  min: number;
  max: number;
}
db.aggregate<MinMax>('range', {
  start: { min: Infinity, max: -Infinity },
  step: (acc, value) => {
    const n = Number(value);
    if (n < acc.min) acc.min = n;
    if (n > acc.max) acc.max = n;
  },
  result: (acc) => acc.max - acc.min,
});

// Example 4: Custom average with object state
interface AvgState {
  sum: number;
  count: number;
}
db.aggregate<AvgState>('custom_avg', {
  start: { sum: 0, count: 0 },
  step: (acc, value) => {
    if (value !== null) {
      acc.sum += Number(value);
      acc.count++;
    }
  },
  result: (acc) => acc.count > 0 ? acc.sum / acc.count : null,
});

// Example 5: Median (collects all values)
db.aggregate<number[]>('median', {
  start: [],
  step: (acc, value) => {
    if (value !== null) {
      acc.push(Number(value));
    }
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
/**
 * Supported SQL value types for binding
 */
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
/**
 * Named parameters object using :name, @name, or $name syntax
 */
type NamedParameters = Record<string, SqlValue>;

/**
 * Positional parameters array using ? or ?NNN syntax
 */
type PositionalParameters = SqlValue[];

/**
 * Union of all parameter binding styles
 */
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

// Blob binding
const data = new Uint8Array([1, 2, 3, 4]);
db.prepare('INSERT INTO files (content) VALUES (?)').run(data);

// Date binding (converted to ISO string)
const now = new Date();
db.prepare('INSERT INTO events (created_at) VALUES (?)').run(now);

// BigInt binding
const bigId = 9007199254740993n;
db.prepare('INSERT INTO big_table (id) VALUES (?)').run(bigId);
```

### Result Types

```typescript
interface RunResult {
  /**
   * Number of rows affected by the statement
   */
  changes: number;

  /**
   * Row ID of the last inserted row (for INSERT statements)
   */
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

interface ErrorContext {
  requestId?: string;
  transactionId?: string;
  sql?: string;
  table?: string;
  column?: string;
  metadata?: Record<string, unknown>;
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

#### Database Error Codes

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

#### Statement Error Codes

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

#### Binding Error Codes

```typescript
enum BindingErrorCode {
  MISSING_PARAM = 'BIND_MISSING_PARAM',
  TYPE_MISMATCH = 'BIND_TYPE_MISMATCH',
  INVALID_TYPE = 'BIND_INVALID_TYPE',
  COUNT_MISMATCH = 'BIND_COUNT_MISMATCH',
  NAMED_EXPECTED = 'BIND_NAMED_EXPECTED',
}
```

#### Syntax Error Codes

```typescript
enum SyntaxErrorCode {
  UNEXPECTED_TOKEN = 'SYNTAX_UNEXPECTED_TOKEN',
  UNEXPECTED_EOF = 'SYNTAX_UNEXPECTED_EOF',
  INVALID_IDENTIFIER = 'SYNTAX_INVALID_IDENTIFIER',
  INVALID_LITERAL = 'SYNTAX_INVALID_LITERAL',
  GENERAL = 'SYNTAX_ERROR',
}
```

### Error Handling Patterns

```typescript
import {
  DoSQLError,
  DatabaseError,
  DatabaseErrorCode,
  StatementError,
  StatementErrorCode,
  BindingError,
  BindingErrorCode,
  SQLSyntaxError,
  ErrorCategory,
} from '@dotdo/dosql';

// Example 1: Basic error handling
try {
  db.prepare('SELECT * FROM nonexistent').all();
} catch (error) {
  if (error instanceof DoSQLError) {
    console.log('Code:', error.code);
    console.log('Category:', error.category);
    console.log('Retryable:', error.isRetryable());
    console.log('User message:', error.toUserMessage());
  }
}

// Example 2: Handle specific error types
try {
  db.prepare('INSERT INTO users (name) VALUES (?)').run();
} catch (error) {
  if (error instanceof BindingError) {
    if (error.code === BindingErrorCode.MISSING_PARAM) {
      console.log('Missing parameter');
    }
  }
}

// Example 3: Handle by category
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
      case ErrorCategory.CONFLICT:
        console.log('Conflict detected - may need retry');
        break;
      case ErrorCategory.TIMEOUT:
        console.log('Operation timed out');
        break;
      default:
        console.log('Unexpected error:', error.message);
    }
  }
}

// Example 4: Structured logging
try {
  db.exec('INVALID SQL');
} catch (error) {
  if (error instanceof DoSQLError) {
    const logEntry = error.toLogEntry();
    logger.error(logEntry);
    // {
    //   level: 'error',
    //   timestamp: '2024-01-15T10:30:00.000Z',
    //   error: { name: 'SQLSyntaxError', code: 'SYNTAX_ERROR', message: '...' },
    //   metadata: { category: 'VALIDATION', ... }
    // }
  }
}

// Example 5: API response serialization
try {
  await handleRequest();
} catch (error) {
  if (error instanceof DoSQLError) {
    return new Response(JSON.stringify(error.toJSON()), {
      status: 400,
      headers: { 'Content-Type': 'application/json' },
    });
  }
}

// Example 6: Add context to errors
try {
  db.prepare('SELECT * FROM users WHERE id = ?').get(userId);
} catch (error) {
  if (error instanceof DoSQLError) {
    throw error
      .withContext({ requestId: ctx.requestId, sql: 'SELECT * FROM users' })
      .withRecoveryHint('Check that the users table exists');
  }
  throw error;
}

// Example 7: Retry pattern for retryable errors
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
        throw error; // Don't retry non-retryable errors
      }
      await new Promise(r => setTimeout(r, 100 * Math.pow(2, attempt)));
    }
  }

  throw lastError;
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
  type AppendOptions,
  type AppendResult,
} from '@dotdo/dosql';

// Create a WAL writer
const writer = createWALWriter(backend, config);

// Write an entry
const result = await writer.append({
  timestamp: Date.now(),
  txnId: generateTxnId(),
  op: 'INSERT',
  table: 'users',
  after: new TextEncoder().encode(JSON.stringify({ id: 1, name: 'Alice' })),
});

// Flush to storage
await writer.flush();

// Get current LSN
const lsn = writer.getCurrentLSN();
```

### WAL Reader

```typescript
import {
  createWALReader,
  tailWAL,
  readWALBatched,
  reconstructTransactions,
  type WALReader,
  type ReadOptions,
} from '@dotdo/dosql';

// Create a reader
const reader = createWALReader(backend);

// Read all entries from LSN
for await (const entry of reader.iterate({ fromLSN: 0n })) {
  console.log(entry.op, entry.table);
}

// Read in batches
const batches = readWALBatched(reader, { batchSize: 100 });
for await (const batch of batches) {
  processBatch(batch);
}

// Tail the WAL (follow new entries)
const tail = tailWAL(reader, { fromLSN: lastLSN, pollInterval: 100 });
for await (const entry of tail) {
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
  type CheckpointManager,
  type Checkpoint,
  type RecoveryState,
} from '@dotdo/dosql';

// Create checkpoint manager
const checkpointManager = createCheckpointManager(storage, writer);

// Create a checkpoint
await checkpointManager.createCheckpoint();

// Get latest checkpoint
const checkpoint = await checkpointManager.getLatestCheckpoint();

// Check if recovery is needed
if (await needsRecovery(storage)) {
  const state = await performRecovery(storage, writer, reader);
  console.log(`Recovered ${state.entriesReplayed} entries`);
}

// Auto-checkpoint
const autoCheckpointer = createAutoCheckpointer(checkpointManager, {
  intervalMs: 60000,
  maxEntries: 10000,
});
autoCheckpointer.start();
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
  type CDCSubscription,
  type ChangeEvent,
  type CDCFilter,
} from '@dotdo/dosql';

// Create CDC instance
const cdc = createCDC(backend);

// Subscribe to all changes
const subscription = cdc.subscribe({ fromLSN: 0n });

for await (const entry of subscription.subscribe(0n)) {
  console.log('Change:', entry.op, entry.table);
}

// Subscribe with typed events
for await (const event of subscription.subscribeChanges(0n, {
  tables: ['users'],
  operations: ['INSERT', 'UPDATE']
}, JSON.parse)) {
  if (event.type === 'insert') {
    console.log('New user:', event.data);
  } else if (event.type === 'update') {
    console.log('Updated user:', event.data, 'was:', event.oldData);
  }
}

// Subscribe to specific table
const userChanges = subscribeTable(cdc, 'users', { fromLSN: 0n });
for await (const change of userChanges) {
  handleUserChange(change);
}

// Batched subscription
const batched = subscribeBatched(cdc, {
  batchSize: 100,
  maxWaitMs: 1000,
});
for await (const batch of batched) {
  await processBatch(batch);
}
```

### Replication Slots

```typescript
import {
  createReplicationSlotManager,
  type ReplicationSlotManager,
  type ReplicationSlot,
} from '@dotdo/dosql';

// Create slot manager
const slots = createReplicationSlotManager(storage);

// Create a slot
await slots.createSlot('my-consumer', 0n);

// Get slot
const slot = await slots.getSlot('my-consumer');
console.log('Last confirmed LSN:', slot?.confirmedLSN);

// Subscribe from slot
const subscription = await slots.subscribeFromSlot('my-consumer');
for await (const entry of subscription) {
  await processEntry(entry);
  await slots.updateSlot('my-consumer', entry.lsn);
}

// List all slots
const allSlots = await slots.listSlots();

// Delete a slot
await slots.deleteSlot('my-consumer');
```

---

## Virtual Tables

### URL Table Sources

Query data directly from URLs using ClickHouse-style syntax.

```typescript
import {
  createURLVirtualTable,
  createVirtualTableRegistry,
  resolveVirtualTableFromClause,
} from '@dotdo/dosql';

// Create registry
const registry = createVirtualTableRegistry({
  fetch: globalThis.fetch,
});

// Query JSON from URL
const users = await registry.query(
  "SELECT * FROM 'https://api.example.com/users.json'"
);

// Query CSV with options
const data = await registry.query(`
  SELECT *
  FROM 'https://data.gov/dataset.csv'
  WITH (headers=true, delimiter=',')
`);

// Query with filtering
const active = await registry.query(`
  SELECT name, email
  FROM 'https://api.example.com/users.json'
  WHERE active = true
  LIMIT 10
`);
```

### R2 Sources

Query data from Cloudflare R2 buckets.

```typescript
import {
  createR2Source,
  parseR2Uri,
  listR2Objects,
} from '@dotdo/dosql';

// Create R2 source
const source = createR2Source(r2Bucket, 'data/users.parquet');

// Scan with projection
for await (const row of source.scan({
  columns: ['id', 'name', 'email'],
  filter: { column: 'active', op: '=', value: true },
})) {
  console.log(row);
}

// List objects
const objects = await listR2Objects(r2Bucket, 'data/');
console.log(objects);

// Parse R2 URI
const { bucket, key } = parseR2Uri('r2://mybucket/path/to/file.parquet');
```

---

## Advanced Features

### FSX Tiered Storage

The FSX (File System Abstraction) module provides a tiered storage backend that combines Durable Object storage (hot tier) with R2 storage (cold tier) for optimal performance and cost.

#### Storage Tiers

```typescript
import {
  createTieredBackend,
  StorageTier,
  type TieredStorageConfig,
  type TieredStorageBackend,
  type MigrationResult,
} from '@dotdo/dosql';

/**
 * Storage tier enum indicating where data resides
 */
enum StorageTier {
  /** Data is in Durable Object storage (hot) */
  HOT = 'hot',
  /** Data is in R2 storage (cold) */
  COLD = 'cold',
  /** Data exists in both tiers */
  BOTH = 'both',
}
```

#### TieredStorageConfig

Configuration options for tiered storage behavior.

```typescript
interface TieredStorageConfig {
  /**
   * Maximum age in milliseconds before data is considered cold
   * Files not accessed within this window are eligible for migration to R2
   * @default 3600000 (1 hour)
   */
  hotDataMaxAge: number;

  /**
   * Maximum total size in bytes for hot storage before triggering migration
   * When exceeded, oldest/least-accessed files migrate to R2
   * @default 104857600 (100MB)
   */
  hotStorageMaxSize: number;

  /**
   * Whether to automatically migrate cold data to R2
   * When true, migration runs during write operations
   * @default true
   */
  autoMigrate: boolean;

  /**
   * Whether to read from hot storage first
   * When true, reads check DO storage before R2
   * @default true
   */
  readHotFirst: boolean;

  /**
   * Whether to cache R2 reads in hot storage
   * When true, data read from R2 is copied to DO for faster subsequent reads
   * @default false
   */
  cacheR2Reads: boolean;

  /**
   * Maximum size of individual files to keep in hot storage
   * Files larger than this go directly to R2 on write
   * @default 10485760 (10MB)
   */
  maxHotFileSize: number;
}

// Default configuration values
const DEFAULT_TIERED_CONFIG: TieredStorageConfig = {
  hotDataMaxAge: 60 * 60 * 1000,      // 1 hour
  hotStorageMaxSize: 100 * 1024 * 1024, // 100MB
  autoMigrate: true,
  readHotFirst: true,
  cacheR2Reads: false,
  maxHotFileSize: 10 * 1024 * 1024,   // 10MB
};
```

#### Creating a Tiered Backend

```typescript
import {
  createTieredBackend,
  createDOBackend,
  createR2Backend,
} from '@dotdo/dosql';

// Create individual backends
const hotBackend = createDOBackend(ctx.storage);
const coldBackend = createR2Backend(env.MY_BUCKET, {
  keyPrefix: 'data/',
});

// Create tiered backend with custom configuration
const tieredBackend = createTieredBackend(hotBackend, coldBackend, {
  hotDataMaxAge: 30 * 60 * 1000,      // 30 minutes
  hotStorageMaxSize: 50 * 1024 * 1024, // 50MB
  autoMigrate: true,
  cacheR2Reads: true,
  maxHotFileSize: 5 * 1024 * 1024,    // 5MB
});
```

#### Migration Behavior

##### Automatic Migration

When `autoMigrate` is enabled, migration is triggered during write operations:

1. **Age-based migration**: Files not accessed within `hotDataMaxAge` are migrated to R2
2. **Size-based migration**: When hot storage exceeds `hotStorageMaxSize`, oldest/least-accessed files migrate

```typescript
// Files are automatically migrated based on access patterns
await tieredBackend.write('data.bin', myData);
// ^ This may trigger migration of older files

// Migration priority is determined by:
// 1. Access count (lower = higher migration priority)
// 2. Last access time (older = higher migration priority)
```

##### Manual Migration

You can explicitly trigger migration with `migrateToR2()`:

```typescript
interface MigrationOptions {
  /** Only migrate files older than this (ms). Default: hotDataMaxAge */
  olderThan?: number;
  /** Maximum number of files to migrate. Default: 100 */
  limit?: number;
  /** Only migrate files matching this prefix */
  prefix?: string;
  /** Delete from hot storage after migration. Default: true */
  deleteFromHot?: boolean;
  /** Target size to free up in bytes */
  targetSize?: number;
}

interface MigrationResult {
  /** Paths that were successfully migrated */
  migrated: string[];
  /** Paths that failed with error messages */
  failed: Array<{ path: string; error: string }>;
  /** Total bytes transferred */
  bytesTransferred: number;
}

// Migrate all files older than 1 hour
const result = await tieredBackend.migrateToR2({
  olderThan: 60 * 60 * 1000,
});

// Migrate specific prefix to free up space
const result = await tieredBackend.migrateToR2({
  prefix: 'logs/',
  deleteFromHot: true,
  targetSize: 10 * 1024 * 1024, // Free 10MB
});
```

##### Promoting Data to Hot Tier

Promote cold data back to hot storage for performance:

```typescript
// Promote specific files
const result = await tieredBackend.promoteToHot([
  'frequently-accessed.bin',
  'important-data.bin',
]);

// Files will be in StorageTier.BOTH after promotion
// (exists in both tiers until next migration)
```

##### Pinning Files to Hot Tier

Prevent automatic migration of specific files:

```typescript
// Pin a file to hot storage
await tieredBackend.pinToHot('critical-config.bin');
// This file will NOT be migrated automatically

// Unpin to allow migration
await tieredBackend.unpinFromHot('critical-config.bin');
```

##### Writing with Tier Hint

Specify initial tier placement for writes:

```typescript
// Write directly to cold storage (skips hot tier)
await tieredBackend.writeWithTier('archive.bin', data, {
  tier: StorageTier.COLD,
});

// Explicitly request hot tier (throws if file exceeds maxHotFileSize)
await tieredBackend.writeWithTier('hot-data.bin', data, {
  tier: StorageTier.HOT,
});
```

#### Reading Behavior

Reads are transparent across tiers:

```typescript
// Read checks hot tier first (if readHotFirst: true)
const data = await tieredBackend.read('myfile.bin');

// Range reads work across tiers
const partial = await tieredBackend.read('largefile.bin', [0, 1023]);

// If cacheR2Reads is enabled, data read from R2 is cached in DO
// for faster subsequent reads (respects maxHotFileSize)
```

#### Metadata and Statistics

```typescript
interface TieredMetadata {
  size: number;
  lastModified: Date;
  tier: StorageTier;
  lastAccessed?: Date;
  migratedAt?: Date;
  etag?: string;
}

// Get file metadata including tier information
const meta = await tieredBackend.metadata('myfile.bin');
console.log('Tier:', meta?.tier);        // 'hot', 'cold', or 'both'
console.log('Migrated:', meta?.migratedAt);

// Get storage statistics
interface TieredStorageStats {
  hot: { fileCount: number; totalSize: number };
  cold: { objectCount: number; totalSize: number };
  index: { entryCount: number };
  totalFiles: number;
  hotToTotalRatio: number;
  migrationPending: number;
  hotStorageWarning?: boolean;  // True when >70% of hotStorageMaxSize used
  lastMigration?: Date;
  migrationCount: number;
  totalBytesMigrated: number;
}

const stats = await tieredBackend.getStats();
console.log('Hot files:', stats.hot.fileCount);
console.log('Cold files:', stats.cold.objectCount);
console.log('Migration pending:', stats.migrationPending);
```

#### Index Management

The tiered backend maintains an index in DO storage to track file locations:

```typescript
// Rebuild index from actual storage contents
// Useful after recovery or inconsistent state
const { indexed, errors } = await tieredBackend.rebuildIndex();

// Load index into memory from storage
const loadedCount = await tieredBackend.loadIndex();
```

#### Migration Best Practices

1. **Configure thresholds based on workload**:
   - Set `hotDataMaxAge` based on your access patterns
   - Set `hotStorageMaxSize` below DO storage limits (typically 1-10GB)

2. **Use pinning for critical data**:
   - Pin configuration files or frequently accessed data
   - Avoid pinning large files that could exhaust hot storage

3. **Monitor with statistics**:
   - Watch `hotStorageWarning` for capacity issues
   - Track `migrationPending` to tune thresholds

4. **Consider cacheR2Reads for read-heavy workloads**:
   - Enable when same cold data is read repeatedly
   - Disable to minimize hot storage usage

5. **Use tier hints for predictable data**:
   - Write archives directly to cold with `tier: StorageTier.COLD`
   - Prevents unnecessary hot storage churn

---

### Sharding

```typescript
import {
  createShardRouter,
  createConsistentHashRouter,
  createRangeShardRouter,
  type ShardRouter,
  type ShardConfig,
} from '@dotdo/dosql';

// Consistent hash sharding
const router = createConsistentHashRouter({
  shards: ['shard1', 'shard2', 'shard3', 'shard4'],
  virtualNodes: 150,
});

// Route a key
const shard = router.route('user:12345');
console.log('Route to:', shard);

// Range-based sharding
const rangeRouter = createRangeShardRouter({
  ranges: [
    { start: 0, end: 1000000, shard: 'shard1' },
    { start: 1000000, end: 2000000, shard: 'shard2' },
  ],
});
```

### Stored Procedures

```typescript
import {
  createProcedureRegistry,
  createProcedureExecutor,
  procedure,
  ProcedureBuilder,
  type Procedure,
  type ProcedureContext,
} from '@dotdo/dosql';

// Define a procedure using the builder
const createUser = procedure()
  .input({ name: 'string', email: 'string' })
  .output({ id: 'number', created: 'boolean' })
  .handler(async (ctx, input) => {
    const result = await ctx.db.sql`
      INSERT INTO users (name, email)
      VALUES (${input.name}, ${input.email})
    `;
    return {
      id: Number(result.lastInsertRowid),
      created: true,
    };
  });

// Create registry
const registry = createProcedureRegistry(catalogStorage);

// Register procedure
await registry.register('createUser', createUser);

// Execute procedure
const executor = createProcedureExecutor(registry, { db });
const result = await executor.execute('createUser', {
  name: 'Alice',
  email: 'alice@example.com',
});
```

---

## Columnar Storage

DoSQL includes a columnar OLAP storage engine optimized for analytics queries. It supports multiple encoding strategies, zone map filtering for predicate pushdown, and projection pushdown for efficient data access.

### Encoding Types

The columnar storage engine supports five encoding strategies, each optimized for different data patterns.

```typescript
import {
  type Encoding,
  type ColumnDataType,
  ColumnarWriter,
  ColumnarReader,
  analyzeForEncoding,
} from '@dotdo/dosql';

/**
 * Supported column encodings:
 * - raw: Direct typed array storage (no compression)
 * - dict: Dictionary encoding for low-cardinality strings
 * - rle: Run-length encoding for repeated values
 * - delta: Delta encoding for sorted/sequential integers
 * - bitpack: Bit-packing for small integers
 */
type Encoding = 'raw' | 'dict' | 'rle' | 'delta' | 'bitpack';
```

#### Raw Encoding

Direct storage using typed arrays. No compression overhead, best for random or high-entropy data.

```typescript
// Raw encoding stores values directly in typed arrays
// Supported data types:
// - int8, int16, int32, int64
// - uint8, uint16, uint32, uint64
// - float32, float64
// - boolean
// - string (length-prefixed UTF-8)
// - bytes (length-prefixed binary)
// - timestamp (64-bit integer)

// Example: Raw encoding is automatically selected for float columns
const schema: ColumnarTableSchema = {
  tableName: 'measurements',
  columns: [
    { name: 'sensor_id', dataType: 'int32', nullable: false },
    { name: 'temperature', dataType: 'float64', nullable: false }, // Uses raw
    { name: 'humidity', dataType: 'float32', nullable: true },     // Uses raw
  ],
};
```

#### Dictionary Encoding

Optimal for low-cardinality string columns (many repeated values). Stores unique values in a dictionary and references them by index.

```typescript
// Dictionary encoding is auto-selected when:
// - Column has >= 100 rows (MIN_ROWS_FOR_DICT)
// - Cardinality ratio <= 10% (DICT_CARDINALITY_THRESHOLD)

// Example: Status column with few unique values
const schema: ColumnarTableSchema = {
  tableName: 'orders',
  columns: [
    { name: 'id', dataType: 'int64', nullable: false },
    { name: 'status', dataType: 'string', nullable: false }, // Auto: dict encoding
    { name: 'customer_name', dataType: 'string', nullable: false }, // Auto: raw
  ],
};

// Force dictionary encoding for a specific column
const writer = new ColumnarWriter(schema, {
  forceEncoding: new Map([['status', 'dict']]),
});

// How dictionary encoding works:
// 1. Build dictionary: ['pending', 'shipped', 'delivered'] (3 unique values)
// 2. Calculate bits per index: ceil(log2(3)) = 2 bits
// 3. Store indices using bit-packing: [0, 1, 2, 1, 0, 2, ...]
// Result: Significant space savings for repeated string values
```

#### Run-Length Encoding (RLE)

Best for columns with many consecutive repeated values, such as sorted data or categorical columns.

```typescript
// RLE stores (value, count) pairs instead of individual values
// Excellent for sorted columns or columns with long runs of the same value

// Example: Event log with repeated timestamps
const schema: ColumnarTableSchema = {
  tableName: 'events',
  columns: [
    { name: 'event_type', dataType: 'int32', nullable: false }, // Good for RLE
    { name: 'batch_id', dataType: 'int64', nullable: false },   // Good for RLE
  ],
};

// Force RLE encoding
const writer = new ColumnarWriter(schema, {
  forceEncoding: new Map([['event_type', 'rle']]),
});

// How RLE works for data: [1, 1, 1, 2, 2, 3, 3, 3, 3, 3]
// Encoded as runs: [(1, 3), (2, 2), (3, 5)]
// Format: [runCount:4][value:N][count:4]...
```

#### Delta Encoding

Optimal for sorted or sequential integer columns. Stores the first value and deltas between consecutive values.

```typescript
// Delta encoding is ideal for:
// - Sorted primary key columns
// - Timestamps in chronological order
// - Sequential IDs

const schema: ColumnarTableSchema = {
  tableName: 'timeseries',
  columns: [
    { name: 'timestamp', dataType: 'timestamp', nullable: false }, // Good for delta
    { name: 'sequence_id', dataType: 'int64', nullable: false },   // Good for delta
  ],
};

// Force delta encoding
const writer = new ColumnarWriter(schema, {
  forceEncoding: new Map([['sequence_id', 'delta']]),
});

// How delta encoding works for sorted IDs: [100, 101, 102, 105, 106]
// 1. Store first value: 100
// 2. Calculate deltas: [1, 1, 3, 1]
// 3. Bit-pack deltas using minimal bits
// Result: Much smaller storage for sequential data
```

#### Bit-Packing

Stores small integers using the minimum number of bits required.

```typescript
// Bit-packing is efficient when values use fewer bits than the data type allows
// Example: int32 column where max value is 15 (needs only 4 bits)

const schema: ColumnarTableSchema = {
  tableName: 'ratings',
  columns: [
    { name: 'user_id', dataType: 'int64', nullable: false },
    { name: 'rating', dataType: 'int32', nullable: false }, // Values 1-5, needs 3 bits
  ],
};

// Force bit-pack encoding
const writer = new ColumnarWriter(schema, {
  forceEncoding: new Map([['rating', 'bitpack']]),
});

// How bit-packing works:
// For values 1-5, only 3 bits needed per value
// 32-bit int32 compressed to 3 bits = 10x compression
// Format: [bitWidth:1][packedData...]
```

### Automatic Encoding Selection

The writer automatically analyzes data and selects the best encoding.

```typescript
import { analyzeForEncoding, type EncodingAnalysis } from '@dotdo/dosql';

// Analyze data to determine optimal encoding
const values = ['active', 'active', 'inactive', 'active', 'pending'];
const analysis: EncodingAnalysis = analyzeForEncoding(values, 'string');

console.log(analysis);
// {
//   recommendedEncoding: 'dict',  // Dictionary recommended
//   estimatedSize: 45,            // Estimated bytes
//   cardinality: 3,               // 3 unique values
// }

// For numeric data
const numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
const numericAnalysis = analyzeForEncoding(numbers, 'int32');
// {
//   recommendedEncoding: 'delta', // Delta recommended for sorted integers
//   estimatedSize: 18,
//   isSorted: true,
//   runCount: 10,
// }
```

### Column Statistics (Zone Maps)

Each column chunk stores statistics for predicate pushdown.

```typescript
import { type ColumnStats, getColumnStats } from '@dotdo/dosql';

interface ColumnStats {
  /** Minimum value in the chunk */
  min: number | bigint | string | null;

  /** Maximum value in the chunk */
  max: number | bigint | string | null;

  /** Number of null values */
  nullCount: number;

  /** Approximate distinct count (optional) */
  distinctCount?: number;

  /** Sum for numeric columns (optional) */
  sum?: number | bigint;
}

// Get stats from serialized data
const stats = getColumnStats(rowGroupData, 'temperature');
console.log(stats);
// {
//   min: 18.5,
//   max: 32.1,
//   nullCount: 0,
//   distinctCount: 1000,
//   sum: 25000.5,
// }

// Stats enable skipping entire row groups during queries
// Example: WHERE temperature > 40
// If stats.max < 40, skip this entire row group
```

### Writer Usage

```typescript
import {
  ColumnarWriter,
  writeColumnar,
  inferSchema,
  type ColumnarTableSchema,
  type WriterConfig,
} from '@dotdo/dosql';

// Define schema explicitly
const schema: ColumnarTableSchema = {
  tableName: 'users',
  columns: [
    { name: 'id', dataType: 'int64', nullable: false },
    { name: 'name', dataType: 'string', nullable: false },
    { name: 'email', dataType: 'string', nullable: true },
    { name: 'age', dataType: 'int32', nullable: true },
    { name: 'created_at', dataType: 'timestamp', nullable: false },
  ],
  primaryKey: ['id'],
};

// Or infer schema from sample data
const sampleData = [
  { id: 1n, name: 'Alice', email: 'alice@example.com', age: 30 },
  { id: 2n, name: 'Bob', email: null, age: 25 },
];
const inferredSchema = inferSchema('users', sampleData);

// Create writer with configuration
const config: WriterConfig = {
  targetRowsPerGroup: 65536,      // Max rows per row group
  targetBytesPerGroup: 1048576,   // Target 1MB per row group
  disableAutoEncoding: false,     // Allow auto encoding selection
  forceEncoding: new Map([        // Force specific encodings
    ['status', 'dict'],
  ]),
  onFlush: async (rowGroup, data) => {
    console.log(`Flushed row group ${rowGroup.id}: ${data.length} bytes`);
  },
};

const writer = new ColumnarWriter(schema, config, fsx);

// Write rows (auto-flushes when thresholds reached)
const rows = [
  { id: 1n, name: 'Alice', email: 'alice@example.com', age: 30, created_at: Date.now() },
  { id: 2n, name: 'Bob', email: null, age: 25, created_at: Date.now() },
  // ... more rows
];

const flushedGroups = await writer.write(rows);

// Finalize any remaining buffered rows
const finalGroup = await writer.finalize();

// Get all flushed row groups
const allGroups = writer.getFlushedRowGroups();

// Convenience function for simple writes
const { rowGroups, serialized } = await writeColumnar(schema, rows, fsx);
```

### Reader Usage

```typescript
import {
  ColumnarReader,
  readColumnarChunk,
  mightMatchPredicates,
  aggregateSumFromStats,
  aggregateCountFromStats,
  aggregateMinMaxFromStats,
  type ReaderConfig,
  type ReadRequest,
  type Predicate,
} from '@dotdo/dosql';

// Create reader
const config: ReaderConfig = {
  enablePredicatePushdown: true,  // Use zone maps to skip chunks
  parallelScan: 4,                // Scan up to 4 row groups in parallel
  prefetchMetadata: true,         // Cache row group metadata
};

const reader = new ColumnarReader(fsx, config);

// Read with projection and predicate pushdown
const request: ReadRequest = {
  table: 'users',
  projection: { columns: ['id', 'name', 'email'] }, // Only these columns
  predicates: [
    { column: 'age', op: 'ge', value: 18 },
    { column: 'age', op: 'lt', value: 65 },
  ],
  limit: 100,
  offset: 0,
};

const result = await reader.read(request);
console.log(result);
// {
//   columns: Map { 'id' => [...], 'name' => [...], 'email' => [...] },
//   rowCount: 100,
//   rowGroupsScanned: 5,
//   rowGroupsSkipped: 3,  // Skipped by predicate pushdown
//   totalBytesRead: 524288,
// }

// Scan returns rows as objects
const scanResult = await reader.scan(request);
console.log(scanResult.rows[0]);
// { id: 1n, name: 'Alice', email: 'alice@example.com' }

// Read a single row group directly
const columns = await reader.readRowGroup(rowGroupData, ['id', 'name']);

// Check if predicates might match (for custom filtering)
const predicates: Predicate[] = [{ column: 'age', op: 'gt', value: 100 }];
const mightMatch = mightMatchPredicates(rowGroupData, predicates);
if (!mightMatch) {
  console.log('Can skip this row group - no matching rows');
}

// Aggregate directly from statistics (no data scan needed)
const metadataList = await getRowGroupMetadataList(fsx, 'users');

const totalAge = aggregateSumFromStats(metadataList, 'age');
const userCount = aggregateCountFromStats(metadataList, 'id', false);
const { min: minAge, max: maxAge } = aggregateMinMaxFromStats(metadataList, 'age');

console.log(`Users: ${userCount}, Age range: ${minAge}-${maxAge}, Total age: ${totalAge}`);
```

### Data Types

```typescript
/**
 * Supported column data types
 */
type ColumnDataType =
  | 'int8'      // 1 byte signed integer
  | 'int16'     // 2 byte signed integer
  | 'int32'     // 4 byte signed integer
  | 'int64'     // 8 byte signed integer (bigint)
  | 'uint8'     // 1 byte unsigned integer
  | 'uint16'    // 2 byte unsigned integer
  | 'uint32'    // 4 byte unsigned integer
  | 'uint64'    // 8 byte unsigned integer (bigint)
  | 'float32'   // 4 byte IEEE 754 float
  | 'float64'   // 8 byte IEEE 754 double
  | 'boolean'   // 1 byte (0 or 1)
  | 'string'    // Variable length UTF-8
  | 'bytes'     // Variable length binary
  | 'timestamp'; // 8 byte Unix timestamp (milliseconds)

// Type helpers
import { isNumericType, isIntegerType, isSignedType, getBytesPerElement } from '@dotdo/dosql';

isNumericType('float64');      // true
isIntegerType('int32');        // true
isSignedType('uint64');        // false
getBytesPerElement('int32');   // 4
getBytesPerElement('string');  // -1 (variable length)
```

### Predicate Operations

```typescript
/**
 * Supported predicate operations for filtering
 */
type PredicateOp = 'eq' | 'ne' | 'lt' | 'le' | 'gt' | 'ge' | 'in' | 'between';

// Example predicates
const predicates: Predicate[] = [
  // Equality
  { column: 'status', op: 'eq', value: 'active' },

  // Not equal
  { column: 'role', op: 'ne', value: 'admin' },

  // Comparisons
  { column: 'age', op: 'ge', value: 18 },
  { column: 'created_at', op: 'lt', value: Date.now() },

  // Range (between is inclusive)
  { column: 'price', op: 'between', value: 10, value2: 100 },

  // IN list
  { column: 'category', op: 'in', value: ['electronics', 'clothing', 'food'] },
];
```

### Constants

```typescript
import {
  MAX_BLOB_SIZE,           // 2MB - 1KB overhead
  TARGET_ROW_GROUP_SIZE,   // 1MB target per row group
  MAX_ROWS_PER_ROW_GROUP,  // 65536 rows max
  MIN_ROWS_FOR_DICT,       // 100 rows minimum for dictionary
  DICT_CARDINALITY_THRESHOLD, // 10% max unique/total ratio
} from '@dotdo/dosql';
```

---

## Related Guides

- [Getting Started](./getting-started.md) - Quick start guide for DoSQL
- [Advanced Features](./advanced.md) - Time travel, branching, CDC
- [Architecture](./architecture.md) - Understanding DoSQL internals
- [Troubleshooting](./TROUBLESHOOTING.md) - Common issues and solutions
- [Testing Guide](./TESTING_REVIEW.md) - Testing best practices
