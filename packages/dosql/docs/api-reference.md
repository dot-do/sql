# DoSQL API Reference

Complete API documentation for DoSQL.

## Table of Contents

- [DB() Function](#db-function)
- [Database Methods](#database-methods)
- [Prepared Statements](#prepared-statements)
- [Transactions](#transactions)
- [PRAGMA Commands](#pragma-commands)
- [SQL Syntax Reference](#sql-syntax-reference)
- [Type System](#type-system)
- [Errors](#errors)

---

## DB() Function

The primary entry point for creating or connecting to a DoSQL database.

### Signature

```typescript
function DB(name: string, options?: DBOptions): Promise<Database>;
```

### Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `name` | `string` | Yes | Database/tenant identifier |
| `options` | `DBOptions` | No | Configuration options |

### DBOptions

```typescript
interface DBOptions {
  /**
   * Migration source
   * - `{ folder: string }` - Load from .sql files in folder
   * - `{ drizzle: string }` - Load from Drizzle Kit folder
   * - `Migration[]` - Inline migration array
   * - `() => Promise<Migration[]>` - Async loader function
   */
  migrations?: MigrationSource;

  /**
   * Automatically apply pending migrations on first access
   * @default true
   */
  autoMigrate?: boolean;

  /**
   * Table name for tracking migrations
   * @default '__dosql_migrations'
   */
  migrationsTable?: string;

  /**
   * Storage configuration
   */
  storage?: {
    /** Durable Object storage for hot data */
    hot: DurableObjectStorage;
    /** R2 bucket for cold storage (optional) */
    cold?: R2Bucket;
  };

  /**
   * Enable Write-Ahead Log for durability
   * @default true
   */
  wal?: boolean;

  /**
   * WAL configuration
   */
  walConfig?: WALConfig;

  /**
   * Enable time travel queries
   * @default true
   */
  timeTravel?: boolean;

  /**
   * Logger for debugging
   */
  logger?: Logger;
}
```

### Migration Type

```typescript
interface Migration {
  /** Unique identifier (e.g., '001_create_users') */
  id: string;
  /** SQL statements to execute */
  sql: string;
  /** Optional checksum for validation */
  checksum?: string;
}
```

### Example Usage

```typescript
// Simple usage
const db = await DB('my-app');

// With folder migrations
const db = await DB('tenants', {
  migrations: { folder: '.do/migrations' },
});

// With inline migrations
const db = await DB('tenants', {
  migrations: [
    { id: '001_init', sql: 'CREATE TABLE users (id INTEGER PRIMARY KEY)' },
  ],
});

// With async loader
const db = await DB('tenants', {
  migrations: async () => {
    const response = await fetch('/api/migrations');
    return response.json();
  },
});

// With full configuration
const db = await DB('analytics', {
  migrations: { folder: '.do/migrations' },
  autoMigrate: true,
  storage: {
    hot: state.storage,
    cold: env.R2_BUCKET,
  },
  wal: true,
  timeTravel: true,
});
```

---

## Database Methods

### query()

Execute a SELECT query and return all matching rows.

```typescript
query<T = Record<string, unknown>>(
  sql: string,
  params?: QueryParams
): Promise<T[]>;
```

**Parameters:**
- `sql` - SQL SELECT statement
- `params` - Optional array or object of parameters

**Returns:** Array of row objects

**Example:**

```typescript
// Simple query
const users = await db.query('SELECT * FROM users');

// With positional parameters
const active = await db.query(
  'SELECT * FROM users WHERE active = ? AND role = ?',
  [true, 'admin']
);

// With named parameters
const user = await db.query(
  'SELECT * FROM users WHERE id = :id',
  { id: 42 }
);
```

### queryOne()

Execute a SELECT query and return the first matching row.

```typescript
queryOne<T = Record<string, unknown>>(
  sql: string,
  params?: QueryParams
): Promise<T | null>;
```

**Example:**

```typescript
const user = await db.queryOne('SELECT * FROM users WHERE id = ?', [42]);
if (user) {
  console.log(user.name);
}
```

### run()

Execute an INSERT, UPDATE, or DELETE statement.

```typescript
run(sql: string, params?: QueryParams): Promise<RunResult>;

interface RunResult {
  /** Number of rows affected */
  rowsAffected: number;
  /** ID of last inserted row (for INSERT) */
  lastInsertRowId: number | bigint;
}
```

**Example:**

```typescript
// Insert
const result = await db.run(
  'INSERT INTO users (name, email) VALUES (?, ?)',
  ['Alice', 'alice@example.com']
);
console.log(result.lastInsertRowId); // 1

// Update
const updated = await db.run(
  'UPDATE users SET active = ? WHERE id = ?',
  [false, 1]
);
console.log(updated.rowsAffected); // 1

// Delete
const deleted = await db.run('DELETE FROM users WHERE id = ?', [1]);
```

### exec()

Execute raw SQL (multiple statements allowed, no parameters).

```typescript
exec(sql: string): Promise<void>;
```

**Example:**

```typescript
await db.exec(`
  CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
  CREATE INDEX idx_users_name ON users(name);
`);
```

### prepare()

Create a prepared statement for repeated execution.

```typescript
prepare(sql: string): PreparedStatement;
```

See [Prepared Statements](#prepared-statements) section below.

### transaction()

Execute multiple operations in a transaction.

```typescript
transaction<T>(fn: (tx: Transaction) => Promise<T>): Promise<T>;
```

See [Transactions](#transactions) section below.

### close()

Close the database connection and release resources.

```typescript
close(): Promise<void>;
```

**Example:**

```typescript
const db = await DB('my-app');
try {
  // ... use database
} finally {
  await db.close();
}
```

---

## Prepared Statements

Prepared statements allow efficient repeated execution of the same query with different parameters.

### Creating a Prepared Statement

```typescript
const stmt = db.prepare('SELECT * FROM users WHERE active = ?');
```

### PreparedStatement Methods

#### all()

Execute and return all rows.

```typescript
all<T = Record<string, unknown>>(params?: QueryParams): Promise<T[]>;
```

```typescript
const stmt = db.prepare('SELECT * FROM users WHERE role = ?');
const admins = await stmt.all(['admin']);
const users = await stmt.all(['user']);
```

#### get()

Execute and return the first row.

```typescript
get<T = Record<string, unknown>>(params?: QueryParams): Promise<T | null>;
```

```typescript
const stmt = db.prepare('SELECT * FROM users WHERE id = ?');
const user = await stmt.get([42]);
```

#### run()

Execute for modifications (INSERT/UPDATE/DELETE).

```typescript
run(params?: QueryParams): Promise<RunResult>;
```

```typescript
const stmt = db.prepare('INSERT INTO users (name) VALUES (?)');
const result1 = await stmt.run(['Alice']);
const result2 = await stmt.run(['Bob']);
```

#### iterate()

Execute and iterate over rows (for large result sets).

```typescript
iterate<T = Record<string, unknown>>(
  params?: QueryParams
): AsyncIterableIterator<T>;
```

```typescript
const stmt = db.prepare('SELECT * FROM logs');
for await (const log of stmt.iterate()) {
  console.log(log);
}
```

#### bind()

Bind parameters without executing (returns new PreparedStatement).

```typescript
bind(params: QueryParams): PreparedStatement;
```

```typescript
const stmt = db.prepare('SELECT * FROM users WHERE active = ? AND role = ?');
const boundStmt = stmt.bind([true, 'admin']);
const users = await boundStmt.all();
```

---

## Transactions

Transactions ensure atomic execution of multiple statements.

### Basic Transaction

```typescript
await db.transaction(async (tx) => {
  await tx.run('INSERT INTO orders (user_id) VALUES (?)', [1]);
  await tx.run('UPDATE users SET order_count = order_count + 1 WHERE id = ?', [1]);
});
```

### Transaction with Return Value

```typescript
const orderId = await db.transaction(async (tx) => {
  const result = await tx.run('INSERT INTO orders (user_id) VALUES (?)', [1]);
  return result.lastInsertRowId;
});
```

### Transaction Rollback

Transactions automatically rollback on error:

```typescript
try {
  await db.transaction(async (tx) => {
    await tx.run('INSERT INTO orders (user_id) VALUES (?)', [1]);
    throw new Error('Something went wrong');
    // Transaction is automatically rolled back
  });
} catch (error) {
  console.log('Transaction rolled back');
}
```

### Transaction Modes

```typescript
// DEFERRED (default) - Locks acquired when needed
await db.transaction(async (tx) => { ... });

// IMMEDIATE - Acquires write lock immediately
await db.transaction(async (tx) => { ... }, { mode: 'IMMEDIATE' });

// EXCLUSIVE - Full exclusive access
await db.transaction(async (tx) => { ... }, { mode: 'EXCLUSIVE' });
```

### Savepoints (Nested Transactions)

```typescript
await db.transaction(async (tx) => {
  await tx.run('INSERT INTO users (name) VALUES (?)', ['Alice']);

  await tx.savepoint('inner', async (sp) => {
    await sp.run('INSERT INTO users (name) VALUES (?)', ['Bob']);
    // Can rollback to savepoint without affecting outer transaction
  });

  await tx.run('INSERT INTO users (name) VALUES (?)', ['Carol']);
});
```

### Transaction Options

```typescript
interface TransactionOptions {
  /** Transaction mode */
  mode?: 'DEFERRED' | 'IMMEDIATE' | 'EXCLUSIVE';

  /** Isolation level */
  isolationLevel?: 'READ_UNCOMMITTED' | 'READ_COMMITTED' | 'REPEATABLE_READ' | 'SNAPSHOT' | 'SERIALIZABLE';

  /** Read-only transaction */
  readOnly?: boolean;

  /** Lock timeout in milliseconds */
  lockTimeout?: number;
}
```

### Transaction Interface

```typescript
interface Transaction {
  /** Execute a query */
  query<T>(sql: string, params?: QueryParams): Promise<T[]>;

  /** Execute a query and return first row */
  queryOne<T>(sql: string, params?: QueryParams): Promise<T | null>;

  /** Execute a modification statement */
  run(sql: string, params?: QueryParams): Promise<RunResult>;

  /** Create a savepoint */
  savepoint<T>(name: string, fn: (sp: Transaction) => Promise<T>): Promise<T>;

  /** Commit the transaction */
  commit(): Promise<void>;

  /** Rollback the transaction */
  rollback(): Promise<void>;
}
```

---

## PRAGMA Commands

DoSQL supports SQLite-compatible PRAGMA commands for database configuration and introspection.

### Database Information

```typescript
// Get database version
const version = await db.queryOne('PRAGMA user_version');

// Set database version
await db.exec('PRAGMA user_version = 2');

// Get table list
const tables = await db.query('PRAGMA table_list');

// Get table info
const columns = await db.query('PRAGMA table_info(users)');

// Get index list
const indexes = await db.query('PRAGMA index_list(users)');

// Get index info
const indexCols = await db.query('PRAGMA index_info(idx_users_email)');

// Get foreign keys
const fks = await db.query('PRAGMA foreign_key_list(posts)');
```

### Performance Configuration

```typescript
// Cache size (in pages, negative for KB)
await db.exec('PRAGMA cache_size = -64000'); // 64MB

// Page size (must be set before creating tables)
await db.exec('PRAGMA page_size = 4096');

// Memory-mapped I/O size
await db.exec('PRAGMA mmap_size = 268435456'); // 256MB

// Synchronous mode
await db.exec('PRAGMA synchronous = NORMAL');
```

### Integrity Checks

```typescript
// Quick integrity check
const result = await db.queryOne('PRAGMA quick_check');

// Full integrity check
const fullResult = await db.query('PRAGMA integrity_check');

// Foreign key check
const fkResult = await db.query('PRAGMA foreign_key_check');
```

### DoSQL-Specific PRAGMAs

```typescript
// Get WAL status
const walStatus = await db.queryOne('PRAGMA dosql_wal_status');

// Get storage tier info
const storageInfo = await db.query('PRAGMA dosql_storage_info');

// Get migration status
const migrations = await db.query('PRAGMA dosql_migrations');

// Get branch info
const branches = await db.query('PRAGMA dosql_branches');

// Get current LSN
const lsn = await db.queryOne('PRAGMA dosql_current_lsn');
```

---

## SQL Syntax Reference

### Supported Statements

#### Data Definition (DDL)

```sql
-- Create table
CREATE TABLE [IF NOT EXISTS] table_name (
  column_name type [constraints],
  ...
  [table_constraints]
);

-- Create index
CREATE [UNIQUE] INDEX [IF NOT EXISTS] index_name
ON table_name (column1 [ASC|DESC], ...);

-- Drop table
DROP TABLE [IF EXISTS] table_name;

-- Drop index
DROP INDEX [IF EXISTS] index_name;

-- Alter table (limited)
ALTER TABLE table_name ADD COLUMN column_name type [constraints];
ALTER TABLE table_name RENAME TO new_name;
ALTER TABLE table_name RENAME COLUMN old_name TO new_name;
```

#### Data Manipulation (DML)

```sql
-- Select
SELECT [DISTINCT] columns
FROM table
[JOIN other_table ON condition]
[WHERE condition]
[GROUP BY columns]
[HAVING condition]
[ORDER BY columns [ASC|DESC]]
[LIMIT count [OFFSET skip]];

-- Insert
INSERT INTO table (columns) VALUES (values);
INSERT INTO table (columns) SELECT ... FROM ...;

-- Update
UPDATE table SET column = value [WHERE condition];

-- Delete
DELETE FROM table [WHERE condition];
```

### Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equal | `WHERE id = 1` |
| `!=`, `<>` | Not equal | `WHERE id != 1` |
| `<`, `>` | Less/greater than | `WHERE age > 18` |
| `<=`, `>=` | Less/greater or equal | `WHERE age >= 21` |
| `AND` | Logical AND | `WHERE a = 1 AND b = 2` |
| `OR` | Logical OR | `WHERE a = 1 OR a = 2` |
| `NOT` | Logical NOT | `WHERE NOT active` |
| `IN` | In set | `WHERE id IN (1, 2, 3)` |
| `BETWEEN` | Range | `WHERE age BETWEEN 18 AND 65` |
| `LIKE` | Pattern match | `WHERE name LIKE 'A%'` |
| `GLOB` | Glob pattern | `WHERE name GLOB 'A*'` |
| `IS NULL` | Null check | `WHERE email IS NULL` |
| `IS NOT NULL` | Not null check | `WHERE email IS NOT NULL` |

### Functions

#### Aggregate Functions

| Function | Description |
|----------|-------------|
| `COUNT(*)` | Count rows |
| `COUNT(column)` | Count non-null values |
| `SUM(column)` | Sum of values |
| `AVG(column)` | Average of values |
| `MIN(column)` | Minimum value |
| `MAX(column)` | Maximum value |
| `GROUP_CONCAT(column)` | Concatenate values |

#### Scalar Functions

| Function | Description |
|----------|-------------|
| `ABS(x)` | Absolute value |
| `LENGTH(s)` | String length |
| `LOWER(s)` | Lowercase |
| `UPPER(s)` | Uppercase |
| `TRIM(s)` | Remove whitespace |
| `SUBSTR(s, start, len)` | Substring |
| `REPLACE(s, old, new)` | Replace string |
| `COALESCE(a, b, ...)` | First non-null |
| `IFNULL(a, b)` | If null then b |
| `NULLIF(a, b)` | Null if a = b |
| `TYPEOF(x)` | Value type |
| `CAST(x AS type)` | Type conversion |

#### Date/Time Functions

| Function | Description |
|----------|-------------|
| `DATE(s)` | Extract date |
| `TIME(s)` | Extract time |
| `DATETIME(s)` | Full datetime |
| `JULIANDAY(s)` | Julian day number |
| `STRFTIME(fmt, s)` | Format datetime |
| `CURRENT_TIMESTAMP` | Current UTC time |
| `CURRENT_DATE` | Current UTC date |
| `CURRENT_TIME` | Current UTC time |

### Virtual Table Syntax

```sql
-- Query JSON from URL
SELECT * FROM 'https://api.example.com/users.json';

-- Query Parquet from R2
SELECT * FROM 'r2://bucket/data.parquet';

-- Query CSV with options
SELECT * FROM 'https://data.gov/file.csv' WITH (headers=true);
```

### Time Travel Syntax

```sql
-- Query at timestamp
SELECT * FROM users FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 12:00:00';

-- Query at LSN
SELECT * FROM users FOR SYSTEM_TIME AS OF LSN 12345;

-- Query at snapshot
SELECT * FROM users FOR SYSTEM_TIME AS OF SNAPSHOT 'main@5';

-- Query on branch
SELECT * FROM users FOR SYSTEM_TIME AS OF BRANCH 'feature-x';
```

---

## Type System

### SQL to TypeScript Type Mapping

| SQL Type | TypeScript Type |
|----------|-----------------|
| `INTEGER` | `number` |
| `REAL` | `number` |
| `TEXT` | `string` |
| `BLOB` | `Uint8Array` |
| `NULL` | `null` |
| `BOOLEAN` | `boolean` |

### Type-Safe Queries

DoSQL provides compile-time type inference:

```typescript
import { createDatabase, type DatabaseSchema } from '@dotdo/dosql';

// Define schema
interface MySchema extends DatabaseSchema {
  users: {
    id: 'number';
    name: 'string';
    email: 'string';
    active: 'boolean';
  };
}

// Create typed database
const db = createDatabase<MySchema>();

// Type-safe queries
const users = await db.sql`SELECT id, name FROM users`;
// users is typed as { id: number; name: string }[]
```

### Query Parameter Types

```typescript
// Array parameters (positional)
type QueryParams = unknown[];

// Named parameters
type QueryParams = Record<string, unknown>;

// Examples
await db.query('SELECT * FROM users WHERE id = ?', [42]);
await db.query('SELECT * FROM users WHERE id = :id', { id: 42 });
```

---

## Errors

### Error Types

```typescript
// Transaction errors
import { TransactionError, TransactionErrorCode } from '@dotdo/dosql/transaction';

try {
  await db.transaction(async (tx) => { ... });
} catch (error) {
  if (error instanceof TransactionError) {
    switch (error.code) {
      case TransactionErrorCode.DEADLOCK:
        // Handle deadlock
        break;
      case TransactionErrorCode.LOCK_TIMEOUT:
        // Handle timeout
        break;
    }
  }
}

// WAL errors
import { WALError, WALErrorCode } from '@dotdo/dosql/wal';

// FSX errors
import { FSXError, FSXErrorCode } from '@dotdo/dosql/fsx';

// Time travel errors
import { TimeTravelError, TimeTravelErrorCode } from '@dotdo/dosql/timetravel';

// CDC errors
import { CDCError, CDCErrorCode } from '@dotdo/dosql/cdc';
```

### Common Error Codes

| Module | Code | Description |
|--------|------|-------------|
| Transaction | `TXN_NO_ACTIVE` | No active transaction |
| Transaction | `TXN_DEADLOCK` | Deadlock detected |
| Transaction | `TXN_LOCK_TIMEOUT` | Lock acquisition timeout |
| WAL | `WAL_CHECKSUM_FAILED` | Data corruption detected |
| WAL | `WAL_SEGMENT_FULL` | Segment capacity exceeded |
| FSX | `FSX_NOT_FOUND` | File not found |
| FSX | `FSX_SIZE_EXCEEDED` | File too large |
| TimeTravel | `TT_POINT_NOT_FOUND` | Time point not found |
| TimeTravel | `TT_DATA_GAP` | Missing data in range |

---

## Next Steps

- [Advanced Features](./advanced.md) - Time travel, branching, CDC
- [Architecture](./architecture.md) - Understanding DoSQL internals
