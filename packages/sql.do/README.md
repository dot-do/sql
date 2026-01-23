> **Developer Preview** - This package is under active development. APIs may change. Not recommended for production use.

# @dotdo/sql.do

[![npm version](https://img.shields.io/npm/v/@dotdo/sql.do.svg)](https://www.npmjs.com/package/@dotdo/sql.do)
[![bundle size](https://img.shields.io/bundlephobia/minzip/@dotdo/sql.do)](https://bundlephobia.com/package/@dotdo/sql.do)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.7-blue.svg)](https://www.typescriptlang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Client SDK for DoSQL - SQL database on Cloudflare Workers with CapnWeb RPC.

## Status

| Property | Value |
|----------|-------|
| Current version | 0.1.0 |
| Stability | Experimental |
| Breaking changes | Expected before 1.0 |

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
  - [createSQLClient](#createsqlclientconfig)
  - [DoSQLClient](#dosqlclient)
    - [query](#querytsql-params-options)
    - [exec](#execsql-params-options)
    - [prepare](#preparesql)
    - [execute](#executestatement-params-options)
    - [beginTransaction](#begintransactionoptions)
    - [commit](#committransactionid)
    - [rollback](#rollbacktransactionid)
    - [transaction](#transactionfn-options)
    - [batch](#batchstatements)
    - [getSchema](#getschematablename)
    - [ping](#ping)
    - [connect](#connect)
    - [isConnected](#isconnected)
    - [close](#close)
    - [on](#onevent-listener)
    - [off](#offevent-listener)
  - [Event Types](#event-types)
  - [TransactionContext](#transactioncontext)
  - [SQLError](#sqlerror)
- [Type Reference](#type-reference)
  - [Configuration Types](#configuration-types)
  - [Query Types](#query-types)
  - [Transaction Types](#transaction-types)
  - [Schema Types](#schema-types)
  - [Branded Types](#branded-types)
- [Advanced Patterns](#advanced-patterns)
  - [Type-Safe Queries](#type-safe-queries)
  - [Transaction Patterns](#transaction-patterns)
  - [Batch Operations](#batch-operations)
  - [Prepared Statements](#prepared-statements)
  - [Time Travel Queries](#time-travel-queries)
  - [Idempotency Keys](#idempotency-keys)
  - [Idempotency Cache Management](#idempotency-cache-management)
- [Error Handling](#error-handling)
- [Configuration Options](#configuration-options)
- [Integration Examples](#integration-examples)
- [Performance Tips](#performance-tips)
- [Experimental Features](#experimental-features)

## Stability

### Stability Legend

- :green_circle: **Stable** - API is stable and unlikely to change. Safe for production use.
- :yellow_circle: **Beta** - API is mostly stable but may have minor changes. Use with caution in production.
- :red_circle: **Experimental** - API is under active development and may change significantly. Not recommended for production.

### API Stability by Category

| API | Methods | Stability |
|-----|---------|-----------|
| Core query execution | `query`, `exec` | :green_circle: Stable |
| Transaction management | `transaction`, `beginTransaction`, `commit`, `rollback` | :green_circle: Stable |
| Prepared statements | `prepare`, `execute` | :green_circle: Stable |
| Connection management | `createSQLClient`, `connect`, `isConnected`, `close`, `ping` | :green_circle: Stable |
| Event listeners | `on`, `off` | :green_circle: Stable |
| Batch operations | `batch` | :green_circle: Stable |
| Time travel queries | `asOf` option | :red_circle: Experimental |
| Schema introspection | `getSchema` | :red_circle: Experimental |
| CDC types and utilities | CDC-related exports | :red_circle: Experimental |
| Sharding types | Shard-related types | :red_circle: Experimental |
| Client capabilities | Capability types | :red_circle: Experimental |

## Version Compatibility

| Dependency | Version |
|------------|---------|
| Node.js | 18+ |
| TypeScript | 5.3+ |

## Installation

```bash
npm install @dotdo/sql.do
# or
pnpm add @dotdo/sql.do
# or
yarn add @dotdo/sql.do
```

## Quick Start

```typescript
import { createSQLClient } from '@dotdo/sql.do';

const client = createSQLClient({
  url: 'https://sql.example.com',
  token: 'your-token',
});

// Execute queries with type safety
interface User {
  id: number;
  name: string;
  email: string;
}

const users = await client.query<User>(
  'SELECT * FROM users WHERE active = ?',
  [true]
);

console.log(users.rows); // User[]

// Execute mutations
await client.exec(
  'INSERT INTO users (name, email) VALUES (?, ?)',
  ['Alice', 'alice@example.com']
);

// Use transactions
await client.transaction(async (tx) => {
  await tx.exec('UPDATE accounts SET balance = balance - ? WHERE id = ?', [100, 1]);
  await tx.exec('UPDATE accounts SET balance = balance + ? WHERE id = ?', [100, 2]);
});

await client.close();
```

---

## API Reference

### `createSQLClient(config)`

Factory function to create a new SQL client instance. This is the recommended way to create clients.

```typescript
function createSQLClient(config: SQLClientConfig): SQLClient;
```

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `config` | `SQLClientConfig` | Client configuration options |

**Returns:** `SQLClient` - A new client instance

**Example:**

```typescript
import { createSQLClient } from '@dotdo/sql.do';

const client = createSQLClient({
  url: 'https://sql.example.com',
  token: 'your-auth-token',
  database: 'mydb',
  timeout: 30000,
  retry: {
    maxRetries: 3,
    baseDelayMs: 100,
    maxDelayMs: 5000,
  },
  idempotency: {
    enabled: true,
    keyPrefix: 'my-service',
  },
});
```

---

### `DoSQLClient`

The main SQL client class implementing the `SQLClient` interface.

#### `query<T>(sql, params?, options?)`

Execute a SELECT query and return typed results.

```typescript
async query<T = Record<string, SQLValue>>(
  sql: string,
  params?: SQLValue[],
  options?: QueryOptions
): Promise<QueryResult<T>>;
```

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `sql` | `string` | SQL SELECT query |
| `params` | `SQLValue[]` | Parameter values for placeholders |
| `options` | `QueryOptions` | Query execution options |

**Returns:** `Promise<QueryResult<T>>`

**Example:**

```typescript
interface Product {
  id: number;
  name: string;
  price: number;
}

// Simple query
const result = await client.query<Product>('SELECT * FROM products');
console.log(result.rows); // Product[]

// Parameterized query
const expensive = await client.query<Product>(
  'SELECT * FROM products WHERE price > ?',
  [100]
);

// With options
const snapshot = await client.query<Product>(
  'SELECT * FROM products',
  [],
  { asOf: new Date('2025-01-01') }
);
```

---

#### `exec(sql, params?, options?)`

Execute a mutation statement (INSERT, UPDATE, DELETE) or DDL.

```typescript
async exec(
  sql: string,
  params?: SQLValue[],
  options?: QueryOptions
): Promise<QueryResult>;
```

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `sql` | `string` | SQL statement to execute |
| `params` | `SQLValue[]` | Parameter values for placeholders |
| `options` | `QueryOptions` | Query execution options |

**Returns:** `Promise<QueryResult>` with `rowsAffected` count

**Example:**

```typescript
// Insert
const insertResult = await client.exec(
  'INSERT INTO users (name, email) VALUES (?, ?)',
  ['Alice', 'alice@example.com']
);
console.log(`Inserted ${insertResult.rowsAffected} row(s)`);
console.log(`Last ID: ${insertResult.lastInsertRowid}`);

// Update
const updateResult = await client.exec(
  'UPDATE users SET status = ? WHERE last_login < ?',
  ['inactive', '2024-01-01']
);
console.log(`Updated ${updateResult.rowsAffected} row(s)`);

// Delete
const deleteResult = await client.exec(
  'DELETE FROM logs WHERE created_at < ?',
  ['2023-01-01']
);
```

---

#### `prepare(sql)`

Prepare a SQL statement for repeated execution.

```typescript
async prepare(sql: string): Promise<PreparedStatement>;
```

**Returns:** `Promise<PreparedStatement>` - Handle for the prepared statement

**Example:**

```typescript
const stmt = await client.prepare(
  'INSERT INTO events (type, data) VALUES (?, ?)'
);

// Execute multiple times efficiently
await client.execute(stmt, ['click', '{"x": 100}']);
await client.execute(stmt, ['scroll', '{"y": 500}']);
await client.execute(stmt, ['submit', '{"form": "login"}']);
```

---

#### `execute<T>(statement, params?, options?)`

Execute a prepared statement with parameters.

```typescript
async execute<T = Record<string, SQLValue>>(
  statement: PreparedStatement,
  params?: SQLValue[],
  options?: QueryOptions
): Promise<QueryResult<T>>;
```

**Example:**

```typescript
const stmt = await client.prepare('SELECT * FROM users WHERE department = ?');

const engineering = await client.execute<User>(stmt, ['engineering']);
const sales = await client.execute<User>(stmt, ['sales']);
const marketing = await client.execute<User>(stmt, ['marketing']);
```

---

#### `beginTransaction(options?)`

Start a new database transaction.

```typescript
async beginTransaction(options?: TransactionOptions): Promise<TransactionState>;
```

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `options.isolationLevel` | `IsolationLevel` | Transaction isolation level |
| `options.readOnly` | `boolean` | Whether transaction is read-only |
| `options.timeout` | `number` | Transaction timeout in ms |

**Returns:** `Promise<TransactionState>`

**Example:**

```typescript
const tx = await client.beginTransaction({
  isolationLevel: 'SERIALIZABLE',
  readOnly: false,
});

try {
  await client.exec('UPDATE ...', [], { transactionId: tx.id });
  await client.commit(tx.id);
} catch (error) {
  await client.rollback(tx.id);
  throw error;
}
```

---

#### `commit(transactionId)`

Commit a transaction, making all changes permanent.

```typescript
async commit(transactionId: TransactionId): Promise<LSN>;
```

**Returns:** `Promise<LSN>` - Log Sequence Number of the commit

---

#### `rollback(transactionId)`

Roll back a transaction, discarding all changes.

```typescript
async rollback(transactionId: TransactionId): Promise<void>;
```

---

#### `transaction<T>(fn, options?)`

Execute a function within an auto-managed transaction. Commits on success, rolls back on error.

```typescript
async transaction<T>(
  fn: (tx: TransactionContext) => Promise<T>,
  options?: TransactionOptions
): Promise<T>;
```

**Example:**

```typescript
// Automatic commit/rollback
const result = await client.transaction(async (tx) => {
  await tx.exec('UPDATE accounts SET balance = balance - ? WHERE id = ?', [amount, fromId]);
  await tx.exec('UPDATE accounts SET balance = balance + ? WHERE id = ?', [amount, toId]);

  // Return value from transaction
  return { transferred: amount, from: fromId, to: toId };
});

// Read-only transaction for consistent reads
const report = await client.transaction(async (tx) => {
  const orders = await tx.query<Order>('SELECT * FROM orders WHERE date > ?', [startDate]);
  const total = await tx.query<{sum: number}>('SELECT SUM(amount) as sum FROM orders');
  return { orders: orders.rows, total: total.rows[0].sum };
}, { readOnly: true, isolationLevel: 'SNAPSHOT' });
```

---

#### `batch(statements)`

Execute multiple statements in a single network round-trip.

```typescript
async batch(
  statements: Array<{ sql: string; params?: SQLValue[] }>
): Promise<QueryResult[]>;
```

**Example:**

```typescript
const results = await client.batch([
  { sql: 'INSERT INTO logs (msg) VALUES (?)', params: ['event1'] },
  { sql: 'INSERT INTO logs (msg) VALUES (?)', params: ['event2'] },
  { sql: 'INSERT INTO logs (msg) VALUES (?)', params: ['event3'] },
]);

console.log(`Inserted ${results.length} log entries`);
```

---

#### `getSchema(tableName)`

Retrieve the schema definition for a table.

```typescript
async getSchema(tableName: string): Promise<TableSchema | null>;
```

**Example:**

```typescript
const schema = await client.getSchema('users');

if (schema) {
  console.log(`Table: ${schema.name}`);
  console.log(`Primary Key: ${schema.primaryKey.join(', ')}`);

  for (const col of schema.columns) {
    console.log(`  ${col.name}: ${col.type}${col.nullable ? '' : ' NOT NULL'}`);
  }
}
```

---

#### `ping()`

Check connection health and measure latency.

```typescript
async ping(): Promise<{ latency: number }>;
```

**Example:**

```typescript
const { latency } = await client.ping();
console.log(`Database latency: ${latency.toFixed(2)}ms`);

if (latency > 1000) {
  console.warn('High latency detected');
}
```

---

#### `connect()`

Explicitly establish a WebSocket connection to the database.

```typescript
async connect(): Promise<void>;
```

This method is optional - the client will automatically connect on first query if not already connected. However, calling `connect()` explicitly allows you to:
- Pre-establish the connection before queries are needed
- Handle connection errors separately from query errors
- Wait for the connection to be ready

If already connected, this method returns immediately without reconnecting. Multiple concurrent calls to `connect()` will share the same connection attempt.

**Throws:** `ConnectionError` when connection fails (network error, auth failure, etc.)

**Example:**

```typescript
const client = createSQLClient({
  url: 'https://sql.example.com',
  token: 'your-token',
});

// Pre-connect before queries
try {
  await client.connect();
  console.log('Connected successfully');
} catch (error) {
  if (error instanceof ConnectionError) {
    console.error('Failed to connect:', error.message);
  }
}

// Now queries won't have connection latency
const result = await client.query('SELECT 1');
```

---

#### `isConnected()`

Check if the client is currently connected to the database.

```typescript
isConnected(): boolean;
```

Returns `true` if a WebSocket connection is established and in the OPEN state. Note that this is a point-in-time check; the connection could change state immediately after this method returns.

**Returns:** `true` if connected, `false` otherwise

**Example:**

```typescript
const client = createSQLClient({ url: 'https://sql.example.com' });

console.log(client.isConnected()); // false (not connected yet)

await client.connect();
console.log(client.isConnected()); // true

await client.close();
console.log(client.isConnected()); // false
```

---

#### `close()`

Close the connection and release resources.

```typescript
async close(): Promise<void>;
```

---

#### `on(event, listener)`

Register an event listener for client events.

```typescript
on<K extends keyof ClientEventMap>(event: K, listener: ClientEventListener<K>): this;
```

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `event` | `'connected' \| 'disconnected' \| 'error'` | The event name to listen for |
| `listener` | `(event: ClientEventMap[K]) => void` | Callback function invoked when the event occurs |

**Returns:** The client instance (for method chaining)

**Example:**

```typescript
const client = createSQLClient({ url: 'wss://sql.example.com' });

// Listen for connection events
client.on('connected', (event) => {
  console.log(`Connected to ${event.url} at ${event.timestamp}`);
});

// Listen for disconnection events
client.on('disconnected', (event) => {
  console.log(`Disconnected from ${event.url}: ${event.reason}`);
});

// Listen for error events
client.on('error', (event) => {
  console.error(`Error in ${event.context}:`, event.error);
  if (event.requestId) {
    console.error(`Request ID: ${event.requestId}`);
  }
});

await client.connect();
```

---

#### `off(event, listener)`

Remove a previously registered event listener.

```typescript
off<K extends keyof ClientEventMap>(event: K, listener: ClientEventListener<K>): this;
```

**Parameters:**

| Name | Type | Description |
|------|------|-------------|
| `event` | `'connected' \| 'disconnected' \| 'error'` | The event name to stop listening for |
| `listener` | `(event: ClientEventMap[K]) => void` | The callback function to remove |

**Returns:** The client instance (for method chaining)

**Example:**

```typescript
const onConnected = (event: ConnectedEvent): void => {
  console.log(`Connected to ${event.url}`);
};

// Register listener
client.on('connected', onConnected);

// Later, remove the listener
client.off('connected', onConnected);
```

---

### Event Types

#### `ClientEventMap`

Map of event names to their event data types.

```typescript
interface ClientEventMap {
  connected: ConnectedEvent;
  disconnected: DisconnectedEvent;
  error: ErrorEvent;
}
```

#### `ConnectedEvent`

Event data emitted when a WebSocket connection is established.

```typescript
interface ConnectedEvent {
  /** The URL that was connected to */
  url: string;
  /** Timestamp when the connection was established */
  timestamp: Date;
}
```

#### `DisconnectedEvent`

Event data emitted when a WebSocket connection is closed.

```typescript
interface DisconnectedEvent {
  /** The URL that was disconnected from */
  url: string;
  /** Timestamp when the disconnection occurred */
  timestamp: Date;
  /** Optional reason for the disconnection */
  reason?: string;
}
```

#### `ErrorEvent`

Event data emitted when an error occurs.

```typescript
interface ErrorEvent {
  /** The error that occurred */
  error: Error;
  /** Timestamp when the error occurred */
  timestamp: Date;
  /** Context about where the error occurred */
  context: 'message_parse' | 'connection' | 'rpc';
  /** Optional request ID if the error was associated with a specific request */
  requestId?: string;
}
```

---

### `TransactionContext`

Scoped context for executing operations within a transaction. Passed to the callback in `client.transaction()`.

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `transactionId` | `TransactionId` | The transaction's unique identifier |

#### Methods

| Method | Description |
|--------|-------------|
| `exec(sql, params?)` | Execute a mutation within the transaction |
| `query<T>(sql, params?)` | Execute a query within the transaction |

**Example:**

```typescript
await client.transaction(async (tx) => {
  console.log(`Transaction ID: ${tx.transactionId}`);

  // All operations use the same transaction
  await tx.exec('INSERT INTO users (name) VALUES (?)', ['Alice']);

  // Reads see uncommitted changes from this transaction
  const result = await tx.query<User>('SELECT * FROM users WHERE name = ?', ['Alice']);
  console.log(`Found ${result.rows.length} user(s)`);
});
```

---

### `SQLError`

Error class for SQL operation failures.

```typescript
class SQLError extends Error {
  readonly code: string;
  readonly details?: unknown;
}
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `code` | `string` | Machine-readable error code |
| `message` | `string` | Human-readable error message |
| `details` | `unknown` | Additional error context |

#### Common Error Codes

| Code | Description |
|------|-------------|
| `SYNTAX_ERROR` | Invalid SQL syntax |
| `CONSTRAINT_VIOLATION` | Unique/FK/check constraint failed |
| `TABLE_NOT_FOUND` | Referenced table doesn't exist |
| `COLUMN_NOT_FOUND` | Referenced column doesn't exist |
| `TIMEOUT` | Query execution timed out |
| `CONNECTION_CLOSED` | WebSocket connection closed |
| `TRANSACTION_NOT_FOUND` | Invalid transaction ID |
| `TRANSACTION_ABORTED` | Transaction was rolled back |
| `DEADLOCK_DETECTED` | Deadlock between transactions |
| `SERIALIZATION_FAILURE` | Serializable isolation conflict |
| `UNAUTHORIZED` | Authentication failed |
| `FORBIDDEN` | Permission denied |

---

## Type Reference

### Configuration Types

#### `SQLClientConfig`

```typescript
interface SQLClientConfig {
  /** DoSQL endpoint URL (HTTP/HTTPS or WS/WSS) */
  url: string;

  /** Authentication token */
  token?: string;

  /** Database name (uses server default if not specified) */
  database?: string;

  /** Request timeout in milliseconds (default: 30000) */
  timeout?: number;

  /** Retry configuration for transient failures */
  retry?: RetryConfig;

  /** Idempotency key configuration for mutations */
  idempotency?: IdempotencyConfig;
}
```

#### `RetryConfig`

```typescript
interface RetryConfig {
  /** Maximum retry attempts (default: 3) */
  maxRetries: number;

  /** Base delay for exponential backoff in ms (default: 100) */
  baseDelayMs: number;

  /** Maximum delay between retries in ms (default: 5000) */
  maxDelayMs: number;
}
```

#### `IdempotencyConfig`

```typescript
interface IdempotencyConfig {
  /** Enable automatic idempotency key generation (default: true) */
  enabled: boolean;

  /** Prefix for generated keys */
  keyPrefix?: string;

  /** Time-to-live for idempotency keys in milliseconds (server-side, default: 24 hours) */
  ttlMs?: number;

  /** Maximum number of entries in the client-side idempotency key cache (default: 1000) */
  maxCacheSize?: number;

  /** Time-to-live for cached idempotency keys in milliseconds (client-side, default: 5 minutes) */
  cacheTtlMs?: number;

  /** Interval in milliseconds for periodic cache cleanup (default: 60000 = 1 minute) */
  cleanupIntervalMs?: number;
}
```

---

### Query Types

#### `QueryOptions`

```typescript
interface QueryOptions {
  /** Transaction ID for transactional queries */
  transactionId?: TransactionId;

  /** Read from a specific point in time */
  asOf?: Date | LSN;

  /** Query timeout in milliseconds */
  timeout?: number;

  /** Target shard for sharded queries */
  shardId?: ShardId;

  /** Named parameters (alternative to positional) */
  namedParams?: Record<string, unknown>;

  /** Branch/namespace for multi-tenant isolation */
  branch?: string;

  /** Stream results in chunks */
  streaming?: boolean;

  /** Maximum rows to return */
  limit?: number;

  /** Offset for pagination */
  offset?: number;
}
```

#### `QueryResult<T>`

```typescript
interface QueryResult<T = Record<string, SQLValue>> {
  /** Result rows */
  rows: T[];

  /** Column names */
  columns: string[];

  /** Column types */
  columnTypes?: ColumnType[];

  /** Number of rows affected (for mutations) */
  rowsAffected: number;

  /** Last inserted row ID */
  lastInsertRowid?: bigint;

  /** Query execution time in ms */
  duration: number;

  /** Current LSN after query */
  lsn?: LSN;

  /** More rows available (pagination) */
  hasMore?: boolean;

  /** Cursor for next page */
  cursor?: string;
}
```

#### `SQLValue`

```typescript
type SQLValue = string | number | boolean | null | Uint8Array | bigint;
```

#### `PreparedStatement`

```typescript
interface PreparedStatement {
  sql: string;
  hash: StatementHash;
}
```

---

### Transaction Types

#### `TransactionOptions`

```typescript
interface TransactionOptions {
  isolationLevel?: IsolationLevel;
  readOnly?: boolean;
  timeout?: number;
  branch?: string;
}
```

#### `IsolationLevel`

```typescript
type IsolationLevel =
  | 'READ_UNCOMMITTED'
  | 'READ_COMMITTED'
  | 'REPEATABLE_READ'
  | 'SERIALIZABLE'
  | 'SNAPSHOT';
```

#### `TransactionState`

```typescript
interface TransactionState {
  id: TransactionId;
  isolationLevel: IsolationLevel;
  readOnly: boolean;
  startedAt: Date;
  snapshotLSN: LSN;
}
```

---

### Schema Types

#### `TableSchema`

```typescript
interface TableSchema {
  name: string;
  columns: ColumnDefinition[];
  primaryKey: string[];
  indexes?: IndexDefinition[];
  foreignKeys?: ForeignKeyDefinition[];
}
```

#### `ColumnDefinition`

```typescript
interface ColumnDefinition {
  name: string;
  type: ColumnType | string;
  nullable: boolean;
  primaryKey: boolean;
  autoIncrement?: boolean;
  defaultValue?: SQLValue | string;
  unique?: boolean;
  doc?: string;
}
```

#### `ColumnType`

```typescript
// SQL-style types
type SQLColumnType = 'INTEGER' | 'REAL' | 'TEXT' | 'BLOB' | 'NULL' | 'BOOLEAN' | 'DATETIME' | 'JSON';

// JavaScript-style types
type JSColumnType = 'string' | 'number' | 'bigint' | 'boolean' | 'date' | 'timestamp' | 'json' | 'blob' | 'null' | 'unknown';

// Union of both
type ColumnType = SQLColumnType | JSColumnType;
```

---

### Branded Types

Branded types provide compile-time safety for identifiers:

```typescript
// Transaction ID - prevents mixing with other string IDs
type TransactionId = string & { readonly [TransactionIdBrand]: never };

// Log Sequence Number - prevents mixing with other bigints
type LSN = bigint & { readonly [LSNBrand]: never };

// Statement Hash - for prepared statement handles
type StatementHash = string & { readonly [StatementHashBrand]: never };

// Shard ID - for sharded database operations
type ShardId = string & { readonly [ShardIdBrand]: never };
```

**Brand Constructors:**

```typescript
import {
  createTransactionId,
  createLSN,
  createStatementHash,
  createShardId,
} from '@dotdo/sql.do';

const txId = createTransactionId('tx-123');
const lsn = createLSN(BigInt(1000));
const hash = createStatementHash('abc123');
const shardId = createShardId('shard-1');
```

---

## Advanced Patterns

### Type-Safe Queries

Define interfaces for your tables and use generics for full type safety:

```typescript
// Define your schema types
interface User {
  id: number;
  name: string;
  email: string;
  created_at: string;
}

interface Order {
  id: number;
  user_id: number;
  total: number;
  status: 'pending' | 'completed' | 'cancelled';
}

// Queries return typed results
const users = await client.query<User>('SELECT * FROM users');
users.rows.forEach(user => {
  console.log(user.name); // TypeScript knows this is a string
});

// Aggregate queries with custom types
interface UserStats {
  total_users: number;
  active_users: number;
  avg_orders: number;
}

const stats = await client.query<UserStats>(`
  SELECT
    COUNT(*) as total_users,
    COUNT(*) FILTER (WHERE active) as active_users,
    AVG(order_count) as avg_orders
  FROM users
`);
```

---

### Transaction Patterns

#### Optimistic Locking

```typescript
async function updateWithVersion(
  client: SQLClient,
  id: number,
  newName: string,
  currentVersion: number
): Promise<boolean> {
  const result = await client.exec(
    'UPDATE users SET name = ?, version = version + 1 WHERE id = ? AND version = ?',
    [newName, id, currentVersion]
  );

  if (result.rowsAffected === 0) {
    throw new Error('Optimistic lock failed - record was modified');
  }

  return true;
}
```

#### Savepoints Pattern

```typescript
await client.transaction(async (tx) => {
  await tx.exec('INSERT INTO orders (user_id, total) VALUES (?, ?)', [userId, 100]);

  try {
    // Attempt to reserve inventory
    await tx.exec('UPDATE inventory SET quantity = quantity - 1 WHERE product_id = ? AND quantity > 0', [productId]);
  } catch (error) {
    // Log the failure but continue with the order
    await tx.exec('INSERT INTO order_notes (order_id, note) VALUES (?, ?)', [orderId, 'Inventory reservation failed']);
  }
});
```

#### Read-Your-Writes Consistency

```typescript
// Insert and immediately read back
const result = await client.transaction(async (tx) => {
  await tx.exec('INSERT INTO users (name, email) VALUES (?, ?)', ['Alice', 'alice@example.com']);

  // This read sees the uncommitted insert
  const user = await tx.query<User>('SELECT * FROM users WHERE email = ?', ['alice@example.com']);

  return user.rows[0];
});
```

---

### Batch Operations

For high-throughput scenarios, batch multiple operations:

```typescript
// Efficient bulk insert
const users = [
  { name: 'Alice', email: 'alice@example.com' },
  { name: 'Bob', email: 'bob@example.com' },
  { name: 'Charlie', email: 'charlie@example.com' },
];

const results = await client.batch(
  users.map(user => ({
    sql: 'INSERT INTO users (name, email) VALUES (?, ?)',
    params: [user.name, user.email],
  }))
);

const totalInserted = results.reduce((sum, r) => sum + r.rowsAffected, 0);
console.log(`Inserted ${totalInserted} users`);
```

---

### Prepared Statements

Optimize repeated queries with prepared statements:

```typescript
// Prepare once
const insertStmt = await client.prepare('INSERT INTO events (type, payload) VALUES (?, ?)');
const selectStmt = await client.prepare('SELECT * FROM events WHERE type = ? LIMIT ?');

// Execute many times
for (const event of events) {
  await client.execute(insertStmt, [event.type, JSON.stringify(event.payload)]);
}

// Reuse for reads
const clicks = await client.execute(selectStmt, ['click', 100]);
const scrolls = await client.execute(selectStmt, ['scroll', 100]);
```

---

### Time Travel Queries

Query historical data using point-in-time snapshots:

```typescript
// Query data as it existed at a specific time
const historicalUsers = await client.query<User>(
  'SELECT * FROM users',
  [],
  { asOf: new Date('2025-01-01T00:00:00Z') }
);

// Query at a specific LSN
import { createLSN } from '@dotdo/sql.do';

const snapshotUsers = await client.query<User>(
  'SELECT * FROM users',
  [],
  { asOf: createLSN(BigInt(1000)) }
);

// Compare current vs historical
const current = await client.query<User>('SELECT COUNT(*) as count FROM users');
const lastMonth = await client.query<User>(
  'SELECT COUNT(*) as count FROM users',
  [],
  { asOf: new Date(Date.now() - 30 * 24 * 60 * 60 * 1000) }
);

console.log(`Users grew from ${lastMonth.rows[0].count} to ${current.rows[0].count}`);
```

---

### Idempotency Keys

Prevent duplicate mutations when retrying requests:

```typescript
import { generateIdempotencyKey, isMutationQuery } from '@dotdo/sql.do';

// Automatic (enabled by default)
const client = createSQLClient({
  url: 'https://sql.example.com',
  idempotency: {
    enabled: true,
    keyPrefix: 'payment-service',
  },
});

// Manual generation for custom scenarios
const key = await generateIdempotencyKey(
  'INSERT INTO payments (user_id, amount) VALUES (?, ?)',
  [userId, amount],
  'payment'
);
console.log(key); // "payment-1705432800000-abc12345-7f3a8b2c"

// Check if a query is a mutation
isMutationQuery('INSERT INTO users (name) VALUES (?)'); // true
isMutationQuery('SELECT * FROM users'); // false
```

---

### Idempotency Cache Management

The client maintains an LRU cache of idempotency keys to ensure the same key is used for retries of the same request. The following methods are available for monitoring and managing this cache.

#### `getCacheSize()`

Gets the current number of entries in the idempotency key cache.

```typescript
getCacheSize(): number;
```

**Returns:** The number of entries currently in the cache

**Example:**

```typescript
const client = createSQLClient({ url: 'https://sql.example.com' });

// After some mutations...
console.log(`Cache has ${client.getCacheSize()} entries`);
```

---

#### `clearIdempotencyCache()`

Clears all entries from the idempotency key cache. This forces new idempotency keys to be generated for all future requests.

```typescript
clearIdempotencyCache(): void;
```

**Warning:** Use with caution as this may affect retry semantics for in-flight requests.

**Example:**

```typescript
// Clear all cached idempotency keys
client.clearIdempotencyCache();
console.log(`Cache cleared, now has ${client.getCacheSize()} entries`); // 0
```

---

#### `getIdempotencyCacheStats()`

Gets statistics about the idempotency key cache. Useful for monitoring cache effectiveness and tuning configuration.

```typescript
getIdempotencyCacheStats(): IdempotencyCacheStats;
```

**Returns:** `IdempotencyCacheStats` object with the following properties:

| Property | Type | Description |
|----------|------|-------------|
| `size` | `number` | Current number of entries in the cache |
| `hits` | `number` | Number of cache hits (key reused for retry) |
| `misses` | `number` | Number of cache misses (new key generated) |
| `evictions` | `number` | Number of entries evicted due to LRU or TTL |
| `maxSize` | `number` | Maximum configured cache size |
| `ttlMs` | `number` | TTL in milliseconds for cache entries |

**Example:**

```typescript
const stats = client.getIdempotencyCacheStats();

// Calculate hit rate
const hitRate = stats.hits / (stats.hits + stats.misses) * 100;
console.log(`Cache hit rate: ${hitRate.toFixed(1)}%`);

// Monitor cache health
console.log(`Size: ${stats.size}/${stats.maxSize}`);
console.log(`Evictions: ${stats.evictions}`);

// Alert on low hit rate (may indicate retries aren't using cached keys)
if (hitRate < 10 && stats.hits + stats.misses > 100) {
  console.warn('Low cache hit rate - check idempotency configuration');
}
```

---

#### `cleanupIdempotencyCache()`

Triggers manual cleanup of expired entries from the idempotency cache. This is normally done automatically on a timer (configurable via `cleanupIntervalMs`), but can be called manually to force immediate cleanup.

```typescript
cleanupIdempotencyCache(): number;
```

**Returns:** The number of expired entries that were removed

**Example:**

```typescript
// Force cleanup before memory-sensitive operation
const removed = client.cleanupIdempotencyCache();
console.log(`Removed ${removed} expired entries`);

// Combine with stats for monitoring
const statsBefore = client.getIdempotencyCacheStats();
const removedCount = client.cleanupIdempotencyCache();
const statsAfter = client.getIdempotencyCacheStats();
console.log(`Cleanup removed ${removedCount} entries (${statsBefore.size} -> ${statsAfter.size})`);
```

---

#### Idempotency Cache Configuration

Configure the cache behavior when creating the client:

```typescript
const client = createSQLClient({
  url: 'https://sql.example.com',
  idempotency: {
    enabled: true,
    keyPrefix: 'my-service',
    maxCacheSize: 1000,           // Maximum entries (default: 1000)
    cacheTtlMs: 5 * 60 * 1000,    // Entry TTL: 5 minutes (default)
    cleanupIntervalMs: 60 * 1000, // Cleanup interval: 1 minute (default)
  },
});
```

---

## Error Handling

The SDK provides several error classes for different failure scenarios:

### `ConnectionError`

Thrown when the client fails to establish or maintain a connection to the database.

```typescript
import { ConnectionError } from '@dotdo/sql.do';

try {
  await client.connect();
} catch (error) {
  if (error instanceof ConnectionError) {
    console.error('Failed to connect:', error.message);
    // error.cause may contain the underlying error
  }
}
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `message` | `string` | Human-readable error message |
| `cause` | `Error \| undefined` | Underlying error that caused the connection failure |

### `TimeoutError`

Thrown when an operation exceeds its configured timeout duration.

```typescript
import { TimeoutError } from '@dotdo/sql.do';

try {
  await client.query('SELECT * FROM large_table', [], { timeout: 5000 });
} catch (error) {
  if (error instanceof TimeoutError) {
    console.error('Query timed out after', error.timeout, 'ms');
    console.error('Operation:', error.operation);
  }
}
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `message` | `string` | Human-readable error message |
| `timeout` | `number` | The timeout duration in milliseconds |
| `operation` | `string` | The operation that timed out (e.g., 'query', 'connect') |

### `SQLError`

```typescript
import { SQLError } from '@dotdo/sql.do';

try {
  await client.exec('INSERT INTO users (id, name) VALUES (?, ?)', [1, 'Alice']);
} catch (error) {
  if (error instanceof SQLError) {
    switch (error.code) {
      case 'CONSTRAINT_VIOLATION':
        console.log('User ID already exists');
        break;

      case 'SYNTAX_ERROR':
        console.log('Invalid SQL syntax:', error.message);
        break;

      case 'TIMEOUT':
        console.log('Query timed out, consider optimizing');
        break;

      case 'DEADLOCK_DETECTED':
        console.log('Deadlock detected, retrying...');
        // Implement retry logic
        break;

      case 'UNAUTHORIZED':
        console.log('Authentication failed');
        break;

      default:
        console.error(`SQL Error [${error.code}]: ${error.message}`);
        if (error.details) {
          console.error('Details:', error.details);
        }
    }
  } else {
    // Non-SQL error (network, etc.)
    throw error;
  }
}
```

### Retry Pattern

```typescript
async function withRetry<T>(
  fn: () => Promise<T>,
  maxRetries = 3,
  baseDelay = 100
): Promise<T> {
  let lastError: Error | undefined;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      if (error instanceof SQLError) {
        const retryableCodes = ['TIMEOUT', 'CONNECTION_CLOSED', 'DEADLOCK_DETECTED'];

        if (!retryableCodes.includes(error.code)) {
          throw error; // Non-retryable, fail fast
        }
      }

      lastError = error as Error;
      const delay = baseDelay * Math.pow(2, attempt);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }

  throw lastError;
}

// Usage
const result = await withRetry(() =>
  client.query('SELECT * FROM large_table')
);
```

---

## Configuration Options

### Full Configuration Example

```typescript
const client = createSQLClient({
  // Required: DoSQL endpoint
  url: 'https://sql.example.com',

  // Authentication
  token: process.env.DOSQL_TOKEN,

  // Database selection
  database: 'production',

  // Timeouts
  timeout: 30000, // 30 seconds

  // Retry configuration
  retry: {
    maxRetries: 3,
    baseDelayMs: 100,
    maxDelayMs: 5000,
  },

  // Idempotency for mutations
  idempotency: {
    enabled: true,
    keyPrefix: 'my-service',
    ttlMs: 24 * 60 * 60 * 1000, // 24 hours
  },
});
```

### Environment-Based Configuration

```typescript
const config: SQLClientConfig = {
  url: process.env.DOSQL_URL!,
  token: process.env.DOSQL_TOKEN,
  database: process.env.DOSQL_DATABASE,
  timeout: parseInt(process.env.DOSQL_TIMEOUT || '30000'),
  retry: {
    maxRetries: parseInt(process.env.DOSQL_MAX_RETRIES || '3'),
    baseDelayMs: 100,
    maxDelayMs: 5000,
  },
};
```

---

## Integration Examples

### Cloudflare Workers

```typescript
import { createSQLClient, type SQLClient } from '@dotdo/sql.do';

export interface Env {
  DOSQL_URL: string;
  DOSQL_TOKEN: string;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const client = createSQLClient({
      url: env.DOSQL_URL,
      token: env.DOSQL_TOKEN,
    });

    try {
      const users = await client.query<{ id: number; name: string }>(
        'SELECT id, name FROM users LIMIT 10'
      );

      return Response.json({ users: users.rows });
    } finally {
      await client.close();
    }
  },
};
```

### Hono Framework

```typescript
import { Hono } from 'hono';
import { createSQLClient } from '@dotdo/sql.do';

const app = new Hono<{ Bindings: { DOSQL_URL: string; DOSQL_TOKEN: string } }>();

// Middleware to inject client
app.use('*', async (c, next) => {
  const client = createSQLClient({
    url: c.env.DOSQL_URL,
    token: c.env.DOSQL_TOKEN,
  });

  c.set('db', client);
  await next();
  await client.close();
});

app.get('/users', async (c) => {
  const db = c.get('db');
  const result = await db.query('SELECT * FROM users');
  return c.json(result.rows);
});

export default app;
```

### Repository Pattern

```typescript
import { createSQLClient, type SQLClient, type QueryResult } from '@dotdo/sql.do';

interface User {
  id: number;
  name: string;
  email: string;
}

class UserRepository {
  constructor(private client: SQLClient) {}

  async findById(id: number): Promise<User | null> {
    const result = await this.client.query<User>(
      'SELECT * FROM users WHERE id = ?',
      [id]
    );
    return result.rows[0] ?? null;
  }

  async findByEmail(email: string): Promise<User | null> {
    const result = await this.client.query<User>(
      'SELECT * FROM users WHERE email = ?',
      [email]
    );
    return result.rows[0] ?? null;
  }

  async create(name: string, email: string): Promise<User> {
    const result = await this.client.query<User>(
      'INSERT INTO users (name, email) VALUES (?, ?) RETURNING *',
      [name, email]
    );
    return result.rows[0];
  }

  async update(id: number, updates: Partial<Omit<User, 'id'>>): Promise<User | null> {
    const fields = Object.keys(updates);
    const values = Object.values(updates);

    const setClause = fields.map(f => `${f} = ?`).join(', ');
    const result = await this.client.query<User>(
      `UPDATE users SET ${setClause} WHERE id = ? RETURNING *`,
      [...values, id]
    );

    return result.rows[0] ?? null;
  }

  async delete(id: number): Promise<boolean> {
    const result = await this.client.exec(
      'DELETE FROM users WHERE id = ?',
      [id]
    );
    return result.rowsAffected > 0;
  }
}
```

---

## Performance Tips

### 1. Use Prepared Statements for Repeated Queries

```typescript
// Slow: Parse query every time
for (const id of userIds) {
  await client.query('SELECT * FROM users WHERE id = ?', [id]);
}

// Fast: Parse once, execute many times
const stmt = await client.prepare('SELECT * FROM users WHERE id = ?');
for (const id of userIds) {
  await client.execute(stmt, [id]);
}
```

### 2. Batch Operations for Bulk Inserts

```typescript
// Slow: Individual requests
for (const record of records) {
  await client.exec('INSERT INTO logs (msg) VALUES (?)', [record.msg]);
}

// Fast: Single batched request
await client.batch(
  records.map(r => ({
    sql: 'INSERT INTO logs (msg) VALUES (?)',
    params: [r.msg],
  }))
);
```

### 3. Use Pagination for Large Result Sets

```typescript
// Memory-efficient pagination
async function* paginatedQuery<T>(
  client: SQLClient,
  sql: string,
  pageSize = 1000
): AsyncGenerator<T[]> {
  let offset = 0;

  while (true) {
    const result = await client.query<T>(
      `${sql} LIMIT ? OFFSET ?`,
      [pageSize, offset]
    );

    if (result.rows.length === 0) break;

    yield result.rows;
    offset += pageSize;

    if (result.rows.length < pageSize) break;
  }
}

// Usage
for await (const batch of paginatedQuery<User>(client, 'SELECT * FROM users')) {
  await processBatch(batch);
}
```

### 4. Use Read-Only Transactions for Reports

```typescript
// Read-only transactions are more efficient
const report = await client.transaction(async (tx) => {
  const users = await tx.query('SELECT COUNT(*) as count FROM users');
  const orders = await tx.query('SELECT SUM(total) as sum FROM orders');
  return { users: users.rows[0], orders: orders.rows[0] };
}, { readOnly: true });
```

### 5. Close Connections When Done

```typescript
const client = createSQLClient({ url: '...' });

try {
  // Use client
} finally {
  await client.close(); // Release resources
}
```

---

## Experimental Features

Import experimental features from the dedicated subpath:

```typescript
import {
  // Idempotency utilities
  generateIdempotencyKey,
  isMutationQuery,

  // CDC types and utilities
  CDCOperationCode,
  isServerCDCEvent,
  isClientCDCEvent,
  serverToClientCDCEvent,
  clientToServerCDCEvent,

  // Response converters
  responseToResult,
  resultToResponse,

  // Client capabilities
  DEFAULT_CLIENT_CAPABILITIES,
} from '@dotdo/sql.do/experimental';
```

> **Warning**: Experimental features may change or be removed without notice. Use with caution in production.

---

## Troubleshooting

### Connection Errors

**Symptoms**: `ConnectionError` when calling `connect()` or executing queries

| Error Code | Cause | Solution |
|------------|-------|----------|
| `CONNECTION_FAILED` | Network issue or invalid URL | Verify URL is correct; check network connectivity |
| `CONNECTION_CLOSED` | WebSocket connection dropped | Implement reconnection logic; check for server issues |
| `UNAUTHORIZED` | Invalid or expired token | Refresh authentication token |

```typescript
import { ConnectionError, SQLError } from '@dotdo/sql.do';

// Handle connection errors gracefully
client.on('error', (event) => {
  if (event.context === 'connection') {
    console.error('Connection error:', event.error.message);
    // Attempt reconnection after delay
    setTimeout(() => client.connect(), 5000);
  }
});

// Pre-connect with error handling
try {
  await client.connect();
} catch (error) {
  if (error instanceof ConnectionError) {
    console.error(`Connection failed: ${error.message}`);
    // Handle based on error type
  }
}
```

### Rate Limiting Errors

**Symptoms**: Queries fail with `RESOURCE_EXHAUSTED` or slow response times

| Error Code | Cause | Solution |
|------------|-------|----------|
| `RESOURCE_EXHAUSTED` | Too many concurrent requests | Implement request queuing; reduce parallelism |
| `TIMEOUT` | Server under heavy load | Increase timeout; retry with backoff |
| `UNAVAILABLE` | Service temporarily unavailable | Retry with exponential backoff |

```typescript
// Configure retry for rate limiting
const client = createSQLClient({
  url: 'https://sql.example.com',
  token: 'your-token',
  retry: {
    maxRetries: 5,
    baseDelayMs: 200,
    maxDelayMs: 10000,
  },
});

// Manual retry for specific errors
import { SQLError, isRetryableError } from '@dotdo/sql.do';

async function queryWithRetry<T>(sql: string, params: unknown[]): Promise<T[]> {
  for (let i = 0; i < 3; i++) {
    try {
      const result = await client.query<T>(sql, params);
      return result.rows;
    } catch (error) {
      if (error instanceof SQLError && isRetryableError(error)) {
        await new Promise(r => setTimeout(r, 100 * Math.pow(2, i)));
        continue;
      }
      throw error;
    }
  }
  throw new Error('Query failed after retries');
}
```

### Transaction Errors

**Symptoms**: Transactions fail to commit or produce unexpected results

| Error Code | Cause | Solution |
|------------|-------|----------|
| `TRANSACTION_NOT_FOUND` | Invalid transaction ID | Ensure transaction is still active |
| `TRANSACTION_ABORTED` | Transaction was rolled back | Check for constraint violations; retry |
| `DEADLOCK_DETECTED` | Concurrent lock conflict | Retry transaction; use consistent ordering |
| `SERIALIZATION_FAILURE` | Isolation conflict | Retry with exponential backoff |

```typescript
// Safe transaction pattern with retry
async function safeTransaction<T>(
  fn: (tx: TransactionContext) => Promise<T>
): Promise<T> {
  const retryableCodes = ['DEADLOCK_DETECTED', 'SERIALIZATION_FAILURE'];

  for (let attempt = 0; attempt < 3; attempt++) {
    try {
      return await client.transaction(fn);
    } catch (error) {
      if (error instanceof SQLError && retryableCodes.includes(error.code)) {
        await new Promise(r => setTimeout(r, 50 * Math.pow(2, attempt)));
        continue;
      }
      throw error;
    }
  }
  throw new Error('Transaction failed after retries');
}

// Usage
await safeTransaction(async (tx) => {
  await tx.exec('UPDATE accounts SET balance = balance - ? WHERE id = ?', [100, 1]);
  await tx.exec('UPDATE accounts SET balance = balance + ? WHERE id = ?', [100, 2]);
});
```

### SQL Errors

**Symptoms**: `SQLError` with various error codes

| Error Code | Cause | Solution |
|------------|-------|----------|
| `SYNTAX_ERROR` | Invalid SQL syntax | Check SQL statement for typos |
| `CONSTRAINT_VIOLATION` | Unique/FK constraint failed | Handle duplicate key; check foreign keys |
| `TABLE_NOT_FOUND` | Table does not exist | Run migrations; check table name spelling |
| `COLUMN_NOT_FOUND` | Column does not exist | Verify column name matches schema |

```typescript
import { SQLError } from '@dotdo/sql.do';

try {
  await client.exec('INSERT INTO users (id, email) VALUES (?, ?)', [1, 'test@example.com']);
} catch (error) {
  if (error instanceof SQLError) {
    switch (error.code) {
      case 'CONSTRAINT_VIOLATION':
        // Handle duplicate key
        console.log('User already exists');
        break;
      case 'TABLE_NOT_FOUND':
        // Handle missing table
        console.error('Users table not found - run migrations');
        break;
      case 'SYNTAX_ERROR':
        // Log for debugging
        console.error('SQL syntax error:', error.message);
        break;
      default:
        throw error;
    }
  }
}
```

### Common Configuration Mistakes

| Issue | Symptom | Fix |
|-------|---------|-----|
| Wrong URL scheme | Connection fails silently | Use `https://` for HTTP or `wss://` for WebSocket |
| Missing token | `UNAUTHORIZED` error | Set `token` in client configuration |
| Timeout too short | `TIMEOUT` on complex queries | Increase `timeout` (default: 30000ms) |
| No retry config | Transient failures not handled | Add `retry` configuration |

```typescript
// Complete configuration example
const client = createSQLClient({
  // Required
  url: 'https://sql.example.com',  // or wss:// for WebSocket

  // Authentication
  token: process.env.DOSQL_TOKEN,

  // Database selection (optional)
  database: 'production',

  // Timeouts
  timeout: 60000,  // 60 seconds for complex queries

  // Retry configuration
  retry: {
    maxRetries: 3,
    baseDelayMs: 100,
    maxDelayMs: 5000,
  },

  // Idempotency for mutations (prevents duplicates on retry)
  idempotency: {
    enabled: true,
    keyPrefix: 'my-service',
  },
});
```

### Performance Issues

| Symptom | Cause | Solution |
|---------|-------|----------|
| Slow queries | Missing indexes | Add indexes; use `EXPLAIN` to analyze |
| High latency | Network round-trips | Use `batch()` for multiple operations |
| Memory issues | Large result sets | Use pagination with `LIMIT`/`OFFSET` |
| Connection overhead | New connection per request | Reuse client instance; use connection pooling |

```typescript
// Batch operations for efficiency
const results = await client.batch([
  { sql: 'INSERT INTO logs (msg) VALUES (?)', params: ['event1'] },
  { sql: 'INSERT INTO logs (msg) VALUES (?)', params: ['event2'] },
  { sql: 'INSERT INTO logs (msg) VALUES (?)', params: ['event3'] },
]);

// Pagination for large datasets
async function* fetchAllUsers(pageSize = 1000): AsyncGenerator<User[]> {
  let offset = 0;
  while (true) {
    const result = await client.query<User>(
      'SELECT * FROM users ORDER BY id LIMIT ? OFFSET ?',
      [pageSize, offset]
    );
    if (result.rows.length === 0) break;
    yield result.rows;
    offset += pageSize;
  }
}
```

---

## License

MIT
