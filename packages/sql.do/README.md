# @dotdo/sql.do

Client SDK for DoSQL - SQL database on Cloudflare Workers.

## Installation

```bash
pnpm add @dotdo/sql.do
```

## Usage

```typescript
import { createSQLClient } from '@dotdo/sql.do';

const client = createSQLClient({
  url: 'https://sql.example.com',
  token: 'your-token',
});

// Execute queries
const users = await client.query<{ id: number; name: string }>(
  'SELECT * FROM users WHERE active = ?',
  [true]
);

console.log(users.rows);

// Execute statements
await client.exec(
  'INSERT INTO users (name, email) VALUES (?, ?)',
  ['Alice', 'alice@example.com']
);

// Use transactions
await client.transaction(async (tx) => {
  await tx.exec('UPDATE accounts SET balance = balance - ? WHERE id = ?', [100, 1]);
  await tx.exec('UPDATE accounts SET balance = balance + ? WHERE id = ?', [100, 2]);
});

// Prepared statements
const stmt = await client.prepare('SELECT * FROM users WHERE id = ?');
const user = await client.execute(stmt, [1]);

// Time travel queries
const historicalData = await client.query(
  'SELECT * FROM orders',
  [],
  { asOf: new Date('2024-01-01') }
);

await client.close();
```

## API

### `createSQLClient(config)`

Create a new SQL client instance.

```typescript
interface SQLClientConfig {
  url: string;        // DoSQL endpoint URL
  token?: string;     // Authentication token
  database?: string;  // Database name
  timeout?: number;   // Request timeout (ms)
  retry?: RetryConfig;
}
```

### Query Methods

- `query<T>(sql, params?, options?)` - Execute SELECT queries
- `exec(sql, params?, options?)` - Execute INSERT/UPDATE/DELETE/DDL
- `prepare(sql)` - Prepare a statement for reuse
- `execute(statement, params?, options?)` - Execute prepared statement
- `batch(statements)` - Execute multiple statements

### Transaction Methods

- `beginTransaction(options?)` - Start a transaction
- `commit(transactionId)` - Commit a transaction
- `rollback(transactionId)` - Rollback a transaction
- `transaction(fn, options?)` - Execute function in transaction

### Other Methods

- `getSchema(tableName)` - Get table schema
- `ping()` - Check connection health
- `close()` - Close connection

## Types

Import types for type-safe usage:

```typescript
import type {
  SQLValue,
  QueryResult,
  TransactionState,
  TableSchema,
} from '@dotdo/sql.do';
```

## License

MIT
