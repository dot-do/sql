# DoSQL CapnWeb RPC Example

> **Pre-release Software**: This is v0.1.0. APIs may change. Not recommended for production use without thorough testing.

This example demonstrates DoSQL with a CapnWeb RPC server running inside a Durable Object.

## Stability

### Stable APIs

- Basic SQL execution via RPC (`exec`, `prepare`, `run`)
- Transaction support
- Health checks (`ping`, `status`)

### Experimental APIs

- CapnWeb RPC protocol (API may change)
- WebSocket pipelining with `.map()` chaining
- Prepared statement management

## Version Compatibility

| Dependency | Version |
|------------|---------|
| Node.js | 18+ |
| TypeScript | 5.3+ |
| Cloudflare Workers | 2024-01-01+ |
| capnweb | 0.4.x |

## Features

- **CapnWeb RPC over WebSocket**: For streaming and pipelining with `.map()` chaining
- **CapnWeb RPC over HTTP**: For simple stateless requests
- **Prepared Statements**: Prepare once, execute many times
- **Transactions**: Execute multiple operations atomically
- **Full SQL Support**: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Client                                    │
│                                                                  │
│  const result = await rpc.exec({ sql: 'SELECT * FROM users' })  │
│    .map(r => r.rows[0])                                         │
│    .map(user => user.name);                                     │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              │ CapnWeb RPC (WebSocket or HTTP)
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Cloudflare Worker                             │
│                                                                  │
│  Routes: /db/:name/* → Durable Object                           │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              │ Durable Object Stub
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│              DoSQLDatabase Durable Object                        │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                   DoSQLRpcTarget                            │ │
│  │                                                             │ │
│  │  exec(sql)     → Execute SQL and return results            │ │
│  │  prepare(sql)  → Return prepared statement handle          │ │
│  │  run(stmtId)   → Execute prepared statement                │ │
│  │  transaction() → Execute multiple ops atomically           │ │
│  │  status()      → Get database status                       │ │
│  │  ping()        → Health check                              │ │
│  │  listTables()  → List all tables                           │ │
│  │  describeTable() → Get table schema                        │ │
│  └────────────────────────────────────────────────────────────┘ │
│                              │                                   │
│                              ▼                                   │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                   SimpleSqlEngine                           │ │
│  │                   (In-memory SQL)                           │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## RPC Methods

| Method | Description |
|--------|-------------|
| `exec({ sql, params? })` | Execute SQL and return results |
| `prepare({ sql })` | Create a prepared statement |
| `run({ stmtId, params? })` | Execute a prepared statement |
| `finalize({ stmtId })` | Close a prepared statement |
| `transaction({ ops })` | Execute multiple operations atomically |
| `status()` | Get database status |
| `ping()` | Health check with timestamp |
| `listTables()` | List all tables |
| `describeTable({ table })` | Get table schema |

## Usage

### WebSocket Client (Recommended)

```typescript
import { newWebSocketRpcSession } from 'capnweb';
import type { DoSQLRpcApi } from './types';

// Connect via WebSocket
const rpc = newWebSocketRpcSession<DoSQLRpcApi>('wss://example.com/db/mydb/rpc');

// Execute SQL
const result = await rpc.exec({ sql: 'SELECT * FROM users' });
console.log(result.rows);

// Use .map() for pipelining (single round trip!)
const userName = await rpc.exec({
  sql: 'SELECT name FROM users WHERE id = ?',
  params: [1]
})
  .map(result => result.rows[0])
  .map(row => row[0]);

console.log('User name:', userName);
```

### HTTP Client

```typescript
import { newHttpBatchRpcSession } from 'capnweb';
import type { DoSQLRpcApi } from './types';

// Connect via HTTP
const rpc = newHttpBatchRpcSession<DoSQLRpcApi>('https://example.com/db/mydb/rpc');

// Execute SQL (each call is an HTTP request)
const result = await rpc.exec({ sql: 'SELECT * FROM users' });
```

### Transactions

```typescript
// Execute multiple operations atomically
const result = await rpc.transaction({
  ops: [
    {
      type: 'exec',
      sql: 'INSERT INTO orders (user_id, total) VALUES (?, ?)',
      params: [1, 99.99]
    },
    {
      type: 'exec',
      sql: 'UPDATE users SET order_count = order_count + 1 WHERE id = ?',
      params: [1]
    },
  ],
});

if (result.committed) {
  console.log('Transaction successful!');
} else {
  console.error('Transaction rolled back');
}
```

### Prepared Statements

```typescript
// Prepare once
const stmt = await rpc.prepare({
  sql: 'INSERT INTO users (name, email) VALUES (?, ?)'
});

// Execute many times
await rpc.run({ stmtId: stmt.id, params: ['Alice', 'alice@example.com'] });
await rpc.run({ stmtId: stmt.id, params: ['Bob', 'bob@example.com'] });
await rpc.run({ stmtId: stmt.id, params: ['Carol', 'carol@example.com'] });

// Clean up
await rpc.finalize({ stmtId: stmt.id });
```

## Development

### Install Dependencies

```bash
npm install
```

### Run Tests

```bash
npm test
```

### Start Development Server

```bash
npm run dev
```

### Deploy

```bash
npm run deploy
```

## Testing

Tests use `@cloudflare/vitest-pool-workers` to run against real Workers and Durable Objects. No mocks are used.

```bash
# Run all tests
npm test

# Run tests in watch mode
npm run test:watch
```

## Pipelining Benefits

The `.map()` chaining feature of CapnWeb allows you to transform results without additional round trips:

```typescript
// Without pipelining: 3 separate round trips
const result = await rpc.exec({ sql: 'SELECT * FROM users WHERE id = ?', params: [1] });
const row = result.rows[0];
const name = row[1];

// With pipelining: 1 round trip with transformations
const name = await rpc.exec({ sql: 'SELECT * FROM users WHERE id = ?', params: [1] })
  .map(result => result.rows[0])
  .map(row => row[1]);
```

This is especially beneficial for:
- High-latency connections
- Complex data transformations
- Reducing overall request count

## Configuration

### wrangler.jsonc

```jsonc
{
  "name": "dosql-capnweb-rpc-example",
  "main": "src/index.ts",
  "compatibility_date": "2026-01-01",
  "durable_objects": {
    "bindings": [
      { "name": "DOSQL_DB", "class_name": "DoSQLDatabase" }
    ]
  }
}
```

## License

MIT
