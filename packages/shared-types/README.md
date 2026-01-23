> **Developer Preview** - This package is under active development. APIs may change. Not recommended for production use.

# @dotdo/shared-types

Unified type definitions for the DoSQL ecosystem. This package provides the canonical type definitions shared between client and server packages.

## Status

| Property | Value |
|----------|-------|
| Current version | 0.1.0-alpha |
| Stability | Experimental |
| Breaking changes | Expected before 1.0 |

## Installation

```bash
npm install @dotdo/shared-types
```

## Overview

This package serves as the single source of truth for types across:

| Package | Role |
|---------|------|
| `@dotdo/sql.do` | SQL client for Cloudflare Workers |
| `@dotdo/dosql` | SQL server (Durable Object) |
| `@dotdo/lake.do` | Lakehouse client |
| `@dotdo/dolake` | Lakehouse server |

By centralizing type definitions, we ensure type compatibility and prevent drift between client and server implementations.

## Branded Types

Branded types provide compile-time type safety by making structurally identical types incompatible. This prevents accidental misuse of primitive values in wrong contexts.

### LSN (Log Sequence Number)

```typescript
import { LSN, createLSN } from '@dotdo/shared-types';

// Create a branded LSN from a bigint
const lsn: LSN = createLSN(12345n);

// LSN is assignable to bigint contexts
const value: bigint = lsn; // OK

// But plain bigint is NOT assignable to LSN
const plain: bigint = 100n;
const notLSN: LSN = plain; // TypeScript Error!

// Use LSN for time-travel queries
const response = await client.query('SELECT * FROM users', {
  asOf: lsn, // Type-safe: only LSN or Date allowed
});
```

### TransactionId

```typescript
import { TransactionId, createTransactionId } from '@dotdo/shared-types';

// Create a branded transaction ID
const txId: TransactionId = createTransactionId('txn_550e8400-e29b-41d4');

// Use in transactional queries
const result = await client.query('UPDATE users SET name = ?', {
  transactionId: txId, // Type-safe
});
```

### ShardId

```typescript
import { ShardId, createShardId } from '@dotdo/shared-types';

// Create a branded shard identifier
const shardId: ShardId = createShardId('us-west-2-shard-001');

// Route queries to specific shards
const result = await client.query('SELECT * FROM orders', {
  shardId: shardId, // Type-safe shard routing
});
```

### StatementHash

```typescript
import { StatementHash, createStatementHash } from '@dotdo/shared-types';

// Create a branded statement hash for prepared statements
const hash: StatementHash = createStatementHash('a1b2c3d4e5f6');

// Reference prepared statements by hash
const stmt: PreparedStatement = {
  sql: 'SELECT * FROM users WHERE id = ?',
  hash: hash,
};
```

## SQL Value Types

### SQLValue

The `SQLValue` type represents all valid SQL parameter and result values:

```typescript
import { SQLValue } from '@dotdo/shared-types';

const values: SQLValue[] = [
  'text',           // string
  42,               // number
  true,             // boolean
  null,             // null
  new Uint8Array(), // blob
  100n,             // bigint
];
```

## Column Types

### ColumnType (Unified)

Covers both SQL-style and JavaScript-style type representations:

```typescript
import { ColumnType, SQLColumnType, JSColumnType } from '@dotdo/shared-types';

// SQL-style (client-facing)
const sqlTypes: SQLColumnType[] = [
  'INTEGER', 'REAL', 'TEXT', 'BLOB',
  'NULL', 'BOOLEAN', 'DATETIME', 'JSON'
];

// JavaScript-style (wire format)
const jsTypes: JSColumnType[] = [
  'string', 'number', 'bigint', 'boolean',
  'date', 'timestamp', 'json', 'blob', 'null', 'unknown'
];
```

### Type Converters

Convert between SQL and JavaScript type representations:

```typescript
import {
  sqlToJsType,
  jsToSqlType,
  SQL_TO_JS_TYPE_MAP,
  JS_TO_SQL_TYPE_MAP
} from '@dotdo/shared-types';

// Convert SQL type to JS type
const jsType = sqlToJsType('INTEGER'); // 'number'
const jsType2 = sqlToJsType('TEXT');   // 'string'

// Convert JS type to SQL type
const sqlType = jsToSqlType('string'); // 'TEXT'
const sqlType2 = jsToSqlType('bigint'); // 'INTEGER'

// Access mapping directly
console.log(SQL_TO_JS_TYPE_MAP.DATETIME); // 'timestamp'
console.log(JS_TO_SQL_TYPE_MAP.json);     // 'JSON'
```

## Query Types

### QueryRequest

Unified query request supporting all client and server features:

```typescript
import { QueryRequest } from '@dotdo/shared-types';

const request: QueryRequest = {
  sql: 'SELECT * FROM users WHERE status = ? AND created_at > ?',
  params: ['active', new Date('2024-01-01')],
  branch: 'production',
  timeout: 5000,
  limit: 100,
  offset: 0,
};

// With named parameters
const namedRequest: QueryRequest = {
  sql: 'SELECT * FROM users WHERE status = :status',
  namedParams: { status: 'active' },
};

// Time-travel query
const timeTravel: QueryRequest = {
  sql: 'SELECT * FROM orders',
  asOf: createLSN(12345n), // Query at specific LSN
};
```

### QueryOptions (Client-Facing)

Simplified options for client use:

```typescript
import { QueryOptions, TransactionId, LSN } from '@dotdo/shared-types';

const options: QueryOptions = {
  transactionId: createTransactionId('txn_123'),
  asOf: new Date('2024-06-01T00:00:00Z'),
  timeout: 3000,
  shardId: createShardId('shard_001'),
  branch: 'staging',
  streaming: false,
  limit: 50,
  offset: 100,
};
```

### QueryResponse and QueryResult

```typescript
import { QueryResponse, QueryResult, responseToResult, resultToResponse } from '@dotdo/shared-types';

// Server returns QueryResponse
const response: QueryResponse<{ id: number; name: string }> = {
  columns: ['id', 'name'],
  columnTypes: ['INTEGER', 'TEXT'],
  rows: [{ id: 1, name: 'Alice' }],
  rowCount: 1,
  rowsAffected: 0,
  executionTimeMs: 5.2,
  lsn: 12345n,
};

// Convert to client-friendly QueryResult
const result = responseToResult(response);
console.log(result.duration);      // 5.2
console.log(result.rowsAffected);  // 1

// Convert back to server format
const serverFormat = resultToResponse(result, 12346n);
```

## CDC (Change Data Capture) Types

### CDCEvent

Unified CDC event covering all package requirements:

```typescript
import {
  CDCEvent,
  CDCOperation,
  CDCOperationCode,
  serverToClientCDCEvent,
  clientToServerCDCEvent
} from '@dotdo/shared-types';

// Define your row type
interface User {
  id: number;
  name: string;
  email: string;
}

// CDC event from server
const serverEvent: CDCEvent<User> = {
  lsn: 12345n,
  sequence: 1,
  table: 'users',
  operation: 'UPDATE',
  timestamp: Date.now(),
  txId: 'tx_abc123',
  oldRow: { id: 1, name: 'Alice', email: 'alice@old.com' },
  newRow: { id: 1, name: 'Alice', email: 'alice@new.com' },
  primaryKey: { id: 1 },
};

// Convert to client format
const clientEvent = serverToClientCDCEvent(serverEvent);
console.log(clientEvent.before);        // { id: 1, name: 'Alice', email: 'alice@old.com' }
console.log(clientEvent.after);         // { id: 1, name: 'Alice', email: 'alice@new.com' }
console.log(clientEvent.transactionId); // 'tx_abc123'

// Operation codes for binary encoding
console.log(CDCOperationCode.INSERT);   // 0
console.log(CDCOperationCode.UPDATE);   // 1
console.log(CDCOperationCode.DELETE);   // 2
console.log(CDCOperationCode.TRUNCATE); // 3
```

### Type Guards for CDC Events

```typescript
import { isServerCDCEvent, isClientCDCEvent } from '@dotdo/shared-types';

function processEvent(event: CDCEvent) {
  if (isServerCDCEvent(event)) {
    // Event has txId property
    console.log('Server event:', event.txId);
  }

  if (isClientCDCEvent(event)) {
    // Event has transactionId property
    console.log('Client event:', event.transactionId);
  }
}
```

## Transaction Types

### IsolationLevel

```typescript
import { IsolationLevel, ServerIsolationLevel } from '@dotdo/shared-types';

// All supported isolation levels
const levels: IsolationLevel[] = [
  'READ_UNCOMMITTED',
  'READ_COMMITTED',
  'REPEATABLE_READ',
  'SERIALIZABLE',
  'SNAPSHOT',
];

// Server-supported subset
const serverLevels: ServerIsolationLevel[] = [
  'READ_COMMITTED',
  'REPEATABLE_READ',
  'SERIALIZABLE',
];
```

### TransactionOptions and TransactionState

```typescript
import {
  TransactionOptions,
  TransactionState,
  TransactionHandle
} from '@dotdo/shared-types';

// Options for beginning a transaction
const txOptions: TransactionOptions = {
  isolationLevel: 'SERIALIZABLE',
  readOnly: false,
  timeout: 30000,
  branch: 'main',
};

// Transaction state (internal tracking)
const state: TransactionState = {
  id: createTransactionId('txn_123'),
  isolationLevel: 'SERIALIZABLE',
  readOnly: false,
  startedAt: new Date(),
  snapshotLSN: createLSN(12345n),
};

// Transaction handle (returned after begin)
const handle: TransactionHandle = {
  txId: 'txn_123',
  startLSN: 12345n,
  expiresAt: Date.now() + 30000,
};
```

## RPC Types

### RPCRequest and RPCResponse

```typescript
import {
  RPCRequest,
  RPCResponse,
  RPCMethod,
  RPCError,
  RPCErrorCode
} from '@dotdo/shared-types';

// RPC request envelope
const request: RPCRequest = {
  id: 'req_001',
  method: 'query',
  params: {
    sql: 'SELECT * FROM users',
    params: [],
  },
};

// RPC response envelope
const response: RPCResponse<QueryResult> = {
  id: 'req_001',
  result: {
    rows: [{ id: 1, name: 'Alice' }],
    columns: ['id', 'name'],
    rowsAffected: 0,
    duration: 5.2,
  },
};

// Error response
const errorResponse: RPCResponse = {
  id: 'req_001',
  error: {
    code: RPCErrorCode.SYNTAX_ERROR,
    message: 'Syntax error near "SELCT"',
    details: { position: 0 },
  },
};
```

### RPCErrorCode

Comprehensive error codes for all error scenarios:

```typescript
import { RPCErrorCode } from '@dotdo/shared-types';

// General errors
RPCErrorCode.UNKNOWN
RPCErrorCode.INVALID_REQUEST
RPCErrorCode.TIMEOUT
RPCErrorCode.INTERNAL_ERROR

// Query errors
RPCErrorCode.SYNTAX_ERROR
RPCErrorCode.TABLE_NOT_FOUND
RPCErrorCode.COLUMN_NOT_FOUND
RPCErrorCode.CONSTRAINT_VIOLATION
RPCErrorCode.TYPE_MISMATCH

// Transaction errors
RPCErrorCode.TRANSACTION_NOT_FOUND
RPCErrorCode.TRANSACTION_ABORTED
RPCErrorCode.DEADLOCK_DETECTED
RPCErrorCode.SERIALIZATION_FAILURE

// CDC errors
RPCErrorCode.INVALID_LSN
RPCErrorCode.SUBSCRIPTION_ERROR
RPCErrorCode.BUFFER_OVERFLOW

// Auth errors
RPCErrorCode.UNAUTHORIZED
RPCErrorCode.FORBIDDEN

// Resource errors
RPCErrorCode.RESOURCE_EXHAUSTED
RPCErrorCode.QUOTA_EXCEEDED
```

## Schema Types

### TableSchema and Related Types

```typescript
import {
  TableSchema,
  ColumnDefinition,
  IndexDefinition,
  ForeignKeyDefinition
} from '@dotdo/shared-types';

const usersSchema: TableSchema = {
  name: 'users',
  columns: [
    {
      name: 'id',
      type: 'INTEGER',
      nullable: false,
      primaryKey: true,
      autoIncrement: true,
    },
    {
      name: 'email',
      type: 'TEXT',
      nullable: false,
      primaryKey: false,
      unique: true,
    },
    {
      name: 'created_at',
      type: 'DATETIME',
      nullable: false,
      primaryKey: false,
      defaultValue: 'CURRENT_TIMESTAMP',
    },
  ],
  primaryKey: ['id'],
  indexes: [
    {
      name: 'idx_users_email',
      columns: ['email'],
      unique: true,
      type: 'BTREE',
    },
  ],
  foreignKeys: [],
};
```

## Sharding Types

```typescript
import { ShardConfig, ShardInfo, ShardId } from '@dotdo/shared-types';

const config: ShardConfig = {
  shardCount: 16,
  shardKey: 'tenant_id',
  shardFunction: 'hash',
};

const shardInfo: ShardInfo = {
  shardId: createShardId('shard_007'),
  keyRange: { min: 0, max: 999 },
  rowCount: 150000,
};
```

## Connection Types

```typescript
import { ConnectionOptions, ConnectionStats } from '@dotdo/shared-types';

const options: ConnectionOptions = {
  url: 'wss://sql.example.com/v1',
  defaultBranch: 'main',
  connectTimeoutMs: 5000,
  queryTimeoutMs: 30000,
  autoReconnect: true,
  maxReconnectAttempts: 5,
  reconnectDelayMs: 1000,
};

// Runtime connection stats
const stats: ConnectionStats = {
  connected: true,
  connectionId: 'conn_abc123',
  branch: 'main',
  currentLSN: 12345n,
  latencyMs: 15,
  messagesSent: 100,
  messagesReceived: 98,
  reconnectCount: 0,
};
```

## Idempotency Configuration

```typescript
import { IdempotencyConfig, DEFAULT_IDEMPOTENCY_CONFIG } from '@dotdo/shared-types';

// Custom idempotency config
const config: IdempotencyConfig = {
  enabled: true,
  keyPrefix: 'myapp_',
  ttlMs: 3600000, // 1 hour
};

// Use defaults (24 hour TTL)
console.log(DEFAULT_IDEMPOTENCY_CONFIG.ttlMs); // 86400000
```

## Client Capabilities

```typescript
import { ClientCapabilities, DEFAULT_CLIENT_CAPABILITIES } from '@dotdo/shared-types';

const capabilities: ClientCapabilities = {
  binaryProtocol: true,
  compression: true,
  batching: true,
  maxBatchSize: 500,
  maxMessageSize: 2 * 1024 * 1024, // 2MB
};

// Defaults
console.log(DEFAULT_CLIENT_CAPABILITIES.maxBatchSize);    // 1000
console.log(DEFAULT_CLIENT_CAPABILITIES.maxMessageSize);  // 4MB
```

## Type-Safe Patterns

### Pattern 1: Branded Type Factory Functions

Always use factory functions to create branded types:

```typescript
import { createLSN, createTransactionId, createShardId } from '@dotdo/shared-types';

// Good - type-safe
const lsn = createLSN(100n);
const txId = createTransactionId('txn_123');

// Bad - bypasses type safety (TypeScript allows but defeats the purpose)
// const lsn = 100n as LSN;  // Avoid this pattern
```

### Pattern 2: Response/Result Conversion

Use converter functions when crossing client/server boundaries:

```typescript
import { responseToResult, resultToResponse } from '@dotdo/shared-types';

// Server handler
function handleQuery(request: QueryRequest): QueryResponse {
  const internalResult = executeQuery(request);
  return resultToResponse(internalResult, getCurrentLSN());
}

// Client wrapper
async function query(sql: string): Promise<QueryResult> {
  const response = await fetch('/query', { body: JSON.stringify({ sql }) });
  const data: QueryResponse = await response.json();
  return responseToResult(data);
}
```

### Pattern 3: CDC Event Transformation

Transform CDC events between wire and application formats:

```typescript
import { serverToClientCDCEvent, clientToServerCDCEvent } from '@dotdo/shared-types';

// Receiving from server
websocket.onmessage = (msg) => {
  const serverEvent = JSON.parse(msg.data);
  const clientEvent = serverToClientCDCEvent(serverEvent);
  // Now use clientEvent.before, clientEvent.after, clientEvent.transactionId
};

// Sending to server
function sendEvent(event: CDCEvent) {
  const serverEvent = clientToServerCDCEvent(event);
  websocket.send(JSON.stringify(serverEvent));
}
```

### Pattern 4: Type Guards for Polymorphic Types

Use type guards for runtime type checking:

```typescript
import { isDateTimestamp, isNumericTimestamp } from '@dotdo/shared-types';

function formatTimestamp(ts: Date | number): string {
  if (isDateTimestamp(ts)) {
    return ts.toISOString();
  }
  if (isNumericTimestamp(ts)) {
    return new Date(ts).toISOString();
  }
  throw new Error('Invalid timestamp');
}
```

## Integration with sql.do

```typescript
import { sql } from '@dotdo/sql.do';
import type { QueryResult, LSN, TransactionState } from '@dotdo/shared-types';

const client = sql('https://your-db.sql.do');

// Type-safe queries
const result: QueryResult<{ id: number; name: string }> =
  await client.query('SELECT id, name FROM users WHERE active = ?', [true]);

// Access LSN from result
if (result.lsn) {
  console.log('Query executed at LSN:', result.lsn);
}

// Transaction with typed state
const tx: TransactionState = await client.beginTransaction();
try {
  await client.query('INSERT INTO logs (msg) VALUES (?)', ['action'], { transactionId: tx.id });
  await client.commit(tx.id);
} catch (e) {
  await client.rollback(tx.id);
}
```

## Integration with lake.do

```typescript
import { lake } from '@dotdo/lake.do';
import type { CDCEvent, LSN } from '@dotdo/shared-types';

const client = lake('https://your-lake.lake.do');

// Subscribe to CDC events with typed callback
client.subscribe<{ id: number; name: string }>('users', (event: CDCEvent) => {
  console.log('Operation:', event.operation);
  console.log('Before:', event.before);
  console.log('After:', event.after);
  console.log('LSN:', event.lsn);
});

// Time-travel query using LSN
const historicalData = await client.query('SELECT * FROM users', {
  asOf: createLSN(12345n),
});
```

## Version Compatibility

| shared-types | sql.do | dosql | lake.do | dolake |
|--------------|--------|-------|---------|--------|
| 0.1.x        | 0.1.x  | 0.1.x | 0.1.x   | 0.1.x  |

All packages in the DoSQL ecosystem should use compatible versions of `@dotdo/shared-types` to ensure type compatibility.

## API Reference

### Branded Types

| Type | Base | Factory |
|------|------|---------|
| `LSN` | `bigint` | `createLSN(bigint)` |
| `TransactionId` | `string` | `createTransactionId(string)` |
| `ShardId` | `string` | `createShardId(string)` |
| `StatementHash` | `string` | `createStatementHash(string)` |

### Type Converters

| Function | Description |
|----------|-------------|
| `sqlToJsType(SQLColumnType)` | Convert SQL type to JS type |
| `jsToSqlType(JSColumnType)` | Convert JS type to SQL type |
| `responseToResult(QueryResponse)` | Convert server response to client result |
| `resultToResponse(QueryResult)` | Convert client result to server response |
| `serverToClientCDCEvent(CDCEvent)` | Transform server CDC event to client format |
| `clientToServerCDCEvent(CDCEvent)` | Transform client CDC event to server format |

### Type Guards

| Function | Description |
|----------|-------------|
| `isServerCDCEvent(CDCEvent)` | Check if event is from server (has `txId`) |
| `isClientCDCEvent(CDCEvent)` | Check if event is from client (has `transactionId`) |
| `isDateTimestamp(Date \| number)` | Check if timestamp is a Date |
| `isNumericTimestamp(Date \| number)` | Check if timestamp is a number |

### Constants

| Constant | Description |
|----------|-------------|
| `SQL_TO_JS_TYPE_MAP` | Mapping from SQL types to JS types |
| `JS_TO_SQL_TYPE_MAP` | Mapping from JS types to SQL types |
| `CDCOperationCode` | Numeric codes for CDC operations |
| `DEFAULT_IDEMPOTENCY_CONFIG` | Default idempotency settings |
| `DEFAULT_CLIENT_CAPABILITIES` | Default client capability settings |

### Runtime Utilities

| Function | Description |
|----------|-------------|
| `getWrapperMapStats()` | Get statistics about internal wrapper maps (size counts for memory monitoring) |
| `clearWrapperMaps()` | Clear internal wrapper maps (useful for testing or memory management) |
| `setDevMode(boolean)` | Enable/disable development mode for extra validation |
| `isDevMode()` | Check if development mode is enabled |
| `setStrictMode(boolean)` | Enable/disable strict mode for format validation |
| `isStrictMode()` | Check if strict mode is enabled |

```typescript
import {
  getWrapperMapStats,
  clearWrapperMaps,
  setDevMode,
  isDevMode
} from '@dotdo/shared-types';

// Check wrapper map memory usage
const stats = getWrapperMapStats();
console.log('Wrapper map sizes:', stats);

// Clear maps in test teardown
afterEach(() => {
  clearWrapperMaps();
});

// Enable dev mode for extra validation
setDevMode(true);
console.log('Dev mode:', isDevMode()); // true
```

## License

MIT
