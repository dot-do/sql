> **Developer Preview** - This package is under active development. APIs may change. Not recommended for production use.

# @dotdo/sql-types

Unified type definitions for the DoSQL ecosystem. This package provides the canonical type definitions shared between client and server packages.

## Status

| Property | Value |
|----------|-------|
| Current version | 0.1.0 |
| Stability | Experimental |
| Breaking changes | Expected before 1.0 |

## Installation

```bash
npm install @dotdo/sql-types
```

## Overview

This package serves as the single source of truth for types across:

| Package | Role |
|---------|------|
| `sql.do` | SQL client for Cloudflare Workers |
| `dosql` | SQL server (Durable Object) |
| `lake.do` | Lakehouse client |
| `dolake` | Lakehouse server |

By centralizing type definitions, we ensure type compatibility and prevent drift between client and server implementations.

## Branded Types

Branded types provide compile-time type safety by making structurally identical types incompatible. This prevents accidental misuse of primitive values in wrong contexts.

### LSN (Log Sequence Number)

```typescript
import { LSN, createLSN } from '@dotdo/sql-types';

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

### LSN Utility Functions

The package provides utility functions for working with LSN values:

```typescript
import {
  LSN,
  createLSN,
  compareLSN,
  incrementLSN,
  lsnValue
} from '@dotdo/sql-types';

// Create LSN values
const lsn1 = createLSN(1000n);
const lsn2 = createLSN(2000n);

// Compare two LSNs for ordering
// Returns: -1 if a < b, 0 if equal, 1 if a > b
const comparison = compareLSN(lsn1, lsn2);
console.log(comparison); // -1 (lsn1 is less than lsn2)

// Increment an LSN by a given amount (default: 1)
const nextLsn = incrementLSN(lsn1);        // 1001n
const skipAhead = incrementLSN(lsn1, 100n); // 1100n

// Extract the raw bigint value from a branded LSN
const rawValue: bigint = lsnValue(lsn1);
console.log(rawValue); // 1000n
```

These utilities are useful for:
- **compareLSN**: Ordering CDC events, determining if one snapshot is newer than another
- **incrementLSN**: Generating the next expected LSN in a sequence
- **lsnValue**: Extracting the raw bigint when you need to perform arithmetic or serialize

### Branded Type Validation Functions

The package provides validation functions to check if unknown values are valid branded types at runtime. These are useful for validating user input, API responses, or data from external sources before creating branded types.

```typescript
import {
  isValidLSN,
  isValidTransactionId,
  isValidShardId,
  isValidStatementHash,
  createLSN,
  createTransactionId,
  createShardId,
  createStatementHash,
} from '@dotdo/sql-types';

// Validate before creating branded types
function processLSN(value: unknown): LSN | null {
  if (isValidLSN(value)) {
    // TypeScript narrows value to bigint, and we know it's >= 0
    return createLSN(value);
  }
  return null;
}

// Validate transaction IDs from API responses
function handleTransaction(response: { txId?: unknown }): TransactionId | null {
  if (isValidTransactionId(response.txId)) {
    // TypeScript narrows to string, and we know it's non-empty
    return createTransactionId(response.txId);
  }
  return null;
}

// Validate shard IDs with length constraints
function routeToShard(shardId: unknown): ShardId | null {
  if (isValidShardId(shardId)) {
    // TypeScript narrows to string, max 255 chars, non-empty
    return createShardId(shardId);
  }
  return null;
}

// Validate statement hashes
function getCachedStatement(hash: unknown): StatementHash | null {
  if (isValidStatementHash(hash)) {
    // TypeScript narrows to non-empty string
    return createStatementHash(hash);
  }
  return null;
}
```

#### Validation Rules

| Function | Type | Validation Rules |
|----------|------|-----------------|
| `isValidLSN(value)` | `bigint` | Must be a bigint >= 0 |
| `isValidTransactionId(value)` | `string` | Must be a non-empty string (after trim) |
| `isValidShardId(value)` | `string` | Must be a non-empty string (after trim), max 255 characters |
| `isValidStatementHash(value)` | `string` | Must be a non-empty string |

#### Tracking Validated Instances

You can also check if a branded type was created through the factory function (i.e., it was properly validated):

```typescript
import {
  isValidatedLSN,
  isValidatedTransactionId,
  isValidatedShardId,
  isValidatedStatementHash,
  createLSN,
  LSN,
} from '@dotdo/sql-types';

// Create a validated LSN
const lsn = createLSN(1000n);

// Check if it was created through the factory
console.log(isValidatedLSN(lsn)); // true

// Cast values bypass validation tracking
const castLsn = 1000n as LSN;
console.log(isValidatedLSN(castLsn)); // false
```

These `isValidated*` functions are useful for:
- **Security**: Ensuring values weren't bypassed via type casting
- **Debugging**: Tracking whether values went through proper validation
- **Testing**: Verifying that code paths use factory functions

### TransactionId

```typescript
import { TransactionId, createTransactionId } from '@dotdo/sql-types';

// Create a branded transaction ID
const txId: TransactionId = createTransactionId('txn_550e8400-e29b-41d4');

// Use in transactional queries
const result = await client.query('UPDATE users SET name = ?', {
  transactionId: txId, // Type-safe
});
```

### ShardId

```typescript
import { ShardId, createShardId } from '@dotdo/sql-types';

// Create a branded shard identifier
const shardId: ShardId = createShardId('us-west-2-shard-001');

// Route queries to specific shards
const result = await client.query('SELECT * FROM orders', {
  shardId: shardId, // Type-safe shard routing
});
```

### StatementHash

```typescript
import { StatementHash, createStatementHash } from '@dotdo/sql-types';

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
import { SQLValue } from '@dotdo/sql-types';

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
import { ColumnType, SQLColumnType, JSColumnType } from '@dotdo/sql-types';

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
} from '@dotdo/sql-types';

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
import { QueryRequest } from '@dotdo/sql-types';

const request: QueryRequest = {
  sql: 'SELECT * FROM users WHERE status = ? AND created_at > ?',
  params: ['active', new Date('2025-01-01')],
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
import { QueryOptions, TransactionId, LSN } from '@dotdo/sql-types';

const options: QueryOptions = {
  transactionId: createTransactionId('txn_123'),
  asOf: new Date('2025-06-01T00:00:00Z'),
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
import { QueryResponse, QueryResult, responseToResult, resultToResponse } from '@dotdo/sql-types';

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
} from '@dotdo/sql-types';

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
import { isServerCDCEvent, isClientCDCEvent } from '@dotdo/sql-types';

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
import { IsolationLevel, ServerIsolationLevel } from '@dotdo/sql-types';

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
} from '@dotdo/sql-types';

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
} from '@dotdo/sql-types';

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

The `RPCErrorCode` enum provides standardized error codes for all RPC communication scenarios. These codes enable programmatic error handling and help clients distinguish between different failure modes.

```typescript
import { RPCErrorCode, RPCError } from '@dotdo/sql-types';
```

#### Error Code Reference

| Code | Category | Description |
|------|----------|-------------|
| `UNKNOWN` | General | An unexpected or unclassified error occurred |
| `INVALID_REQUEST` | General | The request was malformed or missing required fields |
| `TIMEOUT` | General | The operation exceeded the configured timeout |
| `INTERNAL_ERROR` | General | An internal server error occurred |
| `SYNTAX_ERROR` | Query | SQL syntax error in the query |
| `TABLE_NOT_FOUND` | Query | Referenced table does not exist |
| `COLUMN_NOT_FOUND` | Query | Referenced column does not exist |
| `CONSTRAINT_VIOLATION` | Query | A constraint (unique, foreign key, check) was violated |
| `TYPE_MISMATCH` | Query | Parameter or value type is incompatible with column type |
| `TRANSACTION_NOT_FOUND` | Transaction | The specified transaction ID does not exist |
| `TRANSACTION_ABORTED` | Transaction | The transaction was aborted due to a conflict or error |
| `DEADLOCK_DETECTED` | Transaction | A deadlock was detected between concurrent transactions |
| `SERIALIZATION_FAILURE` | Transaction | Transaction failed due to serialization conflict |
| `INVALID_LSN` | CDC | The specified LSN is invalid or out of range |
| `SUBSCRIPTION_ERROR` | CDC | Failed to create or manage CDC subscription |
| `BUFFER_OVERFLOW` | CDC | CDC event buffer exceeded capacity |
| `UNAUTHORIZED` | Auth | Authentication required but not provided or invalid |
| `FORBIDDEN` | Auth | Authenticated but lacking permission for the operation |
| `RESOURCE_EXHAUSTED` | Resource | Server resources (memory, connections) are exhausted |
| `QUOTA_EXCEEDED` | Resource | Account or tenant quota has been exceeded |

#### Usage Examples

##### Basic Error Handling

```typescript
import { RPCErrorCode, RPCResponse, isSuccess } from '@dotdo/sql-types';

async function executeQuery(sql: string): Promise<QueryResult> {
  const response: RPCResponse<QueryResult> = await rpcClient.call('query', { sql });

  if (response.error) {
    switch (response.error.code) {
      case RPCErrorCode.SYNTAX_ERROR:
        throw new Error(`SQL syntax error: ${response.error.message}`);

      case RPCErrorCode.TABLE_NOT_FOUND:
        throw new Error(`Table not found: ${response.error.details?.table}`);

      case RPCErrorCode.TIMEOUT:
        // Retry with longer timeout
        return executeQueryWithRetry(sql);

      case RPCErrorCode.UNAUTHORIZED:
        // Redirect to login
        redirectToLogin();
        throw new Error('Authentication required');

      default:
        throw new Error(`Query failed: ${response.error.message}`);
    }
  }

  return response.result!;
}
```

##### Transaction Error Handling

```typescript
import { RPCErrorCode, RPCError } from '@dotdo/sql-types';

async function transferFunds(fromId: number, toId: number, amount: number): Promise<void> {
  const maxRetries = 3;
  let attempt = 0;

  while (attempt < maxRetries) {
    try {
      const tx = await beginTransaction({ isolationLevel: 'SERIALIZABLE' });

      await query('UPDATE accounts SET balance = balance - ? WHERE id = ?', [amount, fromId], { transactionId: tx.txId });
      await query('UPDATE accounts SET balance = balance + ? WHERE id = ?', [amount, toId], { transactionId: tx.txId });

      await commit(tx.txId);
      return; // Success
    } catch (error) {
      const rpcError = error as RPCError;

      if (rpcError.code === RPCErrorCode.DEADLOCK_DETECTED ||
          rpcError.code === RPCErrorCode.SERIALIZATION_FAILURE) {
        // These are retryable - increment attempt and retry
        attempt++;
        await sleep(100 * Math.pow(2, attempt)); // Exponential backoff
        continue;
      }

      if (rpcError.code === RPCErrorCode.CONSTRAINT_VIOLATION) {
        throw new Error('Insufficient funds or invalid account');
      }

      // Non-retryable error
      throw error;
    }
  }

  throw new Error('Transaction failed after max retries');
}
```

##### CDC Error Handling

```typescript
import { RPCErrorCode, CDCEvent } from '@dotdo/sql-types';

async function subscribeToCDC(fromLSN: bigint, tables: string[]): Promise<AsyncIterable<CDCEvent>> {
  try {
    return await rpcClient.subscribeCDC({ fromLSN, tables });
  } catch (error) {
    const rpcError = error as RPCError;

    switch (rpcError.code) {
      case RPCErrorCode.INVALID_LSN:
        // LSN is too old or invalid - start from current position
        console.warn('Invalid LSN, resubscribing from current position');
        return await rpcClient.subscribeCDC({ fromLSN: 0n, tables });

      case RPCErrorCode.BUFFER_OVERFLOW:
        // Consumer is too slow - acknowledge and restart
        console.warn('Buffer overflow, some events may be lost');
        return await rpcClient.subscribeCDC({ fromLSN, tables, maxBufferSize: 10000 });

      case RPCErrorCode.SUBSCRIPTION_ERROR:
        throw new Error(`CDC subscription failed: ${rpcError.message}`);

      default:
        throw error;
    }
  }
}
```

##### Creating Custom Error Responses

```typescript
import { RPCErrorCode, RPCError, RPCResponse } from '@dotdo/sql-types';

function createErrorResponse(
  requestId: string,
  code: RPCErrorCode,
  message: string,
  details?: Record<string, unknown>
): RPCResponse {
  const error: RPCError = {
    code,
    message,
    details,
  };

  return {
    id: requestId,
    error,
  };
}

// Usage in a server handler
function handleQuery(request: RPCRequest): RPCResponse {
  if (!request.params?.sql) {
    return createErrorResponse(
      request.id,
      RPCErrorCode.INVALID_REQUEST,
      'Missing required field: sql',
      { field: 'sql' }
    );
  }

  try {
    const result = executeSQL(request.params.sql);
    return { id: request.id, result };
  } catch (e) {
    if (e instanceof SQLSyntaxError) {
      return createErrorResponse(
        request.id,
        RPCErrorCode.SYNTAX_ERROR,
        e.message,
        { position: e.position, suggestion: e.suggestion }
      );
    }

    return createErrorResponse(
      request.id,
      RPCErrorCode.INTERNAL_ERROR,
      'An unexpected error occurred'
    );
  }
}
```

##### Categorizing Errors for Retry Logic

```typescript
import { RPCErrorCode } from '@dotdo/sql-types';

const RETRYABLE_ERRORS: Set<RPCErrorCode> = new Set([
  RPCErrorCode.TIMEOUT,
  RPCErrorCode.DEADLOCK_DETECTED,
  RPCErrorCode.SERIALIZATION_FAILURE,
  RPCErrorCode.RESOURCE_EXHAUSTED,
  RPCErrorCode.BUFFER_OVERFLOW,
]);

const AUTH_ERRORS: Set<RPCErrorCode> = new Set([
  RPCErrorCode.UNAUTHORIZED,
  RPCErrorCode.FORBIDDEN,
]);

const QUERY_ERRORS: Set<RPCErrorCode> = new Set([
  RPCErrorCode.SYNTAX_ERROR,
  RPCErrorCode.TABLE_NOT_FOUND,
  RPCErrorCode.COLUMN_NOT_FOUND,
  RPCErrorCode.CONSTRAINT_VIOLATION,
  RPCErrorCode.TYPE_MISMATCH,
]);

function isRetryable(code: RPCErrorCode): boolean {
  return RETRYABLE_ERRORS.has(code);
}

function requiresReauth(code: RPCErrorCode): boolean {
  return AUTH_ERRORS.has(code);
}

function isQueryError(code: RPCErrorCode): boolean {
  return QUERY_ERRORS.has(code);
}

// Usage
async function executeWithRetry<T>(fn: () => Promise<T>, maxRetries = 3): Promise<T> {
  let lastError: RPCError | undefined;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error as RPCError;

      if (!isRetryable(lastError.code as RPCErrorCode)) {
        throw error; // Non-retryable, fail immediately
      }

      await sleep(100 * Math.pow(2, attempt)); // Exponential backoff
    }
  }

  throw lastError;
}
```

## Schema Types

### TableSchema and Related Types

```typescript
import {
  TableSchema,
  ColumnDefinition,
  IndexDefinition,
  ForeignKeyDefinition
} from '@dotdo/sql-types';

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
import { ShardConfig, ShardInfo, ShardId } from '@dotdo/sql-types';

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
import { ConnectionOptions, ConnectionStats } from '@dotdo/sql-types';

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
import { IdempotencyConfig, DEFAULT_IDEMPOTENCY_CONFIG } from '@dotdo/sql-types';

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
import { ClientCapabilities, DEFAULT_CLIENT_CAPABILITIES } from '@dotdo/sql-types';

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

## Dev Mode and Strict Mode Configuration

The package provides two runtime modes that control validation behavior in factory functions. These modes are particularly useful for catching errors during development while allowing optimized performance in production.

### setDevMode

Dev mode enables runtime validation in factory functions like `createLSN()`, `createTransactionId()`, `createShardId()`, and `createStatementHash()`. When enabled, these functions validate inputs and throw descriptive errors for invalid values.

```typescript
import { setDevMode, isDevMode, createLSN, createTransactionId } from '@dotdo/sql-types';

// Check current mode
console.log(isDevMode()); // true (default)

// Disable for production performance
setDevMode(false);

// In dev mode (enabled), validation errors are thrown:
setDevMode(true);
try {
  createLSN(-1n); // Throws: "LSN cannot be negative: -1"
} catch (e) {
  console.error(e.message);
}

try {
  createTransactionId(''); // Throws: "TransactionId cannot be empty"
} catch (e) {
  console.error(e.message);
}

// With dev mode disabled, invalid values pass through (faster but unsafe)
setDevMode(false);
const unsafeLSN = createLSN(-1n); // No error thrown
```

### setStrictMode

Strict mode enables additional format validation beyond basic type checking. When enabled, factory functions enforce stricter rules like format patterns.

```typescript
import { setStrictMode, isStrictMode, createStatementHash } from '@dotdo/sql-types';

// Check current mode
console.log(isStrictMode()); // false (default)

// Enable strict validation
setStrictMode(true);

// Strict mode validates format patterns:
try {
  createStatementHash('invalid-hash!'); // Throws: "Invalid StatementHash format"
} catch (e) {
  console.error(e.message);
}

// Valid hexadecimal hash passes strict validation
const validHash = createStatementHash('a1b2c3d4e5f6'); // OK

// Disable strict mode for relaxed validation
setStrictMode(false);
const relaxedHash = createStatementHash('any-format-ok'); // OK (no format check)
```

### Mode Behavior Summary

| Mode | Default | Effect |
|------|---------|--------|
| Dev Mode | `true` | Validates types and basic constraints (non-empty, non-negative) |
| Strict Mode | `false` | Validates format patterns (e.g., hex-only for StatementHash) |

### Recommended Configuration

```typescript
import { setDevMode, setStrictMode } from '@dotdo/sql-types';

// Development environment: Enable all validation
if (process.env.NODE_ENV === 'development') {
  setDevMode(true);
  setStrictMode(true);
}

// Production environment: Disable for performance
if (process.env.NODE_ENV === 'production') {
  setDevMode(false);
  setStrictMode(false);
}

// Testing environment: Enable strict mode to catch edge cases
if (process.env.NODE_ENV === 'test') {
  setDevMode(true);
  setStrictMode(true);
}
```

### Validation Rules by Factory Function

| Factory | Dev Mode Validation | Strict Mode Validation |
|---------|---------------------|------------------------|
| `createLSN(bigint)` | Must be bigint, >= 0 | Same as dev mode |
| `createTransactionId(string)` | Must be non-empty string | Same as dev mode |
| `createShardId(string)` | Must be non-empty, <= 255 chars | Same as dev mode |
| `createStatementHash(string)` | Must be non-empty string | Must match `/^[a-f0-9]+$/i` |

## Retry Configuration

The `RetryConfig` type configures automatic retry behavior for transient failures using exponential backoff with jitter.

### RetryConfig

```typescript
import {
  RetryConfig,
  DEFAULT_RETRY_CONFIG,
  isRetryConfig,
  createRetryConfig
} from '@dotdo/sql-types';
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `maxRetries` | `number` | Maximum number of retry attempts before giving up. For example, `3` means the request will be attempted up to 4 times total (1 initial + 3 retries). |
| `baseDelayMs` | `number` | Base delay in milliseconds for exponential backoff. The actual delay increases exponentially: `baseDelayMs * 2^attempt`. |
| `maxDelayMs` | `number` | Maximum delay in milliseconds between retry attempts. Caps the exponential backoff to prevent excessively long waits. |

#### Default Configuration

```typescript
// DEFAULT_RETRY_CONFIG provides sensible defaults:
// - maxRetries: 3 (4 total attempts)
// - baseDelayMs: 100ms
// - maxDelayMs: 5000ms (5 seconds)

const config = DEFAULT_RETRY_CONFIG;
console.log(config.maxRetries);   // 3
console.log(config.baseDelayMs);  // 100
console.log(config.maxDelayMs);   // 5000
```

#### Usage Examples

```typescript
import {
  RetryConfig,
  DEFAULT_RETRY_CONFIG,
  isRetryConfig,
  createRetryConfig
} from '@dotdo/sql-types';

// Use default configuration
const defaultConfig = DEFAULT_RETRY_CONFIG;

// Create custom configuration with validation
const customConfig: RetryConfig = createRetryConfig({
  maxRetries: 5,      // Retry up to 5 times
  baseDelayMs: 200,   // Start with 200ms delay
  maxDelayMs: 10000,  // Cap delay at 10 seconds
});

// Type guard for runtime validation
const maybeConfig: unknown = {
  maxRetries: 3,
  baseDelayMs: 100,
  maxDelayMs: 5000,
};

if (isRetryConfig(maybeConfig)) {
  // TypeScript knows maybeConfig is RetryConfig
  console.log(`Will retry ${maybeConfig.maxRetries} times`);
}

// Use in client configuration
const sqlClientConfig = {
  url: 'https://sql.example.com',
  retry: customConfig,
};

const lakeClientConfig = {
  url: 'https://lake.example.com',
  retry: DEFAULT_RETRY_CONFIG,
};
```

#### Validation

The `createRetryConfig` factory function validates constraints:

```typescript
// Throws: maxRetries cannot be negative
createRetryConfig({ maxRetries: -1, baseDelayMs: 100, maxDelayMs: 5000 });

// Throws: baseDelayMs cannot be negative
createRetryConfig({ maxRetries: 3, baseDelayMs: -100, maxDelayMs: 5000 });

// Throws: maxDelayMs cannot be negative
createRetryConfig({ maxRetries: 3, baseDelayMs: 100, maxDelayMs: -5000 });

// Throws: maxDelayMs cannot be less than baseDelayMs
createRetryConfig({ maxRetries: 3, baseDelayMs: 100, maxDelayMs: 50 });
```

#### Edge Cases

```typescript
// Zero retries disables retries (only initial attempt)
const noRetries = createRetryConfig({
  maxRetries: 0,
  baseDelayMs: 100,
  maxDelayMs: 5000,
});

// Equal delays for fixed (non-exponential) backoff
const fixedDelay = createRetryConfig({
  maxRetries: 3,
  baseDelayMs: 1000,
  maxDelayMs: 1000,
});
```

#### Retry Behavior

Only retryable errors trigger retries:
- **Retryable**: Timeouts, connection failures, temporary unavailability
- **Non-retryable**: Syntax errors, constraint violations, authentication errors

## Result Pattern

The Result pattern provides a type-safe way to handle operations that can fail without using exceptions for expected failures. This is the recommended approach for handling errors in the DoSQL ecosystem.

### Type Definitions

#### Result<T>

The main `Result<T>` type is a discriminated union of `Success<T>` and `Failure`:

```typescript
import { Result, Success, Failure, ResultError } from '@dotdo/sql-types';

// Success type - contains the success data
interface Success<T> {
  success: true;
  data: T;
  error?: never;  // Ensures error is not present
}

// Failure type - contains error information
interface Failure {
  success: false;
  data?: never;   // Ensures data is not present
  error: ResultError;
}

// ResultError - structured error information
interface ResultError {
  code: string;                       // Error code for programmatic handling
  message: string;                    // Human-readable error message
  details?: Record<string, unknown>;  // Additional error details
}

// Result is the union type
type Result<T> = Success<T> | Failure;
```

### Type Guards

Use type guards to narrow the Result type and access the appropriate properties:

```typescript
import { Result, isSuccess, isFailure } from '@dotdo/sql-types';

function processResult(result: Result<User>) {
  // Check for success
  if (isSuccess(result)) {
    // TypeScript knows result.data is available
    console.log('User:', result.data.name);
    return result.data;
  }

  // Check for failure
  if (isFailure(result)) {
    // TypeScript knows result.error is available
    console.error(`Error [${result.error.code}]: ${result.error.message}`);
    if (result.error.details) {
      console.error('Details:', result.error.details);
    }
    return null;
  }
}
```

### Constructors

Create Result values using the constructor functions:

```typescript
import { success, failure, failureFromError } from '@dotdo/sql-types';

// Create a success result
function getUser(id: number): Result<User> {
  const user = database.findUser(id);
  if (user) {
    return success(user);
  }
  return failure('USER_NOT_FOUND', `User with id ${id} not found`);
}

// Create a failure with details
function validateEmail(email: string): Result<string> {
  if (!email.includes('@')) {
    return failure('VALIDATION_ERROR', 'Invalid email format', {
      field: 'email',
      value: email,
    });
  }
  return success(email);
}

// Create a failure from an Error object
async function fetchData(url: string): Promise<Result<Data>> {
  try {
    const response = await fetch(url);
    const data = await response.json();
    return success(data);
  } catch (e) {
    return failureFromError(e, 'FETCH_ERROR');
  }
}
```

### Utility Functions

#### unwrap - Extract value or throw

```typescript
import { unwrap, Result } from '@dotdo/sql-types';

// At API boundaries, convert Result to throw style
function handleRequest(result: Result<Response>) {
  // Throws if result is a failure
  const data = unwrap(result);
  return data;
}

// Error message format: "ERROR_CODE: error message"
```

#### unwrapOr - Extract value with default

```typescript
import { unwrapOr, Result } from '@dotdo/sql-types';

// Provide a default value for failures
const count = unwrapOr(getCount(), 0);
const users = unwrapOr(fetchUsers(), []);
const config = unwrapOr(loadConfig(), DEFAULT_CONFIG);
```

#### mapResult - Transform success values

```typescript
import { mapResult, Result } from '@dotdo/sql-types';

// Transform the success value without affecting failures
const userResult: Result<User> = getUser(id);
const nameResult: Result<string> = mapResult(userResult, user => user.name);
const upperResult: Result<string> = mapResult(nameResult, name => name.toUpperCase());

// Failures pass through unchanged
const failedResult = failure('NOT_FOUND', 'User not found');
const stillFailed = mapResult(failedResult, user => user.name); // Still a failure
```

#### flatMapResult - Chain Result-returning operations

```typescript
import { flatMapResult, Result } from '@dotdo/sql-types';

// Chain operations that return Results
function getUser(id: number): Result<User> { /* ... */ }
function getProfile(profileId: number): Result<Profile> { /* ... */ }
function getSettings(userId: number): Result<Settings> { /* ... */ }

const userResult = getUser(123);
const profileResult = flatMapResult(userResult, user => getProfile(user.profileId));
const settingsResult = flatMapResult(profileResult, profile => getSettings(profile.userId));

// Chain multiple operations
const finalResult = flatMapResult(
  flatMapResult(getUser(123), user => getProfile(user.profileId)),
  profile => getSettings(profile.userId)
);
```

#### combineResults - Aggregate multiple Results

```typescript
import { combineResults, Result, isSuccess } from '@dotdo/sql-types';

// Combine multiple Results into one
const results: Result<User>[] = await Promise.all([
  getUser(1),
  getUser(2),
  getUser(3),
]);

const combined: Result<User[]> = combineResults(results);

if (isSuccess(combined)) {
  console.log('All users:', combined.data); // [user1, user2, user3]
} else {
  // Returns the first failure encountered
  console.error('Failed to fetch all users:', combined.error.message);
}
```

### Usage Examples

#### Basic Query Operation

```typescript
import { Result, success, failure, isSuccess } from '@dotdo/sql-types';

interface ParsedQuery {
  type: 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE';
  table: string;
  columns: string[];
}

function parseQuery(sql: string): Result<ParsedQuery> {
  // Validate input
  if (!sql.trim()) {
    return failure('EMPTY_QUERY', 'Query cannot be empty');
  }

  // Attempt parsing
  try {
    const parsed = sqlParser.parse(sql);
    return success(parsed);
  } catch (e) {
    return failure('PARSE_ERROR', `Failed to parse query: ${e.message}`, {
      sql,
      position: e.position,
    });
  }
}

// Usage
const result = parseQuery('SELECT * FROM users');
if (isSuccess(result)) {
  console.log('Parsed:', result.data);
} else {
  console.error(`[${result.error.code}] ${result.error.message}`);
}
```

#### Composing Multiple Operations

```typescript
import {
  Result,
  success,
  failure,
  mapResult,
  flatMapResult,
  combineResults,
  unwrapOr,
} from '@dotdo/sql-types';

// Define operations
function validateInput(input: string): Result<string> {
  if (input.length < 3) {
    return failure('VALIDATION_ERROR', 'Input too short');
  }
  return success(input.trim());
}

function fetchData(query: string): Result<Data[]> {
  // ... implementation
}

function transformData(data: Data[]): Result<TransformedData> {
  // ... implementation
}

// Compose operations
function processInput(input: string): Result<TransformedData> {
  return flatMapResult(
    flatMapResult(validateInput(input), fetchData),
    transformData
  );
}

// Use with default
const data = unwrapOr(processInput(userInput), defaultData);
```

#### Error Handling at API Boundaries

```typescript
import { Result, isSuccess, unwrap, failure, success } from '@dotdo/sql-types';

// Internal function returns Result
function internalOperation(): Result<Data> {
  // ... implementation
}

// API handler converts to HTTP response
async function apiHandler(request: Request): Promise<Response> {
  const result = internalOperation();

  if (isSuccess(result)) {
    return new Response(JSON.stringify(result.data), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  // Map error codes to HTTP status codes
  const statusMap: Record<string, number> = {
    'NOT_FOUND': 404,
    'VALIDATION_ERROR': 400,
    'UNAUTHORIZED': 401,
    'FORBIDDEN': 403,
  };

  const status = statusMap[result.error.code] ?? 500;
  return new Response(JSON.stringify({ error: result.error }), {
    status,
    headers: { 'Content-Type': 'application/json' },
  });
}
```

### Legacy Result Support

For backward compatibility with existing code using the `{ success: boolean, error?: string }` pattern:

```typescript
import {
  LegacyResult,
  isLegacySuccess,
  isLegacyFailure,
  fromLegacyResult,
  Result,
} from '@dotdo/sql-types';

// Legacy result format
interface LegacyResult<T = unknown> {
  success: boolean;
  result?: T;
  error?: string;
  [key: string]: unknown;
}

// Type guards for legacy results
function processLegacy(result: LegacyResult<User>) {
  if (isLegacySuccess(result)) {
    console.log('Success:', result.result);
  }
  if (isLegacyFailure(result)) {
    console.error('Error:', result.error);
  }
}

// Convert legacy to new Result type
const legacyResult: LegacyResult<User> = await oldApi.getUser(id);
const newResult: Result<User> = fromLegacyResult(legacyResult);

// Now use with new Result utilities
if (isSuccess(newResult)) {
  console.log(newResult.data);
}
```

### When to Use Result Pattern

**Use Result for:**
- Expected failures (query errors, validation errors, network errors)
- Operations where callers need to handle both success and failure cases
- Composing multiple fallible operations
- Cross-package API boundaries

**Do NOT use Result for:**
- Programmer errors (invalid arguments) - throw instead
- Unexpected errors - let them propagate as exceptions
- Simple operations that rarely fail

```typescript
// Good: Expected failure, caller should handle
function parseConfig(json: string): Result<Config> {
  try {
    return success(JSON.parse(json));
  } catch {
    return failure('PARSE_ERROR', 'Invalid JSON');
  }
}

// Bad: Programmer error, should throw
function createUser(name: string): Result<User> {
  if (typeof name !== 'string') {
    // Throw instead - this is a programmer error
    throw new TypeError('name must be a string');
  }
  // ...
}
```

## Type-Safe Patterns

### Pattern 1: Branded Type Factory Functions

Always use factory functions to create branded types:

```typescript
import { createLSN, createTransactionId, createShardId } from '@dotdo/sql-types';

// Good - type-safe
const lsn = createLSN(100n);
const txId = createTransactionId('txn_123');

// Bad - bypasses type safety (TypeScript allows but defeats the purpose)
// const lsn = 100n as LSN;  // Avoid this pattern
```

### Pattern 2: Response/Result Conversion

Use converter functions when crossing client/server boundaries:

```typescript
import { responseToResult, resultToResponse } from '@dotdo/sql-types';

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
import { serverToClientCDCEvent, clientToServerCDCEvent } from '@dotdo/sql-types';

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
import { isDateTimestamp, isNumericTimestamp } from '@dotdo/sql-types';

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
import { sql } from 'sql.do';
import type { QueryResult, LSN, TransactionState } from '@dotdo/sql-types';

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
import { lake } from 'lake.do';
import type { CDCEvent, LSN } from '@dotdo/sql-types';

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

All packages in the DoSQL ecosystem should use compatible versions of `@dotdo/sql-types` to ensure type compatibility.

## API Reference

### Stability Legend

- :green_circle: **Stable** - API is stable and unlikely to change
- :yellow_circle: **Beta** - API is mostly stable but may have minor changes
- :red_circle: **Experimental** - API may change significantly

### Branded Types :yellow_circle: Beta

| Type | Base | Factory |
|------|------|---------|
| `LSN` | `bigint` | `createLSN(bigint)` |
| `TransactionId` | `string` | `createTransactionId(string)` |
| `ShardId` | `string` | `createShardId(string)` |
| `StatementHash` | `string` | `createStatementHash(string)` |

### LSN Utilities :green_circle: Stable

| Function | Description |
|----------|-------------|
| `compareLSN(a: LSN, b: LSN)` | Compare two LSNs. Returns -1 if a < b, 0 if equal, 1 if a > b |
| `incrementLSN(lsn: LSN, amount?: bigint)` | Increment an LSN by a given amount (default: 1n) |
| `lsnValue(lsn: LSN)` | Extract the raw bigint value from a branded LSN |

### Type Converters :yellow_circle: Beta

| Function | Description |
|----------|-------------|
| `sqlToJsType(SQLColumnType)` | Convert SQL type to JS type |
| `jsToSqlType(JSColumnType)` | Convert JS type to SQL type |
| `responseToResult(QueryResponse)` | Convert server response to client result |
| `resultToResponse(QueryResult)` | Convert client result to server response |
| `serverToClientCDCEvent(CDCEvent)` | Transform server CDC event to client format |
| `clientToServerCDCEvent(CDCEvent)` | Transform client CDC event to server format |

### Type Guards :green_circle: Stable

| Function | Description |
|----------|-------------|
| `isServerCDCEvent(CDCEvent)` | Check if event is from server (has `txId`) |
| `isClientCDCEvent(CDCEvent)` | Check if event is from client (has `transactionId`) |
| `isDateTimestamp(Date \| number)` | Check if timestamp is a Date |
| `isNumericTimestamp(Date \| number)` | Check if timestamp is a number |
| `isRetryConfig(unknown)` | Check if a value is a valid `RetryConfig` object |
| `isSuccess(Result<T>)` | Check if a Result is successful (narrows to `Success<T>`) |
| `isFailure(Result<T>)` | Check if a Result is a failure (narrows to `Failure`) |
| `isLegacySuccess(LegacyResult)` | Check if a legacy result is successful |
| `isLegacyFailure(LegacyResult)` | Check if a legacy result is a failure |
| `isValidLSN(unknown)` | Check if a value is a valid LSN (bigint >= 0) |
| `isValidTransactionId(unknown)` | Check if a value is a valid TransactionId (non-empty string) |
| `isValidShardId(unknown)` | Check if a value is a valid ShardId (non-empty string, max 255 chars) |
| `isValidStatementHash(unknown)` | Check if a value is a valid StatementHash (non-empty string) |
| `isValidatedLSN(LSN)` | Check if an LSN was created through the factory function |
| `isValidatedTransactionId(TransactionId)` | Check if a TransactionId was created through the factory function |
| `isValidatedShardId(ShardId)` | Check if a ShardId was created through the factory function |
| `isValidatedStatementHash(StatementHash)` | Check if a StatementHash was created through the factory function |

### Result Pattern Types :green_circle: Stable

| Type | Description |
|------|-------------|
| `Result<T>` | Discriminated union of `Success<T>` and `Failure` |
| `Success<T>` | Successful result with `data: T` |
| `Failure` | Failed result with `error: ResultError` |
| `ResultError` | Error information with `code`, `message`, and optional `details` |
| `LegacyResult<T>` | Legacy result format for backward compatibility |

### Result Constructors :green_circle: Stable

| Function | Description |
|----------|-------------|
| `success<T>(data: T)` | Create a successful Result |
| `failure(code, message, details?)` | Create a failed Result |
| `failureFromError(error, code?)` | Create a Failure from an Error object |

### Result Utilities :green_circle: Stable

| Function | Description |
|----------|-------------|
| `unwrap<T>(Result<T>)` | Extract success value or throw on failure |
| `unwrapOr<T>(Result<T>, defaultValue)` | Extract success value or return default |
| `mapResult<T, U>(Result<T>, fn)` | Transform success value, pass through failure |
| `flatMapResult<T, U>(Result<T>, fn)` | Chain Result-returning operations |
| `combineResults<T>(Result<T>[])` | Combine multiple Results into `Result<T[]>` |
| `fromLegacyResult<T>(LegacyResult<T>, dataKey?)` | Convert legacy result to new Result type |

### Constants :green_circle: Stable

| Constant | Description |
|----------|-------------|
| `SQL_TO_JS_TYPE_MAP` | Mapping from SQL types to JS types |
| `JS_TO_SQL_TYPE_MAP` | Mapping from JS types to SQL types |
| `CDCOperationCode` | Numeric codes for CDC operations |
| `DEFAULT_IDEMPOTENCY_CONFIG` | Default idempotency settings |
| `DEFAULT_CLIENT_CAPABILITIES` | Default client capability settings |
| `DEFAULT_RETRY_CONFIG` | Default retry configuration (maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000) |

### Configuration Types :yellow_circle: Beta

| Type | Factory | Description |
|------|---------|-------------|
| `RetryConfig` | `createRetryConfig(config)` | Configuration for automatic retry behavior with exponential backoff |

### Runtime Utilities :red_circle: Experimental

| Function | Description |
|----------|-------------|
| `getWrapperMapStats()` | Get statistics about internal wrapper maps (size counts for memory monitoring) |
| `clearWrapperMaps()` | Clear internal wrapper maps (useful for testing or memory management) |
| `setWrapperCacheConfig(config)` | Configure wrapper cache behavior (maxSize, ttlMs, enabled) |
| `getWrapperCacheConfig()` | Get current wrapper cache configuration |
| `setDevMode(boolean)` | Enable/disable development mode for extra validation |
| `isDevMode()` | Check if development mode is enabled |
| `setStrictMode(boolean)` | Enable/disable strict mode for format validation |
| `isStrictMode()` | Check if strict mode is enabled |

```typescript
import {
  getWrapperMapStats,
  clearWrapperMaps,
  setWrapperCacheConfig,
  getWrapperCacheConfig,
  setDevMode,
  isDevMode
} from '@dotdo/sql-types';

// Configure wrapper cache
setWrapperCacheConfig({
  maxSize: 5000,    // Maximum entries (default: 10000)
  ttlMs: 60000,     // TTL in ms (0 = no expiry, default: 0)
  enabled: true,    // Enable/disable caching
});
console.log('Cache config:', getWrapperCacheConfig());

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
