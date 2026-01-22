# DoSQL Troubleshooting Guide

This guide covers common issues you may encounter when using DoSQL with Cloudflare Workers and Durable Objects, along with their solutions and prevention strategies.

## Table of Contents

- [Connection Issues](#connection-issues)
  - [WebSocket Connection Failures](#websocket-connection-failures)
  - [Authentication Errors](#authentication-errors)
  - [Rate Limiting Errors](#rate-limiting-errors)
- [Query Issues](#query-issues)
  - [SQL Syntax Errors](#sql-syntax-errors)
  - [Parameter Binding Issues](#parameter-binding-issues)
  - [Transaction Deadlocks](#transaction-deadlocks)
- [Performance Issues](#performance-issues)
  - [Slow Queries](#slow-queries)
  - [Memory Limits in Durable Objects](#memory-limits-in-durable-objects)
  - [Connection Pool Exhaustion](#connection-pool-exhaustion)
- [CDC/Streaming Issues](#cdcstreaming-issues)
  - [Subscription Failures](#subscription-failures)
  - [Missing Events](#missing-events)
  - [Backpressure Handling](#backpressure-handling)
- [Migration Issues](#migration-issues)
  - [Schema Migration Errors](#schema-migration-errors)
  - [Data Type Incompatibilities](#data-type-incompatibilities)
  - [Checksum Mismatches](#checksum-mismatches)
- [Worker Deployment Issues](#worker-deployment-issues)
  - [Bundle Size Limits](#bundle-size-limits)
  - [Wrangler Configuration](#wrangler-configuration)
  - [R2 Bucket Binding Errors](#r2-bucket-binding-errors)

---

## Connection Issues

### WebSocket Connection Failures

**Problem:** Client fails to establish WebSocket connection to the Durable Object.

**Symptoms:**
- `WebSocket connection failed` error
- `Connection refused` or `Connection timeout`
- `TIMEOUT` RPC error code

**Root Causes:**
1. Network connectivity issues between client and edge
2. Durable Object not deployed or not accessible
3. Incorrect WebSocket URL format
4. Worker is hibernating and slow to wake

**Solution Steps:**

```typescript
// 1. Verify your connection URL format
const wsClient = await createWebSocketClient({
  url: 'wss://your-worker.your-subdomain.workers.dev/db',
  connectTimeoutMs: 10000,  // Increase timeout for cold starts
  autoReconnect: true,
  maxReconnectAttempts: 5,
  reconnectDelayMs: 1000,
});

// 2. Implement connection retry logic
async function connectWithRetry(maxRetries = 3): Promise<DoSQLClient> {
  for (let i = 0; i < maxRetries; i++) {
    try {
      const client = await createWebSocketClient({ url, autoReconnect: true });
      return client;
    } catch (error) {
      if (i === maxRetries - 1) throw error;
      await new Promise(r => setTimeout(r, 1000 * Math.pow(2, i))); // Exponential backoff
    }
  }
  throw new Error('Connection failed after retries');
}

// 3. Handle connection events
client.on('disconnect', () => {
  console.log('Connection lost, attempting reconnect...');
});

client.on('error', (error) => {
  console.error('Connection error:', error.code, error.message);
});
```

**Prevention Tips:**
- Always configure `autoReconnect: true` for production clients
- Set appropriate timeouts accounting for DO cold start time (~50-200ms)
- Implement health check pings to detect stale connections
- Use HTTP fallback for environments where WebSocket is unreliable

---

### Authentication Errors

**Problem:** Requests are rejected with authentication or authorization errors.

**Symptoms:**
- `UNAUTHORIZED` error code (401)
- `FORBIDDEN` error code (403)
- `R2_PERMISSION_DENIED` for storage operations

**Root Causes:**
1. Missing or invalid authentication headers
2. Expired authentication tokens
3. Insufficient permissions for the requested operation
4. CORS misconfiguration blocking requests

**Solution Steps:**

```typescript
// 1. Verify authentication is properly configured
const client = createHttpClient({
  url: 'https://your-worker.workers.dev/db',
  headers: {
    'Authorization': `Bearer ${await getAuthToken()}`,
  },
});

// 2. Implement token refresh
async function executeWithAuth<T>(fn: () => Promise<T>): Promise<T> {
  try {
    return await fn();
  } catch (error) {
    if (error.code === 'UNAUTHORIZED') {
      await refreshAuthToken();
      return await fn();  // Retry with new token
    }
    throw error;
  }
}

// 3. In your Worker, verify the request
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const authHeader = request.headers.get('Authorization');
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return new Response('Unauthorized', { status: 401 });
    }

    const token = authHeader.slice(7);
    if (!await verifyToken(token, env)) {
      return new Response('Forbidden', { status: 403 });
    }

    // Proceed with request...
  },
};
```

**Prevention Tips:**
- Implement automatic token refresh before expiration
- Use environment-specific authentication (dev vs. production)
- Configure CORS headers properly in your Worker
- Log authentication failures for debugging (without exposing sensitive data)

---

### Rate Limiting Errors

**Problem:** Requests are being rejected due to rate limits.

**Symptoms:**
- `R2_RATE_LIMITED` error code
- HTTP 429 status codes
- `retryAfter` value in error response

**Root Causes:**
1. Too many requests to a single DO instance
2. R2 API rate limits exceeded
3. Cloudflare account-level rate limits
4. Burst traffic patterns

**Solution Steps:**

```typescript
// 1. Implement exponential backoff for retries
async function executeWithBackoff<T>(
  fn: () => Promise<T>,
  maxRetries = 3
): Promise<T> {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      if (error.code === 'R2_RATE_LIMITED' || error.httpStatus === 429) {
        const delay = error.retryAfter
          ? error.retryAfter * 1000
          : Math.min(1000 * Math.pow(2, attempt), 30000);

        console.warn(`Rate limited, retrying in ${delay}ms...`);
        await new Promise(r => setTimeout(r, delay));
        continue;
      }
      throw error;
    }
  }
  throw new Error('Max retries exceeded');
}

// 2. Use batch operations to reduce request count
const batchResult = await client.batch({
  queries: [
    { sql: 'INSERT INTO users (name) VALUES (?)', params: ['Alice'] },
    { sql: 'INSERT INTO users (name) VALUES (?)', params: ['Bob'] },
    { sql: 'INSERT INTO users (name) VALUES (?)', params: ['Carol'] },
  ],
  atomic: true,
});

// 3. Implement client-side rate limiting
import { RateLimiter } from 'limiter';

const limiter = new RateLimiter({ tokensPerInterval: 100, interval: 'second' });

async function rateLimitedQuery(sql: string, params?: unknown[]) {
  await limiter.removeTokens(1);
  return client.query({ sql, params });
}
```

**Prevention Tips:**
- Use batch operations to combine multiple queries
- Implement request queuing with rate limiting
- Distribute load across multiple DO instances (sharding)
- Monitor request rates and set up alerts before hitting limits

---

## Query Issues

### SQL Syntax Errors

**Problem:** SQL queries fail with syntax errors.

**Symptoms:**
- `SQLSyntaxError` exception
- `SYNTAX_ERROR` RPC error code
- Error message with line/column location

**Root Causes:**
1. Invalid SQL syntax
2. Using unsupported SQL features
3. Typos in table or column names
4. Missing semicolons or quotes

**Solution Steps:**

```typescript
// 1. The error includes location information
try {
  await db.query('SELECT * FORM users');  // Typo: FORM instead of FROM
} catch (error) {
  if (error instanceof SQLSyntaxError) {
    console.error(error.format());
    // Output:
    // Unexpected token 'FORM', expected 'FROM' at line 1, column 10
    //   SELECT * FORM users
    //            ^
  }
}

// 2. Validate queries before execution (development)
import { validateSQL } from '@dotdo/dosql/parser';

const validation = validateSQL('SELECT * FROM users WHERE id = ?');
if (!validation.valid) {
  console.error('Invalid SQL:', validation.errors);
}

// 3. Use parameterized queries to avoid injection issues
// WRONG - SQL injection risk
const bad = await db.query(`SELECT * FROM users WHERE name = '${userName}'`);

// CORRECT - Parameterized
const good = await db.query('SELECT * FROM users WHERE name = ?', [userName]);
```

**Common Syntax Mistakes:**

| Mistake | Correct |
|---------|---------|
| `SELECT * FORM users` | `SELECT * FROM users` |
| `WHERE id = '1'` (for INTEGER) | `WHERE id = 1` |
| `INSERT users (name)` | `INSERT INTO users (name)` |
| `DELETE users WHERE id = 1` | `DELETE FROM users WHERE id = 1` |

**Prevention Tips:**
- Use TypeScript for compile-time query validation
- Implement SQL linting in your CI/CD pipeline
- Always use parameterized queries
- Test queries in development before deploying

---

### Parameter Binding Issues

**Problem:** Query parameters are not being bound correctly.

**Symptoms:**
- Wrong results returned
- Type mismatch errors (`TYPE_MISMATCH`)
- Null values where data expected

**Root Causes:**
1. Parameter count mismatch (too few or too many)
2. Wrong parameter order
3. Type conversion issues
4. Mixing positional and named parameters

**Solution Steps:**

```typescript
// 1. Positional parameters - order matters!
// WRONG - Parameters in wrong order
await db.query(
  'SELECT * FROM users WHERE name = ? AND age > ?',
  [25, 'Alice']  // age and name swapped!
);

// CORRECT
await db.query(
  'SELECT * FROM users WHERE name = ? AND age > ?',
  ['Alice', 25]
);

// 2. Named parameters - order doesn't matter
await db.query(
  'SELECT * FROM users WHERE name = :name AND age > :age',
  { name: 'Alice', age: 25 }
);

// 3. Handle NULL values explicitly
await db.query(
  'SELECT * FROM users WHERE email = ? OR email IS NULL',
  [email ?? null]
);

// 4. Type conversion for special types
// BLOB - convert to Uint8Array
const embedding = new Float32Array([0.1, 0.2, 0.3]);
await db.run(
  'INSERT INTO vectors (id, data) VALUES (?, ?)',
  [1, new Uint8Array(embedding.buffer)]
);

// Date/time - use ISO strings
await db.run(
  'INSERT INTO events (name, timestamp) VALUES (?, ?)',
  ['login', new Date().toISOString()]
);
```

**Prevention Tips:**
- Prefer named parameters for complex queries
- Use TypeScript types for parameter validation
- Document expected types for each query
- Test edge cases (null, empty strings, zero values)

---

### Transaction Deadlocks

**Problem:** Transactions are failing due to deadlocks.

**Symptoms:**
- `TransactionError` with code `TXN_DEADLOCK`
- Transactions timing out
- `TXN_LOCK_TIMEOUT` errors

**Root Causes:**
1. Two transactions waiting for each other's locks
2. Long-running transactions holding locks
3. Nested transactions with conflicting access patterns
4. High concurrency on same resources

**Solution Steps:**

```typescript
// 1. Handle deadlock with automatic retry
async function executeWithDeadlockRetry<T>(
  fn: (tx: Transaction) => Promise<T>,
  maxRetries = 3
): Promise<T> {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await db.transaction(fn);
    } catch (error) {
      if (error instanceof TransactionError) {
        if (error.code === TransactionErrorCode.DEADLOCK) {
          const delay = Math.random() * 100 * Math.pow(2, attempt);
          console.warn(`Deadlock detected, retry ${attempt + 1} in ${delay}ms`);
          await new Promise(r => setTimeout(r, delay));
          continue;
        }
        if (error.code === TransactionErrorCode.LOCK_TIMEOUT) {
          console.error('Lock timeout - consider reducing transaction scope');
        }
      }
      throw error;
    }
  }
  throw new Error('Max deadlock retries exceeded');
}

// 2. Use IMMEDIATE mode to acquire locks upfront
await db.transaction(async (tx) => {
  await tx.run('UPDATE accounts SET balance = balance - 100 WHERE id = ?', [1]);
  await tx.run('UPDATE accounts SET balance = balance + 100 WHERE id = ?', [2]);
}, { mode: 'IMMEDIATE' });

// 3. Keep transactions short
// BAD - Long transaction
await db.transaction(async (tx) => {
  const data = await fetchExternalData();  // Slow external call!
  await tx.run('INSERT INTO data VALUES (?)', [data]);
});

// GOOD - Fetch first, then transact
const data = await fetchExternalData();
await db.transaction(async (tx) => {
  await tx.run('INSERT INTO data VALUES (?)', [data]);
});

// 4. Access resources in consistent order
// If multiple transactions access tables A and B,
// always access them in the same order (e.g., A then B)
```

**Prevention Tips:**
- Keep transactions as short as possible
- Access tables in a consistent order across your codebase
- Use `IMMEDIATE` mode when you know you'll write
- Set appropriate lock timeouts
- Monitor for deadlock patterns in production

---

## Performance Issues

### Slow Queries

**Problem:** Queries are taking too long to execute.

**Symptoms:**
- High `executionTimeMs` in query responses
- Request timeouts
- Sluggish user experience

**Root Causes:**
1. Missing indexes on queried columns
2. Full table scans on large tables
3. Complex joins without proper indexing
4. Querying too much data

**Solution Steps:**

```typescript
// 1. Analyze query execution
const result = await db.query(`
  EXPLAIN QUERY PLAN
  SELECT * FROM orders WHERE user_id = 42
`);
console.log(result);
// Look for "SCAN" (bad) vs "SEARCH" (good)

// 2. Add indexes for frequently queried columns
await db.exec(`
  CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
  CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders(created_at);
`);

// 3. Use covering indexes for common queries
await db.exec(`
  CREATE INDEX idx_orders_user_status
  ON orders(user_id, status, created_at);
`);

// 4. Limit result sets
// BAD - Fetches all orders
const allOrders = await db.query('SELECT * FROM orders WHERE user_id = ?', [userId]);

// GOOD - Paginated
const recentOrders = await db.query(`
  SELECT id, status, total
  FROM orders
  WHERE user_id = ?
  ORDER BY created_at DESC
  LIMIT 20 OFFSET 0
`, [userId]);

// 5. Use query streaming for large results
for await (const chunk of client.queryStream({
  sql: 'SELECT * FROM large_table',
  chunkSize: 1000,
})) {
  await processChunk(chunk.rows);
}
```

**Index Guidelines:**

| Query Pattern | Recommended Index |
|---------------|-------------------|
| `WHERE col = ?` | Single column index |
| `WHERE a = ? AND b = ?` | Composite index `(a, b)` |
| `ORDER BY col` | Index on `col` |
| `WHERE a = ? ORDER BY b` | Composite index `(a, b)` |
| Range queries | Index on range column |

**Prevention Tips:**
- Create indexes during schema migrations, not as afterthoughts
- Monitor query performance with logging
- Use `EXPLAIN QUERY PLAN` during development
- Set query timeouts to prevent runaway queries
- Review slow query logs periodically

---

### Memory Limits in Durable Objects

**Problem:** Durable Object is running out of memory.

**Symptoms:**
- Worker crashes with memory errors
- `RESOURCE_EXHAUSTED` errors
- DO becomes unresponsive

**Root Causes:**
1. Loading too much data into memory
2. Large result sets not being streamed
3. Memory leaks in long-running DOs
4. Too many concurrent operations

**Root Cause Details:**
- Durable Objects have a 128MB memory limit
- Large B-tree caches can consume significant memory
- Unbounded query results load entirely into memory

**Solution Steps:**

```typescript
// 1. Stream large result sets
// BAD - Loads all rows into memory
const allRows = await db.query('SELECT * FROM huge_table');

// GOOD - Process in chunks
const stmt = db.prepare('SELECT * FROM huge_table');
for await (const row of stmt.iterate()) {
  await processRow(row);
}

// 2. Configure B-tree cache size appropriately
const db = await DB('tenant', {
  btreeConfig: {
    maxCachedPages: 100,  // Limit cache size
    pageSize: 64 * 1024,  // 64KB pages
  },
});

// 3. Use pagination for user-facing queries
async function* paginatedQuery(sql: string, pageSize = 100) {
  let offset = 0;
  while (true) {
    const rows = await db.query(
      `${sql} LIMIT ? OFFSET ?`,
      [pageSize, offset]
    );
    if (rows.length === 0) break;
    yield rows;
    offset += pageSize;
  }
}

// 4. Clean up resources explicitly
class DatabaseDO implements DurableObject {
  private db: Database | null = null;

  async alarm() {
    // Periodic cleanup
    if (this.db) {
      await this.db.exec('PRAGMA shrink_memory');
    }
  }
}

// 5. Use tiered storage to offload cold data
const db = await DB('tenant', {
  storage: {
    hot: state.storage,
    cold: env.R2_BUCKET,  // Large/old data goes to R2
  },
  tieredConfig: {
    hotDataMaxAge: 60 * 60 * 1000,  // 1 hour
    maxHotFileSize: 5 * 1024 * 1024, // 5MB max in DO
  },
});
```

**Prevention Tips:**
- Always use pagination or streaming for large datasets
- Configure appropriate cache limits
- Use tiered storage (DO + R2) for large databases
- Monitor memory usage with DO analytics
- Implement periodic cleanup alarms

---

### Connection Pool Exhaustion

**Problem:** Unable to create new connections due to pool exhaustion.

**Symptoms:**
- New requests timing out
- `RESOURCE_EXHAUSTED` errors
- Connections stuck in pending state

**Root Causes:**
1. Connections not being properly released
2. Long-running queries holding connections
3. Pool size too small for load
4. Connection leaks from error handling

**Solution Steps:**

```typescript
// 1. Always release connections, even on error
async function safeQuery(sql: string, params?: unknown[]) {
  const conn = await pool.acquire();
  try {
    return await conn.query(sql, params);
  } finally {
    pool.release(conn);  // Always release!
  }
}

// 2. Use connection timeout
const pool = createConnectionPool({
  maxConnections: 10,
  acquireTimeout: 5000,  // Fail fast if pool exhausted
  idleTimeout: 30000,    // Release idle connections
});

// 3. Implement connection health checks
const pool = createConnectionPool({
  validateOnBorrow: true,
  validationQuery: 'SELECT 1',
});

// 4. Monitor pool metrics
setInterval(() => {
  const stats = pool.getStats();
  console.log({
    active: stats.activeConnections,
    idle: stats.idleConnections,
    waiting: stats.waitingRequests,
  });

  if (stats.waitingRequests > 10) {
    console.warn('Connection pool under pressure!');
  }
}, 10000);
```

**Prevention Tips:**
- Use connection pool wrappers that handle release automatically
- Set appropriate pool sizes based on expected load
- Monitor pool metrics and alert on high wait times
- Implement circuit breakers for connection failures

---

## CDC/Streaming Issues

### Subscription Failures

**Problem:** CDC subscription fails to start or stops unexpectedly.

**Symptoms:**
- `CDC_SUBSCRIPTION_FAILED` error
- Subscription silently stops receiving events
- `SUBSCRIPTION_ERROR` RPC code

**Root Causes:**
1. Invalid starting LSN (too old, compacted)
2. Network interruption
3. DO hibernation disconnecting subscribers
4. Invalid filter configuration

**Solution Steps:**

```typescript
// 1. Handle subscription errors
const subscription = cdc.subscribe(fromLSN, filter);

try {
  for await (const event of subscription.iterate()) {
    await processEvent(event);
    // Checkpoint after processing
    await saveCheckpoint(event.lsn);
  }
} catch (error) {
  if (error instanceof CDCError) {
    switch (error.code) {
      case CDCErrorCode.LSN_NOT_FOUND:
        // LSN was compacted, restart from earliest available
        const status = await cdc.getStatus();
        return subscribe(status.earliestLSN);

      case CDCErrorCode.SUBSCRIPTION_FAILED:
        // Retry with backoff
        await retrySubscription();
        break;
    }
  }
  throw error;
}

// 2. Use replication slots for durable position tracking
const slot = await cdc.slots.createSlot('my-consumer', fromLSN);

// Subscription resumes from slot position on reconnect
const subscription = await cdc.slots.subscribeFromSlot('my-consumer');

for await (const event of subscription.iterate()) {
  await processEvent(event);
  // Update slot position (persisted)
  await cdc.slots.updateSlot('my-consumer', event.lsn);
}

// 3. Implement heartbeat/keepalive
const subscription = await client.subscribeCDC({
  fromLSN,
  tables: ['orders'],
});

const heartbeat = setInterval(async () => {
  if (!subscription.isActive()) {
    console.warn('Subscription inactive, reconnecting...');
    clearInterval(heartbeat);
    await reconnect();
  }
}, 30000);
```

**Prevention Tips:**
- Use replication slots for production consumers
- Implement checkpoint persistence
- Monitor subscription health with heartbeats
- Handle DO hibernation gracefully with auto-reconnect

---

### Missing Events

**Problem:** CDC subscription is not receiving all expected events.

**Symptoms:**
- Gaps in event sequence
- `CDC_LSN_NOT_FOUND` when resuming
- Inconsistent data in downstream systems

**Root Causes:**
1. Starting from wrong LSN
2. WAL segments compacted before consumption
3. Filter excluding events unintentionally
4. Race condition between writes and subscription

**Solution Steps:**

```typescript
// 1. Start from correct position
// Get current LSN before starting subscription
const status = await db.queryOne('PRAGMA dosql_current_lsn');
const startLSN = status.lsn;

// Start subscription from known position
const subscription = cdc.subscribe(startLSN);

// 2. Configure WAL retention
const db = await DB('tenant', {
  walConfig: {
    retentionPeriodMs: 7 * 24 * 60 * 60 * 1000, // 7 days
    minRetainedSegments: 100,
  },
});

// 3. Verify filter configuration
const subscription = cdc.subscribe(fromLSN, {
  tables: ['orders', 'order_items'],  // Include all related tables
  operations: ['INSERT', 'UPDATE', 'DELETE'],  // All operations
  includeTransactionControl: true,  // Include BEGIN/COMMIT for grouping
});

// 4. Detect and handle gaps
let lastLSN = fromLSN;

for await (const event of subscription.iterate()) {
  if (event.lsn > lastLSN + 1n) {
    console.warn(`Gap detected: ${lastLSN} -> ${event.lsn}`);
    // May need to resync affected data
    await handleGap(lastLSN, event.lsn);
  }
  lastLSN = event.lsn;
  await processEvent(event);
}
```

**Prevention Tips:**
- Configure adequate WAL retention for your consumption patterns
- Use replication slots that prevent premature compaction
- Implement gap detection and alerting
- Test CDC with high-volume writes to catch race conditions

---

### Backpressure Handling

**Problem:** CDC consumer cannot keep up with event production rate.

**Symptoms:**
- `CDC_BUFFER_OVERFLOW` errors
- Increasing lag between producer and consumer
- Memory pressure on consumer
- `BackpressureSignal` with `type: 'pause'`

**Root Causes:**
1. Consumer processing slower than production rate
2. Buffer size too small
3. Downstream system bottleneck
4. Burst of events overwhelming consumer

**Solution Steps:**

```typescript
// 1. Handle backpressure signals from lakehouse
const streamer = createLakehouseStreamer({
  cdc: subscription,
  lakehouse: {
    r2Bucket: env.DATA_BUCKET,
    prefix: 'cdc/',
    format: 'parquet',
  },
  batch: {
    maxSize: 10000,
    maxWaitMs: 5000,
  },
});

streamer.on('backpressure', (signal: BackpressureSignal) => {
  switch (signal.type) {
    case 'pause':
      console.warn(`Backpressure: pausing, buffer at ${signal.bufferUtilization * 100}%`);
      break;
    case 'slow_down':
      console.warn(`Slowing down, suggested delay: ${signal.suggestedDelayMs}ms`);
      break;
    case 'resume':
      console.log('Backpressure relieved, resuming normal speed');
      break;
  }
});

// 2. Configure appropriate buffer sizes
const subscription = cdc.subscribe(fromLSN, {
  maxBufferSize: 10000,  // Buffer up to 10k events
  batchSize: 1000,        // Process in batches of 1000
});

// 3. Implement batch processing
const batch: CDCEvent[] = [];
const BATCH_SIZE = 100;
const FLUSH_INTERVAL = 1000;

let flushTimer: Timer | null = null;

async function processBatch() {
  if (batch.length === 0) return;

  const toProcess = batch.splice(0, batch.length);
  await bulkInsertToDownstream(toProcess);

  // Acknowledge processed events
  const lastLSN = toProcess[toProcess.length - 1].lsn;
  await cdc.slots.updateSlot('my-consumer', lastLSN);
}

for await (const event of subscription.iterate()) {
  batch.push(event);

  if (batch.length >= BATCH_SIZE) {
    await processBatch();
  } else if (!flushTimer) {
    flushTimer = setTimeout(async () => {
      await processBatch();
      flushTimer = null;
    }, FLUSH_INTERVAL);
  }
}

// 4. Scale consumers horizontally
// Partition events by table or key range
const partition = hashKey(event.table) % NUM_CONSUMERS;
await sendToConsumer(partition, event);
```

**Prevention Tips:**
- Size buffers based on expected burst sizes
- Use batch processing instead of event-by-event
- Monitor consumer lag and alert when it grows
- Implement horizontal scaling for high-volume streams

---

## Migration Issues

### Schema Migration Errors

**Problem:** Database migrations fail to apply.

**Symptoms:**
- Migration runner throws errors
- Tables or columns not created
- Partial migration state

**Root Causes:**
1. SQL syntax errors in migration files
2. Attempting to create existing objects
3. Foreign key constraint violations
4. Transaction timeout during large migrations

**Solution Steps:**

```typescript
// 1. Get detailed error information
const runner = createMigrationRunner(db, {
  logger: consoleLogger,  // Enable logging
  dryRun: true,           // Test first
});

const status = await runner.getStatus(migrations);
console.log('Pending migrations:', status.pending);

// 2. Use IF NOT EXISTS for idempotent migrations
// migrations/001_create_users.sql
`
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
`;

// 3. Handle large data migrations in batches
// migrations/002_backfill_data.sql
`
-- Process in smaller chunks to avoid timeout
UPDATE users SET normalized_email = LOWER(email)
WHERE normalized_email IS NULL
LIMIT 1000;
`;

// Then use migration runner with continue-on-error to run repeatedly
const runner = createMigrationRunner(db, {
  continueOnError: true,  // Keep going on partial success
});

// 4. Implement rollback migrations
const migrations = [
  {
    id: '001_create_users',
    sql: 'CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)',
    downSql: 'DROP TABLE users',  // Rollback SQL
  },
];

// Rollback if needed
await runner.rollbackTo(migrations, '000_initial');
```

**Prevention Tips:**
- Always use `IF NOT EXISTS` / `IF EXISTS` clauses
- Test migrations on a copy of production data
- Implement down migrations for rollback capability
- Break large data migrations into smaller batches
- Use `dryRun: true` to validate migrations first

---

### Data Type Incompatibilities

**Problem:** Data type mismatches between schema and application.

**Symptoms:**
- Type conversion errors
- Unexpected null values
- Data truncation

**Root Causes:**
1. SQL type doesn't match TypeScript type
2. Implicit type conversion issues
3. Precision loss in numeric types
4. Encoding issues with text data

**Solution Steps:**

```typescript
// 1. Understand type mappings
// SQL INTEGER   -> TypeScript number (up to 2^53 - 1)
// SQL REAL      -> TypeScript number
// SQL TEXT      -> TypeScript string
// SQL BLOB      -> TypeScript Uint8Array
// SQL BOOLEAN   -> TypeScript boolean (stored as 0/1)

// 2. Handle BIGINT carefully
// For large integers, use bigint
const result = await db.query<{ id: bigint }>(
  'SELECT id FROM huge_table WHERE id > ?',
  [BigInt('9007199254740991')]  // Larger than Number.MAX_SAFE_INTEGER
);

// 3. Explicit type conversion in queries
await db.query(`
  SELECT
    id,
    CAST(amount AS REAL) as amount,
    CAST(quantity AS INTEGER) as quantity,
    CAST(active AS INTEGER) as active
  FROM products
`);

// 4. Handle dates consistently
// Store as ISO 8601 TEXT
await db.run(
  'INSERT INTO events (timestamp) VALUES (?)',
  [new Date().toISOString()]
);

// Parse when reading
const events = await db.query<{ timestamp: string }>('SELECT timestamp FROM events');
const dates = events.map(e => new Date(e.timestamp));

// 5. Handle BLOB data
// Encoding
const data = new TextEncoder().encode(jsonString);
await db.run('INSERT INTO blobs (data) VALUES (?)', [data]);

// Decoding
const result = await db.queryOne<{ data: Uint8Array }>('SELECT data FROM blobs WHERE id = ?', [1]);
const decoded = new TextDecoder().decode(result.data);
```

**Type Mapping Reference:**

| SQL Type | TypeScript | Notes |
|----------|------------|-------|
| `INTEGER` | `number` or `bigint` | Use `bigint` for values > 2^53 |
| `REAL` | `number` | 64-bit float |
| `TEXT` | `string` | UTF-8 encoded |
| `BLOB` | `Uint8Array` | Binary data |
| `BOOLEAN` | `boolean` | Stored as 0 or 1 |
| `NULL` | `null` | Explicit null |

**Prevention Tips:**
- Document expected types in your schema
- Use TypeScript interfaces for query results
- Test with edge case values (max/min, empty, null)
- Consider using Zod or similar for runtime validation

---

### Checksum Mismatches

**Problem:** Migration checksums don't match previously applied versions.

**Symptoms:**
- `Checksum mismatch` validation error
- Migration runner refuses to apply new migrations

**Root Causes:**
1. Migration file was modified after being applied
2. Line ending differences (CRLF vs LF)
3. Whitespace changes
4. Intentional schema changes to existing migration

**Solution Steps:**

```typescript
// 1. Validate checksums before running migrations
const runner = createMigrationRunner(db);
const validation = await runner.validateChecksums(migrations);

if (!validation.valid) {
  console.error('Checksum mismatches found:');
  for (const mismatch of validation.mismatches) {
    console.error(`  ${mismatch.id}: expected ${mismatch.expected}, got ${mismatch.actual}`);
  }

  // Decide how to handle:
  // 1. Fix the migration file to match original
  // 2. Force update the stored checksum (dangerous!)
  // 3. Create a new corrective migration
}

// 2. If the change was intentional, update the stored checksum
// WARNING: Only do this if you understand the implications!
await db.run(`
  UPDATE __dosql_migrations
  SET checksum = ?
  WHERE id = ?
`, [newChecksum, migrationId]);

// 3. Normalize line endings in migration files
// In your build process or CI:
// npx prettier --write ".do/migrations/**/*.sql" --end-of-line lf

// 4. Create a corrective migration instead of modifying
// migrations/003_fix_column_type.sql
`
-- Corrective migration for schema change
ALTER TABLE users ADD COLUMN email_temp TEXT;
UPDATE users SET email_temp = email;
ALTER TABLE users DROP COLUMN email;
ALTER TABLE users RENAME COLUMN email_temp TO email;
`;
```

**Prevention Tips:**
- Never modify applied migration files
- Normalize line endings in your repository (`.gitattributes`)
- Use migration checksums to detect accidental changes
- Always create new migrations for schema changes
- Document migration modification policy for your team

---

## Worker Deployment Issues

### Bundle Size Limits

**Problem:** Worker bundle exceeds Cloudflare's size limit.

**Symptoms:**
- Wrangler deploy fails with size error
- `Bundle size exceeds limit` error
- Deployment timeout

**Root Causes:**
1. Including unnecessary dependencies
2. Not tree-shaking unused code
3. Including development-only code
4. Large embedded assets

**Solution Steps:**

```bash
# 1. Analyze your bundle
npx wrangler deploy --dry-run --outdir dist
npx esbuild-bundle-analyzer dist/index.js

# 2. Check gzipped size
gzip -c dist/index.js | wc -c
```

```typescript
// 3. Use selective imports
// BAD - Imports entire library
import { DB, createCDC, createShardRouter, ... } from '@dotdo/dosql';

// GOOD - Import only what you need
import { DB } from '@dotdo/dosql';
import { createCDC } from '@dotdo/dosql/cdc';

// 4. Configure esbuild for optimal tree-shaking
// esbuild.config.js
export default {
  entryPoints: ['src/index.ts'],
  bundle: true,
  minify: true,
  format: 'esm',
  target: 'es2022',
  platform: 'browser',
  external: ['cloudflare:*'],  // Don't bundle CF APIs
  treeShaking: true,
  define: {
    'process.env.NODE_ENV': '"production"',
  },
};

// 5. Move large assets to R2
// Instead of embedding data in bundle
// const data = require('./large-data.json');

// Load from R2 at runtime
async function getData(env: Env) {
  const obj = await env.ASSETS_BUCKET.get('data.json');
  return obj?.json();
}
```

**DoSQL Bundle Sizes:**

| Configuration | Gzipped Size |
|--------------|--------------|
| Core only (`DB`) | ~7 KB |
| Core + CDC | ~9 KB |
| Core + Sharding | ~14 KB |
| Full library | ~34 KB |
| CF Free tier limit | 1,000 KB |

**Prevention Tips:**
- Monitor bundle size in CI/CD
- Use selective imports, not `import *`
- Configure proper tree-shaking
- Move static assets to R2 or KV
- Review dependencies for bloat regularly

---

### Wrangler Configuration

**Problem:** Wrangler fails to deploy or worker doesn't function correctly.

**Symptoms:**
- `Invalid configuration` errors
- Missing bindings at runtime
- DO not accessible
- R2 bucket undefined

**Root Causes:**
1. Missing or incorrect `wrangler.toml` / `wrangler.jsonc` settings
2. DO class not exported from entry point
3. Binding name mismatch
4. Migration tag missing

**Solution Steps:**

```jsonc
// wrangler.jsonc - Complete example
{
  "$schema": "node_modules/wrangler/config-schema.json",
  "name": "my-dosql-app",
  "main": "src/index.ts",
  "compatibility_date": "2024-01-01",
  "compatibility_flags": ["nodejs_compat"],

  // Durable Objects - REQUIRED
  "durable_objects": {
    "bindings": [
      {
        "name": "DOSQL_DB",           // Name in env
        "class_name": "TenantDatabase" // Must match exported class
      }
    ]
  },

  // DO migrations - REQUIRED for new classes
  "migrations": [
    {
      "tag": "v1",
      "new_classes": ["TenantDatabase"]
    }
  ],

  // R2 bucket - Optional but recommended
  "r2_buckets": [
    {
      "binding": "DATA_BUCKET",  // Name in env
      "bucket_name": "my-data"   // Actual bucket name
    }
  ],

  // KV namespace - Optional
  "kv_namespaces": [
    {
      "binding": "CACHE",
      "id": "abc123..."
    }
  ],

  // Environment variables
  "vars": {
    "ENVIRONMENT": "production"
  }
}
```

```typescript
// src/index.ts - Entry point
import { TenantDatabase } from './database';

// IMPORTANT: Must export the DO class!
export { TenantDatabase };

export interface Env {
  DOSQL_DB: DurableObjectNamespace;
  DATA_BUCKET: R2Bucket;
  CACHE: KVNamespace;
  ENVIRONMENT: string;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Verify bindings exist
    if (!env.DOSQL_DB) {
      return new Response('DO binding missing', { status: 500 });
    }

    const id = env.DOSQL_DB.idFromName('tenant-1');
    const db = env.DOSQL_DB.get(id);
    return db.fetch(request);
  },
};
```

**Common Configuration Mistakes:**

| Mistake | Fix |
|---------|-----|
| DO class not exported | Add `export { ClassName }` to entry point |
| Binding name mismatch | Ensure `wrangler.jsonc` binding name matches `env.NAME` |
| Missing migration tag | Add initial migration with `new_classes` |
| Wrong `main` path | Verify entry point path is correct |

**Prevention Tips:**
- Use TypeScript for type-safe `Env` interface
- Validate configuration with `wrangler deploy --dry-run`
- Test locally with `wrangler dev` before deploying
- Use environment-specific config files

---

### R2 Bucket Binding Errors

**Problem:** R2 bucket operations fail with binding errors.

**Symptoms:**
- `R2_BUCKET_NOT_BOUND` error
- `undefined` when accessing `env.BUCKET`
- `TypeError: Cannot read property 'get' of undefined`

**Root Causes:**
1. Bucket not configured in `wrangler.jsonc`
2. Bucket doesn't exist in Cloudflare account
3. Wrong binding name in code
4. Environment mismatch (dev vs prod bucket)

**Solution Steps:**

```bash
# 1. Create the R2 bucket if it doesn't exist
wrangler r2 bucket create my-data-bucket

# 2. List buckets to verify
wrangler r2 bucket list
```

```jsonc
// 3. Configure in wrangler.jsonc
{
  "r2_buckets": [
    {
      "binding": "DATA_BUCKET",        // Access via env.DATA_BUCKET
      "bucket_name": "my-data-bucket", // Must exist in your account
      "preview_bucket_name": "my-data-bucket-preview" // For wrangler dev
    }
  ]
}
```

```typescript
// 4. Defensive access in code
class DatabaseDO implements DurableObject {
  private r2: R2Bucket | null;

  constructor(state: DurableObjectState, env: Env) {
    this.r2 = env.DATA_BUCKET ?? null;

    if (!this.r2) {
      console.warn('R2 bucket not bound, cold storage disabled');
    }
  }

  private async getDB(): Promise<Database> {
    return await DB('tenant', {
      storage: {
        hot: this.state.storage,
        cold: this.r2 ?? undefined, // Gracefully handle missing bucket
      },
    });
  }
}

// 5. Handle R2 errors gracefully
async function readFromR2(bucket: R2Bucket, key: string) {
  try {
    const object = await bucket.get(key);
    if (!object) {
      throw new R2Error(R2ErrorCode.NOT_FOUND, `Object not found: ${key}`, key);
    }
    return await object.arrayBuffer();
  } catch (error) {
    const r2Error = createR2Error(error, key, 'read');

    // Log for debugging
    console.error(formatR2ErrorForLog(r2Error));

    // Provide user-friendly message
    throw new Error(r2Error.toUserMessage());
  }
}
```

**Prevention Tips:**
- Always verify bindings exist before using them
- Use preview buckets for local development
- Implement graceful degradation when R2 is unavailable
- Monitor R2 errors and set up alerts
- Document required bindings in README

---

## Getting Help

If you're still experiencing issues after trying these solutions:

1. **Check the logs**: Use `wrangler tail` to view real-time logs
2. **Enable debug logging**: Set `logger: consoleLogger` in DB options
3. **Review recent changes**: Check git history for related modifications
4. **Search issues**: Check the GitHub repository for similar problems
5. **Ask for help**: Open a GitHub issue with:
   - DoSQL version
   - Minimal reproduction code
   - Error messages and stack traces
   - Wrangler configuration (sanitized)

## Related Documentation

- [Getting Started](./getting-started.md) - Basic setup and usage
- [API Reference](./api-reference.md) - Complete API documentation
- [Architecture](./architecture.md) - Understanding DoSQL internals
- [Advanced Features](./advanced.md) - Time travel, branching, CDC
