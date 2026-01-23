# DoSQL & DoLake Troubleshooting Guide

**Version**: 1.0.0
**Last Updated**: 2026-01-22
**Maintainer**: Platform Team

---

## Table of Contents

1. [Quick Decision Tree](#quick-decision-tree)
2. [Connection Issues](#connection-issues)
   - [WebSocket Connection Failed](#websocket-connection-failed)
   - [Connection Timeout](#connection-timeout)
   - [Frequent Disconnections](#frequent-disconnections)
   - [Authentication Failures](#authentication-failures)
3. [Query Errors](#query-errors)
   - [Syntax Errors](#syntax-errors)
   - [Table or Column Not Found](#table-or-column-not-found)
   - [Constraint Violations](#constraint-violations)
   - [Type Mismatches](#type-mismatches)
4. [Performance Problems](#performance-problems)
   - [Slow Queries](#slow-queries)
   - [High Memory Usage](#high-memory-usage)
   - [Storage Growing Unexpectedly](#storage-growing-unexpectedly)
   - [Rate Limiting](#rate-limiting)
5. [CDC Streaming Issues](#cdc-streaming-issues)
   - [CDC Events Not Arriving](#cdc-events-not-arriving)
   - [Buffer Overflow](#buffer-overflow)
   - [LSN Not Found](#lsn-not-found)
   - [Consumer Lag](#consumer-lag)
6. [Transaction Problems](#transaction-problems)
   - [Deadlocks](#deadlocks)
   - [Transaction Timeouts](#transaction-timeouts)
   - [Serialization Failures](#serialization-failures)
   - [WAL Issues](#wal-issues)
7. [Deployment Issues](#deployment-issues)
   - [Worker Deployment Failures](#worker-deployment-failures)
   - [Migration Errors](#migration-errors)
   - [R2 Bucket Configuration](#r2-bucket-configuration)
   - [Environment Configuration](#environment-configuration)
8. [Related Documentation](#related-documentation)

---

## Quick Decision Tree

Use this decision tree to quickly identify your issue category:

```
Is your error related to...

Connection/Network?
├── Can't connect at all? ──────────────> [WebSocket Connection Failed]
├── Connects then drops? ───────────────> [Frequent Disconnections]
├── 401/403 errors? ────────────────────> [Authentication Failures]
└── Timeout errors? ────────────────────> [Connection Timeout]

Query Execution?
├── "Syntax error" message? ────────────> [Syntax Errors]
├── "Table/Column not found"? ──────────> [Table or Column Not Found]
├── "Constraint violation"? ────────────> [Constraint Violations]
└── "Type mismatch"? ───────────────────> [Type Mismatches]

Performance?
├── Queries taking too long? ───────────> [Slow Queries]
├── DO hitting memory limits? ──────────> [High Memory Usage]
├── Storage growing fast? ──────────────> [Storage Growing Unexpectedly]
└── 429 Too Many Requests? ─────────────> [Rate Limiting]

CDC/Streaming?
├── No events arriving? ────────────────> [CDC Events Not Arriving]
├── "buffer_full" NACKs? ───────────────> [Buffer Overflow]
├── "LSN not found" errors? ────────────> [LSN Not Found]
└── Consumer falling behind? ───────────> [Consumer Lag]

Transactions?
├── Deadlock detected? ─────────────────> [Deadlocks]
├── Transaction timeout? ───────────────> [Transaction Timeouts]
├── Serialization failure? ─────────────> [Serialization Failures]
└── WAL segment buildup? ───────────────> [WAL Issues]

Deployment?
├── wrangler deploy fails? ─────────────> [Worker Deployment Failures]
├── DO migration errors? ───────────────> [Migration Errors]
├── R2 access denied? ──────────────────> [R2 Bucket Configuration]
└── Missing env variables? ─────────────> [Environment Configuration]
```

---

## Connection Issues

### WebSocket Connection Failed

#### Symptoms

- Client throws `CONNECTION_ERROR` when attempting to connect
- Error messages like "WebSocket connection failed" or "Failed to establish connection"
- Network requests to the Worker URL return non-101 status codes

#### Possible Causes

1. **Worker not deployed**: The Cloudflare Worker is not deployed or has deployment errors
2. **Incorrect URL**: The WebSocket URL is malformed or points to the wrong endpoint
3. **CORS issues**: Cross-origin requests are being blocked
4. **Cloudflare Access blocking**: Access policies are preventing connections
5. **TLS/SSL issues**: Certificate problems or protocol mismatches

#### Diagnostic Steps

```bash
# 1. Verify Worker is deployed and responding
curl -I https://your-dosql-worker.example.com/health

# 2. Check wrangler deployment status
wrangler deployments list

# 3. Tail Worker logs for errors
wrangler tail --env production

# 4. Test WebSocket upgrade manually
curl -i -N \
  -H "Connection: Upgrade" \
  -H "Upgrade: websocket" \
  -H "Sec-WebSocket-Version: 13" \
  -H "Sec-WebSocket-Key: $(openssl rand -base64 16)" \
  https://your-dosql-worker.example.com/ws
```

```typescript
// Client-side diagnostics
const client = createSQLClient({
  url: 'wss://your-dosql-worker.example.com',
  debug: true, // Enable debug logging
  onConnectionStateChange: (state) => {
    console.log('Connection state:', state);
  },
});

try {
  await client.connect();
} catch (error) {
  console.error('Connection failed:', {
    code: error.code,
    message: error.message,
    cause: error.cause,
  });
}
```

#### Solution

**For Worker not deployed:**
```bash
# Deploy the Worker
wrangler deploy --env production

# Verify deployment
wrangler deployments list
```

**For incorrect URL:**
```typescript
// Ensure URL uses wss:// for secure WebSocket
const client = createSQLClient({
  url: 'wss://your-dosql-worker.example.com', // Not https://
});
```

**For CORS issues:**
```typescript
// In your Worker, add CORS headers
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        },
      });
    }
    // ... rest of handler
  },
};
```

**For Cloudflare Access:**
```typescript
// Include Access token in connection
const client = createSQLClient({
  url: 'wss://your-dosql-worker.example.com',
  headers: {
    'CF-Access-Client-Id': process.env.CF_ACCESS_CLIENT_ID,
    'CF-Access-Client-Secret': process.env.CF_ACCESS_CLIENT_SECRET,
  },
});
```

#### Prevention

- Set up health check monitoring for your Worker endpoints
- Use Cloudflare's built-in analytics to track connection success rates
- Implement connection retry logic with exponential backoff in clients
- Test WebSocket connectivity during CI/CD before deploying

---

### Connection Timeout

#### Symptoms

- `TIMEOUT` error code after connection attempt
- Error message: "Request timeout: connection exceeded Xms"
- Connections hang without completing

#### Possible Causes

1. **Network latency**: High latency between client and Cloudflare edge
2. **Durable Object cold start**: DO taking too long to initialize
3. **Worker overloaded**: Too many concurrent requests
4. **Firewall/proxy issues**: Intermediate systems blocking WebSocket
5. **Client timeout too short**: Default timeout is insufficient for workload

#### Diagnostic Steps

```typescript
// Measure connection time
const start = Date.now();
try {
  await client.connect();
  console.log(`Connected in ${Date.now() - start}ms`);
} catch (error) {
  console.log(`Failed after ${Date.now() - start}ms:`, error);
}

// Check if it's a cold start issue
const result = await client.query('SELECT 1');
console.log('First query latency indicates cold start time');
```

```bash
# Check network latency to Cloudflare
ping your-dosql-worker.example.com

# Trace route to identify bottlenecks
traceroute your-dosql-worker.example.com
```

#### Solution

**Increase client timeout:**
```typescript
const client = createSQLClient({
  url: 'wss://your-dosql-worker.example.com',
  timeout: 60000, // 60 seconds
  connectTimeout: 30000, // 30 seconds for initial connection
});
```

**Optimize DO initialization:**
```typescript
// In your DoSQL class, minimize work in constructor
export class DoSQL implements DurableObject {
  constructor(state: DurableObjectState, env: Env) {
    // Keep constructor lightweight
    this.state = state;
    this.env = env;
    // Defer heavy initialization to first request
  }

  async fetch(request: Request): Promise<Response> {
    // Lazy initialization
    if (!this.initialized) {
      await this.initialize();
    }
    // ...
  }
}
```

**Implement retry logic:**
```typescript
const client = createSQLClient({
  url: 'wss://your-dosql-worker.example.com',
  retry: {
    maxRetries: 3,
    initialDelayMs: 1000,
    maxDelayMs: 10000,
    backoffMultiplier: 2,
  },
});
```

#### Prevention

- Keep DO initialization lightweight (defer heavy work)
- Implement connection pooling for high-traffic scenarios
- Use Cloudflare's location hints to reduce latency
- Monitor cold start times and optimize as needed

---

### Frequent Disconnections

#### Symptoms

- WebSocket connections drop unexpectedly
- Error messages about connection being closed
- Need to frequently reconnect

#### Possible Causes

1. **Idle timeout**: Connection closed due to inactivity
2. **Worker eviction**: Cloudflare evicting idle Workers
3. **Network instability**: Intermittent network issues
4. **DO hibernation**: Durable Object going to sleep
5. **Resource limits**: CPU or memory limits exceeded

#### Diagnostic Steps

```typescript
// Monitor connection events
const client = createSQLClient({
  url: 'wss://your-dosql-worker.example.com',
  onConnectionStateChange: (state, reason) => {
    console.log(`Connection state: ${state}`, reason);
  },
  onError: (error) => {
    console.error('Connection error:', error);
  },
});
```

```bash
# Check Worker analytics for disconnection patterns
# Cloudflare Dashboard > Workers > Analytics

# Tail logs for disconnection events
wrangler tail --env production | grep -i "disconnect\|close\|error"
```

#### Solution

**Implement heartbeat/keepalive:**
```typescript
const client = createSQLClient({
  url: 'wss://your-dosql-worker.example.com',
  heartbeat: {
    intervalMs: 30000,  // Send heartbeat every 30 seconds
    timeoutMs: 10000,   // Consider dead if no response in 10s
  },
});
```

**Enable WebSocket Hibernation in DO:**
```typescript
// DoLake with hibernation support
export class DoLake implements DurableObject {
  async fetch(request: Request): Promise<Response> {
    if (request.headers.get('Upgrade') === 'websocket') {
      const pair = new WebSocketPair();
      const [client, server] = Object.values(pair);

      // Accept with hibernation - connection survives DO sleep
      this.state.acceptWebSocket(server, {
        /* attachment data */
      });

      return new Response(null, { status: 101, webSocket: client });
    }
    // ...
  }

  // Handle messages even after hibernation
  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
    // Process message
  }
}
```

**Implement automatic reconnection:**
```typescript
const client = createSQLClient({
  url: 'wss://your-dosql-worker.example.com',
  autoReconnect: true,
  reconnect: {
    maxRetries: Infinity,
    initialDelayMs: 1000,
    maxDelayMs: 30000,
  },
});
```

#### Prevention

- Use WebSocket Hibernation for long-lived connections
- Implement heartbeat mechanism on both client and server
- Set appropriate idle timeouts based on usage patterns
- Monitor connection duration metrics

---

### Authentication Failures

#### Symptoms

- `UNAUTHORIZED` (401) or `FORBIDDEN` (403) error codes
- Error messages about invalid or missing credentials
- Connections rejected before reaching the Durable Object

#### Possible Causes

1. **Invalid token**: JWT expired or malformed
2. **Missing credentials**: No authentication header provided
3. **Wrong token type**: Using API key where JWT expected
4. **Cloudflare Access misconfiguration**: Access policy blocking requests
5. **Token audience mismatch**: Token issued for different service

#### Diagnostic Steps

```typescript
// Debug authentication
const token = getAuthToken();

// Decode JWT to check expiration (without verification)
const parts = token.split('.');
if (parts.length === 3) {
  const payload = JSON.parse(atob(parts[1]));
  console.log('Token payload:', payload);
  console.log('Expires:', new Date(payload.exp * 1000));
  console.log('Issued:', new Date(payload.iat * 1000));
  console.log('Audience:', payload.aud);
}
```

```bash
# Test authentication manually
curl -H "Authorization: Bearer YOUR_TOKEN" \
  https://your-dosql-worker.example.com/health

# Check Cloudflare Access logs
# Dashboard > Access > Logs
```

#### Solution

**Refresh expired tokens:**
```typescript
const client = createSQLClient({
  url: 'wss://your-dosql-worker.example.com',
  auth: {
    getToken: async () => {
      // Refresh token if expired
      if (isTokenExpired(currentToken)) {
        currentToken = await refreshToken();
      }
      return currentToken;
    },
  },
});
```

**For Cloudflare Access:**
```typescript
// Using service tokens
const client = createSQLClient({
  url: 'wss://your-dosql-worker.example.com',
  headers: {
    'CF-Access-Client-Id': env.CF_ACCESS_CLIENT_ID,
    'CF-Access-Client-Secret': env.CF_ACCESS_CLIENT_SECRET,
  },
});
```

**Verify token in Worker:**
```typescript
// In your Worker
async function validateAuth(request: Request, env: Env): Promise<AuthResult> {
  const authHeader = request.headers.get('Authorization');

  if (!authHeader?.startsWith('Bearer ')) {
    return { valid: false, error: 'Missing Bearer token' };
  }

  const token = authHeader.slice(7);

  try {
    const payload = await verifyJWT(token, env.JWT_SECRET);

    // Check expiration
    if (payload.exp && payload.exp < Date.now() / 1000) {
      return { valid: false, error: 'Token expired' };
    }

    return { valid: true, userId: payload.sub, tenantId: payload.tenant_id };
  } catch (error) {
    return { valid: false, error: 'Invalid token' };
  }
}
```

#### Prevention

- Implement automatic token refresh before expiration
- Use short-lived tokens with refresh tokens
- Log authentication failures for monitoring
- Set up alerts for authentication failure spikes

---

## Query Errors

### Syntax Errors

#### Symptoms

- `SYNTAX_ERROR` error code
- Error message indicating position of error
- Query fails to execute

#### Possible Causes

1. **Typos in SQL keywords**: `SELCT` instead of `SELECT`
2. **Unbalanced quotes or parentheses**: Missing closing quote or bracket
3. **Invalid SQL for SQLite dialect**: Using features not supported
4. **Missing commas or operators**: Incorrect SQL structure
5. **Reserved word conflicts**: Using reserved words as identifiers

#### Diagnostic Steps

```typescript
try {
  await client.query('SELCT * FROM users WHERE id = ?', [1]);
} catch (error) {
  if (error.code === 'SYNTAX_ERROR') {
    console.log('Syntax error at:', error.position);
    console.log('Suggestion:', error.suggestion);
    console.log('Full message:', error.message);
  }
}
```

```sql
-- Test query structure in SQLite
-- Use a local SQLite instance or online tool to validate syntax
sqlite3 :memory: "SELECT * FROM users WHERE id = 1;"
```

#### Solution

**Common fixes:**
```typescript
// WRONG: Typo in keyword
await client.query('SELCT * FROM users');

// CORRECT:
await client.query('SELECT * FROM users');

// WRONG: Unbalanced parentheses
await client.query('SELECT * FROM users WHERE (id = 1');

// CORRECT:
await client.query('SELECT * FROM users WHERE (id = 1)');

// WRONG: Using reserved word without quoting
await client.query('SELECT order FROM orders');

// CORRECT: Quote reserved words
await client.query('SELECT "order" FROM orders');
// Or use different column name
await client.query('SELECT order_number FROM orders');

// WRONG: Missing comma
await client.query('SELECT id name FROM users');

// CORRECT:
await client.query('SELECT id, name FROM users');
```

**Validate SQL before execution:**
```typescript
import { isBalanced } from 'dosql';

function validateSql(sql: string): boolean {
  if (!isBalanced(sql)) {
    throw new Error('Unbalanced quotes or parentheses');
  }
  return true;
}
```

#### Prevention

- Use a SQL linter or IDE with SQL validation
- Implement SQL validation in development mode
- Use query builders for complex queries
- Keep a cheat sheet of SQLite-specific syntax

---

### Table or Column Not Found

#### Symptoms

- `TABLE_NOT_FOUND` or `COLUMN_NOT_FOUND` error codes
- Error message naming the missing object
- Query execution fails

#### Possible Causes

1. **Typo in table/column name**: Case sensitivity or spelling errors
2. **Missing migration**: Table was never created
3. **Wrong database/shard**: Connected to different DO instance
4. **Schema change**: Column was renamed or removed
5. **Table alias confusion**: Using wrong alias in complex queries

#### Diagnostic Steps

```typescript
// List all tables
const tables = await client.query(`
  SELECT name FROM sqlite_master
  WHERE type='table'
  ORDER BY name
`);
console.log('Available tables:', tables.rows);

// Check table schema
const schema = await client.query(`PRAGMA table_info(users)`);
console.log('Columns:', schema.rows);

// Check table exists before query
async function safeQuery(table: string, sql: string, params: unknown[]) {
  const tableExists = await client.query(`
    SELECT 1 FROM sqlite_master
    WHERE type='table' AND name=?
  `, [table]);

  if (tableExists.rows.length === 0) {
    throw new Error(`Table '${table}' does not exist`);
  }

  return client.query(sql, params);
}
```

#### Solution

**Create missing table:**
```typescript
// Check and create table if needed
await client.exec(`
  CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    name TEXT,
    created_at INTEGER DEFAULT (strftime('%s', 'now'))
  )
`);
```

**Fix case sensitivity:**
```typescript
// SQLite is case-insensitive for keywords but case-sensitive for identifiers
// by default. Check exact case of table name.

// If table was created as "Users" (capitalized)
await client.query('SELECT * FROM Users'); // Works
await client.query('SELECT * FROM users'); // May fail depending on collation
```

**Verify correct shard:**
```typescript
// Ensure you're connecting to the right DO
const tenantId = user.tenantId;
const doId = env.DOSQL.idFromName(`tenant:${tenantId}`);
const stub = env.DOSQL.get(doId);

// Each tenant's DO has its own tables
```

#### Prevention

- Use schema migrations that are idempotent (CREATE IF NOT EXISTS)
- Document schema in a shared location
- Validate table existence at application startup
- Use TypeScript types derived from schema

---

### Constraint Violations

#### Symptoms

- `CONSTRAINT_VIOLATION` error code
- Error message specifying which constraint failed
- INSERT, UPDATE, or DELETE operation fails

#### Possible Causes

1. **Duplicate primary/unique key**: Inserting duplicate value
2. **Foreign key violation**: Referenced record doesn't exist
3. **NOT NULL violation**: Required field is missing
4. **CHECK constraint failure**: Value doesn't meet constraint

#### Diagnostic Steps

```typescript
try {
  await client.exec(
    'INSERT INTO users (id, email) VALUES (?, ?)',
    ['user-1', 'duplicate@example.com']
  );
} catch (error) {
  if (error.code === 'CONSTRAINT_VIOLATION') {
    // Parse which constraint failed
    if (error.message.includes('UNIQUE')) {
      console.log('Duplicate key violation');
      // Check existing record
      const existing = await client.query(
        'SELECT * FROM users WHERE email = ?',
        ['duplicate@example.com']
      );
      console.log('Existing record:', existing.rows[0]);
    } else if (error.message.includes('FOREIGN KEY')) {
      console.log('Foreign key violation - referenced record missing');
    } else if (error.message.includes('NOT NULL')) {
      console.log('Required field is null');
    }
  }
}
```

#### Solution

**For UNIQUE violations - use upsert:**
```typescript
// SQLite upsert syntax
await client.exec(`
  INSERT INTO users (id, email, name)
  VALUES (?, ?, ?)
  ON CONFLICT(email) DO UPDATE SET
    name = excluded.name,
    updated_at = strftime('%s', 'now')
`, [id, email, name]);
```

**For FOREIGN KEY violations - verify reference:**
```typescript
// Check reference exists before insert
const parent = await client.query(
  'SELECT id FROM organizations WHERE id = ?',
  [orgId]
);

if (parent.rows.length === 0) {
  throw new Error(`Organization ${orgId} not found`);
}

await client.exec(
  'INSERT INTO users (id, email, org_id) VALUES (?, ?, ?)',
  [id, email, orgId]
);
```

**For NOT NULL violations - provide defaults:**
```typescript
// Ensure all required fields have values
const userData = {
  id: id ?? crypto.randomUUID(),
  email: email,
  name: name ?? 'Unknown',
  created_at: Date.now(),
};

await client.exec(
  'INSERT INTO users (id, email, name, created_at) VALUES (?, ?, ?, ?)',
  [userData.id, userData.email, userData.name, userData.created_at]
);
```

#### Prevention

- Check for existence before insert/update
- Use upsert patterns where appropriate
- Validate data at the application layer before querying
- Provide sensible defaults for optional fields

---

### Type Mismatches

#### Symptoms

- `TYPE_MISMATCH` error code
- Error message about incompatible types
- Data not stored correctly

#### Possible Causes

1. **Wrong JavaScript type**: Passing string where number expected
2. **Date format issues**: Incorrect date/timestamp format
3. **JSON column issues**: Invalid JSON string
4. **Boolean representation**: Using true/false vs 0/1

#### Diagnostic Steps

```typescript
// Check column types
const schema = await client.query('PRAGMA table_info(users)');
for (const col of schema.rows) {
  console.log(`${col.name}: ${col.type} (nullable: ${!col.notnull})`);
}

// Validate types before query
function validateTypes(data: Record<string, unknown>, schema: ColumnDef[]) {
  for (const col of schema) {
    const value = data[col.name];
    if (value === null || value === undefined) continue;

    switch (col.type.toUpperCase()) {
      case 'INTEGER':
        if (typeof value !== 'number' || !Number.isInteger(value)) {
          console.warn(`${col.name}: expected INTEGER, got ${typeof value}`);
        }
        break;
      case 'REAL':
        if (typeof value !== 'number') {
          console.warn(`${col.name}: expected REAL, got ${typeof value}`);
        }
        break;
      case 'TEXT':
        if (typeof value !== 'string') {
          console.warn(`${col.name}: expected TEXT, got ${typeof value}`);
        }
        break;
    }
  }
}
```

#### Solution

**Proper type handling:**
```typescript
// INTEGER columns
const userId = parseInt(userIdString, 10);
if (isNaN(userId)) throw new Error('Invalid user ID');

// Dates - store as INTEGER (Unix timestamp)
const timestamp = new Date().getTime(); // milliseconds
// or
const timestampSeconds = Math.floor(Date.now() / 1000); // seconds

// Boolean - SQLite uses 0/1
const isActive = user.active ? 1 : 0;

// JSON - stringify objects
const metadata = JSON.stringify({ preferences: { theme: 'dark' } });

await client.exec(`
  INSERT INTO users (id, created_at, is_active, metadata)
  VALUES (?, ?, ?, ?)
`, [userId, timestamp, isActive, metadata]);
```

**Reading typed data:**
```typescript
const result = await client.query('SELECT * FROM users WHERE id = ?', [1]);
const user = result.rows[0];

// Parse JSON columns
const metadata = JSON.parse(user.metadata);

// Convert timestamps
const createdAt = new Date(user.created_at);

// Convert booleans
const isActive = Boolean(user.is_active);
```

#### Prevention

- Define TypeScript interfaces for database records
- Create helper functions for type conversion
- Use a schema validation library like Zod
- Document type mappings between JS and SQL

---

## Performance Problems

### Slow Queries

#### Symptoms

- Query execution time exceeds expectations
- `TIMEOUT` errors on complex queries
- Application latency increases

#### Possible Causes

1. **Missing indexes**: Full table scans on large tables
2. **Inefficient query pattern**: N+1 queries, SELECT *
3. **Large result sets**: Returning too many rows
4. **Complex joins**: Multiple table joins without optimization
5. **Lock contention**: Concurrent writes blocking reads

#### Diagnostic Steps

```typescript
// Profile query execution
const start = performance.now();
const result = await client.query(sql, params);
const duration = performance.now() - start;

console.log(`Query took ${duration.toFixed(2)}ms, returned ${result.rows.length} rows`);

// Use EXPLAIN QUERY PLAN
const plan = await client.query('EXPLAIN QUERY PLAN ' + sql, params);
console.log('Query plan:', plan.rows);

// Check for SCAN (bad) vs SEARCH (good)
for (const step of plan.rows) {
  if (step.detail.includes('SCAN')) {
    console.warn('Full table scan detected:', step.detail);
  }
}
```

```sql
-- Analyze query plan
EXPLAIN QUERY PLAN
SELECT * FROM orders
WHERE user_id = 'user-123'
AND created_at > 1705840000000;

-- Good output:
-- SEARCH orders USING INDEX idx_orders_user_created (user_id=? AND created_at>?)

-- Bad output:
-- SCAN orders
```

#### Solution

**Add missing indexes:**
```sql
-- Create index for common query patterns
CREATE INDEX IF NOT EXISTS idx_orders_user_created
ON orders(user_id, created_at);

-- Create covering index for frequently queried columns
CREATE INDEX IF NOT EXISTS idx_orders_user_status_amount
ON orders(user_id, status, amount);

-- After creating indexes, update statistics
ANALYZE orders;
```

**Optimize query patterns:**
```typescript
// SLOW: SELECT * returns all columns
const orders = await client.query('SELECT * FROM orders WHERE user_id = ?', [userId]);

// FAST: Select only needed columns
const orders = await client.query(`
  SELECT id, status, amount, created_at
  FROM orders
  WHERE user_id = ?
`, [userId]);

// SLOW: N+1 query pattern
const users = await client.query('SELECT id FROM users LIMIT 100');
for (const user of users.rows) {
  const orders = await client.query('SELECT * FROM orders WHERE user_id = ?', [user.id]);
}

// FAST: Single query with JOIN
const results = await client.query(`
  SELECT u.id, u.name, o.id as order_id, o.amount
  FROM users u
  LEFT JOIN orders o ON u.id = o.user_id
  LIMIT 100
`);
```

**Limit result sets:**
```typescript
// Always use LIMIT for potentially large result sets
const orders = await client.query(`
  SELECT * FROM orders
  WHERE status = 'pending'
  ORDER BY created_at DESC
  LIMIT 100
`);

// Use pagination for large datasets
async function* paginateOrders(userId: string) {
  let cursor: string | null = null;
  const pageSize = 100;

  while (true) {
    const query = cursor
      ? 'SELECT * FROM orders WHERE user_id = ? AND id > ? ORDER BY id LIMIT ?'
      : 'SELECT * FROM orders WHERE user_id = ? ORDER BY id LIMIT ?';
    const params = cursor ? [userId, cursor, pageSize] : [userId, pageSize];

    const result = await client.query(query, params);
    if (result.rows.length === 0) break;

    yield* result.rows;
    cursor = result.rows[result.rows.length - 1].id;
    if (result.rows.length < pageSize) break;
  }
}
```

#### Prevention

- Review query plans during development
- Set up slow query logging
- Create indexes for common query patterns
- Use query builders to enforce best practices

---

### High Memory Usage

#### Symptoms

- DO approaching or exceeding 128MB memory limit
- Worker crashes with memory errors
- Performance degradation under load

#### Possible Causes

1. **Large result sets in memory**: Loading too many rows at once
2. **Memory leaks**: Unbounded caches or event listeners
3. **Large transaction buffers**: Too many pending writes
4. **Query result caching**: Over-aggressive caching

#### Diagnostic Steps

```typescript
// Log memory usage periodically
setInterval(() => {
  // Note: Exact memory APIs vary by runtime
  console.log('Memory stats:', {
    timestamp: new Date().toISOString(),
    // Add any available memory metrics
  });
}, 10000);

// Monitor cache sizes
console.log('Cache stats:', {
  queryCacheSize: queryCache.size,
  btreeCacheSize: btreeCache.size,
  pendingWrites: walBuffer.length,
});
```

#### Solution

**Stream large result sets:**
```typescript
// Instead of loading all into memory
const allOrders = await client.query('SELECT * FROM orders'); // BAD

// Stream with pagination
async function processOrders() {
  let offset = 0;
  const limit = 100;

  while (true) {
    const batch = await client.query(
      'SELECT * FROM orders LIMIT ? OFFSET ?',
      [limit, offset]
    );

    if (batch.rows.length === 0) break;

    for (const order of batch.rows) {
      await processOrder(order);
    }

    offset += limit;

    // Allow garbage collection between batches
    await new Promise(resolve => setTimeout(resolve, 0));
  }
}
```

**Implement bounded caches:**
```typescript
class BoundedCache<T> {
  private cache = new Map<string, { value: T; size: number }>();
  private currentSize = 0;

  constructor(
    private maxSizeBytes: number,
    private maxEntries: number
  ) {}

  set(key: string, value: T, sizeBytes: number): void {
    // Evict oldest entries if needed
    while (
      (this.currentSize + sizeBytes > this.maxSizeBytes ||
       this.cache.size >= this.maxEntries) &&
      this.cache.size > 0
    ) {
      const oldestKey = this.cache.keys().next().value;
      this.delete(oldestKey);
    }

    this.cache.set(key, { value, size: sizeBytes });
    this.currentSize += sizeBytes;
  }

  get(key: string): T | undefined {
    const entry = this.cache.get(key);
    if (entry) {
      // Move to end (LRU)
      this.cache.delete(key);
      this.cache.set(key, entry);
    }
    return entry?.value;
  }

  delete(key: string): void {
    const entry = this.cache.get(key);
    if (entry) {
      this.currentSize -= entry.size;
      this.cache.delete(key);
    }
  }
}

// Use bounded cache
const queryCache = new BoundedCache<QueryResult>(
  16 * 1024 * 1024, // 16MB max
  1000              // 1000 entries max
);
```

**Clear buffers after operations:**
```typescript
// After large batch operations, clear temporary data
await client.transaction(async (tx) => {
  for (const batch of chunks) {
    await tx.insertBatch('logs', batch);
  }
});

// Force garbage collection opportunity
await new Promise(resolve => setTimeout(resolve, 0));
```

#### Prevention

- Set memory budgets for each subsystem
- Use streaming for large data operations
- Implement LRU eviction for all caches
- Monitor memory usage in production

---

### Storage Growing Unexpectedly

#### Symptoms

- DO storage approaching 10GB limit
- R2 costs increasing unexpectedly
- WAL segment count growing

#### Possible Causes

1. **WAL not being compacted**: Checkpoint not running
2. **Orphaned files**: Old data files not cleaned up
3. **CDC events accumulating**: Consumer not keeping up
4. **No data retention policy**: Old data never deleted

#### Diagnostic Steps

```sql
-- Check total storage size in DO
SELECT SUM(LENGTH(data)) as total_bytes
FROM _wal_segments;

-- Check WAL segment count
SELECT COUNT(*) as segment_count,
       MIN(lsn_start) as oldest_lsn,
       MAX(lsn_end) as newest_lsn
FROM _wal_segments;

-- Check table sizes
SELECT name, SUM(pgsize) as size_bytes
FROM dbstat
GROUP BY name
ORDER BY size_bytes DESC;
```

```bash
# Check R2 bucket size
wrangler r2 object list my-lakehouse-bucket --prefix="warehouse/" | wc -l

# List large objects
wrangler r2 object list my-lakehouse-bucket --json | jq '.[] | select(.size > 100000000)'
```

#### Solution

**Force WAL checkpoint:**
```typescript
// Trigger manual checkpoint
await client.exec('PRAGMA wal_checkpoint(TRUNCATE)');

// Or via admin API
await fetch('https://your-dosql-worker.example.com/admin/checkpoint', {
  method: 'POST',
  headers: { 'Authorization': `Bearer ${adminToken}` },
});
```

**Configure automatic checkpointing:**
```typescript
// In your DO, schedule regular checkpoints
export class DoSQL implements DurableObject {
  async alarm(): Promise<void> {
    // Check if checkpoint needed
    const stats = await this.getWalStats();

    if (stats.segmentCount > 100 || stats.totalBytes > 50 * 1024 * 1024) {
      await this.sql.exec('PRAGMA wal_checkpoint(TRUNCATE)');
    }

    // Schedule next alarm
    await this.state.storage.setAlarm(Date.now() + 60000); // 1 minute
  }
}
```

**Implement data retention:**
```typescript
// Delete old data
await client.exec(`
  DELETE FROM logs
  WHERE created_at < ?
`, [Date.now() - 30 * 24 * 60 * 60 * 1000]); // 30 days

// Vacuum to reclaim space
await client.exec('VACUUM');
```

**Clean orphaned R2 files:**
```typescript
// In DoLake cleanup routine
async function cleanupOrphanedFiles(bucket: R2Bucket) {
  // Get files referenced in manifests
  const referencedFiles = await getReferencedFiles();

  // List all data files
  const allFiles = await bucket.list({ prefix: 'warehouse/' });

  // Delete orphaned files
  for (const file of allFiles.objects) {
    if (file.key.endsWith('.parquet') && !referencedFiles.has(file.key)) {
      console.log('Deleting orphaned file:', file.key);
      await bucket.delete(file.key);
    }
  }
}
```

#### Prevention

- Configure automatic WAL checkpointing
- Implement data retention policies
- Schedule regular cleanup jobs
- Monitor storage usage metrics

---

### Rate Limiting

#### Symptoms

- `rate_limited` or `RATE_LIMITED` error code
- HTTP 429 Too Many Requests responses
- `Retry-After` header in responses

#### Possible Causes

1. **Too many requests**: Exceeding requests per second limit
2. **Too many connections**: Exceeding connection limits
3. **Large payloads**: Exceeding message size limits
4. **Burst traffic**: Sudden traffic spikes

#### Diagnostic Steps

```typescript
// Check rate limit response details
try {
  await client.query('SELECT 1');
} catch (error) {
  if (error.code === 'rate_limited') {
    console.log('Rate limited:', {
      retryAfter: error.retryAfter,
      limit: error.limit,
      remaining: error.remaining,
      resetAt: error.resetAt,
    });
  }
}
```

```bash
# Monitor rate limit metrics
# Check Cloudflare Dashboard > Analytics > Workers

# Look for 429 responses in logs
wrangler tail --env production | grep "429"
```

#### Solution

**Implement exponential backoff:**
```typescript
async function queryWithBackoff<T>(
  fn: () => Promise<T>,
  maxRetries = 5
): Promise<T> {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      if (error.code === 'rate_limited' && attempt < maxRetries - 1) {
        const delay = error.retryAfter
          ? error.retryAfter * 1000
          : Math.min(1000 * Math.pow(2, attempt), 30000);

        console.log(`Rate limited, retrying in ${delay}ms`);
        await new Promise(resolve => setTimeout(resolve, delay));
        continue;
      }
      throw error;
    }
  }
  throw new Error('Max retries exceeded');
}
```

**Batch requests:**
```typescript
// Instead of many small requests
for (const item of items) {
  await client.exec('INSERT INTO items VALUES (?)', [item.id]);
}

// Use batch insert
await client.insertBatch('items', items);
```

**Implement client-side rate limiting:**
```typescript
class RateLimiter {
  private tokens: number;
  private lastRefill: number;

  constructor(
    private tokensPerSecond: number,
    private maxTokens: number
  ) {
    this.tokens = maxTokens;
    this.lastRefill = Date.now();
  }

  async acquire(): Promise<void> {
    this.refill();

    if (this.tokens < 1) {
      const waitTime = (1 - this.tokens) / this.tokensPerSecond * 1000;
      await new Promise(resolve => setTimeout(resolve, waitTime));
      this.refill();
    }

    this.tokens -= 1;
  }

  private refill(): void {
    const now = Date.now();
    const elapsed = (now - this.lastRefill) / 1000;
    this.tokens = Math.min(this.maxTokens, this.tokens + elapsed * this.tokensPerSecond);
    this.lastRefill = now;
  }
}

// Usage
const limiter = new RateLimiter(10, 50); // 10 req/s, burst of 50
await limiter.acquire();
await client.query('SELECT 1');
```

#### Prevention

- Implement client-side rate limiting
- Use request batching where possible
- Cache frequently accessed data
- Monitor rate limit metrics and adjust limits

---

## CDC Streaming Issues

### CDC Events Not Arriving

#### Symptoms

- DoLake not receiving events from DoSQL
- CDC consumer shows no new data
- Event count stuck at zero

#### Possible Causes

1. **WebSocket not connected**: Connection to DoLake failed
2. **CDC not enabled**: CDC streaming not configured
3. **Network issues**: Connectivity between DOs
4. **Buffer full on sender**: DoSQL buffering events locally

#### Diagnostic Steps

```typescript
// Check CDC connection status
const status = await doLakeClient.getStatus();
console.log('DoLake status:', {
  connectedSources: status.sources.length,
  bufferUtilization: status.buffer.utilization,
  lastEventTime: status.buffer.newestBatchTime,
});

// Check DoSQL CDC buffer
const doSqlStatus = await doSqlClient.getCDCStatus();
console.log('DoSQL CDC status:', {
  connected: doSqlStatus.connected,
  pendingEvents: doSqlStatus.pendingEvents,
  lastSentSequence: doSqlStatus.lastSentSequence,
});
```

```bash
# Check WebSocket connectivity
curl -i -N \
  -H "Upgrade: websocket" \
  -H "Connection: Upgrade" \
  https://your-dolake-worker.example.com/ws/cdc

# Tail DoLake logs
wrangler tail dolake-worker --env production
```

#### Solution

**Verify CDC configuration:**
```typescript
// DoSQL configuration
const doSql = new DoSQL(state, env);
await doSql.enableCDC({
  targetUrl: 'wss://your-dolake-worker.example.com',
  batchSize: 100,
  flushIntervalMs: 5000,
});
```

**Check and reconnect:**
```typescript
// In DoSQL, monitor and reconnect CDC
async function ensureCDCConnection() {
  if (!this.cdcConnection || this.cdcConnection.readyState !== WebSocket.OPEN) {
    console.log('CDC connection lost, reconnecting...');
    await this.connectToDoLake();
  }
}

// Schedule periodic check
this.state.storage.setAlarm(Date.now() + 30000);
```

**Flush pending events:**
```typescript
// Force flush of pending CDC events
await doSqlClient.flushCDC();
```

#### Prevention

- Implement connection health checks with heartbeats
- Set up alerts for CDC connection status
- Monitor event lag metrics
- Implement automatic reconnection

---

### Buffer Overflow

#### Symptoms

- `buffer_full` NACK messages
- CDC events being dropped
- DoLake not accepting new batches

#### Possible Causes

1. **Consumer too slow**: DoLake can't write to R2 fast enough
2. **High event volume**: Burst of events exceeding buffer capacity
3. **R2 issues**: Slow or failing writes to R2
4. **Buffer too small**: Configuration doesn't match workload

#### Diagnostic Steps

```typescript
// Check buffer status
const status = await doLakeClient.getStatus();
console.log('Buffer status:', {
  eventCount: status.buffer.eventCount,
  sizeBytes: status.buffer.sizeBytes,
  utilization: status.buffer.utilization,
  oldestEvent: status.buffer.oldestBatchTime,
  timeUntilFlush: status.timeUntilFlush,
});

// Monitor NACK messages
doSqlClient.onNack((nack) => {
  console.error('NACK received:', {
    reason: nack.reason,
    sequenceNumber: nack.sequenceNumber,
    retryDelayMs: nack.retryDelayMs,
  });
});
```

#### Solution

**Increase buffer size:**
```toml
# wrangler.toml
[env.production.vars]
DOLAKE_MAX_BUFFER_SIZE = "134217728"  # 128MB
DOLAKE_EVENT_THRESHOLD = "20000"
```

**Trigger manual flush:**
```typescript
// Force immediate flush to R2
await doLakeClient.flush();
```

**Implement backpressure handling:**
```typescript
// In DoSQL CDC sender
class CDCSender {
  private sendRate = 1000; // events per second

  async handleNack(nack: NackMessage) {
    if (nack.reason === 'buffer_full') {
      // Reduce send rate
      this.sendRate = Math.max(100, this.sendRate * 0.5);
      console.log(`Buffer full, reduced rate to ${this.sendRate}/s`);

      // Wait before retry
      await new Promise(resolve =>
        setTimeout(resolve, nack.retryDelayMs)
      );
    }
  }

  async handleAck(ack: AckMessage) {
    if (ack.details.bufferUtilization < 0.5) {
      // Buffer has room, increase rate
      this.sendRate = Math.min(10000, this.sendRate * 1.1);
    }
  }
}
```

**Reduce batch sizes:**
```typescript
// Send smaller batches more frequently
const cdcConfig = {
  batchSize: 50,           // Smaller batches
  flushIntervalMs: 1000,   // More frequent
  maxBatchSizeBytes: 64 * 1024, // 64KB limit
};
```

#### Prevention

- Monitor buffer utilization continuously
- Implement adaptive send rates
- Size buffers for peak workloads
- Set up alerts for high buffer utilization

---

### LSN Not Found

#### Symptoms

- `CDC_LSN_NOT_FOUND` error code
- Error message: "LSN X not found - oldest available LSN is Y"
- Consumer can't resume from checkpoint

#### Possible Causes

1. **LSN too old**: WAL compacted before consumer caught up
2. **Consumer offline too long**: Missed data was cleaned up
3. **Invalid LSN**: Corrupted or incorrect checkpoint
4. **Full sync required**: Need to start from beginning

#### Diagnostic Steps

```typescript
// Get available LSN range
const range = await doLakeClient.getLSNRange();
console.log('Available LSN range:', {
  oldest: range.oldestLSN.toString(),
  newest: range.newestLSN.toString(),
});

// Compare with checkpoint
const checkpoint = await getConsumerCheckpoint();
console.log('Consumer checkpoint:', checkpoint.lsn.toString());

if (checkpoint.lsn < range.oldestLSN) {
  console.error('Checkpoint is older than available data!');
}
```

#### Solution

**Start from oldest available:**
```typescript
try {
  await consumer.subscribe({ fromLSN: checkpointLSN });
} catch (error) {
  if (error.code === 'CDC_LSN_NOT_FOUND') {
    console.log('Checkpoint too old, starting from oldest available');

    // Get oldest available LSN
    const range = await doLakeClient.getLSNRange();

    // Restart from oldest
    await consumer.subscribe({ fromLSN: range.oldestLSN });

    // NOTE: You may have missed events - handle accordingly
  }
}
```

**Implement full sync fallback:**
```typescript
async function syncFromSource() {
  try {
    // Try incremental sync
    await incrementalSync(lastCheckpointLSN);
  } catch (error) {
    if (error.code === 'CDC_LSN_NOT_FOUND') {
      console.log('Incremental sync failed, performing full sync');

      // Full table sync
      await fullTableSync();

      // Update checkpoint to current
      const range = await doLakeClient.getLSNRange();
      await saveCheckpoint(range.newestLSN);
    }
  }
}
```

**Increase WAL retention:**
```toml
# wrangler.toml - increase WAL retention
[env.production.vars]
WAL_RETENTION_MS = "604800000"  # 7 days
```

#### Prevention

- Checkpoint frequently (every batch or time interval)
- Monitor consumer lag
- Set alerts when lag approaches retention limit
- Keep WAL retention longer than maximum expected outage

---

### Consumer Lag

#### Symptoms

- Events arriving with significant delay
- Growing backlog of unprocessed events
- Consumer sequence far behind producer

#### Possible Causes

1. **Slow consumer processing**: Consumer can't keep up with event rate
2. **Network latency**: High latency between DOs
3. **Consumer errors**: Failing to process events
4. **Insufficient resources**: Consumer needs more capacity

#### Diagnostic Steps

```typescript
// Monitor lag metrics
const producerStatus = await doSqlClient.getCDCStatus();
const consumerStatus = await getConsumerStatus();

const lag = {
  sequenceLag: producerStatus.lastSequence - consumerStatus.lastSequence,
  timeLag: Date.now() - consumerStatus.lastEventTimestamp,
};

console.log('Consumer lag:', lag);

if (lag.sequenceLag > 10000) {
  console.error('High sequence lag - consumer falling behind');
}
if (lag.timeLag > 60000) {
  console.error('High time lag - events more than 1 minute old');
}
```

#### Solution

**Optimize consumer processing:**
```typescript
// Process events in parallel where possible
async function processBatch(events: CDCEvent[]) {
  // Group events by table for batch processing
  const byTable = groupBy(events, 'table');

  // Process tables in parallel
  await Promise.all(
    Object.entries(byTable).map(([table, tableEvents]) =>
      processTableEvents(table, tableEvents)
    )
  );
}

// Use batch operations in consumer
async function processTableEvents(table: string, events: CDCEvent[]) {
  const inserts = events.filter(e => e.operation === 'INSERT');
  const updates = events.filter(e => e.operation === 'UPDATE');
  const deletes = events.filter(e => e.operation === 'DELETE');

  // Batch inserts
  if (inserts.length > 0) {
    await targetDb.insertBatch(table, inserts.map(e => e.after));
  }

  // Batch updates and deletes similarly
}
```

**Scale consumers:**
```typescript
// Use multiple consumer instances with partitioning
const partitions = 4;
const consumerId = parseInt(process.env.CONSUMER_ID);

// Each consumer handles specific tables
const myTables = allTables.filter(
  (_, idx) => idx % partitions === consumerId
);

await consumer.subscribe({
  tables: myTables,
  fromLSN: checkpoints[consumerId],
});
```

**Implement checkpointing:**
```typescript
// Checkpoint after each batch to enable recovery
let processedCount = 0;

for await (const batch of consumer.subscribe({ fromLSN: checkpoint })) {
  await processBatch(batch.events);
  processedCount += batch.events.length;

  // Checkpoint every 1000 events
  if (processedCount >= 1000) {
    await saveCheckpoint(batch.lastLSN);
    processedCount = 0;
  }
}
```

#### Prevention

- Monitor consumer lag continuously
- Set up alerts for lag thresholds
- Scale consumers based on event volume
- Optimize consumer processing

---

## Transaction Problems

### Deadlocks

#### Symptoms

- `DEADLOCK_DETECTED` error code
- Transactions failing with conflict errors
- System throughput decreasing

#### Possible Causes

1. **Circular lock dependencies**: Transaction A waits on B, B waits on A
2. **Long-running transactions**: Holding locks too long
3. **Hot rows**: Many transactions updating same rows
4. **Improper lock ordering**: Inconsistent access patterns

#### Diagnostic Steps

```typescript
// Monitor transaction conflicts
let deadlockCount = 0;
let conflictCount = 0;

client.onTransactionError((error) => {
  if (error.code === 'DEADLOCK_DETECTED') {
    deadlockCount++;
    console.log('Deadlock details:', {
      transactionId: error.transactionId,
      waitingOn: error.waitingOn,
      tables: error.tables,
    });
  }
});

setInterval(() => {
  console.log('Transaction stats:', { deadlockCount, conflictCount });
}, 60000);
```

#### Solution

**Implement retry with backoff:**
```typescript
async function executeWithRetry<T>(
  fn: (tx: TransactionContext) => Promise<T>,
  maxRetries = 3
): Promise<T> {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await client.transaction(fn);
    } catch (error) {
      if (error.code === 'DEADLOCK_DETECTED' && attempt < maxRetries - 1) {
        // Random backoff to prevent repeated conflicts
        const delay = Math.random() * 100 * Math.pow(2, attempt);
        await new Promise(resolve => setTimeout(resolve, delay));
        continue;
      }
      throw error;
    }
  }
  throw new Error('Max retries exceeded');
}
```

**Establish consistent lock ordering:**
```typescript
// Always access tables in alphabetical order
async function transferFunds(fromAccount: string, toAccount: string, amount: number) {
  // Sort account IDs to ensure consistent lock order
  const [first, second] = [fromAccount, toAccount].sort();

  return client.transaction(async (tx) => {
    // Lock accounts in consistent order
    const firstAccount = await tx.query(
      'SELECT * FROM accounts WHERE id = ?',
      [first]
    );
    const secondAccount = await tx.query(
      'SELECT * FROM accounts WHERE id = ?',
      [second]
    );

    // Perform transfer
    // ...
  });
}
```

**Reduce transaction scope:**
```typescript
// SLOW: Long transaction holding locks
await client.transaction(async (tx) => {
  const data = await tx.query('SELECT * FROM large_table');
  const processed = await expensiveProcessing(data); // Holds locks!
  await tx.insertBatch('results', processed);
});

// BETTER: Minimize lock duration
const data = await client.query('SELECT * FROM large_table');
const processed = await expensiveProcessing(data); // No locks held
await client.transaction(async (tx) => {
  await tx.insertBatch('results', processed);
});
```

#### Prevention

- Keep transactions short
- Access resources in consistent order
- Use appropriate isolation levels
- Monitor deadlock rates

---

### Transaction Timeouts

#### Symptoms

- `TXN_LOCK_TIMEOUT` error code
- Transactions failing after waiting for locks
- Application experiencing delays

#### Possible Causes

1. **Long-running transactions**: Other transactions holding locks
2. **Heavy write load**: Many concurrent write transactions
3. **Timeout too short**: Configuration doesn't match workload
4. **Blocking queries**: Queries holding locks longer than expected

#### Diagnostic Steps

```typescript
// Profile transaction durations
const transactionTimes: number[] = [];

async function profiledTransaction<T>(fn: (tx: TransactionContext) => Promise<T>): Promise<T> {
  const start = Date.now();
  try {
    return await client.transaction(fn);
  } finally {
    const duration = Date.now() - start;
    transactionTimes.push(duration);

    if (duration > 1000) {
      console.warn('Slow transaction:', duration, 'ms');
    }
  }
}

// Check average transaction time
setInterval(() => {
  if (transactionTimes.length > 0) {
    const avg = transactionTimes.reduce((a, b) => a + b, 0) / transactionTimes.length;
    const max = Math.max(...transactionTimes);
    console.log('Transaction stats:', { avg, max, count: transactionTimes.length });
    transactionTimes.length = 0;
  }
}, 60000);
```

#### Solution

**Increase timeout for specific transactions:**
```typescript
await client.transaction(
  async (tx) => {
    // Long-running transaction work
  },
  {
    timeout: 60000, // 60 seconds
  }
);
```

**Use read-only transactions where possible:**
```typescript
// Read-only transactions don't acquire write locks
const reports = await client.transaction(
  async (tx) => {
    return tx.query('SELECT * FROM reports WHERE date >= ?', [startDate]);
  },
  { readOnly: true }
);
```

**Break up large transactions:**
```typescript
// Instead of one huge transaction
await client.transaction(async (tx) => {
  for (const item of thousandsOfItems) {
    await tx.exec('INSERT INTO items VALUES (?)', [item.id]);
  }
});

// Use batched approach
const batchSize = 100;
for (let i = 0; i < items.length; i += batchSize) {
  const batch = items.slice(i, i + batchSize);
  await client.transaction(async (tx) => {
    await tx.insertBatch('items', batch);
  });
}
```

#### Prevention

- Keep transactions as short as possible
- Use read-only transactions for queries
- Monitor transaction durations
- Set appropriate timeout values

---

### Serialization Failures

#### Symptoms

- `SERIALIZATION_FAILURE` error code
- Transactions failing under SERIALIZABLE isolation
- Concurrent update conflicts

#### Possible Causes

1. **Concurrent updates to same rows**: Multiple transactions modifying same data
2. **SERIALIZABLE isolation**: Stricter isolation detecting conflicts
3. **Read-write conflicts**: Read data changed before commit
4. **High contention**: Many transactions competing for same resources

#### Diagnostic Steps

```typescript
// Monitor serialization failures
client.onTransactionError((error) => {
  if (error.code === 'SERIALIZATION_FAILURE') {
    console.log('Serialization failure:', {
      table: error.table,
      rowId: error.rowId,
      operation: error.operation,
    });
  }
});
```

#### Solution

**Implement optimistic concurrency:**
```typescript
// Add version column for optimistic locking
async function updateWithOptimisticLock(id: string, updates: Partial<User>) {
  // Get current version
  const current = await client.query(
    'SELECT version FROM users WHERE id = ?',
    [id]
  );

  if (current.rows.length === 0) {
    throw new Error('Record not found');
  }

  const currentVersion = current.rows[0].version;

  // Update with version check
  const result = await client.exec(`
    UPDATE users
    SET name = ?, email = ?, version = version + 1
    WHERE id = ? AND version = ?
  `, [updates.name, updates.email, id, currentVersion]);

  if (result.rowsAffected === 0) {
    throw new Error('Concurrent modification detected');
  }

  return result;
}
```

**Retry serialization failures:**
```typescript
async function serializableTransaction<T>(
  fn: (tx: TransactionContext) => Promise<T>,
  maxRetries = 5
): Promise<T> {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await client.transaction(fn, {
        isolation: 'SERIALIZABLE',
      });
    } catch (error) {
      if (error.code === 'SERIALIZATION_FAILURE' && attempt < maxRetries - 1) {
        // Exponential backoff with jitter
        const delay = Math.random() * 50 * Math.pow(2, attempt);
        await new Promise(resolve => setTimeout(resolve, delay));
        continue;
      }
      throw error;
    }
  }
  throw new Error('Max serialization retries exceeded');
}
```

**Consider lower isolation level:**
```typescript
// Use READ COMMITTED if SERIALIZABLE isn't required
await client.transaction(
  async (tx) => {
    // Transaction work
  },
  { isolation: 'READ_COMMITTED' }
);
```

#### Prevention

- Use appropriate isolation levels
- Implement retry logic for serialization failures
- Consider optimistic concurrency for high-contention scenarios
- Minimize transaction scope to reduce conflicts

---

### WAL Issues

#### Symptoms

- WAL segment count growing
- Storage usage increasing
- Checkpoint errors

#### Possible Causes

1. **Checkpoint not running**: Automatic checkpoint disabled or failing
2. **Long-running transactions**: Preventing WAL cleanup
3. **CDC consumer behind**: WAL retained for CDC replay
4. **Disk space issues**: Can't write checkpoint

#### Diagnostic Steps

```sql
-- Check WAL status
SELECT
  COUNT(*) as segment_count,
  MIN(lsn_start) as oldest_lsn,
  MAX(lsn_end) as newest_lsn,
  SUM(LENGTH(data)) as total_bytes
FROM _wal_segments;

-- Check checkpoint status
PRAGMA wal_checkpoint(PASSIVE);
-- Returns: busy, log, checkpointed
```

```typescript
// Monitor WAL metrics
const walStats = await client.query(`
  SELECT COUNT(*) as count, SUM(LENGTH(data)) as bytes
  FROM _wal_segments
`);
console.log('WAL stats:', walStats.rows[0]);

if (walStats.rows[0].count > 100) {
  console.warn('High WAL segment count - checkpoint may be needed');
}
```

#### Solution

**Force checkpoint:**
```sql
-- Passive checkpoint (doesn't block)
PRAGMA wal_checkpoint(PASSIVE);

-- Full checkpoint (blocks writes briefly)
PRAGMA wal_checkpoint(FULL);

-- Truncate checkpoint (reclaims space)
PRAGMA wal_checkpoint(TRUNCATE);
```

```typescript
// Programmatic checkpoint
await client.exec('PRAGMA wal_checkpoint(TRUNCATE)');
```

**Configure automatic checkpointing:**
```typescript
// In DoSQL alarm handler
async alarm(): Promise<void> {
  // Check if checkpoint needed
  const stats = await this.sql.exec(`
    SELECT COUNT(*) as count FROM _wal_segments
  `);

  if (stats[0].count > 50) {
    this.sql.exec('PRAGMA wal_checkpoint(TRUNCATE)');
  }

  // Schedule next check
  await this.state.storage.setAlarm(Date.now() + 60000);
}
```

**Archive old WAL segments:**
```typescript
// Move old segments to R2 before deletion
async function archiveWalSegments() {
  const segments = await client.query(`
    SELECT * FROM _wal_segments
    WHERE lsn_end < (SELECT MAX(lsn_end) - 1000000 FROM _wal_segments)
  `);

  for (const segment of segments.rows) {
    // Upload to R2
    await bucket.put(
      `_archive/wal/${segment.id}.bin`,
      segment.data
    );

    // Delete from DO
    await client.exec('DELETE FROM _wal_segments WHERE id = ?', [segment.id]);
  }
}
```

#### Prevention

- Enable automatic WAL checkpointing
- Monitor WAL segment count
- Set alerts for WAL growth
- Keep transactions short to allow cleanup

---

## Deployment Issues

### Worker Deployment Failures

#### Symptoms

- `wrangler deploy` command fails
- Error messages during deployment
- Worker not updating

#### Possible Causes

1. **TypeScript compilation errors**: Code doesn't compile
2. **Bundle size exceeded**: Worker too large
3. **Invalid wrangler.toml**: Configuration errors
4. **Authentication issues**: Not logged in to Cloudflare

#### Diagnostic Steps

```bash
# Check TypeScript compilation
npx tsc --noEmit

# Check bundle size
wrangler deploy --dry-run --outdir=dist

# Validate wrangler.toml
wrangler whoami
wrangler config

# Check deployment logs
wrangler deployments list
```

#### Solution

**Fix TypeScript errors:**
```bash
# Run type check
npx tsc --noEmit

# Fix errors shown in output
# Then retry deployment
wrangler deploy
```

**Reduce bundle size:**
```typescript
// Use dynamic imports for large dependencies
const { heavyLib } = await import('./heavy-lib');

// Externalize large dependencies if possible
// wrangler.toml:
// [build]
// external = ["some-large-package"]
```

**Fix wrangler.toml:**
```toml
# Ensure required fields are present
name = "dosql-worker"
main = "src/index.ts"
compatibility_date = "2024-12-01"

# Validate DO bindings
[durable_objects]
bindings = [
  { name = "DOSQL", class_name = "DoSQL" }
]

# Ensure migrations are sequential
[[migrations]]
tag = "v1"
new_sqlite_classes = ["DoSQL"]
```

**Re-authenticate:**
```bash
# Login to Cloudflare
wrangler login

# Verify authentication
wrangler whoami
```

#### Prevention

- Run `tsc --noEmit` in CI before deployment
- Monitor bundle size in CI
- Use deployment previews for testing
- Keep wrangler.toml in version control

---

### Migration Errors

#### Symptoms

- DO migration failures during deployment
- Error: "Migration tag already exists"
- Durable Objects not working after deployment

#### Possible Causes

1. **Reused migration tag**: Same tag used twice
2. **Invalid migration sequence**: Missing intermediate migration
3. **Class name mismatch**: Class name doesn't match binding
4. **SQLite migration issues**: Schema changes failing

#### Diagnostic Steps

```bash
# List current migrations
wrangler durable-objects migrations list

# Check DO status
wrangler durable-objects list DOSQL
```

#### Solution

**Use unique migration tags:**
```toml
# Each migration needs a unique tag
[[migrations]]
tag = "v1"
new_sqlite_classes = ["DoSQL"]

[[migrations]]
tag = "v2"  # Must be different from v1
new_classes = ["DoLake"]

[[migrations]]
tag = "v3"  # Must be different from v1 and v2
renamed_classes = [{ from = "OldName", to = "NewName" }]
```

**Handle SQLite schema migrations:**
```typescript
// In DoSQL constructor, handle schema migrations
export class DoSQL implements DurableObject {
  constructor(state: DurableObjectState, env: Env) {
    this.sql = state.storage.sql;

    // Run schema migrations
    this.migrateSchema();
  }

  private migrateSchema() {
    // Check schema version
    const version = this.sql.exec(`
      SELECT value FROM _meta WHERE key = 'schema_version'
    `);

    const currentVersion = version[0]?.value ? parseInt(version[0].value) : 0;

    // Apply migrations
    if (currentVersion < 1) {
      this.sql.exec(`
        CREATE TABLE IF NOT EXISTS users (
          id TEXT PRIMARY KEY,
          email TEXT NOT NULL
        );
        INSERT OR REPLACE INTO _meta (key, value) VALUES ('schema_version', '1');
      `);
    }

    if (currentVersion < 2) {
      this.sql.exec(`
        ALTER TABLE users ADD COLUMN created_at INTEGER;
        UPDATE _meta SET value = '2' WHERE key = 'schema_version';
      `);
    }
  }
}
```

#### Prevention

- Always use unique, sequential migration tags
- Test migrations in staging before production
- Keep migration history documented
- Use idempotent schema migrations

---

### R2 Bucket Configuration

#### Symptoms

- `R2_BUCKET_NOT_BOUND` error
- "Bucket not found" errors
- Permission denied when accessing R2

#### Possible Causes

1. **Missing binding**: R2 bucket not bound in wrangler.toml
2. **Wrong bucket name**: Bucket doesn't exist or wrong name
3. **Permission issues**: Worker doesn't have R2 access
4. **Environment mismatch**: Using wrong environment

#### Diagnostic Steps

```bash
# List available buckets
wrangler r2 bucket list

# Check binding in wrangler.toml
cat wrangler.toml | grep -A2 "r2_buckets"

# Test bucket access
wrangler r2 object list your-bucket-name
```

#### Solution

**Add R2 binding:**
```toml
# wrangler.toml
[[r2_buckets]]
binding = "LAKEHOUSE_BUCKET"
bucket_name = "dosql-lakehouse-prod"
```

**Create missing bucket:**
```bash
# Create the bucket
wrangler r2 bucket create dosql-lakehouse-prod

# Verify it exists
wrangler r2 bucket list
```

**Fix environment-specific binding:**
```toml
# Different buckets per environment
[[env.dev.r2_buckets]]
binding = "LAKEHOUSE_BUCKET"
bucket_name = "dosql-lakehouse-dev"

[[env.production.r2_buckets]]
binding = "LAKEHOUSE_BUCKET"
bucket_name = "dosql-lakehouse-prod"
```

**Verify in code:**
```typescript
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Check if bucket is bound
    if (!env.LAKEHOUSE_BUCKET) {
      console.error('R2 bucket not bound');
      return new Response('Storage not configured', { status: 500 });
    }

    // Test bucket access
    try {
      await env.LAKEHOUSE_BUCKET.head('test-key');
    } catch (error) {
      console.error('R2 access error:', error);
    }

    // ...
  },
};
```

#### Prevention

- Document required R2 bindings
- Include bucket setup in deployment scripts
- Test R2 access during deployment verification
- Use consistent naming across environments

---

### Environment Configuration

#### Symptoms

- Missing environment variables
- Wrong configuration values
- Different behavior between environments

#### Possible Causes

1. **Missing secrets**: Secrets not set for environment
2. **Wrong environment**: Deploying to wrong environment
3. **Variable typos**: Misspelled variable names
4. **Type coercion issues**: Numbers stored as strings

#### Diagnostic Steps

```bash
# List secrets for environment
wrangler secret list --env production

# Check variables in wrangler.toml
cat wrangler.toml | grep -A20 "\[env.production.vars\]"

# Verify environment in deployment
wrangler deploy --env production --dry-run
```

#### Solution

**Set missing secrets:**
```bash
# Set secret for production
wrangler secret put API_SECRET --env production

# Set secret for all environments
wrangler secret put API_SECRET
```

**Fix wrangler.toml variables:**
```toml
# Base variables (shared)
[vars]
LOG_LEVEL = "info"
ENVIRONMENT = "development"

# Production-specific
[env.production.vars]
LOG_LEVEL = "warn"
ENVIRONMENT = "production"
DOSQL_QUERY_TIMEOUT_MS = "30000"
```

**Handle variable types:**
```typescript
// Environment variables are always strings
interface Env {
  LOG_LEVEL: string;
  DOSQL_QUERY_TIMEOUT_MS: string;
  API_SECRET: string;
}

// Parse numeric values
const timeout = parseInt(env.DOSQL_QUERY_TIMEOUT_MS, 10);
if (isNaN(timeout)) {
  throw new Error('Invalid DOSQL_QUERY_TIMEOUT_MS');
}
```

**Validate configuration at startup:**
```typescript
function validateEnv(env: Env): void {
  const required = ['API_SECRET', 'DOSQL_QUERY_TIMEOUT_MS'];

  for (const key of required) {
    if (!env[key]) {
      throw new Error(`Missing required environment variable: ${key}`);
    }
  }

  // Validate numeric values
  const timeout = parseInt(env.DOSQL_QUERY_TIMEOUT_MS, 10);
  if (isNaN(timeout) || timeout < 1000) {
    throw new Error('DOSQL_QUERY_TIMEOUT_MS must be a number >= 1000');
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    validateEnv(env);
    // ...
  },
};
```

#### Prevention

- Document all required environment variables
- Use environment validation at startup
- Keep secrets in a secure password manager
- Use consistent variable naming

---

## Related Documentation

- [Error Codes Reference](./ERROR_CODES.md) - Comprehensive list of all error codes
- [Operations Guide](./OPERATIONS.md) - Deployment and operational procedures
- [Performance Tuning](./PERFORMANCE_TUNING.md) - Optimization strategies
- [Security Best Practices](./SECURITY.md) - Security guidelines
- [API Stability Policy](./STABILITY.md) - API stability guarantees

### External Resources

- [Cloudflare Workers Documentation](https://developers.cloudflare.com/workers/)
- [Durable Objects Documentation](https://developers.cloudflare.com/durable-objects/)
- [R2 Documentation](https://developers.cloudflare.com/r2/)
- [SQLite Documentation](https://www.sqlite.org/docs.html)

---

*Last updated: 2026-01-22*
*Maintained by: Platform Team*
