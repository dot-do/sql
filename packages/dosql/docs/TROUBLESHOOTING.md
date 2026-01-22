# DoSQL Troubleshooting Guide

This guide covers common issues you may encounter when using DoSQL with Cloudflare Workers and Durable Objects, along with their solutions and prevention strategies.

## Table of Contents

- [Quick Reference: Common Errors](#quick-reference-common-errors)
- [Connection Issues](#connection-issues)
  - [Durable Object Not Found](#durable-object-not-found)
  - [WebSocket Connection Failures](#websocket-connection-failures)
  - [WebSocket Disconnects](#websocket-disconnects)
  - [Authentication Errors](#authentication-errors)
  - [Rate Limiting Errors](#rate-limiting-errors)
  - [Timeout Configuration](#timeout-configuration)
  - [Retry Strategies](#retry-strategies)
- [Query Issues](#query-issues)
  - [SQL Syntax Errors](#sql-syntax-errors)
  - [Parameter Binding Issues](#parameter-binding-issues)
  - [Transaction Deadlocks](#transaction-deadlocks)
  - [Transaction Timeout](#transaction-timeout)
- [Performance Issues](#performance-issues)
  - [Slow Queries](#slow-queries)
  - [Query Analysis](#query-analysis)
  - [Memory Limits in Durable Objects](#memory-limits-in-durable-objects)
  - [High Memory Usage Optimization](#high-memory-usage-optimization)
  - [Cold Start Latency](#cold-start-latency)
  - [Connection Pool Exhaustion](#connection-pool-exhaustion)
- [Debugging Techniques](#debugging-techniques)
  - [Enable Debug Logging](#enable-debug-logging)
  - [Inspect DO State](#inspect-do-state)
  - [Trace Request Flow](#trace-request-flow)
  - [Use Wrangler Tail](#use-wrangler-tail)
  - [Network Debugging](#network-debugging)
- [CDC/Streaming Issues](#cdcstreaming-issues)
  - [Subscription Failures](#subscription-failures)
  - [Missing Events](#missing-events)
  - [Backpressure Handling](#backpressure-handling)
- [Migration Issues](#migration-issues)
  - [Schema Migration Errors](#schema-migration-errors)
  - [Migration Failed Debugging](#migration-failed-debugging)
  - [Data Type Incompatibilities](#data-type-incompatibilities)
  - [Checksum Mismatches](#checksum-mismatches)
  - [Migration Rollback](#migration-rollback)
- [Data Issues](#data-issues)
  - [Consistency Problems](#consistency-problems)
  - [Data Recovery](#data-recovery)
  - [Backup and Restoration](#backup-and-restoration)
- [Worker Deployment Issues](#worker-deployment-issues)
  - [Bundle Size Limits](#bundle-size-limits)
  - [Wrangler Configuration](#wrangler-configuration)
  - [R2 Storage Errors](#r2-storage-errors)
  - [R2 Bucket Binding Errors](#r2-bucket-binding-errors)

---

## Quick Reference: Common Errors

| Error Message | Likely Cause | Quick Fix |
|---------------|--------------|-----------|
| `Durable Object not found` | DO class not exported or migration tag missing | Export class and add migration |
| `Migration failed` | SQL syntax error or constraint violation | Check migration SQL, use dry-run |
| `Transaction timeout` | Long-running transaction or deadlock | Reduce transaction scope, tune timeout |
| `WebSocket connection failed` | Network issues or DO hibernated | Increase timeout, enable auto-reconnect |
| `R2 storage error` | Bucket not bound or doesn't exist | Create bucket, verify binding |
| `UNAUTHORIZED` | Missing or invalid auth token | Check Authorization header |
| `RESOURCE_EXHAUSTED` | Memory limit or pool exhaustion | Stream data, reduce cache size |
| `TXN_DEADLOCK` | Concurrent transactions on same resources | Retry with backoff, consistent ordering |
| `CDC_LSN_NOT_FOUND` | WAL segment compacted | Use replication slots, increase retention |
| `Checksum mismatch` | Migration file modified after apply | Never modify applied migrations |

---

## Connection Issues

### Durable Object Not Found

**Problem:** Requests fail with "Durable Object not found" or similar errors.

**Error Messages:**
- `Error: Durable Object not found`
- `Error: No such Durable Object`
- `Error: class_name not found in exports`
- `Error: Durable Object namespace not bound`

**Root Causes:**
1. DO class is not exported from the Worker entry point
2. Missing or incorrect migration tag in `wrangler.jsonc`
3. Class name mismatch between code and configuration
4. DO namespace not bound in environment
5. Deployment failed or incomplete

**Solution Steps:**

```typescript
// 1. CRITICAL: Export the DO class from your entry point
// src/index.ts
import { TenantDatabase } from './database';

// This export is REQUIRED - the DO class must be exported!
export { TenantDatabase };

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Your worker code
  },
};
```

```jsonc
// 2. Verify wrangler.jsonc has correct DO configuration
{
  "durable_objects": {
    "bindings": [
      {
        "name": "DOSQL_DB",             // Must match env.DOSQL_DB
        "class_name": "TenantDatabase"  // Must match exported class name exactly
      }
    ]
  },

  // 3. CRITICAL: Add migration tag for new DO classes
  "migrations": [
    {
      "tag": "v1",
      "new_classes": ["TenantDatabase"]  // Must match class_name
    }
  ]
}
```

```bash
# 4. Verify the deployment succeeded
wrangler deploy --dry-run

# 5. Check if the DO class is in the deployed worker
wrangler deployments list

# 6. Redeploy if necessary
wrangler deploy
```

**Debugging Checklist:**

| Check | Command/Location |
|-------|------------------|
| Class exported? | `grep "export.*TenantDatabase" src/index.ts` |
| Migration tag exists? | Check `wrangler.jsonc` `migrations` array |
| Class names match? | Compare `class_name` in bindings vs export |
| Binding name correct? | Compare binding `name` vs `env.NAME` in code |
| Deployment succeeded? | `wrangler deployments list` |

**Prevention Tips:**
- Always test with `wrangler deploy --dry-run` before deploying
- Use TypeScript to get compile-time errors for missing exports
- Create a deployment checklist that includes DO verification
- Set up monitoring to detect DO failures in production

---

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

### WebSocket Disconnects

**Problem:** WebSocket connections are dropping unexpectedly.

**Error Messages:**
- `WebSocket closed unexpectedly`
- `Connection reset by peer`
- `WebSocket error: code 1006`
- `Ping timeout`

**Root Causes:**
1. DO hibernation after inactivity (hibernates after ~10 seconds of no requests)
2. Network instability or NAT timeout
3. Cloudflare edge timeout (100 seconds for WebSocket idle)
4. Client-side network changes (mobile, WiFi switching)
5. Server-side error causing connection close

**Solution Steps:**

```typescript
// 1. Implement heartbeat/keepalive to prevent hibernation
const HEARTBEAT_INTERVAL = 30000; // 30 seconds

class WebSocketClientWithHeartbeat {
  private ws: WebSocket;
  private heartbeatTimer: Timer | null = null;
  private pongReceived = true;

  connect(url: string) {
    this.ws = new WebSocket(url);

    this.ws.onopen = () => {
      this.startHeartbeat();
    };

    this.ws.onmessage = (event) => {
      if (event.data === 'pong') {
        this.pongReceived = true;
        return;
      }
      // Handle normal messages
    };

    this.ws.onclose = (event) => {
      this.stopHeartbeat();
      console.log(`WebSocket closed: code=${event.code}, reason=${event.reason}`);

      // Auto-reconnect for unexpected closures
      if (event.code !== 1000) {
        setTimeout(() => this.connect(url), 1000);
      }
    };
  }

  private startHeartbeat() {
    this.heartbeatTimer = setInterval(() => {
      if (!this.pongReceived) {
        console.warn('No pong received, connection may be stale');
        this.ws.close(4000, 'Heartbeat timeout');
        return;
      }
      this.pongReceived = false;
      this.ws.send('ping');
    }, HEARTBEAT_INTERVAL);
  }

  private stopHeartbeat() {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }
}

// 2. Handle hibernation in the DO
export class TenantDatabase implements DurableObject {
  private connections = new Set<WebSocket>();

  async webSocketMessage(ws: WebSocket, message: string) {
    if (message === 'ping') {
      ws.send('pong');
      return;
    }
    // Handle other messages
  }

  async webSocketClose(ws: WebSocket, code: number, reason: string) {
    this.connections.delete(ws);
    console.log(`Client disconnected: code=${code}, reason=${reason}`);
  }

  async webSocketError(ws: WebSocket, error: unknown) {
    console.error('WebSocket error:', error);
    this.connections.delete(ws);
  }
}

// 3. Implement reconnection with state recovery
class ResilientClient {
  private lastLSN: bigint = 0n;

  async reconnect() {
    const client = await createWebSocketClient({
      url: this.url,
      autoReconnect: true,
      maxReconnectAttempts: 10,
      reconnectDelayMs: 1000,
    });

    // Resume from last known position
    if (this.lastLSN > 0n) {
      await client.send({
        type: 'resume',
        fromLSN: this.lastLSN,
      });
    }

    return client;
  }
}
```

**WebSocket Close Codes Reference:**

| Code | Meaning | Action |
|------|---------|--------|
| 1000 | Normal closure | No action needed |
| 1001 | Going away | Reconnect |
| 1006 | Abnormal closure | Reconnect with backoff |
| 1011 | Server error | Reconnect with backoff, check logs |
| 1012 | Service restart | Reconnect immediately |
| 1013 | Try again later | Reconnect with delay |
| 4000+ | Application-specific | Check error message |

**Prevention Tips:**
- Implement heartbeat with 30-second intervals (under Cloudflare's 100s timeout)
- Use the WebSocket Hibernation API for efficient connection management
- Handle all WebSocket events (open, message, close, error)
- Log disconnect reasons for debugging
- Implement exponential backoff for reconnections

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

### Timeout Configuration

**Problem:** Requests are timing out or taking too long.

**Error Messages:**
- `Request timeout`
- `TIMEOUT` RPC error
- `Operation timed out after Xms`
- `Durable Object exceeded CPU time limit`

**Root Causes:**
1. Query execution time exceeds timeout
2. Cold start time not accounted for
3. Network latency between client and edge
4. DO CPU limit exceeded (30 seconds per request)

**Solution Steps:**

```typescript
// 1. Configure appropriate client timeouts
const client = await createWebSocketClient({
  url: 'wss://your-worker.workers.dev/db',

  // Connection timeout (for initial connect)
  connectTimeoutMs: 10000,  // 10 seconds for cold starts

  // Request timeout (for individual queries)
  requestTimeoutMs: 30000,  // 30 seconds max (DO CPU limit)

  // Idle timeout (close connection after inactivity)
  idleTimeoutMs: 60000,     // 60 seconds
});

// 2. Set query-level timeouts
const result = await client.query({
  sql: 'SELECT * FROM large_table WHERE complex_condition',
  params: [],
  timeoutMs: 5000,  // This query should complete in 5 seconds
});

// 3. Use streaming for long-running operations
const stream = client.queryStream({
  sql: 'SELECT * FROM very_large_table',
  chunkSize: 1000,
  // Stream doesn't have overall timeout, uses chunk-level timeouts
  chunkTimeoutMs: 5000,
});

for await (const chunk of stream) {
  await processChunk(chunk);
}

// 4. Configure transaction timeouts
await db.transaction(async (tx) => {
  await tx.run('UPDATE accounts SET balance = balance - 100 WHERE id = 1');
  await tx.run('UPDATE accounts SET balance = balance + 100 WHERE id = 2');
}, {
  timeoutMs: 10000,  // Transaction must complete in 10 seconds
  lockTimeoutMs: 5000,  // Lock acquisition timeout
});
```

**Timeout Guidelines:**

| Operation | Recommended Timeout | Maximum |
|-----------|---------------------|---------|
| Connection (cold) | 10-15 seconds | 30 seconds |
| Connection (warm) | 2-5 seconds | 10 seconds |
| Simple query | 1-5 seconds | 10 seconds |
| Complex query | 5-15 seconds | 30 seconds |
| Transaction | 5-10 seconds | 30 seconds |
| Batch operation | 10-20 seconds | 30 seconds |

**Prevention Tips:**
- Account for DO cold start time (~50-200ms) in connection timeouts
- Set query timeouts based on expected execution time
- Use streaming for operations that may exceed timeouts
- Monitor timeout rates and adjust accordingly

---

### Retry Strategies

**Problem:** Transient failures are causing request failures.

**Solution:** Implement robust retry strategies with exponential backoff.

```typescript
// 1. Generic retry with exponential backoff
async function retryWithBackoff<T>(
  fn: () => Promise<T>,
  options: {
    maxRetries?: number;
    initialDelayMs?: number;
    maxDelayMs?: number;
    retryableErrors?: string[];
  } = {}
): Promise<T> {
  const {
    maxRetries = 3,
    initialDelayMs = 1000,
    maxDelayMs = 30000,
    retryableErrors = ['TIMEOUT', 'R2_RATE_LIMITED', 'TXN_DEADLOCK', 'CONNECTION_ERROR'],
  } = options;

  let lastError: Error;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error as Error;

      // Check if error is retryable
      const errorCode = (error as any).code;
      if (!retryableErrors.includes(errorCode)) {
        throw error;
      }

      if (attempt === maxRetries) {
        throw error;
      }

      // Calculate delay with jitter
      const baseDelay = initialDelayMs * Math.pow(2, attempt);
      const jitter = Math.random() * 0.3 * baseDelay;
      const delay = Math.min(baseDelay + jitter, maxDelayMs);

      console.warn(
        `Attempt ${attempt + 1} failed with ${errorCode}, retrying in ${delay}ms`
      );

      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }

  throw lastError!;
}

// 2. Retry-aware query execution
async function executeQueryWithRetry(
  client: DoSQLClient,
  sql: string,
  params?: unknown[]
) {
  return retryWithBackoff(
    () => client.query({ sql, params }),
    {
      maxRetries: 3,
      retryableErrors: ['TIMEOUT', 'CONNECTION_ERROR', 'R2_TEMPORARY_ERROR'],
    }
  );
}

// 3. Circuit breaker pattern for persistent failures
class CircuitBreaker {
  private failures = 0;
  private lastFailure = 0;
  private state: 'closed' | 'open' | 'half-open' = 'closed';

  constructor(
    private threshold: number = 5,
    private resetTimeMs: number = 30000
  ) {}

  async execute<T>(fn: () => Promise<T>): Promise<T> {
    if (this.state === 'open') {
      if (Date.now() - this.lastFailure > this.resetTimeMs) {
        this.state = 'half-open';
      } else {
        throw new Error('Circuit breaker is open');
      }
    }

    try {
      const result = await fn();
      this.onSuccess();
      return result;
    } catch (error) {
      this.onFailure();
      throw error;
    }
  }

  private onSuccess() {
    this.failures = 0;
    this.state = 'closed';
  }

  private onFailure() {
    this.failures++;
    this.lastFailure = Date.now();
    if (this.failures >= this.threshold) {
      this.state = 'open';
      console.error('Circuit breaker opened');
    }
  }
}

// Usage
const breaker = new CircuitBreaker(5, 30000);
const result = await breaker.execute(() => client.query({ sql: 'SELECT 1' }));
```

**Retry Strategy by Error Type:**

| Error | Retry? | Strategy |
|-------|--------|----------|
| `TIMEOUT` | Yes | Exponential backoff |
| `R2_RATE_LIMITED` | Yes | Use `retryAfter` if provided |
| `TXN_DEADLOCK` | Yes | Random delay to break tie |
| `CONNECTION_ERROR` | Yes | Reconnect then retry |
| `SYNTAX_ERROR` | No | Fix the query |
| `UNAUTHORIZED` | No* | Refresh token then retry |
| `NOT_FOUND` | No | Data doesn't exist |

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

### Transaction Timeout

**Problem:** Transactions are timing out before completion.

**Error Messages:**
- `TransactionError: TXN_TIMEOUT`
- `Transaction exceeded timeout of Xms`
- `Lock acquisition timeout`
- `TXN_LOCK_TIMEOUT`

**Root Causes:**
1. Transaction scope too large (too many operations)
2. External calls within transaction (network latency)
3. Lock contention with other transactions
4. Slow queries within transaction
5. Timeout configured too low for workload

**Solution Steps:**

```typescript
// 1. Configure transaction timeout
await db.transaction(async (tx) => {
  await tx.run('UPDATE accounts SET balance = balance - ? WHERE id = ?', [100, 1]);
  await tx.run('UPDATE accounts SET balance = balance + ? WHERE id = ?', [100, 2]);
}, {
  timeoutMs: 15000,        // Overall transaction timeout
  lockTimeoutMs: 5000,     // How long to wait for locks
  mode: 'IMMEDIATE',       // Acquire write lock upfront
});

// 2. WRONG: External calls inside transaction
await db.transaction(async (tx) => {
  const user = await tx.get('SELECT * FROM users WHERE id = ?', [userId]);
  const externalData = await fetchExternalAPI(user.email);  // DON'T DO THIS!
  await tx.run('UPDATE users SET data = ? WHERE id = ?', [externalData, userId]);
});

// 3. CORRECT: External calls outside transaction
const user = await db.get('SELECT * FROM users WHERE id = ?', [userId]);
const externalData = await fetchExternalAPI(user.email);  // OK: outside tx
await db.transaction(async (tx) => {
  await tx.run('UPDATE users SET data = ? WHERE id = ?', [externalData, userId]);
});

// 4. Break large transactions into smaller ones
// WRONG: One huge transaction
await db.transaction(async (tx) => {
  for (const item of thousandItems) {
    await tx.run('INSERT INTO items VALUES (?)', [item]);
  }
});

// CORRECT: Batch in smaller transactions
const BATCH_SIZE = 100;
for (let i = 0; i < thousandItems.length; i += BATCH_SIZE) {
  const batch = thousandItems.slice(i, i + BATCH_SIZE);
  await db.transaction(async (tx) => {
    for (const item of batch) {
      await tx.run('INSERT INTO items VALUES (?)', [item]);
    }
  });
}

// 5. Use SAVEPOINT for partial rollback
await db.transaction(async (tx) => {
  await tx.run('INSERT INTO orders VALUES (?)', [order]);

  await tx.run('SAVEPOINT items_insert');
  try {
    for (const item of order.items) {
      await tx.run('INSERT INTO order_items VALUES (?, ?)', [order.id, item]);
    }
  } catch (error) {
    // Rollback just the items, keep the order
    await tx.run('ROLLBACK TO items_insert');
    console.warn('Items insert failed, order created without items');
  }
});

// 6. Monitor transaction duration
const startTime = Date.now();
await db.transaction(async (tx) => {
  // ... transaction work ...
});
const duration = Date.now() - startTime;
if (duration > 5000) {
  console.warn(`Transaction took ${duration}ms, consider optimization`);
}
```

**Transaction Timeout Tuning:**

| Workload | Recommended Timeout | Lock Timeout |
|----------|---------------------|--------------|
| Simple CRUD | 5 seconds | 2 seconds |
| Batch insert (100 rows) | 10 seconds | 5 seconds |
| Complex multi-table | 15 seconds | 5 seconds |
| Data migration | 30 seconds | 10 seconds |

**Prevention Tips:**
- Keep transactions focused and minimal
- Never make external/network calls inside transactions
- Use appropriate transaction mode (DEFERRED, IMMEDIATE, EXCLUSIVE)
- Monitor transaction duration and alert on slow transactions
- Consider using batch operations instead of transaction loops

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

### Query Analysis

**Problem:** Need to understand why queries are slow.

**Solution:** Use systematic query analysis techniques.

```typescript
// 1. EXPLAIN QUERY PLAN - Understanding query execution
const plan = await db.query(`
  EXPLAIN QUERY PLAN
  SELECT o.*, u.name
  FROM orders o
  JOIN users u ON o.user_id = u.id
  WHERE o.status = 'pending'
  ORDER BY o.created_at DESC
  LIMIT 100
`);
console.log(plan);

// Good output (uses indexes):
// SEARCH orders USING INDEX idx_orders_status (status=?)
// SEARCH users USING INTEGER PRIMARY KEY (rowid=?)

// Bad output (full table scan):
// SCAN orders
// SEARCH users USING INTEGER PRIMARY KEY (rowid=?)

// 2. Profile query execution time
async function profileQuery(sql: string, params?: unknown[]) {
  const start = performance.now();
  const result = await db.query(sql, params);
  const duration = performance.now() - start;

  console.log({
    sql: sql.substring(0, 100),
    rowCount: result.length,
    durationMs: duration.toFixed(2),
    rowsPerMs: (result.length / duration).toFixed(2),
  });

  return result;
}

// 3. Analyze index usage
const indexStats = await db.query(`
  SELECT
    name,
    tbl_name,
    sql
  FROM sqlite_master
  WHERE type = 'index'
  ORDER BY tbl_name
`);
console.log('Indexes:', indexStats);

// 4. Check table statistics
const tableStats = await db.query(`
  SELECT
    name,
    (SELECT COUNT(*) FROM pragma_table_info(name)) as columns,
    (SELECT COUNT(*) FROM pragma_index_list(name)) as indexes
  FROM sqlite_master
  WHERE type = 'table'
  AND name NOT LIKE 'sqlite_%'
`);

// 5. Identify missing indexes with slow query patterns
const SLOW_QUERY_THRESHOLD_MS = 100;

function wrapQueryWithProfiling(db: Database): Database {
  const originalQuery = db.query.bind(db);

  db.query = async function(sql: string, params?: unknown[]) {
    const start = performance.now();
    const result = await originalQuery(sql, params);
    const duration = performance.now() - start;

    if (duration > SLOW_QUERY_THRESHOLD_MS) {
      console.warn(`SLOW QUERY (${duration.toFixed(0)}ms): ${sql}`);

      // Auto-suggest index
      const match = sql.match(/WHERE\s+(\w+)\s*=/i);
      if (match) {
        console.warn(`Consider: CREATE INDEX idx_${match[1]} ON table(${match[1]})`);
      }
    }

    return result;
  };

  return db;
}
```

**Query Analysis Checklist:**

| Check | Command | What to Look For |
|-------|---------|------------------|
| Execution plan | `EXPLAIN QUERY PLAN ...` | SCAN = bad, SEARCH = good |
| Index usage | `PRAGMA index_list(table)` | Missing indexes on WHERE columns |
| Table size | `SELECT COUNT(*) FROM table` | Large tables need indexes |
| Column types | `PRAGMA table_info(table)` | Type mismatches |

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

### High Memory Usage Optimization

**Problem:** DO is using too much memory and approaching limits.

**Solution:** Implement memory optimization strategies.

```typescript
// 1. Monitor memory usage
class MemoryAwareDO implements DurableObject {
  private checkMemoryUsage() {
    // V8 memory info (approximate)
    if (typeof performance !== 'undefined' && 'memory' in performance) {
      const memory = (performance as any).memory;
      const usedMB = memory.usedJSHeapSize / 1024 / 1024;
      const limitMB = 128; // DO limit

      if (usedMB > limitMB * 0.8) {
        console.warn(`Memory warning: ${usedMB.toFixed(1)}MB / ${limitMB}MB`);
        this.reduceMemoryUsage();
      }
    }
  }

  private async reduceMemoryUsage() {
    // Clear caches
    await this.db.exec('PRAGMA shrink_memory');

    // Force garbage collection hints
    this.queryCache.clear();
    this.preparedStatements.clear();
  }
}

// 2. Limit in-memory data structures
class BoundedCache<K, V> {
  private cache = new Map<K, V>();
  private order: K[] = [];

  constructor(private maxSize: number) {}

  set(key: K, value: V) {
    if (this.cache.has(key)) {
      this.cache.set(key, value);
      return;
    }

    if (this.cache.size >= this.maxSize) {
      const oldest = this.order.shift()!;
      this.cache.delete(oldest);
    }

    this.cache.set(key, value);
    this.order.push(key);
  }

  get(key: K): V | undefined {
    return this.cache.get(key);
  }
}

// 3. Use WeakRef for large objects
class LargeObjectCache {
  private refs = new Map<string, WeakRef<object>>();
  private finalizationRegistry = new FinalizationRegistry((key: string) => {
    this.refs.delete(key);
  });

  set(key: string, value: object) {
    const ref = new WeakRef(value);
    this.refs.set(key, ref);
    this.finalizationRegistry.register(value, key);
  }

  get(key: string): object | undefined {
    const ref = this.refs.get(key);
    return ref?.deref();
  }
}

// 4. Stream processing instead of loading all data
async function processLargeDataset(db: Database) {
  // BAD: Loads everything into memory
  // const all = await db.query('SELECT * FROM big_table');
  // all.forEach(process);

  // GOOD: Process in chunks
  const CHUNK_SIZE = 1000;
  let offset = 0;

  while (true) {
    const chunk = await db.query(
      'SELECT * FROM big_table LIMIT ? OFFSET ?',
      [CHUNK_SIZE, offset]
    );

    if (chunk.length === 0) break;

    for (const row of chunk) {
      await processRow(row);
    }

    offset += CHUNK_SIZE;

    // Allow GC between chunks
    await new Promise(r => setTimeout(r, 0));
  }
}

// 5. Offload cold data to R2
async function offloadToR2(db: Database, r2: R2Bucket) {
  const coldData = await db.query(`
    SELECT id, data FROM archive
    WHERE accessed_at < datetime('now', '-30 days')
    LIMIT 1000
  `);

  for (const row of coldData) {
    // Move to R2
    await r2.put(`archive/${row.id}`, JSON.stringify(row.data));

    // Remove from SQLite
    await db.run('DELETE FROM archive WHERE id = ?', [row.id]);
  }

  // Reclaim space
  await db.exec('VACUUM');
}
```

**Memory Budget Guidelines:**

| Component | Recommended | Maximum |
|-----------|-------------|---------|
| SQLite database file | 50 MB | 100 MB |
| B-tree page cache | 10 MB | 30 MB |
| Query result buffers | 5 MB | 20 MB |
| Application state | 10 MB | 30 MB |
| Overhead | 20 MB | 30 MB |
| **Total** | **~95 MB** | **128 MB** |

---

### Cold Start Latency

**Problem:** First request to a DO takes significantly longer than subsequent requests.

**Error Messages:**
- Request timeout on first request
- High latency spikes in monitoring
- `Connection timeout` on initial connect

**Root Causes:**
1. DO needs to be instantiated (JavaScript code loading)
2. Database needs to be opened and loaded
3. Indexes need to be loaded into memory
4. No prewarming of frequently accessed DOs

**Solution Steps:**

```typescript
// 1. Optimize DO initialization
export class TenantDatabase implements DurableObject {
  private db: Database | null = null;
  private initPromise: Promise<void> | null = null;

  constructor(state: DurableObjectState, env: Env) {
    this.state = state;
    this.env = env;

    // Start initialization immediately (non-blocking)
    this.initPromise = this.initialize();
  }

  private async initialize() {
    // Lightweight initialization only
    // Defer heavy operations
  }

  async fetch(request: Request): Promise<Response> {
    // Wait for init to complete
    await this.initPromise;

    // Lazy load database on first query
    if (!this.db) {
      this.db = await this.openDatabase();
    }

    return this.handleRequest(request);
  }

  private async openDatabase(): Promise<Database> {
    const db = await DB('tenant', {
      btreeConfig: {
        // Smaller cache = faster initial load
        maxCachedPages: 50,
      },
    });

    // Warm critical indexes in background
    this.warmIndexes(db);

    return db;
  }

  private async warmIndexes(db: Database) {
    // Don't await - run in background
    Promise.all([
      db.query('SELECT 1 FROM users LIMIT 1'),
      db.query('SELECT 1 FROM orders LIMIT 1'),
    ]).catch(() => {});
  }
}

// 2. Implement prewarm endpoint
export class TenantDatabase implements DurableObject {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === '/prewarm') {
      await this.ensureInitialized();
      return new Response('OK', { status: 200 });
    }

    // ... rest of handler
  }
}

// 3. Prewarm DOs on deployment or schedule
async function prewarmDOs(env: Env, tenantIds: string[]) {
  const warmups = tenantIds.map(async (tenantId) => {
    const id = env.DOSQL_DB.idFromName(tenantId);
    const stub = env.DOSQL_DB.get(id);

    try {
      await stub.fetch(new Request('https://internal/prewarm'));
    } catch {
      // Ignore failures
    }
  });

  await Promise.allSettled(warmups);
}

// 4. Keep DOs warm with periodic pings (use sparingly)
// In a Cron trigger
export default {
  async scheduled(event: ScheduledEvent, env: Env) {
    if (event.cron === '*/5 * * * *') { // Every 5 minutes
      const activetenants = await getActiveTenants(env);
      await prewarmDOs(env, activeTenants);
    }
  },
};

// 5. Use hibernation efficiently
export class TenantDatabase implements DurableObject {
  // WebSocket Hibernation API - DO stays warm while connections exist
  async fetch(request: Request): Promise<Response> {
    if (request.headers.get('Upgrade') === 'websocket') {
      const pair = new WebSocketPair();
      this.state.acceptWebSocket(pair[1]);
      return new Response(null, { status: 101, webSocket: pair[0] });
    }
    // ...
  }
}
```

**Cold Start Latency Breakdown:**

| Phase | Typical Duration | Optimization |
|-------|------------------|--------------|
| DO instantiation | 20-50ms | Minimize constructor work |
| Database open | 10-30ms | Lazy loading |
| Index loading | 5-20ms | Smaller indexes |
| First query | 5-10ms | Query warm-up |
| **Total** | **40-110ms** | Target: <100ms |

**Prevention Tips:**
- Keep DO constructor lightweight
- Lazy load database and heavy resources
- Use WebSocket Hibernation to keep DOs warm
- Implement prewarm endpoints for critical tenants
- Monitor cold start rates and optimize hot paths
- Set client timeouts to account for cold starts

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

## Debugging Techniques

### Enable Debug Logging

**Problem:** Need detailed information about DoSQL operations for debugging.

**Solution:** Enable comprehensive logging at various levels.

```typescript
// 1. Enable debug logging in DB initialization
import { DB, ConsoleLogger, LogLevel } from '@dotdo/dosql';

const db = await DB('tenant', {
  logger: new ConsoleLogger({
    level: LogLevel.DEBUG,  // DEBUG, INFO, WARN, ERROR
    includeTimestamp: true,
    includeContext: true,
  }),
});

// 2. Custom logger implementation
class CustomLogger implements Logger {
  private context: Record<string, string> = {};

  setContext(key: string, value: string) {
    this.context[key] = value;
  }

  debug(message: string, data?: object) {
    console.debug(JSON.stringify({
      level: 'debug',
      timestamp: new Date().toISOString(),
      message,
      ...this.context,
      ...data,
    }));
  }

  info(message: string, data?: object) {
    console.info(JSON.stringify({
      level: 'info',
      timestamp: new Date().toISOString(),
      message,
      ...this.context,
      ...data,
    }));
  }

  warn(message: string, data?: object) {
    console.warn(JSON.stringify({
      level: 'warn',
      timestamp: new Date().toISOString(),
      message,
      ...this.context,
      ...data,
    }));
  }

  error(message: string, error?: Error, data?: object) {
    console.error(JSON.stringify({
      level: 'error',
      timestamp: new Date().toISOString(),
      message,
      error: error ? {
        name: error.name,
        message: error.message,
        stack: error.stack,
      } : undefined,
      ...this.context,
      ...data,
    }));
  }
}

// 3. Request-scoped logging
export class TenantDatabase implements DurableObject {
  async fetch(request: Request): Promise<Response> {
    const requestId = crypto.randomUUID();
    const logger = new CustomLogger();
    logger.setContext('requestId', requestId);
    logger.setContext('tenantId', this.tenantId);

    logger.info('Request received', {
      method: request.method,
      url: request.url,
    });

    try {
      const result = await this.handleRequest(request, logger);
      logger.info('Request completed', { status: 200 });
      return result;
    } catch (error) {
      logger.error('Request failed', error as Error);
      throw error;
    }
  }
}

// 4. Query logging with timing
const db = await DB('tenant', {
  queryLogger: {
    onQueryStart(sql: string, params?: unknown[]) {
      console.debug(`[QUERY START] ${sql}`, { params });
      return { startTime: performance.now() };
    },
    onQueryEnd(context: { startTime: number }, result: QueryResult) {
      const duration = performance.now() - context.startTime;
      console.debug(`[QUERY END] ${duration.toFixed(2)}ms, ${result.rowCount} rows`);
    },
    onQueryError(context: { startTime: number }, error: Error) {
      const duration = performance.now() - context.startTime;
      console.error(`[QUERY ERROR] ${duration.toFixed(2)}ms`, error);
    },
  },
});

// 5. Environment-based logging
const LOG_LEVEL = env.LOG_LEVEL || (env.ENVIRONMENT === 'production' ? 'warn' : 'debug');

const db = await DB('tenant', {
  logger: new ConsoleLogger({ level: LOG_LEVEL as LogLevel }),
});
```

**Log Levels:**

| Level | When to Use | Production |
|-------|-------------|------------|
| `DEBUG` | Development, detailed tracing | Off |
| `INFO` | Important events, metrics | Optional |
| `WARN` | Recoverable issues, deprecations | On |
| `ERROR` | Failures, exceptions | On |

---

### Inspect DO State

**Problem:** Need to examine the internal state of a Durable Object.

**Solution:** Implement debug endpoints and state inspection.

```typescript
// 1. Add debug endpoint to DO
export class TenantDatabase implements DurableObject {
  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);

    // Only allow in non-production or with auth
    if (url.pathname === '/__debug/state' && this.allowDebug(request)) {
      return this.getDebugState();
    }

    // ... rest of handler
  }

  private allowDebug(request: Request): boolean {
    // Check for debug token or non-production environment
    const debugToken = request.headers.get('X-Debug-Token');
    return debugToken === this.env.DEBUG_TOKEN || this.env.ENVIRONMENT !== 'production';
  }

  private async getDebugState(): Promise<Response> {
    const state = {
      tenantId: this.tenantId,
      initialized: this.initialized,
      connectionCount: this.connections.size,
      databaseStats: await this.getDatabaseStats(),
      memoryUsage: this.getMemoryUsage(),
      uptime: Date.now() - this.startTime,
    };

    return new Response(JSON.stringify(state, null, 2), {
      headers: { 'Content-Type': 'application/json' },
    });
  }

  private async getDatabaseStats() {
    if (!this.db) return null;

    const [pageCount, pageSize, walSize, tables] = await Promise.all([
      this.db.queryOne('PRAGMA page_count'),
      this.db.queryOne('PRAGMA page_size'),
      this.db.queryOne('PRAGMA wal_checkpoint'),
      this.db.query(`
        SELECT name, (SELECT COUNT(*) FROM pragma_table_info(name)) as columns
        FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'
      `),
    ]);

    return {
      sizeBytes: pageCount.page_count * pageSize.page_size,
      pageCount: pageCount.page_count,
      pageSize: pageSize.page_size,
      walSize: walSize,
      tables: tables,
    };
  }

  private getMemoryUsage() {
    if (typeof performance !== 'undefined' && 'memory' in performance) {
      const memory = (performance as any).memory;
      return {
        usedHeapMB: (memory.usedJSHeapSize / 1024 / 1024).toFixed(2),
        totalHeapMB: (memory.totalJSHeapSize / 1024 / 1024).toFixed(2),
      };
    }
    return null;
  }
}

// 2. List all stored keys (use carefully)
async function listStorageKeys(state: DurableObjectState): Promise<string[]> {
  const keys: string[] = [];
  const stored = await state.storage.list({ limit: 1000 });
  for (const [key] of stored) {
    keys.push(key);
  }
  return keys;
}

// 3. Dump DO state for debugging
async function dumpState(state: DurableObjectState): Promise<object> {
  const dump: Record<string, unknown> = {};
  const stored = await state.storage.list({ limit: 100 });

  for (const [key, value] of stored) {
    // Redact sensitive data
    if (key.includes('password') || key.includes('secret')) {
      dump[key] = '[REDACTED]';
    } else {
      dump[key] = value;
    }
  }

  return dump;
}

// 4. Query from outside using wrangler (local dev)
// In another terminal:
// curl http://localhost:8787/__debug/state -H "X-Debug-Token: your-token"
```

---

### Trace Request Flow

**Problem:** Need to understand how a request flows through the system.

**Solution:** Implement distributed tracing.

```typescript
// 1. Add trace context to requests
interface TraceContext {
  traceId: string;
  spanId: string;
  parentSpanId?: string;
  startTime: number;
}

function createTraceContext(request: Request): TraceContext {
  return {
    traceId: request.headers.get('X-Trace-ID') || crypto.randomUUID(),
    spanId: crypto.randomUUID().substring(0, 16),
    parentSpanId: request.headers.get('X-Span-ID') || undefined,
    startTime: performance.now(),
  };
}

// 2. Propagate trace context
function propagateTrace(trace: TraceContext, request: Request): Request {
  const headers = new Headers(request.headers);
  headers.set('X-Trace-ID', trace.traceId);
  headers.set('X-Span-ID', trace.spanId);
  return new Request(request.url, { ...request, headers });
}

// 3. Log with trace context
function logWithTrace(trace: TraceContext, event: string, data?: object) {
  console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    traceId: trace.traceId,
    spanId: trace.spanId,
    parentSpanId: trace.parentSpanId,
    event,
    durationMs: performance.now() - trace.startTime,
    ...data,
  }));
}

// 4. Full request tracing example
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const trace = createTraceContext(request);

    logWithTrace(trace, 'worker.request.start', {
      method: request.method,
      url: request.url,
    });

    try {
      // Route to DO
      const tenantId = getTenantId(request);
      const id = env.DOSQL_DB.idFromName(tenantId);
      const stub = env.DOSQL_DB.get(id);

      logWithTrace(trace, 'worker.do.call', { tenantId });

      const doRequest = propagateTrace(trace, request);
      const response = await stub.fetch(doRequest);

      logWithTrace(trace, 'worker.request.end', {
        status: response.status,
      });

      // Add trace ID to response for debugging
      const headers = new Headers(response.headers);
      headers.set('X-Trace-ID', trace.traceId);
      return new Response(response.body, { ...response, headers });

    } catch (error) {
      logWithTrace(trace, 'worker.request.error', {
        error: (error as Error).message,
      });
      throw error;
    }
  },
};

// 5. Trace database operations
class TracedDatabase {
  constructor(private db: Database, private trace: TraceContext) {}

  async query(sql: string, params?: unknown[]) {
    const spanId = crypto.randomUUID().substring(0, 16);
    const start = performance.now();

    logWithTrace(this.trace, 'db.query.start', {
      spanId,
      sql: sql.substring(0, 100),
    });

    try {
      const result = await this.db.query(sql, params);
      logWithTrace(this.trace, 'db.query.end', {
        spanId,
        durationMs: performance.now() - start,
        rowCount: result.length,
      });
      return result;
    } catch (error) {
      logWithTrace(this.trace, 'db.query.error', {
        spanId,
        durationMs: performance.now() - start,
        error: (error as Error).message,
      });
      throw error;
    }
  }
}
```

---

### Use Wrangler Tail

**Problem:** Need to view real-time logs from deployed Workers.

**Solution:** Use `wrangler tail` and related debugging tools.

```bash
# 1. Basic log tailing
wrangler tail

# 2. Tail with filters
wrangler tail --format pretty  # Formatted output
wrangler tail --status error   # Only errors
wrangler tail --search "tenant-123"  # Filter by content
wrangler tail --ip 203.0.113.0  # Filter by IP

# 3. Tail specific environment
wrangler tail --env production
wrangler tail --env staging

# 4. JSON output for processing
wrangler tail --format json | jq '.logs[]'

# 5. Save logs to file
wrangler tail --format json > logs.json

# 6. Filter by sampling rate (high traffic)
wrangler tail --sampling-rate 0.1  # 10% of requests
```

```typescript
// 7. Make logs more useful with structured logging
export class TenantDatabase implements DurableObject {
  async fetch(request: Request): Promise<Response> {
    const requestId = crypto.randomUUID();

    // These will appear in wrangler tail
    console.log(JSON.stringify({
      type: 'request',
      requestId,
      method: request.method,
      path: new URL(request.url).pathname,
      tenantId: this.tenantId,
    }));

    try {
      const result = await this.handleRequest(request);

      console.log(JSON.stringify({
        type: 'response',
        requestId,
        status: 200,
        durationMs: Date.now() - startTime,
      }));

      return result;
    } catch (error) {
      console.error(JSON.stringify({
        type: 'error',
        requestId,
        error: (error as Error).message,
        stack: (error as Error).stack,
      }));
      throw error;
    }
  }
}
```

**Wrangler Tail Output:**

```
[2024-01-15 10:23:45] GET /api/query 200 OK (45ms)
  {
    "type": "request",
    "requestId": "abc-123",
    "method": "POST",
    "path": "/api/query",
    "tenantId": "tenant-456"
  }
  {
    "type": "response",
    "requestId": "abc-123",
    "status": 200,
    "durationMs": 42
  }
```

---

### Network Debugging

**Problem:** Network-related issues between client and Worker.

**Solution:** Debug network connectivity and latency.

```typescript
// 1. Add timing headers to responses
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const start = Date.now();

    const response = await handleRequest(request, env);

    const headers = new Headers(response.headers);
    headers.set('X-Response-Time', `${Date.now() - start}ms`);
    headers.set('X-CF-Ray', request.headers.get('CF-Ray') || 'unknown');

    return new Response(response.body, { ...response, headers });
  },
};

// 2. Health check endpoint
async function handleHealthCheck(env: Env): Promise<Response> {
  const checks = {
    worker: 'ok',
    database: 'unknown',
    r2: 'unknown',
  };

  // Check database
  try {
    const id = env.DOSQL_DB.idFromName('health-check');
    const stub = env.DOSQL_DB.get(id);
    await stub.fetch(new Request('https://internal/ping'));
    checks.database = 'ok';
  } catch {
    checks.database = 'error';
  }

  // Check R2
  try {
    await env.DATA_BUCKET.head('health-check');
    checks.r2 = 'ok';
  } catch (error) {
    if ((error as any).message.includes('not found')) {
      checks.r2 = 'ok';  // 404 is fine for health check
    } else {
      checks.r2 = 'error';
    }
  }

  const healthy = Object.values(checks).every(v => v === 'ok');

  return new Response(JSON.stringify(checks), {
    status: healthy ? 200 : 503,
    headers: { 'Content-Type': 'application/json' },
  });
}

// 3. Debug WebSocket connectivity
class WebSocketDebugger {
  async testConnection(url: string): Promise<ConnectionTestResult> {
    const results: ConnectionTestResult = {
      dnsResolution: null,
      tcpConnect: null,
      tlsHandshake: null,
      wsUpgrade: null,
      firstMessage: null,
    };

    const start = performance.now();

    try {
      const ws = new WebSocket(url);

      await new Promise<void>((resolve, reject) => {
        ws.onopen = () => {
          results.wsUpgrade = performance.now() - start;
          ws.send('ping');
        };

        ws.onmessage = (event) => {
          if (results.firstMessage === null) {
            results.firstMessage = performance.now() - start;
          }
          if (event.data === 'pong') {
            ws.close(1000);
            resolve();
          }
        };

        ws.onerror = (error) => reject(error);

        setTimeout(() => reject(new Error('Timeout')), 10000);
      });

    } catch (error) {
      results.error = (error as Error).message;
    }

    return results;
  }
}

interface ConnectionTestResult {
  dnsResolution: number | null;
  tcpConnect: number | null;
  tlsHandshake: number | null;
  wsUpgrade: number | null;
  firstMessage: number | null;
  error?: string;
}
```

```bash
# 4. Debug with curl
# Test basic connectivity
curl -v https://your-worker.workers.dev/health

# Test with timing
curl -w "@curl-format.txt" -o /dev/null -s https://your-worker.workers.dev/api/query

# curl-format.txt contents:
#     time_namelookup:  %{time_namelookup}s\n
#        time_connect:  %{time_connect}s\n
#     time_appconnect:  %{time_appconnect}s\n
#    time_pretransfer:  %{time_pretransfer}s\n
#       time_redirect:  %{time_redirect}s\n
#  time_starttransfer:  %{time_starttransfer}s\n
#                     ----------\n
#          time_total:  %{time_total}s\n

# 5. Test WebSocket with wscat
npx wscat -c wss://your-worker.workers.dev/ws
```

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

### Migration Failed Debugging

**Problem:** Migration fails with unclear error.

**Error Messages:**
- `Migration failed: [migration_id]`
- `Error applying migration`
- `SQLITE_CONSTRAINT` during migration
- `Migration partially applied`

**Root Causes:**
1. SQL syntax error in migration file
2. Constraint violation (foreign key, unique, not null)
3. Table or column already exists
4. Timeout during large data migration
5. Insufficient disk space or memory

**Solution Steps:**

```typescript
// 1. Get detailed migration error information
const runner = createMigrationRunner(db, {
  logger: new ConsoleLogger({ level: LogLevel.DEBUG }),
});

try {
  await runner.migrate(migrations);
} catch (error) {
  if (error instanceof MigrationError) {
    console.error('Migration failed:', {
      migrationId: error.migrationId,
      sql: error.failedSql,
      sqliteError: error.cause?.message,
      line: error.line,
      position: error.position,
    });
  }
}

// 2. Dry-run to validate migrations before applying
const status = await runner.dryRun(migrations);
if (!status.valid) {
  console.error('Migration validation failed:');
  for (const error of status.errors) {
    console.error(`  ${error.migrationId}: ${error.message}`);
  }
  return; // Don't proceed
}

// 3. Apply migrations one at a time for debugging
for (const migration of migrations) {
  console.log(`Applying migration: ${migration.id}`);
  try {
    await runner.migrateOne(migration);
    console.log(`  Success`);
  } catch (error) {
    console.error(`  Failed:`, error);
    // Stop and investigate
    break;
  }
}

// 4. Check current migration state
const applied = await db.query(`
  SELECT id, applied_at, checksum, success
  FROM __dosql_migrations
  ORDER BY applied_at DESC
`);
console.log('Applied migrations:', applied);

// 5. Identify partially applied migration
const lastMigration = await db.queryOne(`
  SELECT * FROM __dosql_migrations
  ORDER BY applied_at DESC
  LIMIT 1
`);

if (!lastMigration.success) {
  console.error('Last migration was not successful:', lastMigration);
  // May need manual intervention
}

// 6. Test migration SQL directly
try {
  await db.exec(migrationSql);
} catch (error) {
  console.error('SQL error:', error);
  // Get more details
  if (error.message.includes('UNIQUE constraint')) {
    const duplicates = await db.query(`
      SELECT column_name, COUNT(*) as count
      FROM table_name
      GROUP BY column_name
      HAVING count > 1
    `);
    console.log('Duplicates:', duplicates);
  }
}
```

**Migration Debugging Checklist:**

| Check | How | Fix |
|-------|-----|-----|
| Syntax valid? | `dryRun()` | Fix SQL syntax |
| Already applied? | Check `__dosql_migrations` | Skip or add `IF NOT EXISTS` |
| Constraint violation? | Run SQL manually | Fix data first |
| Timeout? | Check migration size | Break into smaller chunks |
| Partial state? | Check `success` column | Manual cleanup |

---

### Migration Rollback

**Problem:** Need to undo a migration that caused issues.

**Solution:** Implement rollback procedures.

```typescript
// 1. Define migrations with rollback SQL
interface MigrationWithRollback {
  id: string;
  sql: string;
  downSql: string;  // Rollback SQL
  description?: string;
}

const migrations: MigrationWithRollback[] = [
  {
    id: '001_create_users',
    sql: `
      CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT UNIQUE
      );
      CREATE INDEX idx_users_email ON users(email);
    `,
    downSql: `
      DROP INDEX IF EXISTS idx_users_email;
      DROP TABLE IF EXISTS users;
    `,
  },
  {
    id: '002_add_user_status',
    sql: `
      ALTER TABLE users ADD COLUMN status TEXT DEFAULT 'active';
    `,
    downSql: `
      -- SQLite doesn't support DROP COLUMN directly
      -- Recreate table without the column
      CREATE TABLE users_backup AS SELECT id, name, email FROM users;
      DROP TABLE users;
      CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT UNIQUE
      );
      INSERT INTO users SELECT * FROM users_backup;
      DROP TABLE users_backup;
    `,
  },
];

// 2. Implement rollback function
async function rollbackMigration(
  db: Database,
  migrationId: string,
  migrations: MigrationWithRollback[]
): Promise<void> {
  const migration = migrations.find(m => m.id === migrationId);
  if (!migration) {
    throw new Error(`Migration not found: ${migrationId}`);
  }

  // Check if migration was applied
  const applied = await db.queryOne(
    'SELECT * FROM __dosql_migrations WHERE id = ?',
    [migrationId]
  );

  if (!applied) {
    console.log(`Migration ${migrationId} was not applied, skipping rollback`);
    return;
  }

  console.log(`Rolling back migration: ${migrationId}`);

  await db.transaction(async (tx) => {
    // Execute rollback SQL
    await tx.exec(migration.downSql);

    // Remove from migrations table
    await tx.run(
      'DELETE FROM __dosql_migrations WHERE id = ?',
      [migrationId]
    );
  });

  console.log(`Rollback complete: ${migrationId}`);
}

// 3. Rollback to a specific point
async function rollbackTo(
  db: Database,
  targetMigrationId: string,
  migrations: MigrationWithRollback[]
): Promise<void> {
  // Get applied migrations in reverse order
  const applied = await db.query(`
    SELECT id FROM __dosql_migrations
    ORDER BY applied_at DESC
  `);

  // Find migrations to rollback
  const targetIndex = applied.findIndex(m => m.id === targetMigrationId);
  if (targetIndex === -1) {
    throw new Error(`Target migration not found: ${targetMigrationId}`);
  }

  const toRollback = applied.slice(0, targetIndex);

  for (const { id } of toRollback) {
    await rollbackMigration(db, id, migrations);
  }
}

// 4. Emergency rollback without down SQL
async function emergencyRollback(db: Database, migrationId: string): Promise<void> {
  // WARNING: This only marks the migration as rolled back
  // You must manually fix the schema

  console.warn(`EMERGENCY ROLLBACK: ${migrationId}`);
  console.warn('This does NOT undo schema changes!');
  console.warn('Manual intervention required.');

  await db.run(
    'DELETE FROM __dosql_migrations WHERE id = ?',
    [migrationId]
  );

  // Log what needs to be done manually
  const migration = await getMigrationById(migrationId);
  console.warn('Migration SQL that was applied:');
  console.warn(migration.sql);
  console.warn('You must manually reverse these changes.');
}

// 5. Point-in-time recovery using time travel
async function recoverToTimestamp(db: Database, timestamp: Date): Promise<void> {
  // If time travel is enabled, can restore to previous state
  const snapshot = await db.getSnapshotAt(timestamp);
  if (snapshot) {
    await db.restoreFromSnapshot(snapshot);
    console.log(`Restored to snapshot at ${timestamp}`);
  } else {
    throw new Error('No snapshot available for requested timestamp');
  }
}
```

**Rollback Best Practices:**

| Situation | Approach |
|-----------|----------|
| Simple DDL (CREATE TABLE) | Use `DROP TABLE IF EXISTS` |
| ALTER TABLE ADD COLUMN | Recreate table without column (SQLite) |
| Data migration | Keep backup, restore if needed |
| Index changes | `DROP INDEX IF EXISTS` |
| Complex changes | Use time travel/snapshots |

---

## Data Issues

### Consistency Problems

**Problem:** Data appears inconsistent or corrupted.

**Symptoms:**
- Foreign key violations
- Orphaned records
- Duplicate entries where unique expected
- Unexpected NULL values
- Data doesn't match expected state

**Root Causes:**
1. Concurrent modifications without proper locking
2. Partial transaction failure without rollback
3. Application bug writing incorrect data
4. Migration issue corrupting data
5. Race condition in distributed system

**Solution Steps:**

```typescript
// 1. Enable foreign key checking
await db.exec('PRAGMA foreign_keys = ON');

// Check for foreign key violations
const violations = await db.query('PRAGMA foreign_key_check');
if (violations.length > 0) {
  console.error('Foreign key violations found:', violations);
  // violations: [{table, rowid, parent, fkid}]
}

// 2. Find orphaned records
const orphanedOrders = await db.query(`
  SELECT o.*
  FROM orders o
  LEFT JOIN users u ON o.user_id = u.id
  WHERE u.id IS NULL
`);

if (orphanedOrders.length > 0) {
  console.warn(`Found ${orphanedOrders.length} orphaned orders`);

  // Option A: Delete orphaned records
  await db.run(`
    DELETE FROM orders
    WHERE user_id NOT IN (SELECT id FROM users)
  `);

  // Option B: Set to default/null user
  await db.run(`
    UPDATE orders SET user_id = NULL
    WHERE user_id NOT IN (SELECT id FROM users)
  `);
}

// 3. Find duplicate records
const duplicates = await db.query(`
  SELECT email, COUNT(*) as count
  FROM users
  GROUP BY email
  HAVING count > 1
`);

if (duplicates.length > 0) {
  console.warn('Duplicate emails found:', duplicates);

  // Keep oldest, delete rest
  await db.run(`
    DELETE FROM users
    WHERE id NOT IN (
      SELECT MIN(id)
      FROM users
      GROUP BY email
    )
  `);
}

// 4. Data integrity check
async function checkDataIntegrity(db: Database): Promise<IntegrityReport> {
  const report: IntegrityReport = {
    foreignKeyViolations: [],
    duplicateRecords: [],
    nullViolations: [],
    orphanedRecords: [],
  };

  // Run SQLite integrity check
  const integrityResult = await db.query('PRAGMA integrity_check');
  if (integrityResult[0].integrity_check !== 'ok') {
    throw new Error('Database corruption detected: ' + integrityResult[0].integrity_check);
  }

  // Check foreign keys
  report.foreignKeyViolations = await db.query('PRAGMA foreign_key_check');

  // Check for nulls in required fields
  const tables = await db.query(`
    SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'
  `);

  for (const { name } of tables) {
    const columns = await db.query(`PRAGMA table_info(${name})`);
    for (const col of columns) {
      if (col.notnull && !col.dflt_value) {
        const nullCount = await db.queryOne(`
          SELECT COUNT(*) as count FROM ${name} WHERE ${col.name} IS NULL
        `);
        if (nullCount.count > 0) {
          report.nullViolations.push({
            table: name,
            column: col.name,
            count: nullCount.count,
          });
        }
      }
    }
  }

  return report;
}

// 5. Reconcile data between systems
async function reconcileWithSource(
  db: Database,
  sourceData: Record<string, unknown>[]
): Promise<ReconciliationResult> {
  const result = {
    matched: 0,
    mismatched: [],
    missingInDb: [],
    missingInSource: [],
  };

  const dbData = await db.query('SELECT * FROM records');
  const dbMap = new Map(dbData.map(r => [r.id, r]));
  const sourceMap = new Map(sourceData.map(r => [r.id as string, r]));

  // Check each source record
  for (const [id, sourceRecord] of sourceMap) {
    const dbRecord = dbMap.get(id);
    if (!dbRecord) {
      result.missingInDb.push(sourceRecord);
    } else if (!deepEqual(sourceRecord, dbRecord)) {
      result.mismatched.push({ id, source: sourceRecord, db: dbRecord });
    } else {
      result.matched++;
    }
  }

  // Check for extra records in DB
  for (const [id, dbRecord] of dbMap) {
    if (!sourceMap.has(id)) {
      result.missingInSource.push(dbRecord);
    }
  }

  return result;
}
```

---

### Data Recovery

**Problem:** Data was accidentally deleted or modified incorrectly.

**Solution:** Use various recovery techniques based on available backups.

```typescript
// 1. Soft delete pattern (prevention)
// Instead of DELETE, mark as deleted
await db.run(`
  UPDATE users SET deleted_at = datetime('now') WHERE id = ?
`, [userId]);

// Recover soft-deleted record
await db.run(`
  UPDATE users SET deleted_at = NULL WHERE id = ?
`, [userId]);

// 2. Time travel recovery (if enabled)
async function recoverFromTimeTravel(
  db: Database,
  tableName: string,
  recordId: number,
  timestamp: Date
): Promise<void> {
  // Get record at point in time
  const historicalRecord = await db.query(`
    SELECT * FROM ${tableName}
    AS OF TIMESTAMP '${timestamp.toISOString()}'
    WHERE id = ?
  `, [recordId]);

  if (historicalRecord.length > 0) {
    // Restore the record
    const record = historicalRecord[0];
    const columns = Object.keys(record).join(', ');
    const placeholders = Object.keys(record).map(() => '?').join(', ');

    await db.run(`
      INSERT OR REPLACE INTO ${tableName} (${columns})
      VALUES (${placeholders})
    `, Object.values(record));

    console.log(`Recovered record ${recordId} from ${timestamp}`);
  }
}

// 3. WAL-based recovery
async function recoverFromWAL(
  cdc: CDCSubscription,
  tableName: string,
  fromLSN: bigint
): Promise<void> {
  console.log(`Replaying WAL from LSN ${fromLSN} for table ${tableName}`);

  for await (const event of cdc.subscribe(fromLSN, { tables: [tableName] })) {
    switch (event.operation) {
      case 'INSERT':
        console.log('INSERT:', event.newRow);
        // Could re-apply or analyze
        break;
      case 'UPDATE':
        console.log('UPDATE:', event.oldRow, '->', event.newRow);
        break;
      case 'DELETE':
        console.log('DELETE:', event.oldRow);
        // This is what we're looking for to recover
        if (shouldRecover(event.oldRow)) {
          await reinsertRecord(event.oldRow);
        }
        break;
    }
  }
}

// 4. Export/import for manual recovery
async function exportTableToJSON(db: Database, tableName: string): Promise<string> {
  const data = await db.query(`SELECT * FROM ${tableName}`);
  return JSON.stringify(data, null, 2);
}

async function importTableFromJSON(
  db: Database,
  tableName: string,
  jsonData: string,
  options: { replace?: boolean } = {}
): Promise<void> {
  const data = JSON.parse(jsonData);

  if (options.replace) {
    await db.run(`DELETE FROM ${tableName}`);
  }

  for (const record of data) {
    const columns = Object.keys(record).join(', ');
    const placeholders = Object.keys(record).map(() => '?').join(', ');

    await db.run(`
      INSERT OR IGNORE INTO ${tableName} (${columns})
      VALUES (${placeholders})
    `, Object.values(record));
  }

  console.log(`Imported ${data.length} records into ${tableName}`);
}

// 5. Clone from another DO (if sharded/replicated)
async function cloneFromReplica(
  sourceStub: DurableObjectStub,
  targetDb: Database,
  tableName: string
): Promise<void> {
  const response = await sourceStub.fetch(
    new Request(`https://internal/export/${tableName}`)
  );

  const data = await response.json();
  await importTableFromJSON(targetDb, tableName, JSON.stringify(data), { replace: true });
}
```

---

### Backup and Restoration

**Problem:** Need to backup database and restore from backup.

**Solution:** Implement comprehensive backup and restore procedures.

```typescript
// 1. Create backup to R2
async function backupToR2(
  db: Database,
  r2: R2Bucket,
  options: {
    prefix?: string;
    compress?: boolean;
  } = {}
): Promise<string> {
  const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
  const prefix = options.prefix || 'backups';
  const key = `${prefix}/${timestamp}/database.sqlite`;

  // Serialize database
  const dbData = await db.serialize();

  // Optionally compress
  let data: Uint8Array;
  if (options.compress) {
    data = await compress(dbData);
  } else {
    data = dbData;
  }

  // Upload to R2
  await r2.put(key, data, {
    customMetadata: {
      createdAt: new Date().toISOString(),
      compressed: String(options.compress || false),
      version: await db.queryOne('PRAGMA user_version').then(r => String(r.user_version)),
    },
  });

  console.log(`Backup created: ${key} (${data.length} bytes)`);
  return key;
}

// 2. List available backups
async function listBackups(
  r2: R2Bucket,
  prefix: string = 'backups'
): Promise<BackupInfo[]> {
  const backups: BackupInfo[] = [];

  const listed = await r2.list({ prefix });

  for (const object of listed.objects) {
    backups.push({
      key: object.key,
      size: object.size,
      createdAt: object.uploaded,
      metadata: object.customMetadata,
    });
  }

  return backups.sort((a, b) =>
    new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime()
  );
}

// 3. Restore from R2 backup
async function restoreFromR2(
  db: Database,
  r2: R2Bucket,
  backupKey: string
): Promise<void> {
  console.log(`Restoring from backup: ${backupKey}`);

  const object = await r2.get(backupKey);
  if (!object) {
    throw new Error(`Backup not found: ${backupKey}`);
  }

  let data = new Uint8Array(await object.arrayBuffer());

  // Decompress if needed
  if (object.customMetadata?.compressed === 'true') {
    data = await decompress(data);
  }

  // Create backup of current state first
  const currentBackup = await backupToR2(db, r2, {
    prefix: 'pre-restore-backups',
  });
  console.log(`Current state backed up to: ${currentBackup}`);

  // Restore
  await db.deserialize(data);

  console.log('Restore complete');
}

// 4. Automated backup schedule (in DO)
export class TenantDatabase implements DurableObject {
  async alarm() {
    // This runs on schedule
    await this.performBackup();

    // Schedule next backup (24 hours)
    await this.state.storage.setAlarm(Date.now() + 24 * 60 * 60 * 1000);
  }

  private async performBackup() {
    try {
      const key = await backupToR2(this.db, this.env.BACKUP_BUCKET, {
        prefix: `tenants/${this.tenantId}`,
        compress: true,
      });

      // Clean up old backups (keep last 7)
      await this.cleanupOldBackups(7);

      console.log(`Automated backup complete: ${key}`);
    } catch (error) {
      console.error('Automated backup failed:', error);
      // Could send alert here
    }
  }

  private async cleanupOldBackups(keepCount: number) {
    const backups = await listBackups(
      this.env.BACKUP_BUCKET,
      `tenants/${this.tenantId}`
    );

    const toDelete = backups.slice(keepCount);
    for (const backup of toDelete) {
      await this.env.BACKUP_BUCKET.delete(backup.key);
      console.log(`Deleted old backup: ${backup.key}`);
    }
  }
}

// 5. Point-in-time recovery with incremental backups
interface IncrementalBackup {
  baseBackupKey: string;
  walSegments: string[];
  timestamp: Date;
}

async function createIncrementalBackup(
  db: Database,
  cdc: CDCSubscription,
  r2: R2Bucket,
  lastBackup: IncrementalBackup
): Promise<IncrementalBackup> {
  const timestamp = new Date();
  const prefix = `incremental/${timestamp.toISOString().split('T')[0]}`;

  // Get WAL since last backup
  const walEvents: CDCEvent[] = [];
  for await (const event of cdc.subscribe(lastBackup.timestamp)) {
    walEvents.push(event);
    if (new Date(event.timestamp) >= timestamp) break;
  }

  // Save WAL segment
  const walKey = `${prefix}/wal-${Date.now()}.json`;
  await r2.put(walKey, JSON.stringify(walEvents));

  return {
    baseBackupKey: lastBackup.baseBackupKey,
    walSegments: [...lastBackup.walSegments, walKey],
    timestamp,
  };
}

async function restorePointInTime(
  db: Database,
  r2: R2Bucket,
  backup: IncrementalBackup,
  targetTime: Date
): Promise<void> {
  // Restore base backup
  await restoreFromR2(db, r2, backup.baseBackupKey);

  // Replay WAL segments up to target time
  for (const walKey of backup.walSegments) {
    const walObject = await r2.get(walKey);
    if (!walObject) continue;

    const events: CDCEvent[] = JSON.parse(await walObject.text());

    for (const event of events) {
      if (new Date(event.timestamp) > targetTime) {
        console.log(`Reached target time ${targetTime}`);
        return;
      }

      await applyWALEvent(db, event);
    }
  }
}

async function applyWALEvent(db: Database, event: CDCEvent): Promise<void> {
  switch (event.operation) {
    case 'INSERT':
      const cols = Object.keys(event.newRow!).join(', ');
      const vals = Object.keys(event.newRow!).map(() => '?').join(', ');
      await db.run(
        `INSERT INTO ${event.table} (${cols}) VALUES (${vals})`,
        Object.values(event.newRow!)
      );
      break;
    case 'UPDATE':
      const sets = Object.keys(event.newRow!)
        .map(k => `${k} = ?`).join(', ');
      await db.run(
        `UPDATE ${event.table} SET ${sets} WHERE id = ?`,
        [...Object.values(event.newRow!), event.newRow!.id]
      );
      break;
    case 'DELETE':
      await db.run(
        `DELETE FROM ${event.table} WHERE id = ?`,
        [event.oldRow!.id]
      );
      break;
  }
}
```

**Backup Strategy Recommendations:**

| Data Criticality | Full Backup | Incremental | Retention |
|------------------|-------------|-------------|-----------|
| Low | Weekly | None | 2 weeks |
| Medium | Daily | None | 30 days |
| High | Daily | Hourly | 90 days |
| Critical | Hourly | Continuous WAL | 1 year |

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

### R2 Storage Errors

**Problem:** R2 storage operations fail with various errors.

**Error Messages:**
- `R2Error: InternalError`
- `R2Error: NoSuchKey`
- `R2Error: PreconditionFailed`
- `R2Error: EntityTooLarge`
- `R2_RATE_LIMITED`
- `R2_PERMISSION_DENIED`
- `R2_TEMPORARY_ERROR`

**Root Causes:**
1. Object doesn't exist (NoSuchKey)
2. Concurrent modification conflict (PreconditionFailed)
3. Object exceeds size limits (EntityTooLarge)
4. Rate limiting during high traffic
5. Network issues between DO and R2
6. Bucket permissions misconfigured

**Solution Steps:**

```typescript
// 1. Handle common R2 errors
async function safeR2Get(bucket: R2Bucket, key: string): Promise<R2Object | null> {
  try {
    return await bucket.get(key);
  } catch (error) {
    const r2Error = error as R2Error;

    switch (r2Error.name) {
      case 'NoSuchKey':
        // Object doesn't exist - this is often expected
        return null;

      case 'InternalError':
        // R2 internal issue - retry
        console.warn(`R2 internal error for ${key}, retrying...`);
        await delay(1000);
        return bucket.get(key);

      default:
        throw error;
    }
  }
}

// 2. Handle rate limiting
async function r2WithRateLimitRetry<T>(
  operation: () => Promise<T>,
  maxRetries = 3
): Promise<T> {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      if ((error as any).message?.includes('rate limit')) {
        const delay = Math.pow(2, attempt) * 1000 + Math.random() * 1000;
        console.warn(`R2 rate limited, waiting ${delay}ms...`);
        await new Promise(r => setTimeout(r, delay));
        continue;
      }
      throw error;
    }
  }
  throw new Error('R2 rate limit retries exhausted');
}

// 3. Handle large objects (multipart upload)
async function uploadLargeObject(
  bucket: R2Bucket,
  key: string,
  data: Uint8Array
): Promise<void> {
  const MAX_SINGLE_UPLOAD = 5 * 1024 * 1024 * 1024; // 5GB
  const PART_SIZE = 100 * 1024 * 1024; // 100MB parts

  if (data.length < PART_SIZE) {
    // Small enough for single upload
    await bucket.put(key, data);
    return;
  }

  if (data.length > MAX_SINGLE_UPLOAD) {
    throw new Error(`Object too large: ${data.length} bytes (max 5GB)`);
  }

  // Multipart upload
  const upload = await bucket.createMultipartUpload(key);

  try {
    const parts: R2UploadedPart[] = [];
    let offset = 0;
    let partNumber = 1;

    while (offset < data.length) {
      const chunk = data.slice(offset, offset + PART_SIZE);
      const part = await upload.uploadPart(partNumber, chunk);
      parts.push(part);

      offset += PART_SIZE;
      partNumber++;
    }

    await upload.complete(parts);
  } catch (error) {
    await upload.abort();
    throw error;
  }
}

// 4. Handle conditional operations (optimistic locking)
async function updateWithVersion(
  bucket: R2Bucket,
  key: string,
  updater: (current: any) => any
): Promise<void> {
  const MAX_RETRIES = 5;

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    // Get current object with etag
    const current = await bucket.get(key);
    const etag = current?.etag;

    // Compute new value
    const currentData = current ? await current.json() : null;
    const newData = updater(currentData);

    try {
      // Put with condition
      await bucket.put(key, JSON.stringify(newData), {
        onlyIf: etag ? { etagMatches: etag } : { etagDoesNotMatch: '*' },
      });
      return;
    } catch (error) {
      if ((error as any).name === 'PreconditionFailed') {
        // Concurrent modification, retry
        console.warn(`Concurrent modification of ${key}, retry ${attempt + 1}`);
        await delay(Math.random() * 100);
        continue;
      }
      throw error;
    }
  }

  throw new Error(`Failed to update ${key} after ${MAX_RETRIES} attempts`);
}

// 5. Comprehensive error handling
class R2ErrorHandler {
  static handle(error: unknown, key: string, operation: string): never {
    const r2Error = error as any;

    const enhancedError = {
      operation,
      key,
      errorName: r2Error.name,
      errorMessage: r2Error.message,
      retryable: this.isRetryable(r2Error),
      suggestion: this.getSuggestion(r2Error),
    };

    console.error('R2 Error:', enhancedError);

    throw new Error(
      `R2 ${operation} failed for "${key}": ${r2Error.message}. ` +
      `Suggestion: ${enhancedError.suggestion}`
    );
  }

  static isRetryable(error: any): boolean {
    const retryableErrors = [
      'InternalError',
      'ServiceUnavailable',
      'SlowDown',
    ];
    return retryableErrors.includes(error.name);
  }

  static getSuggestion(error: any): string {
    switch (error.name) {
      case 'NoSuchKey':
        return 'Verify the object exists before reading';
      case 'NoSuchBucket':
        return 'Check bucket name in wrangler.jsonc';
      case 'EntityTooLarge':
        return 'Use multipart upload for objects >5GB';
      case 'PreconditionFailed':
        return 'Object was modified concurrently, retry with fresh data';
      case 'InternalError':
        return 'Retry the operation with exponential backoff';
      default:
        return 'Check R2 documentation for this error';
    }
  }
}
```

**R2 Limits Reference:**

| Limit | Value |
|-------|-------|
| Max object size | 5 TB (multipart) |
| Max single PUT | 5 GB |
| Max multipart parts | 10,000 |
| Min part size | 5 MB (except last) |
| Max metadata size | 2 KB |
| Operations/second (free) | 10 |
| Operations/second (paid) | 1,000+ |

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
