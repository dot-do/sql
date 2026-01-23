# DoSQL Troubleshooting Guide

This guide helps developers diagnose and resolve common issues when using DoSQL with Cloudflare Workers and Durable Objects.

## Table of Contents

- [Quick Diagnostic Checklist](#quick-diagnostic-checklist)
- [Common Issues and Solutions](#common-issues-and-solutions)
  - [Connection Issues](#connection-issues)
  - [Query Issues](#query-issues)
  - [Transaction Issues](#transaction-issues)
  - [Performance Issues](#performance-issues)
  - [Migration Issues](#migration-issues)
  - [CDC/Streaming Issues](#cdcstreaming-issues)
  - [Deployment Issues](#deployment-issues)
- [Error Messages Explained](#error-messages-explained)
  - [RPC Error Codes](#rpc-error-codes)
  - [Transaction Error Codes](#transaction-error-codes)
  - [R2 Storage Errors](#r2-storage-errors)
  - [CDC Error Codes](#cdc-error-codes)
  - [Migration Errors](#migration-errors)
- [Debugging Techniques](#debugging-techniques)
  - [Enable Debug Logging](#enable-debug-logging)
  - [Using Wrangler Tail](#using-wrangler-tail)
  - [Inspecting DO State](#inspecting-do-state)
  - [Query Analysis with EXPLAIN](#query-analysis-with-explain)
  - [Tracing Request Flow](#tracing-request-flow)
- [FAQ](#faq)
- [Getting Help](#getting-help)

---

## Quick Diagnostic Checklist

Before diving into specific issues, verify these common setup requirements:

| Check | How to Verify | Common Fix |
|-------|---------------|------------|
| DO class exported | `grep "export.*ClassName" src/index.ts` | Add `export { ClassName }` to entry point |
| Migration tag exists | Check `wrangler.jsonc` for `migrations` array | Add `{ "tag": "v1", "new_classes": ["ClassName"] }` |
| Binding names match | Compare `wrangler.jsonc` binding `name` with `env.NAME` | Ensure exact match |
| R2 bucket exists | `wrangler r2 bucket list` | `wrangler r2 bucket create bucket-name` |
| Deployment succeeded | `wrangler deployments list` | Redeploy with `wrangler deploy` |

---

## Common Issues and Solutions

### Connection Issues

#### Durable Object Not Found

**Symptoms:** `Error: Durable Object not found`, `No such Durable Object`, `class_name not found`

**Root Causes:**
1. DO class not exported from Worker entry point
2. Missing migration tag in `wrangler.jsonc`
3. Class name mismatch between code and configuration

**Solution:**

```typescript
// src/index.ts - MUST export the DO class
export { TenantDatabase } from './database';
```

```jsonc
// wrangler.jsonc
{
  "durable_objects": {
    "bindings": [{ "name": "DOSQL_DB", "class_name": "TenantDatabase" }]
  },
  "migrations": [{ "tag": "v1", "new_classes": ["TenantDatabase"] }]
}
```

#### WebSocket Connection Failures

**Symptoms:** `WebSocket connection failed`, `Connection timeout`, connection drops

**Root Causes:**
1. DO hibernation (DOs hibernate after ~10 seconds of inactivity)
2. Network issues or Cloudflare edge timeout (100 seconds for WebSocket idle)
3. Client timeout too short for cold starts

**Solution:**

```typescript
const client = await createWebSocketClient({
  url: 'wss://your-worker.workers.dev/db',
  connectTimeoutMs: 10000,  // Allow for cold starts
  autoReconnect: true,
  maxReconnectAttempts: 5,
});

// Implement heartbeat to prevent hibernation
setInterval(() => client.send('ping'), 30000);
```

#### Authentication Errors

**Symptoms:** `UNAUTHORIZED` (401), `FORBIDDEN` (403)

**Solution:** Verify authorization header is present and valid:

```typescript
const client = createHttpClient({
  url: 'https://your-worker.workers.dev/db',
  headers: { 'Authorization': `Bearer ${token}` },
});
```

---

### Query Issues

#### SQL Syntax Errors

**Symptoms:** `SQLSyntaxError`, `SYNTAX_ERROR` with line/column location

**Common Mistakes:**

| Wrong | Correct |
|-------|---------|
| `SELECT * FORM users` | `SELECT * FROM users` |
| `INSERT users (name)` | `INSERT INTO users (name)` |
| `DELETE users WHERE id = 1` | `DELETE FROM users WHERE id = 1` |

**Solution:** Use the error's location info and validate queries:

```typescript
import { validateSQL } from '@dotdo/dosql/parser';

const validation = validateSQL('SELECT * FROM users WHERE id = ?');
if (!validation.valid) {
  console.error('Invalid SQL:', validation.errors);
}
```

#### Parameter Binding Issues

**Symptoms:** Wrong results, `TYPE_MISMATCH`, null values

**Root Causes:** Parameter count/order mismatch, type conversion issues

**Solution:** Use named parameters for complex queries:

```typescript
// Positional - order matters
await db.query('SELECT * FROM users WHERE name = ? AND age > ?', ['Alice', 25]);

// Named - order doesn't matter (preferred)
await db.query(
  'SELECT * FROM users WHERE name = :name AND age > :age',
  { name: 'Alice', age: 25 }
);
```

---

### Transaction Issues

#### Transaction Deadlocks

**Symptoms:** `TXN_DEADLOCK`, `TXN_LOCK_TIMEOUT`, transactions timing out

**Root Causes:** Two transactions waiting for each other's locks

**Solution:**

```typescript
// 1. Use IMMEDIATE mode to acquire locks upfront
await db.transaction(async (tx) => {
  await tx.run('UPDATE accounts SET balance = balance - 100 WHERE id = ?', [1]);
  await tx.run('UPDATE accounts SET balance = balance + 100 WHERE id = ?', [2]);
}, { mode: 'IMMEDIATE' });

// 2. Implement deadlock retry
async function withDeadlockRetry<T>(fn: () => Promise<T>, maxRetries = 3): Promise<T> {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      if (error.code === 'TXN_DEADLOCK') {
        await new Promise(r => setTimeout(r, Math.random() * 100 * Math.pow(2, attempt)));
        continue;
      }
      throw error;
    }
  }
  throw new Error('Max deadlock retries exceeded');
}
```

#### Transaction Timeouts

**Symptoms:** `TXN_TIMEOUT`, `Transaction exceeded timeout`

**Root Causes:** Transaction scope too large, external calls inside transaction

**Solution:**

```typescript
// WRONG: External calls inside transaction
await db.transaction(async (tx) => {
  const data = await fetchExternalAPI();  // Don't do this!
  await tx.run('INSERT INTO data VALUES (?)', [data]);
});

// CORRECT: External calls outside transaction
const data = await fetchExternalAPI();
await db.transaction(async (tx) => {
  await tx.run('INSERT INTO data VALUES (?)', [data]);
}, { timeoutMs: 10000 });
```

---

### Performance Issues

#### Slow Queries

**Symptoms:** High `executionTimeMs`, request timeouts

**Root Causes:** Missing indexes, full table scans

**Solution:**

```sql
-- Analyze query execution
EXPLAIN QUERY PLAN SELECT * FROM orders WHERE user_id = 42;

-- Look for "SCAN" (bad) vs "SEARCH" (good)
-- Add indexes for frequently queried columns
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
```

#### Memory Limits

**Symptoms:** `RESOURCE_EXHAUSTED`, DO crashes, high memory usage

**Root Causes:** Loading too much data into memory (DO limit: 128MB)

**Solution:**

```typescript
// BAD: Loads all rows into memory
const allRows = await db.query('SELECT * FROM huge_table');

// GOOD: Stream large results
for await (const chunk of client.queryStream({
  sql: 'SELECT * FROM huge_table',
  chunkSize: 1000,
})) {
  await processChunk(chunk.rows);
}

// GOOD: Use pagination
const page = await db.query(
  'SELECT * FROM huge_table LIMIT 100 OFFSET ?',
  [pageNumber * 100]
);
```

#### Cold Start Latency

**Symptoms:** First request takes 40-110ms longer than subsequent requests

**Solution:**

```typescript
// Optimize DO initialization - keep constructor lightweight
export class TenantDatabase implements DurableObject {
  private db: Database | null = null;

  async fetch(request: Request): Promise<Response> {
    // Lazy load database
    if (!this.db) {
      this.db = await this.openDatabase();
    }
    return this.handleRequest(request);
  }
}

// Implement prewarm endpoint for critical tenants
if (url.pathname === '/prewarm') {
  await this.ensureInitialized();
  return new Response('OK');
}
```

---

### Migration Issues

#### Schema Migration Failures

**Symptoms:** Migration runner throws errors, tables not created

**Root Causes:** SQL syntax errors, constraint violations, objects already exist

**Solution:**

```typescript
// 1. Use dry-run to validate first
const runner = createMigrationRunner(db, { dryRun: true });
const status = await runner.getStatus(migrations);

// 2. Use IF NOT EXISTS for idempotent migrations
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL
);

// 3. Break large data migrations into batches
UPDATE users SET normalized_email = LOWER(email)
WHERE normalized_email IS NULL
LIMIT 1000;
```

#### Checksum Mismatches

**Symptoms:** `Checksum mismatch` validation error

**Root Cause:** Migration file was modified after being applied

**Solution:** Never modify applied migrations. Create a new corrective migration instead:

```sql
-- migrations/003_fix_column_type.sql (new migration)
ALTER TABLE users ADD COLUMN email_temp TEXT;
UPDATE users SET email_temp = email;
-- ... rest of fix
```

---

### CDC/Streaming Issues

#### Missing Events

**Symptoms:** Gaps in event sequence, `CDC_LSN_NOT_FOUND`

**Root Causes:** Starting from wrong LSN, WAL segments compacted

**Solution:**

```typescript
// Configure adequate WAL retention
const db = await DB('tenant', {
  walConfig: {
    retentionPeriodMs: 7 * 24 * 60 * 60 * 1000, // 7 days
    minRetainedSegments: 100,
  },
});

// Use replication slots for durable position tracking
const slot = await cdc.slots.createSlot('my-consumer', fromLSN);
const subscription = await cdc.slots.subscribeFromSlot('my-consumer');
```

#### Backpressure Issues

**Symptoms:** `CDC_BUFFER_OVERFLOW`, increasing lag

**Solution:** Batch processing and handle backpressure signals:

```typescript
streamer.on('backpressure', (signal) => {
  if (signal.type === 'pause') {
    console.warn(`Buffer at ${signal.bufferUtilization * 100}%, pausing`);
  }
});
```

---

### Deployment Issues

#### Bundle Size Limits

**Symptoms:** `Bundle size exceeds limit`

**Solution:**

```typescript
// Use selective imports
import { DB } from '@dotdo/dosql';           // ~7 KB gzipped
import { createCDC } from '@dotdo/dosql/cdc'; // +2 KB

// Don't import everything
// import * from '@dotdo/dosql';  // ~34 KB - avoid!
```

#### R2 Bucket Binding Errors

**Symptoms:** `R2_BUCKET_NOT_BOUND`, `undefined` when accessing bucket

**Solution:**

```bash
# Create bucket
wrangler r2 bucket create my-data-bucket
```

```jsonc
// wrangler.jsonc
{
  "r2_buckets": [{
    "binding": "DATA_BUCKET",
    "bucket_name": "my-data-bucket"
  }]
}
```

---

## Error Messages Explained

### RPC Error Codes

| Code | Meaning | Retryable | Action |
|------|---------|-----------|--------|
| `TIMEOUT` | Request exceeded timeout | Yes | Increase timeout, retry with backoff |
| `UNAUTHORIZED` | Missing/invalid auth | No | Check Authorization header |
| `FORBIDDEN` | Insufficient permissions | No | Verify permissions |
| `NOT_FOUND` | Resource doesn't exist | No | Verify resource exists |
| `SYNTAX_ERROR` | Invalid SQL syntax | No | Fix the SQL query |
| `TYPE_MISMATCH` | Parameter type wrong | No | Check parameter types |
| `RESOURCE_EXHAUSTED` | Memory/pool limit hit | Maybe | Reduce data size, stream results |
| `CONNECTION_ERROR` | Network issue | Yes | Retry with backoff |

### Transaction Error Codes

| Code | Meaning | Action |
|------|---------|--------|
| `TXN_DEADLOCK` | Circular lock dependency | Retry with random delay |
| `TXN_TIMEOUT` | Transaction exceeded time limit | Reduce transaction scope |
| `TXN_LOCK_TIMEOUT` | Lock acquisition timeout | Reduce contention, use IMMEDIATE mode |
| `TXN_CONFLICT` | Serialization conflict | Retry the transaction |
| `TXN_ABORTED` | Transaction was rolled back | Check error cause, retry if appropriate |

### R2 Storage Errors

| Code | Meaning | Action |
|------|---------|--------|
| `R2_NOT_FOUND` / `NoSuchKey` | Object doesn't exist | Verify key, handle gracefully |
| `R2_RATE_LIMITED` | Too many requests | Retry with exponential backoff |
| `R2_PERMISSION_DENIED` | Insufficient permissions | Check bucket binding |
| `R2_TEMPORARY_ERROR` | Transient R2 issue | Retry with backoff |
| `PreconditionFailed` | Concurrent modification | Retry with fresh data |
| `EntityTooLarge` | Object exceeds size limit | Use multipart upload for >5GB |

### CDC Error Codes

| Code | Meaning | Action |
|------|---------|--------|
| `CDC_LSN_NOT_FOUND` | WAL segment compacted | Start from earliest available LSN |
| `CDC_SUBSCRIPTION_FAILED` | Subscription setup failed | Check configuration, retry |
| `CDC_BUFFER_OVERFLOW` | Consumer too slow | Implement batch processing |

### Migration Errors

| Error | Meaning | Action |
|-------|---------|--------|
| `Checksum mismatch` | Migration file modified | Never modify applied migrations |
| `Migration failed` | SQL error during migration | Check SQL syntax, use dry-run |
| `SQLITE_CONSTRAINT` | Constraint violation | Fix data before migration |

---

## Debugging Techniques

### Enable Debug Logging

```typescript
import { DB, ConsoleLogger, LogLevel } from '@dotdo/dosql';

const db = await DB('tenant', {
  logger: new ConsoleLogger({
    level: LogLevel.DEBUG,
    includeTimestamp: true,
  }),
  queryLogger: {
    onQueryStart(sql, params) {
      console.debug(`[QUERY] ${sql}`, params);
      return { startTime: performance.now() };
    },
    onQueryEnd(context, result) {
      console.debug(`[QUERY] ${performance.now() - context.startTime}ms, ${result.rowCount} rows`);
    },
  },
});
```

### Using Wrangler Tail

```bash
# Basic log tailing
wrangler tail

# Filter by status
wrangler tail --status error

# Filter by content
wrangler tail --search "tenant-123"

# JSON output for processing
wrangler tail --format json | jq '.logs[]'

# High traffic sampling
wrangler tail --sampling-rate 0.1
```

### Inspecting DO State

Add a debug endpoint to your DO (protect with auth in production):

```typescript
if (url.pathname === '/__debug/state') {
  return new Response(JSON.stringify({
    tenantId: this.tenantId,
    connectionCount: this.connections.size,
    databaseStats: await this.getDatabaseStats(),
    uptime: Date.now() - this.startTime,
  }, null, 2));
}
```

### Query Analysis with EXPLAIN

```sql
-- Analyze query execution plan
EXPLAIN QUERY PLAN
SELECT o.*, u.name
FROM orders o
JOIN users u ON o.user_id = u.id
WHERE o.status = 'pending'
ORDER BY o.created_at DESC
LIMIT 100;

-- Good output (uses indexes):
-- SEARCH orders USING INDEX idx_orders_status (status=?)

-- Bad output (full table scan):
-- SCAN orders
```

### Tracing Request Flow

```typescript
// Add trace context to all requests
function createTraceContext(request: Request) {
  return {
    traceId: request.headers.get('X-Trace-ID') || crypto.randomUUID(),
    spanId: crypto.randomUUID().substring(0, 16),
    startTime: performance.now(),
  };
}

// Log with trace context
function logWithTrace(trace, event, data) {
  console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    traceId: trace.traceId,
    spanId: trace.spanId,
    event,
    durationMs: performance.now() - trace.startTime,
    ...data,
  }));
}
```

---

## FAQ

### General

**Q: What is the memory limit for Durable Objects?**

A: 128MB. Use streaming for large datasets and tiered storage (DO + R2) for large databases.

**Q: How long before a DO hibernates?**

A: Approximately 10 seconds of no incoming requests. Use WebSocket connections with heartbeats to keep DOs warm.

**Q: What's the maximum request time for a DO?**

A: 30 seconds CPU time per request. Long-running operations should be broken into smaller chunks.

### Connections

**Q: Why do my WebSocket connections keep dropping?**

A: Common causes:
1. DO hibernation - implement 30-second heartbeat
2. Cloudflare edge timeout (100s) - keep connections active
3. Client network changes - implement auto-reconnect

**Q: How do I handle cold start latency?**

A: Cold starts typically add 40-110ms. Solutions:
- Set client timeout to 10+ seconds for initial connection
- Implement prewarm endpoints for critical tenants
- Keep DO constructor lightweight, lazy-load database

### Queries

**Q: How do I prevent SQL injection?**

A: Always use parameterized queries:
```typescript
// WRONG
db.query(`SELECT * FROM users WHERE name = '${userInput}'`);

// CORRECT
db.query('SELECT * FROM users WHERE name = ?', [userInput]);
```

**Q: Why is my query slow?**

A: Check for missing indexes:
```sql
EXPLAIN QUERY PLAN SELECT * FROM orders WHERE user_id = 42;
-- If you see "SCAN", add an index:
CREATE INDEX idx_orders_user_id ON orders(user_id);
```

### Transactions

**Q: How do I avoid deadlocks?**

A:
1. Keep transactions short
2. Access tables in consistent order across your codebase
3. Use `IMMEDIATE` mode when you know you'll write
4. Implement deadlock retry with random backoff

**Q: Why is my transaction timing out?**

A: Don't make external/network calls inside transactions:
```typescript
// Move external calls OUTSIDE the transaction
const data = await fetchExternalAPI();
await db.transaction(async (tx) => {
  await tx.run('INSERT INTO data VALUES (?)', [data]);
});
```

### Migrations

**Q: Can I modify an applied migration?**

A: No. Never modify applied migrations. Create a new corrective migration instead.

**Q: How do I rollback a migration?**

A: Define `downSql` in your migrations and use the rollback function, or restore from a backup.

### CDC

**Q: Why am I missing CDC events?**

A: Common causes:
1. WAL segments compacted before consumption - increase retention
2. Starting from wrong LSN - verify starting position
3. Filter excluding events - check filter configuration

**Q: How do I handle backpressure?**

A: Implement batch processing and listen for backpressure signals:
```typescript
streamer.on('backpressure', (signal) => {
  if (signal.type === 'pause') {
    // Slow down or pause consumption
  }
});
```

### Deployment

**Q: How do I reduce bundle size?**

A: Use selective imports:
```typescript
import { DB } from '@dotdo/dosql';           // ~7 KB
import { createCDC } from '@dotdo/dosql/cdc'; // +2 KB
// Avoid: import * from '@dotdo/dosql';       // ~34 KB
```

**Q: Why is my DO not found after deployment?**

A: Verify:
1. DO class is exported from entry point
2. `wrangler.jsonc` has migration tag with `new_classes`
3. Class name matches exactly in binding and export
4. Deployment succeeded (`wrangler deployments list`)

---

## Getting Help

If you're still experiencing issues:

1. **Check the logs**: Use `wrangler tail` to view real-time logs
2. **Enable debug logging**: Set `logger: new ConsoleLogger({ level: LogLevel.DEBUG })`
3. **Review recent changes**: Check git history for related modifications
4. **Search existing issues**: Check the GitHub repository for similar problems
5. **Open a GitHub issue** with:
   - DoSQL version
   - Minimal reproduction code
   - Error messages and stack traces
   - Wrangler configuration (sanitized)

## Related Documentation

- [Getting Started](./getting-started.md) - Basic setup and usage
- [API Reference](./api-reference.md) - Complete API documentation
- [Architecture](./architecture.md) - Understanding DoSQL internals
- [Advanced Features](./advanced.md) - Time travel, branching, CDC
