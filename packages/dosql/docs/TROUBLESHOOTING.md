# DoSQL Troubleshooting Guide

This guide helps developers diagnose and resolve issues when using DoSQL with Cloudflare Workers and Durable Objects.

## Table of Contents

- [Quick Diagnostic Checklist](#quick-diagnostic-checklist)
- [Setup and Configuration](#setup-and-configuration)
- [Connection Issues](#connection-issues)
- [Query Issues](#query-issues)
- [Transaction Issues](#transaction-issues)
- [Performance Issues](#performance-issues)
- [Migration Issues](#migration-issues)
- [CDC/Streaming Issues](#cdcstreaming-issues)
- [Storage Issues](#storage-issues)
- [Branch and Time Travel Issues](#branch-and-time-travel-issues)
- [Deployment Issues](#deployment-issues)
- [Error Reference](#error-reference)
- [Debugging Techniques](#debugging-techniques)
- [FAQ](#faq)
- [Getting Help](#getting-help)

---

## Quick Diagnostic Checklist

Run through these checks before diving into specific issues:

| Check | Command/Action | Fix |
|-------|----------------|-----|
| Node.js version | `node --version` | Use Node.js 18+ |
| Package installed | `npm ls @dotdo/dosql` | `npm install @dotdo/dosql` |
| DO class exported | Check `src/index.ts` has `export class` | Add `export` keyword |
| Migration tag exists | Check `wrangler.jsonc` migrations array | Add migration tag |
| Binding names match | Compare wrangler config with `env.NAME` | Fix name mismatch |
| R2 bucket exists | `wrangler r2 bucket list` | `wrangler r2 bucket create <name>` |
| Deployment status | `wrangler deployments list` | Redeploy if needed |
| Wrangler version | `wrangler --version` | `npm install -g wrangler@latest` |

---

## Setup and Configuration

### Durable Object Not Found

**Symptoms:**
- `Error: Durable Object not found`
- `No such Durable Object`
- `class_name not found`

**Causes:**
1. DO class not exported from Worker entry point
2. Missing or incorrect migration tag in `wrangler.jsonc`
3. Class name mismatch between code and configuration

**Solution:**

Ensure your DO class is exported:

```typescript
// src/index.ts
export class TenantDatabase implements DurableObject {
  // implementation
}
```

Configure wrangler correctly:

```jsonc
// wrangler.jsonc
{
  "durable_objects": {
    "bindings": [
      { "name": "TENANT_DB", "class_name": "TenantDatabase" }
    ]
  },
  "migrations": [
    { "tag": "v1", "new_classes": ["TenantDatabase"] }
  ]
}
```

### Binding Mismatch

**Symptoms:**
- `TypeError: Cannot read properties of undefined`
- `env.BINDING_NAME is undefined`

**Solution:**

The binding name in `wrangler.jsonc` must match the property name in your Env interface exactly:

```typescript
// src/index.ts
export interface Env {
  TENANT_DB: DurableObjectNamespace;  // Must match wrangler.jsonc
}
```

```jsonc
// wrangler.jsonc
{
  "durable_objects": {
    "bindings": [
      { "name": "TENANT_DB", "class_name": "TenantDatabase" }
    ]
  }
}
```

### Workers Paid Plan Required

**Symptoms:**
- `Durable Objects require a Workers Paid plan`

**Solution:**

Durable Objects require a Workers Paid plan ($5/month). Upgrade at [dash.cloudflare.com](https://dash.cloudflare.com) under Workers & Pages > Plans.

---

## Connection Issues

### WebSocket Connection Failures

**Symptoms:**
- `WebSocket connection failed`
- `Connection timeout`
- Connection drops frequently

**Causes:**
1. DO hibernation (DOs hibernate after ~10 seconds of inactivity)
2. Cloudflare edge timeout (100 seconds for idle WebSocket)
3. Client timeout too short for cold starts

**Solution:**

Configure longer timeouts and implement heartbeat:

```typescript
const client = await createWebSocketClient({
  url: 'wss://your-worker.workers.dev/db',
  connectTimeoutMs: 10000,  // Allow for cold starts
  autoReconnect: true,
  maxReconnectAttempts: 5,
});

// Prevent hibernation with heartbeat
setInterval(() => {
  if (client.readyState === WebSocket.OPEN) {
    client.send('ping');
  }
}, 30000);
```

### Database Connection Closed

**Symptoms:**
- `DB_CLOSED`
- Operations fail after some time

**Causes:**
1. Database handle was explicitly closed
2. DO was evicted and restarted
3. Unhandled exception closed the connection

**Solution:**

Implement lazy initialization:

```typescript
export class TenantDatabase implements DurableObject {
  private db: Database | null = null;

  private async getDB(): Promise<Database> {
    if (!this.db || this.db.closed) {
      this.db = await DB('tenant', {
        storage: { hot: this.state.storage },
      });
    }
    return this.db;
  }

  async fetch(request: Request): Promise<Response> {
    const db = await this.getDB();
    // Use db...
  }
}
```

### Authentication Errors

**Symptoms:**
- `UNAUTHORIZED` (401)
- `FORBIDDEN` (403)

**Solution:**

Verify authorization header is present and valid:

```typescript
const client = createHttpClient({
  url: 'https://your-worker.workers.dev/db',
  headers: { 'Authorization': `Bearer ${token}` },
});
```

---

## Query Issues

### SQL Syntax Errors

**Symptoms:**
- `SYNTAX_ERROR`
- `SYNTAX_UNEXPECTED_TOKEN`
- `SYNTAX_UNEXPECTED_EOF`

**Common mistakes:**

| Wrong | Correct |
|-------|---------|
| `SELECT * FORM users` | `SELECT * FROM users` |
| `INSERT users (name)` | `INSERT INTO users (name)` |
| `DELETE users WHERE id = 1` | `DELETE FROM users WHERE id = 1` |
| `UPDATE users name = 'Alice'` | `UPDATE users SET name = 'Alice'` |

**Solution:**

Use the SQL validator before executing:

```typescript
import { validateSQL } from '@dotdo/dosql/parser';

const result = validateSQL('SELECT * FROM users WHERE id = ?');
if (!result.valid) {
  console.error('SQL errors:', result.errors);
  // errors include: code, message, line, column
}
```

### Parameter Binding Issues

**Symptoms:**
- `BIND_MISSING_PARAM`
- `BIND_COUNT_MISMATCH`
- `BIND_TYPE_MISMATCH`
- Unexpected null values in results

**Solution:**

Use named parameters for clarity:

```typescript
// Positional parameters (order matters)
await db.query(
  'SELECT * FROM users WHERE name = ? AND age > ?',
  ['Alice', 25]
);

// Named parameters (recommended for complex queries)
await db.query(
  'SELECT * FROM users WHERE name = :name AND age > :age',
  { name: 'Alice', age: 25 }
);
```

Always verify parameter count matches placeholders:

```typescript
const sql = 'INSERT INTO users (name, email, role) VALUES (?, ?, ?)';
const params = ['Alice', 'alice@example.com', 'admin']; // Exactly 3 params
await db.run(sql, params);
```

### Table or Column Not Found

**Symptoms:**
- `STMT_TABLE_NOT_FOUND`
- `STMT_COLUMN_NOT_FOUND`

**Solution:**

Verify table and column names:

```typescript
// List all tables
const tables = await db.query(
  "SELECT name FROM sqlite_master WHERE type='table'"
);
console.log('Tables:', tables.map(t => t.name));

// List columns in a table
const columns = await db.query('PRAGMA table_info(users)');
console.log('Columns:', columns.map(c => c.name));
```

Ensure migrations have run:

```typescript
const runner = createMigrationRunner(db);
const status = await runner.getStatus(migrations);
console.log('Pending migrations:', status.pending);
await runner.run(migrations);
```

---

## Transaction Issues

### Deadlocks

**Symptoms:**
- `TXN_DEADLOCK`
- `TXN_LOCK_TIMEOUT`
- Transactions timing out

**Solution:**

1. Use IMMEDIATE mode to acquire locks upfront:

```typescript
await db.transaction(async (tx) => {
  await tx.run('UPDATE accounts SET balance = balance - 100 WHERE id = ?', [1]);
  await tx.run('UPDATE accounts SET balance = balance + 100 WHERE id = ?', [2]);
}, { mode: 'IMMEDIATE' });
```

2. Implement deadlock retry:

```typescript
async function withDeadlockRetry<T>(
  fn: () => Promise<T>,
  maxRetries = 3
): Promise<T> {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      if (error.code === 'TXN_DEADLOCK' && attempt < maxRetries - 1) {
        // Random backoff to prevent repeated collisions
        await new Promise(r =>
          setTimeout(r, Math.random() * 100 * Math.pow(2, attempt))
        );
        continue;
      }
      throw error;
    }
  }
  throw new Error('Unreachable');
}
```

3. Access resources in consistent order across your codebase to prevent deadlocks.

### Transaction Timeouts

**Symptoms:**
- `TXN_TIMEOUT`
- Transaction exceeds timeout

**Cause:**
External calls (network, APIs) inside transaction hold locks too long.

**Solution:**

Move external calls outside the transaction:

```typescript
// WRONG: Network call inside transaction
await db.transaction(async (tx) => {
  const data = await fetchExternalAPI();  // Blocks transaction
  await tx.run('INSERT INTO data VALUES (?)', [data]);
});

// CORRECT: Network call before transaction
const data = await fetchExternalAPI();
await db.transaction(async (tx) => {
  await tx.run('INSERT INTO data VALUES (?)', [data]);
}, { timeoutMs: 5000 });
```

### Read-Only Violation

**Symptoms:**
- `TXN_READ_ONLY_VIOLATION`

**Solution:**

Remove the `readOnly` flag when you need to write:

```typescript
// For read-write operations
await db.transaction(async (tx) => {
  await tx.run('INSERT INTO users (name) VALUES (?)', ['Alice']);
});  // readOnly defaults to false

// Only use readOnly for pure reads
await db.transaction(async (tx) => {
  return await tx.query('SELECT * FROM users');
}, { readOnly: true });
```

### No Active Transaction

**Symptoms:**
- `TXN_NO_ACTIVE`

**Solution:**

Use the transaction wrapper consistently:

```typescript
// Recommended: automatic commit/rollback
await db.transaction(async (tx) => {
  await tx.run('INSERT INTO users (name) VALUES (?)', ['Alice']);
  // Automatically commits on success, rolls back on error
});

// If using manual transactions, check state
if (db.inTransaction) {
  await db.commit();
}
```

---

## Performance Issues

### Slow Queries

**Symptoms:**
- High `executionTimeMs`
- Request timeouts

**Solution:**

Analyze query execution:

```sql
EXPLAIN QUERY PLAN
SELECT * FROM orders WHERE user_id = 42 AND status = 'pending';
```

Look for "SCAN" (bad) vs "SEARCH" (good). Add indexes for frequently queried columns:

```sql
-- Single column index
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);

-- Composite index for multi-column queries
CREATE INDEX IF NOT EXISTS idx_orders_user_status ON orders(user_id, status);
```

Update statistics after adding data:

```sql
ANALYZE;
```

### Memory Limits

**Symptoms:**
- `RESOURCE_EXHAUSTED`
- `FSX_SIZE_EXCEEDED`
- DO crashes

**Cause:**
Loading too much data into memory. DO limit is 128MB.

**Solution:**

Stream large results:

```typescript
// BAD: Loads all rows into memory
const allRows = await db.query('SELECT * FROM huge_table');

// GOOD: Stream with chunks
for await (const chunk of client.queryStream({
  sql: 'SELECT * FROM huge_table',
  chunkSize: 1000,
})) {
  await processChunk(chunk.rows);
}

// GOOD: Cursor-based pagination (most efficient)
let lastId = 0;
while (true) {
  const batch = await db.query(
    'SELECT * FROM huge_table WHERE id > ? ORDER BY id LIMIT 100',
    [lastId]
  );
  if (batch.length === 0) break;
  await processBatch(batch);
  lastId = batch[batch.length - 1].id;
}
```

### Cold Start Latency

**Symptoms:**
- First request takes 40-110ms longer than subsequent requests

**Solution:**

Keep DO constructor lightweight and lazy-load the database:

```typescript
export class TenantDatabase implements DurableObject {
  private db: Database | null = null;

  constructor(state: DurableObjectState) {
    // Keep constructor fast - no async work here
    this.state = state;
  }

  async fetch(request: Request): Promise<Response> {
    // Lazy initialization
    if (!this.db) {
      this.db = await this.openDatabase();
    }
    return this.handleRequest(request);
  }
}
```

For critical tenants, implement a prewarm endpoint:

```typescript
if (url.pathname === '/prewarm') {
  await this.getDB();  // Initialize database
  return new Response('OK');
}
```

### High Write Latency

**Solution:**

Batch writes in transactions:

```typescript
// BAD: Individual inserts
for (const item of items) {
  await db.run('INSERT INTO items (id, data) VALUES (?, ?)', [item.id, item.data]);
}

// GOOD: Batched transaction
await db.transaction(async (tx) => {
  for (const item of items) {
    await tx.run('INSERT INTO items (id, data) VALUES (?, ?)', [item.id, item.data]);
  }
}); // Single commit at the end
```

---

## Migration Issues

### Migration Failures

**Symptoms:**
- Migration runner throws errors
- Tables not created

**Solution:**

1. Use dry-run to validate first:

```typescript
const runner = createMigrationRunner(db, { dryRun: true });
const status = await runner.getStatus(migrations);
console.log('Pending:', status.pending);
```

2. Use IF NOT EXISTS for idempotent migrations:

```sql
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_users_name ON users(name);
```

3. Break large data migrations into batches:

```sql
-- Avoid updating all rows at once
UPDATE users SET normalized_email = LOWER(email)
WHERE normalized_email IS NULL
LIMIT 1000;
```

### Checksum Mismatch

**Symptoms:**
- `Checksum mismatch` validation error

**Cause:**
Migration file was modified after being applied.

**Solution:**

Never modify applied migrations. Create a new corrective migration:

```sql
-- migrations/003_fix_column.sql (new file)
ALTER TABLE users ADD COLUMN email_normalized TEXT;
UPDATE users SET email_normalized = LOWER(email);
```

### Migration Order Issues

**Symptoms:**
- Foreign key errors
- Missing table references

**Solution:**

Name migrations with numeric prefixes to ensure correct order:

```
.do/migrations/
  001_create_users.sql
  002_create_orders.sql      # References users
  003_add_user_indexes.sql
```

---

## CDC/Streaming Issues

### Missing Events

**Symptoms:**
- Gaps in event sequence
- `CDC_LSN_NOT_FOUND`

**Causes:**
1. Starting from wrong LSN
2. WAL segments compacted before consumption

**Solution:**

Configure adequate WAL retention:

```typescript
const db = await DB('tenant', {
  walConfig: {
    retentionPeriodMs: 7 * 24 * 60 * 60 * 1000, // 7 days
    minRetainedSegments: 100,
  },
});
```

Handle LSN not found gracefully:

```typescript
try {
  await cdc.subscribe({ fromLSN: savedLSN });
} catch (error) {
  if (error.code === 'CDC_LSN_NOT_FOUND') {
    const earliest = await cdc.getEarliestLSN();
    await cdc.subscribe({ fromLSN: earliest });
  } else {
    throw error;
  }
}
```

### Backpressure Issues

**Symptoms:**
- `CDC_BUFFER_OVERFLOW`
- Increasing consumer lag

**Solution:**

Process events in batches and handle backpressure signals:

```typescript
streamer.on('backpressure', (signal) => {
  if (signal.type === 'pause') {
    console.warn(`Buffer at ${signal.bufferUtilization * 100}%`);
  }
});

// Batch processing
const batchSize = 100;
let batch = [];

for await (const event of subscription) {
  batch.push(event);
  if (batch.length >= batchSize) {
    await processBatch(batch);
    await subscription.ack(event.lsn);
    batch = [];
  }
}
```

### Replication Slot Issues

**Symptoms:**
- `CDC_SLOT_NOT_FOUND`
- `CDC_SLOT_EXISTS`

**Solution:**

```typescript
// Create slot with existence check
await cdc.slots.createSlot('my-consumer', { ifNotExists: true });

// Or check manually
const slots = await cdc.slots.list();
if (!slots.find(s => s.name === 'my-consumer')) {
  await cdc.slots.createSlot('my-consumer');
}

// Clean up unused slots
await cdc.slots.dropSlot('old-consumer');
```

---

## Storage Issues

### R2 Bucket Not Bound

**Symptoms:**
- `R2_BUCKET_NOT_BOUND`
- `undefined` when accessing bucket

**Solution:**

1. Create the bucket:

```bash
wrangler r2 bucket create my-data-bucket
```

2. Configure binding in `wrangler.jsonc`:

```jsonc
{
  "r2_buckets": [
    {
      "binding": "DATA_BUCKET",
      "bucket_name": "my-data-bucket"
    }
  ]
}
```

3. Verify in code:

```typescript
if (!env.DATA_BUCKET) {
  throw new Error('R2 bucket not bound - check wrangler.jsonc');
}
```

### R2 Rate Limits

**Symptoms:**
- `R2_RATE_LIMITED`
- HTTP 429 errors

**Solution:**

Implement exponential backoff:

```typescript
async function withR2Retry<T>(
  fn: () => Promise<T>,
  maxRetries = 3
): Promise<T> {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      if (error.code === 'R2_RATE_LIMITED') {
        const delay = error.retryAfter ?? Math.pow(2, attempt) * 1000;
        await new Promise(r => setTimeout(r, delay));
        continue;
      }
      throw error;
    }
  }
  throw new Error('R2 rate limit retries exhausted');
}
```

### Large File Uploads

**Symptoms:**
- `R2_SIZE_EXCEEDED`
- Upload failures for large files

**Solution:**

Use multipart upload for files over 5GB:

```typescript
async function uploadLargeFile(
  bucket: R2Bucket,
  key: string,
  data: ArrayBuffer
) {
  const CHUNK_SIZE = 100 * 1024 * 1024; // 100MB

  if (data.byteLength <= 5 * 1024 * 1024 * 1024) {
    return bucket.put(key, data);
  }

  const upload = await bucket.createMultipartUpload(key);
  const parts: R2UploadedPart[] = [];

  for (let i = 0; i * CHUNK_SIZE < data.byteLength; i++) {
    const chunk = data.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE);
    const part = await upload.uploadPart(i + 1, chunk);
    parts.push(part);
  }

  return upload.complete(parts);
}
```

### Data Corruption

**Symptoms:**
- `FSX_CHUNK_CORRUPTED`
- `R2_CHECKSUM_MISMATCH`
- `WAL_CHECKSUM_MISMATCH`

**Solution:**

Enable checksum verification and implement recovery:

```typescript
const storage = createStorage({
  verifyChecksums: true,
  checksumAlgorithm: 'md5',
});

try {
  const data = await storage.read('path/to/file');
} catch (error) {
  if (error.code === 'FSX_CHUNK_CORRUPTED') {
    console.error('Data corruption detected, attempting recovery');
    const backup = await r2Backup.get('path/to/file');
    if (backup) {
      await storage.write('path/to/file', backup);
    }
  }
}
```

---

## Branch and Time Travel Issues

### Branch Not Found

**Symptoms:**
- `BRANCH_NOT_FOUND`

**Solution:**

```typescript
// List available branches
const branches = await branchManager.listBranches();
console.log('Available:', branches.map(b => b.name));

// Create if missing
const branch = await branchManager.getBranch('feature-x');
if (!branch) {
  await branchManager.createBranch({ name: 'feature-x' });
}
await branchManager.checkout('feature-x');
```

### Merge Conflicts

**Symptoms:**
- `MERGE_CONFLICT`

**Solution:**

```typescript
try {
  await branchManager.merge('feature', 'main');
} catch (error) {
  if (error.code === 'MERGE_CONFLICT') {
    for (const conflict of error.details.conflicts) {
      // Choose resolution: 'ours', 'theirs', or custom
      await branchManager.resolveMerge(conflict.path, 'theirs');
    }
    await branchManager.commit('Merge feature into main');
  }
}
```

### Uncommitted Changes

**Symptoms:**
- `UNCOMMITTED_CHANGES` when switching branches

**Solution:**

```typescript
const status = await branchManager.status();
if (status.hasChanges) {
  // Option 1: Commit changes
  await branchManager.commit('WIP: save progress');

  // Option 2: Stash changes
  await branchManager.stash();
  await branchManager.checkout('other-branch');
  // Later: await branchManager.stashPop();
}
```

### Time Travel Errors

**Symptoms:**
- `TT_POINT_NOT_FOUND`
- `TT_LSN_NOT_AVAILABLE`

**Causes:**
1. Requested time point is too old (data archived/deleted)
2. Snapshot doesn't exist
3. Time point is in the future

**Solution:**

```typescript
try {
  const result = await db.queryAsOf(
    'SELECT * FROM users',
    [],
    { timestamp: targetTime }
  );
} catch (error) {
  switch (error.code) {
    case 'TT_POINT_NOT_FOUND':
    case 'TT_LSN_NOT_AVAILABLE':
      // Fall back to earliest available point
      const earliest = await db.getEarliestTimePoint();
      return await db.queryAsOf('SELECT * FROM users', [], earliest);

    case 'TT_FUTURE_POINT':
      throw new Error('Cannot query future time points');

    default:
      throw error;
  }
}
```

---

## Deployment Issues

### Bundle Size Limits

**Symptoms:**
- `Bundle size exceeds limit`
- Deployment fails

**Solution:**

Use selective imports:

```typescript
// Minimal import (~7 KB gzipped)
import { DB } from '@dotdo/dosql';

// Add only what you need
import { createCDC } from '@dotdo/dosql/cdc';  // +2 KB
import { createBranchManager } from '@dotdo/dosql/branch';  // +3 KB

// AVOID: Importing everything (~34 KB)
// import * from '@dotdo/dosql';
```

### Type Errors After Update

**Symptoms:**
- TypeScript errors after updating DoSQL

**Solution:**

```bash
# Clear caches
rm -rf node_modules/.cache
rm -rf dist
rm -rf .wrangler

# Clean install
rm -rf node_modules
npm ci

# Rebuild
npm run build
```

### Port Already in Use

**Symptoms:**
- `Port 8787 is already in use`

**Solution:**

```bash
# Kill existing processes
pkill -f wrangler

# Or use different port
npx wrangler dev --port 8788
```

---

## Error Reference

### Database Errors

| Code | Meaning | Retryable | Action |
|------|---------|-----------|--------|
| `DB_CLOSED` | Connection closed | No | Reopen database |
| `DB_READ_ONLY` | Database is read-only | No | Open with write access |
| `DB_NOT_FOUND` | Database doesn't exist | No | Create database or check path |
| `DB_CONSTRAINT` | Constraint violation | No | Fix data to satisfy constraints |
| `DB_QUERY_ERROR` | Query execution failed | Maybe | Check SQL and parameters |
| `DB_CONNECTION_FAILED` | Connection failed | Yes | Retry with backoff |
| `DB_CONFIG_ERROR` | Configuration error | No | Fix configuration |
| `DB_TIMEOUT` | Operation timed out | Yes | Increase timeout, retry |
| `DB_INTERNAL` | Internal error | Maybe | Contact support if persistent |

### Statement Errors

| Code | Meaning | Action |
|------|---------|--------|
| `STMT_FINALIZED` | Statement already closed | Create new statement |
| `STMT_SYNTAX` | SQL syntax error | Fix SQL syntax |
| `STMT_EXECUTION` | Execution failed | Check query and data |
| `STMT_TABLE_NOT_FOUND` | Table doesn't exist | Create table or check name |
| `STMT_COLUMN_NOT_FOUND` | Column doesn't exist | Check column name |
| `STMT_INVALID_SQL` | Invalid SQL | Review SQL syntax |
| `STMT_UNSUPPORTED` | Unsupported operation | Use alternative approach |

### Binding Errors

| Code | Meaning | Action |
|------|---------|--------|
| `BIND_MISSING_PARAM` | Required parameter missing | Provide all parameters |
| `BIND_TYPE_MISMATCH` | Parameter type wrong | Check parameter types |
| `BIND_INVALID_TYPE` | Invalid type for binding | Use supported types |
| `BIND_COUNT_MISMATCH` | Parameter count wrong | Match placeholders to params |
| `BIND_NAMED_EXPECTED` | Named param expected | Use `:name` syntax |

### Syntax Errors

| Code | Meaning | Action |
|------|---------|--------|
| `SYNTAX_UNEXPECTED_TOKEN` | Unexpected token | Check SQL at error location |
| `SYNTAX_UNEXPECTED_EOF` | Unexpected end of input | Complete the SQL statement |
| `SYNTAX_INVALID_IDENTIFIER` | Invalid identifier | Fix table/column name |
| `SYNTAX_INVALID_LITERAL` | Invalid literal value | Fix string/number literal |
| `SYNTAX_ERROR` | General syntax error | Review SQL syntax |

### Transaction Errors

| Code | Meaning | Retryable | Action |
|------|---------|-----------|--------|
| `TXN_NO_ACTIVE` | No active transaction | No | Start transaction first |
| `TXN_ALREADY_ACTIVE` | Transaction in progress | No | Commit/rollback existing |
| `TXN_SAVEPOINT_NOT_FOUND` | Savepoint doesn't exist | No | Check savepoint name |
| `TXN_DUPLICATE_SAVEPOINT` | Savepoint name taken | No | Use unique name |
| `TXN_LOCK_FAILED` | Lock acquisition failed | Yes | Retry with backoff |
| `TXN_LOCK_TIMEOUT` | Lock wait timed out | Yes | Increase timeout, retry |
| `TXN_DEADLOCK` | Deadlock detected | Yes | Retry with random delay |
| `TXN_SERIALIZATION_FAILURE` | Serialization conflict | Yes | Retry transaction |
| `TXN_ABORTED` | Transaction aborted | Maybe | Check cause, retry |
| `TXN_READ_ONLY_VIOLATION` | Write in read-only txn | No | Use read-write transaction |
| `TXN_TIMEOUT` | Transaction timeout | Yes | Reduce scope, increase timeout |

### R2 Storage Errors

| Code | Meaning | Retryable | Action |
|------|---------|-----------|--------|
| `R2_NOT_FOUND` | Object not found | No | Verify key exists |
| `R2_TIMEOUT` | Operation timed out | Yes | Retry with backoff |
| `R2_RATE_LIMITED` | Rate limit exceeded | Yes | Wait for retryAfter |
| `R2_PERMISSION_DENIED` | Access denied | No | Check bucket permissions |
| `R2_BUCKET_NOT_BOUND` | Bucket not configured | No | Add binding to wrangler.jsonc |
| `R2_SIZE_EXCEEDED` | Object too large | No | Use multipart upload |
| `R2_CONFLICT` | Concurrent modification | Yes | Retry with fresh etag |
| `R2_CHECKSUM_MISMATCH` | Data integrity error | Yes | Retry upload |
| `R2_READ_DURING_WRITE` | Read during write | Yes | Wait and retry |
| `R2_NETWORK_ERROR` | Network issue | Yes | Retry with backoff |

### FSX Storage Errors

| Code | Meaning | Action |
|------|---------|--------|
| `FSX_NOT_FOUND` | File not found | Check path |
| `FSX_WRITE_FAILED` | Write failed | Check quota, retry |
| `FSX_READ_FAILED` | Read failed | Check permissions |
| `FSX_DELETE_FAILED` | Delete failed | Check if file exists |
| `FSX_CHUNK_CORRUPTED` | Data corrupted | Restore from backup |
| `FSX_SIZE_EXCEEDED` | File too large | Use streaming/chunking |
| `FSX_MIGRATION_FAILED` | Tier migration failed | Check storage tiers |
| `FSX_INVALID_RANGE` | Invalid byte range | Check range boundaries |

### CDC Errors

| Code | Meaning | Action |
|------|---------|--------|
| `CDC_SUBSCRIPTION_FAILED` | Subscription failed | Check configuration |
| `CDC_LSN_NOT_FOUND` | WAL position unavailable | Start from earliest |
| `CDC_SLOT_NOT_FOUND` | Replication slot missing | Create slot first |
| `CDC_SLOT_EXISTS` | Slot already exists | Use existing or drop |
| `CDC_BUFFER_OVERFLOW` | Consumer too slow | Implement batching |
| `CDC_DECODE_ERROR` | Event decode failed | Check event format |

### WAL Errors

| Code | Meaning | Action |
|------|---------|--------|
| `WAL_CHECKSUM_MISMATCH` | Checksum failed | Data may be corrupted |
| `WAL_SEGMENT_NOT_FOUND` | Segment missing | May be compacted |
| `WAL_ENTRY_NOT_FOUND` | Entry not found | May be compacted |
| `WAL_INVALID_LSN` | Invalid LSN | Check LSN ordering |
| `WAL_SEGMENT_CORRUPTED` | Segment corrupted | Restore from backup |
| `WAL_RECOVERY_FAILED` | Recovery failed | Contact support |
| `WAL_CHECKPOINT_FAILED` | Checkpoint failed | Retry checkpoint |
| `WAL_FLUSH_FAILED` | Flush failed | Check storage |
| `WAL_SERIALIZATION_ERROR` | Serialization failed | Check data format |

### Branch Errors

| Code | Meaning | Action |
|------|---------|--------|
| `BRANCH_EXISTS` | Branch already exists | Use different name |
| `BRANCH_NOT_FOUND` | Branch doesn't exist | Create if needed |
| `CANNOT_DELETE_CURRENT` | Can't delete current | Switch branch first |
| `BRANCH_PROTECTED` | Branch is protected | Contact admin |
| `BRANCH_ARCHIVED` | Branch is read-only | Unarchive or create new |
| `COMMIT_NOT_FOUND` | Commit doesn't exist | Check commit ID |
| `UNCOMMITTED_CHANGES` | Changes not committed | Commit or discard |
| `MERGE_CONFLICT` | Merge has conflicts | Resolve manually |
| `NOT_FAST_FORWARD` | Can't fast-forward | Merge or rebase |
| `INVALID_BRANCH_NAME` | Invalid characters | Use alphanumeric/hyphens |
| `NOT_MERGED` | Unmerged commits | Merge first or force delete |

### Time Travel Errors

| Code | Meaning | Action |
|------|---------|--------|
| `TT_POINT_NOT_FOUND` | Time point unavailable | Use available point |
| `TT_LSN_NOT_AVAILABLE` | LSN not available | Data may be archived |
| `TT_SNAPSHOT_NOT_FOUND` | Snapshot missing | Check snapshot ID |
| `TT_BRANCH_NOT_FOUND` | Branch missing | Check branch name |
| `TT_TIMEOUT` | Query timed out | Reduce query scope |
| `TT_DATA_GAP` | Gap in data | Check retention |
| `TT_CONSISTENCY_ERROR` | Cross-shard issue | Retry with different point |
| `TT_INVALID_SYNTAX` | Invalid AS OF syntax | Check syntax |
| `TT_FUTURE_POINT` | Time in future | Use current or past time |
| `TT_SESSION_EXPIRED` | Session expired | Create new session |

### Distributed Transaction Errors

| Code | Meaning | Retryable | Action |
|------|---------|-----------|--------|
| `DTX_NO_ACTIVE` | No active transaction | No | Start transaction |
| `DTX_ALREADY_ACTIVE` | Transaction active | No | Commit/rollback first |
| `DTX_INVALID_STATE` | Invalid state | No | Check transaction flow |
| `DTX_PREPARE_FAILED` | Prepare failed | Maybe | Check participants |
| `DTX_COMMIT_FAILED` | Commit failed | Maybe | Check coordinator |
| `DTX_ROLLBACK_FAILED` | Rollback failed | No | Manual intervention |
| `DTX_TIMEOUT` | Transaction timeout | Yes | Increase timeout |
| `DTX_PARTICIPANT_FAILURE` | Participant failed | Yes | Retry transaction |
| `DTX_COORDINATOR_FAILURE` | Coordinator failed | No | Failover required |
| `DTX_LOCK_FAILED` | Lock failed | Yes | Retry with backoff |
| `DTX_SERIALIZATION_FAILURE` | Serialization conflict | Yes | Retry transaction |

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
      console.debug('[QUERY START]', sql, params);
      return { startTime: performance.now() };
    },
    onQueryEnd(context, result) {
      const duration = performance.now() - context.startTime;
      console.debug(`[QUERY END] ${duration.toFixed(2)}ms, ${result.rowCount} rows`);
    },
    onQueryError(context, error) {
      console.error('[QUERY ERROR]', {
        sql: context.sql,
        error: error.message,
        code: error.code,
      });
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

# Sample high traffic (10%)
wrangler tail --sampling-rate 0.1

# Filter specific errors
wrangler tail --format json | jq 'select(.logs[].message | contains("TXN_DEADLOCK"))'
```

### Inspecting DO State

Add a debug endpoint (protect with authentication in production):

```typescript
if (url.pathname === '/__debug/state') {
  const dbStats = await this.getDatabaseStats();
  return Response.json({
    tenantId: this.tenantId,
    uptime: Date.now() - this.startTime,
    connectionCount: this.connections.size,
    activeTransactions: this.transactionManager.getActiveCount(),
    database: dbStats,
  }, { headers: { 'Content-Type': 'application/json' } });
}

async getDatabaseStats() {
  const db = await this.getDB();
  return {
    tables: await db.query("SELECT name FROM sqlite_master WHERE type='table'"),
    pageCount: (await db.query('PRAGMA page_count'))[0]?.page_count,
    pageSize: (await db.query('PRAGMA page_size'))[0]?.page_size,
    journalMode: (await db.query('PRAGMA journal_mode'))[0]?.journal_mode,
  };
}
```

### Query Analysis

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
-- SEARCH users USING INTEGER PRIMARY KEY (rowid=?)

-- Bad output (full table scan):
-- SCAN orders
-- SCAN users

-- Update statistics for better query planning
ANALYZE;

-- Check index usage
SELECT * FROM sqlite_stat1;
```

### Request Tracing

```typescript
function createTraceContext(request: Request) {
  return {
    traceId: request.headers.get('X-Trace-ID') || crypto.randomUUID(),
    spanId: crypto.randomUUID().slice(0, 16),
    startTime: performance.now(),
  };
}

function logWithTrace(trace: TraceContext, event: string, data?: object) {
  console.log(JSON.stringify({
    timestamp: new Date().toISOString(),
    traceId: trace.traceId,
    spanId: trace.spanId,
    durationMs: performance.now() - trace.startTime,
    event,
    ...data,
  }));
}

// Usage
async fetch(request: Request): Promise<Response> {
  const trace = createTraceContext(request);
  logWithTrace(trace, 'request_start', { path: new URL(request.url).pathname });

  try {
    const result = await this.handleRequest(request, trace);
    logWithTrace(trace, 'request_end', { status: 200 });
    return result;
  } catch (error) {
    logWithTrace(trace, 'request_error', { error: error.message, code: error.code });
    throw error;
  }
}
```

### Error Inspection

```typescript
import { isDoSQLError } from '@dotdo/dosql/errors';

// Check if error is from DoSQL
if (isDoSQLError(error)) {
  console.log('Code:', error.code);
  console.log('Category:', error.category);
  console.log('Recovery hint:', error.recoveryHint);
  console.log('Retryable:', error.isRetryable());
  console.log('User message:', error.toUserMessage());
}

// Generic retry wrapper
async function withRetry<T>(
  fn: () => Promise<T>,
  { maxRetries = 3, baseDelay = 100 } = {}
): Promise<T> {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      const retryable = isDoSQLError(error) && error.isRetryable();
      if (!retryable || attempt === maxRetries - 1) throw error;

      const delay = baseDelay * Math.pow(2, attempt) + Math.random() * 100;
      await new Promise(r => setTimeout(r, delay));
    }
  }
  throw new Error('Unreachable');
}
```

---

## FAQ

### General

**What is the memory limit for Durable Objects?**

128MB. Use streaming for large datasets and tiered storage (DO + R2) for large databases.

**How long before a DO hibernates?**

Approximately 10 seconds without incoming requests. Use WebSocket heartbeats to keep DOs active.

**What is the maximum request time?**

30 seconds CPU time per request. Break long operations into smaller chunks.

**What SQLite features are supported?**

Most features including transactions, indexes, CTEs, window functions, JSON functions, and RETURNING clauses. Not supported: ATTACH DATABASE, loadable extensions.

### Connections

**Why do WebSocket connections keep dropping?**

Common causes:
1. DO hibernation - implement 30-second heartbeat
2. Cloudflare edge timeout (100s) - keep connections active
3. Client network changes - implement auto-reconnect

**How do I handle cold start latency?**

Cold starts add 40-110ms. Solutions:
- Set client timeout to 10+ seconds
- Implement prewarm endpoints
- Keep DO constructor lightweight

**How many concurrent connections can a DO handle?**

No hard limit, but each WebSocket consumes memory. Monitor usage and consider connection pooling.

### Queries

**How do I prevent SQL injection?**

Always use parameterized queries:

```typescript
// Safe
await db.query('SELECT * FROM users WHERE email = ?', [userInput]);

// Unsafe - never do this
// await db.query(`SELECT * FROM users WHERE email = '${userInput}'`);
```

**Why is my query slow?**

Usually missing indexes. Check with EXPLAIN QUERY PLAN and add indexes for frequently queried columns.

**What is the maximum query result size?**

Limited by DO memory (128MB). Use LIMIT/OFFSET, cursor pagination, or streaming for large results.

### Transactions

**How do I avoid deadlocks?**

1. Keep transactions short
2. Access tables in consistent order
3. Use IMMEDIATE mode for write transactions
4. Implement retry with random backoff

**Why is my transaction timing out?**

Likely external calls inside the transaction. Move network operations outside transactions.

**What isolation level does DoSQL use?**

SERIALIZABLE by default. READ COMMITTED available for better concurrency.

**Can I nest transactions?**

Use savepoints for nested semantics:

```typescript
await db.transaction(async (tx) => {
  await tx.savepoint('sp1');
  try {
    await tx.run('...');
  } catch {
    await tx.rollbackTo('sp1');
  }
});
```

### Migrations

**Can I modify an applied migration?**

No. Never modify applied migrations. Create a new corrective migration instead.

**How do I rollback a migration?**

Define `downSql` in migrations or restore from backup:

```typescript
const migration = {
  id: '001',
  upSql: 'CREATE TABLE users ...',
  downSql: 'DROP TABLE users',
};
await runner.rollback(migration);
```

### CDC

**Why am I missing CDC events?**

Common causes:
1. WAL compacted - increase retention
2. Wrong starting LSN - verify position
3. Filter excluding events - check configuration

**What is the event ordering guarantee?**

Events are delivered in LSN order within a single DO. Across DOs, use vector clocks or timestamps.

### Deployment

**How do I reduce bundle size?**

Use selective imports. Import only `{ DB }` for minimal size (~7KB gzipped).

**Why is my DO not found after deployment?**

Verify:
1. DO class is exported
2. Migration tag exists in wrangler.jsonc
3. Class name matches exactly
4. Deployment succeeded

---

## Getting Help

If you are still experiencing issues:

1. **Check logs**: Use `wrangler tail` for real-time logs
2. **Enable debug logging**: Set `LogLevel.DEBUG`
3. **Review recent changes**: Check git history
4. **Inspect error details**: Use `error.code`, `error.recoveryHint`
5. **Search existing issues**: Check the GitHub repository

**Opening an issue:**

Include:
- DoSQL version (`npm ls @dotdo/dosql`)
- Minimal reproduction code
- Error messages with codes and stack traces
- Wrangler configuration (sanitized)
- Steps to reproduce

---

## Related Documentation

- [Getting Started](./getting-started.md) - Basic setup and usage
- [API Reference](./api-reference.md) - Complete API documentation
- [Architecture](./architecture.md) - DoSQL internals
- [Advanced Features](./advanced.md) - Time travel, branching, CDC
- [Security](./SECURITY.md) - Security best practices
- [Deployment](./DEPLOYMENT.md) - Production deployment guide
