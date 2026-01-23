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
  - [Storage Issues](#storage-issues)
  - [Branch Issues](#branch-issues)
  - [Deployment Issues](#deployment-issues)
- [Error Messages Explained](#error-messages-explained)
  - [Database Error Codes](#database-error-codes)
  - [Statement Error Codes](#statement-error-codes)
  - [Binding Error Codes](#binding-error-codes)
  - [Syntax Error Codes](#syntax-error-codes)
  - [Transaction Error Codes](#transaction-error-codes)
  - [R2 Storage Errors](#r2-storage-errors)
  - [FSX Storage Errors](#fsx-storage-errors)
  - [CDC Error Codes](#cdc-error-codes)
  - [WAL Error Codes](#wal-error-codes)
  - [Branch Error Codes](#branch-error-codes)
  - [Migration Errors](#migration-errors)
- [Debugging Techniques](#debugging-techniques)
  - [Enable Debug Logging](#enable-debug-logging)
  - [Using Wrangler Tail](#using-wrangler-tail)
  - [Inspecting DO State](#inspecting-do-state)
  - [Query Analysis with EXPLAIN](#query-analysis-with-explain)
  - [Tracing Request Flow](#tracing-request-flow)
  - [Error Inspection Utilities](#error-inspection-utilities)
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
| Node version | `node --version` | Use Node.js 18+ for best compatibility |
| Package version | `npm ls @dotdo/dosql` | Update with `npm update @dotdo/dosql` |

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

#### Database Connection Closed

**Symptoms:** `DB_CLOSED`, operations fail after some time

**Root Causes:**
1. Database handle was explicitly closed
2. DO was evicted and restarted
3. Connection pool exhausted

**Solution:**

```typescript
// Ensure database is open before each operation
async function ensureDatabase() {
  if (!this.db || this.db.closed) {
    this.db = await this.openDatabase();
  }
  return this.db;
}

// Handle in request handler
async fetch(request: Request): Promise<Response> {
  const db = await this.ensureDatabase();
  // ... use db
}
```

---

### Query Issues

#### SQL Syntax Errors

**Symptoms:** `SYNTAX_ERROR`, `SYNTAX_UNEXPECTED_TOKEN`, `SYNTAX_UNEXPECTED_EOF` with line/column location

**Common Mistakes:**

| Wrong | Correct |
|-------|---------|
| `SELECT * FORM users` | `SELECT * FROM users` |
| `INSERT users (name)` | `INSERT INTO users (name)` |
| `DELETE users WHERE id = 1` | `DELETE FROM users WHERE id = 1` |
| `UPDATE users name = 'Alice'` | `UPDATE users SET name = 'Alice'` |
| `SELECT * FROM users WHERE id = ?;` (trailing semicolon with params) | `SELECT * FROM users WHERE id = ?` |

**Solution:** Use the error's location info and validate queries:

```typescript
import { validateSQL } from '@dotdo/dosql/parser';

const validation = validateSQL('SELECT * FROM users WHERE id = ?');
if (!validation.valid) {
  console.error('Invalid SQL:', validation.errors);
  // errors include: code, message, line, column
}
```

#### Parameter Binding Issues

**Symptoms:** Wrong results, `BIND_TYPE_MISMATCH`, `BIND_MISSING_PARAM`, `BIND_COUNT_MISMATCH`, null values

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

// Check parameter count matches placeholders
const sql = 'SELECT * FROM users WHERE a = ? AND b = ? AND c = ?';
const params = ['a', 'b', 'c']; // Must have exactly 3 parameters
```

#### Table or Column Not Found

**Symptoms:** `STMT_TABLE_NOT_FOUND`, `STMT_COLUMN_NOT_FOUND`

**Root Causes:** Table/column doesn't exist, typo in name, migrations not run

**Solution:**

```typescript
// Check table exists
const tables = await db.query(
  "SELECT name FROM sqlite_master WHERE type='table'"
);
console.log('Available tables:', tables.map(t => t.name));

// Check column exists
const columns = await db.query('PRAGMA table_info(users)');
console.log('Columns in users:', columns.map(c => c.name));

// Run migrations first
const runner = createMigrationRunner(db);
await runner.run(migrations);
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

// 3. Access resources in consistent order
// WRONG: Transaction A locks users then orders, Transaction B locks orders then users
// CORRECT: Always lock in same order (alphabetically: orders, then users)
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

#### No Active Transaction

**Symptoms:** `TXN_NO_ACTIVE` when trying to commit or rollback

**Root Causes:** Transaction already committed/rolled back, or never started

**Solution:**

```typescript
// Always use the transaction wrapper
await db.transaction(async (tx) => {
  // tx.commit() and tx.rollback() are handled automatically
  await tx.run('INSERT INTO users (name) VALUES (?)', ['Alice']);
  // Throwing an error will rollback
  // Successful completion will commit
});

// If managing transactions manually, check state
if (db.inTransaction) {
  await db.commit();
}
```

#### Read-Only Violation

**Symptoms:** `TXN_READ_ONLY_VIOLATION` when trying to write

**Solution:**

```typescript
// WRONG: Writing in read-only transaction
await db.transaction(async (tx) => {
  await tx.run('INSERT INTO users ...'); // Error!
}, { readOnly: true });

// CORRECT: Remove readOnly flag for write operations
await db.transaction(async (tx) => {
  await tx.run('INSERT INTO users ...');
}, { readOnly: false }); // or omit readOnly entirely
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

-- Composite indexes for multi-column queries
CREATE INDEX IF NOT EXISTS idx_orders_user_status
ON orders(user_id, status);
```

#### Memory Limits

**Symptoms:** `RESOURCE_EXHAUSTED`, `FSX_SIZE_EXCEEDED`, DO crashes, high memory usage

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

// GOOD: Use cursor-based pagination (more efficient)
const page = await db.query(
  'SELECT * FROM huge_table WHERE id > ? ORDER BY id LIMIT 100',
  [lastSeenId]
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

#### High Write Latency

**Symptoms:** Write operations taking longer than expected

**Solution:**

```typescript
// Batch multiple writes in a single transaction
await db.transaction(async (tx) => {
  for (const item of items) {
    await tx.run('INSERT INTO items VALUES (?, ?)', [item.id, item.data]);
  }
}); // Single commit at the end

// Use INSERT with multiple rows (use parameterized queries in production)
const placeholders = items.map(() => '(?, ?)').join(',');
const values = items.flatMap(i => [i.id, i.data]);
await db.run(`INSERT INTO items (id, data) VALUES ${placeholders}`, values);
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
console.log('Pending migrations:', status.pending);

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

#### Migration Order Issues

**Symptoms:** Foreign key errors, missing table references

**Solution:**

```typescript
// Ensure migrations are numbered correctly
// 001_create_users.sql
// 002_create_orders.sql (depends on users)

// Check migration order
const migrations = [
  { id: '001', name: 'create_users', sql: '...' },
  { id: '002', name: 'create_orders', sql: '...' }, // references users
];

// Run in order
for (const migration of migrations.sort((a, b) => a.id.localeCompare(b.id))) {
  await runner.runMigration(migration);
}
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

// Handle LSN not found gracefully
try {
  await cdc.subscribe({ fromLSN: oldLSN });
} catch (error) {
  if (error.code === 'CDC_LSN_NOT_FOUND') {
    // Fall back to earliest available or do full sync
    const earliest = await cdc.getEarliestLSN();
    await cdc.subscribe({ fromLSN: earliest });
  }
}
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

// Process events in batches
const batchSize = 100;
let batch = [];
for await (const event of subscription) {
  batch.push(event);
  if (batch.length >= batchSize) {
    await processBatch(batch);
    batch = [];
    await subscription.ack(event.lsn);
  }
}
```

#### Replication Slot Issues

**Symptoms:** `CDC_SLOT_NOT_FOUND`, `CDC_SLOT_EXISTS`

**Solution:**

```typescript
// Check if slot exists before creating
const slots = await cdc.slots.list();
if (!slots.find(s => s.name === 'my-slot')) {
  await cdc.slots.createSlot('my-slot');
}

// Or use createIfNotExists
await cdc.slots.createSlot('my-slot', { ifNotExists: true });

// Clean up unused slots
await cdc.slots.dropSlot('old-slot');
```

---

### Storage Issues

#### R2 Bucket Not Bound

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

```typescript
// Access in Worker
export default {
  async fetch(request, env) {
    // Verify binding exists
    if (!env.DATA_BUCKET) {
      throw new Error('R2 bucket not bound');
    }
    // Use the bucket
    await env.DATA_BUCKET.put('key', 'value');
  }
};
```

#### R2 Rate Limits

**Symptoms:** `R2_RATE_LIMITED`, HTTP 429 errors

**Solution:**

```typescript
// Implement exponential backoff
async function withR2Retry<T>(fn: () => Promise<T>, maxRetries = 3): Promise<T> {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      if (error.r2Code === 'R2_RATE_LIMITED') {
        const delay = error.retryAfter ?? Math.pow(2, attempt) * 1000;
        await new Promise(r => setTimeout(r, delay));
        continue;
      }
      throw error;
    }
  }
  throw new Error('Max R2 retries exceeded');
}
```

#### Storage Size Exceeded

**Symptoms:** `FSX_SIZE_EXCEEDED`, `R2_SIZE_EXCEEDED`

**Solution:**

```typescript
// For files > 5GB, use multipart upload
const CHUNK_SIZE = 100 * 1024 * 1024; // 100MB chunks

async function uploadLargeFile(bucket, key, data) {
  if (data.byteLength <= 5 * 1024 * 1024 * 1024) {
    return bucket.put(key, data);
  }

  const upload = await bucket.createMultipartUpload(key);
  const parts = [];

  for (let i = 0; i * CHUNK_SIZE < data.byteLength; i++) {
    const chunk = data.slice(i * CHUNK_SIZE, (i + 1) * CHUNK_SIZE);
    const part = await upload.uploadPart(i + 1, chunk);
    parts.push(part);
  }

  return upload.complete(parts);
}
```

#### Data Corruption

**Symptoms:** `FSX_CHUNK_CORRUPTED`, `R2_CHECKSUM_MISMATCH`, `WAL_CHECKSUM_MISMATCH`

**Solution:**

```typescript
// Enable checksum verification
const storage = createStorage({
  verifyChecksums: true,
  checksumAlgorithm: 'md5',
});

// Handle corruption gracefully
try {
  const data = await storage.read('path/to/file');
} catch (error) {
  if (error.code === 'FSX_CHUNK_CORRUPTED') {
    // Attempt recovery from backup or R2
    const backup = await r2Backup.get('path/to/file');
    if (backup) {
      await storage.write('path/to/file', backup);
    }
  }
}
```

---

### Branch Issues

#### Branch Not Found

**Symptoms:** `BRANCH_NOT_FOUND` when switching branches

**Solution:**

```typescript
// List available branches first
const branches = await branchManager.listBranches();
console.log('Available branches:', branches.map(b => b.name));

// Check if branch exists before checkout
const branch = await branchManager.getBranch('feature-x');
if (!branch) {
  // Create the branch
  await branchManager.createBranch({ name: 'feature-x' });
}
await branchManager.checkout('feature-x');
```

#### Merge Conflicts

**Symptoms:** `MERGE_CONFLICT` when merging branches

**Solution:**

```typescript
try {
  await branchManager.merge('feature', 'main');
} catch (error) {
  if (error.code === 'MERGE_CONFLICT') {
    // List conflicts
    const conflicts = error.details.conflicts;

    // Resolve each conflict
    for (const conflict of conflicts) {
      // Choose resolution strategy
      await branchManager.resolveMerge(
        conflict.path,
        'theirs' // or 'ours' or provide merged content
      );
    }

    // Complete the merge
    await branchManager.commit('Merge feature into main');
  }
}
```

#### Uncommitted Changes

**Symptoms:** `UNCOMMITTED_CHANGES` when switching branches

**Solution:**

```typescript
// Check for uncommitted changes
const status = await branchManager.status();
if (status.hasChanges) {
  // Option 1: Commit changes
  await branchManager.commit('Save work in progress');

  // Option 2: Stash changes (if supported)
  await branchManager.stash();
  await branchManager.checkout('other-branch');
  await branchManager.stashPop();
}
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

#### Type Errors After Update

**Symptoms:** TypeScript errors after updating DoSQL

**Solution:**

```bash
# Clear TypeScript cache
rm -rf node_modules/.cache
rm -rf dist

# Reinstall dependencies
npm ci

# Rebuild
npm run build
```

---

## Error Messages Explained

### Database Error Codes

| Code | Meaning | Retryable | Action |
|------|---------|-----------|--------|
| `DB_CLOSED` | Database connection closed | No | Reopen database connection |
| `DB_READ_ONLY` | Database is read-only | No | Open database with write access |
| `DB_NOT_FOUND` | Database doesn't exist | No | Create database or check path |
| `DB_CONSTRAINT` | Constraint violation | No | Fix data to satisfy constraints |
| `DB_QUERY_ERROR` | Query execution failed | Maybe | Check SQL syntax and parameters |
| `DB_CONNECTION_FAILED` | Connection failed | Yes | Retry with backoff |
| `DB_CONFIG_ERROR` | Configuration error | No | Fix configuration |
| `DB_TIMEOUT` | Operation timed out | Yes | Increase timeout, retry |
| `DB_INTERNAL` | Internal error | Maybe | Contact support if persistent |

### Statement Error Codes

| Code | Meaning | Action |
|------|---------|--------|
| `STMT_FINALIZED` | Statement already closed | Create new statement with db.prepare() |
| `STMT_SYNTAX` | SQL syntax error | Fix SQL syntax |
| `STMT_EXECUTION` | Execution failed | Check query and data |
| `STMT_TABLE_NOT_FOUND` | Table doesn't exist | Create table or check name |
| `STMT_COLUMN_NOT_FOUND` | Column doesn't exist | Check column name |
| `STMT_INVALID_SQL` | Invalid SQL | Review SQL syntax |
| `STMT_UNSUPPORTED` | Unsupported operation | Use alternative approach |

### Binding Error Codes

| Code | Meaning | Action |
|------|---------|--------|
| `BIND_MISSING_PARAM` | Required parameter missing | Provide all parameters |
| `BIND_TYPE_MISMATCH` | Parameter type wrong | Check parameter types |
| `BIND_INVALID_TYPE` | Invalid type for binding | Use supported types |
| `BIND_COUNT_MISMATCH` | Parameter count wrong | Match placeholders to parameters |
| `BIND_NAMED_EXPECTED` | Named param expected | Use named parameters `:name` |

### Syntax Error Codes

| Code | Meaning | Action |
|------|---------|--------|
| `SYNTAX_UNEXPECTED_TOKEN` | Unexpected token | Check SQL at error location |
| `SYNTAX_UNEXPECTED_EOF` | Unexpected end of input | Complete the SQL statement |
| `SYNTAX_INVALID_IDENTIFIER` | Invalid identifier | Fix table/column name |
| `SYNTAX_INVALID_LITERAL` | Invalid literal value | Fix string/number literal |
| `SYNTAX_ERROR` | General syntax error | Review SQL syntax |

### Transaction Error Codes

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
| `TXN_READ_ONLY_VIOLATION` | Write in read-only txn | No | Start read-write transaction |

### R2 Storage Errors

| Code | Meaning | Retryable | Action |
|------|---------|-----------|--------|
| `R2_NOT_FOUND` | Object not found | No | Verify key exists |
| `R2_TIMEOUT` | Operation timed out | Yes | Retry with backoff |
| `R2_RATE_LIMITED` | Rate limit exceeded | Yes | Wait for retryAfter, then retry |
| `R2_PERMISSION_DENIED` | Access denied | No | Check bucket permissions |
| `R2_BUCKET_NOT_BOUND` | Bucket not configured | No | Add binding in wrangler.jsonc |
| `R2_SIZE_EXCEEDED` | Object too large | No | Use multipart upload |
| `R2_CONFLICT` | Concurrent modification | Yes | Retry with fresh etag |
| `R2_CHECKSUM_MISMATCH` | Data integrity error | Yes | Retry upload |
| `R2_READ_DURING_WRITE` | Read during ongoing write | Yes | Wait and retry |
| `R2_NETWORK_ERROR` | Network issue | Yes | Retry with backoff |

### FSX Storage Errors

| Code | Meaning | Action |
|------|---------|--------|
| `FSX_NOT_FOUND` | File not found | Check path, handle gracefully |
| `FSX_WRITE_FAILED` | Write operation failed | Check storage quota, retry |
| `FSX_READ_FAILED` | Read operation failed | Check permissions, retry |
| `FSX_DELETE_FAILED` | Delete operation failed | Check if file exists |
| `FSX_CHUNK_CORRUPTED` | Data chunk corrupted | Restore from backup |
| `FSX_SIZE_EXCEEDED` | File too large | Use streaming or chunking |
| `FSX_MIGRATION_FAILED` | Tier migration failed | Check both storage tiers |
| `FSX_INVALID_RANGE` | Invalid byte range | Check range boundaries |

### CDC Error Codes

| Code | Meaning | Action |
|------|---------|--------|
| `CDC_SUBSCRIPTION_FAILED` | Subscription setup failed | Check configuration, retry |
| `CDC_LSN_NOT_FOUND` | WAL position not available | Start from earliest or full sync |
| `CDC_SLOT_NOT_FOUND` | Replication slot missing | Create the slot first |
| `CDC_SLOT_EXISTS` | Slot already exists | Use existing slot or drop first |
| `CDC_BUFFER_OVERFLOW` | Consumer too slow | Implement batch processing |
| `CDC_DECODE_ERROR` | Event decode failed | Check event format |

### WAL Error Codes

| Code | Meaning | Action |
|------|---------|--------|
| `WAL_CHECKSUM_MISMATCH` | Checksum verification failed | Data may be corrupted |
| `WAL_SEGMENT_NOT_FOUND` | WAL segment missing | May have been compacted |
| `WAL_ENTRY_NOT_FOUND` | WAL entry not found | Entry may have been compacted |
| `WAL_INVALID_LSN` | Invalid LSN value | Check LSN ordering |
| `WAL_SEGMENT_CORRUPTED` | Segment data corrupted | Restore from backup |
| `WAL_RECOVERY_FAILED` | Recovery process failed | Check storage, contact support |
| `WAL_CHECKPOINT_FAILED` | Checkpoint failed | Retry checkpoint |
| `WAL_FLUSH_FAILED` | Flush to storage failed | Check storage availability |
| `WAL_SERIALIZATION_ERROR` | Serialization failed | Check data format |

### Branch Error Codes

| Code | Meaning | Action |
|------|---------|--------|
| `BRANCH_EXISTS` | Branch already exists | Use different name or delete existing |
| `BRANCH_NOT_FOUND` | Branch doesn't exist | Check name, create if needed |
| `CANNOT_DELETE_CURRENT` | Can't delete current branch | Switch to another branch first |
| `BRANCH_PROTECTED` | Branch is protected | Contact admin or use different branch |
| `BRANCH_ARCHIVED` | Branch is read-only | Unarchive or create new branch |
| `COMMIT_NOT_FOUND` | Commit doesn't exist | Check commit ID |
| `UNCOMMITTED_CHANGES` | Changes not committed | Commit or discard changes |
| `MERGE_CONFLICT` | Merge has conflicts | Resolve conflicts manually |
| `NOT_FAST_FORWARD` | Can't fast-forward | Use merge or rebase |
| `INVALID_BRANCH_NAME` | Invalid characters in name | Use alphanumeric and hyphens |
| `NOT_MERGED` | Branch has unmerged commits | Merge first or force delete |

### Migration Errors

| Error | Meaning | Action |
|-------|---------|--------|
| `Checksum mismatch` | Migration file modified | Never modify applied migrations |
| `Migration failed` | SQL error during migration | Check SQL syntax, use dry-run |
| `SQLITE_CONSTRAINT` | Constraint violation | Fix data before migration |
| `Table already exists` | CREATE without IF NOT EXISTS | Add IF NOT EXISTS |
| `Foreign key violation` | Referenced row missing | Check migration order |

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
    onQueryError(context, error) {
      console.error(`[QUERY ERROR] ${error.message}`, {
        sql: context.sql,
        params: context.params,
        duration: performance.now() - context.startTime,
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

# High traffic sampling
wrangler tail --sampling-rate 0.1

# Filter by specific error codes
wrangler tail --format json | jq 'select(.logs[].message | contains("TXN_DEADLOCK"))'
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
    memoryUsage: process.memoryUsage?.() ?? 'unavailable',
    activeTransactions: this.transactionManager.getActiveCount(),
  }, null, 2), {
    headers: { 'Content-Type': 'application/json' },
  });
}

// Get database statistics
async getDatabaseStats() {
  return {
    tables: await this.db.query("SELECT name FROM sqlite_master WHERE type='table'"),
    pageCount: (await this.db.query('PRAGMA page_count'))[0].page_count,
    pageSize: (await this.db.query('PRAGMA page_size'))[0].page_size,
    walMode: (await this.db.query('PRAGMA journal_mode'))[0].journal_mode,
  };
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
-- SEARCH users USING INTEGER PRIMARY KEY (rowid=?)

-- Bad output (full table scan):
-- SCAN orders
-- SCAN users

-- Check index usage
SELECT * FROM sqlite_stat1;

-- Analyze tables to update statistics
ANALYZE;
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

// Usage
async fetch(request: Request) {
  const trace = createTraceContext(request);
  logWithTrace(trace, 'request_start', { path: new URL(request.url).pathname });

  try {
    const result = await this.handleRequest(request, trace);
    logWithTrace(trace, 'request_end', { status: 200 });
    return result;
  } catch (error) {
    logWithTrace(trace, 'request_error', {
      error: error.message,
      code: error.code,
    });
    throw error;
  }
}
```

### Error Inspection Utilities

```typescript
import { isDoSQLError, getErrorCode, isRetryable } from '@dotdo/dosql/errors';

// Check if error is from DoSQL
if (isDoSQLError(error)) {
  console.log('DoSQL error:', error.code);
  console.log('Category:', error.category);
  console.log('Recovery hint:', error.recoveryHint);
  console.log('Retryable:', error.isRetryable());
  console.log('User message:', error.toUserMessage());
}

// Generic retry wrapper
async function withRetry<T>(
  fn: () => Promise<T>,
  options: { maxRetries?: number; baseDelay?: number } = {}
): Promise<T> {
  const { maxRetries = 3, baseDelay = 100 } = options;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      const retryable = isDoSQLError(error) && error.isRetryable();

      if (!retryable || attempt === maxRetries - 1) {
        throw error;
      }

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

**Q: What is the memory limit for Durable Objects?**

A: 128MB. Use streaming for large datasets and tiered storage (DO + R2) for large databases.

**Q: How long before a DO hibernates?**

A: Approximately 10 seconds of no incoming requests. Use WebSocket connections with heartbeats to keep DOs warm.

**Q: What's the maximum request time for a DO?**

A: 30 seconds CPU time per request. Long-running operations should be broken into smaller chunks.

**Q: What SQLite features are supported?**

A: DoSQL supports most SQLite features including:
- Full SQL syntax (SELECT, INSERT, UPDATE, DELETE, etc.)
- Transactions with ACID guarantees
- Indexes and constraints
- JSON functions
- Common Table Expressions (CTEs)
- Window functions
- RETURNING clauses

Not supported: ATTACH DATABASE, loadable extensions.

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

**Q: How many concurrent connections can a DO handle?**

A: There's no hard limit, but each WebSocket connection consumes memory. Monitor memory usage and consider connection pooling for high-concurrency scenarios.

### Queries

**Q: How do I prevent SQL injection?**

A: Always use parameterized queries:
```typescript
// WRONG: Vulnerable to SQL injection
db.query(`SELECT * FROM users WHERE name = '${userInput}'`);

// CORRECT: Parameterized query - safe from injection
db.query('SELECT * FROM users WHERE name = ?', [userInput]);
```

**Q: Why is my query slow?**

A: Check for missing indexes:
```sql
EXPLAIN QUERY PLAN SELECT * FROM orders WHERE user_id = 42;
-- If you see "SCAN", add an index:
CREATE INDEX idx_orders_user_id ON orders(user_id);
```

**Q: What's the maximum query result size?**

A: Limited by DO memory (128MB total). For large results:
- Use LIMIT/OFFSET or cursor pagination
- Stream results with queryStream()
- Select only needed columns

**Q: Can I use multiple databases in one DO?**

A: Each DO instance has one primary database. For multi-tenant scenarios, use separate DO instances per tenant.

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

**Q: What isolation level does DoSQL use?**

A: SERIALIZABLE by default. You can configure READ COMMITTED for better concurrency at the cost of isolation.

**Q: Can I nest transactions?**

A: Use savepoints for nested transaction semantics:
```typescript
await db.transaction(async (tx) => {
  await tx.savepoint('sp1');
  try {
    await tx.run('...');
  } catch (e) {
    await tx.rollbackTo('sp1');
  }
});
```

### Migrations

**Q: Can I modify an applied migration?**

A: No. Never modify applied migrations. Create a new corrective migration instead.

**Q: How do I rollback a migration?**

A: Define `downSql` in your migrations and use the rollback function, or restore from a backup:
```typescript
const migration = {
  id: '001',
  upSql: 'CREATE TABLE users ...',
  downSql: 'DROP TABLE users',
};
await runner.rollback(migration);
```

**Q: How do I run migrations in production safely?**

A:
1. Test migrations in staging first
2. Use dry-run mode to validate
3. Take a backup before migrating
4. Run migrations during low-traffic periods

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

**Q: What's the event ordering guarantee?**

A: Events are delivered in LSN order within a single DO. Across DOs, use vector clocks or event timestamps for ordering.

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

**Q: How do I debug in production?**

A:
1. Use `wrangler tail` for real-time logs
2. Add debug endpoints (protected by auth)
3. Include trace IDs in all logs
4. Use structured logging (JSON format)

**Q: How do I handle DO version migrations?**

A: Use the migration tag system in wrangler.jsonc:
```jsonc
{
  "migrations": [
    { "tag": "v1", "new_classes": ["MyDO"] },
    { "tag": "v2", "renamed_classes": [{ "from": "MyDO", "to": "MyDOv2" }] }
  ]
}
```

---

## Getting Help

If you're still experiencing issues:

1. **Check the logs**: Use `wrangler tail` to view real-time logs
2. **Enable debug logging**: Set `logger: new ConsoleLogger({ level: LogLevel.DEBUG })`
3. **Review recent changes**: Check git history for related modifications
4. **Inspect error details**: Use `error.code`, `error.recoveryHint`, `error.toUserMessage()`
5. **Search existing issues**: Check the GitHub repository for similar problems
6. **Open a GitHub issue** with:
   - DoSQL version (`npm ls @dotdo/dosql`)
   - Minimal reproduction code
   - Error messages and stack traces (including error codes)
   - Wrangler configuration (sanitized)
   - Steps to reproduce

## Related Documentation

- [Getting Started](./getting-started.md) - Basic setup and usage
- [API Reference](./api-reference.md) - Complete API documentation
- [Architecture](./architecture.md) - Understanding DoSQL internals
- [Advanced Features](./advanced.md) - Time travel, branching, CDC
