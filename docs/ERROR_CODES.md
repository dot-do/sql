# Error Codes Reference

This document provides a comprehensive reference for all error codes used across the DoSQL and DoLake packages.

## Table of Contents

- [Query Errors](#query-errors)
- [Transaction Errors](#transaction-errors)
- [Connection Errors](#connection-errors)
- [Authentication Errors](#authentication-errors)
- [Rate Limiting Errors](#rate-limiting-errors)
- [CDC/Streaming Errors](#cdcstreaming-errors)
- [Schema Errors](#schema-errors)
- [Storage Errors](#storage-errors)
- [Internal Errors](#internal-errors)
- [Error Handling Examples](#error-handling-examples)

---

## Query Errors

### SYNTAX_ERROR

| Property | Value |
|----------|-------|
| Code | `SYNTAX_ERROR` |
| HTTP Status | 400 Bad Request |
| Retryable | No |

**Description:** The SQL query contains a syntax error and cannot be parsed.

**Common Causes:**
- Misspelled SQL keywords
- Missing or mismatched parentheses
- Invalid SQL syntax for the dialect

**Example Error Message:**
```
Syntax error near 'FORM' at position 12 - did you mean 'FROM'?
```

**Resolution Steps:**
1. Review the SQL query for typos
2. Validate SQL syntax against supported dialect
3. Use parameterized queries to avoid escaping issues

---

### TABLE_NOT_FOUND

| Property | Value |
|----------|-------|
| Code | `TABLE_NOT_FOUND` |
| HTTP Status | 404 Not Found |
| Retryable | No |

**Description:** The referenced table does not exist in the database.

**Common Causes:**
- Table name is misspelled
- Table was dropped or never created
- Querying wrong branch/database

**Example Error Message:**
```
Table 'users' not found in database
```

**Resolution Steps:**
1. Verify the table name spelling
2. Check if the table exists: `SELECT name FROM sqlite_master WHERE type='table'`
3. Ensure you're connected to the correct branch

---

### COLUMN_NOT_FOUND

| Property | Value |
|----------|-------|
| Code | `COLUMN_NOT_FOUND` |
| HTTP Status | 400 Bad Request |
| Retryable | No |

**Description:** The referenced column does not exist in the table.

**Common Causes:**
- Column name is misspelled
- Column was removed in a schema migration
- Using wrong table alias

**Example Error Message:**
```
Column 'email_address' not found in table 'users'
```

**Resolution Steps:**
1. Verify column names using `getSchema()` or `PRAGMA table_info(table_name)`
2. Check for schema changes
3. Verify table aliases in complex queries

---

### CONSTRAINT_VIOLATION

| Property | Value |
|----------|-------|
| Code | `CONSTRAINT_VIOLATION` |
| HTTP Status | 409 Conflict |
| Retryable | No |

**Description:** An operation violated a database constraint.

**Common Causes:**
- Duplicate primary key or unique constraint violation
- Foreign key constraint violation
- NOT NULL constraint violation
- CHECK constraint failure

**Example Error Message:**
```
UNIQUE constraint failed: users.email
```

**Resolution Steps:**
1. Check for existing records with the same unique values
2. Ensure foreign key references exist
3. Provide values for NOT NULL columns
4. Validate data against CHECK constraints

---

### TYPE_MISMATCH

| Property | Value |
|----------|-------|
| Code | `TYPE_MISMATCH` |
| HTTP Status | 400 Bad Request |
| Retryable | No |

**Description:** A value provided does not match the expected column type.

**Common Causes:**
- Inserting string into integer column
- Date format mismatch
- JSON type validation failure

**Example Error Message:**
```
Cannot insert 'abc' into column 'age' of type INTEGER
```

**Resolution Steps:**
1. Verify the data types of your parameters
2. Use appropriate type casting
3. Check date/timestamp formats

---

## Transaction Errors

### TRANSACTION_NOT_FOUND

| Property | Value |
|----------|-------|
| Code | `TRANSACTION_NOT_FOUND` |
| HTTP Status | 404 Not Found |
| Retryable | No |

**Description:** The specified transaction ID does not exist or has expired.

**Common Causes:**
- Transaction was already committed or rolled back
- Transaction timed out
- Invalid transaction ID

**Example Error Message:**
```
Transaction 'tx_abc123' not found or already completed
```

**Resolution Steps:**
1. Check if the transaction was already completed
2. Increase transaction timeout if needed
3. Implement proper transaction state tracking

---

### TRANSACTION_ABORTED

| Property | Value |
|----------|-------|
| Code | `TRANSACTION_ABORTED` / `TXN_ABORTED` |
| HTTP Status | 409 Conflict |
| Retryable | Yes |

**Description:** The transaction was aborted due to a conflict or error.

**Common Causes:**
- Concurrent modification conflict
- Error during transaction execution
- Manual abort

**Example Error Message:**
```
Transaction aborted due to concurrent modification
```

**Resolution Steps:**
1. Retry the entire transaction
2. Implement optimistic concurrency control
3. Reduce transaction duration to minimize conflicts

---

### DEADLOCK_DETECTED

| Property | Value |
|----------|-------|
| Code | `DEADLOCK_DETECTED` / `TXN_DEADLOCK` |
| HTTP Status | 409 Conflict |
| Retryable | Yes |

**Description:** A deadlock was detected between two or more transactions.

**Common Causes:**
- Circular lock dependencies
- Long-running transactions holding locks
- Improper lock ordering

**Example Error Message:**
```
Deadlock detected: Transaction tx_001 waiting on tx_002, tx_002 waiting on tx_001
```

**Resolution Steps:**
1. Retry the transaction (one will be chosen as the victim)
2. Implement consistent lock ordering across transactions
3. Reduce transaction scope and duration
4. Use appropriate isolation levels

---

### SERIALIZATION_FAILURE

| Property | Value |
|----------|-------|
| Code | `SERIALIZATION_FAILURE` / `TXN_SERIALIZATION_FAILURE` |
| HTTP Status | 409 Conflict |
| Retryable | Yes |

**Description:** Transaction could not be committed due to concurrent modifications (MVCC conflict).

**Common Causes:**
- Read-write conflict under SERIALIZABLE isolation
- Concurrent updates to the same rows

**Example Error Message:**
```
Could not serialize access due to concurrent update
```

**Resolution Steps:**
1. Retry the transaction with exponential backoff
2. Consider using lower isolation level if acceptable
3. Reduce transaction scope

---

### TXN_NO_ACTIVE

| Property | Value |
|----------|-------|
| Code | `TXN_NO_ACTIVE` |
| HTTP Status | 400 Bad Request |
| Retryable | No |

**Description:** Attempted to perform a transaction operation without an active transaction.

**Common Causes:**
- Calling commit/rollback without begin
- Transaction already ended

**Example Error Message:**
```
No active transaction to commit
```

**Resolution Steps:**
1. Ensure `beginTransaction()` is called before transaction operations
2. Check transaction state before operations

---

### TXN_ALREADY_ACTIVE

| Property | Value |
|----------|-------|
| Code | `TXN_ALREADY_ACTIVE` |
| HTTP Status | 400 Bad Request |
| Retryable | No |

**Description:** Attempted to begin a transaction when one is already active.

**Example Error Message:**
```
Transaction already in progress
```

**Resolution Steps:**
1. Commit or rollback the existing transaction first
2. Use savepoints for nested transaction-like behavior

---

### TXN_SAVEPOINT_NOT_FOUND

| Property | Value |
|----------|-------|
| Code | `TXN_SAVEPOINT_NOT_FOUND` |
| HTTP Status | 404 Not Found |
| Retryable | No |

**Description:** The specified savepoint does not exist.

**Example Error Message:**
```
Savepoint 'sp_1' not found
```

**Resolution Steps:**
1. Verify savepoint name
2. Check if savepoint was already released

---

### TXN_LOCK_TIMEOUT

| Property | Value |
|----------|-------|
| Code | `TXN_LOCK_TIMEOUT` |
| HTTP Status | 408 Request Timeout |
| Retryable | Yes |

**Description:** Failed to acquire a lock within the timeout period.

**Example Error Message:**
```
Lock acquisition timed out after 30000ms
```

**Resolution Steps:**
1. Retry with exponential backoff
2. Increase lock timeout configuration
3. Identify and resolve blocking transactions

---

### TXN_READ_ONLY_VIOLATION

| Property | Value |
|----------|-------|
| Code | `TXN_READ_ONLY_VIOLATION` |
| HTTP Status | 403 Forbidden |
| Retryable | No |

**Description:** Attempted write operation in a read-only transaction.

**Example Error Message:**
```
Cannot execute INSERT in read-only transaction
```

**Resolution Steps:**
1. Use a read-write transaction for write operations
2. Remove the readOnly option from transaction options

---

## Connection Errors

### CONNECTION_ERROR

| Property | Value |
|----------|-------|
| Code | `CONNECTION_ERROR` |
| HTTP Status | 503 Service Unavailable |
| Retryable | Yes |

**Description:** Failed to establish or maintain a connection.

**Common Causes:**
- Network issues
- Server unavailable
- WebSocket connection dropped

**Example Error Message:**
```
WebSocket connection closed unexpectedly
```

**Resolution Steps:**
1. Implement automatic reconnection with backoff
2. Check network connectivity
3. Verify server health

---

### TIMEOUT

| Property | Value |
|----------|-------|
| Code | `TIMEOUT` |
| HTTP Status | 408 Request Timeout |
| Retryable | Yes |

**Description:** Operation timed out before completion.

**Common Causes:**
- Query execution took too long
- Network latency
- Server overload

**Example Error Message:**
```
Request timeout: query exceeded 30000ms
```

**Resolution Steps:**
1. Increase timeout configuration
2. Optimize the query
3. Check server health and load

---

## Authentication Errors

### UNAUTHORIZED

| Property | Value |
|----------|-------|
| Code | `UNAUTHORIZED` |
| HTTP Status | 401 Unauthorized |
| Retryable | No |

**Description:** Request lacks valid authentication credentials.

**Common Causes:**
- Missing or invalid token
- Token expired
- Invalid API key

**Example Error Message:**
```
Authentication required
```

**Resolution Steps:**
1. Provide valid authentication token
2. Refresh expired tokens
3. Verify API key configuration

---

### FORBIDDEN

| Property | Value |
|----------|-------|
| Code | `FORBIDDEN` |
| HTTP Status | 403 Forbidden |
| Retryable | No |

**Description:** User is authenticated but not authorized for this operation.

**Common Causes:**
- Insufficient permissions
- Resource access denied
- IP not allowlisted

**Example Error Message:**
```
Access denied to table 'admin_logs'
```

**Resolution Steps:**
1. Request appropriate permissions
2. Verify user roles and access policies
3. Check IP allowlist configuration

---

## Rate Limiting Errors

### RATE_LIMITED / R2_RATE_LIMITED

| Property | Value |
|----------|-------|
| Code | `rate_limited` / `R2_RATE_LIMITED` |
| HTTP Status | 429 Too Many Requests |
| Retryable | Yes |

**Description:** Request rate limit exceeded.

**Common Causes:**
- Too many requests in a short period
- Exceeding API quota
- Buffer full (for streaming)

**Example Error Message:**
```json
{
  "error": "Too Many Requests",
  "retryAfter": 5,
  "message": "Rate limit exceeded. Retry after 5 seconds."
}
```

**Resolution Steps:**
1. Implement exponential backoff
2. Respect `Retry-After` header
3. Reduce request frequency
4. Request quota increase if needed

---

### RESOURCE_EXHAUSTED

| Property | Value |
|----------|-------|
| Code | `RESOURCE_EXHAUSTED` |
| HTTP Status | 429 Too Many Requests |
| Retryable | Yes |

**Description:** Server resources are exhausted.

**Example Error Message:**
```
Server resources exhausted, please retry later
```

**Resolution Steps:**
1. Wait and retry with backoff
2. Reduce concurrent connections
3. Optimize query complexity

---

### QUOTA_EXCEEDED

| Property | Value |
|----------|-------|
| Code | `QUOTA_EXCEEDED` |
| HTTP Status | 429 Too Many Requests |
| Retryable | No (until quota resets) |

**Description:** Account quota has been exceeded.

**Example Error Message:**
```
Storage quota exceeded (10GB limit)
```

**Resolution Steps:**
1. Clean up unused data
2. Upgrade account tier
3. Wait for quota reset period

---

## CDC/Streaming Errors

### CDC_SUBSCRIPTION_FAILED

| Property | Value |
|----------|-------|
| Code | `CDC_SUBSCRIPTION_FAILED` |
| HTTP Status | 500 Internal Server Error |
| Retryable | Yes |

**Description:** Failed to start CDC subscription.

**Common Causes:**
- Invalid subscription parameters
- WAL reader unavailable
- Internal error

**Example Error Message:**
```
Failed to subscribe to CDC stream: WAL reader not initialized
```

**Resolution Steps:**
1. Verify subscription parameters
2. Check WAL availability
3. Retry subscription

---

### CDC_LSN_NOT_FOUND

| Property | Value |
|----------|-------|
| Code | `CDC_LSN_NOT_FOUND` |
| HTTP Status | 410 Gone |
| Retryable | No |

**Description:** The requested LSN is no longer available (compacted/deleted).

**Common Causes:**
- LSN is too old
- WAL was compacted
- Invalid LSN value

**Example Error Message:**
```
LSN 1000 not found - oldest available LSN is 5000
```

**Resolution Steps:**
1. Start from the oldest available LSN
2. Use more frequent checkpoints
3. Implement fallback to full sync

---

### CDC_SLOT_NOT_FOUND

| Property | Value |
|----------|-------|
| Code | `CDC_SLOT_NOT_FOUND` |
| HTTP Status | 404 Not Found |
| Retryable | No |

**Description:** The replication slot does not exist.

**Example Error Message:**
```
Replication slot 'my_slot' not found
```

**Resolution Steps:**
1. Create the replication slot first
2. Verify slot name

---

### CDC_SLOT_EXISTS

| Property | Value |
|----------|-------|
| Code | `CDC_SLOT_EXISTS` |
| HTTP Status | 409 Conflict |
| Retryable | No |

**Description:** A replication slot with this name already exists.

**Example Error Message:**
```
Replication slot 'my_slot' already exists
```

**Resolution Steps:**
1. Use a different slot name
2. Delete existing slot if no longer needed

---

### CDC_BUFFER_OVERFLOW

| Property | Value |
|----------|-------|
| Code | `CDC_BUFFER_OVERFLOW` / `BUFFER_OVERFLOW` |
| HTTP Status | 503 Service Unavailable |
| Retryable | Yes |

**Description:** CDC buffer is full, cannot accept more events.

**Common Causes:**
- Consumer not processing fast enough
- High event volume
- Buffer size too small

**Example Error Message:**
```
CDC buffer full (1000/1000 entries) - consumer lagging
```

**Resolution Steps:**
1. Increase buffer size
2. Speed up consumer processing
3. Implement backpressure handling

---

### CDC_DECODE_ERROR

| Property | Value |
|----------|-------|
| Code | `CDC_DECODE_ERROR` |
| HTTP Status | 400 Bad Request |
| Retryable | No |

**Description:** Failed to decode CDC event data.

**Example Error Message:**
```
Failed to decode event data: Invalid JSON
```

**Resolution Steps:**
1. Check data encoding
2. Verify decoder function
3. Handle schema evolution

---

### buffer_full

| Property | Value |
|----------|-------|
| Code | `buffer_full` |
| HTTP Status | 503 Service Unavailable |
| Retryable | Yes |

**Description:** DoLake buffer is full and cannot accept more CDC batches.

**Example NACK Message:**
```json
{
  "type": "nack",
  "sequenceNumber": 42,
  "reason": "buffer_full",
  "errorMessage": "Buffer at 100% capacity",
  "shouldRetry": true,
  "retryDelayMs": 1000
}
```

**Resolution Steps:**
1. Wait for the suggested retry delay
2. Reduce batch sizes
3. Implement backpressure in the sender

---

### invalid_sequence

| Property | Value |
|----------|-------|
| Code | `invalid_sequence` |
| HTTP Status | 400 Bad Request |
| Retryable | No |

**Description:** Batch sequence number is out of order or invalid.

**Example Error Message:**
```
Expected sequence 10, received 15
```

**Resolution Steps:**
1. Resync from last acknowledged sequence
2. Check for network issues causing out-of-order delivery

---

### payload_too_large / event_too_large

| Property | Value |
|----------|-------|
| Code | `payload_too_large` / `event_too_large` |
| HTTP Status | 413 Payload Too Large |
| Retryable | No |

**Description:** Message or event exceeds size limits.

**Example Error Message:**
```
Batch size 5MB exceeds maximum 4MB
```

**Resolution Steps:**
1. Reduce batch size
2. Split large events
3. Check maxMessageSize configuration

---

## Schema Errors

### INVALID_REQUEST

| Property | Value |
|----------|-------|
| Code | `INVALID_REQUEST` |
| HTTP Status | 400 Bad Request |
| Retryable | No |

**Description:** The request format is invalid.

**Common Causes:**
- Malformed JSON
- Missing required fields
- Invalid field types

**Example Error Message:**
```
Invalid message format: Missing required field 'sql'
```

**Resolution Steps:**
1. Validate request against schema
2. Check required fields
3. Verify data types

---

### MessageValidationError

| Property | Value |
|----------|-------|
| Code | N/A (Exception type) |
| HTTP Status | 400 Bad Request |
| Retryable | No |

**Description:** Zod schema validation failed for RPC message.

**Common Causes:**
- Invalid message type
- Missing required fields
- Type mismatches

**Example Error Message:**
```
Invalid CDC batch message: events must be an array, timestamp is required
```

**Resolution Steps:**
1. Review message against schema
2. Check field types
3. Use `getErrorDetails()` method for specific issues

---

## Storage Errors

### FSX_NOT_FOUND

| Property | Value |
|----------|-------|
| Code | `FSX_NOT_FOUND` |
| HTTP Status | 404 Not Found |
| Retryable | No |

**Description:** Requested file not found in storage.

**Example Error Message:**
```
File 'data/table1/part-00001.parquet' not found
```

**Resolution Steps:**
1. Verify file path
2. Check if file was deleted or moved

---

### FSX_WRITE_FAILED

| Property | Value |
|----------|-------|
| Code | `FSX_WRITE_FAILED` |
| HTTP Status | 500 Internal Server Error |
| Retryable | Yes |

**Description:** Failed to write data to storage.

**Example Error Message:**
```
Write failed for path 'wal/000001.wal': I/O error
```

**Resolution Steps:**
1. Retry the write operation
2. Check storage availability
3. Verify permissions

---

### FSX_READ_FAILED

| Property | Value |
|----------|-------|
| Code | `FSX_READ_FAILED` |
| HTTP Status | 500 Internal Server Error |
| Retryable | Yes |

**Description:** Failed to read data from storage.

**Resolution Steps:**
1. Retry the read operation
2. Check file integrity
3. Verify storage connectivity

---

### FSX_CHUNK_CORRUPTED

| Property | Value |
|----------|-------|
| Code | `FSX_CHUNK_CORRUPTED` |
| HTTP Status | 500 Internal Server Error |
| Retryable | No |

**Description:** Data chunk integrity check failed.

**Example Error Message:**
```
Chunk checksum mismatch for 'data/chunk_001'
```

**Resolution Steps:**
1. Attempt recovery from backup
2. Rebuild corrupted data from WAL
3. Report for investigation

---

### FSX_SIZE_EXCEEDED

| Property | Value |
|----------|-------|
| Code | `FSX_SIZE_EXCEEDED` |
| HTTP Status | 413 Payload Too Large |
| Retryable | No |

**Description:** File or data size exceeds storage limits.

**Example Error Message:**
```
File size 5GB exceeds maximum 2MB for DO storage
```

**Resolution Steps:**
1. Use chunked storage for large files
2. Route to R2 for large objects
3. Compress data if possible

---

### FSX_MIGRATION_FAILED

| Property | Value |
|----------|-------|
| Code | `FSX_MIGRATION_FAILED` |
| HTTP Status | 500 Internal Server Error |
| Retryable | Yes |

**Description:** Failed to migrate data between storage tiers.

**Resolution Steps:**
1. Retry migration
2. Check both storage tiers
3. Verify network connectivity

---

### R2_TIMEOUT

| Property | Value |
|----------|-------|
| Code | `R2_TIMEOUT` |
| HTTP Status | 408 Request Timeout |
| Retryable | Yes |

**Description:** R2 operation timed out.

**Example User Message:**
```
Operation timed out for 'warehouse/table/data.parquet'. Please try again.
```

**Resolution Steps:**
1. Retry with exponential backoff
2. Check network conditions
3. Consider smaller operations

---

### R2_PERMISSION_DENIED

| Property | Value |
|----------|-------|
| Code | `R2_PERMISSION_DENIED` |
| HTTP Status | 403 Forbidden |
| Retryable | No |

**Description:** R2 access denied.

**Example User Message:**
```
Access denied for 'private/data.parquet'.
```

**Resolution Steps:**
1. Verify R2 bucket permissions
2. Check IAM configuration
3. Verify bucket policy

---

### R2_BUCKET_NOT_BOUND

| Property | Value |
|----------|-------|
| Code | `R2_BUCKET_NOT_BOUND` |
| HTTP Status | 500 Internal Server Error |
| Retryable | No |

**Description:** R2 bucket binding not configured.

**Example User Message:**
```
Storage bucket is not configured. Check your wrangler.toml bindings.
```

**Resolution Steps:**
1. Add R2 bucket binding to `wrangler.toml`
2. Deploy updated configuration
3. Verify environment variables

---

### R2_CHECKSUM_MISMATCH

| Property | Value |
|----------|-------|
| Code | `R2_CHECKSUM_MISMATCH` |
| HTTP Status | 500 Internal Server Error |
| Retryable | Yes |

**Description:** Data integrity check failed.

**Example User Message:**
```
Data integrity error for 'data.parquet'. The file may be corrupted.
```

**Resolution Steps:**
1. Retry the operation
2. Re-upload corrupted file
3. Investigate data corruption source

---

### R2_CONFLICT

| Property | Value |
|----------|-------|
| Code | `R2_CONFLICT` |
| HTTP Status | 409 Conflict |
| Retryable | Yes |

**Description:** Concurrent write conflict on R2.

**Example User Message:**
```
Write conflict for 'manifest.json'. Please retry.
```

**Resolution Steps:**
1. Retry with exponential backoff
2. Use conditional writes with ETags
3. Implement proper locking

---

## Internal Errors

### INTERNAL_ERROR

| Property | Value |
|----------|-------|
| Code | `INTERNAL_ERROR` |
| HTTP Status | 500 Internal Server Error |
| Retryable | Sometimes |

**Description:** An unexpected internal error occurred.

**Example Error Message:**
```
Internal error: Unexpected state in query executor
```

**Resolution Steps:**
1. Retry the operation
2. Check logs for details
3. Report issue if persistent

---

### UNKNOWN

| Property | Value |
|----------|-------|
| Code | `UNKNOWN` |
| HTTP Status | 500 Internal Server Error |
| Retryable | Sometimes |

**Description:** An unknown error occurred.

**Resolution Steps:**
1. Check error details for more information
2. Retry with logging enabled
3. Report issue with full context

---

### COMPACTION_ERROR

| Property | Value |
|----------|-------|
| Code | `COMPACTION_ERROR` |
| HTTP Status | 500 Internal Server Error |
| Retryable | Yes |

**Description:** Error during Parquet file compaction.

**Example Error Message:**
```
Compaction failed: Unable to merge files
```

**Resolution Steps:**
1. Retry compaction later
2. Check available disk space
3. Verify file integrity

---

### ICEBERG_ERROR

| Property | Value |
|----------|-------|
| Code | `ICEBERG_ERROR` |
| HTTP Status | Varies |
| Retryable | No |

**Description:** Error in Iceberg table operations.

**Common Sub-errors:**
- Namespace already exists
- Table not found
- Table already exists
- Data file not found
- Metadata file not found

**Resolution Steps:**
1. Check namespace/table existence
2. Verify Iceberg metadata consistency
3. Use appropriate API for operation

---

### PARQUET_WRITE_ERROR

| Property | Value |
|----------|-------|
| Code | `PARQUET_WRITE_ERROR` |
| HTTP Status | 500 Internal Server Error |
| Retryable | Yes |

**Description:** Failed to write Parquet file.

**Example Error Message:**
```
Cannot write empty event list
```

**Resolution Steps:**
1. Verify data is not empty
2. Check schema compatibility
3. Retry the write operation

---

### TXN_WAL_FAILURE

| Property | Value |
|----------|-------|
| Code | `TXN_WAL_FAILURE` |
| HTTP Status | 500 Internal Server Error |
| Retryable | Yes |

**Description:** WAL (Write-Ahead Log) operation failed.

**Resolution Steps:**
1. Retry the transaction
2. Check WAL storage health
3. Verify disk space

---

### TXN_ROLLBACK_FAILED

| Property | Value |
|----------|-------|
| Code | `TXN_ROLLBACK_FAILED` |
| HTTP Status | 500 Internal Server Error |
| Retryable | No |

**Description:** Failed to rollback transaction.

**Resolution Steps:**
1. Investigate error cause
2. May require manual intervention
3. Check transaction log integrity

---

## Error Handling Examples

### TypeScript/JavaScript Client

#### Handling SQL Errors

```typescript
import { createSQLClient, SQLError } from '@dotdo/sql.do';

const client = createSQLClient({ url: 'wss://dosql.example.com' });

async function executeQuery(sql: string) {
  try {
    const result = await client.query(sql);
    return result;
  } catch (error) {
    if (error instanceof SQLError) {
      switch (error.code) {
        case 'SYNTAX_ERROR':
          console.error('Invalid SQL syntax:', error.message);
          break;
        case 'TABLE_NOT_FOUND':
          console.error('Table does not exist:', error.message);
          break;
        case 'CONSTRAINT_VIOLATION':
          console.error('Constraint violation:', error.message);
          break;
        case 'TIMEOUT':
          // Retryable error
          console.log('Query timed out, retrying...');
          return executeQueryWithRetry(sql);
        default:
          console.error('SQL error:', error.code, error.message);
      }
    }
    throw error;
  }
}
```

#### Handling Transaction Errors with Retry

```typescript
async function executeInTransaction<T>(
  fn: (tx: TransactionContext) => Promise<T>,
  maxRetries = 3
): Promise<T> {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await client.transaction(fn);
    } catch (error) {
      if (error instanceof SQLError) {
        // Check if error is retryable
        const retryableCodes = [
          'DEADLOCK_DETECTED',
          'SERIALIZATION_FAILURE',
          'TRANSACTION_ABORTED',
          'TIMEOUT',
        ];

        if (retryableCodes.includes(error.code) && attempt < maxRetries) {
          // Exponential backoff
          const delay = Math.min(100 * Math.pow(2, attempt), 5000);
          console.log(`Retrying transaction (attempt ${attempt + 1}) after ${delay}ms`);
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        }
      }
      throw error;
    }
  }
  throw new Error('Max retries exceeded');
}
```

#### Handling Lake Errors

```typescript
import { createLakeClient, LakeError } from '@dotdo/lake.do';

const lakeClient = createLakeClient({ url: 'wss://dolake.example.com' });

async function queryLakehouse(sql: string) {
  try {
    const result = await lakeClient.query(sql);
    return result;
  } catch (error) {
    if (error instanceof LakeError) {
      switch (error.code) {
        case 'CONNECTION_ERROR':
          console.error('Failed to connect to lakehouse');
          break;
        case 'BUFFER_OVERFLOW':
          console.log('Buffer full, implementing backpressure');
          await new Promise(r => setTimeout(r, 1000));
          return queryLakehouse(sql);
        default:
          console.error('Lake error:', error.code, error.message);
      }
    }
    throw error;
  }
}
```

#### Handling CDC Stream Errors

```typescript
async function subscribeToCDC(fromLSN: bigint) {
  try {
    for await (const batch of lakeClient.subscribe({ fromLSN })) {
      await processBatch(batch);
    }
  } catch (error) {
    if (error instanceof LakeError) {
      switch (error.code) {
        case 'CDC_LSN_NOT_FOUND':
          // LSN too old, start from beginning
          console.log('LSN not found, restarting from earliest');
          return subscribeToCDC(0n);
        case 'CDC_BUFFER_OVERFLOW':
          // Consumer lagging, slow down
          console.log('Buffer overflow, pausing consumer');
          await new Promise(r => setTimeout(r, 5000));
          return subscribeToCDC(fromLSN);
        default:
          throw error;
      }
    }
  }
}
```

### Rate Limiting with Backoff

```typescript
async function sendWithRetry<T>(
  fn: () => Promise<T>,
  maxRetries = 5
): Promise<T> {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      if (error instanceof Error) {
        // Check for rate limiting
        if (error.message.includes('429') ||
            error.message.includes('rate limit') ||
            (error as any).code === 'rate_limited') {

          // Get retry delay from error or use exponential backoff
          const retryAfter = (error as any).retryAfter ??
            Math.min(1000 * Math.pow(2, attempt), 30000);

          console.log(`Rate limited, waiting ${retryAfter}ms`);
          await new Promise(r => setTimeout(r, retryAfter));
          continue;
        }
      }
      throw error;
    }
  }
  throw new Error('Max retries exceeded due to rate limiting');
}
```

### Handling NACK Messages in DoLake Streaming

```typescript
import type { NackMessage, AckMessage } from '@dotdo/dolake';

function handleDoLakeResponse(message: AckMessage | NackMessage) {
  if (message.type === 'nack') {
    const nack = message as NackMessage;

    switch (nack.reason) {
      case 'buffer_full':
        // Pause sending, wait for retry delay
        console.log(`Buffer full, pausing for ${nack.retryDelayMs}ms`);
        break;

      case 'rate_limited':
        // Implement backpressure
        console.log('Rate limited, reducing send rate');
        break;

      case 'invalid_sequence':
        // Resync required
        console.log('Sequence invalid, need to resync');
        break;

      case 'invalid_format':
        // Bug in sender, don't retry
        console.error('Invalid message format:', nack.errorMessage);
        break;

      case 'internal_error':
        // Retry with backoff
        if (nack.shouldRetry) {
          console.log('Internal error, will retry');
        }
        break;

      case 'payload_too_large':
      case 'event_too_large':
        // Need to split messages
        console.error('Message too large:', nack.maxSize);
        break;
    }

    return { shouldRetry: nack.shouldRetry, delayMs: nack.retryDelayMs };
  }

  return { shouldRetry: false };
}
```

---

## Error Code Quick Reference

| Category | Code | HTTP Status | Retryable |
|----------|------|-------------|-----------|
| **Query** | SYNTAX_ERROR | 400 | No |
| | TABLE_NOT_FOUND | 404 | No |
| | COLUMN_NOT_FOUND | 400 | No |
| | CONSTRAINT_VIOLATION | 409 | No |
| | TYPE_MISMATCH | 400 | No |
| **Transaction** | TRANSACTION_NOT_FOUND | 404 | No |
| | DEADLOCK_DETECTED | 409 | Yes |
| | SERIALIZATION_FAILURE | 409 | Yes |
| | TXN_LOCK_TIMEOUT | 408 | Yes |
| **Connection** | CONNECTION_ERROR | 503 | Yes |
| | TIMEOUT | 408 | Yes |
| **Auth** | UNAUTHORIZED | 401 | No |
| | FORBIDDEN | 403 | No |
| **Rate Limit** | rate_limited | 429 | Yes |
| | RESOURCE_EXHAUSTED | 429 | Yes |
| | QUOTA_EXCEEDED | 429 | No |
| **CDC** | CDC_LSN_NOT_FOUND | 410 | No |
| | CDC_BUFFER_OVERFLOW | 503 | Yes |
| **Storage** | FSX_NOT_FOUND | 404 | No |
| | FSX_WRITE_FAILED | 500 | Yes |
| | R2_TIMEOUT | 408 | Yes |
| | R2_RATE_LIMITED | 429 | Yes |
| **Internal** | INTERNAL_ERROR | 500 | Sometimes |
| | COMPACTION_ERROR | 500 | Yes |

---

## Related Documentation

- [DoSQL Client API](/packages/sql.do/README.md)
- [DoLake Client API](/packages/lake.do/README.md)
