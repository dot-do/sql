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
- [FAQ: Common Error Scenarios](#faq-common-error-scenarios)

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

**Troubleshooting Checklist:**
- [ ] Check for common typos: `SELCT` -> `SELECT`, `FORM` -> `FROM`, `WEHRE` -> `WHERE`
- [ ] Verify all parentheses are balanced
- [ ] Ensure string literals use single quotes, not double quotes
- [ ] Check for missing commas between column names

**Code Example:**
```typescript
try {
  await client.query('SELCT * FROM users'); // Typo in SELECT
} catch (error) {
  if (error.code === 'SYNTAX_ERROR') {
    // Use the suggestion property for error recovery hints
    if (error.suggestion) {
      console.log(`Suggestion: ${error.suggestion}`);
      // Output: "Suggestion: Did you mean 'SELECT'?"
    }
  }
}
```

**Related Errors:** [INVALID_REQUEST](#invalid_request), [TYPE_MISMATCH](#type_mismatch)

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

**Troubleshooting Checklist:**
- [ ] Table names are case-sensitive in some configurations
- [ ] Check schema prefix if using multiple schemas
- [ ] Verify the migration that creates this table has run
- [ ] Ensure you are not querying a view that was dropped

**Code Example:**
```typescript
async function safeQuery(tableName: string, sql: string) {
  try {
    return await client.query(sql);
  } catch (error) {
    if (error.code === 'TABLE_NOT_FOUND') {
      // List available tables to help debug
      const tables = await client.query(
        "SELECT name FROM sqlite_master WHERE type='table'"
      );
      console.error(`Table '${tableName}' not found. Available tables:`,
        tables.rows.map(r => r.name));

      // Check for similar names (typo detection)
      const similar = tables.rows.find(r =>
        r.name.toLowerCase() === tableName.toLowerCase()
      );
      if (similar) {
        console.log(`Did you mean '${similar.name}'? (case mismatch)`);
      }
    }
    throw error;
  }
}
```

**Related Errors:** [COLUMN_NOT_FOUND](#column_not_found), [ICEBERG_ERROR](#iceberg_error)

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

**Troubleshooting Checklist:**
- [ ] Column names may have been renamed in a recent migration
- [ ] Check if using the correct table alias in JOIN queries
- [ ] Verify column exists in the specific table being queried (not a joined table)
- [ ] Check for typos in column names (underscores vs camelCase)

**Code Example:**
```typescript
async function validateColumns(tableName: string, columns: string[]) {
  const schema = await client.query(`PRAGMA table_info(${tableName})`);
  const validColumns = new Set(schema.rows.map(r => r.name));

  const invalid = columns.filter(c => !validColumns.has(c));
  if (invalid.length > 0) {
    throw new Error(
      `Invalid columns: ${invalid.join(', ')}. ` +
      `Valid columns are: ${[...validColumns].join(', ')}`
    );
  }
}

// Usage
try {
  await client.query('SELECT email_address FROM users');
} catch (error) {
  if (error.code === 'COLUMN_NOT_FOUND') {
    // The column might be named 'email' not 'email_address'
    await validateColumns('users', ['email_address']);
  }
}
```

**Related Errors:** [TABLE_NOT_FOUND](#table_not_found), [SYNTAX_ERROR](#syntax_error)

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

**Troubleshooting Checklist:**
- [ ] For UNIQUE violations: query existing records first with `SELECT ... WHERE`
- [ ] For FK violations: verify the referenced record exists in the parent table
- [ ] For NOT NULL: ensure all required fields are provided
- [ ] For CHECK: review the constraint definition with `PRAGMA table_info`

**Code Example:**
```typescript
async function upsertUser(email: string, name: string) {
  try {
    await client.query(
      'INSERT INTO users (email, name) VALUES (?, ?)',
      [email, name]
    );
  } catch (error) {
    if (error.code === 'CONSTRAINT_VIOLATION') {
      if (error.message.includes('UNIQUE constraint')) {
        // Handle duplicate - update instead
        console.log('User exists, updating instead');
        await client.query(
          'UPDATE users SET name = ? WHERE email = ?',
          [name, email]
        );
      } else if (error.message.includes('FOREIGN KEY constraint')) {
        // Handle missing reference
        console.error('Referenced record does not exist');
      } else if (error.message.includes('NOT NULL constraint')) {
        // Handle missing required field
        console.error('Required field is missing');
      }
    }
    throw error;
  }
}
```

**Related Errors:** [TYPE_MISMATCH](#type_mismatch), [INVALID_REQUEST](#invalid_request)

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

**Troubleshooting Checklist:**
- [ ] Verify JavaScript types match SQL column types (number for INTEGER, string for TEXT)
- [ ] Dates should be ISO 8601 format: `YYYY-MM-DDTHH:mm:ss.sssZ`
- [ ] JSON columns require valid JSON strings
- [ ] Boolean values should be 0/1 or true/false depending on dialect

**Code Example:**
```typescript
// Type validation helper
function validateTypes(columns: Record<string, any>, schema: ColumnDef[]) {
  const errors: string[] = [];

  for (const col of schema) {
    const value = columns[col.name];
    if (value === undefined || value === null) continue;

    switch (col.type) {
      case 'INTEGER':
        if (typeof value !== 'number' || !Number.isInteger(value)) {
          errors.push(`${col.name}: expected INTEGER, got ${typeof value}`);
        }
        break;
      case 'REAL':
        if (typeof value !== 'number') {
          errors.push(`${col.name}: expected REAL, got ${typeof value}`);
        }
        break;
      case 'TEXT':
        if (typeof value !== 'string') {
          errors.push(`${col.name}: expected TEXT, got ${typeof value}`);
        }
        break;
    }
  }

  if (errors.length > 0) {
    throw new Error(`Type validation failed: ${errors.join('; ')}`);
  }
}
```

**Related Errors:** [CONSTRAINT_VIOLATION](#constraint_violation), [SYNTAX_ERROR](#syntax_error)

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

**Troubleshooting Checklist:**
- [ ] Check if transaction was committed or rolled back elsewhere
- [ ] Verify transaction ID is being stored/passed correctly
- [ ] Review timeout settings (default is typically 30 seconds)
- [ ] Check for network issues that may have caused disconnection

**Code Example:**
```typescript
class TransactionManager {
  private activeTransactions = new Map<string, { startTime: number }>();

  async beginTransaction(): Promise<string> {
    const tx = await client.beginTransaction();
    this.activeTransactions.set(tx.id, { startTime: Date.now() });
    return tx.id;
  }

  async commitTransaction(txId: string) {
    if (!this.activeTransactions.has(txId)) {
      console.warn(`Transaction ${txId} not tracked - may have already completed`);
    }
    try {
      await client.commit(txId);
    } finally {
      this.activeTransactions.delete(txId);
    }
  }

  isActive(txId: string): boolean {
    return this.activeTransactions.has(txId);
  }
}
```

**Related Errors:** [TRANSACTION_ABORTED](#transaction_aborted), [TXN_NO_ACTIVE](#txn_no_active), [TIMEOUT](#timeout)

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

**Troubleshooting Checklist:**
- [ ] Log the conflict details to understand which rows are contended
- [ ] Consider using optimistic locking with version columns
- [ ] Reduce the scope of data modified in a single transaction
- [ ] Implement automatic retry with exponential backoff

**Code Example:**
```typescript
async function withRetry<T>(
  operation: () => Promise<T>,
  { maxRetries = 3, baseDelayMs = 100 } = {}
): Promise<T> {
  let lastError: Error | undefined;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      if (error.code === 'TRANSACTION_ABORTED' ||
          error.code === 'TXN_ABORTED') {
        lastError = error;
        const delay = baseDelayMs * Math.pow(2, attempt);
        console.log(`Transaction aborted, retrying in ${delay}ms (attempt ${attempt + 1})`);
        await new Promise(r => setTimeout(r, delay));
        continue;
      }
      throw error;
    }
  }

  throw lastError;
}

// Usage
await withRetry(async () => {
  await client.transaction(async (tx) => {
    await tx.query('UPDATE accounts SET balance = balance - 100 WHERE id = 1');
    await tx.query('UPDATE accounts SET balance = balance + 100 WHERE id = 2');
  });
});
```

**Related Errors:** [DEADLOCK_DETECTED](#deadlock_detected), [SERIALIZATION_FAILURE](#serialization_failure), [TXN_LOCK_TIMEOUT](#txn_lock_timeout)

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

**Troubleshooting Checklist:**
- [ ] Always acquire locks in a consistent order (e.g., by ID ascending)
- [ ] Avoid long-running transactions that hold locks
- [ ] Consider using `NOWAIT` option to fail fast instead of waiting
- [ ] Review query patterns for circular dependencies

**Code Example:**
```typescript
// Prevent deadlocks by consistent ordering
async function transferBetweenAccounts(
  fromId: number,
  toId: number,
  amount: number
) {
  // Always lock accounts in ascending ID order to prevent deadlocks
  const [firstId, secondId] = fromId < toId
    ? [fromId, toId]
    : [toId, fromId];

  await client.transaction(async (tx) => {
    // Lock in consistent order
    await tx.query('SELECT * FROM accounts WHERE id = ? FOR UPDATE', [firstId]);
    await tx.query('SELECT * FROM accounts WHERE id = ? FOR UPDATE', [secondId]);

    // Now perform the transfer
    await tx.query('UPDATE accounts SET balance = balance - ? WHERE id = ?', [amount, fromId]);
    await tx.query('UPDATE accounts SET balance = balance + ? WHERE id = ?', [amount, toId]);
  });
}
```

**Related Errors:** [TRANSACTION_ABORTED](#transaction_aborted), [SERIALIZATION_FAILURE](#serialization_failure), [TXN_LOCK_TIMEOUT](#txn_lock_timeout)

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

**Troubleshooting Checklist:**
- [ ] This is expected under SERIALIZABLE isolation with concurrent writes
- [ ] Consider READ COMMITTED if your application can tolerate it
- [ ] Batch related operations into fewer transactions
- [ ] Use SELECT FOR UPDATE to explicitly lock rows you plan to modify

**Code Example:**
```typescript
// Retry with exponential backoff for serialization failures
async function serializableTransaction<T>(
  operation: (tx: Transaction) => Promise<T>,
  maxRetries = 5
): Promise<T> {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      return await client.transaction(operation, {
        isolationLevel: 'SERIALIZABLE'
      });
    } catch (error) {
      if (error.code === 'SERIALIZATION_FAILURE' ||
          error.code === 'TXN_SERIALIZATION_FAILURE') {
        if (attempt === maxRetries - 1) {
          throw new Error(`Transaction failed after ${maxRetries} retries: ${error.message}`);
        }

        // Exponential backoff with jitter
        const baseDelay = 50 * Math.pow(2, attempt);
        const jitter = Math.random() * baseDelay * 0.5;
        const delay = baseDelay + jitter;

        console.log(`Serialization failure, retry ${attempt + 1} in ${Math.round(delay)}ms`);
        await new Promise(r => setTimeout(r, delay));
        continue;
      }
      throw error;
    }
  }
  throw new Error('Unreachable');
}
```

**Related Errors:** [DEADLOCK_DETECTED](#deadlock_detected), [TRANSACTION_ABORTED](#transaction_aborted)

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

**Troubleshooting Checklist:**
- [ ] Verify the transaction lifecycle: begin -> operations -> commit/rollback
- [ ] Check for async/await issues causing race conditions
- [ ] Ensure try/finally blocks properly clean up transactions

**Code Example:**
```typescript
// Safe transaction wrapper that ensures proper lifecycle
class SafeTransaction {
  private tx: Transaction | null = null;

  async begin() {
    if (this.tx) {
      throw new Error('Transaction already active');
    }
    this.tx = await client.beginTransaction();
  }

  async query(sql: string, params?: any[]) {
    if (!this.tx) {
      throw new Error('No active transaction - call begin() first');
    }
    return this.tx.query(sql, params);
  }

  async commit() {
    if (!this.tx) {
      throw new Error('No active transaction to commit');
    }
    try {
      await this.tx.commit();
    } finally {
      this.tx = null;
    }
  }

  async rollback() {
    if (this.tx) {
      await this.tx.rollback();
      this.tx = null;
    }
  }
}
```

**Related Errors:** [TXN_ALREADY_ACTIVE](#txn_already_active), [TRANSACTION_NOT_FOUND](#transaction_not_found)

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

**Troubleshooting Checklist:**
- [ ] Check if a previous transaction was not properly closed
- [ ] Look for forgotten await statements on commit/rollback
- [ ] Consider using connection pools with transaction isolation

**Code Example:**
```typescript
// Using savepoints for nested transaction behavior
async function nestedOperation() {
  await client.transaction(async (tx) => {
    await tx.query('INSERT INTO orders (status) VALUES (?)', ['pending']);

    // Create savepoint for nested operation that might fail
    await tx.query('SAVEPOINT before_items');

    try {
      await tx.query('INSERT INTO order_items (order_id, product_id) VALUES (?, ?)', [1, 100]);
      // This might fail
      await tx.query('UPDATE inventory SET quantity = quantity - 1 WHERE product_id = ?', [100]);
    } catch (error) {
      // Rollback to savepoint but keep outer transaction
      console.log('Item insert failed, rolling back to savepoint');
      await tx.query('ROLLBACK TO SAVEPOINT before_items');
      // Continue with partial order
    }

    await tx.query('RELEASE SAVEPOINT before_items');
  });
}
```

**Related Errors:** [TXN_NO_ACTIVE](#txn_no_active), [TXN_SAVEPOINT_NOT_FOUND](#txn_savepoint_not_found)

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

**Troubleshooting Checklist:**
- [ ] Savepoint names are case-sensitive
- [ ] Savepoints are automatically released on commit
- [ ] Check for typos in savepoint names
- [ ] Ensure savepoint was created in the current transaction

**Related Errors:** [TXN_ALREADY_ACTIVE](#txn_already_active), [TXN_NO_ACTIVE](#txn_no_active)

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

**Troubleshooting Checklist:**
- [ ] Identify which transaction is holding the lock
- [ ] Check for long-running queries or forgotten transactions
- [ ] Consider using `NOWAIT` to fail immediately instead of waiting
- [ ] Review lock timeout configuration in connection settings

**Code Example:**
```typescript
// Configure lock timeout and handle timeout errors
const client = createSQLClient({
  url: 'wss://dosql.example.com',
  lockTimeoutMs: 10000, // 10 second lock timeout
});

async function queryWithLockTimeout(sql: string) {
  try {
    return await client.query(sql);
  } catch (error) {
    if (error.code === 'TXN_LOCK_TIMEOUT') {
      // Log which query timed out for debugging
      console.error(`Lock timeout on query: ${sql.substring(0, 100)}...`);

      // Optionally query for blocking transactions
      const blockers = await client.query(`
        SELECT * FROM sys.locks WHERE status = 'WAITING'
      `);
      console.log('Blocking transactions:', blockers.rows);

      throw error;
    }
    throw error;
  }
}
```

**Related Errors:** [DEADLOCK_DETECTED](#deadlock_detected), [TIMEOUT](#timeout)

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

**Troubleshooting Checklist:**
- [ ] Check if transaction was explicitly opened as read-only
- [ ] Verify the connection/session mode allows writes
- [ ] Check for replica/read-replica connection strings

**Code Example:**
```typescript
// Correct usage of read-only vs read-write transactions
async function example() {
  // Read-only transaction (for complex read queries)
  const report = await client.transaction(
    async (tx) => {
      const totals = await tx.query('SELECT SUM(amount) FROM orders');
      const counts = await tx.query('SELECT COUNT(*) FROM orders');
      return { totals, counts };
    },
    { readOnly: true } // Explicitly read-only
  );

  // Read-write transaction (for modifications)
  await client.transaction(
    async (tx) => {
      await tx.query('INSERT INTO logs (message) VALUES (?)', ['Report generated']);
    }
    // No readOnly option = read-write by default
  );
}
```

**Related Errors:** [FORBIDDEN](#forbidden), [UNAUTHORIZED](#unauthorized)

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

**Troubleshooting Checklist:**
- [ ] Check if the server is reachable (ping, curl)
- [ ] Verify firewall rules allow WebSocket connections
- [ ] Check for proxy/load balancer issues with WebSocket upgrades
- [ ] Verify SSL/TLS certificate validity

**Code Example:**
```typescript
// Robust connection handling with automatic reconnection
class ResilientClient {
  private client: SQLClient;
  private reconnectAttempts = 0;
  private maxReconnectAttempts = 10;
  private baseReconnectDelay = 1000;

  constructor(private url: string) {
    this.client = createSQLClient({ url });
    this.setupConnectionHandlers();
  }

  private setupConnectionHandlers() {
    this.client.on('disconnect', async (reason) => {
      console.warn('Disconnected:', reason);
      await this.reconnect();
    });

    this.client.on('error', (error) => {
      if (error.code === 'CONNECTION_ERROR') {
        console.error('Connection error:', error.message);
      }
    });
  }

  private async reconnect() {
    while (this.reconnectAttempts < this.maxReconnectAttempts) {
      const delay = this.baseReconnectDelay * Math.pow(2, this.reconnectAttempts);
      console.log(`Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts + 1})`);

      await new Promise(r => setTimeout(r, delay));

      try {
        await this.client.connect();
        this.reconnectAttempts = 0;
        console.log('Reconnected successfully');
        return;
      } catch (error) {
        this.reconnectAttempts++;
      }
    }

    throw new Error('Max reconnection attempts exceeded');
  }
}
```

**Related Errors:** [TIMEOUT](#timeout), [R2_TIMEOUT](#r2_timeout)

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

**Troubleshooting Checklist:**
- [ ] Check query execution plan for slow operations
- [ ] Add indexes for columns used in WHERE clauses
- [ ] Break large queries into smaller batches
- [ ] Monitor server CPU and memory usage
- [ ] Check for table locks from other transactions

**Code Example:**
```typescript
// Query with timeout handling and optimization hints
async function executeWithTimeout(sql: string, timeoutMs = 30000) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const result = await client.query(sql, [], {
      signal: controller.signal,
      timeout: timeoutMs,
    });
    return result;
  } catch (error) {
    if (error.code === 'TIMEOUT' || error.name === 'AbortError') {
      console.error(`Query timed out after ${timeoutMs}ms`);

      // Log slow query for analysis
      console.log('Slow query:', sql.substring(0, 500));

      // Suggest optimization
      if (sql.toLowerCase().includes('select *')) {
        console.log('Tip: Avoid SELECT * - specify only needed columns');
      }
      if (!sql.toLowerCase().includes('limit')) {
        console.log('Tip: Add LIMIT clause to reduce result set size');
      }
    }
    throw error;
  } finally {
    clearTimeout(timeout);
  }
}
```

**Related Errors:** [CONNECTION_ERROR](#connection_error), [TXN_LOCK_TIMEOUT](#txn_lock_timeout)

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

**Troubleshooting Checklist:**
- [ ] Verify the Authorization header is being sent
- [ ] Check token format (Bearer token, API key, etc.)
- [ ] Verify token has not expired
- [ ] Ensure correct environment (production vs staging keys)

**Code Example:**
```typescript
// Token refresh handling
class AuthenticatedClient {
  private accessToken: string;
  private refreshToken: string;
  private client: SQLClient;

  async query(sql: string) {
    try {
      return await this.client.query(sql);
    } catch (error) {
      if (error.code === 'UNAUTHORIZED') {
        // Attempt token refresh
        try {
          await this.refreshAccessToken();
          // Retry with new token
          return await this.client.query(sql);
        } catch (refreshError) {
          // Refresh failed, user needs to re-authenticate
          throw new Error('Session expired. Please log in again.');
        }
      }
      throw error;
    }
  }

  private async refreshAccessToken() {
    const response = await fetch('/auth/refresh', {
      method: 'POST',
      body: JSON.stringify({ refreshToken: this.refreshToken }),
    });

    if (!response.ok) {
      throw new Error('Token refresh failed');
    }

    const { accessToken } = await response.json();
    this.accessToken = accessToken;
    this.client.setAuthToken(accessToken);
  }
}
```

**Related Errors:** [FORBIDDEN](#forbidden)

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

**Troubleshooting Checklist:**
- [ ] Verify the user/role has access to the specific table/resource
- [ ] Check row-level security policies if enabled
- [ ] Verify IP address is in the allowlist
- [ ] Check for tenant isolation policies

**Code Example:**
```typescript
// Permission-aware query execution
async function queryWithPermissionCheck(tableName: string, sql: string) {
  try {
    return await client.query(sql);
  } catch (error) {
    if (error.code === 'FORBIDDEN') {
      // Check which permission is missing
      const permissions = await client.query(`
        SELECT table_name, privilege_type
        FROM information_schema.table_privileges
        WHERE grantee = CURRENT_USER
          AND table_name = ?
      `, [tableName]);

      if (permissions.rows.length === 0) {
        console.error(`No permissions on table '${tableName}'`);
        console.log('Contact your administrator to grant access');
      } else {
        console.log('Current permissions:', permissions.rows);
        console.log('You may need additional privileges for this operation');
      }
    }
    throw error;
  }
}
```

**Related Errors:** [UNAUTHORIZED](#unauthorized), [TXN_READ_ONLY_VIOLATION](#txn_read_only_violation)

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

**Troubleshooting Checklist:**
- [ ] Check the `Retry-After` header or `retryAfter` field in response
- [ ] Review your request patterns for burst traffic
- [ ] Consider implementing request queuing
- [ ] Check if you need a higher rate limit tier

**Code Example:**
```typescript
// Rate limit aware client with automatic backoff
class RateLimitedClient {
  private requestQueue: Array<() => Promise<any>> = [];
  private isProcessing = false;
  private retryAfterMs = 0;

  async query(sql: string): Promise<any> {
    return new Promise((resolve, reject) => {
      this.requestQueue.push(async () => {
        try {
          const result = await this.executeQuery(sql);
          resolve(result);
        } catch (error) {
          reject(error);
        }
      });
      this.processQueue();
    });
  }

  private async executeQuery(sql: string) {
    // Wait if we're in a rate limit backoff period
    if (this.retryAfterMs > 0) {
      await new Promise(r => setTimeout(r, this.retryAfterMs));
      this.retryAfterMs = 0;
    }

    try {
      return await client.query(sql);
    } catch (error) {
      if (error.code === 'rate_limited' || error.code === 'R2_RATE_LIMITED') {
        this.retryAfterMs = error.retryAfter * 1000 || 5000;
        console.log(`Rate limited. Waiting ${this.retryAfterMs}ms before retry`);

        await new Promise(r => setTimeout(r, this.retryAfterMs));
        this.retryAfterMs = 0;

        // Retry once after backoff
        return await client.query(sql);
      }
      throw error;
    }
  }

  private async processQueue() {
    if (this.isProcessing) return;
    this.isProcessing = true;

    while (this.requestQueue.length > 0) {
      const request = this.requestQueue.shift()!;
      await request();
    }

    this.isProcessing = false;
  }
}
```

**Related Errors:** [RESOURCE_EXHAUSTED](#resource_exhausted), [QUOTA_EXCEEDED](#quota_exceeded), [CDC_BUFFER_OVERFLOW](#cdc_buffer_overflow)

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

**Troubleshooting Checklist:**
- [ ] Check server metrics for memory/CPU utilization
- [ ] Reduce parallel query execution
- [ ] Close idle connections
- [ ] Consider implementing connection pooling

**Related Errors:** [RATE_LIMITED](#rate_limited--r2_rate_limited), [TIMEOUT](#timeout)

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

**Troubleshooting Checklist:**
- [ ] Check current usage vs quota limits in dashboard
- [ ] Identify large tables or files consuming quota
- [ ] Review data retention policies
- [ ] Consider archiving old data to cold storage

**Code Example:**
```typescript
// Quota-aware operations
async function insertWithQuotaCheck(records: any[]) {
  try {
    await client.batchInsert('events', records);
  } catch (error) {
    if (error.code === 'QUOTA_EXCEEDED') {
      console.error('Storage quota exceeded');

      // Check current usage
      const usage = await client.query('SELECT * FROM sys.storage_usage');
      console.log('Current usage:', usage.rows[0]);

      // Suggest cleanup
      const oldData = await client.query(`
        SELECT table_name, COUNT(*) as count
        FROM information_schema.tables
        WHERE created_at < datetime('now', '-90 days')
        GROUP BY table_name
      `);
      console.log('Tables with old data that could be archived:', oldData.rows);

      throw new Error('Storage quota exceeded. Please clean up data or upgrade your plan.');
    }
    throw error;
  }
}
```

**Related Errors:** [RATE_LIMITED](#rate_limited--r2_rate_limited), [FSX_SIZE_EXCEEDED](#fsx_size_exceeded)

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

**Troubleshooting Checklist:**
- [ ] Verify WAL is enabled on the database
- [ ] Check subscription table names exist
- [ ] Ensure proper permissions for CDC
- [ ] Verify connection to CDC endpoint

**Code Example:**
```typescript
// Robust CDC subscription with error recovery
async function subscribeToCDC(tables: string[]) {
  const maxRetries = 3;

  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      const subscription = await lakeClient.subscribe({
        tables,
        fromLSN: await getLastProcessedLSN(),
      });

      for await (const event of subscription) {
        await processEvent(event);
        await saveCheckpoint(event.lsn);
      }
    } catch (error) {
      if (error.code === 'CDC_SUBSCRIPTION_FAILED') {
        console.error(`Subscription failed (attempt ${attempt + 1}):`, error.message);

        if (attempt < maxRetries - 1) {
          await new Promise(r => setTimeout(r, 5000 * (attempt + 1)));
          continue;
        }
      }
      throw error;
    }
  }
}
```

**Related Errors:** [CDC_LSN_NOT_FOUND](#cdc_lsn_not_found), [CDC_SLOT_NOT_FOUND](#cdc_slot_not_found)

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

**Troubleshooting Checklist:**
- [ ] Your checkpoint LSN is older than WAL retention period
- [ ] Consider increasing WAL retention if needed
- [ ] Implement a full resync strategy as fallback
- [ ] Checkpoint more frequently to avoid gaps

**Code Example:**
```typescript
// Handle LSN not found with fallback to full sync
async function syncWithFallback(tableName: string) {
  let fromLSN = await getLastCheckpoint(tableName);

  try {
    for await (const event of lakeClient.subscribe({ tables: [tableName], fromLSN })) {
      await processEvent(event);
      await saveCheckpoint(tableName, event.lsn);
    }
  } catch (error) {
    if (error.code === 'CDC_LSN_NOT_FOUND') {
      console.warn(`LSN ${fromLSN} not found. Falling back to full sync.`);

      // Get the oldest available LSN
      const oldestLSN = error.oldestAvailableLSN || 0n;

      // Perform full table sync first
      await performFullTableSync(tableName);

      // Then continue from oldest available LSN
      await saveCheckpoint(tableName, oldestLSN);
      return syncWithFallback(tableName);
    }
    throw error;
  }
}

async function performFullTableSync(tableName: string) {
  console.log(`Performing full sync for ${tableName}`);
  const rows = await client.query(`SELECT * FROM ${tableName}`);
  for (const row of rows.rows) {
    await upsertToDestination(tableName, row);
  }
}
```

**Related Errors:** [CDC_SUBSCRIPTION_FAILED](#cdc_subscription_failed), [CDC_SLOT_NOT_FOUND](#cdc_slot_not_found)

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

**Troubleshooting Checklist:**
- [ ] Verify slot name spelling
- [ ] Check if slot was dropped by another process
- [ ] Ensure proper permissions to access replication slots

**Related Errors:** [CDC_SLOT_EXISTS](#cdc_slot_exists), [CDC_SUBSCRIPTION_FAILED](#cdc_subscription_failed)

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

**Troubleshooting Checklist:**
- [ ] Choose unique slot names per consumer application
- [ ] Clean up orphaned slots from failed processes
- [ ] List existing slots before creating new ones

**Related Errors:** [CDC_SLOT_NOT_FOUND](#cdc_slot_not_found)

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

**Troubleshooting Checklist:**
- [ ] Monitor consumer lag metrics
- [ ] Increase consumer parallelism
- [ ] Optimize event processing logic
- [ ] Consider increasing buffer configuration

**Code Example:**
```typescript
// Backpressure-aware CDC consumer
class BackpressureConsumer {
  private processing = 0;
  private maxConcurrent = 10;
  private paused = false;

  async consume(subscription: AsyncIterable<CDCEvent>) {
    for await (const event of subscription) {
      // Implement backpressure
      while (this.processing >= this.maxConcurrent) {
        await new Promise(r => setTimeout(r, 100));
      }

      this.processing++;
      this.processEvent(event)
        .catch(error => {
          if (error.code === 'CDC_BUFFER_OVERFLOW') {
            console.warn('Buffer overflow - slowing down');
            this.maxConcurrent = Math.max(1, this.maxConcurrent - 1);
          }
        })
        .finally(() => {
          this.processing--;
        });
    }
  }

  private async processEvent(event: CDCEvent) {
    // Simulate processing
    await new Promise(r => setTimeout(r, 50));
    console.log('Processed event:', event.lsn);
  }
}
```

**Related Errors:** [buffer_full](#buffer_full), [RATE_LIMITED](#rate_limited--r2_rate_limited)

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

**Troubleshooting Checklist:**
- [ ] Verify the encoding matches expected format (JSON, Avro, etc.)
- [ ] Check for schema changes that may affect deserialization
- [ ] Implement schema registry for versioned schemas
- [ ] Add error handling for malformed events

**Related Errors:** [INVALID_REQUEST](#invalid_request), [MessageValidationError](#messagevalidationerror)

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
| Class Location | `dolake` - `packages/dolake/src/schemas.ts` |
| HTTP Status | 400 Bad Request |
| WebSocket NACK Reason | `invalid_format` |
| Retryable | No |

**Description:** Zod schema validation failed for a WebSocket RPC message. This error is thrown when an incoming message fails validation against the defined Zod schemas, either due to missing fields, incorrect types, invalid enum values, or malformed JSON.

**When It's Thrown:**
- When `validateClientMessage()` receives an invalid message
- When `validateRpcMessage()` receives an invalid message
- When type-specific validators fail (e.g., `validateCDCBatchMessage()`)
- When JSON parsing fails during WebSocket message decoding

**Error Format (WebSocket NACK):**
```json
{
  "type": "nack",
  "timestamp": 1705329600000,
  "sequenceNumber": 0,
  "reason": "invalid_format",
  "errorMessage": "sourceDoId: Required; events: Expected array, received object",
  "shouldRetry": false
}
```

**Common Causes and Error Messages:**

| Cause | Example Error |
|-------|---------------|
| Missing required field | `sourceDoId: Required` |
| Wrong type | `timestamp: Expected number, received string` |
| Invalid enum value | `operation: Invalid enum value. Expected 'INSERT' \| 'UPDATE' \| 'DELETE'` |
| Invalid array | `events: Expected array, received object` |
| Malformed JSON | `Failed to parse JSON: Unexpected token 'x' at position 5` |
| Unknown message type | `Invalid discriminator value. Expected 'cdc_batch' \| 'connect' \| ...` |

**Class Properties:**
- `message`: Human-readable error summary
- `name`: Always `'MessageValidationError'`
- `zodError`: The underlying `ZodError` (null for JSON parse errors)

**Resolution Steps:**
1. Use `getErrorDetails()` method for field-by-field error information
2. Review message structure against expected schema
3. Check field types match (numbers vs strings, booleans vs strings)
4. Validate enum values are exactly as specified (case-sensitive)
5. Ensure JSON is well-formed before schema validation

**Code Example - Handling the Error:**
```typescript
import {
  validateClientMessage,
  MessageValidationError,
  CDCBatchMessageSchema,
} from 'dolake';

// Method 1: Using validation functions
try {
  const message = validateClientMessage(rawData);
  // Process valid message
} catch (error) {
  if (error instanceof MessageValidationError) {
    // Get formatted error details
    console.error('Validation failed:', error.getErrorDetails());
    // Output: "sourceDoId: Required; events: Expected array, received object"

    // Access individual Zod issues for programmatic handling
    if (error.zodError) {
      for (const issue of error.zodError.issues) {
        console.log(`Field: ${issue.path.join('.')}`);
        console.log(`Error: ${issue.message}`);
        console.log(`Code: ${issue.code}`);
      }
    }

    // Check if it's a JSON parse error vs schema validation
    if (error.zodError === null) {
      console.error('JSON parsing failed - check message format');
    } else {
      console.error('Schema validation failed - check field values');
    }
  }
}

// Method 2: Using schema directly (non-throwing)
const result = CDCBatchMessageSchema.safeParse(data);
if (!result.success) {
  console.error('Validation errors:', result.error.issues);
  // Handle errors without throwing
} else {
  // result.data is typed correctly
  const validMessage = result.data;
}
```

**Code Example - WebSocket Message Handling:**
```typescript
ws.onmessage = (event) => {
  const response = JSON.parse(event.data);

  if (response.type === 'nack' && response.reason === 'invalid_format') {
    // MessageValidationError was thrown server-side
    console.error('Message format error:', response.errorMessage);

    // Parse individual field errors from errorMessage
    const fieldErrors = response.errorMessage.split('; ').map(err => {
      const [path, message] = err.split(': ');
      return { path, message };
    });

    // Fix and retry with corrected message
    const correctedMessage = fixValidationErrors(originalMessage, fieldErrors);
    ws.send(JSON.stringify(correctedMessage));
  }
};
```

**Troubleshooting Checklist:**
- [ ] Verify `type` field matches exactly: `'cdc_batch'`, `'connect'`, `'heartbeat'`, `'flush_request'`, `'disconnect'`
- [ ] Check `timestamp` is a number (Unix epoch ms), not an ISO string
- [ ] Ensure `sequenceNumber`, `firstEventSequence`, `lastEventSequence` are integers, not strings
- [ ] Verify `events` is an array, not an object or null
- [ ] Check `operation` values are uppercase: `'INSERT'`, `'UPDATE'`, `'DELETE'`
- [ ] Ensure `capabilities` object has all required boolean and number fields
- [ ] Verify JSON is valid (no trailing commas, proper quoting)
- [ ] Check for BigInt values that need special serialization

**Related Errors:** [INVALID_REQUEST](#invalid_request), [invalid_format](#invalid_format), [CDC_DECODE_ERROR](#cdc_decode_error)

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
import { createSQLClient, SQLError } from 'sql.do';

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
import { createLakeClient, LakeError } from 'lake.do';

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
import type { NackMessage, AckMessage } from 'dolake';

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
