# Transactions Guide

This guide covers comprehensive transaction management in DoSQL, including ACID guarantees, isolation levels, savepoints, deadlock handling, and distributed transactions.

## Table of Contents

- [Transaction Basics](#transaction-basics)
- [Isolation Levels](#isolation-levels)
- [Transaction Timeouts](#transaction-timeouts)
- [Savepoints](#savepoints)
- [Deadlock Detection and Resolution](#deadlock-detection-and-resolution)
- [Read-Your-Writes Consistency](#read-your-writes-consistency)
- [Distributed Transactions (2PC)](#distributed-transactions-2pc)
- [Transaction Best Practices](#transaction-best-practices)
- [Error Handling in Transactions](#error-handling-in-transactions)
- [Performance Considerations](#performance-considerations)
- [Example: Money Transfer](#example-money-transfer)
- [Example: Inventory Management](#example-inventory-management)

---

## Transaction Basics

DoSQL provides SQLite-compatible transaction semantics with ACID guarantees (Atomicity, Consistency, Isolation, Durability).

### BEGIN, COMMIT, ROLLBACK

#### SQL Syntax

```sql
-- Begin a transaction
BEGIN;
-- or with explicit mode
BEGIN DEFERRED;
BEGIN IMMEDIATE;
BEGIN EXCLUSIVE;

-- Perform operations
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');
UPDATE accounts SET balance = balance - 100 WHERE user_id = 1;

-- Commit the transaction
COMMIT;

-- Or rollback to undo changes
ROLLBACK;
```

#### TypeScript API

```typescript
import { DB } from 'dosql';

const db = await DB('my-database');

// Using the transaction manager directly
const txnManager = db.getTransactionManager();

// Begin a transaction
const context = await txnManager.begin();
console.log('Transaction ID:', context.txnId);
console.log('State:', context.state); // 'ACTIVE'

try {
  await db.run('INSERT INTO users (name) VALUES (?)', ['Alice']);
  await db.run('UPDATE accounts SET balance = balance - 100 WHERE user_id = ?', [1]);

  // Commit the transaction
  const commitLsn = await txnManager.commit();
  console.log('Committed at LSN:', commitLsn);
} catch (error) {
  // Rollback on error
  await txnManager.rollback();
  throw error;
}
```

### Transaction Modes

DoSQL supports three transaction modes, matching SQLite's behavior:

| Mode | Description | Lock Behavior |
|------|-------------|---------------|
| `DEFERRED` | Default mode. Locks acquired only when needed. | First read acquires shared lock, first write acquires exclusive lock |
| `IMMEDIATE` | Acquires write lock immediately. | Blocks other writers from the start |
| `EXCLUSIVE` | Acquires exclusive lock, preventing all other access. | Full isolation from other transactions |

```typescript
import { TransactionMode } from 'dosql';

// Begin with immediate mode for guaranteed write access
const context = await txnManager.begin({
  mode: TransactionMode.IMMEDIATE
});

// Begin with exclusive mode for full isolation
const context = await txnManager.begin({
  mode: TransactionMode.EXCLUSIVE
});
```

### Functional Transaction Wrapper

The `executeInTransaction` helper provides automatic commit/rollback handling:

```typescript
import { executeInTransaction } from 'dosql';

const result = await executeInTransaction(txnManager, async (ctx) => {
  // All operations within the transaction
  await db.run('INSERT INTO orders (user_id, total) VALUES (?, ?)', [1, 99.99]);
  const orderId = await db.get('SELECT last_insert_rowid() as id');

  await db.run('INSERT INTO order_items (order_id, product_id, qty) VALUES (?, ?, ?)',
    [orderId.id, 42, 2]);

  return { orderId: orderId.id };
});

if (result.committed) {
  console.log('Order created:', result.value.orderId);
  console.log('Committed at LSN:', result.commitLsn);
} else {
  console.error('Transaction failed:', result.error);
}
```

### Read-Only Transactions

Read-only transactions provide optimized performance and prevent accidental writes:

```typescript
import { executeReadOnly } from 'dosql';

const users = await executeReadOnly(txnManager, async (ctx) => {
  // Only read operations allowed
  return await db.query('SELECT * FROM users WHERE active = ?', [true]);
});

// Attempting to write in read-only transaction throws TransactionError
const context = await txnManager.begin({ readOnly: true });
// This will throw: TransactionErrorCode.READ_ONLY_VIOLATION
await db.run('INSERT INTO users (name) VALUES (?)', ['Bob']);
```

---

## Isolation Levels

DoSQL supports multiple isolation levels to balance consistency and performance.

### Available Isolation Levels

| Level | Description | Phenomena Prevented |
|-------|-------------|---------------------|
| `READ_UNCOMMITTED` | Lowest isolation. Can read uncommitted changes. | None |
| `READ_COMMITTED` | Only sees committed data. | Dirty reads |
| `REPEATABLE_READ` | Same query returns same results within transaction. | Dirty reads, non-repeatable reads |
| `SNAPSHOT` | Sees consistent snapshot using MVCC. | Dirty reads, non-repeatable reads, phantom reads |
| `SERIALIZABLE` | Highest isolation. Full serialization (SQLite default). | All phenomena |

### Configuring Isolation Level

#### SQL Syntax

```sql
-- Set isolation level for session
PRAGMA isolation_level = 'SERIALIZABLE';

-- Set isolation level for transaction
BEGIN;
SET TRANSACTION ISOLATION LEVEL SNAPSHOT;
-- ... operations
COMMIT;
```

#### TypeScript API

```typescript
import { IsolationLevel } from 'dosql';

// Set default isolation level for the manager
const txnManager = createTransactionManager({
  defaultIsolationLevel: IsolationLevel.SNAPSHOT
});

// Or specify per-transaction
const context = await txnManager.begin({
  isolationLevel: IsolationLevel.SERIALIZABLE
});
```

### Isolation Level Behaviors

#### READ_UNCOMMITTED

```typescript
// Transaction 1: Uncommitted changes
await txn1.begin();
await db.run('UPDATE accounts SET balance = 500 WHERE id = 1');
// Not committed yet

// Transaction 2: With READ_UNCOMMITTED, can see uncommitted balance
await txn2.begin({ isolationLevel: IsolationLevel.READ_UNCOMMITTED });
const row = await db.get('SELECT balance FROM accounts WHERE id = 1');
console.log(row.balance); // 500 (dirty read)
```

#### READ_COMMITTED

```typescript
// Transaction sees only committed data
await txnManager.begin({ isolationLevel: IsolationLevel.READ_COMMITTED });

// First read
const balance1 = await db.get('SELECT balance FROM accounts WHERE id = 1');

// Another transaction commits a change here...

// Second read may see different value (non-repeatable read)
const balance2 = await db.get('SELECT balance FROM accounts WHERE id = 1');
// balance2 may differ from balance1
```

#### SNAPSHOT (MVCC)

```typescript
// Transaction gets a consistent snapshot at start
const context = await txnManager.begin({
  isolationLevel: IsolationLevel.SNAPSHOT,
  enableMVCC: true
});

console.log('Snapshot LSN:', context.snapshot.snapshotLsn);
console.log('Active transactions:', context.snapshot.activeTransactions);

// All reads see data as of snapshot time
const users = await db.query('SELECT * FROM users');
// Even if other transactions commit, we see the same data
```

#### SERIALIZABLE

```typescript
// Full serialization prevents all anomalies
await txnManager.begin({ isolationLevel: IsolationLevel.SERIALIZABLE });

// Locks are held until commit
await db.run('SELECT * FROM accounts WHERE id = 1'); // Shared lock acquired
await db.run('UPDATE accounts SET balance = 500 WHERE id = 1'); // Exclusive lock

// Other transactions trying to access same rows will wait or fail
await txnManager.commit(); // Locks released
```

---

## Transaction Timeouts

DoSQL enforces transaction timeouts to prevent resource exhaustion from long-running transactions.

### Timeout Configuration

```typescript
import { createTransactionManager, DEFAULT_TIMEOUT_CONFIG } from 'dosql';

const txnManager = createTransactionManager({
  timeoutConfig: {
    defaultTimeoutMs: 30000,    // Default: 30 seconds
    maxTimeoutMs: 120000,       // Maximum allowed: 2 minutes
    gracePeriodMs: 5000,        // Grace period before hard timeout: 5 seconds
    warningThresholdMs: 25000,  // Warning logged after: 25 seconds
  },
  maxExtensions: 3,  // Allow up to 3 timeout extensions

  // Callbacks
  onTimeoutWarning: (txnId, remainingMs) => {
    console.warn(`Transaction ${txnId} has ${remainingMs}ms remaining`);
  },
  onTimeout: (txnId) => {
    console.error(`Transaction ${txnId} timed out and was rolled back`);
  },
  onLongRunningTransaction: (log) => {
    console.warn('Long-running transaction:', log);
  }
});
```

### Per-Transaction Timeout

```typescript
// Set timeout for specific transaction
const context = await txnManager.begin({
  timeoutMs: 60000  // 60 seconds for this transaction
});
```

### Timeout Extension

```typescript
// Request more time for a long operation
const granted = txnManager.requestExtension(context.txnId, 30000); // Request 30 more seconds

if (granted) {
  console.log('Extension granted, remaining time:', txnManager.getRemainingTime(context.txnId));
} else {
  console.log('Extension denied - max extensions reached');
}
```

### Durable Object Alarm Integration

When running in Cloudflare Workers, timeout enforcement uses Durable Object alarms:

```typescript
import { createTransactionManager } from 'dosql';

export class DatabaseDO {
  private txnManager: ExtendedTransactionManager;

  constructor(private state: DurableObjectState) {
    this.txnManager = createTransactionManager({
      doState: state,  // Enable alarm-based timeout enforcement
      timeoutConfig: {
        defaultTimeoutMs: 30000,
      }
    });
  }

  // Handle alarm callback
  async alarm(): Promise<void> {
    await this.txnManager.handleAlarm();
  }
}
```

### I/O Operation Timeout

Individual I/O operations can also have timeouts:

```typescript
// Execute operation with I/O timeout
try {
  const result = await txnManager.executeWithIoTimeout(async () => {
    return await slowExternalCall();
  });
} catch (error) {
  if (error.code === TransactionErrorCode.IO_TIMEOUT) {
    console.error('I/O operation timed out');
  }
}
```

---

## Savepoints

Savepoints enable nested transactions for partial rollback capability.

### Creating and Using Savepoints

#### SQL Syntax

```sql
BEGIN;

INSERT INTO orders (user_id, total) VALUES (1, 100.00);

-- Create savepoint
SAVEPOINT order_items;

INSERT INTO order_items (order_id, product_id, qty) VALUES (1, 42, 2);
INSERT INTO order_items (order_id, product_id, qty) VALUES (1, 43, 1);

-- Oops, wrong items - rollback to savepoint
ROLLBACK TO SAVEPOINT order_items;

-- Try again with correct items
INSERT INTO order_items (order_id, product_id, qty) VALUES (1, 44, 3);

-- Release savepoint (merge changes to parent)
RELEASE SAVEPOINT order_items;

COMMIT;
```

#### TypeScript API

```typescript
const txnManager = db.getTransactionManager();

await txnManager.begin();

try {
  await db.run('INSERT INTO orders (user_id, total) VALUES (?, ?)', [1, 100.00]);

  // Create savepoint
  const savepoint = await txnManager.savepoint('order_items');
  console.log('Savepoint:', savepoint.name, 'at depth:', savepoint.depth);

  try {
    await db.run('INSERT INTO order_items (order_id, product_id, qty) VALUES (?, ?, ?)', [1, 42, 2]);

    // Simulate an error
    throw new Error('Invalid product');

  } catch (itemError) {
    // Rollback only the savepoint, keeping the order
    await txnManager.rollbackTo('order_items');
    console.log('Rolled back to savepoint, order preserved');
  }

  // Continue with different items
  await db.run('INSERT INTO order_items (order_id, product_id, qty) VALUES (?, ?, ?)', [1, 44, 3]);

  // Release savepoint
  await txnManager.release('order_items');

  await txnManager.commit();
} catch (error) {
  await txnManager.rollback();
  throw error;
}
```

### Nested Savepoints

Savepoints can be nested to arbitrary depth:

```typescript
await txnManager.begin();

const sp1 = await txnManager.savepoint('level1');  // depth: 0
const sp2 = await txnManager.savepoint('level2');  // depth: 1
const sp3 = await txnManager.savepoint('level3');  // depth: 2

// Rollback to level2 removes level3
await txnManager.rollbackTo('level2');

// Release level1 removes all nested savepoints
await txnManager.release('level1');

await txnManager.commit();
```

### Savepoint Wrapper Function

```typescript
import { executeWithSavepoint } from 'dosql';

await txnManager.begin();

// Outer operations
await db.run('INSERT INTO users (name) VALUES (?)', ['Alice']);

// Nested operation with savepoint
const innerResult = await executeWithSavepoint(txnManager, 'process_profile', async () => {
  await db.run('INSERT INTO profiles (user_id, bio) VALUES (?, ?)', [1, 'Hello']);

  // This might fail
  await processExternalProfile();

  return { profileCreated: true };
});

if (!innerResult.committed) {
  console.log('Profile creation failed, but user still created');
}

await txnManager.commit();
```

---

## Deadlock Detection and Resolution

DoSQL provides comprehensive deadlock detection and multiple resolution strategies.

### Understanding Deadlocks

A deadlock occurs when two or more transactions are waiting for each other to release locks:

```
Transaction 1: Holds lock on A, waiting for lock on B
Transaction 2: Holds lock on B, waiting for lock on A
```

### Enabling Deadlock Detection

```typescript
import { createLockManager, createTransactionManager } from 'dosql';

const lockManager = createLockManager({
  detectDeadlocks: true,
  defaultTimeout: 5000,  // Lock wait timeout
  maxWaitQueueSize: 100,

  // Victim selection policy
  victimSelection: 'youngest',  // 'youngest' | 'leastWork' | 'roundRobin' | 'preferReadOnly' | 'priority'

  // Callback on deadlock detection
  onDeadlock: (info) => {
    console.error('Deadlock detected:', info.cycle);
    console.log('Victim:', info.victimTxnId);
    console.log('Wait-for graph:', info.graphDot);
  }
});

const txnManager = createTransactionManager({
  lockManager
});
```

### Victim Selection Policies

| Policy | Description |
|--------|-------------|
| `youngest` | Abort the most recently started transaction |
| `leastWork` | Abort the transaction with least work done (lowest cost) |
| `roundRobin` | Rotate victims to prevent starvation |
| `preferReadOnly` | Prefer read-only transactions as victims |
| `priority` | Select based on transaction priority (lowest priority aborted) |

```typescript
// Set transaction cost for leastWork policy
lockManager.setTransactionCost(context.txnId, 100);

// Set transaction priority
lockManager.setTransactionPriority(context.txnId, 10); // Higher = less likely to be victim

// Mark as read-only for preferReadOnly policy
lockManager.markReadOnly(context.txnId, true);
```

### Deadlock Prevention Schemes

Instead of detection, you can use prevention schemes:

```typescript
const lockManager = createLockManager({
  deadlockPrevention: 'waitDie'  // 'waitDie' | 'woundWait' | 'lockOrdering' | 'noWait'
});
```

| Scheme | Description |
|--------|-------------|
| `waitDie` | Older transactions wait, younger die (abort) |
| `woundWait` | Older transactions wound (abort) younger holders |
| `lockOrdering` | Enforce consistent lock acquisition order |
| `noWait` | Never wait for locks, fail immediately |

### Handling Deadlock Errors

```typescript
import { TransactionError, TransactionErrorCode, DeadlockError } from 'dosql';

try {
  await lockManager.acquire({
    txnId: context.txnId,
    lockType: LockType.EXCLUSIVE,
    resource: 'accounts:1',
    timestamp: Date.now()
  });
} catch (error) {
  if (error instanceof DeadlockError) {
    console.log('Deadlock cycle:', error.cycle);
    console.log('Resources involved:', error.resources);
    console.log('Wait times:', error.waitTimes);
    console.log('Graph visualization:', error.graphDot);

    // Retry the transaction
    return retryTransaction();
  }

  if (error.code === TransactionErrorCode.LOCK_TIMEOUT) {
    console.log('Lock acquisition timed out');
  }

  throw error;
}
```

### Deadlock Statistics

```typescript
const stats = lockManager.getDeadlockStats();
console.log('Total deadlocks:', stats.totalDeadlocks);
console.log('Average cycle length:', stats.avgCycleLength);
console.log('Victims by policy:', stats.victimsAborted);
console.log('Prevention activations:', stats.preventionActivations);

// Get deadlock history
const history = lockManager.getDeadlockHistory();
history.forEach(info => {
  console.log(`Deadlock at ${info.timestamp}: ${info.cycle.join(' -> ')}`);
});

// Export report
const report = lockManager.exportReport();
console.log(report);
```

### Wait-For Graph Visualization

```typescript
const graph = lockManager.getWaitForGraph();

// Generate DOT format for visualization
const dot = graph.toDot();
console.log(dot);
/*
digraph WaitFor {
  node [shape=box];
  "txn1" -> "txn2" [label="accounts:1\n(exclusive)"];
  "txn2" -> "txn1" [label="accounts:2\n(exclusive)"];
}
*/

// Check for cycles
if (graph.hasCycle()) {
  const cycle = graph.findCycle();
  console.log('Cycle detected:', cycle);
}
```

---

## Read-Your-Writes Consistency

DoSQL guarantees that transactions see their own uncommitted changes.

### Within a Transaction

```typescript
await txnManager.begin();

// Insert a row
await db.run('INSERT INTO users (id, name) VALUES (?, ?)', [100, 'Alice']);

// Can immediately read the inserted row within same transaction
const user = await db.get('SELECT * FROM users WHERE id = ?', [100]);
console.log(user); // { id: 100, name: 'Alice' }

// Update and read back
await db.run('UPDATE users SET name = ? WHERE id = ?', ['Alicia', 100]);
const updated = await db.get('SELECT * FROM users WHERE id = ?', [100]);
console.log(updated); // { id: 100, name: 'Alicia' }

await txnManager.commit();
```

### With MVCC (Snapshot Isolation)

```typescript
// Transaction sees its own changes even with snapshot isolation
await txnManager.begin({
  isolationLevel: IsolationLevel.SNAPSHOT
});

// Insert is visible to this transaction
await db.run('INSERT INTO orders (id, total) VALUES (?, ?)', [1, 100]);

const order = await db.get('SELECT * FROM orders WHERE id = ?', [1]);
console.log(order); // { id: 1, total: 100 } - own write is visible

await txnManager.commit();
```

### Causal Consistency Across Transactions

For read-your-writes across transactions, track the commit LSN:

```typescript
// Writer transaction
const writeContext = await txnManager.begin();
await db.run('UPDATE users SET balance = ? WHERE id = ?', [500, 1]);
const commitLsn = await txnManager.commit();

// Ensure subsequent read sees the write
const readContext = await txnManager.begin({
  afterLsn: commitLsn  // Wait for this LSN to be visible
});
const balance = await db.get('SELECT balance FROM users WHERE id = ?', [1]);
console.log(balance); // Guaranteed to be 500

await txnManager.commit();
```

---

## Distributed Transactions (2PC)

DoSQL supports two-phase commit for transactions spanning multiple Durable Objects.

### Two-Phase Commit Protocol

```typescript
import { TwoPhaseCoordinator, Participant } from 'dosql';

// Create coordinator
const coordinator = new TwoPhaseCoordinator({
  timeoutMs: 30000,
  onPrepareTimeout: (participants) => {
    console.error('Prepare phase timed out for:', participants);
  }
});

// Define participants (each is a separate Durable Object)
const accountsDO = env.ACCOUNTS_DO.get(accountsId);
const inventoryDO = env.INVENTORY_DO.get(inventoryId);

// Execute distributed transaction
const result = await coordinator.execute([
  {
    participant: accountsDO,
    operations: [
      { sql: 'UPDATE accounts SET balance = balance - ? WHERE id = ?', params: [100, userId] }
    ]
  },
  {
    participant: inventoryDO,
    operations: [
      { sql: 'UPDATE inventory SET quantity = quantity - ? WHERE product_id = ?', params: [1, productId] }
    ]
  }
]);

if (result.committed) {
  console.log('Distributed transaction committed');
} else {
  console.log('Transaction aborted:', result.reason);
}
```

### Manual 2PC Control

```typescript
// Phase 1: Prepare
const txnId = generateTxnId();
const prepareResults = await Promise.all([
  accountsDO.prepare(txnId, operations1),
  inventoryDO.prepare(txnId, operations2)
]);

// Check all participants are prepared
const allPrepared = prepareResults.every(r => r.prepared);

if (allPrepared) {
  // Phase 2: Commit
  await Promise.all([
    accountsDO.commit(txnId),
    inventoryDO.commit(txnId)
  ]);
  console.log('Distributed commit successful');
} else {
  // Phase 2: Abort
  await Promise.all([
    accountsDO.abort(txnId),
    inventoryDO.abort(txnId)
  ]);
  console.log('Distributed transaction aborted');
}
```

### Participant Implementation

```typescript
export class AccountsDO implements DurableObject {
  private preparedTransactions = new Map<string, PreparedState>();

  async prepare(txnId: string, operations: Operation[]): Promise<PrepareResult> {
    const txnManager = this.getTransactionManager();

    try {
      await txnManager.begin({ txnId });

      // Execute operations
      for (const op of operations) {
        await this.db.run(op.sql, op.params);
      }

      // Don't commit yet - just prepare
      const prepareState = {
        context: txnManager.getContext(),
        operations
      };
      this.preparedTransactions.set(txnId, prepareState);

      return { prepared: true };
    } catch (error) {
      await txnManager.rollback();
      return { prepared: false, error: error.message };
    }
  }

  async commit(txnId: string): Promise<void> {
    const state = this.preparedTransactions.get(txnId);
    if (!state) throw new Error('Transaction not prepared');

    await this.txnManager.commit();
    this.preparedTransactions.delete(txnId);
  }

  async abort(txnId: string): Promise<void> {
    const state = this.preparedTransactions.get(txnId);
    if (!state) return;

    await this.txnManager.rollback();
    this.preparedTransactions.delete(txnId);
  }
}
```

### Saga Pattern Alternative

For long-running distributed transactions, consider the Saga pattern:

```typescript
import { SagaOrchestrator } from 'dosql';

const saga = new SagaOrchestrator();

saga.addStep({
  name: 'debit_account',
  execute: async (ctx) => {
    const result = await accountsDO.debit(ctx.userId, ctx.amount);
    return { debitId: result.id };
  },
  compensate: async (ctx, stepResult) => {
    await accountsDO.credit(ctx.userId, ctx.amount, stepResult.debitId);
  }
});

saga.addStep({
  name: 'reserve_inventory',
  execute: async (ctx) => {
    const result = await inventoryDO.reserve(ctx.productId, ctx.quantity);
    return { reservationId: result.id };
  },
  compensate: async (ctx, stepResult) => {
    await inventoryDO.releaseReservation(stepResult.reservationId);
  }
});

saga.addStep({
  name: 'create_order',
  execute: async (ctx) => {
    return await ordersDO.create(ctx.userId, ctx.productId, ctx.quantity, ctx.amount);
  },
  compensate: async (ctx, stepResult) => {
    await ordersDO.cancel(stepResult.orderId);
  }
});

// Execute saga
const result = await saga.execute({
  userId: 1,
  productId: 42,
  quantity: 2,
  amount: 99.99
});

if (result.completed) {
  console.log('Saga completed successfully');
} else {
  console.log('Saga failed, compensations applied:', result.compensations);
}
```

---

## Transaction Best Practices

### 1. Keep Transactions Short

```typescript
// BAD: Long-running transaction holding locks
await txnManager.begin();
const users = await db.query('SELECT * FROM users');
await sendEmailsToAllUsers(users); // Don't do external I/O in transactions!
await txnManager.commit();

// GOOD: Fetch data, then process outside transaction
const users = await executeReadOnly(txnManager, async () => {
  return await db.query('SELECT * FROM users');
});
await sendEmailsToAllUsers(users);
```

### 2. Use Appropriate Isolation Level

```typescript
// Use lowest isolation level that meets requirements
// For simple reads, READ_COMMITTED is often sufficient
await txnManager.begin({ isolationLevel: IsolationLevel.READ_COMMITTED });

// Use SERIALIZABLE only when necessary (e.g., financial transactions)
await txnManager.begin({ isolationLevel: IsolationLevel.SERIALIZABLE });
```

### 3. Order Lock Acquisitions Consistently

```typescript
// BAD: Inconsistent ordering can cause deadlocks
// Transaction 1: locks A, then B
// Transaction 2: locks B, then A

// GOOD: Always acquire locks in the same order
async function transferFunds(fromId: number, toId: number, amount: number) {
  // Always lock lower ID first
  const [firstId, secondId] = fromId < toId ? [fromId, toId] : [toId, fromId];

  await txnManager.begin();
  await db.run('SELECT * FROM accounts WHERE id = ? FOR UPDATE', [firstId]);
  await db.run('SELECT * FROM accounts WHERE id = ? FOR UPDATE', [secondId]);
  // Now safe to perform transfer
  await txnManager.commit();
}
```

### 4. Use Savepoints for Partial Rollback

```typescript
// Process multiple items, continue on individual failures
await txnManager.begin();

for (const item of items) {
  const result = await executeWithSavepoint(txnManager, `item_${item.id}`, async () => {
    await processItem(item);
  });

  if (!result.committed) {
    console.log(`Item ${item.id} failed, continuing with others`);
    failedItems.push(item.id);
  }
}

await txnManager.commit();
```

### 5. Handle Retries for Transient Failures

```typescript
async function executeWithRetry<T>(
  fn: () => Promise<T>,
  maxRetries: number = 3
): Promise<T> {
  let lastError: Error;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;

      // Retry on deadlock or serialization failure
      if (
        error.code === TransactionErrorCode.DEADLOCK ||
        error.code === TransactionErrorCode.SERIALIZATION_FAILURE
      ) {
        const backoff = Math.min(100 * Math.pow(2, attempt), 5000);
        await new Promise(resolve => setTimeout(resolve, backoff));
        continue;
      }

      throw error;
    }
  }

  throw lastError;
}

// Usage
await executeWithRetry(async () => {
  return await executeInTransaction(txnManager, async () => {
    // Transaction logic
  });
});
```

### 6. Set Appropriate Timeouts

```typescript
// Set transaction timeout based on expected duration
await txnManager.begin({
  timeoutMs: 5000  // 5 seconds for quick operations
});

// For longer operations, request extensions proactively
const longContext = await txnManager.begin({ timeoutMs: 30000 });

// Check remaining time before long operation
const remaining = txnManager.getRemainingTime(longContext.txnId);
if (remaining < 10000) {
  txnManager.requestExtension(longContext.txnId, 30000);
}
```

---

## Error Handling in Transactions

### Transaction Error Types

```typescript
import { TransactionError, TransactionErrorCode } from 'dosql';

try {
  await txnManager.begin();
  // ... operations
  await txnManager.commit();
} catch (error) {
  if (error instanceof TransactionError) {
    switch (error.code) {
      case TransactionErrorCode.NO_ACTIVE_TRANSACTION:
        console.error('No transaction active');
        break;

      case TransactionErrorCode.TRANSACTION_ALREADY_ACTIVE:
        console.error('Transaction already in progress');
        break;

      case TransactionErrorCode.SAVEPOINT_NOT_FOUND:
        console.error('Savepoint does not exist:', error.message);
        break;

      case TransactionErrorCode.DUPLICATE_SAVEPOINT:
        console.error('Savepoint name already used');
        break;

      case TransactionErrorCode.LOCK_TIMEOUT:
        console.error('Lock acquisition timed out');
        break;

      case TransactionErrorCode.DEADLOCK:
        console.error('Deadlock detected, transaction was victim');
        break;

      case TransactionErrorCode.SERIALIZATION_FAILURE:
        console.error('Serialization conflict');
        break;

      case TransactionErrorCode.READ_ONLY_VIOLATION:
        console.error('Write attempted in read-only transaction');
        break;

      case TransactionErrorCode.TIMEOUT:
        console.error('Transaction timed out');
        break;

      case TransactionErrorCode.WAL_FAILURE:
        console.error('WAL write failed');
        break;

      case TransactionErrorCode.ROLLBACK_FAILED:
        console.error('Rollback failed');
        break;
    }

    // Access additional context
    console.log('Transaction ID:', error.txnId);
    console.log('Cause:', error.cause);
  }
}
```

### Ensuring Cleanup

```typescript
// Use try/finally to ensure cleanup
let committed = false;

try {
  await txnManager.begin();
  // ... operations
  await txnManager.commit();
  committed = true;
} finally {
  if (!committed && txnManager.isActive()) {
    try {
      await txnManager.rollback();
    } catch (rollbackError) {
      console.error('Rollback failed:', rollbackError);
    }
  }
}
```

### Error Context and Logging

```typescript
const txnManager = createTransactionManager({
  onLongRunningTransaction: (log) => {
    // Log long-running transaction details
    console.warn('Long-running transaction detected:', {
      txnId: log.txnId,
      durationMs: log.durationMs,
      operationCount: log.operationCount,
      isolationLevel: log.isolationLevel,
      readOnly: log.readOnly,
      warningLevel: log.warningLevel,
      operations: log.operations,
      heldLocks: log.heldLocks
    });
  }
});
```

---

## Performance Considerations

### Lock Contention

```typescript
// Monitor lock contention
const lockState = lockManager.getState();
for (const [resource, state] of lockState) {
  if (state.waiters.length > 0) {
    console.log(`Contention on ${resource}: ${state.waiters.length} waiters`);
  }
}
```

### MVCC Vacuum

```typescript
// Periodically clean up old MVCC versions
const mvccStore = createMVCCStore();

// During maintenance window
const oldestActiveLsn = getOldestActiveTransactionLsn();
const cleaned = mvccStore.vacuum(oldestActiveLsn);
console.log(`Vacuumed ${cleaned} old versions`);
```

### Transaction Statistics

```typescript
const stats = txnManager.getStats();
console.log('Transaction statistics:', {
  totalStarted: stats.totalStarted,
  totalCommitted: stats.totalCommitted,
  totalRolledBack: stats.totalRolledBack,
  avgDurationMs: stats.avgDurationMs,
  activeCount: stats.activeCount
});
```

### Batch Operations

```typescript
// BAD: Multiple transactions
for (const user of users) {
  await executeInTransaction(txnManager, async () => {
    await db.run('INSERT INTO users (name) VALUES (?)', [user.name]);
  });
}

// GOOD: Single transaction with batch insert
await executeInTransaction(txnManager, async () => {
  const stmt = await db.prepare('INSERT INTO users (name) VALUES (?)');
  for (const user of users) {
    await stmt.run([user.name]);
  }
  await stmt.finalize();
});
```

---

## Example: Money Transfer

A complete example implementing a secure money transfer with all best practices:

### SQL Schema

```sql
-- .do/migrations/001_accounts.sql
CREATE TABLE accounts (
  id INTEGER PRIMARY KEY,
  user_id INTEGER NOT NULL,
  balance DECIMAL(15,2) NOT NULL DEFAULT 0.00,
  currency TEXT NOT NULL DEFAULT 'USD',
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT positive_balance CHECK (balance >= 0)
);

CREATE TABLE transfers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  from_account_id INTEGER NOT NULL,
  to_account_id INTEGER NOT NULL,
  amount DECIMAL(15,2) NOT NULL,
  currency TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  completed_at TEXT,
  FOREIGN KEY (from_account_id) REFERENCES accounts(id),
  FOREIGN KEY (to_account_id) REFERENCES accounts(id),
  CONSTRAINT positive_amount CHECK (amount > 0)
);

CREATE INDEX idx_transfers_from ON transfers(from_account_id);
CREATE INDEX idx_transfers_to ON transfers(to_account_id);
```

### TypeScript Implementation

```typescript
import {
  DB,
  executeInTransaction,
  TransactionError,
  TransactionErrorCode,
  IsolationLevel
} from 'dosql';

interface TransferResult {
  transferId: number;
  fromBalance: number;
  toBalance: number;
}

interface TransferError {
  code: string;
  message: string;
}

async function transferMoney(
  db: DB,
  fromAccountId: number,
  toAccountId: number,
  amount: number,
  currency: string = 'USD'
): Promise<{ success: true; data: TransferResult } | { success: false; error: TransferError }> {

  // Validate input
  if (amount <= 0) {
    return { success: false, error: { code: 'INVALID_AMOUNT', message: 'Amount must be positive' } };
  }

  if (fromAccountId === toAccountId) {
    return { success: false, error: { code: 'SAME_ACCOUNT', message: 'Cannot transfer to same account' } };
  }

  const txnManager = db.getTransactionManager();

  // Retry logic for transient failures
  const maxRetries = 3;
  let lastError: Error | null = null;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const result = await executeInTransaction(
        txnManager,
        async (ctx) => {
          // Lock accounts in consistent order to prevent deadlocks
          const [firstId, secondId] = fromAccountId < toAccountId
            ? [fromAccountId, toAccountId]
            : [toAccountId, fromAccountId];

          // Lock and fetch accounts
          const firstAccount = await db.get(
            'SELECT * FROM accounts WHERE id = ?',
            [firstId]
          );

          const secondAccount = await db.get(
            'SELECT * FROM accounts WHERE id = ?',
            [secondId]
          );

          // Determine which is source and destination
          const sourceAccount = fromAccountId === firstId ? firstAccount : secondAccount;
          const destAccount = fromAccountId === firstId ? secondAccount : firstAccount;

          // Validate accounts exist
          if (!sourceAccount || !destAccount) {
            throw new Error('ACCOUNT_NOT_FOUND');
          }

          // Validate currency match
          if (sourceAccount.currency !== currency || destAccount.currency !== currency) {
            throw new Error('CURRENCY_MISMATCH');
          }

          // Check sufficient balance
          if (sourceAccount.balance < amount) {
            throw new Error('INSUFFICIENT_FUNDS');
          }

          // Create transfer record
          await db.run(
            `INSERT INTO transfers (from_account_id, to_account_id, amount, currency, status)
             VALUES (?, ?, ?, ?, 'processing')`,
            [fromAccountId, toAccountId, amount, currency]
          );

          const transferId = (await db.get('SELECT last_insert_rowid() as id')).id;

          // Debit source account
          await db.run(
            `UPDATE accounts
             SET balance = balance - ?, updated_at = CURRENT_TIMESTAMP
             WHERE id = ?`,
            [amount, fromAccountId]
          );

          // Credit destination account
          await db.run(
            `UPDATE accounts
             SET balance = balance + ?, updated_at = CURRENT_TIMESTAMP
             WHERE id = ?`,
            [amount, toAccountId]
          );

          // Mark transfer as completed
          await db.run(
            `UPDATE transfers
             SET status = 'completed', completed_at = CURRENT_TIMESTAMP
             WHERE id = ?`,
            [transferId]
          );

          // Fetch final balances
          const finalSource = await db.get('SELECT balance FROM accounts WHERE id = ?', [fromAccountId]);
          const finalDest = await db.get('SELECT balance FROM accounts WHERE id = ?', [toAccountId]);

          return {
            transferId,
            fromBalance: finalSource.balance,
            toBalance: finalDest.balance
          };
        },
        {
          isolationLevel: IsolationLevel.SERIALIZABLE,
          timeoutMs: 10000
        }
      );

      if (result.committed) {
        return { success: true, data: result.value! };
      } else {
        throw result.error;
      }

    } catch (error) {
      lastError = error as Error;

      // Handle specific error types
      if (error instanceof TransactionError) {
        if (error.code === TransactionErrorCode.DEADLOCK ||
            error.code === TransactionErrorCode.SERIALIZATION_FAILURE) {
          // Retry with exponential backoff
          const backoff = Math.min(100 * Math.pow(2, attempt), 2000);
          await new Promise(resolve => setTimeout(resolve, backoff));
          continue;
        }
      }

      // Map known errors
      if (error.message === 'ACCOUNT_NOT_FOUND') {
        return { success: false, error: { code: 'ACCOUNT_NOT_FOUND', message: 'One or both accounts not found' } };
      }
      if (error.message === 'CURRENCY_MISMATCH') {
        return { success: false, error: { code: 'CURRENCY_MISMATCH', message: 'Account currencies do not match' } };
      }
      if (error.message === 'INSUFFICIENT_FUNDS') {
        return { success: false, error: { code: 'INSUFFICIENT_FUNDS', message: 'Insufficient balance' } };
      }

      // Re-throw unexpected errors
      throw error;
    }
  }

  // All retries exhausted
  return {
    success: false,
    error: {
      code: 'TRANSACTION_FAILED',
      message: `Transfer failed after ${maxRetries} attempts: ${lastError?.message}`
    }
  };
}

// Usage
const result = await transferMoney(db, 1, 2, 100.00, 'USD');

if (result.success) {
  console.log(`Transfer #${result.data.transferId} completed`);
  console.log(`Source balance: $${result.data.fromBalance}`);
  console.log(`Destination balance: $${result.data.toBalance}`);
} else {
  console.error(`Transfer failed: ${result.error.code} - ${result.error.message}`);
}
```

---

## Example: Inventory Management

A complete example for inventory management with reservations and stock updates:

### SQL Schema

```sql
-- .do/migrations/001_inventory.sql
CREATE TABLE products (
  id INTEGER PRIMARY KEY,
  sku TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE inventory (
  product_id INTEGER PRIMARY KEY,
  quantity_available INTEGER NOT NULL DEFAULT 0,
  quantity_reserved INTEGER NOT NULL DEFAULT 0,
  reorder_point INTEGER NOT NULL DEFAULT 10,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (product_id) REFERENCES products(id),
  CONSTRAINT non_negative_available CHECK (quantity_available >= 0),
  CONSTRAINT non_negative_reserved CHECK (quantity_reserved >= 0)
);

CREATE TABLE reservations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  product_id INTEGER NOT NULL,
  order_id TEXT NOT NULL,
  quantity INTEGER NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  expires_at TEXT NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (product_id) REFERENCES products(id),
  CONSTRAINT positive_quantity CHECK (quantity > 0)
);

CREATE INDEX idx_reservations_product ON reservations(product_id);
CREATE INDEX idx_reservations_order ON reservations(order_id);
CREATE INDEX idx_reservations_expires ON reservations(expires_at);
```

### TypeScript Implementation

```typescript
import {
  DB,
  executeInTransaction,
  executeWithSavepoint,
  TransactionError,
  TransactionErrorCode,
  IsolationLevel
} from 'dosql';

interface ReservationResult {
  reservationId: number;
  productId: number;
  quantity: number;
  expiresAt: string;
}

interface OrderItem {
  productId: number;
  quantity: number;
}

interface OrderResult {
  orderId: string;
  reservations: ReservationResult[];
  failedItems: { productId: number; reason: string }[];
}

class InventoryManager {
  constructor(private db: DB) {}

  /**
   * Reserve inventory for an order
   * Uses savepoints to handle partial failures
   */
  async reserveForOrder(
    orderId: string,
    items: OrderItem[],
    reservationMinutes: number = 30
  ): Promise<OrderResult> {
    const txnManager = this.db.getTransactionManager();
    const reservations: ReservationResult[] = [];
    const failedItems: { productId: number; reason: string }[] = [];

    const result = await executeInTransaction(
      txnManager,
      async (ctx) => {
        const expiresAt = new Date(Date.now() + reservationMinutes * 60 * 1000).toISOString();

        for (const item of items) {
          // Use savepoint for each item - allows partial success
          const itemResult = await executeWithSavepoint(
            txnManager,
            `reserve_${item.productId}`,
            async () => {
              // Lock the inventory row
              const inventory = await this.db.get(
                `SELECT * FROM inventory WHERE product_id = ?`,
                [item.productId]
              );

              if (!inventory) {
                throw new Error('PRODUCT_NOT_FOUND');
              }

              const availableForReservation = inventory.quantity_available - inventory.quantity_reserved;

              if (availableForReservation < item.quantity) {
                throw new Error(`INSUFFICIENT_STOCK:${availableForReservation}`);
              }

              // Create reservation
              await this.db.run(
                `INSERT INTO reservations (product_id, order_id, quantity, expires_at)
                 VALUES (?, ?, ?, ?)`,
                [item.productId, orderId, item.quantity, expiresAt]
              );

              const reservationId = (await this.db.get('SELECT last_insert_rowid() as id')).id;

              // Update reserved quantity
              await this.db.run(
                `UPDATE inventory
                 SET quantity_reserved = quantity_reserved + ?,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE product_id = ?`,
                [item.quantity, item.productId]
              );

              return {
                reservationId,
                productId: item.productId,
                quantity: item.quantity,
                expiresAt
              };
            }
          );

          if (itemResult.committed) {
            reservations.push(itemResult.value!);
          } else {
            const errorMsg = itemResult.error?.message || 'Unknown error';
            let reason = 'Reservation failed';

            if (errorMsg === 'PRODUCT_NOT_FOUND') {
              reason = 'Product not found';
            } else if (errorMsg.startsWith('INSUFFICIENT_STOCK:')) {
              const available = errorMsg.split(':')[1];
              reason = `Insufficient stock (${available} available)`;
            }

            failedItems.push({ productId: item.productId, reason });
          }
        }

        return { orderId, reservations, failedItems };
      },
      {
        isolationLevel: IsolationLevel.SERIALIZABLE,
        timeoutMs: 30000
      }
    );

    if (result.committed) {
      return result.value!;
    }

    throw result.error;
  }

  /**
   * Fulfill reservations - convert reserved to shipped
   */
  async fulfillReservations(orderId: string): Promise<{ fulfilled: number[]; released: number }> {
    const txnManager = this.db.getTransactionManager();

    return await executeInTransaction(txnManager, async () => {
      // Get active reservations for this order
      const reservations = await this.db.query(
        `SELECT * FROM reservations
         WHERE order_id = ? AND status = 'active'`,
        [orderId]
      );

      const fulfilled: number[] = [];
      let totalReleased = 0;

      for (const reservation of reservations) {
        // Lock and update inventory
        await this.db.run(
          `UPDATE inventory
           SET quantity_available = quantity_available - ?,
               quantity_reserved = quantity_reserved - ?,
               updated_at = CURRENT_TIMESTAMP
           WHERE product_id = ?`,
          [reservation.quantity, reservation.quantity, reservation.product_id]
        );

        // Mark reservation as fulfilled
        await this.db.run(
          `UPDATE reservations SET status = 'fulfilled' WHERE id = ?`,
          [reservation.id]
        );

        fulfilled.push(reservation.id);
        totalReleased += reservation.quantity;

        // Check if we need to reorder
        const inventory = await this.db.get(
          'SELECT * FROM inventory WHERE product_id = ?',
          [reservation.product_id]
        );

        if (inventory.quantity_available <= inventory.reorder_point) {
          // Trigger reorder (could emit event or insert into reorder queue)
          await this.db.run(
            `INSERT OR IGNORE INTO reorder_queue (product_id, suggested_quantity, created_at)
             VALUES (?, ?, CURRENT_TIMESTAMP)`,
            [reservation.product_id, inventory.reorder_point * 2]
          );
        }
      }

      return { fulfilled, released: totalReleased };
    });
  }

  /**
   * Release expired reservations
   */
  async releaseExpiredReservations(): Promise<{ released: number; products: number[] }> {
    const txnManager = this.db.getTransactionManager();

    return await executeInTransaction(txnManager, async () => {
      const now = new Date().toISOString();

      // Find expired reservations
      const expired = await this.db.query(
        `SELECT * FROM reservations
         WHERE status = 'active' AND expires_at < ?`,
        [now]
      );

      const products: number[] = [];
      let released = 0;

      for (const reservation of expired) {
        // Release reserved quantity
        await this.db.run(
          `UPDATE inventory
           SET quantity_reserved = quantity_reserved - ?,
               updated_at = CURRENT_TIMESTAMP
           WHERE product_id = ?`,
          [reservation.quantity, reservation.product_id]
        );

        // Mark reservation as expired
        await this.db.run(
          `UPDATE reservations SET status = 'expired' WHERE id = ?`,
          [reservation.id]
        );

        released += reservation.quantity;
        if (!products.includes(reservation.product_id)) {
          products.push(reservation.product_id);
        }
      }

      return { released, products };
    });
  }

  /**
   * Receive stock - add inventory
   */
  async receiveStock(
    productId: number,
    quantity: number
  ): Promise<{ newAvailable: number; reservedQuantity: number }> {
    const txnManager = this.db.getTransactionManager();

    return await executeInTransaction(txnManager, async () => {
      // Lock and update inventory
      const inventory = await this.db.get(
        'SELECT * FROM inventory WHERE product_id = ?',
        [productId]
      );

      if (!inventory) {
        // Create inventory record
        await this.db.run(
          `INSERT INTO inventory (product_id, quantity_available)
           VALUES (?, ?)`,
          [productId, quantity]
        );

        return { newAvailable: quantity, reservedQuantity: 0 };
      }

      // Update existing inventory
      await this.db.run(
        `UPDATE inventory
         SET quantity_available = quantity_available + ?,
             updated_at = CURRENT_TIMESTAMP
         WHERE product_id = ?`,
        [quantity, productId]
      );

      const updated = await this.db.get(
        'SELECT * FROM inventory WHERE product_id = ?',
        [productId]
      );

      return {
        newAvailable: updated.quantity_available,
        reservedQuantity: updated.quantity_reserved
      };
    });
  }

  /**
   * Get current inventory status
   */
  async getInventoryStatus(productId: number): Promise<{
    available: number;
    reserved: number;
    effectiveAvailable: number;
    activeReservations: number;
  } | null> {
    const txnManager = this.db.getTransactionManager();

    return await executeInTransaction(txnManager, async () => {
      const inventory = await this.db.get(
        'SELECT * FROM inventory WHERE product_id = ?',
        [productId]
      );

      if (!inventory) return null;

      const reservationCount = await this.db.get(
        `SELECT COUNT(*) as count FROM reservations
         WHERE product_id = ? AND status = 'active'`,
        [productId]
      );

      return {
        available: inventory.quantity_available,
        reserved: inventory.quantity_reserved,
        effectiveAvailable: inventory.quantity_available - inventory.quantity_reserved,
        activeReservations: reservationCount.count
      };
    }, { readOnly: true });
  }
}

// Usage Example
const db = await DB('inventory-system');
const inventory = new InventoryManager(db);

// Reserve items for an order
const orderResult = await inventory.reserveForOrder('ORDER-12345', [
  { productId: 1, quantity: 2 },
  { productId: 2, quantity: 1 },
  { productId: 3, quantity: 5 }
]);

console.log('Order reservations:', orderResult.reservations.length);
console.log('Failed items:', orderResult.failedItems);

// Fulfill the order
const fulfillResult = await inventory.fulfillReservations('ORDER-12345');
console.log('Fulfilled reservations:', fulfillResult.fulfilled);

// Cleanup expired reservations (run periodically)
const cleanupResult = await inventory.releaseExpiredReservations();
console.log(`Released ${cleanupResult.released} items from expired reservations`);

// Receive new stock
const stockResult = await inventory.receiveStock(1, 100);
console.log(`New available quantity: ${stockResult.newAvailable}`);
```

---

## Related Documentation

- [Getting Started](./getting-started.md) - Basic DoSQL setup and usage
- [Advanced Features](./advanced.md) - Time travel, branching, CDC
- [API Reference](./api-reference.md) - Complete API documentation
- [Architecture](./architecture.md) - System design and internals
