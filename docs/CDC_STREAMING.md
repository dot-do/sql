# DoSQL CDC Streaming Guide

**Version**: 1.0.0
**Last Updated**: 2026-01-22
**Maintainer**: Platform Team

---

## Table of Contents

1. [CDC Overview and Use Cases](#cdc-overview-and-use-cases)
2. [Enabling CDC on Tables](#enabling-cdc-on-tables)
3. [CDC Event Format](#cdc-event-format)
4. [Subscribing to CDC Streams](#subscribing-to-cdc-streams)
5. [Filtering CDC Events](#filtering-cdc-events)
6. [Ordering Guarantees (LSN)](#ordering-guarantees-lsn)
7. [Error Handling and Retries](#error-handling-and-retries)
8. [Backpressure Management](#backpressure-management)
9. [CDC to Lakehouse Integration](#cdc-to-lakehouse-integration)
10. [Example: Real-time Dashboard](#example-real-time-dashboard)
11. [Example: Event Sourcing](#example-event-sourcing)
12. [Example: Cache Invalidation](#example-cache-invalidation)

---

## CDC Overview and Use Cases

Change Data Capture (CDC) in DoSQL provides real-time streaming of database changes from the Write-Ahead Log (WAL). This enables a wide range of use cases including data synchronization, analytics pipelines, and event-driven architectures.

### What is CDC?

CDC captures all INSERT, UPDATE, and DELETE operations as they occur, along with transaction boundaries (BEGIN, COMMIT, ROLLBACK). Each change event includes:

- The operation type (insert/update/delete)
- The affected table
- The new data (for inserts and updates)
- The previous data (for updates and deletes)
- Transaction context and ordering information

### Common Use Cases

| Use Case | Description |
|----------|-------------|
| **Real-time Analytics** | Stream changes to analytical systems for live dashboards |
| **Data Replication** | Replicate data across regions or to external databases |
| **Cache Invalidation** | Automatically invalidate caches when underlying data changes |
| **Event Sourcing** | Build event-sourced architectures on top of relational data |
| **Audit Logging** | Create immutable audit trails of all data modifications |
| **Search Index Updates** | Keep search indices synchronized with database state |
| **Microservice Integration** | Trigger downstream services when data changes |

### Architecture Overview

```
                                 +------------------+
                                 |  CDC Consumer 1  |
                                 |  (Dashboard)     |
                                 +------------------+
                                        ^
                                        |
+----------+    +-------+    +----------+----------+
| DoSQL    | -> | WAL   | -> | CDC Stream         |
| Writers  |    | Log   |    | (Subscription)     |
+----------+    +-------+    +----------+----------+
                                        |
                                        v
                                 +------------------+
                                 |  CDC Consumer 2  |
                                 |  (Lakehouse)     |
                                 +------------------+
```

---

## Enabling CDC on Tables

CDC is enabled by default on all tables in DoSQL. The WAL automatically captures all changes, making them available for streaming without additional configuration.

### Automatic CDC Capture

Every write operation to DoSQL generates WAL entries:

```typescript
import { createDoSQL } from '@dotdo/dosql';

const db = createDoSQL(env.DOSQL);

// These operations automatically generate CDC events
await db.exec('INSERT INTO users (id, name, email) VALUES (?, ?, ?)',
  ['user-1', 'Alice', 'alice@example.com']);

await db.exec('UPDATE users SET name = ? WHERE id = ?',
  ['Alice Smith', 'user-1']);

await db.exec('DELETE FROM users WHERE id = ?',
  ['user-1']);
```

### WAL Configuration for CDC

Configure WAL behavior for optimal CDC streaming in your Durable Object:

```typescript
import { createCDC } from '@dotdo/dosql/cdc';
import type { WALConfig } from '@dotdo/dosql/wal';

// Custom WAL configuration for high-throughput CDC
const walConfig: Partial<WALConfig> = {
  targetSegmentSize: 2 * 1024 * 1024, // 2MB segments
  maxEntriesPerSegment: 10000,        // Flush after 10k entries
  verifyChecksums: true,              // Ensure data integrity
  autoArchive: true,                  // Archive old segments
};

// Initialize CDC with custom config
const cdc = createCDC(backend, walConfig);
```

### Table-Specific CDC Options

While CDC captures all tables by default, you can configure table-specific behavior:

```typescript
// Create a filtered subscription for specific tables
const subscription = cdc.subscribe({
  fromLSN: 0n,
  filter: {
    tables: ['users', 'orders', 'payments'], // Only these tables
  },
});
```

---

## CDC Event Format

DoSQL CDC provides two event formats: raw WAL entries and structured change events.

### WAL Entry Format

The raw WAL entry format provides direct access to log entries:

```typescript
interface WALEntry {
  /** Log Sequence Number - monotonically increasing */
  lsn: bigint;
  /** Timestamp when entry was created (Unix ms) */
  timestamp: number;
  /** Transaction ID for grouping related operations */
  txnId: string;
  /** Operation type */
  op: 'INSERT' | 'UPDATE' | 'DELETE' | 'BEGIN' | 'COMMIT' | 'ROLLBACK';
  /** Target table name */
  table: string;
  /** Primary key (for UPDATE/DELETE) */
  key?: Uint8Array;
  /** Previous value (for UPDATE/DELETE) */
  before?: Uint8Array;
  /** New value (for INSERT/UPDATE) */
  after?: Uint8Array;
}
```

### Structured Change Event Format

For easier consumption, use the structured change event format:

```typescript
interface ChangeEvent<T = unknown> {
  /** Unique event ID (based on LSN) */
  id: string;
  /** Type of change */
  type: 'insert' | 'update' | 'delete';
  /** Table name */
  table: string;
  /** Transaction ID */
  txnId: string;
  /** Event timestamp */
  timestamp: Date;
  /** LSN of the change */
  lsn: bigint;
  /** The new value (for insert/update) */
  data?: T;
  /** The previous value (for update/delete) */
  oldData?: T;
  /** Primary key (if available) */
  key?: Uint8Array;
}

interface TransactionEvent {
  type: 'begin' | 'commit' | 'rollback';
  txnId: string;
  timestamp: Date;
  lsn: bigint;
}
```

### Example Event Payloads

**Insert Event:**
```json
{
  "id": "1001",
  "type": "insert",
  "table": "users",
  "txnId": "txn_abc123",
  "timestamp": "2026-01-22T10:30:00.000Z",
  "lsn": 1001,
  "data": {
    "id": "user-1",
    "name": "Alice",
    "email": "alice@example.com",
    "created_at": 1737542400000
  }
}
```

**Update Event:**
```json
{
  "id": "1002",
  "type": "update",
  "table": "users",
  "txnId": "txn_def456",
  "timestamp": "2026-01-22T10:31:00.000Z",
  "lsn": 1002,
  "data": {
    "id": "user-1",
    "name": "Alice Smith",
    "email": "alice@example.com",
    "updated_at": 1737542460000
  },
  "oldData": {
    "id": "user-1",
    "name": "Alice",
    "email": "alice@example.com",
    "created_at": 1737542400000
  }
}
```

**Delete Event:**
```json
{
  "id": "1003",
  "type": "delete",
  "table": "users",
  "txnId": "txn_ghi789",
  "timestamp": "2026-01-22T10:32:00.000Z",
  "lsn": 1003,
  "oldData": {
    "id": "user-1",
    "name": "Alice Smith",
    "email": "alice@example.com"
  }
}
```

---

## Subscribing to CDC Streams

DoSQL provides multiple ways to subscribe to CDC streams: async iterators, callback-based streams, and replication slots for durable consumption.

### Basic Subscription (Async Iterator)

The simplest approach uses async iterators:

```typescript
import { createCDC } from '@dotdo/dosql/cdc';

const cdc = createCDC(backend);
const subscription = cdc.subscribe();

// Subscribe to raw WAL entries
for await (const entry of subscription.subscribe(0n)) {
  console.log(`[${entry.op}] ${entry.table} at LSN ${entry.lsn}`);

  if (entry.op === 'INSERT') {
    const data = JSON.parse(new TextDecoder().decode(entry.after!));
    console.log('New row:', data);
  }
}
```

### Typed Change Events

For typed data with automatic decoding:

```typescript
interface User {
  id: string;
  name: string;
  email: string;
  created_at: number;
}

const subscription = cdc.subscribe({
  includeTransactionControl: false, // Skip BEGIN/COMMIT/ROLLBACK
});

// Subscribe with type-safe events
for await (const event of subscription.subscribeChanges<User>(
  0n, // Start from beginning
  { tables: ['users'] }, // Filter to users table
  (data) => JSON.parse(new TextDecoder().decode(data)) // Decoder
)) {
  if (event.type === 'insert') {
    console.log(`New user: ${event.data?.name}`);
  } else if (event.type === 'update') {
    console.log(`Updated: ${event.oldData?.name} -> ${event.data?.name}`);
  } else if (event.type === 'delete') {
    console.log(`Deleted: ${event.oldData?.name}`);
  }
}
```

### Callback-Based Stream

For callback-style processing with pause/resume support:

```typescript
import { createCDCStream } from '@dotdo/dosql/cdc';

const stream = createCDCStream(reader, {
  fromLSN: 0n,
  handler: {
    onChange: async (event) => {
      console.log(`Change: ${event.type} on ${event.table}`);
      // Process the change
      await processChange(event);
    },
    onTransaction: async (event) => {
      if (event.type === 'commit') {
        console.log(`Transaction ${event.txnId} committed`);
      }
    },
    onError: (error) => {
      console.error('CDC error:', error);
    },
    onEnd: () => {
      console.log('Stream ended');
    },
  },
  decoder: (data) => JSON.parse(new TextDecoder().decode(data)),
  autoAck: true, // Automatically advance position
});

// Control the stream
stream.start();

// Pause during high load
if (systemUnderPressure) {
  stream.pause();
}

// Resume when ready
stream.resume();

// Check status
const status = stream.getStatus();
console.log(`Processed ${status.entriesProcessed} entries`);

// Stop when done
stream.stop();
```

### Replication Slots (Durable Consumption)

For persistent position tracking across restarts:

```typescript
// Create a replication slot for durable consumption
const slots = cdc.slots;

// Create slot (typically done once during setup)
await slots.createSlot('analytics-consumer', 0n, {
  tables: ['orders', 'payments'],
  operations: ['INSERT', 'UPDATE'],
});

// Later: resume from saved position
async function startConsumer() {
  const slot = await slots.getSlot('analytics-consumer');
  if (!slot) {
    throw new Error('Slot not found');
  }

  console.log(`Resuming from LSN ${slot.acknowledgedLSN}`);

  const subscription = await slots.subscribeFromSlot('analytics-consumer');

  for await (const entry of subscription.subscribe(slot.acknowledgedLSN)) {
    // Process entry
    await processEntry(entry);

    // Update slot position after successful processing
    await slots.updateSlot('analytics-consumer', entry.lsn);
  }
}

// List all slots
const allSlots = await slots.listSlots();
console.log('Active consumers:', allSlots.map(s => s.name));

// Delete unused slot
await slots.deleteSlot('old-consumer');
```

### Table-Specific Subscription Helper

For subscribing to a single table:

```typescript
import { subscribeTable } from '@dotdo/dosql/cdc';

interface Order {
  id: string;
  user_id: string;
  total: number;
  status: string;
}

// Simple single-table subscription
for await (const event of subscribeTable<Order>(
  reader,
  'orders',
  0n,
  (data) => JSON.parse(new TextDecoder().decode(data))
)) {
  console.log(`Order ${event.data?.id}: ${event.data?.status}`);
}
```

---

## Filtering CDC Events

DoSQL CDC supports flexible filtering to receive only relevant events.

### Filter Options

```typescript
interface CDCFilter {
  /** Filter by table names */
  tables?: string[];
  /** Filter by operation types */
  operations?: ('INSERT' | 'UPDATE' | 'DELETE' | 'BEGIN' | 'COMMIT' | 'ROLLBACK')[];
  /** Filter by transaction IDs */
  txnIds?: string[];
  /** Custom predicate function */
  predicate?: (entry: WALEntry) => boolean;
}
```

### Filter by Tables

```typescript
const subscription = cdc.subscribe({
  filter: {
    // Only receive events from these tables
    tables: ['users', 'orders', 'payments'],
  },
});
```

### Filter by Operations

```typescript
const subscription = cdc.subscribe({
  filter: {
    // Only receive INSERT and UPDATE events
    operations: ['INSERT', 'UPDATE'],
  },
});
```

### Combined Filters

```typescript
const subscription = cdc.subscribe({
  filter: {
    tables: ['orders'],
    operations: ['INSERT', 'UPDATE'],
  },
  includeTransactionControl: false, // Exclude BEGIN/COMMIT/ROLLBACK
});
```

### Custom Predicate Filter

For complex filtering logic:

```typescript
const subscription = cdc.subscribe({
  filter: {
    predicate: (entry) => {
      // Only events for orders over $100
      if (entry.table !== 'orders') return false;
      if (!entry.after) return false;

      const order = JSON.parse(new TextDecoder().decode(entry.after));
      return order.total > 100;
    },
  },
});
```

### Real-World Filter Examples

**E-commerce order updates:**
```typescript
const orderUpdates = cdc.subscribe({
  filter: {
    tables: ['orders'],
    operations: ['UPDATE'],
    predicate: (entry) => {
      if (!entry.before || !entry.after) return false;
      const before = JSON.parse(new TextDecoder().decode(entry.before));
      const after = JSON.parse(new TextDecoder().decode(entry.after));
      // Only when status changes
      return before.status !== after.status;
    },
  },
});
```

**User activity monitoring:**
```typescript
const userActivity = cdc.subscribe({
  filter: {
    tables: ['sessions', 'page_views', 'events'],
    operations: ['INSERT'],
  },
});
```

---

## Ordering Guarantees (LSN)

DoSQL CDC provides strong ordering guarantees through Log Sequence Numbers (LSN).

### LSN Guarantees

1. **Monotonically Increasing**: LSNs always increase; newer events have higher LSNs
2. **Gap-Free within Transactions**: All entries in a transaction have consecutive LSNs
3. **Total Order**: All events across all tables share a single LSN sequence
4. **Durability**: Once assigned, an LSN never changes

### Understanding LSN Ordering

```typescript
// LSNs provide total ordering across all tables
// Example sequence:
// LSN 1001: INSERT users
// LSN 1002: INSERT orders
// LSN 1003: UPDATE users
// LSN 1004: DELETE orders

for await (const event of subscription.subscribe(1000n)) {
  console.log(`LSN ${event.lsn}: ${event.op} on ${event.table}`);

  // Events are always delivered in LSN order
  // You can safely assume: event[n].lsn < event[n+1].lsn
}
```

### Transaction Ordering

Transactions are ordered by their COMMIT LSN:

```typescript
const subscription = cdc.subscribe({
  includeTransactionControl: true,
});

let currentTxn: string | null = null;
let txnEvents: ChangeEvent[] = [];

for await (const event of subscription.subscribeChanges(0n)) {
  if (event.type === 'begin') {
    currentTxn = event.txnId;
    txnEvents = [];
  } else if (event.type === 'commit') {
    // Process all events from this transaction atomically
    await processTransaction(txnEvents);
    currentTxn = null;
  } else if (event.type === 'rollback') {
    // Discard events from rolled-back transaction
    txnEvents = [];
    currentTxn = null;
  } else if (currentTxn) {
    txnEvents.push(event);
  }
}
```

### Resuming from LSN

Use LSN for exactly-once processing:

```typescript
// Save checkpoint
async function saveCheckpoint(lsn: bigint) {
  await env.KV.put('cdc-checkpoint', lsn.toString());
}

// Load checkpoint
async function loadCheckpoint(): Promise<bigint> {
  const saved = await env.KV.get('cdc-checkpoint');
  return saved ? BigInt(saved) : 0n;
}

// Resume from last checkpoint
const lastLSN = await loadCheckpoint();
console.log(`Resuming from LSN ${lastLSN}`);

for await (const event of subscription.subscribe(lastLSN)) {
  await processEvent(event);
  await saveCheckpoint(event.lsn);
}
```

### LSN vs Timestamp

While events have both LSN and timestamp, prefer LSN for ordering:

```typescript
// BAD: Using timestamps can miss events or process duplicates
// Timestamps may have collisions or clock skew

// GOOD: LSN provides guaranteed ordering
for await (const event of subscription.subscribe(lastProcessedLSN)) {
  // Guaranteed to receive all events after lastProcessedLSN
  // No duplicates, no gaps
}
```

---

## Error Handling and Retries

Robust CDC consumers must handle errors gracefully and implement retry logic.

### CDC Error Types

```typescript
import { CDCError, CDCErrorCode } from '@dotdo/dosql/cdc';

enum CDCErrorCode {
  SUBSCRIPTION_FAILED = 'CDC_SUBSCRIPTION_FAILED',
  LSN_NOT_FOUND = 'CDC_LSN_NOT_FOUND',      // LSN too old (compacted)
  SLOT_NOT_FOUND = 'CDC_SLOT_NOT_FOUND',
  SLOT_EXISTS = 'CDC_SLOT_EXISTS',
  BUFFER_OVERFLOW = 'CDC_BUFFER_OVERFLOW',
  DECODE_ERROR = 'CDC_DECODE_ERROR',
}
```

### Basic Error Handling

```typescript
import { CDCError, CDCErrorCode } from '@dotdo/dosql/cdc';

async function runCDCConsumer() {
  try {
    for await (const event of subscription.subscribeChanges(lastLSN)) {
      await processEvent(event);
    }
  } catch (error) {
    if (error instanceof CDCError) {
      switch (error.code) {
        case CDCErrorCode.LSN_NOT_FOUND:
          console.error(`LSN ${error.lsn} no longer available - resetting to beginning`);
          // WAL has been compacted, reset to earliest available
          await resetToEarliestLSN();
          break;

        case CDCErrorCode.DECODE_ERROR:
          console.error(`Failed to decode event at LSN ${error.lsn}:`, error.message);
          // Skip corrupted event and continue
          lastLSN = error.lsn ?? lastLSN;
          break;

        case CDCErrorCode.BUFFER_OVERFLOW:
          console.error('Consumer too slow - events being dropped');
          // Implement backpressure or increase buffer
          break;

        default:
          throw error;
      }
    } else {
      throw error;
    }
  }
}
```

### Retry with Exponential Backoff

```typescript
interface RetryConfig {
  maxAttempts: number;
  initialDelayMs: number;
  maxDelayMs: number;
  backoffMultiplier: number;
}

const DEFAULT_RETRY: RetryConfig = {
  maxAttempts: 5,
  initialDelayMs: 100,
  maxDelayMs: 30000,
  backoffMultiplier: 2,
};

async function withRetry<T>(
  operation: () => Promise<T>,
  config: RetryConfig = DEFAULT_RETRY
): Promise<T> {
  let lastError: Error | undefined;
  let delay = config.initialDelayMs;

  for (let attempt = 1; attempt <= config.maxAttempts; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));

      if (attempt === config.maxAttempts) {
        break;
      }

      console.warn(`Attempt ${attempt} failed, retrying in ${delay}ms:`, lastError.message);
      await new Promise(resolve => setTimeout(resolve, delay));

      delay = Math.min(delay * config.backoffMultiplier, config.maxDelayMs);
    }
  }

  throw lastError;
}

// Usage
async function processEventWithRetry(event: ChangeEvent) {
  await withRetry(async () => {
    await sendToAnalytics(event);
  });
}
```

### Dead Letter Queue Pattern

```typescript
interface DeadLetterEvent {
  event: ChangeEvent;
  error: string;
  attempts: number;
  firstFailedAt: number;
  lastFailedAt: number;
}

class CDCConsumerWithDLQ {
  private dlq: Map<string, DeadLetterEvent> = new Map();

  async processEvent(event: ChangeEvent) {
    try {
      await withRetry(async () => {
        await this.handleEvent(event);
      });
    } catch (error) {
      // Move to DLQ after all retries exhausted
      await this.addToDeadLetterQueue(event, error);
    }
  }

  private async addToDeadLetterQueue(event: ChangeEvent, error: unknown) {
    const dlqEvent: DeadLetterEvent = {
      event,
      error: error instanceof Error ? error.message : String(error),
      attempts: 5,
      firstFailedAt: Date.now(),
      lastFailedAt: Date.now(),
    };

    this.dlq.set(event.id, dlqEvent);

    // Persist to storage for manual review
    await this.backend.write(
      `_dlq/${event.id}.json`,
      new TextEncoder().encode(JSON.stringify(dlqEvent))
    );

    console.error(`Event ${event.id} moved to DLQ:`, dlqEvent.error);
  }

  async retryDeadLetterQueue() {
    for (const [id, dlqEvent] of this.dlq) {
      try {
        await this.handleEvent(dlqEvent.event);
        this.dlq.delete(id);
        await this.backend.delete(`_dlq/${id}.json`);
        console.log(`Successfully reprocessed DLQ event ${id}`);
      } catch (error) {
        dlqEvent.attempts++;
        dlqEvent.lastFailedAt = Date.now();
        console.error(`DLQ retry failed for ${id}:`, error);
      }
    }
  }
}
```

---

## Backpressure Management

When consumers cannot keep up with the event rate, backpressure management prevents memory exhaustion and event loss.

### Buffer Configuration

```typescript
const subscription = cdc.subscribe({
  // Maximum entries to buffer before applying backpressure
  maxBufferSize: 1000,
  // Batch size for reading entries
  batchSize: 100,
  // Poll interval in ms
  pollInterval: 100,
});
```

### Handling Backpressure Signals

When streaming to lakehouse, handle backpressure signals:

```typescript
import { createLakehouseStreamer, type BackpressureSignal } from '@dotdo/dosql/cdc';

const streamer = createLakehouseStreamer(reader, backend, {
  lakehouseUrl: 'wss://lakehouse.example.com',
  sourceDoId: 'my-do-instance',
  maxBatchSize: 1000,
  maxBatchAge: 5000, // 5 seconds
});

streamer.onBackpressure((signal: BackpressureSignal) => {
  switch (signal.type) {
    case 'pause':
      console.log(`Pausing: buffer at ${signal.bufferUtilization * 100}%`);
      // Stop sending until resume signal
      break;

    case 'slow_down':
      console.log(`Slowing down: suggested delay ${signal.suggestedDelayMs}ms`);
      // Reduce send rate
      break;

    case 'resume':
      console.log('Resuming normal operation');
      // Resume normal sending
      break;
  }
});
```

### Adaptive Batch Processing

Adjust batch size based on processing speed:

```typescript
class AdaptiveBatchProcessor {
  private batchSize = 100;
  private readonly minBatchSize = 10;
  private readonly maxBatchSize = 1000;
  private processingTimes: number[] = [];

  async processBatch(events: ChangeEvent[]) {
    const start = Date.now();

    for (const event of events) {
      await this.processEvent(event);
    }

    const duration = Date.now() - start;
    this.recordProcessingTime(duration, events.length);
  }

  private recordProcessingTime(duration: number, count: number) {
    const perEventMs = duration / count;
    this.processingTimes.push(perEventMs);

    // Keep last 10 measurements
    if (this.processingTimes.length > 10) {
      this.processingTimes.shift();
    }

    // Adjust batch size based on average processing time
    const avgMs = this.processingTimes.reduce((a, b) => a + b, 0) / this.processingTimes.length;

    if (avgMs < 10) {
      // Processing fast, increase batch size
      this.batchSize = Math.min(this.batchSize * 1.5, this.maxBatchSize);
    } else if (avgMs > 50) {
      // Processing slow, decrease batch size
      this.batchSize = Math.max(this.batchSize * 0.75, this.minBatchSize);
    }
  }

  getBatchSize(): number {
    return Math.floor(this.batchSize);
  }
}
```

### Rate Limiting Consumer

```typescript
class RateLimitedConsumer {
  private tokens: number;
  private readonly maxTokens: number;
  private readonly refillRate: number; // tokens per second
  private lastRefill: number;

  constructor(maxTokens: number, refillRate: number) {
    this.maxTokens = maxTokens;
    this.tokens = maxTokens;
    this.refillRate = refillRate;
    this.lastRefill = Date.now();
  }

  private refillTokens() {
    const now = Date.now();
    const elapsed = (now - this.lastRefill) / 1000;
    this.tokens = Math.min(this.maxTokens, this.tokens + elapsed * this.refillRate);
    this.lastRefill = now;
  }

  async consume(count: number = 1): Promise<void> {
    this.refillTokens();

    while (this.tokens < count) {
      const waitTime = (count - this.tokens) / this.refillRate * 1000;
      await new Promise(resolve => setTimeout(resolve, waitTime));
      this.refillTokens();
    }

    this.tokens -= count;
  }
}

// Usage: process max 1000 events/second
const rateLimiter = new RateLimitedConsumer(100, 1000);

for await (const event of subscription.subscribeChanges(0n)) {
  await rateLimiter.consume();
  await processEvent(event);
}
```

---

## CDC to Lakehouse Integration

DoSQL provides first-class support for streaming CDC events to DoLake for analytical processing.

### Lakehouse Streamer Setup

```typescript
import { createLakehouseStreamer } from '@dotdo/dosql/cdc';

const streamer = createLakehouseStreamer(walReader, backend, {
  lakehouseUrl: 'wss://your-dolake-instance.workers.dev',
  sourceDoId: env.DO_ID,
  sourceShardName: 'shard-1',

  // Batching configuration
  maxBatchSize: 1000,      // Events per batch
  maxBatchAge: 5000,       // Max 5 seconds before flush

  // Reliability configuration
  retry: {
    maxAttempts: 3,
    initialDelayMs: 100,
    maxDelayMs: 10000,
    backoffMultiplier: 2,
  },

  heartbeatInterval: 30000, // 30 second heartbeat
  exactlyOnce: true,        // Enable deduplication
});

// Connect and start streaming
await streamer.connect();
streamer.start(lastCheckpointLSN);

// Monitor status
setInterval(() => {
  const status = streamer.getStatus();
  console.log(`Streaming: ${status.state}, sent: ${status.totalEntriesSent}, pending: ${status.pendingBatches}`);
}, 10000);
```

### CDC Batch Format

Batches sent to lakehouse include metadata for deduplication and ordering:

```typescript
interface CDCBatch {
  /** Unique batch ID for deduplication */
  batchId: string;
  /** Source DO identifier */
  sourceDoId: string;
  /** Sequence number for ordering */
  sequenceNumber: number;
  /** First LSN in batch */
  firstLSN: bigint;
  /** Last LSN in batch */
  lastLSN: bigint;
  /** Change events in this batch */
  events: CDCEvent[];
  /** Batch creation timestamp */
  createdAt: number;
  /** Estimated size in bytes */
  sizeBytes: number;
  /** Retry metadata */
  isRetry: boolean;
  retryCount: number;
}
```

### Exactly-Once Delivery

Enable exactly-once semantics with checkpointing:

```typescript
const streamer = createLakehouseStreamer(reader, backend, {
  exactlyOnce: true,
  // ... other config
});

// Get checkpoint for recovery
const checkpoint = streamer.getCheckpoint();
if (checkpoint) {
  console.log(`Last committed LSN: ${checkpoint.committedLSN}`);
  console.log(`Pending batches: ${checkpoint.pendingBatchIds.length}`);
}

// On restart, resume from checkpoint
streamer.start(checkpoint?.committedLSN ?? 0n);
```

### Schema Evolution Support

Track schema changes alongside data:

```typescript
interface SchemaChangeEvent {
  type: 'schema_change';
  table: string;
  changeType: 'add_column' | 'drop_column' | 'alter_column' | 'create_table' | 'drop_table';
  column?: string;
  oldType?: string;
  newType?: string;
  schemaVersion: number;
  timestamp: number;
  lsn: bigint;
}

// Schema changes are included in the CDC stream
for await (const event of subscription.subscribeChanges(0n)) {
  if ('changeType' in event) {
    // Handle schema evolution
    await updateLakehouseSchema(event as SchemaChangeEvent);
  } else {
    // Handle data change
    await processDataChange(event);
  }
}
```

### WAL Capturer for Batch Processing

For high-throughput scenarios, use the WAL capturer:

```typescript
import { createWALCapturer, type CaptureBatch } from '@dotdo/dosql/cdc';

const capturer = createWALCapturer(walReader, {
  fromLSN: lastLSN,
  maxBatchSize: 1000,
  maxBatchAge: 5000,
  filter: {
    tables: ['orders', 'order_items', 'payments'],
  },
});

// Continuous capture with batching
for await (const batch of capturer.captureStream()) {
  console.log(`Batch ${batch.batchId}: ${batch.entries.length} entries`);

  // Send to lakehouse
  await sendToLakehouse(batch);

  // Update position
  capturer.setPosition(batch.endLSN);
}
```

---

## Example: Real-time Dashboard

Build a real-time analytics dashboard that updates instantly when data changes.

### Dashboard Architecture

```
+----------+     +----------+     +-----------+     +------------+
|  DoSQL   | --> |   CDC    | --> | WebSocket | --> | Dashboard  |
| Database |     | Consumer |     |  Server   |     |   Client   |
+----------+     +----------+     +-----------+     +------------+
```

### Server-Side CDC Consumer

```typescript
// dashboard-cdc.ts
import { createCDC, type ChangeEvent } from '@dotdo/dosql/cdc';

interface DashboardMetrics {
  totalOrders: number;
  totalRevenue: number;
  ordersByStatus: Record<string, number>;
  recentOrders: Order[];
}

interface Order {
  id: string;
  user_id: string;
  total: number;
  status: string;
  created_at: number;
}

export class DashboardCDCConsumer {
  private metrics: DashboardMetrics = {
    totalOrders: 0,
    totalRevenue: 0,
    ordersByStatus: {},
    recentOrders: [],
  };
  private subscribers: Set<WebSocket> = new Set();

  constructor(private cdc: ReturnType<typeof createCDC>) {}

  async start() {
    const subscription = this.cdc.subscribe({
      filter: {
        tables: ['orders'],
      },
      includeTransactionControl: false,
    });

    for await (const event of subscription.subscribeChanges<Order>(
      0n,
      { tables: ['orders'] },
      (data) => JSON.parse(new TextDecoder().decode(data))
    )) {
      await this.handleOrderChange(event);
    }
  }

  private async handleOrderChange(event: ChangeEvent<Order>) {
    switch (event.type) {
      case 'insert':
        this.handleNewOrder(event.data!);
        break;
      case 'update':
        this.handleOrderUpdate(event.oldData!, event.data!);
        break;
      case 'delete':
        this.handleOrderDelete(event.oldData!);
        break;
    }

    // Broadcast update to all connected clients
    this.broadcastMetrics();
  }

  private handleNewOrder(order: Order) {
    this.metrics.totalOrders++;
    this.metrics.totalRevenue += order.total;
    this.metrics.ordersByStatus[order.status] =
      (this.metrics.ordersByStatus[order.status] || 0) + 1;

    // Keep last 10 orders
    this.metrics.recentOrders.unshift(order);
    if (this.metrics.recentOrders.length > 10) {
      this.metrics.recentOrders.pop();
    }
  }

  private handleOrderUpdate(oldOrder: Order, newOrder: Order) {
    // Update status counts
    if (oldOrder.status !== newOrder.status) {
      this.metrics.ordersByStatus[oldOrder.status]--;
      this.metrics.ordersByStatus[newOrder.status] =
        (this.metrics.ordersByStatus[newOrder.status] || 0) + 1;
    }

    // Update revenue if total changed
    if (oldOrder.total !== newOrder.total) {
      this.metrics.totalRevenue += (newOrder.total - oldOrder.total);
    }

    // Update in recent orders
    const idx = this.metrics.recentOrders.findIndex(o => o.id === newOrder.id);
    if (idx >= 0) {
      this.metrics.recentOrders[idx] = newOrder;
    }
  }

  private handleOrderDelete(order: Order) {
    this.metrics.totalOrders--;
    this.metrics.totalRevenue -= order.total;
    this.metrics.ordersByStatus[order.status]--;
    this.metrics.recentOrders = this.metrics.recentOrders.filter(o => o.id !== order.id);
  }

  private broadcastMetrics() {
    const message = JSON.stringify({
      type: 'metrics_update',
      data: this.metrics,
      timestamp: Date.now(),
    });

    for (const ws of this.subscribers) {
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(message);
      }
    }
  }

  subscribe(ws: WebSocket) {
    this.subscribers.add(ws);
    // Send current state immediately
    ws.send(JSON.stringify({
      type: 'initial_state',
      data: this.metrics,
      timestamp: Date.now(),
    }));
  }

  unsubscribe(ws: WebSocket) {
    this.subscribers.delete(ws);
  }

  getMetrics(): DashboardMetrics {
    return { ...this.metrics };
  }
}
```

### WebSocket Handler

```typescript
// worker.ts
import { DashboardCDCConsumer } from './dashboard-cdc';

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === '/ws/dashboard') {
      // Handle WebSocket upgrade
      const upgradeHeader = request.headers.get('Upgrade');
      if (upgradeHeader !== 'websocket') {
        return new Response('Expected WebSocket', { status: 426 });
      }

      const [client, server] = Object.values(new WebSocketPair());

      // Get the CDC consumer from Durable Object
      const doId = env.DOSQL.idFromName('main');
      const doStub = env.DOSQL.get(doId);

      // Register WebSocket with CDC consumer
      server.accept();
      await doStub.subscribeWebSocket(server);

      server.addEventListener('close', async () => {
        await doStub.unsubscribeWebSocket(server);
      });

      return new Response(null, { status: 101, webSocket: client });
    }

    // REST endpoint for current metrics
    if (url.pathname === '/api/dashboard/metrics') {
      const doId = env.DOSQL.idFromName('main');
      const doStub = env.DOSQL.get(doId);
      const metrics = await doStub.getDashboardMetrics();

      return Response.json(metrics);
    }

    return new Response('Not Found', { status: 404 });
  },
};
```

### Client-Side Dashboard

```typescript
// dashboard-client.ts
interface DashboardMetrics {
  totalOrders: number;
  totalRevenue: number;
  ordersByStatus: Record<string, number>;
  recentOrders: Order[];
}

class DashboardClient {
  private ws: WebSocket | null = null;
  private metrics: DashboardMetrics | null = null;
  private listeners: Set<(metrics: DashboardMetrics) => void> = new Set();

  connect(url: string) {
    this.ws = new WebSocket(url);

    this.ws.onmessage = (event) => {
      const message = JSON.parse(event.data);

      if (message.type === 'initial_state' || message.type === 'metrics_update') {
        this.metrics = message.data;
        this.notifyListeners();
      }
    };

    this.ws.onclose = () => {
      console.log('WebSocket closed, reconnecting...');
      setTimeout(() => this.connect(url), 1000);
    };

    this.ws.onerror = (error) => {
      console.error('WebSocket error:', error);
    };
  }

  private notifyListeners() {
    if (this.metrics) {
      for (const listener of this.listeners) {
        listener(this.metrics);
      }
    }
  }

  subscribe(callback: (metrics: DashboardMetrics) => void) {
    this.listeners.add(callback);
    if (this.metrics) {
      callback(this.metrics);
    }
    return () => this.listeners.delete(callback);
  }

  disconnect() {
    this.ws?.close();
    this.ws = null;
  }
}

// React hook example
function useDashboardMetrics() {
  const [metrics, setMetrics] = useState<DashboardMetrics | null>(null);
  const clientRef = useRef<DashboardClient | null>(null);

  useEffect(() => {
    clientRef.current = new DashboardClient();
    clientRef.current.connect('wss://your-api.workers.dev/ws/dashboard');

    const unsubscribe = clientRef.current.subscribe(setMetrics);

    return () => {
      unsubscribe();
      clientRef.current?.disconnect();
    };
  }, []);

  return metrics;
}
```

---

## Example: Event Sourcing

Implement event sourcing pattern using DoSQL CDC as the event store.

### Event Sourcing Architecture

```
                           +------------------+
                           | Event Store      |
Commands --> Aggregates -> | (DoSQL WAL/CDC) | -> Projections -> Read Models
                           +------------------+
                                    |
                                    v
                           +------------------+
                           | Event Handlers   |
                           +------------------+
```

### Domain Events and Aggregates

```typescript
// events.ts
interface DomainEvent {
  eventId: string;
  aggregateId: string;
  aggregateType: string;
  eventType: string;
  version: number;
  timestamp: number;
  payload: unknown;
  metadata: {
    userId?: string;
    correlationId?: string;
    causationId?: string;
  };
}

// Order aggregate events
interface OrderCreated extends DomainEvent {
  eventType: 'OrderCreated';
  payload: {
    customerId: string;
    items: Array<{ productId: string; quantity: number; price: number }>;
    shippingAddress: string;
  };
}

interface OrderItemAdded extends DomainEvent {
  eventType: 'OrderItemAdded';
  payload: {
    productId: string;
    quantity: number;
    price: number;
  };
}

interface OrderShipped extends DomainEvent {
  eventType: 'OrderShipped';
  payload: {
    trackingNumber: string;
    carrier: string;
    shippedAt: number;
  };
}

type OrderEvent = OrderCreated | OrderItemAdded | OrderShipped;
```

### Event Store Implementation

```typescript
// event-store.ts
import { createDoSQL } from '@dotdo/dosql';
import { createCDC, type ChangeEvent } from '@dotdo/dosql/cdc';

export class EventStore {
  constructor(
    private db: ReturnType<typeof createDoSQL>,
    private cdc: ReturnType<typeof createCDC>
  ) {}

  async initialize() {
    await this.db.exec(`
      CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY,
        aggregate_id TEXT NOT NULL,
        aggregate_type TEXT NOT NULL,
        event_type TEXT NOT NULL,
        version INTEGER NOT NULL,
        timestamp INTEGER NOT NULL,
        payload TEXT NOT NULL,
        metadata TEXT,
        UNIQUE(aggregate_id, version)
      )
    `);

    await this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_events_aggregate
      ON events(aggregate_id, version)
    `);
  }

  async append(event: DomainEvent): Promise<void> {
    // Optimistic concurrency check
    const existing = await this.db.query<{ version: number }>(
      'SELECT MAX(version) as version FROM events WHERE aggregate_id = ?',
      [event.aggregateId]
    );

    const currentVersion = existing.rows[0]?.version ?? 0;

    if (event.version !== currentVersion + 1) {
      throw new Error(
        `Concurrency conflict: expected version ${currentVersion + 1}, got ${event.version}`
      );
    }

    await this.db.exec(
      `INSERT INTO events (event_id, aggregate_id, aggregate_type, event_type, version, timestamp, payload, metadata)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        event.eventId,
        event.aggregateId,
        event.aggregateType,
        event.eventType,
        event.version,
        event.timestamp,
        JSON.stringify(event.payload),
        JSON.stringify(event.metadata),
      ]
    );
  }

  async getEvents(aggregateId: string, fromVersion?: number): Promise<DomainEvent[]> {
    const query = fromVersion
      ? 'SELECT * FROM events WHERE aggregate_id = ? AND version > ? ORDER BY version'
      : 'SELECT * FROM events WHERE aggregate_id = ? ORDER BY version';

    const params = fromVersion ? [aggregateId, fromVersion] : [aggregateId];
    const result = await this.db.query<{
      event_id: string;
      aggregate_id: string;
      aggregate_type: string;
      event_type: string;
      version: number;
      timestamp: number;
      payload: string;
      metadata: string;
    }>(query, params);

    return result.rows.map(row => ({
      eventId: row.event_id,
      aggregateId: row.aggregate_id,
      aggregateType: row.aggregate_type,
      eventType: row.event_type,
      version: row.version,
      timestamp: row.timestamp,
      payload: JSON.parse(row.payload),
      metadata: JSON.parse(row.metadata || '{}'),
    }));
  }

  subscribeToEvents(
    fromLSN: bigint = 0n,
    filter?: { aggregateTypes?: string[]; eventTypes?: string[] }
  ) {
    const subscription = this.cdc.subscribe({
      filter: {
        tables: ['events'],
        operations: ['INSERT'],
      },
    });

    return {
      async *[Symbol.asyncIterator]() {
        for await (const event of subscription.subscribeChanges<DomainEvent>(
          fromLSN,
          { tables: ['events'], operations: ['INSERT'] },
          (data) => {
            const row = JSON.parse(new TextDecoder().decode(data));
            return {
              eventId: row.event_id,
              aggregateId: row.aggregate_id,
              aggregateType: row.aggregate_type,
              eventType: row.event_type,
              version: row.version,
              timestamp: row.timestamp,
              payload: JSON.parse(row.payload),
              metadata: JSON.parse(row.metadata || '{}'),
            };
          }
        )) {
          if (event.type !== 'insert' || !event.data) continue;

          const domainEvent = event.data;

          // Apply filters
          if (filter?.aggregateTypes && !filter.aggregateTypes.includes(domainEvent.aggregateType)) {
            continue;
          }
          if (filter?.eventTypes && !filter.eventTypes.includes(domainEvent.eventType)) {
            continue;
          }

          yield { event: domainEvent, lsn: event.lsn };
        }
      },
    };
  }
}
```

### Order Aggregate

```typescript
// order-aggregate.ts
interface OrderState {
  id: string;
  customerId: string;
  items: Array<{ productId: string; quantity: number; price: number }>;
  status: 'pending' | 'confirmed' | 'shipped' | 'delivered' | 'cancelled';
  shippingAddress: string;
  trackingNumber?: string;
  version: number;
}

export class OrderAggregate {
  private state: OrderState | null = null;
  private uncommittedEvents: OrderEvent[] = [];

  static create(id: string, customerId: string, items: Array<{
    productId: string;
    quantity: number;
    price: number;
  }>, shippingAddress: string): OrderAggregate {
    const aggregate = new OrderAggregate();

    aggregate.apply({
      eventId: crypto.randomUUID(),
      aggregateId: id,
      aggregateType: 'Order',
      eventType: 'OrderCreated',
      version: 1,
      timestamp: Date.now(),
      payload: { customerId, items, shippingAddress },
      metadata: {},
    } as OrderCreated);

    return aggregate;
  }

  static fromEvents(events: OrderEvent[]): OrderAggregate {
    const aggregate = new OrderAggregate();
    for (const event of events) {
      aggregate.applyEvent(event);
    }
    return aggregate;
  }

  addItem(productId: string, quantity: number, price: number) {
    if (!this.state) throw new Error('Order not initialized');
    if (this.state.status !== 'pending') {
      throw new Error('Cannot add items to non-pending order');
    }

    this.apply({
      eventId: crypto.randomUUID(),
      aggregateId: this.state.id,
      aggregateType: 'Order',
      eventType: 'OrderItemAdded',
      version: this.state.version + 1,
      timestamp: Date.now(),
      payload: { productId, quantity, price },
      metadata: {},
    } as OrderItemAdded);
  }

  ship(trackingNumber: string, carrier: string) {
    if (!this.state) throw new Error('Order not initialized');
    if (this.state.status !== 'confirmed') {
      throw new Error('Can only ship confirmed orders');
    }

    this.apply({
      eventId: crypto.randomUUID(),
      aggregateId: this.state.id,
      aggregateType: 'Order',
      eventType: 'OrderShipped',
      version: this.state.version + 1,
      timestamp: Date.now(),
      payload: { trackingNumber, carrier, shippedAt: Date.now() },
      metadata: {},
    } as OrderShipped);
  }

  private apply(event: OrderEvent) {
    this.applyEvent(event);
    this.uncommittedEvents.push(event);
  }

  private applyEvent(event: OrderEvent) {
    switch (event.eventType) {
      case 'OrderCreated':
        this.state = {
          id: event.aggregateId,
          customerId: event.payload.customerId,
          items: event.payload.items,
          status: 'pending',
          shippingAddress: event.payload.shippingAddress,
          version: event.version,
        };
        break;

      case 'OrderItemAdded':
        if (this.state) {
          this.state.items.push(event.payload);
          this.state.version = event.version;
        }
        break;

      case 'OrderShipped':
        if (this.state) {
          this.state.status = 'shipped';
          this.state.trackingNumber = event.payload.trackingNumber;
          this.state.version = event.version;
        }
        break;
    }
  }

  getUncommittedEvents(): OrderEvent[] {
    return [...this.uncommittedEvents];
  }

  markEventsAsCommitted() {
    this.uncommittedEvents = [];
  }

  getState(): OrderState | null {
    return this.state ? { ...this.state } : null;
  }
}
```

### Projection Builder

```typescript
// projections.ts
export class OrderProjection {
  constructor(
    private db: ReturnType<typeof createDoSQL>,
    private eventStore: EventStore
  ) {}

  async initialize() {
    // Create read model tables
    await this.db.exec(`
      CREATE TABLE IF NOT EXISTS order_summary (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT NOT NULL,
        total_amount REAL NOT NULL,
        item_count INTEGER NOT NULL,
        status TEXT NOT NULL,
        created_at INTEGER NOT NULL,
        updated_at INTEGER NOT NULL
      )
    `);

    await this.db.exec(`
      CREATE TABLE IF NOT EXISTS customer_orders (
        customer_id TEXT NOT NULL,
        order_id TEXT NOT NULL,
        total_amount REAL NOT NULL,
        status TEXT NOT NULL,
        PRIMARY KEY (customer_id, order_id)
      )
    `);
  }

  async rebuild(fromLSN: bigint = 0n) {
    for await (const { event, lsn } of this.eventStore.subscribeToEvents(fromLSN, {
      aggregateTypes: ['Order'],
    })) {
      await this.handleEvent(event);
    }
  }

  private async handleEvent(event: DomainEvent) {
    switch (event.eventType) {
      case 'OrderCreated':
        await this.handleOrderCreated(event as OrderCreated);
        break;
      case 'OrderItemAdded':
        await this.handleOrderItemAdded(event as OrderItemAdded);
        break;
      case 'OrderShipped':
        await this.handleOrderShipped(event as OrderShipped);
        break;
    }
  }

  private async handleOrderCreated(event: OrderCreated) {
    const totalAmount = event.payload.items.reduce(
      (sum, item) => sum + item.price * item.quantity,
      0
    );

    await this.db.exec(
      `INSERT INTO order_summary (order_id, customer_id, total_amount, item_count, status, created_at, updated_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [
        event.aggregateId,
        event.payload.customerId,
        totalAmount,
        event.payload.items.length,
        'pending',
        event.timestamp,
        event.timestamp,
      ]
    );

    await this.db.exec(
      `INSERT INTO customer_orders (customer_id, order_id, total_amount, status)
       VALUES (?, ?, ?, ?)`,
      [event.payload.customerId, event.aggregateId, totalAmount, 'pending']
    );
  }

  private async handleOrderItemAdded(event: OrderItemAdded) {
    const { productId, quantity, price } = event.payload;
    const itemTotal = price * quantity;

    await this.db.exec(
      `UPDATE order_summary
       SET total_amount = total_amount + ?,
           item_count = item_count + 1,
           updated_at = ?
       WHERE order_id = ?`,
      [itemTotal, event.timestamp, event.aggregateId]
    );
  }

  private async handleOrderShipped(event: OrderShipped) {
    await this.db.exec(
      `UPDATE order_summary SET status = ?, updated_at = ? WHERE order_id = ?`,
      ['shipped', event.timestamp, event.aggregateId]
    );

    await this.db.exec(
      `UPDATE customer_orders SET status = ? WHERE order_id = ?`,
      ['shipped', event.aggregateId]
    );
  }
}
```

---

## Example: Cache Invalidation

Implement automatic cache invalidation using CDC to keep caches synchronized with the database.

### Cache Invalidation Architecture

```
+----------+     +-------+     +------------+     +---------+
|  DoSQL   | --> |  CDC  | --> | Invalidator| --> |  Cache  |
| Database |     | Stream|     |  Service   |     |  (KV)   |
+----------+     +-------+     +------------+     +---------+
```

### Cache Manager with CDC

```typescript
// cache-manager.ts
import { createCDC, type ChangeEvent } from '@dotdo/dosql/cdc';

interface CacheConfig {
  /** KV namespace for caching */
  kv: KVNamespace;
  /** Default TTL in seconds */
  defaultTTL: number;
  /** Table-specific cache configurations */
  tables: Record<string, TableCacheConfig>;
}

interface TableCacheConfig {
  /** Key pattern: $column references column values */
  keyPattern: string;
  /** TTL in seconds */
  ttl?: number;
  /** Related cache keys to invalidate */
  invalidatePatterns?: string[];
  /** Whether to update cache on change instead of invalidate */
  updateOnChange?: boolean;
}

export class CacheManager {
  private config: CacheConfig;
  private cdc: ReturnType<typeof createCDC>;

  constructor(cdc: ReturnType<typeof createCDC>, config: CacheConfig) {
    this.cdc = cdc;
    this.config = config;
  }

  async startInvalidationConsumer() {
    const subscription = this.cdc.subscribe({
      filter: {
        tables: Object.keys(this.config.tables),
      },
      includeTransactionControl: false,
    });

    for await (const event of subscription.subscribeChanges(
      0n,
      { tables: Object.keys(this.config.tables) },
      (data) => JSON.parse(new TextDecoder().decode(data))
    )) {
      if (event.type === 'insert' || event.type === 'update' || event.type === 'delete') {
        await this.handleChange(event);
      }
    }
  }

  private async handleChange(event: ChangeEvent) {
    const tableConfig = this.config.tables[event.table];
    if (!tableConfig) return;

    const data = event.data ?? event.oldData;
    if (!data) return;

    // Generate cache key(s) to invalidate
    const keysToInvalidate = this.generateCacheKeys(event, tableConfig);

    // Invalidate or update cache
    for (const key of keysToInvalidate) {
      if (tableConfig.updateOnChange && event.type !== 'delete' && event.data) {
        // Update cache with new value
        await this.config.kv.put(
          key,
          JSON.stringify(event.data),
          { expirationTtl: tableConfig.ttl ?? this.config.defaultTTL }
        );
        console.log(`Cache updated: ${key}`);
      } else {
        // Invalidate cache
        await this.config.kv.delete(key);
        console.log(`Cache invalidated: ${key}`);
      }
    }
  }

  private generateCacheKeys(event: ChangeEvent, config: TableCacheConfig): string[] {
    const keys: string[] = [];
    const data = event.data ?? event.oldData;
    if (!data || typeof data !== 'object') return keys;

    // Generate primary cache key
    const primaryKey = this.interpolatePattern(config.keyPattern, data as Record<string, unknown>);
    keys.push(primaryKey);

    // Generate related invalidation keys
    if (config.invalidatePatterns) {
      for (const pattern of config.invalidatePatterns) {
        const relatedKey = this.interpolatePattern(pattern, data as Record<string, unknown>);
        keys.push(relatedKey);
      }
    }

    // For updates, also invalidate old data keys if values changed
    if (event.type === 'update' && event.oldData && typeof event.oldData === 'object') {
      const oldPrimaryKey = this.interpolatePattern(
        config.keyPattern,
        event.oldData as Record<string, unknown>
      );
      if (oldPrimaryKey !== primaryKey) {
        keys.push(oldPrimaryKey);
      }
    }

    return keys;
  }

  private interpolatePattern(pattern: string, data: Record<string, unknown>): string {
    return pattern.replace(/\$(\w+)/g, (_, column) => {
      const value = data[column];
      return value !== undefined ? String(value) : '';
    });
  }

  // Cache read-through helper
  async get<T>(key: string, fetcher: () => Promise<T>, ttl?: number): Promise<T> {
    const cached = await this.config.kv.get(key);
    if (cached) {
      return JSON.parse(cached) as T;
    }

    const value = await fetcher();
    await this.config.kv.put(
      key,
      JSON.stringify(value),
      { expirationTtl: ttl ?? this.config.defaultTTL }
    );
    return value;
  }
}
```

### Usage Example

```typescript
// worker.ts
import { CacheManager } from './cache-manager';
import { createCDC } from '@dotdo/dosql/cdc';

const cacheManager = new CacheManager(cdc, {
  kv: env.CACHE_KV,
  defaultTTL: 3600, // 1 hour
  tables: {
    users: {
      keyPattern: 'user:$id',
      invalidatePatterns: [
        'user:email:$email',
        'users:list:*',
      ],
      updateOnChange: true,
    },
    products: {
      keyPattern: 'product:$id',
      ttl: 300, // 5 minutes for products
      invalidatePatterns: [
        'products:category:$category_id',
        'products:search:*',
      ],
    },
    orders: {
      keyPattern: 'order:$id',
      invalidatePatterns: [
        'user:$user_id:orders',
        'orders:recent',
      ],
    },
  },
});

// Start the invalidation consumer
ctx.waitUntil(cacheManager.startInvalidationConsumer());

// Use cache with automatic invalidation
async function getUser(id: string): Promise<User> {
  return cacheManager.get(
    `user:${id}`,
    async () => {
      const result = await db.query<User>('SELECT * FROM users WHERE id = ?', [id]);
      return result.rows[0];
    }
  );
}
```

### Advanced: Tag-Based Invalidation

```typescript
// tag-cache.ts
interface TaggedCacheEntry {
  value: unknown;
  tags: string[];
  createdAt: number;
}

export class TaggedCacheManager {
  constructor(
    private kv: KVNamespace,
    private cdc: ReturnType<typeof createCDC>
  ) {}

  async set(key: string, value: unknown, tags: string[], ttl: number = 3600) {
    const entry: TaggedCacheEntry = {
      value,
      tags,
      createdAt: Date.now(),
    };

    // Store the entry
    await this.kv.put(key, JSON.stringify(entry), { expirationTtl: ttl });

    // Add key to each tag's set
    for (const tag of tags) {
      const tagKey = `tag:${tag}`;
      const existingKeys = await this.getTagKeys(tag);
      existingKeys.add(key);
      await this.kv.put(tagKey, JSON.stringify([...existingKeys]));
    }
  }

  async get<T>(key: string): Promise<T | null> {
    const raw = await this.kv.get(key);
    if (!raw) return null;

    const entry = JSON.parse(raw) as TaggedCacheEntry;
    return entry.value as T;
  }

  private async getTagKeys(tag: string): Promise<Set<string>> {
    const raw = await this.kv.get(`tag:${tag}`);
    if (!raw) return new Set();
    return new Set(JSON.parse(raw) as string[]);
  }

  async invalidateByTag(tag: string) {
    const keys = await this.getTagKeys(tag);

    // Delete all entries with this tag
    await Promise.all([...keys].map(key => this.kv.delete(key)));

    // Clear the tag set
    await this.kv.delete(`tag:${tag}`);

    console.log(`Invalidated ${keys.size} entries for tag: ${tag}`);
  }

  async startCDCInvalidation(tagMapping: Record<string, (event: ChangeEvent) => string[]>) {
    const subscription = this.cdc.subscribe({
      filter: { tables: Object.keys(tagMapping) },
    });

    for await (const event of subscription.subscribeChanges(0n)) {
      if (event.type === 'insert' || event.type === 'update' || event.type === 'delete') {
        const tagFn = tagMapping[event.table];
        if (tagFn) {
          const tags = tagFn(event);
          for (const tag of tags) {
            await this.invalidateByTag(tag);
          }
        }
      }
    }
  }
}

// Usage
const tagCache = new TaggedCacheManager(env.CACHE_KV, cdc);

// Cache with tags
await tagCache.set(
  'product:123',
  productData,
  ['products', 'category:electronics', 'brand:acme'],
  3600
);

// CDC-based invalidation
ctx.waitUntil(tagCache.startCDCInvalidation({
  products: (event) => {
    const data = event.data ?? event.oldData;
    if (!data) return ['products'];
    return [
      'products',
      `category:${data.category_id}`,
      `brand:${data.brand_id}`,
    ];
  },
  categories: (event) => {
    const data = event.data ?? event.oldData;
    return data ? [`category:${data.id}`] : [];
  },
}));
```

---

## Best Practices

### Performance Optimization

1. **Use filters aggressively** - Only subscribe to tables and operations you need
2. **Process in batches** - Use `subscribeBatched` for high-throughput scenarios
3. **Implement backpressure** - Monitor buffer sizes and slow down when overwhelmed
4. **Use replication slots** - For durable consumers that need to resume after restarts

### Reliability

1. **Save checkpoints frequently** - Update your position after processing each batch
2. **Handle LSN_NOT_FOUND** - WAL compaction may remove old entries
3. **Implement dead letter queues** - Don't lose events that fail processing
4. **Monitor lag** - Track the difference between current WAL position and consumer position

### Security

1. **Filter sensitive data** - Don't stream sensitive columns to unauthorized consumers
2. **Use TLS** - Encrypt CDC streams in transit
3. **Authenticate consumers** - Verify consumer identity before streaming

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Events missing | Consumer started too late | Use replication slots to track position |
| LSN_NOT_FOUND | WAL compacted | Reset to earliest available or use slots |
| High memory usage | Buffer overflow | Reduce batch size, implement backpressure |
| Duplicate events | Consumer crashed mid-batch | Use exactly-once delivery with checkpoints |
| Slow processing | Single-threaded consumer | Batch events, optimize handlers |

### Debugging CDC Streams

```typescript
// Enable detailed logging
const subscription = cdc.subscribe({
  fromLSN: 0n,
});

for await (const event of subscription.subscribeChanges(0n)) {
  console.log(JSON.stringify({
    lsn: event.lsn.toString(),
    type: event.type,
    table: 'table' in event ? event.table : 'transaction',
    txnId: event.txnId,
    timestamp: event.timestamp.toISOString(),
  }));
}

// Check subscription status
setInterval(() => {
  const status = subscription.getStatus();
  console.log('CDC Status:', {
    active: status.active,
    currentLSN: status.currentLSN.toString(),
    processed: status.entriesProcessed,
    buffered: status.bufferedEntries,
    lag: Date.now() - (status.lastEntryAt?.getTime() ?? 0),
  });
}, 5000);
```

---

## Related Documentation

- [DoSQL Architecture](/docs/ARCHITECTURE_REVIEW.md)
- [WAL Configuration](/docs/OPERATIONS.md#wal-compaction)
- [DoLake Integration](/packages/dolake/docs/ARCHITECTURE.md)
- [Error Codes Reference](/docs/ERROR_CODES.md)
- [Performance Tuning](/docs/PERFORMANCE_TUNING.md)
