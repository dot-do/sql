/**
 * CDC Stream Implementation for DoSQL
 *
 * Provides real-time change data capture streaming from the WAL:
 * - Subscribe to changes from any LSN
 * - Filter by table, operation, or custom predicate
 * - Async iterator interface
 * - Replication slots for persistent position tracking
 */

import type { FSXBackend } from '../fsx/types.js';
import type {
  WALEntry,
  WALOperation,
  WALReader,
  WALConfig,
  LSN,
} from '../wal/types.js';
import { createWALReader, tailWAL } from '../wal/reader.js';
import { DEFAULT_WAL_CONFIG } from '../wal/types.js';
import { createLSN, incrementLSN } from '../engine/types.js';
import {
  type CDCFilter,
  type CDCSubscription,
  type CDCSubscriptionOptions,
  type ChangeEvent,
  type TransactionEvent,
  type CDCEvent,
  type SubscriptionStatus,
  type CDCHandler,
  type CDCStream,
  type CDCStreamOptions,
  type ReplicationSlot,
  type ReplicationSlotManager,
  CDCError,
  CDCErrorCode,
} from './types.js';

// =============================================================================
// Filter Helpers
// =============================================================================

/**
 * Check if a WAL entry matches a CDC filter
 */
function matchesFilter(entry: WALEntry, filter?: CDCFilter): boolean {
  if (!filter) return true;

  // Table filter
  if (filter.tables && filter.tables.length > 0) {
    if (!filter.tables.includes(entry.table)) {
      return false;
    }
  }

  // Operation filter
  if (filter.operations && filter.operations.length > 0) {
    if (!filter.operations.includes(entry.op)) {
      return false;
    }
  }

  // Transaction ID filter
  if (filter.txnIds && filter.txnIds.length > 0) {
    if (!filter.txnIds.includes(entry.txnId)) {
      return false;
    }
  }

  // Custom predicate
  if (filter.predicate && !filter.predicate(entry)) {
    return false;
  }

  return true;
}

/**
 * Convert WAL operation to change event type
 */
function opToChangeType(
  op: WALOperation
): 'insert' | 'update' | 'delete' | null {
  switch (op) {
    case 'INSERT':
      return 'insert';
    case 'UPDATE':
      return 'update';
    case 'DELETE':
      return 'delete';
    default:
      return null;
  }
}

/**
 * Convert WAL operation to transaction event type
 */
function opToTxnType(op: WALOperation): 'begin' | 'commit' | 'rollback' | null {
  switch (op) {
    case 'BEGIN':
      return 'begin';
    case 'COMMIT':
      return 'commit';
    case 'ROLLBACK':
      return 'rollback';
    default:
      return null;
  }
}

// =============================================================================
// CDC Subscription Implementation
// =============================================================================

/**
 * Create a CDC subscription
 */
export function createCDCSubscription(
  reader: WALReader,
  options: CDCSubscriptionOptions = {}
): CDCSubscription {
  const pollInterval = options.pollInterval ?? 100;
  const batchSize = options.batchSize ?? 100;
  const includeTransactionControl = options.includeTransactionControl ?? false;

  let active = false;
  let currentLSN: LSN = options.fromLSN ?? createLSN(0n);
  let entriesProcessed = 0;
  let bufferedEntries = 0;
  let startedAt: Date | null = null;
  let lastEntryAt: Date | undefined;
  let stopRequested = false;

  const subscription: CDCSubscription = {
    async *subscribe(
      fromLSN: LSN,
      filter?: CDCFilter
    ): AsyncIterableIterator<WALEntry> {
      if (active) {
        throw new CDCError(
          CDCErrorCode.SUBSCRIPTION_FAILED,
          'Subscription already active'
        );
      }

      active = true;
      startedAt = new Date();
      currentLSN = fromLSN;
      stopRequested = false;

      try {
        // Use tailing iterator for live updates
        for await (const entry of tailWAL(reader, {
          fromLSN: incrementLSN(currentLSN), // Start after the given LSN
          pollInterval,
        })) {
          if (stopRequested) break;

          // Apply filter
          if (!matchesFilter(entry, filter)) {
            continue;
          }

          // Skip transaction control entries if not requested
          if (
            !includeTransactionControl &&
            (entry.op === 'BEGIN' ||
              entry.op === 'COMMIT' ||
              entry.op === 'ROLLBACK')
          ) {
            continue;
          }

          currentLSN = entry.lsn;
          entriesProcessed++;
          lastEntryAt = new Date();

          yield entry;
        }
      } finally {
        active = false;
      }
    },

    async *subscribeChanges<T = unknown>(
      fromLSN: bigint,
      filter?: CDCFilter,
      decoder?: (data: Uint8Array) => T
    ): AsyncIterableIterator<CDCEvent<T>> {
      const decodeValue = decoder ?? ((data: Uint8Array) => data as unknown as T);

      // Extend filter to include transaction control for boundary events
      const extendedFilter: CDCFilter = {
        ...filter,
      };

      for await (const entry of subscription.subscribe(fromLSN, extendedFilter)) {
        // Check if this is a transaction control entry
        const txnType = opToTxnType(entry.op);
        if (txnType) {
          if (includeTransactionControl) {
            const txnEvent: TransactionEvent = {
              type: txnType,
              txnId: entry.txnId,
              timestamp: new Date(entry.timestamp),
              lsn: entry.lsn,
            };
            yield txnEvent;
          }
          continue;
        }

        // This is a data change event
        const changeType = opToChangeType(entry.op);
        if (!changeType) continue;

        try {
          const changeEvent: ChangeEvent<T> = {
            id: `${entry.lsn}`,
            type: changeType,
            table: entry.table,
            txnId: entry.txnId,
            timestamp: new Date(entry.timestamp),
            lsn: entry.lsn,
            key: entry.key,
          };

          // Decode data values
          if (entry.after) {
            changeEvent.data = decodeValue(entry.after);
          }
          if (entry.before) {
            changeEvent.oldData = decodeValue(entry.before);
          }

          yield changeEvent;
        } catch (error) {
          throw new CDCError(
            CDCErrorCode.DECODE_ERROR,
            `Failed to decode entry at LSN ${entry.lsn}`,
            entry.lsn,
            error instanceof Error ? error : undefined
          );
        }
      }
    },

    getStatus(): SubscriptionStatus {
      return {
        active,
        currentLSN,
        entriesProcessed,
        bufferedEntries,
        startedAt: startedAt ?? new Date(),
        lastEntryAt,
      };
    },

    stop(): void {
      stopRequested = true;
    },

    isActive(): boolean {
      return active;
    },
  };

  return subscription;
}

// =============================================================================
// CDC Stream (Callback-based) Implementation
// =============================================================================

/**
 * Create a callback-based CDC stream
 */
export function createCDCStream(
  reader: WALReader,
  options: CDCStreamOptions
): CDCStream {
  const { handler, decoder, autoAck = true } = options;

  let subscription: CDCSubscription | null = null;
  let iteratorPromise: Promise<void> | null = null;
  let paused = false;
  let acknowledgedLSN = options.fromLSN ?? 0n;

  async function runStream(): Promise<void> {
    if (!subscription) return;

    try {
      for await (const event of subscription.subscribeChanges(
        acknowledgedLSN,
        options.filter,
        decoder
      )) {
        // Wait while paused
        while (paused) {
          await new Promise((resolve) => setTimeout(resolve, 100));
        }

        if (!subscription.isActive()) break;

        // Dispatch to appropriate handler
        if (
          event.type === 'insert' ||
          event.type === 'update' ||
          event.type === 'delete'
        ) {
          if (handler.onChange) {
            await handler.onChange(event as ChangeEvent);
          }
        } else {
          if (handler.onTransaction) {
            await handler.onTransaction(event as TransactionEvent);
          }
        }

        // Auto-acknowledge
        if (autoAck) {
          acknowledgedLSN = event.lsn;
        }
      }
    } catch (error) {
      if (handler.onError) {
        handler.onError(
          error instanceof Error ? error : new Error(String(error))
        );
      }
    } finally {
      if (handler.onEnd) {
        handler.onEnd();
      }
    }
  }

  const stream: CDCStream = {
    start(): void {
      if (subscription) return;

      subscription = createCDCSubscription(reader, options);
      iteratorPromise = runStream();
    },

    stop(): void {
      if (subscription) {
        subscription.stop();
        subscription = null;
      }
    },

    pause(): void {
      paused = true;
    },

    resume(): void {
      paused = false;
    },

    getStatus(): SubscriptionStatus {
      if (subscription) {
        return subscription.getStatus();
      }
      return {
        active: false,
        currentLSN: acknowledgedLSN,
        entriesProcessed: 0,
        bufferedEntries: 0,
        startedAt: new Date(),
      };
    },

    acknowledge(lsn: bigint): void {
      acknowledgedLSN = lsn;
    },
  };

  return stream;
}

// =============================================================================
// Replication Slot Manager Implementation
// =============================================================================

/**
 * Encode replication slot to JSON
 */
function encodeSlot(slot: ReplicationSlot): Uint8Array {
  const obj = {
    name: slot.name,
    acknowledgedLSN: slot.acknowledgedLSN.toString(),
    createdAt: slot.createdAt.toISOString(),
    lastUsedAt: slot.lastUsedAt.toISOString(),
    filter: slot.filter,
    metadata: slot.metadata,
  };
  return new TextEncoder().encode(JSON.stringify(obj));
}

/**
 * Decode replication slot from JSON
 */
function decodeSlot(data: Uint8Array): ReplicationSlot {
  const obj = JSON.parse(new TextDecoder().decode(data));
  return {
    name: obj.name,
    acknowledgedLSN: BigInt(obj.acknowledgedLSN),
    createdAt: new Date(obj.createdAt),
    lastUsedAt: new Date(obj.lastUsedAt),
    filter: obj.filter,
    metadata: obj.metadata,
  };
}

/**
 * Create a replication slot manager
 */
export function createReplicationSlotManager(
  backend: FSXBackend,
  reader: WALReader,
  slotPrefix = '_cdc/slots/'
): ReplicationSlotManager {
  function slotPath(name: string): string {
    return `${slotPrefix}${name}.json`;
  }

  const manager: ReplicationSlotManager = {
    async createSlot(
      name: string,
      initialLSN = 0n,
      filter?: CDCFilter
    ): Promise<ReplicationSlot> {
      // Check if slot already exists
      const existing = await manager.getSlot(name);
      if (existing) {
        throw new CDCError(
          CDCErrorCode.SLOT_EXISTS,
          `Replication slot '${name}' already exists`
        );
      }

      const now = new Date();
      const slot: ReplicationSlot = {
        name,
        acknowledgedLSN: initialLSN,
        createdAt: now,
        lastUsedAt: now,
        filter,
      };

      await backend.write(slotPath(name), encodeSlot(slot));
      return slot;
    },

    async getSlot(name: string): Promise<ReplicationSlot | null> {
      const data = await backend.read(slotPath(name));
      if (!data) return null;

      try {
        return decodeSlot(data);
      } catch {
        return null;
      }
    },

    async updateSlot(name: string, acknowledgedLSN: bigint): Promise<void> {
      const slot = await manager.getSlot(name);
      if (!slot) {
        throw new CDCError(
          CDCErrorCode.SLOT_NOT_FOUND,
          `Replication slot '${name}' not found`
        );
      }

      slot.acknowledgedLSN = acknowledgedLSN;
      slot.lastUsedAt = new Date();

      await backend.write(slotPath(name), encodeSlot(slot));
    },

    async deleteSlot(name: string): Promise<void> {
      await backend.delete(slotPath(name));
    },

    async listSlots(): Promise<ReplicationSlot[]> {
      const paths = await backend.list(slotPrefix);
      const slots: ReplicationSlot[] = [];

      for (const path of paths) {
        if (!path.endsWith('.json')) continue;

        const data = await backend.read(path);
        if (data) {
          try {
            slots.push(decodeSlot(data));
          } catch {
            // Skip invalid slots
          }
        }
      }

      return slots;
    },

    async subscribeFromSlot(name: string): Promise<CDCSubscription> {
      const slot = await manager.getSlot(name);
      if (!slot) {
        throw new CDCError(
          CDCErrorCode.SLOT_NOT_FOUND,
          `Replication slot '${name}' not found`
        );
      }

      // Update last used time
      await manager.updateSlot(name, slot.acknowledgedLSN);

      return createCDCSubscription(reader, {
        fromLSN: slot.acknowledgedLSN,
        filter: slot.filter,
      });
    },
  };

  return manager;
}

// =============================================================================
// Convenience Functions
// =============================================================================

/**
 * Create a complete CDC setup from FSX backend
 */
export function createCDC(
  backend: FSXBackend,
  config: Partial<WALConfig> = {}
): {
  reader: WALReader;
  subscribe: (options?: CDCSubscriptionOptions) => CDCSubscription;
  slots: ReplicationSlotManager;
} {
  const reader = createWALReader(backend, config);
  const slots = createReplicationSlotManager(backend, reader);

  return {
    reader,
    subscribe: (options?: CDCSubscriptionOptions) =>
      createCDCSubscription(reader, options),
    slots,
  };
}

/**
 * Simple helper to subscribe to all changes on a table
 */
export async function* subscribeTable<T = unknown>(
  reader: WALReader,
  tableName: string,
  fromLSN: bigint = 0n,
  decoder?: (data: Uint8Array) => T
): AsyncIterableIterator<ChangeEvent<T>> {
  const subscription = createCDCSubscription(reader, {
    includeTransactionControl: false,
  });

  for await (const event of subscription.subscribeChanges(
    fromLSN,
    { tables: [tableName] },
    decoder
  )) {
    // Only yield change events (not transaction events)
    if (
      event.type === 'insert' ||
      event.type === 'update' ||
      event.type === 'delete'
    ) {
      yield event as ChangeEvent<T>;
    }
  }
}

/**
 * Subscribe to changes and process in batches
 */
export async function subscribeBatched<T = unknown>(
  reader: WALReader,
  options: {
    fromLSN?: bigint;
    filter?: CDCFilter;
    decoder?: (data: Uint8Array) => T;
    batchSize: number;
    batchTimeout: number;
    onBatch: (events: ChangeEvent<T>[]) => Promise<void>;
  }
): Promise<void> {
  const { batchSize, batchTimeout, onBatch, decoder } = options;

  const subscription = createCDCSubscription(reader);
  let batch: ChangeEvent<T>[] = [];
  let lastFlush = Date.now();

  async function flushBatch(): Promise<void> {
    if (batch.length > 0) {
      await onBatch(batch);
      batch = [];
      lastFlush = Date.now();
    }
  }

  for await (const event of subscription.subscribeChanges(
    options.fromLSN ?? 0n,
    options.filter,
    decoder
  )) {
    // Only process change events
    if (
      event.type !== 'insert' &&
      event.type !== 'update' &&
      event.type !== 'delete'
    ) {
      continue;
    }

    batch.push(event as ChangeEvent<T>);

    // Flush if batch is full or timeout reached
    if (
      batch.length >= batchSize ||
      Date.now() - lastFlush >= batchTimeout
    ) {
      await flushBatch();
    }
  }

  // Final flush
  await flushBatch();
}

// =============================================================================
// Lakehouse Streaming Implementation
// =============================================================================

import type {
  LakehouseStreamConfig,
  LakehouseStreamStatus,
  LakehouseAck,
  LakehouseNack,
  CDCBatch,
  BackpressureSignal,
  DeliveryCheckpoint,
  DEFAULT_LAKEHOUSE_CONFIG,
} from './types.js';

/**
 * Lakehouse streamer interface
 */
export interface LakehouseStreamer {
  /** Connect to lakehouse */
  connect(): Promise<void>;
  /** Disconnect from lakehouse */
  disconnect(): Promise<void>;
  /** Start streaming changes */
  start(fromLSN?: bigint): void;
  /** Stop streaming */
  stop(): void;
  /** Get current status */
  getStatus(): LakehouseStreamStatus;
  /** Get last checkpoint */
  getCheckpoint(): DeliveryCheckpoint | null;
  /** Force flush pending batches */
  flush(): Promise<void>;
  /** Handle backpressure signal */
  onBackpressure(handler: (signal: BackpressureSignal) => void): void;
}

/**
 * Create a lakehouse streamer for CDC to lakehouse transfer
 */
export function createLakehouseStreamer(
  reader: WALReader,
  backend: FSXBackend,
  config: Partial<LakehouseStreamConfig>
): LakehouseStreamer {
  const fullConfig: LakehouseStreamConfig = {
    lakehouseUrl: config.lakehouseUrl ?? '',
    sourceDoId: config.sourceDoId ?? `do_${Date.now().toString(36)}`,
    sourceShardName: config.sourceShardName,
    maxBatchSize: config.maxBatchSize ?? 1000,
    maxBatchAge: config.maxBatchAge ?? 5000,
    retry: config.retry ?? {
      maxAttempts: 3,
      initialDelayMs: 100,
      maxDelayMs: 10000,
      backoffMultiplier: 2,
    },
    heartbeatInterval: config.heartbeatInterval ?? 30000,
    exactlyOnce: config.exactlyOnce ?? true,
  };

  // State
  let status: LakehouseStreamStatus = {
    state: 'disconnected',
    lastAckLSN: 0n,
    lastSentLSN: 0n,
    pendingBatches: 0,
    totalBatchesSent: 0,
    totalEntriesSent: 0,
  };

  let checkpoint: DeliveryCheckpoint | null = null;
  let backpressureHandler: ((signal: BackpressureSignal) => void) | null = null;
  let sequenceNumber = 0;
  let pendingBatches: Map<string, CDCBatch> = new Map();
  let stopRequested = false;
  let subscription: CDCSubscription | null = null;
  let streamPromise: Promise<void> | null = null;
  let pendingEvents: CDCEvent[] = [];
  let lastBatchTime = Date.now();

  /**
   * Generate unique batch ID
   */
  function generateBatchId(): string {
    return `${fullConfig.sourceDoId.slice(0, 8)}_${sequenceNumber}_${Date.now().toString(36)}`;
  }

  /**
   * Calculate exponential backoff delay
   */
  function calculateBackoff(attempt: number): number {
    const delay = fullConfig.retry.initialDelayMs * Math.pow(fullConfig.retry.backoffMultiplier, attempt);
    return Math.min(delay, fullConfig.retry.maxDelayMs);
  }

  /**
   * Estimate batch size in bytes
   */
  function estimateBatchSize(events: CDCEvent[]): number {
    let size = 100; // Base overhead
    for (const event of events) {
      size += 50; // Event overhead
      if ('table' in event) {
        size += event.table.length;
      }
      size += event.txnId.length;
      // Rough estimate for data
      if ('data' in event && event.data) {
        size += JSON.stringify(event.data).length;
      }
      if ('oldData' in event && event.oldData) {
        size += JSON.stringify(event.oldData).length;
      }
    }
    return size;
  }

  /**
   * Create a batch from pending events
   */
  function createBatch(events: CDCEvent[]): CDCBatch {
    const firstLSN = events.length > 0 ? events[0].lsn : 0n;
    const lastLSN = events.length > 0 ? events[events.length - 1].lsn : 0n;

    const batch: CDCBatch = {
      batchId: generateBatchId(),
      sourceDoId: fullConfig.sourceDoId,
      sequenceNumber: sequenceNumber++,
      firstLSN,
      lastLSN,
      events: [...events],
      createdAt: Date.now(),
      sizeBytes: estimateBatchSize(events),
      isRetry: false,
      retryCount: 0,
    };

    return batch;
  }

  /**
   * Should flush pending events to a batch
   */
  function shouldFlush(): boolean {
    if (pendingEvents.length === 0) return false;
    if (pendingEvents.length >= fullConfig.maxBatchSize) return true;
    if (Date.now() - lastBatchTime >= fullConfig.maxBatchAge) return true;
    return false;
  }

  /**
   * Flush pending events to a batch and send
   */
  async function flushAndSend(): Promise<void> {
    if (pendingEvents.length === 0) return;

    const batch = createBatch(pendingEvents);
    pendingEvents = [];
    lastBatchTime = Date.now();

    await sendBatch(batch);
  }

  /**
   * Send a batch to lakehouse (stub - would use WebSocket in real impl)
   */
  async function sendBatch(batch: CDCBatch): Promise<void> {
    pendingBatches.set(batch.batchId, batch);
    status.pendingBatches = pendingBatches.size;
    status.lastSentLSN = batch.lastLSN;
    status.totalBatchesSent++;
    status.totalEntriesSent += batch.events.length;

    // In a real implementation, this would send via WebSocket to lakehouse
    // For now, simulate immediate acknowledgment
    await simulateAck(batch);
  }

  /**
   * Simulate acknowledgment (for testing without real lakehouse)
   */
  async function simulateAck(batch: CDCBatch): Promise<void> {
    // Remove from pending
    pendingBatches.delete(batch.batchId);
    status.pendingBatches = pendingBatches.size;
    status.lastAckLSN = batch.lastLSN;

    // Update checkpoint
    checkpoint = {
      sourceDoId: fullConfig.sourceDoId,
      committedLSN: batch.lastLSN,
      committedBatchId: batch.batchId,
      checkpointedAt: Date.now(),
      pendingBatchIds: Array.from(pendingBatches.keys()),
    };
  }

  /**
   * Run the streaming loop
   */
  async function runStream(fromLSN: bigint): Promise<void> {
    subscription = createCDCSubscription(reader, {
      fromLSN,
      includeTransactionControl: false,
    });

    try {
      for await (const event of subscription.subscribeChanges(fromLSN, {})) {
        if (stopRequested) break;

        // Skip transaction events
        if (event.type === 'begin' || event.type === 'commit' || event.type === 'rollback') {
          continue;
        }

        pendingEvents.push(event);

        // Check if we should flush
        if (shouldFlush()) {
          await flushAndSend();
        }
      }
    } finally {
      // Flush any remaining events
      await flushAndSend();
    }
  }

  const streamer: LakehouseStreamer = {
    async connect(): Promise<void> {
      status.state = 'connecting';
      // In real impl, establish WebSocket connection here
      status.state = 'connected';
      status.connectedSince = Date.now();
    },

    async disconnect(): Promise<void> {
      status.state = 'disconnected';
      status.connectedSince = undefined;
    },

    start(fromLSN?: bigint): void {
      if (status.state !== 'connected') {
        throw new Error('Not connected to lakehouse');
      }

      stopRequested = false;
      const startLSN = fromLSN ?? checkpoint?.committedLSN ?? 0n;
      streamPromise = runStream(startLSN);
    },

    stop(): void {
      stopRequested = true;
      if (subscription) {
        subscription.stop();
      }
    },

    getStatus(): LakehouseStreamStatus {
      return { ...status };
    },

    getCheckpoint(): DeliveryCheckpoint | null {
      return checkpoint ? { ...checkpoint } : null;
    },

    async flush(): Promise<void> {
      await flushAndSend();
    },

    onBackpressure(handler: (signal: BackpressureSignal) => void): void {
      backpressureHandler = handler;
    },
  };

  return streamer;
}
