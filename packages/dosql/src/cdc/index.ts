/**
 * CDC (Change Data Capture) Module for DoSQL
 *
 * Provides real-time streaming of database changes:
 * - Subscribe to changes from any LSN
 * - Filter by table, operation, or custom predicate
 * - Replication slots for persistent position tracking
 * - Both iterator and callback-based interfaces
 *
 * @example
 * ```typescript
 * import { createCDC } from 'dosql/cdc';
 *
 * // Create CDC from backend
 * const cdc = createCDC(backend);
 *
 * // Subscribe to all changes
 * const subscription = cdc.subscribe({ fromLSN: 0n });
 *
 * for await (const entry of subscription.subscribe(0n)) {
 *   console.log('Change:', entry.op, entry.table);
 * }
 *
 * // Or subscribe with typed events
 * for await (const event of subscription.subscribeChanges(0n, {
 *   tables: ['users'],
 *   operations: ['INSERT', 'UPDATE']
 * }, JSON.parse)) {
 *   if (event.type === 'insert') {
 *     console.log('New user:', event.data);
 *   }
 * }
 *
 * // Using replication slots for durability
 * await cdc.slots.createSlot('my-consumer', 0n);
 * const sub = await cdc.slots.subscribeFromSlot('my-consumer');
 * // ... process events ...
 * await cdc.slots.updateSlot('my-consumer', lastProcessedLSN);
 * ```
 *
 * @packageDocumentation
 */

// Types
export {
  // Filter types
  type CDCFilter,
  type CDCSubscriptionOptions,

  // Event types
  type ChangeEvent,
  type TransactionEvent,
  type CDCEvent,

  // Subscription types
  type CDCSubscription,
  type SubscriptionStatus,

  // Stream types
  type CDCHandler,
  type CDCStream,
  type CDCStreamOptions,

  // Replication slot types
  type ReplicationSlot,
  type ReplicationSlotManager,

  // Lakehouse streaming types
  type LakehouseStreamConfig,
  type LakehouseStreamStatus,
  type LakehouseAck,
  type LakehouseNack,
  type CDCBatch,
  type BackpressureSignal,
  type DeliveryCheckpoint,
  type RetryConfig,
  type SchemaChangeEvent,
  DEFAULT_LAKEHOUSE_CONFIG,

  // Error types
  CDCError,
  CDCErrorCode,
} from './types.js';

// Implementation
export {
  // Core subscription
  createCDCSubscription,

  // Callback-based stream
  createCDCStream,

  // Replication slots
  createReplicationSlotManager,

  // Convenience functions
  createCDC,
  subscribeTable,
  subscribeBatched,

  // Lakehouse streaming
  createLakehouseStreamer,
  type LakehouseStreamer,

  // HLC CDC types
  type HLCCDCEvent,
  type CDCSubscriptionWithHLC,
} from './stream.js';

// Capture module
export {
  createWALCapturer,
  walEntryToChangeEvent,
  batchToChangeEvents,
  type WALCapturer,
  type CaptureOptions,
  type CaptureBatch,
  type CaptureState,
  type CaptureResult,
} from './capture.js';
