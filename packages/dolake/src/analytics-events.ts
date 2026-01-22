/**
 * Analytics Events Ingestion Module
 *
 * Handles analytics events with P2 durability tier (R2 with VFS fallback).
 * Features:
 * - Client-side batching (100 events or 5s timeout)
 * - Schema inference from event payloads
 * - Date-based partitioning (year/month/day)
 * - Deduplication by eventId
 *
 * @see do-d1isn.8 - Analytics events ingestion
 */

import { generateUUID } from './types.js';
import { writeParquet } from './parquet.js';

// =============================================================================
// Types
// =============================================================================

/**
 * Analytics event structure
 */
export interface AnalyticsEvent {
  /** Unique event identifier for deduplication */
  eventId: string;
  /** Event type (e.g., 'page_view', 'click', 'purchase') */
  eventType: string;
  /** Unix timestamp in milliseconds */
  timestamp: number;
  /** Session identifier */
  sessionId: string;
  /** User identifier (optional) */
  userId?: string;
  /** Event payload with arbitrary data */
  payload: Record<string, unknown>;
}

/**
 * Analytics event batch for transmission
 */
export interface AnalyticsEventBatch {
  type: 'analytics_batch';
  batchId: string;
  events: AnalyticsEvent[];
  timestamp: number;
  sizeBytes: number;
  firstEventTime: number;
  lastEventTime: number;
}

/**
 * Durability tier configuration
 */
export interface AnalyticsDurabilityConfig {
  /** Primary storage: 'r2' | 'vfs' | 'sqlite' */
  primaryStorage: 'r2' | 'vfs' | 'sqlite';
  /** Fallback storage when primary fails */
  fallbackStorage: 'vfs' | 'sqlite' | null;
  /** Enable fallback on primary failure */
  fallbackEnabled: boolean;
  /** Number of retry attempts before fallback */
  retryAttempts: number;
  /** Retry delay in milliseconds */
  retryDelayMs: number;
  /** Enable deduplication */
  deduplicationEnabled?: boolean;
  /** Deduplication window in milliseconds */
  deduplicationWindowMs?: number;
  /** Simulate R2 failure for testing */
  simulateR2Failure?: boolean;
}

/**
 * Analytics schema field definition
 */
export interface AnalyticsSchemaField {
  id: number;
  name: string;
  type: 'string' | 'long' | 'double' | 'boolean' | 'timestamptz';
  required: boolean;
}

/**
 * Analytics schema
 */
export interface AnalyticsSchema {
  type: 'struct';
  'schema-id': number;
  fields: AnalyticsSchemaField[];
}

/**
 * Date partition
 */
export interface DatePartition {
  year: number;
  month: number;
  day: number;
  path: string;
}

/**
 * Persist result
 */
export interface PersistResult {
  success: boolean;
  storageTier: 'r2' | 'vfs' | 'sqlite';
  path: string;
  bytesWritten: number;
  usedFallback: boolean;
  retryCount: number;
  filesWritten?: number;
  partitions?: string[];
  error?: string;
}

/**
 * Recovery result
 */
export interface RecoveryResult {
  success: boolean;
  recoveredEvents: number;
  newStorageTier: 'r2' | 'vfs';
  error?: string;
}

/**
 * Batch handling result
 */
export interface BatchHandleResult {
  type: 'analytics_ack' | 'analytics_nack';
  batchId: string;
  success: boolean;
  eventsProcessed: number;
  duplicatesSkipped?: number;
  error?: string;
}

/**
 * Analytics metrics
 */
export interface AnalyticsMetrics {
  batchesReceived: number;
  eventsReceived: number;
  bytesReceived: number;
  r2Writes: number;
  vfsFallbacks: number;
  errors: number;
}

/**
 * Buffer configuration
 */
export interface BufferConfig {
  maxBatchSize?: number;
  flushTimeoutMs?: number;
}

// =============================================================================
// Constants
// =============================================================================

/**
 * Default analytics configuration
 */
export const DEFAULT_ANALYTICS_CONFIG: AnalyticsDurabilityConfig = {
  primaryStorage: 'r2',
  fallbackStorage: 'vfs',
  fallbackEnabled: true,
  retryAttempts: 3,
  retryDelayMs: 1000,
  deduplicationEnabled: true,
  deduplicationWindowMs: 300_000, // 5 minutes
};

/**
 * P2 durability tier configuration
 * R2 primary with VFS fallback
 */
export const P2_DURABILITY_CONFIG: AnalyticsDurabilityConfig = {
  primaryStorage: 'r2',
  fallbackStorage: 'vfs',
  fallbackEnabled: true,
  retryAttempts: 3,
  retryDelayMs: 1000,
  deduplicationEnabled: true,
  deduplicationWindowMs: 300_000,
};

// =============================================================================
// Analytics Event Buffer (Client-side batching)
// =============================================================================

/**
 * Client-side buffer for batching analytics events
 * Implements the 100 events or 5s timeout batching strategy
 */
export class AnalyticsEventBuffer {
  private events: AnalyticsEvent[] = [];
  private firstEventTime: number | null = null;
  private lastEventTime: number | null = null;
  private maxBatchSize: number;
  private flushTimeoutMs: number;

  constructor(config: BufferConfig = {}) {
    this.maxBatchSize = config.maxBatchSize ?? 100;
    this.flushTimeoutMs = config.flushTimeoutMs ?? 5000;
  }

  /**
   * Add an event to the buffer
   * @returns true if buffer should be flushed
   */
  add(event: AnalyticsEvent): boolean {
    if (this.events.length === 0) {
      this.firstEventTime = Date.now();
    }

    this.events.push(event);
    this.lastEventTime = Date.now();

    return this.events.length >= this.maxBatchSize;
  }

  /**
   * Get the number of events in the buffer
   */
  size(): number {
    return this.events.length;
  }

  /**
   * Check if buffer should flush due to time threshold
   */
  shouldFlushByTime(): boolean {
    if (this.events.length === 0 || this.firstEventTime === null) {
      return false;
    }
    return Date.now() - this.firstEventTime >= this.flushTimeoutMs;
  }

  /**
   * Flush the buffer and return the batch
   */
  flush(): AnalyticsEventBatch {
    const batch: AnalyticsEventBatch = {
      type: 'analytics_batch',
      batchId: generateUUID(),
      events: [...this.events],
      timestamp: Date.now(),
      sizeBytes: this.calculateSize(),
      firstEventTime: this.firstEventTime ?? Date.now(),
      lastEventTime: this.lastEventTime ?? Date.now(),
    };

    this.events = [];
    this.firstEventTime = null;
    this.lastEventTime = null;

    return batch;
  }

  /**
   * Serialize batch for WebSocket transmission
   */
  serializeBatch(batch: AnalyticsEventBatch): string {
    return JSON.stringify(batch);
  }

  /**
   * Calculate approximate size of events in bytes
   */
  private calculateSize(): number {
    return this.events.reduce((acc, event) => {
      return acc + JSON.stringify(event).length;
    }, 0);
  }
}

// =============================================================================
// Schema Inference
// =============================================================================

/**
 * Base analytics fields that are always present
 */
const BASE_ANALYTICS_FIELDS: AnalyticsSchemaField[] = [
  { id: 1, name: 'eventId', type: 'string', required: true },
  { id: 2, name: 'eventType', type: 'string', required: true },
  { id: 3, name: 'timestamp', type: 'long', required: true },
  { id: 4, name: 'sessionId', type: 'string', required: true },
  { id: 5, name: 'userId', type: 'string', required: false },
];

/**
 * Infer type from a value
 */
function inferType(value: unknown): 'string' | 'long' | 'double' | 'boolean' | 'timestamptz' {
  if (value === null || value === undefined) {
    return 'string';
  }

  if (typeof value === 'boolean') {
    return 'boolean';
  }

  if (typeof value === 'number') {
    return Number.isInteger(value) ? 'long' : 'double';
  }

  if (typeof value === 'string') {
    // Check if it's an ISO timestamp
    if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(value)) {
      return 'timestamptz';
    }
    return 'string';
  }

  // Arrays and objects are serialized as JSON strings
  return 'string';
}

/**
 * Flatten nested object into dot-notation fields
 */
function flattenObject(
  obj: Record<string, unknown>,
  prefix: string,
  result: Map<string, { type: string; hasNull: boolean }>
): void {
  for (const [key, value] of Object.entries(obj)) {
    const fullKey = prefix ? `${prefix}.${key}` : key;

    if (value === null || value === undefined) {
      const existing = result.get(fullKey);
      if (existing) {
        existing.hasNull = true;
      } else {
        result.set(fullKey, { type: 'string', hasNull: true });
      }
    } else if (typeof value === 'object' && !Array.isArray(value)) {
      flattenObject(value as Record<string, unknown>, fullKey, result);
    } else {
      const type = inferType(value);
      const existing = result.get(fullKey);
      if (existing) {
        // Keep existing type, update nullability
        if (type !== existing.type && existing.type === 'string') {
          existing.type = type;
        }
      } else {
        result.set(fullKey, { type, hasNull: false });
      }
    }
  }
}

/**
 * Infer analytics schema from events
 */
export function inferAnalyticsSchema(events: AnalyticsEvent[]): AnalyticsSchema {
  const fieldMap = new Map<string, { type: string; hasNull: boolean }>();

  // Process all events to collect field types
  for (const event of events) {
    if (event.payload && typeof event.payload === 'object') {
      flattenObject(event.payload, 'payload', fieldMap);
    }
  }

  // Build schema fields
  const payloadFields: AnalyticsSchemaField[] = [];
  let fieldId = BASE_ANALYTICS_FIELDS.length + 1;

  for (const [name, info] of fieldMap) {
    payloadFields.push({
      id: fieldId++,
      name,
      type: info.type as AnalyticsSchemaField['type'],
      required: !info.hasNull,
    });
  }

  return {
    type: 'struct',
    'schema-id': 0,
    fields: [...BASE_ANALYTICS_FIELDS, ...payloadFields],
  };
}

// =============================================================================
// Date Partitioning
// =============================================================================

/**
 * Create date partition from event timestamp
 */
export function createDatePartition(event: AnalyticsEvent): DatePartition {
  const date = new Date(event.timestamp);
  const year = date.getUTCFullYear();
  const month = date.getUTCMonth() + 1;
  const day = date.getUTCDate();

  const monthStr = String(month).padStart(2, '0');
  const dayStr = String(day).padStart(2, '0');

  return {
    year,
    month,
    day,
    path: `year=${year}/month=${monthStr}/day=${dayStr}`,
  };
}

// =============================================================================
// Analytics Event Handler
// =============================================================================

/**
 * Server-side handler for analytics events
 * Implements P2 durability tier with R2 primary and VFS fallback
 */
export class AnalyticsEventHandler {
  private config: AnalyticsDurabilityConfig;
  private metrics: AnalyticsMetrics = {
    batchesReceived: 0,
    eventsReceived: 0,
    bytesReceived: 0,
    r2Writes: 0,
    vfsFallbacks: 0,
    errors: 0,
  };
  private partitions: Set<string> = new Set();
  private deduplicationSet: Map<string, number> = new Map();

  constructor(config: AnalyticsDurabilityConfig) {
    this.config = config;
  }

  /**
   * Get durability tier name
   */
  getDurabilityTier(): string {
    return 'P2';
  }

  /**
   * Get primary storage type
   */
  getPrimaryStorage(): string {
    return this.config.primaryStorage;
  }

  /**
   * Get fallback storage type
   */
  getFallbackStorage(): string | null {
    return this.config.fallbackStorage;
  }

  /**
   * Get current metrics
   */
  getMetrics(): AnalyticsMetrics {
    return { ...this.metrics };
  }

  /**
   * Partition events by date
   */
  partitionEvents(events: AnalyticsEvent[]): Map<string, AnalyticsEvent[]> {
    const partitioned = new Map<string, AnalyticsEvent[]>();

    for (const event of events) {
      const partition = createDatePartition(event);
      const existing = partitioned.get(partition.path) ?? [];
      existing.push(event);
      partitioned.set(partition.path, existing);
    }

    return partitioned;
  }

  /**
   * Register a partition
   */
  registerPartition(partitionPath: string): void {
    this.partitions.add(partitionPath);
  }

  /**
   * Get partitions for a date range
   */
  getPartitionsForRange(start: Date, end: Date): string[] {
    const result: string[] = [];

    for (const partition of this.partitions) {
      // Parse partition path
      const match = partition.match(/year=(\d+)\/month=(\d+)\/day=(\d+)/);
      if (!match) continue;

      const yearStr = match[1];
      const monthStr = match[2];
      const dayStr = match[3];
      if (!yearStr || !monthStr || !dayStr) continue;
      const year = parseInt(yearStr, 10);
      const month = parseInt(monthStr, 10) - 1;
      const day = parseInt(dayStr, 10);
      const partitionDate = new Date(Date.UTC(year, month, day));

      if (partitionDate >= start && partitionDate <= end) {
        result.push(partition);
      }
    }

    return result;
  }

  /**
   * Deduplicate events by eventId
   */
  private deduplicateEvents(events: AnalyticsEvent[]): {
    unique: AnalyticsEvent[];
    duplicates: number;
  } {
    if (!this.config.deduplicationEnabled) {
      return { unique: events, duplicates: 0 };
    }

    const now = Date.now();
    const windowMs = this.config.deduplicationWindowMs ?? 300_000;
    const unique: AnalyticsEvent[] = [];
    let duplicates = 0;

    // Clean up old entries
    for (const [id, timestamp] of this.deduplicationSet) {
      if (now - timestamp > windowMs) {
        this.deduplicationSet.delete(id);
      }
    }

    // Check each event
    for (const event of events) {
      if (this.deduplicationSet.has(event.eventId)) {
        duplicates++;
      } else {
        this.deduplicationSet.set(event.eventId, now);
        unique.push(event);
      }
    }

    return { unique, duplicates };
  }

  /**
   * Persist a batch of events
   */
  async persistBatch(
    events: AnalyticsEvent[],
    context: { r2Bucket: R2Bucket; vfsStorage?: DurableObjectStorage }
  ): Promise<PersistResult> {
    const partitioned = this.partitionEvents(events);
    const now = new Date();
    let totalBytesWritten = 0;
    let usedFallback = false;
    let retryCount = 0;
    const partitionPaths: string[] = [];

    for (const [partitionPath, partitionEvents] of partitioned) {
      this.registerPartition(partitionPath);
      partitionPaths.push(partitionPath);

      // Try R2 first
      if (!this.config.simulateR2Failure) {
        try {
          const path = `analytics/${partitionPath}/${generateUUID()}.parquet`;
          const data = JSON.stringify(partitionEvents);
          const bytes = new TextEncoder().encode(data);

          await context.r2Bucket.put(path, bytes);
          totalBytesWritten += bytes.length;
          this.metrics.r2Writes++;
          continue;
        } catch (error) {
          // R2 failed, will retry then fallback
        }
      }

      // Retry logic
      let success = false;
      for (let i = 0; i < this.config.retryAttempts && !success; i++) {
        retryCount++;
        try {
          if (!this.config.simulateR2Failure) {
            const path = `analytics/${partitionPath}/${generateUUID()}.parquet`;
            const data = JSON.stringify(partitionEvents);
            const bytes = new TextEncoder().encode(data);

            await context.r2Bucket.put(path, bytes);
            totalBytesWritten += bytes.length;
            this.metrics.r2Writes++;
            success = true;
          }
        } catch (error) {
          // Retry failed
          await new Promise((resolve) => setTimeout(resolve, this.config.retryDelayMs));
        }
      }

      // Fallback to VFS if still failed
      if (!success && this.config.fallbackEnabled && context.vfsStorage) {
        usedFallback = true;
        this.metrics.vfsFallbacks++;
        const fallbackKey = `analytics_fallback_${partitionPath.replace(/\//g, '_')}_${Date.now()}`;
        await context.vfsStorage.put(fallbackKey, partitionEvents);
        totalBytesWritten += JSON.stringify(partitionEvents).length;
      }
    }

    const basePath = `analytics/year=${now.getUTCFullYear()}/month=${String(now.getUTCMonth() + 1).padStart(2, '0')}/day=${String(now.getUTCDate()).padStart(2, '0')}`;

    return {
      success: true,
      storageTier: usedFallback ? 'vfs' : 'r2',
      path: basePath,
      bytesWritten: totalBytesWritten,
      usedFallback,
      retryCount,
      filesWritten: partitionPaths.length,
      partitions: partitionPaths,
    };
  }

  /**
   * Recover data from VFS fallback to R2
   */
  async recoverFromFallback(
    vfsData: { batchId: string; events: AnalyticsEvent[]; storedAt: number },
    context: { r2Bucket: R2Bucket }
  ): Promise<RecoveryResult> {
    try {
      const result = await this.persistBatch(vfsData.events, { r2Bucket: context.r2Bucket });

      if (result.success && !result.usedFallback) {
        return {
          success: true,
          recoveredEvents: vfsData.events.length,
          newStorageTier: 'r2',
        };
      }

      return {
        success: false,
        recoveredEvents: 0,
        newStorageTier: 'vfs',
        error: 'Recovery failed, data remains in VFS',
      };
    } catch (error) {
      return {
        success: false,
        recoveredEvents: 0,
        newStorageTier: 'vfs',
        error: String(error),
      };
    }
  }

  /**
   * Handle an incoming analytics batch
   */
  async handleBatch(
    batch: AnalyticsEventBatch,
    context: { r2Bucket: R2Bucket; vfsStorage?: DurableObjectStorage }
  ): Promise<BatchHandleResult> {
    // Validate batch
    if (!batch || !batch.batchId) {
      return {
        type: 'analytics_nack',
        batchId: batch?.batchId ?? 'unknown',
        success: false,
        eventsProcessed: 0,
        error: 'Invalid batch: missing batchId',
      };
    }

    if (!batch.events || !Array.isArray(batch.events)) {
      return {
        type: 'analytics_nack',
        batchId: batch.batchId,
        success: false,
        eventsProcessed: 0,
        error: 'Invalid batch: missing or invalid events array',
      };
    }

    // Update metrics
    this.metrics.batchesReceived++;
    this.metrics.bytesReceived += batch.sizeBytes ?? 0;

    // Handle empty batch
    if (batch.events.length === 0) {
      return {
        type: 'analytics_ack',
        batchId: batch.batchId,
        success: true,
        eventsProcessed: 0,
      };
    }

    // Deduplicate
    const { unique, duplicates } = this.deduplicateEvents(batch.events);
    this.metrics.eventsReceived += unique.length;

    if (unique.length === 0) {
      return {
        type: 'analytics_ack',
        batchId: batch.batchId,
        success: true,
        eventsProcessed: 0,
        duplicatesSkipped: duplicates,
      };
    }

    // Persist
    try {
      const result = await this.persistBatch(unique, context);

      return {
        type: 'analytics_ack',
        batchId: batch.batchId,
        success: result.success,
        eventsProcessed: unique.length,
        duplicatesSkipped: duplicates,
      };
    } catch (error) {
      this.metrics.errors++;
      return {
        type: 'analytics_nack',
        batchId: batch.batchId,
        success: false,
        eventsProcessed: 0,
        error: String(error),
      };
    }
  }
}
