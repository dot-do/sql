/**
 * DoLake VFS Fallback Storage
 *
 * Emergency fallback storage using DO fsx VFS when KV/R2 writes fail.
 * Provides a last-resort storage option for P2 events with:
 *
 * - Storage limit enforcement (10MB default)
 * - Buffer rotation to prevent bloat
 * - Recovery: flush from VFS to R2 on alarm
 * - Deduplication by rowId
 *
 * Issue: do-d1isn.6 - DO fsx VFS emergency fallback
 */

import type { CDCEvent } from './types.js';
import { DurabilityTier, type VFSStorage, type R2Storage } from './durability.js';

// =============================================================================
// Configuration Types
// =============================================================================

/**
 * Configuration for VFS fallback storage
 */
export interface VFSFallbackConfig {
  /** Maximum storage in bytes (default: 10MB) */
  maxStorageBytes: number;
  /** Threshold percentage for rotation trigger (default: 80%) */
  rotationThresholdPercent: number;
  /** Maximum number of buffer files (default: 10) */
  maxBufferFiles: number;
  /** Size of each buffer file in bytes (default: 1MB) */
  bufferFileSizeBytes: number;
  /** Number of events to batch for recovery (default: 100) */
  recoveryBatchSize: number;
  /** Interval between recovery attempts in ms (default: 60000) */
  recoveryIntervalMs: number;
  /** VFS path prefix (default: 'vfs/') */
  vfsPathPrefix: string;
}

/**
 * Default VFS fallback configuration
 */
export const DEFAULT_VFS_FALLBACK_CONFIG: VFSFallbackConfig = {
  maxStorageBytes: 10 * 1024 * 1024, // 10MB
  rotationThresholdPercent: 80,
  maxBufferFiles: 10,
  bufferFileSizeBytes: 1024 * 1024, // 1MB
  recoveryBatchSize: 100,
  recoveryIntervalMs: 60000, // 1 minute
  vfsPathPrefix: 'vfs/',
};

// =============================================================================
// Statistics Types
// =============================================================================

/**
 * Statistics for VFS fallback storage
 */
export interface VFSStorageStats {
  /** Number of events currently in VFS */
  eventsInVFS: number;
  /** Current storage usage in bytes */
  currentStorageBytes: number;
  /** Storage utilization (0-1) */
  storageUtilization: number;
  /** Whether storage warning threshold exceeded */
  storageWarning: boolean;
  /** Number of buffer files */
  bufferFileCount: number;
  /** Number of buffer rotations */
  rotationCount: number;
  /** Last rotation timestamp */
  lastRotationTime?: number;
  /** Total bytes rotated */
  totalBytesRotated: number;
  /** Number of events dropped due to storage limits */
  eventsDropped: number;
  /** Number of corrupted entries found */
  corruptedEntries: number;
  /** KV failure count */
  kvFailureCount: number;
  /** R2 failure count */
  r2FailureCount: number;
  /** Total write attempts */
  totalWrites: number;
  /** Successful writes */
  successfulWrites: number;
  /** Total recovery attempts */
  totalRecoveryAttempts: number;
  /** Successful recoveries */
  successfulRecoveries: number;
  /** Events recovered to R2 */
  eventsRecovered: number;
  /** Last recovery timestamp */
  lastRecoveryTime?: number;
  /** Average write latency in ms */
  avgWriteLatencyMs: number;
  /** Maximum write latency in ms */
  maxWriteLatencyMs: number;
  /** Events by table */
  tableBreakdown?: Record<string, number>;
}

// =============================================================================
// Result Types
// =============================================================================

/**
 * Options for writing an event
 */
export interface VFSWriteOptions {
  /** Durability tier */
  tier: DurabilityTier;
  /** Whether R2 write failed */
  r2Failed?: boolean;
  /** Whether KV write failed */
  kvFailed?: boolean;
  /** Whether to skip KV (for P2) */
  skipKV?: boolean;
}

/**
 * Result of writing an event to VFS
 */
export interface VFSWriteResult {
  /** Whether write succeeded */
  success: boolean;
  /** Whether written to VFS */
  writtenToVFS: boolean;
  /** Event ID */
  eventId?: string | undefined;
  /** Path where event was stored */
  path?: string | undefined;
  /** Whether KV was skipped */
  skippedKV?: boolean | undefined;
  /** Whether storage limit was exceeded */
  storageLimitExceeded?: boolean | undefined;
  /** Error message if failed */
  error?: string | undefined;
  /** Error code */
  errorCode?: string | undefined;
}

/**
 * Result of batch write
 */
export interface VFSBatchWriteResult {
  /** Whether batch write succeeded */
  success: boolean;
  /** Number of events written */
  eventsWritten: number;
  /** Number of batches used */
  batchCount: number;
  /** Error message if failed */
  error?: string;
}

/**
 * Result of buffer rotation
 */
export interface BufferRotationResult {
  /** Whether rotation succeeded */
  success: boolean;
  /** Path of previous buffer */
  previousBufferPath?: string;
  /** Path of new buffer */
  newBufferPath?: string;
  /** Error message if failed */
  error?: string;
}

/**
 * Result of recovery to R2
 */
export interface RecoveryResult {
  /** Whether recovery succeeded completely */
  success: boolean;
  /** Number of events flushed to R2 */
  eventsFlushed: number;
  /** Number of events remaining in VFS */
  eventsRemaining: number;
  /** R2 paths written */
  r2Paths: string[];
  /** Number of batches written */
  batchCount: number;
  /** Whether there was nothing to recover */
  nothingToRecover?: boolean | undefined;
  /** Order of events written */
  writeOrder?: number[] | undefined;
  /** Whether next alarm was scheduled */
  nextAlarmScheduled?: boolean | undefined;
  /** Time of next alarm */
  nextAlarmTime?: number | undefined;
  /** Error message if failed */
  error?: string | undefined;
}

/**
 * Result of alarm handler
 */
export interface AlarmResult {
  /** Whether recovery was attempted */
  recoveryAttempted: boolean;
  /** Recovery result if attempted */
  recoveryResult?: RecoveryResult;
}

// =============================================================================
// Error Types
// =============================================================================

/**
 * Error class for VFS storage operations
 */
export class VFSStorageError extends Error {
  constructor(
    message: string,
    public readonly code: string,
    public readonly context?: Record<string, unknown>
  ) {
    super(message);
    this.name = 'VFSStorageError';
  }
}

// =============================================================================
// Internal Types
// =============================================================================

/**
 * Stored event with metadata
 */
interface StoredEvent {
  event: CDCEvent;
  storedAt: number;
  tier: DurabilityTier;
}

/**
 * Buffer file metadata
 */
interface BufferFileMetadata {
  id: string;
  path: string;
  createdAt: number;
  eventCount: number;
  sizeBytes: number;
}

// =============================================================================
// VFS Backend Interface
// =============================================================================

/**
 * Interface for VFS backend operations
 */
export interface VFSBackend {
  write(key: string, data: unknown): Promise<void>;
  read<T>(key: string): Promise<T | null>;
  delete(key: string): Promise<void>;
  list(prefix: string): Promise<string[]>;
}

// =============================================================================
// VFSFallbackStorage Implementation
// =============================================================================

/**
 * VFS Fallback Storage for emergency event persistence
 */
export class VFSFallbackStorage {
  private readonly config: VFSFallbackConfig;
  private readonly backend: VFSBackend;

  // Current buffer state
  private currentBufferId: string;
  private currentBufferEvents: Map<string, StoredEvent> = new Map();
  private currentBufferSize = 0;

  // Buffer file tracking
  private bufferFiles: BufferFileMetadata[] = [];

  // Statistics
  private stats: VFSStorageStats = {
    eventsInVFS: 0,
    currentStorageBytes: 0,
    storageUtilization: 0,
    storageWarning: false,
    bufferFileCount: 0,
    rotationCount: 0,
    totalBytesRotated: 0,
    eventsDropped: 0,
    corruptedEntries: 0,
    kvFailureCount: 0,
    r2FailureCount: 0,
    totalWrites: 0,
    successfulWrites: 0,
    totalRecoveryAttempts: 0,
    successfulRecoveries: 0,
    eventsRecovered: 0,
    avgWriteLatencyMs: 0,
    maxWriteLatencyMs: 0,
  };

  // Latency tracking
  private writeLatencies: number[] = [];
  private readonly maxLatencySamples = 1000;

  constructor(backend: VFSBackend, config: Partial<VFSFallbackConfig> = {}) {
    // Validate config
    this.validateConfig(config);

    this.config = { ...DEFAULT_VFS_FALLBACK_CONFIG, ...config };
    this.backend = backend;
    this.currentBufferId = this.generateBufferId();
  }

  /**
   * Validate configuration values
   */
  private validateConfig(config: Partial<VFSFallbackConfig>): void {
    if (config.maxStorageBytes !== undefined && config.maxStorageBytes < 0) {
      throw new Error('maxStorageBytes must be non-negative');
    }
    if (
      config.rotationThresholdPercent !== undefined &&
      (config.rotationThresholdPercent < 0 || config.rotationThresholdPercent > 100)
    ) {
      throw new Error('rotationThresholdPercent must be between 0 and 100');
    }
  }

  /**
   * Generate a unique buffer ID
   */
  private generateBufferId(): string {
    return `buffer_${Date.now()}_${Math.random().toString(36).slice(2, 10)}`;
  }

  /**
   * Get the current configuration
   */
  getConfig(): VFSFallbackConfig {
    return { ...this.config };
  }

  /**
   * Write a single event to VFS storage
   */
  async writeEvent(event: CDCEvent, options: VFSWriteOptions): Promise<VFSWriteResult> {
    const startTime = Date.now();
    this.stats.totalWrites++;

    // Track failures
    if (options.kvFailed) {
      this.stats.kvFailureCount++;
    }
    if (options.r2Failed) {
      this.stats.r2FailureCount++;
    }

    try {
      // Check storage limit
      const eventSize = this.calculateEventSize(event);

      // Try rotation first if approaching buffer size limit
      if (this.currentBufferSize >= this.config.bufferFileSizeBytes) {
        await this.rotateBuffer();
      }

      // Check total storage after rotation
      if (this.stats.currentStorageBytes + eventSize > this.config.maxStorageBytes) {
        // Try to enforce buffer limit to free space
        await this.enforceBufferLimit();

        // Check again after cleanup
        if (this.stats.currentStorageBytes + eventSize > this.config.maxStorageBytes) {
          return {
            success: false,
            writtenToVFS: false,
            storageLimitExceeded: true,
            error: `VFS storage limit exceeded: ${this.config.maxStorageBytes} bytes`,
            errorCode: 'STORAGE_LIMIT_EXCEEDED',
          };
        }
      }

      // Check for rotation threshold
      const utilization =
        this.stats.currentStorageBytes / this.config.maxStorageBytes;
      if (utilization >= this.config.rotationThresholdPercent / 100) {
        await this.maybeRotateBuffer();
      }

      // Store event with deduplication
      const storedEvent: StoredEvent = {
        event,
        storedAt: Date.now(),
        tier: options.tier,
      };

      // Use rowId as key for deduplication
      const eventKey = `${event.table}:${event.rowId}`;
      const existingEvent = this.currentBufferEvents.get(eventKey);

      if (existingEvent) {
        // Update existing - adjust size
        const oldSize = this.calculateEventSize(existingEvent.event);
        this.stats.currentStorageBytes -= oldSize;
        this.currentBufferSize -= oldSize;
      }

      this.currentBufferEvents.set(eventKey, storedEvent);
      this.stats.currentStorageBytes += eventSize;
      this.currentBufferSize += eventSize;
      this.stats.eventsInVFS++;

      // Update table breakdown
      if (!this.stats.tableBreakdown) {
        this.stats.tableBreakdown = {};
      }
      this.stats.tableBreakdown[event.table] =
        (this.stats.tableBreakdown[event.table] ?? 0) + 1;

      // Persist to backend
      const path = `${this.config.vfsPathPrefix}${this.currentBufferId}/${eventKey}`;
      await this.backend.write(path, storedEvent);

      // Update stats
      this.stats.successfulWrites++;
      this.updateStorageUtilization();
      this.recordLatency(Date.now() - startTime);

      return {
        success: true,
        writtenToVFS: true,
        eventId: event.rowId,
        path,
        skippedKV: options.skipKV,
      };
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : 'Unknown VFS write failure';

      return {
        success: false,
        writtenToVFS: false,
        error: errorMessage,
        errorCode: 'VFS_WRITE_FAILED',
      };
    }
  }

  /**
   * Write a batch of events to VFS storage
   */
  async writeBatch(
    events: CDCEvent[],
    options: VFSWriteOptions
  ): Promise<VFSBatchWriteResult> {
    let eventsWritten = 0;
    let batchCount = 0;

    try {
      // Group events by table for efficient storage
      const eventsByTable = new Map<string, CDCEvent[]>();
      for (const event of events) {
        const tableEvents = eventsByTable.get(event.table) ?? [];
        tableEvents.push(event);
        eventsByTable.set(event.table, tableEvents);
      }

      // Write each table group as a batch
      for (const [, tableEvents] of eventsByTable) {
        batchCount++;
        for (const event of tableEvents) {
          const result = await this.writeEvent(event, options);
          if (result.success) {
            eventsWritten++;
          }
        }
      }

      return {
        success: eventsWritten === events.length,
        eventsWritten,
        batchCount,
      };
    } catch (error) {
      return {
        success: false,
        eventsWritten,
        batchCount,
        error: error instanceof Error ? error.message : 'Batch write failed',
      };
    }
  }

  /**
   * Read all pending events from VFS
   */
  async readPendingEvents(): Promise<CDCEvent[]> {
    const events: StoredEvent[] = [];
    const seenKeys = new Set<string>();

    // Read from current buffer (in-memory)
    for (const [key, storedEvent] of this.currentBufferEvents.entries()) {
      events.push(storedEvent);
      seenKeys.add(key);
    }

    // Read from rotated buffer files
    for (const bufferFile of this.bufferFiles) {
      try {
        const keys = await this.backend.list(bufferFile.path);
        for (const key of keys) {
          // Skip if we've already seen this key
          const eventKey = key.split('/').pop() ?? key;
          if (seenKeys.has(eventKey)) continue;

          try {
            const storedEvent = await this.backend.read<StoredEvent>(key);
            if (storedEvent?.event) {
              events.push(storedEvent);
              seenKeys.add(eventKey);
            } else {
              this.stats.corruptedEntries++;
            }
          } catch {
            this.stats.corruptedEntries++;
          }
        }
      } catch {
        // Buffer file may not exist
      }
    }

    // Also check for any corrupted entries in the VFS path prefix
    try {
      const allKeys = await this.backend.list(this.config.vfsPathPrefix);
      for (const key of allKeys) {
        const eventKey = key.split('/').pop() ?? key;
        if (seenKeys.has(eventKey)) continue;

        try {
          const data = await this.backend.read<unknown>(key);
          // Check if it's a valid StoredEvent
          if (
            data &&
            typeof data === 'object' &&
            'event' in data &&
            (data as StoredEvent).event
          ) {
            events.push(data as StoredEvent);
            seenKeys.add(eventKey);
          } else if (data !== null) {
            // It's not a valid StoredEvent - mark as corrupted
            this.stats.corruptedEntries++;
          }
        } catch {
          this.stats.corruptedEntries++;
        }
      }
    } catch {
      // Ignore list errors
    }

    // Sort by timestamp
    events.sort((a, b) => {
      const aTime = typeof a.event.timestamp === 'number' ? a.event.timestamp : a.event.timestamp.getTime();
      const bTime = typeof b.event.timestamp === 'number' ? b.event.timestamp : b.event.timestamp.getTime();
      return aTime - bTime;
    });

    return events.map((se) => se.event);
  }

  /**
   * Rotate the current buffer
   */
  async rotateBuffer(): Promise<BufferRotationResult> {
    const previousPath = `${this.config.vfsPathPrefix}${this.currentBufferId}`;
    const previousBufferId = this.currentBufferId;

    try {
      // Persist current buffer to backend
      await this.persistCurrentBuffer();

      // Track rotated buffer
      this.bufferFiles.push({
        id: previousBufferId,
        path: previousPath,
        createdAt: Date.now(),
        eventCount: this.currentBufferEvents.size,
        sizeBytes: this.currentBufferSize,
      });

      // Update stats
      this.stats.rotationCount++;
      this.stats.lastRotationTime = Date.now();
      this.stats.totalBytesRotated += this.currentBufferSize;
      this.stats.bufferFileCount = this.bufferFiles.length;

      // Create new buffer
      this.currentBufferId = this.generateBufferId();
      this.currentBufferEvents.clear();
      this.currentBufferSize = 0;

      // Enforce max buffer files limit
      await this.enforceBufferLimit();

      return {
        success: true,
        previousBufferPath: previousPath,
        newBufferPath: `${this.config.vfsPathPrefix}${this.currentBufferId}`,
      };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : 'Rotation failed',
      };
    }
  }

  /**
   * Maybe rotate buffer if threshold exceeded
   */
  private async maybeRotateBuffer(): Promise<void> {
    if (this.currentBufferSize >= this.config.bufferFileSizeBytes) {
      await this.rotateBuffer();
    }
  }

  /**
   * Persist current buffer to backend storage
   */
  private async persistCurrentBuffer(): Promise<void> {
    const basePath = `${this.config.vfsPathPrefix}${this.currentBufferId}`;

    for (const [eventKey, storedEvent] of this.currentBufferEvents) {
      const path = `${basePath}/${eventKey}`;
      await this.backend.write(path, storedEvent);
    }
  }

  /**
   * Enforce maximum buffer files limit
   */
  private async enforceBufferLimit(): Promise<void> {
    // Drop oldest buffer files when exceeding max
    while (this.bufferFiles.length > this.config.maxBufferFiles) {
      const oldestBuffer = this.bufferFiles.shift();
      if (oldestBuffer) {
        // Delete old buffer file events
        try {
          const keys = await this.backend.list(oldestBuffer.path);
          for (const key of keys) {
            await this.backend.delete(key);
          }
          this.stats.eventsDropped += oldestBuffer.eventCount;
          this.stats.currentStorageBytes -= oldestBuffer.sizeBytes;
          this.stats.eventsInVFS -= oldestBuffer.eventCount;
        } catch {
          // Ignore deletion errors
        }
        this.stats.bufferFileCount = this.bufferFiles.length;
      }
    }

    // Also enforce storage limit by dropping oldest buffers
    while (
      this.stats.currentStorageBytes > this.config.maxStorageBytes &&
      this.bufferFiles.length > 0
    ) {
      const oldestBuffer = this.bufferFiles.shift();
      if (oldestBuffer) {
        try {
          const keys = await this.backend.list(oldestBuffer.path);
          for (const key of keys) {
            await this.backend.delete(key);
          }
          this.stats.eventsDropped += oldestBuffer.eventCount;
          this.stats.currentStorageBytes -= oldestBuffer.sizeBytes;
          this.stats.eventsInVFS -= oldestBuffer.eventCount;
        } catch {
          // Ignore deletion errors
        }
        this.stats.bufferFileCount = this.bufferFiles.length;
      }
    }
  }

  /**
   * Recover events from VFS to R2
   */
  async recoverToR2(r2Storage: R2Storage): Promise<RecoveryResult> {
    this.stats.totalRecoveryAttempts++;

    const pendingEvents = await this.readPendingEvents();

    if (pendingEvents.length === 0) {
      return {
        success: true,
        eventsFlushed: 0,
        eventsRemaining: 0,
        r2Paths: [],
        batchCount: 0,
        nothingToRecover: true,
      };
    }

    const r2Paths: string[] = [];
    const writeOrder: number[] = [];
    let eventsFlushed = 0;
    let batchCount = 0;

    try {
      // Process in batches
      for (
        let i = 0;
        i < pendingEvents.length;
        i += this.config.recoveryBatchSize
      ) {
        const batch = pendingEvents.slice(
          i,
          i + this.config.recoveryBatchSize
        );
        batchCount++;

        for (const event of batch) {
          try {
            const eventData = new TextEncoder().encode(JSON.stringify(event));
            const r2Path = `p2/${event.table}/${event.rowId}.json`;

            await r2Storage.write(r2Path, eventData);

            r2Paths.push(r2Path);
            if (event.sequence !== undefined) {
              writeOrder.push(event.sequence);
            }
            eventsFlushed++;

            // Remove from VFS
            await this.removeEvent(event);
          } catch {
            // Stop on R2 failure
            break;
          }
        }

        // Check if batch failed
        if (eventsFlushed < (i + batch.length)) {
          break;
        }
      }

      const success = eventsFlushed === pendingEvents.length;

      if (success) {
        this.stats.successfulRecoveries++;
      }

      this.stats.eventsRecovered += eventsFlushed;
      this.stats.lastRecoveryTime = Date.now();

      const eventsRemaining = pendingEvents.length - eventsFlushed;
      const needsNextAlarm = eventsRemaining > 0;

      return {
        success,
        eventsFlushed,
        eventsRemaining,
        r2Paths,
        batchCount,
        writeOrder,
        nextAlarmScheduled: needsNextAlarm,
        nextAlarmTime: needsNextAlarm
          ? Date.now() + this.config.recoveryIntervalMs
          : undefined,
      };
    } catch (error) {
      return {
        success: false,
        eventsFlushed,
        eventsRemaining: pendingEvents.length - eventsFlushed,
        r2Paths,
        batchCount,
        writeOrder,
        nextAlarmScheduled: true,
        nextAlarmTime: Date.now() + this.config.recoveryIntervalMs,
        error: error instanceof Error ? error.message : 'Recovery failed',
      };
    }
  }

  /**
   * Remove an event from VFS storage
   */
  private async removeEvent(event: CDCEvent): Promise<void> {
    const eventKey = `${event.table}:${event.rowId}`;

    // Remove from current buffer
    if (this.currentBufferEvents.has(eventKey)) {
      const storedEvent = this.currentBufferEvents.get(eventKey);
      if (storedEvent) {
        const size = this.calculateEventSize(storedEvent.event);
        this.currentBufferEvents.delete(eventKey);
        this.currentBufferSize -= size;
        this.stats.currentStorageBytes -= size;
        this.stats.eventsInVFS--;
      }
    }

    // Try to delete from backend
    const basePath = `${this.config.vfsPathPrefix}${this.currentBufferId}/${eventKey}`;
    try {
      await this.backend.delete(basePath);
    } catch {
      // Ignore deletion errors
    }

    // Also check rotated buffers
    for (const bufferFile of this.bufferFiles) {
      const path = `${bufferFile.path}/${eventKey}`;
      try {
        await this.backend.delete(path);
      } catch {
        // Ignore deletion errors
      }
    }
  }

  /**
   * Handle alarm for recovery
   */
  async onAlarm(r2Storage: R2Storage): Promise<AlarmResult> {
    const pendingEvents = await this.readPendingEvents();

    if (pendingEvents.length === 0) {
      return {
        recoveryAttempted: false,
      };
    }

    const recoveryResult = await this.recoverToR2(r2Storage);

    return {
      recoveryAttempted: true,
      recoveryResult,
    };
  }

  /**
   * Get storage statistics
   */
  getStats(): VFSStorageStats {
    this.updateStorageUtilization();
    return { ...this.stats };
  }

  /**
   * Get Prometheus-compatible metrics
   */
  getPrometheusMetrics(): string {
    const lines: string[] = [];

    lines.push(`# HELP dolake_vfs_events_total Total events in VFS storage`);
    lines.push(`# TYPE dolake_vfs_events_total gauge`);
    lines.push(`dolake_vfs_events_total ${this.stats.eventsInVFS}`);

    lines.push(`# HELP dolake_vfs_storage_bytes Current VFS storage usage in bytes`);
    lines.push(`# TYPE dolake_vfs_storage_bytes gauge`);
    lines.push(`dolake_vfs_storage_bytes ${this.stats.currentStorageBytes}`);

    lines.push(
      `# HELP dolake_vfs_write_latency_seconds Average write latency in seconds`
    );
    lines.push(`# TYPE dolake_vfs_write_latency_seconds gauge`);
    lines.push(
      `dolake_vfs_write_latency_seconds ${this.stats.avgWriteLatencyMs / 1000}`
    );

    lines.push(`# HELP dolake_vfs_rotations_total Total buffer rotations`);
    lines.push(`# TYPE dolake_vfs_rotations_total counter`);
    lines.push(`dolake_vfs_rotations_total ${this.stats.rotationCount}`);

    lines.push(`# HELP dolake_vfs_recoveries_total Total recovery attempts`);
    lines.push(`# TYPE dolake_vfs_recoveries_total counter`);
    lines.push(`dolake_vfs_recoveries_total ${this.stats.totalRecoveryAttempts}`);

    return lines.join('\n');
  }

  /**
   * Get VFS storage interface compatible with DurabilityWriter
   */
  asVFSStorage(): VFSStorage {
    return {
      write: async (key: string, data: unknown) => {
        await this.backend.write(key, data);
      },
      read: async <T>(key: string) => {
        return this.backend.read<T>(key);
      },
      delete: async (key: string) => {
        await this.backend.delete(key);
      },
      list: async (prefix: string) => {
        return this.backend.list(prefix);
      },
    };
  }

  /**
   * Calculate the size of an event in bytes
   */
  private calculateEventSize(event: CDCEvent): number {
    // Include metadata overhead
    const eventJson = JSON.stringify(event);
    const metadataOverhead = 100; // Approximate overhead for stored event metadata
    return eventJson.length + metadataOverhead;
  }

  /**
   * Update storage utilization stats
   */
  private updateStorageUtilization(): void {
    this.stats.storageUtilization =
      this.stats.currentStorageBytes / this.config.maxStorageBytes;
    this.stats.storageWarning =
      this.stats.storageUtilization >=
      this.config.rotationThresholdPercent / 100;
  }

  /**
   * Record write latency
   */
  private recordLatency(latencyMs: number): void {
    // Use performance.now() style minimum of 0.001ms for very fast operations
    const effectiveLatency = Math.max(latencyMs, 0.001);
    this.writeLatencies.push(effectiveLatency);

    // Keep only recent samples
    if (this.writeLatencies.length > this.maxLatencySamples) {
      this.writeLatencies.shift();
    }

    // Update stats
    if (this.writeLatencies.length > 0) {
      const sum = this.writeLatencies.reduce((a, b) => a + b, 0);
      this.stats.avgWriteLatencyMs = sum / this.writeLatencies.length;
      this.stats.maxWriteLatencyMs = Math.max(...this.writeLatencies);
    }
  }
}
