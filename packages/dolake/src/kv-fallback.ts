/**
 * DoLake KV Fallback Storage
 *
 * Provides KV-based fallback storage when R2 writes fail.
 * This is a secondary durability layer for P0/P1 events.
 *
 * Features:
 * - Detect R2 write failure and fallback to KV
 * - Compress events with gzip for efficient storage
 * - Write to KV with 25MB per value limit
 * - Chunk large batches across multiple keys when >25MB
 * - Recovery: retry R2 from KV on startup/alarm
 * - TTL management (7 days default)
 *
 * Issue: do-d1isn.5 - KV fallback storage (25MB limit)
 */

import type { CDCEvent } from './types.js';
import type { R2Storage, KVStorage } from './durability.js';

// =============================================================================
// Configuration Types
// =============================================================================

/**
 * Extended KV storage interface with TTL and list support
 */
export interface ExtendedKVStorage extends KVStorage {
  write(key: string, value: string, options?: { expirationTtl?: number }): Promise<void>;
  list(prefix: string): Promise<string[]>;
}

/**
 * Configuration for KV fallback storage
 */
export interface KVFallbackConfig {
  /** KV storage backend */
  kv: ExtendedKVStorage;
  /** R2 storage backend */
  r2: R2Storage;
  /** Maximum value size in bytes (default: 25MB) */
  maxValueSize?: number;
  /** TTL for KV entries in seconds (default: 7 days) */
  ttlSeconds?: number;
  /** Enable gzip compression (default: true) */
  enableCompression?: boolean;
  /** Enable chunking for large batches (default: true) */
  enableChunking?: boolean;
  /** Expiration warning threshold (0-1, default: 0.9) */
  expirationWarningThreshold?: number;
  /** Callback for recovery events */
  onRecovery?: (batchId: string, success: boolean) => void;
  /** Key prefix for fallback storage */
  keyPrefix?: string;
}

/**
 * Default configuration values
 */
export const DEFAULT_KV_FALLBACK_CONFIG = {
  maxValueSize: 25 * 1024 * 1024, // 25MB
  ttlSeconds: 7 * 24 * 60 * 60, // 7 days
  enableCompression: true,
  enableChunking: true,
  expirationWarningThreshold: 0.9,
  keyPrefix: 'fallback:',
} as const;

// =============================================================================
// Result Types
// =============================================================================

/**
 * Result of a fallback write operation
 */
export interface FallbackWriteResult {
  /** Whether R2 write failed */
  r2Failed: boolean;
  /** R2 error message if failed */
  r2Error?: string;
  /** Whether KV fallback was used */
  usedKVFallback: boolean;
  /** Whether KV write succeeded */
  kvWriteSuccess: boolean;
  /** Whether data was compressed */
  compressed: boolean;
  /** Compression ratio (compressed/original) */
  compressionRatio: number;
  /** Original size in bytes */
  originalSizeBytes: number;
  /** Compressed size in bytes */
  compressedSizeBytes: number;
  /** Whether data was chunked */
  chunked: boolean;
  /** Number of chunks */
  chunkCount: number;
  /** Chunk information */
  chunks?: ChunkInfo[];
  /** Manifest key for chunked data */
  manifestKey?: string;
  /** Number of events in batch */
  eventCount: number;
  /** Error message if operation failed */
  error?: string;
}

/**
 * Information about a single chunk
 */
export interface ChunkInfo {
  /** Chunk key */
  key: string;
  /** Chunk size in bytes */
  size: number;
  /** Chunk index (0-based) */
  index: number;
}

/**
 * Result of a recovery operation
 */
export interface RecoveryResult {
  /** Whether recovery succeeded */
  success: boolean;
  /** Number of events recovered */
  eventsRecovered: number;
  /** Error message if failed */
  error?: string;
}

/**
 * Result of bulk recovery operation
 */
export interface BulkRecoveryResult {
  /** Total batches attempted */
  totalBatches: number;
  /** Number of successful recoveries */
  successfulRecoveries: number;
  /** Number of failed recoveries */
  failedRecoveries: number;
  /** Details of each recovery */
  details: Array<{ batchId: string; success: boolean; error?: string }>;
}

/**
 * Stored batch data
 */
export interface StoredBatch {
  /** Batch ID */
  batchId: string;
  /** Events in batch */
  events: CDCEvent[];
  /** Timestamp when stored */
  timestamp: number;
  /** Expiration timestamp */
  expiresAt: number;
  /** Number of events */
  eventCount: number;
  /** Whether data was compressed */
  compressed: boolean;
  /** Original size before compression */
  originalSize: number;
}

/**
 * Pending batch with TTL info
 */
export interface PendingBatchInfo {
  /** Batch ID */
  batchId: string;
  /** Remaining TTL in seconds */
  remainingTTLSeconds: number;
  /** Event count */
  eventCount: number;
  /** Stored timestamp */
  timestamp: number;
}

/**
 * Recovery metrics
 */
export interface RecoveryMetrics {
  /** Total recovery attempts */
  totalRecoveryAttempts: number;
  /** Successful recoveries */
  successfulRecoveries: number;
  /** Failed recoveries */
  failedRecoveries: number;
  /** Total events recovered */
  totalEventsRecovered: number;
}

/**
 * Expiration warnings
 */
export interface ExpirationWarnings {
  /** Batches nearing expiration */
  expiringBatches: PendingBatchInfo[];
  /** Warning threshold used */
  thresholdPercentage: number;
}

/**
 * Fallback storage status
 */
export interface FallbackStatus {
  /** Number of batches pending recovery */
  pendingRecoveryCount: number;
  /** Total fallback writes performed */
  totalFallbackWrites: number;
  /** Total recoveries performed */
  totalRecoveries: number;
  /** Total events in fallback storage */
  totalEventsInFallback: number;
}

/**
 * Startup recovery result
 */
export interface StartupRecoveryResult {
  /** Number of batches recovered */
  batchesRecovered: number;
  /** Number of batches failed */
  batchesFailed: number;
  /** Total events recovered */
  eventsRecovered: number;
}

/**
 * Alarm handler result
 */
export interface AlarmHandlerResult {
  /** Whether recovery was attempted */
  attempted: boolean;
  /** Number of batches processed */
  batchesProcessed: number;
  /** Number of batches successfully recovered */
  batchesRecovered: number;
}

// =============================================================================
// KV Fallback Storage Implementation
// =============================================================================

/**
 * KV Fallback Storage for handling R2 write failures
 */
export class KVFallbackStorage {
  private kv: ExtendedKVStorage;
  private r2: R2Storage;
  private maxValueSize: number;
  private ttlSeconds: number;
  private enableCompression: boolean;
  private enableChunking: boolean;
  private expirationWarningThreshold: number;
  private onRecovery?: (batchId: string, success: boolean) => void;
  private keyPrefix: string;

  // Metrics
  private metrics: RecoveryMetrics = {
    totalRecoveryAttempts: 0,
    successfulRecoveries: 0,
    failedRecoveries: 0,
    totalEventsRecovered: 0,
  };

  private totalFallbackWrites = 0;

  constructor(config: KVFallbackConfig) {
    this.kv = config.kv;
    this.r2 = config.r2;
    this.maxValueSize = config.maxValueSize ?? DEFAULT_KV_FALLBACK_CONFIG.maxValueSize;
    this.ttlSeconds = config.ttlSeconds ?? DEFAULT_KV_FALLBACK_CONFIG.ttlSeconds;
    this.enableCompression = config.enableCompression ?? DEFAULT_KV_FALLBACK_CONFIG.enableCompression;
    this.enableChunking = config.enableChunking ?? DEFAULT_KV_FALLBACK_CONFIG.enableChunking;
    this.expirationWarningThreshold =
      config.expirationWarningThreshold ?? DEFAULT_KV_FALLBACK_CONFIG.expirationWarningThreshold;
    this.onRecovery = config.onRecovery;
    this.keyPrefix = config.keyPrefix ?? DEFAULT_KV_FALLBACK_CONFIG.keyPrefix;
  }

  /**
   * Write events with R2 primary, KV fallback
   */
  async writeWithFallback(events: CDCEvent[], batchId: string): Promise<FallbackWriteResult> {
    const result: FallbackWriteResult = {
      r2Failed: false,
      usedKVFallback: false,
      kvWriteSuccess: true,
      compressed: false,
      compressionRatio: 1,
      originalSizeBytes: 0,
      compressedSizeBytes: 0,
      chunked: false,
      chunkCount: 1,
      eventCount: events.length,
    };

    // Try R2 first
    try {
      const eventData = new TextEncoder().encode(JSON.stringify(events));
      result.originalSizeBytes = eventData.length;
      result.compressedSizeBytes = eventData.length;

      const r2Path = `events/${batchId}.json`;
      await this.r2.write(r2Path, eventData);

      // R2 succeeded, no fallback needed
      return result;
    } catch (error) {
      result.r2Failed = true;
      result.r2Error = error instanceof Error ? error.message : String(error);
    }

    // R2 failed, use KV fallback
    result.usedKVFallback = true;

    try {
      await this.writeToKV(events, batchId, result);
      this.totalFallbackWrites++;
    } catch (error) {
      result.kvWriteSuccess = false;
      result.error = error instanceof Error ? error.message : String(error);
    }

    return result;
  }

  /**
   * Write data to KV with compression and chunking
   */
  private async writeToKV(events: CDCEvent[], batchId: string, result: FallbackWriteResult): Promise<void> {
    const now = Date.now();
    const expiresAt = now + this.ttlSeconds * 1000;

    // Prepare stored batch
    const storedBatch: StoredBatch = {
      batchId,
      events,
      timestamp: now,
      expiresAt,
      eventCount: events.length,
      compressed: false,
      originalSize: 0,
    };

    // Serialize
    let data = JSON.stringify(storedBatch);
    result.originalSizeBytes = data.length;
    storedBatch.originalSize = data.length;

    // Compress if enabled
    if (this.enableCompression && data.length > 0) {
      const compressed = await this.compress(data);
      if (compressed.length < data.length) {
        data = compressed;
        result.compressed = true;
        storedBatch.compressed = true;
      }
    }

    result.compressedSizeBytes = data.length;
    result.compressionRatio = result.originalSizeBytes > 0 ? data.length / result.originalSizeBytes : 1;

    // Check if chunking is needed
    if (data.length > this.maxValueSize) {
      if (!this.enableChunking) {
        throw new Error(`Data size ${data.length} exceeds max value size ${this.maxValueSize}`);
      }
      await this.writeChunked(data, batchId, result);
    } else {
      // Single write
      const key = this.getKey(batchId);
      await this.kv.write(key, data, { expirationTtl: this.ttlSeconds });
      result.chunkCount = 1;
      result.chunked = false;
    }
  }

  /**
   * Write data in chunks across multiple keys
   */
  private async writeChunked(data: string, batchId: string, result: FallbackWriteResult): Promise<void> {
    const chunks: ChunkInfo[] = [];
    const chunkSize = this.maxValueSize - 100; // Leave room for metadata
    const numChunks = Math.ceil(data.length / chunkSize);

    result.chunked = true;
    result.chunkCount = numChunks;

    // Write each chunk
    for (let i = 0; i < numChunks; i++) {
      const start = i * chunkSize;
      const end = Math.min(start + chunkSize, data.length);
      const chunkData = data.slice(start, end);
      const chunkKey = this.getChunkKey(batchId, i);

      await this.kv.write(chunkKey, chunkData, { expirationTtl: this.ttlSeconds });

      chunks.push({
        key: chunkKey,
        size: chunkData.length,
        index: i,
      });
    }

    // Write manifest
    const manifestKey = this.getManifestKey(batchId);
    const manifest = {
      batchId,
      totalChunks: numChunks,
      chunkKeys: chunks.map((c) => c.key),
      totalSize: data.length,
      createdAt: Date.now(),
    };

    await this.kv.write(manifestKey, JSON.stringify(manifest), { expirationTtl: this.ttlSeconds });

    result.chunks = chunks;
    result.manifestKey = manifestKey;
  }

  /**
   * Read batch from fallback storage
   */
  async readFromFallback(batchId: string): Promise<StoredBatch | null> {
    // First check for manifest (chunked data)
    const manifestKey = this.getManifestKey(batchId);
    const manifestData = await this.kv.read(manifestKey);

    if (manifestData) {
      // Read chunked data
      const manifest = JSON.parse(manifestData) as {
        chunkKeys: string[];
        totalChunks: number;
      };

      const chunks: string[] = [];
      for (const chunkKey of manifest.chunkKeys) {
        const chunkData = await this.kv.read(chunkKey);
        if (!chunkData) {
          return null; // Incomplete data
        }
        chunks.push(chunkData);
      }

      const reassembled = chunks.join('');
      return this.parseStoredData(reassembled);
    }

    // Try single key
    const key = this.getKey(batchId);
    const data = await this.kv.read(key);

    if (!data) {
      return null;
    }

    return this.parseStoredData(data);
  }

  /**
   * Parse stored data, handling compression
   */
  private async parseStoredData(data: string): Promise<StoredBatch> {
    // Try to decompress if it looks compressed
    let parsed: StoredBatch;
    try {
      parsed = JSON.parse(data);
    } catch {
      // Might be compressed
      const decompressed = await this.decompress(data);
      parsed = JSON.parse(decompressed);
    }

    return parsed;
  }

  /**
   * List pending recovery batches
   */
  async listPendingRecovery(): Promise<string[]> {
    const keys = await this.kv.list(this.keyPrefix);

    // Filter to only batch keys (not chunks or manifests)
    const batchIds = new Set<string>();
    for (const key of keys) {
      const batchId = this.extractBatchId(key);
      if (batchId) {
        batchIds.add(batchId);
      }
    }

    return Array.from(batchIds);
  }

  /**
   * List pending batches with TTL information
   */
  async listPendingWithTTL(): Promise<PendingBatchInfo[]> {
    const batchIds = await this.listPendingRecovery();
    const results: PendingBatchInfo[] = [];

    for (const batchId of batchIds) {
      const stored = await this.readFromFallback(batchId);
      if (stored) {
        const remainingMs = stored.expiresAt - Date.now();
        results.push({
          batchId,
          remainingTTLSeconds: Math.max(0, Math.floor(remainingMs / 1000)),
          eventCount: stored.eventCount,
          timestamp: stored.timestamp,
        });
      }
    }

    return results;
  }

  /**
   * Recover a batch to R2
   */
  async recoverToR2(batchId: string): Promise<RecoveryResult> {
    this.metrics.totalRecoveryAttempts++;

    try {
      const stored = await this.readFromFallback(batchId);
      if (!stored) {
        return {
          success: false,
          eventsRecovered: 0,
          error: 'Batch not found in fallback storage',
        };
      }

      // Write to R2
      const eventData = new TextEncoder().encode(JSON.stringify(stored.events));
      const r2Path = `events/${batchId}.json`;
      await this.r2.write(r2Path, eventData);

      // Delete from KV
      await this.deleteFromFallback(batchId);

      this.metrics.successfulRecoveries++;
      this.metrics.totalEventsRecovered += stored.eventCount;

      this.onRecovery?.(batchId, true);

      return {
        success: true,
        eventsRecovered: stored.eventCount,
      };
    } catch (error) {
      this.metrics.failedRecoveries++;
      this.onRecovery?.(batchId, false);

      return {
        success: false,
        eventsRecovered: 0,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  /**
   * Recover all pending batches to R2
   */
  async recoverAllToR2(): Promise<BulkRecoveryResult> {
    const batchIds = await this.listPendingRecovery();
    const result: BulkRecoveryResult = {
      totalBatches: batchIds.length,
      successfulRecoveries: 0,
      failedRecoveries: 0,
      details: [],
    };

    for (const batchId of batchIds) {
      const recoveryResult = await this.recoverToR2(batchId);
      result.details.push({
        batchId,
        success: recoveryResult.success,
        error: recoveryResult.error,
      });

      if (recoveryResult.success) {
        result.successfulRecoveries++;
      } else {
        result.failedRecoveries++;
      }
    }

    return result;
  }

  /**
   * Delete a batch from fallback storage
   */
  private async deleteFromFallback(batchId: string): Promise<void> {
    // Delete manifest if exists
    const manifestKey = this.getManifestKey(batchId);
    const manifestData = await this.kv.read(manifestKey);

    if (manifestData) {
      const manifest = JSON.parse(manifestData) as { chunkKeys: string[] };
      // Delete all chunks
      for (const chunkKey of manifest.chunkKeys) {
        await this.kv.delete(chunkKey);
      }
      // Delete manifest
      await this.kv.delete(manifestKey);
    } else {
      // Delete single key
      await this.kv.delete(this.getKey(batchId));
    }
  }

  /**
   * Recover on startup
   */
  async recoverOnStartup(): Promise<StartupRecoveryResult> {
    const bulkResult = await this.recoverAllToR2();
    return {
      batchesRecovered: bulkResult.successfulRecoveries,
      batchesFailed: bulkResult.failedRecoveries,
      eventsRecovered: this.metrics.totalEventsRecovered,
    };
  }

  /**
   * Handle recovery alarm
   */
  async handleRecoveryAlarm(): Promise<AlarmHandlerResult> {
    const batchIds = await this.listPendingRecovery();

    if (batchIds.length === 0) {
      return {
        attempted: false,
        batchesProcessed: 0,
        batchesRecovered: 0,
      };
    }

    const result = await this.recoverAllToR2();

    return {
      attempted: true,
      batchesProcessed: result.totalBatches,
      batchesRecovered: result.successfulRecoveries,
    };
  }

  /**
   * Get expiration warnings
   */
  async getExpirationWarnings(): Promise<ExpirationWarnings> {
    const pending = await this.listPendingWithTTL();
    const warningThresholdSeconds = this.ttlSeconds * (1 - this.expirationWarningThreshold);

    const expiringBatches = pending.filter((p) => p.remainingTTLSeconds <= warningThresholdSeconds);

    return {
      expiringBatches,
      thresholdPercentage: this.expirationWarningThreshold * 100,
    };
  }

  /**
   * Get recovery metrics
   */
  getRecoveryMetrics(): RecoveryMetrics {
    return { ...this.metrics };
  }

  /**
   * Get fallback storage status
   */
  getStatus(): FallbackStatus {
    return {
      pendingRecoveryCount: 0, // Will be populated async if needed
      totalFallbackWrites: this.totalFallbackWrites,
      totalRecoveries: this.metrics.successfulRecoveries,
      totalEventsInFallback: 0, // Will be populated async if needed
    };
  }

  /**
   * Get KV storage instance
   */
  getKVStorage(): ExtendedKVStorage {
    return this.kv;
  }

  /**
   * Get R2 storage instance
   */
  getR2Storage(): R2Storage {
    return this.r2;
  }

  // =============================================================================
  // Key Generation Helpers
  // =============================================================================

  private getKey(batchId: string): string {
    return `${this.keyPrefix}${batchId}`;
  }

  private getChunkKey(batchId: string, index: number): string {
    return `${this.keyPrefix}${batchId}:chunk:${index}`;
  }

  private getManifestKey(batchId: string): string {
    return `${this.keyPrefix}${batchId}:manifest`;
  }

  private extractBatchId(key: string): string | null {
    if (!key.startsWith(this.keyPrefix)) {
      return null;
    }

    const rest = key.slice(this.keyPrefix.length);

    // Skip chunk and manifest keys
    if (rest.includes(':chunk:') || rest.includes(':manifest')) {
      return rest.split(':')[0];
    }

    return rest;
  }

  // =============================================================================
  // Compression Helpers
  // =============================================================================

  /**
   * Compress data using gzip
   */
  private async compress(data: string): Promise<string> {
    const encoder = new TextEncoder();
    const inputBytes = encoder.encode(data);

    // Use CompressionStream API
    const cs = new CompressionStream('gzip');
    const writer = cs.writable.getWriter();
    writer.write(inputBytes);
    writer.close();

    const compressedChunks: Uint8Array[] = [];
    const reader = cs.readable.getReader();

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      compressedChunks.push(value);
    }

    // Combine chunks
    const totalLength = compressedChunks.reduce((sum, chunk) => sum + chunk.length, 0);
    const compressed = new Uint8Array(totalLength);
    let offset = 0;
    for (const chunk of compressedChunks) {
      compressed.set(chunk, offset);
      offset += chunk.length;
    }

    // Base64 encode for KV storage (KV expects string)
    return this.uint8ArrayToBase64(compressed);
  }

  /**
   * Decompress gzip data
   */
  private async decompress(data: string): Promise<string> {
    const compressed = this.base64ToUint8Array(data);

    const ds = new DecompressionStream('gzip');
    const writer = ds.writable.getWriter();
    writer.write(compressed);
    writer.close();

    const decompressedChunks: Uint8Array[] = [];
    const reader = ds.readable.getReader();

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      decompressedChunks.push(value);
    }

    // Combine chunks
    const totalLength = decompressedChunks.reduce((sum, chunk) => sum + chunk.length, 0);
    const decompressed = new Uint8Array(totalLength);
    let offset = 0;
    for (const chunk of decompressedChunks) {
      decompressed.set(chunk, offset);
      offset += chunk.length;
    }

    const decoder = new TextDecoder();
    return decoder.decode(decompressed);
  }

  /**
   * Convert Uint8Array to base64 string
   */
  private uint8ArrayToBase64(bytes: Uint8Array): string {
    let binary = '';
    for (let i = 0; i < bytes.length; i++) {
      binary += String.fromCharCode(bytes[i]);
    }
    return btoa(binary);
  }

  /**
   * Convert base64 string to Uint8Array
   */
  private base64ToUint8Array(base64: string): Uint8Array {
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
      bytes[i] = binary.charCodeAt(i);
    }
    return bytes;
  }
}
