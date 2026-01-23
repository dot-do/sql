/**
 * DurabilityWriter - Facade that composes WriteBuffer, FlushStrategy, and PersistenceManager
 *
 * This class provides backward compatibility with the original DurabilityWriter API
 * while delegating responsibilities to single-responsibility classes.
 */

import type { CDCEvent } from '../types.js';
import {
  DurabilityTier,
  type WriteResult,
  type DurabilityConfig,
  type R2Storage,
  type KVStorage,
  type VFSStorage,
  DEFAULT_DURABILITY_CONFIG,
} from './types.js';
import { WriteBuffer, type WriteBufferConfig } from './write-buffer.js';
import { FlushStrategy, type FlushStrategyConfig } from './flush-strategy.js';
import { PersistenceManager, type PersistenceManagerConfig } from './persistence-manager.js';
import { classifyEvent } from './classification.js';

/**
 * Durability writer that handles event writes based on tier.
 *
 * This class is a facade that composes:
 * - WriteBuffer: Handles buffering of writes
 * - FlushStrategy: Handles flush timing and policies
 * - PersistenceManager: Handles actual persistence to storage
 */
export class DurabilityWriter {
  private readonly config: DurabilityConfig;
  private readonly writeBuffer: WriteBuffer;
  private readonly flushStrategy: FlushStrategy;
  private readonly persistenceManager: PersistenceManager;

  constructor(
    config: Partial<DurabilityConfig> = {},
    r2?: R2Storage,
    kv?: KVStorage,
    vfs?: VFSStorage
  ) {
    this.config = { ...DEFAULT_DURABILITY_CONFIG, ...config };

    // Initialize WriteBuffer
    const writeBufferConfig: Partial<WriteBufferConfig> = {};
    this.writeBuffer = new WriteBuffer(writeBufferConfig);

    // Initialize FlushStrategy
    const flushStrategyConfig: Partial<FlushStrategyConfig> = {
      maxP0Retries: this.config.maxP0Retries,
      maxP1Retries: this.config.maxP1Retries,
      baseRetryDelayMs: this.config.baseRetryDelayMs,
      maxRetryDelayMs: this.config.maxRetryDelayMs,
    };
    this.flushStrategy = new FlushStrategy(flushStrategyConfig);

    // Initialize PersistenceManager
    const persistenceConfig: Partial<PersistenceManagerConfig> = {
      dlqPathPrefix: this.config.dlqPathPrefix,
      vfsPathPrefix: this.config.vfsPathPrefix,
    };
    this.persistenceManager = new PersistenceManager(persistenceConfig, r2, kv, vfs);
  }

  /**
   * Set storage backends
   */
  setStorages(r2: R2Storage, kv?: KVStorage, vfs?: VFSStorage): void {
    this.persistenceManager.setStorages(r2, kv, vfs);
  }

  /**
   * Write an event with the appropriate durability guarantees
   */
  async writeWithDurability(event: CDCEvent): Promise<WriteResult> {
    const tier = classifyEvent(event);
    const startTime = Date.now();

    this.persistenceManager.recordWriteAttempt(tier);

    let result: WriteResult;

    switch (tier) {
      case DurabilityTier.P0:
        result = await this.writeP0(event);
        break;
      case DurabilityTier.P1:
        result = await this.writeP1(event);
        break;
      case DurabilityTier.P2:
        result = await this.writeP2(event);
        break;
      case DurabilityTier.P3:
        result = await this.writeP3(event);
        break;
      default:
        result = await this.writeP2(event); // Default to P2
    }

    result.latencyMs = Date.now() - startTime;
    result.tier = tier;

    // Update metrics
    this.persistenceManager.recordLatency(tier, result.latencyMs);

    return result;
  }

  /**
   * Write P0 event with dual write (R2 + KV)
   */
  private async writeP0(event: CDCEvent): Promise<WriteResult> {
    const writtenTo: ('R2' | 'KV' | 'VFS')[] = [];
    let retryCount = 0;
    const retryDelays: number[] = [];
    let error: string | undefined;

    // Check idempotency key for deduplication
    const idempotencyKey = event.metadata?.idempotencyKey as string | undefined;
    if (idempotencyKey) {
      const existing = await this.persistenceManager.readIdempotencyKey(idempotencyKey);
      if (existing) {
        return {
          success: true,
          tier: DurabilityTier.P0,
          writtenTo: [],
          usedFallback: false,
          retryCount: 0,
          retryDelays: [],
          dropped: false,
          sentToDLQ: false,
          latencyMs: 0,
        };
      }
    }

    const eventData = new TextEncoder().encode(JSON.stringify(event));
    const eventPath = this.persistenceManager.getEventPath(DurabilityTier.P0, event);

    // Retry loop for R2
    let r2Success = false;
    while (!r2Success && this.flushStrategy.shouldRetry(DurabilityTier.P0, retryCount)) {
      const r2Result = await this.persistenceManager.writeToR2(eventPath, eventData);

      if (r2Result.success) {
        r2Success = true;
        writtenTo.push('R2');
      } else {
        error = r2Result.error;
        if (retryCount > 0) {
          const delay = this.flushStrategy.calculateBackoff(retryCount);
          retryDelays.push(delay);
          await this.flushStrategy.delay(delay);
        }
        retryCount++;
      }
    }

    // Retry loop for KV
    let kvSuccess = false;
    let kvRetryCount = 0;
    while (!kvSuccess && this.flushStrategy.shouldRetry(DurabilityTier.P0, kvRetryCount)) {
      const kvResult = await this.persistenceManager.writeToKV(eventPath, JSON.stringify(event));

      if (kvResult.success) {
        kvSuccess = true;
        writtenTo.push('KV');

        // Store idempotency key
        if (idempotencyKey) {
          await this.persistenceManager.writeIdempotencyKey(idempotencyKey);
        }
      } else {
        error = kvResult.error;
        if (kvRetryCount > 0) {
          const delay = this.flushStrategy.calculateBackoff(kvRetryCount);
          if (!retryDelays.includes(delay)) {
            retryDelays.push(delay);
          }
          await this.flushStrategy.delay(delay);
        }
        kvRetryCount++;
      }
    }

    // Track write order
    if (event.sequence !== undefined) {
      this.writeBuffer.trackP0WriteOrder(event.sequence);
    }

    // If both failed, send to DLQ
    if (!r2Success && !kvSuccess) {
      const dlqPath = await this.persistenceManager.sendToDLQ(event);
      this.persistenceManager.recordWriteFailure(DurabilityTier.P0);
      this.persistenceManager.recordDLQ();

      return {
        success: false,
        tier: DurabilityTier.P0,
        writtenTo,
        usedFallback: false,
        retryCount: retryCount + kvRetryCount,
        retryDelays,
        dropped: false,
        sentToDLQ: true,
        dlqPath,
        error,
        latencyMs: 0,
      };
    }

    // At least one succeeded
    this.persistenceManager.recordWriteSuccess(DurabilityTier.P0);

    return {
      success: true,
      tier: DurabilityTier.P0,
      writtenTo,
      usedFallback: false,
      retryCount: retryCount + kvRetryCount,
      retryDelays,
      dropped: false,
      sentToDLQ: false,
      latencyMs: 0,
    };
  }

  /**
   * Write P1 event with R2 primary, KV fallback
   */
  private async writeP1(event: CDCEvent): Promise<WriteResult> {
    const writtenTo: ('R2' | 'KV' | 'VFS')[] = [];
    let retryCount = 0;
    const retryDelays: number[] = [];
    let error: string | undefined;
    let usedFallback = false;

    const eventData = new TextEncoder().encode(JSON.stringify(event));
    const eventPath = this.persistenceManager.getEventPath(DurabilityTier.P1, event);

    // Try R2 first with retries
    let r2Success = false;
    while (!r2Success && this.flushStrategy.shouldRetry(DurabilityTier.P1, retryCount)) {
      const r2Result = await this.persistenceManager.writeToR2(eventPath, eventData);

      if (r2Result.success) {
        r2Success = true;
        writtenTo.push('R2');
      } else {
        error = r2Result.error;
        if (retryCount > 0) {
          const delay = this.flushStrategy.calculateBackoff(retryCount);
          retryDelays.push(delay);
          await this.flushStrategy.delay(delay);
        }
        retryCount++;
      }
    }

    // If R2 failed, try KV fallback
    if (!r2Success) {
      const kvResult = await this.persistenceManager.writeToKV(eventPath, JSON.stringify(event));

      if (kvResult.success) {
        writtenTo.push('KV');
        usedFallback = true;

        // Track for later R2 sync
        this.writeBuffer.addFallbackEvent(event, DurabilityTier.P1);
        this.persistenceManager.recordP1Fallback();
        this.persistenceManager.recordWriteSuccess(DurabilityTier.P1);

        return {
          success: true,
          tier: DurabilityTier.P1,
          writtenTo,
          usedFallback,
          retryCount,
          retryDelays,
          dropped: false,
          sentToDLQ: false,
          latencyMs: 0,
        };
      } else {
        error = kvResult.error;
      }
    }

    if (r2Success) {
      this.persistenceManager.recordWriteSuccess(DurabilityTier.P1);
      return {
        success: true,
        tier: DurabilityTier.P1,
        writtenTo,
        usedFallback,
        retryCount,
        retryDelays,
        dropped: false,
        sentToDLQ: false,
        latencyMs: 0,
      };
    }

    // Both failed
    this.persistenceManager.recordWriteFailure(DurabilityTier.P1);
    return {
      success: false,
      tier: DurabilityTier.P1,
      writtenTo,
      usedFallback,
      retryCount,
      retryDelays,
      dropped: false,
      sentToDLQ: false,
      error,
      latencyMs: 0,
    };
  }

  /**
   * Write P2 event with R2 primary, VFS fallback
   */
  private async writeP2(event: CDCEvent): Promise<WriteResult> {
    const writtenTo: ('R2' | 'KV' | 'VFS')[] = [];
    let usedFallback = false;
    let error: string | undefined;

    const eventData = new TextEncoder().encode(JSON.stringify(event));
    const eventPath = this.persistenceManager.getEventPath(DurabilityTier.P2, event);

    // Try R2 (single attempt for P2)
    const r2Result = await this.persistenceManager.writeToR2(eventPath, eventData);

    if (r2Result.success) {
      writtenTo.push('R2');
      this.persistenceManager.recordWriteSuccess(DurabilityTier.P2);

      return {
        success: true,
        tier: DurabilityTier.P2,
        writtenTo,
        usedFallback,
        retryCount: 0,
        retryDelays: [],
        dropped: false,
        sentToDLQ: false,
        latencyMs: 0,
      };
    }

    error = r2Result.error;

    // R2 failed, try VFS fallback
    const vfsKey = this.persistenceManager.getVFSKey(event);
    const vfsResult = await this.persistenceManager.writeToVFS(vfsKey, event);

    if (vfsResult.success) {
      writtenTo.push('VFS');
      usedFallback = true;

      // Track for later R2 sync
      this.writeBuffer.addVFSPendingEvent(event, DurabilityTier.P2);
      this.persistenceManager.recordP2VFSFallback();
      this.persistenceManager.recordWriteSuccess(DurabilityTier.P2);

      return {
        success: true,
        tier: DurabilityTier.P2,
        writtenTo,
        usedFallback,
        retryCount: 0,
        retryDelays: [],
        dropped: false,
        sentToDLQ: false,
        latencyMs: 0,
      };
    }

    error = vfsResult.error;

    // Both failed
    this.persistenceManager.recordWriteFailure(DurabilityTier.P2);
    return {
      success: false,
      tier: DurabilityTier.P2,
      writtenTo,
      usedFallback,
      retryCount: 0,
      retryDelays: [],
      dropped: false,
      sentToDLQ: false,
      error,
      latencyMs: 0,
    };
  }

  /**
   * Write P3 event with best-effort (drop on failure)
   */
  private async writeP3(event: CDCEvent): Promise<WriteResult> {
    const writtenTo: ('R2' | 'KV' | 'VFS')[] = [];

    // Increment background writes for async processing
    this.writeBuffer.incrementBackgroundWrites();

    // Try R2 (single attempt, no retry, no fallback)
    const eventData = new TextEncoder().encode(JSON.stringify(event));
    const eventPath = this.persistenceManager.getEventPath(DurabilityTier.P3, event);

    const r2Result = await this.persistenceManager.writeToR2(eventPath, eventData);

    this.writeBuffer.decrementBackgroundWrites();

    if (r2Result.success) {
      writtenTo.push('R2');
      this.persistenceManager.recordWriteSuccess(DurabilityTier.P3);

      return {
        success: true,
        tier: DurabilityTier.P3,
        writtenTo,
        usedFallback: false,
        retryCount: 0,
        retryDelays: [],
        dropped: false,
        sentToDLQ: false,
        latencyMs: 0,
      };
    }

    // Failed - drop silently
    this.persistenceManager.recordWriteFailure(DurabilityTier.P3);

    return {
      success: true, // P3 "success" means we accepted it (even if dropped)
      tier: DurabilityTier.P3,
      writtenTo,
      usedFallback: false,
      retryCount: 0,
      retryDelays: [],
      dropped: true,
      sentToDLQ: false,
      latencyMs: 0,
    };
  }

  // ==========================================================================
  // Public API Methods for Backward Compatibility
  // ==========================================================================

  /**
   * Get VFS pending events for sync
   */
  getVFSPendingEvents(): Array<{ event: CDCEvent; tier: DurabilityTier }> {
    return this.writeBuffer.getVFSPendingEvents().map(({ event, tier }) => ({ event, tier }));
  }

  /**
   * Get fallback events for sync
   */
  getFallbackEvents(): Array<{ event: CDCEvent; tier: DurabilityTier }> {
    return this.writeBuffer.getFallbackEvents().map(({ event, tier }) => ({ event, tier }));
  }

  /**
   * Clear VFS pending events after successful sync
   */
  clearVFSPendingEvents(): void {
    this.writeBuffer.clearVFSPendingEvents();
  }

  /**
   * Clear fallback events after successful sync
   */
  clearFallbackEvents(): void {
    this.writeBuffer.clearFallbackEvents();
  }

  /**
   * Sync VFS events to R2
   */
  async syncVFSToR2(): Promise<{ synced: number; batches: number }> {
    const pendingEvents = this.writeBuffer.getVFSPendingEvents();

    if (pendingEvents.length === 0) {
      return { synced: 0, batches: 0 };
    }

    let synced = 0;
    let batches = 0;

    // Batch events for efficiency
    const batchSize = this.flushStrategy.getVFSSyncBatchSize();
    for (let i = 0; i < pendingEvents.length; i += batchSize) {
      const batch = pendingEvents.slice(i, i + batchSize);
      batches++;

      for (const { event } of batch) {
        const eventData = new TextEncoder().encode(JSON.stringify(event));
        const eventPath = this.persistenceManager.getEventPath(DurabilityTier.P2, event);

        const result = await this.persistenceManager.writeToR2(eventPath, eventData);
        if (result.success) {
          synced++;
        }
      }
    }

    // Remove synced events
    this.writeBuffer.removeVFSSyncedEvents(synced);

    return { synced, batches };
  }

  /**
   * Get P0 write order
   */
  getP0WriteOrder(): number[] {
    return this.writeBuffer.getP0WriteOrder();
  }

  /**
   * Get background writes pending count
   */
  getBackgroundWritesPending(): number {
    return this.writeBuffer.getBackgroundWritesPending();
  }

  /**
   * Get durability statistics
   */
  getStats() {
    return this.persistenceManager.getStats();
  }

  /**
   * Get latency percentiles by tier
   */
  getLatencyPercentiles(): {
    byTier: Record<DurabilityTier, { p50: number; p95: number; p99: number }>;
  } {
    return {
      byTier: this.persistenceManager.getAllLatencyPercentiles(),
    };
  }

  /**
   * Inject a failure for testing
   */
  injectFailure(target: string, count: number, permanent: boolean = false): void {
    this.persistenceManager.injectFailure(target, count, permanent);
  }

  /**
   * Clear all injected failures
   */
  clearFailures(): void {
    this.persistenceManager.clearFailures();
  }

  // ==========================================================================
  // Accessors for Component Classes (for advanced usage)
  // ==========================================================================

  /**
   * Get the WriteBuffer instance
   */
  getWriteBuffer(): WriteBuffer {
    return this.writeBuffer;
  }

  /**
   * Get the FlushStrategy instance
   */
  getFlushStrategy(): FlushStrategy {
    return this.flushStrategy;
  }

  /**
   * Get the PersistenceManager instance
   */
  getPersistenceManager(): PersistenceManager {
    return this.persistenceManager;
  }
}

/**
 * Default durability writer instance
 */
export const defaultDurabilityWriter = new DurabilityWriter();
