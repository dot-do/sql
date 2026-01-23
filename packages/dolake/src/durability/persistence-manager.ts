/**
 * PersistenceManager - Handles actual persistence to storage
 *
 * Single responsibility: Managing writes to R2, KV, and VFS storage
 * backends with proper error handling and metrics tracking.
 */

import type { CDCEvent } from '../types.js';
import { DurabilityTier } from './types.js';
import type { R2Storage, KVStorage, VFSStorage } from './types.js';

/**
 * Configuration for persistence manager
 */
export interface PersistenceManagerConfig {
  /** DLQ path prefix */
  dlqPathPrefix: string;
  /** VFS path prefix for P2 fallback */
  vfsPathPrefix: string;
}

/**
 * Default persistence manager configuration
 */
export const DEFAULT_PERSISTENCE_MANAGER_CONFIG: PersistenceManagerConfig = {
  dlqPathPrefix: 'dlq/',
  vfsPathPrefix: 'vfs/',
};

/**
 * Result of a write operation
 */
export interface WriteOperationResult {
  /** Whether the write was successful */
  success: boolean;
  /** Where the data was written */
  writtenTo: ('R2' | 'KV' | 'VFS')[];
  /** Error message if failed */
  error?: string;
}

/**
 * Metrics for a specific tier
 */
export interface TierMetrics {
  total: number;
  succeeded: number;
  failed: number;
  latencySum: number;
}

/**
 * P0-specific metrics
 */
export interface P0Metrics extends TierMetrics {
  dlq: number;
}

/**
 * P1-specific metrics
 */
export interface P1Metrics extends TierMetrics {
  fallback: number;
}

/**
 * P2-specific metrics
 */
export interface P2Metrics extends TierMetrics {
  vfs: number;
}

/**
 * P3-specific metrics
 */
export interface P3Metrics {
  total: number;
  succeeded: number;
  dropped: number;
  latencySum: number;
}

/**
 * All tier metrics
 */
export interface AllTierMetrics {
  P0: P0Metrics;
  P1: P1Metrics;
  P2: P2Metrics;
  P3: P3Metrics;
}

/**
 * Failure injection for testing
 */
interface FailureInjection {
  count: number;
  permanent: boolean;
}

/**
 * PersistenceManager - Manages writes to storage backends
 */
export class PersistenceManager {
  private readonly config: PersistenceManagerConfig;
  private r2: R2Storage | null = null;
  private kv: KVStorage | null = null;
  private vfs: VFSStorage | null = null;

  // Metrics
  private metrics: AllTierMetrics = {
    P0: { total: 0, succeeded: 0, failed: 0, dlq: 0, latencySum: 0 },
    P1: { total: 0, succeeded: 0, failed: 0, fallback: 0, latencySum: 0 },
    P2: { total: 0, succeeded: 0, failed: 0, vfs: 0, latencySum: 0 },
    P3: { total: 0, succeeded: 0, dropped: 0, latencySum: 0 },
  };

  // Latency tracking
  private latencies: Record<DurabilityTier, number[]> = {
    [DurabilityTier.P0]: [],
    [DurabilityTier.P1]: [],
    [DurabilityTier.P2]: [],
    [DurabilityTier.P3]: [],
  };

  // Failure injection for testing
  private injectedFailures: Map<string, FailureInjection> = new Map();

  constructor(
    config: Partial<PersistenceManagerConfig> = {},
    r2?: R2Storage,
    kv?: KVStorage,
    vfs?: VFSStorage
  ) {
    this.config = { ...DEFAULT_PERSISTENCE_MANAGER_CONFIG, ...config };
    this.r2 = r2 ?? null;
    this.kv = kv ?? null;
    this.vfs = vfs ?? null;
  }

  /**
   * Set storage backends
   */
  setStorages(r2: R2Storage, kv?: KVStorage, vfs?: VFSStorage): void {
    this.r2 = r2;
    this.kv = kv ?? null;
    this.vfs = vfs ?? null;
  }

  /**
   * Get R2 storage
   */
  getR2Storage(): R2Storage | null {
    return this.r2;
  }

  /**
   * Get KV storage
   */
  getKVStorage(): KVStorage | null {
    return this.kv;
  }

  /**
   * Get VFS storage
   */
  getVFSStorage(): VFSStorage | null {
    return this.vfs;
  }

  /**
   * Write to R2 storage
   */
  async writeToR2(path: string, data: Uint8Array): Promise<WriteOperationResult> {
    if (!this.r2) {
      return { success: false, writtenTo: [], error: 'R2 storage not configured' };
    }

    try {
      if (this.shouldFail('R2')) {
        throw new Error('Injected R2 failure');
      }
      await this.r2.write(path, data);
      return { success: true, writtenTo: ['R2'] };
    } catch (e) {
      return { success: false, writtenTo: [], error: String(e) };
    }
  }

  /**
   * Write to KV storage
   */
  async writeToKV(key: string, data: string): Promise<WriteOperationResult> {
    if (!this.kv) {
      return { success: false, writtenTo: [], error: 'KV storage not configured' };
    }

    try {
      if (this.shouldFail('KV')) {
        throw new Error('Injected KV failure');
      }
      await this.kv.write(key, data);
      return { success: true, writtenTo: ['KV'] };
    } catch (e) {
      return { success: false, writtenTo: [], error: String(e) };
    }
  }

  /**
   * Write to VFS storage
   */
  async writeToVFS(key: string, data: unknown): Promise<WriteOperationResult> {
    if (!this.vfs) {
      return { success: false, writtenTo: [], error: 'VFS storage not configured' };
    }

    try {
      await this.vfs.write(key, data);
      return { success: true, writtenTo: ['VFS'] };
    } catch (e) {
      return { success: false, writtenTo: [], error: String(e) };
    }
  }

  /**
   * Read from KV for idempotency check
   */
  async readIdempotencyKey(key: string): Promise<string | null> {
    if (!this.kv) return null;
    try {
      return await this.kv.read(`idempotency:${key}`);
    } catch {
      return null;
    }
  }

  /**
   * Write idempotency key
   */
  async writeIdempotencyKey(key: string): Promise<void> {
    if (!this.kv) return;
    try {
      await this.kv.write(`idempotency:${key}`, 'true');
    } catch {
      // Ignore idempotency key write failures
    }
  }

  /**
   * Send event to dead letter queue
   */
  async sendToDLQ(event: CDCEvent): Promise<string> {
    const timestamp = Date.now();
    const dlqPath = `${this.config.dlqPathPrefix}${event.table}/${timestamp}_${event.rowId}.json`;

    if (this.vfs) {
      try {
        await this.vfs.write(dlqPath, event);
      } catch {
        // Best effort - DLQ write failure is logged but not thrown
      }
    }

    return dlqPath;
  }

  /**
   * Get event path for a specific tier
   */
  getEventPath(tier: DurabilityTier, event: CDCEvent): string {
    const tierPrefix = tier.toLowerCase();
    return `${tierPrefix}/${event.table}/${event.rowId}.json`;
  }

  /**
   * Get VFS key for event
   */
  getVFSKey(event: CDCEvent): string {
    const eventPath = this.getEventPath(DurabilityTier.P2, event);
    return `${this.config.vfsPathPrefix}${eventPath}`;
  }

  // ==========================================================================
  // Metrics Methods
  // ==========================================================================

  /**
   * Record a write attempt for a tier
   */
  recordWriteAttempt(tier: DurabilityTier): void {
    this.metrics[tier].total++;
  }

  /**
   * Record a successful write
   */
  recordWriteSuccess(tier: DurabilityTier): void {
    this.metrics[tier].succeeded++;
  }

  /**
   * Record a failed write
   */
  recordWriteFailure(tier: DurabilityTier): void {
    if (tier === DurabilityTier.P3) {
      (this.metrics.P3 as P3Metrics).dropped++;
    } else {
      (this.metrics[tier] as TierMetrics).failed++;
    }
  }

  /**
   * Record a DLQ send (P0 only)
   */
  recordDLQ(): void {
    this.metrics.P0.dlq++;
  }

  /**
   * Record a fallback use (P1 only)
   */
  recordP1Fallback(): void {
    this.metrics.P1.fallback++;
  }

  /**
   * Record a VFS fallback use (P2 only)
   */
  recordP2VFSFallback(): void {
    this.metrics.P2.vfs++;
  }

  /**
   * Record latency for a tier
   */
  recordLatency(tier: DurabilityTier, latencyMs: number): void {
    this.metrics[tier].latencySum += latencyMs;
    this.latencies[tier].push(latencyMs);

    // Keep only last 1000 latencies
    if (this.latencies[tier].length > 1000) {
      this.latencies[tier].shift();
    }
  }

  /**
   * Get all tier metrics
   */
  getStats(): AllTierMetrics {
    return { ...this.metrics };
  }

  /**
   * Get latency percentiles for a tier
   */
  getLatencyPercentiles(tier: DurabilityTier): { p50: number; p95: number; p99: number } {
    const arr = this.latencies[tier];
    if (arr.length === 0) {
      return { p50: 0, p95: 0, p99: 0 };
    }

    const sorted = [...arr].sort((a, b) => a - b);
    const p50Index = Math.ceil((50 / 100) * sorted.length) - 1;
    const p95Index = Math.ceil((95 / 100) * sorted.length) - 1;
    const p99Index = Math.ceil((99 / 100) * sorted.length) - 1;

    return {
      p50: sorted[Math.max(0, p50Index)] ?? 0,
      p95: sorted[Math.max(0, p95Index)] ?? 0,
      p99: sorted[Math.max(0, p99Index)] ?? 0,
    };
  }

  /**
   * Get all latency percentiles
   */
  getAllLatencyPercentiles(): Record<DurabilityTier, { p50: number; p95: number; p99: number }> {
    return {
      [DurabilityTier.P0]: this.getLatencyPercentiles(DurabilityTier.P0),
      [DurabilityTier.P1]: this.getLatencyPercentiles(DurabilityTier.P1),
      [DurabilityTier.P2]: this.getLatencyPercentiles(DurabilityTier.P2),
      [DurabilityTier.P3]: this.getLatencyPercentiles(DurabilityTier.P3),
    };
  }

  // ==========================================================================
  // Failure Injection (Testing)
  // ==========================================================================

  /**
   * Check if a storage target should fail (for testing)
   */
  shouldFail(target: string): boolean {
    const failure = this.injectedFailures.get(target);
    if (!failure) return false;

    if (failure.permanent) return true;

    if (failure.count > 0) {
      failure.count--;
      return true;
    }

    this.injectedFailures.delete(target);
    return false;
  }

  /**
   * Inject a failure for testing
   */
  injectFailure(target: string, count: number, permanent: boolean = false): void {
    this.injectedFailures.set(target, { count, permanent });
  }

  /**
   * Clear all injected failures
   */
  clearFailures(): void {
    this.injectedFailures.clear();
  }
}
