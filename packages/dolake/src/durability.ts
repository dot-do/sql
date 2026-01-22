/**
 * DoLake Durability Tiers
 *
 * Implements durability classification and write behavior for events.
 * Different events need different durability guarantees:
 *
 * P0 Critical (Stripe webhooks, payments)
 * - Always write to R2 AND KV
 * - Retry until confirmed
 * - Dead letter queue on permanent failure
 *
 * P1 Important (user actions, signups)
 * - Write to R2, fallback to KV on failure
 * - Retry 3x with exponential backoff
 *
 * P2 Standard (analytics, telemetry)
 * - Write to R2 only
 * - Fallback to DO fsx VFS on R2 failure
 * - Retry from VFS on next flush
 *
 * P3 Best-effort (anonymous visits)
 * - Write to R2
 * - Drop on failure (acceptable loss)
 */

import type { CDCEvent } from './types.js';

// =============================================================================
// Durability Tier Enum
// =============================================================================

/**
 * Durability tier levels from most durable (P0) to least (P3)
 */
export enum DurabilityTier {
  /** Critical events - dual write R2+KV, unlimited retry, DLQ on failure */
  P0 = 'P0',
  /** Important events - R2 with KV fallback, 3x retry with backoff */
  P1 = 'P1',
  /** Standard events - R2 with VFS fallback, retry on next flush */
  P2 = 'P2',
  /** Best-effort events - R2 only, drop on failure */
  P3 = 'P3',
}

// =============================================================================
// Classification Tables
// =============================================================================

/**
 * Tables that are classified as P0 (critical)
 */
const P0_TABLES = new Set([
  'stripe_webhooks',
  'payments',
  'payment_intents',
  'transactions',
  'orders',
  'subscriptions',
  'invoices',
  'refunds',
  'disputes',
]);

/**
 * Tables that are classified as P1 (important)
 */
const P1_TABLES = new Set([
  'users',
  'user_actions',
  'user_events',
  'accounts',
  'signups',
  'registrations',
  'authentications',
  'sessions',
  'api_keys',
  'permissions',
  'roles',
  'audit_logs',
]);

/**
 * Tables that are classified as P2 (standard)
 */
const P2_TABLES = new Set([
  'analytics_events',
  'analytics',
  'telemetry',
  'metrics',
  'logs',
  'events',
  'page_views',
  'clicks',
  'impressions',
]);

/**
 * Tables that are classified as P3 (best-effort)
 */
const P3_TABLES = new Set([
  'anonymous_visits',
  'anonymous_events',
  'tracking',
  'bot_visits',
  'health_checks',
  'ping',
]);

/**
 * Sources that imply P0 (critical)
 */
const P0_SOURCES = new Set([
  'stripe',
  'payment',
  'billing',
  'financial',
]);

/**
 * Sources that imply P1 (important)
 */
const P1_SOURCES = new Set([
  'auth',
  'authentication',
  'user',
  'account',
]);

/**
 * Sources that imply P3 (best-effort)
 */
const P3_SOURCES = new Set([
  'tracking',
  'anonymous',
  'bot',
]);

// =============================================================================
// Event Classification
// =============================================================================

/**
 * Classify an event into a durability tier based on its properties.
 *
 * Classification priority:
 * 1. Explicit durability metadata (highest priority)
 * 2. Table name matching
 * 3. Source matching
 * 4. Default to P2 (standard)
 *
 * @param event - The CDC event to classify
 * @returns The durability tier for the event
 */
export function classifyEvent(event: CDCEvent): DurabilityTier {
  // 1. Check for explicit durability metadata (highest priority)
  const explicitDurability = event.metadata?.durability as string | undefined;
  if (explicitDurability) {
    const tier = explicitDurability.toUpperCase();
    if (tier === 'P0' || tier === 'P1' || tier === 'P2' || tier === 'P3') {
      return tier as DurabilityTier;
    }
  }

  // 2. Check table name
  const tableName = event.table.toLowerCase();

  if (P0_TABLES.has(tableName)) {
    return DurabilityTier.P0;
  }
  if (P1_TABLES.has(tableName)) {
    return DurabilityTier.P1;
  }
  if (P2_TABLES.has(tableName)) {
    return DurabilityTier.P2;
  }
  if (P3_TABLES.has(tableName)) {
    return DurabilityTier.P3;
  }

  // Check for partial table name matches
  if (tableName.includes('payment') || tableName.includes('stripe') || tableName.includes('transaction')) {
    return DurabilityTier.P0;
  }
  if (tableName.includes('user') || tableName.includes('auth') || tableName.includes('account')) {
    return DurabilityTier.P1;
  }
  if (tableName.includes('analytics') || tableName.includes('telemetry') || tableName.includes('metric')) {
    return DurabilityTier.P2;
  }
  if (tableName.includes('anonymous') || tableName.includes('tracking') || tableName.includes('visit')) {
    return DurabilityTier.P3;
  }

  // 3. Check source metadata
  const source = (event.metadata?.source as string | undefined)?.toLowerCase();
  if (source) {
    if (P0_SOURCES.has(source)) {
      return DurabilityTier.P0;
    }
    if (P1_SOURCES.has(source)) {
      return DurabilityTier.P1;
    }
    if (P3_SOURCES.has(source)) {
      return DurabilityTier.P3;
    }
  }

  // 4. Default to P2 (standard)
  return DurabilityTier.P2;
}

/**
 * Classify multiple events and return their classifications
 *
 * @param events - Array of CDC events to classify
 * @returns Array of classification results
 */
export function classifyEvents(events: CDCEvent[]): Array<{
  event: CDCEvent;
  tier: DurabilityTier;
}> {
  return events.map((event) => ({
    event,
    tier: classifyEvent(event),
  }));
}

// =============================================================================
// Write Result Types
// =============================================================================

/**
 * Result of a durability write operation
 */
export interface WriteResult {
  /** Whether the write was successful */
  success: boolean;
  /** The durability tier used */
  tier: DurabilityTier;
  /** Where the event was written */
  writtenTo: ('R2' | 'KV' | 'VFS')[];
  /** Whether a fallback was used */
  usedFallback: boolean;
  /** Number of retries attempted */
  retryCount: number;
  /** Retry delays in milliseconds */
  retryDelays: number[];
  /** Whether the event was dropped (P3 only) */
  dropped: boolean;
  /** Whether the event was sent to DLQ (P0 only) */
  sentToDLQ: boolean;
  /** Path to DLQ entry if sent */
  dlqPath?: string | undefined;
  /** Error message if failed */
  error?: string | undefined;
  /** Write latency in milliseconds */
  latencyMs: number;
}

/**
 * Configuration for durability write behavior
 */
export interface DurabilityConfig {
  /** Maximum retries for P0 events (default: unlimited, use maxP0Retries) */
  maxP0Retries: number;
  /** Maximum retries for P1 events (default: 3) */
  maxP1Retries: number;
  /** Base delay for exponential backoff in ms (default: 100) */
  baseRetryDelayMs: number;
  /** Maximum retry delay in ms (default: 30000) */
  maxRetryDelayMs: number;
  /** DLQ path prefix */
  dlqPathPrefix: string;
  /** VFS path prefix for P2 fallback */
  vfsPathPrefix: string;
}

/**
 * Default durability configuration
 */
export const DEFAULT_DURABILITY_CONFIG: DurabilityConfig = {
  maxP0Retries: 100, // Effectively unlimited
  maxP1Retries: 3,
  baseRetryDelayMs: 100,
  maxRetryDelayMs: 30000,
  dlqPathPrefix: 'dlq/',
  vfsPathPrefix: 'vfs/',
};

// =============================================================================
// Storage Interfaces
// =============================================================================

/**
 * Interface for R2 storage operations
 */
export interface R2Storage {
  write(path: string, data: Uint8Array): Promise<void>;
  read(path: string): Promise<Uint8Array | null>;
  delete(path: string): Promise<void>;
}

/**
 * Interface for KV storage operations
 */
export interface KVStorage {
  write(key: string, data: string): Promise<void>;
  read(key: string): Promise<string | null>;
  delete(key: string): Promise<void>;
}

/**
 * Interface for VFS storage operations (DO storage)
 */
export interface VFSStorage {
  write(key: string, data: unknown): Promise<void>;
  read<T>(key: string): Promise<T | null>;
  delete(key: string): Promise<void>;
  list(prefix: string): Promise<string[]>;
}

// =============================================================================
// Durability Writer
// =============================================================================

/**
 * Durability writer that handles event writes based on tier
 */
export class DurabilityWriter {
  private config: DurabilityConfig;
  private r2: R2Storage | null = null;
  private kv: KVStorage | null = null;
  private vfs: VFSStorage | null = null;

  // Metrics
  private metrics = {
    byTier: {
      P0: { total: 0, succeeded: 0, failed: 0, dlq: 0, latencySum: 0 },
      P1: { total: 0, succeeded: 0, failed: 0, fallback: 0, latencySum: 0 },
      P2: { total: 0, succeeded: 0, failed: 0, vfs: 0, latencySum: 0 },
      P3: { total: 0, succeeded: 0, dropped: 0, latencySum: 0 },
    },
    latencies: {
      P0: [] as number[],
      P1: [] as number[],
      P2: [] as number[],
      P3: [] as number[],
    },
  };

  // Failure injection for testing
  private injectedFailures: Map<string, { count: number; permanent: boolean }> = new Map();

  // VFS pending events for later sync
  private vfsPendingEvents: Array<{ event: CDCEvent; tier: DurabilityTier }> = [];

  // Fallback events for later sync
  private fallbackEvents: Array<{ event: CDCEvent; tier: DurabilityTier }> = [];

  // P0 write order tracking
  private p0WriteOrder: number[] = [];

  // Background writes pending
  private backgroundWritesPending = 0;

  constructor(
    config: Partial<DurabilityConfig> = {},
    r2?: R2Storage,
    kv?: KVStorage,
    vfs?: VFSStorage
  ) {
    this.config = { ...DEFAULT_DURABILITY_CONFIG, ...config };
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
   * Write an event with the appropriate durability guarantees
   */
  async writeWithDurability(event: CDCEvent): Promise<WriteResult> {
    const tier = classifyEvent(event);
    const startTime = Date.now();

    this.metrics.byTier[tier].total++;

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
    this.metrics.latencies[tier].push(result.latencyMs);
    this.metrics.byTier[tier].latencySum += result.latencyMs;

    // Keep only last 1000 latencies
    if (this.metrics.latencies[tier].length > 1000) {
      this.metrics.latencies[tier].shift();
    }

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
    if (idempotencyKey && this.kv) {
      const existing = await this.kv.read(`idempotency:${idempotencyKey}`);
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
    const eventPath = `p0/${event.table}/${event.rowId}.json`;

    // Retry loop for R2
    let r2Success = false;
    while (!r2Success && retryCount < this.config.maxP0Retries) {
      try {
        if (this.shouldFail('R2')) {
          throw new Error('Injected R2 failure');
        }
        if (this.r2) {
          await this.r2.write(eventPath, eventData);
          r2Success = true;
          writtenTo.push('R2');
        }
      } catch (e) {
        error = String(e);
        if (retryCount > 0) {
          const delay = this.calculateBackoff(retryCount);
          retryDelays.push(delay);
          await this.delay(delay);
        }
        retryCount++;
      }
    }

    // Retry loop for KV
    let kvSuccess = false;
    let kvRetryCount = 0;
    while (!kvSuccess && kvRetryCount < this.config.maxP0Retries) {
      try {
        if (this.shouldFail('KV')) {
          throw new Error('Injected KV failure');
        }
        if (this.kv) {
          await this.kv.write(eventPath, JSON.stringify(event));
          kvSuccess = true;
          writtenTo.push('KV');

          // Store idempotency key
          if (idempotencyKey) {
            await this.kv.write(`idempotency:${idempotencyKey}`, 'true');
          }
        }
      } catch (e) {
        error = String(e);
        if (kvRetryCount > 0) {
          const delay = this.calculateBackoff(kvRetryCount);
          if (!retryDelays.includes(delay)) {
            retryDelays.push(delay);
          }
          await this.delay(delay);
        }
        kvRetryCount++;
      }
    }

    // Track write order
    if (event.sequence !== undefined) {
      this.p0WriteOrder.push(event.sequence);
    }

    // If both failed, send to DLQ
    if (!r2Success && !kvSuccess) {
      const dlqPath = await this.sendToDLQ(event);
      this.metrics.byTier.P0.failed++;
      this.metrics.byTier.P0.dlq++;

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
    this.metrics.byTier.P0.succeeded++;

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
    const eventPath = `p1/${event.table}/${event.rowId}.json`;

    // Try R2 first with 3 retries
    let r2Success = false;
    while (!r2Success && retryCount < this.config.maxP1Retries) {
      try {
        if (this.shouldFail('R2')) {
          throw new Error('Injected R2 failure');
        }
        if (this.r2) {
          await this.r2.write(eventPath, eventData);
          r2Success = true;
          writtenTo.push('R2');
        }
      } catch (e) {
        error = String(e);
        if (retryCount > 0) {
          const delay = this.calculateBackoff(retryCount);
          retryDelays.push(delay);
          await this.delay(delay);
        }
        retryCount++;
      }
    }

    // If R2 failed, try KV fallback
    if (!r2Success && this.kv) {
      try {
        if (this.shouldFail('KV') || this.shouldFail('ALL')) {
          throw new Error('Injected KV failure');
        }
        await this.kv.write(eventPath, JSON.stringify(event));
        writtenTo.push('KV');
        usedFallback = true;

        // Track for later R2 sync
        this.fallbackEvents.push({ event, tier: DurabilityTier.P1 });
        this.metrics.byTier.P1.fallback++;
        this.metrics.byTier.P1.succeeded++;

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
      } catch (e) {
        error = String(e);
      }
    }

    if (r2Success) {
      this.metrics.byTier.P1.succeeded++;
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
    this.metrics.byTier.P1.failed++;
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
    const eventPath = `p2/${event.table}/${event.rowId}.json`;

    // Try R2 (single attempt for P2)
    try {
      if (this.shouldFail('R2')) {
        throw new Error('Injected R2 failure');
      }
      if (this.r2) {
        await this.r2.write(eventPath, eventData);
        writtenTo.push('R2');
        this.metrics.byTier.P2.succeeded++;

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
    } catch (e) {
      error = String(e);
    }

    // R2 failed, try VFS fallback
    if (this.vfs) {
      try {
        const vfsKey = `${this.config.vfsPathPrefix}${eventPath}`;
        await this.vfs.write(vfsKey, event);
        writtenTo.push('VFS');
        usedFallback = true;

        // Track for later R2 sync
        this.vfsPendingEvents.push({ event, tier: DurabilityTier.P2 });
        this.metrics.byTier.P2.vfs++;
        this.metrics.byTier.P2.succeeded++;

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
      } catch (e) {
        error = String(e);
      }
    }

    // Both failed
    this.metrics.byTier.P2.failed++;
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
    this.backgroundWritesPending++;

    // Try R2 (single attempt, no retry, no fallback)
    try {
      if (this.shouldFail('R2')) {
        throw new Error('Injected R2 failure');
      }
      if (this.r2) {
        const eventData = new TextEncoder().encode(JSON.stringify(event));
        const eventPath = `p3/${event.table}/${event.rowId}.json`;
        await this.r2.write(eventPath, eventData);
        writtenTo.push('R2');
        this.metrics.byTier.P3.succeeded++;

        this.backgroundWritesPending--;
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
    } catch {
      // P3 events are dropped on failure - no error propagation
    }

    // Failed - drop silently
    this.metrics.byTier.P3.dropped++;
    this.backgroundWritesPending--;

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

  /**
   * Send event to dead letter queue
   */
  private async sendToDLQ(event: CDCEvent): Promise<string> {
    const timestamp = Date.now();
    const dlqPath = `${this.config.dlqPathPrefix}${event.table}/${timestamp}_${event.rowId}.json`;

    // Try to write to VFS if available (DO storage is most reliable for DLQ)
    if (this.vfs) {
      await this.vfs.write(dlqPath, event);
    }

    return dlqPath;
  }

  /**
   * Calculate exponential backoff delay
   */
  private calculateBackoff(retryCount: number): number {
    const delay = this.config.baseRetryDelayMs * Math.pow(2, retryCount);
    return Math.min(delay, this.config.maxRetryDelayMs);
  }

  /**
   * Delay helper
   */
  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Check if a storage target should fail (for testing)
   */
  private shouldFail(target: string): boolean {
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

  /**
   * Get VFS pending events for sync
   */
  getVFSPendingEvents(): Array<{ event: CDCEvent; tier: DurabilityTier }> {
    return [...this.vfsPendingEvents];
  }

  /**
   * Get fallback events for sync
   */
  getFallbackEvents(): Array<{ event: CDCEvent; tier: DurabilityTier }> {
    return [...this.fallbackEvents];
  }

  /**
   * Clear VFS pending events after successful sync
   */
  clearVFSPendingEvents(): void {
    this.vfsPendingEvents = [];
  }

  /**
   * Clear fallback events after successful sync
   */
  clearFallbackEvents(): void {
    this.fallbackEvents = [];
  }

  /**
   * Sync VFS events to R2
   */
  async syncVFSToR2(): Promise<{ synced: number; batches: number }> {
    if (this.vfsPendingEvents.length === 0) {
      return { synced: 0, batches: 0 };
    }

    let synced = 0;
    let batches = 0;

    // Batch events for efficiency
    const batchSize = 100;
    for (let i = 0; i < this.vfsPendingEvents.length; i += batchSize) {
      const batch = this.vfsPendingEvents.slice(i, i + batchSize);
      batches++;

      for (const { event } of batch) {
        try {
          const eventData = new TextEncoder().encode(JSON.stringify(event));
          const eventPath = `p2/${event.table}/${event.rowId}.json`;
          if (this.r2) {
            await this.r2.write(eventPath, eventData);
            synced++;
          }
        } catch {
          // Keep in VFS for next sync attempt
        }
      }
    }

    // Remove synced events
    this.vfsPendingEvents = this.vfsPendingEvents.slice(synced);

    return { synced, batches };
  }

  /**
   * Get P0 write order
   */
  getP0WriteOrder(): number[] {
    return [...this.p0WriteOrder];
  }

  /**
   * Get background writes pending count
   */
  getBackgroundWritesPending(): number {
    return this.backgroundWritesPending;
  }

  /**
   * Get durability statistics
   */
  getStats(): typeof this.metrics.byTier {
    return { ...this.metrics.byTier };
  }

  /**
   * Get latency percentiles by tier
   */
  getLatencyPercentiles(): {
    byTier: Record<DurabilityTier, { p50: number; p95: number; p99: number }>;
  } {
    const calculatePercentile = (arr: number[], p: number): number => {
      if (arr.length === 0) return 0;
      const sorted = [...arr].sort((a, b) => a - b);
      const index = Math.ceil((p / 100) * sorted.length) - 1;
      return sorted[Math.max(0, index)] ?? 0;
    };

    return {
      byTier: {
        [DurabilityTier.P0]: {
          p50: calculatePercentile(this.metrics.latencies.P0, 50),
          p95: calculatePercentile(this.metrics.latencies.P0, 95),
          p99: calculatePercentile(this.metrics.latencies.P0, 99),
        },
        [DurabilityTier.P1]: {
          p50: calculatePercentile(this.metrics.latencies.P1, 50),
          p95: calculatePercentile(this.metrics.latencies.P1, 95),
          p99: calculatePercentile(this.metrics.latencies.P1, 99),
        },
        [DurabilityTier.P2]: {
          p50: calculatePercentile(this.metrics.latencies.P2, 50),
          p95: calculatePercentile(this.metrics.latencies.P2, 95),
          p99: calculatePercentile(this.metrics.latencies.P2, 99),
        },
        [DurabilityTier.P3]: {
          p50: calculatePercentile(this.metrics.latencies.P3, 50),
          p95: calculatePercentile(this.metrics.latencies.P3, 95),
          p99: calculatePercentile(this.metrics.latencies.P3, 99),
        },
      },
    };
  }
}

/**
 * Default durability writer instance
 */
export const defaultDurabilityWriter = new DurabilityWriter();
