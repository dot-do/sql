/**
 * Durability Types
 *
 * Shared types for the durability module.
 */

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
