/**
 * WriteBuffer - Handles buffering of writes
 *
 * Single responsibility: Managing write buffers for CDC events
 * including deduplication, ordering, and memory management.
 */

import type { CDCEvent } from '../types.js';
import { DurabilityTier } from './types.js';

/**
 * Pending event with its durability tier
 */
export interface PendingEvent {
  event: CDCEvent;
  tier: DurabilityTier;
  addedAt: number;
}

/**
 * Configuration for the write buffer
 */
export interface WriteBufferConfig {
  /** Maximum number of events to buffer (default: 10000) */
  maxBufferSize: number;
  /** Maximum buffer age in ms before forcing flush (default: 30000) */
  maxBufferAgeMs: number;
}

/**
 * Default write buffer configuration
 */
export const DEFAULT_WRITE_BUFFER_CONFIG: WriteBufferConfig = {
  maxBufferSize: 10000,
  maxBufferAgeMs: 30000,
};

/**
 * WriteBuffer - Manages buffering of CDC events before persistence
 */
export class WriteBuffer {
  private readonly config: WriteBufferConfig;

  // VFS pending events for later sync
  private vfsPendingEvents: PendingEvent[] = [];

  // Fallback events for later sync
  private fallbackEvents: PendingEvent[] = [];

  // P0 write order tracking
  private p0WriteOrder: number[] = [];

  // Background writes pending
  private backgroundWritesPending = 0;

  // Buffer creation time for age tracking
  private bufferCreatedAt: number = Date.now();

  constructor(config: Partial<WriteBufferConfig> = {}) {
    this.config = { ...DEFAULT_WRITE_BUFFER_CONFIG, ...config };
  }

  /**
   * Add an event to the VFS pending buffer
   */
  addVFSPendingEvent(event: CDCEvent, tier: DurabilityTier): void {
    this.vfsPendingEvents.push({
      event,
      tier,
      addedAt: Date.now(),
    });
  }

  /**
   * Add an event to the fallback buffer
   */
  addFallbackEvent(event: CDCEvent, tier: DurabilityTier): void {
    this.fallbackEvents.push({
      event,
      tier,
      addedAt: Date.now(),
    });
  }

  /**
   * Track P0 write order
   */
  trackP0WriteOrder(sequence: number): void {
    this.p0WriteOrder.push(sequence);
  }

  /**
   * Increment background writes counter
   */
  incrementBackgroundWrites(): void {
    this.backgroundWritesPending++;
  }

  /**
   * Decrement background writes counter
   */
  decrementBackgroundWrites(): void {
    this.backgroundWritesPending--;
  }

  /**
   * Get VFS pending events
   */
  getVFSPendingEvents(): PendingEvent[] {
    return [...this.vfsPendingEvents];
  }

  /**
   * Get fallback events
   */
  getFallbackEvents(): PendingEvent[] {
    return [...this.fallbackEvents];
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
   * Clear VFS pending events
   */
  clearVFSPendingEvents(): void {
    this.vfsPendingEvents = [];
  }

  /**
   * Clear fallback events
   */
  clearFallbackEvents(): void {
    this.fallbackEvents = [];
  }

  /**
   * Remove synced events from VFS pending
   */
  removeVFSSyncedEvents(count: number): void {
    this.vfsPendingEvents = this.vfsPendingEvents.slice(count);
  }

  /**
   * Check if buffer needs flush due to size
   */
  needsFlushBySize(): boolean {
    const totalEvents = this.vfsPendingEvents.length + this.fallbackEvents.length;
    return totalEvents >= this.config.maxBufferSize;
  }

  /**
   * Check if buffer needs flush due to age
   */
  needsFlushByAge(): boolean {
    const age = Date.now() - this.bufferCreatedAt;
    return age >= this.config.maxBufferAgeMs;
  }

  /**
   * Check if buffer needs flush
   */
  needsFlush(): boolean {
    return this.needsFlushBySize() || this.needsFlushByAge();
  }

  /**
   * Reset buffer timestamp after flush
   */
  resetBufferAge(): void {
    this.bufferCreatedAt = Date.now();
  }

  /**
   * Get buffer statistics
   */
  getStats(): {
    vfsPendingCount: number;
    fallbackCount: number;
    p0WriteOrderCount: number;
    backgroundWritesPending: number;
    bufferAgeMs: number;
  } {
    return {
      vfsPendingCount: this.vfsPendingEvents.length,
      fallbackCount: this.fallbackEvents.length,
      p0WriteOrderCount: this.p0WriteOrder.length,
      backgroundWritesPending: this.backgroundWritesPending,
      bufferAgeMs: Date.now() - this.bufferCreatedAt,
    };
  }
}
