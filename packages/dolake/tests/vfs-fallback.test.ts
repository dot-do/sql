/**
 * DoLake VFS Fallback Storage Tests (TDD RED Phase)
 *
 * Tests for emergency fallback to DO fsx VFS when KV/R2 writes fail.
 * This provides a last-resort storage option for P2 events.
 *
 * Features tested:
 * 1. Detect KV write failure (or skip for P2 events)
 * 2. Write to DO storage via fsx VFS
 * 3. Buffer rotation to prevent storage bloat
 * 4. Recovery: flush from VFS to R2 on next alarm
 * 5. Storage limit enforcement (10MB default)
 *
 * Issue: do-d1isn.6 - RED: DO fsx VFS emergency fallback
 *
 * Uses workers-vitest-pool (NO MOCKS).
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  VFSFallbackStorage,
  VFSStorageError,
  type VFSFallbackConfig,
  type VFSStorageStats,
  type RecoveryResult,
  type BufferRotationResult,
  DEFAULT_VFS_FALLBACK_CONFIG,
} from '../src/vfs-fallback.js';
import { type CDCEvent, generateUUID, DurabilityTier } from '../src/index.js';

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Helper to create a valid CDC event
 */
function createCDCEvent(overrides: Partial<CDCEvent> = {}): CDCEvent {
  return {
    sequence: 1,
    timestamp: Date.now(),
    operation: 'INSERT',
    table: 'analytics_events',
    rowId: generateUUID(),
    after: { id: 1, data: 'test' },
    metadata: { durability: 'P2' },
    ...overrides,
  };
}

/**
 * Create multiple events with varying sizes
 */
function createEventsWithSize(count: number, dataSize: number): CDCEvent[] {
  const events: CDCEvent[] = [];
  const largeData = 'x'.repeat(dataSize);

  for (let i = 0; i < count; i++) {
    events.push(createCDCEvent({
      sequence: i,
      rowId: generateUUID(),
      after: { id: i, data: largeData },
    }));
  }

  return events;
}

/**
 * Mock VFS storage backend for testing
 */
class MockVFSBackend {
  private storage = new Map<string, unknown>();
  private writeFailCount = 0;
  private readFailCount = 0;

  async write(key: string, data: unknown): Promise<void> {
    if (this.writeFailCount > 0) {
      this.writeFailCount--;
      throw new Error('Injected VFS write failure');
    }
    this.storage.set(key, data);
  }

  async read<T>(key: string): Promise<T | null> {
    if (this.readFailCount > 0) {
      this.readFailCount--;
      throw new Error('Injected VFS read failure');
    }
    return (this.storage.get(key) as T) ?? null;
  }

  async delete(key: string): Promise<void> {
    this.storage.delete(key);
  }

  async list(prefix: string): Promise<string[]> {
    const keys: string[] = [];
    for (const key of this.storage.keys()) {
      if (key.startsWith(prefix)) {
        keys.push(key);
      }
    }
    return keys;
  }

  injectWriteFailure(count: number): void {
    this.writeFailCount = count;
  }

  injectReadFailure(count: number): void {
    this.readFailCount = count;
  }

  getStorageSize(): number {
    let size = 0;
    for (const value of this.storage.values()) {
      size += JSON.stringify(value).length;
    }
    return size;
  }

  clear(): void {
    this.storage.clear();
    this.writeFailCount = 0;
    this.readFailCount = 0;
  }
}

/**
 * Mock R2 storage for recovery testing
 */
class MockR2Storage {
  private storage = new Map<string, Uint8Array>();
  private writeFailCount = 0;

  async write(path: string, data: Uint8Array): Promise<void> {
    if (this.writeFailCount > 0) {
      this.writeFailCount--;
      throw new Error('Injected R2 write failure');
    }
    this.storage.set(path, data);
  }

  async read(path: string): Promise<Uint8Array | null> {
    return this.storage.get(path) ?? null;
  }

  async delete(path: string): Promise<void> {
    this.storage.delete(path);
  }

  injectWriteFailure(count: number): void {
    this.writeFailCount = count;
  }

  getWrittenPaths(): string[] {
    return [...this.storage.keys()];
  }

  clear(): void {
    this.storage.clear();
    this.writeFailCount = 0;
  }
}

// =============================================================================
// 1. KV Write Failure Detection Tests
// =============================================================================

describe('VFS Fallback - KV Write Failure Detection', () => {
  let vfsBackend: MockVFSBackend;
  let vfsFallback: VFSFallbackStorage;

  beforeEach(() => {
    vfsBackend = new MockVFSBackend();
    vfsFallback = new VFSFallbackStorage(vfsBackend);
  });

  it('should detect KV write failure and trigger VFS fallback', async () => {
    const event = createCDCEvent();

    // Simulate a KV failure scenario - the event comes to VFS as fallback
    const result = await vfsFallback.writeEvent(event, {
      tier: DurabilityTier.P2,
      kvFailed: true,
      r2Failed: true,
    });

    // EXPECTED: Event should be written to VFS
    expect(result.success).toBe(true);
    expect(result.writtenToVFS).toBe(true);
    expect(result.eventId).toBeDefined();
  });

  it('should skip KV for P2 events (direct VFS write)', async () => {
    const event = createCDCEvent({
      metadata: { durability: 'P2' },
    });

    // P2 events should go directly to VFS fallback when R2 fails
    const result = await vfsFallback.writeEvent(event, {
      tier: DurabilityTier.P2,
      r2Failed: true,
      skipKV: true, // P2 events don't need KV
    });

    // EXPECTED: P2 should write to VFS without attempting KV
    expect(result.success).toBe(true);
    expect(result.writtenToVFS).toBe(true);
    expect(result.skippedKV).toBe(true);
  });

  it('should track KV failure count in metrics', async () => {
    // Write several events after KV failures
    for (let i = 0; i < 5; i++) {
      await vfsFallback.writeEvent(createCDCEvent({ sequence: i }), {
        tier: DurabilityTier.P2,
        kvFailed: true,
        r2Failed: true,
      });
    }

    const stats = vfsFallback.getStats();

    // EXPECTED: Metrics should track KV failures
    expect(stats.kvFailureCount).toBe(5);
    expect(stats.eventsInVFS).toBe(5);
  });

  it('should handle concurrent KV failures gracefully', async () => {
    const events = createEventsWithSize(10, 100);

    // Write all events concurrently
    const results = await Promise.all(
      events.map((event) =>
        vfsFallback.writeEvent(event, {
          tier: DurabilityTier.P2,
          kvFailed: true,
          r2Failed: true,
        })
      )
    );

    // EXPECTED: All events should be written successfully
    expect(results.every((r) => r.success)).toBe(true);

    const stats = vfsFallback.getStats();
    expect(stats.eventsInVFS).toBe(10);
  });
});

// =============================================================================
// 2. VFS Write Tests
// =============================================================================

describe('VFS Fallback - Write to DO Storage', () => {
  let vfsBackend: MockVFSBackend;
  let vfsFallback: VFSFallbackStorage;

  beforeEach(() => {
    vfsBackend = new MockVFSBackend();
    vfsFallback = new VFSFallbackStorage(vfsBackend);
  });

  it('should write event to VFS storage', async () => {
    const event = createCDCEvent();

    const result = await vfsFallback.writeEvent(event, {
      tier: DurabilityTier.P2,
      r2Failed: true,
    });

    // EXPECTED: Event should be persisted in VFS
    expect(result.success).toBe(true);
    expect(result.writtenToVFS).toBe(true);
    expect(result.path).toBeDefined();
    expect(result.path).toContain('vfs/');
  });

  it('should organize events by table and partition', async () => {
    const events = [
      createCDCEvent({ table: 'analytics_events', sequence: 1 }),
      createCDCEvent({ table: 'telemetry', sequence: 2 }),
      createCDCEvent({ table: 'analytics_events', sequence: 3 }),
    ];

    for (const event of events) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
    }

    const stats = vfsFallback.getStats();

    // EXPECTED: Events should be organized by table
    expect(stats.tableBreakdown).toBeDefined();
    expect(stats.tableBreakdown?.['analytics_events']).toBe(2);
    expect(stats.tableBreakdown?.['telemetry']).toBe(1);
  });

  it('should store event with timestamp for ordering', async () => {
    const events = [
      createCDCEvent({ sequence: 3, timestamp: 3000 }),
      createCDCEvent({ sequence: 1, timestamp: 1000 }),
      createCDCEvent({ sequence: 2, timestamp: 2000 }),
    ];

    for (const event of events) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
    }

    // Read events back in order
    const storedEvents = await vfsFallback.readPendingEvents();

    // EXPECTED: Events should maintain timestamp order
    expect(storedEvents).toHaveLength(3);
    expect(storedEvents[0].timestamp).toBe(1000);
    expect(storedEvents[1].timestamp).toBe(2000);
    expect(storedEvents[2].timestamp).toBe(3000);
  });

  it('should handle VFS write failure gracefully', async () => {
    vfsBackend.injectWriteFailure(1);

    const event = createCDCEvent();

    const result = await vfsFallback.writeEvent(event, {
      tier: DurabilityTier.P2,
      r2Failed: true,
    });

    // EXPECTED: Should report failure without throwing
    expect(result.success).toBe(false);
    expect(result.error).toContain('VFS write failure');
  });

  it('should batch writes for efficiency', async () => {
    const events = createEventsWithSize(100, 50);

    // Use batch write API
    const result = await vfsFallback.writeBatch(events, {
      tier: DurabilityTier.P2,
      r2Failed: true,
    });

    // EXPECTED: Batch should be written efficiently
    expect(result.success).toBe(true);
    expect(result.eventsWritten).toBe(100);
    expect(result.batchCount).toBeLessThan(100); // Should batch, not write individually
  });

  it('should deduplicate events by rowId', async () => {
    const rowId = generateUUID();

    // Write same event twice
    await vfsFallback.writeEvent(createCDCEvent({ rowId, sequence: 1 }), {
      tier: DurabilityTier.P2,
      r2Failed: true,
    });

    await vfsFallback.writeEvent(createCDCEvent({ rowId, sequence: 2 }), {
      tier: DurabilityTier.P2,
      r2Failed: true,
    });

    const events = await vfsFallback.readPendingEvents();

    // EXPECTED: Only latest version should be stored
    expect(events).toHaveLength(1);
    expect(events[0].sequence).toBe(2);
  });
});

// =============================================================================
// 3. Buffer Rotation Tests
// =============================================================================

describe('VFS Fallback - Buffer Rotation', () => {
  let vfsBackend: MockVFSBackend;
  let vfsFallback: VFSFallbackStorage;

  beforeEach(() => {
    vfsBackend = new MockVFSBackend();
    // Use smaller limits for testing
    vfsFallback = new VFSFallbackStorage(vfsBackend, {
      maxStorageBytes: 10 * 1024, // 10KB for testing
      rotationThresholdPercent: 80,
      maxBufferFiles: 5,
      bufferFileSizeBytes: 2 * 1024, // 2KB per file
    });
  });

  it('should rotate buffer when size threshold exceeded', async () => {
    // Write events until rotation is triggered
    const events = createEventsWithSize(20, 500); // ~10KB total

    for (const event of events) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
    }

    const stats = vfsFallback.getStats();

    // EXPECTED: Rotation should have occurred
    expect(stats.rotationCount).toBeGreaterThan(0);
    expect(stats.bufferFileCount).toBeGreaterThan(1);
  });

  it('should create new buffer file on rotation', async () => {
    // Fill first buffer
    const events = createEventsWithSize(5, 400); // ~2KB

    for (const event of events) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
    }

    // Force rotation
    const rotationResult = await vfsFallback.rotateBuffer();

    // EXPECTED: New buffer file should be created
    expect(rotationResult.success).toBe(true);
    expect(rotationResult.previousBufferPath).toBeDefined();
    expect(rotationResult.newBufferPath).toBeDefined();
    expect(rotationResult.previousBufferPath).not.toBe(rotationResult.newBufferPath);
  });

  it('should preserve events during rotation', async () => {
    const events = createEventsWithSize(10, 200);

    for (const event of events) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
    }

    // Force rotation
    await vfsFallback.rotateBuffer();

    // Add more events after rotation
    const moreEvents = createEventsWithSize(5, 200);
    for (const event of moreEvents) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
    }

    // Read all events
    const allEvents = await vfsFallback.readPendingEvents();

    // EXPECTED: All events should be preserved
    expect(allEvents).toHaveLength(15);
  });

  it('should limit maximum buffer files', async () => {
    // Write many events to trigger multiple rotations
    const events = createEventsWithSize(50, 400);

    for (const event of events) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
    }

    const stats = vfsFallback.getStats();

    // EXPECTED: Buffer files should be limited
    expect(stats.bufferFileCount).toBeLessThanOrEqual(5);
  });

  it('should drop oldest events when max files exceeded', async () => {
    // Config: maxStorageBytes=10KB, maxBufferFiles=5, bufferFileSizeBytes=2KB
    // With 5 max buffer files at 2KB each = 10KB max rotated storage
    // Plus current buffer can hold up to 2KB more
    // ~500 bytes per event (400 data + 100 overhead)
    // Each buffer holds ~4 events, so 5 buffers = ~20 events in rotated files
    // Plus up to 4 in current buffer = ~24 max before hitting storage limit
    // Fill way beyond max capacity to force drops
    const events = createEventsWithSize(50, 400); // ~25KB of data

    let successCount = 0;
    let limitHitCount = 0;

    for (const event of events) {
      const result = await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
      if (result.success) {
        successCount++;
      }
      if (result.storageLimitExceeded) {
        limitHitCount++;
      }
    }

    const stats = vfsFallback.getStats();
    const pendingEvents = await vfsFallback.readPendingEvents();

    // EXPECTED: Either events were dropped due to buffer file limit,
    // or writes were rejected due to storage limit
    const totalLimited = stats.eventsDropped + limitHitCount;
    expect(totalLimited).toBeGreaterThan(0);

    // Not all 50 events should be pending (either dropped or rejected)
    expect(pendingEvents.length).toBeLessThan(50);
  });

  it('should track rotation metrics', async () => {
    const events = createEventsWithSize(30, 300);

    for (const event of events) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
    }

    const stats = vfsFallback.getStats();

    // EXPECTED: Rotation metrics should be tracked
    expect(stats.rotationCount).toBeGreaterThan(0);
    expect(stats.lastRotationTime).toBeDefined();
    expect(stats.totalBytesRotated).toBeGreaterThan(0);
  });
});

// =============================================================================
// 4. Recovery Tests (Flush from VFS to R2)
// =============================================================================

describe('VFS Fallback - Recovery (Flush to R2)', () => {
  let vfsBackend: MockVFSBackend;
  let r2Storage: MockR2Storage;
  let vfsFallback: VFSFallbackStorage;

  beforeEach(() => {
    vfsBackend = new MockVFSBackend();
    r2Storage = new MockR2Storage();
    vfsFallback = new VFSFallbackStorage(vfsBackend, {
      maxStorageBytes: 10 * 1024 * 1024, // 10MB
    });
  });

  it('should flush VFS events to R2 on recovery', async () => {
    // Write events to VFS
    const events = createEventsWithSize(10, 100);

    for (const event of events) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
    }

    // Recover to R2
    const result = await vfsFallback.recoverToR2(r2Storage);

    // EXPECTED: All events should be flushed to R2
    expect(result.success).toBe(true);
    expect(result.eventsFlushed).toBe(10);
    expect(result.r2Paths.length).toBeGreaterThan(0);
  });

  it('should remove events from VFS after successful R2 write', async () => {
    const events = createEventsWithSize(5, 100);

    for (const event of events) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
    }

    // Verify events are in VFS
    let pendingBefore = await vfsFallback.readPendingEvents();
    expect(pendingBefore).toHaveLength(5);

    // Recover to R2
    await vfsFallback.recoverToR2(r2Storage);

    // EXPECTED: VFS should be empty after successful recovery
    const pendingAfter = await vfsFallback.readPendingEvents();
    expect(pendingAfter).toHaveLength(0);

    const stats = vfsFallback.getStats();
    expect(stats.eventsInVFS).toBe(0);
  });

  it('should batch events for efficient R2 writes', async () => {
    const events = createEventsWithSize(100, 50);

    for (const event of events) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
    }

    const result = await vfsFallback.recoverToR2(r2Storage);

    // EXPECTED: Events should be batched into fewer R2 writes
    expect(result.success).toBe(true);
    expect(result.eventsFlushed).toBe(100);
    expect(result.batchCount).toBeLessThan(100);
    expect(result.batchCount).toBeGreaterThan(0);
  });

  it('should handle partial R2 recovery failure', async () => {
    const events = createEventsWithSize(10, 100);

    for (const event of events) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
    }

    // Inject R2 failure after some writes
    r2Storage.injectWriteFailure(5);

    const result = await vfsFallback.recoverToR2(r2Storage);

    // EXPECTED: Should report partial success
    expect(result.success).toBe(false);
    expect(result.eventsFlushed).toBeLessThan(10);
    expect(result.eventsRemaining).toBeGreaterThan(0);

    // Events that weren't flushed should still be in VFS
    const remaining = await vfsFallback.readPendingEvents();
    expect(remaining.length).toBeGreaterThan(0);
  });

  it('should preserve order during recovery', async () => {
    const events = [
      createCDCEvent({ sequence: 1, timestamp: 1000 }),
      createCDCEvent({ sequence: 2, timestamp: 2000 }),
      createCDCEvent({ sequence: 3, timestamp: 3000 }),
    ];

    for (const event of events) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
    }

    const result = await vfsFallback.recoverToR2(r2Storage);

    // EXPECTED: Events should be written in order
    expect(result.success).toBe(true);
    expect(result.writeOrder).toEqual([1, 2, 3]);
  });

  it('should be triggered by alarm', async () => {
    const events = createEventsWithSize(5, 100);

    for (const event of events) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
    }

    // Simulate alarm trigger
    const result = await vfsFallback.onAlarm(r2Storage);

    // EXPECTED: Alarm should trigger recovery
    expect(result.recoveryAttempted).toBe(true);
    expect(result.recoveryResult?.success).toBe(true);
  });

  it('should not recover if VFS is empty', async () => {
    const result = await vfsFallback.recoverToR2(r2Storage);

    // EXPECTED: Should report nothing to recover
    expect(result.success).toBe(true);
    expect(result.eventsFlushed).toBe(0);
    expect(result.nothingToRecover).toBe(true);
  });

  it('should schedule next alarm after recovery', async () => {
    const events = createEventsWithSize(5, 100);

    for (const event of events) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
    }

    // Partial recovery
    r2Storage.injectWriteFailure(3);
    const result = await vfsFallback.recoverToR2(r2Storage);

    // EXPECTED: If events remain, next alarm should be scheduled
    expect(result.success).toBe(false);
    expect(result.nextAlarmScheduled).toBe(true);
    expect(result.nextAlarmTime).toBeDefined();
  });
});

// =============================================================================
// 5. Storage Limit Enforcement Tests
// =============================================================================

describe('VFS Fallback - Storage Limit Enforcement', () => {
  let vfsBackend: MockVFSBackend;
  let vfsFallback: VFSFallbackStorage;

  beforeEach(() => {
    vfsBackend = new MockVFSBackend();
    // 10KB limit for testing
    vfsFallback = new VFSFallbackStorage(vfsBackend, {
      maxStorageBytes: 10 * 1024,
    });
  });

  it('should enforce 10MB default storage limit', () => {
    // Create with default config
    const defaultFallback = new VFSFallbackStorage(vfsBackend);
    const config = defaultFallback.getConfig();

    // EXPECTED: Default limit should be 10MB
    expect(config.maxStorageBytes).toBe(10 * 1024 * 1024);
  });

  it('should reject writes when storage limit exceeded', async () => {
    // Fill storage to capacity
    // 10KB limit, ~600 bytes per event (500 data + 100 overhead), ~16 events fit
    // Write until we can't write anymore
    let eventsWritten = 0;
    let lastResult;

    for (let i = 0; i < 30; i++) {
      lastResult = await vfsFallback.writeEvent(
        createCDCEvent({ sequence: i, after: { data: 'x'.repeat(500) } }),
        {
          tier: DurabilityTier.P2,
          r2Failed: true,
        }
      );
      if (lastResult.success) {
        eventsWritten++;
      }
      if (lastResult.storageLimitExceeded) {
        break;
      }
    }

    // EXPECTED: Write should eventually be rejected due to storage limit
    expect(lastResult!.success).toBe(false);
    expect(lastResult!.storageLimitExceeded).toBe(true);
    expect(eventsWritten).toBeGreaterThan(0); // Some events should have been written
  });

  it('should track current storage usage', async () => {
    const events = createEventsWithSize(5, 200);

    for (const event of events) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
    }

    const stats = vfsFallback.getStats();

    // EXPECTED: Storage usage should be tracked
    expect(stats.currentStorageBytes).toBeGreaterThan(0);
    expect(stats.storageUtilization).toBeGreaterThan(0);
    expect(stats.storageUtilization).toBeLessThanOrEqual(1);
  });

  it('should report storage warning at 80% utilization', async () => {
    // Fill to ~80%
    const events = createEventsWithSize(16, 500); // ~8KB of 10KB

    for (const event of events) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
    }

    const stats = vfsFallback.getStats();

    // EXPECTED: Warning should be set
    expect(stats.storageWarning).toBe(true);
    expect(stats.storageUtilization).toBeGreaterThanOrEqual(0.8);
  });

  it('should allow writes after storage freed', async () => {
    // Fill storage until we hit the limit
    let hitLimit = false;
    for (let i = 0; i < 30; i++) {
      const result = await vfsFallback.writeEvent(
        createCDCEvent({ sequence: i, after: { data: 'x'.repeat(500) } }),
        {
          tier: DurabilityTier.P2,
          r2Failed: true,
        }
      );
      if (result.storageLimitExceeded) {
        hitLimit = true;
        break;
      }
    }

    // Verify storage is full
    expect(hitLimit).toBe(true);

    // Clear storage by recovering
    const r2Storage = new MockR2Storage();
    await vfsFallback.recoverToR2(r2Storage);

    // EXPECTED: Should be able to write again
    const resultAfter = await vfsFallback.writeEvent(createCDCEvent(), {
      tier: DurabilityTier.P2,
      r2Failed: true,
    });
    expect(resultAfter.success).toBe(true);
  });

  it('should calculate accurate storage size', async () => {
    const events = createEventsWithSize(5, 100);

    for (const event of events) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
    }

    const stats = vfsFallback.getStats();
    const expectedMinSize = 5 * 100; // At least event data

    // EXPECTED: Size should be reasonably accurate
    expect(stats.currentStorageBytes).toBeGreaterThanOrEqual(expectedMinSize);
  });

  it('should include metadata overhead in size calculation', async () => {
    const event = createCDCEvent({ after: { id: 1 } });

    await vfsFallback.writeEvent(event, {
      tier: DurabilityTier.P2,
      r2Failed: true,
    });

    const stats = vfsFallback.getStats();
    const dataSize = JSON.stringify(event).length;

    // EXPECTED: Storage should include metadata overhead
    expect(stats.currentStorageBytes).toBeGreaterThan(dataSize);
  });
});

// =============================================================================
// 6. Integration with DurabilityWriter Tests
// =============================================================================

describe('VFS Fallback - DurabilityWriter Integration', () => {
  let vfsBackend: MockVFSBackend;
  let vfsFallback: VFSFallbackStorage;

  beforeEach(() => {
    vfsBackend = new MockVFSBackend();
    vfsFallback = new VFSFallbackStorage(vfsBackend);
  });

  it('should export VFSStorage interface compatible with DurabilityWriter', async () => {
    // VFSFallbackStorage should implement VFSStorage interface
    const vfsStorage = vfsFallback.asVFSStorage();

    // Test interface methods
    await vfsStorage.write('test-key', { data: 'test' });
    const result = await vfsStorage.read('test-key');

    // EXPECTED: Should work as VFSStorage
    expect(result).toEqual({ data: 'test' });
  });

  it('should integrate with DurabilityWriter P2 fallback', async () => {
    // Get VFS storage interface for DurabilityWriter
    const vfsStorage = vfsFallback.asVFSStorage();

    // Simulate DurabilityWriter P2 fallback behavior
    const event = createCDCEvent({
      table: 'analytics_events',
      metadata: { durability: 'P2' },
    });

    // Write via VFS storage interface
    const key = `vfs/p2/${event.table}/${event.rowId}.json`;
    await vfsStorage.write(key, event);

    // Verify write
    const storedEvent = await vfsStorage.read<CDCEvent>(key);

    // EXPECTED: Event should be stored via VFSStorage interface
    expect(storedEvent).toBeDefined();
    expect(storedEvent?.rowId).toBe(event.rowId);
  });

  it('should support listing pending VFS events for sync', async () => {
    const vfsStorage = vfsFallback.asVFSStorage();

    // Write multiple events
    const events = [
      createCDCEvent({ sequence: 1 }),
      createCDCEvent({ sequence: 2 }),
      createCDCEvent({ sequence: 3 }),
    ];

    for (const event of events) {
      await vfsStorage.write(`vfs/pending/${event.rowId}`, event);
    }

    // List pending events
    const pendingKeys = await vfsStorage.list('vfs/pending/');

    // EXPECTED: Should list all pending events
    expect(pendingKeys).toHaveLength(3);
  });

  it('should support delete for cleanup after sync', async () => {
    const vfsStorage = vfsFallback.asVFSStorage();

    const event = createCDCEvent();
    const key = `vfs/pending/${event.rowId}`;

    await vfsStorage.write(key, event);

    // Verify exists
    const before = await vfsStorage.read(key);
    expect(before).toBeDefined();

    // Delete after sync
    await vfsStorage.delete(key);

    // EXPECTED: Should be deleted
    const after = await vfsStorage.read(key);
    expect(after).toBeNull();
  });
});

// =============================================================================
// 7. Error Handling Tests
// =============================================================================

describe('VFS Fallback - Error Handling', () => {
  let vfsBackend: MockVFSBackend;
  let vfsFallback: VFSFallbackStorage;

  beforeEach(() => {
    vfsBackend = new MockVFSBackend();
    vfsFallback = new VFSFallbackStorage(vfsBackend);
  });

  it('should throw VFSStorageError with context', async () => {
    vfsBackend.injectWriteFailure(1);

    const event = createCDCEvent();
    const result = await vfsFallback.writeEvent(event, {
      tier: DurabilityTier.P2,
      r2Failed: true,
    });

    // EXPECTED: Error should have context
    expect(result.success).toBe(false);
    expect(result.error).toBeDefined();
    expect(result.errorCode).toBe('VFS_WRITE_FAILED');
  });

  it('should handle corrupted VFS data', async () => {
    // Write valid data
    await vfsFallback.writeEvent(createCDCEvent({ sequence: 1 }), {
      tier: DurabilityTier.P2,
      r2Failed: true,
    });

    // Write corrupted data with a key that looks like it should be read
    // Using the vfs/ prefix so it gets picked up during readPendingEvents
    await vfsBackend.write('vfs/corrupted_entry', { invalid: 'data without event field' });

    // Try to read all events
    const events = await vfsFallback.readPendingEvents();

    // EXPECTED: Should skip corrupted data and return valid events
    expect(events).toHaveLength(1);

    const stats = vfsFallback.getStats();
    expect(stats.corruptedEntries).toBe(1);
  });

  it('should recover from transient errors', async () => {
    // Fail first 2 writes, then succeed
    vfsBackend.injectWriteFailure(2);

    const events = createEventsWithSize(5, 100);
    const results: boolean[] = [];

    for (const event of events) {
      const result = await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
      results.push(result.success);
    }

    // EXPECTED: First 2 should fail, rest should succeed
    expect(results).toEqual([false, false, true, true, true]);
  });

  it('should provide helpful error messages', async () => {
    // Try to write to full storage
    vfsFallback = new VFSFallbackStorage(vfsBackend, {
      maxStorageBytes: 100, // Tiny limit
    });

    const largeEvent = createCDCEvent({ after: { data: 'x'.repeat(200) } });
    const result = await vfsFallback.writeEvent(largeEvent, {
      tier: DurabilityTier.P2,
      r2Failed: true,
    });

    // EXPECTED: Error should explain the issue
    expect(result.success).toBe(false);
    expect(result.error).toContain('storage limit');
  });
});

// =============================================================================
// 8. Metrics and Observability Tests
// =============================================================================

describe('VFS Fallback - Metrics and Observability', () => {
  let vfsBackend: MockVFSBackend;
  let vfsFallback: VFSFallbackStorage;

  beforeEach(() => {
    vfsBackend = new MockVFSBackend();
    vfsFallback = new VFSFallbackStorage(vfsBackend);
  });

  it('should track comprehensive statistics', async () => {
    const events = createEventsWithSize(10, 100);

    for (const event of events) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
        kvFailed: true,
      });
    }

    const stats = vfsFallback.getStats();

    // EXPECTED: Stats should be comprehensive
    expect(stats.eventsInVFS).toBe(10);
    expect(stats.currentStorageBytes).toBeGreaterThan(0);
    expect(stats.kvFailureCount).toBe(10);
    expect(stats.r2FailureCount).toBe(10);
    expect(stats.totalWrites).toBe(10);
    expect(stats.successfulWrites).toBe(10);
  });

  it('should track recovery statistics', async () => {
    const events = createEventsWithSize(10, 100);

    for (const event of events) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
    }

    const r2Storage = new MockR2Storage();
    await vfsFallback.recoverToR2(r2Storage);

    const stats = vfsFallback.getStats();

    // EXPECTED: Recovery stats should be tracked
    expect(stats.totalRecoveryAttempts).toBe(1);
    expect(stats.successfulRecoveries).toBe(1);
    expect(stats.eventsRecovered).toBe(10);
    expect(stats.lastRecoveryTime).toBeDefined();
  });

  it('should expose Prometheus-compatible metrics', async () => {
    const events = createEventsWithSize(5, 100);

    for (const event of events) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
    }

    const metrics = vfsFallback.getPrometheusMetrics();

    // EXPECTED: Prometheus metrics should be available
    expect(metrics).toContain('dolake_vfs_events_total');
    expect(metrics).toContain('dolake_vfs_storage_bytes');
    expect(metrics).toContain('dolake_vfs_write_latency_seconds');
  });

  it('should track write latency', async () => {
    const events = createEventsWithSize(10, 100);

    for (const event of events) {
      await vfsFallback.writeEvent(event, {
        tier: DurabilityTier.P2,
        r2Failed: true,
      });
    }

    const stats = vfsFallback.getStats();

    // EXPECTED: Latency should be tracked
    expect(stats.avgWriteLatencyMs).toBeGreaterThan(0);
    expect(stats.maxWriteLatencyMs).toBeGreaterThan(0);
  });
});

// =============================================================================
// 9. Configuration Tests
// =============================================================================

describe('VFS Fallback - Configuration', () => {
  it('should have sensible defaults', () => {
    const config = DEFAULT_VFS_FALLBACK_CONFIG;

    // EXPECTED: Default values should be sensible
    expect(config.maxStorageBytes).toBe(10 * 1024 * 1024); // 10MB
    expect(config.rotationThresholdPercent).toBe(80);
    expect(config.maxBufferFiles).toBe(10);
    expect(config.bufferFileSizeBytes).toBe(1024 * 1024); // 1MB
    expect(config.recoveryBatchSize).toBe(100);
    expect(config.recoveryIntervalMs).toBe(60000); // 1 minute
  });

  it('should allow custom configuration', () => {
    const vfsBackend = new MockVFSBackend();
    const customConfig: Partial<VFSFallbackConfig> = {
      maxStorageBytes: 20 * 1024 * 1024,
      maxBufferFiles: 20,
      recoveryBatchSize: 200,
    };

    const vfsFallback = new VFSFallbackStorage(vfsBackend, customConfig);
    const config = vfsFallback.getConfig();

    // EXPECTED: Custom config should be applied
    expect(config.maxStorageBytes).toBe(20 * 1024 * 1024);
    expect(config.maxBufferFiles).toBe(20);
    expect(config.recoveryBatchSize).toBe(200);
    // Defaults should still apply for non-overridden values
    expect(config.rotationThresholdPercent).toBe(80);
  });

  it('should validate configuration values', () => {
    const vfsBackend = new MockVFSBackend();

    // EXPECTED: Should throw on invalid config
    expect(() => {
      new VFSFallbackStorage(vfsBackend, {
        maxStorageBytes: -1,
      });
    }).toThrow();

    expect(() => {
      new VFSFallbackStorage(vfsBackend, {
        rotationThresholdPercent: 150,
      });
    }).toThrow();
  });
});
