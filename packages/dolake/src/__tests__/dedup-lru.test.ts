/**
 * Deduplication LRU Eviction Tests (TDD - RED Phase)
 *
 * Tests for LRU (Least Recently Used) eviction in the dedup set.
 * The dedup set should:
 * 1. Evict oldest entries when MAX_DEDUP_ENTRIES is exceeded
 * 2. Maintain LRU order (update access time on checks)
 * 3. Continue dedup functionality correctly after eviction
 * 4. Not evict recently accessed entries
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import { CDCBufferManager, DEFAULT_DEDUP_CONFIG } from '../buffer.js';

describe('Dedup LRU Eviction', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2024-01-01T00:00:00Z'));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe('eviction when exceeding MAX_DEDUP_ENTRIES', () => {
    it('should evict oldest entries when max entries exceeded', () => {
      // Use a small maxEntries for testing
      const maxEntries = 5;
      const manager = new CDCBufferManager({}, { maxEntries, enabled: true });

      // Add entries up to the limit
      for (let i = 0; i < maxEntries; i++) {
        manager.markSeen(`key-${i}`);
        vi.advanceTimersByTime(10); // Small time gap between entries
      }

      // Verify all entries are present
      for (let i = 0; i < maxEntries; i++) {
        expect(manager.isDuplicate(`key-${i}`)).toBe(true);
      }

      // Add one more entry, which should trigger eviction
      manager.markSeen('key-new');

      // The oldest entry (key-0) should be evicted
      expect(manager.isDuplicate('key-0')).toBe(false);

      // The newest entry should exist
      expect(manager.isDuplicate('key-new')).toBe(true);

      // Stats should reflect eviction
      const stats = manager.getDedupStats();
      expect(stats.entriesTracked).toBeLessThanOrEqual(maxEntries);
    });

    it('should evict multiple oldest entries when adding many at once', () => {
      const maxEntries = 5;
      const manager = new CDCBufferManager({}, { maxEntries, enabled: true });

      // Add initial entries
      for (let i = 0; i < maxEntries; i++) {
        manager.markSeen(`old-${i}`);
        vi.advanceTimersByTime(10);
      }

      // Add 3 more entries, should evict 3 oldest
      manager.markSeen('new-1');
      vi.advanceTimersByTime(10);
      manager.markSeen('new-2');
      vi.advanceTimersByTime(10);
      manager.markSeen('new-3');

      // Oldest entries should be evicted
      expect(manager.isDuplicate('old-0')).toBe(false);
      expect(manager.isDuplicate('old-1')).toBe(false);
      expect(manager.isDuplicate('old-2')).toBe(false);

      // Newer entries should remain
      expect(manager.isDuplicate('old-3')).toBe(true);
      expect(manager.isDuplicate('old-4')).toBe(true);
      expect(manager.isDuplicate('new-1')).toBe(true);
      expect(manager.isDuplicate('new-2')).toBe(true);
      expect(manager.isDuplicate('new-3')).toBe(true);

      const stats = manager.getDedupStats();
      expect(stats.entriesTracked).toBeLessThanOrEqual(maxEntries);
    });
  });

  describe('LRU order maintenance', () => {
    it('should update access time when checking existing entries', () => {
      const maxEntries = 5;
      const manager = new CDCBufferManager({}, { maxEntries, enabled: true });

      // Add 5 entries
      for (let i = 0; i < maxEntries; i++) {
        manager.markSeen(`key-${i}`);
        vi.advanceTimersByTime(10);
      }

      // Access key-0 (making it recently used)
      vi.advanceTimersByTime(100);
      expect(manager.isDuplicate('key-0')).toBe(true);

      // Add new entries to trigger eviction
      manager.markSeen('key-new-1');
      vi.advanceTimersByTime(10);
      manager.markSeen('key-new-2');

      // key-0 should NOT be evicted because it was recently accessed
      expect(manager.isDuplicate('key-0')).toBe(true);

      // key-1 and key-2 should be evicted (they were oldest without recent access)
      expect(manager.isDuplicate('key-1')).toBe(false);
      expect(manager.isDuplicate('key-2')).toBe(false);
    });

    it('should preserve entries accessed more recently than insertion order suggests', () => {
      const maxEntries = 4;
      const manager = new CDCBufferManager({}, { maxEntries, enabled: true });

      // Add entries: key-0, key-1, key-2, key-3
      manager.markSeen('key-0');
      vi.advanceTimersByTime(10);
      manager.markSeen('key-1');
      vi.advanceTimersByTime(10);
      manager.markSeen('key-2');
      vi.advanceTimersByTime(10);
      manager.markSeen('key-3');
      vi.advanceTimersByTime(10);

      // Access key-0 and key-1 to make them "recently used"
      expect(manager.isDuplicate('key-0')).toBe(true);
      vi.advanceTimersByTime(10);
      expect(manager.isDuplicate('key-1')).toBe(true);
      vi.advanceTimersByTime(10);

      // Add new entries, which should evict key-2 and key-3 (LRU)
      manager.markSeen('key-new-1');
      vi.advanceTimersByTime(10);
      manager.markSeen('key-new-2');

      // key-0 and key-1 should remain (recently accessed)
      expect(manager.isDuplicate('key-0')).toBe(true);
      expect(manager.isDuplicate('key-1')).toBe(true);

      // key-2 and key-3 should be evicted
      expect(manager.isDuplicate('key-2')).toBe(false);
      expect(manager.isDuplicate('key-3')).toBe(false);

      // New entries should exist
      expect(manager.isDuplicate('key-new-1')).toBe(true);
      expect(manager.isDuplicate('key-new-2')).toBe(true);
    });
  });

  describe('dedup functionality after eviction', () => {
    it('should still correctly detect duplicates after eviction', () => {
      const maxEntries = 3;
      const manager = new CDCBufferManager({}, { maxEntries, enabled: true });

      // Add entries to max
      manager.markSeen('key-1');
      vi.advanceTimersByTime(10);
      manager.markSeen('key-2');
      vi.advanceTimersByTime(10);
      manager.markSeen('key-3');
      vi.advanceTimersByTime(10);

      // Trigger eviction with new entry
      manager.markSeen('key-4');

      // Remaining entries should still be duplicates
      expect(manager.isDuplicate('key-2')).toBe(true);
      expect(manager.isDuplicate('key-3')).toBe(true);
      expect(manager.isDuplicate('key-4')).toBe(true);

      // Evicted entry should not be duplicate
      expect(manager.isDuplicate('key-1')).toBe(false);
    });

    it('should correctly mark evicted keys as seen again', () => {
      const maxEntries = 3;
      const manager = new CDCBufferManager({}, { maxEntries, enabled: true });

      // Add and evict
      manager.markSeen('key-old');
      vi.advanceTimersByTime(10);
      manager.markSeen('key-2');
      vi.advanceTimersByTime(10);
      manager.markSeen('key-3');
      vi.advanceTimersByTime(10);
      manager.markSeen('key-4'); // Evicts key-old

      // key-old was evicted
      expect(manager.isDuplicate('key-old')).toBe(false);

      // Re-add key-old
      manager.markSeen('key-old');

      // Now it should be a duplicate again
      expect(manager.isDuplicate('key-old')).toBe(true);
    });

    it('should maintain accurate duplicate statistics across evictions', () => {
      const maxEntries = 3;
      const manager = new CDCBufferManager({}, { maxEntries, enabled: true });

      // Add entries
      manager.markSeen('key-1');
      manager.markSeen('key-2');
      manager.markSeen('key-3');

      // Check for duplicate
      expect(manager.isDuplicate('key-2')).toBe(true);

      let stats = manager.getDedupStats();
      expect(stats.duplicatesFound).toBe(1);

      // Trigger eviction
      vi.advanceTimersByTime(10);
      manager.markSeen('key-4');

      // Check evicted entry (not a duplicate anymore)
      expect(manager.isDuplicate('key-1')).toBe(false);

      stats = manager.getDedupStats();
      // Duplicate count should not increase for evicted entry
      expect(stats.duplicatesFound).toBe(1);

      // Check remaining entry (still a duplicate)
      expect(manager.isDuplicate('key-2')).toBe(true);

      stats = manager.getDedupStats();
      expect(stats.duplicatesFound).toBe(2);
    });
  });

  describe('recent entries protection', () => {
    it('should not evict entries just added', () => {
      const maxEntries = 3;
      const manager = new CDCBufferManager({}, { maxEntries, enabled: true });

      // Add entries rapidly
      manager.markSeen('key-1');
      manager.markSeen('key-2');
      manager.markSeen('key-3');
      manager.markSeen('key-4'); // Triggers eviction

      // The newest entry should definitely still exist
      expect(manager.isDuplicate('key-4')).toBe(true);

      // At least maxEntries should remain
      const stats = manager.getDedupStats();
      expect(stats.entriesTracked).toBeLessThanOrEqual(maxEntries);
    });

    it('should evict entries in correct LRU order under rapid additions', () => {
      const maxEntries = 5;
      const manager = new CDCBufferManager({}, { maxEntries, enabled: true });

      // Add entries with time gaps
      for (let i = 0; i < 10; i++) {
        manager.markSeen(`key-${i}`);
        vi.advanceTimersByTime(1);
      }

      // Only the 5 most recent entries should remain
      const stats = manager.getDedupStats();
      expect(stats.entriesTracked).toBeLessThanOrEqual(maxEntries);

      // Oldest entries (0-4) should be evicted
      for (let i = 0; i < 5; i++) {
        expect(manager.isDuplicate(`key-${i}`)).toBe(false);
      }

      // Newest entries (5-9) should remain
      for (let i = 5; i < 10; i++) {
        expect(manager.isDuplicate(`key-${i}`)).toBe(true);
      }
    });
  });

  describe('serialization with LRU state', () => {
    it('should preserve LRU access times through serialization', () => {
      const maxEntries = 5;
      const manager = new CDCBufferManager({}, { maxEntries, enabled: true });

      // Add entries with time gaps
      manager.markSeen('key-0');
      vi.advanceTimersByTime(10);
      manager.markSeen('key-1');
      vi.advanceTimersByTime(10);
      manager.markSeen('key-2');
      vi.advanceTimersByTime(10);
      manager.markSeen('key-3');
      vi.advanceTimersByTime(10);
      manager.markSeen('key-4');
      vi.advanceTimersByTime(10);

      // Access key-0 to make it recently used
      expect(manager.isDuplicate('key-0')).toBe(true);

      // Serialize and restore
      const snapshot = manager.serialize();
      const restored = CDCBufferManager.restore(snapshot, {}, { maxEntries, enabled: true });

      // Add new entries to trigger eviction
      vi.advanceTimersByTime(10);
      restored.markSeen('key-new-1');
      vi.advanceTimersByTime(10);
      restored.markSeen('key-new-2');

      // key-0 should still exist (was recently accessed before serialize)
      expect(restored.isDuplicate('key-0')).toBe(true);

      // Older non-accessed entries should be evicted
      expect(restored.isDuplicate('key-1')).toBe(false);
      expect(restored.isDuplicate('key-2')).toBe(false);
    });
  });

  describe('integration with batch deduplication', () => {
    it('should apply LRU eviction to batch dedup keys', () => {
      const maxEntries = 5;
      const manager = new CDCBufferManager({}, { maxEntries, enabled: true });

      // Add batch dedupe entries
      for (let i = 0; i < 10; i++) {
        manager.addBatch(
          'source-1',
          [{ table: 'test', operation: 'INSERT', timestamp: Date.now(), after: { id: i } }],
          i
        );
        vi.advanceTimersByTime(10);
      }

      const stats = manager.getDedupStats();

      // Should not exceed maxEntries
      expect(stats.entriesTracked).toBeLessThanOrEqual(maxEntries);

      // Older batch sequences should be evicted (can be re-added)
      const oldResult = manager.addBatch(
        'source-1',
        [{ table: 'test', operation: 'INSERT', timestamp: Date.now(), after: { id: 100 } }],
        0 // Sequence 0 was evicted
      );
      expect(oldResult.isDuplicate).toBe(false);

      // Recent batch sequences should still be tracked as duplicates
      const recentResult = manager.addBatch(
        'source-1',
        [{ table: 'test', operation: 'INSERT', timestamp: Date.now(), after: { id: 101 } }],
        9 // Sequence 9 should still be tracked
      );
      expect(recentResult.isDuplicate).toBe(true);
    });
  });
});
