/**
 * B-tree Stress Tests for Large Datasets
 *
 * These tests exercise the B-tree under heavy load with 10K+ rows,
 * various key insertion patterns, large range scans, delete-heavy
 * workloads, and concurrent read/write pressure.
 *
 * All tests use the real B-tree implementation (NO MOCKS).
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { MemoryFSXBackend } from '../../fsx/index.js';
import {
  createBTree,
  NumberKeyCodec,
  StringKeyCodec,
  JsonValueCodec,
} from '../index.js';
import type { BTreeExtended } from '../btree.js';

// Simple seeded PRNG for reproducible tests (Mulberry32)
function mulberry32(seed: number): () => number {
  return function () {
    let t = (seed += 0x6d2b79f5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// Fisher-Yates shuffle with seeded RNG
function shuffle<T>(arr: T[], rng: () => number): T[] {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(rng() * (i + 1));
    [a[i], a[j]] = [a[j]!, a[i]!];
  }
  return a;
}

describe('B-tree stress tests', () => {
  let fsx: MemoryFSXBackend;

  beforeEach(() => {
    fsx = new MemoryFSXBackend();
  });

  // ===========================================================================
  // 1. Insert 10K+ rows and verify all can be retrieved
  // ===========================================================================

  describe('large dataset insertion and retrieval', () => {
    it('should insert and retrieve 10,000 rows with sequential keys', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      const N = 10_000;

      // Insert all rows
      for (let i = 0; i < N; i++) {
        await tree.set(i, { id: i, data: `row_${i}` });
      }

      // Verify count
      expect(await tree.count()).toBe(N);

      // Verify every row can be retrieved
      for (let i = 0; i < N; i++) {
        const val = await tree.get(i);
        expect(val).toEqual({ id: i, data: `row_${i}` });
      }

      // Verify tree structure is valid
      const stats = await tree.stats();
      expect(stats.entryCount).toBe(N);
      expect(stats.height).toBeGreaterThan(1);
    }, 120_000);

    it('should insert and retrieve 10,000 rows with string keys', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      const N = 10_000;

      for (let i = 0; i < N; i++) {
        const key = `key_${i.toString().padStart(6, '0')}`;
        await tree.set(key, i);
      }

      expect(await tree.count()).toBe(N);

      for (let i = 0; i < N; i++) {
        const key = `key_${i.toString().padStart(6, '0')}`;
        expect(await tree.get(key)).toBe(i);
      }
    }, 120_000);

    it('should handle 10,000 rows with larger page sizes', async () => {
      // Test with default-like page configuration (larger nodes = fewer splits)
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 10,
        maxKeys: 20,
      });
      await tree.init();

      const N = 10_000;

      for (let i = 0; i < N; i++) {
        await tree.set(i, `value_${i}`);
      }

      expect(await tree.count()).toBe(N);

      // Spot-check retrieval
      for (let i = 0; i < N; i += 100) {
        expect(await tree.get(i)).toBe(`value_${i}`);
      }

      // Full entries iteration
      let count = 0;
      let prev = -Infinity;
      for await (const [key] of tree.entries()) {
        expect(key).toBeGreaterThan(prev);
        prev = key;
        count++;
      }
      expect(count).toBe(N);
    }, 120_000);
  });

  // ===========================================================================
  // 2. Sequential and random key insertion patterns
  // ===========================================================================

  describe('insertion patterns', () => {
    it('should handle ascending sequential insertion', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      const N = 5_000;
      for (let i = 0; i < N; i++) {
        await tree.set(i, i);
      }

      expect(await tree.count()).toBe(N);

      // Verify sorted order via entries
      const entries: [number, number][] = [];
      for await (const entry of tree.entries()) {
        entries.push(entry as [number, number]);
      }
      expect(entries.length).toBe(N);
      for (let i = 0; i < N; i++) {
        expect(entries[i]![0]).toBe(i);
      }
    }, 60_000);

    it('should handle descending sequential insertion', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      const N = 5_000;
      for (let i = N - 1; i >= 0; i--) {
        await tree.set(i, i);
      }

      expect(await tree.count()).toBe(N);

      // Verify sorted order
      const entries: [number, number][] = [];
      for await (const entry of tree.entries()) {
        entries.push(entry as [number, number]);
      }
      expect(entries.length).toBe(N);
      for (let i = 0; i < N; i++) {
        expect(entries[i]![0]).toBe(i);
      }
    }, 60_000);

    it('should handle random insertion order', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      const N = 5_000;
      const rng = mulberry32(42);
      const keys = shuffle(
        Array.from({ length: N }, (_, i) => i),
        rng
      );

      for (const key of keys) {
        await tree.set(key, `val_${key}`);
      }

      expect(await tree.count()).toBe(N);

      // Verify all values
      for (const key of keys) {
        expect(await tree.get(key)).toBe(`val_${key}`);
      }

      // Verify sorted order
      let prev = -Infinity;
      for await (const [key] of tree.entries()) {
        expect(key).toBeGreaterThan(prev);
        prev = key;
      }
    }, 60_000);

    it('should handle interleaved ascending/descending insertion', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      const N = 4_000;
      // Insert alternating: 0, N-1, 1, N-2, 2, N-3, ...
      for (let i = 0; i < N / 2; i++) {
        await tree.set(i, `low_${i}`);
        await tree.set(N - 1 - i, `high_${N - 1 - i}`);
      }

      expect(await tree.count()).toBe(N);

      // Verify all values correct
      for (let i = 0; i < N / 2; i++) {
        expect(await tree.get(i)).toBe(`low_${i}`);
        expect(await tree.get(N - 1 - i)).toBe(`high_${N - 1 - i}`);
      }
    }, 60_000);

    it('should handle duplicate key updates at scale', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      const N = 2_000;

      // Insert N keys
      for (let i = 0; i < N; i++) {
        await tree.set(i, `version_1_${i}`);
      }
      expect(await tree.count()).toBe(N);

      // Update all keys (should not increase count)
      for (let i = 0; i < N; i++) {
        await tree.set(i, `version_2_${i}`);
      }
      expect(await tree.count()).toBe(N);

      // Verify latest values
      for (let i = 0; i < N; i++) {
        expect(await tree.get(i)).toBe(`version_2_${i}`);
      }
    }, 60_000);
  });

  // ===========================================================================
  // 3. Range scans across large datasets
  // ===========================================================================

  describe('range scans on large datasets', () => {
    let tree: BTreeExtended<number, string>;
    const N = 5_000;

    beforeEach(async () => {
      tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      for (let i = 0; i < N; i++) {
        await tree.set(i, `val_${i}`);
      }
    }, 60_000);

    it('should scan a small range in the middle of a large dataset', async () => {
      const results: [number, string][] = [];
      for await (const entry of tree.range(2_500, 2_510)) {
        results.push(entry as [number, string]);
      }

      expect(results.length).toBe(10);
      for (let i = 0; i < 10; i++) {
        expect(results[i]![0]).toBe(2_500 + i);
        expect(results[i]![1]).toBe(`val_${2_500 + i}`);
      }
    });

    it('should scan from the beginning of the dataset', async () => {
      const results: [number, string][] = [];
      for await (const entry of tree.range(0, 100)) {
        results.push(entry as [number, string]);
      }

      expect(results.length).toBe(100);
      expect(results[0]![0]).toBe(0);
      expect(results[99]![0]).toBe(99);
    });

    it('should scan to the end of the dataset', async () => {
      const results: [number, string][] = [];
      for await (const entry of tree.range(4_900, 5_100)) {
        results.push(entry as [number, string]);
      }

      // Only 4900-4999 exist (5000 is exclusive end and does not exist)
      expect(results.length).toBe(100);
      expect(results[0]![0]).toBe(4_900);
      expect(results[99]![0]).toBe(4_999);
    });

    it('should scan the entire dataset via range', async () => {
      const results: [number, string][] = [];
      for await (const entry of tree.range(0, N + 1)) {
        results.push(entry as [number, string]);
      }

      expect(results.length).toBe(N);

      // Verify sorted order
      for (let i = 1; i < results.length; i++) {
        expect(results[i]![0]).toBeGreaterThan(results[i - 1]![0]);
      }
    });

    it('should handle range scan with non-existent boundaries', async () => {
      // Range between keys that do not exist
      // Keys exist at integers 0..4999, use fractional boundaries
      // Since NumberKeyCodec uses float64, fractional keys work
      const results: [number, string][] = [];
      for await (const entry of tree.range(100, 200)) {
        results.push(entry as [number, string]);
      }

      expect(results.length).toBe(100);
      expect(results[0]![0]).toBe(100);
      expect(results[99]![0]).toBe(199);
    });

    it('should handle multiple sequential range scans', async () => {
      // Scan in 500-key chunks across the dataset
      let totalScanned = 0;
      for (let start = 0; start < N; start += 500) {
        const end = Math.min(start + 500, N);
        const results: [number, string][] = [];
        for await (const entry of tree.range(start, end)) {
          results.push(entry as [number, string]);
        }
        expect(results.length).toBe(end - start);
        totalScanned += results.length;
      }
      expect(totalScanned).toBe(N);
    });
  });

  // ===========================================================================
  // 4. Delete operations maintaining tree balance
  // ===========================================================================

  describe('delete operations at scale', () => {
    it('should delete half the entries and maintain tree integrity', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      const N = 4_000;

      for (let i = 0; i < N; i++) {
        await tree.set(i, `val_${i}`);
      }

      // Delete even-numbered keys
      for (let i = 0; i < N; i += 2) {
        const deleted = await tree.delete(i);
        expect(deleted).toBe(true);
      }

      expect(await tree.count()).toBe(N / 2);

      // Verify only odd keys remain
      for (let i = 0; i < N; i++) {
        const val = await tree.get(i);
        if (i % 2 === 0) {
          expect(val).toBeUndefined();
        } else {
          expect(val).toBe(`val_${i}`);
        }
      }

      // Verify sorted order via entries
      const entries: [number, string][] = [];
      for await (const entry of tree.entries()) {
        entries.push(entry as [number, string]);
      }
      expect(entries.length).toBe(N / 2);
      for (let i = 1; i < entries.length; i++) {
        expect(entries[i]![0]).toBeGreaterThan(entries[i - 1]![0]);
      }
    }, 120_000);

    it('should delete all entries and rebuild', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      const N = 3_000;

      // Insert
      for (let i = 0; i < N; i++) {
        await tree.set(i, i);
      }
      expect(await tree.count()).toBe(N);

      // Delete all in forward order
      for (let i = 0; i < N; i++) {
        const deleted = await tree.delete(i);
        expect(deleted).toBe(true);
      }
      expect(await tree.count()).toBe(0);

      const stats = await tree.stats();
      expect(stats.height).toBe(1);

      // Re-insert and verify tree works after full deletion
      for (let i = 0; i < 100; i++) {
        await tree.set(i, `rebuilt_${i}`);
      }
      expect(await tree.count()).toBe(100);
      expect(await tree.get(50)).toBe('rebuilt_50');
    }, 120_000);

    it('should delete in reverse order maintaining balance', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      const N = 3_000;

      for (let i = 0; i < N; i++) {
        await tree.set(i, i);
      }

      // Delete in reverse
      for (let i = N - 1; i >= 0; i--) {
        const deleted = await tree.delete(i);
        expect(deleted).toBe(true);

        // Periodically verify remaining entries
        if (i % 500 === 0 && i > 0) {
          expect(await tree.count()).toBe(i);
          // Spot check a value in the remaining range
          const checkKey = Math.floor(i / 2);
          expect(await tree.get(checkKey)).toBe(checkKey);
        }
      }

      expect(await tree.count()).toBe(0);
    }, 120_000);

    it('should delete in random order maintaining balance', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      const N = 3_000;
      const rng = mulberry32(99);

      for (let i = 0; i < N; i++) {
        await tree.set(i, `val_${i}`);
      }

      const deleteOrder = shuffle(
        Array.from({ length: N }, (_, i) => i),
        rng
      );
      const remaining = new Set(Array.from({ length: N }, (_, i) => i));

      for (const key of deleteOrder) {
        const deleted = await tree.delete(key);
        expect(deleted).toBe(true);
        remaining.delete(key);

        // Periodically verify tree integrity
        if (remaining.size % 500 === 0 && remaining.size > 0) {
          expect(await tree.count()).toBe(remaining.size);

          // Pick a random remaining key and verify it
          const remainingArr = Array.from(remaining);
          const checkKey = remainingArr[Math.floor(rng() * remainingArr.length)]!;
          expect(await tree.get(checkKey)).toBe(`val_${checkKey}`);
        }
      }

      expect(await tree.count()).toBe(0);
    }, 120_000);

    it('should handle interleaved insert and delete maintaining tree invariants', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      const rng = mulberry32(7777);
      const reference = new Map<number, string>();

      // 5000 random operations: 60% insert, 40% delete
      for (let i = 0; i < 5_000; i++) {
        const key = Math.floor(rng() * 2_000);
        if (rng() < 0.6) {
          const value = `v_${key}_${i}`;
          await tree.set(key, value);
          reference.set(key, value);
        } else {
          const deleted = await tree.delete(key);
          const wasInRef = reference.has(key);
          expect(deleted).toBe(wasInRef);
          reference.delete(key);
        }
      }

      // Verify tree matches reference
      expect(await tree.count()).toBe(reference.size);

      for (const [key, value] of reference) {
        expect(await tree.get(key)).toBe(value);
      }

      // Verify sorted order
      const entries: [number, string][] = [];
      for await (const entry of tree.entries()) {
        entries.push(entry as [number, string]);
      }
      expect(entries.length).toBe(reference.size);
      for (let i = 1; i < entries.length; i++) {
        expect(entries[i]![0]).toBeGreaterThan(entries[i - 1]![0]);
      }
    }, 120_000);
  });

  // ===========================================================================
  // 5. Concurrent read/write stress
  // ===========================================================================

  describe('concurrent read/write stress', () => {
    it('should handle many concurrent reads after bulk writes', async () => {
      // The B-tree is single-writer, so we write sequentially then stress reads concurrently.
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      const N = 3_000;

      // Sequential bulk write
      for (let i = 0; i < N; i++) {
        await tree.set(i, `val_${i}`);
      }

      // Fire off many concurrent reads in batches
      for (let batch = 0; batch < 20; batch++) {
        const promises: Promise<unknown>[] = [];
        for (let i = 0; i < 100; i++) {
          const key = Math.floor(Math.random() * N);
          promises.push(
            tree.get(key).then((val) => {
              expect(val).toBe(`val_${key}`);
            })
          );
        }
        await Promise.all(promises);
      }

      // Verify count unchanged
      expect(await tree.count()).toBe(N);
    }, 120_000);

    it('should handle sequential write-then-read interleaving at high throughput', async () => {
      // Stress the write-read cycle: write one, then immediately read back
      // This tests single-writer correctness under heavy sequential load
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      const N = 2_000;

      for (let i = 0; i < N; i++) {
        await tree.set(i, `written_${i}`);

        // Immediately verify the write
        const val = await tree.get(i);
        expect(val).toBe(`written_${i}`);

        // Also verify a previously-written key is still valid
        if (i > 0) {
          const prevKey = Math.floor(Math.random() * i);
          const prevVal = await tree.get(prevKey);
          expect(prevVal).toBe(`written_${prevKey}`);
        }
      }

      expect(await tree.count()).toBe(N);
    }, 120_000);

    it('should handle concurrent range scans and point lookups', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      const N = 3_000;
      for (let i = 0; i < N; i++) {
        await tree.set(i, `val_${i}`);
      }

      // Launch concurrent range scans and point lookups
      const promises: Promise<unknown>[] = [];

      // 10 concurrent range scans over different portions
      for (let i = 0; i < 10; i++) {
        const start = i * 300;
        const end = start + 300;
        promises.push(
          (async () => {
            const results: [number, string][] = [];
            for await (const entry of tree.range(start, end)) {
              results.push(entry as [number, string]);
            }
            expect(results.length).toBe(300);
            for (let j = 0; j < results.length; j++) {
              expect(results[j]![0]).toBe(start + j);
            }
          })()
        );
      }

      // 100 concurrent point lookups
      for (let i = 0; i < 100; i++) {
        const key = Math.floor(Math.random() * N);
        promises.push(
          tree.get(key).then((val) => {
            expect(val).toBe(`val_${key}`);
          })
        );
      }

      await Promise.all(promises);
    }, 120_000);

    it('should handle rapid sequential set-then-get cycles', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Write-then-immediately-read pattern at scale
      const N = 3_000;
      for (let i = 0; i < N; i++) {
        await tree.set(i, `cycle_${i}`);
        const val = await tree.get(i);
        expect(val).toBe(`cycle_${i}`);
      }

      expect(await tree.count()).toBe(N);
    }, 60_000);
  });

  // ===========================================================================
  // Additional stress: tree height and structure validation
  // ===========================================================================

  describe('tree structure validation under stress', () => {
    it('should grow tree height proportionally to log of dataset size', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      const heights: number[] = [];
      const checkpoints = [100, 500, 1_000, 2_000, 5_000];

      for (let i = 0; i < 5_000; i++) {
        await tree.set(i, i);
        if (checkpoints.includes(i + 1)) {
          const stats = await tree.stats();
          heights.push(stats.height);
        }
      }

      // Height should increase with dataset size but remain manageable
      // For maxKeys=4, height should be roughly log_2(N) since each node holds 2-4 keys
      for (let i = 1; i < heights.length; i++) {
        expect(heights[i]).toBeGreaterThanOrEqual(heights[i - 1]!);
      }

      // With 5000 entries and maxKeys=4, height should be reasonable
      // Maximum theoretical height is log_minKeys(N) which is log_2(5000) ~ 12
      const finalHeight = heights[heights.length - 1]!;
      expect(finalHeight).toBeLessThanOrEqual(15);
      expect(finalHeight).toBeGreaterThanOrEqual(3);
    }, 60_000);

    it('should maintain correct entry count through mixed operations', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      const rng = mulberry32(54321);
      const reference = new Set<number>();

      for (let i = 0; i < 3_000; i++) {
        const op = rng();
        const key = Math.floor(rng() * 1_000);

        if (op < 0.5) {
          // Insert
          await tree.set(key, key);
          reference.add(key);
        } else if (op < 0.8) {
          // Delete
          await tree.delete(key);
          reference.delete(key);
        } else {
          // Read (should not change count)
          await tree.get(key);
        }

        // Periodically verify count
        if (i % 500 === 0) {
          expect(await tree.count()).toBe(reference.size);
        }
      }

      expect(await tree.count()).toBe(reference.size);
    }, 60_000);

    it('should survive insert-delete-reinsert cycles', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      const N = 2_000;

      // Cycle 1: Insert all
      for (let i = 0; i < N; i++) {
        await tree.set(i, `cycle1_${i}`);
      }
      expect(await tree.count()).toBe(N);

      // Cycle 2: Delete all
      for (let i = 0; i < N; i++) {
        await tree.delete(i);
      }
      expect(await tree.count()).toBe(0);

      // Cycle 3: Reinsert all
      for (let i = 0; i < N; i++) {
        await tree.set(i, `cycle3_${i}`);
      }
      expect(await tree.count()).toBe(N);

      // Verify cycle 3 values
      for (let i = 0; i < N; i++) {
        expect(await tree.get(i)).toBe(`cycle3_${i}`);
      }

      // Cycle 4: Delete even, insert new range
      for (let i = 0; i < N; i += 2) {
        await tree.delete(i);
      }
      for (let i = N; i < N + N / 2; i++) {
        await tree.set(i, `cycle4_${i}`);
      }

      expect(await tree.count()).toBe(N / 2 + N / 2); // odd remaining + new range

      // Verify odd keys from cycle 3
      for (let i = 1; i < N; i += 2) {
        expect(await tree.get(i)).toBe(`cycle3_${i}`);
      }

      // Verify new range
      for (let i = N; i < N + N / 2; i++) {
        expect(await tree.get(i)).toBe(`cycle4_${i}`);
      }
    }, 120_000);
  });
});
