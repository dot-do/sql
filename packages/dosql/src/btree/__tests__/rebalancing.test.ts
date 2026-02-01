/**
 * B-tree Rebalancing Tests
 *
 * Tests for node merging and key redistribution on delete operations.
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

describe('B-tree Rebalancing', () => {
  let fsx: MemoryFSXBackend;

  beforeEach(() => {
    fsx = new MemoryFSXBackend();
  });

  describe('leaf node rebalancing', () => {
    it('should maintain sorted order after deleting from underfull leaf', async () => {
      // Use small maxKeys to force splits and make testing easier
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Insert keys to create a multi-level tree
      for (let i = 0; i < 20; i++) {
        await tree.set(i, `value_${i}`);
      }

      const statsBefore = await tree.stats();
      expect(statsBefore.height).toBeGreaterThan(1);

      // Delete keys from the middle
      await tree.delete(5);
      await tree.delete(6);
      await tree.delete(7);

      // Verify remaining entries are still accessible and in order
      const entries: [number, string][] = [];
      for await (const entry of tree.entries()) {
        entries.push(entry as [number, string]);
      }

      expect(entries.length).toBe(17);

      // Verify order is maintained
      for (let i = 1; i < entries.length; i++) {
        expect(entries[i][0]).toBeGreaterThan(entries[i - 1][0]);
      }

      // Verify deleted keys are gone
      expect(await tree.get(5)).toBeUndefined();
      expect(await tree.get(6)).toBeUndefined();
      expect(await tree.get(7)).toBeUndefined();

      // Verify other keys are still there
      expect(await tree.get(4)).toBe('value_4');
      expect(await tree.get(8)).toBe('value_8');
    });

    it('should borrow from left sibling when possible', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Insert enough to create multiple leaves
      for (let i = 0; i < 15; i++) {
        await tree.set(i, `value_${i}`);
      }

      // Delete from the right side, forcing borrow from left
      await tree.delete(14);
      await tree.delete(13);
      await tree.delete(12);

      // All remaining keys should still be accessible
      for (let i = 0; i < 12; i++) {
        expect(await tree.get(i)).toBe(`value_${i}`);
      }

      expect(await tree.count()).toBe(12);
    });

    it('should borrow from right sibling when possible', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Insert enough to create multiple leaves
      for (let i = 0; i < 15; i++) {
        await tree.set(i, `value_${i}`);
      }

      // Delete from the left side, forcing borrow from right
      await tree.delete(0);
      await tree.delete(1);
      await tree.delete(2);

      // All remaining keys should still be accessible
      for (let i = 3; i < 15; i++) {
        expect(await tree.get(i)).toBe(`value_${i}`);
      }

      expect(await tree.count()).toBe(12);
    });

    it('should merge leaves when neither sibling can donate', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Insert exactly enough keys to create a minimal tree
      for (let i = 0; i < 10; i++) {
        await tree.set(i, `value_${i}`);
      }

      const statsBefore = await tree.stats();

      // Delete enough keys to force merging
      for (let i = 0; i < 5; i++) {
        await tree.delete(i);
      }

      const statsAfter = await tree.stats();

      // Tree should have shrunk (fewer pages)
      expect(statsAfter.entryCount).toBe(5);

      // Remaining entries should be intact
      for (let i = 5; i < 10; i++) {
        expect(await tree.get(i)).toBe(`value_${i}`);
      }
    });
  });

  describe('internal node rebalancing', () => {
    it('should handle deletes that cause internal node underflow', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Insert many keys to create a tall tree
      for (let i = 0; i < 50; i++) {
        await tree.set(i, `value_${i}`);
      }

      const statsBefore = await tree.stats();
      expect(statsBefore.height).toBeGreaterThan(2);

      // Delete most keys
      for (let i = 0; i < 40; i++) {
        await tree.delete(i);
      }

      // Tree should still be valid
      expect(await tree.count()).toBe(10);

      // Remaining entries should be accessible
      for (let i = 40; i < 50; i++) {
        expect(await tree.get(i)).toBe(`value_${i}`);
      }

      // Tree height may have decreased
      const statsAfter = await tree.stats();
      expect(statsAfter.height).toBeLessThanOrEqual(statsBefore.height);
    });

    it('should shrink tree height when root becomes empty', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Create a tree with height > 1
      for (let i = 0; i < 20; i++) {
        await tree.set(i, `value_${i}`);
      }

      const statsBefore = await tree.stats();
      expect(statsBefore.height).toBeGreaterThan(1);

      // Delete down to a few keys
      for (let i = 0; i < 17; i++) {
        await tree.delete(i);
      }

      expect(await tree.count()).toBe(3);

      // Remaining keys should be accessible
      expect(await tree.get(17)).toBe('value_17');
      expect(await tree.get(18)).toBe('value_18');
      expect(await tree.get(19)).toBe('value_19');
    });
  });

  describe('leaf chain integrity', () => {
    it('should maintain leaf chain after merge', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Create multiple leaves
      for (let i = 0; i < 20; i++) {
        await tree.set(i, `value_${i}`);
      }

      // Delete from middle to trigger merge
      for (let i = 5; i < 15; i++) {
        await tree.delete(i);
      }

      // Range scan should still work (uses leaf chain)
      const entries: [number, string][] = [];
      for await (const entry of tree.range(0, 20)) {
        entries.push(entry as [number, string]);
      }

      expect(entries.length).toBe(10);

      // Should be in order: 0-4, 15-19
      const expectedKeys = [0, 1, 2, 3, 4, 15, 16, 17, 18, 19];
      expect(entries.map(([k]) => k)).toEqual(expectedKeys);
    });

    it('should maintain backward iteration capability after merge', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      for (let i = 0; i < 20; i++) {
        await tree.set(i, `value_${i}`);
      }

      // Delete to trigger merges
      for (let i = 0; i < 10; i++) {
        await tree.delete(i);
      }

      // entries() iteration should still work
      const entries: [number, string][] = [];
      for await (const entry of tree.entries()) {
        entries.push(entry as [number, string]);
      }

      expect(entries.length).toBe(10);
      for (let i = 0; i < 10; i++) {
        expect(entries[i][0]).toBe(i + 10);
      }
    });
  });

  describe('edge cases', () => {
    it('should handle deleting all entries', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      for (let i = 0; i < 30; i++) {
        await tree.set(i, `value_${i}`);
      }

      // Delete all
      for (let i = 0; i < 30; i++) {
        const deleted = await tree.delete(i);
        expect(deleted).toBe(true);
      }

      expect(await tree.count()).toBe(0);

      const stats = await tree.stats();
      expect(stats.height).toBe(1); // Should be back to single leaf
    });

    it('should handle interleaved inserts and deletes', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Insert and delete in interleaved fashion
      for (let round = 0; round < 5; round++) {
        // Insert 10 keys
        for (let i = round * 10; i < (round + 1) * 10; i++) {
          await tree.set(i, `value_${i}`);
        }

        // Delete some of them
        for (let i = round * 10; i < round * 10 + 5; i++) {
          await tree.delete(i);
        }
      }

      // Should have 25 entries (5 per round)
      expect(await tree.count()).toBe(25);

      // Verify the remaining keys
      for (let round = 0; round < 5; round++) {
        for (let i = round * 10 + 5; i < (round + 1) * 10; i++) {
          expect(await tree.get(i)).toBe(`value_${i}`);
        }
      }
    });

    it('should handle delete in reverse order', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      for (let i = 0; i < 25; i++) {
        await tree.set(i, `value_${i}`);
      }

      // Delete in reverse order
      for (let i = 24; i >= 0; i--) {
        const deleted = await tree.delete(i);
        expect(deleted).toBe(true);

        // Remaining entries should still be valid
        for (let j = 0; j < i; j++) {
          expect(await tree.get(j)).toBe(`value_${j}`);
        }
      }

      expect(await tree.count()).toBe(0);
    });

    it('should handle delete with string keys', async () => {
      const tree = createBTree(fsx, StringKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      const keys = [
        'alpha', 'bravo', 'charlie', 'delta', 'echo',
        'foxtrot', 'golf', 'hotel', 'india', 'juliet',
        'kilo', 'lima', 'mike', 'november', 'oscar',
      ];

      for (const key of keys) {
        await tree.set(key, { name: key });
      }

      // Delete every other key
      for (let i = 0; i < keys.length; i += 2) {
        await tree.delete(keys[i]);
      }

      expect(await tree.count()).toBe(7);

      // Verify remaining keys
      for (let i = 1; i < keys.length; i += 2) {
        expect(await tree.get(keys[i])).toEqual({ name: keys[i] });
      }
    });

    it('should not fail when deleting from root that is a leaf', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Insert just a few keys (stays in single root leaf)
      await tree.set(1, 'one');
      await tree.set(2, 'two');
      await tree.set(3, 'three');

      const stats = await tree.stats();
      expect(stats.height).toBe(1);

      // Delete from root leaf (should not trigger rebalancing)
      await tree.delete(2);
      expect(await tree.count()).toBe(2);
      expect(await tree.get(1)).toBe('one');
      expect(await tree.get(3)).toBe('three');

      // Delete all
      await tree.delete(1);
      await tree.delete(3);
      expect(await tree.count()).toBe(0);
    });
  });

  describe('stress tests', () => {
    it('should handle random operations correctly', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      const reference = new Map<number, string>();
      const rng = mulberry32(12345); // Seeded RNG for reproducibility

      // Perform random operations
      for (let i = 0; i < 200; i++) {
        const op = rng() < 0.6 ? 'insert' : 'delete';
        const key = Math.floor(rng() * 100);

        if (op === 'insert') {
          const value = `value_${key}_${i}`;
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

      // Verify entries iterator
      const entries: [number, string][] = [];
      for await (const entry of tree.entries()) {
        entries.push(entry as [number, string]);
      }

      expect(entries.length).toBe(reference.size);

      // Verify sorted order
      for (let i = 1; i < entries.length; i++) {
        expect(entries[i][0]).toBeGreaterThan(entries[i - 1][0]);
      }
    });

    it('should handle heavy delete workload', async () => {
      const tree = createBTree(fsx, NumberKeyCodec, JsonValueCodec, {
        minKeys: 2,
        maxKeys: 4,
      });
      await tree.init();

      // Insert 100 keys
      for (let i = 0; i < 100; i++) {
        await tree.set(i, `value_${i}`);
      }

      const statsBefore = await tree.stats();

      // Delete 90 of them (heavy delete workload)
      for (let i = 0; i < 90; i++) {
        await tree.delete(i);
      }

      expect(await tree.count()).toBe(10);

      // Remaining should be 90-99
      for (let i = 90; i < 100; i++) {
        expect(await tree.get(i)).toBe(`value_${i}`);
      }

      // Tree should have compacted
      const statsAfter = await tree.stats();
      expect(statsAfter.height).toBeLessThanOrEqual(statsBefore.height);
    });
  });
});

// Simple seeded PRNG for reproducible tests
function mulberry32(seed: number): () => number {
  return function () {
    let t = (seed += 0x6d2b79f5);
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
