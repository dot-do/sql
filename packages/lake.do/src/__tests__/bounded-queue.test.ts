/**
 * Tests for lake.do BoundedQueue
 *
 * @module lake.do/tests/bounded-queue
 */

import { describe, it, expect } from 'vitest';
import { BoundedQueue, type BackpressureStrategy } from '../cdc-stream/queue.js';

describe('BoundedQueue', () => {
  describe('constructor', () => {
    it('creates queue with default options', () => {
      const queue = new BoundedQueue<string>();

      expect(queue.maxSize).toBe(1000);
      expect(queue.size).toBe(0);
      expect(queue.backpressureStrategy).toBe('block');
      expect(queue.droppedCount).toBe(0);
      expect(queue.isEmpty).toBe(true);
      expect(queue.isFull).toBe(false);
    });

    it('creates queue with custom max size', () => {
      const queue = new BoundedQueue<string>({ maxSize: 50 });

      expect(queue.maxSize).toBe(50);
    });

    it('creates queue with custom backpressure strategy', () => {
      const strategies: BackpressureStrategy[] = ['block', 'drop-oldest', 'drop-newest'];

      for (const strategy of strategies) {
        const queue = new BoundedQueue<string>({ backpressureStrategy: strategy });
        expect(queue.backpressureStrategy).toBe(strategy);
      }
    });

    it('throws for maxSize less than 1', () => {
      expect(() => new BoundedQueue<string>({ maxSize: 0 })).toThrow('maxSize must be at least 1');
      expect(() => new BoundedQueue<string>({ maxSize: -1 })).toThrow('maxSize must be at least 1');
      expect(() => new BoundedQueue<string>({ maxSize: -100 })).toThrow('maxSize must be at least 1');
    });

    it('allows maxSize of 1', () => {
      const queue = new BoundedQueue<string>({ maxSize: 1 });
      expect(queue.maxSize).toBe(1);
    });
  });

  describe('push and shift', () => {
    it('pushes and shifts items in FIFO order', () => {
      const queue = new BoundedQueue<string>({ maxSize: 10 });

      queue.push('a');
      queue.push('b');
      queue.push('c');

      expect(queue.size).toBe(3);
      expect(queue.shift()).toBe('a');
      expect(queue.shift()).toBe('b');
      expect(queue.shift()).toBe('c');
      expect(queue.shift()).toBeUndefined();
      expect(queue.isEmpty).toBe(true);
    });

    it('returns push result for accepted items', () => {
      const queue = new BoundedQueue<string>({ maxSize: 5 });

      const result = queue.push('item');

      expect(result.accepted).toBe(true);
      expect(result.dropped).toBe(false);
    });

    it('shift returns undefined for empty queue', () => {
      const queue = new BoundedQueue<string>();

      expect(queue.shift()).toBeUndefined();
    });
  });

  describe('peek', () => {
    it('returns first item without removing', () => {
      const queue = new BoundedQueue<string>();

      queue.push('first');
      queue.push('second');

      expect(queue.peek()).toBe('first');
      expect(queue.peek()).toBe('first');
      expect(queue.size).toBe(2);
    });

    it('returns undefined for empty queue', () => {
      const queue = new BoundedQueue<string>();

      expect(queue.peek()).toBeUndefined();
    });
  });

  describe('clear', () => {
    it('clears all items and returns count', () => {
      const queue = new BoundedQueue<string>();

      queue.push('a');
      queue.push('b');
      queue.push('c');

      const cleared = queue.clear();

      expect(cleared).toBe(3);
      expect(queue.size).toBe(0);
      expect(queue.isEmpty).toBe(true);
    });

    it('returns 0 for empty queue', () => {
      const queue = new BoundedQueue<string>();

      expect(queue.clear()).toBe(0);
    });
  });

  describe('isFull and isEmpty', () => {
    it('reports full when at capacity', () => {
      const queue = new BoundedQueue<string>({ maxSize: 2 });

      expect(queue.isFull).toBe(false);
      queue.push('a');
      expect(queue.isFull).toBe(false);
      queue.push('b');
      expect(queue.isFull).toBe(true);
    });

    it('reports empty when no items', () => {
      const queue = new BoundedQueue<string>();

      expect(queue.isEmpty).toBe(true);
      queue.push('a');
      expect(queue.isEmpty).toBe(false);
      queue.shift();
      expect(queue.isEmpty).toBe(true);
    });
  });

  describe('block strategy', () => {
    it('returns false when pushing to full queue', () => {
      const queue = new BoundedQueue<string>({
        maxSize: 2,
        backpressureStrategy: 'block',
      });

      queue.push('a');
      queue.push('b');

      const result = queue.push('c');

      expect(result.accepted).toBe(false);
      expect(result.dropped).toBe(true);
      expect(queue.size).toBe(2);
      expect(queue.droppedCount).toBe(1);
    });

    it('increments dropped count on rejection', () => {
      const queue = new BoundedQueue<string>({
        maxSize: 1,
        backpressureStrategy: 'block',
      });

      queue.push('a');
      queue.push('b');
      queue.push('c');

      expect(queue.droppedCount).toBe(2);
    });
  });

  describe('drop-oldest strategy', () => {
    it('drops oldest item when full', () => {
      const queue = new BoundedQueue<string>({
        maxSize: 3,
        backpressureStrategy: 'drop-oldest',
      });

      queue.push('a');
      queue.push('b');
      queue.push('c');

      const result = queue.push('d');

      expect(result.accepted).toBe(true);
      expect(result.dropped).toBe(true);
      expect(queue.size).toBe(3);
      expect(queue.shift()).toBe('b'); // 'a' was dropped
      expect(queue.shift()).toBe('c');
      expect(queue.shift()).toBe('d');
    });

    it('tracks dropped count', () => {
      const queue = new BoundedQueue<string>({
        maxSize: 2,
        backpressureStrategy: 'drop-oldest',
      });

      queue.push('a');
      queue.push('b');
      queue.push('c');
      queue.push('d');
      queue.push('e');

      expect(queue.droppedCount).toBe(3);
      expect(queue.shift()).toBe('d');
      expect(queue.shift()).toBe('e');
    });
  });

  describe('drop-newest strategy', () => {
    it('rejects new item when full', () => {
      const queue = new BoundedQueue<string>({
        maxSize: 3,
        backpressureStrategy: 'drop-newest',
      });

      queue.push('a');
      queue.push('b');
      queue.push('c');

      const result = queue.push('d');

      expect(result.accepted).toBe(false);
      expect(result.dropped).toBe(true);
      expect(queue.size).toBe(3);
      expect(queue.shift()).toBe('a'); // Original items preserved
      expect(queue.shift()).toBe('b');
      expect(queue.shift()).toBe('c');
    });

    it('tracks dropped count', () => {
      const queue = new BoundedQueue<string>({
        maxSize: 2,
        backpressureStrategy: 'drop-newest',
      });

      queue.push('a');
      queue.push('b');
      queue.push('c');
      queue.push('d');

      expect(queue.droppedCount).toBe(2);
    });
  });

  describe('setMaxSize', () => {
    it('increases max size', () => {
      const queue = new BoundedQueue<string>({ maxSize: 2 });

      queue.push('a');
      queue.push('b');
      expect(queue.isFull).toBe(true);

      queue.setMaxSize(5);

      expect(queue.maxSize).toBe(5);
      expect(queue.isFull).toBe(false);
      expect(queue.push('c').accepted).toBe(true);
    });

    it('drops oldest items when reducing size with drop-oldest strategy', () => {
      const queue = new BoundedQueue<string>({
        maxSize: 5,
        backpressureStrategy: 'drop-oldest',
      });

      queue.push('a');
      queue.push('b');
      queue.push('c');
      queue.push('d');
      queue.push('e');

      const dropped = queue.setMaxSize(2);

      expect(dropped).toBe(3);
      expect(queue.size).toBe(2);
      expect(queue.droppedCount).toBe(3);
      expect(queue.shift()).toBe('d');
      expect(queue.shift()).toBe('e');
    });

    it('drops newest items when reducing size with other strategies', () => {
      const queue = new BoundedQueue<string>({
        maxSize: 5,
        backpressureStrategy: 'block',
      });

      queue.push('a');
      queue.push('b');
      queue.push('c');
      queue.push('d');
      queue.push('e');

      const dropped = queue.setMaxSize(2);

      expect(dropped).toBe(3);
      expect(queue.size).toBe(2);
      expect(queue.shift()).toBe('a');
      expect(queue.shift()).toBe('b');
    });

    it('throws for size less than 1', () => {
      const queue = new BoundedQueue<string>();

      expect(() => queue.setMaxSize(0)).toThrow('maxSize must be at least 1');
      expect(() => queue.setMaxSize(-1)).toThrow('maxSize must be at least 1');
    });

    it('returns 0 when new size is larger than current', () => {
      const queue = new BoundedQueue<string>({ maxSize: 5 });

      queue.push('a');
      queue.push('b');

      const dropped = queue.setMaxSize(10);

      expect(dropped).toBe(0);
      expect(queue.size).toBe(2);
    });
  });

  describe('resetDroppedCount', () => {
    it('resets dropped count to 0', () => {
      const queue = new BoundedQueue<string>({
        maxSize: 2,
        backpressureStrategy: 'block',
      });

      queue.push('a');
      queue.push('b');
      queue.push('c');
      queue.push('d');

      expect(queue.droppedCount).toBe(2);

      queue.resetDroppedCount();

      expect(queue.droppedCount).toBe(0);
    });
  });

  describe('pushAsync', () => {
    it('pushes immediately when queue has space', async () => {
      const queue = new BoundedQueue<string>({ maxSize: 5 });

      const result = await queue.pushAsync('item');

      expect(result.accepted).toBe(true);
      expect(queue.size).toBe(1);
    });

    it('waits for space with block strategy', async () => {
      const queue = new BoundedQueue<string>({
        maxSize: 2,
        backpressureStrategy: 'block',
      });

      queue.push('a');
      queue.push('b');

      // Start async push (will wait)
      const pushPromise = queue.pushAsync('c');

      // Consume one item to make space
      setTimeout(() => queue.shift(), 10);

      const result = await pushPromise;

      expect(result.accepted).toBe(true);
      expect(queue.size).toBe(2);
    });

    it('behaves like push for drop-oldest strategy', async () => {
      const queue = new BoundedQueue<string>({
        maxSize: 2,
        backpressureStrategy: 'drop-oldest',
      });

      queue.push('a');
      queue.push('b');

      const result = await queue.pushAsync('c');

      expect(result.accepted).toBe(true);
      expect(result.dropped).toBe(true);
      expect(queue.shift()).toBe('b');
    });

    it('behaves like push for drop-newest strategy', async () => {
      const queue = new BoundedQueue<string>({
        maxSize: 2,
        backpressureStrategy: 'drop-newest',
      });

      queue.push('a');
      queue.push('b');

      const result = await queue.pushAsync('c');

      expect(result.accepted).toBe(false);
      expect(result.dropped).toBe(true);
    });
  });

  describe('cancelAsyncPushes', () => {
    it('resolves all waiting async push operations', async () => {
      const queue = new BoundedQueue<string>({
        maxSize: 1,
        backpressureStrategy: 'block',
      });

      queue.push('a');

      // Start multiple async pushes
      const promise1 = queue.pushAsync('b');
      const promise2 = queue.pushAsync('c');

      // Cancel all waiting pushes
      queue.cancelAsyncPushes();

      // Both should resolve (they'll try to push to still-full queue)
      await Promise.all([promise1, promise2]);

      // Queue should still have just the original item
      expect(queue.size).toBe(1);
    });
  });

  describe('type safety', () => {
    it('preserves item types', () => {
      interface TestItem {
        id: number;
        name: string;
      }

      const queue = new BoundedQueue<TestItem>();

      queue.push({ id: 1, name: 'first' });
      queue.push({ id: 2, name: 'second' });

      const item = queue.shift();

      expect(item?.id).toBe(1);
      expect(item?.name).toBe('first');
    });
  });
});
