/**
 * Tests for lake.do CDCStreamController
 *
 * This complements the cdc-backpressure.test.ts with additional
 * coverage for the controller functionality.
 *
 * @module lake.do/tests/cdc-stream-controller
 */

import { describe, it, expect, vi } from 'vitest';
import { CDCStreamController } from '../cdc-stream/controller.js';
import type { CDCBatch } from '../types.js';

/**
 * Helper to create a mock CDC batch
 */
function createMockBatch(sequenceNumber: number): CDCBatch {
  return {
    sequenceNumber,
    timestamp: new Date(),
    sourceDoId: 'test-do',
    events: [
      {
        id: `event-${sequenceNumber}` as any,
        table: 'orders',
        operation: 'INSERT',
        timestamp: new Date(),
        after: { id: `order-${sequenceNumber}`, amount: 100 },
      },
    ],
  };
}

describe('CDCStreamController', () => {
  describe('constructor', () => {
    it('creates controller with default options', () => {
      const controller = new CDCStreamController();

      expect(controller.maxQueueSize).toBe(1000);
      expect(controller.queueSize).toBe(0);
      expect(controller.droppedCount).toBe(0);
      expect(controller.backpressureStrategy).toBe('block');
    });

    it('creates controller with custom max queue size', () => {
      const controller = new CDCStreamController({ maxQueueSize: 100 });

      expect(controller.maxQueueSize).toBe(100);
    });

    it('creates controller with custom backpressure strategy', () => {
      const controller = new CDCStreamController({
        backpressureStrategy: 'drop-oldest',
      });

      expect(controller.backpressureStrategy).toBe('drop-oldest');
    });

    it('throws for maxQueueSize less than 1', () => {
      expect(() => new CDCStreamController({ maxQueueSize: 0 })).toThrow('maxQueueSize must be at least 1');
      expect(() => new CDCStreamController({ maxQueueSize: -5 })).toThrow('maxQueueSize must be at least 1');
    });
  });

  describe('push', () => {
    it('pushes batch to queue', () => {
      const controller = new CDCStreamController({ maxQueueSize: 10 });

      const result = controller.push(createMockBatch(1));

      expect(result).toBe(true);
      expect(controller.queueSize).toBe(1);
    });

    it('returns false when closed', () => {
      const controller = new CDCStreamController();

      controller.close();

      const result = controller.push(createMockBatch(1));

      expect(result).toBe(false);
    });

    it('delivers directly to waiting consumer', async () => {
      const controller = new CDCStreamController();

      // Start iteration (consumer waits)
      const iterator = controller[Symbol.asyncIterator]();
      const nextPromise = iterator.next();

      // Push batch
      controller.push(createMockBatch(1));

      // Consumer should receive batch directly
      const result = await nextPromise;

      expect(result.done).toBe(false);
      expect(result.value.sequenceNumber).toBe(1);
    });
  });

  describe('pushAsync', () => {
    it('pushes immediately when queue has space', async () => {
      const controller = new CDCStreamController({ maxQueueSize: 10 });

      const result = await controller.pushAsync(createMockBatch(1));

      expect(result).toBe(true);
      expect(controller.queueSize).toBe(1);
    });

    it('returns false when closed', async () => {
      const controller = new CDCStreamController();

      controller.close();

      const result = await controller.pushAsync(createMockBatch(1));

      expect(result).toBe(false);
    });

    it('waits for space with block strategy', async () => {
      const controller = new CDCStreamController({
        maxQueueSize: 2,
        backpressureStrategy: 'block',
      });

      controller.push(createMockBatch(1));
      controller.push(createMockBatch(2));

      // Start async push (will wait)
      const pushPromise = controller.pushAsync(createMockBatch(3));

      // Consume one batch to make space
      const iterator = controller[Symbol.asyncIterator]();
      await iterator.next();

      const result = await pushPromise;

      expect(result).toBe(true);
    });
  });

  describe('setMaxQueueSize', () => {
    it('increases max queue size', () => {
      const controller = new CDCStreamController({ maxQueueSize: 10 });

      controller.setMaxQueueSize(20);

      expect(controller.maxQueueSize).toBe(20);
    });

    it('decreases max queue size and drops excess with drop-oldest', () => {
      const controller = new CDCStreamController({
        maxQueueSize: 5,
        backpressureStrategy: 'drop-oldest',
      });

      for (let i = 1; i <= 5; i++) {
        controller.push(createMockBatch(i));
      }

      controller.setMaxQueueSize(2);

      expect(controller.queueSize).toBe(2);
      expect(controller.droppedCount).toBe(3);
    });
  });

  describe('getMetrics', () => {
    it('returns complete metrics', () => {
      const controller = new CDCStreamController({ maxQueueSize: 100 });

      controller.push(createMockBatch(1));
      controller.push(createMockBatch(2));
      controller.push(createMockBatch(3));

      const metrics = controller.getMetrics();

      expect(metrics.currentDepth).toBe(3);
      expect(metrics.maxDepth).toBe(100);
      expect(metrics.peakDepth).toBeGreaterThanOrEqual(3);
      expect(metrics.droppedCount).toBe(0);
      expect(metrics.totalPushed).toBe(3);
      expect(metrics.utilizationPercent).toBe(3);
    });

    it('tracks consumed batches', async () => {
      const controller = new CDCStreamController({ maxQueueSize: 100 });

      controller.push(createMockBatch(1));
      controller.push(createMockBatch(2));
      controller.push(createMockBatch(3));

      const iterator = controller[Symbol.asyncIterator]();
      await iterator.next();
      await iterator.next();

      const metrics = controller.getMetrics();

      expect(metrics.totalConsumed).toBe(2);
      expect(metrics.currentDepth).toBe(1);
    });
  });

  describe('resetMetrics', () => {
    it('resets counters while preserving queue', async () => {
      const controller = new CDCStreamController({ maxQueueSize: 100 });

      controller.push(createMockBatch(1));
      controller.push(createMockBatch(2));
      controller.push(createMockBatch(3));

      const iterator = controller[Symbol.asyncIterator]();
      await iterator.next();

      controller.resetMetrics();

      const metrics = controller.getMetrics();

      expect(metrics.currentDepth).toBe(2);
      expect(metrics.peakDepth).toBe(2);
      expect(metrics.totalPushed).toBe(0);
      expect(metrics.totalConsumed).toBe(0);
      expect(metrics.droppedCount).toBe(0);
    });
  });

  describe('close', () => {
    it('closes the controller', () => {
      const controller = new CDCStreamController();

      controller.push(createMockBatch(1));

      controller.close();

      expect(controller.push(createMockBatch(2))).toBe(false);
    });

    it('resolves waiting consumers with done', async () => {
      const controller = new CDCStreamController();

      const iterator = controller[Symbol.asyncIterator]();
      const nextPromise = iterator.next();

      controller.close();

      const result = await nextPromise;

      expect(result.done).toBe(true);
    });

    it('allows consuming remaining items after close', async () => {
      const controller = new CDCStreamController();

      controller.push(createMockBatch(1));
      controller.push(createMockBatch(2));

      controller.close();

      const iterator = controller[Symbol.asyncIterator]();

      const first = await iterator.next();
      expect(first.done).toBe(false);
      expect(first.value.sequenceNumber).toBe(1);

      const second = await iterator.next();
      expect(second.done).toBe(false);
      expect(second.value.sequenceNumber).toBe(2);

      const third = await iterator.next();
      expect(third.done).toBe(true);
    });
  });

  describe('async iteration', () => {
    it('implements Symbol.asyncIterator', () => {
      const controller = new CDCStreamController();

      expect(typeof controller[Symbol.asyncIterator]).toBe('function');
    });

    it('iterates over pushed batches', async () => {
      const controller = new CDCStreamController();

      controller.push(createMockBatch(1));
      controller.push(createMockBatch(2));
      controller.push(createMockBatch(3));

      controller.close();

      const batches: CDCBatch[] = [];
      for await (const batch of controller) {
        batches.push(batch);
      }

      expect(batches).toHaveLength(3);
      expect(batches[0].sequenceNumber).toBe(1);
      expect(batches[1].sequenceNumber).toBe(2);
      expect(batches[2].sequenceNumber).toBe(3);
    });

    it('waits for batches when queue is empty', async () => {
      const controller = new CDCStreamController();

      const iterator = controller[Symbol.asyncIterator]();
      const nextPromise = iterator.next();

      // Push after a delay
      setTimeout(() => controller.push(createMockBatch(42)), 10);

      const result = await nextPromise;

      expect(result.done).toBe(false);
      expect(result.value.sequenceNumber).toBe(42);
    });
  });

  describe('water mark events', () => {
    it('triggers high water mark callback', () => {
      const onHighWaterMark = vi.fn();

      const controller = new CDCStreamController({
        maxQueueSize: 10,
        highWaterMark: 80,
        onHighWaterMark,
      });

      // Fill to 80%
      for (let i = 1; i <= 8; i++) {
        controller.push(createMockBatch(i));
      }

      expect(onHighWaterMark).toHaveBeenCalledTimes(1);
      expect(onHighWaterMark).toHaveBeenCalledWith({
        currentDepth: 8,
        maxDepth: 10,
        utilizationPercent: 80,
      });
    });

    it('triggers low water mark callback', async () => {
      const onHighWaterMark = vi.fn();
      const onLowWaterMark = vi.fn();

      const controller = new CDCStreamController({
        maxQueueSize: 10,
        highWaterMark: 80,
        lowWaterMark: 20,
        onHighWaterMark,
        onLowWaterMark,
      });

      // Fill above high water mark
      for (let i = 1; i <= 9; i++) {
        controller.push(createMockBatch(i));
      }

      // Consume down below low water mark
      const iterator = controller[Symbol.asyncIterator]();
      for (let i = 0; i < 8; i++) {
        await iterator.next();
      }

      expect(onLowWaterMark).toHaveBeenCalled();
    });
  });

  describe('backpressure strategies', () => {
    it('block strategy returns false when full', () => {
      const controller = new CDCStreamController({
        maxQueueSize: 2,
        backpressureStrategy: 'block',
      });

      controller.push(createMockBatch(1));
      controller.push(createMockBatch(2));

      const result = controller.push(createMockBatch(3));

      expect(result).toBe(false);
      expect(controller.queueSize).toBe(2);
    });

    it('drop-oldest strategy drops oldest when full', async () => {
      const controller = new CDCStreamController({
        maxQueueSize: 2,
        backpressureStrategy: 'drop-oldest',
      });

      controller.push(createMockBatch(1));
      controller.push(createMockBatch(2));
      controller.push(createMockBatch(3));

      expect(controller.queueSize).toBe(2);
      expect(controller.droppedCount).toBe(1);

      const iterator = controller[Symbol.asyncIterator]();
      const first = await iterator.next();

      expect(first.value.sequenceNumber).toBe(2); // Not 1
    });

    it('drop-newest strategy drops new when full', () => {
      const controller = new CDCStreamController({
        maxQueueSize: 2,
        backpressureStrategy: 'drop-newest',
      });

      controller.push(createMockBatch(1));
      controller.push(createMockBatch(2));

      const result = controller.push(createMockBatch(3));

      expect(result).toBe(false);
      expect(controller.queueSize).toBe(2);
      expect(controller.droppedCount).toBe(1);
    });
  });
});
