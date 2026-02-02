/**
 * Tests for lake.do MetricsTracker
 *
 * @module lake.do/tests/metrics-tracker
 */

import { describe, it, expect, vi } from 'vitest';
import { MetricsTracker, type WaterMarkEvent } from '../cdc-stream/metrics.js';

describe('MetricsTracker', () => {
  describe('constructor', () => {
    it('creates tracker with basic options', () => {
      const tracker = new MetricsTracker({ maxDepth: 100 });

      expect(tracker.maxDepth).toBe(100);
      expect(tracker.peakDepth).toBe(0);
      expect(tracker.totalPushed).toBe(0);
      expect(tracker.totalConsumed).toBe(0);
    });

    it('creates tracker with water mark options', () => {
      const onHighWaterMark = vi.fn();
      const onLowWaterMark = vi.fn();

      const tracker = new MetricsTracker({
        maxDepth: 100,
        highWaterMark: 80,
        lowWaterMark: 20,
        onHighWaterMark,
        onLowWaterMark,
      });

      expect(tracker.maxDepth).toBe(100);
    });
  });

  describe('maxDepth', () => {
    it('gets and sets maxDepth', () => {
      const tracker = new MetricsTracker({ maxDepth: 100 });

      expect(tracker.maxDepth).toBe(100);

      tracker.maxDepth = 200;

      expect(tracker.maxDepth).toBe(200);
    });
  });

  describe('recordPush', () => {
    it('increments totalPushed', () => {
      const tracker = new MetricsTracker({ maxDepth: 100 });

      tracker.recordPush(1);
      tracker.recordPush(2);
      tracker.recordPush(3);

      expect(tracker.totalPushed).toBe(3);
    });

    it('updates peakDepth when current depth exceeds it', () => {
      const tracker = new MetricsTracker({ maxDepth: 100 });

      tracker.recordPush(5);
      expect(tracker.peakDepth).toBe(5);

      tracker.recordPush(10);
      expect(tracker.peakDepth).toBe(10);

      tracker.recordPush(7);
      expect(tracker.peakDepth).toBe(10); // Still 10, not reduced
    });
  });

  describe('recordConsume', () => {
    it('increments totalConsumed', () => {
      const tracker = new MetricsTracker({ maxDepth: 100 });

      tracker.recordConsume(5);
      tracker.recordConsume(4);
      tracker.recordConsume(3);

      expect(tracker.totalConsumed).toBe(3);
    });

    it('does not update peakDepth', () => {
      const tracker = new MetricsTracker({ maxDepth: 100 });

      tracker.recordPush(10);
      expect(tracker.peakDepth).toBe(10);

      tracker.recordConsume(5);
      expect(tracker.peakDepth).toBe(10); // Unchanged
    });
  });

  describe('recordDroppedPush', () => {
    it('does not increment totalPushed', () => {
      const tracker = new MetricsTracker({ maxDepth: 100 });

      tracker.recordDroppedPush();
      tracker.recordDroppedPush();

      expect(tracker.totalPushed).toBe(0);
    });
  });

  describe('calculateUtilization', () => {
    it('calculates percentage correctly', () => {
      const tracker = new MetricsTracker({ maxDepth: 100 });

      expect(tracker.calculateUtilization(0)).toBe(0);
      expect(tracker.calculateUtilization(25)).toBe(25);
      expect(tracker.calculateUtilization(50)).toBe(50);
      expect(tracker.calculateUtilization(75)).toBe(75);
      expect(tracker.calculateUtilization(100)).toBe(100);
    });

    it('rounds to whole numbers', () => {
      const tracker = new MetricsTracker({ maxDepth: 100 });

      expect(tracker.calculateUtilization(33)).toBe(33);
      expect(tracker.calculateUtilization(66)).toBe(66);
    });

    it('handles non-100 maxDepth', () => {
      const tracker = new MetricsTracker({ maxDepth: 50 });

      expect(tracker.calculateUtilization(25)).toBe(50);
      expect(tracker.calculateUtilization(50)).toBe(100);
    });
  });

  describe('getMetrics', () => {
    it('returns complete metrics object', () => {
      const tracker = new MetricsTracker({ maxDepth: 100 });

      tracker.recordPush(10);
      tracker.recordPush(20);
      tracker.recordPush(30);
      tracker.recordConsume(25);

      const metrics = tracker.getMetrics(25, 5);

      expect(metrics).toEqual({
        currentDepth: 25,
        maxDepth: 100,
        peakDepth: 30,
        droppedCount: 5,
        totalPushed: 3,
        totalConsumed: 1,
        utilizationPercent: 25,
      });
    });
  });

  describe('reset', () => {
    it('resets all counters', () => {
      const tracker = new MetricsTracker({ maxDepth: 100 });

      tracker.recordPush(50);
      tracker.recordPush(60);
      tracker.recordConsume(40);

      tracker.reset(20);

      expect(tracker.peakDepth).toBe(20); // Current depth becomes new peak
      expect(tracker.totalPushed).toBe(0);
      expect(tracker.totalConsumed).toBe(0);
    });

    it('sets current depth as new peak', () => {
      const tracker = new MetricsTracker({ maxDepth: 100 });

      tracker.recordPush(80);
      expect(tracker.peakDepth).toBe(80);

      tracker.reset(30);

      expect(tracker.peakDepth).toBe(30);
    });
  });

  describe('high water mark events', () => {
    it('emits event when crossing high water mark threshold', () => {
      const onHighWaterMark = vi.fn();
      const tracker = new MetricsTracker({
        maxDepth: 100,
        highWaterMark: 80,
        onHighWaterMark,
      });

      // Below threshold - no event
      tracker.checkWaterMarks(79);
      expect(onHighWaterMark).not.toHaveBeenCalled();

      // At threshold - event
      tracker.checkWaterMarks(80);
      expect(onHighWaterMark).toHaveBeenCalledTimes(1);
      expect(onHighWaterMark).toHaveBeenCalledWith({
        currentDepth: 80,
        maxDepth: 100,
        utilizationPercent: 80,
      });
    });

    it('only emits once when staying above threshold', () => {
      const onHighWaterMark = vi.fn();
      const tracker = new MetricsTracker({
        maxDepth: 100,
        highWaterMark: 80,
        onHighWaterMark,
      });

      tracker.checkWaterMarks(80);
      tracker.checkWaterMarks(85);
      tracker.checkWaterMarks(90);
      tracker.checkWaterMarks(95);

      expect(onHighWaterMark).toHaveBeenCalledTimes(1);
    });

    it('emits again after falling below and rising above', () => {
      const onHighWaterMark = vi.fn();
      const onLowWaterMark = vi.fn();
      const tracker = new MetricsTracker({
        maxDepth: 100,
        highWaterMark: 80,
        lowWaterMark: 20,
        onHighWaterMark,
        onLowWaterMark,
      });

      // Rise above high water mark
      tracker.checkWaterMarks(85);
      expect(onHighWaterMark).toHaveBeenCalledTimes(1);

      // Fall below low water mark
      tracker.checkWaterMarks(15);
      expect(onLowWaterMark).toHaveBeenCalledTimes(1);

      // Rise above high water mark again
      tracker.checkWaterMarks(90);
      expect(onHighWaterMark).toHaveBeenCalledTimes(2);
    });
  });

  describe('low water mark events', () => {
    it('emits event when falling below low water mark after being above high', () => {
      const onHighWaterMark = vi.fn();
      const onLowWaterMark = vi.fn();
      const tracker = new MetricsTracker({
        maxDepth: 100,
        highWaterMark: 80,
        lowWaterMark: 20,
        onHighWaterMark,
        onLowWaterMark,
      });

      // Must first go above high water mark
      tracker.checkWaterMarks(85);
      expect(onHighWaterMark).toHaveBeenCalledTimes(1);

      // Not below low water mark yet
      tracker.checkWaterMarks(50);
      expect(onLowWaterMark).not.toHaveBeenCalled();

      // Now below low water mark
      tracker.checkWaterMarks(20);
      expect(onLowWaterMark).toHaveBeenCalledTimes(1);
      expect(onLowWaterMark).toHaveBeenCalledWith({
        currentDepth: 20,
        maxDepth: 100,
        utilizationPercent: 20,
      });
    });

    it('does not emit if never was above high water mark', () => {
      const onLowWaterMark = vi.fn();
      const tracker = new MetricsTracker({
        maxDepth: 100,
        highWaterMark: 80,
        lowWaterMark: 20,
        onLowWaterMark,
      });

      // Start low and stay low
      tracker.checkWaterMarks(10);
      tracker.checkWaterMarks(5);
      tracker.checkWaterMarks(0);

      expect(onLowWaterMark).not.toHaveBeenCalled();
    });

    it('only emits once when staying below threshold', () => {
      const onHighWaterMark = vi.fn();
      const onLowWaterMark = vi.fn();
      const tracker = new MetricsTracker({
        maxDepth: 100,
        highWaterMark: 80,
        lowWaterMark: 20,
        onHighWaterMark,
        onLowWaterMark,
      });

      tracker.checkWaterMarks(85);
      tracker.checkWaterMarks(15);
      tracker.checkWaterMarks(10);
      tracker.checkWaterMarks(5);

      expect(onLowWaterMark).toHaveBeenCalledTimes(1);
    });
  });

  describe('recordPush triggers water mark checks', () => {
    it('triggers high water mark on push', () => {
      const onHighWaterMark = vi.fn();
      const tracker = new MetricsTracker({
        maxDepth: 100,
        highWaterMark: 80,
        onHighWaterMark,
      });

      tracker.recordPush(80);

      expect(onHighWaterMark).toHaveBeenCalled();
    });
  });

  describe('recordConsume triggers water mark checks', () => {
    it('triggers low water mark on consume', () => {
      const onHighWaterMark = vi.fn();
      const onLowWaterMark = vi.fn();
      const tracker = new MetricsTracker({
        maxDepth: 100,
        highWaterMark: 80,
        lowWaterMark: 20,
        onHighWaterMark,
        onLowWaterMark,
      });

      // Go above high water mark first
      tracker.recordPush(85);

      // Now consume to below low water mark
      tracker.recordConsume(15);

      expect(onLowWaterMark).toHaveBeenCalled();
    });
  });

  describe('no callback provided', () => {
    it('handles missing highWaterMark callback gracefully', () => {
      const tracker = new MetricsTracker({
        maxDepth: 100,
        highWaterMark: 80,
        // No callback
      });

      // Should not throw
      expect(() => tracker.checkWaterMarks(90)).not.toThrow();
    });

    it('handles missing lowWaterMark callback gracefully', () => {
      const tracker = new MetricsTracker({
        maxDepth: 100,
        highWaterMark: 80,
        lowWaterMark: 20,
        onHighWaterMark: vi.fn(),
        // No lowWaterMark callback
      });

      tracker.checkWaterMarks(85);

      // Should not throw
      expect(() => tracker.checkWaterMarks(15)).not.toThrow();
    });

    it('handles no water marks configured', () => {
      const tracker = new MetricsTracker({ maxDepth: 100 });

      // Should not throw
      expect(() => {
        tracker.checkWaterMarks(0);
        tracker.checkWaterMarks(50);
        tracker.checkWaterMarks(100);
      }).not.toThrow();
    });
  });
});
