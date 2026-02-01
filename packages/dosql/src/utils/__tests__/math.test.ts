/**
 * Math Utilities Tests
 *
 * Tests for percentile calculations, statistical functions,
 * and latency analysis utilities.
 */

import { describe, it, expect } from 'vitest';
import {
  calculatePercentile,
  calculatePercentileUnsorted,
  calculatePercentiles,
  calculateBasicStats,
  calculateStdDev,
  calculateLatencyStatistics,
  calculateLatencyHistogram,
} from '../math.js';

// =============================================================================
// PERCENTILE CALCULATIONS
// =============================================================================

describe('calculatePercentile', () => {
  it('should return 0 for empty array', () => {
    expect(calculatePercentile([], 50)).toBe(0);
  });

  it('should return the single element for single-element array', () => {
    expect(calculatePercentile([42], 50)).toBe(42);
    expect(calculatePercentile([42], 0)).toBe(42);
    expect(calculatePercentile([42], 100)).toBe(42);
  });

  it('should calculate median (p50) of sorted array', () => {
    const sorted = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    expect(calculatePercentile(sorted, 50)).toBe(5);
  });

  it('should calculate p95', () => {
    const sorted = Array.from({ length: 100 }, (_, i) => i + 1);
    expect(calculatePercentile(sorted, 95)).toBe(95);
  });

  it('should calculate p99', () => {
    const sorted = Array.from({ length: 100 }, (_, i) => i + 1);
    expect(calculatePercentile(sorted, 99)).toBe(99);
  });

  it('should clamp to first element for percentile 0', () => {
    const sorted = [10, 20, 30];
    expect(calculatePercentile(sorted, 0)).toBe(10);
  });

  it('should return last element for percentile 100', () => {
    const sorted = [10, 20, 30];
    expect(calculatePercentile(sorted, 100)).toBe(30);
  });
});

describe('calculatePercentileUnsorted', () => {
  it('should return 0 for empty array', () => {
    expect(calculatePercentileUnsorted([], 50)).toBe(0);
  });

  it('should sort and calculate percentile', () => {
    const unsorted = [5, 3, 1, 4, 2];
    expect(calculatePercentileUnsorted(unsorted, 50)).toBe(3);
  });

  it('should not modify the original array', () => {
    const original = [5, 3, 1, 4, 2];
    const copy = [...original];
    calculatePercentileUnsorted(original, 50);
    expect(original).toEqual(copy);
  });
});

describe('calculatePercentiles', () => {
  it('should calculate multiple percentiles at once', () => {
    const sorted = Array.from({ length: 100 }, (_, i) => i + 1);
    const result = calculatePercentiles(sorted, [50, 90, 99] as const);
    expect(result.p50).toBe(50);
    expect(result.p90).toBe(90);
    expect(result.p99).toBe(99);
  });

  it('should handle empty array', () => {
    const result = calculatePercentiles([], [50, 90] as const);
    expect(result.p50).toBe(0);
    expect(result.p90).toBe(0);
  });
});

// =============================================================================
// STATISTICAL CALCULATIONS
// =============================================================================

describe('calculateBasicStats', () => {
  it('should return zeros for empty array', () => {
    const stats = calculateBasicStats([]);
    expect(stats).toEqual({ min: 0, max: 0, mean: 0, sum: 0, count: 0 });
  });

  it('should calculate stats for a single value', () => {
    const stats = calculateBasicStats([42]);
    expect(stats.min).toBe(42);
    expect(stats.max).toBe(42);
    expect(stats.mean).toBe(42);
    expect(stats.sum).toBe(42);
    expect(stats.count).toBe(1);
  });

  it('should calculate stats for multiple values', () => {
    const stats = calculateBasicStats([10, 20, 30, 40, 50]);
    expect(stats.min).toBe(10);
    expect(stats.max).toBe(50);
    expect(stats.mean).toBe(30);
    expect(stats.sum).toBe(150);
    expect(stats.count).toBe(5);
  });

  it('should handle negative values', () => {
    const stats = calculateBasicStats([-10, -5, 0, 5, 10]);
    expect(stats.min).toBe(-10);
    expect(stats.max).toBe(10);
    expect(stats.mean).toBe(0);
    expect(stats.sum).toBe(0);
  });
});

describe('calculateStdDev', () => {
  it('should return 0 for empty array', () => {
    expect(calculateStdDev([])).toBe(0);
  });

  it('should return 0 for identical values', () => {
    expect(calculateStdDev([5, 5, 5, 5])).toBe(0);
  });

  it('should calculate standard deviation', () => {
    // Values: [2, 4, 4, 4, 5, 5, 7, 9], mean=5, stddev=2
    const values = [2, 4, 4, 4, 5, 5, 7, 9];
    const stddev = calculateStdDev(values);
    expect(stddev).toBeCloseTo(2, 0);
  });

  it('should accept pre-calculated mean', () => {
    const values = [2, 4, 4, 4, 5, 5, 7, 9];
    const stddev = calculateStdDev(values, 5);
    expect(stddev).toBeCloseTo(2, 0);
  });

  it('should return 0 for single value', () => {
    expect(calculateStdDev([42])).toBe(0);
  });
});

// =============================================================================
// LATENCY STATISTICS
// =============================================================================

describe('calculateLatencyStatistics', () => {
  it('should return zeros for empty array', () => {
    const stats = calculateLatencyStatistics([]);
    expect(stats.min).toBe(0);
    expect(stats.max).toBe(0);
    expect(stats.mean).toBe(0);
    expect(stats.median).toBe(0);
    expect(stats.p95).toBe(0);
    expect(stats.p99).toBe(0);
    expect(stats.stdDev).toBe(0);
    expect(stats.count).toBe(0);
  });

  it('should calculate comprehensive latency stats', () => {
    const durations = Array.from({ length: 100 }, (_, i) => i + 1);
    const stats = calculateLatencyStatistics(durations);
    expect(stats.min).toBe(1);
    expect(stats.max).toBe(100);
    expect(stats.mean).toBeCloseTo(50.5);
    expect(stats.count).toBe(100);
    expect(stats.p95).toBe(95);
    expect(stats.p99).toBe(99);
    expect(stats.median).toBe(50);
    expect(stats.stdDev).toBeGreaterThan(0);
  });

  it('should not modify the original array', () => {
    const original = [5, 3, 1, 4, 2];
    const copy = [...original];
    calculateLatencyStatistics(original);
    expect(original).toEqual(copy);
  });
});

describe('calculateLatencyHistogram', () => {
  it('should return zeros for empty array', () => {
    const hist = calculateLatencyHistogram([]);
    expect(hist.p50).toBe(0);
    expect(hist.p90).toBe(0);
    expect(hist.p99).toBe(0);
    expect(hist.min).toBe(0);
    expect(hist.max).toBe(0);
    expect(hist.avg).toBe(0);
    expect(hist.count).toBe(0);
  });

  it('should calculate histogram stats', () => {
    const latencies = Array.from({ length: 100 }, (_, i) => i + 1);
    const hist = calculateLatencyHistogram(latencies);
    expect(hist.min).toBe(1);
    expect(hist.max).toBe(100);
    expect(hist.avg).toBeCloseTo(50.5);
    expect(hist.count).toBe(100);
    expect(hist.p50).toBe(50);
    expect(hist.p90).toBe(90);
    expect(hist.p99).toBe(99);
  });

  it('should handle single-element array', () => {
    const hist = calculateLatencyHistogram([42]);
    expect(hist.min).toBe(42);
    expect(hist.max).toBe(42);
    expect(hist.avg).toBe(42);
    expect(hist.p50).toBe(42);
    expect(hist.count).toBe(1);
  });
});
