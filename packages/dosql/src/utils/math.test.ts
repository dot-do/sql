/**
 * Math Utilities Tests
 *
 * Tests for statistical calculation functions used throughout DoSQL.
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
} from './math.js';

// =============================================================================
// Percentile Calculation Tests
// =============================================================================

describe('calculatePercentile', () => {
  it('should return 0 for empty array', () => {
    expect(calculatePercentile([], 50)).toBe(0);
  });

  it('should return the only element for single-element array', () => {
    expect(calculatePercentile([42], 50)).toBe(42);
    expect(calculatePercentile([42], 0)).toBe(42);
    expect(calculatePercentile([42], 100)).toBe(42);
  });

  it('should calculate median (p50) correctly', () => {
    const sorted = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    expect(calculatePercentile(sorted, 50)).toBe(5);
  });

  it('should calculate p95 correctly', () => {
    const sorted = Array.from({ length: 100 }, (_, i) => i + 1);
    expect(calculatePercentile(sorted, 95)).toBe(95);
  });

  it('should calculate p99 correctly', () => {
    const sorted = Array.from({ length: 100 }, (_, i) => i + 1);
    expect(calculatePercentile(sorted, 99)).toBe(99);
  });

  it('should handle p0 and p100', () => {
    const sorted = [1, 2, 3, 4, 5];
    expect(calculatePercentile(sorted, 0)).toBe(1);
    expect(calculatePercentile(sorted, 100)).toBe(5);
  });

  it('should handle odd-length arrays', () => {
    const sorted = [10, 20, 30, 40, 50];
    expect(calculatePercentile(sorted, 50)).toBe(30);
  });

  it('should handle even-length arrays', () => {
    const sorted = [10, 20, 30, 40];
    expect(calculatePercentile(sorted, 50)).toBe(20);
  });
});

describe('calculatePercentileUnsorted', () => {
  it('should return 0 for empty array', () => {
    expect(calculatePercentileUnsorted([], 50)).toBe(0);
  });

  it('should sort and calculate percentile correctly', () => {
    const unsorted = [5, 3, 1, 4, 2];
    expect(calculatePercentileUnsorted(unsorted, 50)).toBe(3);
  });

  it('should not modify the original array', () => {
    const original = [5, 3, 1, 4, 2];
    const copy = [...original];
    calculatePercentileUnsorted(original, 50);
    expect(original).toEqual(copy);
  });

  it('should handle duplicates', () => {
    const values = [1, 1, 1, 5, 5, 5];
    expect(calculatePercentileUnsorted(values, 50)).toBe(1);
    expect(calculatePercentileUnsorted(values, 75)).toBe(5);
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
    const result = calculatePercentiles([], [50, 90, 99] as const);

    expect(result.p50).toBe(0);
    expect(result.p90).toBe(0);
    expect(result.p99).toBe(0);
  });

  it('should handle single percentile', () => {
    const sorted = [1, 2, 3, 4, 5];
    const result = calculatePercentiles(sorted, [50] as const);

    expect(result.p50).toBe(3);
  });
});

// =============================================================================
// Basic Statistics Tests
// =============================================================================

describe('calculateBasicStats', () => {
  it('should return zeros for empty array', () => {
    const result = calculateBasicStats([]);
    expect(result).toEqual({
      min: 0,
      max: 0,
      mean: 0,
      sum: 0,
      count: 0,
    });
  });

  it('should calculate stats for single element', () => {
    const result = calculateBasicStats([42]);
    expect(result).toEqual({
      min: 42,
      max: 42,
      mean: 42,
      sum: 42,
      count: 1,
    });
  });

  it('should calculate correct min and max', () => {
    const result = calculateBasicStats([5, 2, 8, 1, 9]);
    expect(result.min).toBe(1);
    expect(result.max).toBe(9);
  });

  it('should calculate correct mean', () => {
    const result = calculateBasicStats([10, 20, 30, 40, 50]);
    expect(result.mean).toBe(30);
  });

  it('should calculate correct sum', () => {
    const result = calculateBasicStats([1, 2, 3, 4, 5]);
    expect(result.sum).toBe(15);
  });

  it('should handle negative numbers', () => {
    const result = calculateBasicStats([-5, -3, 0, 3, 5]);
    expect(result.min).toBe(-5);
    expect(result.max).toBe(5);
    expect(result.mean).toBe(0);
  });

  it('should handle floating point numbers', () => {
    const result = calculateBasicStats([1.5, 2.5, 3.5]);
    expect(result.mean).toBeCloseTo(2.5);
    expect(result.sum).toBeCloseTo(7.5);
  });
});

// =============================================================================
// Standard Deviation Tests
// =============================================================================

describe('calculateStdDev', () => {
  it('should return 0 for empty array', () => {
    expect(calculateStdDev([])).toBe(0);
  });

  it('should return 0 for single element', () => {
    expect(calculateStdDev([42])).toBe(0);
  });

  it('should return 0 for identical elements', () => {
    expect(calculateStdDev([5, 5, 5, 5, 5])).toBe(0);
  });

  it('should calculate correct standard deviation', () => {
    // Using known values: stddev of [2, 4, 4, 4, 5, 5, 7, 9] is 2
    const values = [2, 4, 4, 4, 5, 5, 7, 9];
    const stdDev = calculateStdDev(values);
    expect(stdDev).toBeCloseTo(2, 1);
  });

  it('should accept pre-calculated mean', () => {
    const values = [1, 2, 3, 4, 5];
    const mean = 3;
    const stdDev = calculateStdDev(values, mean);
    expect(stdDev).toBeCloseTo(Math.sqrt(2), 5);
  });

  it('should handle floating point precision', () => {
    const values = [0.1, 0.2, 0.3, 0.4, 0.5];
    const stdDev = calculateStdDev(values);
    expect(stdDev).toBeGreaterThan(0);
  });
});

// =============================================================================
// Latency Statistics Tests
// =============================================================================

describe('calculateLatencyStatistics', () => {
  it('should return zeros for empty array', () => {
    const result = calculateLatencyStatistics([]);
    expect(result).toEqual({
      min: 0,
      max: 0,
      mean: 0,
      median: 0,
      p95: 0,
      p99: 0,
      stdDev: 0,
      count: 0,
    });
  });

  it('should calculate all statistics correctly', () => {
    const latencies = Array.from({ length: 100 }, (_, i) => i + 1);
    const result = calculateLatencyStatistics(latencies);

    expect(result.min).toBe(1);
    expect(result.max).toBe(100);
    expect(result.mean).toBeCloseTo(50.5);
    expect(result.median).toBe(50);
    expect(result.p95).toBe(95);
    expect(result.p99).toBe(99);
    expect(result.count).toBe(100);
    expect(result.stdDev).toBeGreaterThan(0);
  });

  it('should not modify the original array', () => {
    const original = [5, 3, 1, 4, 2];
    const copy = [...original];
    calculateLatencyStatistics(original);
    expect(original).toEqual(copy);
  });

  it('should handle realistic latency values', () => {
    // Simulate realistic latencies with mostly low values and a few outliers
    const latencies = [
      ...Array(90).fill(10),
      ...Array(8).fill(50),
      100,
      200,
    ];
    const result = calculateLatencyStatistics(latencies);

    expect(result.min).toBe(10);
    expect(result.max).toBe(200);
    expect(result.p99).toBeGreaterThan(result.p95);
  });
});

// =============================================================================
// Latency Histogram Tests
// =============================================================================

describe('calculateLatencyHistogram', () => {
  it('should return zeros for empty array', () => {
    const result = calculateLatencyHistogram([]);
    expect(result).toEqual({
      p50: 0,
      p90: 0,
      p99: 0,
      min: 0,
      max: 0,
      avg: 0,
      count: 0,
    });
  });

  it('should calculate histogram correctly', () => {
    const latencies = Array.from({ length: 100 }, (_, i) => i + 1);
    const result = calculateLatencyHistogram(latencies);

    expect(result.min).toBe(1);
    expect(result.max).toBe(100);
    expect(result.p50).toBe(50);
    expect(result.p90).toBe(90);
    expect(result.p99).toBe(99);
    expect(result.avg).toBeCloseTo(50.5);
    expect(result.count).toBe(100);
  });

  it('should handle single element', () => {
    const result = calculateLatencyHistogram([42]);

    expect(result.min).toBe(42);
    expect(result.max).toBe(42);
    expect(result.p50).toBe(42);
    expect(result.avg).toBe(42);
    expect(result.count).toBe(1);
  });

  it('should calculate correct average', () => {
    const latencies = [10, 20, 30];
    const result = calculateLatencyHistogram(latencies);

    expect(result.avg).toBe(20);
  });
});
