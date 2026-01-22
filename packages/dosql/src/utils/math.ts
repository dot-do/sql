/**
 * Math Utilities
 *
 * Shared mathematical functions for statistics and calculations.
 */

// =============================================================================
// PERCENTILE CALCULATIONS
// =============================================================================

/**
 * Calculate the value at a given percentile from a sorted array.
 *
 * @param sortedArray - Pre-sorted array of numbers (ascending order)
 * @param percentile - Percentile value (0-100)
 * @returns The value at the given percentile
 *
 * @example
 * ```typescript
 * const sorted = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10].sort((a, b) => a - b);
 * calculatePercentile(sorted, 50); // median
 * calculatePercentile(sorted, 95); // p95
 * calculatePercentile(sorted, 99); // p99
 * ```
 */
export function calculatePercentile(sortedArray: number[], percentile: number): number {
  if (sortedArray.length === 0) {
    return 0;
  }

  const index = Math.ceil((percentile / 100) * sortedArray.length) - 1;
  return sortedArray[Math.max(0, Math.min(index, sortedArray.length - 1))];
}

/**
 * Calculate the value at a given percentile from an unsorted array.
 * This function will sort the array internally.
 *
 * @param values - Array of numbers (will be sorted internally)
 * @param percentile - Percentile value (0-100)
 * @returns The value at the given percentile
 */
export function calculatePercentileUnsorted(values: number[], percentile: number): number {
  if (values.length === 0) {
    return 0;
  }

  const sorted = [...values].sort((a, b) => a - b);
  return calculatePercentile(sorted, percentile);
}

/**
 * Calculate multiple percentiles at once from a sorted array.
 *
 * @param sortedArray - Pre-sorted array of numbers (ascending order)
 * @param percentiles - Array of percentile values (0-100)
 * @returns Object mapping percentile names to values
 *
 * @example
 * ```typescript
 * const sorted = latencies.sort((a, b) => a - b);
 * const { p50, p90, p99 } = calculatePercentiles(sorted, [50, 90, 99]);
 * ```
 */
export function calculatePercentiles<P extends number>(
  sortedArray: number[],
  percentiles: readonly P[]
): Record<`p${P}`, number> {
  const result = {} as Record<`p${P}`, number>;

  for (const p of percentiles) {
    const key = `p${p}` as `p${P}`;
    result[key] = calculatePercentile(sortedArray, p);
  }

  return result;
}

// =============================================================================
// STATISTICAL CALCULATIONS
// =============================================================================

/**
 * Calculate basic statistics from an array of numbers.
 */
export interface BasicStats {
  min: number;
  max: number;
  mean: number;
  sum: number;
  count: number;
}

/**
 * Calculate basic statistics from an array of numbers.
 *
 * @param values - Array of numbers
 * @returns Basic statistical measures
 */
export function calculateBasicStats(values: number[]): BasicStats {
  if (values.length === 0) {
    return { min: 0, max: 0, mean: 0, sum: 0, count: 0 };
  }

  const sum = values.reduce((acc, val) => acc + val, 0);
  let min = values[0];
  let max = values[0];

  for (let i = 1; i < values.length; i++) {
    if (values[i] < min) min = values[i];
    if (values[i] > max) max = values[i];
  }

  return {
    min,
    max,
    mean: sum / values.length,
    sum,
    count: values.length,
  };
}

/**
 * Calculate standard deviation from an array of numbers.
 *
 * @param values - Array of numbers
 * @param mean - Optional pre-calculated mean (will be calculated if not provided)
 * @returns Standard deviation
 */
export function calculateStdDev(values: number[], mean?: number): number {
  if (values.length === 0) {
    return 0;
  }

  const avg = mean ?? values.reduce((acc, val) => acc + val, 0) / values.length;
  const squaredDiffs = values.map((val) => Math.pow(val - avg, 2));
  const avgSquaredDiff = squaredDiffs.reduce((acc, val) => acc + val, 0) / values.length;

  return Math.sqrt(avgSquaredDiff);
}

/**
 * Latency statistics with common percentiles.
 */
export interface LatencyStatistics {
  min: number;
  max: number;
  mean: number;
  median: number;
  p95: number;
  p99: number;
  stdDev: number;
  count: number;
}

/**
 * Calculate comprehensive latency statistics.
 *
 * @param durations - Array of duration/latency values
 * @returns Comprehensive latency statistics
 */
export function calculateLatencyStatistics(durations: number[]): LatencyStatistics {
  if (durations.length === 0) {
    return {
      min: 0,
      max: 0,
      mean: 0,
      median: 0,
      p95: 0,
      p99: 0,
      stdDev: 0,
      count: 0,
    };
  }

  const sorted = [...durations].sort((a, b) => a - b);
  const sum = sorted.reduce((acc, val) => acc + val, 0);
  const mean = sum / sorted.length;

  return {
    min: sorted[0],
    max: sorted[sorted.length - 1],
    mean,
    median: calculatePercentile(sorted, 50),
    p95: calculatePercentile(sorted, 95),
    p99: calculatePercentile(sorted, 99),
    stdDev: calculateStdDev(sorted, mean),
    count: sorted.length,
  };
}

/**
 * Histogram-style latency stats (p50, p90, p99).
 */
export interface LatencyHistogram {
  p50: number;
  p90: number;
  p99: number;
  min: number;
  max: number;
  avg: number;
  count: number;
}

/**
 * Calculate histogram-style latency statistics.
 *
 * @param latencies - Array of latency values
 * @returns Histogram-style latency statistics
 */
export function calculateLatencyHistogram(latencies: number[]): LatencyHistogram {
  if (latencies.length === 0) {
    return {
      p50: 0,
      p90: 0,
      p99: 0,
      min: 0,
      max: 0,
      avg: 0,
      count: 0,
    };
  }

  const sorted = [...latencies].sort((a, b) => a - b);
  const count = sorted.length;

  return {
    p50: calculatePercentile(sorted, 50),
    p90: calculatePercentile(sorted, 90),
    p99: calculatePercentile(sorted, 99),
    min: sorted[0],
    max: sorted[count - 1],
    avg: latencies.reduce((a, b) => a + b, 0) / count,
    count,
  };
}
