/**
 * WAL Retention Metrics Module
 *
 * Handles metrics collection, health checks, cleanup history,
 * growth statistics, and replication status monitoring.
 *
 * @packageDocumentation
 */

import type { WALReader } from '../types.js';
import type { ReplicationSlotManager } from '../../cdc/types.js';
import type {
  RetentionPolicy,
  RetentionMetrics,
  CleanupRecord,
  HealthCheckResult,
  DynamicPolicyResult,
  ReplicationStatus,
  RegionReplicationStatus,
  CleanupLatencyHistogram,
  GrowthStats,
  StorageStats,
  EntryStats,
  MetricsReporter,
  RetentionMetric,
} from '../retention-types.js';

// =============================================================================
// Metrics Collector
// =============================================================================

/**
 * Metrics collector state
 */
export interface MetricsCollectorState {
  /** Last cleanup timestamp */
  lastCleanupTime: number | null;
  /** Segments deleted in last cleanup */
  lastSegmentsDeleted: number;
  /** Bytes freed in last cleanup */
  lastBytesFreed: number;
  /** Cleanup history records */
  cleanupHistory: CleanupRecord[];
  /** Region replication status */
  regionStatus: RegionReplicationStatus;
}

/**
 * Create a metrics collector
 *
 * @param policy - Retention policy
 * @returns Metrics collector instance
 */
export function createMetricsCollector(policy: RetentionPolicy): MetricsCollector {
  const state: MetricsCollectorState = {
    lastCleanupTime: null,
    lastSegmentsDeleted: 0,
    lastBytesFreed: 0,
    cleanupHistory: [],
    regionStatus: {},
  };

  // Initialize region status
  if (policy.regions) {
    for (const region of policy.regions) {
      state.regionStatus[region] = { lastLSN: 0n, lag: 0, healthy: true };
    }
  }

  return new MetricsCollectorImpl(state, policy);
}

// =============================================================================
// Metrics Collector Interface
// =============================================================================

/**
 * Metrics collector interface
 */
export interface MetricsCollector {
  /**
   * Record a cleanup operation
   */
  recordCleanup(record: CleanupRecord): void;

  /**
   * Get cleanup history
   */
  getCleanupHistory(): CleanupRecord[];

  /**
   * Get retention metrics
   */
  getMetrics(stats: StorageStats, oldestSegmentAge: number | null): RetentionMetrics;

  /**
   * Get cleanup latency histogram
   */
  getCleanupLatencyHistogram(): CleanupLatencyHistogram;

  /**
   * Get growth statistics
   */
  getGrowthStats(
    stats: StorageStats,
    entryStats: EntryStats
  ): GrowthStats;

  /**
   * Report a metric to external reporter
   */
  reportMetric(type: string, value: number, labels?: Record<string, string>): void;

  /**
   * Update region replication status
   */
  updateRegionStatus(region: string, lastLSN: bigint, lag: number, healthy: boolean): void;

  /**
   * Get region replication status
   */
  getRegionReplicationStatus(): RegionReplicationStatus;

  /**
   * Get last cleanup time
   */
  getLastCleanupTime(): number | null;

  /**
   * Get last cleanup stats
   */
  getLastCleanupStats(): { segmentsDeleted: number; bytesFreed: number };
}

// =============================================================================
// Metrics Collector Implementation
// =============================================================================

/**
 * Metrics collector implementation
 */
class MetricsCollectorImpl implements MetricsCollector {
  private state: MetricsCollectorState;
  private policy: RetentionPolicy;
  private maxHistorySize: number;

  constructor(state: MetricsCollectorState, policy: RetentionPolicy) {
    this.state = state;
    this.policy = policy;
    this.maxHistorySize = policy.cleanupHistorySize ?? 100;
  }

  recordCleanup(record: CleanupRecord): void {
    this.state.lastCleanupTime = record.timestamp;
    this.state.lastSegmentsDeleted = record.segmentsDeleted;
    this.state.lastBytesFreed = record.bytesFreed;

    this.state.cleanupHistory.push(record);
    if (this.state.cleanupHistory.length > this.maxHistorySize) {
      this.state.cleanupHistory.shift();
    }

    // Report metrics
    this.reportMetric('cleanup_segments_deleted', record.segmentsDeleted);
    this.reportMetric('cleanup_bytes_freed', record.bytesFreed);
    this.reportMetric('cleanup_duration_ms', record.durationMs);
  }

  getCleanupHistory(): CleanupRecord[] {
    return [...this.state.cleanupHistory];
  }

  getMetrics(stats: StorageStats, oldestSegmentAge: number | null): RetentionMetrics {
    return {
      totalSegments: stats.segmentCount,
      totalBytes: stats.totalBytes,
      oldestSegmentAge,
      lastCleanupTime: this.state.lastCleanupTime,
      segmentsDeleted: this.state.lastSegmentsDeleted,
      bytesFreed: this.state.lastBytesFreed,
    };
  }

  getCleanupLatencyHistogram(): CleanupLatencyHistogram {
    const latencies = this.state.cleanupHistory.map((r) => r.durationMs);

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

    const percentile = (p: number): number => {
      const index = Math.ceil((p / 100) * count) - 1;
      const val = sorted[Math.max(0, Math.min(index, count - 1))];
      return val ?? 0;
    };

    const minVal = sorted[0] ?? 0;
    const maxVal = sorted[count - 1] ?? 0;

    return {
      p50: percentile(50),
      p90: percentile(90),
      p99: percentile(99),
      min: minVal,
      max: maxVal,
      avg: latencies.reduce((a, b) => a + b, 0) / count,
      count,
    };
  }

  getGrowthStats(stats: StorageStats, entryStats: EntryStats): GrowthStats {
    const history = this.state.cleanupHistory;

    // If not enough history, return zeros
    if (history.length < 2) {
      return {
        bytesPerHour: 0,
        segmentsPerHour: 0,
        entriesPerHour: 0,
        estimatedTimeToLimit: null,
      };
    }

    // Calculate time span and deltas from history
    const oldestRecord = history[0];
    const newestRecord = history[history.length - 1];
    if (!oldestRecord || !newestRecord) {
      return {
        bytesPerHour: 0,
        segmentsPerHour: 0,
        entriesPerHour: 0,
        estimatedTimeToLimit: null,
      };
    }
    const timeSpanHours = (newestRecord.timestamp - oldestRecord.timestamp) / (1000 * 60 * 60);

    if (timeSpanHours <= 0) {
      return {
        bytesPerHour: 0,
        segmentsPerHour: 0,
        entriesPerHour: 0,
        estimatedTimeToLimit: null,
      };
    }

    // Sum bytes freed as proxy for growth rate
    const totalBytesFreed = history.reduce((sum, r) => sum + r.bytesFreed, 0);
    const bytesPerHour = totalBytesFreed / timeSpanHours;

    // Estimate time to limit
    let estimatedTimeToLimit: number | null = null;
    if (this.policy.maxTotalBytes && bytesPerHour > 0) {
      const remainingBytes = this.policy.maxTotalBytes - stats.totalBytes;
      if (remainingBytes > 0) {
        estimatedTimeToLimit = remainingBytes / bytesPerHour;
      } else {
        estimatedTimeToLimit = 0;
      }
    }

    return {
      bytesPerHour,
      segmentsPerHour: stats.segmentCount / Math.max(timeSpanHours, 1),
      entriesPerHour: entryStats.totalEntries / Math.max(timeSpanHours, 1),
      estimatedTimeToLimit,
    };
  }

  reportMetric(type: string, value: number, labels?: Record<string, string>): void {
    if (this.policy.metricsReporter) {
      const metric: RetentionMetric = {
        type,
        value,
        timestamp: Date.now(),
      };
      if (labels) {
        metric.labels = labels;
      }
      this.policy.metricsReporter.report(metric);
    }
  }

  updateRegionStatus(region: string, lastLSN: bigint, lag: number, healthy: boolean): void {
    this.state.regionStatus[region] = { lastLSN, lag, healthy };
  }

  getRegionReplicationStatus(): RegionReplicationStatus {
    return { ...this.state.regionStatus };
  }

  getLastCleanupTime(): number | null {
    return this.state.lastCleanupTime;
  }

  getLastCleanupStats(): { segmentsDeleted: number; bytesFreed: number } {
    return {
      segmentsDeleted: this.state.lastSegmentsDeleted,
      bytesFreed: this.state.lastBytesFreed,
    };
  }
}

// =============================================================================
// Health Check
// =============================================================================

/**
 * Perform a health check on the retention system
 *
 * @param policy - Retention policy
 * @param stats - Current storage stats
 * @param oldestSegmentAge - Age of oldest segment in ms
 * @param fragmentationRatio - Current fragmentation ratio
 * @returns Health check result
 */
export function performHealthCheck(
  policy: RetentionPolicy,
  stats: StorageStats,
  oldestSegmentAge: number | null,
  fragmentationRatio: number
): HealthCheckResult {
  const issues: string[] = [];
  const recommendations: string[] = [];
  let status: 'healthy' | 'warning' | 'critical' = 'healthy';

  // Check storage usage
  if (policy.maxTotalBytes) {
    const usage = stats.totalBytes / policy.maxTotalBytes;
    if (usage >= 0.95) {
      status = 'critical';
      issues.push('Storage usage exceeds 95%');
      recommendations.push('Consider increasing maxTotalBytes or running cleanup immediately');
    } else if (usage >= 0.8) {
      status = 'warning';
      issues.push('Storage usage exceeds 80%');
      recommendations.push('Consider adjusting retention policy');
    }
  }

  // Check segment age
  if (oldestSegmentAge !== null) {
    const maxAge = policy.maxSegmentAge;
    if (oldestSegmentAge > maxAge * 2) {
      if (status !== 'critical') status = 'warning';
      issues.push('Segments older than twice the retention period exist');
      recommendations.push('Check if cleanup is running properly');
    }
  }

  // Check fragmentation
  if (fragmentationRatio > 0.5) {
    if (status !== 'critical') status = 'warning';
    issues.push(`High fragmentation (${(fragmentationRatio * 100).toFixed(1)}%)`);
    recommendations.push('Consider running compaction');
  }

  // Check segment count
  if (stats.segmentCount < policy.minSegmentCount) {
    // This isn't necessarily bad, just informational
  }

  return { status, issues, recommendations };
}

// =============================================================================
// Dynamic Policy Evaluation
// =============================================================================

/**
 * Evaluate and apply dynamic policy adjustments
 *
 * @param policy - Retention policy (mutable)
 * @param stats - Current storage stats
 * @returns Dynamic policy result
 */
export function evaluateDynamicPolicy(
  policy: RetentionPolicy,
  stats: StorageStats
): DynamicPolicyResult {
  if (!policy.dynamicPolicy) {
    return { applied: false, reason: 'Dynamic policy not configured' };
  }

  if (!policy.maxTotalBytes) {
    return { applied: false, reason: 'maxTotalBytes not configured' };
  }

  const usage = stats.totalBytes / policy.maxTotalBytes;
  if (usage >= policy.dynamicPolicy.storageThreshold) {
    // Apply reduced retention
    policy.maxSegmentAge = policy.dynamicPolicy.reducedRetentionHours * 60 * 60 * 1000;
    return {
      applied: true,
      reason: `Storage usage (${(usage * 100).toFixed(1)}%) exceeded threshold, reduced retention to ${policy.dynamicPolicy.reducedRetentionHours} hours`,
    };
  }

  return { applied: false, reason: 'Storage usage within threshold' };
}

// =============================================================================
// Replication Status
// =============================================================================

/**
 * Get replication status
 *
 * @param slotManager - Replication slot manager
 * @param reader - WAL reader
 * @param policy - Retention policy
 * @returns Replication status
 */
export async function getReplicationStatus(
  slotManager: ReplicationSlotManager | null,
  reader: WALReader,
  policy: RetentionPolicy
): Promise<ReplicationStatus> {
  if (!slotManager) {
    return { currentLag: 0, isWithinTolerance: true };
  }

  const slots = await slotManager.listSlots();
  if (slots.length === 0) {
    return { currentLag: 0, isWithinTolerance: true };
  }

  // Get current WAL position (from latest segment)
  const segments = await reader.listSegments(false);
  let currentLSN = 0n;
  if (segments.length > 0) {
    const lastSegmentId = segments[segments.length - 1];
    if (lastSegmentId) {
      const lastSegment = await reader.readSegment(lastSegmentId);
      if (lastSegment) {
        currentLSN = lastSegment.endLSN;
      }
    }
  }

  // Calculate max lag across all slots
  let maxLag = 0;
  for (const slot of slots) {
    const lag = Number(currentLSN - slot.acknowledgedLSN);
    if (lag > maxLag) {
      maxLag = lag;
    }
  }

  const tolerance = policy.replicationLagTolerance ?? 1000;
  return {
    currentLag: maxLag,
    isWithinTolerance: maxLag <= tolerance,
  };
}

// =============================================================================
// Oldest Segment Age
// =============================================================================

/**
 * Get the age of the oldest segment
 *
 * @param reader - WAL reader
 * @returns Age in milliseconds or null if no segments
 */
export async function getOldestSegmentAge(reader: WALReader): Promise<number | null> {
  const segments = await reader.listSegments(false);
  let oldestAge: number | null = null;

  for (const segmentId of segments) {
    const segment = await reader.readSegment(segmentId);
    if (segment) {
      const age = Date.now() - segment.createdAt;
      if (oldestAge === null || age > oldestAge) {
        oldestAge = age;
      }
    }
  }

  return oldestAge;
}

// =============================================================================
// Metric Helpers
// =============================================================================

/**
 * Create a metrics reporter that logs to console
 *
 * @param prefix - Prefix for log messages
 * @returns Metrics reporter
 */
export function createConsoleMetricsReporter(prefix: string = '[WAL Retention]'): MetricsReporter {
  return {
    report: (metric: RetentionMetric) => {
      const labels = metric.labels
        ? Object.entries(metric.labels)
            .map(([k, v]) => `${k}=${v}`)
            .join(',')
        : '';
      console.log(
        `${prefix} ${metric.type}=${metric.value}${labels ? ` {${labels}}` : ''} @${new Date(metric.timestamp).toISOString()}`
      );
    },
  };
}

/**
 * Create a no-op metrics reporter
 *
 * @returns No-op metrics reporter
 */
export function createNoopMetricsReporter(): MetricsReporter {
  return {
    report: () => {
      // No-op
    },
  };
}

/**
 * Create a buffered metrics reporter that batches metrics
 *
 * @param flushFn - Function to call when flushing
 * @param batchSize - Number of metrics to buffer before flushing
 * @returns Metrics reporter with flush method
 */
export function createBufferedMetricsReporter(
  flushFn: (metrics: RetentionMetric[]) => void,
  batchSize: number = 100
): MetricsReporter & { flush: () => void } {
  const buffer: RetentionMetric[] = [];

  const flush = () => {
    if (buffer.length > 0) {
      flushFn([...buffer]);
      buffer.length = 0;
    }
  };

  return {
    report: (metric: RetentionMetric) => {
      buffer.push(metric);
      if (buffer.length >= batchSize) {
        flush();
      }
    },
    flush,
  };
}
