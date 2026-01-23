/**
 * WAL Retention Policy Module
 *
 * Contains policy definitions, presets, parsing utilities, and validation logic.
 *
 * @packageDocumentation
 */

import type {
  RetentionPolicy,
  RetentionWarning,
  LowActivityWindow,
} from '../retention-types.js';

// =============================================================================
// Default Policy & Presets
// =============================================================================

/**
 * Default retention policy
 */
export const DEFAULT_RETENTION_POLICY: RetentionPolicy = {
  minSegmentCount: 2,
  maxSegmentAge: 24 * 60 * 60 * 1000, // 24 hours
  respectSlotPositions: true,
  readerIdleTimeout: 5 * 60 * 1000, // 5 minutes
  archiveBeforeDelete: true,
};

/**
 * Retention policy presets
 */
export const RETENTION_PRESETS: Record<string, RetentionPolicy> = {
  aggressive: {
    ...DEFAULT_RETENTION_POLICY,
    minSegmentCount: 1,
    maxSegmentAge: 1 * 60 * 60 * 1000, // 1 hour
    retentionHours: 1,
    maxTotalBytes: 50 * 1024 * 1024, // 50MB
    maxEntryCount: 10000,
    archiveBeforeDelete: false,
    compactionThreshold: 0.2,
    autoCompaction: true,
  },
  balanced: {
    ...DEFAULT_RETENTION_POLICY,
    minSegmentCount: 3,
    maxSegmentAge: 24 * 60 * 60 * 1000, // 24 hours
    retentionHours: 24,
    maxTotalBytes: 500 * 1024 * 1024, // 500MB
    maxEntryCount: 100000,
    compactionThreshold: 0.3,
    autoCompaction: true,
  },
  conservative: {
    ...DEFAULT_RETENTION_POLICY,
    minSegmentCount: 5,
    maxSegmentAge: 7 * 24 * 60 * 60 * 1000, // 7 days
    retentionDays: 7,
    maxTotalBytes: 2 * 1024 * 1024 * 1024, // 2GB
    maxEntryCount: 1000000,
    compactionThreshold: 0.5,
    autoCompaction: false,
  },
};

// =============================================================================
// Size Parsing
// =============================================================================

/**
 * Parse human-readable size string to bytes
 *
 * @param size - Size string like "100MB", "1GB", "500KB"
 * @returns Size in bytes
 * @throws Error if format is invalid
 *
 * @example
 * ```typescript
 * parseSizeString("100MB") // 104857600
 * parseSizeString("1GB")   // 1073741824
 * parseSizeString("500KB") // 512000
 * ```
 */
export function parseSizeString(size: string): number {
  const match = size.match(/^(\d+(?:\.\d+)?)\s*(B|KB|MB|GB|TB)$/i);
  if (!match || !match[1] || !match[2]) {
    throw new Error(`Invalid size format: ${size}`);
  }

  const value = parseFloat(match[1]);
  const unit = match[2].toUpperCase();

  const multipliers: Record<string, number> = {
    B: 1,
    KB: 1024,
    MB: 1024 * 1024,
    GB: 1024 * 1024 * 1024,
    TB: 1024 * 1024 * 1024 * 1024,
  };

  const multiplier = multipliers[unit];
  if (multiplier === undefined) {
    throw new Error(`Invalid size unit: ${unit}`);
  }

  return Math.floor(value * multiplier);
}

/**
 * Format bytes to human-readable size string
 *
 * @param bytes - Size in bytes
 * @returns Human-readable string
 *
 * @example
 * ```typescript
 * formatSizeString(104857600)  // "100.00 MB"
 * formatSizeString(1073741824) // "1.00 GB"
 * ```
 */
export function formatSizeString(bytes: number): string {
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let unitIndex = 0;
  let size = bytes;

  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex++;
  }

  return `${size.toFixed(2)} ${units[unitIndex]}`;
}

// =============================================================================
// Time Window Utilities
// =============================================================================

/**
 * Check if current time is within a time window
 *
 * @param window - Time window configuration
 * @returns True if current time is within the window
 *
 * @example
 * ```typescript
 * isInTimeWindow({ start: '02:00', end: '06:00' }) // true if between 2am and 6am
 * isInTimeWindow({ start: '22:00', end: '04:00' }) // handles midnight crossing
 * ```
 */
export function isInTimeWindow(window: LowActivityWindow): boolean {
  const now = new Date();
  const currentTime = now.getHours() * 60 + now.getMinutes();

  const startParts = window.start.split(':').map(Number);
  const endParts = window.end.split(':').map(Number);

  const startHour = startParts[0] ?? 0;
  const startMin = startParts[1] ?? 0;
  const endHour = endParts[0] ?? 0;
  const endMin = endParts[1] ?? 0;

  const startTime = startHour * 60 + startMin;
  const endTime = endHour * 60 + endMin;

  if (startTime <= endTime) {
    return currentTime >= startTime && currentTime <= endTime;
  } else {
    // Window crosses midnight
    return currentTime >= startTime || currentTime <= endTime;
  }
}

/**
 * Get time until next window start
 *
 * @param window - Time window configuration
 * @returns Milliseconds until window start
 */
export function getTimeUntilWindow(window: LowActivityWindow): number {
  const now = new Date();
  const currentTime = now.getHours() * 60 + now.getMinutes();

  const startParts = window.start.split(':').map(Number);
  const startHour = startParts[0] ?? 0;
  const startMin = startParts[1] ?? 0;
  const startTime = startHour * 60 + startMin;

  let minutesUntil: number;
  if (currentTime < startTime) {
    minutesUntil = startTime - currentTime;
  } else {
    // Window is tomorrow
    minutesUntil = 24 * 60 - currentTime + startTime;
  }

  return minutesUntil * 60 * 1000;
}

// =============================================================================
// Cron Parsing
// =============================================================================

/**
 * Parse cron expression and get next run time
 * Simplified implementation supporting common patterns
 *
 * @param cronExpression - Cron expression (minute hour day month weekday)
 * @returns Next scheduled run time
 *
 * @example
 * getNextCronTime('0 6 * * *') returns next 6 AM
 * getNextCronTime('30 2 * * *') returns next 2:30 AM
 */
export function getNextCronTime(cronExpression: string): Date {
  // Format: minute hour day month weekday
  const parts = cronExpression.split(' ');
  if (parts.length !== 5) {
    throw new Error(`Invalid cron expression: ${cronExpression}`);
  }

  const minutePart = parts[0] ?? '0';
  const hourPart = parts[1] ?? '*';

  const now = new Date();
  const next = new Date(now);

  // Parse hour pattern like */6 for "every 6 hours"
  if (hourPart.startsWith('*/')) {
    const interval = parseInt(hourPart.substring(2), 10);
    const currentHour = now.getHours();
    const nextHour = Math.ceil((currentHour + 1) / interval) * interval;
    next.setHours(nextHour % 24, parseInt(minutePart, 10) || 0, 0, 0);
    if (next <= now) {
      next.setHours(next.getHours() + interval);
    }
  } else if (hourPart !== '*') {
    next.setHours(parseInt(hourPart, 10), parseInt(minutePart, 10) || 0, 0, 0);
    if (next <= now) {
      next.setDate(next.getDate() + 1);
    }
  } else {
    // Every hour at specified minute
    next.setMinutes(parseInt(minutePart, 10) || 0, 0, 0);
    if (next <= now) {
      next.setHours(next.getHours() + 1);
    }
  }

  return next;
}

/**
 * Get milliseconds until next cron run
 *
 * @param cronExpression - Cron expression
 * @returns Milliseconds until next run
 */
export function getMillisUntilNextCron(cronExpression: string): number {
  const nextTime = getNextCronTime(cronExpression);
  return nextTime.getTime() - Date.now();
}

// =============================================================================
// Policy Validation
// =============================================================================

/**
 * Validate retention policy configuration
 *
 * @param policy - Policy configuration to validate
 * @throws Error if validation fails
 */
export function validatePolicy(policy: Partial<RetentionPolicy>): void {
  if (policy.minSegmentCount !== undefined && policy.minSegmentCount < 0) {
    throw new Error('minSegmentCount must be non-negative');
  }
  if (policy.maxSegmentAge !== undefined && policy.maxSegmentAge < 0) {
    throw new Error('maxSegmentAge must be non-negative');
  }
  if (policy.maxTotalBytes !== undefined && policy.maxTotalBytes < 0) {
    throw new Error('maxTotalBytes must be non-negative');
  }
  if (policy.maxEntryCount !== undefined && policy.maxEntryCount < 0) {
    throw new Error('maxEntryCount must be non-negative');
  }
  if (
    policy.compactionThreshold !== undefined &&
    (policy.compactionThreshold < 0 || policy.compactionThreshold > 1)
  ) {
    throw new Error('compactionThreshold must be between 0 and 1');
  }
  if (
    policy.warningThreshold !== undefined &&
    (policy.warningThreshold < 0 || policy.warningThreshold > 1)
  ) {
    throw new Error('warningThreshold must be between 0 and 1');
  }
  if (
    policy.sizeWarningThreshold !== undefined &&
    (policy.sizeWarningThreshold < 0 || policy.sizeWarningThreshold > 1)
  ) {
    throw new Error('sizeWarningThreshold must be between 0 and 1');
  }
  if (policy.readerIdleTimeout !== undefined && policy.readerIdleTimeout < 0) {
    throw new Error('readerIdleTimeout must be non-negative');
  }
  if (policy.cleanupIntervalMs !== undefined && policy.cleanupIntervalMs < 0) {
    throw new Error('cleanupIntervalMs must be non-negative');
  }
  if (policy.maxCleanupBatchSize !== undefined && policy.maxCleanupBatchSize < 0) {
    throw new Error('maxCleanupBatchSize must be non-negative');
  }
  if (policy.cleanupThrottleMs !== undefined && policy.cleanupThrottleMs < 0) {
    throw new Error('cleanupThrottleMs must be non-negative');
  }

  // Validate time window format
  if (policy.lowActivityWindow) {
    const timePattern = /^\d{2}:\d{2}$/;
    if (!timePattern.test(policy.lowActivityWindow.start)) {
      throw new Error('lowActivityWindow.start must be in HH:MM format');
    }
    if (!timePattern.test(policy.lowActivityWindow.end)) {
      throw new Error('lowActivityWindow.end must be in HH:MM format');
    }
  }

  // Validate cron expression
  if (policy.cleanupSchedule) {
    const parts = policy.cleanupSchedule.split(' ');
    if (parts.length !== 5) {
      throw new Error('cleanupSchedule must be a valid cron expression (5 parts)');
    }
  }
}

// =============================================================================
// Policy Resolution
// =============================================================================

/**
 * Resolve a policy configuration with presets and defaults
 *
 * @param policy - Partial policy configuration
 * @returns Fully resolved policy
 */
export function resolvePolicy(policy: Partial<RetentionPolicy> = {}): RetentionPolicy {
  // Validate first
  validatePolicy(policy);

  // Resolve preset if specified
  let basePolicy: RetentionPolicy = { ...DEFAULT_RETENTION_POLICY };
  if (policy.preset) {
    const preset = RETENTION_PRESETS[policy.preset];
    if (preset) {
      basePolicy = { ...preset };
    }
  } else if (policy.extends) {
    const preset = RETENTION_PRESETS[policy.extends];
    if (preset) {
      basePolicy = { ...preset };
      if (policy.overrides) {
        Object.assign(basePolicy, policy.overrides);
      }
    }
  }

  const fullPolicy: RetentionPolicy = { ...basePolicy, ...policy };

  // Handle time-based retention convenience properties
  // Priority: retentionMinutes > retentionHours > retentionDays
  if (fullPolicy.retentionMinutes !== undefined) {
    fullPolicy.maxSegmentAge = fullPolicy.retentionMinutes * 60 * 1000;
  } else if (fullPolicy.retentionHours !== undefined) {
    fullPolicy.maxSegmentAge = fullPolicy.retentionHours * 60 * 60 * 1000;
  } else if (fullPolicy.retentionDays !== undefined) {
    fullPolicy.maxSegmentAge = fullPolicy.retentionDays * 24 * 60 * 60 * 1000;
  }

  // Parse human-readable size string
  if (fullPolicy.maxTotalSize && !fullPolicy.maxTotalBytes) {
    fullPolicy.maxTotalBytes = parseSizeString(fullPolicy.maxTotalSize);
  }

  return fullPolicy;
}

// =============================================================================
// Warning Utilities
// =============================================================================

/**
 * Check storage warning threshold
 *
 * @param currentBytes - Current storage usage in bytes
 * @param policy - Retention policy
 * @returns Warning if threshold exceeded, null otherwise
 */
export function checkStorageWarning(
  currentBytes: number,
  policy: RetentionPolicy
): RetentionWarning | null {
  if (!policy.maxTotalBytes) return null;

  const threshold = policy.warningThreshold ?? policy.sizeWarningThreshold;
  if (!threshold) return null;

  const usage = currentBytes / policy.maxTotalBytes;
  if (usage >= threshold) {
    return {
      type: 'size_threshold',
      currentValue: currentBytes,
      threshold: policy.maxTotalBytes,
      message: `WAL size at ${(usage * 100).toFixed(1)}% of limit`,
    };
  }

  return null;
}

/**
 * Check entry count warning threshold
 *
 * @param currentEntries - Current entry count
 * @param policy - Retention policy
 * @returns Warning if threshold exceeded, null otherwise
 */
export function checkEntryWarning(
  currentEntries: number,
  policy: RetentionPolicy
): RetentionWarning | null {
  if (!policy.maxEntryCount) return null;

  const threshold = policy.warningThreshold ?? 0.8;
  const usage = currentEntries / policy.maxEntryCount;
  if (usage >= threshold) {
    return {
      type: 'entry_threshold',
      currentValue: currentEntries,
      threshold: policy.maxEntryCount,
      message: `WAL entry count at ${(usage * 100).toFixed(1)}% of limit`,
    };
  }

  return null;
}

/**
 * Check age warning threshold
 *
 * @param oldestSegmentAge - Age of oldest segment in milliseconds
 * @param policy - Retention policy
 * @returns Warning if threshold exceeded, null otherwise
 */
export function checkAgeWarning(
  oldestSegmentAge: number,
  policy: RetentionPolicy
): RetentionWarning | null {
  const maxAge = policy.maxSegmentAge * 2; // Warn at 2x age
  if (oldestSegmentAge > maxAge) {
    return {
      type: 'age_threshold',
      currentValue: oldestSegmentAge,
      threshold: policy.maxSegmentAge,
      message: `Oldest segment exceeds ${(oldestSegmentAge / policy.maxSegmentAge).toFixed(1)}x retention age`,
    };
  }

  return null;
}
