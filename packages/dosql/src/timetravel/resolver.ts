/**
 * Time Point Resolver for DoSQL
 *
 * Resolves time points to concrete LSN and snapshot references:
 * - LSN -> finds snapshot containing that LSN
 * - Timestamp -> finds nearest snapshot/LSN at or before timestamp
 * - Snapshot -> validates and returns snapshot info
 * - Branch -> resolves to HEAD or specified point within branch
 * - Relative -> computes offset from anchor point
 *
 * @packageDocumentation
 */

import type { FSXBackend } from '../fsx/types.js';
import type {
  Snapshot,
  SnapshotId,
  Branch,
} from '../fsx/cow-types.js';
import type { WALReader, WALEntry, Checkpoint } from '../wal/types.js';
import { SnapshotManager } from '../fsx/snapshot.js';
import {
  type TimePoint,
  type ResolvedTimePoint,
  type ResolvedTimeRange,
  type TimeRange,
  TimeTravelError,
  TimeTravelErrorCode,
} from './types.js';

// =============================================================================
// Resolver Configuration
// =============================================================================

/**
 * Configuration for the time point resolver
 */
export interface ResolverConfig {
  /** Default branch to use when not specified */
  defaultBranch: string;
  /** Maximum time difference (ms) to consider a "close enough" timestamp match */
  maxTimestampDrift: number;
  /** Cache resolved time points */
  enableCache: boolean;
  /** Cache TTL in milliseconds */
  cacheTTL: number;
}

/**
 * Default resolver configuration
 */
export const DEFAULT_RESOLVER_CONFIG: ResolverConfig = {
  defaultBranch: 'main',
  maxTimestampDrift: 1000, // 1 second
  enableCache: true,
  cacheTTL: 60000, // 1 minute
};

// =============================================================================
// Resolver Dependencies
// =============================================================================

/**
 * Dependencies for the resolver
 */
export interface ResolverDependencies {
  /** FSX storage backend */
  backend: FSXBackend;
  /** WAL reader for LSN lookups */
  walReader: WALReader;
  /** Snapshot manager for snapshot lookups */
  snapshotManager: SnapshotManager;
  /** Function to get current branch */
  getCurrentBranch: () => string;
  /** Function to get branch metadata */
  getBranch: (name: string) => Promise<Branch | null>;
  /** Function to get current LSN */
  getCurrentLSN: () => bigint;
  /** Function to get current checkpoint */
  getCheckpoint: () => Promise<Checkpoint | null>;
}

// =============================================================================
// Cache Implementation
// =============================================================================

interface CacheEntry<T> {
  value: T;
  expiresAt: number;
}

class ResolverCache {
  private cache = new Map<string, CacheEntry<ResolvedTimePoint>>();
  private ttl: number;

  constructor(ttl: number) {
    this.ttl = ttl;
  }

  get(key: string): ResolvedTimePoint | null {
    const entry = this.cache.get(key);
    if (!entry) return null;
    if (Date.now() > entry.expiresAt) {
      this.cache.delete(key);
      return null;
    }
    return entry.value;
  }

  set(key: string, value: ResolvedTimePoint): void {
    this.cache.set(key, {
      value,
      expiresAt: Date.now() + this.ttl,
    });
  }

  clear(): void {
    this.cache.clear();
  }

  makeKey(point: TimePoint, branch: string): string {
    switch (point.type) {
      case 'lsn':
        return `lsn:${branch}:${point.lsn}`;
      case 'timestamp':
        return `ts:${branch}:${point.timestamp.getTime()}`;
      case 'snapshot':
        return `snap:${point.snapshotId}`;
      case 'branch':
        return `branch:${point.branch}:${point.point ? this.makeKey(point.point, point.branch) : 'HEAD'}`;
      case 'relative':
        return `rel:${branch}:${point.lsnOffset ?? 0}:${point.timeOffset ?? 0}:${point.versionOffset ?? 0}`;
    }
  }
}

// =============================================================================
// Resolver Implementation
// =============================================================================

/**
 * Time point resolver
 */
export interface TimePointResolver {
  /**
   * Resolve a time point to a concrete LSN and snapshot
   * @param point - The time point to resolve
   * @param branchContext - Optional branch context
   * @returns Resolved time point with LSN and snapshot info
   */
  resolve(point: TimePoint, branchContext?: string): Promise<ResolvedTimePoint>;

  /**
   * Resolve a time range
   * @param range - The time range to resolve
   * @param branchContext - Optional branch context
   * @returns Resolved time range with snapshots
   */
  resolveRange(range: TimeRange, branchContext?: string): Promise<ResolvedTimeRange>;

  /**
   * Find the snapshot containing a specific LSN
   * @param lsn - The LSN to find
   * @param branch - The branch to search
   * @returns The snapshot containing the LSN, or null
   */
  findSnapshotForLSN(lsn: bigint, branch: string): Promise<Snapshot | null>;

  /**
   * Find the LSN nearest to a timestamp
   * @param timestamp - The timestamp to search for
   * @param branch - The branch to search
   * @returns The LSN and exact timestamp
   */
  findLSNForTimestamp(
    timestamp: Date,
    branch: string
  ): Promise<{ lsn: bigint; timestamp: Date; exact: boolean } | null>;

  /**
   * Clear the resolver cache
   */
  clearCache(): void;
}

/**
 * Create a time point resolver
 */
export function createTimePointResolver(
  deps: ResolverDependencies,
  config: Partial<ResolverConfig> = {}
): TimePointResolver {
  const fullConfig: ResolverConfig = { ...DEFAULT_RESOLVER_CONFIG, ...config };
  const cache = fullConfig.enableCache
    ? new ResolverCache(fullConfig.cacheTTL)
    : null;

  /**
   * Resolve LSN time point
   */
  async function resolveLSN(
    lsn: bigint,
    branch: string
  ): Promise<ResolvedTimePoint> {
    // Check if LSN is in the future
    const currentLSN = deps.getCurrentLSN();
    if (lsn > currentLSN) {
      throw new TimeTravelError(
        TimeTravelErrorCode.FUTURE_POINT,
        `LSN ${lsn} is in the future (current: ${currentLSN})`,
        { type: 'lsn', lsn }
      );
    }

    // Find the snapshot at or before this LSN
    const snapshot = await resolver.findSnapshotForLSN(lsn, branch);

    // Find the exact entry to get timestamp
    const entry = await deps.walReader.getEntry(lsn);

    // If no entry and no snapshot, LSN is not available
    if (!entry && !snapshot) {
      throw new TimeTravelError(
        TimeTravelErrorCode.LSN_NOT_AVAILABLE,
        `LSN ${lsn} is not available (may be archived)`,
        { type: 'lsn', lsn }
      );
    }

    const timestamp = entry
      ? new Date(entry.timestamp)
      : new Date(snapshot!.createdAt);

    return {
      lsn,
      timestamp,
      snapshotId: snapshot?.id,
      branch,
      baseSnapshot: snapshot ?? undefined,
      walReplayRange: snapshot
        ? { fromLSN: snapshot.version > 0 ? BigInt(snapshot.version) : 0n, toLSN: lsn }
        : undefined,
      resolution: {
        exact: !!entry,
        original: { type: 'lsn', lsn },
        notes: entry ? undefined : 'LSN not found, using nearest snapshot',
      },
    };
  }

  /**
   * Resolve timestamp time point
   */
  async function resolveTimestamp(
    timestamp: Date,
    branch: string
  ): Promise<ResolvedTimePoint> {
    const now = Date.now();
    if (timestamp.getTime() > now) {
      throw new TimeTravelError(
        TimeTravelErrorCode.FUTURE_POINT,
        `Timestamp ${timestamp.toISOString()} is in the future`,
        { type: 'timestamp', timestamp }
      );
    }

    const result = await resolver.findLSNForTimestamp(timestamp, branch);

    if (!result) {
      throw new TimeTravelError(
        TimeTravelErrorCode.POINT_NOT_FOUND,
        `No data found at or before ${timestamp.toISOString()}`,
        { type: 'timestamp', timestamp }
      );
    }

    return resolveLSN(result.lsn, branch);
  }

  /**
   * Resolve snapshot time point
   */
  async function resolveSnapshot(
    snapshotId: SnapshotId,
    branch: string
  ): Promise<ResolvedTimePoint> {
    const snapshot = await deps.snapshotManager.getSnapshot(snapshotId);

    if (!snapshot) {
      throw new TimeTravelError(
        TimeTravelErrorCode.SNAPSHOT_NOT_FOUND,
        `Snapshot not found: ${snapshotId}`,
        { type: 'snapshot', snapshotId }
      );
    }

    return {
      lsn: BigInt(snapshot.version),
      timestamp: new Date(snapshot.createdAt),
      snapshotId: snapshot.id,
      branch: snapshot.branch,
      baseSnapshot: snapshot,
      resolution: {
        exact: true,
        original: { type: 'snapshot', snapshotId },
      },
    };
  }

  /**
   * Resolve branch time point
   */
  async function resolveBranch(
    branchName: string,
    point: TimePoint | undefined,
    contextBranch: string
  ): Promise<ResolvedTimePoint> {
    const branchData = await deps.getBranch(branchName);

    if (!branchData) {
      throw new TimeTravelError(
        TimeTravelErrorCode.BRANCH_NOT_FOUND,
        `Branch not found: ${branchName}`,
        { type: 'branch', branch: branchName, point }
      );
    }

    if (point) {
      // Resolve the nested point within this branch context
      return resolver.resolve(point, branchName);
    }

    // No nested point - resolve to branch HEAD
    const latestSnapshot = await deps.snapshotManager.getLatestSnapshot(branchName);

    if (latestSnapshot) {
      return {
        lsn: BigInt(latestSnapshot.version),
        timestamp: new Date(latestSnapshot.createdAt),
        snapshotId: latestSnapshot.id,
        branch: branchName,
        baseSnapshot: latestSnapshot,
        resolution: {
          exact: true,
          original: { type: 'branch', branch: branchName, point },
          notes: 'Resolved to branch HEAD snapshot',
        },
      };
    }

    // No snapshots - use current state
    const currentLSN = deps.getCurrentLSN();
    return {
      lsn: currentLSN,
      timestamp: new Date(),
      branch: branchName,
      resolution: {
        exact: false,
        original: { type: 'branch', branch: branchName, point },
        notes: 'No snapshots on branch, using current state',
      },
    };
  }

  /**
   * Resolve relative time point
   */
  async function resolveRelative(
    point: {
      anchor?: TimePoint;
      lsnOffset?: bigint;
      timeOffset?: number;
      versionOffset?: number;
    },
    branch: string
  ): Promise<ResolvedTimePoint> {
    // Resolve anchor point (defaults to HEAD)
    const anchorResolved = point.anchor
      ? await resolver.resolve(point.anchor, branch)
      : await resolveBranch(branch, undefined, branch);

    let targetLSN = anchorResolved.lsn;
    let targetTimestamp = anchorResolved.timestamp;

    // Apply LSN offset
    if (point.lsnOffset !== undefined) {
      targetLSN = anchorResolved.lsn + point.lsnOffset;
      if (targetLSN < 0n) targetLSN = 0n;
    }

    // Apply time offset
    if (point.timeOffset !== undefined) {
      targetTimestamp = new Date(
        anchorResolved.timestamp.getTime() + point.timeOffset
      );
      // Need to find LSN for this timestamp
      const result = await resolver.findLSNForTimestamp(targetTimestamp, branch);
      if (result) {
        targetLSN = result.lsn;
        targetTimestamp = result.timestamp;
      }
    }

    // Apply version offset
    if (point.versionOffset !== undefined) {
      const snapshots = await deps.snapshotManager.listSnapshots(branch);
      const anchorIdx = snapshots.findIndex(
        (s) => s.id === anchorResolved.snapshotId
      );

      if (anchorIdx >= 0) {
        const targetIdx = anchorIdx - point.versionOffset; // Negative offset = earlier
        if (targetIdx >= 0 && targetIdx < snapshots.length) {
          const targetSnapshot = snapshots[targetIdx];
          return {
            lsn: BigInt(targetSnapshot.version),
            timestamp: new Date(targetSnapshot.createdAt),
            snapshotId: targetSnapshot.id,
            branch,
            baseSnapshot: targetSnapshot,
            resolution: {
              exact: true,
              original: {
                type: 'relative',
                anchor: point.anchor,
                versionOffset: point.versionOffset,
              },
              notes: `Resolved ${point.versionOffset} versions from anchor`,
            },
          };
        }
      }
    }

    // Resolve the computed LSN
    return resolveLSN(targetLSN, branch);
  }

  // Public resolver interface
  const resolver: TimePointResolver = {
    async resolve(
      point: TimePoint,
      branchContext?: string
    ): Promise<ResolvedTimePoint> {
      const branch = branchContext ?? deps.getCurrentBranch();

      // Check cache
      if (cache) {
        const key = cache.makeKey(point, branch);
        const cached = cache.get(key);
        if (cached) return cached;
      }

      let resolved: ResolvedTimePoint;

      switch (point.type) {
        case 'lsn':
          resolved = await resolveLSN(point.lsn, branch);
          break;
        case 'timestamp':
          resolved = await resolveTimestamp(point.timestamp, branch);
          break;
        case 'snapshot':
          resolved = await resolveSnapshot(point.snapshotId, branch);
          break;
        case 'branch':
          resolved = await resolveBranch(point.branch, point.point, branch);
          break;
        case 'relative':
          resolved = await resolveRelative(point, branch);
          break;
      }

      // Update cache
      if (cache) {
        const key = cache.makeKey(point, branch);
        cache.set(key, resolved);
      }

      return resolved;
    },

    async resolveRange(
      range: TimeRange,
      branchContext?: string
    ): Promise<ResolvedTimeRange> {
      const branch = branchContext ?? deps.getCurrentBranch();

      const [from, to] = await Promise.all([
        resolver.resolve(range.from, branch),
        resolver.resolve(range.to, branch),
      ]);

      // Get snapshots in range
      const allSnapshots = await deps.snapshotManager.listSnapshots(branch);
      const snapshots = allSnapshots.filter(
        (s) =>
          BigInt(s.version) >= from.lsn && BigInt(s.version) <= to.lsn
      );

      // Count entries in range
      const entries = await deps.walReader.readEntries({
        fromLSN: from.lsn,
        toLSN: to.lsn,
      });

      return {
        from,
        to,
        snapshots,
        entryCount: entries.length,
      };
    },

    async findSnapshotForLSN(
      lsn: bigint,
      branch: string
    ): Promise<Snapshot | null> {
      const snapshots = await deps.snapshotManager.listSnapshots(branch);

      // Snapshots are sorted newest first
      // Find the latest snapshot at or before the LSN
      for (const snapshot of snapshots) {
        if (BigInt(snapshot.version) <= lsn) {
          return snapshot;
        }
      }

      return null;
    },

    async findLSNForTimestamp(
      timestamp: Date,
      branch: string
    ): Promise<{ lsn: bigint; timestamp: Date; exact: boolean } | null> {
      const targetTime = timestamp.getTime();

      // First, check snapshots for a close match
      const snapshots = await deps.snapshotManager.listSnapshots(branch);

      for (const snapshot of snapshots) {
        if (snapshot.createdAt <= targetTime) {
          const timeDiff = targetTime - snapshot.createdAt;

          // If snapshot is within drift tolerance, use it
          if (timeDiff <= fullConfig.maxTimestampDrift) {
            return {
              lsn: BigInt(snapshot.version),
              timestamp: new Date(snapshot.createdAt),
              exact: timeDiff === 0,
            };
          }

          // Otherwise, search WAL between this snapshot and target time
          const entries = await deps.walReader.readEntries({
            fromLSN: BigInt(snapshot.version),
          });

          // Find entry closest to but not after target time
          let bestEntry: WALEntry | null = null;
          for (const entry of entries) {
            if (entry.timestamp <= targetTime) {
              if (!bestEntry || entry.timestamp > bestEntry.timestamp) {
                bestEntry = entry;
              }
            } else {
              break; // Entries are ordered, no need to continue
            }
          }

          if (bestEntry) {
            return {
              lsn: bestEntry.lsn,
              timestamp: new Date(bestEntry.timestamp),
              exact: bestEntry.timestamp === targetTime,
            };
          }

          // Fall back to snapshot
          return {
            lsn: BigInt(snapshot.version),
            timestamp: new Date(snapshot.createdAt),
            exact: false,
          };
        }
      }

      // No snapshots before timestamp - search all WAL entries
      const entries = await deps.walReader.readEntries({});

      let bestEntry: WALEntry | null = null;
      for (const entry of entries) {
        if (entry.timestamp <= targetTime) {
          if (!bestEntry || entry.timestamp > bestEntry.timestamp) {
            bestEntry = entry;
          }
        }
      }

      if (bestEntry) {
        return {
          lsn: bestEntry.lsn,
          timestamp: new Date(bestEntry.timestamp),
          exact: bestEntry.timestamp === targetTime,
        };
      }

      return null;
    },

    clearCache(): void {
      cache?.clear();
    },
  };

  return resolver;
}

// =============================================================================
// Validation Utilities
// =============================================================================

/**
 * Validate that a time point is well-formed
 */
export function validateTimePoint(point: TimePoint): void {
  switch (point.type) {
    case 'lsn':
      if (point.lsn < 0n) {
        throw new TimeTravelError(
          TimeTravelErrorCode.INVALID_SYNTAX,
          'LSN cannot be negative'
        );
      }
      break;

    case 'timestamp':
      if (isNaN(point.timestamp.getTime())) {
        throw new TimeTravelError(
          TimeTravelErrorCode.INVALID_SYNTAX,
          'Invalid timestamp'
        );
      }
      break;

    case 'snapshot':
      if (!point.snapshotId.includes('@')) {
        throw new TimeTravelError(
          TimeTravelErrorCode.INVALID_SYNTAX,
          'Invalid snapshot ID format (expected branch@version)'
        );
      }
      break;

    case 'branch':
      if (!point.branch || point.branch.trim() === '') {
        throw new TimeTravelError(
          TimeTravelErrorCode.INVALID_SYNTAX,
          'Branch name cannot be empty'
        );
      }
      if (point.point) {
        validateTimePoint(point.point);
      }
      break;

    case 'relative':
      if (point.anchor) {
        validateTimePoint(point.anchor);
      }
      break;
  }
}

/**
 * Compare two time points to determine ordering
 * Returns negative if a < b, positive if a > b, 0 if equal
 * Note: Can only compare resolved time points accurately
 */
export function compareTimePoints(
  a: ResolvedTimePoint,
  b: ResolvedTimePoint
): number {
  if (a.lsn < b.lsn) return -1;
  if (a.lsn > b.lsn) return 1;
  return 0;
}
