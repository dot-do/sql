/**
 * Local (DO-level) Time Travel for DoSQL
 *
 * Provides time travel within a single Durable Object by:
 * - Reading from fsx COW snapshots
 * - Replaying WAL entries to reach specific LSN
 * - Combining snapshot + WAL delta for point-in-time reads
 *
 * @packageDocumentation
 */

import type { FSXBackend, ByteRange } from '../fsx/types.js';
import type {
  Snapshot,
  SnapshotId,
  SnapshotManifest,
  ManifestEntry,
} from '../fsx/cow-types.js';
import type { WALReader, WALEntry, Checkpoint } from '../wal/types.js';
import { SnapshotManager } from '../fsx/snapshot.js';
import type { COWBackend } from '../fsx/cow-backend.js';
import type { TimePointResolver } from './resolver.js';
import {
  type TimePoint,
  type ResolvedTimePoint,
  type LocalTimeTravelState,
  type LocalReadResult,
  TimeTravelError,
  TimeTravelErrorCode,
} from './types.js';

// =============================================================================
// Local Time Travel Configuration
// =============================================================================

/**
 * Configuration for local time travel
 */
export interface LocalTimeTravelConfig {
  /** Maximum number of WAL entries to replay per operation */
  maxWALReplay: number;
  /** Cache reconstructed states */
  enableStateCache: boolean;
  /** State cache size limit */
  stateCacheLimit: number;
  /** Verify data integrity during replay */
  verifyIntegrity: boolean;
}

/**
 * Default local time travel configuration
 */
export const DEFAULT_LOCAL_CONFIG: LocalTimeTravelConfig = {
  maxWALReplay: 10000,
  enableStateCache: true,
  stateCacheLimit: 100,
  verifyIntegrity: true,
};

// =============================================================================
// WAL Replay Engine
// =============================================================================

/**
 * A change to apply to a table
 */
export interface TableChange {
  /** Operation type */
  op: 'INSERT' | 'UPDATE' | 'DELETE';
  /** Table name */
  table: string;
  /** Primary key */
  key: Uint8Array | undefined;
  /** Value before change (for UPDATE/DELETE) */
  before: Uint8Array | undefined;
  /** Value after change (for INSERT/UPDATE) */
  after: Uint8Array | undefined;
  /** LSN of this change */
  lsn: bigint;
  /** Timestamp of this change */
  timestamp: number;
}

/**
 * In-memory state reconstructed from snapshot + WAL
 */
export interface ReconstructedState {
  /** Table data keyed by path */
  tables: Map<string, Map<string, Uint8Array>>;
  /** The snapshot this state is based on */
  baseSnapshot: Snapshot | null;
  /** Last LSN applied */
  lastLSN: bigint;
  /** Timestamp of last change */
  lastTimestamp: number;
  /** Changes applied (for debugging/audit) */
  changesApplied: number;
}

/**
 * WAL replay engine for reconstructing state at a point in time
 */
export class WALReplayEngine {
  private walReader: WALReader;
  private config: LocalTimeTravelConfig;

  constructor(walReader: WALReader, config: LocalTimeTravelConfig) {
    this.walReader = walReader;
    this.config = config;
  }

  /**
   * Get changes between two LSN points
   */
  async getChanges(fromLSN: bigint, toLSN: bigint): Promise<TableChange[]> {
    const entries = await this.walReader.readEntries({
      fromLSN: fromLSN + 1n, // Exclusive of from, inclusive of to
      toLSN,
      limit: this.config.maxWALReplay,
    });

    const changes: TableChange[] = [];
    const committedTxns = new Set<string>();
    const pendingTxns = new Map<string, WALEntry[]>();

    // First pass: identify committed transactions
    for (const entry of entries) {
      if (entry.op === 'COMMIT') {
        committedTxns.add(entry.txnId);
      }
    }

    // Second pass: collect changes from committed transactions only
    for (const entry of entries) {
      if (entry.op === 'BEGIN' || entry.op === 'COMMIT' || entry.op === 'ROLLBACK') {
        continue;
      }

      if (committedTxns.has(entry.txnId)) {
        changes.push({
          op: entry.op as 'INSERT' | 'UPDATE' | 'DELETE',
          table: entry.table,
          key: entry.key,
          before: entry.before,
          after: entry.after,
          lsn: entry.lsn,
          timestamp: entry.timestamp,
        });
      }
    }

    return changes;
  }

  /**
   * Apply changes to a state object
   */
  applyChanges(
    state: ReconstructedState,
    changes: TableChange[]
  ): ReconstructedState {
    for (const change of changes) {
      let tableData = state.tables.get(change.table);
      if (!tableData) {
        tableData = new Map();
        state.tables.set(change.table, tableData);
      }

      const key = change.key ? this.keyToString(change.key) : '';

      switch (change.op) {
        case 'INSERT':
          if (change.after) {
            tableData.set(key, change.after);
          }
          break;

        case 'UPDATE':
          if (change.after) {
            tableData.set(key, change.after);
          }
          break;

        case 'DELETE':
          tableData.delete(key);
          break;
      }

      state.lastLSN = change.lsn;
      state.lastTimestamp = change.timestamp;
      state.changesApplied++;
    }

    return state;
  }

  /**
   * Convert key bytes to string for map storage
   */
  private keyToString(key: Uint8Array): string {
    return Array.from(key)
      .map((b) => b.toString(16).padStart(2, '0'))
      .join('');
  }
}

// =============================================================================
// State Cache
// =============================================================================

interface StateCacheEntry {
  state: ReconstructedState;
  createdAt: number;
  accessedAt: number;
}

/**
 * LRU cache for reconstructed states
 */
class StateCache {
  private cache = new Map<string, StateCacheEntry>();
  private limit: number;

  constructor(limit: number) {
    this.limit = limit;
  }

  get(key: string): ReconstructedState | null {
    const entry = this.cache.get(key);
    if (!entry) return null;

    // Update access time
    entry.accessedAt = Date.now();
    return entry.state;
  }

  set(key: string, state: ReconstructedState): void {
    // Evict if at limit
    if (this.cache.size >= this.limit) {
      this.evictLRU();
    }

    this.cache.set(key, {
      state,
      createdAt: Date.now(),
      accessedAt: Date.now(),
    });
  }

  private evictLRU(): void {
    let oldestKey: string | null = null;
    let oldestTime = Infinity;

    for (const [key, entry] of this.cache) {
      if (entry.accessedAt < oldestTime) {
        oldestTime = entry.accessedAt;
        oldestKey = key;
      }
    }

    if (oldestKey) {
      this.cache.delete(oldestKey);
    }
  }

  makeKey(branch: string, lsn: bigint): string {
    return `${branch}:${lsn}`;
  }

  clear(): void {
    this.cache.clear();
  }
}

// =============================================================================
// Local Time Travel Manager
// =============================================================================

/**
 * Dependencies for local time travel
 */
export interface LocalTimeTravelDeps {
  /** COW backend for snapshot access */
  cowBackend: COWBackend;
  /** WAL reader */
  walReader: WALReader;
  /** Time point resolver */
  resolver: TimePointResolver;
  /** Snapshot manager */
  snapshotManager: SnapshotManager;
  /** Get current checkpoint */
  getCheckpoint: () => Promise<Checkpoint | null>;
}

/**
 * Local time travel manager interface
 */
export interface LocalTimeTravelManager {
  /**
   * Get state at a specific time point
   */
  getStateAt(point: TimePoint, branch?: string): Promise<LocalTimeTravelState>;

  /**
   * Read a path at a specific time point
   */
  readAt(
    path: string,
    point: TimePoint,
    branch?: string,
    range?: ByteRange
  ): Promise<LocalReadResult>;

  /**
   * List paths at a specific time point
   */
  listAt(
    prefix: string,
    point: TimePoint,
    branch?: string
  ): Promise<string[]>;

  /**
   * Check if a path exists at a specific time point
   */
  existsAt(
    path: string,
    point: TimePoint,
    branch?: string
  ): Promise<boolean>;

  /**
   * Get the changes between two time points
   */
  getChangesBetween(
    from: TimePoint,
    to: TimePoint,
    branch?: string
  ): Promise<TableChange[]>;

  /**
   * Clear cached states
   */
  clearCache(): void;
}

/**
 * Create a local time travel manager
 */
export function createLocalTimeTravelManager(
  deps: LocalTimeTravelDeps,
  config: Partial<LocalTimeTravelConfig> = {}
): LocalTimeTravelManager {
  const fullConfig: LocalTimeTravelConfig = {
    ...DEFAULT_LOCAL_CONFIG,
    ...config,
  };

  const replayEngine = new WALReplayEngine(deps.walReader, fullConfig);
  const stateCache = fullConfig.enableStateCache
    ? new StateCache(fullConfig.stateCacheLimit)
    : null;

  /**
   * Load snapshot state into memory
   */
  async function loadSnapshotState(
    snapshot: Snapshot | null,
    branch: string
  ): Promise<ReconstructedState> {
    const state: ReconstructedState = {
      tables: new Map(),
      baseSnapshot: snapshot,
      lastLSN: snapshot ? BigInt(snapshot.version) : 0n,
      lastTimestamp: snapshot ? snapshot.createdAt : 0,
      changesApplied: 0,
    };

    if (!snapshot) {
      return state;
    }

    // Load data from snapshot manifest
    for (const entry of snapshot.manifest.entries) {
      // Extract table name from path (assumes paths like "tables/{table}/{key}")
      const parts = entry.path.split('/');
      if (parts[0] === 'tables' && parts.length >= 3) {
        const tableName = parts[1];
        const key = parts.slice(2).join('/');

        let tableData = state.tables.get(tableName);
        if (!tableData) {
          tableData = new Map();
          state.tables.set(tableName, tableData);
        }

        // Read the actual data from snapshot
        const data = await deps.cowBackend.readAt(entry.path, snapshot.id);
        if (data) {
          tableData.set(key, data);
        }
      }
    }

    return state;
  }

  /**
   * Reconstruct state at a resolved time point
   */
  async function reconstructState(
    resolved: ResolvedTimePoint
  ): Promise<ReconstructedState> {
    // Check cache
    if (stateCache) {
      const cached = stateCache.get(
        stateCache.makeKey(resolved.branch, resolved.lsn)
      );
      if (cached) return cached;
    }

    // Load base snapshot
    let state = await loadSnapshotState(
      resolved.baseSnapshot ?? null,
      resolved.branch
    );

    // Apply WAL changes if needed
    if (resolved.walReplayRange) {
      const changes = await replayEngine.getChanges(
        resolved.walReplayRange.fromLSN,
        resolved.walReplayRange.toLSN
      );

      if (changes.length > fullConfig.maxWALReplay) {
        throw new TimeTravelError(
          TimeTravelErrorCode.DATA_GAP,
          `Too many WAL entries to replay (${changes.length} > ${fullConfig.maxWALReplay}). Consider taking more frequent snapshots.`,
          resolved.resolution.original
        );
      }

      state = replayEngine.applyChanges(state, changes);
    }

    // Cache the state
    if (stateCache) {
      stateCache.set(stateCache.makeKey(resolved.branch, resolved.lsn), state);
    }

    return state;
  }

  // Public interface
  const manager: LocalTimeTravelManager = {
    async getStateAt(
      point: TimePoint,
      branch?: string
    ): Promise<LocalTimeTravelState> {
      const resolved = await deps.resolver.resolve(point, branch);

      // Get WAL entries to apply
      const walEntries: WALEntry[] = [];
      if (resolved.walReplayRange) {
        const entries = await deps.walReader.readEntries({
          fromLSN: resolved.walReplayRange.fromLSN + 1n,
          toLSN: resolved.walReplayRange.toLSN,
        });
        walEntries.push(...entries);
      }

      const checkpoint = await deps.getCheckpoint();

      return {
        point: resolved,
        snapshot: resolved.baseSnapshot ?? null,
        walEntries,
        checkpoint,
        branch: resolved.branch,
      };
    },

    async readAt(
      path: string,
      point: TimePoint,
      branch?: string,
      range?: ByteRange
    ): Promise<LocalReadResult> {
      const resolved = await deps.resolver.resolve(point, branch);

      // If we have a snapshot and no WAL replay needed, read directly from snapshot
      if (
        resolved.baseSnapshot &&
        (!resolved.walReplayRange ||
          resolved.walReplayRange.fromLSN >= resolved.walReplayRange.toLSN)
      ) {
        const data = await deps.cowBackend.readAt(
          path,
          resolved.snapshotId!,
          range
        );

        return {
          data,
          state: {
            point: resolved,
            snapshot: resolved.baseSnapshot,
            walEntries: [],
            checkpoint: await deps.getCheckpoint(),
            branch: resolved.branch,
          },
          fromSnapshot: true,
          effectiveLSN: resolved.lsn,
        };
      }

      // Need to reconstruct state with WAL replay
      const state = await reconstructState(resolved);

      // Extract the data from reconstructed state
      // Path format: tables/{table}/{key}
      const parts = path.split('/');
      let data: Uint8Array | null = null;

      if (parts[0] === 'tables' && parts.length >= 3) {
        const tableName = parts[1];
        const key = parts.slice(2).join('/');
        const tableData = state.tables.get(tableName);

        if (tableData) {
          const fullData = tableData.get(key);
          if (fullData) {
            // Apply range if specified
            if (range) {
              const start = range.offset ?? 0;
              const end = range.length
                ? start + range.length
                : fullData.length;
              data = fullData.slice(start, end);
            } else {
              data = fullData;
            }
          }
        }
      } else {
        // Non-table path - try reading from snapshot if available
        if (resolved.baseSnapshot) {
          data = await deps.cowBackend.readAt(
            path,
            resolved.snapshotId!,
            range
          );
        }
      }

      // Get WAL entries
      const walEntries: WALEntry[] = [];
      if (resolved.walReplayRange) {
        const entries = await deps.walReader.readEntries({
          fromLSN: resolved.walReplayRange.fromLSN + 1n,
          toLSN: resolved.walReplayRange.toLSN,
        });
        walEntries.push(...entries);
      }

      return {
        data,
        state: {
          point: resolved,
          snapshot: resolved.baseSnapshot ?? null,
          walEntries,
          checkpoint: await deps.getCheckpoint(),
          branch: resolved.branch,
        },
        fromSnapshot: false,
        effectiveLSN: resolved.lsn,
      };
    },

    async listAt(
      prefix: string,
      point: TimePoint,
      branch?: string
    ): Promise<string[]> {
      const resolved = await deps.resolver.resolve(point, branch);

      // If we have a snapshot, get paths from manifest
      if (resolved.baseSnapshot) {
        const manifest = resolved.baseSnapshot.manifest;
        const paths = manifest.entries
          .map((e) => e.path)
          .filter((p) => p.startsWith(prefix));

        // If no WAL replay needed, return snapshot paths
        if (
          !resolved.walReplayRange ||
          resolved.walReplayRange.fromLSN >= resolved.walReplayRange.toLSN
        ) {
          return paths.sort();
        }

        // Apply WAL changes to path list
        const state = await reconstructState(resolved);
        const resultPaths = new Set<string>(paths);

        // Add paths from reconstructed state
        for (const [tableName, tableData] of state.tables) {
          for (const key of tableData.keys()) {
            const path = `tables/${tableName}/${key}`;
            if (path.startsWith(prefix)) {
              resultPaths.add(path);
            }
          }
        }

        return Array.from(resultPaths).sort();
      }

      // No snapshot - reconstruct full state
      const state = await reconstructState(resolved);
      const resultPaths: string[] = [];

      for (const [tableName, tableData] of state.tables) {
        for (const key of tableData.keys()) {
          const path = `tables/${tableName}/${key}`;
          if (path.startsWith(prefix)) {
            resultPaths.push(path);
          }
        }
      }

      return resultPaths.sort();
    },

    async existsAt(
      path: string,
      point: TimePoint,
      branch?: string
    ): Promise<boolean> {
      const result = await manager.readAt(path, point, branch);
      return result.data !== null;
    },

    async getChangesBetween(
      from: TimePoint,
      to: TimePoint,
      branch?: string
    ): Promise<TableChange[]> {
      const resolvedFrom = await deps.resolver.resolve(from, branch);
      const resolvedTo = await deps.resolver.resolve(to, branch);

      if (resolvedFrom.lsn >= resolvedTo.lsn) {
        return [];
      }

      return replayEngine.getChanges(resolvedFrom.lsn, resolvedTo.lsn);
    },

    clearCache(): void {
      stateCache?.clear();
    },
  };

  return manager;
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Check if local time travel is available for a time point
 * (i.e., the data hasn't been archived)
 */
export async function isLocalTimeTravelAvailable(
  resolver: TimePointResolver,
  walReader: WALReader,
  point: TimePoint,
  branch: string
): Promise<{ available: boolean; reason?: string }> {
  try {
    const resolved = await resolver.resolve(point, branch);

    // Check if we have the necessary snapshot
    if (!resolved.baseSnapshot && resolved.lsn > 0n) {
      // Need to verify WAL entries exist
      const entries = await walReader.readEntries({
        fromLSN: 0n,
        toLSN: resolved.lsn,
        limit: 1,
      });

      if (entries.length === 0) {
        return {
          available: false,
          reason: 'WAL entries have been archived',
        };
      }
    }

    return { available: true };
  } catch (error) {
    if (error instanceof TimeTravelError) {
      return {
        available: false,
        reason: error.message,
      };
    }
    throw error;
  }
}

/**
 * Get the oldest available time point for local time travel
 */
export async function getOldestAvailablePoint(
  snapshotManager: SnapshotManager,
  walReader: WALReader,
  branch: string
): Promise<TimePoint | null> {
  // Check for oldest snapshot
  const snapshots = await snapshotManager.listSnapshots(branch);

  if (snapshots.length > 0) {
    // Snapshots are sorted newest first
    const oldest = snapshots[snapshots.length - 1];
    return { type: 'snapshot', snapshotId: oldest.id };
  }

  // Check for oldest WAL entry
  const segments = await walReader.listSegments(true); // Include archived
  if (segments.length > 0) {
    const oldestSegment = await walReader.readSegment(segments[0]);
    if (oldestSegment && oldestSegment.entries.length > 0) {
      return { type: 'lsn', lsn: oldestSegment.startLSN };
    }
  }

  return null;
}
