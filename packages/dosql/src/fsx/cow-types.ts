/**
 * Copy-on-Write (COW) Types for FSX
 *
 * Extends the FSX storage abstraction with git-like branching semantics.
 * Enables branches to share unchanged pages and only store deltas.
 *
 * Key concepts:
 * - BlobRef: Reference to blob data (direct or COW reference)
 * - Branch: Isolated copy of data that shares unchanged blobs with parent
 * - Snapshot: Immutable point-in-time view of a branch
 * - Reference counting: Enables safe garbage collection of unreferenced blobs
 */

import type { FSXBackend, FSXMetadata, ByteRange } from './types.js';

// =============================================================================
// Blob Reference Types
// =============================================================================

/**
 * Type of blob reference
 */
export type BlobRefType = 'direct' | 'reference';

/**
 * Reference to blob data
 *
 * Two types:
 * - direct: Actual blob data stored at this path
 * - reference: Points to another path (COW - copy-on-write reference)
 */
export interface BlobRef {
  /** Type of reference */
  type: BlobRefType;
  /** Target path for references (where actual data lives) */
  target?: string;
  /** Version number (incremented on each write) */
  version: number;
  /** Reference count (how many refs point to this blob) */
  refCount: number;
  /** Size of the blob in bytes */
  size: number;
  /** Content hash for integrity verification */
  hash?: string;
  /** Creation timestamp */
  createdAt: number;
  /** Last modified timestamp */
  modifiedAt: number;
  /** Branch this blob belongs to */
  branch: string;
}

/**
 * Stored blob entry (metadata + optional inline data)
 */
export interface StoredBlob {
  /** Blob reference metadata */
  ref: BlobRef;
  /** Inline data for small blobs */
  data?: ArrayBuffer;
}

// =============================================================================
// Branch Types
// =============================================================================

/**
 * Branch metadata representing an isolated line of development.
 *
 * Branches in COW-FSX provide git-like isolation where changes are tracked independently
 * until explicitly merged. Key characteristics:
 *
 * ## Copy-on-Write Semantics
 *
 * When a branch is created, it shares all existing blobs with its parent branch via
 * references (no data is copied). Only when a blob is modified does a new copy get
 * created for that specific blob. This makes branch creation O(1) regardless of data size.
 *
 * ## Branch Hierarchy and Merge Ancestry
 *
 * Branches form a tree structure via the `parent` field:
 *
 * ```
 *           main (parent: null)
 *          /    \
 *    feature1   feature2
 *        |
 *    sub-feature
 * ```
 *
 * The `parent` and `baseSnapshot` fields are crucial for merge operations:
 * - `parent`: Defines the branch hierarchy for finding common ancestors
 * - `baseSnapshot`: The exact point-in-time state this branch forked from
 *
 * When merging, the common ancestor is determined by walking up the parent chain
 * and using the baseSnapshot to know the exact state at fork time.
 *
 * ## Version Tracking
 *
 * `headVersion` is incremented on every write operation to the branch. This enables:
 * - Optimistic concurrency control (compare-and-swap)
 * - Efficient change detection (version differs = content differs)
 * - Snapshot creation at specific versions
 *
 * ## Read-Only Branches
 *
 * When `readonly` is true, the branch is archived and cannot receive writes.
 * Useful for:
 * - Creating stable release branches
 * - Preserving historical states
 * - Preventing accidental modifications
 *
 * Attempting to write to a readonly branch throws COWError with BRANCH_READONLY code.
 *
 * @see BranchOptions - Options when creating a branch
 * @see MergeStrategy - How conflicts are resolved when merging branches
 * @see FSXWithCOW.branch - Method to create a new branch
 * @see FSXWithCOW.merge - Method to merge branches
 */
export interface Branch {
  /** Unique branch name (e.g., 'main', 'feature/auth', 'experiment-1') */
  name: string;
  /** Parent branch name, or null for the root branch (typically 'main') */
  parent: string | null;
  /** Snapshot ID capturing the exact state when this branch was created */
  baseSnapshot: SnapshotId | null;
  /** Unix timestamp (ms) when the branch was created */
  createdAt: number;
  /** Unix timestamp (ms) of the most recent write to this branch */
  modifiedAt: number;
  /** Monotonically increasing version, incremented on each write */
  headVersion: number;
  /** If true, branch is archived and rejects all write operations */
  readonly: boolean;
}

/**
 * Branch creation options
 */
export interface BranchOptions {
  /** Create from specific snapshot (default: latest) */
  snapshot?: SnapshotId;
  /** Make branch read-only */
  readonly?: boolean;
}

// =============================================================================
// Snapshot Types
// =============================================================================

/**
 * Unique snapshot identifier (branch + version)
 */
export type SnapshotId = `${string}@${number}`;

/**
 * Parse a snapshot ID into components
 */
export function parseSnapshotId(id: SnapshotId): { branch: string; version: number } {
  const atIndex = id.lastIndexOf('@');
  if (atIndex === -1) {
    throw new Error(`Invalid snapshot ID: ${id}`);
  }
  return {
    branch: id.slice(0, atIndex),
    version: parseInt(id.slice(atIndex + 1), 10),
  };
}

/**
 * Create a snapshot ID
 */
export function makeSnapshotId(branch: string, version: number): SnapshotId {
  return `${branch}@${version}` as SnapshotId;
}

/**
 * Snapshot metadata
 */
export interface Snapshot {
  /** Snapshot ID */
  id: SnapshotId;
  /** Branch name */
  branch: string;
  /** Version number at snapshot time */
  version: number;
  /** Creation timestamp */
  createdAt: number;
  /** Optional description */
  message?: string;
  /** Manifest of paths + versions at snapshot time */
  manifest: SnapshotManifest;
}

/**
 * Manifest entry for a snapshot
 */
export interface ManifestEntry {
  /** Path to the blob */
  path: string;
  /** Version at snapshot time */
  version: number;
  /** Size in bytes */
  size: number;
  /** Content hash */
  hash?: string;
}

/**
 * Snapshot manifest - map of paths to their state at snapshot time
 */
export interface SnapshotManifest {
  /** All blob entries at snapshot time */
  entries: ManifestEntry[];
  /** Total size of all blobs */
  totalSize: number;
  /** Number of entries */
  count: number;
}

// =============================================================================
// Merge Types
// =============================================================================

/**
 * Strategy for resolving merge conflicts when merging branches.
 *
 * Merge conflicts occur when the same path has been modified in both the source
 * and target branches since they diverged from their common ancestor. The merge
 * strategy determines how these conflicts are automatically resolved.
 *
 * ## Strategy Details
 *
 * ### 'ours'
 * Always keeps the target branch's version when a conflict occurs.
 * - Use when: The target branch is authoritative and source changes should not override
 * - Example: Merging experimental changes into a stable branch where stability is paramount
 * - Behavior: Source-only additions are merged, but conflicting modifications use target's version
 *
 * ### 'theirs'
 * Always keeps the source branch's version when a conflict occurs.
 * - Use when: The source branch has the desired state and should override target
 * - Example: Merging a reviewed feature branch where all changes are intentional
 * - Behavior: Target-only additions are preserved, but conflicting modifications use source's version
 *
 * ### 'last-write-wins'
 * Keeps whichever version was modified most recently based on modifiedAt timestamp.
 * - Use when: Temporal ordering is a reasonable proxy for intent (common in real-time systems)
 * - Example: Syncing distributed caches where the latest write represents current truth
 * - Behavior: Each conflict is resolved independently based on modification timestamps
 * - Note: Requires accurate, synchronized clocks across writers for reliable results
 *
 * ### 'fail-on-conflict'
 * Aborts the merge if any conflicts are detected, leaving both branches unchanged.
 * - Use when: Manual review of conflicts is required before proceeding
 * - Example: Production deployments where unexpected conflicts indicate coordination issues
 * - Behavior: Returns MergeResult with success=false and populated conflicts array
 * - Recommended: Use diff() first to preview conflicts before attempting merge
 *
 * ## Conflict Resolution Flow
 *
 * ```
 * 1. Compute common ancestor (base) from branch history
 * 2. Three-way diff: base vs source, base vs target
 * 3. For each path:
 *    - Changed in source only -> apply source change
 *    - Changed in target only -> keep target change
 *    - Changed in both (CONFLICT) -> apply strategy
 *    - Deleted in one, modified in other (CONFLICT) -> apply strategy
 * 4. If strategy='fail-on-conflict' and conflicts exist -> abort
 * 5. Otherwise -> create merged snapshot
 * ```
 *
 * @see MergeConflict - Structure representing a detected conflict
 * @see MergeResult - Result of a merge operation including any conflicts
 * @see FSXWithCOW.merge - Method that performs the merge using this strategy
 * @see FSXWithCOW.diff - Method to preview differences before merging
 */
export type MergeStrategy =
  | 'ours'
  | 'theirs'
  | 'last-write-wins'
  | 'fail-on-conflict';

/**
 * Result of comparing two branches, categorizing all paths by their state.
 *
 * This is returned by the diff() operation and provides a complete picture of
 * how two branches have diverged. It's useful for:
 * - Previewing merge operations before executing them
 * - Understanding what changes a merge will introduce
 * - Identifying potential conflicts (paths in 'modified' may conflict)
 *
 * ## Category Definitions
 *
 * - **added**: Paths that exist in source but not in target. These will be
 *   copied to target during merge.
 *
 * - **removed**: Paths that exist in target but not in source. During merge,
 *   behavior depends on whether target modified these since the common ancestor.
 *
 * - **modified**: Paths that exist in both branches but have different content
 *   (different version numbers or hashes). These are potential conflict sites.
 *
 * - **unchanged**: Paths that are identical in both branches. No action needed
 *   during merge.
 *
 * ## Example Usage
 *
 * ```typescript
 * const diff = await fsx.diff('feature-branch', 'main');
 *
 * console.log(`New files: ${diff.added.length}`);
 * console.log(`Deleted files: ${diff.removed.length}`);
 * console.log(`Potential conflicts: ${diff.modified.length}`);
 *
 * if (diff.modified.length > 0) {
 *   console.log('Modified paths that may conflict:', diff.modified);
 *   // Consider using 'fail-on-conflict' strategy to review
 * }
 * ```
 *
 * @see MergeStrategy - How to resolve conflicts in modified paths
 * @see FSXWithCOW.diff - Method that produces this diff
 */
export interface BranchDiff {
  /** Paths that exist only in source branch (will be added to target on merge) */
  added: string[];
  /** Paths that exist only in target branch (source deletions or target additions) */
  removed: string[];
  /** Paths that exist in both branches but have different content (potential conflicts) */
  modified: string[];
  /** Paths that are identical in both branches (no merge action needed) */
  unchanged: string[];
}

/**
 * Represents a merge conflict between two branches.
 *
 * A conflict occurs when the same path has been modified in both the source and
 * target branches since they diverged from their common ancestor. This structure
 * provides all the information needed to understand and resolve the conflict.
 *
 * ## Conflict Detection
 *
 * Conflicts are detected using three-way merge semantics:
 *
 * ```
 *        base (common ancestor)
 *       /                      \
 *      v                        v
 *   source                   target
 * (sourceVersion)         (targetVersion)
 * ```
 *
 * A conflict exists when:
 * 1. Path exists in both source and target
 * 2. Both versions differ from base (both branches modified the path)
 * 3. Source version differs from target version
 *
 * ## Using Conflict Information
 *
 * - **baseVersion**: If present, this was the version before either branch modified
 *   the path. Useful for understanding what changed on each side.
 *
 * - **sourceModifiedAt / targetModifiedAt**: Timestamps when each branch last
 *   modified the path. Used by 'last-write-wins' strategy. Also useful for
 *   manual conflict resolution to understand timing.
 *
 * - **sourceVersion / targetVersion**: The version numbers in each branch.
 *   Higher versions indicate more writes, which may be relevant for resolution.
 *
 * ## Example: Manual Conflict Resolution
 *
 * ```typescript
 * const result = await fsx.merge('feature', 'main', 'fail-on-conflict');
 *
 * if (!result.success) {
 *   for (const conflict of result.conflicts) {
 *     const sourceData = await fsx.readFrom(conflict.path, 'feature');
 *     const targetData = await fsx.readFrom(conflict.path, 'main');
 *
 *     // Compare data and decide which to keep, or merge manually
 *     const resolved = manuallyMerge(sourceData, targetData);
 *     await fsx.writeTo(conflict.path, resolved, 'main');
 *   }
 *
 *   // Retry merge after manual resolution
 *   await fsx.merge('feature', 'main', 'ours');
 * }
 * ```
 *
 * @see MergeStrategy - Automatic conflict resolution strategies
 * @see MergeResult - Contains array of conflicts when merge fails or has resolved conflicts
 */
export interface MergeConflict {
  /** Path where the conflict occurred */
  path: string;
  /** Version number of this path in the source branch */
  sourceVersion: number;
  /** Version number of this path in the target branch */
  targetVersion: number;
  /** Version number at the common ancestor (if determinable) */
  baseVersion?: number;
  /** Timestamp when source branch last modified this path */
  sourceModifiedAt: number;
  /** Timestamp when target branch last modified this path */
  targetModifiedAt: number;
}

/**
 * Result of a merge operation, containing outcome details and any conflicts.
 *
 * This structure provides complete information about what happened during a merge,
 * whether it succeeded or failed, and what changes were applied.
 *
 * ## Success vs Failure
 *
 * - **success=true**: Merge completed. All conflicts (if any) were resolved using
 *   the specified strategy. A new snapshot was created on the target branch.
 *
 * - **success=false**: Merge aborted. Only happens with 'fail-on-conflict' strategy
 *   when conflicts exist. Both branches remain unchanged. The conflicts array
 *   contains all detected conflicts for review.
 *
 * ## Understanding the Result
 *
 * ```typescript
 * const result = await fsx.merge('feature', 'main', 'last-write-wins');
 *
 * if (result.success) {
 *   console.log(`Merged ${result.merged} blobs`);
 *   console.log(`Updated paths: ${result.updated.join(', ')}`);
 *   console.log(`Deleted paths: ${result.deleted.join(', ')}`);
 *   console.log(`New snapshot: ${result.snapshot}`);
 *
 *   if (result.conflicts.length > 0) {
 *     // Conflicts were auto-resolved using the strategy
 *     console.log(`Auto-resolved ${result.conflicts.length} conflicts`);
 *     for (const c of result.conflicts) {
 *       console.log(`  ${c.path}: kept version from ${
 *         c.sourceModifiedAt > c.targetModifiedAt ? 'source' : 'target'
 *       }`);
 *     }
 *   }
 * } else {
 *   // Only happens with 'fail-on-conflict'
 *   console.log('Merge failed due to conflicts:');
 *   for (const c of result.conflicts) {
 *     console.log(`  ${c.path}: source v${c.sourceVersion} vs target v${c.targetVersion}`);
 *   }
 * }
 * ```
 *
 * ## Idempotency
 *
 * If a merge would result in no changes (branches are identical or source has no
 * new changes), the merge still succeeds with merged=0 and empty updated/deleted arrays.
 * No new snapshot is created in this case (snapshot will be undefined).
 *
 * @see MergeStrategy - Available strategies for conflict resolution
 * @see MergeConflict - Structure of individual conflicts
 * @see FSXWithCOW.merge - Method that produces this result
 */
export interface MergeResult {
  /** Whether the merge completed successfully */
  success: boolean;
  /** Total number of blobs that were merged (added + updated + deleted) */
  merged: number;
  /** Conflicts detected during merge (resolved if success=true, unresolved if success=false) */
  conflicts: MergeConflict[];
  /** The strategy that was used for conflict resolution */
  strategy: MergeStrategy;
  /** Snapshot ID created for the merged state (undefined if no changes or merge failed) */
  snapshot?: SnapshotId;
  /** Paths that were added or updated in the target branch */
  updated: string[];
  /** Paths that were deleted from the target branch */
  deleted: string[];
}

// =============================================================================
// Garbage Collection Types
// =============================================================================

/**
 * Result of garbage collection
 */
export interface GCResult {
  /** Number of blobs deleted */
  blobsDeleted: number;
  /** Bytes reclaimed */
  bytesReclaimed: number;
  /** Paths that were deleted */
  deletedPaths: string[];
  /** Duration in milliseconds */
  durationMs: number;
  /** Number of blobs scanned */
  blobsScanned: number;
  /** Errors encountered */
  errors: Array<{ path: string; error: string }>;
}

/**
 * Options for garbage collection
 */
export interface GCOptions {
  /** Only GC blobs older than this (ms) */
  olderThan?: number;
  /** Dry run - report what would be deleted without deleting */
  dryRun?: boolean;
  /** Maximum number of blobs to delete in one run */
  limit?: number;
  /** Only GC blobs in specific branches */
  branches?: string[];
}

// =============================================================================
// FSX with COW Interface
// =============================================================================

/**
 * Extended FSX backend with copy-on-write semantics
 */
export interface FSXWithCOW extends FSXBackend {
  // ===========================================================================
  // Branch Operations
  // ===========================================================================

  /**
   * Create a new branch from an existing branch
   * @param source - Source branch name
   * @param target - Target branch name
   * @param options - Branch creation options
   */
  branch(source: string, target: string, options?: BranchOptions): Promise<void>;

  /**
   * Delete a branch
   * @param name - Branch name to delete
   * @param force - Delete even if not merged
   */
  deleteBranch(name: string, force?: boolean): Promise<void>;

  /**
   * List all branches
   */
  listBranches(): Promise<Branch[]>;

  /**
   * Get branch metadata
   * @param name - Branch name
   */
  getBranch(name: string): Promise<Branch | null>;

  /**
   * Get current active branch
   */
  getCurrentBranch(): string;

  /**
   * Switch to a different branch
   * @param name - Branch name to switch to
   */
  checkout(name: string): Promise<void>;

  // ===========================================================================
  // Merge Operations
  // ===========================================================================

  /**
   * Merge source branch into target branch
   * @param source - Source branch name
   * @param target - Target branch name
   * @param strategy - Conflict resolution strategy
   */
  merge(source: string, target: string, strategy: MergeStrategy): Promise<MergeResult>;

  /**
   * Compare two branches
   * @param source - Source branch name
   * @param target - Target branch name
   */
  diff(source: string, target: string): Promise<BranchDiff>;

  // ===========================================================================
  // Snapshot Operations
  // ===========================================================================

  /**
   * Create a snapshot of a branch
   * @param branch - Branch name
   * @param message - Optional snapshot message
   */
  snapshot(branch: string, message?: string): Promise<SnapshotId>;

  /**
   * Read data at a specific snapshot
   * @param path - File path
   * @param snapshotId - Snapshot ID
   * @param range - Optional byte range
   */
  readAt(path: string, snapshotId: SnapshotId, range?: ByteRange): Promise<Uint8Array | null>;

  /**
   * List all snapshots for a branch
   * @param branch - Branch name
   */
  listSnapshots(branch: string): Promise<Snapshot[]>;

  /**
   * Get snapshot metadata
   * @param snapshotId - Snapshot ID
   */
  getSnapshot(snapshotId: SnapshotId): Promise<Snapshot | null>;

  /**
   * Delete a snapshot
   * @param snapshotId - Snapshot ID
   */
  deleteSnapshot(snapshotId: SnapshotId): Promise<void>;

  // ===========================================================================
  // Reference Management
  // ===========================================================================

  /**
   * Get blob reference for a path
   * @param path - File path
   */
  getRef(path: string): Promise<BlobRef | null>;

  /**
   * Get blob references for multiple paths
   * @param paths - File paths
   */
  getRefs(paths: string[]): Promise<Map<string, BlobRef>>;

  /**
   * Run garbage collection to clean up unreferenced blobs
   * @param options - GC options
   */
  gc(options?: GCOptions): Promise<GCResult>;

  // ===========================================================================
  // Extended Read/Write with Branch Context
  // ===========================================================================

  /**
   * Read from specific branch
   * @param path - File path
   * @param branch - Branch name
   * @param range - Optional byte range
   */
  readFrom(path: string, branch: string, range?: ByteRange): Promise<Uint8Array | null>;

  /**
   * Write to specific branch
   * @param path - File path
   * @param data - Data to write
   * @param branch - Branch name
   */
  writeTo(path: string, data: Uint8Array, branch: string): Promise<void>;

  /**
   * Delete from specific branch
   * @param path - File path
   * @param branch - Branch name
   */
  deleteFrom(path: string, branch: string): Promise<void>;

  /**
   * List files in specific branch
   * @param prefix - Path prefix
   * @param branch - Branch name
   */
  listFrom(prefix: string, branch: string): Promise<string[]>;
}

// =============================================================================
// COW Error Types
// =============================================================================

/**
 * COW-specific error codes
 */
export enum COWErrorCode {
  BRANCH_NOT_FOUND = 'COW_BRANCH_NOT_FOUND',
  BRANCH_EXISTS = 'COW_BRANCH_EXISTS',
  BRANCH_READONLY = 'COW_BRANCH_READONLY',
  SNAPSHOT_NOT_FOUND = 'COW_SNAPSHOT_NOT_FOUND',
  MERGE_CONFLICT = 'COW_MERGE_CONFLICT',
  INVALID_REF = 'COW_INVALID_REF',
  CIRCULAR_REF = 'COW_CIRCULAR_REF',
  GC_FAILED = 'COW_GC_FAILED',
}

/**
 * COW-specific error
 */
export class COWError extends Error {
  constructor(
    public readonly code: COWErrorCode,
    message: string,
    public readonly details?: unknown
  ) {
    super(message);
    this.name = 'COWError';
  }
}

// =============================================================================
// Constants
// =============================================================================

/** Default/main branch name */
export const DEFAULT_BRANCH = 'main';

/** Maximum reference chain depth (to prevent circular refs) */
export const MAX_REF_DEPTH = 100;

/** Prefix for branch metadata storage */
export const BRANCH_PREFIX = '_cow/branches/';

/** Prefix for snapshot storage */
export const SNAPSHOT_PREFIX = '_cow/snapshots/';

/** Prefix for blob reference storage */
export const REF_PREFIX = '_cow/refs/';

/** Prefix for blob data storage */
export const BLOB_PREFIX = '_cow/blobs/';
