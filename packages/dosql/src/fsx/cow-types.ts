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
 * Branch metadata
 */
export interface Branch {
  /** Branch name */
  name: string;
  /** Parent branch (null for root/main) */
  parent: string | null;
  /** Snapshot ID this branch was created from */
  baseSnapshot: SnapshotId | null;
  /** Creation timestamp */
  createdAt: number;
  /** Last modified timestamp */
  modifiedAt: number;
  /** Head version number */
  headVersion: number;
  /** Whether branch is read-only (archived) */
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
 * Strategy for resolving merge conflicts
 */
export type MergeStrategy =
  | 'ours'           // Keep our version
  | 'theirs'         // Keep their version
  | 'last-write-wins' // Keep most recently modified
  | 'fail-on-conflict'; // Fail if any conflicts exist

/**
 * Result of comparing two branches
 */
export interface BranchDiff {
  /** Paths that exist only in source */
  added: string[];
  /** Paths that exist only in target */
  removed: string[];
  /** Paths that exist in both but differ */
  modified: string[];
  /** Paths that are identical */
  unchanged: string[];
}

/**
 * A merge conflict
 */
export interface MergeConflict {
  /** Path with conflict */
  path: string;
  /** Source branch version */
  sourceVersion: number;
  /** Target branch version */
  targetVersion: number;
  /** Base version (common ancestor) */
  baseVersion?: number;
  /** Source modification time */
  sourceModifiedAt: number;
  /** Target modification time */
  targetModifiedAt: number;
}

/**
 * Result of a merge operation
 */
export interface MergeResult {
  /** Whether merge succeeded */
  success: boolean;
  /** Number of blobs merged */
  merged: number;
  /** Conflicts (if any) */
  conflicts: MergeConflict[];
  /** Strategy used */
  strategy: MergeStrategy;
  /** Resulting snapshot (if successful) */
  snapshot?: SnapshotId;
  /** Paths that were updated */
  updated: string[];
  /** Paths that were deleted */
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
