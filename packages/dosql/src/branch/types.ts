/**
 * Git-Like Branch Types for DoSQL
 *
 * Type definitions for git-like branching system that enables:
 * - Creating branches (like git branches)
 * - Branch isolation (writes don't affect other branches)
 * - Merging branches back together
 * - Copy-on-write for efficient storage
 *
 * @packageDocumentation
 */

// =============================================================================
// Branch Core Types
// =============================================================================

/**
 * Unique branch identifier
 */
export type BranchId = string;

/**
 * Branch metadata
 */
export interface BranchMetadata {
  /** Branch name (unique identifier) */
  name: BranchId;
  /** Parent branch this was created from (null for root/main) */
  parent: BranchId | null;
  /** Commit ID this branch was created from */
  baseCommit: CommitId | null;
  /** Creation timestamp */
  createdAt: number;
  /** Last updated timestamp */
  updatedAt: number;
  /** Whether branch is protected (cannot be deleted) */
  protected: boolean;
  /** Whether branch is archived (read-only) */
  archived: boolean;
  /** Optional description */
  description?: string;
  /** Latest commit on this branch */
  head: CommitId | null;
}

/**
 * Options for creating a new branch
 */
export interface CreateBranchOptions {
  /** Branch name */
  name: BranchId;
  /** Source branch to create from (default: current branch) */
  from?: BranchId;
  /** Specific commit to branch from (default: HEAD of source) */
  commit?: CommitId;
  /** Optional description */
  description?: string;
}

/**
 * Options for deleting a branch
 */
export interface DeleteBranchOptions {
  /** Force delete even if not merged */
  force?: boolean;
  /** Delete remote tracking branch */
  deleteRemote?: boolean;
}

// =============================================================================
// Commit Types
// =============================================================================

/**
 * Unique commit identifier (hash)
 */
export type CommitId = string;

/**
 * Commit metadata
 */
export interface CommitMetadata {
  /** Commit ID (content-addressed hash) */
  id: CommitId;
  /** Branch this commit belongs to */
  branch: BranchId;
  /** Parent commit(s) - can have multiple for merge commits */
  parents: CommitId[];
  /** Tree root hash (represents file state) */
  tree: TreeId;
  /** Commit message */
  message: string;
  /** Author information */
  author: AuthorInfo;
  /** Timestamp */
  timestamp: number;
  /** Additional metadata */
  metadata?: Record<string, unknown>;
}

/**
 * Author information for commits
 */
export interface AuthorInfo {
  name: string;
  email?: string;
}

/**
 * Tree ID (hash of directory structure)
 */
export type TreeId = string;

/**
 * Tree entry (file or directory in a commit)
 */
export interface TreeEntry {
  /** Entry name */
  name: string;
  /** Type: blob (file) or tree (directory) */
  type: 'blob' | 'tree';
  /** Hash of content or subtree */
  hash: string;
  /** Size in bytes (for blobs) */
  size?: number;
  /** File mode (permissions) */
  mode?: number;
}

// =============================================================================
// Checkout Types
// =============================================================================

/**
 * Checkout options
 */
export interface CheckoutOptions {
  /** Create branch if it doesn't exist */
  create?: boolean;
  /** Force checkout (discard uncommitted changes) */
  force?: boolean;
  /** Checkout specific commit instead of branch HEAD */
  commit?: CommitId;
}

/**
 * Result of checkout operation
 */
export interface CheckoutResult {
  /** Previous branch/commit */
  previous: {
    branch?: BranchId;
    commit?: CommitId;
  };
  /** Current branch/commit after checkout */
  current: {
    branch: BranchId;
    commit: CommitId | null;
  };
  /** Files that were changed */
  changed: string[];
  /** Files that had conflicts (when force=false) */
  conflicts?: string[];
}

// =============================================================================
// Merge Types
// =============================================================================

/**
 * Merge strategy
 */
export type MergeStrategy =
  | 'recursive'        // Default three-way merge
  | 'ours'             // Keep current branch version
  | 'theirs'           // Keep incoming branch version
  | 'fast-forward'     // Only allow if no divergence
  | 'no-ff'            // Always create merge commit
  | 'squash';          // Squash all commits into one

/**
 * Options for merge operation
 */
export interface MergeOptions {
  /** Merge strategy */
  strategy?: MergeStrategy;
  /** Commit message for merge commit */
  message?: string;
  /** Abort merge on conflict */
  abortOnConflict?: boolean;
  /** Auto-resolve conflicts with given resolution */
  autoResolve?: 'ours' | 'theirs';
}

/**
 * Result of merge operation
 */
export interface MergeResult {
  /** Whether merge succeeded */
  success: boolean;
  /** Type of merge performed */
  mergeType: 'fast-forward' | 'merge-commit' | 'squash' | 'aborted';
  /** Resulting commit (if successful) */
  commit?: CommitId;
  /** Source branch */
  source: BranchId;
  /** Target branch */
  target: BranchId;
  /** List of conflicts (if any) */
  conflicts: MergeConflict[];
  /** Files that were updated */
  updated: string[];
  /** Files that were added */
  added: string[];
  /** Files that were deleted */
  deleted: string[];
}

/**
 * A merge conflict
 */
export interface MergeConflict {
  /** Path of conflicting file */
  path: string;
  /** Type of conflict */
  type: 'content' | 'add-add' | 'modify-delete' | 'rename';
  /** Our version (target branch) */
  ours?: ConflictVersion;
  /** Their version (source branch) */
  theirs?: ConflictVersion;
  /** Base version (common ancestor) */
  base?: ConflictVersion;
  /** Resolution status */
  resolved: boolean;
  /** Resolution if resolved */
  resolution?: 'ours' | 'theirs' | 'merged' | 'deleted';
}

/**
 * Version info for conflict resolution
 */
export interface ConflictVersion {
  /** Content hash */
  hash: string;
  /** File mode */
  mode: number;
  /** Commit where this version exists */
  commit: CommitId;
}

// =============================================================================
// Branch History Types
// =============================================================================

/**
 * A log entry in branch history
 */
export interface BranchLogEntry {
  /** Commit information */
  commit: CommitMetadata;
  /** Branch at this point */
  branch: BranchId;
  /** Relative position (0 = HEAD) */
  position: number;
}

/**
 * Options for getting branch log/history
 */
export interface BranchLogOptions {
  /** Maximum number of entries to return */
  limit?: number;
  /** Skip this many entries */
  skip?: number;
  /** Only include commits after this date */
  since?: Date;
  /** Only include commits before this date */
  until?: Date;
  /** Only include commits affecting these paths */
  paths?: string[];
  /** Include commits from all branches */
  all?: boolean;
}

/**
 * Branch comparison result
 */
export interface BranchComparison {
  /** Source branch */
  source: BranchId;
  /** Target branch */
  target: BranchId;
  /** Common ancestor commit */
  mergeBase: CommitId | null;
  /** Commits ahead in source */
  ahead: number;
  /** Commits behind (in target) */
  behind: number;
  /** Whether branches have diverged */
  diverged: boolean;
  /** File differences */
  diff: FileDiff[];
}

/**
 * File difference between branches
 */
export interface FileDiff {
  /** File path */
  path: string;
  /** Type of change */
  status: 'added' | 'modified' | 'deleted' | 'renamed';
  /** Old path (for renames) */
  oldPath?: string;
  /** Additions count */
  additions?: number;
  /** Deletions count */
  deletions?: number;
}

// =============================================================================
// SQL Extension Types
// =============================================================================

/**
 * Parsed CREATE BRANCH statement
 */
export interface CreateBranchStatement {
  type: 'CREATE_BRANCH';
  name: BranchId;
  from?: BranchId;
  commit?: CommitId;
}

/**
 * Parsed CHECKOUT BRANCH statement
 */
export interface CheckoutBranchStatement {
  type: 'CHECKOUT_BRANCH';
  name: BranchId;
  create?: boolean;
}

/**
 * Parsed MERGE BRANCH statement
 */
export interface MergeBranchStatement {
  type: 'MERGE_BRANCH';
  source: BranchId;
  into: BranchId;
  strategy?: MergeStrategy;
}

/**
 * Parsed DELETE BRANCH statement
 */
export interface DeleteBranchStatement {
  type: 'DELETE_BRANCH';
  name: BranchId;
  force?: boolean;
}

/**
 * Parsed SHOW BRANCHES statement
 */
export interface ShowBranchesStatement {
  type: 'SHOW_BRANCHES';
  pattern?: string;
  all?: boolean;
}

/**
 * Parsed BRANCH LOG statement
 */
export interface BranchLogStatement {
  type: 'BRANCH_LOG';
  branch?: BranchId;
  limit?: number;
}

/**
 * Union of all branch SQL statements
 */
export type BranchStatement =
  | CreateBranchStatement
  | CheckoutBranchStatement
  | MergeBranchStatement
  | DeleteBranchStatement
  | ShowBranchesStatement
  | BranchLogStatement;

// =============================================================================
// Error Types
// =============================================================================

/**
 * Branch-specific error codes
 */
export enum BranchErrorCode {
  /** Branch already exists */
  BRANCH_EXISTS = 'BRANCH_EXISTS',
  /** Branch not found */
  BRANCH_NOT_FOUND = 'BRANCH_NOT_FOUND',
  /** Cannot delete current branch */
  CANNOT_DELETE_CURRENT = 'CANNOT_DELETE_CURRENT',
  /** Cannot delete protected branch */
  BRANCH_PROTECTED = 'BRANCH_PROTECTED',
  /** Branch is archived (read-only) */
  BRANCH_ARCHIVED = 'BRANCH_ARCHIVED',
  /** Commit not found */
  COMMIT_NOT_FOUND = 'COMMIT_NOT_FOUND',
  /** Uncommitted changes exist */
  UNCOMMITTED_CHANGES = 'UNCOMMITTED_CHANGES',
  /** Merge conflict */
  MERGE_CONFLICT = 'MERGE_CONFLICT',
  /** Cannot fast-forward */
  NOT_FAST_FORWARD = 'NOT_FAST_FORWARD',
  /** Invalid branch name */
  INVALID_BRANCH_NAME = 'INVALID_BRANCH_NAME',
  /** Branch not fully merged */
  NOT_MERGED = 'NOT_MERGED',
  /** Invalid operation */
  INVALID_OPERATION = 'INVALID_OPERATION',
}

/**
 * Branch operation error
 */
export class BranchError extends Error {
  constructor(
    public readonly code: BranchErrorCode,
    message: string,
    public readonly branch?: BranchId,
    public readonly details?: Record<string, unknown>
  ) {
    super(message);
    this.name = 'BranchError';
  }
}

// =============================================================================
// Branch Manager Interface
// =============================================================================

/**
 * Branch manager interface for git-like operations
 */
export interface BranchManager {
  // Branch Operations
  createBranch(options: CreateBranchOptions): Promise<BranchMetadata>;
  deleteBranch(name: BranchId, options?: DeleteBranchOptions): Promise<void>;
  getBranch(name: BranchId): Promise<BranchMetadata | null>;
  listBranches(pattern?: string): Promise<BranchMetadata[]>;
  renameBranch(oldName: BranchId, newName: BranchId): Promise<void>;

  // Checkout Operations
  getCurrentBranch(): BranchId;
  checkout(name: BranchId, options?: CheckoutOptions): Promise<CheckoutResult>;

  // Merge Operations
  merge(source: BranchId, target: BranchId, options?: MergeOptions): Promise<MergeResult>;
  abortMerge(): Promise<void>;
  resolveMerge(path: string, resolution: 'ours' | 'theirs' | Uint8Array): Promise<void>;

  // History Operations
  log(branch?: BranchId, options?: BranchLogOptions): Promise<BranchLogEntry[]>;
  compare(source: BranchId, target: BranchId): Promise<BranchComparison>;
  findMergeBase(branch1: BranchId, branch2: BranchId): Promise<CommitId | null>;

  // Commit Operations
  commit(message: string, author?: AuthorInfo): Promise<CommitId>;
  getCommit(id: CommitId): Promise<CommitMetadata | null>;

  // Working Tree Operations
  status(): Promise<WorkingTreeStatus>;
  diff(branch?: BranchId): Promise<FileDiff[]>;
}

/**
 * Working tree status
 */
export interface WorkingTreeStatus {
  /** Current branch */
  branch: BranchId;
  /** Current commit (HEAD) */
  head: CommitId | null;
  /** Staged changes */
  staged: FileChange[];
  /** Unstaged changes */
  unstaged: FileChange[];
  /** Untracked files */
  untracked: string[];
  /** Whether there's an ongoing merge */
  merging: boolean;
  /** Conflict files during merge */
  conflicts: string[];
}

/**
 * A file change in working tree
 */
export interface FileChange {
  path: string;
  status: 'added' | 'modified' | 'deleted' | 'renamed';
  oldPath?: string;
}

// =============================================================================
// Constants
// =============================================================================

/** Default branch name */
export const DEFAULT_BRANCH = 'main';

/** Protected branch that cannot be deleted */
export const PROTECTED_BRANCHES = ['main', 'master'];

/** Maximum branch name length */
export const MAX_BRANCH_NAME_LENGTH = 255;

/** Branch name validation regex */
export const BRANCH_NAME_REGEX = /^[a-zA-Z0-9][a-zA-Z0-9._/-]*$/;

/** Reserved branch name prefixes */
export const RESERVED_PREFIXES = ['refs/', '_', '.'];

// =============================================================================
// Validation Utilities
// =============================================================================

/**
 * Validate a branch name
 */
export function isValidBranchName(name: string): boolean {
  if (!name || name.length === 0) return false;
  if (name.length > MAX_BRANCH_NAME_LENGTH) return false;
  if (!BRANCH_NAME_REGEX.test(name)) return false;
  if (RESERVED_PREFIXES.some(prefix => name.startsWith(prefix))) return false;
  if (name.includes('..')) return false;
  if (name.endsWith('.lock')) return false;
  if (name.endsWith('/')) return false;
  return true;
}

/**
 * Sanitize a branch name (make it valid)
 */
export function sanitizeBranchName(name: string): string {
  return name
    .trim()
    .replace(/[^a-zA-Z0-9._/-]/g, '-')
    .replace(/\.{2,}/g, '.')
    .replace(/-{2,}/g, '-')
    .replace(/^\.|^-/, '')
    .replace(/\.lock$/, '')
    .replace(/\/$/, '')
    .slice(0, MAX_BRANCH_NAME_LENGTH);
}

/**
 * Generate a commit ID from content
 */
export function generateCommitId(content: string): CommitId {
  // Simple hash for now - in production would use SHA-256
  let hash = 0;
  for (let i = 0; i < content.length; i++) {
    const char = content.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32-bit integer
  }
  return Math.abs(hash).toString(16).padStart(8, '0') +
         Date.now().toString(16);
}
