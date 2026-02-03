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

import {
  DoSQLError,
  ErrorCategory,
  registerErrorClass,
  type ErrorContext,
  type SerializedError,
} from '../errors/base.js';

// =============================================================================
// Branded Types
// =============================================================================

/** Brand symbol for Branch ID */
declare const BranchIdBrand: unique symbol;

/**
 * Branch ID - A branded string type for branch identifiers.
 * Provides type safety to prevent accidental assignment from plain strings.
 *
 * @example
 * const branchId = createBranchId('main');
 * // branchId is BranchId, not assignable from plain string
 */
export type BranchId = string & { readonly [BranchIdBrand]: never };

/**
 * Create a branded BranchId from a string value.
 * This is the only safe way to create a BranchId.
 *
 * @param value - The string value for the branch ID
 * @returns A branded BranchId value
 * @throws {Error} If value is empty or exceeds MAX_BRANCH_NAME_LENGTH
 */
export function createBranchId(value: string): BranchId {
  if (!value || value.length === 0) {
    throw new Error('BranchId cannot be empty');
  }
  if (value.length > MAX_BRANCH_NAME_LENGTH) {
    throw new Error(`BranchId exceeds maximum length of ${MAX_BRANCH_NAME_LENGTH}: ${value}`);
  }
  return value as BranchId;
}

/**
 * Type guard to check if a value is a valid BranchId candidate.
 */
export function isValidBranchIdCandidate(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= MAX_BRANCH_NAME_LENGTH;
}

/** Brand symbol for Commit ID */
declare const CommitIdBrand: unique symbol;

/**
 * Commit ID - A branded string type for commit identifiers (content-addressed hash).
 * Provides type safety to prevent accidental assignment from plain strings.
 *
 * @example
 * const commitId = createCommitId('abc123def456');
 * // commitId is CommitId, not assignable from plain string
 */
export type CommitId = string & { readonly [CommitIdBrand]: never };

/**
 * Create a branded CommitId from a string value.
 * This is the only safe way to create a CommitId.
 *
 * @param value - The string value for the commit ID (typically a hash)
 * @returns A branded CommitId value
 * @throws {Error} If value is empty
 */
export function createCommitId(value: string): CommitId {
  if (!value || value.length === 0) {
    throw new Error('CommitId cannot be empty');
  }
  return value as CommitId;
}

/**
 * Type guard to check if a value is a valid CommitId candidate.
 */
export function isValidCommitIdCandidate(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

/** Brand symbol for Tree ID */
declare const TreeIdBrand: unique symbol;

/**
 * Tree ID - A branded string type for tree identifiers (hash of directory structure).
 * Provides type safety to prevent accidental assignment from plain strings.
 *
 * @example
 * const treeId = createTreeId('tree123hash');
 * // treeId is TreeId, not assignable from plain string
 */
export type TreeId = string & { readonly [TreeIdBrand]: never };

/**
 * Create a branded TreeId from a string value.
 * This is the only safe way to create a TreeId.
 *
 * @param value - The string value for the tree ID (typically a hash)
 * @returns A branded TreeId value
 * @throws {Error} If value is empty
 */
export function createTreeId(value: string): TreeId {
  if (!value || value.length === 0) {
    throw new Error('TreeId cannot be empty');
  }
  return value as TreeId;
}

/**
 * Type guard to check if a value is a valid TreeId candidate.
 */
export function isValidTreeIdCandidate(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

// =============================================================================
// Branch Core Types
// =============================================================================

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
  description?: string | undefined;
  /** Latest commit on this branch */
  head: CommitId | null;
}

/**
 * Options for creating a new branch
 * Note: String values are accepted and will be converted to branded types internally.
 */
export interface CreateBranchOptions {
  /** Branch name */
  name: BranchId | string;
  /** Source branch to create from (default: current branch) */
  from?: BranchId | string | undefined;
  /** Specific commit to branch from (default: HEAD of source) */
  commit?: CommitId | string | undefined;
  /** Optional description */
  description?: string | undefined;
}

/**
 * Options for deleting a branch
 */
export interface DeleteBranchOptions {
  /** Force delete even if not merged */
  force?: boolean | undefined;
  /** Delete remote tracking branch */
  deleteRemote?: boolean | undefined;
}

// =============================================================================
// Commit Types
// =============================================================================

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
  metadata?: Record<string, unknown> | undefined;
}

/**
 * Author information for commits
 */
export interface AuthorInfo {
  name: string;
  email?: string | undefined;
}

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
  size?: number | undefined;
  /** File mode (permissions) */
  mode?: number | undefined;
}

// =============================================================================
// Checkout Types
// =============================================================================

/**
 * Checkout options
 * Note: String values are accepted and will be converted to branded types internally.
 */
export interface CheckoutOptions {
  /** Create branch if it doesn't exist */
  create?: boolean | undefined;
  /** Force checkout (discard uncommitted changes) */
  force?: boolean | undefined;
  /** Checkout specific commit instead of branch HEAD */
  commit?: CommitId | string | undefined;
}

/**
 * Result of checkout operation
 */
export interface CheckoutResult {
  /** Previous branch/commit */
  previous: {
    branch?: BranchId | undefined;
    commit?: CommitId | undefined;
  };
  /** Current branch/commit after checkout */
  current: {
    branch: BranchId;
    commit: CommitId | null;
  };
  /** Files that were changed */
  changed: string[];
  /** Files that had conflicts (when force=false) */
  conflicts?: string[] | undefined;
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
  strategy?: MergeStrategy | undefined;
  /** Commit message for merge commit */
  message?: string | undefined;
  /** Abort merge on conflict */
  abortOnConflict?: boolean | undefined;
  /** Auto-resolve conflicts with given resolution */
  autoResolve?: 'ours' | 'theirs' | undefined;
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
  commit?: CommitId | undefined;
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
  ours?: ConflictVersion | undefined;
  /** Their version (source branch) */
  theirs?: ConflictVersion | undefined;
  /** Base version (common ancestor) */
  base?: ConflictVersion | undefined;
  /** Resolution status */
  resolved: boolean;
  /** Resolution if resolved */
  resolution?: 'ours' | 'theirs' | 'merged' | 'deleted' | undefined;
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
  limit?: number | undefined;
  /** Skip this many entries */
  skip?: number | undefined;
  /** Only include commits after this date */
  since?: Date | undefined;
  /** Only include commits before this date */
  until?: Date | undefined;
  /** Only include commits affecting these paths */
  paths?: string[] | undefined;
  /** Include commits from all branches */
  all?: boolean | undefined;
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
  oldPath?: string | undefined;
  /** Additions count */
  additions?: number | undefined;
  /** Deletions count */
  deletions?: number | undefined;
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
  from?: BranchId | undefined;
  commit?: CommitId | undefined;
}

/**
 * Parsed CHECKOUT BRANCH statement
 */
export interface CheckoutBranchStatement {
  type: 'CHECKOUT_BRANCH';
  name: BranchId;
  create?: boolean | undefined;
}

/**
 * Parsed MERGE BRANCH statement
 */
export interface MergeBranchStatement {
  type: 'MERGE_BRANCH';
  source: BranchId;
  into: BranchId;
  strategy?: MergeStrategy | undefined;
}

/**
 * Parsed DELETE BRANCH statement
 */
export interface DeleteBranchStatement {
  type: 'DELETE_BRANCH';
  name: BranchId;
  force?: boolean | undefined;
}

/**
 * Parsed SHOW BRANCHES statement
 */
export interface ShowBranchesStatement {
  type: 'SHOW_BRANCHES';
  pattern?: string | undefined;
  all?: boolean | undefined;
}

/**
 * Parsed BRANCH LOG statement
 */
export interface BranchLogStatement {
  type: 'BRANCH_LOG';
  branch?: BranchId | undefined;
  limit?: number | undefined;
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
 * Error class for branch operations
 *
 * Extends DoSQLError for unified error handling across the DoSQL ecosystem.
 */
export class BranchError extends DoSQLError {
  readonly code: BranchErrorCode;
  readonly category: ErrorCategory;
  readonly branch?: BranchId | string;
  readonly details?: Record<string, unknown>;

  constructor(
    code: BranchErrorCode,
    message: string,
    branch?: BranchId | string,
    details?: Record<string, unknown>
  ) {
    super(message);
    this.name = 'BranchError';
    this.code = code;
    this.branch = branch;
    this.details = details;
    this.category = this.determineCategory();

    if (this.branch || this.details) {
      this.context = {
        metadata: {
          ...(this.branch && { branch: this.branch }),
          ...(this.details && { details: this.details }),
        },
      };
    }
  }

  private determineCategory(): ErrorCategory {
    switch (this.code) {
      case BranchErrorCode.BRANCH_NOT_FOUND:
      case BranchErrorCode.COMMIT_NOT_FOUND:
        return ErrorCategory.RESOURCE;
      case BranchErrorCode.BRANCH_EXISTS:
      case BranchErrorCode.MERGE_CONFLICT:
      case BranchErrorCode.NOT_FAST_FORWARD:
      case BranchErrorCode.NOT_MERGED:
        return ErrorCategory.CONFLICT;
      case BranchErrorCode.UNCOMMITTED_CHANGES:
      case BranchErrorCode.INVALID_BRANCH_NAME:
      case BranchErrorCode.INVALID_OPERATION:
        return ErrorCategory.VALIDATION;
      default:
        return ErrorCategory.EXECUTION;
    }
  }

  override isRetryable(): boolean {
    return false;
  }

  static fromJSON(json: SerializedError): BranchError {
    return new BranchError(
      json.code as BranchErrorCode,
      json.message,
      json.context?.metadata?.branch as string | undefined,
      json.context?.metadata?.details as Record<string, unknown> | undefined
    );
  }
}

registerErrorClass('BranchError', BranchError);

// =============================================================================
// Branch Manager Interface
// =============================================================================

/**
 * Branch manager interface for git-like operations
 * Note: Methods accept both branded types and plain strings for convenience.
 * Plain strings are converted to branded types internally.
 */
export interface BranchManager {
  // Branch Operations
  createBranch(options: CreateBranchOptions): Promise<BranchMetadata>;
  deleteBranch(name: BranchId | string, options?: DeleteBranchOptions): Promise<void>;
  getBranch(name: BranchId | string): Promise<BranchMetadata | null>;
  listBranches(pattern?: string): Promise<BranchMetadata[]>;
  renameBranch(oldName: BranchId | string, newName: BranchId | string): Promise<void>;

  // Checkout Operations
  getCurrentBranch(): BranchId;
  checkout(name: BranchId | string, options?: CheckoutOptions): Promise<CheckoutResult>;

  // Merge Operations
  merge(source: BranchId | string, target: BranchId | string, options?: MergeOptions): Promise<MergeResult>;
  abortMerge(): Promise<void>;
  resolveMerge(path: string, resolution: 'ours' | 'theirs' | Uint8Array): Promise<void>;

  // History Operations
  log(branch?: BranchId | string, options?: BranchLogOptions): Promise<BranchLogEntry[]>;
  compare(source: BranchId | string, target: BranchId | string): Promise<BranchComparison>;
  findMergeBase(branch1: BranchId | string, branch2: BranchId | string): Promise<CommitId | null>;

  // Commit Operations
  commit(message: string, author?: AuthorInfo): Promise<CommitId>;
  getCommit(id: CommitId | string): Promise<CommitMetadata | null>;

  // Working Tree Operations
  status(): Promise<WorkingTreeStatus>;
  diff(branch?: BranchId | string): Promise<FileDiff[]>;
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
  oldPath?: string | undefined;
}

// =============================================================================
// Constants
// =============================================================================

/** Default branch name */
export const DEFAULT_BRANCH = 'main' as BranchId;

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
  const hashString = Math.abs(hash).toString(16).padStart(8, '0') +
         Date.now().toString(16);
  return createCommitId(hashString);
}
