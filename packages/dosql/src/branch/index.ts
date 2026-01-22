/**
 * Git-Like Branch Module for DoSQL
 *
 * Provides git-like branching capabilities:
 * - CREATE BRANCH 'feature' FROM 'main'
 * - CHECKOUT BRANCH 'feature'
 * - MERGE BRANCH 'feature' INTO 'main'
 * - Branch isolation (writes don't affect other branches)
 * - Conflict detection on merge
 * - Branch history/log
 * - DELETE BRANCH 'feature'
 * - List branches: SHOW BRANCHES
 *
 * @packageDocumentation
 */

// Types
export {
  type BranchId,
  type BranchMetadata,
  type CreateBranchOptions,
  type DeleteBranchOptions,
  type CommitId,
  type CommitMetadata,
  type AuthorInfo,
  type TreeId,
  type TreeEntry,
  type CheckoutOptions,
  type CheckoutResult,
  type MergeStrategy,
  type MergeOptions,
  type MergeResult,
  type MergeConflict,
  type ConflictVersion,
  type BranchLogEntry,
  type BranchLogOptions,
  type BranchComparison,
  type FileDiff,
  type WorkingTreeStatus,
  type FileChange,
  type BranchManager,
  type CreateBranchStatement,
  type CheckoutBranchStatement,
  type MergeBranchStatement,
  type DeleteBranchStatement,
  type ShowBranchesStatement,
  type BranchLogStatement,
  type BranchStatement,
  BranchError,
  BranchErrorCode,
  DEFAULT_BRANCH,
  PROTECTED_BRANCHES,
  MAX_BRANCH_NAME_LENGTH,
  BRANCH_NAME_REGEX,
  RESERVED_PREFIXES,
  isValidBranchName,
  sanitizeBranchName,
  generateCommitId,
} from './types.js';

// Manager
export {
  DOBranchManager,
  createBranchManager,
  type BranchManagerConfig,
} from './manager.js';

// Merge
export {
  type DiffOp,
  type DiffResult,
  type DiffHunk,
  type ThreeWayMergeResult,
  type MergeConflictRegion,
  type MergeOptions as MergeAlgorithmOptions,
  diff,
  unifiedDiff,
  threeWayMerge,
  resolveConflicts,
  hasConflictMarkers,
  extractConflicts,
  isBinary,
  mergeBinary,
  splitLines,
  applyMergeStrategy,
  createConflictFile,
} from './merge.js';
