/**
 * Merge Engine for COW FSX
 *
 * Handles branch merging with various conflict resolution strategies.
 * Supports three-way merge when common ancestor is available.
 *
 * Merge Strategies:
 * - ours: Keep target branch version on conflict
 * - theirs: Keep source branch version on conflict
 * - last-write-wins: Keep most recently modified version
 * - fail-on-conflict: Fail if any conflicts detected
 */

import type { FSXBackend } from './types.js';
import type { COWBackend } from './cow-backend.js';
import {
  type BlobRef,
  type Branch,
  type MergeStrategy,
  type MergeResult,
  type MergeConflict,
  type BranchDiff,
  type SnapshotId,
  type Snapshot,
  COWError,
  COWErrorCode,
} from './cow-types.js';

// =============================================================================
// Merge Engine
// =============================================================================

/**
 * Handles merge operations between branches
 */
export class MergeEngine {
  private readonly storage: FSXBackend;
  private readonly cow: COWBackend;

  constructor(storage: FSXBackend, cow: COWBackend) {
    this.storage = storage;
    this.cow = cow;
  }

  // ===========================================================================
  // Branch Comparison
  // ===========================================================================

  /**
   * Compare two branches to find differences
   */
  async diff(source: string, target: string): Promise<BranchDiff> {
    // Get all paths from both branches
    const [sourcePaths, targetPaths] = await Promise.all([
      this.cow.listFrom('', source),
      this.cow.listFrom('', target),
    ]);

    const sourceSet = new Set(sourcePaths);
    const targetSet = new Set(targetPaths);

    const added: string[] = [];
    const removed: string[] = [];
    const modified: string[] = [];
    const unchanged: string[] = [];

    // Find added (in source but not target)
    for (const path of sourcePaths) {
      if (!targetSet.has(path)) {
        added.push(path);
      }
    }

    // Find removed (in target but not source)
    for (const path of targetPaths) {
      if (!sourceSet.has(path)) {
        removed.push(path);
      }
    }

    // Find modified and unchanged (in both)
    for (const path of sourcePaths) {
      if (targetSet.has(path)) {
        const [sourceRef, targetRef] = await Promise.all([
          this.cow.getRefInternal(path, source),
          this.cow.getRefInternal(path, target),
        ]);

        if (sourceRef && targetRef) {
          if (this.refsAreEqual(sourceRef, targetRef)) {
            unchanged.push(path);
          } else {
            modified.push(path);
          }
        }
      }
    }

    return {
      added: added.sort(),
      removed: removed.sort(),
      modified: modified.sort(),
      unchanged: unchanged.sort(),
    };
  }

  // ===========================================================================
  // Merge Operations
  // ===========================================================================

  /**
   * Merge source branch into target branch
   */
  async merge(source: string, target: string, strategy: MergeStrategy): Promise<MergeResult> {
    // Validate branches exist
    const [sourceBranch, targetBranch] = await Promise.all([
      this.cow.getBranch(source),
      this.cow.getBranch(target),
    ]);

    if (!sourceBranch) {
      throw new COWError(COWErrorCode.BRANCH_NOT_FOUND, `Source branch not found: ${source}`);
    }
    if (!targetBranch) {
      throw new COWError(COWErrorCode.BRANCH_NOT_FOUND, `Target branch not found: ${target}`);
    }
    if (targetBranch.readonly) {
      throw new COWError(COWErrorCode.BRANCH_READONLY, `Target branch is read-only: ${target}`);
    }

    // Get diff
    const diff = await this.diff(source, target);

    // Detect conflicts (paths modified in both branches since common ancestor)
    const conflicts = await this.detectConflicts(source, target, diff.modified);

    // Check if we should fail on conflicts
    if (strategy === 'fail-on-conflict' && conflicts.length > 0) {
      return {
        success: false,
        merged: 0,
        conflicts,
        strategy,
        updated: [],
        deleted: [],
      };
    }

    // Perform merge
    const updated: string[] = [];
    const deleted: string[] = [];
    let merged = 0;

    // Add new paths from source
    for (const path of diff.added) {
      const data = await this.cow.readFrom(path, source);
      if (data) {
        await this.cow.writeTo(path, data, target);
        updated.push(path);
        merged++;
      }
    }

    // Handle modified paths based on strategy
    for (const path of diff.modified) {
      const conflict = conflicts.find((c) => c.path === path);
      const resolution = await this.resolveConflict(source, target, path, strategy, conflict);

      if (resolution.action === 'take-source') {
        const data = await this.cow.readFrom(path, source);
        if (data) {
          await this.cow.writeTo(path, data, target);
          updated.push(path);
        }
      }
      // 'keep-target' means no action needed

      merged++;
    }

    // Create post-merge snapshot
    const snapshotId = await this.cow.snapshot(target, `Merge ${source} into ${target}`);

    return {
      success: true,
      merged,
      conflicts,
      strategy,
      snapshot: snapshotId,
      updated,
      deleted,
    };
  }

  // ===========================================================================
  // Conflict Detection
  // ===========================================================================

  /**
   * Detect conflicts for modified paths
   * A conflict exists when both branches have modified a path since their common ancestor
   */
  async detectConflicts(
    source: string,
    target: string,
    modifiedPaths: string[]
  ): Promise<MergeConflict[]> {
    const conflicts: MergeConflict[] = [];

    // Find common ancestor (base snapshot)
    const baseSnapshot = await this.findCommonAncestor(source, target);

    for (const path of modifiedPaths) {
      const [sourceRef, targetRef] = await Promise.all([
        this.cow.getRefInternal(path, source),
        this.cow.getRefInternal(path, target),
      ]);

      if (!sourceRef || !targetRef) continue;

      // Check if both modified since base
      let baseVersion: number | undefined;

      if (baseSnapshot) {
        const baseEntry = baseSnapshot.manifest.entries.find((e) => e.path === path);
        baseVersion = baseEntry?.version;

        // If either version equals base, no conflict
        if (sourceRef.version === baseVersion || targetRef.version === baseVersion) {
          continue;
        }
      }

      // Both branches modified - this is a conflict
      conflicts.push({
        path,
        sourceVersion: sourceRef.version,
        targetVersion: targetRef.version,
        baseVersion,
        sourceModifiedAt: sourceRef.modifiedAt,
        targetModifiedAt: targetRef.modifiedAt,
      });
    }

    return conflicts;
  }

  /**
   * Find the common ancestor snapshot between two branches
   */
  async findCommonAncestor(source: string, target: string): Promise<Snapshot | null> {
    const sourceBranch = await this.cow.getBranch(source);
    const targetBranch = await this.cow.getBranch(target);

    if (!sourceBranch || !targetBranch) return null;

    // Check if one is parent of the other
    if (sourceBranch.parent === target && sourceBranch.baseSnapshot) {
      return this.cow.getSnapshot(sourceBranch.baseSnapshot);
    }

    if (targetBranch.parent === source && targetBranch.baseSnapshot) {
      return this.cow.getSnapshot(targetBranch.baseSnapshot);
    }

    // Check for common parent
    if (sourceBranch.parent === targetBranch.parent && sourceBranch.parent) {
      // Both branched from same parent - find the older base snapshot
      if (sourceBranch.baseSnapshot && targetBranch.baseSnapshot) {
        const [sourceBase, targetBase] = await Promise.all([
          this.cow.getSnapshot(sourceBranch.baseSnapshot),
          this.cow.getSnapshot(targetBranch.baseSnapshot),
        ]);

        if (sourceBase && targetBase) {
          // Return the older (earlier) snapshot as common ancestor
          return sourceBase.createdAt < targetBase.createdAt ? sourceBase : targetBase;
        }
      }
    }

    // No common ancestor found
    return null;
  }

  // ===========================================================================
  // Conflict Resolution
  // ===========================================================================

  /**
   * Resolve a conflict based on strategy
   */
  async resolveConflict(
    source: string,
    target: string,
    path: string,
    strategy: MergeStrategy,
    conflict?: MergeConflict
  ): Promise<ConflictResolution> {
    switch (strategy) {
      case 'ours':
        // Keep target (ours)
        return { action: 'keep-target', reason: 'Strategy: ours' };

      case 'theirs':
        // Take source (theirs)
        return { action: 'take-source', reason: 'Strategy: theirs' };

      case 'last-write-wins':
        // Compare modification times
        if (conflict) {
          if (conflict.sourceModifiedAt > conflict.targetModifiedAt) {
            return { action: 'take-source', reason: 'Source modified more recently' };
          } else {
            return { action: 'keep-target', reason: 'Target modified more recently' };
          }
        }
        // Fallback to theirs if no conflict info
        return { action: 'take-source', reason: 'Fallback to source' };

      case 'fail-on-conflict':
        // Should have been handled earlier
        throw new COWError(
          COWErrorCode.MERGE_CONFLICT,
          `Conflict at ${path} with fail-on-conflict strategy`
        );

      default:
        return { action: 'keep-target', reason: 'Unknown strategy - keeping target' };
    }
  }

  // ===========================================================================
  // Three-Way Merge
  // ===========================================================================

  /**
   * Perform three-way merge for a specific path
   * Returns merged content or null if auto-merge not possible
   */
  async threeWayMerge(
    source: string,
    target: string,
    path: string,
    baseSnapshot?: Snapshot
  ): Promise<ThreeWayMergeResult> {
    const [sourceData, targetData] = await Promise.all([
      this.cow.readFrom(path, source),
      this.cow.readFrom(path, target),
    ]);

    // Get base data if available
    let baseData: Uint8Array | null = null;
    if (baseSnapshot) {
      const entry = baseSnapshot.manifest.entries.find((e) => e.path === path);
      if (entry) {
        baseData = await this.cow.readAt(path, baseSnapshot.id);
      }
    }

    // If either is missing, simple case
    if (!sourceData && targetData) {
      return { result: 'target-only', data: targetData };
    }
    if (sourceData && !targetData) {
      return { result: 'source-only', data: sourceData };
    }
    if (!sourceData && !targetData) {
      return { result: 'both-deleted', data: null };
    }

    // Both exist - check for equality
    if (sourceData && targetData) {
      if (this.dataEqual(sourceData, targetData)) {
        return { result: 'identical', data: sourceData };
      }

      // Check if one matches base
      if (baseData) {
        if (this.dataEqual(sourceData, baseData)) {
          // Source unchanged, target modified - take target
          return { result: 'target-modified', data: targetData };
        }
        if (this.dataEqual(targetData, baseData)) {
          // Target unchanged, source modified - take source
          return { result: 'source-modified', data: sourceData };
        }
      }

      // Both modified differently - conflict
      return {
        result: 'conflict',
        data: null,
        sourceData,
        targetData,
        baseData,
      };
    }

    return { result: 'unknown', data: null };
  }

  // ===========================================================================
  // Utility Methods
  // ===========================================================================

  /**
   * Check if two refs point to the same content
   */
  private refsAreEqual(a: BlobRef, b: BlobRef): boolean {
    // Compare by hash if available
    if (a.hash && b.hash) {
      return a.hash === b.hash;
    }

    // Compare by version if same branch
    if (a.branch === b.branch) {
      return a.version === b.version;
    }

    // Cannot determine equality
    return false;
  }

  /**
   * Check if two data arrays are equal
   */
  private dataEqual(a: Uint8Array, b: Uint8Array): boolean {
    if (a.length !== b.length) return false;

    for (let i = 0; i < a.length; i++) {
      if (a[i] !== b[i]) return false;
    }

    return true;
  }
}

// =============================================================================
// Types
// =============================================================================

/**
 * Result of conflict resolution
 */
interface ConflictResolution {
  action: 'take-source' | 'keep-target' | 'custom';
  reason: string;
  customData?: Uint8Array;
}

/**
 * Result of three-way merge attempt
 */
interface ThreeWayMergeResult {
  result:
    | 'identical'
    | 'source-only'
    | 'target-only'
    | 'source-modified'
    | 'target-modified'
    | 'both-deleted'
    | 'conflict'
    | 'unknown';
  data: Uint8Array | null;
  sourceData?: Uint8Array;
  targetData?: Uint8Array;
  baseData?: Uint8Array | null;
}

// =============================================================================
// Merge Utilities
// =============================================================================

/**
 * Create a merge preview without actually merging
 */
export async function previewMerge(
  cow: COWBackend,
  source: string,
  target: string,
  strategy: MergeStrategy
): Promise<{
  diff: BranchDiff;
  conflicts: MergeConflict[];
  wouldSucceed: boolean;
}> {
  const mergeEngine = new MergeEngine(cow.getStorage(), cow);
  const diff = await mergeEngine.diff(source, target);
  const conflicts = await mergeEngine.detectConflicts(source, target, diff.modified);

  const wouldSucceed = strategy !== 'fail-on-conflict' || conflicts.length === 0;

  return { diff, conflicts, wouldSucceed };
}

/**
 * Check if a branch can be fast-forwarded to another
 * Fast-forward is possible when target has no changes since the common ancestor
 */
export async function canFastForward(
  cow: COWBackend,
  source: string,
  target: string
): Promise<boolean> {
  const mergeEngine = new MergeEngine(cow.getStorage(), cow);
  const diff = await mergeEngine.diff(source, target);

  // Fast-forward possible if:
  // - No paths removed from source (that exist in target)
  // - No paths modified in target since branching
  // In practice, this means target has no unique changes

  const targetOnlyPaths = diff.removed; // Paths in target but not source

  // If there are paths only in target, we can't fast-forward
  return targetOnlyPaths.length === 0;
}

/**
 * Perform a fast-forward merge
 */
export async function fastForwardMerge(
  cow: COWBackend,
  source: string,
  target: string
): Promise<MergeResult> {
  const canFF = await canFastForward(cow, source, target);

  if (!canFF) {
    return {
      success: false,
      merged: 0,
      conflicts: [],
      strategy: 'fail-on-conflict',
      updated: [],
      deleted: [],
    };
  }

  // Fast-forward: just copy all refs from source to target
  return cow.merge(source, target, 'theirs');
}
