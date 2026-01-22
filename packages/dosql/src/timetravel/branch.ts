/**
 * Branch-Level Time Travel for DoSQL
 *
 * Provides time travel capabilities across branch history:
 * - Navigate branch history tree
 * - Find fork points between branches
 * - Find merge base for comparing branches
 * - Track branch lineage
 *
 * @packageDocumentation
 */

import type { FSXBackend } from '../fsx/types.js';
import type {
  Branch,
  Snapshot,
  SnapshotId,
} from '../fsx/cow-types.js';
import { SnapshotManager } from '../fsx/snapshot.js';
import type { COWBackend } from '../fsx/cow-backend.js';
import type { TimePointResolver } from './resolver.js';
import {
  type TimePoint,
  type ResolvedTimePoint,
  type BranchHistoryNode,
  type MergeBaseResult,
  type ForkPoint,
  TimeTravelError,
  TimeTravelErrorCode,
} from './types.js';

// =============================================================================
// Branch History Configuration
// =============================================================================

/**
 * Configuration for branch history operations
 */
export interface BranchHistoryConfig {
  /** Maximum depth to traverse when building history tree */
  maxHistoryDepth: number;
  /** Cache branch history */
  enableCache: boolean;
  /** Cache TTL in milliseconds */
  cacheTTL: number;
}

/**
 * Default branch history configuration
 */
export const DEFAULT_BRANCH_HISTORY_CONFIG: BranchHistoryConfig = {
  maxHistoryDepth: 100,
  enableCache: true,
  cacheTTL: 300000, // 5 minutes
};

// =============================================================================
// Branch History Cache
// =============================================================================

interface HistoryCacheEntry {
  node: BranchHistoryNode;
  expiresAt: number;
}

class BranchHistoryCache {
  private cache = new Map<string, HistoryCacheEntry>();
  private ttl: number;

  constructor(ttl: number) {
    this.ttl = ttl;
  }

  get(branch: string): BranchHistoryNode | null {
    const entry = this.cache.get(branch);
    if (!entry) return null;
    if (Date.now() > entry.expiresAt) {
      this.cache.delete(branch);
      return null;
    }
    return entry.node;
  }

  set(branch: string, node: BranchHistoryNode): void {
    this.cache.set(branch, {
      node,
      expiresAt: Date.now() + this.ttl,
    });
  }

  invalidate(branch: string): void {
    this.cache.delete(branch);
  }

  clear(): void {
    this.cache.clear();
  }
}

// =============================================================================
// Branch History Manager
// =============================================================================

/**
 * Dependencies for branch history
 */
export interface BranchHistoryDeps {
  /** COW backend for branch/snapshot access */
  cowBackend: COWBackend;
  /** Snapshot manager */
  snapshotManager: SnapshotManager;
  /** Time point resolver */
  resolver: TimePointResolver;
}

/**
 * Branch history manager interface
 */
export interface BranchHistoryManager {
  /**
   * Build the history tree starting from a branch
   */
  buildHistoryTree(branch: string): Promise<BranchHistoryNode>;

  /**
   * Get the full lineage of a branch (ancestors up to root)
   */
  getLineage(branch: string): Promise<Branch[]>;

  /**
   * Find the fork point where a branch diverged from its parent
   */
  getForkPoint(branch: string): Promise<ForkPoint | null>;

  /**
   * Find all branches that forked from a given branch
   */
  getChildren(branch: string): Promise<Branch[]>;

  /**
   * Find the common ancestor (merge base) of two branches
   */
  findMergeBase(source: string, target: string): Promise<MergeBaseResult | null>;

  /**
   * Get all snapshots in the path from one branch to another
   */
  getSnapshotPath(
    from: { branch: string; snapshot?: SnapshotId },
    to: { branch: string; snapshot?: SnapshotId }
  ): Promise<Snapshot[]>;

  /**
   * Check if one branch is an ancestor of another
   */
  isAncestor(potentialAncestor: string, descendant: string): Promise<boolean>;

  /**
   * Get the distance (in commits/snapshots) between two points
   */
  getDistance(
    from: TimePoint,
    to: TimePoint,
    branch?: string
  ): Promise<number>;

  /**
   * Clear the history cache
   */
  clearCache(): void;
}

/**
 * Create a branch history manager
 */
export function createBranchHistoryManager(
  deps: BranchHistoryDeps,
  config: Partial<BranchHistoryConfig> = {}
): BranchHistoryManager {
  const fullConfig: BranchHistoryConfig = {
    ...DEFAULT_BRANCH_HISTORY_CONFIG,
    ...config,
  };

  const cache = fullConfig.enableCache
    ? new BranchHistoryCache(fullConfig.cacheTTL)
    : null;

  /**
   * Recursively build history tree for a branch
   */
  async function buildTree(
    branchName: string,
    depth: number,
    visited: Set<string>
  ): Promise<BranchHistoryNode> {
    if (depth > fullConfig.maxHistoryDepth) {
      throw new TimeTravelError(
        TimeTravelErrorCode.DATA_GAP,
        `Branch history too deep (>${fullConfig.maxHistoryDepth})`,
        { type: 'branch', branch: branchName }
      );
    }

    if (visited.has(branchName)) {
      throw new TimeTravelError(
        TimeTravelErrorCode.DATA_GAP,
        `Circular branch reference detected: ${branchName}`,
        { type: 'branch', branch: branchName }
      );
    }

    visited.add(branchName);

    // Check cache
    if (cache) {
      const cached = cache.get(branchName);
      if (cached) return cached;
    }

    const branch = await deps.cowBackend.getBranch(branchName);
    if (!branch) {
      throw new TimeTravelError(
        TimeTravelErrorCode.BRANCH_NOT_FOUND,
        `Branch not found: ${branchName}`,
        { type: 'branch', branch: branchName }
      );
    }

    // Get snapshots for this branch
    const snapshots = await deps.snapshotManager.listSnapshots(branchName);

    // Find child branches
    const allBranches = await deps.cowBackend.listBranches();
    const childBranches = allBranches.filter((b) => b.parent === branchName);

    // Build child nodes
    const children: BranchHistoryNode[] = [];
    for (const child of childBranches) {
      const childNode = await buildTree(child.name, depth + 1, visited);
      children.push(childNode);
    }

    // Determine fork point if this branch has a parent
    let forkPoint: BranchHistoryNode['forkPoint'];
    if (branch.parent && branch.baseSnapshot) {
      const parentSnapshot = await deps.snapshotManager.getSnapshot(
        branch.baseSnapshot
      );
      if (parentSnapshot) {
        forkPoint = {
          parentBranch: branch.parent,
          parentSnapshot: branch.baseSnapshot,
          forkLSN: BigInt(parentSnapshot.version),
          forkTimestamp: new Date(branch.createdAt),
        };
      }
    }

    const node: BranchHistoryNode = {
      branch,
      snapshots,
      children,
      forkPoint,
    };

    // Cache the result
    if (cache) {
      cache.set(branchName, node);
    }

    return node;
  }

  // Public interface
  const manager: BranchHistoryManager = {
    async buildHistoryTree(branch: string): Promise<BranchHistoryNode> {
      return buildTree(branch, 0, new Set());
    },

    async getLineage(branch: string): Promise<Branch[]> {
      const lineage: Branch[] = [];
      let currentBranch = branch;

      while (currentBranch) {
        const branchData = await deps.cowBackend.getBranch(currentBranch);
        if (!branchData) break;

        lineage.push(branchData);

        if (!branchData.parent) break;
        currentBranch = branchData.parent;

        // Safety check for circular references
        if (lineage.length > fullConfig.maxHistoryDepth) {
          throw new TimeTravelError(
            TimeTravelErrorCode.DATA_GAP,
            'Lineage too deep - possible circular reference',
            { type: 'branch', branch }
          );
        }
      }

      return lineage;
    },

    async getForkPoint(branch: string): Promise<ForkPoint | null> {
      const branchData = await deps.cowBackend.getBranch(branch);
      if (!branchData || !branchData.parent || !branchData.baseSnapshot) {
        return null;
      }

      const snapshot = await deps.snapshotManager.getSnapshot(
        branchData.baseSnapshot
      );
      if (!snapshot) {
        return null;
      }

      return {
        parentBranch: branchData.parent,
        childBranch: branch,
        snapshot,
        lsn: BigInt(snapshot.version),
        timestamp: new Date(branchData.createdAt),
      };
    },

    async getChildren(branch: string): Promise<Branch[]> {
      const allBranches = await deps.cowBackend.listBranches();
      return allBranches.filter((b) => b.parent === branch);
    },

    async findMergeBase(
      source: string,
      target: string
    ): Promise<MergeBaseResult | null> {
      // Get lineages for both branches
      const [sourceLineage, targetLineage] = await Promise.all([
        manager.getLineage(source),
        manager.getLineage(target),
      ]);

      // Create a set of target ancestors for fast lookup
      const targetAncestors = new Set(targetLineage.map((b) => b.name));

      // Find the first common ancestor
      let commonAncestor: Branch | null = null;
      let sourceDistance = 0;

      for (const branch of sourceLineage) {
        if (targetAncestors.has(branch.name)) {
          commonAncestor = branch;
          break;
        }
        sourceDistance++;
      }

      if (!commonAncestor) {
        return null;
      }

      // Calculate target distance
      let targetDistance = 0;
      for (const branch of targetLineage) {
        if (branch.name === commonAncestor.name) break;
        targetDistance++;
      }

      // Get the latest snapshot of the common ancestor at the fork point
      const snapshots = await deps.snapshotManager.listSnapshots(
        commonAncestor.name
      );

      // Find the snapshot that was the base for the divergence
      let baseSnapshot: Snapshot | null = null;

      // Check if source branched from common ancestor
      const sourceBranch = await deps.cowBackend.getBranch(source);
      if (sourceBranch?.baseSnapshot) {
        const snap = await deps.snapshotManager.getSnapshot(
          sourceBranch.baseSnapshot
        );
        if (snap && snap.branch === commonAncestor.name) {
          baseSnapshot = snap;
        }
      }

      // If not found, check target
      if (!baseSnapshot) {
        const targetBranch = await deps.cowBackend.getBranch(target);
        if (targetBranch?.baseSnapshot) {
          const snap = await deps.snapshotManager.getSnapshot(
            targetBranch.baseSnapshot
          );
          if (snap && snap.branch === commonAncestor.name) {
            baseSnapshot = snap;
          }
        }
      }

      // Fall back to latest snapshot
      if (!baseSnapshot && snapshots.length > 0) {
        baseSnapshot = snapshots[0];
      }

      if (!baseSnapshot) {
        return null;
      }

      return {
        snapshot: baseSnapshot,
        lsn: BigInt(baseSnapshot.version),
        timestamp: new Date(baseSnapshot.createdAt),
        sourceDistance,
        targetDistance,
      };
    },

    async getSnapshotPath(
      from: { branch: string; snapshot?: SnapshotId },
      to: { branch: string; snapshot?: SnapshotId }
    ): Promise<Snapshot[]> {
      const path: Snapshot[] = [];

      // If same branch, just get snapshots in range
      if (from.branch === to.branch) {
        const snapshots = await deps.snapshotManager.listSnapshots(from.branch);

        let fromVersion = from.snapshot
          ? parseInt(from.snapshot.split('@')[1], 10)
          : 0;
        let toVersion = to.snapshot
          ? parseInt(to.snapshot.split('@')[1], 10)
          : Infinity;

        // Snapshots are sorted newest first
        for (const snap of snapshots.reverse()) {
          if (snap.version >= fromVersion && snap.version <= toVersion) {
            path.push(snap);
          }
        }

        return path;
      }

      // Different branches - need to find path through common ancestor
      const mergeBase = await manager.findMergeBase(from.branch, to.branch);
      if (!mergeBase) {
        throw new TimeTravelError(
          TimeTravelErrorCode.DATA_GAP,
          `No common ancestor between ${from.branch} and ${to.branch}`,
          { type: 'branch', branch: from.branch }
        );
      }

      // Get snapshots from 'from' back to merge base
      const fromLineage = await manager.getLineage(from.branch);
      for (const branch of fromLineage) {
        const branchSnapshots = await deps.snapshotManager.listSnapshots(
          branch.name
        );
        for (const snap of branchSnapshots.reverse()) {
          if (snap.version <= (from.snapshot ? parseInt(from.snapshot.split('@')[1], 10) : Infinity)) {
            path.push(snap);
            if (snap.id === mergeBase.snapshot.id) break;
          }
        }
        if (branch.name === mergeBase.snapshot.branch) break;
      }

      // Get snapshots from merge base to 'to'
      const toLineage = await manager.getLineage(to.branch);
      const toPath: Snapshot[] = [];
      for (const branch of toLineage.reverse()) {
        if (branch.name === mergeBase.snapshot.branch) continue;
        const branchSnapshots = await deps.snapshotManager.listSnapshots(
          branch.name
        );
        for (const snap of branchSnapshots.reverse()) {
          if (snap.version <= (to.snapshot ? parseInt(to.snapshot.split('@')[1], 10) : Infinity)) {
            toPath.push(snap);
          }
        }
      }

      return [...path, ...toPath];
    },

    async isAncestor(
      potentialAncestor: string,
      descendant: string
    ): Promise<boolean> {
      if (potentialAncestor === descendant) return true;

      const lineage = await manager.getLineage(descendant);
      return lineage.some((b) => b.name === potentialAncestor);
    },

    async getDistance(
      from: TimePoint,
      to: TimePoint,
      branch?: string
    ): Promise<number> {
      const [resolvedFrom, resolvedTo] = await Promise.all([
        deps.resolver.resolve(from, branch),
        deps.resolver.resolve(to, branch),
      ]);

      // If on same branch, count snapshots between them
      if (resolvedFrom.branch === resolvedTo.branch) {
        const snapshots = await deps.snapshotManager.listSnapshots(
          resolvedFrom.branch
        );

        let count = 0;
        let counting = false;

        for (const snap of snapshots.reverse()) {
          if (BigInt(snap.version) >= resolvedFrom.lsn) {
            counting = true;
          }
          if (counting) {
            count++;
          }
          if (BigInt(snap.version) >= resolvedTo.lsn) {
            break;
          }
        }

        return count;
      }

      // Different branches - count through common ancestor
      const snapshotPath = await manager.getSnapshotPath(
        { branch: resolvedFrom.branch, snapshot: resolvedFrom.snapshotId },
        { branch: resolvedTo.branch, snapshot: resolvedTo.snapshotId }
      );

      return snapshotPath.length;
    },

    clearCache(): void {
      cache?.clear();
    },
  };

  return manager;
}

// =============================================================================
// Branch Comparison Utilities
// =============================================================================

/**
 * Result of comparing two branch states
 */
export interface BranchCompareResult {
  /** Source branch */
  source: string;
  /** Target branch */
  target: string;
  /** Common ancestor */
  mergeBase: MergeBaseResult | null;
  /** Number of commits source is ahead */
  sourceAhead: number;
  /** Number of commits source is behind */
  sourceBehind: number;
  /** Whether branches have diverged (both have unique commits) */
  diverged: boolean;
  /** Whether source can fast-forward to target */
  canFastForward: boolean;
}

/**
 * Compare two branches to understand their relationship
 */
export async function compareBranches(
  manager: BranchHistoryManager,
  source: string,
  target: string
): Promise<BranchCompareResult> {
  const [sourceLineage, targetLineage, mergeBase] = await Promise.all([
    manager.getLineage(source),
    manager.getLineage(target),
    manager.findMergeBase(source, target),
  ]);

  const sourceAhead = mergeBase?.sourceDistance ?? sourceLineage.length;
  const sourceBehind = mergeBase?.targetDistance ?? targetLineage.length;

  const diverged = sourceAhead > 0 && sourceBehind > 0;
  const canFastForward = sourceAhead === 0 && sourceBehind > 0;

  return {
    source,
    target,
    mergeBase,
    sourceAhead,
    sourceBehind,
    diverged,
    canFastForward,
  };
}

/**
 * Find all branches containing a specific snapshot or LSN
 */
export async function findBranchesContaining(
  cowBackend: COWBackend,
  snapshotManager: SnapshotManager,
  point: { lsn?: bigint; snapshotId?: SnapshotId }
): Promise<string[]> {
  const branches = await cowBackend.listBranches();
  const result: string[] = [];

  for (const branch of branches) {
    const snapshots = await snapshotManager.listSnapshots(branch.name);

    if (point.snapshotId) {
      if (snapshots.some((s) => s.id === point.snapshotId)) {
        result.push(branch.name);
      }
    } else if (point.lsn !== undefined) {
      // Check if any snapshot contains this LSN
      for (const snap of snapshots) {
        if (BigInt(snap.version) >= point.lsn) {
          result.push(branch.name);
          break;
        }
      }
    }
  }

  return result;
}

/**
 * Get the full branch tree starting from root (main)
 */
export async function getFullBranchTree(
  manager: BranchHistoryManager,
  rootBranch: string = 'main'
): Promise<BranchHistoryNode> {
  return manager.buildHistoryTree(rootBranch);
}

/**
 * Flatten a branch tree into a list
 */
export function flattenBranchTree(node: BranchHistoryNode): Branch[] {
  const result: Branch[] = [node.branch];

  for (const child of node.children) {
    result.push(...flattenBranchTree(child));
  }

  return result;
}

/**
 * Find a branch in the tree
 */
export function findBranchInTree(
  node: BranchHistoryNode,
  branchName: string
): BranchHistoryNode | null {
  if (node.branch.name === branchName) {
    return node;
  }

  for (const child of node.children) {
    const found = findBranchInTree(child, branchName);
    if (found) return found;
  }

  return null;
}
