/**
 * Git-Like Branch Manager for DoSQL
 *
 * Implements git-like branching with:
 * - Branch creation, deletion, and listing
 * - Checkout between branches
 * - Commit management
 * - Working tree status
 * - Copy-on-write for efficient storage
 *
 * @packageDocumentation
 */

import type { DurableObjectStorage } from '@cloudflare/workers-types';
import {
  type BranchManager,
  type BranchMetadata,
  type BranchId,
  type CommitId,
  type CommitMetadata,
  type TreeId,
  type TreeEntry,
  type AuthorInfo,
  type CreateBranchOptions,
  type DeleteBranchOptions,
  type CheckoutOptions,
  type CheckoutResult,
  type MergeOptions,
  type MergeResult,
  type BranchLogEntry,
  type BranchLogOptions,
  type BranchComparison,
  type FileDiff,
  type WorkingTreeStatus,
  type FileChange,
  BranchError,
  BranchErrorCode,
  DEFAULT_BRANCH,
  PROTECTED_BRANCHES,
  isValidBranchName,
  generateCommitId,
  createBranchId,
  createCommitId,
  createTreeId,
} from './types.js';

// =============================================================================
// Storage Keys
// =============================================================================

const BRANCH_PREFIX = '_branch/meta/';
const COMMIT_PREFIX = '_branch/commit/';
const TREE_PREFIX = '_branch/tree/';
const BLOB_PREFIX = '_branch/blob/';
const HEAD_KEY = '_branch/HEAD';
const INDEX_KEY = '_branch/INDEX';
const MERGE_STATE_KEY = '_branch/MERGE_HEAD';

// =============================================================================
// Branch Manager Implementation
// =============================================================================

/**
 * Configuration for the branch manager
 */
export interface BranchManagerConfig {
  /** Default branch name */
  defaultBranch?: BranchId | string;
  /** Default author info */
  author?: AuthorInfo;
  /** Auto-commit on branch operations */
  autoCommit?: boolean;
}

/** Internal resolved config type */
interface ResolvedBranchManagerConfig {
  defaultBranch: BranchId;
  author: AuthorInfo;
  autoCommit: boolean;
}

const DEFAULT_CONFIG: ResolvedBranchManagerConfig = {
  defaultBranch: DEFAULT_BRANCH,
  author: { name: 'DoSQL System' },
  autoCommit: false,
};

/**
 * Index entry for a file in the working tree
 */
interface IndexEntry {
  path: string;
  hash: string;
  mode: number;
  size: number;
  mtime: number;
  staged: boolean;
}

/**
 * Internal merge state
 */
interface MergeState {
  source: BranchId;
  target: BranchId;
  baseCommit: CommitId | null;
  conflicts: string[];
  resolved: Map<string, 'ours' | 'theirs' | string>;
}

/**
 * DoSQL Branch Manager
 *
 * Provides git-like branching capabilities for Durable Objects
 */
export class DOBranchManager implements BranchManager {
  private readonly storage: DurableObjectStorage;
  private readonly config: ResolvedBranchManagerConfig;
  private currentBranch: BranchId;
  private initialized = false;

  // In-memory caches
  private branchCache = new Map<BranchId, BranchMetadata>();
  private commitCache = new Map<CommitId, CommitMetadata>();
  private indexCache: Map<string, IndexEntry> | null = null;
  private mergeState: MergeState | null = null;

  constructor(storage: DurableObjectStorage, config: BranchManagerConfig = {}) {
    this.storage = storage;
    const defaultBranch = config.defaultBranch
      ? createBranchId(config.defaultBranch as string)
      : DEFAULT_BRANCH;
    this.config = {
      defaultBranch,
      author: config.author ?? DEFAULT_CONFIG.author,
      autoCommit: config.autoCommit ?? DEFAULT_CONFIG.autoCommit,
    };
    this.currentBranch = this.config.defaultBranch;
  }

  // ===========================================================================
  // Initialization
  // ===========================================================================

  /**
   * Initialize the branch manager
   */
  async initialize(): Promise<void> {
    if (this.initialized) return;

    // Load current branch
    const head = await this.storage.get<{ branch: BranchId; commit?: CommitId }>(HEAD_KEY);
    if (head) {
      this.currentBranch = head.branch;
    }

    // Create default branch if it doesn't exist
    const mainBranch = await this.getBranch(this.config.defaultBranch);
    if (!mainBranch) {
      await this.createBranchInternal(this.config.defaultBranch, null, null);
    }

    // Load merge state if exists
    const mergeHead = await this.storage.get<MergeState>(MERGE_STATE_KEY);
    if (mergeHead) {
      this.mergeState = mergeHead;
    }

    this.initialized = true;
  }

  // ===========================================================================
  // Branch Operations
  // ===========================================================================

  /**
   * Create a new branch
   */
  async createBranch(options: CreateBranchOptions): Promise<BranchMetadata> {
    await this.ensureInitialized();

    // Convert strings to branded types
    const branchName = createBranchId(options.name as string);
    const fromBranch = options.from
      ? createBranchId(options.from as string)
      : this.currentBranch;
    const commitId = options.commit
      ? createCommitId(options.commit as string)
      : undefined;

    // Validate branch name
    if (!isValidBranchName(branchName)) {
      throw new BranchError(
        BranchErrorCode.INVALID_BRANCH_NAME,
        `Invalid branch name: ${branchName}`,
        branchName
      );
    }

    // Check if branch already exists
    const existing = await this.getBranch(branchName);
    if (existing) {
      throw new BranchError(
        BranchErrorCode.BRANCH_EXISTS,
        `Branch already exists: ${branchName}`,
        branchName
      );
    }

    // Get source branch
    const sourceBranch = await this.getBranch(fromBranch);
    if (!sourceBranch) {
      throw new BranchError(
        BranchErrorCode.BRANCH_NOT_FOUND,
        `Source branch not found: ${fromBranch}`,
        fromBranch
      );
    }

    // Determine base commit
    const baseCommit = commitId || sourceBranch.head;

    // If commit specified, verify it exists
    if (commitId) {
      const commitData = await this.getCommit(commitId);
      if (!commitData) {
        throw new BranchError(
          BranchErrorCode.COMMIT_NOT_FOUND,
          `Commit not found: ${commitId}`,
          branchName,
          { commit: commitId }
        );
      }
    }

    return this.createBranchInternal(branchName, fromBranch, baseCommit, options.description);
  }

  /**
   * Internal branch creation
   */
  private async createBranchInternal(
    name: BranchId,
    parent: BranchId | null,
    baseCommit: CommitId | null,
    description?: string
  ): Promise<BranchMetadata> {
    const now = Date.now();
    const branch: BranchMetadata = {
      name,
      parent,
      baseCommit,
      createdAt: now,
      updatedAt: now,
      protected: PROTECTED_BRANCHES.includes(name),
      archived: false,
      description,
      head: baseCommit,
    };

    await this.storage.put(BRANCH_PREFIX + name, branch);
    this.branchCache.set(name, branch);

    return branch;
  }

  /**
   * Delete a branch
   */
  async deleteBranch(name: BranchId | string, options: DeleteBranchOptions = {}): Promise<void> {
    await this.ensureInitialized();

    const branchName = createBranchId(name as string);
    const branch = await this.getBranch(branchName);
    if (!branch) {
      throw new BranchError(
        BranchErrorCode.BRANCH_NOT_FOUND,
        `Branch not found: ${branchName}`,
        branchName
      );
    }

    // Cannot delete current branch
    if (branchName === this.currentBranch) {
      throw new BranchError(
        BranchErrorCode.CANNOT_DELETE_CURRENT,
        `Cannot delete current branch: ${branchName}`,
        branchName
      );
    }

    // Cannot delete protected branch
    if (branch.protected && !options.force) {
      throw new BranchError(
        BranchErrorCode.BRANCH_PROTECTED,
        `Cannot delete protected branch: ${branchName}`,
        branchName
      );
    }

    // Check if branch is fully merged (unless force)
    if (!options.force) {
      const mainBranch = await this.getBranch(this.config.defaultBranch);
      if (mainBranch && mainBranch.head) {
        const isMerged = await this.isBranchMerged(branchName, this.config.defaultBranch);
        if (!isMerged) {
          throw new BranchError(
            BranchErrorCode.NOT_MERGED,
            `Branch ${branchName} is not fully merged. Use force to delete anyway.`,
            branchName
          );
        }
      }
    }

    // Delete branch metadata
    await this.storage.delete(BRANCH_PREFIX + branchName);
    this.branchCache.delete(branchName);
  }

  /**
   * Get branch metadata
   */
  async getBranch(name: BranchId | string): Promise<BranchMetadata | null> {
    const branchName = createBranchId(name as string);
    // Check cache
    const cached = this.branchCache.get(branchName);
    if (cached) return cached;

    const branch = await this.storage.get<BranchMetadata>(BRANCH_PREFIX + branchName);
    if (branch) {
      this.branchCache.set(branchName, branch);
    }
    return branch || null;
  }

  /**
   * List all branches
   */
  async listBranches(pattern?: string): Promise<BranchMetadata[]> {
    await this.ensureInitialized();

    const entries = await this.storage.list<BranchMetadata>({ prefix: BRANCH_PREFIX });
    const branches: BranchMetadata[] = [];

    for (const [key, value] of entries) {
      const nameStr = key.slice(BRANCH_PREFIX.length);

      // Apply pattern filter if provided
      if (pattern) {
        const regex = new RegExp(pattern.replace(/\*/g, '.*'));
        if (!regex.test(nameStr)) continue;
      }

      branches.push(value);
      // Cast to BranchId - data from storage is already properly formatted
      this.branchCache.set(nameStr as BranchId, value);
    }

    return branches.sort((a, b) => a.name.localeCompare(b.name));
  }

  /**
   * Rename a branch
   */
  async renameBranch(oldName: BranchId | string, newName: BranchId | string): Promise<void> {
    await this.ensureInitialized();

    const oldBranchName = createBranchId(oldName as string);
    const newBranchName = createBranchId(newName as string);

    // Validate new name
    if (!isValidBranchName(newBranchName)) {
      throw new BranchError(
        BranchErrorCode.INVALID_BRANCH_NAME,
        `Invalid branch name: ${newBranchName}`,
        newBranchName
      );
    }

    // Get old branch
    const oldBranch = await this.getBranch(oldBranchName);
    if (!oldBranch) {
      throw new BranchError(
        BranchErrorCode.BRANCH_NOT_FOUND,
        `Branch not found: ${oldBranchName}`,
        oldBranchName
      );
    }

    // Check new name doesn't exist
    const existing = await this.getBranch(newBranchName);
    if (existing) {
      throw new BranchError(
        BranchErrorCode.BRANCH_EXISTS,
        `Branch already exists: ${newBranchName}`,
        newBranchName
      );
    }

    // Create new branch with same data
    const newBranch: BranchMetadata = {
      ...oldBranch,
      name: newBranchName,
      updatedAt: Date.now(),
    };

    // Update storage atomically
    await this.storage.put(BRANCH_PREFIX + newBranchName, newBranch);
    await this.storage.delete(BRANCH_PREFIX + oldBranchName);

    // Update caches
    this.branchCache.delete(oldBranchName);
    this.branchCache.set(newBranchName, newBranch);

    // Update HEAD if current branch was renamed
    if (this.currentBranch === oldBranchName) {
      this.currentBranch = newBranchName;
      await this.updateHead();
    }

    // Update child branches' parent reference
    const allBranches = await this.listBranches();
    for (const branch of allBranches) {
      if (branch.parent === oldBranchName) {
        const updated = { ...branch, parent: newBranchName, updatedAt: Date.now() };
        await this.storage.put(BRANCH_PREFIX + branch.name, updated);
        this.branchCache.set(branch.name, updated);
      }
    }
  }

  // ===========================================================================
  // Checkout Operations
  // ===========================================================================

  /**
   * Get current branch name
   */
  getCurrentBranch(): BranchId {
    return this.currentBranch;
  }

  /**
   * Checkout a branch
   */
  async checkout(name: BranchId | string, options: CheckoutOptions = {}): Promise<CheckoutResult> {
    await this.ensureInitialized();

    const branchName = createBranchId(name as string);
    const targetCommitOpt = options.commit
      ? createCommitId(options.commit as string)
      : undefined;

    const previous = {
      branch: this.currentBranch,
      commit: (await this.getBranch(this.currentBranch))?.head || undefined,
    };

    // Create branch if requested
    if (options.create) {
      const existing = await this.getBranch(branchName);
      if (!existing) {
        await this.createBranch({ name: branchName, from: this.currentBranch });
      }
    }

    // Get target branch
    const branch = await this.getBranch(branchName);
    if (!branch) {
      throw new BranchError(
        BranchErrorCode.BRANCH_NOT_FOUND,
        `Branch not found: ${branchName}`,
        branchName
      );
    }

    // Check for archived branch
    if (branch.archived && !options.force) {
      throw new BranchError(
        BranchErrorCode.BRANCH_ARCHIVED,
        `Branch is archived: ${branchName}`,
        branchName
      );
    }

    // Check for uncommitted changes (unless force)
    if (!options.force) {
      const status = await this.status();
      const hasChanges = status.staged.length > 0 || status.unstaged.length > 0;
      if (hasChanges) {
        throw new BranchError(
          BranchErrorCode.UNCOMMITTED_CHANGES,
          'You have uncommitted changes. Commit or stash them, or use force.',
          branchName
        );
      }
    }

    // Determine target commit
    const targetCommit = targetCommitOpt || branch.head;

    // Calculate changed files
    const changed = await this.getChangedFiles(previous.commit || null, targetCommit);

    // Switch branch
    this.currentBranch = branchName;
    await this.updateHead(targetCommit || undefined);

    // Update working tree (copy files from target commit)
    if (targetCommit && previous.commit !== targetCommit) {
      await this.restoreWorkingTree(targetCommit);
    }

    // Clear index cache to force reload
    this.indexCache = null;

    return {
      previous,
      current: {
        branch: branchName,
        commit: targetCommit,
      },
      changed,
    };
  }

  // ===========================================================================
  // Merge Operations
  // ===========================================================================

  /**
   * Merge one branch into another
   */
  async merge(
    source: BranchId | string,
    target: BranchId | string,
    options: MergeOptions = {}
  ): Promise<MergeResult> {
    await this.ensureInitialized();

    const sourceName = createBranchId(source as string);
    const targetName = createBranchId(target as string);
    const { strategy = 'recursive', message, abortOnConflict = false, autoResolve } = options;

    // Get both branches
    const [sourceBranch, targetBranch] = await Promise.all([
      this.getBranch(sourceName),
      this.getBranch(targetName),
    ]);

    if (!sourceBranch) {
      throw new BranchError(
        BranchErrorCode.BRANCH_NOT_FOUND,
        `Source branch not found: ${sourceName}`,
        sourceName
      );
    }

    if (!targetBranch) {
      throw new BranchError(
        BranchErrorCode.BRANCH_NOT_FOUND,
        `Target branch not found: ${targetName}`,
        targetName
      );
    }

    if (targetBranch.archived) {
      throw new BranchError(
        BranchErrorCode.BRANCH_ARCHIVED,
        `Target branch is archived: ${targetName}`,
        targetName
      );
    }

    // Checkout target branch first
    if (this.currentBranch !== targetName) {
      await this.checkout(targetName, { force: true });
    }

    // Find merge base
    const mergeBase = await this.findMergeBase(sourceName, targetName);

    // Check if fast-forward is possible
    const canFastForward = mergeBase === targetBranch.head;

    // Handle fast-forward
    if (canFastForward && strategy !== 'no-ff') {
      return this.fastForwardMerge(sourceName, targetName, sourceBranch, targetBranch);
    }

    // Check if fast-forward is required but not possible
    if (strategy === 'fast-forward' && !canFastForward) {
      throw new BranchError(
        BranchErrorCode.NOT_FAST_FORWARD,
        `Cannot fast-forward ${targetName} to ${sourceName}`,
        targetName
      );
    }

    // Perform three-way merge
    const { conflicts, added, modified, deleted } = await this.performThreeWayMerge(
      sourceBranch.head,
      targetBranch.head,
      mergeBase
    );

    // Handle conflicts
    if (conflicts.length > 0) {
      if (abortOnConflict) {
        return {
          success: false,
          mergeType: 'aborted',
          source: sourceName,
          target: targetName,
          conflicts: conflicts.map(path => ({
            path,
            type: 'content',
            resolved: false,
          })),
          updated: [],
          added: [],
          deleted: [],
        };
      }

      // Auto-resolve if requested
      if (autoResolve) {
        for (const path of conflicts) {
          await this.resolveMerge(path, autoResolve);
        }
      } else {
        // Store merge state for manual resolution
        this.mergeState = {
          source: sourceName,
          target: targetName,
          baseCommit: mergeBase,
          conflicts,
          resolved: new Map(),
        };
        await this.storage.put(MERGE_STATE_KEY, this.mergeState);

        throw new BranchError(
          BranchErrorCode.MERGE_CONFLICT,
          `Merge conflicts in: ${conflicts.join(', ')}`,
          targetName,
          { conflicts }
        );
      }
    }

    // Create merge commit
    const mergeMessage = message || `Merge branch '${sourceName}' into ${targetName}`;
    const commitId = await this.createMergeCommit(
      mergeMessage,
      [targetBranch.head!, sourceBranch.head!],
      this.config.author
    );

    // Update target branch head
    await this.updateBranchHead(targetName, commitId);

    // Clear merge state
    this.mergeState = null;
    await this.storage.delete(MERGE_STATE_KEY);

    return {
      success: true,
      mergeType: 'merge-commit',
      commit: commitId,
      source: sourceName,
      target: targetName,
      conflicts: [],
      updated: modified,
      added,
      deleted,
    };
  }

  /**
   * Perform fast-forward merge
   */
  private async fastForwardMerge(
    source: BranchId,
    target: BranchId,
    sourceBranch: BranchMetadata,
    targetBranch: BranchMetadata
  ): Promise<MergeResult> {
    const changed = await this.getChangedFiles(
      targetBranch.head,
      sourceBranch.head
    );

    // Simply move target HEAD to source HEAD
    await this.updateBranchHead(target, sourceBranch.head!);

    // Update working tree
    if (sourceBranch.head) {
      await this.restoreWorkingTree(sourceBranch.head);
    }

    return {
      success: true,
      mergeType: 'fast-forward',
      commit: sourceBranch.head!,
      source,
      target,
      conflicts: [],
      updated: changed.filter(f => !changed.some(c => c === f)),
      added: [],
      deleted: [],
    };
  }

  /**
   * Abort ongoing merge
   */
  async abortMerge(): Promise<void> {
    if (!this.mergeState) {
      throw new BranchError(
        BranchErrorCode.INVALID_OPERATION,
        'No merge in progress',
      );
    }

    // Restore to target branch HEAD
    const targetBranch = await this.getBranch(this.mergeState.target);
    if (targetBranch?.head) {
      await this.restoreWorkingTree(targetBranch.head);
    }

    // Clear merge state
    this.mergeState = null;
    await this.storage.delete(MERGE_STATE_KEY);
    this.indexCache = null;
  }

  /**
   * Resolve a merge conflict
   */
  async resolveMerge(
    path: string,
    resolution: 'ours' | 'theirs' | Uint8Array
  ): Promise<void> {
    if (!this.mergeState) {
      throw new BranchError(
        BranchErrorCode.INVALID_OPERATION,
        'No merge in progress',
      );
    }

    if (!this.mergeState.conflicts.includes(path)) {
      throw new BranchError(
        BranchErrorCode.INVALID_OPERATION,
        `No conflict at path: ${path}`,
      );
    }

    // Store resolution
    if (resolution instanceof Uint8Array) {
      // Custom content
      const hash = await this.hashContent(resolution);
      await this.storeBlob(hash, resolution);
      this.mergeState.resolved.set(path, hash);
    } else {
      this.mergeState.resolved.set(path, resolution);
    }

    // Update merge state
    await this.storage.put(MERGE_STATE_KEY, this.mergeState);

    // Check if all conflicts are resolved
    const allResolved = this.mergeState.conflicts.every(
      p => this.mergeState!.resolved.has(p)
    );

    if (allResolved) {
      // Complete the merge
      await this.completeMerge();
    }
  }

  /**
   * Complete merge after all conflicts resolved
   */
  private async completeMerge(): Promise<void> {
    if (!this.mergeState) return;

    const sourceBranch = await this.getBranch(this.mergeState.source);
    const targetBranch = await this.getBranch(this.mergeState.target);

    if (!sourceBranch || !targetBranch) return;

    // Apply resolutions to working tree
    for (const [path, resolution] of this.mergeState.resolved) {
      if (resolution === 'ours') {
        // Keep target version (already in working tree)
        continue;
      } else if (resolution === 'theirs') {
        // Copy from source
        if (sourceBranch.head) {
          const data = await this.readBlobAtCommit(path, sourceBranch.head);
          if (data) {
            await this.writeToWorkingTree(path, data);
          }
        }
      } else {
        // Custom resolution (hash)
        const data = await this.readBlob(resolution);
        if (data) {
          await this.writeToWorkingTree(path, data);
        }
      }
    }

    // Create merge commit
    const message = `Merge branch '${this.mergeState.source}' into ${this.mergeState.target}`;
    const commitId = await this.createMergeCommit(
      message,
      [targetBranch.head!, sourceBranch.head!],
      this.config.author
    );

    // Update branch head
    await this.updateBranchHead(this.mergeState.target, commitId);

    // Clear merge state
    this.mergeState = null;
    await this.storage.delete(MERGE_STATE_KEY);
  }

  // ===========================================================================
  // History Operations
  // ===========================================================================

  /**
   * Get branch history/log
   */
  async log(branch?: BranchId | string, options: BranchLogOptions = {}): Promise<BranchLogEntry[]> {
    await this.ensureInitialized();

    const branchName = branch
      ? createBranchId(branch as string)
      : this.currentBranch;
    const branchMeta = await this.getBranch(branchName);
    if (!branchMeta) {
      throw new BranchError(
        BranchErrorCode.BRANCH_NOT_FOUND,
        `Branch not found: ${branchName}`,
        branchName
      );
    }

    const { limit = 100, skip = 0, since, until, paths } = options;
    const entries: BranchLogEntry[] = [];
    let position = 0;

    // Walk commit history
    let currentCommit = branchMeta.head;
    while (currentCommit && entries.length < limit + skip) {
      const commit = await this.getCommit(currentCommit);
      if (!commit) break;

      // Apply filters
      const commitDate = new Date(commit.timestamp);
      if (since && commitDate < since) break;
      if (until && commitDate > until) {
        currentCommit = commit.parents[0] || null;
        continue;
      }

      // Path filter
      if (paths && paths.length > 0) {
        const changedPaths = await this.getCommitPaths(commit);
        if (!paths.some(p => changedPaths.includes(p))) {
          currentCommit = commit.parents[0] || null;
          continue;
        }
      }

      if (position >= skip) {
        entries.push({
          commit,
          branch: branchName,
          position,
        });
      }

      position++;
      currentCommit = commit.parents[0] || null;
    }

    return entries;
  }

  /**
   * Compare two branches
   */
  async compare(source: BranchId | string, target: BranchId | string): Promise<BranchComparison> {
    await this.ensureInitialized();

    const sourceName = createBranchId(source as string);
    const targetName = createBranchId(target as string);

    const [sourceBranch, targetBranch] = await Promise.all([
      this.getBranch(sourceName),
      this.getBranch(targetName),
    ]);

    if (!sourceBranch) {
      throw new BranchError(
        BranchErrorCode.BRANCH_NOT_FOUND,
        `Branch not found: ${sourceName}`,
        sourceName
      );
    }

    if (!targetBranch) {
      throw new BranchError(
        BranchErrorCode.BRANCH_NOT_FOUND,
        `Branch not found: ${targetName}`,
        targetName
      );
    }

    const mergeBase = await this.findMergeBase(sourceName, targetName);

    // Count commits ahead and behind
    const ahead = await this.countCommits(sourceBranch.head, mergeBase);
    const behind = await this.countCommits(targetBranch.head, mergeBase);

    // Get file differences
    const diff = await this.getFileDiff(sourceBranch.head, targetBranch.head);

    return {
      source: sourceName,
      target: targetName,
      mergeBase,
      ahead,
      behind,
      diverged: ahead > 0 && behind > 0,
      diff,
    };
  }

  /**
   * Find common ancestor of two branches
   */
  async findMergeBase(branch1: BranchId | string, branch2: BranchId | string): Promise<CommitId | null> {
    await this.ensureInitialized();

    const branch1Name = createBranchId(branch1 as string);
    const branch2Name = createBranchId(branch2 as string);

    const [b1, b2] = await Promise.all([
      this.getBranch(branch1Name),
      this.getBranch(branch2Name),
    ]);

    if (!b1 || !b2) return null;
    if (!b1.head || !b2.head) return null;

    // Build ancestor set for branch1
    const ancestors1 = new Set<CommitId>();
    let current: CommitId | null = b1.head;

    while (current) {
      ancestors1.add(current);
      const commit = await this.getCommit(current);
      current = commit?.parents[0] || null;
    }

    // Find first common ancestor in branch2
    current = b2.head;
    while (current) {
      if (ancestors1.has(current)) {
        return current;
      }
      const commit = await this.getCommit(current);
      current = commit?.parents[0] || null;
    }

    return null;
  }

  // ===========================================================================
  // Commit Operations
  // ===========================================================================

  /**
   * Create a commit
   */
  async commit(message: string, author?: AuthorInfo): Promise<CommitId> {
    await this.ensureInitialized();

    const branch = await this.getBranch(this.currentBranch);
    if (!branch) {
      throw new BranchError(
        BranchErrorCode.BRANCH_NOT_FOUND,
        `Current branch not found: ${this.currentBranch}`,
        this.currentBranch
      );
    }

    if (branch.archived) {
      throw new BranchError(
        BranchErrorCode.BRANCH_ARCHIVED,
        `Branch is archived: ${this.currentBranch}`,
        this.currentBranch
      );
    }

    // Build tree from index
    const index = await this.loadIndex();
    const tree = await this.buildTree(index);

    // Create commit
    const commitId = await this.createCommitInternal(
      message,
      tree,
      branch.head ? [branch.head] : [],
      author || this.config.author
    );

    // Update branch head
    await this.updateBranchHead(this.currentBranch, commitId);
    await this.updateHead(commitId);

    // Clear staged entries from index
    await this.clearStagedFromIndex();

    return commitId;
  }

  /**
   * Get commit metadata
   */
  async getCommit(id: CommitId | string): Promise<CommitMetadata | null> {
    const commitId = createCommitId(id as string);
    // Check cache
    const cached = this.commitCache.get(commitId);
    if (cached) return cached;

    const commit = await this.storage.get<CommitMetadata>(COMMIT_PREFIX + commitId);
    if (commit) {
      this.commitCache.set(commitId, commit);
    }
    return commit || null;
  }

  // ===========================================================================
  // Working Tree Operations
  // ===========================================================================

  /**
   * Get working tree status
   */
  async status(): Promise<WorkingTreeStatus> {
    await this.ensureInitialized();

    const index = await this.loadIndex();
    const branch = await this.getBranch(this.currentBranch);

    const staged: FileChange[] = [];
    const unstaged: FileChange[] = [];
    const untracked: string[] = [];
    const conflicts: string[] = [];

    // Get files in HEAD commit
    const headFiles = new Set<string>();
    if (branch?.head) {
      const commit = await this.getCommit(branch.head);
      if (commit) {
        const tree = await this.readTree(commit.tree);
        for (const entry of tree) {
          headFiles.add(entry.name);
        }
      }
    }

    // Compare index to HEAD and working tree
    for (const [path, entry] of index) {
      if (entry.staged) {
        if (!headFiles.has(path)) {
          staged.push({ path, status: 'added' });
        } else {
          staged.push({ path, status: 'modified' });
        }
      }
    }

    // Get conflict files from merge state
    if (this.mergeState) {
      conflicts.push(...this.mergeState.conflicts.filter(
        c => !this.mergeState!.resolved.has(c)
      ));
    }

    return {
      branch: this.currentBranch,
      head: branch?.head || null,
      staged,
      unstaged,
      untracked,
      merging: this.mergeState !== null,
      conflicts,
    };
  }

  /**
   * Get diff for branch
   */
  async diff(branch?: BranchId | string): Promise<FileDiff[]> {
    await this.ensureInitialized();

    const branchName = branch
      ? createBranchId(branch as string)
      : this.currentBranch;
    const branchMeta = await this.getBranch(branchName);

    if (!branchMeta?.head) {
      return [];
    }

    const parentBranch = branchMeta.parent
      ? await this.getBranch(branchMeta.parent)
      : null;

    if (!parentBranch?.head) {
      return [];
    }

    return this.getFileDiff(branchMeta.head, parentBranch.head);
  }

  // ===========================================================================
  // Internal Helper Methods
  // ===========================================================================

  private async ensureInitialized(): Promise<void> {
    if (!this.initialized) {
      await this.initialize();
    }
  }

  private async updateHead(commit?: CommitId): Promise<void> {
    await this.storage.put(HEAD_KEY, { branch: this.currentBranch, commit });
  }

  private async updateBranchHead(branch: BranchId, commit: CommitId): Promise<void> {
    const branchMeta = await this.getBranch(branch);
    if (!branchMeta) return;

    const updated = {
      ...branchMeta,
      head: commit,
      updatedAt: Date.now(),
    };

    await this.storage.put(BRANCH_PREFIX + branch, updated);
    this.branchCache.set(branch, updated);
  }

  private async loadIndex(): Promise<Map<string, IndexEntry>> {
    if (this.indexCache) return this.indexCache;

    const index = await this.storage.get<Record<string, IndexEntry>>(INDEX_KEY);
    this.indexCache = new Map(Object.entries(index || {}));
    return this.indexCache;
  }

  private async saveIndex(): Promise<void> {
    if (!this.indexCache) return;
    const obj = Object.fromEntries(this.indexCache);
    await this.storage.put(INDEX_KEY, obj);
  }

  private async clearStagedFromIndex(): Promise<void> {
    const index = await this.loadIndex();
    for (const [path, entry] of index) {
      if (entry.staged) {
        entry.staged = false;
      }
    }
    await this.saveIndex();
  }

  private async buildTree(index: Map<string, IndexEntry>): Promise<TreeId> {
    const entries: TreeEntry[] = [];

    for (const [path, entry] of index) {
      entries.push({
        name: path,
        type: 'blob',
        hash: entry.hash,
        size: entry.size,
        mode: entry.mode,
      });
    }

    const content = JSON.stringify(entries);
    const hash = await this.hashContent(new TextEncoder().encode(content));
    const treeId = createTreeId(hash);

    await this.storage.put(TREE_PREFIX + treeId, entries);
    return treeId;
  }

  private async readTree(id: TreeId): Promise<TreeEntry[]> {
    const tree = await this.storage.get<TreeEntry[]>(TREE_PREFIX + id);
    return tree || [];
  }

  private async createCommitInternal(
    message: string,
    tree: TreeId,
    parents: CommitId[],
    author: AuthorInfo
  ): Promise<CommitId> {
    const timestamp = Date.now();
    const commitData = JSON.stringify({
      tree,
      parents,
      message,
      author,
      timestamp,
    });

    const id = generateCommitId(commitData);

    const commit: CommitMetadata = {
      id,
      branch: this.currentBranch,
      parents,
      tree,
      message,
      author,
      timestamp,
    };

    await this.storage.put(COMMIT_PREFIX + id, commit);
    this.commitCache.set(id, commit);

    return id;
  }

  private async createMergeCommit(
    message: string,
    parents: CommitId[],
    author: AuthorInfo
  ): Promise<CommitId> {
    const index = await this.loadIndex();
    const tree = await this.buildTree(index);
    return this.createCommitInternal(message, tree, parents, author);
  }

  private async hashContent(data: Uint8Array): Promise<string> {
    if (typeof crypto !== 'undefined' && crypto.subtle) {
      const buffer = new ArrayBuffer(data.length);
      new Uint8Array(buffer).set(data);
      const hashBuffer = await crypto.subtle.digest('SHA-256', buffer);
      const hashArray = new Uint8Array(hashBuffer);
      return Array.from(hashArray)
        .map(b => b.toString(16).padStart(2, '0'))
        .join('');
    }

    // Simple fallback hash
    let hash = 2166136261;
    for (let i = 0; i < data.length; i++) {
      // Non-null assertion: i is within bounds of data array
      hash ^= data[i]!;
      hash = (hash * 16777619) >>> 0;
    }
    return hash.toString(16).padStart(8, '0') + Date.now().toString(16);
  }

  private async storeBlob(hash: string, data: Uint8Array): Promise<void> {
    await this.storage.put(BLOB_PREFIX + hash, data);
  }

  private async readBlob(hash: string): Promise<Uint8Array | null> {
    const data = await this.storage.get<Uint8Array>(BLOB_PREFIX + hash);
    return data || null;
  }

  private async getChangedFiles(
    fromCommit: CommitId | null,
    toCommit: CommitId | null
  ): Promise<string[]> {
    const fromFiles = fromCommit
      ? await this.getCommitFiles(fromCommit)
      : new Map<string, string>();
    const toFiles = toCommit
      ? await this.getCommitFiles(toCommit)
      : new Map<string, string>();

    const changed: string[] = [];

    // Added or modified in 'to'
    for (const [path, hash] of toFiles) {
      if (!fromFiles.has(path) || fromFiles.get(path) !== hash) {
        changed.push(path);
      }
    }

    // Deleted (in 'from' but not 'to')
    for (const path of fromFiles.keys()) {
      if (!toFiles.has(path)) {
        changed.push(path);
      }
    }

    return changed;
  }

  private async getCommitFiles(commitId: CommitId): Promise<Map<string, string>> {
    const commit = await this.getCommit(commitId);
    if (!commit) return new Map();

    const tree = await this.readTree(commit.tree);
    const files = new Map<string, string>();

    for (const entry of tree) {
      files.set(entry.name, entry.hash);
    }

    return files;
  }

  private async getCommitPaths(commit: CommitMetadata): Promise<string[]> {
    const tree = await this.readTree(commit.tree);
    return tree.map(e => e.name);
  }

  private async restoreWorkingTree(commitId: CommitId): Promise<void> {
    const commit = await this.getCommit(commitId);
    if (!commit) return;

    const tree = await this.readTree(commit.tree);
    const index = new Map<string, IndexEntry>();

    for (const entry of tree) {
      index.set(entry.name, {
        path: entry.name,
        hash: entry.hash,
        mode: entry.mode || 0o644,
        size: entry.size || 0,
        mtime: Date.now(),
        staged: false,
      });
    }

    this.indexCache = index;
    await this.saveIndex();
  }

  private async writeToWorkingTree(path: string, data: Uint8Array): Promise<void> {
    const hash = await this.hashContent(data);
    await this.storeBlob(hash, data);

    const index = await this.loadIndex();
    index.set(path, {
      path,
      hash,
      mode: 0o644,
      size: data.length,
      mtime: Date.now(),
      staged: true,
    });

    await this.saveIndex();
  }

  private async readBlobAtCommit(path: string, commitId: CommitId): Promise<Uint8Array | null> {
    const commit = await this.getCommit(commitId);
    if (!commit) return null;

    const tree = await this.readTree(commit.tree);
    const entry = tree.find(e => e.name === path);
    if (!entry) return null;

    return this.readBlob(entry.hash);
  }

  private async isBranchMerged(branch: BranchId, into: BranchId): Promise<boolean> {
    const mergeBase = await this.findMergeBase(branch, into);
    const branchMeta = await this.getBranch(branch);

    return mergeBase === branchMeta?.head;
  }

  private async countCommits(from: CommitId | null, to: CommitId | null): Promise<number> {
    if (!from) return 0;

    let count = 0;
    let current: CommitId | null = from;

    while (current && current !== to) {
      count++;
      const commit = await this.getCommit(current);
      current = commit?.parents[0] || null;
    }

    return count;
  }

  private async getFileDiff(
    sourceCommit: CommitId | null,
    targetCommit: CommitId | null
  ): Promise<FileDiff[]> {
    const sourceFiles = sourceCommit
      ? await this.getCommitFiles(sourceCommit)
      : new Map<string, string>();
    const targetFiles = targetCommit
      ? await this.getCommitFiles(targetCommit)
      : new Map<string, string>();

    const diff: FileDiff[] = [];

    // Added in source
    for (const [path, hash] of sourceFiles) {
      if (!targetFiles.has(path)) {
        diff.push({ path, status: 'added' });
      } else if (targetFiles.get(path) !== hash) {
        diff.push({ path, status: 'modified' });
      }
    }

    // Deleted (in target but not source)
    for (const path of targetFiles.keys()) {
      if (!sourceFiles.has(path)) {
        diff.push({ path, status: 'deleted' });
      }
    }

    return diff;
  }

  private async performThreeWayMerge(
    sourceCommit: CommitId | null,
    targetCommit: CommitId | null,
    baseCommit: CommitId | null
  ): Promise<{
    conflicts: string[];
    added: string[];
    modified: string[];
    deleted: string[];
  }> {
    const [sourceFiles, targetFiles, baseFiles] = await Promise.all([
      sourceCommit ? this.getCommitFiles(sourceCommit) : Promise.resolve(new Map()),
      targetCommit ? this.getCommitFiles(targetCommit) : Promise.resolve(new Map()),
      baseCommit ? this.getCommitFiles(baseCommit) : Promise.resolve(new Map()),
    ]);

    const conflicts: string[] = [];
    const added: string[] = [];
    const modified: string[] = [];
    const deleted: string[] = [];

    // Get all paths
    const allPaths = new Set([
      ...sourceFiles.keys(),
      ...targetFiles.keys(),
      ...baseFiles.keys(),
    ]);

    const index = await this.loadIndex();

    for (const path of allPaths) {
      const sourceHash = sourceFiles.get(path);
      const targetHash = targetFiles.get(path);
      const baseHash = baseFiles.get(path);

      // Simple merge logic
      if (sourceHash === targetHash) {
        // No change or same change - no action needed
        continue;
      }

      if (!targetHash && sourceHash) {
        // Added in source
        const data = await this.readBlobAtCommit(path, sourceCommit!);
        if (data) {
          await this.writeToWorkingTree(path, data);
          added.push(path);
        }
      } else if (targetHash && !sourceHash) {
        // Deleted in source
        if (targetHash === baseHash) {
          // Target unchanged - accept deletion
          index.delete(path);
          deleted.push(path);
        } else {
          // Target modified - conflict
          conflicts.push(path);
        }
      } else if (sourceHash && targetHash) {
        // Both have the file
        if (sourceHash !== baseHash && targetHash === baseHash) {
          // Only source changed - take source
          const data = await this.readBlobAtCommit(path, sourceCommit!);
          if (data) {
            await this.writeToWorkingTree(path, data);
            modified.push(path);
          }
        } else if (sourceHash === baseHash && targetHash !== baseHash) {
          // Only target changed - keep target
          continue;
        } else if (sourceHash !== baseHash && targetHash !== baseHash) {
          // Both changed - conflict
          conflicts.push(path);
        }
      }
    }

    await this.saveIndex();

    return { conflicts, added, modified, deleted };
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a branch manager
 */
export function createBranchManager(
  storage: DurableObjectStorage,
  config?: BranchManagerConfig
): DOBranchManager {
  return new DOBranchManager(storage, config);
}
