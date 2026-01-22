/**
 * Copy-on-Write (COW) Backend Implementation
 *
 * Implements FSXWithCOW interface, providing git-like branching semantics
 * on top of any FSXBackend storage layer.
 *
 * Key implementation details:
 * - Branches share blobs via reference counting
 * - Writes create new versions (copy-on-write)
 * - References follow chain to find actual data
 * - Snapshots capture manifest at point in time
 */

import type { FSXBackend, ByteRange, FSXMetadata } from './types.js';
import {
  type FSXWithCOW,
  type BlobRef,
  type Branch,
  type BranchOptions,
  type Snapshot,
  type SnapshotId,
  type SnapshotManifest,
  type ManifestEntry,
  type MergeStrategy,
  type MergeResult,
  type MergeConflict,
  type BranchDiff,
  type GCResult,
  type GCOptions,
  type StoredBlob,
  COWError,
  COWErrorCode,
  DEFAULT_BRANCH,
  MAX_REF_DEPTH,
  BRANCH_PREFIX,
  SNAPSHOT_PREFIX,
  REF_PREFIX,
  BLOB_PREFIX,
  makeSnapshotId,
  parseSnapshotId,
} from './cow-types.js';
import { SnapshotManager } from './snapshot.js';
import { MergeEngine } from './merge.js';
import { GarbageCollector } from './gc.js';

// =============================================================================
// COW Backend Implementation
// =============================================================================

/**
 * COW backend configuration
 */
export interface COWBackendConfig {
  /** Initial branch name (default: 'main') */
  defaultBranch?: string;
  /** Enable automatic snapshot on branch creation */
  autoSnapshot?: boolean;
  /** Maximum inline blob size (larger blobs use separate storage) */
  maxInlineSize?: number;
  /** Enable hash verification on reads */
  verifyHashes?: boolean;
}

const DEFAULT_CONFIG: Required<COWBackendConfig> = {
  defaultBranch: DEFAULT_BRANCH,
  autoSnapshot: true,
  maxInlineSize: 64 * 1024, // 64KB inline threshold
  verifyHashes: false,
};

/**
 * Copy-on-Write storage backend
 *
 * Storage layout:
 * - `_cow/branches/{name}` - Branch metadata
 * - `_cow/snapshots/{branch}/{version}` - Snapshot metadata
 * - `_cow/refs/{branch}/{path}` - Blob reference metadata
 * - `_cow/blobs/{hash}` - Actual blob data (content-addressed)
 */
export class COWBackend implements FSXWithCOW {
  private readonly storage: FSXBackend;
  private readonly config: Required<COWBackendConfig>;
  private currentBranch: string;

  // Sub-components
  private readonly snapshotManager: SnapshotManager;
  private readonly mergeEngine: MergeEngine;
  private readonly garbageCollector: GarbageCollector;

  // In-memory caches
  private branchCache = new Map<string, Branch>();
  private refCache = new Map<string, BlobRef>();

  constructor(storage: FSXBackend, config: COWBackendConfig = {}) {
    this.storage = storage;
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.currentBranch = this.config.defaultBranch;

    // Initialize sub-components
    this.snapshotManager = new SnapshotManager(storage);
    this.mergeEngine = new MergeEngine(storage, this);
    this.garbageCollector = new GarbageCollector(storage, this);
  }

  // ===========================================================================
  // Initialization
  // ===========================================================================

  /**
   * Initialize the COW backend, creating default branch if needed
   */
  async initialize(): Promise<void> {
    const mainBranch = await this.getBranch(this.config.defaultBranch);
    if (!mainBranch) {
      await this.createBranchInternal(this.config.defaultBranch, null, null);
    }
  }

  // ===========================================================================
  // FSXBackend Implementation (operates on current branch)
  // ===========================================================================

  async read(path: string, range?: ByteRange): Promise<Uint8Array | null> {
    return this.readFrom(path, this.currentBranch, range);
  }

  async write(path: string, data: Uint8Array): Promise<void> {
    return this.writeTo(path, data, this.currentBranch);
  }

  async delete(path: string): Promise<void> {
    return this.deleteFrom(path, this.currentBranch);
  }

  async list(prefix: string): Promise<string[]> {
    return this.listFrom(prefix, this.currentBranch);
  }

  async exists(path: string): Promise<boolean> {
    const ref = await this.getRefInternal(path, this.currentBranch);
    return ref !== null;
  }

  // ===========================================================================
  // Branch-Specific Read/Write
  // ===========================================================================

  async readFrom(path: string, branchName: string, range?: ByteRange): Promise<Uint8Array | null> {
    const ref = await this.getRefInternal(path, branchName);
    if (!ref) return null;

    // Resolve reference chain to find actual data
    const resolvedRef = await this.resolveRef(ref);
    if (!resolvedRef) return null;

    // Read the actual blob data
    return this.readBlobData(resolvedRef, range);
  }

  async writeTo(path: string, data: Uint8Array, branchName: string): Promise<void> {
    // Check branch exists and is writable
    const branch = await this.getBranch(branchName);
    if (!branch) {
      throw new COWError(COWErrorCode.BRANCH_NOT_FOUND, `Branch not found: ${branchName}`);
    }
    if (branch.readonly) {
      throw new COWError(COWErrorCode.BRANCH_READONLY, `Branch is read-only: ${branchName}`);
    }

    // Get existing ref to update refcount
    const existingRef = await this.getRefInternal(path, branchName);

    // Compute hash for content addressing
    const hash = await this.computeHash(data);

    // Check if blob with this hash already exists
    const existingBlobRef = await this.findBlobByHash(hash);

    const now = Date.now();
    let newRef: BlobRef;

    if (existingBlobRef) {
      // Blob exists - create reference to it
      newRef = {
        type: 'reference',
        target: this.getBlobPath(hash),
        version: existingRef ? existingRef.version + 1 : 1,
        refCount: 1,
        size: data.length,
        hash,
        createdAt: existingRef?.createdAt ?? now,
        modifiedAt: now,
        branch: branchName,
      };

      // Increment ref count on target blob
      await this.incrementRefCount(existingBlobRef);
    } else {
      // New blob - store data
      newRef = {
        type: 'direct',
        version: existingRef ? existingRef.version + 1 : 1,
        refCount: 1,
        size: data.length,
        hash,
        createdAt: existingRef?.createdAt ?? now,
        modifiedAt: now,
        branch: branchName,
      };

      // Store blob data
      await this.writeBlobData(hash, data, newRef);
    }

    // Decrement old ref count if updating
    if (existingRef) {
      await this.decrementRefCount(existingRef);
    }

    // Store the new reference
    await this.setRefInternal(path, branchName, newRef);

    // Update branch head version
    await this.updateBranchVersion(branchName);
  }

  async deleteFrom(path: string, branchName: string): Promise<void> {
    const branch = await this.getBranch(branchName);
    if (!branch) {
      throw new COWError(COWErrorCode.BRANCH_NOT_FOUND, `Branch not found: ${branchName}`);
    }
    if (branch.readonly) {
      throw new COWError(COWErrorCode.BRANCH_READONLY, `Branch is read-only: ${branchName}`);
    }

    const ref = await this.getRefInternal(path, branchName);
    if (!ref) return;

    // Decrement ref count
    await this.decrementRefCount(ref);

    // Delete the reference
    await this.deleteRefInternal(path, branchName);

    // Update branch version
    await this.updateBranchVersion(branchName);
  }

  async listFrom(prefix: string, branchName: string): Promise<string[]> {
    const refPrefix = this.getRefPath('', branchName);
    const searchPrefix = refPrefix + prefix;

    const keys = await this.storage.list(searchPrefix);

    // Extract paths from ref keys
    return keys.map((key) => key.slice(refPrefix.length)).sort();
  }

  // ===========================================================================
  // Branch Operations
  // ===========================================================================

  async branch(source: string, target: string, options: BranchOptions = {}): Promise<void> {
    // Check source exists
    const sourceBranch = await this.getBranch(source);
    if (!sourceBranch) {
      throw new COWError(COWErrorCode.BRANCH_NOT_FOUND, `Source branch not found: ${source}`);
    }

    // Check target doesn't exist
    const existingTarget = await this.getBranch(target);
    if (existingTarget) {
      throw new COWError(COWErrorCode.BRANCH_EXISTS, `Branch already exists: ${target}`);
    }

    // Create snapshot of source if configured
    let baseSnapshot: SnapshotId | null = options.snapshot ?? null;
    if (!baseSnapshot && this.config.autoSnapshot) {
      baseSnapshot = await this.snapshot(source, `Branch point for ${target}`);
    }

    // Create new branch
    await this.createBranchInternal(target, source, baseSnapshot, options.readonly);

    // Copy all refs from source to target (as references, not copies)
    await this.copyRefsForBranch(source, target);
  }

  async deleteBranch(name: string, force = false): Promise<void> {
    if (name === this.config.defaultBranch) {
      throw new COWError(COWErrorCode.BRANCH_READONLY, `Cannot delete default branch: ${name}`);
    }

    const branch = await this.getBranch(name);
    if (!branch) {
      throw new COWError(COWErrorCode.BRANCH_NOT_FOUND, `Branch not found: ${name}`);
    }

    if (!force) {
      // Check if branch has unmerged changes
      // For now, we'll skip this check and allow deletion
    }

    // Decrement ref counts for all blobs in branch
    const paths = await this.listFrom('', name);
    for (const path of paths) {
      const ref = await this.getRefInternal(path, name);
      if (ref) {
        await this.decrementRefCount(ref);
      }
    }

    // Delete all refs for this branch
    for (const path of paths) {
      await this.deleteRefInternal(path, name);
    }

    // Delete branch metadata
    await this.storage.delete(BRANCH_PREFIX + name);
    this.branchCache.delete(name);

    // Switch to default if deleting current
    if (this.currentBranch === name) {
      this.currentBranch = this.config.defaultBranch;
    }
  }

  async listBranches(): Promise<Branch[]> {
    const keys = await this.storage.list(BRANCH_PREFIX);
    const branches: Branch[] = [];

    for (const key of keys) {
      const name = key.slice(BRANCH_PREFIX.length);
      const branch = await this.getBranch(name);
      if (branch) {
        branches.push(branch);
      }
    }

    return branches.sort((a, b) => a.name.localeCompare(b.name));
  }

  async getBranch(name: string): Promise<Branch | null> {
    // Check cache
    const cached = this.branchCache.get(name);
    if (cached) return cached;

    const data = await this.storage.read(BRANCH_PREFIX + name);
    if (!data) return null;

    const branch = JSON.parse(new TextDecoder().decode(data)) as Branch;
    this.branchCache.set(name, branch);
    return branch;
  }

  getCurrentBranch(): string {
    return this.currentBranch;
  }

  async checkout(name: string): Promise<void> {
    const branch = await this.getBranch(name);
    if (!branch) {
      throw new COWError(COWErrorCode.BRANCH_NOT_FOUND, `Branch not found: ${name}`);
    }
    this.currentBranch = name;
  }

  // ===========================================================================
  // Merge Operations
  // ===========================================================================

  async merge(source: string, target: string, strategy: MergeStrategy): Promise<MergeResult> {
    return this.mergeEngine.merge(source, target, strategy);
  }

  async diff(source: string, target: string): Promise<BranchDiff> {
    return this.mergeEngine.diff(source, target);
  }

  // ===========================================================================
  // Snapshot Operations
  // ===========================================================================

  async snapshot(branchName: string, message?: string): Promise<SnapshotId> {
    const branch = await this.getBranch(branchName);
    if (!branch) {
      throw new COWError(COWErrorCode.BRANCH_NOT_FOUND, `Branch not found: ${branchName}`);
    }

    // Build manifest from current refs
    const paths = await this.listFrom('', branchName);
    const entries: ManifestEntry[] = [];
    let totalSize = 0;

    for (const path of paths) {
      const ref = await this.getRefInternal(path, branchName);
      if (ref) {
        entries.push({
          path,
          version: ref.version,
          size: ref.size,
          hash: ref.hash,
        });
        totalSize += ref.size;
      }
    }

    const manifest: SnapshotManifest = {
      entries,
      totalSize,
      count: entries.length,
    };

    return this.snapshotManager.createSnapshot(branchName, branch.headVersion, manifest, message);
  }

  async readAt(path: string, snapshotId: SnapshotId, range?: ByteRange): Promise<Uint8Array | null> {
    const snapshot = await this.getSnapshot(snapshotId);
    if (!snapshot) {
      throw new COWError(COWErrorCode.SNAPSHOT_NOT_FOUND, `Snapshot not found: ${snapshotId}`);
    }

    // Find entry in manifest
    const entry = snapshot.manifest.entries.find((e) => e.path === path);
    if (!entry) return null;

    // Read the blob by hash
    if (entry.hash) {
      const blobPath = this.getBlobPath(entry.hash);
      return this.storage.read(blobPath, range);
    }

    // Fallback: read from branch ref (may have changed)
    return this.readFrom(path, snapshot.branch, range);
  }

  async listSnapshots(branchName: string): Promise<Snapshot[]> {
    return this.snapshotManager.listSnapshots(branchName);
  }

  async getSnapshot(snapshotId: SnapshotId): Promise<Snapshot | null> {
    return this.snapshotManager.getSnapshot(snapshotId);
  }

  async deleteSnapshot(snapshotId: SnapshotId): Promise<void> {
    return this.snapshotManager.deleteSnapshot(snapshotId);
  }

  // ===========================================================================
  // Reference Management
  // ===========================================================================

  async getRef(path: string): Promise<BlobRef | null> {
    return this.getRefInternal(path, this.currentBranch);
  }

  async getRefs(paths: string[]): Promise<Map<string, BlobRef>> {
    const result = new Map<string, BlobRef>();

    for (const path of paths) {
      const ref = await this.getRefInternal(path, this.currentBranch);
      if (ref) {
        result.set(path, ref);
      }
    }

    return result;
  }

  async gc(options?: GCOptions): Promise<GCResult> {
    return this.garbageCollector.collect(options);
  }

  // ===========================================================================
  // Internal Reference Operations
  // ===========================================================================

  async getRefInternal(path: string, branchName: string): Promise<BlobRef | null> {
    const cacheKey = `${branchName}:${path}`;
    const cached = this.refCache.get(cacheKey);
    if (cached) return cached;

    const refPath = this.getRefPath(path, branchName);
    const data = await this.storage.read(refPath);
    if (!data) return null;

    const ref = JSON.parse(new TextDecoder().decode(data)) as BlobRef;
    this.refCache.set(cacheKey, ref);
    return ref;
  }

  async setRefInternal(path: string, branchName: string, ref: BlobRef): Promise<void> {
    const refPath = this.getRefPath(path, branchName);
    const data = new TextEncoder().encode(JSON.stringify(ref));
    await this.storage.write(refPath, data);

    const cacheKey = `${branchName}:${path}`;
    this.refCache.set(cacheKey, ref);
  }

  async deleteRefInternal(path: string, branchName: string): Promise<void> {
    const refPath = this.getRefPath(path, branchName);
    await this.storage.delete(refPath);

    const cacheKey = `${branchName}:${path}`;
    this.refCache.delete(cacheKey);
  }

  /**
   * Resolve a reference chain to find the actual blob ref with data
   */
  async resolveRef(ref: BlobRef, depth = 0): Promise<BlobRef | null> {
    if (depth > MAX_REF_DEPTH) {
      throw new COWError(COWErrorCode.CIRCULAR_REF, 'Reference chain too deep - possible circular reference');
    }

    if (ref.type === 'direct') {
      return ref;
    }

    // Follow reference to target
    if (!ref.target) {
      throw new COWError(COWErrorCode.INVALID_REF, 'Reference has no target');
    }

    // Target is a blob path
    const targetData = await this.storage.read(ref.target);
    if (!targetData) return null;

    // Check if target is another ref or actual data
    try {
      const targetRef = JSON.parse(new TextDecoder().decode(targetData)) as BlobRef;
      if (targetRef.type) {
        return this.resolveRef(targetRef, depth + 1);
      }
    } catch {
      // Not JSON - this is actual blob data, return original ref
      return ref;
    }

    return ref;
  }

  // ===========================================================================
  // Internal Blob Operations
  // ===========================================================================

  private async readBlobData(ref: BlobRef, range?: ByteRange): Promise<Uint8Array | null> {
    if (ref.type === 'direct' && ref.hash) {
      const blobPath = this.getBlobPath(ref.hash);
      return this.storage.read(blobPath, range);
    }

    if (ref.type === 'reference' && ref.target) {
      return this.storage.read(ref.target, range);
    }

    return null;
  }

  private async writeBlobData(hash: string, data: Uint8Array, ref: BlobRef): Promise<void> {
    const blobPath = this.getBlobPath(hash);

    // Store blob data
    await this.storage.write(blobPath, data);

    // Store blob metadata
    const metaPath = blobPath + '.meta';
    const metaData = new TextEncoder().encode(JSON.stringify(ref));
    await this.storage.write(metaPath, metaData);
  }

  private async findBlobByHash(hash: string): Promise<BlobRef | null> {
    const blobPath = this.getBlobPath(hash);
    const exists = await this.storage.exists(blobPath);
    if (!exists) return null;

    // Read metadata
    const metaPath = blobPath + '.meta';
    const metaData = await this.storage.read(metaPath);
    if (!metaData) return null;

    return JSON.parse(new TextDecoder().decode(metaData)) as BlobRef;
  }

  private async incrementRefCount(ref: BlobRef): Promise<void> {
    if (!ref.hash) return;

    const metaPath = this.getBlobPath(ref.hash) + '.meta';
    const metaData = await this.storage.read(metaPath);
    if (!metaData) return;

    const blobRef = JSON.parse(new TextDecoder().decode(metaData)) as BlobRef;
    blobRef.refCount++;

    await this.storage.write(metaPath, new TextEncoder().encode(JSON.stringify(blobRef)));
  }

  async decrementRefCount(ref: BlobRef): Promise<void> {
    if (!ref.hash) return;

    const metaPath = this.getBlobPath(ref.hash) + '.meta';
    const metaData = await this.storage.read(metaPath);
    if (!metaData) return;

    const blobRef = JSON.parse(new TextDecoder().decode(metaData)) as BlobRef;
    blobRef.refCount = Math.max(0, blobRef.refCount - 1);

    await this.storage.write(metaPath, new TextEncoder().encode(JSON.stringify(blobRef)));
  }

  /**
   * Compute content hash for deduplication
   */
  private async computeHash(data: Uint8Array): Promise<string> {
    // Use SubtleCrypto if available (browser/Workers), fallback to simple hash
    if (typeof crypto !== 'undefined' && crypto.subtle) {
      // Create a new ArrayBuffer copy to avoid SharedArrayBuffer issues
      const buffer = new ArrayBuffer(data.length);
      new Uint8Array(buffer).set(data);
      const hashBuffer = await crypto.subtle.digest('SHA-256', buffer);
      const hashArray = new Uint8Array(hashBuffer);
      return Array.from(hashArray)
        .map((b) => b.toString(16).padStart(2, '0'))
        .join('');
    }

    // Simple fallback hash (FNV-1a)
    let hash = 2166136261;
    for (let i = 0; i < data.length; i++) {
      hash ^= data[i];
      hash = (hash * 16777619) >>> 0;
    }
    return hash.toString(16).padStart(8, '0');
  }

  // ===========================================================================
  // Internal Branch Operations
  // ===========================================================================

  private async createBranchInternal(
    name: string,
    parent: string | null,
    baseSnapshot: SnapshotId | null,
    readonly = false
  ): Promise<void> {
    const now = Date.now();
    const branch: Branch = {
      name,
      parent,
      baseSnapshot,
      createdAt: now,
      modifiedAt: now,
      headVersion: 1,
      readonly,
    };

    const data = new TextEncoder().encode(JSON.stringify(branch));
    await this.storage.write(BRANCH_PREFIX + name, data);
    this.branchCache.set(name, branch);
  }

  private async updateBranchVersion(name: string): Promise<void> {
    const branch = await this.getBranch(name);
    if (!branch) return;

    branch.headVersion++;
    branch.modifiedAt = Date.now();

    const data = new TextEncoder().encode(JSON.stringify(branch));
    await this.storage.write(BRANCH_PREFIX + name, data);
    this.branchCache.set(name, branch);
  }

  private async copyRefsForBranch(source: string, target: string): Promise<void> {
    const paths = await this.listFrom('', source);

    for (const path of paths) {
      const sourceRef = await this.getRefInternal(path, source);
      if (!sourceRef) continue;

      // Create reference pointing to source's blob
      const targetRef: BlobRef = {
        type: 'reference',
        target: sourceRef.hash ? this.getBlobPath(sourceRef.hash) : sourceRef.target,
        version: 1,
        refCount: 1,
        size: sourceRef.size,
        hash: sourceRef.hash,
        createdAt: Date.now(),
        modifiedAt: Date.now(),
        branch: target,
      };

      // Increment ref count on source blob
      await this.incrementRefCount(sourceRef);

      // Store ref in target branch
      await this.setRefInternal(path, target, targetRef);
    }
  }

  // ===========================================================================
  // Path Helpers
  // ===========================================================================

  private getRefPath(path: string, branchName: string): string {
    return `${REF_PREFIX}${branchName}/${path}`;
  }

  private getBlobPath(hash: string): string {
    // Use first 2 chars as directory for distribution
    return `${BLOB_PREFIX}${hash.slice(0, 2)}/${hash}`;
  }

  // ===========================================================================
  // Accessors for Sub-components
  // ===========================================================================

  getStorage(): FSXBackend {
    return this.storage;
  }

  getConfig(): Required<COWBackendConfig> {
    return this.config;
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a COW backend wrapping an existing FSX backend
 * @param storage - Underlying storage backend
 * @param config - COW configuration options
 */
export async function createCOWBackend(
  storage: FSXBackend,
  config?: COWBackendConfig
): Promise<COWBackend> {
  const backend = new COWBackend(storage, config);
  await backend.initialize();
  return backend;
}
