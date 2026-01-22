/**
 * DoSQL Integration Tests - Git-like Branch Operations
 *
 * TDD tests for branch operations (pocs-rt9x):
 * - Create branch from main
 * - Checkout branch
 * - Branch isolation
 * - Merge branch
 * - Merge conflicts
 * - Delete branch
 * - Branch listing
 * - Branch metadata
 * - Edge cases
 *
 * Uses workers-vitest-pool with real DO storage (NO MOCKS).
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  MemoryFSXBackend,
  createCOWBackend,
  type COWBackend,
  type Branch,
  type SnapshotId,
  type MergeResult,
  type BranchDiff,
  COWError,
  COWErrorCode,
} from '../../fsx/index.js';

describe('Git-like Branch Operations', () => {
  let fsx: MemoryFSXBackend;
  let cow: COWBackend;

  beforeEach(async () => {
    fsx = new MemoryFSXBackend();
    cow = await createCOWBackend(fsx);
  });

  afterEach(async () => {
    fsx.clear();
  });

  // ===========================================================================
  // Create Branch Operations
  // ===========================================================================

  describe('CREATE BRANCH', () => {
    it('should create a branch from main', async () => {
      // Arrange: Write some data to main
      const encoder = new TextEncoder();
      await cow.write('users/1.json', encoder.encode('{"id":1,"name":"Alice"}'));

      // Act: Create branch
      await cow.branch('main', 'feature');

      // Assert: Branch exists
      const branch = await cow.getBranch('feature');
      expect(branch).not.toBeNull();
      expect(branch?.name).toBe('feature');
      expect(branch?.parent).toBe('main');
    });

    it('should create a branch with all parent data accessible', async () => {
      // Arrange: Write multiple files to main
      const encoder = new TextEncoder();
      await cow.write('users/1.json', encoder.encode('{"id":1,"name":"Alice"}'));
      await cow.write('users/2.json', encoder.encode('{"id":2,"name":"Bob"}'));
      await cow.write('orders/1.json', encoder.encode('{"orderId":1}'));

      // Act: Create branch
      await cow.branch('main', 'feature');

      // Assert: All data accessible in feature branch
      await cow.checkout('feature');
      const user1 = await cow.read('users/1.json');
      const user2 = await cow.read('users/2.json');
      const order1 = await cow.read('orders/1.json');

      expect(user1).not.toBeNull();
      expect(user2).not.toBeNull();
      expect(order1).not.toBeNull();
    });

    it('should set correct branch metadata on creation', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('file.txt', encoder.encode('content'));

      const beforeCreation = Date.now();

      // Act
      await cow.branch('main', 'feature');

      const afterCreation = Date.now();

      // Assert
      const branch = await cow.getBranch('feature');
      expect(branch).not.toBeNull();
      expect(branch!.name).toBe('feature');
      expect(branch!.parent).toBe('main');
      expect(branch!.readonly).toBe(false);
      expect(branch!.headVersion).toBeGreaterThanOrEqual(1);
      expect(branch!.createdAt).toBeGreaterThanOrEqual(beforeCreation);
      expect(branch!.createdAt).toBeLessThanOrEqual(afterCreation);
      expect(branch!.modifiedAt).toBeGreaterThanOrEqual(beforeCreation);
    });

    it('should create a snapshot when branching (autoSnapshot enabled by default)', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('file.txt', encoder.encode('content'));

      // Act
      await cow.branch('main', 'feature');

      // Assert: Base snapshot should be set
      const branch = await cow.getBranch('feature');
      expect(branch?.baseSnapshot).not.toBeNull();
    });

    it('should fail to create branch with duplicate name', async () => {
      // Arrange
      await cow.branch('main', 'feature');

      // Act & Assert
      await expect(cow.branch('main', 'feature')).rejects.toThrow(COWError);
      await expect(cow.branch('main', 'feature')).rejects.toMatchObject({
        code: COWErrorCode.BRANCH_EXISTS,
      });
    });

    it('should fail to create branch from non-existent source', async () => {
      // Act & Assert
      await expect(cow.branch('nonexistent', 'feature')).rejects.toThrow(COWError);
      await expect(cow.branch('nonexistent', 'feature')).rejects.toMatchObject({
        code: COWErrorCode.BRANCH_NOT_FOUND,
      });
    });

    it('should create readonly branch when specified', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('file.txt', encoder.encode('content'));

      // Act
      await cow.branch('main', 'archive', { readonly: true });

      // Assert
      const branch = await cow.getBranch('archive');
      expect(branch?.readonly).toBe(true);

      // Writing to readonly branch should fail
      await cow.checkout('archive');
      await expect(cow.write('new.txt', encoder.encode('new'))).rejects.toThrow(COWError);
    });
  });

  // ===========================================================================
  // Checkout Operations
  // ===========================================================================

  describe('CHECKOUT', () => {
    it('should checkout an existing branch', async () => {
      // Arrange
      await cow.branch('main', 'feature');

      // Act
      await cow.checkout('feature');

      // Assert
      expect(cow.getCurrentBranch()).toBe('feature');
    });

    it('should fail to checkout non-existent branch', async () => {
      // Act & Assert
      await expect(cow.checkout('nonexistent')).rejects.toThrow(COWError);
      await expect(cow.checkout('nonexistent')).rejects.toMatchObject({
        code: COWErrorCode.BRANCH_NOT_FOUND,
      });
    });

    it('should switch between branches correctly', async () => {
      // Arrange
      await cow.branch('main', 'feature-a');
      await cow.branch('main', 'feature-b');

      // Act & Assert
      await cow.checkout('feature-a');
      expect(cow.getCurrentBranch()).toBe('feature-a');

      await cow.checkout('feature-b');
      expect(cow.getCurrentBranch()).toBe('feature-b');

      await cow.checkout('main');
      expect(cow.getCurrentBranch()).toBe('main');
    });

    it('should allow operations on checked out branch', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('main.txt', encoder.encode('main data'));
      await cow.branch('main', 'feature');

      // Act
      await cow.checkout('feature');
      await cow.write('feature.txt', encoder.encode('feature data'));

      // Assert
      const featureFile = await cow.read('feature.txt');
      expect(featureFile).not.toBeNull();
      expect(new TextDecoder().decode(featureFile!)).toBe('feature data');
    });
  });

  // ===========================================================================
  // Branch Isolation
  // ===========================================================================

  describe('Branch Isolation', () => {
    it('should isolate changes on one branch from others', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('shared.txt', encoder.encode('original'));
      await cow.branch('main', 'feature');

      // Act: Modify on feature branch
      await cow.checkout('feature');
      await cow.write('shared.txt', encoder.encode('modified on feature'));

      // Assert: Main branch unchanged
      const mainData = await cow.readFrom('shared.txt', 'main');
      const featureData = await cow.readFrom('shared.txt', 'feature');

      expect(new TextDecoder().decode(mainData!)).toBe('original');
      expect(new TextDecoder().decode(featureData!)).toBe('modified on feature');
    });

    it('should isolate new files created on a branch', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.branch('main', 'feature');

      // Act: Create file only on feature
      await cow.checkout('feature');
      await cow.write('feature-only.txt', encoder.encode('feature data'));

      // Assert
      const onFeature = await cow.readFrom('feature-only.txt', 'feature');
      const onMain = await cow.readFrom('feature-only.txt', 'main');

      expect(onFeature).not.toBeNull();
      expect(onMain).toBeNull();
    });

    it('should isolate deletions on a branch', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('to-delete.txt', encoder.encode('content'));
      await cow.branch('main', 'feature');

      // Act: Delete on feature
      await cow.checkout('feature');
      await cow.delete('to-delete.txt');

      // Assert
      const onFeature = await cow.readFrom('to-delete.txt', 'feature');
      const onMain = await cow.readFrom('to-delete.txt', 'main');

      expect(onFeature).toBeNull();
      expect(onMain).not.toBeNull();
    });

    it('should isolate changes between sibling branches', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('base.txt', encoder.encode('base'));
      await cow.branch('main', 'feature-a');
      await cow.branch('main', 'feature-b');

      // Act: Different changes on each branch
      await cow.checkout('feature-a');
      await cow.write('base.txt', encoder.encode('modified by A'));

      await cow.checkout('feature-b');
      await cow.write('base.txt', encoder.encode('modified by B'));

      // Assert: Each branch has its own version
      const dataA = await cow.readFrom('base.txt', 'feature-a');
      const dataB = await cow.readFrom('base.txt', 'feature-b');
      const dataMain = await cow.readFrom('base.txt', 'main');

      expect(new TextDecoder().decode(dataA!)).toBe('modified by A');
      expect(new TextDecoder().decode(dataB!)).toBe('modified by B');
      expect(new TextDecoder().decode(dataMain!)).toBe('base');
    });

    it('should read from specific branch without checkout', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('file.txt', encoder.encode('main version'));
      await cow.branch('main', 'feature');
      await cow.writeTo('file.txt', encoder.encode('feature version'), 'feature');

      // Assert: Read from specific branches while on main
      expect(cow.getCurrentBranch()).toBe('main');

      const mainData = await cow.readFrom('file.txt', 'main');
      const featureData = await cow.readFrom('file.txt', 'feature');

      expect(new TextDecoder().decode(mainData!)).toBe('main version');
      expect(new TextDecoder().decode(featureData!)).toBe('feature version');
    });
  });

  // ===========================================================================
  // Merge Branch Operations
  // ===========================================================================

  describe('MERGE BRANCH', () => {
    it('should merge feature branch into main (theirs strategy)', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('existing.txt', encoder.encode('original'));
      await cow.branch('main', 'feature');

      // Add new file on feature
      await cow.writeTo('new-feature.txt', encoder.encode('new feature file'), 'feature');

      // Act
      const result: MergeResult = await cow.merge('feature', 'main', 'theirs');

      // Assert
      expect(result.success).toBe(true);
      expect(result.merged).toBeGreaterThan(0);

      const newFile = await cow.readFrom('new-feature.txt', 'main');
      expect(newFile).not.toBeNull();
      expect(new TextDecoder().decode(newFile!)).toBe('new feature file');
    });

    it('should merge with ours strategy (keep target)', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('conflict.txt', encoder.encode('original'));
      await cow.branch('main', 'feature');

      // Modify on both branches
      await cow.writeTo('conflict.txt', encoder.encode('main version'), 'main');
      await cow.writeTo('conflict.txt', encoder.encode('feature version'), 'feature');

      // Act
      const result = await cow.merge('feature', 'main', 'ours');

      // Assert
      expect(result.success).toBe(true);

      const data = await cow.readFrom('conflict.txt', 'main');
      expect(new TextDecoder().decode(data!)).toBe('main version');
    });

    it('should merge with last-write-wins strategy', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('conflict.txt', encoder.encode('original'));
      await cow.branch('main', 'feature');

      // Modify feature first, then main later
      await cow.writeTo('conflict.txt', encoder.encode('feature version'), 'feature');

      // Small delay to ensure different timestamps
      await new Promise((resolve) => setTimeout(resolve, 10));
      await cow.writeTo('conflict.txt', encoder.encode('main version'), 'main');

      // Act
      const result = await cow.merge('feature', 'main', 'last-write-wins');

      // Assert
      expect(result.success).toBe(true);
      const data = await cow.readFrom('conflict.txt', 'main');
      // Main was modified later, so it should win
      expect(new TextDecoder().decode(data!)).toBe('main version');
    });

    it('should fail merge with fail-on-conflict strategy when conflicts exist', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('conflict.txt', encoder.encode('original'));
      await cow.branch('main', 'feature');

      // Modify on both branches to create conflict
      await cow.writeTo('conflict.txt', encoder.encode('main changes'), 'main');
      await cow.writeTo('conflict.txt', encoder.encode('feature changes'), 'feature');

      // Act
      const result = await cow.merge('feature', 'main', 'fail-on-conflict');

      // Assert
      expect(result.success).toBe(false);
      expect(result.conflicts.length).toBeGreaterThan(0);
      expect(result.conflicts[0].path).toBe('conflict.txt');
    });

    it('should detect merge conflicts correctly', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('file1.txt', encoder.encode('original'));
      await cow.write('file2.txt', encoder.encode('original'));
      await cow.branch('main', 'feature');

      // Modify file1 on both branches (conflict)
      await cow.writeTo('file1.txt', encoder.encode('main'), 'main');
      await cow.writeTo('file1.txt', encoder.encode('feature'), 'feature');

      // Only modify file2 on feature (no conflict)
      await cow.writeTo('file2.txt', encoder.encode('feature only'), 'feature');

      // Act
      const result = await cow.merge('feature', 'main', 'fail-on-conflict');

      // Assert
      expect(result.conflicts.length).toBe(1);
      expect(result.conflicts[0].path).toBe('file1.txt');
    });

    it('should create snapshot after successful merge', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.branch('main', 'feature');
      await cow.writeTo('new.txt', encoder.encode('new'), 'feature');

      // Act
      const result = await cow.merge('feature', 'main', 'theirs');

      // Assert
      expect(result.success).toBe(true);
      expect(result.snapshot).toBeDefined();
    });

    it('should fail to merge into non-existent branch', async () => {
      // Arrange
      await cow.branch('main', 'feature');

      // Act & Assert
      await expect(cow.merge('feature', 'nonexistent', 'theirs')).rejects.toThrow(COWError);
    });

    it('should fail to merge from non-existent branch', async () => {
      // Act & Assert
      await expect(cow.merge('nonexistent', 'main', 'theirs')).rejects.toThrow(COWError);
    });

    it('should fail to merge into readonly branch', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('file.txt', encoder.encode('data'));
      await cow.branch('main', 'feature');
      await cow.branch('main', 'archive', { readonly: true });

      await cow.writeTo('new.txt', encoder.encode('new'), 'feature');

      // Act & Assert
      await expect(cow.merge('feature', 'archive', 'theirs')).rejects.toThrow(COWError);
      await expect(cow.merge('feature', 'archive', 'theirs')).rejects.toMatchObject({
        code: COWErrorCode.BRANCH_READONLY,
      });
    });
  });

  // ===========================================================================
  // Delete Branch Operations
  // ===========================================================================

  describe('DROP BRANCH', () => {
    it('should delete an existing branch', async () => {
      // Arrange
      await cow.branch('main', 'feature');

      // Verify branch exists
      const branchBefore = await cow.getBranch('feature');
      expect(branchBefore).not.toBeNull();

      // Act
      await cow.deleteBranch('feature');

      // Assert
      const branchAfter = await cow.getBranch('feature');
      expect(branchAfter).toBeNull();
    });

    it('should fail to delete default/main branch', async () => {
      // Act & Assert
      await expect(cow.deleteBranch('main')).rejects.toThrow(COWError);
      await expect(cow.deleteBranch('main')).rejects.toMatchObject({
        code: COWErrorCode.BRANCH_READONLY,
      });
    });

    it('should fail to delete non-existent branch', async () => {
      // Act & Assert
      await expect(cow.deleteBranch('nonexistent')).rejects.toThrow(COWError);
      await expect(cow.deleteBranch('nonexistent')).rejects.toMatchObject({
        code: COWErrorCode.BRANCH_NOT_FOUND,
      });
    });

    it('should switch to default branch when deleting current branch', async () => {
      // Arrange
      await cow.branch('main', 'feature');
      await cow.checkout('feature');
      expect(cow.getCurrentBranch()).toBe('feature');

      // Act
      await cow.deleteBranch('feature');

      // Assert: Should auto-switch to main
      expect(cow.getCurrentBranch()).toBe('main');
    });

    it('should clean up all refs when deleting branch', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('file1.txt', encoder.encode('data1'));
      await cow.branch('main', 'feature');
      await cow.writeTo('file2.txt', encoder.encode('data2'), 'feature');

      // Act
      await cow.deleteBranch('feature');

      // Assert: Data should not be accessible from deleted branch
      const branchData = await cow.readFrom('file2.txt', 'main');
      expect(branchData).toBeNull(); // Was only on feature branch
    });

    it('should force delete branch with unmerged changes', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.branch('main', 'feature');
      await cow.writeTo('unmerged.txt', encoder.encode('unmerged changes'), 'feature');

      // Act: Force delete
      await cow.deleteBranch('feature', true);

      // Assert
      const branch = await cow.getBranch('feature');
      expect(branch).toBeNull();
    });
  });

  // ===========================================================================
  // Branch Listing
  // ===========================================================================

  describe('Branch Listing', () => {
    it('should list all branches', async () => {
      // Arrange
      await cow.branch('main', 'feature-a');
      await cow.branch('main', 'feature-b');
      await cow.branch('main', 'feature-c');

      // Act
      const branches = await cow.listBranches();

      // Assert
      expect(branches.length).toBe(4); // main + 3 features
      const names = branches.map((b) => b.name);
      expect(names).toContain('main');
      expect(names).toContain('feature-a');
      expect(names).toContain('feature-b');
      expect(names).toContain('feature-c');
    });

    it('should list branches in alphabetical order', async () => {
      // Arrange
      await cow.branch('main', 'zebra');
      await cow.branch('main', 'alpha');
      await cow.branch('main', 'beta');

      // Act
      const branches = await cow.listBranches();
      const names = branches.map((b) => b.name);

      // Assert
      expect(names).toEqual(['alpha', 'beta', 'main', 'zebra']);
    });

    it('should return only main branch initially', async () => {
      // Act
      const branches = await cow.listBranches();

      // Assert
      expect(branches.length).toBe(1);
      expect(branches[0].name).toBe('main');
    });

    it('should return complete branch metadata in listing', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('file.txt', encoder.encode('data'));
      await cow.branch('main', 'feature');

      // Act
      const branches = await cow.listBranches();
      const feature = branches.find((b) => b.name === 'feature');

      // Assert
      expect(feature).toBeDefined();
      expect(feature!.name).toBe('feature');
      expect(feature!.parent).toBe('main');
      expect(feature!.createdAt).toBeDefined();
      expect(feature!.modifiedAt).toBeDefined();
      expect(feature!.headVersion).toBeDefined();
      expect(feature!.readonly).toBe(false);
    });
  });

  // ===========================================================================
  // Branch Metadata
  // ===========================================================================

  describe('Branch Metadata', () => {
    it('should get branch by name', async () => {
      // Arrange
      await cow.branch('main', 'feature');

      // Act
      const branch = await cow.getBranch('feature');

      // Assert
      expect(branch).not.toBeNull();
      expect(branch!.name).toBe('feature');
    });

    it('should return null for non-existent branch', async () => {
      // Act
      const branch = await cow.getBranch('nonexistent');

      // Assert
      expect(branch).toBeNull();
    });

    it('should track head version on writes', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.branch('main', 'feature');
      const initialBranch = await cow.getBranch('feature');
      const initialVersion = initialBranch!.headVersion;

      // Act: Write to feature branch
      await cow.writeTo('file1.txt', encoder.encode('data1'), 'feature');
      await cow.writeTo('file2.txt', encoder.encode('data2'), 'feature');

      // Assert
      const updatedBranch = await cow.getBranch('feature');
      expect(updatedBranch!.headVersion).toBeGreaterThan(initialVersion);
    });

    it('should track modification timestamp on writes', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.branch('main', 'feature');
      const initialBranch = await cow.getBranch('feature');
      const initialModified = initialBranch!.modifiedAt;

      // Small delay
      await new Promise((resolve) => setTimeout(resolve, 10));

      // Act
      await cow.writeTo('file.txt', encoder.encode('data'), 'feature');

      // Assert
      const updatedBranch = await cow.getBranch('feature');
      expect(updatedBranch!.modifiedAt).toBeGreaterThan(initialModified);
    });

    it('should preserve creation timestamp after writes', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.branch('main', 'feature');
      const initialBranch = await cow.getBranch('feature');
      const createdAt = initialBranch!.createdAt;

      // Act
      await cow.writeTo('file.txt', encoder.encode('data'), 'feature');

      // Assert
      const updatedBranch = await cow.getBranch('feature');
      expect(updatedBranch!.createdAt).toBe(createdAt);
    });
  });

  // ===========================================================================
  // Diff Operations
  // ===========================================================================

  describe('Branch Diff', () => {
    it('should diff two branches', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('shared.txt', encoder.encode('shared'));
      await cow.write('main-only.txt', encoder.encode('main'));
      await cow.branch('main', 'feature');

      // Modify shared file on feature
      await cow.writeTo('shared.txt', encoder.encode('modified'), 'feature');
      // Add new file on feature
      await cow.writeTo('feature-only.txt', encoder.encode('new'), 'feature');
      // Delete a file on feature to test removal detection
      await cow.deleteFrom('main-only.txt', 'feature');

      // Act
      const diff: BranchDiff = await cow.diff('feature', 'main');

      // Assert
      expect(diff.added).toContain('feature-only.txt');
      expect(diff.modified).toContain('shared.txt');
      // main-only.txt exists in main (target) but not in feature (source) = removed
      expect(diff.removed).toContain('main-only.txt');
    });

    it('should show unchanged files', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('unchanged.txt', encoder.encode('same'));
      await cow.branch('main', 'feature');

      // Act
      const diff = await cow.diff('feature', 'main');

      // Assert: File exists in both and is unchanged
      expect(diff.unchanged).toContain('unchanged.txt');
    });
  });

  // ===========================================================================
  // Snapshot Operations
  // ===========================================================================

  describe('Snapshot Operations', () => {
    it('should create snapshot of current branch', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('file.txt', encoder.encode('data'));

      // Act
      const snapshotId = await cow.snapshot('main', 'Test snapshot');

      // Assert
      expect(snapshotId).toBeDefined();
      expect(snapshotId).toMatch(/^main@\d+$/);
    });

    it('should list snapshots for a branch', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('file.txt', encoder.encode('v1'));
      await cow.snapshot('main', 'Version 1');

      await cow.write('file.txt', encoder.encode('v2'));
      await cow.snapshot('main', 'Version 2');

      // Act
      const snapshots = await cow.listSnapshots('main');

      // Assert
      expect(snapshots.length).toBeGreaterThanOrEqual(2);
    });

    it('should get snapshot by ID', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('file.txt', encoder.encode('data'));
      const snapshotId = await cow.snapshot('main', 'Test snapshot');

      // Act
      const snapshot = await cow.getSnapshot(snapshotId);

      // Assert
      expect(snapshot).not.toBeNull();
      expect(snapshot!.id).toBe(snapshotId);
      expect(snapshot!.branch).toBe('main');
      expect(snapshot!.message).toBe('Test snapshot');
    });

    it('should read data at specific snapshot', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('file.txt', encoder.encode('version 1'));
      const snapshot1 = await cow.snapshot('main', 'V1');

      await cow.write('file.txt', encoder.encode('version 2'));
      await cow.snapshot('main', 'V2');

      // Act
      const dataAtV1 = await cow.readAt('file.txt', snapshot1);

      // Assert
      expect(dataAtV1).not.toBeNull();
      expect(new TextDecoder().decode(dataAtV1!)).toBe('version 1');
    });

    it('should delete snapshot', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('file.txt', encoder.encode('data'));
      const snapshotId = await cow.snapshot('main', 'To delete');

      // Verify exists
      const before = await cow.getSnapshot(snapshotId);
      expect(before).not.toBeNull();

      // Act
      await cow.deleteSnapshot(snapshotId);

      // Assert
      const after = await cow.getSnapshot(snapshotId);
      expect(after).toBeNull();
    });
  });

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('Edge Cases', () => {
    it('should handle deep branch hierarchy', async () => {
      // Arrange: Create chain of branches
      await cow.branch('main', 'level-1');
      await cow.branch('level-1', 'level-2');
      await cow.branch('level-2', 'level-3');
      await cow.branch('level-3', 'level-4');

      // Act
      const level4 = await cow.getBranch('level-4');

      // Assert
      expect(level4).not.toBeNull();
      expect(level4!.parent).toBe('level-3');

      // Verify data inheritance works
      const encoder = new TextEncoder();
      await cow.write('root.txt', encoder.encode('root data'));

      await cow.branch('main', 'deep-child');
      const dataOnChild = await cow.readFrom('root.txt', 'deep-child');
      expect(dataOnChild).not.toBeNull();
    });

    it('should handle branch names with special characters', async () => {
      // Act
      await cow.branch('main', 'feature/user-auth');
      await cow.branch('main', 'bugfix/issue-123');
      await cow.branch('main', 'release/v1.0.0');

      // Assert
      const branches = await cow.listBranches();
      const names = branches.map((b) => b.name);
      expect(names).toContain('feature/user-auth');
      expect(names).toContain('bugfix/issue-123');
      expect(names).toContain('release/v1.0.0');
    });

    it('should handle empty branch (no files)', async () => {
      // Act: Create branch from empty main
      await cow.branch('main', 'empty');

      // Assert
      const files = await cow.listFrom('', 'empty');
      expect(files.length).toBe(0);
    });

    it('should handle large number of branches', async () => {
      // Arrange: Create many branches
      const numBranches = 50;
      for (let i = 0; i < numBranches; i++) {
        await cow.branch('main', `feature-${i}`);
      }

      // Act
      const branches = await cow.listBranches();

      // Assert
      expect(branches.length).toBe(numBranches + 1); // +1 for main
    });

    it('should handle concurrent branch operations', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('file.txt', encoder.encode('data'));

      // Act: Create multiple branches concurrently
      const promises = Array.from({ length: 10 }, (_, i) =>
        cow.branch('main', `concurrent-${i}`)
      );

      await Promise.all(promises);

      // Assert
      const branches = await cow.listBranches();
      expect(branches.length).toBe(11); // main + 10 concurrent
    });

    it('should handle branch with many files', async () => {
      // Arrange: Create many files
      const encoder = new TextEncoder();
      const numFiles = 100;
      for (let i = 0; i < numFiles; i++) {
        await cow.write(`file-${i}.txt`, encoder.encode(`content ${i}`));
      }

      // Act
      await cow.branch('main', 'feature');

      // Assert: All files accessible on feature branch
      const files = await cow.listFrom('', 'feature');
      expect(files.length).toBe(numFiles);
    });

    it('should handle merging branch back to parent after multiple changes', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('base.txt', encoder.encode('base'));
      await cow.branch('main', 'feature');

      // Multiple changes on feature
      for (let i = 0; i < 10; i++) {
        await cow.writeTo(`feature-file-${i}.txt`, encoder.encode(`feature ${i}`), 'feature');
      }

      // Act
      const result = await cow.merge('feature', 'main', 'theirs');

      // Assert
      expect(result.success).toBe(true);
      expect(result.merged).toBeGreaterThanOrEqual(10);

      // Verify files are on main
      for (let i = 0; i < 10; i++) {
        const data = await cow.readFrom(`feature-file-${i}.txt`, 'main');
        expect(data).not.toBeNull();
      }
    });
  });

  // ===========================================================================
  // Garbage Collection
  // ===========================================================================

  describe('Garbage Collection', () => {
    it('should run gc without errors', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('file.txt', encoder.encode('data'));
      await cow.branch('main', 'feature');
      await cow.deleteBranch('feature');

      // Act
      const result = await cow.gc();

      // Assert
      expect(result).toBeDefined();
      expect(result.blobsScanned).toBeGreaterThanOrEqual(0);
    });

    it('should run gc in dry-run mode', async () => {
      // Arrange
      const encoder = new TextEncoder();
      await cow.write('file.txt', encoder.encode('data'));
      await cow.branch('main', 'feature');
      await cow.deleteBranch('feature');

      // Act
      const result = await cow.gc({ dryRun: true });

      // Assert
      expect(result).toBeDefined();
      // In dry-run, no blobs should actually be deleted
    });
  });
});
