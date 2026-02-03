/**
 * Production Validation Tests for Time Travel and Database Branching
 *
 * Validates production-readiness of time travel and branching features:
 * - Create branch from specific commit
 * - Query data at a point in time (AS OF)
 * - Merge branches
 * - Branch isolation (writes to branch don't affect main)
 * - Branch metadata management
 *
 * Uses workers-vitest-pool (NO MOCKS) as required by project guidelines.
 * Reuses the TestBranchDO class already registered in vitest.config.ts
 * from the sibling branch.test.ts file.
 *
 * @packageDocumentation
 */

import { describe, it, expect } from 'vitest';
import { env, runInDurableObject } from 'cloudflare:test';

import { TestBranchDO } from '../branch.test.js';

// =============================================================================
// Test Helper
// =============================================================================

let validationCounter = 0;

function getStub() {
  const id = env.TEST_BRANCH_DO.idFromName(`branch-validation-${Date.now()}-${validationCounter++}`);
  return env.TEST_BRANCH_DO.get(id);
}

// =============================================================================
// 1. Create Branch from Specific Commit
// =============================================================================

describe('branch production validation - create branch from specific commit', () => {
  it('should create a branch from a specific historical commit', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      // Build a commit history on main
      const commit1 = await instance.commit('Initial schema');
      const commit2 = await instance.commit('Add users table');
      const commit3 = await instance.commit('Add orders table');

      // Create a branch pinned to commit2 (before orders table)
      const branch = await instance.createBranch({
        name: 'hotfix-from-users',
        from: 'main',
        commit: commit2,
      });

      expect(branch.name).toBe('hotfix-from-users');
      expect(branch.baseCommit).toBe(commit2);
      expect(branch.head).toBe(commit2);
      expect(branch.parent).toBe('main');

      // The branch head should NOT be at commit3
      expect(branch.head).not.toBe(commit3);
    });
  });

  it('should preserve the full commit chain when branching from a past commit', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      const commit1 = await instance.commit('Genesis');
      const commit2 = await instance.commit('Second');
      const commit3 = await instance.commit('Third');

      // Branch from commit2
      await instance.createBranch({
        name: 'past-branch',
        from: 'main',
        commit: commit2,
      });

      // Checkout and verify the log only contains commit2 and commit1
      await instance.checkout('past-branch');
      const log = await instance.log('past-branch', { limit: 10 });

      const commitIds = log.map(entry => entry.commit.id);
      expect(commitIds).toContain(commit2);
      expect(commitIds).toContain(commit1);
      expect(commitIds).not.toContain(commit3);
    });
  });

  it('should reject branching from a non-existent commit', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.commit('Baseline');

      await expect(
        instance.createBranch({
          name: 'bad-commit-branch',
          from: 'main',
          commit: 'nonexistent-commit-id-12345',
        })
      ).rejects.toThrow();
    });
  });

  it('should allow divergent work on a branch created from a past commit', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      const commit1 = await instance.commit('V1');
      const commit2 = await instance.commit('V2');
      const commit3 = await instance.commit('V3');

      // Branch from commit1
      await instance.createBranch({
        name: 'legacy-fix',
        from: 'main',
        commit: commit1,
      });

      // Switch to the branch and add new work
      await instance.checkout('legacy-fix');
      const fixCommit = await instance.commit('Legacy bugfix');

      // Verify the fix commit's parent is commit1, not commit3
      const manager = await instance.getManager();
      const fixData = await manager.getCommit(fixCommit);
      expect(fixData).not.toBeNull();
      expect(fixData!.parents).toContain(commit1);
      expect(fixData!.parents).not.toContain(commit3);
    });
  });

  it('should create multiple branches from the same historical commit', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      const baseCommit = await instance.commit('Stable release');
      await instance.commit('Unstable work');

      // Create two branches from the same stable point
      const branchA = await instance.createBranch({
        name: 'patch-1.0.1',
        from: 'main',
        commit: baseCommit,
      });
      const branchB = await instance.createBranch({
        name: 'patch-1.0.2',
        from: 'main',
        commit: baseCommit,
      });

      expect(branchA.head).toBe(baseCommit);
      expect(branchB.head).toBe(baseCommit);
      expect(branchA.baseCommit).toBe(branchB.baseCommit);
    });
  });
});

// =============================================================================
// 2. Query Data at a Point in Time (AS OF)
// =============================================================================

describe('branch production validation - query data at a point in time (AS OF)', () => {
  it('should retrieve the exact state at a specific commit via checkout', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.createBranch({ name: 'time-query' });
      await instance.checkout('time-query');

      const commitA = await instance.commit('State A: initial');
      const commitB = await instance.commit('State B: updated');
      const commitC = await instance.commit('State C: latest');

      // Travel back to State B
      const result = await instance.checkout('time-query', {
        commit: commitB,
        force: true,
      });

      expect(result.current.commit).toBe(commitB);

      // Verify we can retrieve the commit data at this point
      const manager = await instance.getManager();
      const commitData = await manager.getCommit(commitB);
      expect(commitData).not.toBeNull();
      expect(commitData!.message).toBe('State B: updated');
    });
  });

  it('should retrieve commit metadata for any historical point', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      const commits: string[] = [];
      for (let i = 0; i < 5; i++) {
        const id = await instance.commit(`Revision ${i}`);
        commits.push(id);
      }

      // Query each historical commit via the manager
      const manager = await instance.getManager();
      for (let i = 0; i < commits.length; i++) {
        const data = await manager.getCommit(commits[i]!);
        expect(data).not.toBeNull();
        expect(data!.message).toBe(`Revision ${i}`);
        expect(data!.id).toBe(commits[i]);
        expect(data!.timestamp).toBeGreaterThan(0);
      }
    });
  });

  it('should filter history by date range for point-in-time queries', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.createBranch({ name: 'date-range' });
      await instance.checkout('date-range');

      await instance.commit('Early work');
      const midTimestamp = Date.now();
      // Small delay to ensure timestamp ordering
      await instance.commit('Later work');

      const sinceDate = new Date(midTimestamp);
      const log = await instance.log('date-range', { since: sinceDate, limit: 100 });

      // All returned commits must be at or after the midTimestamp
      for (const entry of log) {
        expect(entry.commit.timestamp).toBeGreaterThanOrEqual(midTimestamp);
      }
    });
  });

  it('should walk the parent chain to reconstruct full history at any point', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.createBranch({ name: 'parent-chain' });
      await instance.checkout('parent-chain');

      const c1 = await instance.commit('First');
      const c2 = await instance.commit('Second');
      const c3 = await instance.commit('Third');
      const c4 = await instance.commit('Fourth');

      // Start from c4 and walk back using the manager
      const manager = await instance.getManager();
      let current: string | null = c4;
      const chain: string[] = [];

      while (current) {
        chain.push(current);
        const data = await manager.getCommit(current);
        if (!data || data.parents.length === 0) break;
        current = data.parents[0] ?? null;
      }

      expect(chain).toContain(c4);
      expect(chain).toContain(c3);
      expect(chain).toContain(c2);
      expect(chain).toContain(c1);
      // Verify order: c4 first, then c3, c2, c1
      expect(chain.indexOf(c4)).toBeLessThan(chain.indexOf(c3));
      expect(chain.indexOf(c3)).toBeLessThan(chain.indexOf(c2));
      expect(chain.indexOf(c2)).toBeLessThan(chain.indexOf(c1));
    });
  });

  it('should preserve commit author information for audit trails', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      const commitId = await instance.commit('Auditable change');
      const manager = await instance.getManager();
      const data = await manager.getCommit(commitId);

      expect(data).not.toBeNull();
      expect(data!.author).toBeDefined();
      expect(data!.author.name).toBe('Test User');
      expect(data!.author.email).toBe('test@example.com');
    });
  });
});

// =============================================================================
// 3. Merge Branches
// =============================================================================

describe('branch production validation - merge branches', () => {
  it('should fast-forward merge when target has no new commits', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.commit('Main baseline');

      await instance.createBranch({ name: 'ff-merge-test' });
      await instance.checkout('ff-merge-test');
      await instance.commit('Feature work 1');
      await instance.commit('Feature work 2');

      await instance.checkout('main');
      const result = await instance.merge('ff-merge-test', 'main');

      expect(result.success).toBe(true);
      expect(result.mergeType).toBe('fast-forward');
      expect(result.source).toBe('ff-merge-test');
      expect(result.target).toBe('main');
      expect(result.conflicts).toHaveLength(0);
    });
  });

  it('should create a merge commit when branches have diverged', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.commit('Common ancestor');

      await instance.createBranch({ name: 'diverged-merge' });
      await instance.checkout('diverged-merge');
      await instance.commit('Branch-side change');

      await instance.checkout('main');
      await instance.commit('Main-side change');

      const result = await instance.merge('diverged-merge', 'main');

      expect(result.success).toBe(true);
      expect(result.mergeType).toBe('merge-commit');
      expect(result.commit).toBeDefined();

      // The merge commit should have two parents
      const manager = await instance.getManager();
      const mergeCommit = await manager.getCommit(result.commit!);
      expect(mergeCommit).not.toBeNull();
      expect(mergeCommit!.parents.length).toBe(2);
    });
  });

  it('should force a merge commit with no-ff strategy even when ff is possible', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.commit('Base');

      await instance.createBranch({ name: 'no-ff-merge-test' });
      await instance.checkout('no-ff-merge-test');
      await instance.commit('Feature addition');

      await instance.checkout('main');
      const result = await instance.merge('no-ff-merge-test', 'main', {
        strategy: 'no-ff',
      });

      expect(result.success).toBe(true);
      expect(result.mergeType).toBe('merge-commit');
    });
  });

  it('should reject fast-forward strategy when branches have diverged', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.commit('Base');

      await instance.createBranch({ name: 'ff-reject-test' });
      await instance.checkout('ff-reject-test');
      await instance.commit('Feature side');

      await instance.checkout('main');
      await instance.commit('Main side');

      await expect(
        instance.merge('ff-reject-test', 'main', { strategy: 'fast-forward' })
      ).rejects.toThrow();
    });
  });

  it('should merge with a custom commit message', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.commit('Baseline');

      await instance.createBranch({ name: 'custom-msg-merge' });
      await instance.checkout('custom-msg-merge');
      await instance.commit('Work');

      await instance.checkout('main');
      const result = await instance.merge('custom-msg-merge', 'main', {
        message: 'Release: merge custom-msg-merge for v2.0',
      });

      expect(result.success).toBe(true);
    });
  });

  it('should handle sequential merges from multiple feature branches', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.commit('Foundation');

      // First feature
      await instance.createBranch({ name: 'feat-alpha' });
      await instance.checkout('feat-alpha');
      await instance.commit('Alpha work');
      await instance.checkout('main');
      const r1 = await instance.merge('feat-alpha', 'main');
      expect(r1.success).toBe(true);

      // Second feature (branched after alpha was merged)
      await instance.createBranch({ name: 'feat-beta' });
      await instance.checkout('feat-beta');
      await instance.commit('Beta work');
      await instance.checkout('main');
      const r2 = await instance.merge('feat-beta', 'main');
      expect(r2.success).toBe(true);

      // Third feature
      await instance.createBranch({ name: 'feat-gamma' });
      await instance.checkout('feat-gamma');
      await instance.commit('Gamma work');
      await instance.checkout('main');
      const r3 = await instance.merge('feat-gamma', 'main');
      expect(r3.success).toBe(true);

      // Main should have a rich history
      const log = await instance.log('main', { limit: 20 });
      expect(log.length).toBeGreaterThanOrEqual(4);
    });
  });

  it('should correctly track source and target in merge result', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.commit('Root');

      await instance.createBranch({ name: 'source-branch' });
      await instance.checkout('source-branch');
      await instance.commit('Source commit');

      await instance.checkout('main');
      const result = await instance.merge('source-branch', 'main');

      expect(result.source).toBe('source-branch');
      expect(result.target).toBe('main');
    });
  });

  it('should update target branch head after merge', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.commit('Pre-merge');

      const mainBefore = await instance.getBranch('main');
      const headBefore = mainBefore!.head;

      await instance.createBranch({ name: 'head-update-test' });
      await instance.checkout('head-update-test');
      await instance.commit('New content');

      await instance.checkout('main');
      await instance.merge('head-update-test', 'main');

      const mainAfter = await instance.getBranch('main');
      expect(mainAfter!.head).not.toBe(headBefore);
    });
  });
});

// =============================================================================
// 4. Branch Isolation (writes to branch don't affect main)
// =============================================================================

describe('branch production validation - branch isolation', () => {
  it('should isolate commits so branch work does not appear on main', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      const mainCommit = await instance.commit('Main only');

      await instance.createBranch({ name: 'isolated-work' });
      await instance.checkout('isolated-work');
      const branchCommit = await instance.commit('Branch only');

      // Main log should not contain the branch commit
      const mainLog = await instance.log('main', { limit: 20 });
      const mainCommitIds = mainLog.map(e => e.commit.id);
      expect(mainCommitIds).toContain(mainCommit);
      expect(mainCommitIds).not.toContain(branchCommit);

      // Branch log should contain its commit
      const branchLog = await instance.log('isolated-work', { limit: 20 });
      const branchCommitIds = branchLog.map(e => e.commit.id);
      expect(branchCommitIds).toContain(branchCommit);
    });
  });

  it('should give each branch an independent head pointer', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      const mainHead1 = await instance.commit('Main v1');

      await instance.createBranch({ name: 'independent-head' });
      await instance.checkout('independent-head');
      const branchHead = await instance.commit('Branch v1');

      const mainMeta = await instance.getBranch('main');
      const branchMeta = await instance.getBranch('independent-head');

      expect(mainMeta!.head).toBe(mainHead1);
      expect(branchMeta!.head).toBe(branchHead);
      expect(mainMeta!.head).not.toBe(branchMeta!.head);
    });
  });

  it('should not pollute main when multiple branches commit concurrently', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      const mainBase = await instance.commit('Shared base');

      await instance.createBranch({ name: 'worker-a' });
      await instance.createBranch({ name: 'worker-b' });
      await instance.createBranch({ name: 'worker-c' });

      // Commit on each branch
      await instance.checkout('worker-a');
      await instance.commit('A-work-1');
      await instance.commit('A-work-2');

      await instance.checkout('worker-b');
      await instance.commit('B-work-1');

      await instance.checkout('worker-c');
      await instance.commit('C-work-1');
      await instance.commit('C-work-2');
      await instance.commit('C-work-3');

      // Main should still only have the base commit
      const mainLog = await instance.log('main', { limit: 20 });
      const mainMessages = mainLog.map(e => e.commit.message);
      expect(mainMessages).toContain('Shared base');
      expect(mainMessages).not.toContain('A-work-1');
      expect(mainMessages).not.toContain('B-work-1');
      expect(mainMessages).not.toContain('C-work-1');
    });
  });

  it('should maintain branch isolation after checkout back to main', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.commit('Main anchor');

      await instance.createBranch({ name: 'temp-work' });
      await instance.checkout('temp-work');
      await instance.commit('Temp data');

      // Go back to main
      await instance.checkout('main');

      const status = await instance.status();
      expect(status.branch).toBe('main');

      // Main head should be unchanged
      const mainLog = await instance.log('main', { limit: 10 });
      const mainMessages = mainLog.map(e => e.commit.message);
      expect(mainMessages).not.toContain('Temp data');
    });
  });

  it('should keep branch commits visible only in that branch log', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.commit('Baseline');

      await instance.createBranch({ name: 'secret-branch' });
      await instance.checkout('secret-branch');
      const secretCommit = await instance.commit('Secret data');

      // The branch log should contain the secret commit
      const branchLog = await instance.log('secret-branch', { limit: 10 });
      const branchIds = branchLog.map(e => e.commit.id);
      expect(branchIds).toContain(secretCommit);

      // But main's log should not list it
      await instance.checkout('main');
      const mainLog = await instance.log('main', { limit: 50 });
      const mainIds = mainLog.map(e => e.commit.id);
      expect(mainIds).not.toContain(secretCommit);
    });
  });

  it('should reflect correct divergence after independent branch work', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.commit('Fork point');

      await instance.createBranch({ name: 'diverge-test' });

      // Add 2 commits to main
      await instance.commit('Main advance 1');
      await instance.commit('Main advance 2');

      // Add 3 commits to branch
      await instance.checkout('diverge-test');
      await instance.commit('Branch advance 1');
      await instance.commit('Branch advance 2');
      await instance.commit('Branch advance 3');

      const comparison = await instance.compare('diverge-test', 'main');

      expect(comparison.diverged).toBe(true);
      expect(comparison.ahead).toBe(3);
      expect(comparison.behind).toBe(2);
    });
  });
});

// =============================================================================
// 5. Branch Metadata Management
// =============================================================================

describe('branch production validation - branch metadata management', () => {
  it('should store and retrieve description on branch creation', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      const branch = await instance.createBranch({
        name: 'described-branch',
        description: 'Implements user authentication with OAuth2',
      });

      expect(branch.description).toBe('Implements user authentication with OAuth2');

      // Retrieve it again to confirm persistence
      const retrieved = await instance.getBranch('described-branch');
      expect(retrieved).not.toBeNull();
      expect(retrieved!.description).toBe('Implements user authentication with OAuth2');
    });
  });

  it('should record accurate creation and update timestamps', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      const before = Date.now();
      const branch = await instance.createBranch({ name: 'timestamped' });
      const after = Date.now();

      expect(branch.createdAt).toBeGreaterThanOrEqual(before);
      expect(branch.createdAt).toBeLessThanOrEqual(after);
      expect(branch.updatedAt).toBeGreaterThanOrEqual(before);
      expect(branch.updatedAt).toBeLessThanOrEqual(after);
    });
  });

  it('should update the updatedAt timestamp when branch head changes', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.createBranch({ name: 'update-ts' });
      const branchBefore = await instance.getBranch('update-ts');
      const createdAt = branchBefore!.createdAt;

      await instance.checkout('update-ts');
      await instance.commit('Trigger update');

      const branchAfter = await instance.getBranch('update-ts');
      expect(branchAfter!.updatedAt).toBeGreaterThanOrEqual(createdAt);
      expect(branchAfter!.createdAt).toBe(createdAt); // createdAt should not change
    });
  });

  it('should mark main as a protected branch', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      const main = await instance.getBranch('main');
      expect(main).not.toBeNull();
      expect(main!.protected).toBe(true);

      // Cannot delete protected branch without force
      await expect(instance.deleteBranch('main')).rejects.toThrow();
    });
  });

  it('should track parent branch reference correctly', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.createBranch({ name: 'parent-ref', from: 'main' });
      const branch = await instance.getBranch('parent-ref');

      expect(branch!.parent).toBe('main');

      // Main's parent should be null (root)
      const main = await instance.getBranch('main');
      expect(main!.parent).toBeNull();
    });
  });

  it('should support renaming a branch and preserving metadata', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.createBranch({
        name: 'rename-src',
        description: 'Original description',
      });

      await instance.checkout('rename-src');
      const commitId = await instance.commit('Pre-rename work');
      await instance.checkout('main');

      await instance.renameBranch('rename-src', 'rename-dst');

      const oldBranch = await instance.getBranch('rename-src');
      const newBranch = await instance.getBranch('rename-dst');

      expect(oldBranch).toBeNull();
      expect(newBranch).not.toBeNull();
      expect(newBranch!.name).toBe('rename-dst');
      expect(newBranch!.description).toBe('Original description');
      expect(newBranch!.head).toBe(commitId);
    });
  });

  it('should update child branch parent references on rename', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.createBranch({ name: 'old-parent-name' });
      await instance.checkout('old-parent-name');
      await instance.commit('Parent state');

      await instance.createBranch({ name: 'child-branch', from: 'old-parent-name' });
      await instance.checkout('main');

      await instance.renameBranch('old-parent-name', 'new-parent-name');

      const child = await instance.getBranch('child-branch');
      expect(child!.parent).toBe('new-parent-name');
    });
  });

  it('should list all branches with correct metadata', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.createBranch({ name: 'list-a', description: 'Alpha' });
      await instance.createBranch({ name: 'list-b', description: 'Beta' });
      await instance.createBranch({ name: 'list-c', description: 'Gamma' });

      const branches = await instance.listBranches();

      expect(branches.length).toBeGreaterThanOrEqual(4); // main + 3

      const names = branches.map(b => b.name);
      expect(names).toContain('main');
      expect(names).toContain('list-a');
      expect(names).toContain('list-b');
      expect(names).toContain('list-c');

      // Verify they are sorted
      for (let i = 0; i < names.length - 1; i++) {
        expect(names[i]! <= names[i + 1]!).toBe(true);
      }

      const alpha = branches.find(b => b.name === 'list-a');
      expect(alpha!.description).toBe('Alpha');
    });
  });

  it('should filter branches with a glob-like pattern', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.createBranch({ name: 'feature/login' });
      await instance.createBranch({ name: 'feature/signup' });
      await instance.createBranch({ name: 'bugfix/crash' });

      const features = await instance.listBranches('feature/*');

      expect(features.length).toBe(2);
      expect(features.every(b => b.name.startsWith('feature/'))).toBe(true);
    });
  });

  it('should show correct branch status including head and merge state', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.createBranch({ name: 'status-check' });
      await instance.checkout('status-check');
      const commitId = await instance.commit('Status commit');

      const status = await instance.status();

      expect(status.branch).toBe('status-check');
      expect(status.head).toBe(commitId);
      expect(status.merging).toBe(false);
      expect(status.conflicts).toHaveLength(0);
    });
  });

  it('should handle branch with archived=false by default', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      const branch = await instance.createBranch({ name: 'not-archived' });

      expect(branch.archived).toBe(false);
    });
  });

  it('should properly manage baseCommit when creating from a branch HEAD', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      const headCommit = await instance.commit('Current HEAD');

      const branch = await instance.createBranch({ name: 'from-head' });

      expect(branch.baseCommit).toBe(headCommit);
      expect(branch.head).toBe(headCommit);
    });
  });
});

// =============================================================================
// Additional Production Edge Cases
// =============================================================================

describe('branch production validation - edge cases and robustness', () => {
  it('should handle creating a branch from an empty main (no commits)', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      // main exists but has no commits yet
      const branch = await instance.createBranch({ name: 'from-empty' });

      expect(branch.head).toBeNull();
      expect(branch.baseCommit).toBeNull();
    });
  });

  it('should prevent deleting the current branch', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.createBranch({ name: 'current-del' });
      await instance.checkout('current-del');

      await expect(
        instance.deleteBranch('current-del', { force: true })
      ).rejects.toThrow();
    });
  });

  it('should allow deletion of a merged branch without force flag', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.commit('Root');

      await instance.createBranch({ name: 'to-merge-then-delete' });
      await instance.checkout('to-merge-then-delete');
      await instance.commit('Mergeable work');

      await instance.checkout('main');
      await instance.merge('to-merge-then-delete', 'main');

      // Should succeed without force because it is fully merged
      await instance.deleteBranch('to-merge-then-delete');
      const result = await instance.getBranch('to-merge-then-delete');
      expect(result).toBeNull();
    });
  });

  it('should reject deletion of unmerged branch without force', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.commit('Base');

      await instance.createBranch({ name: 'unmerged-reject' });
      await instance.checkout('unmerged-reject');
      await instance.commit('Unmerged change');

      await instance.checkout('main');
      await instance.commit('Main moved on');

      await expect(
        instance.deleteBranch('unmerged-reject')
      ).rejects.toThrow();
    });
  });

  it('should verify merge base is the correct common ancestor', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      const ancestor = await instance.commit('Common point');

      await instance.createBranch({ name: 'left-fork' });
      await instance.createBranch({ name: 'right-fork' });

      await instance.checkout('left-fork');
      await instance.commit('Left 1');
      await instance.commit('Left 2');

      await instance.checkout('right-fork');
      await instance.commit('Right 1');

      const base = await instance.findMergeBase('left-fork', 'right-fork');
      expect(base).toBe(ancestor);
    });
  });

  it('should handle rapid sequential commits on the same branch', async () => {
    const stub = getStub();
    await runInDurableObject(stub, async (instance: TestBranchDO) => {
      await instance.createBranch({ name: 'rapid-commits' });
      await instance.checkout('rapid-commits');

      const commitIds: string[] = [];
      for (let i = 0; i < 20; i++) {
        const id = await instance.commit(`Rapid commit ${i}`);
        commitIds.push(id);
      }

      // All should be unique
      expect(new Set(commitIds).size).toBe(20);

      // Log should return them in reverse order
      const log = await instance.log('rapid-commits', { limit: 20 });
      expect(log[0]!.commit.message).toBe('Rapid commit 19');
      expect(log[19]!.commit.message).toBe('Rapid commit 0');
    });
  });
});
