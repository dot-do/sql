/**
 * DoLake Compaction Coordination Tests
 *
 * Tests for write coordination with compaction operations.
 * Uses workers-vitest-pool (NO MOCKS).
 *
 * Issue: sql-8euu - DoLake compaction lacks coordination with active writes
 *
 * Test scenarios:
 * - Compaction blocked during active writes
 * - Writes wait for compaction to complete critical sections
 * - Concurrent compaction prevention
 * - Recovery from interrupted compaction
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import {
  CompactionManager,
  CompactionCoordinator,
  type CompactionConfig,
  type CompactionCoordinationConfig,
  type LockAcquisitionResult,
  type CompactionLease,
  type CompactionCheckpoint,
  type CompactionPhase,
  type ConflictCheckResult,
  type DataFile,
  DEFAULT_COMPACTION_CONFIG,
  DEFAULT_COORDINATION_CONFIG,
  generateUUID,
} from '../index.js';

// =============================================================================
// Test Utilities
// =============================================================================

function createTestDataFile(overrides: Partial<DataFile> = {}): DataFile {
  return {
    content: 0,
    'file-path': `/warehouse/db/table/data/${generateUUID()}.parquet`,
    'file-format': 'parquet',
    partition: {},
    'record-count': BigInt(100),
    'file-size-in-bytes': BigInt(1024),
    ...overrides,
  };
}

function createSmallFile(sizeBytes: number = 1024): DataFile {
  return createTestDataFile({
    'file-size-in-bytes': BigInt(sizeBytes),
    'record-count': BigInt(Math.floor(sizeBytes / 10)),
  });
}

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// =============================================================================
// CompactionCoordinator Tests
// =============================================================================

describe('CompactionCoordinator', () => {
  let coordinator: CompactionCoordinator;

  beforeEach(() => {
    coordinator = new CompactionCoordinator();
  });

  describe('Worker ID', () => {
    it('should generate a unique worker ID', () => {
      const workerId = coordinator.getWorkerId();
      expect(workerId).toBeDefined();
      expect(typeof workerId).toBe('string');
      expect(workerId.length).toBeGreaterThan(0);
    });

    it('should have different worker IDs for different coordinators', () => {
      const coordinator2 = new CompactionCoordinator();
      expect(coordinator.getWorkerId()).not.toBe(coordinator2.getWorkerId());
    });
  });

  describe('Shared Lock (Write Operations)', () => {
    const targetKey = 'test.table';

    it('should acquire a shared lock for write operations', () => {
      const writeId = 'write-1';
      const result = coordinator.acquireSharedLock(targetKey, writeId);

      expect(result.acquired).toBe(true);
      expect(result.lockId).toBe(writeId);
      expect(result.reason).toBeNull();
    });

    it('should allow multiple shared locks on the same target', () => {
      const result1 = coordinator.acquireSharedLock(targetKey, 'write-1');
      const result2 = coordinator.acquireSharedLock(targetKey, 'write-2');
      const result3 = coordinator.acquireSharedLock(targetKey, 'write-3');

      expect(result1.acquired).toBe(true);
      expect(result2.acquired).toBe(true);
      expect(result3.acquired).toBe(true);
      expect(coordinator.getActiveWriteCount(targetKey)).toBe(3);
    });

    it('should block shared lock when exclusive lock is held', async () => {
      // First acquire exclusive lock
      const exclusiveResult = await coordinator.acquireExclusiveLock(targetKey);
      expect(exclusiveResult.acquired).toBe(true);

      // Try to acquire shared lock
      const sharedResult = coordinator.acquireSharedLock(targetKey, 'write-1');
      expect(sharedResult.acquired).toBe(false);
      expect(sharedResult.reason).toBe('compaction_in_progress');
      expect(sharedResult.retryAfterMs).toBeGreaterThan(0);
    });

    it('should release shared lock correctly', () => {
      const writeId = 'write-1';
      coordinator.acquireSharedLock(targetKey, writeId);
      expect(coordinator.hasActiveWrites(targetKey)).toBe(true);

      coordinator.releaseLock(targetKey, writeId);
      expect(coordinator.hasActiveWrites(targetKey)).toBe(false);
    });
  });

  describe('Exclusive Lock (Compaction)', () => {
    const targetKey = 'test.table';

    it('should acquire an exclusive lock for compaction', async () => {
      const result = await coordinator.acquireExclusiveLock(targetKey);

      expect(result.acquired).toBe(true);
      expect(result.lockId).not.toBeNull();
      expect(coordinator.hasExclusiveLock(targetKey)).toBe(true);
    });

    it('should block exclusive lock when another exclusive lock is held', async () => {
      const result1 = await coordinator.acquireExclusiveLock(targetKey);
      expect(result1.acquired).toBe(true);

      const result2 = await coordinator.acquireExclusiveLock(targetKey);
      expect(result2.acquired).toBe(false);
      expect(result2.reason).toBe('exclusive_lock_held');
      expect(result2.currentHolder).toBe(result1.lockId);
    });

    it('should wait for shared locks before acquiring exclusive lock', async () => {
      // Acquire some shared locks
      coordinator.acquireSharedLock(targetKey, 'write-1');
      coordinator.acquireSharedLock(targetKey, 'write-2');

      // Start exclusive lock acquisition in background
      const exclusivePromise = coordinator.acquireExclusiveLock(targetKey, 500);

      // Release shared locks after a short delay
      setTimeout(() => {
        coordinator.releaseLock(targetKey, 'write-1');
        coordinator.releaseLock(targetKey, 'write-2');
      }, 100);

      const result = await exclusivePromise;
      expect(result.acquired).toBe(true);
    });

    it('should timeout if shared locks are not released', async () => {
      // Acquire shared locks
      coordinator.acquireSharedLock(targetKey, 'write-1');
      coordinator.acquireSharedLock(targetKey, 'write-2');

      // Try to acquire exclusive lock with short timeout
      const result = await coordinator.acquireExclusiveLock(targetKey, 200);
      expect(result.acquired).toBe(false);
      expect(result.reason).toBe('write_wait_timeout');
    });

    it('should release exclusive lock correctly', async () => {
      const result = await coordinator.acquireExclusiveLock(targetKey);
      expect(result.acquired).toBe(true);
      expect(coordinator.hasExclusiveLock(targetKey)).toBe(true);

      coordinator.releaseLock(targetKey, result.lockId!);
      expect(coordinator.hasExclusiveLock(targetKey)).toBe(false);
    });
  });

  describe('Lease Management', () => {
    const targetKey = 'test.table';

    it('should acquire a lease for compaction', async () => {
      const lease = await coordinator.acquireLease(targetKey);

      expect(lease).not.toBeNull();
      expect(lease!.targetKey).toBe(targetKey);
      expect(lease!.workerId).toBe(coordinator.getWorkerId());
      expect(lease!.expiresAt).toBeGreaterThan(Date.now());
      expect(lease!.renewCount).toBe(0);
    });

    it('should not acquire a lease when another lease is active', async () => {
      const lease1 = await coordinator.acquireLease(targetKey);
      expect(lease1).not.toBeNull();

      const lease2 = await coordinator.acquireLease(targetKey);
      expect(lease2).toBeNull();
    });

    it('should renew a lease successfully', async () => {
      const lease = await coordinator.acquireLease(targetKey);
      expect(lease).not.toBeNull();

      const originalExpiry = lease!.expiresAt;
      await delay(10);

      const renewed = coordinator.renewLease(lease!.leaseId);
      expect(renewed).toBe(true);

      const updatedLease = coordinator.getLease(targetKey);
      expect(updatedLease!.expiresAt).toBeGreaterThan(originalExpiry);
      expect(updatedLease!.renewCount).toBe(1);
    });

    it('should check lease validity', async () => {
      const lease = await coordinator.acquireLease(targetKey);
      expect(lease).not.toBeNull();

      expect(coordinator.isLeaseValid(lease!.leaseId)).toBe(true);
      expect(coordinator.isLeaseValid('invalid-lease-id')).toBe(false);
    });

    it('should release a lease', async () => {
      const lease = await coordinator.acquireLease(targetKey);
      expect(lease).not.toBeNull();

      coordinator.releaseLease(lease!.leaseId);

      expect(coordinator.getLease(targetKey)).toBeNull();
      expect(coordinator.hasExclusiveLock(targetKey)).toBe(false);
    });
  });

  describe('Checkpoint Management', () => {
    const targetKey = 'test.table';
    const files = ['file1.parquet', 'file2.parquet', 'file3.parquet'];
    const sequenceNumber = BigInt(100);

    it('should create a checkpoint', () => {
      const checkpoint = coordinator.createCheckpoint(
        targetKey,
        'lease-1',
        files,
        sequenceNumber
      );

      expect(checkpoint.targetKey).toBe(targetKey);
      expect(checkpoint.phase).toBe('initializing');
      expect(checkpoint.processedFiles).toHaveLength(0);
      expect(checkpoint.remainingFiles).toHaveLength(3);
      expect(checkpoint.startSequenceNumber).toBe(sequenceNumber);
    });

    it('should update checkpoint progress', () => {
      const checkpoint = coordinator.createCheckpoint(
        targetKey,
        'lease-1',
        files,
        sequenceNumber
      );

      const updated = coordinator.updateCheckpoint(checkpoint.checkpointId, {
        phase: 'reading_files',
        processedFiles: ['file1.parquet'],
        remainingFiles: ['file2.parquet', 'file3.parquet'],
      });

      expect(updated).toBe(true);

      const retrieved = coordinator.getCheckpoint(checkpoint.checkpointId);
      expect(retrieved!.phase).toBe('reading_files');
      expect(retrieved!.processedFiles).toContain('file1.parquet');
      expect(retrieved!.remainingFiles).toHaveLength(2);
    });

    it('should get checkpoint by target key', () => {
      coordinator.createCheckpoint(targetKey, 'lease-1', files, sequenceNumber);

      const checkpoint = coordinator.getCheckpointByTarget(targetKey);
      expect(checkpoint).not.toBeNull();
      expect(checkpoint!.targetKey).toBe(targetKey);
    });

    it('should complete a checkpoint', () => {
      const checkpoint = coordinator.createCheckpoint(
        targetKey,
        'lease-1',
        files,
        sequenceNumber
      );

      coordinator.completeCheckpoint(checkpoint.checkpointId);

      const retrieved = coordinator.getCheckpoint(checkpoint.checkpointId);
      expect(retrieved!.phase).toBe('completed');
    });

    it('should fail a checkpoint', () => {
      const checkpoint = coordinator.createCheckpoint(
        targetKey,
        'lease-1',
        files,
        sequenceNumber
      );

      coordinator.failCheckpoint(checkpoint.checkpointId, 'Test failure');

      const retrieved = coordinator.getCheckpoint(checkpoint.checkpointId);
      expect(retrieved!.phase).toBe('failed');
      expect(retrieved!.intermediateData.failureReason).toBe('Test failure');
    });

    it('should get active checkpoints', () => {
      coordinator.createCheckpoint('table1', 'lease-1', files, sequenceNumber);
      coordinator.createCheckpoint('table2', 'lease-2', files, sequenceNumber);
      const completed = coordinator.createCheckpoint('table3', 'lease-3', files, sequenceNumber);
      coordinator.completeCheckpoint(completed.checkpointId);

      const active = coordinator.getActiveCheckpoints();
      expect(active.length).toBe(2);
    });
  });

  describe('Conflict Detection', () => {
    const targetKey = 'test.table';

    it('should detect no conflict when nothing changed', () => {
      const result = coordinator.checkForConflicts(
        targetKey,
        BigInt(100),
        BigInt(100),
        ['file1.parquet', 'file2.parquet'],
        ['file1.parquet', 'file2.parquet']
      );

      expect(result.hasConflict).toBe(false);
      expect(result.conflictType).toBeNull();
    });

    it('should detect conflict when new data arrives', () => {
      const result = coordinator.checkForConflicts(
        targetKey,
        BigInt(100),
        BigInt(101),
        ['file1.parquet', 'file2.parquet'],
        ['file1.parquet', 'file2.parquet', 'file3.parquet']
      );

      expect(result.hasConflict).toBe(true);
      expect(result.conflictType).toBe('new_data');
      expect(result.conflictingFiles).toContain('file3.parquet');
    });

    it('should detect conflict when files are removed by concurrent compaction', () => {
      const result = coordinator.checkForConflicts(
        targetKey,
        BigInt(100),
        BigInt(100),
        ['file1.parquet', 'file2.parquet', 'file3.parquet'],
        ['file1.parquet', 'file2.parquet']
      );

      expect(result.hasConflict).toBe(true);
      expect(result.conflictType).toBe('concurrent_compaction');
      expect(result.conflictingFiles).toContain('file3.parquet');
    });

    it('should resolve new_data conflict with merge for small additions', () => {
      const conflict: ConflictCheckResult = {
        hasConflict: true,
        conflictType: 'new_data',
        details: 'New files added',
        conflictingFiles: ['file1.parquet', 'file2.parquet'],
        conflictSequenceNumber: BigInt(101),
      };

      const resolution = coordinator.resolveConflict(conflict);
      expect(resolution).toBe('merge');
    });

    it('should resolve new_data conflict with retry for large additions', () => {
      const conflict: ConflictCheckResult = {
        hasConflict: true,
        conflictType: 'new_data',
        details: 'Many new files added',
        conflictingFiles: Array.from({ length: 15 }, (_, i) => `file${i}.parquet`),
        conflictSequenceNumber: BigInt(101),
      };

      const resolution = coordinator.resolveConflict(conflict);
      expect(resolution).toBe('retry');
    });

    it('should resolve concurrent_compaction conflict with abort', () => {
      const conflict: ConflictCheckResult = {
        hasConflict: true,
        conflictType: 'concurrent_compaction',
        details: 'Files removed',
        conflictingFiles: ['file1.parquet'],
        conflictSequenceNumber: BigInt(101),
      };

      const resolution = coordinator.resolveConflict(conflict);
      expect(resolution).toBe('abort');
    });
  });

  describe('Statistics and Cleanup', () => {
    it('should return correct statistics', async () => {
      const targetKey1 = 'table1';
      const targetKey2 = 'table2';

      // Acquire some locks and leases
      coordinator.acquireSharedLock(targetKey1, 'write-1');
      coordinator.acquireSharedLock(targetKey1, 'write-2');
      await coordinator.acquireLease(targetKey2);
      coordinator.createCheckpoint(targetKey2, 'lease-1', ['file.parquet'], BigInt(100));

      const stats = coordinator.getStats();
      expect(stats.sharedLocks).toBe(2);
      expect(stats.exclusiveLocks).toBe(1);
      expect(stats.activeLeases).toBe(1);
      expect(stats.activeCheckpoints).toBe(1);
    });

    it('should clean up expired locks and leases', async () => {
      const shortConfig: Partial<CompactionCoordinationConfig> = {
        leaseDurationMs: 50,
      };
      const shortCoordinator = new CompactionCoordinator(shortConfig);
      const targetKey = 'test.table';

      // Acquire a lease with short duration
      const lease = await shortCoordinator.acquireLease(targetKey);
      expect(lease).not.toBeNull();

      // Wait for expiration
      await delay(100);

      // Clean up
      shortCoordinator.cleanupExpired();

      // Lease should be gone
      expect(shortCoordinator.getLease(targetKey)).toBeNull();
      expect(shortCoordinator.hasExclusiveLock(targetKey)).toBe(false);
    });
  });
});

// =============================================================================
// CompactionManager Coordination Tests
// =============================================================================

describe('CompactionManager Write Coordination', () => {
  let manager: CompactionManager;

  beforeEach(() => {
    manager = new CompactionManager();
  });

  describe('Write Operations', () => {
    const targetKey = 'test.table';

    it('should start a write operation', () => {
      const result = manager.startWrite(targetKey);

      expect(result.acquired).toBe(true);
      expect(result.lockId).not.toBeNull();
    });

    it('should complete a write operation', () => {
      const result = manager.startWrite(targetKey);
      expect(result.acquired).toBe(true);

      manager.completeWrite(targetKey, result.lockId!);
      expect(manager.getActiveWriteCount(targetKey)).toBe(0);
    });

    it('should block writes during compaction', async () => {
      const files = [createSmallFile(1024), createSmallFile(2048)];

      // Start compaction
      const session = await manager.startCompaction(targetKey, files, BigInt(100));
      expect(session).not.toBeNull();

      // Try to start a write
      const writeResult = manager.startWrite(targetKey);
      expect(writeResult.acquired).toBe(false);
      expect(manager.isWriteBlocked(targetKey)).toBe(true);

      // Complete compaction
      manager.completeCompaction(session!, {
        success: true,
        filesCompacted: 2,
        bytesCompacted: BigInt(3072),
        outputFiles: 1,
        outputBytes: BigInt(3000),
        durationMs: 100,
      });

      // Write should now be allowed
      const writeResult2 = manager.startWrite(targetKey);
      expect(writeResult2.acquired).toBe(true);
    });

    it('should track active write count', () => {
      const result1 = manager.startWrite(targetKey);
      const result2 = manager.startWrite(targetKey);
      const result3 = manager.startWrite(targetKey);

      expect(manager.getActiveWriteCount(targetKey)).toBe(3);

      manager.completeWrite(targetKey, result1.lockId!);
      expect(manager.getActiveWriteCount(targetKey)).toBe(2);

      manager.completeWrite(targetKey, result2.lockId!);
      manager.completeWrite(targetKey, result3.lockId!);
      expect(manager.getActiveWriteCount(targetKey)).toBe(0);
    });
  });

  describe('Coordinated Compaction', () => {
    const targetKey = 'test.table';
    const files = [createSmallFile(1024), createSmallFile(2048), createSmallFile(1536)];

    it('should start a coordinated compaction', async () => {
      const session = await manager.startCompaction(targetKey, files, BigInt(100));

      expect(session).not.toBeNull();
      expect(session!.lease).toBeDefined();
      expect(session!.checkpoint).toBeDefined();
      expect(session!.checkpoint.phase).toBe('initializing');
    });

    it('should block compaction during active writes', async () => {
      // Start some writes
      const write1 = manager.startWrite(targetKey);
      const write2 = manager.startWrite(targetKey);
      expect(write1.acquired).toBe(true);
      expect(write2.acquired).toBe(true);

      // Check if compaction is blocked
      expect(manager.isCompactionBlocked(targetKey)).toBe(true);
    });

    it('should prevent concurrent compaction on same target', async () => {
      const session1 = await manager.startCompaction(targetKey, files, BigInt(100));
      expect(session1).not.toBeNull();

      const session2 = await manager.startCompaction(targetKey, files, BigInt(100));
      expect(session2).toBeNull();

      // Clean up
      manager.abortCompaction(session1!, 'Test cleanup');
    });

    it('should allow compaction on different targets', async () => {
      const session1 = await manager.startCompaction('table1', files, BigInt(100));
      const session2 = await manager.startCompaction('table2', files, BigInt(100));

      expect(session1).not.toBeNull();
      expect(session2).not.toBeNull();

      // Clean up
      manager.abortCompaction(session1!, 'Test cleanup');
      manager.abortCompaction(session2!, 'Test cleanup');
    });

    it('should update compaction progress', async () => {
      const session = await manager.startCompaction(targetKey, files, BigInt(100));
      expect(session).not.toBeNull();

      manager.updateCompactionProgress(session!, 'reading_files', [files[0]['file-path']]);

      expect(session!.checkpoint.phase).toBe('reading_files');
      expect(session!.checkpoint.processedFiles).toHaveLength(1);
      expect(session!.checkpoint.remainingFiles).toHaveLength(2);

      manager.abortCompaction(session!, 'Test cleanup');
    });

    it('should complete compaction successfully', async () => {
      const session = await manager.startCompaction(targetKey, files, BigInt(100));
      expect(session).not.toBeNull();

      manager.completeCompaction(session!, {
        success: true,
        filesCompacted: 3,
        bytesCompacted: BigInt(4608),
        outputFiles: 1,
        outputBytes: BigInt(4500),
        durationMs: 150,
      });

      const metrics = manager.getMetrics();
      expect(metrics.successfulCompactions).toBeGreaterThan(0);
      expect(manager.isWriteBlocked(targetKey)).toBe(false);
    });

    it('should abort compaction and record failure', async () => {
      const session = await manager.startCompaction(targetKey, files, BigInt(100));
      expect(session).not.toBeNull();

      manager.abortCompaction(session!, 'Test abort reason');

      const metrics = manager.getMetrics();
      expect(metrics.failedCompactions).toBeGreaterThan(0);
      expect(manager.isWriteBlocked(targetKey)).toBe(false);
    });

    it('should renew compaction lease', async () => {
      const session = await manager.startCompaction(targetKey, files, BigInt(100));
      expect(session).not.toBeNull();

      const renewed = manager.renewCompactionLease(session!);
      expect(renewed).toBe(true);

      manager.abortCompaction(session!, 'Test cleanup');
    });
  });

  describe('Conflict Detection', () => {
    const targetKey = 'test.table';
    const files = [createSmallFile(1024), createSmallFile(2048)];

    it('should detect conflicts during compaction', async () => {
      const session = await manager.startCompaction(targetKey, files, BigInt(100));
      expect(session).not.toBeNull();

      // Simulate new files being added
      const currentFiles = [
        files[0]['file-path'],
        files[1]['file-path'],
        '/new/file.parquet',
      ];

      const conflict = manager.checkCompactionConflicts(
        session!,
        BigInt(101),
        currentFiles
      );

      expect(conflict.hasConflict).toBe(true);
      expect(conflict.conflictType).toBe('new_data');

      manager.abortCompaction(session!, 'Test cleanup');
    });
  });

  describe('Recovery from Interrupted Compaction', () => {
    const targetKey = 'test.table';
    const files = [createSmallFile(1024), createSmallFile(2048)];

    it('should resume compaction from checkpoint', async () => {
      // Start compaction and make some progress
      const session1 = await manager.startCompaction(targetKey, files, BigInt(100));
      expect(session1).not.toBeNull();

      manager.updateCompactionProgress(session1!, 'reading_files', [files[0]['file-path']]);

      // Simulate interruption by releasing the lease
      manager.getCoordinator().releaseLease(session1!.lease.leaseId);

      // Resume compaction
      const session2 = await manager.resumeCompaction(targetKey);
      expect(session2).not.toBeNull();
      expect(session2!.checkpoint.processedFiles).toHaveLength(1);
      expect(session2!.checkpoint.phase).toBe('reading_files');

      manager.abortCompaction(session2!, 'Test cleanup');
    });

    it('should return null when no checkpoint exists', async () => {
      const session = await manager.resumeCompaction('nonexistent.table');
      expect(session).toBeNull();
    });
  });

  describe('Coordination Statistics', () => {
    it('should return coordination statistics', async () => {
      const targetKey = 'test.table';
      const files = [createSmallFile(1024)];

      // Start some writes
      manager.startWrite(targetKey);
      manager.startWrite(targetKey);

      // Start compaction on different target
      const session = await manager.startCompaction('other.table', files, BigInt(100));

      const stats = manager.getCoordinationStats();
      expect(stats.sharedLocks).toBe(2);
      expect(stats.exclusiveLocks).toBe(1);
      expect(stats.activeLeases).toBe(1);

      if (session) {
        manager.abortCompaction(session, 'Test cleanup');
      }
    });

    it('should clean up expired coordination state', async () => {
      const shortManager = new CompactionManager({}, {
        leaseDurationMs: 50,
      });

      const files = [createSmallFile(1024)];
      const session = await shortManager.startCompaction('test.table', files, BigInt(100));
      expect(session).not.toBeNull();

      // Wait for expiration
      await delay(100);

      // Clean up
      shortManager.cleanupExpiredCoordination();

      // New compaction should now be possible
      const session2 = await shortManager.startCompaction('test.table', files, BigInt(101));
      expect(session2).not.toBeNull();

      if (session2) {
        shortManager.abortCompaction(session2, 'Test cleanup');
      }
    });
  });
});

// =============================================================================
// Integration Tests with DoLake
// =============================================================================

describe('DoLake Compaction Coordination Integration', () => {
  it('should coordinate writes and compaction via DoLake API', async () => {
    const id = env.DOLAKE.idFromName('test-coordination-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Check compaction status
    const statusResponse = await stub.fetch('http://dolake/v1/compaction/status');
    expect([200, 404]).toContain(statusResponse.status);
  });

  it('should report coordination statistics via API', async () => {
    const id = env.DOLAKE.idFromName('test-coordination-stats-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const metricsResponse = await stub.fetch('http://dolake/v1/compaction/metrics');
    expect(metricsResponse.status).toBe(200);

    const metrics = await metricsResponse.json() as Record<string, unknown>;
    expect(metrics.totalCompactions).toBeDefined();
  });

  it('should handle concurrent requests properly', async () => {
    const id = env.DOLAKE.idFromName('test-coordination-concurrent-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Create namespace
    await stub.fetch('http://dolake/v1/namespaces', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        namespace: ['coordination_test'],
        properties: {},
      }),
    });

    // Send multiple concurrent requests
    const requests = Array.from({ length: 5 }, (_, i) =>
      stub.fetch('http://dolake/v1/compaction/run', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          namespace: ['coordination_test'],
          tableName: 'test_table',
          dryRun: true,
        }),
      })
    );

    const responses = await Promise.all(requests);

    // All requests should complete (either success or proper rejection)
    responses.forEach(response => {
      expect([200, 404, 409, 429]).toContain(response.status);
    });
  });
});

// =============================================================================
// Edge Cases
// =============================================================================

describe('Edge Cases', () => {
  it('should handle rapid lock/unlock cycles', () => {
    const coordinator = new CompactionCoordinator();
    const targetKey = 'test.table';

    for (let i = 0; i < 100; i++) {
      const result = coordinator.acquireSharedLock(targetKey, `write-${i}`);
      expect(result.acquired).toBe(true);
      coordinator.releaseLock(targetKey, `write-${i}`);
    }

    expect(coordinator.hasActiveWrites(targetKey)).toBe(false);
  });

  it('should handle simultaneous operations on many targets', async () => {
    const coordinator = new CompactionCoordinator();
    const targets = Array.from({ length: 50 }, (_, i) => `table-${i}`);

    // Acquire locks on all targets
    for (const target of targets) {
      coordinator.acquireSharedLock(target, 'write-1');
    }

    const stats = coordinator.getStats();
    expect(stats.sharedLocks).toBe(50);

    // Release all
    for (const target of targets) {
      coordinator.releaseLock(target, 'write-1');
    }

    const finalStats = coordinator.getStats();
    expect(finalStats.sharedLocks).toBe(0);
  });

  it('should handle checkpoint with no remaining files', () => {
    const coordinator = new CompactionCoordinator();
    const checkpoint = coordinator.createCheckpoint(
      'test.table',
      'lease-1',
      [],
      BigInt(100)
    );

    expect(checkpoint.remainingFiles).toHaveLength(0);
    expect(checkpoint.phase).toBe('initializing');
  });
});
