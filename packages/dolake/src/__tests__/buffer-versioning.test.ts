/**
 * BufferSnapshot Schema Versioning Tests (TDD GREEN Phase)
 *
 * Tests for BufferSnapshot versioning, migration, and forward/backward compatibility.
 * Uses workers-vitest-pool (NO MOCKS).
 *
 * Issue: sql-csd - BufferSnapshot Schema Versioning
 *
 * These tests verify the schema versioning implementation for BufferSnapshot:
 * - Version field in BufferSnapshot interface
 * - Version validation during deserialization
 * - Forward compatibility (newer reader with older data)
 * - Migration path from unversioned to versioned format
 * - Version mismatch error handling
 */

import { describe, it, expect } from 'vitest';
import {
  CDCBufferManager,
  type BufferSnapshot,
  type CDCEvent,
  generateUUID,
  CURRENT_SCHEMA_VERSION,
  MIN_SUPPORTED_VERSION,
  SCHEMA_VERSION_HISTORY,
  BREAKING_CHANGES,
  VersionMismatchError,
} from '../index.js';

// =============================================================================
// Test Utilities
// =============================================================================

function createTestCDCEvent(overrides: Partial<CDCEvent> = {}): CDCEvent {
  return {
    sequence: Date.now(),
    timestamp: Date.now(),
    operation: 'INSERT',
    table: 'test_table',
    rowId: generateUUID(),
    after: { id: generateUUID(), value: Math.random() },
    ...overrides,
  };
}

/**
 * Expected version constants
 */
const EXPECTED_SCHEMA_VERSION = 1;

/**
 * Create a mock unversioned snapshot (legacy format without version)
 */
function createUnversionedSnapshot(): Omit<BufferSnapshot, 'version'> {
  return {
    batches: [],
    sourceStates: [],
    partitionBuffers: [],
    dedupEntries: [],
    stats: {
      totalEventsReceived: 0,
      totalBatchesReceived: 0,
      lastFlushTime: 0,
    },
  };
}

/**
 * Create a mock versioned snapshot
 */
function createVersionedSnapshot(version: number): BufferSnapshot {
  return {
    version,
    batches: [],
    sourceStates: [],
    partitionBuffers: [],
    dedupEntries: [],
    stats: {
      totalEventsReceived: 0,
      totalBatchesReceived: 0,
      lastFlushTime: 0,
    },
  };
}

// =============================================================================
// 1. BufferSnapshot Version Field Tests
// =============================================================================

describe('BufferSnapshot Version Field', () => {
  it('should include version field in BufferSnapshot type', () => {
    const manager = new CDCBufferManager();

    // Add some test data
    manager.addBatch('source-1', [createTestCDCEvent()], 1);

    const snapshot = manager.serialize();

    // Check that version field exists on the snapshot
    expect(snapshot.version).toBeDefined();
  });

  it('should set version to current schema version on serialize', () => {
    const manager = new CDCBufferManager();
    manager.addBatch('source-1', [createTestCDCEvent()], 1);

    const snapshot = manager.serialize();

    // Version should be set to the current schema version
    expect(snapshot.version).toBe(EXPECTED_SCHEMA_VERSION);
  });

  it('should include version as first field in serialized JSON', () => {
    const manager = new CDCBufferManager();
    manager.addBatch('source-1', [createTestCDCEvent()], 1);

    const snapshot = manager.serialize();
    const json = JSON.stringify(snapshot);
    const parsed = JSON.parse(json);

    // Version should be present and be the first key for easy identification
    const keys = Object.keys(parsed);
    expect(keys[0]).toBe('version');
    expect(parsed.version).toBe(EXPECTED_SCHEMA_VERSION);
  });

  it('should expose CURRENT_SCHEMA_VERSION constant', () => {
    // Manager should have a static property for schema version
    expect(CDCBufferManager.SCHEMA_VERSION).toBeDefined();
    expect(CDCBufferManager.SCHEMA_VERSION).toBe(EXPECTED_SCHEMA_VERSION);
  });

  it('should export CURRENT_SCHEMA_VERSION from module', () => {
    expect(CURRENT_SCHEMA_VERSION).toBeDefined();
    expect(CURRENT_SCHEMA_VERSION).toBe(EXPECTED_SCHEMA_VERSION);
  });
});

// =============================================================================
// 2. Serialization with Version Tests
// =============================================================================

describe('Serialization with Version', () => {
  it('should serialize snapshot with version in JSON output', () => {
    const manager = new CDCBufferManager();
    manager.addBatch('source-1', [createTestCDCEvent()], 1);
    manager.addBatch('source-1', [createTestCDCEvent()], 2);

    const snapshot = manager.serialize();
    const jsonString = JSON.stringify(snapshot);

    // JSON should contain version field
    expect(jsonString).toContain('"version":');
    expect(jsonString).toContain(`"version":${EXPECTED_SCHEMA_VERSION}`);
  });

  it('should maintain version through JSON round-trip', () => {
    const manager = new CDCBufferManager();
    manager.addBatch('source-1', [createTestCDCEvent()], 1);

    const snapshot = manager.serialize();
    const jsonString = JSON.stringify(snapshot);
    const parsed = JSON.parse(jsonString) as BufferSnapshot;

    expect(parsed.version).toBe(EXPECTED_SCHEMA_VERSION);
  });

  it('should include version in binary serialization if supported', () => {
    const manager = new CDCBufferManager();
    manager.addBatch('source-1', [createTestCDCEvent()], 1);

    const snapshot = manager.serialize();

    // Verify JSON has version
    const jsonString = JSON.stringify(snapshot);
    const parsed = JSON.parse(jsonString) as BufferSnapshot;

    expect(parsed.version).toBe(EXPECTED_SCHEMA_VERSION);
  });

  it('should serialize version field as integer type', () => {
    const manager = new CDCBufferManager();
    manager.addBatch('source-1', [createTestCDCEvent()], 1);

    const snapshot = manager.serialize();

    // Version should be a number, not a string
    expect(typeof snapshot.version).toBe('number');
    expect(Number.isInteger(snapshot.version)).toBe(true);
  });
});

// =============================================================================
// 3. Deserialization with Version Validation Tests
// =============================================================================

describe('Deserialization with Version Validation', () => {
  it('should validate version on restore and confirm version handling', () => {
    const validSnapshot = createVersionedSnapshot(EXPECTED_SCHEMA_VERSION);

    // Restore should work
    const manager = CDCBufferManager.restore(validSnapshot);
    expect(manager).toBeDefined();

    // The restored snapshot should preserve the version
    const reserialized = manager.serialize();
    expect(reserialized.version).toBe(EXPECTED_SCHEMA_VERSION);
  });

  it('should reject snapshots with version higher than supported', () => {
    const futureSnapshot = createVersionedSnapshot(999); // Future version

    // Should throw error for unsupported future version
    expect(() => {
      CDCBufferManager.restore(futureSnapshot);
    }).toThrow(/unsupported.*version|version.*not supported|incompatible.*version/i);
  });

  it('should provide clear error message for incompatible version', () => {
    const futureSnapshot = createVersionedSnapshot(999);

    try {
      CDCBufferManager.restore(futureSnapshot);
      // Should not reach here
      expect(true).toBe(false);
    } catch (error) {
      // Error message should be clear and actionable
      expect(error).toBeInstanceOf(Error);
      const errorMessage = (error as Error).message;
      expect(errorMessage).toMatch(/version/i);
      expect(errorMessage).toContain('999'); // Should mention the problematic version
      expect(errorMessage).toContain(String(EXPECTED_SCHEMA_VERSION)); // Should mention supported version
    }
  });

  it('should handle missing version field as v0 (unversioned)', () => {
    const unversionedSnapshot = createUnversionedSnapshot();

    // Should treat missing version as version 0 and attempt migration
    const manager = CDCBufferManager.restore(unversionedSnapshot);
    expect(manager).toBeDefined();

    // Serializing should produce versioned output
    const newSnapshot = manager.serialize();
    expect(newSnapshot.version).toBe(EXPECTED_SCHEMA_VERSION);
  });

  it('should validate version is a positive integer', () => {
    const invalidVersionSnapshots = [
      { ...createVersionedSnapshot(1), version: -1 },
      { ...createVersionedSnapshot(1), version: 1.5 },
      { ...createVersionedSnapshot(1), version: '1' as unknown as number },
      { ...createVersionedSnapshot(1), version: null as unknown as number },
    ];

    for (const invalidSnapshot of invalidVersionSnapshots) {
      expect(() => {
        CDCBufferManager.restore(invalidSnapshot as BufferSnapshot);
      }).toThrow(/invalid.*version|version.*invalid/i);
    }
  });
});

// =============================================================================
// 4. Forward Compatibility Tests (Newer Reader, Older Format)
// =============================================================================

describe('Forward Compatibility - Newer Reader with Older Format', () => {
  it('should read v0 (unversioned) snapshots and output versioned format', () => {
    const v0Snapshot = createUnversionedSnapshot();
    v0Snapshot.stats.totalEventsReceived = 42;

    // Current reader should be able to read old unversioned format
    const manager = CDCBufferManager.restore(v0Snapshot);
    expect(manager).toBeDefined();

    // After restore, the serialized output should have a version field
    const reserialized = manager.serialize();
    expect(reserialized.version).toBe(EXPECTED_SCHEMA_VERSION);
    expect(reserialized.stats.totalEventsReceived).toBe(42);
  });

  it('should read v1 snapshots and preserve version metadata', () => {
    const v1Snapshot = createVersionedSnapshot(1);
    v1Snapshot.batches = [];
    v1Snapshot.stats.totalEventsReceived = 100;

    const manager = CDCBufferManager.restore(v1Snapshot);
    expect(manager).toBeDefined();

    // The version should be tracked/preserved after restore
    const reserialized = manager.serialize();
    expect(reserialized.version).toBe(EXPECTED_SCHEMA_VERSION);
    expect(reserialized.stats.totalEventsReceived).toBe(100);
  });

  it('should automatically migrate v0 to current version on restore', () => {
    const v0Snapshot = createUnversionedSnapshot();
    v0Snapshot.stats.totalEventsReceived = 50;

    const manager = CDCBufferManager.restore(v0Snapshot);

    // After restore, serializing should produce current version
    const newSnapshot = manager.serialize();
    expect(newSnapshot.version).toBe(EXPECTED_SCHEMA_VERSION);
    expect(newSnapshot.stats.totalEventsReceived).toBe(50); // Data preserved
  });

  it('should preserve all fields and add version when reading older format', () => {
    const v0Snapshot = createUnversionedSnapshot();
    v0Snapshot.stats = {
      totalEventsReceived: 100,
      totalBatchesReceived: 10,
      lastFlushTime: Date.now() - 1000,
    };
    v0Snapshot.sourceStates = [
      ['source-1', {
        sourceDoId: 'source-1',
        lastReceivedSequence: 5,
        lastAckedSequence: 5,
        connectedAt: Date.now() - 5000,
        lastActivityAt: Date.now() - 1000,
        batchesReceived: 5,
        eventsReceived: 50,
      }],
    ];

    const manager = CDCBufferManager.restore(v0Snapshot);
    const states = manager.getSourceStates();

    expect(states.get('source-1')).toBeDefined();
    expect(states.get('source-1')!.eventsReceived).toBe(50);

    // After restoring, serialize should produce versioned output
    const reserialized = manager.serialize();
    expect(reserialized.version).toBe(EXPECTED_SCHEMA_VERSION);
    expect(reserialized.stats.totalEventsReceived).toBe(100);
  });
});

// =============================================================================
// 5. Migration Path from Unversioned to Versioned Tests
// =============================================================================

describe('Migration Path from Unversioned to Versioned', () => {
  it('should provide migrateSnapshot utility function', () => {
    // There should be a utility to explicitly migrate snapshots
    const migrate = CDCBufferManager.migrateSnapshot;

    expect(migrate).toBeDefined();
    expect(typeof migrate).toBe('function');
  });

  it('should migrate unversioned snapshot to current version', () => {
    const v0Snapshot = createUnversionedSnapshot();
    v0Snapshot.stats.totalEventsReceived = 100;

    const migrated = CDCBufferManager.migrateSnapshot(v0Snapshot);

    expect(migrated.version).toBe(EXPECTED_SCHEMA_VERSION);
    expect(migrated.stats.totalEventsReceived).toBe(100);
  });

  it('should handle migration of snapshots with data', () => {
    const manager = new CDCBufferManager();
    manager.addBatch('source-1', [
      createTestCDCEvent({ table: 'users', operation: 'INSERT' }),
      createTestCDCEvent({ table: 'orders', operation: 'UPDATE' }),
    ], 1);

    const snapshot = manager.serialize();

    // Simulate reading as unversioned, then migrating
    const unversioned = { ...snapshot } as BufferSnapshot;
    delete (unversioned as Partial<BufferSnapshot>).version;

    const restoredManager = CDCBufferManager.restore(unversioned);
    const newSnapshot = restoredManager.serialize();

    expect(newSnapshot.version).toBe(EXPECTED_SCHEMA_VERSION);
    expect(newSnapshot.batches.length).toBe(1);
    expect(newSnapshot.batches[0].events.length).toBe(2);
  });

  it('should support incremental version migration (v0->v1->v2...)', () => {
    const v0Snapshot = createUnversionedSnapshot();

    // Migrate to v1
    const v1 = CDCBufferManager.migrateSnapshot(v0Snapshot, 1);
    expect(v1.version).toBe(1);

    // Migrate to current (should work even if current is v1)
    const current = CDCBufferManager.migrateSnapshot(v1);
    expect(current.version).toBe(EXPECTED_SCHEMA_VERSION);
  });

  it('should validate data integrity after migration and output versioned format', () => {
    const manager = new CDCBufferManager();
    const events = [
      createTestCDCEvent({ table: 'test', rowId: 'row-1' }),
      createTestCDCEvent({ table: 'test', rowId: 'row-2' }),
    ];
    manager.addBatch('source-1', events, 1);

    const originalSnapshot = manager.serialize();
    const unversioned = { ...originalSnapshot } as BufferSnapshot;
    delete (unversioned as Partial<BufferSnapshot>).version;

    const restoredManager = CDCBufferManager.restore(unversioned);

    // Validate data integrity
    const stats = restoredManager.getStats();
    expect(stats.eventCount).toBe(2);
    expect(stats.batchCount).toBe(1);

    const batches = restoredManager.getBatchesForFlush();
    expect(batches.length).toBe(1);
    expect(batches[0].events.length).toBe(2);

    // After migration, serialized output should include version
    const reserialized = restoredManager.serialize();
    expect(reserialized.version).toBe(EXPECTED_SCHEMA_VERSION);
  });
});

// =============================================================================
// 6. Backward Compatibility Tests (Older Reader, Newer Format)
// =============================================================================

describe('Backward Compatibility Considerations', () => {
  it('should document minimum supported version', () => {
    const minVersion = CDCBufferManager.MIN_SUPPORTED_VERSION;

    expect(minVersion).toBeDefined();
    expect(typeof minVersion).toBe('number');
    expect(minVersion).toBeGreaterThanOrEqual(0);
  });

  it('should export MIN_SUPPORTED_VERSION from module', () => {
    expect(MIN_SUPPORTED_VERSION).toBeDefined();
    expect(MIN_SUPPORTED_VERSION).toBe(0);
  });

  it('should handle unknown future fields gracefully and preserve version', () => {
    const snapshotWithExtraFields = {
      ...createVersionedSnapshot(EXPECTED_SCHEMA_VERSION),
      unknownFutureField: 'some value',
      anotherUnknownField: { nested: true },
    };

    // Should restore without error, ignoring unknown fields
    const manager = CDCBufferManager.restore(snapshotWithExtraFields as BufferSnapshot);
    expect(manager).toBeDefined();

    // Serializing should preserve version and known fields
    const newSnapshot = manager.serialize();
    expect(newSnapshot.batches).toBeDefined();
    expect(newSnapshot.stats).toBeDefined();
    // Version should be present in output
    expect(newSnapshot.version).toBe(EXPECTED_SCHEMA_VERSION);
  });

  it('should provide version compatibility check utility', () => {
    const isCompatible = CDCBufferManager.isVersionCompatible;

    expect(isCompatible).toBeDefined();
    expect(typeof isCompatible).toBe('function');

    // Current version should be compatible
    expect(isCompatible(EXPECTED_SCHEMA_VERSION)).toBe(true);

    // Future version should not be compatible
    expect(isCompatible(999)).toBe(false);

    // Version 0 should be compatible (within min supported)
    expect(isCompatible(0)).toBe(true);
  });
});

// =============================================================================
// 7. Schema Evolution Tracking Tests
// =============================================================================

describe('Schema Evolution Tracking', () => {
  it('should track schema version history', () => {
    const versionHistory = CDCBufferManager.SCHEMA_VERSION_HISTORY;

    expect(versionHistory).toBeDefined();
    expect(Array.isArray(versionHistory)).toBe(true);
    expect(versionHistory.length).toBeGreaterThanOrEqual(1);

    // Current version should be in history
    const currentVersion = versionHistory.find(v => v.version === EXPECTED_SCHEMA_VERSION);
    expect(currentVersion).toBeDefined();
    expect(currentVersion!.description).toBeDefined();
  });

  it('should export SCHEMA_VERSION_HISTORY from module', () => {
    expect(SCHEMA_VERSION_HISTORY).toBeDefined();
    expect(Array.isArray(SCHEMA_VERSION_HISTORY)).toBe(true);
    expect(SCHEMA_VERSION_HISTORY.length).toBeGreaterThanOrEqual(2); // v0 and v1
  });

  it('should document breaking changes between versions', () => {
    const breakingChanges = CDCBufferManager.BREAKING_CHANGES;

    expect(breakingChanges).toBeDefined();
    expect(typeof breakingChanges).toBe('object');
  });

  it('should export BREAKING_CHANGES from module', () => {
    expect(BREAKING_CHANGES).toBeDefined();
    expect(typeof BREAKING_CHANGES).toBe('object');
    expect(BREAKING_CHANGES[1]).toBeDefined();
  });

  it('should provide getSchemaVersion method', () => {
    const manager = new CDCBufferManager();

    expect(manager.getSchemaVersion).toBeDefined();
    expect(typeof manager.getSchemaVersion).toBe('function');
    expect(manager.getSchemaVersion()).toBe(EXPECTED_SCHEMA_VERSION);
  });
});

// =============================================================================
// 8. Error Handling for Version Mismatches
// =============================================================================

describe('Error Handling for Version Mismatches', () => {
  it('should throw VersionMismatchError for incompatible versions', () => {
    const futureSnapshot = createVersionedSnapshot(999);

    let didThrow = false;
    let errorName = '';
    try {
      CDCBufferManager.restore(futureSnapshot);
    } catch (error) {
      didThrow = true;
      errorName = (error as Error).name;
    }

    // Should have thrown an error with specific type
    expect(didThrow).toBe(true);
    expect(errorName).toBe('VersionMismatchError');
  });

  it('should export VersionMismatchError class', () => {
    expect(VersionMismatchError).toBeDefined();

    const error = new VersionMismatchError('test', 999, 1, 0);
    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe('VersionMismatchError');
    expect(error.snapshotVersion).toBe(999);
    expect(error.currentVersion).toBe(1);
    expect(error.minSupportedVersion).toBe(0);
  });

  it('should include diagnostic information in version errors', () => {
    const futureSnapshot = createVersionedSnapshot(999);

    let caughtError: Error | null = null;
    try {
      CDCBufferManager.restore(futureSnapshot);
    } catch (error) {
      caughtError = error as Error;
    }

    // Should have thrown an error
    expect(caughtError).not.toBeNull();
    expect(caughtError).toBeInstanceOf(VersionMismatchError);

    const err = caughtError as VersionMismatchError;

    // Error should include helpful properties
    expect(err.snapshotVersion).toBe(999);
    expect(err.currentVersion).toBe(EXPECTED_SCHEMA_VERSION);
    expect(err.minSupportedVersion).toBeDefined();
  });

  it('should suggest upgrade path in version error message', () => {
    const futureSnapshot = createVersionedSnapshot(999);

    let errorMessage = '';
    try {
      CDCBufferManager.restore(futureSnapshot);
    } catch (error) {
      errorMessage = (error as Error).message;
    }

    // Should have thrown an error with helpful message
    expect(errorMessage.length).toBeGreaterThan(0);
    expect(errorMessage).toMatch(/upgrade|update|newer.*version/i);
  });
});

// =============================================================================
// 9. Integration with Persistence Layer Tests
// =============================================================================

describe('Version Integration with Persistence', () => {
  it('should serialize version-aware snapshot to storage', () => {
    const manager = new CDCBufferManager();
    manager.addBatch('source-1', [createTestCDCEvent()], 1);

    const snapshot = manager.serialize();
    const serialized = JSON.stringify(snapshot);

    // Parse and verify version is present
    const parsed = JSON.parse(serialized) as BufferSnapshot;
    expect(parsed.version).toBe(EXPECTED_SCHEMA_VERSION);
  });

  it('should restore from persisted version-aware snapshot', () => {
    const manager = new CDCBufferManager();
    manager.addBatch('source-1', [
      createTestCDCEvent({ table: 'orders', rowId: 'order-1' }),
    ], 1);

    // Simulate persistence round-trip
    const snapshot = manager.serialize();
    const serialized = JSON.stringify(snapshot);
    const restored = JSON.parse(serialized) as BufferSnapshot;

    const newManager = CDCBufferManager.restore(restored);
    const newSnapshot = newManager.serialize();

    expect(newSnapshot.version).toBe(EXPECTED_SCHEMA_VERSION);
    expect(newSnapshot.batches.length).toBe(1);
  });

  it('should handle corrupted version field gracefully', () => {
    const corruptedSnapshot = {
      ...createVersionedSnapshot(EXPECTED_SCHEMA_VERSION),
      version: 'corrupted' as unknown as number,
    };

    expect(() => {
      CDCBufferManager.restore(corruptedSnapshot as BufferSnapshot);
    }).toThrow(/invalid.*version|corrupt|malformed/i);
  });
});
