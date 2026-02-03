/**
 * Sharding Configuration Migration Tests
 *
 * Tests for configuration versioning, migration, and validation:
 * - Version tracking
 * - Migration path finding
 * - Config validation
 * - Migration execution
 * - Rollback support
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

import {
  ShardingConfigManager,
  createVersionedVSchema,
  createConfigManager,
  createMigration,
  createMigrationStep,
  addShardStep,
  removeShardStep,
  changeVindexStep,
  addTableStep,
  removeTableStep,
  calculateConfigChecksum,
  verifyConfigChecksum,
  incrementVersion,
  parseVersion,
  isCompatibleVersion,
  type ConfigVersion,
  type VersionedVSchema,
  type Migration,
  type MigrationStep,
  type MigrationContext,
  type ValidationResult,
} from '../migration.js';

import {
  createShardId,
  shard,
  shardedTable,
  unshardedTable,
  referenceTable,
  hashVindex,
  consistentHashVindex,
  rangeVindex,
  createVSchema,
  type ShardConfig,
  type VSchema,
} from '../types.js';

// =============================================================================
// TEST FIXTURES
// =============================================================================

function createTestVSchema(): VSchema {
  return createVSchema(
    {
      users: shardedTable('tenant_id', hashVindex()),
      orders: shardedTable('user_id', consistentHashVindex(150)),
      countries: referenceTable(),
      config: unshardedTable(createShardId('shard-1')),
    },
    [
      shard(createShardId('shard-1'), 'user-do-ns'),
      shard(createShardId('shard-2'), 'user-do-ns'),
    ]
  );
}

function createTestVersionedVSchema(
  version: ConfigVersion = '1.0.0'
): VersionedVSchema {
  return createVersionedVSchema(createTestVSchema(), version, {
    description: 'Test configuration',
  });
}

// =============================================================================
// VERSIONED VSCHEMA TESTS
// =============================================================================

describe('createVersionedVSchema', () => {
  it('should create a versioned vschema with version', () => {
    const vschema = createTestVSchema();
    const versioned = createVersionedVSchema(vschema, '1.0.0');

    expect(versioned.version).toBe('1.0.0');
    expect(versioned.createdAt).toBeGreaterThan(0);
    expect(versioned.tables).toEqual(vschema.tables);
    expect(versioned.shards).toEqual(vschema.shards);
  });

  it('should create a versioned vschema with description', () => {
    const vschema = createTestVSchema();
    const versioned = createVersionedVSchema(vschema, '1.0.0', {
      description: 'Initial configuration',
    });

    expect(versioned.description).toBe('Initial configuration');
  });

  it('should create a versioned vschema with checksum', () => {
    const vschema = createTestVSchema();
    const checksum = calculateConfigChecksum(vschema);
    const versioned = createVersionedVSchema(vschema, '1.0.0', {
      checksum,
    });

    expect(versioned.checksum).toBe(checksum);
    expect(verifyConfigChecksum(versioned)).toBe(true);
  });
});

// =============================================================================
// CONFIG MANAGER TESTS
// =============================================================================

describe('ShardingConfigManager', () => {
  describe('constructor', () => {
    it('should create a config manager with initial config', () => {
      const config = createTestVersionedVSchema();
      const manager = createConfigManager(config);

      expect(manager.getCurrentConfig()).toEqual(config);
      expect(manager.getCurrentVersion()).toBe('1.0.0');
    });

    it('should create a config manager with logging', () => {
      const config = createTestVersionedVSchema();
      const logFn = vi.fn();
      const manager = createConfigManager(config, { onLog: logFn });

      expect(manager).toBeInstanceOf(ShardingConfigManager);
    });
  });

  describe('version comparison', () => {
    it('should compare versions correctly', () => {
      const config = createTestVersionedVSchema();
      const manager = createConfigManager(config);

      expect(manager.compareVersions('1.0.0', '1.0.0')).toBe(0);
      expect(manager.compareVersions('1.0.0', '2.0.0')).toBeLessThan(0);
      expect(manager.compareVersions('2.0.0', '1.0.0')).toBeGreaterThan(0);
      expect(manager.compareVersions('1.0.0', '1.1.0')).toBeLessThan(0);
      expect(manager.compareVersions('1.1.0', '1.0.0')).toBeGreaterThan(0);
      expect(manager.compareVersions('1.0.0', '1.0.1')).toBeLessThan(0);
      expect(manager.compareVersions('1.0.1', '1.0.0')).toBeGreaterThan(0);
    });
  });

  describe('validation', () => {
    it('should validate a valid configuration', () => {
      const config = createTestVersionedVSchema();
      const manager = createConfigManager(config);

      const result = manager.validate(config);

      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should detect invalid version format', () => {
      const config = createTestVersionedVSchema();
      const manager = createConfigManager(config);

      const invalidConfig = { ...config, version: 'invalid' as ConfigVersion };
      const result = manager.validate(invalidConfig);

      expect(result.valid).toBe(false);
      expect(result.errors).toContainEqual(
        expect.objectContaining({
          code: 'INVALID_VERSION',
          path: 'version',
        })
      );
    });

    it('should detect missing shards', () => {
      const config = { ...createTestVersionedVSchema(), shards: [] };
      const manager = createConfigManager(createTestVersionedVSchema());

      const result = manager.validate(config);

      expect(result.valid).toBe(false);
      expect(result.errors).toContainEqual(
        expect.objectContaining({
          code: 'NO_SHARDS',
          path: 'shards',
        })
      );
    });

    it('should detect duplicate shard IDs', () => {
      const vschema = createVSchema(
        { users: shardedTable('tenant_id', hashVindex()) },
        [
          shard(createShardId('shard-1'), 'user-do-ns'),
          shard(createShardId('shard-1'), 'user-do-ns-2'), // Duplicate
        ]
      );
      const config = createVersionedVSchema(vschema, '1.0.0');
      const manager = createConfigManager(config);

      const result = manager.validate(config);

      expect(result.valid).toBe(false);
      expect(result.errors).toContainEqual(
        expect.objectContaining({
          code: 'DUPLICATE_SHARD_ID',
        })
      );
    });

    it('should detect missing shard key for sharded table', () => {
      const config = createTestVersionedVSchema();
      // Manually break the config
      (config.tables['users'] as unknown as { shardKey: string }).shardKey = '';
      const manager = createConfigManager(createTestVersionedVSchema());

      const result = manager.validate(config);

      expect(result.valid).toBe(false);
      expect(result.errors).toContainEqual(
        expect.objectContaining({
          code: 'MISSING_SHARD_KEY',
          path: 'tables.users.shardKey',
        })
      );
    });

    it('should detect missing vindex for sharded table', () => {
      const config = createTestVersionedVSchema();
      // Manually break the config
      delete (config.tables['users'] as unknown as { vindex?: unknown }).vindex;
      const manager = createConfigManager(createTestVersionedVSchema());

      const result = manager.validate(config);

      expect(result.valid).toBe(false);
      expect(result.errors).toContainEqual(
        expect.objectContaining({
          code: 'MISSING_VINDEX',
          path: 'tables.users.vindex',
        })
      );
    });

    it('should detect invalid hash algorithm', () => {
      const vschema = createVSchema(
        {
          users: shardedTable('tenant_id', {
            type: 'hash',
            algorithm: 'invalid' as unknown as 'fnv1a' | 'xxhash',
          }),
        },
        [shard(createShardId('shard-1'), 'user-do-ns')]
      );
      const config = createVersionedVSchema(vschema, '1.0.0');
      const manager = createConfigManager(config);

      const result = manager.validate(config);

      expect(result.valid).toBe(false);
      expect(result.errors).toContainEqual(
        expect.objectContaining({
          code: 'INVALID_HASH_ALGORITHM',
        })
      );
    });

    it('should detect missing boundaries for range vindex', () => {
      const vschema = createVSchema(
        {
          users: shardedTable('tenant_id', {
            type: 'range',
            boundaries: [],
          }),
        },
        [shard(createShardId('shard-1'), 'user-do-ns')]
      );
      const config = createVersionedVSchema(vschema, '1.0.0');
      const manager = createConfigManager(config);

      const result = manager.validate(config);

      expect(result.valid).toBe(false);
      expect(result.errors).toContainEqual(
        expect.objectContaining({
          code: 'MISSING_BOUNDARIES',
        })
      );
    });

    it('should detect invalid boundary shard reference', () => {
      const vschema = createVSchema(
        {
          users: shardedTable('tenant_id', rangeVindex([
            { shard: createShardId('non-existent'), min: 0, max: 100 },
          ])),
        },
        [shard(createShardId('shard-1'), 'user-do-ns')]
      );
      const config = createVersionedVSchema(vschema, '1.0.0');
      const manager = createConfigManager(config);

      const result = manager.validate(config);

      expect(result.valid).toBe(false);
      expect(result.errors).toContainEqual(
        expect.objectContaining({
          code: 'INVALID_BOUNDARY_SHARD',
        })
      );
    });

    it('should detect invalid unsharded table shard reference', () => {
      const vschema = createVSchema(
        {
          config: unshardedTable(createShardId('non-existent')),
        },
        [shard(createShardId('shard-1'), 'user-do-ns')]
      );
      const config = createVersionedVSchema(vschema, '1.0.0');
      const manager = createConfigManager(config);

      const result = manager.validate(config);

      expect(result.valid).toBe(false);
      expect(result.errors).toContainEqual(
        expect.objectContaining({
          code: 'INVALID_SHARD_REFERENCE',
        })
      );
    });

    it('should detect invalid default shard reference', () => {
      const vschema = createVSchema(
        { users: shardedTable('tenant_id', hashVindex()) },
        [shard(createShardId('shard-1'), 'user-do-ns')],
        { defaultShard: createShardId('non-existent') }
      );
      const config = createVersionedVSchema(vschema, '1.0.0');
      const manager = createConfigManager(config);

      const result = manager.validate(config);

      expect(result.valid).toBe(false);
      expect(result.errors).toContainEqual(
        expect.objectContaining({
          code: 'INVALID_DEFAULT_SHARD',
        })
      );
    });

    it('should warn about invalid virtual node count', () => {
      const vschema = createVSchema(
        {
          users: shardedTable('tenant_id', consistentHashVindex(50000)),
        },
        [shard(createShardId('shard-1'), 'user-do-ns')]
      );
      const config = createVersionedVSchema(vschema, '1.0.0');
      const manager = createConfigManager(config);

      const result = manager.validate(config);

      // Should be valid but with warning
      expect(result.valid).toBe(true);
      expect(result.errors).toContainEqual(
        expect.objectContaining({
          code: 'INVALID_VIRTUAL_NODES',
          severity: 'warning',
        })
      );
    });
  });

  describe('migration registration', () => {
    it('should register a migration', () => {
      const config = createTestVersionedVSchema();
      const manager = createConfigManager(config);

      const migration = createMigration({
        id: 'test-migration',
        fromVersion: '1.0.0',
        toVersion: '1.1.0',
        description: 'Test migration',
        steps: [],
      });

      manager.registerMigration(migration);

      expect(manager.getMigrations()).toContainEqual(migration);
    });

    it('should find direct migration path', () => {
      const config = createTestVersionedVSchema();
      const manager = createConfigManager(config);

      const migration = createMigration({
        id: 'test-migration',
        fromVersion: '1.0.0',
        toVersion: '1.1.0',
        description: 'Test migration',
        steps: [],
      });

      manager.registerMigration(migration);

      const path = manager.findMigrationPath('1.0.0', '1.1.0');

      expect(path).toHaveLength(1);
      expect(path?.[0]).toBe(migration);
    });

    it('should find multi-step migration path', () => {
      const config = createTestVersionedVSchema();
      const manager = createConfigManager(config);

      const migration1 = createMigration({
        id: 'migration-1',
        fromVersion: '1.0.0',
        toVersion: '1.1.0',
        description: 'First migration',
        steps: [],
      });

      const migration2 = createMigration({
        id: 'migration-2',
        fromVersion: '1.1.0',
        toVersion: '1.2.0',
        description: 'Second migration',
        steps: [],
      });

      manager.registerMigration(migration1);
      manager.registerMigration(migration2);

      const path = manager.findMigrationPath('1.0.0', '1.2.0');

      expect(path).toHaveLength(2);
      expect(path?.[0]).toBe(migration1);
      expect(path?.[1]).toBe(migration2);
    });

    it('should return empty path for same version', () => {
      const config = createTestVersionedVSchema();
      const manager = createConfigManager(config);

      const path = manager.findMigrationPath('1.0.0', '1.0.0');

      expect(path).toHaveLength(0);
    });

    it('should return null for no migration path', () => {
      const config = createTestVersionedVSchema();
      const manager = createConfigManager(config);

      const path = manager.findMigrationPath('1.0.0', '2.0.0');

      expect(path).toBeNull();
    });
  });

  describe('migration execution', () => {
    it('should execute a simple migration', async () => {
      const config = createTestVersionedVSchema('1.0.0');
      const manager = createConfigManager(config);

      const executeFn = vi.fn().mockResolvedValue(undefined);

      const migration = createMigration({
        id: 'test-migration',
        fromVersion: '1.0.0',
        toVersion: '1.1.0',
        description: 'Test migration',
        steps: [
          createMigrationStep({
            id: 'step-1',
            description: 'Test step',
            execute: executeFn,
          }),
        ],
      });

      manager.registerMigration(migration);

      const record = await manager.migrate('1.1.0');

      expect(record.status).toBe('completed');
      expect(record.fromVersion).toBe('1.0.0');
      expect(record.toVersion).toBe('1.1.0');
      expect(executeFn).toHaveBeenCalledTimes(1);
    });

    it('should execute multiple migration steps', async () => {
      const config = createTestVersionedVSchema('1.0.0');
      const manager = createConfigManager(config);

      const executeFn1 = vi.fn().mockResolvedValue(undefined);
      const executeFn2 = vi.fn().mockResolvedValue(undefined);

      const migration = createMigration({
        id: 'test-migration',
        fromVersion: '1.0.0',
        toVersion: '1.1.0',
        description: 'Test migration',
        steps: [
          createMigrationStep({
            id: 'step-1',
            description: 'First step',
            execute: executeFn1,
          }),
          createMigrationStep({
            id: 'step-2',
            description: 'Second step',
            execute: executeFn2,
          }),
        ],
      });

      manager.registerMigration(migration);

      await manager.migrate('1.1.0');

      expect(executeFn1).toHaveBeenCalledTimes(1);
      expect(executeFn2).toHaveBeenCalledTimes(1);
    });

    it('should track affected tables and shards', async () => {
      const config = createTestVersionedVSchema('1.0.0');
      const manager = createConfigManager(config);

      const migration = createMigration({
        id: 'test-migration',
        fromVersion: '1.0.0',
        toVersion: '1.1.0',
        description: 'Test migration',
        steps: [
          createMigrationStep({
            id: 'step-1',
            description: 'Test step',
            tables: ['users', 'orders'],
            shards: ['shard-1'],
            execute: async () => {},
          }),
        ],
      });

      manager.registerMigration(migration);

      const record = await manager.migrate('1.1.0');

      expect(record.affectedTables).toContain('users');
      expect(record.affectedTables).toContain('orders');
      expect(record.affectedShards).toContain('shard-1');
    });

    it('should handle migration failure', async () => {
      const config = createTestVersionedVSchema('1.0.0');
      const manager = createConfigManager(config);

      const migration = createMigration({
        id: 'test-migration',
        fromVersion: '1.0.0',
        toVersion: '1.1.0',
        description: 'Test migration',
        steps: [
          createMigrationStep({
            id: 'step-1',
            description: 'Failing step',
            execute: async () => {
              throw new Error('Migration failed');
            },
          }),
        ],
      });

      manager.registerMigration(migration);

      await expect(manager.migrate('1.1.0')).rejects.toThrow('Migration failed');

      const history = manager.getMigrationHistory();
      expect(history[0].status).toBe('failed');
      expect(history[0].error).toContain('Migration failed');
    });

    it('should throw for no migration path', async () => {
      const config = createTestVersionedVSchema('1.0.0');
      const manager = createConfigManager(config);

      await expect(manager.migrate('2.0.0')).rejects.toThrow('No migration path found');
    });

    it('should throw for same version', async () => {
      const config = createTestVersionedVSchema('1.0.0');
      const manager = createConfigManager(config);

      await expect(manager.migrate('1.0.0')).rejects.toThrow('Already at version');
    });
  });

  describe('migration downtime estimation', () => {
    it('should detect safe migration', () => {
      const config = createTestVersionedVSchema('1.0.0');
      const manager = createConfigManager(config);

      const migration = createMigration({
        id: 'safe-migration',
        fromVersion: '1.0.0',
        toVersion: '1.1.0',
        description: 'Zero downtime migration',
        steps: [],
        estimatedDowntimeMs: 0,
      });

      manager.registerMigration(migration);

      expect(manager.isSafeMigration('1.0.0', '1.1.0')).toBe(true);
    });

    it('should detect unsafe migration', () => {
      const config = createTestVersionedVSchema('1.0.0');
      const manager = createConfigManager(config);

      const migration = createMigration({
        id: 'unsafe-migration',
        fromVersion: '1.0.0',
        toVersion: '1.1.0',
        description: 'Migration with downtime',
        steps: [],
        estimatedDowntimeMs: 5000,
      });

      manager.registerMigration(migration);

      expect(manager.isSafeMigration('1.0.0', '1.1.0')).toBe(false);
    });

    it('should estimate total downtime', () => {
      const config = createTestVersionedVSchema('1.0.0');
      const manager = createConfigManager(config);

      const migration1 = createMigration({
        id: 'migration-1',
        fromVersion: '1.0.0',
        toVersion: '1.1.0',
        description: 'First migration',
        steps: [],
        estimatedDowntimeMs: 1000,
      });

      const migration2 = createMigration({
        id: 'migration-2',
        fromVersion: '1.1.0',
        toVersion: '1.2.0',
        description: 'Second migration',
        steps: [],
        estimatedDowntimeMs: 2000,
      });

      manager.registerMigration(migration1);
      manager.registerMigration(migration2);

      expect(manager.estimateDowntime('1.0.0', '1.2.0')).toBe(3000);
    });

    it('should return -1 for no migration path', () => {
      const config = createTestVersionedVSchema('1.0.0');
      const manager = createConfigManager(config);

      expect(manager.estimateDowntime('1.0.0', '2.0.0')).toBe(-1);
    });
  });

  describe('config update', () => {
    it('should update config after validation', () => {
      const config = createTestVersionedVSchema('1.0.0');
      const manager = createConfigManager(config);

      const newConfig = createTestVersionedVSchema('1.1.0');
      manager.updateConfig(newConfig);

      expect(manager.getCurrentConfig()).toEqual(newConfig);
    });

    it('should reject invalid config', () => {
      const config = createTestVersionedVSchema('1.0.0');
      const manager = createConfigManager(config);

      const invalidConfig = { ...createTestVersionedVSchema('1.1.0'), shards: [] };

      expect(() => manager.updateConfig(invalidConfig)).toThrow('Invalid configuration');
    });
  });
});

// =============================================================================
// MIGRATION STEP HELPERS TESTS
// =============================================================================

describe('Migration Step Helpers', () => {
  describe('addShardStep', () => {
    it('should create an add shard step', () => {
      const shardConfig = shard(createShardId('shard-3'), 'user-do-ns');
      const step = addShardStep(shardConfig);

      expect(step.id).toBe('add-shard-shard-3');
      expect(step.shards).toContain('shard-3');
      expect(step.reversible).toBe(true);
    });
  });

  describe('removeShardStep', () => {
    it('should create a remove shard step', () => {
      const step = removeShardStep('shard-3');

      expect(step.id).toBe('remove-shard-shard-3');
      expect(step.shards).toContain('shard-3');
      expect(step.reversible).toBe(false); // Data loss
    });
  });

  describe('changeVindexStep', () => {
    it('should create a change vindex step', () => {
      const newVindex = consistentHashVindex(200);
      const step = changeVindexStep('users', newVindex, ['shard-1', 'shard-2']);

      expect(step.id).toBe('change-vindex-users');
      expect(step.tables).toContain('users');
      expect(step.shards).toContain('shard-1');
      expect(step.shards).toContain('shard-2');
      expect(step.reversible).toBe(false); // Would need data migration
    });
  });

  describe('addTableStep', () => {
    it('should create an add table step', () => {
      const tableConfig = shardedTable('tenant_id', hashVindex());
      const step = addTableStep('new_table', tableConfig, ['shard-1']);

      expect(step.id).toBe('add-table-new_table');
      expect(step.tables).toContain('new_table');
      expect(step.reversible).toBe(true);
    });
  });

  describe('removeTableStep', () => {
    it('should create a remove table step', () => {
      const step = removeTableStep('old_table', ['shard-1']);

      expect(step.id).toBe('remove-table-old_table');
      expect(step.tables).toContain('old_table');
      expect(step.reversible).toBe(false); // Data loss
    });
  });
});

// =============================================================================
// CHECKSUM TESTS
// =============================================================================

describe('Checksum Utilities', () => {
  it('should calculate consistent checksum', () => {
    const vschema = createTestVSchema();
    const checksum1 = calculateConfigChecksum(vschema);
    const checksum2 = calculateConfigChecksum(vschema);

    expect(checksum1).toBe(checksum2);
  });

  it('should calculate different checksum for different configs', () => {
    const vschema1 = createTestVSchema();
    const vschema2 = createVSchema(
      { users: shardedTable('tenant_id', hashVindex()) },
      [shard(createShardId('shard-1'), 'user-do-ns')]
    );

    const checksum1 = calculateConfigChecksum(vschema1);
    const checksum2 = calculateConfigChecksum(vschema2);

    expect(checksum1).not.toBe(checksum2);
  });

  it('should verify valid checksum', () => {
    const vschema = createTestVSchema();
    const checksum = calculateConfigChecksum(vschema);
    const versioned = createVersionedVSchema(vschema, '1.0.0', { checksum });

    expect(verifyConfigChecksum(versioned)).toBe(true);
  });

  it('should detect invalid checksum', () => {
    const vschema = createTestVSchema();
    const versioned = createVersionedVSchema(vschema, '1.0.0', {
      checksum: 'invalid',
    });

    expect(verifyConfigChecksum(versioned)).toBe(false);
  });

  it('should pass verification with no checksum', () => {
    const versioned = createTestVersionedVSchema();

    expect(verifyConfigChecksum(versioned)).toBe(true);
  });
});

// =============================================================================
// VERSION UTILITIES TESTS
// =============================================================================

describe('Version Utilities', () => {
  describe('incrementVersion', () => {
    it('should increment major version', () => {
      expect(incrementVersion('1.2.3', 'major')).toBe('2.0.0');
    });

    it('should increment minor version', () => {
      expect(incrementVersion('1.2.3', 'minor')).toBe('1.3.0');
    });

    it('should increment patch version', () => {
      expect(incrementVersion('1.2.3', 'patch')).toBe('1.2.4');
    });
  });

  describe('parseVersion', () => {
    it('should parse version correctly', () => {
      const parsed = parseVersion('1.2.3');

      expect(parsed.major).toBe(1);
      expect(parsed.minor).toBe(2);
      expect(parsed.patch).toBe(3);
    });
  });

  describe('isCompatibleVersion', () => {
    it('should detect compatible versions', () => {
      expect(isCompatibleVersion('1.0.0', '1.5.0')).toBe(true);
      expect(isCompatibleVersion('1.0.0', '1.0.1')).toBe(true);
    });

    it('should detect incompatible versions', () => {
      expect(isCompatibleVersion('1.0.0', '2.0.0')).toBe(false);
      expect(isCompatibleVersion('2.1.0', '3.0.0')).toBe(false);
    });
  });
});

// =============================================================================
// EDGE CASES
// =============================================================================

describe('Edge Cases', () => {
  it('should handle migration with progress callback', async () => {
    const config = createTestVersionedVSchema('1.0.0');
    const progressFn = vi.fn();
    const manager = createConfigManager(config, { onProgress: progressFn });

    const migration = createMigration({
      id: 'test-migration',
      fromVersion: '1.0.0',
      toVersion: '1.1.0',
      description: 'Test migration',
      steps: [
        createMigrationStep({
          id: 'step-1',
          description: 'Test step',
          execute: async (context) => {
            context.onProgress?.(50, 'Half done');
          },
        }),
      ],
    });

    manager.registerMigration(migration);
    await manager.migrate('1.1.0');

    expect(progressFn).toHaveBeenCalledWith(50, 'Half done');
  });

  it('should handle empty migration steps', async () => {
    const config = createTestVersionedVSchema('1.0.0');
    const manager = createConfigManager(config);

    const migration = createMigration({
      id: 'empty-migration',
      fromVersion: '1.0.0',
      toVersion: '1.1.0',
      description: 'Empty migration',
      steps: [],
    });

    manager.registerMigration(migration);

    const record = await manager.migrate('1.1.0');

    expect(record.status).toBe('completed');
  });

  it('should create migration with automatic reversible detection', () => {
    const migration = createMigration({
      id: 'test',
      fromVersion: '1.0.0',
      toVersion: '1.1.0',
      description: 'Test',
      steps: [
        createMigrationStep({
          id: 'reversible-step',
          description: 'Has rollback',
          execute: async () => {},
          rollback: async () => {},
        }),
        createMigrationStep({
          id: 'non-reversible-step',
          description: 'No rollback',
          execute: async () => {},
        }),
      ],
    });

    expect(migration.reversible).toBe(false); // One step is not reversible
  });

  it('should handle long migration chains', async () => {
    const config = createTestVersionedVSchema('1.0.0');
    const manager = createConfigManager(config);

    // Create a chain of 10 migrations
    for (let i = 0; i < 10; i++) {
      manager.registerMigration(
        createMigration({
          id: `migration-${i}`,
          fromVersion: `1.${i}.0` as ConfigVersion,
          toVersion: `1.${i + 1}.0` as ConfigVersion,
          description: `Migration ${i}`,
          steps: [],
        })
      );
    }

    const path = manager.findMigrationPath('1.0.0', '1.10.0');

    expect(path).toHaveLength(10);
  });

  it('should handle cyclic migration detection', () => {
    const config = createTestVersionedVSchema('1.0.0');
    const manager = createConfigManager(config);

    // Create a cycle (shouldn't happen in practice, but test resilience)
    manager.registerMigration(
      createMigration({
        id: 'migration-a',
        fromVersion: '1.0.0',
        toVersion: '1.1.0',
        description: 'A',
        steps: [],
      })
    );
    manager.registerMigration(
      createMigration({
        id: 'migration-b',
        fromVersion: '1.1.0',
        toVersion: '1.2.0',
        description: 'B',
        steps: [],
      })
    );
    manager.registerMigration(
      createMigration({
        id: 'migration-c',
        fromVersion: '1.2.0',
        toVersion: '1.1.0',
        description: 'C - creates cycle',
        steps: [],
      })
    );

    // Should still find valid path avoiding cycle
    const path = manager.findMigrationPath('1.0.0', '1.2.0');
    expect(path).toHaveLength(2);
  });
});
