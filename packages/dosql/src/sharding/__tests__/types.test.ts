/**
 * Sharding Types and Factory Functions Unit Tests
 *
 * Tests for type factory functions and configuration builders:
 * - Vindex factory functions (hash, consistent-hash, range)
 * - Table configuration factories
 * - Shard and replica configuration factories
 * - VSchema creation
 *
 * @packageDocumentation
 */

import { describe, it, expect } from 'vitest';

import {
  // Factory functions
  hashVindex,
  consistentHashVindex,
  rangeVindex,
  shardedTable,
  unshardedTable,
  referenceTable,
  shard,
  replica,
  createVSchema,
  createShardId,

  // Types for type checking
  type HashVindexConfig,
  type ConsistentHashVindexConfig,
  type RangeVindexConfig,
  type RangeBoundary,
  type ShardedTableConfig,
  type UnshardedTableConfig,
  type ReferenceTableConfig,
  type ShardConfig,
  type ReplicaConfig,
  type VSchema,
  type ShardId,
  type VSchemaSettings,
} from '../types.js';

// =============================================================================
// SHARD ID TESTS
// =============================================================================

describe('createShardId', () => {
  it('should create a branded shard ID', () => {
    const id = createShardId('shard-1');

    expect(id).toBe('shard-1');
    // TypeScript would catch misuse at compile time
  });

  it('should create different IDs for different inputs', () => {
    const id1 = createShardId('shard-1');
    const id2 = createShardId('shard-2');

    expect(id1).not.toBe(id2);
  });

  it('should throw on empty string', () => {
    expect(() => createShardId('')).toThrow('ShardId cannot be empty');
  });

  it('should allow special characters', () => {
    const id = createShardId('shard-us-west-1a');
    expect(id).toBe('shard-us-west-1a');
  });
});

// =============================================================================
// VINDEX FACTORY TESTS
// =============================================================================

describe('hashVindex', () => {
  it('should create hash vindex with default algorithm', () => {
    const vindex = hashVindex();

    expect(vindex.type).toBe('hash');
    expect(vindex.algorithm).toBe('fnv1a');
  });

  it('should create hash vindex with fnv1a algorithm', () => {
    const vindex = hashVindex('fnv1a');

    expect(vindex.type).toBe('hash');
    expect(vindex.algorithm).toBe('fnv1a');
  });

  it('should create hash vindex with xxhash algorithm', () => {
    const vindex = hashVindex('xxhash');

    expect(vindex.type).toBe('hash');
    expect(vindex.algorithm).toBe('xxhash');
  });

  it('should return correct type structure', () => {
    const vindex: HashVindexConfig = hashVindex();

    expect(vindex).toHaveProperty('type');
    expect(vindex).toHaveProperty('algorithm');
  });
});

describe('consistentHashVindex', () => {
  it('should create consistent hash vindex with defaults', () => {
    const vindex = consistentHashVindex();

    expect(vindex.type).toBe('consistent-hash');
    expect(vindex.virtualNodes).toBe(150);
    expect(vindex.algorithm).toBe('fnv1a');
  });

  it('should create consistent hash vindex with custom virtual nodes', () => {
    const vindex = consistentHashVindex(200);

    expect(vindex.virtualNodes).toBe(200);
  });

  it('should create consistent hash vindex with xxhash', () => {
    const vindex = consistentHashVindex(150, 'xxhash');

    expect(vindex.algorithm).toBe('xxhash');
  });

  it('should accept very high virtual node count', () => {
    const vindex = consistentHashVindex(1000);

    expect(vindex.virtualNodes).toBe(1000);
  });

  it('should accept low virtual node count', () => {
    const vindex = consistentHashVindex(1);

    expect(vindex.virtualNodes).toBe(1);
  });

  it('should return correct type structure', () => {
    const vindex: ConsistentHashVindexConfig = consistentHashVindex();

    expect(vindex).toHaveProperty('type');
    expect(vindex).toHaveProperty('virtualNodes');
    expect(vindex).toHaveProperty('algorithm');
  });
});

describe('rangeVindex', () => {
  it('should create range vindex with number boundaries', () => {
    const boundaries: RangeBoundary<number>[] = [
      { shard: createShardId('shard-1'), min: 0, max: 1000 },
      { shard: createShardId('shard-2'), min: 1000, max: null },
    ];

    const vindex = rangeVindex(boundaries);

    expect(vindex.type).toBe('range');
    expect(vindex.boundaries).toHaveLength(2);
  });

  it('should create range vindex with string boundaries', () => {
    const boundaries: RangeBoundary<string>[] = [
      { shard: createShardId('shard-1'), min: 'A', max: 'M' },
      { shard: createShardId('shard-2'), min: 'M', max: null },
    ];

    const vindex = rangeVindex(boundaries);

    expect(vindex.type).toBe('range');
    expect(vindex.boundaries[0]!.min).toBe('A');
  });

  it('should create range vindex with date boundaries', () => {
    const boundaries: RangeBoundary<Date>[] = [
      { shard: createShardId('shard-2024'), min: new Date('2024-01-01'), max: new Date('2025-01-01') },
      { shard: createShardId('shard-2025'), min: new Date('2025-01-01'), max: null },
    ];

    const vindex = rangeVindex(boundaries);

    expect(vindex.type).toBe('range');
    expect(vindex.boundaries[0]!.min).toBeInstanceOf(Date);
  });

  it('should handle empty boundaries', () => {
    const vindex = rangeVindex([]);

    expect(vindex.boundaries).toHaveLength(0);
  });

  it('should handle single boundary', () => {
    const boundaries: RangeBoundary<number>[] = [
      { shard: createShardId('shard-1'), min: 0, max: null },
    ];

    const vindex = rangeVindex(boundaries);

    expect(vindex.boundaries).toHaveLength(1);
    expect(vindex.boundaries[0]!.max).toBeNull();
  });

  it('should return correct type structure', () => {
    const vindex: RangeVindexConfig<number> = rangeVindex([
      { shard: createShardId('shard-1'), min: 0, max: 100 },
    ]);

    expect(vindex).toHaveProperty('type');
    expect(vindex).toHaveProperty('boundaries');
  });
});

// =============================================================================
// TABLE CONFIGURATION FACTORY TESTS
// =============================================================================

describe('shardedTable', () => {
  it('should create sharded table config with hash vindex', () => {
    const config = shardedTable('tenant_id', hashVindex());

    expect(config.type).toBe('sharded');
    expect(config.shardKey).toBe('tenant_id');
    expect(config.vindex.type).toBe('hash');
  });

  it('should create sharded table config with consistent hash vindex', () => {
    const config = shardedTable('user_id', consistentHashVindex(200));

    expect(config.shardKey).toBe('user_id');
    expect(config.vindex.type).toBe('consistent-hash');
  });

  it('should create sharded table config with range vindex', () => {
    const boundaries: RangeBoundary<number>[] = [
      { shard: createShardId('s1'), min: 0, max: 100 },
    ];
    const config = shardedTable('event_id', rangeVindex(boundaries));

    expect(config.shardKey).toBe('event_id');
    expect(config.vindex.type).toBe('range');
  });

  it('should return correct type structure', () => {
    const config: ShardedTableConfig = shardedTable('id', hashVindex());

    expect(config).toHaveProperty('type');
    expect(config).toHaveProperty('shardKey');
    expect(config).toHaveProperty('vindex');
  });

  it('should allow any string as shard key', () => {
    const config = shardedTable('my_custom_column_name', hashVindex());

    expect(config.shardKey).toBe('my_custom_column_name');
  });
});

describe('unshardedTable', () => {
  it('should create unsharded table config without shard', () => {
    const config = unshardedTable();

    expect(config.type).toBe('unsharded');
    expect(config.shard).toBeUndefined();
  });

  it('should create unsharded table config with specific shard', () => {
    const shardId = createShardId('primary-shard');
    const config = unshardedTable(shardId);

    expect(config.type).toBe('unsharded');
    expect(config.shard).toBe(shardId);
  });

  it('should return correct type structure', () => {
    const config: UnshardedTableConfig = unshardedTable();

    expect(config).toHaveProperty('type');
  });
});

describe('referenceTable', () => {
  it('should create reference table config with default read-only false', () => {
    const config = referenceTable();

    expect(config.type).toBe('reference');
    expect(config.readOnly).toBe(false);
  });

  it('should create reference table config with read-only true', () => {
    const config = referenceTable(true);

    expect(config.type).toBe('reference');
    expect(config.readOnly).toBe(true);
  });

  it('should create reference table config with read-only false explicitly', () => {
    const config = referenceTable(false);

    expect(config.readOnly).toBe(false);
  });

  it('should return correct type structure', () => {
    const config: ReferenceTableConfig = referenceTable();

    expect(config).toHaveProperty('type');
    expect(config).toHaveProperty('readOnly');
  });
});

// =============================================================================
// SHARD AND REPLICA FACTORY TESTS
// =============================================================================

describe('shard', () => {
  it('should create shard config with required fields', () => {
    const id = createShardId('shard-1');
    const config = shard(id, 'user-do');

    expect(config.id).toBe(id);
    expect(config.doNamespace).toBe('user-do');
  });

  it('should create shard config with replicas', () => {
    const id = createShardId('shard-1');
    const r1 = replica('replica-1', 'user-do-replica', 'replica');
    const config = shard(id, 'user-do', { replicas: [r1] });

    expect(config.replicas).toHaveLength(1);
    expect(config.replicas![0]!.id).toBe('replica-1');
  });

  it('should create shard config with multiple replicas', () => {
    const id = createShardId('shard-1');
    const config = shard(id, 'user-do', {
      replicas: [
        replica('r1', 'ns1', 'replica'),
        replica('r2', 'ns2', 'replica'),
        replica('analytics', 'ns3', 'analytics'),
      ],
    });

    expect(config.replicas).toHaveLength(3);
  });

  it('should create shard config with custom DO ID', () => {
    const id = createShardId('shard-1');
    const config = shard(id, 'user-do', { doId: 'custom-do-id' });

    expect(config.doId).toBe('custom-do-id');
  });

  it('should create shard config with metadata', () => {
    const id = createShardId('shard-1');
    const config = shard(id, 'user-do', {
      metadata: { region: 'us-west', datacenter: 'dc1' },
    });

    expect(config.metadata!.region).toBe('us-west');
    expect(config.metadata!.datacenter).toBe('dc1');
  });

  it('should create shard config with readOnly flag', () => {
    const id = createShardId('shard-1');
    const config = shard(id, 'user-do', { readOnly: true });

    expect(config.readOnly).toBe(true);
  });

  it('should create shard config with all options', () => {
    const id = createShardId('shard-1');
    const config = shard(id, 'user-do', {
      doId: 'custom-id',
      replicas: [replica('r1', 'ns1', 'replica')],
      metadata: { key: 'value' },
      readOnly: false,
    });

    expect(config.doId).toBe('custom-id');
    expect(config.replicas).toHaveLength(1);
    expect(config.metadata!.key).toBe('value');
    expect(config.readOnly).toBe(false);
  });

  it('should return correct type structure', () => {
    const config: ShardConfig = shard(createShardId('s1'), 'ns');

    expect(config).toHaveProperty('id');
    expect(config).toHaveProperty('doNamespace');
  });
});

describe('replica', () => {
  it('should create replica config with required fields', () => {
    const config = replica('replica-1', 'user-do-replica', 'replica');

    expect(config.id).toBe('replica-1');
    expect(config.doNamespace).toBe('user-do-replica');
    expect(config.role).toBe('replica');
  });

  it('should create primary replica', () => {
    const config = replica('primary-1', 'user-do', 'primary');

    expect(config.role).toBe('primary');
  });

  it('should create analytics replica', () => {
    const config = replica('analytics-1', 'user-do-analytics', 'analytics');

    expect(config.role).toBe('analytics');
  });

  it('should create replica with region', () => {
    const config = replica('r1', 'ns1', 'replica', { region: 'us-west' });

    expect(config.region).toBe('us-west');
  });

  it('should create replica with weight', () => {
    const config = replica('r1', 'ns1', 'replica', { weight: 100 });

    expect(config.weight).toBe(100);
  });

  it('should create replica with DO ID', () => {
    const config = replica('r1', 'ns1', 'replica', { doId: 'custom-do-id' });

    expect(config.doId).toBe('custom-do-id');
  });

  it('should create replica with all options', () => {
    const config = replica('r1', 'ns1', 'replica', {
      doId: 'custom-id',
      region: 'eu-central',
      weight: 50,
    });

    expect(config.doId).toBe('custom-id');
    expect(config.region).toBe('eu-central');
    expect(config.weight).toBe(50);
  });

  it('should return correct type structure', () => {
    const config: ReplicaConfig = replica('r1', 'ns1', 'replica');

    expect(config).toHaveProperty('id');
    expect(config).toHaveProperty('doNamespace');
    expect(config).toHaveProperty('role');
  });
});

// =============================================================================
// VSCHEMA FACTORY TESTS
// =============================================================================

describe('createVSchema', () => {
  it('should create basic VSchema', () => {
    const shards = [shard(createShardId('shard-1'), 'user-do')];
    const vschema = createVSchema(
      {
        users: shardedTable('tenant_id', hashVindex()),
      },
      shards
    );

    expect(vschema.tables).toBeDefined();
    expect(vschema.tables.users).toBeDefined();
    expect(vschema.shards).toHaveLength(1);
  });

  it('should create VSchema with multiple tables', () => {
    const shards = [
      shard(createShardId('shard-1'), 'do-ns'),
      shard(createShardId('shard-2'), 'do-ns'),
    ];

    const vschema = createVSchema(
      {
        users: shardedTable('tenant_id', hashVindex()),
        orders: shardedTable('user_id', hashVindex()),
        config: unshardedTable(),
        countries: referenceTable(true),
      },
      shards
    );

    expect(Object.keys(vschema.tables)).toHaveLength(4);
    expect(vschema.tables.users.type).toBe('sharded');
    expect(vschema.tables.config.type).toBe('unsharded');
    expect(vschema.tables.countries.type).toBe('reference');
  });

  it('should create VSchema with default shard', () => {
    const shardId = createShardId('default-shard');
    const shards = [shard(shardId, 'do-ns')];

    const vschema = createVSchema(
      { users: shardedTable('id', hashVindex()) },
      shards,
      { defaultShard: shardId }
    );

    expect(vschema.defaultShard).toBe(shardId);
  });

  it('should create VSchema with settings', () => {
    const settings: VSchemaSettings = {
      defaultVindexType: 'hash',
      autoDetectShardKey: true,
      maxParallelShards: 16,
      shardTimeoutMs: 5000,
      enableCaching: true,
    };

    const shards = [shard(createShardId('shard-1'), 'do-ns')];
    const vschema = createVSchema(
      { users: shardedTable('id', hashVindex()) },
      shards,
      { settings }
    );

    expect(vschema.settings).toBeDefined();
    expect(vschema.settings!.maxParallelShards).toBe(16);
    expect(vschema.settings!.enableCaching).toBe(true);
  });

  it('should create VSchema with both default shard and settings', () => {
    const shardId = createShardId('shard-1');
    const shards = [shard(shardId, 'do-ns')];

    const vschema = createVSchema(
      { users: shardedTable('id', hashVindex()) },
      shards,
      {
        defaultShard: shardId,
        settings: { maxParallelShards: 8 },
      }
    );

    expect(vschema.defaultShard).toBe(shardId);
    expect(vschema.settings!.maxParallelShards).toBe(8);
  });

  it('should create VSchema with multiple shards and replicas', () => {
    const shards: ShardConfig[] = [
      shard(createShardId('shard-1'), 'do-ns', {
        replicas: [
          replica('r1a', 'do-ns-replica', 'replica', { region: 'us-west' }),
          replica('r1b', 'do-ns-replica', 'replica', { region: 'eu-central' }),
        ],
      }),
      shard(createShardId('shard-2'), 'do-ns', {
        replicas: [
          replica('r2a', 'do-ns-replica', 'replica', { region: 'us-east' }),
        ],
      }),
    ];

    const vschema = createVSchema(
      { users: shardedTable('id', hashVindex()) },
      shards
    );

    expect(vschema.shards).toHaveLength(2);
    expect(vschema.shards[0]!.replicas).toHaveLength(2);
    expect(vschema.shards[1]!.replicas).toHaveLength(1);
  });

  it('should create VSchema with empty tables', () => {
    const shards = [shard(createShardId('shard-1'), 'do-ns')];
    const vschema = createVSchema({}, shards);

    expect(Object.keys(vschema.tables)).toHaveLength(0);
  });

  it('should return correct type structure', () => {
    const shards = [shard(createShardId('shard-1'), 'do-ns')];
    const vschema: VSchema = createVSchema(
      { users: shardedTable('id', hashVindex()) },
      shards
    );

    expect(vschema).toHaveProperty('tables');
    expect(vschema).toHaveProperty('shards');
  });

  it('should preserve table type information', () => {
    const shards = [shard(createShardId('shard-1'), 'do-ns')];
    const vschema = createVSchema(
      {
        users: shardedTable('tenant_id', hashVindex()),
      },
      shards
    );

    // TypeScript should infer the correct type
    const usersConfig = vschema.tables.users;
    expect(usersConfig.type).toBe('sharded');
    if (usersConfig.type === 'sharded') {
      expect(usersConfig.shardKey).toBe('tenant_id');
    }
  });
});

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

describe('Integration - Complete VSchema Configuration', () => {
  it('should create a production-like VSchema', () => {
    const shards: ShardConfig[] = Array.from({ length: 4 }, (_, i) =>
      shard(createShardId(`shard-${i + 1}`), 'user-do', {
        replicas: [
          replica(`replica-${i + 1}-a`, 'user-do-replica', 'replica', {
            region: 'us-west',
            weight: 100,
          }),
          replica(`replica-${i + 1}-b`, 'user-do-replica', 'replica', {
            region: 'us-east',
            weight: 100,
          }),
          replica(`analytics-${i + 1}`, 'user-do-analytics', 'analytics'),
        ],
        metadata: { datacenter: `dc-${i + 1}` },
      })
    );

    const vschema = createVSchema(
      {
        users: shardedTable('tenant_id', hashVindex()),
        orders: shardedTable('user_id', consistentHashVindex(200)),
        events: shardedTable('event_id', rangeVindex([
          { shard: shards[0]!.id, min: 0, max: 1000000 },
          { shard: shards[1]!.id, min: 1000000, max: 2000000 },
          { shard: shards[2]!.id, min: 2000000, max: 3000000 },
          { shard: shards[3]!.id, min: 3000000, max: null },
        ])),
        app_config: unshardedTable(shards[0]!.id),
        countries: referenceTable(true),
        currencies: referenceTable(false),
      },
      shards,
      {
        defaultShard: shards[0]!.id,
        settings: {
          defaultVindexType: 'hash',
          autoDetectShardKey: false,
          maxParallelShards: 16,
          shardTimeoutMs: 5000,
          enableCaching: true,
        },
      }
    );

    // Verify structure
    expect(vschema.shards).toHaveLength(4);
    expect(Object.keys(vschema.tables)).toHaveLength(6);

    // Verify sharded tables
    expect(vschema.tables.users.type).toBe('sharded');
    expect(vschema.tables.orders.type).toBe('sharded');
    expect(vschema.tables.events.type).toBe('sharded');

    // Verify unsharded table
    expect(vschema.tables.app_config.type).toBe('unsharded');

    // Verify reference tables
    expect(vschema.tables.countries.type).toBe('reference');
    expect(vschema.tables.currencies.type).toBe('reference');
    if (vschema.tables.countries.type === 'reference') {
      expect(vschema.tables.countries.readOnly).toBe(true);
    }
    if (vschema.tables.currencies.type === 'reference') {
      expect(vschema.tables.currencies.readOnly).toBe(false);
    }

    // Verify replicas
    expect(vschema.shards[0]!.replicas).toHaveLength(3);
    const analyticsReplica = vschema.shards[0]!.replicas!.find(r => r.role === 'analytics');
    expect(analyticsReplica).toBeDefined();

    // Verify settings
    expect(vschema.settings!.maxParallelShards).toBe(16);
    expect(vschema.defaultShard).toBe(shards[0]!.id);
  });

  it('should allow empty shards (edge case)', () => {
    const vschema = createVSchema(
      { users: shardedTable('id', hashVindex()) },
      []
    );

    expect(vschema.shards).toHaveLength(0);
  });
});
