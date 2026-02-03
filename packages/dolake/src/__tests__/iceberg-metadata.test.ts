/**
 * DoLake Iceberg Metadata Management Tests
 *
 * Tests for Iceberg metadata management including:
 * - Schema creation and evolution
 * - Partition spec creation
 * - Sort order creation
 * - Snapshot creation and management
 * - Table metadata lifecycle
 * - Path generation utilities
 * - Manifest file creation
 *
 * Uses workers-vitest-pool (NO MOCKS).
 *
 * @packageDocumentation
 */

import { describe, it, expect } from 'vitest';
import { generateUUID } from '../types.js';
import {
  // Schema builders
  createSchema,
  addSchemaField,
  // Partition spec builders
  createUnpartitionedSpec,
  createIdentityPartitionSpec,
  createDatePartitionSpec,
  // Sort order builders
  createUnsortedOrder,
  createSortOrder,
  // Snapshot builders
  createAppendSnapshot,
  // Manifest builders
  createManifestFile,
  // Table metadata
  createTableMetadata,
  addSnapshot,
  addSchema,
  // Path utilities
  metadataFilePath,
  manifestListPath,
  manifestFilePath,
  dataFilePath,
  partitionToPath,
} from '../iceberg.js';

// =============================================================================
// Schema Builder Tests
// =============================================================================

describe('Schema Builder', () => {
  it('should create schema with basic fields', () => {
    const schema = createSchema([
      { name: 'id', type: 'long', required: true },
      { name: 'name', type: 'string' },
    ]);

    expect(schema.type).toBe('struct');
    expect(schema['schema-id']).toBe(0);
    expect(schema.fields).toHaveLength(2);
    expect(schema.fields[0].id).toBe(1);
    expect(schema.fields[0].name).toBe('id');
    expect(schema.fields[0].type).toBe('long');
    expect(schema.fields[0].required).toBe(true);
    expect(schema.fields[1].required).toBe(false);
  });

  it('should create schema with custom schema ID', () => {
    const schema = createSchema(
      [{ name: 'id', type: 'long' }],
      5
    );
    expect(schema['schema-id']).toBe(5);
  });

  it('should create schema with identifier field IDs', () => {
    const schema = createSchema(
      [
        { name: 'id', type: 'long', required: true },
        { name: 'name', type: 'string' },
      ],
      0,
      [1]
    );
    expect(schema['identifier-field-ids']).toEqual([1]);
  });

  it('should create schema with doc fields', () => {
    const schema = createSchema([
      { name: 'id', type: 'long', doc: 'Primary key' },
    ]);
    expect(schema.fields[0].doc).toBe('Primary key');
  });

  it('should assign sequential field IDs starting from 1', () => {
    const schema = createSchema([
      { name: 'a', type: 'int' },
      { name: 'b', type: 'string' },
      { name: 'c', type: 'boolean' },
    ]);

    expect(schema.fields[0].id).toBe(1);
    expect(schema.fields[1].id).toBe(2);
    expect(schema.fields[2].id).toBe(3);
  });

  it('should handle empty fields array', () => {
    const schema = createSchema([]);
    expect(schema.fields).toHaveLength(0);
    expect(schema.type).toBe('struct');
  });
});

// =============================================================================
// Schema Evolution Tests
// =============================================================================

describe('Schema Evolution', () => {
  it('should add a field to existing schema', () => {
    const originalSchema = createSchema([
      { name: 'id', type: 'long', required: true },
    ]);

    const evolved = addSchemaField(
      originalSchema,
      { name: 'email', type: 'string' },
      1
    );

    expect(evolved['schema-id']).toBe(1);
    expect(evolved.fields).toHaveLength(2);
    expect(evolved.fields[1].name).toBe('email');
    expect(evolved.fields[1].id).toBe(2); // maxId(1) + 1
  });

  it('should preserve original schema fields when evolving', () => {
    const originalSchema = createSchema([
      { name: 'id', type: 'long', required: true },
      { name: 'name', type: 'string' },
    ]);

    const evolved = addSchemaField(
      originalSchema,
      { name: 'age', type: 'int' },
      1
    );

    expect(evolved.fields[0].name).toBe('id');
    expect(evolved.fields[1].name).toBe('name');
    expect(evolved.fields[2].name).toBe('age');
  });

  it('should increment field ID based on max existing ID', () => {
    const originalSchema = createSchema([
      { name: 'a', type: 'int' },
      { name: 'b', type: 'int' },
      { name: 'c', type: 'int' },
    ]);

    // Max ID should be 3
    const evolved = addSchemaField(
      originalSchema,
      { name: 'd', type: 'int' },
      1
    );

    expect(evolved.fields[3].id).toBe(4);
  });

  it('should set required flag on new field', () => {
    const originalSchema = createSchema([
      { name: 'id', type: 'long' },
    ]);

    const evolved = addSchemaField(
      originalSchema,
      { name: 'required_field', type: 'string', required: true },
      1
    );

    expect(evolved.fields[1].required).toBe(true);
  });

  it('should default new field to not required', () => {
    const originalSchema = createSchema([
      { name: 'id', type: 'long' },
    ]);

    const evolved = addSchemaField(
      originalSchema,
      { name: 'optional_field', type: 'string' },
      1
    );

    expect(evolved.fields[1].required).toBe(false);
  });
});

// =============================================================================
// Partition Spec Tests
// =============================================================================

describe('Partition Spec', () => {
  it('should create unpartitioned spec', () => {
    const spec = createUnpartitionedSpec();
    expect(spec['spec-id']).toBe(0);
    expect(spec.fields).toHaveLength(0);
  });

  it('should create identity partition spec', () => {
    const spec = createIdentityPartitionSpec([
      { sourceId: 1, name: 'region' },
    ]);

    expect(spec['spec-id']).toBe(0);
    expect(spec.fields).toHaveLength(1);
    expect(spec.fields[0]['source-id']).toBe(1);
    expect(spec.fields[0].name).toBe('region');
    expect(spec.fields[0].transform).toBe('identity');
    expect(spec.fields[0]['field-id']).toBe(1000);
  });

  it('should create identity partition spec with multiple fields', () => {
    const spec = createIdentityPartitionSpec([
      { sourceId: 1, name: 'region' },
      { sourceId: 2, name: 'category' },
    ]);

    expect(spec.fields).toHaveLength(2);
    expect(spec.fields[0]['field-id']).toBe(1000);
    expect(spec.fields[1]['field-id']).toBe(1001);
  });

  it('should create date partition spec with year transform', () => {
    const spec = createDatePartitionSpec(1, 'year', 'created_year');

    expect(spec.fields).toHaveLength(1);
    expect(spec.fields[0].transform).toBe('year');
    expect(spec.fields[0].name).toBe('created_year');
    expect(spec.fields[0]['source-id']).toBe(1);
  });

  it('should create date partition spec with month transform', () => {
    const spec = createDatePartitionSpec(2, 'month', 'event_month');
    expect(spec.fields[0].transform).toBe('month');
  });

  it('should create date partition spec with day transform', () => {
    const spec = createDatePartitionSpec(3, 'day', 'log_day');
    expect(spec.fields[0].transform).toBe('day');
  });

  it('should create date partition spec with hour transform', () => {
    const spec = createDatePartitionSpec(4, 'hour', 'metric_hour');
    expect(spec.fields[0].transform).toBe('hour');
  });

  it('should create partition spec with custom spec ID', () => {
    const spec = createIdentityPartitionSpec(
      [{ sourceId: 1, name: 'region' }],
      7
    );
    expect(spec['spec-id']).toBe(7);
  });
});

// =============================================================================
// Sort Order Tests
// =============================================================================

describe('Sort Order', () => {
  it('should create unsorted order', () => {
    const order = createUnsortedOrder();
    expect(order['order-id']).toBe(0);
    expect(order.fields).toHaveLength(0);
  });

  it('should create ascending sort order', () => {
    const order = createSortOrder([
      { sourceId: 1, direction: 'asc' },
    ]);

    expect(order['order-id']).toBe(0);
    expect(order.fields).toHaveLength(1);
    expect(order.fields[0].direction).toBe('asc');
    expect(order.fields[0]['null-order']).toBe('nulls-first');
    expect(order.fields[0].transform).toBe('identity');
  });

  it('should create descending sort order with nulls-last', () => {
    const order = createSortOrder([
      { sourceId: 1, direction: 'desc' },
    ]);

    expect(order.fields[0].direction).toBe('desc');
    expect(order.fields[0]['null-order']).toBe('nulls-last');
  });

  it('should create sort order with custom null order', () => {
    const order = createSortOrder([
      { sourceId: 1, direction: 'asc', nullOrder: 'nulls-last' },
    ]);

    expect(order.fields[0]['null-order']).toBe('nulls-last');
  });

  it('should create sort order with custom transform', () => {
    const order = createSortOrder([
      { sourceId: 1, direction: 'asc', transform: 'day' },
    ]);

    expect(order.fields[0].transform).toBe('day');
  });

  it('should create multi-column sort order', () => {
    const order = createSortOrder([
      { sourceId: 1, direction: 'asc' },
      { sourceId: 2, direction: 'desc' },
    ]);

    expect(order.fields).toHaveLength(2);
    expect(order.fields[0].direction).toBe('asc');
    expect(order.fields[1].direction).toBe('desc');
  });

  it('should create sort order with custom order ID', () => {
    const order = createSortOrder(
      [{ sourceId: 1, direction: 'asc' }],
      42
    );
    expect(order['order-id']).toBe(42);
  });
});

// =============================================================================
// Snapshot Builder Tests
// =============================================================================

describe('Snapshot Builder', () => {
  it('should create append snapshot with no parent', () => {
    const snapshot = createAppendSnapshot(
      null,
      BigInt(1),
      'metadata/snap-1-manifest-list.avro',
      5,
      BigInt(1000),
      BigInt(50000)
    );

    expect(snapshot['parent-snapshot-id']).toBeNull();
    expect(snapshot['sequence-number']).toBe(BigInt(1));
    expect(snapshot['manifest-list']).toBe('metadata/snap-1-manifest-list.avro');
    expect(snapshot.summary.operation).toBe('append');
    expect(snapshot.summary['added-data-files']).toBe('5');
    expect(snapshot.summary['added-records']).toBe('1000');
    expect(snapshot.summary['added-files-size']).toBe('50000');
  });

  it('should create append snapshot with parent', () => {
    const parentId = BigInt(12345);
    const snapshot = createAppendSnapshot(
      parentId,
      BigInt(2),
      'metadata/snap-2-manifest-list.avro',
      3,
      BigInt(500),
      BigInt(25000)
    );

    expect(snapshot['parent-snapshot-id']).toBe(parentId);
  });

  it('should generate unique snapshot IDs', () => {
    const snap1 = createAppendSnapshot(null, BigInt(1), 'path1', 1, BigInt(1), BigInt(1));
    const snap2 = createAppendSnapshot(null, BigInt(2), 'path2', 1, BigInt(1), BigInt(1));

    expect(snap1['snapshot-id']).not.toBe(snap2['snapshot-id']);
  });

  it('should include schema-id when provided', () => {
    const snapshot = createAppendSnapshot(
      null,
      BigInt(1),
      'metadata/snap.avro',
      1,
      BigInt(100),
      BigInt(5000),
      3
    );

    expect(snapshot['schema-id']).toBe(3);
  });

  it('should set timestamp to current time', () => {
    const before = BigInt(Date.now());
    const snapshot = createAppendSnapshot(null, BigInt(1), 'path', 1, BigInt(1), BigInt(1));
    const after = BigInt(Date.now());

    expect(snapshot['timestamp-ms']).toBeGreaterThanOrEqual(before);
    expect(snapshot['timestamp-ms']).toBeLessThanOrEqual(after);
  });
});

// =============================================================================
// Manifest File Builder Tests
// =============================================================================

describe('Manifest File Builder', () => {
  it('should create manifest file entry', () => {
    const snapshotId = BigInt(12345);
    const seqNum = BigInt(5);

    const manifest = createManifestFile(
      'metadata/test-manifest.avro',
      snapshotId,
      seqNum,
      10,
      BigInt(5000)
    );

    expect(manifest['manifest-path']).toBe('metadata/test-manifest.avro');
    expect(manifest['manifest-length']).toBe(BigInt(0)); // Set after writing
    expect(manifest['partition-spec-id']).toBe(0);
    expect(manifest.content).toBe('data');
    expect(manifest['sequence-number']).toBe(seqNum);
    expect(manifest['min-sequence-number']).toBe(seqNum);
    expect(manifest['added-snapshot-id']).toBe(snapshotId);
    expect(manifest['added-files-count']).toBe(10);
    expect(manifest['existing-files-count']).toBe(0);
    expect(manifest['deleted-files-count']).toBe(0);
    expect(manifest['added-rows-count']).toBe(BigInt(5000));
    expect(manifest['existing-rows-count']).toBe(BigInt(0));
    expect(manifest['deleted-rows-count']).toBe(BigInt(0));
  });

  it('should accept custom partition spec ID', () => {
    const manifest = createManifestFile(
      'metadata/m.avro',
      BigInt(1),
      BigInt(1),
      1,
      BigInt(100),
      7
    );
    expect(manifest['partition-spec-id']).toBe(7);
  });
});

// =============================================================================
// Table Metadata Tests
// =============================================================================

describe('Table Metadata', () => {
  it('should create initial table metadata', () => {
    const schema = createSchema([
      { name: 'id', type: 'long', required: true },
      { name: 'name', type: 'string' },
    ]);

    const tableUuid = generateUUID();
    const metadata = createTableMetadata(
      tableUuid,
      '/warehouse/db/test_table',
      schema
    );

    expect(metadata['format-version']).toBe(2);
    expect(metadata['table-uuid']).toBe(tableUuid);
    expect(metadata.location).toBe('/warehouse/db/test_table');
    expect(metadata['last-sequence-number']).toBe(BigInt(0));
    expect(metadata['current-schema-id']).toBe(0);
    expect(metadata.schemas).toHaveLength(1);
    expect(metadata['default-spec-id']).toBe(0);
    expect(metadata['partition-specs']).toHaveLength(1);
    expect(metadata['default-sort-order-id']).toBe(0);
    expect(metadata['sort-orders']).toHaveLength(1);
    expect(metadata['current-snapshot-id']).toBeNull();
    expect(metadata.snapshots).toHaveLength(0);
    expect(metadata.properties?.['created-by']).toBe('dolake');
    expect(metadata.properties?.['write.format.default']).toBe('parquet');
  });

  it('should create table metadata with custom partition spec', () => {
    const schema = createSchema([
      { name: 'id', type: 'long' },
      { name: 'region', type: 'string' },
    ]);
    const spec = createIdentityPartitionSpec([{ sourceId: 2, name: 'region' }]);

    const metadata = createTableMetadata(generateUUID(), '/warehouse/test', schema, spec);

    expect(metadata['partition-specs']).toHaveLength(1);
    expect(metadata['partition-specs'][0].fields).toHaveLength(1);
    expect(metadata['partition-specs'][0].fields[0].name).toBe('region');
  });

  it('should create table metadata with custom sort order', () => {
    const schema = createSchema([{ name: 'id', type: 'long' }]);
    const sortOrder = createSortOrder([{ sourceId: 1, direction: 'asc' }]);

    const metadata = createTableMetadata(
      generateUUID(),
      '/warehouse/test',
      schema,
      createUnpartitionedSpec(),
      sortOrder
    );

    expect(metadata['sort-orders']).toHaveLength(1);
    expect(metadata['sort-orders'][0].fields).toHaveLength(1);
  });

  it('should create table metadata with custom properties', () => {
    const schema = createSchema([{ name: 'id', type: 'long' }]);
    const metadata = createTableMetadata(
      generateUUID(),
      '/warehouse/test',
      schema,
      createUnpartitionedSpec(),
      createUnsortedOrder(),
      { 'custom-key': 'custom-value' }
    );

    expect(metadata.properties?.['custom-key']).toBe('custom-value');
    // Should also have default properties
    expect(metadata.properties?.['created-by']).toBe('dolake');
  });

  it('should set last-column-id from schema', () => {
    const schema = createSchema([
      { name: 'a', type: 'int' },
      { name: 'b', type: 'int' },
      { name: 'c', type: 'int' },
    ]);

    const metadata = createTableMetadata(generateUUID(), '/warehouse/test', schema);
    expect(metadata['last-column-id']).toBe(3);
  });

  it('should initialize refs with main branch', () => {
    const schema = createSchema([{ name: 'id', type: 'long' }]);
    const metadata = createTableMetadata(generateUUID(), '/warehouse/test', schema);

    expect(metadata.refs).toBeDefined();
    expect(metadata.refs!.main).toBeDefined();
    expect(metadata.refs!.main.type).toBe('branch');
  });
});

// =============================================================================
// Add Snapshot Tests
// =============================================================================

describe('Add Snapshot to Metadata', () => {
  it('should add a snapshot and update sequence number', () => {
    const schema = createSchema([{ name: 'id', type: 'long' }]);
    const metadata = createTableMetadata(generateUUID(), '/warehouse/test', schema);

    const snapshot = createAppendSnapshot(
      null,
      BigInt(1),
      'metadata/snap.avro',
      1,
      BigInt(100),
      BigInt(5000)
    );

    const updated = addSnapshot(metadata, snapshot);

    expect(updated['last-sequence-number']).toBe(BigInt(1));
    expect(updated.snapshots).toHaveLength(1);
    expect(updated['current-snapshot-id']).toBe(snapshot['snapshot-id']);
    expect(updated['snapshot-log']).toHaveLength(1);
    expect(updated.refs!.main['snapshot-id']).toBe(snapshot['snapshot-id']);
  });

  it('should increment sequence number for each snapshot', () => {
    const schema = createSchema([{ name: 'id', type: 'long' }]);
    let metadata = createTableMetadata(generateUUID(), '/warehouse/test', schema);

    const snap1 = createAppendSnapshot(null, BigInt(1), 'snap1.avro', 1, BigInt(100), BigInt(5000));
    metadata = addSnapshot(metadata, snap1);
    expect(metadata['last-sequence-number']).toBe(BigInt(1));

    const snap2 = createAppendSnapshot(
      snap1['snapshot-id'],
      BigInt(2),
      'snap2.avro',
      2,
      BigInt(200),
      BigInt(10000)
    );
    metadata = addSnapshot(metadata, snap2);
    expect(metadata['last-sequence-number']).toBe(BigInt(2));
  });

  it('should append to snapshot log', () => {
    const schema = createSchema([{ name: 'id', type: 'long' }]);
    let metadata = createTableMetadata(generateUUID(), '/warehouse/test', schema);

    const snap1 = createAppendSnapshot(null, BigInt(1), 'snap1', 1, BigInt(10), BigInt(100));
    metadata = addSnapshot(metadata, snap1);

    const snap2 = createAppendSnapshot(snap1['snapshot-id'], BigInt(2), 'snap2', 1, BigInt(10), BigInt(100));
    metadata = addSnapshot(metadata, snap2);

    expect(metadata['snapshot-log']).toHaveLength(2);
  });

  it('should update last-updated-ms', () => {
    const schema = createSchema([{ name: 'id', type: 'long' }]);
    const metadata = createTableMetadata(generateUUID(), '/warehouse/test', schema);
    const originalUpdateTime = metadata['last-updated-ms'];

    const snapshot = createAppendSnapshot(null, BigInt(1), 'snap', 1, BigInt(10), BigInt(100));
    const updated = addSnapshot(metadata, snapshot);

    expect(updated['last-updated-ms']).toBeGreaterThanOrEqual(originalUpdateTime);
  });
});

// =============================================================================
// Add Schema Tests
// =============================================================================

describe('Add Schema to Metadata', () => {
  it('should add a new schema version', () => {
    const schema1 = createSchema([{ name: 'id', type: 'long' }]);
    const metadata = createTableMetadata(generateUUID(), '/warehouse/test', schema1);

    const schema2 = createSchema(
      [
        { name: 'id', type: 'long' },
        { name: 'email', type: 'string' },
      ],
      1
    );

    const updated = addSchema(metadata, schema2);

    expect(updated.schemas).toHaveLength(2);
    expect(updated['current-schema-id']).toBe(1);
  });

  it('should optionally not set new schema as current', () => {
    const schema1 = createSchema([{ name: 'id', type: 'long' }]);
    const metadata = createTableMetadata(generateUUID(), '/warehouse/test', schema1);

    const schema2 = createSchema([{ name: 'id', type: 'long' }, { name: 'x', type: 'int' }], 1);

    const updated = addSchema(metadata, schema2, false);

    expect(updated.schemas).toHaveLength(2);
    expect(updated['current-schema-id']).toBe(0); // Should remain at 0
  });

  it('should update last-column-id based on new schema', () => {
    const schema1 = createSchema([{ name: 'id', type: 'long' }]); // last-column-id = 1
    const metadata = createTableMetadata(generateUUID(), '/warehouse/test', schema1);

    const schema2 = createSchema(
      [
        { name: 'id', type: 'long' },
        { name: 'name', type: 'string' },
        { name: 'email', type: 'string' },
      ],
      1
    );

    const updated = addSchema(metadata, schema2);
    expect(updated['last-column-id']).toBe(3);
  });
});

// =============================================================================
// Path Utility Tests
// =============================================================================

describe('Path Utilities', () => {
  it('should generate metadata file path', () => {
    const path = metadataFilePath('/warehouse/db/table', 1);
    expect(path).toBe('/warehouse/db/table/metadata/v1.metadata.json');
  });

  it('should generate metadata file path for version > 1', () => {
    const path = metadataFilePath('/warehouse/db/table', 42);
    expect(path).toBe('/warehouse/db/table/metadata/v42.metadata.json');
  });

  it('should generate manifest list path', () => {
    const path = manifestListPath('/warehouse/db/table', BigInt(12345));
    expect(path).toBe('/warehouse/db/table/metadata/snap-12345-manifest-list.avro');
  });

  it('should generate manifest file path', () => {
    const uuid = 'test-uuid-1234';
    const path = manifestFilePath('/warehouse/db/table', uuid);
    expect(path).toBe('/warehouse/db/table/metadata/test-uuid-1234-manifest.avro');
  });

  it('should generate data file path with partition', () => {
    const path = dataFilePath('/warehouse/db/table', 'date=2024-01-15', 'file.parquet');
    expect(path).toBe('/warehouse/db/table/data/date=2024-01-15/file.parquet');
  });

  it('should generate data file path without partition', () => {
    const path = dataFilePath('/warehouse/db/table', null, 'file.parquet');
    expect(path).toBe('/warehouse/db/table/data/file.parquet');
  });

  it('should convert partition to path for single key', () => {
    const path = partitionToPath({ region: 'us-west' });
    expect(path).toBe('region=us-west');
  });

  it('should convert partition to path for multiple keys', () => {
    const path = partitionToPath({ year: 2024, month: 1 });
    expect(path).toBe('year=2024/month=1');
  });

  it('should return null for empty partition', () => {
    const path = partitionToPath({});
    expect(path).toBeNull();
  });

  it('should handle null partition values with HIVE default', () => {
    const path = partitionToPath({ region: null });
    expect(path).toBe('region=__HIVE_DEFAULT_PARTITION__');
  });

  it('should handle undefined partition values with HIVE default', () => {
    const path = partitionToPath({ region: undefined });
    expect(path).toBe('region=__HIVE_DEFAULT_PARTITION__');
  });
});
