/**
 * Iceberg Integration Spike Tests
 *
 * Demonstrates:
 * 1. DoSQL schema -> Iceberg schema conversion
 * 2. Iceberg schema -> DoSQL schema conversion
 * 3. REST API endpoint structure
 * 4. Client/Server dual mode
 */

import { describe, it, expect } from 'vitest';
import {
  dosqlSchemaToIceberg,
  icebergSchemaToDosql,
  dosqlTypeToIceberg,
  icebergTypeToDosql,
  DoSQLIceberg,
  createIcebergClient,
  createIcebergServer,
  createIcebergDual,
  type DoSQLColumnType,
  type DoSQLTableSchema,
} from './spike.js';
import type {
  Snapshot,
  DataFile,
  ManifestFile,
  DoSQLTableMetadata,
  IcebergRestEndpoints,
} from './types.js';
import {
  generateSnapshotId,
  partitionPath,
  metadataFilePath,
  manifestListPath,
  dataFilePath,
} from './types.js';

describe('DoSQL to Iceberg Schema Conversion', () => {
  it('converts simple DoSQL schema to Iceberg schema', () => {
    const dosqlSchema: DoSQLTableSchema = {
      id: 'number',
      name: 'string',
      active: 'boolean',
      createdAt: 'Date',
    };

    const icebergSchema = dosqlSchemaToIceberg(dosqlSchema);

    expect(icebergSchema.type).toBe('struct');
    expect(icebergSchema['schema-id']).toBe(0);
    expect(icebergSchema.fields).toHaveLength(4);

    // Check field mapping
    expect(icebergSchema.fields[0]).toEqual({
      id: 1,
      name: 'id',
      type: 'double',
      required: true,
    });

    expect(icebergSchema.fields[1]).toEqual({
      id: 2,
      name: 'name',
      type: 'string',
      required: true,
    });

    expect(icebergSchema.fields[2]).toEqual({
      id: 3,
      name: 'active',
      type: 'boolean',
      required: true,
    });

    expect(icebergSchema.fields[3]).toEqual({
      id: 4,
      name: 'createdAt',
      type: 'timestamptz',
      required: true,
    });
  });

  it('handles nullable (null type) columns', () => {
    const dosqlSchema: DoSQLTableSchema = {
      id: 'number',
      metadata: 'null', // nullable column
    };

    const icebergSchema = dosqlSchemaToIceberg(dosqlSchema);

    expect(icebergSchema.fields[1]).toEqual({
      id: 2,
      name: 'metadata',
      type: 'string',
      required: false, // null type becomes optional
    });
  });

  it('supports custom schema ID and identifier fields', () => {
    const dosqlSchema: DoSQLTableSchema = {
      pk: 'number',
      data: 'string',
    };

    const icebergSchema = dosqlSchemaToIceberg(dosqlSchema, {
      schemaId: 5,
      identifierFieldIds: [1], // pk is identifier
    });

    expect(icebergSchema['schema-id']).toBe(5);
    expect(icebergSchema['identifier-field-ids']).toEqual([1]);
  });
});

describe('Iceberg to DoSQL Schema Conversion', () => {
  it('converts Iceberg schema to DoSQL schema', () => {
    const icebergSchema = {
      type: 'struct' as const,
      fields: [
        { id: 1, name: 'id', type: 'long', required: true },
        { id: 2, name: 'name', type: 'string', required: true },
        { id: 3, name: 'price', type: 'decimal(10,2)', required: true },
        { id: 4, name: 'timestamp', type: 'timestamptz', required: true },
        { id: 5, name: 'data', type: 'binary', required: false },
      ],
    };

    const dosqlSchema = icebergSchemaToDosql(icebergSchema);

    expect(dosqlSchema).toEqual({
      id: 'number',
      name: 'string',
      price: 'number',
      timestamp: 'Date',
      data: 'unknown',
    });
  });

  it('handles complex types as unknown', () => {
    const icebergSchema = {
      type: 'struct' as const,
      fields: [
        { id: 1, name: 'id', type: 'long', required: true },
        {
          id: 2,
          name: 'tags',
          type: { type: 'list' as const, 'element-id': 3, element: 'string', 'element-required': true },
          required: true,
        },
        {
          id: 4,
          name: 'metadata',
          type: {
            type: 'map' as const,
            'key-id': 5,
            key: 'string',
            'value-id': 6,
            value: 'string',
            'value-required': true,
          },
          required: true,
        },
      ],
    };

    const dosqlSchema = icebergSchemaToDosql(icebergSchema);

    expect(dosqlSchema).toEqual({
      id: 'number',
      tags: 'unknown', // list becomes unknown
      metadata: 'unknown', // map becomes unknown
    });
  });
});

describe('Type Mapping Functions', () => {
  it('maps DoSQL types to Iceberg types correctly', () => {
    const mappings: Array<[DoSQLColumnType, string]> = [
      ['string', 'string'],
      ['number', 'double'],
      ['boolean', 'boolean'],
      ['Date', 'timestamptz'],
      ['null', 'string'],
      ['unknown', 'binary'],
    ];

    for (const [dosql, iceberg] of mappings) {
      expect(dosqlTypeToIceberg(dosql)).toBe(iceberg);
    }
  });

  it('maps Iceberg types to DoSQL types correctly', () => {
    const mappings: Array<[string, DoSQLColumnType]> = [
      ['boolean', 'boolean'],
      ['int', 'number'],
      ['long', 'number'],
      ['float', 'number'],
      ['double', 'number'],
      ['decimal(10,2)', 'number'],
      ['string', 'string'],
      ['uuid', 'string'],
      ['time', 'string'],
      ['timestamp', 'Date'],
      ['timestamptz', 'Date'],
      ['date', 'Date'],
      ['binary', 'unknown'],
      ['fixed[16]', 'unknown'],
    ];

    for (const [iceberg, dosql] of mappings) {
      expect(icebergTypeToDosql(iceberg)).toBe(dosql);
    }
  });
});

describe('DoSQLIceberg Class', () => {
  describe('Client Mode', () => {
    it('creates client-only instance', () => {
      const iceberg = createIcebergClient('https://catalog.example.com/v1');

      expect(iceberg.hasClient).toBe(true);
      expect(iceberg.hasServer).toBe(false);
      expect(iceberg.client).toBeDefined();
    });

    it('creates client with authentication', () => {
      const iceberg = createIcebergClient('https://catalog.example.com/v1', {
        catalogName: 'production',
        token: 'secret-token',
      });

      expect(iceberg.hasClient).toBe(true);
      expect(iceberg.client).toBeDefined();
    });
  });

  describe('Server Mode', () => {
    it('creates server-only instance', () => {
      const iceberg = createIcebergServer('r2://my-bucket/warehouse');

      expect(iceberg.hasClient).toBe(false);
      expect(iceberg.hasServer).toBe(true);
    });

    it('provides REST endpoint structure', () => {
      const iceberg = createIcebergServer('r2://my-bucket/warehouse');
      const endpoints = iceberg.getRestEndpoints();

      // Verify endpoint structure
      expect(endpoints['GET /v1/config']).toBeDefined();
      expect(endpoints['GET /v1/namespaces']).toBeDefined();
      expect(endpoints['POST /v1/namespaces']).toBeDefined();
      expect(endpoints['GET /v1/namespaces/:namespace']).toBeDefined();
      expect(endpoints['DELETE /v1/namespaces/:namespace']).toBeDefined();
      expect(endpoints['GET /v1/namespaces/:namespace/tables']).toBeDefined();
      expect(endpoints['POST /v1/namespaces/:namespace/tables']).toBeDefined();
      expect(endpoints['GET /v1/namespaces/:namespace/tables/:table']).toBeDefined();
      expect(endpoints['POST /v1/namespaces/:namespace/tables/:table']).toBeDefined();
      expect(endpoints['DELETE /v1/namespaces/:namespace/tables/:table']).toBeDefined();
    });

    it('config endpoint returns warehouse location', () => {
      const iceberg = createIcebergServer('r2://my-bucket/warehouse');
      const endpoints = iceberg.getRestEndpoints();

      const config = endpoints['GET /v1/config']();

      expect(config.defaults['warehouse-location']).toBe('r2://my-bucket/warehouse');
    });
  });

  describe('Dual Mode', () => {
    it('creates dual client+server instance', () => {
      const iceberg = createIcebergDual({
        client: {
          baseUrl: 'https://external-catalog.example.com/v1',
        },
        server: {
          warehouseLocation: 'r2://my-bucket/warehouse',
        },
      });

      expect(iceberg.hasClient).toBe(true);
      expect(iceberg.hasServer).toBe(true);
    });
  });

  describe('Table Creation', () => {
    it('generates Iceberg table creation request from DoSQL schema', () => {
      const iceberg = createIcebergServer('r2://my-bucket/warehouse');

      const dosqlSchema: DoSQLTableSchema = {
        id: 'number',
        name: 'string',
        createdAt: 'Date',
      };

      const request = iceberg.createTableRequest('users', dosqlSchema, {
        properties: {
          'write.parquet.compression-codec': 'zstd',
        },
      });

      expect(request.name).toBe('users');
      expect(request.schema.type).toBe('struct');
      expect(request.schema.fields).toHaveLength(3);
      expect(request.properties).toEqual({
        'created-by': 'dosql',
        'write.format.default': 'parquet',
        'write.parquet.compression-codec': 'zstd',
      });
    });
  });
});

describe('Utility Functions', () => {
  it('generates unique snapshot IDs', () => {
    const id1 = generateSnapshotId();
    const id2 = generateSnapshotId();

    expect(typeof id1).toBe('bigint');
    expect(typeof id2).toBe('bigint');
    expect(id1).not.toBe(id2);
  });

  it('generates partition paths', () => {
    const spec = {
      'spec-id': 0,
      fields: [
        { source_id: 1, field_id: 1000, name: 'year', transform: 'year' },
        { source_id: 2, field_id: 1001, name: 'month', transform: 'month' },
      ],
    };

    const path = partitionPath(spec, { year: 2024, month: 1 });
    expect(path).toBe('year=2024/month=1');
  });

  it('handles null partition values', () => {
    const spec = {
      'spec-id': 0,
      fields: [{ source_id: 1, field_id: 1000, name: 'region', transform: 'identity' }],
    };

    const path = partitionPath(spec, { region: null });
    expect(path).toBe('region=__HIVE_DEFAULT_PARTITION__');
  });

  it('generates metadata file paths', () => {
    const path = metadataFilePath('r2://bucket/warehouse/db/table', 5);
    expect(path).toBe('r2://bucket/warehouse/db/table/metadata/v5.metadata.json');
  });

  it('generates manifest list paths', () => {
    const path = manifestListPath('r2://bucket/warehouse/db/table', 12345678901234n);
    expect(path).toBe('r2://bucket/warehouse/db/table/metadata/snap-12345678901234-manifest-list.avro');
  });

  it('generates data file paths without partition', () => {
    const path = dataFilePath('r2://bucket/warehouse/db/table', null, '00001.parquet');
    expect(path).toBe('r2://bucket/warehouse/db/table/data/00001.parquet');
  });

  it('generates data file paths with partition', () => {
    const path = dataFilePath('r2://bucket/warehouse/db/table', 'year=2024/month=1', '00001.parquet');
    expect(path).toBe('r2://bucket/warehouse/db/table/data/year=2024/month=1/00001.parquet');
  });
});

describe('REST API Endpoint Structure', () => {
  /**
   * This test documents the expected REST API structure that DoSQL
   * would expose when acting as an Iceberg REST Catalog server.
   *
   * External engines (DuckDB, Spark, Trino) would connect to these endpoints.
   */
  it('documents expected REST API endpoints', () => {
    const expectedEndpoints = {
      // Configuration
      'GET /v1/config': 'Returns catalog configuration (warehouse location)',

      // Namespaces (databases)
      'GET /v1/namespaces': 'List all namespaces',
      'POST /v1/namespaces': 'Create a namespace',
      'GET /v1/namespaces/:namespace': 'Get namespace details',
      'DELETE /v1/namespaces/:namespace': 'Drop a namespace',

      // Tables
      'GET /v1/namespaces/:namespace/tables': 'List tables in namespace',
      'POST /v1/namespaces/:namespace/tables': 'Create a table',
      'GET /v1/namespaces/:namespace/tables/:table': 'Load table metadata',
      'POST /v1/namespaces/:namespace/tables/:table': 'Commit changes to table',
      'DELETE /v1/namespaces/:namespace/tables/:table': 'Drop a table',

      // Table rename
      'POST /v1/tables/rename': 'Rename a table',
    };

    // This is a documentation test - verify the endpoints are documented
    expect(Object.keys(expectedEndpoints).length).toBeGreaterThan(0);
  });

  /**
   * This test shows how DoSQL would expose time-travel queries via snapshots.
   */
  it('documents snapshot-based time travel', () => {
    const exampleSnapshot: Snapshot = {
      'snapshot-id': 1234567890123n,
      'parent-snapshot-id': 1234567890122n,
      'sequence-number': 5n,
      'timestamp-ms': 1706054400000n, // 2024-01-24T00:00:00Z
      'manifest-list': 'r2://bucket/warehouse/db/table/metadata/snap-1234567890123-manifest-list.avro',
      summary: {
        operation: 'append',
        'added-data-files': '10',
        'added-records': '1000000',
        'total-records': '5000000',
      },
      'schema-id': 0,
    };

    // Snapshots enable:
    // 1. Time travel queries: SELECT * FROM table AT SNAPSHOT 1234567890123
    // 2. Historical queries: SELECT * FROM table AT TIMESTAMP '2024-01-24'
    // 3. Incremental reads: Read only changes since parent snapshot

    expect(exampleSnapshot['snapshot-id']).toBeDefined();
    expect(exampleSnapshot['parent-snapshot-id']).toBeDefined();
    expect(exampleSnapshot['manifest-list']).toContain('.avro');
  });

  /**
   * This test shows the data file structure DoSQL would write.
   */
  it('documents data file structure', () => {
    const exampleDataFile: DataFile = {
      content: 0, // data (not deletes)
      'file-path': 'r2://bucket/warehouse/db/events/data/year=2024/month=1/00001.parquet',
      'file-format': 'parquet',
      partition: { year: 2024, month: 1 },
      'record-count': 100000n,
      'file-size-in-bytes': 52428800n, // 50MB
      'sort-order-id': 0,
    };

    // Data files are:
    // 1. Written by DoSQL using hyparquet-writer
    // 2. Stored in R2 with Hive-style partition paths
    // 3. Tracked in manifest files for discovery

    expect(exampleDataFile['file-format']).toBe('parquet');
    expect(exampleDataFile['file-path']).toContain('year=2024');
    expect(exampleDataFile.partition).toEqual({ year: 2024, month: 1 });
  });
});
