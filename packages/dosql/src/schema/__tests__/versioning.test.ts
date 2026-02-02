/**
 * Schema Versioning Tests
 *
 * Tests for schema versioning, evolution, and CDC compatibility.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  SchemaVersionRegistry,
  SchemaAwareCDCProcessor,
  createSchemaVersion,
  getSchemaRegistry,
  resetSchemaRegistry,
  type ColumnDefinition,
  type TableSchema,
  type SchemaChangeEvent,
  type SchemaEnrichedCDCEvent,
} from '../versioning.js';

describe('SchemaVersionRegistry', () => {
  let registry: SchemaVersionRegistry;

  beforeEach(() => {
    registry = new SchemaVersionRegistry();
  });

  describe('registerTable', () => {
    it('should register a new table with initial version 1', () => {
      const schema = registry.registerTable('users', {
        tableName: 'users',
        columns: [
          { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, indexed: true, position: 0, columnId: 0 },
          { name: 'email', type: 'TEXT', nullable: false, primaryKey: false, indexed: true, position: 1, columnId: 0 },
          { name: 'name', type: 'TEXT', nullable: true, primaryKey: false, indexed: false, position: 2, columnId: 0 },
        ],
        indexes: [],
        constraints: [],
        primaryKey: ['id'],
      });

      expect(schema.version).toBe(createSchemaVersion(1));
      expect(schema.tableName).toBe('users');
      expect(schema.columns).toHaveLength(3);
      expect(schema.checksum).toBeTruthy();
    });

    it('should assign unique column IDs', () => {
      const schema = registry.registerTable('posts', {
        tableName: 'posts',
        columns: [
          { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, indexed: true, position: 0, columnId: 0 },
          { name: 'title', type: 'TEXT', nullable: false, primaryKey: false, indexed: false, position: 1, columnId: 0 },
        ],
        indexes: [],
        constraints: [],
        primaryKey: ['id'],
      });

      const columnIds = schema.columns.map(c => c.columnId);
      const uniqueIds = new Set(columnIds);
      expect(uniqueIds.size).toBe(columnIds.length);
    });

    it('should emit CREATE_TABLE change event', () => {
      const events: SchemaChangeEvent[] = [];
      registry.onSchemaChange(event => events.push(event));

      registry.registerTable('users', {
        tableName: 'users',
        columns: [
          { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, indexed: true, position: 0, columnId: 0 },
        ],
        indexes: [],
        constraints: [],
        primaryKey: ['id'],
      });

      expect(events).toHaveLength(1);
      expect(events[0].changeType).toBe('CREATE_TABLE');
      expect(events[0].table).toBe('users');
      expect(events[0].afterVersion).toBe(createSchemaVersion(1));
    });
  });

  describe('getSchema', () => {
    it('should return current schema', () => {
      registry.registerTable('users', {
        tableName: 'users',
        columns: [
          { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, indexed: true, position: 0, columnId: 0 },
        ],
        indexes: [],
        constraints: [],
        primaryKey: ['id'],
      });

      const schema = registry.getSchema('users');
      expect(schema).toBeTruthy();
      expect(schema?.tableName).toBe('users');
    });

    it('should return null for non-existent table', () => {
      const schema = registry.getSchema('nonexistent');
      expect(schema).toBeNull();
    });
  });

  describe('addColumn', () => {
    beforeEach(() => {
      registry.registerTable('users', {
        tableName: 'users',
        columns: [
          { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, indexed: true, position: 0, columnId: 0 },
          { name: 'email', type: 'TEXT', nullable: false, primaryKey: false, indexed: true, position: 1, columnId: 0 },
        ],
        indexes: [],
        constraints: [],
        primaryKey: ['id'],
      });
    });

    it('should add a column and increment version', () => {
      const event = registry.addColumn('users', {
        name: 'created_at',
        type: 'TIMESTAMP',
        nullable: true,
        primaryKey: false,
        indexed: false,
      }, 1n, 'txn-1');

      const schema = registry.getSchema('users');
      expect(schema?.version).toBe(createSchemaVersion(2));
      expect(schema?.columns).toHaveLength(3);
      expect(schema?.columns.find(c => c.name === 'created_at')).toBeTruthy();

      expect(event.changeType).toBe('ADD_COLUMN');
      expect(event.column).toBe('created_at');
    });

    it('should mark nullable column additions as FULL_COMPATIBLE', () => {
      const event = registry.addColumn('users', {
        name: 'bio',
        type: 'TEXT',
        nullable: true,
        primaryKey: false,
        indexed: false,
      }, 1n, 'txn-1');

      expect(event.compatibility).toBe('FULL_COMPATIBLE');
    });

    it('should mark non-nullable column with default as FULL_COMPATIBLE', () => {
      const event = registry.addColumn('users', {
        name: 'status',
        type: 'TEXT',
        nullable: false,
        defaultValue: "'active'",
        primaryKey: false,
        indexed: false,
      }, 1n, 'txn-1');

      expect(event.compatibility).toBe('FULL_COMPATIBLE');
    });

    it('should mark non-nullable column without default as BACKWARD_COMPATIBLE', () => {
      const event = registry.addColumn('users', {
        name: 'required_field',
        type: 'TEXT',
        nullable: false,
        primaryKey: false,
        indexed: false,
      }, 1n, 'txn-1');

      expect(event.compatibility).toBe('BACKWARD_COMPATIBLE');
    });
  });

  describe('dropColumn', () => {
    beforeEach(() => {
      registry.registerTable('users', {
        tableName: 'users',
        columns: [
          { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, indexed: true, position: 0, columnId: 0 },
          { name: 'email', type: 'TEXT', nullable: false, primaryKey: false, indexed: true, position: 1, columnId: 0 },
          { name: 'deprecated_field', type: 'TEXT', nullable: true, primaryKey: false, indexed: false, position: 2, columnId: 0 },
        ],
        indexes: [],
        constraints: [],
        primaryKey: ['id'],
      });
    });

    it('should drop a column and increment version', () => {
      const event = registry.dropColumn('users', 'deprecated_field', 1n, 'txn-1');

      const schema = registry.getSchema('users');
      expect(schema?.version).toBe(createSchemaVersion(2));
      expect(schema?.columns).toHaveLength(2);
      expect(schema?.columns.find(c => c.name === 'deprecated_field')).toBeFalsy();

      expect(event.changeType).toBe('DROP_COLUMN');
      expect(event.column).toBe('deprecated_field');
    });

    it('should mark column drops as BREAKING', () => {
      const event = registry.dropColumn('users', 'deprecated_field', 1n, 'txn-1');
      expect(event.compatibility).toBe('BREAKING');
    });

    it('should preserve old column info in event', () => {
      const event = registry.dropColumn('users', 'deprecated_field', 1n, 'txn-1');
      expect(event.oldColumn).toBeTruthy();
      expect(event.oldColumn?.name).toBe('deprecated_field');
      expect(event.oldColumn?.type).toBe('TEXT');
    });
  });

  describe('renameColumn', () => {
    beforeEach(() => {
      registry.registerTable('users', {
        tableName: 'users',
        columns: [
          { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, indexed: true, position: 0, columnId: 0 },
          { name: 'user_name', type: 'TEXT', nullable: false, primaryKey: false, indexed: false, position: 1, columnId: 0 },
        ],
        indexes: [],
        constraints: [],
        primaryKey: ['id'],
      });
    });

    it('should rename a column and preserve column ID', () => {
      const schemaBefore = registry.getSchema('users')!;
      const oldColumnId = schemaBefore.columns.find(c => c.name === 'user_name')?.columnId;

      const event = registry.renameColumn('users', 'user_name', 'username', 1n, 'txn-1');

      const schemaAfter = registry.getSchema('users')!;
      const newColumn = schemaAfter.columns.find(c => c.name === 'username');

      expect(newColumn).toBeTruthy();
      expect(newColumn?.columnId).toBe(oldColumnId);
      expect(event.changeType).toBe('RENAME_COLUMN');
      expect(event.oldColumnName).toBe('user_name');
      expect(event.column).toBe('username');
    });

    it('should mark column renames as BREAKING', () => {
      const event = registry.renameColumn('users', 'user_name', 'username', 1n, 'txn-1');
      expect(event.compatibility).toBe('BREAKING');
    });
  });

  describe('alterColumn', () => {
    beforeEach(() => {
      registry.registerTable('users', {
        tableName: 'users',
        columns: [
          { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, indexed: true, position: 0, columnId: 0 },
          { name: 'age', type: 'INTEGER', nullable: false, primaryKey: false, indexed: false, position: 1, columnId: 0 },
        ],
        indexes: [],
        constraints: [],
        primaryKey: ['id'],
      });
    });

    it('should alter column type', () => {
      const event = registry.alterColumn('users', 'age', { type: 'BIGINT' }, 1n, 'txn-1');

      const schema = registry.getSchema('users');
      const ageColumn = schema?.columns.find(c => c.name === 'age');

      expect(ageColumn?.type).toBe('BIGINT');
      expect(event.changeType).toBe('ALTER_COLUMN_TYPE');
    });

    it('should classify type widening as BACKWARD_COMPATIBLE', () => {
      const event = registry.alterColumn('users', 'age', { type: 'BIGINT' }, 1n, 'txn-1');
      expect(event.compatibility).toBe('BACKWARD_COMPATIBLE');
    });

    it('should classify making column nullable as FULL_COMPATIBLE', () => {
      const event = registry.alterColumn('users', 'age', { nullable: true }, 1n, 'txn-1');
      expect(event.changeType).toBe('ALTER_COLUMN_NULLABLE');
      expect(event.compatibility).toBe('FULL_COMPATIBLE');
    });

    it('should classify making column non-nullable as BREAKING', () => {
      // First make it nullable
      registry.alterColumn('users', 'age', { nullable: true }, 1n, 'txn-1');

      // Then make it non-nullable again
      const event = registry.alterColumn('users', 'age', { nullable: false }, 2n, 'txn-2');
      expect(event.compatibility).toBe('BREAKING');
    });
  });

  describe('dropTable', () => {
    it('should remove table from registry', () => {
      registry.registerTable('users', {
        tableName: 'users',
        columns: [
          { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, indexed: true, position: 0, columnId: 0 },
        ],
        indexes: [],
        constraints: [],
        primaryKey: ['id'],
      });

      const event = registry.dropTable('users', 1n, 'txn-1');

      expect(registry.getSchema('users')).toBeNull();
      expect(event.changeType).toBe('DROP_TABLE');
      expect(event.compatibility).toBe('BREAKING');
    });
  });

  describe('getSchemaHistory', () => {
    it('should return full schema history', () => {
      registry.registerTable('users', {
        tableName: 'users',
        columns: [
          { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, indexed: true, position: 0, columnId: 0 },
        ],
        indexes: [],
        constraints: [],
        primaryKey: ['id'],
      });

      registry.addColumn('users', {
        name: 'email',
        type: 'TEXT',
        nullable: false,
        primaryKey: false,
        indexed: true,
      }, 1n, 'txn-1');

      registry.addColumn('users', {
        name: 'name',
        type: 'TEXT',
        nullable: true,
        primaryKey: false,
        indexed: false,
      }, 2n, 'txn-2');

      const history = registry.getSchemaHistory('users');
      expect(history).toHaveLength(3);
      expect(history[0].version).toBe(createSchemaVersion(1));
      expect(history[1].version).toBe(createSchemaVersion(2));
      expect(history[2].version).toBe(createSchemaVersion(3));
    });
  });

  describe('export/import', () => {
    it('should export and import registry state', () => {
      registry.registerTable('users', {
        tableName: 'users',
        columns: [
          { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, indexed: true, position: 0, columnId: 0 },
        ],
        indexes: [],
        constraints: [],
        primaryKey: ['id'],
      });

      registry.addColumn('users', {
        name: 'email',
        type: 'TEXT',
        nullable: false,
        primaryKey: false,
        indexed: true,
      }, 1n, 'txn-1');

      const exported = registry.export();

      const newRegistry = new SchemaVersionRegistry();
      newRegistry.import(exported);

      const schema = newRegistry.getSchema('users');
      expect(schema?.version).toBe(createSchemaVersion(2));
      expect(schema?.columns).toHaveLength(2);
    });
  });

  describe('onSchemaChange', () => {
    it('should allow unsubscribing from events', () => {
      const events: SchemaChangeEvent[] = [];
      const unsubscribe = registry.onSchemaChange(event => events.push(event));

      registry.registerTable('users', {
        tableName: 'users',
        columns: [
          { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, indexed: true, position: 0, columnId: 0 },
        ],
        indexes: [],
        constraints: [],
        primaryKey: ['id'],
      });

      expect(events).toHaveLength(1);

      unsubscribe();

      registry.addColumn('users', {
        name: 'email',
        type: 'TEXT',
        nullable: false,
        primaryKey: false,
        indexed: true,
      }, 1n, 'txn-1');

      // Should still be 1 after unsubscribing
      expect(events).toHaveLength(1);
    });
  });
});

describe('SchemaAwareCDCProcessor', () => {
  let registry: SchemaVersionRegistry;
  let processor: SchemaAwareCDCProcessor;

  beforeEach(() => {
    registry = new SchemaVersionRegistry();
    processor = new SchemaAwareCDCProcessor(registry);

    registry.registerTable('users', {
      tableName: 'users',
      columns: [
        { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, indexed: true, position: 0, columnId: 0 },
        { name: 'email', type: 'TEXT', nullable: false, primaryKey: false, indexed: true, position: 1, columnId: 0 },
        { name: 'name', type: 'TEXT', nullable: true, primaryKey: false, indexed: false, position: 2, columnId: 0 },
      ],
      indexes: [],
      constraints: [],
      primaryKey: ['id'],
    });
  });

  describe('enrichEvent', () => {
    it('should enrich CDC event with schema metadata', () => {
      const event = {
        id: '1',
        type: 'insert' as const,
        table: 'users',
        txnId: 'txn-1',
        timestamp: new Date(),
        lsn: 1n,
        data: { id: 1, email: 'test@example.com', name: 'Test' },
      };

      const enriched = processor.enrichEvent(event);

      expect(enriched.schemaVersion).toBe(createSchemaVersion(1));
      expect(enriched.schemaChecksum).toBeTruthy();
      expect(enriched.columnIds).toHaveLength(3);
    });

    it('should extract column IDs from data', () => {
      const event = {
        id: '1',
        type: 'update' as const,
        table: 'users',
        txnId: 'txn-1',
        timestamp: new Date(),
        lsn: 1n,
        data: { email: 'new@example.com' },
        oldData: { email: 'old@example.com' },
      };

      const enriched = processor.enrichEvent(event);

      // Should only include column ID for 'email'
      expect(enriched.columnIds).toHaveLength(1);
    });
  });

  describe('validateCompatibility', () => {
    it('should validate compatible schema versions', () => {
      const event = processor.enrichEvent({
        id: '1',
        type: 'insert' as const,
        table: 'users',
        txnId: 'txn-1',
        timestamp: new Date(),
        lsn: 1n,
        data: { id: 1, email: 'test@example.com' },
      });

      const result = processor.validateCompatibility(event, createSchemaVersion(1));
      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });
  });

  describe('transformToVersion', () => {
    it('should handle column renames when transforming', () => {
      // Add a column first
      registry.addColumn('users', {
        name: 'created_at',
        type: 'TIMESTAMP',
        nullable: true,
        primaryKey: false,
        indexed: false,
      }, 1n, 'txn-1');

      // Create event at version 1
      const event = processor.enrichEvent({
        id: '1',
        type: 'insert' as const,
        table: 'users',
        txnId: 'txn-1',
        timestamp: new Date(),
        lsn: 1n,
        data: { id: 1, email: 'test@example.com', name: 'Test' },
      });

      // Transform to version 2 (should have created_at column)
      const transformed = processor.transformToVersion(event, createSchemaVersion(2));

      expect(transformed.schemaVersion).toBe(createSchemaVersion(2));
    });

    it('should return same event if version matches', () => {
      const event = processor.enrichEvent({
        id: '1',
        type: 'insert' as const,
        table: 'users',
        txnId: 'txn-1',
        timestamp: new Date(),
        lsn: 1n,
        data: { id: 1, email: 'test@example.com' },
      });

      const transformed = processor.transformToVersion(event, createSchemaVersion(1));
      expect(transformed).toBe(event);
    });
  });
});

describe('Global Registry', () => {
  beforeEach(() => {
    resetSchemaRegistry();
  });

  it('should provide a singleton registry', () => {
    const registry1 = getSchemaRegistry();
    const registry2 = getSchemaRegistry();

    expect(registry1).toBe(registry2);
  });

  it('should reset the registry', () => {
    const registry1 = getSchemaRegistry();
    registry1.registerTable('test', {
      tableName: 'test',
      columns: [
        { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, indexed: true, position: 0, columnId: 0 },
      ],
      indexes: [],
      constraints: [],
      primaryKey: ['id'],
    });

    resetSchemaRegistry();

    const registry2 = getSchemaRegistry();
    expect(registry2.getSchema('test')).toBeNull();
  });
});

describe('Schema Evolution Scenarios', () => {
  let registry: SchemaVersionRegistry;

  beforeEach(() => {
    registry = new SchemaVersionRegistry();
  });

  it('should handle typical migration: add optional column', () => {
    // v1: Initial schema
    registry.registerTable('orders', {
      tableName: 'orders',
      columns: [
        { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, indexed: true, position: 0, columnId: 0 },
        { name: 'total', type: 'DECIMAL', nullable: false, primaryKey: false, indexed: false, position: 1, columnId: 0 },
      ],
      indexes: [],
      constraints: [],
      primaryKey: ['id'],
    });

    // v2: Add optional notes column
    const event = registry.addColumn('orders', {
      name: 'notes',
      type: 'TEXT',
      nullable: true,
      primaryKey: false,
      indexed: false,
    }, 1n, 'migration-1');

    expect(event.compatibility).toBe('FULL_COMPATIBLE');
    expect(registry.getCurrentVersion('orders')).toBe(createSchemaVersion(2));
  });

  it('should handle migration: add required column with default', () => {
    registry.registerTable('products', {
      tableName: 'products',
      columns: [
        { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, indexed: true, position: 0, columnId: 0 },
        { name: 'name', type: 'TEXT', nullable: false, primaryKey: false, indexed: false, position: 1, columnId: 0 },
      ],
      indexes: [],
      constraints: [],
      primaryKey: ['id'],
    });

    const event = registry.addColumn('products', {
      name: 'status',
      type: 'TEXT',
      nullable: false,
      defaultValue: "'active'",
      primaryKey: false,
      indexed: true,
    }, 1n, 'migration-1');

    expect(event.compatibility).toBe('FULL_COMPATIBLE');
  });

  it('should handle migration: widen column type', () => {
    registry.registerTable('metrics', {
      tableName: 'metrics',
      columns: [
        { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, indexed: true, position: 0, columnId: 0 },
        { name: 'value', type: 'INTEGER', nullable: false, primaryKey: false, indexed: false, position: 1, columnId: 0 },
      ],
      indexes: [],
      constraints: [],
      primaryKey: ['id'],
    });

    // Widen INTEGER to BIGINT
    const event = registry.alterColumn('metrics', 'value', { type: 'BIGINT' }, 1n, 'migration-1');

    expect(event.compatibility).toBe('BACKWARD_COMPATIBLE');
  });

  it('should handle complex migration sequence', () => {
    const events: SchemaChangeEvent[] = [];
    registry.onSchemaChange(e => events.push(e));

    // v1: Create table
    registry.registerTable('audit_logs', {
      tableName: 'audit_logs',
      columns: [
        { name: 'id', type: 'INTEGER', nullable: false, primaryKey: true, indexed: true, position: 0, columnId: 0 },
        { name: 'action', type: 'TEXT', nullable: false, primaryKey: false, indexed: true, position: 1, columnId: 0 },
        { name: 'timestamp', type: 'INTEGER', nullable: false, primaryKey: false, indexed: true, position: 2, columnId: 0 },
      ],
      indexes: [],
      constraints: [],
      primaryKey: ['id'],
    });

    // v2: Add user_id column
    registry.addColumn('audit_logs', {
      name: 'user_id',
      type: 'INTEGER',
      nullable: true,
      primaryKey: false,
      indexed: true,
    }, 1n, 'migration-1');

    // v3: Add metadata column
    registry.addColumn('audit_logs', {
      name: 'metadata',
      type: 'JSON',
      nullable: true,
      primaryKey: false,
      indexed: false,
    }, 2n, 'migration-2');

    // v4: Change timestamp from INTEGER to BIGINT
    registry.alterColumn('audit_logs', 'timestamp', { type: 'BIGINT' }, 3n, 'migration-3');

    expect(events).toHaveLength(4);
    expect(registry.getCurrentVersion('audit_logs')).toBe(createSchemaVersion(4));

    const history = registry.getSchemaHistory('audit_logs');
    expect(history).toHaveLength(4);
    expect(history[3].columns).toHaveLength(5);
  });
});
