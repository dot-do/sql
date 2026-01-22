/**
 * DoSQL ATTACH/DETACH Database Tests
 *
 * TDD tests for multi-database support following SQLite semantics:
 * - ATTACH DATABASE 'path' AS alias
 * - DETACH DATABASE alias
 * - Cross-database queries
 * - Schema qualification
 * - PRAGMA database_list
 * - Transactions across attached databases
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  createDatabaseManager,
  AttachmentManager,
  InMemoryDatabaseConnection,
  DefaultConnectionFactory,
} from './manager.js';
import {
  createSchemaResolver,
  AttachSchemaResolver,
  parseQualifiedTableName,
  parseQualifiedColumnName,
  formatQualifiedTableName,
  formatQualifiedColumnName,
  parseAttachStatement,
  parseDetachStatement,
  isAttachStatement,
  isDetachStatement,
  isDatabaseListPragma,
} from './resolver.js';
import type {
  AttachedDatabase,
  QualifiedTableRef,
  QualifiedColumnRef,
  QueryScope,
} from './types.js';
import type { TableSchema } from '../engine/types.js';

// =============================================================================
// TEST HELPERS
// =============================================================================

/**
 * Create a sample table schema
 */
function createTableSchema(
  name: string,
  columns: Array<{ name: string; type: string; nullable?: boolean; primaryKey?: boolean }>
): TableSchema {
  return {
    name,
    columns: columns.map(c => ({
      name: c.name,
      type: c.type as 'string' | 'number' | 'bigint' | 'boolean' | 'date' | 'bytes',
      nullable: c.nullable ?? false,
      primaryKey: c.primaryKey,
    })),
  };
}

// =============================================================================
// ATTACH DATABASE TESTS
// =============================================================================

describe('ATTACH DATABASE', () => {
  let manager: AttachmentManager;

  beforeEach(() => {
    manager = createDatabaseManager();
  });

  describe('Basic attachment', () => {
    it('should attach a database with a valid alias', async () => {
      const result = await manager.attach('/path/to/db.sqlite', {
        alias: 'sales',
      });

      expect(result.success).toBe(true);
      expect(result.database).toBeDefined();
      expect(result.database?.alias).toBe('sales');
      expect(result.database?.path).toBe('/path/to/db.sqlite');
    });

    it('should attach a memory database', async () => {
      const result = await manager.attach(':memory:', {
        alias: 'testdb',
        type: 'memory',
      });

      expect(result.success).toBe(true);
      expect(result.database?.alias).toBe('testdb');
      expect(result.database?.path).toBe(':memory:');
    });

    it('should attach database with readonly flag', async () => {
      const result = await manager.attach('/data/readonly.db', {
        alias: 'archive',
        readonly: true,
      });

      expect(result.success).toBe(true);
      expect(result.database?.isReadOnly).toBe(true);
    });

    it('should track attachment timestamp', async () => {
      const before = new Date();
      const result = await manager.attach(':memory:', { alias: 'timed' });
      const after = new Date();

      expect(result.database?.attachedAt).toBeDefined();
      expect(result.database?.attachedAt.getTime()).toBeGreaterThanOrEqual(before.getTime());
      expect(result.database?.attachedAt.getTime()).toBeLessThanOrEqual(after.getTime());
    });

    it('should assign sequential sequence numbers', async () => {
      await manager.attach(':memory:', { alias: 'db1' });
      await manager.attach(':memory:', { alias: 'db2' });
      await manager.attach(':memory:', { alias: 'db3' });

      const db1 = manager.get('db1');
      const db2 = manager.get('db2');
      const db3 = manager.get('db3');

      expect(db1?.seq).toBeLessThan(db2!.seq);
      expect(db2?.seq).toBeLessThan(db3!.seq);
    });
  });

  describe('Alias validation', () => {
    it('should reject empty alias', async () => {
      const result = await manager.attach('/path/to/db', { alias: '' });

      expect(result.success).toBe(false);
      expect(result.error).toContain('required');
    });

    it('should reject reserved alias "main"', async () => {
      const result = await manager.attach('/path/to/db', { alias: 'main' });

      expect(result.success).toBe(false);
      expect(result.error).toContain('reserved');
    });

    it('should reject reserved alias "temp"', async () => {
      const result = await manager.attach('/path/to/db', { alias: 'temp' });

      expect(result.success).toBe(false);
      expect(result.error).toContain('reserved');
    });

    it('should reject duplicate alias', async () => {
      await manager.attach(':memory:', { alias: 'mydb' });
      const result = await manager.attach(':memory:', { alias: 'mydb' });

      expect(result.success).toBe(false);
      expect(result.error).toContain('already in use');
    });

    it('should reject invalid alias format (starting with number)', async () => {
      const result = await manager.attach(':memory:', { alias: '123db' });

      expect(result.success).toBe(false);
      expect(result.error).toContain('Invalid');
    });

    it('should accept alias with underscores', async () => {
      const result = await manager.attach(':memory:', { alias: 'my_database_1' });

      expect(result.success).toBe(true);
      expect(result.database?.alias).toBe('my_database_1');
    });
  });

  describe('Maximum attached databases limit', () => {
    it('should enforce maximum attached databases limit', async () => {
      // Attach 10 databases (max limit)
      for (let i = 0; i < 10; i++) {
        const result = await manager.attach(':memory:', { alias: `db${i}` });
        expect(result.success).toBe(true);
      }

      // 11th should fail
      const result = await manager.attach(':memory:', { alias: 'overflow' });
      expect(result.success).toBe(false);
      expect(result.error).toContain('Maximum');
    });

    it('should allow attaching after detaching', async () => {
      // Attach 10 databases
      for (let i = 0; i < 10; i++) {
        await manager.attach(':memory:', { alias: `db${i}` });
      }

      // Detach one
      await manager.detach('db5');

      // Now should be able to attach another
      const result = await manager.attach(':memory:', { alias: 'newdb' });
      expect(result.success).toBe(true);
    });
  });

  describe('Connection types', () => {
    it('should detect r2:// scheme', async () => {
      const result = await manager.attach('r2://mybucket/path/data.db', {
        alias: 'r2db',
      });

      expect(result.success).toBe(true);
      expect(result.database?.options.type).toBe('r2');
    });

    it('should detect kv:// scheme', async () => {
      const result = await manager.attach('kv://mynamespace/key', {
        alias: 'kvdb',
      });

      expect(result.success).toBe(true);
      expect(result.database?.options.type).toBe('kv');
    });

    it('should detect https:// scheme as remote', async () => {
      const result = await manager.attach('https://api.example.com/db', {
        alias: 'remotedb',
      });

      expect(result.success).toBe(true);
      expect(result.database?.options.type).toBe('remote');
    });
  });
});

// =============================================================================
// DETACH DATABASE TESTS
// =============================================================================

describe('DETACH DATABASE', () => {
  let manager: AttachmentManager;

  beforeEach(async () => {
    manager = createDatabaseManager();
    await manager.attach(':memory:', { alias: 'testdb' });
  });

  it('should detach an attached database', async () => {
    expect(manager.has('testdb')).toBe(true);

    const result = await manager.detach('testdb');

    expect(result.success).toBe(true);
    expect(manager.has('testdb')).toBe(false);
  });

  it('should fail to detach main database', async () => {
    const result = await manager.detach('main');

    expect(result.success).toBe(false);
    expect(result.error).toContain('main');
  });

  it('should fail to detach temp database', async () => {
    const result = await manager.detach('temp');

    expect(result.success).toBe(false);
    expect(result.error).toContain('temp');
  });

  it('should fail to detach non-existent database', async () => {
    const result = await manager.detach('nonexistent');

    expect(result.success).toBe(false);
    expect(result.error).toContain('No such database');
  });

  it('should fail to detach database with active transactions', async () => {
    // Start a transaction
    manager.beginTransaction({ databases: ['testdb'] });

    const result = await manager.detach('testdb');

    expect(result.success).toBe(false);
    expect(result.hadActiveTransactions).toBe(true);
  });
});

// =============================================================================
// MAIN AND TEMP DATABASE TESTS
// =============================================================================

describe('main and temp databases', () => {
  let manager: AttachmentManager;

  beforeEach(() => {
    manager = createDatabaseManager();
  });

  it('should have main database by default', () => {
    const main = manager.getMain();

    expect(main).toBeDefined();
    expect(main.alias).toBe('main');
    expect(main.isMain).toBe(true);
  });

  it('should have temp database by default', () => {
    const temp = manager.getTemp();

    expect(temp).toBeDefined();
    expect(temp.alias).toBe('temp');
    expect(temp.isTemp).toBe(true);
  });

  it('should have main with sequence 0', () => {
    const main = manager.getMain();
    expect(main.seq).toBe(0);
  });

  it('should have temp with sequence 1', () => {
    const temp = manager.getTemp();
    expect(temp.seq).toBe(1);
  });

  it('should not be able to get main via attach', async () => {
    const result = await manager.attach(':memory:', { alias: 'main' });
    expect(result.success).toBe(false);
  });
});

// =============================================================================
// PRAGMA DATABASE_LIST TESTS
// =============================================================================

describe('PRAGMA database_list', () => {
  let manager: AttachmentManager;

  beforeEach(async () => {
    manager = createDatabaseManager();
    await manager.attach(':memory:', { alias: 'sales' });
    await manager.attach('/path/to/archive.db', { alias: 'archive' });
  });

  it('should list all attached databases', () => {
    const list = manager.list();

    expect(list.length).toBe(4); // main, temp, sales, archive
    expect(list.map(d => d.name)).toContain('main');
    expect(list.map(d => d.name)).toContain('temp');
    expect(list.map(d => d.name)).toContain('sales');
    expect(list.map(d => d.name)).toContain('archive');
  });

  it('should return databases sorted by sequence', () => {
    const list = manager.list();

    for (let i = 1; i < list.length; i++) {
      expect(list[i].seq).toBeGreaterThan(list[i - 1].seq);
    }
  });

  it('should include file path in list', () => {
    const list = manager.list();
    const archive = list.find(d => d.name === 'archive');

    expect(archive?.file).toBe('/path/to/archive.db');
  });
});

// =============================================================================
// SCHEMA QUALIFICATION TESTS
// =============================================================================

describe('Schema qualification', () => {
  describe('parseQualifiedTableName', () => {
    it('should parse unqualified table name', () => {
      const ref = parseQualifiedTableName('users');

      expect(ref.table).toBe('users');
      expect(ref.database).toBeUndefined();
    });

    it('should parse db.table format', () => {
      const ref = parseQualifiedTableName('sales.customers');

      expect(ref.database).toBe('sales');
      expect(ref.table).toBe('customers');
    });

    it('should throw on invalid format', () => {
      expect(() => parseQualifiedTableName('a.b.c.d')).toThrow();
    });
  });

  describe('parseQualifiedColumnName', () => {
    it('should parse unqualified column name', () => {
      const ref = parseQualifiedColumnName('id');

      expect(ref.column).toBe('id');
      expect(ref.table).toBeUndefined();
      expect(ref.database).toBeUndefined();
    });

    it('should parse table.column format', () => {
      const ref = parseQualifiedColumnName('users.id');

      expect(ref.table).toBe('users');
      expect(ref.column).toBe('id');
      expect(ref.database).toBeUndefined();
    });

    it('should parse db.table.column format', () => {
      const ref = parseQualifiedColumnName('sales.customers.id');

      expect(ref.database).toBe('sales');
      expect(ref.table).toBe('customers');
      expect(ref.column).toBe('id');
    });
  });

  describe('formatQualifiedTableName', () => {
    it('should format unqualified name', () => {
      const name = formatQualifiedTableName({ table: 'users' });
      expect(name).toBe('users');
    });

    it('should format qualified name', () => {
      const name = formatQualifiedTableName({ database: 'sales', table: 'customers' });
      expect(name).toBe('sales.customers');
    });
  });

  describe('formatQualifiedColumnName', () => {
    it('should format column only', () => {
      const name = formatQualifiedColumnName({ column: 'id' });
      expect(name).toBe('id');
    });

    it('should format table.column', () => {
      const name = formatQualifiedColumnName({ table: 'users', column: 'id' });
      expect(name).toBe('users.id');
    });

    it('should format db.table.column', () => {
      const name = formatQualifiedColumnName({
        database: 'sales',
        table: 'users',
        column: 'id',
      });
      expect(name).toBe('sales.users.id');
    });
  });
});

// =============================================================================
// CROSS-DATABASE QUERY TESTS
// =============================================================================

describe('Cross-database queries', () => {
  let manager: AttachmentManager;
  let resolver: AttachSchemaResolver;

  beforeEach(async () => {
    manager = createDatabaseManager();

    // Add tables to main
    manager.addTableToSchema('main', createTableSchema('users', [
      { name: 'id', type: 'number', primaryKey: true },
      { name: 'name', type: 'string' },
    ]));
    manager.addTableToSchema('main', createTableSchema('orders', [
      { name: 'id', type: 'number', primaryKey: true },
      { name: 'user_id', type: 'number' },
      { name: 'total', type: 'number' },
    ]));

    // Attach sales database
    await manager.attach(':memory:', { alias: 'sales' });
    manager.addTableToSchema('sales', createTableSchema('customers', [
      { name: 'id', type: 'number', primaryKey: true },
      { name: 'company', type: 'string' },
    ]));
    manager.addTableToSchema('sales', createTableSchema('invoices', [
      { name: 'id', type: 'number', primaryKey: true },
      { name: 'customer_id', type: 'number' },
      { name: 'amount', type: 'number' },
    ]));

    resolver = createSchemaResolver(manager);
  });

  describe('Table resolution', () => {
    it('should resolve unqualified table from main', () => {
      const result = resolver.resolveTable({ table: 'users' });

      expect('code' in result).toBe(false);
      if (!('code' in result)) {
        expect(result.database).toBe('main');
        expect(result.tableDef.name).toBe('users');
      }
    });

    it('should resolve qualified table from attached database', () => {
      const result = resolver.resolveTable({ database: 'sales', table: 'customers' });

      expect('code' in result).toBe(false);
      if (!('code' in result)) {
        expect(result.database).toBe('sales');
        expect(result.tableDef.name).toBe('customers');
        expect(result.isCrossDatabase).toBe(true);
      }
    });

    it('should return error for non-existent table', () => {
      const result = resolver.resolveTable({ table: 'nonexistent' });

      expect('code' in result).toBe(true);
      if ('code' in result) {
        expect(result.code).toBe('TABLE_NOT_FOUND');
      }
    });

    it('should return error for non-existent database', () => {
      const result = resolver.resolveTable({ database: 'nosuchdb', table: 'users' });

      expect('code' in result).toBe(true);
      if ('code' in result) {
        expect(result.code).toBe('DATABASE_NOT_FOUND');
      }
    });
  });

  describe('Ambiguous table detection', () => {
    beforeEach(() => {
      // Add 'users' to sales to create ambiguity
      manager.addTableToSchema('sales', createTableSchema('users', [
        { name: 'id', type: 'number' },
        { name: 'email', type: 'string' },
      ]));
    });

    it('should detect ambiguous table name', () => {
      const check = resolver.checkAmbiguity('users');

      expect(check.isAmbiguous).toBe(true);
      expect(check.candidates?.length).toBe(2);
    });

    it('should resolve ambiguous table with explicit database', () => {
      const result = resolver.resolveTable({ database: 'sales', table: 'users' });

      expect('code' in result).toBe(false);
      if (!('code' in result)) {
        expect(result.database).toBe('sales');
      }
    });

    it('should report ambiguity for unqualified name', () => {
      const result = resolver.resolveTable({ table: 'users' });

      expect('code' in result).toBe(true);
      if ('code' in result) {
        expect(result.code).toBe('AMBIGUOUS_TABLE');
      }
    });
  });

  describe('Column resolution', () => {
    it('should resolve column with scope', () => {
      const scope = resolver.buildScope([{ table: 'users' }]);
      const result = resolver.resolveColumn({ column: 'name' }, scope);

      expect('code' in result).toBe(false);
      if (!('code' in result)) {
        expect(result.columnDef.name).toBe('name');
        expect(result.database).toBe('main');
      }
    });

    it('should resolve table.column within scope', () => {
      const scope = resolver.buildScope([{ table: 'users' }, { table: 'orders' }]);
      const result = resolver.resolveColumn({ table: 'users', column: 'id' }, scope);

      expect('code' in result).toBe(false);
      if (!('code' in result)) {
        expect(result.table).toBe('users');
      }
    });

    it('should resolve db.table.column', () => {
      const scope = resolver.buildScope([]);
      const result = resolver.resolveColumn({
        database: 'sales',
        table: 'customers',
        column: 'company',
      }, scope);

      expect('code' in result).toBe(false);
      if (!('code' in result)) {
        expect(result.database).toBe('sales');
        expect(result.table).toBe('customers');
        expect(result.columnDef.name).toBe('company');
      }
    });

    it('should detect ambiguous column', () => {
      // Both users and orders have 'id'
      const scope = resolver.buildScope([{ table: 'users' }, { table: 'orders' }]);
      const result = resolver.resolveColumn({ column: 'id' }, scope);

      expect('code' in result).toBe(true);
      if ('code' in result) {
        expect(result.code).toBe('AMBIGUOUS_COLUMN');
      }
    });
  });

  describe('Query scope building', () => {
    it('should build scope from FROM clause', () => {
      const scope = resolver.buildScope([
        { table: 'users' },
        { database: 'sales', table: 'customers' },
      ]);

      expect(scope.tables.size).toBe(2);
      expect(scope.tables.has('users')).toBe(true);
      expect(scope.tables.has('customers')).toBe(true);
    });

    it('should handle table aliases in scope', () => {
      const scope = resolver.buildScope([
        { table: 'users', alias: 'u' },
        { table: 'orders', alias: 'o' },
      ]);

      expect(scope.tables.has('u')).toBe(true);
      expect(scope.tables.has('o')).toBe(true);
      expect(scope.tables.has('users')).toBe(false);
    });

    it('should support child scopes for subqueries', () => {
      const parentScope = resolver.buildScope([{ table: 'users' }]);
      const childScope = resolver.createChildScope(parentScope);

      expect(childScope.parent).toBe(parentScope);
    });
  });
});

// =============================================================================
// TRANSACTION TESTS
// =============================================================================

describe('Transactions across attached databases', () => {
  let manager: AttachmentManager;

  beforeEach(async () => {
    manager = createDatabaseManager();
    await manager.attach(':memory:', { alias: 'db1' });
    await manager.attach(':memory:', { alias: 'db2' });
  });

  it('should begin a transaction across multiple databases', () => {
    const txn = manager.beginTransaction({ databases: ['db1', 'db2'] });

    expect(txn.id).toBeDefined();
    expect(txn.databases).toContain('db1');
    expect(txn.databases).toContain('db2');
    expect(txn.isCrossDatabase).toBe(true);
    expect(txn.state).toBe('active');
  });

  it('should begin a single-database transaction', () => {
    const txn = manager.beginTransaction({ databases: ['db1'] });

    expect(txn.isCrossDatabase).toBe(false);
  });

  it('should commit a transaction', async () => {
    const txn = manager.beginTransaction();

    const success = await manager.commitTransaction(txn.id);

    expect(success).toBe(true);
    expect(manager.getTransaction(txn.id)).toBeUndefined();
  });

  it('should rollback a transaction', async () => {
    const txn = manager.beginTransaction();

    const success = await manager.rollbackTransaction(txn.id);

    expect(success).toBe(true);
  });

  it('should support savepoints', () => {
    const txn = manager.beginTransaction();

    const saved = manager.savepoint(txn.id, 'sp1');

    expect(saved).toBe(true);
    expect(txn.savepoints).toContain('sp1');
  });

  it('should rollback to savepoint', () => {
    const txn = manager.beginTransaction();
    manager.savepoint(txn.id, 'sp1');
    manager.savepoint(txn.id, 'sp2');

    const success = manager.rollbackToSavepoint(txn.id, 'sp1');

    expect(success).toBe(true);
    expect(txn.savepoints).toContain('sp1');
    expect(txn.savepoints).not.toContain('sp2');
  });

  it('should release savepoint', () => {
    const txn = manager.beginTransaction();
    manager.savepoint(txn.id, 'sp1');

    const success = manager.releaseSavepoint(txn.id, 'sp1');

    expect(success).toBe(true);
    expect(txn.savepoints).not.toContain('sp1');
  });

  it('should track active transactions', () => {
    manager.beginTransaction({ databases: ['db1'] });
    manager.beginTransaction({ databases: ['db2'] });

    const active = manager.getActiveTransactions();

    expect(active.length).toBe(2);
  });
});

// =============================================================================
// SQL PARSING TESTS
// =============================================================================

describe('SQL statement parsing', () => {
  describe('ATTACH statement parsing', () => {
    it('should parse ATTACH DATABASE with single quotes', () => {
      const result = parseAttachStatement("ATTACH DATABASE '/path/to/db.sqlite' AS mydb");

      expect(result).not.toBeNull();
      expect(result?.path).toBe('/path/to/db.sqlite');
      expect(result?.alias).toBe('mydb');
    });

    it('should parse ATTACH DATABASE with double quotes', () => {
      const result = parseAttachStatement('ATTACH DATABASE "/path/to/db.sqlite" AS mydb');

      expect(result).not.toBeNull();
      expect(result?.path).toBe('/path/to/db.sqlite');
    });

    it('should parse ATTACH without DATABASE keyword', () => {
      const result = parseAttachStatement("ATTACH '/path/to/db.sqlite' AS mydb");

      expect(result).not.toBeNull();
      expect(result?.path).toBe('/path/to/db.sqlite');
    });

    it('should handle case-insensitive keywords', () => {
      const result = parseAttachStatement("attach database '/path/db' as MYDB");

      expect(result).not.toBeNull();
      expect(result?.alias).toBe('MYDB');
    });

    it('should return null for invalid statement', () => {
      const result = parseAttachStatement('SELECT * FROM users');

      expect(result).toBeNull();
    });
  });

  describe('DETACH statement parsing', () => {
    it('should parse DETACH DATABASE', () => {
      const result = parseDetachStatement('DETACH DATABASE mydb');

      expect(result).not.toBeNull();
      expect(result?.alias).toBe('mydb');
    });

    it('should parse DETACH without DATABASE keyword', () => {
      const result = parseDetachStatement('DETACH mydb');

      expect(result).not.toBeNull();
      expect(result?.alias).toBe('mydb');
    });
  });

  describe('Statement type detection', () => {
    it('should detect ATTACH statement', () => {
      expect(isAttachStatement("ATTACH DATABASE 'db' AS a")).toBe(true);
      expect(isAttachStatement('SELECT * FROM users')).toBe(false);
    });

    it('should detect DETACH statement', () => {
      expect(isDetachStatement('DETACH mydb')).toBe(true);
      expect(isDetachStatement('SELECT * FROM users')).toBe(false);
    });

    it('should detect PRAGMA database_list', () => {
      expect(isDatabaseListPragma('PRAGMA database_list')).toBe(true);
      expect(isDatabaseListPragma('PRAGMA database_list;')).toBe(true);
      expect(isDatabaseListPragma('pragma DATABASE_LIST')).toBe(true);
      expect(isDatabaseListPragma('PRAGMA table_info(users)')).toBe(false);
    });
  });
});

// =============================================================================
// SCHEMA MANAGEMENT TESTS
// =============================================================================

describe('Schema management', () => {
  let manager: AttachmentManager;

  beforeEach(async () => {
    manager = createDatabaseManager();
    await manager.attach(':memory:', { alias: 'sales' });
  });

  it('should add table to database schema', () => {
    const table = createTableSchema('products', [
      { name: 'id', type: 'number' },
      { name: 'name', type: 'string' },
    ]);

    const success = manager.addTableToSchema('sales', table);

    expect(success).toBe(true);
    expect(manager.getSchema('sales')?.tables.has('products')).toBe(true);
  });

  it('should remove table from schema', () => {
    manager.addTableToSchema('sales', createTableSchema('temp_table', [
      { name: 'id', type: 'number' },
    ]));

    const success = manager.removeTableFromSchema('sales', 'temp_table');

    expect(success).toBe(true);
    expect(manager.getSchema('sales')?.tables.has('temp_table')).toBe(false);
  });

  it('should get merged schema from all databases', () => {
    manager.addTableToSchema('main', createTableSchema('users', [
      { name: 'id', type: 'number' },
    ]));
    manager.addTableToSchema('sales', createTableSchema('customers', [
      { name: 'id', type: 'number' },
    ]));

    const merged = manager.getMergedSchema();

    expect(merged.tables.has('main.users')).toBe(true);
    expect(merged.tables.has('sales.customers')).toBe(true);
    // Main tables also available unqualified
    expect(merged.tables.has('users')).toBe(true);
  });

  it('should return undefined for non-existent database schema', () => {
    const schema = manager.getSchema('nonexistent');

    expect(schema).toBeUndefined();
  });
});

// =============================================================================
// MANAGER UTILITY METHODS TESTS
// =============================================================================

describe('Manager utility methods', () => {
  let manager: AttachmentManager;

  beforeEach(async () => {
    manager = createDatabaseManager();
    await manager.attach(':memory:', { alias: 'db1' });
    await manager.attach(':memory:', { alias: 'db2' });
  });

  it('should return correct count', () => {
    expect(manager.count()).toBe(4); // main, temp, db1, db2
  });

  it('should return all aliases', () => {
    const aliases = manager.aliases();

    expect(aliases).toContain('main');
    expect(aliases).toContain('temp');
    expect(aliases).toContain('db1');
    expect(aliases).toContain('db2');
  });

  it('should check if database exists', () => {
    expect(manager.has('db1')).toBe(true);
    expect(manager.has('nonexistent')).toBe(false);
  });

  it('should get database by alias', () => {
    const db = manager.get('db1');

    expect(db).toBeDefined();
    expect(db?.alias).toBe('db1');
  });

  it('should return undefined for non-existent database', () => {
    const db = manager.get('nonexistent');

    expect(db).toBeUndefined();
  });
});

// =============================================================================
// CONNECTION FACTORY TESTS
// =============================================================================

describe('Connection factory', () => {
  it('should create in-memory connection', async () => {
    const factory = new DefaultConnectionFactory();
    const conn = await factory.connect({ type: 'memory', path: ':memory:' });

    expect(conn.type).toBe('memory');
    expect(await conn.ping()).toBe(true);
  });

  it('should close connection', async () => {
    const conn = new InMemoryDatabaseConnection();

    expect(await conn.ping()).toBe(true);

    await conn.close();

    expect(await conn.ping()).toBe(false);
  });

  it('should manage schema in memory connection', async () => {
    const conn = new InMemoryDatabaseConnection();

    conn.addTable(createTableSchema('users', [{ name: 'id', type: 'number' }]));

    const schema = await conn.getSchema();
    expect(schema.tables.has('users')).toBe(true);

    conn.removeTable('users');

    const schema2 = await conn.getSchema();
    expect(schema2.tables.has('users')).toBe(false);
  });
});
