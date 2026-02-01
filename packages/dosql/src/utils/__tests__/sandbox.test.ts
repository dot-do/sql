/**
 * Sandbox Module Utilities Tests
 *
 * Tests for sandbox code generation functions used by
 * stored procedures and triggers.
 */

import { describe, it, expect } from 'vitest';
import {
  buildTableAccessorCode,
  buildDbCallCode,
  buildSqlCode,
  buildTransactionCode,
  buildSandboxModule,
  buildProcedureModule,
  buildCustomSandboxModule,
} from '../sandbox.js';

// =============================================================================
// CODE GENERATION
// =============================================================================

describe('buildTableAccessorCode', () => {
  it('should generate accessor code for a table', () => {
    const code = buildTableAccessorCode('users');
    expect(code).toContain('users:');
    expect(code).toContain("async get(key) { return __dbCall('users', 'get', [key]); }");
    expect(code).toContain("async all(options) { return __dbCall('users', 'all', [options]); }");
    expect(code).toContain("async insert(record) { return __dbCall('users', 'insert', [record]); }");
  });

  it('should include where with function predicate support', () => {
    const code = buildTableAccessorCode('orders');
    expect(code).toContain('async where(predicate, options)');
    expect(code).toContain("typeof predicate === 'function'");
  });

  it('should throw error for function predicates in update/delete', () => {
    const code = buildTableAccessorCode('items');
    expect(code).toContain('Function predicates not supported for update');
    expect(code).toContain('Function predicates not supported for delete');
  });
});

describe('buildDbCallCode', () => {
  it('should generate __dbCall function', () => {
    const code = buildDbCallCode();
    expect(code).toContain('async function __dbCall(table, method, args)');
    expect(code).toContain("fetch('db://internal/call'");
    expect(code).toContain('JSON.stringify({ table, method, args })');
  });
});

describe('buildSqlCode', () => {
  it('should generate sql template literal function', () => {
    const code = buildSqlCode();
    expect(code).toContain('async function sql(strings, ...values)');
    expect(code).toContain("fetch('db://internal/sql'");
  });
});

describe('buildTransactionCode', () => {
  it('should generate transaction function with begin/commit/rollback', () => {
    const code = buildTransactionCode();
    expect(code).toContain('async function transaction(callback)');
    expect(code).toContain("fetch('db://internal/tx/begin'");
    expect(code).toContain("fetch('db://internal/tx/commit'");
    expect(code).toContain("fetch('db://internal/tx/rollback'");
  });
});

// =============================================================================
// MODULE BUILDERS
// =============================================================================

describe('buildSandboxModule', () => {
  it('should build a trigger sandbox module', () => {
    const code = buildSandboxModule('async (event) => { return true; }', ['users', 'posts']);
    expect(code).toContain('async function __dbCall');
    expect(code).toContain('users:');
    expect(code).toContain('posts:');
    expect(code).toContain('db.users = db.tables.users;');
    expect(code).toContain('db.posts = db.tables.posts;');
    expect(code).toContain('const handler =');
    expect(code).toContain('export default handler;');
  });

  it('should handle empty table names array', () => {
    const code = buildSandboxModule('() => {}', []);
    expect(code).toContain('tables: {');
    expect(code).not.toContain('undefined');
  });
});

describe('buildProcedureModule', () => {
  it('should build a procedure sandbox module with SQL and transaction support', () => {
    const code = buildProcedureModule('async function run(db) {}', ['users']);
    expect(code).toContain('async function __dbCall');
    expect(code).toContain('async function sql');
    expect(code).toContain('async function transaction');
    expect(code).toContain('sql,');
    expect(code).toContain('transaction,');
    expect(code).toContain('users:');
    expect(code).toContain('async function run(db) {}');
  });
});

describe('buildCustomSandboxModule', () => {
  it('should build module with all features enabled', () => {
    const code = buildCustomSandboxModule({
      tableNames: ['users'],
      handlerCode: 'const x = 1;',
      includeSql: true,
      includeTransaction: true,
    });
    expect(code).toContain('async function __dbCall');
    expect(code).toContain('async function sql');
    expect(code).toContain('async function transaction');
    expect(code).toContain('users:');
    expect(code).toContain('const x = 1;');
  });

  it('should build module without SQL or transaction', () => {
    const code = buildCustomSandboxModule({
      tableNames: ['orders'],
      handlerCode: 'const x = 1;',
      includeSql: false,
      includeTransaction: false,
    });
    expect(code).toContain('async function __dbCall');
    expect(code).not.toContain('async function sql');
    expect(code).not.toContain('async function transaction');
    expect(code).toContain('orders:');
  });

  it('should include sql in db object when enabled', () => {
    const withSql = buildCustomSandboxModule({
      tableNames: [],
      handlerCode: '',
      includeSql: true,
    });
    expect(withSql).toContain('sql,');
  });
});
