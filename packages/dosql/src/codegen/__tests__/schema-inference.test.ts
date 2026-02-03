/**
 * TypeScript Schema Inference from SQL DDL Tests
 *
 * Tests for generating TypeScript types from CREATE TABLE statements.
 */

import { describe, it, expect } from 'vitest';
import {
  inferSchemaFromDDL,
  inferTableType,
  ddlToTypeScript,
  ddlToTypeScriptWithHelpers,
  toPascalCase,
  toCamelCase,
  type SchemaInferenceOptions,
  type GeneratedInterface,
} from '../schema-inference.js';

// =============================================================================
// UTILITY FUNCTION TESTS
// =============================================================================

describe('toPascalCase', () => {
  it('should convert snake_case to PascalCase', () => {
    expect(toPascalCase('users')).toBe('Users');
    expect(toPascalCase('user_profiles')).toBe('UserProfiles');
    expect(toPascalCase('order_line_items')).toBe('OrderLineItems');
  });

  it('should convert kebab-case to PascalCase', () => {
    expect(toPascalCase('user-profiles')).toBe('UserProfiles');
  });

  it('should handle single words', () => {
    expect(toPascalCase('users')).toBe('Users');
    expect(toPascalCase('ORDER')).toBe('Order');
  });
});

describe('toCamelCase', () => {
  it('should convert snake_case to camelCase', () => {
    expect(toCamelCase('user_id')).toBe('userId');
    expect(toCamelCase('created_at')).toBe('createdAt');
  });
});

// =============================================================================
// BASIC TYPE MAPPING TESTS
// =============================================================================

describe('inferSchemaFromDDL - basic type mapping', () => {
  it('should map INTEGER to number', () => {
    const sql = `CREATE TABLE test (id INTEGER PRIMARY KEY)`;
    const result = inferSchemaFromDDL(sql);

    expect(result.interfaces).toHaveLength(1);
    expect(result.interfaces[0].fields[0].type).toBe('number');
  });

  it('should map TEXT to string', () => {
    const sql = `CREATE TABLE test (name TEXT NOT NULL)`;
    const result = inferSchemaFromDDL(sql);

    expect(result.interfaces[0].fields[0].type).toBe('string');
  });

  it('should map REAL to number', () => {
    const sql = `CREATE TABLE test (price REAL NOT NULL)`;
    const result = inferSchemaFromDDL(sql);

    expect(result.interfaces[0].fields[0].type).toBe('number');
  });

  it('should map BOOLEAN to boolean', () => {
    const sql = `CREATE TABLE test (active BOOLEAN NOT NULL)`;
    const result = inferSchemaFromDDL(sql);

    expect(result.interfaces[0].fields[0].type).toBe('boolean');
  });

  it('should map BLOB to Uint8Array', () => {
    const sql = `CREATE TABLE test (data BLOB)`;
    const result = inferSchemaFromDDL(sql);

    expect(result.interfaces[0].fields[0].type).toBe('Uint8Array');
  });

  it('should map JSON to unknown', () => {
    const sql = `CREATE TABLE test (metadata JSON)`;
    const result = inferSchemaFromDDL(sql);

    expect(result.interfaces[0].fields[0].type).toBe('unknown');
  });

  it('should map UUID to string', () => {
    const sql = `CREATE TABLE test (id UUID PRIMARY KEY)`;
    const result = inferSchemaFromDDL(sql);

    expect(result.interfaces[0].fields[0].type).toBe('string');
  });

  it('should map date/time types to string', () => {
    const sql = `CREATE TABLE test (
      created_at TIMESTAMP,
      updated_at DATETIME,
      birth_date DATE,
      start_time TIME
    )`;
    const result = inferSchemaFromDDL(sql);

    expect(result.interfaces[0].fields[0].type).toBe('string');
    expect(result.interfaces[0].fields[1].type).toBe('string');
    expect(result.interfaces[0].fields[2].type).toBe('string');
    expect(result.interfaces[0].fields[3].type).toBe('string');
  });

  it('should map VARCHAR and CHAR to string', () => {
    const sql = `CREATE TABLE test (
      name VARCHAR(255) NOT NULL,
      code CHAR(10) NOT NULL
    )`;
    const result = inferSchemaFromDDL(sql);

    expect(result.interfaces[0].fields[0].type).toBe('string');
    expect(result.interfaces[0].fields[1].type).toBe('string');
  });

  it('should map DECIMAL to number', () => {
    const sql = `CREATE TABLE test (price DECIMAL(10,2) NOT NULL)`;
    const result = inferSchemaFromDDL(sql);

    expect(result.interfaces[0].fields[0].type).toBe('number');
  });
});

// =============================================================================
// NULLABILITY TESTS
// =============================================================================

describe('inferSchemaFromDDL - nullability', () => {
  it('should mark NOT NULL fields as non-nullable', () => {
    const sql = `CREATE TABLE test (name TEXT NOT NULL)`;
    const result = inferSchemaFromDDL(sql);

    expect(result.interfaces[0].fields[0].nullable).toBe(false);
  });

  it('should mark fields without NOT NULL as nullable', () => {
    const sql = `CREATE TABLE test (name TEXT)`;
    const result = inferSchemaFromDDL(sql);

    expect(result.interfaces[0].fields[0].nullable).toBe(true);
  });

  it('should mark PRIMARY KEY fields as non-nullable', () => {
    const sql = `CREATE TABLE test (id INTEGER PRIMARY KEY)`;
    const result = inferSchemaFromDDL(sql);

    expect(result.interfaces[0].fields[0].nullable).toBe(false);
    expect(result.interfaces[0].fields[0].isPrimaryKey).toBe(true);
  });

  it('should generate correct TypeScript with null union', () => {
    const sql = `CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT
    )`;
    const result = inferSchemaFromDDL(sql);

    expect(result.interfaces[0].code).toContain('id: number;');
    expect(result.interfaces[0].code).toContain('name: string;');
    expect(result.interfaces[0].code).toContain('email: string | null;');
  });
});

// =============================================================================
// EXAMPLE FROM ISSUE
// =============================================================================

describe('inferSchemaFromDDL - example from issue', () => {
  it('should generate correct interface for users table', () => {
    const sql = `
      CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT,
        created_at TEXT
      )
    `;
    const result = inferSchemaFromDDL(sql);

    expect(result.interfaces).toHaveLength(1);

    const usersInterface = result.interfaces[0];
    expect(usersInterface.interfaceName).toBe('UsersRow');
    expect(usersInterface.tableName).toBe('users');

    expect(usersInterface.fields).toHaveLength(4);

    // Check each field
    const fieldMap = new Map(usersInterface.fields.map(f => [f.name, f]));

    expect(fieldMap.get('id')!.type).toBe('number');
    expect(fieldMap.get('id')!.nullable).toBe(false);
    expect(fieldMap.get('id')!.isPrimaryKey).toBe(true);

    expect(fieldMap.get('name')!.type).toBe('string');
    expect(fieldMap.get('name')!.nullable).toBe(false);

    expect(fieldMap.get('email')!.type).toBe('string');
    expect(fieldMap.get('email')!.nullable).toBe(true);

    expect(fieldMap.get('created_at')!.type).toBe('string');
    expect(fieldMap.get('created_at')!.nullable).toBe(true);

    // Check generated code
    expect(usersInterface.code).toContain('export interface UsersRow {');
    expect(usersInterface.code).toContain('id: number;');
    expect(usersInterface.code).toContain('name: string;');
    expect(usersInterface.code).toContain('email: string | null;');
    expect(usersInterface.code).toContain('created_at: string | null;');
  });
});

// =============================================================================
// MULTIPLE TABLES
// =============================================================================

describe('inferSchemaFromDDL - multiple tables', () => {
  it('should handle multiple CREATE TABLE statements', () => {
    const sql = `
      CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
      );

      CREATE TABLE orders (
        id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        total REAL NOT NULL
      );

      CREATE TABLE order_items (
        id INTEGER PRIMARY KEY,
        order_id INTEGER NOT NULL,
        product_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL
      )
    `;
    const result = inferSchemaFromDDL(sql);

    expect(result.interfaces).toHaveLength(3);
    expect(result.interfaces[0].interfaceName).toBe('UsersRow');
    expect(result.interfaces[1].interfaceName).toBe('OrdersRow');
    expect(result.interfaces[2].interfaceName).toBe('OrderItemsRow');
  });
});

// =============================================================================
// OPTIONS TESTS
// =============================================================================

describe('inferSchemaFromDDL - options', () => {
  it('should use custom interface prefix and suffix', () => {
    const sql = `CREATE TABLE users (id INTEGER PRIMARY KEY)`;
    const result = inferSchemaFromDDL(sql, {
      interfacePrefix: 'I',
      interfaceSuffix: '',
    });

    expect(result.interfaces[0].interfaceName).toBe('IUsers');
  });

  it('should use custom type mappings', () => {
    const sql = `CREATE TABLE test (status TEXT NOT NULL)`;
    const result = inferSchemaFromDDL(sql, {
      customTypeMappings: {
        TEXT: { tsType: "'active' | 'inactive'" },
      },
    });

    expect(result.interfaces[0].fields[0].type).toBe("'active' | 'inactive'");
  });

  it('should include comments when requested', () => {
    const sql = `CREATE TABLE users (id INTEGER PRIMARY KEY)`;
    const result = inferSchemaFromDDL(sql, {
      includeComments: true,
    });

    expect(result.fullCode).toContain('/**');
    expect(result.fullCode).toContain('* Row type for the "users" table');
    expect(result.interfaces[0].fields[0].comment).toContain('@primaryKey');
  });

  it('should use branded types when requested', () => {
    const sql = `CREATE TABLE test (
      id UUID PRIMARY KEY,
      created_at TIMESTAMP NOT NULL
    )`;
    const result = inferSchemaFromDDL(sql, {
      useBrandedTypes: true,
    });

    expect(result.interfaces[0].fields[0].type).toBe('UUID');
    expect(result.interfaces[0].fields[1].type).toBe('Timestamp');
    expect(result.fullCode).toContain('type UUID = Brand<string, \'UUID\'>;');
    expect(result.fullCode).toContain('type Timestamp = Brand<string, \'Timestamp\'>;');
  });
});

// =============================================================================
// HELPER TYPES
// =============================================================================

describe('inferSchemaFromDDL - helper types', () => {
  it('should generate insert type with optional auto-generated fields', () => {
    const sql = `CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )`;
    const result = inferSchemaFromDDL(sql, {
      generateHelperTypes: true,
    });

    expect(result.helpers.insertType).toBeDefined();
    expect(result.helpers.insertType).toContain('export type UsersInsertData');
    expect(result.helpers.insertType).toContain('name: string;'); // Required
    expect(result.helpers.insertType).toContain('email?: string | null;'); // Optional (nullable)
    expect(result.helpers.insertType).toContain('created_at?: string'); // Optional (has default)
  });

  it('should generate update type as partial', () => {
    const sql = `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`;
    const result = inferSchemaFromDDL(sql, {
      generateHelperTypes: true,
    });

    expect(result.helpers.insertType).toContain('export type UsersUpdateData = Partial<UsersRow>;');
  });

  it('should generate primary key type', () => {
    const sql = `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`;
    const result = inferSchemaFromDDL(sql, {
      generateHelperTypes: true,
    });

    expect(result.helpers.insertType).toContain('export type UsersPrimaryKey = number;');
  });
});

// =============================================================================
// VALIDATION FUNCTIONS
// =============================================================================

describe('inferSchemaFromDDL - validation', () => {
  it('should generate validation functions when requested', () => {
    const sql = `CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT
    )`;
    const result = inferSchemaFromDDL(sql, {
      generateValidation: true,
    });

    expect(result.fullCode).toContain('export function validateUsers');
    expect(result.fullCode).toContain('data is UsersRow');
    expect(result.fullCode).toContain("typeof obj.id === 'number'");
    expect(result.fullCode).toContain("typeof obj.name === 'string'");
  });
});

// =============================================================================
// DEFAULT VALUES
// =============================================================================

describe('inferSchemaFromDDL - default values', () => {
  it('should detect fields with default values', () => {
    const sql = `CREATE TABLE users (
      id INTEGER PRIMARY KEY,
      status TEXT DEFAULT 'pending',
      count INTEGER DEFAULT 0,
      active BOOLEAN DEFAULT TRUE
    )`;
    const result = inferSchemaFromDDL(sql);

    const fieldMap = new Map(result.interfaces[0].fields.map(f => [f.name, f]));

    expect(fieldMap.get('status')!.hasDefault).toBe(true);
    expect(fieldMap.get('status')!.defaultValue).toBe('pending');

    expect(fieldMap.get('count')!.hasDefault).toBe(true);
    expect(fieldMap.get('count')!.defaultValue).toBe(0);

    expect(fieldMap.get('active')!.hasDefault).toBe(true);
    expect(fieldMap.get('active')!.defaultValue).toBe(true);
  });
});

// =============================================================================
// CONVENIENCE FUNCTIONS
// =============================================================================

describe('inferTableType', () => {
  it('should infer a single table type', () => {
    const sql = `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`;
    const result = inferTableType(sql);

    expect(result).not.toBeNull();
    expect(result!.interfaceName).toBe('UsersRow');
    expect(result!.fields).toHaveLength(2);
  });

  it('should return null for invalid SQL', () => {
    const sql = `SELECT * FROM users`;
    const result = inferTableType(sql);

    expect(result).toBeNull();
  });
});

describe('ddlToTypeScript', () => {
  it('should return TypeScript code string', () => {
    const sql = `CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`;
    const code = ddlToTypeScript(sql);

    expect(typeof code).toBe('string');
    expect(code).toContain('export interface UsersRow');
    expect(code).toContain('Generated TypeScript types from SQL DDL');
  });
});

describe('ddlToTypeScriptWithHelpers', () => {
  it('should return TypeScript code with all helpers', () => {
    const sql = `CREATE TABLE users (id UUID PRIMARY KEY, name TEXT NOT NULL)`;
    const code = ddlToTypeScriptWithHelpers(sql);

    expect(code).toContain('type UUID = Brand<string, \'UUID\'>;');
    expect(code).toContain('export interface UsersRow');
    expect(code).toContain('export type UsersInsertData');
    expect(code).toContain('export function validateUsers');
  });
});

// =============================================================================
// EDGE CASES
// =============================================================================

describe('inferSchemaFromDDL - edge cases', () => {
  it('should handle empty SQL', () => {
    const result = inferSchemaFromDDL('');

    expect(result.interfaces).toHaveLength(0);
  });

  it('should handle invalid SQL with warnings', () => {
    const result = inferSchemaFromDDL('NOT VALID SQL');

    expect(result.interfaces).toHaveLength(0);
    expect(result.warnings.length).toBeGreaterThan(0);
  });

  it('should skip non-CREATE TABLE statements', () => {
    const sql = `
      CREATE TABLE users (id INTEGER PRIMARY KEY);
      CREATE INDEX idx_users ON users(id);
      DROP TABLE old_table;
    `;
    const result = inferSchemaFromDDL(sql);

    expect(result.interfaces).toHaveLength(1);
    expect(result.interfaces[0].tableName).toBe('users');
  });

  it('should handle table with no columns', () => {
    // This would be invalid SQL, but test parser robustness
    const sql = `CREATE TABLE empty ()`;
    const result = inferSchemaFromDDL(sql);

    // Parser should either fail or produce empty columns
    expect(result.warnings.length > 0 || result.interfaces[0]?.fields.length === 0).toBe(true);
  });

  it('should handle quoted identifiers', () => {
    const sql = `CREATE TABLE "user-data" ("full-name" TEXT NOT NULL)`;
    const result = inferSchemaFromDDL(sql);

    expect(result.interfaces).toHaveLength(1);
    expect(result.interfaces[0].tableName).toBe('user-data');
    expect(result.interfaces[0].interfaceName).toBe('UserDataRow');
    expect(result.interfaces[0].fields[0].name).toBe('full-name');
  });

  it('should handle all integer variants', () => {
    const sql = `CREATE TABLE test (
      a TINYINT,
      b SMALLINT,
      c MEDIUMINT,
      d INT,
      e INTEGER,
      f BIGINT
    )`;
    const result = inferSchemaFromDDL(sql);

    for (const field of result.interfaces[0].fields) {
      expect(field.type).toBe('number');
    }
  });

  it('should handle all float variants', () => {
    const sql = `CREATE TABLE test (
      a REAL,
      b FLOAT,
      c DOUBLE,
      d NUMERIC,
      e DECIMAL(10,2)
    )`;
    const result = inferSchemaFromDDL(sql);

    for (const field of result.interfaces[0].fields) {
      expect(field.type).toBe('number');
    }
  });

  it('should handle table-level primary key constraint', () => {
    const sql = `CREATE TABLE test (
      id INTEGER NOT NULL,
      name TEXT NOT NULL,
      PRIMARY KEY (id)
    )`;
    const result = inferSchemaFromDDL(sql);

    expect(result.interfaces[0].fields[0].name).toBe('id');
    // Note: table-level PK is handled differently - the column constraint check won't find it
    // This tests the basic parsing still works
    expect(result.interfaces[0].fields.length).toBe(2);
  });

  it('should handle composite primary keys', () => {
    const sql = `CREATE TABLE order_items (
      order_id INTEGER NOT NULL,
      item_id INTEGER NOT NULL,
      quantity INTEGER NOT NULL,
      PRIMARY KEY (order_id, item_id)
    )`;
    const result = inferSchemaFromDDL(sql);

    expect(result.interfaces[0].fields).toHaveLength(3);
  });
});

// =============================================================================
// FULL CODE OUTPUT
// =============================================================================

describe('inferSchemaFromDDL - full code output', () => {
  it('should generate complete TypeScript module', () => {
    const sql = `
      CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        email TEXT NOT NULL,
        name TEXT
      );

      CREATE TABLE posts (
        id INTEGER PRIMARY KEY,
        user_id INTEGER NOT NULL,
        title TEXT NOT NULL,
        body TEXT
      )
    `;
    const result = inferSchemaFromDDL(sql, {
      includeComments: true,
      generateHelperTypes: true,
    });

    // Check header
    expect(result.fullCode).toContain('Generated TypeScript types from SQL DDL');
    expect(result.fullCode).toContain('DO NOT EDIT');

    // Check both interfaces are present
    expect(result.fullCode).toContain('export interface UsersRow');
    expect(result.fullCode).toContain('export interface PostsRow');

    // Check helper types
    expect(result.fullCode).toContain('export type UsersInsertData');
    expect(result.fullCode).toContain('export type PostsInsertData');
    expect(result.fullCode).toContain('export type UsersUpdateData');
    expect(result.fullCode).toContain('export type PostsUpdateData');
    expect(result.fullCode).toContain('export type UsersPrimaryKey');
    expect(result.fullCode).toContain('export type PostsPrimaryKey');
  });
});
