/**
 * Schema Code Generation Tests
 *
 * Tests for generating TypeScript interfaces, SQL CREATE TABLE statements,
 * and validation functions from schema definitions.
 */

import { describe, it, expect } from 'vitest';
import {
  generateTypeScript,
  generateSql,
  generateCode,
  generateValidationFunctions,
  codegen,
  codegenSql,
} from '../codegen.js';

// =============================================================================
// SAMPLE SCHEMAS
// =============================================================================

const simpleSchema = {
  users: {
    id: 'uuid!',
    name: 'string',
    email: 'string#',
  },
};

const schemaWithRelations = {
  users: {
    id: 'uuid!',
    name: 'string',
    orders: '-> orders[]',
  },
  orders: {
    id: 'uuid!',
    userId: '<- users',
    total: 'decimal(10,2)',
    status: 'string = "pending"',
  },
};

const schemaWithAllTypes = {
  items: {
    id: 'uuid!',
    name: 'string',
    description: 'text?',
    count: 'int',
    price: 'float',
    amount: 'decimal(10,2)',
    active: 'boolean',
    tags: 'json[]',
    avatar: 'blob',
    createdAt: 'timestamp = now()',
  },
};

// =============================================================================
// generateTypeScript TESTS
// =============================================================================

describe('generateTypeScript', () => {
  it('should generate interfaces for all tables', () => {
    const result = generateTypeScript(simpleSchema);
    expect(result.interfaces).toHaveLength(1);
    expect(result.interfaces[0].name).toBe('Users');
  });

  it('should generate PascalCase interface names', () => {
    const result = generateTypeScript({
      user_profiles: {
        id: 'uuid!',
        bio: 'text',
      },
    });
    expect(result.interfaces[0].name).toBe('UserProfiles');
  });

  it('should map SQL types to TypeScript types', () => {
    const result = generateTypeScript(schemaWithAllTypes);
    const fields = result.interfaces[0].fields;

    const fieldMap = new Map(fields.map((f) => [f.name, f]));
    expect(fieldMap.get('id')!.type).toBe('string'); // uuid -> string
    expect(fieldMap.get('name')!.type).toBe('string');
    expect(fieldMap.get('count')!.type).toBe('number'); // int -> number
    expect(fieldMap.get('price')!.type).toBe('number'); // float -> number
    expect(fieldMap.get('amount')!.type).toBe('number'); // decimal -> number
    expect(fieldMap.get('active')!.type).toBe('boolean');
    expect(fieldMap.get('tags')!.type).toBe('unknown[]'); // json[] -> unknown[]
    expect(fieldMap.get('avatar')!.type).toBe('Uint8Array'); // blob -> Uint8Array
    expect(fieldMap.get('createdAt')!.type).toBe('Date'); // timestamp -> Date
  });

  it('should mark nullable fields as optional', () => {
    const result = generateTypeScript(schemaWithAllTypes);
    const fields = result.interfaces[0].fields;
    const descField = fields.find((f) => f.name === 'description');
    expect(descField!.optional).toBe(true);

    const nameField = fields.find((f) => f.name === 'name');
    expect(nameField!.optional).toBe(false);
  });

  it('should generate relation type references', () => {
    const result = generateTypeScript(schemaWithRelations);
    const usersFields = result.interfaces.find((i) => i.name === 'Users')!.fields;
    const ordersField = usersFields.find((f) => f.name === 'orders');
    expect(ordersField!.type).toBe('Orders[]');
  });

  it('should generate full code with header comments', () => {
    const result = generateTypeScript(simpleSchema);
    expect(result.fullCode).toContain('Generated TypeScript interfaces');
    expect(result.fullCode).toContain('DO NOT EDIT');
    expect(result.fullCode).toContain('export interface Users');
  });

  it('should include type helpers in full code', () => {
    const result = generateTypeScript(simpleSchema);
    expect(result.fullCode).toContain('InsertData');
    expect(result.fullCode).toContain('UpdateData');
    expect(result.fullCode).toContain('PrimaryKey');
  });

  it('should add comment annotations for primary key fields', () => {
    const result = generateTypeScript(simpleSchema);
    const idField = result.interfaces[0].fields.find((f) => f.name === 'id');
    expect(idField!.comment).toContain('@primaryKey');
  });

  it('should add comment annotations for indexed fields', () => {
    const result = generateTypeScript(simpleSchema);
    const emailField = result.interfaces[0].fields.find((f) => f.name === 'email');
    expect(emailField!.comment).toContain('@indexed');
    // email is 'string#' (indexed but not primary key)
    expect(emailField!.comment).not.toContain('@primaryKey');
  });
});

// =============================================================================
// generateSql TESTS
// =============================================================================

describe('generateSql', () => {
  it('should generate CREATE TABLE statements', () => {
    const result = generateSql(simpleSchema);
    expect(result.tables).toHaveLength(1);
    expect(result.tables[0].tableName).toBe('users');
    expect(result.tables[0].sql).toContain('CREATE TABLE "users"');
  });

  it('should map SQL types to column types', () => {
    const result = generateSql(schemaWithAllTypes);
    const sql = result.tables[0].sql;

    expect(sql).toContain('UUID'); // uuid -> UUID
    expect(sql).toContain('VARCHAR(255)'); // string -> VARCHAR(255)
    expect(sql).toContain('INTEGER'); // int -> INTEGER
    expect(sql).toContain('REAL'); // float -> REAL
    expect(sql).toContain('BOOLEAN'); // boolean -> BOOLEAN
    expect(sql).toContain('TIMESTAMP'); // timestamp -> TIMESTAMP
    expect(sql).toContain('BYTEA'); // blob -> BYTEA
  });

  it('should add PRIMARY KEY constraint', () => {
    const result = generateSql(simpleSchema);
    const sql = result.tables[0].sql;
    expect(sql).toContain('PRIMARY KEY');
  });

  it('should add NOT NULL for required non-PK fields', () => {
    const result = generateSql(simpleSchema);
    const sql = result.tables[0].sql;
    expect(sql).toContain('NOT NULL');
  });

  it('should generate indexes for indexed fields', () => {
    const result = generateSql(simpleSchema);
    expect(result.tables[0].indexes.length).toBeGreaterThan(0);
    expect(result.tables[0].indexes[0]).toContain('CREATE INDEX');
    expect(result.tables[0].indexes[0]).toContain('email');
  });

  it('should generate foreign keys for backward relations', () => {
    const result = generateSql(schemaWithRelations);
    const ordersTable = result.tables.find((t) => t.tableName === 'orders')!;
    expect(ordersTable.foreignKeys.length).toBeGreaterThan(0);
    expect(ordersTable.foreignKeys[0]).toContain('FOREIGN KEY');
    expect(ordersTable.foreignKeys[0]).toContain('REFERENCES "users"');
  });

  it('should skip forward relations in SQL (virtual columns)', () => {
    const result = generateSql(schemaWithRelations);
    const usersTable = result.tables.find((t) => t.tableName === 'users')!;
    const hasOrdersCol = usersTable.columns.some((c) => c.name === 'orders');
    expect(hasOrdersCol).toBe(false);
  });

  it('should handle default values', () => {
    const result = generateSql(schemaWithRelations);
    const ordersTable = result.tables.find((t) => t.tableName === 'orders')!;
    const sql = ordersTable.sql;
    expect(sql).toContain('DEFAULT');
  });

  it('should handle now() default as CURRENT_TIMESTAMP', () => {
    const result = generateSql(schemaWithAllTypes);
    const sql = result.tables[0].sql;
    expect(sql).toContain('DEFAULT CURRENT_TIMESTAMP');
  });

  it('should generate full script with header comments', () => {
    const result = generateSql(simpleSchema);
    expect(result.fullScript).toContain('Generated SQL from DoSQL schema');
    expect(result.fullScript).toContain('DO NOT EDIT');
    expect(result.fullScript).toContain('TABLES');
  });

  it('should handle topological ordering for foreign key dependencies', () => {
    const result = generateSql(schemaWithRelations);
    // Users should come before orders since orders references users
    const usersIndex = result.tables.findIndex((t) => t.tableName === 'users');
    const ordersIndex = result.tables.findIndex((t) => t.tableName === 'orders');
    expect(usersIndex).toBeLessThan(ordersIndex);
  });

  it('should include indexes section in full script when indexes exist', () => {
    const result = generateSql(simpleSchema);
    expect(result.fullScript).toContain('INDEXES');
  });

  it('should handle decimal with precision in SQL', () => {
    const result = generateSql(schemaWithRelations);
    const ordersTable = result.tables.find((t) => t.tableName === 'orders')!;
    const sql = ordersTable.sql;
    expect(sql).toContain('DECIMAL(10,2)');
  });
});

// =============================================================================
// generateValidationFunctions TESTS
// =============================================================================

describe('generateValidationFunctions', () => {
  it('should generate validation functions for all tables', () => {
    const result = generateValidationFunctions(simpleSchema);
    expect(result).toContain('validateUsers');
    expect(result).toContain('function');
    expect(result).toContain('data is Users');
  });

  it('should check required fields', () => {
    const result = generateValidationFunctions(simpleSchema);
    expect(result).toContain('is required');
    expect(result).toContain('return false');
  });

  it('should handle optional fields', () => {
    const result = generateValidationFunctions(schemaWithAllTypes);
    expect(result).toContain('is optional');
  });

  it('should generate typeof checks for basic types', () => {
    const result = generateValidationFunctions(simpleSchema);
    expect(result).toContain('typeof');
    expect(result).toContain('"string"');
  });

  it('should include header comment', () => {
    const result = generateValidationFunctions(simpleSchema);
    expect(result).toContain('Generated validation functions');
  });
});

// =============================================================================
// generateCode TESTS
// =============================================================================

describe('generateCode', () => {
  it('should generate both TypeScript and SQL', () => {
    const result = generateCode(simpleSchema);
    expect(result.typescript.interfaces.length).toBeGreaterThan(0);
    expect(result.sql.tables.length).toBeGreaterThan(0);
  });

  it('should include validation functions when requested', () => {
    const result = generateCode(simpleSchema, { includeValidation: true });
    expect(result.typescript.fullCode).toContain('validateUsers');
  });

  it('should not include validation functions by default', () => {
    const result = generateCode(simpleSchema);
    expect(result.typescript.fullCode).not.toContain('validateUsers');
  });
});

// =============================================================================
// CONVENIENCE FUNCTIONS
// =============================================================================

describe('codegen', () => {
  it('should return TypeScript code string', () => {
    const result = codegen(simpleSchema);
    expect(typeof result).toBe('string');
    expect(result).toContain('export interface Users');
  });
});

describe('codegenSql', () => {
  it('should return SQL code string', () => {
    const result = codegenSql(simpleSchema);
    expect(typeof result).toBe('string');
    expect(result).toContain('CREATE TABLE "users"');
  });
});
