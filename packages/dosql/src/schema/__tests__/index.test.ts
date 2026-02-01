/**
 * Schema Module API Tests
 *
 * Tests for the main schema API including defineSchema, table helper,
 * SchemaBuilder, and utility functions.
 *
 * Note: We import from individual submodules rather than index.js to avoid
 * pulling in the ast-parser which requires the TypeScript compiler (node:os),
 * which is unavailable in the Workers test runtime.
 */

import { describe, it, expect } from 'vitest';
import { parseField, parseTable, parseSchema } from '../parser.js';
import { validateSchema } from '../validator.js';
import { generateTypeScript, generateSql, codegen, codegenSql } from '../codegen.js';
import type { SchemaDefinition, TableDefinition } from '../types.js';

// =============================================================================
// Replicate the utility functions from index.ts for testing
// (since index.ts imports ast-parser which needs node:os)
// =============================================================================

function defineSchema<S extends SchemaDefinition>(schema: S): S {
  const result = validateSchema(schema);
  if (!result.valid) {
    const errorMessages = result.errors
      .map((e: { code: string; path: string[]; message: string }) =>
        `[${e.code}] ${e.path.join('.')}: ${e.message}`
      )
      .join('\n');
    throw new Error(`Schema validation failed:\n${errorMessages}`);
  }
  return schema;
}

function unsafeDefineSchema<S extends SchemaDefinition>(schema: S): S {
  return schema;
}

function table<T extends TableDefinition>(definition: T): T {
  return definition;
}

function getTableNames<S extends SchemaDefinition>(schema: S): (keyof S)[] {
  return Object.keys(schema).filter((k) => !k.startsWith('@')) as (keyof S)[];
}

function getTable<S extends SchemaDefinition, T extends keyof S>(
  schema: S,
  tableName: T
): S[T] {
  return schema[tableName];
}

function getFieldNames<T extends TableDefinition>(tbl: T): (keyof T)[] {
  return Object.keys(tbl) as (keyof T)[];
}

function toTypeScript(schema: SchemaDefinition): string {
  return generateTypeScript(schema).fullCode;
}

function toSql(schema: SchemaDefinition): string {
  return generateSql(schema).fullScript;
}

function validate(schema: SchemaDefinition) {
  return validateSchema(schema);
}

function parse(schema: SchemaDefinition) {
  return parseSchema(schema);
}

class SchemaBuilder<S extends SchemaDefinition = {}> {
  private schema: S;
  constructor(schema: S = {} as S) {
    this.schema = schema;
  }
  addTable<N extends string, T extends TableDefinition>(
    name: N,
    definition: T
  ): SchemaBuilder<S & { [K in N]: T }> {
    return new SchemaBuilder({
      ...this.schema,
      [name]: definition,
    } as S & { [K in N]: T });
  }
  build(): S {
    return defineSchema(this.schema as SchemaDefinition) as S;
  }
  buildUnsafe(): S {
    return this.schema;
  }
}

function createSchemaBuilder(): SchemaBuilder<{}> {
  return new SchemaBuilder();
}

// =============================================================================
// defineSchema TESTS
// =============================================================================

describe('defineSchema', () => {
  it('should return the schema object unchanged', () => {
    const schema = defineSchema({
      users: {
        id: 'uuid!',
        name: 'string',
        createdAt: 'timestamp = now()',
        updatedAt: 'timestamp = now()',
      },
    });

    expect(schema.users).toBeDefined();
    expect((schema.users as Record<string, string>).id).toBe('uuid!');
    expect((schema.users as Record<string, string>).name).toBe('string');
  });

  it('should throw for invalid schemas', () => {
    expect(() =>
      defineSchema({
        users: {},
      })
    ).toThrow('Schema validation failed');
  });

  it('should accept a valid multi-table schema', () => {
    const schema = defineSchema({
      users: {
        id: 'uuid!',
        name: 'string',
        createdAt: 'timestamp = now()',
        updatedAt: 'timestamp = now()',
      },
      orders: {
        id: 'uuid!',
        userId: '<- users',
        total: 'decimal(10,2)',
        createdAt: 'timestamp = now()',
        updatedAt: 'timestamp = now()',
      },
    });

    expect(Object.keys(schema)).toContain('users');
    expect(Object.keys(schema)).toContain('orders');
  });
});

// =============================================================================
// unsafeDefineSchema TESTS
// =============================================================================

describe('unsafeDefineSchema', () => {
  it('should return the schema without validation', () => {
    const schema = unsafeDefineSchema({
      users: {},
    });
    expect(schema.users).toBeDefined();
  });

  it('should preserve the schema structure', () => {
    const schema = unsafeDefineSchema({
      users: {
        id: 'uuid!',
        name: 'string',
      },
    });
    expect((schema.users as Record<string, string>).id).toBe('uuid!');
  });
});

// =============================================================================
// table TESTS
// =============================================================================

describe('table', () => {
  it('should return the table definition unchanged', () => {
    const users = table({
      id: 'uuid!',
      email: 'string!#',
      name: 'string',
    });

    expect(users.id).toBe('uuid!');
    expect(users.email).toBe('string!#');
    expect(users.name).toBe('string');
  });
});

// =============================================================================
// getTableNames TESTS
// =============================================================================

describe('getTableNames', () => {
  it('should return all table names', () => {
    const schema = {
      users: { id: 'uuid!' },
      orders: { id: 'uuid!' },
    };
    const names = getTableNames(schema);
    expect(names).toContain('users');
    expect(names).toContain('orders');
    expect(names).toHaveLength(2);
  });

  it('should exclude @ prefixed keys', () => {
    // Schema with directive key - getTableNames filters out directive entries
    const schema: Record<string, unknown> = {
      users: { id: 'uuid!' },
      '@index': ['email'],
    };
    const names = getTableNames(schema);
    expect(names).toContain('users');
    expect(names).not.toContain('@index');
  });

  it('should return empty array for empty schema', () => {
    expect(getTableNames({})).toHaveLength(0);
  });
});

// =============================================================================
// getTable TESTS
// =============================================================================

describe('getTable', () => {
  it('should return the specified table definition', () => {
    const schema = {
      users: { id: 'uuid!', name: 'string' },
      orders: { id: 'uuid!' },
    };
    const users = getTable(schema, 'users') as Record<string, string>;
    expect(users.id).toBe('uuid!');
    expect(users.name).toBe('string');
  });
});

// =============================================================================
// getFieldNames TESTS
// =============================================================================

describe('getFieldNames', () => {
  it('should return all field names from a table', () => {
    const tbl = { id: 'uuid!', name: 'string', email: 'string!#' };
    const fields = getFieldNames(tbl);
    expect(fields).toContain('id');
    expect(fields).toContain('name');
    expect(fields).toContain('email');
    expect(fields).toHaveLength(3);
  });

  it('should return empty array for empty table', () => {
    expect(getFieldNames({})).toHaveLength(0);
  });
});

// =============================================================================
// toTypeScript TESTS
// =============================================================================

describe('toTypeScript', () => {
  it('should generate TypeScript code string', () => {
    const result = toTypeScript({
      users: { id: 'uuid!', name: 'string' },
    });
    expect(typeof result).toBe('string');
    expect(result).toContain('export interface Users');
    expect(result).toContain('id: string');
    expect(result).toContain('name: string');
  });
});

// =============================================================================
// toSql TESTS
// =============================================================================

describe('toSql', () => {
  it('should generate SQL code string', () => {
    const result = toSql({
      users: { id: 'uuid!', name: 'string' },
    });
    expect(typeof result).toBe('string');
    expect(result).toContain('CREATE TABLE "users"');
    expect(result).toContain('UUID');
  });
});

// =============================================================================
// validate TESTS
// =============================================================================

describe('validate', () => {
  it('should return validation result', () => {
    const result = validate({
      users: { id: 'uuid!', name: 'string' },
    });
    expect(result.valid).toBe(true);
    expect(result.errors).toBeDefined();
    expect(result.warnings).toBeDefined();
  });

  it('should detect validation errors', () => {
    const result = validate({
      users: {},
    });
    expect(result.valid).toBe(false);
    expect(result.errors.length).toBeGreaterThan(0);
  });
});

// =============================================================================
// parse TESTS
// =============================================================================

describe('parse', () => {
  it('should return a parsed schema map', () => {
    const result = parse({
      users: { id: 'uuid!', name: 'string' },
    });
    expect(result).toBeInstanceOf(Map);
    expect(result.has('users')).toBe(true);
    expect(result.get('users')!.get('id')!.modifiers.primaryKey).toBe(true);
  });
});

// =============================================================================
// SchemaBuilder TESTS
// =============================================================================

describe('SchemaBuilder', () => {
  it('should build a schema using fluent API', () => {
    const builder = createSchemaBuilder();
    const schema = builder
      .addTable('users', {
        id: 'uuid!',
        name: 'string',
        createdAt: 'timestamp = now()',
        updatedAt: 'timestamp = now()',
      })
      .addTable('orders', {
        id: 'uuid!',
        userId: '<- users',
        total: 'decimal(10,2)',
        createdAt: 'timestamp = now()',
        updatedAt: 'timestamp = now()',
      })
      .build();

    expect(schema.users).toBeDefined();
    expect(schema.orders).toBeDefined();
  });

  it('should build without validation using buildUnsafe', () => {
    const builder = createSchemaBuilder();
    const schema = builder
      .addTable('empty', {})
      .buildUnsafe();

    expect(schema.empty).toBeDefined();
  });

  it('should throw on build if schema is invalid', () => {
    const builder = createSchemaBuilder();
    expect(() =>
      builder.addTable('empty', {}).build()
    ).toThrow();
  });

  it('should be immutable - addTable returns new builder', () => {
    const builder1 = createSchemaBuilder();
    const builder2 = builder1.addTable('users', { id: 'uuid!' });
    expect(builder1).not.toBe(builder2);
  });
});

// =============================================================================
// createSchemaBuilder TESTS
// =============================================================================

describe('createSchemaBuilder', () => {
  it('should create an empty schema builder', () => {
    const builder = createSchemaBuilder();
    expect(builder).toBeInstanceOf(SchemaBuilder);
    const schema = builder.buildUnsafe();
    expect(Object.keys(schema)).toHaveLength(0);
  });
});
