/**
 * Schema Parser Tests
 *
 * Tests for the IceType-inspired DSL field parser, table parser,
 * schema parser, and type-checking utility functions.
 */

import { describe, it, expect } from 'vitest';
import {
  parseField,
  parseTable,
  parseSchema,
  isValidBaseType,
  isRelation,
  isForwardRelation,
  isBackwardRelation,
  getRelationTarget,
  isNullable,
  isPrimaryKey,
  isIndexed,
  isArrayType,
  getDefaultValue,
} from '../parser.js';
import type { SchemaDefinition } from '../types.js';

// =============================================================================
// parseField TESTS
// =============================================================================

describe('parseField', () => {
  describe('basic types', () => {
    it('should parse a simple string type', () => {
      const result = parseField('string');
      expect(result.baseType).toBe('string');
      expect(result.modifiers.required).toBe(true);
      expect(result.modifiers.primaryKey).toBe(false);
      expect(result.modifiers.nullable).toBe(false);
      expect(result.modifiers.indexed).toBe(false);
      expect(result.modifiers.isArray).toBe(false);
      expect(result.modifiers.defaultValue).toBeUndefined();
      expect(result.relation).toBeUndefined();
      expect(result.raw).toBe('string');
    });

    it('should parse int type', () => {
      const result = parseField('int');
      expect(result.baseType).toBe('int');
      expect(result.modifiers.required).toBe(true);
    });

    it('should parse integer type', () => {
      const result = parseField('integer');
      expect(result.baseType).toBe('integer');
    });

    it('should parse uuid type', () => {
      const result = parseField('uuid');
      expect(result.baseType).toBe('uuid');
    });

    it('should parse boolean type', () => {
      const result = parseField('boolean');
      expect(result.baseType).toBe('boolean');
    });

    it('should parse bool type', () => {
      const result = parseField('bool');
      expect(result.baseType).toBe('bool');
    });

    it('should parse timestamp type', () => {
      const result = parseField('timestamp');
      expect(result.baseType).toBe('timestamp');
    });

    it('should parse json type', () => {
      const result = parseField('json');
      expect(result.baseType).toBe('json');
    });

    it('should parse blob type', () => {
      const result = parseField('blob');
      expect(result.baseType).toBe('blob');
    });

    it('should parse text type', () => {
      const result = parseField('text');
      expect(result.baseType).toBe('text');
    });

    it('should normalize base type to lowercase', () => {
      const result = parseField('String');
      expect(result.baseType).toBe('string');
    });
  });

  describe('primary key modifier (!)', () => {
    it('should parse uuid! as primary key', () => {
      const result = parseField('uuid!');
      expect(result.baseType).toBe('uuid');
      expect(result.modifiers.primaryKey).toBe(true);
      expect(result.modifiers.required).toBe(true);
      expect(result.modifiers.nullable).toBe(false);
    });

    it('should parse int! as primary key', () => {
      const result = parseField('int!');
      expect(result.baseType).toBe('int');
      expect(result.modifiers.primaryKey).toBe(true);
    });
  });

  describe('indexed modifier (#)', () => {
    it('should parse string# as indexed', () => {
      const result = parseField('string#');
      expect(result.baseType).toBe('string');
      expect(result.modifiers.indexed).toBe(true);
    });

    it('should parse uuid!# as primary key and indexed', () => {
      const result = parseField('uuid!#');
      expect(result.baseType).toBe('uuid');
      expect(result.modifiers.primaryKey).toBe(true);
      expect(result.modifiers.indexed).toBe(true);
    });

    it('should parse string!# with both modifiers', () => {
      const result = parseField('string!#');
      expect(result.modifiers.primaryKey).toBe(true);
      expect(result.modifiers.indexed).toBe(true);
    });
  });

  describe('nullable modifier (?)', () => {
    it('should parse string? as nullable', () => {
      const result = parseField('string?');
      expect(result.baseType).toBe('string');
      expect(result.modifiers.nullable).toBe(true);
      expect(result.modifiers.required).toBe(false);
    });

    it('should parse timestamp? as nullable', () => {
      const result = parseField('timestamp?');
      expect(result.modifiers.nullable).toBe(true);
      expect(result.modifiers.required).toBe(false);
    });
  });

  describe('array modifier ([])', () => {
    it('should parse string[] as array', () => {
      const result = parseField('string[]');
      expect(result.baseType).toBe('string');
      expect(result.modifiers.isArray).toBe(true);
    });

    it('should parse json[] as array', () => {
      const result = parseField('json[]');
      expect(result.baseType).toBe('json');
      expect(result.modifiers.isArray).toBe(true);
    });
  });

  describe('default values', () => {
    it('should parse string with quoted default', () => {
      const result = parseField('string = "pending"');
      expect(result.baseType).toBe('string');
      expect(result.modifiers.defaultValue).toBe('pending');
    });

    it('should parse timestamp with function default', () => {
      const result = parseField('timestamp = now()');
      expect(result.baseType).toBe('timestamp');
      expect(result.modifiers.defaultValue).toBe('now()');
    });

    it('should parse boolean with default true', () => {
      const result = parseField('boolean = true');
      expect(result.baseType).toBe('boolean');
      expect(result.modifiers.defaultValue).toBe('true');
    });

    it('should parse int with numeric default', () => {
      const result = parseField('int = 0');
      expect(result.baseType).toBe('int');
      expect(result.modifiers.defaultValue).toBe('0');
    });
  });

  describe('decimal with precision', () => {
    it('should parse decimal(10,2)', () => {
      const result = parseField('decimal(10,2)');
      expect(result.baseType).toBe('decimal(10,2)');
    });

    it('should parse decimal(10,2) with primary key', () => {
      const result = parseField('decimal(10,2)!');
      expect(result.baseType).toBe('decimal(10,2)');
      expect(result.modifiers.primaryKey).toBe(true);
    });
  });

  describe('forward relations (->)', () => {
    it('should parse -> Order[] as forward to-many relation', () => {
      const result = parseField('-> Order[]');
      expect(result.relation).toBeDefined();
      expect(result.relation!.type).toBe('forward');
      expect(result.relation!.target).toBe('Order');
      expect(result.relation!.isMany).toBe(true);
      expect(result.modifiers.isArray).toBe(true);
    });

    it('should parse -> Profile as forward to-one relation', () => {
      const result = parseField('-> Profile');
      expect(result.relation).toBeDefined();
      expect(result.relation!.type).toBe('forward');
      expect(result.relation!.target).toBe('Profile');
      expect(result.relation!.isMany).toBe(false);
    });

    it('should parse ->Order[] without space', () => {
      const result = parseField('->Order[]');
      expect(result.relation!.type).toBe('forward');
      expect(result.relation!.target).toBe('Order');
      expect(result.relation!.isMany).toBe(true);
    });
  });

  describe('backward relations (<-)', () => {
    it('should parse <- users as backward relation', () => {
      const result = parseField('<- users');
      expect(result.relation).toBeDefined();
      expect(result.relation!.type).toBe('backward');
      expect(result.relation!.target).toBe('users');
      expect(result.relation!.isMany).toBe(false);
    });

    it('should parse <-users without space', () => {
      const result = parseField('<-users');
      expect(result.relation!.type).toBe('backward');
      expect(result.relation!.target).toBe('users');
    });

    it('should parse <- users[] as backward to-many relation', () => {
      const result = parseField('<- users[]');
      expect(result.relation!.type).toBe('backward');
      expect(result.relation!.target).toBe('users');
      expect(result.relation!.isMany).toBe(true);
    });
  });

  describe('combined modifiers', () => {
    it('should handle multiple modifiers: string!#', () => {
      const result = parseField('string!#');
      expect(result.modifiers.primaryKey).toBe(true);
      expect(result.modifiers.indexed).toBe(true);
      expect(result.modifiers.required).toBe(true);
    });

    it('should handle default with other modifiers', () => {
      const result = parseField('string = "pending"');
      expect(result.baseType).toBe('string');
      expect(result.modifiers.defaultValue).toBe('pending');
      expect(result.modifiers.required).toBe(true);
    });
  });

  describe('whitespace handling', () => {
    it('should trim leading whitespace', () => {
      const result = parseField('  string');
      expect(result.baseType).toBe('string');
    });

    it('should trim trailing whitespace', () => {
      const result = parseField('string  ');
      expect(result.baseType).toBe('string');
    });
  });
});

// =============================================================================
// parseTable TESTS
// =============================================================================

describe('parseTable', () => {
  it('should parse a simple table definition', () => {
    const result = parseTable({
      id: 'uuid!',
      name: 'string',
      email: 'string!#',
    });

    expect(result.size).toBe(3);
    expect(result.get('id')!.modifiers.primaryKey).toBe(true);
    expect(result.get('name')!.baseType).toBe('string');
    expect(result.get('email')!.modifiers.indexed).toBe(true);
    expect(result.get('email')!.modifiers.primaryKey).toBe(true);
  });

  it('should parse a table with relations', () => {
    const result = parseTable({
      id: 'uuid!',
      userId: '<- users',
      items: '-> Item[]',
    });

    expect(result.size).toBe(3);
    expect(result.get('userId')!.relation!.type).toBe('backward');
    expect(result.get('userId')!.relation!.target).toBe('users');
    expect(result.get('items')!.relation!.type).toBe('forward');
    expect(result.get('items')!.relation!.isMany).toBe(true);
  });

  it('should parse an empty table', () => {
    const result = parseTable({});
    expect(result.size).toBe(0);
  });

  it('should parse a table with all field types', () => {
    const result = parseTable({
      id: 'uuid!',
      name: 'string',
      bio: 'text?',
      age: 'int',
      score: 'float',
      amount: 'decimal(10,2)',
      active: 'boolean',
      tags: 'json[]',
      avatar: 'blob',
      createdAt: 'timestamp = now()',
    });

    expect(result.size).toBe(10);
    expect(result.get('bio')!.modifiers.nullable).toBe(true);
    expect(result.get('tags')!.modifiers.isArray).toBe(true);
    expect(result.get('createdAt')!.modifiers.defaultValue).toBe('now()');
  });
});

// =============================================================================
// parseSchema TESTS
// =============================================================================

describe('parseSchema', () => {
  it('should parse a schema with multiple tables', () => {
    const result = parseSchema({
      users: {
        id: 'uuid!',
        name: 'string',
        email: 'string!#',
      },
      orders: {
        id: 'uuid!',
        userId: '<- users',
        total: 'decimal(10,2)',
      },
    });

    expect(result.size).toBe(2);
    expect(result.has('users')).toBe(true);
    expect(result.has('orders')).toBe(true);

    const usersFields = result.get('users')!;
    expect(usersFields.size).toBe(3);

    const ordersFields = result.get('orders')!;
    expect(ordersFields.get('userId')!.relation!.target).toBe('users');
  });

  it('should skip schema directives (@ prefixed keys)', () => {
    // Schema with directive key - parseSchema handles mixed table and directive entries
    const result = parseSchema({
      users: {
        id: 'uuid!',
        name: 'string',
      },
      '@index': ['email'],
    } as SchemaDefinition);

    expect(result.size).toBe(1);
    expect(result.has('users')).toBe(true);
    expect(result.has('@index')).toBe(false);
  });

  it('should parse an empty schema', () => {
    const result = parseSchema({});
    expect(result.size).toBe(0);
  });
});

// =============================================================================
// TYPE CHECKING UTILITIES
// =============================================================================

describe('isValidBaseType', () => {
  it('should recognize all valid base types', () => {
    const validTypes = [
      'string', 'text', 'int', 'integer', 'bigint', 'float', 'double',
      'decimal', 'number', 'boolean', 'bool', 'uuid', 'timestamp',
      'datetime', 'date', 'time', 'json', 'jsonb', 'blob', 'binary',
    ];
    for (const type of validTypes) {
      expect(isValidBaseType(type)).toBe(true);
    }
  });

  it('should accept decimal with precision', () => {
    expect(isValidBaseType('decimal(10,2)')).toBe(true);
    expect(isValidBaseType('decimal(5)')).toBe(true);
  });

  it('should reject invalid types', () => {
    expect(isValidBaseType('varchar')).toBe(false);
    expect(isValidBaseType('char')).toBe(false);
    expect(isValidBaseType('unknown_type')).toBe(false);
    expect(isValidBaseType('')).toBe(false);
  });

  it('should be case-insensitive', () => {
    expect(isValidBaseType('STRING')).toBe(true);
    expect(isValidBaseType('Int')).toBe(true);
    expect(isValidBaseType('UUID')).toBe(true);
  });
});

describe('isRelation', () => {
  it('should return true for forward relations', () => {
    expect(isRelation('-> Order[]')).toBe(true);
    expect(isRelation('-> Profile')).toBe(true);
  });

  it('should return true for backward relations', () => {
    expect(isRelation('<- users')).toBe(true);
    expect(isRelation('<- orders[]')).toBe(true);
  });

  it('should return false for non-relations', () => {
    expect(isRelation('string')).toBe(false);
    expect(isRelation('uuid!')).toBe(false);
    expect(isRelation('int#')).toBe(false);
  });
});

describe('isForwardRelation', () => {
  it('should return true for forward relations', () => {
    expect(isForwardRelation('-> Order[]')).toBe(true);
    expect(isForwardRelation('-> Profile')).toBe(true);
  });

  it('should return false for backward relations', () => {
    expect(isForwardRelation('<- users')).toBe(false);
  });

  it('should return false for non-relations', () => {
    expect(isForwardRelation('string')).toBe(false);
  });
});

describe('isBackwardRelation', () => {
  it('should return true for backward relations', () => {
    expect(isBackwardRelation('<- users')).toBe(true);
    expect(isBackwardRelation('<- orders[]')).toBe(true);
  });

  it('should return false for forward relations', () => {
    expect(isBackwardRelation('-> Order[]')).toBe(false);
  });

  it('should return false for non-relations', () => {
    expect(isBackwardRelation('string')).toBe(false);
  });
});

describe('getRelationTarget', () => {
  it('should return target for forward relations', () => {
    expect(getRelationTarget('-> Order[]')).toBe('Order');
    expect(getRelationTarget('-> Profile')).toBe('Profile');
  });

  it('should return target for backward relations', () => {
    expect(getRelationTarget('<- users')).toBe('users');
  });

  it('should return null for non-relations', () => {
    expect(getRelationTarget('string')).toBeNull();
    expect(getRelationTarget('uuid!')).toBeNull();
  });
});

describe('isNullable', () => {
  it('should return true for nullable fields', () => {
    expect(isNullable('string?')).toBe(true);
    expect(isNullable('timestamp?')).toBe(true);
  });

  it('should return false for non-nullable fields', () => {
    expect(isNullable('string')).toBe(false);
    expect(isNullable('uuid!')).toBe(false);
  });
});

describe('isPrimaryKey', () => {
  it('should return true for primary key fields', () => {
    expect(isPrimaryKey('uuid!')).toBe(true);
    expect(isPrimaryKey('int!')).toBe(true);
    expect(isPrimaryKey('uuid!#')).toBe(true);
  });

  it('should return false for non-primary key fields', () => {
    expect(isPrimaryKey('string')).toBe(false);
    expect(isPrimaryKey('string#')).toBe(false);
    expect(isPrimaryKey('string?')).toBe(false);
  });
});

describe('isIndexed', () => {
  it('should return true for indexed fields', () => {
    expect(isIndexed('string#')).toBe(true);
    expect(isIndexed('uuid!#')).toBe(true);
  });

  it('should return false for non-indexed fields', () => {
    expect(isIndexed('string')).toBe(false);
    expect(isIndexed('uuid!')).toBe(false);
  });
});

describe('isArrayType', () => {
  it('should return true for array types', () => {
    expect(isArrayType('string[]')).toBe(true);
    expect(isArrayType('json[]')).toBe(true);
  });

  it('should return true for forward relation arrays', () => {
    expect(isArrayType('-> Order[]')).toBe(true);
  });

  it('should return false for non-array types', () => {
    expect(isArrayType('string')).toBe(false);
    expect(isArrayType('uuid!')).toBe(false);
  });
});

describe('getDefaultValue', () => {
  it('should return default value for fields with defaults', () => {
    expect(getDefaultValue('string = "pending"')).toBe('pending');
    expect(getDefaultValue('timestamp = now()')).toBe('now()');
    expect(getDefaultValue('boolean = true')).toBe('true');
    expect(getDefaultValue('int = 0')).toBe('0');
  });

  it('should return undefined for fields without defaults', () => {
    expect(getDefaultValue('string')).toBeUndefined();
    expect(getDefaultValue('uuid!')).toBeUndefined();
    expect(getDefaultValue('int#')).toBeUndefined();
  });
});
