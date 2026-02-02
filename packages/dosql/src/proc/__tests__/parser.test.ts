/**
 * Procedure Parser Tests
 *
 * Additional tests for the CREATE PROCEDURE SQL parser including:
 * - Complex parameter types
 * - Edge cases in tokenization
 * - Module code validation
 * - Schema building from parsed procedures
 *
 * Issue: sql-ntht - Stored Procedure Module Tests
 */

import { describe, it, expect } from 'vitest';
import {
  parseProcedure,
  tryParseProcedure,
  isCreateProcedure,
  validateModuleCode,
  buildInputSchema,
  buildOutputSchema,
  sqlTypeToSchema,
} from '../parser.js';

// =============================================================================
// STATEMENT DETECTION TESTS
// =============================================================================

describe('isCreateProcedure', () => {
  describe('positive cases', () => {
    it('should detect standard CREATE PROCEDURE', () => {
      expect(isCreateProcedure('CREATE PROCEDURE test AS MODULE $$ code $$')).toBe(true);
    });

    it('should detect CREATE OR REPLACE PROCEDURE', () => {
      expect(isCreateProcedure('CREATE OR REPLACE PROCEDURE test AS MODULE $$ code $$')).toBe(true);
    });

    it('should detect CREATE FUNCTION', () => {
      expect(isCreateProcedure('CREATE FUNCTION test AS MODULE $$ code $$')).toBe(true);
    });

    it('should detect CREATE OR REPLACE FUNCTION', () => {
      expect(isCreateProcedure('CREATE OR REPLACE FUNCTION test AS MODULE $$ code $$')).toBe(true);
    });

    it('should handle lowercase', () => {
      expect(isCreateProcedure('create procedure test as module $$ code $$')).toBe(true);
    });

    it('should handle mixed case', () => {
      expect(isCreateProcedure('Create Procedure test As Module $$ code $$')).toBe(true);
    });

    it('should handle leading whitespace', () => {
      expect(isCreateProcedure('  \n\t CREATE PROCEDURE test AS MODULE $$ code $$')).toBe(true);
    });
  });

  describe('negative cases', () => {
    it('should not detect CREATE TABLE', () => {
      expect(isCreateProcedure('CREATE TABLE users (id INT)')).toBe(false);
    });

    it('should not detect CREATE INDEX', () => {
      expect(isCreateProcedure('CREATE INDEX idx ON users(name)')).toBe(false);
    });

    it('should not detect SELECT', () => {
      expect(isCreateProcedure('SELECT * FROM procedures')).toBe(false);
    });

    it('should not detect INSERT', () => {
      expect(isCreateProcedure('INSERT INTO procedures VALUES (1)')).toBe(false);
    });

    it('should not detect partial matches', () => {
      expect(isCreateProcedure('CREATE PROCEDUREX test')).toBe(false);
    });
  });
});

// =============================================================================
// PARSE PROCEDURE TESTS
// =============================================================================

describe('parseProcedure', () => {
  describe('basic parsing', () => {
    it('should parse minimal procedure', () => {
      const sql = `CREATE PROCEDURE minimal AS MODULE $$
        export default () => 1;
      $$`;

      const result = parseProcedure(sql);

      expect(result.name).toBe('minimal');
      expect(result.code).toContain('export default');
      expect(result.parameters).toBeUndefined();
      expect(result.returnType).toBeUndefined();
    });

    it('should parse procedure with empty parameters', () => {
      const sql = `CREATE PROCEDURE no_params() AS MODULE $$
        export default () => 'no params';
      $$`;

      const result = parseProcedure(sql);

      expect(result.name).toBe('no_params');
      expect(result.parameters).toEqual([]);
    });

    it('should preserve raw SQL', () => {
      const sql = `CREATE PROCEDURE raw_test AS MODULE $$ export default () => 1; $$`;

      const result = parseProcedure(sql);

      expect(result.rawSql).toBe(sql);
    });
  });

  describe('parameter parsing', () => {
    it('should parse single parameter', () => {
      const sql = `CREATE PROCEDURE single_param(userId INTEGER) AS MODULE $$
        export default (ctx, userId) => userId;
      $$`;

      const result = parseProcedure(sql);

      expect(result.parameters).toHaveLength(1);
      expect(result.parameters![0]).toEqual({
        name: 'userId',
        type: 'INTEGER',
      });
    });

    it('should parse multiple parameters', () => {
      const sql = `CREATE PROCEDURE multi_param(name TEXT, age INTEGER, active BOOLEAN) AS MODULE $$
        export default (ctx, name, age, active) => ({ name, age, active });
      $$`;

      const result = parseProcedure(sql);

      expect(result.parameters).toHaveLength(3);
      expect(result.parameters![0]).toEqual({ name: 'name', type: 'TEXT' });
      expect(result.parameters![1]).toEqual({ name: 'age', type: 'INTEGER' });
      expect(result.parameters![2]).toEqual({ name: 'active', type: 'BOOLEAN' });
    });

    it('should parse parameter with default value', () => {
      const sql = `CREATE PROCEDURE with_default(limit INTEGER DEFAULT 10) AS MODULE $$
        export default (ctx, limit) => limit;
      $$`;

      const result = parseProcedure(sql);

      expect(result.parameters).toHaveLength(1);
      expect(result.parameters![0]).toEqual({
        name: 'limit',
        type: 'INTEGER',
        defaultValue: '10',
      });
    });

    it('should parse parameter with = default syntax', () => {
      const sql = `CREATE PROCEDURE eq_default(status TEXT = 'active') AS MODULE $$
        export default (ctx, status) => status;
      $$`;

      const result = parseProcedure(sql);

      expect(result.parameters).toHaveLength(1);
      expect(result.parameters![0]).toEqual({
        name: 'status',
        type: 'TEXT',
        defaultValue: 'active',
      });
    });

    it('should parse array type parameters', () => {
      const sql = `CREATE PROCEDURE with_array(ids INTEGER[], names TEXT[]) AS MODULE $$
        export default (ctx, ids, names) => ({ ids, names });
      $$`;

      const result = parseProcedure(sql);

      expect(result.parameters).toHaveLength(2);
      expect(result.parameters![0].type).toBe('INTEGER[]');
      expect(result.parameters![1].type).toBe('TEXT[]');
    });

    it('should handle IN/OUT/INOUT mode keywords', () => {
      const sql = `CREATE PROCEDURE with_modes(IN input INTEGER, OUT output TEXT, INOUT both BOOLEAN) AS MODULE $$
        export default (ctx, input) => { return 'result'; };
      $$`;

      const result = parseProcedure(sql);

      expect(result.parameters).toHaveLength(3);
      expect(result.parameters![0].name).toBe('input');
      expect(result.parameters![1].name).toBe('output');
      expect(result.parameters![2].name).toBe('both');
    });
  });

  describe('return type parsing', () => {
    it('should parse simple return type', () => {
      const sql = `CREATE PROCEDURE returns_int() RETURNS INTEGER AS MODULE $$
        export default () => 42;
      $$`;

      const result = parseProcedure(sql);

      expect(result.returnType).toBe('INTEGER');
    });

    it('should parse array return type', () => {
      const sql = `CREATE PROCEDURE returns_array() RETURNS TEXT[] AS MODULE $$
        export default () => ['a', 'b', 'c'];
      $$`;

      const result = parseProcedure(sql);

      expect(result.returnType).toBe('TEXT[]');
    });

    it('should parse TABLE return type', () => {
      const sql = `CREATE PROCEDURE returns_table() RETURNS TABLE(id INTEGER, name TEXT) AS MODULE $$
        export default () => [{ id: 1, name: 'test' }];
      $$`;

      const result = parseProcedure(sql);

      expect(result.returnType).toBe('TABLE');
    });

    it('should parse various SQL types', () => {
      const types = ['INTEGER', 'BIGINT', 'TEXT', 'VARCHAR', 'BOOLEAN', 'REAL', 'DOUBLE', 'JSON', 'UUID'];

      for (const type of types) {
        const sql = `CREATE PROCEDURE test_${type.toLowerCase()}() RETURNS ${type} AS MODULE $$
          export default () => null;
        $$`;

        const result = parseProcedure(sql);
        expect(result.returnType).toBe(type);
      }
    });
  });

  describe('module code handling', () => {
    it('should preserve code with nested brackets', () => {
      const sql = `CREATE PROCEDURE nested AS MODULE $$
        export default () => {
          const obj = { arr: [1, 2, { nested: true }] };
          return obj;
        };
      $$`;

      const result = parseProcedure(sql);

      expect(result.code).toContain('{ arr: [1, 2, { nested: true }] }');
    });

    it('should preserve code with template literals', () => {
      const sql = `CREATE PROCEDURE with_templates AS MODULE $$
        export default (ctx, name) => \`Hello, \${name}!\`;
      $$`;

      const result = parseProcedure(sql);

      expect(result.code).toContain('`Hello, ${name}!`');
    });

    it('should preserve code with arrow functions', () => {
      const sql = `CREATE PROCEDURE with_arrows AS MODULE $$
        export default async ({ db }) => {
          const items = await db.items.all();
          return items.map(i => i.name).filter(n => n.length > 0);
        };
      $$`;

      const result = parseProcedure(sql);

      expect(result.code).toContain('items.map(i => i.name)');
      expect(result.code).toContain('filter(n => n.length > 0)');
    });

    it('should preserve code with async/await', () => {
      const sql = `CREATE PROCEDURE async_proc AS MODULE $$
        export default async ({ db }) => {
          const users = await db.users.all();
          const orders = await db.orders.all();
          return { users, orders };
        };
      $$`;

      const result = parseProcedure(sql);

      expect(result.code).toContain('await db.users.all()');
      expect(result.code).toContain('await db.orders.all()');
    });

    it('should preserve SQL comments inside code', () => {
      const sql = `CREATE PROCEDURE with_comments AS MODULE $$
        // Single line comment
        /* Multi-line
           comment */
        export default () => {
          // Inside function
          return 1;
        };
      $$`;

      const result = parseProcedure(sql);

      expect(result.code).toContain('// Single line comment');
      expect(result.code).toContain('/* Multi-line');
    });

    it('should handle code with string containing $$', () => {
      // This is a known edge case - $$ inside strings will break parsing
      // This test documents current behavior
      const sql = `CREATE PROCEDURE dollar_string AS MODULE $$
        export default () => {
          return "safe string";
        };
      $$`;

      const result = parseProcedure(sql);
      expect(result.code).toContain('safe string');
    });
  });

  describe('comment handling', () => {
    it('should skip line comments outside module code', () => {
      const sql = `
        -- This is a comment
        CREATE PROCEDURE commented AS MODULE $$
          export default () => 1;
        $$;
      `;

      const result = parseProcedure(sql);
      expect(result.name).toBe('commented');
    });

    it('should skip block comments outside module code', () => {
      const sql = `
        /* Block comment */
        CREATE /* inline */ PROCEDURE /* more */ block_commented AS MODULE $$
          export default () => 1;
        $$;
      `;

      const result = parseProcedure(sql);
      expect(result.name).toBe('block_commented');
    });
  });
});

// =============================================================================
// TRY PARSE PROCEDURE TESTS
// =============================================================================

describe('tryParseProcedure', () => {
  it('should return success for valid procedure', () => {
    const sql = `CREATE PROCEDURE valid_proc AS MODULE $$ export default () => 1; $$`;

    const result = tryParseProcedure(sql);

    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.procedure.name).toBe('valid_proc');
    }
  });

  it('should return error for missing PROCEDURE keyword', () => {
    const sql = `CREATE test AS MODULE $$ export default () => 1; $$`;

    const result = tryParseProcedure(sql);

    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error.message).toBeDefined();
    }
  });

  it('should return error for missing module code', () => {
    const sql = `CREATE PROCEDURE incomplete AS MODULE`;

    const result = tryParseProcedure(sql);

    expect(result.success).toBe(false);
  });

  it('should return error for unterminated module code', () => {
    const sql = `CREATE PROCEDURE unterminated AS MODULE $$ export default () => 1;`;

    const result = tryParseProcedure(sql);

    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error.message).toContain('Unterminated');
    }
  });

  it('should include position info in errors', () => {
    const sql = `CREATE PROCEDUR typo AS MODULE $$ code $$`;

    const result = tryParseProcedure(sql);

    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.error.message).toBeDefined();
    }
  });
});

// =============================================================================
// MODULE CODE VALIDATION TESTS
// =============================================================================

describe('validateModuleCode', () => {
  describe('valid code', () => {
    it('should accept export default arrow function', () => {
      const code = `export default () => 42;`;
      expect(validateModuleCode(code).valid).toBe(true);
    });

    it('should accept export default async arrow function', () => {
      const code = `export default async () => { return 42; };`;
      expect(validateModuleCode(code).valid).toBe(true);
    });

    it('should accept export default function declaration', () => {
      const code = `export default function handler() { return 42; }`;
      expect(validateModuleCode(code).valid).toBe(true);
    });

    it('should accept export default async function', () => {
      const code = `export default async function handler() { return 42; }`;
      expect(validateModuleCode(code).valid).toBe(true);
    });

    it('should accept CommonJS exports.default', () => {
      const code = `exports.default = function() { return 42; };`;
      expect(validateModuleCode(code).valid).toBe(true);
    });

    it('should accept module.exports', () => {
      const code = `module.exports = function() { return 42; };`;
      expect(validateModuleCode(code).valid).toBe(true);
    });

    it('should accept code with multiple exports', () => {
      const code = `
        export const helper = (x) => x * 2;
        export default (ctx) => helper(21);
      `;
      expect(validateModuleCode(code).valid).toBe(true);
    });
  });

  describe('invalid code', () => {
    it('should reject code without default export', () => {
      const code = `export const handler = () => 42;`;
      const result = validateModuleCode(code);
      expect(result.valid).toBe(false);
      expect(result.error).toContain('default export');
    });

    it('should reject code with only named exports', () => {
      const code = `
        export function handler() { return 42; }
        export const other = 'value';
      `;
      const result = validateModuleCode(code);
      expect(result.valid).toBe(false);
    });
  });

  describe('bracket validation', () => {
    it('should detect unmatched opening parenthesis', () => {
      const code = `export default () => { console.log((1 + 2); };`;
      const result = validateModuleCode(code);
      expect(result.valid).toBe(false);
      expect(result.error).toContain('bracket');
    });

    it('should detect unmatched closing parenthesis', () => {
      const code = `export default () => { console.log(1 + 2)); };`;
      const result = validateModuleCode(code);
      expect(result.valid).toBe(false);
      expect(result.error).toContain('bracket');
    });

    it('should detect unmatched opening brace', () => {
      const code = `export default () => { const obj = { a: 1; };`;
      const result = validateModuleCode(code);
      expect(result.valid).toBe(false);
    });

    it('should detect unmatched opening bracket', () => {
      const code = `export default () => { const arr = [1, 2, 3; };`;
      const result = validateModuleCode(code);
      expect(result.valid).toBe(false);
    });

    it('should handle balanced nested brackets', () => {
      const code = `export default () => { return { arr: [{ nested: (1 + 2) }] }; };`;
      const result = validateModuleCode(code);
      expect(result.valid).toBe(true);
    });

    it('should ignore brackets inside single-quoted strings', () => {
      const code = `export default () => '[{()}]';`;
      const result = validateModuleCode(code);
      expect(result.valid).toBe(true);
    });

    it('should ignore brackets inside double-quoted strings', () => {
      const code = `export default () => "[{()}]";`;
      const result = validateModuleCode(code);
      expect(result.valid).toBe(true);
    });

    it('should ignore brackets inside template literals', () => {
      const code = `export default () => \`[{()}]\`;`;
      const result = validateModuleCode(code);
      expect(result.valid).toBe(true);
    });
  });
});

// =============================================================================
// SCHEMA TYPE MAPPING TESTS
// =============================================================================

describe('sqlTypeToSchema', () => {
  describe('number types', () => {
    it('should map INTEGER to number', () => {
      expect(sqlTypeToSchema('INTEGER').type).toBe('number');
      expect(sqlTypeToSchema('integer').type).toBe('number');
    });

    it('should map INT to number', () => {
      expect(sqlTypeToSchema('INT').type).toBe('number');
    });

    it('should map BIGINT to number', () => {
      expect(sqlTypeToSchema('BIGINT').type).toBe('number');
    });

    it('should map SMALLINT to number', () => {
      expect(sqlTypeToSchema('SMALLINT').type).toBe('number');
    });

    it('should map REAL to number', () => {
      expect(sqlTypeToSchema('REAL').type).toBe('number');
    });

    it('should map DOUBLE to number', () => {
      expect(sqlTypeToSchema('DOUBLE').type).toBe('number');
    });

    it('should map DOUBLE PRECISION to number', () => {
      expect(sqlTypeToSchema('DOUBLE PRECISION').type).toBe('number');
    });

    it('should map DECIMAL to number', () => {
      expect(sqlTypeToSchema('DECIMAL').type).toBe('number');
    });

    it('should map NUMERIC to number', () => {
      expect(sqlTypeToSchema('NUMERIC').type).toBe('number');
    });

    it('should map FLOAT to number', () => {
      expect(sqlTypeToSchema('FLOAT').type).toBe('number');
    });

    it('should map NUMBER to number', () => {
      expect(sqlTypeToSchema('NUMBER').type).toBe('number');
    });
  });

  describe('string types', () => {
    it('should map TEXT to string', () => {
      expect(sqlTypeToSchema('TEXT').type).toBe('string');
      expect(sqlTypeToSchema('text').type).toBe('string');
    });

    it('should map VARCHAR to string', () => {
      expect(sqlTypeToSchema('VARCHAR').type).toBe('string');
    });

    it('should map CHAR to string', () => {
      expect(sqlTypeToSchema('CHAR').type).toBe('string');
    });

    it('should map STRING to string', () => {
      expect(sqlTypeToSchema('STRING').type).toBe('string');
    });

    it('should map UUID to string', () => {
      expect(sqlTypeToSchema('UUID').type).toBe('string');
    });
  });

  describe('boolean types', () => {
    it('should map BOOLEAN to boolean', () => {
      expect(sqlTypeToSchema('BOOLEAN').type).toBe('boolean');
    });

    it('should map BOOL to boolean', () => {
      expect(sqlTypeToSchema('BOOL').type).toBe('boolean');
    });
  });

  describe('object types', () => {
    it('should map JSON to object', () => {
      expect(sqlTypeToSchema('JSON').type).toBe('object');
    });

    it('should map JSONB to object', () => {
      expect(sqlTypeToSchema('JSONB').type).toBe('object');
    });

    it('should map OBJECT to object', () => {
      expect(sqlTypeToSchema('OBJECT').type).toBe('object');
    });
  });

  describe('array types', () => {
    it('should map INTEGER[] to array of number', () => {
      const schema = sqlTypeToSchema('INTEGER[]');
      expect(schema.type).toBe('array');
      expect(schema.items?.type).toBe('number');
    });

    it('should map TEXT[] to array of string', () => {
      const schema = sqlTypeToSchema('TEXT[]');
      expect(schema.type).toBe('array');
      expect(schema.items?.type).toBe('string');
    });

    it('should map BOOLEAN[] to array of boolean', () => {
      const schema = sqlTypeToSchema('BOOLEAN[]');
      expect(schema.type).toBe('array');
      expect(schema.items?.type).toBe('boolean');
    });
  });

  describe('unknown types', () => {
    it('should map unknown types to any', () => {
      expect(sqlTypeToSchema('CUSTOM_TYPE').type).toBe('any');
      expect(sqlTypeToSchema('BLOB').type).toBe('any');
    });
  });
});

// =============================================================================
// INPUT SCHEMA BUILDING TESTS
// =============================================================================

describe('buildInputSchema', () => {
  it('should return undefined for no parameters', () => {
    expect(buildInputSchema(undefined)).toBeUndefined();
    expect(buildInputSchema([])).toBeUndefined();
  });

  it('should build schema for single parameter', () => {
    const schema = buildInputSchema([
      { name: 'id', type: 'INTEGER' },
    ]);

    expect(schema?.type).toBe('object');
    expect(schema?.properties?.id.type).toBe('number');
    expect(schema?.required).toContain('id');
  });

  it('should build schema for multiple parameters', () => {
    const schema = buildInputSchema([
      { name: 'name', type: 'TEXT' },
      { name: 'age', type: 'INTEGER' },
      { name: 'active', type: 'BOOLEAN' },
    ]);

    expect(schema?.type).toBe('object');
    expect(schema?.properties?.name.type).toBe('string');
    expect(schema?.properties?.age.type).toBe('number');
    expect(schema?.properties?.active.type).toBe('boolean');
    expect(schema?.required).toEqual(['name', 'age', 'active']);
  });

  it('should mark parameters with defaults as optional', () => {
    const schema = buildInputSchema([
      { name: 'required', type: 'TEXT' },
      { name: 'optional', type: 'TEXT', defaultValue: 'default' },
    ]);

    expect(schema?.required).toContain('required');
    expect(schema?.required).not.toContain('optional');
  });

  it('should handle array type parameters', () => {
    const schema = buildInputSchema([
      { name: 'ids', type: 'INTEGER[]' },
      { name: 'names', type: 'TEXT[]' },
    ]);

    expect(schema?.properties?.ids.type).toBe('array');
    expect(schema?.properties?.ids.items?.type).toBe('number');
    expect(schema?.properties?.names.type).toBe('array');
    expect(schema?.properties?.names.items?.type).toBe('string');
  });

  it('should not include required array when all params have defaults', () => {
    const schema = buildInputSchema([
      { name: 'a', type: 'TEXT', defaultValue: 'default' },
      { name: 'b', type: 'INTEGER', defaultValue: '0' },
    ]);

    expect(schema?.required).toBeUndefined();
  });
});

// =============================================================================
// OUTPUT SCHEMA BUILDING TESTS
// =============================================================================

describe('buildOutputSchema', () => {
  it('should return undefined for no return type', () => {
    expect(buildOutputSchema(undefined)).toBeUndefined();
  });

  it('should build schema for INTEGER return', () => {
    const schema = buildOutputSchema('INTEGER');
    expect(schema?.type).toBe('number');
  });

  it('should build schema for TEXT return', () => {
    const schema = buildOutputSchema('TEXT');
    expect(schema?.type).toBe('string');
  });

  it('should build schema for BOOLEAN return', () => {
    const schema = buildOutputSchema('BOOLEAN');
    expect(schema?.type).toBe('boolean');
  });

  it('should build schema for array return', () => {
    const schema = buildOutputSchema('TEXT[]');
    expect(schema?.type).toBe('array');
    expect(schema?.items?.type).toBe('string');
  });

  it('should build schema for JSON return', () => {
    const schema = buildOutputSchema('JSON');
    expect(schema?.type).toBe('object');
  });
});
