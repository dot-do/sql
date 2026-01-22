/**
 * DoSQL PRAGMA Tests
 *
 * Comprehensive tests for PRAGMA support including:
 * - Parsing all PRAGMA statement formats
 * - Schema introspection pragmas
 * - Configuration pragmas (get/set)
 * - Application pragmas
 * - Table-valued results
 */

import { describe, it, expect, beforeEach } from 'vitest';

import {
  parsePragma,
  tryParsePragma,
  isPragmaStatement,
  formatPragma,
} from './parser.js';

import {
  createPragmaExecutor,
  createDatabaseState,
  executePragma,
  COMPILE_OPTIONS,
} from './executor.js';

import type {
  PragmaStatement,
  TableInfoRow,
  TableListRow,
  IndexListRow,
  ForeignKeyListRow,
  DatabaseListRow,
  CompileOptionRow,
} from './types.js';

import {
  isPragmaParseSuccess,
  isPragmaParseError,
  isSchemaIntrospectionPragma,
  isConfigurationPragma,
  isApplicationPragma,
  isReadOnlyPragma,
} from './types.js';

import type { TableInfo } from './executor.js';

// =============================================================================
// TEST FIXTURES
// =============================================================================

/**
 * Create a test table for schema introspection tests
 */
function createTestTable(): TableInfo {
  return {
    name: 'users',
    schema: 'main',
    type: 'table',
    columns: [
      { name: 'id', type: 'INTEGER', notNull: true, defaultValue: null, primaryKey: 1 },
      { name: 'name', type: 'TEXT', notNull: true, defaultValue: null, primaryKey: 0 },
      { name: 'email', type: 'TEXT', notNull: false, defaultValue: null, primaryKey: 0 },
      { name: 'created_at', type: 'DATETIME', notNull: true, defaultValue: 'CURRENT_TIMESTAMP', primaryKey: 0 },
    ],
    indexes: [
      {
        name: 'idx_users_email',
        tableName: 'users',
        unique: true,
        origin: 'c',
        partial: false,
        columns: [{ name: 'email', desc: false, collation: 'BINARY' }],
      },
      {
        name: 'idx_users_name',
        tableName: 'users',
        unique: false,
        origin: 'c',
        partial: false,
        columns: [{ name: 'name', desc: false, collation: 'NOCASE' }],
      },
    ],
    foreignKeys: [],
    withoutRowId: false,
    strict: false,
  };
}

/**
 * Create a test table with foreign keys
 */
function createTestTableWithForeignKeys(): TableInfo {
  return {
    name: 'posts',
    schema: 'main',
    type: 'table',
    columns: [
      { name: 'id', type: 'INTEGER', notNull: true, defaultValue: null, primaryKey: 1 },
      { name: 'user_id', type: 'INTEGER', notNull: true, defaultValue: null, primaryKey: 0 },
      { name: 'title', type: 'TEXT', notNull: true, defaultValue: null, primaryKey: 0 },
      { name: 'content', type: 'TEXT', notNull: false, defaultValue: null, primaryKey: 0 },
    ],
    indexes: [],
    foreignKeys: [
      {
        id: 0,
        columns: [{ from: 'user_id', to: 'id' }],
        table: 'users',
        onUpdate: 'CASCADE',
        onDelete: 'SET NULL',
        match: 'NONE',
      },
    ],
    withoutRowId: false,
    strict: false,
  };
}

// =============================================================================
// PARSER TESTS
// =============================================================================

describe('PRAGMA Parser', () => {
  describe('Basic PRAGMA parsing', () => {
    it('should parse simple PRAGMA name', () => {
      const result = parsePragma('PRAGMA foreign_keys');

      expect(isPragmaParseSuccess(result)).toBe(true);
      if (!isPragmaParseSuccess(result)) return;

      expect(result.statement.type).toBe('PRAGMA');
      expect(result.statement.name).toBe('foreign_keys');
      expect(result.statement.mode).toBe('get');
    });

    it('should parse PRAGMA with lowercase', () => {
      const result = parsePragma('pragma journal_mode');

      expect(isPragmaParseSuccess(result)).toBe(true);
      if (!isPragmaParseSuccess(result)) return;

      expect(result.statement.name).toBe('journal_mode');
    });

    it('should parse PRAGMA with mixed case', () => {
      const result = parsePragma('Pragma Synchronous');

      expect(isPragmaParseSuccess(result)).toBe(true);
      if (!isPragmaParseSuccess(result)) return;

      expect(result.statement.name).toBe('synchronous');
    });

    it('should parse PRAGMA with trailing semicolon', () => {
      const result = parsePragma('PRAGMA cache_size;');

      expect(isPragmaParseSuccess(result)).toBe(true);
      if (!isPragmaParseSuccess(result)) return;

      expect(result.statement.name).toBe('cache_size');
    });

    it('should parse PRAGMA with extra whitespace', () => {
      const result = parsePragma('  PRAGMA   page_size  ;  ');

      expect(isPragmaParseSuccess(result)).toBe(true);
      if (!isPragmaParseSuccess(result)) return;

      expect(result.statement.name).toBe('page_size');
    });
  });

  describe('Schema-qualified PRAGMA', () => {
    it('should parse PRAGMA schema.name', () => {
      const result = parsePragma('PRAGMA main.foreign_keys');

      expect(isPragmaParseSuccess(result)).toBe(true);
      if (!isPragmaParseSuccess(result)) return;

      expect(result.statement.schema).toBe('main');
      expect(result.statement.name).toBe('foreign_keys');
    });

    it('should parse PRAGMA temp.table_info', () => {
      const result = parsePragma('PRAGMA temp.table_list');

      expect(isPragmaParseSuccess(result)).toBe(true);
      if (!isPragmaParseSuccess(result)) return;

      expect(result.statement.schema).toBe('temp');
      expect(result.statement.name).toBe('table_list');
    });
  });

  describe('PRAGMA with value assignment', () => {
    it('should parse PRAGMA name = numeric value', () => {
      const result = parsePragma('PRAGMA cache_size = 2000');

      expect(isPragmaParseSuccess(result)).toBe(true);
      if (!isPragmaParseSuccess(result)) return;

      expect(result.statement.name).toBe('cache_size');
      expect(result.statement.mode).toBe('set');
      expect(result.statement.value).toBe(2000);
    });

    it('should parse PRAGMA name = negative value', () => {
      const result = parsePragma('PRAGMA cache_size = -2000');

      expect(isPragmaParseSuccess(result)).toBe(true);
      if (!isPragmaParseSuccess(result)) return;

      expect(result.statement.value).toBe(-2000);
    });

    it('should parse PRAGMA name = ON', () => {
      const result = parsePragma('PRAGMA foreign_keys = ON');

      expect(isPragmaParseSuccess(result)).toBe(true);
      if (!isPragmaParseSuccess(result)) return;

      expect(result.statement.value).toBe(true);
    });

    it('should parse PRAGMA name = OFF', () => {
      const result = parsePragma('PRAGMA foreign_keys = OFF');

      expect(isPragmaParseSuccess(result)).toBe(true);
      if (!isPragmaParseSuccess(result)) return;

      expect(result.statement.value).toBe(false);
    });

    it('should parse PRAGMA name = TRUE', () => {
      const result = parsePragma('PRAGMA recursive_triggers = TRUE');

      expect(isPragmaParseSuccess(result)).toBe(true);
      if (!isPragmaParseSuccess(result)) return;

      expect(result.statement.value).toBe(true);
    });

    it('should parse PRAGMA name = FALSE', () => {
      const result = parsePragma('PRAGMA recursive_triggers = FALSE');

      expect(isPragmaParseSuccess(result)).toBe(true);
      if (!isPragmaParseSuccess(result)) return;

      expect(result.statement.value).toBe(false);
    });

    it('should parse PRAGMA name = string keyword value', () => {
      const result = parsePragma('PRAGMA journal_mode = WAL');

      expect(isPragmaParseSuccess(result)).toBe(true);
      if (!isPragmaParseSuccess(result)) return;

      expect(result.statement.value).toBe('WAL');
    });

    it('should parse PRAGMA name = quoted string value', () => {
      const result = parsePragma("PRAGMA encoding = 'UTF-8'");

      expect(isPragmaParseSuccess(result)).toBe(true);
      if (!isPragmaParseSuccess(result)) return;

      expect(result.statement.value).toBe('UTF-8');
    });

    it('should parse PRAGMA name = NULL', () => {
      const result = parsePragma('PRAGMA busy_timeout = NULL');

      expect(isPragmaParseSuccess(result)).toBe(true);
      if (!isPragmaParseSuccess(result)) return;

      expect(result.statement.value).toBe(null);
    });
  });

  describe('PRAGMA with function-style argument', () => {
    it('should parse PRAGMA table_info(table)', () => {
      const result = parsePragma('PRAGMA table_info(users)');

      expect(isPragmaParseSuccess(result)).toBe(true);
      if (!isPragmaParseSuccess(result)) return;

      expect(result.statement.name).toBe('table_info');
      expect(result.statement.mode).toBe('call');
      expect(result.statement.argument).toBe('users');
    });

    it('should parse PRAGMA index_list(table)', () => {
      const result = parsePragma('PRAGMA index_list(orders)');

      expect(isPragmaParseSuccess(result)).toBe(true);
      if (!isPragmaParseSuccess(result)) return;

      expect(result.statement.name).toBe('index_list');
      expect(result.statement.argument).toBe('orders');
    });

    it('should parse PRAGMA with quoted argument', () => {
      const result = parsePragma("PRAGMA table_info('my table')");

      expect(isPragmaParseSuccess(result)).toBe(true);
      if (!isPragmaParseSuccess(result)) return;

      expect(result.statement.argument).toBe('my table');
    });

    it('should parse schema-qualified PRAGMA with argument', () => {
      const result = parsePragma('PRAGMA main.table_info(users)');

      expect(isPragmaParseSuccess(result)).toBe(true);
      if (!isPragmaParseSuccess(result)) return;

      expect(result.statement.schema).toBe('main');
      expect(result.statement.name).toBe('table_info');
      expect(result.statement.argument).toBe('users');
    });
  });

  describe('Parse errors', () => {
    it('should fail on missing PRAGMA keyword', () => {
      const result = parsePragma('foreign_keys');

      expect(isPragmaParseError(result)).toBe(true);
      if (!isPragmaParseError(result)) return;

      expect(result.error).toContain('Expected PRAGMA');
    });

    it('should fail on missing pragma name', () => {
      const result = parsePragma('PRAGMA');

      expect(isPragmaParseError(result)).toBe(true);
      if (!isPragmaParseError(result)) return;

      expect(result.error).toContain('Expected PRAGMA name');
    });

    it('should fail on missing name after schema prefix', () => {
      const result = parsePragma('PRAGMA main.');

      expect(isPragmaParseError(result)).toBe(true);
      if (!isPragmaParseError(result)) return;

      expect(result.error).toContain('after schema prefix');
    });

    it('should fail on unclosed parenthesis', () => {
      const result = parsePragma('PRAGMA table_info(users');

      expect(isPragmaParseError(result)).toBe(true);
      if (!isPragmaParseError(result)) return;

      expect(result.error).toContain('closing parenthesis');
    });

    it('should fail on unexpected characters', () => {
      const result = parsePragma('PRAGMA foreign_keys EXTRA');

      expect(isPragmaParseError(result)).toBe(true);
      if (!isPragmaParseError(result)) return;

      expect(result.error).toContain('Unexpected characters');
    });
  });

  describe('Helper functions', () => {
    it('isPragmaStatement should detect PRAGMA statements', () => {
      expect(isPragmaStatement('PRAGMA foreign_keys')).toBe(true);
      expect(isPragmaStatement('pragma table_info(users)')).toBe(true);
      expect(isPragmaStatement('SELECT * FROM users')).toBe(false);
      expect(isPragmaStatement('CREATE TABLE test (id INT)')).toBe(false);
    });

    it('tryParsePragma should return statement or null', () => {
      expect(tryParsePragma('PRAGMA foreign_keys')).not.toBeNull();
      expect(tryParsePragma('NOT A PRAGMA')).toBeNull();
    });

    it('formatPragma should recreate SQL', () => {
      const stmt: PragmaStatement = {
        type: 'PRAGMA',
        name: 'foreign_keys',
        mode: 'set',
        value: true,
      };
      expect(formatPragma(stmt)).toBe('PRAGMA foreign_keys = ON');
    });

    it('formatPragma should handle function-style', () => {
      const stmt: PragmaStatement = {
        type: 'PRAGMA',
        name: 'table_info',
        mode: 'call',
        argument: 'users',
      };
      expect(formatPragma(stmt)).toBe("PRAGMA table_info('users')");
    });
  });
});

// =============================================================================
// TYPE GUARD TESTS
// =============================================================================

describe('PRAGMA Type Guards', () => {
  it('isSchemaIntrospectionPragma should identify schema pragmas', () => {
    expect(isSchemaIntrospectionPragma('table_info')).toBe(true);
    expect(isSchemaIntrospectionPragma('TABLE_INFO')).toBe(true);
    expect(isSchemaIntrospectionPragma('index_list')).toBe(true);
    expect(isSchemaIntrospectionPragma('foreign_key_list')).toBe(true);
    expect(isSchemaIntrospectionPragma('database_list')).toBe(true);
    expect(isSchemaIntrospectionPragma('compile_options')).toBe(true);
    expect(isSchemaIntrospectionPragma('foreign_keys')).toBe(false);
  });

  it('isConfigurationPragma should identify config pragmas', () => {
    expect(isConfigurationPragma('foreign_keys')).toBe(true);
    expect(isConfigurationPragma('journal_mode')).toBe(true);
    expect(isConfigurationPragma('synchronous')).toBe(true);
    expect(isConfigurationPragma('cache_size')).toBe(true);
    expect(isConfigurationPragma('table_info')).toBe(false);
  });

  it('isApplicationPragma should identify application pragmas', () => {
    expect(isApplicationPragma('user_version')).toBe(true);
    expect(isApplicationPragma('application_id')).toBe(true);
    expect(isApplicationPragma('data_version')).toBe(true);
    expect(isApplicationPragma('schema_version')).toBe(true);
    expect(isApplicationPragma('foreign_keys')).toBe(false);
  });

  it('isReadOnlyPragma should identify read-only pragmas', () => {
    expect(isReadOnlyPragma('data_version')).toBe(true);
    expect(isReadOnlyPragma('schema_version')).toBe(true);
    expect(isReadOnlyPragma('compile_options')).toBe(true);
    expect(isReadOnlyPragma('foreign_keys')).toBe(false);
    expect(isReadOnlyPragma('user_version')).toBe(false);
  });
});

// =============================================================================
// EXECUTOR TESTS
// =============================================================================

describe('PRAGMA Executor', () => {
  describe('Schema Introspection Pragmas', () => {
    describe('table_info', () => {
      it('should return column information for a table', () => {
        const executor = createPragmaExecutor();
        executor.registerTable(createTestTable());

        const stmt = tryParsePragma('PRAGMA table_info(users)')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.rows).toHaveLength(4);

        const rows = result.rows as TableInfoRow[];
        expect(rows[0]).toEqual({
          cid: 0,
          name: 'id',
          type: 'INTEGER',
          notnull: 1,
          dflt_value: null,
          pk: 1,
        });
        expect(rows[1].name).toBe('name');
        expect(rows[2].name).toBe('email');
        expect(rows[3].name).toBe('created_at');
        expect(rows[3].dflt_value).toBe('CURRENT_TIMESTAMP');
      });

      it('should return empty for non-existent table', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA table_info(nonexistent)')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.rows).toHaveLength(0);
      });

      it('should fail without table argument', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA table_info')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(false);
        expect(result.error).toContain('requires a table name');
      });
    });

    describe('table_list', () => {
      it('should list all tables', () => {
        const executor = createPragmaExecutor();
        executor.registerTable(createTestTable());
        executor.registerTable(createTestTableWithForeignKeys());

        const stmt = tryParsePragma('PRAGMA table_list')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.rows).toHaveLength(2);

        const rows = result.rows as TableListRow[];
        const userTable = rows.find((r) => r.name === 'users');
        expect(userTable).toBeDefined();
        expect(userTable!.schema).toBe('main');
        expect(userTable!.type).toBe('table');
        expect(userTable!.ncol).toBe(4);
      });

      it('should filter by table name when provided', () => {
        const executor = createPragmaExecutor();
        executor.registerTable(createTestTable());
        executor.registerTable(createTestTableWithForeignKeys());

        const stmt = tryParsePragma('PRAGMA table_list(users)')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.rows).toHaveLength(1);
        expect((result.rows as TableListRow[])[0].name).toBe('users');
      });
    });

    describe('index_list', () => {
      it('should list indexes on a table', () => {
        const executor = createPragmaExecutor();
        executor.registerTable(createTestTable());

        const stmt = tryParsePragma('PRAGMA index_list(users)')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.rows).toHaveLength(2);

        const rows = result.rows as IndexListRow[];
        expect(rows[0].name).toBe('idx_users_email');
        expect(rows[0].unique).toBe(1);
        expect(rows[1].name).toBe('idx_users_name');
        expect(rows[1].unique).toBe(0);
      });

      it('should return empty for table without indexes', () => {
        const executor = createPragmaExecutor();
        executor.registerTable(createTestTableWithForeignKeys());

        const stmt = tryParsePragma('PRAGMA index_list(posts)')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.rows).toHaveLength(0);
      });
    });

    describe('index_info', () => {
      it('should return index column information', () => {
        const executor = createPragmaExecutor();
        executor.registerTable(createTestTable());

        const stmt = tryParsePragma('PRAGMA index_info(idx_users_email)')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.rows).toHaveLength(1);

        const rows = result.rows as Array<{ seqno: number; cid: number; name: string | null }>;
        expect(rows[0].seqno).toBe(0);
        expect(rows[0].name).toBe('email');
        expect(rows[0].cid).toBe(2); // email is 3rd column (0-indexed)
      });
    });

    describe('foreign_key_list', () => {
      it('should list foreign keys on a table', () => {
        const executor = createPragmaExecutor();
        executor.registerTable(createTestTableWithForeignKeys());

        const stmt = tryParsePragma('PRAGMA foreign_key_list(posts)')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.rows).toHaveLength(1);

        const rows = result.rows as ForeignKeyListRow[];
        expect(rows[0].id).toBe(0);
        expect(rows[0].table).toBe('users');
        expect(rows[0].from).toBe('user_id');
        expect(rows[0].to).toBe('id');
        expect(rows[0].on_update).toBe('CASCADE');
        expect(rows[0].on_delete).toBe('SET NULL');
      });

      it('should return empty for table without foreign keys', () => {
        const executor = createPragmaExecutor();
        executor.registerTable(createTestTable());

        const stmt = tryParsePragma('PRAGMA foreign_key_list(users)')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.rows).toHaveLength(0);
      });
    });

    describe('database_list', () => {
      it('should list attached databases', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA database_list')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.rows!.length).toBeGreaterThanOrEqual(2);

        const rows = result.rows as DatabaseListRow[];
        const mainDb = rows.find((r) => r.name === 'main');
        const tempDb = rows.find((r) => r.name === 'temp');

        expect(mainDb).toBeDefined();
        expect(tempDb).toBeDefined();
      });
    });

    describe('compile_options', () => {
      it('should list compile options', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA compile_options')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.rows!.length).toBeGreaterThan(0);

        const rows = result.rows as CompileOptionRow[];
        const options = rows.map((r) => r.compile_option);

        expect(options).toContain('DOSQL_CLOUDFLARE_WORKERS');
        expect(options).toContain('ENABLE_FTS5');
      });
    });
  });

  describe('Configuration Pragmas', () => {
    describe('foreign_keys', () => {
      it('should get default value (OFF)', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA foreign_keys')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.value).toBe(false);
      });

      it('should set to ON', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA foreign_keys = ON')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.value).toBe(true);

        // Verify it persists
        const getStmt = tryParsePragma('PRAGMA foreign_keys')!;
        const getResult = executor.execute(getStmt);
        expect(getResult.value).toBe(true);
      });

      it('should set to OFF', () => {
        const executor = createPragmaExecutor();

        // First enable
        executor.execute(tryParsePragma('PRAGMA foreign_keys = ON')!);

        // Then disable
        const stmt = tryParsePragma('PRAGMA foreign_keys = OFF')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.value).toBe(false);
      });

      it('should accept numeric values', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA foreign_keys = 1')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.value).toBe(true);
      });
    });

    describe('journal_mode', () => {
      it('should get default value (DELETE)', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA journal_mode')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.value).toBe('DELETE');
      });

      it('should set to WAL', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA journal_mode = WAL')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.value).toBe('WAL');
      });

      it('should set to MEMORY', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA journal_mode = MEMORY')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.value).toBe('MEMORY');
      });

      it('should reject invalid values', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA journal_mode = INVALID')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(false);
        expect(result.error).toContain('Invalid value');
      });
    });

    describe('synchronous', () => {
      it('should get default value (FULL)', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA synchronous')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.value).toBe('FULL');
      });

      it('should set to OFF', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA synchronous = OFF')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.value).toBe('OFF');
      });

      it('should set to NORMAL', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA synchronous = NORMAL')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.value).toBe('NORMAL');
      });

      it('should accept numeric values', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA synchronous = 1')!; // NORMAL
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.value).toBe('NORMAL');
      });
    });

    describe('cache_size', () => {
      it('should get default value', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA cache_size')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.value).toBe(-2000);
      });

      it('should set positive value (pages)', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA cache_size = 5000')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.value).toBe(5000);
      });

      it('should set negative value (KB)', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA cache_size = -4000')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.value).toBe(-4000);
      });
    });

    describe('page_size', () => {
      it('should get default value', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA page_size')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.value).toBe(4096);
      });

      it('should be read-only (fail on set)', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA page_size = 8192')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(false);
        expect(result.error).toContain('read-only');
      });
    });
  });

  describe('Application Pragmas', () => {
    describe('user_version', () => {
      it('should get default value (0)', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA user_version')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.value).toBe(0);
      });

      it('should set and get value', () => {
        const executor = createPragmaExecutor();

        // Set
        const setStmt = tryParsePragma('PRAGMA user_version = 42')!;
        const setResult = executor.execute(setStmt);
        expect(setResult.success).toBe(true);
        expect(setResult.value).toBe(42);

        // Get
        const getStmt = tryParsePragma('PRAGMA user_version')!;
        const getResult = executor.execute(getStmt);
        expect(getResult.value).toBe(42);
      });
    });

    describe('application_id', () => {
      it('should get default value (0)', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA application_id')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(result.value).toBe(0);
      });

      it('should set and get value', () => {
        const executor = createPragmaExecutor();

        const setStmt = tryParsePragma('PRAGMA application_id = 1234567890')!;
        executor.execute(setStmt);

        const getStmt = tryParsePragma('PRAGMA application_id')!;
        const getResult = executor.execute(getStmt);
        expect(getResult.value).toBe(1234567890);
      });
    });

    describe('data_version', () => {
      it('should get value', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA data_version')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(true);
        expect(typeof result.value).toBe('number');
      });

      it('should be read-only (fail on set)', () => {
        const executor = createPragmaExecutor();

        const stmt = tryParsePragma('PRAGMA data_version = 5')!;
        const result = executor.execute(stmt);

        expect(result.success).toBe(false);
        expect(result.error).toContain('read-only');
      });
    });
  });

  describe('Unknown Pragmas', () => {
    it('should fail on unknown pragma', () => {
      const executor = createPragmaExecutor();

      const stmt = tryParsePragma('PRAGMA unknown_pragma')!;
      const result = executor.execute(stmt);

      expect(result.success).toBe(false);
      expect(result.error).toContain('Unknown PRAGMA');
    });
  });

  describe('Schema-qualified Pragmas', () => {
    it('should support main.table_info', () => {
      const executor = createPragmaExecutor();
      executor.registerTable(createTestTable());

      const stmt = tryParsePragma('PRAGMA main.table_info(users)')!;
      const result = executor.execute(stmt);

      expect(result.success).toBe(true);
      expect(result.rows).toHaveLength(4);
    });

    it('should support schema-qualified config pragmas', () => {
      const executor = createPragmaExecutor();

      const stmt = tryParsePragma('PRAGMA main.foreign_keys = ON')!;
      const result = executor.execute(stmt);

      expect(result.success).toBe(true);
      expect(result.value).toBe(true);
    });
  });

  describe('Executor State Management', () => {
    it('should support initial state', () => {
      const executor = createPragmaExecutor({
        config: {
          foreignKeys: true,
          journalMode: 'WAL',
          synchronous: 'NORMAL',
          cacheSize: -2000,
          pageSize: 4096,
          autoVacuum: 'NONE',
          lockingMode: 'NORMAL',
          tempStore: 'DEFAULT',
          mmapSize: 0,
          maxPageCount: 1073741823,
          walAutocheckpoint: 1000,
          busyTimeout: 0,
          encoding: 'UTF-8',
          recursiveTriggers: false,
          reverseUnorderedSelects: false,
          caseSensitiveLike: false,
          ignoreCheckConstraints: false,
          deferForeignKeys: false,
          legacyAlterTable: false,
          trustedSchema: true,
        },
        application: {
          userVersion: 10,
          applicationId: 12345,
          dataVersion: 1,
          schemaVersion: 5,
        },
      });

      expect(executor.execute(tryParsePragma('PRAGMA foreign_keys')!).value).toBe(true);
      expect(executor.execute(tryParsePragma('PRAGMA journal_mode')!).value).toBe('WAL');
      expect(executor.execute(tryParsePragma('PRAGMA user_version')!).value).toBe(10);
    });

    it('should allow registering and unregistering tables', () => {
      const executor = createPragmaExecutor();

      // Register
      executor.registerTable(createTestTable());

      let result = executor.execute(tryParsePragma('PRAGMA table_info(users)')!);
      expect(result.rows).toHaveLength(4);

      // Unregister
      executor.unregisterTable('main', 'users');

      result = executor.execute(tryParsePragma('PRAGMA table_info(users)')!);
      expect(result.rows).toHaveLength(0);
    });

    it('should expose state via getState', () => {
      const executor = createPragmaExecutor();
      executor.registerTable(createTestTable());

      const state = executor.getState();

      expect(state.schemas.get('main')?.has('users')).toBe(true);
      expect(state.config.foreignKeys).toBe(false);
      expect(state.application.userVersion).toBe(0);
    });
  });
});

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

describe('PRAGMA Integration', () => {
  it('should parse and execute a complete workflow', () => {
    const executor = createPragmaExecutor();

    // Enable foreign keys
    const fkResult = executor.execute(tryParsePragma('PRAGMA foreign_keys = ON')!);
    expect(fkResult.success).toBe(true);

    // Set journal mode
    const jmResult = executor.execute(tryParsePragma('PRAGMA journal_mode = WAL')!);
    expect(jmResult.success).toBe(true);

    // Register tables
    executor.registerTable(createTestTable());
    executor.registerTable(createTestTableWithForeignKeys());

    // Query table info
    const tiResult = executor.execute(tryParsePragma('PRAGMA table_info(users)')!);
    expect(tiResult.success).toBe(true);
    expect(tiResult.rows).toHaveLength(4);

    // Query index list
    const ilResult = executor.execute(tryParsePragma('PRAGMA index_list(users)')!);
    expect(ilResult.success).toBe(true);
    expect(ilResult.rows).toHaveLength(2);

    // Query foreign keys
    const fkListResult = executor.execute(tryParsePragma('PRAGMA foreign_key_list(posts)')!);
    expect(fkListResult.success).toBe(true);
    expect(fkListResult.rows).toHaveLength(1);

    // Set user version
    const uvResult = executor.execute(tryParsePragma('PRAGMA user_version = 1')!);
    expect(uvResult.success).toBe(true);

    // Verify settings
    expect(executor.execute(tryParsePragma('PRAGMA foreign_keys')!).value).toBe(true);
    expect(executor.execute(tryParsePragma('PRAGMA journal_mode')!).value).toBe('WAL');
    expect(executor.execute(tryParsePragma('PRAGMA user_version')!).value).toBe(1);
  });

  it('should support direct executePragma function', () => {
    const state = createDatabaseState();

    // Set foreign keys
    const setResult = executePragma(
      { type: 'PRAGMA', name: 'foreign_keys', mode: 'set', value: true },
      state
    );
    expect(setResult.success).toBe(true);

    // Get foreign keys
    const getResult = executePragma(
      { type: 'PRAGMA', name: 'foreign_keys', mode: 'get' },
      state
    );
    expect(getResult.value).toBe(true);
  });
});
