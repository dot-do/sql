/**
 * DoSQL DDL Parser Tests
 *
 * Comprehensive tests for DDL parsing including:
 * - CREATE TABLE with all SQLite types
 * - Primary key, foreign key, unique, not null constraints
 * - CREATE INDEX with column expressions
 * - ALTER TABLE operations
 * - DROP statements
 * - CREATE VIEW
 */

import { describe, it, expect } from 'vitest';
import {
  parseDDL,
  parseCreateTable,
  parseCreateIndex,
  parseAlterTable,
  parseDropTable,
  parseDropIndex,
  parseCreateView,
  parseDropView,
  isParseSuccess,
  isParseError,
  isCreateTableStatement,
  isCreateIndexStatement,
  isAlterTableStatement,
  isDropTableStatement,
  isDropIndexStatement,
  isCreateViewStatement,
  isDropViewStatement,
  type ColumnConstraint,
  type TableConstraint,
  type PrimaryKeyColumnConstraint,
  type DefaultColumnConstraint,
  type CheckColumnConstraint,
  type CollateColumnConstraint,
  type ReferencesColumnConstraint,
  type GeneratedColumnConstraint,
  type PrimaryKeyTableConstraint,
  type UniqueTableConstraint,
  type ForeignKeyTableConstraint,
  type CheckTableConstraint,
  type AddColumnOperation,
  type DropColumnOperation,
  type RenameTableOperation,
  type RenameColumnOperation,
} from './ddl.js';

// =============================================================================
// TYPE GUARDS FOR CONSTRAINTS
// =============================================================================

function isPrimaryKeyConstraint(c: ColumnConstraint): c is PrimaryKeyColumnConstraint {
  return c.type === 'PRIMARY KEY';
}

function isDefaultConstraint(c: ColumnConstraint): c is DefaultColumnConstraint {
  return c.type === 'DEFAULT';
}

function isCheckConstraint(c: ColumnConstraint): c is CheckColumnConstraint {
  return c.type === 'CHECK';
}

function isCollateConstraint(c: ColumnConstraint): c is CollateColumnConstraint {
  return c.type === 'COLLATE';
}

function isReferencesConstraint(c: ColumnConstraint): c is ReferencesColumnConstraint {
  return c.type === 'REFERENCES';
}

function isGeneratedConstraint(c: ColumnConstraint): c is GeneratedColumnConstraint {
  return c.type === 'GENERATED';
}

function isUniqueTableConstraint(c: TableConstraint): c is UniqueTableConstraint {
  return c.type === 'UNIQUE';
}

function isPrimaryKeyTableConstraint(c: TableConstraint): c is PrimaryKeyTableConstraint {
  return c.type === 'PRIMARY KEY';
}

function isForeignKeyTableConstraint(c: TableConstraint): c is ForeignKeyTableConstraint {
  return c.type === 'FOREIGN KEY';
}

function isCheckTableConstraint(c: TableConstraint): c is CheckTableConstraint {
  return c.type === 'CHECK';
}

function isAddColumnOp(op: { operation: string }): op is AddColumnOperation {
  return op.operation === 'ADD COLUMN';
}

function isDropColumnOp(op: { operation: string }): op is DropColumnOperation {
  return op.operation === 'DROP COLUMN';
}

function isRenameTableOp(op: { operation: string }): op is RenameTableOperation {
  return op.operation === 'RENAME TO';
}

function isRenameColumnOp(op: { operation: string }): op is RenameColumnOperation {
  return op.operation === 'RENAME COLUMN';
}

// =============================================================================
// CREATE TABLE TESTS
// =============================================================================

describe('CREATE TABLE', () => {
  describe('Basic table creation', () => {
    it('should parse simple CREATE TABLE', () => {
      const sql = 'CREATE TABLE users (id INTEGER, name TEXT)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.type).toBe('CREATE TABLE');
      expect(result.statement.name).toBe('users');
      expect(result.statement.columns).toHaveLength(2);
      expect(result.statement.columns[0].name).toBe('id');
      expect(result.statement.columns[0].dataType.name).toBe('INTEGER');
      expect(result.statement.columns[1].name).toBe('name');
      expect(result.statement.columns[1].dataType.name).toBe('TEXT');
    });

    it('should parse CREATE TABLE IF NOT EXISTS', () => {
      const sql = 'CREATE TABLE IF NOT EXISTS users (id INTEGER)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.ifNotExists).toBe(true);
      expect(result.statement.name).toBe('users');
    });

    it('should parse CREATE TEMPORARY TABLE', () => {
      const sql = 'CREATE TEMPORARY TABLE temp_data (value TEXT)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.temporary).toBe(true);
      expect(result.statement.name).toBe('temp_data');
    });

    it('should parse CREATE TEMP TABLE', () => {
      const sql = 'CREATE TEMP TABLE temp_data (value TEXT)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.temporary).toBe(true);
    });

    it('should parse schema-qualified table name', () => {
      const sql = 'CREATE TABLE main.users (id INTEGER)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.schema).toBe('main');
      expect(result.statement.name).toBe('users');
    });

    it('should parse WITHOUT ROWID option', () => {
      const sql = 'CREATE TABLE users (id INTEGER PRIMARY KEY) WITHOUT ROWID';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.withoutRowId).toBe(true);
    });

    it('should parse STRICT option', () => {
      const sql = 'CREATE TABLE users (id INTEGER) STRICT';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.strict).toBe(true);
    });

    it('should parse WITHOUT ROWID and STRICT together', () => {
      const sql = 'CREATE TABLE users (id INTEGER PRIMARY KEY) WITHOUT ROWID STRICT';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.withoutRowId).toBe(true);
      expect(result.statement.strict).toBe(true);
    });
  });

  describe('All SQLite data types', () => {
    it('should parse INTEGER types', () => {
      const sql = `CREATE TABLE t (
        a INTEGER,
        b INT,
        c SMALLINT,
        d MEDIUMINT,
        e BIGINT,
        f TINYINT
      )`;
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.columns[0].dataType.name).toBe('INTEGER');
      expect(result.statement.columns[1].dataType.name).toBe('INT');
      expect(result.statement.columns[2].dataType.name).toBe('SMALLINT');
      expect(result.statement.columns[3].dataType.name).toBe('MEDIUMINT');
      expect(result.statement.columns[4].dataType.name).toBe('BIGINT');
      expect(result.statement.columns[5].dataType.name).toBe('TINYINT');
    });

    it('should parse REAL/FLOAT types', () => {
      const sql = `CREATE TABLE t (
        a REAL,
        b DOUBLE,
        c DOUBLE PRECISION,
        d FLOAT
      )`;
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.columns[0].dataType.name).toBe('REAL');
      expect(result.statement.columns[1].dataType.name).toBe('DOUBLE');
      expect(result.statement.columns[2].dataType.name).toBe('DOUBLE PRECISION');
      expect(result.statement.columns[3].dataType.name).toBe('FLOAT');
    });

    it('should parse NUMERIC types with precision', () => {
      const sql = `CREATE TABLE t (
        a NUMERIC,
        b NUMERIC(10),
        c DECIMAL(10, 2)
      )`;
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.columns[0].dataType.name).toBe('NUMERIC');
      expect(result.statement.columns[0].dataType.precision).toBeUndefined();

      expect(result.statement.columns[1].dataType.name).toBe('NUMERIC');
      expect(result.statement.columns[1].dataType.precision).toBe(10);

      expect(result.statement.columns[2].dataType.name).toBe('DECIMAL');
      expect(result.statement.columns[2].dataType.precision).toBe(10);
      expect(result.statement.columns[2].dataType.scale).toBe(2);
    });

    it('should parse TEXT types', () => {
      const sql = `CREATE TABLE t (
        a TEXT,
        b VARCHAR(255),
        c CHAR(10),
        d CLOB
      )`;
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.columns[0].dataType.name).toBe('TEXT');
      expect(result.statement.columns[1].dataType.name).toBe('VARCHAR');
      expect(result.statement.columns[1].dataType.precision).toBe(255);
      expect(result.statement.columns[2].dataType.name).toBe('CHAR');
      expect(result.statement.columns[2].dataType.precision).toBe(10);
      expect(result.statement.columns[3].dataType.name).toBe('CLOB');
    });

    it('should parse BLOB type', () => {
      const sql = 'CREATE TABLE t (data BLOB)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.columns[0].dataType.name).toBe('BLOB');
    });

    it('should parse date/time types', () => {
      const sql = `CREATE TABLE t (
        a DATE,
        b DATETIME,
        c TIMESTAMP,
        d TIME
      )`;
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.columns[0].dataType.name).toBe('DATE');
      expect(result.statement.columns[1].dataType.name).toBe('DATETIME');
      expect(result.statement.columns[2].dataType.name).toBe('TIMESTAMP');
      expect(result.statement.columns[3].dataType.name).toBe('TIME');
    });

    it('should parse BOOLEAN types', () => {
      const sql = 'CREATE TABLE t (a BOOLEAN, b BOOL)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.columns[0].dataType.name).toBe('BOOLEAN');
      expect(result.statement.columns[1].dataType.name).toBe('BOOL');
    });

    it('should parse JSON types', () => {
      const sql = 'CREATE TABLE t (a JSON, b JSONB)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.columns[0].dataType.name).toBe('JSON');
      expect(result.statement.columns[1].dataType.name).toBe('JSONB');
    });

    it('should parse UUID type', () => {
      const sql = 'CREATE TABLE t (id UUID)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.columns[0].dataType.name).toBe('UUID');
    });
  });

  describe('Column constraints', () => {
    it('should parse PRIMARY KEY constraint', () => {
      const sql = 'CREATE TABLE users (id INTEGER PRIMARY KEY)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const pkConstraint = result.statement.columns[0].constraints.find(
        (c) => c.type === 'PRIMARY KEY'
      );
      expect(pkConstraint).toBeDefined();
    });

    it('should parse PRIMARY KEY with AUTOINCREMENT', () => {
      const sql = 'CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const pkConstraint = result.statement.columns[0].constraints.find(isPrimaryKeyConstraint);
      expect(pkConstraint).toBeDefined();
      expect(pkConstraint?.autoincrement).toBe(true);
    });

    it('should parse PRIMARY KEY with sort order', () => {
      const sql = 'CREATE TABLE users (id INTEGER PRIMARY KEY DESC)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const pkConstraint = result.statement.columns[0].constraints.find(isPrimaryKeyConstraint);
      expect(pkConstraint?.order).toBe('DESC');
    });

    it('should parse NOT NULL constraint', () => {
      const sql = 'CREATE TABLE users (name TEXT NOT NULL)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const nnConstraint = result.statement.columns[0].constraints.find(
        (c) => c.type === 'NOT NULL'
      );
      expect(nnConstraint).toBeDefined();
    });

    it('should parse UNIQUE constraint', () => {
      const sql = 'CREATE TABLE users (email TEXT UNIQUE)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const uqConstraint = result.statement.columns[0].constraints.find(
        (c) => c.type === 'UNIQUE'
      );
      expect(uqConstraint).toBeDefined();
    });

    it('should parse DEFAULT constraint with string value', () => {
      const sql = "CREATE TABLE users (status TEXT DEFAULT 'active')";
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const defConstraint = result.statement.columns[0].constraints.find(isDefaultConstraint);
      expect(defConstraint).toBeDefined();
      expect(defConstraint?.value).toBe('active');
    });

    it('should parse DEFAULT constraint with numeric value', () => {
      const sql = 'CREATE TABLE users (age INTEGER DEFAULT 0)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const defConstraint = result.statement.columns[0].constraints.find(isDefaultConstraint);
      expect(defConstraint).toBeDefined();
      expect(defConstraint?.value).toBe(0);
    });

    it('should parse DEFAULT constraint with NULL', () => {
      const sql = 'CREATE TABLE users (bio TEXT DEFAULT NULL)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const defConstraint = result.statement.columns[0].constraints.find(isDefaultConstraint);
      expect(defConstraint).toBeDefined();
      expect(defConstraint?.value).toBeNull();
    });

    it('should parse DEFAULT constraint with CURRENT_TIMESTAMP', () => {
      const sql = 'CREATE TABLE users (created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const defConstraint = result.statement.columns[0].constraints.find(isDefaultConstraint);
      expect(defConstraint).toBeDefined();
      expect(defConstraint?.value).toBe('CURRENT_TIMESTAMP');
      expect(defConstraint?.isExpression).toBe(true);
    });

    it('should parse CHECK constraint', () => {
      const sql = 'CREATE TABLE users (age INTEGER CHECK (age >= 0))';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const checkConstraint = result.statement.columns[0].constraints.find(isCheckConstraint);
      expect(checkConstraint).toBeDefined();
      expect(checkConstraint?.expression).toBe('age >= 0');
    });

    it('should parse COLLATE constraint', () => {
      const sql = 'CREATE TABLE users (name TEXT COLLATE NOCASE)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const collateConstraint = result.statement.columns[0].constraints.find(isCollateConstraint);
      expect(collateConstraint).toBeDefined();
      expect(collateConstraint?.collation).toBe('NOCASE');
    });

    it('should parse column-level REFERENCES constraint', () => {
      const sql = 'CREATE TABLE orders (user_id INTEGER REFERENCES users(id))';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const refConstraint = result.statement.columns[0].constraints.find(isReferencesConstraint);
      expect(refConstraint).toBeDefined();
      expect(refConstraint?.table).toBe('users');
      expect(refConstraint?.columns).toEqual(['id']);
    });

    it('should parse REFERENCES with ON DELETE CASCADE', () => {
      const sql = 'CREATE TABLE orders (user_id INTEGER REFERENCES users(id) ON DELETE CASCADE)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const refConstraint = result.statement.columns[0].constraints.find(isReferencesConstraint);
      expect(refConstraint?.onDelete).toBe('CASCADE');
    });

    it('should parse REFERENCES with ON UPDATE SET NULL', () => {
      const sql = 'CREATE TABLE orders (user_id INTEGER REFERENCES users(id) ON UPDATE SET NULL)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const refConstraint = result.statement.columns[0].constraints.find(isReferencesConstraint);
      expect(refConstraint?.onUpdate).toBe('SET NULL');
    });

    it('should parse GENERATED ALWAYS AS (computed column)', () => {
      const sql =
        'CREATE TABLE users (first_name TEXT, last_name TEXT, full_name TEXT GENERATED ALWAYS AS (first_name || \' \' || last_name) STORED)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const genConstraint = result.statement.columns[2].constraints.find(isGeneratedConstraint);
      expect(genConstraint).toBeDefined();
      expect(genConstraint?.storage).toBe('STORED');
    });

    it('should parse named constraint', () => {
      const sql = 'CREATE TABLE users (email TEXT CONSTRAINT email_unique UNIQUE)';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const uqConstraint = result.statement.columns[0].constraints.find(
        (c) => c.type === 'UNIQUE'
      );
      expect(uqConstraint?.name).toBe('email_unique');
    });

    it('should parse multiple constraints on one column', () => {
      const sql = "CREATE TABLE users (email TEXT NOT NULL UNIQUE DEFAULT 'unknown')";
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const constraints = result.statement.columns[0].constraints;
      expect(constraints.find((c) => c.type === 'NOT NULL')).toBeDefined();
      expect(constraints.find((c) => c.type === 'UNIQUE')).toBeDefined();
      expect(constraints.find((c) => c.type === 'DEFAULT')).toBeDefined();
    });
  });

  describe('Table constraints', () => {
    it('should parse table-level PRIMARY KEY', () => {
      const sql = 'CREATE TABLE users (id INTEGER, name TEXT, PRIMARY KEY (id))';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const pkConstraint = result.statement.constraints.find(isPrimaryKeyTableConstraint);
      expect(pkConstraint).toBeDefined();
      if (pkConstraint) {
        expect(pkConstraint.columns).toEqual([{ name: 'id' }]);
      }
    });

    it('should parse composite PRIMARY KEY', () => {
      const sql = 'CREATE TABLE order_items (order_id INTEGER, item_id INTEGER, PRIMARY KEY (order_id, item_id))';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const pkConstraint = result.statement.constraints.find(isPrimaryKeyTableConstraint);
      expect(pkConstraint).toBeDefined();
      if (pkConstraint) {
        expect(pkConstraint.columns).toHaveLength(2);
        expect(pkConstraint.columns[0].name).toBe('order_id');
        expect(pkConstraint.columns[1].name).toBe('item_id');
      }
    });

    it('should parse table-level UNIQUE', () => {
      const sql = 'CREATE TABLE users (email TEXT, phone TEXT, UNIQUE (email, phone))';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const uqConstraint = result.statement.constraints.find(isUniqueTableConstraint);
      expect(uqConstraint).toBeDefined();
      if (uqConstraint) {
        expect(uqConstraint.columns).toHaveLength(2);
      }
    });

    it('should parse table-level FOREIGN KEY', () => {
      const sql = `CREATE TABLE orders (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        FOREIGN KEY (user_id) REFERENCES users(id)
      )`;
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const fkConstraint = result.statement.constraints.find(isForeignKeyTableConstraint);
      expect(fkConstraint).toBeDefined();
      if (fkConstraint) {
        expect(fkConstraint.columns).toEqual(['user_id']);
        expect(fkConstraint.references.table).toBe('users');
        expect(fkConstraint.references.columns).toEqual(['id']);
      }
    });

    it('should parse FOREIGN KEY with ON DELETE and ON UPDATE', () => {
      const sql = `CREATE TABLE orders (
        id INTEGER PRIMARY KEY,
        user_id INTEGER,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE ON UPDATE RESTRICT
      )`;
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const fkConstraint = result.statement.constraints.find(isForeignKeyTableConstraint);
      expect(fkConstraint).toBeDefined();
      if (fkConstraint) {
        expect(fkConstraint.onDelete).toBe('CASCADE');
        expect(fkConstraint.onUpdate).toBe('RESTRICT');
      }
    });

    it('should parse table-level CHECK', () => {
      const sql = 'CREATE TABLE products (price INTEGER, discount INTEGER, CHECK (discount <= price))';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const checkConstraint = result.statement.constraints.find(isCheckTableConstraint);
      expect(checkConstraint).toBeDefined();
      if (checkConstraint) {
        expect(checkConstraint.expression).toBe('discount <= price');
      }
    });

    it('should parse named table constraint', () => {
      const sql = 'CREATE TABLE users (id INTEGER, CONSTRAINT pk_users PRIMARY KEY (id))';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      const pkConstraint = result.statement.constraints.find(isPrimaryKeyTableConstraint);
      expect(pkConstraint).toBeDefined();
      if (pkConstraint) {
        expect(pkConstraint.name).toBe('pk_users');
      }
    });
  });

  describe('CREATE TABLE AS SELECT', () => {
    it('should parse CREATE TABLE AS SELECT', () => {
      const sql = 'CREATE TABLE active_users AS SELECT * FROM users WHERE status = \'active\'';
      const result = parseCreateTable(sql);

      expect(isParseSuccess(result)).toBe(true);
      if (!isParseSuccess(result)) return;

      expect(result.statement.name).toBe('active_users');
      expect(result.statement.asSelect).toContain('SELECT');
      expect(result.statement.columns).toHaveLength(0);
    });
  });
});

// =============================================================================
// CREATE INDEX TESTS
// =============================================================================

describe('CREATE INDEX', () => {
  it('should parse simple CREATE INDEX', () => {
    const sql = 'CREATE INDEX idx_users_name ON users (name)';
    const result = parseCreateIndex(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.type).toBe('CREATE INDEX');
    expect(result.statement.name).toBe('idx_users_name');
    expect(result.statement.table).toBe('users');
    expect(result.statement.columns).toHaveLength(1);
    expect(result.statement.columns[0].name).toBe('name');
  });

  it('should parse CREATE UNIQUE INDEX', () => {
    const sql = 'CREATE UNIQUE INDEX idx_users_email ON users (email)';
    const result = parseCreateIndex(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.unique).toBe(true);
  });

  it('should parse CREATE INDEX IF NOT EXISTS', () => {
    const sql = 'CREATE INDEX IF NOT EXISTS idx_users_name ON users (name)';
    const result = parseCreateIndex(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.ifNotExists).toBe(true);
  });

  it('should parse composite index', () => {
    const sql = 'CREATE INDEX idx_users_name_email ON users (name, email)';
    const result = parseCreateIndex(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.columns).toHaveLength(2);
    expect(result.statement.columns[0].name).toBe('name');
    expect(result.statement.columns[1].name).toBe('email');
  });

  it('should parse index with sort order', () => {
    const sql = 'CREATE INDEX idx_users_created ON users (created_at DESC)';
    const result = parseCreateIndex(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.columns[0].order).toBe('DESC');
  });

  it('should parse index with collation', () => {
    const sql = 'CREATE INDEX idx_users_name ON users (name COLLATE NOCASE)';
    const result = parseCreateIndex(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.columns[0].collation).toBe('NOCASE');
  });

  it('should parse partial index with WHERE clause', () => {
    const sql = "CREATE INDEX idx_active_users ON users (name) WHERE status = 'active'";
    const result = parseCreateIndex(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.where).toContain("status = 'active'");
  });

  it('should parse index on expression', () => {
    const sql = 'CREATE INDEX idx_users_lower_name ON users ((lower(name)))';
    const result = parseCreateIndex(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.columns[0].isExpression).toBe(true);
    expect(result.statement.columns[0].name).toContain('lower');
  });

  it('should parse schema-qualified index', () => {
    const sql = 'CREATE INDEX main.idx_users_name ON users (name)';
    const result = parseCreateIndex(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.schema).toBe('main');
    expect(result.statement.name).toBe('idx_users_name');
  });
});

// =============================================================================
// ALTER TABLE TESTS
// =============================================================================

describe('ALTER TABLE', () => {
  it('should parse ALTER TABLE ADD COLUMN', () => {
    const sql = 'ALTER TABLE users ADD COLUMN email TEXT';
    const result = parseAlterTable(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.type).toBe('ALTER TABLE');
    expect(result.statement.name).toBe('users');
    expect(result.statement.operations).toHaveLength(1);
    expect(result.statement.operations[0].operation).toBe('ADD COLUMN');
    const op = result.statement.operations[0];
    if (isAddColumnOp(op)) {
      expect(op.column.name).toBe('email');
      expect(op.column.dataType.name).toBe('TEXT');
    }
  });

  it('should parse ALTER TABLE ADD (without COLUMN keyword)', () => {
    const sql = 'ALTER TABLE users ADD email TEXT';
    const result = parseAlterTable(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.operations[0].operation).toBe('ADD COLUMN');
    const op = result.statement.operations[0];
    if (isAddColumnOp(op)) {
      expect(op.column.name).toBe('email');
    }
  });

  it('should parse ALTER TABLE ADD COLUMN with constraints', () => {
    const sql = 'ALTER TABLE users ADD COLUMN age INTEGER NOT NULL DEFAULT 0';
    const result = parseAlterTable(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    const op = result.statement.operations[0];
    if (isAddColumnOp(op)) {
      const column = op.column;
      expect(column.constraints.find((c) => c.type === 'NOT NULL')).toBeDefined();
      expect(column.constraints.find((c) => c.type === 'DEFAULT')).toBeDefined();
    }
  });

  it('should parse ALTER TABLE DROP COLUMN', () => {
    const sql = 'ALTER TABLE users DROP COLUMN email';
    const result = parseAlterTable(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.operations[0].operation).toBe('DROP COLUMN');
    const op = result.statement.operations[0];
    if (isDropColumnOp(op)) {
      expect(op.column).toBe('email');
    }
  });

  it('should parse ALTER TABLE RENAME TO', () => {
    const sql = 'ALTER TABLE users RENAME TO customers';
    const result = parseAlterTable(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.operations[0].operation).toBe('RENAME TO');
    const op = result.statement.operations[0];
    if (isRenameTableOp(op)) {
      expect(op.newName).toBe('customers');
    }
  });

  it('should parse ALTER TABLE RENAME COLUMN', () => {
    const sql = 'ALTER TABLE users RENAME COLUMN email TO email_address';
    const result = parseAlterTable(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.operations[0].operation).toBe('RENAME COLUMN');
    const op = result.statement.operations[0];
    if (isRenameColumnOp(op)) {
      expect(op.oldName).toBe('email');
      expect(op.newName).toBe('email_address');
    }
  });

  it('should parse schema-qualified ALTER TABLE', () => {
    const sql = 'ALTER TABLE main.users ADD COLUMN email TEXT';
    const result = parseAlterTable(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.schema).toBe('main');
    expect(result.statement.name).toBe('users');
  });
});

// =============================================================================
// DROP STATEMENT TESTS
// =============================================================================

describe('DROP TABLE', () => {
  it('should parse simple DROP TABLE', () => {
    const sql = 'DROP TABLE users';
    const result = parseDropTable(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.type).toBe('DROP TABLE');
    expect(result.statement.name).toBe('users');
    expect(result.statement.ifExists).toBeFalsy();
  });

  it('should parse DROP TABLE IF EXISTS', () => {
    const sql = 'DROP TABLE IF EXISTS users';
    const result = parseDropTable(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.ifExists).toBe(true);
  });

  it('should parse schema-qualified DROP TABLE', () => {
    const sql = 'DROP TABLE main.users';
    const result = parseDropTable(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.schema).toBe('main');
    expect(result.statement.name).toBe('users');
  });
});

describe('DROP INDEX', () => {
  it('should parse simple DROP INDEX', () => {
    const sql = 'DROP INDEX idx_users_email';
    const result = parseDropIndex(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.type).toBe('DROP INDEX');
    expect(result.statement.name).toBe('idx_users_email');
  });

  it('should parse DROP INDEX IF EXISTS', () => {
    const sql = 'DROP INDEX IF EXISTS idx_users_email';
    const result = parseDropIndex(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.ifExists).toBe(true);
  });
});

// =============================================================================
// CREATE VIEW TESTS
// =============================================================================

describe('CREATE VIEW', () => {
  it('should parse simple CREATE VIEW', () => {
    const sql = 'CREATE VIEW active_users AS SELECT * FROM users WHERE status = \'active\'';
    const result = parseCreateView(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.type).toBe('CREATE VIEW');
    expect(result.statement.name).toBe('active_users');
    expect(result.statement.select).toContain('SELECT');
    expect(result.statement.select).toContain('users');
  });

  it('should parse CREATE VIEW IF NOT EXISTS', () => {
    const sql = 'CREATE VIEW IF NOT EXISTS active_users AS SELECT * FROM users';
    const result = parseCreateView(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.ifNotExists).toBe(true);
  });

  it('should parse CREATE TEMPORARY VIEW', () => {
    const sql = 'CREATE TEMPORARY VIEW temp_users AS SELECT * FROM users';
    const result = parseCreateView(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.temporary).toBe(true);
  });

  it('should parse CREATE VIEW with column list', () => {
    const sql = 'CREATE VIEW user_names (id, full_name) AS SELECT id, first_name || \' \' || last_name FROM users';
    const result = parseCreateView(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.columns).toEqual(['id', 'full_name']);
  });

  it('should parse schema-qualified CREATE VIEW', () => {
    const sql = 'CREATE VIEW main.active_users AS SELECT * FROM users';
    const result = parseCreateView(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.schema).toBe('main');
    expect(result.statement.name).toBe('active_users');
  });
});

describe('DROP VIEW', () => {
  it('should parse simple DROP VIEW', () => {
    const sql = 'DROP VIEW active_users';
    const result = parseDropView(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.type).toBe('DROP VIEW');
    expect(result.statement.name).toBe('active_users');
  });

  it('should parse DROP VIEW IF EXISTS', () => {
    const sql = 'DROP VIEW IF EXISTS active_users';
    const result = parseDropView(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.ifExists).toBe(true);
  });
});

// =============================================================================
// TYPE GUARDS TESTS
// =============================================================================

describe('Type guards', () => {
  it('isCreateTableStatement should work correctly', () => {
    const result = parseDDL('CREATE TABLE users (id INTEGER)');
    expect(isParseSuccess(result)).toBe(true);
    if (isParseSuccess(result)) {
      expect(isCreateTableStatement(result.statement)).toBe(true);
      expect(isCreateIndexStatement(result.statement)).toBe(false);
    }
  });

  it('isCreateIndexStatement should work correctly', () => {
    const result = parseDDL('CREATE INDEX idx ON users (id)');
    expect(isParseSuccess(result)).toBe(true);
    if (isParseSuccess(result)) {
      expect(isCreateIndexStatement(result.statement)).toBe(true);
      expect(isCreateTableStatement(result.statement)).toBe(false);
    }
  });

  it('isAlterTableStatement should work correctly', () => {
    const result = parseDDL('ALTER TABLE users ADD COLUMN email TEXT');
    expect(isParseSuccess(result)).toBe(true);
    if (isParseSuccess(result)) {
      expect(isAlterTableStatement(result.statement)).toBe(true);
    }
  });

  it('isDropTableStatement should work correctly', () => {
    const result = parseDDL('DROP TABLE users');
    expect(isParseSuccess(result)).toBe(true);
    if (isParseSuccess(result)) {
      expect(isDropTableStatement(result.statement)).toBe(true);
    }
  });

  it('isDropIndexStatement should work correctly', () => {
    const result = parseDDL('DROP INDEX idx');
    expect(isParseSuccess(result)).toBe(true);
    if (isParseSuccess(result)) {
      expect(isDropIndexStatement(result.statement)).toBe(true);
    }
  });

  it('isCreateViewStatement should work correctly', () => {
    const result = parseDDL('CREATE VIEW v AS SELECT 1');
    expect(isParseSuccess(result)).toBe(true);
    if (isParseSuccess(result)) {
      expect(isCreateViewStatement(result.statement)).toBe(true);
    }
  });

  it('isDropViewStatement should work correctly', () => {
    const result = parseDDL('DROP VIEW v');
    expect(isParseSuccess(result)).toBe(true);
    if (isParseSuccess(result)) {
      expect(isDropViewStatement(result.statement)).toBe(true);
    }
  });
});

// =============================================================================
// ERROR HANDLING TESTS
// =============================================================================

describe('Error handling', () => {
  it('should return error for invalid SQL', () => {
    const result = parseDDL('INVALID SQL STATEMENT');
    expect(isParseError(result)).toBe(true);
  });

  it('should return error for incomplete CREATE TABLE', () => {
    const result = parseDDL('CREATE TABLE');
    expect(isParseError(result)).toBe(true);
  });

  it('should return error for missing parentheses', () => {
    const result = parseDDL('CREATE TABLE users id INTEGER');
    expect(isParseError(result)).toBe(true);
  });

  it('should return specific error for wrong statement type', () => {
    const result = parseCreateTable('CREATE INDEX idx ON users (id)');
    expect(isParseError(result)).toBe(true);
    if (isParseError(result)) {
      expect(result.error).toContain('CREATE TABLE');
    }
  });
});

// =============================================================================
// EDGE CASES AND COMPLEX EXAMPLES
// =============================================================================

describe('Complex examples', () => {
  it('should parse a complete e-commerce schema', () => {
    const sql = `
      CREATE TABLE products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT,
        price DECIMAL(10, 2) NOT NULL CHECK (price >= 0),
        category_id INTEGER REFERENCES categories(id) ON DELETE SET NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP,
        UNIQUE (name)
      )
    `;
    const result = parseCreateTable(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.columns).toHaveLength(7);
    expect(result.statement.constraints).toHaveLength(1);

    // Check PRIMARY KEY with AUTOINCREMENT
    const idCol = result.statement.columns[0];
    const pkConstraint = idCol.constraints.find(isPrimaryKeyConstraint);
    expect(pkConstraint).toBeDefined();
    if (pkConstraint) {
      expect(pkConstraint.autoincrement).toBe(true);
    }

    // Check DECIMAL precision
    const priceCol = result.statement.columns.find((c) => c.name === 'price');
    expect(priceCol?.dataType.precision).toBe(10);
    expect(priceCol?.dataType.scale).toBe(2);

    // Check CHECK constraint
    const checkConstraint = priceCol?.constraints.find(isCheckConstraint);
    expect(checkConstraint).toBeDefined();

    // Check REFERENCES
    const categoryCol = result.statement.columns.find((c) => c.name === 'category_id');
    const refConstraint = categoryCol?.constraints.find(isReferencesConstraint);
    expect(refConstraint).toBeDefined();
    if (refConstraint) {
      expect(refConstraint.table).toBe('categories');
      expect(refConstraint.onDelete).toBe('SET NULL');
    }
  });

  it('should parse a table with quoted identifiers', () => {
    const sql = 'CREATE TABLE "user table" ("user id" INTEGER, "user name" TEXT)';
    const result = parseCreateTable(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.name).toBe('user table');
    expect(result.statement.columns[0].name).toBe('user id');
    expect(result.statement.columns[1].name).toBe('user name');
  });

  it('should handle SQL comments', () => {
    const sql = `
      -- This is a comment
      CREATE TABLE users (
        id INTEGER, -- inline comment
        /* multi-line
           comment */
        name TEXT
      )
    `;
    const result = parseCreateTable(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.columns).toHaveLength(2);
  });

  it('should parse case-insensitive keywords', () => {
    const sql = 'create TABLE Users (ID integer PRIMARY KEY, Name text NOT NULL)';
    const result = parseCreateTable(sql);

    expect(isParseSuccess(result)).toBe(true);
    if (!isParseSuccess(result)) return;

    expect(result.statement.name).toBe('Users');
    expect(result.statement.columns[0].name).toBe('ID');
  });
});
