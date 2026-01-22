/**
 * DoSQL DDL (Data Definition Language) AST Types
 *
 * TypeScript types for representing DDL statements as an Abstract Syntax Tree.
 * Supports SQLite-compatible DDL statements including:
 * - CREATE TABLE
 * - CREATE INDEX / CREATE UNIQUE INDEX
 * - ALTER TABLE
 * - DROP TABLE / DROP INDEX
 * - CREATE VIEW
 */

// =============================================================================
// BASE TYPES
// =============================================================================

/**
 * SQLite-compatible column data types
 */
export type SqliteDataType =
  // Numeric types
  | 'INTEGER'
  | 'INT'
  | 'SMALLINT'
  | 'MEDIUMINT'
  | 'BIGINT'
  | 'TINYINT'
  | 'REAL'
  | 'DOUBLE'
  | 'DOUBLE PRECISION'
  | 'FLOAT'
  | 'NUMERIC'
  | 'DECIMAL'
  // Text types
  | 'TEXT'
  | 'VARCHAR'
  | 'CHAR'
  | 'NCHAR'
  | 'NVARCHAR'
  | 'CLOB'
  // Blob types
  | 'BLOB'
  | 'NONE'
  // Date/time types (SQLite stores as TEXT, REAL, or INTEGER)
  | 'DATE'
  | 'DATETIME'
  | 'TIMESTAMP'
  | 'TIME'
  // Boolean (stored as INTEGER 0/1 in SQLite)
  | 'BOOLEAN'
  | 'BOOL'
  // JSON (SQLite extension, stored as TEXT)
  | 'JSON'
  | 'JSONB'
  // UUID (stored as TEXT or BLOB in SQLite)
  | 'UUID';

/**
 * Column type with optional precision/scale
 */
export interface ColumnDataType {
  /** Base data type name */
  name: string;
  /** Optional precision (e.g., VARCHAR(255), DECIMAL(10,2)) */
  precision?: number;
  /** Optional scale (e.g., DECIMAL(10,2) -> scale is 2) */
  scale?: number;
}

// =============================================================================
// COLUMN CONSTRAINTS
// =============================================================================

/**
 * Conflict resolution clause (OR ROLLBACK | OR ABORT | OR FAIL | OR IGNORE | OR REPLACE)
 */
export type ConflictClause =
  | 'ROLLBACK'
  | 'ABORT'
  | 'FAIL'
  | 'IGNORE'
  | 'REPLACE';

/**
 * Reference actions for foreign keys
 */
export type ReferenceAction =
  | 'NO ACTION'
  | 'RESTRICT'
  | 'SET NULL'
  | 'SET DEFAULT'
  | 'CASCADE';

/**
 * Foreign key deferrable clause
 */
export type DeferrableClause =
  | 'DEFERRABLE'
  | 'NOT DEFERRABLE'
  | 'DEFERRABLE INITIALLY DEFERRED'
  | 'DEFERRABLE INITIALLY IMMEDIATE';

/**
 * Column-level PRIMARY KEY constraint
 */
export interface PrimaryKeyColumnConstraint {
  type: 'PRIMARY KEY';
  /** Optional constraint name */
  name?: string;
  /** Sort order for the primary key */
  order?: 'ASC' | 'DESC';
  /** Conflict resolution clause */
  onConflict?: ConflictClause;
  /** AUTOINCREMENT keyword (SQLite-specific) */
  autoincrement?: boolean;
}

/**
 * Column-level NOT NULL constraint
 */
export interface NotNullColumnConstraint {
  type: 'NOT NULL';
  /** Optional constraint name */
  name?: string;
  /** Conflict resolution clause */
  onConflict?: ConflictClause;
}

/**
 * Column-level UNIQUE constraint
 */
export interface UniqueColumnConstraint {
  type: 'UNIQUE';
  /** Optional constraint name */
  name?: string;
  /** Conflict resolution clause */
  onConflict?: ConflictClause;
}

/**
 * Column-level CHECK constraint
 */
export interface CheckColumnConstraint {
  type: 'CHECK';
  /** Optional constraint name */
  name?: string;
  /** The CHECK expression as a string */
  expression: string;
}

/**
 * Column-level DEFAULT constraint
 */
export interface DefaultColumnConstraint {
  type: 'DEFAULT';
  /** The default value (literal, expression, or function call) */
  value: string | number | boolean | null;
  /** If the value is an expression (e.g., CURRENT_TIMESTAMP) */
  isExpression?: boolean;
}

/**
 * Column-level COLLATE constraint
 */
export interface CollateColumnConstraint {
  type: 'COLLATE';
  /** Collation name (e.g., NOCASE, RTRIM, BINARY) */
  collation: string;
}

/**
 * Column-level REFERENCES (foreign key) constraint
 */
export interface ReferencesColumnConstraint {
  type: 'REFERENCES';
  /** Optional constraint name */
  name?: string;
  /** Referenced table name */
  table: string;
  /** Referenced column(s) */
  columns?: string[];
  /** ON DELETE action */
  onDelete?: ReferenceAction;
  /** ON UPDATE action */
  onUpdate?: ReferenceAction;
  /** MATCH clause */
  match?: 'SIMPLE' | 'PARTIAL' | 'FULL';
  /** Deferrable clause */
  deferrable?: DeferrableClause;
}

/**
 * Column-level GENERATED ALWAYS AS constraint (computed column)
 */
export interface GeneratedColumnConstraint {
  type: 'GENERATED';
  /** The expression that generates the value */
  expression: string;
  /** Storage type: STORED (materialized) or VIRTUAL (computed on read) */
  storage: 'STORED' | 'VIRTUAL';
}

/**
 * Union of all column-level constraints
 */
export type ColumnConstraint =
  | PrimaryKeyColumnConstraint
  | NotNullColumnConstraint
  | UniqueColumnConstraint
  | CheckColumnConstraint
  | DefaultColumnConstraint
  | CollateColumnConstraint
  | ReferencesColumnConstraint
  | GeneratedColumnConstraint;

// =============================================================================
// COLUMN DEFINITION
// =============================================================================

/**
 * Complete column definition
 */
export interface ColumnDefinition {
  /** Column name */
  name: string;
  /** Column data type */
  dataType: ColumnDataType;
  /** Column constraints */
  constraints: ColumnConstraint[];
}

// =============================================================================
// TABLE CONSTRAINTS
// =============================================================================

/**
 * Table-level PRIMARY KEY constraint
 */
export interface PrimaryKeyTableConstraint {
  type: 'PRIMARY KEY';
  /** Optional constraint name */
  name?: string;
  /** Columns included in the primary key */
  columns: Array<{
    name: string;
    order?: 'ASC' | 'DESC';
    collation?: string;
  }>;
  /** Conflict resolution clause */
  onConflict?: ConflictClause;
}

/**
 * Table-level UNIQUE constraint
 */
export interface UniqueTableConstraint {
  type: 'UNIQUE';
  /** Optional constraint name */
  name?: string;
  /** Columns included in the unique constraint */
  columns: Array<{
    name: string;
    order?: 'ASC' | 'DESC';
    collation?: string;
  }>;
  /** Conflict resolution clause */
  onConflict?: ConflictClause;
}

/**
 * Table-level FOREIGN KEY constraint
 */
export interface ForeignKeyTableConstraint {
  type: 'FOREIGN KEY';
  /** Optional constraint name */
  name?: string;
  /** Local columns */
  columns: string[];
  /** Referenced table */
  references: {
    table: string;
    columns: string[];
  };
  /** ON DELETE action */
  onDelete?: ReferenceAction;
  /** ON UPDATE action */
  onUpdate?: ReferenceAction;
  /** MATCH clause */
  match?: 'SIMPLE' | 'PARTIAL' | 'FULL';
  /** Deferrable clause */
  deferrable?: DeferrableClause;
}

/**
 * Table-level CHECK constraint
 */
export interface CheckTableConstraint {
  type: 'CHECK';
  /** Optional constraint name */
  name?: string;
  /** The CHECK expression as a string */
  expression: string;
}

/**
 * Union of all table-level constraints
 */
export type TableConstraint =
  | PrimaryKeyTableConstraint
  | UniqueTableConstraint
  | ForeignKeyTableConstraint
  | CheckTableConstraint;

// =============================================================================
// DDL STATEMENTS
// =============================================================================

/**
 * CREATE TABLE statement
 */
export interface CreateTableStatement {
  type: 'CREATE TABLE';
  /** Table name */
  name: string;
  /** Schema/database name (optional) */
  schema?: string;
  /** IF NOT EXISTS clause */
  ifNotExists?: boolean;
  /** TEMPORARY or TEMP keyword */
  temporary?: boolean;
  /** Column definitions */
  columns: ColumnDefinition[];
  /** Table-level constraints */
  constraints: TableConstraint[];
  /** WITHOUT ROWID option (SQLite-specific) */
  withoutRowId?: boolean;
  /** STRICT option (SQLite 3.37+) */
  strict?: boolean;
  /** AS SELECT clause for CREATE TABLE ... AS SELECT */
  asSelect?: string;
}

/**
 * Index column specification
 */
export interface IndexColumn {
  /** Column name or expression */
  name: string;
  /** Sort order */
  order?: 'ASC' | 'DESC';
  /** Collation */
  collation?: string;
  /** If this is an expression rather than a simple column name */
  isExpression?: boolean;
}

/**
 * CREATE INDEX statement
 */
export interface CreateIndexStatement {
  type: 'CREATE INDEX';
  /** Index name */
  name: string;
  /** Schema/database name (optional) */
  schema?: string;
  /** IF NOT EXISTS clause */
  ifNotExists?: boolean;
  /** UNIQUE index */
  unique?: boolean;
  /** Table name */
  table: string;
  /** Indexed columns or expressions */
  columns: IndexColumn[];
  /** WHERE clause for partial index */
  where?: string;
}

/**
 * ALTER TABLE ADD COLUMN operation
 */
export interface AlterTableAddColumn {
  operation: 'ADD COLUMN';
  /** Column definition to add */
  column: ColumnDefinition;
}

/**
 * ALTER TABLE DROP COLUMN operation
 */
export interface AlterTableDropColumn {
  operation: 'DROP COLUMN';
  /** Column name to drop */
  column: string;
}

/**
 * ALTER TABLE RENAME TO operation
 */
export interface AlterTableRenameTo {
  operation: 'RENAME TO';
  /** New table name */
  newName: string;
}

/**
 * ALTER TABLE RENAME COLUMN operation
 */
export interface AlterTableRenameColumn {
  operation: 'RENAME COLUMN';
  /** Old column name */
  oldName: string;
  /** New column name */
  newName: string;
}

/**
 * Union of ALTER TABLE operations
 */
export type AlterTableOperation =
  | AlterTableAddColumn
  | AlterTableDropColumn
  | AlterTableRenameTo
  | AlterTableRenameColumn;

/**
 * ALTER TABLE statement
 */
export interface AlterTableStatement {
  type: 'ALTER TABLE';
  /** Table name */
  name: string;
  /** Schema/database name (optional) */
  schema?: string;
  /** The alter operation */
  operations: AlterTableOperation[];
}

/**
 * DROP TABLE statement
 */
export interface DropTableStatement {
  type: 'DROP TABLE';
  /** Table name */
  name: string;
  /** Schema/database name (optional) */
  schema?: string;
  /** IF EXISTS clause */
  ifExists?: boolean;
}

/**
 * DROP INDEX statement
 */
export interface DropIndexStatement {
  type: 'DROP INDEX';
  /** Index name */
  name: string;
  /** Schema/database name (optional) */
  schema?: string;
  /** IF EXISTS clause */
  ifExists?: boolean;
}

/**
 * CREATE VIEW statement
 */
export interface CreateViewStatement {
  type: 'CREATE VIEW';
  /** View name */
  name: string;
  /** Schema/database name (optional) */
  schema?: string;
  /** IF NOT EXISTS clause */
  ifNotExists?: boolean;
  /** TEMPORARY or TEMP keyword */
  temporary?: boolean;
  /** Optional column list */
  columns?: string[];
  /** The SELECT statement that defines the view */
  select: string;
}

/**
 * DROP VIEW statement
 */
export interface DropViewStatement {
  type: 'DROP VIEW';
  /** View name */
  name: string;
  /** Schema/database name (optional) */
  schema?: string;
  /** IF EXISTS clause */
  ifExists?: boolean;
}

/**
 * Union of all DDL statements
 */
export type DDLStatement =
  | CreateTableStatement
  | CreateIndexStatement
  | AlterTableStatement
  | DropTableStatement
  | DropIndexStatement
  | CreateViewStatement
  | DropViewStatement;

// =============================================================================
// PARSE RESULT
// =============================================================================

/**
 * Successful parse result
 */
export interface ParseSuccess<T extends DDLStatement> {
  success: true;
  statement: T;
}

/**
 * Parse error
 */
export interface ParseError {
  success: false;
  error: string;
  position?: number;
  line?: number;
  column?: number;
}

/**
 * Parse result union
 */
export type ParseResult<T extends DDLStatement = DDLStatement> =
  | ParseSuccess<T>
  | ParseError;

// =============================================================================
// TYPE GUARDS
// =============================================================================

/**
 * Check if a statement is CREATE TABLE
 */
export function isCreateTableStatement(
  stmt: DDLStatement
): stmt is CreateTableStatement {
  return stmt.type === 'CREATE TABLE';
}

/**
 * Check if a statement is CREATE INDEX
 */
export function isCreateIndexStatement(
  stmt: DDLStatement
): stmt is CreateIndexStatement {
  return stmt.type === 'CREATE INDEX';
}

/**
 * Check if a statement is ALTER TABLE
 */
export function isAlterTableStatement(
  stmt: DDLStatement
): stmt is AlterTableStatement {
  return stmt.type === 'ALTER TABLE';
}

/**
 * Check if a statement is DROP TABLE
 */
export function isDropTableStatement(
  stmt: DDLStatement
): stmt is DropTableStatement {
  return stmt.type === 'DROP TABLE';
}

/**
 * Check if a statement is DROP INDEX
 */
export function isDropIndexStatement(
  stmt: DDLStatement
): stmt is DropIndexStatement {
  return stmt.type === 'DROP INDEX';
}

/**
 * Check if a statement is CREATE VIEW
 */
export function isCreateViewStatement(
  stmt: DDLStatement
): stmt is CreateViewStatement {
  return stmt.type === 'CREATE VIEW';
}

/**
 * Check if a statement is DROP VIEW
 */
export function isDropViewStatement(
  stmt: DDLStatement
): stmt is DropViewStatement {
  return stmt.type === 'DROP VIEW';
}

/**
 * Check if parse result is successful
 */
export function isParseSuccess<T extends DDLStatement>(
  result: ParseResult<T>
): result is ParseSuccess<T> {
  return result.success === true;
}

/**
 * Check if parse result is an error
 */
export function isParseError(result: ParseResult): result is ParseError {
  return result.success === false;
}
