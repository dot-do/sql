/**
 * DoSQL Constraint Types
 *
 * Type definitions for foreign keys and constraints support.
 * Implements SQL standard constraint semantics.
 */

// =============================================================================
// CONSTRAINT TYPES
// =============================================================================

/**
 * Constraint type enumeration
 */
export type ConstraintType =
  | 'PRIMARY_KEY'
  | 'FOREIGN_KEY'
  | 'UNIQUE'
  | 'NOT_NULL'
  | 'CHECK'
  | 'DEFAULT';

/**
 * Foreign key referential actions
 */
export type ReferentialAction =
  | 'CASCADE'
  | 'SET_NULL'
  | 'SET_DEFAULT'
  | 'RESTRICT'
  | 'NO_ACTION';

/**
 * Constraint timing for deferred constraints
 */
export type ConstraintTiming = 'IMMEDIATE' | 'DEFERRED';

/**
 * Constraint deferrable state
 */
export type DeferrableState =
  | 'NOT_DEFERRABLE'
  | 'DEFERRABLE_INITIALLY_IMMEDIATE'
  | 'DEFERRABLE_INITIALLY_DEFERRED';

// =============================================================================
// BASE CONSTRAINT INTERFACE
// =============================================================================

/**
 * Base constraint interface
 */
export interface Constraint {
  /** Constraint name (auto-generated if not specified) */
  name: string;
  /** Constraint type */
  type: ConstraintType;
  /** Table the constraint belongs to */
  tableName: string;
  /** Columns involved in the constraint */
  columns: string[];
  /** Whether the constraint is enabled */
  enabled: boolean;
  /** Deferrable state */
  deferrable: DeferrableState;
}

// =============================================================================
// PRIMARY KEY CONSTRAINT
// =============================================================================

/**
 * Primary key constraint
 * Can be single column or composite
 */
export interface PrimaryKeyConstraint extends Constraint {
  type: 'PRIMARY_KEY';
  /** Whether this is an auto-increment primary key */
  autoIncrement: boolean;
}

// =============================================================================
// FOREIGN KEY CONSTRAINT
// =============================================================================

/**
 * Foreign key constraint definition
 */
export interface ForeignKeyConstraint extends Constraint {
  type: 'FOREIGN_KEY';
  /** Referenced table name */
  referencedTable: string;
  /** Referenced columns in the foreign table */
  referencedColumns: string[];
  /** Action on DELETE of referenced row */
  onDelete: ReferentialAction;
  /** Action on UPDATE of referenced row */
  onUpdate: ReferentialAction;
  /** Match type (FULL, PARTIAL, SIMPLE) */
  matchType: 'FULL' | 'PARTIAL' | 'SIMPLE';
}

// =============================================================================
// UNIQUE CONSTRAINT
// =============================================================================

/**
 * Unique constraint
 * Can be single column or composite
 */
export interface UniqueConstraint extends Constraint {
  type: 'UNIQUE';
  /** Whether NULLs are considered distinct (SQL standard: yes) */
  nullsDistinct: boolean;
}

// =============================================================================
// NOT NULL CONSTRAINT
// =============================================================================

/**
 * Not null constraint
 */
export interface NotNullConstraint extends Constraint {
  type: 'NOT_NULL';
}

// =============================================================================
// CHECK CONSTRAINT
// =============================================================================

/**
 * Check constraint with SQL expression
 */
export interface CheckConstraint extends Constraint {
  type: 'CHECK';
  /** SQL expression that must evaluate to true */
  expression: string;
  /** Parsed expression for validation */
  parsedExpression?: CheckExpression;
}

/**
 * Parsed check expression
 */
export interface CheckExpression {
  /** Left operand (column name or value) */
  left: string | number | null;
  /** Operator */
  operator: CheckOperator;
  /** Right operand (column name or value) */
  right: string | number | null;
  /** For compound expressions */
  and?: CheckExpression;
  or?: CheckExpression;
}

/**
 * Check constraint operators
 */
export type CheckOperator =
  | '='
  | '!='
  | '<>'
  | '<'
  | '<='
  | '>'
  | '>='
  | 'LIKE'
  | 'NOT LIKE'
  | 'IN'
  | 'NOT IN'
  | 'BETWEEN'
  | 'IS NULL'
  | 'IS NOT NULL'
  | 'REGEXP';

// =============================================================================
// DEFAULT CONSTRAINT
// =============================================================================

/**
 * Default value constraint
 */
export interface DefaultConstraint extends Constraint {
  type: 'DEFAULT';
  /** Default value or expression */
  value: SqlDefaultValue;
}

/**
 * SQL default value types
 */
export type SqlDefaultValue =
  | { type: 'literal'; value: string | number | boolean | null }
  | { type: 'expression'; expression: string }
  | { type: 'function'; name: DefaultFunction; args?: unknown[] };

/**
 * Built-in default functions
 */
export type DefaultFunction =
  | 'CURRENT_TIMESTAMP'
  | 'CURRENT_DATE'
  | 'CURRENT_TIME'
  | 'NOW'
  | 'UUID'
  | 'RANDOM';

// =============================================================================
// CONSTRAINT UNION TYPE
// =============================================================================

/**
 * Any constraint type
 */
export type AnyConstraint =
  | PrimaryKeyConstraint
  | ForeignKeyConstraint
  | UniqueConstraint
  | NotNullConstraint
  | CheckConstraint
  | DefaultConstraint;

// =============================================================================
// TABLE CONSTRAINTS
// =============================================================================

/**
 * Table constraints collection
 */
export interface TableConstraints {
  /** Primary key (at most one per table) */
  primaryKey?: PrimaryKeyConstraint;
  /** Foreign keys */
  foreignKeys: ForeignKeyConstraint[];
  /** Unique constraints */
  uniqueConstraints: UniqueConstraint[];
  /** Not null constraints (by column) */
  notNullConstraints: Map<string, NotNullConstraint>;
  /** Check constraints */
  checkConstraints: CheckConstraint[];
  /** Default constraints (by column) */
  defaultConstraints: Map<string, DefaultConstraint>;
}

// =============================================================================
// CONSTRAINT VALIDATION
// =============================================================================

/**
 * Constraint violation error
 */
export interface ConstraintViolation {
  /** Constraint that was violated */
  constraint: AnyConstraint;
  /** Error message */
  message: string;
  /** Error code */
  code: ConstraintViolationCode;
  /** Affected table */
  table: string;
  /** Affected columns */
  columns: string[];
  /** Value that caused the violation */
  value?: unknown;
}

/**
 * Constraint violation codes
 */
export type ConstraintViolationCode =
  | 'PRIMARY_KEY_VIOLATION'
  | 'FOREIGN_KEY_VIOLATION'
  | 'UNIQUE_VIOLATION'
  | 'NOT_NULL_VIOLATION'
  | 'CHECK_VIOLATION'
  | 'INVALID_DEFAULT';

/**
 * Constraint validation result
 */
export interface ConstraintValidationResult {
  /** Whether validation passed */
  valid: boolean;
  /** List of violations */
  violations: ConstraintViolation[];
}

// =============================================================================
// PRAGMA FOREIGN KEYS
// =============================================================================

/**
 * Foreign keys pragma state
 */
export interface ForeignKeysPragma {
  /** Whether foreign keys are enabled */
  enabled: boolean;
  /** Deferred constraint checking mode */
  deferredMode: ConstraintTiming;
}

// =============================================================================
// DEFERRED CONSTRAINTS
// =============================================================================

/**
 * Deferred constraint state for transaction
 */
export interface DeferredConstraintState {
  /** Pending constraint checks */
  pendingChecks: DeferredCheck[];
  /** Current deferral mode */
  mode: ConstraintTiming;
}

/**
 * A deferred constraint check to be performed at commit
 */
export interface DeferredCheck {
  /** Constraint to check */
  constraint: AnyConstraint;
  /** Table being modified */
  table: string;
  /** Row(s) to validate */
  rows: Record<string, unknown>[];
  /** Operation that triggered the check */
  operation: 'INSERT' | 'UPDATE' | 'DELETE';
}

// =============================================================================
// CONSTRAINT DEFINITION PARSING
// =============================================================================

/**
 * Parsed constraint definition from SQL
 */
export interface ParsedConstraintDefinition {
  /** Constraint type */
  type: ConstraintType;
  /** Optional constraint name */
  name?: string;
  /** Column names */
  columns: string[];
  /** Foreign key specific */
  references?: {
    table: string;
    columns: string[];
    onDelete?: ReferentialAction;
    onUpdate?: ReferentialAction;
  };
  /** Check constraint specific */
  expression?: string;
  /** Default value specific */
  defaultValue?: string;
  /** Deferrable specification */
  deferrable?: DeferrableState;
}

// =============================================================================
// CONSTRAINT REGISTRY
// =============================================================================

/**
 * Registry for all constraints in a database
 */
export interface ConstraintRegistry {
  /** All constraints by name */
  byName: Map<string, AnyConstraint>;
  /** Constraints by table */
  byTable: Map<string, TableConstraints>;
  /** Foreign key constraints indexed by referenced table */
  foreignKeysByReferencedTable: Map<string, ForeignKeyConstraint[]>;
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create an empty table constraints object
 */
export function createTableConstraints(): TableConstraints {
  return {
    primaryKey: undefined,
    foreignKeys: [],
    uniqueConstraints: [],
    notNullConstraints: new Map(),
    checkConstraints: [],
    defaultConstraints: new Map(),
  };
}

/**
 * Create an empty constraint registry
 */
export function createConstraintRegistry(): ConstraintRegistry {
  return {
    byName: new Map(),
    byTable: new Map(),
    foreignKeysByReferencedTable: new Map(),
  };
}

/**
 * Create a default foreign keys pragma state
 */
export function createForeignKeysPragma(enabled = true): ForeignKeysPragma {
  return {
    enabled,
    deferredMode: 'IMMEDIATE',
  };
}

/**
 * Create deferred constraint state for a transaction
 */
export function createDeferredConstraintState(): DeferredConstraintState {
  return {
    pendingChecks: [],
    mode: 'IMMEDIATE',
  };
}

// =============================================================================
// TYPE GUARDS
// =============================================================================

/**
 * Check if constraint is a primary key
 */
export function isPrimaryKeyConstraint(
  constraint: AnyConstraint
): constraint is PrimaryKeyConstraint {
  return constraint.type === 'PRIMARY_KEY';
}

/**
 * Check if constraint is a foreign key
 */
export function isForeignKeyConstraint(
  constraint: AnyConstraint
): constraint is ForeignKeyConstraint {
  return constraint.type === 'FOREIGN_KEY';
}

/**
 * Check if constraint is a unique constraint
 */
export function isUniqueConstraint(
  constraint: AnyConstraint
): constraint is UniqueConstraint {
  return constraint.type === 'UNIQUE';
}

/**
 * Check if constraint is a not null constraint
 */
export function isNotNullConstraint(
  constraint: AnyConstraint
): constraint is NotNullConstraint {
  return constraint.type === 'NOT_NULL';
}

/**
 * Check if constraint is a check constraint
 */
export function isCheckConstraint(
  constraint: AnyConstraint
): constraint is CheckConstraint {
  return constraint.type === 'CHECK';
}

/**
 * Check if constraint is a default constraint
 */
export function isDefaultConstraint(
  constraint: AnyConstraint
): constraint is DefaultConstraint {
  return constraint.type === 'DEFAULT';
}

// =============================================================================
// CONSTRAINT NAMING
// =============================================================================

/**
 * Generate a constraint name if not provided
 */
export function generateConstraintName(
  tableName: string,
  type: ConstraintType,
  columns: string[]
): string {
  const typePrefix = {
    PRIMARY_KEY: 'pk',
    FOREIGN_KEY: 'fk',
    UNIQUE: 'uq',
    NOT_NULL: 'nn',
    CHECK: 'ck',
    DEFAULT: 'df',
  }[type];

  return `${typePrefix}_${tableName}_${columns.join('_')}`;
}

// =============================================================================
// REFERENTIAL ACTION HELPERS
// =============================================================================

/**
 * Parse referential action from string
 */
export function parseReferentialAction(action: string): ReferentialAction {
  const normalized = action.toUpperCase().replace(/\s+/g, '_');
  switch (normalized) {
    case 'CASCADE':
      return 'CASCADE';
    case 'SET_NULL':
      return 'SET_NULL';
    case 'SET_DEFAULT':
      return 'SET_DEFAULT';
    case 'RESTRICT':
      return 'RESTRICT';
    case 'NO_ACTION':
    default:
      return 'NO_ACTION';
  }
}

/**
 * Convert referential action to SQL string
 */
export function referentialActionToSql(action: ReferentialAction): string {
  return action.replace('_', ' ');
}
