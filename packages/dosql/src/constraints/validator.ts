/**
 * DoSQL Constraint Validator
 *
 * Validates data against table constraints including primary keys,
 * foreign keys, unique constraints, not null, and check constraints.
 */

import type {
  AnyConstraint,
  ConstraintValidationResult,
  ConstraintViolation,
  ConstraintViolationCode,
  TableConstraints,
  PrimaryKeyConstraint,
  UniqueConstraint,
  NotNullConstraint,
  CheckConstraint,
  CheckExpression,
  CheckOperator,
  DefaultConstraint,
  DeferredConstraintState,
  DeferredCheck,
  ForeignKeysPragma,
  ConstraintRegistry,
} from './types.js';
import {
  isPrimaryKeyConstraint,
  isUniqueConstraint,
  isNotNullConstraint,
  isCheckConstraint,
  isDefaultConstraint,
  createTableConstraints,
} from './types.js';

// =============================================================================
// VALIDATION ERROR
// =============================================================================

/**
 * Error thrown when a constraint is violated
 */
export class ConstraintError extends Error {
  constructor(
    message: string,
    public readonly violation: ConstraintViolation
  ) {
    super(message);
    this.name = 'ConstraintError';
  }
}

// =============================================================================
// CONSTRAINT VALIDATOR
// =============================================================================

/**
 * Constraint validator class
 */
export class ConstraintValidator {
  /** Constraint registry */
  private registry: ConstraintRegistry;

  /** Foreign keys pragma state */
  private foreignKeysPragma: ForeignKeysPragma;

  /** Deferred constraint state (per transaction) */
  private deferredState: DeferredConstraintState | null = null;

  /** Data accessor for checking references */
  private dataAccessor: DataAccessor;

  constructor(
    registry: ConstraintRegistry,
    dataAccessor: DataAccessor,
    foreignKeysPragma: ForeignKeysPragma = { enabled: true, deferredMode: 'IMMEDIATE' }
  ) {
    this.registry = registry;
    this.dataAccessor = dataAccessor;
    this.foreignKeysPragma = foreignKeysPragma;
  }

  // ===========================================================================
  // PRAGMA MANAGEMENT
  // ===========================================================================

  /**
   * Set foreign keys enabled state
   */
  setForeignKeysEnabled(enabled: boolean): void {
    this.foreignKeysPragma.enabled = enabled;
  }

  /**
   * Get foreign keys enabled state
   */
  getForeignKeysEnabled(): boolean {
    return this.foreignKeysPragma.enabled;
  }

  // ===========================================================================
  // DEFERRED CONSTRAINT MANAGEMENT
  // ===========================================================================

  /**
   * Begin deferred constraint checking (for transaction)
   */
  beginDeferredChecking(): void {
    this.deferredState = {
      pendingChecks: [],
      mode: 'IMMEDIATE',
    };
  }

  /**
   * Set deferred mode for all constraints
   */
  setDeferredMode(immediate: boolean): void {
    if (this.deferredState) {
      this.deferredState.mode = immediate ? 'IMMEDIATE' : 'DEFERRED';
    }
  }

  /**
   * End deferred checking and validate all pending checks
   * Call this at transaction commit
   */
  commitDeferredChecks(): ConstraintValidationResult {
    if (!this.deferredState) {
      return { valid: true, violations: [] };
    }

    const violations: ConstraintViolation[] = [];

    for (const check of this.deferredState.pendingChecks) {
      const result = this.validateDeferredCheck(check);
      violations.push(...result.violations);
    }

    this.deferredState = null;

    return {
      valid: violations.length === 0,
      violations,
    };
  }

  /**
   * Rollback deferred checks (cancel pending)
   */
  rollbackDeferredChecks(): void {
    this.deferredState = null;
  }

  // ===========================================================================
  // INSERT VALIDATION
  // ===========================================================================

  /**
   * Validate a row for INSERT
   */
  validateInsert(
    tableName: string,
    row: Record<string, unknown>
  ): ConstraintValidationResult {
    const constraints = this.registry.byTable.get(tableName);
    if (!constraints) {
      return { valid: true, violations: [] };
    }

    const violations: ConstraintViolation[] = [];

    // Validate NOT NULL constraints
    for (const [column, constraint] of constraints.notNullConstraints) {
      const result = this.validateNotNull(constraint, row, column);
      violations.push(...result.violations);
    }

    // Validate PRIMARY KEY (uniqueness and not null)
    if (constraints.primaryKey) {
      const result = this.validatePrimaryKeyInsert(
        constraints.primaryKey,
        tableName,
        row
      );
      violations.push(...result.violations);
    }

    // Validate UNIQUE constraints
    for (const unique of constraints.uniqueConstraints) {
      const result = this.validateUniqueInsert(unique, tableName, row);
      violations.push(...result.violations);
    }

    // Validate CHECK constraints
    for (const check of constraints.checkConstraints) {
      const result = this.validateCheck(check, row);
      violations.push(...result.violations);
    }

    // Validate FOREIGN KEY constraints (if enabled)
    if (this.foreignKeysPragma.enabled) {
      for (const fk of constraints.foreignKeys) {
        const result = this.validateForeignKeyInsert(fk, row);
        if (this.shouldDeferCheck(fk)) {
          this.addDeferredCheck(fk, tableName, [row], 'INSERT');
        } else {
          violations.push(...result.violations);
        }
      }
    }

    return { valid: violations.length === 0, violations };
  }

  // ===========================================================================
  // UPDATE VALIDATION
  // ===========================================================================

  /**
   * Validate a row for UPDATE
   */
  validateUpdate(
    tableName: string,
    oldRow: Record<string, unknown>,
    newRow: Record<string, unknown>,
    updatedColumns: string[]
  ): ConstraintValidationResult {
    const constraints = this.registry.byTable.get(tableName);
    if (!constraints) {
      return { valid: true, violations: [] };
    }

    const violations: ConstraintViolation[] = [];

    // Validate NOT NULL for updated columns
    for (const column of updatedColumns) {
      const constraint = constraints.notNullConstraints.get(column);
      if (constraint) {
        const result = this.validateNotNull(constraint, newRow, column);
        violations.push(...result.violations);
      }
    }

    // Validate PRIMARY KEY if any PK columns are updated
    if (constraints.primaryKey) {
      const pkColumnsUpdated = constraints.primaryKey.columns.some(
        col => updatedColumns.includes(col)
      );
      if (pkColumnsUpdated) {
        const result = this.validatePrimaryKeyUpdate(
          constraints.primaryKey,
          tableName,
          oldRow,
          newRow
        );
        violations.push(...result.violations);
      }
    }

    // Validate UNIQUE constraints for updated columns
    for (const unique of constraints.uniqueConstraints) {
      const uniqueColumnsUpdated = unique.columns.some(
        col => updatedColumns.includes(col)
      );
      if (uniqueColumnsUpdated) {
        const result = this.validateUniqueUpdate(unique, tableName, oldRow, newRow);
        violations.push(...result.violations);
      }
    }

    // Validate CHECK constraints
    for (const check of constraints.checkConstraints) {
      const checkColumnsUpdated = check.columns.some(
        col => updatedColumns.includes(col)
      );
      if (checkColumnsUpdated) {
        const result = this.validateCheck(check, newRow);
        violations.push(...result.violations);
      }
    }

    // Validate FOREIGN KEY constraints (if enabled)
    if (this.foreignKeysPragma.enabled) {
      for (const fk of constraints.foreignKeys) {
        const fkColumnsUpdated = fk.columns.some(
          col => updatedColumns.includes(col)
        );
        if (fkColumnsUpdated) {
          const result = this.validateForeignKeyInsert(fk, newRow);
          if (this.shouldDeferCheck(fk)) {
            this.addDeferredCheck(fk, tableName, [newRow], 'UPDATE');
          } else {
            violations.push(...result.violations);
          }
        }
      }
    }

    return { valid: violations.length === 0, violations };
  }

  // ===========================================================================
  // DELETE VALIDATION
  // ===========================================================================

  /**
   * Validate a row for DELETE (checks referencing foreign keys)
   */
  validateDelete(
    tableName: string,
    row: Record<string, unknown>
  ): ConstraintValidationResult {
    if (!this.foreignKeysPragma.enabled) {
      return { valid: true, violations: [] };
    }

    const violations: ConstraintViolation[] = [];

    // Find all foreign keys that reference this table
    const referencingFks = this.registry.foreignKeysByReferencedTable.get(tableName) || [];

    for (const fk of referencingFks) {
      // Check if any rows in the referencing table point to this row
      const result = this.validateForeignKeyDelete(fk, row);
      if (this.shouldDeferCheck(fk)) {
        this.addDeferredCheck(fk, tableName, [row], 'DELETE');
      } else {
        violations.push(...result.violations);
      }
    }

    return { valid: violations.length === 0, violations };
  }

  // ===========================================================================
  // SPECIFIC CONSTRAINT VALIDATORS
  // ===========================================================================

  /**
   * Validate NOT NULL constraint
   */
  private validateNotNull(
    constraint: NotNullConstraint,
    row: Record<string, unknown>,
    column: string
  ): ConstraintValidationResult {
    const value = row[column];

    if (value === null || value === undefined) {
      return {
        valid: false,
        violations: [
          {
            constraint,
            message: `NOT NULL constraint failed: ${constraint.tableName}.${column}`,
            code: 'NOT_NULL_VIOLATION',
            table: constraint.tableName,
            columns: [column],
            value,
          },
        ],
      };
    }

    return { valid: true, violations: [] };
  }

  /**
   * Validate PRIMARY KEY on INSERT
   */
  private validatePrimaryKeyInsert(
    constraint: PrimaryKeyConstraint,
    tableName: string,
    row: Record<string, unknown>
  ): ConstraintValidationResult {
    const violations: ConstraintViolation[] = [];

    // Check that PK columns are not null
    for (const column of constraint.columns) {
      const value = row[column];
      if (value === null || value === undefined) {
        violations.push({
          constraint,
          message: `PRIMARY KEY column cannot be NULL: ${tableName}.${column}`,
          code: 'PRIMARY_KEY_VIOLATION',
          table: tableName,
          columns: [column],
          value,
        });
      }
    }

    if (violations.length > 0) {
      return { valid: false, violations };
    }

    // Check uniqueness
    const pkValues = constraint.columns.map(col => row[col]);
    const exists = this.dataAccessor.rowExistsWithValues(
      tableName,
      constraint.columns,
      pkValues
    );

    if (exists) {
      return {
        valid: false,
        violations: [
          {
            constraint,
            message: `PRIMARY KEY constraint failed: duplicate key in ${tableName}`,
            code: 'PRIMARY_KEY_VIOLATION',
            table: tableName,
            columns: constraint.columns,
            value: pkValues,
          },
        ],
      };
    }

    return { valid: true, violations: [] };
  }

  /**
   * Validate PRIMARY KEY on UPDATE
   */
  private validatePrimaryKeyUpdate(
    constraint: PrimaryKeyConstraint,
    tableName: string,
    oldRow: Record<string, unknown>,
    newRow: Record<string, unknown>
  ): ConstraintValidationResult {
    const violations: ConstraintViolation[] = [];

    // Check that new PK values are not null
    for (const column of constraint.columns) {
      const value = newRow[column];
      if (value === null || value === undefined) {
        violations.push({
          constraint,
          message: `PRIMARY KEY column cannot be NULL: ${tableName}.${column}`,
          code: 'PRIMARY_KEY_VIOLATION',
          table: tableName,
          columns: [column],
          value,
        });
      }
    }

    if (violations.length > 0) {
      return { valid: false, violations };
    }

    // Check if PK values changed
    const oldPkValues = constraint.columns.map(col => oldRow[col]);
    const newPkValues = constraint.columns.map(col => newRow[col]);

    const pkChanged = !arraysEqual(oldPkValues, newPkValues);

    if (pkChanged) {
      // Check uniqueness of new PK values
      const exists = this.dataAccessor.rowExistsWithValues(
        tableName,
        constraint.columns,
        newPkValues
      );

      if (exists) {
        return {
          valid: false,
          violations: [
            {
              constraint,
              message: `PRIMARY KEY constraint failed: duplicate key in ${tableName}`,
              code: 'PRIMARY_KEY_VIOLATION',
              table: tableName,
              columns: constraint.columns,
              value: newPkValues,
            },
          ],
        };
      }
    }

    return { valid: true, violations: [] };
  }

  /**
   * Validate UNIQUE constraint on INSERT
   */
  private validateUniqueInsert(
    constraint: UniqueConstraint,
    tableName: string,
    row: Record<string, unknown>
  ): ConstraintValidationResult {
    const values = constraint.columns.map(col => row[col]);

    // If all values are NULL and nullsDistinct is true, it's valid
    if (constraint.nullsDistinct && values.every(v => v === null)) {
      return { valid: true, violations: [] };
    }

    // Check uniqueness
    const exists = this.dataAccessor.rowExistsWithValues(
      tableName,
      constraint.columns,
      values
    );

    if (exists) {
      return {
        valid: false,
        violations: [
          {
            constraint,
            message: `UNIQUE constraint failed: ${tableName}.${constraint.columns.join(', ')}`,
            code: 'UNIQUE_VIOLATION',
            table: tableName,
            columns: constraint.columns,
            value: values,
          },
        ],
      };
    }

    return { valid: true, violations: [] };
  }

  /**
   * Validate UNIQUE constraint on UPDATE
   */
  private validateUniqueUpdate(
    constraint: UniqueConstraint,
    tableName: string,
    oldRow: Record<string, unknown>,
    newRow: Record<string, unknown>
  ): ConstraintValidationResult {
    const oldValues = constraint.columns.map(col => oldRow[col]);
    const newValues = constraint.columns.map(col => newRow[col]);

    // If values haven't changed, no need to check
    if (arraysEqual(oldValues, newValues)) {
      return { valid: true, violations: [] };
    }

    // If all new values are NULL and nullsDistinct is true, it's valid
    if (constraint.nullsDistinct && newValues.every(v => v === null)) {
      return { valid: true, violations: [] };
    }

    // Check uniqueness (excluding current row)
    const exists = this.dataAccessor.rowExistsWithValuesExcluding(
      tableName,
      constraint.columns,
      newValues,
      oldRow
    );

    if (exists) {
      return {
        valid: false,
        violations: [
          {
            constraint,
            message: `UNIQUE constraint failed: ${tableName}.${constraint.columns.join(', ')}`,
            code: 'UNIQUE_VIOLATION',
            table: tableName,
            columns: constraint.columns,
            value: newValues,
          },
        ],
      };
    }

    return { valid: true, violations: [] };
  }

  /**
   * Validate CHECK constraint
   */
  private validateCheck(
    constraint: CheckConstraint,
    row: Record<string, unknown>
  ): ConstraintValidationResult {
    const result = evaluateCheckExpression(constraint.expression, row);

    if (!result) {
      return {
        valid: false,
        violations: [
          {
            constraint,
            message: `CHECK constraint failed: ${constraint.name}`,
            code: 'CHECK_VIOLATION',
            table: constraint.tableName,
            columns: constraint.columns,
            value: row,
          },
        ],
      };
    }

    return { valid: true, violations: [] };
  }

  /**
   * Validate FOREIGN KEY on INSERT/UPDATE
   */
  private validateForeignKeyInsert(
    constraint: import('./types.js').ForeignKeyConstraint,
    row: Record<string, unknown>
  ): ConstraintValidationResult {
    const values = constraint.columns.map(col => row[col]);

    // If all FK values are NULL, skip validation (NULL references are allowed)
    if (values.every(v => v === null || v === undefined)) {
      return { valid: true, violations: [] };
    }

    // Check if referenced row exists
    const exists = this.dataAccessor.rowExistsWithValues(
      constraint.referencedTable,
      constraint.referencedColumns,
      values
    );

    if (!exists) {
      return {
        valid: false,
        violations: [
          {
            constraint,
            message: `FOREIGN KEY constraint failed: ${constraint.tableName}.${constraint.columns.join(', ')} references ${constraint.referencedTable}.${constraint.referencedColumns.join(', ')}`,
            code: 'FOREIGN_KEY_VIOLATION',
            table: constraint.tableName,
            columns: constraint.columns,
            value: values,
          },
        ],
      };
    }

    return { valid: true, violations: [] };
  }

  /**
   * Validate FOREIGN KEY on DELETE of referenced row
   */
  private validateForeignKeyDelete(
    constraint: import('./types.js').ForeignKeyConstraint,
    deletedRow: Record<string, unknown>
  ): ConstraintValidationResult {
    const referencedValues = constraint.referencedColumns.map(
      col => deletedRow[col]
    );

    // Check if any rows reference this row
    const referencingRows = this.dataAccessor.findRowsWithValues(
      constraint.tableName,
      constraint.columns,
      referencedValues
    );

    if (referencingRows.length > 0) {
      // Handle based on ON DELETE action
      switch (constraint.onDelete) {
        case 'CASCADE':
          // Cascade handled elsewhere
          return { valid: true, violations: [] };
        case 'SET_NULL':
          // Set null handled elsewhere
          return { valid: true, violations: [] };
        case 'SET_DEFAULT':
          // Set default handled elsewhere
          return { valid: true, violations: [] };
        case 'RESTRICT':
        case 'NO_ACTION':
        default:
          return {
            valid: false,
            violations: [
              {
                constraint,
                message: `FOREIGN KEY constraint failed: cannot delete row from ${constraint.referencedTable} - referenced by ${constraint.tableName}`,
                code: 'FOREIGN_KEY_VIOLATION',
                table: constraint.referencedTable,
                columns: constraint.referencedColumns,
                value: referencedValues,
              },
            ],
          };
      }
    }

    return { valid: true, violations: [] };
  }

  // ===========================================================================
  // DEFERRED CONSTRAINT HELPERS
  // ===========================================================================

  /**
   * Check if a constraint check should be deferred
   */
  private shouldDeferCheck(constraint: AnyConstraint): boolean {
    if (!this.deferredState) {
      return false;
    }

    if (constraint.deferrable === 'NOT_DEFERRABLE') {
      return false;
    }

    if (constraint.deferrable === 'DEFERRABLE_INITIALLY_DEFERRED') {
      return this.deferredState.mode === 'DEFERRED';
    }

    if (constraint.deferrable === 'DEFERRABLE_INITIALLY_IMMEDIATE') {
      return this.deferredState.mode === 'DEFERRED';
    }

    return false;
  }

  /**
   * Add a deferred check
   */
  private addDeferredCheck(
    constraint: AnyConstraint,
    table: string,
    rows: Record<string, unknown>[],
    operation: 'INSERT' | 'UPDATE' | 'DELETE'
  ): void {
    if (this.deferredState) {
      this.deferredState.pendingChecks.push({
        constraint,
        table,
        rows,
        operation,
      });
    }
  }

  /**
   * Validate a deferred check
   */
  private validateDeferredCheck(check: DeferredCheck): ConstraintValidationResult {
    const violations: ConstraintViolation[] = [];

    for (const row of check.rows) {
      let result: ConstraintValidationResult;

      if (isPrimaryKeyConstraint(check.constraint)) {
        result = this.validatePrimaryKeyInsert(
          check.constraint,
          check.table,
          row
        );
      } else if (isUniqueConstraint(check.constraint)) {
        result = this.validateUniqueInsert(check.constraint, check.table, row);
      } else if (isNotNullConstraint(check.constraint)) {
        const column = check.constraint.columns[0];
        result = this.validateNotNull(check.constraint, row, column);
      } else if (isCheckConstraint(check.constraint)) {
        result = this.validateCheck(check.constraint, row);
      } else {
        result = { valid: true, violations: [] };
      }

      violations.push(...result.violations);
    }

    return { valid: violations.length === 0, violations };
  }
}

// =============================================================================
// DATA ACCESSOR INTERFACE
// =============================================================================

/**
 * Interface for accessing table data
 * Implemented by the database engine
 */
export interface DataAccessor {
  /**
   * Check if a row exists with the given column values
   */
  rowExistsWithValues(
    tableName: string,
    columns: string[],
    values: unknown[]
  ): boolean;

  /**
   * Check if a row exists with the given column values, excluding a specific row
   */
  rowExistsWithValuesExcluding(
    tableName: string,
    columns: string[],
    values: unknown[],
    excludeRow: Record<string, unknown>
  ): boolean;

  /**
   * Find all rows with the given column values
   */
  findRowsWithValues(
    tableName: string,
    columns: string[],
    values: unknown[]
  ): Record<string, unknown>[];

  /**
   * Get column values for a row
   */
  getColumnValues(
    tableName: string,
    row: Record<string, unknown>,
    columns: string[]
  ): unknown[];
}

// =============================================================================
// CHECK EXPRESSION EVALUATOR
// =============================================================================

/**
 * Evaluate a check expression against a row
 */
export function evaluateCheckExpression(
  expression: string,
  row: Record<string, unknown>
): boolean {
  // Parse and evaluate simple check expressions
  // Supports: column op value, column op column, AND, OR

  const normalized = expression.trim();

  // Handle BETWEEN first (before AND to avoid conflicts)
  const betweenMatch = normalized.match(
    /^(\w+)\s+BETWEEN\s+(.+?)\s+AND\s+(.+)$/i
  );
  if (betweenMatch) {
    const [, column, minStr, maxStr] = betweenMatch;
    const value = row[column];
    const min = parseValue(minStr.trim());
    const max = parseValue(maxStr.trim());
    return Number(value) >= Number(min) && Number(value) <= Number(max);
  }

  // Handle compound expressions (AND/OR) - must check BETWEEN is not present
  // Match AND only if not preceded by BETWEEN
  if (!normalized.match(/\bBETWEEN\b/i)) {
    const andMatch = normalized.match(/(.+?)\s+AND\s+(.+)/i);
    if (andMatch) {
      return (
        evaluateCheckExpression(andMatch[1], row) &&
        evaluateCheckExpression(andMatch[2], row)
      );
    }
  }

  const orMatch = normalized.match(/(.+?)\s+OR\s+(.+)/i);
  if (orMatch) {
    return (
      evaluateCheckExpression(orMatch[1], row) ||
      evaluateCheckExpression(orMatch[2], row)
    );
  }

  // Handle NOT
  const notMatch = normalized.match(/^NOT\s+(.+)/i);
  if (notMatch) {
    return !evaluateCheckExpression(notMatch[1], row);
  }

  // Handle IS NULL / IS NOT NULL
  const isNullMatch = normalized.match(/^(\w+)\s+IS\s+NULL$/i);
  if (isNullMatch) {
    return row[isNullMatch[1]] === null || row[isNullMatch[1]] === undefined;
  }

  const isNotNullMatch = normalized.match(/^(\w+)\s+IS\s+NOT\s+NULL$/i);
  if (isNotNullMatch) {
    return row[isNotNullMatch[1]] !== null && row[isNotNullMatch[1]] !== undefined;
  }

  // Handle IN
  const inMatch = normalized.match(/^(\w+)\s+IN\s*\((.+)\)$/i);
  if (inMatch) {
    const [, column, valueList] = inMatch;
    const value = row[column];
    const values = valueList.split(',').map(v => parseValue(v.trim()));
    return values.some(v => v === value);
  }

  // Handle NOT IN
  const notInMatch = normalized.match(/^(\w+)\s+NOT\s+IN\s*\((.+)\)$/i);
  if (notInMatch) {
    const [, column, valueList] = notInMatch;
    const value = row[column];
    const values = valueList.split(',').map(v => parseValue(v.trim()));
    return !values.some(v => v === value);
  }

  // Handle comparison operators
  const compMatch = normalized.match(
    /^(\w+)\s*(=|!=|<>|<=|>=|<|>|LIKE|NOT\s+LIKE)\s*(.+)$/i
  );
  if (compMatch) {
    const [, column, op, rightStr] = compMatch;
    const leftValue = row[column];
    const rightValue = rightStr.startsWith("'")
      ? rightStr.slice(1, -1) // String literal
      : row[rightStr] !== undefined
        ? row[rightStr] // Column reference
        : parseValue(rightStr); // Literal

    return evaluateComparison(leftValue, op.toUpperCase(), rightValue);
  }

  // If we can't parse, assume true (fail open)
  return true;
}

/**
 * Parse a literal value
 */
function parseValue(str: string): unknown {
  if (str.startsWith("'") && str.endsWith("'")) {
    return str.slice(1, -1);
  }
  if (str === 'NULL' || str === 'null') {
    return null;
  }
  if (str === 'TRUE' || str === 'true') {
    return true;
  }
  if (str === 'FALSE' || str === 'false') {
    return false;
  }
  const num = Number(str);
  if (!isNaN(num)) {
    return num;
  }
  return str;
}

/**
 * Evaluate a comparison
 */
function evaluateComparison(
  left: unknown,
  op: string,
  right: unknown
): boolean {
  switch (op) {
    case '=':
      return left === right;
    case '!=':
    case '<>':
      return left !== right;
    case '<':
      return Number(left) < Number(right);
    case '<=':
      return Number(left) <= Number(right);
    case '>':
      return Number(left) > Number(right);
    case '>=':
      return Number(left) >= Number(right);
    case 'LIKE':
      if (typeof left === 'string' && typeof right === 'string') {
        const regex = new RegExp(
          '^' + right.replace(/%/g, '.*').replace(/_/g, '.') + '$',
          'i'
        );
        return regex.test(left);
      }
      return false;
    case 'NOT LIKE':
      if (typeof left === 'string' && typeof right === 'string') {
        const regex = new RegExp(
          '^' + right.replace(/%/g, '.*').replace(/_/g, '.') + '$',
          'i'
        );
        return !regex.test(left);
      }
      return true;
    default:
      return false;
  }
}

/**
 * Compare arrays for equality
 */
function arraysEqual(a: unknown[], b: unknown[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

/**
 * Create a constraint validator
 */
export function createConstraintValidator(
  registry: ConstraintRegistry,
  dataAccessor: DataAccessor,
  foreignKeysPragma?: ForeignKeysPragma
): ConstraintValidator {
  return new ConstraintValidator(registry, dataAccessor, foreignKeysPragma);
}

// =============================================================================
// IN-MEMORY DATA ACCESSOR
// =============================================================================

/**
 * Simple in-memory data accessor for testing
 */
export class InMemoryDataAccessor implements DataAccessor {
  private tables: Map<string, Record<string, unknown>[]>;

  constructor(tables: Map<string, Record<string, unknown>[]> = new Map()) {
    this.tables = tables;
  }

  /**
   * Add table data
   */
  setTableData(tableName: string, rows: Record<string, unknown>[]): void {
    this.tables.set(tableName, rows);
  }

  /**
   * Get table data
   */
  getTableData(tableName: string): Record<string, unknown>[] {
    return this.tables.get(tableName) || [];
  }

  rowExistsWithValues(
    tableName: string,
    columns: string[],
    values: unknown[]
  ): boolean {
    const rows = this.tables.get(tableName) || [];
    return rows.some(row =>
      columns.every((col, i) => row[col] === values[i])
    );
  }

  rowExistsWithValuesExcluding(
    tableName: string,
    columns: string[],
    values: unknown[],
    excludeRow: Record<string, unknown>
  ): boolean {
    const rows = this.tables.get(tableName) || [];
    return rows.some(
      row =>
        row !== excludeRow &&
        columns.every((col, i) => row[col] === values[i])
    );
  }

  findRowsWithValues(
    tableName: string,
    columns: string[],
    values: unknown[]
  ): Record<string, unknown>[] {
    const rows = this.tables.get(tableName) || [];
    return rows.filter(row =>
      columns.every((col, i) => row[col] === values[i])
    );
  }

  getColumnValues(
    tableName: string,
    row: Record<string, unknown>,
    columns: string[]
  ): unknown[] {
    return columns.map(col => row[col]);
  }
}

/**
 * Create an in-memory data accessor
 */
export function createInMemoryDataAccessor(): InMemoryDataAccessor {
  return new InMemoryDataAccessor();
}
