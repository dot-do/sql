/**
 * DoSQL Foreign Key Handler
 *
 * Handles foreign key constraint enforcement including:
 * - Referential integrity validation
 * - ON DELETE/UPDATE actions (CASCADE, SET NULL, SET DEFAULT, RESTRICT, NO ACTION)
 * - Deferred constraint checking
 */

import type {
  ForeignKeyConstraint,
  ReferentialAction,
  ConstraintViolation,
  ConstraintValidationResult,
  DeferrableState,
} from './types.js';
import {
  generateConstraintName,
  parseReferentialAction,
  referentialActionToSql,
} from './types.js';
import type { DataAccessor } from './validator.js';

// =============================================================================
// FOREIGN KEY HANDLER
// =============================================================================

/**
 * Foreign key handler for managing referential integrity
 */
export class ForeignKeyHandler {
  /** Data accessor for table operations */
  private dataAccessor: DataAccessor;

  /** Whether foreign keys are enabled (PRAGMA foreign_keys) */
  private enabled: boolean;

  constructor(dataAccessor: DataAccessor, enabled = true) {
    this.dataAccessor = dataAccessor;
    this.enabled = enabled;
  }

  // ===========================================================================
  // ENABLE/DISABLE
  // ===========================================================================

  /**
   * Enable foreign key enforcement
   */
  enable(): void {
    this.enabled = true;
  }

  /**
   * Disable foreign key enforcement
   */
  disable(): void {
    this.enabled = false;
  }

  /**
   * Check if foreign keys are enabled
   */
  isEnabled(): boolean {
    return this.enabled;
  }

  // ===========================================================================
  // VALIDATION
  // ===========================================================================

  /**
   * Validate a foreign key reference exists
   */
  validateReference(
    constraint: ForeignKeyConstraint,
    row: Record<string, unknown>
  ): ConstraintValidationResult {
    if (!this.enabled) {
      return { valid: true, violations: [] };
    }

    const fkValues = constraint.columns.map(col => row[col]);

    // NULL values are allowed (unless NOT NULL is specified separately)
    if (fkValues.every(v => v === null || v === undefined)) {
      return { valid: true, violations: [] };
    }

    // Handle partial nulls based on match type
    if (fkValues.some(v => v === null || v === undefined)) {
      switch (constraint.matchType) {
        case 'FULL':
          // MATCH FULL: all must be null or none
          return {
            valid: false,
            violations: [
              {
                constraint,
                message: `FOREIGN KEY MATCH FULL violation: partial NULL in ${constraint.tableName}`,
                code: 'FOREIGN_KEY_VIOLATION',
                table: constraint.tableName,
                columns: constraint.columns,
                value: fkValues,
              },
            ],
          };
        case 'PARTIAL':
          // MATCH PARTIAL: allow partial matches (not commonly supported)
          return { valid: true, violations: [] };
        case 'SIMPLE':
        default:
          // MATCH SIMPLE: allow if any value is null
          return { valid: true, violations: [] };
      }
    }

    // Check if referenced row exists
    const exists = this.dataAccessor.rowExistsWithValues(
      constraint.referencedTable,
      constraint.referencedColumns,
      fkValues
    );

    if (!exists) {
      return {
        valid: false,
        violations: [
          {
            constraint,
            message: `FOREIGN KEY constraint failed: ${constraint.columns.join(', ')} references ${constraint.referencedTable}(${constraint.referencedColumns.join(', ')})`,
            code: 'FOREIGN_KEY_VIOLATION',
            table: constraint.tableName,
            columns: constraint.columns,
            value: fkValues,
          },
        ],
      };
    }

    return { valid: true, violations: [] };
  }

  /**
   * Check if a delete is allowed (or needs cascading)
   */
  checkDeleteAllowed(
    constraint: ForeignKeyConstraint,
    deletedRow: Record<string, unknown>
  ): DeleteCheckResult {
    if (!this.enabled) {
      return { allowed: true, action: 'NO_ACTION', affectedRows: [] };
    }

    const referencedValues = constraint.referencedColumns.map(
      col => deletedRow[col]
    );

    // Find rows that reference this row
    const affectedRows = this.dataAccessor.findRowsWithValues(
      constraint.tableName,
      constraint.columns,
      referencedValues
    );

    if (affectedRows.length === 0) {
      return { allowed: true, action: 'NO_ACTION', affectedRows: [] };
    }

    // Determine action based on ON DELETE clause
    switch (constraint.onDelete) {
      case 'CASCADE':
        return { allowed: true, action: 'CASCADE', affectedRows };
      case 'SET_NULL':
        return { allowed: true, action: 'SET_NULL', affectedRows };
      case 'SET_DEFAULT':
        return { allowed: true, action: 'SET_DEFAULT', affectedRows };
      case 'RESTRICT':
        return { allowed: false, action: 'RESTRICT', affectedRows };
      case 'NO_ACTION':
      default:
        return { allowed: false, action: 'NO_ACTION', affectedRows };
    }
  }

  /**
   * Check if an update is allowed (or needs cascading)
   */
  checkUpdateAllowed(
    constraint: ForeignKeyConstraint,
    oldRow: Record<string, unknown>,
    newRow: Record<string, unknown>
  ): UpdateCheckResult {
    if (!this.enabled) {
      return { allowed: true, action: 'NO_ACTION', affectedRows: [] };
    }

    const oldValues = constraint.referencedColumns.map(col => oldRow[col]);
    const newValues = constraint.referencedColumns.map(col => newRow[col]);

    // If referenced columns didn't change, no FK check needed
    if (arraysEqual(oldValues, newValues)) {
      return { allowed: true, action: 'NO_ACTION', affectedRows: [] };
    }

    // Find rows that reference the old values
    const affectedRows = this.dataAccessor.findRowsWithValues(
      constraint.tableName,
      constraint.columns,
      oldValues
    );

    if (affectedRows.length === 0) {
      return { allowed: true, action: 'NO_ACTION', affectedRows: [] };
    }

    // Determine action based on ON UPDATE clause
    switch (constraint.onUpdate) {
      case 'CASCADE':
        return {
          allowed: true,
          action: 'CASCADE',
          affectedRows,
          newValues,
        };
      case 'SET_NULL':
        return { allowed: true, action: 'SET_NULL', affectedRows };
      case 'SET_DEFAULT':
        return { allowed: true, action: 'SET_DEFAULT', affectedRows };
      case 'RESTRICT':
        return { allowed: false, action: 'RESTRICT', affectedRows };
      case 'NO_ACTION':
      default:
        return { allowed: false, action: 'NO_ACTION', affectedRows };
    }
  }

  // ===========================================================================
  // CASCADING OPERATIONS
  // ===========================================================================

  /**
   * Generate cascade delete operations
   */
  generateCascadeDeletes(
    constraint: ForeignKeyConstraint,
    affectedRows: Record<string, unknown>[]
  ): CascadeOperation[] {
    return affectedRows.map(row => ({
      operation: 'DELETE',
      table: constraint.tableName,
      row,
      constraint,
    }));
  }

  /**
   * Generate cascade update operations
   */
  generateCascadeUpdates(
    constraint: ForeignKeyConstraint,
    affectedRows: Record<string, unknown>[],
    newValues: unknown[]
  ): CascadeOperation[] {
    return affectedRows.map(row => {
      const updates: Record<string, unknown> = {};
      constraint.columns.forEach((col, i) => {
        updates[col] = newValues[i];
      });

      return {
        operation: 'UPDATE',
        table: constraint.tableName,
        row,
        updates,
        constraint,
      };
    });
  }

  /**
   * Generate SET NULL operations
   */
  generateSetNullOperations(
    constraint: ForeignKeyConstraint,
    affectedRows: Record<string, unknown>[]
  ): CascadeOperation[] {
    return affectedRows.map(row => {
      const updates: Record<string, unknown> = {};
      constraint.columns.forEach(col => {
        updates[col] = null;
      });

      return {
        operation: 'UPDATE',
        table: constraint.tableName,
        row,
        updates,
        constraint,
      };
    });
  }

  /**
   * Generate SET DEFAULT operations
   */
  generateSetDefaultOperations(
    constraint: ForeignKeyConstraint,
    affectedRows: Record<string, unknown>[],
    defaultValues: Record<string, unknown>
  ): CascadeOperation[] {
    return affectedRows.map(row => {
      const updates: Record<string, unknown> = {};
      constraint.columns.forEach(col => {
        updates[col] = defaultValues[col] ?? null;
      });

      return {
        operation: 'UPDATE',
        table: constraint.tableName,
        row,
        updates,
        constraint,
      };
    });
  }
}

// =============================================================================
// RESULT TYPES
// =============================================================================

/**
 * Result of checking if a delete is allowed
 */
export interface DeleteCheckResult {
  /** Whether the delete is allowed */
  allowed: boolean;
  /** Action to take */
  action: ReferentialAction;
  /** Rows affected by the delete */
  affectedRows: Record<string, unknown>[];
}

/**
 * Result of checking if an update is allowed
 */
export interface UpdateCheckResult {
  /** Whether the update is allowed */
  allowed: boolean;
  /** Action to take */
  action: ReferentialAction;
  /** Rows affected by the update */
  affectedRows: Record<string, unknown>[];
  /** New values for CASCADE updates */
  newValues?: unknown[];
}

/**
 * A cascade operation to be executed
 */
export interface CascadeOperation {
  /** Operation type */
  operation: 'DELETE' | 'UPDATE';
  /** Target table */
  table: string;
  /** Row being operated on */
  row: Record<string, unknown>;
  /** Updates to apply (for UPDATE operations) */
  updates?: Record<string, unknown>;
  /** Constraint that triggered this operation */
  constraint: ForeignKeyConstraint;
}

// =============================================================================
// FOREIGN KEY DEFINITION BUILDER
// =============================================================================

/**
 * Builder for creating foreign key constraints
 */
export class ForeignKeyBuilder {
  private constraint: Partial<ForeignKeyConstraint>;

  constructor(tableName: string, columns: string[]) {
    this.constraint = {
      type: 'FOREIGN_KEY',
      tableName,
      columns,
      enabled: true,
      deferrable: 'NOT_DEFERRABLE',
      onDelete: 'NO_ACTION',
      onUpdate: 'NO_ACTION',
      matchType: 'SIMPLE',
    };
  }

  /**
   * Set the referenced table and columns
   */
  references(table: string, columns: string[]): this {
    this.constraint.referencedTable = table;
    this.constraint.referencedColumns = columns;
    return this;
  }

  /**
   * Set the ON DELETE action
   */
  onDelete(action: ReferentialAction): this {
    this.constraint.onDelete = action;
    return this;
  }

  /**
   * Set the ON UPDATE action
   */
  onUpdate(action: ReferentialAction): this {
    this.constraint.onUpdate = action;
    return this;
  }

  /**
   * Set the constraint name
   */
  named(name: string): this {
    this.constraint.name = name;
    return this;
  }

  /**
   * Make the constraint deferrable
   */
  deferrable(initiallyDeferred = false): this {
    this.constraint.deferrable = initiallyDeferred
      ? 'DEFERRABLE_INITIALLY_DEFERRED'
      : 'DEFERRABLE_INITIALLY_IMMEDIATE';
    return this;
  }

  /**
   * Set the match type
   */
  match(type: 'FULL' | 'PARTIAL' | 'SIMPLE'): this {
    this.constraint.matchType = type;
    return this;
  }

  /**
   * Build the constraint
   */
  build(): ForeignKeyConstraint {
    if (!this.constraint.referencedTable || !this.constraint.referencedColumns) {
      throw new Error('Foreign key must have REFERENCES clause');
    }

    if (!this.constraint.name) {
      this.constraint.name = generateConstraintName(
        this.constraint.tableName!,
        'FOREIGN_KEY',
        this.constraint.columns!
      );
    }

    return this.constraint as ForeignKeyConstraint;
  }
}

// =============================================================================
// FOREIGN KEY PARSER
// =============================================================================

/**
 * Parse a foreign key definition from SQL
 */
export function parseForeignKeyDefinition(
  tableName: string,
  definition: string
): ForeignKeyConstraint | null {
  // Pattern: FOREIGN KEY (columns) REFERENCES table(columns) [ON DELETE action] [ON UPDATE action]
  const fkPattern =
    /FOREIGN\s+KEY\s*\(([^)]+)\)\s+REFERENCES\s+(\w+)\s*\(([^)]+)\)(?:\s+ON\s+DELETE\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT|RESTRICT|NO\s+ACTION))?(?:\s+ON\s+UPDATE\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT|RESTRICT|NO\s+ACTION))?(?:\s+(NOT\s+DEFERRABLE|DEFERRABLE(?:\s+INITIALLY\s+(DEFERRED|IMMEDIATE))?))?/i;

  const match = definition.match(fkPattern);
  if (!match) {
    return null;
  }

  const [
    ,
    columnsStr,
    refTable,
    refColumnsStr,
    onDelete,
    onUpdate,
    deferrableStr,
    initiallyDeferred,
  ] = match;

  const columns = columnsStr.split(',').map(c => c.trim());
  const refColumns = refColumnsStr.split(',').map(c => c.trim());

  let deferrable: DeferrableState = 'NOT_DEFERRABLE';
  if (deferrableStr) {
    if (deferrableStr.toUpperCase().includes('NOT DEFERRABLE')) {
      deferrable = 'NOT_DEFERRABLE';
    } else if (initiallyDeferred?.toUpperCase() === 'DEFERRED') {
      deferrable = 'DEFERRABLE_INITIALLY_DEFERRED';
    } else {
      deferrable = 'DEFERRABLE_INITIALLY_IMMEDIATE';
    }
  }

  // Extract constraint name if present
  const nameMatch = definition.match(/CONSTRAINT\s+(\w+)\s+FOREIGN/i);
  const name = nameMatch
    ? nameMatch[1]
    : generateConstraintName(tableName, 'FOREIGN_KEY', columns);

  return {
    type: 'FOREIGN_KEY',
    name,
    tableName,
    columns,
    enabled: true,
    deferrable,
    referencedTable: refTable,
    referencedColumns: refColumns,
    onDelete: onDelete ? parseReferentialAction(onDelete) : 'NO_ACTION',
    onUpdate: onUpdate ? parseReferentialAction(onUpdate) : 'NO_ACTION',
    matchType: 'SIMPLE',
  };
}

/**
 * Parse column-level foreign key (e.g., column_name INTEGER REFERENCES table(column))
 */
export function parseColumnForeignKey(
  tableName: string,
  columnName: string,
  definition: string
): ForeignKeyConstraint | null {
  // Pattern: REFERENCES table(column) [ON DELETE action] [ON UPDATE action]
  const refPattern =
    /REFERENCES\s+(\w+)\s*(?:\(([^)]+)\))?(?:\s+ON\s+DELETE\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT|RESTRICT|NO\s+ACTION))?(?:\s+ON\s+UPDATE\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT|RESTRICT|NO\s+ACTION))?/i;

  const match = definition.match(refPattern);
  if (!match) {
    return null;
  }

  const [, refTable, refColumnStr, onDelete, onUpdate] = match;
  const refColumn = refColumnStr ? refColumnStr.trim() : columnName;

  return {
    type: 'FOREIGN_KEY',
    name: generateConstraintName(tableName, 'FOREIGN_KEY', [columnName]),
    tableName,
    columns: [columnName],
    enabled: true,
    deferrable: 'NOT_DEFERRABLE',
    referencedTable: refTable,
    referencedColumns: [refColumn],
    onDelete: onDelete ? parseReferentialAction(onDelete) : 'NO_ACTION',
    onUpdate: onUpdate ? parseReferentialAction(onUpdate) : 'NO_ACTION',
    matchType: 'SIMPLE',
  };
}

// =============================================================================
// FOREIGN KEY TO SQL
// =============================================================================

/**
 * Convert a foreign key constraint to SQL
 */
export function foreignKeyToSql(constraint: ForeignKeyConstraint): string {
  const parts: string[] = [];

  parts.push(`CONSTRAINT ${constraint.name}`);
  parts.push(`FOREIGN KEY (${constraint.columns.join(', ')})`);
  parts.push(
    `REFERENCES ${constraint.referencedTable}(${constraint.referencedColumns.join(', ')})`
  );

  if (constraint.onDelete !== 'NO_ACTION') {
    parts.push(`ON DELETE ${referentialActionToSql(constraint.onDelete)}`);
  }

  if (constraint.onUpdate !== 'NO_ACTION') {
    parts.push(`ON UPDATE ${referentialActionToSql(constraint.onUpdate)}`);
  }

  if (constraint.deferrable === 'DEFERRABLE_INITIALLY_DEFERRED') {
    parts.push('DEFERRABLE INITIALLY DEFERRED');
  } else if (constraint.deferrable === 'DEFERRABLE_INITIALLY_IMMEDIATE') {
    parts.push('DEFERRABLE INITIALLY IMMEDIATE');
  }

  return parts.join(' ');
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

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
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create a foreign key handler
 */
export function createForeignKeyHandler(
  dataAccessor: DataAccessor,
  enabled = true
): ForeignKeyHandler {
  return new ForeignKeyHandler(dataAccessor, enabled);
}

/**
 * Create a foreign key builder
 */
export function foreignKey(
  tableName: string,
  columns: string | string[]
): ForeignKeyBuilder {
  const cols = Array.isArray(columns) ? columns : [columns];
  return new ForeignKeyBuilder(tableName, cols);
}

// =============================================================================
// FOREIGN KEY INTEGRITY CHECK
// =============================================================================

/**
 * Check foreign key integrity for a table
 * Returns violations for any orphaned references
 */
export function checkForeignKeyIntegrity(
  constraint: ForeignKeyConstraint,
  dataAccessor: DataAccessor
): ConstraintValidationResult {
  // Get all rows from the table with the foreign key
  const rows = dataAccessor.findRowsWithValues(constraint.tableName, [], []);

  const violations: ConstraintViolation[] = [];

  for (const row of rows) {
    const fkValues = constraint.columns.map(col => row[col]);

    // Skip if all values are NULL
    if (fkValues.every(v => v === null || v === undefined)) {
      continue;
    }

    // Check if referenced row exists
    const exists = dataAccessor.rowExistsWithValues(
      constraint.referencedTable,
      constraint.referencedColumns,
      fkValues
    );

    if (!exists) {
      violations.push({
        constraint,
        message: `Orphaned foreign key: ${constraint.tableName}.${constraint.columns.join(', ')} = ${fkValues.join(', ')} references non-existent row in ${constraint.referencedTable}`,
        code: 'FOREIGN_KEY_VIOLATION',
        table: constraint.tableName,
        columns: constraint.columns,
        value: fkValues,
      });
    }
  }

  return {
    valid: violations.length === 0,
    violations,
  };
}

/**
 * PRAGMA foreign_key_check equivalent
 * Returns all foreign key violations in the database
 */
export function pragmaForeignKeyCheck(
  constraints: ForeignKeyConstraint[],
  dataAccessor: DataAccessor
): ForeignKeyCheckResult[] {
  const results: ForeignKeyCheckResult[] = [];

  for (const constraint of constraints) {
    const validation = checkForeignKeyIntegrity(constraint, dataAccessor);
    for (const violation of validation.violations) {
      results.push({
        table: constraint.tableName,
        rowid: 0, // Would need actual rowid tracking
        parent: constraint.referencedTable,
        fkid: 0, // Would need FK index tracking
      });
    }
  }

  return results;
}

/**
 * Result from PRAGMA foreign_key_check
 */
export interface ForeignKeyCheckResult {
  /** Table with the foreign key */
  table: string;
  /** Row ID of the violating row */
  rowid: number;
  /** Parent (referenced) table */
  parent: string;
  /** Foreign key index */
  fkid: number;
}
