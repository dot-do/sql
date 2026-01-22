/**
 * DoSQL DML Executor
 *
 * Executes INSERT, UPDATE, DELETE statements with RETURNING clause support.
 * Returns affected rows when RETURNING is specified.
 *
 * @module executor/dml-executor
 */

import {
  parseDML,
  parseInsert,
  parseUpdate,
  parseDelete,
  parseReplace,
} from '../parser/dml.js';
import type {
  DMLStatement,
  InsertStatement,
  UpdateStatement,
  DeleteStatement,
  ReplaceStatement,
  ReturningClause,
  Expression,
  LiteralExpression,
  ColumnReference,
  SetClause,
  WhereClause,
} from '../parser/dml-types.js';
import { isParseSuccess } from '../parser/dml-types.js';
import {
  evaluateReturning,
  evaluateExpression,
  hasReturningClause,
  expandWildcard,
} from '../parser/returning.js';

// =============================================================================
// TYPES
// =============================================================================

/**
 * Result of DML execution
 */
export interface DMLExecutionResult<T = Record<string, unknown>> {
  /** Number of rows affected */
  changes: number;
  /** Last inserted row ID (for INSERT) */
  lastInsertRowid?: number | bigint;
  /** Rows returned by RETURNING clause */
  rows: T[];
  /** Column names in result */
  columns: string[];
  /** Statement type that was executed */
  statementType: 'insert' | 'update' | 'delete' | 'replace';
}

/**
 * Storage interface for DML operations
 */
export interface DMLStorage {
  /**
   * Get all rows from a table
   */
  getAll(table: string): Promise<Record<string, unknown>[]>;

  /**
   * Get rows matching a predicate
   */
  query(
    table: string,
    predicate: (row: Record<string, unknown>) => boolean
  ): Promise<Record<string, unknown>[]>;

  /**
   * Insert a row and return the inserted row (with generated fields)
   */
  insert(table: string, row: Record<string, unknown>): Promise<Record<string, unknown>>;

  /**
   * Insert multiple rows
   */
  insertMany(table: string, rows: Record<string, unknown>[]): Promise<Record<string, unknown>[]>;

  /**
   * Update rows matching predicate
   */
  update(
    table: string,
    updates: Record<string, unknown>,
    predicate: (row: Record<string, unknown>) => boolean
  ): Promise<Record<string, unknown>[]>;

  /**
   * Delete rows matching predicate
   */
  delete(
    table: string,
    predicate: (row: Record<string, unknown>) => boolean
  ): Promise<Record<string, unknown>[]>;

  /**
   * Get schema columns for a table
   */
  getColumns(table: string): Promise<string[]>;

  /**
   * Generate next ID for a table (for auto-increment)
   */
  nextId(table: string): Promise<number | bigint>;
}

/**
 * Options for DML execution
 */
export interface DMLExecutionOptions {
  /** Parameter values for prepared statement placeholders */
  params?: unknown[];
  /** Custom function evaluator */
  functionEvaluator?: (name: string, args: unknown[]) => unknown;
}

// =============================================================================
// DML EXECUTOR CLASS
// =============================================================================

/**
 * DML Executor handles INSERT, UPDATE, DELETE with RETURNING support
 */
export class DMLExecutor {
  constructor(private readonly storage: DMLStorage) {}

  /**
   * Execute a DML statement string
   */
  async execute<T = Record<string, unknown>>(
    sql: string,
    options?: DMLExecutionOptions
  ): Promise<DMLExecutionResult<T>> {
    const parseResult = parseDML(sql);

    if (!isParseSuccess(parseResult)) {
      throw new Error(`Parse error: ${parseResult.error}`);
    }

    return this.executeStatement<T>(parseResult.statement, options);
  }

  /**
   * Execute a parsed DML statement
   */
  async executeStatement<T = Record<string, unknown>>(
    statement: DMLStatement,
    options?: DMLExecutionOptions
  ): Promise<DMLExecutionResult<T>> {
    switch (statement.type) {
      case 'insert':
        return this.executeInsert<T>(statement, options);
      case 'update':
        return this.executeUpdate<T>(statement, options);
      case 'delete':
        return this.executeDelete<T>(statement, options);
      case 'replace':
        return this.executeReplace<T>(statement, options);
      default:
        throw new Error(`Unknown statement type: ${(statement as any).type}`);
    }
  }

  // ===========================================================================
  // INSERT EXECUTION
  // ===========================================================================

  private async executeInsert<T>(
    statement: InsertStatement,
    options?: DMLExecutionOptions
  ): Promise<DMLExecutionResult<T>> {
    const { table, columns, source, returning } = statement;
    const params = options?.params ?? [];
    let paramIndex = 0;

    const insertedRows: Record<string, unknown>[] = [];

    if (source.type === 'values_list') {
      for (const valuesRow of source.rows) {
        const row: Record<string, unknown> = {};

        for (let i = 0; i < valuesRow.values.length; i++) {
          const expr = valuesRow.values[i];
          const colName = columns?.[i] ?? `col${i}`;

          const { value, newIndex } = this.evaluateInsertValue(
            expr,
            params,
            paramIndex,
            options
          );
          paramIndex = newIndex;
          row[colName] = value;
        }

        const inserted = await this.storage.insert(table, row);
        insertedRows.push(inserted);
      }
    } else if (source.type === 'insert_default') {
      // Insert with all default values
      const inserted = await this.storage.insert(table, {});
      insertedRows.push(inserted);
    } else if (source.type === 'insert_select') {
      // INSERT ... SELECT - would need SELECT executor integration
      throw new Error('INSERT ... SELECT not yet supported');
    }

    // Handle RETURNING clause
    const schemaColumns = await this.storage.getColumns(table);
    const resultRows = returning
      ? this.applyReturning(returning, insertedRows, schemaColumns, options)
      : [];

    const lastId = insertedRows.length > 0
      ? (insertedRows[insertedRows.length - 1].id ?? insertedRows[insertedRows.length - 1].rowid)
      : undefined;

    return {
      changes: insertedRows.length,
      lastInsertRowid: lastId as number | bigint | undefined,
      rows: resultRows as T[],
      columns: returning ? this.getResultColumns(returning, schemaColumns) : [],
      statementType: 'insert',
    };
  }

  // ===========================================================================
  // UPDATE EXECUTION
  // ===========================================================================

  private async executeUpdate<T>(
    statement: UpdateStatement,
    options?: DMLExecutionOptions
  ): Promise<DMLExecutionResult<T>> {
    const { table, set, where, returning, limit, orderBy } = statement;
    const params = options?.params ?? [];

    // Build update function
    const updateFn = (row: Record<string, unknown>): Record<string, unknown> => {
      const updated = { ...row };
      let paramIndex = 0;

      for (const setClause of set) {
        const { value, newIndex } = this.evaluateUpdateValue(
          setClause.value,
          row,
          params,
          paramIndex,
          options
        );
        paramIndex = newIndex;
        updated[setClause.column] = value;
      }

      return updated;
    };

    // Build predicate
    const predicate = where
      ? this.buildWherePredicate(where, params, set.length)
      : () => true;

    // Get affected rows
    let affectedRows = await this.storage.query(table, predicate);

    // Apply ORDER BY if present
    if (orderBy) {
      affectedRows = this.applyOrderBy(affectedRows, orderBy);
    }

    // Apply LIMIT if present
    if (limit) {
      const limitCount = this.evaluateLimitValue(limit.count, params);
      affectedRows = affectedRows.slice(0, limitCount);
    }

    // Execute updates
    const updatedRows: Record<string, unknown>[] = [];
    for (const row of affectedRows) {
      const updated = updateFn(row);
      // In real implementation, we'd update in storage
      // For now, simulate by merging
      Object.assign(row, updated);
      updatedRows.push(row);
    }

    // Handle RETURNING clause
    const schemaColumns = await this.storage.getColumns(table);
    const resultRows = returning
      ? this.applyReturning(returning, updatedRows, schemaColumns, options)
      : [];

    return {
      changes: updatedRows.length,
      rows: resultRows as T[],
      columns: returning ? this.getResultColumns(returning, schemaColumns) : [],
      statementType: 'update',
    };
  }

  // ===========================================================================
  // DELETE EXECUTION
  // ===========================================================================

  private async executeDelete<T>(
    statement: DeleteStatement,
    options?: DMLExecutionOptions
  ): Promise<DMLExecutionResult<T>> {
    const { table, where, returning, limit, orderBy } = statement;
    const params = options?.params ?? [];

    // Build predicate
    const predicate = where
      ? this.buildWherePredicate(where, params, 0)
      : () => true;

    // Get affected rows (before deletion for RETURNING)
    let affectedRows = await this.storage.query(table, predicate);

    // Apply ORDER BY if present
    if (orderBy) {
      affectedRows = this.applyOrderBy(affectedRows, orderBy);
    }

    // Apply LIMIT if present
    if (limit) {
      const limitCount = this.evaluateLimitValue(limit.count, params);
      affectedRows = affectedRows.slice(0, limitCount);
    }

    // Capture rows for RETURNING before deletion
    const rowsForReturning = [...affectedRows];

    // Delete rows
    const deletedRows = await this.storage.delete(table, row =>
      affectedRows.some(affected => affected === row ||
        (affected.id !== undefined && affected.id === row.id))
    );

    // Handle RETURNING clause
    const schemaColumns = await this.storage.getColumns(table);
    const resultRows = returning
      ? this.applyReturning(returning, rowsForReturning, schemaColumns, options)
      : [];

    return {
      changes: deletedRows.length,
      rows: resultRows as T[],
      columns: returning ? this.getResultColumns(returning, schemaColumns) : [],
      statementType: 'delete',
    };
  }

  // ===========================================================================
  // REPLACE EXECUTION
  // ===========================================================================

  private async executeReplace<T>(
    statement: ReplaceStatement,
    options?: DMLExecutionOptions
  ): Promise<DMLExecutionResult<T>> {
    // REPLACE is essentially DELETE + INSERT
    // For simplicity, treat it as INSERT with conflict replacement
    const insertStatement: InsertStatement = {
      type: 'insert',
      table: statement.table,
      alias: statement.alias,
      columns: statement.columns,
      source: statement.source,
      conflict: { type: 'conflict', action: 'REPLACE' },
      returning: statement.returning,
    };

    const result = await this.executeInsert<T>(insertStatement, options);
    return {
      ...result,
      statementType: 'replace',
    };
  }

  // ===========================================================================
  // HELPER METHODS
  // ===========================================================================

  /**
   * Evaluate an expression for INSERT value
   */
  private evaluateInsertValue(
    expr: Expression,
    params: unknown[],
    paramIndex: number,
    options?: DMLExecutionOptions
  ): { value: unknown; newIndex: number } {
    if (expr.type === 'parameter') {
      return {
        value: params[paramIndex],
        newIndex: paramIndex + 1,
      };
    }

    if (expr.type === 'literal') {
      return {
        value: expr.value,
        newIndex: paramIndex,
      };
    }

    if (expr.type === 'null') {
      return {
        value: null,
        newIndex: paramIndex,
      };
    }

    if (expr.type === 'default') {
      return {
        value: undefined, // Let storage handle default
        newIndex: paramIndex,
      };
    }

    if (expr.type === 'function') {
      // Evaluate function with available context
      const args = expr.args.map(arg => {
        const result = this.evaluateInsertValue(arg, params, paramIndex, options);
        paramIndex = result.newIndex;
        return result.value;
      });

      if (options?.functionEvaluator) {
        try {
          return {
            value: options.functionEvaluator(expr.name, args),
            newIndex: paramIndex,
          };
        } catch {
          // Fall through
        }
      }

      return {
        value: null,
        newIndex: paramIndex,
      };
    }

    return {
      value: null,
      newIndex: paramIndex,
    };
  }

  /**
   * Evaluate an expression for UPDATE value
   */
  private evaluateUpdateValue(
    expr: Expression,
    currentRow: Record<string, unknown>,
    params: unknown[],
    paramIndex: number,
    options?: DMLExecutionOptions
  ): { value: unknown; newIndex: number } {
    if (expr.type === 'parameter') {
      return {
        value: params[paramIndex],
        newIndex: paramIndex + 1,
      };
    }

    if (expr.type === 'literal') {
      return {
        value: expr.value,
        newIndex: paramIndex,
      };
    }

    if (expr.type === 'null') {
      return {
        value: null,
        newIndex: paramIndex,
      };
    }

    if (expr.type === 'column') {
      return {
        value: currentRow[expr.name],
        newIndex: paramIndex,
      };
    }

    if (expr.type === 'binary') {
      const { value: leftVal, newIndex: leftIndex } = this.evaluateUpdateValue(
        expr.left,
        currentRow,
        params,
        paramIndex,
        options
      );
      const { value: rightVal, newIndex: rightIndex } = this.evaluateUpdateValue(
        expr.right,
        currentRow,
        params,
        leftIndex,
        options
      );

      return {
        value: this.evaluateBinaryOp(expr.operator, leftVal, rightVal),
        newIndex: rightIndex,
      };
    }

    if (expr.type === 'function') {
      const args: unknown[] = [];
      let currentIndex = paramIndex;

      for (const arg of expr.args) {
        const result = this.evaluateUpdateValue(arg, currentRow, params, currentIndex, options);
        args.push(result.value);
        currentIndex = result.newIndex;
      }

      if (options?.functionEvaluator) {
        try {
          return {
            value: options.functionEvaluator(expr.name, args),
            newIndex: currentIndex,
          };
        } catch {
          // Fall through
        }
      }

      return {
        value: null,
        newIndex: currentIndex,
      };
    }

    return {
      value: null,
      newIndex: paramIndex,
    };
  }

  /**
   * Evaluate a binary operation
   */
  private evaluateBinaryOp(op: string, left: unknown, right: unknown): unknown {
    if (left === null || right === null) {
      if (op === '||') return left === null ? right : (right === null ? left : null);
      return null;
    }

    const leftNum = Number(left);
    const rightNum = Number(right);

    switch (op) {
      case '+':
        return leftNum + rightNum;
      case '-':
        return leftNum - rightNum;
      case '*':
        return leftNum * rightNum;
      case '/':
        return rightNum !== 0 ? leftNum / rightNum : null;
      case '%':
        return rightNum !== 0 ? leftNum % rightNum : null;
      case '||':
        return String(left) + String(right);
      default:
        return null;
    }
  }

  /**
   * Build WHERE predicate function
   */
  private buildWherePredicate(
    where: WhereClause,
    params: unknown[],
    paramOffset: number
  ): (row: Record<string, unknown>) => boolean {
    return (row: Record<string, unknown>) => {
      return this.evaluateCondition(where.condition, row, params, paramOffset);
    };
  }

  /**
   * Evaluate a WHERE condition
   */
  private evaluateCondition(
    expr: Expression,
    row: Record<string, unknown>,
    params: unknown[],
    paramIndex: number
  ): boolean {
    if (expr.type === 'binary') {
      const left = this.resolveExpressionValue(expr.left, row, params, paramIndex);
      const right = this.resolveExpressionValue(expr.right, row, params, paramIndex);

      switch (expr.operator) {
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
        case 'IS':
          return left === right;
        case 'IS NOT':
          return left !== right;
        case 'LIKE':
          if (typeof left === 'string' && typeof right === 'string') {
            const pattern = right.replace(/%/g, '.*').replace(/_/g, '.');
            return new RegExp(`^${pattern}$`, 'i').test(left);
          }
          return false;
        case 'AND':
          return this.evaluateCondition(expr.left, row, params, paramIndex) &&
                 this.evaluateCondition(expr.right, row, params, paramIndex);
        case 'OR':
          return this.evaluateCondition(expr.left, row, params, paramIndex) ||
                 this.evaluateCondition(expr.right, row, params, paramIndex);
        default:
          return false;
      }
    }

    if (expr.type === 'unary' && expr.operator === 'NOT') {
      return !this.evaluateCondition(expr.operand, row, params, paramIndex);
    }

    if (expr.type === 'column') {
      return Boolean(row[expr.name]);
    }

    if (expr.type === 'literal') {
      return Boolean(expr.value);
    }

    return false;
  }

  /**
   * Resolve expression to a value
   */
  private resolveExpressionValue(
    expr: Expression,
    row: Record<string, unknown>,
    params: unknown[],
    paramIndex: number
  ): unknown {
    if (expr.type === 'literal') {
      return expr.value;
    }

    if (expr.type === 'null') {
      return null;
    }

    if (expr.type === 'column') {
      return row[expr.name];
    }

    if (expr.type === 'parameter') {
      const idx = typeof expr.name === 'number' ? expr.name : paramIndex;
      return params[idx];
    }

    return null;
  }

  /**
   * Evaluate LIMIT value
   */
  private evaluateLimitValue(expr: Expression, params: unknown[]): number {
    if (expr.type === 'literal' && typeof expr.value === 'number') {
      return expr.value;
    }
    if (expr.type === 'parameter') {
      const idx = typeof expr.name === 'number' ? expr.name : 0;
      return Number(params[idx]) || 0;
    }
    return 0;
  }

  /**
   * Apply ORDER BY to rows
   */
  private applyOrderBy(
    rows: Record<string, unknown>[],
    orderBy: { items: Array<{ expression: Expression; direction: 'ASC' | 'DESC' }> }
  ): Record<string, unknown>[] {
    return [...rows].sort((a, b) => {
      for (const item of orderBy.items) {
        const aVal = this.getOrderValue(a, item.expression);
        const bVal = this.getOrderValue(b, item.expression);

        if (aVal === bVal) continue;
        if (aVal === null) return item.direction === 'ASC' ? 1 : -1;
        if (bVal === null) return item.direction === 'ASC' ? -1 : 1;

        const cmp = aVal < bVal ? -1 : 1;
        return item.direction === 'DESC' ? -cmp : cmp;
      }
      return 0;
    });
  }

  /**
   * Get ORDER BY value from row
   */
  private getOrderValue(row: Record<string, unknown>, expr: Expression): unknown {
    if (expr.type === 'column') {
      return row[expr.name];
    }
    return null;
  }

  /**
   * Apply RETURNING clause to rows
   */
  private applyReturning(
    returning: ReturningClause,
    rows: Record<string, unknown>[],
    schemaColumns: string[],
    options?: DMLExecutionOptions
  ): Record<string, unknown>[] {
    return rows.map(row =>
      evaluateReturning(returning, row, schemaColumns, {
        functionEvaluator: options?.functionEvaluator,
      })
    );
  }

  /**
   * Get result column names from RETURNING clause
   */
  private getResultColumns(returning: ReturningClause, schemaColumns: string[]): string[] {
    const columns: string[] = [];

    for (const col of returning.columns) {
      if (col.expression === '*') {
        columns.push(...schemaColumns);
      } else if (col.alias) {
        columns.push(col.alias);
      } else if (col.expression.type === 'column') {
        columns.push(col.expression.name);
      } else if (col.expression.type === 'function') {
        columns.push(col.expression.name);
      } else {
        columns.push('?');
      }
    }

    return columns;
  }
}

// =============================================================================
// IN-MEMORY STORAGE IMPLEMENTATION
// =============================================================================

/**
 * Simple in-memory storage for testing
 */
export class InMemoryDMLStorage implements DMLStorage {
  private tables = new Map<string, {
    columns: string[];
    rows: Record<string, unknown>[];
    nextId: number;
  }>();

  /**
   * Create or get a table
   */
  createTable(name: string, columns: string[]): void {
    if (!this.tables.has(name)) {
      this.tables.set(name, {
        columns,
        rows: [],
        nextId: 1,
      });
    }
  }

  async getAll(table: string): Promise<Record<string, unknown>[]> {
    const t = this.tables.get(table);
    return t ? [...t.rows] : [];
  }

  async query(
    table: string,
    predicate: (row: Record<string, unknown>) => boolean
  ): Promise<Record<string, unknown>[]> {
    const t = this.tables.get(table);
    return t ? t.rows.filter(predicate) : [];
  }

  async insert(table: string, row: Record<string, unknown>): Promise<Record<string, unknown>> {
    let t = this.tables.get(table);
    if (!t) {
      t = { columns: Object.keys(row), rows: [], nextId: 1 };
      this.tables.set(table, t);
    }

    const insertedRow = { ...row };
    if (insertedRow.id === undefined) {
      insertedRow.id = t.nextId++;
    }

    t.rows.push(insertedRow);
    return insertedRow;
  }

  async insertMany(table: string, rows: Record<string, unknown>[]): Promise<Record<string, unknown>[]> {
    const results: Record<string, unknown>[] = [];
    for (const row of rows) {
      results.push(await this.insert(table, row));
    }
    return results;
  }

  async update(
    table: string,
    updates: Record<string, unknown>,
    predicate: (row: Record<string, unknown>) => boolean
  ): Promise<Record<string, unknown>[]> {
    const t = this.tables.get(table);
    if (!t) return [];

    const updated: Record<string, unknown>[] = [];
    for (const row of t.rows) {
      if (predicate(row)) {
        Object.assign(row, updates);
        updated.push(row);
      }
    }
    return updated;
  }

  async delete(
    table: string,
    predicate: (row: Record<string, unknown>) => boolean
  ): Promise<Record<string, unknown>[]> {
    const t = this.tables.get(table);
    if (!t) return [];

    const deleted: Record<string, unknown>[] = [];
    const remaining: Record<string, unknown>[] = [];

    for (const row of t.rows) {
      if (predicate(row)) {
        deleted.push(row);
      } else {
        remaining.push(row);
      }
    }

    t.rows = remaining;
    return deleted;
  }

  async getColumns(table: string): Promise<string[]> {
    const t = this.tables.get(table);
    return t?.columns ?? [];
  }

  async nextId(table: string): Promise<number> {
    const t = this.tables.get(table);
    if (!t) return 1;
    return t.nextId++;
  }

  /**
   * Clear all data (for testing)
   */
  clear(): void {
    this.tables.clear();
  }
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create a DML executor with storage
 */
export function createDMLExecutor(storage: DMLStorage): DMLExecutor {
  return new DMLExecutor(storage);
}

/**
 * Create an in-memory DML executor for testing
 */
export function createInMemoryDMLExecutor(): {
  executor: DMLExecutor;
  storage: InMemoryDMLStorage;
} {
  const storage = new InMemoryDMLStorage();
  const executor = new DMLExecutor(storage);
  return { executor, storage };
}
