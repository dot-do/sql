/**
 * DoSQL RETURNING Clause Parser Module
 *
 * Provides utilities for parsing and working with RETURNING clauses in DML statements.
 * The RETURNING clause allows INSERT, UPDATE, DELETE statements to return affected rows.
 *
 * Supported syntax:
 * - RETURNING *
 * - RETURNING col1, col2, col3
 * - RETURNING col AS alias
 * - RETURNING expression AS alias
 * - RETURNING function(col) AS alias
 *
 * @module parser/returning
 */

import type {
  Expression,
  ColumnReference,
  FunctionCall,
  BinaryExpression,
  LiteralExpression,
  ReturningClause,
  ReturningColumn,
  DMLStatement,
  InsertStatement,
  UpdateStatement,
  DeleteStatement,
  ReplaceStatement,
} from './dml-types.js';

// =============================================================================
// TYPE DEFINITIONS
// =============================================================================

/**
 * Result of evaluating a RETURNING clause against a row
 */
export interface ReturningResult {
  /** Column names in the result */
  columns: string[];
  /** Rows returned by the RETURNING clause */
  rows: Record<string, unknown>[];
}

/**
 * Configuration for RETURNING clause evaluation
 */
export interface ReturningEvaluationOptions {
  /** Include metadata columns like rowid */
  includeRowId?: boolean;
  /** Custom function evaluator */
  functionEvaluator?: (name: string, args: unknown[]) => unknown;
}

/**
 * Information about a single returning column
 */
export interface ReturningColumnInfo {
  /** Output name (column name or alias) */
  outputName: string;
  /** Source column name (for column references) */
  sourceColumn?: string;
  /** Source table (for qualified references) */
  sourceTable?: string;
  /** Whether this is a wildcard (*) */
  isWildcard: boolean;
  /** Whether this has an alias */
  hasAlias: boolean;
  /** The original expression */
  expression: Expression | '*';
}

// =============================================================================
// RETURNING CLAUSE UTILITIES
// =============================================================================

/**
 * Check if a DML statement has a RETURNING clause
 */
export function hasReturningClause(statement: DMLStatement): boolean {
  return statement.returning !== undefined && statement.returning.columns.length > 0;
}

/**
 * Get the RETURNING clause from a DML statement, if present
 */
export function getReturningClause(statement: DMLStatement): ReturningClause | undefined {
  return statement.returning;
}

/**
 * Check if a RETURNING clause contains a wildcard (*)
 */
export function hasWildcard(returning: ReturningClause): boolean {
  return returning.columns.some(col => col.expression === '*');
}

/**
 * Get information about each column in a RETURNING clause
 */
export function getReturningColumns(returning: ReturningClause): ReturningColumnInfo[] {
  return returning.columns.map(col => {
    if (col.expression === '*') {
      return {
        outputName: '*',
        isWildcard: true,
        hasAlias: false,
        expression: '*' as const,
      };
    }

    const expr = col.expression;
    let outputName: string;
    let sourceColumn: string | undefined;
    let sourceTable: string | undefined;

    if (expr.type === 'column') {
      sourceColumn = expr.name;
      sourceTable = expr.table;
      outputName = col.alias || expr.name;
    } else if (expr.type === 'function') {
      outputName = col.alias || `${expr.name}(${expr.args.map(formatArg).join(', ')})`;
    } else if (expr.type === 'literal') {
      outputName = col.alias || String(expr.value);
    } else {
      outputName = col.alias || 'expression';
    }

    return {
      outputName,
      sourceColumn,
      sourceTable,
      isWildcard: false,
      hasAlias: col.alias !== undefined,
      expression: expr,
    };
  });
}

/**
 * Format an expression argument for display
 */
function formatArg(expr: Expression): string {
  if (expr.type === 'column') {
    return expr.table ? `${expr.table}.${expr.name}` : expr.name;
  }
  if (expr.type === 'literal') {
    return typeof expr.value === 'string' ? `'${expr.value}'` : String(expr.value);
  }
  if (expr.type === 'function') {
    return `${expr.name}(...)`;
  }
  return '?';
}

/**
 * Get the output column names for a RETURNING clause
 * Note: For wildcard (*), returns ['*'] - caller must expand based on table schema
 */
export function getOutputColumnNames(returning: ReturningClause): string[] {
  return getReturningColumns(returning).map(col => col.outputName);
}

/**
 * Expand wildcard (*) in RETURNING clause given a schema
 */
export function expandWildcard(
  returning: ReturningClause,
  schemaColumns: string[]
): ReturningColumn[] {
  const expandedColumns: ReturningColumn[] = [];

  for (const col of returning.columns) {
    if (col.expression === '*') {
      // Expand wildcard to all schema columns
      for (const schemaCol of schemaColumns) {
        expandedColumns.push({
          expression: { type: 'column', name: schemaCol } as ColumnReference,
        });
      }
    } else {
      expandedColumns.push(col);
    }
  }

  return expandedColumns;
}

/**
 * Create a RETURNING clause that returns all columns (*)
 */
export function createWildcardReturning(): ReturningClause {
  return {
    type: 'returning',
    columns: [{ expression: '*' }],
  };
}

/**
 * Create a RETURNING clause for specific columns
 */
export function createColumnsReturning(columns: string[]): ReturningClause {
  return {
    type: 'returning',
    columns: columns.map(name => ({
      expression: { type: 'column', name } as ColumnReference,
    })),
  };
}

/**
 * Create a RETURNING clause with aliases
 */
export function createAliasedReturning(
  columns: Array<{ column: string; alias?: string }>
): ReturningClause {
  return {
    type: 'returning',
    columns: columns.map(({ column, alias }) => ({
      expression: { type: 'column', name: column } as ColumnReference,
      alias,
    })),
  };
}

// =============================================================================
// EXPRESSION EVALUATION
// =============================================================================

/**
 * Built-in SQL function implementations for RETURNING evaluation
 */
const BUILTIN_FUNCTIONS: Record<string, (...args: unknown[]) => unknown> = {
  // String functions
  UPPER: (s: unknown) => typeof s === 'string' ? s.toUpperCase() : s,
  LOWER: (s: unknown) => typeof s === 'string' ? s.toLowerCase() : s,
  LENGTH: (s: unknown) => typeof s === 'string' ? s.length : null,
  TRIM: (s: unknown) => typeof s === 'string' ? s.trim() : s,
  LTRIM: (s: unknown) => typeof s === 'string' ? s.trimStart() : s,
  RTRIM: (s: unknown) => typeof s === 'string' ? s.trimEnd() : s,
  SUBSTR: (s: unknown, start: unknown, len?: unknown) => {
    if (typeof s !== 'string') return null;
    const startIdx = Number(start) - 1; // SQL is 1-indexed
    if (len !== undefined) {
      return s.substring(startIdx, startIdx + Number(len));
    }
    return s.substring(startIdx);
  },
  REPLACE: (s: unknown, from: unknown, to: unknown) => {
    if (typeof s !== 'string') return null;
    return s.split(String(from)).join(String(to));
  },

  // Numeric functions
  ABS: (n: unknown) => typeof n === 'number' ? Math.abs(n) : null,
  ROUND: (n: unknown, decimals?: unknown) => {
    if (typeof n !== 'number') return null;
    const d = decimals !== undefined ? Number(decimals) : 0;
    const factor = Math.pow(10, d);
    return Math.round(n * factor) / factor;
  },
  FLOOR: (n: unknown) => typeof n === 'number' ? Math.floor(n) : null,
  CEIL: (n: unknown) => typeof n === 'number' ? Math.ceil(n) : null,
  CEILING: (n: unknown) => typeof n === 'number' ? Math.ceil(n) : null,

  // Null handling
  COALESCE: (...args: unknown[]) => args.find(a => a !== null && a !== undefined) ?? null,
  IFNULL: (a: unknown, b: unknown) => a !== null && a !== undefined ? a : b,
  NULLIF: (a: unknown, b: unknown) => a === b ? null : a,

  // Type conversion
  CAST: (value: unknown, _type: unknown) => value, // Simplified - actual implementation would convert
  TYPEOF: (value: unknown) => {
    if (value === null) return 'null';
    if (typeof value === 'number') return Number.isInteger(value) ? 'integer' : 'real';
    if (typeof value === 'string') return 'text';
    if (typeof value === 'object' && value instanceof Uint8Array) return 'blob';
    return 'null';
  },

  // Date/time (simplified)
  DATE: (v: unknown) => v instanceof Date ? v.toISOString().split('T')[0] : String(v),
  TIME: (v: unknown) => v instanceof Date ? v.toISOString().split('T')[1].split('.')[0] : String(v),
  DATETIME: (v: unknown) => v instanceof Date ? v.toISOString().replace('T', ' ').split('.')[0] : String(v),
  STRFTIME: (fmt: unknown, v: unknown) => {
    // Simplified strftime - real implementation would handle format codes
    if (!(v instanceof Date)) return String(v);
    return v.toISOString();
  },

  // Aggregates (for single-row context)
  COUNT: (...args: unknown[]) => args.filter(a => a !== null && a !== undefined).length,
  SUM: (...args: unknown[]) => args.reduce((acc: number, v) => acc + (typeof v === 'number' ? v : 0), 0),
  MIN: (...args: unknown[]) => {
    const nums = args.filter(a => typeof a === 'number') as number[];
    return nums.length > 0 ? Math.min(...nums) : null;
  },
  MAX: (...args: unknown[]) => {
    const nums = args.filter(a => typeof a === 'number') as number[];
    return nums.length > 0 ? Math.max(...nums) : null;
  },
  AVG: (...args: unknown[]) => {
    const nums = args.filter(a => typeof a === 'number') as number[];
    return nums.length > 0 ? nums.reduce((a, b) => a + b, 0) / nums.length : null;
  },
};

/**
 * Evaluate an expression against a row
 */
export function evaluateExpression(
  expr: Expression,
  row: Record<string, unknown>,
  options?: ReturningEvaluationOptions
): unknown {
  switch (expr.type) {
    case 'literal':
      return expr.value;

    case 'null':
      return null;

    case 'column': {
      const colName = expr.name;
      // Try qualified name first if table is specified
      if (expr.table && `${expr.table}.${colName}` in row) {
        return row[`${expr.table}.${colName}`];
      }
      return row[colName];
    }

    case 'function': {
      const args = expr.args.map(arg => evaluateExpression(arg, row, options));

      // Check custom evaluator first
      if (options?.functionEvaluator) {
        try {
          return options.functionEvaluator(expr.name, args);
        } catch {
          // Fall through to built-in
        }
      }

      // Use built-in function
      const fn = BUILTIN_FUNCTIONS[expr.name.toUpperCase()];
      if (fn) {
        return fn(...args);
      }

      // Unknown function - return null
      return null;
    }

    case 'binary': {
      const left = evaluateExpression(expr.left, row, options);
      const right = evaluateExpression(expr.right, row, options);
      return evaluateBinaryOp(expr.operator, left, right);
    }

    case 'unary': {
      const operand = evaluateExpression(expr.operand, row, options);
      return evaluateUnaryOp(expr.operator, operand);
    }

    case 'parameter':
      // Parameters should be resolved before evaluation
      return null;

    case 'subquery':
      // Subqueries need special handling
      return null;

    case 'default':
      return null;

    case 'case':
      // CASE expressions need special handling
      return null;

    default:
      return null;
  }
}

/**
 * Evaluate a binary operation
 */
function evaluateBinaryOp(op: string, left: unknown, right: unknown): unknown {
  // Handle null
  if (left === null || right === null) {
    if (op === 'IS') return left === right;
    if (op === 'IS NOT') return left !== right;
    if (op === '||') return left === null ? right : (right === null ? left : null);
    return null;
  }

  switch (op) {
    // Arithmetic
    case '+':
      return typeof left === 'number' && typeof right === 'number' ? left + right : null;
    case '-':
      return typeof left === 'number' && typeof right === 'number' ? left - right : null;
    case '*':
      return typeof left === 'number' && typeof right === 'number' ? left * right : null;
    case '/':
      if (typeof left === 'number' && typeof right === 'number' && right !== 0) {
        return left / right;
      }
      return null;
    case '%':
      if (typeof left === 'number' && typeof right === 'number' && right !== 0) {
        return left % right;
      }
      return null;

    // Comparison
    case '=':
      return left === right;
    case '!=':
    case '<>':
      return left !== right;
    case '<':
      return left < right;
    case '<=':
      return left <= right;
    case '>':
      return left > right;
    case '>=':
      return left >= right;

    // Logical
    case 'AND':
      return Boolean(left) && Boolean(right);
    case 'OR':
      return Boolean(left) || Boolean(right);

    // String
    case '||':
      return String(left) + String(right);

    // LIKE (simplified)
    case 'LIKE':
      if (typeof left === 'string' && typeof right === 'string') {
        const pattern = right.replace(/%/g, '.*').replace(/_/g, '.');
        return new RegExp(`^${pattern}$`, 'i').test(left);
      }
      return null;

    default:
      return null;
  }
}

/**
 * Evaluate a unary operation
 */
function evaluateUnaryOp(op: string, operand: unknown): unknown {
  switch (op) {
    case 'NOT':
      return !operand;
    case '-':
      return typeof operand === 'number' ? -operand : null;
    case '+':
      return typeof operand === 'number' ? operand : null;
    case '~':
      return typeof operand === 'number' ? ~operand : null;
    default:
      return null;
  }
}

/**
 * Evaluate a RETURNING clause against a row
 */
export function evaluateReturning(
  returning: ReturningClause,
  row: Record<string, unknown>,
  schemaColumns?: string[],
  options?: ReturningEvaluationOptions
): Record<string, unknown> {
  const result: Record<string, unknown> = {};

  for (const col of returning.columns) {
    if (col.expression === '*') {
      // Expand wildcard
      const columns = schemaColumns || Object.keys(row);
      for (const colName of columns) {
        result[colName] = row[colName];
      }
    } else {
      const columnInfo = getReturningColumns({ type: 'returning', columns: [col] })[0];
      const value = evaluateExpression(col.expression, row, options);
      result[columnInfo.outputName] = value;
    }
  }

  return result;
}

/**
 * Evaluate a RETURNING clause against multiple rows
 */
export function evaluateReturningMultiple(
  returning: ReturningClause,
  rows: Record<string, unknown>[],
  schemaColumns?: string[],
  options?: ReturningEvaluationOptions
): ReturningResult {
  if (rows.length === 0) {
    return {
      columns: getOutputColumnNames(returning),
      rows: [],
    };
  }

  const evaluatedRows = rows.map(row =>
    evaluateReturning(returning, row, schemaColumns, options)
  );

  // Get columns from first evaluated row
  const columns = Object.keys(evaluatedRows[0]);

  return {
    columns,
    rows: evaluatedRows,
  };
}

// =============================================================================
// SQL GENERATION
// =============================================================================

/**
 * Generate SQL string for a RETURNING clause
 */
export function generateReturningSql(returning: ReturningClause): string {
  const parts = returning.columns.map(col => {
    if (col.expression === '*') {
      return '*';
    }

    const exprSql = generateExpressionSql(col.expression);
    if (col.alias) {
      return `${exprSql} AS ${col.alias}`;
    }
    return exprSql;
  });

  return `RETURNING ${parts.join(', ')}`;
}

/**
 * Generate SQL for an expression
 */
function generateExpressionSql(expr: Expression): string {
  switch (expr.type) {
    case 'literal':
      if (typeof expr.value === 'string') {
        return `'${expr.value.replace(/'/g, "''")}'`;
      }
      if (expr.value === null) {
        return 'NULL';
      }
      return String(expr.value);

    case 'null':
      return 'NULL';

    case 'column':
      return expr.table ? `${expr.table}.${expr.name}` : expr.name;

    case 'function':
      const args = expr.args.map(generateExpressionSql).join(', ');
      const distinct = expr.distinct ? 'DISTINCT ' : '';
      return `${expr.name}(${distinct}${args})`;

    case 'binary':
      return `(${generateExpressionSql(expr.left)} ${expr.operator} ${generateExpressionSql(expr.right)})`;

    case 'unary':
      if (expr.operator === 'NOT') {
        return `NOT ${generateExpressionSql(expr.operand)}`;
      }
      return `${expr.operator}${generateExpressionSql(expr.operand)}`;

    case 'parameter':
      return expr.raw;

    default:
      return '?';
  }
}

// =============================================================================
// VALIDATION
// =============================================================================

/**
 * Validation result for a RETURNING clause
 */
export interface ReturningValidationResult {
  valid: boolean;
  errors: string[];
  warnings: string[];
}

/**
 * Validate a RETURNING clause against a table schema
 */
export function validateReturning(
  returning: ReturningClause,
  schemaColumns: string[],
  options?: { allowUnknownColumns?: boolean }
): ReturningValidationResult {
  const errors: string[] = [];
  const warnings: string[] = [];

  for (const col of returning.columns) {
    if (col.expression === '*') {
      continue; // Wildcard is always valid
    }

    if (col.expression.type === 'column') {
      const colName = col.expression.name;

      // Check for special SQLite columns
      const isSpecialColumn = ['rowid', '_rowid_', 'oid'].includes(colName.toLowerCase());

      if (!isSpecialColumn && !schemaColumns.includes(colName)) {
        if (options?.allowUnknownColumns) {
          warnings.push(`Column '${colName}' not found in schema`);
        } else {
          errors.push(`Column '${colName}' not found in schema`);
        }
      }
    }

    // Validate function calls
    if (col.expression.type === 'function') {
      const fnName = col.expression.name.toUpperCase();
      if (!BUILTIN_FUNCTIONS[fnName]) {
        warnings.push(`Unknown function '${col.expression.name}'`);
      }
    }
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
  };
}

// =============================================================================
// EXPORTS
// =============================================================================

export {
  type ReturningClause,
  type ReturningColumn,
  type Expression,
  type ColumnReference,
  type FunctionCall,
  type BinaryExpression,
  type LiteralExpression,
};
