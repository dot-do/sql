/**
 * RETURNING Clause Types and Helpers
 *
 * Extracted from database.ts to reduce the size of the main DoSQLDatabase class.
 * Handles parsing, evaluation, and application of SQL RETURNING clauses.
 */

import {
  StatementError,
  StatementErrorCode,
} from '../errors/index.js';

// =============================================================================
// Types
// =============================================================================

export interface ReturningColumn {
  expression: string | '*';
  alias?: string;
}

export interface ParsedReturning {
  columns: ReturningColumn[];
}

// =============================================================================
// Parsing
// =============================================================================

/**
 * Parse RETURNING clause from SQL string
 */
export function parseReturningClause(sql: string): ParsedReturning | null {
  const returningMatch = sql.match(/\s+RETURNING\s+(.+?)(?:;?\s*$)/i);
  if (!returningMatch) return null;

  const columnsStr = returningMatch[1].trim();
  const columns: ReturningColumn[] = [];

  // Handle RETURNING *
  if (columnsStr === '*') {
    return { columns: [{ expression: '*' }] };
  }

  // Parse column expressions with optional aliases
  // This is a simplified parser - handles basic cases:
  // - column_name
  // - column_name AS alias
  // - expression AS alias
  // - function(args) AS alias
  const parts = splitColumns(columnsStr);

  for (const part of parts) {
    const trimmed = part.trim();
    if (!trimmed) continue;

    // Check for AS alias
    const asMatch = trimmed.match(/^(.+?)\s+AS\s+(\w+)$/i);
    if (asMatch) {
      columns.push({
        expression: asMatch[1].trim(),
        alias: asMatch[2],
      });
    } else {
      columns.push({ expression: trimmed });
    }
  }

  return { columns };
}

/**
 * Split column list by commas, respecting parentheses
 */
export function splitColumns(str: string): string[] {
  const result: string[] = [];
  let current = '';
  let depth = 0;

  for (const char of str) {
    if (char === '(') {
      depth++;
      current += char;
    } else if (char === ')') {
      depth--;
      current += char;
    } else if (char === ',' && depth === 0) {
      result.push(current);
      current = '';
    } else {
      current += char;
    }
  }

  if (current) {
    result.push(current);
  }

  return result;
}

// =============================================================================
// Expression Evaluation
// =============================================================================

/**
 * Evaluate an expression against a row
 */
export function evaluateExpression(
  expr: string,
  row: Record<string, unknown>,
  schemaColumns: string[]
): unknown {
  const trimmed = expr.trim();

  // Handle string literals
  if ((trimmed.startsWith("'") && trimmed.endsWith("'")) ||
      (trimmed.startsWith('"') && trimmed.endsWith('"'))) {
    return trimmed.slice(1, -1);
  }

  // Handle numeric literals
  const num = Number(trimmed);
  if (!isNaN(num) && trimmed !== '') {
    return num;
  }

  // Handle NULL
  if (trimmed.toUpperCase() === 'NULL') {
    return null;
  }

  // Handle rowid/_rowid_/oid
  const lowerExpr = trimmed.toLowerCase();
  if (lowerExpr === 'rowid' || lowerExpr === '_rowid_' || lowerExpr === 'oid') {
    return row.id ?? row.rowid ?? row._rowid_;
  }

  // Handle column reference (possibly qualified: table.column)
  if (/^[\w.]+$/.test(trimmed)) {
    const colName = trimmed.includes('.') ? trimmed.split('.').pop()! : trimmed;
    return row[colName];
  }

  // Handle arithmetic expressions (e.g., price * quantity, price * 1.1)
  const arithmeticMatch = trimmed.match(/^([\w.]+)\s*([+\-*/])\s*([\w.]+)$/);
  if (arithmeticMatch) {
    const left = evaluateExpression(arithmeticMatch[1], row, schemaColumns);
    const op = arithmeticMatch[2];
    const right = evaluateExpression(arithmeticMatch[3], row, schemaColumns);

    const leftNum = Number(left);
    const rightNum = Number(right);

    if (!isNaN(leftNum) && !isNaN(rightNum)) {
      switch (op) {
        case '+': return leftNum + rightNum;
        case '-': return leftNum - rightNum;
        case '*': return leftNum * rightNum;
        case '/': return rightNum !== 0 ? leftNum / rightNum : null;
      }
    }
    return null;
  }

  // Handle string concatenation (col1 || ' ' || col2)
  if (trimmed.includes('||')) {
    const parts = trimmed.split('||').map((p) => p.trim());
    return parts.map((p) => {
      const val = evaluateExpression(p, row, schemaColumns);
      return val === null ? '' : String(val);
    }).join('');
  }

  // Handle function calls
  const funcMatch = trimmed.match(/^(\w+)\s*\((.+)\)$/);
  if (funcMatch) {
    const funcName = funcMatch[1].toUpperCase();
    const argsStr = funcMatch[2];

    // Parse function arguments
    const args = splitColumns(argsStr).map((a) =>
      evaluateExpression(a.trim(), row, schemaColumns)
    );

    return evaluateFunction(funcName, args, row, schemaColumns);
  }

  // Default: treat as column name
  return row[trimmed];
}

/**
 * Evaluate a SQL function
 */
export function evaluateFunction(
  name: string,
  args: unknown[],
  row: Record<string, unknown>,
  schemaColumns: string[]
): unknown {
  switch (name) {
    case 'UPPER':
      return typeof args[0] === 'string' ? args[0].toUpperCase() : args[0];

    case 'LOWER':
      return typeof args[0] === 'string' ? args[0].toLowerCase() : args[0];

    case 'LENGTH':
      return typeof args[0] === 'string' ? args[0].length : null;

    case 'COALESCE':
      return args.find((a) => a !== null && a !== undefined) ?? null;

    case 'IFNULL':
      return args[0] !== null && args[0] !== undefined ? args[0] : args[1];

    case 'NULLIF':
      return args[0] === args[1] ? null : args[0];

    case 'SUBSTR':
    case 'SUBSTRING':
      if (typeof args[0] === 'string') {
        const start = Number(args[1]) - 1; // SQL is 1-indexed
        const len = args[2] !== undefined ? Number(args[2]) : undefined;
        return len !== undefined
          ? args[0].substring(start, start + len)
          : args[0].substring(start);
      }
      return null;

    case 'TRIM':
      return typeof args[0] === 'string' ? args[0].trim() : args[0];

    case 'ABS':
      return typeof args[0] === 'number' ? Math.abs(args[0]) : null;

    case 'ROUND':
      if (typeof args[0] === 'number') {
        const decimals = args[1] !== undefined ? Number(args[1]) : 0;
        const factor = Math.pow(10, decimals);
        return Math.round(args[0] * factor) / factor;
      }
      return null;

    case 'DATETIME':
      // Simplified: just return the value as-is or format date
      if (args[0] instanceof Date) {
        return args[0].toISOString().replace('T', ' ').split('.')[0];
      }
      return String(args[0]);

    case 'DATE':
      if (args[0] instanceof Date) {
        return args[0].toISOString().split('T')[0];
      }
      return String(args[0]);

    case 'TIME':
      if (args[0] instanceof Date) {
        return args[0].toISOString().split('T')[1].split('.')[0];
      }
      return String(args[0]);

    // Aggregate functions in RETURNING context should error
    case 'SUM':
    case 'COUNT':
    case 'AVG':
    case 'MIN':
    case 'MAX':
      throw new StatementError(
        StatementErrorCode.UNSUPPORTED,
        `Aggregate function ${name} is not allowed in RETURNING clause`
      );

    default:
      // Unknown function - try to evaluate nested expressions
      return null;
  }
}

// =============================================================================
// RETURNING Application
// =============================================================================

/**
 * Apply RETURNING clause to rows
 */
export function applyReturning(
  returning: ParsedReturning,
  rows: Record<string, unknown>[],
  schemaColumns: string[]
): Record<string, unknown>[] {
  // Validate column references before processing
  for (const col of returning.columns) {
    if (col.expression !== '*') {
      validateColumnReference(col.expression, schemaColumns);
    }
  }

  return rows.map((row) => {
    const result: Record<string, unknown> = {};

    for (const col of returning.columns) {
      if (col.expression === '*') {
        // Return all columns
        for (const schemaCol of schemaColumns) {
          result[schemaCol] = row[schemaCol];
        }
        // Also include id/rowid if present
        if ('id' in row && !schemaColumns.includes('id')) {
          result.id = row.id;
        }
      } else {
        const value = evaluateExpression(col.expression, row, schemaColumns);
        const outputName = col.alias || getExpressionName(col.expression);
        result[outputName] = value;
      }
    }

    return result;
  });
}

/**
 * Validate column references in an expression
 * Throws error if non-existent column is referenced
 */
export function validateColumnReference(expr: string, schemaColumns: string[]): void {
  const trimmed = expr.trim();

  // Skip literals (numbers, strings)
  if (!isNaN(Number(trimmed))) return;
  if ((trimmed.startsWith("'") && trimmed.endsWith("'")) ||
      (trimmed.startsWith('"') && trimmed.endsWith('"'))) return;
  if (trimmed.toUpperCase() === 'NULL') return;

  // Skip rowid variants
  const lower = trimmed.toLowerCase();
  if (lower === 'rowid' || lower === '_rowid_' || lower === 'oid') return;

  // Handle function calls - validate arguments
  const funcMatch = trimmed.match(/^(\w+)\s*\((.+)\)$/);
  if (funcMatch) {
    const argsStr = funcMatch[2];
    const args = splitColumns(argsStr);
    for (const arg of args) {
      validateColumnReference(arg.trim(), schemaColumns);
    }
    return;
  }

  // Handle arithmetic expressions
  const arithmeticMatch = trimmed.match(/^([\w.]+)\s*[+\-*/]\s*([\w.]+)$/);
  if (arithmeticMatch) {
    validateColumnReference(arithmeticMatch[1], schemaColumns);
    validateColumnReference(arithmeticMatch[2], schemaColumns);
    return;
  }

  // Handle string concatenation
  if (trimmed.includes('||')) {
    const parts = trimmed.split('||').map((p) => p.trim());
    for (const part of parts) {
      validateColumnReference(part, schemaColumns);
    }
    return;
  }

  // Plain column reference (possibly qualified)
  if (/^[\w.]+$/.test(trimmed)) {
    const colName = trimmed.includes('.') ? trimmed.split('.').pop()! : trimmed;

    // Check if column exists in schema (or is id/primary key)
    if (!schemaColumns.includes(colName) && colName !== 'id') {
      throw new StatementError(
        StatementErrorCode.COLUMN_NOT_FOUND,
        `no such column: ${colName}`,
        undefined,
        { context: { column: colName } }
      );
    }
  }
}

/**
 * Get output column name for an expression
 */
export function getExpressionName(expr: string): string {
  // For column references, return column name
  if (/^\w+$/.test(expr)) {
    return expr;
  }
  // For qualified references (table.column), return column name
  if (expr.includes('.') && /^[\w.]+$/.test(expr)) {
    return expr.split('.').pop()!;
  }
  // For expressions without alias, return the expression as-is
  return expr;
}

/**
 * Extract SQL without RETURNING clause
 */
export function stripReturningClause(sql: string): string {
  return sql.replace(/\s+RETURNING\s+.+?(?:;?\s*$)/i, '');
}
