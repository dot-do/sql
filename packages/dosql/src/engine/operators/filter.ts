/**
 * Filter Operator
 *
 * Evaluates WHERE predicates and filters rows that don't match.
 */

import {
  type FilterPlan,
  type ExecutionContext,
  type Operator,
  type Row,
  type Predicate,
  type Expression,
  type SqlValue,
} from '../types.js';

// =============================================================================
// EXPRESSION EVALUATION
// =============================================================================

/**
 * Evaluate an expression against a row
 */
export function evaluateExpression(expr: Expression, row: Row): SqlValue {
  switch (expr.type) {
    case 'columnRef': {
      if (expr.table) {
        // Try table.column first
        const fullKey = `${expr.table}.${expr.column}`;
        if (fullKey in row) return row[fullKey];
      }
      return row[expr.column] ?? null;
    }

    case 'literal':
      return expr.value;

    case 'binary': {
      const left = evaluateExpression(expr.left, row);
      const right = evaluateExpression(expr.right, row);

      // Arithmetic operators
      switch (expr.op) {
        case 'add':
          if (typeof left === 'number' && typeof right === 'number') return left + right;
          if (typeof left === 'bigint' && typeof right === 'bigint') return left + right;
          return null;
        case 'sub':
          if (typeof left === 'number' && typeof right === 'number') return left - right;
          if (typeof left === 'bigint' && typeof right === 'bigint') return left - right;
          return null;
        case 'mul':
          if (typeof left === 'number' && typeof right === 'number') return left * right;
          if (typeof left === 'bigint' && typeof right === 'bigint') return left * right;
          return null;
        case 'div':
          if (typeof left === 'number' && typeof right === 'number' && right !== 0) return left / right;
          if (typeof left === 'bigint' && typeof right === 'bigint' && right !== 0n) return left / right;
          return null;
        case 'mod':
          if (typeof left === 'number' && typeof right === 'number' && right !== 0) return left % right;
          if (typeof left === 'bigint' && typeof right === 'bigint' && right !== 0n) return left % right;
          return null;
      }

      // Comparison operators (for use in boolean context)
      switch (expr.op) {
        case 'eq': return left === right;
        case 'ne': return left !== right;
        case 'lt': return left !== null && right !== null && left < right;
        case 'le': return left !== null && right !== null && left <= right;
        case 'gt': return left !== null && right !== null && left > right;
        case 'ge': return left !== null && right !== null && left >= right;
        case 'like': return likeMatch(String(left), String(right));
        case 'and': return Boolean(left) && Boolean(right);
        case 'or': return Boolean(left) || Boolean(right);
      }

      return null;
    }

    case 'unary': {
      const operand = evaluateExpression(expr.operand, row);
      switch (expr.op) {
        case 'not': return !operand;
        case 'neg':
          if (typeof operand === 'number') return -operand;
          if (typeof operand === 'bigint') return -operand;
          return null;
        case 'isNull': return operand === null;
        case 'isNotNull': return operand !== null;
      }
      return null;
    }

    case 'function':
      return evaluateFunction(expr.name, expr.args.map(arg => evaluateExpression(arg, row)));

    case 'aggregate':
      // Aggregates are handled by the aggregate operator, not here
      // This case shouldn't be reached during row-level evaluation
      return null;

    case 'case': {
      for (const { condition, result } of expr.when) {
        if (evaluateExpression(condition, row)) {
          return evaluateExpression(result, row);
        }
      }
      return expr.else ? evaluateExpression(expr.else, row) : null;
    }

    case 'subquery':
      // Subqueries are handled during planning, not execution
      throw new Error('Subquery evaluation not supported in expression context');

    default:
      return null;
  }
}

/**
 * Evaluate a built-in function
 */
function evaluateFunction(name: string, args: SqlValue[]): SqlValue {
  const lowerName = name.toLowerCase();

  switch (lowerName) {
    // String functions
    case 'length':
    case 'len':
      return typeof args[0] === 'string' ? args[0].length : null;
    case 'upper':
      return typeof args[0] === 'string' ? args[0].toUpperCase() : null;
    case 'lower':
      return typeof args[0] === 'string' ? args[0].toLowerCase() : null;
    case 'trim':
      return typeof args[0] === 'string' ? args[0].trim() : null;
    case 'ltrim':
      return typeof args[0] === 'string' ? args[0].trimStart() : null;
    case 'rtrim':
      return typeof args[0] === 'string' ? args[0].trimEnd() : null;
    case 'substring':
    case 'substr':
      if (typeof args[0] === 'string' && typeof args[1] === 'number') {
        const start = args[1] - 1; // SQL is 1-indexed
        const length = typeof args[2] === 'number' ? args[2] : undefined;
        return args[0].substring(start, length ? start + length : undefined);
      }
      return null;
    case 'concat':
      return args.map(a => String(a ?? '')).join('');
    case 'replace':
      if (typeof args[0] === 'string' && typeof args[1] === 'string' && typeof args[2] === 'string') {
        return args[0].replace(new RegExp(escapeRegex(args[1]), 'g'), args[2]);
      }
      return null;

    // Numeric functions
    case 'abs':
      if (typeof args[0] === 'number') return Math.abs(args[0]);
      if (typeof args[0] === 'bigint') return args[0] < 0n ? -args[0] : args[0];
      return null;
    case 'ceil':
    case 'ceiling':
      return typeof args[0] === 'number' ? Math.ceil(args[0]) : null;
    case 'floor':
      return typeof args[0] === 'number' ? Math.floor(args[0]) : null;
    case 'round':
      if (typeof args[0] === 'number') {
        const precision = typeof args[1] === 'number' ? args[1] : 0;
        const factor = Math.pow(10, precision);
        return Math.round(args[0] * factor) / factor;
      }
      return null;
    case 'sqrt':
      return typeof args[0] === 'number' ? Math.sqrt(args[0]) : null;
    case 'power':
    case 'pow':
      if (typeof args[0] === 'number' && typeof args[1] === 'number') {
        return Math.pow(args[0], args[1]);
      }
      return null;

    // Date functions
    case 'now':
    case 'current_timestamp':
      return new Date();
    case 'date':
      if (args[0] instanceof Date) return args[0];
      if (typeof args[0] === 'string') return new Date(args[0]);
      return null;
    case 'year':
      return args[0] instanceof Date ? args[0].getFullYear() : null;
    case 'month':
      return args[0] instanceof Date ? args[0].getMonth() + 1 : null;
    case 'day':
      return args[0] instanceof Date ? args[0].getDate() : null;
    case 'hour':
      return args[0] instanceof Date ? args[0].getHours() : null;
    case 'minute':
      return args[0] instanceof Date ? args[0].getMinutes() : null;
    case 'second':
      return args[0] instanceof Date ? args[0].getSeconds() : null;

    // Null handling
    case 'coalesce':
      for (const arg of args) {
        if (arg !== null) return arg;
      }
      return null;
    case 'nullif':
      return args[0] === args[1] ? null : args[0];
    case 'ifnull':
    case 'isnull':
      return args[0] ?? args[1];

    // Type conversion
    case 'cast':
    case 'convert':
      // Simplified cast - just return the value
      return args[0];

    default:
      throw new Error(`Unknown function: ${name}`);
  }
}

/**
 * Escape special regex characters
 */
function escapeRegex(str: string): string {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * SQL LIKE pattern matching
 */
function likeMatch(value: string, pattern: string): boolean {
  // Convert SQL LIKE pattern to regex
  // % matches any sequence, _ matches single character
  const regexPattern = pattern
    .replace(/[.*+?^${}()|[\]\\]/g, '\\$&') // Escape regex special chars
    .replace(/%/g, '.*')  // % -> .*
    .replace(/_/g, '.');  // _ -> .

  return new RegExp(`^${regexPattern}$`, 'i').test(value);
}

// =============================================================================
// PREDICATE EVALUATION
// =============================================================================

/**
 * Evaluate a predicate against a row
 */
export function evaluatePredicate(predicate: Predicate, row: Row): boolean {
  switch (predicate.type) {
    case 'comparison': {
      const left = evaluateExpression(predicate.left, row);
      const right = evaluateExpression(predicate.right, row);

      // Handle null comparisons
      if (left === null || right === null) {
        // In SQL, NULL comparisons return NULL (false in boolean context)
        return predicate.op === 'ne' ? left !== right : false;
      }

      switch (predicate.op) {
        case 'eq': return left === right;
        case 'ne': return left !== right;
        case 'lt': return left < right;
        case 'le': return left <= right;
        case 'gt': return left > right;
        case 'ge': return left >= right;
        case 'like': return likeMatch(String(left), String(right));
        default: return false;
      }
    }

    case 'logical': {
      switch (predicate.op) {
        case 'and':
          return predicate.operands.every(op => evaluatePredicate(op, row));
        case 'or':
          return predicate.operands.some(op => evaluatePredicate(op, row));
        case 'not':
          return !evaluatePredicate(predicate.operands[0], row);
        default:
          return false;
      }
    }

    case 'between': {
      const value = evaluateExpression(predicate.expr, row);
      const low = evaluateExpression(predicate.low, row);
      const high = evaluateExpression(predicate.high, row);

      if (value === null || low === null || high === null) return false;
      return value >= low && value <= high;
    }

    case 'in': {
      const value = evaluateExpression(predicate.expr, row);
      if (value === null) return false;

      if (Array.isArray(predicate.values)) {
        // List of expressions
        for (const expr of predicate.values) {
          const v = evaluateExpression(expr, row);
          if (v === value) return true;
        }
        return false;
      }
      // Subquery - would need executor context
      throw new Error('IN with subquery not supported');
    }

    case 'isNull': {
      const value = evaluateExpression(predicate.expr, row);
      return predicate.isNot ? value !== null : value === null;
    }

    default:
      return false;
  }
}

// =============================================================================
// FILTER OPERATOR
// =============================================================================

/**
 * Filter operator implementation
 */
export class FilterOperator implements Operator {
  private plan: FilterPlan;
  private input: Operator;
  private ctx!: ExecutionContext;

  constructor(plan: FilterPlan, input: Operator, ctx: ExecutionContext) {
    this.plan = plan;
    this.input = input;
    this.ctx = ctx;
  }

  async open(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx;
    await this.input.open(ctx);
  }

  async next(): Promise<Row | null> {
    while (true) {
      const row = await this.input.next();
      if (row === null) return null;

      if (evaluatePredicate(this.plan.predicate, row)) {
        return row;
      }
    }
  }

  async close(): Promise<void> {
    await this.input.close();
  }

  columns(): string[] {
    return this.input.columns();
  }
}
