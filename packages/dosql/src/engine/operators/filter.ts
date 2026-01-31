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
// HELPER FUNCTIONS
// =============================================================================

/**
 * Convert a SQL value to a boolean for logical operations
 * In SQL: 0 is false, non-zero numbers are true, NULL is NULL
 */
function sqlToBool(value: SqlValue): boolean {
  if (value === null) return false;
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  if (typeof value === 'bigint') return value !== 0n;
  if (typeof value === 'string') return value.length > 0;
  return true;
}

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
      // Try direct column name
      if (expr.column in row) return row[expr.column];
      // Try finding column in prefixed keys (e.g., "users.name" when looking for "name")
      for (const key of Object.keys(row)) {
        if (key.endsWith(`.${expr.column}`)) {
          return row[key];
        }
      }
      return null;
    }

    case 'literal':
      return expr.value;

    case 'binary': {
      const left = evaluateExpression(expr.left, row);
      const right = evaluateExpression(expr.right, row);

      // Arithmetic operators (support both named and symbol forms)
      switch (expr.op) {
        case 'add':
        case '+' as any:
          if (typeof left === 'number' && typeof right === 'number') return left + right;
          if (typeof left === 'bigint' && typeof right === 'bigint') return left + right;
          return null;
        case 'sub':
        case '-' as any:
          if (typeof left === 'number' && typeof right === 'number') return left - right;
          if (typeof left === 'bigint' && typeof right === 'bigint') return left - right;
          return null;
        case 'mul':
        case '*' as any:
          if (typeof left === 'number' && typeof right === 'number') return left * right;
          if (typeof left === 'bigint' && typeof right === 'bigint') return left * right;
          return null;
        case 'div':
        case '/' as any:
          if (typeof left === 'number' && typeof right === 'number' && right !== 0) return left / right;
          if (typeof left === 'bigint' && typeof right === 'bigint' && right !== 0n) return left / right;
          return null;
        case 'mod':
        case '%' as any:
          if (typeof left === 'number' && typeof right === 'number' && right !== 0) return left % right;
          if (typeof left === 'bigint' && typeof right === 'bigint' && right !== 0n) return left % right;
          return null;
      }

      // Comparison operators - return SQL integers (1 for true, 0 for false, null if either operand is null)
      switch (expr.op) {
        case 'eq':
          // NULL = NULL returns NULL in SQL (not true)
          if (left === null || right === null) return null;
          return left === right ? 1 : 0;
        case 'ne':
          if (left === null || right === null) return null;
          return left !== right ? 1 : 0;
        case 'lt':
          if (left === null || right === null) return null;
          return left < right ? 1 : 0;
        case 'le':
          if (left === null || right === null) return null;
          return left <= right ? 1 : 0;
        case 'gt':
          if (left === null || right === null) return null;
          return left > right ? 1 : 0;
        case 'ge':
          if (left === null || right === null) return null;
          return left >= right ? 1 : 0;
        case 'like':
          if (left === null || right === null) return null;
          return likeMatch(String(left), String(right)) ? 1 : 0;
        case 'and':
          // SQL AND: NULL AND FALSE = FALSE, NULL AND TRUE = NULL
          if (left === null && right === null) return null;
          if (left === null) return sqlToBool(right) ? null : 0;
          if (right === null) return sqlToBool(left) ? null : 0;
          return sqlToBool(left) && sqlToBool(right) ? 1 : 0;
        case 'or':
          // SQL OR: NULL OR TRUE = TRUE, NULL OR FALSE = NULL
          if (left === null && right === null) return null;
          if (left === null) return sqlToBool(right) ? 1 : null;
          if (right === null) return sqlToBool(left) ? 1 : null;
          return sqlToBool(left) || sqlToBool(right) ? 1 : 0;
      }

      return null;
    }

    case 'unary': {
      const operand = evaluateExpression(expr.operand, row);
      switch (expr.op) {
        case 'not':
          // NOT NULL returns NULL in SQL
          if (operand === null) return null;
          return sqlToBool(operand) ? 0 : 1;
        case 'neg':
          if (typeof operand === 'number') return -operand;
          if (typeof operand === 'bigint') return -operand;
          return null;
        case 'isNull':
          // IS NULL always returns 1 or 0 (never NULL)
          return operand === null ? 1 : 0;
        case 'isNotNull':
          // IS NOT NULL always returns 1 or 0 (never NULL)
          return operand !== null ? 1 : 0;
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
      // Check if this is a simple CASE (has operand) or searched CASE
      if (expr.operand !== undefined) {
        // Simple CASE: CASE operand WHEN value THEN result END
        const operandValue = evaluateExpression(expr.operand, row);
        for (const whenClause of expr.when) {
          // Simple CASE clauses have 'value' property
          if ('value' in whenClause) {
            const whenValue = evaluateExpression(whenClause.value, row);
            // SQL equality: NULL is not equal to anything, including NULL
            if (operandValue !== null && whenValue !== null && operandValue === whenValue) {
              return evaluateExpression(whenClause.result, row);
            }
          }
        }
      } else {
        // Searched CASE: CASE WHEN condition THEN result END
        for (const whenClause of expr.when) {
          // Searched CASE clauses have 'condition' property
          if ('condition' in whenClause) {
            const conditionValue = evaluateExpression(whenClause.condition, row);
            if (sqlToBool(conditionValue)) {
              return evaluateExpression(whenClause.result, row);
            }
          }
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

      if (Array.isArray(predicate.values)) {
        // Per SQL standard: When the right operand is an empty set, IN returns false
        // regardless of the left operand (even if NULL)
        if (predicate.values.length === 0) {
          return false;
        }

        // If left operand is NULL and list is non-empty, result depends on list contents
        // - If list contains the value: true (but NULL can't equal anything)
        // - If list doesn't contain the value but contains NULL: NULL (treated as false in boolean context)
        // - If list doesn't contain the value and no NULL: false
        if (value === null) return false;

        // List of expressions - check for match
        let hasNull = false;
        for (const expr of predicate.values) {
          const v = evaluateExpression(expr, row);
          if (v === value) return true;
          if (v === null) hasNull = true;
        }
        // If no match found and list contains NULL, result should be NULL (false in boolean context)
        // If no match found and no NULL in list, result is false
        return false;
      }

      // Subquery case - predicate.values is a QueryPlan
      // The subquery should have been executed and materialized into the predicate
      // For now, if we get a plan object, check if it has been materialized
      const subqueryValues = predicate.values as unknown;
      if (subqueryValues && typeof subqueryValues === 'object' && 'materializedValues' in subqueryValues) {
        const values = (subqueryValues as { materializedValues: unknown[] }).materializedValues;
        if (!Array.isArray(values)) return false;

        // Empty subquery result: IN returns false
        if (values.length === 0) return false;

        if (value === null) return false;

        for (const v of values) {
          if (v === value) return true;
        }
        return false;
      }

      // Subquery needs to be executed during planning/materialization phase
      throw new Error('IN with subquery requires subquery to be materialized first');
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
