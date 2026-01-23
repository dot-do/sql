/**
 * SQL Expression Evaluation Utilities
 *
 * Provides utilities for evaluating SQL expressions in JavaScript,
 * particularly for WHEN clauses in triggers and other conditional logic.
 *
 * SECURITY: Uses a safe recursive descent parser instead of new Function()
 * to prevent arbitrary code execution.
 */

// =============================================================================
// SAFE EXPRESSION TOKENIZER
// =============================================================================

/**
 * Token types for the expression parser
 */
type TokenType =
  | 'NEW'
  | 'OLD'
  | 'DOT'
  | 'IDENTIFIER'
  | 'STRING'
  | 'NUMBER'
  | 'NULL'
  | 'TRUE'
  | 'FALSE'
  | 'AND'
  | 'OR'
  | 'NOT'
  | 'IS'
  | 'LIKE'
  | 'IN'
  | 'EQ'
  | 'NEQ'
  | 'LT'
  | 'GT'
  | 'LTE'
  | 'GTE'
  | 'LPAREN'
  | 'RPAREN'
  | 'COMMA'
  | 'EOF';

interface Token {
  type: TokenType;
  value: string | number | boolean | null;
  pos: number;
}

/**
 * Tokenize a SQL WHEN clause expression
 */
function tokenize(expr: string): Token[] {
  const tokens: Token[] = [];
  let pos = 0;

  while (pos < expr.length) {
    // Skip whitespace
    while (pos < expr.length && /\s/.test(expr[pos])) {
      pos++;
    }
    if (pos >= expr.length) break;

    const startPos = pos;
    const char = expr[pos];

    // Single character tokens
    if (char === '(') {
      tokens.push({ type: 'LPAREN', value: '(', pos: startPos });
      pos++;
      continue;
    }
    if (char === ')') {
      tokens.push({ type: 'RPAREN', value: ')', pos: startPos });
      pos++;
      continue;
    }
    if (char === ',') {
      tokens.push({ type: 'COMMA', value: ',', pos: startPos });
      pos++;
      continue;
    }
    if (char === '.') {
      tokens.push({ type: 'DOT', value: '.', pos: startPos });
      pos++;
      continue;
    }

    // Two character operators
    if (expr.slice(pos, pos + 2) === '!=') {
      tokens.push({ type: 'NEQ', value: '!=', pos: startPos });
      pos += 2;
      continue;
    }
    if (expr.slice(pos, pos + 2) === '<>') {
      tokens.push({ type: 'NEQ', value: '<>', pos: startPos });
      pos += 2;
      continue;
    }
    if (expr.slice(pos, pos + 2) === '<=') {
      tokens.push({ type: 'LTE', value: '<=', pos: startPos });
      pos += 2;
      continue;
    }
    if (expr.slice(pos, pos + 2) === '>=') {
      tokens.push({ type: 'GTE', value: '>=', pos: startPos });
      pos += 2;
      continue;
    }

    // Single character operators
    if (char === '=') {
      tokens.push({ type: 'EQ', value: '=', pos: startPos });
      pos++;
      continue;
    }
    if (char === '<') {
      tokens.push({ type: 'LT', value: '<', pos: startPos });
      pos++;
      continue;
    }
    if (char === '>') {
      tokens.push({ type: 'GT', value: '>', pos: startPos });
      pos++;
      continue;
    }

    // String literals (single or double quotes)
    if (char === "'" || char === '"') {
      const quote = char;
      pos++;
      let str = '';
      while (pos < expr.length && expr[pos] !== quote) {
        if (expr[pos] === '\\' && pos + 1 < expr.length) {
          pos++;
          str += expr[pos];
        } else {
          str += expr[pos];
        }
        pos++;
      }
      pos++; // Skip closing quote
      tokens.push({ type: 'STRING', value: str, pos: startPos });
      continue;
    }

    // Numbers
    if (/[0-9]/.test(char) || (char === '-' && pos + 1 < expr.length && /[0-9]/.test(expr[pos + 1]))) {
      let numStr = '';
      if (char === '-') {
        numStr += char;
        pos++;
      }
      while (pos < expr.length && /[0-9.]/.test(expr[pos])) {
        numStr += expr[pos];
        pos++;
      }
      tokens.push({ type: 'NUMBER', value: parseFloat(numStr), pos: startPos });
      continue;
    }

    // Keywords and identifiers
    if (/[a-zA-Z_]/.test(char)) {
      let ident = '';
      while (pos < expr.length && /[a-zA-Z0-9_]/.test(expr[pos])) {
        ident += expr[pos];
        pos++;
      }
      const upper = ident.toUpperCase();

      switch (upper) {
        case 'NEW':
          tokens.push({ type: 'NEW', value: 'NEW', pos: startPos });
          break;
        case 'OLD':
          tokens.push({ type: 'OLD', value: 'OLD', pos: startPos });
          break;
        case 'AND':
          tokens.push({ type: 'AND', value: 'AND', pos: startPos });
          break;
        case 'OR':
          tokens.push({ type: 'OR', value: 'OR', pos: startPos });
          break;
        case 'NOT':
          tokens.push({ type: 'NOT', value: 'NOT', pos: startPos });
          break;
        case 'IS':
          tokens.push({ type: 'IS', value: 'IS', pos: startPos });
          break;
        case 'LIKE':
          tokens.push({ type: 'LIKE', value: 'LIKE', pos: startPos });
          break;
        case 'IN':
          tokens.push({ type: 'IN', value: 'IN', pos: startPos });
          break;
        case 'NULL':
          tokens.push({ type: 'NULL', value: null, pos: startPos });
          break;
        case 'TRUE':
          tokens.push({ type: 'TRUE', value: true, pos: startPos });
          break;
        case 'FALSE':
          tokens.push({ type: 'FALSE', value: false, pos: startPos });
          break;
        default:
          tokens.push({ type: 'IDENTIFIER', value: ident, pos: startPos });
      }
      continue;
    }

    // Unknown character - skip it
    pos++;
  }

  tokens.push({ type: 'EOF', value: null, pos: expr.length });
  return tokens;
}

// =============================================================================
// SAFE EXPRESSION PARSER AND EVALUATOR
// =============================================================================

type EvalValue = string | number | boolean | null;

interface EvalContext {
  OLD: Record<string, unknown> | undefined;
  NEW: Record<string, unknown> | undefined;
}

/**
 * Safe recursive descent parser for SQL WHEN clause expressions.
 *
 * Grammar:
 *   expression     -> or_expr
 *   or_expr        -> and_expr (OR and_expr)*
 *   and_expr       -> not_expr (AND not_expr)*
 *   not_expr       -> NOT? comparison
 *   comparison     -> primary ((EQ|NEQ|LT|GT|LTE|GTE) primary)?
 *                   | primary IS NOT? NULL
 *                   | primary NOT? LIKE pattern
 *                   | primary NOT? IN (value_list)
 *   primary        -> literal | ref | LPAREN expression RPAREN
 *   ref            -> (NEW|OLD) DOT IDENTIFIER
 *   literal        -> STRING | NUMBER | NULL | TRUE | FALSE
 */
class SafeExpressionEvaluator {
  private tokens: Token[];
  private pos: number;
  private ctx: EvalContext;

  constructor(tokens: Token[], ctx: EvalContext) {
    this.tokens = tokens;
    this.pos = 0;
    this.ctx = ctx;
  }

  private peek(): Token {
    return this.tokens[this.pos] || { type: 'EOF', value: null, pos: -1 };
  }

  private advance(): Token {
    const token = this.peek();
    this.pos++;
    return token;
  }

  private match(...types: TokenType[]): boolean {
    const token = this.peek();
    return types.includes(token.type);
  }

  private expect(type: TokenType): Token {
    const token = this.peek();
    if (token.type !== type) {
      throw new Error(`Expected ${type} but got ${token.type} at position ${token.pos}`);
    }
    return this.advance();
  }

  evaluate(): EvalValue {
    const result = this.orExpr();
    if (!this.match('EOF')) {
      throw new Error(`Unexpected token: ${this.peek().type}`);
    }
    return result;
  }

  private orExpr(): EvalValue {
    let left = this.andExpr();
    while (this.match('OR')) {
      this.advance();
      const right = this.andExpr();
      left = this.toBoolean(left) || this.toBoolean(right);
    }
    return left;
  }

  private andExpr(): EvalValue {
    let left = this.notExpr();
    while (this.match('AND')) {
      this.advance();
      const right = this.notExpr();
      left = this.toBoolean(left) && this.toBoolean(right);
    }
    return left;
  }

  private notExpr(): EvalValue {
    if (this.match('NOT')) {
      this.advance();
      const value = this.comparison();
      return !this.toBoolean(value);
    }
    return this.comparison();
  }

  private comparison(): EvalValue {
    const left = this.primary();

    // IS NULL / IS NOT NULL
    if (this.match('IS')) {
      this.advance();
      let negated = false;
      if (this.match('NOT')) {
        this.advance();
        negated = true;
      }
      this.expect('NULL');
      const result = left === null;
      return negated ? !result : result;
    }

    // NOT LIKE / LIKE
    if (this.match('NOT')) {
      this.advance();
      if (this.match('LIKE')) {
        this.advance();
        const pattern = this.primary();
        return !this.evalLike(left, pattern);
      }
      if (this.match('IN')) {
        this.advance();
        const values = this.parseInList();
        return !values.some(v => this.evalEquals(left, v));
      }
      // Backtrack - put NOT back
      this.pos--;
      // Fall through to comparison operators
    }

    if (this.match('LIKE')) {
      this.advance();
      const pattern = this.primary();
      return this.evalLike(left, pattern);
    }

    // IN (value_list)
    if (this.match('IN')) {
      this.advance();
      const values = this.parseInList();
      return values.some(v => this.evalEquals(left, v));
    }

    // Comparison operators
    if (this.match('EQ')) {
      this.advance();
      const right = this.primary();
      return this.evalEquals(left, right);
    }
    if (this.match('NEQ')) {
      this.advance();
      const right = this.primary();
      return !this.evalEquals(left, right);
    }
    if (this.match('LT')) {
      this.advance();
      const right = this.primary();
      return this.evalLessThan(left, right);
    }
    if (this.match('GT')) {
      this.advance();
      const right = this.primary();
      return this.evalGreaterThan(left, right);
    }
    if (this.match('LTE')) {
      this.advance();
      const right = this.primary();
      return this.evalLessThan(left, right) || this.evalEquals(left, right);
    }
    if (this.match('GTE')) {
      this.advance();
      const right = this.primary();
      return this.evalGreaterThan(left, right) || this.evalEquals(left, right);
    }

    return left;
  }

  private parseInList(): EvalValue[] {
    this.expect('LPAREN');
    const values: EvalValue[] = [];
    if (!this.match('RPAREN')) {
      values.push(this.primary());
      while (this.match('COMMA')) {
        this.advance();
        values.push(this.primary());
      }
    }
    this.expect('RPAREN');
    return values;
  }

  private primary(): EvalValue {
    const token = this.peek();

    // Parenthesized expression
    if (this.match('LPAREN')) {
      this.advance();
      const value = this.orExpr();
      this.expect('RPAREN');
      return value;
    }

    // NEW.column or OLD.column reference
    if (this.match('NEW', 'OLD')) {
      const rowType = this.advance().type as 'NEW' | 'OLD';
      this.expect('DOT');
      const columnToken = this.expect('IDENTIFIER');
      const columnName = columnToken.value as string;
      const row = this.ctx[rowType];
      if (!row) return null;
      const value = row[columnName];
      return this.normalizeValue(value);
    }

    // Literals
    if (this.match('STRING')) {
      return this.advance().value as string;
    }
    if (this.match('NUMBER')) {
      return this.advance().value as number;
    }
    if (this.match('NULL')) {
      this.advance();
      return null;
    }
    if (this.match('TRUE')) {
      this.advance();
      return true;
    }
    if (this.match('FALSE')) {
      this.advance();
      return false;
    }

    // Bare identifier (treat as column name for backwards compatibility)
    if (this.match('IDENTIFIER')) {
      const ident = this.advance().value as string;
      // Check if it exists in NEW or OLD
      if (this.ctx.NEW && ident in this.ctx.NEW) {
        return this.normalizeValue(this.ctx.NEW[ident]);
      }
      if (this.ctx.OLD && ident in this.ctx.OLD) {
        return this.normalizeValue(this.ctx.OLD[ident]);
      }
      // Return as string literal if not found
      return ident;
    }

    throw new Error(`Unexpected token: ${token.type} at position ${token.pos}`);
  }

  private normalizeValue(value: unknown): EvalValue {
    if (value === undefined) return null;
    if (value === null) return null;
    if (typeof value === 'string') return value;
    if (typeof value === 'number') return value;
    if (typeof value === 'boolean') return value;
    return String(value);
  }

  private toBoolean(value: EvalValue): boolean {
    if (value === null) return false;
    if (typeof value === 'boolean') return value;
    if (typeof value === 'number') return value !== 0;
    if (typeof value === 'string') return value.length > 0;
    return true;
  }

  private evalEquals(left: EvalValue, right: EvalValue): boolean {
    // NULL comparisons
    if (left === null && right === null) return true;
    if (left === null || right === null) return false;
    // Type coercion for numbers
    if (typeof left === 'number' && typeof right === 'string') {
      const num = parseFloat(right);
      if (!isNaN(num)) return left === num;
    }
    if (typeof left === 'string' && typeof right === 'number') {
      const num = parseFloat(left);
      if (!isNaN(num)) return num === right;
    }
    return left === right;
  }

  private evalLessThan(left: EvalValue, right: EvalValue): boolean {
    if (left === null || right === null) return false;
    if (typeof left === 'number' && typeof right === 'number') {
      return left < right;
    }
    if (typeof left === 'string' && typeof right === 'string') {
      return left < right;
    }
    // Type coercion
    const leftNum = typeof left === 'string' ? parseFloat(left) : left;
    const rightNum = typeof right === 'string' ? parseFloat(right) : right;
    if (typeof leftNum === 'number' && typeof rightNum === 'number' && !isNaN(leftNum) && !isNaN(rightNum)) {
      return leftNum < rightNum;
    }
    return false;
  }

  private evalGreaterThan(left: EvalValue, right: EvalValue): boolean {
    if (left === null || right === null) return false;
    if (typeof left === 'number' && typeof right === 'number') {
      return left > right;
    }
    if (typeof left === 'string' && typeof right === 'string') {
      return left > right;
    }
    // Type coercion
    const leftNum = typeof left === 'string' ? parseFloat(left) : left;
    const rightNum = typeof right === 'string' ? parseFloat(right) : right;
    if (typeof leftNum === 'number' && typeof rightNum === 'number' && !isNaN(leftNum) && !isNaN(rightNum)) {
      return leftNum > rightNum;
    }
    return false;
  }

  private evalLike(value: EvalValue, pattern: EvalValue): boolean {
    if (value === null || pattern === null) return false;
    const strValue = String(value);
    const strPattern = String(pattern);

    // Convert SQL LIKE pattern to regex
    // % matches any sequence, _ matches any single character
    const regexPattern = strPattern
      .replace(/[.*+?^${}()|[\]\\]/g, '\\$&') // Escape regex special chars
      .replace(/%/g, '.*')  // % -> .*
      .replace(/_/g, '.');  // _ -> .

    const regex = new RegExp(`^${regexPattern}$`, 'i');
    return regex.test(strValue);
  }
}

// =============================================================================
// WHEN CLAUSE EVALUATION
// =============================================================================

/**
 * Evaluate a SQL WHEN clause condition.
 *
 * This function uses a safe recursive descent parser to evaluate SQL-style expressions.
 * It does NOT use new Function() or eval() to prevent arbitrary code execution.
 *
 * It supports:
 * - NEW.column and OLD.column references
 * - LIKE and NOT LIKE patterns
 * - IS NULL and IS NOT NULL checks
 * - AND, OR, NOT operators
 * - Comparison operators (=, !=, <>, <, >, <=, >=)
 * - IN (value_list) expressions
 * - Parenthesized expressions
 *
 * @param whenClause - The SQL WHEN clause expression
 * @param oldRow - The OLD row values (for UPDATE/DELETE triggers)
 * @param newRow - The NEW row values (for INSERT/UPDATE triggers)
 * @returns true if the condition is met, false otherwise
 *
 * @example
 * ```ts
 * // Check if email changed
 * const result = evaluateWhenClause(
 *   "NEW.email != OLD.email",
 *   { email: "old@example.com" },
 *   { email: "new@example.com" }
 * );
 * // result: true
 *
 * // Check with LIKE pattern
 * const valid = evaluateWhenClause(
 *   "NEW.email LIKE '%@%'",
 *   undefined,
 *   { email: "test@example.com" }
 * );
 * // valid: true
 * ```
 */
export function evaluateWhenClause<T extends Record<string, unknown>>(
  whenClause: string,
  oldRow: T | undefined,
  newRow: T | undefined
): boolean {
  if (!whenClause) return true;

  try {
    const tokens = tokenize(whenClause);
    const evaluator = new SafeExpressionEvaluator(tokens, {
      OLD: oldRow,
      NEW: newRow,
    });
    const result = evaluator.evaluate();

    // Convert result to boolean
    if (result === null) return false;
    if (typeof result === 'boolean') return result;
    if (typeof result === 'number') return result !== 0;
    if (typeof result === 'string') return result.length > 0;
    return true;
  } catch {
    // If evaluation fails, default to true (fire the trigger)
    return true;
  }
}

// =============================================================================
// ADDITIONAL SQL EVALUATION UTILITIES
// =============================================================================

/**
 * Check if two SQL values are equal using SQL semantics.
 *
 * In SQL, NULL = NULL is false (NULL is unknown), but this function
 * treats NULL = NULL as true for practical comparison purposes.
 * Use evaluateWhenClause for proper SQL NULL handling.
 *
 * @param a - First value
 * @param b - Second value
 * @returns true if values are equal
 */
export function sqlEquals(a: unknown, b: unknown): boolean {
  // Handle null comparisons
  if (a === null && b === null) return true;
  if (a === null || b === null) return false;

  // Handle different types
  if (typeof a !== typeof b) {
    // Try type coercion for numeric comparisons
    if (typeof a === 'number' && typeof b === 'string') {
      return a === Number(b);
    }
    if (typeof a === 'string' && typeof b === 'number') {
      return Number(a) === b;
    }
    return false;
  }

  return a === b;
}

/**
 * Evaluate a LIKE pattern match.
 *
 * @param value - The value to test
 * @param pattern - The SQL LIKE pattern (using % and _ wildcards)
 * @returns true if the value matches the pattern
 */
export function sqlLike(value: string, pattern: string): boolean {
  // Convert SQL LIKE pattern to regex
  const regexPattern = pattern
    .replace(/[.*+?^${}()|[\]\\]/g, '\\$&') // Escape regex chars
    .replace(/%/g, '.*') // % matches any sequence
    .replace(/_/g, '.'); // _ matches any single char

  const regex = new RegExp(`^${regexPattern}$`, 'i');
  return regex.test(value);
}

/**
 * Check if a value is truthy in SQL semantics.
 *
 * In SQL:
 * - 0 is false
 * - Empty string is typically truthy (varies by DB)
 * - NULL is neither true nor false
 * - Non-zero numbers are true
 *
 * @param value - The value to check
 * @returns true if the value is truthy in SQL semantics
 */
export function sqlTruthy(value: unknown): boolean {
  if (value === null || value === undefined) return false;
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  if (typeof value === 'string') return value.length > 0;
  return true;
}
