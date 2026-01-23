/**
 * Arithmetic Expression Parser and Evaluator for DoSQL
 *
 * Provides recursive descent parsing for SQL arithmetic expressions
 * with proper operator precedence and NULL propagation.
 */

import type { SqlValue } from './types.js';

// =============================================================================
// ARITHMETIC EXPRESSION PARSING
// =============================================================================

/**
 * Expression AST node types
 */
export type ExprNode =
  | { type: 'number'; value: number }
  | { type: 'column'; name: string }
  | { type: 'null' }
  | { type: 'string'; value: string }
  | { type: 'binary'; op: '+' | '-' | '*' | '/' | '%'; left: ExprNode; right: ExprNode }
  | { type: 'unary'; op: '+' | '-'; operand: ExprNode }
  | { type: 'function'; name: string; args: ExprNode[] };

/**
 * Token types for lexer
 */
interface Token {
  type: 'number' | 'identifier' | 'operator' | 'lparen' | 'rparen' | 'comma' | 'string' | 'null';
  value: string;
}

/**
 * Tokenize an expression string
 */
function tokenize(expr: string): Token[] {
  const tokens: Token[] = [];
  let i = 0;

  while (i < expr.length) {
    const ch = expr[i];

    // Skip whitespace
    if (/\s/.test(ch)) {
      i++;
      continue;
    }

    // Numbers (including decimals)
    if (/\d/.test(ch) || (ch === '.' && /\d/.test(expr[i + 1] || ''))) {
      let num = '';
      while (i < expr.length && (/\d/.test(expr[i]) || expr[i] === '.')) {
        num += expr[i++];
      }
      tokens.push({ type: 'number', value: num });
      continue;
    }

    // Identifiers and keywords
    if (/[a-zA-Z_]/.test(ch)) {
      let ident = '';
      while (i < expr.length && /[a-zA-Z0-9_]/.test(expr[i])) {
        ident += expr[i++];
      }
      if (ident.toUpperCase() === 'NULL') {
        tokens.push({ type: 'null', value: ident });
      } else {
        tokens.push({ type: 'identifier', value: ident });
      }
      continue;
    }

    // String literals
    if (ch === "'") {
      let str = '';
      i++; // skip opening quote
      while (i < expr.length && expr[i] !== "'") {
        str += expr[i++];
      }
      i++; // skip closing quote
      tokens.push({ type: 'string', value: str });
      continue;
    }

    // Operators
    if (['+', '-', '*', '/', '%'].includes(ch)) {
      tokens.push({ type: 'operator', value: ch });
      i++;
      continue;
    }

    // Parentheses
    if (ch === '(') {
      tokens.push({ type: 'lparen', value: '(' });
      i++;
      continue;
    }
    if (ch === ')') {
      tokens.push({ type: 'rparen', value: ')' });
      i++;
      continue;
    }

    // Comma
    if (ch === ',') {
      tokens.push({ type: 'comma', value: ',' });
      i++;
      continue;
    }

    // Unknown character - skip
    i++;
  }

  return tokens;
}

/**
 * Parser class for recursive descent parsing
 */
class ExpressionParser {
  private tokens: Token[];
  private pos = 0;

  constructor(tokens: Token[]) {
    this.tokens = tokens;
  }

  private peek(): Token | undefined {
    return this.tokens[this.pos];
  }

  private consume(): Token | undefined {
    return this.tokens[this.pos++];
  }

  private match(type: Token['type'], value?: string): boolean {
    const tok = this.peek();
    if (!tok) return false;
    if (tok.type !== type) return false;
    if (value !== undefined && tok.value !== value) return false;
    return true;
  }

  /**
   * Parse an expression (entry point)
   * expression = additive
   */
  parse(): ExprNode {
    return this.parseAdditive();
  }

  /**
   * Parse additive expressions (+ and -)
   * additive = multiplicative (('+' | '-') multiplicative)*
   */
  private parseAdditive(): ExprNode {
    let left = this.parseMultiplicative();

    while (this.match('operator', '+') || this.match('operator', '-')) {
      const op = this.consume()!.value as '+' | '-';
      const right = this.parseMultiplicative();
      left = { type: 'binary', op, left, right };
    }

    return left;
  }

  /**
   * Parse multiplicative expressions (*, /, %)
   * multiplicative = unary (('*' | '/' | '%') unary)*
   */
  private parseMultiplicative(): ExprNode {
    let left = this.parseUnary();

    while (this.match('operator', '*') || this.match('operator', '/') || this.match('operator', '%')) {
      const op = this.consume()!.value as '*' | '/' | '%';
      const right = this.parseUnary();
      left = { type: 'binary', op, left, right };
    }

    return left;
  }

  /**
   * Parse unary expressions (+ and -)
   * unary = ('+' | '-') unary | primary
   */
  private parseUnary(): ExprNode {
    if (this.match('operator', '+') || this.match('operator', '-')) {
      const op = this.consume()!.value as '+' | '-';
      const operand = this.parseUnary();
      return { type: 'unary', op, operand };
    }

    return this.parsePrimary();
  }

  /**
   * Parse primary expressions (numbers, columns, parentheses, functions)
   * primary = number | identifier | function_call | '(' expression ')' | NULL | string
   */
  private parsePrimary(): ExprNode {
    const tok = this.peek();

    if (!tok) {
      throw new Error('Unexpected end of expression');
    }

    // Number literal
    if (tok.type === 'number') {
      this.consume();
      return { type: 'number', value: parseFloat(tok.value) };
    }

    // NULL
    if (tok.type === 'null') {
      this.consume();
      return { type: 'null' };
    }

    // String literal
    if (tok.type === 'string') {
      this.consume();
      return { type: 'string', value: tok.value };
    }

    // Identifier (column or function)
    if (tok.type === 'identifier') {
      this.consume();

      // Check for function call
      if (this.match('lparen')) {
        this.consume(); // consume '('
        const args: ExprNode[] = [];

        // Parse arguments
        if (!this.match('rparen')) {
          args.push(this.parse());
          while (this.match('comma')) {
            this.consume(); // consume ','
            args.push(this.parse());
          }
        }

        if (this.match('rparen')) {
          this.consume(); // consume ')'
        }

        return { type: 'function', name: tok.value.toLowerCase(), args };
      }

      // Just a column reference
      return { type: 'column', name: tok.value };
    }

    // Parenthesized expression
    if (tok.type === 'lparen') {
      this.consume(); // consume '('
      const expr = this.parse();
      if (this.match('rparen')) {
        this.consume(); // consume ')'
      }
      return expr;
    }

    throw new Error(`Unexpected token: ${tok.type} (${tok.value})`);
  }
}

/**
 * Parse an expression string into an AST
 */
export function parseArithmeticExpression(expr: string): ExprNode {
  const tokens = tokenize(expr);
  if (tokens.length === 0) {
    return { type: 'null' };
  }
  const parser = new ExpressionParser(tokens);
  return parser.parse();
}

/**
 * Evaluate an expression AST against a row of data
 */
export function evaluateExpression(node: ExprNode, row: Record<string, SqlValue>): SqlValue {
  switch (node.type) {
    case 'number':
      return node.value;

    case 'string':
      return node.value;

    case 'null':
      return null;

    case 'column':
      return row[node.name] ?? null;

    case 'unary': {
      const operand = evaluateExpression(node.operand, row);

      // NULL propagation
      if (operand === null) {
        return null;
      }

      const numOperand = Number(operand);
      if (isNaN(numOperand)) {
        return null;
      }

      switch (node.op) {
        case '+':
          // Convert -0 to 0 for SQL compatibility
          return numOperand === 0 ? 0 : numOperand;
        case '-':
          // Convert -0 to 0 for SQL compatibility
          const negated = -numOperand;
          return negated === 0 ? 0 : negated;
      }
      break;
    }

    case 'binary': {
      const left = evaluateExpression(node.left, row);
      const right = evaluateExpression(node.right, row);

      // NULL propagation: any arithmetic with NULL yields NULL
      if (left === null || right === null) {
        return null;
      }

      const numLeft = Number(left);
      const numRight = Number(right);

      // Check for valid numbers
      if (isNaN(numLeft) || isNaN(numRight)) {
        return null;
      }

      switch (node.op) {
        case '+':
          return numLeft + numRight;
        case '-':
          return numLeft - numRight;
        case '*':
          return numLeft * numRight;
        case '/':
          // Division by zero returns NULL (SQLite behavior)
          if (numRight === 0) {
            return null;
          }
          // Integer division if both operands are integers
          if (Number.isInteger(numLeft) && Number.isInteger(numRight)) {
            return Math.trunc(numLeft / numRight);
          }
          return numLeft / numRight;
        case '%':
          // Modulo by zero returns NULL
          if (numRight === 0) {
            return null;
          }
          return numLeft % numRight;
      }
      break;
    }

    case 'function': {
      const args = node.args.map(arg => evaluateExpression(arg, row));

      switch (node.name) {
        case 'abs': {
          const arg = args[0];
          if (arg === null) return null;
          const num = Number(arg);
          if (isNaN(num)) return null;
          return Math.abs(num);
        }
        // Add more functions as needed
        default:
          return null;
      }
    }
  }
  return null;
}

/**
 * Check if an expression contains arithmetic operators or function calls
 */
export function isArithmeticExpression(expr: string): boolean {
  // Check for arithmetic operators or function calls like abs(...)
  return /[+\-*/%]/.test(expr) || /\w+\s*\(/.test(expr);
}

/**
 * Parse SELECT column list, handling expressions, aliases, and CASE...END blocks
 */
export function parseSelectColumns(columnList: string): Array<{ expr: string; alias: string | null }> {
  const result: Array<{ expr: string; alias: string | null }> = [];

  // Split by comma, but respect parentheses and CASE...END blocks
  const columns: string[] = [];
  let current = '';
  let parenDepth = 0;
  let caseDepth = 0;
  let inString = false;

  for (let i = 0; i < columnList.length; i++) {
    const ch = columnList[i];
    const remaining = columnList.slice(i);
    const upperRemaining = remaining.toUpperCase();

    // Track string literals
    if (ch === "'" && !inString) {
      inString = true;
      current += ch;
      continue;
    }
    if (ch === "'" && inString) {
      // Check for escaped quote
      if (columnList[i + 1] === "'") {
        current += "''";
        i++;
        continue;
      }
      inString = false;
      current += ch;
      continue;
    }

    if (inString) {
      current += ch;
      continue;
    }

    // Track CASE...END blocks
    if (upperRemaining.match(/^CASE\b/i)) {
      caseDepth++;
      current += remaining.slice(0, 4);
      i += 3;
      continue;
    }
    if (upperRemaining.match(/^END\b/i)) {
      caseDepth--;
      current += remaining.slice(0, 3);
      i += 2;
      continue;
    }

    // Track parentheses
    if (ch === '(') {
      parenDepth++;
      current += ch;
    } else if (ch === ')') {
      parenDepth--;
      current += ch;
    } else if (ch === ',' && parenDepth === 0 && caseDepth === 0) {
      columns.push(current.trim());
      current = '';
    } else {
      current += ch;
    }
  }
  if (current.trim()) {
    columns.push(current.trim());
  }

  for (const col of columns) {
    // Check for AS alias (case insensitive)
    // Need to find AS that's not inside a CASE expression
    const aliasMatch = findAliasOutsideCase(col);
    if (aliasMatch) {
      result.push({ expr: aliasMatch.expr.trim(), alias: aliasMatch.alias });
    } else {
      result.push({ expr: col, alias: null });
    }
  }

  return result;
}

/**
 * Find AS alias that's outside CASE expressions
 */
function findAliasOutsideCase(col: string): { expr: string; alias: string } | null {
  // Search for " AS identifier" pattern from the end, ignoring content inside CASE...END
  let caseDepth = 0;
  let inString = false;

  // First, verify there's no open CASE at the end
  for (let i = 0; i < col.length; i++) {
    const ch = col[i];
    const remaining = col.slice(i);
    const upperRemaining = remaining.toUpperCase();

    if (ch === "'" && !inString) { inString = true; continue; }
    if (ch === "'" && inString) {
      if (col[i + 1] === "'") { i++; continue; }
      inString = false;
      continue;
    }
    if (inString) continue;

    if (upperRemaining.match(/^CASE\b/i)) { caseDepth++; i += 3; continue; }
    if (upperRemaining.match(/^END\b/i)) { caseDepth--; i += 2; continue; }
  }

  // If we're inside a CASE, don't try to parse AS
  if (caseDepth > 0) {
    return null;
  }

  // Look for AS alias at the end (outside of CASE)
  const asMatch = col.match(/^(.+?)\s+AS\s+(\w+)$/i);
  if (asMatch) {
    // Verify the AS is not inside a CASE by checking that the expr part has balanced CASE/END
    const expr = asMatch[1];
    let checkDepth = 0;
    let checkInString = false;
    for (let i = 0; i < expr.length; i++) {
      const ch = expr[i];
      const remaining = expr.slice(i);
      const upperRemaining = remaining.toUpperCase();

      if (ch === "'" && !checkInString) { checkInString = true; continue; }
      if (ch === "'" && checkInString) {
        if (expr[i + 1] === "'") { i++; continue; }
        checkInString = false;
        continue;
      }
      if (checkInString) continue;

      if (upperRemaining.match(/^CASE\b/i)) { checkDepth++; i += 3; continue; }
      if (upperRemaining.match(/^END\b/i)) { checkDepth--; i += 2; continue; }
    }

    if (checkDepth === 0) {
      return { expr: asMatch[1], alias: asMatch[2] };
    }
  }

  return null;
}
