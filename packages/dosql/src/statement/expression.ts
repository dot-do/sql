/**
 * Expression Parser and Evaluator for SQL Arithmetic Expressions
 *
 * Provides parsing and evaluation of SQL arithmetic expressions with:
 * - Basic operators: +, -, *, /, %
 * - Operator precedence (* and / before + and -)
 * - Parentheses for grouping
 * - Unary operators (-, +)
 * - NULL propagation
 * - Built-in functions (abs, round, floor, ceil, etc.)
 */

import type { SqlValue } from './types.js';

/**
 * Token types for the expression lexer
 */
type ExprTokenType =
  | 'NUMBER'
  | 'IDENTIFIER'
  | 'STRING'
  | 'PLUS'
  | 'MINUS'
  | 'STAR'
  | 'SLASH'
  | 'PERCENT'
  | 'LPAREN'
  | 'RPAREN'
  | 'COMMA'
  | 'EOF';

interface ExprToken {
  type: ExprTokenType;
  value: string;
  position: number;
}

/**
 * Tokenize an expression string into tokens
 */
function tokenizeExpression(expr: string): ExprToken[] {
  const tokens: ExprToken[] = [];
  let pos = 0;

  while (pos < expr.length) {
    // Skip whitespace
    if (/\s/.test(expr[pos])) {
      pos++;
      continue;
    }

    const startPos = pos;
    const char = expr[pos];

    // Single character operators
    if (char === '+') {
      tokens.push({ type: 'PLUS', value: '+', position: startPos });
      pos++;
    } else if (char === '-') {
      tokens.push({ type: 'MINUS', value: '-', position: startPos });
      pos++;
    } else if (char === '*') {
      tokens.push({ type: 'STAR', value: '*', position: startPos });
      pos++;
    } else if (char === '/') {
      tokens.push({ type: 'SLASH', value: '/', position: startPos });
      pos++;
    } else if (char === '%') {
      tokens.push({ type: 'PERCENT', value: '%', position: startPos });
      pos++;
    } else if (char === '(') {
      tokens.push({ type: 'LPAREN', value: '(', position: startPos });
      pos++;
    } else if (char === ')') {
      tokens.push({ type: 'RPAREN', value: ')', position: startPos });
      pos++;
    } else if (char === ',') {
      tokens.push({ type: 'COMMA', value: ',', position: startPos });
      pos++;
    } else if (char === "'" || char === '"') {
      // String literal
      const quote = char;
      pos++;
      let value = '';
      while (pos < expr.length && expr[pos] !== quote) {
        if (expr[pos] === '\\' && pos + 1 < expr.length) {
          pos++;
          value += expr[pos];
        } else {
          value += expr[pos];
        }
        pos++;
      }
      pos++; // Skip closing quote
      tokens.push({ type: 'STRING', value, position: startPos });
    } else if (/\d/.test(char) || (char === '.' && pos + 1 < expr.length && /\d/.test(expr[pos + 1]))) {
      // Number
      let numStr = '';
      while (pos < expr.length && (/\d/.test(expr[pos]) || expr[pos] === '.')) {
        numStr += expr[pos];
        pos++;
      }
      tokens.push({ type: 'NUMBER', value: numStr, position: startPos });
    } else if (/[a-zA-Z_]/.test(char)) {
      // Identifier
      let ident = '';
      while (pos < expr.length && /[a-zA-Z0-9_]/.test(expr[pos])) {
        ident += expr[pos];
        pos++;
      }
      tokens.push({ type: 'IDENTIFIER', value: ident, position: startPos });
    } else {
      // Unknown character - skip
      pos++;
    }
  }

  tokens.push({ type: 'EOF', value: '', position: pos });
  return tokens;
}

/**
 * Expression AST node types
 */
export type ExprNode =
  | { type: 'number'; value: number }
  | { type: 'string'; value: string }
  | { type: 'column'; name: string }
  | { type: 'null' }
  | { type: 'unary'; operator: '+' | '-'; operand: ExprNode }
  | { type: 'binary'; operator: '+' | '-' | '*' | '/' | '%'; left: ExprNode; right: ExprNode }
  | { type: 'function'; name: string; args: ExprNode[] };

/**
 * Parse an expression into an AST
 * Uses recursive descent parsing with proper operator precedence
 */
export function parseExpressionToAST(expr: string): ExprNode {
  const tokens = tokenizeExpression(expr);
  let current = 0;

  function peek(): ExprToken {
    return tokens[current];
  }

  function consume(): ExprToken {
    return tokens[current++];
  }

  function match(type: ExprTokenType): boolean {
    return peek().type === type;
  }

  // Grammar:
  // expression     -> additive
  // additive       -> multiplicative (('+' | '-') multiplicative)*
  // multiplicative -> unary (('*' | '/' | '%') unary)*
  // unary          -> ('+' | '-') unary | primary
  // primary        -> NUMBER | STRING | identifier | function_call | '(' expression ')'

  function parseExpr(): ExprNode {
    return parseAdditive();
  }

  function parseAdditive(): ExprNode {
    let left = parseMultiplicative();

    while (match('PLUS') || match('MINUS')) {
      const op = consume().value as '+' | '-';
      const right = parseMultiplicative();
      left = { type: 'binary', operator: op, left, right };
    }

    return left;
  }

  function parseMultiplicative(): ExprNode {
    let left = parseUnary();

    while (match('STAR') || match('SLASH') || match('PERCENT')) {
      const op = consume().value as '*' | '/' | '%';
      const right = parseUnary();
      left = { type: 'binary', operator: op, left, right };
    }

    return left;
  }

  function parseUnary(): ExprNode {
    if (match('PLUS') || match('MINUS')) {
      const op = consume().value as '+' | '-';
      const operand = parseUnary();
      return { type: 'unary', operator: op, operand };
    }
    return parsePrimary();
  }

  function parsePrimary(): ExprNode {
    const token = peek();

    if (token.type === 'NUMBER') {
      consume();
      const num = parseFloat(token.value);
      return { type: 'number', value: num };
    }

    if (token.type === 'STRING') {
      consume();
      return { type: 'string', value: token.value };
    }

    if (token.type === 'IDENTIFIER') {
      consume();
      const name = token.value;

      // Check for NULL
      if (name.toUpperCase() === 'NULL') {
        return { type: 'null' };
      }

      // Check for function call
      if (match('LPAREN')) {
        consume(); // consume '('
        const args: ExprNode[] = [];
        if (!match('RPAREN')) {
          args.push(parseExpr());
          while (match('COMMA')) {
            consume();
            args.push(parseExpr());
          }
        }
        if (match('RPAREN')) {
          consume(); // consume ')'
        }
        return { type: 'function', name: name.toLowerCase(), args };
      }

      // Column reference
      return { type: 'column', name };
    }

    if (token.type === 'LPAREN') {
      consume(); // consume '('
      const innerExpr = parseExpr();
      if (match('RPAREN')) {
        consume(); // consume ')'
      }
      return innerExpr;
    }

    // Fallback: return null node for unexpected token
    return { type: 'null' };
  }

  return parseExpr();
}

/**
 * Evaluate an expression AST against a row
 */
export function evaluateExpressionAST(
  node: ExprNode,
  row: Record<string, SqlValue>,
  functions?: Map<string, (...args: SqlValue[]) => SqlValue>
): SqlValue {
  switch (node.type) {
    case 'number':
      return node.value;

    case 'string':
      return node.value;

    case 'null':
      return null;

    case 'column': {
      const val = row[node.name];
      return val !== undefined ? val : null;
    }

    case 'unary': {
      const operand = evaluateExpressionAST(node.operand, row, functions);
      if (operand === null) return null;
      const num = Number(operand);
      if (node.operator === '-') {
        // Handle -0 case: SQL treats -0 as 0
        const result = -num;
        return Object.is(result, -0) ? 0 : result;
      }
      return num; // unary +
    }

    case 'binary': {
      const left = evaluateExpressionAST(node.left, row, functions);
      const right = evaluateExpressionAST(node.right, row, functions);

      // NULL propagation: any arithmetic with NULL yields NULL
      if (left === null || right === null) return null;

      const leftNum = Number(left);
      const rightNum = Number(right);

      switch (node.operator) {
        case '+':
          return leftNum + rightNum;
        case '-':
          return leftNum - rightNum;
        case '*':
          return leftNum * rightNum;
        case '/':
          // Division by zero returns NULL
          if (rightNum === 0) return null;
          // Integer division when both are integers
          if (Number.isInteger(leftNum) && Number.isInteger(rightNum)) {
            return Math.trunc(leftNum / rightNum);
          }
          return leftNum / rightNum;
        case '%':
          // Modulo by zero returns NULL
          if (rightNum === 0) return null;
          return leftNum % rightNum;
        default:
          return null;
      }
    }

    case 'function': {
      const args = node.args.map(arg => evaluateExpressionAST(arg, row, functions));

      // Built-in functions
      switch (node.name) {
        case 'abs': {
          const val = args[0];
          if (val === null) return null;
          return Math.abs(Number(val));
        }
        case 'round': {
          const val = args[0];
          if (val === null) return null;
          const precision = args.length > 1 && args[1] !== null ? Number(args[1]) : 0;
          const factor = Math.pow(10, precision);
          return Math.round(Number(val) * factor) / factor;
        }
        case 'floor': {
          const val = args[0];
          if (val === null) return null;
          return Math.floor(Number(val));
        }
        case 'ceil': {
          const val = args[0];
          if (val === null) return null;
          return Math.ceil(Number(val));
        }
        case 'min': {
          const nums = args.filter(a => a !== null).map(Number);
          if (nums.length === 0) return null;
          return Math.min(...nums);
        }
        case 'max': {
          const nums = args.filter(a => a !== null).map(Number);
          if (nums.length === 0) return null;
          return Math.max(...nums);
        }
        case 'coalesce': {
          for (const arg of args) {
            if (arg !== null) return arg;
          }
          return null;
        }
        case 'nullif': {
          if (args.length >= 2 && args[0] === args[1]) {
            return null;
          }
          return args[0] ?? null;
        }
        case 'length': {
          const val = args[0];
          if (val === null) return null;
          return String(val).length;
        }
        case 'upper': {
          const val = args[0];
          if (val === null) return null;
          return String(val).toUpperCase();
        }
        case 'lower': {
          const val = args[0];
          if (val === null) return null;
          return String(val).toLowerCase();
        }
        case 'trim': {
          const val = args[0];
          if (val === null) return null;
          return String(val).trim();
        }
        case 'substr':
        case 'substring': {
          const str = args[0];
          if (str === null) return null;
          const start = args.length > 1 && args[1] !== null ? Number(args[1]) - 1 : 0; // SQL is 1-indexed
          const len = args.length > 2 && args[2] !== null ? Number(args[2]) : undefined;
          return String(str).substr(start, len);
        }
        default: {
          // Check user-defined functions
          if (functions && functions.has(node.name)) {
            const fn = functions.get(node.name)!;
            return fn(...args);
          }
          return null;
        }
      }
    }

    default:
      return null;
  }
}

/**
 * Check if a string is a simple column reference (no arithmetic)
 */
export function isSimpleColumnRef(expr: string): boolean {
  return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(expr.trim());
}

/**
 * Generate the output column name for an expression
 * Removes extra whitespace but preserves spacing around operators for readability
 */
export function getExpressionColumnName(expr: string): string {
  // For simple column refs, return as-is (trimmed)
  if (isSimpleColumnRef(expr)) {
    return expr.trim();
  }
  // For expressions, remove all internal whitespace to match SQL standard behavior
  // e.g., "a + b" becomes "a+b"
  return expr.trim().replace(/\s+/g, '');
}

/**
 * Parse SELECT column list, properly handling parentheses and commas
 */
export function parseSelectColumns(columnList: string): string[] {
  const columns: string[] = [];
  let current = '';
  let parenDepth = 0;

  for (let i = 0; i < columnList.length; i++) {
    const char = columnList[i];

    if (char === '(') {
      parenDepth++;
      current += char;
    } else if (char === ')') {
      parenDepth--;
      current += char;
    } else if (char === ',' && parenDepth === 0) {
      columns.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }

  // Push the last column
  if (current.trim()) {
    columns.push(current.trim());
  }

  return columns;
}
