/**
 * DoSQL DML (Data Manipulation Language) Parser
 *
 * Parses INSERT, UPDATE, DELETE, and REPLACE statements.
 * Supports SQLite-compatible syntax including:
 * - INSERT INTO table (cols) VALUES (...)
 * - INSERT INTO table SELECT ...
 * - UPDATE table SET col=val WHERE ...
 * - DELETE FROM table WHERE ...
 * - REPLACE INTO (SQLite extension)
 * - INSERT OR REPLACE/IGNORE/ABORT
 */

import {
  calculateLocation,
  getSuggestionForTypo,
} from './shared/errors.js';

import type {
  DMLStatement,
  InsertStatement,
  UpdateStatement,
  DeleteStatement,
  ReplaceStatement,
  Expression,
  LiteralExpression,
  ColumnReference,
  FunctionCall,
  BinaryExpression,
  UnaryExpression,
  NullExpression,
  DefaultExpression,
  ParameterExpression,
  SubqueryExpression,
  BinaryOperator,
  UnaryOperator,
  WhereClause,
  SetClause,
  ConflictClause,
  ConflictAction,
  OnConflictClause,
  ConflictTarget,
  OnConflictAction,
  ValuesList,
  ValuesRow,
  InsertSource,
  InsertSelect,
  InsertDefault,
  OrderByClause,
  OrderByItem,
  LimitClause,
  ReturningClause,
  ReturningColumn,
  FromClause,
  TableReference,
  IndexHint,
  ParseResult,
  ParseSuccess,
  ParseError,
} from './dml-types.js';

// =============================================================================
// PARSER STATE
// =============================================================================

/**
 * Parser state tracks position in input
 */
interface ParserState {
  input: string;
  position: number;
}

/**
 * Create initial parser state
 */
function createState(input: string): ParserState {
  return { input: input.trim(), position: 0 };
}

/**
 * Create an enhanced parse error with location information
 */
function createParseError(
  error: string,
  position: number,
  input: string,
  options?: { token?: string; expected?: string }
): ParseError {
  const location = calculateLocation(input, position);
  const suggestion = options?.token ? getSuggestionForTypo(options.token) : undefined;

  return {
    success: false,
    error,
    position,
    input,
    line: location.line,
    column: location.column,
    token: options?.token,
    expected: options?.expected,
    suggestion,
  };
}

/**
 * Get remaining input from current position
 */
function remaining(state: ParserState): string {
  return state.input.slice(state.position);
}

/**
 * Check if we've reached end of input
 */
function isEOF(state: ParserState): boolean {
  return state.position >= state.input.length;
}

/**
 * Advance position and return new state
 */
function advance(state: ParserState, count: number): ParserState {
  return { ...state, position: state.position + count };
}

// =============================================================================
// BASIC PARSING UTILITIES
// =============================================================================

/**
 * Skip whitespace
 */
function skipWhitespace(state: ParserState): ParserState {
  const rest = remaining(state);
  const match = rest.match(/^\s+/);
  if (match) {
    return advance(state, match[0].length);
  }
  return state;
}

/**
 * Try to match a keyword (case-insensitive)
 */
function matchKeyword(state: ParserState, keyword: string): ParserState | null {
  const rest = remaining(state);
  // Match keyword followed by non-alphanumeric (word boundary) or end of string
  const regex = new RegExp(`^${keyword}(?=[\\s;,()\\]|$])`, 'i');
  if (regex.test(rest)) {
    return advance(state, keyword.length);
  }
  // Also match if exactly at end of input
  if (rest.toUpperCase() === keyword.toUpperCase()) {
    return advance(state, keyword.length);
  }
  return null;
}

/**
 * Try to match exact string
 */
function matchExact(state: ParserState, str: string): ParserState | null {
  const rest = remaining(state);
  if (rest.startsWith(str)) {
    return advance(state, str.length);
  }
  return null;
}

/**
 * Parse an identifier (table name, column name)
 */
function parseIdentifier(state: ParserState): { state: ParserState; value: string } | null {
  state = skipWhitespace(state);
  const rest = remaining(state);

  // Handle quoted identifiers
  if (rest.startsWith('"') || rest.startsWith('`') || rest.startsWith('[')) {
    const quote = rest[0];
    const endQuote = quote === '[' ? ']' : quote;
    const endIndex = rest.indexOf(endQuote, 1);
    if (endIndex === -1) return null;
    const value = rest.slice(1, endIndex);
    return { state: advance(state, endIndex + 1), value };
  }

  // Unquoted identifier
  const match = rest.match(/^[a-zA-Z_][a-zA-Z0-9_]*/);
  if (match) {
    return { state: advance(state, match[0].length), value: match[0] };
  }
  return null;
}

/**
 * Parse a comma-separated list of identifiers
 */
function parseIdentifierList(state: ParserState): { state: ParserState; values: string[] } | null {
  const values: string[] = [];

  // Parse first identifier
  const first = parseIdentifier(state);
  if (!first) return null;
  values.push(first.value);
  state = first.state;

  // Parse additional identifiers
  while (true) {
    state = skipWhitespace(state);
    if (!matchExact(state, ',')) break;
    state = advance(state, 1);
    state = skipWhitespace(state);

    const next = parseIdentifier(state);
    if (!next) return null;
    values.push(next.value);
    state = next.state;
  }

  return { state, values };
}

// =============================================================================
// EXPRESSION PARSING
// =============================================================================

/**
 * Parse a string literal
 */
function parseStringLiteral(state: ParserState): { state: ParserState; expr: LiteralExpression } | null {
  const rest = remaining(state);
  const quote = rest[0];

  if (quote !== "'" && quote !== '"') return null;

  let i = 1;
  let value = '';
  while (i < rest.length) {
    if (rest[i] === quote) {
      // Check for escaped quote
      if (rest[i + 1] === quote) {
        value += quote;
        i += 2;
      } else {
        return {
          state: advance(state, i + 1),
          expr: { type: 'literal', value, raw: rest.slice(0, i + 1) },
        };
      }
    } else if (rest[i] === '\\' && rest[i + 1]) {
      // Handle escape sequences
      const escaped = rest[i + 1];
      switch (escaped) {
        case 'n': value += '\n'; break;
        case 't': value += '\t'; break;
        case 'r': value += '\r'; break;
        default: value += escaped;
      }
      i += 2;
    } else {
      value += rest[i];
      i++;
    }
  }
  return null; // Unterminated string
}

/**
 * Parse a numeric literal
 */
function parseNumericLiteral(state: ParserState): { state: ParserState; expr: LiteralExpression } | null {
  const rest = remaining(state);
  const match = rest.match(/^-?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?/);
  if (match) {
    const raw = match[0];
    const value = raw.includes('.') || raw.toLowerCase().includes('e')
      ? parseFloat(raw)
      : parseInt(raw, 10);
    return {
      state: advance(state, raw.length),
      expr: { type: 'literal', value, raw },
    };
  }
  return null;
}

/**
 * Parse NULL literal
 */
function parseNullLiteral(state: ParserState): { state: ParserState; expr: NullExpression } | null {
  const matched = matchKeyword(state, 'NULL');
  if (matched) {
    return { state: matched, expr: { type: 'null' } };
  }
  return null;
}

/**
 * Parse DEFAULT keyword
 */
function parseDefaultKeyword(state: ParserState): { state: ParserState; expr: DefaultExpression } | null {
  const matched = matchKeyword(state, 'DEFAULT');
  if (matched) {
    return { state: matched, expr: { type: 'default' } };
  }
  return null;
}

/**
 * Parse boolean literal
 */
function parseBooleanLiteral(state: ParserState): { state: ParserState; expr: LiteralExpression } | null {
  let matched = matchKeyword(state, 'TRUE');
  if (matched) {
    return { state: matched, expr: { type: 'literal', value: true, raw: 'TRUE' } };
  }
  matched = matchKeyword(state, 'FALSE');
  if (matched) {
    return { state: matched, expr: { type: 'literal', value: false, raw: 'FALSE' } };
  }
  return null;
}

/**
 * Parse parameter placeholder (?, :name, $n)
 */
function parseParameter(state: ParserState): { state: ParserState; expr: ParameterExpression } | null {
  const rest = remaining(state);

  // Positional parameter (?)
  if (rest.startsWith('?')) {
    return {
      state: advance(state, 1),
      expr: { type: 'parameter', name: state.position, raw: '?' },
    };
  }

  // Named parameter (:name or $name)
  const match = rest.match(/^[:$]([a-zA-Z_][a-zA-Z0-9_]*)/);
  if (match) {
    return {
      state: advance(state, match[0].length),
      expr: { type: 'parameter', name: match[1], raw: match[0] },
    };
  }

  // Numbered parameter ($1, $2, etc.)
  const numMatch = rest.match(/^\$(\d+)/);
  if (numMatch) {
    return {
      state: advance(state, numMatch[0].length),
      expr: { type: 'parameter', name: parseInt(numMatch[1], 10), raw: numMatch[0] },
    };
  }

  return null;
}

/**
 * Parse a subquery (SELECT in parentheses)
 */
function parseSubquery(state: ParserState): { state: ParserState; expr: SubqueryExpression } | null {
  state = skipWhitespace(state);
  let rest = remaining(state);

  if (!rest.startsWith('(')) return null;

  // Check if it starts with SELECT
  const afterParen = rest.slice(1).trim();
  if (!afterParen.toUpperCase().startsWith('SELECT')) return null;

  // Find matching closing paren
  let depth = 1;
  let i = 1;
  while (i < rest.length && depth > 0) {
    if (rest[i] === '(') depth++;
    if (rest[i] === ')') depth--;
    i++;
  }

  if (depth !== 0) return null;

  const query = rest.slice(1, i - 1).trim();
  return {
    state: advance(state, i),
    expr: { type: 'subquery', query },
  };
}

/**
 * Parse a qualified identifier (table name or schema.table name)
 * Returns the full qualified name as a string
 */
function parseQualifiedIdentifier(state: ParserState): { state: ParserState; value: string } | null {
  const first = parseIdentifier(state);
  if (!first) return null;

  let value = first.value;
  let newState = first.state;

  // Check for schema.table format
  let rest = remaining(newState);
  if (rest.startsWith('.')) {
    newState = advance(newState, 1);
    const second = parseIdentifier(newState);
    if (second) {
      value = value + '.' + second.value;
      newState = second.state;
    }
  }

  return { state: newState, value };
}

/**
 * Parse column reference (possibly qualified)
 * Supports: column, table.column, schema.table.column
 */
function parseColumnReference(state: ParserState): { state: ParserState; expr: ColumnReference } | null {
  const first = parseIdentifier(state);
  if (!first) return null;

  let newState = skipWhitespace(first.state);
  let rest = remaining(newState);

  // Check for qualified name (table.column or schema.table.column)
  if (rest.startsWith('.')) {
    newState = advance(newState, 1);
    const second = parseIdentifier(newState);
    if (second) {
      newState = skipWhitespace(second.state);
      rest = remaining(newState);

      // Check for third part (schema.table.column)
      if (rest.startsWith('.')) {
        newState = advance(newState, 1);
        const third = parseIdentifier(newState);
        if (third) {
          return {
            state: third.state,
            expr: { type: 'column', name: third.value, table: second.value, schema: first.value },
          };
        }
      }

      return {
        state: second.state,
        expr: { type: 'column', name: second.value, table: first.value },
      };
    }
  }

  return {
    state: first.state,
    expr: { type: 'column', name: first.value },
  };
}

/**
 * Parse function call
 */
function parseFunctionCall(state: ParserState): { state: ParserState; expr: FunctionCall } | null {
  const identResult = parseIdentifier(state);
  if (!identResult) return null;

  let newState = skipWhitespace(identResult.state);
  const rest = remaining(newState);

  if (!rest.startsWith('(')) return null;
  newState = advance(newState, 1);
  newState = skipWhitespace(newState);

  const args: Expression[] = [];
  let distinct = false;

  // Check for DISTINCT
  const distinctMatch = matchKeyword(newState, 'DISTINCT');
  if (distinctMatch) {
    distinct = true;
    newState = skipWhitespace(distinctMatch);
  }

  // Check for * (e.g., COUNT(*))
  let checkStar = remaining(newState);
  if (checkStar.startsWith('*')) {
    newState = advance(newState, 1);
    args.push({ type: 'column', name: '*' } as ColumnReference);
  } else {
    // Parse argument list
    if (!remaining(newState).startsWith(')')) {
      const firstArg = parseExpression(newState);
      if (firstArg) {
        args.push(firstArg.expr);
        newState = firstArg.state;

        while (true) {
          newState = skipWhitespace(newState);
          if (!remaining(newState).startsWith(',')) break;
          newState = advance(newState, 1);
          newState = skipWhitespace(newState);
          const nextArg = parseExpression(newState);
          if (!nextArg) break;
          args.push(nextArg.expr);
          newState = nextArg.state;
        }
      }
    }
  }

  newState = skipWhitespace(newState);
  if (!remaining(newState).startsWith(')')) return null;
  newState = advance(newState, 1);

  // Check for OVER clause (window function)
  let hasOver = false;
  newState = skipWhitespace(newState);
  const overMatch = matchKeyword(newState, 'OVER');
  if (overMatch) {
    hasOver = true;
    newState = skipWhitespace(overMatch);
    // Consume the OVER clause content (parentheses with partition/order specifications)
    if (remaining(newState).startsWith('(')) {
      let depth = 1;
      let i = 1;
      const rest = remaining(newState);
      while (i < rest.length && depth > 0) {
        if (rest[i] === '(') depth++;
        if (rest[i] === ')') depth--;
        i++;
      }
      newState = advance(newState, i);
    }
  }

  const funcExpr: FunctionCall = { type: 'function', name: identResult.value.toUpperCase(), args, distinct };
  if (hasOver) {
    funcExpr.hasOver = true;
  }
  return {
    state: newState,
    expr: funcExpr,
  };
}

/**
 * Parse parenthesized expression
 */
function parseParenthesized(state: ParserState): { state: ParserState; expr: Expression } | null {
  const rest = remaining(state);
  if (!rest.startsWith('(')) return null;

  // Try subquery first
  const subquery = parseSubquery(state);
  if (subquery) return subquery;

  // Parse regular expression
  let newState = advance(state, 1);
  newState = skipWhitespace(newState);

  const inner = parseExpression(newState);
  if (!inner) return null;

  newState = skipWhitespace(inner.state);
  if (!remaining(newState).startsWith(')')) return null;
  newState = advance(newState, 1);

  return { state: newState, expr: inner.expr };
}

/**
 * Parse primary expression (literals, identifiers, function calls, etc.)
 */
function parsePrimaryExpression(state: ParserState): { state: ParserState; expr: Expression } | null {
  state = skipWhitespace(state);

  // Try each type in order
  const nullResult = parseNullLiteral(state);
  if (nullResult) return nullResult;

  const defaultResult = parseDefaultKeyword(state);
  if (defaultResult) return defaultResult;

  const boolResult = parseBooleanLiteral(state);
  if (boolResult) return boolResult;

  const numResult = parseNumericLiteral(state);
  if (numResult) return numResult;

  const strResult = parseStringLiteral(state);
  if (strResult) return strResult;

  const paramResult = parseParameter(state);
  if (paramResult) return paramResult;

  const parenResult = parseParenthesized(state);
  if (parenResult) return parenResult;

  // Try function call before column reference
  const funcResult = parseFunctionCall(state);
  if (funcResult) return funcResult;

  const colResult = parseColumnReference(state);
  if (colResult) return colResult;

  return null;
}

/**
 * Parse unary expression (NOT, -, +, ~)
 */
function parseUnaryExpression(state: ParserState): { state: ParserState; expr: Expression } | null {
  state = skipWhitespace(state);
  const rest = remaining(state);

  let op: UnaryOperator | null = null;
  let newState = state;

  if (matchKeyword(state, 'NOT')) {
    op = 'NOT';
    newState = skipWhitespace(matchKeyword(state, 'NOT')!);
  } else if (rest.startsWith('-') && !rest.startsWith('--')) {
    op = '-';
    newState = advance(state, 1);
  } else if (rest.startsWith('+')) {
    op = '+';
    newState = advance(state, 1);
  } else if (rest.startsWith('~')) {
    op = '~';
    newState = advance(state, 1);
  }

  if (op) {
    const operand = parseUnaryExpression(newState);
    if (operand) {
      return {
        state: operand.state,
        expr: { type: 'unary', operator: op, operand: operand.expr } as UnaryExpression,
      };
    }
  }

  return parsePrimaryExpression(state);
}

/**
 * Operator precedence levels
 */
const PRECEDENCE: Record<string, number> = {
  'OR': 1,
  'AND': 2,
  'NOT': 3,
  '=': 4, '!=': 4, '<>': 4, '<': 4, '<=': 4, '>': 4, '>=': 4,
  'IS': 4, 'IS NOT': 4, 'LIKE': 4, 'NOT LIKE': 4, 'GLOB': 4, 'NOT GLOB': 4,
  'REGEXP': 4, 'NOT REGEXP': 4, 'IN': 4, 'NOT IN': 4,
  'BETWEEN': 4, 'NOT BETWEEN': 4,
  '|': 5,
  '&': 6,
  '<<': 7, '>>': 7,
  '+': 8, '-': 8,
  '*': 9, '/': 9, '%': 9,
  '||': 10,
};

/**
 * Parse binary operator
 */
function parseBinaryOperator(state: ParserState): { state: ParserState; op: BinaryOperator } | null {
  state = skipWhitespace(state);
  const rest = remaining(state);

  // Multi-character operators
  const multiOps: BinaryOperator[] = ['||', '<=', '>=', '<>', '!=', '<<', '>>'];
  for (const op of multiOps) {
    if (rest.startsWith(op)) {
      return { state: advance(state, op.length), op };
    }
  }

  // Keyword operators (need word boundary check)
  const keywordOps: BinaryOperator[] = [
    'IS NOT', 'NOT LIKE', 'NOT GLOB', 'NOT REGEXP', 'NOT IN', 'NOT BETWEEN',
    'IS', 'LIKE', 'GLOB', 'REGEXP', 'IN', 'BETWEEN', 'AND', 'OR',
  ];
  for (const op of keywordOps) {
    const matched = matchKeyword(state, op);
    if (matched) {
      return { state: matched, op };
    }
  }

  // Single character operators
  const singleOps: BinaryOperator[] = ['=', '<', '>', '+', '-', '*', '/', '%', '&', '|'];
  for (const op of singleOps) {
    if (rest.startsWith(op)) {
      return { state: advance(state, 1), op };
    }
  }

  return null;
}

/**
 * Parse expression with precedence climbing
 */
function parseExpressionWithPrecedence(
  state: ParserState,
  minPrecedence: number
): { state: ParserState; expr: Expression } | null {
  let left = parseUnaryExpression(state);
  if (!left) return null;

  while (true) {
    const savedState = left.state;
    const opResult = parseBinaryOperator(skipWhitespace(savedState));
    if (!opResult) break;

    const precedence = PRECEDENCE[opResult.op] ?? 0;
    if (precedence < minPrecedence) break;

    // Handle BETWEEN specially
    if (opResult.op === 'BETWEEN' || opResult.op === 'NOT BETWEEN') {
      let newState = skipWhitespace(opResult.state);
      const low = parseExpressionWithPrecedence(newState, precedence + 1);
      if (!low) break;

      newState = skipWhitespace(low.state);
      const andMatch = matchKeyword(newState, 'AND');
      if (!andMatch) break;

      newState = skipWhitespace(andMatch);
      const high = parseExpressionWithPrecedence(newState, precedence + 1);
      if (!high) break;

      // Create nested binary expression for BETWEEN
      left = {
        state: high.state,
        expr: {
          type: 'binary',
          operator: opResult.op,
          left: left.expr,
          right: {
            type: 'binary',
            operator: 'AND',
            left: low.expr,
            right: high.expr,
          } as BinaryExpression,
        } as BinaryExpression,
      };
      continue;
    }

    // Handle IN specially (can have list or subquery)
    if (opResult.op === 'IN' || opResult.op === 'NOT IN') {
      let newState = skipWhitespace(opResult.state);
      const rest = remaining(newState);

      if (rest.startsWith('(')) {
        // Find matching paren
        let depth = 1;
        let i = 1;
        while (i < rest.length && depth > 0) {
          if (rest[i] === '(') depth++;
          if (rest[i] === ')') depth--;
          i++;
        }
        if (depth === 0) {
          const listContent = rest.slice(1, i - 1);
          newState = advance(newState, i);
          left = {
            state: newState,
            expr: {
              type: 'binary',
              operator: opResult.op,
              left: left.expr,
              right: { type: 'subquery', query: listContent } as SubqueryExpression,
            } as BinaryExpression,
          };
          continue;
        }
      }
      break;
    }

    const right = parseExpressionWithPrecedence(opResult.state, precedence + 1);
    if (!right) break;

    left = {
      state: right.state,
      expr: {
        type: 'binary',
        operator: opResult.op,
        left: left.expr,
        right: right.expr,
      } as BinaryExpression,
    };
  }

  return left;
}

/**
 * Parse an expression
 */
function parseExpression(state: ParserState): { state: ParserState; expr: Expression } | null {
  return parseExpressionWithPrecedence(state, 0);
}

/**
 * Parse comma-separated list of expressions
 */
function parseExpressionList(state: ParserState): { state: ParserState; exprs: Expression[] } | null {
  const exprs: Expression[] = [];

  const first = parseExpression(state);
  if (!first) return null;
  exprs.push(first.expr);
  state = first.state;

  while (true) {
    state = skipWhitespace(state);
    if (!remaining(state).startsWith(',')) break;
    state = advance(state, 1);
    state = skipWhitespace(state);

    const next = parseExpression(state);
    if (!next) return null;
    exprs.push(next.expr);
    state = next.state;
  }

  return { state, exprs };
}

// =============================================================================
// CLAUSE PARSING
// =============================================================================

/**
 * Parse WHERE clause
 */
function parseWhereClause(state: ParserState): { state: ParserState; where: WhereClause } | null {
  state = skipWhitespace(state);
  const whereMatch = matchKeyword(state, 'WHERE');
  if (!whereMatch) return null;

  state = skipWhitespace(whereMatch);
  const condition = parseExpression(state);
  if (!condition) return null;

  return {
    state: condition.state,
    where: { type: 'where', condition: condition.expr },
  };
}

/**
 * Parse SET clause (column = value)
 * Handles both simple columns and qualified columns (table.column)
 */
function parseSetClause(state: ParserState): { state: ParserState; set: SetClause } | null {
  state = skipWhitespace(state);

  // Parse the first part of the column name
  const colResult = parseIdentifier(state);
  if (!colResult) return null;

  let columnName = colResult.value;
  state = colResult.state;

  // Check for qualified column name (table.column)
  const rest = remaining(state);
  if (rest.startsWith('.')) {
    state = advance(state, 1);
    const secondPart = parseIdentifier(state);
    if (secondPart) {
      // Use only the column name (after the dot), ignoring table/alias prefix
      columnName = secondPart.value;
      state = secondPart.state;
    }
  }

  state = skipWhitespace(state);
  if (!remaining(state).startsWith('=')) return null;
  state = advance(state, 1);
  state = skipWhitespace(state);

  const valueResult = parseExpression(state);
  if (!valueResult) return null;

  return {
    state: valueResult.state,
    set: { type: 'set', column: columnName, value: valueResult.expr },
  };
}

/**
 * Parse SET clauses list
 */
function parseSetClauses(state: ParserState): { state: ParserState; sets: SetClause[] } | null {
  const sets: SetClause[] = [];

  const first = parseSetClause(state);
  if (!first) return null;
  sets.push(first.set);
  state = first.state;

  while (true) {
    state = skipWhitespace(state);
    if (!remaining(state).startsWith(',')) break;
    state = advance(state, 1);
    state = skipWhitespace(state);

    const next = parseSetClause(state);
    if (!next) return null;
    sets.push(next.set);
    state = next.state;
  }

  return { state, sets };
}

/**
 * Parse ORDER BY clause
 */
function parseOrderByClause(state: ParserState): { state: ParserState; orderBy: OrderByClause } | null {
  state = skipWhitespace(state);
  let orderMatch = matchKeyword(state, 'ORDER');
  if (!orderMatch) return null;

  state = skipWhitespace(orderMatch);
  const byMatch = matchKeyword(state, 'BY');
  if (!byMatch) return null;

  state = skipWhitespace(byMatch);

  const items: OrderByItem[] = [];

  while (true) {
    state = skipWhitespace(state);
    const exprResult = parseExpression(state);
    if (!exprResult) break;

    let direction: 'ASC' | 'DESC' = 'ASC';
    let nulls: 'FIRST' | 'LAST' | undefined;

    state = skipWhitespace(exprResult.state);

    // Check for direction
    if (matchKeyword(state, 'DESC')) {
      direction = 'DESC';
      state = skipWhitespace(matchKeyword(state, 'DESC')!);
    } else if (matchKeyword(state, 'ASC')) {
      direction = 'ASC';
      state = skipWhitespace(matchKeyword(state, 'ASC')!);
    }

    // Check for NULLS FIRST/LAST
    if (matchKeyword(state, 'NULLS')) {
      state = skipWhitespace(matchKeyword(state, 'NULLS')!);
      if (matchKeyword(state, 'FIRST')) {
        nulls = 'FIRST';
        state = skipWhitespace(matchKeyword(state, 'FIRST')!);
      } else if (matchKeyword(state, 'LAST')) {
        nulls = 'LAST';
        state = skipWhitespace(matchKeyword(state, 'LAST')!);
      }
    }

    items.push({ expression: exprResult.expr, direction, nulls });

    // Check for comma
    if (!remaining(state).startsWith(',')) break;
    state = advance(state, 1);
  }

  if (items.length === 0) return null;

  return { state, orderBy: { type: 'order_by', items } };
}

/**
 * Parse LIMIT clause
 */
function parseLimitClause(state: ParserState): { state: ParserState; limit: LimitClause } | null {
  state = skipWhitespace(state);
  const limitMatch = matchKeyword(state, 'LIMIT');
  if (!limitMatch) return null;

  state = skipWhitespace(limitMatch);
  const countResult = parseExpression(state);
  if (!countResult) return null;

  let offset: Expression | undefined;
  state = skipWhitespace(countResult.state);

  // Check for OFFSET or comma
  if (matchKeyword(state, 'OFFSET')) {
    state = skipWhitespace(matchKeyword(state, 'OFFSET')!);
    const offsetResult = parseExpression(state);
    if (offsetResult) {
      offset = offsetResult.expr;
      state = offsetResult.state;
    }
  } else if (remaining(state).startsWith(',')) {
    state = advance(state, 1);
    state = skipWhitespace(state);
    const offsetResult = parseExpression(state);
    if (offsetResult) {
      offset = offsetResult.expr;
      state = offsetResult.state;
    }
  }

  return {
    state,
    limit: { type: 'limit', count: countResult.expr, offset },
  };
}

/**
 * Check if an expression contains a window function (OVER clause)
 */
function containsWindowFunction(expr: Expression): boolean {
  if (expr.type === 'function') {
    // Check if this function has an OVER clause (window function)
    if (expr.hasOver) {
      return true;
    }
    // Check arguments recursively
    for (const arg of expr.args) {
      if (containsWindowFunction(arg)) return true;
    }
  }
  if (expr.type === 'binary') {
    return containsWindowFunction(expr.left) || containsWindowFunction(expr.right);
  }
  if (expr.type === 'unary') {
    return containsWindowFunction(expr.operand);
  }
  return false;
}

/**
 * Check if an expression contains an aggregate function at the top level (not in subquery)
 */
function containsAggregateFunction(expr: Expression): boolean {
  if (expr.type === 'function') {
    const aggregateFuncs = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'GROUP_CONCAT', 'TOTAL'];
    if (aggregateFuncs.includes(expr.name.toUpperCase())) {
      return true;
    }
    // Check arguments recursively
    for (const arg of expr.args) {
      if (containsAggregateFunction(arg)) return true;
    }
  }
  if (expr.type === 'binary') {
    return containsAggregateFunction(expr.left) || containsAggregateFunction(expr.right);
  }
  if (expr.type === 'unary') {
    return containsAggregateFunction(expr.operand);
  }
  return false;
}

/**
 * Reserved keywords that cannot be implicit aliases
 */
const RESERVED_KEYWORDS = new Set([
  'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'LIKE', 'GLOB',
  'BETWEEN', 'EXISTS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'AS', 'ON', 'USING',
  'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'CROSS', 'NATURAL', 'GROUP', 'BY',
  'HAVING', 'ORDER', 'LIMIT', 'OFFSET', 'UNION', 'INTERSECT', 'EXCEPT', 'INSERT',
  'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TABLE', 'INDEX', 'VIEW',
  'RETURNING', 'SET', 'VALUES', 'DEFAULT', 'INTO', 'TRUE', 'FALSE',
]);

/**
 * RETURNING clause parse result - can be success, error, or not found
 */
type ReturningParseResult =
  | { type: 'success'; state: ParserState; returning: ReturningClause }
  | { type: 'error'; message: string; position: number }
  | { type: 'not_found' };

/**
 * Parse RETURNING clause
 */
function parseReturningClause(state: ParserState): ReturningParseResult {
  state = skipWhitespace(state);
  const returningMatch = matchKeyword(state, 'RETURNING');
  if (!returningMatch) return { type: 'not_found' };

  state = skipWhitespace(returningMatch);
  const startPosition = state.position;

  const columns: ReturningColumn[] = [];
  let requiresWildcardExpansion = false;

  while (true) {
    state = skipWhitespace(state);

    // Check for *
    if (remaining(state).startsWith('*')) {
      columns.push({ expression: '*' });
      requiresWildcardExpansion = true;
      state = advance(state, 1);
    } else {
      const exprResult = parseExpression(state);
      if (!exprResult) break;

      // Check for window functions - these are errors in RETURNING
      if (containsWindowFunction(exprResult.expr)) {
        return {
          type: 'error',
          message: 'Window functions are not allowed in RETURNING clause',
          position: startPosition,
        };
      }
      // Check for aggregate functions - these are errors in RETURNING
      if (containsAggregateFunction(exprResult.expr)) {
        return {
          type: 'error',
          message: 'Aggregate functions are not allowed in RETURNING clause',
          position: startPosition,
        };
      }

      let alias: string | undefined;
      state = skipWhitespace(exprResult.state);

      // Check for AS alias
      if (matchKeyword(state, 'AS')) {
        state = skipWhitespace(matchKeyword(state, 'AS')!);
        const aliasResult = parseIdentifier(state);
        if (aliasResult) {
          alias = aliasResult.value;
          state = aliasResult.state;
        }
      } else {
        // Check for implicit alias (identifier without AS keyword)
        // The implicit alias must not be a reserved keyword and must not start with punctuation
        const rest = remaining(state);
        if (!rest.startsWith(',') && !rest.startsWith(';') && rest.length > 0) {
          const aliasResult = parseIdentifier(state);
          if (aliasResult) {
            const upperAlias = aliasResult.value.toUpperCase();
            // Only use as implicit alias if not a reserved keyword
            if (!RESERVED_KEYWORDS.has(upperAlias)) {
              alias = aliasResult.value;
              state = aliasResult.state;
            }
          }
        }
      }

      columns.push({ expression: exprResult.expr, alias });
    }

    // Check for comma
    state = skipWhitespace(state);
    if (!remaining(state).startsWith(',')) break;
    state = advance(state, 1);
  }

  if (columns.length === 0) return { type: 'not_found' };

  const returning: ReturningClause = { type: 'returning', columns };
  if (requiresWildcardExpansion) {
    returning.requiresWildcardExpansion = true;
  }
  return { type: 'success', state, returning };
}

/**
 * Parse conflict clause (OR REPLACE, OR IGNORE, etc.)
 */
function parseConflictClause(state: ParserState): { state: ParserState; conflict: ConflictClause } | null {
  state = skipWhitespace(state);
  const orMatch = matchKeyword(state, 'OR');
  if (!orMatch) return null;

  state = skipWhitespace(orMatch);

  const actions: ConflictAction[] = ['ROLLBACK', 'ABORT', 'REPLACE', 'FAIL', 'IGNORE'];
  for (const action of actions) {
    const actionMatch = matchKeyword(state, action);
    if (actionMatch) {
      return {
        state: actionMatch,
        conflict: { type: 'conflict', action },
      };
    }
  }

  return null;
}

/**
 * Parse ON CONFLICT clause (UPSERT)
 */
function parseOnConflictClause(state: ParserState): { state: ParserState; onConflict: OnConflictClause } | null {
  state = skipWhitespace(state);
  const onMatch = matchKeyword(state, 'ON');
  if (!onMatch) return null;

  state = skipWhitespace(onMatch);
  const conflictMatch = matchKeyword(state, 'CONFLICT');
  if (!conflictMatch) return null;

  state = skipWhitespace(conflictMatch);

  let target: ConflictTarget | undefined;

  // Check for conflict target (columns in parentheses)
  if (remaining(state).startsWith('(')) {
    state = advance(state, 1);
    state = skipWhitespace(state);

    const colsResult = parseIdentifierList(state);
    if (colsResult) {
      target = { columns: colsResult.values };
      state = skipWhitespace(colsResult.state);

      // Check for WHERE clause on target
      const targetWhere = parseWhereClause(state);
      if (targetWhere) {
        target.where = targetWhere.where.condition;
        state = targetWhere.state;
      }

      state = skipWhitespace(state);
      if (!remaining(state).startsWith(')')) return null;
      state = advance(state, 1);
      state = skipWhitespace(state);
    }
  }

  // Parse DO NOTHING or DO UPDATE
  const doMatch = matchKeyword(state, 'DO');
  if (!doMatch) return null;

  state = skipWhitespace(doMatch);

  if (matchKeyword(state, 'NOTHING')) {
    return {
      state: matchKeyword(state, 'NOTHING')!,
      onConflict: {
        type: 'on_conflict',
        target,
        action: { type: 'do_nothing' },
      },
    };
  }

  if (matchKeyword(state, 'UPDATE')) {
    state = skipWhitespace(matchKeyword(state, 'UPDATE')!);

    const setMatch = matchKeyword(state, 'SET');
    if (!setMatch) return null;

    state = skipWhitespace(setMatch);
    const setsResult = parseSetClauses(state);
    if (!setsResult) return null;

    state = setsResult.state;

    // Check for WHERE clause
    let where: Expression | undefined;
    const whereResult = parseWhereClause(state);
    if (whereResult) {
      where = whereResult.where.condition;
      state = whereResult.state;
    }

    return {
      state,
      onConflict: {
        type: 'on_conflict',
        target,
        action: { type: 'do_update', set: setsResult.sets, where },
      },
    };
  }

  return null;
}

/**
 * Parse FROM clause (for UPDATE ... FROM)
 */
function parseFromClause(state: ParserState): { state: ParserState; from: FromClause } | null {
  state = skipWhitespace(state);
  const fromMatch = matchKeyword(state, 'FROM');
  if (!fromMatch) return null;

  state = skipWhitespace(fromMatch);

  const tables: TableReference[] = [];

  while (true) {
    const tableResult = parseIdentifier(state);
    if (!tableResult) break;

    let alias: string | undefined;
    state = skipWhitespace(tableResult.state);

    // Check for alias
    if (matchKeyword(state, 'AS')) {
      state = skipWhitespace(matchKeyword(state, 'AS')!);
      const aliasResult = parseIdentifier(state);
      if (aliasResult) {
        alias = aliasResult.value;
        state = aliasResult.state;
      }
    } else {
      // Check for alias without AS
      const rest = remaining(state);
      if (!rest.startsWith(',') && !rest.startsWith(';')) {
        const nextWord = parseIdentifier(state);
        if (nextWord) {
          const upper = nextWord.value.toUpperCase();
          if (!['SET', 'WHERE', 'ORDER', 'LIMIT', 'RETURNING'].includes(upper)) {
            alias = nextWord.value;
            state = nextWord.state;
          }
        }
      }
    }

    tables.push({ type: 'table_ref', name: tableResult.value, alias });

    state = skipWhitespace(state);
    if (!remaining(state).startsWith(',')) break;
    state = advance(state, 1);
    state = skipWhitespace(state);
  }

  if (tables.length === 0) return null;

  return { state, from: { type: 'from', tables } };
}

/**
 * Parse INDEXED BY or NOT INDEXED hint
 */
function parseIndexHint(state: ParserState): { state: ParserState; hint: IndexHint } | null {
  state = skipWhitespace(state);

  // Check for NOT INDEXED
  const notMatch = matchKeyword(state, 'NOT');
  if (notMatch) {
    let newState = skipWhitespace(notMatch);
    const indexedMatch = matchKeyword(newState, 'INDEXED');
    if (indexedMatch) {
      return {
        state: indexedMatch,
        hint: { type: 'not_indexed' },
      };
    }
  }

  // Check for INDEXED BY index_name
  const indexedMatch = matchKeyword(state, 'INDEXED');
  if (indexedMatch) {
    let newState = skipWhitespace(indexedMatch);
    const byMatch = matchKeyword(newState, 'BY');
    if (byMatch) {
      newState = skipWhitespace(byMatch);
      const indexName = parseIdentifier(newState);
      if (indexName) {
        return {
          state: indexName.state,
          hint: { type: 'indexed_by', indexName: indexName.value },
        };
      }
    }
  }

  return null;
}

// =============================================================================
// DML STATEMENT PARSING
// =============================================================================

/**
 * Parse INSERT statement
 */
function parseInsertStatement(state: ParserState): ParseResult<InsertStatement> {
  state = skipWhitespace(state);

  // Check for INSERT keyword
  const insertMatch = matchKeyword(state, 'INSERT');
  if (!insertMatch) {
    const rest = remaining(state);
    const token = rest.split(/\s/)[0];
    return createParseError(
      `Expected INSERT but got '${token}'`,
      state.position,
      state.input,
      { token, expected: 'INSERT' }
    );
  }
  state = skipWhitespace(insertMatch);

  // Check for conflict clause (OR REPLACE, etc.)
  let conflict: ConflictClause | undefined;
  const conflictResult = parseConflictClause(state);
  if (conflictResult) {
    conflict = conflictResult.conflict;
    state = skipWhitespace(conflictResult.state);
  }

  // Expect INTO
  const intoMatch = matchKeyword(state, 'INTO');
  if (!intoMatch) {
    const rest = remaining(state);
    const token = rest.split(/\s/)[0] || 'end of input';
    return createParseError(
      `Expected INTO after INSERT but got '${token}'`,
      state.position,
      state.input,
      { token, expected: 'INTO' }
    );
  }
  state = skipWhitespace(intoMatch);

  // Parse table name
  const tableResult = parseIdentifier(state);
  if (!tableResult) {
    const rest = remaining(state);
    const token = rest.split(/\s/)[0] || 'end of input';
    return createParseError(
      `Expected table name after INTO but got '${token}'`,
      state.position,
      state.input,
      { token, expected: 'table name' }
    );
  }
  const table = tableResult.value;
  state = skipWhitespace(tableResult.state);

  // Check for alias
  let alias: string | undefined;
  if (matchKeyword(state, 'AS')) {
    state = skipWhitespace(matchKeyword(state, 'AS')!);
    const aliasResult = parseIdentifier(state);
    if (aliasResult) {
      alias = aliasResult.value;
      state = aliasResult.state;
    }
  }
  state = skipWhitespace(state);

  // Check for column list
  let columns: string[] | undefined;
  if (remaining(state).startsWith('(')) {
    state = advance(state, 1);
    state = skipWhitespace(state);

    // Check if this is DEFAULT VALUES (no column list)
    if (!matchKeyword(state, 'SELECT')) {
      const colsResult = parseIdentifierList(state);
      if (colsResult) {
        columns = colsResult.values;
        state = skipWhitespace(colsResult.state);
      }

      if (!remaining(state).startsWith(')')) {
        const rest = remaining(state);
        const token = rest.split(/[\s,)]/)[0] || 'end of input';
        return createParseError(
          `Expected ')' after column list but got '${token}'`,
          state.position,
          state.input,
          { token, expected: ')' }
        );
      }
      state = advance(state, 1);
      state = skipWhitespace(state);
    } else {
      // This is INSERT INTO table (SELECT ...) - backtrack
      state = advance(state, -1);
    }
  }

  // Parse source (VALUES, SELECT, or DEFAULT VALUES)
  let source: InsertSource;

  if (matchKeyword(state, 'DEFAULT')) {
    state = skipWhitespace(matchKeyword(state, 'DEFAULT')!);
    const valuesMatch = matchKeyword(state, 'VALUES');
    if (!valuesMatch) {
      const rest = remaining(state);
      const token = rest.split(/\s/)[0] || 'end of input';
      return createParseError(
        `Expected VALUES after DEFAULT but got '${token}'`,
        state.position,
        state.input,
        { token, expected: 'VALUES' }
      );
    }
    state = valuesMatch;
    source = { type: 'insert_default' };
  } else if (matchKeyword(state, 'VALUES')) {
    state = skipWhitespace(matchKeyword(state, 'VALUES')!);

    const rows: ValuesRow[] = [];

    while (true) {
      state = skipWhitespace(state);
      if (!remaining(state).startsWith('(')) {
        if (rows.length === 0) {
          return createParseError(
            "Expected '(' to start values list",
            state.position,
            state.input,
            { expected: '(' }
          );
        }
        break;
      }
      state = advance(state, 1);
      state = skipWhitespace(state);

      const valuesResult = parseExpressionList(state);
      if (!valuesResult) {
        return createParseError(
          'Expected value expression in VALUES clause',
          state.position,
          state.input,
          { expected: 'value expression' }
        );
      }

      state = skipWhitespace(valuesResult.state);
      if (!remaining(state).startsWith(')')) {
        const rest = remaining(state);
        const token = rest.split(/[\s,)]/)[0] || 'end of input';
        return createParseError(
          `Expected ')' after values but got '${token}'`,
          state.position,
          state.input,
          { token, expected: ')' }
        );
      }
      state = advance(state, 1);

      rows.push({ type: 'values_row', values: valuesResult.exprs });

      state = skipWhitespace(state);
      if (!remaining(state).startsWith(',')) break;
      state = advance(state, 1);
    }

    source = { type: 'values_list', rows };
  } else if (matchKeyword(state, 'SELECT') || remaining(state).startsWith('(')) {
    // INSERT ... SELECT or INSERT ... (SELECT ...)
    let selectQuery: string;

    if (remaining(state).startsWith('(')) {
      // Parenthesized SELECT
      let depth = 1;
      let i = 1;
      const rest = remaining(state);
      while (i < rest.length && depth > 0) {
        if (rest[i] === '(') depth++;
        if (rest[i] === ')') depth--;
        i++;
      }
      selectQuery = rest.slice(1, i - 1).trim();
      state = advance(state, i);
    } else {
      // Find end of SELECT (before ON CONFLICT, RETURNING, or ;)
      const rest = remaining(state);
      let endIndex = rest.length;

      // Find where SELECT ends
      const onConflictIndex = rest.toUpperCase().indexOf(' ON CONFLICT');
      const returningIndex = rest.toUpperCase().indexOf(' RETURNING');
      const semicolonIndex = rest.indexOf(';');

      if (onConflictIndex !== -1 && onConflictIndex < endIndex) endIndex = onConflictIndex;
      if (returningIndex !== -1 && returningIndex < endIndex) endIndex = returningIndex;
      if (semicolonIndex !== -1 && semicolonIndex < endIndex) endIndex = semicolonIndex;

      selectQuery = rest.slice(0, endIndex).trim();
      state = advance(state, endIndex);
    }

    source = { type: 'insert_select', query: selectQuery };
  } else {
    const rest = remaining(state);
    const token = rest.split(/\s/)[0] || 'end of input';
    return createParseError(
      `Expected VALUES, SELECT, or DEFAULT VALUES but got '${token}'`,
      state.position,
      state.input,
      { token, expected: 'VALUES, SELECT, or DEFAULT VALUES' }
    );
  }

  state = skipWhitespace(state);

  // Check for ON CONFLICT
  let onConflict: OnConflictClause | undefined;
  const onConflictResult = parseOnConflictClause(state);
  if (onConflictResult) {
    onConflict = onConflictResult.onConflict;
    state = onConflictResult.state;
  }

  // Check for RETURNING
  let returning: ReturningClause | undefined;
  const returningResult = parseReturningClause(state);
  if (returningResult.type === 'error') {
    return createParseError(
      returningResult.message,
      returningResult.position,
      state.input,
      { expected: 'RETURNING clause' }
    );
  }
  if (returningResult.type === 'success') {
    returning = returningResult.returning;
    state = returningResult.state;
  }

  state = skipWhitespace(state);

  // Skip trailing semicolon
  if (remaining(state).startsWith(';')) {
    state = advance(state, 1);
  }

  const statement: InsertStatement = {
    type: 'insert',
    table,
    source,
  };

  if (conflict) statement.conflict = conflict;
  if (alias) statement.alias = alias;
  if (columns) statement.columns = columns;
  if (onConflict) statement.onConflict = onConflict;
  if (returning) statement.returning = returning;

  return {
    success: true,
    statement,
    remaining: remaining(state).trim(),
  };
}

/**
 * Parse UPDATE statement
 */
function parseUpdateStatement(state: ParserState): ParseResult<UpdateStatement> {
  state = skipWhitespace(state);

  // Check for UPDATE keyword
  const updateMatch = matchKeyword(state, 'UPDATE');
  if (!updateMatch) {
    const rest = remaining(state);
    const token = rest.split(/\s/)[0];
    return createParseError(
      `Expected UPDATE but got '${token}'`,
      state.position,
      state.input,
      { token, expected: 'UPDATE' }
    );
  }
  state = skipWhitespace(updateMatch);

  // Check for conflict clause
  let conflict: ConflictClause | undefined;
  const conflictResult = parseConflictClause(state);
  if (conflictResult) {
    conflict = conflictResult.conflict;
    state = skipWhitespace(conflictResult.state);
  }

  // Parse table name (possibly qualified with schema: schema.table)
  const tableResult = parseQualifiedIdentifier(state);
  if (!tableResult) {
    const rest = remaining(state);
    const token = rest.split(/\s/)[0] || 'end of input';
    return createParseError(
      `Expected table name after UPDATE but got '${token}'`,
      state.position,
      state.input,
      { token, expected: 'table name' }
    );
  }
  const table = tableResult.value;
  state = skipWhitespace(tableResult.state);

  // Check for alias
  let alias: string | undefined;
  if (matchKeyword(state, 'AS')) {
    state = skipWhitespace(matchKeyword(state, 'AS')!);
    const aliasResult = parseIdentifier(state);
    if (aliasResult) {
      alias = aliasResult.value;
      state = aliasResult.state;
    }
  } else {
    // Check for alias without AS (before SET)
    const nextWord = parseIdentifier(state);
    if (nextWord && nextWord.value.toUpperCase() !== 'SET') {
      alias = nextWord.value;
      state = nextWord.state;
    }
  }
  state = skipWhitespace(state);

  // Expect SET
  const setMatch = matchKeyword(state, 'SET');
  if (!setMatch) {
    const rest = remaining(state);
    const token = rest.split(/\s/)[0] || 'end of input';
    return createParseError(
      `Expected SET after table name but got '${token}'`,
      state.position,
      state.input,
      { token, expected: 'SET' }
    );
  }
  state = skipWhitespace(setMatch);

  // Parse SET clauses
  const setsResult = parseSetClauses(state);
  if (!setsResult) {
    const rest = remaining(state);
    const token = rest.split(/[\s=,]/)[0] || 'end of input';
    return createParseError(
      `Expected column assignment (column = value) but got '${token}'`,
      state.position,
      state.input,
      { token, expected: 'column = value' }
    );
  }
  state = setsResult.state;

  // Check for FROM clause
  let from: FromClause | undefined;
  const fromResult = parseFromClause(state);
  if (fromResult) {
    from = fromResult.from;
    state = fromResult.state;
  }

  // Check for WHERE clause
  let where: WhereClause | undefined;
  const whereResult = parseWhereClause(state);
  if (whereResult) {
    where = whereResult.where;
    state = whereResult.state;
  }

  // Check for ORDER BY
  let orderBy: OrderByClause | undefined;
  const orderByResult = parseOrderByClause(state);
  if (orderByResult) {
    orderBy = orderByResult.orderBy;
    state = orderByResult.state;
  }

  // Check for LIMIT
  let limit: LimitClause | undefined;
  const limitResult = parseLimitClause(state);
  if (limitResult) {
    limit = limitResult.limit;
    state = limitResult.state;
  }

  // Check for RETURNING
  let returning: ReturningClause | undefined;
  const returningResult = parseReturningClause(state);
  if (returningResult.type === 'error') {
    return createParseError(
      returningResult.message,
      returningResult.position,
      state.input,
      { expected: 'RETURNING clause' }
    );
  }
  if (returningResult.type === 'success') {
    returning = returningResult.returning;
    state = returningResult.state;
  }

  state = skipWhitespace(state);

  // Skip trailing semicolon
  if (remaining(state).startsWith(';')) {
    state = advance(state, 1);
  }

  const statement: UpdateStatement = {
    type: 'update',
    table,
    set: setsResult.sets,
  };

  if (conflict) statement.conflict = conflict;
  if (alias) statement.alias = alias;
  if (from) statement.from = from;
  if (where) statement.where = where;
  if (orderBy) statement.orderBy = orderBy;
  if (limit) statement.limit = limit;
  if (returning) statement.returning = returning;

  return {
    success: true,
    statement,
    remaining: remaining(state).trim(),
  };
}

/**
 * Parse DELETE statement
 */
function parseDeleteStatement(state: ParserState): ParseResult<DeleteStatement> {
  state = skipWhitespace(state);

  // Check for DELETE keyword
  const deleteMatch = matchKeyword(state, 'DELETE');
  if (!deleteMatch) {
    const rest = remaining(state);
    const token = rest.split(/\s/)[0];
    return createParseError(
      `Expected DELETE but got '${token}'`,
      state.position,
      state.input,
      { token, expected: 'DELETE' }
    );
  }
  state = skipWhitespace(deleteMatch);

  // Expect FROM
  const fromMatch = matchKeyword(state, 'FROM');
  if (!fromMatch) {
    const rest = remaining(state);
    const token = rest.split(/\s/)[0] || 'end of input';
    return createParseError(
      `Expected FROM after DELETE but got '${token}'`,
      state.position,
      state.input,
      { token, expected: 'FROM' }
    );
  }
  state = skipWhitespace(fromMatch);

  // Parse table name
  const tableResult = parseIdentifier(state);
  if (!tableResult) {
    const rest = remaining(state);
    const token = rest.split(/\s/)[0] || 'end of input';
    return createParseError(
      `Expected table name after FROM but got '${token}'`,
      state.position,
      state.input,
      { token, expected: 'table name' }
    );
  }
  const table = tableResult.value;
  state = skipWhitespace(tableResult.state);

  // Check for INDEXED BY or NOT INDEXED (must come before alias)
  let indexHint: IndexHint | undefined;
  const indexHintResult = parseIndexHint(state);
  if (indexHintResult) {
    indexHint = indexHintResult.hint;
    state = skipWhitespace(indexHintResult.state);
  }

  // Check for alias
  let alias: string | undefined;
  if (matchKeyword(state, 'AS')) {
    state = skipWhitespace(matchKeyword(state, 'AS')!);
    const aliasResult = parseIdentifier(state);
    if (aliasResult) {
      alias = aliasResult.value;
      state = aliasResult.state;
    }
  } else {
    // Check for alias without AS (before WHERE)
    // But NOT if it's INDEXED or NOT (those are hints, not aliases)
    const nextWord = parseIdentifier(state);
    if (nextWord) {
      const upper = nextWord.value.toUpperCase();
      if (!['WHERE', 'ORDER', 'LIMIT', 'RETURNING', 'INDEXED', 'NOT'].includes(upper)) {
        alias = nextWord.value;
        state = nextWord.state;
      }
    }
  }
  state = skipWhitespace(state);

  // Check for WHERE clause
  let where: WhereClause | undefined;
  const whereResult = parseWhereClause(state);
  if (whereResult) {
    where = whereResult.where;
    state = whereResult.state;
  }

  // Check for ORDER BY
  let orderBy: OrderByClause | undefined;
  const orderByResult = parseOrderByClause(state);
  if (orderByResult) {
    orderBy = orderByResult.orderBy;
    state = orderByResult.state;
  }

  // Check for LIMIT
  let limit: LimitClause | undefined;
  const limitResult = parseLimitClause(state);
  if (limitResult) {
    limit = limitResult.limit;
    state = limitResult.state;
  }

  // Check for RETURNING
  let returning: ReturningClause | undefined;
  const returningResult = parseReturningClause(state);
  if (returningResult.type === 'error') {
    return createParseError(
      returningResult.message,
      returningResult.position,
      state.input,
      { expected: 'RETURNING clause' }
    );
  }
  if (returningResult.type === 'success') {
    returning = returningResult.returning;
    state = returningResult.state;
  }

  state = skipWhitespace(state);

  // Skip trailing semicolon
  if (remaining(state).startsWith(';')) {
    state = advance(state, 1);
  }

  const statement: DeleteStatement = {
    type: 'delete',
    table,
  };

  if (alias) statement.alias = alias;
  if (indexHint) statement.indexHint = indexHint;
  if (where) statement.where = where;
  if (orderBy) statement.orderBy = orderBy;
  if (limit) statement.limit = limit;
  if (returning) statement.returning = returning;

  return {
    success: true,
    statement,
    remaining: remaining(state).trim(),
  };
}

/**
 * Parse REPLACE statement (SQLite extension)
 */
function parseReplaceStatement(state: ParserState): ParseResult<ReplaceStatement> {
  state = skipWhitespace(state);

  // Check for REPLACE keyword
  const replaceMatch = matchKeyword(state, 'REPLACE');
  if (!replaceMatch) {
    const rest = remaining(state);
    const token = rest.split(/\s/)[0];
    return createParseError(
      `Expected REPLACE but got '${token}'`,
      state.position,
      state.input,
      { token, expected: 'REPLACE' }
    );
  }
  state = skipWhitespace(replaceMatch);

  // Expect INTO
  const intoMatch = matchKeyword(state, 'INTO');
  if (!intoMatch) {
    const rest = remaining(state);
    const token = rest.split(/\s/)[0] || 'end of input';
    return createParseError(
      `Expected INTO after REPLACE but got '${token}'`,
      state.position,
      state.input,
      { token, expected: 'INTO' }
    );
  }
  state = skipWhitespace(intoMatch);

  // Parse table name
  const tableResult = parseIdentifier(state);
  if (!tableResult) {
    const rest = remaining(state);
    const token = rest.split(/\s/)[0] || 'end of input';
    return createParseError(
      `Expected table name after INTO but got '${token}'`,
      state.position,
      state.input,
      { token, expected: 'table name' }
    );
  }
  const table = tableResult.value;
  state = skipWhitespace(tableResult.state);

  // Check for alias
  let alias: string | undefined;
  if (matchKeyword(state, 'AS')) {
    state = skipWhitespace(matchKeyword(state, 'AS')!);
    const aliasResult = parseIdentifier(state);
    if (aliasResult) {
      alias = aliasResult.value;
      state = aliasResult.state;
    }
  }
  state = skipWhitespace(state);

  // Check for column list
  let columns: string[] | undefined;
  if (remaining(state).startsWith('(')) {
    state = advance(state, 1);
    state = skipWhitespace(state);

    if (!matchKeyword(state, 'SELECT')) {
      const colsResult = parseIdentifierList(state);
      if (colsResult) {
        columns = colsResult.values;
        state = skipWhitespace(colsResult.state);
      }

      if (!remaining(state).startsWith(')')) {
        const rest = remaining(state);
        const token = rest.split(/[\s,)]/)[0] || 'end of input';
        return createParseError(
          `Expected ')' after column list but got '${token}'`,
          state.position,
          state.input,
          { token, expected: ')' }
        );
      }
      state = advance(state, 1);
      state = skipWhitespace(state);
    } else {
      state = advance(state, -1);
    }
  }

  // Parse source (VALUES, SELECT, or DEFAULT VALUES)
  let source: InsertSource;

  if (matchKeyword(state, 'DEFAULT')) {
    state = skipWhitespace(matchKeyword(state, 'DEFAULT')!);
    const valuesMatch = matchKeyword(state, 'VALUES');
    if (!valuesMatch) {
      const rest = remaining(state);
      const token = rest.split(/\s/)[0] || 'end of input';
      return createParseError(
        `Expected VALUES after DEFAULT but got '${token}'`,
        state.position,
        state.input,
        { token, expected: 'VALUES' }
      );
    }
    state = valuesMatch;
    source = { type: 'insert_default' };
  } else if (matchKeyword(state, 'VALUES')) {
    state = skipWhitespace(matchKeyword(state, 'VALUES')!);

    const rows: ValuesRow[] = [];

    while (true) {
      state = skipWhitespace(state);
      if (!remaining(state).startsWith('(')) {
        if (rows.length === 0) {
          return createParseError(
            "Expected '(' to start values list",
            state.position,
            state.input,
            { expected: '(' }
          );
        }
        break;
      }
      state = advance(state, 1);
      state = skipWhitespace(state);

      const valuesResult = parseExpressionList(state);
      if (!valuesResult) {
        return createParseError(
          'Expected value expression in VALUES clause',
          state.position,
          state.input,
          { expected: 'value expression' }
        );
      }

      state = skipWhitespace(valuesResult.state);
      if (!remaining(state).startsWith(')')) {
        const rest = remaining(state);
        const token = rest.split(/[\s,)]/)[0] || 'end of input';
        return createParseError(
          `Expected ')' after values but got '${token}'`,
          state.position,
          state.input,
          { token, expected: ')' }
        );
      }
      state = advance(state, 1);

      rows.push({ type: 'values_row', values: valuesResult.exprs });

      state = skipWhitespace(state);
      if (!remaining(state).startsWith(',')) break;
      state = advance(state, 1);
    }

    source = { type: 'values_list', rows };
  } else if (matchKeyword(state, 'SELECT') || remaining(state).startsWith('(')) {
    let selectQuery: string;

    if (remaining(state).startsWith('(')) {
      let depth = 1;
      let i = 1;
      const rest = remaining(state);
      while (i < rest.length && depth > 0) {
        if (rest[i] === '(') depth++;
        if (rest[i] === ')') depth--;
        i++;
      }
      selectQuery = rest.slice(1, i - 1).trim();
      state = advance(state, i);
    } else {
      const rest = remaining(state);
      let endIndex = rest.length;

      const returningIndex = rest.toUpperCase().indexOf(' RETURNING');
      const semicolonIndex = rest.indexOf(';');

      if (returningIndex !== -1 && returningIndex < endIndex) endIndex = returningIndex;
      if (semicolonIndex !== -1 && semicolonIndex < endIndex) endIndex = semicolonIndex;

      selectQuery = rest.slice(0, endIndex).trim();
      state = advance(state, endIndex);
    }

    source = { type: 'insert_select', query: selectQuery };
  } else {
    const rest = remaining(state);
    const token = rest.split(/\s/)[0] || 'end of input';
    return createParseError(
      `Expected VALUES, SELECT, or DEFAULT VALUES but got '${token}'`,
      state.position,
      state.input,
      { token, expected: 'VALUES, SELECT, or DEFAULT VALUES' }
    );
  }

  state = skipWhitespace(state);

  // Check for RETURNING
  let returning: ReturningClause | undefined;
  const returningResult = parseReturningClause(state);
  if (returningResult.type === 'error') {
    return createParseError(
      returningResult.message,
      returningResult.position,
      state.input,
      { expected: 'RETURNING clause' }
    );
  }
  if (returningResult.type === 'success') {
    returning = returningResult.returning;
    state = returningResult.state;
  }

  state = skipWhitespace(state);

  // Skip trailing semicolon
  if (remaining(state).startsWith(';')) {
    state = advance(state, 1);
  }

  const statement: ReplaceStatement = {
    type: 'replace',
    table,
    source,
  };

  if (alias) statement.alias = alias;
  if (columns) statement.columns = columns;
  if (returning) statement.returning = returning;

  return {
    success: true,
    statement,
    remaining: remaining(state).trim(),
  };
}

// =============================================================================
// PUBLIC API
// =============================================================================

/**
 * Parse a DML statement (INSERT, UPDATE, DELETE, or REPLACE)
 */
export function parseDML(sql: string): ParseResult<DMLStatement> {
  const state = createState(sql);
  const trimmed = sql.trim().toUpperCase();

  if (trimmed.startsWith('INSERT')) {
    return parseInsertStatement(state);
  }

  if (trimmed.startsWith('UPDATE')) {
    return parseUpdateStatement(state);
  }

  if (trimmed.startsWith('DELETE')) {
    return parseDeleteStatement(state);
  }

  if (trimmed.startsWith('REPLACE')) {
    return parseReplaceStatement(state);
  }

  // Extract the first token for better error message
  const firstToken = trimmed.split(/\s/)[0] || 'empty input';
  return createParseError(
    `Expected DML statement (INSERT, UPDATE, DELETE, or REPLACE) but got '${firstToken}'`,
    0,
    sql,
    { token: firstToken, expected: 'INSERT, UPDATE, DELETE, or REPLACE' }
  );
}

/**
 * Parse an INSERT statement
 */
export function parseInsert(sql: string): ParseResult<InsertStatement> {
  return parseInsertStatement(createState(sql));
}

/**
 * Parse an UPDATE statement
 */
export function parseUpdate(sql: string): ParseResult<UpdateStatement> {
  return parseUpdateStatement(createState(sql));
}

/**
 * Parse a DELETE statement
 */
export function parseDelete(sql: string): ParseResult<DeleteStatement> {
  return parseDeleteStatement(createState(sql));
}

/**
 * Parse a REPLACE statement
 */
export function parseReplace(sql: string): ParseResult<ReplaceStatement> {
  return parseReplaceStatement(createState(sql));
}

/**
 * Check if a SQL string is a DML statement
 */
export function isDMLStatement(sql: string): boolean {
  const trimmed = sql.trim().toUpperCase();
  return (
    trimmed.startsWith('INSERT') ||
    trimmed.startsWith('UPDATE') ||
    trimmed.startsWith('DELETE') ||
    trimmed.startsWith('REPLACE')
  );
}

/**
 * Get the type of DML statement
 */
export function getDMLType(sql: string): 'INSERT' | 'UPDATE' | 'DELETE' | 'REPLACE' | null {
  const trimmed = sql.trim().toUpperCase();
  if (trimmed.startsWith('INSERT')) return 'INSERT';
  if (trimmed.startsWith('UPDATE')) return 'UPDATE';
  if (trimmed.startsWith('DELETE')) return 'DELETE';
  if (trimmed.startsWith('REPLACE')) return 'REPLACE';
  return null;
}
