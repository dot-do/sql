/**
 * CASE Expression Support for DoSQL
 *
 * Provides parsing and evaluation of SQL CASE expressions.
 * Also supports scalar subquery evaluation within expressions.
 */

import type { SqlValue } from './types.js';

// =============================================================================
// SUBQUERY EVALUATOR TYPE
// =============================================================================

/**
 * Function type for evaluating scalar subqueries
 * Returns the first column of the first row, or null if empty
 */
export type SubqueryEvaluator = (sql: string, params: SqlValue[], outerRow?: Record<string, SqlValue>) => SqlValue;

/**
 * Context for expression evaluation that includes optional subquery support
 */
export interface ExprEvalContext {
  params: SqlValue[];
  pIdx: { value: number };
  /** Optional function to evaluate scalar subqueries */
  evaluateSubquery?: SubqueryEvaluator;
  /** Current outer row for correlated subquery support */
  outerRow?: Record<string, SqlValue>;
}

// =============================================================================
// CASE EXPRESSION TYPES
// =============================================================================

export interface CaseExpression {
  type: 'searched' | 'simple';
  operand?: string;
  whenClauses: Array<{ condition: string; result: string }>;
  elseResult?: string;
}

// =============================================================================
// PARSING HELPERS
// =============================================================================

function findMatchingEnd(str: string, startIndex: number): number {
  let depth = 1, i = startIndex;
  const upper = str.toUpperCase();
  while (i < str.length && depth > 0) {
    if (str[i] === "'") {
      i++;
      while (i < str.length && str[i] !== "'") {
        if (str[i] === "'" && str[i+1] === "'") i += 2;
        else i++;
      }
      i++;
      continue;
    }
    if (upper.slice(i).match(/^CASE\b/)) { depth++; i += 4; continue; }
    if (upper.slice(i).match(/^END\b/)) { depth--; if (depth === 0) return i; i += 3; continue; }
    i++;
  }
  return -1;
}

function findKwPos(str: string, start: number, kw: string): number {
  let depth = 0, inStr = false;
  const upper = str.toUpperCase(), ukw = kw.toUpperCase();
  for (let i = start; i < str.length; i++) {
    if (str[i] === "'" && !inStr) { inStr = true; continue; }
    if (str[i] === "'" && inStr) { if (str[i+1] === "'") { i++; continue; } inStr = false; continue; }
    if (inStr) continue;
    if (upper.slice(i).match(/^CASE\b/)) { depth++; i += 3; continue; }
    if (upper.slice(i).match(/^END\b/)) { if (depth > 0) depth--; i += 2; continue; }
    if (depth === 0 && upper.slice(i).startsWith(ukw)) {
      const after = str[i + ukw.length];
      if (!after || /[\s(,)]/.test(after)) return i;
    }
  }
  return -1;
}

function tokenizeCaseBody(body: string, isSearched: boolean): Array<{type:'when'|'else';condition?:string;result:string}> {
  const tokens: Array<{type:'when'|'else';condition?:string;result:string}> = [];
  let pos = isSearched ? 0 : findKwPos(body, 0, 'WHEN');
  if (pos === -1) pos = 0;

  while (pos < body.length) {
    const rem = body.slice(pos), urem = rem.toUpperCase();
    if (/^\s/.test(rem[0])) { pos++; continue; }

    if (urem.match(/^WHEN\b/)) {
      pos += 4;
      while (pos < body.length && /\s/.test(body[pos])) pos++;
      const thenPos = findKwPos(body, pos, 'THEN');
      if (thenPos === -1) break;
      const cond = body.slice(pos, thenPos).trim();
      pos = thenPos + 4;
      while (pos < body.length && /\s/.test(body[pos])) pos++;
      const nextWhen = findKwPos(body, pos, 'WHEN');
      const nextElse = findKwPos(body, pos, 'ELSE');
      let end = body.length;
      if (nextWhen !== -1 && (nextElse === -1 || nextWhen < nextElse)) end = nextWhen;
      else if (nextElse !== -1) end = nextElse;
      tokens.push({type:'when', condition: cond, result: body.slice(pos, end).trim()});
      pos = end;
      continue;
    }

    if (urem.match(/^ELSE\b/)) {
      pos += 4;
      while (pos < body.length && /\s/.test(body[pos])) pos++;
      tokens.push({type:'else', result: body.slice(pos).trim()});
      break;
    }
    pos++;
  }
  return tokens;
}

// =============================================================================
// PARSING
// =============================================================================

export function parseCaseExpression(expr: string): { caseExpr: CaseExpression; endPos: number } | null {
  const str = expr, upper = str.toUpperCase();
  if (!upper.match(/^CASE\b/)) return null;

  let pos = 4;
  while (pos < str.length && /\s/.test(str[pos])) pos++;

  const endPos = findMatchingEnd(str, pos);
  if (endPos === -1) return null;

  const body = str.slice(pos, endPos).trim();
  const uBody = body.toUpperCase();
  const isSearched = uBody.startsWith('WHEN');

  const caseExpr: CaseExpression = {
    type: isSearched ? 'searched' : 'simple',
    whenClauses: [],
    elseResult: undefined,
    operand: undefined
  };

  if (!isSearched) {
    const whenIdx = findKwPos(body, 0, 'WHEN');
    if (whenIdx === -1) return null;
    caseExpr.operand = body.slice(0, whenIdx).trim();
  }

  for (const t of tokenizeCaseBody(body, isSearched)) {
    if (t.type === 'when') {
      caseExpr.whenClauses.push({ condition: t.condition!, result: t.result });
    } else {
      caseExpr.elseResult = t.result;
    }
  }

  return { caseExpr, endPos: endPos + 3 };
}

// =============================================================================
// EVALUATION
// =============================================================================

export function evaluateCaseExpr(
  expr: string,
  row: Record<string, SqlValue>,
  params: SqlValue[],
  pIdx: {value: number}
): SqlValue {
  const tr = expr.trim(), utr = tr.toUpperCase();

  if (utr.startsWith('CASE')) {
    const p = parseCaseExpression(tr);
    if (p) return evalCaseExpr(p.caseExpr, row, params, pIdx);
  }

  // Handle NOT IN expression: col NOT IN (val1, val2, ...)
  const notInMatch = tr.match(/^(\w+)\s+NOT\s+IN\s*\(([^)]*)\)$/i);
  if (notInMatch) {
    const colName = notInMatch[1];
    const valueList = notInMatch[2];
    const colVal = row[colName] ?? null;

    // NULL NOT IN (...) returns NULL
    if (colVal === null) return null;

    const values = parseValueListForExpr(valueList, params, pIdx);

    // Check if list contains NULL
    const hasNull = values.some(v => v === null);

    // Empty list: NOT IN () = TRUE
    if (values.length === 0) return 1;

    // Check if value is in the list
    const inList = values.some(v => v !== null && valuesEqualForExpr(v, colVal));

    if (inList) return 0; // value found, NOT IN = FALSE

    // Value not found
    if (hasNull) return null; // NULL in list makes result NULL

    return 1; // NOT IN = TRUE
  }

  // Handle IN expression: col IN (val1, val2, ...)
  const inMatch = tr.match(/^(\w+)\s+IN\s*\(([^)]*)\)$/i);
  if (inMatch) {
    const colName = inMatch[1];
    const valueList = inMatch[2];
    const colVal = row[colName] ?? null;

    // NULL IN (...) returns NULL
    if (colVal === null) return null;

    const values = parseValueListForExpr(valueList, params, pIdx);

    // Empty list: IN () = FALSE
    if (values.length === 0) return 0;

    // Check if value is in the list
    const inList = values.some(v => v !== null && valuesEqualForExpr(v, colVal));

    return inList ? 1 : 0;
  }

  if (tr === '?') return params[pIdx.value++];
  if (utr === 'NULL') return null;
  if (tr.startsWith("'") && tr.endsWith("'")) return tr.slice(1, -1).replace(/''/g, "'");
  if (/^-?\d+(\.\d+)?$/.test(tr)) return Number(tr);
  if (tr.startsWith('(') && tr.endsWith(')')) return evaluateCaseExpr(tr.slice(1, -1), row, params, pIdx);
  if (/^\w+$/.test(tr)) return row[tr] ?? null;

  // Handle table.column references (e.g., t1.col)
  if (/^\w+\.\w+$/.test(tr)) return row[tr] ?? null;

  // Handle comparison expressions - return 1 for true, 0 for false, NULL if either operand is NULL
  const compResult = evalComparison(tr, row, params, pIdx);
  if (compResult !== undefined) return compResult;

  return evalArith(tr, row, params, pIdx);
}

/**
 * Parse value list for expression evaluation
 */
function parseValueListForExpr(valueList: string, params: SqlValue[], pIdx: {value: number}): SqlValue[] {
  const values: SqlValue[] = [];
  const items: string[] = [];
  let current = '';
  let inQuote = false;

  for (let i = 0; i < valueList.length; i++) {
    const char = valueList[i];
    if (!inQuote && char === "'") {
      inQuote = true;
      current += char;
    } else if (inQuote && char === "'") {
      if (i + 1 < valueList.length && valueList[i + 1] === "'") {
        current += "''";
        i++;
      } else {
        inQuote = false;
        current += char;
      }
    } else if (!inQuote && char === ',') {
      items.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }
  if (current.trim()) {
    items.push(current.trim());
  }

  for (const item of items) {
    if (item === '?') {
      values.push(params[pIdx.value++]);
    } else if (item.startsWith("'") && item.endsWith("'")) {
      values.push(item.slice(1, -1).replace(/''/g, "'"));
    } else if (item.toUpperCase() === 'NULL') {
      values.push(null);
    } else if (!isNaN(Number(item))) {
      values.push(Number(item));
    } else {
      values.push(item);
    }
  }

  return values;
}

/**
 * Compare values for expression evaluation
 */
function valuesEqualForExpr(a: SqlValue, b: SqlValue): boolean {
  if (a === null || b === null) return false;
  if (typeof a === 'number' && typeof b === 'number') return a === b;
  if (typeof a === 'string' && typeof b === 'string') return a === b;
  return String(a) === String(b) || Number(a) === Number(b);
}

/**
 * Token types for expression lexer
 */
interface ArithToken {
  type: 'number' | 'identifier' | 'operator' | 'lparen' | 'rparen' | 'comma' | 'null' | 'string';
  value: string;
}

/**
 * Expression AST node types
 */
type ArithExprNode =
  | { type: 'number'; value: number }
  | { type: 'string'; value: string }
  | { type: 'column'; name: string }
  | { type: 'null' }
  | { type: 'binary'; op: '+' | '-' | '*' | '/' | '%'; left: ArithExprNode; right: ArithExprNode }
  | { type: 'unary'; op: '+' | '-'; operand: ArithExprNode }
  | { type: 'function'; name: string; args: ArithExprNode[] };

/**
 * Tokenize an expression string for arithmetic parsing
 */
function tokenizeArith(expr: string): ArithToken[] {
  const tokens: ArithToken[] = [];
  let i = 0;

  while (i < expr.length) {
    const ch = expr[i];

    // Skip whitespace
    if (/\s/.test(ch)) { i++; continue; }

    // Numbers (including decimals)
    if (/\d/.test(ch) || (ch === '.' && /\d/.test(expr[i + 1] || ''))) {
      let num = '';
      while (i < expr.length && (/\d/.test(expr[i]) || expr[i] === '.')) {
        num += expr[i++];
      }
      tokens.push({ type: 'number', value: num });
      continue;
    }

    // String literals
    if (ch === "'") {
      let str = '';
      i++; // skip opening quote
      while (i < expr.length) {
        if (expr[i] === "'") {
          if (i + 1 < expr.length && expr[i + 1] === "'") {
            str += "'";
            i += 2;
          } else {
            i++; // skip closing quote
            break;
          }
        } else {
          str += expr[i++];
        }
      }
      tokens.push({ type: 'string', value: str });
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

    // Operators
    if (['+', '-', '*', '/', '%'].includes(ch)) {
      tokens.push({ type: 'operator', value: ch });
      i++;
      continue;
    }

    // Parentheses
    if (ch === '(') { tokens.push({ type: 'lparen', value: '(' }); i++; continue; }
    if (ch === ')') { tokens.push({ type: 'rparen', value: ')' }); i++; continue; }

    // Comma
    if (ch === ',') { tokens.push({ type: 'comma', value: ',' }); i++; continue; }

    // Unknown character - skip
    i++;
  }

  return tokens;
}

/**
 * Parse tokens into an AST using recursive descent
 */
function parseArithTokens(tokens: ArithToken[]): ArithExprNode {
  let pos = 0;

  function peek(): ArithToken | undefined { return tokens[pos]; }
  function consume(): ArithToken | undefined { return tokens[pos++]; }
  function match(type: ArithToken['type'], value?: string): boolean {
    const tok = peek();
    if (!tok) return false;
    if (tok.type !== type) return false;
    if (value !== undefined && tok.value !== value) return false;
    return true;
  }

  function parseExpression(): ArithExprNode { return parseAdditive(); }

  function parseAdditive(): ArithExprNode {
    let left = parseMultiplicative();
    while (match('operator', '+') || match('operator', '-')) {
      const op = consume()!.value as '+' | '-';
      const right = parseMultiplicative();
      left = { type: 'binary', op, left, right };
    }
    return left;
  }

  function parseMultiplicative(): ArithExprNode {
    let left = parseUnary();
    while (match('operator', '*') || match('operator', '/') || match('operator', '%')) {
      const op = consume()!.value as '*' | '/' | '%';
      const right = parseUnary();
      left = { type: 'binary', op, left, right };
    }
    return left;
  }

  function parseUnary(): ArithExprNode {
    if (match('operator', '+') || match('operator', '-')) {
      const op = consume()!.value as '+' | '-';
      const operand = parseUnary();
      return { type: 'unary', op, operand };
    }
    return parsePrimary();
  }

  function parsePrimary(): ArithExprNode {
    const tok = peek();
    if (!tok) throw new Error('Unexpected end of expression');

    if (tok.type === 'number') {
      consume();
      return { type: 'number', value: parseFloat(tok.value) };
    }

    if (tok.type === 'string') {
      consume();
      return { type: 'string', value: tok.value };
    }

    if (tok.type === 'null') {
      consume();
      return { type: 'null' };
    }

    if (tok.type === 'identifier') {
      consume();
      // Check for function call
      if (match('lparen')) {
        consume(); // consume '('
        const args: ArithExprNode[] = [];
        if (!match('rparen')) {
          args.push(parseExpression());
          while (match('comma')) {
            consume();
            args.push(parseExpression());
          }
        }
        if (match('rparen')) consume();
        return { type: 'function', name: tok.value.toLowerCase(), args };
      }
      return { type: 'column', name: tok.value };
    }

    if (tok.type === 'lparen') {
      consume();
      const expr = parseExpression();
      if (match('rparen')) consume();
      return expr;
    }

    throw new Error(`Unexpected token: ${tok.type}`);
  }

  return parseExpression();
}

/**
 * Evaluate an arithmetic AST against a row of data
 */
function evalArithAST(node: ArithExprNode, row: Record<string, SqlValue>): SqlValue {
  switch (node.type) {
    case 'number': return node.value;
    case 'string': return node.value;
    case 'null': return null;
    case 'column': return row[node.name] ?? null;

    case 'unary': {
      const operand = evalArithAST(node.operand, row);
      if (operand === null) return null;
      const num = Number(operand);
      if (isNaN(num)) return null;
      if (node.op === '+') return num === 0 ? 0 : num;
      if (node.op === '-') {
        const negated = -num;
        return negated === 0 ? 0 : negated;
      }
      return null;
    }

    case 'binary': {
      const left = evalArithAST(node.left, row);
      const right = evalArithAST(node.right, row);
      if (left === null || right === null) return null;
      const numL = Number(left), numR = Number(right);
      if (isNaN(numL) || isNaN(numR)) return null;

      switch (node.op) {
        case '+': return numL + numR;
        case '-': return numL - numR;
        case '*': return numL * numR;
        case '/':
          if (numR === 0) return null;
          // Integer division if both operands are integers
          if (Number.isInteger(numL) && Number.isInteger(numR)) {
            return Math.trunc(numL / numR);
          }
          return numL / numR;
        case '%':
          if (numR === 0) return null;
          return numL % numR;
      }
      return null;
    }

    case 'function': {
      const args = node.args.map(arg => evalArithAST(arg, row));
      switch (node.name) {
        case 'abs': {
          const arg = args[0];
          if (arg === null) return null;
          const num = Number(arg);
          if (isNaN(num)) return null;
          return Math.abs(num);
        }
        case 'upper': {
          const arg = args[0];
          if (arg === null) return null;
          return String(arg).toUpperCase();
        }
        case 'lower': {
          const arg = args[0];
          if (arg === null) return null;
          return String(arg).toLowerCase();
        }
        case 'length': {
          const arg = args[0];
          if (arg === null) return null;
          return String(arg).length;
        }
        // Null handling functions
        case 'coalesce': {
          for (const arg of args) {
            if (arg !== null) return arg;
          }
          return null;
        }
        case 'ifnull':
        case 'isnull': {
          return args[0] ?? args[1] ?? null;
        }
        case 'nullif': {
          if (args.length >= 2 && args[0] === args[1]) {
            return null;
          }
          return args[0] ?? null;
        }
        // Numeric aggregate-like functions
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
        // Math functions
        case 'round': {
          const arg = args[0];
          if (arg === null) return null;
          const num = Number(arg);
          if (isNaN(num)) return null;
          const precision = args[1] !== null ? Number(args[1]) : 0;
          const factor = Math.pow(10, precision);
          return Math.round(num * factor) / factor;
        }
        case 'floor': {
          const arg = args[0];
          if (arg === null) return null;
          const num = Number(arg);
          if (isNaN(num)) return null;
          return Math.floor(num);
        }
        case 'ceil':
        case 'ceiling': {
          const arg = args[0];
          if (arg === null) return null;
          const num = Number(arg);
          if (isNaN(num)) return null;
          return Math.ceil(num);
        }
        case 'sqrt': {
          const arg = args[0];
          if (arg === null) return null;
          const num = Number(arg);
          if (isNaN(num)) return null;
          return Math.sqrt(num);
        }
        case 'power':
        case 'pow': {
          if (args[0] === null || args[1] === null) return null;
          const base = Number(args[0]);
          const exp = Number(args[1]);
          if (isNaN(base) || isNaN(exp)) return null;
          return Math.pow(base, exp);
        }
        // String functions
        case 'substr':
        case 'substring': {
          if (args[0] === null) return null;
          const str = String(args[0]);
          const start = args[1] !== null ? Number(args[1]) - 1 : 0; // SQL is 1-indexed
          const len = args[2] !== null ? Number(args[2]) : undefined;
          return str.substring(start, len !== undefined ? start + len : undefined);
        }
        case 'trim': {
          if (args[0] === null) return null;
          return String(args[0]).trim();
        }
        case 'ltrim': {
          if (args[0] === null) return null;
          return String(args[0]).trimStart();
        }
        case 'rtrim': {
          if (args[0] === null) return null;
          return String(args[0]).trimEnd();
        }
        case 'replace': {
          if (args[0] === null || args[1] === null) return null;
          const str = String(args[0]);
          const from = String(args[1]);
          const to = args[2] !== null ? String(args[2]) : '';
          return str.split(from).join(to);
        }
        case 'concat': {
          return args.map(a => a !== null ? String(a) : '').join('');
        }
        case 'instr': {
          if (args[0] === null || args[1] === null) return null;
          const str = String(args[0]);
          const needle = String(args[1]);
          const idx = str.indexOf(needle);
          return idx === -1 ? 0 : idx + 1; // SQL is 1-indexed
        }
        default: return null;
      }
    }
  }
  return null;
}

/**
 * Evaluate a comparison expression and return 1 (true), 0 (false), or null.
 * Returns undefined if the expression is not a comparison.
 * Properly handles SQL comparison semantics:
 * - Returns 1 for true, 0 for false (SQL integers, not JS booleans)
 * - Returns null if either operand is null
 */
function evalComparison(
  expr: string,
  row: Record<string, SqlValue>,
  params: SqlValue[],
  pIdx: {value: number}
): SqlValue | undefined {
  // Find comparison operator outside of parentheses and strings
  // Order matters: check multi-char operators before single-char
  let depth = 0;
  let inStr = false;

  for (let i = 0; i < expr.length; i++) {
    const ch = expr[i];

    if (ch === "'" && !inStr) { inStr = true; continue; }
    if (ch === "'" && inStr) {
      if (expr[i + 1] === "'") { i++; continue; }
      inStr = false; continue;
    }
    if (inStr) continue;

    if (ch === '(') { depth++; continue; }
    if (ch === ')') { depth--; continue; }

    if (depth === 0) {
      // Check for multi-char operators first
      const twoChar = expr.slice(i, i + 2);
      let op: string | null = null;
      let opLen = 0;

      if (twoChar === '>=' || twoChar === '<=' || twoChar === '<>' || twoChar === '!=') {
        op = twoChar;
        opLen = 2;
      } else if (ch === '>' || ch === '<' || ch === '=') {
        // Make sure it's not part of a multi-char operator
        const next = expr[i + 1];
        if ((ch === '>' || ch === '<') && next === '=') continue;
        if (ch === '<' && next === '>') continue;
        if (ch === '!' && next === '=') continue;
        op = ch;
        opLen = 1;
      }

      if (op) {
        const leftExpr = expr.slice(0, i).trim();
        const rightExpr = expr.slice(i + opLen).trim();

        // Ensure we have valid left and right expressions
        if (!leftExpr || !rightExpr) continue;

        const l = evaluateCaseExpr(leftExpr, row, params, { value: pIdx.value });
        const r = evaluateCaseExpr(rightExpr, row, params, { value: pIdx.value });

        // NULL comparison returns NULL (except IS NULL handled elsewhere)
        if (l === null || r === null) return null;

        switch (op) {
          case '=':
            return (l === r || Number(l) === Number(r)) ? 1 : 0;
          case '<>':
          case '!=':
            return (l !== r && Number(l) !== Number(r)) ? 1 : 0;
          case '>':
            if (typeof l === 'string' && typeof r === 'string') return l > r ? 1 : 0;
            return Number(l) > Number(r) ? 1 : 0;
          case '<':
            if (typeof l === 'string' && typeof r === 'string') return l < r ? 1 : 0;
            return Number(l) < Number(r) ? 1 : 0;
          case '>=':
            if (typeof l === 'string' && typeof r === 'string') return l >= r ? 1 : 0;
            return Number(l) >= Number(r) ? 1 : 0;
          case '<=':
            if (typeof l === 'string' && typeof r === 'string') return l <= r ? 1 : 0;
            return Number(l) <= Number(r) ? 1 : 0;
        }
      }
    }
  }

  // Not a comparison expression
  return undefined;
}

function evalArith(
  expr: string,
  row: Record<string, SqlValue>,
  params: SqlValue[],
  pIdx: {value: number}
): SqlValue {
  // Parse and evaluate arithmetic expression with full operator support
  // Handles: +, -, *, /, %, unary operators, function calls like abs()
  try {
    const tokens = tokenizeArith(expr);
    if (tokens.length === 0) return row[expr.trim()] ?? null;
    const ast = parseArithTokens(tokens);
    return evalArithAST(ast, row);
  } catch {
    // Fallback to column lookup if parsing fails
    return row[expr.trim()] ?? null;
  }
}

function evalCond(
  cond: string,
  row: Record<string, SqlValue>,
  params: SqlValue[],
  pIdx: {value: number}
): boolean {
  const tr = cond.trim();

  // Handle IS NULL / IS NOT NULL
  const nullMatch = tr.match(/^(.+?)\s+IS\s+(NOT\s+)?NULL$/i);
  if (nullMatch) {
    const val = evaluateCaseExpr(nullMatch[1], row, params, pIdx);
    return nullMatch[2] ? val !== null : val === null;
  }

  // Handle comparison operators
  const compMatch = tr.match(/^(.+?)\s*(>=|<=|<>|!=|>|<|=)\s*(.+)$/);
  if (compMatch) {
    const l = evaluateCaseExpr(compMatch[1], row, params, pIdx);
    const r = evaluateCaseExpr(compMatch[3], row, params, pIdx);
    if (l === null || r === null) return false;

    const op = compMatch[2];
    if (op === '=') return l === r || Number(l) === Number(r);
    if (op === '<>' || op === '!=') return l !== r && Number(l) !== Number(r);
    if (op === '>') return Number(l) > Number(r);
    if (op === '<') return Number(l) < Number(r);
    if (op === '>=') return Number(l) >= Number(r);
    if (op === '<=') return Number(l) <= Number(r);
  }

  const val = evaluateCaseExpr(tr, row, params, pIdx);
  return val !== null && val !== 0 && val !== false && val !== '';
}

function evalCaseExpr(
  ce: CaseExpression,
  row: Record<string, SqlValue>,
  params: SqlValue[],
  pIdx: {value: number}
): SqlValue {
  if (ce.type === 'searched') {
    for (const w of ce.whenClauses) {
      if (evalCond(w.condition, row, params, pIdx)) {
        return evaluateCaseExpr(w.result, row, params, pIdx);
      }
    }
  } else {
    const opVal = evaluateCaseExpr(ce.operand!, row, params, pIdx);
    for (const w of ce.whenClauses) {
      const wVal = evaluateCaseExpr(w.condition, row, params, pIdx);
      if (opVal === null || wVal === null) continue;
      if (opVal === wVal || Number(opVal) === Number(wVal)) {
        return evaluateCaseExpr(w.result, row, params, pIdx);
      }
    }
  }

  return ce.elseResult !== undefined ? evaluateCaseExpr(ce.elseResult, row, params, pIdx) : null;
}

// =============================================================================
// UTILITIES
// =============================================================================

export function splitSelectColumns(colList: string): string[] {
  const cols: string[] = [];
  let cur = '', depth = 0, inStr = false, pDepth = 0;

  for (let i = 0; i < colList.length; i++) {
    const c = colList[i];
    const rem = colList.slice(i);
    const urem = rem.toUpperCase();

    if (c === "'" && !inStr) { inStr = true; cur += c; continue; }
    if (c === "'" && inStr) {
      if (colList[i+1] === "'") { cur += "''"; i++; continue; }
      inStr = false; cur += c; continue;
    }
    if (inStr) { cur += c; continue; }

    if (c === '(') { pDepth++; cur += c; continue; }
    if (c === ')') { pDepth--; cur += c; continue; }

    if (urem.match(/^CASE\b/)) { depth++; cur += rem.slice(0, 4); i += 3; continue; }
    if (urem.match(/^END\b/)) { depth--; cur += rem.slice(0, 3); i += 2; continue; }

    if (c === ',' && depth === 0 && pDepth === 0) {
      cols.push(cur.trim());
      cur = '';
      continue;
    }
    cur += c;
  }

  if (cur.trim()) cols.push(cur.trim());
  return cols;
}

export function containsCaseExpression(s: string): boolean {
  return /\bCASE\b/i.test(s);
}

/**
 * Find keyword position outside CASE...END, string literals, and parentheses
 */
export function findKeywordOutsideCaseAndStrings(sql: string, startPos: number, keyword: string): number {
  let caseDepth = 0;
  let parenDepth = 0;
  let inString = false;
  const upperSql = sql.toUpperCase();
  const upperKeyword = keyword.toUpperCase();

  for (let i = startPos; i < sql.length; i++) {
    const ch = sql[i];

    if (ch === "'" && !inString) { inString = true; continue; }
    if (ch === "'" && inString) {
      if (sql[i + 1] === "'") { i++; continue; }
      inString = false; continue;
    }
    if (inString) continue;

    // Track parenthesis depth for subqueries
    if (ch === '(') { parenDepth++; continue; }
    if (ch === ')') { parenDepth--; continue; }

    if (upperSql.slice(i).startsWith('CASE') && (i + 4 >= sql.length || !/\w/.test(sql[i + 4]))) {
      caseDepth++; i += 3; continue;
    }
    if (upperSql.slice(i).startsWith('END') && (i + 3 >= sql.length || !/\w/.test(sql[i + 3]))) {
      if (caseDepth > 0) caseDepth--;
      i += 2; continue;
    }

    // Only match keyword at depth 0 (outside parens, CASE, and strings)
    if (caseDepth === 0 && parenDepth === 0 && upperSql.slice(i).startsWith(upperKeyword)) {
      const afterPos = i + upperKeyword.length;
      if (afterPos >= sql.length || /\s/.test(sql[afterPos])) {
        return i;
      }
    }
  }
  return -1;
}

// =============================================================================
// SUBQUERY-AWARE EXPRESSION EVALUATION
// =============================================================================

/**
 * Check if an expression is a scalar subquery: (SELECT ...)
 */
export function isScalarSubqueryExpr(expr: string): boolean {
  const trimmed = expr.trim();
  return trimmed.startsWith('(') && trimmed.endsWith(')') &&
         /^\(\s*SELECT\b/i.test(trimmed);
}

/**
 * Check if expression contains a scalar subquery
 */
export function containsSubqueryExpr(expr: string): boolean {
  return /\(\s*SELECT\b/i.test(expr);
}

/**
 * Evaluate an expression with subquery support.
 * This is an enhanced version of evaluateCaseExpr that can handle scalar subqueries.
 */
export function evaluateExprWithSubqueries(
  expr: string,
  row: Record<string, SqlValue>,
  ctx: ExprEvalContext
): SqlValue {
  const tr = expr.trim();
  const utr = tr.toUpperCase();

  // Handle CASE expressions
  if (utr.startsWith('CASE')) {
    const p = parseCaseExpression(tr);
    if (p) return evalCaseExprWithSubqueries(p.caseExpr, row, ctx);
  }

  // Handle scalar subquery: (SELECT ...)
  if (isScalarSubqueryExpr(tr)) {
    if (!ctx.evaluateSubquery) {
      // Fallback: return null if no subquery evaluator provided
      return null;
    }
    const subquerySql = tr.slice(1, -1).trim(); // Remove outer parens
    return ctx.evaluateSubquery(subquerySql, ctx.params, row);
  }

  // Handle NOT IN expression: col NOT IN (val1, val2, ...)
  const notInMatch = tr.match(/^(\w+)\s+NOT\s+IN\s*\(([^)]*)\)$/i);
  if (notInMatch) {
    const colName = notInMatch[1];
    const valueList = notInMatch[2];
    const colVal = row[colName] ?? null;
    if (colVal === null) return null;
    const values = parseValueListForExpr(valueList, ctx.params, ctx.pIdx);
    const hasNull = values.some(v => v === null);
    if (values.length === 0) return 1;
    const inList = values.some(v => v !== null && valuesEqualForExpr(v, colVal));
    if (inList) return 0;
    if (hasNull) return null;
    return 1;
  }

  // Handle IN expression: col IN (val1, val2, ...)
  const inMatch = tr.match(/^(\w+)\s+IN\s*\(([^)]*)\)$/i);
  if (inMatch) {
    const colName = inMatch[1];
    const valueList = inMatch[2];
    const colVal = row[colName] ?? null;
    if (colVal === null) return null;
    const values = parseValueListForExpr(valueList, ctx.params, ctx.pIdx);
    if (values.length === 0) return 0;
    const inList = values.some(v => v !== null && valuesEqualForExpr(v, colVal));
    return inList ? 1 : 0;
  }

  if (tr === '?') return ctx.params[ctx.pIdx.value++];
  if (utr === 'NULL') return null;
  if (tr.startsWith("'") && tr.endsWith("'")) return tr.slice(1, -1).replace(/''/g, "'");
  if (/^-?\d+(\.\d+)?$/.test(tr)) return Number(tr);

  // Handle parenthesized expressions - check for subquery first
  if (tr.startsWith('(') && tr.endsWith(')')) {
    const inner = tr.slice(1, -1).trim();
    // Check if it's a subquery
    if (/^\s*SELECT\b/i.test(inner)) {
      if (!ctx.evaluateSubquery) return null;
      return ctx.evaluateSubquery(inner, ctx.params, row);
    }
    return evaluateExprWithSubqueries(inner, row, ctx);
  }

  if (/^\w+$/.test(tr)) return row[tr] ?? null;

  // Handle table.column references (e.g., t1.x)
  if (/^\w+\.\w+$/.test(tr)) return row[tr] ?? null;

  // Handle arithmetic/comparison expressions with subquery support
  return evalArithWithSubqueries(tr, row, ctx);
}

/**
 * Evaluate arithmetic expression with subquery support
 */
function evalArithWithSubqueries(
  expr: string,
  row: Record<string, SqlValue>,
  ctx: ExprEvalContext
): SqlValue {
  // If the expression contains a subquery, we need to handle it specially
  if (containsSubqueryExpr(expr)) {
    // Try to parse as a binary expression with subquery operand
    // Pattern: expr OP (SELECT ...)
    const binaryWithSubqueryMatch = expr.match(/^(.+?)\s*([-+*\/%])\s*(\(\s*SELECT\b.+\))$/i);
    if (binaryWithSubqueryMatch) {
      const leftExpr = binaryWithSubqueryMatch[1].trim();
      const op = binaryWithSubqueryMatch[2];
      const rightExpr = binaryWithSubqueryMatch[3].trim();

      const leftVal = evaluateExprWithSubqueries(leftExpr, row, ctx);
      const rightVal = evaluateExprWithSubqueries(rightExpr, row, ctx);

      if (leftVal === null || rightVal === null) return null;
      const numL = Number(leftVal), numR = Number(rightVal);
      if (isNaN(numL) || isNaN(numR)) return null;

      switch (op) {
        case '+': return numL + numR;
        case '-': return numL - numR;
        case '*': return numL * numR;
        case '/': return numR === 0 ? null : (Number.isInteger(numL) && Number.isInteger(numR) ? Math.trunc(numL / numR) : numL / numR);
        case '%': return numR === 0 ? null : numL % numR;
      }
    }

    // Pattern: (SELECT ...) OP expr
    const subqueryFirstMatch = expr.match(/^(\(\s*SELECT\b.+?\))\s*([-+*\/%])\s*(.+)$/i);
    if (subqueryFirstMatch) {
      const leftExpr = subqueryFirstMatch[1].trim();
      const op = subqueryFirstMatch[2];
      const rightExpr = subqueryFirstMatch[3].trim();

      const leftVal = evaluateExprWithSubqueries(leftExpr, row, ctx);
      const rightVal = evaluateExprWithSubqueries(rightExpr, row, ctx);

      if (leftVal === null || rightVal === null) return null;
      const numL = Number(leftVal), numR = Number(rightVal);
      if (isNaN(numL) || isNaN(numR)) return null;

      switch (op) {
        case '+': return numL + numR;
        case '-': return numL - numR;
        case '*': return numL * numR;
        case '/': return numR === 0 ? null : (Number.isInteger(numL) && Number.isInteger(numR) ? Math.trunc(numL / numR) : numL / numR);
        case '%': return numR === 0 ? null : numL % numR;
      }
    }
  }

  // Fall back to regular arithmetic parsing
  try {
    const tokens = tokenizeArith(expr);
    if (tokens.length === 0) return row[expr.trim()] ?? null;
    const ast = parseArithTokens(tokens);
    return evalArithAST(ast, row);
  } catch {
    return row[expr.trim()] ?? null;
  }
}

/**
 * Evaluate CASE expression condition with subquery support
 */
function evalCondWithSubqueries(
  cond: string,
  row: Record<string, SqlValue>,
  ctx: ExprEvalContext
): boolean {
  const tr = cond.trim();

  // Handle IS NULL / IS NOT NULL
  const nullMatch = tr.match(/^(.+?)\s+IS\s+(NOT\s+)?NULL$/i);
  if (nullMatch) {
    const val = evaluateExprWithSubqueries(nullMatch[1], row, ctx);
    return nullMatch[2] ? val !== null : val === null;
  }

  // Handle comparison operators with subquery support
  // Need to be careful to match subqueries properly
  const compMatch = parseComparisonWithSubquery(tr);
  if (compMatch) {
    const l = evaluateExprWithSubqueries(compMatch.left, row, ctx);
    const r = evaluateExprWithSubqueries(compMatch.right, row, ctx);

    if (l === null || r === null) return false;

    const op = compMatch.op;
    if (op === '=') return l === r || Number(l) === Number(r);
    if (op === '<>' || op === '!=') return l !== r && Number(l) !== Number(r);
    if (op === '>') return Number(l) > Number(r);
    if (op === '<') return Number(l) < Number(r);
    if (op === '>=') return Number(l) >= Number(r);
    if (op === '<=') return Number(l) <= Number(r);
  }

  const val = evaluateExprWithSubqueries(tr, row, ctx);
  return val !== null && val !== 0 && val !== false && val !== '';
}

/**
 * Parse a comparison expression that may contain subqueries
 */
function parseComparisonWithSubquery(expr: string): { left: string; op: string; right: string } | null {
  // Find comparison operators outside of parentheses
  let depth = 0;
  let inStr = false;

  for (let i = 0; i < expr.length; i++) {
    const ch = expr[i];

    if (ch === "'" && !inStr) { inStr = true; continue; }
    if (ch === "'" && inStr) {
      if (expr[i + 1] === "'") { i++; continue; }
      inStr = false; continue;
    }
    if (inStr) continue;

    if (ch === '(') { depth++; continue; }
    if (ch === ')') { depth--; continue; }

    if (depth === 0) {
      // Check for multi-char operators first
      const twoChar = expr.slice(i, i + 2);
      if (twoChar === '>=' || twoChar === '<=' || twoChar === '<>' || twoChar === '!=') {
        return {
          left: expr.slice(0, i).trim(),
          op: twoChar,
          right: expr.slice(i + 2).trim()
        };
      }
      // Single char operators
      if (ch === '>' || ch === '<' || ch === '=') {
        // Make sure it's not part of a multi-char operator
        const next = expr[i + 1];
        if ((ch === '>' || ch === '<') && next === '=') continue;
        if (ch === '<' && next === '>') continue;

        return {
          left: expr.slice(0, i).trim(),
          op: ch,
          right: expr.slice(i + 1).trim()
        };
      }
    }
  }
  return null;
}

/**
 * Evaluate CASE expression with subquery support
 */
function evalCaseExprWithSubqueries(
  ce: CaseExpression,
  row: Record<string, SqlValue>,
  ctx: ExprEvalContext
): SqlValue {
  if (ce.type === 'searched') {
    for (const w of ce.whenClauses) {
      if (evalCondWithSubqueries(w.condition, row, ctx)) {
        return evaluateExprWithSubqueries(w.result, row, ctx);
      }
    }
  } else {
    const opVal = evaluateExprWithSubqueries(ce.operand!, row, ctx);
    for (const w of ce.whenClauses) {
      const wVal = evaluateExprWithSubqueries(w.condition, row, ctx);
      if (opVal === null || wVal === null) continue;
      if (opVal === wVal || Number(opVal) === Number(wVal)) {
        return evaluateExprWithSubqueries(w.result, row, ctx);
      }
    }
  }

  return ce.elseResult !== undefined ? evaluateExprWithSubqueries(ce.elseResult, row, ctx) : null;
}
