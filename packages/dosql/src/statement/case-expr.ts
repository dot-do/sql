/**
 * CASE Expression Support for DoSQL
 *
 * Provides parsing and evaluation of SQL CASE expressions.
 */

import type { SqlValue } from './types.js';

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

  if (tr === '?') return params[pIdx.value++];
  if (utr === 'NULL') return null;
  if (tr.startsWith("'") && tr.endsWith("'")) return tr.slice(1, -1).replace(/''/g, "'");
  if (/^-?\d+(\.\d+)?$/.test(tr)) return Number(tr);
  if (tr.startsWith('(') && tr.endsWith(')')) return evaluateCaseExpr(tr.slice(1, -1), row, params, pIdx);
  if (/^\w+$/.test(tr)) return row[tr] ?? null;

  return evalArith(tr, row, params, pIdx);
}

/**
 * Token types for expression lexer
 */
interface ArithToken {
  type: 'number' | 'identifier' | 'operator' | 'lparen' | 'rparen' | 'comma' | 'null';
  value: string;
}

/**
 * Expression AST node types
 */
type ArithExprNode =
  | { type: 'number'; value: number }
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
        default: return null;
      }
    }
  }
  return null;
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
