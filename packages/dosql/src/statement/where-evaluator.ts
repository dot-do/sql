/**
 * WHERE clause evaluator with support for complex conditions
 * Handles: AND, OR, NOT, IN, NOT IN, BETWEEN, comparisons, CASE expressions
 */

import type { SqlValue } from './types.js';
import { evaluateCaseExpr, containsCaseExpression } from './case-expr.js';

type ParseValueListFn = (valueList: string, params: SqlValue[], startParamIndex: number) => { values: SqlValue[]; paramIndex: number };
type ValuesEqualFn = (a: SqlValue, b: SqlValue) => boolean;
type GetColumnValueFn = (row: Record<string, SqlValue>, colRef: string) => SqlValue;

export interface WhereEvaluatorDeps {
  parseValueList: ParseValueListFn;
  valuesEqual: ValuesEqualFn;
  getColumnValue: GetColumnValueFn;
}

/**
 * Evaluate a WHERE condition for a single row
 */
export function evaluateWhereCondition(
  condition: string,
  row: Record<string, SqlValue>,
  params: SqlValue[],
  pIdx: { value: number },
  deps: WhereEvaluatorDeps
): boolean {
  const trimmed = condition.trim();

  // Handle parenthesized expression: NOT (...)
  const notParenMatch = trimmed.match(/^NOT\s*\((.+)\)$/is);
  if (notParenMatch) {
    return !evaluateWhereCondition(notParenMatch[1], row, params, pIdx, deps);
  }

  // Handle parenthesized expression: (...)
  if (trimmed.startsWith('(') && trimmed.endsWith(')')) {
    let depth = 0;
    let isBalanced = true;
    for (let i = 0; i < trimmed.length; i++) {
      if (trimmed[i] === '(') depth++;
      else if (trimmed[i] === ')') depth--;
      if (depth === 0 && i < trimmed.length - 1) {
        isBalanced = false;
        break;
      }
    }
    if (isBalanced) {
      return evaluateWhereCondition(trimmed.slice(1, -1), row, params, pIdx, deps);
    }
  }

  // Handle OR (lower precedence than AND)
  const orSplit = splitByLogicalOp(trimmed, 'OR');
  if (orSplit.length > 1) {
    for (const part of orSplit) {
      if (evaluateWhereCondition(part, row, params, pIdx, deps)) {
        return true;
      }
    }
    return false;
  }

  // Handle AND - but be careful with BETWEEN which uses AND
  const andSplit = splitByLogicalOpNotBetween(trimmed, 'AND');
  if (andSplit.length > 1) {
    for (const part of andSplit) {
      if (!evaluateWhereCondition(part, row, params, pIdx, deps)) {
        return false;
      }
    }
    return true;
  }

  // Handle NOT BETWEEN: col NOT BETWEEN low AND high
  const notBetweenMatch = trimmed.match(/^(.+?)\s+NOT\s+BETWEEN\s+(.+?)\s+AND\s+(.+)$/i);
  if (notBetweenMatch) {
    const exprStr = notBetweenMatch[1].trim();
    const lowStr = notBetweenMatch[2].trim();
    const highStr = notBetweenMatch[3].trim();
    const exprVal = evaluateSimpleExpr(exprStr, row, params, pIdx, deps);
    if (exprVal === null) return false;

    const low = evaluateSimpleExpr(lowStr, row, params, pIdx, deps);
    const high = evaluateSimpleExpr(highStr, row, params, pIdx, deps);
    if (low === null || high === null) return false;

    const numVal = Number(exprVal);
    const numLow = Number(low);
    const numHigh = Number(high);
    // NOT BETWEEN means outside the range
    return numVal < numLow || numVal > numHigh;
  }

  // Handle BETWEEN: expr BETWEEN low AND high (supports expressions like col, b-2, d+2)
  const betweenMatch = trimmed.match(/^(.+?)\s+BETWEEN\s+(.+?)\s+AND\s+(.+)$/i);
  if (betweenMatch) {
    const exprStr = betweenMatch[1].trim();
    const lowStr = betweenMatch[2].trim();
    const highStr = betweenMatch[3].trim();
    const exprVal = evaluateSimpleExpr(exprStr, row, params, pIdx, deps);
    if (exprVal === null) return false;

    const low = evaluateSimpleExpr(lowStr, row, params, pIdx, deps);
    const high = evaluateSimpleExpr(highStr, row, params, pIdx, deps);
    if (low === null || high === null) return false;

    const numVal = Number(exprVal);
    const numLow = Number(low);
    const numHigh = Number(high);
    return numVal >= numLow && numVal <= numHigh;
  }

  // Handle literal NOT IN: 1 NOT IN (2, 3), NULL NOT IN ()
  // Now includes NULL as a valid literal
  const literalNotInMatch = trimmed.match(/^(-?\d+(?:\.\d+)?|'[^']*'|NULL)\s+NOT\s+IN\s*\(([^)]*)\)$/i);
  if (literalNotInMatch) {
    const literalStr = literalNotInMatch[1];
    const valueList = literalNotInMatch[2];
    // Parse the literal value, handling NULL, strings, and numbers
    let literalValue: SqlValue;
    if (literalStr.toUpperCase() === 'NULL') {
      literalValue = null;
    } else if (literalStr.startsWith("'")) {
      literalValue = literalStr.slice(1, -1);
    } else {
      literalValue = Number(literalStr);
    }
    const values: SqlValue[] = [];
    let hasNull = false;
    if (valueList.trim()) {
      const items = deps.parseValueList(valueList, params, pIdx.value);
      for (const item of items.values) {
        if (item === null) hasNull = true;
        values.push(item);
      }
      pIdx.value = items.paramIndex;
    }
    // Per SQL standard (R-52275-55503): When the right operand is an empty set,
    // NOT IN returns TRUE regardless of the left operand (even if NULL)
    if (values.length === 0) return true;
    // NULL NOT IN non-empty list returns NULL (falsy in WHERE)
    if (literalValue === null) return false;
    // If list contains NULL and value not found, result is NULL (falsy)
    if (hasNull) return false;
    return !values.some(v => deps.valuesEqual(v, literalValue));
  }

  // Handle literal IN: 1 IN (1, 2, 3), NULL IN (...)
  // Must come before column IN to catch numeric/string literals
  // Now includes NULL as a valid literal
  const literalInMatch = trimmed.match(/^(-?\d+(?:\.\d+)?|'[^']*'|NULL)\s+IN\s*\(([^)]*)\)$/i);
  if (literalInMatch) {
    const literalStr = literalInMatch[1];
    const valueList = literalInMatch[2];
    // Parse the literal value, handling NULL, strings, and numbers
    let literalValue: SqlValue;
    if (literalStr.toUpperCase() === 'NULL') {
      literalValue = null;
    } else if (literalStr.startsWith("'")) {
      literalValue = literalStr.slice(1, -1);
    } else {
      literalValue = Number(literalStr);
    }
    const values: SqlValue[] = [];
    if (valueList.trim()) {
      const items = deps.parseValueList(valueList, params, pIdx.value);
      values.push(...items.values);
      pIdx.value = items.paramIndex;
    }
    // Per SQL standard: Empty list: x IN () is always FALSE, even for NULL
    if (values.length === 0) return false;
    // NULL IN non-empty list returns NULL (falsy in WHERE)
    if (literalValue === null) return false;
    // If value is found, return TRUE even if NULL is in list
    // If value not found and NULL in list, return FALSE (NULL is falsy in WHERE)
    return values.some(v => deps.valuesEqual(v, literalValue));
  }

  // Handle column NOT IN: col NOT IN (1, 2, 3)
  const notInMatch = trimmed.match(/^(\w+(?:\.\w+)?)\s+NOT\s+IN\s*\(([^)]*)\)$/i);
  if (notInMatch) {
    const col = notInMatch[1];
    const valueList = notInMatch[2];
    const colVal = deps.getColumnValue(row, col);
    if (colVal === null) return false;

    const values: SqlValue[] = [];
    let hasNull = false;
    if (valueList.trim()) {
      const items = deps.parseValueList(valueList, params, pIdx.value);
      for (const item of items.values) {
        if (item === null) hasNull = true;
        values.push(item);
      }
      pIdx.value = items.paramIndex;
    }
    if (hasNull) return false;
    if (values.length === 0) return true;
    return !values.some(v => deps.valuesEqual(v, colVal));
  }

  // Handle column IN: col IN (1, 2, 3)
  const inMatch = trimmed.match(/^(\w+(?:\.\w+)?)\s+IN\s*\(([^)]*)\)$/i);
  if (inMatch) {
    const col = inMatch[1];
    const valueList = inMatch[2];
    const colVal = deps.getColumnValue(row, col);
    if (colVal === null) return false;

    const values: SqlValue[] = [];
    if (valueList.trim()) {
      const items = deps.parseValueList(valueList, params, pIdx.value);
      values.push(...items.values);
      pIdx.value = items.paramIndex;
    }
    if (values.length === 0) return false;
    return values.some(v => deps.valuesEqual(v, colVal));
  }

  // Handle IS NULL / IS NOT NULL
  const isNullMatch = trimmed.match(/^(\w+(?:\.\w+)?)\s+IS\s+(NOT\s+)?NULL$/i);
  if (isNullMatch) {
    const col = isNullMatch[1];
    const isNot = !!isNullMatch[2];
    const colVal = deps.getColumnValue(row, col);
    return isNot ? colVal !== null : colVal === null;
  }

  // Handle CASE expression in WHERE clause (e.g., CASE WHEN ... END = value)
  if (containsCaseExpression(trimmed)) {
    // Find comparison operator outside CASE...END blocks
    let caseDepth = 0;
    let inStr = false;
    let parenDep = 0;
    const upperTrimmed = trimmed.toUpperCase();

    for (let i = 0; i < trimmed.length; i++) {
      const ch = trimmed[i];
      if (ch === "'" && !inStr) { inStr = true; continue; }
      if (ch === "'" && inStr) {
        if (trimmed[i + 1] === "'") { i++; continue; }
        inStr = false; continue;
      }
      if (inStr) continue;
      if (ch === '(') { parenDep++; continue; }
      if (ch === ')') { parenDep--; continue; }
      if (upperTrimmed.slice(i).startsWith('CASE') && (i + 4 >= trimmed.length || !/\w/.test(trimmed[i + 4]))) {
        caseDepth++; i += 3; continue;
      }
      if (upperTrimmed.slice(i).startsWith('END') && (i + 3 >= trimmed.length || !/\w/.test(trimmed[i + 3]))) {
        if (caseDepth > 0) caseDepth--;
        i += 2; continue;
      }

      if (caseDepth === 0 && parenDep === 0) {
        const twoChar = trimmed.slice(i, i + 2);
        let op: string | null = null;
        let opLen = 0;
        if (twoChar === '>=' || twoChar === '<=' || twoChar === '<>' || twoChar === '!=') {
          op = twoChar; opLen = 2;
        } else if (ch === '>' || ch === '<' || ch === '=') {
          const next = trimmed[i + 1];
          if ((ch === '>' || ch === '<') && next === '=') continue;
          if (ch === '<' && next === '>') continue;
          op = ch; opLen = 1;
        }

        if (op) {
          const leftExpr = trimmed.slice(0, i).trim();
          const rightExpr = trimmed.slice(i + opLen).trim();
          if (containsCaseExpression(leftExpr)) {
            const pIdx2 = { value: pIdx.value };
            const leftVal = evaluateCaseExpr(leftExpr, row, params, pIdx2);
            const rightVal = parseConditionValue(rightExpr, params, pIdx);
            return compareValues(leftVal, rightVal, op.toUpperCase(), deps);
          }
        }
      }
    }

    // If we got here with a CASE expression but no comparison, evaluate as boolean
    const pIdx2 = { value: pIdx.value };
    const val = evaluateCaseExpr(trimmed, row, params, pIdx2);
    return val !== null && val !== 0 && val !== false && val !== '';
  }

  // Handle function call comparison: func(args) op val
  // This matches patterns like: coalesce(a,b,c)<>0, abs(b-c)>5
  const funcCompMatch = trimmed.match(/^(\w+\s*\([^)]*\))\s*(>=|<=|<>|!=|>|<|=)\s*(.+)$/i);
  if (funcCompMatch) {
    const funcExpr = funcCompMatch[1];
    const op = funcCompMatch[2].toUpperCase();
    const rightStr = funcCompMatch[3].trim();

    const pIdx2 = { value: pIdx.value };
    const leftVal = evaluateCaseExpr(funcExpr, row, params, pIdx2);
    const rightVal = parseConditionValue(rightStr, params, pIdx);

    return compareValues(leftVal, rightVal, op, deps);
  }

  // Handle comparison: col op val
  const compMatch = trimmed.match(/^(\w+(?:\.\w+)?)\s*(>=|<=|<>|!=|>|<|=|LIKE)\s*(.+)$/i);
  if (compMatch) {
    const col = compMatch[1];
    const op = compMatch[2].toUpperCase();
    const rightStr = compMatch[3].trim();

    const colVal = deps.getColumnValue(row, col);

    // Check if right side is a column reference (table.column or just column)
    if (/^[a-zA-Z_]\w*(\.[a-zA-Z_]\w*)?$/.test(rightStr)) {
      const rightVal = deps.getColumnValue(row, rightStr);
      // If the column exists in the row, use it; otherwise treat as literal
      if (rightVal !== null || rightStr in row) {
        return compareValues(colVal, rightVal, op, deps);
      }
    }

    // Check if right side is an arithmetic expression involving columns (e.g., b-2, b+2)
    const arithMatch = rightStr.match(/^([a-zA-Z_]\w*(?:\.\w+)?)\s*([+\-])\s*(\d+(?:\.\d+)?)$/);
    if (arithMatch) {
      const rightColName = arithMatch[1];
      const arithOp = arithMatch[2];
      const arithNum = Number(arithMatch[3]);
      const rightColVal = deps.getColumnValue(row, rightColName);
      if (rightColVal !== null) {
        const numVal = Number(rightColVal);
        const rightVal = arithOp === '+' ? numVal + arithNum : numVal - arithNum;
        return compareValues(colVal, rightVal, op, deps);
      }
    }

    const rightVal = parseConditionValue(rightStr, params, pIdx);
    return compareValues(colVal, rightVal, op, deps);
  }

  return false;
}

/**
 * Split a condition string by a logical operator, respecting parentheses and strings
 */
function splitByLogicalOp(condition: string, op: string): string[] {
  const parts: string[] = [];
  let current = '';
  let depth = 0;
  let inString = false;
  const upperCond = condition.toUpperCase();
  const upperOp = ` ${op} `;

  for (let i = 0; i < condition.length; i++) {
    const ch = condition[i];

    if (ch === "'" && !inString) { inString = true; current += ch; continue; }
    if (ch === "'" && inString) {
      if (condition[i + 1] === "'") { current += "''"; i++; continue; }
      inString = false; current += ch; continue;
    }
    if (inString) { current += ch; continue; }

    if (ch === '(') { depth++; current += ch; continue; }
    if (ch === ')') { depth--; current += ch; continue; }

    if (depth === 0) {
      const remaining = upperCond.slice(i);
      if (remaining.startsWith(upperOp)) {
        parts.push(current.trim());
        current = '';
        i += upperOp.length - 1;
        continue;
      }
    }

    current += ch;
  }

  if (current.trim()) {
    parts.push(current.trim());
  }

  return parts.length > 0 ? parts : [condition];
}

/**
 * Split by AND but not when it's part of BETWEEN ... AND
 */
function splitByLogicalOpNotBetween(condition: string, op: string): string[] {
  const parts: string[] = [];
  let current = '';
  let depth = 0;
  let inString = false;
  let inBetween = false;
  const upperCond = condition.toUpperCase();
  const upperOp = ` ${op} `;

  for (let i = 0; i < condition.length; i++) {
    const ch = condition[i];

    if (ch === "'" && !inString) { inString = true; current += ch; continue; }
    if (ch === "'" && inString) {
      if (condition[i + 1] === "'") { current += "''"; i++; continue; }
      inString = false; current += ch; continue;
    }
    if (inString) { current += ch; continue; }

    if (ch === '(') { depth++; current += ch; continue; }
    if (ch === ')') { depth--; current += ch; continue; }

    // Check for NOT BETWEEN keyword
    if (depth === 0 && upperCond.slice(i).startsWith(' NOT BETWEEN ')) {
      inBetween = true;
      current += condition.slice(i, i + 13);
      i += 12;
      continue;
    }

    // Check for BETWEEN keyword
    if (depth === 0 && upperCond.slice(i).startsWith(' BETWEEN ')) {
      inBetween = true;
      current += condition.slice(i, i + 9);
      i += 8;
      continue;
    }

    if (depth === 0) {
      const remaining = upperCond.slice(i);
      if (remaining.startsWith(upperOp)) {
        if (inBetween) {
          inBetween = false;
          current += condition.slice(i, i + upperOp.length);
          i += upperOp.length - 1;
          continue;
        }
        parts.push(current.trim());
        current = '';
        i += upperOp.length - 1;
        continue;
      }
    }

    current += ch;
  }

  if (current.trim()) {
    parts.push(current.trim());
  }

  return parts.length > 0 ? parts : [condition];
}

/**
 * Parse a value from a condition string
 */
function parseConditionValue(str: string, params: SqlValue[], pIdx: { value: number }): SqlValue {
  const trimmed = str.trim();
  if (trimmed === '?') return params[pIdx.value++];
  if (trimmed.startsWith("'") && trimmed.endsWith("'")) return trimmed.slice(1, -1).replace(/''/g, "'");
  if (trimmed.toUpperCase() === 'NULL') return null;
  if (!isNaN(Number(trimmed))) return Number(trimmed);
  return trimmed;
}

/**
 * Evaluate a simple expression that can be:
 * - A column reference (col or alias.col)
 * - A literal value (number, string, NULL)
 * - A parameter (?)
 * - A simple arithmetic expression (col+N, col-N, N+col, N-col)
 */
function evaluateSimpleExpr(
  str: string,
  row: Record<string, SqlValue>,
  params: SqlValue[],
  pIdx: { value: number },
  deps: WhereEvaluatorDeps
): SqlValue {
  const trimmed = str.trim();

  // Parameter
  if (trimmed === '?') {
    return params[pIdx.value++];
  }

  // String literal
  if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
    return trimmed.slice(1, -1).replace(/''/g, "'");
  }

  // NULL
  if (trimmed.toUpperCase() === 'NULL') {
    return null;
  }

  // Pure numeric literal
  if (/^-?\d+(?:\.\d+)?$/.test(trimmed)) {
    return Number(trimmed);
  }

  // Simple column reference (no operators)
  if (/^[a-zA-Z_]\w*(?:\.[a-zA-Z_]\w*)?$/.test(trimmed)) {
    return deps.getColumnValue(row, trimmed);
  }

  // Handle addition: left + right
  const addMatch = trimmed.match(/^(.+?)\s*\+\s*(.+)$/);
  if (addMatch) {
    const leftVal = evaluateSimpleExpr(addMatch[1], row, params, pIdx, deps);
    const rightVal = evaluateSimpleExpr(addMatch[2], row, params, pIdx, deps);
    if (leftVal === null || rightVal === null) return null;
    return Number(leftVal) + Number(rightVal);
  }

  // Handle subtraction: left - right (be careful with negative numbers)
  // Match from the end to handle cases like "col-2" vs "-2"
  const subMatch = trimmed.match(/^(.+?)\s*-\s*(\d+(?:\.\d+)?|\w+(?:\.\w+)?)$/);
  if (subMatch) {
    const leftVal = evaluateSimpleExpr(subMatch[1], row, params, pIdx, deps);
    const rightVal = evaluateSimpleExpr(subMatch[2], row, params, pIdx, deps);
    if (leftVal === null || rightVal === null) return null;
    return Number(leftVal) - Number(rightVal);
  }

  // Handle multiplication: left * right
  const mulMatch = trimmed.match(/^(.+?)\s*\*\s*(.+)$/);
  if (mulMatch) {
    const leftVal = evaluateSimpleExpr(mulMatch[1], row, params, pIdx, deps);
    const rightVal = evaluateSimpleExpr(mulMatch[2], row, params, pIdx, deps);
    if (leftVal === null || rightVal === null) return null;
    return Number(leftVal) * Number(rightVal);
  }

  // Handle division: left / right
  const divMatch = trimmed.match(/^(.+?)\s*\/\s*(.+)$/);
  if (divMatch) {
    const leftVal = evaluateSimpleExpr(divMatch[1], row, params, pIdx, deps);
    const rightVal = evaluateSimpleExpr(divMatch[2], row, params, pIdx, deps);
    if (leftVal === null || rightVal === null) return null;
    const rightNum = Number(rightVal);
    if (rightNum === 0) return null;
    return Number(leftVal) / rightNum;
  }

  // Parenthesized expression
  if (trimmed.startsWith('(') && trimmed.endsWith(')')) {
    return evaluateSimpleExpr(trimmed.slice(1, -1), row, params, pIdx, deps);
  }

  // Fallback: try as column reference or literal
  const colVal = deps.getColumnValue(row, trimmed);
  if (colVal !== null && colVal !== undefined) {
    return colVal;
  }

  // If still not matched, try parsing as number
  const num = Number(trimmed);
  if (!isNaN(num)) {
    return num;
  }

  return null;
}

/**
 * Compare two values with an operator
 */
function compareValues(left: SqlValue, right: SqlValue, op: string, deps: WhereEvaluatorDeps): boolean {
  if (left === null || right === null) {
    return false;
  }

  switch (op) {
    case '=': return deps.valuesEqual(left, right);
    case '<>':
    case '!=': return !deps.valuesEqual(left, right);
    case '>': {
      if (typeof left === 'string' && typeof right === 'string') return left > right;
      return Number(left) > Number(right);
    }
    case '<': {
      if (typeof left === 'string' && typeof right === 'string') return left < right;
      return Number(left) < Number(right);
    }
    case '>=': {
      if (typeof left === 'string' && typeof right === 'string') return left >= right;
      return Number(left) >= Number(right);
    }
    case '<=': {
      if (typeof left === 'string' && typeof right === 'string') return left <= right;
      return Number(left) <= Number(right);
    }
    case 'LIKE':
      if (typeof left === 'string' && typeof right === 'string') {
        const regex = new RegExp(
          '^' + right.replace(/%/g, '.*').replace(/_/g, '.') + '$',
          'i'
        );
        return regex.test(left);
      }
      return false;
    default: return false;
  }
}
