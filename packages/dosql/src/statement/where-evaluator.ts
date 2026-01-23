/**
 * WHERE clause evaluator with support for complex conditions
 * Handles: AND, OR, NOT, IN, NOT IN, BETWEEN, comparisons
 */

import type { SqlValue } from './types.js';

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

  // Handle BETWEEN: col BETWEEN low AND high
  const betweenMatch = trimmed.match(/^(\w+(?:\.\w+)?)\s+BETWEEN\s+(.+?)\s+AND\s+(.+)$/i);
  if (betweenMatch) {
    const col = betweenMatch[1];
    const lowStr = betweenMatch[2].trim();
    const highStr = betweenMatch[3].trim();
    const colVal = deps.getColumnValue(row, col);
    if (colVal === null) return false;

    const low = parseConditionValue(lowStr, params, pIdx);
    const high = parseConditionValue(highStr, params, pIdx);
    if (low === null || high === null) return false;

    const numVal = Number(colVal);
    const numLow = Number(low);
    const numHigh = Number(high);
    return numVal >= numLow && numVal <= numHigh;
  }

  // Handle literal NOT IN: 1 NOT IN (2, 3)
  const literalNotInMatch = trimmed.match(/^(-?\d+(?:\.\d+)?|'[^']*')\s+NOT\s+IN\s*\(([^)]*)\)$/i);
  if (literalNotInMatch) {
    const literalStr = literalNotInMatch[1];
    const valueList = literalNotInMatch[2];
    const literalValue: SqlValue = literalStr.startsWith("'")
      ? literalStr.slice(1, -1)
      : Number(literalStr);
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
    return !values.some(v => deps.valuesEqual(v, literalValue));
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

  // Handle comparison: col op val
  const compMatch = trimmed.match(/^(\w+(?:\.\w+)?)\s*(>=|<=|<>|!=|>|<|=|LIKE)\s*(.+)$/i);
  if (compMatch) {
    const col = compMatch[1];
    const op = compMatch[2].toUpperCase();
    const rightStr = compMatch[3].trim();

    const colVal = deps.getColumnValue(row, col);

    // Check if right side is a column reference
    if (/^[a-zA-Z_]\w*\.[a-zA-Z_]\w*$/.test(rightStr)) {
      const rightVal = deps.getColumnValue(row, rightStr);
      return compareValues(colVal, rightVal, op, deps);
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
    case '>': return Number(left) > Number(right);
    case '<': return Number(left) < Number(right);
    case '>=': return Number(left) >= Number(right);
    case '<=': return Number(left) <= Number(right);
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
