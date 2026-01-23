/**
 * Subquery Support for DoSQL
 *
 * Provides subquery parsing and execution support for:
 * - Scalar subqueries in SELECT: (SELECT max(a) FROM t2)
 * - Scalar subqueries in WHERE: a > (SELECT avg(a) FROM t1)
 * - EXISTS/NOT EXISTS: WHERE EXISTS (SELECT 1 FROM t2)
 * - IN with subquery: WHERE a IN (SELECT b FROM t2)
 * - Correlated subqueries: (SELECT count(*) FROM t2 WHERE t2.a < t1.a)
 * - CASE expressions with subqueries
 */

import type { SqlValue } from './types.js';

// =============================================================================
// SUBQUERY PARSING
// =============================================================================

/**
 * Find a keyword in SQL, respecting nested parentheses
 */
export function findKeywordRespectingParens(sql: string, keyword: string, startIndex: number): number {
  let depth = 0;
  const upperSql = sql.toUpperCase();
  const upperKeyword = keyword.toUpperCase();

  for (let i = startIndex; i <= sql.length - keyword.length; i++) {
    const char = sql[i];
    if (char === '(') depth++;
    else if (char === ')') depth--;
    else if (depth === 0) {
      if (upperSql.slice(i, i + keyword.length) === upperKeyword) {
        const before = i > 0 ? sql[i - 1] : ' ';
        const after = i + keyword.length < sql.length ? sql[i + keyword.length] : ' ';
        if (/[\s(]/.test(before) && /[\s(,)]/.test(after)) {
          return i;
        }
      }
    }
  }
  return -1;
}

/**
 * Find the first occurrence of any keyword from a list
 */
export function findFirstKeyword(sql: string, keywords: string[]): number {
  let first = -1;
  for (const kw of keywords) {
    const idx = findKeywordRespectingParens(sql, kw, 0);
    if (idx !== -1 && (first === -1 || idx < first)) {
      first = idx;
    }
  }
  return first;
}

/**
 * Parse a SELECT query, handling nested parentheses for subqueries
 */
export function parseSelectWithSubqueries(sql: string): {
  columnList: string;
  fromClause: string;
  whereClause?: string;
  groupBy?: string;
  having?: string;
  orderBy?: string;
  limit?: number;
} | null {
  const upperSql = sql.toUpperCase();
  const selectIndex = upperSql.indexOf('SELECT');
  if (selectIndex === -1) return null;

  // Find FROM clause (accounting for nested selects)
  const fromIndex = findKeywordRespectingParens(sql, 'FROM', selectIndex + 6);
  if (fromIndex === -1) return null;

  const columnList = sql.slice(selectIndex + 6, fromIndex).trim();

  // Find the end of FROM clause (next major keyword at depth 0)
  let rest = sql.slice(fromIndex + 4);
  const whereIndex = findKeywordRespectingParens(rest, 'WHERE', 0);
  const groupIndex = findKeywordRespectingParens(rest, 'GROUP', 0);
  const havingIndex = findKeywordRespectingParens(rest, 'HAVING', 0);
  const orderIndex = findKeywordRespectingParens(rest, 'ORDER', 0);
  const limitIndex = findKeywordRespectingParens(rest, 'LIMIT', 0);

  // Find the earliest keyword to determine FROM clause end
  let fromEnd = rest.length;
  for (const idx of [whereIndex, groupIndex, havingIndex, orderIndex, limitIndex]) {
    if (idx !== -1 && idx < fromEnd) fromEnd = idx;
  }

  const fromClause = rest.slice(0, fromEnd).trim();
  rest = rest.slice(fromEnd);

  // Parse WHERE clause
  let whereClause: string | undefined;
  if (whereIndex !== -1 && whereIndex === fromEnd) {
    rest = rest.slice(5).trim(); // Skip "WHERE"
    const nextKeyword = findFirstKeyword(rest, ['GROUP', 'HAVING', 'ORDER', 'LIMIT']);
    whereClause = nextKeyword === -1 ? rest.trim() : rest.slice(0, nextKeyword).trim();
    if (nextKeyword !== -1) rest = rest.slice(nextKeyword);
  }

  // Parse GROUP BY clause
  let groupBy: string | undefined;
  const gbIdx = findKeywordRespectingParens(rest, 'GROUP', 0);
  if (gbIdx !== -1) {
    rest = rest.slice(gbIdx);
    const byIdx = findKeywordRespectingParens(rest, 'BY', 0);
    if (byIdx !== -1) {
      rest = rest.slice(byIdx + 2).trim();
      const nextKeyword = findFirstKeyword(rest, ['HAVING', 'ORDER', 'LIMIT']);
      groupBy = nextKeyword === -1 ? rest.trim() : rest.slice(0, nextKeyword).trim();
      if (nextKeyword !== -1) rest = rest.slice(nextKeyword);
    }
  }

  // Parse HAVING clause
  let having: string | undefined;
  const havIdx = findKeywordRespectingParens(rest, 'HAVING', 0);
  if (havIdx !== -1) {
    rest = rest.slice(havIdx + 6).trim();
    const nextKeyword = findFirstKeyword(rest, ['ORDER', 'LIMIT']);
    having = nextKeyword === -1 ? rest.trim() : rest.slice(0, nextKeyword).trim();
    if (nextKeyword !== -1) rest = rest.slice(nextKeyword);
  }

  // Parse ORDER BY clause
  let orderBy: string | undefined;
  const ordIdx = findKeywordRespectingParens(rest, 'ORDER', 0);
  if (ordIdx !== -1) {
    rest = rest.slice(ordIdx);
    const byIdx = findKeywordRespectingParens(rest, 'BY', 0);
    if (byIdx !== -1) {
      rest = rest.slice(byIdx + 2).trim();
      const nextKeyword = findFirstKeyword(rest, ['LIMIT']);
      orderBy = nextKeyword === -1 ? rest.trim() : rest.slice(0, nextKeyword).trim();
      if (nextKeyword !== -1) rest = rest.slice(nextKeyword);
    }
  }

  // Parse LIMIT clause
  let limit: number | undefined;
  const limIdx = findKeywordRespectingParens(rest, 'LIMIT', 0);
  if (limIdx !== -1) {
    const limMatch = rest.slice(limIdx + 5).match(/^\s*(\d+)/);
    if (limMatch) {
      limit = parseInt(limMatch[1], 10);
    }
  }

  return { columnList, fromClause, whereClause, groupBy, having, orderBy, limit };
}

/**
 * Split by keyword respecting parentheses
 */
export function splitByKeywordRespectingParens(str: string, keyword: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let current = '';
  const upperStr = str.toUpperCase();
  const upperKeyword = keyword.toUpperCase();

  for (let i = 0; i < str.length; i++) {
    const char = str[i];
    if (char === '(') depth++;
    else if (char === ')') depth--;

    if (depth === 0) {
      // Check for keyword with word boundaries
      if (upperStr.slice(i).startsWith(upperKeyword)) {
        const prevChar = i > 0 ? str[i - 1] : ' ';
        const nextChar = str[i + keyword.length] || ' ';
        if (/\s/.test(prevChar) && /\s/.test(nextChar)) {
          if (current.trim()) parts.push(current.trim());
          current = '';
          i += keyword.length;
          continue;
        }
      }
    }
    current += char;
  }

  if (current.trim()) {
    parts.push(current.trim());
  }

  return parts.length > 1 ? parts : [str];
}

/**
 * Split by operator respecting parentheses
 */
export function splitByOperatorRespectingParens(str: string, op: string): string[] {
  let depth = 0;
  for (let i = 0; i <= str.length - op.length; i++) {
    const char = str[i];
    if (char === '(') depth++;
    else if (char === ')') depth--;
    else if (depth === 0 && str.slice(i, i + op.length) === op) {
      // Make sure it's not part of a larger operator
      const nextChar = str[i + op.length] || '';
      const prevChar = str[i - 1] || '';
      if (op === '>' && (nextChar === '=' || prevChar === '<')) continue;
      if (op === '<' && (nextChar === '=' || nextChar === '>' || prevChar === '>')) continue;
      if (op === '=' && (prevChar === '>' || prevChar === '<' || prevChar === '!' || nextChar === '=')) continue;

      return [str.slice(0, i), str.slice(i + op.length)];
    }
  }
  return [str];
}

/**
 * Parse column list with subquery support
 */
export function parseColumnListWithSubqueries(columnList: string): Array<{ expr: string; alias: string }> {
  const columns: Array<{ expr: string; alias: string }> = [];
  let depth = 0;
  let current = '';

  for (let i = 0; i < columnList.length; i++) {
    const char = columnList[i];
    if (char === '(') depth++;
    else if (char === ')') depth--;
    else if (char === ',' && depth === 0) {
      if (current.trim()) {
        columns.push(parseColumnExprWithSubquery(current.trim()));
      }
      current = '';
      continue;
    }
    current += char;
  }

  if (current.trim()) {
    columns.push(parseColumnExprWithSubquery(current.trim()));
  }

  return columns;
}

/**
 * Parse a single column expression with subquery support
 */
export function parseColumnExprWithSubquery(col: string): { expr: string; alias: string } {
  // Find AS keyword respecting parentheses
  const upperCol = col.toUpperCase();
  let asIndex = -1;
  let depth = 0;

  for (let i = 0; i < col.length - 3; i++) {
    const char = col[i];
    if (char === '(') depth++;
    else if (char === ')') depth--;
    else if (depth === 0 && upperCol.slice(i, i + 4) === ' AS ') {
      asIndex = i;
      break;
    }
  }

  if (asIndex !== -1) {
    return {
      expr: col.slice(0, asIndex).trim(),
      alias: col.slice(asIndex + 4).trim()
    };
  }

  // No alias - derive from expression
  let alias = col;
  if (col.includes('.') && !col.includes('(')) {
    alias = col.split('.').pop() || col;
  }

  return { expr: col, alias };
}

// =============================================================================
// SUBQUERY DETECTION
// =============================================================================

/**
 * Check if an expression contains a subquery
 */
export function containsSubquery(expr: string): boolean {
  return expr.toUpperCase().includes('SELECT') && expr.includes('(');
}

/**
 * Check if expression is a scalar subquery: (SELECT ...)
 */
export function isScalarSubquery(expr: string): boolean {
  const trimmed = expr.trim();
  return trimmed.startsWith('(') && trimmed.endsWith(')') &&
         trimmed.toUpperCase().includes('SELECT');
}

/**
 * Extract the SQL from a scalar subquery
 */
export function extractSubquerySql(expr: string): string {
  const trimmed = expr.trim();
  return trimmed.slice(1, -1).trim();
}

/**
 * Check if WHERE clause contains EXISTS
 */
export function isExistsCondition(whereClause: string): { isExists: boolean; isNot: boolean; subquery: string } | null {
  const trimmed = whereClause.trim();

  // NOT EXISTS
  const notExistsMatch = trimmed.match(/^\s*NOT\s+EXISTS\s*\((.+)\)\s*$/is);
  if (notExistsMatch) {
    return { isExists: true, isNot: true, subquery: notExistsMatch[1].trim() };
  }

  // EXISTS
  const existsMatch = trimmed.match(/^\s*EXISTS\s*\((.+)\)\s*$/is);
  if (existsMatch) {
    return { isExists: true, isNot: false, subquery: existsMatch[1].trim() };
  }

  return null;
}

/**
 * Check if WHERE clause contains IN with subquery
 */
export function isInSubqueryCondition(whereClause: string): { isIn: boolean; isNot: boolean; column: string; subquery: string } | null {
  const trimmed = whereClause.trim();

  // NOT IN with subquery
  const notInMatch = trimmed.match(/^(\w+(?:\.\w+)?)\s+NOT\s+IN\s*\(\s*(SELECT.+)\s*\)$/is);
  if (notInMatch) {
    return { isIn: true, isNot: true, column: notInMatch[1], subquery: notInMatch[2].trim() };
  }

  // IN with subquery
  const inMatch = trimmed.match(/^(\w+(?:\.\w+)?)\s+IN\s*\(\s*(SELECT.+)\s*\)$/is);
  if (inMatch) {
    return { isIn: true, isNot: false, column: inMatch[1], subquery: inMatch[2].trim() };
  }

  return null;
}

// =============================================================================
// COLUMN RESOLUTION FOR SUBQUERIES
// =============================================================================

/**
 * Resolve column value for subquery context
 * Handles:
 * - Plain column names from current row
 * - Qualified table.column names
 * - Outer row references for correlated subqueries
 */
export function resolveColumnForSubquery(
  columnExpr: string,
  row: Record<string, SqlValue>,
  outerRow?: Record<string, SqlValue>,
  outerTableAlias?: string
): SqlValue {
  const trimmed = columnExpr.trim();

  // Try exact match first in current row
  if (row[trimmed] !== undefined) {
    return row[trimmed];
  }

  // Handle table.column notation
  if (trimmed.includes('.')) {
    const [tbl, col] = trimmed.split('.');

    // Check outer row reference for correlated subqueries
    if (outerRow && outerTableAlias && tbl === outerTableAlias) {
      return outerRow[col] ?? null;
    }

    // Try looking up the qualified name in current row
    if (row[trimmed] !== undefined) return row[trimmed];

    // Try just the column name from current row
    if (row[col] !== undefined) return row[col];
  }

  // Try outer row for correlated subqueries
  if (outerRow && outerRow[trimmed] !== undefined) {
    return outerRow[trimmed];
  }

  return null;
}

// =============================================================================
// VALUE COMPARISON WITH COERCION
// =============================================================================

/**
 * Compare values with type coercion (SQL semantics)
 */
export function compareValuesWithCoercion(left: SqlValue, right: SqlValue, op: string): boolean {
  if (left === null || right === null) return false;

  const numLeft = Number(left);
  const numRight = Number(right);

  switch (op) {
    case '=':
      return left === right || (!isNaN(numLeft) && !isNaN(numRight) && numLeft === numRight);
    case '!=':
    case '<>':
      return left !== right && (isNaN(numLeft) || isNaN(numRight) || numLeft !== numRight);
    case '>': return numLeft > numRight;
    case '<': return numLeft < numRight;
    case '>=': return numLeft >= numRight;
    case '<=': return numLeft <= numRight;
    default: return false;
  }
}

/**
 * Apply arithmetic operation with integer division
 */
export function applyArithmeticOperation(left: number, right: number, op: string): number {
  switch (op) {
    case '+': return left + right;
    case '-': return left - right;
    case '*': return left * right;
    case '/': return right !== 0 ? Math.trunc(left / right) : 0;
    default: return left;
  }
}
