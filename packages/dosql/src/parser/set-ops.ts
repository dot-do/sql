/**
 * DoSQL Set Operations Parser
 *
 * Parses SQL set operations: UNION, INTERSECT, EXCEPT (MINUS)
 * Supports:
 * - UNION / UNION ALL / UNION DISTINCT
 * - INTERSECT / INTERSECT ALL / INTERSECT DISTINCT
 * - EXCEPT / EXCEPT ALL / EXCEPT DISTINCT
 * - MINUS (alias for EXCEPT)
 * - Compound operations with proper precedence
 * - ORDER BY / LIMIT / OFFSET on compound results
 */

// =============================================================================
// TYPES
// =============================================================================

/**
 * Set operation types
 */
export type SetOperationType = 'UNION' | 'INTERSECT' | 'EXCEPT';

/**
 * Position in source SQL for error reporting
 */
export interface SourcePosition {
  start: number;
  end: number;
  line?: number;
  column?: number;
}

/**
 * A simple SELECT statement (without set operations)
 */
export interface SimpleSelectNode {
  type: 'simpleSelect';
  sql: string;
  columns?: string[];
  from?: string;
  where?: string;
  groupBy?: string[];
  having?: string;
  position?: SourcePosition;
}

/**
 * A set operation node combining two queries
 */
export interface SetOperationNode {
  type: 'setOperation';
  operator: SetOperationType;
  all: boolean;
  left: SelectNode;
  right: SelectNode;
  position?: SourcePosition;
}

/**
 * Either a simple SELECT or a set operation
 */
export type SelectNode = SimpleSelectNode | SetOperationNode;

/**
 * Order by direction
 */
export type SortDirection = 'ASC' | 'DESC';

/**
 * Nulls position
 */
export type NullsPosition = 'FIRST' | 'LAST';

/**
 * Order by item
 */
export interface OrderByItem {
  column?: string;
  columnIndex?: number;
  expression?: string;
  direction: SortDirection;
  nullsPosition?: NullsPosition;
}

/**
 * Operation in a compound SELECT
 */
export interface CompoundOperation {
  operator: SetOperationType;
  all: boolean;
  select: SelectNode;
  position?: SourcePosition;
}

/**
 * CTE definition
 */
export interface CTEDefinition {
  name: string;
  columns?: string[];
  query: string;
  recursive?: boolean;
}

/**
 * Compound SELECT statement
 */
export interface CompoundSelectStatement {
  type: 'compoundSelect';
  cte?: CTEDefinition[];
  base: SelectNode;
  operations: CompoundOperation[];
  orderBy?: OrderByItem[];
  limit?: number;
  offset?: number;
  position?: SourcePosition;
}

/**
 * Parse result for set operation
 */
export interface SetOperationParseSuccess {
  success: true;
  ast: SetOperationNode;
}

export interface SetOperationParseError {
  success: false;
  error: string;
  position?: SourcePosition;
}

export type SetOperationParseResult = SetOperationParseSuccess | SetOperationParseError;

/**
 * Parse result for compound SELECT
 */
export interface CompoundSelectParseSuccess {
  success: true;
  ast: CompoundSelectStatement;
}

export interface CompoundSelectParseError {
  success: false;
  error: string;
  position?: SourcePosition;
}

export type CompoundSelectParseResult = CompoundSelectParseSuccess | CompoundSelectParseError;

/**
 * Validation result for set operation compatibility
 */
export interface SetOpValidationResult {
  valid: boolean;
  error?: string;
  warnings?: string[];
}

// =============================================================================
// PARSER STATE
// =============================================================================

interface ParserState {
  input: string;
  position: number;
}

function createState(input: string): ParserState {
  return { input: input.trim(), position: 0 };
}

function remaining(state: ParserState): string {
  return state.input.slice(state.position);
}

function isEOF(state: ParserState): boolean {
  return state.position >= state.input.length;
}

function advance(state: ParserState, count: number): ParserState {
  return { ...state, position: state.position + count };
}

// =============================================================================
// BASIC PARSING UTILITIES
// =============================================================================

function skipWhitespace(state: ParserState): ParserState {
  const rest = remaining(state);
  const match = rest.match(/^\s+/);
  if (match) {
    return advance(state, match[0].length);
  }
  return state;
}

function matchKeyword(state: ParserState, keyword: string): ParserState | null {
  const rest = remaining(state);
  const regex = new RegExp(`^${keyword}(?=[\\s;,()\\[\\]|$])`, 'i');
  if (regex.test(rest)) {
    return advance(state, keyword.length);
  }
  if (rest.toUpperCase() === keyword.toUpperCase()) {
    return advance(state, keyword.length);
  }
  return null;
}

function matchExact(state: ParserState, str: string): ParserState | null {
  const rest = remaining(state);
  if (rest.startsWith(str)) {
    return advance(state, str.length);
  }
  return null;
}

function peekKeyword(state: ParserState, keyword: string): boolean {
  const rest = remaining(state);
  const regex = new RegExp(`^${keyword}(?=[\\s;,()\\[\\]|$])`, 'i');
  return regex.test(rest) || rest.toUpperCase() === keyword.toUpperCase();
}

// =============================================================================
// SET OPERATION DETECTION
// =============================================================================

/**
 * Set operation keywords with their normalized form
 */
const SET_OPERATION_KEYWORDS: Record<string, SetOperationType> = {
  'UNION': 'UNION',
  'INTERSECT': 'INTERSECT',
  'EXCEPT': 'EXCEPT',
  'MINUS': 'EXCEPT', // MINUS is an alias for EXCEPT
};

/**
 * Operator precedence (higher = binds tighter)
 * INTERSECT has higher precedence than UNION and EXCEPT
 */
const OPERATOR_PRECEDENCE: Record<SetOperationType, number> = {
  'INTERSECT': 2,
  'UNION': 1,
  'EXCEPT': 1,
};

/**
 * Check if position is at a set operation keyword
 */
function isAtSetOperator(state: ParserState): { operator: SetOperationType; all: boolean; length: number } | null {
  state = skipWhitespace(state);
  const rest = remaining(state);

  for (const [keyword, normalized] of Object.entries(SET_OPERATION_KEYWORDS)) {
    const regex = new RegExp(`^${keyword}(?:\\s+(ALL|DISTINCT))?(?=[\\s;,()\\[\\]|$])`, 'i');
    const match = rest.match(regex);
    if (match) {
      const modifier = match[1]?.toUpperCase();
      const all = modifier === 'ALL';
      return {
        operator: normalized,
        all,
        length: match[0].length,
      };
    }
  }
  return null;
}

/**
 * Find the position of a set operator at the top level (not inside parentheses)
 */
function findTopLevelSetOperator(sql: string): { position: number; operator: SetOperationType; all: boolean; length: number } | null {
  let depth = 0;
  let i = 0;

  // Track string literals
  let inString = false;
  let stringChar = '';

  while (i < sql.length) {
    const char = sql[i];

    // Handle string literals
    if (!inString && (char === "'" || char === '"')) {
      inString = true;
      stringChar = char;
      i++;
      continue;
    }
    if (inString) {
      if (char === stringChar) {
        // Check for escaped quote
        if (sql[i + 1] === stringChar) {
          i += 2;
          continue;
        }
        inString = false;
      }
      i++;
      continue;
    }

    // Track parentheses depth
    if (char === '(') {
      depth++;
      i++;
      continue;
    }
    if (char === ')') {
      depth--;
      i++;
      continue;
    }

    // Only look for operators at top level
    if (depth === 0) {
      const state = createState(sql.slice(i));
      const op = isAtSetOperator(state);
      if (op) {
        return {
          position: i,
          ...op,
        };
      }
    }

    i++;
  }

  return null;
}

// =============================================================================
// SELECT PARSING
// =============================================================================

/**
 * Parse a simple SELECT (without set operations)
 */
function parseSimpleSelect(sql: string): SimpleSelectNode {
  const trimmed = sql.trim().replace(/;+$/, '').trim();

  // Remove outer parentheses if present
  let processed = trimmed;
  while (processed.startsWith('(') && processed.endsWith(')')) {
    // Check if parentheses are balanced
    let depth = 0;
    let balanced = true;
    for (let i = 0; i < processed.length - 1; i++) {
      if (processed[i] === '(') depth++;
      if (processed[i] === ')') depth--;
      if (depth === 0) {
        balanced = false;
        break;
      }
    }
    if (balanced) {
      processed = processed.slice(1, -1).trim();
    } else {
      break;
    }
  }

  return {
    type: 'simpleSelect',
    sql: processed,
    position: { start: 0, end: sql.length },
  };
}

/**
 * Extract CTE (WITH clause) if present
 */
function extractCTE(sql: string): { cte: CTEDefinition[] | undefined; rest: string } {
  const trimmed = sql.trim();
  const withMatch = trimmed.match(/^WITH\s+(RECURSIVE\s+)?/i);

  if (!withMatch) {
    return { cte: undefined, rest: trimmed };
  }

  // Find where the main SELECT starts (after all CTEs)
  let pos = withMatch[0].length;
  const ctes: CTEDefinition[] = [];
  const isRecursive = !!withMatch[1];

  while (pos < trimmed.length) {
    // Parse CTE name
    const nameMatch = trimmed.slice(pos).match(/^([a-zA-Z_][a-zA-Z0-9_]*)\s*/);
    if (!nameMatch) break;

    const cteName = nameMatch[1];
    pos += nameMatch[0].length;

    // Optional column list
    let columns: string[] | undefined;
    if (trimmed[pos] === '(') {
      const closeParen = findMatchingParen(trimmed, pos);
      if (closeParen > 0) {
        columns = trimmed.slice(pos + 1, closeParen).split(',').map(c => c.trim());
        pos = closeParen + 1;
      }
    }

    // Skip whitespace and "AS"
    const asMatch = trimmed.slice(pos).match(/^\s*AS\s*/i);
    if (asMatch) {
      pos += asMatch[0].length;
    }

    // Parse CTE query (in parentheses)
    if (trimmed[pos] === '(') {
      const closeParen = findMatchingParen(trimmed, pos);
      if (closeParen > 0) {
        const query = trimmed.slice(pos + 1, closeParen);
        ctes.push({
          name: cteName,
          columns,
          query,
          recursive: isRecursive,
        });
        pos = closeParen + 1;
      }
    }

    // Check for comma (more CTEs) or SELECT (end of WITH)
    const afterCte = trimmed.slice(pos).match(/^\s*(,|SELECT)/i);
    if (afterCte) {
      if (afterCte[1] === ',') {
        pos += afterCte[0].length;
        continue;
      } else {
        // Found SELECT, this is the rest
        break;
      }
    } else {
      break;
    }
  }

  return {
    cte: ctes.length > 0 ? ctes : undefined,
    rest: trimmed.slice(pos).trim(),
  };
}

/**
 * Find matching closing parenthesis
 */
function findMatchingParen(sql: string, start: number): number {
  let depth = 0;
  let inString = false;
  let stringChar = '';

  for (let i = start; i < sql.length; i++) {
    const char = sql[i];

    if (!inString && (char === "'" || char === '"')) {
      inString = true;
      stringChar = char;
      continue;
    }
    if (inString) {
      if (char === stringChar) {
        if (sql[i + 1] === stringChar) {
          i++;
          continue;
        }
        inString = false;
      }
      continue;
    }

    if (char === '(') depth++;
    if (char === ')') {
      depth--;
      if (depth === 0) return i;
    }
  }

  return -1;
}

// =============================================================================
// SET OPERATION PARSING
// =============================================================================

/**
 * Parse a set operation (UNION, INTERSECT, EXCEPT) between two SELECTs
 */
export function parseSetOperation(sql: string): SetOperationParseResult {
  const trimmed = sql.trim().replace(/;+$/, '').trim();

  // Find the top-level set operator
  const op = findTopLevelSetOperator(trimmed);

  if (!op) {
    return {
      success: false,
      error: 'No set operation found in SQL',
      position: { start: 0, end: trimmed.length },
    };
  }

  // Split into left and right parts
  const leftSql = trimmed.slice(0, op.position).trim();
  const rightSql = trimmed.slice(op.position + op.length).trim();

  if (!leftSql) {
    return {
      success: false,
      error: 'Missing left operand for set operation',
      position: { start: 0, end: op.position },
    };
  }

  if (!rightSql) {
    return {
      success: false,
      error: 'Missing right operand for set operation',
      position: { start: op.position + op.length, end: trimmed.length },
    };
  }

  // Check if left side starts with SELECT (or parenthesized SELECT)
  const leftClean = leftSql.replace(/^\(+/, '').trim();
  if (!leftClean.match(/^(SELECT|VALUES|WITH)\s/i)) {
    return {
      success: false,
      error: 'Left operand must be a SELECT statement',
      position: { start: 0, end: op.position },
    };
  }

  // Parse left and right as potentially compound selects
  const leftOp = findTopLevelSetOperator(leftSql);
  const rightOp = findTopLevelSetOperator(rightSql);

  let left: SelectNode;
  let right: SelectNode;

  if (leftOp) {
    const leftResult = parseSetOperation(leftSql);
    if (!leftResult.success) {
      return leftResult;
    }
    left = leftResult.ast;
  } else {
    left = parseSimpleSelect(leftSql);
  }

  if (rightOp) {
    const rightResult = parseSetOperation(rightSql);
    if (!rightResult.success) {
      return rightResult;
    }
    right = rightResult.ast;
  } else {
    right = parseSimpleSelect(rightSql);
  }

  return {
    success: true,
    ast: {
      type: 'setOperation',
      operator: op.operator,
      all: op.all,
      left,
      right,
      position: {
        start: 0,
        end: trimmed.length,
      },
    },
  };
}

/**
 * Parse a compound SELECT with multiple set operations
 */
export function parseCompoundSelect(sql: string): CompoundSelectParseResult {
  const trimmed = sql.trim().replace(/;+$/, '').trim();

  // Extract CTE if present
  const { cte, rest } = extractCTE(trimmed);

  // Extract ORDER BY, LIMIT, OFFSET from the end
  const { query, orderBy, limit, offset } = extractTrailingClauses(rest);

  // Find all top-level set operations
  const operations: { position: number; operator: SetOperationType; all: boolean; length: number }[] = [];
  let searchStart = 0;

  while (true) {
    const op = findTopLevelSetOperator(query.slice(searchStart));
    if (!op) break;
    operations.push({
      ...op,
      position: searchStart + op.position,
    });
    searchStart = searchStart + op.position + op.length;
  }

  if (operations.length === 0) {
    // No set operations, just a simple SELECT
    return {
      success: true,
      ast: {
        type: 'compoundSelect',
        cte,
        base: parseSimpleSelect(query),
        operations: [],
        orderBy,
        limit,
        offset,
        position: { start: 0, end: trimmed.length },
      },
    };
  }

  // Build compound select from operations
  const compoundOps: CompoundOperation[] = [];
  let currentPos = 0;

  // The base is everything before the first operator
  const baseSql = query.slice(0, operations[0].position).trim();
  let base: SelectNode = parseSimpleSelect(baseSql);

  // Process each operation
  for (let i = 0; i < operations.length; i++) {
    const op = operations[i];
    const nextOp = operations[i + 1];
    const endPos = nextOp ? nextOp.position : query.length;
    const selectSql = query.slice(op.position + op.length, endPos).trim();

    compoundOps.push({
      operator: op.operator,
      all: op.all,
      select: parseSimpleSelect(selectSql),
      position: { start: op.position, end: endPos },
    });
  }

  return {
    success: true,
    ast: {
      type: 'compoundSelect',
      cte,
      base,
      operations: compoundOps,
      orderBy,
      limit,
      offset,
      position: { start: 0, end: trimmed.length },
    },
  };
}

/**
 * Extract ORDER BY, LIMIT, OFFSET from end of query
 */
function extractTrailingClauses(sql: string): {
  query: string;
  orderBy?: OrderByItem[];
  limit?: number;
  offset?: number;
} {
  let query = sql;
  let orderBy: OrderByItem[] | undefined;
  let limit: number | undefined;
  let offset: number | undefined;

  // Find ORDER BY (at top level, not in subquery)
  const orderByMatch = findTopLevelClause(query, 'ORDER BY');
  if (orderByMatch) {
    const orderBySql = query.slice(orderByMatch.position + 8); // 8 = 'ORDER BY'.length
    query = query.slice(0, orderByMatch.position).trim();
    const parsed = parseOrderBy(orderBySql);
    orderBy = parsed.items;

    // Check for LIMIT in ORDER BY portion
    if (parsed.remaining) {
      const limitMatch = parsed.remaining.match(/LIMIT\s+(\d+)(?:\s+OFFSET\s+(\d+))?/i);
      if (limitMatch) {
        limit = parseInt(limitMatch[1], 10);
        if (limitMatch[2]) {
          offset = parseInt(limitMatch[2], 10);
        }
      }
    }
  } else {
    // Check for LIMIT without ORDER BY
    const limitMatch = findTopLevelClause(query, 'LIMIT');
    if (limitMatch) {
      const limitSql = query.slice(limitMatch.position + 5); // 5 = 'LIMIT'.length
      query = query.slice(0, limitMatch.position).trim();

      const limitValMatch = limitSql.match(/^\s*(\d+)(?:\s+OFFSET\s+(\d+))?/i);
      if (limitValMatch) {
        limit = parseInt(limitValMatch[1], 10);
        if (limitValMatch[2]) {
          offset = parseInt(limitValMatch[2], 10);
        }
      }
    }
  }

  return { query, orderBy, limit, offset };
}

/**
 * Find a clause keyword at top level (not inside parentheses)
 */
function findTopLevelClause(sql: string, clause: string): { position: number } | null {
  let depth = 0;
  let inString = false;
  let stringChar = '';
  const upperSql = sql.toUpperCase();
  const upperClause = clause.toUpperCase();

  for (let i = 0; i < sql.length - clause.length; i++) {
    const char = sql[i];

    if (!inString && (char === "'" || char === '"')) {
      inString = true;
      stringChar = char;
      continue;
    }
    if (inString) {
      if (char === stringChar) {
        if (sql[i + 1] === stringChar) {
          i++;
          continue;
        }
        inString = false;
      }
      continue;
    }

    if (char === '(') {
      depth++;
      continue;
    }
    if (char === ')') {
      depth--;
      continue;
    }

    if (depth === 0 && upperSql.slice(i, i + clause.length) === upperClause) {
      // Make sure it's a word boundary
      const before = i > 0 ? sql[i - 1] : ' ';
      const after = sql[i + clause.length] || ' ';
      if (/\s/.test(before) && /\s/.test(after)) {
        return { position: i };
      }
    }
  }

  return null;
}

/**
 * Parse ORDER BY items
 */
function parseOrderBy(sql: string): { items: OrderByItem[]; remaining?: string } {
  const items: OrderByItem[] = [];

  // Find LIMIT to know where ORDER BY ends
  const limitMatch = sql.match(/\bLIMIT\b/i);
  const orderBySql = limitMatch ? sql.slice(0, limitMatch.index).trim() : sql.trim();
  const remaining = limitMatch ? sql.slice(limitMatch.index).trim() : undefined;

  // Split by comma (top level only)
  const parts = splitTopLevel(orderBySql, ',');

  for (const part of parts) {
    const trimmed = part.trim();
    if (!trimmed) continue;

    const item: OrderByItem = { direction: 'ASC' };

    // Check for NULLS FIRST/LAST
    const nullsMatch = trimmed.match(/\bNULLS\s+(FIRST|LAST)\s*$/i);
    let rest = trimmed;
    if (nullsMatch) {
      item.nullsPosition = nullsMatch[1].toUpperCase() as NullsPosition;
      rest = trimmed.slice(0, nullsMatch.index).trim();
    }

    // Check for ASC/DESC
    const dirMatch = rest.match(/\b(ASC|DESC)\s*$/i);
    if (dirMatch) {
      item.direction = dirMatch[1].toUpperCase() as SortDirection;
      rest = rest.slice(0, dirMatch.index).trim();
    }

    // Check if it's a column index
    const indexMatch = rest.match(/^(\d+)$/);
    if (indexMatch) {
      item.columnIndex = parseInt(indexMatch[1], 10);
    } else {
      item.column = rest;
    }

    items.push(item);
  }

  return { items, remaining };
}

/**
 * Split by delimiter at top level only
 */
function splitTopLevel(sql: string, delimiter: string): string[] {
  const parts: string[] = [];
  let current = '';
  let depth = 0;
  let inString = false;
  let stringChar = '';

  for (let i = 0; i < sql.length; i++) {
    const char = sql[i];

    if (!inString && (char === "'" || char === '"')) {
      inString = true;
      stringChar = char;
      current += char;
      continue;
    }
    if (inString) {
      current += char;
      if (char === stringChar) {
        if (sql[i + 1] === stringChar) {
          current += sql[i + 1];
          i++;
          continue;
        }
        inString = false;
      }
      continue;
    }

    if (char === '(') {
      depth++;
      current += char;
      continue;
    }
    if (char === ')') {
      depth--;
      current += char;
      continue;
    }

    if (depth === 0 && char === delimiter) {
      parts.push(current);
      current = '';
      continue;
    }

    current += char;
  }

  if (current) {
    parts.push(current);
  }

  return parts;
}

// =============================================================================
// VALIDATION
// =============================================================================

/**
 * Validate that two SELECT statements are compatible for set operations
 */
export function validateSetOperationCompatibility(
  leftColumns: string[],
  rightColumns: string[],
  leftTypes?: string[],
  rightTypes?: string[],
): SetOpValidationResult {
  const warnings: string[] = [];

  // Special case: SELECT *
  if (leftColumns.length === 1 && leftColumns[0] === '*') {
    return { valid: true };
  }
  if (rightColumns.length === 1 && rightColumns[0] === '*') {
    return { valid: true };
  }

  // Check column count
  if (leftColumns.length !== rightColumns.length) {
    return {
      valid: false,
      error: `Mismatched column count: left has ${leftColumns.length} columns, right has ${rightColumns.length}`,
    };
  }

  // Check type compatibility if types are provided
  if (leftTypes && rightTypes && leftTypes.length === rightTypes.length) {
    for (let i = 0; i < leftTypes.length; i++) {
      const leftType = leftTypes[i]?.toUpperCase() || 'UNKNOWN';
      const rightType = rightTypes[i]?.toUpperCase() || 'UNKNOWN';

      // NULL is compatible with any type
      if (leftType === 'NULL' || rightType === 'NULL') {
        continue;
      }

      // Check type categories
      const leftCategory = getTypeCategory(leftType);
      const rightCategory = getTypeCategory(rightType);

      if (leftCategory !== rightCategory && leftCategory !== 'any' && rightCategory !== 'any') {
        warnings.push(
          `Column ${i + 1}: implicit cast from ${rightType} to ${leftType} may lose data`,
        );
      }
    }
  }

  return {
    valid: true,
    warnings: warnings.length > 0 ? warnings : undefined,
  };
}

/**
 * Get type category for compatibility checking
 */
function getTypeCategory(type: string): string {
  const numeric = ['INTEGER', 'INT', 'REAL', 'FLOAT', 'DOUBLE', 'NUMERIC', 'DECIMAL', 'NUMBER'];
  const text = ['TEXT', 'VARCHAR', 'CHAR', 'STRING', 'CLOB'];
  const binary = ['BLOB', 'BYTES', 'BINARY', 'VARBINARY'];
  const temporal = ['DATE', 'TIME', 'DATETIME', 'TIMESTAMP'];

  const upper = type.toUpperCase();
  if (numeric.includes(upper)) return 'numeric';
  if (text.includes(upper)) return 'text';
  if (binary.includes(upper)) return 'binary';
  if (temporal.includes(upper)) return 'temporal';
  if (upper === 'BOOLEAN' || upper === 'BOOL') return 'boolean';
  if (upper === 'NULL') return 'any';
  return 'unknown';
}

// =============================================================================
// TYPE-LEVEL HELPERS
// =============================================================================

/**
 * Type-level detection of set operation in SQL string
 */
export type IsSetOperation<SQL extends string> =
  Uppercase<SQL> extends `${string}UNION${string}` ? true :
  Uppercase<SQL> extends `${string}INTERSECT${string}` ? true :
  Uppercase<SQL> extends `${string}EXCEPT${string}` ? true :
  Uppercase<SQL> extends `${string}MINUS${string}` ? true :
  false;

/**
 * Extract the set operation type from SQL
 */
export type ExtractSetOperationType<SQL extends string> =
  Uppercase<SQL> extends `${string}UNION${string}` ? 'UNION' :
  Uppercase<SQL> extends `${string}INTERSECT${string}` ? 'INTERSECT' :
  Uppercase<SQL> extends `${string}EXCEPT${string}` ? 'EXCEPT' :
  Uppercase<SQL> extends `${string}MINUS${string}` ? 'EXCEPT' :
  never;

/**
 * Check if set operation is ALL variant
 */
export type IsSetOperationAll<SQL extends string> =
  Uppercase<SQL> extends `${string}UNION ALL${string}` ? true :
  Uppercase<SQL> extends `${string}INTERSECT ALL${string}` ? true :
  Uppercase<SQL> extends `${string}EXCEPT ALL${string}` ? true :
  Uppercase<SQL> extends `${string}MINUS ALL${string}` ? true :
  false;
