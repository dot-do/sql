/**
 * Window Clause Parser
 *
 * Parses SQL window specifications:
 * - OVER (PARTITION BY ... ORDER BY ...)
 * - Frame specs: ROWS/RANGE/GROUPS BETWEEN ... AND ...
 * - UNBOUNDED PRECEDING/FOLLOWING, CURRENT ROW, n PRECEDING/FOLLOWING
 * - Frame exclusion: EXCLUDE CURRENT ROW, EXCLUDE GROUP, EXCLUDE TIES, EXCLUDE NO OTHERS
 *
 * Supports both named window definitions and inline window specs:
 * - WINDOW w AS (PARTITION BY x ORDER BY y)
 * - func() OVER (...)
 * - func() OVER window_name
 */

// =============================================================================
// TYPES
// =============================================================================

/**
 * Frame boundary types
 */
export type FrameBoundaryType =
  | 'unboundedPreceding'
  | 'unboundedFollowing'
  | 'currentRow'
  | 'preceding'
  | 'following';

/**
 * Frame boundary specification
 */
export interface FrameBoundary {
  type: FrameBoundaryType;
  offset?: number;
}

/**
 * Frame mode
 */
export type FrameMode = 'rows' | 'range' | 'groups';

/**
 * Frame exclusion type
 */
export type FrameExclusion = 'currentRow' | 'group' | 'ties' | 'noOthers';

/**
 * Frame specification
 */
export interface FrameSpec {
  mode: FrameMode;
  start: FrameBoundary;
  end: FrameBoundary;
  exclusion?: FrameExclusion;
}

/**
 * Sort direction for ORDER BY
 */
export type SortDirection = 'asc' | 'desc';

/**
 * Nulls position
 */
export type NullsPosition = 'first' | 'last';

/**
 * Order by item
 */
export interface OrderByItem {
  expression: Expression;
  direction: SortDirection;
  nulls?: NullsPosition;
}

/**
 * Expression types supported in window clauses
 */
export type Expression =
  | ColumnReference
  | LiteralExpression
  | FunctionCall
  | BinaryExpression
  | UnaryExpression;

/**
 * Column reference
 */
export interface ColumnReference {
  type: 'column';
  table?: string;
  column: string;
}

/**
 * Literal value
 */
export interface LiteralExpression {
  type: 'literal';
  value: string | number | boolean | null;
  raw: string;
}

/**
 * Function call (including window functions)
 */
export interface FunctionCall {
  type: 'function';
  name: string;
  args: Expression[];
  over?: WindowSpec | string; // inline spec or named window
  distinct?: boolean;
  filter?: Expression;
}

/**
 * Binary expression
 */
export interface BinaryExpression {
  type: 'binary';
  operator: string;
  left: Expression;
  right: Expression;
}

/**
 * Unary expression
 */
export interface UnaryExpression {
  type: 'unary';
  operator: string;
  operand: Expression;
}

/**
 * Window specification
 */
export interface WindowSpec {
  baseName?: string; // Reference to named window
  partitionBy?: Expression[];
  orderBy?: OrderByItem[];
  frame?: FrameSpec;
}

/**
 * Named window definition (WINDOW clause)
 */
export interface WindowDefinition {
  name: string;
  spec: WindowSpec;
}

/**
 * Window function call result
 */
export interface WindowFunctionCall {
  function: FunctionCall;
  alias?: string;
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
  const regex = new RegExp(`^${keyword}(?=[\\s();,]|$)`, 'i');
  if (regex.test(rest)) {
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

function peek(state: ParserState): string {
  return remaining(state)[0] || '';
}

// =============================================================================
// BASIC PARSING
// =============================================================================

/**
 * Parse an identifier
 */
function parseIdentifier(state: ParserState): { state: ParserState; value: string } | null {
  state = skipWhitespace(state);
  const rest = remaining(state);

  // Quoted identifier
  if (rest.startsWith('"') || rest.startsWith('`') || rest.startsWith('[')) {
    const quote = rest[0];
    const endQuote = quote === '[' ? ']' : quote;
    const endIndex = rest.indexOf(endQuote, 1);
    if (endIndex === -1) return null;
    return { state: advance(state, endIndex + 1), value: rest.slice(1, endIndex) };
  }

  // Unquoted identifier
  const match = rest.match(/^[a-zA-Z_][a-zA-Z0-9_]*/);
  if (match) {
    return { state: advance(state, match[0].length), value: match[0] };
  }
  return null;
}

/**
 * Parse a number
 */
function parseNumber(state: ParserState): { state: ParserState; value: number } | null {
  state = skipWhitespace(state);
  const rest = remaining(state);
  const match = rest.match(/^-?\d+(?:\.\d+)?/);
  if (match) {
    return {
      state: advance(state, match[0].length),
      value: match[0].includes('.') ? parseFloat(match[0]) : parseInt(match[0], 10),
    };
  }
  return null;
}

/**
 * Parse a string literal
 */
function parseStringLiteral(state: ParserState): { state: ParserState; value: string } | null {
  state = skipWhitespace(state);
  const rest = remaining(state);
  const quote = rest[0];

  if (quote !== "'" && quote !== '"') return null;

  let i = 1;
  let value = '';
  while (i < rest.length) {
    if (rest[i] === quote) {
      if (rest[i + 1] === quote) {
        value += quote;
        i += 2;
      } else {
        return { state: advance(state, i + 1), value };
      }
    } else {
      value += rest[i];
      i++;
    }
  }
  return null;
}

// =============================================================================
// EXPRESSION PARSING
// =============================================================================

/**
 * Parse an expression (simplified for window context)
 */
function parseExpression(state: ParserState): { state: ParserState; expr: Expression } | null {
  state = skipWhitespace(state);

  // Try column reference or function call
  const ident = parseIdentifier(state);
  if (ident) {
    state = skipWhitespace(ident.state);

    // Check for function call
    if (remaining(state).startsWith('(')) {
      return parseFunctionCall({ ...state, position: state.position - ident.value.length - (ident.state.position - state.position) }, ident.value);
    }

    // Check for table.column
    if (remaining(state).startsWith('.')) {
      state = advance(state, 1);
      const col = parseIdentifier(state);
      if (col) {
        return {
          state: col.state,
          expr: { type: 'column', table: ident.value, column: col.value },
        };
      }
    }

    // Just a column reference
    return {
      state: ident.state,
      expr: { type: 'column', column: ident.value },
    };
  }

  // Try number literal
  const num = parseNumber(state);
  if (num) {
    return {
      state: num.state,
      expr: { type: 'literal', value: num.value, raw: String(num.value) },
    };
  }

  // Try string literal
  const str = parseStringLiteral(state);
  if (str) {
    return {
      state: str.state,
      expr: { type: 'literal', value: str.value, raw: `'${str.value}'` },
    };
  }

  // Try NULL
  const nullMatch = matchKeyword(state, 'NULL');
  if (nullMatch) {
    return {
      state: nullMatch,
      expr: { type: 'literal', value: null, raw: 'NULL' },
    };
  }

  // Try TRUE/FALSE
  const trueMatch = matchKeyword(state, 'TRUE');
  if (trueMatch) {
    return {
      state: trueMatch,
      expr: { type: 'literal', value: true, raw: 'TRUE' },
    };
  }
  const falseMatch = matchKeyword(state, 'FALSE');
  if (falseMatch) {
    return {
      state: falseMatch,
      expr: { type: 'literal', value: false, raw: 'FALSE' },
    };
  }

  // Try parenthesized expression
  if (remaining(state).startsWith('(')) {
    state = advance(state, 1);
    const inner = parseExpression(state);
    if (inner) {
      state = skipWhitespace(inner.state);
      if (remaining(state).startsWith(')')) {
        return { state: advance(state, 1), expr: inner.expr };
      }
    }
  }

  return null;
}

/**
 * Parse a function call
 */
function parseFunctionCall(
  state: ParserState,
  name?: string
): { state: ParserState; expr: FunctionCall } | null {
  state = skipWhitespace(state);

  // Get function name if not provided
  if (!name) {
    const ident = parseIdentifier(state);
    if (!ident) return null;
    name = ident.value;
    state = ident.state;
  }

  state = skipWhitespace(state);
  if (!remaining(state).startsWith('(')) return null;
  state = advance(state, 1);
  state = skipWhitespace(state);

  const args: Expression[] = [];
  let distinct = false;

  // Check for DISTINCT
  const distinctMatch = matchKeyword(state, 'DISTINCT');
  if (distinctMatch) {
    distinct = true;
    state = skipWhitespace(distinctMatch);
  }

  // Check for * (for COUNT(*))
  if (remaining(state).startsWith('*')) {
    state = advance(state, 1);
    args.push({ type: 'column', column: '*' });
  } else if (!remaining(state).startsWith(')')) {
    // Parse arguments
    while (true) {
      const arg = parseExpression(state);
      if (!arg) break;
      args.push(arg.expr);
      state = skipWhitespace(arg.state);

      if (remaining(state).startsWith(',')) {
        state = advance(state, 1);
        state = skipWhitespace(state);
      } else {
        break;
      }
    }
  }

  state = skipWhitespace(state);
  if (!remaining(state).startsWith(')')) return null;
  state = advance(state, 1);

  const fnCall: FunctionCall = {
    type: 'function',
    name,
    args,
    distinct,
  };

  // Check for FILTER clause
  state = skipWhitespace(state);
  const filterMatch = matchKeyword(state, 'FILTER');
  if (filterMatch) {
    state = skipWhitespace(filterMatch);
    if (remaining(state).startsWith('(')) {
      state = advance(state, 1);
      state = skipWhitespace(state);
      const whereMatch = matchKeyword(state, 'WHERE');
      if (whereMatch) {
        state = skipWhitespace(whereMatch);
        const filterExpr = parseExpression(state);
        if (filterExpr) {
          fnCall.filter = filterExpr.expr;
          state = skipWhitespace(filterExpr.state);
          if (remaining(state).startsWith(')')) {
            state = advance(state, 1);
          }
        }
      }
    }
  }

  // Check for OVER clause
  state = skipWhitespace(state);
  const overMatch = matchKeyword(state, 'OVER');
  if (overMatch) {
    state = skipWhitespace(overMatch);

    // Check for window name or inline spec
    if (remaining(state).startsWith('(')) {
      const windowSpec = parseWindowSpec(state);
      if (windowSpec) {
        fnCall.over = windowSpec.spec;
        state = windowSpec.state;
      }
    } else {
      // Window name reference
      const windowName = parseIdentifier(state);
      if (windowName) {
        fnCall.over = windowName.value;
        state = windowName.state;
      }
    }
  }

  return { state, expr: fnCall };
}

// =============================================================================
// WINDOW SPECIFICATION PARSING
// =============================================================================

/**
 * Parse a window specification: (PARTITION BY ... ORDER BY ... FRAME)
 */
export function parseWindowSpec(
  state: ParserState
): { state: ParserState; spec: WindowSpec } | null {
  state = skipWhitespace(state);

  if (!remaining(state).startsWith('(')) return null;
  state = advance(state, 1);
  state = skipWhitespace(state);

  const spec: WindowSpec = {};

  // Check for base window name
  // Look ahead to see if this is a name reference
  const saved = state;
  const possibleName = parseIdentifier(state);
  if (possibleName) {
    const afterName = skipWhitespace(possibleName.state);
    const rest = remaining(afterName);
    // If followed by PARTITION, ORDER, ROWS, RANGE, GROUPS, or ), it's a base name
    if (
      rest.startsWith(')') ||
      matchKeyword(afterName, 'PARTITION') ||
      matchKeyword(afterName, 'ORDER') ||
      matchKeyword(afterName, 'ROWS') ||
      matchKeyword(afterName, 'RANGE') ||
      matchKeyword(afterName, 'GROUPS')
    ) {
      spec.baseName = possibleName.value;
      state = possibleName.state;
    }
  }
  state = skipWhitespace(state);

  // Parse PARTITION BY
  const partitionMatch = matchKeyword(state, 'PARTITION');
  if (partitionMatch) {
    state = skipWhitespace(partitionMatch);
    const byMatch = matchKeyword(state, 'BY');
    if (!byMatch) return null;
    state = skipWhitespace(byMatch);

    const partitionExprs = parseExpressionList(state);
    if (partitionExprs) {
      spec.partitionBy = partitionExprs.exprs;
      state = partitionExprs.state;
    }
  }
  state = skipWhitespace(state);

  // Parse ORDER BY
  const orderMatch = matchKeyword(state, 'ORDER');
  if (orderMatch) {
    state = skipWhitespace(orderMatch);
    const byMatch = matchKeyword(state, 'BY');
    if (!byMatch) return null;
    state = skipWhitespace(byMatch);

    const orderItems = parseOrderByList(state);
    if (orderItems) {
      spec.orderBy = orderItems.items;
      state = orderItems.state;
    }
  }
  state = skipWhitespace(state);

  // Parse frame specification
  const frameSpec = parseFrameSpec(state);
  if (frameSpec) {
    spec.frame = frameSpec.frame;
    state = frameSpec.state;
  }
  state = skipWhitespace(state);

  if (!remaining(state).startsWith(')')) return null;
  state = advance(state, 1);

  return { state, spec };
}

/**
 * Parse a list of expressions (for PARTITION BY)
 */
function parseExpressionList(
  state: ParserState
): { state: ParserState; exprs: Expression[] } | null {
  const exprs: Expression[] = [];

  while (true) {
    state = skipWhitespace(state);
    const expr = parseExpression(state);
    if (!expr) break;
    exprs.push(expr.expr);
    state = skipWhitespace(expr.state);

    if (remaining(state).startsWith(',')) {
      state = advance(state, 1);
    } else {
      break;
    }
  }

  if (exprs.length === 0) return null;
  return { state, exprs };
}

/**
 * Parse ORDER BY list
 */
function parseOrderByList(
  state: ParserState
): { state: ParserState; items: OrderByItem[] } | null {
  const items: OrderByItem[] = [];

  while (true) {
    state = skipWhitespace(state);
    const expr = parseExpression(state);
    if (!expr) break;

    const item: OrderByItem = {
      expression: expr.expr,
      direction: 'asc',
    };
    state = skipWhitespace(expr.state);

    // Parse direction
    const ascMatch = matchKeyword(state, 'ASC');
    if (ascMatch) {
      item.direction = 'asc';
      state = skipWhitespace(ascMatch);
    } else {
      const descMatch = matchKeyword(state, 'DESC');
      if (descMatch) {
        item.direction = 'desc';
        state = skipWhitespace(descMatch);
      }
    }

    // Parse NULLS FIRST/LAST
    const nullsMatch = matchKeyword(state, 'NULLS');
    if (nullsMatch) {
      state = skipWhitespace(nullsMatch);
      const firstMatch = matchKeyword(state, 'FIRST');
      if (firstMatch) {
        item.nulls = 'first';
        state = skipWhitespace(firstMatch);
      } else {
        const lastMatch = matchKeyword(state, 'LAST');
        if (lastMatch) {
          item.nulls = 'last';
          state = skipWhitespace(lastMatch);
        }
      }
    }

    items.push(item);

    if (remaining(state).startsWith(',')) {
      state = advance(state, 1);
    } else {
      break;
    }
  }

  if (items.length === 0) return null;
  return { state, items };
}

// =============================================================================
// FRAME SPECIFICATION PARSING
// =============================================================================

/**
 * Parse frame specification
 */
function parseFrameSpec(
  state: ParserState
): { state: ParserState; frame: FrameSpec } | null {
  state = skipWhitespace(state);

  // Parse frame mode (ROWS, RANGE, GROUPS)
  let mode: FrameMode;
  const rowsMatch = matchKeyword(state, 'ROWS');
  if (rowsMatch) {
    mode = 'rows';
    state = skipWhitespace(rowsMatch);
  } else {
    const rangeMatch = matchKeyword(state, 'RANGE');
    if (rangeMatch) {
      mode = 'range';
      state = skipWhitespace(rangeMatch);
    } else {
      const groupsMatch = matchKeyword(state, 'GROUPS');
      if (groupsMatch) {
        mode = 'groups';
        state = skipWhitespace(groupsMatch);
      } else {
        return null;
      }
    }
  }

  // Check for BETWEEN
  const betweenMatch = matchKeyword(state, 'BETWEEN');
  if (betweenMatch) {
    state = skipWhitespace(betweenMatch);

    // Parse start boundary
    const startResult = parseFrameBoundary(state);
    if (!startResult) return null;
    state = skipWhitespace(startResult.state);

    // Expect AND
    const andMatch = matchKeyword(state, 'AND');
    if (!andMatch) return null;
    state = skipWhitespace(andMatch);

    // Parse end boundary
    const endResult = parseFrameBoundary(state);
    if (!endResult) return null;
    state = endResult.state;

    const frame: FrameSpec = {
      mode,
      start: startResult.boundary,
      end: endResult.boundary,
    };

    // Parse optional EXCLUDE clause
    state = skipWhitespace(state);
    const exclusion = parseFrameExclusion(state);
    if (exclusion) {
      frame.exclusion = exclusion.exclusion;
      state = exclusion.state;
    }

    return { state, frame };
  } else {
    // Single boundary (start only, end defaults to CURRENT ROW)
    const boundaryResult = parseFrameBoundary(state);
    if (!boundaryResult) return null;

    const frame: FrameSpec = {
      mode,
      start: boundaryResult.boundary,
      end: { type: 'currentRow' },
    };

    state = boundaryResult.state;

    // Parse optional EXCLUDE clause
    state = skipWhitespace(state);
    const exclusion = parseFrameExclusion(state);
    if (exclusion) {
      frame.exclusion = exclusion.exclusion;
      state = exclusion.state;
    }

    return { state, frame };
  }
}

/**
 * Parse a frame boundary
 */
function parseFrameBoundary(
  state: ParserState
): { state: ParserState; boundary: FrameBoundary } | null {
  state = skipWhitespace(state);

  // UNBOUNDED PRECEDING
  const unboundedMatch = matchKeyword(state, 'UNBOUNDED');
  if (unboundedMatch) {
    state = skipWhitespace(unboundedMatch);
    const precedingMatch = matchKeyword(state, 'PRECEDING');
    if (precedingMatch) {
      return { state: precedingMatch, boundary: { type: 'unboundedPreceding' } };
    }
    const followingMatch = matchKeyword(state, 'FOLLOWING');
    if (followingMatch) {
      return { state: followingMatch, boundary: { type: 'unboundedFollowing' } };
    }
    return null;
  }

  // CURRENT ROW
  const currentMatch = matchKeyword(state, 'CURRENT');
  if (currentMatch) {
    state = skipWhitespace(currentMatch);
    const rowMatch = matchKeyword(state, 'ROW');
    if (rowMatch) {
      return { state: rowMatch, boundary: { type: 'currentRow' } };
    }
    return null;
  }

  // n PRECEDING or n FOLLOWING
  const num = parseNumber(state);
  if (num) {
    state = skipWhitespace(num.state);
    const precedingMatch = matchKeyword(state, 'PRECEDING');
    if (precedingMatch) {
      return {
        state: precedingMatch,
        boundary: { type: 'preceding', offset: num.value },
      };
    }
    const followingMatch = matchKeyword(state, 'FOLLOWING');
    if (followingMatch) {
      return {
        state: followingMatch,
        boundary: { type: 'following', offset: num.value },
      };
    }
  }

  return null;
}

/**
 * Parse frame exclusion
 */
function parseFrameExclusion(
  state: ParserState
): { state: ParserState; exclusion: FrameExclusion } | null {
  const excludeMatch = matchKeyword(state, 'EXCLUDE');
  if (!excludeMatch) return null;
  state = skipWhitespace(excludeMatch);

  // EXCLUDE CURRENT ROW
  const currentMatch = matchKeyword(state, 'CURRENT');
  if (currentMatch) {
    state = skipWhitespace(currentMatch);
    const rowMatch = matchKeyword(state, 'ROW');
    if (rowMatch) {
      return { state: rowMatch, exclusion: 'currentRow' };
    }
  }

  // EXCLUDE GROUP
  const groupMatch = matchKeyword(state, 'GROUP');
  if (groupMatch) {
    return { state: groupMatch, exclusion: 'group' };
  }

  // EXCLUDE TIES
  const tiesMatch = matchKeyword(state, 'TIES');
  if (tiesMatch) {
    return { state: tiesMatch, exclusion: 'ties' };
  }

  // EXCLUDE NO OTHERS
  const noMatch = matchKeyword(state, 'NO');
  if (noMatch) {
    state = skipWhitespace(noMatch);
    const othersMatch = matchKeyword(state, 'OTHERS');
    if (othersMatch) {
      return { state: othersMatch, exclusion: 'noOthers' };
    }
  }

  return null;
}

// =============================================================================
// WINDOW CLAUSE PARSING
// =============================================================================

/**
 * Parse WINDOW clause (named window definitions)
 */
export function parseWindowClause(
  state: ParserState
): { state: ParserState; definitions: WindowDefinition[] } | null {
  state = skipWhitespace(state);

  const windowMatch = matchKeyword(state, 'WINDOW');
  if (!windowMatch) return null;
  state = skipWhitespace(windowMatch);

  const definitions: WindowDefinition[] = [];

  while (true) {
    // Parse window name
    const name = parseIdentifier(state);
    if (!name) break;
    state = skipWhitespace(name.state);

    // Expect AS
    const asMatch = matchKeyword(state, 'AS');
    if (!asMatch) return null;
    state = skipWhitespace(asMatch);

    // Parse window spec
    const spec = parseWindowSpec(state);
    if (!spec) return null;
    state = spec.state;

    definitions.push({
      name: name.value,
      spec: spec.spec,
    });

    state = skipWhitespace(state);
    if (remaining(state).startsWith(',')) {
      state = advance(state, 1);
      state = skipWhitespace(state);
    } else {
      break;
    }
  }

  if (definitions.length === 0) return null;
  return { state, definitions };
}

// =============================================================================
// HIGH-LEVEL PARSING FUNCTIONS
// =============================================================================

/**
 * Parse a window function call from SQL string
 */
export function parseWindowFunctionCall(
  sql: string
): WindowFunctionCall | null {
  const state = createState(sql);
  const result = parseFunctionCall(state);
  if (!result) return null;

  const fn = result.expr;
  if (fn.type !== 'function') return null;

  return { function: fn };
}

/**
 * Parse an OVER clause from SQL string
 */
export function parseOverClause(sql: string): WindowSpec | string | null {
  let state = createState(sql);
  state = skipWhitespace(state);

  const overMatch = matchKeyword(state, 'OVER');
  if (!overMatch) return null;
  state = skipWhitespace(overMatch);

  // Check for window name or inline spec
  if (remaining(state).startsWith('(')) {
    const spec = parseWindowSpec(state);
    if (spec) return spec.spec;
  } else {
    const name = parseIdentifier(state);
    if (name) return name.value;
  }

  return null;
}

/**
 * Resolve a window specification against named windows
 */
export function resolveWindowSpec(
  spec: WindowSpec | string,
  namedWindows: Map<string, WindowSpec>
): WindowSpec {
  if (typeof spec === 'string') {
    const named = namedWindows.get(spec);
    if (!named) {
      throw new Error(`Unknown window name: ${spec}`);
    }
    return named;
  }

  if (spec.baseName) {
    const base = namedWindows.get(spec.baseName);
    if (!base) {
      throw new Error(`Unknown window name: ${spec.baseName}`);
    }

    // Merge with base window
    return {
      partitionBy: spec.partitionBy ?? base.partitionBy,
      orderBy: spec.orderBy ?? base.orderBy,
      frame: spec.frame ?? base.frame,
    };
  }

  return spec;
}

/**
 * Validate a window specification
 */
export function validateWindowSpec(spec: WindowSpec): void {
  if (spec.frame) {
    // Validate frame boundaries make sense
    const { start, end } = spec.frame;

    // UNBOUNDED FOLLOWING cannot be start
    if (start.type === 'unboundedFollowing') {
      throw new Error('UNBOUNDED FOLLOWING cannot be used as frame start');
    }

    // UNBOUNDED PRECEDING cannot be end
    if (end.type === 'unboundedPreceding') {
      throw new Error('UNBOUNDED PRECEDING cannot be used as frame end');
    }

    // Check that start <= end for fixed boundaries
    if (
      start.type === 'following' &&
      (end.type === 'preceding' || end.type === 'currentRow')
    ) {
      throw new Error('Frame start cannot be after frame end');
    }

    if (
      start.type === 'following' &&
      end.type === 'following' &&
      (start.offset ?? 0) > (end.offset ?? 0)
    ) {
      throw new Error('Frame start cannot be after frame end');
    }

    if (
      start.type === 'preceding' &&
      end.type === 'preceding' &&
      (start.offset ?? 0) < (end.offset ?? 0)
    ) {
      throw new Error('Frame start cannot be after frame end');
    }
  }
}
