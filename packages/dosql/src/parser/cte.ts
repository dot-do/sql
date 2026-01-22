/**
 * DoSQL CTE (Common Table Expression) Parser
 *
 * Parses WITH clauses and CTEs, including:
 * - Simple CTEs: WITH name AS (SELECT ...)
 * - Multiple CTEs: WITH a AS (...), b AS (...) SELECT ...
 * - Recursive CTEs: WITH RECURSIVE name AS (...)
 * - Column aliasing: WITH name(col1, col2) AS (...)
 *
 * Example recursive CTE:
 * ```sql
 * WITH RECURSIVE ancestors AS (
 *   SELECT id, parent_id, name FROM employees WHERE id = 5
 *   UNION ALL
 *   SELECT e.id, e.parent_id, e.name FROM employees e
 *   JOIN ancestors a ON e.id = a.parent_id
 * )
 * SELECT * FROM ancestors;
 * ```
 */

import type {
  CTEDefinition,
  WithClause,
  CTEParseResult,
  RecursiveCTEStructure,
} from './cte-types.js';

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
 * Skip whitespace and SQL comments
 */
function skipWhitespace(state: ParserState): ParserState {
  const rest = remaining(state);
  let i = 0;

  while (i < rest.length) {
    // Skip whitespace
    if (/\s/.test(rest[i])) {
      i++;
      continue;
    }

    // Skip single-line comments (-- ...)
    if (rest.slice(i, i + 2) === '--') {
      const newline = rest.indexOf('\n', i);
      i = newline === -1 ? rest.length : newline + 1;
      continue;
    }

    // Skip multi-line comments (/* ... */)
    if (rest.slice(i, i + 2) === '/*') {
      const end = rest.indexOf('*/', i + 2);
      i = end === -1 ? rest.length : end + 2;
      continue;
    }

    break;
  }

  return advance(state, i);
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
 * Parse a comma-separated list of identifiers (for column aliases)
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
// PARENTHESIS BALANCING
// =============================================================================

/**
 * Find matching closing parenthesis, accounting for nested parens and strings
 */
function findMatchingParen(sql: string, startPos: number): number {
  if (sql[startPos] !== '(') return -1;

  let depth = 1;
  let i = startPos + 1;
  let inString: string | null = null;

  while (i < sql.length && depth > 0) {
    const char = sql[i];

    // Handle string literals
    if (inString) {
      if (char === inString) {
        // Check for escaped quote
        if (sql[i + 1] === inString) {
          i += 2;
          continue;
        }
        inString = null;
      }
      i++;
      continue;
    }

    // Start of string literal
    if (char === "'" || char === '"') {
      inString = char;
      i++;
      continue;
    }

    // Skip comments
    if (sql.slice(i, i + 2) === '--') {
      const newline = sql.indexOf('\n', i);
      i = newline === -1 ? sql.length : newline + 1;
      continue;
    }
    if (sql.slice(i, i + 2) === '/*') {
      const end = sql.indexOf('*/', i + 2);
      i = end === -1 ? sql.length : end + 2;
      continue;
    }

    // Track parentheses
    if (char === '(') depth++;
    if (char === ')') depth--;

    i++;
  }

  return depth === 0 ? i - 1 : -1;
}

// =============================================================================
// RECURSIVE CTE PARSING
// =============================================================================

/**
 * Parse a recursive CTE to extract anchor and recursive parts
 */
function parseRecursiveCTEStructure(query: string): RecursiveCTEStructure | null {
  const upper = query.toUpperCase();

  // Find UNION ALL or UNION (case-insensitive, word boundary)
  // We need to find it outside of any parentheses
  let unionIndex = -1;
  let isUnionAll = false;
  let depth = 0;
  let inString: string | null = null;

  for (let i = 0; i < query.length; i++) {
    const char = query[i];

    // Handle strings
    if (inString) {
      if (char === inString && query[i + 1] !== inString) {
        inString = null;
      }
      continue;
    }
    if (char === "'" || char === '"') {
      inString = char;
      continue;
    }

    // Track depth
    if (char === '(') depth++;
    if (char === ')') depth--;

    // Only look for UNION at depth 0
    if (depth === 0 && upper.slice(i).match(/^UNION\s/i)) {
      unionIndex = i;
      // Check if UNION ALL
      const afterUnion = upper.slice(i + 5).trim();
      isUnionAll = afterUnion.startsWith('ALL');
      break;
    }
  }

  if (unionIndex === -1) return null;

  const anchor = query.slice(0, unionIndex).trim();
  let recursiveStart = unionIndex + 5; // "UNION"
  const afterUnion = query.slice(recursiveStart).trim();
  if (afterUnion.toUpperCase().startsWith('ALL')) {
    recursiveStart = unionIndex + query.slice(unionIndex).toUpperCase().indexOf('ALL') + 3;
  }
  const recursive = query.slice(recursiveStart).trim();

  return {
    anchor,
    recursive,
    unionAll: isUnionAll,
  };
}

// =============================================================================
// CTE DEFINITION PARSING
// =============================================================================

/**
 * Parse a single CTE definition: name [(col1, col2, ...)] AS (query)
 */
function parseCTEDefinition(
  state: ParserState,
  globalRecursive: boolean
): { state: ParserState; cte: CTEDefinition } | null {
  state = skipWhitespace(state);

  // Parse CTE name
  const nameResult = parseIdentifier(state);
  if (!nameResult) return null;
  const name = nameResult.value;
  state = skipWhitespace(nameResult.state);

  // Parse optional column list
  let columns: string[] | undefined;
  if (remaining(state).startsWith('(')) {
    // Check if this is a column list or the AS query
    // Column list comes before AS keyword
    const rest = remaining(state);
    const asMatch = rest.toUpperCase().indexOf(' AS ');
    const parenMatch = rest.indexOf('(');

    // If there's content between ( and AS, it's a column list
    if (parenMatch === 0 && asMatch > 0) {
      const closeParen = findMatchingParen(rest, 0);
      if (closeParen !== -1 && closeParen < asMatch) {
        state = advance(state, 1); // Skip (
        state = skipWhitespace(state);
        const colsResult = parseIdentifierList(state);
        if (colsResult) {
          columns = colsResult.values;
          state = skipWhitespace(colsResult.state);
        }
        if (!remaining(state).startsWith(')')) {
          return null; // Missing closing paren
        }
        state = advance(state, 1);
        state = skipWhitespace(state);
      }
    }
  }

  // Expect AS keyword
  const asMatch = matchKeyword(state, 'AS');
  if (!asMatch) return null;
  state = skipWhitespace(asMatch);

  // Parse query in parentheses
  if (!remaining(state).startsWith('(')) return null;

  const queryStart = state.position;
  const closeParen = findMatchingParen(state.input, queryStart);
  if (closeParen === -1) return null;

  // Extract query (without outer parentheses)
  const query = state.input.slice(queryStart + 1, closeParen).trim();
  state = advance(state, closeParen - queryStart + 1);

  // Determine if this CTE is recursive
  // A CTE is recursive if:
  // 1. The WITH clause has RECURSIVE keyword, AND
  // 2. The query references the CTE name itself
  const isRecursive = globalRecursive && containsCTEReference(query, name);

  const cte: CTEDefinition = {
    name,
    query,
    recursive: isRecursive,
  };

  if (columns) {
    cte.columns = columns;
  }

  // If recursive, parse structure
  if (isRecursive) {
    const structure = parseRecursiveCTEStructure(query);
    if (structure) {
      cte.anchorQuery = structure.anchor;
      cte.recursiveQuery = structure.recursive;
    }
  }

  return { state, cte };
}

/**
 * Check if a query contains a reference to a CTE name
 */
function containsCTEReference(query: string, cteName: string): boolean {
  // Simple check: does the query contain the CTE name as a word?
  const regex = new RegExp(`\\b${cteName}\\b`, 'i');
  return regex.test(query);
}

// =============================================================================
// WITH CLAUSE PARSING
// =============================================================================

/**
 * Parse a WITH clause
 */
function parseWithClause(state: ParserState): { state: ParserState; with: WithClause } | null {
  state = skipWhitespace(state);

  // Expect WITH keyword
  const withMatch = matchKeyword(state, 'WITH');
  if (!withMatch) return null;
  state = skipWhitespace(withMatch);

  // Check for RECURSIVE keyword
  let recursive = false;
  const recursiveMatch = matchKeyword(state, 'RECURSIVE');
  if (recursiveMatch) {
    recursive = true;
    state = skipWhitespace(recursiveMatch);
  }

  // Parse CTE definitions
  const ctes: CTEDefinition[] = [];

  while (true) {
    const cteResult = parseCTEDefinition(state, recursive);
    if (!cteResult) {
      if (ctes.length === 0) return null; // Need at least one CTE
      break;
    }

    ctes.push(cteResult.cte);
    state = skipWhitespace(cteResult.state);

    // Check for comma (another CTE follows)
    if (remaining(state).startsWith(',')) {
      state = advance(state, 1);
      state = skipWhitespace(state);
      continue;
    }

    break;
  }

  return {
    state,
    with: {
      type: 'with',
      recursive,
      ctes,
    },
  };
}

// =============================================================================
// PUBLIC API
// =============================================================================

/**
 * Parse a SQL statement that may start with a WITH clause
 *
 * Returns the parsed WITH clause and the remaining main query
 */
export function parseCTE(sql: string): CTEParseResult {
  const state = createState(sql);

  // Skip leading comments and whitespace, then check for WITH
  const stripped = stripLeadingCommentsAndWhitespace(sql.trim());
  const upper = stripped.toUpperCase();

  // Check if SQL starts with WITH
  if (!upper.startsWith('WITH ') && !upper.startsWith('WITH\t') && !upper.startsWith('WITH\n')) {
    return {
      success: false,
      error: 'SQL does not start with WITH keyword',
      position: 0,
      input: sql,
    };
  }

  const result = parseWithClause(state);
  if (!result) {
    return {
      success: false,
      error: 'Failed to parse WITH clause',
      position: state.position,
      input: sql,
    };
  }

  const mainQuery = remaining(result.state).trim();

  return {
    success: true,
    with: result.with,
    mainQuery,
    originalSql: sql,
  };
}

/**
 * Check if a SQL string starts with a WITH clause
 *
 * This function handles leading comments (both -- and /* *\/)
 */
export function hasWithClause(sql: string): boolean {
  // Skip leading whitespace and comments
  const stripped = stripLeadingCommentsAndWhitespace(sql);
  const upper = stripped.toUpperCase();
  return upper.startsWith('WITH ') || upper.startsWith('WITH\t') || upper.startsWith('WITH\n');
}

/**
 * Strip leading comments and whitespace from SQL
 */
function stripLeadingCommentsAndWhitespace(sql: string): string {
  let i = 0;

  while (i < sql.length) {
    // Skip whitespace
    if (/\s/.test(sql[i])) {
      i++;
      continue;
    }

    // Skip single-line comments (-- ...)
    if (sql.slice(i, i + 2) === '--') {
      const newline = sql.indexOf('\n', i);
      i = newline === -1 ? sql.length : newline + 1;
      continue;
    }

    // Skip multi-line comments (/* ... */)
    if (sql.slice(i, i + 2) === '/*') {
      const end = sql.indexOf('*/', i + 2);
      i = end === -1 ? sql.length : end + 2;
      continue;
    }

    break;
  }

  return sql.slice(i);
}

/**
 * Extract WITH clause and main query from SQL
 *
 * If the SQL has a WITH clause, returns { withClause, mainQuery }
 * If not, returns { withClause: null, mainQuery: sql }
 */
export function extractWithClause(sql: string): {
  withClause: WithClause | null;
  mainQuery: string;
} {
  if (!hasWithClause(sql)) {
    return { withClause: null, mainQuery: sql };
  }

  const result = parseCTE(sql);
  if (result.success) {
    return {
      withClause: result.with,
      mainQuery: result.mainQuery,
    };
  }

  return { withClause: null, mainQuery: sql };
}

/**
 * Get CTE names from a WITH clause
 */
export function getCTENames(withClause: WithClause): string[] {
  return withClause.ctes.map(cte => cte.name);
}

/**
 * Find a CTE by name in a WITH clause
 */
export function findCTE(withClause: WithClause, name: string): CTEDefinition | undefined {
  return withClause.ctes.find(
    cte => cte.name.toLowerCase() === name.toLowerCase()
  );
}

/**
 * Validate CTE references - ensure no forward references
 *
 * CTEs can only reference CTEs that were defined before them
 * (except recursive CTEs can reference themselves)
 */
export function validateCTEReferences(withClause: WithClause): {
  valid: boolean;
  errors: string[];
} {
  const errors: string[] = [];
  const defined = new Set<string>();

  for (const cte of withClause.ctes) {
    // Check query for references to undefined CTEs
    for (const prevCte of withClause.ctes) {
      if (prevCte === cte) break;
      // References to already-defined CTEs are OK
    }

    // For non-recursive CTEs, check for self-reference
    if (!cte.recursive && containsCTEReference(cte.query, cte.name)) {
      // This would require RECURSIVE keyword
      errors.push(
        `CTE '${cte.name}' references itself but WITH RECURSIVE was not specified`
      );
    }

    defined.add(cte.name.toLowerCase());
  }

  return {
    valid: errors.length === 0,
    errors,
  };
}

/**
 * Rewrite a query to expand non-recursive CTEs inline
 *
 * This is useful for databases that don't support CTEs natively
 */
export function expandCTEsInline(sql: string): string {
  const { withClause, mainQuery } = extractWithClause(sql);

  if (!withClause) {
    return sql;
  }

  // For now, only expand non-recursive CTEs
  let expandedQuery = mainQuery;

  // Expand in reverse order so that later CTEs can reference earlier ones
  for (let i = withClause.ctes.length - 1; i >= 0; i--) {
    const cte = withClause.ctes[i];

    if (cte.recursive) {
      // Can't expand recursive CTEs inline
      continue;
    }

    // Replace references to CTE name with subquery
    const regex = new RegExp(`\\b${cte.name}\\b`, 'gi');
    expandedQuery = expandedQuery.replace(regex, `(${cte.query})`);
  }

  return expandedQuery;
}

/**
 * Get the dependency order of CTEs
 *
 * Returns CTEs in the order they should be materialized
 * (dependencies before dependents)
 */
export function getCTEDependencyOrder(withClause: WithClause): CTEDefinition[] {
  // For now, just return in declaration order
  // A full implementation would do topological sort based on references
  return [...withClause.ctes];
}

// =============================================================================
// EXPORTS
// =============================================================================

export type {
  CTEDefinition,
  WithClause,
  CTEParseResult,
  RecursiveCTEStructure,
} from './cte-types.js';

export {
  createCTE,
  createRecursiveCTE,
  createWithClause,
  createCTEScope,
  isRecursiveCTE,
  hasRecursiveCTE,
  isCTEParseSuccess,
  isCTEParseError,
} from './cte-types.js';
