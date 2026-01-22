/**
 * View Parser for DoSQL
 *
 * Parses SQL VIEW statements:
 * - CREATE VIEW
 * - CREATE OR REPLACE VIEW
 * - CREATE TEMP/TEMPORARY VIEW
 * - CREATE VIEW IF NOT EXISTS
 * - DROP VIEW / DROP VIEW IF EXISTS
 * - Column aliases
 * - WITH CHECK OPTION
 */

import type {
  ParsedView,
  DropViewStatement,
  ViewColumn,
  CheckOption,
} from './types.js';
import { ViewError, ViewErrorCode } from './types.js';

// =============================================================================
// Parser Patterns
// =============================================================================

/**
 * Pattern to match CREATE VIEW statements
 */
const CREATE_VIEW_PATTERN = /^\s*CREATE\s+(OR\s+REPLACE\s+)?(TEMP(?:ORARY)?\s+)?VIEW\s+(IF\s+NOT\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)\s*(?:\(([\w\s,]+)\))?\s+AS\s+/i;

/**
 * Pattern to match DROP VIEW statements
 */
const DROP_VIEW_PATTERN = /^\s*DROP\s+VIEW\s+(IF\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)\s*(CASCADE|RESTRICT)?\s*;?\s*$/i;

/**
 * Pattern to match WITH CHECK OPTION clause
 */
const WITH_CHECK_OPTION_PATTERN = /\s+WITH\s+(CASCADED\s+|LOCAL\s+)?CHECK\s+OPTION\s*;?\s*$/i;

// =============================================================================
// CREATE VIEW Parser
// =============================================================================

/**
 * Parse a CREATE VIEW statement
 *
 * @param sql - The SQL statement to parse
 * @returns Parsed view definition
 * @throws ViewError if parsing fails
 *
 * @example
 * ```typescript
 * const view = parseCreateView(`
 *   CREATE VIEW active_users AS
 *   SELECT * FROM users WHERE active = true
 * `);
 * ```
 */
export function parseCreateView(sql: string): ParsedView {
  const trimmedSql = sql.trim();

  // Match the CREATE VIEW header
  const headerMatch = trimmedSql.match(CREATE_VIEW_PATTERN);
  if (!headerMatch) {
    throw new ViewError(
      ViewErrorCode.INVALID_DEFINITION,
      'Invalid CREATE VIEW syntax. Expected: CREATE [OR REPLACE] [TEMP|TEMPORARY] VIEW [IF NOT EXISTS] [schema.]name [(columns)] AS SELECT ...'
    );
  }

  const [
    fullMatch,
    orReplace,
    temporary,
    ifNotExists,
    schema,
    name,
    columnList,
  ] = headerMatch;

  // Extract the SELECT query (everything after AS)
  const afterAs = trimmedSql.slice(fullMatch.length);

  // Check for WITH CHECK OPTION and extract it
  const checkOptionMatch = afterAs.match(WITH_CHECK_OPTION_PATTERN);
  let selectQuery = afterAs;
  let checkOption: CheckOption;

  if (checkOptionMatch) {
    const [fullCheckMatch, checkType] = checkOptionMatch;
    selectQuery = afterAs.slice(0, -fullCheckMatch.length);
    checkOption = checkType?.trim().toUpperCase() as CheckOption || 'CASCADED';
  }

  // Clean up the SELECT query
  selectQuery = selectQuery.trim().replace(/;?\s*$/, '');

  // Validate the SELECT query starts with SELECT
  if (!/^\s*SELECT\s/i.test(selectQuery) && !/^\s*WITH\s/i.test(selectQuery)) {
    throw new ViewError(
      ViewErrorCode.INVALID_DEFINITION,
      'View definition must start with SELECT or WITH clause',
      name
    );
  }

  // Parse column aliases if provided
  const columns = columnList
    ? columnList.split(',').map(c => c.trim()).filter(c => c.length > 0)
    : undefined;

  // Parse column definitions from the SELECT query
  const columnDefinitions = parseSelectColumns(selectQuery);

  return {
    name,
    schema: schema || undefined,
    columns,
    selectQuery,
    columnDefinitions,
    orReplace: !!orReplace,
    ifNotExists: !!ifNotExists,
    temporary: !!temporary,
    checkOption,
    rawSql: trimmedSql,
  };
}

/**
 * Try to parse a CREATE VIEW statement, returning a result object
 */
export function tryParseCreateView(sql: string):
  | { success: true; view: ParsedView }
  | { success: false; error: ViewError } {
  try {
    const view = parseCreateView(sql);
    return { success: true, view };
  } catch (error) {
    if (error instanceof ViewError) {
      return { success: false, error };
    }
    return {
      success: false,
      error: new ViewError(
        ViewErrorCode.INVALID_DEFINITION,
        error instanceof Error ? error.message : String(error)
      ),
    };
  }
}

/**
 * Check if a SQL statement is a CREATE VIEW statement
 */
export function isCreateView(sql: string): boolean {
  return /^\s*CREATE\s+(OR\s+REPLACE\s+)?(TEMP(?:ORARY)?\s+)?VIEW\s/i.test(sql);
}

// =============================================================================
// DROP VIEW Parser
// =============================================================================

/**
 * Parse a DROP VIEW statement
 *
 * @param sql - The SQL statement to parse
 * @returns Parsed DROP VIEW statement
 * @throws ViewError if parsing fails
 */
export function parseDropView(sql: string): DropViewStatement {
  const trimmedSql = sql.trim();

  const match = trimmedSql.match(DROP_VIEW_PATTERN);
  if (!match) {
    throw new ViewError(
      ViewErrorCode.INVALID_DEFINITION,
      'Invalid DROP VIEW syntax. Expected: DROP VIEW [IF EXISTS] [schema.]name [CASCADE|RESTRICT]'
    );
  }

  const [, ifExists, schema, name, cascadeOrRestrict] = match;

  return {
    name,
    schema: schema || undefined,
    ifExists: !!ifExists,
    cascade: cascadeOrRestrict?.toUpperCase() === 'CASCADE',
  };
}

/**
 * Check if a SQL statement is a DROP VIEW statement
 */
export function isDropView(sql: string): boolean {
  return /^\s*DROP\s+VIEW\s/i.test(sql);
}

// =============================================================================
// Column Parsing
// =============================================================================

/**
 * Parse SELECT columns from a query to extract column definitions
 *
 * This is a simplified parser that handles common cases:
 * - Simple column references: column_name
 * - Qualified column references: table.column_name
 * - Aliased columns: expression AS alias
 * - Star expressions: * or table.*
 */
export function parseSelectColumns(selectQuery: string): ViewColumn[] {
  const columns: ViewColumn[] = [];

  // Extract the SELECT clause (up to FROM, WHERE, GROUP BY, etc.)
  const selectClauseMatch = selectQuery.match(
    /^\s*(?:WITH\s+.+?\s+)?SELECT\s+(DISTINCT\s+|ALL\s+)?(.*?)\s+FROM\s/is
  );

  if (!selectClauseMatch) {
    // Try without FROM (e.g., SELECT 1, 2, 3)
    const simpleMatch = selectQuery.match(
      /^\s*(?:WITH\s+.+?\s+)?SELECT\s+(DISTINCT\s+|ALL\s+)?(.*?)$/is
    );
    if (!simpleMatch) {
      return columns;
    }
    return parseColumnList(simpleMatch[2]);
  }

  return parseColumnList(selectClauseMatch[2]);
}

/**
 * Parse a comma-separated list of column expressions
 */
function parseColumnList(columnStr: string): ViewColumn[] {
  const columns: ViewColumn[] = [];
  const expressions = splitColumnExpressions(columnStr);

  for (const expr of expressions) {
    const column = parseColumnExpression(expr.trim());
    if (column) {
      columns.push(column);
    }
  }

  return columns;
}

/**
 * Split column expressions by comma, respecting parentheses and quotes
 */
function splitColumnExpressions(str: string): string[] {
  const expressions: string[] = [];
  let current = '';
  let depth = 0;
  let inString = false;
  let stringChar = '';

  for (let i = 0; i < str.length; i++) {
    const char = str[i];
    const prevChar = i > 0 ? str[i - 1] : '';

    // Handle string literals
    if ((char === "'" || char === '"') && prevChar !== '\\') {
      if (!inString) {
        inString = true;
        stringChar = char;
      } else if (char === stringChar) {
        inString = false;
        stringChar = '';
      }
    }

    // Handle parentheses
    if (!inString) {
      if (char === '(' || char === '[') {
        depth++;
      } else if (char === ')' || char === ']') {
        depth--;
      } else if (char === ',' && depth === 0) {
        expressions.push(current);
        current = '';
        continue;
      }
    }

    current += char;
  }

  if (current.trim()) {
    expressions.push(current);
  }

  return expressions;
}

/**
 * Parse a single column expression
 */
function parseColumnExpression(expr: string): ViewColumn | null {
  const trimmed = expr.trim();

  if (!trimmed || trimmed === '*') {
    return null;
  }

  // Check for table.* pattern
  if (/^\w+\.\*$/.test(trimmed)) {
    return null; // Can't determine columns without schema info
  }

  // Check for AS alias pattern
  const asMatch = trimmed.match(/^(.+?)\s+AS\s+["']?(\w+)["']?\s*$/i);
  if (asMatch) {
    return {
      name: asMatch[2],
      expression: asMatch[1].trim(),
      type: inferExpressionType(asMatch[1].trim()),
    };
  }

  // Check for space-separated alias (without AS)
  // e.g., "column_name alias" but not "CASE WHEN..." or function calls
  const spaceAliasMatch = trimmed.match(/^([\w.]+)\s+["']?(\w+)["']?\s*$/);
  if (spaceAliasMatch && !isKeyword(spaceAliasMatch[2])) {
    return {
      name: spaceAliasMatch[2],
      expression: spaceAliasMatch[1],
      type: inferExpressionType(spaceAliasMatch[1]),
    };
  }

  // Simple column reference: column_name or table.column_name
  const simpleMatch = trimmed.match(/^(?:(\w+)\.)?(\w+)$/);
  if (simpleMatch) {
    return {
      name: simpleMatch[2],
      expression: trimmed,
      type: undefined, // Type unknown without schema
    };
  }

  // Complex expression without alias - use the expression as name
  // Try to extract a reasonable name
  const cleanName = trimmed
    .replace(/\s+/g, '_')
    .replace(/[^a-zA-Z0-9_]/g, '')
    .slice(0, 30);

  return {
    name: cleanName || `column_${Math.random().toString(36).slice(2, 8)}`,
    expression: trimmed,
    type: inferExpressionType(trimmed),
  };
}

/**
 * Check if a word is a SQL keyword (not an alias)
 */
function isKeyword(word: string): boolean {
  const keywords = new Set([
    'FROM', 'WHERE', 'GROUP', 'HAVING', 'ORDER', 'LIMIT', 'OFFSET',
    'UNION', 'INTERSECT', 'EXCEPT', 'JOIN', 'ON', 'AND', 'OR', 'NOT',
    'IN', 'IS', 'NULL', 'BETWEEN', 'LIKE', 'CASE', 'WHEN', 'THEN',
    'ELSE', 'END', 'AS', 'ASC', 'DESC', 'DISTINCT', 'ALL',
  ]);
  return keywords.has(word.toUpperCase());
}

/**
 * Infer the type of an expression (simplified)
 */
function inferExpressionType(expr: string): string | undefined {
  const upper = expr.toUpperCase().trim();

  // Numeric literals
  if (/^-?\d+$/.test(expr)) return 'INTEGER';
  if (/^-?\d+\.\d+$/.test(expr)) return 'REAL';

  // String literals
  if (/^'.*'$/.test(expr) || /^".*"$/.test(expr)) return 'TEXT';

  // Boolean
  if (upper === 'TRUE' || upper === 'FALSE') return 'BOOLEAN';

  // Aggregate functions
  if (/^COUNT\s*\(/i.test(expr)) return 'INTEGER';
  if (/^(SUM|AVG|MIN|MAX)\s*\(/i.test(expr)) return 'NUMERIC';

  // String functions
  if (/^(UPPER|LOWER|TRIM|SUBSTR|SUBSTRING)\s*\(/i.test(expr)) return 'TEXT';
  if (/^(LENGTH|CHAR_LENGTH)\s*\(/i.test(expr)) return 'INTEGER';

  // Date functions
  if (/^(DATE|TIME|DATETIME|NOW|CURRENT_)\w*\s*\(?/i.test(expr)) return 'DATETIME';

  // CASE expressions - hard to infer
  if (/^CASE\s/i.test(expr)) return undefined;

  return undefined;
}

// =============================================================================
// Table/View Reference Extraction
// =============================================================================

/**
 * Extract table and view references from a SELECT query
 *
 * @param selectQuery - The SELECT query to analyze
 * @returns Object with table and view names
 */
export function extractReferences(selectQuery: string): {
  tables: string[];
  possibleViews: string[];
} {
  const references = new Set<string>();

  // Match FROM clause references
  const fromMatches = selectQuery.matchAll(
    /\bFROM\s+(?:(\w+)\.)?(\w+)(?:\s+(?:AS\s+)?(\w+))?/gi
  );
  for (const match of fromMatches) {
    references.add(match[2]);
  }

  // Match JOIN references
  const joinMatches = selectQuery.matchAll(
    /\bJOIN\s+(?:(\w+)\.)?(\w+)(?:\s+(?:AS\s+)?(\w+))?/gi
  );
  for (const match of joinMatches) {
    references.add(match[2]);
  }

  // Match subquery FROM references (simplified - doesn't handle deeply nested)
  const subqueryMatches = selectQuery.matchAll(
    /\(\s*SELECT.*?\bFROM\s+(?:(\w+)\.)?(\w+)/gi
  );
  for (const match of subqueryMatches) {
    references.add(match[2]);
  }

  // Filter out common SQL keywords that might be matched
  const filtered = Array.from(references).filter(
    ref => !isKeyword(ref) && ref.toLowerCase() !== 'dual'
  );

  // Can't distinguish tables from views without registry access
  return {
    tables: filtered,
    possibleViews: [], // Will be determined by registry
  };
}

// =============================================================================
// View Definition Validation
// =============================================================================

/**
 * Validate a parsed view definition
 *
 * @param view - The parsed view to validate
 * @returns Validation result
 */
export function validateViewDefinition(view: ParsedView): {
  valid: boolean;
  errors: string[];
  warnings: string[];
} {
  const errors: string[] = [];
  const warnings: string[] = [];

  // Check view name
  if (!view.name || !/^\w+$/.test(view.name)) {
    errors.push('Invalid view name');
  }

  // Check for reserved names
  const reserved = ['sqlite_', 'pg_', 'information_schema'];
  if (reserved.some(r => view.name.toLowerCase().startsWith(r))) {
    errors.push(`View name '${view.name}' uses a reserved prefix`);
  }

  // Check SELECT query
  if (!view.selectQuery || view.selectQuery.trim().length === 0) {
    errors.push('View must have a SELECT query');
  }

  // Check for SELECT * with column aliases
  if (view.columns && view.columns.length > 0) {
    if (/\bSELECT\s+\*/i.test(view.selectQuery)) {
      warnings.push(
        'Using SELECT * with column aliases may cause issues if the underlying table changes'
      );
    }
  }

  // Check for potential recursion (self-reference)
  const refs = extractReferences(view.selectQuery);
  if (refs.tables.includes(view.name)) {
    errors.push('View cannot reference itself (recursive views not supported)');
  }

  // Warn about complex views
  if (/\bUNION\b/i.test(view.selectQuery)) {
    warnings.push('Views with UNION may not be updatable');
  }
  if (/\bDISTINCT\b/i.test(view.selectQuery)) {
    warnings.push('Views with DISTINCT may not be updatable');
  }
  if (/\bGROUP\s+BY\b/i.test(view.selectQuery)) {
    warnings.push('Views with GROUP BY may not be updatable');
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
  };
}

// =============================================================================
// SQL Generation
// =============================================================================

/**
 * Generate CREATE VIEW SQL from a parsed view
 */
export function generateCreateViewSql(view: ParsedView): string {
  const parts: string[] = ['CREATE'];

  if (view.orReplace) {
    parts.push('OR REPLACE');
  }

  if (view.temporary) {
    parts.push('TEMPORARY');
  }

  parts.push('VIEW');

  if (view.ifNotExists) {
    parts.push('IF NOT EXISTS');
  }

  if (view.schema) {
    parts.push(`${view.schema}.${view.name}`);
  } else {
    parts.push(view.name);
  }

  if (view.columns && view.columns.length > 0) {
    parts.push(`(${view.columns.join(', ')})`);
  }

  parts.push('AS');
  parts.push(view.selectQuery);

  if (view.checkOption) {
    parts.push(`WITH ${view.checkOption === 'LOCAL' ? 'LOCAL ' : ''}CHECK OPTION`);
  }

  return parts.join(' ');
}

/**
 * Generate DROP VIEW SQL
 */
export function generateDropViewSql(
  name: string,
  options?: { ifExists?: boolean; cascade?: boolean; schema?: string }
): string {
  const parts = ['DROP VIEW'];

  if (options?.ifExists) {
    parts.push('IF EXISTS');
  }

  if (options?.schema) {
    parts.push(`${options.schema}.${name}`);
  } else {
    parts.push(name);
  }

  if (options?.cascade) {
    parts.push('CASCADE');
  }

  return parts.join(' ');
}
