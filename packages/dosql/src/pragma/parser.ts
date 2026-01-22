/**
 * DoSQL PRAGMA Parser
 *
 * Parses SQLite-compatible PRAGMA statements including:
 * - PRAGMA name (get value)
 * - PRAGMA name = value (set value)
 * - PRAGMA schema.name (schema-qualified)
 * - PRAGMA name(table) (table-valued function style)
 */

import type {
  PragmaStatement,
  PragmaValue,
  PragmaMode,
  PragmaParseResult,
} from './types.js';

// =============================================================================
// PARSER UTILITIES
// =============================================================================

/**
 * Whitespace characters
 */
const WHITESPACE = /^[\s\t\n\r]+/;

/**
 * Identifier pattern (letters, digits, underscore)
 */
const IDENTIFIER = /^[a-zA-Z_][a-zA-Z0-9_]*/;

/**
 * Numeric literal pattern
 */
const NUMBER = /^-?\d+(\.\d+)?/;

/**
 * String literal pattern (single or double quotes)
 */
const STRING_SINGLE = /^'([^']*(?:''[^']*)*)'/;
const STRING_DOUBLE = /^"([^"]*(?:""[^"]*)*)"/;

/**
 * Parser state
 */
interface ParserState {
  input: string;
  position: number;
}

/**
 * Create initial parser state
 */
function createState(input: string): ParserState {
  return { input, position: 0 };
}

/**
 * Get remaining input
 */
function remaining(state: ParserState): string {
  return state.input.slice(state.position);
}

/**
 * Check if at end of input
 */
function isEOF(state: ParserState): boolean {
  return state.position >= state.input.length;
}

/**
 * Skip whitespace
 */
function skipWhitespace(state: ParserState): void {
  const match = remaining(state).match(WHITESPACE);
  if (match) {
    state.position += match[0].length;
  }
}

/**
 * Try to match a literal string (case-insensitive for keywords)
 */
function matchLiteral(state: ParserState, literal: string, caseInsensitive = true): boolean {
  skipWhitespace(state);
  const rem = remaining(state);
  const compare = caseInsensitive ? rem.toUpperCase() : rem;
  const lit = caseInsensitive ? literal.toUpperCase() : literal;

  if (compare.startsWith(lit)) {
    // Ensure we're not matching a partial identifier
    const nextChar = rem[literal.length];
    if (nextChar && /[a-zA-Z0-9_]/.test(nextChar)) {
      return false;
    }
    state.position += literal.length;
    return true;
  }
  return false;
}

/**
 * Try to match an identifier
 */
function matchIdentifier(state: ParserState): string | null {
  skipWhitespace(state);
  const match = remaining(state).match(IDENTIFIER);
  if (match) {
    state.position += match[0].length;
    return match[0];
  }
  return null;
}

/**
 * Try to match a number
 */
function matchNumber(state: ParserState): number | null {
  skipWhitespace(state);
  const match = remaining(state).match(NUMBER);
  if (match) {
    state.position += match[0].length;
    return parseFloat(match[0]);
  }
  return null;
}

/**
 * Try to match a string literal
 */
function matchString(state: ParserState): string | null {
  skipWhitespace(state);
  const rem = remaining(state);

  // Single-quoted string
  let match = rem.match(STRING_SINGLE);
  if (match) {
    state.position += match[0].length;
    // Unescape doubled single quotes
    return match[1].replace(/''/g, "'");
  }

  // Double-quoted string
  match = rem.match(STRING_DOUBLE);
  if (match) {
    state.position += match[0].length;
    // Unescape doubled double quotes
    return match[1].replace(/""/g, '"');
  }

  return null;
}

/**
 * Try to match a character
 */
function matchChar(state: ParserState, char: string): boolean {
  skipWhitespace(state);
  if (remaining(state)[0] === char) {
    state.position += 1;
    return true;
  }
  return false;
}

/**
 * Peek at next non-whitespace character
 */
function peekChar(state: ParserState): string | undefined {
  skipWhitespace(state);
  return remaining(state)[0];
}

// =============================================================================
// VALUE PARSING
// =============================================================================

/**
 * Parse a PRAGMA value (for set operations)
 */
function parseValue(state: ParserState): PragmaValue | null {
  skipWhitespace(state);

  // Try boolean keywords
  if (matchLiteral(state, 'ON') || matchLiteral(state, 'TRUE') || matchLiteral(state, 'YES')) {
    return true;
  }
  if (matchLiteral(state, 'OFF') || matchLiteral(state, 'FALSE') || matchLiteral(state, 'NO')) {
    return false;
  }

  // Try NULL
  if (matchLiteral(state, 'NULL')) {
    return null;
  }

  // Try string literal first (before identifier check)
  const strValue = matchString(state);
  if (strValue !== null) {
    return strValue;
  }

  // Try number
  const numValue = matchNumber(state);
  if (numValue !== null) {
    return numValue;
  }

  // Try unquoted identifier as string value (e.g., WAL, DELETE, MEMORY)
  const identValue = matchIdentifier(state);
  if (identValue !== null) {
    return identValue;
  }

  return null;
}

// =============================================================================
// MAIN PARSER
// =============================================================================

/**
 * Parse a PRAGMA statement
 *
 * Supported formats:
 * - PRAGMA name
 * - PRAGMA name = value
 * - PRAGMA schema.name
 * - PRAGMA schema.name = value
 * - PRAGMA name(argument)
 * - PRAGMA schema.name(argument)
 *
 * @param sql - The SQL string to parse
 * @returns Parse result with the PRAGMA AST or error
 */
export function parsePragma(sql: string): PragmaParseResult {
  const state = createState(sql.trim());

  // Must start with PRAGMA
  if (!matchLiteral(state, 'PRAGMA')) {
    return {
      success: false,
      error: 'Expected PRAGMA keyword',
      position: state.position,
    };
  }

  // Parse pragma name (possibly schema-qualified)
  const firstName = matchIdentifier(state);
  if (!firstName) {
    return {
      success: false,
      error: 'Expected PRAGMA name',
      position: state.position,
    };
  }

  let schema: string | undefined;
  let name: string;

  // Check for schema.name pattern
  if (matchChar(state, '.')) {
    schema = firstName;
    const secondName = matchIdentifier(state);
    if (!secondName) {
      return {
        success: false,
        error: 'Expected PRAGMA name after schema prefix',
        position: state.position,
      };
    }
    name = secondName;
  } else {
    name = firstName;
  }

  // Determine operation mode
  let mode: PragmaMode = 'get';
  let value: PragmaValue | undefined;
  let argument: string | undefined;

  skipWhitespace(state);

  // Check for function call style: name(argument)
  if (matchChar(state, '(')) {
    mode = 'call';

    // Parse argument - can be identifier or string
    const strArg = matchString(state);
    if (strArg !== null) {
      argument = strArg;
    } else {
      const identArg = matchIdentifier(state);
      if (identArg) {
        argument = identArg;
      }
      // argument can be empty for some pragmas
    }

    if (!matchChar(state, ')')) {
      return {
        success: false,
        error: 'Expected closing parenthesis',
        position: state.position,
      };
    }
  }
  // Check for assignment: name = value
  else if (matchChar(state, '=')) {
    mode = 'set';
    const startPos = state.position;
    const parsedValue = parseValue(state);
    // If we didn't move past the = sign, there's no value
    if (state.position === startPos && !isEOF(state)) {
      const nextChar = peekChar(state);
      if (nextChar && nextChar !== ';') {
        return {
          success: false,
          error: 'Expected value after =',
          position: state.position,
        };
      }
    }
    value = parsedValue;
  }

  // Skip any trailing whitespace
  skipWhitespace(state);

  // Handle optional semicolon
  matchChar(state, ';');
  skipWhitespace(state);

  // Should be at end of input
  if (!isEOF(state)) {
    return {
      success: false,
      error: `Unexpected characters after PRAGMA statement: ${remaining(state)}`,
      position: state.position,
    };
  }

  // Build the statement
  const statement: PragmaStatement = {
    type: 'PRAGMA',
    name: name.toLowerCase(), // Normalize to lowercase
    mode,
  };

  if (schema) {
    statement.schema = schema.toLowerCase();
  }

  // null is a valid value, so only check for undefined
  if (mode === 'set') {
    statement.value = value;
  }

  if (argument !== undefined) {
    statement.argument = argument;
  }

  return {
    success: true,
    statement,
  };
}

/**
 * Try to parse a PRAGMA statement, returning null on failure
 */
export function tryParsePragma(sql: string): PragmaStatement | null {
  const result = parsePragma(sql);
  return result.success ? result.statement : null;
}

/**
 * Check if a SQL string is a PRAGMA statement
 */
export function isPragmaStatement(sql: string): boolean {
  const trimmed = sql.trim().toUpperCase();
  return trimmed.startsWith('PRAGMA ') || trimmed === 'PRAGMA';
}

/**
 * Format a PRAGMA statement back to SQL
 */
export function formatPragma(stmt: PragmaStatement): string {
  let sql = 'PRAGMA ';

  if (stmt.schema) {
    sql += `${stmt.schema}.`;
  }

  sql += stmt.name;

  if (stmt.mode === 'call' && stmt.argument !== undefined) {
    // Table/index names in function-style pragmas should be quoted
    sql += `('${stmt.argument.replace(/'/g, "''")}')`;
  } else if (stmt.mode === 'set') {
    sql += ` = ${formatValue(stmt.value)}`;
  }

  return sql;
}

/**
 * Format a PRAGMA value for SQL output
 */
function formatValue(value: PragmaValue): string {
  if (value === null) {
    return 'NULL';
  }
  if (typeof value === 'boolean') {
    return value ? 'ON' : 'OFF';
  }
  if (typeof value === 'number') {
    return String(value);
  }
  if (typeof value === 'string') {
    // Use single quotes for strings, escape internal quotes
    if (/^[A-Z_]+$/i.test(value)) {
      // Looks like a keyword (WAL, DELETE, etc.) - use unquoted
      return value.toUpperCase();
    }
    return `'${value.replace(/'/g, "''")}'`;
  }
  return String(value);
}
