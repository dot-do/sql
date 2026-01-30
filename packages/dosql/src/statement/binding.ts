/**
 * Parameter Binding for DoSQL
 *
 * Supports multiple binding styles:
 * - Positional: ? (1-indexed internally)
 * - Numbered: ?1, ?2, ?NNN
 * - Named: :name, @name, $name
 */

import type { SqlValue, NamedParameters, BindParameters } from './types.js';
import {
  BindingError,
  BindingErrorCode,
  createMissingNamedParamError,
  createMissingPositionalParamError,
  createInvalidTypeError,
  createCountMismatchError,
  createNamedExpectedError,
  createNonFiniteNumberError,
  createMixedParametersError,
  createArrayForNamedParamsError,
} from '../errors/index.js';

// Re-export BindingError for backwards compatibility
export { BindingError, BindingErrorCode } from '../errors/index.js';

// =============================================================================
// PARAMETER TOKEN TYPES
// =============================================================================

/**
 * Types of parameter tokens found in SQL
 */
export type ParameterType = 'positional' | 'numbered' | 'named';

/**
 * Parsed parameter token
 */
export interface ParameterToken {
  /**
   * Type of parameter
   */
  type: ParameterType;

  /**
   * Original text including prefix (e.g., '?1', ':name')
   */
  text: string;

  /**
   * Parameter index (for positional/numbered) or name (for named)
   */
  key: number | string;

  /**
   * Start position in SQL string
   */
  start: number;

  /**
   * End position in SQL string
   */
  end: number;
}

/**
 * Result of parsing parameters from SQL
 */
export interface ParsedParameters {
  /**
   * SQL with parameters normalized to ?
   */
  normalizedSql: string;

  /**
   * Original SQL
   */
  originalSql: string;

  /**
   * Parsed parameter tokens
   */
  tokens: ParameterToken[];

  /**
   * Whether any positional parameters (?) were found
   */
  hasPositionalParameters: boolean;

  /**
   * Whether any named parameters were found
   */
  hasNamedParameters: boolean;

  /**
   * Whether any numbered parameters were found
   */
  hasNumberedParameters: boolean;

  /**
   * Maximum numbered parameter index found
   */
  maxNumberedIndex: number;

  /**
   * Set of named parameter names
   */
  namedParameterNames: Set<string>;
}

// =============================================================================
// SQL PARSING STATE
// =============================================================================

/**
 * Context for tracking parsing state
 */
interface ParseContext {
  sql: string;
  position: number;
  tokens: ParameterToken[];
  positionalIndex: number;
  inString: boolean;
  stringChar: string;
}

// =============================================================================
// PARAMETER PARSING
// =============================================================================

/**
 * Regular expressions for parameter detection
 */
const NUMBERED_PARAM_REGEX = /^\?(\d+)/;
const NAMED_PARAM_REGEX = /^([:@$])([a-zA-Z_][a-zA-Z0-9_]*)/;

/**
 * Check if character is a valid identifier start
 */
function isIdentifierStart(char: string): boolean {
  return /[a-zA-Z_]/.test(char);
}

/**
 * Check if character is a valid identifier continuation
 */
function isIdentifierChar(char: string): boolean {
  return /[a-zA-Z0-9_]/.test(char);
}

/**
 * Parse a numbered parameter (?NNN)
 */
function parseNumberedParam(sql: string, position: number): ParameterToken | null {
  const remaining = sql.slice(position);
  const match = NUMBERED_PARAM_REGEX.exec(remaining);

  if (match) {
    const index = parseInt(match[1], 10);
    return {
      type: 'numbered',
      text: match[0],
      key: index,
      start: position,
      end: position + match[0].length,
    };
  }

  return null;
}

/**
 * Parse a named parameter (:name, @name, $name)
 */
function parseNamedParam(sql: string, position: number): ParameterToken | null {
  const remaining = sql.slice(position);
  const match = NAMED_PARAM_REGEX.exec(remaining);

  if (match) {
    const name = match[2];
    return {
      type: 'named',
      text: match[0],
      key: name,
      start: position,
      end: position + match[0].length,
    };
  }

  return null;
}

/**
 * Parse all parameters from a SQL string
 *
 * @param sql - SQL string to parse
 * @returns Parsed parameter information
 *
 * @example
 * ```typescript
 * const result = parseParameters('SELECT * FROM users WHERE id = ? AND name = :name');
 * console.log(result.tokens);
 * // [
 * //   { type: 'positional', text: '?', key: 1, start: 36, end: 37 },
 * //   { type: 'named', text: ':name', key: 'name', start: 50, end: 55 }
 * // ]
 * ```
 */
export function parseParameters(sql: string): ParsedParameters {
  const tokens: ParameterToken[] = [];
  let positionalIndex = 0;
  let maxNumberedIndex = 0;
  const namedParameterNames = new Set<string>();
  let hasPositionalParameters = false;
  let hasNamedParameters = false;
  let hasNumberedParameters = false;

  let inString = false;
  let stringChar = '';
  let i = 0;

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
      // Check for escaped quote
      if (char === stringChar) {
        if (sql[i + 1] === stringChar) {
          // Escaped quote, skip both
          i += 2;
          continue;
        } else {
          // End of string
          inString = false;
          stringChar = '';
        }
      }
      i++;
      continue;
    }

    // Handle comments
    if (char === '-' && sql[i + 1] === '-') {
      // Single line comment - skip to end of line
      while (i < sql.length && sql[i] !== '\n') {
        i++;
      }
      continue;
    }

    if (char === '/' && sql[i + 1] === '*') {
      // Multi-line comment - skip to */
      i += 2;
      while (i < sql.length - 1 && !(sql[i] === '*' && sql[i + 1] === '/')) {
        i++;
      }
      i += 2; // Skip */
      continue;
    }

    // Handle parameters
    if (char === '?') {
      // Try numbered first
      const numberedToken = parseNumberedParam(sql, i);
      if (numberedToken) {
        tokens.push(numberedToken);
        hasNumberedParameters = true;
        maxNumberedIndex = Math.max(maxNumberedIndex, numberedToken.key as number);
        i = numberedToken.end;
        continue;
      }

      // Positional parameter
      positionalIndex++;
      hasPositionalParameters = true;
      tokens.push({
        type: 'positional',
        text: '?',
        key: positionalIndex,
        start: i,
        end: i + 1,
      });
      i++;
      continue;
    }

    // Handle named parameters
    if (char === ':' || char === '@' || char === '$') {
      // Skip :: (PostgreSQL type cast syntax) - not a named parameter
      if (char === ':' && sql[i + 1] === ':') {
        // Skip both colons and the identifier that follows
        i += 2;
        // Skip the identifier after :: (e.g., ::text, ::jsonb)
        while (i < sql.length && isIdentifierChar(sql[i])) {
          i++;
        }
        continue;
      }

      const namedToken = parseNamedParam(sql, i);
      if (namedToken) {
        tokens.push(namedToken);
        hasNamedParameters = true;
        namedParameterNames.add(namedToken.key as string);
        i = namedToken.end;
        continue;
      }
    }

    i++;
  }

  // Normalize SQL to use ? placeholders
  let normalizedSql = sql;
  let offset = 0;

  for (const token of tokens) {
    const before = normalizedSql.slice(0, token.start + offset);
    const after = normalizedSql.slice(token.end + offset);
    normalizedSql = before + '?' + after;
    offset += 1 - token.text.length;
  }

  return {
    normalizedSql,
    originalSql: sql,
    tokens,
    hasPositionalParameters,
    hasNamedParameters,
    hasNumberedParameters,
    maxNumberedIndex,
    namedParameterNames,
  };
}

// =============================================================================
// PARAMETER BINDING
// =============================================================================

/**
 * Check if parameters are named (object) vs positional (array)
 */
export function isNamedParameters(params: BindParameters): params is NamedParameters {
  return params !== null && typeof params === 'object' && !Array.isArray(params);
}

/**
 * Coerce a JavaScript value to a SQL-compatible value
 *
 * @param value - Value to coerce
 * @returns SQL-compatible value
 */
export function coerceValue(value: unknown): SqlValue {
  if (value === undefined) {
    return null;
  }

  if (value === null) {
    return null;
  }

  if (typeof value === 'string') {
    return value;
  }

  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw createNonFiniteNumberError(value);
    }
    return value;
  }

  if (typeof value === 'bigint') {
    return value;
  }

  if (typeof value === 'boolean') {
    // SQLite uses 0/1 for booleans
    return value ? 1 : 0;
  }

  if (value instanceof Date) {
    // Store dates as ISO strings
    return value.toISOString();
  }

  if (value instanceof Uint8Array) {
    return value;
  }

  if (ArrayBuffer.isView(value)) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }

  if (value instanceof ArrayBuffer) {
    return new Uint8Array(value);
  }

  // For objects/arrays, serialize to JSON
  if (typeof value === 'object') {
    return JSON.stringify(value);
  }

  throw createInvalidTypeError(typeof value);
}

/**
 * Bind parameters to parsed SQL
 *
 * @param parsed - Parsed parameter information
 * @param params - Parameters to bind
 * @returns Array of bound values in positional order
 *
 * @example
 * ```typescript
 * const parsed = parseParameters('SELECT * FROM users WHERE id = ? AND name = :name');
 * const values = bindParameters(parsed, { name: 'Alice' }, [1]);
 * // values = [1, 'Alice']
 * ```
 */
export function bindParameters(
  parsed: ParsedParameters,
  ...params: unknown[]
): SqlValue[] {
  const values: SqlValue[] = [];

  // Check for mixed positional and named parameters
  if (parsed.hasPositionalParameters && parsed.hasNamedParameters) {
    throw createMixedParametersError();
  }

  // Handle case where single object is passed for named parameters
  const firstParam = params[0];
  const isNamed = params.length === 1 && isNamedParameters(firstParam as BindParameters);

  // Check if array is passed for named parameters
  if (parsed.hasNamedParameters && params.length === 1 && Array.isArray(firstParam)) {
    throw createArrayForNamedParamsError();
  }

  const namedParams = isNamed ? (firstParam as NamedParameters) : {};
  const positionalParams = isNamed ? [] : (params as SqlValue[]);

  // Track which positional parameters have been used
  let positionalIndex = 0;

  for (const token of parsed.tokens) {
    let value: unknown;

    switch (token.type) {
      case 'positional':
        if (positionalIndex >= positionalParams.length) {
          throw createMissingPositionalParamError(positionalIndex + 1);
        }
        value = positionalParams[positionalIndex++];
        break;

      case 'numbered':
        const numIndex = (token.key as number) - 1; // Convert to 0-indexed
        if (numIndex >= positionalParams.length) {
          throw createMissingPositionalParamError(token.key as number);
        }
        value = positionalParams[numIndex];
        break;

      case 'named':
        const name = token.key as string;
        if (!(name in namedParams)) {
          throw createMissingNamedParamError(name);
        }
        value = namedParams[name];
        break;
    }

    values.push(coerceValue(value));
  }

  return values;
}

/**
 * Validate that all required parameters are provided
 *
 * @param parsed - Parsed parameter information
 * @param params - Parameters provided
 * @throws BindingError if validation fails
 */
export function validateParameters(
  parsed: ParsedParameters,
  ...params: unknown[]
): void {
  // Check for mixed positional and named parameters
  if (parsed.hasPositionalParameters && parsed.hasNamedParameters) {
    throw createMixedParametersError();
  }

  const firstParam = params[0];
  const isNamed = params.length === 1 && isNamedParameters(firstParam as BindParameters);

  if (parsed.hasNamedParameters && !isNamed) {
    throw createNamedExpectedError();
  }

  if (isNamed) {
    const namedParams = firstParam as NamedParameters;
    for (const name of parsed.namedParameterNames) {
      if (!(name in namedParams)) {
        throw createMissingNamedParamError(name);
      }
    }
  } else {
    const positionalCount = parsed.tokens.filter(
      t => t.type === 'positional'
    ).length;

    if (parsed.hasNumberedParameters) {
      if (params.length < parsed.maxNumberedIndex) {
        throw createCountMismatchError(parsed.maxNumberedIndex, params.length);
      }
    } else if (params.length < positionalCount) {
      throw createCountMismatchError(positionalCount, params.length);
    }
  }
}

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

/**
 * Escape a string value for SQL (for debugging/logging only)
 * Use parameter binding for actual queries!
 */
export function escapeString(value: string): string {
  return "'" + value.replace(/'/g, "''") + "'";
}

/**
 * Format SQL with bound values (for debugging/logging)
 *
 * @param sql - SQL string with parameters
 * @param values - Bound values
 * @returns Formatted SQL string
 */
export function formatSqlWithValues(sql: string, values: SqlValue[]): string {
  let result = sql;
  let valueIndex = 0;

  // Simple replacement - for debugging only
  result = result.replace(/\?(\d+)?|[:@$][a-zA-Z_][a-zA-Z0-9_]*/g, () => {
    const value = values[valueIndex++];

    if (value === null) {
      return 'NULL';
    }

    if (typeof value === 'string') {
      return escapeString(value);
    }

    if (typeof value === 'number' || typeof value === 'bigint') {
      return String(value);
    }

    if (value instanceof Uint8Array) {
      return `X'${Buffer.from(value).toString('hex')}'`;
    }

    return String(value);
  });

  return result;
}
