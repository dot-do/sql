/**
 * Syntax Error Classes
 *
 * Standardized error classes for SQL syntax errors with location information.
 * Includes helpful suggestions for common typos and formatted error output.
 *
 * @packageDocumentation
 */

import {
  DoSQLError,
  ErrorCategory,
  registerErrorClass,
  type ErrorContext,
  type SerializedError,
} from './base.js';
import { SyntaxErrorCode } from './codes.js';

// =============================================================================
// Source Location
// =============================================================================

// Import from parser shared types for compatibility
import type { SourceLocation as ParserSourceLocation } from '../parser/shared/types.js';

/**
 * Location in source SQL where error occurred
 * Re-export with the same structure as parser/shared/types.ts
 */
export interface SourceLocation {
  /** Line number (1-indexed) */
  line: number;
  /** Column number (1-indexed) */
  column: number;
  /** Character offset (0-indexed) */
  offset: number;
}

// Type assertion to ensure compatibility
const _typeCheck: SourceLocation = {} as ParserSourceLocation;
void _typeCheck;

// =============================================================================
// Common Typos and Suggestions
// =============================================================================

/**
 * Map of common SQL keyword typos to their correct form
 */
export const COMMON_TYPOS: Record<string, string> = {
  // FROM typos
  'FORM': 'FROM',
  'FOMR': 'FROM',
  'FRON': 'FROM',
  'RFOM': 'FROM',
  'FRMO': 'FROM',

  // WHERE typos
  'WHER': 'WHERE',
  'WHRE': 'WHERE',
  'WEHRE': 'WHERE',
  'WERE': 'WHERE',
  'WHEER': 'WHERE',

  // SELECT typos
  'SLECT': 'SELECT',
  'SELCT': 'SELECT',
  'SELET': 'SELECT',
  'SELCET': 'SELECT',
  'SEELCT': 'SELECT',
  'SELEC': 'SELECT',

  // INSERT typos
  'INSER': 'INSERT',
  'INSRT': 'INSERT',
  'ISERT': 'INSERT',
  'INSRET': 'INSERT',
  'ISNERT': 'INSERT',

  // UPDATE typos
  'UPDAT': 'UPDATE',
  'UPADTE': 'UPDATE',
  'UDPATE': 'UPDATE',
  'UPDAE': 'UPDATE',
  'UPDTE': 'UPDATE',

  // DELETE typos
  'DELET': 'DELETE',
  'DELEET': 'DELETE',
  'DELTE': 'DELETE',
  'DLEET': 'DELETE',
  'DEELTE': 'DELETE',

  // CREATE typos
  'CREAT': 'CREATE',
  'CRATE': 'CREATE',
  'CRAETE': 'CREATE',
  'CEATE': 'CREATE',

  // TABLE typos
  'TABL': 'TABLE',
  'TABEL': 'TABLE',
  'TALBE': 'TABLE',
  'TBALE': 'TABLE',

  // VALUES typos
  'VALUSE': 'VALUES',
  'VLAUES': 'VALUES',
  'VALES': 'VALUES',
  'VAULES': 'VALUES',

  // INTO typos
  'ITNO': 'INTO',
  'INOT': 'INTO',
  'INTO': 'INTO',

  // JOIN typos
  'JION': 'JOIN',
  'JOINN': 'JOIN',
  'JOUN': 'JOIN',

  // ORDER typos
  'ORDR': 'ORDER',
  'ORER': 'ORDER',
  'OERDR': 'ORDER',

  // GROUP typos
  'GROP': 'GROUP',
  'GRUOP': 'GROUP',
  'GROPU': 'GROUP',

  // HAVING typos
  'HAVIGN': 'HAVING',
  'HVAIN': 'HAVING',
  'HAIVNG': 'HAVING',

  // LIMIT typos
  'LIMT': 'LIMIT',
  'LIMTI': 'LIMIT',
  'LMIIT': 'LIMIT',

  // INDEX typos
  'INDX': 'INDEX',
  'IDNEX': 'INDEX',
  'INEDX': 'INDEX',

  // COLUMN typos
  'COLUM': 'COLUMN',
  'COULMN': 'COLUMN',
  'COLMUN': 'COLUMN',

  // DISTINCT typos
  'DISTICT': 'DISTINCT',
  'DISTNCT': 'DISTINCT',
  'DISTINC': 'DISTINCT',

  // BETWEEN typos
  'BEETWEEN': 'BETWEEN',
  'BETEEN': 'BETWEEN',
  'BETWENE': 'BETWEEN',

  // RETURNING typos
  'RETURING': 'RETURNING',
  'RETURNIG': 'RETURNING',
  'REUTRNING': 'RETURNING',

  // SET typos
  'STE': 'SET',
  'ST': 'SET',

  // AND typos
  'ADN': 'AND',
  'NAD': 'AND',

  // OR typos (less common due to short length)
  'RO': 'OR',

  // NULL typos
  'NULK': 'NULL',
  'NULLL': 'NULL',
  'NUL': 'NULL',

  // DEFAULT typos
  'DEFAUTL': 'DEFAULT',
  'DEFUALT': 'DEFAULT',
  'DFEAULT': 'DEFAULT',

  // PRIMARY typos
  'PRIMAY': 'PRIMARY',
  'PRIMRY': 'PRIMARY',
  'PIMARY': 'PRIMARY',

  // FOREIGN typos
  'FORIEGN': 'FOREIGN',
  'FOREGIN': 'FOREIGN',
  'FOREING': 'FOREIGN',

  // REFERENCES typos
  'REFERNCES': 'REFERENCES',
  'REFRENCES': 'REFERENCES',
  'REFERECNES': 'REFERENCES',

  // CONSTRAINT typos
  'CONSTAINT': 'CONSTRAINT',
  'CONSTRATIN': 'CONSTRAINT',
  'CONSRAINT': 'CONSTRAINT',

  // TRIGGER typos
  'TRIGGE': 'TRIGGER',
  'TRIGGR': 'TRIGGER',
  'TRIGERR': 'TRIGGER',

  // VIEW typos
  'VEIW': 'VIEW',
  'VIWE': 'VIEW',

  // ALTER typos
  'ALTR': 'ALTER',
  'ATLER': 'ALTER',
  'ALETER': 'ALTER',

  // DROP typos
  'DORP': 'DROP',
  'DRPO': 'DROP',
  'DRIP': 'DROP',
};

/**
 * Calculate Levenshtein distance between two strings
 */
export function levenshteinDistance(a: string, b: string): number {
  const matrix: number[][] = [];

  for (let i = 0; i <= b.length; i++) {
    matrix[i] = [i];
  }

  for (let j = 0; j <= a.length; j++) {
    matrix[0][j] = j;
  }

  for (let i = 1; i <= b.length; i++) {
    for (let j = 1; j <= a.length; j++) {
      if (b.charAt(i - 1) === a.charAt(j - 1)) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1, // substitution
          matrix[i][j - 1] + 1,     // insertion
          matrix[i - 1][j] + 1      // deletion
        );
      }
    }
  }

  return matrix[b.length][a.length];
}

/**
 * Get a suggestion for a potentially misspelled keyword
 * First checks the explicit typo map, then uses fuzzy matching
 */
export function getSuggestionForTypo(token: string, candidates?: string[]): string | undefined {
  const upperToken = token.toUpperCase();

  // First check explicit typo map
  if (COMMON_TYPOS[upperToken]) {
    return COMMON_TYPOS[upperToken];
  }

  // If no candidates provided, use common SQL keywords
  const defaultCandidates = [
    'SELECT', 'FROM', 'WHERE', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET',
    'DELETE', 'CREATE', 'TABLE', 'INDEX', 'VIEW', 'DROP', 'ALTER', 'ADD',
    'COLUMN', 'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'CONSTRAINT',
    'NOT', 'NULL', 'DEFAULT', 'UNIQUE', 'CHECK', 'AND', 'OR', 'IN', 'LIKE',
    'BETWEEN', 'IS', 'AS', 'ON', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
    'GROUP', 'BY', 'HAVING', 'ORDER', 'ASC', 'DESC', 'LIMIT', 'OFFSET',
    'DISTINCT', 'ALL', 'UNION', 'INTERSECT', 'EXCEPT', 'CASE', 'WHEN',
    'THEN', 'ELSE', 'END', 'RETURNING', 'TRIGGER', 'BEFORE', 'AFTER',
    'BEGIN', 'COMMIT', 'ROLLBACK', 'REPLACE', 'IGNORE', 'CONFLICT',
  ];

  const searchCandidates = candidates ?? defaultCandidates;

  // Find closest match using Levenshtein distance
  let bestMatch: string | undefined;
  let bestDistance = Infinity;
  const maxDistance = Math.max(2, Math.floor(token.length / 3)); // Allow more errors for longer tokens

  for (const candidate of searchCandidates) {
    const distance = levenshteinDistance(upperToken, candidate);
    if (distance < bestDistance && distance <= maxDistance) {
      bestDistance = distance;
      bestMatch = candidate;
    }
  }

  return bestMatch;
}

/**
 * Calculate line and column from offset in source string
 */
export function calculateLocation(sql: string, offset: number): SourceLocation {
  let line = 1;
  let column = 1;

  for (let i = 0; i < offset && i < sql.length; i++) {
    if (sql[i] === '\n') {
      line++;
      column = 1;
    } else {
      column++;
    }
  }

  return { line, column, offset };
}

/**
 * Format a source code snippet with error highlighting
 */
export function formatErrorSnippet(
  sql: string,
  location: SourceLocation,
  tokenLength: number = 1
): string {
  const lines = sql.split('\n');
  const errorLine = lines[location.line - 1];

  if (!errorLine) {
    return '';
  }

  const parts: string[] = [];

  // Add line number prefix
  const lineNumStr = `${location.line} | `;
  parts.push(`  ${lineNumStr}${errorLine}`);

  // Add error pointer line
  const pointerPadding = ' '.repeat(lineNumStr.length + location.column - 1 + 2);
  const pointer = '^'.repeat(Math.max(1, Math.min(tokenLength, errorLine.length - location.column + 1)));
  parts.push(`${pointerPadding}${pointer}`);

  return parts.join('\n');
}

// =============================================================================
// SQL Syntax Error
// =============================================================================

/**
 * SQL Syntax Error with location information
 *
 * @example
 * ```typescript
 * try {
 *   parser.parse('SELECT * FORM users');
 * } catch (error) {
 *   if (error instanceof SQLSyntaxError) {
 *     console.log(error.code);      // 'SYNTAX_UNEXPECTED_TOKEN'
 *     console.log(error.location);  // { line: 1, column: 10, offset: 9 }
 *     console.log(error.format());  // Formatted error with context
 *   }
 * }
 * ```
 *
 * @example Formatted output
 * ```
 * SQLSyntaxError: Unexpected token 'FORM' at line 1, column 10
 *   1 | SELECT * FORM users
 *              ^^^^
 *   Did you mean 'FROM'?
 * ```
 */
export class SQLSyntaxError extends DoSQLError {
  readonly code: SyntaxErrorCode;
  readonly category = ErrorCategory.VALIDATION;

  /** Source location where error occurred */
  location?: SourceLocation;

  /** Original SQL input */
  sql?: string;

  /** Suggested fix (if available) */
  suggestion?: string;

  /** The problematic token (if available) */
  token?: string;

  /** Expected token(s) (if available) */
  expected?: string;

  constructor(
    code: SyntaxErrorCode,
    message: string,
    location?: SourceLocation,
    sql?: string,
    options?: { cause?: Error; context?: ErrorContext; token?: string; expected?: string }
  ) {
    // Build message with location
    const locStr = location
      ? ` at line ${location.line}, column ${location.column}`
      : '';
    super(message + locStr, options);

    this.name = 'SQLSyntaxError';
    this.code = code;
    this.location = location;
    this.sql = sql;
    this.token = options?.token;
    this.expected = options?.expected;

    // Auto-generate suggestion for typos
    if (options?.token && !this.suggestion) {
      const suggestion = getSuggestionForTypo(options.token);
      if (suggestion) {
        this.suggestion = suggestion;
      }
    }

    // Include SQL in context
    if (sql) {
      this.context = { ...this.context, sql };
    }

    this.recoveryHint = 'Check the SQL syntax near the indicated location';
  }

  isRetryable(): boolean {
    // Syntax errors are user errors, not retryable
    return false;
  }

  toUserMessage(): string {
    return 'There is a syntax error in your SQL. ' + this.message;
  }

  /**
   * Get the problematic portion of SQL near the error
   */
  getNearbyContext(contextLength = 20): string | undefined {
    if (!this.sql || !this.location) return undefined;

    const start = Math.max(0, this.location.offset - contextLength);
    const end = Math.min(this.sql.length, this.location.offset + contextLength);

    let context = this.sql.slice(start, end);
    if (start > 0) context = '...' + context;
    if (end < this.sql.length) context = context + '...';

    return context;
  }

  /**
   * Format error with context for display
   *
   * Produces output like:
   * ```
   * SQLSyntaxError: Unexpected token 'FORM' at line 1, column 10
   *   1 | SELECT * FORM users
   *              ^^^^
   *   Did you mean 'FROM'?
   * ```
   */
  format(): string {
    const parts: string[] = [`${this.name}: ${this.message}`];

    if (this.sql && this.location) {
      const tokenLength = this.token?.length ?? 1;
      const snippet = formatErrorSnippet(this.sql, this.location, tokenLength);
      if (snippet) {
        parts.push(snippet);
      }
    }

    if (this.suggestion) {
      parts.push(`  Did you mean '${this.suggestion}'?`);
    }

    if (this.expected && !this.suggestion) {
      parts.push(`  Expected: ${this.expected}`);
    }

    if (this.recoveryHint) {
      parts.push(`  Hint: ${this.recoveryHint}`);
    }

    return parts.join('\n');
  }

  /**
   * Format error as a compact single line for logging
   */
  formatCompact(): string {
    let msg = `${this.name}: ${this.message}`;
    if (this.suggestion) {
      msg += ` (did you mean '${this.suggestion}'?)`;
    }
    return msg;
  }

  /**
   * Set the suggestion manually
   */
  withSuggestion(suggestion: string): this {
    this.suggestion = suggestion;
    return this;
  }

  /**
   * Deserialize from JSON
   */
  static fromJSON(json: SerializedError): SQLSyntaxError {
    const error = new SQLSyntaxError(
      json.code as SyntaxErrorCode,
      json.message,
      undefined,
      json.context?.sql,
      { context: json.context }
    );
    return error;
  }
}

// Register for deserialization
registerErrorClass('SQLSyntaxError', SQLSyntaxError);

// =============================================================================
// Unexpected Token Error
// =============================================================================

/**
 * Error for unexpected token in SQL
 *
 * @example
 * ```typescript
 * // Throwing with auto-suggestion
 * throw new UnexpectedTokenError('FORM', 'FROM', location, sql);
 *
 * // Will produce:
 * // UnexpectedTokenError: Unexpected token 'FORM', expected FROM at line 1, column 10
 * //   1 | SELECT * FORM users
 * //              ^^^^
 * //   Did you mean 'FROM'?
 * ```
 */
export class UnexpectedTokenError extends SQLSyntaxError {
  constructor(
    token: string,
    expected?: string,
    location?: SourceLocation,
    sql?: string,
    options?: { cause?: Error; context?: ErrorContext }
  ) {
    const message = expected
      ? `Unexpected token '${token}', expected ${expected}`
      : `Unexpected token '${token}'`;

    super(SyntaxErrorCode.UNEXPECTED_TOKEN, message, location, sql, {
      ...options,
      token,
      expected,
    });
    this.name = 'UnexpectedTokenError';
  }
}

registerErrorClass('UnexpectedTokenError', UnexpectedTokenError);

// =============================================================================
// Unexpected EOF Error
// =============================================================================

/**
 * Error for unexpected end of input
 *
 * @example
 * ```typescript
 * // Incomplete statement
 * throw new UnexpectedEOFError(')', location, sql);
 *
 * // Will produce:
 * // UnexpectedEOFError: Unexpected end of input, expected ) at line 1, column 25
 * //   1 | SELECT * FROM users WHERE (id > 5
 * //                                         ^
 * //   Expected: )
 * ```
 */
export class UnexpectedEOFError extends SQLSyntaxError {
  constructor(
    expected?: string,
    location?: SourceLocation,
    sql?: string,
    options?: { cause?: Error; context?: ErrorContext }
  ) {
    const message = expected
      ? `Unexpected end of input, expected ${expected}`
      : 'Unexpected end of input';

    super(SyntaxErrorCode.UNEXPECTED_EOF, message, location, sql, {
      ...options,
      expected,
    });
    this.name = 'UnexpectedEOFError';
  }
}

registerErrorClass('UnexpectedEOFError', UnexpectedEOFError);

// =============================================================================
// Missing Keyword Error
// =============================================================================

/**
 * Error for missing required keyword
 *
 * @example
 * ```typescript
 * throw new MissingKeywordError('FROM', location, sql);
 * ```
 */
export class MissingKeywordError extends SQLSyntaxError {
  /** The missing keyword */
  readonly keyword: string;

  constructor(
    keyword: string,
    location?: SourceLocation,
    sql?: string,
    options?: { cause?: Error; context?: ErrorContext }
  ) {
    const message = `Missing required keyword '${keyword}'`;

    super(SyntaxErrorCode.UNEXPECTED_TOKEN, message, location, sql, {
      ...options,
      expected: keyword,
    });
    this.name = 'MissingKeywordError';
    this.keyword = keyword;
  }
}

registerErrorClass('MissingKeywordError', MissingKeywordError);

// =============================================================================
// Invalid Identifier Error
// =============================================================================

/**
 * Error for invalid identifier
 */
export class InvalidIdentifierError extends SQLSyntaxError {
  /** The invalid identifier */
  readonly identifier: string;

  constructor(
    identifier: string,
    reason?: string,
    location?: SourceLocation,
    sql?: string,
    options?: { cause?: Error; context?: ErrorContext }
  ) {
    const message = reason
      ? `Invalid identifier '${identifier}': ${reason}`
      : `Invalid identifier '${identifier}'`;

    super(SyntaxErrorCode.INVALID_IDENTIFIER, message, location, sql, {
      ...options,
      token: identifier,
    });
    this.name = 'InvalidIdentifierError';
    this.identifier = identifier;
  }
}

registerErrorClass('InvalidIdentifierError', InvalidIdentifierError);

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create error from caught exception
 */
export function createErrorFromException(
  error: unknown,
  sql?: string
): SQLSyntaxError {
  if (error instanceof SQLSyntaxError) {
    if (!error.sql && sql) {
      error.sql = sql;
    }
    return error;
  }

  if (error instanceof Error) {
    // Try to extract location from error message
    const locMatch = error.message.match(/at line (\d+), column (\d+)/);
    const location = locMatch
      ? {
          line: parseInt(locMatch[1], 10),
          column: parseInt(locMatch[2], 10),
          offset: 0,
        }
      : undefined;

    // Try to extract position from error message
    const posMatch = error.message.match(/at position (\d+)/);
    let computedLocation = location;
    if (!computedLocation && posMatch && sql) {
      const offset = parseInt(posMatch[1], 10);
      computedLocation = calculateLocation(sql, offset);
    }

    return new SQLSyntaxError(
      SyntaxErrorCode.GENERAL,
      error.message,
      computedLocation,
      sql,
      { cause: error }
    );
  }

  return new SQLSyntaxError(
    SyntaxErrorCode.GENERAL,
    String(error),
    undefined,
    sql
  );
}

/**
 * Create an unexpected token error with auto-suggestion
 */
export function createUnexpectedTokenError(
  token: string,
  expected: string | string[] | undefined,
  sql: string,
  offset: number
): UnexpectedTokenError {
  const location = calculateLocation(sql, offset);
  const expectedStr = Array.isArray(expected) ? expected.join(' or ') : expected;
  return new UnexpectedTokenError(token, expectedStr, location, sql);
}

/**
 * Create an unexpected EOF error
 */
export function createUnexpectedEOFError(
  expected: string | string[] | undefined,
  sql: string
): UnexpectedEOFError {
  const location = calculateLocation(sql, sql.length);
  const expectedStr = Array.isArray(expected) ? expected.join(' or ') : expected;
  return new UnexpectedEOFError(expectedStr, location, sql);
}

/**
 * Create a missing keyword error
 */
export function createMissingKeywordError(
  keyword: string,
  sql: string,
  offset: number
): MissingKeywordError {
  const location = calculateLocation(sql, offset);
  return new MissingKeywordError(keyword, location, sql);
}

/**
 * Create an invalid identifier error
 */
export function createInvalidIdentifierError(
  identifier: string,
  reason: string | undefined,
  sql: string,
  offset: number
): InvalidIdentifierError {
  const location = calculateLocation(sql, offset);
  return new InvalidIdentifierError(identifier, reason, location, sql);
}

/**
 * Create a generic syntax error with location
 */
export function createSyntaxError(
  message: string,
  sql: string,
  offset: number,
  token?: string
): SQLSyntaxError {
  const location = calculateLocation(sql, offset);
  return new SQLSyntaxError(
    SyntaxErrorCode.GENERAL,
    message,
    location,
    sql,
    { token }
  );
}
