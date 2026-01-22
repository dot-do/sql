/**
 * Syntax Error Classes
 *
 * Standardized error classes for SQL syntax errors with location information.
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

  constructor(
    code: SyntaxErrorCode,
    message: string,
    location?: SourceLocation,
    sql?: string,
    options?: { cause?: Error; context?: ErrorContext }
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
   */
  format(): string {
    const parts: string[] = [this.message];

    if (this.sql && this.location) {
      const lines = this.sql.split('\n');
      const line = lines[this.location.line - 1];
      if (line) {
        parts.push(`  ${line}`);
        parts.push(`  ${' '.repeat(this.location.column - 1)}^`);
      }
    }

    if (this.suggestion) {
      parts.push(`  Suggestion: ${this.suggestion}`);
    }

    if (this.recoveryHint) {
      parts.push(`  Hint: ${this.recoveryHint}`);
    }

    return parts.join('\n');
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
 */
export class UnexpectedTokenError extends SQLSyntaxError {
  /** The unexpected token value */
  token: string;

  /** Expected token(s) */
  expected?: string;

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

    super(SyntaxErrorCode.UNEXPECTED_TOKEN, message, location, sql, options);
    this.name = 'UnexpectedTokenError';
    this.token = token;
    this.expected = expected;
  }
}

registerErrorClass('UnexpectedTokenError', UnexpectedTokenError);

// =============================================================================
// Unexpected EOF Error
// =============================================================================

/**
 * Error for unexpected end of input
 */
export class UnexpectedEOFError extends SQLSyntaxError {
  /** What was expected */
  expected?: string;

  constructor(
    expected?: string,
    location?: SourceLocation,
    sql?: string,
    options?: { cause?: Error; context?: ErrorContext }
  ) {
    const message = expected
      ? `Unexpected end of input, expected ${expected}`
      : 'Unexpected end of input';

    super(SyntaxErrorCode.UNEXPECTED_EOF, message, location, sql, options);
    this.name = 'UnexpectedEOFError';
    this.expected = expected;
  }
}

registerErrorClass('UnexpectedEOFError', UnexpectedEOFError);

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

    return new SQLSyntaxError(
      SyntaxErrorCode.GENERAL,
      error.message,
      location,
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
