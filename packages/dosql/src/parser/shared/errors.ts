/**
 * Shared Parser Error Classes
 *
 * Unified error handling for all DoSQL parsers.
 *
 * @packageDocumentation
 */

import type { SourceLocation } from './types.js';

/**
 * SQL Syntax Error with location information
 */
export class SQLSyntaxError extends Error {
  /** Source location where error occurred */
  location?: SourceLocation;
  /** Original SQL input */
  sql?: string;
  /** Suggested fix (if available) */
  suggestion?: string;

  constructor(message: string, location?: SourceLocation, sql?: string) {
    const locStr = location
      ? ` at line ${location.line}, column ${location.column}`
      : '';
    super(message + locStr);
    this.name = 'SQLSyntaxError';
    this.location = location;
    this.sql = sql;
  }

  /**
   * Format error with context
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

    return parts.join('\n');
  }
}

/**
 * Unexpected token error
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
    sql?: string
  ) {
    const message = expected
      ? `Unexpected token '${token}', expected ${expected}`
      : `Unexpected token '${token}'`;
    super(message, location, sql);
    this.name = 'UnexpectedTokenError';
    this.token = token;
    this.expected = expected;
  }
}

/**
 * Unexpected end of input error
 */
export class UnexpectedEOFError extends SQLSyntaxError {
  /** What was expected */
  expected?: string;

  constructor(expected?: string, location?: SourceLocation, sql?: string) {
    const message = expected
      ? `Unexpected end of input, expected ${expected}`
      : 'Unexpected end of input';
    super(message, location, sql);
    this.name = 'UnexpectedEOFError';
    this.expected = expected;
  }
}

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

    return new SQLSyntaxError(error.message, location, sql);
  }

  return new SQLSyntaxError(String(error), undefined, sql);
}
