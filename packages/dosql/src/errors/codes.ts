/**
 * DoSQL Error Code Enumerations
 *
 * Standardized error codes following the pattern: CATEGORY_SPECIFIC
 *
 * @packageDocumentation
 */

// =============================================================================
// Database Error Codes
// =============================================================================

/**
 * Error codes for database-level operations
 */
export enum DatabaseErrorCode {
  /** Database connection is closed */
  CLOSED = 'DB_CLOSED',
  /** Database is read-only */
  READ_ONLY = 'DB_READ_ONLY',
  /** Database not found */
  NOT_FOUND = 'DB_NOT_FOUND',
  /** Constraint violation */
  CONSTRAINT_VIOLATION = 'DB_CONSTRAINT',
  /** Query execution error */
  QUERY_ERROR = 'DB_QUERY_ERROR',
  /** Connection failed */
  CONNECTION_FAILED = 'DB_CONNECTION_FAILED',
  /** Configuration error */
  CONFIG_ERROR = 'DB_CONFIG_ERROR',
  /** Operation timeout */
  TIMEOUT = 'DB_TIMEOUT',
  /** Internal database error */
  INTERNAL = 'DB_INTERNAL',
}

// =============================================================================
// Statement Error Codes
// =============================================================================

/**
 * Error codes for statement operations
 */
export enum StatementErrorCode {
  /** Statement has been finalized */
  FINALIZED = 'STMT_FINALIZED',
  /** Syntax error in SQL */
  SYNTAX_ERROR = 'STMT_SYNTAX',
  /** Statement execution failed */
  EXECUTION_ERROR = 'STMT_EXECUTION',
  /** Table not found */
  TABLE_NOT_FOUND = 'STMT_TABLE_NOT_FOUND',
  /** Column not found */
  COLUMN_NOT_FOUND = 'STMT_COLUMN_NOT_FOUND',
  /** Invalid SQL syntax */
  INVALID_SQL = 'STMT_INVALID_SQL',
  /** Unsupported operation */
  UNSUPPORTED = 'STMT_UNSUPPORTED',
}

// =============================================================================
// Binding Error Codes
// =============================================================================

/**
 * Error codes for parameter binding operations
 */
export enum BindingErrorCode {
  /** Missing required parameter */
  MISSING_PARAM = 'BIND_MISSING_PARAM',
  /** Type mismatch in parameter */
  TYPE_MISMATCH = 'BIND_TYPE_MISMATCH',
  /** Invalid type for binding */
  INVALID_TYPE = 'BIND_INVALID_TYPE',
  /** Parameter count mismatch */
  COUNT_MISMATCH = 'BIND_COUNT_MISMATCH',
  /** Named parameter expected but positional provided */
  NAMED_EXPECTED = 'BIND_NAMED_EXPECTED',
}

// =============================================================================
// Syntax Error Codes
// =============================================================================

/**
 * Error codes for SQL syntax errors
 */
export enum SyntaxErrorCode {
  /** Unexpected token */
  UNEXPECTED_TOKEN = 'SYNTAX_UNEXPECTED_TOKEN',
  /** Unexpected end of input */
  UNEXPECTED_EOF = 'SYNTAX_UNEXPECTED_EOF',
  /** Invalid identifier */
  INVALID_IDENTIFIER = 'SYNTAX_INVALID_IDENTIFIER',
  /** Invalid literal value */
  INVALID_LITERAL = 'SYNTAX_INVALID_LITERAL',
  /** General syntax error */
  GENERAL = 'SYNTAX_ERROR',
}

// =============================================================================
// All Error Codes Union Type
// =============================================================================

/**
 * Union of all error code types
 */
export type DoSQLErrorCode =
  | DatabaseErrorCode
  | StatementErrorCode
  | BindingErrorCode
  | SyntaxErrorCode;

/**
 * Get error code category prefix
 */
export function getErrorCodeCategory(code: string): string {
  const parts = code.split('_');
  return parts[0] ?? 'UNKNOWN';
}

/**
 * Check if a code belongs to a specific category
 */
export function isErrorCodeInCategory(code: string, category: string): boolean {
  return code.startsWith(category + '_');
}
