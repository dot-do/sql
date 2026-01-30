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
// Planner Error Codes
// =============================================================================

/**
 * Error codes for query planner operations
 */
export enum PlannerErrorCode {
  /** No tables to join */
  NO_TABLES = 'PLANNER_NO_TABLES',
  /** Failed to find optimal join order */
  JOIN_ORDER_FAILED = 'PLANNER_JOIN_ORDER_FAILED',
  /** Invalid query plan */
  INVALID_PLAN = 'PLANNER_INVALID_PLAN',
  /** Cost estimation failed */
  COST_ESTIMATION_FAILED = 'PLANNER_COST_ESTIMATION_FAILED',
  /** Index selection failed */
  INDEX_SELECTION_FAILED = 'PLANNER_INDEX_SELECTION_FAILED',
  /** Optimization failed */
  OPTIMIZATION_FAILED = 'PLANNER_OPTIMIZATION_FAILED',
}

// =============================================================================
// Executor Error Codes
// =============================================================================

/**
 * Error codes for query executor operations
 */
export enum ExecutorErrorCode {
  /** Unknown plan type */
  UNKNOWN_PLAN_TYPE = 'EXECUTOR_UNKNOWN_PLAN_TYPE',
  /** Query timeout */
  QUERY_TIMEOUT = 'EXECUTOR_QUERY_TIMEOUT',
  /** Operator error */
  OPERATOR_ERROR = 'EXECUTOR_OPERATOR_ERROR',
  /** Subquery error */
  SUBQUERY_ERROR = 'EXECUTOR_SUBQUERY_ERROR',
  /** CTE not materialized */
  CTE_NOT_MATERIALIZED = 'EXECUTOR_CTE_NOT_MATERIALIZED',
  /** Aggregate error */
  AGGREGATE_ERROR = 'EXECUTOR_AGGREGATE_ERROR',
  /** Window function error */
  WINDOW_FUNCTION_ERROR = 'EXECUTOR_WINDOW_FUNCTION_ERROR',
  /** Transaction error */
  TRANSACTION_ERROR = 'EXECUTOR_TRANSACTION_ERROR',
}

// =============================================================================
// Parser Error Codes
// =============================================================================

/**
 * Error codes for SQL parser operations
 */
export enum ParserErrorCode {
  /** Empty SQL query */
  EMPTY_SQL = 'PARSER_EMPTY_SQL',
  /** Unsupported SQL operation */
  UNSUPPORTED_OPERATION = 'PARSER_UNSUPPORTED_OPERATION',
  /** Invalid statement structure */
  INVALID_STATEMENT = 'PARSER_INVALID_STATEMENT',
  /** Unknown SQL operation */
  UNKNOWN_OPERATION = 'PARSER_UNKNOWN_OPERATION',
  /** Invalid data type */
  INVALID_DATA_TYPE = 'PARSER_INVALID_DATA_TYPE',
  /** Invalid reference action */
  INVALID_REFERENCE_ACTION = 'PARSER_INVALID_REFERENCE_ACTION',
  /** Unknown window name */
  UNKNOWN_WINDOW_NAME = 'PARSER_UNKNOWN_WINDOW_NAME',
  /** Invalid frame specification */
  INVALID_FRAME_SPEC = 'PARSER_INVALID_FRAME_SPEC',
  /** Unknown function */
  UNKNOWN_FUNCTION = 'PARSER_UNKNOWN_FUNCTION',
}

// =============================================================================
// Storage Error Codes
// =============================================================================

/**
 * Error codes for storage operations
 */
export enum StorageErrorCode {
  /** Invalid snapshot ID */
  INVALID_SNAPSHOT_ID = 'STORAGE_INVALID_SNAPSHOT_ID',
  /** Read operation failed */
  READ_FAILED = 'STORAGE_READ_FAILED',
  /** Write operation failed */
  WRITE_FAILED = 'STORAGE_WRITE_FAILED',
  /** Invalid page ID */
  INVALID_PAGE_ID = 'STORAGE_INVALID_PAGE_ID',
  /** Bucket not found */
  BUCKET_NOT_FOUND = 'STORAGE_BUCKET_NOT_FOUND',
  /** Corruption detected */
  CORRUPTION = 'STORAGE_CORRUPTION',
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
  | SyntaxErrorCode
  | PlannerErrorCode
  | ExecutorErrorCode
  | ParserErrorCode
  | StorageErrorCode;

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
