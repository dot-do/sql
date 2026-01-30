/**
 * DoSQL Error Module
 *
 * Unified error handling across all DoSQL packages.
 *
 * @packageDocumentation
 */

// Base classes and types
export {
  DoSQLError,
  AggregateDoSQLError,
  GenericDoSQLError,
  ErrorCategory,
  registerErrorClass,
  deserializeError,
  type ErrorContext,
  type SerializedError,
  type ErrorLogEntry,
} from './base.js';

// Error codes
export {
  DatabaseErrorCode,
  StatementErrorCode,
  BindingErrorCode,
  SyntaxErrorCode,
  PlannerErrorCode,
  ExecutorErrorCode,
  ParserErrorCode,
  StorageErrorCode,
  getErrorCodeCategory,
  isErrorCodeInCategory,
  type DoSQLErrorCode,
} from './codes.js';

// Database errors
export {
  DatabaseError,
  ConnectionError,
  ReadOnlyError,
  createClosedDatabaseError,
  createSavepointNotFoundError,
} from './database-errors.js';

// Statement errors
export {
  StatementError,
  PrepareError,
  ExecuteError,
  createFinalizedStatementError,
  createTableNotFoundError,
  createUnsupportedSqlError,
} from './statement-errors.js';

// Binding errors
export {
  BindingError,
  MissingParameterError,
  TypeCoercionError,
  createMissingNamedParamError,
  createMissingPositionalParamError,
  createInvalidTypeError,
  createCountMismatchError,
  createNamedExpectedError,
  createNonFiniteNumberError,
} from './binding-errors.js';

// Typed errors (Planner, Executor, Parser, Storage)
export {
  PlannerError,
  ExecutorError,
  ParserError,
  StorageError,
  createNoTablesToJoinError,
  createJoinOrderFailedError,
  createUnknownPlanTypeError,
  createQueryTimeoutError,
  createEmptySqlError,
  createUnsupportedOperationError,
  createInvalidSnapshotIdError,
  createStorageReadError,
  createStorageWriteError,
} from './typed-errors.js';

// Syntax errors
export {
  SQLSyntaxError,
  UnexpectedTokenError,
  UnexpectedEOFError,
  MissingKeywordError,
  InvalidIdentifierError,
  createErrorFromException,
  createUnexpectedTokenError,
  createUnexpectedEOFError,
  createMissingKeywordError,
  createInvalidIdentifierError,
  createSyntaxError,
  // Utility functions
  calculateLocation,
  formatErrorSnippet,
  getSuggestionForTypo,
  levenshteinDistance,
  COMMON_TYPOS,
  type SourceLocation,
} from './syntax-errors.js';
