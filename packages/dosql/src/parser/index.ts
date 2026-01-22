/**
 * DoSQL Parser Module
 *
 * Exports unified SQL parsing functionality including:
 * - Unified parser (single entry point for all SQL)
 * - DDL (Data Definition Language)
 * - DML (Data Manipulation Language)
 * - SELECT with CTEs, subqueries, set operations
 * - Window functions
 * - CASE expressions
 * - Virtual tables
 *
 * Issue: pocs-206c - Consolidate parser modules into unified SQL parsing pipeline
 */

// =============================================================================
// UNIFIED SQL PARSER (Recommended entry point)
// =============================================================================

// Main unified parser entry point
export {
  parseSQL,
  detectStatementType,
  registerParserExtension,
  unregisterParserExtension,
  isSelectStatement,
  isDDLStatement as isUnifiedDDLStatement,
  isDMLStatement as isUnifiedDMLStatement,
  formatParseError,
  SubqueryParser,
} from './unified.js';

// Unified Parser Types
export type {
  StatementType,
  ParseResult as UnifiedParseResult,
  ParseSuccess as UnifiedParseSuccess,
  ParseFailure as UnifiedParseFailure,
  ParseError as UnifiedParseError,
  UnifiedParseOptions,
  ParserExtension,
  SQLComment,
  UnifiedAST,
  SourceLocation as UnifiedSourceLocation,
} from './unified.js';

// DDL Parser
export {
  // Main parser function
  parseDDL,
  // Specific statement parsers
  parseCreateTable,
  parseCreateIndex,
  parseAlterTable,
  parseDropTable,
  parseDropIndex,
  parseCreateView,
  parseDropView,
} from './ddl.js';

// DDL Types
export type {
  // Data types
  SqliteDataType,
  ColumnDataType,
  // Constraint types
  ConflictClause,
  ReferenceAction,
  DeferrableClause,
  // Column constraints
  ColumnConstraint,
  PrimaryKeyColumnConstraint,
  NotNullColumnConstraint,
  UniqueColumnConstraint,
  CheckColumnConstraint,
  DefaultColumnConstraint,
  CollateColumnConstraint,
  ReferencesColumnConstraint,
  GeneratedColumnConstraint,
  // Column and table definitions
  ColumnDefinition,
  TableConstraint,
  PrimaryKeyTableConstraint,
  UniqueTableConstraint,
  ForeignKeyTableConstraint,
  CheckTableConstraint,
  // DDL statements
  DDLStatement,
  CreateTableStatement,
  CreateIndexStatement,
  AlterTableStatement,
  DropTableStatement,
  DropIndexStatement,
  CreateViewStatement,
  DropViewStatement,
  IndexColumn,
  AlterTableOperation,
  AlterTableAddColumn,
  AlterTableDropColumn,
  AlterTableRenameTo,
  AlterTableRenameColumn,
  // Parse results
  ParseResult,
  ParseSuccess,
  ParseError,
} from './ddl-types.js';

// Type guards
export {
  isCreateTableStatement,
  isCreateIndexStatement,
  isAlterTableStatement,
  isDropTableStatement,
  isDropIndexStatement,
  isCreateViewStatement,
  isDropViewStatement,
  isParseSuccess,
  isParseError,
} from './ddl-types.js';

// =============================================================================
// CTE (Common Table Expression) Parser
// =============================================================================

// CTE Parser functions
export {
  // Main parser function
  parseCTE,
  // Utility functions
  hasWithClause,
  extractWithClause,
  getCTENames,
  findCTE,
  validateCTEReferences,
  expandCTEsInline,
  getCTEDependencyOrder,
  // Factory functions (re-exported from cte-types)
  createCTE,
  createRecursiveCTE,
  createWithClause,
  createCTEScope,
  // Type guards (re-exported from cte-types)
  isRecursiveCTE,
  hasRecursiveCTE,
  isCTEParseSuccess,
  isCTEParseError,
} from './cte.js';

// CTE Types
export type {
  // Core CTE types
  CTEDefinition,
  WithClause,
  SelectWithCTE,
  // Materialization
  CTEMaterializationHint,
  CTEDefinitionWithHints,
  // Recursive CTE types
  RecursiveCTEStructure,
  RecursiveCTELimits,
  // Parse results
  CTEParseResult,
  CTEParseSuccess,
  CTEParseError,
  // CTE resolution
  CTEScope,
  CTEReference,
} from './cte-types.js';

// Additional type guards from cte-types
export {
  isCTEReference,
  getCTEFromScope,
} from './cte-types.js';

// =============================================================================
// WINDOW CLAUSE PARSER
// =============================================================================

// Window Parser functions
export {
  // Main parser functions
  parseWindowSpec,
  parseOverClause,
  parseWindowClause,
  // Resolution and validation
  resolveWindowSpec,
  validateWindowSpec,
} from './window.js';

// Window Parser Types
export type {
  // Frame types
  FrameBoundaryType,
  FrameBoundary,
  FrameMode,
  FrameExclusion,
  FrameSpec,
  // Sort types
  SortDirection,
  NullsPosition,
  OrderByItem,
  // Expression types
  Expression as WindowExpression,
  ColumnReference,
  LiteralExpression,
  FunctionCall as WindowFunctionCall,
  BinaryExpression,
  UnaryExpression,
  // Window specification types
  WindowSpec,
  WindowDefinition,
} from './window.js';

// =============================================================================
// VIRTUAL TABLE PARSER
// =============================================================================

// Virtual Table Parser functions
export {
  // URL literal detection
  isValidUrlScheme,
  isQuotedUrlLiteral,
  extractUrlFromQuotedLiteral,

  // URL function parsing
  isUrlFunction,
  parseUrlFunction,

  // WITH clause parsing (different from CTE WITH)
  extractWithClause as extractVirtualTableWithClause,
  parseWithClauseOptions,

  // FROM clause parsing
  parseVirtualTableFromClause,

  // Full SQL parsing
  parseSelectWithVirtualTable,
} from './virtual.js';

// Virtual Table Parser Types
export type {
  ParsedVirtualTableSource,
  VirtualTableParseResult,

  // Type-level parsing
  IsVirtualTableFrom,
  ExtractVirtualTableUrl,
  InferVirtualTableFormat,
} from './virtual.js';

// =============================================================================
// SET OPERATIONS PARSER
// =============================================================================

// Set Operations Parser functions
export {
  // Main parser functions
  parseSetOperation,
  parseCompoundSelect,
  // Validation
  validateSetOperationCompatibility,
} from './set-ops.js';

// Set Operations Parser Types
export type {
  // Operation types
  SetOperationType,
  // AST node types
  SourcePosition,
  SimpleSelectNode,
  SetOperationNode,
  SelectNode,
  SortDirection as SetOpSortDirection,
  NullsPosition as SetOpNullsPosition,
  OrderByItem as SetOpOrderByItem,
  CompoundOperation,
  CTEDefinition as SetOpCTEDefinition,
  CompoundSelectStatement,
  // Parse results
  SetOperationParseSuccess,
  SetOperationParseError,
  SetOperationParseResult,
  CompoundSelectParseSuccess,
  CompoundSelectParseError,
  CompoundSelectParseResult,
  // Validation results
  SetOpValidationResult,
} from './set-ops.js';

// Type-level helpers
export type {
  IsSetOperation,
  ExtractSetOperationType,
  IsSetOperationAll,
} from './set-ops.js';

// =============================================================================
// DML (Data Manipulation Language) PARSER
// =============================================================================

// DML Parser functions
export {
  parseDML,
  parseInsert,
  parseUpdate,
  parseDelete,
  parseReplace,
  isDMLStatement,
  getDMLType,
} from './dml.js';

// DML Types
export type {
  // Expression types
  Expression as DMLExpression,
  LiteralExpression as DMLLiteralExpression,
  ColumnReference as DMLColumnReference,
  FunctionCall as DMLFunctionCall,
  BinaryExpression as DMLBinaryExpression,
  UnaryExpression as DMLUnaryExpression,
  SubqueryExpression as DMLSubqueryExpression,
  CaseExpression as DMLCaseExpression,
  NullExpression as DMLNullExpression,
  DefaultExpression as DMLDefaultExpression,
  ParameterExpression as DMLParameterExpression,
  // Operator types
  BinaryOperator as DMLBinaryOperator,
  UnaryOperator as DMLUnaryOperator,
  // Clause types
  WhereClause as DMLWhereClause,
  SetClause as DMLSetClause,
  OrderByClause as DMLOrderByClause,
  OrderByItem as DMLOrderByItem,
  LimitClause as DMLLimitClause,
  ReturningClause as DMLReturningClause,
  ReturningColumn as DMLReturningColumn,
  FromClause as DMLFromClause,
  TableReference as DMLTableReference,
  // Conflict handling
  ConflictAction as DMLConflictAction,
  ConflictClause as DMLConflictClause,
  OnConflictClause as DMLOnConflictClause,
  ConflictTarget as DMLConflictTarget,
  OnConflictAction as DMLOnConflictAction,
  OnConflictDoNothing as DMLOnConflictDoNothing,
  OnConflictDoUpdate as DMLOnConflictDoUpdate,
  // Statement types
  DMLStatement,
  InsertStatement,
  UpdateStatement,
  DeleteStatement,
  ReplaceStatement,
  // Insert source types
  InsertSource,
  ValuesList,
  ValuesRow,
  InsertSelect,
  InsertDefault,
  // Parse results
  ParseResult as DMLParseResult,
  ParseSuccess as DMLParseSuccess,
  ParseError as DMLParseError,
} from './dml-types.js';

// DML Type Guards
export {
  isInsertStatement,
  isUpdateStatement,
  isDeleteStatement,
  isReplaceStatement,
  isLiteralExpression,
  isColumnReference,
  isFunctionCall,
  isBinaryExpression,
  isParseSuccess as isDMLParseSuccess,
  isParseError as isDMLParseError,
} from './dml-types.js';

// =============================================================================
// RETURNING CLAUSE PARSER
// =============================================================================

// RETURNING Parser functions and utilities
export {
  // Query functions
  hasReturningClause,
  getReturningClause,
  hasWildcard,
  getReturningColumns,
  getOutputColumnNames,
  expandWildcard,
  // Factory functions
  createWildcardReturning,
  createColumnsReturning,
  createAliasedReturning,
  // Evaluation functions
  evaluateExpression,
  evaluateReturning,
  evaluateReturningMultiple,
  // SQL generation
  generateReturningSql,
  // Validation
  validateReturning,
} from './returning.js';

// RETURNING Types
export type {
  ReturningResult,
  ReturningEvaluationOptions,
  ReturningColumnInfo,
  ReturningValidationResult,
} from './returning.js';

// =============================================================================
// CASE EXPRESSION PARSER
// =============================================================================

// CASE Parser functions
export {
  // Main parser class and function
  CaseExpressionParser,
  parseCaseExpression,
  // Evaluation function
  evaluateCaseExpression,
  // Error class
  SyntaxError as CaseSyntaxError,
} from './case.js';

// CASE Parser Types
export type {
  // Source location
  SourceLocation as CaseSourceLocation,
  // Expression types
  ParsedExpr as CaseParsedExpr,
  ColumnExpr as CaseColumnExpr,
  LiteralExpr as CaseLiteralExpr,
  BinaryExpr as CaseBinaryExpr,
  UnaryExpr as CaseUnaryExpr,
  FunctionExpr as CaseFunctionExpr,
  AggregateExpr as CaseAggregateExpr,
  BetweenExpr as CaseBetweenExpr,
  InExpr as CaseInExpr,
  IsNullExpr as CaseIsNullExpr,
  SubqueryExpr as CaseSubqueryExpr,
  ExistsExpr as CaseExistsExpr,
  StarExpr as CaseStarExpr,
  // CASE expression types
  BaseCaseExpression,
  SimpleCaseExpression,
  SearchedCaseExpression,
  CaseExpression,
  WhenClause,
  SimpleWhenClause,
  SearchedWhenClause,
  // Parsed statement types
  ParsedColumn as CaseParsedColumn,
  ParsedFrom as CaseParsedFrom,
  ParsedJoin as CaseParsedJoin,
  ParsedOrderBy as CaseParsedOrderBy,
  ParsedSelect as CaseParsedSelect,
} from './case.js';

// =============================================================================
// SHARED PARSER UTILITIES (pocs-ftmh refactor)
// =============================================================================

// Shared tokenizer and utilities for building custom parsers
export {
  // Tokenizer
  Tokenizer,
  tokenize,
  createTokenStream,
  isKeyword,
  SQL_KEYWORDS,
  // Errors
  SQLSyntaxError,
  UnexpectedTokenError,
  UnexpectedEOFError,
  createErrorFromException,
} from './shared/index.js';

// Shared Parser State utilities
export {
  createParserState,
  remaining as parserRemaining,
  isEOF as parserIsEOF,
  getLocation as parserGetLocation,
  advance as parserAdvance,
  peek as parserPeek,
  skipWhitespace as parserSkipWhitespace,
  skipWhitespaceAndComments,
  matchKeyword as parserMatchKeyword,
  matchExact as parserMatchExact,
  matchRegex as parserMatchRegex,
  parseIdentifier as parserParseIdentifier,
  parseIdentifierList as parserParseIdentifierList,
  parseStringLiteral as parserParseStringLiteral,
  parseNumericLiteral as parserParseNumericLiteral,
  parseParenthesized as parserParseParenthesized,
  findMatchingParen,
} from './shared/index.js';

// Shared Parser Types
export type {
  // Token types
  TokenType,
  Token,
  // Source location
  SourceLocation as SharedSourceLocation,
  // Base expression types
  ExpressionBase,
  ColumnExpr as SharedColumnExpr,
  LiteralExpr as SharedLiteralExpr,
  BinaryExpr as SharedBinaryExpr,
  UnaryExpr as SharedUnaryExpr,
  FunctionExpr as SharedFunctionExpr,
  AggregateExpr as SharedAggregateExpr,
  BetweenExpr as SharedBetweenExpr,
  InExpr as SharedInExpr,
  IsNullExpr as SharedIsNullExpr,
  SubqueryExpr as SharedSubqueryExpr,
  ExistsExpr as SharedExistsExpr,
  StarExpr as SharedStarExpr,
  NullExpr as SharedNullExpr,
  DefaultExpr as SharedDefaultExpr,
  ParameterExpr as SharedParameterExpr,
  BaseExpr,
  // Operators
  BinaryOperator as SharedBinaryOperator,
  UnaryOperator as SharedUnaryOperator,
  // Parse results
  ParseSuccessBase,
  ParseErrorBase,
  ParseResultBase,
  // Parser state
  ParserState,
} from './shared/index.js';

// Shared constants and utilities
export {
  OPERATOR_PRECEDENCE,
  isColumnExpr,
  isLiteralExpr,
  isBinaryExpr,
  isFunctionExpr,
  isAggregateExpr,
  isParseSuccessBase,
  isParseErrorBase,
} from './shared/index.js';
