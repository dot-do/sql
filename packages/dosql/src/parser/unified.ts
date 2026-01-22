/**
 * DoSQL Unified SQL Parser
 *
 * Consolidates all parser modules into a unified SQL parsing pipeline:
 * - DDL (CREATE, ALTER, DROP)
 * - DML (INSERT, UPDATE, DELETE, REPLACE)
 * - SELECT with CTEs, subqueries, set operations
 * - Window functions
 * - CASE expressions
 * - Virtual tables
 *
 * Features:
 * - Single entry point for all SQL parsing
 * - Automatic statement type detection
 * - Extension points for custom syntax
 * - Consistent error handling
 *
 * Issue: pocs-206c - Consolidate parser modules into unified SQL parsing pipeline
 *
 * @packageDocumentation
 */

import {
  parseDDL,
  parseCreateTable,
  parseCreateIndex,
  parseAlterTable,
  parseDropTable,
  parseDropIndex,
  parseCreateView,
  parseDropView,
  isParseSuccess as isDDLParseSuccess,
  type DDLStatement,
  type ParseResult as DDLParseResult,
} from './ddl.js';

import {
  parseDML,
  parseInsert,
  parseUpdate,
  parseDelete,
  parseReplace,
  type DMLStatement,
  type ParseResult as DMLParseResultType,
} from './dml.js';

import { isParseSuccess as isDMLParseSuccess } from './dml-types.js';

import { SubqueryParser, type ParsedSelect } from './subquery.js';

import { parseSetOperation, parseCompoundSelect } from './set-ops.js';

// =============================================================================
// TYPES
// =============================================================================

/**
 * Statement types supported by the unified parser
 */
export type StatementType =
  | 'SELECT'
  | 'INSERT'
  | 'UPDATE'
  | 'DELETE'
  | 'REPLACE'
  | 'CREATE_TABLE'
  | 'CREATE_INDEX'
  | 'CREATE_VIEW'
  | 'ALTER_TABLE'
  | 'DROP_TABLE'
  | 'DROP_INDEX'
  | 'DROP_VIEW'
  | 'EXPLAIN'
  | 'UNKNOWN';

/**
 * Source location for error reporting
 */
export interface SourceLocation {
  line: number;
  column: number;
  offset: number;
}

/**
 * Parse error with location information
 */
export interface ParseError {
  message: string;
  location?: SourceLocation;
  sql?: string;
  suggestion?: string;
}

/**
 * Comment from SQL source
 */
export interface SQLComment {
  type: 'line' | 'block';
  text: string;
  location: SourceLocation;
}

/**
 * Unified AST node (union of all statement types)
 */
export type UnifiedAST =
  | (DDLStatement & { location?: SourceLocation })
  | (DMLStatement & { location?: SourceLocation })
  | (ParsedSelect & { location?: SourceLocation })
  | { type: string; [key: string]: unknown; location?: SourceLocation };

/**
 * Successful parse result
 */
export interface ParseSuccess {
  success: true;
  statementType: StatementType;
  ast: UnifiedAST;
  comments?: SQLComment[];
  metadata?: Record<string, unknown>;
}

/**
 * Failed parse result
 */
export interface ParseFailure {
  success: false;
  error: ParseError;
}

/**
 * Unified parse result
 */
export type ParseResult = ParseSuccess | ParseFailure;

/**
 * Parse options
 */
export interface UnifiedParseOptions {
  /** Enable strict mode (fail on warnings) */
  strict?: boolean;
  /** Include source locations in AST */
  includeLocations?: boolean;
  /** Preserve comments in output */
  preserveComments?: boolean;
  /** Enable specific SQL dialect features */
  dialect?: 'sqlite' | 'standard';
}

/**
 * Parser extension interface
 */
export interface ParserExtension {
  /** Unique name for this extension */
  name: string;
  /** Priority (higher = checked first) */
  priority: number;
  /** Check if this extension can handle the SQL */
  canHandle: (sql: string) => boolean;
  /** Parse the SQL (receives default parser as callback) */
  parse: (
    sql: string,
    defaultParse: (sql: string) => ParseResult
  ) => ParseResult | (ParseResult & { metadata?: Record<string, unknown> });
}

// =============================================================================
// EXTENSION REGISTRY
// =============================================================================

const extensions: Map<string, ParserExtension> = new Map();

/**
 * Register a parser extension
 */
export function registerParserExtension(extension: ParserExtension): void {
  extensions.set(extension.name, extension);
}

/**
 * Unregister a parser extension
 */
export function unregisterParserExtension(name: string): void {
  extensions.delete(name);
}

/**
 * Get sorted extensions (by priority, descending)
 */
function getSortedExtensions(): ParserExtension[] {
  return Array.from(extensions.values()).sort((a, b) => b.priority - a.priority);
}

// =============================================================================
// STATEMENT TYPE DETECTION
// =============================================================================

/**
 * Detect the type of SQL statement
 */
export function detectStatementType(sql: string): StatementType {
  const trimmed = sql.trim();
  if (!trimmed) return 'UNKNOWN';

  const upper = trimmed.toUpperCase();

  // WITH clause means SELECT (CTE)
  if (upper.startsWith('WITH ') || upper.startsWith('WITH\n') || upper.startsWith('WITH\t')) {
    return 'SELECT';
  }

  // SELECT
  if (upper.startsWith('SELECT ') || upper.startsWith('SELECT\n') || upper.startsWith('SELECT\t') || upper === 'SELECT') {
    return 'SELECT';
  }

  // INSERT
  if (upper.startsWith('INSERT ') || upper.startsWith('INSERT\n')) {
    return 'INSERT';
  }

  // UPDATE
  if (upper.startsWith('UPDATE ') || upper.startsWith('UPDATE\n')) {
    return 'UPDATE';
  }

  // DELETE
  if (upper.startsWith('DELETE ') || upper.startsWith('DELETE\n')) {
    return 'DELETE';
  }

  // REPLACE
  if (upper.startsWith('REPLACE ') || upper.startsWith('REPLACE\n')) {
    return 'REPLACE';
  }

  // CREATE statements
  if (upper.startsWith('CREATE ')) {
    if (upper.includes(' TABLE ') || upper.match(/^CREATE\s+(TEMP|TEMPORARY\s+)?TABLE\s/)) {
      return 'CREATE_TABLE';
    }
    if (upper.includes(' INDEX ') || upper.match(/^CREATE\s+(UNIQUE\s+)?INDEX\s/)) {
      return 'CREATE_INDEX';
    }
    if (upper.includes(' VIEW ')) {
      return 'CREATE_VIEW';
    }
  }

  // ALTER statements
  if (upper.startsWith('ALTER ')) {
    if (upper.includes(' TABLE ')) {
      return 'ALTER_TABLE';
    }
  }

  // DROP statements
  if (upper.startsWith('DROP ')) {
    if (upper.includes(' TABLE ')) {
      return 'DROP_TABLE';
    }
    if (upper.includes(' INDEX ')) {
      return 'DROP_INDEX';
    }
    if (upper.includes(' VIEW ')) {
      return 'DROP_VIEW';
    }
  }

  return 'UNKNOWN';
}

// =============================================================================
// COMMENT EXTRACTION
// =============================================================================

/**
 * Extract comments from SQL
 */
function extractComments(sql: string): SQLComment[] {
  const comments: SQLComment[] = [];
  let line = 1;
  let column = 1;
  let i = 0;

  while (i < sql.length) {
    // Line comment
    if (sql[i] === '-' && sql[i + 1] === '-') {
      const start = { line, column, offset: i };
      let text = '';
      i += 2;
      column += 2;

      while (i < sql.length && sql[i] !== '\n') {
        text += sql[i];
        i++;
        column++;
      }

      comments.push({ type: 'line', text: text.trim(), location: start });
    }
    // Block comment
    else if (sql[i] === '/' && sql[i + 1] === '*') {
      const start = { line, column, offset: i };
      let text = '';
      i += 2;
      column += 2;

      while (i < sql.length - 1 && !(sql[i] === '*' && sql[i + 1] === '/')) {
        if (sql[i] === '\n') {
          line++;
          column = 1;
        } else {
          column++;
        }
        text += sql[i];
        i++;
      }

      if (i < sql.length - 1) {
        i += 2; // Skip */
        column += 2;
      }

      comments.push({ type: 'block', text: text.trim(), location: start });
    }
    // Track position
    else if (sql[i] === '\n') {
      line++;
      column = 1;
      i++;
    } else {
      column++;
      i++;
    }
  }

  return comments;
}

// =============================================================================
// CORE PARSERS
// =============================================================================

/**
 * Parse a SELECT statement
 */
function parseSelect(sql: string, options: UnifiedParseOptions = {}): ParseResult {
  try {
    const parser = new SubqueryParser();
    const ast = parser.parse(sql);

    // Add location if requested
    const result: UnifiedAST = options.includeLocations
      ? { ...ast, location: { line: 1, column: 1, offset: 0 } }
      : ast;

    return {
      success: true,
      statementType: 'SELECT',
      ast: result,
    };
  } catch (error) {
    return {
      success: false,
      error: createParseError(error, sql),
    };
  }
}

/**
 * Parse a DDL statement (CREATE, ALTER, DROP)
 */
function parseDDLStatement(sql: string, statementType: StatementType, options: UnifiedParseOptions = {}): ParseResult {
  try {
    let result: DDLParseResult;

    switch (statementType) {
      case 'CREATE_TABLE':
        result = parseCreateTable(sql);
        break;
      case 'CREATE_INDEX':
        result = parseCreateIndex(sql);
        break;
      case 'CREATE_VIEW':
        result = parseCreateView(sql);
        break;
      case 'ALTER_TABLE':
        result = parseAlterTable(sql);
        break;
      case 'DROP_TABLE':
        result = parseDropTable(sql);
        break;
      case 'DROP_INDEX':
        result = parseDropIndex(sql);
        break;
      case 'DROP_VIEW':
        result = parseDropView(sql);
        break;
      default:
        result = parseDDL(sql);
    }

    if (isDDLParseSuccess(result)) {
      const ast: UnifiedAST = options.includeLocations
        ? { ...result.statement, location: { line: 1, column: 1, offset: 0 } }
        : result.statement;

      return {
        success: true,
        statementType,
        ast,
      };
    } else {
      return {
        success: false,
        error: {
          message: result.error,
          location: result.position,
          sql,
        },
      };
    }
  } catch (error) {
    return {
      success: false,
      error: createParseError(error, sql),
    };
  }
}

/**
 * Parse a DML statement (INSERT, UPDATE, DELETE, REPLACE)
 */
function parseDMLStatement(sql: string, statementType: StatementType, options: UnifiedParseOptions = {}): ParseResult {
  try {
    let result: DMLParseResultType;

    switch (statementType) {
      case 'INSERT':
        result = parseInsert(sql);
        break;
      case 'UPDATE':
        result = parseUpdate(sql);
        break;
      case 'DELETE':
        result = parseDelete(sql);
        break;
      case 'REPLACE':
        result = parseReplace(sql);
        break;
      default:
        result = parseDML(sql);
    }

    if (isDMLParseSuccess(result)) {
      const ast: UnifiedAST = options.includeLocations
        ? { ...result.statement, location: { line: 1, column: 1, offset: 0 } }
        : result.statement;

      return {
        success: true,
        statementType,
        ast,
      };
    } else {
      return {
        success: false,
        error: {
          message: result.error,
          location: result.position,
          sql,
        },
      };
    }
  } catch (error) {
    return {
      success: false,
      error: createParseError(error, sql),
    };
  }
}

/**
 * Create a parse error from an exception
 */
function createParseError(error: unknown, sql: string): ParseError {
  if (error instanceof Error) {
    // Try to extract location from error message
    const locMatch = error.message.match(/at line (\d+), column (\d+)/);
    const location = locMatch
      ? { line: parseInt(locMatch[1], 10), column: parseInt(locMatch[2], 10), offset: 0 }
      : undefined;

    return {
      message: error.message,
      location,
      sql,
    };
  }

  return {
    message: String(error),
    sql,
  };
}

// =============================================================================
// UNIFIED PARSER
// =============================================================================

/**
 * Default parsing function (without extensions)
 */
function defaultParse(sql: string, options: UnifiedParseOptions = {}): ParseResult {
  const trimmed = sql.trim();

  // Handle empty input
  if (!trimmed) {
    return {
      success: false,
      error: {
        message: 'SQL statement is empty',
        sql,
      },
    };
  }

  const statementType = detectStatementType(trimmed);

  // Route to appropriate parser
  switch (statementType) {
    case 'SELECT':
      return parseSelect(trimmed, options);

    case 'INSERT':
    case 'UPDATE':
    case 'DELETE':
    case 'REPLACE':
      return parseDMLStatement(trimmed, statementType, options);

    case 'CREATE_TABLE':
    case 'CREATE_INDEX':
    case 'CREATE_VIEW':
    case 'ALTER_TABLE':
    case 'DROP_TABLE':
    case 'DROP_INDEX':
    case 'DROP_VIEW':
      return parseDDLStatement(trimmed, statementType, options);

    case 'UNKNOWN':
    default:
      return {
        success: false,
        error: {
          message: `Unknown or unsupported SQL statement type. Statement starts with: "${trimmed.slice(0, 20)}..."`,
          sql,
        },
      };
  }
}

/**
 * Parse SQL using the unified parser
 *
 * This is the main entry point for SQL parsing. It:
 * 1. Checks registered extensions
 * 2. Detects statement type
 * 3. Routes to appropriate parser
 * 4. Returns unified result format
 *
 * @param sql - SQL statement to parse
 * @param options - Parse options
 * @returns Parse result with AST or error
 */
export function parseSQL(sql: string, options: UnifiedParseOptions = {}): ParseResult {
  // Check extensions first (in priority order)
  const sortedExtensions = getSortedExtensions();

  for (const extension of sortedExtensions) {
    if (extension.canHandle(sql)) {
      const result = extension.parse(sql, (s) => defaultParse(s, options));

      // Handle extensions that add metadata
      if (result.success && 'metadata' in result) {
        const { metadata, ...rest } = result as ParseSuccess & { metadata: Record<string, unknown> };
        return { ...rest, metadata } as ParseResult;
      }

      return result;
    }
  }

  // Use default parser
  const result = defaultParse(sql, options);

  // Add comments if requested
  if (options.preserveComments && result.success) {
    const comments = extractComments(sql);
    return { ...result, comments };
  }

  return result;
}

// =============================================================================
// TYPE GUARDS
// =============================================================================

/**
 * Check if AST is a SELECT statement
 */
export function isSelectStatement(ast: UnifiedAST): ast is ParsedSelect & { location?: SourceLocation } {
  return ast && 'type' in ast && ast.type === 'select';
}

/**
 * Check if AST is a DDL statement
 */
export function isDDLStatement(ast: UnifiedAST): ast is DDLStatement & { location?: SourceLocation } {
  if (!ast || !('type' in ast)) return false;
  const ddlTypes = [
    'CREATE TABLE',
    'CREATE INDEX',
    'CREATE VIEW',
    'ALTER TABLE',
    'DROP TABLE',
    'DROP INDEX',
    'DROP VIEW',
  ];
  return ddlTypes.includes(ast.type as string);
}

/**
 * Check if AST is a DML statement
 */
export function isDMLStatement(ast: UnifiedAST): ast is DMLStatement & { location?: SourceLocation } {
  if (!ast || !('type' in ast)) return false;
  const dmlTypes = ['insert', 'update', 'delete', 'replace'];
  return dmlTypes.includes(ast.type as string);
}

// =============================================================================
// ERROR FORMATTING
// =============================================================================

/**
 * Format a parse error for display
 */
export function formatParseError(error: ParseError): string {
  const parts: string[] = [];

  parts.push(`Parse Error: ${error.message}`);

  if (error.location) {
    parts.push(`  at line ${error.location.line}, column ${error.location.column}`);
  }

  if (error.suggestion) {
    parts.push(`  Suggestion: ${error.suggestion}`);
  }

  if (error.sql) {
    const preview = error.sql.length > 50 ? error.sql.slice(0, 50) + '...' : error.sql;
    parts.push(`  SQL: ${preview}`);
  }

  return parts.join('\n');
}

// =============================================================================
// RE-EXPORTS FOR CONVENIENCE
// =============================================================================

export { SubqueryParser } from './subquery.js';
export { parseCompoundSelect, parseSetOperation } from './set-ops.js';
export { parseDDL, parseCreateTable, parseCreateIndex } from './ddl.js';
export { parseDML, parseInsert, parseUpdate, parseDelete } from './dml.js';
