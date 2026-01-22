/**
 * Collation Types for DoSQL
 *
 * Defines the types used for SQLite-compatible collation support including:
 * - Collation function signature
 * - Collation definition interface
 * - Built-in collation names
 * - Collation options for comparisons
 */

/**
 * A collation function takes two strings and returns:
 * - negative number if a < b
 * - positive number if a > b
 * - 0 if a == b
 */
export type CollationFunction = (a: string, b: string) => number;

/**
 * SQLite built-in collation names
 */
export type BuiltinCollation = 'BINARY' | 'NOCASE' | 'RTRIM';

/**
 * Collation definition with metadata
 */
export interface CollationDefinition {
  /** The name of the collation (case-insensitive for lookup) */
  name: string;
  /** The comparison function */
  compare: CollationFunction;
  /** Whether this is a built-in collation */
  builtin: boolean;
  /** Optional description */
  description?: string;
  /** Whether the collation is case-sensitive */
  caseSensitive: boolean;
  /** Whether the collation respects trailing spaces */
  respectsTrailingSpaces: boolean;
}

/**
 * Options for creating a custom collation
 */
export interface CreateCollationOptions {
  /** The name of the collation */
  name: string;
  /** The comparison function */
  compare: CollationFunction;
  /** Optional description */
  description?: string;
  /** Whether to replace existing collation if it exists */
  replace?: boolean;
}

/**
 * Result of parsing COLLATE clause
 */
export interface CollateClause {
  /** The collation name */
  collationName: string;
  /** Whether this was parsed from a column definition */
  fromColumnDef: boolean;
  /** Whether this was parsed from an expression */
  fromExpression: boolean;
}

/**
 * Column definition with optional collation
 */
export interface ColumnCollation {
  /** Column name */
  column: string;
  /** Table name (optional, for qualified references) */
  table?: string;
  /** Collation name (null means default/BINARY) */
  collation: string | null;
}

/**
 * Index definition with collation support
 */
export interface IndexCollation {
  /** Index name */
  indexName: string;
  /** Table name */
  tableName: string;
  /** Columns with their collations */
  columns: Array<{
    name: string;
    collation: string | null;
    sortOrder: 'ASC' | 'DESC';
  }>;
}

/**
 * PRAGMA case_sensitive_like value
 */
export type CaseSensitiveLike = boolean;

/**
 * Unicode collation options (ICU-style)
 */
export interface UnicodeCollationOptions {
  /** Locale identifier (e.g., 'en_US', 'de_DE') */
  locale: string;
  /** Strength of comparison */
  strength?: 'primary' | 'secondary' | 'tertiary' | 'identical';
  /** Whether to ignore case */
  caseFirst?: 'upper' | 'lower' | 'off';
  /** Numeric sorting (e.g., "2" < "10") */
  numeric?: boolean;
  /** Alternate handling (shifted or non-ignorable) */
  alternate?: 'shifted' | 'non-ignorable';
}

/**
 * Result of CREATE COLLATION parsing
 */
export interface CreateCollationStatement {
  type: 'CREATE_COLLATION';
  name: string;
  /** For built-in or known collations */
  baseCollation?: string;
  /** Custom options */
  options?: UnicodeCollationOptions;
}

/**
 * Result of DROP COLLATION parsing
 */
export interface DropCollationStatement {
  type: 'DROP_COLLATION';
  name: string;
  ifExists: boolean;
}

/**
 * Collation comparison result with metadata
 */
export interface CollationCompareResult {
  /** The comparison result (-1, 0, 1) */
  result: number;
  /** The collation used */
  collationName: string;
  /** Whether the comparison was case-sensitive */
  caseSensitive: boolean;
}

/**
 * Error thrown when a collation is not found
 */
export class CollationNotFoundError extends Error {
  constructor(public readonly collationName: string) {
    super(`Collation '${collationName}' not found`);
    this.name = 'CollationNotFoundError';
  }
}

/**
 * Error thrown when trying to create a duplicate collation
 */
export class CollationExistsError extends Error {
  constructor(public readonly collationName: string) {
    super(`Collation '${collationName}' already exists`);
    this.name = 'CollationExistsError';
  }
}

/**
 * Error thrown when trying to modify a built-in collation
 */
export class BuiltinCollationError extends Error {
  constructor(public readonly collationName: string) {
    super(`Cannot modify built-in collation '${collationName}'`);
    this.name = 'BuiltinCollationError';
  }
}
