/**
 * SQL Parser Types - AST node types for parsed SQL queries
 *
 * This module contains type definitions for the SQL parser's
 * Abstract Syntax Tree (AST) nodes.
 *
 * @packageDocumentation
 */

import type { AggregateOp, QueryOperation, SortColumn } from '../types.js';

// =============================================================================
// PARSED QUERY TYPES
// =============================================================================

/**
 * Parsed SQL query structure
 */
export interface ParsedQuery {
  /** Query operation type */
  operation: QueryOperation;
  /** Target table(s) */
  tables: TableReference[];
  /** Selected columns (for SELECT) */
  columns?: ColumnReference[];
  /** WHERE conditions */
  where?: WhereClause;
  /** GROUP BY columns */
  groupBy?: string[];
  /** ORDER BY specification */
  orderBy?: SortColumn[];
  /** LIMIT clause */
  limit?: number;
  /** OFFSET clause */
  offset?: number;
  /** Aggregate functions used */
  aggregates?: AggregateOp[];
  /** Whether query has DISTINCT */
  distinct?: boolean;
}

// =============================================================================
// TABLE REFERENCE TYPES
// =============================================================================

/**
 * Table reference in query
 */
export interface TableReference {
  name: string;
  alias?: string;
}

// =============================================================================
// COLUMN REFERENCE TYPES
// =============================================================================

/**
 * Column reference in query
 */
export interface ColumnReference {
  name: string;
  alias?: string;
  table?: string;
  aggregate?: string;
}

// =============================================================================
// WHERE CLAUSE TYPES
// =============================================================================

/**
 * WHERE clause structure
 */
export interface WhereClause {
  conditions: WhereCondition[];
  operator: 'AND' | 'OR';
}

/**
 * WHERE condition operator types
 */
export type WhereOperator =
  | '='
  | '!='
  | '<'
  | '>'
  | '<='
  | '>='
  | 'IN'
  | 'BETWEEN'
  | 'LIKE'
  | 'IS NULL'
  | 'IS NOT NULL';

/**
 * Single WHERE condition
 */
export interface WhereCondition {
  column: string;
  operator: WhereOperator;
  value?: unknown;
  values?: unknown[]; // For IN operator
  minValue?: unknown; // For BETWEEN
  maxValue?: unknown; // For BETWEEN
}
