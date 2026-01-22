/**
 * DoSQL CTE (Common Table Expression) AST Types
 *
 * Type definitions for WITH clauses and CTEs, including:
 * - Simple CTEs: WITH name AS (SELECT ...)
 * - Multiple CTEs: WITH a AS (...), b AS (...) SELECT ...
 * - Recursive CTEs: WITH RECURSIVE name AS (...)
 * - Column aliasing: WITH name(col1, col2) AS (...)
 */

// Re-export expression types from DML types for consistency
import type { Expression, SubqueryExpression } from './dml-types.js';

// =============================================================================
// CTE DEFINITION TYPES
// =============================================================================

/**
 * A single CTE definition
 *
 * Example:
 *   employees_in_dept AS (SELECT * FROM employees WHERE dept_id = 5)
 *   ancestors(id, parent_id, name) AS (SELECT ...)
 */
export interface CTEDefinition {
  /** CTE name (table alias) */
  name: string;
  /** Optional column list for aliasing result columns */
  columns?: string[];
  /** The query that defines this CTE (as raw SQL string) */
  query: string;
  /** Whether this CTE is recursive */
  recursive: boolean;
  /**
   * For recursive CTEs, the structure:
   * - anchorQuery: The non-recursive base case (before UNION ALL)
   * - recursiveQuery: The recursive part (after UNION ALL, references self)
   */
  anchorQuery?: string;
  recursiveQuery?: string;
}

/**
 * WITH clause containing one or more CTEs
 *
 * Example:
 *   WITH
 *     dept_employees AS (SELECT * FROM employees WHERE dept_id = 5),
 *     managers AS (SELECT * FROM employees WHERE is_manager = 1)
 *   SELECT * FROM dept_employees JOIN managers ON ...
 */
export interface WithClause {
  type: 'with';
  /** Whether any CTE uses RECURSIVE keyword */
  recursive: boolean;
  /** List of CTE definitions (in order) */
  ctes: CTEDefinition[];
}

// =============================================================================
// SELECT STATEMENT WITH CTE
// =============================================================================

/**
 * A SELECT statement that may include a WITH clause
 *
 * This represents the full query including the WITH clause prefix
 */
export interface SelectWithCTE {
  type: 'select_with_cte';
  /** The WITH clause (if present) */
  with?: WithClause;
  /** The main SELECT query (as raw SQL, to be parsed separately) */
  mainQuery: string;
}

// =============================================================================
// MATERIALIZATION HINTS
// =============================================================================

/**
 * Materialization strategy for CTE execution
 */
export type CTEMaterializationHint =
  | 'MATERIALIZED'      // Force materialization (store results)
  | 'NOT MATERIALIZED'  // Force inline expansion
  | 'AUTO';             // Let the optimizer decide

/**
 * Extended CTE definition with execution hints
 */
export interface CTEDefinitionWithHints extends CTEDefinition {
  /** Materialization hint (PostgreSQL 12+ feature) */
  materialization?: CTEMaterializationHint;
}

// =============================================================================
// RECURSIVE CTE SPECIFIC TYPES
// =============================================================================

/**
 * Structure representing a recursive CTE's components
 *
 * A recursive CTE has the form:
 *   WITH RECURSIVE cte_name AS (
 *     <anchor_member>      -- Non-recursive term (base case)
 *     UNION [ALL]
 *     <recursive_member>   -- References cte_name
 *   )
 */
export interface RecursiveCTEStructure {
  /** The anchor (base case) query */
  anchor: string;
  /** The recursive query that references the CTE itself */
  recursive: string;
  /** Whether UNION ALL (true) or UNION DISTINCT (false) is used */
  unionAll: boolean;
}

/**
 * Execution limits for recursive CTEs to prevent infinite loops
 */
export interface RecursiveCTELimits {
  /** Maximum number of iterations allowed */
  maxIterations: number;
  /** Maximum total rows to produce */
  maxRows: number;
}

// =============================================================================
// PARSE RESULT TYPES
// =============================================================================

/**
 * Successful parse result for CTE
 */
export interface CTEParseSuccess {
  success: true;
  /** The parsed WITH clause */
  with: WithClause;
  /** The remaining SQL after the WITH clause (the main query) */
  mainQuery: string;
  /** Original full SQL */
  originalSql: string;
}

/**
 * Failed parse result for CTE
 */
export interface CTEParseError {
  success: false;
  error: string;
  /** Position where error occurred */
  position: number;
  /** The input that caused the error */
  input: string;
}

/**
 * Parse result for CTE
 */
export type CTEParseResult = CTEParseSuccess | CTEParseError;

// =============================================================================
// TYPE GUARDS
// =============================================================================

/**
 * Check if a CTE definition is recursive
 */
export function isRecursiveCTE(cte: CTEDefinition): boolean {
  return cte.recursive === true;
}

/**
 * Check if a WITH clause contains any recursive CTEs
 */
export function hasRecursiveCTE(withClause: WithClause): boolean {
  return withClause.recursive || withClause.ctes.some(cte => cte.recursive);
}

/**
 * Check if parse result is successful
 */
export function isCTEParseSuccess(result: CTEParseResult): result is CTEParseSuccess {
  return result.success === true;
}

/**
 * Check if parse result is an error
 */
export function isCTEParseError(result: CTEParseResult): result is CTEParseError {
  return result.success === false;
}

// =============================================================================
// HELPER TYPES FOR CTE RESOLUTION
// =============================================================================

/**
 * CTE scope for name resolution during query planning
 * Maps CTE names to their definitions
 */
export type CTEScope = Map<string, CTEDefinition>;

/**
 * CTE reference in a query (when the main query references a CTE)
 */
export interface CTEReference {
  type: 'cte_ref';
  /** Name of the CTE being referenced */
  name: string;
  /** Optional alias for this reference */
  alias?: string;
}

/**
 * Check if a table reference is a CTE reference
 */
export function isCTEReference(
  tableName: string,
  scope: CTEScope
): boolean {
  return scope.has(tableName);
}

/**
 * Get CTE definition from scope
 */
export function getCTEFromScope(
  tableName: string,
  scope: CTEScope
): CTEDefinition | undefined {
  return scope.get(tableName);
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create a simple (non-recursive) CTE definition
 */
export function createCTE(
  name: string,
  query: string,
  columns?: string[]
): CTEDefinition {
  return {
    name,
    query,
    columns,
    recursive: false,
  };
}

/**
 * Create a recursive CTE definition
 */
export function createRecursiveCTE(
  name: string,
  anchorQuery: string,
  recursiveQuery: string,
  columns?: string[]
): CTEDefinition {
  return {
    name,
    query: `${anchorQuery} UNION ALL ${recursiveQuery}`,
    columns,
    recursive: true,
    anchorQuery,
    recursiveQuery,
  };
}

/**
 * Create a WITH clause
 */
export function createWithClause(
  ctes: CTEDefinition[],
  recursive: boolean = false
): WithClause {
  return {
    type: 'with',
    recursive: recursive || ctes.some(cte => cte.recursive),
    ctes,
  };
}

/**
 * Create a CTE scope from a WITH clause
 */
export function createCTEScope(withClause: WithClause): CTEScope {
  const scope: CTEScope = new Map();
  for (const cte of withClause.ctes) {
    scope.set(cte.name.toLowerCase(), cte);
  }
  return scope;
}
