/**
 * CTE (Common Table Expression) Operator
 *
 * Handles execution of CTEs, including:
 * - Materializing CTE results
 * - Recursive CTE execution (iterate until fixed point)
 * - CTE reference resolution in main query
 *
 * Example recursive CTE execution flow:
 * 1. Execute anchor query (base case)
 * 2. Execute recursive query using previous iteration's results
 * 3. Repeat step 2 until no new rows are produced
 * 4. Return all accumulated rows
 */

import {
  type ExecutionContext,
  type Operator,
  type Row,
  type QueryPlan,
} from '../types.js';

import type {
  CTEDefinition,
  WithClause,
  RecursiveCTELimits,
} from '../../parser/cte-types.js';

// =============================================================================
// CONSTANTS
// =============================================================================

/** Default maximum iterations for recursive CTEs */
const DEFAULT_MAX_ITERATIONS = 1000;

/** Default maximum rows for recursive CTEs */
const DEFAULT_MAX_ROWS = 100000;

// =============================================================================
// CTE MATERIALIZATION
// =============================================================================

/**
 * Materialized CTE results
 *
 * Stores the computed rows from a CTE for reuse
 */
export interface MaterializedCTE {
  /** CTE name */
  name: string;
  /** Computed rows */
  rows: Row[];
  /** Column names (in order) */
  columns: string[];
}

/**
 * CTE execution context
 *
 * Extends the base execution context with CTE-specific data
 */
export interface CTEExecutionContext extends ExecutionContext {
  /** Materialized CTE results (name -> rows) */
  materializedCTEs: Map<string, MaterializedCTE>;
  /** Limits for recursive CTEs */
  recursiveLimits: RecursiveCTELimits;
}

/**
 * Create a CTE execution context
 */
export function createCTEContext(
  baseCtx: ExecutionContext,
  limits?: Partial<RecursiveCTELimits>
): CTEExecutionContext {
  return {
    ...baseCtx,
    materializedCTEs: new Map(),
    recursiveLimits: {
      maxIterations: limits?.maxIterations ?? DEFAULT_MAX_ITERATIONS,
      maxRows: limits?.maxRows ?? DEFAULT_MAX_ROWS,
    },
  };
}

// =============================================================================
// CTE SCAN OPERATOR
// =============================================================================

/**
 * Operator that scans a materialized CTE
 *
 * This is used when a query references a CTE - it reads from the materialized results
 */
export class CTEScanOperator implements Operator {
  private cteName: string;
  private alias?: string;
  private ctx!: CTEExecutionContext;
  private currentIndex = 0;
  private rows: Row[] = [];
  private outputColumns: string[] = [];

  constructor(cteName: string, alias?: string) {
    this.cteName = cteName;
    this.alias = alias;
  }

  async open(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx as CTEExecutionContext;

    // Get materialized CTE
    const materialized = this.ctx.materializedCTEs?.get(this.cteName.toLowerCase());
    if (!materialized) {
      throw new Error(`CTE '${this.cteName}' is not materialized. Ensure CTEs are executed before referencing them.`);
    }

    this.rows = materialized.rows;
    this.outputColumns = materialized.columns;
    this.currentIndex = 0;
  }

  async next(): Promise<Row | null> {
    if (this.currentIndex >= this.rows.length) {
      return null;
    }

    const row = this.rows[this.currentIndex++];

    // Apply alias if provided
    if (this.alias && this.alias !== this.cteName) {
      const aliasedRow: Row = {};
      for (const col of this.outputColumns) {
        aliasedRow[col] = row[col];
        // Also add with alias prefix for qualified references
        aliasedRow[`${this.alias}.${col}`] = row[col];
      }
      return aliasedRow;
    }

    return row;
  }

  async close(): Promise<void> {
    this.rows = [];
    this.currentIndex = 0;
  }

  columns(): string[] {
    return this.outputColumns;
  }
}

// =============================================================================
// SIMPLE CTE EXECUTOR
// =============================================================================

/**
 * Execute a non-recursive CTE and materialize its results
 */
export async function executeSimpleCTE(
  cte: CTEDefinition,
  queryExecutor: (sql: string, ctx: ExecutionContext) => Promise<Row[]>,
  ctx: CTEExecutionContext
): Promise<MaterializedCTE> {
  // Execute the CTE query
  const rows = await queryExecutor(cte.query, ctx);

  // Determine columns from first row or CTE definition
  let columns: string[];
  if (cte.columns && cte.columns.length > 0) {
    columns = cte.columns;
  } else if (rows.length > 0) {
    columns = Object.keys(rows[0]);
  } else {
    columns = [];
  }

  // If CTE has column aliases, rename the columns
  if (cte.columns && cte.columns.length > 0 && rows.length > 0) {
    const originalColumns = Object.keys(rows[0]);
    if (originalColumns.length !== cte.columns.length) {
      throw new Error(
        `CTE '${cte.name}' column count mismatch: ` +
        `expected ${cte.columns.length}, got ${originalColumns.length}`
      );
    }

    // Rename columns in all rows
    const renamedRows: Row[] = rows.map(row => {
      const newRow: Row = {};
      for (let i = 0; i < originalColumns.length; i++) {
        newRow[cte.columns![i]] = row[originalColumns[i]];
      }
      return newRow;
    });

    return {
      name: cte.name,
      rows: renamedRows,
      columns: cte.columns,
    };
  }

  return {
    name: cte.name,
    rows,
    columns,
  };
}

// =============================================================================
// RECURSIVE CTE EXECUTOR
// =============================================================================

/**
 * Execute a recursive CTE using iterative fixed-point algorithm
 *
 * Algorithm:
 * 1. Execute anchor query to get initial working table
 * 2. While working table is not empty:
 *    a. Execute recursive query using working table
 *    b. Add new rows to result
 *    c. Replace working table with new rows
 * 3. Return all accumulated rows
 */
export async function executeRecursiveCTE(
  cte: CTEDefinition,
  queryExecutor: (sql: string, ctx: ExecutionContext) => Promise<Row[]>,
  ctx: CTEExecutionContext
): Promise<MaterializedCTE> {
  if (!cte.anchorQuery || !cte.recursiveQuery) {
    throw new Error(
      `Recursive CTE '${cte.name}' must have both anchor and recursive queries`
    );
  }

  const { maxIterations, maxRows } = ctx.recursiveLimits;
  let columns: string[] = [];
  const allRows: Row[] = [];
  let workingTable: Row[] = [];

  // Step 1: Execute anchor query
  const anchorRows = await queryExecutor(cte.anchorQuery, ctx);

  if (anchorRows.length === 0) {
    // Base case returns nothing, CTE is empty
    return {
      name: cte.name,
      rows: [],
      columns: cte.columns || [],
    };
  }

  // Determine columns
  columns = cte.columns || Object.keys(anchorRows[0]);

  // Apply column aliases if needed
  if (cte.columns && cte.columns.length > 0) {
    const originalColumns = Object.keys(anchorRows[0]);
    workingTable = anchorRows.map(row => {
      const newRow: Row = {};
      for (let i = 0; i < originalColumns.length; i++) {
        newRow[cte.columns![i]] = row[originalColumns[i]];
      }
      return newRow;
    });
  } else {
    workingTable = [...anchorRows];
  }

  allRows.push(...workingTable);

  // Step 2: Iteratively execute recursive query
  let iteration = 0;

  while (workingTable.length > 0 && iteration < maxIterations) {
    // Check row limit
    if (allRows.length >= maxRows) {
      throw new Error(
        `Recursive CTE '${cte.name}' exceeded maximum row limit (${maxRows}). ` +
        `This may indicate an infinite loop.`
      );
    }

    // Update materialized CTE with current results for self-reference
    ctx.materializedCTEs.set(cte.name.toLowerCase(), {
      name: cte.name,
      rows: workingTable,
      columns,
    });

    // Execute recursive query
    const newRows = await queryExecutor(cte.recursiveQuery, ctx);

    // Apply column aliases
    let processedRows: Row[];
    if (cte.columns && cte.columns.length > 0 && newRows.length > 0) {
      const originalColumns = Object.keys(newRows[0]);
      processedRows = newRows.map(row => {
        const newRow: Row = {};
        for (let i = 0; i < originalColumns.length; i++) {
          newRow[cte.columns![i]] = row[originalColumns[i]];
        }
        return newRow;
      });
    } else {
      processedRows = newRows;
    }

    // Check for termination (no new rows)
    if (processedRows.length === 0) {
      break;
    }

    // Deduplicate: only keep rows we haven't seen before
    const newUniqueRows = processedRows.filter(newRow => {
      const key = JSON.stringify(newRow);
      return !allRows.some(existingRow => JSON.stringify(existingRow) === key);
    });

    if (newUniqueRows.length === 0) {
      // Fixed point reached - no new unique rows
      break;
    }

    allRows.push(...newUniqueRows);
    workingTable = newUniqueRows;
    iteration++;
  }

  if (iteration >= maxIterations) {
    throw new Error(
      `Recursive CTE '${cte.name}' exceeded maximum iterations (${maxIterations}). ` +
      `This may indicate an infinite loop.`
    );
  }

  return {
    name: cte.name,
    rows: allRows,
    columns,
  };
}

// =============================================================================
// WITH CLAUSE EXECUTOR
// =============================================================================

/**
 * Execute all CTEs in a WITH clause and materialize their results
 *
 * CTEs are executed in order, so later CTEs can reference earlier ones
 */
export async function executeWithClause(
  withClause: WithClause,
  queryExecutor: (sql: string, ctx: ExecutionContext) => Promise<Row[]>,
  baseCtx: ExecutionContext,
  limits?: Partial<RecursiveCTELimits>
): Promise<CTEExecutionContext> {
  const ctx = createCTEContext(baseCtx, limits);

  for (const cte of withClause.ctes) {
    let materialized: MaterializedCTE;

    if (cte.recursive) {
      materialized = await executeRecursiveCTE(cte, queryExecutor, ctx);
    } else {
      materialized = await executeSimpleCTE(cte, queryExecutor, ctx);
    }

    ctx.materializedCTEs.set(cte.name.toLowerCase(), materialized);
  }

  return ctx;
}

// =============================================================================
// CTE PLAN NODE
// =============================================================================

/**
 * Query plan node for CTE execution
 */
export interface CTEPlan {
  type: 'cte';
  id: number;
  /** The WITH clause */
  withClause: WithClause;
  /** The main query plan (executed after CTEs are materialized) */
  mainQuery: QueryPlan;
}

/**
 * Query plan node for scanning a CTE
 */
export interface CTEScanPlan {
  type: 'cteScan';
  id: number;
  /** CTE name to scan */
  cteName: string;
  /** Optional alias */
  alias?: string;
  /** Columns to project */
  columns: string[];
}

/**
 * Check if a plan node is a CTE plan
 */
export function isCTEPlan(plan: any): plan is CTEPlan {
  return plan?.type === 'cte';
}

/**
 * Check if a plan node is a CTE scan plan
 */
export function isCTEScanPlan(plan: any): plan is CTEScanPlan {
  return plan?.type === 'cteScan';
}

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

/**
 * Get all rows from a materialized CTE
 */
export function getCTERows(
  ctx: CTEExecutionContext,
  cteName: string
): Row[] | undefined {
  return ctx.materializedCTEs.get(cteName.toLowerCase())?.rows;
}

/**
 * Check if a CTE is materialized
 */
export function isCTEMaterialized(
  ctx: CTEExecutionContext,
  cteName: string
): boolean {
  return ctx.materializedCTEs.has(cteName.toLowerCase());
}

/**
 * Get column names from a materialized CTE
 */
export function getCTEColumns(
  ctx: CTEExecutionContext,
  cteName: string
): string[] | undefined {
  return ctx.materializedCTEs.get(cteName.toLowerCase())?.columns;
}

/**
 * Create a mock query executor for testing
 *
 * Takes a map of table name -> rows and returns rows when queried
 */
export function createMockQueryExecutor(
  tables: Map<string, Row[]>
): (sql: string, ctx: ExecutionContext) => Promise<Row[]> {
  return async (sql: string, ctx: ExecutionContext): Promise<Row[]> => {
    // Simple parser: extract table name from "FROM tablename"
    const fromMatch = sql.match(/FROM\s+(\w+)/i);
    if (fromMatch) {
      const tableName = fromMatch[1].toLowerCase();

      // Check if it's a CTE reference
      const cteCtx = ctx as CTEExecutionContext;
      if (cteCtx.materializedCTEs?.has(tableName)) {
        return cteCtx.materializedCTEs.get(tableName)!.rows;
      }

      // Check tables
      if (tables.has(tableName)) {
        return tables.get(tableName)!;
      }
    }

    return [];
  };
}
