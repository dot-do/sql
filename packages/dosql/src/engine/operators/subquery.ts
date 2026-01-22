/**
 * Subquery Executor Operators
 *
 * Implements execution strategies for different types of subqueries:
 * - Scalar subqueries (single value)
 * - IN subqueries (semi-join)
 * - EXISTS subqueries
 * - Correlated subqueries
 * - ANY/ALL/SOME quantified comparisons
 */

import {
  type ExecutionContext,
  type Operator,
  type Row,
  type QueryPlan,
  type SqlValue,
  type Expression,
  type Predicate,
  type ScanPlan,
  type FilterPlan,
} from '../types.js';
import { evaluateExpression, evaluatePredicate } from './filter.js';

// =============================================================================
// SUBQUERY PLAN TYPES
// =============================================================================

/**
 * Plan node for scalar subquery
 */
export interface ScalarSubqueryPlan {
  type: 'scalarSubquery';
  id: number;
  subquery: QueryPlan;
  /** Column names from outer query that this subquery references */
  correlatedColumns?: string[];
}

/**
 * Plan node for IN subquery (semi-join)
 */
export interface InSubqueryPlan {
  type: 'inSubquery';
  id: number;
  input: QueryPlan;
  /** Expression to match against subquery results */
  expr: Expression;
  /** The subquery plan */
  subquery: QueryPlan;
  /** NOT IN if true */
  not: boolean;
  /** Column names from outer query that this subquery references */
  correlatedColumns?: string[];
}

/**
 * Plan node for EXISTS subquery
 */
export interface ExistsSubqueryPlan {
  type: 'existsSubquery';
  id: number;
  input: QueryPlan;
  /** The EXISTS subquery plan */
  subquery: QueryPlan;
  /** NOT EXISTS if true */
  not: boolean;
  /** Column names from outer query that this subquery references */
  correlatedColumns?: string[];
}

/**
 * Plan node for ANY/ALL/SOME subquery
 */
export interface QuantifiedSubqueryPlan {
  type: 'quantifiedSubquery';
  id: number;
  input: QueryPlan;
  /** Left operand expression */
  left: Expression;
  /** Comparison operator */
  op: 'eq' | 'ne' | 'lt' | 'le' | 'gt' | 'ge';
  /** ANY, ALL, or SOME */
  quantifier: 'any' | 'all' | 'some';
  /** The subquery plan */
  subquery: QueryPlan;
  /** Column names from outer query that this subquery references */
  correlatedColumns?: string[];
}

/**
 * Plan node for derived table (subquery in FROM)
 */
export interface DerivedTablePlan {
  type: 'derivedTable';
  id: number;
  subquery: QueryPlan;
  alias: string;
  /** LATERAL derived table references outer columns */
  lateral?: boolean;
  correlatedColumns?: string[];
}

// =============================================================================
// SUBQUERY CONTEXT
// =============================================================================

/**
 * Extended execution context with outer row for correlated subqueries
 */
export interface SubqueryContext extends ExecutionContext {
  /** Current row from outer query (for correlated subqueries) */
  outerRow?: Row;
  /** Stack of outer rows for nested correlated subqueries */
  outerRowStack?: Row[];
}

// =============================================================================
// SCALAR SUBQUERY OPERATOR
// =============================================================================

/**
 * Executes scalar subqueries - returns a single value
 */
export class ScalarSubqueryOperator {
  private plan: ScalarSubqueryPlan;
  private ctx: SubqueryContext;
  private createOperator: (plan: QueryPlan, ctx: ExecutionContext) => Operator;
  private cachedResult: SqlValue | undefined;
  private isCorrelated: boolean;

  constructor(
    plan: ScalarSubqueryPlan,
    ctx: SubqueryContext,
    createOperator: (plan: QueryPlan, ctx: ExecutionContext) => Operator
  ) {
    this.plan = plan;
    this.ctx = ctx;
    this.createOperator = createOperator;
    this.isCorrelated = (plan.correlatedColumns?.length ?? 0) > 0;
  }

  /**
   * Execute the scalar subquery and return its single value
   */
  async execute(outerRow?: Row): Promise<SqlValue> {
    // For non-correlated subqueries, cache the result
    if (!this.isCorrelated && this.cachedResult !== undefined) {
      return this.cachedResult;
    }

    // Build context with outer row for correlated subqueries
    const subCtx: SubqueryContext = {
      ...this.ctx,
      outerRow: outerRow || this.ctx.outerRow,
    };

    const operator = this.createOperator(this.plan.subquery, subCtx);
    await operator.open(subCtx);

    try {
      const row = await operator.next();

      if (row === null) {
        // Empty result - return NULL
        this.cachedResult = null;
        return null;
      }

      // Get the first column value
      const columns = operator.columns();
      const value = row[columns[0]] ?? null;

      // Verify only one row
      const secondRow = await operator.next();
      if (secondRow !== null) {
        throw new Error('Scalar subquery returned more than one row');
      }

      if (!this.isCorrelated) {
        this.cachedResult = value;
      }

      return value;
    } finally {
      await operator.close();
    }
  }
}

// =============================================================================
// IN SUBQUERY OPERATOR
// =============================================================================

/**
 * Executes IN subquery as a semi-join or anti-join
 *
 * For uncorrelated IN subqueries, materializes the subquery once.
 * For correlated IN subqueries, re-executes per outer row.
 */
export class InSubqueryOperator implements Operator {
  private plan: InSubqueryPlan;
  private input: Operator;
  private ctx!: SubqueryContext;
  private createOperator: (plan: QueryPlan, ctx: ExecutionContext) => Operator;
  private materializedValues: Set<string> | null = null;
  private isCorrelated: boolean;

  constructor(
    plan: InSubqueryPlan,
    input: Operator,
    ctx: SubqueryContext,
    createOperator: (plan: QueryPlan, ctx: ExecutionContext) => Operator
  ) {
    this.plan = plan;
    this.input = input;
    this.ctx = ctx;
    this.createOperator = createOperator;
    this.isCorrelated = (plan.correlatedColumns?.length ?? 0) > 0;
  }

  async open(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx as SubqueryContext;
    await this.input.open(ctx);

    // For uncorrelated subqueries, materialize once
    if (!this.isCorrelated) {
      await this.materializeSubquery();
    }
  }

  async next(): Promise<Row | null> {
    while (true) {
      const row = await this.input.next();
      if (row === null) return null;

      const matches = await this.checkMembership(row);
      const result = this.plan.not ? !matches : matches;

      if (result) {
        return row;
      }
    }
  }

  async close(): Promise<void> {
    await this.input.close();
    this.materializedValues = null;
  }

  columns(): string[] {
    return this.input.columns();
  }

  private async materializeSubquery(outerRow?: Row): Promise<void> {
    const subCtx: SubqueryContext = {
      ...this.ctx,
      outerRow,
    };

    const operator = this.createOperator(this.plan.subquery, subCtx);
    await operator.open(subCtx);

    this.materializedValues = new Set();
    const columns = operator.columns();

    try {
      while (true) {
        const row = await operator.next();
        if (row === null) break;

        // Get the subquery result value
        const value = row[columns[0]];
        if (value !== null) {
          this.materializedValues.add(this.valueToKey(value));
        }
      }
    } finally {
      await operator.close();
    }
  }

  private async checkMembership(outerRow: Row): Promise<boolean> {
    // For correlated subqueries, re-materialize with outer row context
    if (this.isCorrelated) {
      await this.materializeSubquery(outerRow);
    }

    if (!this.materializedValues) {
      return false;
    }

    const value = evaluateExpression(this.plan.expr, outerRow);
    if (value === null) {
      // NULL IN (...) is NULL/false
      return false;
    }

    return this.materializedValues.has(this.valueToKey(value));
  }

  private valueToKey(value: SqlValue): string {
    if (value === null) return 'NULL';
    if (value instanceof Date) return `D:${value.toISOString()}`;
    if (value instanceof Uint8Array) return `B:${Array.from(value).join(',')}`;
    return `${typeof value}:${value}`;
  }
}

// =============================================================================
// EXISTS SUBQUERY OPERATOR
// =============================================================================

/**
 * Executes EXISTS subquery
 *
 * For each outer row, checks if the correlated subquery returns any rows.
 */
export class ExistsSubqueryOperator implements Operator {
  private plan: ExistsSubqueryPlan;
  private input: Operator;
  private ctx!: SubqueryContext;
  private createOperator: (plan: QueryPlan, ctx: ExecutionContext) => Operator;

  constructor(
    plan: ExistsSubqueryPlan,
    input: Operator,
    ctx: SubqueryContext,
    createOperator: (plan: QueryPlan, ctx: ExecutionContext) => Operator
  ) {
    this.plan = plan;
    this.input = input;
    this.ctx = ctx;
    this.createOperator = createOperator;
  }

  async open(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx as SubqueryContext;
    await this.input.open(ctx);
  }

  async next(): Promise<Row | null> {
    while (true) {
      const row = await this.input.next();
      if (row === null) return null;

      const exists = await this.checkExists(row);
      const result = this.plan.not ? !exists : exists;

      if (result) {
        return row;
      }
    }
  }

  async close(): Promise<void> {
    await this.input.close();
  }

  columns(): string[] {
    return this.input.columns();
  }

  private async checkExists(outerRow: Row): Promise<boolean> {
    const subCtx: SubqueryContext = {
      ...this.ctx,
      outerRow,
    };

    const operator = this.createOperator(this.plan.subquery, subCtx);
    await operator.open(subCtx);

    try {
      const row = await operator.next();
      return row !== null;
    } finally {
      await operator.close();
    }
  }
}

// =============================================================================
// QUANTIFIED COMPARISON OPERATOR (ANY/ALL/SOME)
// =============================================================================

/**
 * Executes ANY/ALL/SOME subquery comparisons
 *
 * - value = ANY (subquery): true if value equals at least one subquery result
 * - value > ALL (subquery): true if value is greater than all subquery results
 * - value < SOME (subquery): same as ANY
 */
export class QuantifiedSubqueryOperator implements Operator {
  private plan: QuantifiedSubqueryPlan;
  private input: Operator;
  private ctx!: SubqueryContext;
  private createOperator: (plan: QueryPlan, ctx: ExecutionContext) => Operator;
  private materializedValues: SqlValue[] | null = null;
  private isCorrelated: boolean;

  constructor(
    plan: QuantifiedSubqueryPlan,
    input: Operator,
    ctx: SubqueryContext,
    createOperator: (plan: QueryPlan, ctx: ExecutionContext) => Operator
  ) {
    this.plan = plan;
    this.input = input;
    this.ctx = ctx;
    this.createOperator = createOperator;
    this.isCorrelated = (plan.correlatedColumns?.length ?? 0) > 0;
  }

  async open(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx as SubqueryContext;
    await this.input.open(ctx);

    // For uncorrelated subqueries, materialize once
    if (!this.isCorrelated) {
      await this.materializeSubquery();
    }
  }

  async next(): Promise<Row | null> {
    while (true) {
      const row = await this.input.next();
      if (row === null) return null;

      const matches = await this.checkQuantified(row);
      if (matches) {
        return row;
      }
    }
  }

  async close(): Promise<void> {
    await this.input.close();
    this.materializedValues = null;
  }

  columns(): string[] {
    return this.input.columns();
  }

  private async materializeSubquery(outerRow?: Row): Promise<void> {
    const subCtx: SubqueryContext = {
      ...this.ctx,
      outerRow,
    };

    const operator = this.createOperator(this.plan.subquery, subCtx);
    await operator.open(subCtx);

    this.materializedValues = [];
    const columns = operator.columns();

    try {
      while (true) {
        const row = await operator.next();
        if (row === null) break;

        const value = row[columns[0]];
        this.materializedValues.push(value);
      }
    } finally {
      await operator.close();
    }
  }

  private async checkQuantified(outerRow: Row): Promise<boolean> {
    // For correlated subqueries, re-materialize with outer row context
    if (this.isCorrelated) {
      await this.materializeSubquery(outerRow);
    }

    if (!this.materializedValues || this.materializedValues.length === 0) {
      // Empty subquery:
      // - ANY/SOME: false (no values to match)
      // - ALL: true (vacuously true)
      return this.plan.quantifier === 'all';
    }

    const leftValue = evaluateExpression(this.plan.left, outerRow);
    if (leftValue === null) {
      // NULL compared to anything is NULL/false
      return false;
    }

    const quantifier = this.plan.quantifier === 'some' ? 'any' : this.plan.quantifier;

    if (quantifier === 'any') {
      // True if ANY comparison succeeds
      return this.materializedValues.some(v => this.compare(leftValue, v));
    } else {
      // True if ALL comparisons succeed
      return this.materializedValues.every(v => {
        if (v === null) return false; // NULL fails comparison
        return this.compare(leftValue, v);
      });
    }
  }

  private compare(left: SqlValue, right: SqlValue): boolean {
    if (left === null || right === null) return false;

    switch (this.plan.op) {
      case 'eq': return left === right;
      case 'ne': return left !== right;
      case 'lt': return left < right;
      case 'le': return left <= right;
      case 'gt': return left > right;
      case 'ge': return left >= right;
      default: return false;
    }
  }
}

// =============================================================================
// DERIVED TABLE OPERATOR
// =============================================================================

/**
 * Executes derived table (subquery in FROM clause)
 *
 * For non-LATERAL derived tables, materializes the subquery once.
 * For LATERAL derived tables, re-executes per outer row.
 */
export class DerivedTableOperator implements Operator {
  private plan: DerivedTablePlan;
  private ctx!: SubqueryContext;
  private createOperator: (plan: QueryPlan, ctx: ExecutionContext) => Operator;
  private innerOperator: Operator | null = null;
  private materializedRows: Row[] | null = null;
  private currentIndex = 0;
  private outputColumns: string[] = [];

  constructor(
    plan: DerivedTablePlan,
    ctx: SubqueryContext,
    createOperator: (plan: QueryPlan, ctx: ExecutionContext) => Operator
  ) {
    this.plan = plan;
    this.ctx = ctx;
    this.createOperator = createOperator;
  }

  async open(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx as SubqueryContext;

    // For non-lateral, materialize once
    if (!this.plan.lateral) {
      await this.materialize();
    } else {
      // For lateral, create operator with outer row context
      this.innerOperator = this.createOperator(this.plan.subquery, this.ctx);
      await this.innerOperator.open(this.ctx);
      this.outputColumns = this.innerOperator.columns().map(c => `${this.plan.alias}.${c}`);
    }
  }

  async next(): Promise<Row | null> {
    if (this.materializedRows) {
      if (this.currentIndex >= this.materializedRows.length) {
        return null;
      }
      return this.materializedRows[this.currentIndex++];
    }

    if (this.innerOperator) {
      const row = await this.innerOperator.next();
      if (row === null) return null;

      // Prefix columns with alias
      const aliasedRow: Row = {};
      for (const [key, value] of Object.entries(row)) {
        aliasedRow[`${this.plan.alias}.${key}`] = value;
      }
      return aliasedRow;
    }

    return null;
  }

  async close(): Promise<void> {
    if (this.innerOperator) {
      await this.innerOperator.close();
    }
    this.materializedRows = null;
  }

  columns(): string[] {
    return this.outputColumns;
  }

  private async materialize(): Promise<void> {
    const operator = this.createOperator(this.plan.subquery, this.ctx);
    await operator.open(this.ctx);

    this.materializedRows = [];
    const innerColumns = operator.columns();
    this.outputColumns = innerColumns.map(c => `${this.plan.alias}.${c}`);

    try {
      while (true) {
        const row = await operator.next();
        if (row === null) break;

        // Prefix columns with alias
        const aliasedRow: Row = {};
        for (const [key, value] of Object.entries(row)) {
          aliasedRow[`${this.plan.alias}.${key}`] = value;
        }
        this.materializedRows.push(aliasedRow);
      }
    } finally {
      await operator.close();
    }
  }
}

// =============================================================================
// LATERAL JOIN OPERATOR
// =============================================================================

/**
 * Executes LATERAL JOIN (correlated derived table)
 *
 * For each outer row, executes the LATERAL subquery with outer row context.
 */
export class LateralJoinOperator implements Operator {
  private leftOperator: Operator;
  private subqueryPlan: QueryPlan;
  private ctx!: SubqueryContext;
  private createOperator: (plan: QueryPlan, ctx: ExecutionContext) => Operator;
  private currentLeftRow: Row | null = null;
  private rightOperator: Operator | null = null;
  private alias: string;

  constructor(
    leftOperator: Operator,
    subqueryPlan: QueryPlan,
    alias: string,
    ctx: SubqueryContext,
    createOperator: (plan: QueryPlan, ctx: ExecutionContext) => Operator
  ) {
    this.leftOperator = leftOperator;
    this.subqueryPlan = subqueryPlan;
    this.alias = alias;
    this.ctx = ctx;
    this.createOperator = createOperator;
  }

  async open(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx as SubqueryContext;
    await this.leftOperator.open(ctx);
  }

  async next(): Promise<Row | null> {
    while (true) {
      // Try to get next row from current right side
      if (this.rightOperator) {
        const rightRow = await this.rightOperator.next();
        if (rightRow !== null) {
          // Combine left and right rows
          return { ...this.currentLeftRow, ...this.prefixColumns(rightRow) };
        }
        // Right side exhausted, close it
        await this.rightOperator.close();
        this.rightOperator = null;
      }

      // Get next left row
      this.currentLeftRow = await this.leftOperator.next();
      if (this.currentLeftRow === null) {
        return null;
      }

      // Execute lateral subquery with left row as outer context
      const subCtx: SubqueryContext = {
        ...this.ctx,
        outerRow: this.currentLeftRow,
      };

      this.rightOperator = this.createOperator(this.subqueryPlan, subCtx);
      await this.rightOperator.open(subCtx);
    }
  }

  async close(): Promise<void> {
    await this.leftOperator.close();
    if (this.rightOperator) {
      await this.rightOperator.close();
    }
  }

  columns(): string[] {
    return [
      ...this.leftOperator.columns(),
      // Right columns will be prefixed
    ];
  }

  private prefixColumns(row: Row): Row {
    const prefixed: Row = {};
    for (const [key, value] of Object.entries(row)) {
      prefixed[`${this.alias}.${key}`] = value;
    }
    return prefixed;
  }
}

// =============================================================================
// EXPRESSION EVALUATOR WITH SUBQUERY SUPPORT
// =============================================================================

/**
 * Extended expression evaluation that handles subqueries
 */
export async function evaluateExpressionWithSubqueries(
  expr: Expression & { subquery?: { query: QueryPlan } },
  row: Row,
  ctx: SubqueryContext,
  createOperator: (plan: QueryPlan, ctx: ExecutionContext) => Operator
): Promise<SqlValue> {
  // Handle subquery expression
  if (expr.type === 'subquery' && expr.subquery) {
    const scalarOp = new ScalarSubqueryOperator(
      {
        type: 'scalarSubquery',
        id: 0,
        subquery: expr.subquery.query,
      },
      ctx,
      createOperator
    );
    return scalarOp.execute(row);
  }

  // Delegate to regular expression evaluator for non-subquery expressions
  return evaluateExpression(expr, row);
}

// =============================================================================
// SUBQUERY REWRITER (FOR OPTIMIZATION)
// =============================================================================

/**
 * Utilities for rewriting subqueries into joins
 */
export const SubqueryRewriter = {
  /**
   * Check if an IN subquery can be rewritten as a semi-join
   */
  canRewriteInAsSemiJoin(plan: InSubqueryPlan): boolean {
    // Can rewrite if:
    // 1. Not correlated OR has simple correlation predicate
    // 2. No LIMIT/OFFSET in subquery
    // 3. No DISTINCT with complex expressions
    return !plan.correlatedColumns?.length;
  },

  /**
   * Check if EXISTS can be rewritten as a semi-join
   */
  canRewriteExistsAsSemiJoin(plan: ExistsSubqueryPlan): boolean {
    // EXISTS can usually be rewritten if correlation is on equality
    return true;
  },

  /**
   * Check if NOT IN can be rewritten as an anti-join
   */
  canRewriteNotInAsAntiJoin(plan: InSubqueryPlan): boolean {
    // NOT IN is trickier due to NULL semantics
    // Can only rewrite if subquery column is NOT NULL
    return plan.not && !plan.correlatedColumns?.length;
  },
};
