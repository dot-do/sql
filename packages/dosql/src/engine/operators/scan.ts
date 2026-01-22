/**
 * Scan Operator
 *
 * Reads rows from B-tree or columnar storage.
 * Supports predicate pushdown to filter at the storage level.
 */

import {
  type ScanPlan,
  type ExecutionContext,
  type Operator,
  type Row,
  type Predicate,
  type SqlValue,
} from '../types.js';
import { evaluatePredicate } from './filter.js';
import type { Predicate as ColumnarPredicate } from '../../columnar/index.js';

/**
 * Convert our predicate format to columnar predicate format
 */
function toColumnarPredicates(predicate: Predicate | undefined): ColumnarPredicate[] | undefined {
  if (!predicate) return undefined;

  const predicates: ColumnarPredicate[] = [];

  function extract(p: Predicate): void {
    switch (p.type) {
      case 'comparison': {
        // Only handle simple column = literal comparisons
        if (p.left.type === 'columnRef' && p.right.type === 'literal') {
          const opMap: Record<string, ColumnarPredicate['op']> = {
            eq: 'eq', ne: 'ne', lt: 'lt', le: 'le', gt: 'gt', ge: 'ge',
          };
          if (opMap[p.op]) {
            predicates.push({
              column: p.left.column,
              op: opMap[p.op],
              value: p.right.value as number | bigint | string | null,
            });
          }
        }
        break;
      }
      case 'logical': {
        if (p.op === 'and') {
          for (const operand of p.operands) {
            extract(operand);
          }
        }
        // OR and NOT are harder to push down, skip
        break;
      }
      case 'between': {
        if (p.expr.type === 'columnRef' && p.low.type === 'literal' && p.high.type === 'literal') {
          predicates.push({
            column: p.expr.column,
            op: 'between',
            value: p.low.value as number | bigint | string,
            value2: p.high.value as number | bigint | string,
          });
        }
        break;
      }
      case 'in': {
        if (p.expr.type === 'columnRef' && Array.isArray(p.values)) {
          const values = p.values
            .filter(v => v.type === 'literal')
            .map(v => (v as { type: 'literal'; value: SqlValue }).value as number | bigint | string);
          if (values.length > 0) {
            predicates.push({
              column: p.expr.column,
              op: 'in',
              value: values,
            });
          }
        }
        break;
      }
      case 'isNull': {
        if (p.expr.type === 'columnRef') {
          predicates.push({
            column: p.expr.column,
            op: p.isNot ? 'ne' : 'eq',
            value: null,
          });
        }
        break;
      }
    }
  }

  extract(predicate);
  return predicates.length > 0 ? predicates : undefined;
}

/**
 * Scan operator implementation
 */
export class ScanOperator implements Operator {
  private plan: ScanPlan;
  private ctx!: ExecutionContext;
  private iterator: AsyncIterator<Row> | null = null;
  private outputColumns: string[];

  constructor(plan: ScanPlan, ctx: ExecutionContext) {
    this.plan = plan;
    this.ctx = ctx;
    this.outputColumns = plan.columns;
  }

  async open(ctx: ExecutionContext): Promise<void> {
    this.ctx = ctx;
    const { table, source, predicate, columns } = this.plan;

    // Choose data source
    if (source === 'columnar') {
      // Use columnar storage
      const columnarPredicates = toColumnarPredicates(predicate);
      this.iterator = this.ctx.columnar.scan(table, {
        columns,
        predicates: columnarPredicates,
      });
    } else if (source === 'btree') {
      // Use B-tree storage
      this.iterator = this.ctx.btree.scan(table);
    } else {
      // 'both' - merge hot and cold data
      // For now, just use B-tree as primary and skip columnar
      // A full implementation would merge both sources
      this.iterator = this.ctx.btree.scan(table);
    }
  }

  async next(): Promise<Row | null> {
    if (!this.iterator) return null;

    while (true) {
      const result = await this.iterator.next();
      if (result.done) return null;

      const row = result.value;

      // Apply predicate if not pushed down
      if (this.plan.predicate && this.plan.source !== 'columnar') {
        if (!evaluatePredicate(this.plan.predicate, row)) {
          continue;
        }
      }

      // Project columns if specified
      if (this.plan.columns.length > 0 && !this.plan.columns.includes('*')) {
        const projected: Row = {};
        for (const col of this.plan.columns) {
          if (col in row) {
            projected[col] = row[col];
          }
        }
        return projected;
      }

      return row;
    }
  }

  async close(): Promise<void> {
    // Cleanup iterator if needed
    if (this.iterator && 'return' in this.iterator && typeof this.iterator.return === 'function') {
      await this.iterator.return();
    }
    this.iterator = null;
  }

  columns(): string[] {
    return this.outputColumns;
  }
}
