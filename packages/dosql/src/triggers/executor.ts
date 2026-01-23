/**
 * Trigger Executor Facade
 *
 * Main entry point for trigger execution that coordinates both:
 * - JavaScript/TypeScript triggers (via js-executor.ts)
 * - SQL triggers (via sql-trigger-executor.ts)
 *
 * This module provides backward-compatible exports and factory functions
 * for creating trigger executors with unified interfaces.
 */

// =============================================================================
// Re-exports from JavaScript Trigger Executor
// =============================================================================

export {
  // Types
  type JSTriggerExecutorOptions,
  // Context builders
  buildTriggerContext,
  buildMeta,
  // Direct execution functions
  executeDirectly,
  executeSandboxed,
  // Factory function
  createJSTriggerExecutor,
  // Re-exported utilities
  buildSandboxModule,
} from './js-executor.js';

// =============================================================================
// Re-exports from SQL Trigger Executor
// =============================================================================

export {
  // Types
  type SQLTriggerExecutor,
  type SQLTriggerExecutorOptions,
  // RAISE function utilities
  parseRaiseFunctions,
  parseRaiseFunction,
  evaluateRaiseCondition,
  // Column change detection
  didColumnsChange,
  // Factory function
  createSQLTriggerExecutor,
} from './sql-trigger-executor.js';

// =============================================================================
// Backward-compatible exports (aliases)
// =============================================================================

import type { TriggerRegistry, TriggerExecutor, TriggerExecutionOptions } from './types.js';
import type { DatabaseContext, DatabaseSchema } from '../proc/types.js';
import { createJSTriggerExecutor, type JSTriggerExecutorOptions } from './js-executor.js';
import { createSQLTriggerExecutor, type SQLTriggerExecutorOptions } from './sql-trigger-executor.js';

/**
 * Options for creating a trigger executor
 * @deprecated Use JSTriggerExecutorOptions or SQLTriggerExecutorOptions instead
 */
export interface TriggerExecutorOptions<DB extends DatabaseSchema = DatabaseSchema>
  extends JSTriggerExecutorOptions<DB> {}

/**
 * Create a trigger executor (backward-compatible alias)
 *
 * For JavaScript-only triggers, use createJSTriggerExecutor.
 * For SQL triggers support, use createSQLTriggerExecutor.
 *
 * @deprecated Use createJSTriggerExecutor or createSQLTriggerExecutor instead
 */
export function createTriggerExecutor<DB extends DatabaseSchema = DatabaseSchema>(
  options: TriggerExecutorOptions<DB>
): TriggerExecutor {
  return createJSTriggerExecutor(options);
}

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Create a simple executor for testing (non-sandboxed)
 * Supports both JavaScript triggers and SQL triggers
 */
export function createSimpleTriggerExecutor(
  registry: TriggerRegistry,
  tables: Record<string, Array<Record<string, unknown>>>
) {
  // Create a minimal database context for testing
  const db: DatabaseContext = {
    tables: Object.fromEntries(
      Object.entries(tables).map(([name, data]) => [
        name,
        {
          async get(key: string | number) {
            return data.find((r: Record<string, unknown>) => r.id === key);
          },
          async where(predicate: unknown) {
            if (typeof predicate === 'function') {
              return data.filter(predicate as (r: Record<string, unknown>) => boolean);
            }
            return data.filter(r =>
              Object.entries(predicate as Record<string, unknown>).every(
                ([k, v]) => r[k] === v
              )
            );
          },
          async all() {
            return [...data];
          },
          async count(predicate?: unknown) {
            if (!predicate) return data.length;
            const filtered = await this.where(predicate);
            return filtered.length;
          },
          async insert(record: Record<string, unknown>) {
            const newRecord = { ...record, id: record.id ?? data.length + 1 };
            data.push(newRecord);
            return newRecord;
          },
          async update(predicate: unknown, changes: Record<string, unknown>) {
            const matches = await this.where(predicate);
            for (const match of matches) {
              Object.assign(match, changes);
            }
            return matches.length;
          },
          async delete(predicate: unknown) {
            const matches = await this.where(predicate);
            for (const match of matches) {
              const idx = data.indexOf(match);
              if (idx >= 0) data.splice(idx, 1);
            }
            return matches.length;
          },
        },
      ])
    ) as any,
    sql: async () => [],
    transaction: async (fn: any) => fn({ tables: db.tables, sql: db.sql, commit: async () => {}, rollback: async () => {} }),
  };

  return createSQLTriggerExecutor({ registry, db });
}

/**
 * Wrap a table accessor with trigger execution
 */
export function wrapWithTriggers<T extends Record<string, unknown>>(
  tableName: string,
  accessor: {
    get(key: string | number): Promise<T | undefined>;
    insert(record: Omit<T, 'id'>): Promise<T>;
    update(predicate: unknown, changes: Partial<T>): Promise<number>;
    delete(predicate: unknown): Promise<number>;
    where(predicate: unknown): Promise<T[]>;
  },
  executor: TriggerExecutor,
  execOptions?: TriggerExecutionOptions
): typeof accessor {
  return {
    get: accessor.get.bind(accessor),
    where: accessor.where.bind(accessor),

    async insert(record: Omit<T, 'id'>): Promise<T> {
      const { before, result, after } = await executor.executeAll<T>(
        tableName,
        'insert',
        undefined,
        record as T,
        async (row) => {
          if (!row) return undefined;
          return accessor.insert(row as Omit<T, 'id'>);
        },
        execOptions
      );

      if (!before.proceed) {
        throw before.error;
      }

      return result as T;
    },

    async update(predicate: unknown, changes: Partial<T>): Promise<number> {
      // For update, we'd need to fetch rows first to get old values
      const oldRows = await accessor.where(predicate);

      let count = 0;
      for (const oldRow of oldRows) {
        const newRow = { ...oldRow, ...changes } as T;

        const { before } = await executor.executeAll<T>(
          tableName,
          'update',
          oldRow,
          newRow,
          async (row) => {
            if (!row) return undefined;
            // Apply the actual update
            await accessor.update({ id: oldRow.id }, row as Partial<T>);
            return row;
          },
          execOptions
        );

        if (before.proceed) {
          count++;
        }
      }

      return count;
    },

    async delete(predicate: unknown): Promise<number> {
      const rows = await accessor.where(predicate);

      let count = 0;
      for (const row of rows) {
        const { before } = await executor.executeAll<T>(
          tableName,
          'delete',
          row,
          undefined,
          async () => {
            await accessor.delete({ id: (row as Record<string, unknown>).id });
            return undefined;
          },
          execOptions
        );

        if (before.proceed) {
          count++;
        }
      }

      return count;
    },
  };
}
