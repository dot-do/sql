/**
 * SQL Trigger Executor
 *
 * Executes SQL triggers with support for:
 * - BEFORE/AFTER/INSTEAD OF triggers
 * - WHEN clause evaluation
 * - UPDATE OF column-specific triggers
 * - RAISE function handling (IGNORE, ROLLBACK, ABORT, FAIL)
 * - SQL trigger body execution
 */

import type {
  TriggerRegistry,
  TriggerEvent,
  TriggerTiming,
  TriggerExecutor,
  TriggerExecutionOptions,
  BeforeTriggerResult,
  AfterTriggerResult,
  TriggerExecution,
  SQLTriggerDefinition,
  SQLTriggerEvent,
  RaiseType,
  RaiseResult,
  ParsedSQLTrigger,
} from './types.js';
import { TriggerError, TriggerErrorCode } from './types.js';
import type { DatabaseContext, DatabaseSchema } from '../proc/types.js';
import { evaluateWhenClause } from '../utils/sql-eval.js';
import {
  createJSTriggerExecutor,
  type JSTriggerExecutorOptions,
} from './js-executor.js';

// =============================================================================
// SQL Trigger Types
// =============================================================================

/**
 * SQL Trigger executor interface (extends TriggerExecutor)
 */
export interface SQLTriggerExecutor extends TriggerExecutor {
  /**
   * Execute BEFORE triggers for SQL triggers
   */
  executeBefore<T extends Record<string, unknown>>(
    table: string,
    event: TriggerEvent | SQLTriggerEvent,
    oldRow: T | undefined,
    newRow: T | undefined,
    options?: TriggerExecutionOptions
  ): Promise<BeforeTriggerResult<T>>;

  /**
   * Execute AFTER triggers for SQL triggers
   */
  executeAfter<T extends Record<string, unknown>>(
    table: string,
    event: TriggerEvent | SQLTriggerEvent,
    oldRow: T | undefined,
    newRow: T | undefined,
    options?: TriggerExecutionOptions
  ): Promise<AfterTriggerResult>;
}

/**
 * Options for creating a SQL trigger executor
 */
export interface SQLTriggerExecutorOptions<DB extends DatabaseSchema = DatabaseSchema>
  extends JSTriggerExecutorOptions<DB> {
  /** SQL executor function for trigger body */
  sqlExecutor?: (sql: string, params: Record<string, unknown>) => Promise<unknown[]>;
}

// =============================================================================
// RAISE Function Parsing
// =============================================================================

/**
 * Parse all RAISE functions from SQL trigger body
 */
export function parseRaiseFunctions(body: string): RaiseResult[] {
  const results: RaiseResult[] = [];
  const raisePattern = /RAISE\s*\(\s*(IGNORE|ROLLBACK|ABORT|FAIL)(?:\s*,\s*['"](.*?)['"])?\s*\)/gi;

  let match;
  while ((match = raisePattern.exec(body)) !== null) {
    results.push({
      type: match[1].toUpperCase() as RaiseType,
      message: match[2],
    });
  }

  return results;
}

/**
 * Parse RAISE function from SQL trigger body (first match)
 */
export function parseRaiseFunction(body: string): RaiseResult | null {
  const results = parseRaiseFunctions(body);
  return results.length > 0 ? results[0] : null;
}

/**
 * Evaluate a simple SQL condition with CASE WHEN for RAISE
 * Returns the RAISE result if condition matches, null otherwise
 */
export function evaluateRaiseCondition<T extends Record<string, unknown>>(
  body: string,
  oldRow: T | undefined,
  newRow: T | undefined
): RaiseResult | null {
  // Look for SELECT CASE WHEN ... THEN RAISE(...) END patterns
  const caseMatch = body.match(/SELECT\s+CASE\s+WHEN\s+(.+?)\s+THEN\s+RAISE\s*\(\s*(IGNORE|ROLLBACK|ABORT|FAIL)(?:\s*,\s*['"](.*?)['"])?\s*\)/i);

  if (!caseMatch) {
    // Check for simple RAISE without condition
    return parseRaiseFunction(body);
  }

  const [, condition, raiseType, raiseMessage] = caseMatch;

  // Evaluate the condition
  const conditionMet = evaluateWhenClause(condition, oldRow, newRow);

  if (conditionMet) {
    return {
      type: raiseType.toUpperCase() as RaiseType,
      message: raiseMessage,
    };
  }

  return null;
}

// =============================================================================
// Column Change Detection
// =============================================================================

/**
 * Check if specific columns changed (for UPDATE OF triggers)
 */
export function didColumnsChange<T extends Record<string, unknown>>(
  columns: string[] | undefined,
  oldRow: T | undefined,
  newRow: T | undefined
): boolean {
  if (!columns || columns.length === 0) return true;
  if (!oldRow || !newRow) return true;

  for (const col of columns) {
    if (oldRow[col] !== newRow[col]) {
      return true;
    }
  }

  return false;
}

// =============================================================================
// SQL Trigger Executor Implementation
// =============================================================================

/**
 * Create a SQL trigger executor
 */
export function createSQLTriggerExecutor<DB extends DatabaseSchema = DatabaseSchema>(
  options: SQLTriggerExecutorOptions<DB>
): SQLTriggerExecutor {
  const baseExecutor = createJSTriggerExecutor(options);
  const { sqlExecutor, registry } = options;

  /**
   * Execute SQL trigger body
   */
  async function executeSQLBody<T extends Record<string, unknown>>(
    trigger: SQLTriggerDefinition | ParsedSQLTrigger,
    oldRow: T | undefined,
    newRow: T | undefined
  ): Promise<{ success: boolean; error?: string; raiseResult?: RaiseResult }> {
    if (!sqlExecutor) {
      // Simulate execution for testing by evaluating RAISE conditions
      const raiseResult = evaluateRaiseCondition(trigger.body, oldRow, newRow);

      if (raiseResult) {
        if (raiseResult.type === 'IGNORE') {
          return { success: true };
        }
        return {
          success: false,
          error: raiseResult.message || `${raiseResult.type} raised`,
          raiseResult,
        };
      }
      return { success: true };
    }

    try {
      // Build parameters from OLD and NEW
      const params: Record<string, unknown> = {};
      if (oldRow) {
        for (const [key, value] of Object.entries(oldRow)) {
          params[`OLD.${key}`] = value;
        }
      }
      if (newRow) {
        for (const [key, value] of Object.entries(newRow)) {
          params[`NEW.${key}`] = value;
        }
      }

      await sqlExecutor(trigger.body, params);
      return { success: true };
    } catch (error) {
      return {
        success: false,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  /**
   * Check if SQL trigger should fire based on WHEN clause and columns
   */
  function shouldFireSQLTrigger<T extends Record<string, unknown>>(
    trigger: SQLTriggerDefinition | ParsedSQLTrigger,
    event: SQLTriggerEvent,
    oldRow: T | undefined,
    newRow: T | undefined
  ): boolean {
    // Check if event matches
    if (trigger.event !== event && !trigger.events?.includes(event)) {
      return false;
    }

    // Check UPDATE OF columns
    if (event === 'UPDATE' && trigger.columns && trigger.columns.length > 0) {
      if (!didColumnsChange(trigger.columns, oldRow, newRow)) {
        return false;
      }
    }

    // Check WHEN clause
    if (trigger.whenClause) {
      return evaluateWhenClause(trigger.whenClause, oldRow, newRow);
    }

    return true;
  }

  return {
    ...baseExecutor,

    async executeBefore<T extends Record<string, unknown>>(
      table: string,
      event: TriggerEvent | SQLTriggerEvent,
      oldRow: T | undefined,
      newRow: T | undefined,
      execOptions: TriggerExecutionOptions = {}
    ): Promise<BeforeTriggerResult<T>> {
      const {
        currentDepth = 0,
        maxDepth = 10,
        skipDisabled = true,
      } = execOptions;

      // Check recursion depth
      if (currentDepth >= maxDepth) {
        return {
          proceed: false,
          error: new TriggerError(
            TriggerErrorCode.MAX_DEPTH_EXCEEDED,
            `Maximum trigger depth (${maxDepth}) exceeded`
          ),
          executions: [],
        };
      }

      // Get triggers from registry
      const normalizedEvent = event.toLowerCase() as TriggerEvent;
      let triggers = registry.getForTableEvent(table, 'before', normalizedEvent);

      // Also get SQL triggers (BEFORE timing)
      const sqlTriggers = registry.list({
        table,
        timing: 'BEFORE' as any,
        event: event.toUpperCase() as any,
        enabled: skipDisabled ? true : undefined,
      });

      // Filter disabled triggers
      if (skipDisabled) {
        triggers = triggers.filter(t => t.enabled !== false);
      }

      const executions: TriggerExecution[] = [];
      let currentRow = newRow;

      // Execute JavaScript triggers first
      for (const trigger of triggers) {
        if (trigger.handler) {
          const result = await baseExecutor.executeBefore(
            table,
            normalizedEvent,
            oldRow,
            currentRow,
            { ...execOptions, currentDepth: currentDepth + 1 }
          );

          executions.push(...result.executions);

          if (!result.proceed) {
            return result;
          }

          currentRow = result.row;
        }
      }

      // Execute SQL triggers (filter to only those with SQL body)
      for (const sqlTrigger of sqlTriggers) {
        // Skip if not a SQL trigger (no body property)
        const sqlDef = sqlTrigger as unknown as SQLTriggerDefinition | ParsedSQLTrigger;
        if (!sqlDef.body) {
          continue;
        }

        if (!shouldFireSQLTrigger(sqlDef, event.toUpperCase() as SQLTriggerEvent, oldRow, currentRow)) {
          continue;
        }

        const startTime = Date.now();
        const { success, error, raiseResult } = await executeSQLBody(
          sqlDef,
          oldRow,
          currentRow
        );
        const duration = Date.now() - startTime;

        executions.push({
          triggerName: sqlDef.name,
          timing: 'before',
          event: normalizedEvent,
          success,
          duration,
          error,
        });

        if (!success) {
          return {
            proceed: false,
            error: new TriggerError(
              TriggerErrorCode.OPERATION_REJECTED,
              error || 'Trigger rejected operation',
              sqlDef.name
            ),
            executions,
          };
        }
      }

      return {
        proceed: true,
        row: currentRow,
        executions,
      };
    },

    async executeAfter<T extends Record<string, unknown>>(
      table: string,
      event: TriggerEvent | SQLTriggerEvent,
      oldRow: T | undefined,
      newRow: T | undefined,
      execOptions: TriggerExecutionOptions = {}
    ): Promise<AfterTriggerResult> {
      const {
        currentDepth = 0,
        maxDepth = 10,
        skipDisabled = true,
      } = execOptions;

      // Check recursion depth
      if (currentDepth >= maxDepth) {
        return {
          success: false,
          errors: [{
            triggerName: 'depth_check',
            error: new TriggerError(
              TriggerErrorCode.MAX_DEPTH_EXCEEDED,
              `Maximum trigger depth (${maxDepth}) exceeded`
            ),
          }],
          executions: [],
        };
      }

      // Get JavaScript triggers
      const normalizedEvent = event.toLowerCase() as TriggerEvent;
      let triggers = registry.getForTableEvent(table, 'after', normalizedEvent);

      // Get SQL triggers (AFTER timing)
      const sqlTriggers = registry.list({
        table,
        timing: 'AFTER' as any,
        event: event.toUpperCase() as any,
        enabled: skipDisabled ? true : undefined,
      });

      // Filter disabled triggers
      if (skipDisabled) {
        triggers = triggers.filter(t => t.enabled !== false);
      }

      const executions: TriggerExecution[] = [];
      const errors: Array<{ triggerName: string; error: Error }> = [];

      // Execute JavaScript triggers
      for (const trigger of triggers) {
        if (trigger.handler) {
          const result = await baseExecutor.executeAfter(
            table,
            normalizedEvent,
            oldRow,
            newRow,
            { ...execOptions, currentDepth: currentDepth + 1 }
          );

          executions.push(...result.executions);
          errors.push(...result.errors);
        }
      }

      // Execute SQL triggers (filter to only those with SQL body)
      for (const sqlTrigger of sqlTriggers) {
        // Skip if not a SQL trigger (no body property)
        const sqlDef = sqlTrigger as unknown as SQLTriggerDefinition | ParsedSQLTrigger;
        if (!sqlDef.body) {
          continue;
        }

        if (!shouldFireSQLTrigger(sqlDef, event.toUpperCase() as SQLTriggerEvent, oldRow, newRow)) {
          continue;
        }

        const startTime = Date.now();
        const { success, error } = await executeSQLBody(sqlDef, oldRow, newRow);
        const duration = Date.now() - startTime;

        executions.push({
          triggerName: sqlDef.name,
          timing: 'after',
          event: normalizedEvent,
          success,
          duration,
          error,
        });

        if (!success) {
          errors.push({
            triggerName: sqlDef.name,
            error: new Error(error || 'Unknown error'),
          });
        }
      }

      return {
        success: errors.length === 0,
        errors,
        executions,
      };
    },
  };
}
