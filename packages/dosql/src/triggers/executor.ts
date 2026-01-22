/**
 * Trigger Executor
 *
 * Executes both JavaScript and SQL triggers with support for:
 * - BEFORE triggers that can modify/reject operations
 * - AFTER triggers for side effects
 * - INSTEAD OF triggers for views
 * - Sandboxed execution via ai-evaluate
 * - Transaction awareness
 * - Recursion depth limits
 * - SQL trigger body execution
 * - WHEN clause evaluation
 * - UPDATE OF column-specific triggers
 */

import { evaluate } from 'ai-evaluate';
import type { EvaluateOptions } from 'ai-evaluate';
import type {
  TriggerRegistry,
  TriggerConfig,
  TriggerContext,
  TriggerEvent,
  TriggerTiming,
  TriggerExecutionMeta,
  TriggerExecutor,
  TriggerExecutionOptions,
  BeforeTriggerResult,
  AfterTriggerResult,
  TriggerExecution,
  SQLTriggerDefinition,
  SQLTriggerTiming,
  SQLTriggerEvent,
  RaiseType,
  RaiseResult,
  ParsedSQLTrigger,
} from './types.js';
import { TriggerError, TriggerErrorCode } from './types.js';
import type { DatabaseContext, DatabaseSchema } from '../proc/types.js';

// =============================================================================
// Executor Options
// =============================================================================

/**
 * Options for creating a trigger executor
 */
export interface TriggerExecutorOptions<DB extends DatabaseSchema = DatabaseSchema> {
  /** Trigger registry */
  registry: TriggerRegistry;

  /** Database context for trigger handlers */
  db: DatabaseContext<DB>;

  /** Default timeout per trigger in ms (default: 5000) */
  defaultTimeout?: number;

  /** Maximum recursion depth (default: 10) */
  maxDepth?: number;

  /** Whether to run triggers in sandbox (default: false for performance) */
  sandboxed?: boolean;

  /** Base environment variables for sandboxed execution */
  baseEnv?: Record<string, string>;

  /** Error handler for AFTER trigger errors */
  onAfterError?: (triggerName: string, error: Error) => void;
}

// =============================================================================
// Context Builder
// =============================================================================

/**
 * Build the trigger context object
 */
function buildTriggerContext<T extends Record<string, unknown>>(
  table: string,
  timing: TriggerTiming,
  event: TriggerEvent,
  oldRow: T | undefined,
  newRow: T | undefined,
  db: DatabaseContext,
  meta: TriggerExecutionMeta,
  txnId?: string
): TriggerContext<T> {
  return {
    table,
    event,
    timing,
    old: oldRow,
    new: newRow,
    db,
    txnId,
    meta,
  };
}

/**
 * Build execution metadata
 */
function buildMeta(
  triggerName: string,
  requestId: string,
  depth: number
): TriggerExecutionMeta {
  return {
    triggerName,
    timestamp: new Date(),
    requestId,
    depth,
  };
}

// =============================================================================
// Direct Execution (Non-Sandboxed)
// =============================================================================

/**
 * Execute a trigger handler directly (not sandboxed)
 */
async function executeDirectly<T extends Record<string, unknown>>(
  trigger: TriggerConfig,
  ctx: TriggerContext<T>,
  timeout: number
): Promise<{ result: void | T; modified: boolean }> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      reject(new TriggerError(
        TriggerErrorCode.TIMEOUT,
        `Trigger '${trigger.name}' timed out after ${timeout}ms`,
        trigger.name
      ));
    }, timeout);

    Promise.resolve(trigger.handler(ctx as TriggerContext))
      .then((result) => {
        clearTimeout(timer);
        const modified = result !== undefined && result !== ctx.new;
        resolve({ result: result as void | T, modified });
      })
      .catch((error) => {
        clearTimeout(timer);
        reject(error);
      });
  });
}

// =============================================================================
// Sandboxed Execution
// =============================================================================

/**
 * Build module wrapper for sandboxed execution
 */
function buildSandboxModule(
  handlerCode: string,
  tableNames: string[]
): string {
  const tableAccessors = tableNames.map(name => `
    ${name}: {
      async get(key) { return __dbCall('${name}', 'get', [key]); },
      async where(predicate, options) {
        if (typeof predicate === 'function') {
          const all = await __dbCall('${name}', 'all', []);
          return all.filter(predicate);
        }
        return __dbCall('${name}', 'where', [predicate, options]);
      },
      async all(options) { return __dbCall('${name}', 'all', [options]); },
      async count(predicate) {
        if (typeof predicate === 'function') {
          const all = await __dbCall('${name}', 'all', []);
          return all.filter(predicate).length;
        }
        return __dbCall('${name}', 'count', [predicate]);
      },
      async insert(record) { return __dbCall('${name}', 'insert', [record]); },
      async update(predicate, changes) {
        if (typeof predicate === 'function') {
          throw new Error('Function predicates not supported for update');
        }
        return __dbCall('${name}', 'update', [predicate, changes]);
      },
      async delete(predicate) {
        if (typeof predicate === 'function') {
          throw new Error('Function predicates not supported for delete');
        }
        return __dbCall('${name}', 'delete', [predicate]);
      },
    }`).join(',\n');

  return `
async function __dbCall(table, method, args) {
  const response = await fetch('db://internal/call', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ table, method, args }),
  });
  if (!response.ok) {
    const error = await response.text();
    throw new Error(\`Database error: \${error}\`);
  }
  return response.json();
}

const db = {
  tables: { ${tableAccessors} },
};

${tableNames.map(name => `db.${name} = db.tables.${name};`).join('\n')}

const handler = ${handlerCode};

export default handler;
`;
}

/**
 * Execute a trigger handler in a sandbox
 */
async function executeSandboxed<T extends Record<string, unknown>>(
  trigger: TriggerConfig,
  ctx: TriggerContext<T>,
  db: DatabaseContext,
  timeout: number,
  env: Record<string, string>
): Promise<{ result: void | T; modified: boolean }> {
  const tableNames = Object.keys(db.tables);
  const handlerStr = trigger.handler.toString();
  const moduleCode = buildSandboxModule(handlerStr, tableNames);

  const script = `
    const handler = exports.default;
    const ctx = ${JSON.stringify({
      table: ctx.table,
      event: ctx.event,
      timing: ctx.timing,
      old: ctx.old,
      new: ctx.new,
      txnId: ctx.txnId,
      meta: ctx.meta,
    })};
    ctx.db = db;
    return handler(ctx);
  `;

  // Create outbound RPC handler for database operations
  const outboundRpc = (url: string, request: Request): Response | Promise<Response> | null => {
    if (!url.startsWith('db://internal/')) {
      return null;
    }

    const path = url.replace('db://internal/', '');

    return (async (): Promise<Response> => {
      try {
        if (path === 'call') {
          const body = await request.json() as { table: string; method: string; args: unknown[] };
          const { table, method, args } = body;

          const tableAccessor = db.tables[table as keyof typeof db.tables];
          if (!tableAccessor) {
            return new Response(`Table '${table}' not found`, { status: 404 });
          }

          const fn = (tableAccessor as unknown as Record<string, (...args: unknown[]) => Promise<unknown>>)[method];
          if (typeof fn !== 'function') {
            return new Response(`Method '${method}' not found`, { status: 404 });
          }

          const result = await fn.apply(tableAccessor, args);
          return new Response(JSON.stringify(result), {
            headers: { 'Content-Type': 'application/json' },
          });
        }

        return new Response(`Unknown path: ${path}`, { status: 404 });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return new Response(message, { status: 500 });
      }
    })();
  };

  const evalOptions: EvaluateOptions = {
    module: moduleCode,
    script,
    timeout,
    env,
    outboundRpc,
  };

  const result = await evaluate(evalOptions);

  if (!result.success) {
    throw new TriggerError(
      TriggerErrorCode.HANDLER_ERROR,
      result.error ?? 'Unknown sandbox error',
      trigger.name
    );
  }

  const modified = result.value !== undefined && result.value !== ctx.new;
  return { result: result.value as void | T, modified };
}

// =============================================================================
// Executor Implementation
// =============================================================================

/**
 * Create a trigger executor
 */
export function createTriggerExecutor<DB extends DatabaseSchema = DatabaseSchema>(
  options: TriggerExecutorOptions<DB>
): TriggerExecutor {
  const {
    registry,
    db,
    defaultTimeout = 5000,
    maxDepth = 10,
    sandboxed = false,
    baseEnv = {},
    onAfterError,
  } = options;

  /**
   * Check if condition passes
   */
  function checkCondition<T extends Record<string, unknown>>(
    trigger: TriggerConfig,
    ctx: TriggerContext<T>
  ): boolean {
    if (!trigger.condition) {
      return true;
    }

    if (typeof trigger.condition === 'function') {
      try {
        return trigger.condition(ctx as TriggerContext);
      } catch {
        return false;
      }
    }

    // SQL WHERE condition would need SQL parser - for now just return true
    // This is a simplified implementation
    return true;
  }

  /**
   * Execute a single trigger
   */
  async function executeTrigger<T extends Record<string, unknown>>(
    trigger: TriggerConfig,
    ctx: TriggerContext<T>,
    timeout: number
  ): Promise<{ result: void | T; modified: boolean; execution: TriggerExecution }> {
    const startTime = Date.now();

    try {
      let execResult: { result: void | T; modified: boolean };

      if (sandboxed) {
        execResult = await executeSandboxed(
          trigger,
          ctx,
          db as DatabaseContext,
          timeout,
          baseEnv
        );
      } else {
        execResult = await executeDirectly(trigger, ctx, timeout);
      }

      const duration = Date.now() - startTime;

      return {
        result: execResult.result,
        modified: execResult.modified,
        execution: {
          triggerName: trigger.name,
          timing: trigger.timing,
          event: ctx.event,
          success: true,
          duration,
          modified: execResult.modified,
        },
      };
    } catch (error) {
      const duration = Date.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : String(error);

      return {
        result: undefined as void | T,
        modified: false,
        execution: {
          triggerName: trigger.name,
          timing: trigger.timing,
          event: ctx.event,
          success: false,
          duration,
          error: errorMessage,
        },
      };
    }
  }

  return {
    async executeBefore<T extends Record<string, unknown>>(
      table: string,
      event: TriggerEvent,
      oldRow: T | undefined,
      newRow: T | undefined,
      execOptions: TriggerExecutionOptions = {}
    ): Promise<BeforeTriggerResult<T>> {
      const {
        txnId,
        requestId = `req_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`,
        currentDepth = 0,
        timeout = defaultTimeout,
        skipDisabled = true,
      } = execOptions;

      // Check recursion depth
      if (currentDepth >= (execOptions.maxDepth ?? maxDepth)) {
        return {
          proceed: false,
          error: new TriggerError(
            TriggerErrorCode.MAX_DEPTH_EXCEEDED,
            `Maximum trigger depth (${maxDepth}) exceeded`
          ),
          executions: [],
        };
      }

      // Get triggers for this table/event
      let triggers = registry.getForTableEvent(table, 'before', event);

      // Filter disabled if requested
      if (skipDisabled) {
        triggers = triggers.filter(t => t.enabled !== false);
      }

      if (triggers.length === 0) {
        return {
          proceed: true,
          row: newRow,
          executions: [],
        };
      }

      const executions: TriggerExecution[] = [];
      let currentRow = newRow;

      // Execute triggers in order
      for (const trigger of triggers) {
        const meta = buildMeta(trigger.name, requestId, currentDepth);
        const ctx = buildTriggerContext(
          table,
          'before',
          event,
          oldRow,
          currentRow,
          db as DatabaseContext,
          meta,
          txnId
        );

        // Check condition
        if (!checkCondition(trigger, ctx)) {
          continue;
        }

        const { result, modified, execution } = await executeTrigger(
          trigger,
          ctx,
          timeout
        );

        executions.push(execution);

        if (!execution.success) {
          // BEFORE trigger failure rejects the operation
          return {
            proceed: false,
            error: new TriggerError(
              TriggerErrorCode.OPERATION_REJECTED,
              `Trigger '${trigger.name}' rejected the operation: ${execution.error}`,
              trigger.name
            ),
            executions,
          };
        }

        // Update current row if modified
        if (modified && result !== undefined) {
          currentRow = result as T;
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
      event: TriggerEvent,
      oldRow: T | undefined,
      newRow: T | undefined,
      execOptions: TriggerExecutionOptions = {}
    ): Promise<AfterTriggerResult> {
      const {
        txnId,
        requestId = `req_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`,
        currentDepth = 0,
        timeout = defaultTimeout,
        skipDisabled = true,
      } = execOptions;

      // Check recursion depth
      if (currentDepth >= (execOptions.maxDepth ?? maxDepth)) {
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

      // Get triggers for this table/event
      let triggers = registry.getForTableEvent(table, 'after', event);

      // Filter disabled if requested
      if (skipDisabled) {
        triggers = triggers.filter(t => t.enabled !== false);
      }

      if (triggers.length === 0) {
        return {
          success: true,
          errors: [],
          executions: [],
        };
      }

      const executions: TriggerExecution[] = [];
      const errors: Array<{ triggerName: string; error: Error }> = [];

      // Execute triggers in order
      for (const trigger of triggers) {
        const meta = buildMeta(trigger.name, requestId, currentDepth);
        const ctx = buildTriggerContext(
          table,
          'after',
          event,
          oldRow,
          newRow,
          db as DatabaseContext,
          meta,
          txnId
        );

        // Check condition
        if (!checkCondition(trigger, ctx)) {
          continue;
        }

        const { execution } = await executeTrigger(trigger, ctx, timeout);
        executions.push(execution);

        if (!execution.success) {
          const error = new Error(execution.error ?? 'Unknown error');
          errors.push({ triggerName: trigger.name, error });

          // Call error handler if provided
          if (onAfterError) {
            onAfterError(trigger.name, error);
          }
        }
      }

      return {
        success: errors.length === 0,
        errors,
        executions,
      };
    },

    async executeAll<T extends Record<string, unknown>>(
      table: string,
      event: TriggerEvent,
      oldRow: T | undefined,
      newRow: T | undefined,
      operation: (row: T | undefined) => Promise<T | undefined>,
      execOptions: TriggerExecutionOptions = {}
    ): Promise<{
      before: BeforeTriggerResult<T>;
      result: T | undefined;
      after: AfterTriggerResult;
    }> {
      // Execute BEFORE triggers
      const before = await this.executeBefore(table, event, oldRow, newRow, execOptions);

      if (!before.proceed) {
        return {
          before,
          result: undefined,
          after: { success: true, errors: [], executions: [] },
        };
      }

      // Execute the actual operation with potentially modified row
      const result = await operation(before.row);

      // Execute AFTER triggers with the operation result
      const after = await this.executeAfter(
        table,
        event,
        oldRow,
        event === 'delete' ? oldRow : result,
        execOptions
      );

      return { before, result, after };
    },
  };
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
): SQLTriggerExecutor {
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

// =============================================================================
// SQL Trigger Execution
// =============================================================================

/**
 * Parse all RAISE functions from SQL trigger body
 */
function parseRaiseFunctions(body: string): RaiseResult[] {
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
function parseRaiseFunction(body: string): RaiseResult | null {
  const results = parseRaiseFunctions(body);
  return results.length > 0 ? results[0] : null;
}

/**
 * Evaluate a simple SQL condition with CASE WHEN for RAISE
 * Returns the RAISE result if condition matches, null otherwise
 */
function evaluateRaiseCondition<T extends Record<string, unknown>>(
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

/**
 * Evaluate a WHEN clause condition
 */
function evaluateWhenClause<T extends Record<string, unknown>>(
  whenClause: string,
  oldRow: T | undefined,
  newRow: T | undefined
): boolean {
  if (!whenClause) return true;

  try {
    // Create a simple expression evaluator for WHEN clauses
    // Replace NEW.col and OLD.col with actual values
    let expr = whenClause;

    // Replace NEW references
    if (newRow) {
      for (const [key, value] of Object.entries(newRow)) {
        const pattern = new RegExp(`\\bNEW\\.${key}\\b`, 'gi');
        const replacement = typeof value === 'string' ? `'${value}'` : String(value);
        expr = expr.replace(pattern, replacement);
      }
    }

    // Replace OLD references
    if (oldRow) {
      for (const [key, value] of Object.entries(oldRow)) {
        const pattern = new RegExp(`\\bOLD\\.${key}\\b`, 'gi');
        const replacement = typeof value === 'string' ? `'${value}'` : String(value);
        expr = expr.replace(pattern, replacement);
      }
    }

    // Handle NOT LIKE pattern (convert to regex test)
    // e.g., 'invalid' NOT LIKE '%@%' => !/.*@.*/.test('invalid')
    expr = expr.replace(
      /(['"]\w+['"]|\w+)\s+NOT\s+LIKE\s+['"](%?)([^%'"]+)(%?)['"]/gi,
      (_, val, prefix, pattern, suffix) => {
        const regexPattern = (prefix ? '.*' : '') + pattern.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + (suffix ? '.*' : '');
        return `!/${regexPattern}/.test(${val})`;
      }
    );

    // Handle LIKE pattern (convert to regex test)
    // e.g., 'test@example.com' LIKE '%@%' => /.*@.*/.test('test@example.com')
    expr = expr.replace(
      /(['"]\w+['"]|\w+)\s+LIKE\s+['"](%?)([^%'"]+)(%?)['"]/gi,
      (_, val, prefix, pattern, suffix) => {
        const regexPattern = (prefix ? '.*' : '') + pattern.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + (suffix ? '.*' : '');
        return `/${regexPattern}/.test(${val})`;
      }
    );

    // Handle IS NULL
    expr = expr.replace(/\bIS\s+NULL\b/gi, '=== null');
    expr = expr.replace(/\bIS\s+NOT\s+NULL\b/gi, '!== null');

    // Handle AND/OR
    expr = expr.replace(/\bAND\b/gi, '&&');
    expr = expr.replace(/\bOR\b/gi, '||');

    // Handle NOT (but not NOT IN or NOT LIKE which are handled above)
    expr = expr.replace(/\bNOT\s+(?!IN\b)/gi, '!');

    // Handle SQL equality operators
    expr = expr.replace(/!=/g, '!==');
    expr = expr.replace(/(?<![!<>=])=(?!=)/g, '===');
    expr = expr.replace(/<>/g, '!==');

    // Evaluate the expression (simplified - production would need proper parser)
    // eslint-disable-next-line no-new-func
    const result = new Function(`return ${expr}`)();
    return Boolean(result);
  } catch {
    // If evaluation fails, default to true (fire the trigger)
    return true;
  }
}

/**
 * Check if specific columns changed (for UPDATE OF triggers)
 */
function didColumnsChange<T extends Record<string, unknown>>(
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
  extends TriggerExecutorOptions<DB> {
  /** SQL executor function for trigger body */
  sqlExecutor?: (sql: string, params: Record<string, unknown>) => Promise<unknown[]>;
}

/**
 * Create a SQL trigger executor
 */
export function createSQLTriggerExecutor<DB extends DatabaseSchema = DatabaseSchema>(
  options: SQLTriggerExecutorOptions<DB>
): SQLTriggerExecutor {
  const baseExecutor = createTriggerExecutor(options);
  const { sqlExecutor } = options;

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
      let triggers = options.registry.getForTableEvent(table, 'before', normalizedEvent);

      // Also get SQL triggers (BEFORE timing)
      const sqlTriggers = options.registry.list({
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

      // Execute SQL triggers
      for (const sqlTrigger of sqlTriggers as (SQLTriggerDefinition | ParsedSQLTrigger)[]) {
        if (!shouldFireSQLTrigger(sqlTrigger, event.toUpperCase() as SQLTriggerEvent, oldRow, currentRow)) {
          continue;
        }

        const startTime = Date.now();
        const { success, error, raiseResult } = await executeSQLBody(
          sqlTrigger,
          oldRow,
          currentRow
        );
        const duration = Date.now() - startTime;

        executions.push({
          triggerName: sqlTrigger.name,
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
              sqlTrigger.name
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
      let triggers = options.registry.getForTableEvent(table, 'after', normalizedEvent);

      // Get SQL triggers (AFTER timing)
      const sqlTriggers = options.registry.list({
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

      // Execute SQL triggers
      for (const sqlTrigger of sqlTriggers as (SQLTriggerDefinition | ParsedSQLTrigger)[]) {
        if (!shouldFireSQLTrigger(sqlTrigger, event.toUpperCase() as SQLTriggerEvent, oldRow, newRow)) {
          continue;
        }

        const startTime = Date.now();
        const { success, error } = await executeSQLBody(sqlTrigger, oldRow, newRow);
        const duration = Date.now() - startTime;

        executions.push({
          triggerName: sqlTrigger.name,
          timing: 'after',
          event: normalizedEvent,
          success,
          duration,
          error,
        });

        if (!success) {
          errors.push({
            triggerName: sqlTrigger.name,
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
