/**
 * JavaScript Trigger Executor
 *
 * Executes JavaScript/TypeScript triggers with support for:
 * - BEFORE triggers that can modify/reject operations
 * - AFTER triggers for side effects
 * - Sandboxed execution via ai-evaluate
 * - Transaction awareness
 * - Recursion depth limits
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
} from './types.js';
import { TriggerError, TriggerErrorCode } from './types.js';
import type { DatabaseContext, DatabaseSchema } from '../proc/types.js';
import {
  buildSandboxModule,
  createOutboundRpcHandler,
  type SandboxDatabaseContext,
} from '../utils/sandbox.js';

// =============================================================================
// Executor Options
// =============================================================================

/**
 * Options for creating a JavaScript trigger executor
 */
export interface JSTriggerExecutorOptions<DB extends DatabaseSchema = DatabaseSchema> {
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
export function buildTriggerContext<T extends Record<string, unknown>>(
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
export function buildMeta(
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
export async function executeDirectly<T extends Record<string, unknown>>(
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

// Re-export buildSandboxModule for backward compatibility
export { buildSandboxModule } from '../utils/sandbox.js';

/**
 * Execute a trigger handler in a sandbox
 */
export async function executeSandboxed<T extends Record<string, unknown>>(
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

  // Use shared outbound RPC handler
  const outboundRpc = createOutboundRpcHandler(db as unknown as SandboxDatabaseContext);

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
// JavaScript Trigger Executor Implementation
// =============================================================================

/**
 * Create a JavaScript trigger executor
 */
export function createJSTriggerExecutor<DB extends DatabaseSchema = DatabaseSchema>(
  options: JSTriggerExecutorOptions<DB>
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
