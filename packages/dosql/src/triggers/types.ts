/**
 * Trigger Types for DoSQL
 *
 * Core types for both JavaScript/TypeScript triggers and SQL triggers.
 * Supports:
 * - JavaScript trigger handlers
 * - SQL CREATE TRIGGER syntax (SQLite-compatible)
 * - BEFORE/AFTER/INSTEAD OF triggers
 * - INSERT/UPDATE/DELETE events
 * - UPDATE OF column-specific triggers
 * - WHEN clause conditions
 */

import type { DatabaseContext, DatabaseSchema, TableAccessor } from '../proc/types.js';

// =============================================================================
// SQL Trigger Types
// =============================================================================

/**
 * SQL trigger timing values
 */
export type SQLTriggerTiming = 'BEFORE' | 'AFTER' | 'INSTEAD OF';

/**
 * SQL trigger event values
 */
export type SQLTriggerEvent = 'INSERT' | 'UPDATE' | 'DELETE';

/**
 * Parsed SQL trigger definition from CREATE TRIGGER statement
 */
export interface ParsedSQLTrigger {
  /** Trigger name */
  name: string;

  /** Optional schema for the trigger */
  schema?: string;

  /** Table the trigger is attached to */
  table: string;

  /** Optional schema for the table */
  tableSchema?: string;

  /** When the trigger fires (BEFORE, AFTER, INSTEAD OF) */
  timing: SQLTriggerTiming;

  /** The event that fires the trigger */
  event: SQLTriggerEvent;

  /** Array of events for compatibility */
  events: SQLTriggerEvent[];

  /** Columns that trigger UPDATE OF (only for UPDATE events) */
  columns?: string[];

  /** WHEN clause condition */
  whenClause?: string;

  /** Trigger body (SQL statements) */
  body: string;

  /** Whether FOR EACH ROW is specified (always true for SQLite) */
  forEachRow: boolean;

  /** Whether IF NOT EXISTS was specified */
  ifNotExists: boolean;

  /** Whether this is a TEMP/TEMPORARY trigger */
  temporary: boolean;

  /** Whether the trigger references NEW values */
  referencesNew: boolean;

  /** Whether the trigger references OLD values */
  referencesOld: boolean;

  /** Number of statements in the trigger body */
  statementCount: number;

  /** Original SQL statement */
  rawSql: string;
}

/**
 * SQL trigger definition for storage/registration
 */
export interface SQLTriggerDefinition extends ParsedSQLTrigger {
  /** Unique identifier */
  id?: string;

  /** Whether the trigger is enabled */
  enabled: boolean;

  /** Priority for execution order (lower = earlier) */
  priority: number;

  /** Version number */
  version: number;

  /** Creation timestamp */
  createdAt: Date;

  /** Last update timestamp */
  updatedAt: Date;

  /** Optional description */
  description?: string;

  /** Optional tags */
  tags?: string[];
}

/**
 * DROP TRIGGER statement structure
 */
export interface DropTriggerStatement {
  /** Trigger name to drop */
  name: string;

  /** Optional schema */
  schema?: string;

  /** Whether IF EXISTS was specified */
  ifExists: boolean;
}

/**
 * Result of evaluating a WHEN clause
 */
export interface WhenClauseResult {
  /** Whether the condition evaluated to true */
  shouldFire: boolean;

  /** Any error that occurred during evaluation */
  error?: string;
}

/**
 * RAISE function types in trigger body
 */
export type RaiseType = 'IGNORE' | 'ROLLBACK' | 'ABORT' | 'FAIL';

/**
 * Result of a RAISE function call
 */
export interface RaiseResult {
  type: RaiseType;
  message?: string;
}

// =============================================================================
// Core Trigger Enums and Types
// =============================================================================

/**
 * When the trigger fires relative to the operation
 */
export type TriggerTiming = 'before' | 'after';

/**
 * The type of DML operation that fires the trigger
 */
export type TriggerEvent = 'insert' | 'update' | 'delete';

/**
 * Combined timing and event for specific trigger point
 */
export interface TriggerPoint {
  timing: TriggerTiming;
  event: TriggerEvent;
}

// =============================================================================
// Trigger Context Types
// =============================================================================

/**
 * Context provided to trigger handlers
 *
 * @typeParam T - The row type for the table
 */
export interface TriggerContext<T extends Record<string, unknown> = Record<string, unknown>> {
  /** Name of the table being modified */
  table: string;

  /** The type of operation (insert, update, delete) */
  event: TriggerEvent;

  /** When this trigger is firing (before or after) */
  timing: TriggerTiming;

  /**
   * Previous row value (for update/delete operations)
   * Undefined for insert operations
   */
  old?: T;

  /**
   * New row value (for insert/update operations)
   * Undefined for delete operations
   */
  new?: T;

  /** Database context for accessing other tables */
  db: DatabaseContext;

  /** Transaction ID if within a transaction */
  txnId?: string;

  /** Trigger execution metadata */
  meta: TriggerExecutionMeta;
}

/**
 * Metadata about the current trigger execution
 */
export interface TriggerExecutionMeta {
  /** Name of the trigger being executed */
  triggerName: string;

  /** Execution timestamp */
  timestamp: Date;

  /** Request ID for tracing */
  requestId: string;

  /** Depth of nested trigger calls (to prevent infinite recursion) */
  depth: number;
}

// =============================================================================
// Trigger Handler Types
// =============================================================================

/**
 * Trigger handler function signature
 *
 * For BEFORE triggers:
 * - Return void to proceed unchanged
 * - Return the modified row to change what gets written
 * - Throw an error to reject the operation
 *
 * For AFTER triggers:
 * - Return value is ignored (side effects only)
 * - Can throw errors which will be logged but won't rollback
 *
 * @typeParam T - The row type for the table
 */
export type TriggerHandler<T extends Record<string, unknown> = Record<string, unknown>> =
  (ctx: TriggerContext<T>) => void | T | Promise<void | T>;

/**
 * Synchronous trigger handler (for simple validation)
 */
export type SyncTriggerHandler<T extends Record<string, unknown> = Record<string, unknown>> =
  (ctx: TriggerContext<T>) => void | T;

/**
 * Async trigger handler (for database operations)
 */
export type AsyncTriggerHandler<T extends Record<string, unknown> = Record<string, unknown>> =
  (ctx: TriggerContext<T>) => Promise<void | T>;

// =============================================================================
// Trigger Definition Types
// =============================================================================

/**
 * A single trigger definition
 *
 * @typeParam T - The row type for the table
 */
export interface TriggerDefinition<T extends Record<string, unknown> = Record<string, unknown>> {
  /** Unique name for the trigger */
  name: string;

  /** Table this trigger applies to */
  table: string;

  /** When to fire (before or after the operation) */
  timing: TriggerTiming;

  /** Which operations trigger this (can be multiple) */
  events: TriggerEvent[];

  /** The handler function */
  handler: TriggerHandler<T>;

  /** Priority for ordering (lower runs first, default: 100) */
  priority?: number;

  /** Whether this trigger is currently enabled */
  enabled?: boolean;

  /** Optional description */
  description?: string;

  /** Optional condition (SQL WHERE clause or predicate) */
  condition?: string | ((ctx: TriggerContext<T>) => boolean);
}

/**
 * Full trigger configuration including metadata
 */
export interface TriggerConfig<T extends Record<string, unknown> = Record<string, unknown>>
  extends TriggerDefinition<T> {
  /** Version number (auto-incremented on update) */
  version: number;

  /** Creation timestamp */
  createdAt: Date;

  /** Last update timestamp */
  updatedAt: Date;

  /** Author/creator */
  author?: string;

  /** Tags for categorization */
  tags?: string[];
}

// =============================================================================
// Trigger Registry Types
// =============================================================================

/**
 * Options for registering a trigger
 */
export interface RegisterTriggerOptions {
  /** Replace existing trigger with same name (default: false) */
  replace?: boolean;

  /** Author name */
  author?: string;

  /** Tags for categorization */
  tags?: string[];
}

/**
 * Options for listing triggers
 */
export interface ListTriggersOptions {
  /** Filter by table name */
  table?: string;

  /** Filter by timing */
  timing?: TriggerTiming | SQLTriggerTiming;

  /** Filter by event */
  event?: TriggerEvent | SQLTriggerEvent;

  /** Filter by enabled status */
  enabled?: boolean;

  /** Filter by tag */
  tag?: string;
}

/**
 * Trigger registry interface
 */
export interface TriggerRegistry {
  /**
   * Register a new trigger
   */
  register<T extends Record<string, unknown>>(
    trigger: TriggerDefinition<T>,
    options?: RegisterTriggerOptions
  ): TriggerConfig<T>;

  /**
   * Get a trigger by name
   */
  get(name: string): TriggerConfig | undefined;

  /**
   * List triggers matching criteria
   */
  list(options?: ListTriggersOptions): TriggerConfig[];

  /**
   * Get triggers for a specific table and event
   * Returns triggers sorted by priority
   */
  getForTableEvent(
    table: string,
    timing: TriggerTiming,
    event: TriggerEvent
  ): TriggerConfig[];

  /**
   * Enable a trigger by name
   */
  enable(name: string): boolean;

  /**
   * Disable a trigger by name
   */
  disable(name: string): boolean;

  /**
   * Remove a trigger by name
   */
  remove(name: string): boolean;

  /**
   * Clear all triggers (optionally for a specific table)
   */
  clear(table?: string): void;

  /**
   * Update trigger priority
   */
  setPriority(name: string, priority: number): boolean;
}

// =============================================================================
// Trigger Executor Types
// =============================================================================

/**
 * Result of executing BEFORE triggers
 */
export interface BeforeTriggerResult<T extends Record<string, unknown>> {
  /** Whether the operation should proceed */
  proceed: boolean;

  /** The (potentially modified) row data */
  row?: T;

  /** Error if trigger rejected the operation */
  error?: Error;

  /** Execution details for each trigger */
  executions: TriggerExecution[];
}

/**
 * Result of executing AFTER triggers
 */
export interface AfterTriggerResult {
  /** Whether all triggers completed successfully */
  success: boolean;

  /** Errors from trigger execution (non-fatal) */
  errors: Array<{ triggerName: string; error: Error }>;

  /** Execution details for each trigger */
  executions: TriggerExecution[];
}

/**
 * Details of a single trigger execution
 */
export interface TriggerExecution {
  /** Name of the trigger */
  triggerName: string;

  /** Timing (before/after) */
  timing: TriggerTiming;

  /** Event type */
  event: TriggerEvent;

  /** Whether execution succeeded */
  success: boolean;

  /** Duration in milliseconds */
  duration: number;

  /** Error if failed */
  error?: string;

  /** Whether the row was modified (BEFORE only) */
  modified?: boolean;
}

/**
 * Options for trigger execution
 */
export interface TriggerExecutionOptions {
  /** Transaction ID */
  txnId?: string;

  /** Request ID for tracing */
  requestId?: string;

  /** Maximum trigger depth (default: 10) */
  maxDepth?: number;

  /** Current depth (for internal use) */
  currentDepth?: number;

  /** Timeout per trigger in ms (default: 5000) */
  timeout?: number;

  /** Skip disabled triggers (default: true) */
  skipDisabled?: boolean;
}

/**
 * Trigger executor interface
 */
export interface TriggerExecutor {
  /**
   * Execute BEFORE triggers for an operation
   * Returns modified row or rejects
   */
  executeBefore<T extends Record<string, unknown>>(
    table: string,
    event: TriggerEvent,
    oldRow: T | undefined,
    newRow: T | undefined,
    options?: TriggerExecutionOptions
  ): Promise<BeforeTriggerResult<T>>;

  /**
   * Execute AFTER triggers for an operation
   * Errors are logged but don't prevent the operation
   */
  executeAfter<T extends Record<string, unknown>>(
    table: string,
    event: TriggerEvent,
    oldRow: T | undefined,
    newRow: T | undefined,
    options?: TriggerExecutionOptions
  ): Promise<AfterTriggerResult>;

  /**
   * Execute both BEFORE and AFTER triggers for an operation
   */
  executeAll<T extends Record<string, unknown>>(
    table: string,
    event: TriggerEvent,
    oldRow: T | undefined,
    newRow: T | undefined,
    operation: (row: T | undefined) => Promise<T | undefined>,
    options?: TriggerExecutionOptions
  ): Promise<{
    before: BeforeTriggerResult<T>;
    result: T | undefined;
    after: AfterTriggerResult;
  }>;
}

// =============================================================================
// WAL Integration Types
// =============================================================================

/**
 * WAL entry with trigger metadata
 */
export interface WALEntryWithTriggers {
  /** Original WAL entry fields */
  lsn: bigint;
  timestamp: number;
  txnId: string;
  op: 'INSERT' | 'UPDATE' | 'DELETE';
  table: string;
  key?: Uint8Array;
  before?: Uint8Array;
  after?: Uint8Array;

  /** Triggers that were executed */
  triggersExecuted?: Array<{
    name: string;
    timing: TriggerTiming;
    success: boolean;
    duration: number;
  }>;
}

/**
 * Trigger-aware WAL writer
 */
export interface TriggerAwareWALWriter {
  /**
   * Append with trigger execution
   * - BEFORE triggers fire before WAL write
   * - AFTER triggers fire after WAL commit
   */
  appendWithTriggers<T extends Record<string, unknown>>(
    table: string,
    op: 'INSERT' | 'UPDATE' | 'DELETE',
    oldRow: T | undefined,
    newRow: T | undefined,
    options?: TriggerExecutionOptions
  ): Promise<{
    lsn: bigint;
    before: BeforeTriggerResult<T>;
    after: AfterTriggerResult;
  }>;
}

// =============================================================================
// Error Types
// =============================================================================

/**
 * Trigger-specific error codes
 */
export enum TriggerErrorCode {
  /** Trigger with this name already exists */
  DUPLICATE_TRIGGER = 'TRIGGER_DUPLICATE',
  /** Trigger not found */
  NOT_FOUND = 'TRIGGER_NOT_FOUND',
  /** Trigger handler threw an error */
  HANDLER_ERROR = 'TRIGGER_HANDLER_ERROR',
  /** Trigger execution timed out */
  TIMEOUT = 'TRIGGER_TIMEOUT',
  /** Maximum trigger depth exceeded */
  MAX_DEPTH_EXCEEDED = 'TRIGGER_MAX_DEPTH',
  /** Invalid trigger definition */
  INVALID_DEFINITION = 'TRIGGER_INVALID_DEFINITION',
  /** Trigger rejected the operation */
  OPERATION_REJECTED = 'TRIGGER_OPERATION_REJECTED',
}

/**
 * Custom error class for trigger operations
 */
export class TriggerError extends Error {
  constructor(
    public readonly code: TriggerErrorCode,
    message: string,
    public readonly triggerName?: string,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = 'TriggerError';
  }
}

// =============================================================================
// Fluent Definition Types
// =============================================================================

/**
 * Input type for defineTriggers function
 */
export type TriggerDefinitions<Tables extends Record<string, Record<string, unknown>>> = {
  [K: string]: TriggerDefinition<Tables[keyof Tables]>;
};

/**
 * Result type for defineTriggers function
 */
export interface DefinedTriggers<T extends TriggerDefinitions<any>> {
  /** All trigger definitions */
  definitions: T;

  /** Register all triggers with a registry */
  registerAll(registry: TriggerRegistry, options?: RegisterTriggerOptions): void;

  /** Get trigger by name */
  get<K extends keyof T>(name: K): T[K];
}
