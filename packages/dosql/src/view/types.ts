/**
 * View Types for DoSQL
 *
 * Core types for SQL VIEW support including:
 * - CREATE VIEW / CREATE OR REPLACE VIEW
 * - CREATE TEMP VIEW
 * - DROP VIEW
 * - Updatable views with INSTEAD OF triggers
 * - WITH CHECK OPTION
 */

import {
  DoSQLError,
  ErrorCategory,
  registerErrorClass,
  type SerializedError,
} from '../errors/base.js';

// =============================================================================
// View Definition Types
// =============================================================================

/**
 * Type of view
 */
export type ViewType = 'standard' | 'temporary' | 'materialized';

/**
 * Check option for updatable views
 */
export type CheckOption = 'CASCADED' | 'LOCAL' | undefined;

/**
 * Column definition in a view
 */
export interface ViewColumn {
  /** Column name (alias or original) */
  name: string;

  /** Original expression or column reference */
  expression: string;

  /** Inferred or declared type */
  type?: string | undefined;

  /** Whether this column is nullable */
  nullable?: boolean | undefined;
}

/**
 * Parsed view definition
 */
export interface ParsedView {
  /** View name */
  name: string;

  /** Optional schema name */
  schema?: string | undefined;

  /** Column aliases (if specified) */
  columns?: string[] | undefined;

  /** The SELECT query that defines the view */
  selectQuery: string;

  /** Parsed column definitions */
  columnDefinitions?: ViewColumn[] | undefined;

  /** Whether CREATE OR REPLACE was used */
  orReplace: boolean;

  /** Whether IF NOT EXISTS was used */
  ifNotExists: boolean;

  /** Whether this is a temporary view */
  temporary: boolean;

  /** WITH CHECK OPTION clause */
  checkOption?: CheckOption | undefined;

  /** Original SQL statement */
  rawSql: string;
}

/**
 * Full view definition stored in registry
 */
export interface ViewDefinition extends ParsedView {
  /** Unique identifier */
  id?: string | undefined;

  /** View type */
  type: ViewType;

  /** Whether the view is updatable */
  updatable: boolean;

  /** Tables referenced by the view */
  referencedTables: string[];

  /** Views referenced by the view (for dependency tracking) */
  referencedViews: string[];

  /** Version number */
  version: number;

  /** Creation timestamp */
  createdAt: Date;

  /** Last update timestamp */
  updatedAt: Date;

  /** Whether the view is enabled */
  enabled: boolean;

  /** Optional description */
  description?: string | undefined;

  /** Optional tags */
  tags?: string[] | undefined;
}

// =============================================================================
// DROP VIEW Types
// =============================================================================

/**
 * Parsed DROP VIEW statement
 */
export interface DropViewStatement {
  /** View name to drop */
  name: string;

  /** Optional schema */
  schema?: string | undefined;

  /** Whether IF EXISTS was used */
  ifExists: boolean;

  /** CASCADE or RESTRICT behavior */
  cascade?: boolean | undefined;
}

// =============================================================================
// View Query Types
// =============================================================================

/**
 * Context for executing a view query
 */
export interface ViewQueryContext {
  /** The view being queried */
  view: ViewDefinition;

  /** Parameters for the query */
  parameters?: Record<string, unknown> | undefined;

  /** Request ID for tracing */
  requestId?: string | undefined;

  /** Transaction ID if within a transaction */
  txnId?: string | undefined;
}

/**
 * Result of a view query
 */
export interface ViewQueryResult<T = Record<string, unknown>> {
  /** Rows returned */
  rows: T[];

  /** Column metadata */
  columns: ViewColumn[];

  /** Number of rows */
  rowCount: number;

  /** Execution time in ms */
  executionTime: number;
}

// =============================================================================
// Updatable View Types
// =============================================================================

/**
 * DML operation type for views
 */
export type ViewDMLOperation = 'INSERT' | 'UPDATE' | 'DELETE';

/**
 * Context for view DML operations
 */
export interface ViewDMLContext<T = Record<string, unknown>> {
  /** The view being modified */
  view: ViewDefinition;

  /** The operation type */
  operation: ViewDMLOperation;

  /** Old row (for UPDATE/DELETE) */
  old?: T | undefined;

  /** New row (for INSERT/UPDATE) */
  new?: T | undefined;

  /** Request ID for tracing */
  requestId?: string | undefined;

  /** Transaction ID */
  txnId?: string | undefined;
}

/**
 * Result of a view DML operation
 */
export interface ViewDMLResult {
  /** Whether the operation succeeded */
  success: boolean;

  /** Number of rows affected */
  rowsAffected: number;

  /** Error message if failed */
  error?: string | undefined;
}

/**
 * INSTEAD OF trigger handler for updatable views
 */
export type InsteadOfHandler<T = Record<string, unknown>> = (
  ctx: ViewDMLContext<T>
) => Promise<ViewDMLResult>;

/**
 * INSTEAD OF trigger definition
 */
export interface InsteadOfTrigger<T = Record<string, unknown>> {
  /** Trigger name */
  name: string;

  /** View name this trigger applies to */
  viewName: string;

  /** Operations this trigger handles */
  operations: ViewDMLOperation[];

  /** Handler function */
  handler: InsteadOfHandler<T>;

  /** Whether the trigger is enabled */
  enabled: boolean;

  /** Priority (lower runs first) */
  priority: number;
}

// =============================================================================
// View Registry Types
// =============================================================================

/**
 * Options for registering a view
 */
export interface RegisterViewOptions {
  /** Replace existing view with same name */
  replace?: boolean | undefined;

  /** Description */
  description?: string | undefined;

  /** Tags */
  tags?: string[] | undefined;
}

/**
 * Options for listing views
 */
export interface ListViewsOptions {
  /** Filter by schema */
  schema?: string | undefined;

  /** Filter by type */
  type?: ViewType | undefined;

  /** Filter by referenced table */
  referencedTable?: string | undefined;

  /** Filter by enabled status */
  enabled?: boolean | undefined;

  /** Filter by tag */
  tag?: string | undefined;

  /** Include temporary views */
  includeTemporary?: boolean | undefined;
}

/**
 * View registry interface
 */
export interface ViewRegistry {
  /**
   * Register a new view
   */
  register(view: ParsedView, options?: RegisterViewOptions): ViewDefinition;

  /**
   * Get a view by name
   */
  get(name: string, schema?: string): ViewDefinition | undefined;

  /**
   * Check if a view exists
   */
  exists(name: string, schema?: string): boolean;

  /**
   * List views matching criteria
   */
  list(options?: ListViewsOptions): ViewDefinition[];

  /**
   * Drop a view
   */
  drop(name: string, options?: { ifExists?: boolean; cascade?: boolean; schema?: string }): boolean;

  /**
   * Get views that depend on a given view or table
   */
  getDependents(name: string): ViewDefinition[];

  /**
   * Get views that a given view depends on
   */
  getDependencies(name: string): ViewDefinition[];

  /**
   * Clear all views (optionally only temporary)
   */
  clear(options?: { temporaryOnly?: boolean }): void;

  /**
   * Register an INSTEAD OF trigger for a view
   */
  registerInsteadOfTrigger<T = Record<string, unknown>>(
    trigger: InsteadOfTrigger<T>
  ): void;

  /**
   * Get INSTEAD OF triggers for a view and operation
   */
  getInsteadOfTriggers(
    viewName: string,
    operation: ViewDMLOperation
  ): InsteadOfTrigger[];

  /**
   * Remove an INSTEAD OF trigger
   */
  removeInsteadOfTrigger(name: string): boolean;
}

// =============================================================================
// View Executor Types
// =============================================================================

/**
 * Options for view execution
 */
export interface ViewExecutionOptions {
  /** Request ID for tracing */
  requestId?: string | undefined;

  /** Transaction ID */
  txnId?: string | undefined;

  /** Query timeout in ms */
  timeout?: number | undefined;

  /** Maximum rows to return */
  limit?: number | undefined;

  /** Offset for pagination */
  offset?: number | undefined;
}

/**
 * View executor interface
 */
export interface ViewExecutor {
  /**
   * Execute a SELECT query against a view
   */
  select<T = Record<string, unknown>>(
    viewName: string,
    options?: ViewExecutionOptions & {
      where?: string;
      orderBy?: string;
    }
  ): Promise<ViewQueryResult<T>>;

  /**
   * Execute an INSERT through a view (requires INSTEAD OF trigger)
   */
  insert<T = Record<string, unknown>>(
    viewName: string,
    row: T,
    options?: ViewExecutionOptions
  ): Promise<ViewDMLResult>;

  /**
   * Execute an UPDATE through a view (requires INSTEAD OF trigger)
   */
  update<T = Record<string, unknown>>(
    viewName: string,
    changes: Partial<T>,
    where: string | ((row: T) => boolean),
    options?: ViewExecutionOptions
  ): Promise<ViewDMLResult>;

  /**
   * Execute a DELETE through a view (requires INSTEAD OF trigger)
   */
  delete<T = Record<string, unknown>>(
    viewName: string,
    where: string | ((row: T) => boolean),
    options?: ViewExecutionOptions
  ): Promise<ViewDMLResult>;

  /**
   * Expand a view into its underlying SELECT query
   */
  expand(viewName: string): string;

  /**
   * Validate that a view's definition is still valid
   */
  validate(viewName: string): { valid: boolean; errors: string[] };
}

// =============================================================================
// Error Types
// =============================================================================

/**
 * View-specific error codes
 */
export enum ViewErrorCode {
  /** View with this name already exists */
  DUPLICATE_VIEW = 'VIEW_DUPLICATE',
  /** View not found */
  NOT_FOUND = 'VIEW_NOT_FOUND',
  /** Invalid view definition */
  INVALID_DEFINITION = 'VIEW_INVALID_DEFINITION',
  /** View is not updatable */
  NOT_UPDATABLE = 'VIEW_NOT_UPDATABLE',
  /** No INSTEAD OF trigger for operation */
  NO_INSTEAD_OF_TRIGGER = 'VIEW_NO_INSTEAD_OF_TRIGGER',
  /** Circular view dependency detected */
  CIRCULAR_DEPENDENCY = 'VIEW_CIRCULAR_DEPENDENCY',
  /** View references non-existent table or view */
  INVALID_REFERENCE = 'VIEW_INVALID_REFERENCE',
  /** WITH CHECK OPTION violation */
  CHECK_OPTION_VIOLATION = 'VIEW_CHECK_OPTION_VIOLATION',
  /** View query execution failed */
  QUERY_ERROR = 'VIEW_QUERY_ERROR',
  /** Cannot drop view due to dependencies */
  HAS_DEPENDENTS = 'VIEW_HAS_DEPENDENTS',
}

/**
 * Error class for view operations
 *
 * Extends DoSQLError for unified error handling across the DoSQL ecosystem.
 */
export class ViewError extends DoSQLError {
  readonly code: ViewErrorCode;
  readonly category: ErrorCategory;
  readonly viewName?: string;

  constructor(
    code: ViewErrorCode,
    message: string,
    viewName?: string,
    cause?: Error
  ) {
    super(message, cause ? { cause } : undefined);
    this.name = 'ViewError';
    this.code = code;
    this.viewName = viewName;
    this.category = this.determineCategory();

    if (this.viewName) {
      this.context = { metadata: { viewName: this.viewName } };
    }
  }

  private determineCategory(): ErrorCategory {
    switch (this.code) {
      case ViewErrorCode.VIEW_NOT_FOUND:
        return ErrorCategory.RESOURCE;
      case ViewErrorCode.VIEW_EXISTS:
      case ViewErrorCode.HAS_DEPENDENTS:
        return ErrorCategory.CONFLICT;
      case ViewErrorCode.NOT_UPDATABLE:
      case ViewErrorCode.INVALID_DEFINITION:
      case ViewErrorCode.INVALID_REFERENCE:
      case ViewErrorCode.CHECK_OPTION_VIOLATION:
        return ErrorCategory.VALIDATION;
      case ViewErrorCode.QUERY_ERROR:
        return ErrorCategory.EXECUTION;
      default:
        return ErrorCategory.EXECUTION;
    }
  }

  override isRetryable(): boolean {
    return false;
  }

  static fromJSON(json: SerializedError): ViewError {
    return new ViewError(
      json.code as ViewErrorCode,
      json.message,
      json.context?.metadata?.viewName as string | undefined
    );
  }
}

registerErrorClass('ViewError', ViewError);

// =============================================================================
// Utility Types
// =============================================================================

/**
 * View metadata for introspection
 */
export interface ViewMetadata {
  /** View name */
  name: string;

  /** Schema name */
  schema?: string | undefined;

  /** Column information */
  columns: ViewColumn[];

  /** Referenced tables */
  tables: string[];

  /** Referenced views */
  views: string[];

  /** Whether updatable */
  updatable: boolean;

  /** Check option */
  checkOption?: CheckOption | undefined;

  /** View type */
  type: ViewType;
}

/**
 * Dependency graph node for view analysis
 */
export interface ViewDependencyNode {
  /** View name */
  name: string;

  /** Direct dependencies (tables and views this depends on) */
  dependencies: string[];

  /** Direct dependents (views that depend on this) */
  dependents: string[];

  /** Depth in dependency tree (0 = depends only on tables) */
  depth: number;
}

/**
 * Result of dependency analysis
 */
export interface DependencyAnalysis {
  /** All nodes in the dependency graph */
  nodes: Map<string, ViewDependencyNode>;

  /** Views in topological order (safe creation/update order) */
  topologicalOrder: string[];

  /** Circular dependencies detected */
  circularDependencies: string[][];

  /** Orphaned views (references don't exist) */
  orphanedViews: string[];
}
