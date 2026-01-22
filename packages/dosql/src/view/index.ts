/**
 * View Module for DoSQL
 *
 * Provides SQL VIEW support including:
 * - CREATE VIEW / CREATE OR REPLACE VIEW
 * - CREATE VIEW IF NOT EXISTS
 * - CREATE TEMP VIEW
 * - DROP VIEW / DROP VIEW IF EXISTS
 * - View with parameters (column aliases)
 * - Nested views (view selecting from view)
 * - View with JOINs
 * - Updatable views (INSERT/UPDATE/DELETE through view)
 * - INSTEAD OF triggers on views
 * - WITH CHECK OPTION
 */

// =============================================================================
// Type Exports
// =============================================================================

export type {
  // View definition types
  ViewType,
  CheckOption,
  ViewColumn,
  ParsedView,
  ViewDefinition,

  // DROP VIEW types
  DropViewStatement,

  // Query types
  ViewQueryContext,
  ViewQueryResult,

  // DML types
  ViewDMLOperation,
  ViewDMLContext,
  ViewDMLResult,
  InsteadOfHandler,
  InsteadOfTrigger,

  // Registry types
  RegisterViewOptions,
  ListViewsOptions,
  ViewRegistry,

  // Executor types
  ViewExecutionOptions,
  ViewExecutor,

  // Utility types
  ViewMetadata,
  ViewDependencyNode,
  DependencyAnalysis,
} from './types.js';

export { ViewError, ViewErrorCode } from './types.js';

// =============================================================================
// Parser Exports
// =============================================================================

export {
  // CREATE VIEW parsing
  parseCreateView,
  tryParseCreateView,
  isCreateView,

  // DROP VIEW parsing
  parseDropView,
  isDropView,

  // Column parsing
  parseSelectColumns,
  extractReferences,

  // Validation
  validateViewDefinition,

  // SQL generation
  generateCreateViewSql,
  generateDropViewSql,
} from './parser.js';

// =============================================================================
// Registry Exports
// =============================================================================

export type { ViewStorage, ViewRegistryOptions } from './registry.js';

export {
  createInMemoryViewStorage,
  createViewRegistry,
  analyzeDependencies,
  ViewBuilder,
  view,
} from './registry.js';

// =============================================================================
// Executor Exports
// =============================================================================

export type { SqlExecutorAdapter, ViewExecutorOptions, SqlViewManager } from './executor.js';

export {
  createViewExecutor,
  createMockSqlExecutor,
  createSqlViewManager,
} from './executor.js';
