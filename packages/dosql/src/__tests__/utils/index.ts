/**
 * Test Utilities
 *
 * This module exports test utilities for the DoSQL package.
 * These should ONLY be used in test files, never in production code.
 *
 * @packageDocumentation
 */

export {
  MockQueryExecutor,
  // SQL tokenization helpers (for advanced testing)
  tokenizeSql,
  hasMultipleStatements,
  extractTableFromSelect,
  extractTableFromCreateTable,
  getStatementType,
  hasUnionKeyword,
  hasImplicitJoin,
} from './mock-query-executor.js';
