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
  // Preferred name following NO MOCKS philosophy (FakeQueryExecutor is an alias)
  FakeQueryExecutor,
  // SQL tokenization helpers (for advanced testing)
  tokenizeSql,
  hasMultipleStatements,
  extractTableFromSelect,
  extractTableFromCreateTable,
  getStatementType,
  hasUnionKeyword,
  hasImplicitJoin,
} from './mock-query-executor.js';
