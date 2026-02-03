/**
 * Test Utilities
 *
 * @deprecated Prefer importing from '../test-utils.js' instead, which provides
 * all test utilities from a single consolidated location.
 *
 * This module is kept for backward compatibility and re-exports the
 * MockQueryExecutor and related helpers from mock-query-executor.ts.
 *
 * @packageDocumentation
 */

export {
  MockQueryExecutor,
  FakeQueryExecutor,
  tokenizeSql,
  hasMultipleStatements,
  extractTableFromSelect,
  extractTableFromCreateTable,
  getStatementType,
  hasUnionKeyword,
  hasImplicitJoin,
} from './mock-query-executor.js';
