/**
 * DoSQL Executor Module
 *
 * Exports execution functionality for DML statements.
 */

// DML Executor
export {
  DMLExecutor,
  InMemoryDMLStorage,
  createDMLExecutor,
  createInMemoryDMLExecutor,
} from './dml-executor.js';

// DML Executor Types
export type {
  DMLExecutionResult,
  DMLStorage,
  DMLExecutionOptions,
} from './dml-executor.js';
