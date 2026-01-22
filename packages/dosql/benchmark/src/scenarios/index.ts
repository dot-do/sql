/**
 * Benchmark Scenarios
 *
 * Export all scenario implementations and utilities.
 */

export {
  executeSimpleSelect,
  createSimpleSelectScenario,
  SIMPLE_SELECT_CONFIG,
} from './simple-select.js';

export {
  executeInsert,
  executeBatchInsert,
  createInsertScenario,
  createBatchInsertScenario,
  INSERT_CONFIG,
  BATCH_INSERT_CONFIG,
} from './insert.js';

export {
  executeTransaction,
  createTransactionScenario,
  TRANSACTION_CONFIG,
} from './transaction.js';

import type { ScenarioType, ScenarioConfig, ScenarioExecutor } from '../types.js';
import { executeSimpleSelect, SIMPLE_SELECT_CONFIG } from './simple-select.js';
import {
  executeInsert,
  executeBatchInsert,
  INSERT_CONFIG,
  BATCH_INSERT_CONFIG,
} from './insert.js';
import { executeTransaction, TRANSACTION_CONFIG } from './transaction.js';

/**
 * Map of scenario types to their executors
 */
export const SCENARIO_EXECUTORS: Record<ScenarioType, ScenarioExecutor> = {
  'simple-select': executeSimpleSelect,
  insert: executeInsert,
  'batch-insert': executeBatchInsert,
  transaction: executeTransaction,
  // Placeholders for future scenarios
  'range-query': executeSimpleSelect, // TODO: Implement
  update: executeInsert, // TODO: Implement
  delete: executeInsert, // TODO: Implement
};

/**
 * Map of scenario types to their default configs
 */
export const DEFAULT_SCENARIO_CONFIGS: Record<ScenarioType, ScenarioConfig> = {
  'simple-select': SIMPLE_SELECT_CONFIG,
  insert: INSERT_CONFIG,
  'batch-insert': BATCH_INSERT_CONFIG,
  transaction: TRANSACTION_CONFIG,
  // Placeholders for future scenarios
  'range-query': {
    type: 'range-query',
    name: 'Range Query',
    description: 'SELECT with BETWEEN clause',
    iterations: 100,
    warmupIterations: 10,
    rowCount: 1000,
  },
  update: {
    type: 'update',
    name: 'UPDATE Single Row',
    description: 'Update a single row by primary key',
    iterations: 100,
    warmupIterations: 10,
    rowCount: 1000,
  },
  delete: {
    type: 'delete',
    name: 'DELETE Single Row',
    description: 'Delete a single row by primary key',
    iterations: 100,
    warmupIterations: 10,
    rowCount: 1000,
  },
};

/**
 * Get the executor for a scenario type
 */
export function getScenarioExecutor(type: ScenarioType): ScenarioExecutor {
  return SCENARIO_EXECUTORS[type];
}

/**
 * Get the default config for a scenario type
 */
export function getDefaultScenarioConfig(type: ScenarioType): ScenarioConfig {
  return DEFAULT_SCENARIO_CONFIGS[type];
}
