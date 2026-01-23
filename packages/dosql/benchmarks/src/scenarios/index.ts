/**
 * Benchmark Scenarios Index
 *
 * Export all scenario implementations and utilities for the
 * comprehensive SQLite implementation comparison suite.
 */

import type { ScenarioType, ScenarioConfig, ScenarioExecutor } from '../types.js';

// Import all scenario executors
import { executeSimpleSelect, SIMPLE_SELECT_CONFIG } from './simple-select.js';
import { executeInsert, executeBulkInsert, INSERT_CONFIG, BULK_INSERT_CONFIG } from './insert.js';
import { executeUpdate, UPDATE_CONFIG } from './update.js';
import { executeDelete, DELETE_CONFIG } from './delete.js';
import { executeTransaction, TRANSACTION_CONFIG } from './transaction.js';
import { executeComplexJoin, COMPLEX_JOIN_CONFIG } from './complex-join.js';
import { executeColdStart, COLD_START_CONFIG } from './cold-start.js';
import { executeMemoryUsage, MEMORY_USAGE_CONFIG } from './memory-usage.js';

// Export individual scenarios
export * from './simple-select.js';
export * from './insert.js';
export * from './update.js';
export * from './delete.js';
export * from './transaction.js';
export * from './complex-join.js';
export * from './cold-start.js';
export * from './memory-usage.js';

// =============================================================================
// Scenario Registry
// =============================================================================

/**
 * Map of scenario types to their executors
 */
export const SCENARIO_EXECUTORS: Record<ScenarioType, ScenarioExecutor> = {
  'simple-select': executeSimpleSelect,
  insert: executeInsert,
  'bulk-insert': executeBulkInsert,
  'batch-insert': executeBulkInsert, // Alias
  update: executeUpdate,
  delete: executeDelete,
  transaction: executeTransaction,
  'range-query': executeSimpleSelect, // Uses similar logic
  'complex-join': executeComplexJoin,
  'cold-start': executeColdStart,
  'memory-usage': executeMemoryUsage,
};

/**
 * Map of scenario types to their default configs
 */
export const DEFAULT_SCENARIO_CONFIGS: Record<ScenarioType, ScenarioConfig> = {
  'simple-select': SIMPLE_SELECT_CONFIG,
  insert: INSERT_CONFIG,
  'bulk-insert': BULK_INSERT_CONFIG,
  'batch-insert': BULK_INSERT_CONFIG, // Alias
  update: UPDATE_CONFIG,
  delete: DELETE_CONFIG,
  transaction: TRANSACTION_CONFIG,
  'range-query': {
    type: 'range-query',
    name: 'Range Query',
    description: 'SELECT with BETWEEN clause',
    iterations: 100,
    warmupIterations: 10,
    rowCount: 1000,
  },
  'complex-join': COMPLEX_JOIN_CONFIG,
  'cold-start': COLD_START_CONFIG,
  'memory-usage': MEMORY_USAGE_CONFIG,
};

/**
 * Get the executor for a scenario type
 */
export function getScenarioExecutor(type: ScenarioType): ScenarioExecutor {
  const executor = SCENARIO_EXECUTORS[type];
  if (!executor) {
    throw new Error(`Unknown scenario type: ${type}`);
  }
  return executor;
}

/**
 * Get the default config for a scenario type
 */
export function getDefaultScenarioConfig(type: ScenarioType): ScenarioConfig {
  const config = DEFAULT_SCENARIO_CONFIGS[type];
  if (!config) {
    throw new Error(`Unknown scenario type: ${type}`);
  }
  return config;
}

/**
 * Get all available scenario types
 */
export function getAllScenarioTypes(): ScenarioType[] {
  return Object.keys(SCENARIO_EXECUTORS) as ScenarioType[];
}

/**
 * Get quick benchmark scenarios (subset for fast testing)
 */
export function getQuickScenarios(): ScenarioType[] {
  return ['simple-select', 'insert', 'update', 'transaction'];
}

/**
 * Get comprehensive benchmark scenarios (full suite)
 */
export function getComprehensiveScenarios(): ScenarioType[] {
  return [
    'simple-select',
    'insert',
    'bulk-insert',
    'update',
    'delete',
    'transaction',
    'complex-join',
    'cold-start',
  ];
}

/**
 * Scenario display names for reports
 */
export const SCENARIO_DISPLAY_NAMES: Record<ScenarioType, string> = {
  'simple-select': 'Simple SELECT (by PK)',
  insert: 'INSERT (single row)',
  'bulk-insert': 'Bulk INSERT (1000 rows)',
  'batch-insert': 'Batch INSERT',
  update: 'UPDATE (single row)',
  delete: 'DELETE (single row)',
  transaction: 'Transaction (multi-op)',
  'range-query': 'Range Query',
  'complex-join': 'Complex JOIN',
  'cold-start': 'Cold Start Time',
  'memory-usage': 'Memory Usage',
};
