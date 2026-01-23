/**
 * CLI action handlers.
 * Each action handles a specific CLI command with consistent error handling and output.
 */

export { handleInitAction, type InitActionOptions } from './init.js';
export { handleMigrateAction, type MigrateActionOptions } from './migrate.js';
export { handleGenerateAction, type GenerateActionOptions } from './generate.js';
