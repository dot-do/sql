/**
 * CLI action handler for the init command.
 */

import { resolve } from 'node:path';
import { initProject } from '../commands/init.js';
import { toError } from '../utils/errors.js';
import { logSuccess, logSection, logList, logError } from '../utils/logger.js';

/**
 * Options for the init action.
 */
export interface InitActionOptions {
  directory: string;
  name?: string;
  force: boolean;
}

/**
 * Handles the init CLI command.
 * Creates project structure with config, migrations, and schema directories.
 *
 * @param options - Command options from CLI
 */
export async function handleInitAction(options: InitActionOptions): Promise<void> {
  try {
    const initOptions = {
      directory: resolve(options.directory),
      force: options.force,
      ...(options.name !== undefined && { name: options.name }),
    };
    const result = await initProject(initOptions);

    logSuccess('DoSQL project initialized successfully!');
    logSection('Created files');
    logList(result.createdFiles);
  } catch (error) {
    logError(toError(error).message);
    process.exit(1);
  }
}
