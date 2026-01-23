/**
 * CLI action handler for the generate command.
 */

import { resolve } from 'node:path';
import { generateTypes } from '../commands/generate.js';
import { toError } from '../utils/errors.js';
import { logSuccess, logSection, logList, logError } from '../utils/logger.js';

/**
 * Options for the generate action.
 */
export interface GenerateActionOptions {
  schema: string;
  output: string;
}

/**
 * Handles the generate CLI command.
 * Generates TypeScript types from schema definitions.
 *
 * @param options - Command options from CLI
 */
export async function handleGenerateAction(options: GenerateActionOptions): Promise<void> {
  try {
    const result = await generateTypes({
      schemaDir: resolve(options.schema),
      outputDir: resolve(options.output),
    });

    logSuccess('Types generated successfully!');
    logSection('Generated files');
    logList(result.generatedFiles);
    logSection('Tables processed');
    logList(result.tablesProcessed);
  } catch (error) {
    logError(toError(error).message);
    process.exit(1);
  }
}
