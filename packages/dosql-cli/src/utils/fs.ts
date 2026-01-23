/**
 * File system utilities for common operations.
 */

import { promises as fs } from 'node:fs';

/**
 * Checks if a file or directory exists at the given path.
 * Non-throwing alternative to fs.access().
 *
 * @param path - The path to check
 * @returns Promise resolving to true if the path exists, false otherwise
 *
 * @example
 * ```ts
 * if (await fileExists('./config.ts')) {
 *   console.log('Config found');
 * }
 * ```
 */
export async function fileExists(path: string): Promise<boolean> {
  return fs.access(path).then(() => true).catch(() => false);
}

/**
 * Synchronous version of fileExists.
 * Checks if a file or directory exists at the given path.
 *
 * @param path - The path to check
 * @returns True if the path exists, false otherwise
 *
 * @example
 * ```ts
 * if (fileExistsSync('./config.ts')) {
 *   console.log('Config found');
 * }
 * ```
 */
export function fileExistsSync(path: string): boolean {
  try {
    require('fs').accessSync(path);
    return true;
  } catch {
    return false;
  }
}
