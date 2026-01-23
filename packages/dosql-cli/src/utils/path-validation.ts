/**
 * Path validation utilities to prevent directory traversal attacks.
 */

import { resolve, normalize, relative, isAbsolute, sep } from 'node:path';
import { promises as fs } from 'node:fs';
import { realpath } from 'node:fs/promises';

/** Maximum allowed path length to prevent DoS */
const MAX_PATH_LENGTH = 4096;

export class PathValidationError extends Error {
  public readonly attemptedPath: string;

  constructor(message: string, attemptedPath: string) {
    super(message);
    this.name = 'PathValidationError';
    this.attemptedPath = attemptedPath;
  }
}

/**
 * Checks if a resolved path is within the base directory.
 * Handles prefix attacks by ensuring proper path boundaries.
 */
export function isPathWithinBase(resolvedPath: string, baseDir: string): boolean {
  // Normalize both paths and remove trailing slashes for consistent comparison
  const normalizedResolved = normalize(resolvedPath).replace(/\/+$/, '');
  const normalizedBase = normalize(baseDir).replace(/\/+$/, '');

  // Handle edge case where resolved path equals base path
  if (normalizedResolved === normalizedBase) {
    return true;
  }

  // Get the relative path from base to resolved
  const relativePath = relative(normalizedBase, normalizedResolved);

  // If relative path is absolute, it's on a different drive (Windows)
  if (isAbsolute(relativePath)) {
    return false;
  }

  // Check if relative path tries to go up with ".."
  // We need to check for exactly ".." not "..." or "..foo"
  // Pattern: starts with ".." followed by separator, or equals ".."
  if (relativePath === '..' || relativePath.startsWith('..' + sep)) {
    return false;
  }

  // Empty relative path means they're the same directory
  if (relativePath === '') {
    return true;
  }

  // Ensure the resolved path actually starts with the base path + separator
  // This prevents prefix attacks like /project-evil being considered within /project
  return normalizedResolved.startsWith(normalizedBase + sep);
}

/**
 * Decodes URL-encoded characters that could be used for traversal attacks.
 */
function decodeTraversalPatterns(path: string): string {
  // Decode common URL-encoded traversal patterns
  let decoded = path;

  // %2e = . , %2f = / , %5c = \
  decoded = decoded.replace(/%2e/gi, '.');
  decoded = decoded.replace(/%2f/gi, '/');
  decoded = decoded.replace(/%5c/gi, '\\');

  return decoded;
}

/**
 * Checks for dangerous patterns in the raw path string.
 */
function containsDangerousPatterns(inputPath: string): boolean {
  // Decode any URL-encoded characters first
  const decoded = decodeTraversalPatterns(inputPath);

  // Check for null bytes (can truncate paths in some systems)
  if (decoded.includes('\x00')) {
    return true;
  }

  // Check for explicit directory traversal patterns
  // We need to be careful to only match ".." followed by a separator or at end,
  // not filenames like "..." or "..foo"
  const traversalPatterns = [
    /(?:^|\/)\.\.(?:\/|$)/,   // Unix traversal: starts with ../ or contains /../ or ends with /..
    /(?:^|\\)\.\.(?:\\|$)/,   // Windows traversal: starts with ..\ or contains \..\ or ends with \..
  ];

  for (const pattern of traversalPatterns) {
    if (pattern.test(decoded)) {
      return true;
    }
  }

  return false;
}

/**
 * Validates that a path is safe and within the allowed base directory.
 * @param inputPath - The path provided by user input
 * @param baseDir - The base directory that all paths must stay within
 * @returns The resolved, validated absolute path
 * @throws PathValidationError if path is invalid or attempts directory traversal
 */
export async function validatePath(inputPath: string, baseDir: string): Promise<string> {
  // Handle empty path
  if (!inputPath || inputPath.trim() === '') {
    throw new PathValidationError(
      'Path cannot be empty',
      inputPath
    );
  }

  // Check for extremely long paths (DoS prevention)
  if (inputPath.length > MAX_PATH_LENGTH) {
    throw new PathValidationError(
      `Path exceeds maximum length of ${MAX_PATH_LENGTH} characters`,
      inputPath
    );
  }

  // Check for dangerous patterns in the raw input
  if (containsDangerousPatterns(inputPath)) {
    throw new PathValidationError(
      `Path contains directory traversal sequence or dangerous characters. Attempted path: ${inputPath}`,
      inputPath
    );
  }

  // Normalize the base directory
  const normalizedBase = resolve(baseDir);

  // Resolve the input path relative to the base directory
  let resolvedPath: string;
  if (isAbsolute(inputPath)) {
    resolvedPath = normalize(inputPath);
  } else {
    resolvedPath = resolve(normalizedBase, inputPath);
  }

  // Check if the resolved path is within the base directory
  if (!isPathWithinBase(resolvedPath, normalizedBase)) {
    throw new PathValidationError(
      `Path resolves outside the allowed directory. Path must be within: ${normalizedBase}`,
      inputPath
    );
  }

  // Check if path exists and handle symlinks
  try {
    const stat = await fs.lstat(resolvedPath);

    if (stat.isSymbolicLink()) {
      // Resolve the symlink to its real path
      const realPath = await realpath(resolvedPath);

      // Also resolve the base directory to its real path for comparison
      // This handles cases like macOS where /var -> /private/var
      let realBase: string;
      try {
        realBase = await realpath(normalizedBase);
      } catch {
        // If base doesn't exist or can't be resolved, use normalized base
        realBase = normalizedBase;
      }

      // Verify the real path is also within the real base directory
      if (!isPathWithinBase(realPath, realBase)) {
        throw new PathValidationError(
          `Symbolic link resolves outside the allowed directory. Links must point within: ${normalizedBase}`,
          inputPath
        );
      }

      return realPath;
    }
  } catch (err) {
    // Path doesn't exist yet - that's okay for paths we're about to create
    // But we still validated the resolved path is within bounds
    if ((err as NodeJS.ErrnoException).code !== 'ENOENT') {
      // Re-throw PathValidationError
      if (err instanceof PathValidationError) {
        throw err;
      }
      // For other errors, wrap them
      throw new PathValidationError(
        `Failed to validate path: ${(err as Error).message}`,
        inputPath
      );
    }
  }

  return resolvedPath;
}

/**
 * Validates a path synchronously without checking for symlinks.
 * Use this when you don't need symlink resolution.
 */
export function validatePathSync(inputPath: string, baseDir: string): string {
  // Handle empty path
  if (!inputPath || inputPath.trim() === '') {
    throw new PathValidationError(
      'Path cannot be empty',
      inputPath
    );
  }

  // Check for extremely long paths (DoS prevention)
  if (inputPath.length > MAX_PATH_LENGTH) {
    throw new PathValidationError(
      `Path exceeds maximum length of ${MAX_PATH_LENGTH} characters`,
      inputPath
    );
  }

  // Check for dangerous patterns in the raw input
  if (containsDangerousPatterns(inputPath)) {
    throw new PathValidationError(
      `Path contains directory traversal sequence or dangerous characters. Attempted path: ${inputPath}`,
      inputPath
    );
  }

  // Normalize the base directory
  const normalizedBase = resolve(baseDir);

  // Resolve the input path relative to the base directory
  let resolvedPath: string;
  if (isAbsolute(inputPath)) {
    resolvedPath = normalize(inputPath);
  } else {
    resolvedPath = resolve(normalizedBase, inputPath);
  }

  // Check if the resolved path is within the base directory
  if (!isPathWithinBase(resolvedPath, normalizedBase)) {
    throw new PathValidationError(
      `Path resolves outside the allowed directory. Path must be within: ${normalizedBase}`,
      inputPath
    );
  }

  return resolvedPath;
}

/**
 * Validates a CLI input path.
 * Unlike validatePath, this allows absolute paths anywhere on the system,
 * as long as they don't contain directory traversal sequences.
 *
 * Use this for CLI commands where users are allowed to specify any directory.
 * @param inputPath - The path provided by CLI input
 * @returns The resolved, validated absolute path
 * @throws PathValidationError if path contains traversal patterns or is invalid
 */
export async function validateCliPath(inputPath: string): Promise<string> {
  // Handle empty path
  if (!inputPath || inputPath.trim() === '') {
    throw new PathValidationError(
      'Path cannot be empty',
      inputPath
    );
  }

  // Check for extremely long paths (DoS prevention)
  if (inputPath.length > MAX_PATH_LENGTH) {
    throw new PathValidationError(
      `Path exceeds maximum length of ${MAX_PATH_LENGTH} characters`,
      inputPath
    );
  }

  // Check for dangerous patterns in the raw input
  if (containsDangerousPatterns(inputPath)) {
    throw new PathValidationError(
      `Path contains directory traversal sequence or dangerous characters. Attempted path: ${inputPath}`,
      inputPath
    );
  }

  // Resolve the path to absolute
  const resolvedPath = resolve(inputPath);

  // For symlinks, verify they don't escape using traversal
  try {
    const stat = await fs.lstat(resolvedPath);

    if (stat.isSymbolicLink()) {
      // Read the symlink target
      const linkTarget = await fs.readlink(resolvedPath);

      // Check if the link target itself contains dangerous patterns
      if (containsDangerousPatterns(linkTarget)) {
        throw new PathValidationError(
          `Symbolic link target contains directory traversal sequence. Attempted path: ${inputPath}`,
          inputPath
        );
      }
    }
  } catch (err) {
    // Path doesn't exist yet - that's okay for paths we're about to create
    if ((err as NodeJS.ErrnoException).code !== 'ENOENT') {
      // Re-throw PathValidationError
      if (err instanceof PathValidationError) {
        throw err;
      }
      // Ignore other errors (e.g., permission issues)
    }
  }

  return resolvedPath;
}

/**
 * Synchronous version of validateCliPath.
 */
export function validateCliPathSync(inputPath: string): string {
  // Handle empty path
  if (!inputPath || inputPath.trim() === '') {
    throw new PathValidationError(
      'Path cannot be empty',
      inputPath
    );
  }

  // Check for extremely long paths (DoS prevention)
  if (inputPath.length > MAX_PATH_LENGTH) {
    throw new PathValidationError(
      `Path exceeds maximum length of ${MAX_PATH_LENGTH} characters`,
      inputPath
    );
  }

  // Check for dangerous patterns in the raw input
  if (containsDangerousPatterns(inputPath)) {
    throw new PathValidationError(
      `Path contains directory traversal sequence or dangerous characters. Attempted path: ${inputPath}`,
      inputPath
    );
  }

  // Resolve the path to absolute
  return resolve(inputPath);
}
