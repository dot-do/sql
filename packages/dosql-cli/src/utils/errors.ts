/**
 * Error handling utilities for consistent error coercion across the CLI.
 */

/**
 * Coerces an unknown value to an Error instance.
 * If the value is already an Error, returns it as-is.
 * Otherwise, converts it to a string and wraps it in a new Error.
 *
 * @param error - The unknown error value to coerce
 * @returns An Error instance
 *
 * @example
 * ```ts
 * try {
 *   await someOperation();
 * } catch (error) {
 *   const err = toError(error);
 *   console.error(err.message);
 * }
 * ```
 */
export function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

/**
 * Extracts the error message from an unknown error value.
 * Convenience function for when you only need the message.
 *
 * @param error - The unknown error value
 * @returns The error message string
 *
 * @example
 * ```ts
 * try {
 *   await someOperation();
 * } catch (error) {
 *   console.error(`Error: ${getErrorMessage(error)}`);
 * }
 * ```
 */
export function getErrorMessage(error: unknown): string {
  return toError(error).message;
}
