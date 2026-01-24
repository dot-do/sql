/**
 * sql.do/cli - CLI helpers for terminal-based SQL operations
 *
 * Users install @dotdo/cli separately to use this module:
 * ```bash
 * npm install @dotdo/cli
 * ```
 *
 * @example
 * ```typescript
 * import { formatQueryResult, highlightSQL, withQuerySpinner } from 'sql.do/cli';
 *
 * const result = await withQuerySpinner('Running query...', () =>
 *   client.query('SELECT * FROM users')
 * );
 *
 * console.log(highlightSQL('SELECT * FROM users'));
 * console.log(formatQueryResult(result, 'table'));
 * ```
 *
 * @packageDocumentation
 * @module sql.do/cli
 */

import type { SQLClient } from './types.js';

/**
 * Output format for query results.
 *
 * @public
 * @since 0.2.0
 */
export type OutputFormat = 'table' | 'json' | 'csv';

/**
 * Query result shape compatible with SQLClient.query() results.
 *
 * @public
 * @since 0.2.0
 */
export interface FormattableResult {
  /** Result rows */
  rows: unknown[];
  /** Column names (optional, inferred from rows if not provided) */
  columns?: string[];
}

/**
 * Formats a query result for terminal output.
 *
 * @description Renders query result rows in the specified format using
 * @dotdo/cli's output formatting utilities. Supports table (ASCII art),
 * JSON (pretty-printed), and CSV formats.
 *
 * Requires `@dotdo/cli` to be installed as a peer dependency.
 *
 * @param result - The query result to format
 * @param format - Output format ('table', 'json', or 'csv')
 * @returns Formatted string representation of the result
 * @throws {Error} When @dotdo/cli is not installed
 *
 * @example
 * ```typescript
 * import { formatQueryResult } from 'sql.do/cli';
 *
 * const result = await client.query('SELECT id, name FROM users');
 * console.log(await formatQueryResult(result, 'table'));
 * // +----+-------+
 * // | id | name  |
 * // +----+-------+
 * // |  1 | Alice |
 * // |  2 | Bob   |
 * // +----+-------+
 * ```
 *
 * @public
 * @since 0.2.0
 */
export async function formatQueryResult(
  result: FormattableResult,
  format: OutputFormat = 'table'
): Promise<string> {
  const { formatOutput } = await import('@dotdo/cli/output');
  return formatOutput(result.rows, format);
}

/**
 * Syntax-highlights a SQL string for terminal output.
 *
 * @description Applies ANSI color codes to SQL keywords, strings, numbers,
 * and identifiers for readable terminal output.
 *
 * Requires `@dotdo/cli` to be installed as a peer dependency.
 *
 * @param sql - The SQL string to highlight
 * @returns The SQL string with ANSI color codes applied
 * @throws {Error} When @dotdo/cli is not installed
 *
 * @example
 * ```typescript
 * import { highlightSQL } from 'sql.do/cli';
 *
 * const highlighted = await highlightSQL(
 *   'SELECT name, email FROM users WHERE active = true'
 * );
 * console.log(highlighted); // Colorized SQL output
 * ```
 *
 * @public
 * @since 0.2.0
 */
export async function highlightSQL(sql: string): Promise<string> {
  const { highlightSql } = await import('@dotdo/cli/highlight');
  return highlightSql(sql);
}

/**
 * Wraps an async operation with a terminal spinner.
 *
 * @description Shows a spinner animation in the terminal while the provided
 * function executes. The spinner displays the given label and is automatically
 * stopped when the function resolves or rejects.
 *
 * Requires `@dotdo/cli` to be installed as a peer dependency.
 *
 * @typeParam T - The return type of the wrapped function
 * @param label - Text to display next to the spinner
 * @param fn - Async function to execute while spinner is active
 * @returns The resolved value of the wrapped function
 * @throws {Error} When @dotdo/cli is not installed or the wrapped function throws
 *
 * @example
 * ```typescript
 * import { withQuerySpinner } from 'sql.do/cli';
 *
 * const result = await withQuerySpinner('Executing query...', async () => {
 *   return client.query('SELECT * FROM large_table');
 * });
 * ```
 *
 * @public
 * @since 0.2.0
 */
export async function withQuerySpinner<T>(label: string, fn: () => Promise<T>): Promise<T> {
  const { withSpinner } = await import('@dotdo/cli/spinner');
  return withSpinner(label, fn);
}

/**
 * Creates a SQL client authenticated via @dotdo/cli's built-in auth.
 *
 * @description Uses @dotdo/cli's authentication mechanism (which may prompt
 * the user interactively) to obtain credentials, then creates a SQL client
 * with those credentials.
 *
 * Requires `@dotdo/cli` to be installed as a peer dependency.
 *
 * @param options - Client configuration (url is required)
 * @returns An authenticated SQLClient instance
 * @throws {Error} When @dotdo/cli is not installed or authentication fails
 *
 * @example
 * ```typescript
 * import { createAuthenticatedCLIClient } from 'sql.do/cli';
 *
 * const client = await createAuthenticatedCLIClient({
 *   url: 'https://sql.example.com',
 * });
 *
 * const result = await client.query('SELECT * FROM users');
 * await client.close();
 * ```
 *
 * @public
 * @since 0.2.0
 */
export async function createAuthenticatedCLIClient(options: { url: string; database?: string; timeout?: number }): Promise<SQLClient> {
  const { ensureAuth } = await import('@dotdo/cli/auth');
  const user = await ensureAuth();
  const { createSQLClient } = await import('./client.js');
  return createSQLClient({ ...options, token: user.token });
}
