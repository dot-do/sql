/**
 * lake.do/cli - CLI helpers for terminal-based lakehouse operations
 *
 * Provides lakehouse-specific formatting for partitions, CDC streams,
 * snapshots, and query results.
 *
 * Users install @dotdo/cli separately to use this module:
 * ```bash
 * npm install @dotdo/cli
 * ```
 *
 * @example
 * ```typescript
 * import { formatQueryResult, formatPartitions, withQuerySpinner } from 'lake.do/cli';
 *
 * const result = await withQuerySpinner('Querying lakehouse...', () =>
 *   client.query('SELECT * FROM orders')
 * );
 *
 * console.log(await formatQueryResult(result, 'table'));
 *
 * const partitions = await client.listPartitions('orders');
 * console.log(await formatPartitions(partitions));
 * ```
 *
 * @packageDocumentation
 * @module lake.do/cli
 */

import type { LakeClient, PartitionInfo, Snapshot } from './types.js';

/**
 * Output format for query results.
 *
 * @public
 * @since 0.2.0
 */
export type OutputFormat = 'table' | 'json' | 'csv';

/**
 * Query result shape compatible with LakeClient.query() results.
 *
 * @public
 * @since 0.2.0
 */
export interface FormattableResult {
  /** Result rows */
  rows: unknown[];
  /** Column names (optional, inferred from rows if not provided) */
  columns?: string[];
  /** Bytes scanned (lakehouse-specific) */
  bytesScanned?: number;
  /** Query duration in milliseconds */
  duration?: number;
}

/**
 * Formats a query result for terminal output.
 *
 * @description Renders query result rows in the specified format using
 * @dotdo/cli's output formatting utilities. Includes lakehouse-specific
 * statistics (bytes scanned, duration) when available.
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
 * import { formatQueryResult } from 'lake.do/cli';
 *
 * const result = await client.query('SELECT * FROM orders LIMIT 5');
 * console.log(await formatQueryResult(result, 'table'));
 * // +----+--------+--------+
 * // | id | amount | status |
 * // +----+--------+--------+
 * // |  1 |  99.99 | shipped|
 * // +----+--------+--------+
 * // (5 rows, 1.2 MB scanned in 45ms)
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
 * Formats partition information for terminal output.
 *
 * @description Renders a list of partition info objects as a formatted table
 * showing key, file count, row count, and size for each partition.
 *
 * Requires `@dotdo/cli` to be installed as a peer dependency.
 *
 * @param partitions - Array of partition information objects
 * @returns Formatted string with partition details
 * @throws {Error} When @dotdo/cli is not installed
 *
 * @example
 * ```typescript
 * import { formatPartitions } from 'lake.do/cli';
 *
 * const partitions = await client.listPartitions('orders');
 * console.log(await formatPartitions(partitions));
 * // +----------------+-------+--------+---------+
 * // | key            | files |   rows | size    |
 * // +----------------+-------+--------+---------+
 * // | date=2024-01-01|     3 |  15000 | 12.5 MB |
 * // | date=2024-01-02|     5 |  23000 | 19.2 MB |
 * // +----------------+-------+--------+---------+
 * ```
 *
 * @public
 * @since 0.2.0
 */
export async function formatPartitions(partitions: PartitionInfo[]): Promise<string> {
  const { formatOutput } = await import('@dotdo/cli/output');
  const rows = partitions.map((p) => ({
    key: p.key,
    files: p.fileCount,
    rows: p.rowCount,
    size: formatBytes(p.sizeBytes),
  }));
  return formatOutput(rows, 'table');
}

/**
 * Formats snapshot history for terminal output.
 *
 * @description Renders a list of snapshot objects showing ID, timestamp,
 * and summary statistics for time travel capabilities.
 *
 * Requires `@dotdo/cli` to be installed as a peer dependency.
 *
 * @param snapshots - Array of snapshot objects
 * @returns Formatted string with snapshot details
 * @throws {Error} When @dotdo/cli is not installed
 *
 * @example
 * ```typescript
 * import { formatSnapshots } from 'lake.do/cli';
 *
 * const snapshots = await client.listSnapshots('orders');
 * console.log(await formatSnapshots(snapshots));
 * // +----------+---------------------+--------+---------+
 * // | id       | timestamp           | +rows  | -rows   |
 * // +----------+---------------------+--------+---------+
 * // | snap_001 | 2024-01-15 10:30:00 |   1500 |       0 |
 * // | snap_002 | 2024-01-16 14:00:00 |    200 |      50 |
 * // +----------+---------------------+--------+---------+
 * ```
 *
 * @public
 * @since 0.2.0
 */
export async function formatSnapshots(snapshots: Snapshot[]): Promise<string> {
  const { formatOutput } = await import('@dotdo/cli/output');
  const rows = snapshots.map((s) => ({
    id: s.id,
    timestamp: s.timestamp instanceof Date ? s.timestamp.toISOString() : String(s.timestamp),
    addedRows: s.summary.addedRows,
    deletedRows: s.summary.deletedRows,
  }));
  return formatOutput(rows, 'table');
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
 * import { highlightSQL } from 'lake.do/cli';
 *
 * const highlighted = await highlightSQL(
 *   'SELECT date, SUM(amount) FROM orders GROUP BY date'
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
 * import { withQuerySpinner } from 'lake.do/cli';
 *
 * const result = await withQuerySpinner('Scanning partitions...', async () => {
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
 * Creates a Lake client authenticated via @dotdo/cli's built-in auth.
 *
 * @description Uses @dotdo/cli's authentication mechanism (which may prompt
 * the user interactively) to obtain credentials, then creates a Lake client
 * with those credentials.
 *
 * Requires `@dotdo/cli` to be installed as a peer dependency.
 *
 * @param options - Client configuration (url is required)
 * @returns An authenticated LakeClient instance
 * @throws {Error} When @dotdo/cli is not installed or authentication fails
 *
 * @example
 * ```typescript
 * import { createAuthenticatedCLIClient } from 'lake.do/cli';
 *
 * const client = await createAuthenticatedCLIClient({
 *   url: 'https://lake.example.com',
 * });
 *
 * const result = await client.query('SELECT * FROM orders');
 * await client.close();
 * ```
 *
 * @public
 * @since 0.2.0
 */
export async function createAuthenticatedCLIClient(options: { url: string; timeout?: number }): Promise<LakeClient> {
  const { ensureAuth } = await import('@dotdo/cli/auth');
  const user = await ensureAuth();
  const { createLakeClient } = await import('./client.js');
  return createLakeClient({ ...options, token: user.token });
}

/**
 * Formats a byte count into a human-readable string.
 * @internal
 */
function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}
