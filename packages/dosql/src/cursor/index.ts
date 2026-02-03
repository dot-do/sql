/**
 * Cursor Module for DoSQL
 *
 * Provides server-side cursor support for efficient pagination
 * of large result sets.
 *
 * ## Features
 *
 * - Server-side cursors with DECLARE/FETCH/CLOSE SQL syntax
 * - Programmatic cursor API with cursor tokens
 * - Automatic cursor cleanup and timeout
 * - Memory-efficient streaming for large datasets
 * - Transaction-bound and holdable cursors
 *
 * ## SQL Syntax
 *
 * ```sql
 * -- Declare a cursor
 * DECLARE my_cursor CURSOR FOR SELECT * FROM large_table;
 *
 * -- Declare a scrollable cursor (can move backwards)
 * DECLARE my_cursor SCROLL CURSOR FOR SELECT * FROM large_table;
 *
 * -- Fetch rows
 * FETCH NEXT 100 FROM my_cursor;
 * FETCH 50 FROM my_cursor;  -- shorthand for NEXT
 * FETCH FIRST FROM my_cursor;
 * FETCH PRIOR 10 FROM my_cursor;  -- requires SCROLL
 *
 * -- Close cursor
 * CLOSE my_cursor;
 * CLOSE ALL;  -- closes all cursors
 * ```
 *
 * ## Programmatic API
 *
 * ```typescript
 * const result = await db.query('SELECT * FROM t1', {
 *   cursor: true,
 *   pageSize: 100
 * });
 *
 * // Get next page using cursor token
 * const nextPage = await db.query('SELECT * FROM t1', {
 *   cursor: true,
 *   pageSize: 100,
 *   cursorToken: result.cursorToken
 * });
 * ```
 *
 * @packageDocumentation
 */

// Types
export type {
  CursorId,
  CursorState,
  CursorOptions,
  FetchDirection,
  FetchRequest,
  FetchResult,
  CursorToken,
  CursorManagerStats,
  CursorManagerConfig,
  DeclareCursorCommand,
  FetchCommand,
  CloseCursorCommand,
  CursorCommand,
  CursorQueryOptions,
} from './types.js';

// Type factories
export {
  createCursorId,
  generateCursorId,
  encodeCursorToken,
  decodeCursorToken,
  DEFAULT_CURSOR_CONFIG,
} from './types.js';

// Manager
export { CursorManager, type CursorQueryExecutor } from './manager.js';

// Parser
export {
  isCursorCommand,
  parseCursorCommand,
  isValidCursorName,
  isSelectQuery,
} from './parser.js';
