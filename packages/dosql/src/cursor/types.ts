/**
 * Cursor Types for DoSQL
 *
 * Defines types for server-side cursor management supporting
 * efficient pagination of large result sets.
 *
 * @packageDocumentation
 */

import type { ColumnType } from '../rpc/types.js';

// =============================================================================
// Cursor Identifier
// =============================================================================

/**
 * Branded type for cursor identifiers
 */
export type CursorId = string & { readonly __brand: 'CursorId' };

/**
 * Create a cursor ID from a string
 */
export function createCursorId(id: string): CursorId {
  return id as CursorId;
}

/**
 * Generate a unique cursor ID
 */
export function generateCursorId(): CursorId {
  const timestamp = Date.now().toString(36);
  const random = Math.random().toString(36).slice(2, 10);
  return createCursorId(`cursor_${timestamp}_${random}`);
}

// =============================================================================
// Cursor State
// =============================================================================

/**
 * Internal cursor state stored on the server
 */
export interface CursorState {
  /** Unique cursor identifier */
  id: CursorId;
  /** SQL query for this cursor */
  sql: string;
  /** Query parameters */
  params?: unknown[];
  /** Branch for multi-tenant isolation */
  branch?: string;
  /** Column names from the query */
  columns: string[];
  /** Column types from the query */
  columnTypes: ColumnType[];
  /** Current position in the result set */
  position: number;
  /** Total rows fetched so far */
  totalFetched: number;
  /** Whether the cursor has reached end of results */
  exhausted: boolean;
  /** Timestamp when cursor was created */
  createdAt: number;
  /** Timestamp of last activity */
  lastActivity: number;
  /** Whether cursor is scrollable (can move backwards) */
  scrollable: boolean;
  /** Whether cursor holds data for updates */
  holdable: boolean;
  /** Transaction ID if cursor is within a transaction */
  transactionId?: string;
}

/**
 * Cursor options when declaring a cursor
 */
export interface CursorOptions {
  /** Cursor name (optional, will be generated if not provided) */
  name?: string;
  /** Whether cursor is scrollable (can move backwards) */
  scrollable?: boolean;
  /** Whether cursor persists after transaction commit */
  holdable?: boolean;
  /** Branch for multi-tenant isolation */
  branch?: string;
  /** Transaction ID to bind cursor to */
  transactionId?: string;
}

// =============================================================================
// Cursor Fetch Direction
// =============================================================================

/**
 * Direction for FETCH operations
 */
export type FetchDirection =
  | 'NEXT'
  | 'PRIOR'
  | 'FIRST'
  | 'LAST'
  | 'ABSOLUTE'
  | 'RELATIVE';

/**
 * Fetch request options
 */
export interface FetchRequest {
  /** Cursor to fetch from */
  cursorId: CursorId;
  /** Direction to fetch */
  direction: FetchDirection;
  /** Number of rows to fetch (for NEXT/PRIOR) or position (for ABSOLUTE/RELATIVE) */
  count: number;
}

/**
 * Result of a fetch operation
 */
export interface FetchResult {
  /** Fetched rows */
  rows: unknown[][];
  /** Number of rows in this result */
  rowCount: number;
  /** Whether there are more rows available */
  hasMore: boolean;
  /** Current cursor position after fetch */
  position: number;
  /** Cursor token for client-side pagination (encodes state for next fetch) */
  cursorToken?: string;
}

// =============================================================================
// Cursor Token (for stateless pagination)
// =============================================================================

/**
 * Encoded cursor token for client-side pagination
 *
 * This allows clients to paginate without maintaining server state,
 * though server-side cursors are more efficient for large datasets.
 */
export interface CursorToken {
  /** SQL query hash for validation */
  queryHash: string;
  /** Current offset position */
  offset: number;
  /** Page size hint */
  pageSize: number;
  /** Sort key values for keyset pagination (more efficient than offset) */
  keysetValues?: unknown[];
  /** Timestamp for token expiration */
  timestamp: number;
}

/**
 * Encode a cursor token to string
 */
export function encodeCursorToken(token: CursorToken): string {
  const json = JSON.stringify(token);
  // Base64 encode for URL safety
  return btoa(json);
}

/**
 * Decode a cursor token from string
 */
export function decodeCursorToken(encoded: string): CursorToken | null {
  try {
    const json = atob(encoded);
    return JSON.parse(json) as CursorToken;
  } catch {
    return null;
  }
}

// =============================================================================
// Cursor Manager Interface
// =============================================================================

/**
 * Cursor manager statistics
 */
export interface CursorManagerStats {
  /** Number of active cursors */
  activeCursors: number;
  /** Total cursors created */
  totalCreated: number;
  /** Total cursors closed */
  totalClosed: number;
  /** Cursors expired due to timeout */
  expiredCount: number;
}

/**
 * Configuration options for the cursor manager
 */
export interface CursorManagerConfig {
  /** Maximum concurrent cursors per connection (default: 100) */
  maxCursors?: number;
  /** Cursor TTL in milliseconds (default: 30 minutes) */
  cursorTTLMs?: number;
  /** Default fetch size when not specified (default: 100) */
  defaultFetchSize?: number;
  /** Maximum fetch size allowed (default: 10000) */
  maxFetchSize?: number;
  /** Callback when alarm should be scheduled for cleanup */
  onScheduleAlarm?: (delayMs: number) => void;
}

/**
 * Default cursor manager configuration
 */
export const DEFAULT_CURSOR_CONFIG: Required<Omit<CursorManagerConfig, 'onScheduleAlarm'>> = {
  maxCursors: 100,
  cursorTTLMs: 30 * 60 * 1000, // 30 minutes
  defaultFetchSize: 100,
  maxFetchSize: 10000,
};

// =============================================================================
// SQL Cursor Commands
// =============================================================================

/**
 * Parsed DECLARE CURSOR command
 */
export interface DeclareCursorCommand {
  type: 'DECLARE_CURSOR';
  cursorName: string;
  scrollable: boolean;
  holdable: boolean;
  query: string;
}

/**
 * Parsed FETCH command
 */
export interface FetchCommand {
  type: 'FETCH';
  direction: FetchDirection;
  count: number;
  cursorName: string;
}

/**
 * Parsed CLOSE command
 */
export interface CloseCursorCommand {
  type: 'CLOSE_CURSOR';
  cursorName: string;
}

/**
 * All cursor-related commands
 */
export type CursorCommand = DeclareCursorCommand | FetchCommand | CloseCursorCommand;

// =============================================================================
// Programmatic Query Options with Cursor Support
// =============================================================================

/**
 * Extended query options with cursor pagination support
 */
export interface CursorQueryOptions {
  /** Enable cursor mode (returns results with cursor token) */
  cursor?: boolean;
  /** Page size for cursor pagination */
  pageSize?: number;
  /** Cursor token from previous page */
  cursorToken?: string;
  /** Use keyset pagination instead of offset (more efficient) */
  keysetPagination?: boolean;
  /** Column(s) to use for keyset pagination */
  keysetColumns?: string[];
}
