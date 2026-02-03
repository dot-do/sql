/**
 * Cursor Manager for DoSQL
 *
 * Manages server-side cursors for efficient pagination of large result sets.
 * Supports both SQL cursor syntax (DECLARE, FETCH, CLOSE) and programmatic API.
 *
 * @packageDocumentation
 */

import type { ColumnType } from '../rpc/types.js';
import {
  type CursorId,
  type CursorState,
  type CursorOptions,
  type FetchDirection,
  type FetchRequest,
  type FetchResult,
  type CursorManagerStats,
  type CursorManagerConfig,
  type CursorToken,
  generateCursorId,
  createCursorId,
  encodeCursorToken,
  decodeCursorToken,
  DEFAULT_CURSOR_CONFIG,
} from './types.js';

// =============================================================================
// Query Executor Interface
// =============================================================================

/**
 * Interface for executing queries (provided by the database engine)
 */
export interface CursorQueryExecutor {
  /** Execute a SQL query with pagination */
  execute(
    sql: string,
    params?: unknown[],
    options?: {
      branch?: string;
      limit?: number;
      offset?: number;
      transactionId?: string;
    }
  ): Promise<{
    columns: string[];
    columnTypes: ColumnType[];
    rows: unknown[][];
    rowCount: number;
  }>;
}

// =============================================================================
// Cursor Manager
// =============================================================================

/**
 * Server-side cursor manager
 *
 * Manages cursor lifecycle, cleanup, and pagination operations.
 *
 * @example
 * ```typescript
 * const manager = new CursorManager(executor, {
 *   maxCursors: 50,
 *   cursorTTLMs: 10 * 60 * 1000, // 10 minutes
 * });
 *
 * // Declare a cursor
 * const cursorId = await manager.declareCursor(
 *   'SELECT * FROM large_table WHERE status = ?',
 *   ['active'],
 *   { name: 'my_cursor' }
 * );
 *
 * // Fetch rows
 * const result = await manager.fetch({
 *   cursorId,
 *   direction: 'NEXT',
 *   count: 100,
 * });
 *
 * // Close when done
 * await manager.closeCursor(cursorId);
 * ```
 */
export class CursorManager {
  #cursors: Map<CursorId, CursorState> = new Map();
  #cursorNameMap: Map<string, CursorId> = new Map(); // Maps cursor names to IDs
  #executor: CursorQueryExecutor;
  #config: Required<Omit<CursorManagerConfig, 'onScheduleAlarm'>> & { onScheduleAlarm?: (delayMs: number) => void };
  #stats: CursorManagerStats = {
    activeCursors: 0,
    totalCreated: 0,
    totalClosed: 0,
    expiredCount: 0,
  };
  #alarmPending = false;

  constructor(executor: CursorQueryExecutor, config?: CursorManagerConfig) {
    this.#executor = executor;
    this.#config = {
      ...DEFAULT_CURSOR_CONFIG,
      ...config,
    };
  }

  // ===========================================================================
  // Cursor Lifecycle
  // ===========================================================================

  /**
   * Declare a new cursor for a SELECT query
   *
   * @param sql - SQL SELECT query
   * @param params - Query parameters
   * @param options - Cursor options
   * @returns Cursor ID
   */
  async declareCursor(
    sql: string,
    params?: unknown[],
    options?: CursorOptions
  ): Promise<CursorId> {
    // Check cursor limit
    if (this.#cursors.size >= this.#config.maxCursors) {
      // Try cleanup first
      this.cleanupExpiredCursors();
      if (this.#cursors.size >= this.#config.maxCursors) {
        throw new Error(`Maximum cursor limit reached (${this.#config.maxCursors})`);
      }
    }

    // Generate or use provided cursor ID
    const cursorId = options?.name
      ? createCursorId(options.name)
      : generateCursorId();

    // Check for duplicate name
    if (options?.name && this.#cursorNameMap.has(options.name)) {
      throw new Error(`Cursor '${options.name}' already exists`);
    }

    // Execute initial query to get schema
    const executeOptions: {
      branch?: string;
      limit?: number;
      offset?: number;
      transactionId?: string;
    } = { limit: 0 }; // Just get schema, no rows

    if (options?.branch !== undefined) {
      executeOptions.branch = options.branch;
    }
    if (options?.transactionId !== undefined) {
      executeOptions.transactionId = options.transactionId;
    }

    const schemaResult = await this.#executor.execute(sql, params, executeOptions);

    const now = Date.now();

    const state: CursorState = {
      id: cursorId,
      sql,
      columns: schemaResult.columns,
      columnTypes: schemaResult.columnTypes,
      position: 0,
      totalFetched: 0,
      exhausted: false,
      createdAt: now,
      lastActivity: now,
      scrollable: options?.scrollable ?? false,
      holdable: options?.holdable ?? false,
    };

    // Only set optional properties if they have values
    if (params !== undefined) {
      state.params = params;
    }
    if (options?.branch !== undefined) {
      state.branch = options.branch;
    }
    if (options?.transactionId !== undefined) {
      state.transactionId = options.transactionId;
    }

    this.#cursors.set(cursorId, state);
    if (options?.name) {
      this.#cursorNameMap.set(options.name, cursorId);
    }

    this.#stats.activeCursors++;
    this.#stats.totalCreated++;

    // Schedule cleanup alarm
    this.#scheduleCleanupAlarm();

    return cursorId;
  }

  /**
   * Fetch rows from a cursor
   *
   * @param request - Fetch request with cursor ID and direction
   * @returns Fetch result with rows
   */
  async fetch(request: FetchRequest): Promise<FetchResult> {
    const cursor = this.#getCursor(request.cursorId);

    // Update activity timestamp
    cursor.lastActivity = Date.now();

    // Validate fetch count
    const count = Math.min(
      Math.max(1, request.count),
      this.#config.maxFetchSize
    );

    // Calculate offset based on direction
    let offset = cursor.position;

    switch (request.direction) {
      case 'NEXT':
        // Already at correct position
        break;
      case 'PRIOR':
        if (!cursor.scrollable) {
          throw new Error('PRIOR fetch requires a scrollable cursor');
        }
        offset = Math.max(0, cursor.position - count);
        break;
      case 'FIRST':
        if (!cursor.scrollable) {
          throw new Error('FIRST fetch requires a scrollable cursor');
        }
        offset = 0;
        break;
      case 'LAST':
        throw new Error('LAST fetch is not supported (requires knowing total count)');
      case 'ABSOLUTE':
        if (!cursor.scrollable) {
          throw new Error('ABSOLUTE fetch requires a scrollable cursor');
        }
        offset = Math.max(0, request.count - 1);
        break;
      case 'RELATIVE':
        if (!cursor.scrollable) {
          throw new Error('RELATIVE fetch requires a scrollable cursor');
        }
        offset = Math.max(0, cursor.position + request.count);
        break;
      default:
        throw new Error(`Unknown fetch direction: ${request.direction}`);
    }

    // Build execute options, only including defined values
    const fetchOptions: {
      branch?: string;
      limit?: number;
      offset?: number;
      transactionId?: string;
    } = { limit: count, offset };

    if (cursor.branch !== undefined) {
      fetchOptions.branch = cursor.branch;
    }
    if (cursor.transactionId !== undefined) {
      fetchOptions.transactionId = cursor.transactionId;
    }

    // Execute query with offset/limit
    const result = await this.#executor.execute(cursor.sql, cursor.params, fetchOptions);

    // Update cursor state
    cursor.position = offset + result.rowCount;
    cursor.totalFetched += result.rowCount;
    cursor.exhausted = result.rowCount < count;

    // Build result, only including cursor token if more data available
    const fetchResult: FetchResult = {
      rows: result.rows,
      rowCount: result.rowCount,
      hasMore: !cursor.exhausted,
      position: cursor.position,
    };

    if (!cursor.exhausted) {
      fetchResult.cursorToken = encodeCursorToken({
        queryHash: this.#hashQuery(cursor.sql, cursor.params),
        offset: cursor.position,
        pageSize: count,
        timestamp: Date.now(),
      });
    }

    return fetchResult;
  }

  /**
   * Close a cursor and release its resources
   *
   * @param cursorId - Cursor ID to close
   */
  closeCursor(cursorId: CursorId): void {
    const cursor = this.#cursors.get(cursorId);
    if (!cursor) {
      // Silently ignore closing non-existent cursors
      return;
    }

    this.#cursors.delete(cursorId);

    // Remove from name map if present
    for (const [name, id] of this.#cursorNameMap) {
      if (id === cursorId) {
        this.#cursorNameMap.delete(name);
        break;
      }
    }

    this.#stats.activeCursors--;
    this.#stats.totalClosed++;
  }

  /**
   * Close a cursor by name
   *
   * @param name - Cursor name to close
   */
  closeCursorByName(name: string): void {
    const cursorId = this.#cursorNameMap.get(name);
    if (cursorId) {
      this.closeCursor(cursorId);
    }
  }

  /**
   * Close all cursors
   *
   * @returns Number of cursors closed
   */
  closeAllCursors(): number {
    const count = this.#cursors.size;
    this.#cursors.clear();
    this.#cursorNameMap.clear();
    this.#stats.activeCursors = 0;
    this.#stats.totalClosed += count;
    return count;
  }

  // ===========================================================================
  // Cursor-Token Based Pagination (Stateless)
  // ===========================================================================

  /**
   * Execute a query with cursor-token based pagination
   *
   * This provides a simpler API for pagination without maintaining
   * server-side cursor state between requests.
   *
   * @param sql - SQL query
   * @param params - Query parameters
   * @param options - Pagination options
   * @returns Query result with cursor token for next page
   */
  async executeWithPagination(
    sql: string,
    params?: unknown[],
    options?: {
      pageSize?: number;
      cursorToken?: string;
      branch?: string;
      transactionId?: string;
    }
  ): Promise<{
    columns: string[];
    columnTypes: ColumnType[];
    rows: unknown[][];
    rowCount: number;
    hasMore: boolean;
    cursorToken?: string;
  }> {
    const pageSize = options?.pageSize ?? this.#config.defaultFetchSize;
    let offset = 0;

    // Decode cursor token if provided
    if (options?.cursorToken) {
      const token = decodeCursorToken(options.cursorToken);
      if (!token) {
        throw new Error('Invalid cursor token');
      }

      // Validate token matches query
      const queryHash = this.#hashQuery(sql, params);
      if (token.queryHash !== queryHash) {
        throw new Error('Cursor token does not match query');
      }

      // Check token expiration (1 hour)
      if (Date.now() - token.timestamp > 60 * 60 * 1000) {
        throw new Error('Cursor token has expired');
      }

      offset = token.offset;
    }

    // Build execute options, only including defined values
    const execOptions: {
      branch?: string;
      limit?: number;
      offset?: number;
      transactionId?: string;
    } = { limit: pageSize + 1, offset };

    if (options?.branch !== undefined) {
      execOptions.branch = options.branch;
    }
    if (options?.transactionId !== undefined) {
      execOptions.transactionId = options.transactionId;
    }

    // Fetch one extra row to determine hasMore
    const result = await this.#executor.execute(sql, params, execOptions);

    const hasMore = result.rowCount > pageSize;
    const rows = hasMore ? result.rows.slice(0, pageSize) : result.rows;
    const rowCount = rows.length;

    // Build result object
    const paginationResult: {
      columns: string[];
      columnTypes: ColumnType[];
      rows: unknown[][];
      rowCount: number;
      hasMore: boolean;
      cursorToken?: string;
    } = {
      columns: result.columns,
      columnTypes: result.columnTypes,
      rows,
      rowCount,
      hasMore,
    };

    // Only add cursorToken if there are more results
    if (hasMore) {
      paginationResult.cursorToken = encodeCursorToken({
        queryHash: this.#hashQuery(sql, params),
        offset: offset + rowCount,
        pageSize,
        timestamp: Date.now(),
      });
    }

    return paginationResult;
  }

  // ===========================================================================
  // Cursor Information
  // ===========================================================================

  /**
   * Get cursor state by ID
   */
  getCursorState(cursorId: CursorId): CursorState | undefined {
    return this.#cursors.get(cursorId);
  }

  /**
   * Get cursor ID by name
   */
  getCursorIdByName(name: string): CursorId | undefined {
    return this.#cursorNameMap.get(name);
  }

  /**
   * Check if a cursor exists
   */
  cursorExists(cursorId: CursorId): boolean {
    return this.#cursors.has(cursorId);
  }

  /**
   * Check if a cursor name exists
   */
  cursorNameExists(name: string): boolean {
    return this.#cursorNameMap.has(name);
  }

  /**
   * Get all active cursor IDs
   */
  getActiveCursorIds(): CursorId[] {
    return Array.from(this.#cursors.keys());
  }

  /**
   * Get cursor manager statistics
   */
  getStats(): CursorManagerStats {
    return {
      ...this.#stats,
      activeCursors: this.#cursors.size,
    };
  }

  // ===========================================================================
  // Cleanup
  // ===========================================================================

  /**
   * Clean up expired cursors
   *
   * @returns Number of cursors cleaned up
   */
  cleanupExpiredCursors(): number {
    const now = Date.now();
    const expiredIds: CursorId[] = [];

    for (const [id, cursor] of this.#cursors) {
      if (now - cursor.lastActivity > this.#config.cursorTTLMs) {
        expiredIds.push(id);
      }
    }

    for (const id of expiredIds) {
      this.closeCursor(id);
      this.#stats.expiredCount++;
    }

    // Reschedule alarm if there are still active cursors
    if (this.#cursors.size > 0) {
      this.#alarmPending = false;
      this.#scheduleCleanupAlarm();
    } else {
      this.#alarmPending = false;
    }

    return expiredIds.length;
  }

  /**
   * Close all cursors associated with a transaction
   *
   * Called when a transaction commits or rolls back.
   *
   * @param transactionId - Transaction ID
   * @returns Number of cursors closed
   */
  closeTransactionCursors(transactionId: string): number {
    const toClose: CursorId[] = [];

    for (const [id, cursor] of this.#cursors) {
      if (cursor.transactionId === transactionId && !cursor.holdable) {
        toClose.push(id);
      }
    }

    for (const id of toClose) {
      this.closeCursor(id);
    }

    return toClose.length;
  }

  // ===========================================================================
  // Configuration
  // ===========================================================================

  /**
   * Update configuration at runtime
   */
  updateConfig(config: Partial<CursorManagerConfig>): void {
    Object.assign(this.#config, config);
  }

  /**
   * Get current configuration
   */
  getConfig(): CursorManagerConfig {
    return { ...this.#config };
  }

  // ===========================================================================
  // Private Helpers
  // ===========================================================================

  #getCursor(cursorId: CursorId): CursorState {
    const cursor = this.#cursors.get(cursorId);
    if (!cursor) {
      throw new Error(`Cursor '${cursorId}' not found`);
    }
    return cursor;
  }

  #scheduleCleanupAlarm(): void {
    if (this.#alarmPending || !this.#config.onScheduleAlarm) return;
    if (this.#cursors.size === 0) return;

    this.#alarmPending = true;
    // Schedule alarm for TTL + 1 minute buffer
    this.#config.onScheduleAlarm(this.#config.cursorTTLMs + 60000);
  }

  #hashQuery(sql: string, params?: unknown[]): string {
    // Simple hash for query matching
    const input = sql + JSON.stringify(params ?? []);
    let hash = 0;
    for (let i = 0; i < input.length; i++) {
      const char = input.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash; // Convert to 32bit integer
    }
    return hash.toString(36);
  }
}
