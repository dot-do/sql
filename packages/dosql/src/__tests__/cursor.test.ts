/**
 * Cursor-Based Pagination Tests
 *
 * Tests for server-side cursor management and efficient
 * pagination of large result sets.
 *
 * Features tested:
 * - DECLARE CURSOR / FETCH / CLOSE SQL syntax
 * - Programmatic cursor API with cursor tokens
 * - Cursor timeout and cleanup
 * - Memory-efficient streaming
 * - Scrollable cursors
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  CursorManager,
  type CursorQueryExecutor,
  createCursorId,
  generateCursorId,
  encodeCursorToken,
  decodeCursorToken,
  parseCursorCommand,
  isCursorCommand,
  isValidCursorName,
  isSelectQuery,
  DEFAULT_CURSOR_CONFIG,
  type CursorState,
  type FetchDirection,
} from '../cursor/index.js';
import type { ColumnType } from '../rpc/types.js';

// =============================================================================
// Mock Query Executor
// =============================================================================

class MockCursorExecutor implements CursorQueryExecutor {
  #data: Map<string, { columns: string[]; columnTypes: ColumnType[]; rows: unknown[][] }> = new Map();
  #executionLog: Array<{ sql: string; limit?: number; offset?: number }> = [];

  constructor() {
    // Add some test data
    this.addTable('users', ['id', 'name', 'email'], ['number', 'string', 'string'], [
      [1, 'Alice', 'alice@example.com'],
      [2, 'Bob', 'bob@example.com'],
      [3, 'Charlie', 'charlie@example.com'],
      [4, 'Diana', 'diana@example.com'],
      [5, 'Eve', 'eve@example.com'],
    ]);

    this.addTable('large_table', ['id', 'value'], ['number', 'string'],
      Array.from({ length: 1000 }, (_, i) => [i + 1, `value_${i + 1}`])
    );
  }

  addTable(name: string, columns: string[], columnTypes: ColumnType[], rows: unknown[][]) {
    this.#data.set(name, { columns, columnTypes, rows });
  }

  async execute(
    sql: string,
    params?: unknown[],
    options?: { branch?: string; limit?: number; offset?: number; transactionId?: string }
  ) {
    this.#executionLog.push({ sql, limit: options?.limit, offset: options?.offset });

    // Extract table name from SELECT
    const tableMatch = sql.match(/FROM\s+(\w+)/i);
    const tableName = tableMatch?.[1]?.toLowerCase();

    if (!tableName || !this.#data.has(tableName)) {
      return {
        columns: [],
        columnTypes: [] as ColumnType[],
        rows: [],
        rowCount: 0,
      };
    }

    const table = this.#data.get(tableName)!;
    let rows = [...table.rows];

    // Apply offset
    if (options?.offset !== undefined && options.offset > 0) {
      rows = rows.slice(options.offset);
    }

    // Apply limit
    if (options?.limit !== undefined && options.limit >= 0) {
      rows = rows.slice(0, options.limit);
    }

    return {
      columns: table.columns,
      columnTypes: table.columnTypes,
      rows,
      rowCount: rows.length,
    };
  }

  getExecutionLog() {
    return this.#executionLog;
  }

  clearLog() {
    this.#executionLog = [];
  }
}

// =============================================================================
// Type Tests
// =============================================================================

describe('Cursor Types', () => {
  describe('CursorId', () => {
    it('should create cursor ID from string', () => {
      const id = createCursorId('my_cursor');
      expect(id).toBe('my_cursor');
    });

    it('should generate unique cursor IDs', () => {
      const id1 = generateCursorId();
      const id2 = generateCursorId();
      expect(id1).not.toBe(id2);
      expect(id1).toMatch(/^cursor_[a-z0-9]+_[a-z0-9]+$/);
    });
  });

  describe('CursorToken', () => {
    it('should encode and decode cursor tokens', () => {
      const token = {
        queryHash: 'abc123',
        offset: 100,
        pageSize: 50,
        timestamp: Date.now(),
      };

      const encoded = encodeCursorToken(token);
      expect(typeof encoded).toBe('string');
      expect(encoded.length).toBeGreaterThan(0);

      const decoded = decodeCursorToken(encoded);
      expect(decoded).toEqual(token);
    });

    it('should return null for invalid tokens', () => {
      expect(decodeCursorToken('invalid')).toBeNull();
      expect(decodeCursorToken('')).toBeNull();
    });
  });

  describe('DEFAULT_CURSOR_CONFIG', () => {
    it('should have sensible defaults', () => {
      expect(DEFAULT_CURSOR_CONFIG.maxCursors).toBe(100);
      expect(DEFAULT_CURSOR_CONFIG.cursorTTLMs).toBe(30 * 60 * 1000);
      expect(DEFAULT_CURSOR_CONFIG.defaultFetchSize).toBe(100);
      expect(DEFAULT_CURSOR_CONFIG.maxFetchSize).toBe(10000);
    });
  });
});

// =============================================================================
// Parser Tests
// =============================================================================

describe('Cursor Parser', () => {
  describe('isCursorCommand', () => {
    it('should detect DECLARE commands', () => {
      expect(isCursorCommand('DECLARE my_cursor CURSOR FOR SELECT * FROM users')).toBe(true);
      expect(isCursorCommand('  declare foo cursor for select 1')).toBe(true);
    });

    it('should detect FETCH commands', () => {
      expect(isCursorCommand('FETCH NEXT 10 FROM my_cursor')).toBe(true);
      expect(isCursorCommand('fetch 5 from cursor1')).toBe(true);
    });

    it('should detect CLOSE commands', () => {
      expect(isCursorCommand('CLOSE my_cursor')).toBe(true);
      expect(isCursorCommand('close all')).toBe(true);
    });

    it('should return false for non-cursor commands', () => {
      expect(isCursorCommand('SELECT * FROM users')).toBe(false);
      expect(isCursorCommand('INSERT INTO users VALUES (1)')).toBe(false);
    });
  });

  describe('parseCursorCommand - DECLARE', () => {
    it('should parse basic DECLARE CURSOR', () => {
      const result = parseCursorCommand('DECLARE my_cursor CURSOR FOR SELECT * FROM users');
      expect(result).toEqual({
        type: 'DECLARE_CURSOR',
        cursorName: 'my_cursor',
        scrollable: false,
        holdable: false,
        query: 'SELECT * FROM users',
      });
    });

    it('should parse SCROLL CURSOR', () => {
      const result = parseCursorCommand('DECLARE scroll_cur SCROLL CURSOR FOR SELECT id FROM users');
      expect(result).toEqual({
        type: 'DECLARE_CURSOR',
        cursorName: 'scroll_cur',
        scrollable: true,
        holdable: false,
        query: 'SELECT id FROM users',
      });
    });

    it('should parse CURSOR WITH HOLD', () => {
      const result = parseCursorCommand('DECLARE hold_cur CURSOR WITH HOLD FOR SELECT * FROM users');
      expect(result).toEqual({
        type: 'DECLARE_CURSOR',
        cursorName: 'hold_cur',
        scrollable: false,
        holdable: true,
        query: 'SELECT * FROM users',
      });
    });

    it('should parse SCROLL CURSOR WITH HOLD', () => {
      const result = parseCursorCommand('DECLARE both_cur SCROLL CURSOR WITH HOLD FOR SELECT 1');
      expect(result).toEqual({
        type: 'DECLARE_CURSOR',
        cursorName: 'both_cur',
        scrollable: true,
        holdable: true,
        query: 'SELECT 1',
      });
    });

    it('should throw for non-SELECT queries', () => {
      expect(() => {
        parseCursorCommand('DECLARE bad_cur CURSOR FOR INSERT INTO users VALUES (1)');
      }).toThrow('query must be a SELECT');
    });
  });

  describe('parseCursorCommand - FETCH', () => {
    it('should parse FETCH NEXT with count', () => {
      const result = parseCursorCommand('FETCH NEXT 10 FROM my_cursor');
      expect(result).toEqual({
        type: 'FETCH',
        direction: 'NEXT',
        count: 10,
        cursorName: 'my_cursor',
      });
    });

    it('should parse FETCH with count only (defaults to NEXT)', () => {
      const result = parseCursorCommand('FETCH 5 FROM cursor1');
      expect(result).toEqual({
        type: 'FETCH',
        direction: 'NEXT',
        count: 5,
        cursorName: 'cursor1',
      });
    });

    it('should parse FETCH FROM (defaults to NEXT 1)', () => {
      const result = parseCursorCommand('FETCH FROM my_cursor');
      expect(result).toEqual({
        type: 'FETCH',
        direction: 'NEXT',
        count: 1,
        cursorName: 'my_cursor',
      });
    });

    it('should parse FETCH NEXT FROM (defaults to count 1)', () => {
      const result = parseCursorCommand('FETCH NEXT FROM my_cursor');
      expect(result).toEqual({
        type: 'FETCH',
        direction: 'NEXT',
        count: 1,
        cursorName: 'my_cursor',
      });
    });

    it('should parse FETCH PRIOR', () => {
      const result = parseCursorCommand('FETCH PRIOR 3 FROM scroll_cursor');
      expect(result).toEqual({
        type: 'FETCH',
        direction: 'PRIOR',
        count: 3,
        cursorName: 'scroll_cursor',
      });
    });

    it('should parse FETCH FIRST', () => {
      const result = parseCursorCommand('FETCH FIRST FROM cursor1');
      expect(result).toEqual({
        type: 'FETCH',
        direction: 'FIRST',
        count: 1,
        cursorName: 'cursor1',
      });
    });

    it('should parse FETCH ABSOLUTE', () => {
      const result = parseCursorCommand('FETCH ABSOLUTE 50 FROM cursor1');
      expect(result).toEqual({
        type: 'FETCH',
        direction: 'ABSOLUTE',
        count: 50,
        cursorName: 'cursor1',
      });
    });

    it('should parse FETCH RELATIVE', () => {
      const result = parseCursorCommand('FETCH RELATIVE -5 FROM cursor1');
      expect(result).toEqual({
        type: 'FETCH',
        direction: 'RELATIVE',
        count: -5,
        cursorName: 'cursor1',
      });
    });

    it('should parse FETCH with ROWS keyword', () => {
      const result = parseCursorCommand('FETCH NEXT 100 ROWS FROM cursor1');
      expect(result).toEqual({
        type: 'FETCH',
        direction: 'NEXT',
        count: 100,
        cursorName: 'cursor1',
      });
    });

    it('should parse FETCH FORWARD (alias for NEXT)', () => {
      const result = parseCursorCommand('FETCH FORWARD 10 FROM cursor1');
      expect(result).toEqual({
        type: 'FETCH',
        direction: 'NEXT',
        count: 10,
        cursorName: 'cursor1',
      });
    });

    it('should parse FETCH ALL', () => {
      const result = parseCursorCommand('FETCH ALL FROM my_cursor');
      expect(result).toEqual({
        type: 'FETCH',
        direction: 'NEXT',
        count: Number.MAX_SAFE_INTEGER,
        cursorName: 'my_cursor',
      });
    });
  });

  describe('parseCursorCommand - CLOSE', () => {
    it('should parse CLOSE cursor', () => {
      const result = parseCursorCommand('CLOSE my_cursor');
      expect(result).toEqual({
        type: 'CLOSE_CURSOR',
        cursorName: 'my_cursor',
      });
    });

    it('should parse CLOSE ALL', () => {
      const result = parseCursorCommand('CLOSE ALL');
      expect(result).toEqual({
        type: 'CLOSE_CURSOR',
        cursorName: 'ALL',
      });
    });
  });

  describe('isValidCursorName', () => {
    it('should accept valid cursor names', () => {
      expect(isValidCursorName('my_cursor')).toBe(true);
      expect(isValidCursorName('cursor1')).toBe(true);
      expect(isValidCursorName('_private')).toBe(true);
      expect(isValidCursorName('CamelCase')).toBe(true);
    });

    it('should reject invalid cursor names', () => {
      expect(isValidCursorName('1cursor')).toBe(false);
      expect(isValidCursorName('my-cursor')).toBe(false);
      expect(isValidCursorName('my cursor')).toBe(false);
      expect(isValidCursorName('')).toBe(false);
    });
  });

  describe('isSelectQuery', () => {
    it('should identify SELECT queries', () => {
      expect(isSelectQuery('SELECT * FROM users')).toBe(true);
      expect(isSelectQuery('  select id from t')).toBe(true);
      expect(isSelectQuery('SELECT 1')).toBe(true);
    });

    it('should reject non-SELECT queries', () => {
      expect(isSelectQuery('INSERT INTO users VALUES (1)')).toBe(false);
      expect(isSelectQuery('UPDATE users SET name = ?')).toBe(false);
      expect(isSelectQuery('DELETE FROM users')).toBe(false);
    });
  });
});

// =============================================================================
// Cursor Manager Tests
// =============================================================================

describe('CursorManager', () => {
  let executor: MockCursorExecutor;
  let manager: CursorManager;

  beforeEach(() => {
    executor = new MockCursorExecutor();
    manager = new CursorManager(executor, {
      maxCursors: 10,
      cursorTTLMs: 5000,
      defaultFetchSize: 10,
      maxFetchSize: 100,
    });
  });

  describe('declareCursor', () => {
    it('should create a cursor', async () => {
      const cursorId = await manager.declareCursor('SELECT * FROM users');
      expect(cursorId).toBeDefined();
      expect(manager.cursorExists(cursorId)).toBe(true);
    });

    it('should create a cursor with custom name', async () => {
      const cursorId = await manager.declareCursor('SELECT * FROM users', undefined, {
        name: 'my_cursor',
      });
      expect(cursorId).toBe('my_cursor');
      expect(manager.cursorNameExists('my_cursor')).toBe(true);
    });

    it('should reject duplicate cursor names', async () => {
      await manager.declareCursor('SELECT * FROM users', undefined, { name: 'cursor1' });

      await expect(
        manager.declareCursor('SELECT * FROM users', undefined, { name: 'cursor1' })
      ).rejects.toThrow("Cursor 'cursor1' already exists");
    });

    it('should respect max cursors limit', async () => {
      // Create max cursors
      for (let i = 0; i < 10; i++) {
        await manager.declareCursor('SELECT * FROM users');
      }

      await expect(
        manager.declareCursor('SELECT * FROM users')
      ).rejects.toThrow('Maximum cursor limit reached');
    });

    it('should initialize cursor state correctly', async () => {
      const cursorId = await manager.declareCursor(
        'SELECT * FROM users',
        undefined,
        { scrollable: true, holdable: true }
      );

      const state = manager.getCursorState(cursorId);
      expect(state).toBeDefined();
      expect(state!.position).toBe(0);
      expect(state!.totalFetched).toBe(0);
      expect(state!.exhausted).toBe(false);
      expect(state!.scrollable).toBe(true);
      expect(state!.holdable).toBe(true);
      expect(state!.columns).toEqual(['id', 'name', 'email']);
    });
  });

  describe('fetch', () => {
    it('should fetch rows from cursor', async () => {
      const cursorId = await manager.declareCursor('SELECT * FROM users');
      const result = await manager.fetch({
        cursorId,
        direction: 'NEXT',
        count: 2,
      });

      expect(result.rows).toHaveLength(2);
      expect(result.rowCount).toBe(2);
      expect(result.hasMore).toBe(true);
      expect(result.position).toBe(2);
      expect(result.cursorToken).toBeDefined();
    });

    it('should track cursor position across fetches', async () => {
      const cursorId = await manager.declareCursor('SELECT * FROM users');

      // First fetch
      let result = await manager.fetch({ cursorId, direction: 'NEXT', count: 2 });
      expect(result.rows[0]).toEqual([1, 'Alice', 'alice@example.com']);
      expect(result.position).toBe(2);

      // Second fetch
      result = await manager.fetch({ cursorId, direction: 'NEXT', count: 2 });
      expect(result.rows[0]).toEqual([3, 'Charlie', 'charlie@example.com']);
      expect(result.position).toBe(4);
    });

    it('should detect exhaustion', async () => {
      const cursorId = await manager.declareCursor('SELECT * FROM users');

      // Fetch all rows
      const result = await manager.fetch({ cursorId, direction: 'NEXT', count: 10 });
      expect(result.rows).toHaveLength(5);
      expect(result.hasMore).toBe(false);
      expect(result.cursorToken).toBeUndefined();

      const state = manager.getCursorState(cursorId);
      expect(state!.exhausted).toBe(true);
    });

    it('should enforce max fetch size', async () => {
      const cursorId = await manager.declareCursor('SELECT * FROM large_table');
      executor.clearLog();

      // Request more than max
      await manager.fetch({ cursorId, direction: 'NEXT', count: 1000 });

      // Should be clamped to maxFetchSize
      const log = executor.getExecutionLog();
      expect(log[0]!.limit).toBe(100); // maxFetchSize
    });

    it('should throw for non-existent cursor', async () => {
      await expect(
        manager.fetch({
          cursorId: createCursorId('nonexistent'),
          direction: 'NEXT',
          count: 10,
        })
      ).rejects.toThrow('Cursor');
    });

    describe('scrollable cursors', () => {
      it('should support PRIOR direction', async () => {
        const cursorId = await manager.declareCursor('SELECT * FROM users', undefined, {
          scrollable: true,
        });

        // Move forward
        await manager.fetch({ cursorId, direction: 'NEXT', count: 3 });

        // Move backward
        const result = await manager.fetch({ cursorId, direction: 'PRIOR', count: 2 });
        expect(result.rows).toHaveLength(2);
        expect(result.rows[0]).toEqual([2, 'Bob', 'bob@example.com']);
      });

      it('should support FIRST direction', async () => {
        const cursorId = await manager.declareCursor('SELECT * FROM users', undefined, {
          scrollable: true,
        });

        // Move forward
        await manager.fetch({ cursorId, direction: 'NEXT', count: 3 });

        // Go to first
        const result = await manager.fetch({ cursorId, direction: 'FIRST', count: 1 });
        expect(result.rows[0]).toEqual([1, 'Alice', 'alice@example.com']);
      });

      it('should support ABSOLUTE direction', async () => {
        const cursorId = await manager.declareCursor('SELECT * FROM users', undefined, {
          scrollable: true,
        });

        const result = await manager.fetch({ cursorId, direction: 'ABSOLUTE', count: 3 });
        expect(result.rows[0]).toEqual([3, 'Charlie', 'charlie@example.com']);
      });

      it('should throw for PRIOR on non-scrollable cursor', async () => {
        const cursorId = await manager.declareCursor('SELECT * FROM users');

        await expect(
          manager.fetch({ cursorId, direction: 'PRIOR', count: 1 })
        ).rejects.toThrow('scrollable cursor');
      });
    });
  });

  describe('closeCursor', () => {
    it('should close cursor by ID', async () => {
      const cursorId = await manager.declareCursor('SELECT * FROM users');
      expect(manager.cursorExists(cursorId)).toBe(true);

      manager.closeCursor(cursorId);
      expect(manager.cursorExists(cursorId)).toBe(false);
    });

    it('should close cursor by name', async () => {
      await manager.declareCursor('SELECT * FROM users', undefined, { name: 'named_cursor' });
      expect(manager.cursorNameExists('named_cursor')).toBe(true);

      manager.closeCursorByName('named_cursor');
      expect(manager.cursorNameExists('named_cursor')).toBe(false);
    });

    it('should silently ignore non-existent cursor', () => {
      expect(() => {
        manager.closeCursor(createCursorId('nonexistent'));
      }).not.toThrow();
    });

    it('should close all cursors', async () => {
      await manager.declareCursor('SELECT * FROM users');
      await manager.declareCursor('SELECT * FROM large_table');
      await manager.declareCursor('SELECT 1');

      const closed = manager.closeAllCursors();
      expect(closed).toBe(3);
      expect(manager.getStats().activeCursors).toBe(0);
    });
  });

  describe('executeWithPagination', () => {
    it('should paginate results with cursor tokens', async () => {
      // First page
      const page1 = await manager.executeWithPagination('SELECT * FROM users', undefined, {
        pageSize: 2,
      });

      expect(page1.rows).toHaveLength(2);
      expect(page1.hasMore).toBe(true);
      expect(page1.cursorToken).toBeDefined();

      // Second page
      const page2 = await manager.executeWithPagination('SELECT * FROM users', undefined, {
        pageSize: 2,
        cursorToken: page1.cursorToken,
      });

      expect(page2.rows).toHaveLength(2);
      expect(page2.hasMore).toBe(true);
      expect(page2.rows[0]).toEqual([3, 'Charlie', 'charlie@example.com']);

      // Third page (last)
      const page3 = await manager.executeWithPagination('SELECT * FROM users', undefined, {
        pageSize: 2,
        cursorToken: page2.cursorToken,
      });

      expect(page3.rows).toHaveLength(1);
      expect(page3.hasMore).toBe(false);
      expect(page3.cursorToken).toBeUndefined();
    });

    it('should reject mismatched cursor tokens', async () => {
      const page1 = await manager.executeWithPagination('SELECT * FROM users', undefined, {
        pageSize: 2,
      });

      // Try to use token with different query
      await expect(
        manager.executeWithPagination('SELECT * FROM large_table', undefined, {
          pageSize: 2,
          cursorToken: page1.cursorToken,
        })
      ).rejects.toThrow('does not match');
    });

    it('should reject invalid cursor tokens', async () => {
      await expect(
        manager.executeWithPagination('SELECT * FROM users', undefined, {
          cursorToken: 'invalid_token',
        })
      ).rejects.toThrow('Invalid cursor token');
    });
  });

  describe('cleanup', () => {
    it('should clean up expired cursors', async () => {
      // Create cursor with short TTL
      const shortTTLManager = new CursorManager(executor, {
        cursorTTLMs: 100, // 100ms
      });

      const cursorId = await shortTTLManager.declareCursor('SELECT * FROM users');
      expect(shortTTLManager.cursorExists(cursorId)).toBe(true);

      // Wait for expiration
      await new Promise((resolve) => setTimeout(resolve, 150));

      const cleaned = shortTTLManager.cleanupExpiredCursors();
      expect(cleaned).toBe(1);
      expect(shortTTLManager.cursorExists(cursorId)).toBe(false);
    });

    it('should track expired count in stats', async () => {
      const shortTTLManager = new CursorManager(executor, {
        cursorTTLMs: 50,
      });

      await shortTTLManager.declareCursor('SELECT * FROM users');
      await shortTTLManager.declareCursor('SELECT * FROM large_table');

      await new Promise((resolve) => setTimeout(resolve, 100));

      shortTTLManager.cleanupExpiredCursors();
      expect(shortTTLManager.getStats().expiredCount).toBe(2);
    });

    it('should close transaction cursors on commit', async () => {
      const cursorId = await manager.declareCursor(
        'SELECT * FROM users',
        undefined,
        { transactionId: 'tx123' }
      );

      expect(manager.cursorExists(cursorId)).toBe(true);

      const closed = manager.closeTransactionCursors('tx123');
      expect(closed).toBe(1);
      expect(manager.cursorExists(cursorId)).toBe(false);
    });

    it('should preserve holdable cursors on transaction close', async () => {
      const cursorId = await manager.declareCursor(
        'SELECT * FROM users',
        undefined,
        { transactionId: 'tx123', holdable: true }
      );

      const closed = manager.closeTransactionCursors('tx123');
      expect(closed).toBe(0);
      expect(manager.cursorExists(cursorId)).toBe(true);
    });
  });

  describe('statistics', () => {
    it('should track cursor statistics', async () => {
      let stats = manager.getStats();
      expect(stats.activeCursors).toBe(0);
      expect(stats.totalCreated).toBe(0);

      const cursor1 = await manager.declareCursor('SELECT * FROM users');
      const cursor2 = await manager.declareCursor('SELECT * FROM large_table');

      stats = manager.getStats();
      expect(stats.activeCursors).toBe(2);
      expect(stats.totalCreated).toBe(2);

      manager.closeCursor(cursor1);
      stats = manager.getStats();
      expect(stats.activeCursors).toBe(1);
      expect(stats.totalClosed).toBe(1);

      manager.closeCursor(cursor2);
      stats = manager.getStats();
      expect(stats.activeCursors).toBe(0);
      expect(stats.totalClosed).toBe(2);
    });
  });

  describe('configuration', () => {
    it('should allow runtime configuration updates', async () => {
      expect(manager.getConfig().maxCursors).toBe(10);

      manager.updateConfig({ maxCursors: 5 });
      expect(manager.getConfig().maxCursors).toBe(5);
    });
  });
});

// =============================================================================
// Integration Tests
// =============================================================================

describe('Cursor Integration', () => {
  let executor: MockCursorExecutor;
  let manager: CursorManager;

  beforeEach(() => {
    executor = new MockCursorExecutor();
    manager = new CursorManager(executor);
  });

  it('should handle complete cursor workflow', async () => {
    // 1. Declare cursor
    const cursorId = await manager.declareCursor(
      'SELECT * FROM large_table',
      undefined,
      { name: 'large_cursor' }
    );

    // 2. Fetch in batches
    let totalRows = 0;
    let hasMore = true;
    const batches: unknown[][][] = [];

    while (hasMore) {
      const result = await manager.fetch({
        cursorId,
        direction: 'NEXT',
        count: 100,
      });

      batches.push(result.rows);
      totalRows += result.rowCount;
      hasMore = result.hasMore;
    }

    expect(totalRows).toBe(1000);
    // Note: With defaultFetchSize=100, there will be 11 batches due to the
    // exhaustion check fetching one more time after the last full batch
    expect(batches.length).toBeGreaterThanOrEqual(10);

    // 3. Close cursor
    manager.closeCursor(cursorId);
    expect(manager.cursorExists(cursorId)).toBe(false);
  });

  it('should handle concurrent cursors', async () => {
    const cursor1 = await manager.declareCursor('SELECT * FROM users', undefined, { name: 'c1' });
    const cursor2 = await manager.declareCursor('SELECT * FROM large_table', undefined, { name: 'c2' });

    // Interleaved fetches
    const r1a = await manager.fetch({ cursorId: cursor1, direction: 'NEXT', count: 2 });
    const r2a = await manager.fetch({ cursorId: cursor2, direction: 'NEXT', count: 50 });
    const r1b = await manager.fetch({ cursorId: cursor1, direction: 'NEXT', count: 2 });
    const r2b = await manager.fetch({ cursorId: cursor2, direction: 'NEXT', count: 50 });

    // Verify independence
    expect(r1a.rows[0]).toEqual([1, 'Alice', 'alice@example.com']);
    expect(r1b.rows[0]).toEqual([3, 'Charlie', 'charlie@example.com']);
    expect(r2a.rows[0]).toEqual([1, 'value_1']);
    expect(r2b.rows[0]).toEqual([51, 'value_51']);

    manager.closeAllCursors();
  });

  it('should handle memory-efficient large result streaming', async () => {
    // Add very large table
    executor.addTable(
      'huge_table',
      ['id', 'data'],
      ['number', 'string'],
      Array.from({ length: 100000 }, (_, i) => [i + 1, `data_${i + 1}`])
    );

    const cursorId = await manager.declareCursor('SELECT * FROM huge_table');

    // Stream in batches, verifying we don't load all data at once
    let processedCount = 0;
    let hasMore = true;

    while (hasMore && processedCount < 50000) {
      const result = await manager.fetch({
        cursorId,
        direction: 'NEXT',
        count: 1000,
      });

      processedCount += result.rowCount;
      hasMore = result.hasMore;

      // Verify execution uses proper LIMIT/OFFSET
      const log = executor.getExecutionLog();
      const lastExec = log[log.length - 1];
      expect(lastExec?.limit).toBeLessThanOrEqual(10000); // maxFetchSize
    }

    expect(processedCount).toBe(50000);
    manager.closeCursor(cursorId);
  });
});
