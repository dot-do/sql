/**
 * DoSQLClient Tests - RED Phase TDD
 *
 * These tests document the EXPECTED behavior of the DoSQLClient.
 * Tests using `it.fails()` document missing features that SHOULD exist.
 * Tests using `it()` verify existing behavior.
 *
 * Issue: sql-218
 *
 * Test categories:
 * 1. Constructor validation - GAP: Missing URL validation, config validation
 * 2. connect() establishes WebSocket - GAP: No explicit connect() method
 * 3. execute() runs queries - GAP: execute() method naming differs from spec
 * 4. executeMany() batches operations - GAP: No executeMany() method
 * 5. Transaction begin/commit/rollback - Partial implementation exists
 * 6. query() vs execute() semantics - GAP: Semantic differences not enforced
 * 7. WebSocket reconnection - GAP: No automatic reconnection
 * 8. Error handling for network failures - GAP: Incomplete error handling
 * 9. Connection timeout handling - GAP: No connection-level timeout
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import {
  DoSQLClient,
  SQLError,
  ConnectionError,
  TimeoutError,
  createSQLClient,
  TransactionContext,
} from '../client.js';
import type { SQLClientConfig, ClientEventMap } from '../client.js';

// =============================================================================
// Test Helpers
// =============================================================================

/**
 * Mock WebSocket for testing
 */
class MockWebSocket {
  static READY_STATE_CONNECTING = 0;
  static READY_STATE_OPEN = 1;
  static READY_STATE_CLOSING = 2;
  static READY_STATE_CLOSED = 3;

  readyState = MockWebSocket.READY_STATE_CONNECTING;
  url: string;
  private listeners: Map<string, Set<(event: any) => void>> = new Map();

  constructor(url: string) {
    this.url = url;
    // Simulate async connection
    setTimeout(() => {
      this.readyState = MockWebSocket.READY_STATE_OPEN;
      this.emit('open', {});
    }, 10);
  }

  addEventListener(event: string, callback: (event: any) => void): void {
    if (!this.listeners.has(event)) {
      this.listeners.set(event, new Set());
    }
    this.listeners.get(event)!.add(callback);
  }

  removeEventListener(event: string, callback: (event: any) => void): void {
    this.listeners.get(event)?.delete(callback);
  }

  send(data: string): void {
    // Simulate server response
    const request = JSON.parse(data);
    setTimeout(() => {
      this.emit('message', {
        data: JSON.stringify({
          id: request.id,
          result: { rows: [], rowsAffected: 0 },
        }),
      });
    }, 5);
  }

  close(): void {
    this.readyState = MockWebSocket.READY_STATE_CLOSED;
    this.emit('close', {});
  }

  private emit(event: string, data: any): void {
    this.listeners.get(event)?.forEach((callback) => callback(data));
  }

  // Test helpers
  simulateError(error: Error): void {
    this.emit('error', { error });
  }

  simulateClose(): void {
    this.readyState = MockWebSocket.READY_STATE_CLOSED;
    this.emit('close', {});
  }

  simulateMessage(data: any): void {
    this.emit('message', { data: JSON.stringify(data) });
  }
}

// =============================================================================
// 1. CONSTRUCTOR VALIDATION - GAP: Missing comprehensive validation
// =============================================================================

describe('DoSQLClient Constructor Validation', () => {
  /**
   * GAP: Constructor should validate that URL is provided
   * Currently: No validation - client created with undefined URL
   */
  it.fails('should throw error when URL is not provided', () => {
    // @ts-expect-error Testing runtime validation
    expect(() => new DoSQLClient({})).toThrow('URL is required');
  });

  /**
   * GAP: Constructor should validate URL format
   * Currently: No URL format validation
   */
  it.fails('should throw error for invalid URL format', () => {
    expect(() => new DoSQLClient({ url: 'not-a-valid-url' })).toThrow(
      'Invalid URL format'
    );
  });

  /**
   * GAP: Constructor should validate URL protocol
   * Currently: Accepts any protocol, should only accept ws://, wss://, http://, https://
   */
  it.fails('should throw error for unsupported URL protocol', () => {
    expect(() => new DoSQLClient({ url: 'ftp://sql.example.com' })).toThrow(
      'Unsupported protocol'
    );
  });

  /**
   * GAP: Constructor should validate timeout is positive
   * Currently: Accepts any number including negative
   */
  it.fails('should throw error for negative timeout', () => {
    expect(
      () => new DoSQLClient({ url: 'ws://localhost:8080', timeout: -1000 })
    ).toThrow('Timeout must be a positive number');
  });

  /**
   * GAP: Constructor should validate timeout is not zero
   * Currently: Accepts zero which would cause immediate timeout
   */
  it.fails('should throw error for zero timeout', () => {
    expect(
      () => new DoSQLClient({ url: 'ws://localhost:8080', timeout: 0 })
    ).toThrow('Timeout must be a positive number');
  });

  /**
   * GAP: Constructor should validate retry config
   * Currently: No validation of retry config values
   */
  it.fails('should throw error for invalid retry config', () => {
    expect(
      () =>
        new DoSQLClient({
          url: 'ws://localhost:8080',
          retry: { maxRetries: -1, baseDelayMs: 100, maxDelayMs: 5000 },
        })
    ).toThrow('maxRetries must be non-negative');
  });

  /**
   * GAP: Constructor should validate baseDelayMs <= maxDelayMs
   * Currently: No relationship validation between delay values
   */
  it.fails('should throw error when baseDelayMs > maxDelayMs', () => {
    expect(
      () =>
        new DoSQLClient({
          url: 'ws://localhost:8080',
          retry: { maxRetries: 3, baseDelayMs: 10000, maxDelayMs: 1000 },
        })
    ).toThrow('baseDelayMs cannot exceed maxDelayMs');
  });

  /**
   * Existing behavior: Constructor accepts valid config
   */
  it('should create client with valid config', () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      token: 'test-token',
      database: 'testdb',
      timeout: 30000,
    });
    expect(client).toBeInstanceOf(DoSQLClient);
  });

  /**
   * Existing behavior: createSQLClient factory creates client
   */
  it('should create client via factory function', () => {
    const client = createSQLClient({
      url: 'ws://localhost:8080',
    });
    expect(client).toBeDefined();
  });

  /**
   * GAP: Should expose config for inspection
   * Currently: Config is private with no getter
   */
  it.fails('should expose readonly config', () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      timeout: 30000,
    });

    // GAP: getConfig() or config getter should exist
    const config = (client as any).getConfig?.() ?? (client as any).config;
    expect(config.url).toBe('ws://localhost:8080');
    expect(config.timeout).toBe(30000);

    // Config should be immutable
    expect(() => {
      config.url = 'ws://other:8080';
    }).toThrow();
  });
});

// =============================================================================
// 2. CONNECT() ESTABLISHES WEBSOCKET - GAP: No explicit connect method
// =============================================================================

describe('Connection Management', () => {
  let originalWebSocket: typeof globalThis.WebSocket;

  beforeEach(() => {
    originalWebSocket = globalThis.WebSocket;
    (globalThis as any).WebSocket = MockWebSocket;
  });

  afterEach(() => {
    (globalThis as any).WebSocket = originalWebSocket;
  });

  /**
   * IMPLEMENTED: Explicit connect() method exists
   */
  it('should have explicit connect() method', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // connect() method exists
    expect(typeof client.connect).toBe('function');
    await client.connect();

    // Connection should be established
    expect(client.isConnected()).toBe(true);
  });

  /**
   * GAP: connect() should return connection status
   * Currently: No explicit connect method
   */
  it.fails('should return connection status from connect()', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: connect() should return status object
    const status = await (client as any).connect?.();
    expect(status).toMatchObject({
      connected: true,
      latency: expect.any(Number),
      serverVersion: expect.any(String),
    });
  });

  /**
   * IMPLEMENTED: isConnected() method checks connection status
   */
  it('should expose isConnected() method', () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    expect(typeof client.isConnected).toBe('function');
    expect(client.isConnected()).toBe(false);
  });

  /**
   * IMPLEMENTED: Connection events are emitted
   */
  it('should emit connected event', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    const onConnected = vi.fn();

    // on() method exists for event registration
    client.on('connected', onConnected);

    await client.connect();

    expect(onConnected).toHaveBeenCalledWith(
      expect.objectContaining({
        url: 'ws://localhost:8080',
      })
    );
  });

  /**
   * IMPLEMENTED: Disconnection events are emitted
   */
  it('should emit disconnected event', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    const onDisconnected = vi.fn();

    // on() method exists for event registration
    client.on('disconnected', onDisconnected);

    await client.connect();
    await client.close();

    expect(onDisconnected).toHaveBeenCalled();
  });

  /**
   * GAP: Should prevent operations before connect when eager mode enabled
   * Currently: Lazy connection allows operations without explicit connect
   */
  it.fails('should require explicit connect in eager mode', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      // GAP: eagerConnect option should exist
      eagerConnect: true,
    } as any);

    // Should throw because connect() wasn't called
    await expect(client.query('SELECT 1')).rejects.toThrow(
      'Not connected. Call connect() first.'
    );
  });
});

// =============================================================================
// 3. EXECUTE() RUNS QUERIES - GAP: execute() naming and semantics
// =============================================================================

describe('Query Execution', () => {
  let originalWebSocket: typeof globalThis.WebSocket;
  let mockWs: MockWebSocket;

  beforeEach(() => {
    originalWebSocket = globalThis.WebSocket;
    (globalThis as any).WebSocket = class extends MockWebSocket {
      constructor(url: string) {
        super(url);
        mockWs = this;
      }
    };
  });

  afterEach(() => {
    (globalThis as any).WebSocket = originalWebSocket;
  });

  /**
   * Existing behavior: exec() exists for write operations
   */
  it('should have exec() method for write operations', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    expect(typeof client.exec).toBe('function');
  });

  /**
   * Note: execute() EXISTS but is for prepared statements only.
   * This documents that execute() with raw SQL is not supported.
   */
  it('should have execute() method for prepared statements', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // execute() exists but requires a PreparedStatement, not raw SQL
    expect(typeof client.execute).toBe('function');
  });

  /**
   * GAP: Should have executeRaw() for direct SQL execution as alternative to exec()
   * Currently: exec() is the only way, no executeRaw() alias exists
   */
  it.fails('should have executeRaw() as alternative to exec()', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: executeRaw() method should exist as alias for exec()
    expect(typeof (client as any).executeRaw).toBe('function');

    // Should accept raw SQL like exec()
    const result = await (client as any).executeRaw('INSERT INTO users VALUES (1)');
    expect(result.rowsAffected).toBeDefined();
  });

  /**
   * GAP: run() method for DDL statements
   * Currently: exec() handles everything, no semantic separation
   */
  it.fails('should have run() method for DDL statements', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: run() method should exist for DDL
    expect(typeof (client as any).run).toBe('function');

    const result = await (client as any).run(
      'CREATE TABLE test (id INT PRIMARY KEY)'
    );
    expect(result.success).toBe(true);
  });

  /**
   * GAP: exec() should reject SELECT statements
   * Currently: exec() accepts any SQL statement
   */
  it.fails('should reject SELECT statements in exec()', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    await expect(client.exec('SELECT * FROM users')).rejects.toThrow(
      'Use query() for SELECT statements'
    );
  });

  /**
   * GAP: query() should reject mutation statements
   * Currently: query() accepts any SQL statement
   */
  it.fails('should reject mutation statements in query()', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    await expect(
      client.query('INSERT INTO users VALUES (1)')
    ).rejects.toThrow('Use exec() for INSERT/UPDATE/DELETE statements');
  });

  /**
   * GAP: Should track query execution time
   * Currently: No execution time tracking in result
   */
  it.fails('should track query execution time', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    const result = await client.query('SELECT * FROM users');

    // GAP: executionTimeMs should be in result
    expect(result.executionTimeMs).toBeDefined();
    expect(typeof result.executionTimeMs).toBe('number');
  });

  /**
   * GAP: Should provide query plan option
   * Currently: No EXPLAIN support
   */
  it.fails('should support EXPLAIN query plan', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: explain option or explainQuery() method should exist
    const plan = await (client as any).explainQuery?.('SELECT * FROM users');

    expect(plan).toBeDefined();
    expect(plan.plan).toBeDefined();
  });
});

// =============================================================================
// 4. EXECUTEMANY() BATCHES OPERATIONS - GAP: No executeMany method
// =============================================================================

describe('Batch Execution', () => {
  let originalWebSocket: typeof globalThis.WebSocket;

  beforeEach(() => {
    originalWebSocket = globalThis.WebSocket;
    (globalThis as any).WebSocket = MockWebSocket;
  });

  afterEach(() => {
    (globalThis as any).WebSocket = originalWebSocket;
  });

  /**
   * Existing behavior: batch() method exists
   */
  it('should have batch() method', () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    expect(typeof client.batch).toBe('function');
  });

  /**
   * GAP: executeMany() should exist for parameterized batch inserts
   * Currently: Only batch() exists which takes full SQL statements
   */
  it.fails('should have executeMany() for parameterized batch operations', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: executeMany() should exist
    expect(typeof (client as any).executeMany).toBe('function');

    // executeMany should take a single SQL with multiple param sets
    const result = await (client as any).executeMany(
      'INSERT INTO users (id, name) VALUES (?, ?)',
      [
        [1, 'Alice'],
        [2, 'Bob'],
        [3, 'Charlie'],
      ]
    );

    expect(result.totalRowsAffected).toBe(3);
    expect(result.results).toHaveLength(3);
  });

  /**
   * GAP: executeMany() should support transactional mode
   * Currently: batch() is not transactional by default
   */
  it.fails('should support transactional executeMany()', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: executeMany with transactional option
    const result = await (client as any).executeMany(
      'INSERT INTO users (id, name) VALUES (?, ?)',
      [
        [1, 'Alice'],
        [2, 'Bob'],
      ],
      { transactional: true }
    );

    expect(result.transactionId).toBeDefined();
  });

  /**
   * GAP: batch() should return partial results on failure
   * Currently: batch() does not expose partial results or failure index
   */
  it.fails('should return partial results on batch failure', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: SQLError should have partialResults and failedIndex properties
    const error = new SQLError({
      code: 'BATCH_PARTIAL_FAILURE',
      message: 'Batch failed at index 1',
    });

    // These properties should exist on the error but don't
    expect((error as any).partialResults).toBeDefined();
    expect((error as any).failedIndex).toBe(1);
  });

  /**
   * GAP: batch() should support continue-on-error mode
   * Currently: No option to continue after errors
   */
  it.fails('should support continue-on-error mode', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: continueOnError option
    const results = await (client as any).batch(
      [
        { sql: 'INSERT INTO users VALUES (1)', params: [] },
        { sql: 'INVALID SQL', params: [] },
        { sql: 'INSERT INTO users VALUES (3)', params: [] },
      ],
      { continueOnError: true }
    );

    expect(results).toHaveLength(3);
    expect(results[0].success).toBe(true);
    expect(results[1].error).toBeDefined();
    expect(results[2].success).toBe(true);
  });

  /**
   * GAP: Should support parallel batch execution
   * Currently: Sequential execution only
   */
  it.fails('should support parallel batch execution', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: parallel option for independent operations
    const results = await client.batch(
      [
        { sql: 'SELECT * FROM table1', params: [] },
        { sql: 'SELECT * FROM table2', params: [] },
        { sql: 'SELECT * FROM table3', params: [] },
      ],
      { parallel: true } as any
    );

    expect(results).toHaveLength(3);
  });
});

// =============================================================================
// 5. TRANSACTION BEGIN/COMMIT/ROLLBACK
// =============================================================================

describe('Transaction Management', () => {
  let originalWebSocket: typeof globalThis.WebSocket;

  beforeEach(() => {
    originalWebSocket = globalThis.WebSocket;
    (globalThis as any).WebSocket = MockWebSocket;
  });

  afterEach(() => {
    (globalThis as any).WebSocket = originalWebSocket;
  });

  /**
   * Existing behavior: beginTransaction() exists
   */
  it('should have beginTransaction() method', () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    expect(typeof client.beginTransaction).toBe('function');
  });

  /**
   * Existing behavior: commit() exists
   */
  it('should have commit() method', () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    expect(typeof client.commit).toBe('function');
  });

  /**
   * Existing behavior: rollback() exists
   */
  it('should have rollback() method', () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    expect(typeof client.rollback).toBe('function');
  });

  /**
   * Existing behavior: transaction() wrapper exists
   */
  it('should have transaction() convenience method', () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    expect(typeof client.transaction).toBe('function');
  });

  /**
   * GAP: Should support nested transactions (savepoints)
   * Currently: No savepoint support
   */
  it.fails('should support nested transactions via savepoints', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: savepoint() and releaseSavepoint() should exist
    const tx = await client.beginTransaction();

    // Create savepoint
    const savepoint = await (client as any).savepoint?.(tx.id, 'sp1');
    expect(savepoint).toBeDefined();

    // Rollback to savepoint
    await (client as any).rollbackToSavepoint?.(tx.id, 'sp1');

    // Release savepoint
    await (client as any).releaseSavepoint?.(tx.id, 'sp1');

    await client.commit(tx.id);
  });

  /**
   * GAP: Transaction should track its state
   * Currently: No state tracking exposed
   */
  it.fails('should track transaction state', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    const tx = await client.beginTransaction();

    // GAP: getTransactionState() should exist
    const state = await (client as any).getTransactionState?.(tx.id);

    expect(state).toMatchObject({
      id: tx.id,
      status: 'active',
      startedAt: expect.any(Date),
      statementCount: 0,
    });
  });

  /**
   * GAP: Transaction should auto-rollback on timeout
   * Currently: No transaction timeout support
   */
  it.fails('should auto-rollback transaction on timeout', async () => {
    vi.useFakeTimers();

    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: transactionTimeout option should exist
    const tx = await client.beginTransaction({
      timeout: 1000, // 1 second timeout
    } as any);

    // Advance time past timeout
    vi.advanceTimersByTime(1500);

    // Transaction should be auto-rolled back
    await expect(client.commit(tx.id)).rejects.toThrow(
      'Transaction timed out and was rolled back'
    );

    vi.useRealTimers();
  });

  /**
   * GAP: Should detect transaction conflicts
   * Currently: No conflict detection exposed
   */
  it.fails('should expose transaction conflict information', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    try {
      await client.transaction(async (tx) => {
        await tx.exec('UPDATE accounts SET balance = 100 WHERE id = 1');
        // Simulate conflict
        throw new SQLError({
          code: 'TRANSACTION_CONFLICT',
          message: 'Transaction conflict detected',
        });
      });
    } catch (error: any) {
      // GAP: Conflict details should be exposed
      expect(error.conflictingTransaction).toBeDefined();
      expect(error.conflictingTable).toBe('accounts');
      expect(error.conflictingRow).toBeDefined();
    }
  });

  /**
   * GAP: TransactionContext should expose more information
   * Currently: Only transactionId is exposed
   */
  it.fails('should expose full transaction info in TransactionContext', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    await client.transaction(async (tx) => {
      // Existing: transactionId is exposed
      expect(tx.transactionId).toBeDefined();

      // GAP: These should also be exposed
      expect((tx as any).isolationLevel).toBeDefined();
      expect((tx as any).readOnly).toBeDefined();
      expect((tx as any).startedAt).toBeInstanceOf(Date);
      expect((tx as any).snapshotLSN).toBeDefined();
    });
  });

  /**
   * GAP: Should support retryable transactions
   * Currently: No built-in retry support for transactions
   */
  it.fails('should support retryable transactions', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    let attempts = 0;

    // GAP: retryableTransaction() should exist
    const result = await (client as any).retryableTransaction?.(
      async (tx: TransactionContext) => {
        attempts++;
        if (attempts < 3) {
          throw new SQLError({
            code: 'TRANSACTION_CONFLICT',
            message: 'Conflict',
          });
        }
        return { success: true };
      },
      { maxRetries: 5 }
    );

    expect(attempts).toBe(3);
    expect(result.success).toBe(true);
  });
});

// =============================================================================
// 6. QUERY() VS EXECUTE() SEMANTICS - GAP: Semantic enforcement
// =============================================================================

describe('Query vs Execute Semantics', () => {
  let originalWebSocket: typeof globalThis.WebSocket;

  beforeEach(() => {
    originalWebSocket = globalThis.WebSocket;
    (globalThis as any).WebSocket = MockWebSocket;
  });

  afterEach(() => {
    (globalThis as any).WebSocket = originalWebSocket;
  });

  /**
   * Existing behavior: query() returns typed results
   */
  it('should have query() method for SELECT', () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    expect(typeof client.query).toBe('function');
  });

  /**
   * GAP: query() should only accept read-only statements
   * Currently: query() accepts any SQL
   */
  it.fails('should validate query() only accepts SELECT', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // These should all throw
    await expect(client.query('INSERT INTO t VALUES (1)')).rejects.toThrow();
    await expect(client.query('UPDATE t SET x = 1')).rejects.toThrow();
    await expect(client.query('DELETE FROM t')).rejects.toThrow();
    await expect(client.query('DROP TABLE t')).rejects.toThrow();
  });

  /**
   * GAP: exec() should not return rows
   * Currently: exec() returns QueryResult with rows array
   */
  it.fails('should not return rows from exec()', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    const result = await client.exec('INSERT INTO users VALUES (1)');

    // Result should only have mutation info, not rows
    expect(result.rows).toBeUndefined();
    expect(result.rowsAffected).toBeDefined();
    expect(result.lastInsertRowId).toBeDefined();
  });

  /**
   * GAP: Should have queryOne() for single row queries
   * Currently: No convenience method for single row
   */
  it.fails('should have queryOne() for single row queries', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: queryOne() should exist
    const user = await (client as any).queryOne?.<{ id: number; name: string }>(
      'SELECT * FROM users WHERE id = ?',
      [1]
    );

    // Returns single object, not array
    expect(user).toBeDefined();
    expect(user.id).toBe(1);
  });

  /**
   * GAP: queryOne() should throw if multiple rows returned
   * Currently: No queryOne() method exists
   */
  it.fails('should throw if queryOne() returns multiple rows', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    await expect(
      (client as any).queryOne?.('SELECT * FROM users')
    ).rejects.toThrow('Query returned multiple rows');
  });

  /**
   * GAP: Should have queryValue() for scalar queries
   * Currently: No convenience method for scalar values
   */
  it.fails('should have queryValue() for scalar queries', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: queryValue() should exist
    const count = await (client as any).queryValue?.<number>(
      'SELECT COUNT(*) FROM users'
    );

    expect(typeof count).toBe('number');
  });

  /**
   * GAP: Should have exists() convenience method
   * Currently: No exists() method
   */
  it.fails('should have exists() convenience method', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: exists() should exist
    const hasUser = await (client as any).exists?.(
      'SELECT 1 FROM users WHERE id = ?',
      [1]
    );

    expect(typeof hasUser).toBe('boolean');
  });
});

// =============================================================================
// 7. WEBSOCKET RECONNECTION - GAP: No automatic reconnection
// =============================================================================

describe('WebSocket Reconnection', () => {
  let originalWebSocket: typeof globalThis.WebSocket;
  let mockWs: MockWebSocket;

  beforeEach(() => {
    originalWebSocket = globalThis.WebSocket;
    (globalThis as any).WebSocket = class extends MockWebSocket {
      constructor(url: string) {
        super(url);
        mockWs = this;
      }
    };
  });

  afterEach(() => {
    (globalThis as any).WebSocket = originalWebSocket;
  });

  /**
   * GAP: Should support auto-reconnect option
   * Currently: No reconnection support
   */
  it.fails('should auto-reconnect when connection is lost', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      // GAP: autoReconnect option should exist
      autoReconnect: true,
    } as any);

    // Connect
    await (client as any).connect?.();

    // Simulate connection loss
    mockWs.simulateClose();

    // Wait for reconnection
    await new Promise((r) => setTimeout(r, 100));

    // Should be reconnected
    expect((client as any).isConnected?.()).toBe(true);
  });

  /**
   * GAP: Should emit reconnecting event
   * Currently: No event system
   */
  it.fails('should emit reconnecting event', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      autoReconnect: true,
    } as any);

    const onReconnecting = vi.fn();
    (client as any).on?.('reconnecting', onReconnecting);

    await (client as any).connect?.();
    mockWs.simulateClose();

    expect(onReconnecting).toHaveBeenCalledWith(
      expect.objectContaining({
        attempt: 1,
        maxAttempts: expect.any(Number),
      })
    );
  });

  /**
   * GAP: Should support reconnect backoff configuration
   * Currently: No reconnection support
   */
  it.fails('should use exponential backoff for reconnection', async () => {
    vi.useFakeTimers();

    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      autoReconnect: true,
      reconnect: {
        maxAttempts: 5,
        baseDelayMs: 1000,
        maxDelayMs: 30000,
      },
    } as any);

    const delays: number[] = [];
    (client as any).on?.('reconnecting', (e: any) => delays.push(e.delayMs));

    await (client as any).connect?.();
    mockWs.simulateClose();

    // Simulate multiple reconnection attempts
    for (let i = 0; i < 3; i++) {
      vi.advanceTimersByTime(delays[delays.length - 1] || 1000);
      mockWs.simulateClose();
    }

    // Delays should increase exponentially
    expect(delays[1]).toBeGreaterThan(delays[0]);
    expect(delays[2]).toBeGreaterThan(delays[1]);

    vi.useRealTimers();
  });

  /**
   * GAP: Should queue requests during reconnection
   * Currently: Requests fail immediately when disconnected
   */
  it.fails('should queue requests during reconnection', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      autoReconnect: true,
      // GAP: queueWhileReconnecting option
      queueWhileReconnecting: true,
    } as any);

    await (client as any).connect?.();
    mockWs.simulateClose();

    // Start query while disconnected
    const queryPromise = client.query('SELECT 1');

    // Simulate reconnection
    mockWs = new MockWebSocket('ws://localhost:8080');

    // Query should succeed after reconnection
    await expect(queryPromise).resolves.toBeDefined();
  });

  /**
   * GAP: Should provide manual reconnect method
   * Currently: No reconnect() method
   */
  it.fails('should provide manual reconnect() method', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    await (client as any).connect?.();
    await client.close();

    // GAP: reconnect() should exist
    await (client as any).reconnect?.();
    expect((client as any).isConnected?.()).toBe(true);
  });

  /**
   * GAP: Should respect max reconnection attempts
   * Currently: No reconnection support
   */
  it.fails('should stop reconnecting after max attempts', async () => {
    vi.useFakeTimers();

    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      autoReconnect: true,
      reconnect: { maxAttempts: 3 },
    } as any);

    const onReconnectFailed = vi.fn();
    (client as any).on?.('reconnectFailed', onReconnectFailed);

    await (client as any).connect?.();

    // Fail connection permanently
    (globalThis as any).WebSocket = class extends MockWebSocket {
      constructor(url: string) {
        super(url);
        setTimeout(() => this.simulateError(new Error('Connection refused')), 10);
      }
    };

    mockWs.simulateClose();

    // Advance through all retry attempts
    for (let i = 0; i < 5; i++) {
      vi.advanceTimersByTime(10000);
    }

    expect(onReconnectFailed).toHaveBeenCalledWith(
      expect.objectContaining({
        attempts: 3,
        lastError: expect.any(Error),
      })
    );

    vi.useRealTimers();
  });
});

// =============================================================================
// 8. ERROR HANDLING FOR NETWORK FAILURES - GAP: Incomplete error handling
// =============================================================================

describe('Network Error Handling', () => {
  let originalWebSocket: typeof globalThis.WebSocket;
  let mockWs: MockWebSocket;

  beforeEach(() => {
    originalWebSocket = globalThis.WebSocket;
    (globalThis as any).WebSocket = class extends MockWebSocket {
      constructor(url: string) {
        super(url);
        mockWs = this;
      }
    };
  });

  afterEach(() => {
    (globalThis as any).WebSocket = originalWebSocket;
  });

  /**
   * Existing behavior: SQLError class exists
   */
  it('should export SQLError class', () => {
    expect(SQLError).toBeDefined();
    expect(typeof SQLError).toBe('function');
  });

  /**
   * IMPLEMENTED: ConnectionError type for connection failures
   */
  it('should have ConnectionError for connection failures', async () => {
    // ConnectionError is exported
    expect(ConnectionError).toBeDefined();

    const error = new ConnectionError('Failed to connect');
    expect(error.code).toBe('CONNECTION_FAILED');
    expect(error.retryable).toBe(true);
  });

  /**
   * IMPLEMENTED: TimeoutError type for request timeouts
   */
  it('should have TimeoutError for timeouts', async () => {
    // TimeoutError is exported
    expect(TimeoutError).toBeDefined();

    const error = new TimeoutError('Request timed out', 30000);
    expect(error.code).toBe('TIMEOUT');
    expect(error.timeoutMs).toBe(30000);
    expect(error.retryable).toBe(true);
  });

  /**
   * GAP: Errors should include request context
   * Currently: Errors don't include request details
   */
  it.fails('should include request context in errors', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // Simulate error
    setTimeout(() => {
      mockWs.simulateMessage({
        id: '1',
        error: {
          code: 'SYNTAX_ERROR',
          message: 'Invalid SQL',
        },
      });
    }, 50);

    try {
      await client.query('INVALID SQL');
    } catch (error: any) {
      // GAP: Request context should be included
      expect(error.request).toBeDefined();
      expect(error.request.sql).toBe('INVALID SQL');
      expect(error.request.method).toBe('query');
    }
  });

  /**
   * GAP: Should categorize errors by retryability
   * Currently: isRetryableError is private
   */
  it.fails('should expose error retryability', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: isRetryable should be on error object or exposed method
    const retryableError = new SQLError({
      code: 'TIMEOUT',
      message: 'Request timed out',
    });
    expect((retryableError as any).retryable).toBe(true);

    const nonRetryableError = new SQLError({
      code: 'SYNTAX_ERROR',
      message: 'Invalid SQL',
    });
    expect((nonRetryableError as any).retryable).toBe(false);
  });

  /**
   * GAP: Should provide error recovery suggestions
   * Currently: No recovery suggestions
   */
  it.fails('should provide error recovery suggestions', async () => {
    const error = new SQLError({
      code: 'CONNECTION_CLOSED',
      message: 'Connection closed unexpectedly',
    });

    // GAP: recoverySuggestion should exist
    expect((error as any).recoverySuggestion).toBeDefined();
    expect(typeof (error as any).recoverySuggestion).toBe('string');
  });

  /**
   * GAP: Should have network error retry with backoff
   * Currently: No automatic retry on network errors
   */
  it.fails('should retry network errors with backoff', async () => {
    let attempts = 0;

    (globalThis as any).WebSocket = class extends MockWebSocket {
      send(data: string): void {
        attempts++;
        if (attempts < 3) {
          setTimeout(() => this.simulateError(new Error('Network error')), 5);
        } else {
          super.send(data);
        }
      }
    };

    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      retry: { maxRetries: 5, baseDelayMs: 10, maxDelayMs: 100 },
    });

    const result = await client.query('SELECT 1');

    expect(attempts).toBe(3);
    expect(result).toBeDefined();
  });

  /**
   * GAP: Should provide detailed network diagnostics
   * Currently: No network diagnostics
   */
  it.fails('should provide network diagnostics', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: getNetworkDiagnostics() should exist
    const diagnostics = await (client as any).getNetworkDiagnostics?.();

    expect(diagnostics).toMatchObject({
      connected: expect.any(Boolean),
      latencyMs: expect.any(Number),
      reconnectAttempts: expect.any(Number),
      lastError: expect.anything(),
      connectionUptime: expect.any(Number),
    });
  });
});

// =============================================================================
// 9. CONNECTION TIMEOUT HANDLING - GAP: No connection-level timeout
// =============================================================================

describe('Connection Timeout Handling', () => {
  let originalWebSocket: typeof globalThis.WebSocket;

  beforeEach(() => {
    originalWebSocket = globalThis.WebSocket;
  });

  afterEach(() => {
    (globalThis as any).WebSocket = originalWebSocket;
  });

  /**
   * GAP: Should have connection timeout option
   * Currently: Only request timeout exists
   */
  it.fails('should support connection timeout', async () => {
    // WebSocket that never connects
    (globalThis as any).WebSocket = class extends MockWebSocket {
      constructor(url: string) {
        super(url);
        // Never emit 'open' event
        this.readyState = MockWebSocket.READY_STATE_CONNECTING;
      }
    };

    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      // GAP: connectionTimeout option should exist (separate from request timeout)
      connectionTimeout: 1000,
    } as any);

    await expect((client as any).connect?.()).rejects.toThrow(
      'Connection timeout after 1000ms'
    );
  });

  /**
   * GAP: Should distinguish connection timeout from request timeout
   * Currently: Same timeout for both
   */
  it.fails('should have separate connection and request timeouts', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      connectionTimeout: 5000, // 5 second connection timeout
      timeout: 30000, // 30 second request timeout
    } as any);

    // GAP: getTimeoutConfig() should exist
    const config = (client as any).getTimeoutConfig?.();

    expect(config.connectionTimeout).toBe(5000);
    expect(config.requestTimeout).toBe(30000);
  });

  /**
   * GAP: Should emit timeout events
   * Currently: No event system for timeouts
   */
  it.fails('should emit connection timeout event', async () => {
    (globalThis as any).WebSocket = class extends MockWebSocket {
      constructor(url: string) {
        super(url);
        this.readyState = MockWebSocket.READY_STATE_CONNECTING;
      }
    };

    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      connectionTimeout: 100,
    } as any);

    const onConnectionTimeout = vi.fn();
    (client as any).on?.('connectionTimeout', onConnectionTimeout);

    try {
      await (client as any).connect?.();
    } catch {
      // Expected to fail
    }

    expect(onConnectionTimeout).toHaveBeenCalledWith(
      expect.objectContaining({
        timeoutMs: 100,
        url: 'ws://localhost:8080',
      })
    );
  });

  /**
   * GAP: Should support idle timeout
   * Currently: No idle timeout to close unused connections
   */
  it.fails('should support idle timeout', async () => {
    vi.useFakeTimers();

    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      // GAP: idleTimeout option should exist
      idleTimeout: 60000, // 1 minute idle timeout
    } as any);

    (globalThis as any).WebSocket = MockWebSocket;

    await (client as any).connect?.();
    expect((client as any).isConnected?.()).toBe(true);

    // Advance time past idle timeout
    vi.advanceTimersByTime(70000);

    // Connection should be closed due to idle timeout
    expect((client as any).isConnected?.()).toBe(false);

    vi.useRealTimers();
  });

  /**
   * GAP: Should support keep-alive pings
   * Currently: No keep-alive support
   */
  it.fails('should support keep-alive pings', async () => {
    vi.useFakeTimers();

    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      // GAP: keepAlive option should exist
      keepAlive: {
        interval: 30000, // Ping every 30 seconds
        timeout: 5000, // 5 second timeout for pong
      },
    } as any);

    (globalThis as any).WebSocket = MockWebSocket;

    await (client as any).connect?.();

    const pingSpy = vi.spyOn(client, 'ping');

    // Advance time to trigger keep-alive
    vi.advanceTimersByTime(35000);

    expect(pingSpy).toHaveBeenCalled();

    vi.useRealTimers();
  });

  /**
   * GAP: Should handle partial response timeout
   * Currently: Only full request timeout
   */
  it.fails('should handle streaming response timeout', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      // GAP: streamingTimeout option for long-running queries
      streamingTimeout: 60000,
    } as any);

    // GAP: queryStreaming() should exist
    await expect(
      (client as any).queryStreaming?.('SELECT * FROM large_table')
    ).resolves.toBeDefined();
  });
});

// =============================================================================
// ADDITIONAL GAPS - Other missing features discovered during analysis
// =============================================================================

describe('Additional Client Features', () => {
  /**
   * GAP: Should support connection pooling
   * Currently: Single connection per client
   */
  it.fails('should support connection pooling', () => {
    // GAP: createPool() should exist
    const pool = (createSQLClient as any).createPool?.({
      url: 'ws://localhost:8080',
      minConnections: 2,
      maxConnections: 10,
    });

    expect(pool).toBeDefined();
    expect(pool.acquire).toBeDefined();
    expect(pool.release).toBeDefined();
  });

  /**
   * GAP: Should support read replicas
   * Currently: Single endpoint only
   */
  it.fails('should support read replicas', () => {
    const client = new DoSQLClient({
      url: 'ws://primary:8080',
      // GAP: readReplicas option should exist
      readReplicas: [
        'ws://replica1:8080',
        'ws://replica2:8080',
      ],
    } as any);

    // GAP: getReplicaStatus() should exist
    const status = (client as any).getReplicaStatus?.();
    expect(status.primary).toBeDefined();
    expect(status.replicas).toHaveLength(2);
  });

  /**
   * GAP: Should expose connection statistics
   * Currently: No statistics exposed
   */
  it.fails('should expose connection statistics', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: getStats() should exist
    const stats = (client as any).getStats?.();

    expect(stats).toMatchObject({
      totalQueries: expect.any(Number),
      totalErrors: expect.any(Number),
      averageLatency: expect.any(Number),
      connectionUptime: expect.any(Number),
    });
  });

  /**
   * GAP: Should support query cancellation
   * Currently: No way to cancel running queries
   */
  it.fails('should support query cancellation', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: AbortController support
    const controller = new AbortController();

    const queryPromise = client.query('SELECT * FROM large_table', [], {
      signal: controller.signal,
    } as any);

    controller.abort();

    await expect(queryPromise).rejects.toThrow('Query cancelled');
  });

  /**
   * GAP: Should support query logging/tracing
   * Currently: No query logging
   */
  it.fails('should support query logging', async () => {
    const queryLog: any[] = [];

    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      // GAP: queryLogger option should exist
      queryLogger: (entry: any) => queryLog.push(entry),
    } as any);

    (globalThis as any).WebSocket = MockWebSocket;

    await client.query('SELECT 1');

    expect(queryLog).toHaveLength(1);
    expect(queryLog[0]).toMatchObject({
      sql: 'SELECT 1',
      startTime: expect.any(Number),
      endTime: expect.any(Number),
      duration: expect.any(Number),
    });
  });
});
