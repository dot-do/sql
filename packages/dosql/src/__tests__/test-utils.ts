/**
 * Test Utilities for dosql Package
 *
 * This module provides type-safe test helpers, mock factories, and fixture creators
 * to replace `as any` type assertions in test files.
 *
 * @packageDocumentation
 */

import type { DurableObjectState, DurableObjectStorage } from '@cloudflare/workers-types';

// =============================================================================
// Mock DO State and Storage
// =============================================================================

/**
 * Mock transaction interface for DO storage
 */
export interface MockStorageTransaction {
  get<T>(key: string): Promise<T | undefined>;
  get<T>(keys: string[]): Promise<Map<string, T>>;
  put<T>(key: string, value: T): Promise<void>;
  put<T>(entries: Map<string, T>): Promise<void>;
  delete(key: string): Promise<boolean>;
  delete(keys: string[]): Promise<number>;
  list<T>(options?: { prefix?: string; limit?: number }): Promise<Map<string, T>>;
  rollback(): void;
}

/**
 * Mock implementation of DurableObjectStorage for testing
 */
export class MockDOStorage implements Partial<DurableObjectStorage> {
  private data = new Map<string, unknown>();

  async get<T = unknown>(key: string): Promise<T | undefined>;
  async get<T = unknown>(keys: string[]): Promise<Map<string, T>>;
  async get<T = unknown>(keyOrKeys: string | string[]): Promise<T | undefined | Map<string, T>> {
    if (Array.isArray(keyOrKeys)) {
      const result = new Map<string, T>();
      for (const key of keyOrKeys) {
        const value = this.data.get(key) as T | undefined;
        if (value !== undefined) {
          result.set(key, value);
        }
      }
      return result;
    }
    return this.data.get(keyOrKeys) as T | undefined;
  }

  async put<T>(key: string, value: T): Promise<void>;
  async put<T>(entries: Map<string, T>): Promise<void>;
  async put<T>(keyOrEntries: string | Map<string, T>, value?: T): Promise<void> {
    if (typeof keyOrEntries === 'string') {
      this.data.set(keyOrEntries, value);
    } else {
      for (const [k, v] of keyOrEntries) {
        this.data.set(k, v);
      }
    }
  }

  async delete(key: string): Promise<boolean>;
  async delete(keys: string[]): Promise<number>;
  async delete(keyOrKeys: string | string[]): Promise<boolean | number> {
    if (Array.isArray(keyOrKeys)) {
      let count = 0;
      for (const key of keyOrKeys) {
        if (this.data.delete(key)) count++;
      }
      return count;
    }
    return this.data.delete(keyOrKeys);
  }

  async list<T = unknown>(options?: { prefix?: string; limit?: number }): Promise<Map<string, T>> {
    const result = new Map<string, T>();
    let count = 0;
    for (const [key, value] of this.data) {
      if (options?.prefix && !key.startsWith(options.prefix)) continue;
      if (options?.limit && count >= options.limit) break;
      result.set(key, value as T);
      count++;
    }
    return result;
  }

  async transaction<T>(closure: (txn: MockStorageTransaction) => Promise<T>): Promise<T> {
    // Simple implementation - doesn't actually provide isolation
    const txn: MockStorageTransaction = {
      get: this.get.bind(this) as MockStorageTransaction['get'],
      put: this.put.bind(this) as MockStorageTransaction['put'],
      delete: this.delete.bind(this) as MockStorageTransaction['delete'],
      list: this.list.bind(this) as MockStorageTransaction['list'],
      rollback: () => {
        // In a real implementation, this would undo changes
      },
    };
    return closure(txn);
  }

  // Test helper methods
  clear(): void {
    this.data.clear();
  }

  getAll(): Map<string, unknown> {
    return new Map(this.data);
  }
}

/**
 * Mock implementation of DurableObjectState for testing
 */
export class MockDOState implements Partial<DurableObjectState> {
  storage: MockDOStorage;
  id: { toString(): string; name?: string };

  constructor(id?: string) {
    this.storage = new MockDOStorage();
    this.id = {
      toString: () => id ?? 'mock-do-id',
      name: id ?? 'mock-do-id',
    };
  }

  blockConcurrencyWhile<T>(callback: () => Promise<T>): Promise<T> {
    return callback();
  }

  waitUntil(_promise: Promise<unknown>): void {
    // No-op in tests
  }
}

/**
 * Creates a mock DO state for testing
 */
export function createMockDOState(id?: string): MockDOState {
  return new MockDOState(id);
}

// =============================================================================
// Trigger Test Utilities
// =============================================================================

/**
 * Valid trigger timing values
 */
export type TriggerTiming = 'before' | 'after' | 'BEFORE' | 'AFTER';

/**
 * Valid trigger event values
 */
export type TriggerEvent = 'insert' | 'update' | 'delete' | 'INSERT' | 'UPDATE' | 'DELETE';

/**
 * Invalid trigger timing for validation tests
 */
export type InvalidTriggerTiming = 'during';

/**
 * Invalid trigger event for validation tests
 */
export type InvalidTriggerEvent = 'create';

/**
 * Trigger definition with explicitly invalid fields for testing validation
 */
export interface InvalidTriggerDefinition {
  name: string;
  table: string;
  timing: TriggerTiming | InvalidTriggerTiming;
  events: (TriggerEvent | InvalidTriggerEvent)[];
  handler: (() => void) | null;
}

/**
 * Creates an invalid trigger definition for validation testing
 */
export function createInvalidTriggerDefinition(
  overrides: Partial<InvalidTriggerDefinition>
): InvalidTriggerDefinition {
  return {
    name: overrides.name ?? 'test_trigger',
    table: overrides.table ?? 'users',
    timing: overrides.timing ?? 'before',
    events: overrides.events ?? ['insert'],
    handler: overrides.handler ?? (() => {}),
  };
}

/**
 * Casts an invalid trigger definition for use with registry.register()
 * This is an explicit controlled cast for testing validation behavior
 */
export function asInvalidTrigger<T>(trigger: InvalidTriggerDefinition): T {
  return trigger as unknown as T;
}

// =============================================================================
// WAL Test Utilities
// =============================================================================

/**
 * Mock WAL entry for testing
 */
export interface MockWALEntry {
  lsn: bigint;
  timestamp: number;
  operation: 'INSERT' | 'UPDATE' | 'DELETE' | 'DDL';
  table: string;
  data: unknown;
}

/**
 * Mock WAL writer interface
 */
export interface MockWALWriter {
  append(entry: MockWALEntry, options?: { sync?: boolean }): Promise<void>;
  flush(): Promise<void>;
  getLastLSN(): bigint;
}

/**
 * Creates a mock WAL writer for testing
 */
export function createMockWALWriter(): MockWALWriter {
  let lastLSN = BigInt(0);
  const entries: MockWALEntry[] = [];

  return {
    async append(entry: MockWALEntry, _options?: { sync?: boolean }): Promise<void> {
      lastLSN = entry.lsn;
      entries.push(entry);
    },
    async flush(): Promise<void> {
      // No-op
    },
    getLastLSN(): bigint {
      return lastLSN;
    },
  };
}

// =============================================================================
// Replication Test Utilities
// =============================================================================

/**
 * Replica info with metadata
 */
export interface ReplicaInfo {
  id: string;
  role: 'primary' | 'replica';
  lsn: bigint;
  lag?: number;
  metadata?: Record<string, unknown>;
}

/**
 * Creates replica info with proper typing
 */
export function createReplicaInfo(
  id: string,
  overrides: Partial<ReplicaInfo> = {}
): ReplicaInfo {
  return {
    id,
    role: overrides.role ?? 'replica',
    lsn: overrides.lsn ?? BigInt(0),
    lag: overrides.lag,
    metadata: overrides.metadata,
  };
}

/**
 * Adds metadata to replica info (type-safe alternative to `info.metadata = ...`)
 */
export function withReplicaMetadata(
  info: ReplicaInfo,
  metadata: Record<string, unknown>
): ReplicaInfo {
  return {
    ...info,
    metadata: { ...info.metadata, ...metadata },
  };
}

// =============================================================================
// Engine Test Utilities
// =============================================================================

/**
 * Mock engine storage interface
 */
export interface MockEngineStorage {
  transaction<T>(closure: (txn: MockStorageTransaction) => Promise<T>): Promise<T>;
}

/**
 * Creates a minimal mock storage for engine tests
 */
export function createMockEngineStorage(): MockEngineStorage {
  const data = new Map<string, unknown>();

  const storage: MockEngineStorage = {
    async transaction<T>(closure: (txn: MockStorageTransaction) => Promise<T>): Promise<T> {
      const txn: MockStorageTransaction = {
        get: async <V>(key: string) => data.get(key) as V | undefined,
        put: async <V>(k: string, v: V) => {
          data.set(k, v);
        },
        delete: async (key: string) => data.delete(key),
        list: async <V>() => data as unknown as Map<string, V>,
        rollback: () => {},
      } as MockStorageTransaction;
      return closure(txn);
    },
  };

  return storage;
}

// =============================================================================
// SQLite Test Utilities
// =============================================================================

/**
 * Column info from SQLite pragma table_info
 */
export interface SQLiteColumnInfo {
  cid: number;
  name: string;
  type: string;
  notnull: number;
  dflt_value: string | null;
  pk: number;
}

/**
 * Index info from SQLite pragma index_list
 */
export interface SQLiteIndexInfo {
  seq: number;
  name: string;
  unique: number;
  origin: string;
  partial: number;
}

/**
 * Type guard for column info
 */
export function isSQLiteColumnInfo(obj: unknown): obj is SQLiteColumnInfo {
  if (typeof obj !== 'object' || obj === null) return false;
  const col = obj as Record<string, unknown>;
  return (
    typeof col.cid === 'number' &&
    typeof col.name === 'string' &&
    typeof col.type === 'string'
  );
}

/**
 * Finds a column in table info results
 */
export function findColumn(
  tableInfo: SQLiteColumnInfo[],
  name: string
): SQLiteColumnInfo | undefined {
  return tableInfo.find((c) => c.name === name);
}

// =============================================================================
// Row Type Utilities
// =============================================================================

/**
 * Generic row type for query results
 */
export interface GenericRow {
  [column: string]: unknown;
}

/**
 * Row with count column (for COUNT queries)
 */
export interface CountRow {
  count: number;
}

/**
 * Row with column names as col0, col1, etc.
 */
export interface IndexedRow {
  col0: unknown;
  col1?: unknown;
  col2?: unknown;
  col3?: unknown;
}

/**
 * Type-safe accessor for row properties
 */
export function getRowValue<T>(row: GenericRow, column: string): T | undefined {
  return row[column] as T | undefined;
}

/**
 * Type-safe accessor for count results
 */
export function getCountValue(row: GenericRow): number {
  const count = row['count'] ?? row['COUNT(*)'] ?? row['count(*)'];
  if (typeof count === 'number') return count;
  if (typeof count === 'bigint') return Number(count);
  if (typeof count === 'string') return parseInt(count, 10);
  return 0;
}

// =============================================================================
// Transaction Test Utilities
// =============================================================================

/**
 * Deadlock event information
 */
export interface DeadlockEvent {
  transactionId: string;
  waitingFor: string;
  timestamp: number;
  chain?: string[];
}

/**
 * Deadlock handler callback type
 */
export type DeadlockHandler = (info: DeadlockEvent) => void;

/**
 * Transaction config with deadlock handler
 */
export interface TransactionConfigWithDeadlockHandler {
  onDeadlock: DeadlockHandler;
  timeout?: number;
}

/**
 * Creates a transaction config with deadlock handler
 */
export function createTransactionConfig(
  onDeadlock: DeadlockHandler
): TransactionConfigWithDeadlockHandler {
  return {
    onDeadlock,
  };
}

// =============================================================================
// WebSocket Hibernation Test Utilities
// =============================================================================

/**
 * Hibernatable WebSocket interface for testing
 */
export interface HibernatableWebSocket {
  id: string;
  readyState: number;
  send(data: string | ArrayBuffer): void;
  close(code?: number, reason?: string): void;
  serializeAttachment(): unknown;
  deserializeAttachment(data: unknown): void;
}

/**
 * Hibernation state for WebSocket
 */
export interface HibernationState {
  wsId: string;
  sessionState: unknown;
  lastActivity: number;
}

/**
 * RPC session state persistence interface
 */
export interface RPCSessionPersistence {
  persistRpcSessionState(wsId: string, state: unknown): Promise<void>;
  restoreRpcSessionState(wsId: string): Promise<unknown>;
}

// =============================================================================
// Benchmark Test Utilities
// =============================================================================

/**
 * Benchmark result type
 */
export interface BenchmarkResult {
  name: string;
  ops: number;
  avgMs: number;
  minMs: number;
  maxMs: number;
  results?: { name: string }[];
}

/**
 * D1 adapter benchmark result
 */
export interface D1BenchmarkResult extends BenchmarkResult {
  results?: { name: string }[];
}

/**
 * Gets index names from D1 results
 */
export function getIndexNames(result: D1BenchmarkResult): string[] {
  return result.results?.map((r) => r.name) ?? [];
}
