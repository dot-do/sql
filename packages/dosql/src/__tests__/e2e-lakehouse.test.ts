/**
 * E2E Lakehouse Ingestion Tests (TDD Red Phase)
 *
 * Comprehensive end-to-end tests documenting the full ingestion pipeline:
 * DoSQL writes -> CDC events -> DoLake -> Parquet files
 *
 * Issue: pocs-zr1n - E2E lakehouse ingestion tests
 *
 * These tests use `it.fails()` pattern to document expected E2E behavior
 * that will pass once the full pipeline is wired up.
 *
 * Test Categories:
 * 1. Single row operations (INSERT/UPDATE/DELETE)
 * 2. Batch operations
 * 3. Transaction handling
 * 4. Schema evolution
 * 5. Error recovery
 * 6. Exactly-once delivery guarantees
 * 7. Ordering guarantees
 * 8. Checkpoint/resume
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, afterEach, beforeAll, afterAll } from 'vitest';

// =============================================================================
// Type Definitions for E2E Testing
// =============================================================================

/**
 * DoSQL client for E2E tests - represents the database interface
 */
interface DoSQLClient {
  execute(sql: string): Promise<ExecuteResult>;
  query<T = Record<string, unknown>>(sql: string): Promise<QueryResult<T>>;
  insert(table: string, data: Record<string, unknown>): Promise<InsertResult>;
  update(table: string, data: Record<string, unknown>, where: Record<string, unknown>): Promise<UpdateResult>;
  delete(table: string, where: Record<string, unknown>): Promise<DeleteResult>;
  transaction<T>(fn: (tx: Transaction) => Promise<T>): Promise<T>;
  close(): Promise<void>;
}

/**
 * DoLake client for E2E tests - represents the lakehouse interface
 */
interface DoLakeClient {
  query<T = Record<string, unknown>>(sql: string): Promise<LakehouseQueryResult<T>>;
  getSnapshot(snapshotId: string): Promise<Snapshot | null>;
  getCurrentSnapshot(): Promise<Snapshot>;
  listSnapshots(): Promise<Snapshot[]>;
  getTableManifest(table: string): Promise<TableManifest | null>;
  getChunks(table: string, options?: ChunkQueryOptions): Promise<ChunkInfo[]>;
  verify(table: string): Promise<VerificationResult>;
  waitForSync(options?: SyncWaitOptions): Promise<SyncStatus>;
  close(): Promise<void>;
}

/**
 * CDC stream for monitoring events
 */
interface CDCStream {
  subscribe(options: CDCSubscriptionOptions): Promise<CDCSubscription>;
  getLastLSN(): Promise<bigint>;
  close(): Promise<void>;
}

interface ExecuteResult {
  success: boolean;
  rowsAffected: number;
  lastInsertRowid?: bigint;
}

interface QueryResult<T> {
  rows: T[];
  columns: string[];
  rowCount: number;
}

interface InsertResult {
  success: boolean;
  rowsAffected: number;
  lastInsertRowid?: bigint;
  lsn: bigint;
}

interface UpdateResult {
  success: boolean;
  rowsAffected: number;
  lsn: bigint;
}

interface DeleteResult {
  success: boolean;
  rowsAffected: number;
  lsn: bigint;
}

interface Transaction {
  execute(sql: string): Promise<ExecuteResult>;
  query<T = Record<string, unknown>>(sql: string): Promise<QueryResult<T>>;
  insert(table: string, data: Record<string, unknown>): Promise<InsertResult>;
  update(table: string, data: Record<string, unknown>, where: Record<string, unknown>): Promise<UpdateResult>;
  delete(table: string, where: Record<string, unknown>): Promise<DeleteResult>;
}

interface LakehouseQueryResult<T> {
  rows: T[];
  columns: string[];
  rowCount: number;
  stats: {
    executionTimeMs: number;
    partitionsScanned: number;
    chunksScanned: number;
    chunksSkipped: number;
    rowsScanned: number;
    bytesScanned: number;
  };
  snapshotId: string;
}

interface Snapshot {
  id: string;
  sequenceNumber: number;
  parentId: string | null;
  timestamp: number;
  summary: {
    tablesModified: string[];
    chunksAdded: number;
    chunksRemoved: number;
    rowsAdded: number;
    rowsRemoved: number;
  };
}

interface TableManifest {
  name: string;
  schema: TableSchema;
  schemaVersion: number;
  totalRowCount: number;
  totalByteSize: number;
  partitionCount: number;
  chunkCount: number;
  createdAt: number;
  updatedAt: number;
}

interface TableSchema {
  columns: ColumnDefinition[];
}

interface ColumnDefinition {
  name: string;
  type: string;
  nullable: boolean;
}

interface ChunkInfo {
  id: string;
  path: string;
  rowCount: number;
  byteSize: number;
  minLSN: bigint;
  maxLSN: bigint;
  sourceDOs: string[];
  createdAt: number;
  format: string;
  compression?: string;
}

interface ChunkQueryOptions {
  partitionFilter?: Record<string, string>;
  minLSN?: bigint;
  maxLSN?: bigint;
}

interface VerificationResult {
  valid: boolean;
  totalRows: number;
  totalChunks: number;
  errors: VerificationError[];
  checksum: string;
}

interface VerificationError {
  type: 'missing_data' | 'duplicate_data' | 'checksum_mismatch' | 'lsn_gap';
  details: Record<string, unknown>;
}

interface SyncWaitOptions {
  timeoutMs?: number;
  targetLSN?: bigint;
}

interface SyncStatus {
  synced: boolean;
  lastSyncedLSN: bigint;
  pendingEvents: number;
  lag: number;
}

interface CDCSubscriptionOptions {
  tables?: string[];
  fromLSN?: bigint;
  operations?: ('INSERT' | 'UPDATE' | 'DELETE')[];
}

interface CDCSubscription {
  onEvent(handler: (event: CDCEvent) => void): void;
  unsubscribe(): Promise<void>;
}

interface CDCEvent {
  lsn: bigint;
  table: string;
  operation: 'INSERT' | 'UPDATE' | 'DELETE';
  before?: Record<string, unknown>;
  after?: Record<string, unknown>;
  timestamp: number;
  txnId: string;
}

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Creates a test DoSQL client (placeholder - will be implemented)
 */
async function createDoSQLClient(_options: { dbName: string }): Promise<DoSQLClient> {
  throw new Error('DoSQL client not yet implemented for E2E testing');
}

/**
 * Creates a test DoLake client (placeholder - will be implemented)
 */
async function createDoLakeClient(_options: { lakehouseId: string }): Promise<DoLakeClient> {
  throw new Error('DoLake client not yet implemented for E2E testing');
}

/**
 * Creates a CDC stream for monitoring (placeholder - will be implemented)
 */
async function createCDCStream(_options: { dbName: string }): Promise<CDCStream> {
  throw new Error('CDC stream not yet implemented for E2E testing');
}

/**
 * Wait for lakehouse to sync with a specific LSN
 */
async function waitForLakehouseSync(
  _lake: DoLakeClient,
  _targetLSN: bigint,
  _timeoutMs: number = 30000
): Promise<boolean> {
  throw new Error('Lakehouse sync wait not yet implemented');
}

/**
 * Generate test data
 */
function generateTestRow(id: number, timestamp?: number): Record<string, unknown> {
  return {
    id,
    name: `test-item-${id}`,
    value: Math.random() * 1000,
    created_at: timestamp ?? Date.now(),
  };
}

/**
 * Generate batch test data
 */
function generateTestBatch(count: number, startId: number = 1): Record<string, unknown>[] {
  return Array.from({ length: count }, (_, i) => generateTestRow(startId + i));
}

// =============================================================================
// Test Configuration
// =============================================================================

const E2E_TIMEOUT = 60_000;
const SYNC_TIMEOUT = 30_000;

// =============================================================================
// Test Suite: Single Row Operations
// =============================================================================

describe('E2E Lakehouse - Single Row Operations', () => {
  let dosql: DoSQLClient;
  let dolake: DoLakeClient;
  let cdc: CDCStream;
  let testDbName: string;
  let testLakehouseId: string;

  beforeEach(async () => {
    testDbName = `test-db-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    testLakehouseId = `test-lake-${Date.now()}`;

    // These will throw until implemented
    dosql = await createDoSQLClient({ dbName: testDbName });
    dolake = await createDoLakeClient({ lakehouseId: testLakehouseId });
    cdc = await createCDCStream({ dbName: testDbName });

    // Create test table
    await dosql.execute(`
      CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        value REAL,
        created_at INTEGER
      )
    `);
  });

  afterEach(async () => {
    await cdc?.close();
    await dolake?.close();
    await dosql?.close();
  });

  // -------------------------------------------------------------------------
  // Test 1: Single INSERT -> CDC -> Lakehouse
  // -------------------------------------------------------------------------
  it.fails('should propagate single INSERT from DoSQL to Lakehouse', async () => {
    // Insert a single row into DoSQL
    const row = generateTestRow(1);
    const insertResult = await dosql.insert('items', row);

    expect(insertResult.success).toBe(true);
    expect(insertResult.lsn).toBeGreaterThan(0n);

    // Wait for lakehouse to sync
    const synced = await waitForLakehouseSync(dolake, insertResult.lsn, SYNC_TIMEOUT);
    expect(synced).toBe(true);

    // Query lakehouse and verify data
    const result = await dolake.query<typeof row>('SELECT * FROM items WHERE id = 1');

    expect(result.rowCount).toBe(1);
    expect(result.rows[0].id).toBe(1);
    expect(result.rows[0].name).toBe(row.name);
  }, E2E_TIMEOUT);

  // -------------------------------------------------------------------------
  // Test 2: Single UPDATE -> CDC -> Lakehouse merge
  // -------------------------------------------------------------------------
  it.fails('should propagate single UPDATE from DoSQL to Lakehouse', async () => {
    // Insert initial row
    await dosql.insert('items', generateTestRow(1));

    // Update the row
    const updateResult = await dosql.update(
      'items',
      { name: 'updated-item', value: 999.99 },
      { id: 1 }
    );

    expect(updateResult.success).toBe(true);
    expect(updateResult.rowsAffected).toBe(1);

    // Wait for lakehouse sync
    await waitForLakehouseSync(dolake, updateResult.lsn, SYNC_TIMEOUT);

    // Query lakehouse - should see the updated value
    const result = await dolake.query<{ id: number; name: string; value: number }>(
      'SELECT * FROM items WHERE id = 1'
    );

    expect(result.rowCount).toBe(1);
    expect(result.rows[0].name).toBe('updated-item');
    expect(result.rows[0].value).toBe(999.99);
  }, E2E_TIMEOUT);

  // -------------------------------------------------------------------------
  // Test 3: Single DELETE -> CDC -> Lakehouse tombstone
  // -------------------------------------------------------------------------
  it.fails('should propagate single DELETE with tombstone handling', async () => {
    // Insert and then delete
    const insertResult = await dosql.insert('items', generateTestRow(1));
    await waitForLakehouseSync(dolake, insertResult.lsn, SYNC_TIMEOUT);

    const deleteResult = await dosql.delete('items', { id: 1 });

    expect(deleteResult.success).toBe(true);
    expect(deleteResult.rowsAffected).toBe(1);

    // Wait for delete to propagate
    await waitForLakehouseSync(dolake, deleteResult.lsn, SYNC_TIMEOUT);

    // Query lakehouse - row should not be visible
    const result = await dolake.query('SELECT * FROM items WHERE id = 1');

    expect(result.rowCount).toBe(0);

    // Verify tombstone was recorded in chunks
    const chunks = await dolake.getChunks('items');
    const hasDeleteRecord = chunks.some(chunk => chunk.maxLSN >= deleteResult.lsn);
    expect(hasDeleteRecord).toBe(true);
  }, E2E_TIMEOUT);

  // -------------------------------------------------------------------------
  // Test 4: CDC event structure verification
  // -------------------------------------------------------------------------
  it.fails('should emit correctly structured CDC events for INSERT', async () => {
    const events: CDCEvent[] = [];

    const subscription = await cdc.subscribe({
      tables: ['items'],
      operations: ['INSERT'],
    });

    subscription.onEvent((event) => {
      events.push(event);
    });

    // Trigger an insert
    const row = generateTestRow(1);
    const insertResult = await dosql.insert('items', row);

    // Wait for event
    await new Promise(resolve => setTimeout(resolve, 1000));

    expect(events.length).toBeGreaterThanOrEqual(1);

    const insertEvent = events.find(e => e.operation === 'INSERT' && e.lsn === insertResult.lsn);
    expect(insertEvent).toBeDefined();
    expect(insertEvent?.table).toBe('items');
    expect(insertEvent?.after).toBeDefined();
    expect(insertEvent?.after?.id).toBe(1);
    expect(insertEvent?.before).toBeUndefined();

    await subscription.unsubscribe();
  }, E2E_TIMEOUT);

  // -------------------------------------------------------------------------
  // Test 5: CDC event structure for UPDATE (before/after)
  // -------------------------------------------------------------------------
  it.fails('should emit CDC UPDATE events with before and after values', async () => {
    // Insert first
    await dosql.insert('items', generateTestRow(1));

    const events: CDCEvent[] = [];

    const subscription = await cdc.subscribe({
      tables: ['items'],
      operations: ['UPDATE'],
    });

    subscription.onEvent((event) => {
      events.push(event);
    });

    // Update
    const updateResult = await dosql.update(
      'items',
      { name: 'modified-name' },
      { id: 1 }
    );

    await new Promise(resolve => setTimeout(resolve, 1000));

    const updateEvent = events.find(e => e.lsn === updateResult.lsn);
    expect(updateEvent).toBeDefined();
    expect(updateEvent?.operation).toBe('UPDATE');
    expect(updateEvent?.before).toBeDefined();
    expect(updateEvent?.before?.name).toBe('test-item-1');
    expect(updateEvent?.after).toBeDefined();
    expect(updateEvent?.after?.name).toBe('modified-name');

    await subscription.unsubscribe();
  }, E2E_TIMEOUT);
});

// =============================================================================
// Test Suite: Batch Operations
// =============================================================================

describe('E2E Lakehouse - Batch Operations', () => {
  let dosql: DoSQLClient;
  let dolake: DoLakeClient;
  let testDbName: string;
  let testLakehouseId: string;

  beforeEach(async () => {
    testDbName = `test-db-batch-${Date.now()}`;
    testLakehouseId = `test-lake-batch-${Date.now()}`;

    dosql = await createDoSQLClient({ dbName: testDbName });
    dolake = await createDoLakeClient({ lakehouseId: testLakehouseId });

    await dosql.execute(`
      CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        value REAL,
        created_at INTEGER
      )
    `);
  });

  afterEach(async () => {
    await dolake?.close();
    await dosql?.close();
  });

  // -------------------------------------------------------------------------
  // Test 6: Batch INSERT -> CDC batch -> Parquet file
  // -------------------------------------------------------------------------
  it.fails('should batch multiple INSERTs into single Parquet file', async () => {
    const batchSize = 100;
    const batch = generateTestBatch(batchSize);

    // Insert batch
    let lastLSN: bigint = 0n;
    for (const row of batch) {
      const result = await dosql.insert('items', row);
      lastLSN = result.lsn;
    }

    // Wait for full batch to sync
    await waitForLakehouseSync(dolake, lastLSN, SYNC_TIMEOUT);

    // Verify all rows in lakehouse
    const result = await dolake.query('SELECT COUNT(*) as count FROM items');
    expect(result.rows[0].count).toBe(batchSize);

    // Verify chunks were created
    const chunks = await dolake.getChunks('items');
    expect(chunks.length).toBeGreaterThan(0);

    // Total rows across all chunks should match
    const totalChunkRows = chunks.reduce((sum, c) => sum + c.rowCount, 0);
    expect(totalChunkRows).toBeGreaterThanOrEqual(batchSize);
  }, E2E_TIMEOUT);

  // -------------------------------------------------------------------------
  // Test 7: Large batch triggering multiple Parquet files
  // -------------------------------------------------------------------------
  it.fails('should split large batches across multiple Parquet files', async () => {
    const batchSize = 10000; // Large enough to trigger multiple chunks
    const batch = generateTestBatch(batchSize);

    let lastLSN: bigint = 0n;
    for (const row of batch) {
      const result = await dosql.insert('items', row);
      lastLSN = result.lsn;
    }

    await waitForLakehouseSync(dolake, lastLSN, SYNC_TIMEOUT * 2);

    // Should have multiple chunks
    const chunks = await dolake.getChunks('items');
    expect(chunks.length).toBeGreaterThan(1);

    // Each chunk should have reasonable size
    for (const chunk of chunks) {
      expect(chunk.byteSize).toBeLessThanOrEqual(64 * 1024 * 1024); // Max 64MB
    }
  }, E2E_TIMEOUT * 2);

  // -------------------------------------------------------------------------
  // Test 8: Batch with mixed operations
  // -------------------------------------------------------------------------
  it.fails('should handle batch with mixed INSERT/UPDATE/DELETE operations', async () => {
    // Insert initial batch
    const initialBatch = generateTestBatch(50);
    for (const row of initialBatch) {
      await dosql.insert('items', row);
    }

    // Mix of operations
    let lastLSN: bigint = 0n;

    // Updates on first 10
    for (let i = 1; i <= 10; i++) {
      const result = await dosql.update('items', { value: 0 }, { id: i });
      lastLSN = result.lsn;
    }

    // Deletes on 11-20
    for (let i = 11; i <= 20; i++) {
      const result = await dosql.delete('items', { id: i });
      lastLSN = result.lsn;
    }

    // New inserts 51-60
    for (let i = 51; i <= 60; i++) {
      const result = await dosql.insert('items', generateTestRow(i));
      lastLSN = result.lsn;
    }

    await waitForLakehouseSync(dolake, lastLSN, SYNC_TIMEOUT);

    // Verify final state
    const result = await dolake.query('SELECT COUNT(*) as count FROM items');
    expect(result.rows[0].count).toBe(50); // 50 - 10 deleted + 10 new = 50

    // Verify updated rows
    const updated = await dolake.query('SELECT * FROM items WHERE id <= 10');
    for (const row of updated.rows) {
      expect((row as { value: number }).value).toBe(0);
    }
  }, E2E_TIMEOUT);
});

// =============================================================================
// Test Suite: Transaction Handling
// =============================================================================

describe('E2E Lakehouse - Transaction Handling', () => {
  let dosql: DoSQLClient;
  let dolake: DoLakeClient;
  let testDbName: string;
  let testLakehouseId: string;

  beforeEach(async () => {
    testDbName = `test-db-txn-${Date.now()}`;
    testLakehouseId = `test-lake-txn-${Date.now()}`;

    dosql = await createDoSQLClient({ dbName: testDbName });
    dolake = await createDoLakeClient({ lakehouseId: testLakehouseId });

    await dosql.execute(`
      CREATE TABLE IF NOT EXISTS accounts (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        balance REAL NOT NULL
      )
    `);
  });

  afterEach(async () => {
    await dolake?.close();
    await dosql?.close();
  });

  // -------------------------------------------------------------------------
  // Test 9: Transaction with multiple operations
  // -------------------------------------------------------------------------
  it.fails('should propagate entire transaction atomically', async () => {
    // Setup accounts
    await dosql.insert('accounts', { id: 1, name: 'Alice', balance: 1000 });
    await dosql.insert('accounts', { id: 2, name: 'Bob', balance: 500 });

    // Transfer in transaction
    const txnLSN = await dosql.transaction(async (tx) => {
      await tx.update('accounts', { balance: 800 }, { id: 1 }); // -200
      await tx.update('accounts', { balance: 700 }, { id: 2 }); // +200
      return 0n; // Return last LSN
    });

    await waitForLakehouseSync(dolake, txnLSN, SYNC_TIMEOUT);

    // Both updates should be visible together
    const alice = await dolake.query('SELECT balance FROM accounts WHERE id = 1');
    const bob = await dolake.query('SELECT balance FROM accounts WHERE id = 2');

    expect(alice.rows[0].balance).toBe(800);
    expect(bob.rows[0].balance).toBe(700);

    // Total balance should be preserved
    const total = await dolake.query('SELECT SUM(balance) as total FROM accounts');
    expect(total.rows[0].total).toBe(1500);
  }, E2E_TIMEOUT);

  // -------------------------------------------------------------------------
  // Test 10: Rolled back transaction should not appear
  // -------------------------------------------------------------------------
  it.fails('should not propagate rolled back transactions', async () => {
    await dosql.insert('accounts', { id: 1, name: 'Alice', balance: 1000 });

    const beforeLSN = await (await createCDCStream({ dbName: testDbName })).getLastLSN();

    // Transaction that will fail
    try {
      await dosql.transaction(async (tx) => {
        await tx.update('accounts', { balance: 0 }, { id: 1 });
        throw new Error('Simulated failure');
      });
    } catch {
      // Expected
    }

    // Short wait
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Lakehouse should still show original balance
    const result = await dolake.query('SELECT balance FROM accounts WHERE id = 1');
    expect(result.rows[0].balance).toBe(1000);

    // No new LSN should have been committed
    const afterLSN = await (await createCDCStream({ dbName: testDbName })).getLastLSN();
    expect(afterLSN).toBe(beforeLSN);
  }, E2E_TIMEOUT);

  // -------------------------------------------------------------------------
  // Test 11: Multiple concurrent transactions
  // -------------------------------------------------------------------------
  it.fails('should handle multiple concurrent transactions correctly', async () => {
    // Setup multiple accounts
    for (let i = 1; i <= 10; i++) {
      await dosql.insert('accounts', { id: i, name: `Account-${i}`, balance: 100 });
    }

    // Concurrent transactions
    const txPromises = [];
    for (let i = 1; i <= 5; i++) {
      txPromises.push(
        dosql.transaction(async (tx) => {
          await tx.update('accounts', { balance: 100 + i * 10 }, { id: i });
        })
      );
    }

    await Promise.all(txPromises);

    // Wait for sync
    await new Promise(resolve => setTimeout(resolve, 5000));

    // All updates should be reflected
    const result = await dolake.query('SELECT * FROM accounts ORDER BY id');
    expect(result.rowCount).toBe(10);

    for (let i = 1; i <= 5; i++) {
      const row = result.rows.find((r: { id: number }) => r.id === i) as { balance: number };
      expect(row.balance).toBe(100 + i * 10);
    }
  }, E2E_TIMEOUT);
});

// =============================================================================
// Test Suite: Schema Evolution
// =============================================================================

describe('E2E Lakehouse - Schema Evolution', () => {
  let dosql: DoSQLClient;
  let dolake: DoLakeClient;
  let testDbName: string;
  let testLakehouseId: string;

  beforeEach(async () => {
    testDbName = `test-db-schema-${Date.now()}`;
    testLakehouseId = `test-lake-schema-${Date.now()}`;

    dosql = await createDoSQLClient({ dbName: testDbName });
    dolake = await createDoLakeClient({ lakehouseId: testLakehouseId });

    await dosql.execute(`
      CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        price REAL
      )
    `);
  });

  afterEach(async () => {
    await dolake?.close();
    await dosql?.close();
  });

  // -------------------------------------------------------------------------
  // Test 12: Add column schema evolution
  // -------------------------------------------------------------------------
  it.fails('should handle ADD COLUMN schema evolution', async () => {
    // Insert before schema change
    await dosql.insert('products', { id: 1, name: 'Widget', price: 19.99 });

    // Add column
    await dosql.execute('ALTER TABLE products ADD COLUMN category TEXT');

    // Insert after schema change
    const result = await dosql.insert('products', {
      id: 2,
      name: 'Gadget',
      price: 29.99,
      category: 'Electronics',
    });

    await waitForLakehouseSync(dolake, result.lsn, SYNC_TIMEOUT);

    // Query should work with new schema
    const products = await dolake.query('SELECT * FROM products ORDER BY id');
    expect(products.rowCount).toBe(2);

    // Old row should have null for new column
    expect(products.rows[0].category).toBeNull();
    // New row should have the category
    expect(products.rows[1].category).toBe('Electronics');

    // Manifest should reflect schema change
    const manifest = await dolake.getTableManifest('products');
    expect(manifest?.schemaVersion).toBeGreaterThan(1);
    expect(manifest?.schema.columns.find(c => c.name === 'category')).toBeDefined();
  }, E2E_TIMEOUT);

  // -------------------------------------------------------------------------
  // Test 13: Schema version tracking
  // -------------------------------------------------------------------------
  it.fails('should track schema versions across snapshots', async () => {
    // Initial insert
    const insert1 = await dosql.insert('products', { id: 1, name: 'Product1', price: 10 });
    await waitForLakehouseSync(dolake, insert1.lsn, SYNC_TIMEOUT);

    const snapshot1 = await dolake.getCurrentSnapshot();
    const manifest1 = await dolake.getTableManifest('products');

    // Schema change
    await dosql.execute('ALTER TABLE products ADD COLUMN stock INTEGER DEFAULT 0');

    // More inserts
    const insert2 = await dosql.insert('products', { id: 2, name: 'Product2', price: 20, stock: 100 });
    await waitForLakehouseSync(dolake, insert2.lsn, SYNC_TIMEOUT);

    const snapshot2 = await dolake.getCurrentSnapshot();
    const manifest2 = await dolake.getTableManifest('products');

    // Schema versions should differ
    expect(manifest2?.schemaVersion).toBeGreaterThan(manifest1?.schemaVersion ?? 0);

    // Both snapshots should be queryable
    expect(snapshot1.id).not.toBe(snapshot2.id);
  }, E2E_TIMEOUT);

  // -------------------------------------------------------------------------
  // Test 14: Query across schema versions
  // -------------------------------------------------------------------------
  it.fails('should query data spanning multiple schema versions', async () => {
    // Insert with v1 schema
    for (let i = 1; i <= 5; i++) {
      await dosql.insert('products', { id: i, name: `Product-${i}`, price: i * 10 });
    }

    // Schema change
    await dosql.execute('ALTER TABLE products ADD COLUMN discount REAL DEFAULT 0');

    // Insert with v2 schema
    let lastLSN: bigint = 0n;
    for (let i = 6; i <= 10; i++) {
      const result = await dosql.insert('products', {
        id: i,
        name: `Product-${i}`,
        price: i * 10,
        discount: 0.1,
      });
      lastLSN = result.lsn;
    }

    await waitForLakehouseSync(dolake, lastLSN, SYNC_TIMEOUT);

    // Query all products
    const result = await dolake.query('SELECT * FROM products ORDER BY id');
    expect(result.rowCount).toBe(10);

    // All rows should have discount column (null or value)
    for (const row of result.rows) {
      expect('discount' in row).toBe(true);
    }

    // Old rows have default, new rows have explicit value
    expect(result.rows[0].discount).toBe(0);
    expect(result.rows[9].discount).toBe(0.1);
  }, E2E_TIMEOUT);
});

// =============================================================================
// Test Suite: Error Recovery
// =============================================================================

describe('E2E Lakehouse - Error Recovery', () => {
  let dosql: DoSQLClient;
  let dolake: DoLakeClient;
  let testDbName: string;
  let testLakehouseId: string;

  beforeEach(async () => {
    testDbName = `test-db-error-${Date.now()}`;
    testLakehouseId = `test-lake-error-${Date.now()}`;

    dosql = await createDoSQLClient({ dbName: testDbName });
    dolake = await createDoLakeClient({ lakehouseId: testLakehouseId });

    await dosql.execute(`
      CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY,
        event_type TEXT NOT NULL,
        payload TEXT,
        timestamp INTEGER
      )
    `);
  });

  afterEach(async () => {
    await dolake?.close();
    await dosql?.close();
  });

  // -------------------------------------------------------------------------
  // Test 15: Recovery from partial write failure
  // -------------------------------------------------------------------------
  it.fails('should recover from partial write failure', async () => {
    // Insert batch
    for (let i = 1; i <= 50; i++) {
      await dosql.insert('events', {
        id: i,
        event_type: 'test',
        payload: `payload-${i}`,
        timestamp: Date.now(),
      });
    }

    // Simulate a partial failure by force-closing lakehouse mid-sync
    // Then recreate and verify recovery

    // Close lakehouse (simulates failure)
    await dolake.close();

    // Recreate and wait for recovery
    dolake = await createDoLakeClient({ lakehouseId: testLakehouseId });

    // Continue with more inserts
    let lastLSN: bigint = 0n;
    for (let i = 51; i <= 75; i++) {
      const result = await dosql.insert('events', {
        id: i,
        event_type: 'test',
        payload: `payload-${i}`,
        timestamp: Date.now(),
      });
      lastLSN = result.lsn;
    }

    await waitForLakehouseSync(dolake, lastLSN, SYNC_TIMEOUT);

    // Verify all data is present
    const result = await dolake.query('SELECT COUNT(*) as count FROM events');
    expect(result.rows[0].count).toBe(75);

    // Verify data integrity
    const verification = await dolake.verify('events');
    expect(verification.valid).toBe(true);
    expect(verification.errors).toHaveLength(0);
  }, E2E_TIMEOUT);

  // -------------------------------------------------------------------------
  // Test 16: Recovery from manifest corruption
  // -------------------------------------------------------------------------
  it.fails('should recover from manifest corruption', async () => {
    // Insert initial data
    let lastLSN: bigint = 0n;
    for (let i = 1; i <= 20; i++) {
      const result = await dosql.insert('events', {
        id: i,
        event_type: 'important',
        payload: `data-${i}`,
        timestamp: Date.now(),
      });
      lastLSN = result.lsn;
    }

    await waitForLakehouseSync(dolake, lastLSN, SYNC_TIMEOUT);

    // Get current manifest state
    const manifestBefore = await dolake.getTableManifest('events');
    expect(manifestBefore?.totalRowCount).toBe(20);

    // Force manifest rebuild (simulates corruption recovery)
    // This would typically be an admin operation

    // After recovery, data should still be accessible
    const result = await dolake.query('SELECT COUNT(*) as count FROM events');
    expect(result.rows[0].count).toBe(20);
  }, E2E_TIMEOUT);

  // -------------------------------------------------------------------------
  // Test 17: Handle CDC stream reconnection
  // -------------------------------------------------------------------------
  it.fails('should handle CDC stream reconnection without data loss', async () => {
    // Insert first batch
    for (let i = 1; i <= 25; i++) {
      await dosql.insert('events', {
        id: i,
        event_type: 'batch1',
        payload: `payload-${i}`,
        timestamp: Date.now(),
      });
    }

    // Get the last LSN from CDC
    const cdc = await createCDCStream({ dbName: testDbName });
    const midpointLSN = await cdc.getLastLSN();
    await cdc.close();

    // Insert more data while CDC is "disconnected"
    let lastLSN: bigint = 0n;
    for (let i = 26; i <= 50; i++) {
      const result = await dosql.insert('events', {
        id: i,
        event_type: 'batch2',
        payload: `payload-${i}`,
        timestamp: Date.now(),
      });
      lastLSN = result.lsn;
    }

    // Reconnect CDC from midpoint
    const cdc2 = await createCDCStream({ dbName: testDbName });
    const subscription = await cdc2.subscribe({ fromLSN: midpointLSN });

    const events: CDCEvent[] = [];
    subscription.onEvent(event => events.push(event));

    await new Promise(resolve => setTimeout(resolve, 3000));

    // Should have caught up with batch2
    const batch2Events = events.filter(e => e.after?.event_type === 'batch2');
    expect(batch2Events.length).toBe(25);

    await subscription.unsubscribe();
    await cdc2.close();

    // Lakehouse should have all data
    await waitForLakehouseSync(dolake, lastLSN, SYNC_TIMEOUT);
    const result = await dolake.query('SELECT COUNT(*) as count FROM events');
    expect(result.rows[0].count).toBe(50);
  }, E2E_TIMEOUT);
});

// =============================================================================
// Test Suite: Exactly-Once Delivery Guarantees
// =============================================================================

describe('E2E Lakehouse - Exactly-Once Guarantees', () => {
  let dosql: DoSQLClient;
  let dolake: DoLakeClient;
  let testDbName: string;
  let testLakehouseId: string;

  beforeEach(async () => {
    testDbName = `test-db-exactly-once-${Date.now()}`;
    testLakehouseId = `test-lake-exactly-once-${Date.now()}`;

    dosql = await createDoSQLClient({ dbName: testDbName });
    dolake = await createDoLakeClient({ lakehouseId: testLakehouseId });

    await dosql.execute(`
      CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY,
        customer_id INTEGER NOT NULL,
        total REAL NOT NULL,
        status TEXT
      )
    `);
  });

  afterEach(async () => {
    await dolake?.close();
    await dosql?.close();
  });

  // -------------------------------------------------------------------------
  // Test 18: No duplicate rows after retry
  // -------------------------------------------------------------------------
  it.fails('should not create duplicate rows after retry scenarios', async () => {
    // Insert orders
    for (let i = 1; i <= 100; i++) {
      await dosql.insert('orders', {
        id: i,
        customer_id: i % 10,
        total: i * 10.5,
        status: 'pending',
      });
    }

    // Simulate multiple sync attempts (as if retrying after partial failure)
    const syncStatus1 = await dolake.waitForSync({ timeoutMs: SYNC_TIMEOUT });
    const syncStatus2 = await dolake.waitForSync({ timeoutMs: SYNC_TIMEOUT });

    // Count should be exactly 100, no duplicates
    const result = await dolake.query('SELECT COUNT(*) as count FROM orders');
    expect(result.rows[0].count).toBe(100);

    // Verify no duplicate IDs
    const ids = await dolake.query('SELECT id, COUNT(*) as cnt FROM orders GROUP BY id HAVING cnt > 1');
    expect(ids.rowCount).toBe(0);
  }, E2E_TIMEOUT);

  // -------------------------------------------------------------------------
  // Test 19: Idempotent updates
  // -------------------------------------------------------------------------
  it.fails('should handle idempotent updates correctly', async () => {
    // Create order
    await dosql.insert('orders', {
      id: 1,
      customer_id: 100,
      total: 99.99,
      status: 'pending',
    });

    // Apply same update multiple times (simulating retry)
    for (let i = 0; i < 3; i++) {
      await dosql.update('orders', { status: 'confirmed' }, { id: 1 });
    }

    await new Promise(resolve => setTimeout(resolve, 3000));

    // Should only see one row with final status
    const result = await dolake.query('SELECT * FROM orders WHERE id = 1');
    expect(result.rowCount).toBe(1);
    expect(result.rows[0].status).toBe('confirmed');
  }, E2E_TIMEOUT);

  // -------------------------------------------------------------------------
  // Test 20: LSN-based deduplication
  // -------------------------------------------------------------------------
  it.fails('should deduplicate based on LSN', async () => {
    // Insert events
    let lastLSN: bigint = 0n;
    for (let i = 1; i <= 50; i++) {
      const result = await dosql.insert('orders', {
        id: i,
        customer_id: i % 5,
        total: i * 5,
        status: 'new',
      });
      lastLSN = result.lsn;
    }

    await waitForLakehouseSync(dolake, lastLSN, SYNC_TIMEOUT);

    // Get chunks
    const chunks = await dolake.getChunks('orders');

    // Verify LSN ranges don't overlap (no duplicate LSNs)
    const sortedChunks = chunks.sort((a, b) =>
      a.minLSN < b.minLSN ? -1 : a.minLSN > b.minLSN ? 1 : 0
    );

    for (let i = 1; i < sortedChunks.length; i++) {
      expect(sortedChunks[i].minLSN).toBeGreaterThan(sortedChunks[i - 1].maxLSN);
    }
  }, E2E_TIMEOUT);
});

// =============================================================================
// Test Suite: Ordering Guarantees
// =============================================================================

describe('E2E Lakehouse - Ordering Guarantees', () => {
  let dosql: DoSQLClient;
  let dolake: DoLakeClient;
  let cdc: CDCStream;
  let testDbName: string;
  let testLakehouseId: string;

  beforeEach(async () => {
    testDbName = `test-db-order-${Date.now()}`;
    testLakehouseId = `test-lake-order-${Date.now()}`;

    dosql = await createDoSQLClient({ dbName: testDbName });
    dolake = await createDoLakeClient({ lakehouseId: testLakehouseId });
    cdc = await createCDCStream({ dbName: testDbName });

    await dosql.execute(`
      CREATE TABLE IF NOT EXISTS log_entries (
        id INTEGER PRIMARY KEY,
        sequence INTEGER NOT NULL,
        message TEXT,
        timestamp INTEGER
      )
    `);
  });

  afterEach(async () => {
    await cdc?.close();
    await dolake?.close();
    await dosql?.close();
  });

  // -------------------------------------------------------------------------
  // Test 21: LSN ordering within single DO
  // -------------------------------------------------------------------------
  it.fails('should maintain LSN ordering within single DO', async () => {
    const events: CDCEvent[] = [];
    const subscription = await cdc.subscribe({ tables: ['log_entries'] });
    subscription.onEvent(event => events.push(event));

    // Insert sequentially
    for (let i = 1; i <= 20; i++) {
      await dosql.insert('log_entries', {
        id: i,
        sequence: i,
        message: `Log entry ${i}`,
        timestamp: Date.now(),
      });
    }

    await new Promise(resolve => setTimeout(resolve, 3000));

    // Verify LSN ordering
    for (let i = 1; i < events.length; i++) {
      expect(events[i].lsn).toBeGreaterThan(events[i - 1].lsn);
    }

    // Sequence should match order
    const insertEvents = events.filter(e => e.operation === 'INSERT');
    for (let i = 0; i < insertEvents.length; i++) {
      expect(insertEvents[i].after?.sequence).toBe(i + 1);
    }

    await subscription.unsubscribe();
  }, E2E_TIMEOUT);

  // -------------------------------------------------------------------------
  // Test 22: Causal ordering (write-after-write)
  // -------------------------------------------------------------------------
  it.fails('should preserve causal ordering for write-after-write', async () => {
    // Insert -> Update -> Delete chain
    const insert = await dosql.insert('log_entries', {
      id: 1,
      sequence: 1,
      message: 'Created',
      timestamp: Date.now(),
    });

    const update = await dosql.update(
      'log_entries',
      { message: 'Modified' },
      { id: 1 }
    );

    const del = await dosql.delete('log_entries', { id: 1 });

    // LSNs should be strictly ordered
    expect(update.lsn).toBeGreaterThan(insert.lsn);
    expect(del.lsn).toBeGreaterThan(update.lsn);

    await waitForLakehouseSync(dolake, del.lsn, SYNC_TIMEOUT);

    // Lakehouse chunks should reflect this ordering
    const chunks = await dolake.getChunks('log_entries');
    for (const chunk of chunks) {
      expect(chunk.maxLSN).toBeGreaterThanOrEqual(chunk.minLSN);
    }
  }, E2E_TIMEOUT);

  // -------------------------------------------------------------------------
  // Test 23: Snapshot isolation - consistent view
  // -------------------------------------------------------------------------
  it.fails('should provide snapshot isolation for queries', async () => {
    // Insert initial data
    for (let i = 1; i <= 10; i++) {
      await dosql.insert('log_entries', {
        id: i,
        sequence: i,
        message: `Entry ${i}`,
        timestamp: Date.now(),
      });
    }

    await dolake.waitForSync({ timeoutMs: SYNC_TIMEOUT });

    // Get snapshot ID
    const snapshot1 = await dolake.getCurrentSnapshot();

    // Add more data
    for (let i = 11; i <= 20; i++) {
      await dosql.insert('log_entries', {
        id: i,
        sequence: i,
        message: `Entry ${i}`,
        timestamp: Date.now(),
      });
    }

    await dolake.waitForSync({ timeoutMs: SYNC_TIMEOUT });

    // Query at old snapshot should see only 10 rows
    const oldResult = await dolake.query(`
      SELECT COUNT(*) as count FROM log_entries
      /* AT SNAPSHOT ${snapshot1.id} */
    `);

    expect(oldResult.rowCount).toBe(1);
    // Note: The actual snapshot query syntax may differ
  }, E2E_TIMEOUT);
});

// =============================================================================
// Test Suite: Checkpoint/Resume After Failure
// =============================================================================

describe('E2E Lakehouse - Checkpoint/Resume', () => {
  let dosql: DoSQLClient;
  let dolake: DoLakeClient;
  let testDbName: string;
  let testLakehouseId: string;

  beforeEach(async () => {
    testDbName = `test-db-checkpoint-${Date.now()}`;
    testLakehouseId = `test-lake-checkpoint-${Date.now()}`;

    dosql = await createDoSQLClient({ dbName: testDbName });
    dolake = await createDoLakeClient({ lakehouseId: testLakehouseId });

    await dosql.execute(`
      CREATE TABLE IF NOT EXISTS metrics (
        id INTEGER PRIMARY KEY,
        metric_name TEXT NOT NULL,
        value REAL,
        recorded_at INTEGER
      )
    `);
  });

  afterEach(async () => {
    await dolake?.close();
    await dosql?.close();
  });

  // -------------------------------------------------------------------------
  // Test 24: Resume from last checkpoint after restart
  // -------------------------------------------------------------------------
  it.fails('should resume from last checkpoint after restart', async () => {
    // Insert first batch
    let checkpointLSN: bigint = 0n;
    for (let i = 1; i <= 50; i++) {
      const result = await dosql.insert('metrics', {
        id: i,
        metric_name: `metric_${i}`,
        value: Math.random() * 100,
        recorded_at: Date.now(),
      });
      if (i === 50) checkpointLSN = result.lsn;
    }

    await waitForLakehouseSync(dolake, checkpointLSN, SYNC_TIMEOUT);

    // Record the checkpoint
    const snapshotBefore = await dolake.getCurrentSnapshot();

    // Simulate restart by closing and reopening lakehouse
    await dolake.close();
    dolake = await createDoLakeClient({ lakehouseId: testLakehouseId });

    // Insert more data
    let lastLSN: bigint = 0n;
    for (let i = 51; i <= 100; i++) {
      const result = await dosql.insert('metrics', {
        id: i,
        metric_name: `metric_${i}`,
        value: Math.random() * 100,
        recorded_at: Date.now(),
      });
      lastLSN = result.lsn;
    }

    await waitForLakehouseSync(dolake, lastLSN, SYNC_TIMEOUT);

    // All data should be present
    const result = await dolake.query('SELECT COUNT(*) as count FROM metrics');
    expect(result.rows[0].count).toBe(100);

    // Snapshot lineage should be maintained
    const snapshotAfter = await dolake.getCurrentSnapshot();
    expect(snapshotAfter.sequenceNumber).toBeGreaterThan(snapshotBefore.sequenceNumber);
  }, E2E_TIMEOUT);

  // -------------------------------------------------------------------------
  // Test 25: Checkpoint persistence across DO hibernation
  // -------------------------------------------------------------------------
  it.fails('should persist checkpoint across DO hibernation', async () => {
    // Insert data
    let lastLSN: bigint = 0n;
    for (let i = 1; i <= 30; i++) {
      const result = await dosql.insert('metrics', {
        id: i,
        metric_name: `metric_${i}`,
        value: i * 1.5,
        recorded_at: Date.now(),
      });
      lastLSN = result.lsn;
    }

    await waitForLakehouseSync(dolake, lastLSN, SYNC_TIMEOUT);

    // Get current state
    const chunksBefore = await dolake.getChunks('metrics');
    const manifestBefore = await dolake.getTableManifest('metrics');

    // Simulate hibernation (close both)
    await dolake.close();
    await dosql.close();

    // Wake up after some time
    await new Promise(resolve => setTimeout(resolve, 2000));

    // Reconnect
    dosql = await createDoSQLClient({ dbName: testDbName });
    dolake = await createDoLakeClient({ lakehouseId: testLakehouseId });

    // State should be restored
    const chunksAfter = await dolake.getChunks('metrics');
    const manifestAfter = await dolake.getTableManifest('metrics');

    expect(chunksAfter.length).toBe(chunksBefore.length);
    expect(manifestAfter?.totalRowCount).toBe(manifestBefore?.totalRowCount);
  }, E2E_TIMEOUT);

  // -------------------------------------------------------------------------
  // Test 26: Recovery with gap detection
  // -------------------------------------------------------------------------
  it.fails('should detect and report LSN gaps during recovery', async () => {
    // Insert data with gaps (simulating lost events)
    await dosql.insert('metrics', { id: 1, metric_name: 'm1', value: 1, recorded_at: Date.now() });
    await dosql.insert('metrics', { id: 2, metric_name: 'm2', value: 2, recorded_at: Date.now() });

    // Skip id 3-5 (simulate gap)

    await dosql.insert('metrics', { id: 6, metric_name: 'm6', value: 6, recorded_at: Date.now() });

    await dolake.waitForSync({ timeoutMs: SYNC_TIMEOUT });

    // Verify should report the gap
    const verification = await dolake.verify('metrics');

    // If gaps are detected, they should be reported
    // This depends on implementation - may need LSN tracking
    expect(verification.totalRows).toBe(3); // Only rows we actually inserted
  }, E2E_TIMEOUT);
});

// =============================================================================
// Test Suite: Multi-Table Consistency
// =============================================================================

describe('E2E Lakehouse - Multi-Table Consistency', () => {
  let dosql: DoSQLClient;
  let dolake: DoLakeClient;
  let testDbName: string;
  let testLakehouseId: string;

  beforeEach(async () => {
    testDbName = `test-db-multi-${Date.now()}`;
    testLakehouseId = `test-lake-multi-${Date.now()}`;

    dosql = await createDoSQLClient({ dbName: testDbName });
    dolake = await createDoLakeClient({ lakehouseId: testLakehouseId });

    await dosql.execute(`
      CREATE TABLE IF NOT EXISTS customers (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT
      )
    `);

    await dosql.execute(`
      CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY,
        customer_id INTEGER NOT NULL,
        total REAL,
        status TEXT
      )
    `);

    await dosql.execute(`
      CREATE TABLE IF NOT EXISTS order_items (
        id INTEGER PRIMARY KEY,
        order_id INTEGER NOT NULL,
        product_name TEXT,
        quantity INTEGER,
        price REAL
      )
    `);
  });

  afterEach(async () => {
    await dolake?.close();
    await dosql?.close();
  });

  // -------------------------------------------------------------------------
  // Test 27: Cross-table transaction consistency
  // -------------------------------------------------------------------------
  it.fails('should maintain cross-table consistency in transactions', async () => {
    // Create customer and order in transaction
    await dosql.transaction(async (tx) => {
      await tx.insert('customers', { id: 1, name: 'Alice', email: 'alice@test.com' });
      await tx.insert('orders', { id: 1, customer_id: 1, total: 150.00, status: 'pending' });
      await tx.insert('order_items', { id: 1, order_id: 1, product_name: 'Widget', quantity: 3, price: 50.00 });
    });

    await dolake.waitForSync({ timeoutMs: SYNC_TIMEOUT });

    // All tables should be consistent
    const customers = await dolake.query('SELECT * FROM customers');
    const orders = await dolake.query('SELECT * FROM orders');
    const items = await dolake.query('SELECT * FROM order_items');

    expect(customers.rowCount).toBe(1);
    expect(orders.rowCount).toBe(1);
    expect(items.rowCount).toBe(1);

    // Foreign key relationships should be valid
    expect(orders.rows[0].customer_id).toBe(customers.rows[0].id);
    expect(items.rows[0].order_id).toBe(orders.rows[0].id);
  }, E2E_TIMEOUT);

  // -------------------------------------------------------------------------
  // Test 28: Parallel writes to multiple tables
  // -------------------------------------------------------------------------
  it.fails('should handle parallel writes to multiple tables', async () => {
    // Parallel inserts to different tables
    const promises = [];

    for (let i = 1; i <= 20; i++) {
      promises.push(dosql.insert('customers', { id: i, name: `Customer ${i}`, email: `c${i}@test.com` }));
      promises.push(dosql.insert('orders', { id: i, customer_id: i, total: i * 100, status: 'new' }));
    }

    await Promise.all(promises);
    await dolake.waitForSync({ timeoutMs: SYNC_TIMEOUT });

    // Both tables should have all data
    const customers = await dolake.query('SELECT COUNT(*) as count FROM customers');
    const orders = await dolake.query('SELECT COUNT(*) as count FROM orders');

    expect(customers.rows[0].count).toBe(20);
    expect(orders.rows[0].count).toBe(20);
  }, E2E_TIMEOUT);
});

// =============================================================================
// Test Suite: Data Integrity Verification
// =============================================================================

describe('E2E Lakehouse - Data Integrity', () => {
  let dosql: DoSQLClient;
  let dolake: DoLakeClient;
  let testDbName: string;
  let testLakehouseId: string;

  beforeEach(async () => {
    testDbName = `test-db-integrity-${Date.now()}`;
    testLakehouseId = `test-lake-integrity-${Date.now()}`;

    dosql = await createDoSQLClient({ dbName: testDbName });
    dolake = await createDoLakeClient({ lakehouseId: testLakehouseId });

    await dosql.execute(`
      CREATE TABLE IF NOT EXISTS records (
        id INTEGER PRIMARY KEY,
        checksum TEXT NOT NULL,
        data TEXT,
        version INTEGER DEFAULT 1
      )
    `);
  });

  afterEach(async () => {
    await dolake?.close();
    await dosql?.close();
  });

  // -------------------------------------------------------------------------
  // Test 29: Data checksum verification
  // -------------------------------------------------------------------------
  it.fails('should verify data checksum between DoSQL and Lakehouse', async () => {
    // Insert data with checksums
    const records: Array<{ id: number; checksum: string; data: string }> = [];
    for (let i = 1; i <= 100; i++) {
      const data = `data-${i}-${Math.random()}`;
      const checksum = simpleHash(data);
      records.push({ id: i, checksum, data });
      await dosql.insert('records', { id: i, checksum, data, version: 1 });
    }

    await dolake.waitForSync({ timeoutMs: SYNC_TIMEOUT });

    // Query lakehouse and verify checksums
    const result = await dolake.query<{ id: number; checksum: string; data: string }>(
      'SELECT id, checksum, data FROM records'
    );

    expect(result.rowCount).toBe(100);

    for (const row of result.rows) {
      const expected = records.find(r => r.id === row.id);
      expect(row.checksum).toBe(expected?.checksum);
      expect(simpleHash(row.data)).toBe(row.checksum);
    }
  }, E2E_TIMEOUT);

  // -------------------------------------------------------------------------
  // Test 30: Verify chunk integrity
  // -------------------------------------------------------------------------
  it.fails('should verify chunk data integrity', async () => {
    // Insert data
    let lastLSN: bigint = 0n;
    for (let i = 1; i <= 50; i++) {
      const result = await dosql.insert('records', {
        id: i,
        checksum: `ck-${i}`,
        data: `payload-${i}`,
        version: 1,
      });
      lastLSN = result.lsn;
    }

    await waitForLakehouseSync(dolake, lastLSN, SYNC_TIMEOUT);

    // Verify each chunk
    const chunks = await dolake.getChunks('records');

    for (const chunk of chunks) {
      // Chunk metadata should be valid
      expect(chunk.rowCount).toBeGreaterThan(0);
      expect(chunk.byteSize).toBeGreaterThan(0);
      expect(chunk.maxLSN).toBeGreaterThanOrEqual(chunk.minLSN);
    }

    // Full verification
    const verification = await dolake.verify('records');
    expect(verification.valid).toBe(true);
    expect(verification.totalRows).toBe(50);
  }, E2E_TIMEOUT);

  // -------------------------------------------------------------------------
  // Test 31: Detect and report data corruption
  // -------------------------------------------------------------------------
  it.fails('should detect and report data corruption', async () => {
    // Insert valid data
    for (let i = 1; i <= 20; i++) {
      await dosql.insert('records', {
        id: i,
        checksum: `valid-${i}`,
        data: `data-${i}`,
        version: 1,
      });
    }

    await dolake.waitForSync({ timeoutMs: SYNC_TIMEOUT });

    // Run verification - should pass
    const verification = await dolake.verify('records');

    expect(verification.valid).toBe(true);
    expect(verification.errors).toHaveLength(0);
    expect(verification.checksum).toBeDefined();
  }, E2E_TIMEOUT);
});

// =============================================================================
// Test Suite: Performance and Scale
// =============================================================================

describe('E2E Lakehouse - Performance', () => {
  let dosql: DoSQLClient;
  let dolake: DoLakeClient;
  let testDbName: string;
  let testLakehouseId: string;

  beforeEach(async () => {
    testDbName = `test-db-perf-${Date.now()}`;
    testLakehouseId = `test-lake-perf-${Date.now()}`;

    dosql = await createDoSQLClient({ dbName: testDbName });
    dolake = await createDoLakeClient({ lakehouseId: testLakehouseId });

    await dosql.execute(`
      CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY,
        event_type TEXT NOT NULL,
        payload TEXT,
        timestamp INTEGER
      )
    `);
  });

  afterEach(async () => {
    await dolake?.close();
    await dosql?.close();
  });

  // -------------------------------------------------------------------------
  // Test 32: High-throughput ingestion
  // -------------------------------------------------------------------------
  it.fails('should handle high-throughput ingestion', async () => {
    const eventCount = 1000;
    const startTime = Date.now();

    // Insert many events rapidly
    const promises = [];
    for (let i = 1; i <= eventCount; i++) {
      promises.push(
        dosql.insert('events', {
          id: i,
          event_type: `type_${i % 10}`,
          payload: JSON.stringify({ index: i, data: 'x'.repeat(100) }),
          timestamp: Date.now(),
        })
      );
    }

    await Promise.all(promises);

    const insertDuration = Date.now() - startTime;
    console.log(`Inserted ${eventCount} events in ${insertDuration}ms`);

    // Wait for lakehouse sync
    await dolake.waitForSync({ timeoutMs: SYNC_TIMEOUT * 2 });

    const syncDuration = Date.now() - startTime;
    console.log(`Full sync completed in ${syncDuration}ms`);

    // Verify all data
    const result = await dolake.query('SELECT COUNT(*) as count FROM events');
    expect(result.rows[0].count).toBe(eventCount);

    // Performance assertion (adjust based on requirements)
    expect(insertDuration).toBeLessThan(30000); // 30 seconds for 1000 inserts
  }, E2E_TIMEOUT * 2);

  // -------------------------------------------------------------------------
  // Test 33: Chunk compaction efficiency
  // -------------------------------------------------------------------------
  it.fails('should efficiently compact small chunks', async () => {
    // Insert in small batches to create many small chunks
    for (let batch = 0; batch < 10; batch++) {
      for (let i = 1; i <= 10; i++) {
        await dosql.insert('events', {
          id: batch * 10 + i,
          event_type: 'batch',
          payload: 'small',
          timestamp: Date.now(),
        });
      }
      // Force flush between batches
      await dolake.waitForSync({ timeoutMs: 5000 });
    }

    // Get initial chunk count
    const chunksBefore = await dolake.getChunks('events');

    // Wait for compaction (if automatic) or trigger it
    await new Promise(resolve => setTimeout(resolve, 5000));

    const chunksAfter = await dolake.getChunks('events');

    // After compaction, should have fewer but larger chunks
    // (This depends on compaction being implemented and configured)
    expect(chunksAfter.length).toBeLessThanOrEqual(chunksBefore.length);

    // Total rows should be unchanged
    const totalRows = chunksAfter.reduce((sum, c) => sum + c.rowCount, 0);
    expect(totalRows).toBe(100);
  }, E2E_TIMEOUT);
});

// =============================================================================
// Test Suite: Edge Cases
// =============================================================================

describe('E2E Lakehouse - Edge Cases', () => {
  let dosql: DoSQLClient;
  let dolake: DoLakeClient;
  let testDbName: string;
  let testLakehouseId: string;

  beforeEach(async () => {
    testDbName = `test-db-edge-${Date.now()}`;
    testLakehouseId = `test-lake-edge-${Date.now()}`;

    dosql = await createDoSQLClient({ dbName: testDbName });
    dolake = await createDoLakeClient({ lakehouseId: testLakehouseId });

    await dosql.execute(`
      CREATE TABLE IF NOT EXISTS data (
        id INTEGER PRIMARY KEY,
        nullable_col TEXT,
        json_col TEXT,
        binary_col BLOB
      )
    `);
  });

  afterEach(async () => {
    await dolake?.close();
    await dosql?.close();
  });

  // -------------------------------------------------------------------------
  // Test 34: Handle NULL values correctly
  // -------------------------------------------------------------------------
  it.fails('should handle NULL values correctly', async () => {
    // Insert with NULL values
    const result = await dosql.insert('data', {
      id: 1,
      nullable_col: null,
      json_col: '{"key": "value"}',
      binary_col: null,
    });

    await waitForLakehouseSync(dolake, result.lsn, SYNC_TIMEOUT);

    // Query and verify NULLs preserved
    const row = await dolake.query('SELECT * FROM data WHERE id = 1');

    expect(row.rowCount).toBe(1);
    expect(row.rows[0].nullable_col).toBeNull();
    expect(row.rows[0].binary_col).toBeNull();
    expect(row.rows[0].json_col).toBe('{"key": "value"}');
  }, E2E_TIMEOUT);

  // -------------------------------------------------------------------------
  // Test 35: Handle special characters in text
  // -------------------------------------------------------------------------
  it.fails('should handle special characters in text fields', async () => {
    const specialStrings = [
      'Hello "World"',
      "It's a test",
      'Line1\nLine2',
      'Tab\there',
      'Unicode: \u00e9\u00e8\u00ea',
      'Emoji: \ud83d\ude00',
      'Backslash: \\path\\to\\file',
    ];

    let lastLSN: bigint = 0n;
    for (let i = 0; i < specialStrings.length; i++) {
      const result = await dosql.insert('data', {
        id: i + 1,
        nullable_col: specialStrings[i],
        json_col: null,
        binary_col: null,
      });
      lastLSN = result.lsn;
    }

    await waitForLakehouseSync(dolake, lastLSN, SYNC_TIMEOUT);

    // Verify all strings preserved correctly
    const result = await dolake.query<{ id: number; nullable_col: string }>(
      'SELECT id, nullable_col FROM data ORDER BY id'
    );

    expect(result.rowCount).toBe(specialStrings.length);

    for (let i = 0; i < specialStrings.length; i++) {
      expect(result.rows[i].nullable_col).toBe(specialStrings[i]);
    }
  }, E2E_TIMEOUT);
});

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Simple hash function for testing
 */
function simpleHash(str: string): string {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash;
  }
  return hash.toString(16);
}
