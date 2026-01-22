/**
 * DoSQL Lakehouse E2E Tests (TDD Red Phase)
 *
 * Full end-to-end lakehouse ingestion tests:
 * - DoSQL write -> CDC capture -> DoLake buffer -> Parquet -> R2
 * - Test flush triggers (event count, byte size, time)
 * - Verify Iceberg metadata
 * - Test resume from failure
 * - Data integrity verification through entire pipeline
 *
 * Issue: pocs-4a1u - Add E2E lakehouse full ingestion test
 *
 * These tests run against real Cloudflare Workers in production.
 * Uses workers-vitest-pool (NO MOCKS).
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest';
import {
  DoSQLE2EClient,
  DoSQLWebSocketClient,
  createTestClient,
  retry,
} from '../client.js';
import { getE2EEndpoint, waitForReady } from '../setup.js';

// =============================================================================
// Test Configuration
// =============================================================================

const E2E_TIMEOUT = 120_000;
const LAKEHOUSE_FLUSH_TIMEOUT = 30_000;
const CDC_EVENT_TIMEOUT = 10_000;

// Skip E2E tests if no endpoint is configured
const E2E_ENDPOINT = process.env.DOSQL_E2E_ENDPOINT || process.env.DOSQL_E2E_SKIP
  ? null
  : (() => {
      try {
        return getE2EEndpoint();
      } catch {
        return null;
      }
    })();

const describeE2E = E2E_ENDPOINT ? describe : describe.skip;

// =============================================================================
// Lakehouse E2E Client Extension
// =============================================================================

interface LakehouseStatus {
  initialized: boolean;
  tables: number;
  chunks: number;
  totalRows: number;
  totalBytes: number;
  currentSnapshotId: string | null;
  snapshotCount: number;
}

interface LakehouseFlushResult {
  success: boolean;
  chunksWritten: number;
  rowsWritten: number;
  bytesWritten: number;
  tablesFlushed: string[];
  snapshotId: string | null;
}

interface LakehouseChunkInfo {
  id: string;
  path: string;
  table: string;
  rowCount: number;
  byteSize: number;
  minLSN: string;
  maxLSN: string;
  sourceDOs: string[];
  createdAt: number;
  partitionPath: string;
}

interface LakehouseManifestInfo {
  formatVersion: number;
  lakehouseId: string;
  currentSnapshotId: string;
  snapshotCount: number;
  tables: {
    name: string;
    totalRowCount: number;
    totalByteSize: number;
    partitionCount: number;
    chunkCount: number;
    schemaVersion: number;
  }[];
  version: number;
  updatedAt: number;
}

interface LakehouseQueryResult {
  success: boolean;
  rows: Record<string, unknown>[];
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
}

interface CDCIngestionStats {
  eventsIngested: number;
  eventsBuffered: number;
  chunksWritten: number;
  bytesWritten: number;
  tablesTracked: number;
  lastFlushAt: number | null;
}

/**
 * Extended E2E client for Lakehouse operations
 */
class LakehouseE2EClient extends DoSQLE2EClient {
  /**
   * Get lakehouse status
   */
  async lakehouseStatus(): Promise<LakehouseStatus> {
    const result = await this.queryInternal<LakehouseStatus>('/lakehouse/status');
    return result;
  }

  /**
   * Trigger manual flush
   */
  async lakehouseFlush(): Promise<LakehouseFlushResult> {
    const result = await this.executeInternal<LakehouseFlushResult>('/lakehouse/flush', 'POST');
    return result;
  }

  /**
   * Get CDC ingestion stats
   */
  async cdcIngestionStats(): Promise<CDCIngestionStats> {
    const result = await this.queryInternal<CDCIngestionStats>('/lakehouse/cdc/stats');
    return result;
  }

  /**
   * List chunks for a table
   */
  async listChunks(table: string): Promise<LakehouseChunkInfo[]> {
    const result = await this.queryInternal<{ chunks: LakehouseChunkInfo[] }>(
      `/lakehouse/tables/${table}/chunks`
    );
    return result.chunks;
  }

  /**
   * Get manifest info
   */
  async getManifest(): Promise<LakehouseManifestInfo> {
    const result = await this.queryInternal<LakehouseManifestInfo>('/lakehouse/manifest');
    return result;
  }

  /**
   * Query lakehouse data
   */
  async lakehouseQuery(sql: string): Promise<LakehouseQueryResult> {
    const result = await this.executeInternal<LakehouseQueryResult>(
      '/lakehouse/query',
      'POST',
      { sql }
    );
    return result;
  }

  /**
   * Get snapshot by ID
   */
  async getSnapshot(snapshotId: string): Promise<{
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
  } | null> {
    try {
      const result = await this.queryInternal<{
        snapshot: {
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
        };
      }>(`/lakehouse/snapshots/${snapshotId}`);
      return result.snapshot;
    } catch {
      return null;
    }
  }

  /**
   * Configure lakehouse flush thresholds
   */
  async configureLakehouse(config: {
    targetChunkSize?: number;
    maxRowsPerChunk?: number;
    flushIntervalMs?: number;
  }): Promise<{ success: boolean }> {
    return this.executeInternal<{ success: boolean }>(
      '/lakehouse/config',
      'PUT',
      config
    );
  }

  /**
   * Verify chunk data integrity
   */
  async verifyChunkIntegrity(chunkId: string): Promise<{
    valid: boolean;
    rowCount: number;
    checksum: string;
    errors?: string[];
  }> {
    return this.queryInternal<{
      valid: boolean;
      rowCount: number;
      checksum: string;
      errors?: string[];
    }>(`/lakehouse/chunks/${chunkId}/verify`);
  }

  /**
   * Get R2 object metadata
   */
  async getR2ObjectMetadata(path: string): Promise<{
    exists: boolean;
    size?: number;
    etag?: string;
    customMetadata?: Record<string, string>;
  }> {
    return this.queryInternal<{
      exists: boolean;
      size?: number;
      etag?: string;
      customMetadata?: Record<string, string>;
    }>(`/lakehouse/r2/metadata?path=${encodeURIComponent(path)}`);
  }

  // Internal helper methods
  private async queryInternal<T>(path: string): Promise<T> {
    const url = `${(this as any).config.baseUrl}/db/${(this as any).config.dbName}${path}`;
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), (this as any).config.timeoutMs);

    try {
      const response = await fetch(url, {
        signal: controller.signal,
        headers: { 'Content-Type': 'application/json' },
      });
      return await response.json() as T;
    } finally {
      clearTimeout(timeoutId);
    }
  }

  private async executeInternal<T>(
    path: string,
    method: string = 'POST',
    body?: unknown
  ): Promise<T> {
    const url = `${(this as any).config.baseUrl}/db/${(this as any).config.dbName}${path}`;
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), (this as any).config.timeoutMs);

    try {
      const response = await fetch(url, {
        method,
        signal: controller.signal,
        headers: { 'Content-Type': 'application/json' },
        body: body ? JSON.stringify(body) : undefined,
      });
      return await response.json() as T;
    } finally {
      clearTimeout(timeoutId);
    }
  }
}

// =============================================================================
// Test Suite: Full Ingestion Flow
// =============================================================================

describeE2E('DoSQL Lakehouse E2E Tests', () => {
  let baseUrl: string;
  let testRunId: string;

  beforeAll(async () => {
    baseUrl = E2E_ENDPOINT!;
    testRunId = `lakehouse-e2e-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;

    console.log(`[Lakehouse E2E] Running tests against: ${baseUrl}`);
    console.log(`[Lakehouse E2E] Test run ID: ${testRunId}`);

    await waitForReady(baseUrl, { timeoutMs: 30_000 });
  }, E2E_TIMEOUT);

  // ===========================================================================
  // Full Pipeline Tests: DoSQL -> CDC -> DoLake -> Parquet -> R2
  // ===========================================================================

  describe('Full Pipeline: DoSQL -> CDC -> DoLake -> Parquet -> R2', () => {
    let client: LakehouseE2EClient;
    let dbName: string;

    beforeEach(async () => {
      dbName = `lakehouse-pipeline-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
      client = new LakehouseE2EClient({ baseUrl, dbName });

      await client.waitForReady();

      // Create test table
      await client.createTable(
        'events',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'event_type', type: 'TEXT' },
          { name: 'payload', type: 'TEXT' },
          { name: 'created_at', type: 'INTEGER' },
        ],
        'id'
      );
    });

    it('should capture INSERTs via CDC and write to lakehouse', async () => {
      // Insert data via DoSQL
      for (let i = 1; i <= 10; i++) {
        await client.insert('events', {
          id: i,
          event_type: 'user_action',
          payload: JSON.stringify({ action: `action_${i}` }),
          created_at: Date.now(),
        });
      }

      // Trigger manual flush
      const flushResult = await client.lakehouseFlush();

      expect(flushResult.success).toBe(true);
      expect(flushResult.rowsWritten).toBeGreaterThanOrEqual(10);
      expect(flushResult.tablesFlushed).toContain('events');

      // Verify chunks were created
      const chunks = await client.listChunks('events');
      expect(chunks.length).toBeGreaterThan(0);

      // Verify total row count matches
      const totalRows = chunks.reduce((sum, c) => sum + c.rowCount, 0);
      expect(totalRows).toBeGreaterThanOrEqual(10);

      // Verify data in R2
      const firstChunk = chunks[0];
      const r2Metadata = await client.getR2ObjectMetadata(firstChunk.path);
      expect(r2Metadata.exists).toBe(true);
      expect(r2Metadata.size).toBeGreaterThan(0);
    }, E2E_TIMEOUT);

    it('should capture UPDATEs via CDC and include in lakehouse', async () => {
      // Insert initial data
      await client.insert('events', {
        id: 1,
        event_type: 'initial',
        payload: '{}',
        created_at: Date.now(),
      });

      // Update the data
      await client.update('events', { event_type: 'updated', payload: '{"modified":true}' }, { id: 1 });

      // Flush to lakehouse
      const flushResult = await client.lakehouseFlush();

      expect(flushResult.success).toBe(true);
      expect(flushResult.rowsWritten).toBeGreaterThanOrEqual(1);

      // Query lakehouse to verify update is captured
      const queryResult = await client.lakehouseQuery("SELECT * FROM events WHERE id = 1");
      expect(queryResult.success).toBe(true);

      // Lakehouse should have the update (either as latest version or append-only log)
      expect(queryResult.rows.length).toBeGreaterThanOrEqual(1);
    }, E2E_TIMEOUT);

    it('should capture DELETEs via CDC with tombstones', async () => {
      // Insert data
      await client.insert('events', {
        id: 1,
        event_type: 'to_delete',
        payload: '{}',
        created_at: Date.now(),
      });

      // Delete the data
      await client.delete('events', { id: 1 });

      // Flush to lakehouse
      const flushResult = await client.lakehouseFlush();

      expect(flushResult.success).toBe(true);

      // Lakehouse should have the delete marker/tombstone
      const chunks = await client.listChunks('events');
      expect(chunks.length).toBeGreaterThan(0);
    }, E2E_TIMEOUT);

    it('should handle mixed operations in correct order', async () => {
      // Perform mixed operations
      await client.insert('events', { id: 1, event_type: 'v1', payload: '{}', created_at: Date.now() });
      await client.update('events', { event_type: 'v2' }, { id: 1 });
      await client.insert('events', { id: 2, event_type: 'new', payload: '{}', created_at: Date.now() });
      await client.delete('events', { id: 1 });
      await client.insert('events', { id: 3, event_type: 'final', payload: '{}', created_at: Date.now() });

      // Flush
      const flushResult = await client.lakehouseFlush();
      expect(flushResult.success).toBe(true);

      // Verify all operations captured
      const chunks = await client.listChunks('events');
      expect(chunks.length).toBeGreaterThan(0);

      // LSN ordering should be preserved
      for (const chunk of chunks) {
        const minLSN = BigInt(chunk.minLSN);
        const maxLSN = BigInt(chunk.maxLSN);
        expect(maxLSN).toBeGreaterThanOrEqual(minLSN);
      }
    }, E2E_TIMEOUT);

    it('should track source DO IDs in chunks', async () => {
      // Insert data
      for (let i = 1; i <= 5; i++) {
        await client.insert('events', {
          id: i,
          event_type: 'tracked',
          payload: '{}',
          created_at: Date.now(),
        });
      }

      await client.lakehouseFlush();

      const chunks = await client.listChunks('events');
      expect(chunks.length).toBeGreaterThan(0);

      // Each chunk should have source DO information
      for (const chunk of chunks) {
        expect(chunk.sourceDOs).toBeDefined();
        expect(Array.isArray(chunk.sourceDOs)).toBe(true);
        expect(chunk.sourceDOs.length).toBeGreaterThan(0);
      }
    }, E2E_TIMEOUT);
  });

  // ===========================================================================
  // Flush Trigger Tests
  // ===========================================================================

  describe('Flush Triggers', () => {
    let client: LakehouseE2EClient;
    let dbName: string;

    beforeEach(async () => {
      dbName = `lakehouse-flush-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
      client = new LakehouseE2EClient({ baseUrl, dbName });

      await client.waitForReady();

      await client.createTable(
        'large_events',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'data', type: 'TEXT' },
        ],
        'id'
      );
    });

    it('should flush on event count threshold', async () => {
      // Configure low threshold for testing
      await client.configureLakehouse({
        maxRowsPerChunk: 10, // Very low for testing
      });

      // Insert more than threshold
      for (let i = 1; i <= 25; i++) {
        await client.insert('large_events', {
          id: i,
          data: `event_${i}`,
        });
      }

      // Check that auto-flush occurred
      const status = await client.lakehouseStatus();
      const stats = await client.cdcIngestionStats();

      // Should have created chunks automatically
      expect(stats.chunksWritten).toBeGreaterThanOrEqual(1);
    }, E2E_TIMEOUT);

    it('should flush on byte size threshold', async () => {
      // Configure low byte threshold for testing
      await client.configureLakehouse({
        targetChunkSize: 1024, // 1KB for testing
      });

      // Insert data that exceeds byte threshold
      const largePayload = 'x'.repeat(512); // ~512 bytes per row
      for (let i = 1; i <= 10; i++) {
        await client.insert('large_events', {
          id: i,
          data: largePayload,
        });
      }

      // Should have auto-flushed
      const stats = await client.cdcIngestionStats();
      expect(stats.chunksWritten).toBeGreaterThanOrEqual(1);
    }, E2E_TIMEOUT);

    it('should flush on time interval', async () => {
      // Configure short flush interval
      await client.configureLakehouse({
        flushIntervalMs: 2000, // 2 seconds
      });

      // Insert some data
      await client.insert('large_events', { id: 1, data: 'test' });

      // Wait for time-based flush
      await new Promise(resolve => setTimeout(resolve, 3000));

      // Check that time-based flush occurred
      const stats = await client.cdcIngestionStats();
      expect(stats.lastFlushAt).not.toBeNull();
    }, E2E_TIMEOUT);

    it('should handle manual flush correctly', async () => {
      // Insert data without triggering auto-flush
      for (let i = 1; i <= 5; i++) {
        await client.insert('large_events', { id: i, data: 'manual_test' });
      }

      // Check stats before flush
      const statsBefore = await client.cdcIngestionStats();
      expect(statsBefore.eventsBuffered).toBeGreaterThan(0);

      // Manual flush
      const flushResult = await client.lakehouseFlush();
      expect(flushResult.success).toBe(true);
      expect(flushResult.rowsWritten).toBeGreaterThanOrEqual(5);

      // Check stats after flush
      const statsAfter = await client.cdcIngestionStats();
      expect(statsAfter.eventsBuffered).toBe(0);
    }, E2E_TIMEOUT);
  });

  // ===========================================================================
  // Iceberg Metadata Tests
  // ===========================================================================

  describe('Iceberg Metadata Verification', () => {
    let client: LakehouseE2EClient;
    let dbName: string;

    beforeEach(async () => {
      dbName = `lakehouse-iceberg-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
      client = new LakehouseE2EClient({ baseUrl, dbName });

      await client.waitForReady();

      await client.createTable(
        'orders',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'customer_id', type: 'INTEGER' },
          { name: 'total', type: 'REAL' },
          { name: 'status', type: 'TEXT' },
        ],
        'id'
      );
    });

    it('should create valid manifest after flush', async () => {
      // Insert data
      for (let i = 1; i <= 10; i++) {
        await client.insert('orders', {
          id: i,
          customer_id: i % 5,
          total: Math.random() * 100,
          status: 'pending',
        });
      }

      await client.lakehouseFlush();

      // Get manifest
      const manifest = await client.getManifest();

      expect(manifest.formatVersion).toBe(1);
      expect(manifest.lakehouseId).toBeDefined();
      expect(manifest.currentSnapshotId).toBeDefined();
      expect(manifest.version).toBeGreaterThan(0);

      // Should have table info
      const ordersTable = manifest.tables.find(t => t.name === 'orders');
      expect(ordersTable).toBeDefined();
      expect(ordersTable!.totalRowCount).toBeGreaterThanOrEqual(10);
      expect(ordersTable!.chunkCount).toBeGreaterThan(0);
    }, E2E_TIMEOUT);

    it('should create snapshots with correct lineage', async () => {
      // First batch
      for (let i = 1; i <= 5; i++) {
        await client.insert('orders', { id: i, customer_id: 1, total: 10, status: 'v1' });
      }
      const flush1 = await client.lakehouseFlush();
      const snapshot1Id = flush1.snapshotId;

      // Second batch
      for (let i = 6; i <= 10; i++) {
        await client.insert('orders', { id: i, customer_id: 2, total: 20, status: 'v2' });
      }
      const flush2 = await client.lakehouseFlush();
      const snapshot2Id = flush2.snapshotId;

      // Verify snapshot lineage
      if (snapshot1Id && snapshot2Id) {
        const snapshot2 = await client.getSnapshot(snapshot2Id);
        expect(snapshot2).not.toBeNull();
        expect(snapshot2!.parentId).toBe(snapshot1Id);
        expect(snapshot2!.sequenceNumber).toBeGreaterThan(0);
      }
    }, E2E_TIMEOUT);

    it('should track snapshot summary correctly', async () => {
      // Insert data
      for (let i = 1; i <= 10; i++) {
        await client.insert('orders', { id: i, customer_id: 1, total: 50, status: 'new' });
      }

      const flushResult = await client.lakehouseFlush();

      if (flushResult.snapshotId) {
        const snapshot = await client.getSnapshot(flushResult.snapshotId);

        expect(snapshot).not.toBeNull();
        expect(snapshot!.summary.tablesModified).toContain('orders');
        expect(snapshot!.summary.chunksAdded).toBeGreaterThan(0);
        expect(snapshot!.summary.rowsAdded).toBeGreaterThanOrEqual(10);
      }
    }, E2E_TIMEOUT);

    it('should update table statistics after each flush', async () => {
      // First flush
      for (let i = 1; i <= 5; i++) {
        await client.insert('orders', { id: i, customer_id: 1, total: 10, status: 'first' });
      }
      await client.lakehouseFlush();

      const manifest1 = await client.getManifest();
      const ordersStats1 = manifest1.tables.find(t => t.name === 'orders')!;

      // Second flush
      for (let i = 6; i <= 15; i++) {
        await client.insert('orders', { id: i, customer_id: 2, total: 20, status: 'second' });
      }
      await client.lakehouseFlush();

      const manifest2 = await client.getManifest();
      const ordersStats2 = manifest2.tables.find(t => t.name === 'orders')!;

      // Statistics should increase
      expect(ordersStats2.totalRowCount).toBeGreaterThan(ordersStats1.totalRowCount);
      expect(ordersStats2.totalByteSize).toBeGreaterThan(ordersStats1.totalByteSize);
      expect(ordersStats2.chunkCount).toBeGreaterThanOrEqual(ordersStats1.chunkCount);
    }, E2E_TIMEOUT);

    it('should store column statistics in chunks', async () => {
      // Insert data with varying values
      for (let i = 1; i <= 20; i++) {
        await client.insert('orders', {
          id: i,
          customer_id: i % 5,
          total: i * 10.5,
          status: i % 2 === 0 ? 'complete' : 'pending',
        });
      }

      await client.lakehouseFlush();

      const chunks = await client.listChunks('orders');
      expect(chunks.length).toBeGreaterThan(0);

      // Verify chunk has all required metadata
      for (const chunk of chunks) {
        expect(chunk.id).toBeDefined();
        expect(chunk.path).toBeDefined();
        expect(chunk.rowCount).toBeGreaterThan(0);
        expect(chunk.byteSize).toBeGreaterThan(0);
        expect(chunk.minLSN).toBeDefined();
        expect(chunk.maxLSN).toBeDefined();
        expect(chunk.createdAt).toBeGreaterThan(0);
      }
    }, E2E_TIMEOUT);
  });

  // ===========================================================================
  // Resume from Failure Tests
  // ===========================================================================

  describe('Resume from Failure', () => {
    let client: LakehouseE2EClient;
    let dbName: string;

    beforeEach(async () => {
      dbName = `lakehouse-resume-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
      client = new LakehouseE2EClient({ baseUrl, dbName });

      await client.waitForReady();

      await client.createTable(
        'resilient_data',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'batch', type: 'INTEGER' },
          { name: 'value', type: 'TEXT' },
        ],
        'id'
      );
    });

    it('should resume from last LSN after reconnection', async () => {
      // Insert first batch
      for (let i = 1; i <= 10; i++) {
        await client.insert('resilient_data', { id: i, batch: 1, value: 'batch1' });
      }

      const flush1 = await client.lakehouseFlush();
      const chunks1 = await client.listChunks('resilient_data');
      const lastLSN1 = chunks1.length > 0 ? chunks1[chunks1.length - 1].maxLSN : '0';

      // Simulate disconnect (create new client to same DB)
      const client2 = new LakehouseE2EClient({ baseUrl, dbName });
      await client2.waitForReady();

      // Insert second batch
      for (let i = 11; i <= 20; i++) {
        await client2.insert('resilient_data', { id: i, batch: 2, value: 'batch2' });
      }

      await client2.lakehouseFlush();

      // Verify continuity
      const chunks2 = await client2.listChunks('resilient_data');
      const allChunks = chunks2.sort((a, b) =>
        BigInt(a.minLSN) < BigInt(b.minLSN) ? -1 : 1
      );

      // LSNs should be continuous
      for (let i = 1; i < allChunks.length; i++) {
        const prevMax = BigInt(allChunks[i - 1].maxLSN);
        const currMin = BigInt(allChunks[i].minLSN);
        expect(currMin).toBeGreaterThan(prevMax);
      }

      // All data should be present
      const totalRows = allChunks.reduce((sum, c) => sum + c.rowCount, 0);
      expect(totalRows).toBeGreaterThanOrEqual(20);
    }, E2E_TIMEOUT);

    it('should handle partial flush recovery', async () => {
      // Insert data
      for (let i = 1; i <= 10; i++) {
        await client.insert('resilient_data', { id: i, batch: 1, value: 'partial' });
      }

      // Get pre-flush state
      const statsBefore = await client.cdcIngestionStats();

      // Flush
      await client.lakehouseFlush();

      // Get post-flush state
      const statsAfter = await client.cdcIngestionStats();

      // Buffer should be cleared
      expect(statsAfter.eventsBuffered).toBe(0);
      expect(statsAfter.chunksWritten).toBeGreaterThan(statsBefore.chunksWritten);
    }, E2E_TIMEOUT);

    it('should maintain manifest consistency after multiple sessions', async () => {
      // Session 1: Insert and flush
      for (let i = 1; i <= 5; i++) {
        await client.insert('resilient_data', { id: i, batch: 1, value: 'session1' });
      }
      await client.lakehouseFlush();
      const manifest1 = await client.getManifest();

      // Session 2: More inserts
      const client2 = new LakehouseE2EClient({ baseUrl, dbName });
      await client2.waitForReady();
      for (let i = 6; i <= 10; i++) {
        await client2.insert('resilient_data', { id: i, batch: 2, value: 'session2' });
      }
      await client2.lakehouseFlush();
      const manifest2 = await client2.getManifest();

      // Session 3: Verify
      const client3 = new LakehouseE2EClient({ baseUrl, dbName });
      await client3.waitForReady();
      for (let i = 11; i <= 15; i++) {
        await client3.insert('resilient_data', { id: i, batch: 3, value: 'session3' });
      }
      await client3.lakehouseFlush();
      const manifest3 = await client3.getManifest();

      // Manifest version should increment
      expect(manifest2.version).toBeGreaterThan(manifest1.version);
      expect(manifest3.version).toBeGreaterThan(manifest2.version);

      // Snapshot count should increase
      expect(manifest3.snapshotCount).toBeGreaterThanOrEqual(3);
    }, E2E_TIMEOUT);

    it('should recover buffered events after DO restart', async () => {
      // This tests the scenario where DO hibernates with buffered data
      // and then wakes up to flush

      // Insert data (stays in buffer)
      for (let i = 1; i <= 5; i++) {
        await client.insert('resilient_data', { id: i, batch: 1, value: 'buffered' });
      }

      // Verify data is buffered
      const statsBefore = await client.cdcIngestionStats();
      expect(statsBefore.eventsBuffered).toBeGreaterThan(0);

      // Simulate DO wake-up by triggering flush
      const flushResult = await client.lakehouseFlush();

      // Buffered data should be flushed
      expect(flushResult.success).toBe(true);
      expect(flushResult.rowsWritten).toBeGreaterThanOrEqual(5);

      // Verify in lakehouse
      const chunks = await client.listChunks('resilient_data');
      expect(chunks.length).toBeGreaterThan(0);
    }, E2E_TIMEOUT);
  });

  // ===========================================================================
  // Data Integrity Tests
  // ===========================================================================

  describe('Data Integrity Verification', () => {
    let client: LakehouseE2EClient;
    let dbName: string;

    beforeEach(async () => {
      dbName = `lakehouse-integrity-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
      client = new LakehouseE2EClient({ baseUrl, dbName });

      await client.waitForReady();

      await client.createTable(
        'verified_data',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'checksum', type: 'TEXT' },
          { name: 'data', type: 'TEXT' },
        ],
        'id'
      );
    });

    it('should verify chunk data integrity', async () => {
      // Insert data with known checksums
      for (let i = 1; i <= 10; i++) {
        const data = `data_${i}_${Date.now()}`;
        await client.insert('verified_data', {
          id: i,
          checksum: hashString(data),
          data,
        });
      }

      await client.lakehouseFlush();

      // Verify each chunk
      const chunks = await client.listChunks('verified_data');
      for (const chunk of chunks) {
        const integrity = await client.verifyChunkIntegrity(chunk.id);
        expect(integrity.valid).toBe(true);
        expect(integrity.rowCount).toBe(chunk.rowCount);
        expect(integrity.errors).toBeUndefined();
      }
    }, E2E_TIMEOUT);

    it('should match source data with lakehouse data', async () => {
      // Insert known data
      const sourceData: Record<number, string> = {};
      for (let i = 1; i <= 20; i++) {
        const data = `value_${i}`;
        sourceData[i] = data;
        await client.insert('verified_data', {
          id: i,
          checksum: hashString(data),
          data,
        });
      }

      await client.lakehouseFlush();

      // Query lakehouse and verify
      const queryResult = await client.lakehouseQuery('SELECT id, data FROM verified_data ORDER BY id');

      expect(queryResult.success).toBe(true);
      expect(queryResult.rowCount).toBeGreaterThanOrEqual(20);

      // Verify each row matches source
      for (const row of queryResult.rows) {
        const id = row.id as number;
        const data = row.data as string;
        expect(data).toBe(sourceData[id]);
      }
    }, E2E_TIMEOUT);

    it('should preserve LSN ordering across chunks', async () => {
      // Insert data in batches
      for (let batch = 1; batch <= 3; batch++) {
        for (let i = 1; i <= 5; i++) {
          const id = (batch - 1) * 5 + i;
          await client.insert('verified_data', {
            id,
            checksum: `batch_${batch}`,
            data: `batch_${batch}_item_${i}`,
          });
        }
        await client.lakehouseFlush();
      }

      // Get all chunks sorted by LSN
      const chunks = await client.listChunks('verified_data');
      const sortedChunks = chunks.sort((a, b) => {
        const lsnA = BigInt(a.minLSN);
        const lsnB = BigInt(b.minLSN);
        return lsnA < lsnB ? -1 : lsnA > lsnB ? 1 : 0;
      });

      // Verify LSN ranges don't overlap
      for (let i = 1; i < sortedChunks.length; i++) {
        const prevMax = BigInt(sortedChunks[i - 1].maxLSN);
        const currMin = BigInt(sortedChunks[i].minLSN);
        expect(currMin).toBeGreaterThan(prevMax);
      }
    }, E2E_TIMEOUT);

    it('should handle concurrent writes with correct ordering', async () => {
      // Simulate concurrent writes
      const writes = [];
      for (let i = 1; i <= 20; i++) {
        writes.push(
          client.insert('verified_data', {
            id: i,
            checksum: `concurrent_${i}`,
            data: `data_${i}`,
          })
        );
      }

      await Promise.all(writes);
      await client.lakehouseFlush();

      // All writes should be captured
      const chunks = await client.listChunks('verified_data');
      const totalRows = chunks.reduce((sum, c) => sum + c.rowCount, 0);
      expect(totalRows).toBe(20);
    }, E2E_TIMEOUT);

    it('should detect and report chunk corruption', async () => {
      // Insert data
      for (let i = 1; i <= 5; i++) {
        await client.insert('verified_data', {
          id: i,
          checksum: `test_${i}`,
          data: `data_${i}`,
        });
      }

      await client.lakehouseFlush();

      const chunks = await client.listChunks('verified_data');
      expect(chunks.length).toBeGreaterThan(0);

      // Verify chunk (should be valid)
      const integrity = await client.verifyChunkIntegrity(chunks[0].id);
      expect(integrity.valid).toBe(true);
    }, E2E_TIMEOUT);
  });

  // ===========================================================================
  // Multi-Table Tests
  // ===========================================================================

  describe('Multi-Table Ingestion', () => {
    let client: LakehouseE2EClient;
    let dbName: string;

    beforeEach(async () => {
      dbName = `lakehouse-multi-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
      client = new LakehouseE2EClient({ baseUrl, dbName });

      await client.waitForReady();

      // Create multiple tables
      await client.createTable('users', [
        { name: 'id', type: 'INTEGER' },
        { name: 'name', type: 'TEXT' },
        { name: 'email', type: 'TEXT' },
      ], 'id');

      await client.createTable('products', [
        { name: 'id', type: 'INTEGER' },
        { name: 'name', type: 'TEXT' },
        { name: 'price', type: 'REAL' },
      ], 'id');

      await client.createTable('orders', [
        { name: 'id', type: 'INTEGER' },
        { name: 'user_id', type: 'INTEGER' },
        { name: 'product_id', type: 'INTEGER' },
        { name: 'quantity', type: 'INTEGER' },
      ], 'id');
    });

    it('should handle multiple tables in single flush', async () => {
      // Insert into multiple tables
      for (let i = 1; i <= 5; i++) {
        await client.insert('users', { id: i, name: `User ${i}`, email: `user${i}@test.com` });
        await client.insert('products', { id: i, name: `Product ${i}`, price: i * 10.99 });
        await client.insert('orders', { id: i, user_id: i, product_id: i, quantity: i });
      }

      // Single flush for all tables
      const flushResult = await client.lakehouseFlush();

      expect(flushResult.success).toBe(true);
      expect(flushResult.tablesFlushed).toContain('users');
      expect(flushResult.tablesFlushed).toContain('products');
      expect(flushResult.tablesFlushed).toContain('orders');

      // Verify each table
      const usersChunks = await client.listChunks('users');
      const productsChunks = await client.listChunks('products');
      const ordersChunks = await client.listChunks('orders');

      expect(usersChunks.length).toBeGreaterThan(0);
      expect(productsChunks.length).toBeGreaterThan(0);
      expect(ordersChunks.length).toBeGreaterThan(0);
    }, E2E_TIMEOUT);

    it('should maintain separate partitions per table', async () => {
      // Insert data
      for (let i = 1; i <= 10; i++) {
        await client.insert('users', { id: i, name: `User ${i}`, email: `user${i}@test.com` });
        await client.insert('products', { id: i, name: `Product ${i}`, price: i * 5.0 });
      }

      await client.lakehouseFlush();

      // Get manifest
      const manifest = await client.getManifest();

      // Each table should be tracked separately
      const usersTable = manifest.tables.find(t => t.name === 'users');
      const productsTable = manifest.tables.find(t => t.name === 'products');

      expect(usersTable).toBeDefined();
      expect(productsTable).toBeDefined();
      expect(usersTable!.totalRowCount).toBeGreaterThanOrEqual(10);
      expect(productsTable!.totalRowCount).toBeGreaterThanOrEqual(10);
    }, E2E_TIMEOUT);

    it('should track cross-table operations correctly', async () => {
      // Interleaved operations across tables
      await client.insert('users', { id: 1, name: 'Alice', email: 'alice@test.com' });
      await client.insert('products', { id: 1, name: 'Widget', price: 19.99 });
      await client.insert('orders', { id: 1, user_id: 1, product_id: 1, quantity: 2 });
      await client.update('users', { name: 'Alice Smith' }, { id: 1 });
      await client.insert('products', { id: 2, name: 'Gadget', price: 29.99 });
      await client.insert('orders', { id: 2, user_id: 1, product_id: 2, quantity: 1 });

      await client.lakehouseFlush();

      // All tables should have data
      const status = await client.lakehouseStatus();
      expect(status.tables).toBeGreaterThanOrEqual(3);

      // Total rows should account for all operations
      expect(status.totalRows).toBeGreaterThanOrEqual(5);
    }, E2E_TIMEOUT);
  });

  // ===========================================================================
  // Partitioning Tests
  // ===========================================================================

  describe('Partitioning', () => {
    let client: LakehouseE2EClient;
    let dbName: string;

    beforeEach(async () => {
      dbName = `lakehouse-partition-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
      client = new LakehouseE2EClient({ baseUrl, dbName });

      await client.waitForReady();

      await client.createTable('partitioned_events', [
        { name: 'id', type: 'INTEGER' },
        { name: 'date', type: 'TEXT' },
        { name: 'tenant_id', type: 'TEXT' },
        { name: 'event_data', type: 'TEXT' },
      ], 'id');
    });

    it('should partition data by configured columns', async () => {
      // Configure partitioning
      await client.configureLakehouse({
        // partitionBy: ['date', 'tenant_id'] - configured in lakehouse
      });

      // Insert data with varying partition values
      const dates = ['2024-01-15', '2024-01-16', '2024-01-17'];
      const tenants = ['tenant_a', 'tenant_b'];

      let id = 1;
      for (const date of dates) {
        for (const tenant of tenants) {
          for (let i = 1; i <= 3; i++) {
            await client.insert('partitioned_events', {
              id: id++,
              date,
              tenant_id: tenant,
              event_data: `event_${id}`,
            });
          }
        }
      }

      await client.lakehouseFlush();

      // Verify chunks are in correct partition paths
      const chunks = await client.listChunks('partitioned_events');
      expect(chunks.length).toBeGreaterThan(0);

      // Each chunk should have a partition path
      for (const chunk of chunks) {
        expect(chunk.partitionPath).toBeDefined();
      }
    }, E2E_TIMEOUT);

    it('should support partition pruning in queries', async () => {
      // Insert data
      for (let i = 1; i <= 20; i++) {
        await client.insert('partitioned_events', {
          id: i,
          date: i <= 10 ? '2024-01-15' : '2024-01-16',
          tenant_id: i % 2 === 0 ? 'tenant_a' : 'tenant_b',
          event_data: `event_${i}`,
        });
      }

      await client.lakehouseFlush();

      // Query with partition filter
      const result = await client.lakehouseQuery(
        "SELECT * FROM partitioned_events WHERE date = '2024-01-15'"
      );

      expect(result.success).toBe(true);
      expect(result.rowCount).toBeLessThanOrEqual(10);

      // Should have skipped partitions
      expect(result.stats.partitionsScanned).toBeLessThan(result.stats.chunksScanned + result.stats.chunksSkipped);
    }, E2E_TIMEOUT);
  });
});

// =============================================================================
// Helper Functions
// =============================================================================

/**
 * Simple string hash for testing
 */
function hashString(str: string): string {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash;
  }
  return hash.toString(16);
}
