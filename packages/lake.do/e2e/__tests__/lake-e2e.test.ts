/**
 * DoLake E2E Tests (TDD Red Phase)
 *
 * End-to-end tests for lake.do against a real deployed Cloudflare Worker.
 * These tests document expected behavior that doesn't exist yet.
 *
 * Test Categories:
 * - Connect to deployed DoLake worker
 * - Write CDC batch and verify ack
 * - Read back written data via query
 * - Stream CDC events in real-time
 * - Time travel query to snapshot
 * - Cold start latency measurement
 * - Connection resilience (disconnect/reconnect)
 *
 * Issue: sql-cth - lake.do E2E tests against deployed worker
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest';
import {
  DoLakeE2EClient,
  DoLakeWebSocketClient,
  createTestClient,
  retry,
  generateCDCEvents,
} from '../client.js';
import { getE2EEndpoint, waitForReady, measureColdStart } from '../setup.js';

// =============================================================================
// Test Configuration
// =============================================================================

const E2E_TIMEOUT = 120_000;
const CDC_BATCH_TIMEOUT = 10_000;
const QUERY_TIMEOUT = 30_000;

// Skip E2E tests if no endpoint is configured
const E2E_ENDPOINT = process.env.DOLAKE_E2E_ENDPOINT || process.env.DOLAKE_URL || process.env.DOLAKE_E2E_SKIP
  ? null
  : (() => {
      try {
        return getE2EEndpoint();
      } catch {
        return null;
      }
    })();

// Use describe.skip when no endpoint, so tests appear in output but skip gracefully
const describeE2E = E2E_ENDPOINT ? describe : describe.skip;

// =============================================================================
// Test Suite: Connection & Health
// =============================================================================

describeE2E('DoLake E2E: Connection & Health', () => {
  let baseUrl: string;
  let testRunId: string;

  beforeAll(async () => {
    baseUrl = E2E_ENDPOINT!;
    testRunId = `lake-e2e-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;

    console.log(`[DoLake E2E] Running tests against: ${baseUrl}`);
    console.log(`[DoLake E2E] Test run ID: ${testRunId}`);
  }, E2E_TIMEOUT);

  describe('Worker Connection', () => {
    /**
     * RED Phase: Connect to deployed DoLake worker
     * Expected: Worker responds to health check with status: ok
     * Actual: E2E infrastructure not deployed yet
     */
    it.fails('connects to deployed DoLake worker', async () => {
      const client = new DoLakeE2EClient({ baseUrl });

      const health = await client.health();

      expect(health.status).toBe('ok');
      expect(health.service).toBe('dolake');
    }, E2E_TIMEOUT);

    /**
     * RED Phase: Lakehouse-specific health check
     * Expected: Individual lakehouse responds to health check
     * Actual: E2E infrastructure not deployed yet
     */
    it.fails('checks lakehouse-specific health', async () => {
      const client = createTestClient(baseUrl, 'health-check');

      const health = await client.lakehouseHealth();

      expect(health.status).toBe('ok');
      expect(health.initialized).toBeDefined();
    }, E2E_TIMEOUT);

    /**
     * RED Phase: Ping measures latency
     * Expected: Ping returns round-trip latency measurement
     * Actual: E2E infrastructure not deployed yet
     */
    it.fails('measures ping latency', async () => {
      const client = createTestClient(baseUrl, 'ping-test');

      await client.waitForReady();
      const { latency } = await client.ping();

      expect(latency).toBeGreaterThan(0);
      expect(latency).toBeLessThan(5000); // Should be under 5s
    }, E2E_TIMEOUT);
  });

  describe('WebSocket Connection', () => {
    /**
     * RED Phase: WebSocket connection establishment
     * Expected: WebSocket connects successfully
     * Actual: E2E infrastructure not deployed yet
     */
    it.fails('establishes WebSocket connection', async () => {
      const wsClient = new DoLakeWebSocketClient({
        baseUrl,
        lakehouseName: `ws-connect-${Date.now()}`,
      });

      try {
        await wsClient.connect();
        expect(wsClient.isConnected()).toBe(true);
      } finally {
        wsClient.close();
      }
    }, E2E_TIMEOUT);

    /**
     * RED Phase: WebSocket reconnection after disconnect
     * Expected: Can reconnect after close
     * Actual: E2E infrastructure not deployed yet
     */
    it.fails('reconnects after disconnect', async () => {
      const wsClient = new DoLakeWebSocketClient({
        baseUrl,
        lakehouseName: `ws-reconnect-${Date.now()}`,
      });

      try {
        // First connection
        await wsClient.connect();
        expect(wsClient.isConnected()).toBe(true);

        // Close
        wsClient.close();
        expect(wsClient.isConnected()).toBe(false);

        // Wait briefly
        await new Promise((resolve) => setTimeout(resolve, 500));

        // Reconnect
        await wsClient.connect();
        expect(wsClient.isConnected()).toBe(true);
      } finally {
        wsClient.close();
      }
    }, E2E_TIMEOUT);
  });
});

// =============================================================================
// Test Suite: CDC Write & Ack
// =============================================================================

describeE2E('DoLake E2E: CDC Write & Ack', () => {
  let baseUrl: string;
  let client: DoLakeE2EClient;

  beforeAll(async () => {
    baseUrl = E2E_ENDPOINT!;
  }, E2E_TIMEOUT);

  beforeEach(async () => {
    client = createTestClient(baseUrl, 'cdc-write');
  });

  /**
   * RED Phase: Write CDC batch and verify ack
   * Expected: CDC events are written and acknowledged
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('writes CDC batch and receives ack', async () => {
    const events = generateCDCEvents('orders', 10, 'INSERT');

    const result = await client.writeCDCBatch(events);

    expect(result.success).toBe(true);
    expect(result.eventsWritten).toBe(10);
    expect(result.sequenceNumber).toBeGreaterThan(0);
  }, E2E_TIMEOUT);

  /**
   * RED Phase: Write multiple CDC batches
   * Expected: Sequence numbers increment correctly
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('writes multiple CDC batches with incrementing sequence numbers', async () => {
    const batch1 = generateCDCEvents('orders', 5, 'INSERT');
    const batch2 = generateCDCEvents('orders', 5, 'INSERT');

    const result1 = await client.writeCDCBatch(batch1);
    const result2 = await client.writeCDCBatch(batch2);

    expect(result1.success).toBe(true);
    expect(result2.success).toBe(true);
    expect(result2.sequenceNumber).toBeGreaterThan(result1.sequenceNumber);
  }, E2E_TIMEOUT);

  /**
   * RED Phase: Acknowledge CDC events
   * Expected: Events are acknowledged up to sequence number
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('acknowledges CDC events up to sequence number', async () => {
    const events = generateCDCEvents('orders', 10, 'INSERT');
    const writeResult = await client.writeCDCBatch(events);

    const ackResult = await client.ackCDC(writeResult.sequenceNumber);

    expect(ackResult.success).toBe(true);

    const state = await client.getCDCState();
    expect(state.lastAckedSequenceNumber).toBe(writeResult.sequenceNumber);
    expect(state.pendingEvents).toBe(0);
  }, E2E_TIMEOUT);

  /**
   * RED Phase: Write CDC with mixed operations
   * Expected: INSERT, UPDATE, DELETE all captured
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('handles mixed CDC operations', async () => {
    const inserts = generateCDCEvents('orders', 3, 'INSERT');
    const updates = generateCDCEvents('orders', 2, 'UPDATE');
    const deletes = generateCDCEvents('orders', 1, 'DELETE');

    const allEvents = [...inserts, ...updates, ...deletes];
    const result = await client.writeCDCBatch(allEvents);

    expect(result.success).toBe(true);
    expect(result.eventsWritten).toBe(6);
  }, E2E_TIMEOUT);
});

// =============================================================================
// Test Suite: Query Operations
// =============================================================================

describeE2E('DoLake E2E: Query Operations', () => {
  let baseUrl: string;
  let client: DoLakeE2EClient;

  beforeAll(async () => {
    baseUrl = E2E_ENDPOINT!;
  }, E2E_TIMEOUT);

  beforeEach(async () => {
    client = createTestClient(baseUrl, 'query');
  });

  /**
   * RED Phase: Read back written data via query
   * Expected: Query returns data that was written via CDC
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('reads back written data via query', async () => {
    // Write data
    const events = generateCDCEvents('products', 10, 'INSERT');
    await client.writeCDCBatch(events);

    // Allow time for data to be queryable
    await new Promise((resolve) => setTimeout(resolve, 2000));

    // Query data back
    const result = await client.query<{ id: number; value: string }>(
      'SELECT * FROM products ORDER BY id'
    );

    expect(result.rowCount).toBeGreaterThanOrEqual(10);
    expect(result.rows.length).toBeGreaterThanOrEqual(10);
    expect(result.columns).toContain('id');
    expect(result.columns).toContain('value');
  }, QUERY_TIMEOUT);

  /**
   * RED Phase: Query with filters
   * Expected: WHERE clause filters results correctly
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('queries with WHERE filters', async () => {
    const events = generateCDCEvents('users', 20, 'INSERT');
    await client.writeCDCBatch(events);

    await new Promise((resolve) => setTimeout(resolve, 2000));

    const result = await client.query<{ id: number }>(
      'SELECT id FROM users WHERE id < 10'
    );

    expect(result.rowCount).toBeLessThan(20);
    for (const row of result.rows) {
      expect(row.id).toBeLessThan(10);
    }
  }, QUERY_TIMEOUT);

  /**
   * RED Phase: Query returns execution statistics
   * Expected: Query result includes bytes scanned, files scanned, duration
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('returns query execution statistics', async () => {
    const events = generateCDCEvents('stats_test', 50, 'INSERT');
    await client.writeCDCBatch(events);

    await new Promise((resolve) => setTimeout(resolve, 2000));

    const result = await client.query('SELECT COUNT(*) as count FROM stats_test');

    expect(result.bytesScanned).toBeGreaterThan(0);
    expect(result.filesScanned).toBeGreaterThan(0);
    expect(result.duration).toBeGreaterThan(0);
  }, QUERY_TIMEOUT);
});

// =============================================================================
// Test Suite: CDC Streaming
// =============================================================================

describeE2E('DoLake E2E: CDC Streaming', () => {
  let baseUrl: string;

  beforeAll(async () => {
    baseUrl = E2E_ENDPOINT!;
  }, E2E_TIMEOUT);

  /**
   * RED Phase: Stream CDC events in real-time
   * Expected: WebSocket receives CDC events as they're written
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('streams CDC events in real-time', async () => {
    const lakehouseName = `stream-${Date.now()}`;
    const httpClient = new DoLakeE2EClient({ baseUrl, lakehouseName });
    const wsClient = new DoLakeWebSocketClient({ baseUrl, lakehouseName });

    try {
      await wsClient.connect();
      await wsClient.subscribe({ tables: ['events'] });

      // Write some events
      const events = generateCDCEvents('events', 5, 'INSERT');
      await httpClient.writeCDCBatch(events);

      // Receive events via WebSocket
      const batch = await wsClient.nextBatch(CDC_BATCH_TIMEOUT);

      expect(batch.events.length).toBeGreaterThan(0);
      expect(batch.sequenceNumber).toBeGreaterThan(0);
    } finally {
      wsClient.close();
    }
  }, E2E_TIMEOUT);

  /**
   * RED Phase: Subscribe to specific tables
   * Expected: Only receives events for subscribed tables
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('subscribes to specific tables only', async () => {
    const lakehouseName = `table-filter-${Date.now()}`;
    const httpClient = new DoLakeE2EClient({ baseUrl, lakehouseName });
    const wsClient = new DoLakeWebSocketClient({ baseUrl, lakehouseName });

    try {
      await wsClient.connect();
      await wsClient.subscribe({ tables: ['orders'] });

      // Write to both tables
      const orderEvents = generateCDCEvents('orders', 3, 'INSERT');
      const userEvents = generateCDCEvents('users', 3, 'INSERT');

      await httpClient.writeCDCBatch([...orderEvents, ...userEvents]);

      // Collect batches
      const batches = await wsClient.collectBatches(5000);

      // Should only have orders events
      const allEvents = batches.flatMap((b) => b.events);
      for (const event of allEvents) {
        expect((event as any).table).toBe('orders');
      }
    } finally {
      wsClient.close();
    }
  }, E2E_TIMEOUT);

  /**
   * RED Phase: Subscribe to specific operations
   * Expected: Only receives events for subscribed operations
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('subscribes to specific operations only', async () => {
    const lakehouseName = `op-filter-${Date.now()}`;
    const httpClient = new DoLakeE2EClient({ baseUrl, lakehouseName });
    const wsClient = new DoLakeWebSocketClient({ baseUrl, lakehouseName });

    try {
      await wsClient.connect();
      await wsClient.subscribe({ operations: ['INSERT'] });

      // Write mixed operations
      const inserts = generateCDCEvents('data', 3, 'INSERT');
      const updates = generateCDCEvents('data', 2, 'UPDATE');
      const deletes = generateCDCEvents('data', 1, 'DELETE');

      await httpClient.writeCDCBatch([...inserts, ...updates, ...deletes]);

      // Collect batches
      const batches = await wsClient.collectBatches(5000);

      // Should only have INSERT events
      const allEvents = batches.flatMap((b) => b.events);
      for (const event of allEvents) {
        expect((event as any).operation).toBe('INSERT');
      }
    } finally {
      wsClient.close();
    }
  }, E2E_TIMEOUT);

  /**
   * RED Phase: Resume from LSN
   * Expected: Can resume subscription from specific LSN
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('resumes subscription from specific LSN', async () => {
    const lakehouseName = `lsn-resume-${Date.now()}`;
    const httpClient = new DoLakeE2EClient({ baseUrl, lakehouseName });

    // First session: write events and get LSN
    const wsClient1 = new DoLakeWebSocketClient({ baseUrl, lakehouseName });
    let lastLSN: bigint;

    try {
      await wsClient1.connect();
      await wsClient1.subscribe({});

      const firstBatch = generateCDCEvents('data', 5, 'INSERT');
      await httpClient.writeCDCBatch(firstBatch);

      const batch = await wsClient1.nextBatch(CDC_BATCH_TIMEOUT);
      lastLSN = BigInt(batch.sequenceNumber);
      wsClient1.close();

      // Write more events while disconnected
      const secondBatch = generateCDCEvents('data', 5, 'INSERT');
      await httpClient.writeCDCBatch(secondBatch);

      // Second session: resume from LSN
      const wsClient2 = new DoLakeWebSocketClient({ baseUrl, lakehouseName });
      await wsClient2.connect();
      await wsClient2.subscribe({ fromLSN: lastLSN });

      const resumedBatch = await wsClient2.nextBatch(CDC_BATCH_TIMEOUT);

      // Should only get events after the LSN
      expect(BigInt(resumedBatch.sequenceNumber)).toBeGreaterThan(lastLSN);

      wsClient2.close();
    } finally {
      wsClient1.close();
    }
  }, E2E_TIMEOUT);
});

// =============================================================================
// Test Suite: Time Travel
// =============================================================================

describeE2E('DoLake E2E: Time Travel', () => {
  let baseUrl: string;
  let client: DoLakeE2EClient;

  beforeAll(async () => {
    baseUrl = E2E_ENDPOINT!;
  }, E2E_TIMEOUT);

  beforeEach(async () => {
    client = createTestClient(baseUrl, 'timetravel');
  });

  /**
   * RED Phase: Time travel query to snapshot
   * Expected: Query at specific snapshot returns historical data
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('queries data at specific snapshot', async () => {
    // Write initial data
    const initialEvents = generateCDCEvents('inventory', 10, 'INSERT');
    await client.writeCDCBatch(initialEvents);

    // Get snapshots
    const snapshots = await client.listSnapshots('inventory');
    expect(snapshots.length).toBeGreaterThan(0);

    const firstSnapshot = snapshots[snapshots.length - 1]; // Oldest

    // Write more data
    const moreEvents = generateCDCEvents('inventory', 10, 'INSERT');
    await client.writeCDCBatch(moreEvents);

    // Query at first snapshot - should only have initial data
    const historicalResult = await client.query(
      'SELECT COUNT(*) as count FROM inventory',
      { asOf: firstSnapshot.id as unknown as string }
    );

    // Query current - should have all data
    const currentResult = await client.query(
      'SELECT COUNT(*) as count FROM inventory'
    );

    expect((historicalResult.rows[0] as any).count).toBeLessThan(
      (currentResult.rows[0] as any).count
    );
  }, QUERY_TIMEOUT);

  /**
   * RED Phase: Time travel query by timestamp
   * Expected: Query at specific timestamp returns data from that time
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('queries data at specific timestamp', async () => {
    // Write initial data
    const initialEvents = generateCDCEvents('timeseries', 5, 'INSERT');
    await client.writeCDCBatch(initialEvents);

    const checkpoint = new Date();

    // Wait and write more data
    await new Promise((resolve) => setTimeout(resolve, 2000));
    const moreEvents = generateCDCEvents('timeseries', 5, 'INSERT');
    await client.writeCDCBatch(moreEvents);

    // Query at checkpoint timestamp
    const historicalResult = await client.query(
      'SELECT COUNT(*) as count FROM timeseries',
      { asOf: checkpoint }
    );

    // Query current
    const currentResult = await client.query(
      'SELECT COUNT(*) as count FROM timeseries'
    );

    expect((historicalResult.rows[0] as any).count).toBeLessThanOrEqual(5);
    expect((currentResult.rows[0] as any).count).toBeGreaterThanOrEqual(10);
  }, QUERY_TIMEOUT);

  /**
   * RED Phase: List snapshots
   * Expected: Returns list of available snapshots for time travel
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('lists available snapshots', async () => {
    // Write data in batches to create snapshots
    for (let i = 0; i < 3; i++) {
      const events = generateCDCEvents('snapshotted', 5, 'INSERT');
      await client.writeCDCBatch(events);
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    const snapshots = await client.listSnapshots('snapshotted');

    expect(snapshots.length).toBeGreaterThanOrEqual(1);
    for (const snapshot of snapshots) {
      expect(snapshot.id).toBeDefined();
      expect(snapshot.timestamp).toBeDefined();
      expect(snapshot.summary).toBeDefined();
    }
  }, QUERY_TIMEOUT);
});

// =============================================================================
// Test Suite: Performance
// =============================================================================

describeE2E('DoLake E2E: Performance', () => {
  let baseUrl: string;

  beforeAll(async () => {
    baseUrl = E2E_ENDPOINT!;
  }, E2E_TIMEOUT);

  /**
   * RED Phase: Cold start latency measurement
   * Expected: Measures and validates cold start performance
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('measures cold start latency', async () => {
    const measurements = await measureColdStart(baseUrl, { iterations: 5 });

    console.log('[DoLake E2E] Cold start latency:');
    console.log(`  p50: ${measurements.p50.toFixed(2)}ms`);
    console.log(`  p95: ${measurements.p95.toFixed(2)}ms`);
    console.log(`  p99: ${measurements.p99.toFixed(2)}ms`);
    console.log(`  min: ${measurements.min.toFixed(2)}ms`);
    console.log(`  max: ${measurements.max.toFixed(2)}ms`);
    console.log(`  avg: ${measurements.avg.toFixed(2)}ms`);

    // Cold start should be under reasonable threshold
    expect(measurements.p95).toBeLessThan(5000); // 5 seconds max
    expect(measurements.avg).toBeLessThan(3000); // 3 seconds average
  }, E2E_TIMEOUT);

  /**
   * RED Phase: Ping latency measurement
   * Expected: Ping latency is within acceptable range
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('measures ping latency', async () => {
    const client = createTestClient(baseUrl, 'perf-ping');
    await client.waitForReady();

    const measurements = await client.measureLatency(
      () => client.ping(),
      20
    );

    console.log('[DoLake E2E] Ping latency:');
    console.log(`  p50: ${measurements.p50.toFixed(2)}ms`);
    console.log(`  p95: ${measurements.p95.toFixed(2)}ms`);
    console.log(`  avg: ${measurements.avg.toFixed(2)}ms`);

    // Ping should be fast
    expect(measurements.p95).toBeLessThan(500); // 500ms max
  }, E2E_TIMEOUT);

  /**
   * RED Phase: Query latency measurement
   * Expected: Query latency is within acceptable range
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('measures query latency', async () => {
    const client = createTestClient(baseUrl, 'perf-query');

    // Seed some data
    const events = generateCDCEvents('perf_data', 100, 'INSERT');
    await client.writeCDCBatch(events);
    await new Promise((resolve) => setTimeout(resolve, 2000));

    const measurements = await client.measureLatency(
      () => client.query('SELECT COUNT(*) FROM perf_data'),
      10
    );

    console.log('[DoLake E2E] Query latency:');
    console.log(`  p50: ${measurements.p50.toFixed(2)}ms`);
    console.log(`  p95: ${measurements.p95.toFixed(2)}ms`);
    console.log(`  avg: ${measurements.avg.toFixed(2)}ms`);

    // Query should complete in reasonable time
    expect(measurements.p95).toBeLessThan(2000); // 2 seconds max
  }, E2E_TIMEOUT);
});

// =============================================================================
// Test Suite: Connection Resilience
// =============================================================================

describeE2E('DoLake E2E: Connection Resilience', () => {
  let baseUrl: string;

  beforeAll(async () => {
    baseUrl = E2E_ENDPOINT!;
  }, E2E_TIMEOUT);

  /**
   * RED Phase: Connection resilience (disconnect/reconnect)
   * Expected: Client reconnects and resumes operation after disconnect
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('handles disconnect and reconnect gracefully', async () => {
    const lakehouseName = `resilience-${Date.now()}`;
    const httpClient = new DoLakeE2EClient({ baseUrl, lakehouseName });
    const wsClient = new DoLakeWebSocketClient({ baseUrl, lakehouseName });

    try {
      // Connect and write initial data
      await wsClient.connect();
      await wsClient.subscribe({});

      const batch1 = generateCDCEvents('resilient', 5, 'INSERT');
      await httpClient.writeCDCBatch(batch1);

      const received1 = await wsClient.nextBatch(CDC_BATCH_TIMEOUT);
      expect(received1.events.length).toBeGreaterThan(0);

      // Disconnect
      wsClient.close();
      expect(wsClient.isConnected()).toBe(false);

      // Write while disconnected
      const batch2 = generateCDCEvents('resilient', 5, 'INSERT');
      await httpClient.writeCDCBatch(batch2);

      // Reconnect
      await wsClient.connect();
      await wsClient.subscribe({});

      // Write new data
      const batch3 = generateCDCEvents('resilient', 5, 'INSERT');
      await httpClient.writeCDCBatch(batch3);

      // Should receive the new data
      const received2 = await wsClient.nextBatch(CDC_BATCH_TIMEOUT);
      expect(received2.events.length).toBeGreaterThan(0);
    } finally {
      wsClient.close();
    }
  }, E2E_TIMEOUT);

  /**
   * RED Phase: HTTP client handles transient errors
   * Expected: Client retries and succeeds on transient failures
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('retries on transient HTTP errors', async () => {
    const client = createTestClient(baseUrl, 'retry-test');

    // Use retry helper
    const result = await retry(
      async () => {
        const health = await client.health();
        if (health.status !== 'ok') {
          throw new Error('Not ready');
        }
        return health;
      },
      { maxAttempts: 5, initialDelayMs: 500 }
    );

    expect(result.status).toBe('ok');
  }, E2E_TIMEOUT);

  /**
   * RED Phase: WebSocket handles connection timeout
   * Expected: Throws appropriate error on connection timeout
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('handles WebSocket connection timeout', async () => {
    const wsClient = new DoLakeWebSocketClient({
      baseUrl: 'wss://nonexistent.example.com',
      lakehouseName: 'timeout-test',
      timeoutMs: 1000, // 1 second timeout
    });

    await expect(wsClient.connect()).rejects.toThrow(/timeout/i);
  }, E2E_TIMEOUT);

  /**
   * RED Phase: Handles concurrent connections
   * Expected: Multiple clients can connect simultaneously
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('handles concurrent WebSocket connections', async () => {
    const lakehouseName = `concurrent-${Date.now()}`;
    const clients = Array.from({ length: 3 }, () =>
      new DoLakeWebSocketClient({ baseUrl, lakehouseName })
    );

    try {
      // Connect all clients concurrently
      await Promise.all(clients.map((c) => c.connect()));

      // All should be connected
      for (const client of clients) {
        expect(client.isConnected()).toBe(true);
      }

      // Subscribe all
      await Promise.all(clients.map((c) => c.subscribe({})));

      // All should still be connected
      for (const client of clients) {
        expect(client.isConnected()).toBe(true);
      }
    } finally {
      clients.forEach((c) => c.close());
    }
  }, E2E_TIMEOUT);
});

// =============================================================================
// Test Suite: Metadata Operations
// =============================================================================

describeE2E('DoLake E2E: Metadata Operations', () => {
  let baseUrl: string;
  let client: DoLakeE2EClient;

  beforeAll(async () => {
    baseUrl = E2E_ENDPOINT!;
  }, E2E_TIMEOUT);

  beforeEach(async () => {
    client = createTestClient(baseUrl, 'metadata');
  });

  /**
   * RED Phase: Get table metadata
   * Expected: Returns schema and partition info for table
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('retrieves table metadata', async () => {
    const events = generateCDCEvents('metadata_test', 10, 'INSERT');
    await client.writeCDCBatch(events);

    const metadata = await client.getMetadata('metadata_test');

    expect(metadata.tableId).toBeDefined();
    expect(metadata.schema).toBeDefined();
    expect(metadata.schema.columns.length).toBeGreaterThan(0);
  }, QUERY_TIMEOUT);

  /**
   * RED Phase: List tables
   * Expected: Returns list of all tables in lakehouse
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('lists all tables', async () => {
    // Create some tables via CDC
    await client.writeCDCBatch(generateCDCEvents('table1', 1, 'INSERT'));
    await client.writeCDCBatch(generateCDCEvents('table2', 1, 'INSERT'));
    await client.writeCDCBatch(generateCDCEvents('table3', 1, 'INSERT'));

    const tables = await client.listTables();

    expect(tables).toContain('table1');
    expect(tables).toContain('table2');
    expect(tables).toContain('table3');
  }, QUERY_TIMEOUT);

  /**
   * RED Phase: List partitions
   * Expected: Returns partition info for table
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('lists table partitions', async () => {
    const events = generateCDCEvents('partitioned_table', 50, 'INSERT');
    await client.writeCDCBatch(events);

    const partitions = await client.listPartitions('partitioned_table');

    expect(partitions.length).toBeGreaterThan(0);
    for (const partition of partitions) {
      expect(partition.key).toBeDefined();
      expect(partition.fileCount).toBeGreaterThanOrEqual(0);
      expect(partition.rowCount).toBeGreaterThanOrEqual(0);
    }
  }, QUERY_TIMEOUT);
});

// =============================================================================
// Test Suite: Compaction
// =============================================================================

describeE2E('DoLake E2E: Compaction', () => {
  let baseUrl: string;
  let client: DoLakeE2EClient;

  beforeAll(async () => {
    baseUrl = E2E_ENDPOINT!;
  }, E2E_TIMEOUT);

  beforeEach(async () => {
    client = createTestClient(baseUrl, 'compaction');
  });

  /**
   * RED Phase: Trigger compaction
   * Expected: Compaction job is created and completes
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('triggers compaction for partition', async () => {
    // Write enough data to warrant compaction
    for (let i = 0; i < 10; i++) {
      const events = generateCDCEvents('compact_test', 10, 'INSERT');
      await client.writeCDCBatch(events);
    }

    const partitions = await client.listPartitions('compact_test');
    expect(partitions.length).toBeGreaterThan(0);

    const partition = partitions[0];
    const job = await client.compact('compact_test', partition.key as unknown as string);

    expect(job.id).toBeDefined();
    expect(job.status).toMatch(/pending|running|completed/);
  }, E2E_TIMEOUT);

  /**
   * RED Phase: Get compaction job status
   * Expected: Can track compaction job progress
   * Actual: E2E infrastructure not deployed yet
   */
  it.fails('tracks compaction job status', async () => {
    for (let i = 0; i < 5; i++) {
      const events = generateCDCEvents('compact_status', 10, 'INSERT');
      await client.writeCDCBatch(events);
    }

    const partitions = await client.listPartitions('compact_status');
    const partition = partitions[0];
    const job = await client.compact('compact_status', partition.key as unknown as string);

    // Poll for completion
    let status = job;
    let attempts = 0;
    while (status.status !== 'completed' && status.status !== 'failed' && attempts < 30) {
      await new Promise((resolve) => setTimeout(resolve, 1000));
      status = await client.getCompactionStatus(job.id as unknown as string);
      attempts++;
    }

    expect(status.status).toBe('completed');
    expect(status.outputFiles).toBeDefined();
  }, E2E_TIMEOUT);
});
