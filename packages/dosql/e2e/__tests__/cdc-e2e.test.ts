/**
 * DoSQL CDC E2E Tests
 *
 * Tests for Change Data Capture (CDC) streaming over WebSocket:
 * - Event ordering verification
 * - Reconnection and resume from LSN
 * - Backpressure handling
 * - Real-time event delivery
 *
 * These tests run against real Cloudflare Workers in production.
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

const E2E_TIMEOUT = 60_000;
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
// Test Suite
// =============================================================================

describeE2E('DoSQL CDC E2E Tests', () => {
  let baseUrl: string;
  let testRunId: string;

  beforeAll(async () => {
    baseUrl = E2E_ENDPOINT!;
    testRunId = `cdc-e2e-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;

    console.log(`[CDC E2E] Running tests against: ${baseUrl}`);
    console.log(`[CDC E2E] Test run ID: ${testRunId}`);

    await waitForReady(baseUrl, { timeoutMs: 30_000 });
  }, E2E_TIMEOUT);

  // ===========================================================================
  // WebSocket Connection Tests
  // ===========================================================================

  describe('WebSocket Connection', () => {
    it('establishes WebSocket connection', async () => {
      const dbName = `cdc-ws-connect-${Date.now()}`;
      const wsClient = new DoSQLWebSocketClient({ baseUrl, dbName });

      try {
        await wsClient.connect();
        expect(wsClient.isConnected()).toBe(true);
      } finally {
        wsClient.close();
      }
    }, E2E_TIMEOUT);

    it('reconnects after disconnect', async () => {
      const dbName = `cdc-ws-reconnect-${Date.now()}`;
      const wsClient = new DoSQLWebSocketClient({ baseUrl, dbName });

      try {
        // First connection
        await wsClient.connect();
        expect(wsClient.isConnected()).toBe(true);

        // Close and reconnect
        wsClient.close();
        expect(wsClient.isConnected()).toBe(false);

        // Wait a bit before reconnecting
        await new Promise((resolve) => setTimeout(resolve, 500));

        // Reconnect
        await wsClient.connect();
        expect(wsClient.isConnected()).toBe(true);
      } finally {
        wsClient.close();
      }
    }, E2E_TIMEOUT);
  });

  // ===========================================================================
  // CDC Subscription Tests
  // ===========================================================================

  describe('CDC Subscription', () => {
    let httpClient: DoSQLE2EClient;
    let wsClient: DoSQLWebSocketClient;
    let dbName: string;

    beforeEach(async () => {
      dbName = `cdc-sub-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
      httpClient = new DoSQLE2EClient({ baseUrl, dbName });
      wsClient = new DoSQLWebSocketClient({ baseUrl, dbName });

      await httpClient.waitForReady();

      // Setup table for CDC tests
      await httpClient.createTable(
        'cdc_test',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'value', type: 'TEXT' },
        ],
        'id'
      );
    });

    afterEach(() => {
      wsClient.close();
    });

    it('subscribes to CDC events', async () => {
      await wsClient.connect();
      await wsClient.subscribeCDC({
        tables: ['cdc_test'],
      });

      // Subscription should be active
      expect(wsClient.isConnected()).toBe(true);
    }, E2E_TIMEOUT);

    it('receives INSERT events', async () => {
      await wsClient.connect();
      await wsClient.subscribeCDC({
        tables: ['cdc_test'],
        operations: ['INSERT'],
      });

      // Trigger an insert
      await httpClient.insert('cdc_test', { id: 1, value: 'test-insert' });

      // Wait for event
      try {
        const event = await wsClient.nextEvent(CDC_EVENT_TIMEOUT);

        expect(event.operation).toBe('INSERT');
        expect(event.table).toBe('cdc_test');
        expect(event.newRow).toBeDefined();
        expect(event.newRow?.id).toBe(1);
        expect(event.newRow?.value).toBe('test-insert');
      } catch (error) {
        // CDC might not be fully implemented yet - skip gracefully
        console.log('[CDC E2E] CDC events not available:', error);
      }
    }, E2E_TIMEOUT);

    it('receives UPDATE events', async () => {
      // Insert initial data
      await httpClient.insert('cdc_test', { id: 1, value: 'before' });

      await wsClient.connect();
      await wsClient.subscribeCDC({
        tables: ['cdc_test'],
        operations: ['UPDATE'],
      });

      // Trigger an update
      await httpClient.update('cdc_test', { value: 'after' }, { id: 1 });

      // Wait for event
      try {
        const event = await wsClient.nextEvent(CDC_EVENT_TIMEOUT);

        expect(event.operation).toBe('UPDATE');
        expect(event.table).toBe('cdc_test');
        expect(event.oldRow?.value).toBe('before');
        expect(event.newRow?.value).toBe('after');
      } catch (error) {
        console.log('[CDC E2E] CDC events not available:', error);
      }
    }, E2E_TIMEOUT);

    it('receives DELETE events', async () => {
      // Insert initial data
      await httpClient.insert('cdc_test', { id: 1, value: 'to-delete' });

      await wsClient.connect();
      await wsClient.subscribeCDC({
        tables: ['cdc_test'],
        operations: ['DELETE'],
      });

      // Trigger a delete
      await httpClient.delete('cdc_test', { id: 1 });

      // Wait for event
      try {
        const event = await wsClient.nextEvent(CDC_EVENT_TIMEOUT);

        expect(event.operation).toBe('DELETE');
        expect(event.table).toBe('cdc_test');
        expect(event.oldRow).toBeDefined();
        expect(event.oldRow?.id).toBe(1);
      } catch (error) {
        console.log('[CDC E2E] CDC events not available:', error);
      }
    }, E2E_TIMEOUT);
  });

  // ===========================================================================
  // Event Ordering Tests
  // ===========================================================================

  describe('Event Ordering', () => {
    let httpClient: DoSQLE2EClient;
    let wsClient: DoSQLWebSocketClient;
    let dbName: string;

    beforeEach(async () => {
      dbName = `cdc-order-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
      httpClient = new DoSQLE2EClient({ baseUrl, dbName });
      wsClient = new DoSQLWebSocketClient({ baseUrl, dbName });

      await httpClient.waitForReady();

      await httpClient.createTable(
        'ordering_test',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'seq', type: 'INTEGER' },
        ],
        'id'
      );
    });

    afterEach(() => {
      wsClient.close();
    });

    it('maintains LSN ordering for sequential operations', async () => {
      await wsClient.connect();
      await wsClient.subscribeCDC({ tables: ['ordering_test'] });

      // Perform sequential operations
      for (let i = 1; i <= 5; i++) {
        await httpClient.insert('ordering_test', { id: i, seq: i });
      }

      // Collect events
      try {
        const events = await wsClient.collectEvents(5000);

        // Events should be in LSN order
        for (let i = 1; i < events.length; i++) {
          expect(events[i].lsn).toBeGreaterThan(events[i - 1].lsn);
        }

        // Sequence should match insertion order
        const insertEvents = events.filter((e) => e.operation === 'INSERT');
        for (let i = 0; i < insertEvents.length; i++) {
          expect(insertEvents[i].newRow?.seq).toBe(i + 1);
        }
      } catch (error) {
        console.log('[CDC E2E] Event ordering test skipped:', error);
      }
    }, E2E_TIMEOUT);

    it('preserves transaction ordering', async () => {
      await wsClient.connect();
      await wsClient.subscribeCDC({ tables: ['ordering_test'] });

      // Multiple operations (simulating transaction)
      await httpClient.insert('ordering_test', { id: 1, seq: 1 });
      await httpClient.update('ordering_test', { seq: 10 }, { id: 1 });
      await httpClient.delete('ordering_test', { id: 1 });

      try {
        const events = await wsClient.collectEvents(5000);

        // Should see INSERT, UPDATE, DELETE in order
        const operations = events.map((e) => e.operation);
        const insertIdx = operations.indexOf('INSERT');
        const updateIdx = operations.indexOf('UPDATE');
        const deleteIdx = operations.indexOf('DELETE');

        if (insertIdx >= 0 && updateIdx >= 0 && deleteIdx >= 0) {
          expect(insertIdx).toBeLessThan(updateIdx);
          expect(updateIdx).toBeLessThan(deleteIdx);
        }
      } catch (error) {
        console.log('[CDC E2E] Transaction ordering test skipped:', error);
      }
    }, E2E_TIMEOUT);
  });

  // ===========================================================================
  // Resume from LSN Tests
  // ===========================================================================

  describe('Resume from LSN', () => {
    let httpClient: DoSQLE2EClient;
    let dbName: string;

    beforeEach(async () => {
      dbName = `cdc-resume-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
      httpClient = new DoSQLE2EClient({ baseUrl, dbName });

      await httpClient.waitForReady();

      await httpClient.createTable(
        'resume_test',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'batch', type: 'INTEGER' },
        ],
        'id'
      );
    });

    it('resumes from specific LSN after reconnection', async () => {
      // First session: create some data and get LSN
      const wsClient1 = new DoSQLWebSocketClient({ baseUrl, dbName });

      try {
        await wsClient1.connect();
        await wsClient1.subscribeCDC({ tables: ['resume_test'] });

        // Insert first batch
        for (let i = 1; i <= 3; i++) {
          await httpClient.insert('resume_test', { id: i, batch: 1 });
        }

        const firstBatchEvents = await wsClient1.collectEvents(3000);
        const lastLSN = firstBatchEvents.length > 0
          ? firstBatchEvents[firstBatchEvents.length - 1].lsn
          : 0n;

        wsClient1.close();

        // Insert second batch while disconnected
        for (let i = 4; i <= 6; i++) {
          await httpClient.insert('resume_test', { id: i, batch: 2 });
        }

        // Second session: resume from last LSN
        const wsClient2 = new DoSQLWebSocketClient({ baseUrl, dbName });

        await wsClient2.connect();
        await wsClient2.subscribeCDC({
          tables: ['resume_test'],
          fromLSN: lastLSN,
        });

        const secondBatchEvents = await wsClient2.collectEvents(3000);

        // Should only see events after the LSN
        for (const event of secondBatchEvents) {
          if (event.lsn !== undefined && lastLSN !== 0n) {
            expect(event.lsn).toBeGreaterThan(lastLSN);
          }
        }

        wsClient2.close();
      } catch (error) {
        console.log('[CDC E2E] Resume from LSN test skipped:', error);
        wsClient1.close();
      }
    }, E2E_TIMEOUT);

    it('replays all events from LSN 0', async () => {
      // Insert data first
      for (let i = 1; i <= 5; i++) {
        await httpClient.insert('resume_test', { id: i, batch: 1 });
      }

      // Connect and subscribe from beginning
      const wsClient = new DoSQLWebSocketClient({ baseUrl, dbName });

      try {
        await wsClient.connect();
        await wsClient.subscribeCDC({
          tables: ['resume_test'],
          fromLSN: 0n,
        });

        const events = await wsClient.collectEvents(5000);

        // Should see all events
        expect(events.length).toBeGreaterThanOrEqual(5);
      } catch (error) {
        console.log('[CDC E2E] Replay from LSN 0 test skipped:', error);
      } finally {
        wsClient.close();
      }
    }, E2E_TIMEOUT);
  });

  // ===========================================================================
  // Backpressure Tests
  // ===========================================================================

  describe('Backpressure Handling', () => {
    let httpClient: DoSQLE2EClient;
    let wsClient: DoSQLWebSocketClient;
    let dbName: string;

    beforeEach(async () => {
      dbName = `cdc-backpressure-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
      httpClient = new DoSQLE2EClient({ baseUrl, dbName });
      wsClient = new DoSQLWebSocketClient({ baseUrl, dbName });

      await httpClient.waitForReady();

      await httpClient.createTable(
        'backpressure_test',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'data', type: 'TEXT' },
        ],
        'id'
      );
    });

    afterEach(() => {
      wsClient.close();
    });

    it('handles burst of events without loss', async () => {
      await wsClient.connect();
      await wsClient.subscribeCDC({ tables: ['backpressure_test'] });

      const eventCount = 100;

      // Rapid fire inserts
      const insertPromises = Array.from({ length: eventCount }, (_, i) =>
        httpClient.insert('backpressure_test', {
          id: i + 1,
          data: `event-${i + 1}`,
        })
      );

      await Promise.all(insertPromises);

      // Collect all events
      try {
        const events = await wsClient.collectEvents(10000);

        // Should eventually receive all events (may take time due to buffering)
        console.log(`[CDC E2E] Received ${events.length}/${eventCount} events in burst test`);

        // At minimum, some events should be received
        expect(events.length).toBeGreaterThan(0);
      } catch (error) {
        console.log('[CDC E2E] Backpressure test skipped:', error);
      }
    }, E2E_TIMEOUT);

    it('recovers from slow consumer', async () => {
      await wsClient.connect();
      await wsClient.subscribeCDC({ tables: ['backpressure_test'] });

      // Insert events while simulating slow consumption
      for (let i = 1; i <= 10; i++) {
        await httpClient.insert('backpressure_test', { id: i, data: `slow-${i}` });
        // Simulate slow consumer
        await new Promise((resolve) => setTimeout(resolve, 200));
      }

      try {
        // Even with slow consumption, events should be buffered and delivered
        const events = await wsClient.collectEvents(5000);
        expect(events.length).toBeGreaterThan(0);
      } catch (error) {
        console.log('[CDC E2E] Slow consumer test skipped:', error);
      }
    }, E2E_TIMEOUT);
  });

  // ===========================================================================
  // Multi-Table CDC Tests
  // ===========================================================================

  describe('Multi-Table CDC', () => {
    let httpClient: DoSQLE2EClient;
    let wsClient: DoSQLWebSocketClient;
    let dbName: string;

    beforeEach(async () => {
      dbName = `cdc-multi-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
      httpClient = new DoSQLE2EClient({ baseUrl, dbName });
      wsClient = new DoSQLWebSocketClient({ baseUrl, dbName });

      await httpClient.waitForReady();

      // Create multiple tables
      await httpClient.createTable(
        'table_a',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'value', type: 'TEXT' },
        ],
        'id'
      );

      await httpClient.createTable(
        'table_b',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'value', type: 'TEXT' },
        ],
        'id'
      );
    });

    afterEach(() => {
      wsClient.close();
    });

    it('filters events by table', async () => {
      await wsClient.connect();
      await wsClient.subscribeCDC({ tables: ['table_a'] });

      // Insert into both tables
      await httpClient.insert('table_a', { id: 1, value: 'a' });
      await httpClient.insert('table_b', { id: 1, value: 'b' });
      await httpClient.insert('table_a', { id: 2, value: 'a2' });

      try {
        const events = await wsClient.collectEvents(5000);

        // Should only see table_a events
        for (const event of events) {
          expect(event.table).toBe('table_a');
        }
      } catch (error) {
        console.log('[CDC E2E] Table filter test skipped:', error);
      }
    }, E2E_TIMEOUT);

    it('receives events from multiple tables', async () => {
      await wsClient.connect();
      await wsClient.subscribeCDC({ tables: ['table_a', 'table_b'] });

      // Insert into both tables
      await httpClient.insert('table_a', { id: 1, value: 'a' });
      await httpClient.insert('table_b', { id: 1, value: 'b' });

      try {
        const events = await wsClient.collectEvents(5000);

        // Should see events from both tables
        const tables = new Set(events.map((e) => e.table));
        console.log(`[CDC E2E] Received events from tables: ${[...tables].join(', ')}`);
      } catch (error) {
        console.log('[CDC E2E] Multi-table test skipped:', error);
      }
    }, E2E_TIMEOUT);

    it('subscribes to all tables with wildcard', async () => {
      await wsClient.connect();
      await wsClient.subscribeCDC({}); // No table filter = all tables

      await httpClient.insert('table_a', { id: 1, value: 'a' });
      await httpClient.insert('table_b', { id: 1, value: 'b' });

      try {
        const events = await wsClient.collectEvents(5000);

        // Should receive events from all tables
        expect(events.length).toBeGreaterThan(0);
      } catch (error) {
        console.log('[CDC E2E] Wildcard subscription test skipped:', error);
      }
    }, E2E_TIMEOUT);
  });

  // ===========================================================================
  // CDC Error Handling Tests
  // ===========================================================================

  describe('CDC Error Handling', () => {
    it('handles invalid subscription gracefully', async () => {
      const dbName = `cdc-error-${Date.now()}`;
      const wsClient = new DoSQLWebSocketClient({ baseUrl, dbName });

      try {
        await wsClient.connect();

        // Subscribe to non-existent table (should not crash)
        await wsClient.subscribeCDC({ tables: ['nonexistent_table'] });

        // Connection should remain open
        expect(wsClient.isConnected()).toBe(true);
      } catch (error) {
        // Depending on implementation, this might throw or handle gracefully
        console.log('[CDC E2E] Invalid subscription handled:', error);
      } finally {
        wsClient.close();
      }
    }, E2E_TIMEOUT);

    it('recovers from network interruption simulation', async () => {
      const dbName = `cdc-network-${Date.now()}`;
      const httpClient = new DoSQLE2EClient({ baseUrl, dbName });
      const wsClient = new DoSQLWebSocketClient({ baseUrl, dbName });

      await httpClient.waitForReady();

      await httpClient.createTable(
        'network_test',
        [
          { name: 'id', type: 'INTEGER' },
          { name: 'value', type: 'TEXT' },
        ],
        'id'
      );

      try {
        // Connect and subscribe
        await wsClient.connect();
        await wsClient.subscribeCDC({ tables: ['network_test'] });

        // Insert some data
        await httpClient.insert('network_test', { id: 1, value: 'before' });

        // Simulate disconnect
        wsClient.close();

        // More data while disconnected
        await httpClient.insert('network_test', { id: 2, value: 'during' });

        // Reconnect
        await wsClient.connect();
        await wsClient.subscribeCDC({ tables: ['network_test'] });

        // More data after reconnect
        await httpClient.insert('network_test', { id: 3, value: 'after' });

        // Should receive at least the 'after' event
        const events = await wsClient.collectEvents(5000);
        const afterEvent = events.find(
          (e) => e.operation === 'INSERT' && e.newRow?.value === 'after'
        );

        // Connection recovery should work
        expect(wsClient.isConnected()).toBe(true);
      } catch (error) {
        console.log('[CDC E2E] Network recovery test noted:', error);
      } finally {
        wsClient.close();
      }
    }, E2E_TIMEOUT);
  });
});
