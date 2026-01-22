/**
 * WebSocket Message Size Validation Tests (TDD RED Phase)
 *
 * Issue: sql-zhy.3 - Tests for memory exhaustion via oversized JSON payloads
 *
 * VULNERABILITY: Current implementation performs JSON.parse BEFORE size validation.
 *
 * In dolake.ts webSocketMessage():
 * ```typescript
 * async webSocketMessage(ws: WebSocket, message: ArrayBuffer | string): Promise<void> {
 *   // ...
 *   const rpcMessage = this.decodeMessage(message); // JSON.parse happens HERE first
 *
 *   // Calculate payload size AFTER parsing
 *   const payloadSize = typeof message === 'string'
 *     ? new TextEncoder().encode(message).length
 *     : message.byteLength;
 *
 *   // Rate limit check happens AFTER memory already allocated
 *   const rateLimitResult = this.rateLimiter.checkMessage(..., payloadSize, ...);
 * ```
 *
 * The correct implementation should:
 * 1. Check raw message size BEFORE any parsing
 * 2. Reject oversized messages immediately without JSON.parse
 * 3. Use streaming parsers for valid large payloads
 * 4. Provide configurable size limits
 * 5. Return proper error responses for oversized messages
 *
 * These tests use `it.fails()` pattern to document expected behavior that is currently MISSING.
 * When using it.fails():
 * - Test PASSES in vitest = the inner assertions FAILED = behavior is MISSING (RED phase)
 * - Test FAILS in vitest = the inner assertions PASSED = behavior already exists
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import {
  type CDCEvent,
  type CDCBatchMessage,
  type NackMessage,
  generateUUID,
  DEFAULT_CLIENT_CAPABILITIES,
} from '../index.js';
import { DEFAULT_RATE_LIMIT_CONFIG } from '../rate-limiter.js';

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Helper to create a WebSocket connection to DoLake
 */
async function connectWebSocket(dolakeStub: DurableObjectStub): Promise<{
  client: WebSocket;
  response: Response;
}> {
  const response = await dolakeStub.fetch('http://dolake/ws', {
    headers: {
      Upgrade: 'websocket',
      'X-Client-ID': generateUUID(),
    },
  });

  const client = response.webSocket!;
  client.accept();

  return { client, response };
}

/**
 * Helper to send a message and wait for response
 */
async function sendAndReceive(
  ws: WebSocket,
  message: string | ArrayBuffer,
  timeoutMs: number = 2000
): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error('Timeout waiting for response'));
    }, timeoutMs);

    ws.addEventListener(
      'message',
      (event) => {
        clearTimeout(timeout);
        try {
          resolve(JSON.parse(event.data as string));
        } catch {
          resolve(event.data);
        }
      },
      { once: true }
    );

    ws.addEventListener(
      'error',
      (event) => {
        clearTimeout(timeout);
        reject(event);
      },
      { once: true }
    );

    ws.send(message);
  });
}

/**
 * Helper to create a valid CDC event
 */
function createValidCDCEvent(overrides: Partial<CDCEvent> = {}): CDCEvent {
  return {
    sequence: 1,
    timestamp: Date.now(),
    operation: 'INSERT',
    table: 'users',
    rowId: generateUUID(),
    after: { id: 1, name: 'Test User' },
    ...overrides,
  };
}

/**
 * Helper to create a valid CDCBatchMessage
 */
function createValidCDCBatchMessage(
  overrides: Partial<CDCBatchMessage> = {}
): CDCBatchMessage {
  return {
    type: 'cdc_batch',
    timestamp: Date.now(),
    sourceDoId: generateUUID(),
    events: [createValidCDCEvent()],
    sequenceNumber: 1,
    firstEventSequence: 1,
    lastEventSequence: 1,
    sizeBytes: 100,
    isRetry: false,
    retryCount: 0,
    ...overrides,
  };
}

/**
 * Create a string payload of specified size
 */
function createLargePayload(sizeBytes: number): string {
  const baseEvent = {
    type: 'cdc_batch',
    timestamp: Date.now(),
    sourceDoId: generateUUID(),
    events: [
      {
        sequence: 1,
        timestamp: Date.now(),
        operation: 'INSERT',
        table: 'large_data',
        rowId: generateUUID(),
        after: {
          id: 1,
          data: '',
        },
      },
    ],
    sequenceNumber: 1,
    firstEventSequence: 1,
    lastEventSequence: 1,
    sizeBytes: sizeBytes,
    isRetry: false,
    retryCount: 0,
  };

  const baseSize = JSON.stringify(baseEvent).length;
  const paddingNeeded = Math.max(0, sizeBytes - baseSize);
  baseEvent.events[0].after.data = 'x'.repeat(paddingNeeded);

  return JSON.stringify(baseEvent);
}

/**
 * Create a binary payload of specified size
 */
function createLargeBinaryPayload(sizeBytes: number): ArrayBuffer {
  const encoder = new TextEncoder();
  const payload = createLargePayload(sizeBytes);
  return encoder.encode(payload).buffer;
}

// =============================================================================
// 1. Pre-Parse Size Validation Tests
// =============================================================================

describe('Pre-Parse Message Size Validation', () => {
  /**
   * CRITICAL: Size validation MUST happen BEFORE JSON.parse to prevent memory exhaustion.
   * Currently, JSON.parse is called first, then size is checked - this is vulnerable.
   */

  it.fails('should check message size BEFORE calling JSON.parse', async () => {
    const id = env.DOLAKE.idFromName('test-pre-parse-order-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    // Create a payload that exceeds maxPayloadSize (default: 4MB)
    const oversizedPayload = createLargePayload(5 * 1024 * 1024); // 5MB

    try {
      const response = (await sendAndReceive(client, oversizedPayload)) as NackMessage;

      // EXPECTED: Should reject with payload_too_large BEFORE attempting JSON.parse
      // CURRENT BEHAVIOR: JSON.parse runs first, potentially causing memory exhaustion
      // Then size is checked AFTER the memory has already been allocated
      expect(response.type).toBe('nack');
      expect(response.reason).toBe('payload_too_large');

      // The key indicator is that rejection should happen without any Zod validation error
      // If we see 'invalid_format' it means JSON.parse already ran
      expect(response.reason).not.toBe('invalid_format');
      expect(response.maxSize).toBe(DEFAULT_RATE_LIMIT_CONFIG.maxPayloadSize);
    } finally {
      client.close();
    }
  });

  it.fails('should not invoke TextDecoder for oversized binary messages', async () => {
    const id = env.DOLAKE.idFromName('test-no-decode-oversize-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    // Create oversized binary payload
    const oversizedBinary = createLargeBinaryPayload(5 * 1024 * 1024);

    try {
      const response = (await sendAndReceive(client, oversizedBinary)) as NackMessage;

      // EXPECTED: Should check ArrayBuffer.byteLength BEFORE TextDecoder.decode()
      // Currently, decodeMessage() does: new TextDecoder().decode(message) first
      expect(response.type).toBe('nack');
      expect(response.reason).toBe('payload_too_large');
    } finally {
      client.close();
    }
  });

  it.fails('should have zero memory growth for rejected oversized payloads', async () => {
    const id = env.DOLAKE.idFromName('test-zero-mem-growth-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    const payloadSize = 5 * 1024 * 1024; // 5MB
    const payload = createLargePayload(payloadSize);

    try {
      // Get memory baseline
      const memBefore = await stub.fetch('http://dolake/v1/memory-stats');
      const beforeStats = await memBefore.json() as { currentUsageBytes: number; peakUsageBytes: number };

      // Send oversized message
      const response = (await sendAndReceive(client, payload)) as NackMessage;

      // Get memory after
      const memAfter = await stub.fetch('http://dolake/v1/memory-stats');
      const afterStats = await memAfter.json() as { currentUsageBytes: number; peakUsageBytes: number };

      // EXPECTED: Memory should NOT have spiked significantly
      // Current behavior: JSON.parse allocates ~5MB before we reject
      expect(response.type).toBe('nack');

      // Memory growth should be minimal (< 100KB) since we rejected BEFORE parsing
      const memGrowth = afterStats.peakUsageBytes - beforeStats.peakUsageBytes;
      expect(memGrowth).toBeLessThan(100 * 1024);
    } finally {
      client.close();
    }
  });
});

// =============================================================================
// 2. Size Limit Configuration Tests
// =============================================================================

describe('Size Limit Configuration', () => {
  /**
   * Size limits should be configurable per-DO instance and consistently enforced.
   */

  it.fails('should allow runtime configuration of maxPayloadSize', async () => {
    const id = env.DOLAKE.idFromName('test-config-max-payload-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // EXPECTED: Should be able to configure maxPayloadSize via API
    const configResponse = await stub.fetch('http://dolake/v1/config/rate-limits', {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        maxPayloadSize: 1024 * 1024, // 1MB
      }),
    });

    // Current behavior: No such endpoint exists
    expect(configResponse.ok).toBe(true);

    const { client } = await connectWebSocket(stub);

    // Create 2MB payload - over the new limit but under default
    const payload = createLargePayload(2 * 1024 * 1024);

    try {
      const response = (await sendAndReceive(client, payload)) as NackMessage;

      // Should reject based on the new custom limit
      expect(response.type).toBe('nack');
      expect(response.reason).toBe('payload_too_large');
      expect(response.maxSize).toBe(1024 * 1024);
    } finally {
      client.close();
    }
  });

  it.fails('should report all size limits in status endpoint', async () => {
    const id = env.DOLAKE.idFromName('test-status-size-limits-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const statusResponse = await stub.fetch('http://dolake/status');
    const status = await statusResponse.json() as {
      rateLimits: {
        maxPayloadSize: number;
        maxEventSize: number;
        maxMessageSize: number;
        maxEventsPerBatch: number;
      };
      sizeLimits?: {
        preParseMaxSize: number;
        streamingParseThreshold: number;
      };
    };

    // EXPECTED: Status should include all size-related limits
    expect(status.rateLimits).toBeDefined();
    expect(status.rateLimits.maxPayloadSize).toBe(DEFAULT_RATE_LIMIT_CONFIG.maxPayloadSize);
    expect(status.rateLimits.maxEventSize).toBe(DEFAULT_RATE_LIMIT_CONFIG.maxEventSize);

    // Additional size configuration that should exist
    expect(status.sizeLimits).toBeDefined();
    expect(status.sizeLimits?.preParseMaxSize).toBeDefined();
    expect(status.sizeLimits?.streamingParseThreshold).toBeDefined();
  });

  it.fails('should expose size limits via WebSocket connect response', async () => {
    const id = env.DOLAKE.idFromName('test-ws-size-limits-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    // Send connect message
    const connectMsg = {
      type: 'connect',
      timestamp: Date.now(),
      sourceDoId: generateUUID(),
      lastAckSequence: 0,
      protocolVersion: 1,
      capabilities: DEFAULT_CLIENT_CAPABILITIES,
    };

    try {
      const response = (await sendAndReceive(client, JSON.stringify(connectMsg))) as {
        type: string;
        sizeLimits?: {
          maxPayloadSize: number;
          maxEventSize: number;
        };
      };

      // EXPECTED: Status response should include size limits so client knows upfront
      expect(response.type).toBe('status');
      expect(response.sizeLimits).toBeDefined();
      expect(response.sizeLimits?.maxPayloadSize).toBe(DEFAULT_RATE_LIMIT_CONFIG.maxPayloadSize);
    } finally {
      client.close();
    }
  });
});

// =============================================================================
// 3. Memory Protection Tests
// =============================================================================

describe('Memory Protection', () => {
  /**
   * System must be protected against memory exhaustion attacks.
   */

  it.fails('should enforce strict memory budget per connection', async () => {
    const id = env.DOLAKE.idFromName('test-mem-budget-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    try {
      // Send multiple valid payloads near the limit
      const nearLimitSize = 3 * 1024 * 1024; // 3MB
      const numPayloads = 5;

      for (let i = 0; i < numPayloads; i++) {
        const payload = createLargePayload(nearLimitSize);
        await sendAndReceive(client, payload, 5000);
      }

      // Get memory stats
      const memStats = await stub.fetch('http://dolake/v1/memory-stats');
      const stats = await memStats.json() as {
        connectionMemoryBudget: number;
        currentConnectionMemory: Record<string, number>;
      };

      // EXPECTED: Should track and enforce per-connection memory budget
      expect(stats.connectionMemoryBudget).toBeDefined();
      expect(Object.keys(stats.currentConnectionMemory).length).toBeGreaterThan(0);
    } finally {
      client.close();
    }
  });

  it.fails('should implement memory pressure detection', async () => {
    const id = env.DOLAKE.idFromName('test-mem-pressure-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    try {
      // Simulate high memory usage
      const payloads: string[] = [];
      for (let i = 0; i < 3; i++) {
        payloads.push(createLargePayload(3 * 1024 * 1024));
      }

      // Send all at once
      const responses = await Promise.all(
        payloads.map(p => sendAndReceive(client, p, 5000).catch(e => ({ error: e.message })))
      );

      // Check for memory pressure response
      const nacks = responses.filter((r: any) => r.type === 'nack' && r.reason === 'memory_pressure');

      // EXPECTED: Should detect memory pressure and start rejecting
      expect(nacks.length).toBeGreaterThan(0);
    } finally {
      client.close();
    }
  });

  it.fails('should track cumulative bytes received per connection', async () => {
    const id = env.DOLAKE.idFromName('test-cumulative-bytes-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    const payloadSize = 100 * 1024; // 100KB
    const numMessages = 10;

    try {
      for (let i = 0; i < numMessages; i++) {
        const msg = createValidCDCBatchMessage({ sequenceNumber: i + 1 });
        const payload = JSON.stringify(msg);
        await sendAndReceive(client, payload);
      }

      // Get connection stats
      const memStats = await stub.fetch('http://dolake/v1/memory-stats');
      const stats = await memStats.json() as {
        totalBytesReceived: number;
        bytesReceivedPerConnection: Record<string, number>;
      };

      // EXPECTED: Should track total bytes received
      expect(stats.totalBytesReceived).toBeGreaterThan(0);
      expect(Object.values(stats.bytesReceivedPerConnection).some(b => b > 0)).toBe(true);
    } finally {
      client.close();
    }
  });
});

// =============================================================================
// 4. Error Response Quality Tests
// =============================================================================

describe('Error Response Quality', () => {
  /**
   * Error responses for size violations must be informative.
   */

  it.fails('should include actualSize in oversized message rejection', async () => {
    const id = env.DOLAKE.idFromName('test-actual-size-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    const payloadSize = 5 * 1024 * 1024;
    const payload = createLargePayload(payloadSize);

    try {
      const response = (await sendAndReceive(client, payload)) as NackMessage & {
        actualSize?: number;
        receivedSize?: number;
      };

      // EXPECTED: Response should include the actual/received size
      expect(response.type).toBe('nack');
      expect(response.actualSize ?? response.receivedSize).toBeDefined();
      expect(response.actualSize ?? response.receivedSize).toBeGreaterThanOrEqual(payloadSize);
    } finally {
      client.close();
    }
  });

  it.fails('should include human-readable size in error message', async () => {
    const id = env.DOLAKE.idFromName('test-human-readable-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    const payload = createLargePayload(5 * 1024 * 1024);

    try {
      const response = (await sendAndReceive(client, payload)) as NackMessage;

      // EXPECTED: Error message should include human-readable sizes
      expect(response.type).toBe('nack');
      expect(response.errorMessage).toMatch(/MB|KB|bytes/i);
      expect(response.errorMessage).toMatch(/exceeded|too large|limit/i);
    } finally {
      client.close();
    }
  });

  it.fails('should use sequenceNumber 0 for pre-parse rejections', async () => {
    const id = env.DOLAKE.idFromName('test-seq-zero-preparse-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    const payload = createLargePayload(5 * 1024 * 1024);

    try {
      const response = (await sendAndReceive(client, payload)) as NackMessage;

      // EXPECTED: When rejected before parsing, we can't know the sequence number
      expect(response.type).toBe('nack');
      expect(response.reason).toBe('payload_too_large');
      expect(response.sequenceNumber).toBe(0);
    } finally {
      client.close();
    }
  });
});

// =============================================================================
// 5. Streaming Parser Tests
// =============================================================================

describe('Streaming Parser for Large Valid Payloads', () => {
  /**
   * Large but valid payloads should be handled efficiently.
   */

  it.fails('should use streaming JSON parser for payloads over threshold', async () => {
    const id = env.DOLAKE.idFromName('test-streaming-parser-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    // Create a valid payload that should trigger streaming mode
    const streamingThreshold = 1 * 1024 * 1024; // Assume 1MB threshold
    const payload = createLargePayload(streamingThreshold + 100 * 1024);

    try {
      const response = (await sendAndReceive(client, payload, 10000)) as {
        type: string;
        details?: {
          parsingMode?: string;
        };
      };

      // EXPECTED: Should indicate streaming parsing was used
      expect(response.type).toBe('ack');
      expect(response.details?.parsingMode).toBe('streaming');
    } finally {
      client.close();
    }
  });

  it.fails('should process events incrementally during streaming parse', async () => {
    const id = env.DOLAKE.idFromName('test-incremental-events-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    // Create batch with many events
    const numEvents = 1000;
    const events = Array.from({ length: numEvents }, (_, i) => ({
      sequence: i + 1,
      timestamp: Date.now(),
      operation: 'INSERT' as const,
      table: 'batch_test',
      rowId: generateUUID(),
      after: { id: i + 1, data: 'x'.repeat(1000) },
    }));

    const message = {
      type: 'cdc_batch',
      timestamp: Date.now(),
      sourceDoId: generateUUID(),
      events,
      sequenceNumber: 1,
      firstEventSequence: 1,
      lastEventSequence: numEvents,
      sizeBytes: 0,
      isRetry: false,
      retryCount: 0,
    };

    try {
      const response = (await sendAndReceive(client, JSON.stringify(message), 10000)) as {
        type: string;
        details?: {
          eventsProcessed: number;
          processingMode?: string;
          incrementalProcessing?: boolean;
        };
      };

      // EXPECTED: Should report incremental processing
      expect(response.type).toBe('ack');
      expect(response.details?.eventsProcessed).toBe(numEvents);
      expect(response.details?.incrementalProcessing).toBe(true);
    } finally {
      client.close();
    }
  });

  it.fails('should abort streaming parse on finding invalid event early', async () => {
    const id = env.DOLAKE.idFromName('test-streaming-abort-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    // Create a large batch with an invalid event early
    const validEvents = Array.from({ length: 100 }, (_, i) => ({
      sequence: i + 1,
      timestamp: Date.now(),
      operation: 'INSERT' as const,
      table: 'test',
      rowId: generateUUID(),
      after: { id: i + 1 },
    }));

    // Insert invalid event at position 5
    const invalidEvent = {
      sequence: 'not-a-number', // Invalid type
      timestamp: Date.now(),
      operation: 'INSERT',
      table: 'test',
      rowId: generateUUID(),
    };

    const events = [...validEvents.slice(0, 5), invalidEvent, ...validEvents.slice(5)];

    const message = {
      type: 'cdc_batch',
      timestamp: Date.now(),
      sourceDoId: generateUUID(),
      events,
      sequenceNumber: 1,
      firstEventSequence: 1,
      lastEventSequence: events.length,
      sizeBytes: 0,
      isRetry: false,
      retryCount: 0,
    };

    try {
      const response = (await sendAndReceive(client, JSON.stringify(message))) as NackMessage & {
        failedAtEvent?: number;
      };

      // EXPECTED: Should fail early and report which event failed
      expect(response.type).toBe('nack');
      expect(response.reason).toBe('invalid_format');
      expect(response.failedAtEvent).toBe(5); // Should report the failing event index
    } finally {
      client.close();
    }
  });
});

// =============================================================================
// 6. Connection Behavior Tests
// =============================================================================

describe('Connection Behavior with Size Violations', () => {
  /**
   * Repeated size violations should affect connection state.
   */

  it.fails('should track size violations separately from other violations', async () => {
    const id = env.DOLAKE.idFromName('test-separate-tracking-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    try {
      // Send oversized payload
      const oversized = createLargePayload(5 * 1024 * 1024);
      await sendAndReceive(client, oversized);

      // Check metrics
      const metricsResponse = await stub.fetch('http://dolake/metrics');
      const metrics = await metricsResponse.text();

      // EXPECTED: Should have separate counter for pre-parse size rejections
      expect(metrics).toContain('dolake_preparse_size_rejections');
      expect(metrics).toMatch(/dolake_preparse_size_rejections\s+\d+/);
    } finally {
      client.close();
    }
  });

  it.fails('should implement exponential backoff for size violation retries', async () => {
    const id = env.DOLAKE.idFromName('test-backoff-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    const oversized = createLargePayload(5 * 1024 * 1024);

    try {
      const responses: NackMessage[] = [];

      for (let i = 0; i < 3; i++) {
        const response = (await sendAndReceive(client, oversized)) as NackMessage;
        responses.push(response);
      }

      // EXPECTED: Should suggest increasing backoff delays
      // Note: For size errors, should NOT suggest retry at all (shouldRetry: false)
      for (const response of responses) {
        expect(response.shouldRetry).toBe(false);
      }
    } finally {
      client.close();
    }
  });

  it.fails('should warn before closing connection due to size violations', async () => {
    const id = env.DOLAKE.idFromName('test-warn-before-close-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    const oversized = createLargePayload(5 * 1024 * 1024);

    try {
      // Send violations approaching the limit
      const responses: (NackMessage & { warning?: string; violationsRemaining?: number })[] = [];

      for (let i = 0; i < 2; i++) {
        const response = (await sendAndReceive(client, oversized)) as NackMessage & {
          warning?: string;
          violationsRemaining?: number;
        };
        responses.push(response);
      }

      // EXPECTED: Should warn about impending connection close
      const lastResponse = responses[responses.length - 1];
      expect(lastResponse.warning).toBeDefined();
      expect(lastResponse.warning).toMatch(/violation|close|connection/i);
      expect(lastResponse.violationsRemaining).toBeDefined();
    } finally {
      client.close();
    }
  });
});

// =============================================================================
// 7. Binary Message Handling Tests
// =============================================================================

describe('Binary Message Size Handling', () => {
  /**
   * Binary (ArrayBuffer) messages need special handling.
   */

  it.fails('should check ArrayBuffer.byteLength before any decoding', async () => {
    const id = env.DOLAKE.idFromName('test-arraybuffer-check-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    const oversizedBinary = createLargeBinaryPayload(5 * 1024 * 1024);

    try {
      const response = (await sendAndReceive(client, oversizedBinary)) as NackMessage;

      // EXPECTED: Should check byteLength immediately, before TextDecoder
      expect(response.type).toBe('nack');
      expect(response.reason).toBe('payload_too_large');

      // Should not have attempted to decode (no decode error)
      expect(response.errorMessage).not.toContain('decode');
    } finally {
      client.close();
    }
  });

  it.fails('should validate UTF-8 validity before size-based JSON parse', async () => {
    const id = env.DOLAKE.idFromName('test-utf8-validity-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    // Create invalid UTF-8 sequence
    const invalidUtf8 = new Uint8Array([0x80, 0x81, 0x82, 0x83]); // Invalid continuation bytes

    try {
      const response = (await sendAndReceive(client, invalidUtf8.buffer)) as NackMessage;

      // EXPECTED: Should detect invalid UTF-8 with specific error
      expect(response.type).toBe('nack');
      expect(response.reason).toMatch(/invalid_format|invalid_encoding/);
    } finally {
      client.close();
    }
  });

  it.fails('should handle mixed binary and text messages consistently', async () => {
    const id = env.DOLAKE.idFromName('test-mixed-format-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    const validMessage = createValidCDCBatchMessage();
    const jsonString = JSON.stringify(validMessage);
    const binaryPayload = new TextEncoder().encode(jsonString).buffer;

    try {
      // Send same logical message as both string and binary
      const stringResponse = (await sendAndReceive(client, jsonString)) as { type: string };
      const binaryResponse = (await sendAndReceive(client, binaryPayload)) as { type: string };

      // EXPECTED: Both should be processed identically
      expect(stringResponse.type).toBe('ack');
      expect(binaryResponse.type).toBe('ack');
    } finally {
      client.close();
    }
  });
});

// =============================================================================
// 8. Rate Limit Integration Tests
// =============================================================================

describe('Size Validation and Rate Limiting Integration', () => {
  /**
   * Size validation should integrate properly with rate limiting.
   */

  it.fails('should count pre-parse rejections in rate limit metrics', async () => {
    const id = env.DOLAKE.idFromName('test-preparse-metrics-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    // Get initial metrics
    const initialMetrics = await stub.fetch('http://dolake/metrics');
    const initialText = await initialMetrics.text();
    const initialCount = parseInt(
      initialText.match(/dolake_preparse_size_rejections\s+(\d+)/)?.[1] || '0'
    );

    try {
      // Send oversized payload
      const oversized = createLargePayload(5 * 1024 * 1024);
      await sendAndReceive(client, oversized);

      // Get updated metrics
      const updatedMetrics = await stub.fetch('http://dolake/metrics');
      const updatedText = await updatedMetrics.text();
      const updatedCount = parseInt(
        updatedText.match(/dolake_preparse_size_rejections\s+(\d+)/)?.[1] || '0'
      );

      // EXPECTED: Should have incremented pre-parse rejection counter
      expect(updatedCount).toBe(initialCount + 1);
    } finally {
      client.close();
    }
  });

  it.fails('should NOT consume rate limit tokens for pre-parse rejections', async () => {
    const id = env.DOLAKE.idFromName('test-no-token-consume-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    try {
      // Get initial token count via a valid message
      const validMsg = createValidCDCBatchMessage({ sequenceNumber: 1 });
      const initialResponse = (await sendAndReceive(client, JSON.stringify(validMsg))) as {
        type: string;
        rateLimit?: { remaining: number };
      };
      const initialRemaining = initialResponse.rateLimit?.remaining ?? 100;

      // Send oversized payload
      const oversized = createLargePayload(5 * 1024 * 1024);
      await sendAndReceive(client, oversized);

      // Send another valid message and check tokens
      const validMsg2 = createValidCDCBatchMessage({ sequenceNumber: 2 });
      const afterResponse = (await sendAndReceive(client, JSON.stringify(validMsg2))) as {
        type: string;
        rateLimit?: { remaining: number };
      };

      // EXPECTED: Pre-parse rejection should not have consumed a token
      // Only one token should have been consumed (for the second valid message)
      expect(afterResponse.rateLimit?.remaining).toBeGreaterThanOrEqual(initialRemaining - 2);
    } finally {
      client.close();
    }
  });
});

// =============================================================================
// 9. Edge Cases
// =============================================================================

describe('Message Size Edge Cases', () => {
  /**
   * Edge cases around size boundaries.
   */

  it.fails('should accept payload exactly at maxPayloadSize', async () => {
    const id = env.DOLAKE.idFromName('test-exact-max-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    // Create payload exactly at the limit
    const maxSize = DEFAULT_RATE_LIMIT_CONFIG.maxPayloadSize;
    let payload = createLargePayload(maxSize - 100);

    // Adjust to exactly hit the limit
    const currentSize = new TextEncoder().encode(payload).length;
    const diff = maxSize - currentSize;
    if (diff > 0 && diff < 1000) {
      const parsed = JSON.parse(payload);
      parsed.events[0].after.data += 'x'.repeat(diff);
      payload = JSON.stringify(parsed);
    }

    try {
      const response = (await sendAndReceive(client, payload, 10000)) as { type: string };

      // EXPECTED: Exactly at limit should be accepted
      expect(response.type).toBe('ack');
    } finally {
      client.close();
    }
  });

  it.fails('should reject payload one byte over maxPayloadSize', async () => {
    const id = env.DOLAKE.idFromName('test-one-over-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    const maxSize = DEFAULT_RATE_LIMIT_CONFIG.maxPayloadSize;
    const payload = createLargePayload(maxSize + 1);

    try {
      const response = (await sendAndReceive(client, payload)) as NackMessage;

      // EXPECTED: Even one byte over should be rejected
      expect(response.type).toBe('nack');
      expect(response.reason).toBe('payload_too_large');
    } finally {
      client.close();
    }
  });

  it.fails('should handle zero-length message gracefully', async () => {
    const id = env.DOLAKE.idFromName('test-zero-length-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    try {
      const response = (await sendAndReceive(client, '')) as NackMessage;

      // EXPECTED: Empty message should be rejected with clear error
      expect(response.type).toBe('nack');
      expect(response.reason).toBe('invalid_format');
      expect(response.errorMessage).toMatch(/empty|zero|no data/i);
    } finally {
      client.close();
    }
  });

  it.fails('should handle null byte in message', async () => {
    const id = env.DOLAKE.idFromName('test-null-byte-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    // Create message with embedded null bytes
    const validMsg = JSON.stringify(createValidCDCBatchMessage());
    const withNull = validMsg.slice(0, 50) + '\x00' + validMsg.slice(50);

    try {
      const response = (await sendAndReceive(client, withNull)) as NackMessage;

      // EXPECTED: Should handle null byte gracefully
      expect(response.type).toBe('nack');
    } finally {
      client.close();
    }
  });
});

// =============================================================================
// 10. Concurrent Handling Tests
// =============================================================================

describe('Concurrent Size Violation Handling', () => {
  /**
   * Multiple simultaneous size violations should be handled safely.
   */

  it.fails('should handle concurrent oversized messages without crash', async () => {
    const id = env.DOLAKE.idFromName('test-concurrent-oversized-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Create multiple connections
    const connections = await Promise.all([
      connectWebSocket(stub),
      connectWebSocket(stub),
      connectWebSocket(stub),
    ]);

    const oversized = createLargePayload(5 * 1024 * 1024);

    try {
      // Send from all connections simultaneously
      const responses = await Promise.all(
        connections.map(({ client }) =>
          sendAndReceive(client, oversized).catch(e => ({ error: e.message }))
        )
      );

      // EXPECTED: All should be rejected properly
      for (const response of responses) {
        if ('error' in response) continue;
        expect((response as NackMessage).type).toBe('nack');
        expect((response as NackMessage).reason).toBe('payload_too_large');
      }
    } finally {
      connections.forEach(({ client }) => client.close());
    }
  });

  it.fails('should maintain memory bounds under sustained attack', async () => {
    const id = env.DOLAKE.idFromName('test-sustained-attack-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Get baseline memory
    const baselineMem = await stub.fetch('http://dolake/v1/memory-stats');
    const baseline = await baselineMem.json() as { currentUsageBytes: number };

    // Simulate sustained attack
    const numAttacks = 10;
    const oversized = createLargePayload(5 * 1024 * 1024);

    for (let i = 0; i < numAttacks; i++) {
      const { client } = await connectWebSocket(stub);
      try {
        await sendAndReceive(client, oversized).catch(() => {});
      } finally {
        client.close();
      }
    }

    // Check final memory
    const finalMem = await stub.fetch('http://dolake/v1/memory-stats');
    const final = await finalMem.json() as { currentUsageBytes: number };

    // EXPECTED: Memory should not grow unboundedly
    const growth = final.currentUsageBytes - baseline.currentUsageBytes;
    expect(growth).toBeLessThan(10 * 1024 * 1024); // Less than 10MB growth
  });
});
