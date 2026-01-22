/**
 * DoLake Durability Tiers Tests (TDD RED Phase)
 *
 * Tests for event durability classification and write behavior.
 * Different events need different durability guarantees:
 *
 * P0 Critical (Stripe webhooks, payments)
 * - Always write to R2 AND KV
 * - Retry until confirmed
 * - Dead letter queue on permanent failure
 *
 * P1 Important (user actions, signups)
 * - Write to R2, fallback to KV on failure
 * - Retry 3x with exponential backoff
 *
 * P2 Standard (analytics, telemetry)
 * - Write to R2 only
 * - Fallback to DO fsx VFS on R2 failure
 * - Retry from VFS on next flush
 *
 * P3 Best-effort (anonymous visits)
 * - Write to R2
 * - Drop on failure (acceptable loss)
 *
 * Issue: do-d1isn.4 - RED: Durability tiers (P0-P3)
 *
 * Uses workers-vitest-pool (NO MOCKS).
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import {
  type CDCEvent,
  type CDCBatchMessage,
  generateUUID,
  DEFAULT_CLIENT_CAPABILITIES,
} from '../src/index.js';

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Helper to create a WebSocket connection to DoLake
 */
async function connectWebSocket(
  dolakeStub: DurableObjectStub,
  headers: Record<string, string> = {}
): Promise<{
  client: WebSocket;
  response: Response;
}> {
  const response = await dolakeStub.fetch('http://dolake/ws', {
    headers: {
      Upgrade: 'websocket',
      'X-Client-ID': generateUUID(),
      ...headers,
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
  message: unknown,
  timeoutMs: number = 5000
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

    ws.send(typeof message === 'string' ? message : JSON.stringify(message));
  });
}

/**
 * Helper to create a valid CDC event with specific type
 */
function createCDCEvent(overrides: Partial<CDCEvent> = {}): CDCEvent {
  return {
    sequence: 1,
    timestamp: Date.now(),
    operation: 'INSERT',
    table: 'events',
    rowId: generateUUID(),
    after: { id: 1, data: 'test' },
    ...overrides,
  };
}

/**
 * Helper to create a valid CDCBatchMessage
 */
function createCDCBatchMessage(
  overrides: Partial<CDCBatchMessage> = {}
): CDCBatchMessage {
  return {
    type: 'cdc_batch',
    timestamp: Date.now(),
    sourceDoId: generateUUID(),
    events: [createCDCEvent()],
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
 * Create a P0 critical event (e.g., Stripe webhook)
 */
function createP0Event(overrides: Partial<CDCEvent> = {}): CDCEvent {
  return createCDCEvent({
    table: 'stripe_webhooks',
    after: {
      id: generateUUID(),
      type: 'payment_intent.succeeded',
      amount: 10000,
      currency: 'usd',
    },
    metadata: {
      durability: 'P0',
      source: 'stripe',
      idempotencyKey: generateUUID(),
    },
    ...overrides,
  });
}

/**
 * Create a P1 important event (e.g., user signup)
 */
function createP1Event(overrides: Partial<CDCEvent> = {}): CDCEvent {
  return createCDCEvent({
    table: 'users',
    after: {
      id: generateUUID(),
      email: 'user@example.com',
      name: 'Test User',
      created_at: new Date().toISOString(),
    },
    metadata: {
      durability: 'P1',
      source: 'auth',
    },
    ...overrides,
  });
}

/**
 * Create a P2 standard event (e.g., analytics)
 */
function createP2Event(overrides: Partial<CDCEvent> = {}): CDCEvent {
  return createCDCEvent({
    table: 'analytics_events',
    after: {
      event_type: 'page_view',
      user_id: generateUUID(),
      page: '/dashboard',
      timestamp: Date.now(),
    },
    metadata: {
      durability: 'P2',
      source: 'analytics',
    },
    ...overrides,
  });
}

/**
 * Create a P3 best-effort event (e.g., anonymous visit)
 */
function createP3Event(overrides: Partial<CDCEvent> = {}): CDCEvent {
  return createCDCEvent({
    table: 'anonymous_visits',
    after: {
      visitor_id: generateUUID(),
      page: '/landing',
      referrer: 'https://google.com',
      timestamp: Date.now(),
    },
    metadata: {
      durability: 'P3',
      source: 'tracking',
    },
    ...overrides,
  });
}

// =============================================================================
// 1. Event Classification Tests
// =============================================================================

describe('Event Classification', () => {
  /**
   * RED Phase: Event classification is not implemented.
   * Expected behavior: Events should be classified into durability tiers
   * based on their source, table, and metadata.
   */

  it('should classify stripe webhook as P0 critical', async () => {
    const id = env.DOLAKE.idFromName('test-classify-p0-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/classify-event', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ event: createP0Event() }),
    });

    expect(response.status).toBe(200);
    const result = await response.json() as { tier: string };

    // EXPECTED: Stripe webhooks should be P0
    expect(result.tier).toBe('P0');
  });

  it('should classify payment events as P0 critical', async () => {
    const id = env.DOLAKE.idFromName('test-classify-payment-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const paymentEvent = createCDCEvent({
      table: 'payments',
      after: {
        id: generateUUID(),
        amount: 5000,
        status: 'completed',
      },
    });

    const response = await stub.fetch('http://dolake/v1/classify-event', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ event: paymentEvent }),
    });

    expect(response.status).toBe(200);
    const result = await response.json() as { tier: string };

    // EXPECTED: Payment events should be P0
    expect(result.tier).toBe('P0');
  });

  it('should classify user signup as P1 important', async () => {
    const id = env.DOLAKE.idFromName('test-classify-p1-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/classify-event', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ event: createP1Event() }),
    });

    expect(response.status).toBe(200);
    const result = await response.json() as { tier: string };

    // EXPECTED: User signups should be P1
    expect(result.tier).toBe('P1');
  });

  it('should classify user action events as P1 important', async () => {
    const id = env.DOLAKE.idFromName('test-classify-user-action-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const userActionEvent = createCDCEvent({
      table: 'user_actions',
      after: {
        user_id: generateUUID(),
        action: 'profile_update',
        timestamp: Date.now(),
      },
    });

    const response = await stub.fetch('http://dolake/v1/classify-event', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ event: userActionEvent }),
    });

    expect(response.status).toBe(200);
    const result = await response.json() as { tier: string };

    // EXPECTED: User actions should be P1
    expect(result.tier).toBe('P1');
  });

  it('should classify analytics event as P2 standard', async () => {
    const id = env.DOLAKE.idFromName('test-classify-p2-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/classify-event', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ event: createP2Event() }),
    });

    expect(response.status).toBe(200);
    const result = await response.json() as { tier: string };

    // EXPECTED: Analytics events should be P2
    expect(result.tier).toBe('P2');
  });

  it('should classify telemetry event as P2 standard', async () => {
    const id = env.DOLAKE.idFromName('test-classify-telemetry-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const telemetryEvent = createCDCEvent({
      table: 'telemetry',
      after: {
        metric: 'cpu_usage',
        value: 75.5,
        timestamp: Date.now(),
      },
    });

    const response = await stub.fetch('http://dolake/v1/classify-event', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ event: telemetryEvent }),
    });

    expect(response.status).toBe(200);
    const result = await response.json() as { tier: string };

    // EXPECTED: Telemetry events should be P2
    expect(result.tier).toBe('P2');
  });

  it('should classify anonymous visit as P3 best-effort', async () => {
    const id = env.DOLAKE.idFromName('test-classify-p3-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/classify-event', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ event: createP3Event() }),
    });

    expect(response.status).toBe(200);
    const result = await response.json() as { tier: string };

    // EXPECTED: Anonymous visits should be P3
    expect(result.tier).toBe('P3');
  });

  it('should respect explicit durability metadata', async () => {
    const id = env.DOLAKE.idFromName('test-classify-explicit-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Event with explicit P0 durability even though table would suggest P2
    const eventWithExplicitDurability = createCDCEvent({
      table: 'analytics_events',
      metadata: {
        durability: 'P0', // Explicit override
      },
    });

    const response = await stub.fetch('http://dolake/v1/classify-event', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ event: eventWithExplicitDurability }),
    });

    expect(response.status).toBe(200);
    const result = await response.json() as { tier: string };

    // EXPECTED: Explicit durability should be respected
    expect(result.tier).toBe('P0');
  });

  it('should default to P2 for unknown event types', async () => {
    const id = env.DOLAKE.idFromName('test-classify-unknown-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const unknownEvent = createCDCEvent({
      table: 'unknown_table',
      after: { data: 'test' },
    });

    const response = await stub.fetch('http://dolake/v1/classify-event', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ event: unknownEvent }),
    });

    expect(response.status).toBe(200);
    const result = await response.json() as { tier: string };

    // EXPECTED: Unknown events should default to P2
    expect(result.tier).toBe('P2');
  });

  it('should classify batch of events with mixed tiers', async () => {
    const id = env.DOLAKE.idFromName('test-classify-batch-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const events = [
      createP0Event(),
      createP1Event(),
      createP2Event(),
      createP3Event(),
    ];

    const response = await stub.fetch('http://dolake/v1/classify-events', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ events }),
    });

    expect(response.status).toBe(200);
    const result = await response.json() as { classifications: Array<{ tier: string }> };

    // EXPECTED: Each event should be correctly classified
    expect(result.classifications).toHaveLength(4);
    expect(result.classifications[0].tier).toBe('P0');
    expect(result.classifications[1].tier).toBe('P1');
    expect(result.classifications[2].tier).toBe('P2');
    expect(result.classifications[3].tier).toBe('P3');
  });
});

// =============================================================================
// 2. P0 Dual-Write Behavior Tests
// =============================================================================

describe('P0 Critical - Dual Write (R2+KV)', () => {
  /**
   * RED Phase: P0 dual-write is not implemented.
   * Expected behavior: P0 events should be written to both R2 AND KV
   * with retries until confirmed.
   */

  it('should write P0 event to both R2 and KV', async () => {
    const id = env.DOLAKE.idFromName('test-p0-dual-write-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    try {
      const p0Event = createP0Event();
      const message = createCDCBatchMessage({
        events: [p0Event],
      });

      const response = await sendAndReceive(client, message) as {
        type: string;
        details?: {
          writtenToR2?: boolean;
          writtenToKV?: boolean;
          durabilityTier?: string;
        };
      };

      // EXPECTED: P0 should confirm writes to both R2 and KV
      expect(response.type).toBe('ack');
      expect(response.details?.durabilityTier).toBe('P0');
      expect(response.details?.writtenToR2).toBe(true);
      expect(response.details?.writtenToKV).toBe(true);
    } finally {
      client.close();
    }
  });

  it('should retry P0 event until both writes confirmed', async () => {
    const id = env.DOLAKE.idFromName('test-p0-retry-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Inject failure for first attempt
    await stub.fetch('http://dolake/v1/test/inject-write-failure', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target: 'R2', failCount: 2 }),
    });

    const { client } = await connectWebSocket(stub);

    try {
      const p0Event = createP0Event();
      const message = createCDCBatchMessage({
        events: [p0Event],
      });

      const response = await sendAndReceive(client, message, 10000) as {
        type: string;
        details?: {
          retryCount?: number;
          writtenToR2?: boolean;
          writtenToKV?: boolean;
        };
      };

      // EXPECTED: Should retry and eventually succeed
      expect(response.type).toBe('ack');
      expect(response.details?.retryCount).toBeGreaterThan(0);
      expect(response.details?.writtenToR2).toBe(true);
      expect(response.details?.writtenToKV).toBe(true);
    } finally {
      client.close();
    }
  });

  it('should send P0 event to dead letter queue on permanent failure', async () => {
    const id = env.DOLAKE.idFromName('test-p0-dlq-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Inject permanent failure
    await stub.fetch('http://dolake/v1/test/inject-write-failure', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target: 'R2', failCount: 100, permanent: true }),
    });

    const { client } = await connectWebSocket(stub);

    try {
      const p0Event = createP0Event();
      const message = createCDCBatchMessage({
        events: [p0Event],
      });

      const response = await sendAndReceive(client, message, 15000) as {
        type: string;
        details?: {
          sentToDLQ?: boolean;
          dlqPath?: string;
        };
      };

      // EXPECTED: Event should go to dead letter queue
      expect(response.details?.sentToDLQ).toBe(true);
      expect(response.details?.dlqPath).toBeDefined();
    } finally {
      client.close();
    }
  });

  it('should maintain write order for P0 events', async () => {
    const id = env.DOLAKE.idFromName('test-p0-order-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    try {
      const events = [
        createP0Event({ sequence: 1 }),
        createP0Event({ sequence: 2 }),
        createP0Event({ sequence: 3 }),
      ];

      for (const event of events) {
        const message = createCDCBatchMessage({
          events: [event],
          sequenceNumber: event.sequence,
        });
        await sendAndReceive(client, message);
      }

      // Verify write order
      const statusResponse = await stub.fetch('http://dolake/v1/durability/write-order');
      const status = await statusResponse.json() as {
        p0WriteOrder: number[];
      };

      // EXPECTED: Events should be written in order
      expect(status.p0WriteOrder).toEqual([1, 2, 3]);
    } finally {
      client.close();
    }
  });

  it('should use idempotency key for P0 deduplication', async () => {
    const id = env.DOLAKE.idFromName('test-p0-idempotency-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    try {
      const idempotencyKey = generateUUID();
      const p0Event = createP0Event({
        metadata: {
          durability: 'P0',
          idempotencyKey,
        },
      });

      // Send same event twice
      const message1 = createCDCBatchMessage({ events: [p0Event], sequenceNumber: 1 });
      const message2 = createCDCBatchMessage({ events: [p0Event], sequenceNumber: 2 });

      const response1 = await sendAndReceive(client, message1) as {
        type: string;
        status?: string;
      };
      const response2 = await sendAndReceive(client, message2) as {
        type: string;
        status?: string;
      };

      // EXPECTED: Second write should be deduplicated
      expect(response1.type).toBe('ack');
      expect(response2.type).toBe('ack');
      expect(response2.status).toBe('duplicate');
    } finally {
      client.close();
    }
  });
});

// =============================================================================
// 3. P1 Fallback Behavior Tests (R2 -> KV)
// =============================================================================

describe('P1 Important - Fallback (R2 -> KV)', () => {
  /**
   * RED Phase: P1 fallback is not implemented.
   * Expected behavior: P1 events should write to R2, falling back to KV
   * on failure with 3x retry using exponential backoff.
   */

  it('should write P1 event to R2 primarily', async () => {
    const id = env.DOLAKE.idFromName('test-p1-r2-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    try {
      const p1Event = createP1Event();
      const message = createCDCBatchMessage({
        events: [p1Event],
      });

      const response = await sendAndReceive(client, message) as {
        type: string;
        details?: {
          writtenToR2?: boolean;
          writtenToKV?: boolean;
          durabilityTier?: string;
        };
      };

      // EXPECTED: P1 should write to R2 primarily
      expect(response.type).toBe('ack');
      expect(response.details?.durabilityTier).toBe('P1');
      expect(response.details?.writtenToR2).toBe(true);
      // KV may or may not be used depending on success
    } finally {
      client.close();
    }
  });

  it('should fallback to KV when R2 fails for P1', async () => {
    const id = env.DOLAKE.idFromName('test-p1-fallback-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Inject R2 failure
    await stub.fetch('http://dolake/v1/test/inject-write-failure', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target: 'R2', failCount: 5 }),
    });

    const { client } = await connectWebSocket(stub);

    try {
      const p1Event = createP1Event();
      const message = createCDCBatchMessage({
        events: [p1Event],
      });

      const response = await sendAndReceive(client, message, 10000) as {
        type: string;
        status?: string;
        details?: {
          writtenToR2?: boolean;
          writtenToKV?: boolean;
          usedFallback?: boolean;
        };
      };

      // EXPECTED: Should fallback to KV
      expect(response.type).toBe('ack');
      expect(response.status).toBe('fallback');
      expect(response.details?.usedFallback).toBe(true);
      expect(response.details?.writtenToKV).toBe(true);
    } finally {
      client.close();
    }
  });

  it('should retry P1 event 3x with exponential backoff', async () => {
    const id = env.DOLAKE.idFromName('test-p1-retry-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Inject failure for exactly 2 attempts
    await stub.fetch('http://dolake/v1/test/inject-write-failure', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target: 'R2', failCount: 2 }),
    });

    const { client } = await connectWebSocket(stub);

    try {
      const p1Event = createP1Event();
      const message = createCDCBatchMessage({
        events: [p1Event],
      });

      const startTime = Date.now();
      const response = await sendAndReceive(client, message, 15000) as {
        type: string;
        details?: {
          retryCount?: number;
          retryDelays?: number[];
        };
      };
      const duration = Date.now() - startTime;

      // EXPECTED: Should retry with exponential backoff
      expect(response.type).toBe('ack');
      expect(response.details?.retryCount).toBe(2);

      // Verify exponential backoff (delays should increase)
      const delays = response.details?.retryDelays ?? [];
      if (delays.length >= 2) {
        expect(delays[1]).toBeGreaterThan(delays[0]);
      }
    } finally {
      client.close();
    }
  });

  it('should fail P1 after 3 retries without fallback success', async () => {
    const id = env.DOLAKE.idFromName('test-p1-fail-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Inject permanent failure for both R2 and KV
    await stub.fetch('http://dolake/v1/test/inject-write-failure', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target: 'ALL', failCount: 100, permanent: true }),
    });

    const { client } = await connectWebSocket(stub);

    try {
      const p1Event = createP1Event();
      const message = createCDCBatchMessage({
        events: [p1Event],
      });

      const response = await sendAndReceive(client, message, 20000) as {
        type: string;
        reason?: string;
        details?: {
          retryCount?: number;
          maxRetries?: number;
        };
      };

      // EXPECTED: Should fail after max retries
      expect(response.type).toBe('nack');
      expect(response.reason).toBe('write_failed');
      expect(response.details?.retryCount).toBe(3);
      expect(response.details?.maxRetries).toBe(3);
    } finally {
      client.close();
    }
  });

  it('should track P1 fallback events for later R2 sync', async () => {
    const id = env.DOLAKE.idFromName('test-p1-track-fallback-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Inject R2 failure
    await stub.fetch('http://dolake/v1/test/inject-write-failure', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target: 'R2', failCount: 5 }),
    });

    const { client } = await connectWebSocket(stub);

    try {
      const p1Event = createP1Event();
      const message = createCDCBatchMessage({
        events: [p1Event],
      });

      await sendAndReceive(client, message, 10000);

      // Check fallback tracking
      const statusResponse = await stub.fetch('http://dolake/v1/durability/fallback-status');
      const status = await statusResponse.json() as {
        pendingFallbackSync: number;
        fallbackEvents: Array<{ tier: string }>;
      };

      // EXPECTED: Event should be tracked for later sync
      expect(status.pendingFallbackSync).toBeGreaterThan(0);
      expect(status.fallbackEvents.some(e => e.tier === 'P1')).toBe(true);
    } finally {
      client.close();
    }
  });
});

// =============================================================================
// 4. P2 Fallback Behavior Tests (R2 -> VFS)
// =============================================================================

describe('P2 Standard - Fallback (R2 -> VFS)', () => {
  /**
   * RED Phase: P2 VFS fallback is not implemented.
   * Expected behavior: P2 events should write to R2, falling back to DO
   * fsx VFS on R2 failure, with retry from VFS on next flush.
   */

  it('should write P2 event to R2', async () => {
    const id = env.DOLAKE.idFromName('test-p2-r2-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    try {
      const p2Event = createP2Event();
      const message = createCDCBatchMessage({
        events: [p2Event],
      });

      const response = await sendAndReceive(client, message) as {
        type: string;
        details?: {
          writtenToR2?: boolean;
          durabilityTier?: string;
        };
      };

      // EXPECTED: P2 should write to R2
      expect(response.type).toBe('ack');
      expect(response.details?.durabilityTier).toBe('P2');
      expect(response.details?.writtenToR2).toBe(true);
    } finally {
      client.close();
    }
  });

  it('should fallback to VFS when R2 fails for P2', async () => {
    const id = env.DOLAKE.idFromName('test-p2-vfs-fallback-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Inject R2 failure
    await stub.fetch('http://dolake/v1/test/inject-write-failure', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target: 'R2', failCount: 5 }),
    });

    const { client } = await connectWebSocket(stub);

    try {
      const p2Event = createP2Event();
      const message = createCDCBatchMessage({
        events: [p2Event],
      });

      const response = await sendAndReceive(client, message, 10000) as {
        type: string;
        status?: string;
        details?: {
          writtenToR2?: boolean;
          writtenToVFS?: boolean;
          usedFallback?: boolean;
        };
      };

      // EXPECTED: Should fallback to VFS
      expect(response.type).toBe('ack');
      expect(response.status).toBe('fallback');
      expect(response.details?.usedFallback).toBe(true);
      expect(response.details?.writtenToVFS).toBe(true);
    } finally {
      client.close();
    }
  });

  it('should retry VFS events on next flush', async () => {
    const id = env.DOLAKE.idFromName('test-p2-vfs-retry-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Inject R2 failure for first attempt only
    await stub.fetch('http://dolake/v1/test/inject-write-failure', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target: 'R2', failCount: 1 }),
    });

    const { client } = await connectWebSocket(stub);

    try {
      const p2Event = createP2Event();
      const message = createCDCBatchMessage({
        events: [p2Event],
      });

      // First write goes to VFS
      await sendAndReceive(client, message, 10000);

      // Clear R2 failure
      await stub.fetch('http://dolake/v1/test/clear-failures', { method: 'POST' });

      // Trigger flush (should retry VFS events to R2)
      const flushResponse = await stub.fetch('http://dolake/flush', { method: 'POST' });
      const flushResult = await flushResponse.json() as {
        success: boolean;
        vfsEventsSynced?: number;
      };

      // EXPECTED: VFS events should be synced to R2
      expect(flushResult.success).toBe(true);
      expect(flushResult.vfsEventsSynced).toBeGreaterThan(0);
    } finally {
      client.close();
    }
  });

  it('should preserve P2 events in VFS across hibernation', async () => {
    const id = env.DOLAKE.idFromName('test-p2-vfs-persist-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Inject R2 failure
    await stub.fetch('http://dolake/v1/test/inject-write-failure', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target: 'R2', failCount: 100 }),
    });

    const { client } = await connectWebSocket(stub);

    try {
      const p2Event = createP2Event();
      const message = createCDCBatchMessage({
        events: [p2Event],
      });

      await sendAndReceive(client, message, 10000);

      // Check VFS status
      const vfsStatusBefore = await stub.fetch('http://dolake/v1/durability/vfs-status');
      const statusBefore = await vfsStatusBefore.json() as { pendingEvents: number };

      expect(statusBefore.pendingEvents).toBeGreaterThan(0);

      // Simulate hibernation (close connection and wait)
      client.close();

      // Reconnect after "hibernation"
      const { client: newClient } = await connectWebSocket(stub);

      // Check VFS status after "wake"
      const vfsStatusAfter = await stub.fetch('http://dolake/v1/durability/vfs-status');
      const statusAfter = await vfsStatusAfter.json() as { pendingEvents: number };

      // EXPECTED: Events should persist in VFS
      expect(statusAfter.pendingEvents).toBeGreaterThanOrEqual(statusBefore.pendingEvents);

      newClient.close();
    } catch {
      client.close();
    }
  });

  it('should batch P2 VFS events for efficient R2 retry', async () => {
    const id = env.DOLAKE.idFromName('test-p2-vfs-batch-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Inject R2 failure
    await stub.fetch('http://dolake/v1/test/inject-write-failure', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target: 'R2', failCount: 10 }),
    });

    const { client } = await connectWebSocket(stub);

    try {
      // Send multiple P2 events
      for (let i = 0; i < 5; i++) {
        const p2Event = createP2Event({ sequence: i });
        const message = createCDCBatchMessage({
          events: [p2Event],
          sequenceNumber: i,
        });
        await sendAndReceive(client, message, 5000);
      }

      // Clear failures and flush
      await stub.fetch('http://dolake/v1/test/clear-failures', { method: 'POST' });
      const flushResponse = await stub.fetch('http://dolake/flush', { method: 'POST' });
      const flushResult = await flushResponse.json() as {
        success: boolean;
        vfsEventsSynced?: number;
        batchCount?: number;
      };

      // EXPECTED: Events should be batched for efficiency
      expect(flushResult.vfsEventsSynced).toBe(5);
      expect(flushResult.batchCount).toBeLessThan(5); // Batched together
    } finally {
      client.close();
    }
  });
});

// =============================================================================
// 5. P3 Drop-on-Failure Behavior Tests
// =============================================================================

describe('P3 Best-Effort - Drop on Failure', () => {
  /**
   * RED Phase: P3 drop-on-failure is not implemented.
   * Expected behavior: P3 events should attempt R2 write but be dropped
   * on failure with no retry or fallback (acceptable loss).
   */

  it('should write P3 event to R2 on success', async () => {
    const id = env.DOLAKE.idFromName('test-p3-r2-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    try {
      const p3Event = createP3Event();
      const message = createCDCBatchMessage({
        events: [p3Event],
      });

      const response = await sendAndReceive(client, message) as {
        type: string;
        details?: {
          writtenToR2?: boolean;
          durabilityTier?: string;
        };
      };

      // EXPECTED: P3 should write to R2 on success
      expect(response.type).toBe('ack');
      expect(response.details?.durabilityTier).toBe('P3');
      expect(response.details?.writtenToR2).toBe(true);
    } finally {
      client.close();
    }
  });

  it('should drop P3 event on R2 failure without retry', async () => {
    const id = env.DOLAKE.idFromName('test-p3-drop-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Inject R2 failure
    await stub.fetch('http://dolake/v1/test/inject-write-failure', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target: 'R2', failCount: 5 }),
    });

    const { client } = await connectWebSocket(stub);

    try {
      const p3Event = createP3Event();
      const message = createCDCBatchMessage({
        events: [p3Event],
      });

      const response = await sendAndReceive(client, message) as {
        type: string;
        status?: string;
        details?: {
          dropped?: boolean;
          retryCount?: number;
        };
      };

      // EXPECTED: P3 should be dropped without retry
      expect(response.type).toBe('ack');
      expect(response.status).toBe('dropped');
      expect(response.details?.dropped).toBe(true);
      expect(response.details?.retryCount).toBe(0);
    } finally {
      client.close();
    }
  });

  it('should not use fallback storage for P3 events', async () => {
    const id = env.DOLAKE.idFromName('test-p3-no-fallback-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Inject R2 failure
    await stub.fetch('http://dolake/v1/test/inject-write-failure', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target: 'R2', failCount: 5 }),
    });

    const { client } = await connectWebSocket(stub);

    try {
      const p3Event = createP3Event();
      const message = createCDCBatchMessage({
        events: [p3Event],
      });

      const response = await sendAndReceive(client, message) as {
        type: string;
        details?: {
          writtenToKV?: boolean;
          writtenToVFS?: boolean;
          usedFallback?: boolean;
        };
      };

      // EXPECTED: P3 should NOT use any fallback
      expect(response.details?.usedFallback).toBe(false);
      expect(response.details?.writtenToKV).toBe(false);
      expect(response.details?.writtenToVFS).toBe(false);
    } finally {
      client.close();
    }
  });

  it('should track P3 drop rate in metrics', async () => {
    const id = env.DOLAKE.idFromName('test-p3-metrics-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Inject R2 failure
    await stub.fetch('http://dolake/v1/test/inject-write-failure', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target: 'R2', failCount: 100 }),
    });

    const { client } = await connectWebSocket(stub);

    try {
      // Send multiple P3 events
      for (let i = 0; i < 10; i++) {
        const p3Event = createP3Event({ sequence: i });
        const message = createCDCBatchMessage({
          events: [p3Event],
          sequenceNumber: i,
        });
        await sendAndReceive(client, message);
      }

      // Check metrics
      const metricsResponse = await stub.fetch('http://dolake/metrics');
      const metricsText = await metricsResponse.text();

      // EXPECTED: Metrics should include P3 drop count
      expect(metricsText).toContain('dolake_p3_events_dropped');
    } finally {
      client.close();
    }
  });

  it('should ack P3 event immediately regardless of write outcome', async () => {
    const id = env.DOLAKE.idFromName('test-p3-fast-ack-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    try {
      const p3Event = createP3Event();
      const message = createCDCBatchMessage({
        events: [p3Event],
      });

      const startTime = Date.now();
      const response = await sendAndReceive(client, message) as {
        type: string;
      };
      const duration = Date.now() - startTime;

      // EXPECTED: P3 should ack quickly (no waiting for retries)
      expect(response.type).toBe('ack');
      expect(duration).toBeLessThan(1000); // Should be very fast
    } finally {
      client.close();
    }
  });

  it('should process P3 events in background', async () => {
    const id = env.DOLAKE.idFromName('test-p3-background-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    try {
      // Send multiple P3 events rapidly
      const responses: Array<{ type: string }> = [];
      for (let i = 0; i < 20; i++) {
        const p3Event = createP3Event({ sequence: i });
        const message = createCDCBatchMessage({
          events: [p3Event],
          sequenceNumber: i,
        });
        const response = await sendAndReceive(client, message) as { type: string };
        responses.push(response);
      }

      // EXPECTED: All should be acked (write happens async)
      expect(responses.every(r => r.type === 'ack')).toBe(true);

      // Check status to verify background processing
      const statusResponse = await stub.fetch('http://dolake/v1/durability/status');
      const status = await statusResponse.json() as {
        backgroundWritesPending?: number;
      };

      // Background writes may still be in progress
      expect(status.backgroundWritesPending).toBeDefined();
    } finally {
      client.close();
    }
  });
});

// =============================================================================
// 6. Mixed Tier Batch Tests
// =============================================================================

describe('Mixed Tier Batches', () => {
  /**
   * RED Phase: Mixed tier handling is not implemented.
   * Expected behavior: When a batch contains events of different tiers,
   * each event should be processed according to its tier's durability rules.
   */

  it('should process mixed tier batch with appropriate durability per event', async () => {
    const id = env.DOLAKE.idFromName('test-mixed-batch-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    try {
      const events = [
        createP0Event({ sequence: 1 }),
        createP1Event({ sequence: 2 }),
        createP2Event({ sequence: 3 }),
        createP3Event({ sequence: 4 }),
      ];

      const message = createCDCBatchMessage({
        events,
        sequenceNumber: 1,
      });

      const response = await sendAndReceive(client, message, 10000) as {
        type: string;
        details?: {
          eventResults?: Array<{
            sequence: number;
            tier: string;
            success: boolean;
            writtenTo?: string[];
          }>;
        };
      };

      // EXPECTED: Each event should be processed according to its tier
      expect(response.type).toBe('ack');
      const results = response.details?.eventResults ?? [];
      expect(results).toHaveLength(4);

      // P0 should write to R2+KV
      const p0Result = results.find(r => r.tier === 'P0');
      expect(p0Result?.writtenTo).toContain('R2');
      expect(p0Result?.writtenTo).toContain('KV');

      // P1 should write to R2
      const p1Result = results.find(r => r.tier === 'P1');
      expect(p1Result?.writtenTo).toContain('R2');

      // P2 should write to R2
      const p2Result = results.find(r => r.tier === 'P2');
      expect(p2Result?.writtenTo).toContain('R2');

      // P3 should write to R2
      const p3Result = results.find(r => r.tier === 'P3');
      expect(p3Result?.writtenTo).toContain('R2');
    } finally {
      client.close();
    }
  });

  it('should not block lower tier events when higher tier fails', async () => {
    const id = env.DOLAKE.idFromName('test-mixed-isolation-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Inject KV failure (affects P0)
    await stub.fetch('http://dolake/v1/test/inject-write-failure', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target: 'KV', failCount: 5 }),
    });

    const { client } = await connectWebSocket(stub);

    try {
      const events = [
        createP0Event({ sequence: 1 }),
        createP3Event({ sequence: 2 }),
      ];

      const message = createCDCBatchMessage({
        events,
        sequenceNumber: 1,
      });

      const response = await sendAndReceive(client, message, 10000) as {
        type: string;
        details?: {
          eventResults?: Array<{
            sequence: number;
            tier: string;
            success: boolean;
          }>;
        };
      };

      // EXPECTED: P3 should succeed even if P0 is retrying
      const results = response.details?.eventResults ?? [];
      const p3Result = results.find(r => r.tier === 'P3');
      expect(p3Result?.success).toBe(true);
    } finally {
      client.close();
    }
  });

  it('should report highest tier failure as batch status', async () => {
    const id = env.DOLAKE.idFromName('test-mixed-status-' + Date.now());
    const stub = env.DOLAKE.get(id);

    // Inject permanent R2 failure
    await stub.fetch('http://dolake/v1/test/inject-write-failure', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target: 'R2', failCount: 100, permanent: true }),
    });

    // Inject permanent KV failure too (for P0)
    await stub.fetch('http://dolake/v1/test/inject-write-failure', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ target: 'KV', failCount: 100, permanent: true }),
    });

    const { client } = await connectWebSocket(stub);

    try {
      const events = [
        createP0Event({ sequence: 1 }),
        createP3Event({ sequence: 2 }), // P3 would be dropped, not failed
      ];

      const message = createCDCBatchMessage({
        events,
        sequenceNumber: 1,
      });

      const response = await sendAndReceive(client, message, 20000) as {
        type: string;
        details?: {
          highestFailedTier?: string;
        };
      };

      // EXPECTED: Batch status should reflect P0 failure (highest tier)
      // P3 being dropped is "success" for P3's contract
      expect(response.details?.highestFailedTier).toBe('P0');
    } finally {
      client.close();
    }
  });
});

// =============================================================================
// 7. Durability Status and Metrics Tests
// =============================================================================

describe('Durability Status and Metrics', () => {
  /**
   * RED Phase: Durability metrics are not implemented.
   * Expected behavior: DoLake should expose durability metrics for monitoring.
   */

  it('should expose durability tier statistics', async () => {
    const id = env.DOLAKE.idFromName('test-durability-stats-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/v1/durability/stats');
    expect(response.status).toBe(200);

    const stats = await response.json() as {
      byTier: {
        P0: { total: number; succeeded: number; failed: number; dlq: number };
        P1: { total: number; succeeded: number; failed: number; fallback: number };
        P2: { total: number; succeeded: number; failed: number; vfs: number };
        P3: { total: number; succeeded: number; dropped: number };
      };
    };

    // EXPECTED: Should have stats for all tiers
    expect(stats.byTier).toBeDefined();
    expect(stats.byTier.P0).toBeDefined();
    expect(stats.byTier.P1).toBeDefined();
    expect(stats.byTier.P2).toBeDefined();
    expect(stats.byTier.P3).toBeDefined();
  });

  it('should include durability metrics in prometheus format', async () => {
    const id = env.DOLAKE.idFromName('test-durability-prometheus-' + Date.now());
    const stub = env.DOLAKE.get(id);

    const response = await stub.fetch('http://dolake/metrics');
    const metricsText = await response.text();

    // EXPECTED: Should include durability-specific metrics
    expect(metricsText).toContain('dolake_durability_writes_total');
    expect(metricsText).toContain('dolake_durability_failures_total');
    expect(metricsText).toContain('dolake_durability_fallback_total');
    expect(metricsText).toContain('dolake_durability_dlq_total');
  });

  it('should track write latency by tier', async () => {
    const id = env.DOLAKE.idFromName('test-durability-latency-' + Date.now());
    const stub = env.DOLAKE.get(id);
    const { client } = await connectWebSocket(stub);

    try {
      // Send events of each tier
      for (const createEvent of [createP0Event, createP1Event, createP2Event, createP3Event]) {
        const event = createEvent();
        const message = createCDCBatchMessage({
          events: [event],
          sequenceNumber: event.sequence,
        });
        await sendAndReceive(client, message);
      }

      const response = await stub.fetch('http://dolake/v1/durability/latency');
      const latency = await response.json() as {
        byTier: {
          P0: { p50: number; p95: number; p99: number };
          P1: { p50: number; p95: number; p99: number };
          P2: { p50: number; p95: number; p99: number };
          P3: { p50: number; p95: number; p99: number };
        };
      };

      // EXPECTED: Should track latency percentiles by tier
      expect(latency.byTier.P0).toBeDefined();
      expect(latency.byTier.P1).toBeDefined();
      expect(latency.byTier.P2).toBeDefined();
      expect(latency.byTier.P3).toBeDefined();

      // P0 may have higher latency due to dual-write
      // P3 should have lowest latency
      expect(latency.byTier.P3.p50).toBeLessThanOrEqual(latency.byTier.P0.p50);
    } finally {
      client.close();
    }
  });
});
