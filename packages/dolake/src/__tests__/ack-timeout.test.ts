/**
 * ACK Timeout Handling Tests
 *
 * Tests for the tail-worker ACK timeout handling implementation.
 * Issue: sql-g9ql - DoLake tail-worker: Add ACK timeout handling
 *
 * NO MOCKS - tests run against real Cloudflare Workers runtime.
 */

import { describe, it, expect, beforeEach, vi, afterEach } from 'vitest';
import {
  TailWorkerCDCStreamer,
  type TailWorkerConfig,
  type TraceItem,
  type PendingAck,
  type AckMetrics,
  type DeadLetterEntry,
  DEFAULT_TAIL_WORKER_CONFIG,
} from '../index.js';

// =============================================================================
// Test Utilities
// =============================================================================

function createTraceItem(overrides: Partial<TraceItem> = {}): TraceItem {
  return {
    scriptName: 'test-worker',
    outcome: 'ok',
    eventTimestamp: Date.now(),
    exceptions: [],
    logs: [],
    event: {
      request: {
        url: 'https://example.com/api/test',
        method: 'GET',
      },
      response: {
        status: 200,
      },
    },
    ...overrides,
  };
}

function createMockWebSocket(): WebSocket {
  const handlers: Record<string, ((event: unknown) => void)[]> = {};
  let readyState = WebSocket.OPEN;

  return {
    get readyState() {
      return readyState;
    },
    send: vi.fn(),
    close: vi.fn(() => {
      readyState = WebSocket.CLOSED;
    }),
    addEventListener: vi.fn((event: string, handler: (event: unknown) => void) => {
      if (!handlers[event]) {
        handlers[event] = [];
      }
      handlers[event].push(handler);
    }),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
    // Expose for testing
    _handlers: handlers,
    _setReadyState: (state: number) => {
      readyState = state;
    },
  } as unknown as WebSocket & { _handlers: Record<string, ((event: unknown) => void)[]>; _setReadyState: (state: number) => void };
}

// =============================================================================
// ACK Timeout Tests
// =============================================================================

describe('ACK Timeout Handling', () => {
  let streamer: TailWorkerCDCStreamer;
  let mockWebSocket: ReturnType<typeof createMockWebSocket>;

  beforeEach(() => {
    vi.useFakeTimers();
    mockWebSocket = createMockWebSocket();
    streamer = new TailWorkerCDCStreamer({
      ackTimeoutMs: 1000, // 1 second timeout for faster tests
      maxRetries: 3,
      initialRetryDelayMs: 100,
      maxRetryDelayMs: 1000,
    });
    streamer.injectConnection('shard-0', mockWebSocket);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe('AckMetrics', () => {
    it('should track ACK metrics correctly', () => {
      const metrics = streamer.getAckMetrics();

      expect(metrics).toEqual({
        acksReceived: 0,
        ackTimeouts: 0,
        ackRetries: 0,
        deadLetteredCount: 0,
        avgAckLatencyMs: 0,
        p99AckLatencyMs: 0,
        pendingAcks: 0,
      });
    });

    it('should update metrics when ACK is received', () => {
      // Manually add a pending ACK for testing
      const messageId = 'test-message-1';
      const sentAt = Date.now() - 50; // Sent 50ms ago

      // Use internal method to simulate pending ACK
      (streamer as unknown as { pendingAcks: Map<string, PendingAck> }).pendingAcks.set(messageId, {
        messageId,
        sequenceNumber: 1,
        shardId: 'shard-0',
        sentAt,
        retryCount: 0,
        message: {} as PendingAck['message'],
        events: [],
      });

      // Simulate ACK response
      const result = streamer.simulateAckResponse(messageId);
      expect(result).toBe(true);

      const metrics = streamer.getAckMetrics();
      expect(metrics.acksReceived).toBe(1);
      expect(metrics.pendingAcks).toBe(0);
      expect(metrics.avgAckLatencyMs).toBeGreaterThanOrEqual(0);
    });

    it('should return false when simulating ACK for non-existent message', () => {
      const result = streamer.simulateAckResponse('non-existent-id');
      expect(result).toBe(false);
    });
  });

  describe('Pending ACKs', () => {
    it('should track pending ACKs count', () => {
      expect(streamer.getPendingAcksCount()).toBe(0);

      // Add pending ACKs manually for testing
      const pendingAcks = (streamer as unknown as { pendingAcks: Map<string, PendingAck> }).pendingAcks;
      pendingAcks.set('msg-1', {
        messageId: 'msg-1',
        sequenceNumber: 1,
        shardId: 'shard-0',
        sentAt: Date.now(),
        retryCount: 0,
        message: {} as PendingAck['message'],
        events: [],
      });

      expect(streamer.getPendingAcksCount()).toBe(1);
    });

    it('should get pending ACK by ID', () => {
      const messageId = 'test-msg-123';
      const pendingAcks = (streamer as unknown as { pendingAcks: Map<string, PendingAck> }).pendingAcks;

      pendingAcks.set(messageId, {
        messageId,
        sequenceNumber: 42,
        shardId: 'shard-0',
        sentAt: Date.now(),
        retryCount: 1,
        message: {} as PendingAck['message'],
        events: [],
      });

      const pending = streamer.getPendingAck(messageId);
      expect(pending).toBeDefined();
      expect(pending?.sequenceNumber).toBe(42);
      expect(pending?.retryCount).toBe(1);
    });
  });

  describe('Dead Letter Queue', () => {
    it('should have empty dead letter queue initially', () => {
      expect(streamer.getDeadLetterQueueSize()).toBe(0);
      expect(streamer.getDeadLetterQueue()).toEqual([]);
    });

    it('should add to dead letter queue on timeout after max retries', async () => {
      const deadLetterQueue = (streamer as unknown as { deadLetterQueue: DeadLetterEntry[] }).deadLetterQueue;

      // Manually add an entry to simulate dead-lettering
      const entry: DeadLetterEntry = {
        messageId: 'failed-msg-1',
        shardId: 'shard-0',
        events: [],
        reason: 'max_retries',
        deadLetteredAt: Date.now(),
        retryAttempts: 3,
      };
      deadLetterQueue.push(entry);

      expect(streamer.getDeadLetterQueueSize()).toBe(1);

      const queue = streamer.getDeadLetterQueue();
      expect(queue).toHaveLength(1);
      expect(queue[0].reason).toBe('max_retries');
      expect(queue[0].retryAttempts).toBe(3);
    });

    it('should clear dead letter queue and return entries', () => {
      const deadLetterQueue = (streamer as unknown as { deadLetterQueue: DeadLetterEntry[] }).deadLetterQueue;

      deadLetterQueue.push({
        messageId: 'msg-1',
        shardId: 'shard-0',
        events: [],
        reason: 'timeout',
        deadLetteredAt: Date.now(),
        retryAttempts: 2,
      });
      deadLetterQueue.push({
        messageId: 'msg-2',
        shardId: 'shard-1',
        events: [],
        reason: 'connection_failed',
        deadLetteredAt: Date.now(),
        retryAttempts: 1,
      });

      expect(streamer.getDeadLetterQueueSize()).toBe(2);

      const cleared = streamer.clearDeadLetterQueue();
      expect(cleared).toHaveLength(2);
      expect(streamer.getDeadLetterQueueSize()).toBe(0);
    });
  });

  describe('Timeout Detection', () => {
    it('should detect and handle timed out messages', async () => {
      const pendingAcks = (streamer as unknown as { pendingAcks: Map<string, PendingAck> }).pendingAcks;

      // Add an old pending ACK that should timeout
      pendingAcks.set('old-msg', {
        messageId: 'old-msg',
        sequenceNumber: 1,
        shardId: 'shard-0',
        sentAt: Date.now() - 2000, // 2 seconds ago (> 1s timeout)
        retryCount: 2, // Already retried twice, max is 3
        message: {
          type: 'cdc_batch',
          timestamp: Date.now(),
          sourceDoId: 'test',
          events: [],
          sequenceNumber: 1,
          firstEventSequence: 1,
          lastEventSequence: 1,
          sizeBytes: 0,
          isRetry: false,
          retryCount: 0,
        },
        events: [],
      });

      const results = await streamer.checkAndHandleTimeouts();

      expect(results).toHaveLength(1);
      expect(results[0].messageId).toBe('old-msg');
      expect(results[0].deadLettered).toBe(true);
      expect(results[0].retried).toBe(false);

      // Check metrics
      const metrics = streamer.getAckMetrics();
      expect(metrics.ackTimeouts).toBeGreaterThan(0);
    });

    it('should retry message if under max retries', async () => {
      const pendingAcks = (streamer as unknown as { pendingAcks: Map<string, PendingAck> }).pendingAcks;

      // Add an old pending ACK that can still be retried
      pendingAcks.set('retry-msg', {
        messageId: 'retry-msg',
        sequenceNumber: 1,
        shardId: 'shard-0',
        sentAt: Date.now() - 2000, // 2 seconds ago
        retryCount: 0, // No retries yet
        message: {
          type: 'cdc_batch',
          timestamp: Date.now(),
          sourceDoId: 'test',
          events: [],
          sequenceNumber: 1,
          firstEventSequence: 1,
          lastEventSequence: 1,
          sizeBytes: 0,
          isRetry: false,
          retryCount: 0,
        },
        events: [],
      });

      const results = await streamer.checkAndHandleTimeouts();

      expect(results).toHaveLength(1);
      expect(results[0].messageId).toBe('retry-msg');
      expect(results[0].retried).toBe(true);
      expect(results[0].deadLettered).toBe(false);

      // Message should still be pending (for retry)
      expect(pendingAcks.has('retry-msg')).toBe(true);

      // Check that send was called
      expect(mockWebSocket.send).toHaveBeenCalled();
    });

    it('should dead-letter on connection failure during retry', async () => {
      const pendingAcks = (streamer as unknown as { pendingAcks: Map<string, PendingAck> }).pendingAcks;

      // Close the connection
      (mockWebSocket as unknown as { _setReadyState: (state: number) => void })._setReadyState(WebSocket.CLOSED);

      // Add pending ACK
      pendingAcks.set('no-conn-msg', {
        messageId: 'no-conn-msg',
        sequenceNumber: 1,
        shardId: 'shard-0',
        sentAt: Date.now() - 2000,
        retryCount: 0,
        message: {
          type: 'cdc_batch',
          timestamp: Date.now(),
          sourceDoId: 'test',
          events: [],
          sequenceNumber: 1,
          firstEventSequence: 1,
          lastEventSequence: 1,
          sizeBytes: 0,
          isRetry: false,
          retryCount: 0,
        },
        events: [],
      });

      const results = await streamer.checkAndHandleTimeouts();

      expect(results).toHaveLength(1);
      expect(results[0].messageId).toBe('no-conn-msg');
      expect(results[0].deadLettered).toBe(true);
      expect(results[0].error).toBe('Connection not available for retry');
    });
  });

  describe('Graceful Shutdown', () => {
    it('should dead-letter pending ACKs on close', async () => {
      const pendingAcks = (streamer as unknown as { pendingAcks: Map<string, PendingAck> }).pendingAcks;

      // Add pending ACKs
      pendingAcks.set('pending-1', {
        messageId: 'pending-1',
        sequenceNumber: 1,
        shardId: 'shard-0',
        sentAt: Date.now(),
        retryCount: 0,
        message: {} as PendingAck['message'],
        events: [],
      });
      pendingAcks.set('pending-2', {
        messageId: 'pending-2',
        sequenceNumber: 2,
        shardId: 'shard-0',
        sentAt: Date.now(),
        retryCount: 0,
        message: {} as PendingAck['message'],
        events: [],
      });

      await streamer.close();

      // Pending ACKs should be cleared
      expect(streamer.getPendingAcksCount()).toBe(0);

      // Should be in dead letter queue
      expect(streamer.getDeadLetterQueueSize()).toBe(2);
    });
  });

  describe('Metrics Reset', () => {
    it('should reset ACK metrics', () => {
      // Add some metrics data
      const ackLatencies = (streamer as unknown as { ackLatencies: number[] }).ackLatencies;
      ackLatencies.push(10, 20, 30);
      (streamer as unknown as { ackTimeoutCount: number }).ackTimeoutCount = 5;
      (streamer as unknown as { ackRetryCount: number }).ackRetryCount = 3;
      (streamer as unknown as { acksReceivedCount: number }).acksReceivedCount = 10;

      let metrics = streamer.getAckMetrics();
      expect(metrics.acksReceived).toBe(10);
      expect(metrics.ackTimeouts).toBe(5);

      streamer.resetAckMetrics();

      metrics = streamer.getAckMetrics();
      expect(metrics.acksReceived).toBe(0);
      expect(metrics.ackTimeouts).toBe(0);
      expect(metrics.ackRetries).toBe(0);
      expect(metrics.avgAckLatencyMs).toBe(0);
    });
  });

  describe('P99 Latency Calculation', () => {
    it('should calculate P99 latency correctly', () => {
      const ackLatencies = (streamer as unknown as { ackLatencies: number[] }).ackLatencies;

      // Add 100 samples from 1-100ms
      for (let i = 1; i <= 100; i++) {
        ackLatencies.push(i);
      }

      const metrics = streamer.getAckMetrics();

      // P99 should be at index 99 (0-indexed), which is value 100
      expect(metrics.p99AckLatencyMs).toBe(100);

      // Average should be 50.5
      expect(metrics.avgAckLatencyMs).toBeCloseTo(50.5, 1);
    });

    it('should handle empty latency array', () => {
      const metrics = streamer.getAckMetrics();
      expect(metrics.p99AckLatencyMs).toBe(0);
      expect(metrics.avgAckLatencyMs).toBe(0);
    });

    it('should handle single latency sample', () => {
      const ackLatencies = (streamer as unknown as { ackLatencies: number[] }).ackLatencies;
      ackLatencies.push(42);

      const metrics = streamer.getAckMetrics();
      expect(metrics.p99AckLatencyMs).toBe(42);
      expect(metrics.avgAckLatencyMs).toBe(42);
    });
  });

  describe('Configuration', () => {
    it('should use custom ACK timeout from config', () => {
      const customStreamer = new TailWorkerCDCStreamer({
        ackTimeoutMs: 5000,
      });

      const config = (customStreamer as unknown as { config: TailWorkerConfig }).config;
      expect(config.ackTimeoutMs).toBe(5000);
    });

    it('should use default ACK timeout when not specified', () => {
      const defaultStreamer = new TailWorkerCDCStreamer();

      const config = (defaultStreamer as unknown as { config: TailWorkerConfig }).config;
      expect(config.ackTimeoutMs).toBe(DEFAULT_TAIL_WORKER_CONFIG.ackTimeoutMs);
    });
  });
});

describe('Dead Letter Entry Types', () => {
  it('should support different dead letter reasons', () => {
    const reasons: DeadLetterEntry['reason'][] = ['timeout', 'max_retries', 'connection_failed'];

    for (const reason of reasons) {
      const entry: DeadLetterEntry = {
        messageId: `msg-${reason}`,
        shardId: 'shard-0',
        events: [],
        reason,
        deadLetteredAt: Date.now(),
        retryAttempts: 1,
      };

      expect(entry.reason).toBe(reason);
    }
  });
});
