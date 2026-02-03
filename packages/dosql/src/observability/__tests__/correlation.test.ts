/**
 * Cross-Package Correlation ID Propagation Tests
 *
 * Tests for correlation ID generation, propagation through CDC events and batches,
 * and end-to-end flow from DoSQL request context through CDC to DoLake.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  getCurrentCorrelationId,
  generateCorrelationId,
  enrichCDCEventWithCorrelation,
  enrichCDCBatchWithCorrelation,
  extractCorrelationIdFromHeaders,
} from '../correlation.js';
import {
  createDistributedTracer,
  DistributedTraceStorage,
  withDistributedContext,
  type DistributedTracer,
  type DistributedTraceContext,
} from '../distributed-tracing.js';
import {
  createUnifiedObservability,
  type UnifiedObservability,
} from '../unified.js';

// =============================================================================
// Test Helpers
// =============================================================================

function createTestTracer(): DistributedTracer {
  return createDistributedTracer({
    enabled: true,
    serviceName: 'test-service',
    sampler: 'always_on',
    samplingRate: 1.0,
  });
}

function createTestObservability(): UnifiedObservability {
  return createUnifiedObservability({
    serviceName: 'test-service',
    tracing: { enabled: true },
    metrics: { enabled: true },
  });
}

// =============================================================================
// generateCorrelationId Tests
// =============================================================================

describe('generateCorrelationId', () => {
  it('generates a non-empty string', () => {
    const id = generateCorrelationId();
    expect(id).toBeDefined();
    expect(id.length).toBeGreaterThan(0);
  });

  it('generates unique IDs', () => {
    const ids = new Set(Array.from({ length: 100 }, () => generateCorrelationId()));
    expect(ids.size).toBe(100);
  });

  it('generates valid UUID format', () => {
    const id = generateCorrelationId();
    // UUID v4 format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    // or hex string: 32 hex characters
    expect(id.length).toBeGreaterThanOrEqual(32);
  });
});

// =============================================================================
// getCurrentCorrelationId Tests
// =============================================================================

describe('getCurrentCorrelationId', () => {
  it('returns undefined when no context is active', () => {
    expect(getCurrentCorrelationId()).toBeUndefined();
  });

  it('returns correlation ID within distributed trace context', async () => {
    const tracer = createTestTracer();
    const context = tracer.createRootContext();

    let captured: string | undefined;
    await withDistributedContext(context, () => {
      captured = getCurrentCorrelationId();
    });

    expect(captured).toBe(context.correlationId);
  });

  it('returns correct ID in nested async operations', async () => {
    const tracer = createTestTracer();
    const context = tracer.createRootContext();

    const results: (string | undefined)[] = [];

    await withDistributedContext(context, async () => {
      results.push(getCurrentCorrelationId());
      await new Promise((resolve) => setTimeout(resolve, 1));
      results.push(getCurrentCorrelationId());
      await Promise.resolve();
      results.push(getCurrentCorrelationId());
    });

    expect(results).toHaveLength(3);
    expect(results.every((id) => id === context.correlationId)).toBe(true);
  });

  it('isolates between parallel contexts', async () => {
    const tracer = createTestTracer();
    const context1 = tracer.createRootContext();
    const context2 = tracer.createRootContext();

    const results: { label: string; id: string | undefined }[] = [];

    await Promise.all([
      withDistributedContext(context1, async () => {
        await new Promise((resolve) => setTimeout(resolve, 5));
        results.push({ label: 'ctx1', id: getCurrentCorrelationId() });
      }),
      withDistributedContext(context2, async () => {
        results.push({ label: 'ctx2', id: getCurrentCorrelationId() });
      }),
    ]);

    const ctx1Result = results.find((r) => r.label === 'ctx1');
    const ctx2Result = results.find((r) => r.label === 'ctx2');

    expect(ctx1Result?.id).toBe(context1.correlationId);
    expect(ctx2Result?.id).toBe(context2.correlationId);
    expect(ctx1Result?.id).not.toBe(ctx2Result?.id);
  });
});

// =============================================================================
// enrichCDCEventWithCorrelation Tests
// =============================================================================

describe('enrichCDCEventWithCorrelation', () => {
  it('adds correlation ID to event when context is active', async () => {
    const tracer = createTestTracer();
    const context = tracer.createRootContext();

    let enrichedEvent: { type: string; correlationId?: string } | undefined;

    await withDistributedContext(context, () => {
      const event = { type: 'insert', table: 'users', correlationId: undefined as string | undefined };
      enrichedEvent = enrichCDCEventWithCorrelation(event);
    });

    expect(enrichedEvent?.correlationId).toBe(context.correlationId);
  });

  it('does not modify event when no context is active', () => {
    const event = { type: 'insert', table: 'users', correlationId: undefined as string | undefined };
    const result = enrichCDCEventWithCorrelation(event);

    expect(result.correlationId).toBeUndefined();
  });

  it('preserves existing event properties', async () => {
    const tracer = createTestTracer();
    const context = tracer.createRootContext();

    let enrichedEvent: Record<string, unknown> | undefined;

    await withDistributedContext(context, () => {
      const event = {
        type: 'update',
        table: 'orders',
        txnId: 'txn-123',
        lsn: 42n,
        correlationId: undefined as string | undefined,
      };
      enrichedEvent = enrichCDCEventWithCorrelation(event);
    });

    expect(enrichedEvent?.type).toBe('update');
    expect(enrichedEvent?.table).toBe('orders');
    expect(enrichedEvent?.txnId).toBe('txn-123');
    expect(enrichedEvent?.lsn).toBe(42n);
    expect(enrichedEvent?.correlationId).toBe(context.correlationId);
  });

  it('does not mutate the original event', async () => {
    const tracer = createTestTracer();
    const context = tracer.createRootContext();

    const originalEvent = { type: 'delete', table: 'sessions', correlationId: undefined as string | undefined };

    await withDistributedContext(context, () => {
      const enriched = enrichCDCEventWithCorrelation(originalEvent);
      // Original should not be modified
      expect(originalEvent.correlationId).toBeUndefined();
      // Enriched should have the correlationId
      expect(enriched.correlationId).toBe(context.correlationId);
    });
  });
});

// =============================================================================
// enrichCDCBatchWithCorrelation Tests
// =============================================================================

describe('enrichCDCBatchWithCorrelation', () => {
  it('adds correlation ID to batch when context is active', async () => {
    const tracer = createTestTracer();
    const context = tracer.createRootContext();

    let enrichedBatch: { batchId: string; correlationId?: string } | undefined;

    await withDistributedContext(context, () => {
      const batch = { batchId: 'batch-001', sourceDoId: 'do-1', correlationId: undefined as string | undefined };
      enrichedBatch = enrichCDCBatchWithCorrelation(batch);
    });

    expect(enrichedBatch?.correlationId).toBe(context.correlationId);
  });

  it('does not modify batch when no context is active', () => {
    const batch = { batchId: 'batch-002', sourceDoId: 'do-1', correlationId: undefined as string | undefined };
    const result = enrichCDCBatchWithCorrelation(batch);

    expect(result.correlationId).toBeUndefined();
  });

  it('preserves all batch properties', async () => {
    const tracer = createTestTracer();
    const context = tracer.createRootContext();

    let enrichedBatch: Record<string, unknown> | undefined;

    await withDistributedContext(context, () => {
      const batch = {
        batchId: 'batch-003',
        sourceDoId: 'do-2',
        sequenceNumber: 5,
        events: [{ type: 'insert' }],
        correlationId: undefined as string | undefined,
      };
      enrichedBatch = enrichCDCBatchWithCorrelation(batch);
    });

    expect(enrichedBatch?.batchId).toBe('batch-003');
    expect(enrichedBatch?.sourceDoId).toBe('do-2');
    expect(enrichedBatch?.sequenceNumber).toBe(5);
    expect(enrichedBatch?.events).toEqual([{ type: 'insert' }]);
    expect(enrichedBatch?.correlationId).toBe(context.correlationId);
  });
});

// =============================================================================
// extractCorrelationIdFromHeaders Tests
// =============================================================================

describe('extractCorrelationIdFromHeaders', () => {
  it('extracts correlation ID from x-correlation-id header', () => {
    const headers = new Headers();
    headers.set('x-correlation-id', 'my-corr-id-123');

    const id = extractCorrelationIdFromHeaders(headers);
    expect(id).toBe('my-corr-id-123');
  });

  it('returns undefined when header is not present', () => {
    const headers = new Headers();

    const id = extractCorrelationIdFromHeaders(headers);
    expect(id).toBeUndefined();
  });

  it('supports custom header name', () => {
    const headers = new Headers();
    headers.set('x-custom-correlation', 'custom-123');

    const id = extractCorrelationIdFromHeaders(headers, 'x-custom-correlation');
    expect(id).toBe('custom-123');
  });

  it('is case-insensitive for header names', () => {
    const headers = new Headers();
    headers.set('X-Correlation-ID', 'case-test-456');

    const id = extractCorrelationIdFromHeaders(headers);
    expect(id).toBe('case-test-456');
  });
});

// =============================================================================
// End-to-End Correlation Flow Tests
// =============================================================================

describe('End-to-End Correlation Flow', () => {
  it('propagates correlation from request through CDC event', async () => {
    const obs = createTestObservability();

    // Simulate incoming request with a correlation ID
    const parentContext = obs.tracer.createRootContext();
    const headers = new Headers();
    obs.tracer.injectDistributedContext(headers, parentContext);

    const request = new Request('https://example.com/query', { headers });

    let capturedEventCorrelationId: string | undefined;

    await obs.traceRequest(request, 'handle-query', async (span) => {
      // Inside the traced request, create a CDC event
      const cdcEvent = {
        type: 'insert',
        table: 'users',
        correlationId: undefined as string | undefined,
      };

      // Enrich with correlation - should pick up the request's correlation ID
      const enriched = enrichCDCEventWithCorrelation(cdcEvent);
      capturedEventCorrelationId = enriched.correlationId;
    });

    // The CDC event should carry the parent request's correlation ID
    expect(capturedEventCorrelationId).toBe(parentContext.correlationId);
  });

  it('propagates correlation from request through CDC batch', async () => {
    const obs = createTestObservability();

    const parentContext = obs.tracer.createRootContext();
    const headers = new Headers();
    obs.tracer.injectDistributedContext(headers, parentContext);

    const request = new Request('https://example.com/query', { headers });

    let capturedBatchCorrelationId: string | undefined;

    await obs.traceRequest(request, 'handle-query', async () => {
      const batch = {
        batchId: 'test-batch-1',
        sourceDoId: 'do-test',
        correlationId: undefined as string | undefined,
      };

      const enriched = enrichCDCBatchWithCorrelation(batch);
      capturedBatchCorrelationId = enriched.correlationId;
    });

    expect(capturedBatchCorrelationId).toBe(parentContext.correlationId);
  });

  it('maintains same correlation ID across query and CDC enrichment', async () => {
    const obs = createTestObservability();

    const parentContext = obs.tracer.createRootContext();
    const headers = new Headers();
    obs.tracer.injectDistributedContext(headers, parentContext);

    const request = new Request('https://example.com/query', { headers });

    const correlationIds: (string | undefined)[] = [];

    await obs.traceRequest(request, 'handle-query', async (span) => {
      // Correlation from span
      correlationIds.push(span.getCorrelationId());

      // Correlation from getCurrentCorrelationId
      correlationIds.push(getCurrentCorrelationId());

      // Correlation from enriched event
      const event = enrichCDCEventWithCorrelation({ correlationId: undefined as string | undefined });
      correlationIds.push(event.correlationId);

      // Correlation from enriched batch
      const batch = enrichCDCBatchWithCorrelation({ correlationId: undefined as string | undefined });
      correlationIds.push(batch.correlationId);

      // Correlation from logger
      correlationIds.push(obs.logger.getCurrentCorrelationId());
    });

    // All should be the same parent correlation ID
    expect(correlationIds).toHaveLength(5);
    expect(correlationIds.every((id) => id === parentContext.correlationId)).toBe(true);
  });

  it('uses withCorrelation to set explicit correlation ID', async () => {
    const obs = createTestObservability();

    const explicitId = 'explicit-correlation-id-abc';
    let capturedId: string | undefined;
    let capturedEventId: string | undefined;

    await obs.withCorrelation(explicitId, async () => {
      capturedId = getCurrentCorrelationId();

      const event = enrichCDCEventWithCorrelation({ correlationId: undefined as string | undefined });
      capturedEventId = event.correlationId;
    });

    expect(capturedId).toBe(explicitId);
    expect(capturedEventId).toBe(explicitId);
  });

  it('generates new correlation ID for fresh requests', async () => {
    const obs = createTestObservability();

    // Request without any trace context headers
    const request = new Request('https://example.com/query');

    let spanCorrelationId: string | undefined;
    let eventCorrelationId: string | undefined;

    await obs.traceRequest(request, 'handle-query', async (span) => {
      spanCorrelationId = span.getCorrelationId();

      const event = enrichCDCEventWithCorrelation({ correlationId: undefined as string | undefined });
      eventCorrelationId = event.correlationId;
    });

    // Should have generated a correlation ID
    expect(spanCorrelationId).toBeDefined();
    expect(spanCorrelationId!.length).toBeGreaterThan(0);

    // CDC event should carry the same generated correlation ID
    expect(eventCorrelationId).toBe(spanCorrelationId);
  });

  it('correlation flows through nested instrumented operations', async () => {
    const obs = createTestObservability();

    const parentContext = obs.tracer.createRootContext();
    const headers = new Headers();
    obs.tracer.injectDistributedContext(headers, parentContext);

    const request = new Request('https://example.com/query', { headers });

    const correlationIds: (string | undefined)[] = [];

    await obs.traceRequest(request, 'handle-request', async () => {
      correlationIds.push(getCurrentCorrelationId());

      // Nested query
      await obs.instrumentQuery('SELECT * FROM users', undefined, async () => {
        correlationIds.push(getCurrentCorrelationId());
        return [];
      });

      // Nested transaction
      await obs.instrumentTransaction('txn-1', async () => {
        correlationIds.push(getCurrentCorrelationId());
      });

      // Final check
      correlationIds.push(getCurrentCorrelationId());
    });

    expect(correlationIds.length).toBe(4);
    expect(correlationIds.every((id) => id === parentContext.correlationId)).toBe(true);
  });
});

// =============================================================================
// Header Propagation Round-Trip Tests
// =============================================================================

describe('Header Propagation Round-Trip', () => {
  it('correlation ID survives inject -> extract -> inject cycle', () => {
    const tracer = createTestTracer();

    // Create root context with correlation
    const original = tracer.createRootContext();

    // Inject into headers
    const headers1 = new Headers();
    tracer.injectDistributedContext(headers1, original);

    // Extract from headers
    const extracted = tracer.extractDistributedContext(headers1);
    expect(extracted).not.toBeNull();
    expect(extracted!.correlationId).toBe(original.correlationId);

    // Re-inject the extracted context
    const headers2 = new Headers();
    tracer.injectDistributedContext(headers2, extracted!);

    // Verify correlation ID survived the round trip
    expect(headers2.get('x-correlation-id')).toBe(original.correlationId);
  });

  it('correlation ID can be extracted from headers', () => {
    const headers = new Headers();
    headers.set('x-correlation-id', 'external-correlation-id');

    const id = extractCorrelationIdFromHeaders(headers);
    expect(id).toBe('external-correlation-id');
  });
});
