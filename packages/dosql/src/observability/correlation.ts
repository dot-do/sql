/**
 * Cross-Package Correlation ID Propagation
 *
 * Provides utilities for propagating correlation IDs from DoSQL requests
 * through CDC events to DoLake, enabling end-to-end request tracing
 * across package boundaries.
 *
 * The correlation flow is:
 * 1. A request arrives at DoSQL with a correlation ID (or one is generated)
 * 2. As the request causes WAL writes and CDC events, the correlation ID
 *    is attached to ChangeEvent and CDCBatch objects
 * 3. When CDC batches are streamed to DoLake, the correlation ID travels
 *    with the batch data
 * 4. DoLake extracts the correlation ID and attaches it to its spans and logs
 *
 * @packageDocumentation
 */

import { DistributedTraceStorage } from './distributed-tracing.js';

/**
 * Get the current correlation ID from the active distributed trace context.
 *
 * This is a convenience function that extracts the correlation ID from
 * AsyncLocalStorage without requiring a tracer instance.
 *
 * @returns The current correlation ID, or undefined if no context is active
 */
export function getCurrentCorrelationId(): string | undefined {
  return DistributedTraceStorage.getStore()?.correlationId;
}

/**
 * Generate a new correlation ID.
 *
 * Uses crypto.randomUUID() when available for standards-compliant UUID v4,
 * with a fallback to hex-based generation for environments without it.
 *
 * @returns A new unique correlation ID string
 */
export function generateCorrelationId(): string {
  if (typeof crypto !== 'undefined' && crypto.randomUUID) {
    return crypto.randomUUID();
  }
  // Fallback: generate 32 hex characters
  const array = new Uint8Array(16);
  crypto.getRandomValues(array);
  return Array.from(array)
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
}

/**
 * Enriches a CDC change event with the current correlation ID.
 *
 * If a correlation ID is active in the current distributed trace context,
 * it will be attached to the event. If no context is active, the event
 * is returned unchanged.
 *
 * @param event - The CDC event to enrich
 * @returns The event with correlationId set (if available)
 *
 * @example
 * ```typescript
 * const changeEvent = walEntryToChangeEvent(entry);
 * const enrichedEvent = enrichCDCEventWithCorrelation(changeEvent);
 * // enrichedEvent.correlationId is now set if a trace context is active
 * ```
 */
export function enrichCDCEventWithCorrelation<T extends { correlationId?: string | undefined }>(
  event: T
): T {
  const correlationId = getCurrentCorrelationId();
  if (correlationId) {
    return { ...event, correlationId };
  }
  return event;
}

/**
 * Enriches a CDC batch with the current correlation ID.
 *
 * Attaches the active correlation ID to the batch object so it can be
 * propagated to DoLake during CDC streaming.
 *
 * @param batch - The CDC batch to enrich
 * @returns The batch with correlationId set (if available)
 *
 * @example
 * ```typescript
 * const batch = createCDCBatch(events);
 * const enrichedBatch = enrichCDCBatchWithCorrelation(batch);
 * // enrichedBatch.correlationId is now set for cross-package tracking
 * ```
 */
export function enrichCDCBatchWithCorrelation<T extends { correlationId?: string | undefined }>(
  batch: T
): T {
  const correlationId = getCurrentCorrelationId();
  if (correlationId) {
    return { ...batch, correlationId };
  }
  return batch;
}

/**
 * Extract correlation ID from HTTP headers.
 *
 * Checks the standard `x-correlation-id` header used by the distributed
 * tracing module for cross-service correlation.
 *
 * @param headers - HTTP headers to extract from
 * @param headerName - Custom header name (defaults to 'x-correlation-id')
 * @returns The correlation ID if present, undefined otherwise
 */
export function extractCorrelationIdFromHeaders(
  headers: Headers,
  headerName = 'x-correlation-id'
): string | undefined {
  return headers.get(headerName) ?? undefined;
}
