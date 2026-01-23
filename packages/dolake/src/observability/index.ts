/**
 * DoLake Observability Module
 *
 * Provides OpenTelemetry tracing and Prometheus metrics for production monitoring
 * of CDC processing, Parquet writing, and Iceberg commit operations.
 *
 * @example Basic Usage
 * ```typescript
 * import { createObservability, createDoLakeMetrics } from 'dolake/observability';
 *
 * const { tracer, metrics } = createObservability();
 * const dolakeMetrics = createDoLakeMetrics(metrics);
 *
 * // Instrument CDC batch processing
 * const span = tracer.startSpan('dolake.processCDCBatch', {
 *   kind: 'CONSUMER',
 *   attributes: {
 *     'cdc.batch_id': batch.batchId,
 *     'cdc.table': batch.table,
 *   },
 * });
 *
 * try {
 *   const result = await processBatch(batch);
 *   dolakeMetrics.cdcEventsTotal.inc({ table: batch.table, operation: 'INSERT' }, result.count);
 *   span.setStatus('OK');
 * } catch (error) {
 *   dolakeMetrics.errorsTotal.inc({ operation: 'cdc_process', error_type: 'validation', table: batch.table });
 *   span.setStatus('ERROR', error.message);
 *   throw error;
 * } finally {
 *   span.end();
 * }
 * ```
 *
 * @packageDocumentation
 */

// =============================================================================
// TYPE EXPORTS
// =============================================================================

export {
  // Span types
  type Span,
  type SpanKind,
  type SpanStatus,
  type SpanEvent,
  type SpanOptions,
  type TraceContext,
  type Tracer,
  type AttributeValue,
  type WebSocketTraceAttachment,

  // Metric types
  type Counter,
  type Histogram,
  type HistogramValue,
  type Gauge,
  type MetricsRegistry,

  // DoLake-specific types
  type CDCProcessResult,
  type FlushTraceResult,
  type DoLakeMetrics,

  // Configuration types
  type TracingConfig,
  type MetricsConfig,
  type ObservabilityConfig,
  DEFAULT_OBSERVABILITY_CONFIG,
} from './types.js';

// =============================================================================
// TRACER EXPORTS
// =============================================================================

export {
  TracerImpl,
  NoOpTracer,
  createTracer,
} from './tracer.js';

// =============================================================================
// METRICS EXPORTS
// =============================================================================

export {
  MetricsRegistryImpl,
  NoOpMetricsRegistry,
  createMetricsRegistry,
  createDoLakeMetrics,
} from './metrics.js';

// =============================================================================
// CONVENIENCE FACTORY
// =============================================================================

import type {
  Tracer,
  MetricsRegistry,
  ObservabilityConfig,
  DoLakeMetrics,
  Span,
  CDCProcessResult,
  FlushTraceResult,
} from './types.js';
import { DEFAULT_OBSERVABILITY_CONFIG } from './types.js';
import { createTracer } from './tracer.js';
import { createMetricsRegistry, createDoLakeMetrics } from './metrics.js';

/**
 * Observability instance containing all components
 */
export interface Observability {
  tracer: Tracer;
  metrics: MetricsRegistry;
  config: ObservabilityConfig;
}

/**
 * Create an observability instance with all components
 */
export function createObservability(config: Partial<ObservabilityConfig> = {}): Observability {
  const mergedConfig: ObservabilityConfig = {
    tracing: { ...DEFAULT_OBSERVABILITY_CONFIG.tracing, ...config.tracing },
    metrics: { ...DEFAULT_OBSERVABILITY_CONFIG.metrics, ...config.metrics },
  };

  return {
    tracer: createTracer(mergedConfig.tracing),
    metrics: createMetricsRegistry(mergedConfig.metrics),
    config: mergedConfig,
  };
}

// =============================================================================
// INSTRUMENTATION HELPERS
// =============================================================================

/**
 * Instrument CDC batch processing
 */
export async function instrumentCDCBatch<T>(
  observability: Observability,
  dolakeMetrics: DoLakeMetrics,
  batchInfo: { batchId: string; sourceId: string; table: string; eventCount: number; sizeBytes: number },
  execute: () => Promise<T & CDCProcessResult>
): Promise<T & CDCProcessResult> {
  const { tracer } = observability;

  const span = tracer.startSpan('dolake.processCDCBatch', {
    kind: 'CONSUMER',
    attributes: {
      'cdc.batch_id': batchInfo.batchId,
      'cdc.source_id': batchInfo.sourceId,
      'cdc.table': batchInfo.table,
      'cdc.event_count': batchInfo.eventCount,
    },
  });

  const startTime = performance.now();

  try {
    const result = await execute();

    const durationSeconds = (performance.now() - startTime) / 1000;

    span.setAttribute('cdc.events_accepted', result.eventsAccepted);
    span.setAttribute('cdc.is_duplicate', result.isDuplicate);

    if (result.isDuplicate) {
      dolakeMetrics.cdcDuplicatesTotal.inc({ table: batchInfo.table });
      span.addEvent('duplicate_detected');
    }

    dolakeMetrics.cdcBatchesTotal.inc({ table: batchInfo.table, source: batchInfo.sourceId });
    dolakeMetrics.cdcEventsTotal.inc({ table: batchInfo.table, operation: 'batch' }, batchInfo.eventCount);
    dolakeMetrics.cdcBytesTotal.inc({ table: batchInfo.table }, batchInfo.sizeBytes);
    dolakeMetrics.cdcProcessingDuration.observe({ table: batchInfo.table }, durationSeconds);

    span.setStatus('OK');

    return result;
  } catch (error) {
    const errorType = error instanceof Error ? (error as Error).constructor.name : 'Error';

    span.setStatus('ERROR', error instanceof Error ? error.message : String(error));
    span.addEvent('exception', {
      'exception.type': errorType,
      'exception.message': error instanceof Error ? error.message : String(error),
    });

    dolakeMetrics.errorsTotal.inc({ operation: 'cdc_process', error_type: errorType, table: batchInfo.table });

    throw error;
  } finally {
    span.end();
  }
}

/**
 * Instrument flush operation
 */
export async function instrumentFlush<T>(
  observability: Observability,
  dolakeMetrics: DoLakeMetrics,
  trigger: string,
  execute: () => Promise<T & FlushTraceResult>
): Promise<T & FlushTraceResult> {
  const { tracer } = observability;

  const span = tracer.startSpan('dolake.flush', {
    kind: 'INTERNAL',
    attributes: {
      'flush.trigger': trigger,
    },
  });

  const startTime = performance.now();

  try {
    const result = await execute();

    const durationSeconds = (performance.now() - startTime) / 1000;

    span.setAttribute('flush.files_written', result.filesWritten);
    span.setAttribute('flush.events_written', result.eventsWritten);
    span.setAttribute('flush.bytes_written', result.bytesWritten);
    span.setAttribute('flush.tables', result.tables.join(','));

    for (const table of result.tables) {
      dolakeMetrics.flushTotal.inc({ table, trigger, success: 'true' });
      dolakeMetrics.flushDuration.observe({ table, trigger }, durationSeconds);
      dolakeMetrics.flushFilesWritten.inc({ table }, result.filesWritten);
      dolakeMetrics.flushEventsWritten.inc({ table }, result.eventsWritten);
    }

    span.setStatus('OK');

    return result;
  } catch (error) {
    const errorType = error instanceof Error ? (error as Error).constructor.name : 'Error';

    span.setStatus('ERROR', error instanceof Error ? error.message : String(error));
    span.addEvent('exception', {
      'exception.type': errorType,
      'exception.message': error instanceof Error ? error.message : String(error),
    });

    dolakeMetrics.flushTotal.inc({ table: 'unknown', trigger, success: 'false' });
    dolakeMetrics.errorsTotal.inc({ operation: 'flush', error_type: errorType, table: 'unknown' });

    throw error;
  } finally {
    span.end();
  }
}

/**
 * Instrument Parquet write
 */
export async function instrumentParquetWrite<T>(
  observability: Observability,
  dolakeMetrics: DoLakeMetrics,
  table: string,
  execute: () => Promise<T & { rowCount: number; sizeBytes: number }>
): Promise<T & { rowCount: number; sizeBytes: number }> {
  const { tracer } = observability;

  const span = tracer.startSpan('dolake.parquetWrite', {
    kind: 'INTERNAL',
    attributes: {
      'parquet.table': table,
    },
  });

  const startTime = performance.now();

  try {
    const result = await execute();

    const durationSeconds = (performance.now() - startTime) / 1000;

    span.setAttribute('parquet.row_count', result.rowCount);
    span.setAttribute('parquet.file_size_bytes', result.sizeBytes);

    dolakeMetrics.parquetWritesTotal.inc({ table });
    dolakeMetrics.parquetRowsWritten.inc({ table }, result.rowCount);
    dolakeMetrics.parquetBytesWritten.inc({ table }, result.sizeBytes);
    dolakeMetrics.parquetWriteDuration.observe({ table }, durationSeconds);

    span.setStatus('OK');

    return result;
  } catch (error) {
    const errorType = error instanceof Error ? (error as Error).constructor.name : 'Error';

    span.setStatus('ERROR', error instanceof Error ? error.message : String(error));
    dolakeMetrics.errorsTotal.inc({ operation: 'parquet_write', error_type: errorType, table });

    throw error;
  } finally {
    span.end();
  }
}

/**
 * Instrument Iceberg commit
 */
export async function instrumentIcebergCommit<T>(
  observability: Observability,
  dolakeMetrics: DoLakeMetrics,
  table: string,
  execute: () => Promise<T>
): Promise<T> {
  const { tracer } = observability;

  const span = tracer.startSpan('dolake.icebergCommit', {
    kind: 'INTERNAL',
    attributes: {
      'iceberg.table': table,
    },
  });

  const startTime = performance.now();

  try {
    const result = await execute();

    const durationSeconds = (performance.now() - startTime) / 1000;

    span.addEvent('iceberg.commit', {
      'iceberg.table': table,
    });

    dolakeMetrics.icebergCommitsTotal.inc({ table, success: 'true' });
    dolakeMetrics.icebergCommitDuration.observe({ table }, durationSeconds);

    span.setStatus('OK');

    return result;
  } catch (error) {
    const errorType = error instanceof Error ? (error as Error).constructor.name : 'Error';

    span.setStatus('ERROR', error instanceof Error ? error.message : String(error));
    dolakeMetrics.icebergCommitsTotal.inc({ table, success: 'false' });
    dolakeMetrics.errorsTotal.inc({ operation: 'iceberg_commit', error_type: errorType, table });

    throw error;
  } finally {
    span.end();
  }
}

/**
 * Generate enhanced Prometheus metrics string
 * Combines existing dolake metrics with new observability metrics
 */
export function generateEnhancedMetrics(
  dolakeMetrics: DoLakeMetrics,
  registry: MetricsRegistry,
  additionalMetrics?: Record<string, number | string>
): string {
  let output = registry.getMetrics();

  // Add any additional ad-hoc metrics
  if (additionalMetrics) {
    const lines: string[] = [];
    for (const [key, value] of Object.entries(additionalMetrics)) {
      if (typeof value === 'number') {
        lines.push(`${key} ${value}`);
      }
    }
    if (lines.length > 0) {
      output += '\n\n' + lines.join('\n');
    }
  }

  return output;
}
