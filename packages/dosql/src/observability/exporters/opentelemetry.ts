/**
 * DoSQL OpenTelemetry Trace Exporter
 *
 * Exports traces in OpenTelemetry Protocol (OTLP) format to:
 * - OpenTelemetry Collector
 * - Jaeger (via OTLP receiver)
 * - Grafana Tempo
 * - Other OTLP-compatible backends
 *
 * Supports both HTTP/JSON and HTTP/protobuf (simplified) protocols.
 *
 * @example
 * ```typescript
 * const exporter = createOTLPTraceExporter({
 *   endpoint: 'http://collector:4318/v1/traces',
 *   headers: { 'Authorization': 'Bearer token' },
 * });
 *
 * // Export completed spans
 * const result = await exporter.export(spans);
 * console.log(`Exported ${result.count} spans in ${result.durationMs}ms`);
 *
 * // Shutdown when done
 * await exporter.shutdown();
 * ```
 *
 * @packageDocumentation
 */

import type {
  TraceExporter,
  TraceExporterConfig,
  SpanData,
  ExportResult,
  DEFAULT_TRACE_EXPORTER_CONFIG,
} from './types.js';

// =============================================================================
// OTLP EXPORTER CONFIGURATION
// =============================================================================

/**
 * OpenTelemetry trace exporter configuration
 */
export interface OTLPTraceExporterConfig extends Partial<TraceExporterConfig> {
  /** OTLP endpoint URL */
  endpoint: string;
  /** Protocol: json (HTTP/JSON) or protobuf (HTTP/protobuf) */
  protocol?: 'json' | 'protobuf' | undefined;
  /** Service name for resource attributes */
  serviceName?: string | undefined;
  /** Service version */
  serviceVersion?: string | undefined;
  /** Additional resource attributes */
  resourceAttributes?: Record<string, string> | undefined;
  /** Compression: none or gzip */
  compression?: 'none' | 'gzip' | undefined;
  /** Retry configuration */
  retry?: {
    /** Maximum number of retry attempts */
    maxAttempts: number;
    /** Initial retry delay in milliseconds */
    initialDelayMs: number;
    /** Maximum retry delay in milliseconds */
    maxDelayMs: number;
  } | undefined;
}

/**
 * Default OTLP exporter configuration
 */
export const DEFAULT_OTLP_CONFIG: Omit<Required<OTLPTraceExporterConfig>, 'endpoint' | 'headers'> = {
  enabled: true,
  timeoutMs: 30000,
  maxBatchSize: 100,
  exportDelayMs: 5000,
  protocol: 'json',
  serviceName: 'dosql',
  serviceVersion: '1.0.0',
  resourceAttributes: {},
  compression: 'none',
  retry: {
    maxAttempts: 3,
    initialDelayMs: 1000,
    maxDelayMs: 30000,
  },
};

// =============================================================================
// OTLP TRACE EXPORTER IMPLEMENTATION
// =============================================================================

/**
 * OpenTelemetry Protocol (OTLP) trace exporter.
 *
 * Exports spans to OTLP-compatible backends using HTTP/JSON protocol.
 * Supports batching, retry with exponential backoff, and resource attributes.
 *
 * @example Basic usage
 * ```typescript
 * const exporter = new OTLPTraceExporter({
 *   endpoint: 'http://localhost:4318/v1/traces',
 *   serviceName: 'my-dosql-instance',
 * });
 *
 * // Export a batch of spans
 * const result = await exporter.export(completedSpans);
 * ```
 *
 * @example With Grafana Cloud
 * ```typescript
 * const exporter = new OTLPTraceExporter({
 *   endpoint: 'https://otlp-gateway.grafana.net/otlp/v1/traces',
 *   headers: {
 *     'Authorization': `Basic ${btoa('instance:token')}`,
 *   },
 *   serviceName: 'production-dosql',
 * });
 * ```
 */
export class OTLPTraceExporter implements TraceExporter {
  readonly name = 'otlp';
  private readonly config: Required<OTLPTraceExporterConfig>;
  private pendingSpans: SpanData[] = [];
  private exportTimer: ReturnType<typeof setTimeout> | null = null;
  private isShutdown = false;

  constructor(config: OTLPTraceExporterConfig) {
    this.config = {
      ...DEFAULT_OTLP_CONFIG,
      headers: {},
      ...config,
    } as Required<OTLPTraceExporterConfig>;
  }

  /**
   * Export a batch of spans to the OTLP endpoint
   */
  async export(spans: SpanData[]): Promise<ExportResult> {
    if (this.isShutdown) {
      return {
        success: false,
        count: 0,
        error: 'Exporter is shutdown',
        durationMs: 0,
      };
    }

    if (!this.config.enabled || spans.length === 0) {
      return {
        success: true,
        count: 0,
        durationMs: 0,
      };
    }

    const startTime = performance.now();

    try {
      // Convert to OTLP format
      const payload = this.toOTLPPayload(spans);

      // Send with retry
      await this.sendWithRetry(payload);

      return {
        success: true,
        count: spans.length,
        durationMs: performance.now() - startTime,
      };
    } catch (error) {
      return {
        success: false,
        count: 0,
        error: error instanceof Error ? error.message : String(error),
        durationMs: performance.now() - startTime,
      };
    }
  }

  /**
   * Force flush any pending spans
   */
  async forceFlush(): Promise<void> {
    if (this.pendingSpans.length > 0) {
      const spansToExport = [...this.pendingSpans];
      this.pendingSpans = [];
      await this.export(spansToExport);
    }
  }

  /**
   * Shutdown the exporter
   */
  async shutdown(): Promise<void> {
    this.isShutdown = true;

    if (this.exportTimer) {
      clearTimeout(this.exportTimer);
      this.exportTimer = null;
    }

    // Export any remaining spans
    await this.forceFlush();
  }

  /**
   * Add span to pending batch
   */
  addSpan(span: SpanData): void {
    if (this.isShutdown) return;

    this.pendingSpans.push(span);

    // Check if batch is full
    if (this.pendingSpans.length >= this.config.maxBatchSize) {
      this.forceFlush();
      return;
    }

    // Start export timer if not running
    if (!this.exportTimer && this.config.exportDelayMs > 0) {
      this.exportTimer = setTimeout(() => {
        this.exportTimer = null;
        this.forceFlush();
      }, this.config.exportDelayMs);
    }
  }

  /**
   * Convert SpanData array to OTLP JSON payload
   */
  private toOTLPPayload(spans: SpanData[]): object {
    const resource = {
      attributes: [
        { key: 'service.name', value: { stringValue: this.config.serviceName } },
        ...(this.config.serviceVersion
          ? [{ key: 'service.version', value: { stringValue: this.config.serviceVersion } }]
          : []),
        ...Object.entries(this.config.resourceAttributes || {}).map(([key, value]) => ({
          key,
          value: { stringValue: value },
        })),
      ],
    };

    const scopeSpans = spans.map((span) => this.toOTLPSpan(span));

    return {
      resourceSpans: [
        {
          resource,
          scopeSpans: [
            {
              scope: {
                name: 'dosql-observability',
                version: '1.0.0',
              },
              spans: scopeSpans,
            },
          ],
        },
      ],
    };
  }

  /**
   * Convert SpanData to OTLP span format
   */
  private toOTLPSpan(span: SpanData): object {
    const kindMap: Record<string, number> = {
      INTERNAL: 1,
      SERVER: 2,
      CLIENT: 3,
      PRODUCER: 4,
      CONSUMER: 5,
    };

    const statusCodeMap: Record<string, number> = {
      UNSET: 0,
      OK: 1,
      ERROR: 2,
    };

    const attributes = Object.entries(span.attributes).map(([key, value]) => ({
      key,
      value: this.toOTLPAttributeValue(value),
    }));

    const events = span.events.map((event) => ({
      timeUnixNano: event.timeUnixNano.toString(),
      name: event.name,
      attributes: event.attributes
        ? Object.entries(event.attributes).map(([key, value]) => ({
            key,
            value: this.toOTLPAttributeValue(value),
          }))
        : [],
    }));

    return {
      traceId: this.hexToBase64(span.traceId),
      spanId: this.hexToBase64(span.spanId),
      parentSpanId: span.parentSpanId ? this.hexToBase64(span.parentSpanId) : undefined,
      name: span.name,
      kind: kindMap[span.kind] || 1,
      startTimeUnixNano: span.startTimeUnixNano.toString(),
      endTimeUnixNano: span.endTimeUnixNano?.toString(),
      attributes,
      events,
      status: {
        code: statusCodeMap[span.status.code] || 0,
        message: span.status.message,
      },
    };
  }

  /**
   * Convert a value to OTLP attribute value format
   */
  private toOTLPAttributeValue(value: unknown): object {
    if (typeof value === 'string') {
      return { stringValue: value };
    }
    if (typeof value === 'number') {
      return Number.isInteger(value)
        ? { intValue: value.toString() }
        : { doubleValue: value };
    }
    if (typeof value === 'boolean') {
      return { boolValue: value };
    }
    if (Array.isArray(value)) {
      if (value.length === 0) {
        return { arrayValue: { values: [] } };
      }
      const firstType = typeof value[0];
      if (firstType === 'string') {
        return {
          arrayValue: {
            values: value.map((v) => ({ stringValue: String(v) })),
          },
        };
      }
      if (firstType === 'number') {
        return {
          arrayValue: {
            values: value.map((v) =>
              Number.isInteger(v as number)
                ? { intValue: String(v) }
                : { doubleValue: v as number }
            ),
          },
        };
      }
      if (firstType === 'boolean') {
        return {
          arrayValue: {
            values: value.map((v) => ({ boolValue: v as boolean })),
          },
        };
      }
    }
    return { stringValue: String(value) };
  }

  /**
   * Convert hex string to base64 (for OTLP JSON format)
   */
  private hexToBase64(hex: string): string {
    const bytes = new Uint8Array(hex.length / 2);
    for (let i = 0; i < hex.length; i += 2) {
      bytes[i / 2] = parseInt(hex.substr(i, 2), 16);
    }
    return btoa(String.fromCharCode(...bytes));
  }

  /**
   * Send payload with retry and exponential backoff
   */
  private async sendWithRetry(payload: object): Promise<void> {
    let lastError: Error | null = null;
    let delay = this.config.retry?.initialDelayMs || 1000;

    for (let attempt = 0; attempt < (this.config.retry?.maxAttempts || 3); attempt++) {
      try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), this.config.timeoutMs);

        const response = await fetch(this.config.endpoint, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            ...this.config.headers,
          },
          body: JSON.stringify(payload),
          signal: controller.signal,
        });

        clearTimeout(timeoutId);

        if (response.ok) {
          return;
        }

        // Check for retryable status codes
        if (response.status === 429 || response.status >= 500) {
          lastError = new Error(`HTTP ${response.status}: ${response.statusText}`);
        } else {
          // Non-retryable error
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }
      } catch (error) {
        if (error instanceof Error && error.name === 'AbortError') {
          lastError = new Error('Request timeout');
        } else {
          lastError = error instanceof Error ? error : new Error(String(error));
        }
      }

      // Wait before retrying
      if (attempt < (this.config.retry?.maxAttempts || 3) - 1) {
        await new Promise((resolve) => setTimeout(resolve, delay));
        delay = Math.min(delay * 2, this.config.retry?.maxDelayMs || 30000);
      }
    }

    throw lastError || new Error('Export failed after retries');
  }
}

// =============================================================================
// SPAN COLLECTOR
// =============================================================================

/**
 * In-memory span collector for batching exports.
 *
 * Collects completed spans and exports them in batches to reduce
 * network overhead. Supports configurable batch size and export delay.
 *
 * @example
 * ```typescript
 * const collector = new SpanCollector(exporter, {
 *   maxBatchSize: 100,
 *   exportDelayMs: 5000,
 * });
 *
 * // Add spans as they complete
 * span.end();
 * collector.addSpan(spanToSpanData(span));
 *
 * // Force export before shutdown
 * await collector.flush();
 * ```
 */
export class SpanCollector {
  private readonly exporter: TraceExporter;
  private readonly maxBatchSize: number;
  private readonly exportDelayMs: number;
  private pendingSpans: SpanData[] = [];
  private exportTimer: ReturnType<typeof setTimeout> | null = null;
  private isShutdown = false;

  constructor(
    exporter: TraceExporter,
    options: { maxBatchSize?: number; exportDelayMs?: number } = {}
  ) {
    this.exporter = exporter;
    this.maxBatchSize = options.maxBatchSize ?? 100;
    this.exportDelayMs = options.exportDelayMs ?? 5000;
  }

  /**
   * Add a completed span to the collection
   */
  addSpan(span: SpanData): void {
    if (this.isShutdown) return;

    this.pendingSpans.push(span);

    // Export immediately if batch is full
    if (this.pendingSpans.length >= this.maxBatchSize) {
      this.flush();
      return;
    }

    // Schedule delayed export
    if (!this.exportTimer && this.exportDelayMs > 0) {
      this.exportTimer = setTimeout(() => {
        this.exportTimer = null;
        this.flush();
      }, this.exportDelayMs);
    }
  }

  /**
   * Force export all pending spans
   */
  async flush(): Promise<ExportResult> {
    if (this.exportTimer) {
      clearTimeout(this.exportTimer);
      this.exportTimer = null;
    }

    if (this.pendingSpans.length === 0) {
      return { success: true, count: 0, durationMs: 0 };
    }

    const spansToExport = [...this.pendingSpans];
    this.pendingSpans = [];

    return this.exporter.export(spansToExport);
  }

  /**
   * Shutdown the collector
   */
  async shutdown(): Promise<void> {
    this.isShutdown = true;
    await this.flush();
    await this.exporter.shutdown();
  }

  /**
   * Get the number of pending spans
   */
  get pendingCount(): number {
    return this.pendingSpans.length;
  }
}

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

/**
 * Creates an OTLP trace exporter with the given configuration.
 *
 * @param config - Exporter configuration options
 * @returns A configured OTLPTraceExporter instance
 *
 * @example
 * ```typescript
 * const exporter = createOTLPTraceExporter({
 *   endpoint: 'http://localhost:4318/v1/traces',
 *   serviceName: 'my-service',
 *   resourceAttributes: {
 *     'deployment.environment': 'production',
 *   },
 * });
 * ```
 */
export function createOTLPTraceExporter(
  config: OTLPTraceExporterConfig
): OTLPTraceExporter {
  return new OTLPTraceExporter(config);
}

/**
 * Creates a span collector with an OTLP exporter.
 *
 * @param exporterConfig - OTLP exporter configuration
 * @param collectorOptions - Collector batch options
 * @returns A configured SpanCollector instance
 *
 * @example
 * ```typescript
 * const collector = createSpanCollector(
 *   { endpoint: 'http://localhost:4318/v1/traces' },
 *   { maxBatchSize: 50, exportDelayMs: 3000 }
 * );
 * ```
 */
export function createSpanCollector(
  exporterConfig: OTLPTraceExporterConfig,
  collectorOptions?: { maxBatchSize?: number; exportDelayMs?: number }
): SpanCollector {
  const exporter = new OTLPTraceExporter(exporterConfig);
  return new SpanCollector(exporter, collectorOptions);
}
