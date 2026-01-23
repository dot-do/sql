/**
 * DoLake Prometheus Metrics Implementation
 *
 * Prometheus-compatible metrics collection for CDC processing,
 * Parquet writing, and Iceberg commit operations.
 */

import type {
  Counter,
  Histogram,
  HistogramValue,
  Gauge,
  MetricsRegistry,
  MetricsConfig,
  DoLakeMetrics,
} from './types.js';

// =============================================================================
// METRIC IMPLEMENTATIONS
// =============================================================================

function labelsToKey(labels: Record<string, string>): string {
  const entries = Object.entries(labels).sort((a, b) => a[0].localeCompare(b[0]));
  return entries.map(([k, v]) => `${k}="${v}"`).join(',');
}

class CounterImpl implements Counter {
  readonly name: string;
  readonly help: string;
  readonly labels: string[];
  private readonly values: Map<string, number> = new Map();

  constructor(name: string, help: string, labels: string[] = []) {
    this.name = name;
    this.help = help;
    this.labels = labels;
  }

  inc(labels: Record<string, string> = {}, value = 1): void {
    const key = labelsToKey(labels);
    const current = this.values.get(key) ?? 0;
    this.values.set(key, current + value);
  }

  get(labels: Record<string, string> = {}): number {
    const key = labelsToKey(labels);
    return this.values.get(key) ?? 0;
  }

  reset(): void {
    this.values.clear();
  }

  toPrometheus(): string {
    const lines: string[] = [];
    lines.push(`# HELP ${this.name} ${this.help}`);
    lines.push(`# TYPE ${this.name} counter`);

    if (this.values.size === 0) {
      lines.push(`${this.name} 0`);
    } else {
      for (const [key, value] of Array.from(this.values.entries())) {
        if (key) {
          lines.push(`${this.name}{${key}} ${value}`);
        } else {
          lines.push(`${this.name} ${value}`);
        }
      }
    }

    return lines.join('\n');
  }
}

class HistogramImpl implements Histogram {
  readonly name: string;
  readonly help: string;
  readonly labels: string[];
  readonly buckets: number[];
  private readonly values: Map<string, { sum: number; count: number; buckets: Map<number, number> }> = new Map();

  constructor(name: string, help: string, labels: string[] = [], buckets: number[] = []) {
    this.name = name;
    this.help = help;
    this.labels = labels;
    this.buckets = [...buckets].sort((a, b) => a - b);
  }

  observe(labels: Record<string, string>, value: number): void {
    const key = labelsToKey(labels);
    let data = this.values.get(key);

    if (!data) {
      data = {
        sum: 0,
        count: 0,
        buckets: new Map(this.buckets.map((b) => [b, 0])),
      };
      this.values.set(key, data);
    }

    data.sum += value;
    data.count += 1;

    for (const bucket of this.buckets) {
      if (value <= bucket) {
        data.buckets.set(bucket, (data.buckets.get(bucket) ?? 0) + 1);
      }
    }
  }

  get(labels: Record<string, string> = {}): HistogramValue {
    const key = labelsToKey(labels);
    const data = this.values.get(key);

    if (!data) {
      return {
        sum: 0,
        count: 0,
        buckets: new Map(this.buckets.map((b) => [b, 0])),
      };
    }

    return {
      sum: data.sum,
      count: data.count,
      buckets: new Map(data.buckets),
    };
  }

  reset(): void {
    this.values.clear();
  }

  toPrometheus(): string {
    const lines: string[] = [];
    lines.push(`# HELP ${this.name} ${this.help}`);
    lines.push(`# TYPE ${this.name} histogram`);

    for (const [key, data] of Array.from(this.values.entries())) {
      let cumulative = 0;
      for (const bucket of this.buckets) {
        cumulative += data.buckets.get(bucket) ?? 0;
        if (key) {
          lines.push(`${this.name}_bucket{${key},le="${bucket}"} ${cumulative}`);
        } else {
          lines.push(`${this.name}_bucket{le="${bucket}"} ${cumulative}`);
        }
      }

      if (key) {
        lines.push(`${this.name}_bucket{${key},le="+Inf"} ${data.count}`);
        lines.push(`${this.name}_sum{${key}} ${data.sum}`);
        lines.push(`${this.name}_count{${key}} ${data.count}`);
      } else {
        lines.push(`${this.name}_bucket{le="+Inf"} ${data.count}`);
        lines.push(`${this.name}_sum ${data.sum}`);
        lines.push(`${this.name}_count ${data.count}`);
      }
    }

    if (this.values.size === 0) {
      for (const bucket of this.buckets) {
        lines.push(`${this.name}_bucket{le="${bucket}"} 0`);
      }
      lines.push(`${this.name}_bucket{le="+Inf"} 0`);
      lines.push(`${this.name}_sum 0`);
      lines.push(`${this.name}_count 0`);
    }

    return lines.join('\n');
  }
}

class GaugeImpl implements Gauge {
  readonly name: string;
  readonly help: string;
  readonly labels: string[];
  private readonly values: Map<string, number> = new Map();

  constructor(name: string, help: string, labels: string[] = []) {
    this.name = name;
    this.help = help;
    this.labels = labels;
  }

  set(labels: Record<string, string>, value: number): void {
    const key = labelsToKey(labels);
    this.values.set(key, value);
  }

  inc(labels: Record<string, string> = {}, value = 1): void {
    const key = labelsToKey(labels);
    const current = this.values.get(key) ?? 0;
    this.values.set(key, current + value);
  }

  dec(labels: Record<string, string> = {}, value = 1): void {
    const key = labelsToKey(labels);
    const current = this.values.get(key) ?? 0;
    this.values.set(key, current - value);
  }

  get(labels: Record<string, string> = {}): number {
    const key = labelsToKey(labels);
    return this.values.get(key) ?? 0;
  }

  reset(): void {
    this.values.clear();
  }

  toPrometheus(): string {
    const lines: string[] = [];
    lines.push(`# HELP ${this.name} ${this.help}`);
    lines.push(`# TYPE ${this.name} gauge`);

    if (this.values.size === 0) {
      lines.push(`${this.name} 0`);
    } else {
      for (const [key, value] of Array.from(this.values.entries())) {
        if (key) {
          lines.push(`${this.name}{${key}} ${value}`);
        } else {
          lines.push(`${this.name} ${value}`);
        }
      }
    }

    return lines.join('\n');
  }
}

// =============================================================================
// METRICS REGISTRY
// =============================================================================

export class MetricsRegistryImpl implements MetricsRegistry {
  private readonly config: MetricsConfig;
  private readonly counters: Map<string, CounterImpl> = new Map();
  private readonly histograms: Map<string, HistogramImpl> = new Map();
  private readonly gauges: Map<string, GaugeImpl> = new Map();

  constructor(config: MetricsConfig) {
    this.config = config;
  }

  createCounter(name: string, help: string, labels: string[] = []): Counter {
    const fullName = this.config.prefix ? `${this.config.prefix}_${name}` : name;
    let counter = this.counters.get(fullName);

    if (!counter) {
      counter = new CounterImpl(fullName, help, labels);
      this.counters.set(fullName, counter);
    }

    return counter;
  }

  createHistogram(
    name: string,
    help: string,
    labels: string[] = [],
    buckets: number[] = this.config.histogramBuckets.latency
  ): Histogram {
    const fullName = this.config.prefix ? `${this.config.prefix}_${name}` : name;
    let histogram = this.histograms.get(fullName);

    if (!histogram) {
      histogram = new HistogramImpl(fullName, help, labels, buckets);
      this.histograms.set(fullName, histogram);
    }

    return histogram;
  }

  createGauge(name: string, help: string, labels: string[] = []): Gauge {
    const fullName = this.config.prefix ? `${this.config.prefix}_${name}` : name;
    let gauge = this.gauges.get(fullName);

    if (!gauge) {
      gauge = new GaugeImpl(fullName, help, labels);
      this.gauges.set(fullName, gauge);
    }

    return gauge;
  }

  getMetrics(): string {
    const sections: string[] = [];

    for (const counter of Array.from(this.counters.values())) {
      sections.push(counter.toPrometheus());
    }

    for (const histogram of Array.from(this.histograms.values())) {
      sections.push(histogram.toPrometheus());
    }

    for (const gauge of Array.from(this.gauges.values())) {
      sections.push(gauge.toPrometheus());
    }

    return sections.join('\n\n');
  }

  reset(): void {
    for (const counter of Array.from(this.counters.values())) {
      counter.reset();
    }
    for (const histogram of Array.from(this.histograms.values())) {
      histogram.reset();
    }
    for (const gauge of Array.from(this.gauges.values())) {
      gauge.reset();
    }
  }
}

// =============================================================================
// NO-OP IMPLEMENTATIONS
// =============================================================================

class NoOpCounter implements Counter {
  readonly name = '';
  readonly help = '';
  readonly labels: string[] = [];
  inc(): void {}
  get(): number { return 0; }
  reset(): void {}
}

class NoOpHistogram implements Histogram {
  readonly name = '';
  readonly help = '';
  readonly labels: string[] = [];
  readonly buckets: number[] = [];
  observe(): void {}
  get(): HistogramValue { return { sum: 0, count: 0, buckets: new Map() }; }
  reset(): void {}
}

class NoOpGauge implements Gauge {
  readonly name = '';
  readonly help = '';
  readonly labels: string[] = [];
  set(): void {}
  inc(): void {}
  dec(): void {}
  get(): number { return 0; }
  reset(): void {}
}

export class NoOpMetricsRegistry implements MetricsRegistry {
  private static readonly noOpCounter = new NoOpCounter();
  private static readonly noOpHistogram = new NoOpHistogram();
  private static readonly noOpGauge = new NoOpGauge();

  createCounter(): Counter { return NoOpMetricsRegistry.noOpCounter; }
  createHistogram(): Histogram { return NoOpMetricsRegistry.noOpHistogram; }
  createGauge(): Gauge { return NoOpMetricsRegistry.noOpGauge; }
  getMetrics(): string { return ''; }
  reset(): void {}
}

/**
 * Create a metrics registry
 */
export function createMetricsRegistry(config: MetricsConfig): MetricsRegistry {
  if (!config.enabled) {
    return new NoOpMetricsRegistry();
  }
  return new MetricsRegistryImpl(config);
}

// =============================================================================
// DOLAKE-SPECIFIC METRICS
// =============================================================================

/**
 * Create standard DoLake metrics
 */
export function createDoLakeMetrics(registry: MetricsRegistry): DoLakeMetrics {
  const latencyBuckets = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];

  return {
    // CDC metrics
    cdcBatchesTotal: registry.createCounter(
      'cdc_batches_total',
      'Total CDC batches received',
      ['table', 'source']
    ),

    cdcEventsTotal: registry.createCounter(
      'cdc_events_total',
      'Total CDC events received',
      ['table', 'operation']
    ),

    cdcBytesTotal: registry.createCounter(
      'cdc_bytes_total',
      'Total CDC bytes received',
      ['table']
    ),

    cdcProcessingDuration: registry.createHistogram(
      'cdc_processing_duration_seconds',
      'CDC batch processing duration in seconds',
      ['table'],
      latencyBuckets
    ),

    cdcDuplicatesTotal: registry.createCounter(
      'cdc_duplicates_total',
      'Total duplicate CDC batches detected',
      ['table']
    ),

    cdcLag: registry.createGauge(
      'cdc_lag_seconds',
      'CDC replication lag in seconds',
      ['table']
    ),

    // Flush metrics
    flushTotal: registry.createCounter(
      'flush_total',
      'Total flush operations',
      ['table', 'trigger', 'success']
    ),

    flushDuration: registry.createHistogram(
      'flush_duration_seconds',
      'Flush operation duration in seconds',
      ['table', 'trigger'],
      latencyBuckets
    ),

    flushFilesWritten: registry.createCounter(
      'flush_files_written_total',
      'Total files written during flushes',
      ['table']
    ),

    flushEventsWritten: registry.createCounter(
      'flush_events_written_total',
      'Total events written during flushes',
      ['table']
    ),

    bufferPressure: registry.createGauge(
      'buffer_pressure',
      'Buffer pressure (0-1)',
      ['table']
    ),

    bufferEvents: registry.createGauge(
      'buffer_events',
      'Current events in buffer',
      ['table']
    ),

    bufferBytes: registry.createGauge(
      'buffer_bytes',
      'Current bytes in buffer',
      ['table']
    ),

    // Parquet metrics
    parquetWritesTotal: registry.createCounter(
      'parquet_writes_total',
      'Total Parquet files written',
      ['table']
    ),

    parquetRowsWritten: registry.createCounter(
      'parquet_rows_written_total',
      'Total rows written to Parquet',
      ['table']
    ),

    parquetBytesWritten: registry.createCounter(
      'parquet_bytes_written_total',
      'Total bytes written to Parquet',
      ['table']
    ),

    parquetWriteDuration: registry.createHistogram(
      'parquet_write_duration_seconds',
      'Parquet write duration in seconds',
      ['table'],
      latencyBuckets
    ),

    // Iceberg metrics
    icebergCommitsTotal: registry.createCounter(
      'iceberg_commits_total',
      'Total Iceberg commits',
      ['table', 'success']
    ),

    icebergCommitDuration: registry.createHistogram(
      'iceberg_commit_duration_seconds',
      'Iceberg commit duration in seconds',
      ['table'],
      latencyBuckets
    ),

    icebergSnapshots: registry.createGauge(
      'iceberg_snapshots',
      'Current number of Iceberg snapshots',
      ['table']
    ),

    icebergDataFiles: registry.createGauge(
      'iceberg_data_files',
      'Current number of Iceberg data files',
      ['table']
    ),

    // Compaction metrics
    compactionTotal: registry.createCounter(
      'compaction_total',
      'Total compaction operations',
      ['table']
    ),

    compactionFilesMerged: registry.createCounter(
      'compaction_files_merged_total',
      'Total files merged during compaction',
      ['table']
    ),

    compactionBytesSaved: registry.createCounter(
      'compaction_bytes_saved_total',
      'Total bytes saved by compaction',
      ['table']
    ),

    compactionDuration: registry.createHistogram(
      'compaction_duration_seconds',
      'Compaction duration in seconds',
      ['table'],
      [0.1, 0.5, 1, 2.5, 5, 10, 30, 60]
    ),

    // Error metrics
    errorsTotal: registry.createCounter(
      'errors_total',
      'Total errors',
      ['operation', 'error_type', 'table']
    ),

    // Connection metrics
    connectedSources: registry.createGauge(
      'connected_sources',
      'Number of connected source DOs'
    ),
  };
}
