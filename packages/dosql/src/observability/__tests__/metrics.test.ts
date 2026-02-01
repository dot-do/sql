/**
 * Metrics Tests
 *
 * Tests for Prometheus-compatible metrics: counters, histograms, gauges,
 * the metrics registry, and Prometheus export format.
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { MetricsRegistryImpl, NoOpMetricsRegistry, createMetricsRegistry } from '../metrics.js';
import type { MetricsConfig } from '../types.js';

function makeConfig(overrides: Partial<MetricsConfig> = {}): MetricsConfig {
  return {
    enabled: true,
    prefix: 'test',
    defaultLabels: {},
    histogramBuckets: {
      latency: [0.01, 0.05, 0.1, 0.5, 1],
      size: [100, 1000, 10000],
    },
    ...overrides,
  };
}

// =============================================================================
// Counter
// =============================================================================

describe('Counter', () => {
  let registry: MetricsRegistryImpl;

  beforeEach(() => {
    registry = new MetricsRegistryImpl(makeConfig());
  });

  it('starts at zero', () => {
    const counter = registry.createCounter('requests_total', 'Total requests');
    expect(counter.get()).toBe(0);
  });

  it('increments by 1 by default', () => {
    const counter = registry.createCounter('requests_total', 'Total requests');
    counter.inc();
    expect(counter.get()).toBe(1);
  });

  it('increments by custom value', () => {
    const counter = registry.createCounter('bytes_total', 'Total bytes');
    counter.inc({}, 100);
    expect(counter.get()).toBe(100);
  });

  it('tracks values per label combination', () => {
    const counter = registry.createCounter('requests_total', 'Total requests', ['method']);
    counter.inc({ method: 'GET' }, 5);
    counter.inc({ method: 'POST' }, 3);

    expect(counter.get({ method: 'GET' })).toBe(5);
    expect(counter.get({ method: 'POST' })).toBe(3);
  });

  it('accumulates increments', () => {
    const counter = registry.createCounter('requests_total', 'Total requests');
    counter.inc();
    counter.inc();
    counter.inc();
    expect(counter.get()).toBe(3);
  });

  it('resets to zero', () => {
    const counter = registry.createCounter('requests_total', 'Total requests');
    counter.inc({}, 10);
    counter.reset();
    expect(counter.get()).toBe(0);
  });

  it('applies prefix from config', () => {
    const counter = registry.createCounter('requests_total', 'Total requests');
    expect(counter.name).toBe('test_requests_total');
  });
});

// =============================================================================
// Histogram
// =============================================================================

describe('Histogram', () => {
  let registry: MetricsRegistryImpl;

  beforeEach(() => {
    registry = new MetricsRegistryImpl(makeConfig());
  });

  it('starts with zero sum and count', () => {
    const histogram = registry.createHistogram('duration', 'Duration', [], [0.1, 0.5, 1]);
    const value = histogram.get();
    expect(value.sum).toBe(0);
    expect(value.count).toBe(0);
  });

  it('records observations', () => {
    const histogram = registry.createHistogram('duration', 'Duration', [], [0.1, 0.5, 1]);
    histogram.observe({}, 0.05);
    histogram.observe({}, 0.3);

    const value = histogram.get();
    expect(value.count).toBe(2);
    expect(value.sum).toBeCloseTo(0.35);
  });

  it('distributes values into cumulative buckets', () => {
    const histogram = registry.createHistogram('duration', 'Duration', [], [0.1, 0.5, 1]);
    histogram.observe({}, 0.05); // <= 0.1
    histogram.observe({}, 0.3);  // <= 0.5
    histogram.observe({}, 0.8);  // <= 1

    const value = histogram.get();
    expect(value.buckets.get(0.1)).toBe(1);  // cumulative: 1
    expect(value.buckets.get(0.5)).toBe(2);  // cumulative: 1+1=2
    expect(value.buckets.get(1)).toBe(3);    // cumulative: 1+1+1=3
  });

  it('tracks values per label combination', () => {
    const histogram = registry.createHistogram('duration', 'Duration', ['op'], [0.1, 1]);
    histogram.observe({ op: 'read' }, 0.05);
    histogram.observe({ op: 'write' }, 0.5);

    expect(histogram.get({ op: 'read' }).count).toBe(1);
    expect(histogram.get({ op: 'write' }).count).toBe(1);
  });

  it('resets all data', () => {
    const histogram = registry.createHistogram('duration', 'Duration', [], [0.1, 1]);
    histogram.observe({}, 0.5);
    histogram.reset();

    const value = histogram.get();
    expect(value.count).toBe(0);
    expect(value.sum).toBe(0);
  });

  it('applies prefix from config', () => {
    const histogram = registry.createHistogram('latency', 'Latency');
    expect(histogram.name).toBe('test_latency');
  });
});

// =============================================================================
// Gauge
// =============================================================================

describe('Gauge', () => {
  let registry: MetricsRegistryImpl;

  beforeEach(() => {
    registry = new MetricsRegistryImpl(makeConfig());
  });

  it('starts at zero', () => {
    const gauge = registry.createGauge('connections', 'Connections');
    expect(gauge.get()).toBe(0);
  });

  it('sets a value', () => {
    const gauge = registry.createGauge('connections', 'Connections');
    gauge.set({}, 42);
    expect(gauge.get()).toBe(42);
  });

  it('increments', () => {
    const gauge = registry.createGauge('connections', 'Connections');
    gauge.inc();
    gauge.inc();
    expect(gauge.get()).toBe(2);
  });

  it('decrements', () => {
    const gauge = registry.createGauge('connections', 'Connections');
    gauge.set({}, 10);
    gauge.dec();
    expect(gauge.get()).toBe(9);
  });

  it('increments by custom value', () => {
    const gauge = registry.createGauge('connections', 'Connections');
    gauge.inc({}, 5);
    expect(gauge.get()).toBe(5);
  });

  it('decrements by custom value', () => {
    const gauge = registry.createGauge('connections', 'Connections');
    gauge.set({}, 10);
    gauge.dec({}, 3);
    expect(gauge.get()).toBe(7);
  });

  it('tracks values per label combination', () => {
    const gauge = registry.createGauge('connections', 'Connections', ['shard']);
    gauge.set({ shard: 'a' }, 5);
    gauge.set({ shard: 'b' }, 10);

    expect(gauge.get({ shard: 'a' })).toBe(5);
    expect(gauge.get({ shard: 'b' })).toBe(10);
  });

  it('resets to zero', () => {
    const gauge = registry.createGauge('connections', 'Connections');
    gauge.set({}, 42);
    gauge.reset();
    expect(gauge.get()).toBe(0);
  });

  it('applies prefix from config', () => {
    const gauge = registry.createGauge('active', 'Active');
    expect(gauge.name).toBe('test_active');
  });
});

// =============================================================================
// MetricsRegistry
// =============================================================================

describe('MetricsRegistryImpl', () => {
  it('returns the same counter for the same name', () => {
    const registry = new MetricsRegistryImpl(makeConfig());
    const c1 = registry.createCounter('req', 'Requests');
    const c2 = registry.createCounter('req', 'Requests');

    c1.inc();
    expect(c2.get()).toBe(1);
  });

  it('returns the same histogram for the same name', () => {
    const registry = new MetricsRegistryImpl(makeConfig());
    const h1 = registry.createHistogram('lat', 'Latency', [], [1]);
    const h2 = registry.createHistogram('lat', 'Latency', [], [1]);

    h1.observe({}, 0.5);
    expect(h2.get().count).toBe(1);
  });

  it('returns the same gauge for the same name', () => {
    const registry = new MetricsRegistryImpl(makeConfig());
    const g1 = registry.createGauge('conn', 'Connections');
    const g2 = registry.createGauge('conn', 'Connections');

    g1.set({}, 7);
    expect(g2.get()).toBe(7);
  });

  it('exports Prometheus format', () => {
    const registry = new MetricsRegistryImpl(makeConfig());
    const counter = registry.createCounter('hits', 'Total hits');
    counter.inc({}, 5);

    const output = registry.getMetrics();
    expect(output).toContain('# HELP test_hits Total hits');
    expect(output).toContain('# TYPE test_hits counter');
    expect(output).toContain('test_hits 5');
  });

  it('exports histogram in Prometheus format with buckets', () => {
    const registry = new MetricsRegistryImpl(makeConfig());
    const histogram = registry.createHistogram('latency_seconds', 'Latency', [], [0.1, 0.5]);
    histogram.observe({}, 0.05);

    const output = registry.getMetrics();
    expect(output).toContain('# TYPE test_latency_seconds histogram');
    expect(output).toContain('test_latency_seconds_bucket{le="0.1"} 1');
    expect(output).toContain('test_latency_seconds_count 1');
    expect(output).toContain('test_latency_seconds_sum');
  });

  it('exports gauge in Prometheus format with labels', () => {
    const registry = new MetricsRegistryImpl(makeConfig());
    const gauge = registry.createGauge('connections', 'Active connections', ['shard']);
    gauge.set({ shard: 'primary' }, 10);

    const output = registry.getMetrics();
    expect(output).toContain('test_connections{shard="primary"} 10');
  });

  it('resets all metrics', () => {
    const registry = new MetricsRegistryImpl(makeConfig());
    const counter = registry.createCounter('hits', 'Hits');
    const gauge = registry.createGauge('conn', 'Connections');

    counter.inc({}, 10);
    gauge.set({}, 5);
    registry.reset();

    expect(counter.get()).toBe(0);
    expect(gauge.get()).toBe(0);
  });

  it('works without prefix', () => {
    const registry = new MetricsRegistryImpl(makeConfig({ prefix: '' }));
    const counter = registry.createCounter('raw_counter', 'Raw');
    expect(counter.name).toBe('raw_counter');
  });
});

// =============================================================================
// NoOpMetricsRegistry
// =============================================================================

describe('NoOpMetricsRegistry', () => {
  it('returns no-op counter that does nothing', () => {
    const registry = new NoOpMetricsRegistry();
    const counter = registry.createCounter('x', 'X');
    counter.inc();
    expect(counter.get()).toBe(0);
  });

  it('returns no-op histogram that does nothing', () => {
    const registry = new NoOpMetricsRegistry();
    const histogram = registry.createHistogram('x', 'X');
    histogram.observe({}, 1);
    expect(histogram.get().count).toBe(0);
  });

  it('returns no-op gauge that does nothing', () => {
    const registry = new NoOpMetricsRegistry();
    const gauge = registry.createGauge('x', 'X');
    gauge.set({}, 10);
    expect(gauge.get()).toBe(0);
  });

  it('returns empty string for getMetrics', () => {
    const registry = new NoOpMetricsRegistry();
    expect(registry.getMetrics()).toBe('');
  });
});

// =============================================================================
// createMetricsRegistry factory
// =============================================================================

describe('createMetricsRegistry', () => {
  it('returns MetricsRegistryImpl when enabled', () => {
    const registry = createMetricsRegistry(makeConfig({ enabled: true }));
    expect(registry).toBeInstanceOf(MetricsRegistryImpl);
  });

  it('returns NoOpMetricsRegistry when disabled', () => {
    const registry = createMetricsRegistry(makeConfig({ enabled: false }));
    expect(registry).toBeInstanceOf(NoOpMetricsRegistry);
  });
});
