/**
 * Observability Integration Tests
 *
 * Tests for the createObservability factory, createDoSQLMetrics,
 * instrumentQuery, and instrumentTransaction helpers.
 */

import { describe, it, expect } from 'vitest';
import {
  createObservability,
  createDoSQLMetrics,
  instrumentQuery,
  instrumentTransaction,
} from '../index.js';
import { TracerImpl } from '../tracer.js';
import { MetricsRegistryImpl } from '../metrics.js';
import { NoOpTracer } from '../tracer.js';
import { NoOpMetricsRegistry } from '../metrics.js';

// =============================================================================
// createObservability
// =============================================================================

describe('createObservability', () => {
  it('creates an observability instance with all components', () => {
    const obs = createObservability();

    expect(obs.tracer).toBeDefined();
    expect(obs.metrics).toBeDefined();
    expect(obs.sanitizer).toBeDefined();
    expect(obs.config).toBeDefined();
  });

  it('uses defaults when no config is provided', () => {
    const obs = createObservability();

    expect(obs.config.tracing.enabled).toBe(true);
    expect(obs.config.tracing.serviceName).toBe('dosql');
    expect(obs.config.metrics.enabled).toBe(true);
    expect(obs.config.metrics.prefix).toBe('dosql');
  });

  it('returns TracerImpl when tracing is enabled', () => {
    const obs = createObservability({ tracing: { enabled: true } as any });
    expect(obs.tracer).toBeInstanceOf(TracerImpl);
  });

  it('returns NoOpTracer when tracing is disabled', () => {
    const obs = createObservability({ tracing: { enabled: false } as any });
    expect(obs.tracer).toBeInstanceOf(NoOpTracer);
  });

  it('returns MetricsRegistryImpl when metrics are enabled', () => {
    const obs = createObservability({ metrics: { enabled: true } as any });
    expect(obs.metrics).toBeInstanceOf(MetricsRegistryImpl);
  });

  it('returns NoOpMetricsRegistry when metrics are disabled', () => {
    const obs = createObservability({ metrics: { enabled: false } as any });
    expect(obs.metrics).toBeInstanceOf(NoOpMetricsRegistry);
  });

  it('merges custom config with defaults', () => {
    const obs = createObservability({
      tracing: { serviceName: 'custom-service' } as any,
    });

    expect(obs.config.tracing.serviceName).toBe('custom-service');
    expect(obs.config.tracing.enabled).toBe(true); // from defaults
  });
});

// =============================================================================
// createDoSQLMetrics
// =============================================================================

describe('createDoSQLMetrics', () => {
  it('creates all standard DoSQL metrics', () => {
    const obs = createObservability();
    const metrics = createDoSQLMetrics(obs.metrics);

    expect(metrics.queryTotal).toBeDefined();
    expect(metrics.queryDuration).toBeDefined();
    expect(metrics.queryErrors).toBeDefined();
    expect(metrics.activeConnections).toBeDefined();
    expect(metrics.transactionsTotal).toBeDefined();
    expect(metrics.transactionDuration).toBeDefined();
    expect(metrics.walWrites).toBeDefined();
    expect(metrics.walSize).toBeDefined();
    expect(metrics.walCheckpoints).toBeDefined();
    expect(metrics.cdcEventsTotal).toBeDefined();
    expect(metrics.cdcLag).toBeDefined();
  });

  it('metrics are functional and record values', () => {
    const obs = createObservability();
    const metrics = createDoSQLMetrics(obs.metrics);

    metrics.queryTotal.inc({ operation: 'SELECT', table: 'users', status: 'success' });
    expect(metrics.queryTotal.get({ operation: 'SELECT', table: 'users', status: 'success' })).toBe(1);

    metrics.activeConnections.set({ shard: 'primary' }, 5);
    expect(metrics.activeConnections.get({ shard: 'primary' })).toBe(5);
  });
});

// =============================================================================
// instrumentQuery
// =============================================================================

describe('instrumentQuery', () => {
  it('returns the result of the execute function on success', async () => {
    const obs = createObservability();
    const metrics = createDoSQLMetrics(obs.metrics);

    const result = await instrumentQuery(
      obs,
      metrics,
      'SELECT * FROM users',
      undefined,
      async () => [{ id: 1 }]
    );

    expect(result).toEqual([{ id: 1 }]);
  });

  it('increments query counter with success status', async () => {
    const obs = createObservability();
    const metrics = createDoSQLMetrics(obs.metrics);

    await instrumentQuery(obs, metrics, 'SELECT * FROM users', undefined, async () => []);

    expect(metrics.queryTotal.get({ operation: 'SELECT', table: 'users', status: 'success' })).toBe(1);
  });

  it('records query duration', async () => {
    const obs = createObservability();
    const metrics = createDoSQLMetrics(obs.metrics);

    await instrumentQuery(obs, metrics, 'SELECT * FROM users', undefined, async () => []);

    const histValue = metrics.queryDuration.get({ operation: 'SELECT', table: 'users' });
    expect(histValue.count).toBe(1);
    expect(histValue.sum).toBeGreaterThanOrEqual(0);
  });

  it('re-throws errors from execute', async () => {
    const obs = createObservability();
    const metrics = createDoSQLMetrics(obs.metrics);

    await expect(
      instrumentQuery(obs, metrics, 'SELECT * FROM users', undefined, async () => {
        throw new Error('query failed');
      })
    ).rejects.toThrow('query failed');
  });

  it('increments error counter on failure', async () => {
    const obs = createObservability();
    const metrics = createDoSQLMetrics(obs.metrics);

    try {
      await instrumentQuery(obs, metrics, 'SELECT * FROM users', undefined, async () => {
        throw new Error('query failed');
      });
    } catch {
      // expected
    }

    expect(metrics.queryTotal.get({ operation: 'SELECT', table: 'users', status: 'error' })).toBe(1);
    expect(metrics.queryErrors.get({ operation: 'SELECT', error_type: 'Error' })).toBe(1);
  });

  it('detects statement type and table from SQL', async () => {
    const obs = createObservability();
    const metrics = createDoSQLMetrics(obs.metrics);

    await instrumentQuery(obs, metrics, 'INSERT INTO orders (id) VALUES (1)', undefined, async () => ({}));

    expect(metrics.queryTotal.get({ operation: 'INSERT', table: 'orders', status: 'success' })).toBe(1);
  });
});

// =============================================================================
// instrumentTransaction
// =============================================================================

describe('instrumentTransaction', () => {
  it('returns the result on successful transaction', async () => {
    const obs = createObservability();
    const metrics = createDoSQLMetrics(obs.metrics);

    const result = await instrumentTransaction(obs, metrics, async () => 'committed');

    expect(result).toBe('committed');
  });

  it('increments commit counter on success', async () => {
    const obs = createObservability();
    const metrics = createDoSQLMetrics(obs.metrics);

    await instrumentTransaction(obs, metrics, async () => null);

    expect(metrics.transactionsTotal.get({ outcome: 'commit' })).toBe(1);
  });

  it('records transaction duration on success', async () => {
    const obs = createObservability();
    const metrics = createDoSQLMetrics(obs.metrics);

    await instrumentTransaction(obs, metrics, async () => null);

    const histValue = metrics.transactionDuration.get({ outcome: 'commit' });
    expect(histValue.count).toBe(1);
  });

  it('re-throws errors on transaction failure', async () => {
    const obs = createObservability();
    const metrics = createDoSQLMetrics(obs.metrics);

    await expect(
      instrumentTransaction(obs, metrics, async () => {
        throw new Error('tx failed');
      })
    ).rejects.toThrow('tx failed');
  });

  it('increments rollback counter on failure', async () => {
    const obs = createObservability();
    const metrics = createDoSQLMetrics(obs.metrics);

    try {
      await instrumentTransaction(obs, metrics, async () => {
        throw new Error('tx failed');
      });
    } catch {
      // expected
    }

    expect(metrics.transactionsTotal.get({ outcome: 'rollback' })).toBe(1);
  });

  it('records transaction duration on failure', async () => {
    const obs = createObservability();
    const metrics = createDoSQLMetrics(obs.metrics);

    try {
      await instrumentTransaction(obs, metrics, async () => {
        throw new Error('tx failed');
      });
    } catch {
      // expected
    }

    const histValue = metrics.transactionDuration.get({ outcome: 'rollback' });
    expect(histValue.count).toBe(1);
  });
});
