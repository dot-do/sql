/**
 * Cache Invalidation Tests for DoLake Module
 *
 * Tests for the query result cache invalidation logic based on CDC events.
 * Uses real implementations (no mocks) per project philosophy.
 *
 * @module dolake/__tests__/cache-invalidation
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  CacheInvalidator,
  type CacheInvalidatorConfig,
  type CDCEvent,
} from '../cache-invalidation.js';

// =============================================================================
// Test Utilities
// =============================================================================

/**
 * Create a mock CDC event for testing
 */
function createCDCEvent(
  operation: CDCEvent['operation'],
  table: string,
  overrides: Partial<CDCEvent> = {}
): CDCEvent {
  return {
    operation,
    table,
    before: operation === 'DELETE' ? { id: 1 } : undefined,
    after: operation !== 'DELETE' ? { id: 1 } : undefined,
    timestamp: Date.now(),
    sequence: Math.floor(Math.random() * 10000),
    rowId: `row-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    ...overrides,
  };
}

// =============================================================================
// Test Suite: CacheInvalidator Class
// =============================================================================

describe('CacheInvalidator', () => {
  describe('Configuration', () => {
    it('should accept configuration options', () => {
      const config: CacheInvalidatorConfig = {
        enabled: true,
        ttlMs: 60_000,
      };

      const invalidator = new CacheInvalidator(config);
      expect(invalidator).toBeDefined();
    });

    it('should work with disabled configuration', () => {
      const config: CacheInvalidatorConfig = {
        enabled: false,
        ttlMs: 0,
      };

      const invalidator = new CacheInvalidator(config);
      expect(invalidator).toBeDefined();
    });
  });

  describe('Cache Entry Registration', () => {
    let invalidator: CacheInvalidator;

    beforeEach(() => {
      invalidator = new CacheInvalidator({ enabled: true, ttlMs: 60_000 });
    });

    it('should register a cache entry for a table', () => {
      invalidator.registerCacheEntry('users');

      const status = invalidator.getCacheEntryStatus('users');
      expect(status).toBeDefined();
      expect(status?.tableId).toBe('users');
      expect(status?.cached).toBe(true);
      expect(status?.lastInvalidatedAt).toBeNull();
      expect(status?.invalidationCount).toBe(0);
    });

    it('should register multiple cache entries', () => {
      invalidator.registerCacheEntry('users');
      invalidator.registerCacheEntry('orders');
      invalidator.registerCacheEntry('products');

      expect(invalidator.getCacheEntryStatus('users')).toBeDefined();
      expect(invalidator.getCacheEntryStatus('orders')).toBeDefined();
      expect(invalidator.getCacheEntryStatus('products')).toBeDefined();
    });

    it('should return null for unregistered tables', () => {
      const status = invalidator.getCacheEntryStatus('nonexistent');
      expect(status).toBeNull();
    });

    it('should overwrite existing entry on re-registration', () => {
      invalidator.registerCacheEntry('users');
      invalidator.invalidate('users');
      const statusAfterInvalidation = invalidator.getCacheEntryStatus('users');
      expect(statusAfterInvalidation?.invalidationCount).toBe(1);

      // Re-register
      invalidator.registerCacheEntry('users');
      const statusAfterReRegister = invalidator.getCacheEntryStatus('users');
      expect(statusAfterReRegister?.invalidationCount).toBe(0);
      expect(statusAfterReRegister?.cached).toBe(true);
    });
  });

  describe('Manual Invalidation', () => {
    let invalidator: CacheInvalidator;

    beforeEach(() => {
      invalidator = new CacheInvalidator({ enabled: true, ttlMs: 60_000 });
    });

    it('should invalidate a registered cache entry', () => {
      invalidator.registerCacheEntry('users');

      invalidator.invalidate('users');

      const status = invalidator.getCacheEntryStatus('users');
      expect(status?.cached).toBe(false);
      expect(status?.lastInvalidatedAt).not.toBeNull();
      expect(status?.invalidationCount).toBe(1);
    });

    it('should track multiple invalidations', () => {
      invalidator.registerCacheEntry('users');

      invalidator.invalidate('users');
      invalidator.invalidate('users');
      invalidator.invalidate('users');

      const status = invalidator.getCacheEntryStatus('users');
      expect(status?.invalidationCount).toBe(3);
    });

    it('should handle invalidation of unregistered table', () => {
      // Should not throw
      invalidator.invalidate('nonexistent');

      const status = invalidator.getCacheEntryStatus('nonexistent');
      expect(status).toBeNull();
    });

    it('should set lastInvalidatedAt timestamp', () => {
      invalidator.registerCacheEntry('users');

      const beforeTime = Date.now();
      invalidator.invalidate('users');
      const afterTime = Date.now();

      const status = invalidator.getCacheEntryStatus('users');
      expect(status?.lastInvalidatedAt).toBeGreaterThanOrEqual(beforeTime);
      expect(status?.lastInvalidatedAt).toBeLessThanOrEqual(afterTime);
    });
  });

  describe('markValid', () => {
    let invalidator: CacheInvalidator;

    beforeEach(() => {
      invalidator = new CacheInvalidator({ enabled: true, ttlMs: 60_000 });
    });

    it('should mark an invalidated entry as valid', () => {
      invalidator.registerCacheEntry('users');
      invalidator.invalidate('users');

      expect(invalidator.getCacheEntryStatus('users')?.cached).toBe(false);

      invalidator.markValid('users');

      expect(invalidator.getCacheEntryStatus('users')?.cached).toBe(true);
    });

    it('should preserve invalidation count when marked valid', () => {
      invalidator.registerCacheEntry('users');
      invalidator.invalidate('users');
      invalidator.invalidate('users');

      invalidator.markValid('users');

      const status = invalidator.getCacheEntryStatus('users');
      expect(status?.cached).toBe(true);
      expect(status?.invalidationCount).toBe(2);
    });

    it('should handle marking unregistered entry as valid', () => {
      // Should not throw
      invalidator.markValid('nonexistent');

      const status = invalidator.getCacheEntryStatus('nonexistent');
      expect(status).toBeNull();
    });
  });

  describe('CDC Event Processing', () => {
    let invalidator: CacheInvalidator;

    beforeEach(() => {
      invalidator = new CacheInvalidator({ enabled: true, ttlMs: 60_000 });
      invalidator.registerCacheEntry('users');
      invalidator.registerCacheEntry('orders');
    });

    it('should invalidate cache on INSERT event', async () => {
      const event = createCDCEvent('INSERT', 'users');

      await invalidator.processCDCEvents([event]);

      const status = invalidator.getCacheEntryStatus('users');
      expect(status?.cached).toBe(false);
      expect(status?.invalidationCount).toBe(1);
    });

    it('should invalidate cache on UPDATE event', async () => {
      const event = createCDCEvent('UPDATE', 'users', {
        before: { id: 1, name: 'Alice' },
        after: { id: 1, name: 'Bob' },
      });

      await invalidator.processCDCEvents([event]);

      const status = invalidator.getCacheEntryStatus('users');
      expect(status?.cached).toBe(false);
    });

    it('should invalidate cache on DELETE event', async () => {
      const event = createCDCEvent('DELETE', 'users', {
        before: { id: 1 },
      });

      await invalidator.processCDCEvents([event]);

      const status = invalidator.getCacheEntryStatus('users');
      expect(status?.cached).toBe(false);
    });

    it('should process multiple events', async () => {
      const events: CDCEvent[] = [
        createCDCEvent('INSERT', 'users'),
        createCDCEvent('UPDATE', 'orders'),
      ];

      await invalidator.processCDCEvents(events);

      expect(invalidator.getCacheEntryStatus('users')?.cached).toBe(false);
      expect(invalidator.getCacheEntryStatus('orders')?.cached).toBe(false);
    });

    it('should only invalidate registered tables', async () => {
      const events: CDCEvent[] = [
        createCDCEvent('INSERT', 'users'),
        createCDCEvent('INSERT', 'unregistered_table'),
      ];

      await invalidator.processCDCEvents(events);

      expect(invalidator.getCacheEntryStatus('users')?.cached).toBe(false);
      expect(invalidator.getCacheEntryStatus('unregistered_table')).toBeNull();
    });

    it('should handle empty event list', async () => {
      await invalidator.processCDCEvents([]);

      // Should not throw and cache should remain valid
      expect(invalidator.getCacheEntryStatus('users')?.cached).toBe(true);
    });

    it('should accumulate invalidation count from CDC events', async () => {
      await invalidator.processCDCEvents([createCDCEvent('INSERT', 'users')]);
      await invalidator.processCDCEvents([createCDCEvent('UPDATE', 'users')]);
      await invalidator.processCDCEvents([createCDCEvent('DELETE', 'users')]);

      const status = invalidator.getCacheEntryStatus('users');
      expect(status?.invalidationCount).toBe(3);
    });
  });

  describe('Clear All Entries', () => {
    let invalidator: CacheInvalidator;

    beforeEach(() => {
      invalidator = new CacheInvalidator({ enabled: true, ttlMs: 60_000 });
      invalidator.registerCacheEntry('users');
      invalidator.registerCacheEntry('orders');
      invalidator.registerCacheEntry('products');
    });

    it('should clear all registered entries', () => {
      invalidator.clear();

      expect(invalidator.getCacheEntryStatus('users')).toBeNull();
      expect(invalidator.getCacheEntryStatus('orders')).toBeNull();
      expect(invalidator.getCacheEntryStatus('products')).toBeNull();
    });

    it('should allow re-registration after clear', () => {
      invalidator.clear();

      invalidator.registerCacheEntry('users');

      expect(invalidator.getCacheEntryStatus('users')).toBeDefined();
      expect(invalidator.getCacheEntryStatus('users')?.cached).toBe(true);
    });
  });

  describe('Concurrent Access Patterns', () => {
    let invalidator: CacheInvalidator;

    beforeEach(() => {
      invalidator = new CacheInvalidator({ enabled: true, ttlMs: 60_000 });
    });

    it('should handle concurrent registrations', async () => {
      const tables = ['t1', 't2', 't3', 't4', 't5'];

      await Promise.all(
        tables.map((t) => {
          invalidator.registerCacheEntry(t);
          return Promise.resolve();
        })
      );

      for (const t of tables) {
        expect(invalidator.getCacheEntryStatus(t)).toBeDefined();
      }
    });

    it('should handle concurrent invalidations', async () => {
      invalidator.registerCacheEntry('users');

      await Promise.all(
        Array.from({ length: 10 }).map(() => {
          invalidator.invalidate('users');
          return Promise.resolve();
        })
      );

      const status = invalidator.getCacheEntryStatus('users');
      expect(status?.invalidationCount).toBe(10);
    });

    it('should handle concurrent CDC event processing', async () => {
      const tables = ['t1', 't2', 't3', 't4', 't5'];
      tables.forEach((t) => invalidator.registerCacheEntry(t));

      await Promise.all(
        tables.map((t) => invalidator.processCDCEvents([createCDCEvent('INSERT', t)]))
      );

      for (const t of tables) {
        expect(invalidator.getCacheEntryStatus(t)?.cached).toBe(false);
      }
    });

    it('should handle interleaved register and invalidate', async () => {
      const operations = [
        () => invalidator.registerCacheEntry('users'),
        () => invalidator.invalidate('users'),
        () => invalidator.markValid('users'),
        () => invalidator.invalidate('users'),
      ];

      await Promise.all(operations.map((op) => Promise.resolve().then(op)));

      // After all operations, status should be consistent
      const status = invalidator.getCacheEntryStatus('users');
      expect(status).toBeDefined();
    });
  });

  describe('Edge Cases', () => {
    let invalidator: CacheInvalidator;

    beforeEach(() => {
      invalidator = new CacheInvalidator({ enabled: true, ttlMs: 60_000 });
    });

    it('should handle table names with special characters', () => {
      const specialNames = ['table-with-dash', 'table_with_underscore', 'Table.With.Dots'];

      for (const name of specialNames) {
        invalidator.registerCacheEntry(name);
        expect(invalidator.getCacheEntryStatus(name)?.cached).toBe(true);

        invalidator.invalidate(name);
        expect(invalidator.getCacheEntryStatus(name)?.cached).toBe(false);
      }
    });

    it('should handle empty table name', () => {
      invalidator.registerCacheEntry('');
      expect(invalidator.getCacheEntryStatus('')?.cached).toBe(true);
    });

    it('should handle very long table names', () => {
      const longName = 'a'.repeat(1000);
      invalidator.registerCacheEntry(longName);
      expect(invalidator.getCacheEntryStatus(longName)?.cached).toBe(true);
    });

    it('should handle rapid invalidation cycles', async () => {
      invalidator.registerCacheEntry('users');

      for (let i = 0; i < 100; i++) {
        invalidator.invalidate('users');
        invalidator.markValid('users');
      }

      const status = invalidator.getCacheEntryStatus('users');
      expect(status?.invalidationCount).toBe(100);
      expect(status?.cached).toBe(true);
    });
  });
});
