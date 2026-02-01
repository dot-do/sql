/**
 * TDD Tests: Wire StorageConfig through FSX tiered backend
 *
 * Verifies that TieredStorageBackend respects per-table (per-path-prefix)
 * StorageConfig overrides for hotStorageMaxSize, hotDataMaxAge, and maxHotFileSize.
 *
 * Config inheritance: DEFAULT_TIERED_CONFIG -> constructor config -> per-table overrides
 */

import { describe, it, expect, beforeEach } from 'vitest';

import {
  TieredStorageBackend,
  createTieredBackend,
  DOStorageBackend,
  R2StorageBackend,
  createR2Backend,
  MemoryFSXBackend,
  createMemoryBackend,
  StorageTier,
  DEFAULT_TIERED_CONFIG,
  type TieredStorageConfig,
  type R2BucketLike,
} from '../index.js';

import { env } from 'cloudflare:test';

// =============================================================================
// Helpers
// =============================================================================

function generateTestData(size: number, seed = 0): Uint8Array {
  const data = new Uint8Array(size);
  for (let i = 0; i < size; i++) {
    data[i] = (i + seed) % 256;
  }
  return data;
}

function createHotBackendWithStats(): MemoryFSXBackend & {
  getStats: () => Promise<{ fileCount: number; totalSize: number; chunkedFileCount: number }>;
} {
  const backend = createMemoryBackend();
  const hotWithStats = backend as MemoryFSXBackend & {
    getStats: () => Promise<{ fileCount: number; totalSize: number; chunkedFileCount: number }>;
  };

  hotWithStats.getStats = async () => {
    let totalSize = 0;
    for (const path of backend.paths()) {
      const data = await backend.read(path);
      if (data) totalSize += data.length;
    }
    return {
      fileCount: backend.size,
      totalSize,
      chunkedFileCount: 0,
    };
  };

  return hotWithStats;
}

// =============================================================================
// Tests
// =============================================================================

describe('TieredStorageBackend - Per-Table StorageConfig', () => {
  let hotBackend: ReturnType<typeof createHotBackendWithStats>;
  let coldBackend: R2StorageBackend;

  beforeEach(() => {
    hotBackend = createHotBackendWithStats();
    coldBackend = createR2Backend(env.TEST_R2_BUCKET as R2BucketLike, {
      keyPrefix: 'tiered-config-test',
    });
  });

  // ===========================================================================
  // Backward compatibility: no per-table config uses defaults
  // ===========================================================================

  describe('backward compatibility (no per-table config)', () => {
    it('should use constructor config when no per-table overrides', async () => {
      const tiered = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        {
          maxHotFileSize: 5 * 1024, // 5KB
          autoMigrate: false,
        },
      );

      // A 6KB file should go to cold since maxHotFileSize = 5KB
      const data = generateTestData(6 * 1024);
      await tiered.write('sometable/data.bin', data);

      // Should be in cold tier (file exceeds maxHotFileSize)
      const meta = await tiered.metadata('sometable/data.bin');
      expect(meta?.tier).toBe(StorageTier.COLD);
    });

    it('should use DEFAULT_TIERED_CONFIG when no config provided', async () => {
      const tiered = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        { autoMigrate: false },
      );

      // A 1KB file should go to hot (default maxHotFileSize is 10MB)
      const data = generateTestData(1024);
      await tiered.write('anytable/small.bin', data);

      const meta = await tiered.metadata('anytable/small.bin');
      expect(meta?.tier).toBe(StorageTier.HOT);
    });
  });

  // ===========================================================================
  // Per-table maxHotFileSize override
  // ===========================================================================

  describe('per-table maxHotFileSize override', () => {
    it('should use per-table maxHotFileSize for matching path prefix', async () => {
      const tiered = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        {
          maxHotFileSize: 10 * 1024, // 10KB default
          autoMigrate: false,
        },
      );

      // Set a per-table override: "logs/" table has maxHotFileSize of 2KB
      tiered.setTableConfig('logs/', { maxHotFileSize: 2 * 1024 });

      // A 3KB file under logs/ should go to cold (exceeds table limit of 2KB)
      const logsData = generateTestData(3 * 1024);
      await tiered.write('logs/entry1.bin', logsData);
      const logsMeta = await tiered.metadata('logs/entry1.bin');
      expect(logsMeta?.tier).toBe(StorageTier.COLD);

      // A 3KB file under other/ should go to hot (within default 10KB limit)
      const otherData = generateTestData(3 * 1024);
      await tiered.write('other/entry1.bin', otherData);
      const otherMeta = await tiered.metadata('other/entry1.bin');
      expect(otherMeta?.tier).toBe(StorageTier.HOT);
    });

    it('should use per-table maxHotFileSize when explicitly writing to HOT', async () => {
      const tiered = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        {
          maxHotFileSize: 10 * 1024,
          autoMigrate: false,
        },
      );

      tiered.setTableConfig('logs/', { maxHotFileSize: 2 * 1024 });

      // Writing 3KB to hot for logs/ should throw (exceeds table limit)
      const data = generateTestData(3 * 1024);
      await expect(
        tiered.writeWithTier('logs/entry.bin', data, { tier: StorageTier.HOT }),
      ).rejects.toThrow(/exceeds maxHotFileSize/);
    });
  });

  // ===========================================================================
  // Per-table hotStorageMaxSize override
  // ===========================================================================

  describe('per-table hotStorageMaxSize override', () => {
    it('should use per-table hotStorageMaxSize for caching decisions', async () => {
      const tiered = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        {
          hotStorageMaxSize: 100 * 1024, // 100KB global
          cacheR2Reads: true,
          autoMigrate: false,
        },
      );

      // Table "events/" has a much smaller hot storage max
      tiered.setTableConfig('events/', { hotStorageMaxSize: 1 * 1024 }); // 1KB

      // Write a 500B file directly to cold tier
      const data = generateTestData(500);
      await tiered.writeWithTier('events/evt1.bin', data, { tier: StorageTier.COLD });

      // The config for "events/" should report 1KB hotStorageMaxSize
      const resolved = tiered.getConfigForPath('events/evt1.bin');
      expect(resolved.hotStorageMaxSize).toBe(1 * 1024);
    });
  });

  // ===========================================================================
  // Per-table hotDataMaxAge override
  // ===========================================================================

  describe('per-table hotDataMaxAge override', () => {
    it('should resolve per-table hotDataMaxAge config', () => {
      const tiered = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        {
          hotDataMaxAge: 60 * 60 * 1000, // 1 hour default
          autoMigrate: false,
        },
      );

      // "metrics/" table gets a shorter max age
      tiered.setTableConfig('metrics/', { hotDataMaxAge: 5 * 60 * 1000 }); // 5 minutes

      const metricsConfig = tiered.getConfigForPath('metrics/data.bin');
      expect(metricsConfig.hotDataMaxAge).toBe(5 * 60 * 1000);

      // Unmatched paths use default
      const defaultConfig = tiered.getConfigForPath('other/data.bin');
      expect(defaultConfig.hotDataMaxAge).toBe(60 * 60 * 1000);
    });
  });

  // ===========================================================================
  // Config merging: default -> constructor -> per-table
  // ===========================================================================

  describe('config merging (default -> constructor -> per-table)', () => {
    it('should merge per-table config over constructor config over defaults', () => {
      const tiered = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        {
          maxHotFileSize: 5 * 1024 * 1024,   // 5MB (overrides default 10MB)
          hotDataMaxAge: 30 * 60 * 1000,       // 30 min (overrides default 1h)
          autoMigrate: false,
        },
      );

      // Table override only sets maxHotFileSize
      tiered.setTableConfig('archive/', { maxHotFileSize: 1 * 1024 * 1024 }); // 1MB

      const archiveConfig = tiered.getConfigForPath('archive/file.bin');
      // maxHotFileSize: from table override
      expect(archiveConfig.maxHotFileSize).toBe(1 * 1024 * 1024);
      // hotDataMaxAge: from constructor config (table didn't override)
      expect(archiveConfig.hotDataMaxAge).toBe(30 * 60 * 1000);
      // hotStorageMaxSize: from DEFAULT_TIERED_CONFIG (neither constructor nor table overrode)
      expect(archiveConfig.hotStorageMaxSize).toBe(DEFAULT_TIERED_CONFIG.hotStorageMaxSize);
    });

    it('should allow multiple tables with different configs', () => {
      const tiered = createTieredBackend(
        hotBackend as unknown as DOStorageBackend,
        coldBackend,
        { autoMigrate: false },
      );

      tiered.setTableConfig('logs/', { maxHotFileSize: 1024 });
      tiered.setTableConfig('metrics/', { maxHotFileSize: 2048, hotDataMaxAge: 1000 });
      tiered.setTableConfig('events/', { hotStorageMaxSize: 50 * 1024 * 1024 });

      expect(tiered.getConfigForPath('logs/file.bin').maxHotFileSize).toBe(1024);
      expect(tiered.getConfigForPath('metrics/file.bin').maxHotFileSize).toBe(2048);
      expect(tiered.getConfigForPath('metrics/file.bin').hotDataMaxAge).toBe(1000);
      expect(tiered.getConfigForPath('events/file.bin').hotStorageMaxSize).toBe(50 * 1024 * 1024);

      // Defaults still work for unmatched
      expect(tiered.getConfigForPath('unknown/file.bin').maxHotFileSize).toBe(DEFAULT_TIERED_CONFIG.maxHotFileSize);
    });
  });
});
