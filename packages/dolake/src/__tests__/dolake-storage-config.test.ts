/**
 * DoLake Storage Config Tests (TDD)
 *
 * Tests that DoLake respects per-table StorageConfig from DoSQL,
 * particularly the `parquetFileSize` setting.
 *
 * Issue: sql-zo55 - Wire StorageConfig through DoLake Parquet writer
 */

import { describe, it, expect } from 'vitest';
import {
  type ScalingConfig,
  DEFAULT_SCALING_CONFIG,
} from '../scalability.js';
import {
  getMaxParquetFileSize,
  applyTableStorageConfig,
  type TableStorageOverrides,
} from '../storage-config-bridge.js';

// =============================================================================
// Default parquetFileSize behavior
// =============================================================================

describe('DoLake StorageConfig integration', () => {
  describe('default parquetFileSize', () => {
    it('uses 512MB as the global default maxParquetFileSize', () => {
      const defaultSize = DEFAULT_SCALING_CONFIG.maxParquetFileSize;
      expect(defaultSize).toBe(512 * 1024 * 1024);
    });

    it('returns global default when no per-table config is set', () => {
      const size = getMaxParquetFileSize('users');
      expect(size).toBe(512 * 1024 * 1024);
    });

    it('returns global default for unknown tables', () => {
      const size = getMaxParquetFileSize('nonexistent_table');
      expect(size).toBe(512 * 1024 * 1024);
    });
  });

  // ===========================================================================
  // Per-table parquetFileSize overrides
  // ===========================================================================

  describe('per-table parquetFileSize overrides', () => {
    it('respects per-table parquetFileSize setting', () => {
      const overrides: TableStorageOverrides = {
        tables: {
          events: { parquetFileSize: 256 * 1024 * 1024 }, // 256MB
        },
      };

      const size = getMaxParquetFileSize('events', overrides);
      expect(size).toBe(256 * 1024 * 1024);
    });

    it('different tables can have different parquetFileSize', () => {
      const overrides: TableStorageOverrides = {
        tables: {
          logs: { parquetFileSize: 128 * 1024 * 1024 },     // 128MB
          metrics: { parquetFileSize: 1024 * 1024 * 1024 },  // 1GB
        },
      };

      expect(getMaxParquetFileSize('logs', overrides)).toBe(128 * 1024 * 1024);
      expect(getMaxParquetFileSize('metrics', overrides)).toBe(1024 * 1024 * 1024);
    });

    it('falls back to global default for tables without override', () => {
      const overrides: TableStorageOverrides = {
        tables: {
          events: { parquetFileSize: 256 * 1024 * 1024 },
        },
      };

      // 'users' has no override, should get the default
      expect(getMaxParquetFileSize('users', overrides)).toBe(512 * 1024 * 1024);
    });

    it('supports a global override that applies to all tables', () => {
      const overrides: TableStorageOverrides = {
        globalParquetFileSize: 64 * 1024 * 1024, // 64MB global
        tables: {},
      };

      expect(getMaxParquetFileSize('any_table', overrides)).toBe(64 * 1024 * 1024);
    });

    it('per-table config overrides global config', () => {
      const overrides: TableStorageOverrides = {
        globalParquetFileSize: 64 * 1024 * 1024, // 64MB global
        tables: {
          events: { parquetFileSize: 256 * 1024 * 1024 }, // 256MB per-table
        },
      };

      // Per-table override wins over global override
      expect(getMaxParquetFileSize('events', overrides)).toBe(256 * 1024 * 1024);
      // Other tables get the global override
      expect(getMaxParquetFileSize('users', overrides)).toBe(64 * 1024 * 1024);
    });
  });

  // ===========================================================================
  // ScalingConfig merge with per-table overrides
  // ===========================================================================

  describe('ScalingConfig merge with per-table overrides', () => {
    it('applies per-table parquetFileSize to ScalingConfig', () => {
      const baseConfig: ScalingConfig = { ...DEFAULT_SCALING_CONFIG };
      const overrides: TableStorageOverrides = {
        tables: {
          events: { parquetFileSize: 256 * 1024 * 1024 },
        },
      };

      const merged = applyTableStorageConfig(baseConfig, 'events', overrides);
      expect(merged.maxParquetFileSize).toBe(256 * 1024 * 1024);
    });

    it('preserves other ScalingConfig fields when applying overrides', () => {
      const baseConfig: ScalingConfig = {
        ...DEFAULT_SCALING_CONFIG,
        maxParallelWriters: 8,
        scalingMode: 'auto',
      };
      const overrides: TableStorageOverrides = {
        tables: {
          events: { parquetFileSize: 256 * 1024 * 1024 },
        },
      };

      const merged = applyTableStorageConfig(baseConfig, 'events', overrides);

      // parquetFileSize is overridden
      expect(merged.maxParquetFileSize).toBe(256 * 1024 * 1024);
      // Other fields are preserved
      expect(merged.maxParallelWriters).toBe(8);
      expect(merged.scalingMode).toBe('auto');
    });

    it('returns base config unchanged when table has no override', () => {
      const baseConfig: ScalingConfig = { ...DEFAULT_SCALING_CONFIG };
      const overrides: TableStorageOverrides = {
        tables: {
          events: { parquetFileSize: 256 * 1024 * 1024 },
        },
      };

      const merged = applyTableStorageConfig(baseConfig, 'users', overrides);
      expect(merged.maxParquetFileSize).toBe(DEFAULT_SCALING_CONFIG.maxParquetFileSize);
    });

    it('returns base config unchanged when no overrides provided', () => {
      const baseConfig: ScalingConfig = { ...DEFAULT_SCALING_CONFIG };

      const merged = applyTableStorageConfig(baseConfig, 'events');
      expect(merged.maxParquetFileSize).toBe(DEFAULT_SCALING_CONFIG.maxParquetFileSize);
    });
  });
});
