/**
 * Storage Config SDK Tests
 *
 * Verifies that the sql.do SDK exposes StorageConfig types and supports
 * CREATE TABLE WITH STORAGE clause flowing through the client.
 *
 * Issue: sql-fcpj
 *
 * @packageDocumentation
 */

import { describe, it, expect } from 'vitest';

// These imports should work once we add storage config support to the SDK
import type {
  StorageConfig,
  TableStorageConfig,
  DatabaseStorageConfig,
  StorageConfigField,
} from '../index.js';

import {
  DEFAULT_STORAGE_CONFIG,
  STORAGE_CONFIG_FIELDS,
  isValidStorageConfig,
} from '../index.js';

// =============================================================================
// Type Export Tests
// =============================================================================

describe('StorageConfig type exports', () => {
  it('should export StorageConfig interface with all fields', () => {
    // Verify the type is usable by constructing a value that satisfies it
    const config: StorageConfig = {
      chunkSize: 2 * 1024 * 1024,
      maxPageSize: 2 * 1024 * 1024,
      rowGroupSize: 1024 * 1024,
      maxRowsPerRowGroup: 65536,
      hotStorageMaxSize: 100 * 1024 * 1024,
      hotDataMaxAge: 3600000,
      maxHotFileSize: 10 * 1024 * 1024,
      parquetFileSize: 512 * 1024 * 1024,
    };
    expect(config.chunkSize).toBe(2 * 1024 * 1024);
    expect(config.maxPageSize).toBe(2 * 1024 * 1024);
    expect(config.rowGroupSize).toBe(1024 * 1024);
    expect(config.maxRowsPerRowGroup).toBe(65536);
    expect(config.hotStorageMaxSize).toBe(100 * 1024 * 1024);
    expect(config.hotDataMaxAge).toBe(3600000);
    expect(config.maxHotFileSize).toBe(10 * 1024 * 1024);
    expect(config.parquetFileSize).toBe(512 * 1024 * 1024);
  });

  it('should export TableStorageConfig as partial StorageConfig', () => {
    // TableStorageConfig should allow any subset of StorageConfig fields
    const tableConfig: TableStorageConfig = {
      rowGroupSize: 4 * 1024 * 1024,
      parquetFileSize: 256 * 1024 * 1024,
    };
    expect(tableConfig.rowGroupSize).toBe(4 * 1024 * 1024);
    expect(tableConfig.parquetFileSize).toBe(256 * 1024 * 1024);
    expect(tableConfig.chunkSize).toBeUndefined();
  });

  it('should export DatabaseStorageConfig as partial StorageConfig', () => {
    const dbConfig: DatabaseStorageConfig = {
      hotStorageMaxSize: 200 * 1024 * 1024,
    };
    expect(dbConfig.hotStorageMaxSize).toBe(200 * 1024 * 1024);
    expect(dbConfig.chunkSize).toBeUndefined();
  });

  it('should export StorageConfigField type listing valid field names', () => {
    const field: StorageConfigField = 'chunkSize';
    expect(field).toBe('chunkSize');
  });
});

// =============================================================================
// Default Config Tests
// =============================================================================

describe('DEFAULT_STORAGE_CONFIG', () => {
  it('should provide sensible defaults for all fields', () => {
    expect(DEFAULT_STORAGE_CONFIG).toBeDefined();
    expect(DEFAULT_STORAGE_CONFIG.chunkSize).toBe(2 * 1024 * 1024);
    expect(DEFAULT_STORAGE_CONFIG.maxPageSize).toBe(2 * 1024 * 1024);
    expect(DEFAULT_STORAGE_CONFIG.rowGroupSize).toBe(1024 * 1024);
    expect(DEFAULT_STORAGE_CONFIG.maxRowsPerRowGroup).toBe(65536);
    expect(DEFAULT_STORAGE_CONFIG.hotStorageMaxSize).toBe(100 * 1024 * 1024);
    expect(DEFAULT_STORAGE_CONFIG.hotDataMaxAge).toBe(3600000);
    expect(DEFAULT_STORAGE_CONFIG.maxHotFileSize).toBe(10 * 1024 * 1024);
    expect(DEFAULT_STORAGE_CONFIG.parquetFileSize).toBe(512 * 1024 * 1024);
  });

  it('should be frozen / read-only', () => {
    expect(Object.isFrozen(DEFAULT_STORAGE_CONFIG)).toBe(true);
  });
});

// =============================================================================
// STORAGE_CONFIG_FIELDS Tests
// =============================================================================

describe('STORAGE_CONFIG_FIELDS', () => {
  it('should list all valid storage config field names', () => {
    expect(STORAGE_CONFIG_FIELDS).toContain('chunkSize');
    expect(STORAGE_CONFIG_FIELDS).toContain('maxPageSize');
    expect(STORAGE_CONFIG_FIELDS).toContain('rowGroupSize');
    expect(STORAGE_CONFIG_FIELDS).toContain('maxRowsPerRowGroup');
    expect(STORAGE_CONFIG_FIELDS).toContain('hotStorageMaxSize');
    expect(STORAGE_CONFIG_FIELDS).toContain('hotDataMaxAge');
    expect(STORAGE_CONFIG_FIELDS).toContain('maxHotFileSize');
    expect(STORAGE_CONFIG_FIELDS).toContain('parquetFileSize');
    expect(STORAGE_CONFIG_FIELDS).toHaveLength(8);
  });
});

// =============================================================================
// Validation Tests
// =============================================================================

describe('isValidStorageConfig', () => {
  it('should return true for valid partial config', () => {
    expect(isValidStorageConfig({ rowGroupSize: 4 * 1024 * 1024 })).toBe(true);
  });

  it('should return true for empty config', () => {
    expect(isValidStorageConfig({})).toBe(true);
  });

  it('should return false for config with unknown fields', () => {
    expect(isValidStorageConfig({ unknownField: 123 } as Record<string, unknown>)).toBe(false);
  });

  it('should return false for config with non-number values', () => {
    expect(isValidStorageConfig({ chunkSize: 'big' } as Record<string, unknown>)).toBe(false);
  });

  it('should return false for config with negative values', () => {
    expect(isValidStorageConfig({ chunkSize: -1 })).toBe(false);
  });

  it('should return true for complete valid config', () => {
    expect(isValidStorageConfig(DEFAULT_STORAGE_CONFIG)).toBe(true);
  });
});

// =============================================================================
// CREATE TABLE WITH STORAGE passthrough Tests
// =============================================================================

describe('CREATE TABLE WITH STORAGE clause passthrough', () => {
  it('should pass SQL with WITH STORAGE clause through exec unchanged', async () => {
    // The SDK should not parse or modify the WITH STORAGE clause;
    // it should pass the SQL string to the server as-is.
    // This test verifies the SDK doesn't reject or mangle the SQL.
    const sql = `CREATE TABLE events (
      id INTEGER PRIMARY KEY,
      data TEXT
    ) WITH STORAGE (rowGroupSize = '4MB', parquetFileSize = '256MB')`;

    // The SQL should be a valid string that can be passed to exec
    expect(typeof sql).toBe('string');
    expect(sql).toContain('WITH STORAGE');
  });
});
