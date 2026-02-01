/**
 * Storage Configuration Tests (TDD - Red Phase)
 *
 * Tests for per-table and per-database storage configuration.
 * These settings control chunk sizes, row group sizes, tiered storage
 * thresholds, and Parquet file sizes.
 */

import { describe, it, expect } from 'vitest';
import {
  type StorageConfig,
  type TableStorageConfig,
  type DatabaseStorageConfig,
  DEFAULT_STORAGE_CONFIG,
  mergeStorageConfig,
  validateStorageConfig,
  parseStorageSize,
  formatStorageSize,
  parseWithStorageClause,
} from '../storage-config.js';

// =============================================================================
// StorageConfig Type Tests
// =============================================================================

describe('StorageConfig', () => {
  describe('DEFAULT_STORAGE_CONFIG', () => {
    it('should have sensible defaults matching current hardcoded values', () => {
      expect(DEFAULT_STORAGE_CONFIG.chunkSize).toBe(2 * 1024 * 1024); // 2MB
      expect(DEFAULT_STORAGE_CONFIG.maxPageSize).toBe(2 * 1024 * 1024); // 2MB
      expect(DEFAULT_STORAGE_CONFIG.rowGroupSize).toBe(1 * 1024 * 1024); // 1MB
      expect(DEFAULT_STORAGE_CONFIG.maxRowsPerRowGroup).toBe(65536);
      expect(DEFAULT_STORAGE_CONFIG.hotStorageMaxSize).toBe(100 * 1024 * 1024); // 100MB
      expect(DEFAULT_STORAGE_CONFIG.hotDataMaxAge).toBe(60 * 60 * 1000); // 1 hour
      expect(DEFAULT_STORAGE_CONFIG.maxHotFileSize).toBe(10 * 1024 * 1024); // 10MB
      expect(DEFAULT_STORAGE_CONFIG.parquetFileSize).toBe(512 * 1024 * 1024); // 512MB
    });

    it('should be frozen (immutable)', () => {
      expect(Object.isFrozen(DEFAULT_STORAGE_CONFIG)).toBe(true);
    });
  });

  describe('mergeStorageConfig', () => {
    it('should return defaults when no overrides provided', () => {
      const result = mergeStorageConfig();
      expect(result).toEqual(DEFAULT_STORAGE_CONFIG);
    });

    it('should merge database-level overrides', () => {
      const dbConfig: DatabaseStorageConfig = {
        rowGroupSize: 4 * 1024 * 1024, // 4MB
      };
      const result = mergeStorageConfig(dbConfig);
      expect(result.rowGroupSize).toBe(4 * 1024 * 1024);
      // All other values should be defaults
      expect(result.chunkSize).toBe(DEFAULT_STORAGE_CONFIG.chunkSize);
      expect(result.parquetFileSize).toBe(DEFAULT_STORAGE_CONFIG.parquetFileSize);
    });

    it('should merge table-level overrides on top of database-level', () => {
      const dbConfig: DatabaseStorageConfig = {
        rowGroupSize: 4 * 1024 * 1024,
        parquetFileSize: 256 * 1024 * 1024,
      };
      const tableConfig: TableStorageConfig = {
        rowGroupSize: 256 * 1024, // Override db-level
      };
      const result = mergeStorageConfig(dbConfig, tableConfig);
      expect(result.rowGroupSize).toBe(256 * 1024); // Table wins
      expect(result.parquetFileSize).toBe(256 * 1024 * 1024); // From db
      expect(result.chunkSize).toBe(DEFAULT_STORAGE_CONFIG.chunkSize); // Default
    });

    it('should handle undefined values in overrides (skip them)', () => {
      const dbConfig: DatabaseStorageConfig = {
        rowGroupSize: undefined,
        parquetFileSize: 64 * 1024 * 1024,
      };
      const result = mergeStorageConfig(dbConfig);
      expect(result.rowGroupSize).toBe(DEFAULT_STORAGE_CONFIG.rowGroupSize); // Not overridden
      expect(result.parquetFileSize).toBe(64 * 1024 * 1024);
    });
  });

  describe('validateStorageConfig', () => {
    it('should accept valid configuration', () => {
      const result = validateStorageConfig(DEFAULT_STORAGE_CONFIG);
      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should reject chunkSize > 2MB (DO storage limit)', () => {
      const result = validateStorageConfig({
        ...DEFAULT_STORAGE_CONFIG,
        chunkSize: 3 * 1024 * 1024, // 3MB - exceeds DO limit
      });
      expect(result.valid).toBe(false);
      expect(result.errors).toContainEqual(
        expect.objectContaining({ field: 'chunkSize' })
      );
    });

    it('should reject chunkSize < 1KB', () => {
      const result = validateStorageConfig({
        ...DEFAULT_STORAGE_CONFIG,
        chunkSize: 512, // Too small
      });
      expect(result.valid).toBe(false);
    });

    it('should reject rowGroupSize > chunkSize * 64 (unreasonable)', () => {
      const result = validateStorageConfig({
        ...DEFAULT_STORAGE_CONFIG,
        rowGroupSize: 200 * 1024 * 1024, // 200MB row group
        chunkSize: 2 * 1024 * 1024,
      });
      expect(result.valid).toBe(false);
    });

    it('should reject maxRowsPerRowGroup < 1', () => {
      const result = validateStorageConfig({
        ...DEFAULT_STORAGE_CONFIG,
        maxRowsPerRowGroup: 0,
      });
      expect(result.valid).toBe(false);
    });

    it('should reject hotStorageMaxSize < 1MB', () => {
      const result = validateStorageConfig({
        ...DEFAULT_STORAGE_CONFIG,
        hotStorageMaxSize: 512 * 1024, // 512KB - too small
      });
      expect(result.valid).toBe(false);
    });

    it('should reject parquetFileSize < 1MB', () => {
      const result = validateStorageConfig({
        ...DEFAULT_STORAGE_CONFIG,
        parquetFileSize: 512 * 1024,
      });
      expect(result.valid).toBe(false);
    });

    it('should reject parquetFileSize > 5GB (R2 single object limit)', () => {
      const result = validateStorageConfig({
        ...DEFAULT_STORAGE_CONFIG,
        parquetFileSize: 6 * 1024 * 1024 * 1024,
      });
      expect(result.valid).toBe(false);
    });

    it('should reject negative values', () => {
      const result = validateStorageConfig({
        ...DEFAULT_STORAGE_CONFIG,
        hotDataMaxAge: -1000,
      });
      expect(result.valid).toBe(false);
    });
  });

  describe('parseStorageSize', () => {
    it('should parse KB values', () => {
      expect(parseStorageSize('256KB')).toBe(256 * 1024);
      expect(parseStorageSize('256kb')).toBe(256 * 1024);
    });

    it('should parse MB values', () => {
      expect(parseStorageSize('4MB')).toBe(4 * 1024 * 1024);
      expect(parseStorageSize('4mb')).toBe(4 * 1024 * 1024);
    });

    it('should parse GB values', () => {
      expect(parseStorageSize('2GB')).toBe(2 * 1024 * 1024 * 1024);
    });

    it('should parse bare numbers as bytes', () => {
      expect(parseStorageSize('1048576')).toBe(1048576);
    });

    it('should throw on invalid format', () => {
      expect(() => parseStorageSize('abc')).toThrow();
      expect(() => parseStorageSize('')).toThrow();
      expect(() => parseStorageSize('-1MB')).toThrow();
    });
  });

  describe('formatStorageSize', () => {
    it('should format bytes to human-readable', () => {
      expect(formatStorageSize(1024)).toBe('1KB');
      expect(formatStorageSize(1048576)).toBe('1MB');
      expect(formatStorageSize(1073741824)).toBe('1GB');
      expect(formatStorageSize(512)).toBe('512B');
    });

    it('should handle fractional sizes', () => {
      expect(formatStorageSize(1.5 * 1024 * 1024)).toBe('1.5MB');
    });
  });

  describe('parseWithStorageClause', () => {
    it('should return null when no WITH STORAGE clause is present', () => {
      const sql = 'CREATE TABLE foo (id INT)';
      const result = parseWithStorageClause(sql);
      expect(result).toBeNull();
    });

    it('should parse single setting with size value', () => {
      const sql = "CREATE TABLE foo (id INT) WITH STORAGE (rowGroupSize = '4MB')";
      const result = parseWithStorageClause(sql);
      expect(result).toEqual({
        rowGroupSize: 4 * 1024 * 1024,
      });
    });

    it('should parse multiple settings', () => {
      const sql = "CREATE TABLE foo (id INT) WITH STORAGE (rowGroupSize = '4MB', parquetFileSize = '256MB')";
      const result = parseWithStorageClause(sql);
      expect(result).toEqual({
        rowGroupSize: 4 * 1024 * 1024,
        parquetFileSize: 256 * 1024 * 1024,
      });
    });

    it('should parse numeric millisecond values', () => {
      const sql = 'CREATE TABLE foo (id INT) WITH STORAGE (hotDataMaxAge = 3600000)';
      const result = parseWithStorageClause(sql);
      expect(result).toEqual({
        hotDataMaxAge: 3600000,
      });
    });

    it('should parse bare number values', () => {
      const sql = 'CREATE TABLE foo (id INT) WITH STORAGE (rowGroupSize = 1048576)';
      const result = parseWithStorageClause(sql);
      expect(result).toEqual({
        rowGroupSize: 1048576,
      });
    });

    it('should throw on unknown setting names', () => {
      const sql = "CREATE TABLE foo (id INT) WITH STORAGE (unknownSetting = '4MB')";
      expect(() => parseWithStorageClause(sql)).toThrow(/unknown.*setting/i);
    });

    it('should throw on invalid values', () => {
      const sql = "CREATE TABLE foo (id INT) WITH STORAGE (rowGroupSize = '-1')";
      expect(() => parseWithStorageClause(sql)).toThrow();
    });

    it('should throw on invalid size format', () => {
      const sql = "CREATE TABLE foo (id INT) WITH STORAGE (rowGroupSize = 'invalid')";
      expect(() => parseWithStorageClause(sql)).toThrow(/invalid storage size format/i);
    });

    it('should be case insensitive for WITH STORAGE keyword', () => {
      const sql = "CREATE TABLE foo (id INT) with storage (rowGroupSize = '4MB')";
      const result = parseWithStorageClause(sql);
      expect(result).toEqual({
        rowGroupSize: 4 * 1024 * 1024,
      });
    });

    it('should handle extra whitespace', () => {
      const sql = `CREATE TABLE foo (id INT) WITH STORAGE (
        rowGroupSize = '4MB' ,
        parquetFileSize = '256MB'
      )`;
      const result = parseWithStorageClause(sql);
      expect(result).toEqual({
        rowGroupSize: 4 * 1024 * 1024,
        parquetFileSize: 256 * 1024 * 1024,
      });
    });

    it('should handle double-quoted values', () => {
      const sql = 'CREATE TABLE foo (id INT) WITH STORAGE (rowGroupSize = "4MB")';
      const result = parseWithStorageClause(sql);
      expect(result).toEqual({
        rowGroupSize: 4 * 1024 * 1024,
      });
    });

    it('should parse all valid storage config keys', () => {
      const sql = `CREATE TABLE foo (id INT) WITH STORAGE (
        chunkSize = '1MB',
        maxPageSize = '2MB',
        rowGroupSize = '4MB',
        maxRowsPerRowGroup = 32768,
        hotStorageMaxSize = '200MB',
        hotDataMaxAge = 7200000,
        maxHotFileSize = '20MB',
        parquetFileSize = '1GB'
      )`;
      const result = parseWithStorageClause(sql);
      expect(result).toEqual({
        chunkSize: 1 * 1024 * 1024,
        maxPageSize: 2 * 1024 * 1024,
        rowGroupSize: 4 * 1024 * 1024,
        maxRowsPerRowGroup: 32768,
        hotStorageMaxSize: 200 * 1024 * 1024,
        hotDataMaxAge: 7200000,
        maxHotFileSize: 20 * 1024 * 1024,
        parquetFileSize: 1 * 1024 * 1024 * 1024,
      });
    });

    it('should handle mixed quoted and unquoted values', () => {
      const sql = "CREATE TABLE foo (id INT) WITH STORAGE (rowGroupSize = '4MB', maxRowsPerRowGroup = 32768)";
      const result = parseWithStorageClause(sql);
      expect(result).toEqual({
        rowGroupSize: 4 * 1024 * 1024,
        maxRowsPerRowGroup: 32768,
      });
    });
  });
});
