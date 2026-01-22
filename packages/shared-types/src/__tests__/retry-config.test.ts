/**
 * TDD GREEN Phase Tests: RetryConfig Consolidation
 *
 * These tests verify the consolidated RetryConfig API in shared-types.
 *
 * Implementation State:
 * - RetryConfig interface is exported from @dotdo/shared-types
 * - DEFAULT_RETRY_CONFIG constant is exported from @dotdo/shared-types
 * - isRetryConfig type guard is exported from @dotdo/shared-types
 * - createRetryConfig factory is exported from @dotdo/shared-types
 * - sql.do imports and re-exports RetryConfig from @dotdo/shared-types
 * - lake.do imports and re-exports RetryConfig from @dotdo/shared-types
 * - Type compatibility is maintained across all packages
 *
 * Note: Cross-package integration tests (verifying sql.do and lake.do re-exports)
 * should be in the respective consumer packages, not here, since shared-types
 * doesn't have those as dependencies.
 */

import { describe, it, expect } from 'vitest';
import {
  DEFAULT_RETRY_CONFIG,
  isRetryConfig,
  createRetryConfig,
  type RetryConfig,
} from '../index.js';

// =============================================================================
// Test 1: RetryConfig type should exist in shared-types
// =============================================================================

describe('RetryConfig in shared-types', () => {
  it('should export DEFAULT_RETRY_CONFIG constant from shared-types', () => {
    // Check that DEFAULT_RETRY_CONFIG exists and has the expected shape
    expect(DEFAULT_RETRY_CONFIG).toBeDefined();
    expect(DEFAULT_RETRY_CONFIG).toMatchObject({
      maxRetries: expect.any(Number),
      baseDelayMs: expect.any(Number),
      maxDelayMs: expect.any(Number),
    });
  });

  it('DEFAULT_RETRY_CONFIG should have sensible defaults', () => {
    // Verify default values are reasonable
    expect(DEFAULT_RETRY_CONFIG.maxRetries).toBeGreaterThanOrEqual(1);
    expect(DEFAULT_RETRY_CONFIG.maxRetries).toBeLessThanOrEqual(10);
    expect(DEFAULT_RETRY_CONFIG.baseDelayMs).toBeGreaterThanOrEqual(50);
    expect(DEFAULT_RETRY_CONFIG.baseDelayMs).toBeLessThanOrEqual(500);
    expect(DEFAULT_RETRY_CONFIG.maxDelayMs).toBeGreaterThan(
      DEFAULT_RETRY_CONFIG.baseDelayMs
    );
  });

  it('RetryConfig type should be usable for type annotations', () => {
    // Verify that the type can be used for variable annotations
    // This test would fail at compile time if the type isn't exported properly
    const config: RetryConfig = {
      maxRetries: 3,
      baseDelayMs: 100,
      maxDelayMs: 5000,
    };

    expect(config.maxRetries).toBe(3);
    expect(config.baseDelayMs).toBe(100);
    expect(config.maxDelayMs).toBe(5000);
  });
});

// =============================================================================
// Test 2: DEFAULT_RETRY_CONFIG constant in shared-types
// =============================================================================

describe('DEFAULT_RETRY_CONFIG in shared-types', () => {
  it('should be a valid RetryConfig object', () => {
    // Verify DEFAULT_RETRY_CONFIG has all required fields
    const config = DEFAULT_RETRY_CONFIG;

    expect(config).toHaveProperty('maxRetries');
    expect(config).toHaveProperty('baseDelayMs');
    expect(config).toHaveProperty('maxDelayMs');

    expect(typeof config.maxRetries).toBe('number');
    expect(typeof config.baseDelayMs).toBe('number');
    expect(typeof config.maxDelayMs).toBe('number');
  });

  it('should match the current duplicated defaults', () => {
    // The consolidated default should match what both packages previously used
    // Both sql.do and lake.do previously had:
    // maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000
    expect(DEFAULT_RETRY_CONFIG).toEqual({
      maxRetries: 3,
      baseDelayMs: 100,
      maxDelayMs: 5000,
    });
  });

  it('should pass the isRetryConfig type guard', () => {
    // DEFAULT_RETRY_CONFIG should itself pass the type guard
    expect(isRetryConfig(DEFAULT_RETRY_CONFIG)).toBe(true);
  });
});

// =============================================================================
// Test 3: Type compatibility
// =============================================================================

describe('RetryConfig type compatibility', () => {
  it('RetryConfig should be usable for client configuration', () => {
    // Create a config using the shared type
    const sharedRetryConfig: RetryConfig = {
      maxRetries: 3,
      baseDelayMs: 100,
      maxDelayMs: 5000,
    };

    // Both client configs should accept this
    const sqlClientConfig = {
      url: 'https://sql.example.com',
      retry: sharedRetryConfig,
    };

    const lakeClientConfig = {
      url: 'https://lake.example.com',
      retry: sharedRetryConfig,
    };

    // Same retry config works for both
    expect(sqlClientConfig.retry).toBe(lakeClientConfig.retry);
    expect(sqlClientConfig.retry.maxRetries).toBe(3);
    expect(lakeClientConfig.retry.maxRetries).toBe(3);
  });

  it('isRetryConfig type guard should validate correctly', () => {
    // A type guard function should be provided for runtime validation
    expect(typeof isRetryConfig).toBe('function');

    // Valid config should pass
    const validConfig = { maxRetries: 3, baseDelayMs: 100, maxDelayMs: 5000 };
    expect(isRetryConfig(validConfig)).toBe(true);

    // Invalid configs should fail
    expect(isRetryConfig({})).toBe(false);
    expect(isRetryConfig({ maxRetries: 3 })).toBe(false);
    expect(isRetryConfig({ maxRetries: 3, baseDelayMs: 100 })).toBe(false);
    expect(isRetryConfig(null)).toBe(false);
    expect(isRetryConfig(undefined)).toBe(false);
    expect(isRetryConfig('not a config')).toBe(false);
    expect(isRetryConfig(123)).toBe(false);
    expect(isRetryConfig([])).toBe(false);
  });

  it('isRetryConfig should reject objects with wrong property types', () => {
    // Properties with wrong types should fail
    expect(isRetryConfig({ maxRetries: '3', baseDelayMs: 100, maxDelayMs: 5000 })).toBe(false);
    expect(isRetryConfig({ maxRetries: 3, baseDelayMs: '100', maxDelayMs: 5000 })).toBe(false);
    expect(isRetryConfig({ maxRetries: 3, baseDelayMs: 100, maxDelayMs: '5000' })).toBe(false);
  });

  it('createRetryConfig factory should create valid configs', () => {
    // A factory function should allow creating configs with validation
    expect(typeof createRetryConfig).toBe('function');

    // Should create a valid config
    const config = createRetryConfig({
      maxRetries: 5,
      baseDelayMs: 200,
      maxDelayMs: 10000,
    });

    expect(config.maxRetries).toBe(5);
    expect(config.baseDelayMs).toBe(200);
    expect(config.maxDelayMs).toBe(10000);
  });

  it('createRetryConfig should validate constraints', () => {
    // Should throw for negative maxRetries
    expect(() => createRetryConfig({
      maxRetries: -1,
      baseDelayMs: 100,
      maxDelayMs: 5000,
    })).toThrow('maxRetries cannot be negative');

    // Should throw for negative baseDelayMs
    expect(() => createRetryConfig({
      maxRetries: 3,
      baseDelayMs: -100,
      maxDelayMs: 5000,
    })).toThrow('baseDelayMs cannot be negative');

    // Should throw for negative maxDelayMs
    expect(() => createRetryConfig({
      maxRetries: 3,
      baseDelayMs: 100,
      maxDelayMs: -5000,
    })).toThrow('maxDelayMs cannot be negative');

    // Should throw when maxDelayMs < baseDelayMs
    expect(() => createRetryConfig({
      maxRetries: 3,
      baseDelayMs: 100,
      maxDelayMs: 50,
    })).toThrow('maxDelayMs cannot be less than baseDelayMs');
  });

  it('createRetryConfig should allow edge cases', () => {
    // Zero retries should be valid (disables retries)
    const noRetries = createRetryConfig({
      maxRetries: 0,
      baseDelayMs: 100,
      maxDelayMs: 5000,
    });
    expect(noRetries.maxRetries).toBe(0);

    // Zero delays should be valid
    const noDelay = createRetryConfig({
      maxRetries: 3,
      baseDelayMs: 0,
      maxDelayMs: 0,
    });
    expect(noDelay.baseDelayMs).toBe(0);
    expect(noDelay.maxDelayMs).toBe(0);

    // Equal delays should be valid
    const equalDelays = createRetryConfig({
      maxRetries: 3,
      baseDelayMs: 1000,
      maxDelayMs: 1000,
    });
    expect(equalDelays.baseDelayMs).toBe(1000);
    expect(equalDelays.maxDelayMs).toBe(1000);
  });
});
