/**
 * Runtime Configuration for @dotdo/shared-types
 *
 * This module contains runtime state and configuration functions.
 * It is separated from the pure type definitions in index.ts to maintain
 * the "pure types" principle where possible.
 *
 * IMPORTANT: This file contains runtime state (global variables) that affect
 * the behavior of factory functions in the main module:
 * - _devMode: Enables/disables runtime validation in factory functions
 * - _strictMode: Enables stricter format validation
 * - _wrapperCacheConfig: Controls wrapper cache behavior (LRU, TTL, size limits)
 *
 * @module config
 */

// =============================================================================
// Wrapper Cache Configuration
// =============================================================================

/**
 * Default maximum size for wrapper cache Maps
 */
export const DEFAULT_MAX_WRAPPER_CACHE_SIZE = 10000;

/**
 * Wrapper cache configuration
 *
 * The cache uses LRU (Least Recently Used) eviction strategy.
 */
export interface WrapperCacheConfig {
  /** Maximum number of entries in the cache (default: 10000) */
  maxSize?: number;
  /** Time-to-live for cache entries in milliseconds (0 = no TTL, default: 0) */
  ttlMs?: number;
  /** Whether caching is enabled (default: true) */
  enabled?: boolean;
}

// Internal cache configuration state
let _wrapperCacheConfig: Required<WrapperCacheConfig> = {
  maxSize: DEFAULT_MAX_WRAPPER_CACHE_SIZE,
  ttlMs: 0,
  enabled: true,
};

/**
 * Set wrapper cache configuration
 */
export function setWrapperCacheConfig(config: WrapperCacheConfig): void {
  _wrapperCacheConfig = {
    maxSize: config.maxSize ?? _wrapperCacheConfig.maxSize,
    ttlMs: config.ttlMs ?? _wrapperCacheConfig.ttlMs,
    enabled: config.enabled ?? _wrapperCacheConfig.enabled,
  };
}

/**
 * Get current wrapper cache configuration
 */
export function getWrapperCacheConfig(): Required<WrapperCacheConfig> {
  return { ..._wrapperCacheConfig };
}

/**
 * Internal getter for cache config (used by factory functions)
 * Returns the actual reference for performance
 * @internal
 */
export function _getWrapperCacheConfigInternal(): Required<WrapperCacheConfig> {
  return _wrapperCacheConfig;
}

// =============================================================================
// Runtime Mode Configuration
// =============================================================================

let _devMode = true; // Default to dev mode for safety
let _strictMode = false; // Strict mode for additional format validation

/**
 * Set development mode for enabling runtime validation
 */
export function setDevMode(enabled: boolean): void {
  _devMode = enabled;
}

/**
 * Check if development mode is enabled
 */
export function isDevMode(): boolean {
  return _devMode;
}

/**
 * Set strict mode for additional format validation
 */
export function setStrictMode(enabled: boolean): void {
  _strictMode = enabled;
}

/**
 * Check if strict mode is enabled
 */
export function isStrictMode(): boolean {
  return _strictMode;
}

/**
 * Internal getter for dev mode (used by factory functions)
 * @internal
 */
export function _isDevModeInternal(): boolean {
  return _devMode;
}

/**
 * Internal getter for strict mode (used by factory functions)
 * @internal
 */
export function _isStrictModeInternal(): boolean {
  return _strictMode;
}
