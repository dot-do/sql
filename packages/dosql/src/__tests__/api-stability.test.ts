/**
 * API Stability Tests
 *
 * These tests verify that all exports declared as "stable" in the
 * API stability manifest remain present at runtime. If a stable
 * export is accidentally removed or renamed, these tests will fail,
 * preventing unintentional breaking changes.
 *
 * Philosophy: stable APIs are a contract with consumers. This test
 * suite is the enforcement mechanism for that contract.
 */

import { describe, it, expect } from 'vitest';
import {
  type StabilityLevel,
  type APIStabilityEntry,
  DOSQL_STABLE_EXPORTS,
  DOSQL_EXPERIMENTAL_EXPORTS,
  validateStableExports,
  getRuntimeExports,
  getExportsByStability,
} from '../api-stability.js';

// Import the full module namespace to check exports
import * as dosql from '../index.js';

// =============================================================================
// MANIFEST INTEGRITY
// =============================================================================

describe('API stability manifest', () => {
  it('should have non-empty stable exports manifest', () => {
    expect(DOSQL_STABLE_EXPORTS.length).toBeGreaterThan(0);
  });

  it('should have non-empty experimental exports manifest', () => {
    expect(DOSQL_EXPERIMENTAL_EXPORTS.length).toBeGreaterThan(0);
  });

  it('should have valid stability levels in stable manifest', () => {
    for (const entry of DOSQL_STABLE_EXPORTS) {
      expect(entry.stability).toBe('stable');
    }
  });

  it('should have valid stability levels in experimental manifest', () => {
    for (const entry of DOSQL_EXPERIMENTAL_EXPORTS) {
      expect(entry.stability).toBe('experimental');
    }
  });

  it('should not have duplicate names within stable manifest', () => {
    const names = DOSQL_STABLE_EXPORTS.map((e) => e.name);
    const unique = new Set(names);
    expect(unique.size).toBe(names.length);
  });

  it('should not have duplicate names within experimental manifest', () => {
    const names = DOSQL_EXPERIMENTAL_EXPORTS.map((e) => e.name);
    const unique = new Set(names);
    expect(unique.size).toBe(names.length);
  });

  it('should have names that are non-empty strings', () => {
    for (const entry of [...DOSQL_STABLE_EXPORTS, ...DOSQL_EXPERIMENTAL_EXPORTS]) {
      expect(typeof entry.name).toBe('string');
      expect(entry.name.length).toBeGreaterThan(0);
    }
  });
});

// =============================================================================
// STABLE EXPORT PRESENCE
// =============================================================================

describe('stable API exports', () => {
  const runtimeStable = getRuntimeExports(DOSQL_STABLE_EXPORTS);

  it('should export all stable runtime symbols from the package', () => {
    const missing = validateStableExports(
      dosql as unknown as Record<string, unknown>,
      DOSQL_STABLE_EXPORTS,
    );
    if (missing.length > 0) {
      throw new Error(
        `Missing stable exports (this is a BREAKING CHANGE):\n` +
        missing.map((name) => `  - ${name}`).join('\n'),
      );
    }
  });

  // Generate individual test cases for each stable runtime export
  // so that failures are easy to diagnose
  for (const entry of runtimeStable) {
    it(`should export stable symbol: ${entry.name}`, () => {
      expect((dosql as Record<string, unknown>)[entry.name]).toBeDefined();
    });
  }
});

// =============================================================================
// EXPERIMENTAL EXPORT PRESENCE
// =============================================================================

describe('experimental API exports', () => {
  const runtimeExperimental = getRuntimeExports(DOSQL_EXPERIMENTAL_EXPORTS);

  it('should export all experimental runtime symbols from the package', () => {
    const missing = validateStableExports(
      dosql as unknown as Record<string, unknown>,
      DOSQL_EXPERIMENTAL_EXPORTS,
    );
    if (missing.length > 0) {
      // Experimental exports being missing is a warning, not a hard failure,
      // but we still fail the test to catch unintentional removals.
      throw new Error(
        `Missing experimental exports (was this intentional?):\n` +
        missing.map((name) => `  - ${name}`).join('\n'),
      );
    }
  });

  for (const entry of runtimeExperimental) {
    it(`should export experimental symbol: ${entry.name}`, () => {
      expect((dosql as Record<string, unknown>)[entry.name]).toBeDefined();
    });
  }
});

// =============================================================================
// STABILITY UTILITIES
// =============================================================================

describe('stability utility functions', () => {
  it('validateStableExports returns empty array when all present', () => {
    const mockModule: Record<string, unknown> = { foo: 1, bar: 'hello' };
    const manifest: APIStabilityEntry[] = [
      { name: 'foo', stability: 'stable' },
      { name: 'bar', stability: 'stable' },
    ];
    expect(validateStableExports(mockModule, manifest)).toEqual([]);
  });

  it('validateStableExports returns missing names', () => {
    const mockModule: Record<string, unknown> = { foo: 1 };
    const manifest: APIStabilityEntry[] = [
      { name: 'foo', stability: 'stable' },
      { name: 'bar', stability: 'stable' },
      { name: 'baz', stability: 'stable' },
    ];
    expect(validateStableExports(mockModule, manifest)).toEqual(['bar', 'baz']);
  });

  it('validateStableExports skips type-only entries', () => {
    const mockModule: Record<string, unknown> = {};
    const manifest: APIStabilityEntry[] = [
      { name: 'MyType', stability: 'stable', typeOnly: true },
      { name: 'MyOtherType', stability: 'stable', typeOnly: true },
    ];
    expect(validateStableExports(mockModule, manifest)).toEqual([]);
  });

  it('getRuntimeExports filters out type-only entries', () => {
    const manifest: APIStabilityEntry[] = [
      { name: 'foo', stability: 'stable' },
      { name: 'FooType', stability: 'stable', typeOnly: true },
      { name: 'bar', stability: 'stable' },
    ];
    const result = getRuntimeExports(manifest);
    expect(result).toHaveLength(2);
    expect(result.map((e) => e.name)).toEqual(['foo', 'bar']);
  });

  it('getExportsByStability filters by level', () => {
    const manifest: APIStabilityEntry[] = [
      { name: 'stableFn', stability: 'stable' },
      { name: 'expFn', stability: 'experimental' },
      { name: 'deprecatedFn', stability: 'deprecated' },
      { name: 'stableFn2', stability: 'stable' },
    ];
    const stableOnly = getExportsByStability(manifest, 'stable');
    expect(stableOnly).toHaveLength(2);
    expect(stableOnly.map((e) => e.name)).toEqual(['stableFn', 'stableFn2']);

    const expOnly = getExportsByStability(manifest, 'experimental');
    expect(expOnly).toHaveLength(1);
    expect(expOnly[0].name).toBe('expFn');
  });
});

// =============================================================================
// CORE STABLE API SHAPE VERIFICATION
// =============================================================================

describe('stable API shape verification', () => {
  it('createDatabase should be a function', () => {
    expect(typeof dosql.createDatabase).toBe('function');
  });

  it('createQuery should be a function', () => {
    expect(typeof dosql.createQuery).toBe('function');
  });

  it('TransactionState should be an object (enum)', () => {
    expect(typeof dosql.TransactionState).toBe('object');
  });

  it('TransactionMode should be an object (enum)', () => {
    expect(typeof dosql.TransactionMode).toBe('object');
  });

  it('IsolationLevel should be an object (enum)', () => {
    expect(typeof dosql.IsolationLevel).toBe('object');
  });

  it('TransactionError should be a constructor', () => {
    expect(typeof dosql.TransactionError).toBe('function');
  });

  it('createTransactionManager should be a function', () => {
    expect(typeof dosql.createTransactionManager).toBe('function');
  });

  it('executeInTransaction should be a function', () => {
    expect(typeof dosql.executeInTransaction).toBe('function');
  });

  it('createLockManager should be a function', () => {
    expect(typeof dosql.createLockManager).toBe('function');
  });

  it('createMVCCStore should be a function', () => {
    expect(typeof dosql.createMVCCStore).toBe('function');
  });

  it('createIsolationEnforcer should be a function', () => {
    expect(typeof dosql.createIsolationEnforcer).toBe('function');
  });

  it('validateStableExports should be a function', () => {
    expect(typeof dosql.validateStableExports).toBe('function');
  });

  it('DOSQL_STABLE_EXPORTS should be an array', () => {
    expect(Array.isArray(dosql.DOSQL_STABLE_EXPORTS)).toBe(true);
  });

  it('DOSQL_EXPERIMENTAL_EXPORTS should be an array', () => {
    expect(Array.isArray(dosql.DOSQL_EXPERIMENTAL_EXPORTS)).toBe(true);
  });
});
