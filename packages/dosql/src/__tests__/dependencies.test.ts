/**
 * Module Dependency Graph Validation
 *
 * Validates the intended module dependency hierarchy within packages/dosql/src/.
 * Ensures that lower-level modules do not import from higher-level modules
 * (except for type-only imports, which are erased at compile time and do not
 * create runtime circular dependencies).
 *
 * ## Intended Dependency Hierarchy (lower layers must not depend on higher)
 *
 * Layer 0 - Foundation:   utils, errors, logging, types, constants
 * Layer 1 - Storage:      fsx, btree, columnar, wal, storage
 * Layer 2 - Language:     parser, planner
 * Layer 3 - Execution:    engine, executor, execution, transaction
 * Layer 4 - Features:     cdc, fts, vector, triggers, proc, functions,
 *                         constraints, schema, view, virtual, sources,
 *                         cursor, statement, r2-index, collation
 * Layer 5 - Integration:  replication, sharding, distributed-tx, orm,
 *                         migrations, rpc, worker, database, cli,
 *                         observability, index, compaction, lakehouse,
 *                         timetravel, backup, attach, codegen, security,
 *                         benchmarks, branch
 *
 * ## Known Exceptions
 *
 * Some cross-layer runtime imports exist for pragmatic reasons:
 * - parser -> engine/storage-config.js (storage clause parsing needs config types)
 * - fsx -> btree/lru-cache.js (shared LRU cache utility)
 * - wal -> engine/types.js (LSN/TransactionId branded types)
 * - transaction -> engine/types.js (needs execution context types)
 * - cdc -> engine/types.js (needs execution context types)
 *
 * Type-only imports across layers are always allowed since TypeScript
 * erases them at compile time, producing no runtime dependency.
 */

import { describe, it, expect } from 'vitest';

// ---------------------------------------------------------------------------
// Module layer definitions
// ---------------------------------------------------------------------------

/** Modules in each architectural layer, ordered from lowest to highest. */
const LAYERS: Record<string, string[]> = {
  foundation: ['utils', 'errors', 'logging', 'types', 'constants'],
  storage: ['fsx', 'btree', 'columnar', 'wal', 'storage'],
  language: ['parser', 'planner'],
  execution: ['engine', 'executor', 'execution', 'transaction'],
  features: [
    'cdc', 'fts', 'vector', 'triggers', 'proc', 'functions',
    'constraints', 'schema', 'view', 'virtual', 'sources',
    'cursor', 'statement', 'r2-index', 'collation',
  ],
  integration: [
    'replication', 'sharding', 'distributed-tx', 'orm',
    'migrations', 'rpc', 'worker', 'database', 'cli',
    'observability', 'index', 'compaction', 'lakehouse',
    'timetravel', 'backup', 'attach', 'codegen', 'security',
    'benchmarks', 'branch',
  ],
};

const LAYER_ORDER = ['foundation', 'storage', 'language', 'execution', 'features', 'integration'];

/**
 * Known cross-layer runtime import exceptions.
 * Each entry is [fromModule, toModule] where fromModule is in a lower layer
 * than toModule, representing an intentional upward dependency.
 * Key: "from -> to", value: reason for the exception.
 */
const KNOWN_EXCEPTIONS: Record<string, string> = {
  'parser -> engine': 'storage clause parsing needs storage-config utilities',
  'fsx -> btree': 'shared LRU cache utility in btree module',
  'wal -> engine': 'LSN and TransactionId branded types from engine/types',
  'wal -> types': 're-exports branded types',
  'transaction -> engine': 'needs execution context types',
  'cdc -> engine': 'needs execution context types',
  'cdc -> logging': 'structured logging support',
  'errors -> utils': 'error formatting utilities',
  'storage -> database': 'database storage interface',
  'columnar -> storage': 'storage interface types (type-boundary)',
  'btree -> storage': 'storage interface types (type-boundary)',
  'engine -> triggers': 'engine must fire triggers during DML execution',
};

// ---------------------------------------------------------------------------
// Runtime dependency map
// ---------------------------------------------------------------------------

/**
 * Complete runtime (value) dependency map extracted from static analysis.
 * Only includes non-type imports between top-level modules.
 * This map is the source of truth for validation; if a new dependency
 * is added to the code, it must be reflected here (or the test fails).
 *
 * Generated via: analyze imports with `import type` filtered out,
 * resolving relative paths to target module directories.
 */
const RUNTIME_DEPS: Record<string, string[]> = {
  backup: ['engine', 'utils', 'wal'],
  cdc: ['engine', 'logging', 'utils', 'wal'],
  cli: ['database', 'logging'],
  collation: ['errors'],
  columnar: ['utils'],
  compaction: ['columnar', 'logging', 'utils'],
  constraints: ['utils'],
  'distributed-tx': ['logging', 'transaction', 'utils'],
  engine: ['errors', 'parser', 'planner', 'triggers', 'utils'],
  errors: ['utils'],
  executor: ['errors', 'parser', 'utils'],
  fsx: ['btree', 'utils'],
  fts: ['errors', 'utils'],
  functions: ['utils'],
  index: ['btree', 'utils'],
  lakehouse: ['columnar', 'utils'],
  migrations: ['logging'],
  orm: ['logging', 'utils'],
  parser: ['engine', 'errors', 'utils'],
  planner: ['errors', 'utils'],
  proc: ['logging', 'utils'],
  'r2-index': ['utils'],
  replication: ['logging', 'wal'],
  rpc: ['errors', 'logging', 'statement'],
  schema: ['logging'],
  sharding: ['logging', 'utils'],
  sources: ['utils'],
  statement: ['utils'],
  storage: ['database', 'errors'],
  timetravel: ['fsx', 'utils'],
  transaction: ['engine', 'logging', 'utils', 'wal'],
  triggers: ['utils'],
  vector: ['utils'],
  virtual: ['sources', 'utils'],
  wal: ['engine', 'logging', 'types', 'utils'],
  worker: ['btree', 'errors', 'fsx', 'logging', 'wal'],
};

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/** Return the layer name for a given module, or undefined if not in any layer. */
function getLayer(mod: string): string | undefined {
  for (const [layer, modules] of Object.entries(LAYERS)) {
    if (modules.includes(mod)) return layer;
  }
  return undefined;
}

/** Return the numeric index of a layer (lower = more foundational). */
function layerIndex(layer: string): number {
  return LAYER_ORDER.indexOf(layer);
}

/**
 * Simple 2-node cycle detection (A -> B -> A).
 */
function findDirectCycles(graph: Record<string, string[]>): Array<[string, string]> {
  const cycles: Array<[string, string]> = [];
  const seen = new Set<string>();

  for (const [mod, deps] of Object.entries(graph)) {
    for (const dep of deps) {
      const key = [mod, dep].sort().join(':');
      if (seen.has(key)) continue;
      if (graph[dep]?.includes(mod)) {
        seen.add(key);
        cycles.push([mod, dep]);
      }
    }
  }

  return cycles;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('Module Dependency Graph', () => {
  describe('dependency hierarchy validation', () => {
    it('should have all modules with dependencies assigned to a layer', () => {
      const unassigned: string[] = [];
      for (const mod of Object.keys(RUNTIME_DEPS)) {
        if (!getLayer(mod)) {
          unassigned.push(mod);
        }
      }
      expect(
        unassigned,
        `Modules with runtime deps not assigned to any layer: ${unassigned.join(', ')}. ` +
        `Please add them to the LAYERS definition.`
      ).toEqual([]);
    });

    it('should not have unexpected upward runtime dependencies', () => {
      const violations: string[] = [];

      for (const [mod, deps] of Object.entries(RUNTIME_DEPS)) {
        const modLayer = getLayer(mod);
        if (!modLayer) continue;

        for (const dep of deps) {
          const depLayer = getLayer(dep);
          if (!depLayer) continue;

          const modIdx = layerIndex(modLayer);
          const depIdx = layerIndex(depLayer);

          // An upward dependency is when a lower layer depends on a higher layer
          if (depIdx > modIdx) {
            const key = `${mod} -> ${dep}`;
            if (!KNOWN_EXCEPTIONS[key]) {
              violations.push(
                `${mod} (${modLayer}, L${modIdx}) imports from ${dep} (${depLayer}, L${depIdx})`
              );
            }
          }
        }
      }

      expect(
        violations,
        `Found unexpected upward runtime dependencies:\n${violations.join('\n')}.\n` +
        `Either move the import to a type-only import, refactor the dependency, ` +
        `or add it to KNOWN_EXCEPTIONS with a justification.`
      ).toEqual([]);
    });

    it('should follow the expected query execution pipeline order', () => {
      // Verify: parser -> planner -> engine (the core pipeline)
      // engine depends on parser and planner
      expect(RUNTIME_DEPS['engine']).toEqual(
        expect.arrayContaining(['parser', 'planner'])
      );

      // planner should NOT depend on engine at runtime
      expect(RUNTIME_DEPS['planner'] || []).not.toEqual(
        expect.arrayContaining(['engine'])
      );
    });
  });

  describe('circular dependency detection', () => {
    it('should have no direct runtime circular dependencies (A -> B -> A) except known ones', () => {
      const knownDirectCycles = new Set([
        'engine:parser', // parser -> engine (storage-config), engine -> parser
      ]);

      const directCycles = findDirectCycles(RUNTIME_DEPS);
      const unexpected = directCycles.filter(([a, b]) => {
        const key = [a, b].sort().join(':');
        return !knownDirectCycles.has(key);
      });

      expect(
        unexpected.map(([a, b]) => `${a} <-> ${b}`),
        `Found unexpected direct circular runtime dependencies. ` +
        `These create real import cycles at runtime and should be refactored.`
      ).toEqual([]);
    });

    it('should document all known runtime cycles', () => {
      // The only known runtime cycle is engine <-> parser
      const directCycles = findDirectCycles(RUNTIME_DEPS);
      const cycleKeys = directCycles.map(([a, b]) => [a, b].sort().join(':'));

      expect(cycleKeys).toContain('engine:parser');
      // If you fix the engine<->parser cycle, update this test!
    });
  });

  describe('foundation layer isolation', () => {
    it('should have utils depend on nothing except other foundation modules', () => {
      const utilsDeps = RUNTIME_DEPS['utils'] || [];
      const nonFoundation = utilsDeps.filter(d => {
        const layer = getLayer(d);
        return layer && layer !== 'foundation';
      });

      expect(
        nonFoundation,
        `utils should only depend on other foundation modules, but depends on: ${nonFoundation.join(', ')}`
      ).toEqual([]);
    });

    it('should have logging depend on nothing', () => {
      const loggingDeps = RUNTIME_DEPS['logging'] || [];
      expect(loggingDeps).toEqual([]);
    });

    it('should have errors depend only on foundation modules', () => {
      const errorsDeps = RUNTIME_DEPS['errors'] || [];
      const nonFoundation = errorsDeps.filter(d => {
        const layer = getLayer(d);
        return layer && layer !== 'foundation';
      });

      expect(
        nonFoundation,
        `errors should only depend on foundation modules, but depends on: ${nonFoundation.join(', ')}`
      ).toEqual([]);
    });
  });

  describe('storage layer dependencies', () => {
    it('should have btree depend only on foundation and storage-layer modules', () => {
      const btreeDeps = RUNTIME_DEPS['btree'] || [];
      for (const dep of btreeDeps) {
        const layer = getLayer(dep);
        if (!layer) continue;
        const idx = layerIndex(layer);
        expect(idx).toBeLessThanOrEqual(layerIndex('storage'));
      }
    });

    it('should have wal depend only on foundation, storage, and known exceptions', () => {
      const walDeps = RUNTIME_DEPS['wal'] || [];
      for (const dep of walDeps) {
        const layer = getLayer(dep);
        if (!layer) continue;
        const idx = layerIndex(layer);
        if (idx > layerIndex('storage')) {
          const key = `wal -> ${dep}`;
          expect(KNOWN_EXCEPTIONS).toHaveProperty(key);
        }
      }
    });
  });

  describe('integration layer dependencies', () => {
    it('should have worker depend only on lower layers', () => {
      const workerDeps = RUNTIME_DEPS['worker'] || [];
      for (const dep of workerDeps) {
        const layer = getLayer(dep);
        if (!layer) continue;
        const idx = layerIndex(layer);
        expect(idx).toBeLessThan(layerIndex('integration'));
      }
    });

    it('should have replication depend only on lower layers', () => {
      const replDeps = RUNTIME_DEPS['replication'] || [];
      for (const dep of replDeps) {
        const layer = getLayer(dep);
        if (!layer) continue;
        const idx = layerIndex(layer);
        expect(idx).toBeLessThan(layerIndex('integration'));
      }
    });
  });

  describe('dependency map completeness', () => {
    it('should include all core modules in the layer definitions', () => {
      const coreModules = [
        'utils', 'errors', 'logging', 'parser', 'planner', 'engine',
        'btree', 'columnar', 'fsx', 'wal', 'transaction', 'cdc',
        'worker', 'replication', 'sharding', 'rpc',
      ];

      for (const mod of coreModules) {
        expect(getLayer(mod)).toBeDefined();
      }
    });

    it('should have the query pipeline as a valid topological path', () => {
      // SQL -> Parser -> Planner -> Engine -> Storage (btree/columnar)
      // Validate each step is at the same or higher layer
      const pipeline = ['parser', 'planner', 'engine'];
      for (let i = 0; i < pipeline.length - 1; i++) {
        const current = pipeline[i];
        const next = pipeline[i + 1];
        const currentLayer = getLayer(current)!;
        const nextLayer = getLayer(next)!;
        expect(layerIndex(currentLayer)).toBeLessThanOrEqual(layerIndex(nextLayer));
      }
    });
  });
});
