/**
 * Benchmark Adapters Index
 *
 * Export all benchmark adapter implementations for the comprehensive
 * SQLite implementation comparison suite.
 */

// DoSQL (in-memory simulation - DEPRECATED, use dosql-real)
export { DoSQLAdapter, createDoSQLAdapter, type DoSQLAdapterConfig } from './dosql.js';

// DoSQL REAL (actual Database class from src/database.ts)
export {
  DoSQLRealAdapter,
  createDoSQLRealAdapter,
  type DoSQLRealAdapterConfig,
} from './dosql-real.js';

// Native SQLite via better-sqlite3
export {
  BetterSQLite3Adapter,
  createBetterSQLite3Adapter,
  type BetterSQLite3AdapterConfig,
} from './better-sqlite3.js';

// LibSQL (local embedded)
export { LibSQLAdapter, createLibSQLAdapter, type LibSQLAdapterConfig } from './libsql.js';

// Turso (edge SQLite via HTTP)
export {
  TursoAdapter,
  createTursoAdapter,
  createTursoAdapterFromEnv,
  type TursoAdapterConfig,
} from './turso.js';

// Cloudflare D1
export { D1Adapter, createD1Adapter, type D1AdapterConfig } from './d1.js';

// Durable Object SQLite
export {
  DOSqliteAdapter,
  createDOSqliteAdapter,
  type DOSqliteAdapterConfig,
} from './do-sqlite.js';

// Re-export types
export type { BenchmarkAdapter, AdapterType } from '../types.js';

// =============================================================================
// Adapter Factory
// =============================================================================

import type { AdapterType, BenchmarkAdapter } from '../types.js';
import { DoSQLAdapter } from './dosql.js';
import { DoSQLRealAdapter } from './dosql-real.js';
import { BetterSQLite3Adapter } from './better-sqlite3.js';
import { LibSQLAdapter } from './libsql.js';
import { TursoAdapter, type TursoAdapterConfig } from './turso.js';

/**
 * Options for creating adapters
 */
export interface AdapterFactoryOptions {
  /** Turso configuration (required for turso adapter) */
  turso?: TursoAdapterConfig;
  /** D1 database binding (required for d1 adapter in Workers) */
  d1?: unknown;
  /** DO SQL storage (required for do-sqlite adapter in Workers) */
  doSql?: unknown;
}

/**
 * Create adapters by type for Node.js environments
 *
 * Note: D1 and DO-SQLite adapters require Cloudflare Worker bindings
 * and cannot be created in Node.js environments.
 */
export async function createAdapter(
  type: AdapterType,
  options: AdapterFactoryOptions = {}
): Promise<BenchmarkAdapter | null> {
  switch (type) {
    case 'dosql':
      return new DoSQLAdapter();

    case 'dosql-real':
      return new DoSQLRealAdapter();

    case 'better-sqlite3':
      return new BetterSQLite3Adapter();

    case 'libsql':
      return new LibSQLAdapter();

    case 'turso':
      if (!options.turso) {
        console.warn('Turso adapter requires turso configuration (url and authToken)');
        return null;
      }
      return new TursoAdapter(options.turso);

    case 'd1':
      console.warn('D1 adapter requires Cloudflare Worker environment');
      return null;

    case 'do-sqlite':
      console.warn('DO-SQLite adapter requires Cloudflare Worker environment');
      return null;

    default:
      console.warn(`Unknown adapter type: ${type}`);
      return null;
  }
}

/**
 * Get available adapters for the current environment
 */
export function getAvailableAdapters(options: AdapterFactoryOptions = {}): AdapterType[] {
  const adapters: AdapterType[] = ['dosql', 'dosql-real', 'better-sqlite3', 'libsql'];

  if (options.turso?.url && options.turso?.authToken) {
    adapters.push('turso');
  }

  // D1 and DO-SQLite only available in Workers
  if (typeof globalThis !== 'undefined' && 'caches' in globalThis) {
    if (options.d1) {
      adapters.push('d1');
    }
    if (options.doSql) {
      adapters.push('do-sqlite');
    }
  }

  return adapters;
}

/**
 * Adapter display names for reports
 */
export const ADAPTER_DISPLAY_NAMES: Record<AdapterType, string> = {
  dosql: 'DoSQL (simulated)',
  'dosql-real': 'DoSQL (real engine)',
  'better-sqlite3': 'SQLite (better-sqlite3)',
  libsql: 'LibSQL (local)',
  turso: 'Turso (edge)',
  d1: 'Cloudflare D1',
  'do-sqlite': 'DO SQLite',
};

/**
 * Adapter descriptions for documentation
 */
export const ADAPTER_DESCRIPTIONS: Record<AdapterType, string> = {
  dosql: 'DoSQL simulated in-memory store (DEPRECATED - use dosql-real)',
  'dosql-real': 'DoSQL REAL engine using src/database.ts Database class',
  'better-sqlite3': 'Native SQLite via better-sqlite3 (synchronous, fastest)',
  libsql: 'LibSQL embedded mode (async, SQLite fork with extensions)',
  turso: 'Turso edge SQLite over HTTP (includes network latency)',
  d1: 'Cloudflare D1 serverless SQLite',
  'do-sqlite': 'Durable Object ctx.storage.sql (raw DO SQLite API)',
};
