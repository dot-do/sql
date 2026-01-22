/**
 * Benchmark Adapters
 *
 * Export all benchmark adapter implementations for the comparison suite.
 */

export { DoSQLAdapter, createDoSQLAdapter, type DoSQLAdapterConfig } from './dosql.js';

// D1 and DO-SQLite adapters require runtime bindings and are provided as placeholders.
// Use the adapters from the parent package when running in a Cloudflare Worker environment.

/**
 * Placeholder for D1 adapter config
 * Use the actual D1Adapter from the parent package in a Worker environment
 */
export interface D1AdapterConfig {
  db: unknown; // D1Database
}

/**
 * Placeholder for DO-SQLite adapter config
 * Use the actual DOSqliteAdapter from the parent package in a Worker environment
 */
export interface DOSqliteAdapterConfig {
  storage: unknown; // DOStorage
}
