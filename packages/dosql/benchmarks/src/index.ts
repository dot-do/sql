/**
 * DoSQL Benchmark Suite
 *
 * Comprehensive benchmark suite for comparing DoSQL against other SQLite implementations:
 * - SQLite (better-sqlite3)
 * - LibSQL (local embedded)
 * - Turso (edge SQLite via HTTP)
 * - Cloudflare D1
 * - DO SQLite (raw Durable Object SQLite API)
 *
 * @packageDocumentation
 */

// Types
export * from './types.js';

// Adapters
export * from './adapters/index.js';

// Scenarios
export * from './scenarios/index.js';

// Runner
export { BenchmarkRunner, createBenchmarkRunner, type BenchmarkRunnerOptions } from './runner.js';

// Report
export {
  generateMarkdownReport,
  serializeResults,
  deserializeResults,
  type ReportConfig,
} from './report.js';
