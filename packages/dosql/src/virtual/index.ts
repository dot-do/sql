/**
 * DoSQL Virtual Table Module
 *
 * Provides virtual table support for querying remote data sources directly:
 * - SELECT * FROM 'https://api.example.com/users.json'
 * - SELECT * FROM 'r2://mybucket/data/sales.parquet' WHERE year = 2024
 * - SELECT * FROM 'https://data.gov/dataset.csv' WITH (headers=true, delimiter=',')
 *
 * @packageDocumentation
 */

// =============================================================================
// TYPE EXPORTS
// =============================================================================

export type {
  // Format types
  VirtualTableFormat,

  // URL source options
  AuthOptions,
  URLSourceOptions,
  WithClauseOptions,

  // Virtual table interface
  VirtualTableSchema,
  VirtualTableStats,
  VirtualTable,
  VirtualTableScanOptions,
  VirtualTableResult,

  // URL scheme types
  VirtualTableScheme,
  ParsedVirtualTableUrl,

  // Type-level parsing
  ParseVirtualTableScheme,
  IsVirtualTableUrl,
  ExtractVirtualTableUrl,
  InferVirtualTableFormat,

  // Factory types
  VirtualTableFactory,
  SchemeHandler,
} from './types.js';

// =============================================================================
// URL TABLE EXPORTS
// =============================================================================

export {
  // URL parsing
  parseVirtualTableUrl,

  // URL virtual table class
  URLVirtualTable,

  // Factory functions
  createURLVirtualTable,
  fetchVirtualTable,
} from './url-table.js';

// =============================================================================
// REGISTRY EXPORTS
// =============================================================================

export {
  // Registry class
  VirtualTableRegistry,

  // Factory functions
  createVirtualTableRegistry,
  createVirtualTable,
  resolveVirtualTableFromClause,

  // WITH clause parsing
  parseWithClause,
  withClauseToOptions,
} from './registry.js';

export type {
  VirtualTableBindings,
  VirtualTableRegistryOptions,
} from './registry.js';

// =============================================================================
// PARSER EXPORTS
// =============================================================================

export {
  // URL literal detection
  isValidUrlScheme,
  isQuotedUrlLiteral,
  extractUrlFromQuotedLiteral,

  // URL function parsing
  isUrlFunction,
  parseUrlFunction as parseVirtualTableUrlFunction,

  // WITH clause parsing
  extractWithClause,
  parseWithClauseOptions,

  // FROM clause parsing
  parseVirtualTableFromClause,

  // Full SQL parsing
  parseSelectWithVirtualTable,
} from '../parser/virtual.js';

export type {
  ParsedVirtualTableSource,
  VirtualTableParseResult,

  // Type-level parsing
  IsVirtualTableFrom,
  ExtractVirtualTableUrl as ExtractVirtualTableUrlType,
  InferVirtualTableFormat as InferVirtualTableFormatType,
} from '../parser/virtual.js';
