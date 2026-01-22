/**
 * DoSQL URL Table Sources
 *
 * Provides ClickHouse-style URL table source support:
 * - SELECT * FROM 'https://example.com/data.csv'
 * - SELECT * FROM 'r2://bucket/path.parquet'
 * - SELECT * FROM url('https://api.example.com/data', 'JSON')
 *
 * @packageDocumentation
 */

// =============================================================================
// TYPE EXPORTS
// =============================================================================

export type {
  // Core types
  Format,
  FormatDetectionResult,
  TableSource,
  ScanOptions,
  SourceOptions,
  R2SourceOptions,
  UrlSourceOptions,

  // Expression types
  Expression,
  ComparisonExpression,
  AndExpression,
  OrExpression,
  NotExpression,
  LiteralExpression,
  ColumnExpression,

  // URI types
  UriScheme,
  ParsedUri,
  TableSourceFactory,

  // Type-level URL parsing
  ParseUrlScheme,
  ParseUrlPath,
  ParseUrlHost,
  ParseUrlExtension,
  InferFormatFromUrl,
  IsUrlLiteral,
  ExtractUrlFromLiteral,
  ParseUrlFunction,

  // Cloudflare types (minimal)
  R2Bucket,
  R2GetOptions,
  R2Range,
  R2Object,
  R2ObjectBody,
  R2HTTPMetadata,
  R2ListOptions,
  R2Objects,
} from './types.js';

// =============================================================================
// PARSER EXPORTS
// =============================================================================

export {
  // Format detection
  detectFormat,
  detectFormatFromExtension,
  detectFormatFromContentType,
  detectFormatFromContent,

  // JSON parsing
  parseJsonArray,
  parseJsonArrayIterator,

  // NDJSON parsing
  parseNdjson,
  parseNdjsonLine,
  parseNdjsonStream,
  parseNdjsonIterator,

  // CSV parsing
  parseCsv,
  parseCsvLine,
  parseCsvIterator,
  parseCsvStream,

  // Parquet (stub)
  parseParquetIterator,
  isParquet,

  // Schema inference
  inferSchema,
  inferSchemaFromCsvHeader,

  // Unified parser
  createFormatIterator,
} from './parser.js';

export type { CsvParseOptions } from './parser.js';

// =============================================================================
// URL SOURCE EXPORTS
// =============================================================================

export {
  UrlTableSource,
  createUrlSource,
  fetchUrl,
} from './url.js';

// =============================================================================
// R2 SOURCE EXPORTS
// =============================================================================

export {
  R2TableSource,
  createR2Source,
  parseR2Uri,
  listR2Objects,
  readParquetFooterSize,
  readParquetFooter,
} from './r2.js';

// =============================================================================
// RESOLVER EXPORTS
// =============================================================================

export {
  // Resolver class
  TableSourceResolver,
  createResolver,
  resolveSource,

  // URI parsing
  parseUri,
  getUriScheme,

  // SQL helpers
  isUrlLiteral,
  extractUrlFromLiteral,
  parseUrlFunction,
  resolveFromClause,
} from './resolver.js';

export type {
  Bindings,
  ResolverOptions,
} from './resolver.js';

// =============================================================================
// TYPE-LEVEL URL PARSING EXPORTS
// =============================================================================

export type {
  // URL scheme parsing
  SupportedScheme,
  ParseUrlScheme as ParseScheme,
  IsSupportedScheme,

  // URL component parsing
  ParseUrlHost as ParseHost,
  ParseUrlPath as ParsePath,
  ParseUrlPort as ParsePort,
  ParseUrlExtension as ParseExtension,
  InferFormatFromUrl as InferFormat,

  // URL literal detection
  IsUrlLiteral as IsLiteral,
  ExtractUrlFromLiteral as ExtractFromLiteral,

  // URL function parsing
  IsUrlFunction,
  ParseUrlFunction as ParseFunction,

  // Parsed result types
  ParsedUrlSource,
  ParseUrlSource as ParseSource,

  // Schema types
  UrlSourceSchema,
  DefineUrlSchema,
  TypedUrlSource,
  SchemaToType,
  UrlQueryResult,

  // SQL integration
  IsUrlSourceFrom,
  ExtractUrlFromFrom,

  // Registry types
  UrlSourceRegistry,
  GetRegisteredSchema,
  HasRegisteredSchema,
} from './url-types.js';
