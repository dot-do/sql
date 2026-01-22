/**
 * North Star Benchmark Datasets for DoSQL
 *
 * Standard database benchmark datasets used for testing different query patterns
 * and validating DoSQL performance across various workloads.
 *
 * Datasets included:
 * - **Northwind**: Classic Microsoft sample database (~8,000 rows)
 *   - OLTP workloads: customers, orders, products
 *   - Query patterns: joins, aggregations, date filtering
 *
 * - **O*NET**: Occupational Information Network (~170,000 rows)
 *   - Workforce analysis: occupations, skills, abilities
 *   - Query patterns: text search, hierarchical, range queries
 *
 * - **IMDB**: Movie database subset (~100,000 rows)
 *   - Entertainment data: movies, actors, ratings
 *   - Query patterns: many-to-many joins, top-N, aggregations
 *
 * - **UNSPSC**: Product classification codes (~50,000 rows)
 *   - Procurement taxonomy: hierarchical product codes
 *   - Query patterns: prefix queries, tree traversal, path queries
 *
 * @packageDocumentation
 */

// =============================================================================
// Northwind Dataset
// =============================================================================

export {
  // Types
  type NorthwindCustomer,
  type NorthwindEmployee,
  type NorthwindCategory,
  type NorthwindSupplier,
  type NorthwindProduct,
  type NorthwindOrder,
  type NorthwindOrderDetail,
  type NorthwindShipper,

  // Schema
  NORTHWIND_SCHEMA,
  NORTHWIND_INDEXES,

  // Generator
  type NorthwindGeneratorConfig,
  type NorthwindDataset,
  generateNorthwindData,

  // Queries
  type NorthwindQueryCategory,
  type NorthwindBenchmarkQuery,
  NORTHWIND_BENCHMARK_QUERIES,

  // Metadata
  NORTHWIND_METADATA,

  // Utilities
  getNorthwindSchemaStatements,
  getNorthwindQueriesByCategory,
} from './northwind.js';

// =============================================================================
// O*NET Dataset
// =============================================================================

export {
  // Types
  type OnetOccupation,
  type OnetSkill,
  type OnetOccupationSkill,
  type OnetAbility,
  type OnetOccupationAbility,
  type OnetKnowledge,
  type OnetOccupationKnowledge,
  type OnetWorkActivity,
  type OnetOccupationActivity,
  type OnetWorkContext,
  type OnetOccupationContext,

  // Schema
  ONET_SCHEMA,
  ONET_INDEXES,

  // Generator
  type OnetGeneratorConfig,
  type OnetDataset,
  generateOnetData,

  // Queries
  type OnetQueryCategory,
  type OnetBenchmarkQuery,
  ONET_BENCHMARK_QUERIES,

  // Metadata
  ONET_METADATA,

  // Utilities
  getOnetSchemaStatements,
  getOnetQueriesByCategory,
} from './onet.js';

// =============================================================================
// IMDB Dataset
// =============================================================================

export {
  // Types
  type ImdbMovie,
  type ImdbPerson,
  type ImdbRole,
  type ImdbDirector,
  type ImdbRating,
  type ImdbAward,
  type ImdbGenre,
  type ImdbMovieGenre,

  // Schema
  IMDB_SCHEMA,
  IMDB_INDEXES,

  // Generator
  type ImdbGeneratorConfig,
  type ImdbDataset,
  generateImdbData,

  // Queries
  type ImdbQueryCategory,
  type ImdbBenchmarkQuery,
  IMDB_BENCHMARK_QUERIES,

  // Metadata
  IMDB_METADATA,

  // Utilities
  getImdbSchemaStatements,
  getImdbQueriesByCategory,
} from './imdb.js';

// =============================================================================
// UNSPSC Dataset
// =============================================================================

export {
  // Types
  type UnspscSegment,
  type UnspscFamily,
  type UnspscClass,
  type UnspscCommodity,
  type UnspscFullPath,
  type UnspscSynonym,
  type UnspscCrossRef,

  // Schema
  UNSPSC_SCHEMA,
  UNSPSC_INDEXES,

  // Generator
  type UnspscGeneratorConfig,
  type UnspscDataset,
  generateUnspscData,

  // Queries
  type UnspscQueryCategory,
  type UnspscBenchmarkQuery,
  UNSPSC_BENCHMARK_QUERIES,

  // Metadata
  UNSPSC_METADATA,

  // Utilities
  getUnspscSchemaStatements,
  getUnspscQueriesByCategory,
} from './unspsc.js';

// =============================================================================
// Combined Dataset Information
// =============================================================================

/**
 * All available benchmark datasets
 */
export const BENCHMARK_DATASETS = {
  northwind: 'Northwind',
  onet: 'O*NET',
  imdb: 'IMDB',
  unspsc: 'UNSPSC',
} as const;

/**
 * Dataset name type
 */
export type BenchmarkDatasetName = keyof typeof BENCHMARK_DATASETS;

/**
 * Combined metadata for all datasets
 */
export const ALL_DATASET_METADATA = {
  northwind: {
    name: 'Northwind',
    approximateRows: 8000,
    description: 'Classic OLTP benchmark with customers, orders, products',
    primaryUseCase: 'OLTP workloads, joins, aggregations',
  },
  onet: {
    name: 'O*NET',
    approximateRows: 170000,
    description: 'Occupational data with skills and abilities',
    primaryUseCase: 'Text search, hierarchical queries, range queries',
  },
  imdb: {
    name: 'IMDB',
    approximateRows: 100000,
    description: 'Movie database with actors, directors, ratings',
    primaryUseCase: 'Many-to-many joins, top-N queries, complex aggregations',
  },
  unspsc: {
    name: 'UNSPSC',
    approximateRows: 50000,
    description: 'Hierarchical product classification codes',
    primaryUseCase: 'Tree traversal, prefix queries, path queries',
  },
} as const;

/**
 * Query category types from all datasets
 */
export type AllQueryCategories =
  | 'point_lookup'
  | 'range'
  | 'join'
  | 'aggregate'
  | 'text_search'
  | 'date_filter'
  | 'subquery'
  | 'hierarchical'
  | 'top_n'
  | 'many_to_many'
  | 'self_join'
  | 'prefix'
  | 'tree_traversal'
  | 'path';

/**
 * Get total query count across all datasets
 */
export function getTotalQueryCount(): number {
  return (
    NORTHWIND_BENCHMARK_QUERIES.length +
    ONET_BENCHMARK_QUERIES.length +
    IMDB_BENCHMARK_QUERIES.length +
    UNSPSC_BENCHMARK_QUERIES.length
  );
}

/**
 * Get total approximate row count across all datasets
 */
export function getTotalApproximateRows(): number {
  return (
    ALL_DATASET_METADATA.northwind.approximateRows +
    ALL_DATASET_METADATA.onet.approximateRows +
    ALL_DATASET_METADATA.imdb.approximateRows +
    ALL_DATASET_METADATA.unspsc.approximateRows
  );
}

/**
 * Get all benchmark queries grouped by category
 */
export function getAllQueriesByCategory(): Record<AllQueryCategories, number> {
  const categories: Record<string, number> = {};

  // Helper to count queries by category
  const countQueries = (queries: Array<{ category: string }>) => {
    for (const query of queries) {
      categories[query.category] = (categories[query.category] || 0) + 1;
    }
  };

  countQueries(NORTHWIND_BENCHMARK_QUERIES);
  countQueries(ONET_BENCHMARK_QUERIES);
  countQueries(IMDB_BENCHMARK_QUERIES);
  countQueries(UNSPSC_BENCHMARK_QUERIES);

  return categories as Record<AllQueryCategories, number>;
}

// Import the actual query arrays for the function above
import { NORTHWIND_BENCHMARK_QUERIES } from './northwind.js';
import { ONET_BENCHMARK_QUERIES } from './onet.js';
import { IMDB_BENCHMARK_QUERIES } from './imdb.js';
import { UNSPSC_BENCHMARK_QUERIES } from './unspsc.js';
