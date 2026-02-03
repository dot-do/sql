/**
 * API Stability Policy for DoSQL
 *
 * This module defines the stability levels used across all DoSQL packages
 * and provides utilities for verifying API surface contracts.
 *
 * ## Stability Levels
 *
 * - **stable**: The API will not have breaking changes in minor or patch versions.
 *   Safe for production use. Breaking changes require a major version bump.
 *
 * - **experimental**: The API may change in any version without notice.
 *   Use with caution. Suitable for prototyping and non-critical workloads.
 *   Experimental APIs may be promoted to stable or removed entirely.
 *
 * - **deprecated**: The API is scheduled for removal. A migration path to a
 *   replacement API is documented in the deprecation notice. Deprecated APIs
 *   will be removed in the next major version.
 *
 * - **internal**: The API is not part of the public contract. It may change
 *   or be removed at any time. Do not depend on internal APIs.
 *
 * ## Policy
 *
 * 1. All public exports MUST be annotated with a stability level via JSDoc.
 * 2. Stable APIs follow semver: no breaking changes in minor/patch versions.
 * 3. Experimental APIs MAY change in any release but SHOULD include a changelog note.
 * 4. Deprecated APIs MUST specify the replacement and the target removal version.
 * 5. Internal APIs are not re-exported from package entry points.
 *
 * @packageDocumentation
 */

// =============================================================================
// STABILITY LEVELS
// =============================================================================

/**
 * Stability level for a public API export.
 *
 * @stable
 */
export type StabilityLevel = 'stable' | 'experimental' | 'deprecated' | 'internal';

/**
 * Metadata describing a public API export's stability contract.
 *
 * @stable
 */
export interface APIStabilityEntry {
  /** The exported symbol name */
  readonly name: string;
  /** The stability level */
  readonly stability: StabilityLevel;
  /** The version when this API was introduced */
  readonly since?: string;
  /** For deprecated APIs, the replacement symbol name */
  readonly replacedBy?: string;
  /** For deprecated APIs, the version when it will be removed */
  readonly removeIn?: string;
  /** Whether this is a type-only export (no runtime value) */
  readonly typeOnly?: boolean;
}

// =============================================================================
// DOSQL STABLE API MANIFEST
// =============================================================================

/**
 * The stable API surface of the `dosql` package.
 *
 * This manifest enumerates every public export that is considered stable.
 * Tests verify that all symbols listed here remain exported from the
 * package entry point. Adding to this list is always safe; removing an
 * entry requires a major version bump.
 *
 * @stable
 */
export const DOSQL_STABLE_EXPORTS: readonly APIStabilityEntry[] = [
  // Parser - core types
  { name: 'ColumnType', stability: 'stable', typeOnly: true },
  { name: 'TableSchema', stability: 'stable', typeOnly: true },
  { name: 'DatabaseSchema', stability: 'stable', typeOnly: true },
  { name: 'QueryResult', stability: 'stable', typeOnly: true },
  { name: 'TypedDatabase', stability: 'stable', typeOnly: true },
  { name: 'SQL', stability: 'stable', typeOnly: true },
  { name: 'createDatabase', stability: 'stable' },
  { name: 'createQuery', stability: 'stable' },

  // Transaction - state enums and error types
  { name: 'TransactionState', stability: 'stable' },
  { name: 'TransactionMode', stability: 'stable' },
  { name: 'IsolationLevel', stability: 'stable' },
  { name: 'LockType', stability: 'stable' },
  { name: 'TransactionError', stability: 'stable' },
  { name: 'TransactionErrorCode', stability: 'stable' },
  { name: 'createTransactionLog', stability: 'stable' },
  { name: 'createSavepointStack', stability: 'stable' },
  { name: 'createTransactionManager', stability: 'stable' },
  { name: 'executeInTransaction', stability: 'stable' },
  { name: 'executeWithSavepoint', stability: 'stable' },
  { name: 'executeReadOnly', stability: 'stable' },
  { name: 'createAutoCommitManager', stability: 'stable' },
  { name: 'createLockManager', stability: 'stable' },
  { name: 'createMVCCStore', stability: 'stable' },
  { name: 'createIsolationEnforcer', stability: 'stable' },
  { name: 'createSnapshot', stability: 'stable' },
  { name: 'isVersionVisible', stability: 'stable' },
] as const;

/**
 * The experimental API surface of the `dosql` package.
 *
 * Experimental exports may change in any minor or patch version.
 * They are included here for documentation and tracking purposes.
 *
 * @experimental
 */
export const DOSQL_EXPERIMENTAL_EXPORTS: readonly APIStabilityEntry[] = [
  // Aggregates (type-level)
  { name: 'AggregateFunctionName', stability: 'experimental', typeOnly: true, since: '0.1.0' },
  { name: 'IsAggregateFunction', stability: 'experimental', typeOnly: true, since: '0.1.0' },
  { name: 'ExtractAggregateFn', stability: 'experimental', typeOnly: true, since: '0.1.0' },
  { name: 'ExtractAggregateArg', stability: 'experimental', typeOnly: true, since: '0.1.0' },
  { name: 'AggregateResultType', stability: 'experimental', typeOnly: true, since: '0.1.0' },
  { name: 'ArithmeticOperator', stability: 'experimental', typeOnly: true, since: '0.1.0' },
  { name: 'IsArithmeticExpression', stability: 'experimental', typeOnly: true, since: '0.1.0' },
  { name: 'IsNumericLiteral', stability: 'experimental', typeOnly: true, since: '0.1.0' },
  { name: 'ParseExpressionWithAlias', stability: 'experimental', typeOnly: true, since: '0.1.0' },
  { name: 'GetExpressionOutputName', stability: 'experimental', typeOnly: true, since: '0.1.0' },
  { name: 'ResolveExpressionType', stability: 'experimental', typeOnly: true, since: '0.1.0' },
  { name: 'HasGroupBy', stability: 'experimental', typeOnly: true, since: '0.1.0' },
  { name: 'ExtractGroupByColumns', stability: 'experimental', typeOnly: true, since: '0.1.0' },
  { name: 'ParseSelectExpression', stability: 'experimental', typeOnly: true, since: '0.1.0' },
  { name: 'ParseSelectExpressions', stability: 'experimental', typeOnly: true, since: '0.1.0' },
  { name: 'ExpressionToResultType', stability: 'experimental', typeOnly: true, since: '0.1.0' },

  // Stored procedures
  { name: 'createTableAccessor', stability: 'experimental', since: '0.1.0' },
  { name: 'createSqlFunction', stability: 'experimental', since: '0.1.0' },
  { name: 'createTransactionContext', stability: 'experimental', since: '0.1.0' },
  { name: 'createTransactionFunction', stability: 'experimental', since: '0.1.0' },
  { name: 'createDatabaseContext', stability: 'experimental', since: '0.1.0' },
  { name: 'createInMemoryAdapter', stability: 'experimental', since: '0.1.0' },
  { name: 'createInMemorySqlExecutor', stability: 'experimental', since: '0.1.0' },
  { name: 'createInMemoryTransactionManager', stability: 'experimental', since: '0.1.0' },
  { name: 'parseProcedure', stability: 'experimental', since: '0.1.0' },
  { name: 'tryParseProcedure', stability: 'experimental', since: '0.1.0' },
  { name: 'isCreateProcedure', stability: 'experimental', since: '0.1.0' },
  { name: 'validateModuleCode', stability: 'experimental', since: '0.1.0' },
  { name: 'ProcedureBuilder', stability: 'experimental', since: '0.1.0' },
  { name: 'procedure', stability: 'experimental', since: '0.1.0' },
  { name: 'createProcedureRegistry', stability: 'experimental', since: '0.1.0' },
  { name: 'createProcedureExecutor', stability: 'experimental', since: '0.1.0' },
  { name: 'createSimpleExecutor', stability: 'experimental', since: '0.1.0' },
  { name: 'createProcedureCall', stability: 'experimental', since: '0.1.0' },
  { name: 'batchExecute', stability: 'experimental', since: '0.1.0' },
  { name: 'sequentialExecute', stability: 'experimental', since: '0.1.0' },
  { name: 'createMockExecutor', stability: 'experimental', since: '0.1.0' },

  // WAL
  { name: 'createWALWriter', stability: 'experimental', since: '0.1.0' },
  { name: 'createWALReader', stability: 'experimental', since: '0.1.0' },
  { name: 'createCheckpointManager', stability: 'experimental', since: '0.1.0' },
  { name: 'createWALRetentionManager', stability: 'experimental', since: '0.1.0' },
  { name: 'WALError', stability: 'experimental', since: '0.1.0' },
  { name: 'WALErrorCode', stability: 'experimental', since: '0.1.0' },

  // CDC
  { name: 'createCDCSubscription', stability: 'experimental', since: '0.1.0' },
  { name: 'createCDCStream', stability: 'experimental', since: '0.1.0' },
  { name: 'createCDC', stability: 'experimental', since: '0.1.0' },
  { name: 'CDCError', stability: 'experimental', since: '0.1.0' },
  { name: 'CDCErrorCode', stability: 'experimental', since: '0.1.0' },
  // Note: CDC_PROTOCOL_VERSION is declared in cdc/index.ts but not yet defined at runtime

  // Sharding
  { name: 'createShardingClient', stability: 'experimental', since: '0.1.0' },
  { name: 'createTypedShardingClient', stability: 'experimental', since: '0.1.0' },
  { name: 'createVSchema', stability: 'experimental', since: '0.1.0' },
  { name: 'shardedTable', stability: 'experimental', since: '0.1.0' },
  { name: 'unshardedTable', stability: 'experimental', since: '0.1.0' },
  { name: 'referenceTable', stability: 'experimental', since: '0.1.0' },
  { name: 'hashVindex', stability: 'experimental', since: '0.1.0' },
  { name: 'consistentHashVindex', stability: 'experimental', since: '0.1.0' },
  { name: 'rangeVindex', stability: 'experimental', since: '0.1.0' },

  // Sources
  { name: 'detectFormat', stability: 'experimental', since: '0.1.0' },
  { name: 'parseCsv', stability: 'experimental', since: '0.1.0' },
  { name: 'parseNdjson', stability: 'experimental', since: '0.1.0' },
  { name: 'parseJsonArray', stability: 'experimental', since: '0.1.0' },
  { name: 'createUrlSource', stability: 'experimental', since: '0.1.0' },
  { name: 'createR2Source', stability: 'experimental', since: '0.1.0' },
  { name: 'createResolver', stability: 'experimental', since: '0.1.0' },

  // Virtual tables
  { name: 'createURLVirtualTable', stability: 'experimental', since: '0.1.0' },
  { name: 'createVirtualTableRegistry', stability: 'experimental', since: '0.1.0' },
  { name: 'createVirtualTable', stability: 'experimental', since: '0.1.0' },
] as const;

// =============================================================================
// VALIDATION UTILITIES
// =============================================================================

/**
 * Check whether a given module object exports all the stable symbols
 * listed in a manifest.
 *
 * Returns an array of missing symbol names. An empty array means
 * the module satisfies the stability contract.
 *
 * @param moduleExports - The module namespace object (e.g. `import * as mod from 'dosql'`)
 * @param manifest - The stability manifest to validate against
 * @returns Array of missing export names (empty if all present)
 *
 * @stable
 */
export function validateStableExports(
  moduleExports: Record<string, unknown>,
  manifest: readonly APIStabilityEntry[],
): string[] {
  const missing: string[] = [];
  for (const entry of manifest) {
    // Type-only exports don't exist at runtime, skip them
    if (entry.typeOnly) continue;
    if (!(entry.name in moduleExports)) {
      missing.push(entry.name);
    }
  }
  return missing;
}

/**
 * Returns only the runtime (non type-only) entries from a manifest.
 *
 * @param manifest - The stability manifest
 * @returns Filtered entries that have runtime values
 *
 * @stable
 */
export function getRuntimeExports(
  manifest: readonly APIStabilityEntry[],
): APIStabilityEntry[] {
  return manifest.filter((entry) => !entry.typeOnly);
}

/**
 * Returns entries for a specific stability level.
 *
 * @param manifest - The stability manifest
 * @param level - The stability level to filter by
 * @returns Filtered entries matching the given level
 *
 * @stable
 */
export function getExportsByStability(
  manifest: readonly APIStabilityEntry[],
  level: StabilityLevel,
): APIStabilityEntry[] {
  return manifest.filter((entry) => entry.stability === level);
}
