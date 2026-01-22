/**
 * DoSQL Iceberg Integration Module
 *
 * This module provides bidirectional Iceberg interoperability for DoSQL:
 *
 * 1. **Client Mode**: Connect to external Iceberg REST Catalogs
 *    - Use iceberg-js to query Snowflake, AWS Glue, Tabular, etc.
 *    - Federate queries across external Iceberg tables
 *
 * 2. **Server Mode**: Expose DoSQL tables via Iceberg REST API
 *    - Allow Spark, DuckDB, Trino to query DoSQL tables
 *    - Standard Iceberg protocol for universal compatibility
 *
 * @example
 * ```typescript
 * // Client mode - connect to external catalog
 * import { createIcebergClient } from '@dotdo/dosql/iceberg';
 *
 * const client = createIcebergClient('https://tabular.io/api/v1', {
 *   token: process.env.TABULAR_TOKEN,
 * });
 *
 * const { dosqlSchema } = await client.loadExternalTableSchema(
 *   ['analytics'],
 *   'events'
 * );
 * ```
 *
 * @example
 * ```typescript
 * // Server mode - expose DoSQL as catalog
 * import { createIcebergServer } from '@dotdo/dosql/iceberg';
 * import { Hono } from 'hono';
 *
 * const app = new Hono();
 * const iceberg = createIcebergServer('r2://my-bucket/warehouse');
 *
 * // Mount Iceberg REST API endpoints
 * const endpoints = iceberg.getRestEndpoints();
 * app.get('/v1/config', (c) => c.json(endpoints['GET /v1/config']()));
 * // ... mount other endpoints
 * ```
 *
 * @module
 */

// Main exports from spike implementation
export {
  // Core class
  DoSQLIceberg,

  // Factory functions
  createIcebergClient,
  createIcebergServer,
  createIcebergDual,

  // Schema conversion functions
  dosqlSchemaToIceberg,
  icebergSchemaToDosql,
  dosqlTypeToIceberg,
  icebergTypeToDosql,

  // Re-exported iceberg-js client
  IcebergRestCatalog,
  IcebergError,

  // Re-exported iceberg-js utilities
  getCurrentSchema,
  parseDecimalType,
  parseFixedType,
  isDecimalType,
  isFixedType,
  typesEqual,

  // Types
  type DoSQLIcebergConfig,
  type DoSQLColumnType,
  type DoSQLTableSchema,

} from './spike.js';

// Re-export iceberg-js types with Iceberg prefix for clarity
export type {
  TableSchema as IcebergTableSchema,
  StructField as IcebergStructField,
  TableField as IcebergTableField,
  IcebergType,
  PrimitiveType as IcebergPrimitiveType,
  StructType as IcebergStructType,
  ListType as IcebergListType,
  MapType as IcebergMapType,
  PrimitiveTypeValue as IcebergPrimitiveTypeValue,
  PartitionSpec as IcebergPartitionSpec,
  PartitionField as IcebergPartitionField,
  SortOrder as IcebergSortOrder,
  SortField as IcebergSortField,
  TableMetadata as IcebergTableMetadata,
  NamespaceIdentifier as IcebergNamespaceIdentifier,
  NamespaceMetadata as IcebergNamespaceMetadata,
  CreateTableRequest as IcebergCreateTableRequest,
  UpdateTableRequest as IcebergUpdateTableRequest,
  DropTableRequest as IcebergDropTableRequest,
  CommitTableResponse as IcebergCommitTableResponse,
  CreateNamespaceResponse as IcebergCreateNamespaceResponse,
  TableIdentifier as IcebergTableIdentifier,
  AuthConfig as IcebergAuthConfig,
  AccessDelegation as IcebergAccessDelegation,
  IcebergRestCatalogOptions,
} from 'iceberg-js';

// Extended types not in iceberg-js
export {
  // Utility functions
  generateSnapshotId,
  partitionPath,
  metadataFilePath,
  manifestListPath,
  manifestFilePath,
  dataFilePath,

  // Types
  type Snapshot,
  type SnapshotOperation,
  type SnapshotSummary,
  type SnapshotRef,
  type ManifestFile,
  type ManifestEntry,
  type ManifestEntryStatus,
  type ManifestContent,
  type PartitionFieldSummary,
  type DataFile,
  type FileFormat,
  type DoSQLTableMetadata,
  type DOStorage,
  type IcebergRestEndpoints,
  type CommitRequirement,
  type CommitUpdate,
  type CommitTableRequest,
  type CommitTableResponse,
  type PartitionTransform,
} from './types.js';
