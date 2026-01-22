/**
 * Extended Iceberg Types for DoSQL
 *
 * iceberg-js provides excellent types for catalog operations but lacks:
 * - Snapshot types (for time travel)
 * - Manifest types (for data file tracking)
 * - Data file types (for Parquet files)
 *
 * These types are essential for DoSQL's native Iceberg write path with hyparquet.
 */

import type {
  TableSchema,
  PartitionSpec,
  SortOrder,
  TableMetadata,
} from 'iceberg-js';

/**
 * =============================================================================
 * Snapshot Types (Iceberg v2 Format)
 * =============================================================================
 */

/** Operations that can create snapshots */
export type SnapshotOperation = 'append' | 'replace' | 'overwrite' | 'delete';

/** Summary statistics for a snapshot */
export interface SnapshotSummary {
  operation: SnapshotOperation;
  'added-data-files'?: string;
  'added-records'?: string;
  'added-files-size'?: string;
  'deleted-data-files'?: string;
  'deleted-records'?: string;
  'changed-partition-count'?: string;
  'total-records'?: string;
  'total-files-size'?: string;
  'total-data-files'?: string;
  'total-delete-files'?: string;
  'total-position-deletes'?: string;
  'total-equality-deletes'?: string;
  [key: string]: string | undefined;
}

/**
 * Iceberg Snapshot
 *
 * Represents a point-in-time view of a table. Snapshots are immutable
 * and form a chain via parent_snapshot_id for time travel.
 */
export interface Snapshot {
  /** Unique snapshot ID (monotonically increasing) */
  'snapshot-id': bigint;

  /** Parent snapshot ID (null for first snapshot) */
  'parent-snapshot-id': bigint | null;

  /** Monotonically increasing sequence number */
  'sequence-number': bigint;

  /** Timestamp when snapshot was created (ms since epoch) */
  'timestamp-ms': bigint;

  /** Location of the manifest list file */
  'manifest-list': string;

  /** Summary statistics */
  summary: SnapshotSummary;

  /** Schema ID at time of snapshot */
  'schema-id'?: number;
}

/**
 * =============================================================================
 * Manifest Types
 * =============================================================================
 */

/** Content type of files in a manifest */
export type ManifestContent = 'data' | 'deletes';

/**
 * Manifest File
 *
 * A manifest file tracks a collection of data files. Multiple manifests
 * can be grouped in a manifest list for a snapshot.
 */
export interface ManifestFile {
  /** Manifest file path */
  'manifest-path': string;

  /** Length in bytes */
  'manifest-length': bigint;

  /** Partition spec ID used to write the manifest */
  'partition-spec-id': number;

  /** Content type (data or deletes) */
  content: ManifestContent;

  /** Sequence number when manifest was added */
  'sequence-number': bigint;

  /** Minimum sequence number of files in manifest */
  'min-sequence-number': bigint;

  /** Snapshot ID that added this manifest */
  'added-snapshot-id': bigint;

  /** Count of entries with ADDED status */
  'added-files-count': number;

  /** Count of entries with EXISTING status */
  'existing-files-count': number;

  /** Count of entries with DELETED status */
  'deleted-files-count': number;

  /** Total sum of added rows */
  'added-rows-count': bigint;

  /** Total sum of existing rows */
  'existing-rows-count': bigint;

  /** Total sum of deleted rows */
  'deleted-rows-count': bigint;

  /** Partition field summaries (optional) */
  partitions?: PartitionFieldSummary[];
}

/** Summary stats for a partition field in a manifest */
export interface PartitionFieldSummary {
  'contains-null': boolean;
  'contains-nan'?: boolean;
  'lower-bound'?: Uint8Array;
  'upper-bound'?: Uint8Array;
}

/** Status of a file entry in a manifest */
export type ManifestEntryStatus = 0 | 1 | 2; // EXISTING = 0, ADDED = 1, DELETED = 2

/**
 * Manifest Entry
 *
 * An entry in a manifest file describing a single data file.
 */
export interface ManifestEntry {
  /** Status: 0=EXISTING, 1=ADDED, 2=DELETED */
  status: ManifestEntryStatus;

  /** Snapshot ID when file was added */
  'snapshot-id'?: bigint;

  /** Sequence number when file was added */
  'sequence-number'?: bigint;

  /** File sequence number */
  'file-sequence-number'?: bigint;

  /** The data file */
  'data-file': DataFile;
}

/**
 * =============================================================================
 * Data File Types
 * =============================================================================
 */

/** File format of data files */
export type FileFormat = 'avro' | 'orc' | 'parquet';

/**
 * Data File
 *
 * Describes a single data file (Parquet, Avro, ORC) in the table.
 */
export interface DataFile {
  /** Content type (0=data, 1=position-deletes, 2=equality-deletes) */
  content: 0 | 1 | 2;

  /** Full path to the file */
  'file-path': string;

  /** File format */
  'file-format': FileFormat;

  /** Partition values as a struct */
  partition: Record<string, unknown>;

  /** Number of records in the file */
  'record-count': bigint;

  /** File size in bytes */
  'file-size-in-bytes': bigint;

  /** Per-column value counts (key: field-id) */
  'column-sizes'?: Map<number, bigint>;

  /** Per-column value counts */
  'value-counts'?: Map<number, bigint>;

  /** Per-column null counts */
  'null-value-counts'?: Map<number, bigint>;

  /** Per-column NaN counts */
  'nan-value-counts'?: Map<number, bigint>;

  /** Per-column lower bounds (serialized) */
  'lower-bounds'?: Map<number, Uint8Array>;

  /** Per-column upper bounds (serialized) */
  'upper-bounds'?: Map<number, Uint8Array>;

  /** Split offsets for the file */
  'split-offsets'?: bigint[];

  /** Equality field IDs for equality deletes */
  'equality-ids'?: number[];

  /** Sort order ID used for file */
  'sort-order-id'?: number;
}

/**
 * =============================================================================
 * Extended Table Metadata for DoSQL
 * =============================================================================
 *
 * iceberg-js TableMetadata has optional/unknown snapshot fields.
 * We extend with strongly typed snapshot support.
 */

export interface DoSQLTableMetadata extends Omit<TableMetadata, 'snapshots' | 'snapshot-log'> {
  /** Table UUID */
  'table-uuid': string;

  /** Format version (always 2 for DoSQL) */
  'format-version': 2;

  /** Last sequence number assigned */
  'last-sequence-number': bigint;

  /** Last update timestamp */
  'last-updated-ms': bigint;

  /** Last column ID assigned */
  'last-column-id': number;

  /** Last partition ID assigned */
  'last-partition-id'?: number;

  /** All snapshots */
  snapshots: Snapshot[];

  /** Snapshot history log */
  'snapshot-log': Array<{
    'snapshot-id': bigint;
    'timestamp-ms': bigint;
  }>;

  /** Metadata history log */
  'metadata-log'?: Array<{
    'metadata-file': string;
    'timestamp-ms': bigint;
  }>;

  /** Named references (branches, tags) */
  refs?: Record<string, SnapshotRef>;

  /** Current snapshot ID */
  'current-snapshot-id': bigint | null;
}

/** Snapshot reference (branch or tag) */
export interface SnapshotRef {
  'snapshot-id': bigint;
  type: 'branch' | 'tag';
  'max-ref-age-ms'?: bigint;
  'max-snapshot-age-ms'?: bigint;
  'min-snapshots-to-keep'?: number;
}

/**
 * =============================================================================
 * DoSQL Schema to Iceberg Mapping Types
 * =============================================================================
 */

/** DoSQL column type (mirrors parser.ts ColumnType) */
export type DoSQLColumnType = 'string' | 'number' | 'boolean' | 'Date' | 'null' | 'unknown';

/** DoSQL table schema (mirrors parser.ts TableSchema) */
export type DoSQLTableSchema = Record<string, DoSQLColumnType>;

/**
 * Type-level mapping from DoSQL types to Iceberg types
 */
export type DoSQLToIcebergType<T extends DoSQLColumnType> = T extends 'string'
  ? 'string'
  : T extends 'number'
    ? 'double'
    : T extends 'boolean'
      ? 'boolean'
      : T extends 'Date'
        ? 'timestamptz'
        : T extends 'null'
          ? 'string'
          : 'binary';

/**
 * Type-level mapping from Iceberg primitive types to DoSQL types
 */
export type IcebergToDoSQLType<T extends string> = T extends 'boolean'
  ? 'boolean'
  : T extends 'int' | 'long' | 'float' | 'double'
    ? 'number'
    : T extends `decimal(${number},${number})`
      ? 'number'
      : T extends 'string' | 'uuid' | 'time'
        ? 'string'
        : T extends 'timestamp' | 'timestamptz' | 'date'
          ? 'Date'
          : 'unknown';

/**
 * =============================================================================
 * REST API Endpoint Types
 * =============================================================================
 *
 * These types define the contract for DoSQL's Iceberg REST API server.
 */

/** Generic DO storage interface */
export interface DOStorage {
  get<T>(key: string): Promise<T | undefined>;
  put<T>(key: string, value: T): Promise<void>;
  delete(key: string): Promise<void>;
  list(options?: { prefix?: string; limit?: number }): Promise<Map<string, unknown>>;
}

/**
 * REST API endpoint handlers
 *
 * These are the handlers that DoSQL implements to serve as an Iceberg catalog.
 */
export interface IcebergRestEndpoints {
  // Config
  'GET /v1/config': () => {
    overrides: Record<string, string>;
    defaults: Record<string, string>;
  };

  // Namespaces
  'GET /v1/namespaces': (storage: DOStorage) => Promise<{ namespaces: string[][] }>;
  'POST /v1/namespaces': (
    storage: DOStorage,
    body: { namespace: string[]; properties?: Record<string, string> }
  ) => Promise<{ namespace: string[]; properties: Record<string, string> }>;
  'GET /v1/namespaces/:namespace': (
    storage: DOStorage,
    namespace: string
  ) => Promise<{ namespace: string[]; properties: Record<string, string> }>;
  'DELETE /v1/namespaces/:namespace': (storage: DOStorage, namespace: string) => Promise<void>;

  // Tables
  'GET /v1/namespaces/:namespace/tables': (
    storage: DOStorage,
    namespace: string
  ) => Promise<{ identifiers: Array<{ namespace: string[]; name: string }> }>;
  'POST /v1/namespaces/:namespace/tables': (
    storage: DOStorage,
    namespace: string,
    body: unknown
  ) => Promise<TableMetadata>;
  'GET /v1/namespaces/:namespace/tables/:table': (
    storage: DOStorage,
    namespace: string,
    table: string
  ) => Promise<TableMetadata>;
  'POST /v1/namespaces/:namespace/tables/:table': (
    storage: DOStorage,
    namespace: string,
    table: string,
    body: unknown
  ) => Promise<{ 'metadata-location': string; metadata: TableMetadata }>;
  'DELETE /v1/namespaces/:namespace/tables/:table': (
    storage: DOStorage,
    namespace: string,
    table: string
  ) => Promise<void>;
}

/**
 * =============================================================================
 * Commit Types
 * =============================================================================
 *
 * Types for the Iceberg commit protocol. iceberg-js doesn't expose these
 * since it's a client library, but DoSQL as a server needs them.
 */

/** Requirement types for commit validation */
export type CommitRequirement =
  | { type: 'assert-ref-snapshot-id'; ref: string; 'snapshot-id': bigint | null }
  | { type: 'assert-table-uuid'; uuid: string }
  | { type: 'assert-last-assigned-field-id'; 'last-assigned-field-id': number }
  | { type: 'assert-current-schema-id'; 'current-schema-id': number }
  | { type: 'assert-last-assigned-partition-id'; 'last-assigned-partition-id': number }
  | { type: 'assert-default-spec-id'; 'default-spec-id': number }
  | { type: 'assert-default-sort-order-id'; 'default-sort-order-id': number };

/** Update types for commits */
export type CommitUpdate =
  | { action: 'add-snapshot'; snapshot: Omit<Snapshot, 'sequence-number'> & { 'sequence-number'?: bigint } }
  | { action: 'set-snapshot-ref'; 'ref-name': string; 'snapshot-id': bigint; type?: 'branch' | 'tag' }
  | { action: 'set-properties'; updates: Record<string, string> }
  | { action: 'remove-properties'; removals: string[] }
  | { action: 'add-schema'; schema: TableSchema; 'last-column-id'?: number }
  | { action: 'set-current-schema'; 'schema-id': number }
  | { action: 'add-partition-spec'; spec: PartitionSpec }
  | { action: 'set-default-spec'; 'spec-id': number }
  | { action: 'add-sort-order'; 'sort-order': SortOrder }
  | { action: 'set-default-sort-order'; 'sort-order-id': number }
  | { action: 'set-location'; location: string };

/** Full commit request */
export interface CommitTableRequest {
  requirements: CommitRequirement[];
  updates: CommitUpdate[];
}

/** Commit response */
export interface CommitTableResponse {
  'metadata-location': string;
  metadata: DoSQLTableMetadata;
}

/**
 * =============================================================================
 * Utility Types
 * =============================================================================
 */

/**
 * Partition transform type
 */
export type PartitionTransform =
  | 'identity'
  | 'year'
  | 'month'
  | 'day'
  | 'hour'
  | `bucket[${number}]`
  | `truncate[${number}]`
  | 'void';

/**
 * Generate partition path from partition values
 */
export function partitionPath(
  spec: PartitionSpec,
  values: Record<string, unknown>
): string {
  return spec.fields
    .map((field) => {
      const value = values[field.name];
      return `${field.name}=${value ?? '__HIVE_DEFAULT_PARTITION__'}`;
    })
    .join('/');
}

/**
 * Generate a unique snapshot ID
 */
export function generateSnapshotId(): bigint {
  // Use timestamp + random for uniqueness
  const timestamp = BigInt(Date.now());
  const random = BigInt(Math.floor(Math.random() * 1000000));
  return timestamp * 1000000n + random;
}

/**
 * Generate a metadata file path
 */
export function metadataFilePath(
  tableLocation: string,
  version: number | bigint
): string {
  return `${tableLocation}/metadata/v${version}.metadata.json`;
}

/**
 * Generate a manifest list path
 */
export function manifestListPath(
  tableLocation: string,
  snapshotId: bigint
): string {
  return `${tableLocation}/metadata/snap-${snapshotId}-manifest-list.avro`;
}

/**
 * Generate a manifest file path
 */
export function manifestFilePath(
  tableLocation: string,
  uuid: string
): string {
  return `${tableLocation}/metadata/${uuid}-manifest.avro`;
}

/**
 * Generate a data file path
 */
export function dataFilePath(
  tableLocation: string,
  partitionPath: string | null,
  filename: string
): string {
  if (partitionPath) {
    return `${tableLocation}/data/${partitionPath}/${filename}`;
  }
  return `${tableLocation}/data/${filename}`;
}
