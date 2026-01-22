/**
 * DoLake Iceberg Metadata Manager
 *
 * Manages Iceberg table metadata, snapshots, and manifests.
 * Provides the foundation for the REST Catalog API.
 */

import {
  type IcebergTableMetadata,
  type IcebergSchema,
  type IcebergSnapshot,
  type IcebergPartitionSpec,
  type IcebergSortOrder,
  type ManifestFile,
  type DataFile,
  type NamespaceIdentifier,
  type NamespaceProperties,
  type TableIdentifier,
  generateSnapshotId,
  generateUUID,
  IcebergError,
} from './types.js';

// =============================================================================
// Iceberg Path Utilities
// =============================================================================

/**
 * Generate metadata file path
 */
export function metadataFilePath(tableLocation: string, version: number): string {
  return `${tableLocation}/metadata/v${version}.metadata.json`;
}

/**
 * Generate manifest list path
 */
export function manifestListPath(tableLocation: string, snapshotId: bigint): string {
  return `${tableLocation}/metadata/snap-${snapshotId}-manifest-list.avro`;
}

/**
 * Generate manifest file path
 */
export function manifestFilePath(tableLocation: string, uuid: string): string {
  return `${tableLocation}/metadata/${uuid}-manifest.avro`;
}

/**
 * Generate data file path
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

/**
 * Generate partition path from partition values
 */
export function partitionToPath(partition: Record<string, unknown>): string | null {
  const entries = Object.entries(partition);
  if (entries.length === 0) {
    return null;
  }
  return entries
    .map(([key, value]) => `${key}=${value ?? '__HIVE_DEFAULT_PARTITION__'}`)
    .join('/');
}

// =============================================================================
// Schema Builder
// =============================================================================

/**
 * Create a new Iceberg schema
 */
export function createSchema(
  fields: Array<{
    name: string;
    type: string;
    required?: boolean;
    doc?: string;
  }>,
  schemaId: number = 0,
  identifierFieldIds?: number[]
): IcebergSchema {
  return {
    type: 'struct',
    'schema-id': schemaId,
    fields: fields.map((f, i) => ({
      id: i + 1,
      name: f.name,
      type: f.type,
      required: f.required ?? false,
      doc: f.doc,
    })),
    'identifier-field-ids': identifierFieldIds,
  };
}

/**
 * Add a field to an existing schema (creates new schema version)
 */
export function addSchemaField(
  schema: IcebergSchema,
  field: { name: string; type: string; required?: boolean; doc?: string },
  newSchemaId: number
): IcebergSchema {
  const maxId = Math.max(...schema.fields.map((f) => f.id));
  return {
    ...schema,
    'schema-id': newSchemaId,
    fields: [
      ...schema.fields,
      {
        id: maxId + 1,
        name: field.name,
        type: field.type,
        required: field.required ?? false,
        doc: field.doc,
      },
    ],
  };
}

// =============================================================================
// Partition Spec Builder
// =============================================================================

/**
 * Create an unpartitioned spec
 */
export function createUnpartitionedSpec(): IcebergPartitionSpec {
  return {
    'spec-id': 0,
    fields: [],
  };
}

/**
 * Create a partition spec with identity transform
 */
export function createIdentityPartitionSpec(
  sourceFieldIds: Array<{ sourceId: number; name: string }>,
  specId: number = 0
): IcebergPartitionSpec {
  return {
    'spec-id': specId,
    fields: sourceFieldIds.map((f, i) => ({
      'source-id': f.sourceId,
      'field-id': 1000 + i, // Partition field IDs start at 1000 by convention
      name: f.name,
      transform: 'identity',
    })),
  };
}

/**
 * Create a date-based partition spec
 */
export function createDatePartitionSpec(
  sourceFieldId: number,
  transform: 'year' | 'month' | 'day' | 'hour',
  fieldName: string,
  specId: number = 0
): IcebergPartitionSpec {
  return {
    'spec-id': specId,
    fields: [
      {
        'source-id': sourceFieldId,
        'field-id': 1000,
        name: fieldName,
        transform,
      },
    ],
  };
}

// =============================================================================
// Sort Order Builder
// =============================================================================

/**
 * Create an unsorted sort order
 */
export function createUnsortedOrder(): IcebergSortOrder {
  return {
    'order-id': 0,
    fields: [],
  };
}

/**
 * Create a sort order
 */
export function createSortOrder(
  fields: Array<{
    sourceId: number;
    direction: 'asc' | 'desc';
    nullOrder?: 'nulls-first' | 'nulls-last';
    transform?: string;
  }>,
  orderId: number = 0
): IcebergSortOrder {
  return {
    'order-id': orderId,
    fields: fields.map((f) => ({
      'source-id': f.sourceId,
      direction: f.direction,
      'null-order': f.nullOrder ?? (f.direction === 'asc' ? 'nulls-first' : 'nulls-last'),
      transform: f.transform ?? 'identity',
    })),
  };
}

// =============================================================================
// Snapshot Builder
// =============================================================================

/**
 * Create a new snapshot for an append operation
 */
export function createAppendSnapshot(
  parentSnapshotId: bigint | null,
  sequenceNumber: bigint,
  manifestListPath: string,
  addedFiles: number,
  addedRecords: bigint,
  addedFilesSize: bigint,
  schemaId?: number
): IcebergSnapshot {
  return {
    'snapshot-id': generateSnapshotId(),
    'parent-snapshot-id': parentSnapshotId,
    'sequence-number': sequenceNumber,
    'timestamp-ms': BigInt(Date.now()),
    'manifest-list': manifestListPath,
    summary: {
      operation: 'append',
      'added-data-files': String(addedFiles),
      'added-records': String(addedRecords),
      'added-files-size': String(addedFilesSize),
      'total-records': String(addedRecords),
      'total-files-size': String(addedFilesSize),
      'total-data-files': String(addedFiles),
    },
    'schema-id': schemaId,
  };
}

// =============================================================================
// Manifest Builder
// =============================================================================

/**
 * Create a manifest file entry
 */
export function createManifestFile(
  manifestPath: string,
  snapshotId: bigint,
  sequenceNumber: bigint,
  addedFilesCount: number,
  addedRowsCount: bigint,
  partitionSpecId: number = 0
): ManifestFile {
  return {
    'manifest-path': manifestPath,
    'manifest-length': BigInt(0), // Set after writing
    'partition-spec-id': partitionSpecId,
    content: 'data',
    'sequence-number': sequenceNumber,
    'min-sequence-number': sequenceNumber,
    'added-snapshot-id': snapshotId,
    'added-files-count': addedFilesCount,
    'existing-files-count': 0,
    'deleted-files-count': 0,
    'added-rows-count': addedRowsCount,
    'existing-rows-count': BigInt(0),
    'deleted-rows-count': BigInt(0),
  };
}

// =============================================================================
// Table Metadata Manager
// =============================================================================

/**
 * Create initial table metadata
 */
export function createTableMetadata(
  tableUuid: string,
  location: string,
  schema: IcebergSchema,
  partitionSpec: IcebergPartitionSpec = createUnpartitionedSpec(),
  sortOrder: IcebergSortOrder = createUnsortedOrder(),
  properties?: Record<string, string>
): IcebergTableMetadata {
  const lastColumnId = Math.max(...schema.fields.map((f) => f.id));

  return {
    'format-version': 2,
    'table-uuid': tableUuid,
    location,
    'last-sequence-number': BigInt(0),
    'last-updated-ms': BigInt(Date.now()),
    'last-column-id': lastColumnId,
    'last-partition-id': partitionSpec.fields.length > 0
      ? Math.max(...partitionSpec.fields.map((f) => f['field-id']))
      : undefined,
    'current-schema-id': schema['schema-id'],
    schemas: [schema],
    'default-spec-id': partitionSpec['spec-id'],
    'partition-specs': [partitionSpec],
    'default-sort-order-id': sortOrder['order-id'],
    'sort-orders': [sortOrder],
    properties: {
      'created-by': 'dolake',
      'write.format.default': 'parquet',
      ...properties,
    },
    'current-snapshot-id': null,
    snapshots: [],
    'snapshot-log': [],
    refs: {
      main: {
        'snapshot-id': BigInt(0),
        type: 'branch',
      },
    },
  };
}

/**
 * Add a snapshot to table metadata
 */
export function addSnapshot(
  metadata: IcebergTableMetadata,
  snapshot: IcebergSnapshot
): IcebergTableMetadata {
  const newSequenceNumber = metadata['last-sequence-number'] + BigInt(1);

  // Update snapshot with correct sequence number
  const updatedSnapshot: IcebergSnapshot = {
    ...snapshot,
    'sequence-number': newSequenceNumber,
  };

  return {
    ...metadata,
    'last-sequence-number': newSequenceNumber,
    'last-updated-ms': BigInt(Date.now()),
    'current-snapshot-id': snapshot['snapshot-id'],
    snapshots: [...metadata.snapshots, updatedSnapshot],
    'snapshot-log': [
      ...metadata['snapshot-log'],
      {
        'snapshot-id': snapshot['snapshot-id'],
        'timestamp-ms': snapshot['timestamp-ms'],
      },
    ],
    refs: {
      ...metadata.refs,
      main: {
        'snapshot-id': snapshot['snapshot-id'],
        type: 'branch',
      },
    },
  };
}

/**
 * Add a new schema version
 */
export function addSchema(
  metadata: IcebergTableMetadata,
  schema: IcebergSchema,
  setAsCurrent: boolean = true
): IcebergTableMetadata {
  const lastColumnId = Math.max(
    metadata['last-column-id'],
    ...schema.fields.map((f) => f.id)
  );

  return {
    ...metadata,
    'last-updated-ms': BigInt(Date.now()),
    'last-column-id': lastColumnId,
    'current-schema-id': setAsCurrent ? schema['schema-id'] : metadata['current-schema-id'],
    schemas: [...metadata.schemas, schema],
  };
}

// =============================================================================
// Iceberg Storage Interface
// =============================================================================

/**
 * Interface for Iceberg table storage operations
 */
export interface IcebergStorage {
  // Namespace operations
  createNamespace(namespace: NamespaceIdentifier, properties?: NamespaceProperties): Promise<void>;
  loadNamespace(namespace: NamespaceIdentifier): Promise<NamespaceProperties>;
  dropNamespace(namespace: NamespaceIdentifier): Promise<void>;
  listNamespaces(parent?: NamespaceIdentifier): Promise<NamespaceIdentifier[]>;
  namespaceExists(namespace: NamespaceIdentifier): Promise<boolean>;
  updateNamespaceProperties(
    namespace: NamespaceIdentifier,
    updates: NamespaceProperties,
    removals: string[]
  ): Promise<void>;

  // Table operations
  createTable(identifier: TableIdentifier, metadata: IcebergTableMetadata): Promise<void>;
  loadTable(identifier: TableIdentifier): Promise<IcebergTableMetadata>;
  dropTable(identifier: TableIdentifier, purge?: boolean): Promise<void>;
  listTables(namespace: NamespaceIdentifier): Promise<TableIdentifier[]>;
  tableExists(identifier: TableIdentifier): Promise<boolean>;
  commitTable(
    identifier: TableIdentifier,
    metadata: IcebergTableMetadata,
    requirements?: CommitRequirement[]
  ): Promise<IcebergTableMetadata>;

  // Data file operations
  writeDataFile(path: string, content: Uint8Array): Promise<void>;
  readDataFile(path: string): Promise<Uint8Array>;
  deleteDataFile(path: string): Promise<void>;
  listDataFiles(prefix: string): Promise<string[]>;

  // Metadata file operations
  writeMetadata(path: string, metadata: IcebergTableMetadata): Promise<void>;
  readMetadata(path: string): Promise<IcebergTableMetadata>;
}

/**
 * Commit requirement types
 */
export type CommitRequirement =
  | { type: 'assert-ref-snapshot-id'; ref: string; 'snapshot-id': bigint | null }
  | { type: 'assert-table-uuid'; uuid: string }
  | { type: 'assert-last-assigned-field-id'; 'last-assigned-field-id': number }
  | { type: 'assert-current-schema-id'; 'current-schema-id': number }
  | { type: 'assert-default-spec-id'; 'default-spec-id': number }
  | { type: 'assert-default-sort-order-id'; 'default-sort-order-id': number };

// =============================================================================
// R2 Iceberg Storage Implementation
// =============================================================================

/**
 * R2-backed Iceberg storage implementation
 */
export class R2IcebergStorage implements IcebergStorage {
  private bucket: R2Bucket;
  private basePath: string;
  private doStorage: DurableObjectStorage;

  constructor(bucket: R2Bucket, basePath: string, doStorage: DurableObjectStorage) {
    this.bucket = bucket;
    this.basePath = basePath;
    this.doStorage = doStorage;
  }

  // ===========================================================================
  // Namespace Operations
  // ===========================================================================

  async createNamespace(
    namespace: NamespaceIdentifier,
    properties: NamespaceProperties = {}
  ): Promise<void> {
    const key = this.namespaceKey(namespace);
    const existing = await this.doStorage.get<NamespaceProperties>(key);
    if (existing) {
      throw new IcebergError(`Namespace already exists: ${namespace.join('.')}`);
    }
    await this.doStorage.put(key, properties);
  }

  async loadNamespace(namespace: NamespaceIdentifier): Promise<NamespaceProperties> {
    const key = this.namespaceKey(namespace);
    const properties = await this.doStorage.get<NamespaceProperties>(key);
    if (!properties) {
      throw new IcebergError(`Namespace not found: ${namespace.join('.')}`);
    }
    return properties;
  }

  async dropNamespace(namespace: NamespaceIdentifier): Promise<void> {
    // Check if namespace has tables
    const tables = await this.listTables(namespace);
    if (tables.length > 0) {
      throw new IcebergError(`Cannot drop non-empty namespace: ${namespace.join('.')}`);
    }
    await this.doStorage.delete(this.namespaceKey(namespace));
  }

  async listNamespaces(parent?: NamespaceIdentifier): Promise<NamespaceIdentifier[]> {
    const prefix = parent ? `namespace:${parent.join('.')}:` : 'namespace:';
    const entries = await this.doStorage.list({ prefix });
    const namespaces: NamespaceIdentifier[] = [];

    for (const key of entries.keys()) {
      const parts = key.replace('namespace:', '').split(':')[0].split('.');
      if (!parent || (parts.length === parent.length + 1 &&
          parts.slice(0, parent.length).join('.') === parent.join('.'))) {
        namespaces.push(parts);
      }
    }

    return namespaces;
  }

  async namespaceExists(namespace: NamespaceIdentifier): Promise<boolean> {
    const key = this.namespaceKey(namespace);
    const properties = await this.doStorage.get(key);
    return properties !== undefined;
  }

  async updateNamespaceProperties(
    namespace: NamespaceIdentifier,
    updates: NamespaceProperties,
    removals: string[]
  ): Promise<void> {
    const key = this.namespaceKey(namespace);
    const current = await this.doStorage.get<NamespaceProperties>(key);
    if (!current) {
      throw new IcebergError(`Namespace not found: ${namespace.join('.')}`);
    }

    const updated = { ...current, ...updates };
    for (const removal of removals) {
      delete updated[removal];
    }

    await this.doStorage.put(key, updated);
  }

  // ===========================================================================
  // Table Operations
  // ===========================================================================

  async createTable(
    identifier: TableIdentifier,
    metadata: IcebergTableMetadata
  ): Promise<void> {
    // Ensure namespace exists
    const nsExists = await this.namespaceExists(identifier.namespace);
    if (!nsExists) {
      throw new IcebergError(`Namespace not found: ${identifier.namespace.join('.')}`);
    }

    // Check table doesn't exist
    const tableKey = this.tableKey(identifier);
    const existing = await this.doStorage.get(tableKey);
    if (existing) {
      throw new IcebergError(`Table already exists: ${this.tableIdentifierToString(identifier)}`);
    }

    // Write metadata to R2
    const metadataPath = metadataFilePath(metadata.location, 1);
    await this.writeMetadata(metadataPath, metadata);

    // Store table reference in DO storage
    await this.doStorage.put(tableKey, {
      location: metadata.location,
      'metadata-location': metadataPath,
      'table-uuid': metadata['table-uuid'],
    });
  }

  async loadTable(identifier: TableIdentifier): Promise<IcebergTableMetadata> {
    const tableKey = this.tableKey(identifier);
    const tableRef = await this.doStorage.get<{
      location: string;
      'metadata-location': string;
    }>(tableKey);

    if (!tableRef) {
      throw new IcebergError(`Table not found: ${this.tableIdentifierToString(identifier)}`);
    }

    return this.readMetadata(tableRef['metadata-location']);
  }

  async dropTable(identifier: TableIdentifier, purge: boolean = false): Promise<void> {
    const tableKey = this.tableKey(identifier);
    const tableRef = await this.doStorage.get<{
      location: string;
      'metadata-location': string;
    }>(tableKey);

    if (!tableRef) {
      throw new IcebergError(`Table not found: ${this.tableIdentifierToString(identifier)}`);
    }

    // Delete table reference
    await this.doStorage.delete(tableKey);

    // If purge, delete all data and metadata files
    if (purge) {
      const files = await this.listDataFiles(tableRef.location);
      for (const file of files) {
        await this.deleteDataFile(file);
      }
    }
  }

  async listTables(namespace: NamespaceIdentifier): Promise<TableIdentifier[]> {
    const prefix = `table:${namespace.join('.')}:`;
    const entries = await this.doStorage.list({ prefix });
    const tables: TableIdentifier[] = [];

    for (const key of entries.keys()) {
      const tableName = key.replace(prefix, '');
      tables.push({ namespace, name: tableName });
    }

    return tables;
  }

  async tableExists(identifier: TableIdentifier): Promise<boolean> {
    const tableKey = this.tableKey(identifier);
    const tableRef = await this.doStorage.get(tableKey);
    return tableRef !== undefined;
  }

  async commitTable(
    identifier: TableIdentifier,
    metadata: IcebergTableMetadata,
    requirements: CommitRequirement[] = []
  ): Promise<IcebergTableMetadata> {
    const tableKey = this.tableKey(identifier);
    const tableRef = await this.doStorage.get<{
      location: string;
      'metadata-location': string;
      'table-uuid': string;
    }>(tableKey);

    if (!tableRef) {
      throw new IcebergError(`Table not found: ${this.tableIdentifierToString(identifier)}`);
    }

    // Load current metadata for validation
    const currentMetadata = await this.readMetadata(tableRef['metadata-location']);

    // Validate requirements
    for (const req of requirements) {
      this.validateRequirement(req, currentMetadata);
    }

    // Determine next version
    const currentVersion = this.extractVersion(tableRef['metadata-location']);
    const nextVersion = currentVersion + 1;

    // Write new metadata
    const newMetadataPath = metadataFilePath(metadata.location, nextVersion);
    await this.writeMetadata(newMetadataPath, metadata);

    // Update table reference
    await this.doStorage.put(tableKey, {
      location: metadata.location,
      'metadata-location': newMetadataPath,
      'table-uuid': metadata['table-uuid'],
    });

    return metadata;
  }

  // ===========================================================================
  // Data File Operations
  // ===========================================================================

  async writeDataFile(path: string, content: Uint8Array): Promise<void> {
    const fullPath = path.startsWith(this.basePath) ? path : `${this.basePath}/${path}`;
    await this.bucket.put(fullPath, content, {
      httpMetadata: {
        contentType: 'application/octet-stream',
      },
    });
  }

  async readDataFile(path: string): Promise<Uint8Array> {
    const fullPath = path.startsWith(this.basePath) ? path : `${this.basePath}/${path}`;
    const object = await this.bucket.get(fullPath);
    if (!object) {
      throw new IcebergError(`Data file not found: ${path}`);
    }
    return new Uint8Array(await object.arrayBuffer());
  }

  async deleteDataFile(path: string): Promise<void> {
    const fullPath = path.startsWith(this.basePath) ? path : `${this.basePath}/${path}`;
    await this.bucket.delete(fullPath);
  }

  async listDataFiles(prefix: string): Promise<string[]> {
    const fullPrefix = prefix.startsWith(this.basePath) ? prefix : `${this.basePath}/${prefix}`;
    const listed = await this.bucket.list({ prefix: fullPrefix });
    return listed.objects.map((o) => o.key);
  }

  // ===========================================================================
  // Metadata File Operations
  // ===========================================================================

  async writeMetadata(path: string, metadata: IcebergTableMetadata): Promise<void> {
    const fullPath = path.startsWith(this.basePath) ? path : `${this.basePath}/${path}`;
    const json = JSON.stringify(metadata, (_, v) =>
      typeof v === 'bigint' ? v.toString() : v
    );
    await this.bucket.put(fullPath, json, {
      httpMetadata: {
        contentType: 'application/json',
      },
    });
  }

  async readMetadata(path: string): Promise<IcebergTableMetadata> {
    const fullPath = path.startsWith(this.basePath) ? path : `${this.basePath}/${path}`;
    const object = await this.bucket.get(fullPath);
    if (!object) {
      throw new IcebergError(`Metadata file not found: ${path}`);
    }
    const json = await object.text();
    return JSON.parse(json, (_, v) => {
      // Convert numeric strings that look like bigints back to bigint
      if (typeof v === 'string' && /^\d{15,}$/.test(v)) {
        return BigInt(v);
      }
      return v;
    });
  }

  // ===========================================================================
  // Helper Methods
  // ===========================================================================

  private namespaceKey(namespace: NamespaceIdentifier): string {
    return `namespace:${namespace.join('.')}`;
  }

  private tableKey(identifier: TableIdentifier): string {
    return `table:${identifier.namespace.join('.')}:${identifier.name}`;
  }

  private tableIdentifierToString(identifier: TableIdentifier): string {
    return `${identifier.namespace.join('.')}.${identifier.name}`;
  }

  private extractVersion(metadataPath: string): number {
    const match = metadataPath.match(/v(\d+)\.metadata\.json$/);
    return match ? parseInt(match[1], 10) : 0;
  }

  private validateRequirement(
    req: CommitRequirement,
    metadata: IcebergTableMetadata
  ): void {
    switch (req.type) {
      case 'assert-ref-snapshot-id':
        if (req.ref === 'main') {
          if (metadata['current-snapshot-id'] !== req['snapshot-id']) {
            throw new IcebergError(
              `Commit conflict: expected snapshot ${req['snapshot-id']}, ` +
              `but found ${metadata['current-snapshot-id']}`
            );
          }
        }
        break;

      case 'assert-table-uuid':
        if (metadata['table-uuid'] !== req.uuid) {
          throw new IcebergError(
            `Table UUID mismatch: expected ${req.uuid}, but found ${metadata['table-uuid']}`
          );
        }
        break;

      case 'assert-last-assigned-field-id':
        if (metadata['last-column-id'] !== req['last-assigned-field-id']) {
          throw new IcebergError(
            `Last column ID mismatch: expected ${req['last-assigned-field-id']}, ` +
            `but found ${metadata['last-column-id']}`
          );
        }
        break;

      case 'assert-current-schema-id':
        if (metadata['current-schema-id'] !== req['current-schema-id']) {
          throw new IcebergError(
            `Current schema ID mismatch: expected ${req['current-schema-id']}, ` +
            `but found ${metadata['current-schema-id']}`
          );
        }
        break;

      case 'assert-default-spec-id':
        if (metadata['default-spec-id'] !== req['default-spec-id']) {
          throw new IcebergError(
            `Default partition spec ID mismatch: expected ${req['default-spec-id']}, ` +
            `but found ${metadata['default-spec-id']}`
          );
        }
        break;

      case 'assert-default-sort-order-id':
        if (metadata['default-sort-order-id'] !== req['default-sort-order-id']) {
          throw new IcebergError(
            `Default sort order ID mismatch: expected ${req['default-sort-order-id']}, ` +
            `but found ${metadata['default-sort-order-id']}`
          );
        }
        break;
    }
  }
}
