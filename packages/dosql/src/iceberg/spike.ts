/**
 * Iceberg Integration Spike for DoSQL
 *
 * =============================================================================
 * RECOMMENDATION (TL;DR)
 * =============================================================================
 *
 * DoSQL should BE an Iceberg REST Catalog server AND USE iceberg-js for client ops.
 *
 * Why BOTH?
 * 1. Server: DoSQL Durable Objects can expose REST Catalog endpoints, allowing
 *    Spark, Flink, DuckDB, Trino, etc. to query DoSQL tables via standard Iceberg API.
 *
 * 2. Client: When DoSQL needs to federate queries across external Iceberg tables
 *    (e.g., Snowflake, AWS Glue, Tabular), iceberg-js provides the catalog client.
 *
 * 3. Types: iceberg-js types are excellent for schema/metadata but lack snapshot
 *    and manifest types (it's catalog-only, not data). We extend with our own.
 *
 * Architecture:
 *
 *   External Query Engines          DoSQL Durable Object          External Catalogs
 *   (Spark, DuckDB, Trino)              (Worker)                  (Snowflake, S3)
 *          |                               |                            |
 *          |  REST Catalog API             |                            |
 *          +------------------------------>|                            |
 *          |  (DoSQL as SERVER)            |                            |
 *          |                               |   iceberg-js client        |
 *          |                               +--------------------------->|
 *          |                               |   (DoSQL as CLIENT)        |
 *          |                               |                            |
 *
 * Data Flow:
 * - DoSQL writes Parquet to R2 via hyparquet-writer
 * - DoSQL maintains Iceberg metadata (snapshots, manifests) in DO storage
 * - DoSQL exposes REST Catalog API for external engines to discover tables
 * - DoSQL can read from external Iceberg catalogs via iceberg-js
 *
 * =============================================================================
 */

// Re-export useful types from iceberg-js for consistent usage
export type {
  // Schema types - use these for all schema definitions
  TableSchema as IcebergTableSchema,
  StructField as IcebergStructField,
  TableField as IcebergTableField,
  IcebergType,
  PrimitiveType as IcebergPrimitiveType,
  StructType as IcebergStructType,
  ListType as IcebergListType,
  MapType as IcebergMapType,
  PrimitiveTypeValue as IcebergPrimitiveTypeValue,

  // Partition & Sort types
  PartitionSpec as PartitionSpec,
  PartitionField as IcebergPartitionField,
  SortOrder as SortOrder,
  SortField as IcebergSortField,

  // Table metadata (partial - lacks snapshot details)
  TableMetadata as TableMetadata,

  // Namespace types
  NamespaceIdentifier as IcebergNamespaceIdentifier,
  NamespaceMetadata as IcebergNamespaceMetadata,

  // Request/Response types
  CreateTableRequest as CreateTableRequest,
  UpdateTableRequest as IcebergUpdateTableRequest,
  DropTableRequest as IcebergDropTableRequest,
  CommitTableResponse as IcebergCommitTableResponse,
  CreateNamespaceResponse as IcebergCreateNamespaceResponse,

  // Table identifier
  TableIdentifier as IcebergTableIdentifier,

  // Auth config
  AuthConfig as IcebergAuthConfig,
  AccessDelegation as IcebergAccessDelegation,
  IcebergRestCatalogOptions,
} from 'iceberg-js';

// Re-export client and utilities
export {
  IcebergRestCatalog,
  IcebergError,
  getCurrentSchema,
  parseDecimalType,
  parseFixedType,
  isDecimalType,
  isFixedType,
  typesEqual,
} from 'iceberg-js';

// Import our extended types
import type {
  Snapshot,
  ManifestFile,
  ManifestEntry,
  DataFile,
  DoSQLTableMetadata,
  IcebergRestEndpoints,
} from './types.js';

// Re-export extended types
export type {
  Snapshot,
  ManifestFile,
  ManifestEntry,
  DataFile,
  DoSQLTableMetadata,
  IcebergRestEndpoints,
} from './types.js';

// Import for implementation
// Note: Using original iceberg-js names for internal imports
import type {
  TableSchema,
  StructField,
  PartitionSpec,
  SortOrder,
  TableMetadata,
  NamespaceIdentifier,
  CreateTableRequest,
} from 'iceberg-js';
import { IcebergRestCatalog } from 'iceberg-js';

/**
 * =============================================================================
 * DoSQL <-> Iceberg Schema Mapping
 * =============================================================================
 *
 * DoSQL uses simple TypeScript-friendly types. Iceberg uses richer types.
 * This mapper handles the conversion both ways.
 */

/** DoSQL column type (from parser.ts) */
export type DoSQLColumnType = 'string' | 'number' | 'boolean' | 'Date' | 'null' | 'unknown';

/** DoSQL table schema (from parser.ts) */
export type DoSQLTableSchema = Record<string, DoSQLColumnType>;

/**
 * Convert DoSQL column type to Iceberg primitive type
 */
export function dosqlTypeToIceberg(type: DoSQLColumnType): string {
  switch (type) {
    case 'string':
      return 'string';
    case 'number':
      return 'double'; // Default to double for flexibility; could use 'long' for integers
    case 'boolean':
      return 'boolean';
    case 'Date':
      return 'timestamptz';
    case 'null':
      return 'string'; // Nullable string as fallback
    case 'unknown':
      return 'binary'; // Binary blob for unknown types
    default:
      return 'string';
  }
}

/**
 * Convert Iceberg primitive type to DoSQL column type
 */
export function icebergTypeToDosql(type: string): DoSQLColumnType {
  // Handle primitive types
  const primitiveMap: Record<string, DoSQLColumnType> = {
    boolean: 'boolean',
    int: 'number',
    long: 'number',
    float: 'number',
    double: 'number',
    string: 'string',
    timestamp: 'Date',
    timestamptz: 'Date',
    date: 'Date',
    time: 'string', // Time as string HH:MM:SS
    uuid: 'string',
    binary: 'unknown',
  };

  // Check for decimal(p,s) pattern
  if (type.startsWith('decimal(')) {
    return 'number';
  }

  // Check for fixed[L] pattern
  if (type.startsWith('fixed[')) {
    return 'unknown';
  }

  return primitiveMap[type] ?? 'unknown';
}

/**
 * Convert DoSQL table schema to Iceberg schema
 */
export function dosqlSchemaToIceberg(
  schema: DoSQLTableSchema,
  options?: {
    schemaId?: number;
    identifierFieldIds?: number[];
  }
): TableSchema {
  const fields: StructField[] = [];
  let fieldId = 1;

  for (const [name, type] of Object.entries(schema)) {
    fields.push({
      id: fieldId,
      name,
      type: dosqlTypeToIceberg(type),
      required: type !== 'null', // null type columns are optional
    });
    fieldId++;
  }

  return {
    type: 'struct',
    fields,
    'schema-id': options?.schemaId ?? 0,
    'identifier-field-ids': options?.identifierFieldIds,
  };
}

/**
 * Convert Iceberg schema to DoSQL table schema
 */
export function icebergSchemaToDosql(schema: TableSchema): DoSQLTableSchema {
  const dosqlSchema: DoSQLTableSchema = {};

  for (const field of schema.fields) {
    if (typeof field.type === 'string') {
      dosqlSchema[field.name] = icebergTypeToDosql(field.type);
    } else {
      // Complex types (struct, list, map) become 'unknown' in DoSQL
      dosqlSchema[field.name] = 'unknown';
    }
  }

  return dosqlSchema;
}

/**
 * =============================================================================
 * Dual Client/Server Wrapper
 * =============================================================================
 *
 * This wrapper allows DoSQL to operate in both client and server modes:
 * - Client mode: Connect to external Iceberg catalogs
 * - Server mode: Expose DoSQL tables as Iceberg REST API
 */

export interface DoSQLIcebergConfig {
  /** Client configuration for connecting to external catalogs */
  client?: {
    baseUrl: string;
    catalogName?: string;
    auth?: {
      type: 'none' | 'bearer' | 'header' | 'custom';
      token?: string;
      headerName?: string;
      headerValue?: string;
      getHeaders?: () => Record<string, string> | Promise<Record<string, string>>;
    };
  };

  /** Server configuration for exposing DoSQL as a catalog */
  server?: {
    /** Warehouse location prefix (e.g., 'r2://bucket/warehouse') */
    warehouseLocation: string;
    /** Enable metrics endpoint */
    enableMetrics?: boolean;
  };
}

/**
 * DoSQL Iceberg integration wrapper
 *
 * Provides both client and server functionality for Iceberg interoperability.
 */
export class DoSQLIceberg {
  private clientCatalog?: IcebergRestCatalog;
  private serverConfig?: DoSQLIcebergConfig['server'];

  constructor(config: DoSQLIcebergConfig) {
    // Initialize client if config provided
    if (config.client) {
      const auth = config.client.auth;
      let authConfig: { type: 'none' } | { type: 'bearer'; token: string } | { type: 'header'; name: string; value: string } | { type: 'custom'; getHeaders: () => Record<string, string> | Promise<Record<string, string>> };

      if (!auth || auth.type === 'none') {
        authConfig = { type: 'none' };
      } else if (auth.type === 'bearer' && auth.token) {
        authConfig = { type: 'bearer', token: auth.token };
      } else if (auth.type === 'header' && auth.headerName && auth.headerValue) {
        authConfig = { type: 'header', name: auth.headerName, value: auth.headerValue };
      } else if (auth.type === 'custom' && auth.getHeaders) {
        authConfig = { type: 'custom', getHeaders: auth.getHeaders };
      } else {
        authConfig = { type: 'none' };
      }

      this.clientCatalog = new IcebergRestCatalog({
        baseUrl: config.client.baseUrl,
        catalogName: config.client.catalogName,
        auth: authConfig,
      });
    }

    // Store server config
    this.serverConfig = config.server;
  }

  /**
   * Get the Iceberg client for connecting to external catalogs.
   */
  get client(): IcebergRestCatalog | undefined {
    return this.clientCatalog;
  }

  /**
   * Check if client mode is enabled
   */
  get hasClient(): boolean {
    return !!this.clientCatalog;
  }

  /**
   * Check if server mode is enabled
   */
  get hasServer(): boolean {
    return !!this.serverConfig;
  }

  /**
   * =============================================================================
   * CLIENT OPERATIONS (DoSQL as consumer of external Iceberg catalogs)
   * =============================================================================
   */

  /**
   * Load an external table's schema and convert to DoSQL format.
   * This enables federated queries across Iceberg tables.
   */
  async loadExternalTableSchema(
    namespace: string[],
    tableName: string
  ): Promise<{ dosqlSchema: DoSQLTableSchema; icebergMetadata: TableMetadata }> {
    if (!this.clientCatalog) {
      throw new Error('Iceberg client not configured');
    }

    const metadata = await this.clientCatalog.loadTable({
      namespace,
      name: tableName,
    });

    // Get current schema
    const currentSchemaId = metadata['current-schema-id'];
    const currentSchema = metadata.schemas.find((s) => s['schema-id'] === currentSchemaId);

    if (!currentSchema) {
      throw new Error(`Schema with id ${currentSchemaId} not found in table metadata`);
    }

    return {
      dosqlSchema: icebergSchemaToDosql(currentSchema),
      icebergMetadata: metadata,
    };
  }

  /**
   * List tables in an external namespace
   */
  async listExternalTables(namespace: string[]): Promise<{ namespace: string[]; name: string }[]> {
    if (!this.clientCatalog) {
      throw new Error('Iceberg client not configured');
    }

    return this.clientCatalog.listTables({ namespace });
  }

  /**
   * =============================================================================
   * SERVER OPERATIONS (DoSQL as Iceberg REST Catalog provider)
   * =============================================================================
   */

  /**
   * Get REST API endpoint handlers for DoSQL tables.
   *
   * These can be mounted on a Hono app to expose DoSQL as an Iceberg catalog.
   *
   * Usage:
   * ```ts
   * const app = new Hono();
   * const iceberg = new DoSQLIceberg({ server: { warehouseLocation: 'r2://...' } });
   *
   * // Mount Iceberg REST API endpoints
   * app.route('/v1', iceberg.createRestHandlers(storage));
   * ```
   */
  getRestEndpoints(): IcebergRestEndpoints {
    if (!this.serverConfig) {
      throw new Error('Iceberg server not configured');
    }

    return {
      // Configuration endpoint
      'GET /v1/config': () => ({
        overrides: {},
        defaults: {
          'warehouse-location': this.serverConfig!.warehouseLocation,
        },
      }),

      // Namespace endpoints
      'GET /v1/namespaces': async (_storage) => {
        // Implementation would query DO storage for namespaces
        return { namespaces: [] };
      },

      'POST /v1/namespaces': async (_storage, _body) => {
        // Implementation would create namespace in DO storage
        return { namespace: [], properties: {} };
      },

      'GET /v1/namespaces/:namespace': async (_storage, _namespace) => {
        // Implementation would load namespace from DO storage
        return { namespace: [], properties: {} };
      },

      'DELETE /v1/namespaces/:namespace': async (_storage, _namespace) => {
        // Implementation would delete namespace from DO storage
      },

      // Table endpoints
      'GET /v1/namespaces/:namespace/tables': async (_storage, _namespace) => {
        // Implementation would list tables from DO storage
        return { identifiers: [] };
      },

      'POST /v1/namespaces/:namespace/tables': async (_storage, _namespace, _body) => {
        // Implementation would create table in DO storage
        return {} as TableMetadata;
      },

      'GET /v1/namespaces/:namespace/tables/:table': async (_storage, _namespace, _table) => {
        // Implementation would load table from DO storage
        return {} as TableMetadata;
      },

      'POST /v1/namespaces/:namespace/tables/:table': async (_storage, _namespace, _table, _body) => {
        // Implementation would commit to table in DO storage
        return { 'metadata-location': '', metadata: {} as TableMetadata };
      },

      'DELETE /v1/namespaces/:namespace/tables/:table': async (_storage, _namespace, _table) => {
        // Implementation would drop table from DO storage
      },
    };
  }

  /**
   * Convert a DoSQL table definition to Iceberg table creation request.
   *
   * This is the key integration point: when a DoSQL table is created,
   * we can automatically create the corresponding Iceberg metadata.
   */
  createTableRequest(
    tableName: string,
    dosqlSchema: DoSQLTableSchema,
    options?: {
      partitionSpec?: PartitionSpec;
      sortOrder?: SortOrder;
      properties?: Record<string, string>;
    }
  ): CreateTableRequest {
    return {
      name: tableName,
      schema: dosqlSchemaToIceberg(dosqlSchema),
      'partition-spec': options?.partitionSpec,
      'write-order': options?.sortOrder,
      properties: {
        'created-by': 'dosql',
        'write.format.default': 'parquet',
        ...options?.properties,
      },
    };
  }
}

/**
 * =============================================================================
 * Factory Functions
 * =============================================================================
 */

/**
 * Create a DoSQL Iceberg client for connecting to external catalogs.
 */
export function createIcebergClient(
  baseUrl: string,
  options?: {
    catalogName?: string;
    token?: string;
  }
): DoSQLIceberg {
  return new DoSQLIceberg({
    client: {
      baseUrl,
      catalogName: options?.catalogName,
      auth: options?.token ? { type: 'bearer', token: options.token } : { type: 'none' },
    },
  });
}

/**
 * Create a DoSQL Iceberg server for exposing tables via REST API.
 */
export function createIcebergServer(warehouseLocation: string): DoSQLIceberg {
  return new DoSQLIceberg({
    server: {
      warehouseLocation,
    },
  });
}

/**
 * Create a dual-mode DoSQL Iceberg instance (both client and server).
 */
export function createIcebergDual(config: DoSQLIcebergConfig): DoSQLIceberg {
  return new DoSQLIceberg(config);
}
