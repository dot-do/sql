/**
 * DoLake REST Catalog API
 *
 * Implements the Iceberg REST Catalog specification for external query engines.
 * Allows Spark, DuckDB, Trino, etc. to query DoLake tables via standard Iceberg API.
 *
 * Spec: https://iceberg.apache.org/spec/#rest-catalog
 */

import {
  type IcebergTableMetadata,
  type IcebergSchema,
  type IcebergPartitionSpec,
  type IcebergSortOrder,
  type NamespaceIdentifier,
  type NamespaceProperties,
  type TableIdentifier,
  IcebergError,
  generateUUID,
} from './types.js';
import {
  type IcebergStorage,
  type CommitRequirement,
  createTableMetadata,
  createSchema,
  createUnpartitionedSpec,
  createUnsortedOrder,
  addSnapshot,
  addSchema,
} from './iceberg.js';

// =============================================================================
// REST Catalog Types
// =============================================================================

/**
 * Catalog configuration response
 */
export interface CatalogConfig {
  overrides: Record<string, string>;
  defaults: Record<string, string>;
}

/**
 * List namespaces response
 */
export interface ListNamespacesResponse {
  namespaces: NamespaceIdentifier[];
  'next-page-token'?: string;
}

/**
 * Create namespace request
 */
export interface CreateNamespaceRequest {
  namespace: NamespaceIdentifier;
  properties?: NamespaceProperties;
}

/**
 * Create namespace response
 */
export interface CreateNamespaceResponse {
  namespace: NamespaceIdentifier;
  properties: NamespaceProperties;
}

/**
 * Get namespace response
 */
export interface GetNamespaceResponse {
  namespace: NamespaceIdentifier;
  properties: NamespaceProperties;
}

/**
 * Update namespace properties request
 */
export interface UpdateNamespacePropertiesRequest {
  updates?: NamespaceProperties;
  removals?: string[];
}

/**
 * Update namespace properties response
 */
export interface UpdateNamespacePropertiesResponse {
  updated: string[];
  removed: string[];
  missing: string[];
}

/**
 * List tables response
 */
export interface ListTablesResponse {
  identifiers: TableIdentifier[];
  'next-page-token'?: string;
}

/**
 * Create table request
 */
export interface CreateTableRequest {
  name: string;
  schema: IcebergSchema;
  'partition-spec'?: IcebergPartitionSpec;
  'write-order'?: IcebergSortOrder;
  'stage-create'?: boolean;
  properties?: Record<string, string>;
}

/**
 * Load table response
 */
export interface LoadTableResponse {
  'metadata-location'?: string;
  metadata: IcebergTableMetadata;
  config?: Record<string, string>;
}

/**
 * Commit table request
 */
export interface CommitTableRequest {
  requirements: CommitRequirement[];
  updates: TableUpdate[];
}

/**
 * Table update types
 */
export type TableUpdate =
  | { action: 'add-snapshot'; snapshot: unknown }
  | { action: 'set-snapshot-ref'; 'ref-name': string; 'snapshot-id': bigint; type?: 'branch' | 'tag' }
  | { action: 'set-properties'; updates: Record<string, string> }
  | { action: 'remove-properties'; removals: string[] }
  | { action: 'add-schema'; schema: IcebergSchema; 'last-column-id'?: number }
  | { action: 'set-current-schema'; 'schema-id': number }
  | { action: 'add-partition-spec'; spec: IcebergPartitionSpec }
  | { action: 'set-default-spec'; 'spec-id': number }
  | { action: 'add-sort-order'; 'sort-order': IcebergSortOrder }
  | { action: 'set-default-sort-order'; 'sort-order-id': number }
  | { action: 'set-location'; location: string };

/**
 * Commit table response
 */
export interface CommitTableResponse {
  'metadata-location': string;
  metadata: IcebergTableMetadata;
}

/**
 * Error response
 */
export interface ErrorResponse {
  error: {
    message: string;
    type: string;
    code: number;
    stack?: string[];
  };
}

// =============================================================================
// REST Catalog Handler
// =============================================================================

/**
 * REST Catalog configuration
 */
export interface RestCatalogConfig {
  /** Warehouse location */
  warehouseLocation: string;
  /** Catalog name */
  catalogName: string;
  /** Default properties */
  defaultProperties?: Record<string, string>;
  /** Enable metrics */
  enableMetrics?: boolean;
}

/**
 * REST Catalog Handler
 *
 * Handles Iceberg REST Catalog API requests.
 */
export class RestCatalogHandler {
  private readonly storage: IcebergStorage;
  private readonly config: RestCatalogConfig;

  constructor(storage: IcebergStorage, config: RestCatalogConfig) {
    this.storage = storage;
    this.config = config;
  }

  // ===========================================================================
  // Request Router
  // ===========================================================================

  /**
   * Handle an HTTP request
   */
  async handleRequest(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const method = request.method;
    const path = url.pathname;

    try {
      // Configuration
      if (path === '/v1/config' && method === 'GET') {
        return this.jsonResponse(this.getConfig());
      }

      // Namespaces
      if (path === '/v1/namespaces' && method === 'GET') {
        return this.jsonResponse(await this.listNamespaces(url.searchParams));
      }
      if (path === '/v1/namespaces' && method === 'POST') {
        const body = await request.json() as CreateNamespaceRequest;
        return this.jsonResponse(await this.createNamespace(body), 200);
      }

      // Namespace by ID
      const nsMatch = path.match(/^\/v1\/namespaces\/([^/]+)$/);
      if (nsMatch && nsMatch[1]) {
        const namespace = this.decodeNamespace(nsMatch[1]);
        if (method === 'GET') {
          return this.jsonResponse(await this.getNamespace(namespace));
        }
        if (method === 'DELETE') {
          await this.dropNamespace(namespace);
          return new Response(null, { status: 204 });
        }
        if (method === 'POST') {
          const body = await request.json() as UpdateNamespacePropertiesRequest;
          return this.jsonResponse(await this.updateNamespaceProperties(namespace, body));
        }
      }

      // Tables list
      const tablesMatch = path.match(/^\/v1\/namespaces\/([^/]+)\/tables$/);
      if (tablesMatch && tablesMatch[1]) {
        const namespace = this.decodeNamespace(tablesMatch[1]);
        if (method === 'GET') {
          return this.jsonResponse(await this.listTables(namespace));
        }
        if (method === 'POST') {
          const body = await request.json() as CreateTableRequest;
          return this.jsonResponse(await this.createTable(namespace, body), 200);
        }
      }

      // Table by ID
      const tableMatch = path.match(/^\/v1\/namespaces\/([^/]+)\/tables\/([^/]+)$/);
      if (tableMatch && tableMatch[1] && tableMatch[2]) {
        const namespace = this.decodeNamespace(tableMatch[1]);
        const tableName = decodeURIComponent(tableMatch[2]);
        const identifier = { namespace, name: tableName };

        if (method === 'GET') {
          return this.jsonResponse(await this.loadTable(identifier));
        }
        if (method === 'DELETE') {
          const purge = url.searchParams.get('purgeRequested') === 'true';
          await this.dropTable(identifier, purge);
          return new Response(null, { status: 204 });
        }
        if (method === 'POST') {
          const body = await request.json() as CommitTableRequest;
          return this.jsonResponse(await this.commitTable(identifier, body));
        }
      }

      // Table exists (HEAD)
      if (tableMatch && tableMatch[1] && tableMatch[2] && method === 'HEAD') {
        const namespace = this.decodeNamespace(tableMatch[1]);
        const tableName = decodeURIComponent(tableMatch[2]);
        const identifier = { namespace, name: tableName };
        const exists = await this.storage.tableExists(identifier);
        return new Response(null, { status: exists ? 204 : 404 });
      }

      // Namespace exists (HEAD)
      if (nsMatch && nsMatch[1] && method === 'HEAD') {
        const namespace = this.decodeNamespace(nsMatch[1]);
        const exists = await this.storage.namespaceExists(namespace);
        return new Response(null, { status: exists ? 204 : 404 });
      }

      return this.errorResponse(404, 'NotFoundError', `Unknown endpoint: ${method} ${path}`);
    } catch (error) {
      if (error instanceof IcebergError) {
        const code = this.errorToStatusCode(error);
        return this.errorResponse(code, error.code, error.message);
      }
      console.error('Unhandled error:', error);
      return this.errorResponse(500, 'InternalError', String(error));
    }
  }

  // ===========================================================================
  // Configuration Endpoint
  // ===========================================================================

  /**
   * GET /v1/config
   */
  getConfig(): CatalogConfig {
    return {
      overrides: {},
      defaults: {
        'warehouse': this.config.warehouseLocation,
        'catalog-impl': 'org.apache.iceberg.rest.RESTCatalog',
        ...this.config.defaultProperties,
      },
    };
  }

  // ===========================================================================
  // Namespace Endpoints
  // ===========================================================================

  /**
   * GET /v1/namespaces
   */
  async listNamespaces(params: URLSearchParams): Promise<ListNamespacesResponse> {
    const parent = params.get('parent');
    const parentNs = parent ? this.decodeNamespace(parent) : undefined;
    const namespaces = await this.storage.listNamespaces(parentNs);
    return { namespaces };
  }

  /**
   * POST /v1/namespaces
   */
  async createNamespace(request: CreateNamespaceRequest): Promise<CreateNamespaceResponse> {
    const properties = request.properties ?? {};
    await this.storage.createNamespace(request.namespace, properties);
    return {
      namespace: request.namespace,
      properties,
    };
  }

  /**
   * GET /v1/namespaces/{namespace}
   */
  async getNamespace(namespace: NamespaceIdentifier): Promise<GetNamespaceResponse> {
    const properties = await this.storage.loadNamespace(namespace);
    return { namespace, properties };
  }

  /**
   * DELETE /v1/namespaces/{namespace}
   */
  async dropNamespace(namespace: NamespaceIdentifier): Promise<void> {
    await this.storage.dropNamespace(namespace);
  }

  /**
   * POST /v1/namespaces/{namespace}/properties
   */
  async updateNamespaceProperties(
    namespace: NamespaceIdentifier,
    request: UpdateNamespacePropertiesRequest
  ): Promise<UpdateNamespacePropertiesResponse> {
    const updates = request.updates ?? {};
    const removals = request.removals ?? [];

    await this.storage.updateNamespaceProperties(namespace, updates, removals);

    return {
      updated: Object.keys(updates),
      removed: removals,
      missing: [],
    };
  }

  // ===========================================================================
  // Table Endpoints
  // ===========================================================================

  /**
   * GET /v1/namespaces/{namespace}/tables
   */
  async listTables(namespace: NamespaceIdentifier): Promise<ListTablesResponse> {
    const tables = await this.storage.listTables(namespace);
    return { identifiers: tables };
  }

  /**
   * POST /v1/namespaces/{namespace}/tables
   */
  async createTable(
    namespace: NamespaceIdentifier,
    request: CreateTableRequest
  ): Promise<LoadTableResponse> {
    const tableUuid = generateUUID();
    const location = `${this.config.warehouseLocation}/${namespace.join('/')}/${request.name}`;

    const partitionSpec = request['partition-spec'] ?? createUnpartitionedSpec();
    const sortOrder = request['write-order'] ?? createUnsortedOrder();

    const metadata = createTableMetadata(
      tableUuid,
      location,
      request.schema,
      partitionSpec,
      sortOrder,
      request.properties
    );

    const identifier = { namespace, name: request.name };
    await this.storage.createTable(identifier, metadata);

    return {
      'metadata-location': `${location}/metadata/v1.metadata.json`,
      metadata,
    };
  }

  /**
   * GET /v1/namespaces/{namespace}/tables/{table}
   */
  async loadTable(identifier: TableIdentifier): Promise<LoadTableResponse> {
    const metadata = await this.storage.loadTable(identifier);
    return {
      'metadata-location': `${metadata.location}/metadata/v1.metadata.json`,
      metadata,
    };
  }

  /**
   * DELETE /v1/namespaces/{namespace}/tables/{table}
   */
  async dropTable(identifier: TableIdentifier, purge: boolean = false): Promise<void> {
    await this.storage.dropTable(identifier, purge);
  }

  /**
   * POST /v1/namespaces/{namespace}/tables/{table}
   */
  async commitTable(
    identifier: TableIdentifier,
    request: CommitTableRequest
  ): Promise<CommitTableResponse> {
    // Load current metadata
    let metadata = await this.storage.loadTable(identifier);

    // Apply updates
    for (const update of request.updates) {
      metadata = this.applyUpdate(metadata, update);
    }

    // Commit with requirements
    const committed = await this.storage.commitTable(
      identifier,
      metadata,
      request.requirements
    );

    return {
      'metadata-location': `${committed.location}/metadata/v${committed['last-sequence-number']}.metadata.json`,
      metadata: committed,
    };
  }

  // ===========================================================================
  // Update Application
  // ===========================================================================

  /**
   * Apply a table update to metadata
   */
  private applyUpdate(
    metadata: IcebergTableMetadata,
    update: TableUpdate
  ): IcebergTableMetadata {
    switch (update.action) {
      case 'add-snapshot':
        return addSnapshot(metadata, update.snapshot as never);

      case 'set-snapshot-ref':
        return {
          ...metadata,
          'last-updated-ms': BigInt(Date.now()),
          refs: {
            ...metadata.refs,
            [update['ref-name']]: {
              'snapshot-id': update['snapshot-id'],
              type: update.type ?? 'branch',
            },
          },
        };

      case 'set-properties':
        return {
          ...metadata,
          'last-updated-ms': BigInt(Date.now()),
          properties: {
            ...metadata.properties,
            ...update.updates,
          },
        };

      case 'remove-properties': {
        const newProps = { ...metadata.properties };
        for (const key of update.removals) {
          delete newProps[key];
        }
        return {
          ...metadata,
          'last-updated-ms': BigInt(Date.now()),
          properties: newProps,
        };
      }

      case 'add-schema':
        return addSchema(metadata, update.schema, false);

      case 'set-current-schema':
        return {
          ...metadata,
          'last-updated-ms': BigInt(Date.now()),
          'current-schema-id': update['schema-id'],
        };

      case 'add-partition-spec':
        return {
          ...metadata,
          'last-updated-ms': BigInt(Date.now()),
          'partition-specs': [...metadata['partition-specs'], update.spec],
        };

      case 'set-default-spec':
        return {
          ...metadata,
          'last-updated-ms': BigInt(Date.now()),
          'default-spec-id': update['spec-id'],
        };

      case 'add-sort-order':
        return {
          ...metadata,
          'last-updated-ms': BigInt(Date.now()),
          'sort-orders': [...metadata['sort-orders'], update['sort-order']],
        };

      case 'set-default-sort-order':
        return {
          ...metadata,
          'last-updated-ms': BigInt(Date.now()),
          'default-sort-order-id': update['sort-order-id'],
        };

      case 'set-location':
        return {
          ...metadata,
          'last-updated-ms': BigInt(Date.now()),
          location: update.location,
        };

      default:
        return metadata;
    }
  }

  // ===========================================================================
  // Helper Methods
  // ===========================================================================

  /**
   * Decode namespace from URL path segment
   */
  private decodeNamespace(encoded: string): NamespaceIdentifier {
    // Iceberg namespaces in URLs use %1F as level separator
    return decodeURIComponent(encoded).split('\x1f');
  }

  /**
   * Create JSON response
   */
  private jsonResponse(data: unknown, status: number = 200): Response {
    return new Response(
      JSON.stringify(data, (_, v) => (typeof v === 'bigint' ? v.toString() : v)),
      {
        status,
        headers: {
          'Content-Type': 'application/json',
        },
      }
    );
  }

  /**
   * Create error response
   */
  private errorResponse(code: number, type: string, message: string): Response {
    const error: ErrorResponse = {
      error: {
        message,
        type,
        code,
      },
    };
    return new Response(JSON.stringify(error), {
      status: code,
      headers: {
        'Content-Type': 'application/json',
      },
    });
  }

  /**
   * Map error to HTTP status code
   */
  private errorToStatusCode(error: IcebergError): number {
    if (error.message.includes('not found')) {
      return 404;
    }
    if (error.message.includes('already exists')) {
      return 409;
    }
    if (error.message.includes('conflict')) {
      return 409;
    }
    if (error.message.includes('mismatch')) {
      return 409;
    }
    return 500;
  }
}

// =============================================================================
// Factory Functions
// =============================================================================

/**
 * Create a REST catalog handler
 */
export function createRestCatalog(
  storage: IcebergStorage,
  config: RestCatalogConfig
): RestCatalogHandler {
  return new RestCatalogHandler(storage, config);
}
