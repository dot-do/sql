/**
 * Procedure Registry
 *
 * Stores and manages ESM stored procedures with versioning support.
 * Procedures are stored in a catalog that can be backed by various
 * storage implementations (in-memory, D1, R2, etc.)
 */

import type {
  Procedure,
  ProcedureMetadata,
  ProcedureRegistry,
  RegistryEntry,
  Schema,
  ParsedProcedure,
} from './types.js';
import {
  parseProcedure,
  buildInputSchema,
  buildOutputSchema,
  validateModuleCode,
  isCreateProcedure,
} from './parser.js';

// =============================================================================
// CATALOG STORAGE INTERFACE
// =============================================================================

/**
 * Storage backend for the procedure catalog
 */
export interface CatalogStorage {
  /** Get a procedure by name */
  get(name: string): Promise<RegistryEntry | undefined>;

  /** Set a procedure entry */
  set(name: string, entry: RegistryEntry): Promise<void>;

  /** Delete a procedure */
  delete(name: string): Promise<boolean>;

  /** List all procedure names */
  list(): Promise<string[]>;
}

// =============================================================================
// IN-MEMORY CATALOG STORAGE
// =============================================================================

/**
 * In-memory implementation of catalog storage
 */
export function createInMemoryCatalogStorage(): CatalogStorage {
  const store = new Map<string, RegistryEntry>();

  return {
    async get(name: string): Promise<RegistryEntry | undefined> {
      return store.get(name);
    },

    async set(name: string, entry: RegistryEntry): Promise<void> {
      store.set(name, entry);
    },

    async delete(name: string): Promise<boolean> {
      return store.delete(name);
    },

    async list(): Promise<string[]> {
      return Array.from(store.keys());
    },
  };
}

// =============================================================================
// PROCEDURE REGISTRY IMPLEMENTATION
// =============================================================================

/**
 * Options for creating a procedure registry
 */
export interface RegistryOptions {
  /** Storage backend */
  storage: CatalogStorage;

  /** Maximum versions to retain per procedure */
  maxVersions?: number;

  /** Enable validation of module code */
  validateCode?: boolean;
}

/**
 * Create a new procedure registry
 */
export function createProcedureRegistry(options: RegistryOptions): ProcedureRegistry {
  const { storage, maxVersions = 10, validateCode = true } = options;

  return {
    async register(input): Promise<Procedure> {
      const { name, code, inputSchema, outputSchema, timeout, memoryLimit } = input;
      const now = new Date();

      // Validate module code
      if (validateCode) {
        const validation = validateModuleCode(code);
        if (!validation.valid) {
          throw new Error(`Invalid module code: ${validation.error}`);
        }
      }

      // Get existing entry or create new one
      const existing = await storage.get(name);
      const version = existing ? existing.current.metadata.version + 1 : 1;

      // Create metadata
      const metadata: ProcedureMetadata = {
        name,
        version,
        createdAt: existing?.current.metadata.createdAt ?? now,
        updatedAt: now,
        description: input.metadata?.description,
        author: input.metadata?.author,
        tags: input.metadata?.tags,
      };

      // Create procedure
      const procedure: Procedure = {
        name,
        code,
        inputSchema: inputSchema as Schema | undefined,
        outputSchema: outputSchema as Schema | undefined,
        metadata,
        timeout,
        memoryLimit,
      };

      // Build version history
      const versions = existing?.versions ?? [];
      if (existing) {
        versions.push({
          version: existing.current.metadata.version,
          procedure: existing.current,
          timestamp: now,
        });

        // Trim old versions if needed
        while (versions.length > maxVersions) {
          versions.shift();
        }
      }

      // Create registry entry
      const entry: RegistryEntry = {
        current: procedure,
        versions,
      };

      await storage.set(name, entry);
      return procedure;
    },

    async get(name: string, version?: number): Promise<Procedure | undefined> {
      const entry = await storage.get(name);
      if (!entry) {
        return undefined;
      }

      if (version === undefined) {
        return entry.current;
      }

      // Find specific version
      if (entry.current.metadata.version === version) {
        return entry.current;
      }

      const versionEntry = entry.versions.find(v => v.version === version);
      return versionEntry?.procedure;
    },

    async list(): Promise<Procedure[]> {
      const names = await storage.list();
      const procedures: Procedure[] = [];

      for (const name of names) {
        const entry = await storage.get(name);
        if (entry) {
          procedures.push(entry.current);
        }
      }

      return procedures;
    },

    async delete(name: string): Promise<boolean> {
      return storage.delete(name);
    },

    async history(name: string): Promise<RegistryEntry['versions']> {
      const entry = await storage.get(name);
      if (!entry) {
        return [];
      }

      // Include current version in history
      return [
        ...entry.versions,
        {
          version: entry.current.metadata.version,
          procedure: entry.current,
          timestamp: entry.current.metadata.updatedAt,
        },
      ];
    },
  };
}

// =============================================================================
// SQL INTERFACE
// =============================================================================

/**
 * SQL-style interface for managing procedures
 */
export interface SqlProcedureManager {
  /** Execute a CREATE PROCEDURE statement */
  execute(sql: string): Promise<Procedure>;

  /** Get a procedure by name */
  get(name: string): Promise<Procedure | undefined>;

  /** List all procedures */
  list(): Promise<Procedure[]>;

  /** Drop a procedure */
  drop(name: string): Promise<boolean>;
}

/**
 * Create a SQL-style procedure manager
 */
export function createSqlProcedureManager(registry: ProcedureRegistry): SqlProcedureManager {
  return {
    async execute(sql: string): Promise<Procedure> {
      if (!isCreateProcedure(sql)) {
        throw new Error('Only CREATE PROCEDURE/FUNCTION statements are supported');
      }

      const parsed = parseProcedure(sql);
      const inputSchema = buildInputSchema(parsed.parameters);
      const outputSchema = buildOutputSchema(parsed.returnType);

      return registry.register({
        name: parsed.name,
        code: parsed.code,
        inputSchema,
        outputSchema,
      });
    },

    async get(name: string): Promise<Procedure | undefined> {
      return registry.get(name);
    },

    async list(): Promise<Procedure[]> {
      return registry.list();
    },

    async drop(name: string): Promise<boolean> {
      return registry.delete(name);
    },
  };
}

// =============================================================================
// PROCEDURE BUILDER
// =============================================================================

/**
 * Fluent builder for creating procedures
 */
export class ProcedureBuilder {
  private _name: string = '';
  private _code: string = '';
  private _inputSchema?: Schema;
  private _outputSchema?: Schema;
  private _description?: string;
  private _author?: string;
  private _tags: string[] = [];
  private _timeout?: number;
  private _memoryLimit?: number;

  /**
   * Set the procedure name
   */
  name(name: string): this {
    this._name = name;
    return this;
  }

  /**
   * Set the ESM module code
   */
  code(code: string): this {
    this._code = code;
    return this;
  }

  /**
   * Set the input parameter schema
   */
  input(schema: Schema): this {
    this._inputSchema = schema;
    return this;
  }

  /**
   * Set the output/return schema
   */
  output(schema: Schema): this {
    this._outputSchema = schema;
    return this;
  }

  /**
   * Set the description
   */
  description(description: string): this {
    this._description = description;
    return this;
  }

  /**
   * Set the author
   */
  author(author: string): this {
    this._author = author;
    return this;
  }

  /**
   * Add tags
   */
  tag(...tags: string[]): this {
    this._tags.push(...tags);
    return this;
  }

  /**
   * Set execution timeout (ms)
   */
  timeout(ms: number): this {
    this._timeout = ms;
    return this;
  }

  /**
   * Set memory limit (MB)
   */
  memoryLimit(mb: number): this {
    this._memoryLimit = mb;
    return this;
  }

  /**
   * Build and register the procedure
   */
  async register(registry: ProcedureRegistry): Promise<Procedure> {
    if (!this._name) {
      throw new Error('Procedure name is required');
    }
    if (!this._code) {
      throw new Error('Procedure code is required');
    }

    return registry.register({
      name: this._name,
      code: this._code,
      inputSchema: this._inputSchema,
      outputSchema: this._outputSchema,
      timeout: this._timeout,
      memoryLimit: this._memoryLimit,
      metadata: {
        description: this._description,
        author: this._author,
        tags: this._tags.length > 0 ? this._tags : undefined,
      },
    });
  }

  /**
   * Build the procedure definition without registering
   */
  build(): Omit<Procedure, 'metadata'> & { metadata?: Partial<ProcedureMetadata> } {
    if (!this._name) {
      throw new Error('Procedure name is required');
    }
    if (!this._code) {
      throw new Error('Procedure code is required');
    }

    return {
      name: this._name,
      code: this._code,
      inputSchema: this._inputSchema,
      outputSchema: this._outputSchema,
      timeout: this._timeout,
      memoryLimit: this._memoryLimit,
      metadata: {
        description: this._description,
        author: this._author,
        tags: this._tags.length > 0 ? this._tags : undefined,
      },
    };
  }
}

/**
 * Create a new procedure builder
 */
export function procedure(): ProcedureBuilder {
  return new ProcedureBuilder();
}

// =============================================================================
// CATALOG QUERIES
// =============================================================================

/**
 * Query options for listing procedures
 */
export interface ListOptions {
  /** Filter by tag */
  tag?: string;

  /** Filter by author */
  author?: string;

  /** Filter by name pattern (glob) */
  namePattern?: string;

  /** Sort order */
  sortBy?: 'name' | 'createdAt' | 'updatedAt' | 'version';
  sortOrder?: 'asc' | 'desc';

  /** Pagination */
  offset?: number;
  limit?: number;
}

/**
 * Extended registry with query capabilities
 */
export interface ExtendedProcedureRegistry extends ProcedureRegistry {
  /** Query procedures with filters */
  query(options: ListOptions): Promise<Procedure[]>;

  /** Count procedures matching filters */
  count(options?: Omit<ListOptions, 'offset' | 'limit' | 'sortBy' | 'sortOrder'>): Promise<number>;

  /** Search procedures by description */
  search(query: string): Promise<Procedure[]>;
}

/**
 * Create an extended procedure registry with query capabilities
 */
export function createExtendedRegistry(registry: ProcedureRegistry): ExtendedProcedureRegistry {
  return {
    ...registry,

    async query(options: ListOptions): Promise<Procedure[]> {
      let procedures = await registry.list();

      // Apply filters
      if (options.tag) {
        procedures = procedures.filter(p =>
          p.metadata.tags?.includes(options.tag!)
        );
      }

      if (options.author) {
        procedures = procedures.filter(p =>
          p.metadata.author === options.author
        );
      }

      if (options.namePattern) {
        const regex = globToRegex(options.namePattern);
        procedures = procedures.filter(p => regex.test(p.name));
      }

      // Apply sorting
      if (options.sortBy) {
        const direction = options.sortOrder === 'desc' ? -1 : 1;
        procedures.sort((a, b) => {
          let aVal: string | number | Date;
          let bVal: string | number | Date;

          switch (options.sortBy) {
            case 'name':
              aVal = a.name;
              bVal = b.name;
              break;
            case 'createdAt':
              aVal = a.metadata.createdAt;
              bVal = b.metadata.createdAt;
              break;
            case 'updatedAt':
              aVal = a.metadata.updatedAt;
              bVal = b.metadata.updatedAt;
              break;
            case 'version':
              aVal = a.metadata.version;
              bVal = b.metadata.version;
              break;
            default:
              return 0;
          }

          if (aVal < bVal) return -1 * direction;
          if (aVal > bVal) return 1 * direction;
          return 0;
        });
      }

      // Apply pagination
      if (options.offset !== undefined) {
        procedures = procedures.slice(options.offset);
      }
      if (options.limit !== undefined) {
        procedures = procedures.slice(0, options.limit);
      }

      return procedures;
    },

    async count(options?: Omit<ListOptions, 'offset' | 'limit' | 'sortBy' | 'sortOrder'>): Promise<number> {
      const procedures = await this.query({ ...options, limit: undefined, offset: undefined });
      return procedures.length;
    },

    async search(query: string): Promise<Procedure[]> {
      const procedures = await registry.list();
      const lowerQuery = query.toLowerCase();

      return procedures.filter(p =>
        p.name.toLowerCase().includes(lowerQuery) ||
        p.metadata.description?.toLowerCase().includes(lowerQuery) ||
        p.metadata.tags?.some(t => t.toLowerCase().includes(lowerQuery))
      );
    },
  };
}

/**
 * Convert glob pattern to regex
 */
function globToRegex(pattern: string): RegExp {
  const escaped = pattern
    .replace(/[.+^${}()|[\]\\]/g, '\\$&')
    .replace(/\*/g, '.*')
    .replace(/\?/g, '.');
  return new RegExp(`^${escaped}$`, 'i');
}
