/**
 * Schema Versioning for CDC/Lakehouse Compatibility
 *
 * Provides schema version tracking and evolution support for CDC streams.
 * This module enables:
 * - Monotonically increasing schema version numbers per table
 * - Schema change detection and classification
 * - CDC event enrichment with schema metadata
 * - Schema compatibility validation for DoLake consumers
 *
 * @module schema/versioning
 */

import type { ParsedField } from './types.js';

// =============================================================================
// Schema Version Types
// =============================================================================

/** Brand symbol for Schema Version */
declare const SchemaVersionBrand: unique symbol;

/**
 * Branded type for schema version identifiers.
 *
 * SchemaVersion is a bigint branded type that represents the version of a table schema.
 * Schema versions are monotonically increasing numbers that track schema evolution.
 * Used for CDC event enrichment and schema compatibility validation.
 *
 * Use `createSchemaVersion()` to create validated instances.
 *
 * @example
 * ```typescript
 * import { createSchemaVersion, SchemaVersion } from './versioning.js';
 *
 * // Create a schema version
 * const version: SchemaVersion = createSchemaVersion(1);
 *
 * // Use in schema tracking
 * const schema: TableSchema = {
 *   tableName: 'users',
 *   version: version,
 *   // ...
 * };
 *
 * // Increment version for schema changes
 * const nextVersion = createSchemaVersion(version + 1n);
 * ```
 *
 * @public
 * @stability stable
 */
export type SchemaVersion = bigint & { readonly [SchemaVersionBrand]: never };

/**
 * Creates a branded SchemaVersion from a bigint or number value.
 *
 * This is the recommended way to create SchemaVersion values.
 * The function converts numbers to bigint automatically.
 *
 * @param value - The version number as bigint or number
 * @returns A branded SchemaVersion value
 *
 * @example
 * ```typescript
 * // From number
 * const v1 = createSchemaVersion(1);
 *
 * // From bigint
 * const v2 = createSchemaVersion(2n);
 *
 * // Increment existing version
 * const v3 = createSchemaVersion(v2 + 1n);
 * ```
 *
 * @public
 * @stability stable
 */
export function createSchemaVersion(value: bigint | number): SchemaVersion {
  return BigInt(value) as SchemaVersion;
}

/**
 * Schema change operation types
 */
export type SchemaChangeType =
  | 'ADD_COLUMN'
  | 'DROP_COLUMN'
  | 'RENAME_COLUMN'
  | 'ALTER_COLUMN_TYPE'
  | 'ALTER_COLUMN_NULLABLE'
  | 'ALTER_COLUMN_DEFAULT'
  | 'ADD_INDEX'
  | 'DROP_INDEX'
  | 'ADD_CONSTRAINT'
  | 'DROP_CONSTRAINT'
  | 'CREATE_TABLE'
  | 'DROP_TABLE'
  | 'RENAME_TABLE';

/**
 * Schema compatibility level for change classification
 */
export type CompatibilityLevel =
  | 'BACKWARD_COMPATIBLE' // Can read old data with new schema
  | 'FORWARD_COMPATIBLE' // Can read new data with old schema
  | 'FULL_COMPATIBLE' // Both backward and forward compatible
  | 'BREAKING'; // Requires data migration

/**
 * Column definition for schema tracking
 */
export interface ColumnDefinition {
  /** Column name */
  name: string;
  /** SQL type (e.g., 'INTEGER', 'TEXT', 'TIMESTAMP') */
  type: string;
  /** Is nullable */
  nullable: boolean;
  /** Default value (SQL expression) */
  defaultValue?: string;
  /** Is primary key */
  primaryKey: boolean;
  /** Is indexed */
  indexed: boolean;
  /** Column position (for ordering) */
  position: number;
  /** Column ID (immutable, for tracking renames) */
  columnId: number;
}

/**
 * Index definition for schema tracking
 */
export interface IndexDefinition {
  /** Index name */
  name: string;
  /** Columns in the index */
  columns: string[];
  /** Is unique index */
  unique: boolean;
  /** Partial index predicate (WHERE clause) */
  predicate?: string;
}

/**
 * Constraint definition for schema tracking
 */
export interface ConstraintDefinition {
  /** Constraint name */
  name: string;
  /** Constraint type */
  type: 'PRIMARY_KEY' | 'UNIQUE' | 'FOREIGN_KEY' | 'CHECK' | 'NOT_NULL';
  /** Columns involved */
  columns: string[];
  /** Reference table (for foreign keys) */
  referenceTable?: string;
  /** Reference columns (for foreign keys) */
  referenceColumns?: string[];
  /** Check expression */
  checkExpression?: string;
}

/**
 * Complete table schema snapshot
 */
export interface TableSchema {
  /** Table name */
  tableName: string;
  /** Schema version number */
  version: SchemaVersion;
  /** Column definitions */
  columns: ColumnDefinition[];
  /** Index definitions */
  indexes: IndexDefinition[];
  /** Constraint definitions */
  constraints: ConstraintDefinition[];
  /** Primary key columns */
  primaryKey: string[];
  /** Schema creation timestamp */
  createdAt: number;
  /** Last modified timestamp */
  modifiedAt: number;
  /** Previous schema version (for lineage tracking) */
  previousVersion?: SchemaVersion;
  /** Schema checksum for quick comparison */
  checksum: string;
}

/**
 * Schema change event for CDC
 */
export interface SchemaChangeEvent {
  /** Event type discriminator */
  type: 'schema_change';
  /** Table affected */
  table: string;
  /** Type of schema change */
  changeType: SchemaChangeType;
  /** Schema version before change */
  beforeVersion: SchemaVersion;
  /** Schema version after change */
  afterVersion: SchemaVersion;
  /** Column affected (for column changes) */
  column?: string;
  /** Old column name (for renames) */
  oldColumnName?: string;
  /** New column definition (for adds/alters) */
  newColumn?: ColumnDefinition;
  /** Old column definition (for drops/alters) */
  oldColumn?: ColumnDefinition;
  /** Compatibility level of this change */
  compatibility: CompatibilityLevel;
  /** Change timestamp (Unix ms) */
  timestamp: number;
  /** LSN of the schema change */
  lsn: bigint;
  /** Transaction ID */
  txnId: string;
  /** Human-readable description */
  description: string;
}

/**
 * Schema registry entry for tracking table schemas
 */
export interface SchemaRegistryEntry {
  /** Table name */
  tableName: string;
  /** Current schema version */
  currentVersion: SchemaVersion;
  /** Schema history (version -> schema) */
  history: Map<string, TableSchema>;
  /** Pending changes (not yet committed) */
  pendingChanges: SchemaChangeEvent[];
}

// =============================================================================
// Schema Version Registry
// =============================================================================

/**
 * Schema version registry for tracking schema evolution
 */
export class SchemaVersionRegistry {
  /** Table schemas by name */
  private tables: Map<string, SchemaRegistryEntry> = new Map();
  /** Next column ID for new columns */
  private nextColumnId: number = 1;
  /** Schema change listeners */
  private listeners: Array<(event: SchemaChangeEvent) => void> = [];

  /**
   * Register a new table schema
   */
  registerTable(tableName: string, schema: Omit<TableSchema, 'version' | 'checksum' | 'createdAt' | 'modifiedAt'>): TableSchema {
    const now = Date.now();
    const version = createSchemaVersion(1);

    // Assign column IDs if not present
    const columns = schema.columns.map((col, idx) => ({
      ...col,
      columnId: col.columnId || this.nextColumnId++,
      position: col.position ?? idx,
    }));

    const fullSchema: TableSchema = {
      ...schema,
      tableName,
      columns,
      version,
      createdAt: now,
      modifiedAt: now,
      checksum: this.computeChecksum({ ...schema, columns }),
    };

    const entry: SchemaRegistryEntry = {
      tableName,
      currentVersion: version,
      history: new Map([[version.toString(), fullSchema]]),
      pendingChanges: [],
    };

    this.tables.set(tableName, entry);

    // Emit schema creation event
    const createEvent: SchemaChangeEvent = {
      type: 'schema_change',
      table: tableName,
      changeType: 'CREATE_TABLE',
      beforeVersion: createSchemaVersion(0),
      afterVersion: version,
      compatibility: 'BACKWARD_COMPATIBLE',
      timestamp: now,
      lsn: 0n,
      txnId: '',
      description: `Created table ${tableName} with ${columns.length} columns`,
    };

    this.notifyListeners(createEvent);

    return fullSchema;
  }

  /**
   * Get current schema for a table
   */
  getSchema(tableName: string): TableSchema | null {
    const entry = this.tables.get(tableName);
    if (!entry) return null;
    return entry.history.get(entry.currentVersion.toString()) ?? null;
  }

  /**
   * Get schema at a specific version
   */
  getSchemaAtVersion(tableName: string, version: SchemaVersion): TableSchema | null {
    const entry = this.tables.get(tableName);
    if (!entry) return null;
    return entry.history.get(version.toString()) ?? null;
  }

  /**
   * Get current schema version for a table
   */
  getCurrentVersion(tableName: string): SchemaVersion | null {
    const entry = this.tables.get(tableName);
    return entry?.currentVersion ?? null;
  }

  /**
   * Add a column to a table
   */
  addColumn(
    tableName: string,
    column: Omit<ColumnDefinition, 'columnId' | 'position'>,
    lsn: bigint,
    txnId: string
  ): SchemaChangeEvent {
    const entry = this.tables.get(tableName);
    if (!entry) {
      throw new Error(`Table ${tableName} not found in schema registry`);
    }

    const currentSchema = entry.history.get(entry.currentVersion.toString())!;
    const now = Date.now();
    const newVersion = createSchemaVersion(entry.currentVersion + 1n);

    const newColumn: ColumnDefinition = {
      ...column,
      columnId: this.nextColumnId++,
      position: currentSchema.columns.length,
    };

    const newSchema: TableSchema = {
      ...currentSchema,
      version: newVersion,
      modifiedAt: now,
      previousVersion: entry.currentVersion,
      columns: [...currentSchema.columns, newColumn],
      checksum: '', // Will be recomputed
    };
    newSchema.checksum = this.computeChecksum(newSchema);

    entry.history.set(newVersion.toString(), newSchema);
    entry.currentVersion = newVersion;

    const changeEvent: SchemaChangeEvent = {
      type: 'schema_change',
      table: tableName,
      changeType: 'ADD_COLUMN',
      beforeVersion: currentSchema.version,
      afterVersion: newVersion,
      column: column.name,
      newColumn,
      compatibility: column.nullable || column.defaultValue !== undefined
        ? 'FULL_COMPATIBLE'
        : 'BACKWARD_COMPATIBLE',
      timestamp: now,
      lsn,
      txnId,
      description: `Added column ${column.name} (${column.type}) to ${tableName}`,
    };

    this.notifyListeners(changeEvent);
    return changeEvent;
  }

  /**
   * Drop a column from a table
   */
  dropColumn(
    tableName: string,
    columnName: string,
    lsn: bigint,
    txnId: string
  ): SchemaChangeEvent {
    const entry = this.tables.get(tableName);
    if (!entry) {
      throw new Error(`Table ${tableName} not found in schema registry`);
    }

    const currentSchema = entry.history.get(entry.currentVersion.toString())!;
    const columnToDrop = currentSchema.columns.find(c => c.name === columnName);
    if (!columnToDrop) {
      throw new Error(`Column ${columnName} not found in table ${tableName}`);
    }

    const now = Date.now();
    const newVersion = createSchemaVersion(entry.currentVersion + 1n);

    const newSchema: TableSchema = {
      ...currentSchema,
      version: newVersion,
      modifiedAt: now,
      previousVersion: entry.currentVersion,
      columns: currentSchema.columns.filter(c => c.name !== columnName),
      checksum: '',
    };
    newSchema.checksum = this.computeChecksum(newSchema);

    entry.history.set(newVersion.toString(), newSchema);
    entry.currentVersion = newVersion;

    const changeEvent: SchemaChangeEvent = {
      type: 'schema_change',
      table: tableName,
      changeType: 'DROP_COLUMN',
      beforeVersion: currentSchema.version,
      afterVersion: newVersion,
      column: columnName,
      oldColumn: columnToDrop,
      compatibility: 'BREAKING',
      timestamp: now,
      lsn,
      txnId,
      description: `Dropped column ${columnName} from ${tableName}`,
    };

    this.notifyListeners(changeEvent);
    return changeEvent;
  }

  /**
   * Rename a column
   */
  renameColumn(
    tableName: string,
    oldName: string,
    newName: string,
    lsn: bigint,
    txnId: string
  ): SchemaChangeEvent {
    const entry = this.tables.get(tableName);
    if (!entry) {
      throw new Error(`Table ${tableName} not found in schema registry`);
    }

    const currentSchema = entry.history.get(entry.currentVersion.toString())!;
    const columnToRename = currentSchema.columns.find(c => c.name === oldName);
    if (!columnToRename) {
      throw new Error(`Column ${oldName} not found in table ${tableName}`);
    }

    const now = Date.now();
    const newVersion = createSchemaVersion(entry.currentVersion + 1n);

    const renamedColumn: ColumnDefinition = {
      ...columnToRename,
      name: newName,
    };

    const newSchema: TableSchema = {
      ...currentSchema,
      version: newVersion,
      modifiedAt: now,
      previousVersion: entry.currentVersion,
      columns: currentSchema.columns.map(c =>
        c.name === oldName ? renamedColumn : c
      ),
      checksum: '',
    };
    newSchema.checksum = this.computeChecksum(newSchema);

    entry.history.set(newVersion.toString(), newSchema);
    entry.currentVersion = newVersion;

    const changeEvent: SchemaChangeEvent = {
      type: 'schema_change',
      table: tableName,
      changeType: 'RENAME_COLUMN',
      beforeVersion: currentSchema.version,
      afterVersion: newVersion,
      column: newName,
      oldColumnName: oldName,
      oldColumn: columnToRename,
      newColumn: renamedColumn,
      compatibility: 'BREAKING', // Renaming columns breaks backward compatibility
      timestamp: now,
      lsn,
      txnId,
      description: `Renamed column ${oldName} to ${newName} in ${tableName}`,
    };

    this.notifyListeners(changeEvent);
    return changeEvent;
  }

  /**
   * Alter a column's type or nullability
   */
  alterColumn(
    tableName: string,
    columnName: string,
    changes: Partial<Pick<ColumnDefinition, 'type' | 'nullable' | 'defaultValue'>>,
    lsn: bigint,
    txnId: string
  ): SchemaChangeEvent {
    const entry = this.tables.get(tableName);
    if (!entry) {
      throw new Error(`Table ${tableName} not found in schema registry`);
    }

    const currentSchema = entry.history.get(entry.currentVersion.toString())!;
    const columnToAlter = currentSchema.columns.find(c => c.name === columnName);
    if (!columnToAlter) {
      throw new Error(`Column ${columnName} not found in table ${tableName}`);
    }

    const now = Date.now();
    const newVersion = createSchemaVersion(entry.currentVersion + 1n);

    const alteredColumn: ColumnDefinition = {
      ...columnToAlter,
      ...changes,
    };

    const newSchema: TableSchema = {
      ...currentSchema,
      version: newVersion,
      modifiedAt: now,
      previousVersion: entry.currentVersion,
      columns: currentSchema.columns.map(c =>
        c.name === columnName ? alteredColumn : c
      ),
      checksum: '',
    };
    newSchema.checksum = this.computeChecksum(newSchema);

    entry.history.set(newVersion.toString(), newSchema);
    entry.currentVersion = newVersion;

    // Determine compatibility based on change type
    let compatibility: CompatibilityLevel = 'FULL_COMPATIBLE';
    let changeType: SchemaChangeType = 'ALTER_COLUMN_TYPE';

    if (changes.type !== undefined && changes.type !== columnToAlter.type) {
      compatibility = this.classifyTypeChange(columnToAlter.type, changes.type);
      changeType = 'ALTER_COLUMN_TYPE';
    } else if (changes.nullable !== undefined) {
      changeType = 'ALTER_COLUMN_NULLABLE';
      // Making nullable -> non-nullable is breaking
      // Making non-nullable -> nullable is backward compatible
      if (columnToAlter.nullable && !changes.nullable) {
        compatibility = 'BREAKING';
      } else {
        compatibility = 'FULL_COMPATIBLE';
      }
    } else if (changes.defaultValue !== undefined) {
      changeType = 'ALTER_COLUMN_DEFAULT';
    }

    const changeEvent: SchemaChangeEvent = {
      type: 'schema_change',
      table: tableName,
      changeType,
      beforeVersion: currentSchema.version,
      afterVersion: newVersion,
      column: columnName,
      oldColumn: columnToAlter,
      newColumn: alteredColumn,
      compatibility,
      timestamp: now,
      lsn,
      txnId,
      description: `Altered column ${columnName} in ${tableName}`,
    };

    this.notifyListeners(changeEvent);
    return changeEvent;
  }

  /**
   * Drop a table
   */
  dropTable(tableName: string, lsn: bigint, txnId: string): SchemaChangeEvent {
    const entry = this.tables.get(tableName);
    if (!entry) {
      throw new Error(`Table ${tableName} not found in schema registry`);
    }

    const currentSchema = entry.history.get(entry.currentVersion.toString())!;
    const now = Date.now();

    const changeEvent: SchemaChangeEvent = {
      type: 'schema_change',
      table: tableName,
      changeType: 'DROP_TABLE',
      beforeVersion: currentSchema.version,
      afterVersion: createSchemaVersion(0),
      compatibility: 'BREAKING',
      timestamp: now,
      lsn,
      txnId,
      description: `Dropped table ${tableName}`,
    };

    this.tables.delete(tableName);
    this.notifyListeners(changeEvent);
    return changeEvent;
  }

  /**
   * Subscribe to schema change events
   */
  onSchemaChange(listener: (event: SchemaChangeEvent) => void): () => void {
    this.listeners.push(listener);
    return () => {
      const index = this.listeners.indexOf(listener);
      if (index >= 0) {
        this.listeners.splice(index, 1);
      }
    };
  }

  /**
   * Get schema history for a table
   */
  getSchemaHistory(tableName: string): TableSchema[] {
    const entry = this.tables.get(tableName);
    if (!entry) return [];

    return Array.from(entry.history.values())
      .sort((a, b) => Number(a.version - b.version));
  }

  /**
   * Check if a schema change is compatible
   */
  isCompatibleChange(
    tableName: string,
    fromVersion: SchemaVersion,
    toVersion: SchemaVersion
  ): { compatible: boolean; level: CompatibilityLevel; changes: SchemaChangeEvent[] } {
    const entry = this.tables.get(tableName);
    if (!entry) {
      return { compatible: false, level: 'BREAKING', changes: [] };
    }

    const fromSchema = entry.history.get(fromVersion.toString());
    const toSchema = entry.history.get(toVersion.toString());

    if (!fromSchema || !toSchema) {
      return { compatible: false, level: 'BREAKING', changes: [] };
    }

    // Collect all changes between versions
    const changes: SchemaChangeEvent[] = [];
    let currentVersion = fromVersion;

    while (currentVersion < toVersion) {
      const nextVersion = createSchemaVersion(currentVersion + 1n);
      const nextSchema = entry.history.get(nextVersion.toString());
      if (nextSchema) {
        // Find changes between consecutive versions
        // This is a simplified implementation - real implementation would track actual change events
        currentVersion = nextVersion;
      } else {
        break;
      }
    }

    // Determine overall compatibility
    let overallLevel: CompatibilityLevel = 'FULL_COMPATIBLE';
    for (const change of changes) {
      if (change.compatibility === 'BREAKING') {
        overallLevel = 'BREAKING';
        break;
      } else if (change.compatibility === 'BACKWARD_COMPATIBLE' && overallLevel === 'FULL_COMPATIBLE') {
        overallLevel = 'BACKWARD_COMPATIBLE';
      } else if (change.compatibility === 'FORWARD_COMPATIBLE' && overallLevel === 'FULL_COMPATIBLE') {
        overallLevel = 'FORWARD_COMPATIBLE';
      }
    }

    return {
      compatible: overallLevel !== 'BREAKING',
      level: overallLevel,
      changes,
    };
  }

  /**
   * Export registry state for persistence
   */
  export(): {
    tables: Array<{ name: string; entry: { currentVersion: string; history: Array<[string, TableSchema]> } }>;
    nextColumnId: number;
  } {
    const tables: Array<{ name: string; entry: { currentVersion: string; history: Array<[string, TableSchema]> } }> = [];

    for (const [name, entry] of this.tables.entries()) {
      tables.push({
        name,
        entry: {
          currentVersion: entry.currentVersion.toString(),
          history: Array.from(entry.history.entries()),
        },
      });
    }

    return { tables, nextColumnId: this.nextColumnId };
  }

  /**
   * Import registry state from persistence
   */
  import(state: ReturnType<SchemaVersionRegistry['export']>): void {
    this.tables.clear();
    this.nextColumnId = state.nextColumnId;

    for (const { name, entry } of state.tables) {
      const history = new Map<string, TableSchema>();
      for (const [version, schema] of entry.history) {
        history.set(version, schema);
      }

      this.tables.set(name, {
        tableName: name,
        currentVersion: createSchemaVersion(BigInt(entry.currentVersion)),
        history,
        pendingChanges: [],
      });
    }
  }

  // Private helpers

  private notifyListeners(event: SchemaChangeEvent): void {
    for (const listener of this.listeners) {
      try {
        listener(event);
      } catch (error) {
        console.error('Schema change listener error:', error);
      }
    }
  }

  private computeChecksum(schema: Omit<TableSchema, 'checksum' | 'version' | 'createdAt' | 'modifiedAt' | 'previousVersion'>): string {
    // Simple checksum based on column definitions
    const parts: string[] = [];
    for (const col of schema.columns) {
      parts.push(`${col.name}:${col.type}:${col.nullable}:${col.defaultValue ?? ''}:${col.columnId}`);
    }
    for (const idx of schema.indexes) {
      parts.push(`idx:${idx.name}:${idx.columns.join(',')}:${idx.unique}`);
    }
    for (const cst of schema.constraints) {
      parts.push(`cst:${cst.name}:${cst.type}:${cst.columns.join(',')}`);
    }

    // Simple hash (in production, use crypto.subtle)
    const str = parts.join('|');
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash;
    }
    return hash.toString(16);
  }

  private classifyTypeChange(fromType: string, toType: string): CompatibilityLevel {
    // Type widening is generally backward compatible
    const typeWidening: Record<string, string[]> = {
      'INTEGER': ['BIGINT', 'REAL', 'DOUBLE', 'TEXT'],
      'SMALLINT': ['INTEGER', 'BIGINT', 'REAL', 'DOUBLE', 'TEXT'],
      'TINYINT': ['SMALLINT', 'INTEGER', 'BIGINT', 'REAL', 'DOUBLE', 'TEXT'],
      'REAL': ['DOUBLE', 'TEXT'],
      'FLOAT': ['DOUBLE', 'TEXT'],
      'VARCHAR': ['TEXT'],
    };

    const fromNorm = fromType.toUpperCase();
    const toNorm = toType.toUpperCase();

    if (fromNorm === toNorm) return 'FULL_COMPATIBLE';

    const allowedWidenings = typeWidening[fromNorm] ?? [];
    if (allowedWidenings.includes(toNorm)) {
      return 'BACKWARD_COMPATIBLE';
    }

    // Type narrowing is breaking
    return 'BREAKING';
  }
}

// =============================================================================
// CDC Schema Enrichment
// =============================================================================

/**
 * Enriches a CDC event with schema version metadata
 */
export interface SchemaEnrichedCDCEvent<T = unknown> {
  /** Original event data */
  event: {
    id: string;
    type: 'insert' | 'update' | 'delete';
    table: string;
    txnId: string;
    timestamp: Date;
    lsn: bigint;
    data?: T;
    oldData?: T;
    key?: Uint8Array;
  };
  /** Schema version at time of change */
  schemaVersion: SchemaVersion;
  /** Schema checksum for quick validation */
  schemaChecksum: string;
  /** Column IDs present in the data (for schema evolution tracking) */
  columnIds: number[];
}

/**
 * Schema-aware CDC event processor
 */
export class SchemaAwareCDCProcessor {
  constructor(private registry: SchemaVersionRegistry) {}

  /**
   * Enrich a CDC event with schema metadata
   */
  enrichEvent<T>(
    event: SchemaEnrichedCDCEvent<T>['event']
  ): SchemaEnrichedCDCEvent<T> {
    const schema = this.registry.getSchema(event.table);
    if (!schema) {
      throw new Error(`No schema registered for table ${event.table}`);
    }

    // Extract column IDs from data keys
    const dataKeys = new Set<string>();
    if (event.data && typeof event.data === 'object') {
      for (const key of Object.keys(event.data as object)) {
        dataKeys.add(key);
      }
    }
    if (event.oldData && typeof event.oldData === 'object') {
      for (const key of Object.keys(event.oldData as object)) {
        dataKeys.add(key);
      }
    }

    const columnIds = schema.columns
      .filter(col => dataKeys.has(col.name))
      .map(col => col.columnId);

    return {
      event,
      schemaVersion: schema.version,
      schemaChecksum: schema.checksum,
      columnIds,
    };
  }

  /**
   * Validate that a CDC event is compatible with a target schema version
   */
  validateCompatibility(
    event: SchemaEnrichedCDCEvent,
    targetVersion: SchemaVersion
  ): { valid: boolean; errors: string[] } {
    const errors: string[] = [];

    const { compatible, level } = this.registry.isCompatibleChange(
      event.event.table,
      event.schemaVersion,
      targetVersion
    );

    if (!compatible) {
      errors.push(
        `Schema version ${event.schemaVersion} is not compatible with target version ${targetVersion} (${level})`
      );
    }

    return { valid: errors.length === 0, errors };
  }

  /**
   * Transform event data to match a target schema version
   */
  transformToVersion<T>(
    event: SchemaEnrichedCDCEvent<T>,
    targetVersion: SchemaVersion
  ): SchemaEnrichedCDCEvent<T> {
    if (event.schemaVersion === targetVersion) {
      return event;
    }

    const targetSchema = this.registry.getSchemaAtVersion(event.event.table, targetVersion);
    if (!targetSchema) {
      throw new Error(`Target schema version ${targetVersion} not found for table ${event.event.table}`);
    }

    // Get current and target column mappings
    const currentSchema = this.registry.getSchemaAtVersion(event.event.table, event.schemaVersion);
    if (!currentSchema) {
      throw new Error(`Current schema version ${event.schemaVersion} not found for table ${event.event.table}`);
    }

    // Create column ID to name mapping for both schemas
    const currentIdToName = new Map(currentSchema.columns.map(c => [c.columnId, c.name]));
    const targetIdToName = new Map(targetSchema.columns.map(c => [c.columnId, c.name]));
    const targetNameToCol = new Map(targetSchema.columns.map(c => [c.name, c]));

    // Transform data
    const transformData = (data: T | undefined): T | undefined => {
      if (!data || typeof data !== 'object') return data;

      const transformed: Record<string, unknown> = {};
      const dataObj = data as Record<string, unknown>;

      // Map by column ID to handle renames
      for (const col of currentSchema.columns) {
        const currentName = col.name;
        const targetName = targetIdToName.get(col.columnId);

        if (targetName && currentName in dataObj) {
          transformed[targetName] = dataObj[currentName];
        }
      }

      // Add default values for new columns
      for (const col of targetSchema.columns) {
        if (!(col.name in transformed) && col.defaultValue !== undefined) {
          transformed[col.name] = this.parseDefaultValue(col.defaultValue, col.type);
        }
      }

      return transformed as T;
    };

    return {
      event: {
        ...event.event,
        data: transformData(event.event.data),
        oldData: transformData(event.event.oldData),
      },
      schemaVersion: targetVersion,
      schemaChecksum: targetSchema.checksum,
      columnIds: Array.from(new Set([
        ...targetSchema.columns.map(c => c.columnId).filter(id => event.columnIds.includes(id)),
      ])),
    };
  }

  private parseDefaultValue(defaultExpr: string, type: string): unknown {
    const upper = type.toUpperCase();
    const expr = defaultExpr.toLowerCase();

    if (expr === 'null') return null;
    if (expr === 'true') return true;
    if (expr === 'false') return false;

    if (upper.includes('INT') || upper === 'REAL' || upper === 'FLOAT' || upper === 'DOUBLE') {
      const num = parseFloat(defaultExpr);
      if (!isNaN(num)) return num;
    }

    if (expr === 'current_timestamp' || expr === 'now()') {
      return new Date();
    }

    // String value (strip quotes if present)
    if ((defaultExpr.startsWith("'") && defaultExpr.endsWith("'")) ||
        (defaultExpr.startsWith('"') && defaultExpr.endsWith('"'))) {
      return defaultExpr.slice(1, -1);
    }

    return defaultExpr;
  }
}

// =============================================================================
// Singleton Registry Instance
// =============================================================================

let globalRegistry: SchemaVersionRegistry | null = null;

/**
 * Get or create the global schema version registry
 */
export function getSchemaRegistry(): SchemaVersionRegistry {
  if (!globalRegistry) {
    globalRegistry = new SchemaVersionRegistry();
  }
  return globalRegistry;
}

/**
 * Reset the global schema version registry (for testing)
 */
export function resetSchemaRegistry(): void {
  globalRegistry = null;
}
