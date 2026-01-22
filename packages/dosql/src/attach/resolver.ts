/**
 * DoSQL Schema Resolution
 *
 * Handles resolution of table and column references across attached databases:
 * - Resolves qualified names: db.table.column
 * - Detects and reports ambiguous references
 * - Builds query scopes for name resolution
 */

import type {
  SchemaResolver,
  QualifiedTableRef,
  QualifiedColumnRef,
  ResolvedTableRef,
  ResolvedColumnRef,
  QueryScope,
  AmbiguityCheck,
  AttachError,
  DatabaseManager,
  AttachedDatabase,
} from './types.js';
import type { Schema, TableSchema, ColumnDef } from '../engine/types.js';

// =============================================================================
// QUALIFIED NAME PARSER
// =============================================================================

/**
 * Parse a potentially qualified table name
 * Formats: table, db.table
 */
export function parseQualifiedTableName(name: string): QualifiedTableRef {
  const parts = name.split('.');

  if (parts.length === 1) {
    return { table: parts[0] };
  } else if (parts.length === 2) {
    return { database: parts[0], table: parts[1] };
  } else {
    // Too many parts - treat as invalid
    throw new Error(`Invalid qualified table name: ${name}`);
  }
}

/**
 * Parse a potentially qualified column name
 * Formats: column, table.column, db.table.column
 */
export function parseQualifiedColumnName(name: string): QualifiedColumnRef {
  const parts = name.split('.');

  if (parts.length === 1) {
    return { column: parts[0] };
  } else if (parts.length === 2) {
    return { table: parts[0], column: parts[1] };
  } else if (parts.length === 3) {
    return { database: parts[0], table: parts[1], column: parts[2] };
  } else {
    throw new Error(`Invalid qualified column name: ${name}`);
  }
}

/**
 * Format a qualified table reference as string
 */
export function formatQualifiedTableName(ref: QualifiedTableRef): string {
  if (ref.database) {
    return `${ref.database}.${ref.table}`;
  }
  return ref.table;
}

/**
 * Format a qualified column reference as string
 */
export function formatQualifiedColumnName(ref: QualifiedColumnRef): string {
  const parts: string[] = [];
  if (ref.database) {
    parts.push(ref.database);
  }
  if (ref.table) {
    parts.push(ref.table);
  }
  parts.push(ref.column);
  return parts.join('.');
}

// =============================================================================
// SCHEMA RESOLVER IMPLEMENTATION
// =============================================================================

/**
 * Implementation of the schema resolver
 */
export class AttachSchemaResolver implements SchemaResolver {
  private _manager: DatabaseManager;

  constructor(manager: DatabaseManager) {
    this._manager = manager;
  }

  /**
   * Resolve a table reference to its database location
   */
  resolveTable(ref: QualifiedTableRef): ResolvedTableRef | AttachError {
    // If database is explicitly specified
    if (ref.database) {
      const db = this._manager.get(ref.database);
      if (!db) {
        return {
          code: 'DATABASE_NOT_FOUND',
          message: `No such database: ${ref.database}`,
          details: { alias: ref.database },
        };
      }

      const tableDef = db.schema.tables.get(ref.table);
      if (!tableDef) {
        return {
          code: 'TABLE_NOT_FOUND',
          message: `No such table: ${ref.database}.${ref.table}`,
          details: { tables: [ref.table] },
        };
      }

      return {
        database: ref.database,
        tableDef,
        storagePath: `${ref.database}/${ref.table}`,
        isCrossDatabase: ref.database !== 'main',
      };
    }

    // Search for table across all databases
    const candidates: Array<{ database: string; tableDef: TableSchema }> = [];

    // Search main first (has priority)
    const mainDb = this._manager.getMain();
    const mainTable = mainDb.schema.tables.get(ref.table);
    if (mainTable) {
      candidates.push({ database: 'main', tableDef: mainTable });
    }

    // Search temp next
    const tempDb = this._manager.getTemp();
    const tempTable = tempDb.schema.tables.get(ref.table);
    if (tempTable) {
      // Temp has highest priority for unqualified names
      candidates.unshift({ database: 'temp', tableDef: tempTable });
    }

    // Search other databases
    for (const alias of this._manager.aliases()) {
      if (alias === 'main' || alias === 'temp') continue;

      const db = this._manager.get(alias);
      if (!db) continue;

      const tableDef = db.schema.tables.get(ref.table);
      if (tableDef) {
        candidates.push({ database: alias, tableDef });
      }
    }

    // No matches
    if (candidates.length === 0) {
      return {
        code: 'TABLE_NOT_FOUND',
        message: `No such table: ${ref.table}`,
        details: { tables: [ref.table] },
      };
    }

    // Ambiguous if multiple matches (excluding temp priority)
    if (candidates.length > 1 && candidates[0].database !== 'temp') {
      // Check if all are the same table name in different DBs
      const databases = candidates.map(c => c.database);
      return {
        code: 'AMBIGUOUS_TABLE',
        message: `Ambiguous table name: ${ref.table}. Found in databases: ${databases.join(', ')}`,
        details: { tables: [ref.table], databases },
      };
    }

    // Return first (highest priority) match
    const match = candidates[0];
    return {
      database: match.database,
      tableDef: match.tableDef,
      storagePath: `${match.database}/${ref.table}`,
      isCrossDatabase: match.database !== 'main',
    };
  }

  /**
   * Resolve a column reference within a query scope
   */
  resolveColumn(ref: QualifiedColumnRef, scope: QueryScope): ResolvedColumnRef | AttachError {
    // Fully qualified: db.table.column
    if (ref.database && ref.table) {
      return this._resolveFullyQualifiedColumn(ref);
    }

    // Partially qualified: table.column
    if (ref.table) {
      return this._resolvePartiallyQualifiedColumn(ref, scope);
    }

    // Unqualified: column
    return this._resolveUnqualifiedColumn(ref, scope);
  }

  /**
   * Resolve fully qualified column reference
   */
  private _resolveFullyQualifiedColumn(ref: QualifiedColumnRef): ResolvedColumnRef | AttachError {
    const db = this._manager.get(ref.database!);
    if (!db) {
      return {
        code: 'DATABASE_NOT_FOUND',
        message: `No such database: ${ref.database}`,
        details: { alias: ref.database },
      };
    }

    const tableDef = db.schema.tables.get(ref.table!);
    if (!tableDef) {
      return {
        code: 'TABLE_NOT_FOUND',
        message: `No such table: ${ref.database}.${ref.table}`,
        details: { tables: [ref.table!] },
      };
    }

    const columnDef = tableDef.columns.find(c => c.name === ref.column);
    if (!columnDef) {
      return {
        code: 'COLUMN_NOT_FOUND',
        message: `No such column: ${ref.database}.${ref.table}.${ref.column}`,
        details: { columns: [ref.column] },
      };
    }

    return {
      database: ref.database!,
      table: ref.table!,
      columnDef: {
        name: columnDef.name,
        type: columnDef.type,
        nullable: columnDef.nullable,
        primaryKey: columnDef.primaryKey,
      },
      qualifiedName: `${ref.database}.${ref.table}.${ref.column}`,
    };
  }

  /**
   * Resolve table.column reference using scope
   */
  private _resolvePartiallyQualifiedColumn(
    ref: QualifiedColumnRef,
    scope: QueryScope
  ): ResolvedColumnRef | AttachError {
    // Look for table in scope (may be an alias)
    const tableRef = scope.tables.get(ref.table!);

    if (tableRef) {
      // Found in scope - resolve the actual table
      const resolved = this.resolveTable(tableRef);
      if ('code' in resolved) {
        return resolved as AttachError;
      }

      const columnDef = resolved.tableDef.columns.find(c => c.name === ref.column);
      if (!columnDef) {
        return {
          code: 'COLUMN_NOT_FOUND',
          message: `No such column: ${ref.table}.${ref.column}`,
          details: { columns: [ref.column] },
        };
      }

      return {
        database: resolved.database,
        table: resolved.tableDef.name,
        columnDef: {
          name: columnDef.name,
          type: columnDef.type,
          nullable: columnDef.nullable,
          primaryKey: columnDef.primaryKey,
        },
        qualifiedName: `${resolved.database}.${resolved.tableDef.name}.${ref.column}`,
      };
    }

    // Not in scope - try as a database-less qualified name
    const tableResolved = this.resolveTable({ table: ref.table! });
    if ('code' in tableResolved) {
      return tableResolved as AttachError;
    }

    const columnDef = tableResolved.tableDef.columns.find(c => c.name === ref.column);
    if (!columnDef) {
      return {
        code: 'COLUMN_NOT_FOUND',
        message: `No such column: ${ref.table}.${ref.column}`,
        details: { columns: [ref.column] },
      };
    }

    return {
      database: tableResolved.database,
      table: tableResolved.tableDef.name,
      columnDef: {
        name: columnDef.name,
        type: columnDef.type,
        nullable: columnDef.nullable,
        primaryKey: columnDef.primaryKey,
      },
      qualifiedName: `${tableResolved.database}.${tableResolved.tableDef.name}.${ref.column}`,
    };
  }

  /**
   * Resolve unqualified column reference using scope
   */
  private _resolveUnqualifiedColumn(
    ref: QualifiedColumnRef,
    scope: QueryScope
  ): ResolvedColumnRef | AttachError {
    // Check column aliases first
    const aliasRef = scope.columnAliases.get(ref.column);
    if (aliasRef) {
      return this.resolveColumn(aliasRef, scope);
    }

    // Search all tables in scope
    const candidates: ResolvedColumnRef[] = [];

    for (const [tableName, tableRef] of scope.tables) {
      const resolved = this.resolveTable(tableRef);
      if ('code' in resolved) continue;

      const columnDef = resolved.tableDef.columns.find(c => c.name === ref.column);
      if (columnDef) {
        candidates.push({
          database: resolved.database,
          table: resolved.tableDef.name,
          columnDef: {
            name: columnDef.name,
            type: columnDef.type,
            nullable: columnDef.nullable,
            primaryKey: columnDef.primaryKey,
          },
          qualifiedName: `${resolved.database}.${resolved.tableDef.name}.${ref.column}`,
        });
      }
    }

    // No matches
    if (candidates.length === 0) {
      // Try parent scope for correlated subqueries
      if (scope.parent) {
        return this._resolveUnqualifiedColumn(ref, scope.parent);
      }

      return {
        code: 'COLUMN_NOT_FOUND',
        message: `No such column: ${ref.column}`,
        details: { columns: [ref.column] },
      };
    }

    // Ambiguous
    if (candidates.length > 1) {
      return {
        code: 'AMBIGUOUS_COLUMN',
        message: `Ambiguous column name: ${ref.column}. Found in tables: ${candidates.map(c => `${c.database}.${c.table}`).join(', ')}`,
        details: {
          columns: [ref.column],
          tables: candidates.map(c => c.table),
          databases: candidates.map(c => c.database),
        },
      };
    }

    return candidates[0];
  }

  /**
   * Check for ambiguous table references
   */
  checkAmbiguity(tableName: string): AmbiguityCheck {
    const candidates: Array<{ database: string; table: string }> = [];

    for (const alias of this._manager.aliases()) {
      const db = this._manager.get(alias);
      if (!db) continue;

      if (db.schema.tables.has(tableName)) {
        candidates.push({ database: alias, table: tableName });
      }
    }

    if (candidates.length === 0) {
      return {
        isAmbiguous: false,
      };
    }

    if (candidates.length === 1) {
      const match = candidates[0];
      const db = this._manager.get(match.database)!;
      const tableDef = db.schema.tables.get(tableName)!;

      return {
        isAmbiguous: false,
        resolved: {
          database: match.database,
          table: tableName,
          columnDef: tableDef.columns[0] ? {
            name: tableDef.columns[0].name,
            type: tableDef.columns[0].type,
            nullable: tableDef.columns[0].nullable,
          } : { name: '', type: 'string', nullable: true },
          qualifiedName: `${match.database}.${tableName}`,
        },
      };
    }

    return {
      isAmbiguous: true,
      candidates,
    };
  }

  /**
   * Get all tables across all databases
   */
  getAllTables(): Map<string, QualifiedTableRef[]> {
    const result = new Map<string, QualifiedTableRef[]>();

    for (const alias of this._manager.aliases()) {
      const db = this._manager.get(alias);
      if (!db) continue;

      for (const tableName of db.schema.tables.keys()) {
        const refs = result.get(tableName) || [];
        refs.push({ database: alias, table: tableName });
        result.set(tableName, refs);
      }
    }

    return result;
  }

  /**
   * Build query scope from FROM clause tables
   */
  buildScope(fromTables: QualifiedTableRef[]): QueryScope {
    const tables = new Map<string, QualifiedTableRef>();

    for (const ref of fromTables) {
      // Use alias if provided, otherwise table name
      const key = ref.alias || ref.table;
      tables.set(key, ref);
    }

    return {
      tables,
      columnAliases: new Map(),
      subqueryAliases: new Map(),
    };
  }

  /**
   * Add a table to an existing scope
   */
  addTableToScope(scope: QueryScope, ref: QualifiedTableRef): QueryScope {
    const key = ref.alias || ref.table;
    scope.tables.set(key, ref);
    return scope;
  }

  /**
   * Add a column alias to a scope
   */
  addColumnAliasToScope(
    scope: QueryScope,
    alias: string,
    ref: QualifiedColumnRef
  ): QueryScope {
    scope.columnAliases.set(alias, ref);
    return scope;
  }

  /**
   * Create a child scope (for subqueries)
   */
  createChildScope(parent: QueryScope): QueryScope {
    return {
      tables: new Map(),
      columnAliases: new Map(),
      subqueryAliases: new Map(),
      parent,
    };
  }
}

// =============================================================================
// SQL PARSER HELPERS
// =============================================================================

/**
 * Parse ATTACH DATABASE statement
 * Format: ATTACH DATABASE 'path' AS alias
 */
export function parseAttachStatement(sql: string): {
  path: string;
  alias: string;
} | null {
  // Normalize whitespace
  const normalized = sql.trim().replace(/\s+/g, ' ');

  // Match ATTACH DATABASE pattern
  const match = normalized.match(
    /^ATTACH\s+(?:DATABASE\s+)?['"]([^'"]+)['"]\s+AS\s+(\w+)$/i
  );

  if (!match) {
    return null;
  }

  return {
    path: match[1],
    alias: match[2],
  };
}

/**
 * Parse DETACH DATABASE statement
 * Format: DETACH DATABASE alias
 */
export function parseDetachStatement(sql: string): { alias: string } | null {
  const normalized = sql.trim().replace(/\s+/g, ' ');

  const match = normalized.match(/^DETACH\s+(?:DATABASE\s+)?(\w+)$/i);

  if (!match) {
    return null;
  }

  return {
    alias: match[1],
  };
}

/**
 * Check if SQL is an ATTACH statement
 */
export function isAttachStatement(sql: string): boolean {
  return /^\s*ATTACH\s/i.test(sql);
}

/**
 * Check if SQL is a DETACH statement
 */
export function isDetachStatement(sql: string): boolean {
  return /^\s*DETACH\s/i.test(sql);
}

/**
 * Check if SQL is PRAGMA database_list
 */
export function isDatabaseListPragma(sql: string): boolean {
  return /^\s*PRAGMA\s+database_list\s*;?\s*$/i.test(sql);
}

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

/**
 * Create a schema resolver for a database manager
 */
export function createSchemaResolver(manager: DatabaseManager): AttachSchemaResolver {
  return new AttachSchemaResolver(manager);
}
