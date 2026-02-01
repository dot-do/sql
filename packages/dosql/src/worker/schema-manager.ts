/**
 * Schema Manager
 *
 * Extracted from database.ts to reduce the size of the main DoSQLDatabase class.
 * Handles table schema CRUD operations, auto-increment IDs, and default values.
 */

import type { BTree } from '../btree/index.js';
import type { DOStorageBackend } from '../fsx/index.js';
import type { WALWriter } from '../wal/index.js';

// =============================================================================
// Types
// =============================================================================

export interface TableSchema {
  name: string;
  columns: { name: string; type: string; defaultValue?: string }[];
  primaryKey: string;
}

// =============================================================================
// Schema Manager
// =============================================================================

export class SchemaManager {
  constructor(
    private tables: Map<string, TableSchema>,
    private maxIdCache: Map<string, number>,
    private fsx: DOStorageBackend,
  ) {}

  /**
   * Load table schemas from storage
   */
  async loadSchemas(): Promise<void> {
    const schemaData = await this.fsx.read('_meta/schemas');
    if (schemaData) {
      const schemas = JSON.parse(new TextDecoder().decode(schemaData)) as TableSchema[];
      for (const schema of schemas) {
        this.tables.set(schema.name, schema);
      }
    }
  }

  /**
   * Persist table schemas to storage
   */
  async persistSchemas(): Promise<void> {
    const schemas = Array.from(this.tables.values());
    const data = new TextEncoder().encode(JSON.stringify(schemas));
    await this.fsx.write('_meta/schemas', data);
  }

  /**
   * Get a table schema by name
   */
  getSchema(tableName: string): TableSchema | undefined {
    return this.tables.get(tableName);
  }

  /**
   * Get all table schemas
   */
  getAllSchemas(): TableSchema[] {
    return Array.from(this.tables.values());
  }

  /**
   * Get schema column names for a table
   */
  getSchemaColumns(tableName: string): string[] {
    const schema = this.tables.get(tableName);
    if (!schema) return [];
    return schema.columns.map((c) => c.name);
  }

  /**
   * CREATE TABLE implementation
   */
  async executeCreateTable(
    sql: string
  ): Promise<{ rows: Record<string, unknown>[]; rowsAffected: number }> {
    // Simple parser: CREATE TABLE name (col1 TYPE, col2 TYPE, PRIMARY KEY (col))
    const match = sql.match(/CREATE\s+TABLE\s+(\w+)\s*\(([\s\S]+)\)/i);
    if (!match) {
      throw new Error('Invalid CREATE TABLE syntax');
    }

    const tableName = match[1];
    const columnDefs = match[2];

    // Parse columns
    const columns: { name: string; type: string; defaultValue?: string }[] = [];
    let primaryKey = 'id';

    const parts = columnDefs.split(',').map((p) => p.trim());
    for (const part of parts) {
      // Check for standalone PRIMARY KEY constraint
      const pkMatch = part.match(/PRIMARY\s+KEY\s*\((\w+)\)/i);
      if (pkMatch) {
        primaryKey = pkMatch[1];
        continue;
      }

      // Parse column with optional DEFAULT and inline PRIMARY KEY
      // Format: column_name TYPE [PRIMARY KEY] [DEFAULT value]
      const colMatch = part.match(/(\w+)\s+(\w+)(?:\s+PRIMARY\s+KEY)?(?:\s+DEFAULT\s+(\w+|'[^']*'))?/i);
      if (colMatch) {
        const column: { name: string; type: string; defaultValue?: string } = {
          name: colMatch[1],
          type: colMatch[2],
        };
        if (colMatch[3]) {
          column.defaultValue = colMatch[3];
        }
        columns.push(column);

        // Check if this column has inline PRIMARY KEY
        if (/\bPRIMARY\s+KEY\b/i.test(part)) {
          primaryKey = column.name;
        }
      }
    }

    const schema: TableSchema = { name: tableName, columns, primaryKey };
    this.tables.set(tableName, schema);

    // Persist schemas
    await this.persistSchemas();

    return { rows: [], rowsAffected: 0 };
  }

  /**
   * DROP TABLE implementation
   */
  async executeDropTable(
    sql: string,
    btree: BTree<string, Record<string, unknown>>,
    wal: WALWriter | null
  ): Promise<{ rows: Record<string, unknown>[]; rowsAffected: number }> {
    // Parse: DROP TABLE [IF EXISTS] tablename
    const match = sql.match(/DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(\w+)/i);
    if (!match) {
      throw new Error('Invalid DROP TABLE syntax');
    }

    const tableName = match[1];
    const ifExists = /IF\s+EXISTS/i.test(sql);

    // Check if table exists
    const schema = this.tables.get(tableName);
    if (!schema) {
      if (ifExists) {
        // IF EXISTS specified, silently succeed
        return { rows: [], rowsAffected: 0 };
      }
      throw new Error(`no such table: ${tableName}`);
    }

    // Delete all rows from the B-tree
    const prefix = `${tableName}:`;
    const keysToDelete: string[] = [];

    for await (const [key] of btree.range(prefix, prefix + '\uffff')) {
      keysToDelete.push(key);
    }

    for (const key of keysToDelete) {
      await btree.delete(key);
    }

    // Remove table from schema and ID cache
    this.tables.delete(tableName);
    this.maxIdCache.delete(tableName);

    // Persist updated schemas
    await this.persistSchemas();

    // Write WAL entry for DROP TABLE (sync:true ensures flush before acknowledging)
    if (wal) {
      await wal.append({
        timestamp: Date.now(),
        txnId: `txn_${Date.now()}`,
        op: 'DELETE', // Use DELETE op to represent table drop
        table: tableName,
        before: new TextEncoder().encode(JSON.stringify({ _dropped: true, rowCount: keysToDelete.length })),
      }, { sync: true });
    }

    return { rows: [], rowsAffected: keysToDelete.length };
  }

  /**
   * Get next auto-increment ID for a table.
   * Uses an in-memory cache to avoid a full table scan on every insert.
   * The cache is populated once on first access and then incremented.
   */
  async getNextId(tableName: string, btree: BTree<string, Record<string, unknown>>): Promise<number> {
    if (!this.maxIdCache.has(tableName)) {
      // First access: scan once to find the current max ID
      let maxId = 0;
      const prefix = `${tableName}:`;

      for await (const [_key, value] of btree.range(prefix, prefix + '\uffff')) {
        const id = Number(value.id);
        if (!isNaN(id) && id > maxId) {
          maxId = id;
        }
      }

      this.maxIdCache.set(tableName, maxId);
    }

    const nextId = this.maxIdCache.get(tableName)! + 1;
    this.maxIdCache.set(tableName, nextId);
    return nextId;
  }

  /**
   * Update the max ID cache when an explicit ID is inserted.
   * Ensures the cache stays consistent so auto-increment never collides.
   */
  updateMaxIdCache(tableName: string, pkValue: unknown): void {
    const id = Number(pkValue);
    if (!isNaN(id)) {
      const current = this.maxIdCache.get(tableName) ?? 0;
      if (id > current) {
        this.maxIdCache.set(tableName, id);
      }
    }
  }

  /**
   * Evaluate a DEFAULT value expression
   */
  evaluateDefaultValue(defaultValue: string): unknown {
    const upper = defaultValue.toUpperCase();

    // Handle CURRENT_TIMESTAMP and similar date/time functions
    if (upper === 'CURRENT_TIMESTAMP' || upper === 'DATETIME()' || upper === "DATETIME('NOW')") {
      return new Date().toISOString().replace('T', ' ').split('.')[0];
    }
    if (upper === 'CURRENT_DATE' || upper === 'DATE()' || upper === "DATE('NOW')") {
      return new Date().toISOString().split('T')[0];
    }
    if (upper === 'CURRENT_TIME' || upper === 'TIME()' || upper === "TIME('NOW')") {
      return new Date().toISOString().split('T')[1].split('.')[0];
    }

    // Handle NULL
    if (upper === 'NULL') {
      return null;
    }

    // Handle string literals
    if (defaultValue.startsWith("'") && defaultValue.endsWith("'")) {
      return defaultValue.slice(1, -1);
    }

    // Handle numbers
    const num = Number(defaultValue);
    if (!isNaN(num)) {
      return num;
    }

    // Return as-is for other cases
    return defaultValue;
  }
}
