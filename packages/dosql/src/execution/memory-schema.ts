/**
 * In-Memory Schema Manager
 *
 * Simple in-memory schema management for testing and embedding.
 */

import type {
  SchemaManager,
  TableSchema,
  KVStorage,
} from './types.js';

/**
 * In-memory schema manager that stores schemas in memory.
 * Useful for testing and embedding scenarios.
 */
export class MemorySchemaManager implements SchemaManager {
  private tables = new Map<string, TableSchema>();
  private maxIdCache = new Map<string, number>();
  private storage: KVStorage;

  constructor(storage: KVStorage) {
    this.storage = storage;
  }

  getSchema(tableName: string): TableSchema | undefined {
    return this.tables.get(tableName);
  }

  getAllSchemas(): TableSchema[] {
    return [...this.tables.values()];
  }

  getSchemaColumns(tableName: string): string[] {
    const schema = this.tables.get(tableName);
    return schema ? schema.columns.map((c) => c.name) : [];
  }

  async createTable(schema: TableSchema): Promise<void> {
    this.tables.set(schema.name, schema);
  }

  async dropTable(tableName: string): Promise<boolean> {
    const existed = this.tables.has(tableName);
    this.tables.delete(tableName);
    this.maxIdCache.delete(tableName);
    return existed;
  }

  evaluateDefaultValue(defaultValue: string): unknown {
    const upper = defaultValue.toUpperCase();

    // Handle CURRENT_TIMESTAMP and similar date/time functions
    if (
      upper === 'CURRENT_TIMESTAMP' ||
      upper === 'DATETIME()' ||
      upper === "DATETIME('NOW')"
    ) {
      return new Date().toISOString().replace('T', ' ').split('.')[0];
    }
    if (
      upper === 'CURRENT_DATE' ||
      upper === 'DATE()' ||
      upper === "DATE('NOW')"
    ) {
      return new Date().toISOString().split('T')[0];
    }
    if (
      upper === 'CURRENT_TIME' ||
      upper === 'TIME()' ||
      upper === "TIME('NOW')"
    ) {
      return new Date().toISOString().split('T')[1]!.split('.')[0];
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

  async getNextId(tableName: string): Promise<number> {
    if (!this.maxIdCache.has(tableName)) {
      // Scan storage to find max ID
      let maxId = 0;
      const prefix = `${tableName}:`;

      for await (const [_key, value] of this.storage.range(
        prefix,
        prefix + '\uffff'
      )) {
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
   * Clear all schemas (useful for test cleanup)
   */
  clear(): void {
    this.tables.clear();
    this.maxIdCache.clear();
  }
}

/**
 * Create a new in-memory schema manager
 */
export function createMemorySchemaManager(storage: KVStorage): MemorySchemaManager {
  return new MemorySchemaManager(storage);
}
