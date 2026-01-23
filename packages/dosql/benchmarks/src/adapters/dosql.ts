/**
 * DoSQL Benchmark Adapter
 *
 * Implements the BenchmarkAdapter interface using an in-memory SQL-like store.
 * This simulates DoSQL database operations for benchmarking purposes.
 */

import type {
  BenchmarkAdapter,
  BenchmarkOperation,
  TableSchemaConfig,
  StorageMetrics,
  ColdStartMetrics,
} from '../types.js';

// =============================================================================
// In-Memory Storage Types
// =============================================================================

interface TableData {
  schema: TableSchemaConfig;
  rows: Map<unknown, Record<string, unknown>>;
  indexes: Map<string, Map<unknown, Set<unknown>>>; // indexName -> indexValue -> primaryKeys
}

// =============================================================================
// DoSQL Adapter Implementation
// =============================================================================

/**
 * Configuration for DoSQLAdapter
 */
export interface DoSQLAdapterConfig {
  /** Enable verbose logging */
  verbose?: boolean;
}

/**
 * DoSQL benchmark adapter using in-memory storage
 *
 * Simulates DoSQL database operations for benchmarking purposes.
 * In production, this would connect to the actual DoSQL engine.
 */
export class DoSQLAdapter implements BenchmarkAdapter {
  readonly name = 'dosql';
  readonly version = '1.0.0';

  private tables: Map<string, TableData> = new Map();
  private initialized = false;
  private initStartTime: number = 0;
  private firstQueryTime: number | null = null;

  constructor(_config: DoSQLAdapterConfig = {}) {
    // Config reserved for future use
  }

  // ===========================================================================
  // Lifecycle Methods
  // ===========================================================================

  async initialize(): Promise<void> {
    if (this.initialized) {
      return;
    }

    this.initStartTime = performance.now();

    // Simulate DoSQL initialization
    this.tables = new Map();

    this.initialized = true;
  }

  async cleanup(): Promise<void> {
    this.tables.clear();
    this.initialized = false;
    this.firstQueryTime = null;
  }

  // ===========================================================================
  // Table Operations
  // ===========================================================================

  async createTable(schema: TableSchemaConfig): Promise<void> {
    const tableData: TableData = {
      schema,
      rows: new Map(),
      indexes: new Map(),
    };

    // Initialize index maps
    if (schema.indexes) {
      for (const indexCol of schema.indexes) {
        tableData.indexes.set(indexCol, new Map());
      }
    }

    this.tables.set(schema.tableName, tableData);
  }

  async dropTable(tableName: string): Promise<void> {
    this.tables.delete(tableName);
  }

  // ===========================================================================
  // CRUD Operations
  // ===========================================================================

  async insert(
    tableName: string,
    row: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    const startedAt = performance.now();

    try {
      const table = this.tables.get(tableName);
      if (!table) {
        throw new Error(`Table ${tableName} does not exist`);
      }

      const primaryKey = table.schema.primaryKey;
      const pkValue = row[primaryKey];

      // Insert row
      table.rows.set(pkValue, { ...row });

      // Update indexes
      for (const [indexCol, indexMap] of table.indexes) {
        const indexValue = row[indexCol];
        if (!indexMap.has(indexValue)) {
          indexMap.set(indexValue, new Set());
        }
        indexMap.get(indexValue)!.add(pkValue);
      }

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'create',
        sql: `INSERT INTO ${tableName} ...`,
        params: row,
        durationMs,
        rowCount: 1,
        success: true,
        startedAt,
      };
    } catch (e) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'create',
        sql: `INSERT INTO ${tableName} ...`,
        params: row,
        durationMs,
        rowCount: 0,
        success: false,
        error: e instanceof Error ? e.message : String(e),
        startedAt,
      };
    }
  }

  async insertBatch(
    tableName: string,
    rows: Record<string, unknown>[]
  ): Promise<BenchmarkOperation> {
    const startedAt = performance.now();

    if (rows.length === 0) {
      return {
        type: 'batch',
        sql: '',
        durationMs: 0,
        rowCount: 0,
        success: true,
        startedAt,
      };
    }

    try {
      const table = this.tables.get(tableName);
      if (!table) {
        throw new Error(`Table ${tableName} does not exist`);
      }

      const primaryKey = table.schema.primaryKey;

      for (const row of rows) {
        const pkValue = row[primaryKey];
        table.rows.set(pkValue, { ...row });

        // Update indexes
        for (const [indexCol, indexMap] of table.indexes) {
          const indexValue = row[indexCol];
          if (!indexMap.has(indexValue)) {
            indexMap.set(indexValue, new Set());
          }
          indexMap.get(indexValue)!.add(pkValue);
        }
      }

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'batch',
        sql: `INSERT BATCH (${rows.length} rows)`,
        durationMs,
        rowCount: rows.length,
        success: true,
        startedAt,
      };
    } catch (e) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'batch',
        sql: `INSERT BATCH (${rows.length} rows)`,
        durationMs,
        rowCount: 0,
        success: false,
        error: e instanceof Error ? e.message : String(e),
        startedAt,
      };
    }
  }

  async read(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown
  ): Promise<BenchmarkOperation> {
    const startedAt = performance.now();

    try {
      const table = this.tables.get(tableName);
      if (!table) {
        throw new Error(`Table ${tableName} does not exist`);
      }

      const row = table.rows.get(primaryKeyValue);
      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'read',
        sql: `SELECT * FROM ${tableName} WHERE ${primaryKey} = ?`,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: row ? 1 : 0,
        success: true,
        startedAt,
      };
    } catch (e) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'read',
        sql: `SELECT * FROM ${tableName} WHERE ${primaryKey} = ?`,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: 0,
        success: false,
        error: e instanceof Error ? e.message : String(e),
        startedAt,
      };
    }
  }

  async readMany(
    tableName: string,
    whereClause: string,
    params?: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    const startedAt = performance.now();

    try {
      const table = this.tables.get(tableName);
      if (!table) {
        throw new Error(`Table ${tableName} does not exist`);
      }

      // Simple implementation: scan all rows
      const matchingRows: Record<string, unknown>[] = [];
      for (const row of table.rows.values()) {
        // Simple BETWEEN parsing for range queries
        if (whereClause.includes('BETWEEN') && params) {
          const start = params['start'] as number;
          const end = params['end'] as number;
          const id = row['id'] as number;
          if (id >= start && id <= end) {
            matchingRows.push(row);
          }
        } else {
          matchingRows.push(row);
        }
      }

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'read',
        sql: `SELECT * FROM ${tableName} WHERE ${whereClause}`,
        params,
        durationMs,
        rowCount: matchingRows.length,
        success: true,
        startedAt,
      };
    } catch (e) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'read',
        sql: `SELECT * FROM ${tableName} WHERE ${whereClause}`,
        params,
        durationMs,
        rowCount: 0,
        success: false,
        error: e instanceof Error ? e.message : String(e),
        startedAt,
      };
    }
  }

  async update(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown,
    updates: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    const startedAt = performance.now();

    try {
      const table = this.tables.get(tableName);
      if (!table) {
        throw new Error(`Table ${tableName} does not exist`);
      }

      const row = table.rows.get(primaryKeyValue);
      if (!row) {
        const durationMs = performance.now() - startedAt;
        return {
          type: 'update',
          sql: `UPDATE ${tableName} SET ... WHERE ${primaryKey} = ?`,
          params: { ...updates, [primaryKey]: primaryKeyValue },
          durationMs,
          rowCount: 0,
          success: true,
          startedAt,
        };
      }

      // Update indexes for changed values
      for (const [indexCol, indexMap] of table.indexes) {
        if (indexCol in updates) {
          const oldValue = row[indexCol];
          const newValue = updates[indexCol];
          if (oldValue !== newValue) {
            // Remove from old index
            const oldSet = indexMap.get(oldValue);
            if (oldSet) {
              oldSet.delete(primaryKeyValue);
            }
            // Add to new index
            if (!indexMap.has(newValue)) {
              indexMap.set(newValue, new Set());
            }
            indexMap.get(newValue)!.add(primaryKeyValue);
          }
        }
      }

      // Apply updates
      Object.assign(row, updates);

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'update',
        sql: `UPDATE ${tableName} SET ... WHERE ${primaryKey} = ?`,
        params: { ...updates, [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: 1,
        success: true,
        startedAt,
      };
    } catch (e) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'update',
        sql: `UPDATE ${tableName} SET ... WHERE ${primaryKey} = ?`,
        params: { ...updates, [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: 0,
        success: false,
        error: e instanceof Error ? e.message : String(e),
        startedAt,
      };
    }
  }

  async delete(
    tableName: string,
    primaryKey: string,
    primaryKeyValue: unknown
  ): Promise<BenchmarkOperation> {
    const startedAt = performance.now();

    try {
      const table = this.tables.get(tableName);
      if (!table) {
        throw new Error(`Table ${tableName} does not exist`);
      }

      const row = table.rows.get(primaryKeyValue);
      if (!row) {
        const durationMs = performance.now() - startedAt;
        return {
          type: 'delete',
          sql: `DELETE FROM ${tableName} WHERE ${primaryKey} = ?`,
          params: { [primaryKey]: primaryKeyValue },
          durationMs,
          rowCount: 0,
          success: true,
          startedAt,
        };
      }

      // Remove from indexes
      for (const [indexCol, indexMap] of table.indexes) {
        const indexValue = row[indexCol];
        const indexSet = indexMap.get(indexValue);
        if (indexSet) {
          indexSet.delete(primaryKeyValue);
        }
      }

      // Delete row
      table.rows.delete(primaryKeyValue);

      const durationMs = performance.now() - startedAt;
      this.recordFirstQuery(durationMs);

      return {
        type: 'delete',
        sql: `DELETE FROM ${tableName} WHERE ${primaryKey} = ?`,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: 1,
        success: true,
        startedAt,
      };
    } catch (e) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'delete',
        sql: `DELETE FROM ${tableName} WHERE ${primaryKey} = ?`,
        params: { [primaryKey]: primaryKeyValue },
        durationMs,
        rowCount: 0,
        success: false,
        error: e instanceof Error ? e.message : String(e),
        startedAt,
      };
    }
  }

  async query(
    sql: string,
    params?: Record<string, unknown>
  ): Promise<BenchmarkOperation> {
    const startedAt = performance.now();

    // Simplified query implementation
    const durationMs = performance.now() - startedAt;
    this.recordFirstQuery(durationMs);

    return {
      type: 'query',
      sql,
      params,
      durationMs,
      rowCount: 0,
      success: true,
      startedAt,
    };
  }

  async transaction(
    operations: Array<{
      type: 'insert' | 'update' | 'delete';
      tableName: string;
      data: Record<string, unknown>;
    }>
  ): Promise<BenchmarkOperation> {
    const startedAt = performance.now();
    let rowCount = 0;

    try {
      for (const op of operations) {
        switch (op.type) {
          case 'insert': {
            const result = await this.insert(op.tableName, op.data);
            if (!result.success) {
              throw new Error(result.error || 'Insert failed');
            }
            rowCount += result.rowCount;
            break;
          }
          case 'update': {
            const { id, ...updates } = op.data;
            const result = await this.update(op.tableName, 'id', id, updates);
            if (!result.success) {
              throw new Error(result.error || 'Update failed');
            }
            rowCount += result.rowCount;
            break;
          }
          case 'delete': {
            const { id } = op.data;
            const result = await this.delete(op.tableName, 'id', id);
            if (!result.success) {
              throw new Error(result.error || 'Delete failed');
            }
            rowCount += result.rowCount;
            break;
          }
        }
      }

      const durationMs = performance.now() - startedAt;

      return {
        type: 'transaction',
        sql: `TRANSACTION (${operations.length} operations)`,
        durationMs,
        rowCount,
        success: true,
        startedAt,
      };
    } catch (e) {
      const durationMs = performance.now() - startedAt;
      return {
        type: 'transaction',
        sql: `TRANSACTION (${operations.length} operations)`,
        durationMs,
        rowCount: 0,
        success: false,
        error: e instanceof Error ? e.message : String(e),
        startedAt,
      };
    }
  }

  // ===========================================================================
  // Metrics
  // ===========================================================================

  async getStorageMetrics(): Promise<StorageMetrics> {
    let totalRows = 0;

    for (const table of this.tables.values()) {
      totalRows += table.rows.size;
    }

    // Estimate 100 bytes per row
    const estimatedBytes = totalRows * 100;

    return {
      totalBytes: estimatedBytes,
      rowCount: totalRows,
      tableCount: this.tables.size,
      limitUtilization: 0,
    };
  }

  async measureColdStart(): Promise<ColdStartMetrics> {
    const initTime =
      this.initStartTime > 0 ? performance.now() - this.initStartTime : 0;

    return {
      timeToFirstQuery: this.firstQueryTime ?? initTime,
      initializationTime: initTime,
      connectionTime: 0,
    };
  }

  // ===========================================================================
  // Private Methods
  // ===========================================================================

  private recordFirstQuery(durationMs: number): void {
    if (this.firstQueryTime === null) {
      this.firstQueryTime = durationMs;
    }
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a DoSQL benchmark adapter
 */
export function createDoSQLAdapter(config: DoSQLAdapterConfig = {}): DoSQLAdapter {
  return new DoSQLAdapter(config);
}
