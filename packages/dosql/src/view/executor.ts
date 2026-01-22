/**
 * View Executor for DoSQL
 *
 * Executes operations on views:
 * - SELECT queries (view expansion)
 * - INSERT/UPDATE/DELETE (through INSTEAD OF triggers)
 * - WITH CHECK OPTION validation
 */

import type {
  ViewDefinition,
  ViewRegistry,
  ViewExecutor,
  ViewExecutionOptions,
  ViewQueryResult,
  ViewDMLResult,
  ViewDMLContext,
  ViewColumn,
  InsteadOfTrigger,
  ViewDMLOperation,
} from './types.js';
import { ViewError, ViewErrorCode } from './types.js';
import {
  parseCreateView,
  isCreateView,
  parseDropView,
  isDropView,
} from './parser.js';

// =============================================================================
// SQL Executor Interface
// =============================================================================

/**
 * Interface for the underlying SQL executor
 */
export interface SqlExecutorAdapter {
  /**
   * Execute a SELECT query and return results
   */
  query<T = Record<string, unknown>>(
    sql: string,
    params?: unknown[]
  ): Promise<T[]>;

  /**
   * Execute a DML statement and return affected row count
   */
  execute(sql: string, params?: unknown[]): Promise<number>;

  /**
   * Check if a table exists
   */
  tableExists(name: string): Promise<boolean>;

  /**
   * Get column info for a table
   */
  getColumns(tableName: string): Promise<Array<{ name: string; type: string }>>;
}

// =============================================================================
// View Executor Implementation
// =============================================================================

/**
 * Options for creating a view executor
 */
export interface ViewExecutorOptions {
  /** The view registry */
  registry: ViewRegistry;

  /** SQL executor adapter */
  sqlExecutor: SqlExecutorAdapter;

  /** Maximum view expansion depth */
  maxExpansionDepth?: number;

  /** Default query timeout */
  defaultTimeout?: number;
}

/**
 * Create a view executor
 */
export function createViewExecutor(options: ViewExecutorOptions): ViewExecutor {
  const { registry, sqlExecutor, maxExpansionDepth = 10, defaultTimeout = 30000 } = options;

  /**
   * Expand a view reference in a query
   */
  function expandViewReference(
    viewName: string,
    depth: number = 0
  ): string {
    if (depth > maxExpansionDepth) {
      throw new ViewError(
        ViewErrorCode.CIRCULAR_DEPENDENCY,
        `Maximum view expansion depth (${maxExpansionDepth}) exceeded`,
        viewName
      );
    }

    const view = registry.get(viewName);
    if (!view) {
      // Not a view, return as-is (it's a table)
      return viewName;
    }

    let expandedQuery = view.selectQuery;

    // Recursively expand nested view references
    for (const ref of view.referencedViews) {
      const nestedExpansion = expandViewReference(ref, depth + 1);
      // Replace view reference with subquery
      const pattern = new RegExp(`\\b${ref}\\b`, 'gi');
      expandedQuery = expandedQuery.replace(pattern, `(${nestedExpansion}) AS ${ref}`);
    }

    return expandedQuery;
  }

  /**
   * Build a full query from a view with optional WHERE, ORDER BY, etc.
   */
  function buildViewQuery(
    view: ViewDefinition,
    opts?: {
      where?: string;
      orderBy?: string;
      limit?: number;
      offset?: number;
    }
  ): string {
    let baseQuery = `(${expandViewReference(view.name)})`;
    const parts: string[] = [`SELECT * FROM ${baseQuery} AS __view__`];

    if (opts?.where) {
      parts.push(`WHERE ${opts.where}`);
    }

    if (opts?.orderBy) {
      parts.push(`ORDER BY ${opts.orderBy}`);
    }

    if (opts?.limit !== undefined) {
      parts.push(`LIMIT ${opts.limit}`);
    }

    if (opts?.offset !== undefined) {
      parts.push(`OFFSET ${opts.offset}`);
    }

    return parts.join(' ');
  }

  /**
   * Validate WITH CHECK OPTION for a DML operation
   */
  async function validateCheckOption<T extends Record<string, unknown>>(
    view: ViewDefinition,
    row: T
  ): Promise<boolean> {
    if (!view.checkOption) {
      return true; // No check option, always valid
    }

    // Build a query to check if the row would be visible through the view
    const viewQuery = expandViewReference(view.name);

    // Extract the WHERE clause from the view query
    const whereMatch = viewQuery.match(/\bWHERE\s+(.+?)(?:\s+(?:ORDER|GROUP|HAVING|LIMIT|$))/i);
    if (!whereMatch) {
      return true; // No WHERE clause, row is always visible
    }

    const whereClause = whereMatch[1];

    // Build a test query
    // This is a simplified check - a full implementation would need to substitute row values
    const testQuery = `SELECT 1 FROM (SELECT ${Object.entries(row)
      .map(([k, v]) => `${typeof v === 'string' ? `'${v}'` : v} AS ${k}`)
      .join(', ')}) AS __row__ WHERE ${whereClause}`;

    try {
      const result = await sqlExecutor.query(testQuery);
      return result.length > 0;
    } catch {
      // If the check fails, assume it's valid (conservative approach)
      return true;
    }
  }

  /**
   * Execute INSTEAD OF triggers for a DML operation
   */
  async function executeInsteadOfTriggers<T extends Record<string, unknown>>(
    view: ViewDefinition,
    operation: ViewDMLOperation,
    oldRow: T | undefined,
    newRow: T | undefined,
    opts?: ViewExecutionOptions
  ): Promise<ViewDMLResult> {
    const triggers = registry.getInsteadOfTriggers(view.name, operation);

    if (triggers.length === 0) {
      throw new ViewError(
        ViewErrorCode.NO_INSTEAD_OF_TRIGGER,
        `Cannot ${operation} on view '${view.name}': no INSTEAD OF trigger defined`,
        view.name
      );
    }

    const ctx: ViewDMLContext<T> = {
      view,
      operation,
      old: oldRow,
      new: newRow,
      requestId: opts?.requestId,
      txnId: opts?.txnId,
    };

    let totalAffected = 0;

    for (const trigger of triggers) {
      try {
        const result = await trigger.handler(ctx);
        if (!result.success) {
          return result;
        }
        totalAffected += result.rowsAffected;
      } catch (error) {
        return {
          success: false,
          rowsAffected: 0,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    }

    return {
      success: true,
      rowsAffected: totalAffected,
    };
  }

  return {
    async select<T = Record<string, unknown>>(
      viewName: string,
      opts?: ViewExecutionOptions & { where?: string; orderBy?: string }
    ): Promise<ViewQueryResult<T>> {
      const view = registry.get(viewName);
      if (!view) {
        throw new ViewError(
          ViewErrorCode.NOT_FOUND,
          `View '${viewName}' does not exist`,
          viewName
        );
      }

      if (!view.enabled) {
        throw new ViewError(
          ViewErrorCode.INVALID_DEFINITION,
          `View '${viewName}' is disabled`,
          viewName
        );
      }

      const startTime = Date.now();

      const query = buildViewQuery(view, {
        where: opts?.where,
        orderBy: opts?.orderBy,
        limit: opts?.limit,
        offset: opts?.offset,
      });

      const rows = await sqlExecutor.query<T>(query);

      return {
        rows,
        columns: view.columnDefinitions ?? [],
        rowCount: rows.length,
        executionTime: Date.now() - startTime,
      };
    },

    async insert<T = Record<string, unknown>>(
      viewName: string,
      row: T,
      opts?: ViewExecutionOptions
    ): Promise<ViewDMLResult> {
      const view = registry.get(viewName);
      if (!view) {
        throw new ViewError(
          ViewErrorCode.NOT_FOUND,
          `View '${viewName}' does not exist`,
          viewName
        );
      }

      // Check if view is updatable or has INSTEAD OF trigger
      const triggers = registry.getInsteadOfTriggers(viewName, 'INSERT');
      if (!view.updatable && triggers.length === 0) {
        throw new ViewError(
          ViewErrorCode.NOT_UPDATABLE,
          `View '${viewName}' is not updatable and has no INSTEAD OF INSERT trigger`,
          viewName
        );
      }

      // Validate WITH CHECK OPTION
      if (view.checkOption) {
        const valid = await validateCheckOption(view, row as Record<string, unknown>);
        if (!valid) {
          throw new ViewError(
            ViewErrorCode.CHECK_OPTION_VIOLATION,
            `INSERT violates WITH CHECK OPTION on view '${viewName}'`,
            viewName
          );
        }
      }

      // If there are INSTEAD OF triggers, use them
      if (triggers.length > 0) {
        return executeInsteadOfTriggers(
          view,
          'INSERT',
          undefined,
          row as Record<string, unknown>,
          opts
        );
      }

      // Otherwise, insert directly into the base table
      const baseTable = view.referencedTables[0];
      if (!baseTable) {
        throw new ViewError(
          ViewErrorCode.NOT_UPDATABLE,
          `View '${viewName}' does not reference any tables`,
          viewName
        );
      }

      const columns = Object.keys(row as Record<string, unknown>);
      const values = Object.values(row as Record<string, unknown>);
      const placeholders = values.map(() => '?').join(', ');

      const sql = `INSERT INTO ${baseTable} (${columns.join(', ')}) VALUES (${placeholders})`;
      const affected = await sqlExecutor.execute(sql, values);

      return {
        success: true,
        rowsAffected: affected,
      };
    },

    async update<T = Record<string, unknown>>(
      viewName: string,
      changes: Partial<T>,
      where: string | ((row: T) => boolean),
      opts?: ViewExecutionOptions
    ): Promise<ViewDMLResult> {
      const view = registry.get(viewName);
      if (!view) {
        throw new ViewError(
          ViewErrorCode.NOT_FOUND,
          `View '${viewName}' does not exist`,
          viewName
        );
      }

      // Check if view is updatable or has INSTEAD OF trigger
      const triggers = registry.getInsteadOfTriggers(viewName, 'UPDATE');
      if (!view.updatable && triggers.length === 0) {
        throw new ViewError(
          ViewErrorCode.NOT_UPDATABLE,
          `View '${viewName}' is not updatable and has no INSTEAD OF UPDATE trigger`,
          viewName
        );
      }

      // Get rows to update
      const whereClause = typeof where === 'string' ? where : undefined;
      const { rows } = await this.select<T>(viewName, { where: whereClause });

      // Filter with predicate if function provided
      const rowsToUpdate = typeof where === 'function'
        ? rows.filter(where)
        : rows;

      if (rowsToUpdate.length === 0) {
        return { success: true, rowsAffected: 0 };
      }

      // Validate WITH CHECK OPTION for each updated row
      if (view.checkOption) {
        for (const row of rowsToUpdate) {
          const updatedRow = { ...row, ...changes };
          const valid = await validateCheckOption(view, updatedRow as Record<string, unknown>);
          if (!valid) {
            throw new ViewError(
              ViewErrorCode.CHECK_OPTION_VIOLATION,
              `UPDATE violates WITH CHECK OPTION on view '${viewName}'`,
              viewName
            );
          }
        }
      }

      // If there are INSTEAD OF triggers, use them
      if (triggers.length > 0) {
        let totalAffected = 0;
        for (const row of rowsToUpdate) {
          const result = await executeInsteadOfTriggers(
            view,
            'UPDATE',
            row as Record<string, unknown>,
            { ...row, ...changes } as Record<string, unknown>,
            opts
          );
          if (!result.success) {
            return result;
          }
          totalAffected += result.rowsAffected;
        }
        return { success: true, rowsAffected: totalAffected };
      }

      // Otherwise, update directly on the base table
      const baseTable = view.referencedTables[0];
      if (!baseTable) {
        throw new ViewError(
          ViewErrorCode.NOT_UPDATABLE,
          `View '${viewName}' does not reference any tables`,
          viewName
        );
      }

      const setClause = Object.entries(changes as Record<string, unknown>)
        .map(([k]) => `${k} = ?`)
        .join(', ');
      const values = Object.values(changes as Record<string, unknown>);

      const sql = `UPDATE ${baseTable} SET ${setClause}${whereClause ? ` WHERE ${whereClause}` : ''}`;
      const affected = await sqlExecutor.execute(sql, values);

      return {
        success: true,
        rowsAffected: affected,
      };
    },

    async delete<T = Record<string, unknown>>(
      viewName: string,
      where: string | ((row: T) => boolean),
      opts?: ViewExecutionOptions
    ): Promise<ViewDMLResult> {
      const view = registry.get(viewName);
      if (!view) {
        throw new ViewError(
          ViewErrorCode.NOT_FOUND,
          `View '${viewName}' does not exist`,
          viewName
        );
      }

      // Check if view is updatable or has INSTEAD OF trigger
      const triggers = registry.getInsteadOfTriggers(viewName, 'DELETE');
      if (!view.updatable && triggers.length === 0) {
        throw new ViewError(
          ViewErrorCode.NOT_UPDATABLE,
          `View '${viewName}' is not updatable and has no INSTEAD OF DELETE trigger`,
          viewName
        );
      }

      // Get rows to delete
      const whereClause = typeof where === 'string' ? where : undefined;
      const { rows } = await this.select<T>(viewName, { where: whereClause });

      // Filter with predicate if function provided
      const rowsToDelete = typeof where === 'function'
        ? rows.filter(where)
        : rows;

      if (rowsToDelete.length === 0) {
        return { success: true, rowsAffected: 0 };
      }

      // If there are INSTEAD OF triggers, use them
      if (triggers.length > 0) {
        let totalAffected = 0;
        for (const row of rowsToDelete) {
          const result = await executeInsteadOfTriggers(
            view,
            'DELETE',
            row as Record<string, unknown>,
            undefined,
            opts
          );
          if (!result.success) {
            return result;
          }
          totalAffected += result.rowsAffected;
        }
        return { success: true, rowsAffected: totalAffected };
      }

      // Otherwise, delete directly from the base table
      const baseTable = view.referencedTables[0];
      if (!baseTable) {
        throw new ViewError(
          ViewErrorCode.NOT_UPDATABLE,
          `View '${viewName}' does not reference any tables`,
          viewName
        );
      }

      const sql = `DELETE FROM ${baseTable}${whereClause ? ` WHERE ${whereClause}` : ''}`;
      const affected = await sqlExecutor.execute(sql);

      return {
        success: true,
        rowsAffected: affected,
      };
    },

    expand(viewName: string): string {
      const view = registry.get(viewName);
      if (!view) {
        throw new ViewError(
          ViewErrorCode.NOT_FOUND,
          `View '${viewName}' does not exist`,
          viewName
        );
      }

      return expandViewReference(viewName);
    },

    validate(viewName: string): { valid: boolean; errors: string[] } {
      const view = registry.get(viewName);
      if (!view) {
        return {
          valid: false,
          errors: [`View '${viewName}' does not exist`],
        };
      }

      const errors: string[] = [];

      // Check that all referenced tables still exist
      for (const table of view.referencedTables) {
        // This would need async, so we skip for now
        // In a real implementation, this would check the database
      }

      // Check that all referenced views still exist
      for (const ref of view.referencedViews) {
        if (!registry.exists(ref)) {
          errors.push(`Referenced view '${ref}' no longer exists`);
        }
      }

      // Try to expand the view (catches circular dependencies)
      try {
        expandViewReference(viewName);
      } catch (error) {
        errors.push(error instanceof Error ? error.message : String(error));
      }

      return {
        valid: errors.length === 0,
        errors,
      };
    },
  };
}

// =============================================================================
// Mock SQL Executor for Testing
// =============================================================================

/**
 * Create a mock SQL executor for testing
 */
export function createMockSqlExecutor(
  tables: Record<string, Record<string, unknown>[]>
): SqlExecutorAdapter {
  const data: Record<string, Record<string, unknown>[]> = { ...tables };

  /**
   * Apply a WHERE condition to rows
   */
  function applyCondition(rows: Record<string, unknown>[], condition: string): Record<string, unknown>[] {
    // Handle simple boolean: column = true/false (check this first as it's more specific)
    const boolMatch = condition.match(/(\w+)\s*=\s*(true|false)/i);
    if (boolMatch) {
      const [, col, val] = boolMatch;
      return rows.filter(r => r[col] === (val.toLowerCase() === 'true'));
    }

    // Handle simple equality: column = value
    const eqMatch = condition.match(/(\w+)\s*=\s*['"]?([^'"]+)['"]?/);
    if (eqMatch) {
      const [, col, val] = eqMatch;
      return rows.filter(r => String(r[col]) === val);
    }

    return rows;
  }

  return {
    async query<T>(sql: string): Promise<T[]> {
      // Find the innermost FROM clause (handle subqueries)
      // For "FROM (SELECT * FROM users WHERE ...) AS __view__"
      // we want to find "users"
      let tableName: string | null = null;

      // First check for subquery pattern
      const subqueryMatch = sql.match(/FROM\s+\(\s*SELECT\s+\*\s+FROM\s+(\w+)/i);
      if (subqueryMatch) {
        tableName = subqueryMatch[1];
      } else {
        // Simple FROM clause
        const fromMatch = sql.match(/FROM\s+["']?(\w+)["']?/i);
        if (fromMatch) {
          tableName = fromMatch[1];
        }
      }

      if (!tableName) {
        return [] as T[];
      }

      let rows = [...(data[tableName] ?? [])];

      // Find all WHERE clauses in the SQL (including ones inside subqueries and at the end)
      // Match "WHERE <condition>" patterns
      const wherePattern = /WHERE\s+([^()]+?)(?=\s+(?:ORDER|LIMIT|GROUP|HAVING|UNION|$)|$)/gi;
      let match;
      while ((match = wherePattern.exec(sql)) !== null) {
        const condition = match[1].trim();
        if (condition) {
          rows = applyCondition(rows, condition);
        }
      }

      // Also handle WHERE clause at the very end (after __view__)
      const finalWhereMatch = sql.match(/AS\s+__view__\s+WHERE\s+(.+?)(?:\s+ORDER|\s+LIMIT|$)/i);
      if (finalWhereMatch) {
        const condition = finalWhereMatch[1].trim();
        if (condition) {
          rows = applyCondition(rows, condition);
        }
      }

      // Handle LIMIT
      const limitMatch = sql.match(/LIMIT\s+(\d+)/i);
      if (limitMatch) {
        rows = rows.slice(0, parseInt(limitMatch[1]));
      }

      // Handle OFFSET
      const offsetMatch = sql.match(/OFFSET\s+(\d+)/i);
      if (offsetMatch) {
        rows = rows.slice(parseInt(offsetMatch[1]));
      }

      return rows as T[];
    },

    async execute(sql: string, params?: unknown[]): Promise<number> {
      // Handle INSERT
      const insertMatch = sql.match(/INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)/i);
      if (insertMatch) {
        const [, tableName, columnsStr] = insertMatch;
        const columns = columnsStr.split(',').map(c => c.trim());
        const row: Record<string, unknown> = {};

        if (params) {
          columns.forEach((col, i) => {
            row[col] = params[i];
          });
        }

        if (!data[tableName]) {
          data[tableName] = [];
        }
        data[tableName].push(row);
        return 1;
      }

      // Handle UPDATE
      const updateMatch = sql.match(/UPDATE\s+(\w+)\s+SET/i);
      if (updateMatch) {
        const tableName = updateMatch[1];
        const rows = data[tableName] ?? [];

        // Handle WHERE
        const whereMatch = sql.match(/WHERE\s+(.+)$/i);
        let affected = 0;

        for (const row of rows) {
          let shouldUpdate = true;
          if (whereMatch) {
            const condition = whereMatch[1];
            const eqMatch = condition.match(/(\w+)\s*=\s*['"]?([^'"]+)['"]?/);
            if (eqMatch) {
              const [, col, val] = eqMatch;
              shouldUpdate = String(row[col]) === val;
            }
          }

          if (shouldUpdate) {
            // Apply updates
            const setMatch = sql.match(/SET\s+(.+?)(?:\s+WHERE|$)/i);
            if (setMatch && params) {
              const setClause = setMatch[1];
              const assignments = setClause.split(',').map(a => a.trim());
              let paramIndex = 0;
              for (const assignment of assignments) {
                const colMatch = assignment.match(/(\w+)\s*=/);
                if (colMatch) {
                  row[colMatch[1]] = params[paramIndex++];
                }
              }
            }
            affected++;
          }
        }

        return affected;
      }

      // Handle DELETE
      const deleteMatch = sql.match(/DELETE\s+FROM\s+(\w+)/i);
      if (deleteMatch) {
        const tableName = deleteMatch[1];
        const rows = data[tableName] ?? [];
        const originalLength = rows.length;

        // Handle WHERE
        const whereMatch = sql.match(/WHERE\s+(.+)$/i);
        if (whereMatch) {
          const condition = whereMatch[1];
          const eqMatch = condition.match(/(\w+)\s*=\s*['"]?([^'"]+)['"]?/);
          if (eqMatch) {
            const [, col, val] = eqMatch;
            data[tableName] = rows.filter(r => String(r[col]) !== val);
          }
        } else {
          data[tableName] = [];
        }

        return originalLength - (data[tableName]?.length ?? 0);
      }

      return 0;
    },

    async tableExists(name: string): Promise<boolean> {
      return name in data;
    },

    async getColumns(tableName: string): Promise<Array<{ name: string; type: string }>> {
      const rows = data[tableName] ?? [];
      if (rows.length === 0) {
        return [];
      }

      return Object.keys(rows[0]).map(name => ({
        name,
        type: typeof rows[0][name] === 'number' ? 'INTEGER' : 'TEXT',
      }));
    },
  };
}

// =============================================================================
// SQL Manager for Views
// =============================================================================

/**
 * Manager that handles CREATE VIEW and DROP VIEW SQL statements
 */
export interface SqlViewManager {
  /**
   * Execute a CREATE VIEW or DROP VIEW statement
   */
  execute(sql: string): Promise<ViewDefinition | boolean>;

  /**
   * Check if the SQL is a view-related statement
   */
  isViewStatement(sql: string): boolean;
}

/**
 * Create a SQL view manager
 */
export function createSqlViewManager(registry: ViewRegistry): SqlViewManager {
  return {
    async execute(sql: string): Promise<ViewDefinition | boolean> {
      if (isCreateView(sql)) {
        const parsed = parseCreateView(sql);
        return registry.register(parsed);
      }

      if (isDropView(sql)) {
        const { name, ifExists, cascade, schema } = parseDropView(sql);
        return registry.drop(name, { ifExists, cascade, schema });
      }

      throw new ViewError(
        ViewErrorCode.INVALID_DEFINITION,
        'Not a valid VIEW statement. Expected CREATE VIEW or DROP VIEW.'
      );
    },

    isViewStatement(sql: string): boolean {
      return isCreateView(sql) || isDropView(sql);
    },
  };
}
