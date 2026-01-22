/**
 * Table Accessor Utilities
 *
 * Shared utilities for creating table accessors, particularly for testing.
 */

import type { TableAccessor, Predicate, QueryOptions } from '../proc/types.js';

// =============================================================================
// IN-MEMORY TABLE ACCESSOR
// =============================================================================

/**
 * Configuration for creating a simple table accessor from array data
 */
export interface SimpleTableAccessorConfig<T extends Record<string, unknown>> {
  /** The data array to operate on */
  data: T[];
  /** Primary key field name (default: 'id') */
  primaryKey?: string;
}

/**
 * Create a simple table accessor from an array for testing purposes.
 *
 * This creates a minimal TableAccessor implementation that operates directly
 * on an array in memory. Useful for testing and prototyping.
 *
 * @param config - Configuration with data array
 * @returns TableAccessor that operates on the array
 *
 * @example
 * ```typescript
 * const users: User[] = [{ id: 1, name: 'Alice' }];
 * const accessor = createSimpleTableAccessor({ data: users });
 * const user = await accessor.get(1);
 * ```
 */
export function createSimpleTableAccessor<T extends Record<string, unknown>>(
  config: SimpleTableAccessorConfig<T>
): TableAccessor<T> {
  const { data, primaryKey = 'id' } = config;

  function matchesFilter(record: T, filter: Partial<T>): boolean {
    for (const [key, value] of Object.entries(filter)) {
      if (record[key] !== value) {
        return false;
      }
    }
    return true;
  }

  function applyQueryOptions(results: T[], options?: QueryOptions): T[] {
    if (!options) return results;

    let filtered = [...results];

    if (options.orderBy) {
      const dir = options.orderDirection === 'desc' ? -1 : 1;
      filtered.sort((a, b) => {
        const aVal = a[options.orderBy!];
        const bVal = b[options.orderBy!];
        if (aVal < bVal) return -1 * dir;
        if (aVal > bVal) return 1 * dir;
        return 0;
      });
    }

    if (options.offset !== undefined) {
      filtered = filtered.slice(options.offset);
    }

    if (options.limit !== undefined) {
      filtered = filtered.slice(0, options.limit);
    }

    return filtered;
  }

  return {
    async get(key: string | number): Promise<T | undefined> {
      return data.find((r) => r[primaryKey] === key);
    },

    async where(
      predicateOrFilter: Predicate<T> | Partial<T>,
      options?: QueryOptions
    ): Promise<T[]> {
      let results: T[];
      if (typeof predicateOrFilter === 'function') {
        results = data.filter(predicateOrFilter);
      } else {
        results = data.filter((r) => matchesFilter(r, predicateOrFilter));
      }
      return applyQueryOptions(results, options);
    },

    async all(options?: QueryOptions): Promise<T[]> {
      return applyQueryOptions([...data], options);
    },

    async count(predicateOrFilter?: Predicate<T> | Partial<T>): Promise<number> {
      if (!predicateOrFilter) {
        return data.length;
      }
      if (typeof predicateOrFilter === 'function') {
        return data.filter(predicateOrFilter).length;
      }
      return data.filter((r) => matchesFilter(r, predicateOrFilter)).length;
    },

    async insert(record: Omit<T, 'id'>): Promise<T> {
      const id = data.length > 0
        ? Math.max(...data.map((r) => typeof r[primaryKey] === 'number' ? r[primaryKey] as number : 0)) + 1
        : 1;
      const newRecord = { ...record, [primaryKey]: id } as unknown as T;
      data.push(newRecord);
      return newRecord;
    },

    async update(
      predicateOrFilter: Predicate<T> | Partial<T>,
      changes: Partial<T>
    ): Promise<number> {
      let count = 0;
      for (const record of data) {
        const matches = typeof predicateOrFilter === 'function'
          ? predicateOrFilter(record)
          : matchesFilter(record, predicateOrFilter);
        if (matches) {
          Object.assign(record, changes);
          count++;
        }
      }
      return count;
    },

    async delete(predicateOrFilter: Predicate<T> | Partial<T>): Promise<number> {
      let count = 0;
      for (let i = data.length - 1; i >= 0; i--) {
        const record = data[i];
        const matches = typeof predicateOrFilter === 'function'
          ? predicateOrFilter(record)
          : matchesFilter(record, predicateOrFilter);
        if (matches) {
          data.splice(i, 1);
          count++;
        }
      }
      return count;
    },
  };
}

/**
 * Create table accessors from a record of table names to data arrays.
 *
 * @param tables - Record mapping table names to data arrays
 * @returns Record mapping table names to TableAccessors
 *
 * @example
 * ```typescript
 * const accessors = createSimpleTableAccessors({
 *   users: [{ id: 1, name: 'Alice' }],
 *   posts: [{ id: 1, title: 'Hello', userId: 1 }]
 * });
 * const user = await accessors.users.get(1);
 * ```
 */
export function createSimpleTableAccessors<
  T extends Record<string, Array<Record<string, unknown>>>
>(tables: T): { [K in keyof T]: TableAccessor<T[K][number]> } {
  const result: Record<string, TableAccessor<Record<string, unknown>>> = {};

  for (const [name, data] of Object.entries(tables)) {
    result[name] = createSimpleTableAccessor({ data });
  }

  return result as { [K in keyof T]: TableAccessor<T[K][number]> };
}
