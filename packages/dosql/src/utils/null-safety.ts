/**
 * Null Safety Utilities
 *
 * Provides helpers for strict null checks with optional chaining edge cases:
 * - Nested optional chaining
 * - Optional chaining with nullish coalescing
 * - Optional chaining in async contexts
 * - Optional method calls
 */

import { isObject, isNullish } from './type-guards.js';

// =============================================================================
// NESTED OPTIONAL CHAINING
// =============================================================================

/**
 * Safely chain property access on a nullable value
 *
 * @example
 * ```typescript
 * const name = safeChain(user, u => u?.profile?.name);
 * ```
 */
export function safeChain<T, R>(
  value: T | null | undefined,
  accessor: (value: T) => R
): R | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }
  try {
    return accessor(value);
  } catch {
    return undefined;
  }
}

/**
 * Safely get a deeply nested property using a path array
 *
 * @example
 * ```typescript
 * const city = safeDeepGet(user, ['address', 'city']);
 * ```
 */
export function safeDeepGet(
  obj: unknown,
  path: (string | number)[]
): unknown {
  let current: unknown = obj;

  for (const key of path) {
    if (current === null || current === undefined) {
      return undefined;
    }

    if (typeof key === 'number' && Array.isArray(current)) {
      current = current[key];
    } else if (isObject(current) && typeof key === 'string') {
      current = current[key];
    } else if (Array.isArray(current) && typeof key === 'string' && /^\d+$/.test(key)) {
      current = current[parseInt(key, 10)];
    } else {
      return undefined;
    }
  }

  return current;
}

/**
 * Safely set a deeply nested property, creating intermediate objects as needed
 * Returns a new object with the property set (immutable)
 *
 * @example
 * ```typescript
 * const updated = safeDeepSet(config, ['database', 'host'], 'localhost');
 * ```
 */
export function safeDeepSet<T extends Record<string, unknown>>(
  obj: T,
  path: (string | number)[],
  value: unknown
): T {
  if (path.length === 0) {
    return value as T;
  }

  const result = Array.isArray(obj) ? [...obj] : { ...obj };
  const [head, ...tail] = path;

  if (tail.length === 0) {
    (result as Record<string | number, unknown>)[head] = value;
  } else {
    const current = (result as Record<string | number, unknown>)[head];
    const nextIsArray = typeof tail[0] === 'number';
    const intermediate = isObject(current)
      ? current
      : Array.isArray(current)
        ? current
        : nextIsArray
          ? []
          : {};
    (result as Record<string | number, unknown>)[head] = safeDeepSet(
      intermediate as Record<string, unknown>,
      tail,
      value
    );
  }

  return result as T;
}

// =============================================================================
// OPTIONAL CHAINING WITH NULLISH COALESCING
// =============================================================================

/**
 * Chain property access with a default value
 * Uses nullish coalescing (returns default only for null/undefined, not for falsy values)
 *
 * @example
 * ```typescript
 * const count = coalesceChain(data, d => d?.items?.length, 0);
 * ```
 */
export function coalesceChain<T, R>(
  value: T | null | undefined,
  accessor: (value: T) => R | undefined | null,
  defaultValue: R
): R {
  if (value === null || value === undefined) {
    return defaultValue;
  }
  const result = accessor(value);
  return result ?? defaultValue;
}

// =============================================================================
// OPTIONAL METHOD CALLS
// =============================================================================

/**
 * Safely call a method on a nullable object
 *
 * @example
 * ```typescript
 * const result = safeCall(obj, 'process', data);
 * ```
 */
export function safeCall<
  T extends Record<string, unknown>,
  K extends keyof T,
  Args extends unknown[]
>(
  obj: T | null | undefined,
  method: K,
  ...args: Args
): T[K] extends (...args: Args) => infer R ? R | undefined : undefined {
  if (obj === null || obj === undefined) {
    return undefined as never;
  }

  const fn = obj[method];
  if (typeof fn !== 'function') {
    return undefined as never;
  }

  return fn.apply(obj, args);
}

/**
 * Safely call an async method on a nullable object
 *
 * @example
 * ```typescript
 * const data = await safeCallAsync(service, 'fetchData', id);
 * ```
 */
export async function safeCallAsync<
  T extends Record<string, unknown>,
  K extends keyof T,
  Args extends unknown[]
>(
  obj: T | null | undefined,
  method: K,
  ...args: Args
): Promise<T[K] extends (...args: Args) => Promise<infer R> ? R | undefined : undefined> {
  if (obj === null || obj === undefined) {
    return undefined as never;
  }

  const fn = obj[method];
  if (typeof fn !== 'function') {
    return undefined as never;
  }

  return fn.apply(obj, args);
}

// =============================================================================
// ASYNC CONTEXTS
// =============================================================================

type PipelineFunction<I, O> = (input: I) => O | Promise<O>;

/**
 * Execute a pipeline of async functions, short-circuiting on null/undefined
 *
 * @example
 * ```typescript
 * const result = await asyncGuardedPipeline(
 *   userId,
 *   fetchUser,
 *   user => user?.profile,
 *   profile => profile?.email
 * );
 * ```
 */
export async function asyncGuardedPipeline<T>(
  initial: T | null | undefined,
  ...fns: PipelineFunction<unknown, unknown>[]
): Promise<unknown> {
  let current: unknown = initial;

  for (const fn of fns) {
    if (current === null || current === undefined) {
      return undefined;
    }
    current = await fn(current);
  }

  return current;
}

/**
 * Execute a pipeline of sync functions, short-circuiting on null/undefined
 *
 * @example
 * ```typescript
 * const result = guardedPipeline(
 *   data,
 *   d => d.items,
 *   items => items[0],
 *   item => item.name
 * );
 * ```
 */
export function guardedPipeline<T>(
  initial: T | null | undefined,
  ...fns: ((input: unknown) => unknown)[]
): unknown {
  let current: unknown = initial;

  for (const fn of fns) {
    if (current === null || current === undefined) {
      return undefined;
    }
    current = fn(current);
  }

  return current;
}

// =============================================================================
// NULL-SAFE COLLECTION OPERATIONS
// =============================================================================

/**
 * Safely spread a nullable array (returns empty array for null/undefined)
 *
 * @example
 * ```typescript
 * const combined = [...nullSafeSpread(arr1), ...nullSafeSpread(arr2)];
 * ```
 */
export function nullSafeSpread<T>(arr: T[] | null | undefined): T[] {
  return arr ?? [];
}

/**
 * Concatenate multiple nullable arrays
 *
 * @example
 * ```typescript
 * const all = nullSafeConcat(arr1, arr2, arr3);
 * ```
 */
export function nullSafeConcat<T>(
  ...arrays: (T[] | null | undefined)[]
): T[] {
  return arrays.reduce<T[]>((acc, arr) => {
    if (arr) {
      acc.push(...arr);
    }
    return acc;
  }, []);
}

/**
 * Filter null and undefined values from an array
 *
 * @example
 * ```typescript
 * const nonNull = filterNullable([1, null, 2, undefined, 3]);
 * // [1, 2, 3]
 * ```
 */
export function filterNullable<T>(arr: (T | null | undefined)[]): T[] {
  return arr.filter((item): item is T => item !== null && item !== undefined);
}

// =============================================================================
// TYPE ASSERTIONS
// =============================================================================

/**
 * Assert that a value is defined (not null or undefined)
 * Throws if the value is null or undefined
 *
 * @example
 * ```typescript
 * const user = assertDefined(maybeUser, 'User is required');
 * ```
 */
export function assertDefined<T>(
  value: T | null | undefined,
  message = 'Value is null or undefined'
): T {
  if (value === null || value === undefined) {
    throw new Error(message);
  }
  return value;
}

/**
 * Assert that a value is not null (undefined is allowed)
 * Throws if the value is null
 *
 * @example
 * ```typescript
 * const result = assertNotNull(maybeNull, 'Result cannot be null');
 * ```
 */
export function assertNotNull<T>(
  value: T | null,
  message = 'Value is null'
): T {
  if (value === null) {
    throw new Error(message);
  }
  return value;
}

/**
 * Unwrap a nullable value with a default
 *
 * @example
 * ```typescript
 * const count = unwrapOr(maybeCount, 0);
 * ```
 */
export function unwrapOr<T>(value: T | null | undefined, defaultValue: T): T {
  return value ?? defaultValue;
}

/**
 * Unwrap a nullable value or throw
 *
 * @example
 * ```typescript
 * const id = unwrapOrThrow(maybeId, 'ID is required');
 * ```
 */
export function unwrapOrThrow<T>(
  value: T | null | undefined,
  message = 'Value is null or undefined'
): T {
  return assertDefined(value, message);
}

// =============================================================================
// NULLABLE MAPPING
// =============================================================================

/**
 * Map a function over a nullable value
 * Returns undefined if the value is null/undefined
 *
 * @example
 * ```typescript
 * const uppercased = mapNullable(maybeName, name => name.toUpperCase());
 * ```
 */
export function mapNullable<T, R>(
  value: T | null | undefined,
  fn: (value: T) => R
): R | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }
  return fn(value);
}

/**
 * FlatMap a function over a nullable value
 * Returns undefined if the value is null/undefined or if the function returns null/undefined
 *
 * @example
 * ```typescript
 * const user = flatMapNullable(userId, id => users.get(id));
 * ```
 */
export function flatMapNullable<T, R>(
  value: T | null | undefined,
  fn: (value: T) => R | null | undefined
): R | undefined {
  if (value === null || value === undefined) {
    return undefined;
  }
  const result = fn(value);
  return result ?? undefined;
}

// =============================================================================
// STRICT TYPE NARROWING
// =============================================================================

/**
 * Type narrowing helper that returns true only if value is neither null nor undefined
 * More explicit than isDefined for use in filter callbacks
 */
export function isNotNullish<T>(value: T | null | undefined): value is T {
  return value !== null && value !== undefined;
}

/**
 * Type narrowing helper that returns true only if value is null
 */
export function isStrictlyNull(value: unknown): value is null {
  return value === null;
}

/**
 * Type narrowing helper that returns true only if value is undefined
 */
export function isStrictlyUndefined(value: unknown): value is undefined {
  return value === undefined;
}
