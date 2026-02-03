/**
 * Type Guard Utilities for Record<string, unknown>
 *
 * Provides safe type narrowing functions for working with unknown objects
 * and Record<string, unknown> types throughout the codebase.
 */

// =============================================================================
// BASIC TYPE GUARDS
// =============================================================================

/**
 * Check if a value is a non-null object (not an array)
 */
export function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Alias for isObject - checks if a value is a Record<string, unknown>
 * This is the same as isObject but with a more explicit name for record patterns
 */
export const isRecord = isObject;

/**
 * Check if a value is a non-null object or array
 */
export function isObjectOrArray(value: unknown): value is Record<string, unknown> | unknown[] {
  return typeof value === 'object' && value !== null;
}

/**
 * Check if a value is an array
 */
export function isArray(value: unknown): value is unknown[] {
  return Array.isArray(value);
}

/**
 * Check if a value is a typed array
 */
export function isTypedArray<T>(value: unknown, itemGuard: (item: unknown) => item is T): value is T[] {
  return Array.isArray(value) && value.every(itemGuard);
}

// =============================================================================
// PROPERTY TYPE GUARDS
// =============================================================================

/**
 * Check if an object has a property (property exists and is not undefined)
 */
export function hasProperty<K extends string>(
  obj: unknown,
  key: K
): obj is Record<K, unknown> {
  return isObject(obj) && key in obj;
}

/**
 * Check if an object has a property of a specific type
 */
export function hasPropertyOfType<K extends string, T>(
  obj: unknown,
  key: K,
  typeGuard: (value: unknown) => value is T
): obj is Record<K, T> {
  return hasProperty(obj, key) && typeGuard(obj[key]);
}

/**
 * Check if an object has a string property
 */
export function hasStringProperty<K extends string>(
  obj: unknown,
  key: K
): obj is Record<K, string> {
  return hasProperty(obj, key) && typeof obj[key] === 'string';
}

/**
 * Check if an object has a number property
 */
export function hasNumberProperty<K extends string>(
  obj: unknown,
  key: K
): obj is Record<K, number> {
  return hasProperty(obj, key) && typeof obj[key] === 'number';
}

/**
 * Check if an object has a boolean property
 */
export function hasBooleanProperty<K extends string>(
  obj: unknown,
  key: K
): obj is Record<K, boolean> {
  return hasProperty(obj, key) && typeof obj[key] === 'boolean';
}

/**
 * Check if an object has an array property
 */
export function hasArrayProperty<K extends string>(
  obj: unknown,
  key: K
): obj is Record<K, unknown[]> {
  return hasProperty(obj, key) && Array.isArray(obj[key]);
}

/**
 * Check if an object has a typed array property
 */
export function hasTypedArrayProperty<K extends string, T>(
  obj: unknown,
  key: K,
  itemGuard: (item: unknown) => item is T
): obj is Record<K, T[]> {
  return hasArrayProperty(obj, key) && (obj[key] as unknown[]).every(itemGuard);
}

/**
 * Check if an object has an object property (non-null, non-array)
 */
export function hasObjectProperty<K extends string>(
  obj: unknown,
  key: K
): obj is Record<K, Record<string, unknown>> {
  return hasProperty(obj, key) && isObject(obj[key]);
}

/**
 * Check if an object has a function property
 */
export function hasFunctionProperty<K extends string>(
  obj: unknown,
  key: K
): obj is Record<K, (...args: unknown[]) => unknown> {
  return hasProperty(obj, key) && typeof obj[key] === 'function';
}

// =============================================================================
// MULTI-PROPERTY TYPE GUARDS
// =============================================================================

/**
 * Check if an object has all specified string properties
 */
export function hasStringProperties<K extends string>(
  obj: unknown,
  keys: K[]
): obj is Record<K, string> {
  return keys.every((key) => hasStringProperty(obj, key));
}

/**
 * Check if an object has all specified properties
 */
export function hasProperties<K extends string>(
  obj: unknown,
  keys: K[]
): obj is Record<K, unknown> {
  return keys.every((key) => hasProperty(obj, key));
}

// =============================================================================
// OPTIONAL PROPERTY ACCESSORS
// =============================================================================

/**
 * Safely get a string property from an object, returning undefined if not present or not a string
 */
export function getStringProperty<K extends string>(
  obj: unknown,
  key: K
): string | undefined {
  if (hasStringProperty(obj, key)) {
    return obj[key];
  }
  return undefined;
}

/**
 * Safely get a number property from an object, returning undefined if not present or not a number
 */
export function getNumberProperty<K extends string>(
  obj: unknown,
  key: K
): number | undefined {
  if (hasNumberProperty(obj, key)) {
    return obj[key];
  }
  return undefined;
}

/**
 * Safely get a boolean property from an object, returning undefined if not present or not a boolean
 */
export function getBooleanProperty<K extends string>(
  obj: unknown,
  key: K
): boolean | undefined {
  if (hasBooleanProperty(obj, key)) {
    return obj[key];
  }
  return undefined;
}

/**
 * Safely get an array property from an object, returning undefined if not present or not an array
 */
export function getArrayProperty<K extends string>(
  obj: unknown,
  key: K
): unknown[] | undefined {
  if (hasArrayProperty(obj, key)) {
    return obj[key];
  }
  return undefined;
}

/**
 * Safely get an object property from an object, returning undefined if not present or not an object
 */
export function getObjectProperty<K extends string>(
  obj: unknown,
  key: K
): Record<string, unknown> | undefined {
  if (hasObjectProperty(obj, key)) {
    return obj[key];
  }
  return undefined;
}

/**
 * Safely get a property with a type guard, returning undefined if not valid
 */
export function getTypedProperty<K extends string, T>(
  obj: unknown,
  key: K,
  guard: (value: unknown) => value is T
): T | undefined {
  if (hasPropertyOfType(obj, key, guard)) {
    return obj[key];
  }
  return undefined;
}

// =============================================================================
// COMMON OBJECT SHAPE GUARDS
// =============================================================================

/**
 * Type guard for objects with a 'code' string property (common for errors)
 */
export function hasErrorCode(obj: unknown): obj is { code: string } {
  return hasStringProperty(obj, 'code');
}

/**
 * Type guard for objects with a 'type' string property (common for discriminated unions)
 */
export function hasTypeProperty(obj: unknown): obj is { type: string } {
  return hasStringProperty(obj, 'type');
}

/**
 * Type guard for objects with both 'name' and 'message' properties (error-like)
 */
export function isErrorLike(obj: unknown): obj is { name: string; message: string } {
  return hasStringProperty(obj, 'name') && hasStringProperty(obj, 'message');
}

/**
 * Type guard for objects with a specific type discriminator value
 */
export function hasTypeDiscriminator<T extends string>(
  obj: unknown,
  typeValue: T
): obj is { type: T } {
  return hasStringProperty(obj, 'type') && obj.type === typeValue;
}

// =============================================================================
// RECORD ITERATION HELPERS
// =============================================================================

/**
 * Safely iterate over an object's entries with type narrowing
 */
export function* iterateRecord(
  obj: unknown
): Generator<[string, unknown], void, undefined> {
  if (isObject(obj)) {
    for (const [key, value] of Object.entries(obj)) {
      yield [key, value];
    }
  }
}

/**
 * Safely get keys from an object
 */
export function getKeys(obj: unknown): string[] {
  if (isObject(obj)) {
    return Object.keys(obj);
  }
  return [];
}

/**
 * Safely get values from an object
 */
export function getValues(obj: unknown): unknown[] {
  if (isObject(obj)) {
    return Object.values(obj);
  }
  return [];
}

/**
 * Safely get entries from an object
 */
export function getEntries(obj: unknown): [string, unknown][] {
  if (isObject(obj)) {
    return Object.entries(obj);
  }
  return [];
}

// =============================================================================
// PRIMITIVE TYPE GUARDS
// =============================================================================

/**
 * Type guard for string values
 */
export function isString(value: unknown): value is string {
  return typeof value === 'string';
}

/**
 * Type guard for number values
 */
export function isNumber(value: unknown): value is number {
  return typeof value === 'number';
}

/**
 * Type guard for boolean values
 */
export function isBoolean(value: unknown): value is boolean {
  return typeof value === 'boolean';
}

/**
 * Type guard for bigint values
 */
export function isBigInt(value: unknown): value is bigint {
  return typeof value === 'bigint';
}

/**
 * Type guard for function values
 */
export function isFunction(value: unknown): value is (...args: unknown[]) => unknown {
  return typeof value === 'function';
}

/**
 * Type guard for null or undefined
 */
export function isNullish(value: unknown): value is null | undefined {
  return value === null || value === undefined;
}

/**
 * Type guard for non-null, non-undefined values
 */
export function isDefined<T>(value: T | null | undefined): value is T {
  return value !== null && value !== undefined;
}

/**
 * Type guard for null values
 */
export function isNull(value: unknown): value is null {
  return value === null;
}

/**
 * Type guard for undefined values
 */
export function isUndefined(value: unknown): value is undefined {
  return value === undefined;
}

// =============================================================================
// CASTING HELPERS (with validation)
// =============================================================================

/**
 * Cast unknown to Record<string, unknown> with validation
 * Returns undefined if the value is not a valid object
 */
export function asRecord(value: unknown): Record<string, unknown> | undefined {
  return isObject(value) ? value : undefined;
}

/**
 * Cast unknown to Record<string, unknown> or throw
 */
export function asRecordOrThrow(value: unknown, message = 'Expected object'): Record<string, unknown> {
  if (!isObject(value)) {
    throw new TypeError(message);
  }
  return value;
}

/**
 * Cast unknown to array with validation
 * Returns undefined if the value is not an array
 */
export function asArray(value: unknown): unknown[] | undefined {
  return Array.isArray(value) ? value : undefined;
}

/**
 * Cast unknown to typed array with validation
 * Returns undefined if the value is not a valid typed array
 */
export function asTypedArray<T>(
  value: unknown,
  itemGuard: (item: unknown) => item is T
): T[] | undefined {
  if (!Array.isArray(value)) return undefined;
  if (!value.every(itemGuard)) return undefined;
  return value;
}

// =============================================================================
// UTILITY TYPES
// =============================================================================

/**
 * Makes specific properties of a Record required
 */
export type RecordWithRequired<R extends Record<string, unknown>, K extends keyof R> =
  R & Required<Pick<R, K>>;

/**
 * A record with at least one required property
 */
export type RecordWith<K extends string, V> = Record<string, unknown> & Record<K, V>;

/**
 * A record with optional properties of specific types
 */
export type PartialRecord<K extends string, V> = Partial<Record<K, V>>;

/**
 * Union type representing common SQL values
 */
export type SqlValue = string | number | boolean | null | bigint | Uint8Array;

/**
 * Type guard for SQL-compatible values
 */
export function isSqlValue(value: unknown): value is SqlValue {
  if (value === null) return true;
  const type = typeof value;
  if (type === 'string' || type === 'number' || type === 'boolean' || type === 'bigint') {
    return true;
  }
  if (value instanceof Uint8Array) return true;
  return false;
}

/**
 * Row type commonly used in query results
 */
export type Row = Record<string, SqlValue>;

/**
 * Type guard for Row type
 */
export function isRow(value: unknown): value is Row {
  if (!isObject(value)) return false;
  for (const val of Object.values(value)) {
    if (!isSqlValue(val)) return false;
  }
  return true;
}

/**
 * Query result type
 */
export interface QueryResult<T = Row> {
  rows: T[];
  rowsAffected?: number | undefined;
  columns?: { name: string; type: string }[] | undefined;
}

/**
 * Type guard for QueryResult type
 */
export function isQueryResult<T = Row>(value: unknown): value is QueryResult<T> {
  if (!isObject(value)) return false;
  if (!hasArrayProperty(value, 'rows')) return false;
  // rowsAffected is optional, but if present must be a number
  if ('rowsAffected' in value && value.rowsAffected !== undefined) {
    if (typeof value.rowsAffected !== 'number') return false;
  }
  // columns is optional, but if present must be an array
  if ('columns' in value && value.columns !== undefined) {
    if (!isArray(value.columns)) return false;
    for (const col of value.columns) {
      if (!isObject(col)) return false;
      if (!hasStringProperty(col, 'name')) return false;
      if (!hasStringProperty(col, 'type')) return false;
    }
  }
  return true;
}

// =============================================================================
// SAFE PROPERTY ACCESS
// =============================================================================

/**
 * Safely get a property from an unknown value
 * Returns undefined if the value is not an object or doesn't have the property
 */
export function safeGet<K extends string>(
  obj: unknown,
  key: K
): unknown {
  if (isObject(obj) && key in obj) {
    return obj[key];
  }
  return undefined;
}

/**
 * Safely get a nested property from an unknown value using a path
 * Path can be a dot-separated string or an array of keys
 * Returns undefined if any part of the path doesn't exist
 */
export function safeGetPath(
  obj: unknown,
  path: string | (string | number)[]
): unknown {
  const segments = typeof path === 'string' ? path.split('.') : path;

  let current: unknown = obj;

  for (const segment of segments) {
    if (current === null || current === undefined) {
      return undefined;
    }

    if (typeof segment === 'number') {
      if (Array.isArray(current)) {
        current = current[segment];
      } else {
        return undefined;
      }
    } else if (isObject(current)) {
      current = current[segment];
    } else if (Array.isArray(current) && /^\d+$/.test(segment)) {
      current = current[parseInt(segment, 10)];
    } else {
      return undefined;
    }
  }

  return current;
}

/**
 * Narrowing helper that asserts a value is an object and returns it
 * Throws if the value is not an object
 */
export function narrowToRecord(value: unknown, errorMessage = 'Expected object'): Record<string, unknown> {
  if (!isObject(value)) {
    throw new TypeError(errorMessage);
  }
  return value;
}

/**
 * Narrowing helper that returns the value if it's an object, or a default
 */
export function narrowToRecordOr<T>(
  value: unknown,
  defaultValue: T
): Record<string, unknown> | T {
  return isObject(value) ? value : defaultValue;
}

/**
 * Type-safe property setter for Record<string, unknown>
 * Returns a new object with the property set
 */
export function setProperty<K extends string, V>(
  obj: Record<string, unknown>,
  key: K,
  value: V
): Record<string, unknown> & Record<K, V> {
  return { ...obj, [key]: value } as Record<string, unknown> & Record<K, V>;
}

/**
 * Check if value is a plain object created by Object constructor or object literal
 */
export function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (!isObject(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === null || proto === Object.prototype;
}
