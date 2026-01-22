/**
 * SQLite JSON Functions
 *
 * Implements SQLite-compatible JSON functions:
 * - json(x) - Validates and minifies JSON
 * - json_valid(x) - Returns 1 if valid JSON, 0 otherwise
 * - json_extract(x, path) - Extract value at JSON path
 * - json_type(x, path?) - Returns JSON type at path
 * - json_array(...) - Create JSON array from arguments
 * - json_object(...) - Create JSON object from key-value pairs
 * - json_array_length(x, path?) - Length of JSON array
 * - json_each(x, path?) - Table-valued function returning each element
 * - json_tree(x, path?) - Table-valued function returning tree
 * - json_insert(x, path, value, ...) - Insert values
 * - json_replace(x, path, value, ...) - Replace values
 * - json_set(x, path, value, ...) - Set values (insert or replace)
 * - json_remove(x, path, ...) - Remove values at paths
 * - json_patch(x, y) - Apply JSON merge patch
 * - json_quote(x) - Convert SQL value to JSON value
 * - json_group_array(x) - Aggregate: collect values into JSON array
 * - json_group_object(name, value) - Aggregate: collect into JSON object
 *
 * JSON Path syntax:
 * - $ - root element
 * - $.key - object member
 * - $[n] - array element (0-indexed)
 * - $.key.subkey - nested access
 * - $[0].key - mixed access
 */

import type { SqlValue } from '../engine/types.js';
import type { SqlFunction, FunctionSignature } from './registry.js';

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Parse a JSON string, returning null if invalid
 */
function parseJson(x: SqlValue): unknown {
  if (x === null) return null;

  if (typeof x === 'string') {
    try {
      return JSON.parse(x);
    } catch {
      return undefined; // Invalid JSON
    }
  }

  // Already a JS value - return as-is
  if (typeof x === 'object') {
    return x;
  }

  // Primitive values
  return x;
}

/**
 * Parse a JSON path and return path segments
 * Path syntax: $ (root), $.key, $[n], $.key.subkey, $[0].key
 */
function parsePath(path: string): (string | number)[] {
  const segments: (string | number)[] = [];

  if (!path.startsWith('$')) {
    throw new Error(`Invalid JSON path: ${path}`);
  }

  let i = 1; // Skip the '$'

  while (i < path.length) {
    if (path[i] === '.') {
      // Object key
      i++;
      const start = i;
      while (i < path.length && path[i] !== '.' && path[i] !== '[') {
        i++;
      }
      if (i === start) {
        throw new Error(`Invalid JSON path: empty key at position ${start}`);
      }
      segments.push(path.slice(start, i));
    } else if (path[i] === '[') {
      // Array index or quoted key
      i++;
      if (path[i] === '"' || path[i] === "'") {
        // Quoted key
        const quote = path[i];
        i++;
        const start = i;
        while (i < path.length && path[i] !== quote) {
          if (path[i] === '\\') i++; // Skip escaped char
          i++;
        }
        segments.push(path.slice(start, i));
        i++; // Skip closing quote
        if (path[i] !== ']') {
          throw new Error(`Invalid JSON path: expected ] at position ${i}`);
        }
        i++;
      } else if (path[i] === '#') {
        // Special: array length reference
        i++;
        if (path[i] !== ']') {
          throw new Error(`Invalid JSON path: expected ] after # at position ${i}`);
        }
        i++;
        segments.push('#');
      } else {
        // Numeric index
        const start = i;
        while (i < path.length && path[i] !== ']') {
          i++;
        }
        const indexStr = path.slice(start, i);
        const index = parseInt(indexStr, 10);
        if (isNaN(index)) {
          throw new Error(`Invalid JSON path: invalid index "${indexStr}"`);
        }
        segments.push(index);
        i++; // Skip ']'
      }
    } else {
      throw new Error(`Invalid JSON path: unexpected character at position ${i}`);
    }
  }

  return segments;
}

/**
 * Get value at JSON path
 */
function getAtPath(obj: unknown, pathSegments: (string | number)[]): unknown {
  let current = obj;

  for (const segment of pathSegments) {
    if (current === null || current === undefined) {
      return undefined;
    }

    if (segment === '#') {
      // Array length
      if (Array.isArray(current)) {
        return current.length;
      }
      return undefined;
    }

    if (typeof segment === 'number') {
      if (Array.isArray(current)) {
        current = current[segment];
      } else {
        return undefined;
      }
    } else {
      if (typeof current === 'object' && current !== null && !Array.isArray(current)) {
        current = (current as Record<string, unknown>)[segment];
      } else {
        return undefined;
      }
    }
  }

  return current;
}

/**
 * Set value at JSON path (mutates the object)
 */
function setAtPath(
  obj: unknown,
  pathSegments: (string | number)[],
  value: unknown
): unknown {
  if (pathSegments.length === 0) {
    return value;
  }

  // Ensure we have a mutable object
  let root: unknown;
  if (obj === null || obj === undefined) {
    // Create structure based on first segment
    root = typeof pathSegments[0] === 'number' ? [] : {};
  } else if (typeof obj === 'object') {
    root = Array.isArray(obj) ? [...obj] : { ...obj };
  } else {
    return obj; // Can't set path on primitive
  }

  let current = root;

  for (let i = 0; i < pathSegments.length - 1; i++) {
    const segment = pathSegments[i];
    const nextSegment = pathSegments[i + 1];

    if (typeof segment === 'number') {
      if (!Array.isArray(current)) return obj;
      if (current[segment] === undefined || current[segment] === null) {
        current[segment] = typeof nextSegment === 'number' ? [] : {};
      } else if (typeof current[segment] === 'object') {
        current[segment] = Array.isArray(current[segment])
          ? [...current[segment]]
          : { ...current[segment] };
      }
      current = current[segment];
    } else {
      if (typeof current !== 'object' || current === null || Array.isArray(current)) {
        return obj;
      }
      const rec = current as Record<string, unknown>;
      if (rec[segment] === undefined || rec[segment] === null) {
        rec[segment] = typeof nextSegment === 'number' ? [] : {};
      } else if (typeof rec[segment] === 'object') {
        rec[segment] = Array.isArray(rec[segment])
          ? [...(rec[segment] as unknown[])]
          : { ...(rec[segment] as object) };
      }
      current = rec[segment];
    }
  }

  const lastSegment = pathSegments[pathSegments.length - 1];
  if (typeof lastSegment === 'number') {
    if (Array.isArray(current)) {
      current[lastSegment] = value;
    }
  } else {
    if (typeof current === 'object' && current !== null && !Array.isArray(current)) {
      (current as Record<string, unknown>)[lastSegment] = value;
    }
  }

  return root;
}

/**
 * Remove value at JSON path
 */
function removeAtPath(obj: unknown, pathSegments: (string | number)[]): unknown {
  if (pathSegments.length === 0 || obj === null || obj === undefined) {
    return obj;
  }

  // Deep clone
  let root: unknown;
  if (typeof obj === 'object') {
    root = Array.isArray(obj) ? [...obj] : { ...obj };
  } else {
    return obj;
  }

  if (pathSegments.length === 1) {
    const segment = pathSegments[0];
    if (typeof segment === 'number' && Array.isArray(root)) {
      root.splice(segment, 1);
    } else if (typeof segment === 'string' && typeof root === 'object' && !Array.isArray(root)) {
      delete (root as Record<string, unknown>)[segment];
    }
    return root;
  }

  // Navigate to parent
  let current = root;
  for (let i = 0; i < pathSegments.length - 1; i++) {
    const segment = pathSegments[i];
    if (typeof segment === 'number') {
      if (!Array.isArray(current)) return obj;
      if (typeof current[segment] === 'object' && current[segment] !== null) {
        current[segment] = Array.isArray(current[segment])
          ? [...current[segment]]
          : { ...current[segment] };
      }
      current = current[segment];
    } else {
      if (typeof current !== 'object' || current === null || Array.isArray(current)) {
        return obj;
      }
      const rec = current as Record<string, unknown>;
      if (typeof rec[segment] === 'object' && rec[segment] !== null) {
        rec[segment] = Array.isArray(rec[segment])
          ? [...(rec[segment] as unknown[])]
          : { ...(rec[segment] as object) };
      }
      current = rec[segment];
    }
  }

  // Remove at last segment
  const lastSegment = pathSegments[pathSegments.length - 1];
  if (typeof lastSegment === 'number' && Array.isArray(current)) {
    current.splice(lastSegment, 1);
  } else if (typeof lastSegment === 'string' && typeof current === 'object' && !Array.isArray(current)) {
    delete (current as Record<string, unknown>)[lastSegment];
  }

  return root;
}

/**
 * Get the JSON type name
 */
function getJsonType(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'array';
  if (typeof value === 'object') return 'object';
  if (typeof value === 'string') return 'text';
  if (typeof value === 'number') {
    return Number.isInteger(value) ? 'integer' : 'real';
  }
  if (typeof value === 'boolean') return value ? 'true' : 'false';
  return 'null';
}

// =============================================================================
// JSON FUNCTION IMPLEMENTATIONS
// =============================================================================

/**
 * json(x) - Validates and minifies JSON
 * Returns the minified JSON string if valid, or throws on invalid
 */
export function json(x: SqlValue): SqlValue {
  if (x === null) return null;

  const parsed = parseJson(x);
  if (parsed === undefined) {
    throw new Error('malformed JSON');
  }

  return JSON.stringify(parsed);
}

/**
 * json_valid(x) - Returns 1 if valid JSON, 0 otherwise
 */
export function json_valid(x: SqlValue): SqlValue {
  if (x === null) return 0;

  const parsed = parseJson(x);
  return parsed !== undefined ? 1 : 0;
}

/**
 * json_extract(x, path, ...) - Extract value(s) at JSON path(s)
 * With one path: returns the value
 * With multiple paths: returns JSON array of values
 */
export function json_extract(x: SqlValue, ...paths: SqlValue[]): SqlValue {
  if (x === null || paths.length === 0) return null;

  const parsed = parseJson(x);
  if (parsed === undefined) return null;

  if (paths.length === 1) {
    const path = paths[0];
    if (path === null) return null;

    try {
      const segments = parsePath(String(path));
      const value = getAtPath(parsed, segments);

      if (value === undefined) return null;

      // Return JSON string for objects/arrays, otherwise the value
      if (typeof value === 'object' && value !== null) {
        return JSON.stringify(value);
      }
      return value as SqlValue;
    } catch {
      return null;
    }
  }

  // Multiple paths: return JSON array
  const results: unknown[] = [];
  for (const path of paths) {
    if (path === null) {
      results.push(null);
      continue;
    }

    try {
      const segments = parsePath(String(path));
      const value = getAtPath(parsed, segments);
      results.push(value === undefined ? null : value);
    } catch {
      results.push(null);
    }
  }

  return JSON.stringify(results);
}

/**
 * json_type(x, path?) - Returns JSON type at path
 * Types: null, true, false, integer, real, text, array, object
 */
export function json_type(x: SqlValue, path?: SqlValue): SqlValue {
  if (x === null) return null;

  const parsed = parseJson(x);
  if (parsed === undefined) return null;

  let value = parsed;

  if (path !== undefined && path !== null) {
    try {
      const segments = parsePath(String(path));
      value = getAtPath(parsed, segments);
      if (value === undefined) return null;
    } catch {
      return null;
    }
  }

  return getJsonType(value);
}

/**
 * json_array(...) - Create JSON array from arguments
 */
export function json_array(...args: SqlValue[]): SqlValue {
  const arr = args.map(a => {
    if (a === null) return null;
    if (typeof a === 'string') {
      // Check if it's a JSON string
      const parsed = parseJson(a);
      if (parsed !== undefined && typeof parsed === 'object') {
        return parsed;
      }
    }
    return a;
  });
  return JSON.stringify(arr);
}

/**
 * json_object(...) - Create JSON object from key-value pairs
 * Arguments must be pairs: key1, value1, key2, value2, ...
 */
export function json_object(...args: SqlValue[]): SqlValue {
  if (args.length % 2 !== 0) {
    throw new Error('json_object requires an even number of arguments');
  }

  const obj: Record<string, unknown> = {};

  for (let i = 0; i < args.length; i += 2) {
    const key = args[i];
    const value = args[i + 1];

    if (key === null) {
      throw new Error('json_object key cannot be NULL');
    }

    const keyStr = String(key);

    if (value === null) {
      obj[keyStr] = null;
    } else if (typeof value === 'string') {
      // Check if it's a JSON string
      const parsed = parseJson(value);
      if (parsed !== undefined && typeof parsed === 'object') {
        obj[keyStr] = parsed;
      } else {
        obj[keyStr] = value;
      }
    } else {
      obj[keyStr] = value;
    }
  }

  return JSON.stringify(obj);
}

/**
 * json_array_length(x, path?) - Returns length of JSON array at path
 */
export function json_array_length(x: SqlValue, path?: SqlValue): SqlValue {
  if (x === null) return null;

  const parsed = parseJson(x);
  if (parsed === undefined) return null;

  let value = parsed;

  if (path !== undefined && path !== null) {
    try {
      const segments = parsePath(String(path));
      value = getAtPath(parsed, segments);
      if (value === undefined) return null;
    } catch {
      return null;
    }
  }

  if (!Array.isArray(value)) return null;
  return value.length;
}

/**
 * json_insert(x, path, value, ...) - Insert values only if path doesn't exist
 */
export function json_insert(x: SqlValue, ...pathValuePairs: SqlValue[]): SqlValue {
  if (x === null || pathValuePairs.length < 2) return null;
  if (pathValuePairs.length % 2 !== 0) return null;

  let parsed = parseJson(x);
  if (parsed === undefined) return null;

  for (let i = 0; i < pathValuePairs.length; i += 2) {
    const path = pathValuePairs[i];
    const value = pathValuePairs[i + 1];

    if (path === null) continue;

    try {
      const segments = parsePath(String(path));
      const existing = getAtPath(parsed, segments);

      // Only insert if doesn't exist
      if (existing === undefined) {
        const newValue = typeof value === 'string' ? parseJson(value) ?? value : value;
        parsed = setAtPath(parsed, segments, newValue);
      }
    } catch {
      continue;
    }
  }

  return JSON.stringify(parsed);
}

/**
 * json_replace(x, path, value, ...) - Replace values only if path exists
 */
export function json_replace(x: SqlValue, ...pathValuePairs: SqlValue[]): SqlValue {
  if (x === null || pathValuePairs.length < 2) return null;
  if (pathValuePairs.length % 2 !== 0) return null;

  let parsed = parseJson(x);
  if (parsed === undefined) return null;

  for (let i = 0; i < pathValuePairs.length; i += 2) {
    const path = pathValuePairs[i];
    const value = pathValuePairs[i + 1];

    if (path === null) continue;

    try {
      const segments = parsePath(String(path));
      const existing = getAtPath(parsed, segments);

      // Only replace if exists
      if (existing !== undefined) {
        const newValue = typeof value === 'string' ? parseJson(value) ?? value : value;
        parsed = setAtPath(parsed, segments, newValue);
      }
    } catch {
      continue;
    }
  }

  return JSON.stringify(parsed);
}

/**
 * json_set(x, path, value, ...) - Set values (insert or replace)
 */
export function json_set(x: SqlValue, ...pathValuePairs: SqlValue[]): SqlValue {
  if (x === null || pathValuePairs.length < 2) return null;
  if (pathValuePairs.length % 2 !== 0) return null;

  let parsed = parseJson(x);
  if (parsed === undefined) return null;

  for (let i = 0; i < pathValuePairs.length; i += 2) {
    const path = pathValuePairs[i];
    const value = pathValuePairs[i + 1];

    if (path === null) continue;

    try {
      const segments = parsePath(String(path));
      const newValue = typeof value === 'string' ? parseJson(value) ?? value : value;
      parsed = setAtPath(parsed, segments, newValue);
    } catch {
      continue;
    }
  }

  return JSON.stringify(parsed);
}

/**
 * json_remove(x, path, ...) - Remove values at paths
 */
export function json_remove(x: SqlValue, ...paths: SqlValue[]): SqlValue {
  if (x === null) return null;

  let parsed = parseJson(x);
  if (parsed === undefined) return null;

  for (const path of paths) {
    if (path === null) continue;

    try {
      const segments = parsePath(String(path));
      parsed = removeAtPath(parsed, segments);
    } catch {
      continue;
    }
  }

  return JSON.stringify(parsed);
}

/**
 * json_patch(x, y) - Apply JSON merge patch (RFC 7396)
 */
export function json_patch(x: SqlValue, y: SqlValue): SqlValue {
  if (x === null) return y === null ? null : json(y);
  if (y === null) return json(x);

  const target = parseJson(x);
  const patch = parseJson(y);

  if (target === undefined || patch === undefined) return null;

  const result = applyMergePatch(target, patch);
  return JSON.stringify(result);
}

function applyMergePatch(target: unknown, patch: unknown): unknown {
  if (patch === null || typeof patch !== 'object' || Array.isArray(patch)) {
    return patch;
  }

  let result: Record<string, unknown>;
  if (target === null || typeof target !== 'object' || Array.isArray(target)) {
    result = {};
  } else {
    result = { ...target };
  }

  for (const [key, value] of Object.entries(patch as Record<string, unknown>)) {
    if (value === null) {
      delete result[key];
    } else {
      result[key] = applyMergePatch(result[key], value);
    }
  }

  return result;
}

/**
 * json_quote(x) - Convert SQL value to JSON value
 */
export function json_quote(x: SqlValue): SqlValue {
  if (x === null) return 'null';
  if (typeof x === 'string') return JSON.stringify(x);
  if (typeof x === 'number') return JSON.stringify(x);
  if (typeof x === 'bigint') return x.toString();
  if (typeof x === 'boolean') return x ? 'true' : 'false';
  if (x instanceof Date) return JSON.stringify(x.toISOString());
  if (x instanceof Uint8Array) {
    // Blobs become JSON strings with base64? or hex?
    const hex = Array.from(x)
      .map(b => b.toString(16).padStart(2, '0'))
      .join('');
    return JSON.stringify(hex);
  }
  return JSON.stringify(String(x));
}

/**
 * json_each(x, path?) - Returns rows for each element
 * Returns: { key, value, type, atom, id, parent, fullkey, path }
 */
export interface JsonEachRow {
  key: string | number | null;
  value: string;
  type: string;
  atom: SqlValue;
  id: number;
  parent: number | null;
  fullkey: string;
  path: string;
}

export function json_each(x: SqlValue, path?: SqlValue): JsonEachRow[] {
  if (x === null) return [];

  const parsed = parseJson(x);
  if (parsed === undefined) return [];

  let root = parsed;
  let rootPath = '$';

  if (path !== undefined && path !== null) {
    try {
      const segments = parsePath(String(path));
      root = getAtPath(parsed, segments);
      if (root === undefined) return [];
      rootPath = String(path);
    } catch {
      return [];
    }
  }

  const rows: JsonEachRow[] = [];
  let id = 0;

  if (Array.isArray(root)) {
    for (let i = 0; i < root.length; i++) {
      const value = root[i];
      const type = getJsonType(value);
      rows.push({
        key: i,
        value: JSON.stringify(value),
        type,
        atom: type === 'object' || type === 'array' ? null : (value as SqlValue),
        id: id++,
        parent: null,
        fullkey: `${rootPath}[${i}]`,
        path: rootPath,
      });
    }
  } else if (typeof root === 'object' && root !== null) {
    for (const [key, value] of Object.entries(root)) {
      const type = getJsonType(value);
      rows.push({
        key,
        value: JSON.stringify(value),
        type,
        atom: type === 'object' || type === 'array' ? null : (value as SqlValue),
        id: id++,
        parent: null,
        fullkey: `${rootPath}.${key}`,
        path: rootPath,
      });
    }
  }

  return rows;
}

/**
 * json_tree(x, path?) - Returns rows for entire tree (recursive walk)
 * Returns: { key, value, type, atom, id, parent, fullkey, path }
 */
export interface JsonTreeRow {
  key: string | number | null;
  value: string;
  type: string;
  atom: SqlValue;
  id: number;
  parent: number | null;
  fullkey: string;
  path: string;
}

export function json_tree(x: SqlValue, path?: SqlValue): JsonTreeRow[] {
  if (x === null) return [];

  const parsed = parseJson(x);
  if (parsed === undefined) return [];

  let root = parsed;
  let rootPath = '$';

  if (path !== undefined && path !== null) {
    try {
      const segments = parsePath(String(path));
      root = getAtPath(parsed, segments);
      if (root === undefined) return [];
      rootPath = String(path);
    } catch {
      return [];
    }
  }

  const rows: JsonTreeRow[] = [];
  let idCounter = 0;

  function walk(
    value: unknown,
    key: string | number | null,
    parentId: number | null,
    currentPath: string,
    fullkey: string
  ): void {
    const type = getJsonType(value);
    const id = idCounter++;

    rows.push({
      key,
      value: JSON.stringify(value),
      type,
      atom: type === 'object' || type === 'array' ? null : (value as SqlValue),
      id,
      parent: parentId,
      fullkey,
      path: currentPath,
    });

    if (Array.isArray(value)) {
      for (let i = 0; i < value.length; i++) {
        walk(value[i], i, id, fullkey, `${fullkey}[${i}]`);
      }
    } else if (typeof value === 'object' && value !== null) {
      for (const [k, v] of Object.entries(value)) {
        walk(v, k, id, fullkey, `${fullkey}.${k}`);
      }
    }
  }

  walk(root, null, null, rootPath, rootPath);

  return rows;
}

// =============================================================================
// FUNCTION SIGNATURES FOR REGISTRY
// =============================================================================

export const jsonFunctions: Record<string, SqlFunction> = {
  json: { fn: json, minArgs: 1, maxArgs: 1 },
  json_valid: { fn: json_valid, minArgs: 1, maxArgs: 1 },
  json_extract: { fn: json_extract, minArgs: 2, maxArgs: Infinity },
  json_type: { fn: json_type, minArgs: 1, maxArgs: 2 },
  json_array: { fn: json_array, minArgs: 0, maxArgs: Infinity },
  json_object: { fn: json_object, minArgs: 0, maxArgs: Infinity },
  json_array_length: { fn: json_array_length, minArgs: 1, maxArgs: 2 },
  json_insert: { fn: json_insert, minArgs: 3, maxArgs: Infinity },
  json_replace: { fn: json_replace, minArgs: 3, maxArgs: Infinity },
  json_set: { fn: json_set, minArgs: 3, maxArgs: Infinity },
  json_remove: { fn: json_remove, minArgs: 2, maxArgs: Infinity },
  json_patch: { fn: json_patch, minArgs: 2, maxArgs: 2 },
  json_quote: { fn: json_quote, minArgs: 1, maxArgs: 1 },
};

export const jsonSignatures: Record<string, FunctionSignature> = {
  json: {
    name: 'json',
    params: [{ name: 'x', type: 'string' }],
    returnType: 'string',
    description: 'Validates and minifies JSON',
  },
  json_valid: {
    name: 'json_valid',
    params: [{ name: 'x', type: 'string' }],
    returnType: 'number',
    description: 'Returns 1 if valid JSON, 0 otherwise',
  },
  json_extract: {
    name: 'json_extract',
    params: [
      { name: 'json', type: 'string' },
      { name: 'path', type: 'string', variadic: true },
    ],
    returnType: 'any',
    description: 'Extract value at JSON path',
  },
  json_type: {
    name: 'json_type',
    params: [
      { name: 'json', type: 'string' },
      { name: 'path', type: 'string', optional: true },
    ],
    returnType: 'string',
    description: 'Returns JSON type at path',
  },
  json_array: {
    name: 'json_array',
    params: [{ name: 'values', type: 'any', variadic: true }],
    returnType: 'string',
    description: 'Create JSON array from arguments',
  },
  json_object: {
    name: 'json_object',
    params: [{ name: 'pairs', type: 'any', variadic: true }],
    returnType: 'string',
    description: 'Create JSON object from key-value pairs',
  },
  json_array_length: {
    name: 'json_array_length',
    params: [
      { name: 'json', type: 'string' },
      { name: 'path', type: 'string', optional: true },
    ],
    returnType: 'number',
    description: 'Returns length of JSON array',
  },
  json_insert: {
    name: 'json_insert',
    params: [
      { name: 'json', type: 'string' },
      { name: 'path', type: 'string' },
      { name: 'value', type: 'any' },
    ],
    returnType: 'string',
    description: 'Insert value if path does not exist',
  },
  json_replace: {
    name: 'json_replace',
    params: [
      { name: 'json', type: 'string' },
      { name: 'path', type: 'string' },
      { name: 'value', type: 'any' },
    ],
    returnType: 'string',
    description: 'Replace value if path exists',
  },
  json_set: {
    name: 'json_set',
    params: [
      { name: 'json', type: 'string' },
      { name: 'path', type: 'string' },
      { name: 'value', type: 'any' },
    ],
    returnType: 'string',
    description: 'Set value (insert or replace)',
  },
  json_remove: {
    name: 'json_remove',
    params: [
      { name: 'json', type: 'string' },
      { name: 'paths', type: 'string', variadic: true },
    ],
    returnType: 'string',
    description: 'Remove values at paths',
  },
  json_patch: {
    name: 'json_patch',
    params: [
      { name: 'target', type: 'string' },
      { name: 'patch', type: 'string' },
    ],
    returnType: 'string',
    description: 'Apply JSON merge patch (RFC 7396)',
  },
  json_quote: {
    name: 'json_quote',
    params: [{ name: 'x', type: 'any' }],
    returnType: 'string',
    description: 'Convert SQL value to JSON value',
  },
};
