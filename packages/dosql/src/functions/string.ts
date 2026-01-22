/**
 * SQLite String Functions
 *
 * Implements SQLite-compatible string functions:
 * - length(x) - Returns the number of characters in string x
 * - substr(x, y, z) - Returns substring starting at position y with length z
 * - upper(x) - Converts string to uppercase
 * - lower(x) - Converts string to lowercase
 * - trim(x), ltrim(x), rtrim(x) - Trim whitespace
 * - replace(x, y, z) - Replace occurrences of y with z in x
 * - instr(x, y) - Returns position of first occurrence of y in x
 * - printf(format, ...) - Formatted string output
 * - quote(x) - Returns SQL string literal
 * - hex(x) - Convert to hexadecimal representation
 * - unhex(x) - Convert hexadecimal to bytes
 * - zeroblob(n) - Returns n zero bytes
 */

import type { SqlValue } from '../engine/types.js';
import type { SqlFunction, FunctionSignature } from './registry.js';

// =============================================================================
// STRING FUNCTION IMPLEMENTATIONS
// =============================================================================

/**
 * length(x) - Returns the number of characters in string x
 * For blobs, returns the number of bytes
 * Returns NULL if x is NULL
 */
export function length(x: SqlValue): SqlValue {
  if (x === null) return null;
  if (typeof x === 'string') return x.length;
  if (x instanceof Uint8Array) return x.length;
  return String(x).length;
}

/**
 * substr(x, y, z) - Returns a substring of x starting at position y
 * y is 1-indexed (SQLite convention)
 * If z is provided, returns at most z characters
 * Negative y counts from the end of the string
 */
export function substr(x: SqlValue, y: SqlValue, z?: SqlValue): SqlValue {
  if (x === null || y === null) return null;

  const str = String(x);
  let start = Number(y);
  const len = z !== undefined && z !== null ? Number(z) : undefined;

  // SQLite is 1-indexed
  if (start > 0) {
    start = start - 1;
  } else if (start < 0) {
    // Negative index: count from end
    start = str.length + start;
    if (start < 0) start = 0;
  } else {
    // start === 0 is treated as 1 in SQLite
    start = 0;
  }

  if (len !== undefined) {
    return str.substring(start, start + len);
  }
  return str.substring(start);
}

/**
 * upper(x) - Convert string to uppercase
 */
export function upper(x: SqlValue): SqlValue {
  if (x === null) return null;
  return String(x).toUpperCase();
}

/**
 * lower(x) - Convert string to lowercase
 */
export function lower(x: SqlValue): SqlValue {
  if (x === null) return null;
  return String(x).toLowerCase();
}

/**
 * trim(x, y?) - Remove characters from both ends of x
 * If y is provided, removes characters in y (default: whitespace)
 */
export function trim(x: SqlValue, y?: SqlValue): SqlValue {
  if (x === null) return null;
  const str = String(x);

  if (y === undefined || y === null) {
    return str.trim();
  }

  const chars = String(y);
  const charSet = new Set(chars.split(''));

  let start = 0;
  let end = str.length;

  while (start < end && charSet.has(str[start])) start++;
  while (end > start && charSet.has(str[end - 1])) end--;

  return str.slice(start, end);
}

/**
 * ltrim(x, y?) - Remove characters from left side of x
 */
export function ltrim(x: SqlValue, y?: SqlValue): SqlValue {
  if (x === null) return null;
  const str = String(x);

  if (y === undefined || y === null) {
    return str.trimStart();
  }

  const chars = String(y);
  const charSet = new Set(chars.split(''));

  let start = 0;
  while (start < str.length && charSet.has(str[start])) start++;

  return str.slice(start);
}

/**
 * rtrim(x, y?) - Remove characters from right side of x
 */
export function rtrim(x: SqlValue, y?: SqlValue): SqlValue {
  if (x === null) return null;
  const str = String(x);

  if (y === undefined || y === null) {
    return str.trimEnd();
  }

  const chars = String(y);
  const charSet = new Set(chars.split(''));

  let end = str.length;
  while (end > 0 && charSet.has(str[end - 1])) end--;

  return str.slice(0, end);
}

/**
 * replace(x, y, z) - Replace all occurrences of y with z in x
 */
export function replace(x: SqlValue, y: SqlValue, z: SqlValue): SqlValue {
  if (x === null || y === null || z === null) return null;

  const str = String(x);
  const search = String(y);
  const replacement = String(z);

  // SQLite replace is global
  return str.split(search).join(replacement);
}

/**
 * instr(x, y) - Returns the 1-indexed position of first occurrence of y in x
 * Returns 0 if not found
 */
export function instr(x: SqlValue, y: SqlValue): SqlValue {
  if (x === null || y === null) return null;

  const str = String(x);
  const search = String(y);

  const pos = str.indexOf(search);
  // SQLite returns 1-indexed position, 0 if not found
  return pos === -1 ? 0 : pos + 1;
}

/**
 * printf(format, ...) - Formatted string output
 * Supports: %s (string), %d (integer), %f (float), %% (literal %)
 * Also supports width and precision specifiers
 */
export function printf(format: SqlValue, ...args: SqlValue[]): SqlValue {
  if (format === null) return null;

  const fmt = String(format);
  let argIndex = 0;

  return fmt.replace(/%(-?\d*\.?\d*)([sdfeEgGxXoc%])/g, (match, spec, type) => {
    if (type === '%') return '%';

    if (argIndex >= args.length) return match;
    const arg = args[argIndex++];

    if (arg === null) return 'NULL';

    switch (type) {
      case 's':
        return formatString(String(arg), spec);
      case 'd':
      case 'i':
        return formatInteger(Number(arg), spec);
      case 'f':
      case 'e':
      case 'E':
      case 'g':
      case 'G':
        return formatFloat(Number(arg), spec, type);
      case 'x':
        return Math.floor(Number(arg)).toString(16);
      case 'X':
        return Math.floor(Number(arg)).toString(16).toUpperCase();
      case 'o':
        return Math.floor(Number(arg)).toString(8);
      case 'c':
        return String.fromCharCode(Number(arg));
      default:
        return match;
    }
  });
}

function formatString(s: string, spec: string): string {
  if (!spec) return s;
  const width = parseInt(spec, 10);
  if (isNaN(width)) return s;
  if (width < 0) {
    // Left-align
    return s.padEnd(-width);
  }
  return s.padStart(width);
}

function formatInteger(n: number, spec: string): string {
  const int = Math.floor(n);
  if (!spec) return int.toString();
  const width = parseInt(spec, 10);
  if (isNaN(width)) return int.toString();
  const str = int.toString();
  if (width < 0) {
    return str.padEnd(-width);
  }
  return str.padStart(width);
}

function formatFloat(n: number, spec: string, type: string): string {
  const match = spec.match(/(-?\d*)\.?(\d*)/);
  const width = match && match[1] ? parseInt(match[1], 10) : 0;
  const precision = match && match[2] ? parseInt(match[2], 10) : 6;

  let str: string;
  switch (type) {
    case 'e':
      str = n.toExponential(precision);
      break;
    case 'E':
      str = n.toExponential(precision).toUpperCase();
      break;
    case 'g':
    case 'G':
      // Use shorter of %e or %f
      const exp = n.toExponential(precision);
      const fix = n.toFixed(precision);
      str = exp.length < fix.length ? exp : fix;
      if (type === 'G') str = str.toUpperCase();
      break;
    default:
      str = n.toFixed(precision);
  }

  if (width !== 0) {
    if (width < 0) {
      str = str.padEnd(-width);
    } else {
      str = str.padStart(width);
    }
  }

  return str;
}

/**
 * quote(x) - Returns a string that is a SQL string literal
 * NULL returns 'NULL', strings get single-quoted with escaping
 */
export function quote(x: SqlValue): SqlValue {
  if (x === null) return 'NULL';

  if (typeof x === 'string') {
    // Escape single quotes by doubling them
    const escaped = x.replace(/'/g, "''");
    return `'${escaped}'`;
  }

  if (x instanceof Uint8Array) {
    // Blob literal in SQLite
    return `X'${hex(x)}'`;
  }

  if (typeof x === 'number' || typeof x === 'bigint') {
    return String(x);
  }

  if (typeof x === 'boolean') {
    return x ? '1' : '0';
  }

  if (x instanceof Date) {
    return `'${x.toISOString()}'`;
  }

  return String(x);
}

/**
 * hex(x) - Returns a hexadecimal representation of x
 * For strings, returns hex of UTF-8 bytes
 * For blobs, returns hex of bytes
 */
export function hex(x: SqlValue): SqlValue {
  if (x === null) return null;

  let bytes: Uint8Array;

  if (x instanceof Uint8Array) {
    bytes = x;
  } else {
    // Convert string to UTF-8 bytes
    const str = String(x);
    bytes = new TextEncoder().encode(str);
  }

  return Array.from(bytes)
    .map(b => b.toString(16).padStart(2, '0').toUpperCase())
    .join('');
}

/**
 * unhex(x, y?) - Convert hexadecimal string to blob
 * If y is provided, it's the default value if x is not valid hex
 */
export function unhex(x: SqlValue, y?: SqlValue): SqlValue {
  if (x === null) return null;

  const hexStr = String(x);

  // Validate hex string
  if (!/^[0-9a-fA-F]*$/.test(hexStr)) {
    return y !== undefined ? y : null;
  }

  // Must have even length
  if (hexStr.length % 2 !== 0) {
    return y !== undefined ? y : null;
  }

  const bytes = new Uint8Array(hexStr.length / 2);
  for (let i = 0; i < hexStr.length; i += 2) {
    bytes[i / 2] = parseInt(hexStr.slice(i, i + 2), 16);
  }

  return bytes;
}

/**
 * zeroblob(n) - Returns a blob of n zero bytes
 */
export function zeroblob(n: SqlValue): SqlValue {
  if (n === null) return null;

  const size = Math.max(0, Math.floor(Number(n)));
  return new Uint8Array(size);
}

/**
 * concat(...args) - Concatenate strings
 * NULL values are treated as empty strings
 */
export function concat(...args: SqlValue[]): SqlValue {
  return args.map(a => (a === null ? '' : String(a))).join('');
}

/**
 * concat_ws(separator, ...args) - Concatenate with separator
 * NULL values are skipped
 */
export function concat_ws(separator: SqlValue, ...args: SqlValue[]): SqlValue {
  if (separator === null) return null;
  const sep = String(separator);
  const nonNull = args.filter(a => a !== null).map(a => String(a));
  return nonNull.join(sep);
}

/**
 * char(...args) - Returns characters for given Unicode code points
 */
export function char_fn(...args: SqlValue[]): SqlValue {
  return args
    .filter(a => a !== null)
    .map(a => String.fromCodePoint(Number(a)))
    .join('');
}

/**
 * unicode(x) - Returns Unicode code point of first character
 */
export function unicode(x: SqlValue): SqlValue {
  if (x === null) return null;
  const str = String(x);
  if (str.length === 0) return null;
  return str.codePointAt(0) ?? null;
}

/**
 * like(x, pattern, escape?) - SQL LIKE pattern matching
 * % matches any sequence, _ matches single character
 */
export function like(x: SqlValue, pattern: SqlValue, escape?: SqlValue): SqlValue {
  if (x === null || pattern === null) return null;

  const str = String(x);
  const pat = String(pattern);
  const esc = escape !== undefined && escape !== null ? String(escape) : null;

  // Convert LIKE pattern to regex
  let regexPattern = '';
  let i = 0;

  while (i < pat.length) {
    const c = pat[i];

    if (esc && c === esc && i + 1 < pat.length) {
      // Escape next character
      const next = pat[i + 1];
      regexPattern += escapeRegex(next);
      i += 2;
    } else if (c === '%') {
      regexPattern += '.*';
      i++;
    } else if (c === '_') {
      regexPattern += '.';
      i++;
    } else {
      regexPattern += escapeRegex(c);
      i++;
    }
  }

  const regex = new RegExp(`^${regexPattern}$`, 'i');
  return regex.test(str);
}

/**
 * glob(pattern, x) - Unix glob pattern matching (case-sensitive)
 * * matches any sequence, ? matches single character, [...] matches character class
 */
export function glob(pattern: SqlValue, x: SqlValue): SqlValue {
  if (x === null || pattern === null) return null;

  const str = String(x);
  const pat = String(pattern);

  // Convert glob pattern to regex
  let regexPattern = '';
  let i = 0;

  while (i < pat.length) {
    const c = pat[i];

    if (c === '*') {
      regexPattern += '.*';
      i++;
    } else if (c === '?') {
      regexPattern += '.';
      i++;
    } else if (c === '[') {
      // Find matching ]
      const end = pat.indexOf(']', i + 1);
      if (end === -1) {
        regexPattern += '\\[';
        i++;
      } else {
        regexPattern += pat.slice(i, end + 1);
        i = end + 1;
      }
    } else {
      regexPattern += escapeRegex(c);
      i++;
    }
  }

  const regex = new RegExp(`^${regexPattern}$`);
  return regex.test(str);
}

function escapeRegex(str: string): string {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// =============================================================================
// FUNCTION SIGNATURES FOR REGISTRY
// =============================================================================

export const stringFunctions: Record<string, SqlFunction> = {
  length: { fn: length, minArgs: 1, maxArgs: 1 },
  substr: { fn: substr, minArgs: 2, maxArgs: 3 },
  substring: { fn: substr, minArgs: 2, maxArgs: 3 },
  upper: { fn: upper, minArgs: 1, maxArgs: 1 },
  lower: { fn: lower, minArgs: 1, maxArgs: 1 },
  trim: { fn: trim, minArgs: 1, maxArgs: 2 },
  ltrim: { fn: ltrim, minArgs: 1, maxArgs: 2 },
  rtrim: { fn: rtrim, minArgs: 1, maxArgs: 2 },
  replace: { fn: replace, minArgs: 3, maxArgs: 3 },
  instr: { fn: instr, minArgs: 2, maxArgs: 2 },
  printf: { fn: printf, minArgs: 1, maxArgs: Infinity },
  quote: { fn: quote, minArgs: 1, maxArgs: 1 },
  hex: { fn: hex, minArgs: 1, maxArgs: 1 },
  unhex: { fn: unhex, minArgs: 1, maxArgs: 2 },
  zeroblob: { fn: zeroblob, minArgs: 1, maxArgs: 1 },
  concat: { fn: concat, minArgs: 0, maxArgs: Infinity },
  concat_ws: { fn: concat_ws, minArgs: 1, maxArgs: Infinity },
  char: { fn: char_fn, minArgs: 0, maxArgs: Infinity },
  unicode: { fn: unicode, minArgs: 1, maxArgs: 1 },
  like: { fn: like, minArgs: 2, maxArgs: 3 },
  glob: { fn: glob, minArgs: 2, maxArgs: 2 },
};

export const stringSignatures: Record<string, FunctionSignature> = {
  length: {
    name: 'length',
    params: [{ name: 'x', type: 'any' }],
    returnType: 'number',
    description: 'Returns the number of characters in string x',
  },
  substr: {
    name: 'substr',
    params: [
      { name: 'x', type: 'string' },
      { name: 'y', type: 'number' },
      { name: 'z', type: 'number', optional: true },
    ],
    returnType: 'string',
    description: 'Returns substring starting at position y with optional length z',
  },
  upper: {
    name: 'upper',
    params: [{ name: 'x', type: 'string' }],
    returnType: 'string',
    description: 'Converts string to uppercase',
  },
  lower: {
    name: 'lower',
    params: [{ name: 'x', type: 'string' }],
    returnType: 'string',
    description: 'Converts string to lowercase',
  },
  trim: {
    name: 'trim',
    params: [
      { name: 'x', type: 'string' },
      { name: 'y', type: 'string', optional: true },
    ],
    returnType: 'string',
    description: 'Removes characters from both ends of x',
  },
  ltrim: {
    name: 'ltrim',
    params: [
      { name: 'x', type: 'string' },
      { name: 'y', type: 'string', optional: true },
    ],
    returnType: 'string',
    description: 'Removes characters from left side of x',
  },
  rtrim: {
    name: 'rtrim',
    params: [
      { name: 'x', type: 'string' },
      { name: 'y', type: 'string', optional: true },
    ],
    returnType: 'string',
    description: 'Removes characters from right side of x',
  },
  replace: {
    name: 'replace',
    params: [
      { name: 'x', type: 'string' },
      { name: 'y', type: 'string' },
      { name: 'z', type: 'string' },
    ],
    returnType: 'string',
    description: 'Replace all occurrences of y with z in x',
  },
  instr: {
    name: 'instr',
    params: [
      { name: 'x', type: 'string' },
      { name: 'y', type: 'string' },
    ],
    returnType: 'number',
    description: 'Returns 1-indexed position of first occurrence of y in x',
  },
  printf: {
    name: 'printf',
    params: [
      { name: 'format', type: 'string' },
      { name: 'args', type: 'any', variadic: true },
    ],
    returnType: 'string',
    description: 'Formatted string output',
  },
  quote: {
    name: 'quote',
    params: [{ name: 'x', type: 'any' }],
    returnType: 'string',
    description: 'Returns SQL string literal for x',
  },
  hex: {
    name: 'hex',
    params: [{ name: 'x', type: 'any' }],
    returnType: 'string',
    description: 'Returns hexadecimal representation of x',
  },
  unhex: {
    name: 'unhex',
    params: [
      { name: 'x', type: 'string' },
      { name: 'y', type: 'any', optional: true },
    ],
    returnType: 'bytes',
    description: 'Convert hexadecimal string to blob',
  },
  zeroblob: {
    name: 'zeroblob',
    params: [{ name: 'n', type: 'number' }],
    returnType: 'bytes',
    description: 'Returns a blob of n zero bytes',
  },
};
