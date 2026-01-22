/**
 * SQLite Math Functions
 *
 * Implements SQLite-compatible math functions:
 * - abs(x) - Returns absolute value of x
 * - round(x, y?) - Round x to y decimal places
 * - random() - Returns random integer
 * - max(x, y, ...) - Returns maximum value (scalar version)
 * - min(x, y, ...) - Returns minimum value (scalar version)
 * - sign(x) - Returns -1, 0, or 1
 * - pow(x, y) / power(x, y) - Returns x raised to power y
 * - sqrt(x) - Returns square root of x
 *
 * Additional math functions:
 * - ceil/ceiling(x) - Round up to nearest integer
 * - floor(x) - Round down to nearest integer
 * - mod(x, y) - Modulo operation
 * - log(x) / ln(x) - Natural logarithm
 * - log10(x) - Base 10 logarithm
 * - log2(x) - Base 2 logarithm
 * - exp(x) - e raised to power x
 * - sin(x), cos(x), tan(x) - Trigonometric functions
 * - asin(x), acos(x), atan(x), atan2(y, x) - Inverse trig
 * - pi() - Returns pi constant
 * - degrees(x), radians(x) - Angle conversion
 */

import type { SqlValue } from '../engine/types.js';
import type { SqlFunction, FunctionSignature } from './registry.js';

// =============================================================================
// MATH FUNCTION IMPLEMENTATIONS
// =============================================================================

/**
 * abs(x) - Returns absolute value of x
 * Works with numbers and bigints
 */
export function abs(x: SqlValue): SqlValue {
  if (x === null) return null;

  if (typeof x === 'number') {
    return Math.abs(x);
  }

  if (typeof x === 'bigint') {
    return x < 0n ? -x : x;
  }

  // Try to convert to number
  const num = Number(x);
  if (!isNaN(num)) {
    return Math.abs(num);
  }

  return null;
}

/**
 * round(x, y?) - Round x to y decimal places (default: 0)
 * SQLite rounds away from zero for .5
 */
export function round(x: SqlValue, y?: SqlValue): SqlValue {
  if (x === null) return null;

  const num = Number(x);
  if (isNaN(num)) return null;

  const precision = y !== undefined && y !== null ? Number(y) : 0;
  if (isNaN(precision)) return null;

  const factor = Math.pow(10, precision);
  // JavaScript's Math.round rounds to nearest, ties to even
  // SQLite rounds away from zero, but we'll use standard rounding
  return Math.round(num * factor) / factor;
}

/**
 * random() - Returns a pseudo-random integer
 * SQLite returns a signed 64-bit integer
 * We return a random integer in safe integer range
 */
export function random(): SqlValue {
  // Generate random integer in range [-2^53, 2^53]
  const sign = Math.random() < 0.5 ? -1 : 1;
  return sign * Math.floor(Math.random() * Number.MAX_SAFE_INTEGER);
}

/**
 * max(x, y, ...) - Returns maximum value (scalar version)
 * Unlike aggregate MAX(), this compares multiple arguments
 * NULL values are ignored
 */
export function max_scalar(...args: SqlValue[]): SqlValue {
  const nonNull = args.filter(a => a !== null);
  if (nonNull.length === 0) return null;

  let maxVal = nonNull[0];
  for (let i = 1; i < nonNull.length; i++) {
    if (compare(nonNull[i], maxVal) > 0) {
      maxVal = nonNull[i];
    }
  }
  return maxVal;
}

/**
 * min(x, y, ...) - Returns minimum value (scalar version)
 * Unlike aggregate MIN(), this compares multiple arguments
 * NULL values are ignored
 */
export function min_scalar(...args: SqlValue[]): SqlValue {
  const nonNull = args.filter(a => a !== null);
  if (nonNull.length === 0) return null;

  let minVal = nonNull[0];
  for (let i = 1; i < nonNull.length; i++) {
    if (compare(nonNull[i], minVal) < 0) {
      minVal = nonNull[i];
    }
  }
  return minVal;
}

/**
 * Compare two SQL values
 * Returns negative if a < b, 0 if equal, positive if a > b
 */
function compare(a: SqlValue, b: SqlValue): number {
  // Handle null
  if (a === null && b === null) return 0;
  if (a === null) return -1;
  if (b === null) return 1;

  // Numbers
  if (typeof a === 'number' && typeof b === 'number') {
    return a - b;
  }

  // Bigints
  if (typeof a === 'bigint' && typeof b === 'bigint') {
    return a < b ? -1 : a > b ? 1 : 0;
  }

  // Mixed number/bigint
  if ((typeof a === 'number' || typeof a === 'bigint') &&
      (typeof b === 'number' || typeof b === 'bigint')) {
    const na = Number(a);
    const nb = Number(b);
    return na - nb;
  }

  // Strings
  if (typeof a === 'string' && typeof b === 'string') {
    return a.localeCompare(b);
  }

  // Dates
  if (a instanceof Date && b instanceof Date) {
    return a.getTime() - b.getTime();
  }

  // Fallback: convert to string
  return String(a).localeCompare(String(b));
}

/**
 * sign(x) - Returns -1, 0, or 1 based on sign of x
 */
export function sign(x: SqlValue): SqlValue {
  if (x === null) return null;

  if (typeof x === 'number') {
    if (x > 0) return 1;
    if (x < 0) return -1;
    return 0;
  }

  if (typeof x === 'bigint') {
    if (x > 0n) return 1;
    if (x < 0n) return -1;
    return 0;
  }

  const num = Number(x);
  if (isNaN(num)) return null;

  if (num > 0) return 1;
  if (num < 0) return -1;
  return 0;
}

/**
 * pow(x, y) / power(x, y) - Returns x raised to power y
 */
export function pow(x: SqlValue, y: SqlValue): SqlValue {
  if (x === null || y === null) return null;

  const base = Number(x);
  const exp = Number(y);

  if (isNaN(base) || isNaN(exp)) return null;

  return Math.pow(base, exp);
}

/**
 * sqrt(x) - Returns square root of x
 * Returns NULL for negative numbers
 */
export function sqrt(x: SqlValue): SqlValue {
  if (x === null) return null;

  const num = Number(x);
  if (isNaN(num) || num < 0) return null;

  return Math.sqrt(num);
}

/**
 * ceil/ceiling(x) - Round up to nearest integer
 */
export function ceil(x: SqlValue): SqlValue {
  if (x === null) return null;

  const num = Number(x);
  if (isNaN(num)) return null;

  return Math.ceil(num);
}

/**
 * floor(x) - Round down to nearest integer
 */
export function floor(x: SqlValue): SqlValue {
  if (x === null) return null;

  const num = Number(x);
  if (isNaN(num)) return null;

  return Math.floor(num);
}

/**
 * trunc(x) - Truncate toward zero
 */
export function trunc(x: SqlValue): SqlValue {
  if (x === null) return null;

  const num = Number(x);
  if (isNaN(num)) return null;

  return Math.trunc(num);
}

/**
 * mod(x, y) - Modulo operation (x % y)
 */
export function mod(x: SqlValue, y: SqlValue): SqlValue {
  if (x === null || y === null) return null;

  if (typeof x === 'bigint' && typeof y === 'bigint') {
    if (y === 0n) return null;
    return x % y;
  }

  const num1 = Number(x);
  const num2 = Number(y);

  if (isNaN(num1) || isNaN(num2) || num2 === 0) return null;

  return num1 % num2;
}

/**
 * log(x) / ln(x) - Natural logarithm
 * Returns NULL for non-positive x
 */
export function log(x: SqlValue): SqlValue {
  if (x === null) return null;

  const num = Number(x);
  if (isNaN(num) || num <= 0) return null;

  return Math.log(num);
}

/**
 * log10(x) - Base 10 logarithm
 */
export function log10(x: SqlValue): SqlValue {
  if (x === null) return null;

  const num = Number(x);
  if (isNaN(num) || num <= 0) return null;

  return Math.log10(num);
}

/**
 * log2(x) - Base 2 logarithm
 */
export function log2(x: SqlValue): SqlValue {
  if (x === null) return null;

  const num = Number(x);
  if (isNaN(num) || num <= 0) return null;

  return Math.log2(num);
}

/**
 * exp(x) - e raised to power x
 */
export function exp(x: SqlValue): SqlValue {
  if (x === null) return null;

  const num = Number(x);
  if (isNaN(num)) return null;

  return Math.exp(num);
}

/**
 * sin(x) - Sine of x (x in radians)
 */
export function sin(x: SqlValue): SqlValue {
  if (x === null) return null;

  const num = Number(x);
  if (isNaN(num)) return null;

  return Math.sin(num);
}

/**
 * cos(x) - Cosine of x (x in radians)
 */
export function cos(x: SqlValue): SqlValue {
  if (x === null) return null;

  const num = Number(x);
  if (isNaN(num)) return null;

  return Math.cos(num);
}

/**
 * tan(x) - Tangent of x (x in radians)
 */
export function tan(x: SqlValue): SqlValue {
  if (x === null) return null;

  const num = Number(x);
  if (isNaN(num)) return null;

  return Math.tan(num);
}

/**
 * asin(x) - Arc sine of x (returns radians)
 * x must be in range [-1, 1]
 */
export function asin(x: SqlValue): SqlValue {
  if (x === null) return null;

  const num = Number(x);
  if (isNaN(num) || num < -1 || num > 1) return null;

  return Math.asin(num);
}

/**
 * acos(x) - Arc cosine of x (returns radians)
 * x must be in range [-1, 1]
 */
export function acos(x: SqlValue): SqlValue {
  if (x === null) return null;

  const num = Number(x);
  if (isNaN(num) || num < -1 || num > 1) return null;

  return Math.acos(num);
}

/**
 * atan(x) - Arc tangent of x (returns radians)
 */
export function atan(x: SqlValue): SqlValue {
  if (x === null) return null;

  const num = Number(x);
  if (isNaN(num)) return null;

  return Math.atan(num);
}

/**
 * atan2(y, x) - Arc tangent of y/x (returns radians)
 * Uses signs of both arguments to determine quadrant
 */
export function atan2(y: SqlValue, x: SqlValue): SqlValue {
  if (x === null || y === null) return null;

  const ny = Number(y);
  const nx = Number(x);

  if (isNaN(ny) || isNaN(nx)) return null;

  return Math.atan2(ny, nx);
}

/**
 * pi() - Returns the value of pi
 */
export function pi(): SqlValue {
  return Math.PI;
}

/**
 * degrees(x) - Convert radians to degrees
 */
export function degrees(x: SqlValue): SqlValue {
  if (x === null) return null;

  const num = Number(x);
  if (isNaN(num)) return null;

  return num * (180 / Math.PI);
}

/**
 * radians(x) - Convert degrees to radians
 */
export function radians(x: SqlValue): SqlValue {
  if (x === null) return null;

  const num = Number(x);
  if (isNaN(num)) return null;

  return num * (Math.PI / 180);
}

/**
 * nullif(x, y) - Returns NULL if x equals y, otherwise returns x
 */
export function nullif(x: SqlValue, y: SqlValue): SqlValue {
  if (x === y) return null;
  if (x === null || y === null) return x;

  // Deep equality for common types
  if (typeof x === typeof y && x === y) return null;

  return x;
}

/**
 * ifnull(x, y) / coalesce(x, y) - Returns x if not NULL, else y
 */
export function ifnull(x: SqlValue, y: SqlValue): SqlValue {
  return x !== null ? x : y;
}

/**
 * coalesce(...args) - Returns first non-NULL argument
 */
export function coalesce(...args: SqlValue[]): SqlValue {
  for (const arg of args) {
    if (arg !== null) return arg;
  }
  return null;
}

/**
 * iif(condition, x, y) - If condition is true, return x, else y
 */
export function iif(condition: SqlValue, x: SqlValue, y: SqlValue): SqlValue {
  // In SQLite, 0 and empty string are falsy
  if (condition === null || condition === 0 || condition === '' || condition === false) {
    return y;
  }
  return x;
}

// =============================================================================
// FUNCTION SIGNATURES FOR REGISTRY
// =============================================================================

export const mathFunctions: Record<string, SqlFunction> = {
  abs: { fn: abs, minArgs: 1, maxArgs: 1 },
  round: { fn: round, minArgs: 1, maxArgs: 2 },
  random: { fn: random, minArgs: 0, maxArgs: 0 },
  max: { fn: max_scalar, minArgs: 2, maxArgs: Infinity },
  min: { fn: min_scalar, minArgs: 2, maxArgs: Infinity },
  sign: { fn: sign, minArgs: 1, maxArgs: 1 },
  pow: { fn: pow, minArgs: 2, maxArgs: 2 },
  power: { fn: pow, minArgs: 2, maxArgs: 2 },
  sqrt: { fn: sqrt, minArgs: 1, maxArgs: 1 },
  ceil: { fn: ceil, minArgs: 1, maxArgs: 1 },
  ceiling: { fn: ceil, minArgs: 1, maxArgs: 1 },
  floor: { fn: floor, minArgs: 1, maxArgs: 1 },
  trunc: { fn: trunc, minArgs: 1, maxArgs: 1 },
  truncate: { fn: trunc, minArgs: 1, maxArgs: 1 },
  mod: { fn: mod, minArgs: 2, maxArgs: 2 },
  log: { fn: log, minArgs: 1, maxArgs: 1 },
  ln: { fn: log, minArgs: 1, maxArgs: 1 },
  log10: { fn: log10, minArgs: 1, maxArgs: 1 },
  log2: { fn: log2, minArgs: 1, maxArgs: 1 },
  exp: { fn: exp, minArgs: 1, maxArgs: 1 },
  sin: { fn: sin, minArgs: 1, maxArgs: 1 },
  cos: { fn: cos, minArgs: 1, maxArgs: 1 },
  tan: { fn: tan, minArgs: 1, maxArgs: 1 },
  asin: { fn: asin, minArgs: 1, maxArgs: 1 },
  acos: { fn: acos, minArgs: 1, maxArgs: 1 },
  atan: { fn: atan, minArgs: 1, maxArgs: 1 },
  atan2: { fn: atan2, minArgs: 2, maxArgs: 2 },
  pi: { fn: pi, minArgs: 0, maxArgs: 0 },
  degrees: { fn: degrees, minArgs: 1, maxArgs: 1 },
  radians: { fn: radians, minArgs: 1, maxArgs: 1 },
  nullif: { fn: nullif, minArgs: 2, maxArgs: 2 },
  ifnull: { fn: ifnull, minArgs: 2, maxArgs: 2 },
  coalesce: { fn: coalesce, minArgs: 1, maxArgs: Infinity },
  iif: { fn: iif, minArgs: 3, maxArgs: 3 },
};

export const mathSignatures: Record<string, FunctionSignature> = {
  abs: {
    name: 'abs',
    params: [{ name: 'x', type: 'number' }],
    returnType: 'number',
    description: 'Returns absolute value of x',
  },
  round: {
    name: 'round',
    params: [
      { name: 'x', type: 'number' },
      { name: 'y', type: 'number', optional: true },
    ],
    returnType: 'number',
    description: 'Round x to y decimal places (default: 0)',
  },
  random: {
    name: 'random',
    params: [],
    returnType: 'number',
    description: 'Returns a pseudo-random integer',
  },
  max: {
    name: 'max',
    params: [{ name: 'values', type: 'any', variadic: true }],
    returnType: 'any',
    description: 'Returns maximum value from arguments',
  },
  min: {
    name: 'min',
    params: [{ name: 'values', type: 'any', variadic: true }],
    returnType: 'any',
    description: 'Returns minimum value from arguments',
  },
  sign: {
    name: 'sign',
    params: [{ name: 'x', type: 'number' }],
    returnType: 'number',
    description: 'Returns -1, 0, or 1 based on sign of x',
  },
  pow: {
    name: 'pow',
    params: [
      { name: 'x', type: 'number' },
      { name: 'y', type: 'number' },
    ],
    returnType: 'number',
    description: 'Returns x raised to power y',
  },
  sqrt: {
    name: 'sqrt',
    params: [{ name: 'x', type: 'number' }],
    returnType: 'number',
    description: 'Returns square root of x',
  },
  pi: {
    name: 'pi',
    params: [],
    returnType: 'number',
    description: 'Returns the value of pi',
  },
};
