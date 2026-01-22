/**
 * SQLite Type Affinity Implementation
 *
 * Implements SQLite type affinity rules as defined in https://www.sqlite.org/datatype3.html
 *
 * SQLite uses a dynamic type system where the type of a value is associated with
 * the value itself, not with its container. Column affinity determines how values
 * are coerced when stored and compared.
 */

/**
 * The five column affinities in SQLite
 */
export type Affinity = 'TEXT' | 'NUMERIC' | 'INTEGER' | 'REAL' | 'BLOB';

/**
 * The five storage classes in SQLite, in sort order
 */
export type StorageClass = 'NULL' | 'INTEGER' | 'REAL' | 'TEXT' | 'BLOB';

/**
 * Value types that can be stored in SQLite
 */
export type StorageValue = null | number | string | Uint8Array | ArrayBuffer;

/**
 * Result of coercing a value to a storage class
 */
export interface CoercedValue {
  value: StorageValue;
  storageClass: StorageClass;
}

/**
 * Operand with optional affinity for comparison operations
 */
export interface ComparisonOperand {
  value: unknown;
  affinity: Affinity | null;
}

/**
 * Coerced operand ready for comparison
 */
export interface CoercedOperand {
  value: StorageValue;
  storageClass: StorageClass;
}

// Storage class sort order (for comparison)
const STORAGE_CLASS_ORDER: Record<StorageClass, number> = {
  NULL: 0,
  INTEGER: 1,
  REAL: 2,
  TEXT: 3,
  BLOB: 4,
};

/**
 * Determines the affinity of a column from its declared type name.
 *
 * Rules (applied in order):
 * 1. If the type contains "INT" -> INTEGER affinity
 * 2. If the type contains "CHAR", "CLOB", or "TEXT" -> TEXT affinity
 * 3. If the type contains "BLOB" or is empty/null -> BLOB affinity
 * 4. If the type contains "REAL", "FLOA", or "DOUB" -> REAL affinity
 * 5. Otherwise -> NUMERIC affinity (default)
 *
 * @param typeName The declared type name for a column
 * @returns The determined affinity
 */
export function determineAffinity(typeName: string | null | undefined): Affinity {
  // Handle null/undefined/empty as BLOB
  if (typeName == null || typeName === '') {
    return 'BLOB';
  }

  const upperType = typeName.toUpperCase();

  // Rule 1: Contains "INT" -> INTEGER
  if (upperType.includes('INT')) {
    return 'INTEGER';
  }

  // Rule 2: Contains "CHAR", "CLOB", or "TEXT" -> TEXT
  if (upperType.includes('CHAR') || upperType.includes('CLOB') || upperType.includes('TEXT')) {
    return 'TEXT';
  }

  // Rule 3: Contains "BLOB" -> BLOB
  if (upperType.includes('BLOB')) {
    return 'BLOB';
  }

  // Rule 4: Contains "REAL", "FLOA", or "DOUB" -> REAL
  if (upperType.includes('REAL') || upperType.includes('FLOA') || upperType.includes('DOUB')) {
    return 'REAL';
  }

  // Rule 5: Default -> NUMERIC
  return 'NUMERIC';
}

/**
 * Determines the storage class of a JavaScript value.
 *
 * @param value The value to classify
 * @returns The storage class
 */
export function determineStorageClass(value: unknown): StorageClass {
  if (value === null || value === undefined) {
    return 'NULL';
  }

  if (typeof value === 'boolean') {
    // Booleans are stored as integers (0 or 1)
    return 'INTEGER';
  }

  if (typeof value === 'bigint') {
    return 'INTEGER';
  }

  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      return 'REAL';
    }
    if (Number.isInteger(value)) {
      return 'INTEGER';
    }
    return 'REAL';
  }

  if (typeof value === 'string') {
    return 'TEXT';
  }

  if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) {
    return 'BLOB';
  }

  // Default to TEXT for other types (objects, etc.)
  return 'TEXT';
}

/**
 * Checks if a string represents a well-formed numeric literal (not hexadecimal).
 *
 * @param str The string to check
 * @returns True if the string is a valid numeric literal
 */
function isNumericLiteral(str: string): boolean {
  const trimmed = str.trim();

  // Empty or whitespace-only is not numeric
  if (trimmed === '') {
    return false;
  }

  // Hexadecimal is not converted
  if (/^0x/i.test(trimmed)) {
    return false;
  }

  // Check for valid numeric format
  // Allow optional sign, digits, optional decimal point, optional exponent
  const numericRegex = /^[+-]?(\d+\.?\d*|\d*\.?\d+)([eE][+-]?\d+)?$/;
  return numericRegex.test(trimmed);
}

/**
 * Checks if a number can be exactly represented as an integer.
 *
 * @param value The number to check
 * @returns True if the value can be represented as an integer
 */
function canBeInteger(value: number): boolean {
  if (!Number.isFinite(value)) {
    return false;
  }
  return Number.isInteger(value) && Math.abs(value) <= Number.MAX_SAFE_INTEGER;
}

/**
 * Coerces a value according to the specified column affinity.
 *
 * @param value The value to coerce
 * @param affinity The target affinity
 * @returns The coerced value and its storage class
 */
export function coerceValue(value: unknown, affinity: Affinity): CoercedValue {
  // NULL and BLOB are never converted by affinity
  if (value === null || value === undefined) {
    return { value: null, storageClass: 'NULL' };
  }

  if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) {
    return {
      value: value as Uint8Array | ArrayBuffer,
      storageClass: 'BLOB',
    };
  }

  // Convert boolean to integer first
  if (typeof value === 'boolean') {
    value = value ? 1 : 0;
  }

  // Convert BigInt to number
  if (typeof value === 'bigint') {
    value = Number(value);
  }

  switch (affinity) {
    case 'TEXT':
      return coerceToText(value);

    case 'NUMERIC':
    case 'INTEGER':
      return coerceToNumeric(value);

    case 'REAL':
      return coerceToReal(value);

    case 'BLOB':
      // BLOB affinity means no coercion - store as-is
      return coerceNoConversion(value);
  }
}

/**
 * Coerces a value to TEXT affinity.
 */
function coerceToText(value: unknown): CoercedValue {
  if (typeof value === 'string') {
    return { value, storageClass: 'TEXT' };
  }

  if (typeof value === 'number') {
    return { value: String(value), storageClass: 'TEXT' };
  }

  // Fallback: convert to string
  return { value: String(value), storageClass: 'TEXT' };
}

/**
 * Coerces a value to NUMERIC/INTEGER affinity.
 *
 * - TEXT is converted to INTEGER or REAL if it's a well-formed numeric literal
 * - Floating-point values that can be exactly represented as integers are converted
 * - Hexadecimal strings are NOT converted
 */
function coerceToNumeric(value: unknown): CoercedValue {
  if (typeof value === 'number') {
    // If it can be an integer, store as integer
    if (canBeInteger(value)) {
      // Normalize -0 to 0
      const intValue = Math.trunc(value);
      return { value: intValue === 0 ? 0 : intValue, storageClass: 'INTEGER' };
    }
    return { value, storageClass: 'REAL' };
  }

  if (typeof value === 'string') {
    // Try to convert string to number
    if (isNumericLiteral(value)) {
      const num = parseFloat(value.trim());

      // Check if the number is too large for safe integer
      if (Math.abs(num) > Number.MAX_SAFE_INTEGER && Number.isInteger(num)) {
        return { value: num, storageClass: 'REAL' };
      }

      if (canBeInteger(num)) {
        return { value: Math.trunc(num), storageClass: 'INTEGER' };
      }
      return { value: num, storageClass: 'REAL' };
    }

    // Non-numeric string stays as TEXT
    return { value, storageClass: 'TEXT' };
  }

  // For other types, determine storage class
  const storageClass = determineStorageClass(value);
  return { value: value as StorageValue, storageClass };
}

/**
 * Coerces a value to REAL affinity.
 *
 * Forces integer values to floating-point representation.
 */
function coerceToReal(value: unknown): CoercedValue {
  if (typeof value === 'number') {
    // Force to REAL even if it could be integer
    return { value, storageClass: 'REAL' };
  }

  if (typeof value === 'string') {
    if (isNumericLiteral(value)) {
      const num = parseFloat(value.trim());
      return { value: num, storageClass: 'REAL' };
    }

    // Non-numeric string stays as TEXT
    return { value, storageClass: 'TEXT' };
  }

  // For other types, determine storage class
  const storageClass = determineStorageClass(value);
  return { value: value as StorageValue, storageClass };
}

/**
 * No conversion - BLOB affinity.
 */
function coerceNoConversion(value: unknown): CoercedValue {
  const storageClass = determineStorageClass(value);

  if (typeof value === 'number') {
    return { value, storageClass };
  }

  if (typeof value === 'string') {
    return { value, storageClass: 'TEXT' };
  }

  return { value: value as StorageValue, storageClass };
}

/**
 * Applies affinity rules for comparison operations.
 *
 * Rules:
 * 1. If one operand has INTEGER, REAL, or NUMERIC affinity AND the other has
 *    TEXT, BLOB, or no affinity -> apply NUMERIC affinity to the other operand
 * 2. If one operand has TEXT affinity AND the other has no affinity ->
 *    apply TEXT affinity to the other operand
 * 3. Otherwise -> no affinity applied
 *
 * @param left The left operand
 * @param right The right operand
 * @returns Tuple of coerced operands
 */
export function applyAffinityForComparison(
  left: ComparisonOperand,
  right: ComparisonOperand
): [CoercedOperand, CoercedOperand] {
  const isNumericAffinity = (a: Affinity | null): boolean =>
    a === 'INTEGER' || a === 'REAL' || a === 'NUMERIC';

  const isTextOrBlobOrNone = (a: Affinity | null): boolean =>
    a === 'TEXT' || a === 'BLOB' || a === null;

  let leftCoerced = coerceValue(left.value, left.affinity ?? 'BLOB');
  let rightCoerced = coerceValue(right.value, right.affinity ?? 'BLOB');

  // Rule 1: Numeric affinity vs TEXT/BLOB/none
  if (isNumericAffinity(left.affinity) && isTextOrBlobOrNone(right.affinity)) {
    rightCoerced = coerceValue(right.value, 'NUMERIC');
  } else if (isNumericAffinity(right.affinity) && isTextOrBlobOrNone(left.affinity)) {
    leftCoerced = coerceValue(left.value, 'NUMERIC');
  }
  // Rule 2: TEXT affinity vs none
  else if (left.affinity === 'TEXT' && right.affinity === null) {
    rightCoerced = coerceValue(right.value, 'TEXT');
  } else if (right.affinity === 'TEXT' && left.affinity === null) {
    leftCoerced = coerceValue(left.value, 'TEXT');
  }

  return [leftCoerced, rightCoerced];
}

/**
 * Compares two values according to SQLite comparison rules.
 *
 * Storage class ordering: NULL < INTEGER/REAL < TEXT < BLOB
 * (INTEGER and REAL are compared numerically with each other)
 *
 * @param a The first value
 * @param b The second value
 * @returns Negative if a < b, positive if a > b, 0 if equal
 */
export function compareWithAffinity(a: unknown, b: unknown): number {
  const aClass = determineStorageClass(a);
  const bClass = determineStorageClass(b);

  // Handle NULL comparisons
  if (aClass === 'NULL' && bClass === 'NULL') {
    return 0;
  }
  if (aClass === 'NULL') {
    return -1;
  }
  if (bClass === 'NULL') {
    return 1;
  }

  // INTEGER and REAL are compared numerically
  const aIsNumeric = aClass === 'INTEGER' || aClass === 'REAL';
  const bIsNumeric = bClass === 'INTEGER' || bClass === 'REAL';

  if (aIsNumeric && bIsNumeric) {
    return (a as number) - (b as number);
  }

  // Different storage classes: use class ordering
  if (aClass !== bClass) {
    // INTEGER/REAL < TEXT < BLOB
    if (aIsNumeric && !bIsNumeric) {
      return -1;
    }
    if (!aIsNumeric && bIsNumeric) {
      return 1;
    }

    // TEXT < BLOB
    if (aClass === 'TEXT' && bClass === 'BLOB') {
      return -1;
    }
    if (aClass === 'BLOB' && bClass === 'TEXT') {
      return 1;
    }

    // Fallback to storage class order
    return STORAGE_CLASS_ORDER[aClass] - STORAGE_CLASS_ORDER[bClass];
  }

  // Same storage class: compare values
  if (aClass === 'TEXT') {
    return (a as string).localeCompare(b as string);
  }

  if (aClass === 'BLOB') {
    return compareBlobs(a as Uint8Array | ArrayBuffer, b as Uint8Array | ArrayBuffer);
  }

  // Should not reach here
  return 0;
}

/**
 * Compares two blobs using memcmp-like comparison.
 */
function compareBlobs(a: Uint8Array | ArrayBuffer, b: Uint8Array | ArrayBuffer): number {
  const aBytes = a instanceof ArrayBuffer ? new Uint8Array(a) : a;
  const bBytes = b instanceof ArrayBuffer ? new Uint8Array(b) : b;

  const minLen = Math.min(aBytes.length, bBytes.length);

  for (let i = 0; i < minLen; i++) {
    if (aBytes[i] !== bBytes[i]) {
      return aBytes[i] - bBytes[i];
    }
  }

  // If all compared bytes are equal, shorter blob comes first
  return aBytes.length - bBytes.length;
}
