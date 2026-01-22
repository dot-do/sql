/**
 * SQLite Type Affinity Tests
 *
 * Tests for SQLite type affinity rules as defined in https://www.sqlite.org/datatype3.html
 *
 * SQLite uses a dynamic type system where the type of a value is associated with
 * the value itself, not with its container. Column affinity determines how values
 * are coerced when stored and compared.
 */

import { describe, it, expect } from 'vitest';
import {
  type Affinity,
  type StorageClass,
  type StorageValue,
  determineAffinity,
  determineStorageClass,
  coerceValue,
  compareWithAffinity,
  applyAffinityForComparison,
} from './affinity.js';

// =============================================================================
// AFFINITY DETERMINATION TESTS
// =============================================================================

describe('determineAffinity', () => {
  describe('INTEGER affinity (Rule 1: contains "INT")', () => {
    it('should return INTEGER for INT', () => {
      expect(determineAffinity('INT')).toBe('INTEGER');
    });

    it('should return INTEGER for INTEGER', () => {
      expect(determineAffinity('INTEGER')).toBe('INTEGER');
    });

    it('should return INTEGER for TINYINT', () => {
      expect(determineAffinity('TINYINT')).toBe('INTEGER');
    });

    it('should return INTEGER for SMALLINT', () => {
      expect(determineAffinity('SMALLINT')).toBe('INTEGER');
    });

    it('should return INTEGER for MEDIUMINT', () => {
      expect(determineAffinity('MEDIUMINT')).toBe('INTEGER');
    });

    it('should return INTEGER for BIGINT', () => {
      expect(determineAffinity('BIGINT')).toBe('INTEGER');
    });

    it('should return INTEGER for UNSIGNED BIG INT', () => {
      expect(determineAffinity('UNSIGNED BIG INT')).toBe('INTEGER');
    });

    it('should return INTEGER for INT2', () => {
      expect(determineAffinity('INT2')).toBe('INTEGER');
    });

    it('should return INTEGER for INT8', () => {
      expect(determineAffinity('INT8')).toBe('INTEGER');
    });

    it('should be case-insensitive for INT', () => {
      expect(determineAffinity('int')).toBe('INTEGER');
      expect(determineAffinity('Int')).toBe('INTEGER');
      expect(determineAffinity('iNt')).toBe('INTEGER');
    });

    // Edge case: INT takes precedence over other rules
    it('should return INTEGER for FLOATING POINT (contains INT from POINT)', () => {
      expect(determineAffinity('FLOATING POINT')).toBe('INTEGER');
    });

    it('should return INTEGER for CHARINT (INT takes precedence over CHAR)', () => {
      expect(determineAffinity('CHARINT')).toBe('INTEGER');
    });
  });

  describe('TEXT affinity (Rule 2: contains "CHAR", "CLOB", or "TEXT")', () => {
    it('should return TEXT for TEXT', () => {
      expect(determineAffinity('TEXT')).toBe('TEXT');
    });

    it('should return TEXT for CHAR', () => {
      expect(determineAffinity('CHAR')).toBe('TEXT');
    });

    it('should return TEXT for CHARACTER(20)', () => {
      expect(determineAffinity('CHARACTER(20)')).toBe('TEXT');
    });

    it('should return TEXT for VARCHAR(255)', () => {
      expect(determineAffinity('VARCHAR(255)')).toBe('TEXT');
    });

    it('should return TEXT for VARYING CHARACTER(255)', () => {
      expect(determineAffinity('VARYING CHARACTER(255)')).toBe('TEXT');
    });

    it('should return TEXT for NCHAR(55)', () => {
      expect(determineAffinity('NCHAR(55)')).toBe('TEXT');
    });

    it('should return TEXT for NATIVE CHARACTER(70)', () => {
      expect(determineAffinity('NATIVE CHARACTER(70)')).toBe('TEXT');
    });

    it('should return TEXT for NVARCHAR(100)', () => {
      expect(determineAffinity('NVARCHAR(100)')).toBe('TEXT');
    });

    it('should return TEXT for CLOB', () => {
      expect(determineAffinity('CLOB')).toBe('TEXT');
    });

    it('should be case-insensitive for TEXT', () => {
      expect(determineAffinity('text')).toBe('TEXT');
      expect(determineAffinity('Text')).toBe('TEXT');
      expect(determineAffinity('tExT')).toBe('TEXT');
    });

    it('should be case-insensitive for CHAR', () => {
      expect(determineAffinity('char')).toBe('TEXT');
      expect(determineAffinity('Char')).toBe('TEXT');
      expect(determineAffinity('varchar(50)')).toBe('TEXT');
    });
  });

  describe('BLOB affinity (Rule 3: contains "BLOB" or no type specified)', () => {
    it('should return BLOB for BLOB', () => {
      expect(determineAffinity('BLOB')).toBe('BLOB');
    });

    it('should return BLOB for empty string (no type)', () => {
      expect(determineAffinity('')).toBe('BLOB');
    });

    it('should return BLOB for undefined/null type', () => {
      expect(determineAffinity(undefined as unknown as string)).toBe('BLOB');
      expect(determineAffinity(null as unknown as string)).toBe('BLOB');
    });

    it('should be case-insensitive for BLOB', () => {
      expect(determineAffinity('blob')).toBe('BLOB');
      expect(determineAffinity('Blob')).toBe('BLOB');
      expect(determineAffinity('bLoB')).toBe('BLOB');
    });
  });

  describe('REAL affinity (Rule 4: contains "REAL", "FLOA", or "DOUB")', () => {
    it('should return REAL for REAL', () => {
      expect(determineAffinity('REAL')).toBe('REAL');
    });

    it('should return REAL for DOUBLE', () => {
      expect(determineAffinity('DOUBLE')).toBe('REAL');
    });

    it('should return REAL for DOUBLE PRECISION', () => {
      expect(determineAffinity('DOUBLE PRECISION')).toBe('REAL');
    });

    it('should return REAL for FLOAT', () => {
      expect(determineAffinity('FLOAT')).toBe('REAL');
    });

    it('should be case-insensitive for REAL types', () => {
      expect(determineAffinity('real')).toBe('REAL');
      expect(determineAffinity('double')).toBe('REAL');
      expect(determineAffinity('float')).toBe('REAL');
    });
  });

  describe('NUMERIC affinity (Rule 5: default fallback)', () => {
    it('should return NUMERIC for NUMERIC', () => {
      expect(determineAffinity('NUMERIC')).toBe('NUMERIC');
    });

    it('should return NUMERIC for DECIMAL(10,5)', () => {
      expect(determineAffinity('DECIMAL(10,5)')).toBe('NUMERIC');
    });

    it('should return NUMERIC for BOOLEAN', () => {
      expect(determineAffinity('BOOLEAN')).toBe('NUMERIC');
    });

    it('should return NUMERIC for DATE', () => {
      expect(determineAffinity('DATE')).toBe('NUMERIC');
    });

    it('should return NUMERIC for DATETIME', () => {
      expect(determineAffinity('DATETIME')).toBe('NUMERIC');
    });

    // STRING does not match any of rules 1-4
    it('should return NUMERIC for STRING', () => {
      expect(determineAffinity('STRING')).toBe('NUMERIC');
    });

    it('should return NUMERIC for unknown types', () => {
      expect(determineAffinity('CUSTOM_TYPE')).toBe('NUMERIC');
      expect(determineAffinity('MONEY')).toBe('NUMERIC');
    });

    it('should be case-insensitive for NUMERIC', () => {
      expect(determineAffinity('numeric')).toBe('NUMERIC');
      expect(determineAffinity('decimal(10,2)')).toBe('NUMERIC');
    });
  });
});

// =============================================================================
// STORAGE CLASS DETERMINATION TESTS
// =============================================================================

describe('determineStorageClass', () => {
  it('should return NULL for null values', () => {
    expect(determineStorageClass(null)).toBe('NULL');
  });

  it('should return NULL for undefined values', () => {
    expect(determineStorageClass(undefined)).toBe('NULL');
  });

  it('should return INTEGER for integers', () => {
    expect(determineStorageClass(0)).toBe('INTEGER');
    expect(determineStorageClass(1)).toBe('INTEGER');
    expect(determineStorageClass(-1)).toBe('INTEGER');
    expect(determineStorageClass(42)).toBe('INTEGER');
    expect(determineStorageClass(Number.MAX_SAFE_INTEGER)).toBe('INTEGER');
    expect(determineStorageClass(Number.MIN_SAFE_INTEGER)).toBe('INTEGER');
  });

  it('should return REAL for floating point numbers', () => {
    expect(determineStorageClass(3.14)).toBe('REAL');
    expect(determineStorageClass(-0.5)).toBe('REAL');
    expect(determineStorageClass(0.1)).toBe('REAL');
    // Note: 1.0e10 is actually an integer (10000000000)
    expect(determineStorageClass(1.0e10)).toBe('INTEGER');
  });

  it('should return TEXT for strings', () => {
    expect(determineStorageClass('')).toBe('TEXT');
    expect(determineStorageClass('hello')).toBe('TEXT');
    expect(determineStorageClass('123')).toBe('TEXT');
    expect(determineStorageClass('3.14')).toBe('TEXT');
  });

  it('should return BLOB for ArrayBuffer and TypedArrays', () => {
    expect(determineStorageClass(new ArrayBuffer(10))).toBe('BLOB');
    expect(determineStorageClass(new Uint8Array([1, 2, 3]))).toBe('BLOB');
    expect(determineStorageClass(new Int8Array([1, 2, 3]))).toBe('BLOB');
  });

  it('should return INTEGER for booleans (treated as 0/1)', () => {
    expect(determineStorageClass(true)).toBe('INTEGER');
    expect(determineStorageClass(false)).toBe('INTEGER');
  });

  it('should return INTEGER for BigInt within safe integer range', () => {
    expect(determineStorageClass(BigInt(42))).toBe('INTEGER');
    expect(determineStorageClass(BigInt(-100))).toBe('INTEGER');
  });
});

// =============================================================================
// VALUE COERCION TESTS (INSERT/UPDATE)
// =============================================================================

describe('coerceValue', () => {
  describe('TEXT affinity coercion', () => {
    it('should convert integer to text', () => {
      const result = coerceValue(500, 'TEXT');
      expect(result.value).toBe('500');
      expect(result.storageClass).toBe('TEXT');
    });

    it('should convert float to text', () => {
      const result = coerceValue(500.5, 'TEXT');
      expect(result.value).toBe('500.5');
      expect(result.storageClass).toBe('TEXT');
    });

    it('should keep text as text', () => {
      const result = coerceValue('hello', 'TEXT');
      expect(result.value).toBe('hello');
      expect(result.storageClass).toBe('TEXT');
    });

    it('should keep NULL as NULL', () => {
      const result = coerceValue(null, 'TEXT');
      expect(result.value).toBe(null);
      expect(result.storageClass).toBe('NULL');
    });

    it('should keep BLOB as BLOB', () => {
      const blob = new Uint8Array([1, 2, 3]);
      const result = coerceValue(blob, 'TEXT');
      expect(result.value).toBe(blob);
      expect(result.storageClass).toBe('BLOB');
    });
  });

  describe('NUMERIC affinity coercion', () => {
    it('should convert valid integer string to integer', () => {
      const result = coerceValue('500', 'NUMERIC');
      expect(result.value).toBe(500);
      expect(result.storageClass).toBe('INTEGER');
    });

    it('should convert integer-like float string to integer', () => {
      const result = coerceValue('500.0', 'NUMERIC');
      expect(result.value).toBe(500);
      expect(result.storageClass).toBe('INTEGER');
    });

    it('should convert float string to real', () => {
      const result = coerceValue('500.5', 'NUMERIC');
      expect(result.value).toBe(500.5);
      expect(result.storageClass).toBe('REAL');
    });

    it('should keep non-numeric string as text', () => {
      const result = coerceValue('hello', 'NUMERIC');
      expect(result.value).toBe('hello');
      expect(result.storageClass).toBe('TEXT');
    });

    it('should keep hexadecimal strings as text', () => {
      const result = coerceValue('0x1F', 'NUMERIC');
      expect(result.value).toBe('0x1F');
      expect(result.storageClass).toBe('TEXT');
    });

    it('should convert float that can be integer to integer', () => {
      const result = coerceValue(500.0, 'NUMERIC');
      expect(result.value).toBe(500);
      expect(result.storageClass).toBe('INTEGER');
    });

    it('should keep proper float as real', () => {
      const result = coerceValue(500.5, 'NUMERIC');
      expect(result.value).toBe(500.5);
      expect(result.storageClass).toBe('REAL');
    });

    it('should keep integer as integer', () => {
      const result = coerceValue(500, 'NUMERIC');
      expect(result.value).toBe(500);
      expect(result.storageClass).toBe('INTEGER');
    });

    it('should keep NULL as NULL', () => {
      const result = coerceValue(null, 'NUMERIC');
      expect(result.value).toBe(null);
      expect(result.storageClass).toBe('NULL');
    });

    it('should keep BLOB as BLOB', () => {
      const blob = new Uint8Array([1, 2, 3]);
      const result = coerceValue(blob, 'NUMERIC');
      expect(result.value).toBe(blob);
      expect(result.storageClass).toBe('BLOB');
    });

    it('should convert very large integer string to real', () => {
      // Number larger than MAX_SAFE_INTEGER
      const result = coerceValue('9007199254740993', 'NUMERIC');
      expect(result.storageClass).toBe('REAL');
      expect(typeof result.value).toBe('number');
    });
  });

  describe('INTEGER affinity coercion', () => {
    it('should behave like NUMERIC for integer string', () => {
      const result = coerceValue('42', 'INTEGER');
      expect(result.value).toBe(42);
      expect(result.storageClass).toBe('INTEGER');
    });

    it('should behave like NUMERIC for float string', () => {
      const result = coerceValue('42.5', 'INTEGER');
      expect(result.value).toBe(42.5);
      expect(result.storageClass).toBe('REAL');
    });

    it('should convert float that can be integer to integer', () => {
      const result = coerceValue(42.0, 'INTEGER');
      expect(result.value).toBe(42);
      expect(result.storageClass).toBe('INTEGER');
    });
  });

  describe('REAL affinity coercion', () => {
    it('should force integer to real', () => {
      const result = coerceValue(500, 'REAL');
      expect(result.value).toBe(500.0);
      expect(result.storageClass).toBe('REAL');
    });

    it('should keep float as real', () => {
      const result = coerceValue(500.5, 'REAL');
      expect(result.value).toBe(500.5);
      expect(result.storageClass).toBe('REAL');
    });

    it('should convert integer string to real', () => {
      const result = coerceValue('500', 'REAL');
      expect(result.value).toBe(500.0);
      expect(result.storageClass).toBe('REAL');
    });

    it('should convert float string to real', () => {
      const result = coerceValue('500.5', 'REAL');
      expect(result.value).toBe(500.5);
      expect(result.storageClass).toBe('REAL');
    });

    it('should keep non-numeric string as text', () => {
      const result = coerceValue('hello', 'REAL');
      expect(result.value).toBe('hello');
      expect(result.storageClass).toBe('TEXT');
    });

    it('should keep NULL as NULL', () => {
      const result = coerceValue(null, 'REAL');
      expect(result.value).toBe(null);
      expect(result.storageClass).toBe('NULL');
    });

    it('should keep BLOB as BLOB', () => {
      const blob = new Uint8Array([1, 2, 3]);
      const result = coerceValue(blob, 'REAL');
      expect(result.value).toBe(blob);
      expect(result.storageClass).toBe('BLOB');
    });
  });

  describe('BLOB affinity coercion', () => {
    it('should keep integer as integer (no conversion)', () => {
      const result = coerceValue(500, 'BLOB');
      expect(result.value).toBe(500);
      expect(result.storageClass).toBe('INTEGER');
    });

    it('should keep float as real (no conversion)', () => {
      const result = coerceValue(500.5, 'BLOB');
      expect(result.value).toBe(500.5);
      expect(result.storageClass).toBe('REAL');
    });

    it('should keep text as text (no conversion)', () => {
      const result = coerceValue('hello', 'BLOB');
      expect(result.value).toBe('hello');
      expect(result.storageClass).toBe('TEXT');
    });

    it('should keep BLOB as BLOB', () => {
      const blob = new Uint8Array([1, 2, 3]);
      const result = coerceValue(blob, 'BLOB');
      expect(result.value).toBe(blob);
      expect(result.storageClass).toBe('BLOB');
    });

    it('should keep NULL as NULL', () => {
      const result = coerceValue(null, 'BLOB');
      expect(result.value).toBe(null);
      expect(result.storageClass).toBe('NULL');
    });
  });

  describe('boolean coercion', () => {
    it('should convert true to 1 for NUMERIC', () => {
      const result = coerceValue(true, 'NUMERIC');
      expect(result.value).toBe(1);
      expect(result.storageClass).toBe('INTEGER');
    });

    it('should convert false to 0 for NUMERIC', () => {
      const result = coerceValue(false, 'NUMERIC');
      expect(result.value).toBe(0);
      expect(result.storageClass).toBe('INTEGER');
    });

    it('should convert true to "1" for TEXT', () => {
      const result = coerceValue(true, 'TEXT');
      expect(result.value).toBe('1');
      expect(result.storageClass).toBe('TEXT');
    });

    it('should convert false to "0" for TEXT', () => {
      const result = coerceValue(false, 'TEXT');
      expect(result.value).toBe('0');
      expect(result.storageClass).toBe('TEXT');
    });
  });
});

// =============================================================================
// COMPARISON AFFINITY APPLICATION TESTS
// =============================================================================

describe('applyAffinityForComparison', () => {
  describe('NUMERIC/INTEGER/REAL affinity vs TEXT/BLOB/none', () => {
    it('should apply NUMERIC to text when comparing with integer', () => {
      const [left, right] = applyAffinityForComparison(
        { value: 500, affinity: 'INTEGER' },
        { value: '500', affinity: null }
      );
      expect(right.value).toBe(500);
      expect(right.storageClass).toBe('INTEGER');
    });

    it('should apply NUMERIC to text when comparing with real', () => {
      const [left, right] = applyAffinityForComparison(
        { value: 500.5, affinity: 'REAL' },
        { value: '500.5', affinity: null }
      );
      expect(right.value).toBe(500.5);
      expect(right.storageClass).toBe('REAL');
    });

    it('should apply NUMERIC to text when comparing with numeric', () => {
      const [left, right] = applyAffinityForComparison(
        { value: 500, affinity: 'NUMERIC' },
        { value: '500', affinity: null }
      );
      expect(right.value).toBe(500);
      expect(right.storageClass).toBe('INTEGER');
    });
  });

  describe('TEXT affinity vs none', () => {
    it('should apply TEXT to non-affinity operand when comparing with text', () => {
      const [left, right] = applyAffinityForComparison(
        { value: 'hello', affinity: 'TEXT' },
        { value: 123, affinity: null }
      );
      expect(right.value).toBe('123');
      expect(right.storageClass).toBe('TEXT');
    });
  });

  describe('no affinity applied', () => {
    it('should not apply affinity when both have TEXT', () => {
      const [left, right] = applyAffinityForComparison(
        { value: 'hello', affinity: 'TEXT' },
        { value: 'world', affinity: 'TEXT' }
      );
      expect(left.value).toBe('hello');
      expect(right.value).toBe('world');
    });

    it('should not apply affinity when both have NUMERIC', () => {
      const [left, right] = applyAffinityForComparison(
        { value: 100, affinity: 'NUMERIC' },
        { value: 200, affinity: 'NUMERIC' }
      );
      expect(left.value).toBe(100);
      expect(right.value).toBe(200);
    });
  });
});

// =============================================================================
// COMPARISON TESTS
// =============================================================================

describe('compareWithAffinity', () => {
  describe('NULL comparisons', () => {
    it('should consider NULL less than any other value', () => {
      expect(compareWithAffinity(null, 0)).toBeLessThan(0);
      expect(compareWithAffinity(null, '')).toBeLessThan(0);
      expect(compareWithAffinity(null, 'text')).toBeLessThan(0);
    });

    it('should consider two NULLs equal for sorting purposes', () => {
      expect(compareWithAffinity(null, null)).toBe(0);
    });
  });

  describe('INTEGER/REAL comparisons', () => {
    it('should compare integers numerically', () => {
      expect(compareWithAffinity(1, 2)).toBeLessThan(0);
      expect(compareWithAffinity(2, 1)).toBeGreaterThan(0);
      expect(compareWithAffinity(1, 1)).toBe(0);
    });

    it('should compare reals numerically', () => {
      expect(compareWithAffinity(1.5, 2.5)).toBeLessThan(0);
      expect(compareWithAffinity(2.5, 1.5)).toBeGreaterThan(0);
      expect(compareWithAffinity(1.5, 1.5)).toBe(0);
    });

    it('should compare integer and real numerically', () => {
      expect(compareWithAffinity(1, 1.5)).toBeLessThan(0);
      expect(compareWithAffinity(2, 1.5)).toBeGreaterThan(0);
      expect(compareWithAffinity(1, 1.0)).toBe(0);
    });

    it('should consider numbers less than text', () => {
      expect(compareWithAffinity(100, 'a')).toBeLessThan(0);
      expect(compareWithAffinity(100.5, 'a')).toBeLessThan(0);
    });
  });

  describe('TEXT comparisons', () => {
    it('should compare text lexicographically', () => {
      expect(compareWithAffinity('a', 'b')).toBeLessThan(0);
      expect(compareWithAffinity('b', 'a')).toBeGreaterThan(0);
      expect(compareWithAffinity('a', 'a')).toBe(0);
    });

    it('should consider text less than blob', () => {
      const blob = new Uint8Array([1, 2, 3]);
      expect(compareWithAffinity('text', blob)).toBeLessThan(0);
    });
  });

  describe('BLOB comparisons', () => {
    it('should compare blobs using memcmp-like comparison', () => {
      const blob1 = new Uint8Array([1, 2, 3]);
      const blob2 = new Uint8Array([1, 2, 4]);
      const blob3 = new Uint8Array([1, 2, 3]);
      expect(compareWithAffinity(blob1, blob2)).toBeLessThan(0);
      expect(compareWithAffinity(blob2, blob1)).toBeGreaterThan(0);
      expect(compareWithAffinity(blob1, blob3)).toBe(0);
    });

    it('should compare blobs of different lengths', () => {
      const blob1 = new Uint8Array([1, 2, 3]);
      const blob2 = new Uint8Array([1, 2, 3, 4]);
      expect(compareWithAffinity(blob1, blob2)).toBeLessThan(0);
      expect(compareWithAffinity(blob2, blob1)).toBeGreaterThan(0);
    });
  });

  describe('storage class ordering', () => {
    it('should order: NULL < INTEGER < REAL < TEXT < BLOB', () => {
      const nullVal = null;
      const intVal = 100;
      const realVal = 100.5;
      const textVal = 'text';
      const blobVal = new Uint8Array([1, 2, 3]);

      expect(compareWithAffinity(nullVal, intVal)).toBeLessThan(0);
      expect(compareWithAffinity(intVal, realVal)).toBeLessThan(0);
      expect(compareWithAffinity(realVal, textVal)).toBeLessThan(0);
      expect(compareWithAffinity(textVal, blobVal)).toBeLessThan(0);
    });
  });
});

// =============================================================================
// EDGE CASES AND SPECIAL BEHAVIOR TESTS
// =============================================================================

describe('edge cases', () => {
  describe('scientific notation', () => {
    it('should parse scientific notation as numeric', () => {
      // 1.5e10 = 15000000000 which is an integer
      const result = coerceValue('1.5e10', 'NUMERIC');
      expect(result.value).toBe(1.5e10);
      // This is actually an integer since 1.5e10 = 15000000000
      expect(result.storageClass).toBe('INTEGER');
    });

    it('should parse scientific notation with decimal result as real', () => {
      const result = coerceValue('1.5e-1', 'NUMERIC');
      expect(result.value).toBe(0.15);
      expect(result.storageClass).toBe('REAL');
    });

    it('should parse negative scientific notation', () => {
      const result = coerceValue('-1.5e-10', 'NUMERIC');
      expect(result.value).toBe(-1.5e-10);
      expect(result.storageClass).toBe('REAL');
    });
  });

  describe('leading/trailing whitespace', () => {
    it('should handle leading whitespace in numeric strings', () => {
      const result = coerceValue('  42', 'NUMERIC');
      expect(result.value).toBe(42);
      expect(result.storageClass).toBe('INTEGER');
    });

    it('should handle trailing whitespace in numeric strings', () => {
      const result = coerceValue('42  ', 'NUMERIC');
      expect(result.value).toBe(42);
      expect(result.storageClass).toBe('INTEGER');
    });
  });

  describe('special float values', () => {
    it('should handle Infinity', () => {
      const result = coerceValue(Infinity, 'NUMERIC');
      expect(result.value).toBe(Infinity);
      expect(result.storageClass).toBe('REAL');
    });

    it('should handle -Infinity', () => {
      const result = coerceValue(-Infinity, 'NUMERIC');
      expect(result.value).toBe(-Infinity);
      expect(result.storageClass).toBe('REAL');
    });

    it('should handle NaN', () => {
      const result = coerceValue(NaN, 'NUMERIC');
      expect(Number.isNaN(result.value)).toBe(true);
      expect(result.storageClass).toBe('REAL');
    });
  });

  describe('negative zero', () => {
    it('should treat -0 as integer 0', () => {
      const result = coerceValue(-0, 'NUMERIC');
      expect(result.value).toBe(0);
      expect(result.storageClass).toBe('INTEGER');
    });
  });

  describe('empty and whitespace-only strings', () => {
    it('should keep empty string as text for NUMERIC', () => {
      const result = coerceValue('', 'NUMERIC');
      expect(result.value).toBe('');
      expect(result.storageClass).toBe('TEXT');
    });

    it('should keep whitespace-only string as text for NUMERIC', () => {
      const result = coerceValue('   ', 'NUMERIC');
      expect(result.value).toBe('   ');
      expect(result.storageClass).toBe('TEXT');
    });
  });

  describe('number precision', () => {
    it('should preserve up to 15 significant digits', () => {
      // 15 digits should be preserved
      const result = coerceValue('123456789012345', 'NUMERIC');
      expect(result.value).toBe(123456789012345);
      expect(result.storageClass).toBe('INTEGER');
    });
  });

  describe('negative numbers', () => {
    it('should handle negative integer strings', () => {
      const result = coerceValue('-42', 'NUMERIC');
      expect(result.value).toBe(-42);
      expect(result.storageClass).toBe('INTEGER');
    });

    it('should handle negative float strings', () => {
      const result = coerceValue('-42.5', 'NUMERIC');
      expect(result.value).toBe(-42.5);
      expect(result.storageClass).toBe('REAL');
    });
  });

  describe('plus sign prefix', () => {
    it('should handle plus sign in numeric strings', () => {
      const result = coerceValue('+42', 'NUMERIC');
      expect(result.value).toBe(42);
      expect(result.storageClass).toBe('INTEGER');
    });
  });

  describe('mixed valid/invalid strings', () => {
    it('should keep string with trailing text as text', () => {
      const result = coerceValue('42abc', 'NUMERIC');
      expect(result.value).toBe('42abc');
      expect(result.storageClass).toBe('TEXT');
    });

    it('should keep string with leading text as text', () => {
      const result = coerceValue('abc42', 'NUMERIC');
      expect(result.value).toBe('abc42');
      expect(result.storageClass).toBe('TEXT');
    });
  });
});

// =============================================================================
// TYPE EXPORTS VALIDATION
// =============================================================================

describe('type exports', () => {
  it('should export Affinity type with correct values', () => {
    const affinities: Affinity[] = ['TEXT', 'NUMERIC', 'INTEGER', 'REAL', 'BLOB'];
    expect(affinities).toHaveLength(5);
  });

  it('should export StorageClass type with correct values', () => {
    const classes: StorageClass[] = ['NULL', 'INTEGER', 'REAL', 'TEXT', 'BLOB'];
    expect(classes).toHaveLength(5);
  });
});
