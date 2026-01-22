/**
 * Comprehensive Tests for SQLite Built-in Functions
 *
 * Tests all function categories:
 * - String functions
 * - Math functions
 * - Date/time functions
 * - Aggregate functions
 * - JSON functions
 * - Function registry
 */

import { describe, it, expect, beforeEach } from 'vitest';

// String functions
import {
  length, substr, upper, lower, trim, ltrim, rtrim,
  replace, instr, printf, quote, hex, unhex, zeroblob,
  concat, concat_ws, char_fn, unicode, like, glob,
} from './string.js';

// Math functions
import {
  abs, round, random, max_scalar, min_scalar, sign, pow, sqrt,
  ceil, floor, trunc, mod, log, log10, log2, exp,
  sin, cos, tan, asin, acos, atan, atan2, pi, degrees, radians,
  nullif, ifnull, coalesce, iif,
} from './math.js';

// Date functions
import {
  date, time, datetime, julianday, strftime, unixepoch,
  current_date, current_time, current_timestamp, timediff,
} from './date.js';

// Aggregate functions
import {
  aggregateFactories, executeAggregate, isAggregateFunction,
  createGroupConcat, createGroupConcatDistinct,
} from './aggregate.js';

// JSON functions
import {
  json, json_valid, json_extract, json_type, json_array,
  json_object, json_array_length, json_insert, json_replace,
  json_set, json_remove, json_patch, json_quote, json_each,
} from './json.js';

// Registry
import {
  FunctionRegistry, defaultRegistry, invokeFunction, hasFunction, isAggregate,
} from './registry.js';

// =============================================================================
// STRING FUNCTION TESTS
// =============================================================================

describe('String Functions', () => {
  describe('length()', () => {
    it('returns length of string', () => {
      expect(length('hello')).toBe(5);
      expect(length('')).toBe(0);
      expect(length('日本語')).toBe(3);
    });

    it('returns null for null input', () => {
      expect(length(null)).toBe(null);
    });

    it('handles blobs', () => {
      expect(length(new Uint8Array([1, 2, 3]))).toBe(3);
    });
  });

  describe('substr()', () => {
    it('extracts substring with 1-indexed position', () => {
      expect(substr('hello', 1, 3)).toBe('hel');
      expect(substr('hello', 2, 3)).toBe('ell');
      expect(substr('hello', 1)).toBe('hello');
    });

    it('handles negative indices', () => {
      expect(substr('hello', -2, 2)).toBe('lo');
      expect(substr('hello', -3)).toBe('llo');
    });

    it('returns null for null inputs', () => {
      expect(substr(null, 1, 3)).toBe(null);
      expect(substr('hello', null, 3)).toBe(null);
    });
  });

  describe('upper() and lower()', () => {
    it('converts case', () => {
      expect(upper('hello')).toBe('HELLO');
      expect(lower('HELLO')).toBe('hello');
      expect(upper('HeLLo')).toBe('HELLO');
    });

    it('handles null', () => {
      expect(upper(null)).toBe(null);
      expect(lower(null)).toBe(null);
    });
  });

  describe('trim(), ltrim(), rtrim()', () => {
    it('trims whitespace by default', () => {
      expect(trim('  hello  ')).toBe('hello');
      expect(ltrim('  hello  ')).toBe('hello  ');
      expect(rtrim('  hello  ')).toBe('  hello');
    });

    it('trims specified characters', () => {
      expect(trim('xxhelloxx', 'x')).toBe('hello');
      expect(ltrim('xxhello', 'x')).toBe('hello');
      expect(rtrim('helloxx', 'x')).toBe('hello');
      expect(trim('abchelloabc', 'abc')).toBe('hello');
    });

    it('handles null', () => {
      expect(trim(null)).toBe(null);
    });
  });

  describe('replace()', () => {
    it('replaces all occurrences', () => {
      expect(replace('hello world', 'o', '0')).toBe('hell0 w0rld');
      expect(replace('aaa', 'a', 'b')).toBe('bbb');
    });

    it('handles null', () => {
      expect(replace(null, 'a', 'b')).toBe(null);
      expect(replace('hello', null, 'b')).toBe(null);
      expect(replace('hello', 'a', null)).toBe(null);
    });
  });

  describe('instr()', () => {
    it('returns 1-indexed position', () => {
      expect(instr('hello', 'l')).toBe(3);
      expect(instr('hello', 'lo')).toBe(4);
      expect(instr('hello', 'x')).toBe(0);
    });

    it('handles null', () => {
      expect(instr(null, 'a')).toBe(null);
      expect(instr('hello', null)).toBe(null);
    });
  });

  describe('printf()', () => {
    it('formats strings', () => {
      expect(printf('%s', 'hello')).toBe('hello');
      expect(printf('Hello, %s!', 'world')).toBe('Hello, world!');
    });

    it('formats integers', () => {
      expect(printf('%d', 42)).toBe('42');
      expect(printf('%5d', 42)).toBe('   42');
    });

    it('formats floats', () => {
      expect(printf('%.2f', 3.14159)).toBe('3.14');
      expect(printf('%8.2f', 3.14)).toBe('    3.14');
    });

    it('handles literal %', () => {
      expect(printf('100%%')).toBe('100%');
    });
  });

  describe('quote()', () => {
    it('quotes strings with escaping', () => {
      expect(quote('hello')).toBe("'hello'");
      expect(quote("it's")).toBe("'it''s'");
    });

    it('handles null', () => {
      expect(quote(null)).toBe('NULL');
    });

    it('handles numbers', () => {
      expect(quote(42)).toBe('42');
      expect(quote(3.14)).toBe('3.14');
    });
  });

  describe('hex() and unhex()', () => {
    it('converts to/from hex', () => {
      expect(hex('AB')).toBe('4142');
      expect(unhex('4142')).toEqual(new Uint8Array([0x41, 0x42]));
    });

    it('handles blobs', () => {
      expect(hex(new Uint8Array([0xDE, 0xAD, 0xBE, 0xEF]))).toBe('DEADBEEF');
    });

    it('handles invalid hex', () => {
      expect(unhex('xyz')).toBe(null);
      expect(unhex('xyz', 'default')).toBe('default');
    });
  });

  describe('zeroblob()', () => {
    it('creates zero-filled blob', () => {
      const blob = zeroblob(5);
      expect(blob).toEqual(new Uint8Array([0, 0, 0, 0, 0]));
    });
  });

  describe('concat() and concat_ws()', () => {
    it('concatenates strings', () => {
      expect(concat('a', 'b', 'c')).toBe('abc');
      expect(concat_ws(',', 'a', 'b', 'c')).toBe('a,b,c');
    });

    it('handles null in concat', () => {
      expect(concat('a', null, 'c')).toBe('ac');
    });

    it('skips null in concat_ws', () => {
      expect(concat_ws(',', 'a', null, 'c')).toBe('a,c');
    });
  });

  describe('like() and glob()', () => {
    it('matches LIKE patterns', () => {
      expect(like('hello', 'h%')).toBe(true);
      expect(like('hello', '%llo')).toBe(true);
      expect(like('hello', 'h_llo')).toBe(true);
      expect(like('hello', 'world')).toBe(false);
    });

    it('matches glob patterns', () => {
      expect(glob('h*', 'hello')).toBe(true);
      expect(glob('h?llo', 'hello')).toBe(true);
      expect(glob('[a-z]*', 'hello')).toBe(true);
    });
  });
});

// =============================================================================
// MATH FUNCTION TESTS
// =============================================================================

describe('Math Functions', () => {
  describe('abs()', () => {
    it('returns absolute value', () => {
      expect(abs(-5)).toBe(5);
      expect(abs(5)).toBe(5);
      expect(abs(-5n)).toBe(5n);
    });

    it('handles null', () => {
      expect(abs(null)).toBe(null);
    });
  });

  describe('round()', () => {
    it('rounds to specified precision', () => {
      expect(round(3.14159, 2)).toBe(3.14);
      expect(round(3.5)).toBe(4);
      expect(round(2.5)).toBe(3); // JS rounding
    });

    it('handles null', () => {
      expect(round(null)).toBe(null);
    });
  });

  describe('random()', () => {
    it('returns a number', () => {
      const r = random();
      expect(typeof r).toBe('number');
    });
  });

  describe('max_scalar() and min_scalar()', () => {
    it('returns max/min of multiple values', () => {
      expect(max_scalar(1, 5, 3)).toBe(5);
      expect(min_scalar(1, 5, 3)).toBe(1);
    });

    it('compares strings', () => {
      expect(max_scalar('a', 'c', 'b')).toBe('c');
      expect(min_scalar('a', 'c', 'b')).toBe('a');
    });

    it('ignores null values', () => {
      expect(max_scalar(null, 5, 3)).toBe(5);
      expect(min_scalar(1, null, 3)).toBe(1);
    });
  });

  describe('sign()', () => {
    it('returns sign of number', () => {
      expect(sign(5)).toBe(1);
      expect(sign(-5)).toBe(-1);
      expect(sign(0)).toBe(0);
    });
  });

  describe('pow() and sqrt()', () => {
    it('calculates power', () => {
      expect(pow(2, 3)).toBe(8);
      expect(pow(4, 0.5)).toBe(2);
    });

    it('calculates square root', () => {
      expect(sqrt(16)).toBe(4);
      expect(sqrt(-1)).toBe(null);
    });
  });

  describe('ceil(), floor(), trunc()', () => {
    it('rounds correctly', () => {
      expect(ceil(3.2)).toBe(4);
      expect(ceil(-3.2)).toBe(-3);
      expect(floor(3.8)).toBe(3);
      expect(floor(-3.2)).toBe(-4);
      expect(trunc(3.8)).toBe(3);
      expect(trunc(-3.8)).toBe(-3);
    });
  });

  describe('mod()', () => {
    it('calculates modulo', () => {
      expect(mod(10, 3)).toBe(1);
      expect(mod(10n, 3n)).toBe(1n);
    });

    it('handles division by zero', () => {
      expect(mod(10, 0)).toBe(null);
    });
  });

  describe('log functions', () => {
    it('calculates logarithms', () => {
      expect(log(Math.E)).toBeCloseTo(1);
      expect(log10(100)).toBeCloseTo(2);
      expect(log2(8)).toBeCloseTo(3);
    });

    it('returns null for non-positive', () => {
      expect(log(0)).toBe(null);
      expect(log(-1)).toBe(null);
    });
  });

  describe('trig functions', () => {
    it('calculates sine, cosine, tangent', () => {
      expect(sin(0)).toBeCloseTo(0);
      expect(cos(0)).toBeCloseTo(1);
      expect(tan(0)).toBeCloseTo(0);
    });

    it('calculates inverse trig', () => {
      expect(asin(0)).toBeCloseTo(0);
      expect(acos(1)).toBeCloseTo(0);
      expect(atan(0)).toBeCloseTo(0);
    });

    it('handles atan2', () => {
      expect(atan2(1, 1)).toBeCloseTo(Math.PI / 4);
    });
  });

  describe('pi() and angle conversion', () => {
    it('returns pi', () => {
      expect(pi()).toBeCloseTo(Math.PI);
    });

    it('converts angles', () => {
      expect(degrees(Math.PI)).toBeCloseTo(180);
      expect(radians(180)).toBeCloseTo(Math.PI);
    });
  });

  describe('null handling functions', () => {
    it('nullif returns null if equal', () => {
      expect(nullif(1, 1)).toBe(null);
      expect(nullif(1, 2)).toBe(1);
    });

    it('ifnull returns first non-null', () => {
      expect(ifnull(null, 5)).toBe(5);
      expect(ifnull(3, 5)).toBe(3);
    });

    it('coalesce returns first non-null', () => {
      expect(coalesce(null, null, 3, 4)).toBe(3);
      expect(coalesce(1, 2, 3)).toBe(1);
    });

    it('iif returns based on condition', () => {
      expect(iif(true, 'yes', 'no')).toBe('yes');
      expect(iif(false, 'yes', 'no')).toBe('no');
      expect(iif(0, 'yes', 'no')).toBe('no');
    });
  });
});

// =============================================================================
// DATE FUNCTION TESTS
// =============================================================================

describe('Date Functions', () => {
  describe('date()', () => {
    it('parses date strings', () => {
      expect(date('2024-01-15')).toBe('2024-01-15');
      expect(date('2024-01-15 12:30:00')).toBe('2024-01-15');
    });

    it('handles modifiers', () => {
      expect(date('2024-01-15', '+1 day')).toBe('2024-01-16');
      expect(date('2024-01-15', '-1 month')).toBe('2023-12-15');
      expect(date('2024-01-15', 'start of month')).toBe('2024-01-01');
    });

    it('handles null', () => {
      expect(date(null)).toBe(null);
    });
  });

  describe('time()', () => {
    it('parses time strings', () => {
      expect(time('12:30:45')).toBe('12:30:45');
      expect(time('2024-01-15 12:30:45')).toBe('12:30:45');
    });

    it('handles modifiers', () => {
      expect(time('12:30:00', '+1 hour')).toBe('13:30:00');
      expect(time('12:30:00', '+30 minutes')).toBe('13:00:00');
    });
  });

  describe('datetime()', () => {
    it('parses datetime strings', () => {
      expect(datetime('2024-01-15 12:30:45')).toBe('2024-01-15 12:30:45');
    });

    it('handles modifiers', () => {
      expect(datetime('2024-01-15 12:30:00', '+1 day', '+1 hour')).toBe('2024-01-16 13:30:00');
    });
  });

  describe('julianday()', () => {
    it('calculates Julian day', () => {
      // Known value: Jan 1, 2000 12:00:00 = 2451545.0
      const jd = julianday('2000-01-01 12:00:00') as number;
      expect(jd).toBeCloseTo(2451545.0, 4);
    });
  });

  describe('strftime()', () => {
    it('formats datetime', () => {
      expect(strftime('%Y-%m-%d', '2024-01-15')).toBe('2024-01-15');
      expect(strftime('%H:%M:%S', '2024-01-15 12:30:45')).toBe('12:30:45');
      expect(strftime('%w', '2024-01-15')).toBe('1'); // Monday
    });

    it('handles various format specifiers', () => {
      expect(strftime('%Y', '2024-01-15')).toBe('2024');
      expect(strftime('%m', '2024-01-15')).toBe('01');
      expect(strftime('%d', '2024-01-15')).toBe('15');
      expect(strftime('%%', '2024-01-15')).toBe('%');
    });
  });

  describe('unixepoch()', () => {
    it('returns Unix timestamp', () => {
      const ts = unixepoch('1970-01-01 00:00:00') as number;
      expect(ts).toBe(0);
    });

    it('handles subsec modifier', () => {
      const ts = unixepoch('1970-01-01 00:00:00.123', 'subsec') as number;
      expect(ts).toBeCloseTo(0.123, 3);
    });
  });

  describe('current_* functions', () => {
    it('returns current date/time', () => {
      // Just check format, not exact values
      expect(current_date()).toMatch(/^\d{4}-\d{2}-\d{2}$/);
      expect(current_time()).toMatch(/^\d{2}:\d{2}:\d{2}$/);
      expect(current_timestamp()).toMatch(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/);
    });
  });

  describe('timediff()', () => {
    it('calculates time difference', () => {
      const diff = timediff('2024-01-15 13:00:00', '2024-01-15 12:00:00') as string;
      expect(diff).toBe('01:00:00.000');
    });

    it('handles negative differences', () => {
      const diff = timediff('2024-01-15 12:00:00', '2024-01-15 13:00:00') as string;
      expect(diff).toBe('-01:00:00.000');
    });
  });
});

// =============================================================================
// AGGREGATE FUNCTION TESTS
// =============================================================================

describe('Aggregate Functions', () => {
  describe('count()', () => {
    it('counts non-null values', () => {
      expect(executeAggregate('count', [1, 2, 3, null, 5])).toBe(4);
    });

    it('count(*) counts all rows', () => {
      const acc = aggregateFactories.count_star();
      [1, null, 3].forEach(v => acc.step(v));
      expect(acc.finalize()).toBe(3);
    });

    it('count distinct counts unique values', () => {
      expect(executeAggregate('count', [1, 1, 2, 2, 3], { distinct: true })).toBe(3);
    });
  });

  describe('sum()', () => {
    it('sums values', () => {
      expect(executeAggregate('sum', [1, 2, 3, 4])).toBe(10);
    });

    it('returns null for all nulls', () => {
      expect(executeAggregate('sum', [null, null])).toBe(null);
    });

    it('ignores null values', () => {
      expect(executeAggregate('sum', [1, null, 3])).toBe(4);
    });

    it('handles bigints', () => {
      expect(executeAggregate('sum', [1n, 2n, 3n])).toBe(6n);
    });
  });

  describe('total()', () => {
    it('returns 0.0 for empty/all-null', () => {
      expect(executeAggregate('total', [])).toBe(0);
      expect(executeAggregate('total', [null, null])).toBe(0);
    });

    it('sums values', () => {
      expect(executeAggregate('total', [1, 2, 3])).toBe(6);
    });
  });

  describe('avg()', () => {
    it('calculates average', () => {
      expect(executeAggregate('avg', [2, 4, 6])).toBe(4);
    });

    it('returns null for all nulls', () => {
      expect(executeAggregate('avg', [null, null])).toBe(null);
    });
  });

  describe('min() and max()', () => {
    it('finds min/max', () => {
      expect(executeAggregate('min', [3, 1, 4, 1, 5])).toBe(1);
      expect(executeAggregate('max', [3, 1, 4, 1, 5])).toBe(5);
    });

    it('compares strings', () => {
      expect(executeAggregate('min', ['b', 'a', 'c'])).toBe('a');
      expect(executeAggregate('max', ['b', 'a', 'c'])).toBe('c');
    });

    it('ignores null', () => {
      expect(executeAggregate('min', [null, 3, 1])).toBe(1);
    });
  });

  describe('group_concat()', () => {
    it('concatenates values', () => {
      expect(executeAggregate('group_concat', ['a', 'b', 'c'])).toBe('a,b,c');
    });

    it('uses custom separator', () => {
      const acc = createGroupConcat(' | ');
      ['a', 'b', 'c'].forEach(v => acc.step(v));
      expect(acc.finalize()).toBe('a | b | c');
    });

    it('handles distinct', () => {
      const acc = createGroupConcatDistinct(',');
      ['a', 'b', 'a', 'c'].forEach(v => acc.step(v));
      expect(acc.finalize()).toBe('a,b,c');
    });
  });

  describe('isAggregateFunction()', () => {
    it('identifies aggregate functions', () => {
      expect(isAggregateFunction('count')).toBe(true);
      expect(isAggregateFunction('SUM')).toBe(true);
      expect(isAggregateFunction('upper')).toBe(false);
    });
  });
});

// =============================================================================
// JSON FUNCTION TESTS
// =============================================================================

describe('JSON Functions', () => {
  describe('json()', () => {
    it('validates and minifies JSON', () => {
      expect(json('{ "a" : 1 }')).toBe('{"a":1}');
      expect(json('[1, 2, 3]')).toBe('[1,2,3]');
    });

    it('throws on invalid JSON', () => {
      expect(() => json('invalid')).toThrow();
    });

    it('handles null', () => {
      expect(json(null)).toBe(null);
    });
  });

  describe('json_valid()', () => {
    it('returns 1 for valid JSON', () => {
      expect(json_valid('{"a":1}')).toBe(1);
      expect(json_valid('[1,2,3]')).toBe(1);
    });

    it('returns 0 for invalid JSON', () => {
      expect(json_valid('invalid')).toBe(0);
      expect(json_valid(null)).toBe(0);
    });
  });

  describe('json_extract()', () => {
    it('extracts values at path', () => {
      expect(json_extract('{"a":{"b":1}}', '$.a.b')).toBe(1);
      expect(json_extract('[1,2,3]', '$[1]')).toBe(2);
    });

    it('returns objects/arrays as JSON strings', () => {
      expect(json_extract('{"a":{"b":1}}', '$.a')).toBe('{"b":1}');
    });

    it('returns null for missing path', () => {
      expect(json_extract('{"a":1}', '$.b')).toBe(null);
    });

    it('handles multiple paths', () => {
      expect(json_extract('{"a":1,"b":2}', '$.a', '$.b')).toBe('[1,2]');
    });
  });

  describe('json_type()', () => {
    it('returns type of value', () => {
      expect(json_type('null')).toBe('null');
      expect(json_type('true')).toBe('true');
      expect(json_type('false')).toBe('false');
      expect(json_type('42')).toBe('integer');
      expect(json_type('3.14')).toBe('real');
      expect(json_type('"hello"')).toBe('text');
      expect(json_type('[]')).toBe('array');
      expect(json_type('{}')).toBe('object');
    });

    it('returns type at path', () => {
      expect(json_type('{"a":1}', '$.a')).toBe('integer');
    });
  });

  describe('json_array()', () => {
    it('creates JSON array', () => {
      expect(json_array(1, 'two', 3)).toBe('[1,"two",3]');
      expect(json_array()).toBe('[]');
    });

    it('handles null', () => {
      expect(json_array(1, null, 3)).toBe('[1,null,3]');
    });
  });

  describe('json_object()', () => {
    it('creates JSON object', () => {
      expect(json_object('a', 1, 'b', 2)).toBe('{"a":1,"b":2}');
    });

    it('throws for odd arguments', () => {
      expect(() => json_object('a', 1, 'b')).toThrow();
    });

    it('throws for null key', () => {
      expect(() => json_object(null, 1)).toThrow();
    });
  });

  describe('json_array_length()', () => {
    it('returns array length', () => {
      expect(json_array_length('[1,2,3]')).toBe(3);
      expect(json_array_length('[]')).toBe(0);
    });

    it('returns null for non-array', () => {
      expect(json_array_length('{"a":1}')).toBe(null);
    });

    it('handles path', () => {
      expect(json_array_length('{"a":[1,2,3]}', '$.a')).toBe(3);
    });
  });

  describe('json_insert(), json_replace(), json_set()', () => {
    const obj = '{"a":1}';

    it('json_insert adds only if missing', () => {
      expect(json_insert(obj, '$.b', 2)).toBe('{"a":1,"b":2}');
      expect(json_insert(obj, '$.a', 2)).toBe('{"a":1}'); // Not replaced
    });

    it('json_replace replaces only if exists', () => {
      expect(json_replace(obj, '$.a', 2)).toBe('{"a":2}');
      expect(json_replace(obj, '$.b', 2)).toBe('{"a":1}'); // Not added
    });

    it('json_set inserts or replaces', () => {
      expect(json_set(obj, '$.a', 2)).toBe('{"a":2}');
      expect(json_set(obj, '$.b', 2)).toBe('{"a":1,"b":2}');
    });
  });

  describe('json_remove()', () => {
    it('removes values at paths', () => {
      expect(json_remove('{"a":1,"b":2}', '$.a')).toBe('{"b":2}');
      expect(json_remove('[1,2,3]', '$[1]')).toBe('[1,3]');
    });
  });

  describe('json_patch()', () => {
    it('applies merge patch', () => {
      expect(json_patch('{"a":1,"b":2}', '{"a":3,"c":4}')).toBe('{"a":3,"b":2,"c":4}');
      expect(json_patch('{"a":1,"b":2}', '{"a":null}')).toBe('{"b":2}');
    });
  });

  describe('json_quote()', () => {
    it('quotes values as JSON', () => {
      expect(json_quote(null)).toBe('null');
      expect(json_quote('hello')).toBe('"hello"');
      expect(json_quote(42)).toBe('42');
      expect(json_quote(true)).toBe('true');
    });
  });

  describe('json_each()', () => {
    it('returns rows for array elements', () => {
      const rows = json_each('[1,2,3]');
      expect(rows).toHaveLength(3);
      expect(rows[0].key).toBe(0);
      expect(rows[0].value).toBe('1');
    });

    it('returns rows for object members', () => {
      const rows = json_each('{"a":1,"b":2}');
      expect(rows).toHaveLength(2);
      expect(rows.some(r => r.key === 'a' && r.value === '1')).toBe(true);
    });

    it('handles path', () => {
      const rows = json_each('{"items":[1,2,3]}', '$.items');
      expect(rows).toHaveLength(3);
    });
  });
});

// =============================================================================
// FUNCTION REGISTRY TESTS
// =============================================================================

describe('Function Registry', () => {
  let registry: FunctionRegistry;

  beforeEach(() => {
    registry = new FunctionRegistry();
  });

  describe('built-in functions', () => {
    it('has string functions', () => {
      expect(registry.has('upper')).toBe(true);
      expect(registry.has('lower')).toBe(true);
      expect(registry.has('substr')).toBe(true);
    });

    it('has math functions', () => {
      expect(registry.has('abs')).toBe(true);
      expect(registry.has('round')).toBe(true);
      expect(registry.has('sqrt')).toBe(true);
    });

    it('has date functions', () => {
      expect(registry.has('date')).toBe(true);
      expect(registry.has('datetime')).toBe(true);
      expect(registry.has('strftime')).toBe(true);
    });

    it('has json functions', () => {
      expect(registry.has('json')).toBe(true);
      expect(registry.has('json_extract')).toBe(true);
      expect(registry.has('json_array')).toBe(true);
    });
  });

  describe('invoke()', () => {
    it('invokes functions', () => {
      expect(registry.invoke('upper', ['hello'])).toBe('HELLO');
      expect(registry.invoke('abs', [-5])).toBe(5);
    });

    it('validates argument count', () => {
      expect(() => registry.invoke('upper', [])).toThrow(/requires at least/);
      expect(() => registry.invoke('abs', [1, 2, 3])).toThrow(/accepts at most/);
    });

    it('throws for unknown function', () => {
      expect(() => registry.invoke('unknown_fn', [])).toThrow(/Unknown function/);
    });
  });

  describe('isAggregate()', () => {
    it('identifies aggregates', () => {
      expect(registry.isAggregate('count')).toBe(true);
      expect(registry.isAggregate('sum')).toBe(true);
      expect(registry.isAggregate('upper')).toBe(false);
    });
  });

  describe('user-defined functions', () => {
    it('registers and invokes UDFs', () => {
      registry.register('double', {
        fn: (x) => typeof x === 'number' ? x * 2 : null,
        minArgs: 1,
        maxArgs: 1,
      });

      expect(registry.has('double')).toBe(true);
      expect(registry.invoke('double', [5])).toBe(10);
    });

    it('unregisters functions', () => {
      registry.register('test_fn', {
        fn: () => 'test',
        minArgs: 0,
        maxArgs: 0,
      });

      expect(registry.has('test_fn')).toBe(true);
      registry.unregister('test_fn');
      expect(registry.has('test_fn')).toBe(false);
    });

    it('registers custom aggregates', () => {
      registry.registerAggregate('product', () => {
        let result = 1;
        return {
          step(value) {
            if (typeof value === 'number') result *= value;
          },
          finalize() {
            return result;
          },
          reset() {
            result = 1;
          },
        };
      });

      const acc = registry.createAccumulator('product');
      [2, 3, 4].forEach(v => acc.step(v));
      expect(acc.finalize()).toBe(24);
    });
  });

  describe('getSignature()', () => {
    it('returns function signatures', () => {
      const sig = registry.getSignature('upper');
      expect(sig).toBeDefined();
      expect(sig?.name).toBe('upper');
      expect(sig?.returnType).toBe('string');
    });
  });

  describe('getFunctionNames()', () => {
    it('returns all function names', () => {
      const names = registry.getFunctionNames();
      expect(names).toContain('upper');
      expect(names).toContain('abs');
      expect(names).toContain('date');
      expect(names).toContain('json');
      expect(names.length).toBeGreaterThan(50);
    });
  });
});

describe('Default Registry Exports', () => {
  it('invokeFunction works', () => {
    expect(invokeFunction('upper', ['hello'])).toBe('HELLO');
  });

  it('hasFunction works', () => {
    expect(hasFunction('upper')).toBe(true);
    expect(hasFunction('nonexistent')).toBe(false);
  });

  it('isAggregate works', () => {
    expect(isAggregate('count')).toBe(true);
    expect(isAggregate('upper')).toBe(false);
  });
});
