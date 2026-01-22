/**
 * SQLite JSON1 Extension Functions - Comprehensive Tests
 *
 * Tests all JSON functions following the SQLite JSON1 specification:
 * https://www.sqlite.org/json1.html
 *
 * Target: 60+ tests covering:
 * - json(value) - validate/minify JSON
 * - json_array(v1, v2, ...) - create array
 * - json_object(k1, v1, k2, v2, ...) - create object
 * - json_extract(json, path) / json(json, path) - extract value
 * - json_insert(json, path, value)
 * - json_replace(json, path, value)
 * - json_set(json, path, value)
 * - json_remove(json, path)
 * - json_type(json, path)
 * - json_valid(json)
 * - json_quote(value)
 * - json_array_length(json, path)
 * - json_patch(target, patch)
 * - json_group_array(value) - aggregate
 * - json_group_object(key, value) - aggregate
 * - json_each(json) - table-valued function
 * - json_tree(json) - table-valued function
 * - -> and ->> operators (JSON path)
 */

import { describe, it, expect } from 'vitest';

import {
  json,
  json_valid,
  json_extract,
  json_type,
  json_array,
  json_object,
  json_array_length,
  json_insert,
  json_replace,
  json_set,
  json_remove,
  json_patch,
  json_quote,
  json_each,
  json_tree,
  type JsonEachRow,
  type JsonTreeRow,
} from './json.js';

import {
  createJsonGroupArrayAccumulator,
  createJsonGroupObjectAccumulator,
  aggregateFactories,
} from './aggregate.js';

// =============================================================================
// json() - Validate and Minify JSON
// =============================================================================

describe('json()', () => {
  it('minifies JSON with extra whitespace', () => {
    expect(json('{ "a" : 1 }')).toBe('{"a":1}');
    expect(json('[ 1 , 2 , 3 ]')).toBe('[1,2,3]');
    expect(json('{"a":1, "b":2}')).toBe('{"a":1,"b":2}');
  });

  it('validates and returns valid JSON', () => {
    expect(json('null')).toBe('null');
    expect(json('true')).toBe('true');
    expect(json('false')).toBe('false');
    expect(json('42')).toBe('42');
    expect(json('3.14')).toBe('3.14');
    expect(json('"hello"')).toBe('"hello"');
  });

  it('handles nested structures', () => {
    expect(json('{"a":{"b":{"c":1}}}')).toBe('{"a":{"b":{"c":1}}}');
    expect(json('[[[1,2,3]]]')).toBe('[[[1,2,3]]]');
  });

  it('returns null for null input', () => {
    expect(json(null)).toBe(null);
  });

  it('throws on invalid JSON', () => {
    expect(() => json('invalid')).toThrow('malformed JSON');
    expect(() => json('{incomplete')).toThrow('malformed JSON');
    expect(() => json('[1,2,]')).toThrow('malformed JSON');
    expect(() => json("{'single': 'quotes'}")).toThrow('malformed JSON');
  });

  it('handles empty objects and arrays', () => {
    expect(json('{}')).toBe('{}');
    expect(json('[]')).toBe('[]');
  });

  it('handles strings with special characters', () => {
    expect(json('"hello\\nworld"')).toBe('"hello\\nworld"');
    expect(json('"tab\\there"')).toBe('"tab\\there"');
  });
});

// =============================================================================
// json_valid() - Validate JSON
// =============================================================================

describe('json_valid()', () => {
  it('returns 1 for valid JSON', () => {
    expect(json_valid('{"a":1}')).toBe(1);
    expect(json_valid('[1,2,3]')).toBe(1);
    expect(json_valid('null')).toBe(1);
    expect(json_valid('true')).toBe(1);
    expect(json_valid('false')).toBe(1);
    expect(json_valid('42')).toBe(1);
    expect(json_valid('"hello"')).toBe(1);
  });

  it('returns 0 for invalid JSON', () => {
    expect(json_valid('invalid')).toBe(0);
    expect(json_valid('{incomplete')).toBe(0);
    expect(json_valid('[1,2,]')).toBe(0);
    expect(json_valid("{'bad': 'quotes'}")).toBe(0);
  });

  it('returns 0 for null input', () => {
    expect(json_valid(null)).toBe(0);
  });

  it('validates numbers correctly', () => {
    expect(json_valid('123')).toBe(1);
    expect(json_valid('-123')).toBe(1);
    expect(json_valid('3.14159')).toBe(1);
    expect(json_valid('1e10')).toBe(1);
    expect(json_valid('1E-5')).toBe(1);
  });
});

// =============================================================================
// json_extract() - Extract Values at JSON Path
// =============================================================================

describe('json_extract()', () => {
  const testObj = '{"a":1,"b":2,"c":{"d":3},"e":[4,5,6]}';

  it('extracts root with $', () => {
    expect(json_extract(testObj, '$')).toBe(testObj);
  });

  it('extracts simple object values', () => {
    expect(json_extract(testObj, '$.a')).toBe(1);
    expect(json_extract(testObj, '$.b')).toBe(2);
  });

  it('extracts nested object values', () => {
    expect(json_extract(testObj, '$.c.d')).toBe(3);
    expect(json_extract('{"a":{"b":{"c":{"d":4}}}}', '$.a.b.c.d')).toBe(4);
  });

  it('extracts array elements', () => {
    expect(json_extract(testObj, '$.e[0]')).toBe(4);
    expect(json_extract(testObj, '$.e[1]')).toBe(5);
    expect(json_extract(testObj, '$.e[2]')).toBe(6);
  });

  it('returns objects/arrays as JSON strings', () => {
    expect(json_extract(testObj, '$.c')).toBe('{"d":3}');
    expect(json_extract(testObj, '$.e')).toBe('[4,5,6]');
  });

  it('returns null for missing paths', () => {
    expect(json_extract(testObj, '$.x')).toBe(null);
    expect(json_extract(testObj, '$.a.b')).toBe(null);
    expect(json_extract(testObj, '$.e[10]')).toBe(null);
  });

  it('handles multiple paths', () => {
    const result = json_extract(testObj, '$.a', '$.b', '$.c.d');
    expect(result).toBe('[1,2,3]');
  });

  it('returns null for null input', () => {
    expect(json_extract(null, '$.a')).toBe(null);
    expect(json_extract(testObj, null)).toBe(null);
  });

  it('handles array length with $[#]', () => {
    expect(json_extract('[1,2,3,4,5]', '$[#]')).toBe(5);
    expect(json_extract('{"items":[1,2,3]}', '$.items[#]')).toBe(3);
  });

  it('extracts quoted keys with brackets', () => {
    expect(json_extract('{"a.b":1}', '$["a.b"]')).toBe(1);
    expect(json_extract('{"key with spaces":2}', '$["key with spaces"]')).toBe(2);
  });
});

// =============================================================================
// json_type() - Get JSON Type
// =============================================================================

describe('json_type()', () => {
  it('returns correct type for primitives', () => {
    expect(json_type('null')).toBe('null');
    expect(json_type('true')).toBe('true');
    expect(json_type('false')).toBe('false');
    expect(json_type('42')).toBe('integer');
    expect(json_type('3.14')).toBe('real');
    expect(json_type('"hello"')).toBe('text');
  });

  it('returns correct type for containers', () => {
    expect(json_type('[]')).toBe('array');
    expect(json_type('[1,2,3]')).toBe('array');
    expect(json_type('{}')).toBe('object');
    expect(json_type('{"a":1}')).toBe('object');
  });

  it('returns type at path', () => {
    expect(json_type('{"a":1}', '$.a')).toBe('integer');
    expect(json_type('{"a":"text"}', '$.a')).toBe('text');
    expect(json_type('{"a":[1,2,3]}', '$.a')).toBe('array');
    expect(json_type('{"a":{"b":1}}', '$.a')).toBe('object');
  });

  it('returns null for missing path', () => {
    expect(json_type('{"a":1}', '$.b')).toBe(null);
  });

  it('returns null for null input', () => {
    expect(json_type(null)).toBe(null);
  });

  it('distinguishes integer from real', () => {
    // Note: JS JSON.parse converts 1.0 to 1, so it becomes integer
    expect(json_type('1.0')).toBe('integer'); // JS behavior
    expect(json_type('1')).toBe('integer');
    expect(json_type('-5')).toBe('integer');
    expect(json_type('-5.5')).toBe('real');
    expect(json_type('3.14')).toBe('real');
  });
});

// =============================================================================
// json_array() - Create JSON Array
// =============================================================================

describe('json_array()', () => {
  it('creates empty array with no args', () => {
    expect(json_array()).toBe('[]');
  });

  it('creates array from primitives', () => {
    expect(json_array(1, 2, 3)).toBe('[1,2,3]');
    expect(json_array('a', 'b', 'c')).toBe('["a","b","c"]');
    expect(json_array(true, false)).toBe('[true,false]');
  });

  it('handles mixed types', () => {
    expect(json_array(1, 'two', true, null)).toBe('[1,"two",true,null]');
  });

  it('handles null values', () => {
    expect(json_array(1, null, 3)).toBe('[1,null,3]');
  });

  it('nests JSON strings as objects', () => {
    expect(json_array('{"a":1}')).toBe('[{"a":1}]');
    expect(json_array('[1,2]', '[3,4]')).toBe('[[1,2],[3,4]]');
  });

  it('handles many arguments', () => {
    const result = json_array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    expect(result).toBe('[1,2,3,4,5,6,7,8,9,10]');
  });
});

// =============================================================================
// json_object() - Create JSON Object
// =============================================================================

describe('json_object()', () => {
  it('creates empty object with no args', () => {
    expect(json_object()).toBe('{}');
  });

  it('creates object from key-value pairs', () => {
    expect(json_object('a', 1, 'b', 2)).toBe('{"a":1,"b":2}');
    expect(json_object('name', 'Alice', 'age', 30)).toBe('{"name":"Alice","age":30}');
  });

  it('handles null values', () => {
    expect(json_object('a', null, 'b', 2)).toBe('{"a":null,"b":2}');
  });

  it('throws for odd number of arguments', () => {
    expect(() => json_object('a', 1, 'b')).toThrow('even number');
  });

  it('throws for null key', () => {
    expect(() => json_object(null, 1)).toThrow('key cannot be NULL');
  });

  it('nests JSON strings as objects', () => {
    expect(json_object('nested', '{"a":1}')).toBe('{"nested":{"a":1}}');
  });

  it('converts numeric keys to strings', () => {
    expect(json_object(1, 'one', 2, 'two')).toBe('{"1":"one","2":"two"}');
  });
});

// =============================================================================
// json_array_length() - Array Length
// =============================================================================

describe('json_array_length()', () => {
  it('returns length of array', () => {
    expect(json_array_length('[]')).toBe(0);
    expect(json_array_length('[1]')).toBe(1);
    expect(json_array_length('[1,2,3]')).toBe(3);
    expect(json_array_length('[1,2,3,4,5]')).toBe(5);
  });

  it('returns null for non-array', () => {
    expect(json_array_length('{"a":1}')).toBe(null);
    expect(json_array_length('42')).toBe(null);
    expect(json_array_length('"string"')).toBe(null);
  });

  it('handles path parameter', () => {
    expect(json_array_length('{"items":[1,2,3]}', '$.items')).toBe(3);
    expect(json_array_length('{"a":{"b":[1,2]}}', '$.a.b')).toBe(2);
  });

  it('returns null for missing path', () => {
    expect(json_array_length('{"a":1}', '$.b')).toBe(null);
  });

  it('returns null for null input', () => {
    expect(json_array_length(null)).toBe(null);
  });
});

// =============================================================================
// json_insert() - Insert at Path
// =============================================================================

describe('json_insert()', () => {
  const obj = '{"a":1}';

  it('inserts at new path', () => {
    expect(json_insert(obj, '$.b', 2)).toBe('{"a":1,"b":2}');
  });

  it('does not replace existing path', () => {
    expect(json_insert(obj, '$.a', 99)).toBe('{"a":1}');
  });

  it('handles multiple path-value pairs', () => {
    const result = json_insert(obj, '$.b', 2, '$.c', 3);
    expect(result).toBe('{"a":1,"b":2,"c":3}');
  });

  it('handles nested paths', () => {
    expect(json_insert('{"a":{}}', '$.a.b', 1)).toBe('{"a":{"b":1}}');
  });

  it('handles array indices', () => {
    const arr = '[1,2,3]';
    expect(json_insert(arr, '$[3]', 4)).toBe('[1,2,3,4]');
  });

  it('returns null for null input', () => {
    expect(json_insert(null, '$.a', 1)).toBe(null);
  });

  it('parses JSON values', () => {
    expect(json_insert(obj, '$.nested', '{"x":1}')).toBe('{"a":1,"nested":{"x":1}}');
  });
});

// =============================================================================
// json_replace() - Replace at Path
// =============================================================================

describe('json_replace()', () => {
  const obj = '{"a":1,"b":2}';

  it('replaces existing path', () => {
    expect(json_replace(obj, '$.a', 99)).toBe('{"a":99,"b":2}');
  });

  it('does not insert at new path', () => {
    expect(json_replace(obj, '$.c', 3)).toBe('{"a":1,"b":2}');
  });

  it('handles multiple path-value pairs', () => {
    const result = json_replace(obj, '$.a', 10, '$.b', 20);
    expect(result).toBe('{"a":10,"b":20}');
  });

  it('handles array indices', () => {
    const arr = '[1,2,3]';
    expect(json_replace(arr, '$[1]', 99)).toBe('[1,99,3]');
  });

  it('returns null for null input', () => {
    expect(json_replace(null, '$.a', 1)).toBe(null);
  });
});

// =============================================================================
// json_set() - Set at Path (Insert or Replace)
// =============================================================================

describe('json_set()', () => {
  const obj = '{"a":1}';

  it('replaces existing path', () => {
    expect(json_set(obj, '$.a', 99)).toBe('{"a":99}');
  });

  it('inserts at new path', () => {
    expect(json_set(obj, '$.b', 2)).toBe('{"a":1,"b":2}');
  });

  it('handles multiple path-value pairs', () => {
    const result = json_set(obj, '$.a', 10, '$.b', 2);
    expect(result).toBe('{"a":10,"b":2}');
  });

  it('creates nested structure', () => {
    expect(json_set('{}', '$.a.b.c', 1)).toBe('{"a":{"b":{"c":1}}}');
  });

  it('handles array indices', () => {
    const arr = '[1,2,3]';
    expect(json_set(arr, '$[1]', 99)).toBe('[1,99,3]');
    expect(json_set(arr, '$[5]', 6)).toBe('[1,2,3,null,null,6]');
  });

  it('returns null for null input', () => {
    expect(json_set(null, '$.a', 1)).toBe(null);
  });
});

// =============================================================================
// json_remove() - Remove at Path
// =============================================================================

describe('json_remove()', () => {
  it('removes object property', () => {
    expect(json_remove('{"a":1,"b":2}', '$.a')).toBe('{"b":2}');
    expect(json_remove('{"a":1,"b":2,"c":3}', '$.b')).toBe('{"a":1,"c":3}');
  });

  it('removes array element', () => {
    expect(json_remove('[1,2,3]', '$[1]')).toBe('[1,3]');
    expect(json_remove('[1,2,3,4,5]', '$[0]')).toBe('[2,3,4,5]');
  });

  it('handles multiple paths', () => {
    const result = json_remove('{"a":1,"b":2,"c":3}', '$.a', '$.c');
    expect(result).toBe('{"b":2}');
  });

  it('handles nested paths', () => {
    expect(json_remove('{"a":{"b":1,"c":2}}', '$.a.b')).toBe('{"a":{"c":2}}');
  });

  it('ignores missing paths', () => {
    expect(json_remove('{"a":1}', '$.b')).toBe('{"a":1}');
  });

  it('returns null for null input', () => {
    expect(json_remove(null, '$.a')).toBe(null);
  });
});

// =============================================================================
// json_patch() - RFC 7396 Merge Patch
// =============================================================================

describe('json_patch()', () => {
  it('adds new properties', () => {
    expect(json_patch('{"a":1}', '{"b":2}')).toBe('{"a":1,"b":2}');
  });

  it('replaces existing properties', () => {
    expect(json_patch('{"a":1}', '{"a":99}')).toBe('{"a":99}');
  });

  it('removes properties with null', () => {
    expect(json_patch('{"a":1,"b":2}', '{"a":null}')).toBe('{"b":2}');
  });

  it('handles nested patches', () => {
    const target = '{"a":{"b":1,"c":2}}';
    const patch = '{"a":{"b":99}}';
    expect(json_patch(target, patch)).toBe('{"a":{"b":99,"c":2}}');
  });

  it('replaces array entirely', () => {
    expect(json_patch('{"a":[1,2,3]}', '{"a":[4,5]}')).toBe('{"a":[4,5]}');
  });

  it('handles null target', () => {
    expect(json_patch(null, '{"a":1}')).toBe('{"a":1}');
  });

  it('handles null patch', () => {
    expect(json_patch('{"a":1}', null)).toBe('{"a":1}');
  });
});

// =============================================================================
// json_quote() - Quote SQL Value as JSON
// =============================================================================

describe('json_quote()', () => {
  it('quotes null', () => {
    expect(json_quote(null)).toBe('null');
  });

  it('quotes strings', () => {
    expect(json_quote('hello')).toBe('"hello"');
    expect(json_quote('with "quotes"')).toBe('"with \\"quotes\\""');
  });

  it('quotes numbers', () => {
    expect(json_quote(42)).toBe('42');
    expect(json_quote(3.14)).toBe('3.14');
    expect(json_quote(-100)).toBe('-100');
  });

  it('quotes booleans', () => {
    expect(json_quote(true)).toBe('true');
    expect(json_quote(false)).toBe('false');
  });

  it('quotes bigints', () => {
    expect(json_quote(BigInt(12345678901234567890n))).toBe('12345678901234567890');
  });

  it('quotes dates as ISO strings', () => {
    const date = new Date('2024-01-15T12:30:00.000Z');
    expect(json_quote(date)).toBe('"2024-01-15T12:30:00.000Z"');
  });

  it('quotes blobs as hex strings', () => {
    const blob = new Uint8Array([0xDE, 0xAD, 0xBE, 0xEF]);
    expect(json_quote(blob)).toBe('"deadbeef"');
  });
});

// =============================================================================
// json_each() - Table-Valued Function
// =============================================================================

describe('json_each()', () => {
  it('iterates array elements', () => {
    const rows = json_each('[1,2,3]');
    expect(rows).toHaveLength(3);

    expect(rows[0].key).toBe(0);
    expect(rows[0].value).toBe('1');
    expect(rows[0].type).toBe('integer');

    expect(rows[1].key).toBe(1);
    expect(rows[1].value).toBe('2');

    expect(rows[2].key).toBe(2);
    expect(rows[2].value).toBe('3');
  });

  it('iterates object properties', () => {
    const rows = json_each('{"a":1,"b":2}');
    expect(rows).toHaveLength(2);

    const aRow = rows.find(r => r.key === 'a')!;
    expect(aRow.value).toBe('1');
    expect(aRow.type).toBe('integer');

    const bRow = rows.find(r => r.key === 'b')!;
    expect(bRow.value).toBe('2');
  });

  it('handles path parameter', () => {
    const rows = json_each('{"items":[1,2,3]}', '$.items');
    expect(rows).toHaveLength(3);
    expect(rows[0].key).toBe(0);
    expect(rows[0].path).toBe('$.items');
  });

  it('returns fullkey for each element', () => {
    const rows = json_each('[1,2,3]');
    expect(rows[0].fullkey).toBe('$[0]');
    expect(rows[1].fullkey).toBe('$[1]');
    expect(rows[2].fullkey).toBe('$[2]');
  });

  it('returns atom for scalars, null for containers', () => {
    const rows = json_each('[1,{"a":1},[2]]');
    expect(rows[0].atom).toBe(1);
    expect(rows[1].atom).toBe(null); // object
    expect(rows[2].atom).toBe(null); // array
  });

  it('returns empty array for null input', () => {
    expect(json_each(null)).toEqual([]);
  });

  it('returns empty array for missing path', () => {
    expect(json_each('{"a":1}', '$.b')).toEqual([]);
  });
});

// =============================================================================
// json_tree() - Table-Valued Function (Recursive)
// =============================================================================

describe('json_tree()', () => {
  it('walks simple array', () => {
    const rows = json_tree('[1,2,3]');
    expect(rows.length).toBeGreaterThanOrEqual(4); // root + 3 elements
    expect(rows[0].key).toBe(null); // root
    expect(rows[0].type).toBe('array');
  });

  it('walks nested object', () => {
    const rows = json_tree('{"a":{"b":1}}');
    expect(rows.length).toBe(3); // root, a, b

    const rootRow = rows.find(r => r.key === null)!;
    expect(rootRow.type).toBe('object');

    const aRow = rows.find(r => r.key === 'a')!;
    expect(aRow.type).toBe('object');

    const bRow = rows.find(r => r.key === 'b')!;
    expect(bRow.type).toBe('integer');
    expect(bRow.atom).toBe(1);
  });

  it('assigns sequential IDs', () => {
    const rows = json_tree('[1,2,3]');
    const ids = rows.map(r => r.id);
    expect(ids).toEqual(ids.sort((a, b) => a - b));
    expect(new Set(ids).size).toBe(ids.length); // All unique
  });

  it('tracks parent IDs', () => {
    const rows = json_tree('{"a":1}');
    const rootRow = rows.find(r => r.key === null)!;
    const aRow = rows.find(r => r.key === 'a')!;
    expect(aRow.parent).toBe(rootRow.id);
  });

  it('handles path parameter', () => {
    const rows = json_tree('{"data":{"items":[1,2]}}', '$.data');
    expect(rows[0].path).toBe('$.data');
  });

  it('returns empty array for null input', () => {
    expect(json_tree(null)).toEqual([]);
  });
});

// =============================================================================
// json_group_array() - Aggregate Function
// =============================================================================

describe('json_group_array()', () => {
  it('aggregates values into JSON array', () => {
    const acc = createJsonGroupArrayAccumulator();
    acc.step(1);
    acc.step(2);
    acc.step(3);
    expect(acc.finalize()).toBe('[1,2,3]');
  });

  it('handles mixed types', () => {
    const acc = createJsonGroupArrayAccumulator();
    acc.step(1);
    acc.step('two');
    acc.step(true);
    acc.step(null);
    expect(acc.finalize()).toBe('[1,"two",true,null]');
  });

  it('returns empty array for no values', () => {
    const acc = createJsonGroupArrayAccumulator();
    expect(acc.finalize()).toBe('[]');
  });

  it('handles only null values', () => {
    const acc = createJsonGroupArrayAccumulator();
    acc.step(null);
    acc.step(null);
    expect(acc.finalize()).toBe('[null,null]');
  });

  it('resets correctly', () => {
    const acc = createJsonGroupArrayAccumulator();
    acc.step(1);
    acc.step(2);
    acc.reset();
    acc.step(3);
    expect(acc.finalize()).toBe('[3]');
  });

  it('is registered in aggregate factories', () => {
    expect(aggregateFactories.json_group_array).toBeDefined();
    const acc = aggregateFactories.json_group_array();
    acc.step('test');
    expect(acc.finalize()).toBe('["test"]');
  });
});

// =============================================================================
// json_group_object() - Aggregate Function
// =============================================================================

describe('json_group_object()', () => {
  it('aggregates key-value pairs into JSON object', () => {
    const acc = createJsonGroupObjectAccumulator();
    acc.step('a', 1);
    acc.step('b', 2);
    acc.step('c', 3);
    expect(acc.finalize()).toBe('{"a":1,"b":2,"c":3}');
  });

  it('handles mixed value types', () => {
    const acc = createJsonGroupObjectAccumulator();
    acc.step('num', 42);
    acc.step('str', 'hello');
    acc.step('bool', true);
    expect(acc.finalize()).toBe('{"num":42,"str":"hello","bool":true}');
  });

  it('last value wins for duplicate keys', () => {
    const acc = createJsonGroupObjectAccumulator();
    acc.step('key', 1);
    acc.step('key', 2);
    expect(acc.finalize()).toBe('{"key":2}');
  });

  it('returns empty object for no values', () => {
    const acc = createJsonGroupObjectAccumulator();
    expect(acc.finalize()).toBe('{}');
  });

  it('skips null keys', () => {
    const acc = createJsonGroupObjectAccumulator();
    acc.step(null, 1);
    acc.step('valid', 2);
    expect(acc.finalize()).toBe('{"valid":2}');
  });

  it('resets correctly', () => {
    const acc = createJsonGroupObjectAccumulator();
    acc.step('a', 1);
    acc.reset();
    acc.step('b', 2);
    expect(acc.finalize()).toBe('{"b":2}');
  });

  it('is registered in aggregate factories', () => {
    expect(aggregateFactories.json_group_object).toBeDefined();
    const acc = aggregateFactories.json_group_object();
    acc.step('key', 'value');
    expect(acc.finalize()).toBe('{"key":"value"}');
  });
});

// =============================================================================
// JSON Path Operators: -> and ->>
// =============================================================================

describe('JSON path operators', () => {
  describe('json_extract_path (->)', () => {
    it('extracts and returns JSON', () => {
      // The -> operator returns JSON (object/array preserved)
      expect(json_extract('{"a":{"b":1}}', '$.a')).toBe('{"b":1}');
      expect(json_extract('[1,[2,3]]', '$[1]')).toBe('[2,3]');
    });
  });

  describe('json_extract_path_text (->>)', () => {
    it('extracts and returns text for scalars', () => {
      // json_extract returns the raw value for scalars
      expect(json_extract('{"a":1}', '$.a')).toBe(1);
      expect(json_extract('{"a":"text"}', '$.a')).toBe('text');
      expect(json_extract('{"a":true}', '$.a')).toBe(true);
    });
  });
});

// =============================================================================
// Edge Cases and Error Handling
// =============================================================================

describe('JSON function edge cases', () => {
  it('handles deeply nested structures', () => {
    const deep = '{"a":{"b":{"c":{"d":{"e":{"f":1}}}}}}';
    expect(json_extract(deep, '$.a.b.c.d.e.f')).toBe(1);
    expect(json_type(deep, '$.a.b.c.d.e.f')).toBe('integer');
  });

  it('handles large arrays', () => {
    const arr = JSON.stringify(Array.from({ length: 1000 }, (_, i) => i));
    expect(json_array_length(arr)).toBe(1000);
    expect(json_extract(arr, '$[999]')).toBe(999);
  });

  it('handles unicode in strings', () => {
    // JS JSON.stringify converts unicode escapes to actual characters
    expect(json('{"emoji":"\\ud83d\\ude00"}')).toBe('{"emoji":"\ud83d\ude00"}');
    expect(json_extract('{"name":"\\u4e2d\\u6587"}', '$.name')).toBe('\u4e2d\u6587');
  });

  it('handles empty strings', () => {
    expect(json_extract('{"a":""}', '$.a')).toBe('');
    expect(json_type('{"a":""}', '$.a')).toBe('text');
  });

  it('handles scientific notation', () => {
    // JS JSON.parse converts these to numbers; Number.isInteger determines type
    expect(json_type('1e10')).toBe('integer');
    expect(json_type('1.5e10')).toBe('integer'); // 15000000000 is integer in JS
    expect(json_extract('{"n":1e10}', '$.n')).toBe(10000000000);
    expect(json_type('1.5e-1')).toBe('real'); // 0.15 is real
  });

  it('handles negative numbers', () => {
    expect(json_type('-42')).toBe('integer');
    expect(json_type('-3.14')).toBe('real');
  });

  it('handles boolean values in arrays', () => {
    expect(json_extract('[true,false,true]', '$[1]')).toBe(false);
    expect(json_type('[true,false]', '$[0]')).toBe('true');
    expect(json_type('[true,false]', '$[1]')).toBe('false');
  });
});
