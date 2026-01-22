/**
 * Tests for URL Table Sources
 *
 * Tests cover:
 * - Format parsers (JSON, NDJSON, CSV)
 * - Format detection
 * - URL source functionality
 * - R2 source functionality
 * - URI resolver
 * - Type-level URL parsing
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { env } from 'cloudflare:test';
import type { R2Bucket } from '@cloudflare/workers-types';

// Test environment type
interface TestEnv {
  TEST_R2_BUCKET: R2Bucket;
}

// =============================================================================
// PARSER TESTS
// =============================================================================

import {
  // Format detection
  detectFormat,
  detectFormatFromExtension,
  detectFormatFromContentType,
  detectFormatFromContent,

  // JSON parsing
  parseJsonArray,

  // NDJSON parsing
  parseNdjson,
  parseNdjsonLine,

  // CSV parsing
  parseCsv,
  parseCsvLine,

  // Schema inference
  inferSchema,
} from './parser.js';

describe('Format Detection', () => {
  describe('detectFormatFromExtension', () => {
    it('should detect JSON format', () => {
      const result = detectFormatFromExtension('data.json');
      expect(result?.format).toBe('json');
      expect(result?.confidence).toBe('high');
    });

    it('should detect NDJSON format', () => {
      expect(detectFormatFromExtension('data.ndjson')?.format).toBe('ndjson');
      expect(detectFormatFromExtension('data.jsonl')?.format).toBe('ndjson');
    });

    it('should detect CSV format', () => {
      const result = detectFormatFromExtension('data.csv');
      expect(result?.format).toBe('csv');
    });

    it('should detect Parquet format', () => {
      const result = detectFormatFromExtension('data.parquet');
      expect(result?.format).toBe('parquet');
    });

    it('should return null for unknown extensions', () => {
      expect(detectFormatFromExtension('data.txt')).toBeNull();
      expect(detectFormatFromExtension('data')).toBeNull();
    });
  });

  describe('detectFormatFromContentType', () => {
    it('should detect application/json', () => {
      const result = detectFormatFromContentType('application/json');
      expect(result?.format).toBe('json');
    });

    it('should detect application/x-ndjson', () => {
      const result = detectFormatFromContentType('application/x-ndjson');
      expect(result?.format).toBe('ndjson');
    });

    it('should detect text/csv', () => {
      expect(detectFormatFromContentType('text/csv')?.format).toBe('csv');
      expect(detectFormatFromContentType('application/csv')?.format).toBe('csv');
    });

    it('should handle charset parameters', () => {
      const result = detectFormatFromContentType('application/json; charset=utf-8');
      expect(result?.format).toBe('json');
    });
  });

  describe('detectFormatFromContent', () => {
    it('should detect JSON array', () => {
      const result = detectFormatFromContent('[{"id": 1}]');
      expect(result?.format).toBe('json');
    });

    it('should detect NDJSON', () => {
      const result = detectFormatFromContent('{"id": 1}\n{"id": 2}');
      expect(result?.format).toBe('ndjson');
    });

    it('should detect Parquet magic bytes', () => {
      const result = detectFormatFromContent('PAR1...');
      expect(result?.format).toBe('parquet');
    });
  });

  describe('detectFormat', () => {
    it('should prioritize extension over content-type', () => {
      const result = detectFormat('data.csv', 'application/json');
      expect(result.format).toBe('csv');
    });

    it('should use content-type when extension is unknown', () => {
      const result = detectFormat('data', 'application/json');
      expect(result.format).toBe('json');
    });

    it('should default to JSON', () => {
      const result = detectFormat('data');
      expect(result.format).toBe('json');
      expect(result.confidence).toBe('low');
    });
  });
});

describe('JSON Parser', () => {
  it('should parse JSON array', () => {
    const result = parseJsonArray('[{"id": 1, "name": "Alice"}]');
    expect(result).toEqual([{ id: 1, name: 'Alice' }]);
  });

  it('should wrap single object in array', () => {
    const result = parseJsonArray('{"id": 1, "name": "Bob"}');
    expect(result).toEqual([{ id: 1, name: 'Bob' }]);
  });

  it('should handle empty array', () => {
    const result = parseJsonArray('[]');
    expect(result).toEqual([]);
  });

  it('should throw on invalid JSON', () => {
    expect(() => parseJsonArray('not json')).toThrow();
  });
});

describe('NDJSON Parser', () => {
  it('should parse NDJSON lines', () => {
    const content = '{"id": 1}\n{"id": 2}\n{"id": 3}';
    const result = parseNdjson(content);
    expect(result).toHaveLength(3);
    expect(result[0]).toEqual({ id: 1 });
    expect(result[2]).toEqual({ id: 3 });
  });

  it('should skip empty lines', () => {
    const content = '{"id": 1}\n\n{"id": 2}\n';
    const result = parseNdjson(content);
    expect(result).toHaveLength(2);
  });

  it('should handle single line', () => {
    const result = parseNdjsonLine('{"name": "test"}');
    expect(result).toEqual({ name: 'test' });
  });

  it('should return null for empty line', () => {
    expect(parseNdjsonLine('')).toBeNull();
    expect(parseNdjsonLine('   ')).toBeNull();
  });
});

describe('CSV Parser', () => {
  describe('parseCsvLine', () => {
    it('should parse simple line', () => {
      const result = parseCsvLine('a,b,c');
      expect(result).toEqual(['a', 'b', 'c']);
    });

    it('should handle quoted fields', () => {
      const result = parseCsvLine('a,"b,c",d');
      expect(result).toEqual(['a', 'b,c', 'd']);
    });

    it('should handle escaped quotes', () => {
      const result = parseCsvLine('a,"b""c",d');
      expect(result).toEqual(['a', 'b"c', 'd']);
    });

    it('should trim whitespace', () => {
      const result = parseCsvLine('  a  ,  b  ,  c  ');
      expect(result).toEqual(['a', 'b', 'c']);
    });
  });

  describe('parseCsv', () => {
    it('should parse CSV with header', () => {
      const content = 'id,name,active\n1,Alice,true\n2,Bob,false';
      const result = parseCsv(content);

      expect(result).toHaveLength(2);
      expect(result[0]).toEqual({ id: 1, name: 'Alice', active: true });
      expect(result[1]).toEqual({ id: 2, name: 'Bob', active: false });
    });

    it('should parse numbers', () => {
      const content = 'a,b\n1,2.5\n3,4';
      const result = parseCsv(content);

      expect(result[0]).toEqual({ a: 1, b: 2.5 });
    });

    it('should handle null values', () => {
      const content = 'a,b\n1,\n2,null';
      const result = parseCsv(content);

      expect(result[0].b).toBeNull();
      expect(result[1].b).toBeNull();
    });

    it('should use custom delimiter', () => {
      const content = 'a;b;c\n1;2;3';
      const result = parseCsv(content, { delimiter: ';' });

      expect(result[0]).toEqual({ a: 1, b: 2, c: 3 });
    });

    it('should skip empty lines', () => {
      const content = 'a,b\n1,2\n\n3,4';
      const result = parseCsv(content);

      expect(result).toHaveLength(2);
    });

    it('should use custom columns when header is false', () => {
      const content = '1,2,3\n4,5,6';
      const result = parseCsv(content, {
        header: false,
        columns: ['x', 'y', 'z'],
      });

      expect(result[0]).toEqual({ x: 1, y: 2, z: 3 });
    });
  });
});

describe('Schema Inference', () => {
  it('should infer types from records', () => {
    const records = [
      { id: 1, name: 'Alice', active: true },
      { id: 2, name: 'Bob', active: false },
    ];
    const schema = inferSchema(records);

    expect(schema.id).toBe('number');
    expect(schema.name).toBe('string');
    expect(schema.active).toBe('boolean');
  });

  it('should handle null values', () => {
    const records = [
      { id: 1, value: null },
      { id: 2, value: 'test' },
    ];
    const schema = inferSchema(records);

    expect(schema.value).toBe('string'); // null merged with string = string
  });

  it('should handle missing fields', () => {
    const records = [
      { id: 1 },
      { id: 2, extra: 'value' },
    ];
    const schema = inferSchema(records);

    expect(schema.id).toBe('number');
    expect(schema.extra).toBe('string');
  });
});

// =============================================================================
// RESOLVER TESTS
// =============================================================================

import {
  parseUri,
  getUriScheme,
  isUrlLiteral,
  extractUrlFromLiteral,
  parseUrlFunction,
  TableSourceResolver,
} from './resolver.js';

describe('URI Parsing', () => {
  describe('getUriScheme', () => {
    it('should detect https scheme', () => {
      expect(getUriScheme('https://example.com')).toBe('https');
    });

    it('should detect http scheme', () => {
      expect(getUriScheme('http://example.com')).toBe('http');
    });

    it('should detect r2 scheme', () => {
      expect(getUriScheme('r2://bucket/path')).toBe('r2');
    });

    it('should detect kv scheme', () => {
      expect(getUriScheme('kv://namespace/key')).toBe('kv');
    });

    it('should throw on unknown scheme', () => {
      expect(() => getUriScheme('ftp://example.com')).toThrow();
    });
  });

  describe('parseUri', () => {
    it('should parse HTTPS URL', () => {
      const result = parseUri('https://example.com/path/to/file.json');

      expect(result.scheme).toBe('https');
      expect(result.host).toBe('example.com');
      expect(result.path).toBe('/path/to/file.json');
    });

    it('should parse URL with port', () => {
      const result = parseUri('https://example.com:8080/path');

      expect(result.host).toBe('example.com');
      expect(result.port).toBe(8080);
    });

    it('should parse URL with query string', () => {
      const result = parseUri('https://example.com/path?key=value&foo=bar');

      expect(result.query).toEqual({ key: 'value', foo: 'bar' });
    });

    it('should parse R2 URI', () => {
      const result = parseUri('r2://my-bucket/data/file.parquet');

      expect(result.scheme).toBe('r2');
      expect(result.host).toBe('my-bucket');
      expect(result.path).toBe('/data/file.parquet');
    });
  });
});

describe('URL Literal Detection', () => {
  it('should detect single-quoted URL', () => {
    expect(isUrlLiteral("'https://example.com/data.json'")).toBe(true);
  });

  it('should detect double-quoted URL', () => {
    expect(isUrlLiteral('"https://example.com/data.json"')).toBe(true);
  });

  it('should detect R2 URL literal', () => {
    expect(isUrlLiteral("'r2://bucket/path.csv'")).toBe(true);
  });

  it('should reject non-URL strings', () => {
    expect(isUrlLiteral("'users'")).toBe(false);
    expect(isUrlLiteral("'not-a-url'")).toBe(false);
  });

  it('should extract URL from literal', () => {
    expect(extractUrlFromLiteral("'https://example.com'")).toBe('https://example.com');
    expect(extractUrlFromLiteral('"r2://bucket/path"')).toBe('r2://bucket/path');
  });
});

describe('URL Function Parsing', () => {
  it('should parse url() with single argument', () => {
    const result = parseUrlFunction("url('https://example.com/data.json')");

    expect(result).not.toBeNull();
    expect(result?.url).toBe('https://example.com/data.json');
    expect(result?.format).toBeUndefined();
  });

  it('should parse url() with format argument', () => {
    const result = parseUrlFunction("url('https://api.example.com/data', 'JSON')");

    expect(result?.url).toBe('https://api.example.com/data');
    expect(result?.format).toBe('json');
  });

  it('should handle case-insensitive URL keyword', () => {
    const result = parseUrlFunction("URL('https://example.com')");
    expect(result).not.toBeNull();
  });

  it('should return null for non-url functions', () => {
    expect(parseUrlFunction('foo()')).toBeNull();
    expect(parseUrlFunction('users')).toBeNull();
  });
});

describe('TableSourceResolver', () => {
  it('should resolve HTTPS URL', async () => {
    const resolver = new TableSourceResolver();
    const source = await resolver.resolve('https://example.com/data.json');

    expect(source.uri).toBe('https://example.com/data.json');
  });

  it('should throw for R2 without bucket', async () => {
    const resolver = new TableSourceResolver();

    await expect(
      resolver.resolve('r2://bucket/path.json')
    ).rejects.toThrow('R2 bucket not found');
  });

  it('should resolve R2 with provided bucket', async () => {
    // Use real R2 bucket from miniflare environment
    const typedEnv = env as unknown as TestEnv;
    const realBucket = typedEnv.TEST_R2_BUCKET;

    const resolver = new TableSourceResolver({
      bindings: { r2: { bucket: realBucket } },
    });

    const source = await resolver.resolve('r2://bucket/path.json');
    expect(source.uri).toBe('r2://bucket/path.json');
  });

  it('should support adding R2 buckets dynamically', async () => {
    // Use real R2 bucket from miniflare environment
    const typedEnv = env as unknown as TestEnv;
    const realBucket = typedEnv.TEST_R2_BUCKET;

    const resolver = new TableSourceResolver();
    resolver.addR2Bucket('my-bucket', realBucket);

    const source = await resolver.resolve('r2://my-bucket/data.json');
    expect(source.uri).toBe('r2://my-bucket/data.json');
  });
});

// =============================================================================
// TYPE-LEVEL TESTS
// =============================================================================

import type {
  ParseUrlScheme,
  ParseUrlHost,
  ParseUrlPath,
  ParseUrlExtension,
  InferFormatFromUrl,
  IsUrlLiteral as IsUrlLiteralType,
  ExtractUrlFromLiteral as ExtractUrlFromLiteralType,
  IsUrlFunction,
  ParseUrlFunction as ParseUrlFunctionType,
  ParsedUrlSource,
  ParseUrlSource,
  IsUrlSourceFrom,
} from './url-types.js';

// Type helpers
type Expect<T extends true> = T;
type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2) ? true : false;

describe('Type-Level URL Parsing', () => {
  it('should parse URL schemes at compile time', () => {
    // These are compile-time tests - if they compile, they pass

    type HttpsScheme = ParseUrlScheme<'https://example.com'>;
    type _TestHttps = Expect<Equal<HttpsScheme, 'https'>>;

    type HttpScheme = ParseUrlScheme<'http://example.com'>;
    type _TestHttp = Expect<Equal<HttpScheme, 'http'>>;

    type R2Scheme = ParseUrlScheme<'r2://bucket/path'>;
    type _TestR2 = Expect<Equal<R2Scheme, 'r2'>>;

    // Runtime assertion for type safety
    expect(true).toBe(true);
  });

  it('should parse URL hosts at compile time', () => {
    type Host1 = ParseUrlHost<'https://example.com/path'>;
    type _TestHost1 = Expect<Equal<Host1, 'example.com'>>;

    type Host2 = ParseUrlHost<'r2://my-bucket/data.json'>;
    type _TestHost2 = Expect<Equal<Host2, 'my-bucket'>>;

    expect(true).toBe(true);
  });

  it('should parse URL paths at compile time', () => {
    type Path1 = ParseUrlPath<'https://example.com/data/file.json'>;
    type _TestPath1 = Expect<Equal<Path1, '/data/file.json'>>;

    type Path2 = ParseUrlPath<'r2://bucket/path/to/data.csv'>;
    type _TestPath2 = Expect<Equal<Path2, '/path/to/data.csv'>>;

    expect(true).toBe(true);
  });

  it('should infer format from URL extension at compile time', () => {
    type JsonFormat = InferFormatFromUrl<'https://example.com/data.json'>;
    type _TestJson = Expect<Equal<JsonFormat, 'json'>>;

    type CsvFormat = InferFormatFromUrl<'r2://bucket/data.csv'>;
    type _TestCsv = Expect<Equal<CsvFormat, 'csv'>>;

    type NdjsonFormat = InferFormatFromUrl<'https://api.example.com/stream.ndjson'>;
    type _TestNdjson = Expect<Equal<NdjsonFormat, 'ndjson'>>;

    type ParquetFormat = InferFormatFromUrl<'r2://bucket/data.parquet'>;
    type _TestParquet = Expect<Equal<ParquetFormat, 'parquet'>>;

    // Default to json when no extension
    type DefaultFormat = InferFormatFromUrl<'https://api.example.com/data'>;
    type _TestDefault = Expect<Equal<DefaultFormat, 'json'>>;

    expect(true).toBe(true);
  });

  it('should detect URL literals at compile time', () => {
    type SingleQuoted = IsUrlLiteralType<"'https://example.com'">;
    type _TestSingle = Expect<Equal<SingleQuoted, true>>;

    type DoubleQuoted = IsUrlLiteralType<'"https://example.com"'>;
    type _TestDouble = Expect<Equal<DoubleQuoted, true>>;

    type NotLiteral = IsUrlLiteralType<'users'>;
    type _TestNot = Expect<Equal<NotLiteral, false>>;

    expect(true).toBe(true);
  });

  it('should extract URL from literal at compile time', () => {
    type Url1 = ExtractUrlFromLiteralType<"'https://example.com'">;
    type _TestUrl1 = Expect<Equal<Url1, 'https://example.com'>>;

    type Url2 = ExtractUrlFromLiteralType<'"r2://bucket/path"'>;
    type _TestUrl2 = Expect<Equal<Url2, 'r2://bucket/path'>>;

    expect(true).toBe(true);
  });

  it('should detect url() function at compile time', () => {
    type IsFunc1 = IsUrlFunction<"url('https://example.com')">;
    type _TestFunc1 = Expect<Equal<IsFunc1, true>>;

    type IsFunc2 = IsUrlFunction<"URL('https://example.com', 'JSON')">;
    type _TestFunc2 = Expect<Equal<IsFunc2, true>>;

    type NotFunc = IsUrlFunction<'users'>;
    type _TestNotFunc = Expect<Equal<NotFunc, false>>;

    expect(true).toBe(true);
  });

  it('should parse url() function at compile time', () => {
    type Parsed1 = ParseUrlFunctionType<"url('https://example.com/data.json')">;
    type _TestParsed1 = Expect<Equal<Parsed1, { url: 'https://example.com/data.json'; format: 'json' }>>;

    type Parsed2 = ParseUrlFunctionType<"url('https://api.example.com/data', 'csv')">;
    type _TestParsed2 = Expect<Equal<Parsed2, { url: 'https://api.example.com/data'; format: 'csv' }>>;

    expect(true).toBe(true);
  });

  it('should check if FROM clause is URL source at compile time', () => {
    type IsUrl1 = IsUrlSourceFrom<"'https://example.com/data.json'">;
    type _TestIsUrl1 = Expect<Equal<IsUrl1, true>>;

    type IsUrl2 = IsUrlSourceFrom<"url('https://example.com')">;
    type _TestIsUrl2 = Expect<Equal<IsUrl2, true>>;

    type IsNotUrl = IsUrlSourceFrom<'users'>;
    type _TestNotUrl = Expect<Equal<IsNotUrl, false>>;

    expect(true).toBe(true);
  });
});

// Runtime test to make vitest happy
describe('Type-Level Tests Runtime Check', () => {
  it('all type tests pass if this compiles', () => {
    expect(true).toBe(true);
  });
});
