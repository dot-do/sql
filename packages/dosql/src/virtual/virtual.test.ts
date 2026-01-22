/**
 * Tests for Virtual Table Support
 *
 * Tests cover:
 * - Virtual table types
 * - URL virtual table creation and scanning
 * - Virtual table registry
 * - Parser support (URL literals, WITH clause, url() function)
 * - R2 virtual tables (mocked)
 * - Format auto-detection
 * - Caching behavior
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// =============================================================================
// TYPE TESTS
// =============================================================================

import type {
  VirtualTable,
  VirtualTableFormat,
  URLSourceOptions,
  WithClauseOptions,
  ParseVirtualTableScheme,
  IsVirtualTableUrl,
  ExtractVirtualTableUrl,
  InferVirtualTableFormat,
} from './types.js';

// Type helpers
type Expect<T extends true> = T;
type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2) ? true : false;

describe('Virtual Table Types', () => {
  it('should have correct VirtualTableFormat type', () => {
    const formats: VirtualTableFormat[] = ['json', 'ndjson', 'csv', 'parquet'];
    expect(formats).toHaveLength(4);
  });

  it('should parse URL schemes at compile time', () => {
    type HttpsScheme = ParseVirtualTableScheme<'https://example.com'>;
    type _TestHttps = Expect<Equal<HttpsScheme, 'https'>>;

    type HttpScheme = ParseVirtualTableScheme<'http://example.com'>;
    type _TestHttp = Expect<Equal<HttpScheme, 'http'>>;

    type R2Scheme = ParseVirtualTableScheme<'r2://bucket/path'>;
    type _TestR2 = Expect<Equal<R2Scheme, 'r2'>>;

    type S3Scheme = ParseVirtualTableScheme<'s3://bucket/key'>;
    type _TestS3 = Expect<Equal<S3Scheme, 's3'>>;

    expect(true).toBe(true);
  });

  it('should detect virtual table URLs at compile time', () => {
    type SingleQuoted = IsVirtualTableUrl<"'https://example.com'">;
    type _TestSingle = Expect<Equal<SingleQuoted, true>>;

    type DoubleQuoted = IsVirtualTableUrl<'"r2://bucket/path"'>;
    type _TestDouble = Expect<Equal<DoubleQuoted, true>>;

    type NotUrl = IsVirtualTableUrl<'users'>;
    type _TestNot = Expect<Equal<NotUrl, false>>;

    type UnquotedUrl = IsVirtualTableUrl<'https://example.com'>;
    type _TestUnquoted = Expect<Equal<UnquotedUrl, true>>;

    expect(true).toBe(true);
  });

  it('should extract URL from literal at compile time', () => {
    type Url1 = ExtractVirtualTableUrl<"'https://example.com/data.json'">;
    type _TestUrl1 = Expect<Equal<Url1, 'https://example.com/data.json'>>;

    type Url2 = ExtractVirtualTableUrl<'"r2://mybucket/file.csv"'>;
    type _TestUrl2 = Expect<Equal<Url2, 'r2://mybucket/file.csv'>>;

    expect(true).toBe(true);
  });

  it('should infer format from URL at compile time', () => {
    type JsonFormat = InferVirtualTableFormat<'https://api.example.com/data.json'>;
    type _TestJson = Expect<Equal<JsonFormat, 'json'>>;

    type CsvFormat = InferVirtualTableFormat<'r2://bucket/data.csv'>;
    type _TestCsv = Expect<Equal<CsvFormat, 'csv'>>;

    type NdjsonFormat = InferVirtualTableFormat<'https://stream.example.com/events.ndjson'>;
    type _TestNdjson = Expect<Equal<NdjsonFormat, 'ndjson'>>;

    type ParquetFormat = InferVirtualTableFormat<'r2://lakehouse/table.parquet'>;
    type _TestParquet = Expect<Equal<ParquetFormat, 'parquet'>>;

    type DefaultFormat = InferVirtualTableFormat<'https://api.example.com/data'>;
    type _TestDefault = Expect<Equal<DefaultFormat, 'json'>>;

    expect(true).toBe(true);
  });
});

// =============================================================================
// URL TABLE TESTS
// =============================================================================

import {
  parseVirtualTableUrl,
  URLVirtualTable,
  createURLVirtualTable,
  fetchVirtualTable,
} from './url-table.js';

describe('URL Parsing', () => {
  it('should parse HTTPS URLs', () => {
    const result = parseVirtualTableUrl('https://example.com/path/to/data.json');

    expect(result.scheme).toBe('https');
    expect(result.host).toBe('example.com');
    expect(result.path).toBe('/path/to/data.json');
    expect(result.detectedFormat).toBe('json');
  });

  it('should parse URLs with ports', () => {
    const result = parseVirtualTableUrl('https://example.com:8080/data.csv');

    expect(result.host).toBe('example.com');
    expect(result.port).toBe(8080);
    expect(result.path).toBe('/data.csv');
    expect(result.detectedFormat).toBe('csv');
  });

  it('should parse URLs with query strings', () => {
    const result = parseVirtualTableUrl('https://api.example.com/users?page=1&limit=10');

    expect(result.query).toEqual({ page: '1', limit: '10' });
  });

  it('should parse R2 URLs', () => {
    const result = parseVirtualTableUrl('r2://mybucket/data/file.parquet');

    expect(result.scheme).toBe('r2');
    expect(result.host).toBe('mybucket');
    expect(result.path).toBe('/data/file.parquet');
    expect(result.detectedFormat).toBe('parquet');
  });

  it('should parse S3 URLs', () => {
    const result = parseVirtualTableUrl('s3://my-bucket/prefix/data.ndjson');

    expect(result.scheme).toBe('s3');
    expect(result.host).toBe('my-bucket');
    expect(result.path).toBe('/prefix/data.ndjson');
    expect(result.detectedFormat).toBe('ndjson');
  });

  it('should detect format from extension', () => {
    expect(parseVirtualTableUrl('https://example.com/data.json').detectedFormat).toBe('json');
    expect(parseVirtualTableUrl('https://example.com/data.ndjson').detectedFormat).toBe('ndjson');
    expect(parseVirtualTableUrl('https://example.com/data.jsonl').detectedFormat).toBe('ndjson');
    expect(parseVirtualTableUrl('https://example.com/data.csv').detectedFormat).toBe('csv');
    expect(parseVirtualTableUrl('https://example.com/data.parquet').detectedFormat).toBe('parquet');
    expect(parseVirtualTableUrl('https://example.com/data').detectedFormat).toBeUndefined();
  });

  it('should throw for invalid URLs', () => {
    expect(() => parseVirtualTableUrl('not-a-url')).toThrow();
    expect(() => parseVirtualTableUrl('ftp://example.com')).toThrow();
  });
});

describe('URLVirtualTable', () => {
  // Mock fetch globally
  const originalFetch = global.fetch;
  let mockFetch: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    mockFetch = vi.fn();
    global.fetch = mockFetch;
  });

  afterEach(() => {
    global.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  it('should create URL virtual table', () => {
    const table = createURLVirtualTable('https://api.example.com/users.json');

    expect(table).toBeInstanceOf(URLVirtualTable);
    expect(table.uri).toBe('https://api.example.com/users.json');
    expect(table.format).toBe('json');
  });

  it('should use explicit format over auto-detected', () => {
    const table = createURLVirtualTable('https://api.example.com/data', {
      format: 'csv',
    });

    expect(table.format).toBe('csv');
  });

  it('should scan JSON URL source', async () => {
    const jsonData = [
      { id: 1, name: 'Alice', email: 'alice@example.com' },
      { id: 2, name: 'Bob', email: 'bob@example.com' },
    ];

    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      headers: new Headers({ 'Content-Type': 'application/json' }),
      text: vi.fn().mockResolvedValue(JSON.stringify(jsonData)),
      body: null,
    });

    const table = createURLVirtualTable('https://api.example.com/users.json');
    const records: Record<string, unknown>[] = [];

    for await (const record of table.scan()) {
      records.push(record);
    }

    expect(records).toHaveLength(2);
    expect(records[0]).toEqual({ id: 1, name: 'Alice', email: 'alice@example.com' });
  });

  it('should scan CSV URL source with headers', async () => {
    const csvData = `id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com`;

    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      headers: new Headers({ 'Content-Type': 'text/csv' }),
      text: vi.fn().mockResolvedValue(csvData),
      body: null,
    });

    const table = createURLVirtualTable('https://data.gov/dataset.csv');
    const records: Record<string, unknown>[] = [];

    for await (const record of table.scan()) {
      records.push(record);
    }

    expect(records).toHaveLength(2);
    expect(records[0]).toHaveProperty('id', 1);
    expect(records[0]).toHaveProperty('name', 'Alice');
  });

  it('should apply projection', async () => {
    const jsonData = [
      { id: 1, name: 'Alice', email: 'alice@example.com', age: 30 },
      { id: 2, name: 'Bob', email: 'bob@example.com', age: 25 },
    ];

    mockFetch.mockResolvedValue({
      ok: true,
      headers: new Headers({ 'Content-Type': 'application/json' }),
      text: vi.fn().mockResolvedValue(JSON.stringify(jsonData)),
    });

    const table = createURLVirtualTable('https://api.example.com/users.json');
    const records: Record<string, unknown>[] = [];

    for await (const record of table.scan({ projection: ['id', 'name'] })) {
      records.push(record);
    }

    expect(records[0]).toEqual({ id: 1, name: 'Alice' });
    expect(records[0]).not.toHaveProperty('email');
    expect(records[0]).not.toHaveProperty('age');
  });

  it('should apply limit and offset', async () => {
    const jsonData = [
      { id: 1, name: 'Alice' },
      { id: 2, name: 'Bob' },
      { id: 3, name: 'Charlie' },
      { id: 4, name: 'Diana' },
    ];

    mockFetch.mockResolvedValue({
      ok: true,
      headers: new Headers({ 'Content-Type': 'application/json' }),
      text: vi.fn().mockResolvedValue(JSON.stringify(jsonData)),
    });

    const table = createURLVirtualTable('https://api.example.com/users.json');
    const records: Record<string, unknown>[] = [];

    for await (const record of table.scan({ limit: 2, offset: 1 })) {
      records.push(record);
    }

    expect(records).toHaveLength(2);
    expect(records[0]).toEqual({ id: 2, name: 'Bob' });
    expect(records[1]).toEqual({ id: 3, name: 'Charlie' });
  });

  it('should get schema from explicit definition', async () => {
    const table = createURLVirtualTable('https://api.example.com/users.json', {
      schema: {
        id: 'number',
        name: 'string',
        active: 'boolean',
      },
    });

    const schema = await table.getSchema();

    expect(schema.columns).toEqual({
      id: 'number',
      name: 'string',
      active: 'boolean',
    });
  });

  it('should infer schema from data', async () => {
    const jsonData = [
      { id: 1, name: 'Alice', active: true },
      { id: 2, name: 'Bob', active: false },
    ];

    mockFetch.mockResolvedValue({
      ok: true,
      headers: new Headers({ 'Content-Type': 'application/json' }),
      text: vi.fn().mockResolvedValue(JSON.stringify(jsonData)),
    });

    const table = createURLVirtualTable('https://api.example.com/users.json');
    const schema = await table.getSchema();

    expect(schema.columns.id).toBe('number');
    expect(schema.columns.name).toBe('string');
    expect(schema.columns.active).toBe('boolean');
  });

  it('should check availability', async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
    });

    const table = createURLVirtualTable('https://api.example.com/users.json');
    const available = await table.isAvailable();

    expect(available).toBe(true);
  });

  it('should handle HTTP errors', async () => {
    mockFetch.mockResolvedValue({
      ok: false,
      status: 404,
      statusText: 'Not Found',
    });

    const table = createURLVirtualTable('https://api.example.com/nonexistent.json');

    await expect(async () => {
      for await (const _ of table.scan()) {
        // Should throw before yielding
      }
    }).rejects.toThrow('HTTP 404');
  });

  it('should reject non-HTTP URLs in createURLVirtualTable', () => {
    expect(() => createURLVirtualTable('r2://bucket/path.json'))
      .toThrow('createURLVirtualTable only supports http:// and https:// URLs');
  });
});

// =============================================================================
// REGISTRY TESTS
// =============================================================================

import {
  VirtualTableRegistry,
  createVirtualTableRegistry,
  parseWithClause,
  withClauseToOptions,
  resolveVirtualTableFromClause,
} from './registry.js';

describe('VirtualTableRegistry', () => {
  it('should create registry with defaults', () => {
    const registry = createVirtualTableRegistry();

    expect(registry.supports('https')).toBe(true);
    expect(registry.supports('http')).toBe(true);
    expect(registry.supports('r2')).toBe(true);
    expect(registry.supports('s3')).toBe(false);
  });

  it('should create HTTP virtual table', async () => {
    const registry = createVirtualTableRegistry();
    const table = await registry.create('https://example.com/data.json');

    expect(table.uri).toBe('https://example.com/data.json');
  });

  it('should throw for R2 without bucket binding', async () => {
    const registry = createVirtualTableRegistry();

    await expect(registry.create('r2://bucket/data.json'))
      .rejects.toThrow('R2 bucket not found');
  });

  it('should create R2 virtual table with binding', async () => {
    const mockBucket = {
      get: vi.fn().mockResolvedValue(null),
      head: vi.fn().mockResolvedValue(null),
      list: vi.fn().mockResolvedValue({ objects: [], truncated: false, delimitedPrefixes: [] }),
    };

    const registry = createVirtualTableRegistry({
      bindings: { r2: { mybucket: mockBucket as any } },
    });

    const table = await registry.create('r2://mybucket/data.json');
    expect(table.uri).toBe('r2://mybucket/data.json');
  });

  it('should add R2 bucket dynamically', async () => {
    const mockBucket = {
      get: vi.fn().mockResolvedValue(null),
      head: vi.fn().mockResolvedValue(null),
      list: vi.fn().mockResolvedValue({ objects: [], truncated: false, delimitedPrefixes: [] }),
    };

    const registry = createVirtualTableRegistry();
    registry.addR2Bucket('mybucket', mockBucket as any);

    const table = await registry.create('r2://mybucket/data.json');
    expect(table.uri).toBe('r2://mybucket/data.json');
  });

  it('should get supported schemes', () => {
    const registry = createVirtualTableRegistry();
    const schemes = registry.getSupportedSchemes();

    expect(schemes).toContain('https');
    expect(schemes).toContain('http');
    expect(schemes).toContain('r2');
  });
});

describe('WITH Clause Parsing', () => {
  it('should parse format option', () => {
    const options = parseWithClause("(format='csv')");
    expect(options.format).toBe('csv');
  });

  it('should parse headers option', () => {
    const options = parseWithClause("(headers=true)");
    expect(options.headers).toBe(true);

    const options2 = parseWithClause("(headers=false)");
    expect(options2.headers).toBe(false);
  });

  it('should parse delimiter option', () => {
    const options = parseWithClause("(delimiter=';')");
    expect(options.delimiter).toBe(';');
  });

  it('should parse multiple options', () => {
    const options = parseWithClause("(format='csv', headers=true, delimiter=',')");

    expect(options.format).toBe('csv');
    expect(options.headers).toBe(true);
    expect(options.delimiter).toBe(',');
  });

  it('should parse timeout option', () => {
    const options = parseWithClause("(timeout=5000)");
    expect(options.timeout).toBe(5000);
  });

  it('should parse cache options', () => {
    const options = parseWithClause("(cache=true, cacheTtl=600)");

    expect(options.cache).toBe(true);
    expect(options.cacheTtl).toBe(600);
  });

  it('should parse basic auth options', () => {
    const options = parseWithClause("(auth='basic', username='user', password='pass')");

    expect(options.auth).toBe('basic');
    expect(options.username).toBe('user');
    expect(options.password).toBe('pass');
  });

  it('should parse bearer auth options', () => {
    const options = parseWithClause("(auth='bearer', token='my-token-123')");

    expect(options.auth).toBe('bearer');
    expect(options.token).toBe('my-token-123');
  });

  it('should parse API key auth options', () => {
    const options = parseWithClause("(auth='api-key', apiKeyHeader='X-API-Key', apiKeyValue='secret')");

    expect(options.auth).toBe('api-key');
    expect(options.apiKeyHeader).toBe('X-API-Key');
    expect(options.apiKeyValue).toBe('secret');
  });

  it('should handle double quoted values', () => {
    const options = parseWithClause('(format="json", delimiter=",")');

    expect(options.format).toBe('json');
    expect(options.delimiter).toBe(',');
  });

  it('should convert WithClauseOptions to URLSourceOptions', () => {
    const withOptions: WithClauseOptions = {
      format: 'csv',
      headers: true,
      delimiter: ';',
      timeout: 5000,
      cache: true,
      cacheTtl: 600,
      auth: 'bearer',
      token: 'my-token',
    };

    const urlOptions = withClauseToOptions(withOptions);

    expect(urlOptions.format).toBe('csv');
    expect(urlOptions.csvHeaders).toBe(true);
    expect(urlOptions.delimiter).toBe(';');
    expect(urlOptions.timeout).toBe(5000);
    expect(urlOptions.cache).toBe(true);
    expect(urlOptions.cacheTtl).toBe(600);
    expect(urlOptions.auth?.bearer).toBe('my-token');
  });
});

describe('resolveVirtualTableFromClause', () => {
  it('should resolve quoted URL literal', async () => {
    const registry = createVirtualTableRegistry();
    const table = await resolveVirtualTableFromClause(
      "'https://example.com/data.json'",
      registry
    );

    expect(table).not.toBeNull();
    expect(table!.uri).toBe('https://example.com/data.json');
  });

  it('should resolve double quoted URL literal', async () => {
    const registry = createVirtualTableRegistry();
    const table = await resolveVirtualTableFromClause(
      '"https://example.com/data.csv"',
      registry
    );

    expect(table).not.toBeNull();
    expect(table!.uri).toBe('https://example.com/data.csv');
  });

  it('should resolve url() function', async () => {
    const registry = createVirtualTableRegistry();
    const table = await resolveVirtualTableFromClause(
      "url('https://api.example.com/users', 'JSON')",
      registry
    );

    expect(table).not.toBeNull();
    expect(table!.uri).toBe('https://api.example.com/users');
    expect(table!.format).toBe('json');
  });

  it('should apply WITH clause options', async () => {
    const registry = createVirtualTableRegistry();
    const table = await resolveVirtualTableFromClause(
      "'https://data.gov/dataset.csv'",
      registry,
      "(format='csv', headers=true)"
    );

    expect(table).not.toBeNull();
    expect(table!.format).toBe('csv');
  });

  it('should return null for non-URL sources', async () => {
    const registry = createVirtualTableRegistry();
    const table = await resolveVirtualTableFromClause('users', registry);

    expect(table).toBeNull();
  });
});

// =============================================================================
// PARSER TESTS
// =============================================================================

import {
  isValidUrlScheme,
  isQuotedUrlLiteral,
  extractUrlFromQuotedLiteral,
  isUrlFunction,
  parseUrlFunction,
  extractWithClause,
  parseWithClauseOptions,
  parseVirtualTableFromClause,
  parseSelectWithVirtualTable,
} from '../parser/virtual.js';

describe('URL Literal Detection', () => {
  it('should validate URL schemes', () => {
    expect(isValidUrlScheme('https://example.com')).toBe(true);
    expect(isValidUrlScheme('http://example.com')).toBe(true);
    expect(isValidUrlScheme('r2://bucket/path')).toBe(true);
    expect(isValidUrlScheme('s3://bucket/key')).toBe(true);
    expect(isValidUrlScheme('file:///path/to/file')).toBe(true);
    expect(isValidUrlScheme('ftp://example.com')).toBe(false);
    expect(isValidUrlScheme('users')).toBe(false);
  });

  it('should detect quoted URL literals', () => {
    expect(isQuotedUrlLiteral("'https://example.com/data.json'")).toBe(true);
    expect(isQuotedUrlLiteral('"r2://bucket/file.csv"')).toBe(true);
    expect(isQuotedUrlLiteral("'users'")).toBe(false);
    expect(isQuotedUrlLiteral('https://example.com')).toBe(false);
  });

  it('should extract URL from quoted literal', () => {
    expect(extractUrlFromQuotedLiteral("'https://example.com'")).toBe('https://example.com');
    expect(extractUrlFromQuotedLiteral('"r2://bucket/path"')).toBe('r2://bucket/path');
    expect(extractUrlFromQuotedLiteral('users')).toBeNull();
  });
});

describe('URL Function Parsing', () => {
  it('should detect url() function', () => {
    expect(isUrlFunction("url('https://example.com')")).toBe(true);
    expect(isUrlFunction("URL('https://example.com')")).toBe(true);
    expect(isUrlFunction("url('https://example.com', 'JSON')")).toBe(true);
    expect(isUrlFunction('users')).toBe(false);
    expect(isUrlFunction("'https://example.com'")).toBe(false);
  });

  it('should parse url() with single argument', () => {
    const result = parseUrlFunction("url('https://api.example.com/data.json')");

    expect(result).not.toBeNull();
    expect(result!.url).toBe('https://api.example.com/data.json');
    expect(result!.format).toBeUndefined();
  });

  it('should parse url() with format argument', () => {
    const result = parseUrlFunction("url('https://api.example.com/data', 'CSV')");

    expect(result).not.toBeNull();
    expect(result!.url).toBe('https://api.example.com/data');
    expect(result!.format).toBe('csv');
  });

  it('should handle case-insensitive URL keyword', () => {
    const result1 = parseUrlFunction("URL('https://example.com')");
    const result2 = parseUrlFunction("Url('https://example.com')");

    expect(result1).not.toBeNull();
    expect(result2).not.toBeNull();
  });

  it('should return null for invalid url() calls', () => {
    expect(parseUrlFunction('foo()')).toBeNull();
    expect(parseUrlFunction('url()')).toBeNull();
    expect(parseUrlFunction('url(123)')).toBeNull();
  });
});

describe('WITH Clause Extraction', () => {
  it('should extract WITH clause from SQL', () => {
    const sql = "'https://example.com/data.csv' WITH (format='csv', headers=true)";
    const { withClause, remainingSql } = extractWithClause(sql);

    expect(withClause).toBe("(format='csv', headers=true)");
    expect(remainingSql).toBe("'https://example.com/data.csv'");
  });

  it('should handle SQL without WITH clause', () => {
    const sql = "'https://example.com/data.json'";
    const { withClause, remainingSql } = extractWithClause(sql);

    expect(withClause).toBeNull();
    expect(remainingSql).toBe(sql);
  });

  it('should handle nested parentheses', () => {
    const sql = "'https://example.com' WITH (filter=(a > 1))";
    const { withClause, remainingSql } = extractWithClause(sql);

    expect(withClause).toBe('(filter=(a > 1))');
  });
});

describe('Virtual Table FROM Clause Parsing', () => {
  it('should parse quoted URL literal', () => {
    const result = parseVirtualTableFromClause("'https://api.example.com/users.json'");

    expect(result.isVirtualTable).toBe(true);
    expect(result.source?.type).toBe('url-literal');
    expect(result.source?.url).toBe('https://api.example.com/users.json');
    expect(result.source?.format).toBe('json');
  });

  it('should parse url() function', () => {
    const result = parseVirtualTableFromClause("url('https://api.example.com/data', 'CSV')");

    expect(result.isVirtualTable).toBe(true);
    expect(result.source?.type).toBe('url-function');
    expect(result.source?.url).toBe('https://api.example.com/data');
    expect(result.source?.format).toBe('csv');
  });

  it('should parse with alias using AS', () => {
    const result = parseVirtualTableFromClause("'https://example.com/users.json' AS users");

    expect(result.isVirtualTable).toBe(true);
    expect(result.source?.alias).toBe('users');
  });

  it('should parse with implicit alias', () => {
    const result = parseVirtualTableFromClause("'https://example.com/data.json' data");

    expect(result.isVirtualTable).toBe(true);
    expect(result.source?.alias).toBe('data');
  });

  it('should not confuse keywords with alias', () => {
    const result = parseVirtualTableFromClause("'https://example.com/users.json' WHERE id = 1");

    expect(result.isVirtualTable).toBe(true);
    expect(result.source?.alias).toBeUndefined();
    expect(result.remainingSql).toContain('WHERE');
  });

  it('should reject non-URL sources', () => {
    const result = parseVirtualTableFromClause('users');

    expect(result.isVirtualTable).toBe(false);
    expect(result.source).toBeUndefined();
  });
});

describe('Full SELECT Parsing', () => {
  it('should parse SELECT with virtual table', () => {
    const sql = "SELECT * FROM 'https://api.example.com/users.json'";
    const result = parseSelectWithVirtualTable(sql);

    expect(result.hasVirtualTable).toBe(true);
    expect(result.source?.url).toBe('https://api.example.com/users.json');
    expect(result.selectColumns).toBe('*');
  });

  it('should parse SELECT with specific columns', () => {
    const sql = "SELECT id, name, email FROM 'https://api.example.com/users.json'";
    const result = parseSelectWithVirtualTable(sql);

    expect(result.hasVirtualTable).toBe(true);
    expect(result.selectColumns).toBe('id, name, email');
  });

  it('should parse SELECT with WHERE clause', () => {
    const sql = "SELECT * FROM 'r2://bucket/sales.parquet' WHERE year = 2024";
    const result = parseSelectWithVirtualTable(sql);

    expect(result.hasVirtualTable).toBe(true);
    expect(result.whereClause).toBe('year = 2024');
  });

  it('should parse SELECT with ORDER BY', () => {
    const sql = "SELECT * FROM 'https://api.example.com/data.json' ORDER BY created_at DESC";
    const result = parseSelectWithVirtualTable(sql);

    expect(result.hasVirtualTable).toBe(true);
    expect(result.orderByClause).toBe('created_at DESC');
  });

  it('should parse SELECT with LIMIT', () => {
    const sql = "SELECT * FROM 'https://api.example.com/data.json' LIMIT 10";
    const result = parseSelectWithVirtualTable(sql);

    expect(result.hasVirtualTable).toBe(true);
    expect(result.limitClause).toContain('LIMIT 10');
  });

  it('should parse complex SELECT', () => {
    const sql = `SELECT id, name
                 FROM 'https://api.example.com/users.json'
                 WHERE active = true
                 ORDER BY created_at DESC
                 LIMIT 100`;
    const result = parseSelectWithVirtualTable(sql);

    expect(result.hasVirtualTable).toBe(true);
    expect(result.whereClause).toBe('active = true');
    expect(result.orderByClause).toBe('created_at DESC');
    expect(result.limitClause).toContain('LIMIT 100');
  });

  it('should handle regular table (non-virtual)', () => {
    const sql = 'SELECT * FROM users WHERE id = 1';
    const result = parseSelectWithVirtualTable(sql);

    expect(result.hasVirtualTable).toBe(false);
  });
});

// =============================================================================
// FORMAT AUTO-DETECTION TESTS
// =============================================================================

describe('Format Auto-detection', () => {
  const originalFetch = global.fetch;
  let mockFetch: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    mockFetch = vi.fn();
    global.fetch = mockFetch;
  });

  afterEach(() => {
    global.fetch = originalFetch;
  });

  it('should auto-detect JSON from extension', () => {
    const table = createURLVirtualTable('https://api.example.com/data.json');
    expect(table.format).toBe('json');
  });

  it('should auto-detect CSV from extension', () => {
    const table = createURLVirtualTable('https://data.gov/dataset.csv');
    expect(table.format).toBe('csv');
  });

  it('should auto-detect NDJSON from extension', () => {
    const table1 = createURLVirtualTable('https://stream.example.com/events.ndjson');
    expect(table1.format).toBe('ndjson');

    const table2 = createURLVirtualTable('https://stream.example.com/events.jsonl');
    expect(table2.format).toBe('ndjson');
  });

  it('should auto-detect Parquet from extension', () => {
    const table = createURLVirtualTable('https://lakehouse.example.com/table.parquet');
    expect(table.format).toBe('parquet');
  });

  it('should use Content-Type when no extension', async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      headers: new Headers({ 'Content-Type': 'text/csv' }),
      text: vi.fn().mockResolvedValue('id,name\n1,Alice'),
    });

    const table = createURLVirtualTable('https://api.example.com/data');
    const records: Record<string, unknown>[] = [];

    // Default format is json, but content-type will override during scan
    for await (const record of table.scan()) {
      records.push(record);
    }

    expect(records).toHaveLength(1);
  });

  it('should default to JSON when no format detected', () => {
    const table = createURLVirtualTable('https://api.example.com/users');
    expect(table.format).toBe('json');
  });
});

// =============================================================================
// TYPE-LEVEL TESTS (Parser Module)
// =============================================================================

import type {
  IsVirtualTableFrom,
  ExtractVirtualTableUrl as ExtractUrlType,
  InferVirtualTableFormat as InferFormatType,
} from '../parser/virtual.js';

describe('Type-Level Virtual Table Parsing', () => {
  it('should detect virtual table in FROM clause at compile time', () => {
    type IsVirtual1 = IsVirtualTableFrom<"'https://example.com/data.json'">;
    type _Test1 = Expect<Equal<IsVirtual1, true>>;

    type IsVirtual2 = IsVirtualTableFrom<'"r2://bucket/file.csv" AS data'>;
    type _Test2 = Expect<Equal<IsVirtual2, true>>;

    type IsVirtual3 = IsVirtualTableFrom<"url('https://api.example.com')">;
    type _Test3 = Expect<Equal<IsVirtual3, true>>;

    type NotVirtual = IsVirtualTableFrom<'users'>;
    type _TestNot = Expect<Equal<NotVirtual, false>>;

    expect(true).toBe(true);
  });

  it('should extract URL from FROM clause at compile time', () => {
    type Url1 = ExtractUrlType<"'https://api.example.com/users.json'">;
    type _TestUrl1 = Expect<Equal<Url1, 'https://api.example.com/users.json'>>;

    type Url2 = ExtractUrlType<'"r2://mybucket/data.parquet" AS sales'>;
    type _TestUrl2 = Expect<Equal<Url2, 'r2://mybucket/data.parquet'>>;

    expect(true).toBe(true);
  });

  it('should infer format from URL at compile time', () => {
    type Format1 = InferFormatType<'https://api.example.com/data.json'>;
    type _TestFormat1 = Expect<Equal<Format1, 'json'>>;

    type Format2 = InferFormatType<'r2://bucket/data.csv'>;
    type _TestFormat2 = Expect<Equal<Format2, 'csv'>>;

    type Format3 = InferFormatType<'https://stream.example.com/events.ndjson'>;
    type _TestFormat3 = Expect<Equal<Format3, 'ndjson'>>;

    type Format4 = InferFormatType<'r2://lakehouse/table.parquet'>;
    type _TestFormat4 = Expect<Equal<Format4, 'parquet'>>;

    expect(true).toBe(true);
  });
});

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

describe('Integration Tests', () => {
  const originalFetch = global.fetch;
  let mockFetch: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    mockFetch = vi.fn();
    global.fetch = mockFetch;
  });

  afterEach(() => {
    global.fetch = originalFetch;
  });

  it('should execute complete flow: parse SQL -> create table -> scan', async () => {
    const sql = "SELECT id, name FROM 'https://api.example.com/users.json' WHERE active = true LIMIT 10";

    const jsonData = [
      { id: 1, name: 'Alice', active: true },
      { id: 2, name: 'Bob', active: false },
      { id: 3, name: 'Charlie', active: true },
    ];

    mockFetch.mockResolvedValue({
      ok: true,
      headers: new Headers({ 'Content-Type': 'application/json' }),
      text: vi.fn().mockResolvedValue(JSON.stringify(jsonData)),
    });

    // Step 1: Parse SQL
    const parsed = parseSelectWithVirtualTable(sql);
    expect(parsed.hasVirtualTable).toBe(true);
    expect(parsed.source?.url).toBe('https://api.example.com/users.json');

    // Step 2: Create virtual table
    const table = createURLVirtualTable(parsed.source!.url);

    // Step 3: Scan with projection (simulating the query)
    const columns = parsed.selectColumns.split(',').map(c => c.trim());
    const records: Record<string, unknown>[] = [];

    for await (const record of table.scan({ projection: columns, limit: 10 })) {
      records.push(record);
    }

    expect(records).toHaveLength(3);
    expect(Object.keys(records[0])).toEqual(['id', 'name']);
  });

  it('should handle CSV with custom options from WITH clause', async () => {
    const sql = "SELECT * FROM 'https://data.gov/dataset.csv' WITH (headers=true, delimiter=';')";

    const csvData = 'id;name;value\n1;Alice;100\n2;Bob;200';

    mockFetch.mockResolvedValue({
      ok: true,
      headers: new Headers({ 'Content-Type': 'text/csv' }),
      text: vi.fn().mockResolvedValue(csvData),
    });

    // Parse SQL
    const parsed = parseSelectWithVirtualTable(sql);
    expect(parsed.hasVirtualTable).toBe(true);

    // Get WITH options
    const withOptions = parsed.source?.withOptions;
    expect(withOptions?.headers).toBe(true);
    expect(withOptions?.delimiter).toBe(';');

    // Create table with options
    const urlOptions = withClauseToOptions(withOptions!);
    const table = createURLVirtualTable(parsed.source!.url, {
      ...urlOptions,
      format: 'csv',
    });

    const records: Record<string, unknown>[] = [];
    for await (const record of table.scan()) {
      records.push(record);
    }

    expect(records).toHaveLength(2);
    expect(records[0]).toHaveProperty('id');
    expect(records[0]).toHaveProperty('name');
    expect(records[0]).toHaveProperty('value');
  });
});
