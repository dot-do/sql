/**
 * Tests for virtual table parser module
 *
 * Verifies parsing of virtual table sources in SQL queries:
 * - URL literal detection (quoted URLs with various schemes)
 * - url() function parsing with format arguments
 * - WITH clause extraction and option parsing
 * - FROM clause parsing for virtual table sources
 * - Full SELECT statement parsing with virtual tables
 */

import { describe, it, expect } from 'vitest';
import {
  isValidUrlScheme,
  isQuotedUrlLiteral,
  extractUrlFromQuotedLiteral,
  isUrlFunction,
  parseUrlFunction,
  extractVirtualTableWithClause,
  parseWithClauseOptions,
  parseVirtualTableFromClause,
  parseSelectWithVirtualTable,
} from '../virtual.js';

// =============================================================================
// URL Literal Detection
// =============================================================================

describe('Virtual Table Parser - URL Scheme Detection', () => {
  it('should recognize https:// scheme', () => {
    expect(isValidUrlScheme('https://example.com/data.json')).toBe(true);
  });

  it('should recognize http:// scheme', () => {
    expect(isValidUrlScheme('http://example.com/data.json')).toBe(true);
  });

  it('should recognize r2:// scheme', () => {
    expect(isValidUrlScheme('r2://bucket/path/data.parquet')).toBe(true);
  });

  it('should recognize s3:// scheme', () => {
    expect(isValidUrlScheme('s3://bucket/path/data.csv')).toBe(true);
  });

  it('should recognize file:// scheme', () => {
    expect(isValidUrlScheme('file:///path/to/data.json')).toBe(true);
  });

  it('should reject invalid schemes', () => {
    expect(isValidUrlScheme('ftp://example.com')).toBe(false);
    expect(isValidUrlScheme('example.com')).toBe(false);
    expect(isValidUrlScheme('users')).toBe(false);
    expect(isValidUrlScheme('')).toBe(false);
  });
});

describe('Virtual Table Parser - Quoted URL Literal', () => {
  it('should detect single-quoted URL literal', () => {
    expect(isQuotedUrlLiteral("'https://example.com/data.json'")).toBe(true);
  });

  it('should detect double-quoted URL literal', () => {
    expect(isQuotedUrlLiteral('"https://example.com/data.json"')).toBe(true);
  });

  it('should handle whitespace around quoted URL', () => {
    expect(isQuotedUrlLiteral("  'https://example.com/data.json'  ")).toBe(true);
  });

  it('should reject non-URL quoted strings', () => {
    expect(isQuotedUrlLiteral("'users'")).toBe(false);
    expect(isQuotedUrlLiteral('"my_table"')).toBe(false);
  });

  it('should reject unquoted URLs', () => {
    expect(isQuotedUrlLiteral('https://example.com')).toBe(false);
  });

  it('should reject mismatched quotes', () => {
    expect(isQuotedUrlLiteral("'https://example.com\"")).toBe(false);
  });
});

describe('Virtual Table Parser - Extract URL from Quoted Literal', () => {
  it('should extract URL from single-quoted literal', () => {
    expect(extractUrlFromQuotedLiteral("'https://example.com/data.json'")).toBe(
      'https://example.com/data.json',
    );
  });

  it('should extract URL from double-quoted literal', () => {
    expect(extractUrlFromQuotedLiteral('"r2://bucket/data.parquet"')).toBe(
      'r2://bucket/data.parquet',
    );
  });

  it('should return null for unquoted strings', () => {
    expect(extractUrlFromQuotedLiteral('https://example.com')).toBeNull();
  });

  it('should handle whitespace around the literal', () => {
    expect(extractUrlFromQuotedLiteral("  'https://api.example.com/v1/data'  ")).toBe(
      'https://api.example.com/v1/data',
    );
  });
});

// =============================================================================
// URL Function Parsing
// =============================================================================

describe('Virtual Table Parser - URL Function Detection', () => {
  it('should detect url() function call', () => {
    expect(isUrlFunction("url('https://example.com')")).toBe(true);
  });

  it('should detect url() with whitespace', () => {
    expect(isUrlFunction("  url('https://example.com')  ")).toBe(true);
  });

  it('should be case-insensitive', () => {
    expect(isUrlFunction("URL('https://example.com')")).toBe(true);
    expect(isUrlFunction("Url('https://example.com')")).toBe(true);
  });

  it('should reject non-url function calls', () => {
    expect(isUrlFunction("count(*)")).toBe(false);
    expect(isUrlFunction("'https://example.com'")).toBe(false);
  });
});

describe('Virtual Table Parser - URL Function Parsing', () => {
  it('should parse url() with single-quoted URL', () => {
    const result = parseUrlFunction("url('https://example.com/data.json')");
    expect(result).not.toBeNull();
    expect(result!.url).toBe('https://example.com/data.json');
    expect(result!.format).toBeUndefined();
  });

  it('should parse url() with double-quoted URL', () => {
    const result = parseUrlFunction('url("https://example.com/data.csv")');
    expect(result).not.toBeNull();
    expect(result!.url).toBe('https://example.com/data.csv');
  });

  it('should parse url() with format argument - JSON', () => {
    const result = parseUrlFunction("url('https://example.com/data', 'JSON')");
    expect(result).not.toBeNull();
    expect(result!.url).toBe('https://example.com/data');
    expect(result!.format).toBe('json');
  });

  it('should parse url() with format argument - CSV', () => {
    const result = parseUrlFunction("url('https://example.com/data', 'CSV')");
    expect(result).not.toBeNull();
    expect(result!.format).toBe('csv');
  });

  it('should parse url() with format argument - NDJSON', () => {
    const result = parseUrlFunction("url('https://example.com/data', 'ndjson')");
    expect(result).not.toBeNull();
    expect(result!.format).toBe('ndjson');
  });

  it('should parse url() with format argument - JSONL (alias for ndjson)', () => {
    const result = parseUrlFunction("url('https://example.com/data', 'jsonl')");
    expect(result).not.toBeNull();
    expect(result!.format).toBe('ndjson');
  });

  it('should parse url() with format argument - Parquet', () => {
    const result = parseUrlFunction("url('https://example.com/data', 'parquet')");
    expect(result).not.toBeNull();
    expect(result!.format).toBe('parquet');
  });

  it('should return null for malformed url() calls', () => {
    expect(parseUrlFunction("url(https://example.com)")).toBeNull();
    expect(parseUrlFunction("url()")).toBeNull();
    expect(parseUrlFunction("not_url('x')")).toBeNull();
  });
});

// =============================================================================
// WITH Clause Parsing
// =============================================================================

describe('Virtual Table Parser - WITH Clause Extraction', () => {
  it('should extract WITH clause from SQL', () => {
    const sql = "'https://example.com' WITH (format='csv')";
    const result = extractVirtualTableWithClause(sql);
    expect(result.withClause).not.toBeNull();
    expect(result.withClause).toContain("format='csv'");
  });

  it('should return null withClause when no WITH present', () => {
    const sql = "'https://example.com/data.json'";
    const result = extractVirtualTableWithClause(sql);
    expect(result.withClause).toBeNull();
    expect(result.remainingSql).toBe(sql);
  });

  it('should handle nested parentheses in WITH clause', () => {
    const sql = "'https://example.com' WITH (format='csv', headers=true)";
    const result = extractVirtualTableWithClause(sql);
    expect(result.withClause).not.toBeNull();
  });

  it('should handle unbalanced parentheses gracefully', () => {
    const sql = "'https://example.com' WITH (format='csv'";
    const result = extractVirtualTableWithClause(sql);
    expect(result.withClause).toBeNull();
  });
});

describe('Virtual Table Parser - WITH Clause Options', () => {
  it('should parse format option', () => {
    const options = parseWithClauseOptions("(format='csv')");
    expect(options.format).toBe('csv');
  });

  it('should parse headers option', () => {
    const options = parseWithClauseOptions("(headers=true)");
    expect(options.headers).toBe(true);
  });

  it('should parse false headers', () => {
    const options = parseWithClauseOptions("(headers=false)");
    expect(options.headers).toBe(false);
  });

  it('should parse delimiter option', () => {
    const options = parseWithClauseOptions("(delimiter=',')");
    expect(options.delimiter).toBe(',');
  });

  it('should parse timeout option', () => {
    const options = parseWithClauseOptions('(timeout=5000)');
    expect(options.timeout).toBe(5000);
  });

  it('should parse cache option', () => {
    const options = parseWithClauseOptions('(cache=true)');
    expect(options.cache).toBe(true);
  });

  it('should parse cacheTtl option', () => {
    const options = parseWithClauseOptions('(cacheTtl=60000)');
    expect(options.cacheTtl).toBe(60000);
  });

  it('should parse cache_ttl alternative key', () => {
    const options = parseWithClauseOptions('(cache_ttl=30000)');
    expect(options.cacheTtl).toBe(30000);
  });

  it('should parse auth options', () => {
    const options = parseWithClauseOptions("(auth=bearer, token='my-token')");
    expect(options.auth).toBe('bearer');
    expect(options.token).toBe('my-token');
  });

  it('should parse multiple options', () => {
    const options = parseWithClauseOptions("(format='json', headers=true, timeout=3000)");
    expect(options.format).toBe('json');
    expect(options.headers).toBe(true);
    expect(options.timeout).toBe(3000);
  });

  it('should parse api key options', () => {
    const options = parseWithClauseOptions("(apikey='abc123')");
    expect(options.apiKeyValue).toBe('abc123');
  });

  it('should parse api_key_header option', () => {
    const options = parseWithClauseOptions("(api_key_header='X-API-Key')");
    expect(options.apiKeyHeader).toBe('X-API-Key');
  });

  it('should parse username and password', () => {
    const options = parseWithClauseOptions("(username='user', password='pass')");
    expect(options.username).toBe('user');
    expect(options.password).toBe('pass');
  });
});

// =============================================================================
// FROM Clause Parsing
// =============================================================================

describe('Virtual Table Parser - FROM Clause Parsing', () => {
  it('should parse quoted URL literal as virtual table source', () => {
    const result = parseVirtualTableFromClause("'https://example.com/data.json'");
    expect(result.isVirtualTable).toBe(true);
    expect(result.source).toBeDefined();
    expect(result.source!.type).toBe('url-literal');
    expect(result.source!.url).toBe('https://example.com/data.json');
  });

  it('should detect format from URL extension', () => {
    const result = parseVirtualTableFromClause("'https://example.com/data.csv'");
    expect(result.isVirtualTable).toBe(true);
    expect(result.source!.format).toBe('csv');
  });

  it('should detect parquet format from URL extension', () => {
    const result = parseVirtualTableFromClause("'r2://bucket/data.parquet'");
    expect(result.isVirtualTable).toBe(true);
    expect(result.source!.format).toBe('parquet');
  });

  it('should parse url() function as virtual table source', () => {
    const result = parseVirtualTableFromClause("url('https://api.example.com/data', 'JSON')");
    expect(result.isVirtualTable).toBe(true);
    expect(result.source!.type).toBe('url-function');
    expect(result.source!.url).toBe('https://api.example.com/data');
    expect(result.source!.format).toBe('json');
  });

  it('should parse virtual table with AS alias', () => {
    const result = parseVirtualTableFromClause("'https://example.com/data.json' AS data");
    expect(result.isVirtualTable).toBe(true);
    expect(result.source!.alias).toBe('data');
  });

  it('should parse virtual table with WITH clause', () => {
    const result = parseVirtualTableFromClause(
      "'https://example.com/data' WITH (format='csv', headers=true)",
    );
    expect(result.isVirtualTable).toBe(true);
    expect(result.source!.withOptions).toBeDefined();
    expect(result.source!.withOptions!.format).toBe('csv');
    expect(result.source!.withOptions!.headers).toBe(true);
  });

  it('should return isVirtualTable=false for regular table names', () => {
    const result = parseVirtualTableFromClause('users');
    expect(result.isVirtualTable).toBe(false);
  });

  it('should return isVirtualTable=false for non-URL quoted strings', () => {
    const result = parseVirtualTableFromClause("'hello world'");
    expect(result.isVirtualTable).toBe(false);
  });
});

// =============================================================================
// Full SELECT Statement Parsing
// =============================================================================

describe('Virtual Table Parser - Full SELECT Parsing', () => {
  it('should parse SELECT with virtual table FROM clause', () => {
    const result = parseSelectWithVirtualTable(
      "SELECT * FROM 'https://example.com/data.json'",
    );
    expect(result.hasVirtualTable).toBe(true);
    expect(result.source).toBeDefined();
    expect(result.source!.url).toBe('https://example.com/data.json');
    expect(result.selectColumns).toBe('*');
  });

  it('should parse SELECT columns', () => {
    const result = parseSelectWithVirtualTable(
      "SELECT id, name FROM 'https://example.com/users.json'",
    );
    expect(result.hasVirtualTable).toBe(true);
    expect(result.selectColumns).toBe('id, name');
  });

  it('should parse WHERE clause', () => {
    const result = parseSelectWithVirtualTable(
      "SELECT * FROM 'https://example.com/data.json' WHERE age > 21",
    );
    expect(result.hasVirtualTable).toBe(true);
    expect(result.whereClause).toBeDefined();
    expect(result.whereClause).toContain('age');
  });

  it('should parse ORDER BY clause', () => {
    const result = parseSelectWithVirtualTable(
      "SELECT * FROM 'https://example.com/data.json' ORDER BY name",
    );
    expect(result.hasVirtualTable).toBe(true);
    expect(result.orderByClause).toBeDefined();
    expect(result.orderByClause).toContain('name');
  });

  it('should parse LIMIT clause', () => {
    const result = parseSelectWithVirtualTable(
      "SELECT * FROM 'https://example.com/data.json' LIMIT 10",
    );
    expect(result.hasVirtualTable).toBe(true);
    expect(result.limitClause).toBeDefined();
    expect(result.limitClause).toContain('10');
  });

  it('should parse GROUP BY clause', () => {
    const result = parseSelectWithVirtualTable(
      "SELECT department, COUNT(*) FROM 'https://example.com/data.json' GROUP BY department",
    );
    expect(result.hasVirtualTable).toBe(true);
    expect(result.groupByClause).toBeDefined();
    expect(result.groupByClause).toContain('department');
  });

  it('should return hasVirtualTable=false for regular SELECT', () => {
    const result = parseSelectWithVirtualTable('SELECT * FROM users');
    expect(result.hasVirtualTable).toBe(false);
  });

  it('should return hasVirtualTable=false for SELECT without FROM', () => {
    const result = parseSelectWithVirtualTable('SELECT 1 + 1');
    expect(result.hasVirtualTable).toBe(false);
  });

  it('should parse SELECT with url() function', () => {
    const result = parseSelectWithVirtualTable(
      "SELECT * FROM url('https://api.example.com/data', 'JSON')",
    );
    expect(result.hasVirtualTable).toBe(true);
    expect(result.source!.type).toBe('url-function');
    expect(result.source!.format).toBe('json');
  });
});
