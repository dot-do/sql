/**
 * Virtual Table Parser Support
 *
 * Provides parsing support for virtual table sources in SQL queries:
 * - Detect quoted string as table source: 'https://...' or "r2://..."
 * - Parse WITH clause for options: WITH (format='csv', headers=true)
 * - Parse url() function: url('https://...', 'JSON')
 *
 * @packageDocumentation
 */

import type { VirtualTableFormat, WithClauseOptions } from '../virtual/types.js';

// =============================================================================
// PARSED VIRTUAL TABLE SOURCE
// =============================================================================

/**
 * Parsed virtual table source from SQL
 */
export interface ParsedVirtualTableSource {
  /** Type of source (literal or function) */
  type: 'url-literal' | 'url-function';

  /** The URL to fetch */
  url: string;

  /** Detected or specified format */
  format?: VirtualTableFormat;

  /** WITH clause options */
  withOptions?: WithClauseOptions;

  /** Table alias (if specified) */
  alias?: string;
}

/**
 * Result of parsing a FROM clause for virtual table
 */
export interface VirtualTableParseResult {
  /** Whether the FROM clause contains a virtual table source */
  isVirtualTable: boolean;

  /** Parsed virtual table source (if found) */
  source?: ParsedVirtualTableSource;

  /** Remaining SQL after FROM clause (WHERE, ORDER BY, etc.) */
  remainingSql?: string;

  /** Parse error message (if any) */
  error?: string;
}

// =============================================================================
// URL LITERAL DETECTION
// =============================================================================

/**
 * Check if a string is a valid URL scheme
 */
export function isValidUrlScheme(str: string): boolean {
  return (
    str.startsWith('https://') ||
    str.startsWith('http://') ||
    str.startsWith('r2://') ||
    str.startsWith('s3://') ||
    str.startsWith('file://')
  );
}

/**
 * Check if a string is a quoted URL literal
 */
export function isQuotedUrlLiteral(str: string): boolean {
  const trimmed = str.trim();

  // Single quoted
  if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
    const inner = trimmed.slice(1, -1);
    return isValidUrlScheme(inner);
  }

  // Double quoted
  if (trimmed.startsWith('"') && trimmed.endsWith('"')) {
    const inner = trimmed.slice(1, -1);
    return isValidUrlScheme(inner);
  }

  return false;
}

/**
 * Extract URL from quoted literal
 */
export function extractUrlFromQuotedLiteral(str: string): string | null {
  const trimmed = str.trim();

  if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
    return trimmed.slice(1, -1);
  }

  if (trimmed.startsWith('"') && trimmed.endsWith('"')) {
    return trimmed.slice(1, -1);
  }

  return null;
}

// =============================================================================
// URL FUNCTION PARSING
// =============================================================================

/**
 * Check if string is a url() function call
 */
export function isUrlFunction(str: string): boolean {
  const trimmed = str.trim().toLowerCase();
  return trimmed.startsWith('url(') && trimmed.endsWith(')');
}

/**
 * Parse url() function call
 *
 * Supports:
 * - url('https://...')
 * - url('https://...', 'JSON')
 * - url("https://...", "CSV")
 */
export function parseUrlFunction(str: string): { url: string; format?: VirtualTableFormat } | null {
  const match = str
    .trim()
    .match(/^url\s*\(\s*(['"])([^'"]+)\1(?:\s*,\s*(['"])([^'"]+)\3)?\s*\)$/i);

  if (!match) {
    return null;
  }

  const url = match[2];
  const formatStr = match[4]?.toLowerCase();

  let format: VirtualTableFormat | undefined;
  if (formatStr) {
    switch (formatStr) {
      case 'json':
        format = 'json';
        break;
      case 'ndjson':
      case 'jsonl':
        format = 'ndjson';
        break;
      case 'csv':
        format = 'csv';
        break;
      case 'parquet':
        format = 'parquet';
        break;
    }
  }

  return { url, format };
}

// =============================================================================
// WITH CLAUSE PARSING
// =============================================================================

/**
 * Extract WITH clause from SQL
 *
 * Returns the WITH clause content and the remaining SQL
 */
export function extractWithClause(sql: string): {
  withClause: string | null;
  remainingSql: string;
} {
  // Match WITH followed by parentheses
  // Handle nested parentheses properly
  const withMatch = sql.match(/\bWITH\s*\(/i);

  if (!withMatch) {
    return { withClause: null, remainingSql: sql };
  }

  const startIndex = withMatch.index!;
  const withKeywordEnd = startIndex + withMatch[0].length;

  // Find matching closing parenthesis
  let depth = 1;
  let endIndex = withKeywordEnd;

  for (let i = withKeywordEnd; i < sql.length && depth > 0; i++) {
    if (sql[i] === '(') {
      depth++;
    } else if (sql[i] === ')') {
      depth--;
      if (depth === 0) {
        endIndex = i + 1;
      }
    }
  }

  if (depth !== 0) {
    // Unbalanced parentheses
    return { withClause: null, remainingSql: sql };
  }

  // Extract WITH clause content (without WITH keyword)
  const withClause = sql.slice(withKeywordEnd - 1, endIndex); // Include opening paren
  const remainingSql = sql.slice(0, startIndex).trim() + ' ' + sql.slice(endIndex).trim();

  return {
    withClause: withClause.trim(),
    remainingSql: remainingSql.trim(),
  };
}

/**
 * Parse WITH clause options
 */
export function parseWithClauseOptions(withClause: string): WithClauseOptions {
  const options: WithClauseOptions = {};

  // Remove parentheses
  let content = withClause.trim();
  if (content.startsWith('(') && content.endsWith(')')) {
    content = content.slice(1, -1);
  }

  // Split by comma (respecting quoted values)
  const pairs = splitByComma(content);

  for (const pair of pairs) {
    const [key, value] = parseKeyValue(pair);
    const keyLower = key.toLowerCase();

    switch (keyLower) {
      case 'format':
        options.format = parseFormatValue(value);
        break;
      case 'headers':
        options.headers = parseBooleanValue(value);
        break;
      case 'delimiter':
        options.delimiter = parseStringValue(value);
        break;
      case 'quote':
        options.quote = parseStringValue(value);
        break;
      case 'timeout':
        options.timeout = parseInt(value, 10);
        break;
      case 'cache':
        options.cache = parseBooleanValue(value);
        break;
      case 'cachettl':
      case 'cache_ttl':
        options.cacheTtl = parseInt(value, 10);
        break;
      case 'auth':
        options.auth = value.toLowerCase() as 'basic' | 'bearer' | 'api-key';
        break;
      case 'username':
        options.username = parseStringValue(value);
        break;
      case 'password':
        options.password = parseStringValue(value);
        break;
      case 'token':
        options.token = parseStringValue(value);
        break;
      case 'apikeyheader':
      case 'api_key_header':
        options.apiKeyHeader = parseStringValue(value);
        break;
      case 'apikeyvalue':
      case 'api_key_value':
      case 'apikey':
      case 'api_key':
        options.apiKeyValue = parseStringValue(value);
        break;
    }
  }

  return options;
}

/**
 * Split string by comma, respecting quoted values
 */
function splitByComma(str: string): string[] {
  const result: string[] = [];
  let current = '';
  let inQuotes = false;
  let quoteChar = '';

  for (let i = 0; i < str.length; i++) {
    const char = str[i];

    if (!inQuotes && (char === "'" || char === '"')) {
      inQuotes = true;
      quoteChar = char;
      current += char;
    } else if (inQuotes && char === quoteChar) {
      inQuotes = false;
      current += char;
    } else if (!inQuotes && char === ',') {
      if (current.trim()) {
        result.push(current.trim());
      }
      current = '';
    } else {
      current += char;
    }
  }

  if (current.trim()) {
    result.push(current.trim());
  }

  return result;
}

/**
 * Parse key=value pair
 */
function parseKeyValue(pair: string): [string, string] {
  const eqIndex = pair.indexOf('=');
  if (eqIndex === -1) {
    return [pair.trim(), 'true'];
  }
  return [pair.slice(0, eqIndex).trim(), pair.slice(eqIndex + 1).trim()];
}

/**
 * Parse string value (remove quotes)
 */
function parseStringValue(value: string): string {
  if ((value.startsWith("'") && value.endsWith("'")) ||
      (value.startsWith('"') && value.endsWith('"'))) {
    return value.slice(1, -1);
  }
  return value;
}

/**
 * Parse boolean value
 */
function parseBooleanValue(value: string): boolean {
  const lower = value.toLowerCase();
  return lower === 'true' || lower === '1' || lower === 'yes';
}

/**
 * Parse format value
 */
function parseFormatValue(value: string): VirtualTableFormat | undefined {
  const lower = parseStringValue(value).toLowerCase();
  switch (lower) {
    case 'json':
      return 'json';
    case 'ndjson':
    case 'jsonl':
      return 'ndjson';
    case 'csv':
      return 'csv';
    case 'parquet':
      return 'parquet';
    default:
      return undefined;
  }
}

// =============================================================================
// FROM CLAUSE PARSING
// =============================================================================

/**
 * Parse FROM clause for virtual table source
 *
 * Handles:
 * - Quoted URL literals: FROM 'https://...'
 * - url() function: FROM url('https://...', 'JSON')
 * - WITH clause: FROM 'https://...' WITH (format='csv')
 * - Alias: FROM 'https://...' AS data
 */
export function parseVirtualTableFromClause(fromClause: string): VirtualTableParseResult {
  let sql = fromClause.trim();

  // Extract WITH clause first
  const { withClause, remainingSql } = extractWithClause(sql);
  sql = remainingSql;

  // Parse WITH options if present
  const withOptions = withClause ? parseWithClauseOptions(withClause) : undefined;

  // Extract alias (AS clause or implicit)
  const { source: sourceStr, alias, remaining } = extractAliasFromSource(sql);

  // Try url() function first
  if (isUrlFunction(sourceStr)) {
    const parsed = parseUrlFunction(sourceStr);
    if (parsed) {
      return {
        isVirtualTable: true,
        source: {
          type: 'url-function',
          url: parsed.url,
          format: parsed.format || withOptions?.format,
          withOptions,
          alias,
        },
        remainingSql: remaining,
      };
    }
  }

  // Try quoted URL literal
  if (isQuotedUrlLiteral(sourceStr)) {
    const url = extractUrlFromQuotedLiteral(sourceStr);
    if (url) {
      return {
        isVirtualTable: true,
        source: {
          type: 'url-literal',
          url,
          format: withOptions?.format || detectFormatFromUrl(url),
          withOptions,
          alias,
        },
        remainingSql: remaining,
      };
    }
  }

  // Not a virtual table source
  return {
    isVirtualTable: false,
    remainingSql: fromClause,
  };
}

/**
 * Extract alias and remaining SQL from source string
 */
function extractAliasFromSource(sql: string): {
  source: string;
  alias?: string;
  remaining: string;
} {
  // Look for AS keyword (case-insensitive)
  const asMatch = sql.match(/^(.+?)\s+AS\s+(\w+)(?:\s+(.*))?$/i);
  if (asMatch) {
    return {
      source: asMatch[1].trim(),
      alias: asMatch[2],
      remaining: asMatch[3]?.trim() || '',
    };
  }

  // Look for implicit alias (word after source before WHERE/JOIN/etc.)
  // This is more complex - need to handle quoted strings
  const source = extractTableSource(sql);
  const afterSource = sql.slice(source.length).trim();

  // Check for implicit alias (identifier before keywords)
  const implicitMatch = afterSource.match(/^(\w+)(?:\s+(.*))?$/i);
  if (implicitMatch) {
    const potentialAlias = implicitMatch[1].toUpperCase();
    const keywords = ['WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'CROSS',
                      'ORDER', 'GROUP', 'HAVING', 'LIMIT', 'OFFSET', 'UNION'];

    if (!keywords.includes(potentialAlias)) {
      return {
        source: source,
        alias: implicitMatch[1],
        remaining: implicitMatch[2]?.trim() || '',
      };
    }
  }

  return {
    source: source,
    remaining: afterSource,
  };
}

/**
 * Extract table source from SQL (handles quoted strings and functions)
 */
function extractTableSource(sql: string): string {
  const trimmed = sql.trim();

  // Check for url() function
  if (trimmed.toLowerCase().startsWith('url(')) {
    let depth = 0;
    for (let i = 0; i < trimmed.length; i++) {
      if (trimmed[i] === '(') depth++;
      else if (trimmed[i] === ')') {
        depth--;
        if (depth === 0) {
          return trimmed.slice(0, i + 1);
        }
      }
    }
  }

  // Check for quoted string
  if (trimmed.startsWith("'")) {
    const endQuote = trimmed.indexOf("'", 1);
    if (endQuote !== -1) {
      return trimmed.slice(0, endQuote + 1);
    }
  }

  if (trimmed.startsWith('"')) {
    const endQuote = trimmed.indexOf('"', 1);
    if (endQuote !== -1) {
      return trimmed.slice(0, endQuote + 1);
    }
  }

  // Regular identifier
  const match = trimmed.match(/^[\w.]+/);
  return match ? match[0] : trimmed;
}

/**
 * Detect format from URL extension
 */
function detectFormatFromUrl(url: string): VirtualTableFormat | undefined {
  const cleanUrl = url.split(/[?#]/)[0];
  const ext = cleanUrl.split('.').pop()?.toLowerCase();

  switch (ext) {
    case 'json':
      return 'json';
    case 'ndjson':
    case 'jsonl':
      return 'ndjson';
    case 'csv':
      return 'csv';
    case 'parquet':
      return 'parquet';
    default:
      return undefined;
  }
}

// =============================================================================
// FULL SQL PARSING
// =============================================================================

/**
 * Parse a SELECT statement to extract virtual table source
 */
export function parseSelectWithVirtualTable(sql: string): {
  hasVirtualTable: boolean;
  source?: ParsedVirtualTableSource;
  selectColumns: string;
  whereClause?: string;
  orderByClause?: string;
  limitClause?: string;
  groupByClause?: string;
  havingClause?: string;
} {
  const upperSql = sql.toUpperCase();

  // Find FROM position
  const fromIndex = upperSql.indexOf(' FROM ');
  if (fromIndex === -1) {
    return {
      hasVirtualTable: false,
      selectColumns: extractSelectColumns(sql),
    };
  }

  // Extract SELECT columns
  const selectPart = sql.slice(0, fromIndex);
  const selectColumns = extractSelectColumns(selectPart);

  // Find the end of FROM clause (next keyword or end)
  const afterFrom = sql.slice(fromIndex + 6).trim();
  const { fromClause, remaining } = extractFromClauseEnd(afterFrom);

  // Parse virtual table from FROM clause
  const virtualTableResult = parseVirtualTableFromClause(fromClause);

  // Combine remaining SQL parts
  const combinedRemaining = [
    remaining,
    virtualTableResult.remainingSql || ''
  ].filter(Boolean).join(' ').trim();

  // Parse remaining clauses
  const { whereClause, orderByClause, limitClause, groupByClause, havingClause } =
    parseRemainingClauses(combinedRemaining);

  return {
    hasVirtualTable: virtualTableResult.isVirtualTable,
    source: virtualTableResult.source,
    selectColumns,
    whereClause,
    orderByClause,
    limitClause,
    groupByClause,
    havingClause,
  };
}

/**
 * Extract SELECT columns from SELECT clause
 */
function extractSelectColumns(selectPart: string): string {
  const match = selectPart.match(/SELECT\s+(.*)/i);
  return match ? match[1].trim() : '*';
}

/**
 * Extract FROM clause end (before WHERE, JOIN, ORDER, etc.)
 */
function extractFromClauseEnd(sql: string): {
  fromClause: string;
  remaining: string;
} {
  const upperSql = sql.toUpperCase();
  const keywords = ['WHERE', 'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN',
                    'OUTER JOIN', 'CROSS JOIN', 'ORDER BY', 'GROUP BY',
                    'HAVING', 'LIMIT', 'OFFSET', 'UNION', 'INTERSECT', 'EXCEPT'];

  let minIndex = sql.length;
  let foundKeyword = '';

  for (const keyword of keywords) {
    // Look for keyword as a whole word (preceded by space or start)
    const pattern = new RegExp(`\\s+${keyword.replace(' ', '\\s+')}\\b`, 'i');
    const match = upperSql.match(pattern);
    if (match && match.index !== undefined && match.index < minIndex) {
      minIndex = match.index;
      foundKeyword = keyword;
    }
  }

  return {
    fromClause: sql.slice(0, minIndex).trim(),
    remaining: sql.slice(minIndex).trim(),
  };
}

/**
 * Parse remaining SQL clauses
 */
function parseRemainingClauses(sql: string): {
  whereClause?: string;
  orderByClause?: string;
  limitClause?: string;
  groupByClause?: string;
  havingClause?: string;
} {
  const result: {
    whereClause?: string;
    orderByClause?: string;
    limitClause?: string;
    groupByClause?: string;
    havingClause?: string;
  } = {};

  const trimmedSql = sql.trim();
  if (!trimmedSql) return result;

  // Extract WHERE - capture until next keyword or end
  const whereMatch = trimmedSql.match(/\bWHERE\s+(.+?)(?=\s+(?:ORDER\s+BY|GROUP\s+BY|HAVING|LIMIT)\b|$)/is);
  if (whereMatch) {
    result.whereClause = whereMatch[1].trim();
  }

  // Extract GROUP BY
  const groupByMatch = trimmedSql.match(/\bGROUP\s+BY\s+(.+?)(?=\s+(?:HAVING|ORDER\s+BY|LIMIT)\b|$)/is);
  if (groupByMatch) {
    result.groupByClause = groupByMatch[1].trim();
  }

  // Extract HAVING
  const havingMatch = trimmedSql.match(/\bHAVING\s+(.+?)(?=\s+(?:ORDER\s+BY|LIMIT)\b|$)/is);
  if (havingMatch) {
    result.havingClause = havingMatch[1].trim();
  }

  // Extract ORDER BY
  const orderByMatch = trimmedSql.match(/\bORDER\s+BY\s+(.+?)(?=\s+(?:LIMIT|OFFSET)\b|$)/is);
  if (orderByMatch) {
    result.orderByClause = orderByMatch[1].trim();
  }

  // Extract LIMIT
  const limitMatch = trimmedSql.match(/\bLIMIT\s+(\d+)(?:\s+OFFSET\s+(\d+))?/i);
  if (limitMatch) {
    result.limitClause = limitMatch[0];
  }

  return result;
}

// =============================================================================
// TYPE-LEVEL PARSING (COMPILE-TIME)
// =============================================================================

/**
 * Type-level check if FROM clause contains URL literal
 */
export type IsVirtualTableFrom<S extends string> =
  S extends `'${infer Url}'${string}` ? IsValidScheme<Url> :
  S extends `"${infer Url}"${string}` ? IsValidScheme<Url> :
  Uppercase<S> extends `URL(${string})${string}` ? true :
  false;

type IsValidScheme<S extends string> =
  S extends `https://${string}` ? true :
  S extends `http://${string}` ? true :
  S extends `r2://${string}` ? true :
  S extends `s3://${string}` ? true :
  false;

/**
 * Type-level extraction of URL from FROM clause
 */
export type ExtractVirtualTableUrl<S extends string> =
  S extends `'${infer Url}'${string}` ? Url :
  S extends `"${infer Url}"${string}` ? Url :
  never;

/**
 * Type-level format inference from URL
 */
export type InferVirtualTableFormat<S extends string> =
  S extends `${string}.json${string}` ? 'json' :
  S extends `${string}.ndjson${string}` ? 'ndjson' :
  S extends `${string}.jsonl${string}` ? 'ndjson' :
  S extends `${string}.csv${string}` ? 'csv' :
  S extends `${string}.parquet${string}` ? 'parquet' :
  'json';
