/**
 * SQL Tokenizer - State-aware tokenization for SQL parsing
 *
 * This tokenizer properly handles:
 * - Block comments
 * - Line comments (-- and #)
 * - String literals with escaped quotes
 * - Dollar-quoted strings (PostgreSQL)
 * - Numeric literals (integers and decimals)
 * - Operators and punctuation
 * - Identifiers and keywords
 *
 * @packageDocumentation
 */

// =============================================================================
// TOKEN TYPES
// =============================================================================

/**
 * Token type classification
 */
export type TokenType =
  | 'keyword'
  | 'identifier'
  | 'operator'
  | 'string'
  | 'number'
  | 'comment'
  | 'punctuation'
  | 'whitespace'
  | 'parameter';

/**
 * A single SQL token
 */
export interface SQLToken {
  /** Token type */
  type: TokenType;
  /** Token value (normalized - uppercase for keywords) */
  value: string;
  /** Original value from source */
  original: string;
  /** Start position in source */
  start: number;
  /** End position in source */
  end: number;
}

// =============================================================================
// TOKENIZER STATE
// =============================================================================

/** Tokenizer internal state */
type TokenizerState = 'normal' | 'string' | 'block-comment' | 'line-comment' | 'dollar-string';

/**
 * SQL keywords for token classification
 */
const SQL_KEYWORDS = new Set([
  'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'BETWEEN', 'LIKE', 'IS',
  'NULL', 'TRUE', 'FALSE', 'AS', 'ON', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
  'FULL', 'CROSS', 'NATURAL', 'GROUP', 'BY', 'HAVING', 'ORDER', 'ASC', 'DESC',
  'LIMIT', 'OFFSET', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE',
  'CREATE', 'DROP', 'ALTER', 'TABLE', 'INDEX', 'VIEW', 'DISTINCT', 'ALL',
  'UNION', 'INTERSECT', 'EXCEPT', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
  'EXISTS', 'ANY', 'SOME', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'NULLS',
  'FIRST', 'LAST', 'WITH', 'RECURSIVE', 'USING', 'RETURNING', 'CAST',
  'COALESCE', 'NULLIF', 'GREATEST', 'LEAST', 'LOWER', 'UPPER', 'TRIM',
  'SUBSTRING', 'POSITION', 'DATE_TRUNC', 'EXTRACT',
]);

/**
 * Multi-character operators
 */
const MULTI_CHAR_OPERATORS = ['>=', '<=', '!=', '<>', '||', '&&', '::'];

// =============================================================================
// TOKENIZER CLASS
// =============================================================================

/**
 * SQL Tokenizer with state-aware parsing
 */
export class SQLTokenizer {
  private sql: string = '';
  private pos: number = 0;
  private tokens: SQLToken[] = [];

  /**
   * Tokenize a SQL string
   */
  tokenize(sql: string): SQLToken[] {
    this.sql = sql;
    this.pos = 0;
    this.tokens = [];

    while (this.pos < this.sql.length) {
      const token = this.nextToken();
      if (token) {
        this.tokens.push(token);
      }
    }

    return this.tokens;
  }

  /**
   * Get the next token from the input
   */
  private nextToken(): SQLToken | null {
    this.skipWhitespace();

    if (this.pos >= this.sql.length) {
      return null;
    }

    const start = this.pos;
    const char = this.sql[this.pos];
    const nextChar = this.sql[this.pos + 1];

    // Block comment: /* ... */
    if (char === '/' && nextChar === '*') {
      return this.readBlockComment(start);
    }

    // Line comment: -- ...
    if (char === '-' && nextChar === '-') {
      return this.readLineComment(start);
    }

    // Hash line comment (MySQL): # ...
    if (char === '#') {
      return this.readLineComment(start);
    }

    // Dollar-quoted string: $tag$...$tag$
    if (char === '$') {
      const dollarToken = this.tryReadDollarQuotedString(start);
      if (dollarToken) {
        return dollarToken;
      }
      // If not a dollar-quoted string, treat $ as parameter
      return this.readParameter(start);
    }

    // Single-quoted string: '...'
    if (char === "'") {
      return this.readString(start, "'");
    }

    // Double-quoted identifier: "..."
    if (char === '"') {
      return this.readQuotedIdentifier(start);
    }

    // E-string (PostgreSQL): E'...'
    if ((char === 'E' || char === 'e') && nextChar === "'") {
      return this.readEString(start);
    }

    // Parameter placeholders: ?, :name, $1
    if (char === '?' || char === ':') {
      return this.readParameter(start);
    }

    // Numbers (including negative)
    if (this.isDigit(char) || (char === '-' && this.isDigit(nextChar) && this.shouldBeNumber())) {
      return this.readNumber(start);
    }

    // Multi-character operators
    const twoChar = char + (nextChar || '');
    if (MULTI_CHAR_OPERATORS.includes(twoChar)) {
      this.pos += 2;
      return {
        type: 'operator',
        value: twoChar,
        original: twoChar,
        start,
        end: this.pos,
      };
    }

    // Single-character operators
    if ('=<>!+-*/%&|^~'.includes(char)) {
      this.pos++;
      return {
        type: 'operator',
        value: char,
        original: char,
        start,
        end: this.pos,
      };
    }

    // Punctuation
    if ('(),;.[]{}:'.includes(char)) {
      this.pos++;
      return {
        type: 'punctuation',
        value: char,
        original: char,
        start,
        end: this.pos,
      };
    }

    // Identifier or keyword
    if (this.isIdentifierStart(char)) {
      return this.readIdentifierOrKeyword(start);
    }

    // Unknown character - skip it
    this.pos++;
    return null;
  }

  /**
   * Skip whitespace characters
   */
  private skipWhitespace(): void {
    while (this.pos < this.sql.length && this.isWhitespace(this.sql[this.pos])) {
      this.pos++;
    }
  }

  /**
   * Read a block comment
   */
  private readBlockComment(start: number): SQLToken {
    this.pos += 2; // Skip /*
    let depth = 1;

    while (this.pos < this.sql.length && depth > 0) {
      const char = this.sql[this.pos];
      const nextChar = this.sql[this.pos + 1];

      if (char === '/' && nextChar === '*') {
        depth++;
        this.pos += 2;
      } else if (char === '*' && nextChar === '/') {
        depth--;
        this.pos += 2;
      } else {
        this.pos++;
      }
    }

    const value = this.sql.slice(start, this.pos);
    return {
      type: 'comment',
      value,
      original: value,
      start,
      end: this.pos,
    };
  }

  /**
   * Read a line comment -- ... or # ...
   */
  private readLineComment(start: number): SQLToken {
    while (this.pos < this.sql.length && this.sql[this.pos] !== '\n') {
      this.pos++;
    }

    const value = this.sql.slice(start, this.pos);
    return {
      type: 'comment',
      value,
      original: value,
      start,
      end: this.pos,
    };
  }

  /**
   * Try to read a dollar-quoted string: $tag$...$tag$
   */
  private tryReadDollarQuotedString(start: number): SQLToken | null {
    // Find the opening tag
    let tagEnd = this.pos + 1;
    while (tagEnd < this.sql.length && (this.isIdentifierPart(this.sql[tagEnd]) || this.sql[tagEnd] === '$')) {
      if (this.sql[tagEnd] === '$') {
        tagEnd++;
        break;
      }
      tagEnd++;
    }

    if (tagEnd > this.pos + 1 && this.sql[tagEnd - 1] === '$') {
      const tag = this.sql.slice(this.pos, tagEnd);

      // Find the closing tag
      const closingPos = this.sql.indexOf(tag, tagEnd);
      if (closingPos !== -1) {
        this.pos = closingPos + tag.length;
        const content = this.sql.slice(tagEnd, closingPos);
        return {
          type: 'string',
          value: content,
          original: this.sql.slice(start, this.pos),
          start,
          end: this.pos,
        };
      }
    }

    return null;
  }

  /**
   * Read a single-quoted string with proper escape handling
   */
  private readString(start: number, quote: string): SQLToken {
    this.pos++; // Skip opening quote
    let value = '';

    while (this.pos < this.sql.length) {
      const char = this.sql[this.pos];

      if (char === quote) {
        // Check for escaped quote (doubled)
        if (this.sql[this.pos + 1] === quote) {
          value += quote;
          this.pos += 2;
        } else {
          // End of string
          this.pos++;
          break;
        }
      } else if (char === '\\') {
        // Handle backslash escapes
        const nextChar = this.sql[this.pos + 1];
        if (nextChar === quote || nextChar === '\\' || nextChar === 'n' || nextChar === 'r' || nextChar === 't') {
          if (nextChar === 'n') value += '\n';
          else if (nextChar === 'r') value += '\r';
          else if (nextChar === 't') value += '\t';
          else value += nextChar;
          this.pos += 2;
        } else {
          value += char;
          this.pos++;
        }
      } else {
        value += char;
        this.pos++;
      }
    }

    return {
      type: 'string',
      value,
      original: this.sql.slice(start, this.pos),
      start,
      end: this.pos,
    };
  }

  /**
   * Read a double-quoted identifier
   */
  private readQuotedIdentifier(start: number): SQLToken {
    this.pos++; // Skip opening quote
    let value = '';

    while (this.pos < this.sql.length) {
      const char = this.sql[this.pos];

      if (char === '"') {
        // Check for escaped quote (doubled)
        if (this.sql[this.pos + 1] === '"') {
          value += '"';
          this.pos += 2;
        } else {
          // End of identifier
          this.pos++;
          break;
        }
      } else {
        value += char;
        this.pos++;
      }
    }

    return {
      type: 'identifier',
      value,
      original: this.sql.slice(start, this.pos),
      start,
      end: this.pos,
    };
  }

  /**
   * Read an E-string (PostgreSQL extended string)
   */
  private readEString(start: number): SQLToken {
    this.pos++; // Skip E
    return this.readString(start, "'");
  }

  /**
   * Read a parameter placeholder
   */
  private readParameter(start: number): SQLToken {
    const char = this.sql[this.pos];

    if (char === '?') {
      this.pos++;
      return {
        type: 'parameter',
        value: '?',
        original: '?',
        start,
        end: this.pos,
      };
    }

    if (char === ':') {
      this.pos++;
      // Named parameter :name
      while (this.pos < this.sql.length && this.isIdentifierPart(this.sql[this.pos])) {
        this.pos++;
      }
      const value = this.sql.slice(start, this.pos);
      return {
        type: 'parameter',
        value,
        original: value,
        start,
        end: this.pos,
      };
    }

    if (char === '$') {
      this.pos++;
      // Positional parameter $1, $2, etc.
      while (this.pos < this.sql.length && this.isDigit(this.sql[this.pos])) {
        this.pos++;
      }
      const value = this.sql.slice(start, this.pos);
      return {
        type: 'parameter',
        value,
        original: value,
        start,
        end: this.pos,
      };
    }

    // Fallback
    this.pos++;
    return {
      type: 'parameter',
      value: char,
      original: char,
      start,
      end: this.pos,
    };
  }

  /**
   * Read a numeric literal
   */
  private readNumber(start: number): SQLToken {
    if (this.sql[this.pos] === '-') {
      this.pos++;
    }

    // Integer part
    while (this.pos < this.sql.length && this.isDigit(this.sql[this.pos])) {
      this.pos++;
    }

    // Decimal part
    if (this.sql[this.pos] === '.' && this.isDigit(this.sql[this.pos + 1])) {
      this.pos++; // Skip .
      while (this.pos < this.sql.length && this.isDigit(this.sql[this.pos])) {
        this.pos++;
      }
    }

    // Exponent
    if (this.sql[this.pos] === 'e' || this.sql[this.pos] === 'E') {
      const next = this.sql[this.pos + 1];
      if (this.isDigit(next) || next === '+' || next === '-') {
        this.pos++; // Skip e/E
        if (this.sql[this.pos] === '+' || this.sql[this.pos] === '-') {
          this.pos++;
        }
        while (this.pos < this.sql.length && this.isDigit(this.sql[this.pos])) {
          this.pos++;
        }
      }
    }

    const value = this.sql.slice(start, this.pos);
    return {
      type: 'number',
      value,
      original: value,
      start,
      end: this.pos,
    };
  }

  /**
   * Read an identifier or keyword
   */
  private readIdentifierOrKeyword(start: number): SQLToken {
    while (this.pos < this.sql.length && this.isIdentifierPart(this.sql[this.pos])) {
      this.pos++;
    }

    const original = this.sql.slice(start, this.pos);
    const upper = original.toUpperCase();
    const isKeyword = SQL_KEYWORDS.has(upper);

    return {
      type: isKeyword ? 'keyword' : 'identifier',
      value: isKeyword ? upper : original,
      original,
      start,
      end: this.pos,
    };
  }

  /**
   * Check if the next minus should be a number (not operator)
   */
  private shouldBeNumber(): boolean {
    // After operator or punctuation, minus is likely part of a number
    const lastToken = this.tokens[this.tokens.length - 1];
    if (!lastToken) return true;
    return lastToken.type === 'operator' || lastToken.type === 'punctuation' || lastToken.type === 'keyword';
  }

  // Character classification helpers
  private isWhitespace(char: string): boolean {
    return /\s/.test(char);
  }

  private isDigit(char: string): boolean {
    return char >= '0' && char <= '9';
  }

  private isIdentifierStart(char: string): boolean {
    return /[a-zA-Z_\u0080-\uFFFF]/.test(char);
  }

  private isIdentifierPart(char: string): boolean {
    return /[a-zA-Z0-9_\u0080-\uFFFF]/.test(char);
  }
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Tokenize a SQL string (convenience function)
 */
export function tokenizeSQL(sql: string): SQLToken[] {
  return new SQLTokenizer().tokenize(sql);
}

/**
 * Filter out comments from tokens
 */
export function stripComments(tokens: SQLToken[]): SQLToken[] {
  return tokens.filter(t => t.type !== 'comment');
}

/**
 * Get only meaningful tokens (no comments, whitespace already stripped)
 */
export function getMeaningfulTokens(sql: string): SQLToken[] {
  return stripComments(tokenizeSQL(sql));
}

/**
 * Find a keyword token by value
 */
export function findKeyword(tokens: SQLToken[], keyword: string): SQLToken | undefined {
  const upper = keyword.toUpperCase();
  return tokens.find(t => t.type === 'keyword' && t.value === upper);
}

/**
 * Find the index of a keyword token
 */
export function findKeywordIndex(tokens: SQLToken[], keyword: string): number {
  const upper = keyword.toUpperCase();
  return tokens.findIndex(t => t.type === 'keyword' && t.value === upper);
}

/**
 * Extract tokens between two keywords
 */
export function getTokensBetweenKeywords(
  tokens: SQLToken[],
  startKeyword: string,
  endKeywords: string[]
): SQLToken[] {
  const startIndex = findKeywordIndex(tokens, startKeyword);
  if (startIndex === -1) return [];

  const endKeywordSet = new Set(endKeywords.map(k => k.toUpperCase()));
  let endIndex = tokens.length;

  for (let i = startIndex + 1; i < tokens.length; i++) {
    if (tokens[i].type === 'keyword' && endKeywordSet.has(tokens[i].value)) {
      endIndex = i;
      break;
    }
  }

  return tokens.slice(startIndex + 1, endIndex);
}

/**
 * Extract shard key value from WHERE clause tokens
 *
 * @param tokens - Array of SQL tokens (comments already stripped)
 * @param shardKeyColumn - The shard key column name
 * @returns The shard key value or null if not found
 */
export function extractShardKeyFromTokens(
  tokens: SQLToken[],
  shardKeyColumn: string
): { value: unknown; method: 'equality' | 'in-list' | 'none'; values?: unknown[] } | null {
  // Find WHERE keyword
  const whereIndex = findKeywordIndex(tokens, 'WHERE');
  if (whereIndex === -1) {
    return null;
  }

  // Get tokens after WHERE until GROUP BY, ORDER BY, LIMIT, etc.
  const endKeywords = ['GROUP', 'ORDER', 'LIMIT', 'OFFSET', 'HAVING', 'UNION', 'INTERSECT', 'EXCEPT'];
  const whereTokens = getTokensBetweenKeywords(tokens, 'WHERE', endKeywords);

  // Check for OR at the top level (can't optimize to single shard)
  if (hasTopLevelOr(whereTokens)) {
    return null;
  }

  const shardKeyLower = shardKeyColumn.toLowerCase();

  // Scan for shard key equality: column = value
  for (let i = 0; i < whereTokens.length; i++) {
    const token = whereTokens[i];

    // Check if this is our shard key column (case-insensitive)
    if (token.type === 'identifier' && token.value.toLowerCase() === shardKeyLower) {
      // Check for table.column pattern before this
      if (i > 1 && whereTokens[i - 1].value === '.') {
        // Skip the table prefix, this is still our column
      }

      // Look for = operator
      const nextNonWhitespace = findNextMeaningful(whereTokens, i + 1);
      if (nextNonWhitespace !== -1 && whereTokens[nextNonWhitespace].value === '=') {
        // Get the value
        const valueIndex = findNextMeaningful(whereTokens, nextNonWhitespace + 1);
        if (valueIndex !== -1) {
          const valueToken = whereTokens[valueIndex];
          const value = parseTokenValue(valueToken);
          if (value !== null) {
            return { value, method: 'equality' };
          }
        }
      }

      // Look for IN operator
      if (nextNonWhitespace !== -1 && whereTokens[nextNonWhitespace].value === 'IN') {
        const values = parseInList(whereTokens, nextNonWhitespace + 1);
        if (values.length > 0) {
          return { value: values[0], method: 'in-list', values };
        }
      }
    }

    // Also check for aliased column: alias.column
    if (token.value === '.' && i + 1 < whereTokens.length) {
      const columnToken = whereTokens[i + 1];
      if (columnToken.type === 'identifier' && columnToken.value.toLowerCase() === shardKeyLower) {
        // Look for = operator
        const nextNonWhitespace = findNextMeaningful(whereTokens, i + 2);
        if (nextNonWhitespace !== -1 && whereTokens[nextNonWhitespace].value === '=') {
          const valueIndex = findNextMeaningful(whereTokens, nextNonWhitespace + 1);
          if (valueIndex !== -1) {
            const valueToken = whereTokens[valueIndex];
            const value = parseTokenValue(valueToken);
            if (value !== null) {
              return { value, method: 'equality' };
            }
          }
        }
      }
    }
  }

  return null;
}

/**
 * Check if WHERE clause has top-level OR (not nested in parentheses)
 */
function hasTopLevelOr(tokens: SQLToken[]): boolean {
  let depth = 0;
  for (const token of tokens) {
    if (token.value === '(') depth++;
    else if (token.value === ')') depth--;
    else if (token.type === 'keyword' && token.value === 'OR' && depth === 0) {
      return true;
    }
  }
  return false;
}

/**
 * Find the next meaningful token (skip punctuation like parentheses for IN)
 */
function findNextMeaningful(tokens: SQLToken[], startIndex: number): number {
  for (let i = startIndex; i < tokens.length; i++) {
    const t = tokens[i];
    // Skip only whitespace-equivalent gaps - we shouldn't skip punctuation normally
    // but for finding the operator after a column, we should skip nothing
    return i;
  }
  return -1;
}

/**
 * Parse a token value to JavaScript value
 */
function parseTokenValue(token: SQLToken): unknown {
  switch (token.type) {
    case 'string':
      return token.value;
    case 'number':
      return parseFloat(token.value);
    case 'keyword':
      if (token.value === 'TRUE') return true;
      if (token.value === 'FALSE') return false;
      if (token.value === 'NULL') return null;
      return null;
    case 'parameter':
      return { placeholder: token.value };
    case 'identifier':
      // Could be a column reference or unquoted string - treat as column
      return null;
    default:
      return null;
  }
}

/**
 * Parse values from an IN list: IN (1, 2, 3)
 */
function parseInList(tokens: SQLToken[], startIndex: number): unknown[] {
  const values: unknown[] = [];
  let i = startIndex;

  // Find opening parenthesis
  while (i < tokens.length && tokens[i].value !== '(') {
    i++;
  }
  if (i >= tokens.length) return [];
  i++; // Skip (

  let depth = 1;
  while (i < tokens.length && depth > 0) {
    const token = tokens[i];

    if (token.value === '(') {
      depth++;
      // This might be a subquery - we can't extract values from it
      return [];
    } else if (token.value === ')') {
      depth--;
    } else if (token.value === ',' && depth === 1) {
      // Skip commas
    } else if (token.type === 'string' || token.type === 'number') {
      const value = parseTokenValue(token);
      if (value !== null) {
        values.push(value);
      }
    } else if (token.type === 'keyword' && (token.value === 'SELECT' || token.value === 'WITH')) {
      // Subquery - can't extract
      return [];
    }

    i++;
  }

  return values;
}

/**
 * Parse all conditions from WHERE clause tokens
 */
export interface ParsedCondition {
  column: string;
  tableAlias?: string;
  operator: string;
  value?: unknown;
  values?: unknown[];
  minValue?: unknown;
  maxValue?: unknown;
}

/**
 * Extract all conditions from WHERE clause
 */
export function extractConditionsFromTokens(tokens: SQLToken[]): {
  conditions: ParsedCondition[];
  operator: 'AND' | 'OR';
} {
  const conditions: ParsedCondition[] = [];
  let primaryOperator: 'AND' | 'OR' = 'AND';

  // Find WHERE keyword
  const whereIndex = findKeywordIndex(tokens, 'WHERE');
  if (whereIndex === -1) {
    return { conditions, operator: primaryOperator };
  }

  // Get tokens after WHERE
  const endKeywords = ['GROUP', 'ORDER', 'LIMIT', 'OFFSET', 'HAVING', 'UNION', 'INTERSECT', 'EXCEPT'];
  const whereTokens = getTokensBetweenKeywords(tokens, 'WHERE', endKeywords);

  // Check for top-level OR
  if (hasTopLevelOr(whereTokens)) {
    primaryOperator = 'OR';
  }

  // Parse conditions - simplified approach focusing on equality conditions
  let i = 0;
  while (i < whereTokens.length) {
    const token = whereTokens[i];

    // Skip AND/OR keywords
    if (token.type === 'keyword' && (token.value === 'AND' || token.value === 'OR')) {
      i++;
      continue;
    }

    // Skip parentheses at start
    if (token.value === '(') {
      i++;
      continue;
    }

    // Look for column identifier
    if (token.type === 'identifier') {
      let column = token.value;
      let tableAlias: string | undefined;
      let nextIndex = i + 1;

      // Check for table.column
      if (nextIndex < whereTokens.length && whereTokens[nextIndex].value === '.') {
        tableAlias = column;
        nextIndex++;
        if (nextIndex < whereTokens.length && whereTokens[nextIndex].type === 'identifier') {
          column = whereTokens[nextIndex].value;
          nextIndex++;
        }
      }

      // Look for operator
      if (nextIndex < whereTokens.length) {
        const opToken = whereTokens[nextIndex];

        if (opToken.value === '=') {
          // Equality condition
          nextIndex++;
          if (nextIndex < whereTokens.length) {
            const valueToken = whereTokens[nextIndex];
            const value = parseTokenValue(valueToken);
            if (value !== null || valueToken.type === 'string' || valueToken.type === 'number') {
              conditions.push({
                column,
                tableAlias,
                operator: '=',
                value: value !== null ? value : parseTokenValue(valueToken),
              });
            }
            i = nextIndex + 1;
            continue;
          }
        } else if (opToken.type === 'keyword' && opToken.value === 'IN') {
          // IN condition
          const values = parseInList(whereTokens, nextIndex + 1);
          // Always push the condition even if values is empty (e.g., subquery)
          conditions.push({
            column,
            tableAlias,
            operator: 'IN',
            values,
          });
          // Skip to end of IN list
          let depth = 0;
          while (nextIndex < whereTokens.length) {
            if (whereTokens[nextIndex].value === '(') depth++;
            if (whereTokens[nextIndex].value === ')') {
              depth--;
              if (depth === 0) {
                nextIndex++;
                break;
              }
            }
            nextIndex++;
          }
          i = nextIndex;
          continue;
        } else if (opToken.type === 'keyword' && opToken.value === 'BETWEEN') {
          // BETWEEN condition
          nextIndex++;
          let minValue: unknown = null;
          let maxValue: unknown = null;

          if (nextIndex < whereTokens.length) {
            minValue = parseTokenValue(whereTokens[nextIndex]);
            nextIndex++;
          }

          // Skip AND
          if (nextIndex < whereTokens.length && whereTokens[nextIndex].value === 'AND') {
            nextIndex++;
          }

          if (nextIndex < whereTokens.length) {
            maxValue = parseTokenValue(whereTokens[nextIndex]);
            nextIndex++;
          }

          if (minValue !== null && maxValue !== null) {
            conditions.push({
              column,
              tableAlias,
              operator: 'BETWEEN',
              minValue,
              maxValue,
            });
          }
          i = nextIndex;
          continue;
        } else if (['!=', '<>', '>', '<', '>=', '<='].includes(opToken.value)) {
          // Comparison operators
          nextIndex++;
          if (nextIndex < whereTokens.length) {
            const valueToken = whereTokens[nextIndex];
            const value = parseTokenValue(valueToken);
            conditions.push({
              column,
              tableAlias,
              operator: opToken.value === '<>' ? '!=' : opToken.value,
              value,
            });
            i = nextIndex + 1;
            continue;
          }
        } else if (opToken.type === 'keyword' && opToken.value === 'IS') {
          // IS NULL / IS NOT NULL
          nextIndex++;
          let isNot = false;
          if (nextIndex < whereTokens.length && whereTokens[nextIndex].value === 'NOT') {
            isNot = true;
            nextIndex++;
          }
          if (nextIndex < whereTokens.length && whereTokens[nextIndex].value === 'NULL') {
            conditions.push({
              column,
              tableAlias,
              operator: isNot ? 'IS NOT NULL' : 'IS NULL',
            });
            i = nextIndex + 1;
            continue;
          }
        } else if (opToken.type === 'keyword' && opToken.value === 'LIKE') {
          // LIKE condition
          nextIndex++;
          if (nextIndex < whereTokens.length) {
            const valueToken = whereTokens[nextIndex];
            conditions.push({
              column,
              tableAlias,
              operator: 'LIKE',
              value: parseTokenValue(valueToken),
            });
            i = nextIndex + 1;
            continue;
          }
        }
      }
    }

    i++;
  }

  return { conditions, operator: primaryOperator };
}
