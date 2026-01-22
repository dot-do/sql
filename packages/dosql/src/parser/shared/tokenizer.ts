/**
 * Shared SQL Tokenizer
 *
 * Unified tokenizer for all DoSQL parsers. Handles SQL lexical analysis
 * including keywords, identifiers, strings, numbers, operators, and comments.
 *
 * @packageDocumentation
 */

import type { Token, TokenType, SourceLocation } from './types.js';
import { SQLSyntaxError } from './errors.js';

// =============================================================================
// SQL KEYWORDS
// =============================================================================

/**
 * Comprehensive set of SQL keywords recognized by the tokenizer
 */
export const SQL_KEYWORDS = new Set([
  // DML
  'select', 'from', 'where', 'and', 'or', 'not', 'in', 'between', 'like', 'ilike',
  'is', 'null', 'true', 'false', 'as', 'on', 'join', 'inner', 'left', 'right',
  'full', 'outer', 'cross', 'group', 'by', 'having', 'order', 'asc', 'desc',
  'limit', 'offset', 'distinct', 'all', 'union', 'intersect', 'except',
  'count', 'sum', 'avg', 'min', 'max', 'nulls', 'first', 'last',
  'exists', 'any', 'some', 'case', 'when', 'then', 'else', 'end',
  'with', 'recursive', 'coalesce', 'nullif', 'iif', 'if',
  'insert', 'into', 'values', 'update', 'set', 'delete', 'replace',
  'returning', 'conflict', 'do', 'nothing',

  // DDL
  'create', 'table', 'index', 'view', 'drop', 'alter', 'add', 'column', 'rename',
  'to', 'exists', 'temporary', 'temp', 'unique', 'primary', 'key', 'foreign',
  'references', 'cascade', 'restrict', 'default', 'check', 'constraint',
  'autoincrement', 'collate', 'without', 'rowid', 'strict', 'generated',
  'always', 'stored', 'virtual', 'no', 'action', 'abort', 'fail', 'ignore',
  'rollback', 'match', 'simple', 'partial', 'deferrable', 'initially',
  'deferred', 'immediate',

  // Data types
  'integer', 'int', 'smallint', 'mediumint', 'bigint', 'tinyint',
  'real', 'double', 'precision', 'float', 'numeric', 'decimal',
  'text', 'varchar', 'char', 'nchar', 'nvarchar', 'clob',
  'blob', 'none', 'date', 'datetime', 'timestamp', 'time',
  'boolean', 'bool', 'json', 'jsonb', 'uuid',

  // Window functions
  'over', 'partition', 'rows', 'range', 'groups', 'unbounded', 'preceding',
  'following', 'current', 'row', 'exclude', 'ties', 'others', 'window',
  'filter', 'within', 'respect', 'ntile', 'lag', 'lead', 'first_value',
  'last_value', 'nth_value', 'row_number', 'rank', 'dense_rank', 'percent_rank',
  'cume_dist',
]);

/**
 * Check if a string is a SQL keyword
 */
export function isKeyword(value: string): boolean {
  return SQL_KEYWORDS.has(value.toLowerCase());
}

// =============================================================================
// TOKENIZER CLASS
// =============================================================================

/**
 * SQL Tokenizer
 *
 * Converts SQL source string into a stream of tokens for parsing.
 */
export class Tokenizer {
  private sql: string;
  private pos = 0;
  private line = 1;
  private column = 1;
  private tokens: Token[] = [];

  /**
   * Create a new tokenizer
   * @param sql - SQL source string to tokenize
   */
  constructor(sql: string) {
    this.sql = sql;
  }

  /**
   * Tokenize the entire SQL string
   * @param includeWhitespace - Whether to include whitespace tokens
   * @param includeComments - Whether to include comment tokens
   * @returns Array of tokens
   */
  tokenize(includeWhitespace = false, includeComments = false): Token[] {
    this.tokens = [];
    this.pos = 0;
    this.line = 1;
    this.column = 1;

    while (this.pos < this.sql.length) {
      const token = this.readToken();
      if (token) {
        if (token.type === 'whitespace' && !includeWhitespace) continue;
        if (token.type === 'comment' && !includeComments) continue;
        this.tokens.push(token);
      }
    }

    // Add EOF token
    this.tokens.push({
      type: 'eof',
      value: '',
      location: this.location(),
    });

    return this.tokens;
  }

  /**
   * Get current source location
   */
  private location(): SourceLocation {
    return { line: this.line, column: this.column, offset: this.pos };
  }

  /**
   * Advance position by one character
   */
  private advance(): string {
    const char = this.sql[this.pos];
    this.pos++;
    if (char === '\n') {
      this.line++;
      this.column = 1;
    } else {
      this.column++;
    }
    return char;
  }

  /**
   * Peek at character at offset from current position
   */
  private peek(offset = 0): string {
    return this.sql[this.pos + offset] || '';
  }

  /**
   * Read next token from input
   */
  private readToken(): Token | null {
    const loc = this.location();
    const char = this.peek();

    // Whitespace
    if (/\s/.test(char)) {
      return this.readWhitespace(loc);
    }

    // Single-line comment
    if (char === '-' && this.peek(1) === '-') {
      return this.readLineComment(loc);
    }

    // Multi-line comment
    if (char === '/' && this.peek(1) === '*') {
      return this.readBlockComment(loc);
    }

    // String literal (single quotes)
    if (char === "'") {
      return this.readString(loc, "'");
    }

    // Quoted identifier (double quotes or backticks)
    if (char === '"' || char === '`') {
      return this.readQuotedIdentifier(loc, char);
    }

    // Square bracket quoted identifier
    if (char === '[') {
      return this.readBracketIdentifier(loc);
    }

    // Number
    if (/[0-9]/.test(char) || (char === '.' && /[0-9]/.test(this.peek(1)))) {
      return this.readNumber(loc);
    }

    // Parameter (?, :name, $n)
    if (char === '?' || char === ':' || char === '$') {
      return this.readParameter(loc);
    }

    // Identifier or keyword
    if (/[a-zA-Z_]/.test(char)) {
      return this.readIdentifier(loc);
    }

    // Two-character operators
    const twoChar = char + this.peek(1);
    if (['<=', '>=', '<>', '!=', '||', '<<', '>>', '--', '/*'].includes(twoChar)) {
      this.advance();
      this.advance();
      return { type: 'operator', value: twoChar, location: loc };
    }

    // Single-character operators
    if ('=<>+-*/%&|~'.includes(char)) {
      return { type: 'operator', value: this.advance(), location: loc };
    }

    // Punctuation
    if ('(),;.'.includes(char)) {
      return { type: 'punctuation', value: this.advance(), location: loc };
    }

    // Unknown character - skip it with error
    throw new SQLSyntaxError(`Unexpected character '${char}'`, loc, this.sql);
  }

  /**
   * Read whitespace token
   */
  private readWhitespace(loc: SourceLocation): Token {
    let value = '';
    while (this.pos < this.sql.length && /\s/.test(this.peek())) {
      value += this.advance();
    }
    return { type: 'whitespace', value, location: loc };
  }

  /**
   * Read single-line comment (-- ...)
   */
  private readLineComment(loc: SourceLocation): Token {
    let value = '';
    this.advance(); // -
    this.advance(); // -
    while (this.pos < this.sql.length && this.peek() !== '\n') {
      value += this.advance();
    }
    return { type: 'comment', value: value.trim(), location: loc };
  }

  /**
   * Read multi-line comment
   */
  private readBlockComment(loc: SourceLocation): Token {
    let value = '';
    this.advance(); // /
    this.advance(); // *
    while (this.pos < this.sql.length - 1) {
      if (this.peek() === '*' && this.peek(1) === '/') {
        this.advance(); // *
        this.advance(); // /
        break;
      }
      value += this.advance();
    }
    return { type: 'comment', value: value.trim(), location: loc };
  }

  /**
   * Read string literal
   */
  private readString(loc: SourceLocation, quote: string): Token {
    let value = '';
    this.advance(); // opening quote

    while (this.pos < this.sql.length) {
      const char = this.peek();

      if (char === quote) {
        // Check for escaped quote (doubled)
        if (this.peek(1) === quote) {
          value += quote;
          this.advance();
          this.advance();
        } else {
          this.advance(); // closing quote
          break;
        }
      } else if (char === '\\') {
        // Handle escape sequences
        this.advance();
        const escaped = this.advance();
        switch (escaped) {
          case 'n': value += '\n'; break;
          case 't': value += '\t'; break;
          case 'r': value += '\r'; break;
          case '\\': value += '\\'; break;
          case "'": value += "'"; break;
          case '"': value += '"'; break;
          default: value += escaped;
        }
      } else {
        value += this.advance();
      }
    }

    return { type: 'string', value, location: loc };
  }

  /**
   * Read quoted identifier
   */
  private readQuotedIdentifier(loc: SourceLocation, quote: string): Token {
    let value = '';
    this.advance(); // opening quote

    while (this.pos < this.sql.length && this.peek() !== quote) {
      value += this.advance();
    }

    if (this.peek() === quote) {
      this.advance(); // closing quote
    }

    return { type: 'identifier', value, location: loc };
  }

  /**
   * Read square bracket quoted identifier
   */
  private readBracketIdentifier(loc: SourceLocation): Token {
    let value = '';
    this.advance(); // [

    while (this.pos < this.sql.length && this.peek() !== ']') {
      value += this.advance();
    }

    if (this.peek() === ']') {
      this.advance(); // ]
    }

    return { type: 'identifier', value, location: loc };
  }

  /**
   * Read numeric literal
   */
  private readNumber(loc: SourceLocation): Token {
    let value = '';

    // Integer part
    while (/[0-9]/.test(this.peek())) {
      value += this.advance();
    }

    // Decimal part
    if (this.peek() === '.' && /[0-9]/.test(this.peek(1))) {
      value += this.advance(); // .
      while (/[0-9]/.test(this.peek())) {
        value += this.advance();
      }
    }

    // Scientific notation
    if (this.peek().toLowerCase() === 'e') {
      value += this.advance(); // e/E
      if (this.peek() === '+' || this.peek() === '-') {
        value += this.advance();
      }
      while (/[0-9]/.test(this.peek())) {
        value += this.advance();
      }
    }

    return { type: 'number', value, location: loc };
  }

  /**
   * Read parameter placeholder
   */
  private readParameter(loc: SourceLocation): Token {
    let value = this.advance(); // ?, :, or $

    // Named parameter (:name, $name)
    if ((value === ':' || value === '$') && /[a-zA-Z_]/.test(this.peek())) {
      while (/[a-zA-Z0-9_]/.test(this.peek())) {
        value += this.advance();
      }
    }
    // Numbered parameter ($1, $2, etc.)
    else if (value === '$' && /[0-9]/.test(this.peek())) {
      while (/[0-9]/.test(this.peek())) {
        value += this.advance();
      }
    }

    return { type: 'parameter', value, location: loc };
  }

  /**
   * Read identifier or keyword
   */
  private readIdentifier(loc: SourceLocation): Token {
    let value = '';

    while (/[a-zA-Z0-9_]/.test(this.peek())) {
      value += this.advance();
    }

    const lower = value.toLowerCase();
    const type: TokenType = SQL_KEYWORDS.has(lower) ? 'keyword' : 'identifier';

    return { type, value: type === 'keyword' ? lower : value, location: loc };
  }
}

// =============================================================================
// CONVENIENCE FUNCTIONS
// =============================================================================

/**
 * Tokenize a SQL string
 *
 * @param sql - SQL source string
 * @param options - Tokenization options
 * @returns Array of tokens
 */
export function tokenize(
  sql: string,
  options: { includeWhitespace?: boolean; includeComments?: boolean } = {}
): Token[] {
  const tokenizer = new Tokenizer(sql);
  return tokenizer.tokenize(
    options.includeWhitespace ?? false,
    options.includeComments ?? false
  );
}

/**
 * Create a token stream that filters whitespace
 */
export function createTokenStream(sql: string): Token[] {
  return tokenize(sql, { includeWhitespace: false, includeComments: false });
}
