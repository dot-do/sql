/**
 * MATCH Query Parser
 *
 * Parses FTS5-style MATCH query syntax into an AST.
 * Reference: https://www.sqlite.org/fts5.html#full_text_query_syntax
 */

import type {
  MatchQuery,
  TermQuery,
  PhraseQuery,
  PrefixQuery,
  AndQuery,
  OrQuery,
  NotQuery,
  NearQuery,
  ColumnQuery,
} from './types.js';

// Re-export query types for convenience
export type {
  MatchQuery,
  TermQuery,
  PhraseQuery,
  PrefixQuery,
  AndQuery,
  OrQuery,
  NotQuery,
  NearQuery,
  ColumnQuery,
};

// =============================================================================
// Token Types for Parser
// =============================================================================

type TokenType =
  | 'TERM'
  | 'PHRASE'
  | 'PREFIX'
  | 'AND'
  | 'OR'
  | 'NOT'
  | 'NEAR'
  | 'LPAREN'
  | 'RPAREN'
  | 'COLON'
  | 'COMMA'
  | 'NUMBER'
  | 'EOF';

interface ParserToken {
  type: TokenType;
  value: string;
  position: number;
}

// =============================================================================
// Lexer
// =============================================================================

/**
 * Tokenize query string into parser tokens
 */
function lexQuery(query: string): ParserToken[] {
  const tokens: ParserToken[] = [];
  let pos = 0;

  const skipWhitespace = () => {
    while (pos < query.length && /\s/.test(query[pos])) {
      pos++;
    }
  };

  while (pos < query.length) {
    skipWhitespace();
    if (pos >= query.length) break;

    const startPos = pos;
    const char = query[pos];

    // Phrase: "..."
    if (char === '"') {
      pos++; // Skip opening quote
      let phrase = '';
      while (pos < query.length && query[pos] !== '"') {
        phrase += query[pos];
        pos++;
      }
      pos++; // Skip closing quote
      tokens.push({ type: 'PHRASE', value: phrase, position: startPos });
      continue;
    }

    // Parentheses
    if (char === '(') {
      tokens.push({ type: 'LPAREN', value: '(', position: startPos });
      pos++;
      continue;
    }
    if (char === ')') {
      tokens.push({ type: 'RPAREN', value: ')', position: startPos });
      pos++;
      continue;
    }

    // Comma
    if (char === ',') {
      tokens.push({ type: 'COMMA', value: ',', position: startPos });
      pos++;
      continue;
    }

    // Colon
    if (char === ':') {
      tokens.push({ type: 'COLON', value: ':', position: startPos });
      pos++;
      continue;
    }

    // Number
    if (/\d/.test(char)) {
      let num = '';
      while (pos < query.length && /\d/.test(query[pos])) {
        num += query[pos];
        pos++;
      }
      tokens.push({ type: 'NUMBER', value: num, position: startPos });
      continue;
    }

    // Word (term, keyword, or prefix)
    if (/[a-zA-Z_]/.test(char)) {
      let word = '';
      while (pos < query.length && /[a-zA-Z0-9_]/.test(query[pos])) {
        word += query[pos];
        pos++;
      }

      // Check for prefix (word*)
      if (pos < query.length && query[pos] === '*') {
        pos++; // Skip *
        tokens.push({ type: 'PREFIX', value: word, position: startPos });
        continue;
      }

      // Check for keywords
      const upper = word.toUpperCase();
      if (upper === 'AND') {
        tokens.push({ type: 'AND', value: word, position: startPos });
      } else if (upper === 'OR') {
        tokens.push({ type: 'OR', value: word, position: startPos });
      } else if (upper === 'NOT') {
        tokens.push({ type: 'NOT', value: word, position: startPos });
      } else if (upper === 'NEAR') {
        tokens.push({ type: 'NEAR', value: word, position: startPos });
      } else {
        tokens.push({ type: 'TERM', value: word, position: startPos });
      }
      continue;
    }

    // Skip unknown characters
    pos++;
  }

  tokens.push({ type: 'EOF', value: '', position: pos });
  return tokens;
}

// =============================================================================
// Parser
// =============================================================================

class QueryParser {
  private tokens: ParserToken[];
  private current: number = 0;

  constructor(query: string) {
    this.tokens = lexQuery(query);
  }

  /**
   * Parse the query
   */
  parse(): MatchQuery {
    if (this.isAtEnd() || this.peek().type === 'EOF') {
      // Empty query - return a term that matches nothing
      return { type: 'term', term: '' };
    }
    return this.parseOr();
  }

  /**
   * Parse OR expression (lowest precedence)
   */
  private parseOr(): MatchQuery {
    let left = this.parseAnd();

    while (this.match('OR')) {
      const right = this.parseAnd();
      left = { type: 'or', left, right };
    }

    return left;
  }

  /**
   * Parse AND expression
   */
  private parseAnd(): MatchQuery {
    let left = this.parseNot();

    // Explicit AND or implicit AND (space-separated terms)
    while (this.match('AND') || this.isImplicitAnd()) {
      const right = this.parseNot();
      left = { type: 'and', left, right };
    }

    return left;
  }

  /**
   * Parse NOT expression
   */
  private parseNot(): MatchQuery {
    const include = this.parsePrimary();

    if (this.match('NOT')) {
      const exclude = this.parsePrimary();
      return { type: 'not', include, exclude };
    }

    return include;
  }

  /**
   * Parse primary expressions
   */
  private parsePrimary(): MatchQuery {
    // Parenthesized expression
    if (this.match('LPAREN')) {
      const expr = this.parseOr();
      this.consume('RPAREN');
      return expr;
    }

    // NEAR expression
    if (this.check('NEAR')) {
      return this.parseNear();
    }

    // Column filter
    if (this.check('TERM') && this.checkNext('COLON')) {
      return this.parseColumnFilter();
    }

    // Phrase
    if (this.check('PHRASE')) {
      const token = this.advance();
      const terms = token.value.toLowerCase().split(/\s+/).filter(Boolean);
      return { type: 'phrase', terms };
    }

    // Prefix
    if (this.check('PREFIX')) {
      const token = this.advance();
      return { type: 'prefix', prefix: token.value.toLowerCase() };
    }

    // Term
    if (this.check('TERM')) {
      const token = this.advance();
      return { type: 'term', term: token.value.toLowerCase() };
    }

    // Default to empty term
    return { type: 'term', term: '' };
  }

  /**
   * Parse NEAR expression: NEAR(term1 term2, distance)
   */
  private parseNear(): NearQuery {
    this.consume('NEAR');
    this.consume('LPAREN');

    const terms: string[] = [];
    let distance = 10; // Default distance

    // Parse terms
    while (!this.check('COMMA') && !this.check('RPAREN')) {
      if (this.check('TERM')) {
        terms.push(this.advance().value.toLowerCase());
      } else {
        break;
      }
    }

    // Parse optional distance
    if (this.match('COMMA')) {
      if (this.check('NUMBER')) {
        distance = parseInt(this.advance().value, 10);
      }
    }

    this.consume('RPAREN');

    return { type: 'near', terms, distance };
  }

  /**
   * Parse column filter: column:query
   */
  private parseColumnFilter(): ColumnQuery {
    const column = this.advance().value; // Get column name
    this.consume('COLON');
    const query = this.parsePrimary();
    return { type: 'column', column, query };
  }

  /**
   * Check if this looks like an implicit AND
   */
  private isImplicitAnd(): boolean {
    if (this.isAtEnd()) return false;
    const type = this.peek().type;
    return (
      type === 'TERM' ||
      type === 'PHRASE' ||
      type === 'PREFIX' ||
      type === 'NEAR' ||
      type === 'LPAREN'
    );
  }

  // =============================================================================
  // Helper Methods
  // =============================================================================

  private peek(): ParserToken {
    return this.tokens[this.current];
  }

  private peekNext(): ParserToken | undefined {
    if (this.current + 1 < this.tokens.length) {
      return this.tokens[this.current + 1];
    }
    return undefined;
  }

  private isAtEnd(): boolean {
    return this.current >= this.tokens.length || this.peek().type === 'EOF';
  }

  private check(type: TokenType): boolean {
    if (this.isAtEnd()) return false;
    return this.peek().type === type;
  }

  private checkNext(type: TokenType): boolean {
    const next = this.peekNext();
    return next !== undefined && next.type === type;
  }

  private advance(): ParserToken {
    if (!this.isAtEnd()) {
      this.current++;
    }
    return this.tokens[this.current - 1];
  }

  private match(type: TokenType): boolean {
    if (this.check(type)) {
      this.advance();
      return true;
    }
    return false;
  }

  private consume(type: TokenType): ParserToken {
    if (this.check(type)) {
      return this.advance();
    }
    // If expected token not found, return a dummy token
    return { type, value: '', position: this.current };
  }
}

// =============================================================================
// Public API
// =============================================================================

/**
 * Parse a MATCH query string into an AST
 */
export function parseMatchQuery(query: string): MatchQuery {
  const parser = new QueryParser(query);
  return parser.parse();
}

/**
 * Convert query AST back to string (for debugging)
 */
export function queryToString(query: MatchQuery): string {
  switch (query.type) {
    case 'term':
      return query.term;
    case 'phrase':
      return `"${query.terms.join(' ')}"`;
    case 'prefix':
      return `${query.prefix}*`;
    case 'and':
      return `(${queryToString(query.left)} AND ${queryToString(query.right)})`;
    case 'or':
      return `(${queryToString(query.left)} OR ${queryToString(query.right)})`;
    case 'not':
      return `(${queryToString(query.include)} NOT ${queryToString(query.exclude)})`;
    case 'near':
      return `NEAR(${query.terms.join(' ')}, ${query.distance})`;
    case 'column':
      return `${query.column}:${queryToString(query.query)}`;
    default:
      return '';
  }
}

/**
 * Extract all terms from a query for highlighting
 */
export function extractTerms(query: MatchQuery): string[] {
  const terms: string[] = [];

  function walk(q: MatchQuery) {
    switch (q.type) {
      case 'term':
        if (q.term) terms.push(q.term);
        break;
      case 'phrase':
        terms.push(...q.terms);
        break;
      case 'prefix':
        terms.push(q.prefix);
        break;
      case 'and':
      case 'or':
        walk(q.left);
        walk(q.right);
        break;
      case 'not':
        walk(q.include);
        // Don't include excluded terms
        break;
      case 'near':
        terms.push(...q.terms);
        break;
      case 'column':
        walk(q.query);
        break;
    }
  }

  walk(query);
  return [...new Set(terms)];
}
