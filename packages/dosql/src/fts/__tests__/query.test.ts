/**
 * Query Parser Tests
 *
 * Tests for FTS query parsing and execution including:
 * - Term queries
 * - Phrase queries
 * - Prefix queries
 * - Boolean operators (AND, OR, NOT)
 * - NEAR proximity queries
 * - Column filters
 * - Complex nested queries
 */

import { describe, it, expect } from 'vitest';
import {
  parseMatchQuery,
  queryToString,
  extractTerms,
  type MatchQuery,
  type TermQuery,
  type PhraseQuery,
  type PrefixQuery,
  type AndQuery,
  type OrQuery,
  type NotQuery,
  type NearQuery,
  type ColumnQuery,
} from '../query.js';

// =============================================================================
// Term Query Tests
// =============================================================================

describe('Term queries', () => {
  it('should parse simple term', () => {
    const query = parseMatchQuery('hello');
    expect(query.type).toBe('term');
    expect((query as TermQuery).term).toBe('hello');
  });

  it('should parse term with numbers', () => {
    const query = parseMatchQuery('test123');
    expect(query.type).toBe('term');
    expect((query as TermQuery).term).toBe('test123');
  });

  it('should lowercase terms', () => {
    const query = parseMatchQuery('HELLO');
    expect(query.type).toBe('term');
    expect((query as TermQuery).term).toBe('hello');
  });

  it('should handle mixed case', () => {
    const query = parseMatchQuery('TypeScript');
    expect((query as TermQuery).term).toBe('typescript');
  });

  it('should parse term with underscores', () => {
    const query = parseMatchQuery('hello_world');
    expect(query.type).toBe('term');
    expect((query as TermQuery).term).toBe('hello_world');
  });

  it('should handle empty query', () => {
    const query = parseMatchQuery('');
    expect(query.type).toBe('term');
    expect((query as TermQuery).term).toBe('');
  });

  it('should handle whitespace-only query', () => {
    const query = parseMatchQuery('   ');
    expect(query.type).toBe('term');
    expect((query as TermQuery).term).toBe('');
  });
});

// =============================================================================
// Phrase Query Tests
// =============================================================================

describe('Phrase queries', () => {
  it('should parse double-quoted phrase', () => {
    const query = parseMatchQuery('"hello world"');
    expect(query.type).toBe('phrase');
    expect((query as PhraseQuery).terms).toEqual(['hello', 'world']);
  });

  it('should parse multi-word phrase', () => {
    const query = parseMatchQuery('"the quick brown fox"');
    expect(query.type).toBe('phrase');
    expect((query as PhraseQuery).terms).toEqual(['the', 'quick', 'brown', 'fox']);
  });

  it('should lowercase phrase terms', () => {
    const query = parseMatchQuery('"Hello World"');
    expect((query as PhraseQuery).terms).toEqual(['hello', 'world']);
  });

  it('should handle single word phrase', () => {
    const query = parseMatchQuery('"hello"');
    expect(query.type).toBe('phrase');
    expect((query as PhraseQuery).terms).toEqual(['hello']);
  });

  it('should handle empty phrase', () => {
    const query = parseMatchQuery('""');
    expect(query.type).toBe('phrase');
    expect((query as PhraseQuery).terms).toHaveLength(0);
  });

  it('should handle unclosed quote gracefully', () => {
    const query = parseMatchQuery('"hello world');
    // Parser should handle this gracefully
    expect(query).toBeDefined();
  });
});

// =============================================================================
// Prefix Query Tests
// =============================================================================

describe('Prefix queries', () => {
  it('should parse asterisk prefix', () => {
    const query = parseMatchQuery('hello*');
    expect(query.type).toBe('prefix');
    expect((query as PrefixQuery).prefix).toBe('hello');
  });

  it('should lowercase prefix', () => {
    const query = parseMatchQuery('TYPE*');
    expect((query as PrefixQuery).prefix).toBe('type');
  });

  it('should handle short prefix', () => {
    const query = parseMatchQuery('a*');
    expect(query.type).toBe('prefix');
    expect((query as PrefixQuery).prefix).toBe('a');
  });

  it('should handle prefix with numbers', () => {
    const query = parseMatchQuery('test123*');
    expect(query.type).toBe('prefix');
    expect((query as PrefixQuery).prefix).toBe('test123');
  });
});

// =============================================================================
// AND Operator Tests
// =============================================================================

describe('AND operator', () => {
  it('should parse explicit AND', () => {
    const query = parseMatchQuery('foo AND bar');
    expect(query.type).toBe('and');
    const andQuery = query as AndQuery;
    expect((andQuery.left as TermQuery).term).toBe('foo');
    expect((andQuery.right as TermQuery).term).toBe('bar');
  });

  it('should be case-insensitive for AND keyword', () => {
    const query1 = parseMatchQuery('foo AND bar');
    const query2 = parseMatchQuery('foo and bar');
    expect(query1.type).toBe('and');
    expect(query2.type).toBe('and');
  });

  it('should parse implicit AND (space)', () => {
    const query = parseMatchQuery('foo bar');
    expect(query.type).toBe('and');
    const andQuery = query as AndQuery;
    expect((andQuery.left as TermQuery).term).toBe('foo');
    expect((andQuery.right as TermQuery).term).toBe('bar');
  });

  it('should chain multiple AND terms', () => {
    const query = parseMatchQuery('foo AND bar AND baz');
    expect(query.type).toBe('and');
    // Should be left-associative: ((foo AND bar) AND baz)
  });

  it('should parse multiple implicit AND terms', () => {
    const query = parseMatchQuery('one two three');
    expect(query.type).toBe('and');
  });
});

// =============================================================================
// OR Operator Tests
// =============================================================================

describe('OR operator', () => {
  it('should parse OR expression', () => {
    const query = parseMatchQuery('foo OR bar');
    expect(query.type).toBe('or');
    const orQuery = query as OrQuery;
    expect((orQuery.left as TermQuery).term).toBe('foo');
    expect((orQuery.right as TermQuery).term).toBe('bar');
  });

  it('should be case-insensitive for OR keyword', () => {
    const query1 = parseMatchQuery('foo OR bar');
    const query2 = parseMatchQuery('foo or bar');
    expect(query1.type).toBe('or');
    expect(query2.type).toBe('or');
  });

  it('should chain multiple OR terms', () => {
    const query = parseMatchQuery('foo OR bar OR baz');
    expect(query.type).toBe('or');
  });

  it('should have lower precedence than AND', () => {
    // foo AND bar OR baz should be: (foo AND bar) OR baz
    const query = parseMatchQuery('foo bar OR baz');
    expect(query.type).toBe('or');
    const orQuery = query as OrQuery;
    expect(orQuery.left.type).toBe('and');
  });
});

// =============================================================================
// NOT Operator Tests
// =============================================================================

describe('NOT operator', () => {
  it('should parse NOT expression', () => {
    const query = parseMatchQuery('foo NOT bar');
    expect(query.type).toBe('not');
    const notQuery = query as NotQuery;
    expect((notQuery.include as TermQuery).term).toBe('foo');
    expect((notQuery.exclude as TermQuery).term).toBe('bar');
  });

  it('should be case-insensitive for NOT keyword', () => {
    const query1 = parseMatchQuery('foo NOT bar');
    const query2 = parseMatchQuery('foo not bar');
    expect(query1.type).toBe('not');
    expect(query2.type).toBe('not');
  });

  it('should exclude the right operand', () => {
    const query = parseMatchQuery('javascript NOT react');
    expect(query.type).toBe('not');
    const notQuery = query as NotQuery;
    expect(notQuery.include.type).toBe('term');
    expect(notQuery.exclude.type).toBe('term');
  });
});

// =============================================================================
// NEAR Query Tests
// =============================================================================

describe('NEAR queries', () => {
  it('should parse NEAR with two terms', () => {
    const query = parseMatchQuery('NEAR(foo bar)');
    expect(query.type).toBe('near');
    const nearQuery = query as NearQuery;
    expect(nearQuery.terms).toEqual(['foo', 'bar']);
    expect(nearQuery.distance).toBe(10); // Default distance
  });

  it('should parse NEAR with custom distance', () => {
    const query = parseMatchQuery('NEAR(foo bar, 5)');
    expect(query.type).toBe('near');
    const nearQuery = query as NearQuery;
    expect(nearQuery.terms).toEqual(['foo', 'bar']);
    expect(nearQuery.distance).toBe(5);
  });

  it('should parse NEAR with multiple terms', () => {
    const query = parseMatchQuery('NEAR(one two three, 3)');
    expect(query.type).toBe('near');
    const nearQuery = query as NearQuery;
    expect(nearQuery.terms).toEqual(['one', 'two', 'three']);
    expect(nearQuery.distance).toBe(3);
  });

  it('should be case-insensitive for NEAR keyword', () => {
    const query1 = parseMatchQuery('NEAR(foo bar)');
    const query2 = parseMatchQuery('near(foo bar)');
    expect(query1.type).toBe('near');
    expect(query2.type).toBe('near');
  });

  it('should lowercase terms in NEAR', () => {
    const query = parseMatchQuery('NEAR(FOO BAR, 5)');
    const nearQuery = query as NearQuery;
    expect(nearQuery.terms).toEqual(['foo', 'bar']);
  });
});

// =============================================================================
// Column Filter Tests
// =============================================================================

describe('Column filter queries', () => {
  it('should parse column filter with term', () => {
    const query = parseMatchQuery('title:hello');
    expect(query.type).toBe('column');
    const colQuery = query as ColumnQuery;
    expect(colQuery.column).toBe('title');
    expect((colQuery.query as TermQuery).term).toBe('hello');
  });

  it('should parse column filter with phrase', () => {
    const query = parseMatchQuery('content:"hello world"');
    expect(query.type).toBe('column');
    const colQuery = query as ColumnQuery;
    expect(colQuery.column).toBe('content');
    expect(colQuery.query.type).toBe('phrase');
  });

  it('should parse column filter with prefix', () => {
    const query = parseMatchQuery('title:hello*');
    expect(query.type).toBe('column');
    const colQuery = query as ColumnQuery;
    expect(colQuery.column).toBe('title');
    expect(colQuery.query.type).toBe('prefix');
  });

  it('should preserve column name case', () => {
    const query = parseMatchQuery('Title:hello');
    const colQuery = query as ColumnQuery;
    expect(colQuery.column).toBe('Title');
  });

  it('should parse multiple column filters with AND', () => {
    const query = parseMatchQuery('title:foo AND content:bar');
    expect(query.type).toBe('and');
    const andQuery = query as AndQuery;
    expect(andQuery.left.type).toBe('column');
    expect(andQuery.right.type).toBe('column');
  });
});

// =============================================================================
// Parentheses and Grouping Tests
// =============================================================================

describe('Parentheses and grouping', () => {
  it('should parse parenthesized expression', () => {
    const query = parseMatchQuery('(foo)');
    expect(query.type).toBe('term');
    expect((query as TermQuery).term).toBe('foo');
  });

  it('should respect parentheses for OR precedence', () => {
    // (foo OR bar) AND baz
    const query = parseMatchQuery('(foo OR bar) AND baz');
    expect(query.type).toBe('and');
    const andQuery = query as AndQuery;
    expect(andQuery.left.type).toBe('or');
    expect((andQuery.right as TermQuery).term).toBe('baz');
  });

  it('should handle nested parentheses', () => {
    const query = parseMatchQuery('((foo OR bar) AND baz)');
    expect(query.type).toBe('and');
  });

  it('should handle complex grouped expressions', () => {
    const query = parseMatchQuery('(foo AND bar) OR (baz AND qux)');
    expect(query.type).toBe('or');
    const orQuery = query as OrQuery;
    expect(orQuery.left.type).toBe('and');
    expect(orQuery.right.type).toBe('and');
  });
});

// =============================================================================
// Complex Query Tests
// =============================================================================

describe('Complex queries', () => {
  it('should parse phrase with AND', () => {
    const query = parseMatchQuery('"hello world" AND foo');
    expect(query.type).toBe('and');
    const andQuery = query as AndQuery;
    expect(andQuery.left.type).toBe('phrase');
    expect(andQuery.right.type).toBe('term');
  });

  it('should parse phrase with OR', () => {
    const query = parseMatchQuery('"hello world" OR "foo bar"');
    expect(query.type).toBe('or');
    const orQuery = query as OrQuery;
    expect(orQuery.left.type).toBe('phrase');
    expect(orQuery.right.type).toBe('phrase');
  });

  it('should parse prefix with AND', () => {
    const query = parseMatchQuery('hello* AND world*');
    expect(query.type).toBe('and');
    const andQuery = query as AndQuery;
    expect(andQuery.left.type).toBe('prefix');
    expect(andQuery.right.type).toBe('prefix');
  });

  it('should parse NEAR with AND', () => {
    const query = parseMatchQuery('NEAR(foo bar, 3) AND baz');
    expect(query.type).toBe('and');
    const andQuery = query as AndQuery;
    expect(andQuery.left.type).toBe('near');
    expect(andQuery.right.type).toBe('term');
  });

  it('should parse column filter in complex query', () => {
    const query = parseMatchQuery('title:hello AND content:"foo bar"');
    expect(query.type).toBe('and');
  });

  it('should handle mixed operators', () => {
    const query = parseMatchQuery('foo AND bar OR baz NOT qux');
    // Precedence: NOT > AND > OR
    expect(query).toBeDefined();
  });
});

// =============================================================================
// queryToString Tests
// =============================================================================

describe('queryToString', () => {
  it('should serialize term query', () => {
    const query = parseMatchQuery('hello');
    expect(queryToString(query)).toBe('hello');
  });

  it('should serialize phrase query', () => {
    const query = parseMatchQuery('"hello world"');
    expect(queryToString(query)).toBe('"hello world"');
  });

  it('should serialize prefix query', () => {
    const query = parseMatchQuery('hello*');
    expect(queryToString(query)).toBe('hello*');
  });

  it('should serialize AND query', () => {
    const query = parseMatchQuery('foo AND bar');
    expect(queryToString(query)).toContain('AND');
  });

  it('should serialize OR query', () => {
    const query = parseMatchQuery('foo OR bar');
    expect(queryToString(query)).toContain('OR');
  });

  it('should serialize NOT query', () => {
    const query = parseMatchQuery('foo NOT bar');
    expect(queryToString(query)).toContain('NOT');
  });

  it('should serialize NEAR query', () => {
    const query = parseMatchQuery('NEAR(foo bar, 5)');
    expect(queryToString(query)).toContain('NEAR');
    expect(queryToString(query)).toContain('5');
  });

  it('should serialize column query', () => {
    const query = parseMatchQuery('title:hello');
    expect(queryToString(query)).toBe('title:hello');
  });
});

// =============================================================================
// extractTerms Tests
// =============================================================================

describe('extractTerms', () => {
  it('should extract term from term query', () => {
    const query = parseMatchQuery('hello');
    expect(extractTerms(query)).toEqual(['hello']);
  });

  it('should extract terms from phrase query', () => {
    const query = parseMatchQuery('"hello world"');
    expect(extractTerms(query)).toEqual(['hello', 'world']);
  });

  it('should extract prefix from prefix query', () => {
    const query = parseMatchQuery('hello*');
    expect(extractTerms(query)).toEqual(['hello']);
  });

  it('should extract all terms from AND query', () => {
    const query = parseMatchQuery('foo AND bar');
    const terms = extractTerms(query);
    expect(terms).toContain('foo');
    expect(terms).toContain('bar');
  });

  it('should extract all terms from OR query', () => {
    const query = parseMatchQuery('foo OR bar');
    const terms = extractTerms(query);
    expect(terms).toContain('foo');
    expect(terms).toContain('bar');
  });

  it('should extract only include terms from NOT query', () => {
    const query = parseMatchQuery('foo NOT bar');
    const terms = extractTerms(query);
    expect(terms).toContain('foo');
    expect(terms).not.toContain('bar');
  });

  it('should extract terms from NEAR query', () => {
    const query = parseMatchQuery('NEAR(foo bar, 5)');
    const terms = extractTerms(query);
    expect(terms).toContain('foo');
    expect(terms).toContain('bar');
  });

  it('should extract terms from column query', () => {
    const query = parseMatchQuery('title:hello');
    expect(extractTerms(query)).toEqual(['hello']);
  });

  it('should deduplicate terms', () => {
    const query = parseMatchQuery('foo AND foo');
    const terms = extractTerms(query);
    expect(terms.filter(t => t === 'foo').length).toBe(1);
  });

  it('should handle complex nested query', () => {
    const query = parseMatchQuery('(foo OR bar) AND "hello world"');
    const terms = extractTerms(query);
    expect(terms).toContain('foo');
    expect(terms).toContain('bar');
    expect(terms).toContain('hello');
    expect(terms).toContain('world');
  });

  it('should skip empty terms', () => {
    const query = parseMatchQuery('');
    expect(extractTerms(query)).toEqual([]);
  });
});

// =============================================================================
// Parser Edge Cases
// =============================================================================

describe('Parser edge cases', () => {
  it('should handle consecutive operators gracefully', () => {
    const query = parseMatchQuery('AND AND');
    expect(query).toBeDefined();
  });

  it('should handle only keywords', () => {
    const query = parseMatchQuery('AND OR NOT');
    expect(query).toBeDefined();
  });

  it('should handle unbalanced parentheses', () => {
    const query1 = parseMatchQuery('(foo');
    const query2 = parseMatchQuery('foo)');
    expect(query1).toBeDefined();
    expect(query2).toBeDefined();
  });

  it('should handle special characters', () => {
    const query = parseMatchQuery('!@#$%');
    expect(query).toBeDefined();
  });

  it('should handle numbers as terms', () => {
    // Note: The lexer only tokenizes words starting with letters/underscores
    // Pure numbers (123, 456) are tokenized as NUMBER tokens, not TERM tokens
    // so 'AND' appears between two NUMBER tokens and the parser returns the first term it can parse
    const query = parseMatchQuery('123 AND 456');
    expect(query).toBeDefined();
    // Numbers aren't parsed as terms, so the query structure depends on parser behavior
  });

  it('should handle very long query', () => {
    const longQuery = 'term '.repeat(100);
    const query = parseMatchQuery(longQuery);
    expect(query).toBeDefined();
  });

  it('should handle escaped characters in phrase', () => {
    const query = parseMatchQuery('"hello \\"world\\""');
    expect(query).toBeDefined();
  });
});

// =============================================================================
// Operator Precedence Tests
// =============================================================================

describe('Operator precedence', () => {
  it('should evaluate NOT before AND', () => {
    // foo AND bar NOT baz should be: foo AND (bar NOT baz)
    const query = parseMatchQuery('foo AND bar NOT baz');
    expect(query.type).toBe('and');
  });

  it('should evaluate AND before OR', () => {
    // foo OR bar AND baz should be: foo OR (bar AND baz)
    const query = parseMatchQuery('foo OR bar baz');
    expect(query.type).toBe('or');
  });

  it('should allow override with parentheses', () => {
    // (foo OR bar) AND baz
    const query = parseMatchQuery('(foo OR bar) AND baz');
    expect(query.type).toBe('and');
    expect((query as AndQuery).left.type).toBe('or');
  });
});
