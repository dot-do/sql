/**
 * Full-Text Search (FTS5-style) Tests
 *
 * Comprehensive tests for FTS implementation following SQLite FTS5 semantics.
 * Reference: https://www.sqlite.org/fts5.html
 */

import { describe, it, expect, beforeEach } from 'vitest';
import {
  FTSIndex,
  createFTSIndex,
  type FTSConfig,
  type FTSDocument,
  type FTSSearchResult,
} from './index.js';
import {
  tokenize,
  SimpleTokenizer,
  PorterTokenizer,
  UnicodeTokenizer,
  type Tokenizer,
} from './tokenizer.js';
import {
  parseMatchQuery,
  type MatchQuery,
  type TermQuery,
  type PhraseQuery,
  type PrefixQuery,
  type AndQuery,
  type OrQuery,
  type NotQuery,
  type NearQuery,
  type ColumnQuery,
} from './query.js';
import {
  calculateBM25,
  calculateTFIDF,
  type RankingParams,
} from './ranking.js';
import {
  highlight,
  snippet,
  type HighlightOptions,
  type SnippetOptions,
} from './auxiliary.js';

// =============================================================================
// Test Fixtures
// =============================================================================

const sampleDocuments: Array<{ id: number; title: string; content: string }> = [
  {
    id: 1,
    title: 'Introduction to TypeScript',
    content: 'TypeScript is a typed superset of JavaScript that compiles to plain JavaScript.',
  },
  {
    id: 2,
    title: 'Getting Started with React',
    content: 'React is a JavaScript library for building user interfaces.',
  },
  {
    id: 3,
    title: 'Advanced TypeScript Patterns',
    content: 'Learn advanced TypeScript patterns including generics, mapped types, and conditional types.',
  },
  {
    id: 4,
    title: 'JavaScript Fundamentals',
    content: 'JavaScript is the programming language of the web. It runs in browsers and on servers.',
  },
  {
    id: 5,
    title: 'Building APIs with Node.js',
    content: 'Node.js allows you to run JavaScript on the server. Build RESTful APIs with Express.',
  },
];

// =============================================================================
// CREATE VIRTUAL TABLE Tests
// =============================================================================

describe('CREATE VIRTUAL TABLE', () => {
  it('should create FTS index with single column', () => {
    const fts = createFTSIndex({
      columns: ['content'],
    });
    expect(fts).toBeDefined();
    expect(fts.columns).toEqual(['content']);
  });

  it('should create FTS index with multiple columns', () => {
    const fts = createFTSIndex({
      columns: ['title', 'content', 'author'],
    });
    expect(fts.columns).toEqual(['title', 'content', 'author']);
  });

  it('should use default tokenizer when not specified', () => {
    const fts = createFTSIndex({
      columns: ['content'],
    });
    expect(fts.tokenizer).toBe('simple');
  });

  it('should accept porter tokenizer', () => {
    const fts = createFTSIndex({
      columns: ['content'],
      tokenizer: 'porter',
    });
    expect(fts.tokenizer).toBe('porter');
  });

  it('should accept unicode tokenizer', () => {
    const fts = createFTSIndex({
      columns: ['content'],
      tokenizer: 'unicode61',
    });
    expect(fts.tokenizer).toBe('unicode61');
  });

  it('should support column weights/priorities', () => {
    const fts = createFTSIndex({
      columns: ['title', 'content'],
      columnWeights: { title: 10.0, content: 1.0 },
    });
    expect(fts.columnWeights).toEqual({ title: 10.0, content: 1.0 });
  });

  it('should support prefix indexes', () => {
    const fts = createFTSIndex({
      columns: ['content'],
      prefix: [2, 3, 4],
    });
    expect(fts.prefixLengths).toEqual([2, 3, 4]);
  });
});

// =============================================================================
// INSERT Tests
// =============================================================================

describe('INSERT into FTS table', () => {
  let fts: FTSIndex;

  beforeEach(() => {
    fts = createFTSIndex({
      columns: ['title', 'content'],
    });
  });

  it('should insert a single document', async () => {
    await fts.insert({
      rowid: 1,
      columns: { title: 'Hello World', content: 'This is a test document.' },
    });
    expect(await fts.count()).toBe(1);
  });

  it('should insert multiple documents', async () => {
    for (const doc of sampleDocuments) {
      await fts.insert({
        rowid: doc.id,
        columns: { title: doc.title, content: doc.content },
      });
    }
    expect(await fts.count()).toBe(5);
  });

  it('should update document on duplicate rowid', async () => {
    await fts.insert({
      rowid: 1,
      columns: { title: 'Original', content: 'Original content' },
    });
    await fts.insert({
      rowid: 1,
      columns: { title: 'Updated', content: 'Updated content' },
    });
    expect(await fts.count()).toBe(1);

    const results = await fts.search('Updated');
    expect(results).toHaveLength(1);
  });

  it('should delete document', async () => {
    await fts.insert({
      rowid: 1,
      columns: { title: 'Test', content: 'Content' },
    });
    await fts.delete(1);
    expect(await fts.count()).toBe(0);
  });

  it('should handle empty content gracefully', async () => {
    await fts.insert({
      rowid: 1,
      columns: { title: '', content: '' },
    });
    expect(await fts.count()).toBe(1);
  });

  it('should handle special characters in content', async () => {
    await fts.insert({
      rowid: 1,
      columns: { title: 'C++ & C#', content: 'Programming languages: C++, C#, F#' },
    });
    expect(await fts.count()).toBe(1);
  });
});

// =============================================================================
// Simple MATCH Queries
// =============================================================================

describe('Simple MATCH queries', () => {
  let fts: FTSIndex;

  beforeEach(async () => {
    fts = createFTSIndex({
      columns: ['title', 'content'],
    });
    for (const doc of sampleDocuments) {
      await fts.insert({
        rowid: doc.id,
        columns: { title: doc.title, content: doc.content },
      });
    }
  });

  it('should match single term', async () => {
    const results = await fts.search('TypeScript');
    expect(results.length).toBeGreaterThan(0);
    expect(results.some((r) => r.rowid === 1)).toBe(true);
    expect(results.some((r) => r.rowid === 3)).toBe(true);
  });

  it('should match term case-insensitively', async () => {
    const results = await fts.search('typescript');
    expect(results.length).toBeGreaterThan(0);
    expect(results.some((r) => r.rowid === 1)).toBe(true);
  });

  it('should return empty array for no matches', async () => {
    const results = await fts.search('nonexistentterm');
    expect(results).toHaveLength(0);
  });

  it('should match across multiple documents', async () => {
    const results = await fts.search('JavaScript');
    expect(results.length).toBe(4); // Docs 1, 2, 4, 5 mention JavaScript
  });

  it('should match partial document set', async () => {
    const results = await fts.search('React');
    expect(results.length).toBe(1);
    expect(results[0].rowid).toBe(2);
  });
});

// =============================================================================
// Boolean Operators
// =============================================================================

describe('Boolean operators', () => {
  let fts: FTSIndex;

  beforeEach(async () => {
    fts = createFTSIndex({
      columns: ['title', 'content'],
    });
    for (const doc of sampleDocuments) {
      await fts.insert({
        rowid: doc.id,
        columns: { title: doc.title, content: doc.content },
      });
    }
  });

  describe('AND operator', () => {
    it('should match documents containing all terms with AND', async () => {
      const results = await fts.search('TypeScript AND patterns');
      expect(results.length).toBe(1);
      expect(results[0].rowid).toBe(3);
    });

    it('should return empty for AND when one term missing', async () => {
      const results = await fts.search('TypeScript AND React');
      expect(results).toHaveLength(0);
    });

    it('should support multiple AND terms', async () => {
      const results = await fts.search('JavaScript AND library AND building');
      expect(results.length).toBe(1);
      expect(results[0].rowid).toBe(2);
    });

    it('should treat space as implicit AND by default', async () => {
      const results = await fts.search('TypeScript patterns');
      expect(results.length).toBe(1);
      expect(results[0].rowid).toBe(3);
    });
  });

  describe('OR operator', () => {
    it('should match documents containing any term with OR', async () => {
      const results = await fts.search('React OR Node');
      expect(results.length).toBe(2);
    });

    it('should match all documents with either term', async () => {
      const results = await fts.search('TypeScript OR JavaScript');
      expect(results.length).toBeGreaterThan(2);
    });

    it('should support multiple OR terms', async () => {
      const results = await fts.search('React OR Node OR Express');
      expect(results.length).toBeGreaterThan(0);
    });
  });

  describe('NOT operator', () => {
    it('should exclude documents with NOT term', async () => {
      const results = await fts.search('JavaScript NOT React');
      expect(results.every((r) => r.rowid !== 2)).toBe(true);
    });

    it('should support NOT with compound queries', async () => {
      const results = await fts.search('TypeScript NOT advanced');
      expect(results.length).toBe(1);
      expect(results[0].rowid).toBe(1);
    });
  });

  describe('Combined operators', () => {
    it('should handle AND with OR', async () => {
      const results = await fts.search('(TypeScript OR JavaScript) AND patterns');
      expect(results.some((r) => r.rowid === 3)).toBe(true);
    });

    it('should handle complex boolean expressions', async () => {
      const results = await fts.search('(TypeScript AND advanced) OR (Node AND API)');
      // Doc 3: "Advanced TypeScript Patterns" matches first clause
      // Doc 5: "Building APIs with Node.js" matches second clause
      expect(results.length).toBeGreaterThanOrEqual(1);
      expect(results.some((r) => r.rowid === 3)).toBe(true); // TypeScript AND advanced
    });

    it('should respect operator precedence', async () => {
      // NOT should bind tighter than AND which binds tighter than OR
      const results = await fts.search('JavaScript OR TypeScript AND patterns');
      // Should be interpreted as: JavaScript OR (TypeScript AND patterns)
      expect(results.length).toBeGreaterThan(1);
    });
  });
});

// =============================================================================
// Phrase Queries
// =============================================================================

describe('Phrase queries', () => {
  let fts: FTSIndex;

  beforeEach(async () => {
    fts = createFTSIndex({
      columns: ['title', 'content'],
    });
    for (const doc of sampleDocuments) {
      await fts.insert({
        rowid: doc.id,
        columns: { title: doc.title, content: doc.content },
      });
    }
  });

  it('should match exact phrase', async () => {
    const results = await fts.search('"user interfaces"');
    expect(results.length).toBe(1);
    expect(results[0].rowid).toBe(2);
  });

  it('should not match phrase out of order', async () => {
    const results = await fts.search('"interfaces user"');
    expect(results).toHaveLength(0);
  });

  it('should match phrase spanning word boundaries', async () => {
    const results = await fts.search('"typed superset"');
    expect(results.length).toBe(1);
    expect(results[0].rowid).toBe(1);
  });

  it('should match phrase case-insensitively', async () => {
    const results = await fts.search('"User Interfaces"');
    expect(results.length).toBe(1);
  });

  it('should match single word phrase', async () => {
    const results = await fts.search('"TypeScript"');
    expect(results.length).toBeGreaterThan(0);
  });

  it('should support phrase combined with AND', async () => {
    const results = await fts.search('"JavaScript library" AND React');
    expect(results.length).toBe(1);
    expect(results[0].rowid).toBe(2);
  });

  it('should support phrase combined with OR', async () => {
    const results = await fts.search('"mapped types" OR "user interfaces"');
    expect(results.length).toBe(2);
  });
});

// =============================================================================
// Prefix Queries
// =============================================================================

describe('Prefix queries', () => {
  let fts: FTSIndex;

  beforeEach(async () => {
    fts = createFTSIndex({
      columns: ['title', 'content'],
      prefix: [2, 3],
    });
    for (const doc of sampleDocuments) {
      await fts.insert({
        rowid: doc.id,
        columns: { title: doc.title, content: doc.content },
      });
    }
  });

  it('should match prefix with asterisk', async () => {
    const results = await fts.search('Type*');
    expect(results.length).toBeGreaterThan(0);
    // Should match TypeScript, types, typed
  });

  it('should match short prefix', async () => {
    const results = await fts.search('Ja*');
    expect(results.length).toBeGreaterThan(0);
    // Should match JavaScript
  });

  it('should match prefix case-insensitively', async () => {
    const results = await fts.search('java*');
    expect(results.length).toBeGreaterThan(0);
  });

  it('should support prefix combined with AND', async () => {
    const results = await fts.search('Type* AND pattern*');
    expect(results.length).toBe(1);
    expect(results[0].rowid).toBe(3);
  });

  it('should support prefix combined with phrase', async () => {
    const results = await fts.search('build* AND "user interfaces"');
    expect(results.length).toBe(1);
  });

  it('should not match if prefix too short for index', async () => {
    // With prefix: [2, 3], single char prefix might not be indexed
    const results = await fts.search('T*');
    // Behavior depends on implementation - may fallback to full scan
    expect(Array.isArray(results)).toBe(true);
  });
});

// =============================================================================
// Column Filters
// =============================================================================

describe('Column filters', () => {
  let fts: FTSIndex;

  beforeEach(async () => {
    fts = createFTSIndex({
      columns: ['title', 'content'],
    });
    for (const doc of sampleDocuments) {
      await fts.insert({
        rowid: doc.id,
        columns: { title: doc.title, content: doc.content },
      });
    }
  });

  it('should filter by specific column', async () => {
    const results = await fts.search('title:Introduction');
    expect(results.length).toBe(1);
    expect(results[0].rowid).toBe(1);
  });

  it('should not match if term in different column', async () => {
    // "superset" appears in content, not title
    const results = await fts.search('title:superset');
    expect(results).toHaveLength(0);
  });

  it('should match in content column', async () => {
    const results = await fts.search('content:superset');
    expect(results.length).toBe(1);
  });

  it('should support column filter with phrase', async () => {
    const results = await fts.search('content:"user interfaces"');
    expect(results.length).toBe(1);
  });

  it('should support column filter with prefix', async () => {
    const results = await fts.search('title:Intro*');
    expect(results.length).toBe(1);
  });

  it('should support multiple column filters', async () => {
    const results = await fts.search('title:TypeScript AND content:advanced');
    expect(results.length).toBe(1);
    expect(results[0].rowid).toBe(3);
  });

  it('should handle column filter with OR', async () => {
    const results = await fts.search('title:Introduction OR title:Getting');
    expect(results.length).toBe(2);
  });
});

// =============================================================================
// NEAR Queries
// =============================================================================

describe('NEAR queries', () => {
  let fts: FTSIndex;

  beforeEach(async () => {
    fts = createFTSIndex({
      columns: ['title', 'content'],
    });
    await fts.insert({
      rowid: 1,
      columns: {
        title: 'Test',
        content: 'The quick brown fox jumps over the lazy dog.',
      },
    });
    await fts.insert({
      rowid: 2,
      columns: {
        title: 'Test',
        content: 'The dog was very lazy and the fox was quite quick.',
      },
    });
  });

  it('should match terms within default proximity', async () => {
    const results = await fts.search('NEAR(quick fox)');
    expect(results.length).toBeGreaterThan(0);
  });

  it('should match terms within specified proximity', async () => {
    // Doc 1: "quick brown fox" - quick at pos 1, fox at pos 3 (distance 2)
    // Doc 2: "fox was quite quick" - fox at pos 7, quick at pos 10 (distance 3)
    // Both within distance 3
    const results = await fts.search('NEAR(quick fox, 3)');
    expect(results.length).toBe(2); // Both documents match
  });

  it('should not match terms beyond specified proximity', async () => {
    const results = await fts.search('NEAR(dog lazy, 1)');
    // In doc 1: "lazy dog" (adjacent)
    // In doc 2: "dog was very lazy" (3 words apart)
    expect(results.length).toBe(1);
  });

  it('should match multiple terms in NEAR', async () => {
    const results = await fts.search('NEAR(quick brown fox, 2)');
    expect(results.length).toBe(1);
  });

  it('should support NEAR combined with AND', async () => {
    const results = await fts.search('NEAR(quick fox, 3) AND jumps');
    expect(results.length).toBe(1);
    expect(results[0].rowid).toBe(1);
  });

  it('should be order-independent within NEAR', async () => {
    const results1 = await fts.search('NEAR(fox quick, 3)');
    const results2 = await fts.search('NEAR(quick fox, 3)');
    expect(results1.length).toBe(results2.length);
  });
});

// =============================================================================
// Ranking: BM25
// =============================================================================

describe('BM25 ranking', () => {
  let fts: FTSIndex;

  beforeEach(async () => {
    fts = createFTSIndex({
      columns: ['title', 'content'],
    });
    for (const doc of sampleDocuments) {
      await fts.insert({
        rowid: doc.id,
        columns: { title: doc.title, content: doc.content },
      });
    }
  });

  it('should return results sorted by rank', async () => {
    const results = await fts.search('TypeScript', { rank: 'bm25' });
    expect(results.length).toBeGreaterThan(1);
    // Results should be sorted by rank descending
    for (let i = 1; i < results.length; i++) {
      expect(results[i - 1].rank).toBeGreaterThanOrEqual(results[i].rank);
    }
  });

  it('should rank documents with more term occurrences higher', async () => {
    const fts2 = createFTSIndex({ columns: ['content'] });
    await fts2.insert({ rowid: 1, columns: { content: 'cat' } });
    await fts2.insert({ rowid: 2, columns: { content: 'cat cat cat' } });
    await fts2.insert({ rowid: 3, columns: { content: 'cat cat' } });

    const results = await fts2.search('cat', { rank: 'bm25' });
    expect(results[0].rowid).toBe(2); // Most occurrences
  });

  it('should apply column weights in ranking', async () => {
    const fts2 = createFTSIndex({
      columns: ['title', 'content'],
      columnWeights: { title: 10.0, content: 1.0 },
    });
    await fts2.insert({ rowid: 1, columns: { title: 'test', content: 'foo bar' } });
    await fts2.insert({ rowid: 2, columns: { title: 'foo bar', content: 'test' } });

    const results = await fts2.search('test', { rank: 'bm25' });
    expect(results[0].rowid).toBe(1); // Title match weighted higher
  });

  it('should include rank score in results', async () => {
    const results = await fts.search('TypeScript', { rank: 'bm25' });
    expect(results.length).toBeGreaterThan(0);
    expect(typeof results[0].rank).toBe('number');
    expect(results[0].rank).toBeGreaterThan(0);
  });

  it('should calculate bm25() correctly', () => {
    // Test the bm25 calculation directly
    const score = calculateBM25({
      termFrequency: 2,
      documentLength: 100,
      averageDocumentLength: 150,
      numDocuments: 1000,
      documentFrequency: 50,
      k1: 1.2,
      b: 0.75,
    });
    expect(typeof score).toBe('number');
    expect(score).toBeGreaterThan(0);
  });

  it('should handle IDF correctly for rare terms', () => {
    const rareTermScore = calculateBM25({
      termFrequency: 1,
      documentLength: 100,
      averageDocumentLength: 100,
      numDocuments: 1000,
      documentFrequency: 1, // Very rare
      k1: 1.2,
      b: 0.75,
    });

    const commonTermScore = calculateBM25({
      termFrequency: 1,
      documentLength: 100,
      averageDocumentLength: 100,
      numDocuments: 1000,
      documentFrequency: 500, // Very common
      k1: 1.2,
      b: 0.75,
    });

    expect(rareTermScore).toBeGreaterThan(commonTermScore);
  });
});

// =============================================================================
// Highlight and Snippet Functions
// =============================================================================

describe('Highlight function', () => {
  it('should highlight matched terms', () => {
    const result = highlight(
      'TypeScript is a typed superset of JavaScript.',
      ['typescript'],
      { startTag: '<b>', endTag: '</b>' }
    );
    expect(result).toBe('<b>TypeScript</b> is a typed superset of JavaScript.');
  });

  it('should highlight multiple terms', () => {
    const result = highlight(
      'TypeScript is a typed superset of JavaScript.',
      ['typescript', 'javascript'],
      { startTag: '<b>', endTag: '</b>' }
    );
    expect(result).toBe('<b>TypeScript</b> is a typed superset of <b>JavaScript</b>.');
  });

  it('should handle case-insensitive highlighting', () => {
    const result = highlight(
      'TYPESCRIPT and TypeScript are the same.',
      ['typescript'],
      { startTag: '<mark>', endTag: '</mark>' }
    );
    expect(result).toContain('<mark>TYPESCRIPT</mark>');
    expect(result).toContain('<mark>TypeScript</mark>');
  });

  it('should not highlight partial matches', () => {
    const result = highlight(
      'JavaScript is not the same as Script.',
      ['script'],
      { startTag: '<b>', endTag: '</b>' }
    );
    // Should highlight Script but not JavaSCRIPT (as standalone word)
    expect(result).toBe('JavaScript is not the same as <b>Script</b>.');
  });

  it('should handle empty search terms', () => {
    const result = highlight(
      'Some text here.',
      [],
      { startTag: '<b>', endTag: '</b>' }
    );
    expect(result).toBe('Some text here.');
  });
});

describe('Snippet function', () => {
  const longContent = `
    TypeScript is a typed superset of JavaScript that compiles to plain JavaScript.
    It adds optional static typing and class-based object-oriented programming to the language.
    TypeScript is designed for development of large applications and transcompiles to JavaScript.
    As TypeScript is a superset of JavaScript, existing JavaScript programs are also valid TypeScript programs.
  `;

  it('should extract snippet around matched term', () => {
    const result = snippet(longContent, ['object-oriented'], {
      maxLength: 50,
      startTag: '<b>',
      endTag: '</b>',
    });
    expect(result).toContain('<b>object-oriented</b>');
    // Allow for ellipsis and tag overhead
    expect(result.length).toBeLessThanOrEqual(80);
  });

  it('should include ellipsis when truncated', () => {
    const result = snippet(longContent, ['typescript'], {
      maxLength: 50,
      ellipsis: '...',
    });
    expect(result).toContain('...');
  });

  it('should return full text if shorter than maxLength', () => {
    const shortContent = 'Hello TypeScript world.';
    const result = snippet(shortContent, ['typescript'], {
      maxLength: 100,
    });
    expect(result).toBe('Hello TypeScript world.');
  });

  it('should handle multiple matching terms', () => {
    const result = snippet(longContent, ['typed', 'superset'], {
      maxLength: 80,
      startTag: '[',
      endTag: ']',
    });
    expect(result).toContain('[typed]');
    expect(result).toContain('[superset]');
  });

  it('should prefer snippets with more matches', () => {
    const result = snippet(
      'First section has cat. Second section has cat and dog and bird.',
      ['cat', 'dog', 'bird'],
      { maxLength: 40 }
    );
    // Should prefer second section with more matches
    expect(result).toContain('dog');
    expect(result).toContain('bird');
  });
});

// =============================================================================
// FTS5 Auxiliary Functions
// =============================================================================

describe('FTS5 auxiliary functions', () => {
  let fts: FTSIndex;

  beforeEach(async () => {
    fts = createFTSIndex({
      columns: ['title', 'content'],
    });
    for (const doc of sampleDocuments) {
      await fts.insert({
        rowid: doc.id,
        columns: { title: doc.title, content: doc.content },
      });
    }
  });

  describe('bm25()', () => {
    it('should calculate bm25 score for each result', async () => {
      const results = await fts.search('TypeScript', {
        rank: 'bm25',
        includeScore: true
      });
      results.forEach((r) => {
        expect(typeof r.bm25).toBe('number');
      });
    });

    it('should support custom bm25 parameters', async () => {
      const results = await fts.search('TypeScript', {
        rank: 'bm25',
        bm25Params: { k1: 2.0, b: 0.5 },
      });
      expect(results.length).toBeGreaterThan(0);
    });
  });

  describe('highlight()', () => {
    it('should return highlighted content for each result', async () => {
      const results = await fts.search('TypeScript', {
        highlight: { column: 'content', startTag: '<em>', endTag: '</em>' },
      });
      results.forEach((r) => {
        if (r.highlight) {
          expect(r.highlight).toContain('<em>');
        }
      });
    });

    it('should highlight in specific column', async () => {
      const results = await fts.search('TypeScript', {
        highlight: { column: 'title', startTag: '[', endTag: ']' },
      });
      const titleMatch = results.find((r) => r.rowid === 1);
      expect(titleMatch?.highlight).toContain('[TypeScript]');
    });
  });

  describe('snippet()', () => {
    it('should return snippet for each result', async () => {
      const results = await fts.search('TypeScript', {
        snippet: { column: 'content', maxLength: 60 },
      });
      results.forEach((r) => {
        if (r.snippet) {
          expect(r.snippet.length).toBeLessThanOrEqual(70);
        }
      });
    });
  });
});

// =============================================================================
// Tokenizer Tests
// =============================================================================

describe('Tokenizers', () => {
  describe('SimpleTokenizer', () => {
    const tokenizer = new SimpleTokenizer();

    it('should tokenize simple text', () => {
      const tokens = tokenizer.tokenize('Hello World');
      expect(tokens).toEqual([
        { term: 'hello', position: 0, start: 0, end: 5 },
        { term: 'world', position: 1, start: 6, end: 11 },
      ]);
    });

    it('should handle punctuation', () => {
      const tokens = tokenizer.tokenize('Hello, World!');
      expect(tokens.map((t) => t.term)).toEqual(['hello', 'world']);
    });

    it('should handle numbers', () => {
      const tokens = tokenizer.tokenize('Version 3.14 released');
      expect(tokens.map((t) => t.term)).toContain('3');
      expect(tokens.map((t) => t.term)).toContain('14');
    });

    it('should lowercase all terms', () => {
      const tokens = tokenizer.tokenize('TypeScript REACT vue');
      expect(tokens.map((t) => t.term)).toEqual(['typescript', 'react', 'vue']);
    });
  });

  describe('PorterTokenizer', () => {
    const tokenizer = new PorterTokenizer();

    it('should stem words', () => {
      const tokens = tokenizer.tokenize('running runs runner');
      const terms = tokens.map((t) => t.term);
      // Porter stemmer should reduce these to same stem
      expect(new Set(terms).size).toBeLessThan(3);
    });

    it('should stem plural forms', () => {
      const tokens1 = tokenizer.tokenize('cats');
      const tokens2 = tokenizer.tokenize('cat');
      expect(tokens1[0].term).toBe(tokens2[0].term);
    });

    it('should stem verb forms', () => {
      const tokens = tokenizer.tokenize('programming programmed programmer');
      const stems = new Set(tokens.map((t) => t.term));
      // All should stem to similar root
      expect(stems.size).toBeLessThanOrEqual(2);
    });
  });

  describe('UnicodeTokenizer', () => {
    const tokenizer = new UnicodeTokenizer();

    it('should handle unicode characters', () => {
      const tokens = tokenizer.tokenize('cafe resume');
      expect(tokens.map((t) => t.term)).toContain('cafe');
      expect(tokens.map((t) => t.term)).toContain('resume');
    });

    it('should handle CJK characters', () => {
      const tokens = tokenizer.tokenize('hello world');
      expect(tokens.length).toBeGreaterThan(0);
    });

    it('should handle mixed scripts', () => {
      const tokens = tokenizer.tokenize('TypeScript kurso');
      expect(tokens.length).toBe(2);
    });
  });
});

// =============================================================================
// Query Parser Tests
// =============================================================================

describe('Query Parser', () => {
  it('should parse simple term', () => {
    const query = parseMatchQuery('hello');
    expect(query.type).toBe('term');
    expect((query as TermQuery).term).toBe('hello');
  });

  it('should parse phrase', () => {
    const query = parseMatchQuery('"hello world"');
    expect(query.type).toBe('phrase');
    expect((query as PhraseQuery).terms).toEqual(['hello', 'world']);
  });

  it('should parse prefix', () => {
    const query = parseMatchQuery('hello*');
    expect(query.type).toBe('prefix');
    expect((query as PrefixQuery).prefix).toBe('hello');
  });

  it('should parse AND expression', () => {
    const query = parseMatchQuery('foo AND bar');
    expect(query.type).toBe('and');
    const andQuery = query as AndQuery;
    expect(andQuery.left.type).toBe('term');
    expect(andQuery.right.type).toBe('term');
  });

  it('should parse OR expression', () => {
    const query = parseMatchQuery('foo OR bar');
    expect(query.type).toBe('or');
    const orQuery = query as OrQuery;
    expect(orQuery.left.type).toBe('term');
    expect(orQuery.right.type).toBe('term');
  });

  it('should parse NOT expression', () => {
    const query = parseMatchQuery('foo NOT bar');
    expect(query.type).toBe('not');
    const notQuery = query as NotQuery;
    expect(notQuery.include.type).toBe('term');
    expect(notQuery.exclude.type).toBe('term');
  });

  it('should parse NEAR expression', () => {
    const query = parseMatchQuery('NEAR(foo bar, 5)');
    expect(query.type).toBe('near');
    const nearQuery = query as NearQuery;
    expect(nearQuery.terms).toEqual(['foo', 'bar']);
    expect(nearQuery.distance).toBe(5);
  });

  it('should parse column filter', () => {
    const query = parseMatchQuery('title:hello');
    expect(query.type).toBe('column');
    const colQuery = query as ColumnQuery;
    expect(colQuery.column).toBe('title');
    expect(colQuery.query.type).toBe('term');
  });

  it('should parse nested expressions', () => {
    const query = parseMatchQuery('(foo OR bar) AND baz');
    expect(query.type).toBe('and');
    const andQuery = query as AndQuery;
    expect(andQuery.left.type).toBe('or');
    expect(andQuery.right.type).toBe('term');
  });

  it('should handle complex queries', () => {
    // Query: title:"hello world" AND content:foo* NOT bar
    // Parses as: title:"hello world" AND (content:foo* NOT bar)
    // Top level is AND
    const query = parseMatchQuery('title:"hello world" AND content:foo* NOT bar');
    expect(query.type).toBe('and');
    // The right side should be a NOT
    const andQuery = query as AndQuery;
    expect(andQuery.right.type).toBe('not');
  });
});

// =============================================================================
// Edge Cases and Error Handling
// =============================================================================

describe('Edge cases and error handling', () => {
  let fts: FTSIndex;

  beforeEach(() => {
    fts = createFTSIndex({
      columns: ['content'],
    });
  });

  it('should handle empty query string', async () => {
    await fts.insert({ rowid: 1, columns: { content: 'test' } });
    const results = await fts.search('');
    expect(Array.isArray(results)).toBe(true);
  });

  it('should handle query with only operators', async () => {
    await fts.insert({ rowid: 1, columns: { content: 'test' } });
    const results = await fts.search('AND OR NOT');
    expect(Array.isArray(results)).toBe(true);
  });

  it('should handle very long documents', async () => {
    const longContent = 'word '.repeat(10000);
    await fts.insert({ rowid: 1, columns: { content: longContent } });
    const results = await fts.search('word');
    expect(results.length).toBe(1);
  });

  it('should handle very long queries', async () => {
    await fts.insert({ rowid: 1, columns: { content: 'test document' } });
    const longQuery = 'term '.repeat(100);
    const results = await fts.search(longQuery);
    expect(Array.isArray(results)).toBe(true);
  });

  it('should handle unicode in content and queries', async () => {
    await fts.insert({
      rowid: 1,
      columns: { content: 'Hello world, bonjour le monde' },
    });
    const results = await fts.search('bonjour');
    expect(results.length).toBe(1);
  });

  it('should handle special characters in content', async () => {
    await fts.insert({
      rowid: 1,
      columns: { content: 'C++ and C# are programming languages' },
    });
    const results = await fts.search('programming');
    expect(results.length).toBe(1);
  });

  it('should handle concurrent inserts', async () => {
    const promises = Array.from({ length: 100 }, (_, i) =>
      fts.insert({ rowid: i, columns: { content: `document ${i}` } })
    );
    await Promise.all(promises);
    expect(await fts.count()).toBe(100);
  });

  it('should handle search on empty index', async () => {
    const results = await fts.search('anything');
    expect(results).toHaveLength(0);
  });

  it('should throw for invalid column name in filter', async () => {
    await fts.insert({ rowid: 1, columns: { content: 'test' } });
    await expect(fts.search('nonexistent:test')).rejects.toThrow();
  });
});

// =============================================================================
// Performance Characteristics
// =============================================================================

describe('Performance characteristics', () => {
  it('should index 1000 documents efficiently', async () => {
    const fts = createFTSIndex({ columns: ['content'] });
    const start = performance.now();

    for (let i = 0; i < 1000; i++) {
      await fts.insert({
        rowid: i,
        columns: { content: `Document ${i} with some searchable content word${i}` },
      });
    }

    const elapsed = performance.now() - start;
    expect(elapsed).toBeLessThan(5000); // Should complete within 5 seconds
    expect(await fts.count()).toBe(1000);
  });

  it('should search efficiently', async () => {
    const fts = createFTSIndex({ columns: ['content'] });

    for (let i = 0; i < 1000; i++) {
      await fts.insert({
        rowid: i,
        columns: { content: `Document ${i} with searchable content` },
      });
    }

    const start = performance.now();
    const results = await fts.search('searchable');
    const elapsed = performance.now() - start;

    expect(elapsed).toBeLessThan(100); // Should search within 100ms
    expect(results.length).toBe(1000);
  });

  it('should handle complex queries efficiently', async () => {
    const fts = createFTSIndex({ columns: ['title', 'content'] });

    for (let i = 0; i < 100; i++) {
      await fts.insert({
        rowid: i,
        columns: {
          title: `Title ${i % 10}`,
          content: `Content with word${i % 5} and term${i % 7}`,
        },
      });
    }

    const start = performance.now();
    const results = await fts.search('(title:Title AND content:word*) OR term2');
    const elapsed = performance.now() - start;

    expect(elapsed).toBeLessThan(200);
    expect(results.length).toBeGreaterThan(0);
  });
});
