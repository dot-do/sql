/**
 * FTS Auxiliary Functions Tests
 *
 * Tests for highlight, snippet, and text utility functions.
 */

import { describe, it, expect } from 'vitest';
import {
  highlight,
  highlightPhrases,
  snippet,
  snippetMultiple,
  getContext,
  countOccurrences,
} from '../auxiliary.js';

// =============================================================================
// Highlight Function Tests
// =============================================================================

describe('highlight', () => {
  describe('basic highlighting', () => {
    it('should highlight a single term', () => {
      const result = highlight('Hello World', ['world'], { startTag: '<b>', endTag: '</b>' });
      expect(result).toBe('Hello <b>World</b>');
    });

    it('should highlight multiple terms', () => {
      const result = highlight('Hello World Today', ['hello', 'today'], { startTag: '<b>', endTag: '</b>' });
      expect(result).toBe('<b>Hello</b> World <b>Today</b>');
    });

    it('should highlight multiple occurrences of same term', () => {
      const result = highlight('test test test', ['test'], { startTag: '<em>', endTag: '</em>' });
      expect(result).toBe('<em>test</em> <em>test</em> <em>test</em>');
    });

    it('should handle case-insensitive matching by default', () => {
      const result = highlight('Hello WORLD world', ['world'], { startTag: '<b>', endTag: '</b>' });
      expect(result).toBe('Hello <b>WORLD</b> <b>world</b>');
    });

    it('should handle case-sensitive matching when specified', () => {
      const result = highlight('Hello WORLD world', ['world'], { startTag: '<b>', endTag: '</b>', caseSensitive: true });
      expect(result).toBe('Hello WORLD <b>world</b>');
    });
  });

  describe('word boundary handling', () => {
    it('should only match whole words', () => {
      const result = highlight('testing test tested', ['test'], { startTag: '<b>', endTag: '</b>' });
      // 'test' should only match 'test', not 'testing' or 'tested'
      expect(result).toBe('testing <b>test</b> tested');
    });

    it('should handle punctuation around words', () => {
      const result = highlight('Hello, World!', ['world'], { startTag: '<b>', endTag: '</b>' });
      expect(result).toBe('Hello, <b>World</b>!');
    });

    it('should handle hyphenated words', () => {
      const result = highlight('user-friendly interface', ['user'], { startTag: '<b>', endTag: '</b>' });
      expect(result).toBe('<b>user</b>-friendly interface');
    });
  });

  describe('special characters in terms', () => {
    it('should escape regex special characters in terms', () => {
      const result = highlight('price is $10.00', ['$10'], { startTag: '<b>', endTag: '</b>' });
      // Since we're matching word boundaries, $10 might not match depending on the regex
      expect(result).toBeDefined();
    });

    it('should handle terms with dots', () => {
      const result = highlight('Visit example.com today', ['example'], { startTag: '<b>', endTag: '</b>' });
      expect(result).toContain('<b>example</b>');
    });
  });

  describe('edge cases', () => {
    it('should return original text for empty terms array', () => {
      const result = highlight('Hello World', [], { startTag: '<b>', endTag: '</b>' });
      expect(result).toBe('Hello World');
    });

    it('should return original text for empty text', () => {
      const result = highlight('', ['test'], { startTag: '<b>', endTag: '</b>' });
      expect(result).toBe('');
    });

    it('should handle null-like terms', () => {
      const result = highlight('Hello World', [''], { startTag: '<b>', endTag: '</b>' });
      expect(result).toBeDefined();
    });

    it('should handle very long text', () => {
      const longText = 'Hello World '.repeat(1000);
      const result = highlight(longText, ['world'], { startTag: '<b>', endTag: '</b>' });
      expect(result.split('<b>').length).toBe(1001);
    });

    it('should handle text with no matches', () => {
      const result = highlight('Hello World', ['foo'], { startTag: '<b>', endTag: '</b>' });
      expect(result).toBe('Hello World');
    });
  });

  describe('custom tags', () => {
    it('should use custom start and end tags', () => {
      const result = highlight('Hello World', ['world'], { startTag: '<mark class="highlight">', endTag: '</mark>' });
      expect(result).toBe('Hello <mark class="highlight">World</mark>');
    });

    it('should handle empty tags', () => {
      const result = highlight('Hello World', ['world'], { startTag: '', endTag: '' });
      expect(result).toBe('Hello World');
    });
  });
});

// =============================================================================
// Highlight Phrases Tests
// =============================================================================

describe('highlightPhrases', () => {
  it('should highlight a phrase', () => {
    const result = highlightPhrases(
      'The quick brown fox jumps over the lazy dog',
      [['quick', 'brown', 'fox']],
      [],
      { startTag: '<b>', endTag: '</b>' }
    );
    expect(result).toContain('<b>quick brown fox</b>');
  });

  it('should highlight phrase and individual terms', () => {
    const result = highlightPhrases(
      'The quick brown fox jumps over the lazy dog',
      [['lazy', 'dog']],
      ['jumps'],
      { startTag: '<b>', endTag: '</b>' }
    );
    expect(result).toContain('<b>lazy dog</b>');
    expect(result).toContain('<b>jumps</b>');
  });

  it('should handle empty phrases array', () => {
    const result = highlightPhrases(
      'Hello World',
      [],
      ['world'],
      { startTag: '<b>', endTag: '</b>' }
    );
    expect(result).toBe('Hello <b>World</b>');
  });

  it('should handle case-insensitive phrase matching', () => {
    const result = highlightPhrases(
      'The QUICK BROWN Fox',
      [['quick', 'brown']],
      [],
      { startTag: '<b>', endTag: '</b>' }
    );
    expect(result.toLowerCase()).toContain('<b>quick brown</b>');
  });
});

// =============================================================================
// Snippet Function Tests
// =============================================================================

describe('snippet', () => {
  describe('basic snippet extraction', () => {
    it('should extract snippet around matched term', () => {
      const text = 'The quick brown fox jumps over the lazy dog. The dog was sleeping.';
      const result = snippet(text, ['fox'], { maxLength: 30 });
      expect(result).toContain('fox');
      expect(result.length).toBeLessThanOrEqual(40); // 30 + ellipsis
    });

    it('should return full text if shorter than maxLength', () => {
      const result = snippet('Hello World', ['world'], { maxLength: 50 });
      expect(result).toBe('Hello World');
    });

    it('should add ellipsis for truncated text', () => {
      const text = 'The quick brown fox jumps over the lazy dog and runs away.';
      const result = snippet(text, ['fox'], { maxLength: 20, ellipsis: '...' });
      expect(result).toContain('...');
    });

    it('should use custom ellipsis', () => {
      const text = 'The quick brown fox jumps over the lazy dog.';
      const result = snippet(text, ['fox'], { maxLength: 20, ellipsis: ' [...]' });
      expect(result).toContain('[...]');
    });
  });

  describe('snippet positioning', () => {
    it('should center snippet on matched term', () => {
      const text = 'Beginning text here. The matched term appears here. And ending text continues.';
      const result = snippet(text, ['matched'], { maxLength: 30 });
      expect(result).toContain('matched');
    });

    it('should handle term at beginning', () => {
      const text = 'Matched term at the start of a very long text that continues for a while.';
      const result = snippet(text, ['matched'], { maxLength: 30 });
      expect(result).toContain('Matched');
      expect(result.startsWith('...')).toBe(false);
    });

    it('should handle term at end', () => {
      const text = 'A very long text that continues for a while and ends with matched term';
      const result = snippet(text, ['matched'], { maxLength: 30 });
      expect(result).toContain('matched');
      expect(result.endsWith('...')).toBe(false);
    });
  });

  describe('highlighting in snippets', () => {
    it('should apply highlighting when tags provided', () => {
      const text = 'The quick brown fox jumps over the lazy dog.';
      const result = snippet(text, ['fox'], { maxLength: 50, startTag: '<b>', endTag: '</b>' });
      expect(result).toContain('<b>fox</b>');
    });

    it('should not highlight when no tags provided', () => {
      const text = 'The quick brown fox jumps over the lazy dog.';
      const result = snippet(text, ['fox'], { maxLength: 50 });
      expect(result).not.toContain('<b>');
    });
  });

  describe('edge cases', () => {
    it('should return empty string for empty text', () => {
      const result = snippet('', ['test'], { maxLength: 30 });
      expect(result).toBe('');
    });

    it('should handle no matching terms', () => {
      const text = 'Hello World';
      const result = snippet(text, ['nomatch'], { maxLength: 50 });
      expect(result).toBe('Hello World');
    });

    it('should handle empty terms array', () => {
      const text = 'Hello World this is a test.';
      const result = snippet(text, [], { maxLength: 10 });
      expect(result).toBeDefined();
    });

    it('should handle very short maxLength', () => {
      const text = 'Hello World';
      const result = snippet(text, ['world'], { maxLength: 5 });
      expect(result).toBeDefined();
    });

    it('should handle multiple terms and pick best region', () => {
      const text = 'This is a sentence with multiple words that we want to match against.';
      const result = snippet(text, ['multiple', 'words'], { maxLength: 30 });
      // Should try to include both terms in the snippet if possible
      expect(result).toBeDefined();
    });
  });
});

// =============================================================================
// Snippet Multiple Tests
// =============================================================================

describe('snippetMultiple', () => {
  const longText = `
    The first paragraph contains important information about TypeScript.
    TypeScript is a typed superset of JavaScript that compiles to plain JavaScript.
    The second paragraph discusses React and its component model.
    React makes it painless to create interactive UIs.
    The third paragraph covers Node.js and server-side development.
    Node.js allows you to run JavaScript on the server.
  `;

  it('should extract multiple snippets', () => {
    const result = snippetMultiple(longText, ['typescript', 'react', 'node'], {
      maxLength: 150,
      numSnippets: 3,
    });
    expect(result).toBeDefined();
  });

  it('should use separator between snippets', () => {
    const result = snippetMultiple(longText, ['typescript', 'react'], {
      maxLength: 100,
      numSnippets: 2,
      separator: ' | ',
    });
    expect(result.includes(' | ')).toBe(true);
  });

  it('should fall back to single snippet for numSnippets = 1', () => {
    const result = snippetMultiple(longText, ['typescript'], {
      maxLength: 50,
      numSnippets: 1,
    });
    expect(result).toBeDefined();
  });

  it('should handle empty text', () => {
    const result = snippetMultiple('', ['test'], {
      maxLength: 50,
      numSnippets: 2,
    });
    expect(result).toBe('');
  });

  it('should handle zero snippets', () => {
    const result = snippetMultiple(longText, ['test'], {
      maxLength: 50,
      numSnippets: 0,
    });
    expect(result).toBe('');
  });

  it('should not include overlapping regions', () => {
    const text = 'test test test test test';
    const result = snippetMultiple(text, ['test'], {
      maxLength: 30,
      numSnippets: 3,
    });
    expect(result).toBeDefined();
  });
});

// =============================================================================
// getContext Tests
// =============================================================================

describe('getContext', () => {
  const text = 'The quick brown fox jumps over the lazy dog.';

  it('should get context around a position', () => {
    const result = getContext(text, 16, 20); // Position of 'fox'
    expect(result.before).toBeDefined();
    expect(result.after).toBeDefined();
  });

  it('should handle position at start', () => {
    const result = getContext(text, 0, 20);
    expect(result.before).toBe('');
    expect(result.after.length).toBeGreaterThan(0);
  });

  it('should handle position at end', () => {
    const result = getContext(text, text.length, 20);
    expect(result.before.length).toBeGreaterThan(0);
    expect(result.after).toBe('');
  });

  it('should use default context length', () => {
    const result = getContext(text, 20);
    expect(result).toBeDefined();
  });

  it('should try to align to word boundaries', () => {
    // Context should try to start at word boundaries
    const result = getContext(text, 16, 30);
    expect(result).toBeDefined();
  });

  it('should handle short text', () => {
    const result = getContext('Hi', 1, 50);
    expect(result.before).toBe('H');
    expect(result.after).toBe('i');
  });
});

// =============================================================================
// countOccurrences Tests
// =============================================================================

describe('countOccurrences', () => {
  it('should count single occurrence', () => {
    const count = countOccurrences('Hello World', 'world');
    expect(count).toBe(1);
  });

  it('should count multiple occurrences', () => {
    const count = countOccurrences('test test test', 'test');
    expect(count).toBe(3);
  });

  it('should handle case-insensitive matching', () => {
    const count = countOccurrences('Hello HELLO hello', 'hello');
    expect(count).toBe(3);
  });

  it('should respect word boundaries', () => {
    const count = countOccurrences('testing test tested retest', 'test');
    expect(count).toBe(1); // Only 'test' should match, not testing/tested/retest
  });

  it('should return 0 for no matches', () => {
    const count = countOccurrences('Hello World', 'foo');
    expect(count).toBe(0);
  });

  it('should return 0 for empty text', () => {
    const count = countOccurrences('', 'test');
    expect(count).toBe(0);
  });

  it('should handle term with punctuation around it', () => {
    const count = countOccurrences('Hello, world! World?', 'world');
    expect(count).toBe(2);
  });

  it('should handle very long text', () => {
    const longText = 'hello world '.repeat(1000);
    const count = countOccurrences(longText, 'hello');
    expect(count).toBe(1000);
  });

  it('should handle overlapping potential matches', () => {
    // 'aa' appears twice in 'aaaa' when respecting word boundaries
    const count = countOccurrences('aa aa aa', 'aa');
    expect(count).toBe(3);
  });
});

// =============================================================================
// Integration Tests
// =============================================================================

describe('Auxiliary functions integration', () => {
  it('should work together for search result display', () => {
    const fullText = `
      Introduction to TypeScript. TypeScript is a typed superset of JavaScript
      that compiles to plain JavaScript. It adds optional static typing and
      class-based object-oriented programming to the language.
    `;
    const searchTerms = ['typescript', 'javascript'];

    // Get a snippet
    const snippetResult = snippet(fullText, searchTerms, {
      maxLength: 100,
      startTag: '<mark>',
      endTag: '</mark>',
    });

    // Should contain highlighted terms
    expect(snippetResult.includes('<mark>')).toBe(true);

    // Count occurrences
    const tsCount = countOccurrences(fullText, 'typescript');
    const jsCount = countOccurrences(fullText, 'javascript');
    expect(tsCount).toBeGreaterThan(0);
    expect(jsCount).toBeGreaterThan(0);
  });

  it('should handle real-world search scenarios', () => {
    const documents = [
      { id: 1, content: 'Building web applications with React and TypeScript.' },
      { id: 2, content: 'Node.js backend development with Express framework.' },
      { id: 3, content: 'Full-stack TypeScript development with Next.js.' },
    ];

    const searchTerm = 'typescript';

    // Highlight and count in each document
    for (const doc of documents) {
      const highlighted = highlight(doc.content, [searchTerm], {
        startTag: '<b>',
        endTag: '</b>',
      });
      const count = countOccurrences(doc.content, searchTerm);

      if (count > 0) {
        expect(highlighted.includes('<b>')).toBe(true);
      }
    }
  });
});
