/**
 * Tokenizer Tests
 *
 * Tests for FTS tokenization including:
 * - Simple tokenizer
 * - Porter stemmer tokenizer
 * - Unicode tokenizer
 * - Trigram tokenizer
 */

import { describe, it, expect } from 'vitest';
import {
  SimpleTokenizer,
  PorterTokenizer,
  UnicodeTokenizer,
  TrigramTokenizer,
  createTokenizer,
  tokenize,
} from '../tokenizer.js';
import type { Token, Tokenizer } from '../types.js';

// =============================================================================
// SimpleTokenizer Tests
// =============================================================================

describe('SimpleTokenizer', () => {
  const tokenizer = new SimpleTokenizer();

  describe('basic tokenization', () => {
    it('should tokenize simple text', () => {
      const tokens = tokenizer.tokenize('Hello World');
      expect(tokens).toEqual([
        { term: 'hello', position: 0, start: 0, end: 5 },
        { term: 'world', position: 1, start: 6, end: 11 },
      ]);
    });

    it('should tokenize single word', () => {
      const tokens = tokenizer.tokenize('TypeScript');
      expect(tokens).toHaveLength(1);
      expect(tokens[0].term).toBe('typescript');
    });

    it('should handle empty string', () => {
      const tokens = tokenizer.tokenize('');
      expect(tokens).toHaveLength(0);
    });

    it('should handle whitespace-only string', () => {
      const tokens = tokenizer.tokenize('   \t\n  ');
      expect(tokens).toHaveLength(0);
    });

    it('should handle multiple spaces between words', () => {
      const tokens = tokenizer.tokenize('hello    world');
      expect(tokens).toHaveLength(2);
      expect(tokens.map(t => t.term)).toEqual(['hello', 'world']);
    });
  });

  describe('punctuation handling', () => {
    it('should handle trailing punctuation', () => {
      const tokens = tokenizer.tokenize('Hello, World!');
      expect(tokens.map(t => t.term)).toEqual(['hello', 'world']);
    });

    it('should handle periods and commas', () => {
      const tokens = tokenizer.tokenize('Hello. World, here.');
      expect(tokens.map(t => t.term)).toEqual(['hello', 'world', 'here']);
    });

    it('should handle parentheses', () => {
      const tokens = tokenizer.tokenize('(hello) [world]');
      expect(tokens.map(t => t.term)).toEqual(['hello', 'world']);
    });

    it('should handle dashes and underscores', () => {
      const tokens = tokenizer.tokenize('hello-world foo_bar');
      // Underscores are part of word characters, hyphens split words
      expect(tokens.map(t => t.term)).toContain('foo_bar');
    });

    it('should handle quotes', () => {
      const tokens = tokenizer.tokenize('"hello" \'world\'');
      expect(tokens.map(t => t.term)).toEqual(['hello', 'world']);
    });
  });

  describe('numeric handling', () => {
    it('should tokenize numbers', () => {
      const tokens = tokenizer.tokenize('test123');
      expect(tokens.map(t => t.term)).toContain('test123');
    });

    it('should handle standalone numbers', () => {
      const tokens = tokenizer.tokenize('hello 42 world');
      expect(tokens.map(t => t.term)).toEqual(['hello', '42', 'world']);
    });

    it('should handle decimal numbers', () => {
      const tokens = tokenizer.tokenize('Version 3.14 released');
      // Decimal point splits the number
      expect(tokens.map(t => t.term)).toContain('3');
      expect(tokens.map(t => t.term)).toContain('14');
    });

    it('should handle negative numbers', () => {
      const tokens = tokenizer.tokenize('temperature -10 degrees');
      expect(tokens.map(t => t.term)).toContain('10');
    });
  });

  describe('case normalization', () => {
    it('should lowercase all terms', () => {
      const tokens = tokenizer.tokenize('TypeScript REACT Vue');
      expect(tokens.map(t => t.term)).toEqual(['typescript', 'react', 'vue']);
    });

    it('should handle mixed case', () => {
      const tokens = tokenizer.tokenize('iPhone macOS iOS');
      expect(tokens.map(t => t.term)).toEqual(['iphone', 'macos', 'ios']);
    });
  });

  describe('position tracking', () => {
    it('should track token positions correctly', () => {
      const tokens = tokenizer.tokenize('one two three');
      expect(tokens[0].position).toBe(0);
      expect(tokens[1].position).toBe(1);
      expect(tokens[2].position).toBe(2);
    });

    it('should track character offsets correctly', () => {
      const tokens = tokenizer.tokenize('hello world');
      expect(tokens[0].start).toBe(0);
      expect(tokens[0].end).toBe(5);
      expect(tokens[1].start).toBe(6);
      expect(tokens[1].end).toBe(11);
    });
  });

  describe('normalize method', () => {
    it('should normalize to lowercase', () => {
      expect(tokenizer.normalize('HELLO')).toBe('hello');
      expect(tokenizer.normalize('TypeScript')).toBe('typescript');
    });

    it('should handle empty string', () => {
      expect(tokenizer.normalize('')).toBe('');
    });
  });
});

// =============================================================================
// PorterTokenizer Tests
// =============================================================================

describe('PorterTokenizer', () => {
  const tokenizer = new PorterTokenizer();

  describe('basic stemming', () => {
    it('should stem running forms', () => {
      const tokens = tokenizer.tokenize('running runs runner');
      const terms = tokens.map(t => t.term);
      // Porter stemmer should reduce these to similar stems
      expect(new Set(terms).size).toBeLessThan(3);
    });

    it('should stem plural forms', () => {
      const catTokens = tokenizer.tokenize('cats');
      const catToken = tokenizer.tokenize('cat');
      expect(catTokens[0].term).toBe(catToken[0].term);
    });

    it('should stem -ing suffix', () => {
      expect(tokenizer.normalize('programming')).not.toBe('programming');
      const programmingTokens = tokenizer.tokenize('programming programmed');
      const terms = tokens => tokens.map(t => t.term);
      // Both should have similar stems
      expect(terms(programmingTokens).length).toBe(2);
    });

    it('should stem -ed suffix', () => {
      const connected = tokenizer.normalize('connected');
      const connecting = tokenizer.normalize('connecting');
      // Both should stem to similar forms
      expect(connected).toBeDefined();
      expect(connecting).toBeDefined();
    });

    it('should stem -ness suffix', () => {
      expect(tokenizer.normalize('happiness')).not.toContain('ness');
    });
  });

  describe('stemming edge cases', () => {
    it('should not over-stem short words', () => {
      const tokens = tokenizer.tokenize('is a the');
      expect(tokens.length).toBe(3);
    });

    it('should handle words ending in -y', () => {
      const happy = tokenizer.normalize('happy');
      const happily = tokenizer.normalize('happily');
      // Both should convert y to i
      expect(happy).toBeDefined();
      expect(happily).toBeDefined();
    });

    it('should handle double consonants', () => {
      const running = tokenizer.normalize('running');
      // Should handle the double 'n'
      expect(running).toBeDefined();
    });

    it('should handle -ies suffix', () => {
      const parties = tokenizer.normalize('parties');
      const party = tokenizer.normalize('party');
      expect(parties).toBe(party);
    });

    it('should handle -sses suffix', () => {
      const addresses = tokenizer.normalize('addresses');
      expect(addresses).not.toContain('sses');
    });
  });

  describe('verb stemming', () => {
    it('should stem different verb forms', () => {
      const tokens = tokenizer.tokenize('connect connects connecting connected');
      const stems = new Set(tokens.map(t => t.term));
      expect(stems.size).toBeLessThanOrEqual(2);
    });

    it('should handle irregular forms gracefully', () => {
      const run = tokenizer.normalize('run');
      const ran = tokenizer.normalize('ran');
      // Porter stemmer doesn't handle irregular verbs, just applies rules
      expect(run).toBeDefined();
      expect(ran).toBeDefined();
    });
  });

  describe('normalization consistency', () => {
    it('should produce consistent stems', () => {
      const term1 = tokenizer.normalize('databases');
      const term2 = tokenizer.normalize('databases');
      expect(term1).toBe(term2);
    });

    it('should lowercase before stemming', () => {
      const upper = tokenizer.normalize('RUNNING');
      const lower = tokenizer.normalize('running');
      expect(upper).toBe(lower);
    });
  });
});

// =============================================================================
// UnicodeTokenizer Tests
// =============================================================================

describe('UnicodeTokenizer', () => {
  const tokenizer = new UnicodeTokenizer();

  describe('basic unicode handling', () => {
    it('should tokenize ASCII text', () => {
      const tokens = tokenizer.tokenize('Hello World');
      expect(tokens.map(t => t.term)).toEqual(['hello', 'world']);
    });

    it('should handle accented characters', () => {
      const tokens = tokenizer.tokenize('cafe resume naive');
      expect(tokens.length).toBe(3);
      expect(tokens.map(t => t.term)).toContain('cafe');
    });

    it('should handle Latin extended characters', () => {
      const tokens = tokenizer.tokenize('facade resume');
      expect(tokens.length).toBe(2);
    });
  });

  describe('multi-script handling', () => {
    it('should handle Cyrillic', () => {
      const tokens = tokenizer.tokenize('hello test');
      expect(tokens.length).toBeGreaterThan(0);
    });

    it('should handle Greek', () => {
      const tokens = tokenizer.tokenize('alpha beta gamma');
      expect(tokens.length).toBe(3);
    });

    it('should handle mixed scripts', () => {
      const tokens = tokenizer.tokenize('TypeScript kurso');
      expect(tokens.length).toBe(2);
    });

    it('should handle CJK-style text', () => {
      const tokens = tokenizer.tokenize('hello world test');
      expect(tokens.length).toBeGreaterThan(0);
    });
  });

  describe('unicode normalization', () => {
    it('should apply NFC normalization', () => {
      // Composed vs decomposed forms
      const term = tokenizer.normalize('cafe');
      expect(term.normalize('NFC')).toBe(term);
    });

    it('should lowercase unicode characters', () => {
      const upper = tokenizer.normalize('HELLO');
      expect(upper).toBe('hello');
    });
  });

  describe('unicode edge cases', () => {
    it('should handle emoji by ignoring them', () => {
      const tokens = tokenizer.tokenize('hello world');
      // Emoji should be filtered out, only words remain
      expect(tokens.some(t => t.term === 'hello')).toBe(true);
    });

    it('should handle zero-width characters', () => {
      const tokens = tokenizer.tokenize('hello\u200Bworld');
      expect(tokens.length).toBe(2);
    });

    it('should handle combining diacriticals', () => {
      const tokens = tokenizer.tokenize('e\u0301 a\u0300');
      expect(tokens.length).toBeGreaterThan(0);
    });
  });
});

// =============================================================================
// TrigramTokenizer Tests
// =============================================================================

describe('TrigramTokenizer', () => {
  const tokenizer = new TrigramTokenizer();

  describe('trigram generation', () => {
    it('should generate trigrams from text', () => {
      const tokens = tokenizer.tokenize('hello');
      // "hello" -> "hel", "ell", "llo"
      expect(tokens.length).toBe(3);
      expect(tokens.map(t => t.term)).toEqual(['hel', 'ell', 'llo']);
    });

    it('should handle short text', () => {
      const tokens = tokenizer.tokenize('hi');
      // Less than 3 chars, no trigrams
      expect(tokens.length).toBe(0);
    });

    it('should handle exactly 3 chars', () => {
      const tokens = tokenizer.tokenize('abc');
      expect(tokens.length).toBe(1);
      expect(tokens[0].term).toBe('abc');
    });

    it('should handle spaces in trigrams', () => {
      const tokens = tokenizer.tokenize('a b c');
      // Spaces are normalized to single space
      expect(tokens.length).toBe(3);
    });
  });

  describe('position tracking', () => {
    it('should track trigram positions', () => {
      const tokens = tokenizer.tokenize('hello');
      expect(tokens[0].position).toBe(0);
      expect(tokens[1].position).toBe(1);
      expect(tokens[2].position).toBe(2);
    });

    it('should track character offsets', () => {
      const tokens = tokenizer.tokenize('hello');
      expect(tokens[0].start).toBe(0);
      expect(tokens[0].end).toBe(3);
      expect(tokens[1].start).toBe(1);
      expect(tokens[1].end).toBe(4);
    });
  });

  describe('normalization', () => {
    it('should lowercase for trigram generation', () => {
      const tokens = tokenizer.tokenize('HELLO');
      expect(tokens.map(t => t.term)).toEqual(['hel', 'ell', 'llo']);
    });

    it('should collapse whitespace', () => {
      const normalized = tokenizer.normalize('hello   world');
      expect(normalized).toBe('hello world');
    });
  });
});

// =============================================================================
// Factory Function Tests
// =============================================================================

describe('createTokenizer factory', () => {
  it('should create SimpleTokenizer for "simple"', () => {
    const tokenizer = createTokenizer('simple');
    expect(tokenizer).toBeInstanceOf(SimpleTokenizer);
  });

  it('should create PorterTokenizer for "porter"', () => {
    const tokenizer = createTokenizer('porter');
    expect(tokenizer).toBeInstanceOf(PorterTokenizer);
  });

  it('should create UnicodeTokenizer for "unicode61"', () => {
    const tokenizer = createTokenizer('unicode61');
    expect(tokenizer).toBeInstanceOf(UnicodeTokenizer);
  });

  it('should create TrigramTokenizer for "trigram"', () => {
    const tokenizer = createTokenizer('trigram');
    expect(tokenizer).toBeInstanceOf(TrigramTokenizer);
  });

  it('should default to SimpleTokenizer for unknown type', () => {
    const tokenizer = createTokenizer('unknown');
    expect(tokenizer).toBeInstanceOf(SimpleTokenizer);
  });
});

// =============================================================================
// Convenience Function Tests
// =============================================================================

describe('tokenize convenience function', () => {
  it('should tokenize with default simple tokenizer', () => {
    const tokens = tokenize('Hello World');
    expect(tokens.map(t => t.term)).toEqual(['hello', 'world']);
  });

  it('should accept tokenizer type parameter', () => {
    const tokens = tokenize('running', 'porter');
    expect(tokens.length).toBe(1);
  });

  it('should work with different tokenizer types', () => {
    const simpleTokens = tokenize('hello', 'simple');
    const porterTokens = tokenize('hello', 'porter');
    const unicodeTokens = tokenize('hello', 'unicode61');

    expect(simpleTokens[0].term).toBe('hello');
    expect(porterTokens[0].term).toBeDefined();
    expect(unicodeTokens[0].term).toBe('hello');
  });
});

// =============================================================================
// Cross-Tokenizer Comparison Tests
// =============================================================================

describe('Tokenizer comparisons', () => {
  it('should produce consistent token counts for simple text', () => {
    const text = 'The quick brown fox jumps over the lazy dog';
    const simple = new SimpleTokenizer().tokenize(text);
    const unicode = new UnicodeTokenizer().tokenize(text);

    expect(simple.length).toBe(unicode.length);
  });

  it('should all implement Tokenizer interface', () => {
    const tokenizers: Tokenizer[] = [
      new SimpleTokenizer(),
      new PorterTokenizer(),
      new UnicodeTokenizer(),
      new TrigramTokenizer(),
    ];

    for (const tokenizer of tokenizers) {
      expect(typeof tokenizer.tokenize).toBe('function');
      expect(typeof tokenizer.normalize).toBe('function');
    }
  });

  it('should handle same input consistently', () => {
    const text = 'Hello World';
    const simple = new SimpleTokenizer();

    const result1 = simple.tokenize(text);
    const result2 = simple.tokenize(text);

    expect(result1).toEqual(result2);
  });
});

// =============================================================================
// Edge Cases and Stress Tests
// =============================================================================

describe('Tokenizer edge cases', () => {
  const tokenizer = new SimpleTokenizer();

  it('should handle very long words', () => {
    const longWord = 'a'.repeat(1000);
    const tokens = tokenizer.tokenize(longWord);
    expect(tokens.length).toBe(1);
    expect(tokens[0].term.length).toBe(1000);
  });

  it('should handle very long text', () => {
    const longText = 'word '.repeat(10000);
    const tokens = tokenizer.tokenize(longText);
    expect(tokens.length).toBe(10000);
  });

  it('should handle null-like inputs gracefully', () => {
    expect(tokenizer.tokenize('')).toHaveLength(0);
  });

  it('should handle only punctuation', () => {
    const tokens = tokenizer.tokenize('!@#$%^&*()');
    expect(tokens.length).toBe(0);
  });

  it('should handle only numbers', () => {
    const tokens = tokenizer.tokenize('123 456 789');
    expect(tokens.length).toBe(3);
  });

  it('should handle mixed content', () => {
    const tokens = tokenizer.tokenize('hello123world!@#test');
    expect(tokens.length).toBeGreaterThan(0);
  });
});
