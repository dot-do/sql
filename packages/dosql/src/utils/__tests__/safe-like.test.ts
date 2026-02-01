/**
 * Safe LIKE Pattern Matching Tests
 *
 * Tests for the ReDoS-safe implementation of SQL LIKE pattern matching.
 * Addresses security issue sql-axvi.
 */

import { describe, it, expect } from 'vitest';
import {
  safeLikeMatch,
  safeLikeMatchWithEscape,
  safeGlobMatch,
} from '../safe-like.js';

// =============================================================================
// safeLikeMatch - Basic functionality
// =============================================================================

describe('safeLikeMatch', () => {
  describe('basic matching', () => {
    it('should match exact strings', () => {
      expect(safeLikeMatch('hello', 'hello')).toBe(true);
      expect(safeLikeMatch('hello', 'world')).toBe(false);
    });

    it('should handle % wildcard (zero or more characters)', () => {
      expect(safeLikeMatch('hello world', '%world')).toBe(true);
      expect(safeLikeMatch('hello world', 'hello%')).toBe(true);
      expect(safeLikeMatch('hello world', '%lo wo%')).toBe(true);
      expect(safeLikeMatch('hello', '%')).toBe(true);
      expect(safeLikeMatch('', '%')).toBe(true);
      expect(safeLikeMatch('hello', '%%')).toBe(true);
      expect(safeLikeMatch('hello', '%hello%')).toBe(true);
    });

    it('should handle _ wildcard (exactly one character)', () => {
      expect(safeLikeMatch('abc', 'a_c')).toBe(true);
      expect(safeLikeMatch('aXc', 'a_c')).toBe(true);
      expect(safeLikeMatch('abbc', 'a_c')).toBe(false);
      expect(safeLikeMatch('ac', 'a_c')).toBe(false);
      expect(safeLikeMatch('abc', '___')).toBe(true);
      expect(safeLikeMatch('ab', '___')).toBe(false);
    });

    it('should handle combined wildcards', () => {
      expect(safeLikeMatch('hello world', 'h%_d')).toBe(true);
      expect(safeLikeMatch('hd', 'h%_d')).toBe(false);
      expect(safeLikeMatch('abcdef', 'a_c%f')).toBe(true);
      expect(safeLikeMatch('abcf', 'a_c%f')).toBe(true);
      expect(safeLikeMatch('acf', 'a_c%f')).toBe(false);
    });
  });

  describe('case sensitivity', () => {
    it('should be case insensitive by default', () => {
      expect(safeLikeMatch('Hello', 'hello')).toBe(true);
      expect(safeLikeMatch('hello', 'HELLO')).toBe(true);
      expect(safeLikeMatch('HELLO', '%ello')).toBe(true);
    });

    it('should support case sensitive matching', () => {
      expect(safeLikeMatch('Hello', 'hello', false)).toBe(false);
      expect(safeLikeMatch('hello', 'hello', false)).toBe(true);
      expect(safeLikeMatch('HELLO', 'HELLO', false)).toBe(true);
    });
  });

  describe('regex special characters (should be treated as literals)', () => {
    it('should treat dots as literal characters', () => {
      expect(safeLikeMatch('test.value', 'test.%')).toBe(true);
      expect(safeLikeMatch('testXvalue', 'test.%')).toBe(false);
      expect(safeLikeMatch('a.b.c', '%.%.%')).toBe(true);
    });

    it('should treat other regex chars as literals', () => {
      expect(safeLikeMatch('test[1]', 'test[%')).toBe(true);
      expect(safeLikeMatch('test(1)', 'test(%')).toBe(true);
      expect(safeLikeMatch('test*', 'test*')).toBe(true);
      expect(safeLikeMatch('test+', 'test+')).toBe(true);
      expect(safeLikeMatch('test?', 'test?')).toBe(true);
      expect(safeLikeMatch('$100', '$%')).toBe(true);
      expect(safeLikeMatch('test^value', 'test^%')).toBe(true);
      expect(safeLikeMatch('test|value', 'test|%')).toBe(true);
    });
  });

  describe('edge cases', () => {
    it('should handle empty strings', () => {
      expect(safeLikeMatch('', '')).toBe(true);
      expect(safeLikeMatch('', '%')).toBe(true);
      expect(safeLikeMatch('', '_')).toBe(false);
      expect(safeLikeMatch('a', '')).toBe(false);
    });

    it('should handle patterns with consecutive wildcards', () => {
      expect(safeLikeMatch('hello', '%%%')).toBe(true);
      expect(safeLikeMatch('hello', '%%hello%%')).toBe(true);
      expect(safeLikeMatch('abc', 'a%%c')).toBe(true);
    });
  });
});

// =============================================================================
// safeLikeMatch - ReDoS protection (sql-axvi)
// =============================================================================

describe('safeLikeMatch - ReDoS protection', () => {
  it('should handle patterns that would cause catastrophic backtracking with regex', () => {
    // This pattern with regex would cause O(2^n) time complexity
    // Pattern: %a%a%a%a%a%a%a%a%a%a%b (ends with b, which is not in input)
    // String: 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' (no b)
    const maliciousPattern = '%a%a%a%a%a%a%a%a%a%a%b';
    const maliciousInput = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

    const start = performance.now();
    const result = safeLikeMatch(maliciousInput, maliciousPattern);
    const elapsed = performance.now() - start;

    // Should complete in under 100ms (regex would take exponential time)
    expect(elapsed).toBeLessThan(100);
    expect(result).toBe(false); // 'b' at the end doesn't exist in input
  });

  it('should handle deeply nested patterns efficiently', () => {
    // Create a pattern with many % wildcards
    const pattern = '%x'.repeat(20) + '%';
    const input = 'x'.repeat(50);

    const start = performance.now();
    const result = safeLikeMatch(input, pattern);
    const elapsed = performance.now() - start;

    expect(elapsed).toBeLessThan(100);
    expect(result).toBe(true);
  });

  it('should handle pathological non-matching cases efficiently', () => {
    // Pattern that requires checking many possibilities before failing
    const pattern = '%a%b%c%d%e%f%g%h%i%j%';
    const input = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx';

    const start = performance.now();
    const result = safeLikeMatch(input, pattern);
    const elapsed = performance.now() - start;

    expect(elapsed).toBeLessThan(100);
    expect(result).toBe(false);
  });

  it('should handle long strings with complex patterns', () => {
    const pattern = '%test%value%data%';
    const input = 'a'.repeat(1000) + 'test' + 'b'.repeat(1000) + 'value' + 'c'.repeat(1000) + 'data' + 'd'.repeat(1000);

    const start = performance.now();
    const result = safeLikeMatch(input, pattern);
    const elapsed = performance.now() - start;

    expect(elapsed).toBeLessThan(200);
    expect(result).toBe(true);
  });

  it('should reject patterns exceeding maximum length', () => {
    const longPattern = 'a'.repeat(1001);
    expect(() => safeLikeMatch('test', longPattern)).toThrow(/exceeds maximum length/);
  });

  it('should reject strings exceeding maximum length', () => {
    const longString = 'a'.repeat(100001);
    expect(() => safeLikeMatch(longString, '%')).toThrow(/exceeds maximum length/);
  });
});

// =============================================================================
// safeLikeMatchWithEscape
// =============================================================================

describe('safeLikeMatchWithEscape', () => {
  it('should match literal % when escaped', () => {
    expect(safeLikeMatchWithEscape('50%', '%\\%', '\\')).toBe(true);
    expect(safeLikeMatchWithEscape('50x', '%\\%', '\\')).toBe(false);
    expect(safeLikeMatchWithEscape('100% complete', '%\\%%', '\\')).toBe(true);
  });

  it('should match literal _ when escaped', () => {
    expect(safeLikeMatchWithEscape('a_b', 'a\\_b', '\\')).toBe(true);
    expect(safeLikeMatchWithEscape('axb', 'a\\_b', '\\')).toBe(false);
  });

  it('should work with different escape characters', () => {
    expect(safeLikeMatchWithEscape('50%', '%!%', '!')).toBe(true);
    expect(safeLikeMatchWithEscape('a_b', 'a#_b', '#')).toBe(true);
  });

  it('should fall back to safeLikeMatch when no escape character', () => {
    expect(safeLikeMatchWithEscape('hello', '%ello', null)).toBe(true);
    expect(safeLikeMatchWithEscape('hello', 'h_llo', null)).toBe(true);
  });

  it('should handle ReDoS patterns safely', () => {
    // Pattern ends with 'b' which is not in input
    const maliciousPattern = '%a%a%a%a%a%a%a%a%a%a%b';
    const maliciousInput = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

    const start = performance.now();
    const result = safeLikeMatchWithEscape(maliciousInput, maliciousPattern, '\\');
    const elapsed = performance.now() - start;

    expect(elapsed).toBeLessThan(100);
    expect(result).toBe(false);
  });
});

// =============================================================================
// safeGlobMatch
// =============================================================================

describe('safeGlobMatch', () => {
  describe('basic matching', () => {
    it('should match exact strings', () => {
      expect(safeGlobMatch('hello', 'hello')).toBe(true);
      expect(safeGlobMatch('hello', 'world')).toBe(false);
    });

    it('should handle * wildcard (zero or more characters)', () => {
      expect(safeGlobMatch('hello.txt', '*.txt')).toBe(true);
      expect(safeGlobMatch('hello.txt', 'hello*')).toBe(true);
      expect(safeGlobMatch('hello', '*')).toBe(true);
      expect(safeGlobMatch('', '*')).toBe(true);
    });

    it('should handle ? wildcard (exactly one character)', () => {
      expect(safeGlobMatch('abc', 'a?c')).toBe(true);
      expect(safeGlobMatch('abbc', 'a?c')).toBe(false);
      expect(safeGlobMatch('test123', 'test???')).toBe(true);
    });

    it('should handle character classes', () => {
      expect(safeGlobMatch('abc', '[abc][abc][abc]')).toBe(true);
      expect(safeGlobMatch('xyz', '[abc][abc][abc]')).toBe(false);
      expect(safeGlobMatch('test1', 'test[0-9]')).toBe(true);
      expect(safeGlobMatch('testa', 'test[0-9]')).toBe(false);
    });

    it('should handle negated character classes', () => {
      expect(safeGlobMatch('testx', 'test[!0-9]')).toBe(true);
      expect(safeGlobMatch('test5', 'test[!0-9]')).toBe(false);
      expect(safeGlobMatch('testx', 'test[^0-9]')).toBe(true);
    });
  });

  describe('case sensitivity', () => {
    it('should be case sensitive by default', () => {
      expect(safeGlobMatch('Hello', 'hello')).toBe(false);
      expect(safeGlobMatch('hello', 'hello')).toBe(true);
    });

    it('should support case insensitive matching', () => {
      expect(safeGlobMatch('Hello', 'hello', true)).toBe(true);
      expect(safeGlobMatch('HELLO', '*ello', true)).toBe(true);
    });
  });

  describe('ReDoS protection', () => {
    it('should handle patterns that would cause catastrophic backtracking', () => {
      // Pattern ends with 'b' which is not in input
      const maliciousPattern = '*a*a*a*a*a*a*a*a*a*a*b';
      const maliciousInput = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa';

      const start = performance.now();
      const result = safeGlobMatch(maliciousInput, maliciousPattern);
      const elapsed = performance.now() - start;

      expect(elapsed).toBeLessThan(100);
      expect(result).toBe(false);
    });

    it('should reject patterns exceeding maximum length', () => {
      const longPattern = 'a'.repeat(1001);
      expect(() => safeGlobMatch('test', longPattern)).toThrow(/exceeds maximum length/);
    });
  });

  describe('unclosed bracket handling', () => {
    it('should treat unclosed bracket as literal', () => {
      expect(safeGlobMatch('test[', 'test[')).toBe(true);
      expect(safeGlobMatch('test[x', 'test[*')).toBe(true);
    });
  });
});
