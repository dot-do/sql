/**
 * Tests for consolidated SQL LIKE pattern matching utilities
 *
 * This tests the consolidated LIKE implementation (sql-8hdu) which provides:
 * - Secure regex escaping
 * - ReDoS-safe pattern matching via dynamic programming
 * - Support for escape characters
 * - SQL NULL semantics
 */

import { describe, it, expect } from 'vitest';
import {
  escapeRegex,
  likeToRegex,
  likeToRegexPattern,
  likeMatch,
  sqlLikeMatch,
  likeMatchWithEscape,
  sqlLikeMatchWithEscape,
} from '../like.js';

describe('LIKE Pattern Matching Utilities', () => {
  // =========================================================================
  // escapeRegex
  // =========================================================================

  describe('escapeRegex', () => {
    it('escapes regex metacharacters', () => {
      expect(escapeRegex('hello.world')).toBe('hello\\.world');
      expect(escapeRegex('test[1]')).toBe('test\\[1\\]');
      expect(escapeRegex('a*b+c?')).toBe('a\\*b\\+c\\?');
      expect(escapeRegex('^start$end')).toBe('\\^start\\$end');
      expect(escapeRegex('a{2,3}')).toBe('a\\{2,3\\}');
      expect(escapeRegex('a|b')).toBe('a\\|b');
      expect(escapeRegex('(group)')).toBe('\\(group\\)');
      expect(escapeRegex('back\\slash')).toBe('back\\\\slash');
    });

    it('leaves safe characters unchanged', () => {
      expect(escapeRegex('hello world')).toBe('hello world');
      expect(escapeRegex('abc123')).toBe('abc123');
      expect(escapeRegex('user@example.com')).toBe('user@example\\.com');
    });
  });

  // =========================================================================
  // likeToRegex
  // =========================================================================

  describe('likeToRegex', () => {
    it('converts % to .*', () => {
      const regex = likeToRegex('hello%');
      expect(regex.test('hello')).toBe(true);
      expect(regex.test('hello world')).toBe(true);
      expect(regex.test('hi')).toBe(false);
    });

    it('converts _ to .', () => {
      const regex = likeToRegex('h_llo');
      expect(regex.test('hello')).toBe(true);
      expect(regex.test('hallo')).toBe(true);
      expect(regex.test('hillo')).toBe(true);
      expect(regex.test('hllo')).toBe(false);
      expect(regex.test('heello')).toBe(false);
    });

    it('escapes regex special characters', () => {
      const regex = likeToRegex('test.txt');
      expect(regex.test('test.txt')).toBe(true);
      expect(regex.test('testXtxt')).toBe(false);
    });

    it('is case-insensitive by default', () => {
      const regex = likeToRegex('hello');
      expect(regex.test('hello')).toBe(true);
      expect(regex.test('HELLO')).toBe(true);
      expect(regex.test('Hello')).toBe(true);
    });

    it('respects flags parameter', () => {
      const regex = likeToRegex('hello', '');
      expect(regex.test('hello')).toBe(true);
      expect(regex.test('HELLO')).toBe(false);
    });
  });

  // =========================================================================
  // likeToRegexPattern
  // =========================================================================

  describe('likeToRegexPattern', () => {
    it('converts LIKE pattern to regex pattern string', () => {
      expect(likeToRegexPattern('hello%')).toBe('hello.*');
      expect(likeToRegexPattern('test_')).toBe('test.');
      expect(likeToRegexPattern('%world')).toBe('.*world');
      expect(likeToRegexPattern('a%b%c')).toBe('a.*b.*c');
    });

    it('escapes special characters', () => {
      expect(likeToRegexPattern('test.txt')).toBe('test\\.txt');
      expect(likeToRegexPattern('file[1]')).toBe('file\\[1\\]');
    });
  });

  // =========================================================================
  // likeMatch (ReDoS-safe via DP)
  // =========================================================================

  describe('likeMatch', () => {
    describe('basic wildcards', () => {
      it('matches % wildcard (any sequence)', () => {
        expect(likeMatch('hello world', '%world')).toBe(true);
        expect(likeMatch('hello world', 'hello%')).toBe(true);
        expect(likeMatch('hello world', '%o w%')).toBe(true);
        expect(likeMatch('hello world', '%')).toBe(true);
        expect(likeMatch('', '%')).toBe(true);
      });

      it('matches _ wildcard (single char)', () => {
        expect(likeMatch('abc', 'a_c')).toBe(true);
        expect(likeMatch('aXc', 'a_c')).toBe(true);
        expect(likeMatch('ac', 'a_c')).toBe(false);
        expect(likeMatch('abbc', 'a_c')).toBe(false);
        expect(likeMatch('abc', '___')).toBe(true);
      });

      it('matches combined wildcards', () => {
        expect(likeMatch('hello world', 'h%o_world')).toBe(true);
        expect(likeMatch('test123', 't%_3')).toBe(true);
        expect(likeMatch('abcdef', 'a%_f')).toBe(true);
      });
    });

    describe('literal matching', () => {
      it('matches exact strings', () => {
        expect(likeMatch('hello', 'hello')).toBe(true);
        expect(likeMatch('hello', 'world')).toBe(false);
        expect(likeMatch('', '')).toBe(true);
      });

      it('is case-insensitive by default', () => {
        expect(likeMatch('hello', 'HELLO')).toBe(true);
        expect(likeMatch('HELLO', 'hello')).toBe(true);
        expect(likeMatch('HeLLo', 'hEllO')).toBe(true);
      });

      it('is case-sensitive when specified', () => {
        expect(likeMatch('hello', 'HELLO', true)).toBe(false);
        expect(likeMatch('hello', 'hello', true)).toBe(true);
      });
    });

    describe('security: regex special characters as literals', () => {
      it('treats . as literal', () => {
        expect(likeMatch('a.b', 'a.b')).toBe(true);
        expect(likeMatch('aXb', 'a.b')).toBe(false);
      });

      it('treats * as literal', () => {
        expect(likeMatch('a*b', 'a*b')).toBe(true);
        expect(likeMatch('ab', 'a*b')).toBe(false);
        expect(likeMatch('aaaaab', 'a*b')).toBe(false);
      });

      it('treats + as literal', () => {
        expect(likeMatch('a+b', 'a+b')).toBe(true);
        expect(likeMatch('ab', 'a+b')).toBe(false);
      });

      it('treats ? as literal', () => {
        expect(likeMatch('a?b', 'a?b')).toBe(true);
        expect(likeMatch('ab', 'a?b')).toBe(false);
      });

      it('treats ^ and $ as literals', () => {
        expect(likeMatch('^start', '^start')).toBe(true);
        expect(likeMatch('end$', 'end$')).toBe(true);
        expect(likeMatch('start', '^start')).toBe(false);
      });

      it('treats {} as literals', () => {
        expect(likeMatch('a{2}', 'a{2}')).toBe(true);
        expect(likeMatch('aa', 'a{2}')).toBe(false);
      });

      it('treats () as literals', () => {
        expect(likeMatch('(test)', '(test)')).toBe(true);
      });

      it('treats [] as literals', () => {
        expect(likeMatch('[abc]', '[abc]')).toBe(true);
        expect(likeMatch('a', '[abc]')).toBe(false);
      });

      it('treats | as literal', () => {
        expect(likeMatch('a|b', 'a|b')).toBe(true);
        expect(likeMatch('a', 'a|b')).toBe(false);
      });

      it('treats backslash as literal', () => {
        expect(likeMatch('a\\b', 'a\\b')).toBe(true);
      });
    });

    describe('ReDoS safety', () => {
      it('handles pathological patterns efficiently', () => {
        // This pattern would cause catastrophic backtracking with naive regex
        const value = 'a'.repeat(30) + 'b';
        const pattern = '%a%a%a%a%a%a%';

        const start = performance.now();
        const result = likeMatch(value, pattern);
        const elapsed = performance.now() - start;

        expect(result).toBe(true);
        // Should complete in reasonable time (< 100ms)
        expect(elapsed).toBeLessThan(100);
      });

      it('handles many wildcards efficiently', () => {
        const value = 'abcdefghijklmnopqrstuvwxyz';
        const pattern = '%' + 'a%b%c%d%e%f%g%h%i%j%k%l%m%n%o%p%q%r%s%t%u%v%w%x%y%z'.split('').join('%') + '%';

        const start = performance.now();
        const result = likeMatch(value, 'a%z');
        const elapsed = performance.now() - start;

        expect(result).toBe(true);
        expect(elapsed).toBeLessThan(100);
      });
    });
  });

  // =========================================================================
  // sqlLikeMatch (with NULL semantics)
  // =========================================================================

  describe('sqlLikeMatch', () => {
    it('returns false for null value', () => {
      expect(sqlLikeMatch(null, 'hello%')).toBe(false);
    });

    it('returns false for null pattern', () => {
      expect(sqlLikeMatch('hello', null)).toBe(false);
    });

    it('returns false for undefined value', () => {
      expect(sqlLikeMatch(undefined, 'hello%')).toBe(false);
    });

    it('converts non-strings to strings', () => {
      expect(sqlLikeMatch(123, '1%')).toBe(true);
      expect(sqlLikeMatch(123, '12_')).toBe(true);
      expect(sqlLikeMatch(true, 'true')).toBe(true);
    });

    it('matches strings correctly', () => {
      expect(sqlLikeMatch('hello', 'h%')).toBe(true);
      expect(sqlLikeMatch('hello', 'H%')).toBe(true);
      expect(sqlLikeMatch('hello', 'world%')).toBe(false);
    });
  });

  // =========================================================================
  // likeMatchWithEscape
  // =========================================================================

  describe('likeMatchWithEscape', () => {
    it('escapes % with escape character', () => {
      expect(likeMatchWithEscape('50%', '50\\%', '\\')).toBe(true);
      expect(likeMatchWithEscape('50', '50\\%', '\\')).toBe(false);
      expect(likeMatchWithEscape('50xxx', '50\\%', '\\')).toBe(false);
    });

    it('escapes _ with escape character', () => {
      expect(likeMatchWithEscape('a_b', 'a\\_b', '\\')).toBe(true);
      expect(likeMatchWithEscape('aXb', 'a\\_b', '\\')).toBe(false);
    });

    it('allows custom escape character', () => {
      expect(likeMatchWithEscape('50%', '50!%', '!')).toBe(true);
      expect(likeMatchWithEscape('a_b', 'a!_b', '!')).toBe(true);
    });

    it('mixes escaped and unescaped wildcards', () => {
      expect(likeMatchWithEscape('50% off', '50\\% %', '\\')).toBe(true);
      expect(likeMatchWithEscape('50% discount', '50\\% %', '\\')).toBe(true);
      expect(likeMatchWithEscape('50 off', '50\\% %', '\\')).toBe(false);
    });

    it('escapes the escape character itself', () => {
      expect(likeMatchWithEscape('a\\b', 'a\\\\b', '\\')).toBe(true);
    });
  });

  // =========================================================================
  // sqlLikeMatchWithEscape
  // =========================================================================

  describe('sqlLikeMatchWithEscape', () => {
    it('returns false for null value', () => {
      expect(sqlLikeMatchWithEscape(null, '50\\%', '\\')).toBe(false);
    });

    it('returns false for null pattern', () => {
      expect(sqlLikeMatchWithEscape('50%', null, '\\')).toBe(false);
    });

    it('falls back to likeMatch when escape is null', () => {
      expect(sqlLikeMatchWithEscape('hello', 'h%', null)).toBe(true);
      expect(sqlLikeMatchWithEscape('hello', 'h%', undefined)).toBe(true);
    });

    it('uses escape character when provided', () => {
      expect(sqlLikeMatchWithEscape('50%', '50\\%', '\\')).toBe(true);
      expect(sqlLikeMatchWithEscape('50xxx', '50\\%', '\\')).toBe(false);
    });
  });

  // =========================================================================
  // Edge cases
  // =========================================================================

  describe('edge cases', () => {
    it('handles empty strings', () => {
      expect(likeMatch('', '')).toBe(true);
      expect(likeMatch('', '%')).toBe(true);
      expect(likeMatch('', '_')).toBe(false);
      expect(likeMatch('a', '')).toBe(false);
    });

    it('handles patterns that are only wildcards', () => {
      expect(likeMatch('anything', '%')).toBe(true);
      expect(likeMatch('a', '_')).toBe(true);
      expect(likeMatch('ab', '__')).toBe(true);
      expect(likeMatch('abc', '__')).toBe(false);
    });

    it('handles consecutive wildcards', () => {
      expect(likeMatch('abc', '%%')).toBe(true);
      expect(likeMatch('abc', '%_%')).toBe(true);
      expect(likeMatch('a', '%_%')).toBe(true);
      expect(likeMatch('', '%_%')).toBe(false);
    });

    it('handles unicode characters', () => {
      expect(likeMatch('hello', 'hello')).toBe(true);
      expect(likeMatch('cafe', 'caf_')).toBe(true);
      expect(likeMatch('Test', '%')).toBe(true);
    });
  });
});
