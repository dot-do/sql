/**
 * SQL LIKE Pattern Matching Utility
 *
 * This module provides a secure, consolidated implementation of SQL LIKE
 * pattern matching. All LIKE operations in the codebase should use this
 * utility to ensure consistent behavior and proper regex escaping.
 *
 * For performance and ReDoS safety, this module delegates to safeLikeMatch
 * which uses a dynamic programming algorithm instead of regex.
 *
 * SQL LIKE wildcards:
 * - % matches any sequence of zero or more characters
 * - _ matches any single character
 *
 * @example
 * ```ts
 * import { likeMatch, likeToRegex } from './utils/like.js';
 *
 * likeMatch('hello', 'h%');     // true
 * likeMatch('hello', 'h_llo'); // true
 * likeMatch('hello', 'h.llo'); // false (. is not a wildcard)
 * ```
 */

import { safeLikeMatch, safeLikeMatchWithEscape } from './safe-like.js';

// =============================================================================
// REGEX ESCAPING
// =============================================================================

/**
 * Characters that have special meaning in regular expressions and must be escaped.
 * This includes all standard regex metacharacters.
 */
const REGEX_SPECIAL_CHARS = /[.*+?^${}()|[\]\\]/g;

/**
 * Escape special regex characters in a string.
 *
 * @param str - The string to escape
 * @returns The escaped string safe for use in a regex
 *
 * @example
 * ```ts
 * escapeRegex('hello.world'); // 'hello\\.world'
 * escapeRegex('test[1]');     // 'test\\[1\\]'
 * ```
 */
export function escapeRegex(str: string): string {
  return str.replace(REGEX_SPECIAL_CHARS, '\\$&');
}

// =============================================================================
// LIKE PATTERN CONVERSION (for cases where regex is explicitly needed)
// =============================================================================

/**
 * Convert a SQL LIKE pattern to a JavaScript RegExp.
 *
 * This function properly escapes all regex special characters in the pattern
 * before converting SQL wildcards (% and _) to their regex equivalents.
 *
 * SECURITY: This function is designed to be safe against regex injection.
 * User input containing regex metacharacters (like . * + ?) will be properly
 * escaped and treated as literal characters.
 *
 * WARNING: For LIKE matching, prefer using likeMatch() which uses a ReDoS-safe
 * dynamic programming algorithm. Only use this function when you explicitly
 * need a RegExp object.
 *
 * @param pattern - The SQL LIKE pattern
 * @param flags - Optional regex flags (default: 'i' for case-insensitive)
 * @returns A compiled RegExp object
 *
 * @example
 * ```ts
 * const regex = likeToRegex('hello%');
 * regex.test('hello world'); // true
 *
 * // Dots are literal, not wildcards
 * const regex2 = likeToRegex('test.txt');
 * regex2.test('test.txt');  // true
 * regex2.test('testXtxt');  // false
 * ```
 */
export function likeToRegex(pattern: string, flags = 'i'): RegExp {
  // First escape all regex special characters, then convert SQL wildcards
  const regexPattern = escapeRegex(pattern)
    .replace(/%/g, '.*')  // % -> .* (any sequence)
    .replace(/_/g, '.');  // _ -> . (any single char)

  return new RegExp(`^${regexPattern}$`, flags);
}

/**
 * Convert a SQL LIKE pattern to a regex pattern string.
 *
 * Use this when you need the pattern string rather than a compiled RegExp,
 * for example when caching or serializing patterns.
 *
 * WARNING: For LIKE matching, prefer using likeMatch() which uses a ReDoS-safe
 * dynamic programming algorithm.
 *
 * @param pattern - The SQL LIKE pattern
 * @returns The regex pattern string (without anchors or flags)
 *
 * @example
 * ```ts
 * likeToRegexPattern('hello%'); // 'hello.*'
 * likeToRegexPattern('test_');  // 'test.'
 * ```
 */
export function likeToRegexPattern(pattern: string): string {
  return escapeRegex(pattern)
    .replace(/%/g, '.*')
    .replace(/_/g, '.');
}

// =============================================================================
// LIKE MATCHING (ReDoS-safe using dynamic programming)
// =============================================================================

/**
 * Match a value against a SQL LIKE pattern.
 *
 * This is the primary function for LIKE pattern matching. It handles:
 * - Proper handling of regex special characters (treated as literals)
 * - SQL wildcards (% and _)
 * - Case-insensitive matching (default)
 * - ReDoS-safe via dynamic programming algorithm
 *
 * @param value - The value to test
 * @param pattern - The SQL LIKE pattern
 * @param caseSensitive - If true, matching is case-sensitive (default: false)
 * @returns true if the value matches the pattern, false otherwise
 *
 * @example
 * ```ts
 * likeMatch('hello', 'h%');      // true
 * likeMatch('hello', 'H%');      // true (case-insensitive)
 * likeMatch('hello', 'H%', true); // false (case-sensitive)
 * likeMatch('hello', 'h_llo');   // true
 * likeMatch('hello', 'h.llo');   // false (. is not a wildcard)
 * likeMatch('a.b.c', 'a%c');     // true
 * likeMatch('a*b*c', 'a%c');     // true
 * ```
 */
export function likeMatch(
  value: string,
  pattern: string,
  caseSensitive = false
): boolean {
  return safeLikeMatch(value, pattern, !caseSensitive);
}

/**
 * Match a SQL value against a SQL LIKE pattern with SQL NULL semantics.
 *
 * In SQL, LIKE comparisons with NULL always return NULL (which is falsy).
 * This function implements that behavior.
 *
 * @param value - The SQL value to test (may be null)
 * @param pattern - The SQL LIKE pattern (may be null)
 * @param caseSensitive - If true, matching is case-sensitive (default: false)
 * @returns true if matches, false if no match or either value is null
 *
 * @example
 * ```ts
 * sqlLikeMatch('hello', 'h%');   // true
 * sqlLikeMatch(null, 'h%');      // false (NULL semantics)
 * sqlLikeMatch('hello', null);   // false (NULL semantics)
 * sqlLikeMatch(123, '1%');       // true (coerced to string)
 * ```
 */
export function sqlLikeMatch(
  value: unknown,
  pattern: unknown,
  caseSensitive = false
): boolean {
  // SQL NULL semantics: NULL LIKE anything = NULL (falsy)
  if (value === null || value === undefined) return false;
  if (pattern === null || pattern === undefined) return false;

  return likeMatch(String(value), String(pattern), caseSensitive);
}

// =============================================================================
// LIKE WITH ESCAPE CHARACTER
// =============================================================================

/**
 * Match a value against a SQL LIKE pattern with an escape character.
 *
 * The escape character allows matching literal % and _ characters.
 * This is the SQL LIKE ... ESCAPE syntax.
 *
 * @param value - The value to test
 * @param pattern - The SQL LIKE pattern
 * @param escapeChar - The escape character (e.g., '\\' or '!')
 * @param caseSensitive - If true, matching is case-sensitive (default: false)
 * @returns true if the value matches the pattern
 *
 * @example
 * ```ts
 * // Match literal % using \ as escape
 * likeMatchWithEscape('50%', '50\\%', '\\');  // true
 * likeMatchWithEscape('50x', '50\\%', '\\');  // false
 *
 * // Match literal _ using ! as escape
 * likeMatchWithEscape('a_b', 'a!_b', '!');    // true
 * likeMatchWithEscape('axb', 'a!_b', '!');    // false
 * ```
 */
export function likeMatchWithEscape(
  value: string,
  pattern: string,
  escapeChar: string,
  caseSensitive = false
): boolean {
  return safeLikeMatchWithEscape(value, pattern, escapeChar, !caseSensitive);
}

/**
 * Match a SQL value against a LIKE pattern with escape character and SQL NULL semantics.
 *
 * @param value - The SQL value to test (may be null)
 * @param pattern - The SQL LIKE pattern (may be null)
 * @param escapeChar - The escape character (may be null for no escape)
 * @param caseSensitive - If true, matching is case-sensitive (default: false)
 * @returns true if matches, false if no match or value/pattern is null
 *
 * @example
 * ```ts
 * sqlLikeMatchWithEscape('50%', '50\\%', '\\');  // true
 * sqlLikeMatchWithEscape(null, '50\\%', '\\');   // false
 * sqlLikeMatchWithEscape('hello', 'h%', null);   // true (no escape char)
 * ```
 */
export function sqlLikeMatchWithEscape(
  value: unknown,
  pattern: unknown,
  escapeChar: unknown,
  caseSensitive = false
): boolean {
  if (value === null || value === undefined) return false;
  if (pattern === null || pattern === undefined) return false;

  const strValue = String(value);
  const strPattern = String(pattern);

  if (escapeChar === null || escapeChar === undefined) {
    return likeMatch(strValue, strPattern, caseSensitive);
  }

  return likeMatchWithEscape(
    strValue,
    strPattern,
    String(escapeChar),
    caseSensitive
  );
}
