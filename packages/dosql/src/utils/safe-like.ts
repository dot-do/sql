/**
 * Safe SQL LIKE Pattern Matching
 *
 * Provides ReDoS-safe implementation of SQL LIKE pattern matching.
 * Uses a custom algorithm instead of regex to avoid catastrophic backtracking.
 *
 * SECURITY: This module addresses ReDoS vulnerability (sql-axvi)
 * The naive approach of converting LIKE patterns to regex with:
 *   pattern.replace(/%/g, '.*').replace(/_/g, '.')
 * is vulnerable to catastrophic backtracking on patterns like:
 *   %a%a%a%a%a%a% matched against "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab"
 *
 * Instead, we use a dynamic programming approach that runs in O(n*m) time
 * where n is the string length and m is the pattern length.
 */

/**
 * Maximum pattern length to prevent excessive memory/time usage
 */
const MAX_PATTERN_LENGTH = 1000;

/**
 * Maximum string length to prevent excessive memory/time usage
 */
const MAX_STRING_LENGTH = 100000;

/**
 * Match a string against a SQL LIKE pattern safely.
 *
 * @param value - The string to test
 * @param pattern - The SQL LIKE pattern (using % and _ wildcards)
 * @param caseInsensitive - Whether to perform case-insensitive matching (default: true)
 * @returns true if the value matches the pattern
 *
 * @example
 * ```ts
 * safeLikeMatch('hello world', '%world'); // true
 * safeLikeMatch('hello world', 'hello%'); // true
 * safeLikeMatch('abc', 'a_c'); // true
 * safeLikeMatch('abbc', 'a_c'); // false
 * ```
 */
export function safeLikeMatch(
  value: string,
  pattern: string,
  caseInsensitive = true
): boolean {
  // Handle length limits to prevent DoS
  if (pattern.length > MAX_PATTERN_LENGTH) {
    throw new Error(`LIKE pattern exceeds maximum length of ${MAX_PATTERN_LENGTH}`);
  }
  if (value.length > MAX_STRING_LENGTH) {
    throw new Error(`String exceeds maximum length of ${MAX_STRING_LENGTH}`);
  }

  // Normalize case if case-insensitive
  const s = caseInsensitive ? value.toLowerCase() : value;
  const p = caseInsensitive ? pattern.toLowerCase() : pattern;

  const sLen = s.length;
  const pLen = p.length;

  // Dynamic programming approach
  // dp[i][j] = true if s[0..i-1] matches p[0..j-1]
  // We use two rows to save memory (current and previous)
  let prev = new Array<boolean>(pLen + 1).fill(false);
  let curr = new Array<boolean>(pLen + 1).fill(false);

  // Empty string matches empty pattern
  prev[0] = true;

  // Handle patterns starting with % (which can match empty string)
  for (let j = 1; j <= pLen; j++) {
    if (p[j - 1] === '%') {
      prev[j] = prev[j - 1] ?? false;
    }
  }

  // Fill the DP table
  for (let i = 1; i <= sLen; i++) {
    curr[0] = false;

    for (let j = 1; j <= pLen; j++) {
      const pc = p[j - 1];

      if (pc === '%') {
        // % matches zero or more characters
        // Either we don't use % (prev[j]) or we use it (curr[j-1])
        curr[j] = (prev[j] ?? false) || (curr[j - 1] ?? false);
      } else if (pc === '_') {
        // _ matches exactly one character
        curr[j] = prev[j - 1] ?? false;
      } else {
        // Literal character - must match exactly
        curr[j] = (prev[j - 1] ?? false) && s[i - 1] === pc;
      }
    }

    // Swap rows
    [prev, curr] = [curr, prev];
  }

  return prev[pLen] ?? false;
}

/**
 * Match a string against a SQL LIKE pattern with ESCAPE clause support.
 *
 * @param value - The string to test
 * @param pattern - The SQL LIKE pattern (using % and _ wildcards)
 * @param escape - The escape character (e.g., '\' to match literal % with \%)
 * @param caseInsensitive - Whether to perform case-insensitive matching (default: true)
 * @returns true if the value matches the pattern
 *
 * @example
 * ```ts
 * safeLikeMatchWithEscape('50%', '%\\%', '\\'); // true - matches literal %
 * safeLikeMatchWithEscape('_test', '\\_test', '\\'); // true - matches literal _
 * ```
 */
export function safeLikeMatchWithEscape(
  value: string,
  pattern: string,
  escape: string | null,
  caseInsensitive = true
): boolean {
  // Handle length limits to prevent DoS
  if (pattern.length > MAX_PATTERN_LENGTH) {
    throw new Error(`LIKE pattern exceeds maximum length of ${MAX_PATTERN_LENGTH}`);
  }
  if (value.length > MAX_STRING_LENGTH) {
    throw new Error(`String exceeds maximum length of ${MAX_STRING_LENGTH}`);
  }

  // If no escape character, use the simple matcher
  if (!escape) {
    return safeLikeMatch(value, pattern, caseInsensitive);
  }

  // Parse the pattern with escape handling
  // Convert to a normalized pattern where escape sequences are resolved
  const normalizedPattern: Array<{ type: 'literal' | '%' | '_'; char?: string }> = [];
  let i = 0;

  while (i < pattern.length) {
    const c = pattern[i];

    if (c === escape && i + 1 < pattern.length) {
      // Escape next character - treat as literal
      const next = pattern[i + 1];
      normalizedPattern.push({ type: 'literal', char: next });
      i += 2;
    } else if (c === '%') {
      normalizedPattern.push({ type: '%' });
      i++;
    } else if (c === '_') {
      normalizedPattern.push({ type: '_' });
      i++;
    } else {
      normalizedPattern.push({ type: 'literal', char: c });
      i++;
    }
  }

  // Normalize case
  const s = caseInsensitive ? value.toLowerCase() : value;
  const sLen = s.length;
  const pLen = normalizedPattern.length;

  // DP matching with normalized pattern
  let prev = new Array<boolean>(pLen + 1).fill(false);
  let curr = new Array<boolean>(pLen + 1).fill(false);

  prev[0] = true;

  // Handle patterns starting with % (which can match empty string)
  for (let j = 1; j <= pLen; j++) {
    const np = normalizedPattern[j - 1];
    if (np && np.type === '%') {
      prev[j] = prev[j - 1] ?? false;
    }
  }

  // Fill the DP table
  for (let si = 1; si <= sLen; si++) {
    curr[0] = false;

    for (let j = 1; j <= pLen; j++) {
      const pe = normalizedPattern[j - 1];
      if (!pe) continue;

      if (pe.type === '%') {
        curr[j] = (prev[j] ?? false) || (curr[j - 1] ?? false);
      } else if (pe.type === '_') {
        curr[j] = prev[j - 1] ?? false;
      } else {
        // Literal character
        const pc = caseInsensitive ? (pe.char ?? '').toLowerCase() : (pe.char ?? '');
        curr[j] = (prev[j - 1] ?? false) && s[si - 1] === pc;
      }
    }

    [prev, curr] = [curr, prev];
  }

  return prev[pLen] ?? false;
}

/**
 * Match a string against a Unix glob pattern safely.
 *
 * SECURITY: Uses DP approach to avoid ReDoS (sql-axvi)
 *
 * @param value - The string to test
 * @param pattern - The glob pattern (using * and ? wildcards, [...] character classes)
 * @param caseInsensitive - Whether to perform case-insensitive matching (default: false for glob)
 * @returns true if the value matches the pattern
 *
 * @example
 * ```ts
 * safeGlobMatch('hello.txt', '*.txt'); // true
 * safeGlobMatch('test123', 'test???'); // true
 * safeGlobMatch('abc', '[abc][abc][abc]'); // true
 * ```
 */
export function safeGlobMatch(
  value: string,
  pattern: string,
  caseInsensitive = false
): boolean {
  // Handle length limits to prevent DoS
  if (pattern.length > MAX_PATTERN_LENGTH) {
    throw new Error(`Glob pattern exceeds maximum length of ${MAX_PATTERN_LENGTH}`);
  }
  if (value.length > MAX_STRING_LENGTH) {
    throw new Error(`String exceeds maximum length of ${MAX_STRING_LENGTH}`);
  }

  // Parse the glob pattern into normalized tokens
  type GlobToken = { type: '*' | '?' | 'literal' | 'class'; char?: string; chars?: Set<string>; negated?: boolean };
  const tokens: GlobToken[] = [];
  let i = 0;

  while (i < pattern.length) {
    const c = pattern[i];

    if (c === '*') {
      tokens.push({ type: '*' });
      i++;
    } else if (c === '?') {
      tokens.push({ type: '?' });
      i++;
    } else if (c === '[') {
      // Parse character class
      const end = pattern.indexOf(']', i + 1);
      if (end === -1) {
        // No closing bracket - treat as literal
        tokens.push({ type: 'literal', char: c });
        i++;
      } else {
        // Parse character class content
        let classStart = i + 1;
        let negated = false;

        if (pattern[classStart] === '!' || pattern[classStart] === '^') {
          negated = true;
          classStart++;
        }

        const classContent = pattern.slice(classStart, end);
        const chars = new Set<string>();

        // Parse character ranges and individual chars
        let j = 0;
        while (j < classContent.length) {
          if (j + 2 < classContent.length && classContent[j + 1] === '-') {
            // Range like a-z
            const start = classContent.charCodeAt(j);
            const endChar = classContent.charCodeAt(j + 2);
            for (let code = start; code <= endChar; code++) {
              const ch = String.fromCharCode(code);
              chars.add(caseInsensitive ? ch.toLowerCase() : ch);
            }
            j += 3;
          } else {
            const ch = classContent[j];
            chars.add(caseInsensitive ? ch.toLowerCase() : ch);
            j++;
          }
        }

        tokens.push({ type: 'class', chars, negated });
        i = end + 1;
      }
    } else {
      tokens.push({ type: 'literal', char: c });
      i++;
    }
  }

  // Normalize case
  const s = caseInsensitive ? value.toLowerCase() : value;
  const sLen = s.length;
  const pLen = tokens.length;

  // DP matching
  let prev = new Array<boolean>(pLen + 1).fill(false);
  let curr = new Array<boolean>(pLen + 1).fill(false);

  prev[0] = true;

  // Handle patterns starting with * (which can match empty string)
  for (let j = 1; j <= pLen; j++) {
    const tok = tokens[j - 1];
    if (tok && tok.type === '*') {
      prev[j] = prev[j - 1] ?? false;
    }
  }

  // Fill the DP table
  for (let si = 1; si <= sLen; si++) {
    curr[0] = false;

    for (let j = 1; j <= pLen; j++) {
      const token = tokens[j - 1];
      if (!token) continue;

      if (token.type === '*') {
        // * matches zero or more characters
        curr[j] = (prev[j] ?? false) || (curr[j - 1] ?? false);
      } else if (token.type === '?') {
        // ? matches exactly one character
        curr[j] = prev[j - 1] ?? false;
      } else if (token.type === 'class') {
        // Character class
        const charInClass = token.chars ? token.chars.has(s[si - 1] ?? '') : false;
        const matches = token.negated ? !charInClass : charInClass;
        curr[j] = (prev[j - 1] ?? false) && matches;
      } else {
        // Literal character
        const pc = caseInsensitive ? (token.char ?? '').toLowerCase() : (token.char ?? '');
        curr[j] = (prev[j - 1] ?? false) && s[si - 1] === pc;
      }
    }

    [prev, curr] = [curr, prev];
  }

  return prev[pLen] ?? false;
}
