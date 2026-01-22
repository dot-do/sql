/**
 * Tokenizers for Full-Text Search
 *
 * Implements FTS5-style tokenizers: simple, porter, and unicode61.
 */

import type { Token, Tokenizer } from './types.js';

// =============================================================================
// Simple Tokenizer
// =============================================================================

/**
 * Simple tokenizer that splits on whitespace and punctuation,
 * converts to lowercase, and removes non-alphanumeric characters.
 */
export class SimpleTokenizer implements Tokenizer {
  /**
   * Tokenize text into normalized tokens
   */
  tokenize(text: string): Token[] {
    const tokens: Token[] = [];
    // Match word characters (letters, numbers, underscore)
    const regex = /\b(\w+)\b/g;
    let match: RegExpExecArray | null;
    let position = 0;

    while ((match = regex.exec(text)) !== null) {
      const term = this.normalize(match[1]);
      if (term.length > 0) {
        tokens.push({
          term,
          position: position++,
          start: match.index,
          end: match.index + match[0].length,
        });
      }
    }

    return tokens;
  }

  /**
   * Normalize a term to lowercase
   */
  normalize(term: string): string {
    return term.toLowerCase();
  }
}

// =============================================================================
// Porter Stemmer Tokenizer
// =============================================================================

/**
 * Porter stemming algorithm implementation
 * Based on the Porter Stemmer algorithm by Martin Porter
 */
function porterStem(word: string): string {
  if (word.length < 3) return word;

  let stem = word.toLowerCase();

  // Step 1a
  if (stem.endsWith('sses')) {
    stem = stem.slice(0, -2);
  } else if (stem.endsWith('ies')) {
    stem = stem.slice(0, -2);
  } else if (stem.endsWith('ss')) {
    // Do nothing
  } else if (stem.endsWith('s')) {
    stem = stem.slice(0, -1);
  }

  // Step 1b
  const step1bRules: Array<[string, string, boolean]> = [
    ['eed', 'ee', false],
    ['ed', '', true],
    ['ing', '', true],
  ];

  for (const [suffix, replacement, checkVowel] of step1bRules) {
    if (stem.endsWith(suffix)) {
      const base = stem.slice(0, -suffix.length);
      if (!checkVowel || containsVowel(base)) {
        stem = base + replacement;
        // Additional processing after removing ed/ing
        if (checkVowel && replacement === '') {
          if (stem.endsWith('at') || stem.endsWith('bl') || stem.endsWith('iz')) {
            stem += 'e';
          } else if (endsWithDouble(stem)) {
            stem = stem.slice(0, -1);
          } else if (isCVC(stem)) {
            stem += 'e';
          }
        }
        break;
      }
    }
  }

  // Step 1c
  if (stem.endsWith('y') && containsVowel(stem.slice(0, -1))) {
    stem = stem.slice(0, -1) + 'i';
  }

  // Step 2
  const step2Rules: Array<[string, string]> = [
    ['ational', 'ate'],
    ['tional', 'tion'],
    ['enci', 'ence'],
    ['anci', 'ance'],
    ['izer', 'ize'],
    ['abli', 'able'],
    ['alli', 'al'],
    ['entli', 'ent'],
    ['eli', 'e'],
    ['ousli', 'ous'],
    ['ization', 'ize'],
    ['ation', 'ate'],
    ['ator', 'ate'],
    ['alism', 'al'],
    ['iveness', 'ive'],
    ['fulness', 'ful'],
    ['ousness', 'ous'],
    ['aliti', 'al'],
    ['iviti', 'ive'],
    ['biliti', 'ble'],
  ];

  for (const [suffix, replacement] of step2Rules) {
    if (stem.endsWith(suffix)) {
      const base = stem.slice(0, -suffix.length);
      if (measure(base) > 0) {
        stem = base + replacement;
        break;
      }
    }
  }

  // Step 3
  const step3Rules: Array<[string, string]> = [
    ['icate', 'ic'],
    ['ative', ''],
    ['alize', 'al'],
    ['iciti', 'ic'],
    ['ical', 'ic'],
    ['ful', ''],
    ['ness', ''],
  ];

  for (const [suffix, replacement] of step3Rules) {
    if (stem.endsWith(suffix)) {
      const base = stem.slice(0, -suffix.length);
      if (measure(base) > 0) {
        stem = base + replacement;
        break;
      }
    }
  }

  // Step 4
  const step4Suffixes = [
    'al', 'ance', 'ence', 'er', 'ic', 'able', 'ible', 'ant', 'ement',
    'ment', 'ent', 'ion', 'ou', 'ism', 'ate', 'iti', 'ous', 'ive', 'ize',
  ];

  for (const suffix of step4Suffixes) {
    if (stem.endsWith(suffix)) {
      const base = stem.slice(0, -suffix.length);
      if (measure(base) > 1) {
        if (suffix === 'ion') {
          if (base.endsWith('s') || base.endsWith('t')) {
            stem = base;
          }
        } else {
          stem = base;
        }
        break;
      }
    }
  }

  // Step 5a
  if (stem.endsWith('e')) {
    const base = stem.slice(0, -1);
    if (measure(base) > 1 || (measure(base) === 1 && !isCVC(base))) {
      stem = base;
    }
  }

  // Step 5b
  if (stem.endsWith('ll') && measure(stem) > 1) {
    stem = stem.slice(0, -1);
  }

  return stem;
}

/**
 * Check if string contains a vowel
 */
function containsVowel(s: string): boolean {
  return /[aeiou]/.test(s);
}

/**
 * Check if string ends with a double consonant
 */
function endsWithDouble(s: string): boolean {
  if (s.length < 2) return false;
  const last = s[s.length - 1];
  return s[s.length - 2] === last && !'aeiou'.includes(last);
}

/**
 * Check if string ends with consonant-vowel-consonant
 * where the last consonant is not w, x, or y
 */
function isCVC(s: string): boolean {
  if (s.length < 3) return false;
  const last3 = s.slice(-3);
  const [c1, v, c2] = last3;
  return (
    !'aeiou'.includes(c1) &&
    'aeiou'.includes(v) &&
    !'aeiouwxy'.includes(c2)
  );
}

/**
 * Calculate the measure (m) of a word
 * m = number of VC (vowel-consonant) sequences
 */
function measure(s: string): number {
  // Convert to VC pattern
  let pattern = '';
  for (const c of s) {
    pattern += 'aeiou'.includes(c) ? 'V' : 'C';
  }
  // Count VC sequences
  const matches = pattern.match(/VC/g);
  return matches ? matches.length : 0;
}

/**
 * Porter stemmer tokenizer that applies Porter stemming
 */
export class PorterTokenizer implements Tokenizer {
  private simple = new SimpleTokenizer();

  /**
   * Tokenize and stem text
   */
  tokenize(text: string): Token[] {
    const tokens = this.simple.tokenize(text);
    return tokens.map((token) => ({
      ...token,
      term: this.normalize(token.term),
    }));
  }

  /**
   * Normalize term with Porter stemming
   */
  normalize(term: string): string {
    return porterStem(term.toLowerCase());
  }
}

// =============================================================================
// Unicode Tokenizer
// =============================================================================

/**
 * Unicode-aware tokenizer (unicode61-style)
 * Handles Unicode text with proper word boundaries
 */
export class UnicodeTokenizer implements Tokenizer {
  /**
   * Tokenize Unicode text
   */
  tokenize(text: string): Token[] {
    const tokens: Token[] = [];
    // Use Unicode word boundary regex
    // This handles letters from various scripts
    const regex = /[\p{L}\p{N}]+/gu;
    let match: RegExpExecArray | null;
    let position = 0;

    while ((match = regex.exec(text)) !== null) {
      const term = this.normalize(match[0]);
      if (term.length > 0) {
        tokens.push({
          term,
          position: position++,
          start: match.index,
          end: match.index + match[0].length,
        });
      }
    }

    return tokens;
  }

  /**
   * Normalize term with Unicode normalization
   */
  normalize(term: string): string {
    // NFC normalization, then lowercase
    return term.normalize('NFC').toLowerCase();
  }
}

// =============================================================================
// Trigram Tokenizer
// =============================================================================

/**
 * Trigram tokenizer for substring matching
 */
export class TrigramTokenizer implements Tokenizer {
  /**
   * Generate trigrams from text
   */
  tokenize(text: string): Token[] {
    const normalized = this.normalize(text);
    const tokens: Token[] = [];
    let position = 0;

    // Generate trigrams
    for (let i = 0; i <= normalized.length - 3; i++) {
      const trigram = normalized.slice(i, i + 3);
      tokens.push({
        term: trigram,
        position: position++,
        start: i,
        end: i + 3,
      });
    }

    return tokens;
  }

  /**
   * Normalize text for trigram generation
   */
  normalize(term: string): string {
    return term.toLowerCase().replace(/\s+/g, ' ');
  }
}

// =============================================================================
// Factory Function
// =============================================================================

/**
 * Create a tokenizer by type
 */
export function createTokenizer(type: string): Tokenizer {
  switch (type) {
    case 'porter':
      return new PorterTokenizer();
    case 'unicode61':
      return new UnicodeTokenizer();
    case 'trigram':
      return new TrigramTokenizer();
    case 'simple':
    default:
      return new SimpleTokenizer();
  }
}

/**
 * Convenience function to tokenize text
 */
export function tokenize(text: string, tokenizerType: string = 'simple'): Token[] {
  const tokenizer = createTokenizer(tokenizerType);
  return tokenizer.tokenize(text);
}
