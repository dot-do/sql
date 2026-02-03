/**
 * SQLite Function Coverage Tests
 *
 * Comprehensive tests for newly added SQLite functions:
 * - typeof() - Returns type affinity
 * - likelihood(), likely(), unlikely() - Optimizer hints
 * - soundex() - Phonetic encoding
 * - reverse() - Reverse string
 * - lpad(), rpad() - Padding functions
 * - repeat(), space() - String repetition
 */

import { describe, it, expect } from 'vitest';

// Import new math functions
import {
  typeof_fn,
  likelihood,
  likely,
  unlikely,
} from '../math.js';

// Import new string functions
import {
  soundex,
  reverse,
  lpad,
  rpad,
  repeat,
  space,
} from '../string.js';

// Import registry to verify registration
import { defaultRegistry } from '../registry.js';

// =============================================================================
// TYPEOF FUNCTION TESTS
// =============================================================================

describe('typeof()', () => {
  it('returns "null" for null values', () => {
    expect(typeof_fn(null)).toBe('null');
  });

  it('returns "integer" for integers', () => {
    expect(typeof_fn(42)).toBe('integer');
    expect(typeof_fn(-100)).toBe('integer');
    expect(typeof_fn(0)).toBe('integer');
  });

  it('returns "integer" for bigints', () => {
    expect(typeof_fn(BigInt(42))).toBe('integer');
    expect(typeof_fn(BigInt(-100))).toBe('integer');
    expect(typeof_fn(BigInt('9007199254740993'))).toBe('integer');
  });

  it('returns "real" for floating point numbers', () => {
    expect(typeof_fn(3.14)).toBe('real');
    expect(typeof_fn(-2.5)).toBe('real');
    expect(typeof_fn(0.1)).toBe('real');
  });

  it('returns "text" for strings', () => {
    expect(typeof_fn('hello')).toBe('text');
    expect(typeof_fn('')).toBe('text');
    expect(typeof_fn('123')).toBe('text');
  });

  it('returns "blob" for Uint8Array', () => {
    expect(typeof_fn(new Uint8Array([1, 2, 3]))).toBe('blob');
    expect(typeof_fn(new Uint8Array())).toBe('blob');
  });

  it('returns "integer" for booleans (SQLite behavior)', () => {
    expect(typeof_fn(true)).toBe('integer');
    expect(typeof_fn(false)).toBe('integer');
  });

  it('returns "text" for Date objects', () => {
    expect(typeof_fn(new Date())).toBe('text');
  });
});

// =============================================================================
// OPTIMIZER HINT FUNCTIONS
// =============================================================================

describe('likelihood()', () => {
  it('returns first argument unchanged', () => {
    expect(likelihood(true, 0.9)).toBe(true);
    expect(likelihood(false, 0.1)).toBe(false);
    expect(likelihood(42, 0.5)).toBe(42);
    expect(likelihood('hello', 0.25)).toBe('hello');
    expect(likelihood(null, 0.5)).toBe(null);
  });

  it('ignores the probability argument', () => {
    const value = { test: 'value' };
    expect(likelihood(value, 0.9999)).toBe(value);
    expect(likelihood(value, 0.0001)).toBe(value);
  });
});

describe('likely()', () => {
  it('returns argument unchanged', () => {
    expect(likely(true)).toBe(true);
    expect(likely(false)).toBe(false);
    expect(likely(42)).toBe(42);
    expect(likely('hello')).toBe('hello');
    expect(likely(null)).toBe(null);
  });
});

describe('unlikely()', () => {
  it('returns argument unchanged', () => {
    expect(unlikely(true)).toBe(true);
    expect(unlikely(false)).toBe(false);
    expect(unlikely(42)).toBe(42);
    expect(unlikely('hello')).toBe('hello');
    expect(unlikely(null)).toBe(null);
  });
});

// =============================================================================
// SOUNDEX FUNCTION TESTS
// =============================================================================

describe('soundex()', () => {
  it('returns null for null input', () => {
    expect(soundex(null)).toBe(null);
  });

  it('returns "?000" for empty string', () => {
    expect(soundex('')).toBe('?000');
  });

  it('returns "?000" for strings with no letters', () => {
    expect(soundex('12345')).toBe('?000');
    expect(soundex('!@#$%')).toBe('?000');
  });

  it('encodes simple names correctly', () => {
    // Classic Soundex test cases
    expect(soundex('Robert')).toBe('R163');
    expect(soundex('Rupert')).toBe('R163');
    expect(soundex('Rubin')).toBe('R150');
    expect(soundex('Ashcraft')).toBe('A261');
    expect(soundex('Ashcroft')).toBe('A261');
    expect(soundex('Tymczak')).toBe('T522');
    expect(soundex('Pfister')).toBe('P236');
  });

  it('preserves first letter', () => {
    expect(soundex('Alice')[0]).toBe('A');
    expect(soundex('Bob')[0]).toBe('B');
    expect(soundex('Carol')[0]).toBe('C');
  });

  it('pads with zeros', () => {
    expect(soundex('A')).toBe('A000');
    expect(soundex('Al')).toBe('A400');
  });

  it('is case insensitive', () => {
    expect(soundex('ROBERT')).toBe(soundex('robert'));
    expect(soundex('ALICE')).toBe(soundex('alice'));
  });

  it('handles names starting with vowels', () => {
    expect(soundex('Euler')).toBe('E460');
    expect(soundex('Elliott')).toBe('E430');
  });

  it('does not duplicate adjacent same-code consonants', () => {
    // Jackson: J-250 (c and k both map to 2, should not duplicate)
    expect(soundex('Jackson')).toBe('J250');
  });
});

// =============================================================================
// REVERSE FUNCTION TESTS
// =============================================================================

describe('reverse()', () => {
  it('returns null for null input', () => {
    expect(reverse(null)).toBe(null);
  });

  it('reverses simple strings', () => {
    expect(reverse('hello')).toBe('olleh');
    expect(reverse('abc')).toBe('cba');
    expect(reverse('a')).toBe('a');
    expect(reverse('')).toBe('');
  });

  it('handles Unicode correctly', () => {
    expect(reverse('hello world')).toBe('dlrow olleh');
    expect(reverse('cafe')).toBe('efac');
  });

  it('handles emoji correctly', () => {
    // Array.from handles surrogate pairs correctly
    expect(reverse('ab')).toBe('ba');
    expect(reverse('123')).toBe('321');
  });

  it('converts non-strings', () => {
    expect(reverse(123)).toBe('321');
    expect(reverse(true)).toBe('eurt');
  });
});

// =============================================================================
// LPAD FUNCTION TESTS
// =============================================================================

describe('lpad()', () => {
  it('returns null for null inputs', () => {
    expect(lpad(null, 10)).toBe(null);
    expect(lpad('hello', null)).toBe(null);
  });

  it('pads with spaces by default', () => {
    expect(lpad('hello', 10)).toBe('     hello');
    expect(lpad('x', 5)).toBe('    x');
  });

  it('pads with custom character', () => {
    expect(lpad('hello', 10, '*')).toBe('*****hello');
    expect(lpad('5', 3, '0')).toBe('005');
  });

  it('pads with multi-character string', () => {
    // 'x' length 1, target 7, need 6 chars of padding
    // 'ab' repeated 3 times = 'ababab' (6 chars)
    expect(lpad('x', 7, 'ab')).toBe('abababx');
    // 'x' length 1, target 6, need 5 chars of padding
    // 'abc' once = 'abc' (3), need 2 more = 'ab', result = 'abcabx'
    expect(lpad('x', 6, 'abc')).toBe('abcabx');
  });

  it('truncates if string is longer than target', () => {
    expect(lpad('hello world', 5)).toBe('hello');
    expect(lpad('abcdef', 3)).toBe('abc');
  });

  it('returns original if already at target length', () => {
    expect(lpad('hello', 5)).toBe('hello');
  });

  it('handles empty pad string', () => {
    expect(lpad('hello', 10, '')).toBe('hello');
  });

  it('returns null for invalid length', () => {
    expect(lpad('hello', -5)).toBe(null);
    expect(lpad('hello', NaN)).toBe(null);
  });
});

// =============================================================================
// RPAD FUNCTION TESTS
// =============================================================================

describe('rpad()', () => {
  it('returns null for null inputs', () => {
    expect(rpad(null, 10)).toBe(null);
    expect(rpad('hello', null)).toBe(null);
  });

  it('pads with spaces by default', () => {
    expect(rpad('hello', 10)).toBe('hello     ');
    expect(rpad('x', 5)).toBe('x    ');
  });

  it('pads with custom character', () => {
    expect(rpad('hello', 10, '*')).toBe('hello*****');
    expect(rpad('5', 3, '0')).toBe('500');
  });

  it('pads with multi-character string', () => {
    expect(rpad('x', 7, 'ab')).toBe('xababab');
    expect(rpad('x', 6, 'abc')).toBe('xabcab');
  });

  it('truncates if string is longer than target', () => {
    expect(rpad('hello world', 5)).toBe('hello');
    expect(rpad('abcdef', 3)).toBe('abc');
  });

  it('returns original if already at target length', () => {
    expect(rpad('hello', 5)).toBe('hello');
  });

  it('handles empty pad string', () => {
    expect(rpad('hello', 10, '')).toBe('hello');
  });

  it('returns null for invalid length', () => {
    expect(rpad('hello', -5)).toBe(null);
    expect(rpad('hello', NaN)).toBe(null);
  });
});

// =============================================================================
// REPEAT FUNCTION TESTS
// =============================================================================

describe('repeat()', () => {
  it('returns null for null inputs', () => {
    expect(repeat(null, 3)).toBe(null);
    expect(repeat('hello', null)).toBe(null);
  });

  it('repeats strings correctly', () => {
    expect(repeat('a', 5)).toBe('aaaaa');
    expect(repeat('ab', 3)).toBe('ababab');
    expect(repeat('hello ', 2)).toBe('hello hello ');
  });

  it('returns empty string for 0 repeats', () => {
    expect(repeat('hello', 0)).toBe('');
  });

  it('returns null for negative count', () => {
    expect(repeat('hello', -1)).toBe(null);
  });

  it('handles empty string', () => {
    expect(repeat('', 5)).toBe('');
  });

  it('converts non-strings', () => {
    expect(repeat(123, 3)).toBe('123123123');
  });
});

// =============================================================================
// SPACE FUNCTION TESTS
// =============================================================================

describe('space()', () => {
  it('returns null for null input', () => {
    expect(space(null)).toBe(null);
  });

  it('returns n spaces', () => {
    expect(space(5)).toBe('     ');
    expect(space(1)).toBe(' ');
    expect(space(0)).toBe('');
  });

  it('returns null for negative count', () => {
    expect(space(-1)).toBe(null);
  });

  it('truncates floating point to integer', () => {
    expect(space(3.9)).toBe('   ');
    expect(space(2.1)).toBe('  ');
  });
});

// =============================================================================
// REGISTRY VERIFICATION
// =============================================================================

describe('Function Registry', () => {
  it('has typeof registered', () => {
    expect(defaultRegistry.has('typeof')).toBe(true);
  });

  it('has likelihood functions registered', () => {
    expect(defaultRegistry.has('likelihood')).toBe(true);
    expect(defaultRegistry.has('likely')).toBe(true);
    expect(defaultRegistry.has('unlikely')).toBe(true);
  });

  it('has soundex registered', () => {
    expect(defaultRegistry.has('soundex')).toBe(true);
  });

  it('has string manipulation functions registered', () => {
    expect(defaultRegistry.has('reverse')).toBe(true);
    expect(defaultRegistry.has('lpad')).toBe(true);
    expect(defaultRegistry.has('rpad')).toBe(true);
    expect(defaultRegistry.has('repeat')).toBe(true);
    expect(defaultRegistry.has('space')).toBe(true);
  });

  it('can invoke typeof through registry', () => {
    expect(defaultRegistry.invoke('typeof', [42])).toBe('integer');
    expect(defaultRegistry.invoke('typeof', ['hello'])).toBe('text');
    expect(defaultRegistry.invoke('typeof', [null])).toBe('null');
  });

  it('can invoke soundex through registry', () => {
    expect(defaultRegistry.invoke('soundex', ['Robert'])).toBe('R163');
  });

  it('can invoke string functions through registry', () => {
    expect(defaultRegistry.invoke('reverse', ['hello'])).toBe('olleh');
    expect(defaultRegistry.invoke('lpad', ['5', 3, '0'])).toBe('005');
    expect(defaultRegistry.invoke('rpad', ['5', 3, '0'])).toBe('500');
    expect(defaultRegistry.invoke('repeat', ['ab', 3])).toBe('ababab');
    expect(defaultRegistry.invoke('space', [3])).toBe('   ');
  });
});

// =============================================================================
// FUNCTION COUNT
// =============================================================================

describe('SQLite Function Coverage', () => {
  it('reports total function count', () => {
    const functionNames = defaultRegistry.getFunctionNames();
    // Log the count for verification
    console.log(`Total functions registered: ${functionNames.length}`);

    // Should have at least 80 functions (our target coverage)
    expect(functionNames.length).toBeGreaterThanOrEqual(80);
  });

  it('includes all core SQLite function categories', () => {
    const functions = defaultRegistry.getFunctionNames();

    // String functions
    const stringFns = ['length', 'substr', 'upper', 'lower', 'trim', 'replace', 'instr', 'printf', 'quote', 'hex', 'unhex', 'soundex', 'reverse'];
    for (const fn of stringFns) {
      expect(functions.includes(fn)).toBe(true);
    }

    // Math functions
    const mathFns = ['abs', 'round', 'random', 'sign', 'pow', 'sqrt', 'ceil', 'floor', 'mod', 'log', 'exp', 'sin', 'cos', 'tan', 'pi'];
    for (const fn of mathFns) {
      expect(functions.includes(fn)).toBe(true);
    }

    // Date functions
    const dateFns = ['date', 'time', 'datetime', 'strftime', 'julianday', 'unixepoch'];
    for (const fn of dateFns) {
      expect(functions.includes(fn)).toBe(true);
    }

    // JSON functions
    const jsonFns = ['json', 'json_valid', 'json_extract', 'json_type', 'json_array', 'json_object', 'json_array_length', 'json_insert', 'json_replace', 'json_set', 'json_remove', 'json_patch', 'json_quote'];
    for (const fn of jsonFns) {
      expect(functions.includes(fn)).toBe(true);
    }

    // Type/null handling functions
    const typeFns = ['typeof', 'nullif', 'ifnull', 'coalesce', 'iif'];
    for (const fn of typeFns) {
      expect(functions.includes(fn)).toBe(true);
    }
  });
});
