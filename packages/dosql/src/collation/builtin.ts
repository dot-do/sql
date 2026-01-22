/**
 * Built-in Collations for DoSQL
 *
 * Implements SQLite's three built-in collating sequences:
 * - BINARY: byte-by-byte comparison using memcmp()
 * - NOCASE: case-insensitive comparison for ASCII letters (A-Z, a-z)
 * - RTRIM: like BINARY but ignores trailing spaces
 *
 * @see https://www.sqlite.org/datatype3.html#collation
 */

import type { CollationFunction, CollationDefinition, UnicodeCollationOptions } from './types.js';

/**
 * BINARY collation - byte-by-byte comparison.
 *
 * This is the default collation for SQLite. It compares strings
 * using memcmp() semantics (byte-by-byte comparison).
 *
 * In JavaScript, this maps to standard string comparison.
 */
export const binaryCollation: CollationFunction = (a: string, b: string): number => {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
};

/**
 * NOCASE collation - case-insensitive for ASCII only.
 *
 * SQLite's NOCASE is specifically designed for ASCII characters only.
 * It folds uppercase ASCII letters (A-Z, codepoints 65-90) to lowercase
 * (a-z, codepoints 97-122).
 *
 * IMPORTANT: This does NOT handle Unicode case folding.
 * For Unicode support, use a custom collation with Intl.Collator.
 */
export const nocaseCollation: CollationFunction = (a: string, b: string): number => {
  // Convert only ASCII A-Z to lowercase for comparison
  const aLower = asciiLowerCase(a);
  const bLower = asciiLowerCase(b);

  if (aLower < bLower) return -1;
  if (aLower > bLower) return 1;
  return 0;
};

/**
 * RTRIM collation - ignores trailing spaces.
 *
 * Compares strings the same as BINARY, but ignores trailing
 * space characters (ASCII 0x20) on both operands.
 */
export const rtrimCollation: CollationFunction = (a: string, b: string): number => {
  // Remove trailing spaces
  const aTrimmed = a.replace(/\s+$/, '');
  const bTrimmed = b.replace(/\s+$/, '');

  if (aTrimmed < bTrimmed) return -1;
  if (aTrimmed > bTrimmed) return 1;
  return 0;
};

/**
 * Converts ASCII uppercase letters (A-Z) to lowercase.
 * Does NOT convert any other characters (including Unicode uppercase).
 */
function asciiLowerCase(str: string): string {
  let result = '';
  for (let i = 0; i < str.length; i++) {
    const code = str.charCodeAt(i);
    // A-Z (65-90) -> a-z (97-122)
    if (code >= 65 && code <= 90) {
      result += String.fromCharCode(code + 32);
    } else {
      result += str[i];
    }
  }
  return result;
}

/**
 * Built-in collation definitions with metadata
 */
export const BUILTIN_COLLATIONS: ReadonlyMap<string, CollationDefinition> = new Map([
  [
    'BINARY',
    {
      name: 'BINARY',
      compare: binaryCollation,
      builtin: true,
      description: 'Byte-by-byte comparison (default)',
      caseSensitive: true,
      respectsTrailingSpaces: true,
    },
  ],
  [
    'NOCASE',
    {
      name: 'NOCASE',
      compare: nocaseCollation,
      builtin: true,
      description: 'Case-insensitive for ASCII letters (A-Z)',
      caseSensitive: false,
      respectsTrailingSpaces: true,
    },
  ],
  [
    'RTRIM',
    {
      name: 'RTRIM',
      compare: rtrimCollation,
      builtin: true,
      description: 'Like BINARY but ignores trailing spaces',
      caseSensitive: true,
      respectsTrailingSpaces: false,
    },
  ],
]);

/**
 * Creates a Unicode-aware collation using Intl.Collator.
 *
 * This provides ICU-style collation for proper Unicode sorting
 * and comparison, which SQLite does not natively support.
 *
 * @param options Unicode collation options
 * @returns A collation function
 */
export function createUnicodeCollation(options: UnicodeCollationOptions): CollationFunction {
  // Map our options to Intl.Collator options
  const collatorOptions: Intl.CollatorOptions = {
    usage: 'sort',
    sensitivity: mapStrengthToSensitivity(options.strength),
    numeric: options.numeric ?? false,
  };

  if (options.caseFirst && options.caseFirst !== 'off') {
    collatorOptions.caseFirst = options.caseFirst;
  }

  if (options.alternate === 'shifted') {
    collatorOptions.ignorePunctuation = true;
  }

  const collator = new Intl.Collator(options.locale, collatorOptions);

  return (a: string, b: string): number => {
    return collator.compare(a, b);
  };
}

/**
 * Maps our strength enum to Intl.Collator sensitivity
 */
function mapStrengthToSensitivity(strength?: string): Intl.CollatorOptions['sensitivity'] {
  switch (strength) {
    case 'primary':
      // Base characters only (ignore case and accents)
      return 'base';
    case 'secondary':
      // Base characters and accents (ignore case)
      return 'accent';
    case 'tertiary':
      // Base characters, accents, and case
      return 'variant';
    case 'identical':
      // Full comparison
      return 'variant';
    default:
      return 'variant';
  }
}

/**
 * Creates a case-insensitive collation for the specified locale.
 * This is a convenience function for common use cases.
 *
 * @param locale Locale identifier (e.g., 'en', 'de', 'fr')
 * @returns A case-insensitive collation function
 */
export function createCaseInsensitiveCollation(locale: string = 'en'): CollationFunction {
  return createUnicodeCollation({
    locale,
    strength: 'secondary', // Ignore case but respect accents
  });
}

/**
 * Creates a natural sort collation that handles embedded numbers.
 * For example: "file2" < "file10" (instead of "file10" < "file2")
 *
 * @param locale Locale identifier (default: 'en')
 * @returns A natural sort collation function
 */
export function createNaturalSortCollation(locale: string = 'en'): CollationFunction {
  return createUnicodeCollation({
    locale,
    numeric: true,
    strength: 'tertiary',
  });
}

/**
 * Creates a locale-specific collation.
 *
 * @param locale Locale identifier
 * @returns A collation definition
 */
export function createLocaleCollationDefinition(locale: string): CollationDefinition {
  const collationName = `LOCALE_${locale.toUpperCase().replace('-', '_')}`;

  return {
    name: collationName,
    compare: createUnicodeCollation({ locale }),
    builtin: false,
    description: `Unicode collation for locale: ${locale}`,
    caseSensitive: true,
    respectsTrailingSpaces: true,
  };
}

/**
 * Gets a built-in collation by name.
 *
 * @param name The collation name (case-insensitive)
 * @returns The collation definition or undefined if not found
 */
export function getBuiltinCollation(name: string): CollationDefinition | undefined {
  return BUILTIN_COLLATIONS.get(name.toUpperCase());
}

/**
 * Checks if a collation name refers to a built-in collation.
 *
 * @param name The collation name
 * @returns True if it's a built-in collation
 */
export function isBuiltinCollation(name: string): boolean {
  return BUILTIN_COLLATIONS.has(name.toUpperCase());
}

/**
 * Gets all built-in collation names.
 *
 * @returns Array of built-in collation names
 */
export function getBuiltinCollationNames(): string[] {
  return Array.from(BUILTIN_COLLATIONS.keys());
}
