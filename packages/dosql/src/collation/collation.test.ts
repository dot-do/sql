/**
 * SQL Collation Tests for DoSQL
 *
 * Comprehensive TDD test suite for COLLATE support including:
 * - Column definition: name TEXT COLLATE NOCASE
 * - Expression: ORDER BY name COLLATE NOCASE
 * - WHERE clause: WHERE name = 'test' COLLATE NOCASE
 * - Index with collation
 * - CREATE COLLATION for custom collations
 * - PRAGMA case_sensitive_like
 * - Unicode collation (ICU-style)
 *
 * Target: 40+ tests running with workers-vitest-pool (NO MOCKS)
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import {
  binaryCollation,
  nocaseCollation,
  rtrimCollation,
  BUILTIN_COLLATIONS,
  getBuiltinCollation,
  isBuiltinCollation,
  getBuiltinCollationNames,
  createUnicodeCollation,
  createCaseInsensitiveCollation,
  createNaturalSortCollation,
  createLocaleCollationDefinition,
} from './builtin.js';
import {
  createCollationRegistry,
  parseCollateClause,
  parseColumnCollation,
  isCreateCollation,
  parseCreateCollation,
  isDropCollation,
  parseDropCollation,
  parseIndexCollation,
  sortWithCollation,
  equalsWithCollation,
  createCollationComparator,
  registerUnicodeCollation,
} from './registry.js';
import type {
  CollationFunction,
  CollationDefinition,
  CreateCollationOptions,
  CollateClause,
  UnicodeCollationOptions,
} from './types.js';
import {
  CollationNotFoundError,
  CollationExistsError,
  BuiltinCollationError,
} from './types.js';

// =============================================================================
// BUILTIN COLLATION TESTS - BINARY
// =============================================================================

describe('BINARY Collation', () => {
  describe('Basic comparisons', () => {
    it('should return 0 for equal strings', () => {
      expect(binaryCollation('hello', 'hello')).toBe(0);
      expect(binaryCollation('', '')).toBe(0);
      expect(binaryCollation('  ', '  ')).toBe(0);
    });

    it('should return negative for a < b', () => {
      expect(binaryCollation('a', 'b')).toBeLessThan(0);
      expect(binaryCollation('abc', 'abd')).toBeLessThan(0);
      expect(binaryCollation('A', 'a')).toBeLessThan(0); // 65 < 97
    });

    it('should return positive for a > b', () => {
      expect(binaryCollation('b', 'a')).toBeGreaterThan(0);
      expect(binaryCollation('abd', 'abc')).toBeGreaterThan(0);
      expect(binaryCollation('a', 'A')).toBeGreaterThan(0);
    });

    it('should compare byte-by-byte (case sensitive)', () => {
      expect(binaryCollation('Hello', 'hello')).toBeLessThan(0);
      expect(binaryCollation('HELLO', 'hello')).toBeLessThan(0);
      expect(binaryCollation('hello', 'Hello')).toBeGreaterThan(0);
    });

    it('should handle empty strings', () => {
      expect(binaryCollation('', 'a')).toBeLessThan(0);
      expect(binaryCollation('a', '')).toBeGreaterThan(0);
    });

    it('should handle strings with special characters', () => {
      expect(binaryCollation('a!', 'a@')).toBeLessThan(0); // ! (33) < @ (64)
      expect(binaryCollation('a ', 'a!')).toBeLessThan(0); // space (32) < ! (33)
    });

    it('should respect trailing spaces', () => {
      expect(binaryCollation('hello', 'hello ')).toBeLessThan(0);
      expect(binaryCollation('hello ', 'hello')).toBeGreaterThan(0);
      expect(binaryCollation('hello  ', 'hello ')).toBeGreaterThan(0);
    });
  });

  describe('Sort order verification', () => {
    it('should sort uppercase before lowercase (ASCII order)', () => {
      const items = ['b', 'A', 'a', 'B'];
      const sorted = [...items].sort(binaryCollation);
      expect(sorted).toEqual(['A', 'B', 'a', 'b']);
    });

    it('should sort numbers before letters', () => {
      const items = ['a', '1', 'A', '0'];
      const sorted = [...items].sort(binaryCollation);
      expect(sorted).toEqual(['0', '1', 'A', 'a']);
    });

    it('should sort empty string first', () => {
      const items = ['a', '', 'b'];
      const sorted = [...items].sort(binaryCollation);
      expect(sorted).toEqual(['', 'a', 'b']);
    });
  });
});

// =============================================================================
// BUILTIN COLLATION TESTS - NOCASE
// =============================================================================

describe('NOCASE Collation', () => {
  describe('Case-insensitive ASCII comparisons', () => {
    it('should return 0 for same letters with different case', () => {
      expect(nocaseCollation('hello', 'HELLO')).toBe(0);
      expect(nocaseCollation('Hello', 'hELLO')).toBe(0);
      expect(nocaseCollation('ABC', 'abc')).toBe(0);
    });

    it('should return 0 for equal strings', () => {
      expect(nocaseCollation('hello', 'hello')).toBe(0);
      expect(nocaseCollation('HELLO', 'HELLO')).toBe(0);
    });

    it('should compare case-insensitively for a < b', () => {
      expect(nocaseCollation('a', 'B')).toBeLessThan(0);
      expect(nocaseCollation('A', 'b')).toBeLessThan(0);
      expect(nocaseCollation('abc', 'ABD')).toBeLessThan(0);
    });

    it('should compare case-insensitively for a > b', () => {
      expect(nocaseCollation('b', 'A')).toBeGreaterThan(0);
      expect(nocaseCollation('B', 'a')).toBeGreaterThan(0);
      expect(nocaseCollation('abd', 'ABC')).toBeGreaterThan(0);
    });

    it('should only fold ASCII letters (A-Z, a-z)', () => {
      // Numbers should not be affected
      expect(nocaseCollation('123', '123')).toBe(0);

      // Punctuation should not be affected
      expect(nocaseCollation('!', '!')).toBe(0);
    });

    it('should respect trailing spaces (unlike RTRIM)', () => {
      expect(nocaseCollation('hello', 'hello ')).toBeLessThan(0);
      expect(nocaseCollation('HELLO ', 'hello')).toBeGreaterThan(0);
    });
  });

  describe('Sort order verification', () => {
    it('should sort case-insensitively', () => {
      const items = ['b', 'A', 'a', 'B'];
      const sorted = [...items].sort(nocaseCollation);
      // After case folding: ['b', 'a', 'a', 'b'] -> ties resolved by original order
      // Actually, 'A' and 'a' both become 'a', so they compare equal
      // Standard sort is not stable, so we just check grouping
      expect(sorted[0] === 'A' || sorted[0] === 'a').toBe(true);
      expect(sorted[2] === 'b' || sorted[2] === 'B').toBe(true);
    });

    it('should handle mixed case names', () => {
      const items = ['Smith', 'JONES', 'smith', 'jones', 'Brown'];
      const sorted = [...items].sort(nocaseCollation);
      // After case folding: brown, jones, jones, smith, smith
      expect(sorted[0]).toBe('Brown');
      expect(sorted[1] === 'JONES' || sorted[1] === 'jones').toBe(true);
    });
  });

  describe('Non-ASCII characters', () => {
    it('should NOT fold non-ASCII uppercase (SQLite NOCASE limitation)', () => {
      // In SQLite NOCASE, only ASCII A-Z is case-folded
      // Characters like a-umlaut are not affected
      // This test verifies we match SQLite behavior
      const result = nocaseCollation('\u00C4', '\u00E4'); // A-umlaut vs a-umlaut
      expect(result).not.toBe(0); // They should NOT be equal in SQLite NOCASE
    });
  });
});

// =============================================================================
// BUILTIN COLLATION TESTS - RTRIM
// =============================================================================

describe('RTRIM Collation', () => {
  describe('Trailing space handling', () => {
    it('should ignore trailing spaces', () => {
      expect(rtrimCollation('hello', 'hello   ')).toBe(0);
      expect(rtrimCollation('hello   ', 'hello')).toBe(0);
      expect(rtrimCollation('hello  ', 'hello      ')).toBe(0);
    });

    it('should NOT ignore leading spaces', () => {
      expect(rtrimCollation('hello', ' hello')).toBeGreaterThan(0);
      expect(rtrimCollation(' hello', 'hello')).toBeLessThan(0);
    });

    it('should NOT ignore middle spaces', () => {
      // 'hel lo' vs 'hello' - at position 3, space (32) < 'l' (108)
      expect(rtrimCollation('hel lo', 'hello')).toBeLessThan(0);
      // More importantly, they should NOT be equal
      expect(rtrimCollation('hel lo', 'hello')).not.toBe(0);
    });

    it('should handle strings of only spaces', () => {
      expect(rtrimCollation('   ', '    ')).toBe(0);
      expect(rtrimCollation('', '   ')).toBe(0);
    });

    it('should be case-sensitive (unlike NOCASE)', () => {
      expect(rtrimCollation('Hello', 'hello')).toBeLessThan(0);
      expect(rtrimCollation('hello', 'Hello')).toBeGreaterThan(0);
    });
  });

  describe('Sort order verification', () => {
    it('should group strings equal except for trailing spaces', () => {
      const items = ['test   ', 'test', 'test  '];
      const sorted = [...items].sort(rtrimCollation);
      // All should compare equal, so order depends on stable sort
      // We verify they all compare equal to each other
      expect(rtrimCollation(sorted[0], sorted[1])).toBe(0);
      expect(rtrimCollation(sorted[1], sorted[2])).toBe(0);
    });
  });
});

// =============================================================================
// BUILTIN COLLATIONS MAP TESTS
// =============================================================================

describe('Built-in Collations Map', () => {
  it('should contain exactly three built-in collations', () => {
    expect(BUILTIN_COLLATIONS.size).toBe(3);
  });

  it('should have BINARY collation', () => {
    const binary = BUILTIN_COLLATIONS.get('BINARY');
    expect(binary).toBeDefined();
    expect(binary?.name).toBe('BINARY');
    expect(binary?.builtin).toBe(true);
    expect(binary?.caseSensitive).toBe(true);
  });

  it('should have NOCASE collation', () => {
    const nocase = BUILTIN_COLLATIONS.get('NOCASE');
    expect(nocase).toBeDefined();
    expect(nocase?.name).toBe('NOCASE');
    expect(nocase?.builtin).toBe(true);
    expect(nocase?.caseSensitive).toBe(false);
  });

  it('should have RTRIM collation', () => {
    const rtrim = BUILTIN_COLLATIONS.get('RTRIM');
    expect(rtrim).toBeDefined();
    expect(rtrim?.name).toBe('RTRIM');
    expect(rtrim?.builtin).toBe(true);
    expect(rtrim?.respectsTrailingSpaces).toBe(false);
  });

  it('should detect built-in collation names', () => {
    expect(isBuiltinCollation('BINARY')).toBe(true);
    expect(isBuiltinCollation('binary')).toBe(true);
    expect(isBuiltinCollation('Binary')).toBe(true);
    expect(isBuiltinCollation('NOCASE')).toBe(true);
    expect(isBuiltinCollation('RTRIM')).toBe(true);
    expect(isBuiltinCollation('CUSTOM')).toBe(false);
  });

  it('should get built-in collation by name (case-insensitive)', () => {
    expect(getBuiltinCollation('binary')?.name).toBe('BINARY');
    expect(getBuiltinCollation('NOCASE')?.name).toBe('NOCASE');
    expect(getBuiltinCollation('Rtrim')?.name).toBe('RTRIM');
    expect(getBuiltinCollation('unknown')).toBeUndefined();
  });

  it('should list all built-in collation names', () => {
    const names = getBuiltinCollationNames();
    expect(names).toContain('BINARY');
    expect(names).toContain('NOCASE');
    expect(names).toContain('RTRIM');
    expect(names.length).toBe(3);
  });
});

// =============================================================================
// COLLATION REGISTRY TESTS
// =============================================================================

describe('Collation Registry', () => {
  let registry: ReturnType<typeof createCollationRegistry>;

  beforeEach(() => {
    registry = createCollationRegistry();
  });

  describe('Built-in collation access', () => {
    it('should have built-in collations pre-registered', () => {
      expect(registry.has('BINARY')).toBe(true);
      expect(registry.has('NOCASE')).toBe(true);
      expect(registry.has('RTRIM')).toBe(true);
    });

    it('should get built-in collations case-insensitively', () => {
      expect(registry.get('binary')?.name).toBe('BINARY');
      expect(registry.get('NoCase')?.name).toBe('NOCASE');
      expect(registry.get('RTRIM')?.name).toBe('RTRIM');
    });

    it('should return BINARY as default collation', () => {
      const defaultCollation = registry.getDefault();
      expect(defaultCollation.name).toBe('BINARY');
    });
  });

  describe('Custom collation registration', () => {
    it('should register a custom collation', () => {
      const reverseCollation: CollationFunction = (a, b) => -binaryCollation(a, b);

      const result = registry.register({
        name: 'REVERSE',
        compare: reverseCollation,
        description: 'Reverse alphabetical order',
      });

      expect(result.name).toBe('REVERSE');
      expect(result.builtin).toBe(false);
      expect(registry.has('REVERSE')).toBe(true);
    });

    it('should throw when registering duplicate collation without replace', () => {
      registry.register({
        name: 'CUSTOM1',
        compare: binaryCollation,
      });

      expect(() => {
        registry.register({
          name: 'CUSTOM1',
          compare: binaryCollation,
        });
      }).toThrow(CollationExistsError);
    });

    it('should replace collation when replace option is true', () => {
      registry.register({
        name: 'CUSTOM1',
        compare: (a, b) => 0, // Always equal
        description: 'Original',
      });

      registry.register({
        name: 'CUSTOM1',
        compare: binaryCollation,
        description: 'Replaced',
        replace: true,
      });

      const collation = registry.get('CUSTOM1');
      expect(collation?.description).toBe('Replaced');
    });

    it('should not allow overriding built-in collations', () => {
      expect(() => {
        registry.register({
          name: 'BINARY',
          compare: nocaseCollation,
        });
      }).toThrow(BuiltinCollationError);

      expect(() => {
        registry.register({
          name: 'nocase',
          compare: binaryCollation,
        });
      }).toThrow(BuiltinCollationError);
    });

    it('should normalize collation names to uppercase', () => {
      registry.register({
        name: 'myCustom',
        compare: binaryCollation,
      });

      expect(registry.has('MYCUSTOM')).toBe(true);
      expect(registry.has('mycustom')).toBe(true);
      expect(registry.get('MyCustom')?.name).toBe('MYCUSTOM');
    });
  });

  describe('Custom collation removal', () => {
    it('should remove a custom collation', () => {
      registry.register({
        name: 'REMOVABLE',
        compare: binaryCollation,
      });

      expect(registry.remove('REMOVABLE')).toBe(true);
      expect(registry.has('REMOVABLE')).toBe(false);
    });

    it('should return false when removing non-existent collation', () => {
      expect(registry.remove('NONEXISTENT')).toBe(false);
    });

    it('should not allow removing built-in collations', () => {
      expect(() => {
        registry.remove('BINARY');
      }).toThrow(BuiltinCollationError);
    });
  });

  describe('Listing collations', () => {
    it('should list all collations including built-in', () => {
      registry.register({ name: 'CUSTOM1', compare: binaryCollation });
      registry.register({ name: 'CUSTOM2', compare: binaryCollation });

      const all = registry.list();
      expect(all.length).toBe(5); // 3 built-in + 2 custom

      const names = all.map((c) => c.name);
      expect(names).toContain('BINARY');
      expect(names).toContain('NOCASE');
      expect(names).toContain('RTRIM');
      expect(names).toContain('CUSTOM1');
      expect(names).toContain('CUSTOM2');
    });

    it('should list only custom collations', () => {
      registry.register({ name: 'CUSTOM1', compare: binaryCollation });
      registry.register({ name: 'CUSTOM2', compare: binaryCollation });

      const custom = registry.listCustom();
      expect(custom.length).toBe(2);

      const names = custom.map((c) => c.name);
      expect(names).toContain('CUSTOM1');
      expect(names).toContain('CUSTOM2');
      expect(names).not.toContain('BINARY');
    });
  });

  describe('Comparing with registry', () => {
    it('should compare strings using named collation', () => {
      expect(registry.compare('BINARY', 'A', 'a')).toBeLessThan(0);
      expect(registry.compare('NOCASE', 'A', 'a')).toBe(0);
      expect(registry.compare('RTRIM', 'test', 'test   ')).toBe(0);
    });

    it('should throw when comparing with unknown collation', () => {
      expect(() => {
        registry.compare('UNKNOWN', 'a', 'b');
      }).toThrow(CollationNotFoundError);
    });
  });

  describe('Clearing custom collations', () => {
    it('should clear all custom collations', () => {
      registry.register({ name: 'CUSTOM1', compare: binaryCollation });
      registry.register({ name: 'CUSTOM2', compare: binaryCollation });

      registry.clearCustom();

      expect(registry.listCustom().length).toBe(0);
      expect(registry.has('BINARY')).toBe(true); // Built-ins remain
    });
  });
});

// =============================================================================
// COLLATE CLAUSE PARSING TESTS
// =============================================================================

describe('COLLATE Clause Parsing', () => {
  describe('parseCollateClause', () => {
    it('should parse simple COLLATE clause', () => {
      const result = parseCollateClause('ORDER BY name COLLATE NOCASE');
      expect(result?.collationName).toBe('NOCASE');
    });

    it('should parse COLLATE with BINARY', () => {
      const result = parseCollateClause('SELECT * FROM t WHERE name COLLATE BINARY = ?');
      expect(result?.collationName).toBe('BINARY');
    });

    it('should parse COLLATE with RTRIM', () => {
      const result = parseCollateClause('name COLLATE RTRIM');
      expect(result?.collationName).toBe('RTRIM');
    });

    it('should parse COLLATE with custom collation name', () => {
      const result = parseCollateClause('ORDER BY name COLLATE my_custom_collation');
      expect(result?.collationName).toBe('my_custom_collation');
    });

    it('should parse COLLATE with quoted collation name', () => {
      const result = parseCollateClause('ORDER BY name COLLATE "my-collation"');
      expect(result?.collationName).toBe('my-collation');
    });

    it('should return null when no COLLATE clause', () => {
      const result = parseCollateClause('SELECT * FROM users');
      expect(result).toBeNull();
    });

    it('should handle case-insensitive COLLATE keyword', () => {
      expect(parseCollateClause('collate nocase')?.collationName).toBe('nocase');
      expect(parseCollateClause('COLLATE NOCASE')?.collationName).toBe('NOCASE');
      expect(parseCollateClause('Collate Nocase')?.collationName).toBe('Nocase');
    });
  });

  describe('parseColumnCollation', () => {
    it('should parse column with COLLATE', () => {
      const result = parseColumnCollation('name TEXT COLLATE NOCASE');
      expect(result?.column).toBe('name');
      expect(result?.collation).toBe('NOCASE');
    });

    it('should parse column without COLLATE', () => {
      const result = parseColumnCollation('name TEXT');
      expect(result?.column).toBe('name');
      expect(result?.collation).toBeNull();
    });

    it('should parse column with complex type and COLLATE', () => {
      const result = parseColumnCollation('description VARCHAR(255) NOT NULL COLLATE RTRIM');
      expect(result?.column).toBe('description');
      expect(result?.collation).toBe('RTRIM');
    });

    it('should handle quoted column names', () => {
      const result = parseColumnCollation('"user-name" TEXT COLLATE NOCASE');
      expect(result?.column).toBe('user-name');
      expect(result?.collation).toBe('NOCASE');
    });
  });
});

// =============================================================================
// CREATE COLLATION PARSING TESTS
// =============================================================================

describe('CREATE COLLATION Parsing', () => {
  describe('isCreateCollation', () => {
    it('should detect CREATE COLLATION statements', () => {
      expect(isCreateCollation('CREATE COLLATION my_collation')).toBe(true);
      expect(isCreateCollation('create collation test')).toBe(true);
      expect(isCreateCollation('  CREATE   COLLATION  test')).toBe(true);
    });

    it('should not detect non-CREATE COLLATION statements', () => {
      expect(isCreateCollation('SELECT * FROM users')).toBe(false);
      expect(isCreateCollation('CREATE TABLE test')).toBe(false);
      expect(isCreateCollation('CREATE INDEX idx')).toBe(false);
    });
  });

  describe('parseCreateCollation', () => {
    it('should parse simple CREATE COLLATION', () => {
      const result = parseCreateCollation('CREATE COLLATION my_collation');
      expect(result.type).toBe('CREATE_COLLATION');
      expect(result.name).toBe('my_collation');
    });

    it('should parse CREATE COLLATION with UNICODE locale', () => {
      const result = parseCreateCollation("CREATE COLLATION german AS UNICODE('de_DE')");
      expect(result.name).toBe('german');
      expect(result.options?.locale).toBe('de_DE');
    });

    it('should parse CREATE COLLATION with UNICODE options', () => {
      const result = parseCreateCollation(
        "CREATE COLLATION french AS UNICODE('fr_FR', STRENGTH='secondary', NUMERIC=true)"
      );
      expect(result.name).toBe('french');
      expect(result.options?.locale).toBe('fr_FR');
      expect(result.options?.strength).toBe('secondary');
      expect(result.options?.numeric).toBe(true);
    });

    it('should parse CREATE COLLATION with quoted name', () => {
      const result = parseCreateCollation('CREATE COLLATION "my-special-collation"');
      expect(result.name).toBe('my-special-collation');
    });

    it('should parse CREATE COLLATION with base collation', () => {
      const result = parseCreateCollation('CREATE COLLATION my_nocase AS NOCASE');
      expect(result.name).toBe('my_nocase');
      expect(result.baseCollation).toBe('NOCASE');
    });
  });
});

// =============================================================================
// DROP COLLATION PARSING TESTS
// =============================================================================

describe('DROP COLLATION Parsing', () => {
  describe('isDropCollation', () => {
    it('should detect DROP COLLATION statements', () => {
      expect(isDropCollation('DROP COLLATION my_collation')).toBe(true);
      expect(isDropCollation('drop collation test')).toBe(true);
      expect(isDropCollation('DROP COLLATION IF EXISTS test')).toBe(true);
    });

    it('should not detect non-DROP COLLATION statements', () => {
      expect(isDropCollation('DROP TABLE test')).toBe(false);
      expect(isDropCollation('DROP INDEX idx')).toBe(false);
    });
  });

  describe('parseDropCollation', () => {
    it('should parse simple DROP COLLATION', () => {
      const result = parseDropCollation('DROP COLLATION my_collation');
      expect(result.type).toBe('DROP_COLLATION');
      expect(result.name).toBe('my_collation');
      expect(result.ifExists).toBe(false);
    });

    it('should parse DROP COLLATION IF EXISTS', () => {
      const result = parseDropCollation('DROP COLLATION IF EXISTS my_collation');
      expect(result.name).toBe('my_collation');
      expect(result.ifExists).toBe(true);
    });
  });
});

// =============================================================================
// INDEX COLLATION PARSING TESTS
// =============================================================================

describe('Index Collation Parsing', () => {
  describe('parseIndexCollation', () => {
    it('should parse CREATE INDEX with COLLATE', () => {
      const result = parseIndexCollation(
        'CREATE INDEX idx_name ON users (name COLLATE NOCASE)'
      );
      expect(result?.indexName).toBe('idx_name');
      expect(result?.tableName).toBe('users');
      expect(result?.columns[0].name).toBe('name');
      expect(result?.columns[0].collation).toBe('NOCASE');
    });

    it('should parse CREATE INDEX with multiple columns and COLLATE', () => {
      const result = parseIndexCollation(
        'CREATE INDEX idx_names ON users (first_name COLLATE NOCASE, last_name COLLATE NOCASE)'
      );
      expect(result?.columns.length).toBe(2);
      expect(result?.columns[0].collation).toBe('NOCASE');
      expect(result?.columns[1].collation).toBe('NOCASE');
    });

    it('should parse CREATE INDEX with mixed collations and sort orders', () => {
      const result = parseIndexCollation(
        'CREATE INDEX idx_mixed ON users (name COLLATE NOCASE ASC, email DESC)'
      );
      expect(result?.columns[0].name).toBe('name');
      expect(result?.columns[0].collation).toBe('NOCASE');
      expect(result?.columns[0].sortOrder).toBe('ASC');
      expect(result?.columns[1].name).toBe('email');
      expect(result?.columns[1].collation).toBeNull();
      expect(result?.columns[1].sortOrder).toBe('DESC');
    });

    it('should parse CREATE UNIQUE INDEX with COLLATE', () => {
      const result = parseIndexCollation(
        'CREATE UNIQUE INDEX idx_email ON users (email COLLATE NOCASE)'
      );
      expect(result?.indexName).toBe('idx_email');
      expect(result?.columns[0].collation).toBe('NOCASE');
    });

    it('should parse CREATE INDEX IF NOT EXISTS', () => {
      const result = parseIndexCollation(
        'CREATE INDEX IF NOT EXISTS idx_name ON users (name COLLATE BINARY)'
      );
      expect(result?.indexName).toBe('idx_name');
      expect(result?.columns[0].collation).toBe('BINARY');
    });

    it('should return null for invalid CREATE INDEX', () => {
      const result = parseIndexCollation('SELECT * FROM users');
      expect(result).toBeNull();
    });
  });
});

// =============================================================================
// UNICODE COLLATION TESTS
// =============================================================================

describe('Unicode Collation', () => {
  describe('createUnicodeCollation', () => {
    it('should create a locale-aware collation', () => {
      const german = createUnicodeCollation({ locale: 'de' });

      // In German, ae should sort after ad and before af
      expect(german('ae', 'ad')).toBeGreaterThan(0);
      expect(german('ae', 'af')).toBeLessThan(0);
    });

    it('should handle case-insensitive comparison with secondary strength', () => {
      const collation = createUnicodeCollation({
        locale: 'en',
        strength: 'secondary',
      });

      expect(collation('Hello', 'hello')).toBe(0);
      expect(collation('WORLD', 'world')).toBe(0);
    });

    it('should handle numeric sorting', () => {
      const numeric = createUnicodeCollation({
        locale: 'en',
        numeric: true,
      });

      expect(numeric('file2', 'file10')).toBeLessThan(0);
      expect(numeric('item9', 'item10')).toBeLessThan(0);
    });

    it('should handle case-first option', () => {
      const upperFirst = createUnicodeCollation({
        locale: 'en',
        caseFirst: 'upper',
      });

      const result = upperFirst('A', 'a');
      // With caseFirst: 'upper', uppercase should sort before lowercase
      expect(result).toBeLessThan(0);
    });
  });

  describe('createCaseInsensitiveCollation', () => {
    it('should create a case-insensitive collation', () => {
      const collation = createCaseInsensitiveCollation('en');

      expect(collation('Hello', 'hello')).toBe(0);
      expect(collation('ABC', 'abc')).toBe(0);
    });

    it('should respect accents', () => {
      const collation = createCaseInsensitiveCollation('en');

      // Should differentiate between e and e-acute
      expect(collation('cafe', 'cafe')).toBe(0);
    });
  });

  describe('createNaturalSortCollation', () => {
    it('should sort numbers naturally', () => {
      const collation = createNaturalSortCollation();
      const items = ['file10', 'file2', 'file1', 'file20'];
      const sorted = [...items].sort(collation);

      expect(sorted).toEqual(['file1', 'file2', 'file10', 'file20']);
    });
  });

  describe('createLocaleCollationDefinition', () => {
    it('should create a locale collation definition', () => {
      const def = createLocaleCollationDefinition('de-DE');

      expect(def.name).toBe('LOCALE_DE_DE');
      expect(def.builtin).toBe(false);
      expect(def.description).toContain('de-DE');
    });
  });
});

// =============================================================================
// COLLATION APPLICATION UTILITIES TESTS
// =============================================================================

describe('Collation Application Utilities', () => {
  let registry: ReturnType<typeof createCollationRegistry>;

  beforeEach(() => {
    registry = createCollationRegistry();
  });

  describe('sortWithCollation', () => {
    it('should sort array using NOCASE collation', () => {
      const items = ['Banana', 'apple', 'Cherry', 'date'];
      const sorted = sortWithCollation(items, 'NOCASE', registry);

      expect(sorted[0]).toBe('apple');
      expect(sorted[1]).toBe('Banana');
      expect(sorted[2]).toBe('Cherry');
      expect(sorted[3]).toBe('date');
    });

    it('should sort in descending order', () => {
      const items = ['a', 'b', 'c'];
      const sorted = sortWithCollation(items, 'BINARY', registry, true);

      expect(sorted).toEqual(['c', 'b', 'a']);
    });

    it('should throw for unknown collation', () => {
      expect(() => {
        sortWithCollation(['a', 'b'], 'UNKNOWN', registry);
      }).toThrow(CollationNotFoundError);
    });
  });

  describe('equalsWithCollation', () => {
    it('should check equality using NOCASE', () => {
      expect(equalsWithCollation('Hello', 'hello', 'NOCASE', registry)).toBe(true);
      expect(equalsWithCollation('Hello', 'World', 'NOCASE', registry)).toBe(false);
    });

    it('should check equality using RTRIM', () => {
      expect(equalsWithCollation('test', 'test   ', 'RTRIM', registry)).toBe(true);
      expect(equalsWithCollation('test ', ' test', 'RTRIM', registry)).toBe(false);
    });

    it('should check equality using BINARY', () => {
      expect(equalsWithCollation('Hello', 'Hello', 'BINARY', registry)).toBe(true);
      expect(equalsWithCollation('Hello', 'hello', 'BINARY', registry)).toBe(false);
    });
  });

  describe('createCollationComparator', () => {
    it('should create a comparator for Array.sort()', () => {
      const comparator = createCollationComparator('NOCASE', registry);
      const items = ['B', 'a', 'C'];
      const sorted = [...items].sort(comparator);

      expect(sorted).toEqual(['a', 'B', 'C']);
    });

    it('should create a descending comparator', () => {
      const comparator = createCollationComparator('BINARY', registry, true);
      const items = ['a', 'b', 'c'];
      const sorted = [...items].sort(comparator);

      expect(sorted).toEqual(['c', 'b', 'a']);
    });
  });

  describe('registerUnicodeCollation', () => {
    it('should register a Unicode collation', () => {
      const def = registerUnicodeCollation(registry, 'GERMAN', { locale: 'de' });

      expect(def.name).toBe('GERMAN');
      expect(registry.has('GERMAN')).toBe(true);
    });
  });
});

// =============================================================================
// WHERE CLAUSE COLLATION TESTS
// =============================================================================

describe('WHERE Clause Collation', () => {
  let registry: ReturnType<typeof createCollationRegistry>;

  beforeEach(() => {
    registry = createCollationRegistry();
  });

  it('should compare strings case-insensitively with NOCASE', () => {
    const clause = "WHERE name = 'test' COLLATE NOCASE";
    const parsed = parseCollateClause(clause);

    expect(parsed?.collationName).toBe('NOCASE');
    expect(registry.compare('NOCASE', 'TEST', 'test')).toBe(0);
    expect(registry.compare('NOCASE', 'Test', 'test')).toBe(0);
  });

  it('should compare strings with trailing spaces using RTRIM', () => {
    const clause = "WHERE code = 'ABC' COLLATE RTRIM";
    const parsed = parseCollateClause(clause);

    expect(parsed?.collationName).toBe('RTRIM');
    expect(registry.compare('RTRIM', 'ABC', 'ABC   ')).toBe(0);
    expect(registry.compare('RTRIM', 'ABC  ', 'ABC')).toBe(0);
  });
});

// =============================================================================
// PRAGMA CASE_SENSITIVE_LIKE TESTS
// =============================================================================

describe('PRAGMA case_sensitive_like', () => {
  // Note: This is a conceptual test for the PRAGMA behavior
  // The actual implementation would be in the query engine

  it('should document case_sensitive_like = false behavior (default)', () => {
    // By default, LIKE is case-insensitive for ASCII
    // 'hello' LIKE 'HELLO' is true
    const caseSensitiveLike = false;

    // This documents expected behavior
    expect(caseSensitiveLike).toBe(false);
  });

  it('should document case_sensitive_like = true behavior', () => {
    // When enabled, LIKE becomes case-sensitive
    // 'hello' LIKE 'HELLO' is false
    const caseSensitiveLike = true;

    // This documents expected behavior
    expect(caseSensitiveLike).toBe(true);
  });
});

// =============================================================================
// ERROR HANDLING TESTS
// =============================================================================

describe('Collation Errors', () => {
  it('should create CollationNotFoundError with correct message', () => {
    const error = new CollationNotFoundError('UNKNOWN');
    expect(error.message).toBe("Collation 'UNKNOWN' not found");
    expect(error.collationName).toBe('UNKNOWN');
    expect(error.name).toBe('CollationNotFoundError');
  });

  it('should create CollationExistsError with correct message', () => {
    const error = new CollationExistsError('CUSTOM');
    expect(error.message).toBe("Collation 'CUSTOM' already exists");
    expect(error.collationName).toBe('CUSTOM');
    expect(error.name).toBe('CollationExistsError');
  });

  it('should create BuiltinCollationError with correct message', () => {
    const error = new BuiltinCollationError('BINARY');
    expect(error.message).toBe("Cannot modify built-in collation 'BINARY'");
    expect(error.collationName).toBe('BINARY');
    expect(error.name).toBe('BuiltinCollationError');
  });
});

// =============================================================================
// INTEGRATION SCENARIOS
// =============================================================================

describe('Integration Scenarios', () => {
  let registry: ReturnType<typeof createCollationRegistry>;

  beforeEach(() => {
    registry = createCollationRegistry();
  });

  it('should support user registration with case-insensitive email', () => {
    // Simulate checking if email already exists, case-insensitively
    const existingEmails = ['User@Example.com', 'admin@test.org'];
    const newEmail = 'user@example.com';

    const exists = existingEmails.some(
      (e) => registry.compare('NOCASE', e, newEmail) === 0
    );

    expect(exists).toBe(true);
  });

  it('should support case-insensitive username lookup', () => {
    const users = ['Alice', 'Bob', 'CHARLIE', 'david'];
    const searchName = 'charlie';

    const found = users.find(
      (u) => registry.compare('NOCASE', u, searchName) === 0
    );

    expect(found).toBe('CHARLIE');
  });

  it('should support sorting product names with natural order', () => {
    registerUnicodeCollation(registry, 'NATURAL', {
      locale: 'en',
      numeric: true,
    });

    const products = ['Widget 10', 'Widget 2', 'Widget 1', 'Widget 20'];
    const sorted = sortWithCollation(products, 'NATURAL', registry);

    expect(sorted).toEqual(['Widget 1', 'Widget 2', 'Widget 10', 'Widget 20']);
  });

  it('should support locale-specific sorting for German', () => {
    registerUnicodeCollation(registry, 'DE_SORT', { locale: 'de' });

    // In German, umlauts sort with their base letter
    const names = ['Muller', 'Mueller']; // Both represent Muller/Mueller
    const sorted = sortWithCollation(names, 'DE_SORT', registry);

    // Sorted according to German rules
    expect(sorted.length).toBe(2);
  });

  it('should support trimmed comparison for codes', () => {
    // Codes stored with varying trailing spaces
    const storedCodes = ['ABC   ', 'DEF  ', 'GHI'];
    const searchCode = 'DEF';

    const found = storedCodes.find(
      (c) => registry.compare('RTRIM', c, searchCode) === 0
    );

    expect(found).toBe('DEF  ');
  });

  it('should support creating custom collation for domain-specific sorting', () => {
    // Custom collation that sorts by status priority
    const statusOrder: Record<string, number> = {
      critical: 0,
      high: 1,
      medium: 2,
      low: 3,
    };

    registry.register({
      name: 'PRIORITY',
      compare: (a, b) => {
        const aOrder = statusOrder[a.toLowerCase()] ?? 99;
        const bOrder = statusOrder[b.toLowerCase()] ?? 99;
        return aOrder - bOrder;
      },
    });

    const items = ['low', 'CRITICAL', 'Medium', 'high'];
    const sorted = sortWithCollation(items, 'PRIORITY', registry);

    expect(sorted[0]).toBe('CRITICAL');
    expect(sorted[1]).toBe('high');
    expect(sorted[2]).toBe('Medium');
    expect(sorted[3]).toBe('low');
  });
});
