/**
 * RED Phase TDD Tests for Named Parameter Binding Edge Cases
 *
 * Issue: sql-zhy.21
 *
 * These tests document edge cases for named parameter binding. Tests using
 * `it.fails()` indicate functionality that is NOT yet implemented and documents
 * expected behavior for the GREEN phase.
 *
 * Edge cases covered:
 * 1. Parameters inside string literals should NOT be extracted (IMPLEMENTED)
 * 2. Escaped parameter markers (::escaped) - PostgreSQL type cast syntax (NOT IMPLEMENTED)
 * 3. Parameters in comments should be ignored (IMPLEMENTED)
 * 4. Mixed positional and named parameters - validation (NOT IMPLEMENTED)
 * 5. Duplicate named parameters (should use same value) (IMPLEMENTED)
 * 6. Missing named parameter error (IMPLEMENTED)
 */

import { describe, it, expect } from 'vitest';

import {
  parseParameters,
  bindParameters,
  validateParameters,
  BindingError,
} from '../binding.js';

// =============================================================================
// EDGE CASE 1: PARAMETERS INSIDE STRING LITERALS
// Status: IMPLEMENTED - These tests verify existing working functionality
// =============================================================================

describe('Named Parameters - String Literals', () => {
  describe('parseParameters', () => {
    it('should NOT extract parameters inside single-quoted string literals', () => {
      // This test verifies that :not_a_param inside a string is not treated as a parameter
      const result = parseParameters(
        "SELECT * FROM users WHERE name = ':not_a_param'"
      );

      // Expected: No parameters should be found
      expect(result.tokens).toHaveLength(0);
      expect(result.hasNamedParameters).toBe(false);
      expect(result.namedParameterNames.size).toBe(0);
    });

    it('should NOT extract parameters inside double-quoted string literals', () => {
      const result = parseParameters(
        'SELECT * FROM users WHERE name = ":not_a_param"'
      );

      expect(result.tokens).toHaveLength(0);
      expect(result.hasNamedParameters).toBe(false);
    });

    it('should correctly distinguish real parameters from string-embedded ones', () => {
      const result = parseParameters(
        "SELECT * FROM users WHERE name = ':fake' AND id = :real_param"
      );

      expect(result.tokens).toHaveLength(1);
      expect(result.tokens[0]).toMatchObject({
        type: 'named',
        key: 'real_param',
      });
      expect(result.namedParameterNames.has('fake')).toBe(false);
      expect(result.namedParameterNames.has('real_param')).toBe(true);
    });

    it('should handle string with escaped quotes containing parameter-like text', () => {
      const result = parseParameters(
        "SELECT * FROM users WHERE bio = 'User''s :tag is cool' AND id = :id"
      );

      expect(result.tokens).toHaveLength(1);
      expect(result.tokens[0].key).toBe('id');
    });
  });
});

// =============================================================================
// EDGE CASE 2: ESCAPED PARAMETER MARKERS (PostgreSQL :: Type Cast)
// Status: NOT IMPLEMENTED - Parser incorrectly extracts identifier after ::
// =============================================================================

describe('Named Parameters - Escaped Markers', () => {
  describe('parseParameters', () => {
    // This test passes because :: is inside a string literal
    it('should treat :: inside string as literal (already handled by string parsing)', () => {
      const result = parseParameters(
        "SELECT * FROM users WHERE data LIKE '::escaped'"
      );

      // Inside a string, so no parameters - this works
      expect(result.tokens).toHaveLength(0);
    });

    it.fails('should treat ::identifier outside strings as type cast, not parameter', () => {
      // PostgreSQL uses :: for type casting: value::text
      // Current implementation INCORRECTLY parses "text" and "date" as named parameters
      // This should NOT be parsed as a named parameter
      const result = parseParameters(
        'SELECT created_at::date FROM users WHERE id = :id'
      );

      // Should only find :id, not "date" from ::date
      // CURRENT BUG: Parser finds both :date and :id
      expect(result.tokens).toHaveLength(1);
      expect(result.tokens[0].key).toBe('id');
      expect(result.namedParameterNames.has('date')).toBe(false);
    });

    it.fails('should handle multiple :: type casts in a query', () => {
      // CURRENT BUG: Parser extracts 'text', 'timestamp', and 'name' as parameters
      const result = parseParameters(
        'SELECT id::text, created_at::timestamp FROM users WHERE name = :name'
      );

      expect(result.tokens).toHaveLength(1);
      expect(result.tokens[0].key).toBe('name');
      // Should NOT have extracted 'text' or 'timestamp' as parameters
      expect(result.namedParameterNames.has('text')).toBe(false);
      expect(result.namedParameterNames.has('timestamp')).toBe(false);
    });

    it.fails('should handle ::jsonb JSON type cast', () => {
      // PostgreSQL JSON operators should not be confused with named parameters
      // CURRENT BUG: Parser extracts 'jsonb' as a named parameter
      const result = parseParameters(
        "SELECT data->>'name' FROM users WHERE data::jsonb ? :key"
      );

      expect(result.tokens).toHaveLength(1);
      expect(result.tokens[0].key).toBe('key');
      expect(result.namedParameterNames.has('jsonb')).toBe(false);
    });
  });
});

// =============================================================================
// EDGE CASE 3: PARAMETERS IN COMMENTS
// Status: IMPLEMENTED - These tests verify existing working functionality
// =============================================================================

describe('Named Parameters - Comments', () => {
  describe('parseParameters', () => {
    it('should NOT extract parameters from single-line comments', () => {
      const sql = `SELECT * FROM users -- WHERE id = :in_comment
WHERE active = :real_param`;

      const result = parseParameters(sql);

      expect(result.tokens).toHaveLength(1);
      expect(result.tokens[0].key).toBe('real_param');
      expect(result.namedParameterNames.has('in_comment')).toBe(false);
    });

    it('should NOT extract parameters from block comments', () => {
      const sql = `SELECT * FROM users /* WHERE id = :in_block_comment */ WHERE active = :real_param`;

      const result = parseParameters(sql);

      expect(result.tokens).toHaveLength(1);
      expect(result.tokens[0].key).toBe('real_param');
      expect(result.namedParameterNames.has('in_block_comment')).toBe(false);
    });

    it('should NOT extract parameters from multi-line block comments', () => {
      const sql = `SELECT * FROM users
/*
 * This is a comment block
 * :commented_param should not be extracted
 */
WHERE id = :actual_id`;

      const result = parseParameters(sql);

      expect(result.tokens).toHaveLength(1);
      expect(result.tokens[0].key).toBe('actual_id');
      expect(result.namedParameterNames.has('commented_param')).toBe(false);
    });

    it('should handle comment at end of line with parameter', () => {
      const sql = `SELECT * FROM users WHERE id = :id -- filter by id`;

      const result = parseParameters(sql);

      expect(result.tokens).toHaveLength(1);
      expect(result.tokens[0].key).toBe('id');
    });
  });
});

// =============================================================================
// EDGE CASE 4: MIXED POSITIONAL AND NAMED PARAMETERS
// Status: NOT IMPLEMENTED - Parser allows mixing but validation is unclear
// =============================================================================

describe('Named Parameters - Mixed Positional and Named', () => {
  describe('parseParameters', () => {
    it.fails('should throw or warn when mixing positional and named parameters', () => {
      // Most SQL engines do not support mixing positional (?) and named (:name) parameters
      // The parser should either:
      // 1. Throw an error during parsing, OR
      // 2. Set a flag indicating mixed usage for validation to reject
      const sql = 'SELECT * FROM users WHERE id = ? AND name = :name';

      // Option: Throw an error during parsing
      expect(() => parseParameters(sql)).toThrow(/mix/i);
    });
  });

  describe('validateParameters', () => {
    it.fails('should reject mixed ? and :name in same query via validation', () => {
      // Alternative approach: Parse successfully but validateParameters should fail
      const parsed = parseParameters('SELECT * FROM users WHERE id = ? AND name = :name');

      // The parsed result would show mixed parameter types
      const hasPositional = parsed.tokens.some(t => t.type === 'positional');
      const hasNamed = parsed.hasNamedParameters;

      // Should have detected the mixing
      expect(hasPositional && hasNamed).toBe(true);

      // Validation SHOULD fail for mixed parameters - but it doesn't currently
      expect(() => validateParameters(parsed, 1)).toThrow(/mix/i);
    });
  });

  describe('bindParameters', () => {
    it.fails('should provide clear error message for mixed parameter styles', () => {
      const parsed = parseParameters('SELECT * FROM users WHERE id = ? AND name = :name');

      // Should throw with a descriptive error about mixing styles
      // CURRENT BUG: Throws generic "named parameter expected" instead of explaining the mixing issue
      expect(() => bindParameters(parsed, 1)).toThrow(/mix.*positional.*named/i);
    });
  });
});

// =============================================================================
// EDGE CASE 5: DUPLICATE NAMED PARAMETERS
// Status: IMPLEMENTED - These tests verify existing working functionality
// =============================================================================

describe('Named Parameters - Duplicates', () => {
  describe('parseParameters', () => {
    it('should detect duplicate named parameters in SQL', () => {
      const sql = 'SELECT * FROM users WHERE created_at > :date OR updated_at > :date';

      const result = parseParameters(sql);

      // Should find both occurrences in tokens
      expect(result.tokens).toHaveLength(2);
      expect(result.tokens[0].key).toBe('date');
      expect(result.tokens[1].key).toBe('date');

      // namedParameterNames should be a Set with unique names
      expect(result.namedParameterNames.size).toBe(1);
      expect(result.namedParameterNames.has('date')).toBe(true);
    });
  });

  describe('bindParameters', () => {
    it('should use the same value for duplicate named parameters', () => {
      const parsed = parseParameters(
        'SELECT * FROM users WHERE created_at > :date OR updated_at > :date'
      );

      const dateValue = '2024-01-01';
      const values = bindParameters(parsed, { date: dateValue });

      // Both positions should have the same value
      expect(values).toHaveLength(2);
      expect(values[0]).toBe(dateValue);
      expect(values[1]).toBe(dateValue);
    });

    it('should handle multiple different duplicated parameters', () => {
      const parsed = parseParameters(
        'SELECT * FROM audit WHERE actor = :user OR target = :user AND time > :date OR logged_at > :date'
      );

      const values = bindParameters(parsed, { user: 'alice', date: '2024-01-01' });

      expect(values).toHaveLength(4);
      expect(values[0]).toBe('alice'); // first :user
      expect(values[1]).toBe('alice'); // second :user
      expect(values[2]).toBe('2024-01-01'); // first :date
      expect(values[3]).toBe('2024-01-01'); // second :date
    });
  });
});

// =============================================================================
// EDGE CASE 6: MISSING NAMED PARAMETER ERROR
// Status: IMPLEMENTED - These tests verify existing working functionality
// =============================================================================

describe('Named Parameters - Missing Parameter Errors', () => {
  describe('validateParameters', () => {
    it('should throw BindingError for missing named parameter', () => {
      const parsed = parseParameters('SELECT * FROM users WHERE id = :missing');

      expect(() => validateParameters(parsed, { other: 1 })).toThrow(BindingError);
    });

    it('should include the missing parameter name in error message', () => {
      const parsed = parseParameters('SELECT * FROM users WHERE id = :missing');

      try {
        validateParameters(parsed, { other: 1 });
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(BindingError);
        expect((error as Error).message).toContain('missing');
      }
    });

    it('should list all missing parameters when multiple are missing', () => {
      const parsed = parseParameters(
        'SELECT * FROM users WHERE id = :id AND name = :name AND email = :email'
      );

      try {
        validateParameters(parsed, { id: 1 }); // missing name and email
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(BindingError);
        const message = (error as Error).message;
        // Error should mention at least one missing parameter
        expect(message.match(/name|email/)).toBeTruthy();
      }
    });
  });

  describe('bindParameters', () => {
    it('should throw BindingError when binding with missing named parameter', () => {
      const parsed = parseParameters('SELECT * FROM users WHERE id = :missing');

      expect(() => bindParameters(parsed, { other: 1 })).toThrow(BindingError);
    });

    it.fails('should provide helpful error when passing array instead of object for named params', () => {
      // Common mistake: passing array when named params are expected
      const parsed = parseParameters('SELECT * FROM users WHERE id = :id');

      // Current behavior throws a generic error
      // Should provide a clear error about parameter type mismatch
      expect(() => bindParameters(parsed, [1])).toThrow(/named.*parameter.*object/i);
    });
  });
});

// =============================================================================
// ADDITIONAL EDGE CASES
// Status: Mixed - some work, some need implementation
// =============================================================================

describe('Named Parameters - Additional Edge Cases', () => {
  describe('parseParameters', () => {
    // This actually works - the implementation handles underscores
    it('should handle parameter names with underscores correctly', () => {
      const result = parseParameters('SELECT * FROM users WHERE user_id = :user_id');

      expect(result.tokens).toHaveLength(1);
      expect(result.tokens[0].key).toBe('user_id');
    });

    // This also works - parameters can be adjacent to operators
    it('should handle parameter immediately after operator without space', () => {
      const result = parseParameters('SELECT * FROM users WHERE id=:id');

      expect(result.tokens).toHaveLength(1);
      expect(result.tokens[0].key).toBe('id');
    });

    // This works because the URL is inside a string literal
    it('should NOT extract :// as a parameter when inside string (URL-like patterns)', () => {
      const result = parseParameters(
        "SELECT * FROM urls WHERE url = 'https://example.com' AND id = :id"
      );

      expect(result.tokens).toHaveLength(1);
      expect(result.tokens[0].key).toBe('id');
    });

    // This actually works - :// is not parsed because / is not a valid identifier char
    it('should NOT extract :// as a parameter when outside string', () => {
      // Edge case: :// appears outside a string
      // The parser correctly recognizes / is not a valid identifier start char
      const result = parseParameters(
        'SELECT url FROM urls WHERE scheme = :// AND id = :id'
      );

      // :// is not parsed as a parameter (/ is not a valid identifier char)
      // Only :id is found
      expect(result.tokens).toHaveLength(1);
      expect(result.tokens[0].key).toBe('id');
    });

    // This works - parameters can be at start of SQL
    it('should handle parameters at start of SQL', () => {
      // Edge case: parameter is the very first thing in the SQL
      const result = parseParameters(':table_name');

      expect(result.tokens).toHaveLength(1);
      expect(result.tokens[0].key).toBe('table_name');
    });

    // This works - consecutive parameters are parsed separately
    it('should handle consecutive parameters without space', () => {
      // Edge case: two parameters back-to-back :a:b
      // Parser correctly identifies both :a and :b as separate parameters
      const result = parseParameters('SELECT * FROM users WHERE id IN (:a:b)');

      expect(result.tokens).toHaveLength(2);
      expect(result.tokens[0].key).toBe('a');
      expect(result.tokens[1].key).toBe('b');
    });
  });
});
