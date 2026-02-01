/**
 * SQL Expression Evaluation Tests
 *
 * Tests for the safe recursive descent SQL expression evaluator
 * used in trigger WHEN clauses and other conditional logic.
 */

import { describe, it, expect } from 'vitest';
import {
  evaluateWhenClause,
  sqlEquals,
  sqlLike,
  sqlTruthy,
} from '../sql-eval.js';

// =============================================================================
// evaluateWhenClause - COMPARISON OPERATORS
// =============================================================================

describe('evaluateWhenClause', () => {
  describe('empty/null clauses', () => {
    it('should return true for empty when clause', () => {
      expect(evaluateWhenClause('', undefined, undefined)).toBe(true);
    });
  });

  describe('equality operators', () => {
    it('should evaluate NEW.column = value', () => {
      expect(evaluateWhenClause(
        "NEW.status = 'active'",
        undefined,
        { status: 'active' }
      )).toBe(true);
    });

    it('should evaluate NEW.column != OLD.column', () => {
      expect(evaluateWhenClause(
        'NEW.email != OLD.email',
        { email: 'old@example.com' },
        { email: 'new@example.com' }
      )).toBe(true);
    });

    it('should evaluate <> operator', () => {
      expect(evaluateWhenClause(
        "NEW.status <> 'inactive'",
        undefined,
        { status: 'active' }
      )).toBe(true);
    });

    it('should handle numeric equality', () => {
      expect(evaluateWhenClause(
        'NEW.age = 25',
        undefined,
        { age: 25 }
      )).toBe(true);
    });

    it('should handle numeric coercion in equality', () => {
      expect(evaluateWhenClause(
        "NEW.age = '25'",
        undefined,
        { age: 25 }
      )).toBe(true);
    });
  });

  describe('comparison operators', () => {
    it('should evaluate > operator', () => {
      expect(evaluateWhenClause(
        'NEW.age > 18',
        undefined,
        { age: 25 }
      )).toBe(true);

      expect(evaluateWhenClause(
        'NEW.age > 30',
        undefined,
        { age: 25 }
      )).toBe(false);
    });

    it('should evaluate < operator', () => {
      expect(evaluateWhenClause(
        'NEW.price < 100',
        undefined,
        { price: 50 }
      )).toBe(true);
    });

    it('should evaluate >= operator', () => {
      expect(evaluateWhenClause(
        'NEW.score >= 90',
        undefined,
        { score: 90 }
      )).toBe(true);

      expect(evaluateWhenClause(
        'NEW.score >= 90',
        undefined,
        { score: 89 }
      )).toBe(false);
    });

    it('should evaluate <= operator', () => {
      expect(evaluateWhenClause(
        'NEW.score <= 100',
        undefined,
        { score: 100 }
      )).toBe(true);
    });
  });

  describe('logical operators', () => {
    it('should evaluate AND', () => {
      expect(evaluateWhenClause(
        "NEW.status = 'active' AND NEW.age > 18",
        undefined,
        { status: 'active', age: 25 }
      )).toBe(true);

      expect(evaluateWhenClause(
        "NEW.status = 'active' AND NEW.age > 30",
        undefined,
        { status: 'active', age: 25 }
      )).toBe(false);
    });

    it('should evaluate OR', () => {
      expect(evaluateWhenClause(
        "NEW.status = 'active' OR NEW.status = 'pending'",
        undefined,
        { status: 'pending' }
      )).toBe(true);
    });

    it('should evaluate NOT', () => {
      expect(evaluateWhenClause(
        "NOT NEW.status = 'deleted'",
        undefined,
        { status: 'active' }
      )).toBe(true);
    });

    it('should respect operator precedence (AND before OR)', () => {
      // "a OR b AND c" should be "a OR (b AND c)"
      expect(evaluateWhenClause(
        "NEW.x = 1 OR NEW.y = 2 AND NEW.z = 3",
        undefined,
        { x: 1, y: 0, z: 0 }
      )).toBe(true);
    });
  });

  describe('NULL handling', () => {
    it('should evaluate IS NULL', () => {
      expect(evaluateWhenClause(
        'NEW.email IS NULL',
        undefined,
        { email: null }
      )).toBe(true);

      expect(evaluateWhenClause(
        'NEW.email IS NULL',
        undefined,
        { email: 'test@example.com' }
      )).toBe(false);
    });

    it('should evaluate IS NOT NULL', () => {
      expect(evaluateWhenClause(
        'NEW.email IS NOT NULL',
        undefined,
        { email: 'test@example.com' }
      )).toBe(true);
    });

    it('should treat undefined as null', () => {
      expect(evaluateWhenClause(
        'NEW.missing IS NULL',
        undefined,
        {}
      )).toBe(true);
    });
  });

  describe('LIKE operator', () => {
    it('should evaluate LIKE with % wildcard', () => {
      expect(evaluateWhenClause(
        "NEW.email LIKE '%@%'",
        undefined,
        { email: 'test@example.com' }
      )).toBe(true);
    });

    it('should evaluate LIKE with prefix match', () => {
      expect(evaluateWhenClause(
        "NEW.name LIKE 'Al%'",
        undefined,
        { name: 'Alice' }
      )).toBe(true);
    });

    it('should evaluate LIKE with _ wildcard', () => {
      expect(evaluateWhenClause(
        "NEW.code LIKE 'A_C'",
        undefined,
        { code: 'ABC' }
      )).toBe(true);

      expect(evaluateWhenClause(
        "NEW.code LIKE 'A_C'",
        undefined,
        { code: 'ABBC' }
      )).toBe(false);
    });

    it('should evaluate NOT LIKE', () => {
      expect(evaluateWhenClause(
        "NEW.email NOT LIKE '%@spam.com'",
        undefined,
        { email: 'test@example.com' }
      )).toBe(true);
    });
  });

  describe('IN operator', () => {
    it('should evaluate IN with string values', () => {
      expect(evaluateWhenClause(
        "NEW.status IN ('active', 'pending')",
        undefined,
        { status: 'active' }
      )).toBe(true);

      expect(evaluateWhenClause(
        "NEW.status IN ('active', 'pending')",
        undefined,
        { status: 'deleted' }
      )).toBe(false);
    });

    it('should evaluate IN with numeric values', () => {
      expect(evaluateWhenClause(
        'NEW.id IN (1, 2, 3)',
        undefined,
        { id: 2 }
      )).toBe(true);
    });

    it('should evaluate NOT IN', () => {
      expect(evaluateWhenClause(
        "NEW.status NOT IN ('deleted', 'archived')",
        undefined,
        { status: 'active' }
      )).toBe(true);
    });
  });

  describe('parenthesized expressions', () => {
    it('should handle parenthesized sub-expressions', () => {
      expect(evaluateWhenClause(
        "(NEW.a = 1 OR NEW.b = 2) AND NEW.c = 3",
        undefined,
        { a: 0, b: 2, c: 3 }
      )).toBe(true);

      expect(evaluateWhenClause(
        "(NEW.a = 1 OR NEW.b = 2) AND NEW.c = 3",
        undefined,
        { a: 0, b: 2, c: 0 }
      )).toBe(false);
    });
  });

  describe('literal values', () => {
    it('should handle TRUE and FALSE literals', () => {
      expect(evaluateWhenClause(
        'NEW.active = TRUE',
        undefined,
        { active: true }
      )).toBe(true);

      expect(evaluateWhenClause(
        'NEW.active = FALSE',
        undefined,
        { active: false }
      )).toBe(true);
    });

    it('should handle NULL literal in comparison', () => {
      expect(evaluateWhenClause(
        'NEW.value = NULL',
        undefined,
        { value: null }
      )).toBe(true);
    });
  });

  describe('OLD row references', () => {
    it('should access OLD row values', () => {
      expect(evaluateWhenClause(
        'OLD.status != NEW.status',
        { status: 'pending' },
        { status: 'active' }
      )).toBe(true);
    });

    it('should return null when OLD row is undefined', () => {
      expect(evaluateWhenClause(
        'OLD.name IS NULL',
        undefined,
        { name: 'Alice' }
      )).toBe(true);
    });
  });

  describe('error handling', () => {
    it('should return true on evaluation error (fire trigger by default)', () => {
      // This should fail to parse but return true as default
      expect(evaluateWhenClause(
        '%%% invalid expression &&&',
        undefined,
        {}
      )).toBe(true);
    });
  });
});

// =============================================================================
// sqlEquals
// =============================================================================

describe('sqlEquals', () => {
  it('should return true for equal values', () => {
    expect(sqlEquals(1, 1)).toBe(true);
    expect(sqlEquals('hello', 'hello')).toBe(true);
    expect(sqlEquals(null, null)).toBe(true);
  });

  it('should return false for null vs non-null', () => {
    expect(sqlEquals(null, 1)).toBe(false);
    expect(sqlEquals(1, null)).toBe(false);
  });

  it('should handle number-string coercion', () => {
    expect(sqlEquals(42, '42')).toBe(true);
    expect(sqlEquals('42', 42)).toBe(true);
  });

  it('should return false for incompatible types', () => {
    expect(sqlEquals(true, 'true')).toBe(false);
  });

  it('should return false for different values', () => {
    expect(sqlEquals(1, 2)).toBe(false);
    expect(sqlEquals('a', 'b')).toBe(false);
  });
});

// =============================================================================
// sqlLike
// =============================================================================

describe('sqlLike', () => {
  it('should match with % wildcard', () => {
    expect(sqlLike('hello world', '%world')).toBe(true);
    expect(sqlLike('hello world', 'hello%')).toBe(true);
    expect(sqlLike('hello world', '%lo wo%')).toBe(true);
  });

  it('should match with _ wildcard', () => {
    expect(sqlLike('abc', 'a_c')).toBe(true);
    expect(sqlLike('aXc', 'a_c')).toBe(true);
    expect(sqlLike('abbc', 'a_c')).toBe(false);
  });

  it('should match exact strings without wildcards', () => {
    expect(sqlLike('hello', 'hello')).toBe(true);
    expect(sqlLike('hello', 'world')).toBe(false);
  });

  it('should be case insensitive', () => {
    expect(sqlLike('Hello', 'hello')).toBe(true);
    expect(sqlLike('HELLO', '%ello')).toBe(true);
  });

  it('should handle % matching empty string', () => {
    expect(sqlLike('hello', '%hello')).toBe(true);
    expect(sqlLike('hello', 'hello%')).toBe(true);
  });

  it('should handle regex special characters in pattern', () => {
    expect(sqlLike('test.value', 'test.%')).toBe(true);
    expect(sqlLike('test[1]', 'test[%')).toBe(true);
  });
});

// =============================================================================
// sqlTruthy
// =============================================================================

describe('sqlTruthy', () => {
  it('should return false for null and undefined', () => {
    expect(sqlTruthy(null)).toBe(false);
    expect(sqlTruthy(undefined)).toBe(false);
  });

  it('should handle booleans', () => {
    expect(sqlTruthy(true)).toBe(true);
    expect(sqlTruthy(false)).toBe(false);
  });

  it('should handle numbers (0 is false)', () => {
    expect(sqlTruthy(0)).toBe(false);
    expect(sqlTruthy(1)).toBe(true);
    expect(sqlTruthy(-1)).toBe(true);
  });

  it('should handle strings (empty is falsy)', () => {
    expect(sqlTruthy('')).toBe(false);
    expect(sqlTruthy('hello')).toBe(true);
  });

  it('should return true for objects', () => {
    expect(sqlTruthy({})).toBe(true);
    expect(sqlTruthy([])).toBe(true);
  });
});
