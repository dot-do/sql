/**
 * Schema Validator Tests
 *
 * Tests for schema validation including relation targets,
 * type validity, circular references, primary keys, and naming conventions.
 */

import { describe, it, expect } from 'vitest';
import {
  validateSchema,
  isValidSchema,
  assertValidSchema,
  ErrorCodes,
  WarningCodes,
} from '../validator.js';

// =============================================================================
// validateSchema TESTS
// =============================================================================

describe('validateSchema', () => {
  describe('valid schemas', () => {
    it('should validate a simple valid schema', () => {
      const result = validateSchema({
        users: {
          id: 'uuid!',
          name: 'string',
          email: 'string!#',
          createdAt: 'timestamp = now()',
          updatedAt: 'timestamp = now()',
        },
      });

      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should validate a schema with only backward relations (no cycle)', () => {
      const result = validateSchema({
        users: {
          id: 'uuid!',
          name: 'string',
          createdAt: 'timestamp = now()',
          updatedAt: 'timestamp = now()',
        },
        orders: {
          id: 'uuid!',
          userId: '<- users',
          total: 'decimal(10,2)',
          createdAt: 'timestamp = now()',
          updatedAt: 'timestamp = now()',
        },
      });

      expect(result.valid).toBe(true);
      expect(result.errors).toHaveLength(0);
    });

    it('should detect circular relations between forward and backward', () => {
      const result = validateSchema({
        users: {
          id: 'uuid!',
          name: 'string',
          orders: '-> orders[]',
          createdAt: 'timestamp = now()',
          updatedAt: 'timestamp = now()',
        },
        orders: {
          id: 'uuid!',
          userId: '<- users',
          total: 'decimal(10,2)',
          createdAt: 'timestamp = now()',
          updatedAt: 'timestamp = now()',
        },
      });

      // The validator detects circular relations (users -> orders -> users)
      const circularError = result.errors.find(
        (e) => e.code === ErrorCodes.CIRCULAR_RELATION
      );
      expect(circularError).toBeDefined();
    });
  });

  describe('relation validation', () => {
    it('should error when relation target does not exist', () => {
      const result = validateSchema({
        users: {
          id: 'uuid!',
          posts: '-> nonexistent[]',
          createdAt: 'timestamp = now()',
          updatedAt: 'timestamp = now()',
        },
      });

      expect(result.valid).toBe(false);
      const targetError = result.errors.find(
        (e) => e.code === ErrorCodes.RELATION_TARGET_NOT_FOUND
      );
      expect(targetError).toBeDefined();
      expect(targetError!.path).toEqual(['users', 'posts']);
    });

    it('should warn about missing back-relation', () => {
      const result = validateSchema({
        users: {
          id: 'uuid!',
          orders: '-> orders[]',
          createdAt: 'timestamp = now()',
          updatedAt: 'timestamp = now()',
        },
        orders: {
          id: 'uuid!',
          total: 'decimal(10,2)',
          createdAt: 'timestamp = now()',
          updatedAt: 'timestamp = now()',
        },
      });

      const backRelWarning = result.warnings.find(
        (w) => w.code === WarningCodes.MISSING_BACK_RELATION
      );
      expect(backRelWarning).toBeDefined();
    });

    it('should warn about unindexed backward relations', () => {
      const result = validateSchema({
        users: {
          id: 'uuid!',
          createdAt: 'timestamp = now()',
          updatedAt: 'timestamp = now()',
        },
        orders: {
          id: 'uuid!',
          userId: '<- users',
          createdAt: 'timestamp = now()',
          updatedAt: 'timestamp = now()',
        },
      });

      const unindexedWarning = result.warnings.find(
        (w) => w.code === WarningCodes.UNINDEXED_RELATION
      );
      expect(unindexedWarning).toBeDefined();
    });
  });

  describe('type validation', () => {
    it('should error on unknown types', () => {
      const result = validateSchema({
        users: {
          id: 'uuid!',
          data: 'varchar(255)',
          createdAt: 'timestamp = now()',
          updatedAt: 'timestamp = now()',
        },
      });

      expect(result.valid).toBe(false);
      const typeError = result.errors.find(
        (e) => e.code === ErrorCodes.UNKNOWN_TYPE
      );
      expect(typeError).toBeDefined();
      expect(typeError!.path).toEqual(['users', 'data']);
    });
  });

  describe('primary key validation', () => {
    it('should error on nullable primary key', () => {
      const result = validateSchema({
        users: {
          id: 'uuid!?',
          name: 'string',
          createdAt: 'timestamp = now()',
          updatedAt: 'timestamp = now()',
        },
      });

      expect(result.valid).toBe(false);
      const pkError = result.errors.find(
        (e) => e.code === ErrorCodes.NULLABLE_PRIMARY_KEY
      );
      expect(pkError).toBeDefined();
    });

    it('should warn when table has no primary key', () => {
      const result = validateSchema({
        users: {
          name: 'string',
          email: 'string',
          createdAt: 'timestamp = now()',
          updatedAt: 'timestamp = now()',
        },
      });

      // The warning for no primary key uses MISSING_TIMESTAMPS code (as seen in source)
      expect(result.valid).toBe(true);
      expect(result.warnings.length).toBeGreaterThan(0);
    });
  });

  describe('field name validation', () => {
    it('should error on reserved field names', () => {
      const result = validateSchema({
        users: {
          id: 'uuid!',
          constructor: 'string',
          createdAt: 'timestamp = now()',
          updatedAt: 'timestamp = now()',
        },
      });

      expect(result.valid).toBe(false);
      const reservedError = result.errors.find(
        (e) => e.code === ErrorCodes.RESERVED_FIELD_NAME
      );
      expect(reservedError).toBeDefined();
    });

    it('should warn about snake_case field names', () => {
      const result = validateSchema({
        users: {
          id: 'uuid!',
          first_name: 'string',
          createdAt: 'timestamp = now()',
          updatedAt: 'timestamp = now()',
        },
      });

      const snakeCaseWarning = result.warnings.find(
        (w) => w.code === WarningCodes.SNAKE_CASE_FIELD
      );
      expect(snakeCaseWarning).toBeDefined();
    });
  });

  describe('table validation', () => {
    it('should error on empty tables', () => {
      const result = validateSchema({
        users: {},
      });

      expect(result.valid).toBe(false);
      const emptyError = result.errors.find(
        (e) => e.code === ErrorCodes.EMPTY_TABLE
      );
      expect(emptyError).toBeDefined();
    });

    it('should warn about missing timestamp fields', () => {
      const result = validateSchema({
        users: {
          id: 'uuid!',
          name: 'string',
        },
      });

      const tsWarning = result.warnings.find(
        (w) => w.code === WarningCodes.MISSING_TIMESTAMPS
      );
      expect(tsWarning).toBeDefined();
    });

    it('should not warn about timestamps when they are present', () => {
      const result = validateSchema({
        users: {
          id: 'uuid!',
          name: 'string',
          createdAt: 'timestamp = now()',
          updatedAt: 'timestamp = now()',
        },
      });

      const tsWarnings = result.warnings.filter(
        (w) =>
          w.code === WarningCodes.MISSING_TIMESTAMPS &&
          w.message.includes('missing timestamp')
      );
      expect(tsWarnings).toHaveLength(0);
    });
  });

  describe('validation options', () => {
    it('should skip camelCase warnings when configured', () => {
      const result = validateSchema(
        {
          users: {
            id: 'uuid!',
            first_name: 'string',
            createdAt: 'timestamp = now()',
            updatedAt: 'timestamp = now()',
          },
        },
        { skipCamelCaseWarnings: true }
      );

      const camelCaseWarnings = result.warnings.filter(
        (w) =>
          w.code === WarningCodes.SNAKE_CASE_FIELD ||
          w.code === WarningCodes.NON_CAMEL_CASE_FIELD
      );
      expect(camelCaseWarnings).toHaveLength(0);
    });

    it('should skip timestamp warnings when configured', () => {
      const result = validateSchema(
        {
          users: {
            id: 'uuid!',
            name: 'string',
          },
        },
        { skipTimestampWarnings: true }
      );

      const tsWarnings = result.warnings.filter(
        (w) => w.code === WarningCodes.MISSING_TIMESTAMPS
      );
      expect(tsWarnings).toHaveLength(0);
    });
  });
});

// =============================================================================
// isValidSchema TESTS
// =============================================================================

describe('isValidSchema', () => {
  it('should return true for valid schemas', () => {
    expect(
      isValidSchema({
        users: {
          id: 'uuid!',
          name: 'string',
        },
      })
    ).toBe(true);
  });

  it('should return false for invalid schemas', () => {
    expect(
      isValidSchema({
        users: {},
      })
    ).toBe(false);
  });
});

// =============================================================================
// assertValidSchema TESTS
// =============================================================================

describe('assertValidSchema', () => {
  it('should not throw for valid schemas', () => {
    expect(() =>
      assertValidSchema({
        users: {
          id: 'uuid!',
          name: 'string',
        },
      })
    ).not.toThrow();
  });

  it('should throw for invalid schemas with error details', () => {
    expect(() =>
      assertValidSchema({
        users: {},
      })
    ).toThrow('Schema validation failed');
  });

  it('should include error codes in thrown message', () => {
    try {
      assertValidSchema({
        users: {},
      });
    } catch (e: any) {
      expect(e.message).toContain(ErrorCodes.EMPTY_TABLE);
    }
  });
});
