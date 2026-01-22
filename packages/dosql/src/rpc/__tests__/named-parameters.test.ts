/**
 * Named Parameter Conversion Tests
 *
 * Tests for the DoSQLTarget's #convertNamedParams method.
 * These tests verify that named parameter conversion:
 * - Respects string literal boundaries
 * - Handles escaped parameter markers
 * - Handles parameters in comments
 * - Throws errors for missing parameters
 * - Handles duplicate named parameters correctly
 *
 * @see /packages/dosql/src/statement/binding.ts - parseParameters function
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { DoSQLTarget } from '../index.js';
import { MockQueryExecutor } from '../../__tests__/utils/index.js';

describe('Named Parameter Conversion', () => {
  let executor: MockQueryExecutor;
  let target: DoSQLTarget;

  beforeEach(() => {
    executor = new MockQueryExecutor();
    executor.addTable('users', ['id', 'name', 'email'], ['number', 'string', 'string'], [
      [1, 'Alice', 'alice@example.com'],
      [2, 'Bob', 'bob@example.com'],
      [3, 'Charlie', 'charlie@example.com'],
    ]);
    target = new DoSQLTarget(executor);
  });

  // ===========================================================================
  // Basic Named Parameter Conversion
  // ===========================================================================

  describe('Basic Named Parameters', () => {
    it('should convert :name parameters to positional', async () => {
      const response = await target.query({
        sql: 'SELECT * FROM users WHERE id = :id',
        namedParams: { id: 1 },
      });

      expect(response.rows).toBeDefined();
      expect(response.error).toBeUndefined();
    });

    it('should convert @name parameters to positional', async () => {
      const response = await target.query({
        sql: 'SELECT * FROM users WHERE id = @id',
        namedParams: { id: 1 },
      });

      expect(response.rows).toBeDefined();
      expect(response.error).toBeUndefined();
    });

    it('should convert $name parameters to positional', async () => {
      const response = await target.query({
        sql: 'SELECT * FROM users WHERE id = $id',
        namedParams: { id: 1 },
      });

      expect(response.rows).toBeDefined();
      expect(response.error).toBeUndefined();
    });

    it('should convert multiple named parameters in order', async () => {
      const response = await target.query({
        sql: 'SELECT * FROM users WHERE id = :id AND name = :name',
        namedParams: { id: 1, name: 'Alice' },
      });

      expect(response.rows).toBeDefined();
      expect(response.error).toBeUndefined();
    });
  });

  // ===========================================================================
  // Parameters Inside String Literals
  // ===========================================================================

  describe('Parameters Inside String Literals', () => {
    it('should NOT extract :param from inside single-quoted string literals', async () => {
      // The :email inside the string is NOT a parameter
      const response = await target.query({
        sql: "SELECT * FROM users WHERE bio = 'Contact: :email@example.com'",
        namedParams: {}, // No params needed - :email is inside string
      });

      expect(response.rows).toBeDefined();
      expect(response.error).toBeUndefined();
    });

    it('should NOT extract @param from inside single-quoted string literals', async () => {
      const response = await target.query({
        sql: "SELECT * FROM users WHERE bio = 'Twitter: @username'",
        namedParams: {},
      });

      expect(response.rows).toBeDefined();
      expect(response.error).toBeUndefined();
    });

    it('should NOT extract $param from inside single-quoted string literals', async () => {
      const response = await target.query({
        sql: "SELECT * FROM users WHERE bio = 'Price: $amount dollars'",
        namedParams: {},
      });

      expect(response.rows).toBeDefined();
      expect(response.error).toBeUndefined();
    });

    it('should handle mix of real params and params inside strings', async () => {
      // :id is a real parameter, :fake is inside a string
      const response = await target.query({
        sql: "SELECT * FROM users WHERE id = :id AND bio LIKE 'Contact: :fake'",
        namedParams: { id: 1 }, // Only :id is needed
      });

      expect(response.rows).toBeDefined();
      expect(response.error).toBeUndefined();
    });

    it('should handle escaped quotes within strings', async () => {
      // The string is: O'Brien's contact: :email
      // :email is inside the string (after escaped quote)
      const response = await target.query({
        sql: "SELECT * FROM users WHERE name = 'O''Brien''s contact: :email'",
        namedParams: {},
      });

      expect(response.rows).toBeDefined();
      expect(response.error).toBeUndefined();
    });
  });

  // ===========================================================================
  // Parameters Inside Comments
  // ===========================================================================

  describe('Parameters Inside Comments', () => {
    it('should NOT extract params from single-line comments', async () => {
      const response = await target.query({
        sql: 'SELECT * FROM users -- WHERE id = :id',
        namedParams: {}, // :id is in comment, no params needed
      });

      expect(response.rows).toBeDefined();
      expect(response.error).toBeUndefined();
    });

    it('should NOT extract params from block comments', async () => {
      const response = await target.query({
        sql: 'SELECT * FROM users /* WHERE id = :id */',
        namedParams: {},
      });

      expect(response.rows).toBeDefined();
      expect(response.error).toBeUndefined();
    });

    it('should handle params before comments', async () => {
      // :id is a real param, :commented is in comment
      const response = await target.query({
        sql: 'SELECT * FROM users WHERE id = :id -- AND name = :commented',
        namedParams: { id: 1 },
      });

      expect(response.rows).toBeDefined();
      expect(response.error).toBeUndefined();
    });
  });

  // ===========================================================================
  // Missing Parameter Errors
  // ===========================================================================

  describe('Missing Parameter Errors', () => {
    it('should throw error when named parameter is not provided', async () => {
      await expect(
        target.query({
          sql: 'SELECT * FROM users WHERE id = :id',
          namedParams: {}, // Missing :id
        })
      ).rejects.toThrow(/missing.*:?id/i);
    });

    it('should throw error when one of multiple parameters is missing', async () => {
      await expect(
        target.query({
          sql: 'SELECT * FROM users WHERE id = :id AND name = :name',
          namedParams: { id: 1 }, // Missing :name
        })
      ).rejects.toThrow(/missing.*:?name/i);
    });

    it('should NOT throw for params inside strings even if not provided', async () => {
      // :fake is inside the string, so it's not a real parameter
      const response = await target.query({
        sql: "SELECT * FROM users WHERE bio = 'See :fake for details'",
        namedParams: {}, // :fake not needed since it's in string
      });

      expect(response.rows).toBeDefined();
      expect(response.error).toBeUndefined();
    });
  });

  // ===========================================================================
  // Duplicate Named Parameters
  // ===========================================================================

  describe('Duplicate Named Parameters', () => {
    it('should handle the same parameter used multiple times', async () => {
      // :id appears twice but should use the same value
      const response = await target.query({
        sql: 'SELECT * FROM users WHERE id = :id OR id = :id',
        namedParams: { id: 1 },
      });

      expect(response.rows).toBeDefined();
      expect(response.error).toBeUndefined();
    });

    it('should substitute all occurrences with the same value', async () => {
      // When :name appears multiple times, it should be substituted everywhere
      const response = await target.query({
        sql: "SELECT * FROM users WHERE name = :name OR email LIKE :name || '%'",
        namedParams: { name: 'Alice' },
      });

      expect(response.rows).toBeDefined();
      expect(response.error).toBeUndefined();
    });
  });

  // ===========================================================================
  // Edge Cases
  // ===========================================================================

  describe('Edge Cases', () => {
    it('should handle parameter at end of SQL', async () => {
      const response = await target.query({
        sql: 'SELECT * FROM users WHERE id = :id',
        namedParams: { id: 1 },
      });

      expect(response.rows).toBeDefined();
    });

    it('should handle parameter at beginning of WHERE clause', async () => {
      const response = await target.query({
        sql: 'SELECT * FROM users WHERE :active = 1',
        namedParams: { active: 1 },
      });

      expect(response.rows).toBeDefined();
    });

    it('should handle parameter with underscore in name', async () => {
      const response = await target.query({
        sql: 'SELECT * FROM users WHERE id = :user_id',
        namedParams: { user_id: 1 },
      });

      expect(response.rows).toBeDefined();
    });

    it('should handle parameter with numbers in name', async () => {
      const response = await target.query({
        sql: 'SELECT * FROM users WHERE id = :id1',
        namedParams: { id1: 1 },
      });

      expect(response.rows).toBeDefined();
    });

    it('should return positional params when namedParams is not provided', async () => {
      const response = await target.query({
        sql: 'SELECT * FROM users WHERE id = $1',
        params: [1],
      });

      expect(response.rows).toBeDefined();
    });
  });
});
