/**
 * Scalar IN Expression Tests
 *
 * Tests for scalar IN expressions evaluated without a FROM clause.
 * These patterns evaluate IN expressions as SELECT columns, not WHERE conditions.
 *
 * Run with: cd packages/dosql && pnpm test src/__tests__/scalar-in-expressions.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { Database } from '../database.js';

describe('Scalar IN expressions without FROM clause', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
    // Create table t1 for subquery tests
    db.exec('CREATE TABLE t1 (x INTEGER)');
    db.exec('INSERT INTO t1 VALUES (1)');
    db.exec('INSERT INTO t1 VALUES (2)');
    db.exec('INSERT INTO t1 VALUES (3)');
  });

  describe('Literal IN expressions', () => {
    it('SELECT 1 IN (2) should return 0 (false)', () => {
      const result = db.prepare('SELECT 1 IN (2)').get() as Record<string, unknown>;
      const key = Object.keys(result)[0];
      expect(result[key]).toBe(0);
    });

    it('SELECT 1 IN (1) should return 1 (true)', () => {
      const result = db.prepare('SELECT 1 IN (1)').get() as Record<string, unknown>;
      const key = Object.keys(result)[0];
      expect(result[key]).toBe(1);
    });

    it('SELECT 1 IN (2,3,4,5,6,7,8,9) should return 0 (false)', () => {
      const result = db.prepare('SELECT 1 IN (2,3,4,5,6,7,8,9)').get() as Record<string, unknown>;
      const key = Object.keys(result)[0];
      expect(result[key]).toBe(0);
    });

    it('SELECT 1 IN (1,2,3,4,5,6,7,8,9) should return 1 (true)', () => {
      const result = db.prepare('SELECT 1 IN (1,2,3,4,5,6,7,8,9)').get() as Record<string, unknown>;
      const key = Object.keys(result)[0];
      expect(result[key]).toBe(1);
    });
  });

  describe('Literal NOT IN expressions', () => {
    it('SELECT 1 NOT IN (2) should return 1 (true)', () => {
      const result = db.prepare('SELECT 1 NOT IN (2)').get() as Record<string, unknown>;
      const key = Object.keys(result)[0];
      expect(result[key]).toBe(1);
    });

    it('SELECT 1 NOT IN (1) should return 0 (false)', () => {
      const result = db.prepare('SELECT 1 NOT IN (1)').get() as Record<string, unknown>;
      const key = Object.keys(result)[0];
      expect(result[key]).toBe(0);
    });

    it('SELECT 1 NOT IN (2,3,4,5,6,7,8,9) should return 1 (true)', () => {
      const result = db.prepare('SELECT 1 NOT IN (2,3,4,5,6,7,8,9)').get() as Record<string, unknown>;
      const key = Object.keys(result)[0];
      expect(result[key]).toBe(1);
    });
  });

  describe('NULL IN expressions', () => {
    it('SELECT null IN (SELECT * FROM t1) should return null', () => {
      const result = db.prepare('SELECT null IN (SELECT x FROM t1)').get() as Record<string, unknown>;
      const key = Object.keys(result)[0];
      expect(result[key]).toBe(null);
    });

    it('SELECT null IN (1, 2, 3) should return null', () => {
      const result = db.prepare('SELECT null IN (1, 2, 3)').get() as Record<string, unknown>;
      const key = Object.keys(result)[0];
      expect(result[key]).toBe(null);
    });

    it('SELECT null IN () should return 0 (false) - empty list override', () => {
      const result = db.prepare('SELECT null IN ()').get() as Record<string, unknown>;
      const key = Object.keys(result)[0];
      expect(result[key]).toBe(0);
    });

    it('SELECT null NOT IN () should return 1 (true) - empty list override', () => {
      const result = db.prepare('SELECT null NOT IN ()').get() as Record<string, unknown>;
      const key = Object.keys(result)[0];
      expect(result[key]).toBe(1);
    });
  });

  describe('IN with subquery', () => {
    it('SELECT 1 IN (SELECT x FROM t1) should return 1 (true)', () => {
      const result = db.prepare('SELECT 1 IN (SELECT x FROM t1)').get() as Record<string, unknown>;
      const key = Object.keys(result)[0];
      expect(result[key]).toBe(1);
    });

    it('SELECT 5 IN (SELECT x FROM t1) should return 0 (false)', () => {
      const result = db.prepare('SELECT 5 IN (SELECT x FROM t1)').get() as Record<string, unknown>;
      const key = Object.keys(result)[0];
      expect(result[key]).toBe(0);
    });
  });
});
