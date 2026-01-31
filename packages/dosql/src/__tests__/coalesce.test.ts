/**
 * Tests for coalesce and null-handling functions
 */

import { describe, it, expect } from 'vitest';
import { Database } from '../database.js';

describe('coalesce and null-handling functions', () => {
  describe('coalesce()', () => {
    it('returns first non-null argument', () => {
      const db = new Database(':memory:');
      db.exec('CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER)');
      db.exec('INSERT INTO t1 VALUES(1, 2, 3)');
      db.exec('INSERT INTO t1 VALUES(NULL, 2, 3)');
      db.exec('INSERT INTO t1 VALUES(NULL, NULL, 3)');
      db.exec('INSERT INTO t1 VALUES(NULL, NULL, NULL)');

      const result = db.prepare('SELECT coalesce(a, b, c) as coal FROM t1').all();
      expect(result).toEqual([
        { coal: 1 },
        { coal: 2 },
        { coal: 3 },
        { coal: null },
      ]);
    });

    it('works in SELECT expressions', () => {
      const db = new Database(':memory:');
      const result = db.prepare('SELECT coalesce(NULL, 42)').all();
      expect(result[0]).toEqual({ 'coalesce(NULL, 42)': 42 });
    });

    it('works in WHERE clause', () => {
      const db = new Database(':memory:');
      db.exec('CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)');
      db.exec('INSERT INTO t1 VALUES(104, NULL, 102, 101, NULL)');
      db.exec('INSERT INTO t1 VALUES(107, 105, 106, 108, 109)');
      db.exec('INSERT INTO t1 VALUES(NULL, 112, 113, 114, 110)');

      const result = db.prepare('SELECT a, b FROM t1 WHERE coalesce(a,b,c,d,e) > 100').all();
      expect(result).toHaveLength(3);
    });

    it('works with compound WHERE conditions', () => {
      const db = new Database(':memory:');
      db.exec('CREATE TABLE t1(a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)');
      db.exec('INSERT INTO t1 VALUES(131, 130, 134, 133, 132)');  // a=131, b=130, difference=1, should match a>b-2 AND a<b+2
      db.exec('INSERT INTO t1 VALUES(138, 139, 137, 136, 135)');  // a=138, b=139, difference=-1, should match
      db.exec('INSERT INTO t1 VALUES(142, 143, 141, 140, 144)');  // a=142, b=143, difference=-1, should match
      db.exec('INSERT INTO t1 VALUES(100, 200, 300, 400, 500)');  // a=100, b=200, difference=-100, should NOT match

      const result = db.prepare(`
        SELECT a, b FROM t1
        WHERE coalesce(a,b,c,d,e)<>0
          AND (a>b-2 AND a<b+2)
      `).all();
      expect(result).toHaveLength(3);
      expect(result.map(r => r.a)).toEqual([131, 138, 142]);
    });
  });

  describe('ifnull()', () => {
    it('returns first arg if non-null, else second', () => {
      const db = new Database(':memory:');
      db.exec('CREATE TABLE t1(a INTEGER)');
      db.exec('INSERT INTO t1 VALUES(42)');
      db.exec('INSERT INTO t1 VALUES(NULL)');

      const result = db.prepare('SELECT ifnull(a, 999) as val FROM t1').all();
      expect(result).toEqual([
        { val: 42 },
        { val: 999 },
      ]);
    });
  });

  describe('nullif()', () => {
    it('returns null if both args equal, else first', () => {
      const db = new Database(':memory:');
      db.exec('CREATE TABLE t1(a INTEGER)');
      db.exec('INSERT INTO t1 VALUES(42)');
      db.exec('INSERT INTO t1 VALUES(100)');

      const result = db.prepare('SELECT nullif(a, 42) as val FROM t1').all();
      expect(result).toEqual([
        { val: null },
        { val: 100 },
      ]);
    });
  });

  describe('abs()', () => {
    it('returns absolute value in SELECT', () => {
      const db = new Database(':memory:');
      db.exec('CREATE TABLE t1(a INTEGER, b INTEGER)');
      db.exec('INSERT INTO t1 VALUES(10, 15)');
      db.exec('INSERT INTO t1 VALUES(20, 15)');

      const result = db.prepare('SELECT abs(a-b) as diff FROM t1').all();
      expect(result).toEqual([
        { diff: 5 },
        { diff: 5 },
      ]);
    });

    it('works in WHERE clause', () => {
      const db = new Database(':memory:');
      db.exec('CREATE TABLE t1(a INTEGER, b INTEGER)');
      db.exec('INSERT INTO t1 VALUES(10, 15)');  // diff = 5
      db.exec('INSERT INTO t1 VALUES(20, 15)');  // diff = 5
      db.exec('INSERT INTO t1 VALUES(100, 15)'); // diff = 85

      const result = db.prepare('SELECT a FROM t1 WHERE abs(a-b) < 10').all();
      expect(result).toHaveLength(2);
    });
  });
});
