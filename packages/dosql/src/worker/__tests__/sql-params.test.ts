/**
 * SQL Parameter Substitution Tests
 *
 * Tests for the substituteParams function which handles named parameter
 * substitution while avoiding modification of string literals and
 * quoted identifiers.
 */

import { describe, it, expect } from 'vitest';
import { escapeSqlValue, substituteParams, parseSqlValue } from '../sql-params.js';

describe('escapeSqlValue', () => {
  it('should escape null/undefined to NULL', () => {
    expect(escapeSqlValue(null)).toBe('NULL');
    expect(escapeSqlValue(undefined)).toBe('NULL');
  });

  it('should return numbers as-is', () => {
    expect(escapeSqlValue(42)).toBe('42');
    expect(escapeSqlValue(3.14)).toBe('3.14');
    expect(escapeSqlValue(-7)).toBe('-7');
  });

  it('should escape Infinity/NaN to NULL', () => {
    expect(escapeSqlValue(Infinity)).toBe('NULL');
    expect(escapeSqlValue(-Infinity)).toBe('NULL');
    expect(escapeSqlValue(NaN)).toBe('NULL');
  });

  it('should convert booleans to 1/0', () => {
    expect(escapeSqlValue(true)).toBe('1');
    expect(escapeSqlValue(false)).toBe('0');
  });

  it('should wrap strings in single quotes', () => {
    expect(escapeSqlValue('hello')).toBe("'hello'");
    expect(escapeSqlValue('world')).toBe("'world'");
  });

  it('should escape single quotes by doubling', () => {
    expect(escapeSqlValue("it's")).toBe("'it''s'");
    expect(escapeSqlValue("O'Brien")).toBe("'O''Brien'");
    expect(escapeSqlValue("test''double")).toBe("'test''''double'");
  });
});

describe('substituteParams', () => {
  describe('basic parameter substitution', () => {
    it('should substitute named parameters', () => {
      const sql = 'SELECT * FROM users WHERE id = :id';
      const result = substituteParams(sql, { id: 42 });
      expect(result).toBe('SELECT * FROM users WHERE id = 42');
    });

    it('should substitute multiple parameters', () => {
      const sql = 'SELECT * FROM users WHERE id = :id AND name = :name';
      const result = substituteParams(sql, { id: 1, name: 'Alice' });
      expect(result).toBe("SELECT * FROM users WHERE id = 1 AND name = 'Alice'");
    });

    it('should not substitute unknown parameters', () => {
      const sql = 'SELECT * FROM users WHERE id = :id AND status = :unknown';
      const result = substituteParams(sql, { id: 1 });
      expect(result).toBe('SELECT * FROM users WHERE id = 1 AND status = :unknown');
    });
  });

  describe('single-quoted string literals', () => {
    it('should not substitute parameters inside single-quoted strings', () => {
      const sql = "SELECT * FROM users WHERE note = ':name is here' AND id = :id";
      const result = substituteParams(sql, { id: 1, name: 'Alice' });
      expect(result).toBe("SELECT * FROM users WHERE note = ':name is here' AND id = 1");
    });

    it('should handle escaped single quotes in string literals', () => {
      const sql = "SELECT * FROM users WHERE note = 'it''s :name' AND id = :id";
      const result = substituteParams(sql, { id: 1, name: 'Alice' });
      expect(result).toBe("SELECT * FROM users WHERE note = 'it''s :name' AND id = 1");
    });

    it('should handle multiple escaped quotes', () => {
      const sql = "SELECT * FROM t WHERE a = 'test''s :x and ''more''' AND id = :id";
      const result = substituteParams(sql, { id: 42, x: 'val' });
      expect(result).toBe("SELECT * FROM t WHERE a = 'test''s :x and ''more''' AND id = 42");
    });
  });

  describe('double-quoted identifiers', () => {
    it('should not substitute parameters inside double-quoted identifiers', () => {
      const sql = 'SELECT * FROM "table:name" WHERE id = :id';
      const result = substituteParams(sql, { id: 1, name: 'test' });
      expect(result).toBe('SELECT * FROM "table:name" WHERE id = 1');
    });

    it('should not substitute parameters that look like column names in identifiers', () => {
      const sql = 'SELECT "col:umn" FROM users WHERE id = :id';
      const result = substituteParams(sql, { id: 42, umn: 'value' });
      expect(result).toBe('SELECT "col:umn" FROM users WHERE id = 42');
    });

    it('should handle multiple double-quoted identifiers', () => {
      const sql = 'SELECT "a:b", "c:d" FROM "table:x" WHERE "col:y" = :value';
      const result = substituteParams(sql, { value: 'test', b: 1, d: 2, x: 3, y: 4 });
      expect(result).toBe('SELECT "a:b", "c:d" FROM "table:x" WHERE "col:y" = \'test\'');
    });

    it('should handle escaped double quotes inside identifiers', () => {
      const sql = 'SELECT * FROM "table""with""quotes:name" WHERE id = :id';
      const result = substituteParams(sql, { id: 1, name: 'test' });
      expect(result).toBe('SELECT * FROM "table""with""quotes:name" WHERE id = 1');
    });

    it('should handle mixed single and double quotes', () => {
      const sql = "SELECT \"col:name\" FROM t WHERE x = ':param' AND id = :id";
      const result = substituteParams(sql, { id: 99, name: 'a', param: 'b' });
      expect(result).toBe("SELECT \"col:name\" FROM t WHERE x = ':param' AND id = 99");
    });
  });

  describe('backtick-quoted identifiers (MySQL style)', () => {
    it('should not substitute parameters inside backtick-quoted identifiers', () => {
      const sql = 'SELECT * FROM `table:name` WHERE id = :id';
      const result = substituteParams(sql, { id: 1, name: 'test' });
      expect(result).toBe('SELECT * FROM `table:name` WHERE id = 1');
    });

    it('should not substitute parameters that look like column names in backtick identifiers', () => {
      const sql = 'SELECT `col:umn` FROM users WHERE id = :id';
      const result = substituteParams(sql, { id: 42, umn: 'value' });
      expect(result).toBe('SELECT `col:umn` FROM users WHERE id = 42');
    });

    it('should handle multiple backtick-quoted identifiers', () => {
      const sql = 'SELECT `a:b`, `c:d` FROM `table:x` WHERE `col:y` = :value';
      const result = substituteParams(sql, { value: 'test', b: 1, d: 2, x: 3, y: 4 });
      expect(result).toBe("SELECT `a:b`, `c:d` FROM `table:x` WHERE `col:y` = 'test'");
    });

    it('should handle escaped backticks inside identifiers', () => {
      const sql = 'SELECT * FROM `table``with``backticks:name` WHERE id = :id';
      const result = substituteParams(sql, { id: 1, name: 'test' });
      expect(result).toBe('SELECT * FROM `table``with``backticks:name` WHERE id = 1');
    });

    it('should handle mixed backticks and other quotes', () => {
      const sql = "SELECT `col:name` FROM t WHERE x = ':param' AND \"y:z\" = :id";
      const result = substituteParams(sql, { id: 99, name: 'a', param: 'b', z: 'c' });
      expect(result).toBe("SELECT `col:name` FROM t WHERE x = ':param' AND \"y:z\" = 99");
    });

    it('should handle unclosed backtick gracefully', () => {
      const sql = 'SELECT * FROM `unclosed';
      const result = substituteParams(sql, {});
      expect(result).toBe('SELECT * FROM `unclosed');
    });
  });

  describe('bracket-quoted identifiers (SQL Server style)', () => {
    it('should not substitute parameters inside bracket-quoted identifiers', () => {
      const sql = 'SELECT * FROM [table:name] WHERE id = :id';
      const result = substituteParams(sql, { id: 1, name: 'test' });
      expect(result).toBe('SELECT * FROM [table:name] WHERE id = 1');
    });

    it('should not substitute parameters that look like column names in bracket identifiers', () => {
      const sql = 'SELECT [col:umn] FROM users WHERE id = :id';
      const result = substituteParams(sql, { id: 42, umn: 'value' });
      expect(result).toBe('SELECT [col:umn] FROM users WHERE id = 42');
    });

    it('should handle multiple bracket-quoted identifiers', () => {
      const sql = 'SELECT [a:b], [c:d] FROM [table:x] WHERE [col:y] = :value';
      const result = substituteParams(sql, { value: 'test', b: 1, d: 2, x: 3, y: 4 });
      expect(result).toBe("SELECT [a:b], [c:d] FROM [table:x] WHERE [col:y] = 'test'");
    });

    it('should handle escaped brackets inside identifiers', () => {
      const sql = 'SELECT * FROM [table]]with]]brackets:name] WHERE id = :id';
      const result = substituteParams(sql, { id: 1, name: 'test' });
      expect(result).toBe('SELECT * FROM [table]]with]]brackets:name] WHERE id = 1');
    });

    it('should handle mixed brackets and other quotes', () => {
      const sql = "SELECT [col:name] FROM t WHERE x = ':param' AND \"y:z\" = :id";
      const result = substituteParams(sql, { id: 99, name: 'a', param: 'b', z: 'c' });
      expect(result).toBe("SELECT [col:name] FROM t WHERE x = ':param' AND \"y:z\" = 99");
    });

    it('should handle unclosed bracket gracefully', () => {
      const sql = 'SELECT * FROM [unclosed';
      const result = substituteParams(sql, {});
      expect(result).toBe('SELECT * FROM [unclosed');
    });
  });

  describe('edge cases', () => {
    it('should handle empty strings', () => {
      expect(substituteParams('', {})).toBe('');
    });

    it('should handle SQL with no parameters', () => {
      const sql = 'SELECT * FROM users';
      expect(substituteParams(sql, {})).toBe('SELECT * FROM users');
    });

    it('should handle parameter at start of string', () => {
      const sql = ':id is the id';
      expect(substituteParams(sql, { id: 1 })).toBe('1 is the id');
    });

    it('should handle parameter at end of string', () => {
      const sql = 'value is :id';
      expect(substituteParams(sql, { id: 1 })).toBe('value is 1');
    });

    it('should handle adjacent parameters', () => {
      const sql = ':a:b:c';
      expect(substituteParams(sql, { a: 1, b: 2, c: 3 })).toBe('123');
    });

    it('should handle unclosed single quote gracefully', () => {
      const sql = "SELECT * FROM t WHERE x = 'unclosed";
      // Should not throw, just copy the rest as-is
      const result = substituteParams(sql, {});
      expect(result).toBe("SELECT * FROM t WHERE x = 'unclosed");
    });

    it('should handle unclosed double quote gracefully', () => {
      const sql = 'SELECT * FROM "unclosed';
      // Should not throw, just copy the rest as-is
      const result = substituteParams(sql, {});
      expect(result).toBe('SELECT * FROM "unclosed');
    });
  });
});

describe('parseSqlValue', () => {
  it('should parse single-quoted strings', () => {
    expect(parseSqlValue("'hello'")).toBe('hello');
    expect(parseSqlValue("'world'")).toBe('world');
  });

  it('should unescape doubled single quotes', () => {
    expect(parseSqlValue("'it''s'")).toBe("it's");
    expect(parseSqlValue("'test''''value'")).toBe("test''value");
  });

  it('should parse numbers', () => {
    expect(parseSqlValue('42')).toBe(42);
    expect(parseSqlValue('3.14')).toBe(3.14);
    expect(parseSqlValue('-7')).toBe(-7);
  });

  it('should parse NULL', () => {
    expect(parseSqlValue('NULL')).toBe(null);
    expect(parseSqlValue('null')).toBe(null);
    expect(parseSqlValue('Null')).toBe(null);
  });

  it('should return other values as-is', () => {
    expect(parseSqlValue('identifier')).toBe('identifier');
    expect(parseSqlValue('column_name')).toBe('column_name');
  });
});
