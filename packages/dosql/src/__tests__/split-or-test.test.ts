/**
 * Test for splitOrOutsideSubqueries
 */
import { describe, it, expect } from 'vitest';

describe('Split OR logic', () => {
  function splitOrOutsideSubqueries(where: string): string[] {
    const parts: string[] = [];
    let current = '';
    let depth = 0;
    let inStr = false;
    const upper = where.toUpperCase();

    for (let i = 0; i < where.length; i++) {
      const ch = where[i];
      if (ch === "'" && !inStr) { inStr = true; current += ch; continue; }
      if (ch === "'" && inStr) {
        if (where[i + 1] === "'") { current += "''"; i++; continue; }
        inStr = false; current += ch; continue;
      }
      if (inStr) { current += ch; continue; }
      if (ch === '(') { depth++; current += ch; continue; }
      if (ch === ')') { depth--; current += ch; continue; }

      if (depth === 0 && upper.slice(i).startsWith(' OR ')) {
        parts.push(current.trim());
        current = '';
        i += 3; // skip ' OR '
        continue;
      }
      current += ch;
    }
    if (current.trim()) parts.push(current.trim());
    return parts;
  }

  it('should split simple OR', () => {
    const result = splitOrOutsideSubqueries('a = 1 OR b = 2');
    console.log('Simple OR:', result);
    expect(result).toEqual(['a = 1', 'b = 2']);
  });

  it('should not split OR inside parentheses', () => {
    const result = splitOrOutsideSubqueries('(a = 1 OR b = 2) AND c = 3');
    console.log('OR in parens:', result);
    expect(result.length).toBe(1);
    expect(result[0]).toBe('(a = 1 OR b = 2) AND c = 3');
  });

  it('should split EXISTS OR boolean', () => {
    const where = 'EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b) OR (a>b-2 AND a<b+2)';
    const result = splitOrOutsideSubqueries(where);
    console.log('EXISTS OR boolean:', result);
    expect(result.length).toBe(2);
    expect(result[0]).toBe('EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)');
    expect(result[1]).toBe('(a>b-2 AND a<b+2)');
  });

  it('should handle multiline', () => {
    const where = `EXISTS(SELECT 1 FROM t1 AS x WHERE x.b<t1.b)
   OR (a>b-2 AND a<b+2)`;
    const result = splitOrOutsideSubqueries(where);
    console.log('Multiline:', result);
    expect(result.length).toBe(2);
  });
});
