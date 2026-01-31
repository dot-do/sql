/**
 * SQLLogicTest Arithmetic Expression Tests - GREEN Phase TDD
 *
 * These tests document expected SQL arithmetic behavior that is NOW
 * implemented in the DoSQL InMemoryEngine. Tests that previously failed
 * (RED phase) are now passing (GREEN phase).
 *
 * Implemented features:
 * - Basic arithmetic operators: +, -, *, /, %
 * - Operator precedence: * and / before + and -
 * - Parentheses for grouping: (a+b)*c
 * - Multiple arithmetic columns in SELECT
 * - Unary operators: -a, +b
 * - NULL propagation (NULL + 1 = NULL)
 * - Column aliases with arithmetic expressions (expr AS alias)
 * - Function calls in expressions (e.g., abs())
 *
 * Run with: npx vitest run src/__tests__/sqllogictest-arithmetic.test.ts
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { Database } from '../database.js';
import {
  InMemoryEngine,
  createInMemoryStorage,
  type InMemoryStorage,
} from '../statement/statement.js';

// =============================================================================
// TEST SETUP
// =============================================================================

describe('SQLLogicTest Arithmetic Expressions - RED Phase', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
  });

  // ===========================================================================
  // BASIC ARITHMETIC OPERATORS
  // ===========================================================================

  describe('Basic Arithmetic Operators', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (id INTEGER, a INTEGER, b INTEGER, c INTEGER, d INTEGER)
      `);
      db.exec(`INSERT INTO t1 (id, a, b, c, d) VALUES (1, 10, 5, 3, 2)`);
      db.exec(`INSERT INTO t1 (id, a, b, c, d) VALUES (2, 20, 10, 6, 4)`);
      db.exec(`INSERT INTO t1 (id, a, b, c, d) VALUES (3, 30, 15, 9, 3)`);
    });

    /**
     * KNOWN FAILURE: Addition in SELECT clause
     *
     * Bug: The InMemoryEngine does not evaluate arithmetic expressions
     * in SELECT columns. Expected behavior is to compute a + b.
     *
     * SQLLogicTest: SELECT a+b FROM t1
     * Expected: 15, 30, 45
     * Actual: Error or raw expression string
     */
    it('should evaluate addition in SELECT clause', () => {
      const result = db.prepare('SELECT a+b FROM t1 ORDER BY id').all();

      expect(result.length).toBe(3);
      expect((result[0] as Record<string, unknown>)['a+b']).toBe(15);
      expect((result[1] as Record<string, unknown>)['a+b']).toBe(30);
      expect((result[2] as Record<string, unknown>)['a+b']).toBe(45);
    });

    /**
     * KNOWN FAILURE: Subtraction in SELECT clause
     *
     * Bug: The InMemoryEngine does not evaluate arithmetic expressions
     * in SELECT columns.
     *
     * SQLLogicTest: SELECT a-b FROM t1
     * Expected: 5, 10, 15
     */
    it('should evaluate subtraction in SELECT clause', () => {
      const result = db.prepare('SELECT a-b FROM t1 ORDER BY id').all();

      expect(result.length).toBe(3);
      expect((result[0] as Record<string, unknown>)['a-b']).toBe(5);
      expect((result[1] as Record<string, unknown>)['a-b']).toBe(10);
      expect((result[2] as Record<string, unknown>)['a-b']).toBe(15);
    });

    /**
     * KNOWN FAILURE: Multiplication in SELECT clause
     *
     * Bug: The InMemoryEngine does not evaluate arithmetic expressions
     * in SELECT columns.
     *
     * SQLLogicTest: SELECT b*c FROM t1
     * Expected: 15, 60, 135
     */
    it('should evaluate multiplication in SELECT clause', () => {
      const result = db.prepare('SELECT b*c FROM t1 ORDER BY id').all();

      expect(result.length).toBe(3);
      expect((result[0] as Record<string, unknown>)['b*c']).toBe(15);
      expect((result[1] as Record<string, unknown>)['b*c']).toBe(60);
      expect((result[2] as Record<string, unknown>)['b*c']).toBe(135);
    });

    /**
     * KNOWN FAILURE: Division in SELECT clause
     *
     * Bug: The InMemoryEngine does not evaluate arithmetic expressions
     * in SELECT columns.
     *
     * SQLLogicTest: SELECT a/b FROM t1
     * Expected: 2, 2, 2 (integer division)
     */
    it('should evaluate division in SELECT clause', () => {
      const result = db.prepare('SELECT a/b FROM t1 ORDER BY id').all();

      expect(result.length).toBe(3);
      expect((result[0] as Record<string, unknown>)['a/b']).toBe(2);
      expect((result[1] as Record<string, unknown>)['a/b']).toBe(2);
      expect((result[2] as Record<string, unknown>)['a/b']).toBe(2);
    });

    /**
     * KNOWN FAILURE: Modulo operator in SELECT clause
     *
     * Bug: The InMemoryEngine does not support the modulo (%) operator.
     *
     * SQLLogicTest: SELECT a%b FROM t1
     * Expected: 0, 0, 0 (10%5=0, 20%10=0, 30%15=0)
     */
    it('should evaluate modulo operator in SELECT clause', () => {
      const result = db.prepare('SELECT a%b FROM t1 ORDER BY id').all();

      expect(result.length).toBe(3);
      expect((result[0] as Record<string, unknown>)['a%b']).toBe(0);
      expect((result[1] as Record<string, unknown>)['a%b']).toBe(0);
      expect((result[2] as Record<string, unknown>)['a%b']).toBe(0);
    });

    /**
     * KNOWN FAILURE: Modulo with remainder
     *
     * Bug: The InMemoryEngine does not support the modulo (%) operator.
     *
     * SQLLogicTest: SELECT c%d FROM t1
     * Expected: 1, 2, 0 (3%2=1, 6%4=2, 9%3=0)
     */
    it('should evaluate modulo operator with remainder', () => {
      const result = db.prepare('SELECT c%d FROM t1 ORDER BY id').all();

      expect(result.length).toBe(3);
      expect((result[0] as Record<string, unknown>)['c%d']).toBe(1);
      expect((result[1] as Record<string, unknown>)['c%d']).toBe(2);
      expect((result[2] as Record<string, unknown>)['c%d']).toBe(0);
    });
  });

  // ===========================================================================
  // OPERATOR PRECEDENCE
  // ===========================================================================

  describe('Operator Precedence', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (id INTEGER, a INTEGER, b INTEGER, c INTEGER)
      `);
      db.exec(`INSERT INTO t1 (id, a, b, c) VALUES (1, 10, 5, 2)`);
      db.exec(`INSERT INTO t1 (id, a, b, c) VALUES (2, 20, 3, 4)`);
    });

    /**
     * KNOWN FAILURE: Multiplication has higher precedence than addition
     *
     * Bug: The InMemoryEngine does not properly handle operator precedence.
     * a+b*2 should be computed as a+(b*2), not (a+b)*2.
     *
     * SQLLogicTest: SELECT a+b*2 FROM t1
     * Expected: 20 (10+5*2=10+10=20), 26 (20+3*2=20+6=26)
     */
    it('should respect multiplication precedence over addition', () => {
      const result = db.prepare('SELECT a+b*2 FROM t1 ORDER BY id').all();

      expect(result.length).toBe(2);
      // a+b*2 = a + (b*2)
      expect((result[0] as Record<string, unknown>)['a+b*2']).toBe(20); // 10 + (5*2) = 20
      expect((result[1] as Record<string, unknown>)['a+b*2']).toBe(26); // 20 + (3*2) = 26
    });

    /**
     * KNOWN FAILURE: Complex expression with multiple operators
     *
     * Bug: The InMemoryEngine does not parse and evaluate complex arithmetic.
     *
     * SQLLogicTest: SELECT a+b*2+c*3 FROM t1
     * Expected: 26 (10+10+6=26), 38 (20+6+12=38)
     */
    it('should handle complex expression a+b*2+c*3', () => {
      const result = db.prepare('SELECT a+b*2+c*3 FROM t1 ORDER BY id').all();

      expect(result.length).toBe(2);
      // a + (b*2) + (c*3)
      expect((result[0] as Record<string, unknown>)['a+b*2+c*3']).toBe(26); // 10 + (5*2) + (2*3) = 10+10+6 = 26
      expect((result[1] as Record<string, unknown>)['a+b*2+c*3']).toBe(38); // 20 + (3*2) + (4*3) = 20+6+12 = 38
    });

    /**
     * KNOWN FAILURE: Division has higher precedence than subtraction
     *
     * Bug: The InMemoryEngine does not properly handle operator precedence.
     * a-b/c should be computed as a-(b/c).
     *
     * SQLLogicTest: SELECT a-b/c FROM t1
     * Expected: 8 (10-5/2=10-2=8), 20 (20-3/4=20-0=20)
     */
    it('should respect division precedence over subtraction', () => {
      const result = db.prepare('SELECT a-b/c FROM t1 ORDER BY id').all();

      expect(result.length).toBe(2);
      // a - (b/c) with integer division
      expect((result[0] as Record<string, unknown>)['a-b/c']).toBe(8);  // 10 - (5/2) = 10-2 = 8
      expect((result[1] as Record<string, unknown>)['a-b/c']).toBe(20); // 20 - (3/4) = 20-0 = 20
    });

    /**
     * KNOWN FAILURE: Mixed multiplication and division (left-to-right)
     *
     * Bug: The InMemoryEngine does not handle left-to-right associativity
     * for operators of equal precedence.
     *
     * SQLLogicTest: SELECT a*b/c FROM t1
     * Expected: 25 (10*5/2=50/2=25), 15 (20*3/4=60/4=15)
     */
    it('should evaluate multiplication and division left-to-right', () => {
      const result = db.prepare('SELECT a*b/c FROM t1 ORDER BY id').all();

      expect(result.length).toBe(2);
      // (a*b)/c - left to right for same precedence
      expect((result[0] as Record<string, unknown>)['a*b/c']).toBe(25); // (10*5)/2 = 25
      expect((result[1] as Record<string, unknown>)['a*b/c']).toBe(15); // (20*3)/4 = 15
    });
  });

  // ===========================================================================
  // PARENTHESES FOR GROUPING
  // ===========================================================================

  describe('Parentheses for Grouping', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (id INTEGER, a INTEGER, b INTEGER, c INTEGER)
      `);
      db.exec(`INSERT INTO t1 (id, a, b, c) VALUES (1, 12, 6, 3)`);
      db.exec(`INSERT INTO t1 (id, a, b, c) VALUES (2, 24, 8, 4)`);
    });

    /**
     * KNOWN FAILURE: Parentheses override precedence
     *
     * Bug: The InMemoryEngine does not parse parentheses in expressions.
     *
     * SQLLogicTest: SELECT (a+b)*c FROM t1
     * Expected: 54 ((12+6)*3=18*3=54), 128 ((24+8)*4=32*4=128)
     */
    it('should use parentheses to override precedence', () => {
      const result = db.prepare('SELECT (a+b)*c FROM t1 ORDER BY id').all();

      expect(result.length).toBe(2);
      // (a+b)*c - parentheses override precedence
      expect((result[0] as Record<string, unknown>)['(a+b)*c']).toBe(54);  // (12+6)*3 = 54
      expect((result[1] as Record<string, unknown>)['(a+b)*c']).toBe(128); // (24+8)*4 = 128
    });

    /**
     * KNOWN FAILURE: Division of summed values
     *
     * Bug: The InMemoryEngine does not parse parentheses in expressions.
     *
     * SQLLogicTest: SELECT (a+b+c)/3 FROM t1
     * Expected: 7 ((12+6+3)/3=21/3=7), 12 ((24+8+4)/3=36/3=12)
     */
    it('should handle division of grouped sum (a+b+c)/3', () => {
      const result = db.prepare('SELECT (a+b+c)/3 FROM t1 ORDER BY id').all();

      expect(result.length).toBe(2);
      expect((result[0] as Record<string, unknown>)['(a+b+c)/3']).toBe(7);  // (12+6+3)/3 = 7
      expect((result[1] as Record<string, unknown>)['(a+b+c)/3']).toBe(12); // (24+8+4)/3 = 12
    });

    /**
     * KNOWN FAILURE: Nested parentheses
     *
     * Bug: The InMemoryEngine does not parse nested parentheses.
     *
     * SQLLogicTest: SELECT ((a+b)*c)/2 FROM t1
     * Expected: 27 (((12+6)*3)/2=54/2=27), 64 (((24+8)*4)/2=128/2=64)
     */
    it('should handle nested parentheses', () => {
      const result = db.prepare('SELECT ((a+b)*c)/2 FROM t1 ORDER BY id').all();

      expect(result.length).toBe(2);
      expect((result[0] as Record<string, unknown>)['((a+b)*c)/2']).toBe(27); // ((12+6)*3)/2 = 27
      expect((result[1] as Record<string, unknown>)['((a+b)*c)/2']).toBe(64); // ((24+8)*4)/2 = 64
    });

    /**
     * KNOWN FAILURE: Parentheses changing subtraction order
     *
     * Bug: The InMemoryEngine does not parse parentheses in expressions.
     *
     * SQLLogicTest: SELECT a-(b-c) FROM t1
     * Expected: 9 (12-(6-3)=12-3=9), 20 (24-(8-4)=24-4=20)
     */
    it('should handle parentheses in subtraction', () => {
      const result = db.prepare('SELECT a-(b-c) FROM t1 ORDER BY id').all();

      expect(result.length).toBe(2);
      expect((result[0] as Record<string, unknown>)['a-(b-c)']).toBe(9);  // 12-(6-3) = 9
      expect((result[1] as Record<string, unknown>)['a-(b-c)']).toBe(20); // 24-(8-4) = 20
    });
  });

  // ===========================================================================
  // MULTIPLE ARITHMETIC COLUMNS
  // ===========================================================================

  describe('Multiple Arithmetic Columns', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (id INTEGER, a INTEGER, b INTEGER, c INTEGER, d INTEGER)
      `);
      db.exec(`INSERT INTO t1 (id, a, b, c, d) VALUES (1, 10, 5, 3, 2)`);
      db.exec(`INSERT INTO t1 (id, a, b, c, d) VALUES (2, 20, 8, 6, 4)`);
    });

    /**
     * KNOWN FAILURE: Multiple arithmetic expressions in SELECT
     *
     * Bug: The InMemoryEngine cannot handle multiple computed columns.
     *
     * SQLLogicTest: SELECT a-b, b*c, c/d FROM t1
     * Expected: (5, 15, 1), (12, 48, 1)
     */
    it('should handle multiple arithmetic columns', () => {
      const result = db.prepare('SELECT a-b, b*c, c/d FROM t1 ORDER BY id').all();

      expect(result.length).toBe(2);
      const row0 = result[0] as Record<string, unknown>;
      const row1 = result[1] as Record<string, unknown>;

      expect(row0['a-b']).toBe(5);
      expect(row0['b*c']).toBe(15);
      expect(row0['c/d']).toBe(1);

      expect(row1['a-b']).toBe(12);
      expect(row1['b*c']).toBe(48);
      expect(row1['c/d']).toBe(1);
    });

    /**
     * KNOWN FAILURE: Mix of columns and arithmetic expressions
     *
     * Bug: The InMemoryEngine cannot mix regular columns with computed.
     *
     * SQLLogicTest: SELECT id, a, a+b, a*b FROM t1
     * Expected: (1, 10, 15, 50), (2, 20, 28, 160)
     */
    it('should mix regular columns with arithmetic expressions', () => {
      const result = db.prepare('SELECT id, a, a+b, a*b FROM t1 ORDER BY id').all();

      expect(result.length).toBe(2);
      const row0 = result[0] as Record<string, unknown>;
      const row1 = result[1] as Record<string, unknown>;

      expect(row0['id']).toBe(1);
      expect(row0['a']).toBe(10);
      expect(row0['a+b']).toBe(15);
      expect(row0['a*b']).toBe(50);

      expect(row1['id']).toBe(2);
      expect(row1['a']).toBe(20);
      expect(row1['a+b']).toBe(28);
      expect(row1['a*b']).toBe(160);
    });
  });

  // ===========================================================================
  // UNARY OPERATORS
  // ===========================================================================

  describe('Unary Operators', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (id INTEGER, a INTEGER, b INTEGER)
      `);
      db.exec(`INSERT INTO t1 (id, a, b) VALUES (1, 10, -5)`);
      db.exec(`INSERT INTO t1 (id, a, b) VALUES (2, -20, 15)`);
      db.exec(`INSERT INTO t1 (id, a, b) VALUES (3, 0, 0)`);
    });

    /**
     * KNOWN FAILURE: Unary minus in SELECT
     *
     * Bug: The InMemoryEngine does not support unary minus operator.
     *
     * SQLLogicTest: SELECT -a FROM t1
     * Expected: -10, 20, 0
     */
    it('should evaluate unary minus operator', () => {
      const result = db.prepare('SELECT -a FROM t1 ORDER BY id').all();

      expect(result.length).toBe(3);
      expect((result[0] as Record<string, unknown>)['-a']).toBe(-10);
      expect((result[1] as Record<string, unknown>)['-a']).toBe(20);
      expect((result[2] as Record<string, unknown>)['-a']).toBe(0);
    });

    /**
     * KNOWN FAILURE: Unary plus in SELECT
     *
     * Bug: The InMemoryEngine does not support unary plus operator.
     *
     * SQLLogicTest: SELECT +b FROM t1
     * Expected: -5, 15, 0 (unary plus is a no-op)
     */
    it('should evaluate unary plus operator', () => {
      const result = db.prepare('SELECT +b FROM t1 ORDER BY id').all();

      expect(result.length).toBe(3);
      expect((result[0] as Record<string, unknown>)['+b']).toBe(-5);
      expect((result[1] as Record<string, unknown>)['+b']).toBe(15);
      expect((result[2] as Record<string, unknown>)['+b']).toBe(0);
    });

    /**
     * KNOWN FAILURE: Combined unary operators
     *
     * Bug: The InMemoryEngine does not support multiple unary operators.
     *
     * SQLLogicTest: SELECT -a, +b FROM t1
     * Expected: (-10, -5), (20, 15), (0, 0)
     */
    it('should handle multiple unary operators in SELECT', () => {
      const result = db.prepare('SELECT -a, +b FROM t1 ORDER BY id').all();

      expect(result.length).toBe(3);
      const row0 = result[0] as Record<string, unknown>;
      const row1 = result[1] as Record<string, unknown>;
      const row2 = result[2] as Record<string, unknown>;

      expect(row0['-a']).toBe(-10);
      expect(row0['+b']).toBe(-5);
      expect(row1['-a']).toBe(20);
      expect(row1['+b']).toBe(15);
      expect(row2['-a']).toBe(0);
      expect(row2['+b']).toBe(0);
    });

    /**
     * KNOWN FAILURE: Double negative
     *
     * Bug: The InMemoryEngine does not support chained unary operators.
     *
     * SQLLogicTest: SELECT --a FROM t1 (or SELECT -(-a) FROM t1)
     * Expected: 10, -20, 0
     */
    it('should handle double negative', () => {
      const result = db.prepare('SELECT -(-a) FROM t1 ORDER BY id').all();

      expect(result.length).toBe(3);
      expect((result[0] as Record<string, unknown>)['-(-a)']).toBe(10);
      expect((result[1] as Record<string, unknown>)['-(-a)']).toBe(-20);
      expect((result[2] as Record<string, unknown>)['-(-a)']).toBe(0);
    });

    /**
     * KNOWN FAILURE: Unary minus with arithmetic
     *
     * Bug: The InMemoryEngine does not combine unary with binary operators.
     *
     * SQLLogicTest: SELECT -a + b FROM t1
     * Expected: -15, 35, 0
     */
    it('should combine unary minus with addition', () => {
      const result = db.prepare('SELECT -a + b FROM t1 ORDER BY id').all();

      expect(result.length).toBe(3);
      expect((result[0] as Record<string, unknown>)['-a + b']).toBe(-15); // -10 + (-5) = -15
      expect((result[1] as Record<string, unknown>)['-a + b']).toBe(35);  // 20 + 15 = 35
      expect((result[2] as Record<string, unknown>)['-a + b']).toBe(0);   // 0 + 0 = 0
    });
  });

  // ===========================================================================
  // ARITHMETIC WITH NULL VALUES
  // ===========================================================================

  describe('Arithmetic with NULL Values', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE nullable (id INTEGER, a INTEGER, b INTEGER)
      `);
      db.exec(`INSERT INTO nullable (id, a, b) VALUES (1, 10, 5)`);
      db.exec(`INSERT INTO nullable (id, a, b) VALUES (2, NULL, 5)`);
      db.exec(`INSERT INTO nullable (id, a, b) VALUES (3, 10, NULL)`);
      db.exec(`INSERT INTO nullable (id, a, b) VALUES (4, NULL, NULL)`);
    });

    /**
     * KNOWN FAILURE: NULL propagation in addition
     *
     * Bug: The InMemoryEngine does not handle NULL in arithmetic.
     * Per SQL standard, any arithmetic with NULL yields NULL.
     *
     * SQLLogicTest: SELECT a+b FROM nullable
     * Expected: 15, NULL, NULL, NULL
     */
    it('should propagate NULL in addition', () => {
      const result = db.prepare('SELECT a+b FROM nullable ORDER BY id').all();

      expect(result.length).toBe(4);
      expect((result[0] as Record<string, unknown>)['a+b']).toBe(15);
      expect((result[1] as Record<string, unknown>)['a+b']).toBeNull();
      expect((result[2] as Record<string, unknown>)['a+b']).toBeNull();
      expect((result[3] as Record<string, unknown>)['a+b']).toBeNull();
    });

    /**
     * KNOWN FAILURE: NULL propagation in subtraction
     *
     * Bug: The InMemoryEngine does not handle NULL in arithmetic.
     *
     * SQLLogicTest: SELECT a-b FROM nullable
     * Expected: 5, NULL, NULL, NULL
     */
    it('should propagate NULL in subtraction', () => {
      const result = db.prepare('SELECT a-b FROM nullable ORDER BY id').all();

      expect(result.length).toBe(4);
      expect((result[0] as Record<string, unknown>)['a-b']).toBe(5);
      expect((result[1] as Record<string, unknown>)['a-b']).toBeNull();
      expect((result[2] as Record<string, unknown>)['a-b']).toBeNull();
      expect((result[3] as Record<string, unknown>)['a-b']).toBeNull();
    });

    /**
     * KNOWN FAILURE: NULL propagation in multiplication
     *
     * Bug: The InMemoryEngine does not handle NULL in arithmetic.
     *
     * SQLLogicTest: SELECT a*b FROM nullable
     * Expected: 50, NULL, NULL, NULL
     */
    it('should propagate NULL in multiplication', () => {
      const result = db.prepare('SELECT a*b FROM nullable ORDER BY id').all();

      expect(result.length).toBe(4);
      expect((result[0] as Record<string, unknown>)['a*b']).toBe(50);
      expect((result[1] as Record<string, unknown>)['a*b']).toBeNull();
      expect((result[2] as Record<string, unknown>)['a*b']).toBeNull();
      expect((result[3] as Record<string, unknown>)['a*b']).toBeNull();
    });

    /**
     * KNOWN FAILURE: NULL propagation in division
     *
     * Bug: The InMemoryEngine does not handle NULL in arithmetic.
     *
     * SQLLogicTest: SELECT a/b FROM nullable
     * Expected: 2, NULL, NULL, NULL
     */
    it('should propagate NULL in division', () => {
      const result = db.prepare('SELECT a/b FROM nullable ORDER BY id').all();

      expect(result.length).toBe(4);
      expect((result[0] as Record<string, unknown>)['a/b']).toBe(2);
      expect((result[1] as Record<string, unknown>)['a/b']).toBeNull();
      expect((result[2] as Record<string, unknown>)['a/b']).toBeNull();
      expect((result[3] as Record<string, unknown>)['a/b']).toBeNull();
    });

    /**
     * KNOWN FAILURE: NULL propagation in modulo
     *
     * Bug: The InMemoryEngine does not handle NULL in arithmetic.
     *
     * SQLLogicTest: SELECT a%b FROM nullable
     * Expected: 0, NULL, NULL, NULL
     */
    it('should propagate NULL in modulo', () => {
      const result = db.prepare('SELECT a%b FROM nullable ORDER BY id').all();

      expect(result.length).toBe(4);
      expect((result[0] as Record<string, unknown>)['a%b']).toBe(0);
      expect((result[1] as Record<string, unknown>)['a%b']).toBeNull();
      expect((result[2] as Record<string, unknown>)['a%b']).toBeNull();
      expect((result[3] as Record<string, unknown>)['a%b']).toBeNull();
    });

    /**
     * KNOWN FAILURE: NULL propagation with unary minus
     *
     * Bug: The InMemoryEngine does not handle NULL in arithmetic.
     *
     * SQLLogicTest: SELECT -a FROM nullable
     * Expected: -10, NULL, -10, NULL
     */
    it('should propagate NULL with unary minus', () => {
      const result = db.prepare('SELECT -a FROM nullable ORDER BY id').all();

      expect(result.length).toBe(4);
      expect((result[0] as Record<string, unknown>)['-a']).toBe(-10);
      expect((result[1] as Record<string, unknown>)['-a']).toBeNull();
      expect((result[2] as Record<string, unknown>)['-a']).toBe(-10);
      expect((result[3] as Record<string, unknown>)['-a']).toBeNull();
    });
  });

  // ===========================================================================
  // ARITHMETIC WITH FUNCTIONS
  // ===========================================================================

  describe('Arithmetic with Functions', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (id INTEGER, a INTEGER, b INTEGER)
      `);
      db.exec(`INSERT INTO t1 (id, a, b) VALUES (1, 10, 3)`);
      db.exec(`INSERT INTO t1 (id, a, b) VALUES (2, -20, 7)`);
      db.exec(`INSERT INTO t1 (id, a, b) VALUES (3, 15, 4)`);
    });

    /**
     * KNOWN FAILURE: abs() function with arithmetic
     *
     * Bug: The InMemoryEngine may not combine functions with arithmetic.
     *
     * SQLLogicTest: SELECT abs(a-b) FROM t1
     * Expected: 7, 27, 11
     */
    it('should evaluate abs() with arithmetic expression', () => {
      const result = db.prepare('SELECT abs(a-b) FROM t1 ORDER BY id').all();

      expect(result.length).toBe(3);
      expect((result[0] as Record<string, unknown>)['abs(a-b)']).toBe(7);  // abs(10-3) = 7
      expect((result[1] as Record<string, unknown>)['abs(a-b)']).toBe(27); // abs(-20-7) = 27
      expect((result[2] as Record<string, unknown>)['abs(a-b)']).toBe(11); // abs(15-4) = 11
    });

    /**
     * KNOWN FAILURE: Combined abs() and modulo
     *
     * Bug: The InMemoryEngine cannot handle multiple expressions with functions.
     *
     * SQLLogicTest: SELECT abs(a-b), a%b FROM t1
     * Expected: (7, 1), (27, -6), (11, 3)
     */
    it('should handle abs() and modulo together', () => {
      const result = db.prepare('SELECT abs(a-b), a%b FROM t1 ORDER BY id').all();

      expect(result.length).toBe(3);
      const row0 = result[0] as Record<string, unknown>;
      const row1 = result[1] as Record<string, unknown>;
      const row2 = result[2] as Record<string, unknown>;

      expect(row0['abs(a-b)']).toBe(7);
      expect(row0['a%b']).toBe(1);  // 10 % 3 = 1

      expect(row1['abs(a-b)']).toBe(27);
      expect(row1['a%b']).toBe(-6); // -20 % 7 = -6 (or 1 depending on implementation)

      expect(row2['abs(a-b)']).toBe(11);
      expect(row2['a%b']).toBe(3);  // 15 % 4 = 3
    });
  });

  // ===========================================================================
  // DIVISION EDGE CASES
  // ===========================================================================

  describe('Division Edge Cases', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE division_test (id INTEGER, a INTEGER, b INTEGER)
      `);
      db.exec(`INSERT INTO division_test (id, a, b) VALUES (1, 10, 3)`);
      db.exec(`INSERT INTO division_test (id, a, b) VALUES (2, 7, 2)`);
      db.exec(`INSERT INTO division_test (id, a, b) VALUES (3, 10, 0)`);
    });

    /**
     * KNOWN FAILURE: Integer division truncation
     *
     * Bug: The InMemoryEngine may not properly truncate integer division.
     *
     * SQLLogicTest: SELECT a/b FROM division_test WHERE b != 0
     * Expected: 3 (10/3=3.33->3), 3 (7/2=3.5->3)
     */
    it('should truncate integer division toward zero', () => {
      const result = db.prepare('SELECT a/b FROM division_test WHERE b != 0 ORDER BY id').all();

      expect(result.length).toBe(2);
      expect((result[0] as Record<string, unknown>)['a/b']).toBe(3);  // 10/3 = 3
      expect((result[1] as Record<string, unknown>)['a/b']).toBe(3);  // 7/2 = 3
    });

    /**
     * Division by zero handling
     *
     * SQLite returns NULL for division by zero.
     * Note: This test passes - the engine correctly handles division by zero.
     *
     * SQLLogicTest: SELECT a/b FROM division_test WHERE id = 3
     * Expected: NULL (division by zero)
     */
    it('should return NULL for division by zero', () => {
      const result = db.prepare('SELECT a/b FROM division_test WHERE id = 3').all();

      expect(result.length).toBe(1);
      expect((result[0] as Record<string, unknown>)['a/b']).toBeNull();
    });

    /**
     * Modulo by zero handling
     *
     * SQLite returns NULL for modulo by zero.
     * Note: This test passes - the engine correctly handles modulo by zero.
     *
     * SQLLogicTest: SELECT a%b FROM division_test WHERE id = 3
     * Expected: NULL (modulo by zero)
     */
    it('should return NULL for modulo by zero', () => {
      const result = db.prepare('SELECT a%b FROM division_test WHERE id = 3').all();

      expect(result.length).toBe(1);
      expect((result[0] as Record<string, unknown>)['a%b']).toBeNull();
    });
  });

  // ===========================================================================
  // ARITHMETIC WITH REAL/FLOAT VALUES
  // ===========================================================================

  describe('Arithmetic with REAL/Float Values', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE floats (id INTEGER, a REAL, b REAL)
      `);
      db.exec(`INSERT INTO floats (id, a, b) VALUES (1, 10.5, 3.2)`);
      db.exec(`INSERT INTO floats (id, a, b) VALUES (2, 7.8, 2.5)`);
    });

    /**
     * KNOWN FAILURE: Float arithmetic
     *
     * Bug: The InMemoryEngine may not support arithmetic on REAL columns.
     *
     * SQLLogicTest: SELECT a+b FROM floats
     * Expected: 13.7, 10.3
     */
    it('should handle float addition', () => {
      const result = db.prepare('SELECT a+b FROM floats ORDER BY id').all();

      expect(result.length).toBe(2);
      expect((result[0] as Record<string, unknown>)['a+b']).toBeCloseTo(13.7, 5);
      expect((result[1] as Record<string, unknown>)['a+b']).toBeCloseTo(10.3, 5);
    });

    /**
     * KNOWN FAILURE: Float division (non-truncating)
     *
     * Bug: The InMemoryEngine may not support float division.
     *
     * SQLLogicTest: SELECT a/b FROM floats
     * Expected: 3.28125, 3.12
     */
    it('should handle float division', () => {
      const result = db.prepare('SELECT a/b FROM floats ORDER BY id').all();

      expect(result.length).toBe(2);
      expect((result[0] as Record<string, unknown>)['a/b']).toBeCloseTo(3.28125, 5);
      expect((result[1] as Record<string, unknown>)['a/b']).toBeCloseTo(3.12, 5);
    });
  });

  // ===========================================================================
  // ARITHMETIC WITH COLUMN ALIASES
  // ===========================================================================

  describe('Arithmetic with Column Aliases', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE t1 (id INTEGER, a INTEGER, b INTEGER)
      `);
      db.exec(`INSERT INTO t1 (id, a, b) VALUES (1, 10, 5)`);
      db.exec(`INSERT INTO t1 (id, a, b) VALUES (2, 20, 8)`);
    });

    /**
     * KNOWN FAILURE: Arithmetic with AS alias
     *
     * Bug: The InMemoryEngine may not handle aliases for arithmetic expressions.
     *
     * SQLLogicTest: SELECT a+b AS total FROM t1
     * Expected: 15, 28 (with column named 'total')
     */
    it('should handle arithmetic with AS alias', () => {
      const result = db.prepare('SELECT a+b AS total FROM t1 ORDER BY id').all();

      expect(result.length).toBe(2);
      expect((result[0] as Record<string, unknown>)['total']).toBe(15);
      expect((result[1] as Record<string, unknown>)['total']).toBe(28);
    });

    /**
     * KNOWN FAILURE: Multiple arithmetic expressions with aliases
     *
     * Bug: The InMemoryEngine may not handle multiple aliased expressions.
     *
     * SQLLogicTest: SELECT a+b AS sum, a*b AS product, a-b AS diff FROM t1
     * Expected: (15, 50, 5), (28, 160, 12)
     */
    it('should handle multiple arithmetic expressions with aliases', () => {
      const result = db.prepare('SELECT a+b AS sum, a*b AS product, a-b AS diff FROM t1 ORDER BY id').all();

      expect(result.length).toBe(2);
      const row0 = result[0] as Record<string, unknown>;
      const row1 = result[1] as Record<string, unknown>;

      expect(row0['sum']).toBe(15);
      expect(row0['product']).toBe(50);
      expect(row0['diff']).toBe(5);

      expect(row1['sum']).toBe(28);
      expect(row1['product']).toBe(160);
      expect(row1['diff']).toBe(12);
    });
  });

  // ===========================================================================
  // SELECT ALL / SELECT DISTINCT WITH ARITHMETIC
  // ===========================================================================

  describe('SELECT ALL/DISTINCT with Arithmetic', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE tab1 (id INTEGER, x INTEGER, y INTEGER, z INTEGER)
      `);
      db.exec(`INSERT INTO tab1 (id, x, y, z) VALUES (1, 10, 20, 30)`);
      db.exec(`INSERT INTO tab1 (id, x, y, z) VALUES (2, 20, 40, 60)`);
      db.exec(`INSERT INTO tab1 (id, x, y, z) VALUES (3, 30, 60, 90)`);
    });

    /**
     * SELECT ALL with unary plus and multiplication
     *
     * SQLLogicTest: SELECT ALL ( + 39 ) * 15 FROM tab1
     * Expected: 585, 585, 585
     */
    it('should evaluate SELECT ALL with unary plus in parentheses', () => {
      const result = db.prepare('SELECT ALL ( + 39 ) * 15 FROM tab1').all();

      expect(result.length).toBe(3);
      expect((result[0] as Record<string, unknown>)['( + 39 ) * 15']).toBe(585);
      expect((result[1] as Record<string, unknown>)['( + 39 ) * 15']).toBe(585);
      expect((result[2] as Record<string, unknown>)['( + 39 ) * 15']).toBe(585);
    });

    /**
     * SELECT ALL with simple multiplication and alias
     *
     * SQLLogicTest: SELECT ALL 13 * 21 AS col2
     * Expected: 273
     */
    it('should evaluate SELECT ALL with multiplication and alias (no FROM)', () => {
      const result = db.prepare('SELECT ALL 13 * 21 AS col2').all();

      expect(result.length).toBe(1);
      expect((result[0] as Record<string, unknown>)['col2']).toBe(273);
    });

    /**
     * SELECT ALL with column arithmetic
     *
     * SQLLogicTest: SELECT ALL x + y FROM tab1
     * Expected: 30, 60, 90
     */
    it('should evaluate SELECT ALL with column addition', () => {
      const result = db.prepare('SELECT ALL x + y FROM tab1 ORDER BY id').all();

      expect(result.length).toBe(3);
      expect((result[0] as Record<string, unknown>)['x + y']).toBe(30);
      expect((result[1] as Record<string, unknown>)['x + y']).toBe(60);
      expect((result[2] as Record<string, unknown>)['x + y']).toBe(90);
    });

    /**
     * SELECT DISTINCT with arithmetic
     *
     * SQLLogicTest: SELECT DISTINCT 1+1
     * Expected: 2
     */
    it('should evaluate SELECT DISTINCT with arithmetic (no FROM)', () => {
      const result = db.prepare('SELECT DISTINCT 1+1 AS result').all();

      expect(result.length).toBe(1);
      expect((result[0] as Record<string, unknown>)['result']).toBe(2);
    });
  });
});

// =============================================================================
// LOW-LEVEL INMEMORYENGINE ARITHMETIC TESTS
// =============================================================================

describe('InMemoryEngine Direct Arithmetic Tests - RED Phase', () => {
  let engine: InMemoryEngine;
  let storage: InMemoryStorage;

  beforeEach(() => {
    storage = createInMemoryStorage();
    engine = new InMemoryEngine(storage);

    // Set up test table
    engine.execute('CREATE TABLE test (id INTEGER, a INTEGER, b INTEGER, c INTEGER)', []);
    engine.execute('INSERT INTO test (id, a, b, c) VALUES (?, ?, ?, ?)', [1, 10, 5, 2]);
    engine.execute('INSERT INTO test (id, a, b, c) VALUES (?, ?, ?, ?)', [2, 20, 8, 3]);
  });

  /**
   * KNOWN FAILURE: Engine execute with arithmetic
   *
   * Bug: The execute method does not evaluate arithmetic in SELECT.
   */
  it('should execute SELECT with addition', () => {
    const result = engine.execute('SELECT a+b FROM test WHERE id = 1', []);

    expect(result.rows.length).toBe(1);
    expect(result.rows[0]['a+b']).toBe(15);
  });

  /**
   * KNOWN FAILURE: Engine execute with complex arithmetic
   */
  it('should execute SELECT with complex arithmetic expression', () => {
    const result = engine.execute('SELECT a+b*c FROM test WHERE id = 1', []);

    expect(result.rows.length).toBe(1);
    // a + (b*c) = 10 + (5*2) = 20
    expect(result.rows[0]['a+b*c']).toBe(20);
  });
});
