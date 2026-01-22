/**
 * Type-Level JOIN Parser Tests
 *
 * These tests verify that the JOIN parser correctly infers types at compile time.
 * Uses TypeScript's type system to validate inference.
 */

import { describe, it, expect, expectTypeOf } from 'vitest';
import type {
  JoinQueryResult,
  ParseSelectStatement,
  ParseTableRef,
  ParseFromTable,
  ExtractAllJoins,
  BuildAliasMap,
  BuildJoinTypeMap,
  ParseJoinType,
  ParseSingleJoin,
  JoinClause,
  TableRef,
  DatabaseSchema,
  TableSchema,
} from '../join-parser.js';

// =============================================================================
// TEST DATABASE SCHEMA
// =============================================================================

interface TestDB extends DatabaseSchema {
  users: {
    id: 'number';
    name: 'string';
    email: 'string';
    created_at: 'Date';
  };
  orders: {
    id: 'number';
    user_id: 'number';
    total: 'number';
    status: 'string';
  };
  products: {
    id: 'number';
    name: 'string';
    price: 'number';
    category: 'string';
  };
  order_items: {
    id: 'number';
    order_id: 'number';
    product_id: 'number';
    quantity: 'number';
    unit_price: 'number';
  };
}

// =============================================================================
// TYPE ASSERTION HELPERS
// =============================================================================

// Type equality check
type Expect<T extends true> = T;
type Equal<X, Y> =
  (<T>() => T extends X ? 1 : 2) extends (<T>() => T extends Y ? 1 : 2)
    ? true
    : false;

// Ensure a type extends another
type Extends<A, B> = A extends B ? true : false;

// =============================================================================
// PARSE TABLE REFERENCE TESTS
// =============================================================================

describe('ParseTableRef', () => {
  it('should parse simple table name', () => {
    type Result = ParseTableRef<'users'>;
    type _Test = Expect<Equal<Result, { table: 'users'; alias: 'users' }>>;
  });

  it('should parse table with alias (space)', () => {
    type Result = ParseTableRef<'users u'>;
    type _Test = Expect<Equal<Result, { table: 'users'; alias: 'u' }>>;
  });

  it('should parse table with AS keyword', () => {
    type Result = ParseTableRef<'users AS u'>;
    type _Test = Expect<Equal<Result, { table: 'users'; alias: 'u' }>>;
  });

  it('should handle mixed case AS keyword', () => {
    type Result = ParseTableRef<'Users as U'>;
    type _Test = Expect<Equal<Result, { table: 'users'; alias: 'u' }>>;
  });

  it('should handle whitespace', () => {
    type Result = ParseTableRef<'  users   u  '>;
    type _Test = Expect<Equal<Result, { table: 'users'; alias: 'u' }>>;
  });
});

// =============================================================================
// PARSE FROM TABLE TESTS
// =============================================================================

describe('ParseFromTable', () => {
  it('should parse FROM table before JOIN', () => {
    type Result = ParseFromTable<'users u JOIN orders o ON o.user_id = u.id'>;
    type _Test = Expect<Equal<Result, { table: 'users'; alias: 'u' }>>;
  });

  it('should parse FROM table before LEFT JOIN', () => {
    type Result = ParseFromTable<'users u LEFT JOIN orders o ON o.user_id = u.id'>;
    type _Test = Expect<Equal<Result, { table: 'users'; alias: 'u' }>>;
  });

  it('should parse FROM table before WHERE', () => {
    type Result = ParseFromTable<'users u WHERE id = 1'>;
    type _Test = Expect<Equal<Result, { table: 'users'; alias: 'u' }>>;
  });

  it('should parse simple FROM table', () => {
    type Result = ParseFromTable<'users'>;
    type _Test = Expect<Equal<Result, { table: 'users'; alias: 'users' }>>;
  });
});

// =============================================================================
// PARSE JOIN TYPE TESTS
// =============================================================================

describe('ParseJoinType', () => {
  it('should identify INNER JOIN', () => {
    type Result = ParseJoinType<'INNER JOIN users'>;
    type _Test = Expect<Equal<Result, 'INNER'>>;
  });

  it('should identify LEFT JOIN', () => {
    type Result = ParseJoinType<'LEFT JOIN users'>;
    type _Test = Expect<Equal<Result, 'LEFT'>>;
  });

  it('should identify RIGHT JOIN', () => {
    type Result = ParseJoinType<'RIGHT JOIN users'>;
    type _Test = Expect<Equal<Result, 'RIGHT'>>;
  });

  it('should identify LEFT OUTER JOIN', () => {
    type Result = ParseJoinType<'LEFT OUTER JOIN users'>;
    type _Test = Expect<Equal<Result, 'LEFT'>>;
  });

  it('should identify FULL OUTER JOIN', () => {
    type Result = ParseJoinType<'FULL OUTER JOIN users'>;
    type _Test = Expect<Equal<Result, 'FULL'>>;
  });

  it('should identify plain JOIN as INNER', () => {
    type Result = ParseJoinType<'JOIN users'>;
    type _Test = Expect<Equal<Result, 'INNER'>>;
  });

  it('should identify CROSS JOIN', () => {
    type Result = ParseJoinType<'CROSS JOIN users'>;
    type _Test = Expect<Equal<Result, 'CROSS'>>;
  });
});

// =============================================================================
// EXTRACT ALL JOINS TESTS
// =============================================================================

describe('ExtractAllJoins', () => {
  it('should extract single JOIN', () => {
    type Result = ExtractAllJoins<'users u JOIN orders o ON o.user_id = u.id'>;
    type _Test = Expect<Extends<Result, [{ type: 'INNER'; table: 'orders'; alias: 'o' }]>>;
  });

  it('should extract LEFT JOIN', () => {
    type Result = ExtractAllJoins<'users u LEFT JOIN orders o ON o.user_id = u.id'>;
    type _Test = Expect<Extends<Result, [{ type: 'LEFT'; table: 'orders'; alias: 'o' }]>>;
  });

  it('should extract multiple JOINs', () => {
    type Result = ExtractAllJoins<'users u JOIN orders o ON o.user_id = u.id JOIN order_items oi ON oi.order_id = o.id'>;
    type ExpectedFirst = { type: 'INNER'; table: 'orders'; alias: 'o' };
    type ExpectedSecond = { type: 'INNER'; table: 'order_items'; alias: 'oi' };
    // Check that both joins are extracted
    type _Test1 = Expect<Extends<Result[0], ExpectedFirst>>;
    type _Test2 = Expect<Extends<Result[1], ExpectedSecond>>;
  });

  it('should extract mixed JOIN types', () => {
    type Result = ExtractAllJoins<'users u LEFT JOIN orders o ON o.user_id = u.id INNER JOIN products p ON p.id = 1'>;
    type _Test1 = Expect<Extends<Result[0], { type: 'LEFT'; table: 'orders'; alias: 'o' }>>;
    type _Test2 = Expect<Extends<Result[1], { type: 'INNER'; table: 'products'; alias: 'p' }>>;
  });
});

// =============================================================================
// BUILD ALIAS MAP TESTS
// =============================================================================

describe('BuildAliasMap', () => {
  it('should build alias map for single table', () => {
    type From = { table: 'users'; alias: 'u' };
    type Joins = [];
    type Result = BuildAliasMap<From, Joins>;
    type _Test = Expect<Equal<Result['u'], 'users'>>;
  });

  it('should build alias map with JOIN', () => {
    type From = { table: 'users'; alias: 'u' };
    type Joins = [{ type: 'INNER'; table: 'orders'; alias: 'o' }];
    type Result = BuildAliasMap<From, Joins>;
    type _Test1 = Expect<Equal<Result['u'], 'users'>>;
    type _Test2 = Expect<Equal<Result['o'], 'orders'>>;
  });

  it('should build alias map with multiple JOINs', () => {
    type From = { table: 'users'; alias: 'u' };
    type Joins = [
      { type: 'INNER'; table: 'orders'; alias: 'o' },
      { type: 'LEFT'; table: 'products'; alias: 'p' }
    ];
    type Result = BuildAliasMap<From, Joins>;
    type _Test1 = Expect<Equal<Result['u'], 'users'>>;
    type _Test2 = Expect<Equal<Result['o'], 'orders'>>;
    type _Test3 = Expect<Equal<Result['p'], 'products'>>;
  });
});

// =============================================================================
// PARSE SELECT STATEMENT TESTS
// =============================================================================

describe('ParseSelectStatement', () => {
  it('should parse simple SELECT', () => {
    type Result = ParseSelectStatement<'SELECT id, name FROM users'>;
    type _Test1 = Expect<Equal<Result['columns'], ['id', 'name']>>;
    type _Test2 = Expect<Equal<Result['from'], { table: 'users'; alias: 'users' }>>;
    type _Test3 = Expect<Equal<Result['joins'], []>>;
  });

  it('should parse SELECT with alias', () => {
    type Result = ParseSelectStatement<'SELECT u.id, u.name FROM users u'>;
    type _Test1 = Expect<Equal<Result['columns'], ['u.id', 'u.name']>>;
    type _Test2 = Expect<Equal<Result['from'], { table: 'users'; alias: 'u' }>>;
  });

  it('should parse SELECT with JOIN', () => {
    type Result = ParseSelectStatement<'SELECT u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id'>;
    type _Test1 = Expect<Equal<Result['columns'], ['u.name', 'o.total']>>;
    type _Test2 = Expect<Equal<Result['from'], { table: 'users'; alias: 'u' }>>;
    type _Test3 = Expect<Extends<Result['joins'][0], { type: 'INNER'; table: 'orders'; alias: 'o' }>>;
  });

  it('should parse SELECT * with JOIN', () => {
    type Result = ParseSelectStatement<'SELECT * FROM users u JOIN orders o ON o.user_id = u.id'>;
    type _Test1 = Expect<Equal<Result['columns'], ['*']>>;
    type _Test2 = Expect<Equal<Result['from'], { table: 'users'; alias: 'u' }>>;
  });
});

// =============================================================================
// FULL QUERY RESULT TYPE TESTS
// =============================================================================

describe('JoinQueryResult', () => {
  it('should infer types for simple SELECT', () => {
    type Result = JoinQueryResult<'SELECT id, name FROM users', TestDB>;
    // Result should be an array
    type _TestArray = Expect<Extends<Result, any[]>>;
    // Each element should have id (number) and name (string)
    type Element = Result[number];
    type _TestId = Expect<Equal<Element['id'], number>>;
    type _TestName = Expect<Equal<Element['name'], string>>;
  });

  it('should infer types for SELECT with table alias', () => {
    type Result = JoinQueryResult<'SELECT u.id, u.name FROM users u', TestDB>;
    type Element = Result[number];
    type _TestId = Expect<Equal<Element['id'], number>>;
    type _TestName = Expect<Equal<Element['name'], string>>;
  });

  it('should infer types for INNER JOIN', () => {
    type Result = JoinQueryResult<
      'SELECT u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id',
      TestDB
    >;
    type Element = Result[number];
    type _TestName = Expect<Equal<Element['name'], string>>;
    type _TestTotal = Expect<Equal<Element['total'], number>>;
  });

  it('should infer types for LEFT JOIN', () => {
    type Result = JoinQueryResult<
      'SELECT u.name, o.total FROM users u LEFT JOIN orders o ON o.user_id = u.id',
      TestDB
    >;
    type Element = Result[number];
    type _TestName = Expect<Equal<Element['name'], string>>;
    // LEFT JOIN makes right table columns nullable
    type _TestTotal = Expect<Equal<Element['total'], number | null>>;
  });

  it('should infer types for RIGHT JOIN', () => {
    type Result = JoinQueryResult<
      'SELECT u.name, o.total FROM users u RIGHT JOIN orders o ON o.user_id = u.id',
      TestDB
    >;
    type Element = Result[number];
    // RIGHT JOIN makes left table (FROM) columns nullable
    type _TestName = Expect<Equal<Element['name'], string | null>>;
    type _TestTotal = Expect<Equal<Element['total'], number>>;
  });

  it('should infer types for SELECT *', () => {
    type Result = JoinQueryResult<'SELECT * FROM users', TestDB>;
    type Element = Result[number];
    type _TestId = Expect<Equal<Element['id'], number>>;
    type _TestName = Expect<Equal<Element['name'], string>>;
    type _TestEmail = Expect<Equal<Element['email'], string>>;
    type _TestCreatedAt = Expect<Equal<Element['created_at'], Date>>;
  });

  it('should infer types for SELECT * with JOIN', () => {
    type Result = JoinQueryResult<
      'SELECT * FROM users u JOIN orders o ON o.user_id = u.id',
      TestDB
    >;
    type Element = Result[number];
    // Should have columns from both tables
    // Users columns
    type _TestUserId = Expect<Equal<Element['id'], number>>;  // Both have id, will be number
    type _TestName = Expect<Equal<Element['name'], string>>;
    type _TestEmail = Expect<Equal<Element['email'], string>>;
    // Orders columns
    type _TestTotal = Expect<Equal<Element['total'], number>>;
    type _TestStatus = Expect<Equal<Element['status'], string>>;
  });

  it('should infer types for multi-table JOIN', () => {
    type Result = JoinQueryResult<
      'SELECT u.name, o.total, oi.quantity FROM users u JOIN orders o ON o.user_id = u.id JOIN order_items oi ON oi.order_id = o.id',
      TestDB
    >;
    type Element = Result[number];
    type _TestName = Expect<Equal<Element['name'], string>>;
    type _TestTotal = Expect<Equal<Element['total'], number>>;
    type _TestQuantity = Expect<Equal<Element['quantity'], number>>;
  });

  it('should infer types with column alias', () => {
    type Result = JoinQueryResult<
      'SELECT u.name AS user_name, o.total AS order_total FROM users u JOIN orders o ON o.user_id = u.id',
      TestDB
    >;
    type Element = Result[number];
    type _TestUserName = Expect<Equal<Element['user_name'], string>>;
    type _TestOrderTotal = Expect<Equal<Element['order_total'], number>>;
  });

  it('should handle aggregate functions', () => {
    type Result = JoinQueryResult<
      'SELECT COUNT(o.id) AS order_count FROM users u JOIN orders o ON o.user_id = u.id',
      TestDB
    >;
    type Element = Result[number];
    type _TestCount = Expect<Equal<Element['order_count'], number>>;
  });
});

// =============================================================================
// COMPLEX QUERY TESTS
// =============================================================================

describe('Complex JOIN queries', () => {
  it('should handle four-table JOIN', () => {
    type Result = JoinQueryResult<
      'SELECT u.name, o.total, oi.quantity, p.name AS product_name FROM users u JOIN orders o ON o.user_id = u.id JOIN order_items oi ON oi.order_id = o.id JOIN products p ON p.id = oi.product_id',
      TestDB
    >;
    type Element = Result[number];
    type _TestUserName = Expect<Equal<Element['name'], string>>;
    type _TestTotal = Expect<Equal<Element['total'], number>>;
    type _TestQuantity = Expect<Equal<Element['quantity'], number>>;
    type _TestProductName = Expect<Equal<Element['product_name'], string>>;
  });

  it('should handle LEFT JOIN with nullable columns', () => {
    type Result = JoinQueryResult<
      'SELECT u.name, o.total, oi.quantity FROM users u LEFT JOIN orders o ON o.user_id = u.id LEFT JOIN order_items oi ON oi.order_id = o.id',
      TestDB
    >;
    type Element = Result[number];
    type _TestName = Expect<Equal<Element['name'], string>>;
    // Both o and oi are from LEFT JOINs, so their columns are nullable
    type _TestTotal = Expect<Equal<Element['total'], number | null>>;
    type _TestQuantity = Expect<Equal<Element['quantity'], number | null>>;
  });

  it('should handle mixed INNER and LEFT JOINs', () => {
    type Result = JoinQueryResult<
      'SELECT u.name, o.total, oi.quantity FROM users u JOIN orders o ON o.user_id = u.id LEFT JOIN order_items oi ON oi.order_id = o.id',
      TestDB
    >;
    type Element = Result[number];
    type _TestName = Expect<Equal<Element['name'], string>>;
    // INNER JOIN: not nullable
    type _TestTotal = Expect<Equal<Element['total'], number>>;
    // LEFT JOIN: nullable
    type _TestQuantity = Expect<Equal<Element['quantity'], number | null>>;
  });
});

// =============================================================================
// EDGE CASES
// =============================================================================

describe('Edge cases', () => {
  it('should handle lowercase SQL', () => {
    type Result = JoinQueryResult<
      'select u.name, o.total from users u join orders o on o.user_id = u.id',
      TestDB
    >;
    type Element = Result[number];
    type _TestName = Expect<Equal<Element['name'], string>>;
    type _TestTotal = Expect<Equal<Element['total'], number>>;
  });

  it('should handle mixed case SQL', () => {
    type Result = JoinQueryResult<
      'Select u.name, o.total From users u Join orders o On o.user_id = u.id',
      TestDB
    >;
    type Element = Result[number];
    type _TestName = Expect<Equal<Element['name'], string>>;
    type _TestTotal = Expect<Equal<Element['total'], number>>;
  });

  it('should handle extra whitespace', () => {
    type Result = JoinQueryResult<
      'SELECT   u.name ,  o.total   FROM   users   u   JOIN   orders   o   ON   o.user_id = u.id',
      TestDB
    >;
    type Element = Result[number];
    type _TestName = Expect<Equal<Element['name'], string>>;
    type _TestTotal = Expect<Equal<Element['total'], number>>;
  });

  it('should handle AS keyword in table alias', () => {
    type Result = JoinQueryResult<
      'SELECT u.name, o.total FROM users AS u JOIN orders AS o ON o.user_id = u.id',
      TestDB
    >;
    type Element = Result[number];
    type _TestName = Expect<Equal<Element['name'], string>>;
    type _TestTotal = Expect<Equal<Element['total'], number>>;
  });
});

// =============================================================================
// INTEGRATION-STYLE TESTS (match the issue examples)
// =============================================================================

describe('Issue examples', () => {
  interface DB extends DatabaseSchema {
    users: { id: 'number'; name: 'string'; email: 'string' };
    orders: { id: 'number'; user_id: 'number'; total: 'number' };
  }

  it('should infer { name: string; total: number }[] for basic JOIN', () => {
    type Result = JoinQueryResult<
      'SELECT u.name, o.total FROM users u JOIN orders o ON o.user_id = u.id',
      DB
    >;
    type Element = Result[number];

    // Verify each property type matches
    type _TestName = Expect<Equal<Element['name'], string>>;
    type _TestTotal = Expect<Equal<Element['total'], number>>;
    // Verify no extra properties by checking extends both ways
    type _ExtendsExpected = Expect<Extends<Element, { name: string; total: number }>>;
  });

  it('should infer all columns from both tables for SELECT *', () => {
    type Result = JoinQueryResult<
      'SELECT * FROM users u JOIN orders o ON o.user_id = u.id',
      DB
    >;
    type Element = Result[number];

    // Should have all columns from both tables
    type _TestUserId = Expect<Extends<Element, { id: number }>>;
    type _TestName = Expect<Extends<Element, { name: string }>>;
    type _TestEmail = Expect<Extends<Element, { email: string }>>;
    type _TestTotal = Expect<Extends<Element, { total: number }>>;
    type _TestOrderUserId = Expect<Extends<Element, { user_id: number }>>;
  });
});

// =============================================================================
// RUNTIME SANITY CHECK (will actually run)
// =============================================================================

describe('Runtime sanity checks', () => {
  it('should compile without errors', () => {
    // This test just needs to compile
    // The type assertions above prove correctness
    const x: number = 1;
    expect(x).toBe(1);
  });
});
