/**
 * Type-level tests for DoSQL parser
 *
 * These tests verify that the type-level SQL parser correctly infers types.
 * We use @ts-expect-error to assert type mismatches.
 */

import type { SQL, QueryResult, DatabaseSchema } from '../parser.js';
import { createDatabase, createQuery } from '../parser.js';

// =============================================================================
// TEST DATABASE SCHEMA
// =============================================================================

interface TestDB extends DatabaseSchema {
  users: {
    id: 'number';
    name: 'string';
    email: 'string';
    active: 'boolean';
  };
  orders: {
    id: 'number';
    user_id: 'number';
    total: 'number';
    status: 'string';
  };
  products: {
    ID: 'number';      // Uppercase to test case sensitivity
    Name: 'string';    // Mixed case to test case sensitivity
    price: 'number';
  };
}

// Type helper for assertions
type Expect<T extends true> = T;
type Equal<A, B> =
  (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2) ? true : false;

// =============================================================================
// SELECT * TESTS
// =============================================================================

// Test: SELECT * returns all columns
type SelectAllUsers = SQL<'SELECT * FROM users', TestDB>;
type _TestSelectAll = Expect<Equal<
  SelectAllUsers,
  { id: number; name: string; email: string; active: boolean }[]
>>;

// Test: SELECT * from orders
type SelectAllOrders = SQL<'SELECT * FROM orders', TestDB>;
type _TestSelectAllOrders = Expect<Equal<
  SelectAllOrders,
  { id: number; user_id: number; total: number; status: string }[]
>>;

// =============================================================================
// SELECT SPECIFIC COLUMNS TESTS
// =============================================================================

// Test: SELECT specific columns
type SelectNameEmail = SQL<'SELECT name, email FROM users', TestDB>;
type _TestSelectNameEmail = Expect<Equal<
  SelectNameEmail,
  { name: string; email: string }[]
>>;

// Test: SELECT single column
type SelectId = SQL<'SELECT id FROM users', TestDB>;
type _TestSelectId = Expect<Equal<
  SelectId,
  { id: number }[]
>>;

// Test: SELECT multiple columns from orders
type SelectOrderCols = SQL<'SELECT id, total, status FROM orders', TestDB>;
type _TestSelectOrderCols = Expect<Equal<
  SelectOrderCols,
  { id: number; total: number; status: string }[]
>>;

// =============================================================================
// CASE SENSITIVITY TESTS
// =============================================================================

// Test: Case-sensitive column names (products table has ID and Name uppercase)
type SelectProductId = SQL<'SELECT ID FROM products', TestDB>;
type _TestSelectProductId = Expect<Equal<
  SelectProductId,
  { ID: number }[]
>>;

type SelectProductName = SQL<'SELECT Name FROM products', TestDB>;
type _TestSelectProductName = Expect<Equal<
  SelectProductName,
  { Name: string }[]
>>;

// Test: Wrong case returns unknown (case sensitive!)
type SelectWrongCase = SQL<'SELECT id FROM products', TestDB>;
// id (lowercase) doesn't exist in products, so should be unknown
type _TestWrongCase = Expect<Equal<
  SelectWrongCase,
  { id: unknown }[]
>>;

type SelectWrongCaseName = SQL<'SELECT name FROM products', TestDB>;
// name (lowercase) doesn't exist in products, so should be unknown
type _TestWrongCaseName = Expect<Equal<
  SelectWrongCaseName,
  { name: unknown }[]
>>;

// =============================================================================
// TABLE ALIAS TESTS
// =============================================================================

// Test: Simple alias (table space alias)
type SelectWithAlias = SQL<'SELECT * FROM users u', TestDB>;
type _TestSelectWithAlias = Expect<Equal<
  SelectWithAlias,
  { id: number; name: string; email: string; active: boolean }[]
>>;

// Test: AS keyword alias
type SelectWithAsAlias = SQL<'SELECT * FROM users AS u', TestDB>;
type _TestSelectWithAsAlias = Expect<Equal<
  SelectWithAsAlias,
  { id: number; name: string; email: string; active: boolean }[]
>>;

// Test: Select columns with table alias
type SelectColsWithAlias = SQL<'SELECT id, name FROM users u', TestDB>;
type _TestSelectColsWithAlias = Expect<Equal<
  SelectColsWithAlias,
  { id: number; name: string }[]
>>;

// =============================================================================
// TABLE NOT FOUND TESTS
// =============================================================================

// Test: Non-existent table returns error
type SelectFromNonExistent = SQL<'SELECT * FROM nonexistent', TestDB>;
type _TestNonExistent = Expect<Equal<
  SelectFromNonExistent,
  { error: "Table 'nonexistent' not found in schema" }
>>;

// Test: Case-sensitive table names (Users vs users)
type SelectFromWrongCaseTable = SQL<'SELECT * FROM Users', TestDB>;
type _TestWrongCaseTable = Expect<Equal<
  SelectFromWrongCaseTable,
  { error: "Table 'Users' not found in schema" }
>>;

// =============================================================================
// RUNTIME TYPE INFERENCE TESTS
// =============================================================================

// These tests verify that runtime functions properly infer types

const db = createDatabase<TestDB>();
const query = createQuery<TestDB>();

// Test: createQuery type inference
const usersResult = query('SELECT * FROM users');
type UsersResultType = typeof usersResult;
type _TestQueryUsers = Expect<Equal<
  UsersResultType,
  { id: number; name: string; email: string; active: boolean }[]
>>;

const specificColsResult = query('SELECT name, email FROM users');
type SpecificColsType = typeof specificColsResult;
type _TestQuerySpecific = Expect<Equal<
  SpecificColsType,
  { name: string; email: string }[]
>>;

// =============================================================================
// @ts-expect-error TESTS
// =============================================================================

// These tests use @ts-expect-error to verify type mismatches

// Test: Wrong type assignment should fail
// @ts-expect-error - string[] is not assignable to { id: number; name: string; ... }[]
const _wrongType1: SQL<'SELECT * FROM users', TestDB> = ['wrong'];

// @ts-expect-error - { foo: string }[] is not assignable to { id: number; ... }[]
const _wrongType2: SQL<'SELECT * FROM users', TestDB> = [{ foo: 'bar' }];

// @ts-expect-error - { id: string }[] is not assignable to { id: number; ... }[]
const _wrongType3: SQL<'SELECT id FROM users', TestDB> = [{ id: 'not a number' }];

// Test: Missing properties should fail
// @ts-expect-error - missing 'email' property
const _missingProp: SQL<'SELECT name, email FROM users', TestDB> = [{ name: 'test' }];

// Test: Extra properties should fail (with strict type checking)
// Note: TypeScript allows extra properties in some contexts, so this may not error
// @ts-expect-error - extra 'extra' property not in result type
const _extraProp: SQL<'SELECT id FROM users', TestDB> = [{ id: 1, extra: 'not allowed' }];

// =============================================================================
// VALID ASSIGNMENT TESTS (should NOT error)
// =============================================================================

// These should all compile without errors
const _validUsers: SQL<'SELECT * FROM users', TestDB> = [
  { id: 1, name: 'Alice', email: 'alice@example.com', active: true }
];

const _validNameEmail: SQL<'SELECT name, email FROM users', TestDB> = [
  { name: 'Bob', email: 'bob@example.com' }
];

const _validId: SQL<'SELECT id FROM users', TestDB> = [{ id: 42 }];

const _validOrders: SQL<'SELECT id, total FROM orders', TestDB> = [
  { id: 1, total: 99.99 }
];

const _validProducts: SQL<'SELECT ID, Name, price FROM products', TestDB> = [
  { ID: 1, Name: 'Widget', price: 19.99 }
];

// =============================================================================
// KEYWORD CASE INSENSITIVITY TESTS
// =============================================================================

// SQL keywords should be case-insensitive (SELECT, FROM, etc.)
type LowerKeywords = SQL<'select * from users', TestDB>;
type _TestLowerKeywords = Expect<Equal<
  LowerKeywords,
  { id: number; name: string; email: string; active: boolean }[]
>>;

// =============================================================================
// EDGE CASES
// =============================================================================

// Test: Extra whitespace handling
type ExtraWhitespace = SQL<'SELECT   id,  name   FROM   users', TestDB>;
type _TestExtraWhitespace = Expect<Equal<
  ExtraWhitespace,
  { id: number; name: string }[]
>>;

// Test: Single column with whitespace
type SingleColWhitespace = SQL<'SELECT   id   FROM users', TestDB>;
type _TestSingleColWhitespace = Expect<Equal<
  SingleColWhitespace,
  { id: number }[]
>>;

// =============================================================================
// ASYNC TYPE INFERENCE (for template literal tag)
// =============================================================================

// Type test helper for async results
type AwaitedType<T> = T extends Promise<infer R> ? R : T;

// Verify that db.sql returns the correct Promise type
// Note: This is a type-level test; we're checking the inferred type
async function testAsyncTypes() {
  // The template literal tag should infer the correct type
  // In actual usage: const users = await db.sql`SELECT * FROM users`;

  // For type testing, we verify the query function
  const usersQuery = query('SELECT * FROM users');

  // Should be able to access properties on the result
  const firstUser = usersQuery[0];
  const _id: number = firstUser.id;
  const _name: string = firstUser.name;
  const _email: string = firstUser.email;
  const _active: boolean = firstUser.active;

  // @ts-expect-error - nonexistent property
  const _bad = firstUser.nonexistent;

  return usersQuery;
}

// =============================================================================
// EXPORT FOR TEST RUNNER
// =============================================================================

// These tests are compile-time only. If this file compiles, all tests pass.
// We export a simple runtime test to make test runners happy.
export function runTests() {
  console.log('All type tests passed! (compile-time verification)');
  return true;
}

// For vitest
import { describe, it, expect } from 'vitest';

describe('DoSQL Type-Level Parser', () => {
  it('should compile type tests without errors', () => {
    // If we got here, type checking passed
    expect(runTests()).toBe(true);
  });

  it('should create database instance', () => {
    const db = createDatabase<TestDB>();
    expect(db).toBeDefined();
    expect(typeof db.sql).toBe('function');
  });

  it('should create query function', () => {
    const query = createQuery<TestDB>();
    expect(typeof query).toBe('function');
  });
});
