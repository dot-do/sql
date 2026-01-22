# TypeScript Strict Guide

This guide documents TypeScript strict mode settings, patterns, and best practices for the DoSQL ecosystem.

## Table of Contents

1. [Recommended tsconfig.json Settings](#recommended-tsconfigjson-settings)
2. [Understanding Each Strict Flag](#understanding-each-strict-flag)
3. [Branded Types Pattern](#branded-types-pattern)
4. [Type-Safe Query Patterns](#type-safe-query-patterns)
5. [Error Handling with Discriminated Unions](#error-handling-with-discriminated-unions)
6. [Migration Guide: Loose to Strict](#migration-guide-loose-to-strict)
7. [Common Type Issues and Solutions](#common-type-issues-and-solutions)

---

## Recommended tsconfig.json Settings

### Full Strict Configuration

This is the recommended configuration for all packages in the DoSQL ecosystem:

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "lib": ["ES2022"],

    "strict": true,
    "declaration": true,
    "declarationMap": true,
    "sourceMap": true,
    "outDir": "./dist",
    "rootDir": "./src",
    "skipLibCheck": true,
    "esModuleInterop": true,

    "verbatimModuleSyntax": true,
    "noUncheckedIndexedAccess": true,
    "exactOptionalPropertyTypes": true,
    "noImplicitOverride": true,
    "useUnknownInCatchVariables": true
  },
  "include": ["src/**/*"],
  "exclude": ["node_modules", "dist", "**/*.test.ts"]
}
```

### Cloudflare Workers Configuration

For Cloudflare Workers projects, add the workers types:

```json
{
  "compilerOptions": {
    "types": ["@cloudflare/workers-types"]
  }
}
```

### Package Strictness Levels

| Setting | Minimum | Recommended | Maximum |
|---------|---------|-------------|---------|
| `strict` | Yes | Yes | Yes |
| `noUncheckedIndexedAccess` | No | Yes | Yes |
| `exactOptionalPropertyTypes` | No | Yes | Yes |
| `noImplicitOverride` | No | Yes | Yes |
| `useUnknownInCatchVariables` | No | Yes | Yes |
| `verbatimModuleSyntax` | No | Yes | Yes |
| `noPropertyAccessFromIndexSignature` | No | No | Yes |

---

## Understanding Each Strict Flag

### Core Strict Mode (`strict: true`)

The `strict` flag enables a family of type-checking options:

#### `strictNullChecks`

Ensures `null` and `undefined` are only assignable to their respective types and `unknown`.

```typescript
// Without strictNullChecks - DANGEROUS
function getUser(id: string): User {
  const user = users.get(id);
  return user; // Could be undefined!
}

// With strictNullChecks - SAFE
function getUser(id: string): User | undefined {
  return users.get(id);
}
```

#### `strictFunctionTypes`

Enables contravariant checking of function parameter types.

```typescript
// Without strictFunctionTypes - allows unsafe assignments
type Logger = (msg: string | number) => void;
const logString: (msg: string) => void = (msg) => console.log(msg);
const logger: Logger = logString; // Allowed but unsafe!

// With strictFunctionTypes - catches the error
// Error: Type '(msg: string) => void' is not assignable to type 'Logger'
```

#### `strictBindCallApply`

Ensures correct typing for `bind`, `call`, and `apply` methods.

```typescript
function greet(name: string, age: number) {
  return `Hello ${name}, you are ${age}`;
}

// Without - allows wrong arguments
greet.call(null, 'Alice', 'thirty'); // No error

// With - catches type errors
greet.call(null, 'Alice', 'thirty'); // Error: Argument of type 'string' is not assignable to parameter of type 'number'
```

#### `strictPropertyInitialization`

Ensures class properties are initialized in the constructor.

```typescript
// Without - allows uninitialized properties
class User {
  name: string;
  email: string;
}

// With - requires initialization
class User {
  name: string;
  email: string;

  constructor(name: string, email: string) {
    this.name = name;
    this.email = email;
  }
}

// Or use definite assignment assertion when initialized elsewhere
class User {
  name!: string; // Initialized in init() method
}
```

#### `noImplicitAny`

Prevents variables from implicitly having `any` type.

```typescript
// Without - implicit any
function process(data) {
  return data.value; // data is any
}

// With - requires explicit types
function process(data: { value: unknown }): unknown {
  return data.value;
}
```

#### `noImplicitThis`

Prevents `this` from having an implicit `any` type.

```typescript
// Without - this is any
const obj = {
  value: 42,
  getValue() {
    return function() {
      return this.value; // this is any
    };
  }
};

// With - requires explicit this type
const obj = {
  value: 42,
  getValue() {
    return function(this: typeof obj) {
      return this.value;
    };
  }
};
```

### Advanced Strict Settings

#### `noUncheckedIndexedAccess`

Adds `undefined` to index signature returns, preventing silent undefined access.

```typescript
interface StringMap {
  [key: string]: string;
}

const map: StringMap = { foo: 'bar' };

// Without noUncheckedIndexedAccess
const value: string = map['baz']; // No error, but value is undefined at runtime!

// With noUncheckedIndexedAccess
const value: string | undefined = map['baz']; // Requires null check
if (value !== undefined) {
  console.log(value.toUpperCase()); // Safe
}
```

**This is critical for DoSQL** where row data often uses index signatures:

```typescript
type Row = Record<string, unknown>;

function processRow(row: Row) {
  // Without noUncheckedIndexedAccess - runtime error if 'name' missing
  const name: unknown = row['name'];

  // With noUncheckedIndexedAccess - forces safe access
  const name = row['name'];
  if (name !== undefined && typeof name === 'string') {
    console.log(name.toUpperCase()); // Safe
  }
}
```

#### `exactOptionalPropertyTypes`

Distinguishes between `undefined` as a value and a missing property.

```typescript
interface User {
  name: string;
  nickname?: string;
}

// Without exactOptionalPropertyTypes
const user: User = { name: 'Alice', nickname: undefined }; // Allowed

// With exactOptionalPropertyTypes
const user: User = { name: 'Alice', nickname: undefined }; // Error!
const user: User = { name: 'Alice' }; // Must omit the property
```

#### `useUnknownInCatchVariables`

Types `catch` clause variables as `unknown` instead of `any`.

```typescript
// Without useUnknownInCatchVariables
try {
  riskyOperation();
} catch (error) {
  console.log(error.message); // error is any - no type safety
}

// With useUnknownInCatchVariables
try {
  riskyOperation();
} catch (error) {
  if (error instanceof Error) {
    console.log(error.message); // Safe after type guard
  }
}
```

#### `verbatimModuleSyntax`

Enforces consistent use of `import type` for type-only imports.

```typescript
// Without verbatimModuleSyntax - ambiguous
import { User, createUser } from './types';

// With verbatimModuleSyntax - explicit
import type { User } from './types';
import { createUser } from './types';
```

#### `noImplicitOverride`

Requires explicit `override` keyword when overriding base class members.

```typescript
class BaseEngine {
  execute(query: string): void {}
}

// Without noImplicitOverride
class CustomEngine extends BaseEngine {
  execute(query: string): void {} // Silently overrides
}

// With noImplicitOverride
class CustomEngine extends BaseEngine {
  override execute(query: string): void {} // Explicit override
}
```

---

## Branded Types Pattern

Branded types (also known as nominal types or opaque types) prevent accidentally mixing values that have the same underlying type but different semantic meaning.

### The Problem

```typescript
// Without branded types - dangerous
type UserId = string;
type OrderId = string;

function getUser(userId: UserId): User { ... }
function getOrder(orderId: OrderId): Order { ... }

const userId: UserId = 'user_123';
const orderId: OrderId = 'order_456';

// BUG: No error! TypeScript sees both as string
getUser(orderId);
getOrder(userId);
```

### The Solution: Branded Types

```typescript
// Declare unique symbols for each brand
declare const LSNBrand: unique symbol;
declare const TransactionIdBrand: unique symbol;
declare const ShardIdBrand: unique symbol;
declare const PageIdBrand: unique symbol;

// Create branded types using intersection
export type LSN = bigint & { readonly [LSNBrand]: never };
export type TransactionId = string & { readonly [TransactionIdBrand]: never };
export type ShardId = string & { readonly [ShardIdBrand]: never };
export type PageId = number & { readonly [PageIdBrand]: never };
```

### Factory Functions with Validation

Always create branded values through factory functions that validate inputs:

```typescript
/**
 * Create a branded LSN from a bigint value.
 * @param value - The bigint value for the LSN
 * @returns A branded LSN value
 * @throws {Error} If value is negative
 */
export function createLSN(value: bigint): LSN {
  if (value < 0n) {
    throw new Error(`LSN cannot be negative: ${value}`);
  }
  return value as LSN;
}

/**
 * Create a branded TransactionId from a string value.
 * @param value - The string value for the transaction ID
 * @returns A branded TransactionId value
 * @throws {Error} If value is empty
 */
export function createTransactionId(value: string): TransactionId {
  if (!value || value.length === 0) {
    throw new Error('TransactionId cannot be empty');
  }
  return value as TransactionId;
}

/**
 * Create a branded ShardId from a string value.
 * @param value - The string value for the shard ID
 * @returns A branded ShardId value
 * @throws {Error} If value is empty
 */
export function createShardId(value: string): ShardId {
  if (!value || value.length === 0) {
    throw new Error('ShardId cannot be empty');
  }
  return value as ShardId;
}

/**
 * Create a branded PageId from a number value.
 * @param value - The number value for the page ID
 * @returns A branded PageId value
 * @throws {Error} If value is negative or not an integer
 */
export function createPageId(value: number): PageId {
  if (value < 0) {
    throw new Error(`PageId cannot be negative: ${value}`);
  }
  if (!Number.isInteger(value)) {
    throw new Error(`PageId must be an integer: ${value}`);
  }
  return value as PageId;
}
```

### Type Guards

Provide type guards for runtime validation:

```typescript
/**
 * Type guard to check if a value is a valid LSN candidate.
 */
export function isValidLSN(value: unknown): value is bigint {
  return typeof value === 'bigint' && value >= 0n;
}

/**
 * Type guard to check if a value is a valid TransactionId candidate.
 */
export function isValidTransactionId(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

/**
 * Type guard to check if a value is a valid ShardId candidate.
 */
export function isValidShardId(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

/**
 * Type guard to check if a value is a valid PageId candidate.
 */
export function isValidPageId(value: unknown): value is number {
  return typeof value === 'number' && Number.isInteger(value) && value >= 0;
}
```

### Utility Functions

Provide utilities for common operations:

```typescript
/**
 * Compare two LSN values.
 * @returns negative if a < b, positive if a > b, 0 if equal
 */
export function compareLSN(a: LSN, b: LSN): number {
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}

/**
 * Get the raw bigint value from an LSN.
 * Useful for serialization or arithmetic operations.
 */
export function lsnValue(lsn: LSN): bigint {
  return lsn as bigint;
}

/**
 * Increment an LSN by a given amount.
 * @param lsn - The LSN to increment
 * @param amount - The amount to add (default: 1n)
 * @returns A new LSN with the incremented value
 */
export function incrementLSN(lsn: LSN, amount: bigint = 1n): LSN {
  return createLSN((lsn as bigint) + amount);
}
```

### Usage Examples

```typescript
// Type-safe usage
const lsn = createLSN(100n);
const txnId = createTransactionId('txn_abc123');
const shardId = createShardId('shard_001');

// Compile-time error: Cannot assign TransactionId to ShardId
processShardId(txnId); // Error!

// Branded types preserve underlying operations
console.log(lsn > 50n); // true - bigint comparison works
console.log(txnId.startsWith('txn_')); // true - string methods work

// Using in interfaces
interface TransactionRecord {
  id: TransactionId;
  startLsn: LSN;
  endLsn?: LSN;
  shard: ShardId;
}
```

---

## Type-Safe Query Patterns

### Generic Query Results

Use generics with defaults for flexible yet type-safe results:

```typescript
/**
 * SQL value types at runtime
 */
export type SQLValue = string | number | bigint | boolean | Date | null | Uint8Array;

/**
 * Generic query result interface
 */
export interface QueryResult<T = Record<string, SQLValue>> {
  rows: T[];
  columns: string[];
  columnTypes?: ColumnType[];
  rowsAffected: number;
  lastInsertRowid?: bigint;
  duration: number;
  lsn?: LSN;
}

// Usage with explicit type
interface User {
  id: number;
  name: string;
  email: string;
}

const result: QueryResult<User> = await db.query<User>(
  'SELECT id, name, email FROM users WHERE id = ?',
  [1]
);

// Type-safe access
const user = result.rows[0];
if (user) {
  console.log(user.name); // TypeScript knows this is string
}
```

### SQL Template Tag

Type-safe parameterized queries with template literals:

```typescript
/**
 * Tagged template result for parameterized queries
 */
export interface SqlTemplate {
  sql: string;
  parameters: SQLValue[];
}

/**
 * Tagged template function for SQL queries
 */
export function sql(strings: TemplateStringsArray, ...values: SQLValue[]): SqlTemplate {
  const parts: string[] = [];
  for (let i = 0; i < strings.length; i++) {
    parts.push(strings[i]);
    if (i < values.length) {
      parts.push(`$${i + 1}`);
    }
  }
  return {
    sql: parts.join(''),
    parameters: values,
  };
}

// Usage
const userId = 1;
const status = 'active';

const query = sql`SELECT * FROM users WHERE id = ${userId} AND status = ${status}`;
// query.sql = 'SELECT * FROM users WHERE id = $1 AND status = $2'
// query.parameters = [1, 'active']
```

### Type-Safe Engine Interface

```typescript
/**
 * DoSQL execution engine interface
 */
export interface Engine {
  /** Execute a SQL query with full result metadata */
  execute<T = Row>(query: string | SqlTemplate): Promise<QueryResult<T>>;

  /** Execute a SQL query and return just the rows */
  query<T = Row>(query: string | SqlTemplate): Promise<T[]>;

  /** Execute a SQL query and return the first row */
  queryOne<T = Row>(query: string | SqlTemplate): Promise<T | null>;

  /** Prepare a query plan (for analysis/debugging) */
  prepare(query: string): Promise<QueryPlan>;

  /** Explain the query plan */
  explain(query: string): Promise<string>;
}
```

---

## Error Handling with Discriminated Unions

### Basic Pattern

Discriminated unions use a common property (the discriminant) to distinguish between variants:

```typescript
// Expression types using discriminated union
export type Expression =
  | ColumnRef
  | Literal
  | BinaryExpr
  | UnaryExpr
  | FunctionCall
  | AggregateExpr
  | CaseExpr
  | SubqueryExpr;

export interface ColumnRef {
  type: 'columnRef';
  table?: string;
  column: string;
}

export interface Literal {
  type: 'literal';
  value: SQLValue;
  dataType: 'string' | 'number' | 'bigint' | 'boolean' | 'date' | 'bytes' | 'null';
}

export interface BinaryExpr {
  type: 'binary';
  op: ComparisonOp | ArithmeticOp | LogicalOp;
  left: Expression;
  right: Expression;
}

// Type-safe handling with exhaustiveness checking
function evaluateExpression(expr: Expression): SQLValue {
  switch (expr.type) {
    case 'columnRef':
      return resolveColumn(expr.table, expr.column);
    case 'literal':
      return expr.value;
    case 'binary':
      return evaluateBinary(expr.op, expr.left, expr.right);
    case 'unary':
      return evaluateUnary(expr.op, expr.operand);
    case 'function':
      return callFunction(expr.name, expr.args);
    case 'aggregate':
      return computeAggregate(expr.function, expr.arg);
    case 'case':
      return evaluateCase(expr.when, expr.else);
    case 'subquery':
      return executeSubquery(expr.plan);
    default:
      // Exhaustiveness check - TypeScript error if a case is missing
      const _exhaustive: never = expr;
      throw new Error(`Unknown expression type: ${(_exhaustive as Expression).type}`);
  }
}
```

### Result Types for Error Handling

Use discriminated unions for operations that can fail:

```typescript
// Result type pattern
export type Result<T, E = Error> =
  | { success: true; value: T }
  | { success: false; error: E };

// Query result with structured errors
export type QueryOutcome<T> =
  | { type: 'success'; result: QueryResult<T> }
  | { type: 'syntax_error'; error: SQLSyntaxError }
  | { type: 'constraint_error'; error: ConstraintViolationError }
  | { type: 'timeout'; duration: number }
  | { type: 'cancelled' };

// Usage
async function executeQuery<T>(sql: string): Promise<QueryOutcome<T>> {
  try {
    const result = await engine.execute<T>(sql);
    return { type: 'success', result };
  } catch (error) {
    if (error instanceof SQLSyntaxError) {
      return { type: 'syntax_error', error };
    }
    if (error instanceof ConstraintViolationError) {
      return { type: 'constraint_error', error };
    }
    throw error; // Re-throw unexpected errors
  }
}

// Type-safe handling
const outcome = await executeQuery<User>('SELECT * FROM users');

switch (outcome.type) {
  case 'success':
    console.log(`Found ${outcome.result.rows.length} users`);
    break;
  case 'syntax_error':
    console.log(`Syntax error: ${outcome.error.format()}`);
    break;
  case 'constraint_error':
    console.log(`Constraint violation: ${outcome.error.message}`);
    break;
  case 'timeout':
    console.log(`Query timed out after ${outcome.duration}ms`);
    break;
  case 'cancelled':
    console.log('Query was cancelled');
    break;
}
```

### Type Guards for Discriminated Unions

```typescript
// Type guards for RPC messages
export interface CDCBatchMessage {
  type: 'cdc_batch';
  events: CDCEvent[];
}

export interface AckMessage {
  type: 'ack';
  sequence: number;
}

export interface ErrorMessage {
  type: 'error';
  code: string;
  message: string;
}

export type RpcMessage = CDCBatchMessage | AckMessage | ErrorMessage;

// Type guard functions
export function isCDCBatchMessage(msg: RpcMessage): msg is CDCBatchMessage {
  return msg.type === 'cdc_batch';
}

export function isAckMessage(msg: RpcMessage): msg is AckMessage {
  return msg.type === 'ack';
}

export function isErrorMessage(msg: RpcMessage): msg is ErrorMessage {
  return msg.type === 'error';
}

// Usage
function handleMessage(msg: RpcMessage): void {
  if (isCDCBatchMessage(msg)) {
    // TypeScript knows msg is CDCBatchMessage
    processCDCEvents(msg.events);
  } else if (isAckMessage(msg)) {
    // TypeScript knows msg is AckMessage
    confirmAcknowledgment(msg.sequence);
  } else if (isErrorMessage(msg)) {
    // TypeScript knows msg is ErrorMessage
    handleError(msg.code, msg.message);
  }
}
```

---

## Migration Guide: Loose to Strict

### Phase 1: Enable Core Strict Mode

**Step 1:** Enable `strict: true` in tsconfig.json

```json
{
  "compilerOptions": {
    "strict": true
  }
}
```

**Step 2:** Fix `strictNullChecks` errors

Common patterns:

```typescript
// Before
function getUser(id: string): User {
  return userMap[id];
}

// After - Option A: Return union type
function getUser(id: string): User | undefined {
  return userMap[id];
}

// After - Option B: Throw if not found
function getUser(id: string): User {
  const user = userMap[id];
  if (!user) {
    throw new Error(`User not found: ${id}`);
  }
  return user;
}
```

**Step 3:** Fix `noImplicitAny` errors

```typescript
// Before
function process(data) {
  return data.value;
}

// After
function process(data: { value: string }): string {
  return data.value;
}

// Or if type is truly unknown
function process(data: unknown): unknown {
  if (typeof data === 'object' && data !== null && 'value' in data) {
    return (data as { value: unknown }).value;
  }
  return undefined;
}
```

### Phase 2: Enable Advanced Strict Settings

**Step 4:** Enable `noUncheckedIndexedAccess`

```json
{
  "compilerOptions": {
    "noUncheckedIndexedAccess": true
  }
}
```

Fix pattern:

```typescript
// Before
const row: Record<string, unknown> = { name: 'Alice' };
const name: unknown = row['name']; // Assumed to exist

// After
const row: Record<string, unknown> = { name: 'Alice' };
const name = row['name']; // Type is unknown | undefined
if (name !== undefined) {
  // Safe to use name here
}

// Or use non-null assertion when you're certain (use sparingly)
const name = row['name']!; // Asserts value exists
```

**Step 5:** Enable `useUnknownInCatchVariables`

```json
{
  "compilerOptions": {
    "useUnknownInCatchVariables": true
  }
}
```

Fix pattern:

```typescript
// Before
try {
  riskyOperation();
} catch (error) {
  console.log(error.message);
}

// After
try {
  riskyOperation();
} catch (error) {
  if (error instanceof Error) {
    console.log(error.message);
  } else {
    console.log('Unknown error:', error);
  }
}

// Or create a helper function
function getErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
}
```

**Step 6:** Enable `exactOptionalPropertyTypes`

```json
{
  "compilerOptions": {
    "exactOptionalPropertyTypes": true
  }
}
```

Fix pattern:

```typescript
interface Config {
  timeout?: number;
}

// Before
const config: Config = { timeout: undefined }; // Explicit undefined

// After - Option A: Omit the property
const config: Config = {}; // Property is absent

// After - Option B: Use union type if undefined is meaningful
interface Config {
  timeout?: number | undefined;
}
const config: Config = { timeout: undefined }; // Now valid
```

**Step 7:** Enable `verbatimModuleSyntax`

```json
{
  "compilerOptions": {
    "verbatimModuleSyntax": true
  }
}
```

Fix pattern:

```typescript
// Before
import { User, createUser } from './types';

// After - separate type and value imports
import type { User } from './types';
import { createUser } from './types';

// Or combined syntax
import { type User, createUser } from './types';
```

### Phase 3: Incremental Migration Strategy

For large codebases, use incremental migration:

```json
{
  "compilerOptions": {
    "strict": true,
    "noUncheckedIndexedAccess": true
  },
  "include": ["src/**/*"],
  "exclude": ["src/legacy/**/*"]
}
```

Create a separate tsconfig for legacy code:

```json
// tsconfig.legacy.json
{
  "extends": "./tsconfig.json",
  "compilerOptions": {
    "strict": false,
    "noUncheckedIndexedAccess": false
  },
  "include": ["src/legacy/**/*"]
}
```

---

## Common Type Issues and Solutions

### Issue 1: Array Index Access Returns `undefined`

**Problem:** With `noUncheckedIndexedAccess`, array access returns `T | undefined`.

```typescript
const users: User[] = [{ name: 'Alice' }];
const first: User | undefined = users[0]; // Type includes undefined
```

**Solutions:**

```typescript
// Solution 1: Use optional chaining and nullish coalescing
const name = users[0]?.name ?? 'Unknown';

// Solution 2: Check before access
if (users.length > 0) {
  const first = users[0]; // Still T | undefined, use non-null assertion
  const name = first!.name; // Assert it exists after length check
}

// Solution 3: Use array methods that narrow types
const first = users.find((_, i) => i === 0);
if (first) {
  console.log(first.name); // TypeScript knows first is User
}

// Solution 4: Use at() with undefined check
const first = users.at(0);
if (first !== undefined) {
  console.log(first.name);
}
```

### Issue 2: Object Index Signature

**Problem:** Index signatures return `T | undefined` with `noUncheckedIndexedAccess`.

```typescript
interface Cache {
  [key: string]: CacheEntry;
}
const cache: Cache = {};
const entry: CacheEntry | undefined = cache['key'];
```

**Solutions:**

```typescript
// Solution 1: Use Map instead of index signature
const cache = new Map<string, CacheEntry>();
const entry = cache.get('key'); // Returns CacheEntry | undefined (expected)

// Solution 2: Use explicit get method
interface Cache {
  entries: Record<string, CacheEntry>;
  get(key: string): CacheEntry | undefined;
  set(key: string, value: CacheEntry): void;
}

// Solution 3: Use non-null assertion when you know key exists
const entry = cache['known-key']!;
```

### Issue 3: Catch Variable Typing

**Problem:** `catch` variable is `unknown` with `useUnknownInCatchVariables`.

```typescript
try {
  await fetch(url);
} catch (error) {
  // error is unknown
}
```

**Solution:**

```typescript
// Create a standardized error handler
function handleError(error: unknown): Error {
  if (error instanceof Error) {
    return error;
  }
  if (typeof error === 'string') {
    return new Error(error);
  }
  return new Error(`Unknown error: ${JSON.stringify(error)}`);
}

try {
  await fetch(url);
} catch (error) {
  const err = handleError(error);
  console.log(err.message);
}
```

### Issue 4: Optional Property vs Undefined Value

**Problem:** With `exactOptionalPropertyTypes`, `undefined` is not assignable to optional properties.

```typescript
interface Options {
  timeout?: number;
}

function configure(opts: Partial<Options>): void {
  const timeout = opts.timeout; // number | undefined
}

// Error: Cannot assign undefined to timeout
const opts: Options = { timeout: undefined };
```

**Solutions:**

```typescript
// Solution 1: Omit the property instead of setting undefined
const opts: Options = {};

// Solution 2: Use union type explicitly
interface Options {
  timeout?: number | undefined;
}
const opts: Options = { timeout: undefined }; // Now valid

// Solution 3: Use a function to handle undefined
function setOption<K extends keyof Options>(
  opts: Options,
  key: K,
  value: Options[K] | undefined
): Options {
  if (value === undefined) {
    const { [key]: _, ...rest } = opts;
    return rest as Options;
  }
  return { ...opts, [key]: value };
}
```

### Issue 5: Generic Constraints with Index Signatures

**Problem:** Generic types with index access need careful handling.

```typescript
function getValue<T extends Record<string, unknown>>(obj: T, key: string): unknown {
  return obj[key]; // Returns unknown | undefined with noUncheckedIndexedAccess
}
```

**Solution:**

```typescript
// Use keyof constraint for known keys
function getValue<T extends object, K extends keyof T>(obj: T, key: K): T[K] {
  return obj[key];
}

// For dynamic keys, explicitly type the return
function getValue<T extends Record<string, unknown>>(
  obj: T,
  key: string
): T[string] | undefined {
  return obj[key];
}
```

### Issue 6: Type Narrowing Not Working

**Problem:** TypeScript doesn't narrow types as expected.

```typescript
interface A { type: 'a'; valueA: string; }
interface B { type: 'b'; valueB: number; }
type AB = A | B;

function process(item: AB) {
  if (item.type === 'a') {
    // item.valueA should work
  }
}
```

**Solutions:**

```typescript
// Ensure discriminant is a literal type
interface A { readonly type: 'a'; valueA: string; }
interface B { readonly type: 'b'; valueB: number; }

// Use const assertion for object literals
const item = { type: 'a' as const, valueA: 'test' };

// Use type predicates for complex narrowing
function isA(item: AB): item is A {
  return item.type === 'a';
}

function process(item: AB) {
  if (isA(item)) {
    console.log(item.valueA); // TypeScript knows this is A
  }
}
```

### Issue 7: Third-Party Library Types

**Problem:** Third-party libraries may have loose types.

**Solutions:**

```typescript
// Solution 1: Create wrapper functions with stricter types
import { unstrictFunction } from 'some-library';

function strictWrapper(input: string): string {
  const result: unknown = unstrictFunction(input);
  if (typeof result !== 'string') {
    throw new Error('Unexpected result type');
  }
  return result;
}

// Solution 2: Use declaration merging to augment types
declare module 'some-library' {
  export function unstrictFunction(input: string): string;
}

// Solution 3: Create local type definitions
// types/some-library.d.ts
declare module 'some-library' {
  export interface StricterTypes {
    value: string;
  }
}
```

---

## Best Practices Summary

1. **Always enable `strict: true`** as the baseline
2. **Prefer `unknown` over `any`** - it forces explicit type narrowing
3. **Use branded types** for domain identifiers to prevent mixing
4. **Use discriminated unions** for type-safe handling of variants
5. **Create factory functions** with validation for branded types
6. **Provide type guards** for runtime type checking
7. **Enable `noUncheckedIndexedAccess`** to catch undefined access bugs
8. **Use `useUnknownInCatchVariables`** for safe error handling
9. **Separate type imports** with `verbatimModuleSyntax`
10. **Add exhaustiveness checks** in switch statements for discriminated unions

---

## Related Documentation

- [Error Codes Reference](/docs/ERROR_CODES.md)
- [TypeScript Review](/docs/TYPESCRIPT_REVIEW.md)
- [Architecture Overview](/docs/ARCHITECTURE_REVIEW.md)

---

*Last updated: 2026-01-22*
