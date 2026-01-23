# Error Handling Patterns - DoSQL Codebase

This document describes the standardized error handling patterns that should be used across all DoSQL, sql.do, lake.do, and dolake packages.

## Table of Contents

- [Base Error Class Hierarchy](#base-error-class-hierarchy)
- [Error Categories](#error-categories)
- [Standard Error Properties](#standard-error-properties)
- [When to Use Throw vs Result](#when-to-use-throw-vs-result)
- [Error Wrapping (Cause Chains)](#error-wrapping-cause-chains)
- [Creating New Error Classes](#creating-new-error-classes)
- [Migration Guide for Non-Compliant Errors](#migration-guide-for-non-compliant-errors)
- [Current Violations](#current-violations)

---

## Base Error Class Hierarchy

### DoSQL Core Package (`packages/dosql/src/errors/`)

The canonical base error class hierarchy is defined in `packages/dosql/src/errors/base.ts`:

```
Error (JavaScript built-in)
  |
  +-- DoSQLError (abstract base class)
        |
        +-- DatabaseError
        |     +-- ConnectionError
        |     +-- ReadOnlyError
        |
        +-- StatementError
        |     +-- PrepareError
        |     +-- ExecuteError
        |
        +-- BindingError
        |     +-- MissingParameterError
        |     +-- TypeCoercionError
        |
        +-- SQLSyntaxError
        |     +-- UnexpectedTokenError
        |     +-- UnexpectedEOFError
        |     +-- MissingKeywordError
        |     +-- InvalidIdentifierError
        |
        +-- AggregateDoSQLError
        +-- GenericDoSQLError
```

### Lake.do Package (`packages/lake.do/src/errors.ts`)

```
Error (JavaScript built-in)
  |
  +-- LakeError (base class)
        |
        +-- ConnectionError
        +-- QueryError
        +-- TimeoutError
```

### DoLake Package (`packages/dolake/src/types.ts`)

```
Error (JavaScript built-in)
  |
  +-- DoLakeError (base class)
        |
        +-- VersionMismatchError
        +-- ConnectionError
        +-- BufferOverflowError
        +-- FlushError
        +-- ParquetWriteError
        +-- IcebergError
```

### SQL.do Package (`packages/sql.do/src/errors.ts`)

```
Error (JavaScript built-in)
  |
  +-- SQLError (standalone, doesn't extend a common base)
  +-- ConnectionError (standalone)
  +-- TimeoutError (standalone)
  +-- MessageParseError (standalone)
```

---

## Error Categories

All errors should be categorized using the `ErrorCategory` enum from `packages/dosql/src/errors/base.ts`:

```typescript
export enum ErrorCategory {
  /** Connection/networking errors */
  CONNECTION = 'CONNECTION',
  /** Query execution errors */
  EXECUTION = 'EXECUTION',
  /** Input validation errors */
  VALIDATION = 'VALIDATION',
  /** Resource errors (not found, quota exceeded) */
  RESOURCE = 'RESOURCE',
  /** Conflict errors (deadlock, serialization) */
  CONFLICT = 'CONFLICT',
  /** Timeout errors */
  TIMEOUT = 'TIMEOUT',
  /** Internal errors (bugs, unexpected states) */
  INTERNAL = 'INTERNAL',
}
```

---

## Standard Error Properties

All error classes extending the base error classes MUST include:

### Required Properties

| Property | Type | Description |
|----------|------|-------------|
| `code` | `string` | Machine-readable error code (e.g., `DB_CLOSED`, `STMT_SYNTAX`) |
| `category` | `ErrorCategory` | High-level error category |
| `timestamp` | `number` | Unix timestamp when error occurred |

### Optional Properties

| Property | Type | Description |
|----------|------|-------------|
| `context` | `ErrorContext` | Additional debugging context |
| `recoveryHint` | `string` | Actionable suggestion for developers |
| `cause` | `Error` | Original error that caused this one |
| `details` | `unknown` | Structured error details |
| `retryable` | `boolean` | Whether the operation can be retried |

### Required Methods

| Method | Return Type | Description |
|--------|-------------|-------------|
| `isRetryable()` | `boolean` | Whether this error is safe to retry |
| `toUserMessage()` | `string` | User-friendly error message |
| `toJSON()` | `SerializedError` | Serialized error for RPC |
| `toLogEntry()` | `ErrorLogEntry` | Structured logging format |

---

## When to Use Throw vs Result

### Use `throw` When:

1. **Exceptional conditions** - The operation cannot proceed meaningfully
2. **Programmer errors** - Invalid arguments, violated invariants
3. **Infrastructure failures** - Connection errors, timeouts
4. **External API boundaries** - Public methods consumed by users

```typescript
// Good: Throwing for validation errors
function prepareStatement(sql: string): PreparedStatement {
  if (!sql || typeof sql !== 'string') {
    throw new BindingError(
      BindingErrorCode.INVALID_TYPE,
      'SQL must be a non-empty string'
    );
  }
  // ...
}
```

### Use `Result<T, E>` When:

1. **Expected failures** - Operations that commonly fail (parsing, validation)
2. **Performance-critical paths** - Where exception overhead matters
3. **Internal module boundaries** - Within a module where failure is part of normal flow
4. **Batch operations** - Where partial success is meaningful

```typescript
// Good: Result for parsing that may fail
type ParseResult<T> = { ok: true; value: T } | { ok: false; error: string };

function parseSQL(sql: string): ParseResult<AST> {
  // Parsing can fail as part of normal operation
  const tokens = tokenize(sql);
  if (tokens.length === 0) {
    return { ok: false, error: 'Empty SQL statement' };
  }
  // ...
}
```

### Consistency Guidelines

| Package | Pattern | Rationale |
|---------|---------|-----------|
| `dosql` | Both (throw at boundaries, Result internally) | Complex module with internal parsing |
| `sql.do` | Throw | Client library, exceptions are expected |
| `lake.do` | Throw | Client library, exceptions are expected |
| `dolake` | Throw | Server-side, errors are exceptional |

---

## Error Wrapping (Cause Chains)

### Always Wrap Underlying Errors

When catching an error and re-throwing, always preserve the original error as the `cause`:

```typescript
// Good: Preserving error chain
try {
  await storage.write(path, data);
} catch (error) {
  throw new FSXError(
    FSXErrorCode.WRITE_FAILED,
    `Failed to write to ${path}`,
    { cause: error }  // Preserve original error
  );
}
```

```typescript
// Bad: Losing original error
try {
  await storage.write(path, data);
} catch (error) {
  throw new Error(`Write failed: ${error.message}`);  // Original stack lost
}
```

### Accessing Cause Chain

```typescript
function getFullErrorChain(error: Error): Error[] {
  const chain: Error[] = [error];
  let current = error.cause as Error | undefined;
  while (current) {
    chain.push(current);
    current = current.cause as Error | undefined;
  }
  return chain;
}
```

---

## Creating New Error Classes

### Step 1: Choose the Right Base Class

- For `dosql` package: Extend `DoSQLError`
- For `lake.do` package: Extend `LakeError`
- For `dolake` package: Extend `DoLakeError`
- For `sql.do` package: Extend `Error` (current pattern) or consider extending a shared base

### Step 2: Define Error Code

Add error code to the appropriate enum in `packages/dosql/src/errors/codes.ts`:

```typescript
export enum MyModuleErrorCode {
  /** Description of error condition */
  MY_ERROR = 'MODULE_MY_ERROR',
}
```

### Step 3: Implement the Error Class

```typescript
import {
  DoSQLError,
  ErrorCategory,
  registerErrorClass,
  type ErrorContext,
  type SerializedError,
} from './base.js';
import { MyModuleErrorCode } from './codes.js';

/**
 * Error thrown when [describe condition]
 *
 * @example
 * ```typescript
 * try {
 *   // operation
 * } catch (error) {
 *   if (error instanceof MyModuleError) {
 *     console.log(error.code);  // 'MODULE_MY_ERROR'
 *   }
 * }
 * ```
 */
export class MyModuleError extends DoSQLError {
  readonly code: MyModuleErrorCode;
  readonly category = ErrorCategory.EXECUTION;  // Choose appropriate category

  constructor(
    code: MyModuleErrorCode,
    message: string,
    options?: { cause?: Error; context?: ErrorContext }
  ) {
    super(message, options);
    this.name = 'MyModuleError';
    this.code = code;

    this.setRecoveryHint();
  }

  private setRecoveryHint(): void {
    switch (this.code) {
      case MyModuleErrorCode.MY_ERROR:
        this.recoveryHint = 'Describe how to fix this';
        break;
    }
  }

  isRetryable(): boolean {
    return false;  // Override based on error codes
  }

  toUserMessage(): string {
    switch (this.code) {
      case MyModuleErrorCode.MY_ERROR:
        return 'User-friendly description';
      default:
        return this.message;
    }
  }

  static fromJSON(json: SerializedError): MyModuleError {
    return new MyModuleError(
      json.code as MyModuleErrorCode,
      json.message,
      { context: json.context }
    );
  }
}

// Register for deserialization
registerErrorClass('MyModuleError', MyModuleError);
```

### Step 4: Create Factory Functions (Optional)

```typescript
/**
 * Create a MyModuleError for specific condition
 *
 * @example
 * ```typescript
 * throw createSpecificError('detail');
 * ```
 */
export function createSpecificError(detail: string): MyModuleError {
  return new MyModuleError(
    MyModuleErrorCode.MY_ERROR,
    `Error occurred: ${detail}`,
    { context: { metadata: { detail } } }
  );
}
```

---

## Migration Guide for Non-Compliant Errors

### Pattern: Error Class Extending Error Directly

**Before (Non-Compliant):**
```typescript
export class TransactionError extends Error {
  constructor(
    public readonly code: TransactionErrorCode,
    message: string,
    public readonly txnId?: TransactionId,
    public readonly cause?: Error
  ) {
    super(message);
    this.name = 'TransactionError';
  }
}
```

**After (Compliant):**
```typescript
export class TransactionError extends DoSQLError {
  readonly code: TransactionErrorCode;
  readonly category = ErrorCategory.CONFLICT;
  readonly txnId?: TransactionId;

  constructor(
    code: TransactionErrorCode,
    message: string,
    options?: { cause?: Error; context?: ErrorContext; txnId?: TransactionId }
  ) {
    super(message, { cause: options?.cause, context: options?.context });
    this.name = 'TransactionError';
    this.code = code;
    this.txnId = options?.txnId;

    if (this.txnId) {
      this.context = { ...this.context, transactionId: this.txnId };
    }
  }

  isRetryable(): boolean {
    return [
      TransactionErrorCode.ABORTED,
      TransactionErrorCode.DEADLOCK,
      TransactionErrorCode.SERIALIZATION_FAILURE,
    ].includes(this.code);
  }

  toUserMessage(): string {
    // Implement user-friendly messages
    return this.message;
  }
}

registerErrorClass('TransactionError', TransactionError);
```

### Pattern: throw new Error() with String Message

**Before (Non-Compliant):**
```typescript
if (!sql) {
  throw new Error('SQL query is required');
}
```

**After (Compliant):**
```typescript
import { BindingError, BindingErrorCode } from './errors/binding-errors.js';

if (!sql) {
  throw new BindingError(
    BindingErrorCode.MISSING_PARAM,
    'SQL query is required'
  );
}
```

Or use factory function:
```typescript
import { createMissingNamedParamError } from './errors/binding-errors.js';

if (!sql) {
  throw createMissingNamedParamError('sql');
}
```

---

## Current Violations

The following error classes in the codebase extend `Error` directly instead of the appropriate base class. These should be migrated:

### High Priority (Core Functionality)

| Class | Location | Should Extend |
|-------|----------|---------------|
| `TransactionError` | `packages/dosql/src/transaction/types.ts:559` | `DoSQLError` |
| `WALError` | `packages/dosql/src/wal/types.ts:407` | `DoSQLError` |
| `CDCError` | `packages/dosql/src/cdc/types.ts:376` | `DoSQLError` |

### Medium Priority (Feature Modules)

| Class | Location | Should Extend |
|-------|----------|---------------|
| `TriggerError` | `packages/dosql/src/triggers/types.ts:626` | `DoSQLError` |
| `ReplicationError` | `packages/dosql/src/replication/types.ts:670` | `DoSQLError` |
| `CompactionError` | `packages/dosql/src/compaction/types.ts:579` | `DoSQLError` |
| `BranchError` | `packages/dosql/src/branch/types.ts:429` | `DoSQLError` |
| `ViewError` | `packages/dosql/src/view/types.ts:468` | `DoSQLError` |
| `TimeTravelError` | `packages/dosql/src/timetravel/types.ts:454` | `DoSQLError` |

### Lower Priority (Utility Modules)

| Class | Location | Should Extend |
|-------|----------|---------------|
| `HLCError` | `packages/dosql/src/hlc.ts:117` | `DoSQLError` |
| `ConstraintError` | `packages/dosql/src/constraints/validator.ts:42` | `DoSQLError` |
| `RetentionError` | `packages/dosql/src/wal/retention-types.ts:714` | `DoSQLError` |
| `LakehouseError` | `packages/dosql/src/lakehouse/types.ts:554` | `DoSQLError` |
| `FSXError` | `packages/dosql/src/fsx/types.ts:195` | `DoSQLError` |
| `COWError` | `packages/dosql/src/fsx/cow-types.ts:701` | `DoSQLError` |

### Cross-Package Inconsistencies

| Package | Issue |
|---------|-------|
| `sql.do` | Error classes don't share a common base with `dosql` |
| `lake.do` | `LakeError` is a good pattern but not shared with `dosql` |
| `dolake` | `DoLakeError` is a good pattern but not shared with `dosql` |

### Generic `throw new Error()` Usage

The following files contain `throw new Error()` statements that should be replaced with typed errors:

- `packages/dosql/src/proc/*.ts` - Procedure-related errors
- `packages/dosql/src/columnar/*.ts` - Columnar storage errors
- `packages/dosql/src/migrations/*.ts` - Migration errors
- `packages/dolake/src/serialization.ts` - Serialization errors
- `packages/dolake/src/rate-limiter.ts` - Rate limiting errors
- `packages/lake.do/src/client.ts` - Client validation errors
- `packages/sql.do/src/pool.ts` - Pool management errors

---

## Best Practices Summary

1. **Always extend the appropriate base class** - Never extend `Error` directly for production code
2. **Use error codes** - Every error should have a machine-readable code
3. **Preserve cause chains** - Always pass original errors as `cause`
4. **Provide recovery hints** - Help developers understand how to fix the issue
5. **Implement `isRetryable()`** - Indicate whether the operation can be safely retried
6. **Register for deserialization** - Use `registerErrorClass()` for RPC errors
7. **Use factory functions** - Create helper functions for common error patterns
8. **Document error codes** - Add new codes to `ERROR_CODES.md`

---

## Related Documentation

- [Error Codes Reference](/docs/ERROR_CODES.md)
- [DoSQL API Reference](/packages/dosql/docs/api-reference.md)
- [Testing Best Practices](/packages/dosql/docs/TESTING_REVIEW.md)
