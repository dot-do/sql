# Code Review: @dotdo/sql Monorepo

**Date:** 2026-01-22
**Reviewer:** Claude Opus 4.5
**Packages Reviewed:**
- `sql.do` - Client SDK for DoSQL
- `lake.do` - Client SDK for DoLake
- `dosql` - Server (Durable Object) SQL database engine
- `dolake` - Server (Durable Object) Lakehouse

---

## Executive Summary

The codebase demonstrates a well-architected distributed SQL database system built on Cloudflare Durable Objects. The architecture separates concerns cleanly between client SDKs and server implementations, with proper use of TypeScript for type safety. However, there are several areas requiring attention, particularly around security hardening, error handling consistency, and code duplication.

**Overall Assessment:** Good foundation with opportunities for improvement in security and consistency.

---

## Issues by Severity

### Critical

#### 1. SQL Injection Risk in Mock Query Executor
**File:** `/Users/nathanclevenger/projects/sql/packages/dosql/src/rpc/server.ts`
**Lines:** 719-797

The `MockQueryExecutor` class performs naive SQL parsing without proper sanitization:

```typescript
// Line 729-732
if (upperSql.startsWith('SELECT')) {
  const fromMatch = upperSql.match(/FROM\s+(\w+)/);
  const tableName = fromMatch?.[1]?.toLowerCase();
```

**Risk:** While labeled as "for testing," this pattern could be copied into production code. The regex-based parsing is vulnerable to injection attacks.

**Recommendation:**
- Add prominent warnings that this is test-only code
- Consider using a proper SQL parser library even for mocks
- Add integration tests that verify parameterized queries are used in production paths

#### 2. Missing Input Validation on WebSocket Messages Before Parsing
**File:** `/Users/nathanclevenger/projects/sql/packages/dolake/src/dolake.ts`
**Lines:** Various WebSocket handlers

While Zod validation exists (`schemas.ts`), there's no size limit check before attempting to parse large JSON payloads, which could lead to memory exhaustion attacks.

**Recommendation:**
- Check message size before JSON parsing
- Add hard limits on message payload sizes at the WebSocket level
- Implement streaming parsing for large payloads

---

### High

#### 3. Type Duplication Between Client and Server Packages
**Files:**
- `/Users/nathanclevenger/projects/sql/packages/sql.do/src/types.ts`
- `/Users/nathanclevenger/projects/sql/packages/dosql/src/rpc/types.ts`
- `/Users/nathanclevenger/projects/sql/packages/lake.do/src/types.ts`
- `/Users/nathanclevenger/projects/sql/packages/dolake/src/types.ts`

Multiple interface definitions are duplicated across packages (e.g., `QueryRequest`, `QueryResponse`, `CDCEvent`, `ClientCapabilities`).

**Example duplication:**

```typescript
// In sql.do/src/types.ts
export interface QueryRequest {
  sql: string;
  params?: unknown[];
  // ...
}

// In dosql/src/rpc/types.ts (similar but not identical)
export interface QueryRequest {
  sql: string;
  params?: unknown[];
  namedParams?: Record<string, unknown>;
  // ...
}
```

**Recommendation:**
- Create a shared `@dotdo/sql-types` package
- Use package references to share types between client and server
- Consider using Protocol Buffers or similar for guaranteed type compatibility

#### 4. Inconsistent Error Handling Patterns
**Files:** Multiple across all packages

Some functions throw custom error classes, while others return error objects:

```typescript
// Throwing pattern (database.ts:39)
export class DatabaseError extends Error {
  constructor(message: string, public readonly code?: string) {
    super(message);
    this.name = 'DatabaseError';
  }
}

// Return pattern (rpc/server.ts:322-327)
return {
  success: false,
  error: error instanceof Error ? error.message : String(error),
};
```

**Recommendation:**
- Standardize on one error handling pattern across the codebase
- Document the error handling contract in each package's public API
- Create a shared error hierarchy in the types package

#### 5. Missing Rate Limit Headers Validation
**File:** `/Users/nathanclevenger/projects/sql/packages/dolake/src/rate-limiter.ts`
**Lines:** 850-862

The `getRateLimitHeaders` method trusts the `RateLimitResult` object without validation:

```typescript
getRateLimitHeaders(result: RateLimitResult): Record<string, string> {
  const headers: Record<string, string> = {
    'X-RateLimit-Limit': result.rateLimit.limit.toString(),
    'X-RateLimit-Remaining': result.rateLimit.remaining.toString(),
    'X-RateLimit-Reset': result.rateLimit.resetAt.toString(),
  };
```

**Recommendation:**
- Validate numeric values before converting to strings
- Ensure negative values cannot appear in headers
- Add bounds checking for all rate limit values

---

### Medium

#### 6. Incomplete Parameter Binding in Named Parameters Conversion
**File:** `/Users/nathanclevenger/projects/sql/packages/dosql/src/rpc/server.ts`
**Lines:** 541-560

The `#convertNamedParams` method uses a simple regex that doesn't handle edge cases:

```typescript
#convertNamedParams(request: QueryRequest): unknown[] {
  // ...
  const paramPattern = /[:@$](\w+)/g;
  let match;
  while ((match = paramPattern.exec(request.sql)) !== null) {
    const paramName = match[1];
    if (paramName in namedParams) {
      params.push(namedParams[paramName]);
    }
  }
  return params;
}
```

**Issues:**
- Doesn't handle parameters inside string literals (could extract false positives)
- Doesn't handle escaped parameter markers
- Silent failure when parameter not found (should throw)

**Recommendation:**
- Use the existing `parseParameters` function from `binding.ts`
- Throw an error when a named parameter is referenced but not provided
- Add comprehensive tests for edge cases

#### 7. Missing Timeout on Transaction Operations
**File:** `/Users/nathanclevenger/projects/sql/packages/dosql/src/rpc/server.ts`
**Lines:** 288-347

Transaction operations don't enforce timeouts server-side:

```typescript
async beginTransaction(request: BeginTransactionRequest): Promise<TransactionHandle> {
  // ...
  const expiresAt = Date.now() + (request.timeoutMs ?? 30000);
  return {
    txId,
    startLSN: this.#executor.getCurrentLSN(),
    expiresAt, // Only returned to client, not enforced server-side
  };
}
```

**Recommendation:**
- Implement server-side transaction timeout enforcement
- Use Durable Object alarms to automatically rollback expired transactions
- Log long-running transactions for monitoring

#### 8. BigInt Serialization Concerns
**File:** `/Users/nathanclevenger/projects/sql/packages/dolake/src/types.ts`
**Lines:** 617-696 (IcebergSnapshot, IcebergTableMetadata)

Multiple interfaces use `bigint` for IDs and sequence numbers:

```typescript
export interface IcebergSnapshot {
  'snapshot-id': bigint;
  'parent-snapshot-id': bigint | null;
  'sequence-number': bigint;
  // ...
}
```

**Risk:** `bigint` values don't serialize to JSON by default and will throw errors.

**Recommendation:**
- Add custom serialization/deserialization for bigint fields
- Consider using string representation for IDs in wire protocol
- Document the serialization approach

#### 9. Potential Memory Leak in Stream State Management
**File:** `/Users/nathanclevenger/projects/sql/packages/dosql/src/rpc/server.ts`
**Lines:** 157-158, 210-276

Streams and CDC subscriptions are stored in Maps without TTL or cleanup:

```typescript
#streams: Map<string, StreamState> = new Map();
#cdcSubscriptions: Map<string, CDCSubscription> = new Map();
```

**Recommendation:**
- Implement automatic cleanup of abandoned streams
- Add TTL-based expiration
- Limit the maximum number of concurrent streams per connection

#### 10. Client SDK Retries Without Idempotency Keys
**File:** `/Users/nathanclevenger/projects/sql/packages/sql.do/src/client.ts`
**Lines:** Various

The client SDK has retry logic but doesn't use idempotency keys for mutation operations:

**Recommendation:**
- Generate idempotency keys for INSERT/UPDATE/DELETE operations
- Pass idempotency key in request headers
- Server should detect and deduplicate retried operations

---

### Low

#### 11. Missing JSDoc on Public API Methods
**Files:** Multiple across client SDKs

Many public methods lack comprehensive JSDoc documentation:

```typescript
// sql.do/src/client.ts
async query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<T[]> {
  // No JSDoc describing return type, errors, etc.
}
```

**Recommendation:**
- Add JSDoc to all public API methods
- Include @param, @returns, @throws annotations
- Add usage examples in documentation

#### 12. Inconsistent Use of `readonly` Modifier
**Files:** Multiple

Some class properties that should be immutable lack the `readonly` modifier:

```typescript
// dolake/src/rate-limiter.ts:256
private readonly config: RateLimitConfig;  // Good
private readonly connections: Map<string, ConnectionState> = new Map();  // The Map itself is readonly, but its contents aren't

// database.ts:97
private engine: ExecutionEngine;  // Should be readonly
```

**Recommendation:**
- Apply `readonly` consistently to properties that shouldn't be reassigned
- Consider using `ReadonlyMap` and `ReadonlySet` where mutation isn't needed

#### 13. Magic Numbers and Strings
**File:** `/Users/nathanclevenger/projects/sql/packages/dolake/src/rate-limiter.ts`
**Lines:** Various

Several magic numbers appear without named constants:

```typescript
// Line 463
while (events.length < 100) { // Max 100 events per poll
```

**Recommendation:**
- Extract magic numbers to named constants
- Group related constants in a configuration object
- Make limits configurable via constructor options

#### 14. Test Code Organization
**Files:** Multiple `*.test.ts` files

Some test files are located alongside source files rather than in dedicated test directories:

```
packages/dosql/src/aggregates.test.ts
packages/dosql/src/join.test.ts
packages/dosql/src/types.test.ts
packages/dolake/src/dolake.test.ts
```

**Recommendation:**
- Move all tests to `__tests__` directories for consistency
- Update test configuration to find tests in standard locations

#### 15. Unused Import Statement
**File:** `/Users/nathanclevenger/projects/sql/packages/dosql/src/statement/binding.ts`
**Line:** 10

```typescript
import type { SqlValue, NamedParameters, BindParameters } from './types.js';
```

Verify all imported types are used.

**Recommendation:**
- Run `eslint` with `@typescript-eslint/no-unused-vars` rule
- Configure `import/no-unused-modules` for stricter checking

---

## Code Quality Observations

### Positive Aspects

1. **Strong TypeScript Usage:** Comprehensive use of interfaces, generics, and type guards throughout the codebase.

2. **Well-Designed Rate Limiter:** The `RateLimiter` class in dolake implements multiple protection mechanisms:
   - Token bucket algorithm
   - Per-IP and per-connection limits
   - Subnet-level rate limiting
   - Backpressure signaling

3. **Comprehensive Zod Validation:** The `schemas.ts` file in dolake provides thorough runtime validation of WebSocket messages.

4. **Clean Separation of Concerns:** Clear boundaries between client SDKs, server implementations, and shared utilities.

5. **Type-Level SQL Parsing:** The parser in `dosql/src/parser.ts` attempts compile-time SQL validation, which is an innovative approach.

6. **CDC Architecture:** Well-designed Change Data Capture system with:
   - Batching for efficiency
   - Position tracking for resumability
   - Backpressure handling

### Areas for Improvement

1. **Testing Coverage:** Several critical paths lack comprehensive test coverage, particularly:
   - Error handling edge cases
   - Concurrent operation handling
   - Recovery from partial failures

2. **Documentation:** While code comments are present, architecture-level documentation is sparse.

3. **Monitoring and Observability:** Limited instrumentation for production monitoring.

---

## Recommendations Summary

### Immediate Actions (Critical/High)

1. Add message size validation before JSON parsing in WebSocket handlers
2. Create shared types package to eliminate duplication
3. Standardize error handling patterns across all packages
4. Add prominent warnings to test-only code that could be misused

### Short-Term Improvements (Medium)

1. Implement server-side transaction timeout enforcement
2. Add cleanup mechanisms for abandoned streams and subscriptions
3. Implement idempotency keys for mutation retries
4. Fix named parameter conversion to use existing parser

### Long-Term Enhancements (Low)

1. Comprehensive JSDoc documentation
2. Consistent test organization
3. Extract magic numbers to configuration
4. Consider property immutability patterns

---

## Conclusion

The @dotdo/sql monorepo demonstrates solid architectural decisions and good TypeScript practices. The main areas requiring attention are security hardening (input validation, injection prevention), code consistency (error handling, type sharing), and operational concerns (timeouts, cleanup, monitoring). Addressing the critical and high-priority items should be prioritized before production deployment.

---

*Generated by Claude Opus 4.5 on 2026-01-22*
