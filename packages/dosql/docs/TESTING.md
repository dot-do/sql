# DoSQL Testing Guidelines

This document provides guidelines for writing tests in the DoSQL codebase, with particular focus on timing-sensitive tests and the "NO MOCKS" philosophy.

## Table of Contents

1. [Core Principles](#core-principles)
2. [Timing-Sensitive Tests](#timing-sensitive-tests)
3. [NO MOCKS Philosophy](#no-mocks-philosophy)
4. [Test Patterns](#test-patterns)
5. [Common Pitfalls](#common-pitfalls)

---

## Core Principles

### TDD with Real Environments

Tests run in actual Cloudflare Workers environment via `@cloudflare/vitest-pool-workers`. We use real Durable Objects and real SQLite, not mocks. Integration tests are preferred over unit tests.

### Test Organization

- Feature-based `describe` blocks
- Clear "should do X when Y" naming convention
- Logical grouping of related test cases
- Explicit documentation of expected behavior vs. current gaps

---

## Timing-Sensitive Tests

### When to Use Fake Timers

Use `vi.useFakeTimers()` when:

1. **Testing timeout behavior** - Transaction timeouts, connection timeouts, TTL expiration
2. **Testing cache expiration** - TTL-based caches, stale data detection
3. **Testing periodic operations** - Heartbeats, polling intervals, scheduled tasks
4. **Avoiding flaky tests** - Any test with delays > 50ms that doesn't require real time

```typescript
// GOOD: Use fake timers for timeout testing
describe('Transaction Timeout', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should timeout after configured duration', async () => {
    const manager = createTransactionManager({ timeoutMs: 100 });
    const txn = await manager.begin();

    // Advance time without waiting
    vi.advanceTimersByTime(150);

    expect(txn.state).toBe('timed_out');
  });
});
```

### When Real Timers Are Necessary

Some tests genuinely require real time and cannot use fake timers:

1. **Async iteration with `for await`** - Generator-based streaming doesn't work well with fake timers
2. **WebSocket communication** - Real network timing required
3. **Concurrent operation ordering** - Testing race conditions with multiple async operations
4. **Event loop behavior** - Testing microtask/macrotask ordering

For these cases, document WHY real timers are necessary:

```typescript
// Real timers required: Testing async iterator backpressure with actual event loop timing
// The generator-based CDC stream relies on real Promise scheduling to test pause/resume
it('should handle stream pause and resume', async () => {
  // ... test with real setTimeout
});
```

### Recommended Delay Thresholds

| Delay | Recommendation |
|-------|----------------|
| < 10ms | Can use real timers (minimal flakiness risk) |
| 10-50ms | Consider fake timers if test is timing-dependent |
| 50-100ms | Should use fake timers unless real time is necessary |
| > 100ms | Must use fake timers or document why real time is required |

### Converting Real Timers to Fake Timers

**Before (problematic):**
```typescript
it('should cleanup after timeout', async () => {
  const obj = createWithTimeout(100);
  await new Promise(resolve => setTimeout(resolve, 150)); // SLOW & FLAKY
  expect(obj.isCleanedUp()).toBe(true);
});
```

**After (deterministic):**
```typescript
it('should cleanup after timeout', async () => {
  vi.useFakeTimers();
  try {
    const obj = createWithTimeout(100);
    vi.advanceTimersByTime(150);
    expect(obj.isCleanedUp()).toBe(true);
  } finally {
    vi.useRealTimers();
  }
});
```

### Async Fake Timers

When testing async operations with fake timers, use `vi.advanceTimersByTimeAsync()`:

```typescript
it('should handle async timeout', async () => {
  vi.useFakeTimers();
  try {
    const promise = asyncOperationWithTimeout(100);
    await vi.advanceTimersByTimeAsync(150);
    await expect(promise).rejects.toThrow('timeout');
  } finally {
    vi.useRealTimers();
  }
});
```

### Patterns for Tests That Need Small Delays

When a small delay is needed for async operation ordering (not timing logic), use the minimal delay:

```typescript
// ACCEPTABLE: Small delay to ensure async operation completes
// This is not testing timing logic, just ensuring ordering
await new Promise(resolve => setTimeout(resolve, 10));
```

Document when delays are for ordering vs. timing logic:

```typescript
// Small delay to ensure Promise.race settles before assertion
// Not testing timing - just ensuring deterministic test ordering
await new Promise(resolve => setTimeout(resolve, 5));
```

---

## NO MOCKS Philosophy

### Core Rule

For Durable Object integration tests: **NO MOCKS**. Use the real `@cloudflare/vitest-pool-workers` environment with real DOs.

### Acceptable Abstractions

These are NOT mocks - they are real implementations for testing:

| Pattern | Description | When to Use |
|---------|-------------|-------------|
| `createMemoryBackend()` | In-memory FSX backend | Unit tests for storage logic |
| `SimulatedShardRPC` | Full ShardRPC implementation | Cross-DO communication tests |
| `createTestR2Bucket()` | In-memory R2 bucket | R2 integration without network |
| `FakeHandler` | Configurable test double | Trigger execution tests |

### When Mocks Are Acceptable

Mocks are acceptable ONLY for:

1. **External services** - Network calls outside the DO environment
2. **Unit tests of pure logic** - Testing algorithms without DO integration
3. **Expensive operations** - When the operation isn't being tested

```typescript
// ACCEPTABLE: Mocking external WAL writer in transaction state machine tests
// We're testing the state machine, not the WAL writing
function createMockWALWriter() {
  return {
    async append() { return { lsn: nextLSN++ }; },
    async flush() { /* no-op */ },
  };
}
```

---

## Test Patterns

### Testing CDC Streams

CDC streams use async iterators. Use timeout protection to avoid hanging tests:

```typescript
/**
 * Collects items from async iterator with timeout protection.
 * Necessary for CDC streams that may poll indefinitely.
 */
async function collectWithTimeout<T>(
  iterator: AsyncIterableIterator<T>,
  maxItems: number,
  timeoutMs: number = 1000
): Promise<T[]> {
  const items: T[] = [];
  const timeoutPromise = new Promise<'timeout'>((resolve) =>
    setTimeout(() => resolve('timeout'), timeoutMs)
  );

  while (items.length < maxItems) {
    const result = await Promise.race([iterator.next(), timeoutPromise]);
    if (result === 'timeout' || result.done) break;
    items.push(result.value);
  }

  return items;
}
```

### Testing Replication with Heartbeats

For tests involving heartbeat timeouts:

```typescript
describe('Replication Heartbeat', () => {
  it('should detect primary failure after timeout', async () => {
    const replica = createReplica({
      config: { heartbeatTimeoutMs: 50, autoFailover: true },
    });

    // Initialize replica
    await replica.initialize('https://primary.do', replicaInfo);

    // Wait for timeout (short enough to be acceptable)
    await new Promise(resolve => setTimeout(resolve, 100));

    const eligibility = await replica.checkPromotionEligibility();
    expect(eligibility.eligible).toBe(true);
  });
});
```

### Testing Circuit Breakers

Circuit breakers should use fake timers for reset timeout testing:

```typescript
describe('Circuit Breaker', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('should transition to HALF_OPEN after resetTimeoutMs', async () => {
    const breaker = createCircuitBreaker({ resetTimeoutMs: 5000 });

    // Trip the breaker
    await tripBreaker(breaker);
    expect(breaker.state).toBe('OPEN');

    // Advance past reset timeout
    vi.advanceTimersByTime(5100);

    expect(breaker.state).toBe('HALF_OPEN');
  });
});
```

---

## Common Pitfalls

### Pitfall 1: Long Delays in Tests

**Bad:**
```typescript
await new Promise(resolve => setTimeout(resolve, 5000)); // 5 second delay!
```

**Fix:** Use fake timers or reduce the timeout configuration for tests.

### Pitfall 2: Not Cleaning Up Fake Timers

**Bad:**
```typescript
it('test 1', () => {
  vi.useFakeTimers();
  // ... test
});

it('test 2', () => {
  // Fake timers still active from test 1!
});
```

**Fix:** Always restore real timers in `afterEach`:
```typescript
afterEach(() => {
  vi.useRealTimers();
});
```

### Pitfall 3: Forgetting Async with Fake Timers

**Bad:**
```typescript
vi.useFakeTimers();
const promise = asyncOperation();
vi.advanceTimersByTime(100); // Promise hasn't resolved!
await promise; // Hangs forever
```

**Fix:** Use `advanceTimersByTimeAsync`:
```typescript
vi.useFakeTimers();
const promise = asyncOperation();
await vi.advanceTimersByTimeAsync(100);
await promise; // Works correctly
```

### Pitfall 4: Relying on Specific Timing

**Bad:**
```typescript
// Assumes operation takes exactly 50ms
await operation();
expect(elapsed).toBe(50);
```

**Fix:** Use ranges or fake timers:
```typescript
// Use range for real-time tests
expect(elapsed).toBeGreaterThan(40);
expect(elapsed).toBeLessThan(100);

// Or use fake timers for exact timing
vi.useFakeTimers();
vi.advanceTimersByTime(50);
```

---

## Known Flaky Test Patterns

The following patterns have been identified as potentially flaky. When encountering these, consider refactoring:

1. **Fixed delays without fake timers** - Tests with `setTimeout` > 100ms
2. **Race conditions in test setup** - Multiple async operations without proper sequencing
3. **Time-dependent assertions** - `Date.now()` comparisons without tolerance
4. **Polling loops** - `while` loops waiting for conditions without timeout

---

## Related Resources

- [Vitest Fake Timers Documentation](https://vitest.dev/guide/mocking.html#timers)
- [Testing Async Code in Vitest](https://vitest.dev/guide/testing-async-code.html)
- [NO MOCKS Compliance Analysis](../notes/TESTING_REVIEW.md)
