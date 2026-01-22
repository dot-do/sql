/**
 * Client SDK Idempotency Keys Tests - RED Phase TDD
 *
 * These tests document the MISSING idempotency key behavior in the SQL client.
 * While basic idempotency key generation exists, several critical features are missing.
 *
 * Issue: sql-zhy.27
 *
 * Tests document gaps:
 * 1. Idempotency key generation - EXISTING (some tests verify current implementation)
 * 2. Idempotency key in requests - GAP: Missing X-Idempotency-Key header
 * 3. Retry behavior with idempotency - GAP: No server cached result indicator
 * 4. Idempotency window - GAP: No TTL expiration enforcement
 * 5. Key collision handling - GAP: No collision detection
 * 6. Transaction idempotency - GAP: No transaction-level keys
 *
 * NOTE: Tests using `it.fails()` document missing features that SHOULD exist
 * Tests using `it()` verify existing behavior
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';
import {
  generateIdempotencyKey,
  isMutationQuery,
  DoSQLClient,
} from '../client.js';
import { DEFAULT_IDEMPOTENCY_CONFIG } from '../types.js';

// =============================================================================
// DOCUMENTED GAPS - Features that should be implemented
// =============================================================================
//
// 1. Request headers: X-Idempotency-Key header for mutations (server compatibility)
// 2. Server caching: Server returns cached result with fromCache indicator
// 3. Window configuration: Client-side TTL enforcement (idempotencyWindowMs)
// 4. Key expiration: Keys expire after window period (re-generation after TTL)
// 5. Collision detection: Warn when same key used for different operations
// 6. Transaction keys: Single key for entire transaction
// 7. Batch keys: Single key for batch with sub-keys for statements
// 8. Server error handling: IDEMPOTENCY_CONFLICT, IDEMPOTENCY_KEY_EXPIRED
// 9. Error context: Include idempotency key in error details
//
// =============================================================================

// =============================================================================
// 1. IDEMPOTENCY KEY GENERATION (EXISTING - Verify current implementation)
// =============================================================================

describe('Idempotency Key Generation', () => {
  describe('isMutationQuery', () => {
    it('should identify INSERT statements as mutations', () => {
      expect(isMutationQuery('INSERT INTO users (name) VALUES (?)')).toBe(true);
      expect(isMutationQuery('  INSERT INTO users VALUES (1, "test")')).toBe(true);
      expect(isMutationQuery('insert into users values (1)')).toBe(true);
    });

    it('should identify UPDATE statements as mutations', () => {
      expect(isMutationQuery('UPDATE users SET name = ? WHERE id = ?')).toBe(true);
      expect(isMutationQuery('  UPDATE users SET active = true')).toBe(true);
      expect(isMutationQuery('update users set name = "test"')).toBe(true);
    });

    it('should identify DELETE statements as mutations', () => {
      expect(isMutationQuery('DELETE FROM users WHERE id = ?')).toBe(true);
      expect(isMutationQuery('  DELETE FROM users')).toBe(true);
      expect(isMutationQuery('delete from users where id = 1')).toBe(true);
    });

    it('should not identify SELECT statements as mutations', () => {
      expect(isMutationQuery('SELECT * FROM users')).toBe(false);
      expect(isMutationQuery('  SELECT id, name FROM users WHERE id = 1')).toBe(false);
      expect(isMutationQuery('select * from users')).toBe(false);
    });

    it('should not identify DDL statements as mutations', () => {
      expect(isMutationQuery('CREATE TABLE users (id INT)')).toBe(false);
      expect(isMutationQuery('DROP TABLE users')).toBe(false);
      expect(isMutationQuery('ALTER TABLE users ADD COLUMN email TEXT')).toBe(false);
    });
  });

  describe('generateIdempotencyKey', () => {
    it('should generate key with format {timestamp}-{random}-{hash}', async () => {
      const key = await generateIdempotencyKey('INSERT INTO users VALUES (1)', [1]);

      const parts = key.split('-');
      expect(parts.length).toBe(3);

      // Timestamp should be a valid number (Unix milliseconds)
      const timestamp = parseInt(parts[0], 10);
      expect(timestamp).toBeGreaterThan(0);
      expect(timestamp).toBeLessThanOrEqual(Date.now());

      // Random part should be 8 characters
      expect(parts[1].length).toBe(8);

      // Hash part should be 8 characters
      expect(parts[2].length).toBe(8);
    });

    it('should generate different keys for different SQL statements', async () => {
      const key1 = await generateIdempotencyKey('INSERT INTO users VALUES (1)', [1]);
      const key2 = await generateIdempotencyKey('INSERT INTO users VALUES (2)', [2]);

      // Keys should be different
      expect(key1).not.toBe(key2);

      // Hash parts should be different (different SQL/params)
      const hash1 = key1.split('-')[2];
      const hash2 = key2.split('-')[2];
      expect(hash1).not.toBe(hash2);
    });

    it('should generate consistent hash for same SQL and params', async () => {
      const sql = 'INSERT INTO users VALUES (?)';
      const params = [1];

      const key1 = await generateIdempotencyKey(sql, params);
      const key2 = await generateIdempotencyKey(sql, params);

      // Hash parts should be the same
      const hash1 = key1.split('-')[2];
      const hash2 = key2.split('-')[2];
      expect(hash1).toBe(hash2);
    });

    it('should support custom prefix', async () => {
      const key = await generateIdempotencyKey(
        'INSERT INTO users VALUES (1)',
        [1],
        'myprefix'
      );

      expect(key.startsWith('myprefix-')).toBe(true);
      const parts = key.split('-');
      expect(parts.length).toBe(4); // prefix-timestamp-random-hash
    });

    it('should handle empty params', async () => {
      const key = await generateIdempotencyKey('INSERT INTO users DEFAULT VALUES');
      expect(key).toBeDefined();
      expect(key.split('-').length).toBe(3);
    });

    it('should handle undefined params', async () => {
      const key = await generateIdempotencyKey('INSERT INTO users DEFAULT VALUES', undefined);
      expect(key).toBeDefined();
      expect(key.split('-').length).toBe(3);
    });
  });

  describe('DoSQLClient idempotency', () => {
    it('should have idempotency enabled by default', () => {
      expect(DEFAULT_IDEMPOTENCY_CONFIG.enabled).toBe(true);
    });

    it('should have default TTL of 24 hours', () => {
      expect(DEFAULT_IDEMPOTENCY_CONFIG.ttlMs).toBe(24 * 60 * 60 * 1000);
    });

    describe('getIdempotencyKey', () => {
      let client: DoSQLClient;

      beforeEach(() => {
        client = new DoSQLClient({
          url: 'ws://localhost:8080',
        });
      });

      it('should generate idempotency key for INSERT', async () => {
        const key = await client.getIdempotencyKey('INSERT INTO users VALUES (1)', [1]);
        expect(key).toBeDefined();
        expect(typeof key).toBe('string');
      });

      it('should generate idempotency key for UPDATE', async () => {
        const key = await client.getIdempotencyKey('UPDATE users SET name = ?', ['test']);
        expect(key).toBeDefined();
        expect(typeof key).toBe('string');
      });

      it('should generate idempotency key for DELETE', async () => {
        const key = await client.getIdempotencyKey('DELETE FROM users WHERE id = ?', [1]);
        expect(key).toBeDefined();
        expect(typeof key).toBe('string');
      });

      it('should NOT generate idempotency key for SELECT', async () => {
        const key = await client.getIdempotencyKey('SELECT * FROM users', []);
        expect(key).toBeUndefined();
      });

      it('should reuse same key for same request (retry scenario)', async () => {
        const sql = 'INSERT INTO users VALUES (?)';
        const params = [1];

        const key1 = await client.getIdempotencyKey(sql, params);
        const key2 = await client.getIdempotencyKey(sql, params);

        // Same key should be returned (cached for retry)
        expect(key1).toBe(key2);
      });

      it('should generate different keys for different requests', async () => {
        const key1 = await client.getIdempotencyKey('INSERT INTO users VALUES (?)', [1]);
        const key2 = await client.getIdempotencyKey('INSERT INTO users VALUES (?)', [2]);

        // Different keys for different params
        expect(key1).not.toBe(key2);
      });
    });

    describe('clearIdempotencyKey', () => {
      let client: DoSQLClient;

      beforeEach(() => {
        client = new DoSQLClient({
          url: 'ws://localhost:8080',
        });
      });

      it('should clear cached key and generate new one', async () => {
        const sql = 'INSERT INTO users VALUES (?)';
        const params = [1];

        // Generate and cache a key
        const key1 = await client.getIdempotencyKey(sql, params);

        // Clear the key
        client.clearIdempotencyKey(sql, params);

        // Generate a new key - should be different (new timestamp/random)
        const key2 = await client.getIdempotencyKey(sql, params);

        // Keys should be different (though hash will be the same)
        expect(key1).not.toBe(key2);

        // But the hash part should be the same since SQL and params are the same
        const hash1 = key1!.split('-')[2];
        const hash2 = key2!.split('-')[2];
        expect(hash1).toBe(hash2);
      });
    });

    describe('idempotency disabled', () => {
      let client: DoSQLClient;

      beforeEach(() => {
        client = new DoSQLClient({
          url: 'ws://localhost:8080',
          idempotency: { enabled: false },
        });
      });

      it('should not generate idempotency key when disabled', async () => {
        const key = await client.getIdempotencyKey('INSERT INTO users VALUES (1)', [1]);
        expect(key).toBeUndefined();
      });
    });

    describe('idempotency with custom prefix', () => {
      let client: DoSQLClient;

      beforeEach(() => {
        client = new DoSQLClient({
          url: 'ws://localhost:8080',
          idempotency: { enabled: true, keyPrefix: 'myapp' },
        });
      });

      it('should include prefix in generated key', async () => {
        const key = await client.getIdempotencyKey('INSERT INTO users VALUES (1)', [1]);
        expect(key).toBeDefined();
        expect(key!.startsWith('myapp-')).toBe(true);
      });
    });
  });
});

// =============================================================================
// 2. IDEMPOTENCY KEY IN REQUESTS - GAP: Missing header support
// =============================================================================

describe('Idempotency Key in Requests', () => {
  /**
   * GAP: RPC requests should include X-Idempotency-Key header for server compatibility
   * Currently: Key is only in request body params, not in headers
   */
  it.fails('should include X-Idempotency-Key in request headers', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: getRPCHeaders() method should exist to inspect outgoing headers
    const headers = (client as any).getRPCHeaders?.('exec', {
      sql: 'INSERT INTO users VALUES (1)',
      params: [1],
    });

    expect(headers).toBeDefined();
    expect(headers['X-Idempotency-Key']).toBeDefined();
  });

  /**
   * GAP: HTTP fallback should send X-Idempotency-Key header
   */
  it.fails('should include X-Idempotency-Key in HTTP fallback requests', async () => {
    const client = new DoSQLClient({
      url: 'http://localhost:8080',
      // GAP: httpFallback option should exist
      httpFallback: true,
    } as any);

    // GAP: execViaHttp() method should exist for HTTP transport
    const request = (client as any).buildHttpRequest?.('exec', {
      sql: 'INSERT INTO users VALUES (1)',
      params: [1],
    });

    expect(request).toBeDefined();
    expect(request.headers.get('X-Idempotency-Key')).toBeDefined();
  });
});

// =============================================================================
// 3. RETRY BEHAVIOR WITH IDEMPOTENCY - GAP: No cached result indicator
// =============================================================================

describe('Retry Behavior with Idempotency', () => {
  /**
   * GAP: Server should return fromCache indicator when returning cached result
   * Currently: No way to know if result came from idempotency cache
   */
  it.fails('should indicate when result is from idempotency cache', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: QueryResult should have fromIdempotencyCache field
    // This requires the client to expose this information from server responses
    const hasFromCacheField = 'fromIdempotencyCache' in client;

    // GAP: getIdempotencyCacheStatus() should exist to check cache state
    const cacheStatus = (client as any).getIdempotencyCacheStatus?.('some-key');

    expect(hasFromCacheField || cacheStatus).toBeDefined();
    expect(cacheStatus).toMatchObject({
      inCache: expect.any(Boolean),
      cachedAt: expect.any(Number),
    });
  });

  /**
   * GAP: Client should expose pending idempotency keys for debugging
   */
  it.fails('should expose pending idempotency keys', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // Start a request (don't await)
    client.getIdempotencyKey('INSERT INTO users VALUES (1)', [1]);

    // GAP: getPendingIdempotencyKeys() should exist
    const pendingKeys = (client as any).getPendingIdempotencyKeys?.();

    expect(pendingKeys).toBeDefined();
    expect(pendingKeys instanceof Map || Array.isArray(pendingKeys)).toBe(true);
  });

  /**
   * GAP: Should track retry count per idempotency key
   */
  it.fails('should track retry count per idempotency key', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    const key = await client.getIdempotencyKey('INSERT INTO users VALUES (1)', [1]);

    // GAP: getRetryCount() should exist
    const retryCount = (client as any).getRetryCount?.(key);

    expect(retryCount).toBeDefined();
    expect(typeof retryCount).toBe('number');
  });
});

// =============================================================================
// 4. IDEMPOTENCY WINDOW - GAP: No TTL expiration enforcement
// =============================================================================

describe('Idempotency Window', () => {
  /**
   * GAP: Keys should expire after TTL and regenerate
   * Currently: Keys are cached indefinitely until cleared manually
   */
  it.fails('should regenerate key after TTL expires', async () => {
    vi.useFakeTimers();

    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      idempotency: {
        enabled: true,
        ttlMs: 1000, // 1 second TTL
      },
    });

    const sql = 'INSERT INTO users VALUES (?)';
    const params = [1];

    // Generate key
    const key1 = await client.getIdempotencyKey(sql, params);

    // Advance time past TTL
    vi.advanceTimersByTime(1500);

    // GAP: Should regenerate key after TTL
    const key2 = await client.getIdempotencyKey(sql, params);

    // Keys should be different (TTL expired)
    expect(key1).not.toBe(key2);

    vi.useRealTimers();
  });

  /**
   * GAP: Should provide method to check if key is expired
   */
  it.fails('should provide method to check key expiration', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      idempotency: {
        enabled: true,
        ttlMs: 1000,
      },
    });

    const key = await client.getIdempotencyKey('INSERT INTO users VALUES (1)', [1]);

    // GAP: isIdempotencyKeyExpired() should exist
    const expired = (client as any).isIdempotencyKeyExpired?.(key);

    expect(expired).toBe(false);
  });

  /**
   * GAP: Should support per-request TTL override
   */
  it.fails('should support per-request TTL override', async () => {
    vi.useFakeTimers();

    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      idempotency: {
        enabled: true,
        ttlMs: 24 * 60 * 60 * 1000, // 24 hours default
      },
    });

    const sql = 'INSERT INTO users VALUES (?)';
    const params = [1];

    // GAP: getIdempotencyKey should accept per-request TTL override option
    // Currently the method signature is: getIdempotencyKey(sql, params)
    // Should be: getIdempotencyKey(sql, params, options?)
    const key1 = await client.getIdempotencyKey(sql, params);

    // Wait 2 seconds (past the 1 second override, but before 24 hour default)
    vi.advanceTimersByTime(2000);

    // With per-request TTL of 1 second, this should be a NEW key
    // But since feature doesn't exist, it returns the same cached key
    const key2 = await client.getIdempotencyKey(sql, params);

    // This assertion will fail because there's no per-request TTL support
    // The keys will be the same (cached), but we expect different (TTL expired)
    expect(key1).not.toBe(key2);

    vi.useRealTimers();
  });
});

// =============================================================================
// 5. KEY COLLISION HANDLING - GAP: No collision detection
// =============================================================================

describe('Key Collision Handling', () => {
  /**
   * GAP: Should warn when same key is reused for different operation type
   */
  it.fails('should warn on key reuse for different operation type', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {});

    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // Generate key for INSERT
    const insertKey = await client.getIdempotencyKey('INSERT INTO users VALUES (1)', [1]);

    // GAP: setIdempotencyKey() should exist to manually set a key
    // Using same key for DELETE should warn
    (client as any).setIdempotencyKey?.('DELETE FROM users WHERE id = 1', [1], insertKey);

    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining('Idempotency key collision')
    );

    warnSpy.mockRestore();
  });

  /**
   * GAP: Should detect when different SQL uses same hash
   */
  it.fails('should detect hash collision', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: detectHashCollision() should exist
    const hasCollision = (client as any).detectHashCollision?.(
      'INSERT INTO users VALUES (1)',
      [1],
      'INSERT INTO orders VALUES (1)',
      [1]
    );

    expect(typeof hasCollision).toBe('boolean');
  });

  /**
   * GAP: Should provide collision statistics
   */
  it.fails('should provide collision statistics', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: getIdempotencyStats() should exist
    const stats = (client as any).getIdempotencyStats?.();

    expect(stats).toBeDefined();
    expect(stats.collisionCount).toBeDefined();
    expect(stats.totalKeysGenerated).toBeDefined();
  });
});

// =============================================================================
// 6. TRANSACTION IDEMPOTENCY - GAP: No transaction-level keys
// =============================================================================

describe('Transaction Idempotency', () => {
  /**
   * GAP: Transaction should have a single idempotency key
   */
  it.fails('should assign idempotency key to transaction', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: beginTransaction should return idempotencyKey
    const state = await (client as any).beginTransactionWithIdempotency?.();

    expect(state).toBeDefined();
    expect(state.idempotencyKey).toBeDefined();
    expect(state.idempotencyKey).toMatch(/^\d+-[a-z0-9]+-[a-z0-9]+$/);
  });

  /**
   * GAP: Transaction operations should reference transaction's key
   */
  it.fails('should use transaction key for all operations', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: TransactionContext should include idempotencyKey property
    // Currently TransactionContext only has transactionId, not idempotencyKey
    const TransactionContextProto = (client as any).TransactionContext?.prototype ?? {};

    // Check if TransactionContext has idempotencyKey getter
    const hasIdempotencyKey = 'idempotencyKey' in TransactionContextProto;

    // GAP: getTransactionIdempotencyKey() should exist on client
    const hasGetMethod = typeof (client as any).getTransactionIdempotencyKey === 'function';

    // At least one of these should be true for the feature to exist
    expect(hasIdempotencyKey || hasGetMethod).toBe(true);
  });

  /**
   * GAP: Retried transaction should use same idempotency key
   */
  it.fails('should preserve idempotency key on transaction retry', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: retryableTransaction() should exist
    let attempts = 0;
    let keys: string[] = [];

    await (client as any).retryableTransaction?.(
      async (tx: any) => {
        attempts++;
        keys.push(tx.idempotencyKey);
        if (attempts < 2) {
          throw new Error('Transient failure');
        }
      },
      { maxRetries: 3 }
    );

    // All attempts should use the same key
    expect(new Set(keys).size).toBe(1);
  });

  /**
   * GAP: Commit should include transaction idempotency key
   */
  it.fails('should include idempotency key in commit', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: commitWithIdempotency() should exist
    const txnId = 'txn-123';
    const idempotencyKey = '12345-abcdefgh-commit';

    const commitParams = (client as any).buildCommitParams?.(txnId, idempotencyKey);

    expect(commitParams).toBeDefined();
    expect(commitParams.idempotencyKey).toBe(idempotencyKey);
  });
});

// =============================================================================
// 7. BATCH OPERATION IDEMPOTENCY - GAP: No batch-level keys
// =============================================================================

describe('Batch Operation Idempotency', () => {
  /**
   * GAP: Batch should have a single idempotency key
   */
  it.fails('should assign idempotency key to batch', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: batchWithIdempotency() should exist
    const batchKey = await (client as any).getBatchIdempotencyKey?.([
      { sql: 'INSERT INTO users VALUES (1)', params: [1] },
      { sql: 'INSERT INTO users VALUES (2)', params: [2] },
    ]);

    expect(batchKey).toBeDefined();
    expect(batchKey).toMatch(/^\d+-[a-z0-9]+-[a-z0-9]+$/);
  });

  /**
   * GAP: Batch statements should have sub-keys
   */
  it.fails('should assign sub-keys to batch statements', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    const batchKey = '12345-abcdefgh-batch123';

    // GAP: generateBatchSubKeys() should exist
    const subKeys = (client as any).generateBatchSubKeys?.(batchKey, 3);

    expect(subKeys).toBeDefined();
    expect(subKeys.length).toBe(3);
    expect(subKeys[0]).toBe(`${batchKey}-0`);
    expect(subKeys[1]).toBe(`${batchKey}-1`);
    expect(subKeys[2]).toBe(`${batchKey}-2`);
  });
});

// =============================================================================
// 8. SERVER ERROR HANDLING - GAP: No idempotency-specific error handling
// =============================================================================

describe('Server Error Handling', () => {
  /**
   * GAP: Should handle IDEMPOTENCY_CONFLICT error
   */
  it.fails('should handle IDEMPOTENCY_CONFLICT error', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: Client should export IdempotencyConflictError class
    // Currently no such error type exists in the client exports
    const { IdempotencyConflictError } = await import('../client.js') as any;

    expect(IdempotencyConflictError).toBeDefined();
    expect(typeof IdempotencyConflictError).toBe('function');

    // Should be able to instantiate it
    const error = new IdempotencyConflictError('12345-abc-hash', { sql: 'different' });
    expect(error.code).toBe('IDEMPOTENCY_CONFLICT');
  });

  /**
   * GAP: Should handle IDEMPOTENCY_KEY_EXPIRED error with auto-regeneration
   */
  it.fails('should auto-regenerate key on IDEMPOTENCY_KEY_EXPIRED', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: handleIdempotencyExpired() should exist
    const newKey = await (client as any).handleIdempotencyExpired?.(
      'INSERT INTO users VALUES (1)',
      [1],
      'expired-key'
    );

    expect(newKey).toBeDefined();
    expect(newKey).not.toBe('expired-key');
  });

  /**
   * GAP: Error should include idempotency key in context
   */
  it.fails('should include idempotency key in error context', async () => {
    // Import SQLError to check its structure
    const { SQLError } = await import('../client.js');

    // GAP: SQLError class should have idempotencyKey property
    // Currently SQLError only has: code, details
    // Check if idempotencyKey is a known property
    const errorInstance = new SQLError({
      code: 'TEST_ERROR',
      message: 'Test error',
    });

    // The idempotencyKey property should exist on SQLError
    expect('idempotencyKey' in errorInstance).toBe(true);
  });
});

// =============================================================================
// 9. CONFIGURATION OPTIONS - GAP: Missing advanced configuration
// =============================================================================

describe('Configuration Options', () => {
  /**
   * GAP: Should support custom key generator function
   */
  it.fails('should support custom key generator', async () => {
    const customGenerator = vi.fn().mockResolvedValue('custom-key-123');

    // GAP: keyGenerator option should exist
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      idempotency: {
        enabled: true,
        keyGenerator: customGenerator,
      },
    } as any);

    const key = await client.getIdempotencyKey('INSERT INTO users VALUES (1)', [1]);

    expect(customGenerator).toHaveBeenCalled();
    expect(key).toBe('custom-key-123');
  });

  /**
   * GAP: Should support key storage backend
   */
  it.fails('should support custom key storage', async () => {
    const storage = new Map<string, string>();

    // GAP: keyStorage option should exist
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      idempotency: {
        enabled: true,
        keyStorage: {
          get: (k: string) => storage.get(k),
          set: (k: string, v: string) => storage.set(k, v),
          delete: (k: string) => storage.delete(k),
        },
      },
    } as any);

    await client.getIdempotencyKey('INSERT INTO users VALUES (1)', [1]);

    expect(storage.size).toBeGreaterThan(0);
  });

  /**
   * GAP: Should support disabling idempotency per-request
   */
  it.fails('should support skipIdempotency option per request', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      idempotency: { enabled: true },
    });

    // GAP: getIdempotencyKey should accept skipIdempotency option
    const key = await (client as any).getIdempotencyKey?.(
      'INSERT INTO logs VALUES (1)',
      [1],
      { skipIdempotency: true }
    );

    expect(key).toBeUndefined();
  });
});

// =============================================================================
// INTEGRATION TESTS - Document end-to-end gaps
// =============================================================================

describe('Idempotency Key Integration', () => {
  /**
   * GAP: Full idempotency lifecycle with server coordination
   */
  it.fails('should enforce idempotency in complete request lifecycle', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
      idempotency: {
        enabled: true,
        ttlMs: 60000,
      },
    });

    // GAP: execWithFullIdempotency() should exist
    const result = await (client as any).execWithFullIdempotency?.(
      'INSERT INTO users VALUES (1)',
      [1]
    );

    // Result should include idempotency metadata
    expect(result.idempotencyKey).toBeDefined();
    expect(result.fromIdempotencyCache).toBeDefined();
    expect(result.idempotencyKeyExpiresAt).toBeDefined();
  });

  /**
   * GAP: Should provide idempotency status endpoint
   */
  it.fails('should provide idempotency status', async () => {
    const client = new DoSQLClient({
      url: 'ws://localhost:8080',
    });

    // GAP: getIdempotencyStatus() should exist
    const status = await (client as any).getIdempotencyStatus?.();

    expect(status).toBeDefined();
    expect(status.enabled).toBe(true);
    expect(status.cachedKeyCount).toBeDefined();
    expect(status.ttlMs).toBeDefined();
  });
});
