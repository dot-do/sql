/**
 * Null Safety Tests
 *
 * Tests for strict null checks with optional chaining edge cases:
 * - Nested optional chaining
 * - Optional chaining with nullish coalescing
 * - Optional chaining in async contexts
 * - Optional method calls
 */

import { describe, it, expect, vi } from 'vitest';
import {
  isObject,
  isNullish,
  isDefined,
  safeGet,
  safeGetPath,
  hasProperty,
  hasObjectProperty,
  getObjectProperty,
} from '../type-guards.js';
import {
  safeChain,
  safeCall,
  safeCallAsync,
  safeDeepGet,
  safeDeepSet,
  coalesceChain,
  guardedPipeline,
  asyncGuardedPipeline,
  nullSafeSpread,
  nullSafeConcat,
  assertDefined,
  assertNotNull,
  unwrapOr,
  unwrapOrThrow,
  mapNullable,
  flatMapNullable,
  filterNullable,
  isNotNullish,
  isStrictlyNull,
  isStrictlyUndefined,
} from '../null-safety.js';

describe('null-safety', () => {
  // ============================================================================
  // Nested Optional Chaining
  // ============================================================================

  describe('safeChain', () => {
    it('should safely navigate nested object properties', () => {
      const obj = {
        user: {
          profile: {
            name: 'Alice',
            address: {
              city: 'NYC',
            },
          },
        },
      };

      expect(safeChain(obj, (o) => o.user?.profile?.name)).toBe('Alice');
      expect(safeChain(obj, (o) => o.user?.profile?.address?.city)).toBe('NYC');
    });

    it('should return undefined for missing nested properties', () => {
      const obj: { user?: { profile?: { name?: string } } } = {};

      expect(safeChain(obj, (o) => o.user?.profile?.name)).toBeUndefined();
    });

    it('should return undefined for null input', () => {
      expect(safeChain(null, (o) => o?.value)).toBeUndefined();
      expect(safeChain(undefined, (o) => o?.value)).toBeUndefined();
    });

    it('should handle arrays in the chain', () => {
      const obj = {
        items: [{ id: 1 }, { id: 2 }],
      };

      expect(safeChain(obj, (o) => o.items?.[0]?.id)).toBe(1);
      expect(safeChain(obj, (o) => o.items?.[5]?.id)).toBeUndefined();
    });
  });

  describe('safeDeepGet', () => {
    it('should safely get deeply nested properties', () => {
      const obj = {
        a: {
          b: {
            c: {
              d: 'deep value',
            },
          },
        },
      };

      expect(safeDeepGet(obj, ['a', 'b', 'c', 'd'])).toBe('deep value');
    });

    it('should return undefined for missing intermediate properties', () => {
      const obj = {
        a: {
          b: null,
        },
      };

      expect(safeDeepGet(obj, ['a', 'b', 'c', 'd'])).toBeUndefined();
    });

    it('should handle arrays in the path', () => {
      const obj = {
        users: [
          { name: 'Alice' },
          { name: 'Bob' },
        ],
      };

      expect(safeDeepGet(obj, ['users', 0, 'name'])).toBe('Alice');
      expect(safeDeepGet(obj, ['users', 10, 'name'])).toBeUndefined();
    });
  });

  describe('safeDeepSet', () => {
    it('should safely set deeply nested properties', () => {
      const obj = { a: { b: { c: 1 } } };
      const result = safeDeepSet(obj, ['a', 'b', 'c'], 2);

      expect(result.a.b.c).toBe(2);
      expect(obj.a.b.c).toBe(1); // Original unchanged
    });

    it('should create intermediate objects if needed', () => {
      const obj: Record<string, unknown> = {};
      const result = safeDeepSet(obj, ['a', 'b', 'c'], 'value');

      expect(safeGetPath(result, 'a.b.c')).toBe('value');
    });

    it('should handle array indices in the path', () => {
      const obj = { items: [{ value: 1 }, { value: 2 }] };
      const result = safeDeepSet(obj, ['items', 0, 'value'], 100);

      expect((result.items as Array<{ value: number }>)[0].value).toBe(100);
    });
  });

  // ============================================================================
  // Optional Chaining with Nullish Coalescing
  // ============================================================================

  describe('coalesceChain', () => {
    it('should return the chain result if defined', () => {
      const obj = { value: 42 };
      expect(coalesceChain(obj, (o) => o.value, 0)).toBe(42);
    });

    it('should return default for undefined chain result', () => {
      const obj: { value?: number } = {};
      expect(coalesceChain(obj, (o) => o.value, 0)).toBe(0);
    });

    it('should not use default for falsy but defined values', () => {
      const obj = { value: 0, empty: '', flag: false };

      expect(coalesceChain(obj, (o) => o.value, 999)).toBe(0);
      expect(coalesceChain(obj, (o) => o.empty, 'default')).toBe('');
      expect(coalesceChain(obj, (o) => o.flag, true)).toBe(false);
    });

    it('should handle null in the chain', () => {
      const obj: { nested: { value: number } | null } = { nested: null };
      expect(coalesceChain(obj, (o) => o.nested?.value, -1)).toBe(-1);
    });
  });

  // ============================================================================
  // Optional Method Calls
  // ============================================================================

  describe('safeCall', () => {
    it('should safely call a method if it exists', () => {
      const obj = {
        greet: (name: string) => `Hello, ${name}!`,
      };

      expect(safeCall(obj, 'greet', 'World')).toBe('Hello, World!');
    });

    it('should return undefined if method does not exist', () => {
      const obj: { greet?: (name: string) => string } = {};

      expect(safeCall(obj, 'greet', 'World')).toBeUndefined();
    });

    it('should return undefined for null/undefined objects', () => {
      expect(safeCall(null, 'method')).toBeUndefined();
      expect(safeCall(undefined, 'method')).toBeUndefined();
    });

    it('should pass multiple arguments correctly', () => {
      const obj = {
        add: (a: number, b: number, c: number) => a + b + c,
      };

      expect(safeCall(obj, 'add', 1, 2, 3)).toBe(6);
    });
  });

  describe('safeCallAsync', () => {
    it('should safely call an async method if it exists', async () => {
      const obj = {
        fetchData: async (id: number) => ({ id, name: 'Item' }),
      };

      const result = await safeCallAsync(obj, 'fetchData', 42);
      expect(result).toEqual({ id: 42, name: 'Item' });
    });

    it('should return undefined if async method does not exist', async () => {
      const obj: { fetchData?: (id: number) => Promise<unknown> } = {};

      const result = await safeCallAsync(obj, 'fetchData', 42);
      expect(result).toBeUndefined();
    });

    it('should handle methods that return promises', async () => {
      const obj = {
        slowOp: (value: string) => Promise.resolve(value.toUpperCase()),
      };

      const result = await safeCallAsync(obj, 'slowOp', 'hello');
      expect(result).toBe('HELLO');
    });
  });

  // ============================================================================
  // Async Contexts
  // ============================================================================

  describe('asyncGuardedPipeline', () => {
    it('should chain async operations safely', async () => {
      const result = await asyncGuardedPipeline(
        5,
        async (n) => n * 2,
        async (n) => n + 1,
        async (n) => `Result: ${n}`
      );

      expect(result).toBe('Result: 11');
    });

    it('should short-circuit on null/undefined', async () => {
      const fn = vi.fn().mockResolvedValue(42);

      const result = await asyncGuardedPipeline(
        null as number | null,
        async (n) => (n === 0 ? null : n * 2),
        fn
      );

      expect(result).toBeUndefined();
      expect(fn).not.toHaveBeenCalled();
    });

    it('should handle mixed sync and async functions', async () => {
      const result = await asyncGuardedPipeline(
        'hello',
        (s) => s.toUpperCase(),
        async (s) => s + '!',
        (s) => s.length
      );

      expect(result).toBe(6);
    });
  });

  describe('guardedPipeline', () => {
    it('should chain sync operations safely', () => {
      const result = guardedPipeline(
        5,
        (n) => n * 2,
        (n) => n + 1,
        (n) => `Result: ${n}`
      );

      expect(result).toBe('Result: 11');
    });

    it('should short-circuit on null/undefined', () => {
      const fn = vi.fn().mockReturnValue(42);

      const result = guardedPipeline(
        null as number | null,
        (n) => n * 2,
        fn
      );

      expect(result).toBeUndefined();
      expect(fn).not.toHaveBeenCalled();
    });
  });

  // ============================================================================
  // Null-Safe Collection Operations
  // ============================================================================

  describe('nullSafeSpread', () => {
    it('should spread defined arrays', () => {
      const arr = [1, 2, 3];
      expect(nullSafeSpread(arr)).toEqual([1, 2, 3]);
    });

    it('should return empty array for null/undefined', () => {
      expect(nullSafeSpread(null)).toEqual([]);
      expect(nullSafeSpread(undefined)).toEqual([]);
    });

    it('should work in array concatenation', () => {
      const a: number[] | undefined = undefined;
      const b = [4, 5];

      expect([...nullSafeSpread(a), ...nullSafeSpread(b)]).toEqual([4, 5]);
    });
  });

  describe('nullSafeConcat', () => {
    it('should concatenate multiple nullable arrays', () => {
      const a: number[] | undefined = [1, 2];
      const b: number[] | null = null;
      const c: number[] = [5, 6];

      expect(nullSafeConcat(a, b, c)).toEqual([1, 2, 5, 6]);
    });

    it('should handle all null/undefined inputs', () => {
      expect(nullSafeConcat(null, undefined, null)).toEqual([]);
    });
  });

  describe('filterNullable', () => {
    it('should filter out null and undefined values', () => {
      const arr = [1, null, 2, undefined, 3, null];
      expect(filterNullable(arr)).toEqual([1, 2, 3]);
    });

    it('should keep falsy but defined values', () => {
      const arr = [0, '', false, null, undefined];
      expect(filterNullable(arr)).toEqual([0, '', false]);
    });
  });

  // ============================================================================
  // Type Assertions
  // ============================================================================

  describe('assertDefined', () => {
    it('should return the value if defined', () => {
      expect(assertDefined(42)).toBe(42);
      expect(assertDefined('hello')).toBe('hello');
      expect(assertDefined(false)).toBe(false);
    });

    it('should throw for null/undefined', () => {
      expect(() => assertDefined(null)).toThrow();
      expect(() => assertDefined(undefined)).toThrow();
    });

    it('should use custom error message', () => {
      expect(() => assertDefined(null, 'Value required')).toThrow('Value required');
    });
  });

  describe('assertNotNull', () => {
    it('should return the value if not null', () => {
      expect(assertNotNull(42)).toBe(42);
      expect(assertNotNull(undefined)).toBe(undefined);
    });

    it('should throw for null', () => {
      expect(() => assertNotNull(null)).toThrow();
    });
  });

  describe('unwrapOr', () => {
    it('should return value if defined', () => {
      expect(unwrapOr(42, 0)).toBe(42);
      expect(unwrapOr('hello', '')).toBe('hello');
    });

    it('should return default for null/undefined', () => {
      expect(unwrapOr(null, 0)).toBe(0);
      expect(unwrapOr(undefined, 'default')).toBe('default');
    });

    it('should not use default for falsy but defined values', () => {
      expect(unwrapOr(0, 999)).toBe(0);
      expect(unwrapOr('', 'default')).toBe('');
      expect(unwrapOr(false, true)).toBe(false);
    });
  });

  describe('unwrapOrThrow', () => {
    it('should return value if defined', () => {
      expect(unwrapOrThrow(42)).toBe(42);
    });

    it('should throw for null/undefined', () => {
      expect(() => unwrapOrThrow(null)).toThrow();
      expect(() => unwrapOrThrow(undefined)).toThrow();
    });
  });

  // ============================================================================
  // Nullable Mapping
  // ============================================================================

  describe('mapNullable', () => {
    it('should map defined values', () => {
      expect(mapNullable(5, (n) => n * 2)).toBe(10);
      expect(mapNullable('hello', (s) => s.toUpperCase())).toBe('HELLO');
    });

    it('should return undefined for null/undefined', () => {
      expect(mapNullable(null, (n: number) => n * 2)).toBeUndefined();
      expect(mapNullable(undefined, (s: string) => s.toUpperCase())).toBeUndefined();
    });
  });

  describe('flatMapNullable', () => {
    it('should flat map defined values', () => {
      const getUser = (id: number): { name: string } | undefined =>
        id === 1 ? { name: 'Alice' } : undefined;

      expect(flatMapNullable(1, getUser)).toEqual({ name: 'Alice' });
      expect(flatMapNullable(2, getUser)).toBeUndefined();
    });

    it('should return undefined for null/undefined input', () => {
      expect(flatMapNullable(null, () => ({ value: 1 }))).toBeUndefined();
    });
  });

  // ============================================================================
  // Integration with Existing Type Guards
  // ============================================================================

  describe('integration with type-guards', () => {
    it('should work with isObject', () => {
      const obj: unknown = { nested: { value: 42 } };

      if (isObject(obj)) {
        const nested = safeGet(obj, 'nested');
        if (isObject(nested)) {
          expect(safeGet(nested, 'value')).toBe(42);
        }
      }
    });

    it('should work with hasProperty', () => {
      const obj: unknown = { user: { profile: { name: 'Alice' } } };

      if (hasProperty(obj, 'user') && hasObjectProperty(obj.user, 'profile')) {
        const profile = getObjectProperty(obj.user, 'profile');
        expect(profile).toBeDefined();
        expect(safeGet(profile, 'name')).toBe('Alice');
      }
    });

    it('should work with isDefined and isNullish', () => {
      const values: (number | null | undefined)[] = [1, null, 2, undefined, 3];

      const defined = values.filter(isDefined);
      const nullish = values.filter(isNullish);

      expect(defined).toEqual([1, 2, 3]);
      expect(nullish).toEqual([null, undefined]);
    });
  });

  // ============================================================================
  // Edge Cases
  // ============================================================================

  describe('edge cases', () => {
    it('should handle empty objects', () => {
      expect(safeDeepGet({}, ['a', 'b'])).toBeUndefined();
      expect(safeChain({}, () => undefined)).toBeUndefined();
    });

    it('should handle circular references gracefully', () => {
      const obj: Record<string, unknown> = { value: 1 };
      obj.self = obj;

      // Should not throw
      expect(safeGet(obj, 'value')).toBe(1);
      expect(safeGet(obj, 'self')).toBe(obj);
    });

    it('should handle prototype chain properties', () => {
      class Base {
        getValue() {
          return 42;
        }
      }
      const obj = new Base();

      expect(safeCall(obj, 'getValue')).toBe(42);
    });

    it('should handle Symbol properties', () => {
      const sym = Symbol('test');
      const obj = { [sym]: 'symbol value' };

      // Symbol properties are not accessible via string keys
      expect(safeGet(obj, 'test')).toBeUndefined();
    });

    it('should handle arrays with holes', () => {
      // eslint-disable-next-line no-sparse-arrays
      const arr = [1, , 3]; // Sparse array with hole at index 1

      expect(safeDeepGet({ arr }, ['arr', 1])).toBeUndefined();
      expect(safeDeepGet({ arr }, ['arr', 2])).toBe(3);
    });

    it('should handle very deep nesting', () => {
      let obj: Record<string, unknown> = { value: 'deep' };
      const path: string[] = ['value'];

      // Create a very deep object
      for (let i = 0; i < 100; i++) {
        obj = { nested: obj };
        path.unshift('nested');
      }

      expect(safeDeepGet(obj, path)).toBe('deep');
    });
  });

  // ============================================================================
  // Strict Type Narrowing
  // ============================================================================

  describe('isNotNullish', () => {
    it('should return true for defined values', () => {
      expect(isNotNullish(0)).toBe(true);
      expect(isNotNullish('')).toBe(true);
      expect(isNotNullish(false)).toBe(true);
      expect(isNotNullish({})).toBe(true);
    });

    it('should return false for null and undefined', () => {
      expect(isNotNullish(null)).toBe(false);
      expect(isNotNullish(undefined)).toBe(false);
    });

    it('should work as a filter predicate', () => {
      const arr = [1, null, 2, undefined, 3];
      const filtered = arr.filter(isNotNullish);
      expect(filtered).toEqual([1, 2, 3]);
    });
  });

  describe('isStrictlyNull', () => {
    it('should return true only for null', () => {
      expect(isStrictlyNull(null)).toBe(true);
      expect(isStrictlyNull(undefined)).toBe(false);
      expect(isStrictlyNull(0)).toBe(false);
    });
  });

  describe('isStrictlyUndefined', () => {
    it('should return true only for undefined', () => {
      expect(isStrictlyUndefined(undefined)).toBe(true);
      expect(isStrictlyUndefined(null)).toBe(false);
      expect(isStrictlyUndefined(0)).toBe(false);
    });
  });

  // ============================================================================
  // Real-World Patterns from Codebase
  // ============================================================================

  describe('real-world patterns', () => {
    // Pattern from fts/index.ts - optional weights with array index
    it('should handle optional weights array with index access', () => {
      interface SearchOptions {
        bm25Params?: { k1?: number; b?: number };
      }

      const DEFAULT_BM25_PARAMS = { k1: 1.2, b: 0.75 };

      const getParams = (options?: SearchOptions) => ({
        k1: options?.bm25Params?.k1 ?? DEFAULT_BM25_PARAMS.k1,
        b: options?.bm25Params?.b ?? DEFAULT_BM25_PARAMS.b,
      });

      expect(getParams(undefined)).toEqual({ k1: 1.2, b: 0.75 });
      expect(getParams({})).toEqual({ k1: 1.2, b: 0.75 });
      expect(getParams({ bm25Params: {} })).toEqual({ k1: 1.2, b: 0.75 });
      expect(getParams({ bm25Params: { k1: 2.0 } })).toEqual({ k1: 2.0, b: 0.75 });
    });

    // Pattern from observability/tracer.ts - nested optional parent context
    it('should handle nested optional parent context', () => {
      interface SpanOptions {
        parent?: { traceId: string; spanId: string };
      }

      const generateId = (bytes: number) => 'x'.repeat(bytes * 2);

      const startSpan = (options?: SpanOptions) => {
        const traceId = options?.parent?.traceId ?? generateId(16);
        const parentSpanId = options?.parent?.spanId;

        return { traceId, parentSpanId };
      };

      expect(startSpan(undefined)).toEqual({
        traceId: 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
        parentSpanId: undefined,
      });

      expect(startSpan({ parent: { traceId: 'trace1', spanId: 'span1' } })).toEqual({
        traceId: 'trace1',
        parentSpanId: 'span1',
      });
    });

    // Pattern from hibernating-database.ts - session transaction check
    it('should handle session transaction validation', () => {
      interface Session {
        transaction?: { txId: string };
      }

      const isValidTransaction = (session: Session | undefined, txId: string) => {
        return session?.transaction?.txId === txId;
      };

      expect(isValidTransaction(undefined, 'tx1')).toBe(false);
      expect(isValidTransaction({}, 'tx1')).toBe(false);
      expect(isValidTransaction({ transaction: undefined }, 'tx1')).toBe(false);
      expect(isValidTransaction({ transaction: { txId: 'tx2' } }, 'tx1')).toBe(false);
      expect(isValidTransaction({ transaction: { txId: 'tx1' } }, 'tx1')).toBe(true);
    });

    // Pattern from migrations/runner.ts - optional async methods
    it('should handle optional async methods in database executor', async () => {
      interface DatabaseExecutor {
        exec: (sql: string) => Promise<void>;
        beginTransaction?: () => Promise<void>;
        commit?: () => Promise<void>;
        rollback?: () => Promise<void>;
      }

      const runInTransaction = async (
        db: DatabaseExecutor,
        sql: string,
        transactional: boolean
      ) => {
        if (transactional && db.beginTransaction) {
          try {
            await db.beginTransaction();
            await db.exec(sql);
            await db.commit?.();
            return { success: true };
          } catch (error) {
            await db.rollback?.();
            return { success: false, error };
          }
        } else {
          await db.exec(sql);
          return { success: true };
        }
      };

      const mockDb: DatabaseExecutor = {
        exec: vi.fn().mockResolvedValue(undefined),
        beginTransaction: vi.fn().mockResolvedValue(undefined),
        commit: vi.fn().mockResolvedValue(undefined),
        rollback: vi.fn().mockResolvedValue(undefined),
      };

      const result = await runInTransaction(mockDb, 'SELECT 1', true);
      expect(result.success).toBe(true);
      expect(mockDb.beginTransaction).toHaveBeenCalled();
      expect(mockDb.commit).toHaveBeenCalled();
    });

    // Pattern from triggers/definition.ts - optional insert method
    it('should handle optional method calls on dynamic objects', async () => {
      interface DbContext {
        tables: Record<
          string,
          { insert?: (record: Record<string, unknown>) => Promise<void> }
        >;
      }

      const insertAudit = async (ctx: DbContext, tableName: string, record: Record<string, unknown>) => {
        const table = ctx.tables[tableName];
        if (table?.insert) {
          await table.insert(record);
          return true;
        }
        return false;
      };

      const mockInsert = vi.fn().mockResolvedValue(undefined);
      const ctx: DbContext = {
        tables: {
          audit: { insert: mockInsert },
          readonly: {},
        },
      };

      expect(await insertAudit(ctx, 'audit', { event: 'test' })).toBe(true);
      expect(mockInsert).toHaveBeenCalledWith({ event: 'test' });

      expect(await insertAudit(ctx, 'readonly', { event: 'test' })).toBe(false);
      expect(await insertAudit(ctx, 'missing', { event: 'test' })).toBe(false);
    });

    // Pattern from planner/cache.ts - regex match array access
    it('should handle regex match array access safely', () => {
      const extractTableName = (sql: string) => {
        const alterMatch = sql.match(/ALTER\s+TABLE\s+(\w+)/i);
        const dropMatch = sql.match(/DROP\s+TABLE\s+(\w+)/i);
        const createMatch = sql.match(/CREATE\s+TABLE\s+(\w+)/i);

        return alterMatch?.[1] || dropMatch?.[1] || createMatch?.[1] || null;
      };

      expect(extractTableName('ALTER TABLE users ADD COLUMN email TEXT')).toBe('users');
      expect(extractTableName('DROP TABLE orders')).toBe('orders');
      expect(extractTableName('CREATE TABLE products (id INT)')).toBe('products');
      expect(extractTableName('SELECT * FROM users')).toBe(null);
    });

    // Pattern from cdc/types.ts - nested JSON context metadata
    it('should handle nested JSON metadata extraction', () => {
      interface SerializedError {
        code: string;
        message: string;
        context?: {
          metadata?: {
            lsn?: string;
            segmentId?: string;
          };
        };
      }

      const extractLsn = (json: SerializedError): bigint | undefined => {
        const lsnStr = json.context?.metadata?.lsn as string | undefined;
        return lsnStr !== undefined ? BigInt(lsnStr) : undefined;
      };

      expect(extractLsn({ code: 'ERR', message: 'test' })).toBeUndefined();
      expect(
        extractLsn({ code: 'ERR', message: 'test', context: {} })
      ).toBeUndefined();
      expect(
        extractLsn({ code: 'ERR', message: 'test', context: { metadata: {} } })
      ).toBeUndefined();
      expect(
        extractLsn({
          code: 'ERR',
          message: 'test',
          context: { metadata: { lsn: '12345' } },
        })
      ).toBe(BigInt(12345));
    });

    // Pattern from fts/ranking.ts - array weights with index access
    it('should handle array weights with nullish coalescing at index', () => {
      const calculateScore = (scores: number[], weights?: number[]) => {
        let total = 0;
        for (let i = 0; i < scores.length; i++) {
          const weight = weights?.[i] ?? 1;
          total += scores[i] * weight;
        }
        return total;
      };

      expect(calculateScore([10, 20, 30])).toBe(60);
      expect(calculateScore([10, 20, 30], [1, 2, 3])).toBe(140);
      expect(calculateScore([10, 20, 30], [2])).toBe(20 + 20 + 30);
    });
  });
});
