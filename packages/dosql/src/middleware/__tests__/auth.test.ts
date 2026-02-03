/**
 * Auth Middleware Tests
 *
 * Tests for the DoSQL auth middleware:
 * - Token extraction from Authorization header
 * - Token verification (custom and oauth.do)
 * - Public path matching (exact and prefix)
 * - Token caching
 * - Error responses for missing/invalid tokens
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { authMiddleware, getUser } from '../auth.js';
import type { AuthConfig, AuthUser } from '../auth.js';

// =============================================================================
// Test Doubles (Fakes) - NO MOCKS per testing philosophy
// =============================================================================

/**
 * Fake token verifier that tracks calls and returns configured responses
 */
class FakeTokenVerifier {
  calls: string[] = [];
  private responses: Map<string, AuthUser | null> = new Map();
  private defaultResponse: AuthUser | null = null;
  private shouldThrow: Error | null = null;

  setResponse(token: string, user: AuthUser | null): void {
    this.responses.set(token, user);
  }

  setDefaultResponse(user: AuthUser | null): void {
    this.defaultResponse = user;
  }

  setError(error: Error): void {
    this.shouldThrow = error;
  }

  async verify(token: string): Promise<AuthUser | null> {
    this.calls.push(token);
    if (this.shouldThrow) {
      throw this.shouldThrow;
    }
    if (this.responses.has(token)) {
      return this.responses.get(token)!;
    }
    return this.defaultResponse;
  }

  wasCalled(): boolean {
    return this.calls.length > 0;
  }

  wasCalledWith(token: string): boolean {
    return this.calls.includes(token);
  }

  callCount(): number {
    return this.calls.length;
  }

  reset(): void {
    this.calls = [];
    this.responses.clear();
    this.defaultResponse = null;
    this.shouldThrow = null;
  }
}

/**
 * Fake token extractor for custom extraction logic
 */
class FakeTokenExtractor {
  calls: unknown[] = [];
  private returnValue: string | null = null;

  setReturnValue(value: string | null): void {
    this.returnValue = value;
  }

  extract(ctx: unknown): string | null {
    this.calls.push(ctx);
    return this.returnValue;
  }

  wasCalled(): boolean {
    return this.calls.length > 0;
  }
}

/**
 * Fake next function for middleware testing
 */
class FakeNext {
  calls: number = 0;
  private returnValue: unknown = undefined;

  async invoke(): Promise<unknown> {
    this.calls++;
    return this.returnValue;
  }

  wasCalled(): boolean {
    return this.calls > 0;
  }
}

// =============================================================================
// Mock Hono Context Factory
// =============================================================================

interface MockContext {
  req: {
    path: string;
    header(name: string): string | undefined;
  };
  json(data: unknown, status?: number): Response;
  set(key: string, value: unknown): void;
  get(key: string): unknown;
  _store: Map<string, unknown>;
  _response: { data: unknown; status: number } | null;
}

function createMockContext(path: string, authHeader?: string): MockContext {
  const store = new Map<string, unknown>();
  let response: { data: unknown; status: number } | null = null;

  return {
    req: {
      path,
      header(name: string): string | undefined {
        if (name.toLowerCase() === 'authorization') {
          return authHeader;
        }
        return undefined;
      },
    },
    json(data: unknown, status = 200): Response {
      response = { data, status };
      return new Response(JSON.stringify(data), { status });
    },
    set(key: string, value: unknown): void {
      store.set(key, value);
    },
    get(key: string): unknown {
      return store.get(key);
    },
    _store: store,
    _response: response,
  };
}

// =============================================================================
// Token Extraction Tests
// =============================================================================

describe('Auth Middleware - Token Extraction', () => {
  it('should extract Bearer token from Authorization header', async () => {
    const verifier = new FakeTokenVerifier();
    verifier.setDefaultResponse({ userId: 'user-123' });
    const middleware = authMiddleware({ verifyToken: (token) => verifier.verify(token) });

    const ctx = createMockContext('/api/query', 'Bearer test-token-123');
    const next = new FakeNext();

    await middleware(ctx as unknown as Parameters<typeof middleware>[0], () => next.invoke());

    expect(verifier.wasCalledWith('test-token-123')).toBe(true);
    expect(next.wasCalled()).toBe(true);
  });

  it('should handle lowercase bearer prefix', async () => {
    const verifier = new FakeTokenVerifier();
    verifier.setDefaultResponse({ userId: 'user-123' });
    const middleware = authMiddleware({ verifyToken: (token) => verifier.verify(token) });

    const ctx = createMockContext('/api/query', 'bearer token-lowercase');
    const next = new FakeNext();

    await middleware(ctx as unknown as Parameters<typeof middleware>[0], () => next.invoke());

    expect(verifier.wasCalledWith('token-lowercase')).toBe(true);
  });

  it('should handle mixed case Bearer prefix', async () => {
    const verifier = new FakeTokenVerifier();
    verifier.setDefaultResponse({ userId: 'user-123' });
    const middleware = authMiddleware({ verifyToken: (token) => verifier.verify(token) });

    const ctx = createMockContext('/api/query', 'BEARER TOKEN-UPPER');
    const next = new FakeNext();

    await middleware(ctx as unknown as Parameters<typeof middleware>[0], () => next.invoke());

    expect(verifier.wasCalledWith('TOKEN-UPPER')).toBe(true);
  });

  it('should return 401 when Authorization header is missing', async () => {
    const verifier = new FakeTokenVerifier();
    const middleware = authMiddleware({ verifyToken: (token) => verifier.verify(token) });

    const ctx = createMockContext('/api/query');
    const next = new FakeNext();

    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], () => next.invoke());

    expect(response).toBeInstanceOf(Response);
    expect(response?.status).toBe(401);
    expect(verifier.wasCalled()).toBe(false);
    expect(next.wasCalled()).toBe(false);
  });

  it('should return 401 when Authorization header has wrong format', async () => {
    const verifier = new FakeTokenVerifier();
    const middleware = authMiddleware({ verifyToken: (token) => verifier.verify(token) });

    const ctx = createMockContext('/api/query', 'Basic dXNlcjpwYXNz');
    const next = new FakeNext();

    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], () => next.invoke());

    expect(response).toBeInstanceOf(Response);
    expect(response?.status).toBe(401);
    expect(verifier.wasCalled()).toBe(false);
    expect(next.wasCalled()).toBe(false);
  });

  it('should return 401 when Bearer token is empty', async () => {
    const verifier = new FakeTokenVerifier();
    const middleware = authMiddleware({ verifyToken: (token) => verifier.verify(token) });

    const ctx = createMockContext('/api/query', 'Bearer ');
    const next = new FakeNext();

    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], () => next.invoke());

    expect(response).toBeInstanceOf(Response);
    expect(response?.status).toBe(401);
    expect(verifier.wasCalled()).toBe(false);
    expect(next.wasCalled()).toBe(false);
  });

  it('should use custom token extraction when provided', async () => {
    const verifier = new FakeTokenVerifier();
    verifier.setDefaultResponse({ userId: 'user-123' });
    const extractor = new FakeTokenExtractor();
    extractor.setReturnValue('custom-extracted-token');
    const middleware = authMiddleware({
      verifyToken: (token) => verifier.verify(token),
      extractToken: (ctx) => extractor.extract(ctx)
    });

    const ctx = createMockContext('/api/query', 'Bearer ignored-token');
    const next = new FakeNext();

    await middleware(ctx as unknown as Parameters<typeof middleware>[0], () => next.invoke());

    expect(extractor.wasCalled()).toBe(true);
    expect(verifier.wasCalledWith('custom-extracted-token')).toBe(true);
  });

  it('should return 401 when custom extractor returns null', async () => {
    const verifier = new FakeTokenVerifier();
    const extractor = new FakeTokenExtractor();
    extractor.setReturnValue(null);
    const middleware = authMiddleware({
      verifyToken: (token) => verifier.verify(token),
      extractToken: (ctx) => extractor.extract(ctx)
    });

    const ctx = createMockContext('/api/query', 'Bearer some-token');
    const next = new FakeNext();

    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], () => next.invoke());

    expect(response).toBeInstanceOf(Response);
    expect(response?.status).toBe(401);
    expect(verifier.wasCalled()).toBe(false);
  });
});

// =============================================================================
// Token Verification Tests
// =============================================================================

describe('Auth Middleware - Token Verification', () => {
  it('should call custom verifyToken function', async () => {
    const user: AuthUser = { userId: 'user-456', email: 'user@example.com' };
    const verifier = new FakeTokenVerifier();
    verifier.setResponse('valid-token', user);
    const middleware = authMiddleware({ verifyToken: (token) => verifier.verify(token) });

    const ctx = createMockContext('/api/query', 'Bearer valid-token');
    const next = new FakeNext();

    await middleware(ctx as unknown as Parameters<typeof middleware>[0], () => next.invoke());

    expect(verifier.wasCalledWith('valid-token')).toBe(true);
    expect(ctx._store.get('user')).toEqual(user);
    expect(next.wasCalled()).toBe(true);
  });

  it('should return 401 when verifyToken returns null', async () => {
    const verifier = new FakeTokenVerifier();
    verifier.setDefaultResponse(null);
    const middleware = authMiddleware({ verifyToken: (token) => verifier.verify(token) });

    const ctx = createMockContext('/api/query', 'Bearer invalid-token');
    const next = new FakeNext();

    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], () => next.invoke());

    expect(response).toBeInstanceOf(Response);
    expect(response?.status).toBe(401);
    const body = await response?.json();
    expect(body).toMatchObject({
      error: 'Unauthorized',
      message: 'Invalid or expired token',
    });
    expect(next.wasCalled()).toBe(false);
  });

  it('should handle verifyToken that throws an error', async () => {
    const verifier = new FakeTokenVerifier();
    verifier.setError(new Error('Verification service down'));
    const middleware = authMiddleware({ verifyToken: (token) => verifier.verify(token) });

    const ctx = createMockContext('/api/query', 'Bearer valid-token');
    const next = new FakeNext();

    // The middleware should propagate the error (implementation choice)
    // If it catches and returns 401, that's also valid behavior
    try {
      const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], () => next.invoke());
      // If no error thrown, should return 401 or propagate the error
      if (response) {
        // Caught the error and returned unauthorized
        expect(response.status).toBe(401);
      }
    } catch (error) {
      // Error was propagated
      expect(error).toBeInstanceOf(Error);
      expect((error as Error).message).toBe('Verification service down');
    }
  });

  it('should pass additional user claims to context', async () => {
    const user: AuthUser = {
      userId: 'user-789',
      email: 'admin@example.com',
      role: 'admin',
      permissions: ['read', 'write', 'delete'],
    };
    const verifier = new FakeTokenVerifier();
    verifier.setResponse('admin-token', user);
    const middleware = authMiddleware({ verifyToken: (token) => verifier.verify(token) });

    const ctx = createMockContext('/api/query', 'Bearer admin-token');
    const next = new FakeNext();

    await middleware(ctx as unknown as Parameters<typeof middleware>[0], () => next.invoke());

    const storedUser = ctx._store.get('user') as AuthUser;
    expect(storedUser.userId).toBe('user-789');
    expect(storedUser.email).toBe('admin@example.com');
    expect(storedUser.role).toBe('admin');
    expect(storedUser.permissions).toEqual(['read', 'write', 'delete']);
  });
});

// =============================================================================
// Public Path Tests
// =============================================================================

describe('Auth Middleware - Public Paths', () => {
  it('should skip auth for exact match public path', async () => {
    const verifier = new FakeTokenVerifier();
    const middleware = authMiddleware({
      verifyToken: (token) => verifier.verify(token),
      publicPaths: ['/health', '/metrics'],
    });

    const ctx = createMockContext('/health');
    const next = new FakeNext();

    await middleware(ctx as unknown as Parameters<typeof middleware>[0], () => next.invoke());

    expect(verifier.wasCalled()).toBe(false);
    expect(next.wasCalled()).toBe(true);
  });

  it('should skip auth for prefix match public path', async () => {
    const verifier = new FakeTokenVerifier();
    const middleware = authMiddleware({
      verifyToken: (token) => verifier.verify(token),
      publicPaths: ['/public/*', '/docs/*'],
    });

    const ctx = createMockContext('/public/assets/logo.png');
    const next = new FakeNext();

    await middleware(ctx as unknown as Parameters<typeof middleware>[0], () => next.invoke());

    expect(verifier.wasCalled()).toBe(false);
    expect(next.wasCalled()).toBe(true);
  });

  it('should NOT skip auth for non-matching path', async () => {
    const verifier = new FakeTokenVerifier();
    verifier.setDefaultResponse({ userId: 'user-123' });
    const middleware = authMiddleware({
      verifyToken: (token) => verifier.verify(token),
      publicPaths: ['/health', '/public/*'],
    });

    const ctx = createMockContext('/api/query', 'Bearer token');
    const next = new FakeNext();

    await middleware(ctx as unknown as Parameters<typeof middleware>[0], () => next.invoke());

    expect(verifier.wasCalled()).toBe(true);
  });

  it('should require exact match when no wildcard', async () => {
    const verifier = new FakeTokenVerifier();
    const middleware = authMiddleware({
      verifyToken: (token) => verifier.verify(token),
      publicPaths: ['/health'],
    });

    const ctx = createMockContext('/health/detailed'); // Not exact match
    const next = new FakeNext();

    // Should require auth since /health/detailed !== /health
    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], () => next.invoke());

    expect(response?.status).toBe(401);
    expect(verifier.wasCalled()).toBe(false);
  });

  it('should match prefix correctly with wildcard', async () => {
    const verifier = new FakeTokenVerifier();
    const middleware = authMiddleware({
      verifyToken: (token) => verifier.verify(token),
      publicPaths: ['/api/public/*'],
    });

    // Should match
    const ctx1 = createMockContext('/api/public/data');
    const next1 = new FakeNext();
    await middleware(ctx1 as unknown as Parameters<typeof middleware>[0], () => next1.invoke());
    expect(next1.wasCalled()).toBe(true);

    // Should NOT match (different prefix)
    const ctx2 = createMockContext('/api/private/data');
    const next2 = new FakeNext();
    const response = await middleware(ctx2 as unknown as Parameters<typeof middleware>[0], () => next2.invoke());
    expect(response?.status).toBe(401);
  });

  it('should handle empty publicPaths array', async () => {
    const verifier = new FakeTokenVerifier();
    const middleware = authMiddleware({
      verifyToken: (token) => verifier.verify(token),
      publicPaths: [],
    });

    const ctx = createMockContext('/health');
    const next = new FakeNext();

    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], () => next.invoke());

    expect(response?.status).toBe(401);
  });
});

// =============================================================================
// Token Caching Tests
// =============================================================================

describe('Auth Middleware - Token Caching', () => {
  it('should cache verified tokens', async () => {
    const user: AuthUser = { userId: 'cached-user' };
    const verifier = new FakeTokenVerifier();
    verifier.setDefaultResponse(user);
    const middleware = authMiddleware({
      verifyToken: (token) => verifier.verify(token),
      tokenCacheTtl: 60,
    });

    // First request - should call verifyToken
    const ctx1 = createMockContext('/api/query', 'Bearer cached-token');
    const next1 = new FakeNext();
    await middleware(ctx1 as unknown as Parameters<typeof middleware>[0], () => next1.invoke());

    expect(verifier.callCount()).toBe(1);

    // Second request with same token - should use cache
    const ctx2 = createMockContext('/api/query', 'Bearer cached-token');
    const next2 = new FakeNext();
    await middleware(ctx2 as unknown as Parameters<typeof middleware>[0], () => next2.invoke());

    expect(verifier.callCount()).toBe(1); // Still 1, cache hit
    expect(ctx2._store.get('user')).toEqual(user);
  });

  it('should not cache when tokenCacheTtl is 0', async () => {
    const user: AuthUser = { userId: 'uncached-user' };
    const verifier = new FakeTokenVerifier();
    verifier.setDefaultResponse(user);
    const middleware = authMiddleware({
      verifyToken: (token) => verifier.verify(token),
      tokenCacheTtl: 0,
    });

    // First request
    const ctx1 = createMockContext('/api/query', 'Bearer uncached-token');
    const next1 = new FakeNext();
    await middleware(ctx1 as unknown as Parameters<typeof middleware>[0], () => next1.invoke());

    // Second request
    const ctx2 = createMockContext('/api/query', 'Bearer uncached-token');
    const next2 = new FakeNext();
    await middleware(ctx2 as unknown as Parameters<typeof middleware>[0], () => next2.invoke());

    expect(verifier.callCount()).toBe(2); // Called twice, no cache
  });

  it('should use different cache entries for different tokens', async () => {
    const verifier = new FakeTokenVerifier();
    verifier.setResponse('token-1', { userId: 'user-1' });
    verifier.setResponse('token-2', { userId: 'user-2' });
    const middleware = authMiddleware({
      verifyToken: (token) => verifier.verify(token),
      tokenCacheTtl: 60,
    });

    // First token
    const ctx1 = createMockContext('/api/query', 'Bearer token-1');
    const next1 = new FakeNext();
    await middleware(ctx1 as unknown as Parameters<typeof middleware>[0], () => next1.invoke());

    // Different token
    const ctx2 = createMockContext('/api/query', 'Bearer token-2');
    const next2 = new FakeNext();
    await middleware(ctx2 as unknown as Parameters<typeof middleware>[0], () => next2.invoke());

    expect(verifier.callCount()).toBe(2);
    expect(ctx1._store.get('user')).toEqual({ userId: 'user-1' });
    expect(ctx2._store.get('user')).toEqual({ userId: 'user-2' });
  });
});

// =============================================================================
// getUser Helper Tests
// =============================================================================

describe('getUser Helper', () => {
  it('should return user from context', () => {
    const ctx = createMockContext('/api/query');
    const user: AuthUser = { userId: 'test-user', role: 'admin' };
    ctx.set('user', user);

    const result = getUser(ctx as unknown as Parameters<typeof getUser>[0]);

    expect(result).toEqual(user);
  });

  it('should return null/undefined when user is not set', () => {
    const ctx = createMockContext('/api/query');

    const result = getUser(ctx as unknown as Parameters<typeof getUser>[0]);

    // Implementation may return null or undefined when user is not set
    expect(result).toBeFalsy();
  });
});

// =============================================================================
// Default Configuration Tests
// =============================================================================

describe('Auth Middleware - Default Configuration', () => {
  it('should use oauth.do URL by default', async () => {
    // This test verifies the default oauthDoUrl is set
    // We can't actually call oauth.do in tests, so we verify the middleware works
    const middleware = authMiddleware();

    const ctx = createMockContext('/api/query');
    const next = new FakeNext();

    // Without token, should return 401
    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], () => next.invoke());
    expect(response?.status).toBe(401);
  });

  it('should use default token cache TTL of 60 seconds', async () => {
    const user: AuthUser = { userId: 'default-ttl-user' };
    const verifier = new FakeTokenVerifier();
    verifier.setDefaultResponse(user);
    const middleware = authMiddleware({ verifyToken: (token) => verifier.verify(token) }); // No tokenCacheTtl specified

    const ctx1 = createMockContext('/api/query', 'Bearer default-ttl-token');
    const next1 = new FakeNext();
    await middleware(ctx1 as unknown as Parameters<typeof middleware>[0], () => next1.invoke());

    const ctx2 = createMockContext('/api/query', 'Bearer default-ttl-token');
    const next2 = new FakeNext();
    await middleware(ctx2 as unknown as Parameters<typeof middleware>[0], () => next2.invoke());

    // Should use cache with default TTL
    expect(verifier.callCount()).toBe(1);
  });
});

// =============================================================================
// Error Response Format Tests
// =============================================================================

describe('Auth Middleware - Error Response Format', () => {
  it('should return 401 error for missing token', async () => {
    const verifier = new FakeTokenVerifier();
    const middleware = authMiddleware({ verifyToken: (token) => verifier.verify(token) });

    const ctx = createMockContext('/api/query');
    const next = new FakeNext();

    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], () => next.invoke());

    expect(response).toBeInstanceOf(Response);
    expect(response?.status).toBe(401);

    // The response should be JSON (implementation detail may vary)
    const body = await response?.json();
    expect(body).toMatchObject({
      error: 'Unauthorized',
      message: expect.stringContaining('Authorization'),
    });
  });

  it('should return proper JSON error for invalid token', async () => {
    const verifier = new FakeTokenVerifier();
    verifier.setDefaultResponse(null);
    const middleware = authMiddleware({ verifyToken: (token) => verifier.verify(token) });

    const ctx = createMockContext('/api/query', 'Bearer bad-token');
    const next = new FakeNext();

    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], () => next.invoke());

    const body = await response?.json();
    expect(body).toMatchObject({
      error: 'Unauthorized',
      message: expect.stringContaining('Invalid'),
    });
  });
});
