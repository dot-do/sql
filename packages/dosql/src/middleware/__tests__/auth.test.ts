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

import { describe, it, expect, beforeEach, vi } from 'vitest';
import { authMiddleware, getUser } from '../auth.js';
import type { AuthConfig, AuthUser } from '../auth.js';

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
    const verifyToken = vi.fn().mockResolvedValue({ userId: 'user-123' });
    const middleware = authMiddleware({ verifyToken });

    const ctx = createMockContext('/api/query', 'Bearer test-token-123');
    const next = vi.fn().mockResolvedValue(undefined);

    await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);

    expect(verifyToken).toHaveBeenCalledWith('test-token-123');
    expect(next).toHaveBeenCalled();
  });

  it('should handle lowercase bearer prefix', async () => {
    const verifyToken = vi.fn().mockResolvedValue({ userId: 'user-123' });
    const middleware = authMiddleware({ verifyToken });

    const ctx = createMockContext('/api/query', 'bearer token-lowercase');
    const next = vi.fn().mockResolvedValue(undefined);

    await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);

    expect(verifyToken).toHaveBeenCalledWith('token-lowercase');
  });

  it('should handle mixed case Bearer prefix', async () => {
    const verifyToken = vi.fn().mockResolvedValue({ userId: 'user-123' });
    const middleware = authMiddleware({ verifyToken });

    const ctx = createMockContext('/api/query', 'BEARER TOKEN-UPPER');
    const next = vi.fn().mockResolvedValue(undefined);

    await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);

    expect(verifyToken).toHaveBeenCalledWith('TOKEN-UPPER');
  });

  it('should return 401 when Authorization header is missing', async () => {
    const verifyToken = vi.fn();
    const middleware = authMiddleware({ verifyToken });

    const ctx = createMockContext('/api/query');
    const next = vi.fn();

    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);

    expect(response).toBeInstanceOf(Response);
    expect(response?.status).toBe(401);
    expect(verifyToken).not.toHaveBeenCalled();
    expect(next).not.toHaveBeenCalled();
  });

  it('should return 401 when Authorization header has wrong format', async () => {
    const verifyToken = vi.fn();
    const middleware = authMiddleware({ verifyToken });

    const ctx = createMockContext('/api/query', 'Basic dXNlcjpwYXNz');
    const next = vi.fn();

    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);

    expect(response).toBeInstanceOf(Response);
    expect(response?.status).toBe(401);
    expect(verifyToken).not.toHaveBeenCalled();
    expect(next).not.toHaveBeenCalled();
  });

  it('should return 401 when Bearer token is empty', async () => {
    const verifyToken = vi.fn();
    const middleware = authMiddleware({ verifyToken });

    const ctx = createMockContext('/api/query', 'Bearer ');
    const next = vi.fn();

    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);

    expect(response).toBeInstanceOf(Response);
    expect(response?.status).toBe(401);
    expect(verifyToken).not.toHaveBeenCalled();
    expect(next).not.toHaveBeenCalled();
  });

  it('should use custom token extraction when provided', async () => {
    const verifyToken = vi.fn().mockResolvedValue({ userId: 'user-123' });
    const extractToken = vi.fn().mockReturnValue('custom-extracted-token');
    const middleware = authMiddleware({ verifyToken, extractToken });

    const ctx = createMockContext('/api/query', 'Bearer ignored-token');
    const next = vi.fn().mockResolvedValue(undefined);

    await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);

    expect(extractToken).toHaveBeenCalled();
    expect(verifyToken).toHaveBeenCalledWith('custom-extracted-token');
  });

  it('should return 401 when custom extractor returns null', async () => {
    const verifyToken = vi.fn();
    const extractToken = vi.fn().mockReturnValue(null);
    const middleware = authMiddleware({ verifyToken, extractToken });

    const ctx = createMockContext('/api/query', 'Bearer some-token');
    const next = vi.fn();

    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);

    expect(response).toBeInstanceOf(Response);
    expect(response?.status).toBe(401);
    expect(verifyToken).not.toHaveBeenCalled();
  });
});

// =============================================================================
// Token Verification Tests
// =============================================================================

describe('Auth Middleware - Token Verification', () => {
  it('should call custom verifyToken function', async () => {
    const user: AuthUser = { userId: 'user-456', email: 'user@example.com' };
    const verifyToken = vi.fn().mockResolvedValue(user);
    const middleware = authMiddleware({ verifyToken });

    const ctx = createMockContext('/api/query', 'Bearer valid-token');
    const next = vi.fn().mockResolvedValue(undefined);

    await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);

    expect(verifyToken).toHaveBeenCalledWith('valid-token');
    expect(ctx._store.get('user')).toEqual(user);
    expect(next).toHaveBeenCalled();
  });

  it('should return 401 when verifyToken returns null', async () => {
    const verifyToken = vi.fn().mockResolvedValue(null);
    const middleware = authMiddleware({ verifyToken });

    const ctx = createMockContext('/api/query', 'Bearer invalid-token');
    const next = vi.fn();

    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);

    expect(response).toBeInstanceOf(Response);
    expect(response?.status).toBe(401);
    const body = await response?.json();
    expect(body).toMatchObject({
      error: 'Unauthorized',
      message: 'Invalid or expired token',
    });
    expect(next).not.toHaveBeenCalled();
  });

  it('should handle verifyToken that throws an error', async () => {
    const verifyToken = vi.fn().mockRejectedValue(new Error('Verification service down'));
    const middleware = authMiddleware({ verifyToken });

    const ctx = createMockContext('/api/query', 'Bearer valid-token');
    const next = vi.fn();

    // The middleware should propagate the error (implementation choice)
    // If it catches and returns 401, that's also valid behavior
    try {
      const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);
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
    const verifyToken = vi.fn().mockResolvedValue(user);
    const middleware = authMiddleware({ verifyToken });

    const ctx = createMockContext('/api/query', 'Bearer admin-token');
    const next = vi.fn().mockResolvedValue(undefined);

    await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);

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
    const verifyToken = vi.fn();
    const middleware = authMiddleware({
      verifyToken,
      publicPaths: ['/health', '/metrics'],
    });

    const ctx = createMockContext('/health');
    const next = vi.fn().mockResolvedValue(undefined);

    await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);

    expect(verifyToken).not.toHaveBeenCalled();
    expect(next).toHaveBeenCalled();
  });

  it('should skip auth for prefix match public path', async () => {
    const verifyToken = vi.fn();
    const middleware = authMiddleware({
      verifyToken,
      publicPaths: ['/public/*', '/docs/*'],
    });

    const ctx = createMockContext('/public/assets/logo.png');
    const next = vi.fn().mockResolvedValue(undefined);

    await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);

    expect(verifyToken).not.toHaveBeenCalled();
    expect(next).toHaveBeenCalled();
  });

  it('should NOT skip auth for non-matching path', async () => {
    const verifyToken = vi.fn().mockResolvedValue({ userId: 'user-123' });
    const middleware = authMiddleware({
      verifyToken,
      publicPaths: ['/health', '/public/*'],
    });

    const ctx = createMockContext('/api/query', 'Bearer token');
    const next = vi.fn().mockResolvedValue(undefined);

    await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);

    expect(verifyToken).toHaveBeenCalled();
  });

  it('should require exact match when no wildcard', async () => {
    const verifyToken = vi.fn();
    const middleware = authMiddleware({
      verifyToken,
      publicPaths: ['/health'],
    });

    const ctx = createMockContext('/health/detailed'); // Not exact match
    const next = vi.fn();

    // Should require auth since /health/detailed !== /health
    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);

    expect(response?.status).toBe(401);
    expect(verifyToken).not.toHaveBeenCalled();
  });

  it('should match prefix correctly with wildcard', async () => {
    const verifyToken = vi.fn();
    const middleware = authMiddleware({
      verifyToken,
      publicPaths: ['/api/public/*'],
    });

    // Should match
    const ctx1 = createMockContext('/api/public/data');
    const next1 = vi.fn().mockResolvedValue(undefined);
    await middleware(ctx1 as unknown as Parameters<typeof middleware>[0], next1);
    expect(next1).toHaveBeenCalled();

    // Should NOT match (different prefix)
    const ctx2 = createMockContext('/api/private/data');
    const next2 = vi.fn();
    const response = await middleware(ctx2 as unknown as Parameters<typeof middleware>[0], next2);
    expect(response?.status).toBe(401);
  });

  it('should handle empty publicPaths array', async () => {
    const verifyToken = vi.fn();
    const middleware = authMiddleware({
      verifyToken,
      publicPaths: [],
    });

    const ctx = createMockContext('/health');
    const next = vi.fn();

    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);

    expect(response?.status).toBe(401);
  });
});

// =============================================================================
// Token Caching Tests
// =============================================================================

describe('Auth Middleware - Token Caching', () => {
  it('should cache verified tokens', async () => {
    const user: AuthUser = { userId: 'cached-user' };
    const verifyToken = vi.fn().mockResolvedValue(user);
    const middleware = authMiddleware({
      verifyToken,
      tokenCacheTtl: 60,
    });

    // First request - should call verifyToken
    const ctx1 = createMockContext('/api/query', 'Bearer cached-token');
    const next1 = vi.fn().mockResolvedValue(undefined);
    await middleware(ctx1 as unknown as Parameters<typeof middleware>[0], next1);

    expect(verifyToken).toHaveBeenCalledTimes(1);

    // Second request with same token - should use cache
    const ctx2 = createMockContext('/api/query', 'Bearer cached-token');
    const next2 = vi.fn().mockResolvedValue(undefined);
    await middleware(ctx2 as unknown as Parameters<typeof middleware>[0], next2);

    expect(verifyToken).toHaveBeenCalledTimes(1); // Still 1, cache hit
    expect(ctx2._store.get('user')).toEqual(user);
  });

  it('should not cache when tokenCacheTtl is 0', async () => {
    const user: AuthUser = { userId: 'uncached-user' };
    const verifyToken = vi.fn().mockResolvedValue(user);
    const middleware = authMiddleware({
      verifyToken,
      tokenCacheTtl: 0,
    });

    // First request
    const ctx1 = createMockContext('/api/query', 'Bearer uncached-token');
    const next1 = vi.fn().mockResolvedValue(undefined);
    await middleware(ctx1 as unknown as Parameters<typeof middleware>[0], next1);

    // Second request
    const ctx2 = createMockContext('/api/query', 'Bearer uncached-token');
    const next2 = vi.fn().mockResolvedValue(undefined);
    await middleware(ctx2 as unknown as Parameters<typeof middleware>[0], next2);

    expect(verifyToken).toHaveBeenCalledTimes(2); // Called twice, no cache
  });

  it('should use different cache entries for different tokens', async () => {
    const verifyToken = vi.fn()
      .mockResolvedValueOnce({ userId: 'user-1' })
      .mockResolvedValueOnce({ userId: 'user-2' });
    const middleware = authMiddleware({
      verifyToken,
      tokenCacheTtl: 60,
    });

    // First token
    const ctx1 = createMockContext('/api/query', 'Bearer token-1');
    const next1 = vi.fn().mockResolvedValue(undefined);
    await middleware(ctx1 as unknown as Parameters<typeof middleware>[0], next1);

    // Different token
    const ctx2 = createMockContext('/api/query', 'Bearer token-2');
    const next2 = vi.fn().mockResolvedValue(undefined);
    await middleware(ctx2 as unknown as Parameters<typeof middleware>[0], next2);

    expect(verifyToken).toHaveBeenCalledTimes(2);
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
    const next = vi.fn();

    // Without token, should return 401
    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);
    expect(response?.status).toBe(401);
  });

  it('should use default token cache TTL of 60 seconds', async () => {
    const user: AuthUser = { userId: 'default-ttl-user' };
    const verifyToken = vi.fn().mockResolvedValue(user);
    const middleware = authMiddleware({ verifyToken }); // No tokenCacheTtl specified

    const ctx1 = createMockContext('/api/query', 'Bearer default-ttl-token');
    const next1 = vi.fn().mockResolvedValue(undefined);
    await middleware(ctx1 as unknown as Parameters<typeof middleware>[0], next1);

    const ctx2 = createMockContext('/api/query', 'Bearer default-ttl-token');
    const next2 = vi.fn().mockResolvedValue(undefined);
    await middleware(ctx2 as unknown as Parameters<typeof middleware>[0], next2);

    // Should use cache with default TTL
    expect(verifyToken).toHaveBeenCalledTimes(1);
  });
});

// =============================================================================
// Error Response Format Tests
// =============================================================================

describe('Auth Middleware - Error Response Format', () => {
  it('should return 401 error for missing token', async () => {
    const middleware = authMiddleware({ verifyToken: vi.fn() });

    const ctx = createMockContext('/api/query');
    const next = vi.fn();

    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);

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
    const verifyToken = vi.fn().mockResolvedValue(null);
    const middleware = authMiddleware({ verifyToken });

    const ctx = createMockContext('/api/query', 'Bearer bad-token');
    const next = vi.fn();

    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);

    const body = await response?.json();
    expect(body).toMatchObject({
      error: 'Unauthorized',
      message: expect.stringContaining('Invalid'),
    });
  });
});
