/**
 * Authentication middleware tests for DoSQL
 *
 * Tests the auth module in isolation using real Request objects.
 * No mocks - just direct function calls with real inputs.
 */

import { describe, it, expect } from 'vitest';
import {
  authenticate,
  checkAuth,
  unauthorizedResponse,
  createDefaultAuthConfig,
  type AuthConfig,
} from '../auth.js';

// =============================================================================
// Helpers
// =============================================================================

function makeRequest(
  path: string,
  headers: Record<string, string> = {}
): Request {
  return new Request(`http://localhost${path}`, { headers });
}

// =============================================================================
// Tests
// =============================================================================

describe('Auth Module', () => {
  // ---------------------------------------------------------------------------
  // Default config (auth disabled)
  // ---------------------------------------------------------------------------
  describe('createDefaultAuthConfig', () => {
    it('should return a config with auth disabled', () => {
      const config = createDefaultAuthConfig();
      expect(config.enabled).toBe(false);
      expect(config.bearerTokens).toEqual([]);
      expect(config.apiKeys).toEqual([]);
      expect(config.publicPaths).toEqual(['/health']);
    });
  });

  // ---------------------------------------------------------------------------
  // Auth disabled (backward compatibility)
  // ---------------------------------------------------------------------------
  describe('auth disabled', () => {
    const config: AuthConfig = { enabled: false };

    it('should allow requests without credentials', () => {
      const req = makeRequest('/query');
      const result = authenticate(req, config);
      expect(result.authenticated).toBe(true);
      expect(result.method).toBe('public');
    });

    it('should allow requests to any path', () => {
      const req = makeRequest('/execute');
      const result = authenticate(req, config);
      expect(result.authenticated).toBe(true);
    });

    it('should allow requests even with invalid credentials', () => {
      const req = makeRequest('/query', { Authorization: 'Bearer bad-token' });
      const result = authenticate(req, config);
      expect(result.authenticated).toBe(true);
    });
  });

  // ---------------------------------------------------------------------------
  // Bearer token authentication
  // ---------------------------------------------------------------------------
  describe('Bearer token auth', () => {
    const config: AuthConfig = {
      enabled: true,
      bearerTokens: ['valid-token-1', 'valid-token-2'],
      apiKeys: [],
    };

    it('should authenticate with a valid bearer token', () => {
      const req = makeRequest('/query', {
        Authorization: 'Bearer valid-token-1',
      });
      const result = authenticate(req, config);
      expect(result.authenticated).toBe(true);
      expect(result.method).toBe('bearer');
    });

    it('should authenticate with a second valid bearer token', () => {
      const req = makeRequest('/query', {
        Authorization: 'Bearer valid-token-2',
      });
      const result = authenticate(req, config);
      expect(result.authenticated).toBe(true);
      expect(result.method).toBe('bearer');
    });

    it('should reject an invalid bearer token', () => {
      const req = makeRequest('/query', {
        Authorization: 'Bearer wrong-token',
      });
      const result = authenticate(req, config);
      expect(result.authenticated).toBe(false);
      expect(result.error).toBe('Invalid bearer token');
    });

    it('should reject requests with no credentials', () => {
      const req = makeRequest('/query');
      const result = authenticate(req, config);
      expect(result.authenticated).toBe(false);
      expect(result.error).toContain('Authentication required');
    });

    it('should be case-insensitive for Bearer prefix', () => {
      const req = makeRequest('/query', {
        Authorization: 'bearer valid-token-1',
      });
      const result = authenticate(req, config);
      expect(result.authenticated).toBe(true);
      expect(result.method).toBe('bearer');
    });
  });

  // ---------------------------------------------------------------------------
  // API key authentication
  // ---------------------------------------------------------------------------
  describe('API key auth', () => {
    const config: AuthConfig = {
      enabled: true,
      bearerTokens: [],
      apiKeys: ['key-abc-123', 'key-def-456'],
    };

    it('should authenticate with a valid API key', () => {
      const req = makeRequest('/execute', {
        'X-API-Key': 'key-abc-123',
      });
      const result = authenticate(req, config);
      expect(result.authenticated).toBe(true);
      expect(result.method).toBe('api-key');
    });

    it('should authenticate with a second valid API key', () => {
      const req = makeRequest('/execute', {
        'X-API-Key': 'key-def-456',
      });
      const result = authenticate(req, config);
      expect(result.authenticated).toBe(true);
      expect(result.method).toBe('api-key');
    });

    it('should reject an invalid API key', () => {
      const req = makeRequest('/query', {
        'X-API-Key': 'invalid-key',
      });
      const result = authenticate(req, config);
      expect(result.authenticated).toBe(false);
      expect(result.error).toBe('Invalid API key');
    });

    it('should reject requests with no credentials', () => {
      const req = makeRequest('/tables');
      const result = authenticate(req, config);
      expect(result.authenticated).toBe(false);
      expect(result.error).toContain('Authentication required');
    });
  });

  // ---------------------------------------------------------------------------
  // Combined bearer + API key
  // ---------------------------------------------------------------------------
  describe('combined bearer and API key auth', () => {
    const config: AuthConfig = {
      enabled: true,
      bearerTokens: ['my-secret-token'],
      apiKeys: ['my-api-key'],
    };

    it('should accept bearer token', () => {
      const req = makeRequest('/query', {
        Authorization: 'Bearer my-secret-token',
      });
      const result = authenticate(req, config);
      expect(result.authenticated).toBe(true);
      expect(result.method).toBe('bearer');
    });

    it('should accept API key', () => {
      const req = makeRequest('/query', {
        'X-API-Key': 'my-api-key',
      });
      const result = authenticate(req, config);
      expect(result.authenticated).toBe(true);
      expect(result.method).toBe('api-key');
    });

    it('should prefer bearer token when both are provided', () => {
      const req = makeRequest('/query', {
        Authorization: 'Bearer my-secret-token',
        'X-API-Key': 'my-api-key',
      });
      const result = authenticate(req, config);
      expect(result.authenticated).toBe(true);
      expect(result.method).toBe('bearer');
    });

    it('should reject when both credentials are invalid', () => {
      const req = makeRequest('/query', {
        Authorization: 'Bearer wrong',
        'X-API-Key': 'also-wrong',
      });
      const result = authenticate(req, config);
      expect(result.authenticated).toBe(false);
      // Bearer is checked first, so we get bearer error
      expect(result.error).toBe('Invalid bearer token');
    });
  });

  // ---------------------------------------------------------------------------
  // Public paths
  // ---------------------------------------------------------------------------
  describe('public paths', () => {
    const config: AuthConfig = {
      enabled: true,
      bearerTokens: ['secret'],
      publicPaths: ['/health', '/'],
    };

    it('should allow unauthenticated access to /health', () => {
      const req = makeRequest('/health');
      const result = authenticate(req, config);
      expect(result.authenticated).toBe(true);
      expect(result.method).toBe('public');
    });

    it('should allow unauthenticated access to /', () => {
      const req = makeRequest('/');
      const result = authenticate(req, config);
      expect(result.authenticated).toBe(true);
      expect(result.method).toBe('public');
    });

    it('should require auth for non-public paths', () => {
      const req = makeRequest('/query');
      const result = authenticate(req, config);
      expect(result.authenticated).toBe(false);
    });

    it('should use default public paths when none specified', () => {
      const configNoPublic: AuthConfig = {
        enabled: true,
        bearerTokens: ['secret'],
        // publicPaths not set - defaults to ['/health']
      };
      const req = makeRequest('/health');
      const result = authenticate(req, configNoPublic);
      expect(result.authenticated).toBe(true);
      expect(result.method).toBe('public');
    });
  });

  // ---------------------------------------------------------------------------
  // checkAuth middleware
  // ---------------------------------------------------------------------------
  describe('checkAuth', () => {
    const config: AuthConfig = {
      enabled: true,
      bearerTokens: ['valid-token'],
    };

    it('should return null for authenticated requests', () => {
      const req = makeRequest('/query', {
        Authorization: 'Bearer valid-token',
      });
      const response = checkAuth(req, config);
      expect(response).toBeNull();
    });

    it('should return 401 Response for unauthenticated requests', async () => {
      const req = makeRequest('/query');
      const response = checkAuth(req, config);
      expect(response).not.toBeNull();
      expect(response!.status).toBe(401);

      const body = await response!.json() as { success: boolean; error: string };
      expect(body.success).toBe(false);
      expect(body.error).toContain('Authentication required');
    });

    it('should return null when auth is disabled', () => {
      const disabledConfig: AuthConfig = { enabled: false };
      const req = makeRequest('/query');
      const response = checkAuth(req, disabledConfig);
      expect(response).toBeNull();
    });

    it('should return null for public paths even when auth is enabled', () => {
      const req = makeRequest('/health');
      const response = checkAuth(req, config);
      expect(response).toBeNull();
    });
  });

  // ---------------------------------------------------------------------------
  // unauthorizedResponse
  // ---------------------------------------------------------------------------
  describe('unauthorizedResponse', () => {
    it('should return a 401 status', () => {
      const response = unauthorizedResponse();
      expect(response.status).toBe(401);
    });

    it('should include WWW-Authenticate header', () => {
      const response = unauthorizedResponse();
      expect(response.headers.get('WWW-Authenticate')).toBe('Bearer');
    });

    it('should include Content-Type header', () => {
      const response = unauthorizedResponse();
      expect(response.headers.get('Content-Type')).toBe('application/json');
    });

    it('should include custom error message', async () => {
      const response = unauthorizedResponse('Custom error');
      const body = await response.json() as { error: string };
      expect(body.error).toBe('Custom error');
    });

    it('should default to "Unauthorized" message', async () => {
      const response = unauthorizedResponse();
      const body = await response.json() as { error: string };
      expect(body.error).toBe('Unauthorized');
    });
  });

  // ---------------------------------------------------------------------------
  // Edge cases
  // ---------------------------------------------------------------------------
  describe('edge cases', () => {
    it('should handle empty bearer token list', () => {
      const config: AuthConfig = {
        enabled: true,
        bearerTokens: [],
        apiKeys: ['valid-key'],
      };
      // Bearer header present but no tokens configured - falls through to API key check
      const req = makeRequest('/query', {
        Authorization: 'Bearer some-token',
        'X-API-Key': 'valid-key',
      });
      const result = authenticate(req, config);
      // Bearer tokens list is empty, so bearer check is skipped, falls through to API key
      expect(result.authenticated).toBe(true);
      expect(result.method).toBe('api-key');
    });

    it('should handle malformed Authorization header', () => {
      const config: AuthConfig = {
        enabled: true,
        bearerTokens: ['valid'],
        apiKeys: ['key'],
      };
      // Not a Bearer scheme
      const req = makeRequest('/query', {
        Authorization: 'Basic dXNlcjpwYXNz',
        'X-API-Key': 'key',
      });
      const result = authenticate(req, config);
      // Basic auth is not bearer, so falls through to API key
      expect(result.authenticated).toBe(true);
      expect(result.method).toBe('api-key');
    });

    it('should handle Authorization header without scheme', () => {
      const config: AuthConfig = {
        enabled: true,
        bearerTokens: ['valid'],
      };
      const req = makeRequest('/query', {
        Authorization: 'just-a-token',
      });
      const result = authenticate(req, config);
      expect(result.authenticated).toBe(false);
    });
  });
});
