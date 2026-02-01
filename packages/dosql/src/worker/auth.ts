/**
 * Authentication middleware for DoSQL
 *
 * Supports two authentication methods:
 * - Bearer token: `Authorization: Bearer <token>`
 * - API key: `X-API-Key: <key>`
 *
 * Authentication is optional and disabled by default for backward compatibility.
 * Enable by providing an AuthConfig with at least one valid credential.
 *
 * @packageDocumentation
 */

// =============================================================================
// Types
// =============================================================================

export interface AuthConfig {
  /** Enable authentication. When false, all requests are allowed. */
  enabled: boolean;

  /**
   * Valid bearer tokens. Requests with `Authorization: Bearer <token>`
   * are checked against this set.
   */
  bearerTokens?: string[];

  /**
   * Valid API keys. Requests with `X-API-Key: <key>` header
   * are checked against this set.
   */
  apiKeys?: string[];

  /**
   * Paths that bypass authentication (e.g., health checks).
   * Defaults to ['/health'].
   */
  publicPaths?: string[];
}

export interface AuthResult {
  /** Whether the request is authenticated */
  authenticated: boolean;

  /** The authentication method used, if authenticated */
  method?: 'bearer' | 'api-key' | 'public';

  /** Error message if not authenticated */
  error?: string;
}

// =============================================================================
// Constants
// =============================================================================

const DEFAULT_PUBLIC_PATHS = ['/health'];

const API_KEY_HEADER = 'x-api-key';

// =============================================================================
// Authentication Functions
// =============================================================================

/**
 * Create a default AuthConfig with authentication disabled.
 */
export function createDefaultAuthConfig(): AuthConfig {
  return {
    enabled: false,
    bearerTokens: [],
    apiKeys: [],
    publicPaths: DEFAULT_PUBLIC_PATHS,
  };
}

/**
 * Authenticate a request against the provided configuration.
 *
 * When auth is disabled, all requests are allowed.
 * When enabled, requests must provide a valid Bearer token or API key,
 * unless the path is in the public paths list.
 */
export function authenticate(request: Request, config: AuthConfig): AuthResult {
  // Auth disabled - allow everything
  if (!config.enabled) {
    return { authenticated: true, method: 'public' };
  }

  // Check if path is public
  const url = new URL(request.url);
  const publicPaths = config.publicPaths ?? DEFAULT_PUBLIC_PATHS;
  if (publicPaths.includes(url.pathname)) {
    return { authenticated: true, method: 'public' };
  }

  // Check Bearer token
  const authHeader = request.headers.get('authorization');
  if (authHeader) {
    const bearerMatch = authHeader.match(/^Bearer\s+(.+)$/i);
    if (bearerMatch) {
      const token = bearerMatch[1];
      if (config.bearerTokens && config.bearerTokens.length > 0) {
        if (config.bearerTokens.includes(token)) {
          return { authenticated: true, method: 'bearer' };
        }
        return {
          authenticated: false,
          error: 'Invalid bearer token',
        };
      }
    }
  }

  // Check API key
  const apiKey = request.headers.get(API_KEY_HEADER);
  if (apiKey) {
    if (config.apiKeys && config.apiKeys.length > 0) {
      if (config.apiKeys.includes(apiKey)) {
        return { authenticated: true, method: 'api-key' };
      }
      return {
        authenticated: false,
        error: 'Invalid API key',
      };
    }
  }

  // No valid credentials provided
  return {
    authenticated: false,
    error: 'Authentication required. Provide a Bearer token or API key.',
  };
}

/**
 * Build a 401 Unauthorized response.
 */
export function unauthorizedResponse(error?: string): Response {
  return new Response(
    JSON.stringify({
      success: false,
      error: error ?? 'Unauthorized',
    }),
    {
      status: 401,
      headers: {
        'Content-Type': 'application/json',
        'WWW-Authenticate': 'Bearer',
      },
    }
  );
}

/**
 * Middleware-style auth check. Returns a 401 Response if the request
 * fails authentication, or null if the request is allowed to proceed.
 */
export function checkAuth(request: Request, config: AuthConfig): Response | null {
  const result = authenticate(request, config);
  if (!result.authenticated) {
    return unauthorizedResponse(result.error);
  }
  return null;
}
