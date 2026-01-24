/**
 * DoSQL Auth Middleware
 *
 * OAuth middleware using oauth.do for request authentication.
 * Pluggable: users can swap oauth.do for their own verification logic.
 *
 * @packageDocumentation
 */

// =============================================================================
// Types (declared inline to avoid requiring hono/oauth.do install for types)
// =============================================================================

/** Minimal Hono Context interface */
interface HonoContext {
  req: {
    path: string
    header(name: string): string | undefined
  }
  set(key: string, value: unknown): void
  json(data: unknown, status?: number): Response
}

/** Minimal Hono MiddlewareHandler */
type MiddlewareHandler = (c: HonoContext, next: () => Promise<void>) => Promise<void | Response>

/**
 * Verified user info returned from token verification
 */
export interface AuthUser {
  /** User ID from the auth provider */
  userId: string
  /** Additional claims from the token */
  [key: string]: unknown
}

/**
 * Auth middleware configuration
 */
export interface AuthConfig {
  /** Skip auth for these paths (supports exact match and prefix with trailing *) */
  publicPaths?: string[]
  /** Custom token extraction from the request context */
  extractToken?: (c: HonoContext) => string | null
  /** Custom token verification (uses oauth.do by default) */
  verifyToken?: (token: string) => Promise<AuthUser | null>
  /** oauth.do endpoint URL (default: https://oauth.do/verify) */
  oauthDoUrl?: string
  /** Cache verified tokens in memory for this many seconds (default: 60) */
  tokenCacheTtl?: number
}

// =============================================================================
// Token Cache
// =============================================================================

interface CachedToken {
  user: AuthUser
  expiresAt: number
}

const tokenCache = new Map<string, CachedToken>()

function getCachedUser(token: string): AuthUser | null {
  const cached = tokenCache.get(token)
  if (!cached) return null
  if (Date.now() > cached.expiresAt) {
    tokenCache.delete(token)
    return null
  }
  return cached.user
}

function cacheUser(token: string, user: AuthUser, ttlSeconds: number): void {
  tokenCache.set(token, {
    user,
    expiresAt: Date.now() + ttlSeconds * 1000,
  })
  // Evict expired entries periodically (keep cache bounded)
  if (tokenCache.size > 1000) {
    const now = Date.now()
    for (const [key, value] of tokenCache) {
      if (now > value.expiresAt) {
        tokenCache.delete(key)
      }
    }
  }
}

// =============================================================================
// Default Token Extraction
// =============================================================================

/**
 * Default token extraction: reads Bearer token from Authorization header
 */
function defaultExtractToken(c: HonoContext): string | null {
  const authHeader = c.req.header('Authorization')
  if (!authHeader) return null
  const match = authHeader.match(/^Bearer\s+(.+)$/i)
  return match ? match[1]! : null
}

// =============================================================================
// Default Token Verification via oauth.do
// =============================================================================

/**
 * Create a verify function that calls oauth.do
 */
function createOAuthDoVerifier(oauthDoUrl: string): (token: string) => Promise<AuthUser | null> {
  return async (token: string): Promise<AuthUser | null> => {
    try {
      const response = await fetch(oauthDoUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`,
        },
        body: JSON.stringify({ token }),
      })

      if (!response.ok) {
        return null
      }

      const data = await response.json() as { valid?: boolean; userId?: string; [key: string]: unknown }

      if (!data.valid || !data.userId) {
        return null
      }

      return { userId: data.userId, ...data }
    } catch {
      return null
    }
  }
}

// =============================================================================
// Path Matching
// =============================================================================

function isPublicPath(path: string, publicPaths: string[]): boolean {
  for (const publicPath of publicPaths) {
    if (publicPath.endsWith('*')) {
      // Prefix match
      const prefix = publicPath.slice(0, -1)
      if (path.startsWith(prefix)) return true
    } else {
      // Exact match
      if (path === publicPath) return true
    }
  }
  return false
}

// =============================================================================
// Auth Middleware
// =============================================================================

/**
 * Create auth middleware for DoSQL routes
 *
 * Verifies Bearer tokens using oauth.do (or custom verifier).
 * Sets `c.set('user', authUser)` on successful authentication.
 *
 * @param config - Auth configuration options
 * @returns Hono middleware handler
 *
 * @example
 * ```typescript
 * import { Hono } from 'hono'
 * import { authMiddleware } from 'dosql/middleware'
 *
 * const app = new Hono()
 *
 * // Protect all routes except /health
 * app.use('*', authMiddleware({
 *   publicPaths: ['/health'],
 * }))
 *
 * // Custom token verification
 * app.use('*', authMiddleware({
 *   verifyToken: async (token) => {
 *     const decoded = await myJwtVerify(token)
 *     return decoded ? { userId: decoded.sub } : null
 *   },
 * }))
 * ```
 */
export function authMiddleware(config?: AuthConfig): MiddlewareHandler {
  const {
    publicPaths = [],
    extractToken = defaultExtractToken,
    oauthDoUrl = 'https://oauth.do/verify',
    tokenCacheTtl = 60,
  } = config ?? {}

  const verifyToken = config?.verifyToken ?? createOAuthDoVerifier(oauthDoUrl)

  return async (c, next) => {
    // Check if path is public
    if (publicPaths.length > 0 && isPublicPath(c.req.path, publicPaths)) {
      await next()
      return
    }

    // Extract token
    const token = extractToken(c)
    if (!token) {
      return c.json(
        { error: 'Unauthorized', message: 'Missing or invalid Authorization header' },
        401
      )
    }

    // Check cache first
    const cachedUser = getCachedUser(token)
    if (cachedUser) {
      c.set('user', cachedUser)
      await next()
      return
    }

    // Verify token
    const user = await verifyToken(token)
    if (!user) {
      return c.json(
        { error: 'Unauthorized', message: 'Invalid or expired token' },
        401
      )
    }

    // Cache the verified user
    if (tokenCacheTtl > 0) {
      cacheUser(token, user, tokenCacheTtl)
    }

    // Set user in context
    c.set('user', user)

    await next()
  }
}

/**
 * Helper to get the authenticated user from context
 *
 * @example
 * ```typescript
 * app.get('/me', (c) => {
 *   const user = getUser(c)
 *   return c.json(user)
 * })
 * ```
 */
export function getUser(c: HonoContext): AuthUser | null {
  return (c as unknown as { get(key: string): unknown }).get('user') as AuthUser | null
}
