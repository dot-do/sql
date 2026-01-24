/**
 * sql.do/auth - Integration with oauth.do for seamless authentication
 *
 * Users install oauth.do separately to use this module:
 * ```bash
 * npm install oauth.do
 * ```
 *
 * @example
 * ```typescript
 * import { createAuthenticatedClient } from 'sql.do/auth';
 *
 * const client = await createAuthenticatedClient({
 *   url: 'https://sql.example.com',
 * });
 *
 * const result = await client.query('SELECT * FROM users');
 * ```
 *
 * @packageDocumentation
 * @module sql.do/auth
 */

import type { SQLClient } from './types.js';

/**
 * Options for creating an authenticated SQL client.
 *
 * @public
 * @since 0.2.0
 */
export interface AuthenticatedClientOptions {
  /** DoSQL endpoint URL */
  url: string;
  /** OAuth authentication options */
  authOptions?: {
    /** OAuth client ID */
    clientId?: string;
    /** OAuth scopes to request */
    scopes?: string[];
  };
  /** Request timeout in milliseconds */
  timeout?: number;
  /** Database name to connect to */
  database?: string;
}

/**
 * Options for ensuring authentication.
 *
 * @public
 * @since 0.2.0
 */
export interface EnsureAuthOptions {
  /** OAuth client ID */
  clientId?: string;
}

/**
 * Creates a SQL client authenticated via oauth.do.
 *
 * @description Obtains an auth token from oauth.do (reusing an existing session
 * if available, or initiating a new auth flow) and creates a SQL client with it.
 *
 * Requires `oauth.do` to be installed as a peer dependency.
 *
 * @param options - Client and auth configuration
 * @returns A fully authenticated SQLClient instance
 * @throws {Error} When oauth.do is not installed or authentication fails
 *
 * @example
 * ```typescript
 * import { createAuthenticatedClient } from 'sql.do/auth';
 *
 * const client = await createAuthenticatedClient({
 *   url: 'https://sql.example.com',
 *   authOptions: { scopes: ['sql:read', 'sql:write'] },
 * });
 *
 * const result = await client.query('SELECT * FROM users');
 * await client.close();
 * ```
 *
 * @public
 * @since 0.2.0
 */
export async function createAuthenticatedClient(options: AuthenticatedClientOptions): Promise<SQLClient> {
  // Dynamic import so oauth.do is truly optional at runtime
  const { auth, getToken } = await import('oauth.do');
  const token = await getToken() ?? (await auth(options.authOptions)).token;
  const { createSQLClient } = await import('./client.js');
  return createSQLClient({ ...options, token });
}

/**
 * Ensures the user is authenticated via oauth.do.
 *
 * @description Triggers the oauth.do authentication flow if not already
 * authenticated. Returns the auth result containing user info and token.
 *
 * Requires `oauth.do` to be installed as a peer dependency.
 *
 * @param options - Optional auth configuration
 * @returns The authentication result from oauth.do
 * @throws {Error} When oauth.do is not installed or authentication fails
 *
 * @example
 * ```typescript
 * import { ensureAuth } from 'sql.do/auth';
 *
 * const authResult = await ensureAuth({ clientId: 'my-app' });
 * console.log(`Authenticated as: ${authResult.user}`);
 * ```
 *
 * @public
 * @since 0.2.0
 */
export async function ensureAuth(options?: EnsureAuthOptions): Promise<{ token: string; user?: string }> {
  const { auth } = await import('oauth.do');
  return auth(options);
}

/**
 * Retrieves the current auth token from oauth.do without initiating a new flow.
 *
 * @description Returns the cached token if available, or null if no active
 * session exists. Does not trigger a new authentication flow.
 *
 * Requires `oauth.do` to be installed as a peer dependency.
 *
 * @returns The current auth token, or null if not authenticated
 * @throws {Error} When oauth.do is not installed
 *
 * @example
 * ```typescript
 * import { getAuthToken } from 'sql.do/auth';
 *
 * const token = await getAuthToken();
 * if (token) {
 *   console.log('Already authenticated');
 * } else {
 *   console.log('Not authenticated, need to log in');
 * }
 * ```
 *
 * @public
 * @since 0.2.0
 */
export async function getAuthToken(): Promise<string | null> {
  const { getToken } = await import('oauth.do');
  return getToken();
}
