# DoSQL Security Model

**Version:** 2.0
**Last Updated:** 2026-01-23
**Applies To:** DoSQL Edge Database for Cloudflare Workers

---

## Executive Summary

DoSQL is a SQL database engine designed for Cloudflare Workers and Durable Objects. This guide documents the security model, explains what DoSQL protects against, and provides best practices for secure deployment.

**Key Security Properties:**
- **Tenant Isolation**: Each tenant operates in a dedicated Durable Object with isolated storage
- **SQL Injection Prevention**: State-aware tokenizer and parameterized query support
- **Transport Security**: All traffic encrypted via Cloudflare-managed TLS
- **Defense in Depth**: Multiple security layers from network to application

---

## Table of Contents

1. [Security Boundaries](#1-security-boundaries)
2. [SQL Injection Prevention](#2-sql-injection-prevention)
3. [Tenant Isolation Model](#3-tenant-isolation-model)
4. [Authentication Patterns](#4-authentication-patterns)
5. [Authorization and Access Control](#5-authorization-and-access-control)
6. [Data Protection](#6-data-protection)
7. [Network Security](#7-network-security)
8. [Configuration Options](#8-configuration-options)
9. [Audit and Compliance](#9-audit-and-compliance)
10. [Security Checklist](#10-security-checklist)
11. [Incident Response](#11-incident-response)
12. [References](#12-references)

---

## 1. Security Boundaries

### 1.1 Architecture Overview

DoSQL operates within Cloudflare's edge infrastructure. Understanding the trust boundaries is essential for proper security configuration.

```
                    TRUST BOUNDARIES
                          |
    [Internet]            |           [Cloudflare Edge]
         |                |                  |
         v                |                  v
   +-----------+          |         +------------------+
   |  Client   | --TLS--> | ------> | Worker (Entry)   |
   |  Browser  |          |         | - Authentication |
   |  API      |          |         | - Rate Limiting  |
   |  Mobile   |          |         | - Input Validation
   +-----------+          |         +--------+---------+
                          |                  |
                          |         +--------v---------+
                          |         |  Durable Object  |
                          |         |  (DoSQL Engine)  |
                          |         | - Query Execution|
                          |         | - Transaction Mgmt
                          |         | - CDC Streaming  |
                          |         +--------+---------+
                          |                  |
                          |         +--------v---------+
                          |         |       R2         |
                          |         |  (Cold Storage)  |
                          |         +------------------+
```

### 1.2 Trust Boundary Definitions

| Boundary | Location | Security Controls |
|----------|----------|-------------------|
| **External** | Internet to Cloudflare Edge | TLS encryption, DDoS protection, WAF |
| **Worker Entry** | Cloudflare to your Worker | Authentication, input validation, rate limiting |
| **DO Boundary** | Worker to Durable Object | Tenant routing, authorization |
| **Storage** | DO to R2 | Access control via binding, encryption at rest |

### 1.3 What DoSQL Protects Against

| Threat | Protection Level | Mechanism |
|--------|------------------|-----------|
| **SQL Injection** | Strong | Parameterized queries, state-aware tokenizer |
| **Cross-Tenant Access** | Strong | DO isolation (1 DO per tenant) |
| **Man-in-the-Middle** | Strong | TLS enforced by Cloudflare |
| **Data at Rest Exposure** | Strong | Cloudflare R2 encryption |
| **DoS via Query Complexity** | Configurable | Query timeouts, statement limits |
| **Unauthorized Access** | Application-dependent | Requires proper auth implementation |

### 1.4 What Requires Your Implementation

DoSQL provides the database engine, but certain security controls require your implementation:

- **Authentication**: You must validate user identity at the Worker layer
- **Authorization**: You must enforce role-based access within your application
- **Input Validation**: You should validate inputs before they reach queries
- **Audit Logging**: You must capture security-relevant events
- **Key Management**: You must securely manage secrets and API keys

---

## 2. SQL Injection Prevention

### 2.1 Overview

SQL injection is the most critical database vulnerability. DoSQL provides multiple layers of protection:

1. **Parameterized Queries**: The primary defense - always use `?` placeholders
2. **State-Aware Tokenizer**: Defense-in-depth for `exec()` multi-statement execution
3. **Input Validation**: Your responsibility to validate before querying

### 2.2 Parameterized Queries (Primary Defense)

**Always use parameter placeholders.** This is the most important security practice.

```typescript
import { createDatabase } from 'dosql';

const db = createDatabase();

// CORRECT: Parameterized queries - parameters are safely bound
const userId = request.params.id;  // User input
const user = db.prepare('SELECT * FROM users WHERE id = ?').get(userId);

// CORRECT: Named parameters
const stmt = db.prepare('SELECT * FROM users WHERE email = :email');
const user = stmt.get({ email: userEmail });

// CORRECT: Multiple parameters
const stmt = db.prepare(`
  INSERT INTO orders (user_id, product_id, quantity)
  VALUES (?, ?, ?)
`);
stmt.run(userId, productId, quantity);
```

**NEVER do this:**

```typescript
// CRITICAL VULNERABILITY: String interpolation
const userId = request.params.id;  // Could be: "1; DROP TABLE users; --"
const user = db.prepare(`SELECT * FROM users WHERE id = ${userId}`).get();

// CRITICAL VULNERABILITY: String concatenation
const searchTerm = request.query.search;
const sql = "SELECT * FROM products WHERE name LIKE '%" + searchTerm + "%'";

// CRITICAL VULNERABILITY: Template literals without parameters
const user = db.prepare(`SELECT * FROM users WHERE email = '${email}'`).get();
```

### 2.3 State-Aware Tokenizer (Defense-in-Depth)

When using `exec()` for multi-statement execution, DoSQL's tokenizer properly handles edge cases that naive semicolon splitting would mishandle:

```typescript
// The tokenizer correctly handles:

// 1. Semicolons inside string literals (NOT a statement separator)
db.exec("INSERT INTO users (name) VALUES ('John; Doe')");
// -> Single statement, semicolon is part of the data

// 2. Semicolons inside comments (NOT a statement separator)
db.exec("SELECT * /* comment; here */ FROM users");
// -> Single statement

// 3. Escaped quotes with semicolons
db.exec("SELECT 'He said ''Hello; World'''");
// -> Single statement

// 4. Line comments with semicolons
db.exec("SELECT * FROM users -- ignore; this");
// -> Single statement
```

**How the tokenizer works:**

The tokenizer maintains a state machine that tracks context:
- `normal`: Regular SQL parsing - semicolons split statements
- `single_quote`: Inside `'string'` - semicolons are literal
- `double_quote`: Inside `"identifier"` - semicolons are literal
- `backtick`: Inside `` `identifier` `` - semicolons are literal
- `block_comment`: Inside `/* comment */` - semicolons are literal
- `line_comment`: After `--` until newline - semicolons are literal

**Important**: The tokenizer is defense-in-depth. It cannot prevent injection attacks that work at the SQL level. Always use parameterized queries as your primary defense.

### 2.4 Dynamic SQL Patterns

Sometimes you need dynamic table or column names. Use allow-lists:

```typescript
// WRONG: User input directly in SQL
const table = request.query.table;
db.exec(`SELECT * FROM ${table}`);  // INJECTION RISK

// CORRECT: Allow-list for table names
const ALLOWED_TABLES = new Set(['users', 'orders', 'products']);

function validateTableName(name: string): string {
  if (!ALLOWED_TABLES.has(name)) {
    throw new Error('Invalid table name');
  }
  return name;
}

const table = validateTableName(request.query.table);
db.exec(`SELECT * FROM ${table}`);  // Safe - from allow-list
```

```typescript
// WRONG: User input for column names
const sortBy = request.query.sort;
db.exec(`SELECT * FROM users ORDER BY ${sortBy}`);  // INJECTION RISK

// CORRECT: Map user input to allowed columns
const SORT_COLUMNS: Record<string, string> = {
  'name': 'name',
  'date': 'created_at',
  'email': 'email',
};

const sortColumn = SORT_COLUMNS[request.query.sort] || 'created_at';
const sql = `SELECT * FROM users ORDER BY ${sortColumn}`;  // Safe
```

### 2.5 Input Validation

Validate inputs before they reach your queries:

```typescript
import { z } from 'zod';

// Define schemas
const UserIdSchema = z.string().uuid();
const EmailSchema = z.string().email().max(255);
const PageSchema = z.number().int().positive().max(1000);

async function getUser(request: Request): Promise<Response> {
  const params = await request.json();

  // Validate input
  const result = UserIdSchema.safeParse(params.userId);
  if (!result.success) {
    return new Response(
      JSON.stringify({ error: 'Invalid user ID format' }),
      { status: 400 }
    );
  }

  // Safe to use in parameterized query
  const user = db.prepare('SELECT * FROM users WHERE id = ?').get(result.data);
  return new Response(JSON.stringify(user));
}
```

---

## 3. Tenant Isolation Model

### 3.1 One Durable Object Per Tenant

DoSQL's primary isolation mechanism leverages Cloudflare Durable Objects. Each tenant gets a dedicated DO instance with completely isolated storage.

```typescript
// Worker entry point - routes to tenant-specific DO
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // 1. Authenticate and get tenant ID from token
    const tenantId = await authenticateAndGetTenant(request);
    if (!tenantId) {
      return new Response('Unauthorized', { status: 401 });
    }

    // 2. Route to tenant-specific Durable Object
    //    Each tenant has completely isolated storage
    const doId = env.DOSQL_DO.idFromName(`tenant:${tenantId}`);
    const stub = env.DOSQL_DO.get(doId);

    return stub.fetch(request);
  }
};
```

### 3.2 Isolation Guarantees

| Property | Guarantee |
|----------|-----------|
| **Storage Isolation** | Each DO has its own SQLite-compatible storage |
| **Memory Isolation** | No shared memory between DO instances |
| **Execution Isolation** | Each DO runs in its own V8 isolate |
| **Namespace Isolation** | DO IDs are scoped to your Worker |

### 3.3 Anti-Patterns to Avoid

```typescript
// WRONG: Shared DO for multiple tenants
const doId = env.DOSQL_DO.idFromName('shared-database');
// This puts all tenants in the same database!

// WRONG: Tenant ID in query instead of DO routing
const result = db.query(
  `SELECT * FROM users WHERE tenant_id = ${tenantId}`
);
// This relies on query-level filtering, not isolation

// WRONG: Trusting tenant ID from user input
const tenantId = request.headers.get('X-Tenant-Id');
const doId = env.DOSQL_DO.idFromName(`tenant:${tenantId}`);
// Attacker can specify any tenant ID!

// CORRECT: Derive tenant ID from authenticated token
const token = await verifyJWT(request.headers.get('Authorization'));
const tenantId = token.tenant_id;  // From verified claim
const doId = env.DOSQL_DO.idFromName(`tenant:${tenantId}`);
```

### 3.4 Multi-Tenant Data Model

Inside each tenant's DO, data belongs only to that tenant:

```typescript
// Inside Durable Object - all data is tenant-scoped by design
class DoSQLDurableObject {
  async query(sql: string, params: unknown[]): Promise<Result> {
    // No tenant_id column needed!
    // This DO's storage is isolated to this tenant
    return this.db.prepare(sql).all(...params);
  }
}

// The tenant boundary is at the DO level, not the row level
// This eliminates entire classes of cross-tenant bugs
```

---

## 4. Authentication Patterns

### 4.1 Worker-Level Authentication

Authentication should happen at the Worker layer BEFORE requests reach the Durable Object:

```typescript
// auth.ts
import { verify } from '@tsndr/cloudflare-worker-jwt';

export interface AuthUser {
  id: string;
  tenantId: string;
  email: string;
  roles: string[];
}

export async function authenticateRequest(
  request: Request,
  env: Env
): Promise<AuthUser | null> {
  const authHeader = request.headers.get('Authorization');
  if (!authHeader?.startsWith('Bearer ')) {
    return null;
  }

  const token = authHeader.slice(7);

  try {
    const isValid = await verify(token, env.JWT_SECRET);
    if (!isValid) return null;

    const payload = JSON.parse(atob(token.split('.')[1]));

    // Validate required claims
    if (!payload.sub || !payload.tenant_id) {
      return null;
    }

    // Check expiration
    if (payload.exp && payload.exp < Date.now() / 1000) {
      return null;
    }

    return {
      id: payload.sub,
      tenantId: payload.tenant_id,
      email: payload.email || '',
      roles: payload.roles || [],
    };
  } catch {
    return null;
  }
}
```

### 4.2 Worker Entry Point

```typescript
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // 1. Authenticate
    const user = await authenticateRequest(request, env);
    if (!user) {
      return new Response(
        JSON.stringify({ error: 'Unauthorized' }),
        { status: 401, headers: { 'Content-Type': 'application/json' } }
      );
    }

    // 2. Pass user context to DO (via headers)
    const enrichedRequest = new Request(request, {
      headers: new Headers(request.headers)
    });
    enrichedRequest.headers.set('X-User-Id', user.id);
    enrichedRequest.headers.set('X-Tenant-Id', user.tenantId);
    enrichedRequest.headers.set('X-User-Roles', JSON.stringify(user.roles));

    // 3. Route to tenant's Durable Object
    const doId = env.DOSQL_DO.idFromName(`tenant:${user.tenantId}`);
    return env.DOSQL_DO.get(doId).fetch(enrichedRequest);
  }
};
```

### 4.3 API Key Authentication

For service-to-service communication:

```typescript
interface APIKey {
  id: string;
  hashedKey: string;      // SHA-256 hash, never store plaintext
  tenantId: string;
  scopes: string[];       // ['read', 'write', 'admin']
  rateLimit: number;
  expiresAt?: number;
  createdAt: number;
}

// Hash function using Web Crypto API
async function hashKey(key: string): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(key);
  const hash = await crypto.subtle.digest('SHA-256', data);
  return btoa(String.fromCharCode(...new Uint8Array(hash)));
}

// Generate a new API key (return plaintext to user once, store hash)
async function generateAPIKey(
  tenantId: string,
  scopes: string[],
  env: Env
): Promise<string> {
  const key = `dosql_${tenantId}_${crypto.randomUUID().replace(/-/g, '')}`;
  const hashedKey = await hashKey(key);

  const apiKey: APIKey = {
    id: crypto.randomUUID(),
    hashedKey,
    tenantId,
    scopes,
    rateLimit: 1000,
    createdAt: Date.now(),
  };

  // Store in KV
  await env.API_KEYS.put(`apikey:${apiKey.id}`, JSON.stringify(apiKey));
  await env.API_KEYS.put(`hash:${hashedKey}`, apiKey.id);

  return key;  // Only time plaintext is visible
}

// Validate an API key
async function validateAPIKey(key: string, env: Env): Promise<APIKey | null> {
  const hashedKey = await hashKey(key);
  const keyId = await env.API_KEYS.get(`hash:${hashedKey}`);
  if (!keyId) return null;

  const apiKeyJson = await env.API_KEYS.get(`apikey:${keyId}`);
  if (!apiKeyJson) return null;

  const apiKey: APIKey = JSON.parse(apiKeyJson);

  // Check expiration
  if (apiKey.expiresAt && apiKey.expiresAt < Date.now()) {
    return null;
  }

  return apiKey;
}
```

---

## 5. Authorization and Access Control

### 5.1 Role-Based Access Control (RBAC)

Implement RBAC inside your Durable Object:

```typescript
class DoSQLDurableObject implements DurableObject {
  private readonly PERMISSIONS: Record<string, string[]> = {
    // Query operations
    'query': ['read', 'write', 'admin'],
    'batch': ['write', 'admin'],

    // Transaction operations
    'beginTransaction': ['write', 'admin'],
    'commit': ['write', 'admin'],
    'rollback': ['write', 'admin'],

    // Schema operations
    'getSchema': ['read', 'admin'],

    // CDC operations
    'subscribeCDC': ['read', 'admin'],

    // Admin-only operations
    'dropTable': ['admin'],
    'truncate': ['admin'],
    'createIndex': ['admin'],
    'dropIndex': ['admin'],
  };

  async fetch(request: Request): Promise<Response> {
    const userId = request.headers.get('X-User-Id');
    const roles = JSON.parse(request.headers.get('X-User-Roles') || '[]');

    const rpcRequest = await request.json();

    // Authorization check
    if (!this.isAuthorized(rpcRequest.method, roles)) {
      return new Response(
        JSON.stringify({
          error: 'Forbidden',
          code: 'INSUFFICIENT_PERMISSIONS',
          required: this.PERMISSIONS[rpcRequest.method],
        }),
        { status: 403 }
      );
    }

    // Process request...
  }

  private isAuthorized(method: string, roles: string[]): boolean {
    const required = this.PERMISSIONS[method] || ['admin'];
    return required.some(perm => roles.includes(perm));
  }
}
```

### 5.2 Row-Level Security Patterns

For fine-grained access within a tenant:

```typescript
// User can only see their own records
async function getUserOrders(userId: string, requestingUserId: string): Promise<Order[]> {
  // Check if user is requesting their own data
  if (userId !== requestingUserId) {
    // Check if requester has admin role
    const requester = await getUser(requestingUserId);
    if (!requester.roles.includes('admin')) {
      throw new Error('Access denied');
    }
  }

  return db.prepare('SELECT * FROM orders WHERE user_id = ?').all(userId);
}

// Or use SQL-level filtering based on user context
function buildQuery(baseQuery: string, userContext: UserContext): string {
  if (userContext.roles.includes('admin')) {
    return baseQuery;  // Admin sees all
  }
  // Regular users see only their records
  return `${baseQuery} WHERE user_id = ?`;
}
```

---

## 6. Data Protection

### 6.1 Encryption at Rest

Cloudflare R2 provides server-side encryption at rest by default. For additional security with sensitive fields:

```typescript
// Application-level encryption for highly sensitive data
import { encrypt, decrypt } from './crypto-utils';

async function storeSSN(userId: string, ssn: string, env: Env): Promise<void> {
  const { ciphertext, iv } = await encrypt(ssn, env.ENCRYPTION_KEY);

  db.prepare(`
    UPDATE users SET ssn_encrypted = ?, ssn_iv = ? WHERE id = ?
  `).run(ciphertext, iv, userId);
}

async function retrieveSSN(userId: string, env: Env): Promise<string | null> {
  const user = db.prepare(
    'SELECT ssn_encrypted, ssn_iv FROM users WHERE id = ?'
  ).get(userId);

  if (!user?.ssn_encrypted) return null;

  return decrypt(user.ssn_encrypted, user.ssn_iv, env.ENCRYPTION_KEY);
}

// AES-GCM encryption using Web Crypto API
export async function encrypt(
  plaintext: string,
  keyBase64: string
): Promise<{ ciphertext: string; iv: string }> {
  const key = await crypto.subtle.importKey(
    'raw',
    Uint8Array.from(atob(keyBase64), c => c.charCodeAt(0)),
    { name: 'AES-GCM' },
    false,
    ['encrypt']
  );

  const iv = crypto.getRandomValues(new Uint8Array(12));
  const encoded = new TextEncoder().encode(plaintext);

  const ciphertext = await crypto.subtle.encrypt(
    { name: 'AES-GCM', iv },
    key,
    encoded
  );

  return {
    ciphertext: btoa(String.fromCharCode(...new Uint8Array(ciphertext))),
    iv: btoa(String.fromCharCode(...iv)),
  };
}

export async function decrypt(
  ciphertextBase64: string,
  ivBase64: string,
  keyBase64: string
): Promise<string> {
  const key = await crypto.subtle.importKey(
    'raw',
    Uint8Array.from(atob(keyBase64), c => c.charCodeAt(0)),
    { name: 'AES-GCM' },
    false,
    ['decrypt']
  );

  const iv = Uint8Array.from(atob(ivBase64), c => c.charCodeAt(0));
  const ciphertext = Uint8Array.from(atob(ciphertextBase64), c => c.charCodeAt(0));

  const plaintext = await crypto.subtle.decrypt(
    { name: 'AES-GCM', iv },
    key,
    ciphertext
  );

  return new TextDecoder().decode(plaintext);
}
```

### 6.2 Sensitive Data Handling

```typescript
// Define sensitive field patterns
const SENSITIVE_PATTERNS = [
  /password/i,
  /secret/i,
  /token/i,
  /key/i,
  /ssn/i,
  /credit.?card/i,
  /cvv/i,
  /pin/i,
];

// Redact sensitive fields for logging
function redactSensitive(obj: Record<string, unknown>): Record<string, unknown> {
  const redacted: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(obj)) {
    if (SENSITIVE_PATTERNS.some(p => p.test(key))) {
      redacted[key] = '[REDACTED]';
    } else if (typeof value === 'object' && value !== null) {
      redacted[key] = redactSensitive(value as Record<string, unknown>);
    } else {
      redacted[key] = value;
    }
  }

  return redacted;
}

// Mask data for display
function maskSSN(ssn: string): string {
  return `***-**-${ssn.slice(-4)}`;
}

function maskEmail(email: string): string {
  const [local, domain] = email.split('@');
  return `${local.slice(0, 2)}***@${domain}`;
}

function maskCreditCard(number: string): string {
  return `****-****-****-${number.slice(-4)}`;
}
```

---

## 7. Network Security

### 7.1 Transport Layer Security

Cloudflare enforces TLS for all edge traffic. Enhance with security headers:

```typescript
export function addSecurityHeaders(response: Response): Response {
  const headers = new Headers(response.headers);

  // HSTS - enforce HTTPS
  headers.set('Strict-Transport-Security', 'max-age=31536000; includeSubDomains');

  // Prevent MIME sniffing
  headers.set('X-Content-Type-Options', 'nosniff');

  // XSS protection
  headers.set('X-XSS-Protection', '1; mode=block');

  // Prevent framing (clickjacking)
  headers.set('X-Frame-Options', 'DENY');

  // Content Security Policy
  headers.set('Content-Security-Policy', "default-src 'self'; frame-ancestors 'none'");

  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}
```

### 7.2 WebSocket Security

For persistent WebSocket connections:

```typescript
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const upgradeHeader = request.headers.get('upgrade');

    if (upgradeHeader?.toLowerCase() === 'websocket') {
      // 1. Validate authentication BEFORE upgrade
      const authToken = request.headers.get('Authorization');
      const user = await validateToken(authToken, env);

      if (!user) {
        return new Response('Unauthorized', { status: 401 });
      }

      // 2. Validate origin for CSRF protection
      const origin = request.headers.get('Origin');
      if (!isAllowedOrigin(origin, env.ALLOWED_ORIGINS)) {
        return new Response('Forbidden', { status: 403 });
      }

      // 3. Route to tenant DO
      const doId = env.DOSQL_DO.idFromName(`tenant:${user.tenantId}`);
      return env.DOSQL_DO.get(doId).fetch(request);
    }

    // HTTP request handling...
  }
};

function isAllowedOrigin(origin: string | null, allowed: string[]): boolean {
  if (!origin) return false;
  return allowed.some(pattern => {
    if (pattern === '*') return true;  // Not recommended for production
    if (pattern.startsWith('*.')) {
      const domain = pattern.slice(2);
      return origin.endsWith(domain);
    }
    return origin === pattern;
  });
}
```

### 7.3 Rate Limiting

Implement rate limiting to prevent abuse:

```typescript
class RateLimiter implements DurableObject {
  private requests: number[] = [];
  private readonly windowMs = 60000;  // 1 minute
  private readonly maxRequests = 1000;

  async fetch(request: Request): Promise<Response> {
    const now = Date.now();

    // Clean old requests
    this.requests = this.requests.filter(t => now - t < this.windowMs);

    if (this.requests.length >= this.maxRequests) {
      return new Response(
        JSON.stringify({ error: 'Rate limit exceeded' }),
        {
          status: 429,
          headers: {
            'Retry-After': String(Math.ceil(this.windowMs / 1000)),
            'X-RateLimit-Limit': String(this.maxRequests),
            'X-RateLimit-Remaining': '0',
            'X-RateLimit-Reset': String(
              Math.ceil((this.requests[0] + this.windowMs) / 1000)
            ),
          }
        }
      );
    }

    this.requests.push(now);

    return new Response(
      JSON.stringify({ allowed: true }),
      {
        headers: {
          'X-RateLimit-Limit': String(this.maxRequests),
          'X-RateLimit-Remaining': String(this.maxRequests - this.requests.length),
        }
      }
    );
  }
}
```

### 7.4 Batch Request Limits

Protect against oversized batch requests:

```typescript
const MAX_BATCH_SIZE = 100;
const MAX_QUERY_LENGTH = 10000;

async function handleBatchRequest(request: Request): Promise<Response> {
  const batch = await request.json() as BatchRequest;

  // Validate batch size
  if (batch.queries.length > MAX_BATCH_SIZE) {
    return new Response(
      JSON.stringify({ error: 'Batch size exceeds limit' }),
      { status: 400 }
    );
  }

  // Validate individual query lengths
  for (const query of batch.queries) {
    if (query.sql.length > MAX_QUERY_LENGTH) {
      return new Response(
        JSON.stringify({ error: 'Query exceeds maximum length' }),
        { status: 400 }
      );
    }
  }

  // Process batch...
}
```

---

## 8. Configuration Options

### 8.1 Database Security Options

```typescript
interface DatabaseSecurityOptions {
  /**
   * Maximum query execution time in milliseconds
   * @default 5000
   */
  queryTimeout?: number;

  /**
   * Maximum number of rows a query can return
   * @default 10000
   */
  maxResultRows?: number;

  /**
   * Whether to allow CREATE/DROP TABLE statements
   * @default true
   */
  allowDDL?: boolean;

  /**
   * Whether to allow ATTACH DATABASE
   * @default false
   */
  allowAttach?: boolean;

  /**
   * Maximum size of prepared statement cache
   * @default 100
   */
  statementCacheSize?: number;
}

const db = new Database(':memory:', {
  queryTimeout: 3000,
  maxResultRows: 5000,
  allowDDL: false,  // Disable DDL in production
});
```

### 8.2 RPC Security Configuration

```typescript
interface RPCSecurityConfig {
  /**
   * Maximum request body size in bytes
   * @default 1048576 (1MB)
   */
  maxRequestSize: number;

  /**
   * Maximum number of queries in a batch
   * @default 100
   */
  maxBatchSize: number;

  /**
   * Request timeout in milliseconds
   * @default 30000
   */
  requestTimeout: number;

  /**
   * Enable request logging
   * @default true
   */
  enableLogging: boolean;

  /**
   * Allowed origins for CORS
   * @default []
   */
  allowedOrigins: string[];
}
```

### 8.3 Secrets Management

Store secrets in Cloudflare secrets, not environment variables:

```bash
# Set secrets using wrangler
wrangler secret put JWT_SECRET
wrangler secret put ENCRYPTION_KEY
wrangler secret put DATABASE_ENCRYPTION_KEY
```

```typescript
// Access in Worker
export interface Env {
  JWT_SECRET: string;      // From wrangler secret
  ENCRYPTION_KEY: string;  // From wrangler secret
  DOSQL_DO: DurableObjectNamespace;
  API_KEYS: KVNamespace;
}

// Secrets are available on env, never exposed in logs or errors
async function handler(request: Request, env: Env) {
  const isValid = await verify(token, env.JWT_SECRET);
  // ...
}
```

---

## 9. Audit and Compliance

### 9.1 Audit Events to Capture

```typescript
const AUDIT_EVENTS = {
  // Authentication (always log)
  AUTH_SUCCESS: 'auth.success',
  AUTH_FAILURE: 'auth.failure',
  AUTH_LOGOUT: 'auth.logout',
  TOKEN_REFRESH: 'auth.token_refresh',

  // Query events (configurable)
  QUERY_READ: 'query.read',
  QUERY_WRITE: 'query.write',
  QUERY_DDL: 'query.ddl',

  // Data changes (via CDC)
  DATA_INSERT: 'data.insert',
  DATA_UPDATE: 'data.update',
  DATA_DELETE: 'data.delete',

  // Admin events (always log)
  SCHEMA_CHANGE: 'admin.schema_change',
  USER_CREATE: 'admin.user_create',
  USER_DELETE: 'admin.user_delete',
  PERMISSION_CHANGE: 'admin.permission_change',
  API_KEY_CREATE: 'admin.api_key_create',
  API_KEY_REVOKE: 'admin.api_key_revoke',

  // Security events (always log)
  SQL_INJECTION_ATTEMPT: 'security.sql_injection',
  AUTH_BYPASS_ATTEMPT: 'security.auth_bypass',
  RATE_LIMIT_EXCEEDED: 'security.rate_limit',
};
```

### 9.2 Audit Log Implementation

```typescript
interface AuditEvent {
  timestamp: Date;
  eventType: string;
  userId: string;
  tenantId: string;
  action: string;
  resource?: string;
  details?: Record<string, unknown>;
  ipAddress?: string;
  userAgent?: string;
  requestId: string;
  success: boolean;
  errorMessage?: string;
}

class AuditLogger {
  private queue: AuditEvent[] = [];

  constructor(
    private storage: R2Bucket,
    private tenantId: string
  ) {}

  log(event: Omit<AuditEvent, 'timestamp' | 'requestId'>): void {
    this.queue.push({
      ...event,
      timestamp: new Date(),
      requestId: crypto.randomUUID(),
    });

    // Flush immediately for security-critical events
    if (this.isSecurityCritical(event.action)) {
      this.flush();
    }
  }

  private isSecurityCritical(action: string): boolean {
    return [
      'auth.failure',
      'security.sql_injection',
      'security.auth_bypass',
      'admin.permission_change',
      'admin.api_key_revoke',
    ].includes(action);
  }

  async flush(): Promise<void> {
    if (this.queue.length === 0) return;

    const events = this.queue;
    this.queue = [];

    const now = new Date();
    const path = [
      'audit-logs',
      this.tenantId,
      now.getFullYear(),
      String(now.getMonth() + 1).padStart(2, '0'),
      String(now.getDate()).padStart(2, '0'),
      `${now.getTime()}-${crypto.randomUUID().slice(0, 8)}.jsonl`,
    ].join('/');

    const content = events.map(e => JSON.stringify(e)).join('\n');
    await this.storage.put(path, content);
  }
}
```

### 9.3 Compliance Considerations

| Requirement | Implementation |
|-------------|----------------|
| **SOC 2** | Audit logs (90+ days), access controls, encryption |
| **GDPR** | Data encryption, right to erasure, data portability |
| **HIPAA** | Encryption (transit/rest), audit trails, access controls |
| **PCI DSS** | No plaintext card data, encryption, access logging |

---

## 10. Security Checklist

### 10.1 Pre-Deployment Checklist

#### Authentication & Authorization
- [ ] JWT/token validation with proper signature verification
- [ ] Token expiration enforced
- [ ] API keys hashed before storage (never plaintext)
- [ ] Role-based access control implemented
- [ ] Rate limiting configured
- [ ] Failed auth attempts logged and limited

#### SQL Injection Prevention
- [ ] All queries use parameterized statements (`?` placeholders)
- [ ] No string concatenation/interpolation in SQL
- [ ] Dynamic table/column names use allow-lists only
- [ ] Input validation using schemas (Zod, etc.)
- [ ] Query length limits enforced

#### Network Security
- [ ] HTTPS enforced (no HTTP fallback)
- [ ] HSTS headers configured
- [ ] CORS properly configured (no `*` in production)
- [ ] WebSocket origin validation implemented
- [ ] Security headers added (CSP, X-Frame-Options, etc.)

#### Data Protection
- [ ] Sensitive fields encrypted at application level
- [ ] PII fields identified and properly handled
- [ ] Data masking for logs/displays
- [ ] Secrets stored in Cloudflare Secrets (not env vars)

#### Tenant Isolation
- [ ] Each tenant has dedicated Durable Object
- [ ] Tenant ID derived from authenticated token only
- [ ] No shared state between tenants
- [ ] Cross-tenant access testing performed

#### Monitoring & Logging
- [ ] Audit logging enabled for auth events
- [ ] Audit logging enabled for admin operations
- [ ] Security events captured and alerted
- [ ] Log retention policies configured

### 10.2 Regular Security Reviews

| Frequency | Tasks |
|-----------|-------|
| **Weekly** | Review failed auth logs, check rate limiting, monitor unusual patterns |
| **Monthly** | Rotate service API keys, review permissions, audit privileged operations |
| **Quarterly** | Dependency vulnerability scan (`npm audit`), penetration testing |
| **Annually** | Full architecture review, compliance audit, key rotation |

---

## 11. Incident Response

### 11.1 Emergency Procedures

```typescript
// Disable compromised API key
await env.API_KEYS.delete(`hash:${hashedKey}`);
await env.API_KEYS.delete(`apikey:${keyId}`);

// Invalidate all sessions for a user
db.prepare('DELETE FROM user_sessions WHERE user_id = ?').run(userId);

// Block IP address via Cloudflare WAF API
await fetch(
  `https://api.cloudflare.com/client/v4/zones/${zoneId}/firewall/access_rules/rules`,
  {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${env.CF_API_TOKEN}` },
    body: JSON.stringify({
      mode: 'block',
      configuration: { target: 'ip', value: suspiciousIP },
      notes: `Blocked: security incident ${new Date().toISOString()}`,
    }),
  }
);

// Enable maintenance mode (read-only)
await env.KV.put('maintenance_mode', 'true');
```

### 11.2 Incident Response Steps

1. **Detect**: Monitor for anomalies (failed auth spikes, unusual queries)
2. **Contain**: Disable compromised credentials, block suspicious IPs
3. **Investigate**: Review audit logs, identify scope
4. **Remediate**: Patch vulnerability, rotate credentials
5. **Recover**: Restore service, verify integrity
6. **Document**: Post-incident report, update procedures

---

## 12. References

### Cloudflare Security Documentation

- [Durable Objects Security Model](https://developers.cloudflare.com/durable-objects/reference/security-model/)
- [Workers Security](https://developers.cloudflare.com/workers/platform/security/)
- [R2 Data Security](https://developers.cloudflare.com/r2/reference/data-security/)
- [Secrets Management](https://developers.cloudflare.com/workers/configuration/secrets/)
- [Web Application Firewall](https://developers.cloudflare.com/waf/)
- [DDoS Protection](https://developers.cloudflare.com/ddos-protection/)
- [Cloudflare Zero Trust](https://developers.cloudflare.com/cloudflare-one/)

### DoSQL Documentation

- [Architecture](./architecture.md) - System architecture details
- [API Reference](./api-reference.md) - Complete API documentation
- [Deployment](./DEPLOYMENT.md) - Deployment guide

---

*This security guide is maintained as part of the DoSQL project. Report security vulnerabilities responsibly via security@dotdo.ai.*
