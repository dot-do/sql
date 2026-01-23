# DoSQL Security Guide

**Version:** 1.0
**Last Updated:** 2026-01-22
**Applies To:** DoSQL Edge Database for Cloudflare Workers

---

## Table of Contents

1. [Threat Model](#1-threat-model)
2. [SQL Injection Prevention](#2-sql-injection-prevention)
3. [Authentication & Authorization](#3-authentication--authorization)
4. [Data Encryption](#4-data-encryption)
5. [Audit Logging](#5-audit-logging)
6. [Security Checklist](#6-security-checklist)

---

## 1. Threat Model

### 1.1 Edge Database Attack Surface

DoSQL runs at the edge in Cloudflare Workers and Durable Objects. Understanding the attack surface is crucial for implementing proper security controls.

```
                                      TRUST BOUNDARY
                                            |
    [Internet Users]                        |        [Cloudflare Edge]
          |                                 |              |
          v                                 |              v
    +-------------+                         |     +------------------+
    |   Browser   |  <------ TLS -------->  |  -->|  Worker (Entry)  |
    |   Mobile    |                         |     +--------+---------+
    |   API       |                         |              |
    +-------------+                         |              | Auth Check
                                            |              v
                                            |     +------------------+
                                            |     |  Durable Object  |
                                            |     |  (DoSQL Engine)  |
                                            |     +--------+---------+
                                            |              |
                                            |     +--------+---------+
                                            |     |        R2        |
                                            |     |  (Cold Storage)  |
                                            |     +------------------+
```

**Attack Vectors:**

| Vector | Risk Level | Mitigation |
|--------|------------|------------|
| SQL Injection | Critical | Parameterized queries, tokenizer validation |
| Authentication Bypass | Critical | Worker-level auth middleware |
| Cross-Tenant Data Access | Critical | DO isolation (1 DO per tenant) |
| WebSocket Hijacking | High | Origin validation, auth tokens |
| DoS via Resource Exhaustion | High | Rate limiting, query timeouts |
| Man-in-the-Middle | Medium | TLS enforced by Cloudflare |
| Data Exfiltration | Medium | Audit logging, access controls |

### 1.2 Multi-Tenant Isolation (1 DO per Tenant)

DoSQL leverages Cloudflare Durable Objects' inherent isolation model where each tenant gets a dedicated DO instance.

```typescript
// Worker entry point - routes to tenant-specific DO
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Extract tenant from authenticated request
    const tenantId = await authenticateAndGetTenant(request);

    if (!tenantId) {
      return new Response('Unauthorized', { status: 401 });
    }

    // Route to tenant-specific Durable Object
    // Each tenant has completely isolated storage
    const doId = env.DOSQL_DO.idFromName(`tenant:${tenantId}`);
    const stub = env.DOSQL_DO.get(doId);

    return stub.fetch(request);
  }
};
```

**Isolation Guarantees:**

- Each DO has its own isolated storage (SQLite-compatible)
- No shared memory between DO instances
- Storage is physically isolated per DO ID
- Cross-tenant access requires explicit code bugs (not architecture flaws)

**Anti-Patterns to Avoid:**

```typescript
// BAD: Shared DO for multiple tenants
const doId = env.DOSQL_DO.idFromName('shared-database'); // DON'T DO THIS

// BAD: Tenant ID in query instead of DO routing
const result = db.query(
  `SELECT * FROM users WHERE tenant_id = ${tenantId}` // SQL INJECTION + WRONG ISOLATION
);

// GOOD: Tenant isolation via DO routing
const doId = env.DOSQL_DO.idFromName(`tenant:${tenantId}`);
// Inside the DO, all data belongs to this tenant
const result = db.query('SELECT * FROM users'); // No tenant filter needed
```

### 1.3 Network Threats (WebSocket, HTTP)

#### WebSocket Security

DoSQL supports persistent WebSocket connections for RPC. These require additional security considerations:

```typescript
// In your Worker - WebSocket upgrade handling
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const upgradeHeader = request.headers.get('upgrade');

    if (upgradeHeader?.toLowerCase() === 'websocket') {
      // Validate authentication BEFORE upgrade
      const authToken = request.headers.get('Authorization');
      const user = await validateToken(authToken, env);

      if (!user) {
        return new Response('Unauthorized', { status: 401 });
      }

      // Validate origin for CSRF protection
      const origin = request.headers.get('Origin');
      if (!isAllowedOrigin(origin, env.ALLOWED_ORIGINS)) {
        return new Response('Forbidden', { status: 403 });
      }

      // Route to tenant DO
      const doId = env.DOSQL_DO.idFromName(`tenant:${user.tenantId}`);
      return env.DOSQL_DO.get(doId).fetch(request);
    }

    // HTTP request handling...
  }
};

function isAllowedOrigin(origin: string | null, allowed: string[]): boolean {
  if (!origin) return false;
  return allowed.some(pattern => {
    if (pattern === '*') return true;
    if (pattern.startsWith('*.')) {
      const domain = pattern.slice(2);
      return origin.endsWith(domain);
    }
    return origin === pattern;
  });
}
```

#### HTTP Batch Request Security

```typescript
// Validate batch request limits
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

## 2. SQL Injection Prevention

### 2.1 Parameterized Queries (Always Use `?`)

DoSQL provides a secure tokenizer and parameterized query support. **Always** use parameter placeholders instead of string concatenation.

```typescript
import { createDatabase } from '@dotdo/dosql';

const db = createDatabase();

// CORRECT: Parameterized query
const userId = request.params.id; // User input
const user = db.prepare('SELECT * FROM users WHERE id = ?').get(userId);

// CORRECT: Named parameters
const stmt = db.prepare('SELECT * FROM users WHERE name = :name AND status = :status');
const users = stmt.all({ name: userName, status: 'active' });

// CORRECT: Multiple parameters
const stmt = db.prepare(`
  INSERT INTO orders (user_id, product_id, quantity, price)
  VALUES (?, ?, ?, ?)
`);
stmt.run(userId, productId, quantity, price);
```

**WRONG - Never Do This:**

```typescript
// CRITICAL VULNERABILITY: String interpolation
const userId = request.params.id; // Could be: "1; DROP TABLE users; --"
const user = db.prepare(`SELECT * FROM users WHERE id = ${userId}`).get();

// CRITICAL VULNERABILITY: String concatenation
const searchTerm = request.query.search;
const sql = "SELECT * FROM products WHERE name LIKE '%" + searchTerm + "%'";

// CRITICAL VULNERABILITY: Template literals without parameters
const user = db.prepare(`SELECT * FROM users WHERE email = '${email}'`).get();
```

### 2.2 DoSQL's State-Aware Tokenizer

DoSQL includes a secure SQL tokenizer that properly handles edge cases like semicolons inside string literals and comments. This provides defense-in-depth but is NOT a substitute for parameterized queries.

From `src/database/tokenizer.ts`:

```typescript
/**
 * Tokenize a SQL string into individual statements.
 *
 * This function implements a state-aware lexer that properly handles:
 * - String literals with escaped quotes
 * - Double-quoted and backtick-quoted identifiers
 * - Block comments (/* ... */)
 * - Line comments (-- ...)
 *
 * Only splits on semicolons in NORMAL state.
 */
export function tokenizeSQL(sql: string): string[] {
  // State machine: 'normal' | 'single_quote' | 'double_quote' |
  //                'backtick' | 'block_comment' | 'line_comment'
  // ... implementation details ...
}
```

The tokenizer prevents:
- Statement splitting on semicolons inside strings: `'a;b'` is one statement
- Comment-based injection: `-- ; DROP TABLE` doesn't split
- Quote-escaped injection: `'O''Brien; DROP'` is properly handled

### 2.3 Input Validation Patterns

Always validate input before it reaches your queries:

```typescript
import { z } from 'zod';

// Define schemas for your inputs
const UserIdSchema = z.string().uuid();
const UserNameSchema = z.string().min(1).max(100).regex(/^[a-zA-Z0-9_-]+$/);
const EmailSchema = z.string().email().max(255);
const PageSchema = z.number().int().positive().max(1000);
const LimitSchema = z.number().int().positive().max(100);

// Validate before querying
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

  // Safe to use in query
  const user = db.prepare('SELECT * FROM users WHERE id = ?').get(result.data);
  return new Response(JSON.stringify(user));
}

// Validate table/column names if dynamic (rare, avoid if possible)
const ALLOWED_TABLES = new Set(['users', 'orders', 'products']);
const ALLOWED_SORT_COLUMNS = new Set(['created_at', 'name', 'price']);

function validateTableName(name: string): string {
  if (!ALLOWED_TABLES.has(name)) {
    throw new Error('Invalid table name');
  }
  return name;
}
```

### 2.4 Anti-Patterns to Avoid

```typescript
// BAD: Dynamic table names from user input
const table = request.query.table;
db.exec(`SELECT * FROM ${table}`); // INJECTION RISK

// BAD: Dynamic column names
const sortBy = request.query.sort;
db.exec(`SELECT * FROM users ORDER BY ${sortBy}`); // INJECTION RISK

// BAD: Building WHERE clauses dynamically
const conditions = [];
if (name) conditions.push(`name = '${name}'`); // INJECTION RISK
const where = conditions.join(' AND ');

// BETTER: Use allow-lists for dynamic parts
const ALLOWED_SORT = { name: 'name', date: 'created_at', price: 'price' };
const sortColumn = ALLOWED_SORT[request.query.sort] || 'created_at';
const sql = `SELECT * FROM products ORDER BY ${sortColumn}`; // Safe - from allow-list

// BEST: Build queries programmatically with a query builder
const query = db.prepare('SELECT * FROM products ORDER BY ?').bind(sortColumn);
```

---

## 3. Authentication & Authorization

### 3.1 Worker-Level Auth Patterns

Authentication should happen at the Worker layer BEFORE requests reach the Durable Object:

```typescript
// auth.ts - Authentication middleware
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
  // Check Authorization header
  const authHeader = request.headers.get('Authorization');
  if (!authHeader?.startsWith('Bearer ')) {
    return null;
  }

  const token = authHeader.slice(7);

  try {
    // Verify JWT
    const isValid = await verify(token, env.JWT_SECRET);
    if (!isValid) return null;

    // Decode payload (verify already checked signature)
    const payload = JSON.parse(atob(token.split('.')[1]));

    // Validate required claims
    if (!payload.sub || !payload.tenant_id) {
      return null;
    }

    // Check token expiration
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

// Worker entry point
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Authenticate first
    const user = await authenticateRequest(request, env);

    if (!user) {
      return new Response(
        JSON.stringify({ error: 'Unauthorized' }),
        {
          status: 401,
          headers: { 'Content-Type': 'application/json' }
        }
      );
    }

    // Add user context to request
    const enrichedRequest = new Request(request, {
      headers: new Headers(request.headers)
    });
    enrichedRequest.headers.set('X-User-Id', user.id);
    enrichedRequest.headers.set('X-Tenant-Id', user.tenantId);
    enrichedRequest.headers.set('X-User-Roles', JSON.stringify(user.roles));

    // Route to tenant's Durable Object
    const doId = env.DOSQL_DO.idFromName(`tenant:${user.tenantId}`);
    return env.DOSQL_DO.get(doId).fetch(enrichedRequest);
  }
};
```

### 3.2 Durable Object Access Control

Inside the Durable Object, implement role-based access control:

```typescript
// Inside Durable Object
export class DoSQLDurableObject implements DurableObject {
  constructor(
    private state: DurableObjectState,
    private env: Env
  ) {}

  async fetch(request: Request): Promise<Response> {
    // Extract user context (set by Worker)
    const userId = request.headers.get('X-User-Id');
    const roles = JSON.parse(request.headers.get('X-User-Roles') || '[]');

    // Parse the RPC request
    const rpcRequest = await request.json();

    // Authorization check
    if (!this.isAuthorized(rpcRequest.method, roles)) {
      return new Response(
        JSON.stringify({ error: 'Forbidden', code: 'INSUFFICIENT_PERMISSIONS' }),
        { status: 403 }
      );
    }

    // Process the request...
  }

  private isAuthorized(method: string, roles: string[]): boolean {
    // Define permission requirements
    const PERMISSIONS: Record<string, string[]> = {
      'query': ['read', 'admin'],
      'batch': ['write', 'admin'],
      'beginTransaction': ['write', 'admin'],
      'commit': ['write', 'admin'],
      'rollback': ['write', 'admin'],
      'getSchema': ['read', 'admin'],
      'subscribeCDC': ['read', 'admin'],
      // Admin-only operations
      'dropTable': ['admin'],
      'truncate': ['admin'],
    };

    const required = PERMISSIONS[method] || ['admin'];
    return required.some(perm => roles.includes(perm));
  }
}
```

### 3.3 API Key Management

For service-to-service authentication, use API keys with proper scoping:

```typescript
// API Key structure stored in KV
interface APIKey {
  id: string;
  hashedKey: string;      // SHA-256 hash of the key
  tenantId: string;
  scopes: string[];       // ['read', 'write', 'admin']
  rateLimit: number;      // Requests per minute
  expiresAt?: number;     // Optional expiration
  createdAt: number;
  lastUsedAt?: number;
}

// Generate API key (run once, store hash)
async function generateAPIKey(tenantId: string, scopes: string[]): Promise<string> {
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

  // Return the key to the user (only time it's visible)
  return key;
}

async function hashKey(key: string): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(key);
  const hash = await crypto.subtle.digest('SHA-256', data);
  return btoa(String.fromCharCode(...new Uint8Array(hash)));
}

// Validate API key
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

  // Update last used (async, don't wait)
  env.API_KEYS.put(`apikey:${keyId}`, JSON.stringify({
    ...apiKey,
    lastUsedAt: Date.now(),
  }));

  return apiKey;
}
```

### 3.4 Rate Limiting

Implement rate limiting to prevent abuse:

```typescript
// Simple sliding window rate limiter using Durable Objects
export class RateLimiter implements DurableObject {
  private requests: number[] = [];
  private windowMs = 60000; // 1 minute
  private maxRequests = 1000;

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
            'X-RateLimit-Reset': String(Math.ceil((this.requests[0] + this.windowMs) / 1000)),
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

---

## 4. Data Encryption

### 4.1 At-Rest Encryption (R2)

Cloudflare R2 provides server-side encryption at rest by default. For additional security with sensitive data:

```typescript
// Application-level encryption for sensitive fields
import { encrypt, decrypt } from './crypto-utils';

interface User {
  id: string;
  email: string;
  ssn_encrypted: string;  // Sensitive field encrypted at application level
  ssn_iv: string;         // Initialization vector
}

// Encrypt before storing
async function createUser(data: { email: string; ssn: string }, env: Env): Promise<void> {
  const { ciphertext, iv } = await encrypt(data.ssn, env.ENCRYPTION_KEY);

  db.prepare(`
    INSERT INTO users (id, email, ssn_encrypted, ssn_iv)
    VALUES (?, ?, ?, ?)
  `).run(crypto.randomUUID(), data.email, ciphertext, iv);
}

// Decrypt after retrieval
async function getUserSSN(userId: string, env: Env): Promise<string | null> {
  const user = db.prepare('SELECT ssn_encrypted, ssn_iv FROM users WHERE id = ?')
    .get(userId) as User | null;

  if (!user) return null;

  return decrypt(user.ssn_encrypted, user.ssn_iv, env.ENCRYPTION_KEY);
}

// crypto-utils.ts
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

### 4.2 In-Transit Encryption (TLS)

Cloudflare enforces TLS for all edge traffic. Ensure your configuration is optimal:

```toml
# wrangler.toml
[vars]
# Force HTTPS for all requests
FORCE_HTTPS = "true"

# HSTS header settings
HSTS_MAX_AGE = "31536000"
HSTS_INCLUDE_SUBDOMAINS = "true"
```

```typescript
// Middleware to enforce HTTPS
export function enforceHTTPS(request: Request): Response | null {
  if (request.headers.get('X-Forwarded-Proto') === 'http') {
    const url = new URL(request.url);
    url.protocol = 'https:';
    return Response.redirect(url.toString(), 301);
  }
  return null;
}

// Add security headers to responses
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

### 4.3 Sensitive Data Handling

Define and enforce handling rules for sensitive data:

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

// Redact sensitive fields in logs
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
  const maskedLocal = local.slice(0, 2) + '***';
  return `${maskedLocal}@${domain}`;
}

function maskCreditCard(number: string): string {
  return `****-****-****-${number.slice(-4)}`;
}
```

---

## 5. Audit Logging

### 5.1 What to Log

DoSQL's CDC (Change Data Capture) system provides the foundation for audit logging:

```typescript
import { createCDC, type ChangeEvent } from '@dotdo/dosql/cdc';

// Events to capture for security audit
interface AuditEvent {
  timestamp: Date;
  eventType: 'auth' | 'query' | 'change' | 'error' | 'admin';
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

// Audit log levels
const AUDIT_EVENTS = {
  // Authentication events (always log)
  AUTH_SUCCESS: 'auth.success',
  AUTH_FAILURE: 'auth.failure',
  AUTH_LOGOUT: 'auth.logout',
  TOKEN_REFRESH: 'auth.token_refresh',

  // Query events (configurable - can be verbose)
  QUERY_READ: 'query.read',
  QUERY_WRITE: 'query.write',
  QUERY_DDL: 'query.ddl',

  // Data change events (via CDC)
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

  // Error events (always log)
  SQL_INJECTION_ATTEMPT: 'error.sql_injection',
  AUTH_BYPASS_ATTEMPT: 'error.auth_bypass',
  RATE_LIMIT_EXCEEDED: 'error.rate_limit',
  INVALID_INPUT: 'error.invalid_input',
};
```

### 5.2 Audit Logging Implementation

```typescript
// audit-logger.ts
export class AuditLogger {
  private queue: AuditEvent[] = [];
  private flushInterval: number = 5000; // 5 seconds

  constructor(
    private storage: R2Bucket,
    private tenantId: string
  ) {
    // Flush periodically
    setInterval(() => this.flush(), this.flushInterval);
  }

  log(event: Omit<AuditEvent, 'timestamp' | 'requestId'>): void {
    this.queue.push({
      ...event,
      timestamp: new Date(),
      requestId: crypto.randomUUID(),
    });

    // Immediate flush for security-critical events
    if (this.isSecurityCritical(event.eventType, event.action)) {
      this.flush();
    }
  }

  private isSecurityCritical(type: string, action: string): boolean {
    const critical = [
      'AUTH_FAILURE',
      'SQL_INJECTION_ATTEMPT',
      'AUTH_BYPASS_ATTEMPT',
      'PERMISSION_CHANGE',
      'API_KEY_REVOKE',
    ];
    return critical.includes(action);
  }

  async flush(): Promise<void> {
    if (this.queue.length === 0) return;

    const events = this.queue;
    this.queue = [];

    // Store in R2 with time-based partitioning
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
    await this.storage.put(path, content, {
      customMetadata: {
        eventCount: String(events.length),
        tenantId: this.tenantId,
      },
    });
  }

  // Log authentication events
  logAuth(
    success: boolean,
    userId: string,
    details: Record<string, unknown>,
    request: Request
  ): void {
    this.log({
      eventType: 'auth',
      userId,
      tenantId: this.tenantId,
      action: success ? AUDIT_EVENTS.AUTH_SUCCESS : AUDIT_EVENTS.AUTH_FAILURE,
      details,
      ipAddress: request.headers.get('CF-Connecting-IP') || undefined,
      userAgent: request.headers.get('User-Agent') || undefined,
      success,
    });
  }

  // Log query execution
  logQuery(
    userId: string,
    sql: string,
    queryType: 'read' | 'write' | 'ddl',
    executionTimeMs: number,
    success: boolean,
    errorMessage?: string
  ): void {
    this.log({
      eventType: 'query',
      userId,
      tenantId: this.tenantId,
      action: `query.${queryType}`,
      details: {
        sql: sql.slice(0, 1000), // Truncate long queries
        executionTimeMs,
      },
      success,
      errorMessage,
    });
  }

  // Log CDC events for data changes
  logDataChange(event: ChangeEvent, userId: string): void {
    this.log({
      eventType: 'change',
      userId,
      tenantId: this.tenantId,
      action: `data.${event.type}`,
      resource: event.table,
      details: {
        lsn: event.lsn.toString(),
        primaryKey: event.key ? Array.from(event.key) : undefined,
        // Don't log full row data - may contain sensitive info
      },
      success: true,
    });
  }
}

// Usage in Durable Object
export class DoSQLDurableObject implements DurableObject {
  private auditLogger: AuditLogger;

  constructor(state: DurableObjectState, env: Env) {
    const tenantId = state.id.toString();
    this.auditLogger = new AuditLogger(env.AUDIT_BUCKET, tenantId);

    // Subscribe to CDC for data change auditing
    this.setupCDCAudit();
  }

  private async setupCDCAudit(): Promise<void> {
    const cdc = createCDC(this.backend);

    for await (const event of cdc.subscribe({ fromLSN: 0n })) {
      // Get user from transaction context (if available)
      const userId = this.getCurrentUserId() || 'system';
      this.auditLogger.logDataChange(event, userId);
    }
  }
}
```

### 5.3 Compliance Considerations

For regulatory compliance (SOC 2, GDPR, HIPAA, etc.):

```typescript
// Compliance configuration
interface ComplianceConfig {
  // Retention periods
  auditLogRetentionDays: number;      // SOC 2: min 90 days
  accessLogRetentionDays: number;

  // Data handling
  encryptPII: boolean;                 // GDPR, HIPAA
  enableDataMasking: boolean;
  allowDataExport: boolean;            // GDPR right to portability
  allowDataDeletion: boolean;          // GDPR right to erasure

  // Access controls
  requireMFA: boolean;                 // SOC 2, HIPAA
  enforcePasswordPolicy: boolean;
  sessionTimeoutMinutes: number;

  // Monitoring
  enableAnomalyDetection: boolean;
  alertOnFailedAuth: number;           // Alert after N failures
}

const SOC2_CONFIG: ComplianceConfig = {
  auditLogRetentionDays: 365,
  accessLogRetentionDays: 90,
  encryptPII: true,
  enableDataMasking: true,
  allowDataExport: false,
  allowDataDeletion: false,
  requireMFA: true,
  enforcePasswordPolicy: true,
  sessionTimeoutMinutes: 30,
  enableAnomalyDetection: true,
  alertOnFailedAuth: 5,
};

const GDPR_CONFIG: ComplianceConfig = {
  auditLogRetentionDays: 365,
  accessLogRetentionDays: 90,
  encryptPII: true,
  enableDataMasking: true,
  allowDataExport: true,      // Required by GDPR
  allowDataDeletion: true,    // Required by GDPR
  requireMFA: true,
  enforcePasswordPolicy: true,
  sessionTimeoutMinutes: 60,
  enableAnomalyDetection: true,
  alertOnFailedAuth: 5,
};

// GDPR Data Subject Request handling
interface DataSubjectRequest {
  type: 'access' | 'rectification' | 'erasure' | 'portability' | 'restriction';
  subjectEmail: string;
  requestDate: Date;
  responseDeadline: Date;  // 30 days from request
  status: 'pending' | 'in_progress' | 'completed' | 'denied';
}

async function handleErasureRequest(
  userId: string,
  env: Env,
  auditLogger: AuditLogger
): Promise<void> {
  // Log the request
  auditLogger.log({
    eventType: 'admin',
    userId: 'system',
    tenantId: env.TENANT_ID,
    action: 'gdpr.erasure_request',
    details: { targetUserId: userId },
    success: true,
  });

  // Delete user data
  db.prepare('DELETE FROM users WHERE id = ?').run(userId);
  db.prepare('DELETE FROM user_preferences WHERE user_id = ?').run(userId);
  db.prepare('DELETE FROM user_sessions WHERE user_id = ?').run(userId);

  // Anonymize audit logs (keep for compliance, but anonymize)
  db.prepare(`
    UPDATE audit_logs
    SET user_id = 'anonymized', details = '{"anonymized": true}'
    WHERE user_id = ?
  `).run(userId);

  // Log completion
  auditLogger.log({
    eventType: 'admin',
    userId: 'system',
    tenantId: env.TENANT_ID,
    action: 'gdpr.erasure_completed',
    details: { targetUserId: userId },
    success: true,
  });
}
```

---

## 6. Security Checklist

### 6.1 Pre-Deployment Checklist

Use this checklist before deploying to production:

#### Authentication & Authorization
- [ ] JWT validation implemented with proper signature verification
- [ ] Token expiration enforced
- [ ] API keys are hashed before storage (never store plaintext)
- [ ] Role-based access control implemented
- [ ] Rate limiting configured
- [ ] Failed auth attempts are logged and limited

#### SQL Injection Prevention
- [ ] All user inputs use parameterized queries (`?` placeholders)
- [ ] No string concatenation/interpolation in SQL queries
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
- [ ] Data masking implemented for logs/displays
- [ ] Backup encryption enabled

#### Tenant Isolation
- [ ] Each tenant has dedicated Durable Object
- [ ] No shared state between tenants
- [ ] Tenant ID derived from authenticated token (not user input)
- [ ] Cross-tenant access testing performed

#### Monitoring & Logging
- [ ] Audit logging enabled for auth events
- [ ] Audit logging enabled for admin operations
- [ ] CDC configured for data change tracking
- [ ] Error logging captures security events
- [ ] Log retention policies configured

### 6.2 Regular Security Review Items

Perform these reviews on a regular schedule:

#### Weekly
- [ ] Review failed authentication logs
- [ ] Check rate limiting effectiveness
- [ ] Monitor for unusual query patterns
- [ ] Review error logs for injection attempts

#### Monthly
- [ ] Rotate API keys for service accounts
- [ ] Review user access permissions
- [ ] Audit privileged operations log
- [ ] Test backup restoration

#### Quarterly
- [ ] Dependency vulnerability scan (`npm audit`)
- [ ] Review and update security documentation
- [ ] Penetration testing (internal or external)
- [ ] Incident response drill

#### Annually
- [ ] Full security architecture review
- [ ] Compliance audit preparation
- [ ] Key rotation for encryption keys
- [ ] Third-party security assessment

### 6.3 Incident Response Quick Reference

```typescript
// Emergency: Disable compromised API key
await env.API_KEYS.delete(`hash:${hashedKey}`);
await env.API_KEYS.delete(`apikey:${keyId}`);

// Emergency: Invalidate all sessions for a user
db.prepare('DELETE FROM user_sessions WHERE user_id = ?').run(userId);

// Emergency: Block IP address (using Cloudflare WAF API)
await fetch(`https://api.cloudflare.com/client/v4/zones/${zoneId}/firewall/access_rules/rules`, {
  method: 'POST',
  headers: { 'Authorization': `Bearer ${env.CF_API_TOKEN}` },
  body: JSON.stringify({
    mode: 'block',
    configuration: { target: 'ip', value: suspiciousIP },
    notes: `Blocked due to security incident at ${new Date().toISOString()}`,
  }),
});

// Emergency: Enable maintenance mode (read-only)
await env.KV.put('maintenance_mode', 'true');
```

---

## Related Documentation

- [ARCHITECTURE_REVIEW.md](./ARCHITECTURE_REVIEW.md) - System architecture details
- [USE_CASES.md](./USE_CASES.md) - Common usage patterns

---

## Cloudflare Security Documentation References

DoSQL leverages Cloudflare's security infrastructure. For comprehensive understanding of the underlying security model, consult these official Cloudflare resources:

### Durable Objects Security

- [Durable Objects Overview](https://developers.cloudflare.com/durable-objects/) - Core architecture and isolation model
- [Durable Objects Security Model](https://developers.cloudflare.com/durable-objects/reference/security-model/) - How DOs provide tenant isolation and data protection
- [Durable Objects Storage Limits](https://developers.cloudflare.com/durable-objects/platform/limits/) - Storage constraints and rate limiting
- [Jurisdictional Restrictions](https://developers.cloudflare.com/durable-objects/reference/data-location/) - Data residency and geographic controls

### R2 Access Control

- [R2 Overview](https://developers.cloudflare.com/r2/) - Object storage fundamentals
- [R2 Authentication](https://developers.cloudflare.com/r2/api/s3/tokens/) - API tokens and access credentials
- [R2 Bucket Access](https://developers.cloudflare.com/r2/buckets/public-buckets/) - Public vs private bucket configurations
- [R2 Encryption](https://developers.cloudflare.com/r2/reference/data-security/) - Server-side encryption at rest
- [R2 Access Policies](https://developers.cloudflare.com/r2/api/s3/iam-policies/) - IAM-style access policies for fine-grained control

### Workers Security Model

- [Workers Security](https://developers.cloudflare.com/workers/platform/security/) - Runtime isolation and sandboxing
- [Workers Runtime APIs](https://developers.cloudflare.com/workers/runtime-apis/) - Secure cryptographic APIs available in Workers
- [Cloudflare Access Integration](https://developers.cloudflare.com/cloudflare-one/applications/configure-apps/self-hosted-apps/) - Zero-trust authentication for Workers
- [mTLS Authentication](https://developers.cloudflare.com/cloudflare-one/identity/devices/mutual-tls-authentication/) - Mutual TLS for service-to-service security
- [Web Application Firewall (WAF)](https://developers.cloudflare.com/waf/) - Request filtering and threat protection
- [DDoS Protection](https://developers.cloudflare.com/ddos-protection/) - Automatic DDoS mitigation for Workers endpoints
- [Secrets Management](https://developers.cloudflare.com/workers/configuration/secrets/) - Secure storage of API keys and credentials

### Additional Security Resources

- [Cloudflare Security Center](https://developers.cloudflare.com/security-center/) - Security posture monitoring
- [Cloudflare Zero Trust](https://developers.cloudflare.com/cloudflare-one/) - Enterprise security and access control
- [Bot Management](https://developers.cloudflare.com/bots/) - Protection against automated threats
- [Rate Limiting](https://developers.cloudflare.com/waf/rate-limiting-rules/) - Advanced rate limiting rules

---

*This security guide is maintained as part of the DoSQL project. Report security vulnerabilities responsibly.*
