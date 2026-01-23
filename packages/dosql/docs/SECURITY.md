# DoSQL Security Model

**Version:** 3.1
**Last Updated:** 2026-01-23
**Applies To:** DoSQL Edge Database for Cloudflare Workers

---

## Executive Summary

DoSQL is a SQL database engine designed for Cloudflare Workers and Durable Objects. This document explains the security model, what DoSQL protects against, what you must implement, and how to deploy securely.

**Security at a Glance:**

| Property | Protection | How It Works |
|----------|------------|--------------|
| Tenant Isolation | Strong | Each tenant operates in a dedicated Durable Object with isolated storage |
| SQL Injection Prevention | Strong | Parameterized queries and state-aware tokenizer |
| Transport Security | Strong | Cloudflare-managed TLS 1.3 for all traffic |
| Data at Rest | Strong | AES-256 encryption managed by Cloudflare |
| Defense in Depth | Comprehensive | Multiple layers from network edge to storage |

**Your Responsibilities:**
- Authentication (JWT/API key verification)
- Authorization (role-based access control)
- Input validation (schema validation)
- Audit logging (security events)

---

## Table of Contents

1. [Security Boundaries](#1-security-boundaries)
2. [SQL Injection Prevention](#2-sql-injection-prevention)
3. [Tenant Isolation Model](#3-tenant-isolation-model)
4. [Authentication Patterns](#4-authentication-patterns)
5. [Authorization and Access Control](#5-authorization-and-access-control)
6. [Data Protection](#6-data-protection)
7. [Network Security](#7-network-security)
8. [Configuration Reference](#8-configuration-reference)
9. [Audit and Compliance](#9-audit-and-compliance)
10. [Security Checklist](#10-security-checklist)
11. [Incident Response](#11-incident-response)
12. [References](#12-references)

---

## 1. Security Boundaries

### 1.1 Architecture Overview

DoSQL operates within Cloudflare's edge infrastructure. Security is enforced at four distinct layers:

```
    INTERNET                   TRUST BOUNDARY                 CLOUDFLARE EDGE
                                     |
  +-------------+                    |              +------------------------+
  |   Client    |                    |              |     Worker (Entry)     |
  | - Browser   |  ---- TLS 1.3 ---- | -----------> | - Authentication       |
  | - API       |                    |              | - Rate Limiting        |
  | - Mobile    |                    |              | - Input Validation     |
  +-------------+                    |              +-----------+------------+
                                     |                          |
       Untrusted                     |              +-----------v------------+
                                     |              |    Durable Object      |
                                     |              |    (DoSQL Engine)      |
                                     |              | - Query Execution      |
                                     |              | - Transaction Mgmt     |
                                     |              | - Tenant Isolation     |
                                     |              +-----------+------------+
                                     |                          |
                                     |              +-----------v------------+
                                     |              |    R2 Cold Storage     |
                                     |              |    (Encrypted)         |
                                     |              +------------------------+
                                     |
                                     |                      Trusted
```

**Layer Responsibilities:**

| Layer | What It Protects | Managed By |
|-------|------------------|------------|
| Network Edge | DDoS, TLS termination, WAF | Cloudflare |
| Worker Entry | Authentication, rate limits, validation | You |
| Durable Object | Tenant isolation, query safety | DoSQL + You |
| Storage | Encryption at rest | Cloudflare |

### 1.2 Trust Boundaries

| Boundary | Controls | Your Responsibility |
|----------|----------|---------------------|
| Internet to Edge | TLS encryption, DDoS protection | Configure WAF rules |
| Edge to Worker | Request routing | Implement authentication |
| Worker to DO | Tenant routing | Derive tenant ID from verified token |
| DO to R2 | Binding-based access control | None (Cloudflare managed) |

### 1.3 Threat Model

#### What DoSQL Protects Against

| Threat | Protection | Notes |
|--------|------------|-------|
| SQL Injection | Parameterized queries, state-aware tokenizer | Always use `?` placeholders |
| Cross-Tenant Access | DO isolation (one DO per tenant) | Each tenant has isolated storage |
| Man-in-the-Middle | TLS 1.3 enforced by Cloudflare | Automatic for all edge traffic |
| Data at Rest Exposure | AES-256 encryption | Cloudflare R2 server-side encryption |
| Query DoS | Configurable timeouts, row limits | Set limits based on workload |
| Memory Exhaustion | Result limits, batch size limits | Configure appropriate limits |

#### What You Must Implement

| Threat | Your Responsibility | Recommended Approach |
|--------|---------------------|----------------------|
| Unauthorized Access | Worker authentication | JWT verification, API key validation |
| Privilege Escalation | RBAC implementation | Role-based permissions per operation |
| Credential Stuffing | Failed auth limiting | Rate limit + progressive lockout |
| Data Exfiltration | Audit logging | Log all data access patterns |
| Replay Attacks | Token validation | Short-lived tokens, nonce validation |

### 1.4 Security Implementation Summary

```typescript
// Every request must pass through these checks:

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // 1. AUTHENTICATE - Verify user identity
    const user = await authenticateRequest(request, env);
    if (!user) {
      return new Response('Unauthorized', { status: 401 });
    }

    // 2. AUTHORIZE - Check permissions
    const requiredRole = getRequiredRole(request);
    if (!user.roles.includes(requiredRole)) {
      return new Response('Forbidden', { status: 403 });
    }

    // 3. ISOLATE - Route to tenant DO (ID from verified token, never user input)
    const doId = env.DOSQL_DO.idFromName(`tenant:${user.tenantId}`);
    const stub = env.DOSQL_DO.get(doId);

    // 4. VALIDATE - Input validation happens in the request handler
    // 5. AUDIT - Logging happens in the DO

    return stub.fetch(request);
  }
};
```

---

## 2. SQL Injection Prevention

SQL injection is the most critical database vulnerability. DoSQL provides multiple layers of defense.

### 2.1 Parameterized Queries (Primary Defense)

**Always use parameter placeholders.** This is your most important security practice.

```typescript
import { createDatabase } from '@dotdo/dosql';

const db = createDatabase();

// CORRECT: Positional parameters
const user = db.prepare('SELECT * FROM users WHERE id = ?').get(userId);

// CORRECT: Named parameters
const user = db.prepare('SELECT * FROM users WHERE email = :email')
  .get({ email: userEmail });

// CORRECT: Multiple parameters
db.prepare('INSERT INTO orders (user_id, product_id, qty) VALUES (?, ?, ?)')
  .run(userId, productId, quantity);

// CORRECT: IN clause with array
const ids = [1, 2, 3];
const placeholders = ids.map(() => '?').join(', ');
db.prepare(`SELECT * FROM users WHERE id IN (${placeholders})`).all(...ids);
```

**Never do this:**

```typescript
// VULNERABLE: String interpolation
db.prepare(`SELECT * FROM users WHERE id = ${userId}`).get();
//                                         ^^^^^^^^^^ INJECTION POINT

// VULNERABLE: String concatenation
const sql = "SELECT * FROM users WHERE name = '" + name + "'";
//                                                 ^^^^ INJECTION POINT

// VULNERABLE: Template literal without parameters
db.prepare(`SELECT * FROM users WHERE email = '${email}'`).get();
//                                              ^^^^^^^^ INJECTION POINT
```

### 2.2 State-Aware Tokenizer (Defense in Depth)

When using `exec()` for multi-statement execution, DoSQL's tokenizer correctly handles edge cases:

```typescript
// The tokenizer tracks parsing context:

// Semicolons inside strings - NOT statement separators
db.exec("INSERT INTO users (name) VALUES ('John; Doe')");
// -> Single statement

// Semicolons inside comments - NOT statement separators
db.exec("SELECT * /* comment; here */ FROM users");
// -> Single statement

// Escaped quotes with semicolons
db.exec("SELECT 'Value with ''semicolon;'''");
// -> Single statement
```

**Tokenizer State Machine:**

| State | Context | Semicolon Behavior |
|-------|---------|-------------------|
| `normal` | SQL parsing | Splits statements |
| `single_quote` | Inside `'string'` | Literal character |
| `double_quote` | Inside `"identifier"` | Literal character |
| `block_comment` | Inside `/* */` | Literal character |
| `line_comment` | After `--` | Literal character |

**Important:** The tokenizer is defense-in-depth, not primary protection. Always use parameterized queries.

### 2.3 Safe Dynamic SQL Patterns

When you need dynamic table or column names, use allow-lists:

```typescript
// Safe table name handling
const ALLOWED_TABLES = new Set(['users', 'orders', 'products']) as ReadonlySet<string>;

function getTableName(input: string): string {
  if (!ALLOWED_TABLES.has(input)) {
    throw new Error(`Invalid table: ${input}`);
  }
  return input;
}

const table = getTableName(userInput);
const result = db.prepare(`SELECT * FROM ${table} WHERE id = ?`).get(id);

// Safe column sorting
const SORT_COLUMNS: Readonly<Record<string, string>> = {
  name: 'name',
  date: 'created_at',
  email: 'email',
};

const column = SORT_COLUMNS[userInput.sort];
if (!column) {
  return new Response('Invalid sort column', { status: 400 });
}

const direction = userInput.dir?.toUpperCase() === 'DESC' ? 'DESC' : 'ASC';
const sql = `SELECT * FROM users ORDER BY ${column} ${direction}`;
```

### 2.4 Input Validation

Validate all inputs before they reach queries:

```typescript
import { z } from 'zod';

const SearchSchema = z.object({
  query: z.string().min(1).max(100).transform(s => s.trim()),
  page: z.number().int().positive().max(1000).default(1),
  limit: z.number().int().min(1).max(100).default(20),
});

async function search(request: Request): Promise<Response> {
  const result = SearchSchema.safeParse(await request.json());

  if (!result.success) {
    return new Response(JSON.stringify({
      error: 'Invalid input',
      details: result.error.flatten(),
    }), { status: 400 });
  }

  const { query, page, limit } = result.data;

  // Safe to use in parameterized query
  const users = db.prepare(`
    SELECT id, name, email FROM users
    WHERE name LIKE ? LIMIT ? OFFSET ?
  `).all(`%${query}%`, limit, (page - 1) * limit);

  return Response.json(users);
}
```

---

## 3. Tenant Isolation Model

### 3.1 One Durable Object Per Tenant

DoSQL's primary isolation mechanism uses Cloudflare Durable Objects. Each tenant gets a dedicated DO instance with completely isolated storage.

```typescript
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Authenticate and get tenant from verified token
    const user = await authenticateRequest(request, env);
    if (!user) {
      return new Response('Unauthorized', { status: 401 });
    }

    // Route to tenant-specific DO - isolated storage
    const doId = env.DOSQL_DO.idFromName(`tenant:${user.tenantId}`);
    return env.DOSQL_DO.get(doId).fetch(request);
  }
};
```

### 3.2 Isolation Guarantees

| Property | Guarantee | Enforced By |
|----------|-----------|-------------|
| Storage Isolation | Each DO has its own SQLite storage | Cloudflare |
| Memory Isolation | No shared memory between DOs | V8 isolates |
| Execution Isolation | Each DO runs in its own isolate | Cloudflare |
| Namespace Isolation | DO IDs scoped to your Worker | Cloudflare |

**Why DO isolation is superior to row-level filtering:**

```
Row-Level Filtering (Fragile):        DO-Level Isolation (Structural):

+---------------------------+         +-------------+  +-------------+
|     SHARED DATABASE       |         |  Tenant A   |  |  Tenant B   |
| +-----------------------+ |         |    DO       |  |    DO       |
| | A | B | A | B | A | B | |         | +---------+ |  | +---------+ |
| +-----------------------+ |         | | Storage | |  | | Storage | |
|                           |         | +---------+ |  | +---------+ |
| WHERE tenant_id = ?       |         +-------------+  +-------------+
| (One missing filter =     |
|  data breach)             |         No tenant_id needed -
+---------------------------+         isolation is architectural
```

### 3.3 Critical Anti-Patterns

```typescript
// WRONG: Shared DO for multiple tenants
const doId = env.DOSQL_DO.idFromName('shared-database');
// NO ISOLATION - all tenants share the same database

// WRONG: Tenant ID from user-controlled header
const tenantId = request.headers.get('X-Tenant-Id');
const doId = env.DOSQL_DO.idFromName(`tenant:${tenantId}`);
// FORGERY RISK - attacker can specify any tenant

// WRONG: Tenant ID from URL parameter
const tenantId = new URL(request.url).searchParams.get('tenant');
const doId = env.DOSQL_DO.idFromName(`tenant:${tenantId}`);
// FORGERY RISK - attacker can specify any tenant

// WRONG: Tenant ID from request body
const { tenantId } = await request.json();
const doId = env.DOSQL_DO.idFromName(`tenant:${tenantId}`);
// FORGERY RISK - attacker can specify any tenant

// CORRECT: Tenant ID from verified JWT claim
const token = await verifyJWT(request.headers.get('Authorization'), env.JWT_SECRET);
const tenantId = token.tenant_id;  // Cryptographically verified
const doId = env.DOSQL_DO.idFromName(`tenant:${tenantId}`);
```

---

## 4. Authentication Patterns

### 4.1 JWT Authentication

Authenticate at the Worker layer before requests reach the Durable Object:

```typescript
import { verify } from '@tsndr/cloudflare-worker-jwt';

interface AuthUser {
  id: string;
  tenantId: string;
  email: string;
  roles: string[];
}

async function authenticateRequest(
  request: Request,
  env: Env
): Promise<AuthUser | null> {
  const authHeader = request.headers.get('Authorization');
  if (!authHeader?.startsWith('Bearer ')) {
    return null;
  }

  const token = authHeader.slice(7);

  try {
    // Verify signature
    if (!await verify(token, env.JWT_SECRET)) {
      return null;
    }

    // Decode and validate payload
    const payload = JSON.parse(atob(token.split('.')[1]));

    // Required claims
    if (!payload.sub || !payload.tenant_id) {
      return null;
    }

    // Expiration check
    const now = Date.now() / 1000;
    if (payload.exp && payload.exp < now) {
      return null;
    }

    // Not-before check
    if (payload.nbf && payload.nbf > now) {
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

**Worker Entry Point:**

```typescript
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const user = await authenticateRequest(request, env);

    if (!user) {
      return new Response(JSON.stringify({ error: 'Unauthorized' }), {
        status: 401,
        headers: {
          'Content-Type': 'application/json',
          'WWW-Authenticate': 'Bearer realm="DoSQL API"',
        },
      });
    }

    // Enrich request with verified user context
    const headers = new Headers(request.headers);
    headers.set('X-User-Id', user.id);
    headers.set('X-Tenant-Id', user.tenantId);
    headers.set('X-User-Roles', JSON.stringify(user.roles));

    // Route to tenant DO
    const doId = env.DOSQL_DO.idFromName(`tenant:${user.tenantId}`);
    return env.DOSQL_DO.get(doId).fetch(new Request(request, { headers }));
  }
};
```

### 4.2 API Key Authentication

For service-to-service communication:

```typescript
interface APIKey {
  id: string;
  hashedKey: string;    // SHA-256 hash - never store plaintext
  tenantId: string;
  scopes: string[];
  rateLimit: number;
  expiresAt?: number;
  createdAt: number;
}

// Hash using Web Crypto API
async function hashKey(key: string): Promise<string> {
  const data = new TextEncoder().encode(key);
  const hash = await crypto.subtle.digest('SHA-256', data);
  return btoa(String.fromCharCode(...new Uint8Array(hash)));
}

// Generate new API key - returns plaintext ONCE
async function generateAPIKey(
  tenantId: string,
  scopes: string[],
  env: Env
): Promise<{ key: string; id: string }> {
  const key = `dosql_${tenantId}_${crypto.randomUUID().replace(/-/g, '')}`;
  const hashedKey = await hashKey(key);
  const id = crypto.randomUUID();

  const apiKey: APIKey = {
    id,
    hashedKey,
    tenantId,
    scopes,
    rateLimit: 1000,
    createdAt: Date.now(),
  };

  await Promise.all([
    env.API_KEYS.put(`apikey:${id}`, JSON.stringify(apiKey)),
    env.API_KEYS.put(`hash:${hashedKey}`, id),
  ]);

  return { key, id };  // Plaintext visible only once
}

// Validate API key
async function validateAPIKey(key: string, env: Env): Promise<APIKey | null> {
  const hashedKey = await hashKey(key);
  const keyId = await env.API_KEYS.get(`hash:${hashedKey}`);
  if (!keyId) return null;

  const apiKeyJson = await env.API_KEYS.get(`apikey:${keyId}`);
  if (!apiKeyJson) return null;

  const apiKey: APIKey = JSON.parse(apiKeyJson);

  if (apiKey.expiresAt && apiKey.expiresAt < Date.now()) {
    return null;
  }

  return apiKey;
}
```

### 4.3 WebSocket Authentication

Authenticate before upgrading the connection:

```typescript
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (request.headers.get('upgrade')?.toLowerCase() === 'websocket') {
      // Authenticate BEFORE upgrade
      const user = await authenticateRequest(request, env);
      if (!user) {
        return new Response('Unauthorized', { status: 401 });
      }

      // Validate origin (CSRF protection)
      const origin = request.headers.get('Origin');
      if (!isAllowedOrigin(origin, env.ALLOWED_ORIGINS)) {
        return new Response('Forbidden', { status: 403 });
      }

      // Route to tenant DO
      const doId = env.DOSQL_DO.idFromName(`tenant:${user.tenantId}`);
      return env.DOSQL_DO.get(doId).fetch(request);
    }

    // HTTP handling...
  }
};

function isAllowedOrigin(origin: string | null, allowed: string[]): boolean {
  if (!origin) return false;
  return allowed.some(pattern => {
    if (pattern.startsWith('*.')) {
      return new URL(origin).hostname.endsWith(pattern.slice(2));
    }
    return origin === pattern;
  });
}
```

---

## 5. Authorization and Access Control

### 5.1 Role-Based Access Control

Implement RBAC in your Durable Object:

```typescript
class DoSQLDurableObject implements DurableObject {
  private readonly PERMISSIONS: Record<string, string[]> = {
    // Query operations
    query: ['read', 'write', 'admin'],
    prepare: ['read', 'write', 'admin'],
    batch: ['write', 'admin'],

    // Transaction operations
    beginTransaction: ['write', 'admin'],
    commit: ['write', 'admin'],
    rollback: ['write', 'admin'],

    // Admin operations
    dropTable: ['admin'],
    truncate: ['admin'],
    vacuum: ['admin'],
  };

  async fetch(request: Request): Promise<Response> {
    const roles: string[] = JSON.parse(
      request.headers.get('X-User-Roles') || '[]'
    );

    const { method } = await request.json() as { method: string };

    if (!this.isAuthorized(method, roles)) {
      return Response.json({
        error: 'Forbidden',
        code: 'INSUFFICIENT_PERMISSIONS',
        required: this.PERMISSIONS[method] || ['admin'],
      }, { status: 403 });
    }

    // Process authorized request...
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
// Pattern 1: User can only access their own records
async function getUserOrders(userId: string, user: AuthUser): Promise<Order[]> {
  if (userId !== user.id && !user.roles.includes('admin')) {
    throw new ForbiddenError('Cannot access other user orders');
  }
  return db.prepare('SELECT * FROM orders WHERE user_id = ?').all(userId);
}

// Pattern 2: Query builder with automatic filtering
class SecureQueryBuilder {
  constructor(private user: AuthUser) {}

  selectFrom(table: string): PreparedQuery {
    if (this.user.roles.includes('admin')) {
      return db.prepare(`SELECT * FROM ${table}`);
    }
    return db.prepare(`SELECT * FROM ${table} WHERE user_id = ?`)
      .bind(this.user.id);
  }
}
```

### 5.3 Operation-Level Permissions

Control which SQL operations are allowed:

```typescript
type Operation = 'select' | 'insert' | 'update' | 'delete' | 'ddl';

const ROLE_PERMISSIONS: Record<string, Record<Operation, boolean>> = {
  read: { select: true, insert: false, update: false, delete: false, ddl: false },
  write: { select: true, insert: true, update: true, delete: true, ddl: false },
  admin: { select: true, insert: true, update: true, delete: true, ddl: true },
};

function detectOperation(sql: string): Operation {
  const cmd = sql.trim().toUpperCase().split(/\s+/)[0];
  const ops: Record<string, Operation> = {
    SELECT: 'select', INSERT: 'insert', UPDATE: 'update',
    DELETE: 'delete', CREATE: 'ddl', ALTER: 'ddl', DROP: 'ddl',
  };
  return ops[cmd] || 'select';
}

function canExecute(sql: string, roles: string[]): boolean {
  const op = detectOperation(sql);
  return roles.some(role => ROLE_PERMISSIONS[role]?.[op]);
}
```

---

## 6. Data Protection

### 6.1 Encryption at Rest

Cloudflare provides automatic AES-256 encryption for all stored data:

| Layer | Encryption | Key Management |
|-------|------------|----------------|
| DO Storage | AES-256 | Cloudflare managed |
| R2 Storage | AES-256 | Cloudflare managed |
| Network | TLS 1.3 | Cloudflare managed |

### 6.2 Application-Level Encryption

For highly sensitive fields (PII, financial data), add application-level encryption:

```typescript
class FieldEncryptor {
  private keyPromise: Promise<CryptoKey>;

  constructor(keyBase64: string) {
    this.keyPromise = crypto.subtle.importKey(
      'raw',
      Uint8Array.from(atob(keyBase64), c => c.charCodeAt(0)),
      { name: 'AES-GCM' },
      false,
      ['encrypt', 'decrypt']
    );
  }

  async encrypt(plaintext: string): Promise<{ ciphertext: string; iv: string }> {
    const key = await this.keyPromise;
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

  async decrypt(ciphertextBase64: string, ivBase64: string): Promise<string> {
    const key = await this.keyPromise;
    const iv = Uint8Array.from(atob(ivBase64), c => c.charCodeAt(0));
    const ciphertext = Uint8Array.from(atob(ciphertextBase64), c => c.charCodeAt(0));

    const plaintext = await crypto.subtle.decrypt(
      { name: 'AES-GCM', iv },
      key,
      ciphertext
    );

    return new TextDecoder().decode(plaintext);
  }
}

// Usage
const encryptor = new FieldEncryptor(env.FIELD_ENCRYPTION_KEY);

async function storeSSN(userId: string, ssn: string): Promise<void> {
  const { ciphertext, iv } = await encryptor.encrypt(ssn);
  db.prepare('UPDATE users SET ssn_encrypted = ?, ssn_iv = ? WHERE id = ?')
    .run(ciphertext, iv, userId);
}
```

### 6.3 Sensitive Data Handling

```typescript
// Redact sensitive fields from logs
const SENSITIVE_PATTERNS = [
  /password/i, /secret/i, /token/i, /key/i,
  /ssn/i, /credit.?card/i, /cvv/i, /pin/i,
];

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

// Data masking for display
const mask = {
  ssn: (ssn: string) => `***-**-${ssn.slice(-4)}`,
  email: (email: string) => {
    const [local, domain] = email.split('@');
    return domain ? `${local.slice(0, 2)}***@${domain}` : '***@***';
  },
  creditCard: (num: string) => `****-****-****-${num.replace(/\D/g, '').slice(-4)}`,
  phone: (phone: string) => `(***) ***-${phone.replace(/\D/g, '').slice(-4)}`,
};
```

---

## 7. Network Security

### 7.1 Transport Layer Security

Cloudflare enforces TLS for all edge traffic. Configure your zone for maximum security:

```bash
# Set minimum TLS version to 1.2
curl -X PATCH "https://api.cloudflare.com/client/v4/zones/{zone_id}/settings/min_tls_version" \
  -H "Authorization: Bearer {api_token}" \
  -H "Content-Type: application/json" \
  --data '{"value":"1.2"}'

# Enable TLS 1.3
curl -X PATCH "https://api.cloudflare.com/client/v4/zones/{zone_id}/settings/tls_1_3" \
  -H "Authorization: Bearer {api_token}" \
  -H "Content-Type: application/json" \
  --data '{"value":"on"}'
```

### 7.2 Security Headers

Add security headers to all responses:

```typescript
function addSecurityHeaders(response: Response): Response {
  const headers = new Headers(response.headers);

  // HSTS - enforce HTTPS for 1 year
  headers.set('Strict-Transport-Security', 'max-age=31536000; includeSubDomains; preload');

  // Prevent MIME sniffing
  headers.set('X-Content-Type-Options', 'nosniff');

  // Prevent framing (clickjacking)
  headers.set('X-Frame-Options', 'DENY');

  // Content Security Policy
  headers.set('Content-Security-Policy', "default-src 'self'; frame-ancestors 'none'");

  // Referrer Policy
  headers.set('Referrer-Policy', 'strict-origin-when-cross-origin');

  // Permissions Policy
  headers.set('Permissions-Policy', 'geolocation=(), microphone=(), camera=()');

  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}
```

### 7.3 Rate Limiting

```typescript
class RateLimiter {
  private requests = new Map<string, number[]>();

  constructor(
    private windowMs: number,
    private maxRequests: number
  ) {}

  check(key: string): { allowed: boolean; remaining: number; resetAt: number } {
    const now = Date.now();
    const windowStart = now - this.windowMs;

    let timestamps = this.requests.get(key) || [];
    timestamps = timestamps.filter(t => t > windowStart);

    const remaining = Math.max(0, this.maxRequests - timestamps.length);
    const resetAt = timestamps[0] ? timestamps[0] + this.windowMs : now + this.windowMs;

    if (timestamps.length >= this.maxRequests) {
      return { allowed: false, remaining: 0, resetAt };
    }

    timestamps.push(now);
    this.requests.set(key, timestamps);

    return { allowed: true, remaining: remaining - 1, resetAt };
  }
}

// Usage
const limiter = new RateLimiter(60000, 100);  // 100 req/min

async function handleRequest(request: Request): Promise<Response> {
  const ip = request.headers.get('CF-Connecting-IP') || 'unknown';
  const result = limiter.check(ip);

  if (!result.allowed) {
    return new Response(JSON.stringify({ error: 'Rate limit exceeded' }), {
      status: 429,
      headers: {
        'Content-Type': 'application/json',
        'Retry-After': String(Math.ceil((result.resetAt - Date.now()) / 1000)),
        'X-RateLimit-Remaining': '0',
        'X-RateLimit-Reset': String(Math.ceil(result.resetAt / 1000)),
      },
    });
  }

  // Process request...
}
```

### 7.4 Request Validation

```typescript
interface RequestLimits {
  maxBodySize: number;
  maxBatchSize: number;
  maxQueryLength: number;
  maxParameterCount: number;
}

const LIMITS: RequestLimits = {
  maxBodySize: 1024 * 1024,    // 1MB
  maxBatchSize: 100,
  maxQueryLength: 10000,
  maxParameterCount: 1000,
};

async function validateRequest(
  request: Request
): Promise<{ valid: true; body: unknown } | { valid: false; error: string }> {
  const contentLength = request.headers.get('Content-Length');
  if (contentLength && parseInt(contentLength) > LIMITS.maxBodySize) {
    return { valid: false, error: 'Request body too large' };
  }

  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return { valid: false, error: 'Invalid JSON body' };
  }

  if (Array.isArray(body) && body.length > LIMITS.maxBatchSize) {
    return { valid: false, error: `Batch exceeds limit of ${LIMITS.maxBatchSize}` };
  }

  const queries = Array.isArray(body) ? body : [body];
  for (const q of queries) {
    if (typeof q.sql === 'string' && q.sql.length > LIMITS.maxQueryLength) {
      return { valid: false, error: 'Query too long' };
    }
    if (Array.isArray(q.params) && q.params.length > LIMITS.maxParameterCount) {
      return { valid: false, error: 'Too many parameters' };
    }
  }

  return { valid: true, body };
}
```

---

## 8. Configuration Reference

### 8.1 Database Security Options

```typescript
interface DatabaseSecurityOptions {
  /** Max query execution time (ms). Default: 5000 */
  queryTimeout?: number;

  /** Max rows per query. Default: 10000 */
  maxResultRows?: number;

  /** Allow DDL (CREATE/ALTER/DROP). Default: true */
  allowDDL?: boolean;

  /** Allow ATTACH DATABASE. Default: false */
  allowAttach?: boolean;

  /** Max tables per database. Default: 1000 */
  maxTables?: number;

  /** Log queries exceeding threshold. Default: true */
  logSlowQueries?: boolean;

  /** Slow query threshold (ms). Default: 1000 */
  slowQueryThreshold?: number;
}

// Production configuration
const productionConfig: DatabaseSecurityOptions = {
  queryTimeout: 3000,
  maxResultRows: 5000,
  allowDDL: false,           // Disable DDL in production
  allowAttach: false,
  maxTables: 100,
  logSlowQueries: true,
  slowQueryThreshold: 500,
};

const db = createDatabase(':memory:', productionConfig);
```

### 8.2 RPC Security Configuration

```typescript
interface RPCSecurityConfig {
  /** Max request body (bytes). Default: 1MB */
  maxRequestSize: number;

  /** Max queries per batch. Default: 100 */
  maxBatchSize: number;

  /** Request timeout (ms). Default: 30000 */
  requestTimeout: number;

  /** CORS allowed origins. Never use '*' in production */
  allowedOrigins: string[];

  /** CORS max age (seconds). Default: 86400 */
  corsMaxAge: number;
}

const rpcConfig: RPCSecurityConfig = {
  maxRequestSize: 512 * 1024,
  maxBatchSize: 50,
  requestTimeout: 10000,
  allowedOrigins: ['https://app.example.com', 'https://admin.example.com'],
  corsMaxAge: 86400,
};
```

### 8.3 Secrets Management

Store secrets using Cloudflare Secrets, never in code or environment variables:

```bash
# Set secrets
wrangler secret put JWT_SECRET
wrangler secret put ENCRYPTION_KEY
wrangler secret put FIELD_ENCRYPTION_KEY

# List secrets (values hidden)
wrangler secret list

# Delete secret
wrangler secret delete OLD_SECRET
```

**Access in Worker:**

```typescript
export interface Env {
  // Secrets (from wrangler secret put)
  JWT_SECRET: string;
  ENCRYPTION_KEY: string;

  // Bindings
  DOSQL_DO: DurableObjectNamespace;
  API_KEYS: KVNamespace;
  AUDIT_LOGS: R2Bucket;
}

// Secrets are available on env, automatically redacted from logs
async function handler(request: Request, env: Env) {
  const isValid = await verifyJWT(token, env.JWT_SECRET);
  // NEVER: console.log(env.JWT_SECRET);
}
```

### 8.4 Environment Configuration

**wrangler.toml:**

```toml
name = "dosql-app"
main = "src/index.ts"
compatibility_date = "2024-01-01"

[env.production]
vars = { ENVIRONMENT = "production", LOG_LEVEL = "error" }

[env.staging]
vars = { ENVIRONMENT = "staging", LOG_LEVEL = "info" }

[env.development]
vars = { ENVIRONMENT = "development", LOG_LEVEL = "debug" }
```

**Per-environment security:**

```typescript
function getSecurityConfig(env: Env) {
  const isProduction = env.ENVIRONMENT === 'production';

  return {
    queryTimeout: isProduction ? 3000 : 30000,
    maxResultRows: isProduction ? 5000 : 100000,
    allowDDL: !isProduction,
    allowedOrigins: isProduction
      ? ['https://app.example.com']
      : ['http://localhost:3000'],
    rateLimit: { windowMs: 60000, max: isProduction ? 100 : 1000 },
  };
}
```

---

## 9. Audit and Compliance

### 9.1 Audit Events

```typescript
const AUDIT_EVENTS = {
  // Authentication (always log)
  AUTH_SUCCESS: 'auth.success',
  AUTH_FAILURE: 'auth.failure',
  TOKEN_REVOKED: 'auth.token_revoked',

  // Authorization (log failures)
  AUTHZ_DENIED: 'authz.denied',

  // Data operations
  QUERY_READ: 'query.read',
  QUERY_WRITE: 'query.write',
  QUERY_DDL: 'query.ddl',

  // Admin operations (always log)
  SCHEMA_CHANGE: 'admin.schema_change',
  USER_CREATE: 'admin.user_create',
  PERMISSION_CHANGE: 'admin.permission_change',
  API_KEY_CREATE: 'admin.api_key_create',
  API_KEY_REVOKE: 'admin.api_key_revoke',

  // Security events (always log)
  RATE_LIMIT_EXCEEDED: 'security.rate_limit',
  SUSPICIOUS_ACTIVITY: 'security.suspicious',
} as const;
```

### 9.2 Audit Log Implementation

```typescript
interface AuditEvent {
  id: string;
  timestamp: string;
  eventType: string;
  severity: 'info' | 'warning' | 'error' | 'critical';
  userId: string | null;
  tenantId: string;
  action: string;
  resource?: string;
  details?: Record<string, unknown>;
  requestId: string;
  success: boolean;
  duration?: number;
}

class AuditLogger {
  private queue: AuditEvent[] = [];

  constructor(
    private storage: R2Bucket,
    private tenantId: string
  ) {}

  log(event: Omit<AuditEvent, 'id' | 'timestamp' | 'tenantId'>): void {
    this.queue.push({
      ...event,
      id: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
      tenantId: this.tenantId,
    });

    // Flush immediately for security-critical events
    if (this.isSecurityCritical(event.eventType)) {
      this.flush();
    }
  }

  private isSecurityCritical(eventType: string): boolean {
    return ['auth.failure', 'authz.denied', 'security.suspicious',
            'admin.permission_change', 'admin.api_key_revoke'].includes(eventType);
  }

  async flush(): Promise<void> {
    if (this.queue.length === 0) return;

    const events = this.queue;
    this.queue = [];

    const now = new Date();
    const path = `audit-logs/${this.tenantId}/${now.toISOString().slice(0, 10)}/${now.getTime()}.jsonl`;
    const content = events.map(e => JSON.stringify(e)).join('\n');

    await this.storage.put(path, content);
  }
}
```

### 9.3 Compliance Considerations

| Requirement | DoSQL Provides | You Implement |
|-------------|----------------|---------------|
| **SOC 2** | Encryption, isolation | Audit logs (90+ days), access controls |
| **GDPR** | Data isolation | Right to erasure, consent tracking |
| **HIPAA** | Encryption, access controls | BAA with CF, audit trails |
| **PCI DSS** | Encryption | Tokenization, access logging |

**Data Retention:**

```typescript
const RETENTION: Record<string, { auditLogs: number; backups: number }> = {
  soc2: { auditLogs: 90, backups: 90 },
  hipaa: { auditLogs: 2190, backups: 365 },  // 6 years
  gdpr: { auditLogs: 30, backups: 30 },
  pciDss: { auditLogs: 365, backups: 365 },
};
```

---

## 10. Security Checklist

### 10.1 Pre-Deployment

**Authentication and Authorization**
- [ ] JWT signature verification implemented
- [ ] Token expiration checked
- [ ] API keys hashed (SHA-256) before storage
- [ ] RBAC implemented for all operations
- [ ] Failed auth attempts logged and rate limited

**SQL Injection Prevention**
- [ ] All queries use parameterized statements
- [ ] No string concatenation in SQL
- [ ] Dynamic identifiers validated against allow-lists
- [ ] Input validation with schema library

**Tenant Isolation**
- [ ] Each tenant has dedicated Durable Object
- [ ] Tenant ID from verified token only, never user input
- [ ] Cross-tenant access tested and blocked

**Network Security**
- [ ] HTTPS enforced
- [ ] HSTS headers with preload
- [ ] CORS configured (specific origins, not `*`)
- [ ] Rate limiting implemented
- [ ] Security headers added

**Data Protection**
- [ ] Sensitive fields encrypted at application level
- [ ] Secrets in Cloudflare Secrets
- [ ] Sensitive data redacted from logs

**Monitoring**
- [ ] Audit logging for auth events
- [ ] Audit logging for admin operations
- [ ] Security event alerting configured

### 10.2 Regular Reviews

| Frequency | Tasks |
|-----------|-------|
| Weekly | Review failed auth logs, check rate limiting |
| Monthly | Rotate service API keys, review permissions |
| Quarterly | Dependency audit (`npm audit`), penetration testing |
| Annually | Full security review, key rotation, incident drill |

---

## 11. Incident Response

### 11.1 Emergency Procedures

```typescript
// Revoke compromised API key immediately
async function revokeAPIKeyEmergency(keyId: string, env: Env): Promise<void> {
  const apiKeyJson = await env.API_KEYS.get(`apikey:${keyId}`);
  if (!apiKeyJson) return;

  const apiKey: APIKey = JSON.parse(apiKeyJson);
  await Promise.all([
    env.API_KEYS.delete(`hash:${apiKey.hashedKey}`),
    env.API_KEYS.delete(`apikey:${keyId}`),
  ]);
}

// Invalidate all sessions for compromised user
async function invalidateUserSessions(userId: string, db: Database): Promise<void> {
  db.prepare('DELETE FROM user_sessions WHERE user_id = ?').run(userId);
  db.prepare('DELETE FROM refresh_tokens WHERE user_id = ?').run(userId);
}

// Enable maintenance mode (read-only)
async function enableMaintenanceMode(env: Env): Promise<void> {
  await env.KV.put('maintenance_mode', JSON.stringify({
    enabled: true,
    reason: 'Security incident',
    startedAt: new Date().toISOString(),
  }));
}
```

### 11.2 Response Process

```
1. DETECT
   - Monitor for anomalies
   - Alert on failed auth spikes (>10/min per user)
   - Alert on rate limit triggers

2. CONTAIN (0-15 min)
   [ ] Disable compromised credentials
   [ ] Block suspicious IPs
   [ ] Enable maintenance mode if severe
   [ ] Preserve logs

3. INVESTIGATE (1-4 hours)
   [ ] Review audit logs
   [ ] Determine scope
   [ ] Identify attack vector
   [ ] Identify affected data

4. REMEDIATE (4-24 hours)
   [ ] Patch vulnerability
   [ ] Rotate credentials
   [ ] Deploy fixes

5. RECOVER (Day 2+)
   [ ] Restore operations
   [ ] Notify affected users
   [ ] Monitor for recurrence

6. DOCUMENT (Week 1)
   [ ] Incident report
   [ ] Root cause analysis
   [ ] Update procedures
```

**Severity Levels:**

| Level | Description | Response Time |
|-------|-------------|---------------|
| P1 | Active breach | Immediate |
| P2 | Attempted breach | < 1 hour |
| P3 | Misconfiguration | < 24 hours |
| P4 | Improvement opportunity | < 1 week |

---

## 12. References

### Cloudflare Documentation
- [Durable Objects Security Model](https://developers.cloudflare.com/durable-objects/reference/security-model/)
- [Workers Security](https://developers.cloudflare.com/workers/platform/security/)
- [R2 Data Security](https://developers.cloudflare.com/r2/reference/data-security/)
- [Secrets Management](https://developers.cloudflare.com/workers/configuration/secrets/)

### Security Standards
- [OWASP SQL Injection Prevention](https://cheatsheetseries.owasp.org/cheatsheets/SQL_Injection_Prevention_Cheat_Sheet.html)
- [OWASP Authentication Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html)
- [CWE-89: SQL Injection](https://cwe.mitre.org/data/definitions/89.html)

### DoSQL Documentation
- [Architecture](./architecture.md)
- [API Reference](./api-reference.md)
- [Deployment](./DEPLOYMENT.md)
- [Troubleshooting](./TROUBLESHOOTING.md)

---

**Security Contact:** security@dotdo.ai

Report vulnerabilities with:
- Description of the issue
- Steps to reproduce
- Impact assessment
- Suggested remediation

We acknowledge reports within 24 hours and provide initial assessment within 72 hours.

---

*DoSQL Security Model v3.1 - Last updated 2026-01-23*
