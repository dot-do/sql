# DoSQL Security Model

**Version:** 3.0
**Last Updated:** 2026-01-23
**Applies To:** DoSQL Edge Database for Cloudflare Workers

---

## Executive Summary

DoSQL is a SQL database engine designed for Cloudflare Workers and Durable Objects. This guide documents the security model, explains what DoSQL protects against, and provides best practices for secure deployment.

**Key Security Properties:**

| Property | Protection Level | Mechanism |
|----------|------------------|-----------|
| Tenant Isolation | Strong | Each tenant operates in a dedicated Durable Object with isolated storage |
| SQL Injection Prevention | Strong | State-aware tokenizer and parameterized query support |
| Transport Security | Strong | All traffic encrypted via Cloudflare-managed TLS |
| Defense in Depth | Comprehensive | Multiple security layers from network to application |

---

## Table of Contents

1. [Security Boundaries](#1-security-boundaries)
   - [Architecture Overview](#11-architecture-overview)
   - [Trust Boundary Definitions](#12-trust-boundary-definitions)
   - [Threat Model](#13-threat-model)
   - [Your Responsibilities](#14-your-responsibilities)
2. [SQL Injection Prevention](#2-sql-injection-prevention)
   - [Parameterized Queries](#21-parameterized-queries-primary-defense)
   - [State-Aware Tokenizer](#22-state-aware-tokenizer-defense-in-depth)
   - [Dynamic SQL Patterns](#23-dynamic-sql-patterns)
   - [Input Validation](#24-input-validation)
3. [Tenant Isolation Model](#3-tenant-isolation-model)
   - [One Durable Object Per Tenant](#31-one-durable-object-per-tenant)
   - [Isolation Guarantees](#32-isolation-guarantees)
   - [Anti-Patterns to Avoid](#33-anti-patterns-to-avoid)
4. [Authentication Patterns](#4-authentication-patterns)
   - [Worker-Level Authentication](#41-worker-level-authentication)
   - [API Key Authentication](#42-api-key-authentication)
   - [WebSocket Authentication](#43-websocket-authentication)
5. [Authorization and Access Control](#5-authorization-and-access-control)
   - [Role-Based Access Control](#51-role-based-access-control-rbac)
   - [Row-Level Security](#52-row-level-security-patterns)
   - [Operation-Level Permissions](#53-operation-level-permissions)
6. [Data Protection](#6-data-protection)
   - [Encryption at Rest](#61-encryption-at-rest)
   - [Application-Level Encryption](#62-application-level-encryption)
   - [Sensitive Data Handling](#63-sensitive-data-handling)
7. [Network Security](#7-network-security)
   - [Transport Layer Security](#71-transport-layer-security)
   - [Security Headers](#72-security-headers)
   - [Rate Limiting](#73-rate-limiting)
   - [Request Validation](#74-request-validation)
8. [Configuration Options](#8-configuration-options)
   - [Database Security Options](#81-database-security-options)
   - [RPC Security Configuration](#82-rpc-security-configuration)
   - [Secrets Management](#83-secrets-management)
   - [Environment Configuration](#84-environment-configuration)
9. [Audit and Compliance](#9-audit-and-compliance)
   - [Audit Events](#91-audit-events-to-capture)
   - [Audit Log Implementation](#92-audit-log-implementation)
   - [Compliance Considerations](#93-compliance-considerations)
10. [Security Checklist](#10-security-checklist)
    - [Pre-Deployment Checklist](#101-pre-deployment-checklist)
    - [Regular Security Reviews](#102-regular-security-reviews)
11. [Incident Response](#11-incident-response)
    - [Emergency Procedures](#111-emergency-procedures)
    - [Incident Response Steps](#112-incident-response-steps)
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

**Security is enforced at multiple layers:**

1. **Network Edge**: Cloudflare's global network provides DDoS protection, TLS termination, and WAF capabilities
2. **Worker Entry**: Your code validates authentication, enforces rate limits, and validates input
3. **Durable Object**: DoSQL enforces tenant isolation and query safety
4. **Storage**: R2 provides encryption at rest with access controlled via bindings

### 1.2 Trust Boundary Definitions

| Boundary | Location | Security Controls | Your Responsibility |
|----------|----------|-------------------|---------------------|
| **External** | Internet to Cloudflare Edge | TLS encryption, DDoS protection, WAF | Configure WAF rules |
| **Worker Entry** | Cloudflare to your Worker | Authentication, input validation, rate limiting | Implement all controls |
| **DO Boundary** | Worker to Durable Object | Tenant routing, authorization | Derive tenant ID from auth token |
| **Storage** | DO to R2 | Access control via binding, encryption at rest | None (managed by Cloudflare) |

### 1.3 Threat Model

#### What DoSQL Protects Against

| Threat | Protection Level | Mechanism | Notes |
|--------|------------------|-----------|-------|
| **SQL Injection** | Strong | Parameterized queries, state-aware tokenizer | Always use `?` placeholders |
| **Cross-Tenant Access** | Strong | DO isolation (1 DO per tenant) | Each tenant has isolated storage |
| **Man-in-the-Middle** | Strong | TLS enforced by Cloudflare | Automatic for all edge traffic |
| **Data at Rest Exposure** | Strong | Cloudflare R2 encryption | AES-256 server-side encryption |
| **DoS via Query Complexity** | Configurable | Query timeouts, statement limits | Configure based on workload |
| **Memory Exhaustion** | Configurable | Result row limits, batch size limits | Set appropriate limits |

#### What Requires Your Implementation

| Threat | Your Responsibility | Recommended Approach |
|--------|---------------------|----------------------|
| **Unauthorized Access** | Authentication at Worker layer | JWT verification, API key validation |
| **Privilege Escalation** | RBAC implementation | Role-based permissions per operation |
| **Credential Stuffing** | Failed auth limiting | Rate limit + lockout policies |
| **Data Exfiltration** | Audit logging | Log all data access patterns |
| **Insider Threats** | Access controls, audit trails | Least privilege, comprehensive logging |

### 1.4 Your Responsibilities

DoSQL provides the database engine, but you must implement these security controls:

```typescript
// Security checklist - implement each of these:

// 1. Authentication - validate user identity
const user = await authenticateRequest(request, env);
if (!user) return new Response('Unauthorized', { status: 401 });

// 2. Authorization - enforce role-based access
if (!user.roles.includes(requiredRole)) {
  return new Response('Forbidden', { status: 403 });
}

// 3. Tenant Isolation - derive tenant from auth, never from user input
const tenantId = user.tenantId;  // From verified JWT claim
const doId = env.DOSQL_DO.idFromName(`tenant:${tenantId}`);

// 4. Input Validation - validate before querying
const validated = schema.safeParse(input);
if (!validated.success) return new Response('Bad Request', { status: 400 });

// 5. Audit Logging - capture security-relevant events
await auditLog.log({ action: 'query', userId: user.id, resource: table });
```

---

## 2. SQL Injection Prevention

SQL injection is the most critical database vulnerability. DoSQL provides multiple layers of protection.

### 2.1 Parameterized Queries (Primary Defense)

**Always use parameter placeholders.** This is the most important security practice.

```typescript
import { createDatabase } from '@dotdo/dosql';

const db = createDatabase();

// CORRECT: Positional parameters with ?
const userId = request.params.id;  // User input
const user = db.prepare('SELECT * FROM users WHERE id = ?').get(userId);

// CORRECT: Named parameters with :name
const stmt = db.prepare('SELECT * FROM users WHERE email = :email');
const user = stmt.get({ email: userEmail });

// CORRECT: Multiple parameters
const stmt = db.prepare(`
  INSERT INTO orders (user_id, product_id, quantity)
  VALUES (?, ?, ?)
`);
stmt.run(userId, productId, quantity);

// CORRECT: IN clause with array expansion
const ids = [1, 2, 3];
const placeholders = ids.map(() => '?').join(', ');
const stmt = db.prepare(`SELECT * FROM users WHERE id IN (${placeholders})`);
const users = stmt.all(...ids);
```

**Never do this:**

```typescript
// CRITICAL VULNERABILITY: String interpolation
const userId = request.params.id;  // Could be: "1; DROP TABLE users; --"
const user = db.prepare(`SELECT * FROM users WHERE id = ${userId}`).get();
//                                                      ^^^^^^^^^^^
//                                                      INJECTION RISK

// CRITICAL VULNERABILITY: String concatenation
const searchTerm = request.query.search;
const sql = "SELECT * FROM products WHERE name LIKE '%" + searchTerm + "%'";
//                                                       ^^^^^^^^^^
//                                                       INJECTION RISK

// CRITICAL VULNERABILITY: Template literals without parameters
const user = db.prepare(`SELECT * FROM users WHERE email = '${email}'`).get();
//                                                          ^^^^^^^^
//                                                          INJECTION RISK
```

### 2.2 State-Aware Tokenizer (Defense-in-Depth)

When using `exec()` for multi-statement execution, DoSQL's tokenizer properly handles edge cases that naive semicolon splitting would mishandle:

```typescript
// The tokenizer correctly handles these cases:

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

| State | Context | Semicolon Behavior |
|-------|---------|-------------------|
| `normal` | Regular SQL parsing | Splits statements |
| `single_quote` | Inside `'string'` | Literal character |
| `double_quote` | Inside `"identifier"` | Literal character |
| `backtick` | Inside `` `identifier` `` | Literal character |
| `block_comment` | Inside `/* comment */` | Literal character |
| `line_comment` | After `--` until newline | Literal character |

**Important**: The tokenizer is defense-in-depth. It cannot prevent injection attacks that work at the SQL level. Always use parameterized queries as your primary defense.

### 2.3 Dynamic SQL Patterns

Sometimes you need dynamic table or column names. Use allow-lists:

```typescript
// SAFE: Allow-list for table names
const ALLOWED_TABLES = new Set(['users', 'orders', 'products']);

function validateTableName(name: string): string {
  if (!ALLOWED_TABLES.has(name)) {
    throw new Error('Invalid table name');
  }
  return name;
}

const table = validateTableName(request.query.table);
const stmt = db.prepare(`SELECT * FROM ${table} WHERE id = ?`);
const result = stmt.get(id);

// SAFE: Map user input to allowed columns
const SORT_COLUMNS: Record<string, string> = {
  'name': 'name',
  'date': 'created_at',
  'email': 'email',
};

const sortColumn = SORT_COLUMNS[request.query.sort];
if (!sortColumn) {
  return new Response('Invalid sort column', { status: 400 });
}
const sql = `SELECT * FROM users ORDER BY ${sortColumn}`;

// SAFE: Allow-list for sort direction
const SORT_DIRECTIONS = new Set(['ASC', 'DESC']);
const direction = SORT_DIRECTIONS.has(request.query.dir?.toUpperCase())
  ? request.query.dir.toUpperCase()
  : 'ASC';
const sql = `SELECT * FROM users ORDER BY ${sortColumn} ${direction}`;
```

### 2.4 Input Validation

Validate inputs before they reach your queries:

```typescript
import { z } from 'zod';

// Define strict schemas
const UserIdSchema = z.string().uuid();
const EmailSchema = z.string().email().max(255);
const PageSchema = z.number().int().positive().max(1000);
const LimitSchema = z.number().int().min(1).max(100).default(20);

// Search input with sanitization
const SearchSchema = z.object({
  query: z.string().min(1).max(100).transform(s => s.trim()),
  page: PageSchema.optional().default(1),
  limit: LimitSchema,
});

async function searchUsers(request: Request): Promise<Response> {
  const params = await request.json();

  // Validate input
  const result = SearchSchema.safeParse(params);
  if (!result.success) {
    return new Response(
      JSON.stringify({
        error: 'Invalid input',
        details: result.error.flatten(),
      }),
      { status: 400 }
    );
  }

  const { query, page, limit } = result.data;
  const offset = (page - 1) * limit;

  // Safe to use in parameterized query
  const users = db.prepare(`
    SELECT id, name, email FROM users
    WHERE name LIKE ?
    LIMIT ? OFFSET ?
  `).all(`%${query}%`, limit, offset);

  return new Response(JSON.stringify(users));
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
    // 1. Authenticate and get tenant ID from verified token
    const user = await authenticateRequest(request, env);
    if (!user) {
      return new Response('Unauthorized', { status: 401 });
    }

    // 2. Route to tenant-specific Durable Object
    //    Each tenant has completely isolated storage
    const doId = env.DOSQL_DO.idFromName(`tenant:${user.tenantId}`);
    const stub = env.DOSQL_DO.get(doId);

    // 3. Forward authenticated request
    return stub.fetch(request);
  }
};
```

### 3.2 Isolation Guarantees

| Property | Guarantee | Enforcement |
|----------|-----------|-------------|
| **Storage Isolation** | Each DO has its own SQLite-compatible storage | Cloudflare platform |
| **Memory Isolation** | No shared memory between DO instances | V8 isolate boundaries |
| **Execution Isolation** | Each DO runs in its own V8 isolate | Cloudflare platform |
| **Namespace Isolation** | DO IDs are scoped to your Worker | Cloudflare platform |
| **Network Isolation** | DOs cannot directly communicate | Must go through Worker |

**Why DO-level isolation is superior to row-level filtering:**

```
Row-Level Filtering (WEAK):              DO-Level Isolation (STRONG):

+---------------------------+            +-------------+  +-------------+
|     Shared Database       |            |  Tenant A   |  |  Tenant B   |
|  +-----+-----+-----+      |            |  DO + DB    |  |  DO + DB    |
|  |  A  |  B  |  A  |      |            +-------------+  +-------------+
|  +-----+-----+-----+      |                 |                 |
|  |  B  |  A  |  B  |      |            Isolated       Isolated
|  +-----+-----+-----+      |            Storage        Storage
|                           |
| WHERE tenant_id = ?       |            No tenant_id column needed!
| (Must NEVER forget!)      |            Isolation is structural.
+---------------------------+
```

### 3.3 Anti-Patterns to Avoid

```typescript
// WRONG: Shared DO for multiple tenants
const doId = env.DOSQL_DO.idFromName('shared-database');
// This puts all tenants in the same database - NO ISOLATION

// WRONG: Tenant ID in query instead of DO routing
const result = db.query(
  `SELECT * FROM users WHERE tenant_id = ?`,
  [tenantId]
);
// Bug: if you forget the WHERE clause, you leak data

// WRONG: Trusting tenant ID from user input
const tenantId = request.headers.get('X-Tenant-Id');
const doId = env.DOSQL_DO.idFromName(`tenant:${tenantId}`);
// Attacker can specify any tenant ID in the header!

// WRONG: Trusting tenant ID from URL parameter
const tenantId = new URL(request.url).searchParams.get('tenant');
const doId = env.DOSQL_DO.idFromName(`tenant:${tenantId}`);
// Attacker can specify any tenant ID in the URL!

// CORRECT: Derive tenant ID from verified authentication token
const token = await verifyJWT(request.headers.get('Authorization'), env.JWT_SECRET);
const tenantId = token.tenant_id;  // From verified claim - cannot be forged
const doId = env.DOSQL_DO.idFromName(`tenant:${tenantId}`);
```

---

## 4. Authentication Patterns

### 4.1 Worker-Level Authentication

Authentication must happen at the Worker layer BEFORE requests reach the Durable Object:

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
    // Verify signature
    const isValid = await verify(token, env.JWT_SECRET);
    if (!isValid) return null;

    // Decode payload
    const payload = JSON.parse(atob(token.split('.')[1]));

    // Validate required claims
    if (!payload.sub || !payload.tenant_id) {
      return null;
    }

    // Check expiration
    if (payload.exp && payload.exp < Date.now() / 1000) {
      return null;
    }

    // Check not-before time
    if (payload.nbf && payload.nbf > Date.now() / 1000) {
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
    // 1. Authenticate
    const user = await authenticateRequest(request, env);
    if (!user) {
      return new Response(
        JSON.stringify({ error: 'Unauthorized' }),
        {
          status: 401,
          headers: {
            'Content-Type': 'application/json',
            'WWW-Authenticate': 'Bearer realm="DoSQL API"',
          },
        }
      );
    }

    // 2. Create enriched request with user context
    const headers = new Headers(request.headers);
    headers.set('X-User-Id', user.id);
    headers.set('X-Tenant-Id', user.tenantId);
    headers.set('X-User-Roles', JSON.stringify(user.roles));

    const enrichedRequest = new Request(request, { headers });

    // 3. Route to tenant's Durable Object
    const doId = env.DOSQL_DO.idFromName(`tenant:${user.tenantId}`);
    return env.DOSQL_DO.get(doId).fetch(enrichedRequest);
  }
};
```

### 4.2 API Key Authentication

For service-to-service communication:

```typescript
interface APIKey {
  id: string;
  hashedKey: string;      // SHA-256 hash, NEVER store plaintext
  tenantId: string;
  scopes: string[];       // ['read', 'write', 'admin']
  rateLimit: number;      // Requests per minute
  expiresAt?: number;     // Optional expiration timestamp
  createdAt: number;
  lastUsedAt?: number;
}

// Secure hash function using Web Crypto API
async function hashKey(key: string): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(key);
  const hash = await crypto.subtle.digest('SHA-256', data);
  return btoa(String.fromCharCode(...new Uint8Array(hash)));
}

// Generate a new API key
// Returns plaintext ONCE - store the hash, give plaintext to user
async function generateAPIKey(
  tenantId: string,
  scopes: string[],
  env: Env
): Promise<{ key: string; id: string }> {
  // Secure random key generation
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

  // Store in KV with indices for lookup
  await Promise.all([
    env.API_KEYS.put(`apikey:${id}`, JSON.stringify(apiKey)),
    env.API_KEYS.put(`hash:${hashedKey}`, id),
  ]);

  return { key, id };  // key is only visible this once
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

  // Update last used timestamp (async, don't wait)
  env.API_KEYS.put(`apikey:${keyId}`, JSON.stringify({
    ...apiKey,
    lastUsedAt: Date.now(),
  }));

  return apiKey;
}

// Revoke an API key
async function revokeAPIKey(keyId: string, env: Env): Promise<boolean> {
  const apiKeyJson = await env.API_KEYS.get(`apikey:${keyId}`);
  if (!apiKeyJson) return false;

  const apiKey: APIKey = JSON.parse(apiKeyJson);

  await Promise.all([
    env.API_KEYS.delete(`apikey:${keyId}`),
    env.API_KEYS.delete(`hash:${apiKey.hashedKey}`),
  ]);

  return true;
}
```

### 4.3 WebSocket Authentication

For persistent WebSocket connections:

```typescript
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const upgradeHeader = request.headers.get('upgrade');

    if (upgradeHeader?.toLowerCase() === 'websocket') {
      // 1. Validate authentication BEFORE upgrade
      const user = await authenticateRequest(request, env);
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
      const originUrl = new URL(origin);
      return originUrl.hostname.endsWith(domain);
    }
    return origin === pattern;
  });
}
```

---

## 5. Authorization and Access Control

### 5.1 Role-Based Access Control (RBAC)

Implement RBAC inside your Durable Object:

```typescript
class DoSQLDurableObject implements DurableObject {
  // Define permissions for each operation
  private readonly PERMISSIONS: Record<string, string[]> = {
    // Query operations
    'query': ['read', 'write', 'admin'],
    'prepare': ['read', 'write', 'admin'],
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
    'vacuum': ['admin'],
  };

  async fetch(request: Request): Promise<Response> {
    const userId = request.headers.get('X-User-Id');
    const roles: string[] = JSON.parse(
      request.headers.get('X-User-Roles') || '[]'
    );

    const rpcRequest = await request.json() as { method: string };

    // Check authorization
    if (!this.isAuthorized(rpcRequest.method, roles)) {
      return new Response(
        JSON.stringify({
          error: 'Forbidden',
          code: 'INSUFFICIENT_PERMISSIONS',
          required: this.PERMISSIONS[rpcRequest.method] || ['admin'],
        }),
        {
          status: 403,
          headers: { 'Content-Type': 'application/json' },
        }
      );
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

For fine-grained access control within a tenant:

```typescript
// Pattern 1: Filter by user ID
async function getUserOrders(
  userId: string,
  requestingUser: AuthUser
): Promise<Order[]> {
  // Users can only see their own records
  if (userId !== requestingUser.id) {
    // Unless they have admin role
    if (!requestingUser.roles.includes('admin')) {
      throw new ForbiddenError('Cannot access other users orders');
    }
  }

  return db.prepare('SELECT * FROM orders WHERE user_id = ?').all(userId);
}

// Pattern 2: Query builder with automatic filtering
class SecureQueryBuilder {
  private user: AuthUser;

  constructor(user: AuthUser) {
    this.user = user;
  }

  selectFrom(table: string, columns: string[] = ['*']): PreparedQuery {
    const columnList = columns.join(', ');

    if (this.user.roles.includes('admin')) {
      // Admins see all records
      return db.prepare(`SELECT ${columnList} FROM ${table}`);
    }

    // Regular users see only their records
    return db.prepare(
      `SELECT ${columnList} FROM ${table} WHERE user_id = ?`
    ).bind(this.user.id);
  }
}

// Pattern 3: Data classification with access levels
const DATA_CLASSIFICATION: Record<string, string[]> = {
  'public': ['read', 'write', 'admin'],
  'internal': ['write', 'admin'],
  'confidential': ['admin'],
  'restricted': ['superadmin'],
};

function canAccessData(
  classification: string,
  roles: string[]
): boolean {
  const allowedRoles = DATA_CLASSIFICATION[classification] || ['superadmin'];
  return allowedRoles.some(role => roles.includes(role));
}
```

### 5.3 Operation-Level Permissions

Control which SQL operations are allowed:

```typescript
interface OperationPermissions {
  select: boolean;
  insert: boolean;
  update: boolean;
  delete: boolean;
  ddl: boolean;  // CREATE, ALTER, DROP
}

const ROLE_PERMISSIONS: Record<string, OperationPermissions> = {
  read: {
    select: true,
    insert: false,
    update: false,
    delete: false,
    ddl: false,
  },
  write: {
    select: true,
    insert: true,
    update: true,
    delete: true,
    ddl: false,
  },
  admin: {
    select: true,
    insert: true,
    update: true,
    delete: true,
    ddl: true,
  },
};

function canExecuteOperation(
  operation: keyof OperationPermissions,
  roles: string[]
): boolean {
  return roles.some(role => {
    const perms = ROLE_PERMISSIONS[role];
    return perms && perms[operation];
  });
}

// Detect operation type from SQL
function detectOperation(sql: string): keyof OperationPermissions {
  const trimmed = sql.trim().toUpperCase();
  if (trimmed.startsWith('SELECT')) return 'select';
  if (trimmed.startsWith('INSERT')) return 'insert';
  if (trimmed.startsWith('UPDATE')) return 'update';
  if (trimmed.startsWith('DELETE')) return 'delete';
  if (
    trimmed.startsWith('CREATE') ||
    trimmed.startsWith('ALTER') ||
    trimmed.startsWith('DROP')
  ) {
    return 'ddl';
  }
  return 'select';  // Default to select for PRAGMA, etc.
}
```

---

## 6. Data Protection

### 6.1 Encryption at Rest

Cloudflare R2 provides server-side AES-256 encryption at rest by default for all stored data. No configuration is required.

**Storage encryption summary:**

| Layer | Encryption | Key Management |
|-------|------------|----------------|
| DO Storage | AES-256 | Cloudflare managed |
| R2 Storage | AES-256 | Cloudflare managed |
| Network | TLS 1.3 | Cloudflare managed |

### 6.2 Application-Level Encryption

For highly sensitive fields (PII, financial data), add application-level encryption:

```typescript
// AES-GCM encryption using Web Crypto API
export class FieldEncryptor {
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

// Usage example
const encryptor = new FieldEncryptor(env.FIELD_ENCRYPTION_KEY);

async function storeSSN(userId: string, ssn: string): Promise<void> {
  const { ciphertext, iv } = await encryptor.encrypt(ssn);

  db.prepare(`
    UPDATE users SET ssn_encrypted = ?, ssn_iv = ? WHERE id = ?
  `).run(ciphertext, iv, userId);
}

async function retrieveSSN(userId: string): Promise<string | null> {
  const user = db.prepare(
    'SELECT ssn_encrypted, ssn_iv FROM users WHERE id = ?'
  ).get(userId) as { ssn_encrypted: string; ssn_iv: string } | undefined;

  if (!user?.ssn_encrypted) return null;

  return encryptor.decrypt(user.ssn_encrypted, user.ssn_iv);
}
```

### 6.3 Sensitive Data Handling

```typescript
// Define sensitive field patterns for logging redaction
const SENSITIVE_PATTERNS = [
  /password/i,
  /secret/i,
  /token/i,
  /key/i,
  /ssn/i,
  /social.?security/i,
  /credit.?card/i,
  /card.?number/i,
  /cvv/i,
  /cvc/i,
  /pin/i,
  /account.?number/i,
];

// Redact sensitive fields from objects before logging
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

// Data masking utilities
const mask = {
  ssn(ssn: string): string {
    return `***-**-${ssn.slice(-4)}`;
  },

  email(email: string): string {
    const [local, domain] = email.split('@');
    if (!domain) return '***@***';
    return `${local.slice(0, 2)}***@${domain}`;
  },

  creditCard(number: string): string {
    const digits = number.replace(/\D/g, '');
    return `****-****-****-${digits.slice(-4)}`;
  },

  phone(phone: string): string {
    const digits = phone.replace(/\D/g, '');
    return `(***) ***-${digits.slice(-4)}`;
  },

  // Generic masking - shows first and last character
  partial(value: string, visibleChars: number = 2): string {
    if (value.length <= visibleChars * 2) {
      return '*'.repeat(value.length);
    }
    return (
      value.slice(0, visibleChars) +
      '*'.repeat(value.length - visibleChars * 2) +
      value.slice(-visibleChars)
    );
  },
};
```

---

## 7. Network Security

### 7.1 Transport Layer Security

Cloudflare enforces TLS for all edge traffic. Configure your zone for maximum security:

```bash
# Via Cloudflare API - set minimum TLS version
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
export function addSecurityHeaders(response: Response): Response {
  const headers = new Headers(response.headers);

  // HSTS - enforce HTTPS for 1 year
  headers.set(
    'Strict-Transport-Security',
    'max-age=31536000; includeSubDomains; preload'
  );

  // Prevent MIME type sniffing
  headers.set('X-Content-Type-Options', 'nosniff');

  // XSS protection (legacy browsers)
  headers.set('X-XSS-Protection', '1; mode=block');

  // Prevent framing (clickjacking protection)
  headers.set('X-Frame-Options', 'DENY');

  // Content Security Policy
  headers.set(
    'Content-Security-Policy',
    "default-src 'self'; frame-ancestors 'none'; base-uri 'self'"
  );

  // Referrer Policy
  headers.set('Referrer-Policy', 'strict-origin-when-cross-origin');

  // Permissions Policy
  headers.set(
    'Permissions-Policy',
    'geolocation=(), microphone=(), camera=()'
  );

  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

// Apply to all responses
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const response = await handleRequest(request, env);
    return addSecurityHeaders(response);
  }
};
```

### 7.3 Rate Limiting

Implement rate limiting to prevent abuse:

```typescript
interface RateLimitConfig {
  windowMs: number;      // Time window in milliseconds
  maxRequests: number;   // Max requests per window
  keyGenerator: (request: Request) => string;
}

class RateLimiter {
  private requests: Map<string, number[]> = new Map();

  constructor(private config: RateLimitConfig) {}

  async check(request: Request): Promise<{
    allowed: boolean;
    remaining: number;
    resetAt: number;
  }> {
    const key = this.config.keyGenerator(request);
    const now = Date.now();
    const windowStart = now - this.config.windowMs;

    // Get existing requests for this key
    let timestamps = this.requests.get(key) || [];

    // Remove expired entries
    timestamps = timestamps.filter(t => t > windowStart);

    const remaining = Math.max(0, this.config.maxRequests - timestamps.length);
    const resetAt = timestamps.length > 0
      ? timestamps[0] + this.config.windowMs
      : now + this.config.windowMs;

    if (timestamps.length >= this.config.maxRequests) {
      return { allowed: false, remaining: 0, resetAt };
    }

    // Record this request
    timestamps.push(now);
    this.requests.set(key, timestamps);

    return { allowed: true, remaining: remaining - 1, resetAt };
  }
}

// Usage
const rateLimiter = new RateLimiter({
  windowMs: 60000,  // 1 minute
  maxRequests: 100,
  keyGenerator: (request) => {
    // Rate limit by IP + user combo
    const ip = request.headers.get('CF-Connecting-IP') || 'unknown';
    const userId = request.headers.get('X-User-Id') || 'anonymous';
    return `${ip}:${userId}`;
  },
});

async function handleWithRateLimit(
  request: Request,
  handler: () => Promise<Response>
): Promise<Response> {
  const result = await rateLimiter.check(request);

  const headers = {
    'X-RateLimit-Limit': String(100),
    'X-RateLimit-Remaining': String(result.remaining),
    'X-RateLimit-Reset': String(Math.ceil(result.resetAt / 1000)),
  };

  if (!result.allowed) {
    return new Response(
      JSON.stringify({ error: 'Rate limit exceeded' }),
      {
        status: 429,
        headers: {
          ...headers,
          'Content-Type': 'application/json',
          'Retry-After': String(Math.ceil((result.resetAt - Date.now()) / 1000)),
        },
      }
    );
  }

  const response = await handler();
  const newHeaders = new Headers(response.headers);
  Object.entries(headers).forEach(([k, v]) => newHeaders.set(k, v));

  return new Response(response.body, {
    status: response.status,
    headers: newHeaders,
  });
}
```

### 7.4 Request Validation

Protect against oversized and malformed requests:

```typescript
interface RequestLimits {
  maxBodySize: number;       // Maximum request body in bytes
  maxBatchSize: number;      // Maximum queries in a batch
  maxQueryLength: number;    // Maximum SQL query length
  maxParameterCount: number; // Maximum bind parameters
}

const DEFAULT_LIMITS: RequestLimits = {
  maxBodySize: 1024 * 1024,      // 1MB
  maxBatchSize: 100,
  maxQueryLength: 10000,
  maxParameterCount: 1000,
};

async function validateRequest(
  request: Request,
  limits: RequestLimits = DEFAULT_LIMITS
): Promise<{ valid: true; body: unknown } | { valid: false; error: string }> {
  // Check content length
  const contentLength = request.headers.get('Content-Length');
  if (contentLength && parseInt(contentLength) > limits.maxBodySize) {
    return { valid: false, error: 'Request body too large' };
  }

  // Parse body
  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return { valid: false, error: 'Invalid JSON body' };
  }

  // Validate batch requests
  if (Array.isArray(body)) {
    if (body.length > limits.maxBatchSize) {
      return {
        valid: false,
        error: `Batch size ${body.length} exceeds limit ${limits.maxBatchSize}`,
      };
    }
  }

  // Validate query requests
  const queries = Array.isArray(body) ? body : [body];
  for (const query of queries) {
    if (typeof query.sql === 'string') {
      if (query.sql.length > limits.maxQueryLength) {
        return {
          valid: false,
          error: `Query length ${query.sql.length} exceeds limit ${limits.maxQueryLength}`,
        };
      }
    }

    if (Array.isArray(query.params)) {
      if (query.params.length > limits.maxParameterCount) {
        return {
          valid: false,
          error: `Parameter count ${query.params.length} exceeds limit ${limits.maxParameterCount}`,
        };
      }
    }
  }

  return { valid: true, body };
}
```

---

## 8. Configuration Options

### 8.1 Database Security Options

```typescript
interface DatabaseSecurityOptions {
  /**
   * Maximum query execution time in milliseconds.
   * Queries exceeding this timeout will be cancelled.
   * @default 5000
   */
  queryTimeout?: number;

  /**
   * Maximum number of rows a single query can return.
   * Prevents memory exhaustion from unbounded SELECTs.
   * @default 10000
   */
  maxResultRows?: number;

  /**
   * Whether to allow DDL statements (CREATE, ALTER, DROP).
   * Set to false in production for defense-in-depth.
   * @default true
   */
  allowDDL?: boolean;

  /**
   * Whether to allow ATTACH DATABASE statements.
   * Generally should be false for security.
   * @default false
   */
  allowAttach?: boolean;

  /**
   * Maximum number of tables that can be created.
   * Prevents resource exhaustion.
   * @default 1000
   */
  maxTables?: number;

  /**
   * Maximum size of prepared statement cache.
   * @default 100
   */
  statementCacheSize?: number;

  /**
   * Whether to log slow queries.
   * @default true
   */
  logSlowQueries?: boolean;

  /**
   * Threshold in ms for slow query logging.
   * @default 1000
   */
  slowQueryThreshold?: number;
}

// Example: Production configuration
const productionConfig: DatabaseSecurityOptions = {
  queryTimeout: 3000,
  maxResultRows: 5000,
  allowDDL: false,         // Disable DDL in production
  allowAttach: false,
  maxTables: 100,
  statementCacheSize: 200,
  logSlowQueries: true,
  slowQueryThreshold: 500,
};

const db = createDatabase(':memory:', productionConfig);
```

### 8.2 RPC Security Configuration

```typescript
interface RPCSecurityConfig {
  /**
   * Maximum request body size in bytes.
   * @default 1048576 (1MB)
   */
  maxRequestSize: number;

  /**
   * Maximum number of queries in a batch request.
   * @default 100
   */
  maxBatchSize: number;

  /**
   * Request timeout in milliseconds.
   * @default 30000 (30 seconds)
   */
  requestTimeout: number;

  /**
   * Enable request/response logging.
   * @default true in development, false in production
   */
  enableLogging: boolean;

  /**
   * Allowed origins for CORS.
   * Use specific domains, never '*' in production.
   * @default []
   */
  allowedOrigins: string[];

  /**
   * Allowed methods for CORS.
   * @default ['GET', 'POST', 'OPTIONS']
   */
  allowedMethods: string[];

  /**
   * Max age for CORS preflight cache (seconds).
   * @default 86400 (24 hours)
   */
  corsMaxAge: number;

  /**
   * Enable CORS credentials.
   * @default false
   */
  corsCredentials: boolean;
}

// Example: Production RPC configuration
const rpcConfig: RPCSecurityConfig = {
  maxRequestSize: 512 * 1024,  // 512KB
  maxBatchSize: 50,
  requestTimeout: 10000,
  enableLogging: false,
  allowedOrigins: [
    'https://app.example.com',
    'https://admin.example.com',
  ],
  allowedMethods: ['GET', 'POST', 'OPTIONS'],
  corsMaxAge: 86400,
  corsCredentials: true,
};
```

### 8.3 Secrets Management

Store secrets in Cloudflare Secrets, never in environment variables or code:

```bash
# Set secrets using wrangler CLI
wrangler secret put JWT_SECRET
wrangler secret put ENCRYPTION_KEY
wrangler secret put FIELD_ENCRYPTION_KEY

# List secrets (values not shown)
wrangler secret list

# Delete a secret
wrangler secret delete OLD_SECRET
```

**Access in Worker:**

```typescript
export interface Env {
  // Secrets (from wrangler secret put)
  JWT_SECRET: string;
  ENCRYPTION_KEY: string;
  FIELD_ENCRYPTION_KEY: string;

  // Bindings
  DOSQL_DO: DurableObjectNamespace;
  API_KEYS: KVNamespace;
  AUDIT_LOGS: R2Bucket;
}

// Secrets are available on env, automatically redacted from logs
async function handler(request: Request, env: Env) {
  // Safe - secret values are never logged
  const isValid = await verifyJWT(token, env.JWT_SECRET);

  // WRONG - never log secrets
  // console.log(env.JWT_SECRET);  // DON'T DO THIS
}
```

### 8.4 Environment Configuration

Configure security settings per environment:

**wrangler.toml:**

```toml
name = "dosql-app"
main = "src/index.ts"
compatibility_date = "2024-01-01"

# Production environment
[env.production]
vars = { ENVIRONMENT = "production", LOG_LEVEL = "error" }

# Staging environment
[env.staging]
vars = { ENVIRONMENT = "staging", LOG_LEVEL = "info" }

# Development - more permissive for debugging
[env.development]
vars = { ENVIRONMENT = "development", LOG_LEVEL = "debug" }
```

**Security config per environment:**

```typescript
function getSecurityConfig(env: Env): SecurityConfig {
  const isProduction = env.ENVIRONMENT === 'production';

  return {
    // Stricter limits in production
    queryTimeout: isProduction ? 3000 : 30000,
    maxResultRows: isProduction ? 5000 : 100000,
    allowDDL: !isProduction,

    // Logging
    enableRequestLogging: !isProduction,
    logSlowQueries: true,
    slowQueryThreshold: isProduction ? 500 : 5000,

    // CORS
    allowedOrigins: isProduction
      ? ['https://app.example.com']
      : ['http://localhost:3000', 'http://localhost:5173'],

    // Rate limiting
    rateLimit: {
      windowMs: 60000,
      maxRequests: isProduction ? 100 : 1000,
    },
  };
}
```

---

## 9. Audit and Compliance

### 9.1 Audit Events to Capture

```typescript
// Audit event types
const AUDIT_EVENTS = {
  // Authentication events (ALWAYS log)
  AUTH_SUCCESS: 'auth.success',
  AUTH_FAILURE: 'auth.failure',
  AUTH_LOGOUT: 'auth.logout',
  TOKEN_REFRESH: 'auth.token_refresh',
  TOKEN_REVOKED: 'auth.token_revoked',

  // Authorization events (ALWAYS log failures)
  AUTHZ_DENIED: 'authz.denied',

  // Query events (configurable)
  QUERY_READ: 'query.read',
  QUERY_WRITE: 'query.write',
  QUERY_DDL: 'query.ddl',

  // Data change events (via CDC)
  DATA_INSERT: 'data.insert',
  DATA_UPDATE: 'data.update',
  DATA_DELETE: 'data.delete',

  // Admin events (ALWAYS log)
  SCHEMA_CHANGE: 'admin.schema_change',
  USER_CREATE: 'admin.user_create',
  USER_DELETE: 'admin.user_delete',
  USER_UPDATE: 'admin.user_update',
  PERMISSION_CHANGE: 'admin.permission_change',
  API_KEY_CREATE: 'admin.api_key_create',
  API_KEY_REVOKE: 'admin.api_key_revoke',

  // Security events (ALWAYS log)
  SQL_INJECTION_ATTEMPT: 'security.sql_injection',
  AUTH_BYPASS_ATTEMPT: 'security.auth_bypass',
  RATE_LIMIT_EXCEEDED: 'security.rate_limit',
  INVALID_INPUT: 'security.invalid_input',
  SUSPICIOUS_ACTIVITY: 'security.suspicious',
} as const;
```

### 9.2 Audit Log Implementation

```typescript
interface AuditEvent {
  id: string;
  timestamp: string;        // ISO 8601
  eventType: string;
  severity: 'info' | 'warning' | 'error' | 'critical';
  userId: string | null;
  tenantId: string;
  action: string;
  resource?: string;
  resourceId?: string;
  details?: Record<string, unknown>;
  ipAddress?: string;
  userAgent?: string;
  requestId: string;
  success: boolean;
  errorCode?: string;
  errorMessage?: string;
  duration?: number;         // Milliseconds
}

class AuditLogger {
  private queue: AuditEvent[] = [];
  private flushInterval: number;

  constructor(
    private storage: R2Bucket,
    private tenantId: string,
    flushIntervalMs: number = 5000
  ) {
    this.flushInterval = flushIntervalMs;
  }

  log(event: Omit<AuditEvent, 'id' | 'timestamp' | 'tenantId'>): void {
    const fullEvent: AuditEvent = {
      ...event,
      id: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
      tenantId: this.tenantId,
    };

    this.queue.push(fullEvent);

    // Flush immediately for security-critical events
    if (this.isSecurityCritical(event.eventType)) {
      this.flush();
    }
  }

  private isSecurityCritical(eventType: string): boolean {
    return [
      'auth.failure',
      'authz.denied',
      'security.sql_injection',
      'security.auth_bypass',
      'security.suspicious',
      'admin.permission_change',
      'admin.api_key_revoke',
    ].includes(eventType);
  }

  async flush(): Promise<void> {
    if (this.queue.length === 0) return;

    const events = this.queue;
    this.queue = [];

    const now = new Date();
    const path = [
      'audit-logs',
      this.tenantId,
      now.getUTCFullYear(),
      String(now.getUTCMonth() + 1).padStart(2, '0'),
      String(now.getUTCDate()).padStart(2, '0'),
      `${now.getTime()}-${crypto.randomUUID().slice(0, 8)}.jsonl`,
    ].join('/');

    const content = events.map(e => JSON.stringify(e)).join('\n');
    await this.storage.put(path, content, {
      customMetadata: {
        eventCount: String(events.length),
        firstEventTime: events[0].timestamp,
        lastEventTime: events[events.length - 1].timestamp,
      },
    });
  }
}

// Usage in Durable Object
class DoSQLDurableObject {
  private auditLogger: AuditLogger;

  constructor(state: DurableObjectState, env: Env) {
    this.auditLogger = new AuditLogger(
      env.AUDIT_LOGS,
      state.id.toString()
    );
  }

  async fetch(request: Request): Promise<Response> {
    const requestId = crypto.randomUUID();
    const startTime = Date.now();
    const userId = request.headers.get('X-User-Id');

    try {
      const result = await this.handleRequest(request);

      this.auditLogger.log({
        eventType: AUDIT_EVENTS.QUERY_READ,
        severity: 'info',
        userId,
        action: 'query',
        requestId,
        success: true,
        duration: Date.now() - startTime,
      });

      return result;
    } catch (error) {
      this.auditLogger.log({
        eventType: AUDIT_EVENTS.QUERY_READ,
        severity: 'error',
        userId,
        action: 'query',
        requestId,
        success: false,
        errorMessage: error instanceof Error ? error.message : 'Unknown error',
        duration: Date.now() - startTime,
      });

      throw error;
    }
  }
}
```

### 9.3 Compliance Considerations

| Requirement | DoSQL Features | Your Implementation |
|-------------|----------------|---------------------|
| **SOC 2** | Encryption at rest/transit, DO isolation | Audit logs (90+ days), access controls |
| **GDPR** | Data isolation per tenant | Right to erasure, data portability, consent tracking |
| **HIPAA** | Encryption, access controls | BAA with Cloudflare, audit trails, access logging |
| **PCI DSS** | No card data in DoSQL recommended | Tokenization, encryption, access logging |

**Data Retention Configuration:**

```typescript
interface RetentionPolicy {
  auditLogs: number;      // Days to retain audit logs
  transactionLogs: number; // Days to retain WAL
  backups: number;        // Days to retain backups
}

const COMPLIANCE_RETENTION: Record<string, RetentionPolicy> = {
  soc2: {
    auditLogs: 90,
    transactionLogs: 30,
    backups: 90,
  },
  hipaa: {
    auditLogs: 2190,  // 6 years
    transactionLogs: 90,
    backups: 365,
  },
  gdpr: {
    auditLogs: 30,    // Minimize data retention
    transactionLogs: 7,
    backups: 30,
  },
  pciDss: {
    auditLogs: 365,
    transactionLogs: 90,
    backups: 365,
  },
};
```

---

## 10. Security Checklist

### 10.1 Pre-Deployment Checklist

#### Authentication and Authorization

- [ ] JWT/token validation with proper signature verification
- [ ] Token expiration enforced and checked
- [ ] Token not-before (nbf) claim checked
- [ ] API keys hashed before storage (SHA-256, never plaintext)
- [ ] Role-based access control implemented for all operations
- [ ] Failed authentication attempts logged
- [ ] Failed authentication rate limited (max 5 attempts per minute)

#### SQL Injection Prevention

- [ ] All queries use parameterized statements (`?` or `:name` placeholders)
- [ ] No string concatenation or interpolation in SQL
- [ ] Dynamic table/column names validated against allow-lists
- [ ] Input validation using schema validation (Zod, etc.)
- [ ] Query length limits enforced (max 10KB recommended)
- [ ] Parameter count limits enforced

#### Tenant Isolation

- [ ] Each tenant has a dedicated Durable Object
- [ ] Tenant ID derived ONLY from authenticated token claims
- [ ] Tenant ID NEVER from user input (headers, query params, body)
- [ ] No shared state between tenant DOs
- [ ] Cross-tenant access testing performed

#### Network Security

- [ ] HTTPS enforced for all traffic (Cloudflare default)
- [ ] HSTS headers configured with preload
- [ ] CORS properly configured (specific origins, not `*`)
- [ ] WebSocket origin validation implemented
- [ ] Security headers added (CSP, X-Frame-Options, etc.)
- [ ] Rate limiting implemented per user/IP

#### Data Protection

- [ ] Sensitive fields encrypted at application level (PII, financial)
- [ ] PII fields identified and documented
- [ ] Data masking implemented for logs and displays
- [ ] Secrets stored in Cloudflare Secrets (not env vars or code)
- [ ] Encryption keys rotated annually

#### Monitoring and Logging

- [ ] Audit logging enabled for authentication events
- [ ] Audit logging enabled for authorization failures
- [ ] Audit logging enabled for admin operations
- [ ] Security events alerting configured
- [ ] Log retention policies match compliance requirements
- [ ] Sensitive data redacted from logs

### 10.2 Regular Security Reviews

| Frequency | Tasks |
|-----------|-------|
| **Daily** | Monitor error rates, check for unusual patterns in dashboards |
| **Weekly** | Review failed auth logs, check rate limiting effectiveness, review security alerts |
| **Monthly** | Rotate service API keys, review permission assignments, audit privileged operations |
| **Quarterly** | Dependency vulnerability scan (`npm audit`), penetration testing, review access policies |
| **Annually** | Full architecture security review, compliance audit, encryption key rotation, incident response drill |

**Automated Security Checks:**

```bash
# Weekly dependency audit
npm audit --audit-level=moderate

# Monthly - check for known vulnerabilities
npx snyk test

# Before each deployment
npm audit --audit-level=high
```

---

## 11. Incident Response

### 11.1 Emergency Procedures

```typescript
// Emergency: Disable compromised API key immediately
async function revokeAPIKeyEmergency(keyId: string, env: Env): Promise<void> {
  const apiKeyJson = await env.API_KEYS.get(`apikey:${keyId}`);
  if (!apiKeyJson) return;

  const apiKey: APIKey = JSON.parse(apiKeyJson);

  // Delete both the key and the hash lookup
  await Promise.all([
    env.API_KEYS.delete(`hash:${apiKey.hashedKey}`),
    env.API_KEYS.delete(`apikey:${keyId}`),
  ]);

  // Log the revocation
  console.log(`SECURITY: API key ${keyId} revoked at ${new Date().toISOString()}`);
}

// Emergency: Invalidate all sessions for a compromised user
async function invalidateUserSessions(userId: string, db: Database): Promise<void> {
  db.prepare('DELETE FROM user_sessions WHERE user_id = ?').run(userId);
  db.prepare('DELETE FROM refresh_tokens WHERE user_id = ?').run(userId);
}

// Emergency: Block suspicious IP via Cloudflare API
async function blockIP(ip: string, reason: string, env: Env): Promise<void> {
  await fetch(
    `https://api.cloudflare.com/client/v4/zones/${env.CF_ZONE_ID}/firewall/access_rules/rules`,
    {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${env.CF_API_TOKEN}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        mode: 'block',
        configuration: { target: 'ip', value: ip },
        notes: `Blocked: ${reason} at ${new Date().toISOString()}`,
      }),
    }
  );
}

// Emergency: Enable maintenance mode (read-only)
async function enableMaintenanceMode(env: Env): Promise<void> {
  await env.KV.put('maintenance_mode', JSON.stringify({
    enabled: true,
    reason: 'Security incident investigation',
    startedAt: new Date().toISOString(),
  }));
}

// Check maintenance mode in request handler
async function checkMaintenanceMode(env: Env): Promise<boolean> {
  const mode = await env.KV.get('maintenance_mode');
  if (!mode) return false;
  return JSON.parse(mode).enabled;
}
```

### 11.2 Incident Response Steps

```
INCIDENT RESPONSE PROCESS
=========================

1. DETECT
   - Monitor dashboards for anomalies
   - Alert on failed auth spikes (>10/min per user)
   - Alert on unusual query patterns
   - Alert on rate limit triggers

2. CONTAIN (First 15 minutes)
   □ Disable compromised credentials
   □ Block suspicious IPs
   □ Enable maintenance mode if severe
   □ Preserve logs and evidence

3. INVESTIGATE (Hours 1-4)
   □ Review audit logs for affected period
   □ Identify scope of compromise
   □ Determine attack vector
   □ Identify affected data/users

4. REMEDIATE (Hours 4-24)
   □ Patch vulnerability
   □ Rotate affected credentials
   □ Update security rules
   □ Deploy fixes

5. RECOVER (Day 2+)
   □ Restore normal operations
   □ Verify system integrity
   □ Notify affected users if required
   □ Monitor for recurrence

6. DOCUMENT (Week 1)
   □ Write incident report
   □ Root cause analysis
   □ Update procedures
   □ Conduct lessons learned
```

**Incident Severity Levels:**

| Level | Description | Response Time | Examples |
|-------|-------------|---------------|----------|
| P1 - Critical | Active breach, data exfiltration | Immediate | SQL injection success, auth bypass |
| P2 - High | Vulnerability discovered, attempted breach | < 1 hour | Failed SQL injection, credential stuffing |
| P3 - Medium | Security misconfiguration | < 24 hours | Missing rate limiting, weak CORS |
| P4 - Low | Security improvement opportunity | < 1 week | Dependency update, config hardening |

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

### Security Standards

- [OWASP SQL Injection Prevention Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/SQL_Injection_Prevention_Cheat_Sheet.html)
- [OWASP Authentication Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html)
- [OWASP Session Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html)
- [CWE-89: SQL Injection](https://cwe.mitre.org/data/definitions/89.html)

### DoSQL Documentation

- [Architecture](./architecture.md) - System architecture details
- [API Reference](./api-reference.md) - Complete API documentation
- [Deployment](./DEPLOYMENT.md) - Deployment guide
- [Troubleshooting](./TROUBLESHOOTING.md) - Common issues and solutions

---

**Security Contact**

Report security vulnerabilities responsibly via security@dotdo.ai. Please include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact assessment
- Any suggested remediation

We aim to acknowledge reports within 24 hours and provide an initial assessment within 72 hours.

---

*This security guide is maintained as part of the DoSQL project. Version 3.0 - Last updated 2026-01-23.*
