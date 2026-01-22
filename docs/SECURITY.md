# DoSQL/DoLake Security Best Practices

**Version:** 1.0
**Last Updated:** 2026-01-22
**Status:** Production Guidelines

---

## Table of Contents

1. [Overview](#overview)
2. [Authentication Model](#authentication-model)
3. [Authorization](#authorization)
4. [SQL Injection Prevention](#sql-injection-prevention)
5. [Input Validation](#input-validation)
6. [Data Protection](#data-protection)
7. [Audit Logging](#audit-logging)
8. [Security Checklist](#security-checklist)
9. [Incident Response](#incident-response)
10. [Security Contacts](#security-contacts)

---

## Overview

DoSQL and DoLake are edge-native database systems built on Cloudflare Workers and Durable Objects. This document outlines security best practices for deploying and operating these systems in production environments.

### Security Philosophy

DoSQL/DoLake follow the principle of **defense in depth**:

1. **Authentication** is delegated to the caller (your application)
2. **Authorization** is implemented at the application layer
3. **Input validation** uses Zod schemas for runtime type safety
4. **SQL injection prevention** requires parameterized queries
5. **Data protection** leverages Cloudflare's infrastructure encryption
6. **Rate limiting** protects against abuse and DoS attacks

### Shared Responsibility Model

```
+-------------------------------------------------------------------------+
|                         YOUR RESPONSIBILITY                              |
|  +-------------------------------------------------------------------+  |
|  | Authentication | Authorization | Application Logic | Audit Logs  |  |
|  +-------------------------------------------------------------------+  |
+-------------------------------------------------------------------------+
|                      DOSQL/DOLAKE RESPONSIBILITY                         |
|  +-------------------------------------------------------------------+  |
|  | Input Validation | SQL Parsing | Rate Limiting | Type Safety     |  |
|  +-------------------------------------------------------------------+  |
+-------------------------------------------------------------------------+
|                     CLOUDFLARE RESPONSIBILITY                            |
|  +-------------------------------------------------------------------+  |
|  | TLS Termination | DDoS Protection | Encryption at Rest | Network |  |
|  +-------------------------------------------------------------------+  |
+-------------------------------------------------------------------------+
```

---

## Authentication Model

### Overview

DoSQL and DoLake **do not implement authentication internally**. Authentication is delegated to your application layer. This design provides flexibility to integrate with any identity provider while keeping the database layer focused on data operations.

### Why Authentication is Delegated

1. **Flexibility**: Support any auth provider (Auth0, Clerk, Firebase, Cloudflare Access, etc.)
2. **Single Sign-On**: Leverage existing enterprise SSO infrastructure
3. **Edge Performance**: Auth can be validated at the edge before reaching DOs
4. **Separation of Concerns**: Database focuses on data, not identity

### Integration with Cloudflare Access

Cloudflare Access provides zero-trust authentication at the edge. This is the recommended approach for production deployments.

#### Configuration Example

```typescript
// wrangler.toml
[[access]]
required = true
team_domain = "your-team.cloudflareaccess.com"

// In your Worker
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Cloudflare Access JWT is automatically validated
    // Extract user identity from the JWT
    const identity = request.headers.get('Cf-Access-Authenticated-User-Email');

    if (!identity) {
      return new Response('Unauthorized', { status: 401 });
    }

    // Pass identity to DoSQL for audit logging
    const doId = env.DOSQL.idFromName('primary');
    const stub = env.DOSQL.get(doId);

    return stub.fetch(request, {
      headers: {
        ...Object.fromEntries(request.headers),
        'X-User-Identity': identity,
      },
    });
  },
};
```

### Token-Based Authentication Patterns

For API-based access, implement token validation in your Worker:

```typescript
// Token validation middleware
async function validateToken(request: Request, env: Env): Promise<AuthResult> {
  const authHeader = request.headers.get('Authorization');

  if (!authHeader?.startsWith('Bearer ')) {
    return { valid: false, error: 'Missing or invalid Authorization header' };
  }

  const token = authHeader.slice(7);

  try {
    // Validate JWT using your preferred method
    const payload = await verifyJWT(token, env.JWT_SECRET);

    return {
      valid: true,
      userId: payload.sub,
      tenantId: payload.tenant_id,
      scopes: payload.scopes ?? [],
      expiresAt: payload.exp,
    };
  } catch (error) {
    return { valid: false, error: 'Invalid or expired token' };
  }
}

// Usage in Worker
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const auth = await validateToken(request, env);

    if (!auth.valid) {
      return new Response(JSON.stringify({ error: auth.error }), {
        status: 401,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    // Route to tenant-specific Durable Object
    const doId = env.DOSQL.idFromName(`tenant:${auth.tenantId}`);
    const stub = env.DOSQL.get(doId);

    // Forward request with authenticated context
    const headers = new Headers(request.headers);
    headers.set('X-User-Id', auth.userId);
    headers.set('X-Tenant-Id', auth.tenantId);
    headers.set('X-Scopes', auth.scopes.join(','));

    return stub.fetch(new Request(request.url, {
      method: request.method,
      headers,
      body: request.body,
    }));
  },
};
```

### API Key Authentication

For service-to-service communication:

```typescript
// API Key validation
async function validateApiKey(
  request: Request,
  env: Env
): Promise<{ valid: boolean; clientId?: string }> {
  const apiKey = request.headers.get('X-API-Key');

  if (!apiKey) {
    return { valid: false };
  }

  // Hash the API key for secure comparison
  const keyHash = await crypto.subtle.digest(
    'SHA-256',
    new TextEncoder().encode(apiKey)
  );
  const hashHex = Array.from(new Uint8Array(keyHash))
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');

  // Look up in KV (keys should be stored as hashes)
  const clientId = await env.API_KEYS.get(`key:${hashHex}`);

  if (!clientId) {
    return { valid: false };
  }

  return { valid: true, clientId };
}
```

### mTLS Authentication

For high-security environments, use Cloudflare mTLS:

```typescript
// Check client certificate
function validateClientCert(request: Request): boolean {
  const certPresent = request.cf?.tlsClientAuth?.certPresented;
  const certVerified = request.cf?.tlsClientAuth?.certVerified === 'SUCCESS';

  if (!certPresent || !certVerified) {
    return false;
  }

  // Optionally check certificate subject/issuer
  const certSubject = request.cf?.tlsClientAuth?.certSubjectDN;
  // Validate against allowlist...

  return true;
}
```

---

## Authorization

### Current State

DoSQL/DoLake currently **do not implement Role-Based Access Control (RBAC)** internally. Authorization decisions must be made at the application layer before queries reach the database.

### Why RBAC is Not Built-In

1. **Flexibility**: Every application has different authorization requirements
2. **Performance**: Authorization at the edge avoids DO round-trips
3. **Simplicity**: The database layer remains focused and auditable

### Recommended Authorization Patterns

#### Pattern 1: Per-Tenant Isolation with Durable Objects

The strongest isolation model uses separate Durable Objects per tenant:

```typescript
// Each tenant gets their own Durable Object instance
function getTenantDO(env: Env, tenantId: string): DurableObjectStub {
  // Using tenant ID in the DO name ensures complete data isolation
  const doId = env.DOSQL.idFromName(`tenant:${tenantId}`);
  return env.DOSQL.get(doId);
}

// Worker handler
export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext) {
    const auth = await validateAuth(request);

    if (!auth.valid) {
      return new Response('Unauthorized', { status: 401 });
    }

    // Tenant isolation: each tenant's data is in a separate DO
    const stub = getTenantDO(env, auth.tenantId);

    return stub.fetch(request);
  },
};
```

**Benefits:**
- Complete data isolation between tenants
- No cross-tenant queries possible
- Independent scaling per tenant
- Simpler audit trails

**Considerations:**
- Cannot query across tenants easily
- More DOs to manage
- May need aggregation layer for multi-tenant analytics

#### Pattern 2: Application-Layer Authorization

Implement authorization logic in your Worker:

```typescript
// Authorization middleware
interface AuthContext {
  userId: string;
  tenantId: string;
  role: 'admin' | 'editor' | 'viewer';
  permissions: string[];
}

function checkPermission(
  auth: AuthContext,
  resource: string,
  action: 'read' | 'write' | 'delete'
): boolean {
  // Role-based checks
  if (auth.role === 'admin') return true;
  if (auth.role === 'viewer' && action !== 'read') return false;

  // Permission-based checks
  const requiredPermission = `${resource}:${action}`;
  return auth.permissions.includes(requiredPermission);
}

// Query authorization
async function authorizeQuery(
  auth: AuthContext,
  sql: string
): Promise<{ allowed: boolean; reason?: string }> {
  const normalizedSql = sql.trim().toUpperCase();

  // Check for data modification
  if (normalizedSql.startsWith('INSERT') ||
      normalizedSql.startsWith('UPDATE') ||
      normalizedSql.startsWith('DELETE')) {
    if (auth.role === 'viewer') {
      return { allowed: false, reason: 'Viewers cannot modify data' };
    }
  }

  // Check for DDL operations
  if (normalizedSql.startsWith('CREATE') ||
      normalizedSql.startsWith('ALTER') ||
      normalizedSql.startsWith('DROP')) {
    if (auth.role !== 'admin') {
      return { allowed: false, reason: 'Only admins can perform DDL' };
    }
  }

  return { allowed: true };
}

// Usage
export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const auth = await getAuthContext(request);
    const body = await request.json<{ sql: string; params?: unknown[] }>();

    const authResult = await authorizeQuery(auth, body.sql);

    if (!authResult.allowed) {
      return new Response(JSON.stringify({
        error: 'Forbidden',
        reason: authResult.reason,
      }), { status: 403 });
    }

    // Forward to DoSQL
    const stub = env.DOSQL.get(env.DOSQL.idFromName(`tenant:${auth.tenantId}`));
    return stub.fetch(request);
  },
};
```

#### Pattern 3: Row-Level Security via Query Rewriting

For applications requiring row-level security:

```typescript
// Automatically inject tenant filter into queries
function injectTenantFilter(
  sql: string,
  tenantId: string
): string {
  // IMPORTANT: This is a simplified example.
  // Production implementations should use a proper SQL parser.

  const normalizedSql = sql.trim();

  // For SELECT queries, add WHERE clause or AND to existing WHERE
  if (normalizedSql.toUpperCase().startsWith('SELECT')) {
    if (normalizedSql.toUpperCase().includes('WHERE')) {
      // Add to existing WHERE
      return normalizedSql.replace(
        /WHERE/i,
        `WHERE tenant_id = '${tenantId}' AND`
      );
    } else {
      // Find where to insert WHERE clause
      // This is simplified - use a proper parser!
      const fromIndex = normalizedSql.toUpperCase().indexOf('FROM');
      const tableEndIndex = findTableEndIndex(normalizedSql, fromIndex);

      return (
        normalizedSql.slice(0, tableEndIndex) +
        ` WHERE tenant_id = '${tenantId}'` +
        normalizedSql.slice(tableEndIndex)
      );
    }
  }

  // For INSERT/UPDATE/DELETE, ensure tenant_id matches
  // Implementation varies based on query type...

  return sql;
}
```

**Warning:** Query rewriting is complex and error-prone. Consider using per-tenant DOs instead when possible.

#### Pattern 4: Column-Level Access Control

For sensitive columns, implement masking at the application layer:

```typescript
// Define sensitive columns per table
const sensitiveColumns: Record<string, string[]> = {
  users: ['ssn', 'credit_card', 'password_hash'],
  payments: ['card_number', 'cvv'],
};

// Mask sensitive data based on role
function maskSensitiveData(
  tableName: string,
  rows: Record<string, unknown>[],
  role: string
): Record<string, unknown>[] {
  const sensitive = sensitiveColumns[tableName] ?? [];

  if (role === 'admin') {
    return rows; // Admins see everything
  }

  return rows.map(row => {
    const masked = { ...row };

    for (const col of sensitive) {
      if (col in masked) {
        masked[col] = '***REDACTED***';
      }
    }

    return masked;
  });
}
```

---

## SQL Injection Prevention

### The Golden Rule

**ALWAYS use parameterized queries. NEVER concatenate user input into SQL strings.**

### Parameterized Queries (Required Pattern)

DoSQL supports parameterized queries with positional (`?`) and named (`:name`, `@name`, `$name`) parameters:

```typescript
// CORRECT: Parameterized query with positional parameters
const result = await db.query(
  'SELECT * FROM users WHERE id = ? AND status = ?',
  [userId, 'active']
);

// CORRECT: Parameterized query with named parameters
const result = await db.query(
  'SELECT * FROM users WHERE id = :id AND status = :status',
  { id: userId, status: 'active' }
);

// CORRECT: Using prepared statements
const stmt = db.prepare('SELECT * FROM users WHERE email = ?');
const user = stmt.get(email);

// CORRECT: Batch operations with parameterized queries
const insert = db.prepare('INSERT INTO logs (user_id, action) VALUES (?, ?)');
const insertMany = db.transaction((entries) => {
  for (const entry of entries) {
    insert.run(entry.userId, entry.action);
  }
});
insertMany(logEntries);
```

### What NOT to Do (SQL Injection Vulnerabilities)

```typescript
// WRONG: String concatenation - SQL INJECTION VULNERABILITY!
const query = `SELECT * FROM users WHERE id = ${userId}`;

// WRONG: Template literals without parameters - SQL INJECTION VULNERABILITY!
const query = `SELECT * FROM users WHERE email = '${userEmail}'`;

// WRONG: String interpolation - SQL INJECTION VULNERABILITY!
const query = 'SELECT * FROM users WHERE name = "' + userName + '"';

// WRONG: Even with "sanitization" - STILL VULNERABLE!
const sanitized = userName.replace(/'/g, "''");
const query = `SELECT * FROM users WHERE name = '${sanitized}'`;
// This can be bypassed with unicode normalization attacks, etc.

// WRONG: Building dynamic column names from user input
const query = `SELECT ${userSelectedColumn} FROM users`;
// If userSelectedColumn is "id; DROP TABLE users; --", disaster ensues
```

### The State-Aware Tokenizer

DoSQL includes a state-aware SQL tokenizer (`packages/dosql/src/database/tokenizer.ts`) that properly handles SQL statement parsing. This tokenizer:

1. **Tracks parsing state** (normal, string literal, identifier, comment)
2. **Handles escaped quotes** (`''` for single quotes, `""` for double quotes)
3. **Recognizes comment syntax** (`--` line comments, `/* */` block comments)
4. **Splits only on real statement boundaries** (semicolons outside strings/comments)

However, the tokenizer is for **parsing**, not **sanitization**. It does not make string concatenation safe.

```typescript
import { tokenizeSQL, isBalanced } from '@dotdo/dosql';

// Check if SQL is syntactically balanced (for validation, not sanitization)
if (!isBalanced(sql)) {
  throw new Error('Malformed SQL: unclosed string or comment');
}

// Split multiple statements
const statements = tokenizeSQL(multiStatementSql);
for (const stmt of statements) {
  await db.exec(stmt);
}
```

### Safe Dynamic Queries

For legitimate dynamic query needs (e.g., sorting, filtering):

```typescript
// Safe column whitelist for ORDER BY
const ALLOWED_SORT_COLUMNS = ['id', 'name', 'created_at', 'updated_at'];
const ALLOWED_DIRECTIONS = ['ASC', 'DESC'];

function buildSortedQuery(
  baseQuery: string,
  sortColumn: string,
  sortDirection: string
): string {
  // Validate against whitelist
  if (!ALLOWED_SORT_COLUMNS.includes(sortColumn)) {
    throw new Error(`Invalid sort column: ${sortColumn}`);
  }

  if (!ALLOWED_DIRECTIONS.includes(sortDirection.toUpperCase())) {
    throw new Error(`Invalid sort direction: ${sortDirection}`);
  }

  // Safe to concatenate because we validated against whitelist
  return `${baseQuery} ORDER BY ${sortColumn} ${sortDirection}`;
}

// Safe table name handling
const ALLOWED_TABLES = ['users', 'orders', 'products'];

function queryTable(tableName: string, params: unknown[]): QueryResult {
  if (!ALLOWED_TABLES.includes(tableName)) {
    throw new Error(`Invalid table: ${tableName}`);
  }

  // Table name is safe because it's from our whitelist
  return db.query(`SELECT * FROM ${tableName} WHERE active = ?`, params);
}

// Safe dynamic WHERE clauses with parameterized values
function buildDynamicWhere(
  filters: Record<string, unknown>,
  allowedColumns: string[]
): { sql: string; params: unknown[] } {
  const conditions: string[] = [];
  const params: unknown[] = [];

  for (const [column, value] of Object.entries(filters)) {
    // Validate column name against whitelist
    if (!allowedColumns.includes(column)) {
      throw new Error(`Invalid filter column: ${column}`);
    }

    conditions.push(`${column} = ?`);
    params.push(value);
  }

  return {
    sql: conditions.length > 0 ? `WHERE ${conditions.join(' AND ')}` : '',
    params,
  };
}
```

---

## Input Validation

### Zod Schema Validation in DoLake

DoLake validates all incoming WebSocket messages using Zod schemas (`packages/dolake/src/schemas.ts`). This provides:

1. **Runtime type validation** - Ensures messages match expected shapes
2. **Type inference** - TypeScript types are derived from schemas
3. **Detailed error messages** - Pinpoints exactly what's wrong
4. **Discriminated unions** - Efficient message type routing

#### Message Schemas

```typescript
// CDC Event validation
export const CDCEventSchema = z.object({
  sequence: z.number().int(),
  timestamp: z.number().int(),
  operation: z.enum(['INSERT', 'UPDATE', 'DELETE']),
  table: z.string().min(1),
  rowId: z.string(),
  before: z.unknown().optional(),
  after: z.unknown().optional(),
  metadata: z.record(z.string(), z.unknown()).optional(),
});

// CDC Batch message validation
export const CDCBatchMessageSchema = z.object({
  type: z.literal('cdc_batch'),
  timestamp: z.number().int(),
  sourceDoId: z.string().min(1),
  events: z.array(CDCEventSchema),
  sequenceNumber: z.number().int(),
  firstEventSequence: z.number().int(),
  lastEventSequence: z.number().int(),
  sizeBytes: z.number().int(),
  isRetry: z.boolean(),
  retryCount: z.number().int(),
});

// Discriminated union for all client messages
export const ClientRpcMessageSchema = z.discriminatedUnion('type', [
  CDCBatchMessageSchema,
  ConnectMessageSchema,
  HeartbeatMessageSchema,
  FlushRequestMessageSchema,
  DisconnectMessageSchema,
]);
```

#### Message Validation Usage

```typescript
import {
  validateClientMessage,
  MessageValidationError
} from './schemas.js';

// In DoLake WebSocket handler
private decodeMessage(message: ArrayBuffer | string): ValidatedClientRpcMessage {
  // Parse JSON
  let raw: unknown;
  try {
    const text = typeof message === 'string'
      ? message
      : new TextDecoder().decode(message);
    raw = JSON.parse(text);
  } catch {
    throw new MessageValidationError('Invalid JSON');
  }

  // Validate with Zod schema
  return validateClientMessage(raw);
}
```

### Message Size Limits

DoLake enforces message size limits at multiple levels:

```typescript
// Rate limiter configuration (rate-limiter.ts)
export const DEFAULT_RATE_LIMIT_CONFIG: RateLimitConfig = {
  maxPayloadSize: 4 * 1024 * 1024,  // 4MB max total payload
  maxEventSize: 1 * 1024 * 1024,    // 1MB max per event
  // ...
};

// Size validation in rate limiter
checkMessage(connectionId, messageType, payloadSize, eventSizes) {
  if (payloadSize > this.config.maxPayloadSize) {
    return {
      allowed: false,
      reason: 'payload_too_large',
      maxSize: this.config.maxPayloadSize,
    };
  }

  for (const eventSize of eventSizes) {
    if (eventSize > this.config.maxEventSize) {
      return {
        allowed: false,
        reason: 'event_too_large',
        maxSize: this.config.maxEventSize,
      };
    }
  }
  // ...
}
```

### Rate Limiting

DoLake implements comprehensive rate limiting (`packages/dolake/src/rate-limiter.ts`):

#### Rate Limit Features

1. **Token Bucket Algorithm** - Allows controlled bursting
2. **Per-Connection Limits** - Individual connection rate limits
3. **Per-IP Limits** - Prevents single IP from exhausting resources
4. **Per-Source Limits** - Limits connections from a single client
5. **Subnet-Level Limits** - Prevents distributed attacks from /24 subnets
6. **Load Shedding** - Graceful degradation under extreme load
7. **Exponential Backoff** - Progressive retry delays for rate-limited clients

#### Default Configuration

```typescript
export const DEFAULT_RATE_LIMIT_CONFIG: RateLimitConfig = {
  connectionsPerSecond: 20,      // New connections per second
  messagesPerSecond: 100,        // Messages per connection per second
  burstCapacity: 50,             // Token bucket capacity
  refillRate: 10,                // Tokens refilled per second
  maxPayloadSize: 4 * 1024 * 1024,   // 4MB max payload
  maxEventSize: 1 * 1024 * 1024,     // 1MB max event
  maxConnectionsPerSource: 5,    // Max connections per client ID
  maxConnectionsPerIp: 30,       // Max connections per IP
  subnetRateLimitThreshold: 100, // Max connections per /24 subnet
  windowMs: 1000,                // Rate limit window
  backpressureThreshold: 0.8,    // Buffer utilization for backpressure
  baseRetryDelayMs: 100,         // Base exponential backoff delay
  maxRetryDelayMs: 30000,        // Max exponential backoff delay
  maxSizeViolations: 3,          // Violations before connection close
};
```

#### Custom Rate Limit Configuration

```typescript
// Override defaults for specific deployments
const rateLimiter = new RateLimiter({
  connectionsPerSecond: 10,        // More conservative
  maxPayloadSize: 1 * 1024 * 1024, // 1MB limit
  whitelistedIps: ['10.0.0.0/8'],  // Internal networks bypass limits
});
```

### Input Validation Best Practices

1. **Validate early** - Check inputs at the edge before reaching DOs
2. **Use schemas** - Define Zod schemas for all message types
3. **Enforce size limits** - Reject oversized payloads before parsing
4. **Whitelist, don't blacklist** - Allow known-good values, reject everything else
5. **Sanitize on display** - Even trusted data should be escaped for HTML output

---

## Data Protection

### Encryption at Rest

Data stored in Durable Objects and R2 is automatically encrypted at rest by Cloudflare:

- **Durable Object Storage**: Encrypted with AES-256
- **R2 Storage**: Encrypted with AES-256 (server-side encryption by default)

You do not need to configure encryption - it's enabled by default.

### Encryption in Transit

All communication is encrypted via TLS:

```
Client ──[HTTPS/WSS]──> Cloudflare Edge ──[mTLS]──> Durable Object
                            │
                            └──[Internal TLS]──> R2 Storage
```

- **Client to Edge**: HTTPS/TLS 1.3 (managed by Cloudflare)
- **Edge to DO**: Internal encrypted channels
- **DO to R2**: Internal encrypted channels

### Key Management Considerations

#### Secrets Management

Store sensitive configuration in Cloudflare's encrypted secrets:

```bash
# Store secrets via wrangler
wrangler secret put JWT_SECRET
wrangler secret put API_KEY_SALT
wrangler secret put ENCRYPTION_KEY
```

Access secrets in Workers:

```typescript
export interface Env {
  JWT_SECRET: string;
  API_KEY_SALT: string;
  ENCRYPTION_KEY: string;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    // Secrets are available as env bindings
    const secret = env.JWT_SECRET;
    // ...
  },
};
```

#### Application-Level Encryption

For highly sensitive data, consider additional encryption:

```typescript
// Encrypt sensitive fields before storage
async function encryptField(
  data: string,
  key: CryptoKey
): Promise<{ iv: string; data: string }> {
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const encoded = new TextEncoder().encode(data);

  const encrypted = await crypto.subtle.encrypt(
    { name: 'AES-GCM', iv },
    key,
    encoded
  );

  return {
    iv: btoa(String.fromCharCode(...iv)),
    data: btoa(String.fromCharCode(...new Uint8Array(encrypted))),
  };
}

// Decrypt on retrieval
async function decryptField(
  encrypted: { iv: string; data: string },
  key: CryptoKey
): Promise<string> {
  const iv = Uint8Array.from(atob(encrypted.iv), c => c.charCodeAt(0));
  const data = Uint8Array.from(atob(encrypted.data), c => c.charCodeAt(0));

  const decrypted = await crypto.subtle.decrypt(
    { name: 'AES-GCM', iv },
    key,
    data
  );

  return new TextDecoder().decode(decrypted);
}
```

#### Key Rotation

Implement key rotation for long-lived deployments:

```typescript
interface EncryptedData {
  keyVersion: number;
  iv: string;
  data: string;
}

// Store multiple key versions
const keys: Map<number, CryptoKey> = new Map();

async function decryptWithRotation(
  encrypted: EncryptedData
): Promise<string> {
  const key = keys.get(encrypted.keyVersion);
  if (!key) {
    throw new Error(`Unknown key version: ${encrypted.keyVersion}`);
  }

  return decryptField({ iv: encrypted.iv, data: encrypted.data }, key);
}

// Always encrypt with current key
const CURRENT_KEY_VERSION = 2;

async function encrypt(data: string): Promise<EncryptedData> {
  const key = keys.get(CURRENT_KEY_VERSION)!;
  const encrypted = await encryptField(data, key);

  return {
    keyVersion: CURRENT_KEY_VERSION,
    ...encrypted,
  };
}
```

### Data Residency

Cloudflare Durable Objects support jurisdiction hints for data residency:

```typescript
// Specify jurisdiction when creating DO
const doId = env.DOSQL.idFromName('eu-tenant-123');
// Use locationHint to suggest region (Cloudflare may override for availability)

// Or use jurisdiction-restricted namespaces (Enterprise feature)
// Configure in wrangler.toml with jurisdiction = "eu"
```

---

## Audit Logging

### Current State

DoSQL/DoLake do **not implement audit logging** internally. Audit logging must be implemented at the application layer.

### Recommended Implementation

#### Structured Audit Log Format

```typescript
interface AuditLogEntry {
  // Event identification
  id: string;
  timestamp: string;  // ISO 8601
  eventType: 'query' | 'mutation' | 'ddl' | 'auth' | 'admin';

  // Actor information
  actor: {
    type: 'user' | 'service' | 'system';
    id: string;
    ip: string;
    userAgent?: string;
  };

  // Resource information
  resource: {
    type: 'table' | 'database' | 'connection';
    id: string;
    tenant?: string;
  };

  // Action details
  action: {
    operation: string;
    sql?: string;        // Sanitized SQL (no param values)
    affectedRows?: number;
    duration: number;    // Milliseconds
    success: boolean;
    error?: string;
  };

  // Security context
  security: {
    authMethod: 'jwt' | 'apikey' | 'mtls' | 'cloudflare_access';
    scopes: string[];
    rateLimited: boolean;
  };
}
```

#### Logging Implementation

```typescript
// Audit logger using Workers Analytics Engine
class AuditLogger {
  constructor(private analytics: AnalyticsEngine) {}

  log(entry: AuditLogEntry): void {
    this.analytics.writeDataPoint({
      blobs: [
        entry.id,
        entry.actor.id,
        entry.resource.id,
        entry.action.sql ?? '',
      ],
      doubles: [
        entry.action.duration,
        entry.action.affectedRows ?? 0,
      ],
      indexes: [
        entry.eventType,
        entry.action.operation,
        entry.action.success ? 'success' : 'failure',
      ],
    });
  }
}

// Usage in request handler
async function handleQuery(
  request: Request,
  auth: AuthContext,
  env: Env
): Promise<Response> {
  const logger = new AuditLogger(env.ANALYTICS);
  const startTime = Date.now();

  try {
    const result = await executeQuery(request, auth, env);

    logger.log({
      id: crypto.randomUUID(),
      timestamp: new Date().toISOString(),
      eventType: 'query',
      actor: {
        type: 'user',
        id: auth.userId,
        ip: request.headers.get('CF-Connecting-IP') ?? 'unknown',
        userAgent: request.headers.get('User-Agent') ?? undefined,
      },
      resource: {
        type: 'database',
        id: auth.tenantId,
        tenant: auth.tenantId,
      },
      action: {
        operation: 'SELECT',
        sql: sanitizeSql(result.sql),
        affectedRows: result.rowCount,
        duration: Date.now() - startTime,
        success: true,
      },
      security: {
        authMethod: 'jwt',
        scopes: auth.scopes,
        rateLimited: false,
      },
    });

    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (error) {
    logger.log({
      // ... error entry
      action: {
        operation: 'SELECT',
        duration: Date.now() - startTime,
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error',
      },
      // ...
    });

    throw error;
  }
}

// Sanitize SQL to remove parameter values
function sanitizeSql(sql: string): string {
  // Replace string literals with placeholders
  return sql
    .replace(/'[^']*'/g, "'***'")
    .replace(/"[^"]*"/g, '"***"')
    .replace(/\d+/g, '?');
}
```

#### Log Retention and Compliance

```typescript
// Configure log retention via Workers Analytics Engine
// In wrangler.toml:
// [[analytics_engine_datasets]]
// binding = "ANALYTICS"
// dataset = "audit_logs"

// Query logs via SQL API
const query = `
  SELECT
    timestamp,
    blob1 as event_id,
    blob2 as user_id,
    index1 as event_type,
    index3 as status
  FROM audit_logs
  WHERE timestamp > NOW() - INTERVAL '7' DAY
    AND index3 = 'failure'
  ORDER BY timestamp DESC
  LIMIT 100
`;
```

---

## Security Checklist

### Pre-Deployment Checklist

#### Authentication & Authorization

- [ ] **Authentication method chosen** (Cloudflare Access, JWT, API keys, mTLS)
- [ ] **Token validation implemented** with proper expiration checks
- [ ] **Authorization checks** at the Worker layer before DO access
- [ ] **Per-tenant isolation** using separate DO instances per tenant
- [ ] **Admin operations protected** with additional verification

#### Input Validation

- [ ] **All SQL uses parameterized queries** - no string concatenation
- [ ] **Zod schemas defined** for all message types
- [ ] **Message size limits enforced** before JSON parsing
- [ ] **Rate limiting configured** with appropriate thresholds
- [ ] **Input whitelists** for dynamic column/table names

#### Data Protection

- [ ] **TLS enabled** for all external connections (automatic with CF)
- [ ] **Secrets stored** in Cloudflare encrypted secrets
- [ ] **Sensitive data identified** and additional encryption considered
- [ ] **Data residency requirements** addressed if applicable

#### Monitoring & Logging

- [ ] **Audit logging implemented** for security-relevant events
- [ ] **Error logging configured** without sensitive data leakage
- [ ] **Rate limit monitoring** to detect abuse
- [ ] **Alerting configured** for security anomalies

#### Code Quality

- [ ] **Dependencies audited** for known vulnerabilities
- [ ] **TypeScript strict mode enabled** for type safety
- [ ] **No hardcoded secrets** in source code
- [ ] **Error messages** don't leak internal details

### Production Monitoring

- [ ] **Rate limit metrics** tracked and alerted
- [ ] **Authentication failure rates** monitored
- [ ] **Query latency** tracked for anomaly detection
- [ ] **Connection patterns** analyzed for abuse
- [ ] **Error rates** monitored and alerted

### Periodic Security Tasks

- [ ] **Monthly**: Review rate limit effectiveness
- [ ] **Quarterly**: Audit access patterns and permissions
- [ ] **Quarterly**: Update dependencies for security patches
- [ ] **Annually**: Security review and penetration testing

---

## Incident Response

### Security Incident Classification

| Severity | Description | Response Time | Examples |
|----------|-------------|---------------|----------|
| **Critical** | Active data breach or system compromise | Immediate | SQL injection exploited, unauthorized data access |
| **High** | Vulnerability with potential for breach | 4 hours | Authentication bypass discovered, exposed secrets |
| **Medium** | Security issue without immediate impact | 24 hours | Rate limiting ineffective, missing audit logs |
| **Low** | Minor security improvement | 1 week | Configuration hardening, documentation updates |

### Incident Response Steps

#### 1. Identification
- Monitor alerts and logs for anomalies
- Verify incident is genuine (not false positive)
- Classify severity

#### 2. Containment
- For Critical/High: Disable affected endpoints immediately
- Revoke compromised credentials
- Enable additional logging

#### 3. Eradication
- Identify root cause
- Deploy fix to all environments
- Verify fix effectiveness

#### 4. Recovery
- Restore normal operations
- Monitor for recurrence
- Verify data integrity

#### 5. Post-Incident
- Document timeline and actions
- Conduct post-mortem
- Update procedures as needed

### Emergency Procedures

#### Immediate Credential Revocation

```typescript
// Emergency API key revocation
export async function revokeAllApiKeys(env: Env): Promise<void> {
  // Clear all API keys from KV
  const keys = await env.API_KEYS.list({ prefix: 'key:' });
  for (const key of keys.keys) {
    await env.API_KEYS.delete(key.name);
  }

  // Log the action
  console.log(`Emergency revocation: ${keys.keys.length} API keys revoked`);
}

// Emergency JWT secret rotation
// Update JWT_SECRET via wrangler secret put JWT_SECRET
// All existing tokens will become invalid
```

#### Emergency Rate Limiting

```typescript
// Emergency rate limit override
const EMERGENCY_RATE_LIMIT: Partial<RateLimitConfig> = {
  connectionsPerSecond: 1,
  messagesPerSecond: 5,
  maxConnectionsPerIp: 2,
};

// Apply during incident
const rateLimiter = new RateLimiter(EMERGENCY_RATE_LIMIT);
```

---

## Security Contacts

### Reporting Security Issues

If you discover a security vulnerability in DoSQL/DoLake:

1. **Do not** open a public GitHub issue
2. **Email**: security@dotdo.dev (replace with actual contact)
3. **Include**: Description, reproduction steps, potential impact
4. **Expect**: Acknowledgment within 24 hours

### Security Updates

- **Security advisories**: Published via GitHub Security Advisories
- **Updates**: Follow semantic versioning; security fixes in patch releases
- **Deprecations**: Announced 90 days in advance for security-related changes

---

## Appendix: Security Reference

### Common Attack Vectors and Mitigations

| Attack | Description | Mitigation |
|--------|-------------|------------|
| SQL Injection | Malicious SQL in user input | Parameterized queries |
| Authentication Bypass | Accessing resources without valid credentials | Token validation at edge |
| Authorization Bypass | Accessing resources without permission | Application-layer authorization |
| DoS/DDoS | Overwhelming system with requests | Rate limiting, Cloudflare DDoS protection |
| Data Exfiltration | Unauthorized data access | Per-tenant isolation, audit logging |
| Session Hijacking | Stealing user sessions | Short token expiration, secure cookies |
| MITM | Intercepting traffic | TLS everywhere (automatic with CF) |

### Security Headers

Recommended HTTP security headers for responses:

```typescript
const SECURITY_HEADERS = {
  'Strict-Transport-Security': 'max-age=31536000; includeSubDomains',
  'X-Content-Type-Options': 'nosniff',
  'X-Frame-Options': 'DENY',
  'Content-Security-Policy': "default-src 'self'",
  'X-XSS-Protection': '1; mode=block',
  'Referrer-Policy': 'strict-origin-when-cross-origin',
};

function addSecurityHeaders(response: Response): Response {
  const headers = new Headers(response.headers);

  for (const [key, value] of Object.entries(SECURITY_HEADERS)) {
    headers.set(key, value);
  }

  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}
```

---

*Document generated: 2026-01-22*
*Applies to: DoSQL v1.x, DoLake v1.x*
