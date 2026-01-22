# Security Model Guide

This document provides comprehensive security documentation for the SQL system, covering architecture, authentication, authorization, data protection, and security best practices.

## Table of Contents

1. [Security Architecture Overview](#security-architecture-overview)
2. [Authentication Mechanisms](#authentication-mechanisms)
3. [Authorization and Access Control](#authorization-and-access-control)
4. [Data Encryption](#data-encryption)
5. [SQL Injection Prevention](#sql-injection-prevention)
6. [Input Validation](#input-validation)
7. [Rate Limiting](#rate-limiting)
8. [WebSocket Security](#websocket-security)
9. [Secrets Management](#secrets-management)
10. [Audit Logging](#audit-logging)
11. [Compliance Considerations](#compliance-considerations)
12. [Security Best Practices](#security-best-practices)
13. [Threat Model](#threat-model)
14. [Security Guarantees](#security-guarantees)

---

## Security Architecture Overview

### Defense in Depth

The SQL system implements a multi-layered security architecture:

```
                    +------------------+
                    |   Edge Network   |
                    |  (TLS/HTTPS)     |
                    +--------+---------+
                             |
                    +--------v---------+
                    |   Rate Limiter   |
                    |  (DoS Protection)|
                    +--------+---------+
                             |
                    +--------v---------+
                    | Input Validation |
                    |   (Zod Schemas)  |
                    +--------+---------+
                             |
                    +--------v---------+
                    | Authentication   |
                    |  (Token-based)   |
                    +--------+---------+
                             |
                    +--------v---------+
                    |  Authorization   |
                    | (Role/Resource)  |
                    +--------+---------+
                             |
                    +--------v---------+
                    |   SQL Parser     |
                    |(Injection Guard) |
                    +--------+---------+
                             |
                    +--------v---------+
                    |  Query Engine    |
                    |(Constraint Check)|
                    +--------+---------+
                             |
                    +--------v---------+
                    | Storage Layer    |
                    | (Encrypted R2)   |
                    +------------------+
```

### Core Security Principles

1. **Least Privilege**: Components have minimal permissions required for operation
2. **Defense in Depth**: Multiple security layers protect against single-point failures
3. **Fail Secure**: System defaults to secure state on errors
4. **Input Validation**: All external input is validated before processing
5. **Secure by Default**: Security features are enabled by default

### Isolation Model

The system leverages Cloudflare's isolation model:

- **Durable Objects**: Each database instance runs in isolated V8 isolates
- **Memory Isolation**: No shared memory between tenants
- **Network Isolation**: Communication only through defined RPC interfaces
- **Storage Isolation**: Data stored in tenant-specific R2 buckets/KV namespaces

---

## Authentication Mechanisms

### Token-Based Authentication

The system supports multiple authentication methods:

#### Client ID Authentication

```typescript
// WebSocket connection with client authentication
const response = await fetch('http://dolake/ws', {
  headers: {
    Upgrade: 'websocket',
    'X-Client-ID': clientId,  // Required client identifier
    'X-Priority': 'high',      // Optional priority level
  },
});
```

#### API Token Authentication

For HTTP API endpoints:

```typescript
// API request with bearer token
const response = await fetch('/api/query', {
  headers: {
    'Authorization': `Bearer ${apiToken}`,
    'Content-Type': 'application/json',
  },
  body: JSON.stringify({ sql: 'SELECT * FROM users' }),
});
```

### Token Validation

- Tokens are validated on every request
- Invalid or expired tokens result in `401 Unauthorized`
- Token rotation is supported without service interruption

### Protocol Version Validation

Connect messages must include a valid protocol version:

```typescript
const connectMessage: ConnectMessage = {
  type: 'connect',
  timestamp: Date.now(),
  sourceDoId: clientId,
  lastAckSequence: 0,
  protocolVersion: 1,  // Must be non-negative integer
  capabilities: DEFAULT_CLIENT_CAPABILITIES,
};
```

---

## Authorization and Access Control

### Role-Based Access Control (RBAC)

The system supports role-based authorization:

| Role | Permissions |
|------|-------------|
| `admin` | Full access to all operations |
| `writer` | INSERT, UPDATE, DELETE, SELECT |
| `reader` | SELECT only |
| `schema_admin` | DDL operations (CREATE, ALTER, DROP) |

### Resource-Level Access Control

Fine-grained access control at the table/column level:

```typescript
// Example: Table-level permissions
interface TablePermission {
  table: string;
  operations: ('SELECT' | 'INSERT' | 'UPDATE' | 'DELETE')[];
  columns?: string[];  // Optional column-level restrictions
}
```

### Constraint Enforcement

Database constraints provide additional authorization checks:

```typescript
// Foreign key constraints ensure referential integrity
// Check constraints validate business rules
// Unique constraints prevent duplicate entries
validateInsert(tableName: string, row: Record<string, unknown>): ConstraintValidationResult;
validateUpdate(tableName: string, oldRow, newRow, updatedColumns): ConstraintValidationResult;
validateDelete(tableName: string, row: Record<string, unknown>): ConstraintValidationResult;
```

---

## Data Encryption

### Encryption at Rest

#### R2 Storage Encryption

All data stored in Cloudflare R2 is encrypted at rest using:

- **AES-256-GCM** encryption
- Managed encryption keys by Cloudflare
- Optional customer-managed keys (BYOK)

#### KV Storage

Cloudflare KV storage provides:

- Automatic encryption at rest
- Geographic distribution with encrypted replication

### Encryption in Transit

#### TLS/HTTPS

All network communication uses TLS 1.3:

```typescript
// All URLs are automatically upgraded to HTTPS
// HTTP connections are redirected to HTTPS
if (url.protocol === 'http:') {
  url.protocol = 'https:';
}
```

#### WebSocket Security (WSS)

WebSocket connections require secure transport:

- Mandatory `wss://` protocol in production
- TLS certificate validation
- No downgrade attacks

### Data Classification

| Classification | Storage | Transit | Access |
|---------------|---------|---------|--------|
| Sensitive (PII) | AES-256 | TLS 1.3 | Role-restricted |
| Internal | AES-256 | TLS 1.3 | Authenticated |
| Public | AES-256 | TLS 1.3 | Open |

---

## SQL Injection Prevention

### Parameterized Queries

The system enforces parameterized queries to prevent SQL injection:

```typescript
// CORRECT: Parameterized query
const result = await db.query(
  'SELECT * FROM users WHERE id = ? AND name = :name',
  [userId, { name: userName }]
);

// WRONG: String concatenation (rejected)
// const result = await db.query(`SELECT * FROM users WHERE id = ${userId}`);
```

### Parameter Binding

The binding system supports multiple styles:

```typescript
// Positional parameters
parseParameters('SELECT * FROM users WHERE id = ?');

// Numbered parameters
parseParameters('SELECT * FROM users WHERE id = ?1 AND status = ?2');

// Named parameters
parseParameters('SELECT * FROM users WHERE id = :id AND name = @name');
```

### SQL Tokenization Security

The parser uses proper tokenization to handle edge cases:

```typescript
// Semicolons in string literals are NOT treated as statement separators
'INSERT INTO users (name) VALUES (\'test; DROP TABLE users--\')'
// This is parsed as a single INSERT with the literal string value

// SQL comments inside strings are treated as literal text
'SELECT * FROM users WHERE name = \'test/*\''
// The /* is part of the string, not a comment

// Multi-statement queries are rejected
'SELECT * FROM users; DROP TABLE secrets'
// Throws: "Multi-statement queries are not supported"

// UNION queries are rejected to prevent data exfiltration
'SELECT id, name FROM users UNION SELECT id, data FROM secrets'
// Throws: "UNION queries are not supported"
```

### Injection Attack Prevention

The tokenizer prevents common attack vectors:

| Attack Type | Prevention |
|-------------|------------|
| Classic injection (`' OR '1'='1`) | Parameterized queries |
| Comment injection (`--`) | String literal detection |
| Union-based | UNION query rejection |
| Multi-statement | Statement separator detection |
| Null byte | Null byte filtering |
| Encoding attacks | Unicode normalization |

---

## Input Validation

### Schema Validation with Zod

All incoming messages are validated using Zod schemas:

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

// Message validation function
export function validateClientMessage(data: unknown): ValidatedClientRpcMessage {
  const result = ClientRpcMessageSchema.safeParse(data);
  if (!result.success) {
    throw new MessageValidationError(
      `Invalid message format: ${result.error.issues.map((e) => e.message).join(', ')}`,
      result.error
    );
  }
  return result.data;
}
```

### Validation Rules

#### Type Validation

- Numbers must be finite (no `NaN`, `Infinity`, `-Infinity`)
- Strings must meet minimum length requirements
- Arrays must contain valid element types
- Objects must have required fields

```typescript
// Non-finite number validation
if (!Number.isFinite(value)) {
  throw createNonFiniteNumberError(value);
}

// Type coercion with validation
export function coerceValue(value: unknown): SqlValue {
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) {
      throw createNonFiniteNumberError(value);
    }
    return value;
  }
  // ... other type handling
}
```

#### Size Limits

```typescript
// Message size limits
const SIZE_LIMITS = {
  MAX_PAYLOAD_SIZE: 4 * 1024 * 1024,  // 4MB
  MAX_EVENT_SIZE: 1 * 1024 * 1024,     // 1MB
};

// Validation enforcement
if (payloadSize > config.maxPayloadSize) {
  return {
    allowed: false,
    reason: 'payload_too_large',
    maxSize: config.maxPayloadSize,
  };
}
```

### Malformed Input Handling

```typescript
// Malformed JSON is rejected
'{ "type": "cdc_batch", invalid }'  // -> nack

// Empty objects are rejected
{}  // -> nack with 'invalid_format'

// Missing required fields are rejected
{ type: 'cdc_batch' }  // -> nack (missing sourceDoId, events, etc.)

// Wrong field types are rejected
{ type: 'cdc_batch', sequenceNumber: '1' }  // -> nack (string instead of number)
```

---

## Rate Limiting

### Token Bucket Algorithm

The system implements token bucket rate limiting:

```typescript
export interface TokenBucket {
  tokens: number;        // Current tokens
  capacity: number;      // Maximum tokens
  refillRate: number;    // Tokens per second
  lastRefillTime: number;
}

// Token consumption
private refillBucket(bucket: TokenBucket, now: number): void {
  const elapsed = (now - bucket.lastRefillTime) / 1000;
  const tokensToAdd = elapsed * bucket.refillRate;
  bucket.tokens = Math.min(bucket.capacity, bucket.tokens + tokensToAdd);
  bucket.lastRefillTime = now;
}
```

### Rate Limit Configuration

```typescript
export const DEFAULT_RATE_LIMIT_CONFIG: RateLimitConfig = {
  connectionsPerSecond: 50,           // Max new connections/second
  messagesPerSecond: 100,             // Max messages/connection/second
  burstCapacity: 50,                  // Burst token bucket capacity
  refillRate: 10,                     // Token refill rate
  maxPayloadSize: 4 * 1024 * 1024,    // 4MB max payload
  maxEventSize: 1 * 1024 * 1024,      // 1MB max event
  maxConnectionsPerSource: 5,          // Per-source connection limit
  maxConnectionsPerIp: 30,            // Per-IP connection limit
  subnetRateLimitThreshold: 100,      // /24 subnet limit
  windowMs: 1000,                     // Rate limit window
  backpressureThreshold: 0.8,         // Buffer backpressure trigger
  maxSizeViolations: 3,               // Violations before disconnect
};
```

### Per-IP Rate Limiting

```typescript
// IP-based rate limiting
checkConnection(clientId: string, ip?: string, priority?: string): RateLimitResult {
  // Check IP whitelist
  if (ip && this.isWhitelisted(ip)) {
    return this.allowedResult(now);
  }

  // Check per-IP limit
  const ipState = this.getOrCreateIpState(ip);
  if (ipState.connectionCount >= this.config.maxConnectionsPerIp) {
    this.metrics.ipRejections++;
    return this.rejectedResult(now, 'ip_limit', 'Maximum connections per IP exceeded');
  }

  // Check subnet limit (/24 for IPv4, /64 for IPv6)
  const subnet = this.getSubnet(ip);
  if (subnet) {
    const subnetState = this.getOrCreateSubnetState(subnet);
    if (subnetState.connectionCount >= this.config.subnetRateLimitThreshold) {
      return this.rejectedResult(now, 'ip_limit', 'Subnet rate limit exceeded');
    }
  }
}
```

### Rate Limit Headers

HTTP responses include standard rate limit headers:

```typescript
// Rate limit response headers
{
  'X-RateLimit-Limit': limit.toString(),
  'X-RateLimit-Remaining': remaining.toString(),
  'X-RateLimit-Reset': resetAt.toString(),
  'Retry-After': retryAfterSeconds.toString(),  // Only on 429
}
```

### Exponential Backoff

```typescript
private calculateRetryDelay(state: ConnectionState): number {
  const baseDelay = this.config.baseRetryDelayMs;
  const multiplier = Math.pow(2, Math.min(state.consecutiveRateLimits, 10));
  const delay = Math.min(baseDelay * multiplier, this.config.maxRetryDelayMs);
  // Add jitter to prevent thundering herd
  return Math.floor(delay * (0.5 + Math.random() * 0.5));
}
```

---

## WebSocket Security

### Connection Security

```typescript
// WebSocket upgrade validation
async function handleWebSocketUpgrade(request: Request): Promise<Response> {
  // Validate upgrade header
  if (request.headers.get('Upgrade') !== 'websocket') {
    return new Response('Expected WebSocket', { status: 426 });
  }

  // Validate client ID
  const clientId = request.headers.get('X-Client-ID');
  if (!clientId) {
    return new Response('Missing X-Client-ID', { status: 400 });
  }

  // Rate limit check
  const rateLimitResult = rateLimiter.checkConnection(clientId, clientIp);
  if (!rateLimitResult.allowed) {
    return new Response('Rate limited', {
      status: 429,
      headers: rateLimiter.getRateLimitHeaders(rateLimitResult),
    });
  }

  // Accept connection
  const pair = new WebSocketPair();
  // ... setup handlers
  return new Response(null, { status: 101, webSocket: pair[0] });
}
```

### Message Validation

All WebSocket messages are validated before processing:

```typescript
private decodeMessage(message: ArrayBuffer | string): ValidatedClientRpcMessage {
  // Parse JSON
  const raw = typeof message === 'string'
    ? JSON.parse(message)
    : JSON.parse(new TextDecoder().decode(message));

  // Validate with Zod schema
  const result = ClientRpcMessageSchema.safeParse(raw);
  if (!result.success) {
    throw new MessageValidationError(
      `Invalid message format: ${result.error.issues.map(e => e.message).join(', ')}`,
      result.error
    );
  }

  return result.data;
}
```

### Connection State Management

```typescript
interface ConnectionState {
  messageBucket: TokenBucket;      // Rate limiting
  heartbeatBucket: TokenBucket;    // Separate bucket for heartbeats
  sizeViolationCount: number;      // Track size violations
  consecutiveRateLimits: number;   // For exponential backoff
  priority: 'high' | 'normal' | 'low';
  connectedAt: number;
}

// Close connections with repeated violations
shouldCloseConnection(connectionId: string): boolean {
  const state = this.connections.get(connectionId);
  return state && state.sizeViolationCount >= this.config.maxSizeViolations;
}
```

### Heartbeat and Keep-Alive

```typescript
// Heartbeats have separate, more lenient rate limits
const heartbeatBucket = this.createBucket(
  this.config.burstCapacity * 2,  // 2x capacity
  this.config.refillRate * 2       // 2x refill rate
);

// Heartbeat validation
const HeartbeatMessageSchema = z.object({
  type: z.literal('heartbeat'),
  timestamp: z.number().int(),
  sourceDoId: z.string().min(1),
  lastAckSequence: z.number().int(),
  pendingEvents: z.number().int(),
});
```

---

## Secrets Management

### Environment Variables

Sensitive configuration is managed through environment variables:

```typescript
// Never hardcode secrets
// Use environment bindings instead
interface Env {
  DATABASE_URL: string;      // Connection string
  API_SECRET: string;        // API authentication
  ENCRYPTION_KEY: string;    // Data encryption
}
```

### Secret Rotation

- Secrets can be rotated without service restart
- Old secrets remain valid during rotation window
- Atomic secret updates via Cloudflare dashboard or API

### Credential Files

Files that may contain secrets are excluded from commits:

```
# .gitignore
.env
.env.local
.env.*.local
credentials.json
*.pem
*.key
```

### KV-Based Secrets

For dynamic secrets:

```typescript
// Fetch secret from KV (encrypted at rest)
const apiKey = await env.SECRETS_KV.get('API_KEY');

// Secrets are never logged
// Secrets are never returned in error messages
```

---

## Audit Logging

### Event Logging

The system logs security-relevant events:

```typescript
interface AuditEvent {
  timestamp: number;
  eventType: 'auth' | 'query' | 'admin' | 'error';
  action: string;
  actor: {
    clientId: string;
    ip?: string;
    userAgent?: string;
  };
  resource: {
    type: string;
    identifier: string;
  };
  outcome: 'success' | 'failure';
  details?: Record<string, unknown>;
}
```

### Logged Events

| Event Category | Events |
|---------------|--------|
| Authentication | Login, logout, token refresh, failed auth |
| Authorization | Permission granted, denied, role change |
| Data Access | Query execution, data modification |
| Configuration | Schema changes, settings updates |
| Security | Rate limiting, injection attempts, validation failures |

### Rate Limiting Metrics

```typescript
export interface RateLimitMetrics {
  connectionsRateLimited: number;
  messagesRateLimited: number;
  payloadRejections: number;
  eventSizeRejections: number;
  connectionsClosed: number;
  ipRejections: number;
  loadSheddingEvents: number;
  activeConnections: number;
  peakConnections: number;
}
```

### Log Security

- Logs do not contain sensitive data (passwords, tokens, PII)
- IP addresses are masked in logs: `192.168.1.xxx`
- Log retention follows compliance requirements
- Logs are immutable once written

---

## Compliance Considerations

### Data Residency

- Data can be restricted to specific geographic regions
- R2 storage supports regional buckets
- Durable Objects support location hints

### GDPR Compliance

- Data deletion on request (right to erasure)
- Data export capability (data portability)
- Consent tracking for data processing
- Privacy by design principles

### SOC 2 Controls

| Control | Implementation |
|---------|----------------|
| Access Control | RBAC, authentication |
| Encryption | TLS 1.3, AES-256 at rest |
| Logging | Comprehensive audit logs |
| Monitoring | Rate limiting metrics |
| Availability | Geographic distribution |

### PCI DSS Considerations

- No storage of cardholder data by default
- Strong cryptography for data in transit
- Access control and authentication
- Regular security testing

---

## Security Best Practices

### For Developers

1. **Always use parameterized queries**
   ```typescript
   // Good
   db.query('SELECT * FROM users WHERE id = ?', [userId]);

   // Bad - never do this
   db.query(`SELECT * FROM users WHERE id = ${userId}`);
   ```

2. **Validate all input**
   ```typescript
   const validated = validateClientMessage(rawInput);
   // Only use validated data
   ```

3. **Handle errors securely**
   ```typescript
   try {
     await processRequest(data);
   } catch (error) {
     // Log full error internally
     console.error('Internal error:', error);
     // Return sanitized error to client
     return { error: 'Request failed' };
   }
   ```

4. **Never expose internal details**
   - Don't return stack traces to clients
   - Don't reveal database structure in errors
   - Don't include sensitive data in logs

### For Operators

1. **Enable all security features**
   - Rate limiting
   - Input validation
   - TLS enforcement

2. **Monitor security metrics**
   - Rate limiting events
   - Authentication failures
   - Validation errors

3. **Regular security reviews**
   - Review access permissions
   - Audit API token usage
   - Check for unusual patterns

4. **Keep dependencies updated**
   - Monitor security advisories
   - Apply patches promptly
   - Test updates in staging

### For API Consumers

1. **Protect API tokens**
   - Never commit tokens to code
   - Rotate tokens regularly
   - Use short-lived tokens when possible

2. **Implement retry logic**
   - Respect `Retry-After` headers
   - Use exponential backoff
   - Don't retry 4xx errors (except 429)

3. **Validate responses**
   - Don't trust server responses blindly
   - Validate data types and ranges
   - Handle unexpected responses gracefully

---

## Threat Model

### Assets to Protect

| Asset | Impact if Compromised |
|-------|----------------------|
| Database content | Data breach, regulatory fines |
| API credentials | Unauthorized access |
| Query history | Privacy violation |
| System configuration | Service disruption |

### Threat Actors

| Actor | Motivation | Capability |
|-------|-----------|------------|
| External attacker | Data theft, disruption | Medium-High |
| Malicious insider | Data theft, sabotage | High |
| Automated bot | Resource abuse | Low-Medium |
| Competitor | Business intelligence | Medium |

### Attack Vectors

| Vector | Mitigation |
|--------|-----------|
| SQL injection | Parameterized queries, input validation |
| DoS/DDoS | Rate limiting, geographic distribution |
| Credential theft | Token rotation, short expiry |
| Man-in-the-middle | TLS 1.3, certificate pinning |
| Privilege escalation | RBAC, least privilege |
| Data exfiltration | Access logging, anomaly detection |

### Risk Matrix

| Threat | Likelihood | Impact | Risk | Mitigation Status |
|--------|-----------|--------|------|-------------------|
| SQL injection | Low | Critical | Medium | Implemented |
| DoS attack | Medium | High | High | Implemented |
| Auth bypass | Low | Critical | Medium | Implemented |
| Data breach | Low | Critical | Medium | Implemented |
| Insider threat | Low | High | Medium | Partial |

---

## Security Guarantees

### What We Guarantee

1. **Query Safety**: All SQL queries are parameterized; direct string interpolation is never used for user input

2. **Input Validation**: All external input is validated against strict schemas before processing

3. **Rate Limiting**: All endpoints have rate limiting to prevent abuse

4. **Transport Security**: All communication uses TLS 1.3

5. **Data Encryption**: All data at rest is encrypted using AES-256

6. **Isolation**: Each tenant's data is isolated at the storage and compute levels

7. **Audit Trail**: All security-relevant events are logged

### What We Don't Guarantee

1. **Application-Level Security**: Security of custom application code built on top of the SQL system

2. **Client-Side Security**: Security of client applications or how they handle credentials

3. **Third-Party Integrations**: Security of external services or APIs

4. **Physical Security**: Physical security of end-user devices

### Security Reporting

To report a security vulnerability:

1. **Do not** create a public issue
2. Email security concerns to the appropriate security contact
3. Include detailed reproduction steps
4. Allow reasonable time for response and fix

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2026-01-22 | Initial security model documentation |

---

## References

- [OWASP SQL Injection Prevention Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/SQL_Injection_Prevention_Cheat_Sheet.html)
- [Cloudflare Security Documentation](https://developers.cloudflare.com/fundamentals/security/)
- [NIST Cybersecurity Framework](https://www.nist.gov/cyberframework)
- [CWE Top 25 Software Weaknesses](https://cwe.mitre.org/top25/)
