/**
 * Router Middleware Tests
 *
 * Tests for the complete DoSQL router:
 * - HTTP request routing (GET, POST to various endpoints)
 * - Health check endpoint
 * - REST API endpoints (/query, /exec, /transaction)
 * - RPC endpoint integration
 * - Auth middleware integration
 * - Configuration options
 *
 * @packageDocumentation
 */

import { describe, it, expect, vi } from 'vitest';
import { createDoSQLRouter } from '../router.js';
import type { DoSQLRouterConfig } from '../router.js';
import type { AuthConfig } from '../auth.js';
import type { RPCConfig } from '../rpc.js';

// =============================================================================
// Mock DO Stub Factory
// =============================================================================

interface MockDOStub {
  fetch: ReturnType<typeof vi.fn>;
}

function createMockDOStub(responses: Map<string, unknown> = new Map()): MockDOStub {
  return {
    fetch: vi.fn().mockImplementation(async (request: Request) => {
      const body = await request.json() as { method: string; params: unknown[] };
      const result = responses.get(body.method);

      if (result instanceof Error) {
        return new Response(JSON.stringify({ error: result.message }), {
          status: 500,
          headers: { 'Content-Type': 'application/json' },
        });
      }

      return new Response(JSON.stringify(result ?? { ok: true }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      });
    }),
  };
}

// =============================================================================
// Mock DO Namespace
// =============================================================================

function createMockNamespace(stub: MockDOStub): DurableObjectNamespace {
  return {
    idFromName: vi.fn().mockReturnValue({ toString: () => 'mock-id' }),
    idFromString: vi.fn().mockReturnValue({ toString: () => 'mock-id' }),
    newUniqueId: vi.fn().mockReturnValue({ toString: () => 'unique-id' }),
    get: vi.fn().mockReturnValue(stub),
    jurisdiction: vi.fn().mockReturnValue({
      idFromName: vi.fn().mockReturnValue({ toString: () => 'mock-id' }),
      idFromString: vi.fn().mockReturnValue({ toString: () => 'mock-id' }),
      newUniqueId: vi.fn().mockReturnValue({ toString: () => 'unique-id' }),
      get: vi.fn().mockReturnValue(stub),
    }),
  } as unknown as DurableObjectNamespace;
}

// =============================================================================
// Health Check Endpoint Tests
// =============================================================================

describe('DoSQL Router - Health Check Endpoint', () => {
  it('should respond to GET /health with status ok', async () => {
    const stub = createMockDOStub(new Map([['ping', { pong: true }]]));
    const namespace = createMockNamespace(stub);

    const router = createDoSQLRouter({
      rpc: { doBinding: 'SQL_DO' },
      health: true,
    });

    const request = new Request('https://example.com/health', { method: 'GET' });
    const response = await router.fetch(request, { SQL_DO: namespace });

    expect(response.status).toBe(200);
    const body = await response.json() as {
      status: string;
      service: string;
      durable_object: string;
      timestamp: number;
    };
    expect(body.status).toBe('ok');
    expect(body.service).toBe('dosql');
    expect(body.timestamp).toBeGreaterThan(0);
  });

  it('should report DO status as unavailable when ping fails', async () => {
    const stub = createMockDOStub();
    stub.fetch.mockRejectedValue(new Error('DO unavailable'));
    const namespace = createMockNamespace(stub);

    const router = createDoSQLRouter({
      rpc: { doBinding: 'SQL_DO' },
      health: true,
    });

    const request = new Request('https://example.com/health', { method: 'GET' });
    const response = await router.fetch(request, { SQL_DO: namespace });

    const body = await response.json() as { durable_object: string };
    expect(body.durable_object).toBe('unavailable');
  });

  it('should not expose /health when health option is false', async () => {
    const stub = createMockDOStub();
    const namespace = createMockNamespace(stub);

    const router = createDoSQLRouter({
      rpc: { doBinding: 'SQL_DO' },
      health: false,
    });

    const request = new Request('https://example.com/health', { method: 'GET' });
    const response = await router.fetch(request, { SQL_DO: namespace });

    // Should get 404 or route to next handler
    expect(response.status).toBe(404);
  });
});

// =============================================================================
// REST API Endpoint Tests
// =============================================================================

describe('DoSQL Router - REST API Endpoints', () => {
  describe('POST /query', () => {
    it('should execute SELECT query', async () => {
      const stub = createMockDOStub(new Map([
        ['exec', { columns: ['id', 'name'], rows: [[1, 'Alice']] }],
      ]));
      const namespace = createMockNamespace(stub);

      const router = createDoSQLRouter({
        rpc: { doBinding: 'SQL_DO' },
        rest: true,
      });

      const request = new Request('https://example.com/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: 'SELECT * FROM users' }),
      });

      const response = await router.fetch(request, { SQL_DO: namespace });

      expect(response.status).toBe(200);
      const body = await response.json() as { columns: string[]; rows: unknown[][] };
      expect(body.columns).toEqual(['id', 'name']);
      expect(body.rows).toEqual([[1, 'Alice']]);
    });

    it('should pass params to query', async () => {
      const stub = createMockDOStub(new Map([
        ['exec', { columns: ['id'], rows: [[1]] }],
      ]));
      const namespace = createMockNamespace(stub);

      const router = createDoSQLRouter({
        rpc: { doBinding: 'SQL_DO' },
        rest: true,
      });

      const request = new Request('https://example.com/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'SELECT * FROM users WHERE id = ?',
          params: [1],
        }),
      });

      const response = await router.fetch(request, { SQL_DO: namespace });

      expect(response.status).toBe(200);
      expect(stub.fetch).toHaveBeenCalled();
    });

    it('should return error for missing sql field', async () => {
      const stub = createMockDOStub();
      const namespace = createMockNamespace(stub);

      const router = createDoSQLRouter({
        rpc: { doBinding: 'SQL_DO' },
        rest: true,
      });

      const request = new Request('https://example.com/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ params: [1] }), // Missing sql
      });

      const response = await router.fetch(request, { SQL_DO: namespace });

      expect(response.status).toBe(400);
      const body = await response.json() as { error: string };
      expect(body.error).toContain('sql');
    });

    it('should return error for invalid JSON body', async () => {
      const stub = createMockDOStub();
      const namespace = createMockNamespace(stub);

      const router = createDoSQLRouter({
        rpc: { doBinding: 'SQL_DO' },
        rest: true,
      });

      const request = new Request('https://example.com/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: 'not json',
      });

      const response = await router.fetch(request, { SQL_DO: namespace });

      expect(response.status).toBe(400);
      const body = await response.json() as { error: string };
      expect(body.error).toContain('Invalid JSON');
    });

    it('should use db parameter for database selection', async () => {
      const stub = createMockDOStub(new Map([
        ['exec', { columns: [], rows: [] }],
      ]));
      const namespace = createMockNamespace(stub);

      const router = createDoSQLRouter({
        rpc: { doBinding: 'SQL_DO' },
        rest: true,
      });

      const request = new Request('https://example.com/query', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'SELECT 1',
          db: 'custom-database',
        }),
      });

      await router.fetch(request, { SQL_DO: namespace });

      expect(namespace.idFromName).toHaveBeenCalledWith('custom-database');
    });
  });

  describe('POST /exec', () => {
    it('should execute INSERT/UPDATE/DELETE', async () => {
      const stub = createMockDOStub(new Map([
        ['exec', { rowCount: 1, lastRowId: 42 }],
      ]));
      const namespace = createMockNamespace(stub);

      const router = createDoSQLRouter({
        rpc: { doBinding: 'SQL_DO' },
        rest: true,
      });

      const request = new Request('https://example.com/exec', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          sql: 'INSERT INTO users (name) VALUES (?)',
          params: ['Bob'],
        }),
      });

      const response = await router.fetch(request, { SQL_DO: namespace });

      expect(response.status).toBe(200);
      const body = await response.json() as { rowCount: number };
      expect(body.rowCount).toBe(1);
    });

    it('should return error for missing sql field', async () => {
      const stub = createMockDOStub();
      const namespace = createMockNamespace(stub);

      const router = createDoSQLRouter({
        rpc: { doBinding: 'SQL_DO' },
        rest: true,
      });

      const request = new Request('https://example.com/exec', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      });

      const response = await router.fetch(request, { SQL_DO: namespace });

      expect(response.status).toBe(400);
    });
  });

  describe('POST /transaction', () => {
    it('should execute transaction with multiple operations', async () => {
      const stub = createMockDOStub(new Map([
        ['transaction', { results: [{ rowCount: 1 }, { rowCount: 1 }] }],
      ]));
      const namespace = createMockNamespace(stub);

      const router = createDoSQLRouter({
        rpc: { doBinding: 'SQL_DO' },
        rest: true,
      });

      const request = new Request('https://example.com/transaction', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ops: [
            { type: 'exec', sql: 'INSERT INTO users VALUES (1)' },
            { type: 'exec', sql: 'INSERT INTO logs VALUES (1)' },
          ],
        }),
      });

      const response = await router.fetch(request, { SQL_DO: namespace });

      expect(response.status).toBe(200);
    });

    it('should return error for missing ops field', async () => {
      const stub = createMockDOStub();
      const namespace = createMockNamespace(stub);

      const router = createDoSQLRouter({
        rpc: { doBinding: 'SQL_DO' },
        rest: true,
      });

      const request = new Request('https://example.com/transaction', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sql: 'SELECT 1' }), // Missing ops
      });

      const response = await router.fetch(request, { SQL_DO: namespace });

      expect(response.status).toBe(400);
      const body = await response.json() as { error: string };
      expect(body.error).toContain('ops');
    });

    it('should return error for non-array ops', async () => {
      const stub = createMockDOStub();
      const namespace = createMockNamespace(stub);

      const router = createDoSQLRouter({
        rpc: { doBinding: 'SQL_DO' },
        rest: true,
      });

      const request = new Request('https://example.com/transaction', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ops: 'not an array' }),
      });

      const response = await router.fetch(request, { SQL_DO: namespace });

      expect(response.status).toBe(400);
    });
  });

  describe('REST disabled', () => {
    it('should not expose REST endpoints when rest option is false', async () => {
      const stub = createMockDOStub();
      const namespace = createMockNamespace(stub);

      const router = createDoSQLRouter({
        rpc: { doBinding: 'SQL_DO' },
        rest: false,
      });

      const endpoints = ['/query', '/exec', '/transaction'];

      for (const endpoint of endpoints) {
        const request = new Request(`https://example.com${endpoint}`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ sql: 'SELECT 1' }),
        });

        const response = await router.fetch(request, { SQL_DO: namespace });

        expect(response.status).toBe(404);
      }
    });
  });
});

// =============================================================================
// RPC Endpoint Tests
// =============================================================================

describe('DoSQL Router - RPC Endpoint', () => {
  it('should handle JSON-RPC request at POST /rpc', async () => {
    const stub = createMockDOStub(new Map([['ping', { pong: true }]]));
    const namespace = createMockNamespace(stub);

    const router = createDoSQLRouter({
      rpc: { doBinding: 'SQL_DO' },
    });

    const request = new Request('https://example.com/rpc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        method: 'ping',
        id: 1,
      }),
    });

    const response = await router.fetch(request, { SQL_DO: namespace });

    expect(response.status).toBe(200);
    const body = await response.json() as { jsonrpc: string; result: unknown; id: number };
    expect(body.jsonrpc).toBe('2.0');
    expect(body.result).toEqual({ pong: true });
  });

  it('should return protocol info at GET /rpc without websocket upgrade', async () => {
    const stub = createMockDOStub();
    const namespace = createMockNamespace(stub);

    const router = createDoSQLRouter({
      rpc: { doBinding: 'SQL_DO' },
      websocket: true,
    });

    const request = new Request('https://example.com/rpc', { method: 'GET' });
    const response = await router.fetch(request, { SQL_DO: namespace });

    expect(response.status).toBe(200);
    const body = await response.json() as { protocol: string; transport: string[] };
    expect(body.protocol).toBe('rpc.do');
    expect(body.transport).toContain('http-post');
    expect(body.transport).toContain('websocket');
  });

  it('should list methods at GET /rpc/methods', async () => {
    const stub = createMockDOStub();
    const namespace = createMockNamespace(stub);

    const router = createDoSQLRouter({
      rpc: {
        doBinding: 'SQL_DO',
        allowedMethods: ['exec', 'query', 'ping'],
      },
    });

    const request = new Request('https://example.com/rpc/methods', { method: 'GET' });
    const response = await router.fetch(request, { SQL_DO: namespace });

    expect(response.status).toBe(200);
    const body = await response.json() as { methods: string[] };
    expect(body.methods).toContain('exec');
    expect(body.methods).toContain('query');
    expect(body.methods).toContain('ping');
  });

  it('should handle batch RPC request', async () => {
    const stub = createMockDOStub(new Map([
      ['ping', { pong: true }],
      ['status', { ready: true }],
    ]));
    const namespace = createMockNamespace(stub);

    const router = createDoSQLRouter({
      rpc: { doBinding: 'SQL_DO' },
    });

    const request = new Request('https://example.com/rpc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify([
        { jsonrpc: '2.0', method: 'ping', id: 1 },
        { jsonrpc: '2.0', method: 'status', id: 2 },
      ]),
    });

    const response = await router.fetch(request, { SQL_DO: namespace });

    expect(response.status).toBe(200);
    const body = await response.json() as Array<{ id: number }>;
    expect(body).toHaveLength(2);
  });
});

// =============================================================================
// Auth Integration Tests
// =============================================================================

describe('DoSQL Router - Auth Integration', () => {
  it('should apply auth middleware when configured', async () => {
    const stub = createMockDOStub(new Map([['ping', { pong: true }]]));
    const namespace = createMockNamespace(stub);

    const router = createDoSQLRouter({
      auth: {
        verifyToken: vi.fn().mockResolvedValue({ userId: 'test-user' }),
      },
      rpc: { doBinding: 'SQL_DO' },
    });

    // Without auth header
    const unauthRequest = new Request('https://example.com/rpc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ jsonrpc: '2.0', method: 'ping', id: 1 }),
    });

    const unauthResponse = await router.fetch(unauthRequest, { SQL_DO: namespace });
    expect(unauthResponse.status).toBe(401);

    // With auth header
    const authRequest = new Request('https://example.com/rpc', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer valid-token',
      },
      body: JSON.stringify({ jsonrpc: '2.0', method: 'ping', id: 1 }),
    });

    const authResponse = await router.fetch(authRequest, { SQL_DO: namespace });
    expect(authResponse.status).toBe(200);
  });

  it('should skip auth for public paths', async () => {
    const stub = createMockDOStub(new Map([['ping', { pong: true }]]));
    const namespace = createMockNamespace(stub);

    const verifyToken = vi.fn();
    const router = createDoSQLRouter({
      auth: {
        verifyToken,
        publicPaths: ['/health'],
      },
      rpc: { doBinding: 'SQL_DO' },
      health: true,
    });

    const request = new Request('https://example.com/health', { method: 'GET' });
    const response = await router.fetch(request, { SQL_DO: namespace });

    expect(response.status).toBe(200);
    expect(verifyToken).not.toHaveBeenCalled();
  });
});

// =============================================================================
// Error Handling Tests
// =============================================================================

describe('DoSQL Router - Error Handling', () => {
  it('should return 500 when DO is unavailable for REST endpoint', async () => {
    const router = createDoSQLRouter({
      rpc: { doBinding: 'SQL_DO' },
      rest: true,
    });

    const request = new Request('https://example.com/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'SELECT 1' }),
    });

    // No SQL_DO binding in env
    const response = await router.fetch(request, {});

    expect(response.status).toBe(500);
    const body = await response.json() as { error: string };
    expect(body.error).toContain('not available');
  });

  it('should return DO error details in REST response', async () => {
    const stub = createMockDOStub();
    stub.fetch.mockResolvedValue(new Response('Syntax error', { status: 400 }));
    const namespace = createMockNamespace(stub);

    const router = createDoSQLRouter({
      rpc: { doBinding: 'SQL_DO' },
      rest: true,
    });

    const request = new Request('https://example.com/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'INVALID SQL' }),
    });

    const response = await router.fetch(request, { SQL_DO: namespace });

    expect(response.status).toBe(500);
    const body = await response.json() as { error: string };
    expect(body.error).toBeDefined();
  });
});

// =============================================================================
// Configuration Tests
// =============================================================================

describe('DoSQL Router - Configuration', () => {
  it('should use default configuration when minimal config provided', async () => {
    const stub = createMockDOStub(new Map([['ping', { pong: true }]]));
    const namespace = createMockNamespace(stub);

    const router = createDoSQLRouter({
      rpc: { doBinding: 'SQL_DO' },
    });

    // Should have REST enabled by default
    const restRequest = new Request('https://example.com/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: 'SELECT 1' }),
    });
    const restResponse = await router.fetch(restRequest, { SQL_DO: namespace });
    expect(restResponse.status).toBe(200);

    // Should have health enabled by default
    const healthRequest = new Request('https://example.com/health', { method: 'GET' });
    const healthResponse = await router.fetch(healthRequest, { SQL_DO: namespace });
    expect(healthResponse.status).toBe(200);
  });

  it('should respect custom RPC config', async () => {
    const stub = createMockDOStub(new Map([['customMethod', { ok: true }]]));
    const namespace = createMockNamespace(stub);

    const router = createDoSQLRouter({
      rpc: {
        doBinding: 'CUSTOM_DO',
        allowedMethods: ['customMethod'],
        maxBatchSize: 5,
      },
    });

    const request = new Request('https://example.com/rpc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        method: 'customMethod',
        id: 1,
      }),
    });

    const response = await router.fetch(request, { CUSTOM_DO: namespace });

    expect(response.status).toBe(200);
  });

  it('should support custom resolveId function', async () => {
    const stub = createMockDOStub(new Map([['ping', { pong: true }]]));
    const namespace = createMockNamespace(stub);

    const router = createDoSQLRouter({
      rpc: {
        doBinding: 'SQL_DO',
        resolveId: (c) => c.req.header('X-Tenant-Id') ?? 'default-tenant',
      },
    });

    const request = new Request('https://example.com/rpc', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Tenant-Id': 'tenant-abc',
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        method: 'ping',
        id: 1,
      }),
    });

    await router.fetch(request, { SQL_DO: namespace });

    expect(namespace.idFromName).toHaveBeenCalledWith('tenant-abc');
  });
});

// =============================================================================
// WebSocket Tests
// =============================================================================

describe('DoSQL Router - WebSocket', () => {
  it('should forward WebSocket upgrade to DO when websocket enabled', async () => {
    const stub = createMockDOStub();
    // Note: Response constructor doesn't accept status 101, so we use 200
    // and verify the stub was called with the correct upgrade request
    stub.fetch.mockResolvedValue(new Response(null, { status: 200 }));
    const namespace = createMockNamespace(stub);

    const router = createDoSQLRouter({
      rpc: { doBinding: 'SQL_DO' },
      websocket: true,
    });

    const request = new Request('https://example.com/rpc', {
      method: 'GET',
      headers: { Upgrade: 'websocket' },
    });

    const response = await router.fetch(request, { SQL_DO: namespace });

    // Verify the WebSocket upgrade was forwarded to the DO stub
    expect(stub.fetch).toHaveBeenCalled();
    const calledRequest = stub.fetch.mock.calls[0][0] as Request;
    expect(calledRequest.headers.get('upgrade')).toBe('websocket');
    expect(response.status).toBe(200);
  });

  it('should not handle WebSocket when websocket disabled', async () => {
    const stub = createMockDOStub();
    const namespace = createMockNamespace(stub);

    const router = createDoSQLRouter({
      rpc: { doBinding: 'SQL_DO' },
      websocket: false,
    });

    const request = new Request('https://example.com/rpc', {
      method: 'GET',
      headers: { Upgrade: 'websocket' },
    });

    const response = await router.fetch(request, { SQL_DO: namespace });

    // Should return 404 or not handle the upgrade
    expect(response.status).not.toBe(101);
  });
});

// =============================================================================
// Edge Cases
// =============================================================================

describe('DoSQL Router - Edge Cases', () => {
  it('should handle empty request body gracefully', async () => {
    const stub = createMockDOStub();
    const namespace = createMockNamespace(stub);

    const router = createDoSQLRouter({
      rpc: { doBinding: 'SQL_DO' },
      rest: true,
    });

    const request = new Request('https://example.com/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '',
    });

    const response = await router.fetch(request, { SQL_DO: namespace });

    expect(response.status).toBe(400);
  });

  it('should handle concurrent requests', async () => {
    const stub = createMockDOStub(new Map([
      ['exec', { columns: [], rows: [] }],
    ]));
    const namespace = createMockNamespace(stub);

    const router = createDoSQLRouter({
      rpc: { doBinding: 'SQL_DO' },
      rest: true,
    });

    const requests = Array.from({ length: 10 }, (_, i) =>
      router.fetch(
        new Request('https://example.com/query', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ sql: `SELECT ${i}` }),
        }),
        { SQL_DO: namespace }
      )
    );

    const responses = await Promise.all(requests);

    expect(responses.every(r => r.status === 200)).toBe(true);
  });

  it('should handle very long SQL queries', async () => {
    const stub = createMockDOStub(new Map([
      ['exec', { columns: [], rows: [] }],
    ]));
    const namespace = createMockNamespace(stub);

    const router = createDoSQLRouter({
      rpc: { doBinding: 'SQL_DO' },
      rest: true,
    });

    const longSql = 'SELECT ' + Array.from({ length: 1000 }, (_, i) => `col${i}`).join(', ') + ' FROM table1';

    const request = new Request('https://example.com/query', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ sql: longSql }),
    });

    const response = await router.fetch(request, { SQL_DO: namespace });

    expect(response.status).toBe(200);
  });
});
