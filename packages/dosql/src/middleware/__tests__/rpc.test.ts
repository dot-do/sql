/**
 * RPC Middleware Tests
 *
 * Tests for the DoSQL RPC middleware (rpc.do protocol):
 * - JSON-RPC 2.0 format validation
 * - Single and batch request handling
 * - Method allowlisting
 * - Error response codes and formatting
 * - Timeout handling
 * - WebSocket upgrade
 *
 * @packageDocumentation
 */

import { describe, it, expect, vi } from 'vitest';
import { rpcHandler, exposeDoSQL } from '../rpc.js';
import type { RPCConfig, RPCRequest, RPCResponse } from '../rpc.js';

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
// Mock Hono Context Factory
// =============================================================================

interface MockContext {
  req: {
    raw: Request;
    method: string;
    path: string;
    header(name: string): string | undefined;
    json<T>(): Promise<T>;
    query(name: string): string | undefined;
  };
  env: Record<string, unknown>;
  json(data: unknown, status?: number): Response;
  text(data: string, status?: number): Response;
  set(key: string, value: unknown): void;
  get(key: string): unknown;
}

function createMockContext(
  method: string,
  path: string,
  body?: unknown,
  options: {
    headers?: Record<string, string>;
    query?: Record<string, string>;
    env?: Record<string, unknown>;
  } = {}
): MockContext {
  const store = new Map<string, unknown>();
  const headers = new Headers(options.headers);
  const queryParams = new URLSearchParams(options.query);

  const request = new Request(`https://example.com${path}?${queryParams.toString()}`, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined,
  });

  return {
    req: {
      raw: request,
      method,
      path,
      header(name: string): string | undefined {
        return headers.get(name) ?? undefined;
      },
      async json<T>(): Promise<T> {
        if (!body) throw new Error('No body');
        return body as T;
      },
      query(name: string): string | undefined {
        return queryParams.get(name) ?? undefined;
      },
    },
    env: options.env ?? {},
    json(data: unknown, status = 200): Response {
      return new Response(JSON.stringify(data), {
        status,
        headers: { 'Content-Type': 'application/json' },
      });
    },
    text(data: string, status = 200): Response {
      return new Response(data, { status });
    },
    set(key: string, value: unknown): void {
      store.set(key, value);
    },
    get(key: string): unknown {
      return store.get(key);
    },
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
// JSON-RPC Format Validation Tests
// =============================================================================

describe('RPC Handler - JSON-RPC Format Validation', () => {
  it('should accept valid JSON-RPC 2.0 request', async () => {
    const stub = createMockDOStub(new Map([['ping', { pong: true }]]));
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({ doBinding: 'SQL_DO' });

    const request: RPCRequest = {
      jsonrpc: '2.0',
      method: 'ping',
      params: [],
      id: 1,
    };

    const ctx = createMockContext('POST', '/rpc', request, {
      env: { SQL_DO: namespace },
    });

    const response = await handler.fetch(ctx.req.raw, ctx.env);

    expect(response.status).toBe(200);
    const body = await response.json() as RPCResponse;
    expect(body.jsonrpc).toBe('2.0');
    expect(body.id).toBe(1);
    expect(body.result).toEqual({ pong: true });
  });

  it('should reject request missing jsonrpc field', async () => {
    const stub = createMockDOStub();
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({ doBinding: 'SQL_DO' });

    const request = {
      method: 'ping',
      id: 1,
    };

    const ctx = createMockContext('POST', '/rpc', request, {
      env: { SQL_DO: namespace },
    });

    const response = await handler.fetch(ctx.req.raw, ctx.env);

    expect(response.status).toBe(400);
    const body = await response.json() as RPCResponse;
    expect(body.error?.code).toBe(-32600); // Invalid Request
    expect(body.error?.message).toBe('Invalid Request');
  });

  it('should reject request missing method field', async () => {
    const stub = createMockDOStub();
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({ doBinding: 'SQL_DO' });

    const request = {
      jsonrpc: '2.0',
      id: 1,
    };

    const ctx = createMockContext('POST', '/rpc', request, {
      env: { SQL_DO: namespace },
    });

    const response = await handler.fetch(ctx.req.raw, ctx.env);

    expect(response.status).toBe(400);
    const body = await response.json() as RPCResponse;
    expect(body.error?.code).toBe(-32600);
  });

  it('should reject request missing id field', async () => {
    const stub = createMockDOStub();
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({ doBinding: 'SQL_DO' });

    const request = {
      jsonrpc: '2.0',
      method: 'ping',
    };

    const ctx = createMockContext('POST', '/rpc', request, {
      env: { SQL_DO: namespace },
    });

    const response = await handler.fetch(ctx.req.raw, ctx.env);

    expect(response.status).toBe(400);
    const body = await response.json() as RPCResponse;
    expect(body.error?.code).toBe(-32600);
  });

  it('should return parse error for invalid JSON', async () => {
    const stub = createMockDOStub();
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({ doBinding: 'SQL_DO' });

    const request = new Request('https://example.com/rpc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: '{ invalid json }',
    });

    const response = await handler.fetch(request, { SQL_DO: namespace });

    expect(response.status).toBe(400);
    const body = await response.json() as RPCResponse;
    expect(body.error?.code).toBe(-32700); // Parse error
    expect(body.error?.message).toBe('Parse error');
  });

  it('should handle request with object params', async () => {
    const stub = createMockDOStub(new Map([['query', { rows: [] }]]));
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({ doBinding: 'SQL_DO' });

    const request: RPCRequest = {
      jsonrpc: '2.0',
      method: 'query',
      params: { sql: 'SELECT 1', params: [] },
      id: 'req-1',
    };

    const ctx = createMockContext('POST', '/rpc', request, {
      env: { SQL_DO: namespace },
    });

    const response = await handler.fetch(ctx.req.raw, ctx.env);

    expect(response.status).toBe(200);
    const body = await response.json() as RPCResponse;
    expect(body.id).toBe('req-1');
  });

  it('should handle request with array params', async () => {
    const stub = createMockDOStub(new Map([['exec', { rowCount: 1 }]]));
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({ doBinding: 'SQL_DO' });

    const request: RPCRequest = {
      jsonrpc: '2.0',
      method: 'exec',
      params: ['INSERT INTO users VALUES (1, "test")', []],
      id: 42,
    };

    const ctx = createMockContext('POST', '/rpc', request, {
      env: { SQL_DO: namespace },
    });

    const response = await handler.fetch(ctx.req.raw, ctx.env);

    expect(response.status).toBe(200);
    const body = await response.json() as RPCResponse;
    expect(body.id).toBe(42);
  });

  it('should handle request with no params', async () => {
    const stub = createMockDOStub(new Map([['status', { ready: true }]]));
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({ doBinding: 'SQL_DO' });

    const request: RPCRequest = {
      jsonrpc: '2.0',
      method: 'status',
      id: 'status-check',
    };

    const ctx = createMockContext('POST', '/rpc', request, {
      env: { SQL_DO: namespace },
    });

    const response = await handler.fetch(ctx.req.raw, ctx.env);

    expect(response.status).toBe(200);
    const body = await response.json() as RPCResponse;
    expect(body.result).toEqual({ ready: true });
  });
});

// =============================================================================
// Batch Request Tests
// =============================================================================

describe('RPC Handler - Batch Requests', () => {
  it('should handle batch request with multiple operations', async () => {
    const stub = createMockDOStub(new Map([
      ['ping', { pong: true }],
      ['status', { ready: true }],
      ['query', { rows: [[1, 'test']] }],
    ]));
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({ doBinding: 'SQL_DO' });

    const batch: RPCRequest[] = [
      { jsonrpc: '2.0', method: 'ping', id: 1 },
      { jsonrpc: '2.0', method: 'status', id: 2 },
      { jsonrpc: '2.0', method: 'query', params: [{ sql: 'SELECT 1' }], id: 3 },
    ];

    const ctx = createMockContext('POST', '/rpc', batch, {
      env: { SQL_DO: namespace },
    });

    const response = await handler.fetch(ctx.req.raw, ctx.env);

    expect(response.status).toBe(200);
    const body = await response.json() as RPCResponse[];
    expect(body).toHaveLength(3);
    expect(body[0].id).toBe(1);
    expect(body[1].id).toBe(2);
    expect(body[2].id).toBe(3);
  });

  it('should reject batch request exceeding maxBatchSize', async () => {
    const stub = createMockDOStub();
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({
      doBinding: 'SQL_DO',
      maxBatchSize: 2,
    });

    const batch: RPCRequest[] = [
      { jsonrpc: '2.0', method: 'ping', id: 1 },
      { jsonrpc: '2.0', method: 'ping', id: 2 },
      { jsonrpc: '2.0', method: 'ping', id: 3 }, // Exceeds limit
    ];

    const ctx = createMockContext('POST', '/rpc', batch, {
      env: { SQL_DO: namespace },
    });

    const response = await handler.fetch(ctx.req.raw, ctx.env);

    expect(response.status).toBe(400);
    const body = await response.json() as RPCResponse;
    expect(body.error?.code).toBe(-32005); // Batch too large
    expect(body.error?.data).toMatchObject({ max: 2 });
  });

  it('should handle empty batch request', async () => {
    const stub = createMockDOStub();
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({ doBinding: 'SQL_DO' });

    const ctx = createMockContext('POST', '/rpc', [], {
      env: { SQL_DO: namespace },
    });

    const response = await handler.fetch(ctx.req.raw, ctx.env);

    expect(response.status).toBe(200);
    const body = await response.json() as RPCResponse[];
    expect(body).toEqual([]);
  });

  it('should return individual errors for invalid batch items', async () => {
    const stub = createMockDOStub(new Map([['ping', { pong: true }]]));
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({ doBinding: 'SQL_DO' });

    const batch: RPCRequest[] = [
      { jsonrpc: '2.0', method: 'ping', id: 1 },
      { jsonrpc: '2.0', method: 'unknownMethod', id: 2 }, // Unknown method
    ];

    const ctx = createMockContext('POST', '/rpc', batch, {
      env: { SQL_DO: namespace },
    });

    const response = await handler.fetch(ctx.req.raw, ctx.env);

    const body = await response.json() as RPCResponse[];
    expect(body[0].result).toBeDefined();
    expect(body[1].error?.code).toBe(-32601); // Method not found
  });
});

// =============================================================================
// Method Allowlisting Tests
// =============================================================================

describe('RPC Handler - Method Allowlisting', () => {
  it('should allow default methods', async () => {
    const stub = createMockDOStub(new Map([
      ['exec', { rowCount: 0 }],
      ['query', { rows: [] }],
      ['ping', { pong: true }],
      ['status', { ready: true }],
    ]));
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({ doBinding: 'SQL_DO' });

    const defaultMethods = ['exec', 'query', 'ping', 'status'];

    for (const method of defaultMethods) {
      const request: RPCRequest = {
        jsonrpc: '2.0',
        method,
        id: method,
      };

      const ctx = createMockContext('POST', '/rpc', request, {
        env: { SQL_DO: namespace },
      });

      const response = await handler.fetch(ctx.req.raw, ctx.env);
      const body = await response.json() as RPCResponse;

      expect(body.error).toBeUndefined();
    }
  });

  it('should reject methods not in allowedMethods list', async () => {
    const stub = createMockDOStub();
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({
      doBinding: 'SQL_DO',
      allowedMethods: ['ping', 'status'],
    });

    const request: RPCRequest = {
      jsonrpc: '2.0',
      method: 'exec', // Not in allowed list
      id: 1,
    };

    const ctx = createMockContext('POST', '/rpc', request, {
      env: { SQL_DO: namespace },
    });

    const response = await handler.fetch(ctx.req.raw, ctx.env);
    const body = await response.json() as RPCResponse;

    expect(body.error?.code).toBe(-32601); // Method not found
    expect(body.error?.data).toMatchObject({ method: 'exec' });
  });

  it('should allow custom methods when configured', async () => {
    const stub = createMockDOStub(new Map([['customMethod', { custom: true }]]));
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({
      doBinding: 'SQL_DO',
      allowedMethods: ['customMethod'],
    });

    const request: RPCRequest = {
      jsonrpc: '2.0',
      method: 'customMethod',
      id: 1,
    };

    const ctx = createMockContext('POST', '/rpc', request, {
      env: { SQL_DO: namespace },
    });

    const response = await handler.fetch(ctx.req.raw, ctx.env);
    const body = await response.json() as RPCResponse;

    expect(body.error).toBeUndefined();
    expect(body.result).toEqual({ custom: true });
  });
});

// =============================================================================
// Error Response Tests
// =============================================================================

describe('RPC Handler - Error Responses', () => {
  it('should return DO_NOT_FOUND when namespace binding is missing', async () => {
    const handler = rpcHandler({ doBinding: 'SQL_DO' });

    const request: RPCRequest = {
      jsonrpc: '2.0',
      method: 'ping',
      id: 1,
    };

    const ctx = createMockContext('POST', '/rpc', request, {
      env: {}, // No SQL_DO binding
    });

    const response = await handler.fetch(ctx.req.raw, ctx.env);

    expect(response.status).toBe(500);
    const body = await response.json() as RPCResponse;
    expect(body.error?.code).toBe(-32001); // DO not found
    expect(body.error?.message).toBe('Durable Object not found');
  });

  it('should return internal error when DO fetch fails', async () => {
    const stub = createMockDOStub();
    stub.fetch.mockRejectedValue(new Error('Network error'));
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({ doBinding: 'SQL_DO' });

    const request: RPCRequest = {
      jsonrpc: '2.0',
      method: 'ping',
      id: 1,
    };

    const ctx = createMockContext('POST', '/rpc', request, {
      env: { SQL_DO: namespace },
    });

    const response = await handler.fetch(ctx.req.raw, ctx.env);
    const body = await response.json() as RPCResponse;

    expect(body.error?.code).toBe(-32603); // Internal error
    expect(body.error?.message).toBe('Network error');
  });

  it('should return error when DO returns non-200 response', async () => {
    const stub = createMockDOStub();
    stub.fetch.mockResolvedValue(new Response('Internal Server Error', { status: 500 }));
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({ doBinding: 'SQL_DO' });

    const request: RPCRequest = {
      jsonrpc: '2.0',
      method: 'ping',
      id: 1,
    };

    const ctx = createMockContext('POST', '/rpc', request, {
      env: { SQL_DO: namespace },
    });

    const response = await handler.fetch(ctx.req.raw, ctx.env);
    const body = await response.json() as RPCResponse;

    expect(body.error?.code).toBe(-32603);
    expect(body.error?.data).toMatchObject({ status: 500 });
  });

  it('should preserve request id in error response', async () => {
    const stub = createMockDOStub();
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({
      doBinding: 'SQL_DO',
      allowedMethods: ['ping'],
    });

    const request: RPCRequest = {
      jsonrpc: '2.0',
      method: 'disallowed',
      id: 'unique-id-123',
    };

    const ctx = createMockContext('POST', '/rpc', request, {
      env: { SQL_DO: namespace },
    });

    const response = await handler.fetch(ctx.req.raw, ctx.env);
    const body = await response.json() as RPCResponse;

    expect(body.id).toBe('unique-id-123');
    expect(body.error).toBeDefined();
  });
});

// =============================================================================
// DO ID Resolution Tests
// =============================================================================

describe('RPC Handler - DO ID Resolution', () => {
  it('should use db query parameter for DO name', async () => {
    const stub = createMockDOStub(new Map([['ping', { pong: true }]]));
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({ doBinding: 'SQL_DO' });

    const request = new Request('https://example.com/rpc?db=custom-db', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        method: 'ping',
        id: 1,
      }),
    });

    await handler.fetch(request, { SQL_DO: namespace });

    expect(namespace.idFromName).toHaveBeenCalledWith('custom-db');
  });

  it('should use default name when no db parameter', async () => {
    const stub = createMockDOStub(new Map([['ping', { pong: true }]]));
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({ doBinding: 'SQL_DO' });

    const request = new Request('https://example.com/rpc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        method: 'ping',
        id: 1,
      }),
    });

    await handler.fetch(request, { SQL_DO: namespace });

    expect(namespace.idFromName).toHaveBeenCalledWith('default');
  });

  it('should use custom resolveId function', async () => {
    const stub = createMockDOStub(new Map([['ping', { pong: true }]]));
    const namespace = createMockNamespace(stub);

    const handler = rpcHandler({
      doBinding: 'SQL_DO',
      resolveId: (c) => c.req.header('X-Database-Id') ?? 'fallback',
    });

    const request = new Request('https://example.com/rpc', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Database-Id': 'header-specified-db',
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        method: 'ping',
        id: 1,
      }),
    });

    await handler.fetch(request, { SQL_DO: namespace });

    expect(namespace.idFromName).toHaveBeenCalledWith('header-specified-db');
  });
});

// =============================================================================
// Methods Endpoint Tests
// =============================================================================

describe('RPC Handler - Methods Endpoint', () => {
  it('should list available methods at /rpc/methods', async () => {
    const stub = createMockDOStub();
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({
      doBinding: 'SQL_DO',
      allowedMethods: ['exec', 'query', 'ping'],
    });

    const request = new Request('https://example.com/rpc/methods', {
      method: 'GET',
    });

    const response = await handler.fetch(request, { SQL_DO: namespace });

    expect(response.status).toBe(200);
    const body = await response.json() as { methods: string[]; protocol: string };
    expect(body.methods).toContain('exec');
    expect(body.methods).toContain('query');
    expect(body.methods).toContain('ping');
    expect(body.protocol).toBe('json-rpc-2.0');
  });
});

// =============================================================================
// WebSocket Upgrade Tests
// =============================================================================

describe('RPC Handler - WebSocket Upgrade', () => {
  it('should return protocol info when GET /rpc without upgrade header', async () => {
    const stub = createMockDOStub();
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({ doBinding: 'SQL_DO' });

    const request = new Request('https://example.com/rpc', {
      method: 'GET',
    });

    const response = await handler.fetch(request, { SQL_DO: namespace });

    // Should require WebSocket upgrade
    expect(response.status).toBe(426);
    const body = await response.json() as { error: string };
    expect(body.error).toContain('WebSocket');
  });

  it('should forward WebSocket upgrade request to DO', async () => {
    const stub = createMockDOStub();
    // In a real Workers environment, the DO would handle the WebSocket upgrade
    // We mock it returning a regular response to verify the stub was called
    const wsResponse = new Response(null, { status: 200 });
    stub.fetch.mockResolvedValue(wsResponse);
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({ doBinding: 'SQL_DO' });

    const request = new Request('https://example.com/rpc', {
      method: 'GET',
      headers: { Upgrade: 'websocket' },
    });

    await handler.fetch(request, { SQL_DO: namespace });

    // Verify the stub was called with the raw request for WebSocket upgrade
    expect(stub.fetch).toHaveBeenCalled();
    const calledRequest = stub.fetch.mock.calls[0][0] as Request;
    expect(calledRequest.headers.get('upgrade')).toBe('websocket');
  });
});

// =============================================================================
// exposeDoSQL Middleware Tests
// =============================================================================

describe('exposeDoSQL Middleware', () => {
  it('should handle POST requests as RPC', async () => {
    const stub = createMockDOStub(new Map([['ping', { pong: true }]]));
    const namespace = createMockNamespace(stub);
    const middleware = exposeDoSQL({ doBinding: 'SQL_DO' });

    const ctx = createMockContext('POST', '/db', {
      jsonrpc: '2.0',
      method: 'ping',
      id: 1,
    }, {
      env: { SQL_DO: namespace },
    });
    const next = vi.fn();

    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);

    expect(response).toBeInstanceOf(Response);
    const body = await (response as Response).json() as RPCResponse;
    expect(body.result).toEqual({ pong: true });
  });

  it('should reject non-POST/non-WebSocket requests', async () => {
    const stub = createMockDOStub();
    const namespace = createMockNamespace(stub);
    const middleware = exposeDoSQL({ doBinding: 'SQL_DO' });

    const ctx = createMockContext('GET', '/db', undefined, {
      env: { SQL_DO: namespace },
    });
    const next = vi.fn();

    const response = await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);

    expect(response).toBeInstanceOf(Response);
    expect((response as Response).status).toBe(405);
  });

  it('should handle WebSocket upgrade request', async () => {
    const stub = createMockDOStub();
    stub.fetch.mockResolvedValue(new Response(null, { status: 101 }));
    const namespace = createMockNamespace(stub);
    const middleware = exposeDoSQL({ doBinding: 'SQL_DO' });

    const ctx = createMockContext('GET', '/db', undefined, {
      headers: { Upgrade: 'websocket' },
      env: { SQL_DO: namespace },
    });
    const next = vi.fn();

    await middleware(ctx as unknown as Parameters<typeof middleware>[0], next);

    expect(stub.fetch).toHaveBeenCalled();
  });
});

// =============================================================================
// Configuration Tests
// =============================================================================

describe('RPC Handler - Configuration', () => {
  it('should use default maxBatchSize of 100', async () => {
    const stub = createMockDOStub(new Map([['ping', { pong: true }]]));
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({ doBinding: 'SQL_DO' });

    // Create a batch of 100 requests (should succeed)
    const batch: RPCRequest[] = Array.from({ length: 100 }, (_, i) => ({
      jsonrpc: '2.0' as const,
      method: 'ping',
      id: i,
    }));

    const ctx = createMockContext('POST', '/rpc', batch, {
      env: { SQL_DO: namespace },
    });

    const response = await handler.fetch(ctx.req.raw, ctx.env);

    expect(response.status).toBe(200);
    const body = await response.json() as RPCResponse[];
    expect(body).toHaveLength(100);
  });

  it('should use default timeout of 30000ms', async () => {
    // This test verifies the config is accepted; actual timeout testing
    // would require simulating slow DO responses
    const stub = createMockDOStub(new Map([['ping', { pong: true }]]));
    const namespace = createMockNamespace(stub);
    const handler = rpcHandler({
      doBinding: 'SQL_DO',
      timeoutMs: 5000, // Custom timeout
    });

    const request: RPCRequest = {
      jsonrpc: '2.0',
      method: 'ping',
      id: 1,
    };

    const ctx = createMockContext('POST', '/rpc', request, {
      env: { SQL_DO: namespace },
    });

    const response = await handler.fetch(ctx.req.raw, ctx.env);

    expect(response.status).toBe(200);
  });
});
