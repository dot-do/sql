/**
 * Request Handler
 *
 * Extracted from database.ts to reduce the size of the main DoSQLDatabase class.
 * Handles HTTP request routing, input validation, and response formatting.
 */

import type { QueryExecutor } from './query-executor.js';
import type { SchemaManager } from './schema-manager.js';
import { RateLimiter } from './rate-limiter.js';
import { checkAuth, type AuthConfig } from './auth.js';
import { checkContentLength, parseAndValidateBody } from './input-validation.js';

// =============================================================================
// Types
// =============================================================================

export interface QueryRequest {
  sql: string;
  params?: Record<string, unknown>;
}

export interface QueryResponse {
  success: boolean;
  rows?: Record<string, unknown>[];
  error?: string;
  stats?: {
    rowsAffected: number;
    executionTimeMs: number;
  };
}

// =============================================================================
// Request Handler
// =============================================================================

export class RequestHandler {
  constructor(
    private queryExecutor: QueryExecutor,
    private schemaManager: SchemaManager,
    private isInitialized: () => boolean,
    private rateLimiter: RateLimiter,
    private getAuthConfig: () => AuthConfig,
  ) {}

  /**
   * Route an HTTP request to the appropriate handler
   */
  async handleRequest(request: Request, path: string): Promise<Response> {
    // Check authentication before processing any request
    const authResponse = checkAuth(request, this.getAuthConfig());
    if (authResponse) {
      return authResponse;
    }

    // Apply rate limiting to query and execute endpoints
    if ((path === '/query' || path === '/execute') && request.method === 'POST') {
      if (!this.rateLimiter.consume()) {
        return RateLimiter.tooManyRequestsResponse();
      }
    }

    // Route requests
    if (path === '/query' && request.method === 'POST') {
      return this.handleQuery(request);
    }

    if (path === '/execute' && request.method === 'POST') {
      return this.handleExecute(request);
    }

    if (path === '/tables' && request.method === 'GET') {
      return this.handleListTables();
    }

    if (path === '/health' && request.method === 'GET') {
      return new Response(JSON.stringify({ status: 'ok', initialized: this.isInitialized() }), {
        headers: { 'Content-Type': 'application/json' },
      });
    }

    // Not found
    return new Response(JSON.stringify({ error: 'Not found' }), {
      status: 404,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  /**
   * Handle SELECT queries
   */
  private async handleQuery(request: Request): Promise<Response> {
    // Validate input size limits
    const contentLengthError = checkContentLength(request);
    if (contentLengthError) return contentLengthError;

    const bodyOrError = await parseAndValidateBody(request);
    if (bodyOrError instanceof Response) return bodyOrError;
    const body = bodyOrError;
    const startTime = performance.now();

    try {
      const result = await this.queryExecutor.executeSQL(body.sql, body.params);
      const endTime = performance.now();

      const response: QueryResponse = {
        success: true,
        rows: result.rows,
        stats: {
          rowsAffected: result.rows.length,
          executionTimeMs: endTime - startTime,
        },
      };

      return new Response(JSON.stringify(response), {
        headers: { 'Content-Type': 'application/json' },
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      return new Response(JSON.stringify({ success: false, error: message }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }
  }

  /**
   * Handle INSERT/UPDATE/DELETE mutations
   */
  private async handleExecute(request: Request): Promise<Response> {
    // Validate input size limits
    const contentLengthError = checkContentLength(request);
    if (contentLengthError) return contentLengthError;

    const bodyOrError = await parseAndValidateBody(request);
    if (bodyOrError instanceof Response) return bodyOrError;
    const body = bodyOrError;
    const startTime = performance.now();

    try {
      const result = await this.queryExecutor.executeSQL(body.sql, body.params);
      const endTime = performance.now();

      const response: QueryResponse = {
        success: true,
        rows: result.rows,
        stats: {
          rowsAffected: result.rowsAffected,
          executionTimeMs: endTime - startTime,
        },
      };

      return new Response(JSON.stringify(response), {
        headers: { 'Content-Type': 'application/json' },
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Unknown error';
      return new Response(JSON.stringify({ success: false, error: message }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }
  }

  /**
   * List all tables
   */
  private async handleListTables(): Promise<Response> {
    const tables = this.schemaManager.getAllSchemas();
    return new Response(JSON.stringify({ tables }), {
      headers: { 'Content-Type': 'application/json' },
    });
  }
}
