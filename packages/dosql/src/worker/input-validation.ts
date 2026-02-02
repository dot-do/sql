/**
 * Input Validation
 *
 * Extracted from database.ts to reduce the size of the main DoSQLDatabase class.
 * Handles request body size limits, SQL length limits, and parameter count validation.
 */

import type { QueryRequest } from './request-handler.js';

// =============================================================================
// Input Size Limits
// =============================================================================

/** Maximum request body size in bytes (1MB) */
export const MAX_REQUEST_BODY_SIZE = 1 * 1024 * 1024;

/** Maximum SQL query string length in characters (1MB) */
export const MAX_SQL_LENGTH = 1 * 1024 * 1024;

/** Maximum number of bound parameters per query */
export const MAX_PARAM_COUNT = 1000;

/**
 * Validate request body size using Content-Length header.
 * Returns an HTTP 413 Response if the payload exceeds limits, or null if acceptable.
 */
export function checkContentLength(request: Request): Response | null {
  const contentLength = request.headers.get('Content-Length');
  if (contentLength !== null) {
    const size = parseInt(contentLength, 10);
    if (!isNaN(size) && size > MAX_REQUEST_BODY_SIZE) {
      return new Response(
        JSON.stringify({
          success: false,
          error: `Request body too large: ${size} bytes exceeds maximum of ${MAX_REQUEST_BODY_SIZE} bytes`,
        }),
        {
          status: 413,
          headers: { 'Content-Type': 'application/json' },
        },
      );
    }
  }
  return null;
}

/**
 * Read request body with streaming size enforcement.
 * Rejects immediately when size limit is exceeded, before consuming entire body.
 * This prevents memory exhaustion from chunked transfer encoding attacks.
 */
async function readBodyWithSizeLimit(request: Request): Promise<string | Response> {
  const body = request.body;
  if (!body) {
    return '';
  }

  const reader = body.getReader();
  const chunks: Uint8Array[] = [];
  let totalSize = 0;

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      totalSize += value.byteLength;

      // Reject immediately when limit is exceeded - don't wait to read entire body
      if (totalSize > MAX_REQUEST_BODY_SIZE) {
        // Cancel the reader to stop reading more data
        await reader.cancel();
        return new Response(
          JSON.stringify({
            success: false,
            error: `Request body too large: exceeds maximum of ${MAX_REQUEST_BODY_SIZE} bytes`,
          }),
          {
            status: 413,
            headers: { 'Content-Type': 'application/json' },
          },
        );
      }

      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }

  // Concatenate chunks and decode as UTF-8
  const combined = new Uint8Array(totalSize);
  let offset = 0;
  for (const chunk of chunks) {
    combined.set(chunk, offset);
    offset += chunk.byteLength;
  }

  return new TextDecoder().decode(combined);
}

/**
 * Parse and validate the request body for query endpoints.
 * Returns a QueryRequest on success or an HTTP error Response on failure.
 */
export async function parseAndValidateBody(request: Request): Promise<QueryRequest | Response> {
  // Read body with streaming size enforcement to prevent memory exhaustion
  const bodyResult = await readBodyWithSizeLimit(request);

  // If we got a Response, it means size limit was exceeded
  if (bodyResult instanceof Response) {
    return bodyResult;
  }

  const bodyText = bodyResult;

  let body: QueryRequest;
  try {
    body = JSON.parse(bodyText) as QueryRequest;
  } catch {
    return new Response(
      JSON.stringify({ success: false, error: 'Invalid JSON in request body' }),
      {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      },
    );
  }

  // Validate SQL field exists and is a string
  if (typeof body.sql !== 'string') {
    return new Response(
      JSON.stringify({ success: false, error: 'Missing or invalid "sql" field' }),
      {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      },
    );
  }

  // Validate SQL length
  if (body.sql.length > MAX_SQL_LENGTH) {
    return new Response(
      JSON.stringify({
        success: false,
        error: `SQL query too large: ${body.sql.length} characters exceeds maximum of ${MAX_SQL_LENGTH} characters`,
      }),
      {
        status: 413,
        headers: { 'Content-Type': 'application/json' },
      },
    );
  }

  // Validate parameter count
  if (body.params !== undefined && body.params !== null) {
    if (typeof body.params !== 'object' || Array.isArray(body.params)) {
      return new Response(
        JSON.stringify({ success: false, error: 'Invalid "params" field: must be an object' }),
        {
          status: 400,
          headers: { 'Content-Type': 'application/json' },
        },
      );
    }

    const paramCount = Object.keys(body.params).length;
    if (paramCount > MAX_PARAM_COUNT) {
      return new Response(
        JSON.stringify({
          success: false,
          error: `Too many parameters: ${paramCount} exceeds maximum of ${MAX_PARAM_COUNT}`,
        }),
        {
          status: 413,
          headers: { 'Content-Type': 'application/json' },
        },
      );
    }
  }

  return body;
}
