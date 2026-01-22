/**
 * DoSQL Durable Object Test Setup
 *
 * Utilities for testing DoSQL with real Workers runtime via @cloudflare/vitest-pool-workers.
 * NO MOCKS - all tests run against real miniflare/workerd runtime.
 */

import { env, SELF } from 'cloudflare:test';
import type { DurableObjectNamespace, R2Bucket } from '@cloudflare/workers-types';

// =============================================================================
// Environment Types
// =============================================================================

/**
 * Test environment bindings provided by miniflare
 */
export interface TestEnv {
  /** DoSQL Database Durable Object namespace */
  DOSQL_DB: DurableObjectNamespace;
  /** Test R2 bucket */
  TEST_R2_BUCKET: R2Bucket;
}

// =============================================================================
// DO Stub Helpers
// =============================================================================

/**
 * Get a DoSQL Database stub by name
 * Creates a new instance if it doesn't exist
 */
export function getDoSqlStub(name: string = 'test-db'): DurableObjectStub {
  const typedEnv = env as unknown as TestEnv;
  const id = typedEnv.DOSQL_DB.idFromName(name);
  return typedEnv.DOSQL_DB.get(id);
}

/**
 * Get a unique DoSQL Database stub using a random ID
 * Useful for isolation between tests
 */
export function getUniqueDoSqlStub(): DurableObjectStub {
  const typedEnv = env as unknown as TestEnv;
  const id = typedEnv.DOSQL_DB.newUniqueId();
  return typedEnv.DOSQL_DB.get(id);
}

/**
 * Get the R2 test bucket
 */
export function getR2Bucket(): R2Bucket {
  const typedEnv = env as unknown as TestEnv;
  return typedEnv.TEST_R2_BUCKET;
}

// =============================================================================
// SQL Execution Helpers
// =============================================================================

/**
 * Execute SQL against a DoSQL Durable Object via HTTP
 */
export async function executeSQL(
  stub: DurableObjectStub,
  sql: string,
  params?: Record<string, unknown>
): Promise<{
  success: boolean;
  rows?: Record<string, unknown>[];
  error?: string;
  stats?: { rowsAffected: number; executionTimeMs: number };
}> {
  const response = await stub.fetch('http://localhost/execute', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql, params }),
  });

  return response.json();
}

/**
 * Execute a SELECT query against a DoSQL Durable Object
 */
export async function querySQL(
  stub: DurableObjectStub,
  sql: string,
  params?: Record<string, unknown>
): Promise<{
  success: boolean;
  rows?: Record<string, unknown>[];
  error?: string;
  stats?: { rowsAffected: number; executionTimeMs: number };
}> {
  const response = await stub.fetch('http://localhost/query', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql, params }),
  });

  return response.json();
}

/**
 * Get the health status of a DoSQL Durable Object
 */
export async function getHealth(stub: DurableObjectStub): Promise<{
  status: string;
  initialized: boolean;
}> {
  const response = await stub.fetch('http://localhost/health');
  return response.json();
}

/**
 * List tables in a DoSQL Durable Object
 */
export async function listTables(stub: DurableObjectStub): Promise<{
  tables: Array<{
    name: string;
    columns: Array<{ name: string; type: string }>;
    primaryKey: string;
  }>;
}> {
  const response = await stub.fetch('http://localhost/tables');
  return response.json();
}

// =============================================================================
// Worker Fetch Helper
// =============================================================================

/**
 * Make a request to the worker using SELF binding
 */
export async function workerFetch(
  path: string,
  init?: RequestInit
): Promise<Response> {
  return SELF.fetch(`http://localhost${path}`, init);
}

// =============================================================================
// Test Data Helpers
// =============================================================================

/**
 * Create a test table with sample data
 */
export async function createTestTable(
  stub: DurableObjectStub,
  tableName: string = 'test_users'
): Promise<void> {
  // Create table
  await executeSQL(
    stub,
    `CREATE TABLE ${tableName} (id INTEGER, name TEXT, email TEXT, PRIMARY KEY (id))`
  );

  // Insert sample data
  await executeSQL(
    stub,
    `INSERT INTO ${tableName} (id, name, email) VALUES (1, 'Alice', 'alice@example.com')`
  );
  await executeSQL(
    stub,
    `INSERT INTO ${tableName} (id, name, email) VALUES (2, 'Bob', 'bob@example.com')`
  );
  await executeSQL(
    stub,
    `INSERT INTO ${tableName} (id, name, email) VALUES (3, 'Charlie', 'charlie@example.com')`
  );
}

/**
 * Seed R2 bucket with test data
 */
export async function seedR2TestData(bucket: R2Bucket): Promise<void> {
  // Add some test JSON data
  await bucket.put(
    'data/users.json',
    JSON.stringify([
      { id: 1, name: 'Alice', email: 'alice@example.com' },
      { id: 2, name: 'Bob', email: 'bob@example.com' },
    ])
  );

  // Add some CSV data
  await bucket.put(
    'data/products.csv',
    'id,name,price\n1,Widget,9.99\n2,Gadget,19.99\n3,Gizmo,29.99'
  );

  // Add some Parquet-like metadata (simulated)
  await bucket.put(
    'data/metadata.json',
    JSON.stringify({
      format: 'parquet',
      schema: ['id', 'name', 'value'],
      rowCount: 1000,
    })
  );
}

/**
 * Clean up R2 bucket test data
 */
export async function cleanupR2TestData(bucket: R2Bucket): Promise<void> {
  const objects = await bucket.list({ prefix: 'data/' });
  for (const obj of objects.objects) {
    await bucket.delete(obj.key);
  }
}

// =============================================================================
// Type Declarations for Cloudflare Test Utilities
// =============================================================================

declare module 'cloudflare:test' {
  const env: TestEnv;
  const SELF: {
    fetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response>;
  };
}

// Re-export for convenience
export { env, SELF } from 'cloudflare:test';
