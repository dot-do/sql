/**
 * DoSQL E2E Test Module
 *
 * Exports utilities for running E2E tests against production Cloudflare Workers.
 *
 * @packageDocumentation
 */

// Setup utilities
export {
  deploy,
  teardown,
  createTestContext,
  getE2EEndpoint,
  waitForReady,
  isAuthenticated,
  getAccountInfo,
  runSetup,
  type DeploymentConfig,
  type DeploymentResult,
  type E2ETestContext,
} from './setup.js';

// Client utilities
export {
  DoSQLE2EClient,
  DoSQLWebSocketClient,
  createTestClient,
  retry,
  assert,
  assertEqual,
  assertLength,
  assertWithinTime,
  type E2EClientConfig,
  type QueryResult,
  type ExecuteResult,
  type HealthResponse,
  type TablesResponse,
  type TableSchema,
  type LatencyMeasurement,
  type CDCEvent,
} from './client.js';
