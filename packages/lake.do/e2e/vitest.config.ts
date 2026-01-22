/**
 * Vitest Configuration for DoLake E2E Tests
 *
 * These tests run against real deployed Cloudflare Workers in production.
 * They use native Node.js fetch and WebSocket, not Workers runtime.
 *
 * @packageDocumentation
 */

import { defineConfig } from 'vitest/config';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  test: {
    // E2E tests run in Node environment (not Workers)
    environment: 'node',

    // Root directory for tests
    root: __dirname,

    // Only run E2E tests in this directory
    include: ['__tests__/**/*.test.ts'],

    // Longer timeout for E2E tests (network latency, cold starts)
    testTimeout: 120_000,

    // Hook timeout for setup/teardown
    hookTimeout: 60_000,

    // Run tests sequentially to avoid overwhelming the endpoint
    sequence: {
      concurrent: false,
    },

    // Single worker to avoid parallel test interference
    pool: 'forks',
    poolOptions: {
      forks: {
        singleFork: true,
      },
    },

    // Retry flaky network tests
    retry: 1,

    // Reporter for CI visibility
    reporters: process.env.CI ? ['default', 'junit'] : ['default'],

    // Output file for CI
    outputFile: {
      junit: './test-results/e2e-junit.xml',
    },

    // Global setup for E2E environment
    globalSetup: process.env.E2E_AUTO_DEPLOY ? ['./global-setup.ts'] : undefined,

  },

  // Resolve configuration
  resolve: {
    alias: {
      // Use source files directly
      '@': resolve(__dirname, '../src'),
    },
  },
});
