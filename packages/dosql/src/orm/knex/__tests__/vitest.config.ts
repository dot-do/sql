/**
 * Vitest configuration for Knex integration tests
 *
 * Knex is a Node.js library and requires Node.js environment.
 * This config runs tests in Node.js instead of workers-vitest-pool.
 *
 * Run with: npx vitest run --config src/orm/knex/__tests__/vitest.config.ts
 */

import { defineConfig } from 'vitest/config';
import { resolve } from 'path';

export default defineConfig({
  test: {
    // Use absolute path to ensure the test file is found
    include: [resolve(__dirname, 'knex.test.ts')],
    environment: 'node',
    globals: true,
    // Root needs to be set to find the test file
    root: resolve(__dirname),
  },
});
