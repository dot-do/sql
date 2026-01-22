/**
 * Vitest configuration for Knex integration tests
 *
 * Knex is a Node.js library and requires Node.js environment.
 * This config runs tests in Node.js instead of workers-vitest-pool.
 */

import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['knex.test.ts'],
    environment: 'node',
    globals: true,
  },
});
