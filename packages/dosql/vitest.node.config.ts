/**
 * Vitest configuration for Node.js-based tests
 *
 * This config runs tests that require Node.js modules (fs, os, path)
 * and native dependencies (better-sqlite3) that are not available
 * in the Cloudflare Workers environment.
 *
 * Run with: npx vitest run --config vitest.node.config.ts
 */
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: [
      // Tests requiring Node.js file system access
      'src/__tests__/sqlite-file-compat.test.ts',
      // Knex tests require node:os, node:fs, node:path
      'src/orm/knex/__tests__/**/*.test.ts',
    ],
    // Standard Node.js environment
    environment: 'node',
    // Longer timeout for file operations
    testTimeout: 30000,
    // Enable globals for better DX
    globals: true,
    // Pool settings for Node.js
    pool: 'forks',
    poolOptions: {
      forks: {
        singleFork: true,
      },
    },
  },
});
