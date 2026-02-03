import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config';

export default defineWorkersConfig({
  test: {
    include: ['src/**/*.test.ts', 'tests/**/*.test.ts'],
    coverage: {
      provider: 'istanbul',
      reporter: ['text', 'json', 'html', 'lcov'],
      reportsDirectory: './coverage',
      exclude: [
        '**/node_modules/**',
        '**/dist/**',
        '**/*.test.ts',
        '**/__tests__/**',
        '**/tests/**',
      ],
      thresholds: {
        lines: 80,
        branches: 80,
        functions: 80,
        statements: 80,
      },
    },
    poolOptions: {
      workers: {
        wrangler: { configPath: './wrangler.jsonc' },
        singleWorker: true,
        isolatedStorage: false, // Disable isolated storage to avoid known issues with DO storage cleanup
        miniflare: {
          durableObjects: {
            DOLAKE: 'DoLake',
          },
          r2Buckets: ['LAKEHOUSE_BUCKET'],
          kvNamespaces: ['KV_FALLBACK'],
        },
      },
    },
  },
});
