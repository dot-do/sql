import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config';

export default defineWorkersConfig({
  test: {
    include: ['src/**/*.test.ts', 'tests/**/*.test.ts'],
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
