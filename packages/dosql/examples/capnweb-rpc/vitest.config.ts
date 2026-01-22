import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config';

export default defineWorkersConfig({
  test: {
    include: ['test/**/*.test.ts'],
    poolOptions: {
      workers: {
        wrangler: { configPath: './wrangler.jsonc' },
        singleWorker: true,
        // Disable isolated storage to avoid known issues with DO storage cleanup
        // See: https://developers.cloudflare.com/workers/testing/vitest-integration/known-issues/#isolated-storage
        isolatedStorage: false,
        miniflare: {
          durableObjects: {
            DOSQL_DB: 'DoSQLDatabase',
          },
        },
      },
    },
  },
});
