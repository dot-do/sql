import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config';

export default defineWorkersConfig({
  test: {
    include: ['src/**/*.test.ts'],
    // Exclude tests that require Node.js modules not available in Workers
    exclude: [
      '**/node_modules/**',
      // Knex tests require node:os, node:fs, node:path - not available in Workers
      'src/orm/knex/__tests__/**',
      // SQLite file compat tests require Node.js fs/os/path modules and better-sqlite3
      'src/__tests__/sqlite-file-compat.test.ts',
    ],
    // Disable isolated storage to avoid known issues with DO storage cleanup
    // See: https://developers.cloudflare.com/workers/testing/vitest-integration/known-issues/#isolated-storage
    isolatedStorage: false,
    poolOptions: {
      workers: {
        wrangler: { configPath: './wrangler.jsonc' },
        singleWorker: true,
        miniflare: {
          durableObjects: {
            DOSQL_DB: 'DoSQLDatabase',
            TEST_BRANCH_DO: 'TestBranchDO',
            REPLICATION_PRIMARY: 'ReplicationPrimaryDO',
            REPLICATION_REPLICA: 'ReplicationReplicaDO',
            BENCHMARK_TEST_DO: { className: 'BenchmarkTestDO', useSQLite: true },
            PERFORMANCE_BENCHMARK_DO: { className: 'PerformanceBenchmarkDO', useSQLite: true },
            PERFORMANCE_REGRESSION_DO: { className: 'PerformanceRegressionDO', useSQLite: true },
            PRODUCTION_BENCHMARK_DO: { className: 'ProductionBenchmarkDO', useSQLite: true },
          },
          r2Buckets: ['TEST_R2_BUCKET'],
          d1Databases: ['TEST_D1'],
        },
      },
    },
  },
});
