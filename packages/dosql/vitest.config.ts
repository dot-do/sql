import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config';

export default defineWorkersConfig({
  test: {
    include: ['src/**/*.test.ts'],
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
