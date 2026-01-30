import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config';

export default defineWorkersConfig({
  test: {
    include: ['src/**/*.test.ts'],
    coverage: {
      provider: 'istanbul',
      reporter: ['text', 'json', 'html', 'lcov'],
      reportsDirectory: './coverage',
      exclude: [
        '**/node_modules/**',
        '**/dist/**',
        '**/*.test.ts',
        '**/__tests__/**',
        '**/benchmarks/**',
        '**/e2e/**',
        '**/scripts/**',
        // Exclude tests that require Node.js modules
        'src/orm/knex/**',
      ],
      thresholds: {
        lines: 50,
        branches: 50,
        functions: 50,
        statements: 50,
      },
    },
    // Exclude tests that require Node.js modules not available in Workers
    exclude: [
      '**/node_modules/**',
      // Knex tests require node:os, node:fs, node:path - not available in Workers
      'src/orm/knex/__tests__/**',
      // SQLite file compat tests require Node.js fs/os/path modules and better-sqlite3
      'src/__tests__/sqlite-file-compat.test.ts',
      // SQLite CLI compat tests require Node.js child_process and sqlite3 CLI
      'src/__tests__/sqlite-cli-compat.test.ts',
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
