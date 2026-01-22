/**
 * Test Environment Type Declarations
 *
 * Extends the cloudflare:test module to provide types for test bindings.
 */

import { BenchmarkTestDO } from './benchmarks/adapters/__tests__/do-sqlite.test.js';

declare module 'cloudflare:test' {
  interface ProvidedEnv {
    /** D1 database binding for benchmark adapter tests */
    TEST_D1: D1Database;

    /** Durable Object namespace for benchmark tests */
    BENCHMARK_TEST_DO: DurableObjectNamespace<BenchmarkTestDO>;

    /** Durable Object namespace for DoSQL database */
    DOSQL_DB: DurableObjectNamespace;

    /** Durable Object namespace for branch tests */
    TEST_BRANCH_DO: DurableObjectNamespace;

    /** R2 bucket for tests */
    TEST_R2_BUCKET: R2Bucket;

    /** Replication primary DO */
    REPLICATION_PRIMARY: DurableObjectNamespace;

    /** Replication replica DO */
    REPLICATION_REPLICA: DurableObjectNamespace;
  }
}
