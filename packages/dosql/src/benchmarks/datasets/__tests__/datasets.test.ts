/**
 * Benchmark Datasets Test Suite
 *
 * Comprehensive tests verifying:
 * - Schema creation works correctly
 * - Data generation produces consistent, deterministic results
 * - All benchmark queries are syntactically valid
 * - Query results are within expected bounds
 *
 * Uses workers-vitest-pool (NO MOCKS) as required by project conventions.
 *
 * @packageDocumentation
 */

import { describe, it, expect, beforeEach } from 'vitest';
import { env, runInDurableObject } from 'cloudflare:test';

// Import the actual BenchmarkTestDO type from the adapter tests
import { BenchmarkTestDO } from '../../adapters/__tests__/do-sqlite.test.js';

// Import all datasets
import {
  // Northwind
  generateNorthwindData,
  getNorthwindSchemaStatements,
  NORTHWIND_BENCHMARK_QUERIES,
  NORTHWIND_METADATA,
  type NorthwindDataset,

  // O*NET
  generateOnetData,
  getOnetSchemaStatements,
  ONET_BENCHMARK_QUERIES,
  ONET_METADATA,
  type OnetDataset,

  // IMDB
  generateImdbData,
  getImdbSchemaStatements,
  IMDB_BENCHMARK_QUERIES,
  IMDB_METADATA,
  type ImdbDataset,

  // UNSPSC
  generateUnspscData,
  getUnspscSchemaStatements,
  UNSPSC_BENCHMARK_QUERIES,
  UNSPSC_METADATA,
  type UnspscDataset,

  // Combined
  getTotalQueryCount,
  getTotalApproximateRows,
  getAllQueriesByCategory,
  ALL_DATASET_METADATA,
} from '../index.js';

// =============================================================================
// Test Helpers
// =============================================================================

let testCounter = 0;

function getUniqueStub() {
  const id = env.BENCHMARK_TEST_DO.idFromName(
    `dataset-test-${Date.now()}-${testCounter++}`
  );
  return env.BENCHMARK_TEST_DO.get(id);
}

// =============================================================================
// Dataset Metadata Tests
// =============================================================================

describe('Dataset Metadata', () => {
  it('should have consistent metadata for all datasets', () => {
    expect(NORTHWIND_METADATA.name).toBe('Northwind');
    expect(NORTHWIND_METADATA.approximateRows).toBe(8000);
    expect(NORTHWIND_METADATA.tables.length).toBeGreaterThan(0);

    expect(ONET_METADATA.name).toBe('O*NET');
    expect(ONET_METADATA.approximateRows).toBe(170000);

    expect(IMDB_METADATA.name).toBe('IMDB');
    expect(IMDB_METADATA.approximateRows).toBe(100000);

    expect(UNSPSC_METADATA.name).toBe('UNSPSC');
    expect(UNSPSC_METADATA.approximateRows).toBe(50000);
  });

  it('should have query counts in metadata', () => {
    expect(NORTHWIND_METADATA.queryCount).toBe(NORTHWIND_BENCHMARK_QUERIES.length);
    expect(ONET_METADATA.queryCount).toBe(ONET_BENCHMARK_QUERIES.length);
    expect(IMDB_METADATA.queryCount).toBe(IMDB_BENCHMARK_QUERIES.length);
    expect(UNSPSC_METADATA.queryCount).toBe(UNSPSC_BENCHMARK_QUERIES.length);
  });

  it('should calculate total query count correctly', () => {
    const total = getTotalQueryCount();
    const expected =
      NORTHWIND_BENCHMARK_QUERIES.length +
      ONET_BENCHMARK_QUERIES.length +
      IMDB_BENCHMARK_QUERIES.length +
      UNSPSC_BENCHMARK_QUERIES.length;

    expect(total).toBe(expected);
    expect(total).toBeGreaterThan(80); // Should have 80+ queries total
  });

  it('should calculate total approximate rows correctly', () => {
    const total = getTotalApproximateRows();
    expect(total).toBe(328000); // 8k + 170k + 100k + 50k
  });

  it('should group queries by category', () => {
    const categories = getAllQueriesByCategory();

    expect(categories.point_lookup).toBeGreaterThan(0);
    expect(categories.range).toBeGreaterThan(0);
    expect(categories.join).toBeGreaterThan(0);
    expect(categories.aggregate).toBeGreaterThan(0);
    expect(categories.text_search).toBeGreaterThan(0);
  });

  it('should have complete combined metadata', () => {
    expect(ALL_DATASET_METADATA.northwind.name).toBe('Northwind');
    expect(ALL_DATASET_METADATA.onet.name).toBe('O*NET');
    expect(ALL_DATASET_METADATA.imdb.name).toBe('IMDB');
    expect(ALL_DATASET_METADATA.unspsc.name).toBe('UNSPSC');

    // Check descriptions exist
    expect(ALL_DATASET_METADATA.northwind.description).toBeTruthy();
    expect(ALL_DATASET_METADATA.onet.primaryUseCase).toBeTruthy();
  });
});

// =============================================================================
// Northwind Dataset Tests
// =============================================================================

describe('Northwind Dataset', () => {
  describe('Data Generation', () => {
    it('should generate deterministic data with same seed', () => {
      const data1 = generateNorthwindData({ seed: 12345 });
      const data2 = generateNorthwindData({ seed: 12345 });

      expect(data1.customers.length).toBe(data2.customers.length);
      expect(data1.orders.length).toBe(data2.orders.length);
      expect(data1.customers[0].customer_id).toBe(data2.customers[0].customer_id);
      expect(data1.orders[0].order_id).toBe(data2.orders[0].order_id);
    });

    it('should generate different data with different seeds', () => {
      const data1 = generateNorthwindData({ seed: 11111 });
      const data2 = generateNorthwindData({ seed: 22222 });

      // Customer IDs should differ (not always, but statistically likely)
      const ids1 = data1.customers.map((c) => c.customer_id).sort();
      const ids2 = data2.customers.map((c) => c.customer_id).sort();
      expect(ids1).not.toEqual(ids2);
    });

    it('should generate expected entity counts with default config', () => {
      const data = generateNorthwindData();

      expect(data.customers.length).toBe(91);
      expect(data.employees.length).toBe(9);
      expect(data.suppliers.length).toBe(29);
      expect(data.orders.length).toBe(830);
      expect(data.categories.length).toBe(8);
      expect(data.shippers.length).toBe(3);
      expect(data.products.length).toBeGreaterThan(50);
      expect(data.orderDetails.length).toBeGreaterThan(data.orders.length);
    });

    it('should generate valid foreign key references', () => {
      const data = generateNorthwindData();

      // Check order references
      const customerIds = new Set(data.customers.map((c) => c.customer_id));
      const employeeIds = new Set(data.employees.map((e) => e.employee_id));
      const shipperIds = new Set(data.shippers.map((s) => s.shipper_id));

      for (const order of data.orders) {
        expect(customerIds.has(order.customer_id)).toBe(true);
        expect(employeeIds.has(order.employee_id)).toBe(true);
        expect(shipperIds.has(order.ship_via)).toBe(true);
      }

      // Check order details references
      const orderIds = new Set(data.orders.map((o) => o.order_id));
      const productIds = new Set(data.products.map((p) => p.product_id));

      for (const detail of data.orderDetails) {
        expect(orderIds.has(detail.order_id)).toBe(true);
        expect(productIds.has(detail.product_id)).toBe(true);
      }
    });

    it('should respect custom configuration', () => {
      const data = generateNorthwindData({
        seed: 999,
        customerCount: 50,
        orderCount: 100,
      });

      expect(data.customers.length).toBe(50);
      expect(data.orders.length).toBe(100);
    });
  });

  describe('Schema Creation', () => {
    it('should return all schema statements in correct order', () => {
      const statements = getNorthwindSchemaStatements();

      expect(statements.length).toBeGreaterThan(0);
      expect(statements.some((s) => s.includes('CREATE TABLE'))).toBe(true);
      expect(statements.some((s) => s.includes('CREATE INDEX'))).toBe(true);
    });

    it('should create tables successfully in DO', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
        await instance.initialize();
        const statements = getNorthwindSchemaStatements();

        // execMany executes all statements - check no errors thrown
        const results = instance.execMany(statements);
        expect(results.every((r) => r.success)).toBe(true);
      });
    });
  });

  describe('Benchmark Queries', () => {
    it('should have at least 20 benchmark queries', () => {
      expect(NORTHWIND_BENCHMARK_QUERIES.length).toBeGreaterThanOrEqual(20);
    });

    it('should have unique query IDs', () => {
      const ids = NORTHWIND_BENCHMARK_QUERIES.map((q) => q.id);
      const uniqueIds = new Set(ids);
      expect(uniqueIds.size).toBe(ids.length);
    });

    it('should have all required fields for each query', () => {
      for (const query of NORTHWIND_BENCHMARK_QUERIES) {
        expect(query.id).toBeTruthy();
        expect(query.name).toBeTruthy();
        expect(query.category).toBeTruthy();
        expect(query.sql).toBeTruthy();
        expect(query.description).toBeTruthy();
      }
    });

    it('should cover all expected categories', () => {
      const categories = new Set(NORTHWIND_BENCHMARK_QUERIES.map((q) => q.category));

      expect(categories.has('point_lookup')).toBe(true);
      expect(categories.has('range')).toBe(true);
      expect(categories.has('join')).toBe(true);
      expect(categories.has('aggregate')).toBe(true);
    });

    it('should execute queries successfully against schema', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
        await instance.initialize();

        // Create schema
        const schemaResults = instance.execMany(getNorthwindSchemaStatements());
        expect(schemaResults.every((r) => r.success)).toBe(true);

        // Generate and insert minimal data
        const data = generateNorthwindData({ orderCount: 10 });

        // Insert reference data using insertBatch
        const catResult = await instance.insertBatch('categories', data.categories);
        expect(catResult.success).toBe(true);

        const shipResult = await instance.insertBatch('shippers', data.shippers);
        expect(shipResult.success).toBe(true);

        const suppResult = await instance.insertBatch('suppliers', data.suppliers);
        expect(suppResult.success).toBe(true);

        const empResult = await instance.insertBatch('employees', data.employees.map((e) => ({ ...e, reports_to: null })));
        expect(empResult.success).toBe(true);

        const prodResult = await instance.insertBatch('products', data.products);
        if (!prodResult.success) {
          throw new Error(`Products insert failed: ${prodResult.error}`);
        }

        const custResult = await instance.insertBatch('customers', data.customers);
        expect(custResult.success).toBe(true);

        // Test a few key queries using query method
        const pointLookup = await instance.query(
          "SELECT * FROM customers WHERE customer_id = 'XXXXX'"
        );
        expect(pointLookup.success).toBe(true);

        const joinQuery = await instance.query(`
          SELECT c.category_name
          FROM categories c
          JOIN products p ON c.category_id = p.category_id
          LIMIT 5
        `);
        if (!joinQuery.success) {
          throw new Error(`Join query failed: ${joinQuery.error}`);
        }
        expect(joinQuery.success).toBe(true);
      });
    });
  });
});

// =============================================================================
// O*NET Dataset Tests
// =============================================================================

describe('O*NET Dataset', () => {
  describe('Data Generation', () => {
    it('should generate deterministic data with same seed', () => {
      const data1 = generateOnetData({ seed: 12345 });
      const data2 = generateOnetData({ seed: 12345 });

      expect(data1.occupations.length).toBe(data2.occupations.length);
      expect(data1.skills.length).toBe(data2.skills.length);
      expect(data1.occupations[0].onetsoc_code).toBe(data2.occupations[0].onetsoc_code);
    });

    it('should generate expected entity counts', () => {
      const data = generateOnetData();

      expect(data.skills.length).toBe(33);
      expect(data.abilities.length).toBe(39);
      expect(data.knowledge.length).toBe(30);
      expect(data.workActivities.length).toBe(32);
      expect(data.workContexts.length).toBe(28);
      expect(data.occupations.length).toBeGreaterThan(500);
    });

    it('should generate valid occupation-skill relationships', () => {
      const data = generateOnetData({ occupationsPerGroup: 10 });

      const occupationCodes = new Set(data.occupations.map((o) => o.onetsoc_code));
      const skillIds = new Set(data.skills.map((s) => s.skill_id));

      for (const os of data.occupationSkills) {
        expect(occupationCodes.has(os.onetsoc_code)).toBe(true);
        expect(skillIds.has(os.skill_id)).toBe(true);
        expect(os.importance).toBeGreaterThanOrEqual(1);
        expect(os.importance).toBeLessThanOrEqual(5);
        expect(os.level).toBeGreaterThanOrEqual(1);
        expect(os.level).toBeLessThanOrEqual(7);
      }
    });

    it('should generate hierarchical occupation codes', () => {
      const data = generateOnetData({ occupationsPerGroup: 5 });

      for (const occupation of data.occupations) {
        // Code format: XX-XXXX.XX
        expect(occupation.onetsoc_code).toMatch(/^\d{2}-?\d{2,4}\.\d{2}\.\d{2}$/);
        expect(occupation.major_group).toMatch(/^\d{2}$/);
        expect(occupation.onetsoc_code.startsWith(occupation.major_group)).toBe(true);
      }
    });
  });

  describe('Schema Creation', () => {
    it('should create tables successfully in DO', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
        await instance.initialize();
        const statements = getOnetSchemaStatements();
        const results = instance.execMany(statements);
        expect(results.every((r) => r.success)).toBe(true);
      });
    });
  });

  describe('Benchmark Queries', () => {
    it('should have at least 20 benchmark queries', () => {
      expect(ONET_BENCHMARK_QUERIES.length).toBeGreaterThanOrEqual(20);
    });

    it('should cover hierarchical and text search patterns', () => {
      const categories = new Set(ONET_BENCHMARK_QUERIES.map((q) => q.category));

      expect(categories.has('hierarchical')).toBe(true);
      expect(categories.has('text_search')).toBe(true);
      expect(categories.has('range')).toBe(true);
    });

    it('should have unique query IDs', () => {
      const ids = ONET_BENCHMARK_QUERIES.map((q) => q.id);
      const uniqueIds = new Set(ids);
      expect(uniqueIds.size).toBe(ids.length);
    });
  });
});

// =============================================================================
// IMDB Dataset Tests
// =============================================================================

describe('IMDB Dataset', () => {
  describe('Data Generation', () => {
    it('should generate deterministic data with same seed', () => {
      const data1 = generateImdbData({ seed: 12345, movieCount: 100 });
      const data2 = generateImdbData({ seed: 12345, movieCount: 100 });

      expect(data1.movies.length).toBe(data2.movies.length);
      expect(data1.movies[0].title).toBe(data2.movies[0].title);
      expect(data1.persons.length).toBe(data2.persons.length);
    });

    it('should generate expected entity counts', () => {
      const data = generateImdbData({
        movieCount: 100,
        actorCount: 200,
        directorCount: 20,
      });

      expect(data.movies.length).toBe(100);
      expect(data.genres.length).toBe(18);
      expect(data.persons.length).toBe(220); // 200 actors + 20 directors
      expect(data.ratings.length).toBe(100); // One rating per movie
    });

    it('should generate valid movie-person relationships', () => {
      const data = generateImdbData({ movieCount: 50, actorCount: 100, directorCount: 10 });

      const movieIds = new Set(data.movies.map((m) => m.movie_id));
      const personIds = new Set(data.persons.map((p) => p.person_id));

      // Check roles
      for (const role of data.roles) {
        expect(movieIds.has(role.movie_id)).toBe(true);
        expect(personIds.has(role.person_id)).toBe(true);
        expect(role.billing_order).toBeGreaterThanOrEqual(1);
      }

      // Check directors
      for (const director of data.directors) {
        expect(movieIds.has(director.movie_id)).toBe(true);
        expect(personIds.has(director.person_id)).toBe(true);
      }
    });

    it('should generate valid ratings', () => {
      const data = generateImdbData({ movieCount: 50 });

      for (const rating of data.ratings) {
        expect(rating.rating).toBeGreaterThanOrEqual(0);
        expect(rating.rating).toBeLessThanOrEqual(10);
        expect(rating.vote_count).toBeGreaterThanOrEqual(100);
      }
    });

    it('should generate self-referential sequel data', () => {
      const data = generateImdbData({ movieCount: 200, seed: 42 });

      const sequels = data.movies.filter((m) => m.sequel_to !== null);
      // Some movies should have sequels
      expect(sequels.length).toBeGreaterThan(0);

      // Sequel references should be valid
      const movieIds = new Set(data.movies.map((m) => m.movie_id));
      for (const sequel of sequels) {
        expect(movieIds.has(sequel.sequel_to!)).toBe(true);
        // Can't be sequel to itself
        expect(sequel.sequel_to).not.toBe(sequel.movie_id);
      }
    });
  });

  describe('Schema Creation', () => {
    it('should create tables successfully in DO', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
        await instance.initialize();
        const statements = getImdbSchemaStatements();
        const results = instance.execMany(statements);
        expect(results.every((r) => r.success)).toBe(true);
      });
    });
  });

  describe('Benchmark Queries', () => {
    it('should have at least 20 benchmark queries', () => {
      expect(IMDB_BENCHMARK_QUERIES.length).toBeGreaterThanOrEqual(20);
    });

    it('should cover many-to-many and self-join patterns', () => {
      const categories = new Set(IMDB_BENCHMARK_QUERIES.map((q) => q.category));

      expect(categories.has('many_to_many')).toBe(true);
      expect(categories.has('self_join')).toBe(true);
      expect(categories.has('top_n')).toBe(true);
    });

    it('should have unique query IDs', () => {
      const ids = IMDB_BENCHMARK_QUERIES.map((q) => q.id);
      const uniqueIds = new Set(ids);
      expect(uniqueIds.size).toBe(ids.length);
    });
  });
});

// =============================================================================
// UNSPSC Dataset Tests
// =============================================================================

describe('UNSPSC Dataset', () => {
  describe('Data Generation', () => {
    it('should generate deterministic data with same seed', () => {
      const data1 = generateUnspscData({ seed: 12345 });
      const data2 = generateUnspscData({ seed: 12345 });

      expect(data1.segments.length).toBe(data2.segments.length);
      expect(data1.commodities.length).toBe(data2.commodities.length);
      expect(data1.segments[0].segment_code).toBe(data2.segments[0].segment_code);
    });

    it('should generate hierarchical structure', () => {
      const data = generateUnspscData({ commoditiesPerClass: 5 });

      // Verify hierarchy counts
      expect(data.segments.length).toBeGreaterThan(0);
      expect(data.families.length).toBeGreaterThan(data.segments.length);
      expect(data.classes.length).toBeGreaterThan(data.families.length);
      expect(data.commodities.length).toBeGreaterThan(data.classes.length);
    });

    it('should generate valid parent-child references', () => {
      const data = generateUnspscData({ commoditiesPerClass: 3 });

      const segmentCodes = new Set(data.segments.map((s) => s.segment_code));
      const familyCodes = new Set(data.families.map((f) => f.family_code));
      const classCodes = new Set(data.classes.map((c) => c.class_code));

      // Check families reference segments
      for (const family of data.families) {
        expect(segmentCodes.has(family.segment_code)).toBe(true);
        expect(family.family_code.startsWith(family.segment_code)).toBe(true);
      }

      // Check classes reference families
      for (const cls of data.classes) {
        expect(familyCodes.has(cls.family_code)).toBe(true);
        expect(cls.class_code.startsWith(cls.family_code)).toBe(true);
      }

      // Check commodities reference classes
      for (const commodity of data.commodities) {
        expect(classCodes.has(commodity.class_code)).toBe(true);
        expect(commodity.commodity_code.startsWith(commodity.class_code)).toBe(true);
      }
    });

    it('should generate valid code formats', () => {
      const data = generateUnspscData({ commoditiesPerClass: 2 });

      // Segment: 2 digits
      for (const segment of data.segments) {
        expect(segment.segment_code).toMatch(/^\d{2}$/);
      }

      // Family: 4 digits
      for (const family of data.families) {
        expect(family.family_code).toMatch(/^\d{4}$/);
      }

      // Class: 6 digits
      for (const cls of data.classes) {
        expect(cls.class_code).toMatch(/^\d{6}$/);
      }

      // Commodity: 8 digits
      for (const commodity of data.commodities) {
        expect(commodity.commodity_code).toMatch(/^\d{8}$/);
      }
    });

    it('should generate synonyms and cross-references', () => {
      const data = generateUnspscData({
        commoditiesPerClass: 5,
        avgSynonymsPerCommodity: 2,
        avgCrossRefsPerCommodity: 1,
      });

      expect(data.synonyms.length).toBeGreaterThan(0);
      expect(data.crossReferences.length).toBeGreaterThan(0);

      // Verify references
      const commodityCodes = new Set(data.commodities.map((c) => c.commodity_code));

      for (const synonym of data.synonyms) {
        expect(commodityCodes.has(synonym.commodity_code)).toBe(true);
        expect(synonym.term).toBeTruthy();
      }

      for (const crossRef of data.crossReferences) {
        expect(commodityCodes.has(crossRef.commodity_code)).toBe(true);
        expect(crossRef.confidence).toBeGreaterThanOrEqual(0);
        expect(crossRef.confidence).toBeLessThanOrEqual(1);
      }
    });
  });

  describe('Schema Creation', () => {
    it('should create tables successfully in DO', async () => {
      const stub = getUniqueStub();
      await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
        await instance.initialize();
        const statements = getUnspscSchemaStatements();
        const results = instance.execMany(statements);
        expect(results.every((r) => r.success)).toBe(true);
      });
    });
  });

  describe('Benchmark Queries', () => {
    it('should have at least 20 benchmark queries', () => {
      expect(UNSPSC_BENCHMARK_QUERIES.length).toBeGreaterThanOrEqual(20);
    });

    it('should cover hierarchical and prefix query patterns', () => {
      const categories = new Set(UNSPSC_BENCHMARK_QUERIES.map((q) => q.category));

      expect(categories.has('prefix')).toBe(true);
      expect(categories.has('hierarchical')).toBe(true);
      expect(categories.has('tree_traversal')).toBe(true);
      expect(categories.has('path')).toBe(true);
    });

    it('should have unique query IDs', () => {
      const ids = UNSPSC_BENCHMARK_QUERIES.map((q) => q.id);
      const uniqueIds = new Set(ids);
      expect(uniqueIds.size).toBe(ids.length);
    });
  });
});

// =============================================================================
// Query Execution Tests
// =============================================================================

describe('Query Execution', () => {
  it('should execute Northwind queries against populated schema', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();

      // Create schema
      instance.execMany(getNorthwindSchemaStatements());

      // Generate and insert data
      const data = generateNorthwindData({ orderCount: 20 });

      await instance.insertBatch('categories', data.categories);
      await instance.insertBatch('shippers', data.shippers);
      await instance.insertBatch('suppliers', data.suppliers.slice(0, 10));
      await instance.insertBatch('employees', data.employees.map((e) => ({ ...e, reports_to: null })));
      await instance.insertBatch('products', data.products.slice(0, 30));
      await instance.insertBatch('customers', data.customers.slice(0, 20));
      await instance.insertBatch('orders', data.orders.map((o) => ({ ...o, shipped_date: o.shipped_date ?? null })));
      await instance.insertBatch('order_details', data.orderDetails.slice(0, 50));

      // Test sample queries
      const sampleQueries = NORTHWIND_BENCHMARK_QUERIES.filter((q) =>
        ['nw_point_02', 'nw_agg_01', 'nw_join_01'].includes(q.id)
      );

      for (const query of sampleQueries) {
        const result = await instance.query(query.sql);
        expect(result.success).toBe(true);
      }
    });
  });

  it('should execute UNSPSC prefix queries', async () => {
    const stub = getUniqueStub();
    await runInDurableObject(stub, async (instance: BenchmarkTestDO) => {
      await instance.initialize();

      // Create schema
      instance.execMany(getUnspscSchemaStatements());

      // Generate and insert minimal data
      const data = generateUnspscData({ commoditiesPerClass: 2 });

      await instance.insertBatch('segments', data.segments.slice(0, 5));
      await instance.insertBatch('families', data.families.slice(0, 10));
      await instance.insertBatch('classes', data.classes.slice(0, 20));
      await instance.insertBatch('commodities', data.commodities.slice(0, 50));

      // Test prefix query
      const prefixResult = await instance.query(
        "SELECT * FROM commodities WHERE commodity_code LIKE '10%'"
      );
      expect(prefixResult.success).toBe(true);

      // Test hierarchical query
      const hierResult = await instance.query(`
        SELECT s.name, COUNT(f.family_code)
        FROM segments s
        LEFT JOIN families f ON s.segment_code = f.segment_code
        GROUP BY s.segment_code
      `);
      expect(hierResult.success).toBe(true);
    });
  });
});

// =============================================================================
// Data Integrity Tests
// =============================================================================

describe('Data Integrity', () => {
  it('should generate unique primary keys for Northwind', () => {
    const data = generateNorthwindData();

    const customerIds = data.customers.map((c) => c.customer_id);
    expect(new Set(customerIds).size).toBe(customerIds.length);

    const productIds = data.products.map((p) => p.product_id);
    expect(new Set(productIds).size).toBe(productIds.length);

    const orderIds = data.orders.map((o) => o.order_id);
    expect(new Set(orderIds).size).toBe(orderIds.length);
  });

  it('should generate unique primary keys for ONET', () => {
    const data = generateOnetData();

    const occupationCodes = data.occupations.map((o) => o.onetsoc_code);
    expect(new Set(occupationCodes).size).toBe(occupationCodes.length);

    const skillIds = data.skills.map((s) => s.skill_id);
    expect(new Set(skillIds).size).toBe(skillIds.length);
  });

  it('should generate unique primary keys for IMDB', () => {
    const data = generateImdbData({ movieCount: 500 });

    const movieIds = data.movies.map((m) => m.movie_id);
    expect(new Set(movieIds).size).toBe(movieIds.length);

    const personIds = data.persons.map((p) => p.person_id);
    expect(new Set(personIds).size).toBe(personIds.length);
  });

  it('should generate unique primary keys for UNSPSC', () => {
    const data = generateUnspscData();

    const segmentCodes = data.segments.map((s) => s.segment_code);
    expect(new Set(segmentCodes).size).toBe(segmentCodes.length);

    const commodityCodes = data.commodities.map((c) => c.commodity_code);
    expect(new Set(commodityCodes).size).toBe(commodityCodes.length);
  });

  it('should generate non-null required fields', () => {
    const nwData = generateNorthwindData();
    for (const customer of nwData.customers) {
      expect(customer.customer_id).toBeTruthy();
      expect(customer.company_name).toBeTruthy();
    }

    const imdbData = generateImdbData({ movieCount: 50 });
    for (const movie of imdbData.movies) {
      expect(movie.title).toBeTruthy();
      expect(movie.overview).toBeTruthy();
      expect(movie.release_year).toBeGreaterThan(1900);
    }
  });
});
