#!/usr/bin/env npx tsx
/**
 * Real Performance Benchmarks using Northwind, ONET, and IMDB Datasets
 *
 * This benchmark compares DoSQL against better-sqlite3 using realistic
 * datasets and query patterns.
 *
 * Usage:
 *   npx tsx benchmarks/real-benchmark.ts
 */

import { Database } from '../src/database.js';
import {
  generateNorthwindData,
  getNorthwindSchemaStatements,
  NORTHWIND_BENCHMARK_QUERIES,
  type NorthwindDataset,
} from '../src/benchmarks/datasets/northwind.js';
import {
  generateOnetData,
  getOnetSchemaStatements,
  ONET_BENCHMARK_QUERIES,
  type OnetDataset,
} from '../src/benchmarks/datasets/onet.js';
import {
  generateImdbData,
  getImdbSchemaStatements,
  IMDB_BENCHMARK_QUERIES,
  type ImdbDataset,
} from '../src/benchmarks/datasets/imdb.js';
import type BetterSqlite3 from 'better-sqlite3';

// =============================================================================
// Types
// =============================================================================

interface BenchmarkResult {
  dataset: string;
  queryId: string;
  queryName: string;
  category: string;
  dosqlMs: number;
  sqliteMs: number;
  ratio: number;
  dosqlRows: number;
  sqliteRows: number;
  success: boolean;
  error?: string;
}

interface DatasetStats {
  name: string;
  totalRows: number;
  tables: { name: string; rows: number }[];
  loadTimeDoSQL: number;
  loadTimeSQLite: number;
}

// =============================================================================
// Utility Functions
// =============================================================================

function formatMs(ms: number): string {
  if (ms < 0.01) return '<0.01';
  if (ms < 1) return ms.toFixed(2);
  if (ms < 100) return ms.toFixed(1);
  return ms.toFixed(0);
}

function formatRatio(ratio: number): string {
  if (ratio < 1) return `${(1 / ratio).toFixed(2)}x faster`;
  if (ratio === 1) return '1.00x';
  return `${ratio.toFixed(2)}x`;
}

function runTimed<T>(fn: () => T): { result: T; ms: number } {
  const start = performance.now();
  const result = fn();
  const ms = performance.now() - start;
  return { result, ms };
}

// =============================================================================
// DoSQL Database Helpers
// =============================================================================

function loadNorthwindIntoDoSQL(db: Database, data: NorthwindDataset): number {
  let totalRows = 0;

  // Insert categories
  const insertCategory = db.prepare(`INSERT INTO categories (category_id, category_name, description) VALUES (?, ?, ?)`);
  for (const cat of data.categories) {
    insertCategory.run(cat.category_id, cat.category_name, cat.description);
    totalRows++;
  }

  // Insert shippers
  const insertShipper = db.prepare(`INSERT INTO shippers (shipper_id, company_name, phone) VALUES (?, ?, ?)`);
  for (const s of data.shippers) {
    insertShipper.run(s.shipper_id, s.company_name, s.phone);
    totalRows++;
  }

  // Insert suppliers
  const insertSupplier = db.prepare(`INSERT INTO suppliers (supplier_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax, home_page) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
  for (const s of data.suppliers) {
    insertSupplier.run(s.supplier_id, s.company_name, s.contact_name, s.contact_title, s.address, s.city, s.region, s.postal_code, s.country, s.phone, s.fax, s.home_page);
    totalRows++;
  }

  // Insert products
  const insertProduct = db.prepare(`INSERT INTO products (product_id, product_name, supplier_id, category_id, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
  for (const p of data.products) {
    insertProduct.run(p.product_id, p.product_name, p.supplier_id, p.category_id, p.quantity_per_unit, p.unit_price, p.units_in_stock, p.units_on_order, p.reorder_level, p.discontinued);
    totalRows++;
  }

  // Insert employees
  const insertEmployee = db.prepare(`INSERT INTO employees (employee_id, last_name, first_name, title, title_of_courtesy, birth_date, hire_date, address, city, region, postal_code, country, home_phone, extension, notes, reports_to) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
  for (const e of data.employees) {
    insertEmployee.run(e.employee_id, e.last_name, e.first_name, e.title, e.title_of_courtesy, e.birth_date, e.hire_date, e.address, e.city, e.region, e.postal_code, e.country, e.home_phone, e.extension, e.notes, e.reports_to);
    totalRows++;
  }

  // Insert customers
  const insertCustomer = db.prepare(`INSERT INTO customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
  for (const c of data.customers) {
    insertCustomer.run(c.customer_id, c.company_name, c.contact_name, c.contact_title, c.address, c.city, c.region, c.postal_code, c.country, c.phone, c.fax);
    totalRows++;
  }

  // Insert orders
  const insertOrder = db.prepare(`INSERT INTO orders (order_id, customer_id, employee_id, order_date, required_date, shipped_date, ship_via, freight, ship_name, ship_address, ship_city, ship_region, ship_postal_code, ship_country) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
  for (const o of data.orders) {
    insertOrder.run(o.order_id, o.customer_id, o.employee_id, o.order_date, o.required_date, o.shipped_date, o.ship_via, o.freight, o.ship_name, o.ship_address, o.ship_city, o.ship_region, o.ship_postal_code, o.ship_country);
    totalRows++;
  }

  // Insert order details
  const insertOrderDetail = db.prepare(`INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount) VALUES (?, ?, ?, ?, ?)`);
  for (const od of data.orderDetails) {
    insertOrderDetail.run(od.order_id, od.product_id, od.unit_price, od.quantity, od.discount);
    totalRows++;
  }

  return totalRows;
}

function loadOnetIntoDoSQL(db: Database, data: OnetDataset): number {
  let totalRows = 0;

  // Insert occupations
  const insertOccupation = db.prepare(`INSERT INTO occupations (onetsoc_code, title, description, major_group, minor_group, broad_occupation, job_zone, education_level, experience_months, training_level) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
  for (const o of data.occupations) {
    insertOccupation.run(o.onetsoc_code, o.title, o.description, o.major_group, o.minor_group, o.broad_occupation, o.job_zone, o.education_level, o.experience_months, o.training_level);
    totalRows++;
  }

  // Insert skills
  const insertSkill = db.prepare(`INSERT INTO skills (skill_id, name, description, category) VALUES (?, ?, ?, ?)`);
  for (const s of data.skills) {
    insertSkill.run(s.skill_id, s.name, s.description, s.category);
    totalRows++;
  }

  // Insert occupation_skills
  const insertOccSkill = db.prepare(`INSERT INTO occupation_skills (onetsoc_code, skill_id, importance, level, data_source) VALUES (?, ?, ?, ?, ?)`);
  for (const os of data.occupationSkills) {
    insertOccSkill.run(os.onetsoc_code, os.skill_id, os.importance, os.level, os.data_source);
    totalRows++;
  }

  // Insert abilities
  const insertAbility = db.prepare(`INSERT INTO abilities (ability_id, name, description, category) VALUES (?, ?, ?, ?)`);
  for (const a of data.abilities) {
    insertAbility.run(a.ability_id, a.name, a.description, a.category);
    totalRows++;
  }

  // Insert occupation_abilities
  const insertOccAbility = db.prepare(`INSERT INTO occupation_abilities (onetsoc_code, ability_id, importance, level) VALUES (?, ?, ?, ?)`);
  for (const oa of data.occupationAbilities) {
    insertOccAbility.run(oa.onetsoc_code, oa.ability_id, oa.importance, oa.level);
    totalRows++;
  }

  // Insert knowledge
  const insertKnowledge = db.prepare(`INSERT INTO knowledge (knowledge_id, name, description, category) VALUES (?, ?, ?, ?)`);
  for (const k of data.knowledge) {
    insertKnowledge.run(k.knowledge_id, k.name, k.description, k.category);
    totalRows++;
  }

  // Insert occupation_knowledge
  const insertOccKnowledge = db.prepare(`INSERT INTO occupation_knowledge (onetsoc_code, knowledge_id, importance, level) VALUES (?, ?, ?, ?)`);
  for (const ok of data.occupationKnowledge) {
    insertOccKnowledge.run(ok.onetsoc_code, ok.knowledge_id, ok.importance, ok.level);
    totalRows++;
  }

  // Insert work_activities
  const insertActivity = db.prepare(`INSERT INTO work_activities (activity_id, name, description, category) VALUES (?, ?, ?, ?)`);
  for (const wa of data.workActivities) {
    insertActivity.run(wa.activity_id, wa.name, wa.description, wa.category);
    totalRows++;
  }

  // Insert occupation_activities
  const insertOccActivity = db.prepare(`INSERT INTO occupation_activities (onetsoc_code, activity_id, importance, level) VALUES (?, ?, ?, ?)`);
  for (const oa of data.occupationActivities) {
    insertOccActivity.run(oa.onetsoc_code, oa.activity_id, oa.importance, oa.level);
    totalRows++;
  }

  // Insert work_contexts
  const insertContext = db.prepare(`INSERT INTO work_contexts (context_id, name, description, category) VALUES (?, ?, ?, ?)`);
  for (const wc of data.workContexts) {
    insertContext.run(wc.context_id, wc.name, wc.description, wc.category);
    totalRows++;
  }

  // Insert occupation_contexts
  const insertOccContext = db.prepare(`INSERT INTO occupation_contexts (onetsoc_code, context_id, rating, category_description) VALUES (?, ?, ?, ?)`);
  for (const oc of data.occupationContexts) {
    insertOccContext.run(oc.onetsoc_code, oc.context_id, oc.rating, oc.category_description);
    totalRows++;
  }

  return totalRows;
}

function loadImdbIntoDoSQL(db: Database, data: ImdbDataset): number {
  let totalRows = 0;

  // Insert genres
  const insertGenre = db.prepare(`INSERT INTO genres (genre_id, name, description) VALUES (?, ?, ?)`);
  for (const g of data.genres) {
    insertGenre.run(g.genre_id, g.name, g.description);
    totalRows++;
  }

  // Insert movies
  const insertMovie = db.prepare(`INSERT INTO movies (movie_id, title, original_title, release_year, runtime_minutes, genre, genre_secondary, budget, revenue, tagline, overview, production_country, original_language, status, sequel_to) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
  for (const m of data.movies) {
    insertMovie.run(m.movie_id, m.title, m.original_title, m.release_year, m.runtime_minutes, m.genre, m.genre_secondary, m.budget, m.revenue, m.tagline, m.overview, m.production_country, m.original_language, m.status, m.sequel_to);
    totalRows++;
  }

  // Insert movie_genres
  const insertMovieGenre = db.prepare(`INSERT INTO movie_genres (movie_id, genre_id, is_primary) VALUES (?, ?, ?)`);
  for (const mg of data.movieGenres) {
    insertMovieGenre.run(mg.movie_id, mg.genre_id, mg.is_primary);
    totalRows++;
  }

  // Insert persons
  const insertPerson = db.prepare(`INSERT INTO persons (person_id, name, birth_date, death_date, birth_place, biography, primary_profession, gender, popularity) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`);
  for (const p of data.persons) {
    insertPerson.run(p.person_id, p.name, p.birth_date, p.death_date, p.birth_place, p.biography, p.primary_profession, p.gender, p.popularity);
    totalRows++;
  }

  // Insert roles
  const insertRole = db.prepare(`INSERT INTO roles (movie_id, person_id, character_name, billing_order, role_type) VALUES (?, ?, ?, ?, ?)`);
  for (const r of data.roles) {
    insertRole.run(r.movie_id, r.person_id, r.character_name, r.billing_order, r.role_type);
    totalRows++;
  }

  // Insert directors
  const insertDirector = db.prepare(`INSERT INTO directors (movie_id, person_id, is_co_director) VALUES (?, ?, ?)`);
  for (const d of data.directors) {
    insertDirector.run(d.movie_id, d.person_id, d.is_co_director);
    totalRows++;
  }

  // Insert ratings
  const insertRating = db.prepare(`INSERT INTO ratings (movie_id, rating, vote_count, critic_score, audience_score, updated_at) VALUES (?, ?, ?, ?, ?, ?)`);
  for (const r of data.ratings) {
    insertRating.run(r.movie_id, r.rating, r.vote_count, r.critic_score, r.audience_score, r.updated_at);
    totalRows++;
  }

  // Insert awards
  const insertAward = db.prepare(`INSERT INTO awards (award_id, movie_id, person_id, ceremony, category, year, won) VALUES (?, ?, ?, ?, ?, ?, ?)`);
  for (const a of data.awards) {
    insertAward.run(a.award_id, a.movie_id, a.person_id, a.ceremony, a.category, a.year, a.won);
    totalRows++;
  }

  return totalRows;
}

// =============================================================================
// better-sqlite3 Helpers
// =============================================================================

function loadNorthwindIntoSQLite(db: BetterSqlite3.Database, data: NorthwindDataset): number {
  let totalRows = 0;

  // Use transactions for faster loading
  const loadTx = db.transaction(() => {
    // Insert categories
    const insertCategory = db.prepare(`INSERT INTO categories (category_id, category_name, description) VALUES (?, ?, ?)`);
    for (const cat of data.categories) {
      insertCategory.run(cat.category_id, cat.category_name, cat.description);
      totalRows++;
    }

    // Insert shippers
    const insertShipper = db.prepare(`INSERT INTO shippers (shipper_id, company_name, phone) VALUES (?, ?, ?)`);
    for (const s of data.shippers) {
      insertShipper.run(s.shipper_id, s.company_name, s.phone);
      totalRows++;
    }

    // Insert suppliers
    const insertSupplier = db.prepare(`INSERT INTO suppliers (supplier_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax, home_page) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    for (const s of data.suppliers) {
      insertSupplier.run(s.supplier_id, s.company_name, s.contact_name, s.contact_title, s.address, s.city, s.region, s.postal_code, s.country, s.phone, s.fax, s.home_page);
      totalRows++;
    }

    // Insert products
    const insertProduct = db.prepare(`INSERT INTO products (product_id, product_name, supplier_id, category_id, quantity_per_unit, unit_price, units_in_stock, units_on_order, reorder_level, discontinued) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    for (const p of data.products) {
      insertProduct.run(p.product_id, p.product_name, p.supplier_id, p.category_id, p.quantity_per_unit, p.unit_price, p.units_in_stock, p.units_on_order, p.reorder_level, p.discontinued);
      totalRows++;
    }

    // Insert employees
    const insertEmployee = db.prepare(`INSERT INTO employees (employee_id, last_name, first_name, title, title_of_courtesy, birth_date, hire_date, address, city, region, postal_code, country, home_phone, extension, notes, reports_to) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    for (const e of data.employees) {
      insertEmployee.run(e.employee_id, e.last_name, e.first_name, e.title, e.title_of_courtesy, e.birth_date, e.hire_date, e.address, e.city, e.region, e.postal_code, e.country, e.home_phone, e.extension, e.notes, e.reports_to);
      totalRows++;
    }

    // Insert customers
    const insertCustomer = db.prepare(`INSERT INTO customers (customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    for (const c of data.customers) {
      insertCustomer.run(c.customer_id, c.company_name, c.contact_name, c.contact_title, c.address, c.city, c.region, c.postal_code, c.country, c.phone, c.fax);
      totalRows++;
    }

    // Insert orders
    const insertOrder = db.prepare(`INSERT INTO orders (order_id, customer_id, employee_id, order_date, required_date, shipped_date, ship_via, freight, ship_name, ship_address, ship_city, ship_region, ship_postal_code, ship_country) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    for (const o of data.orders) {
      insertOrder.run(o.order_id, o.customer_id, o.employee_id, o.order_date, o.required_date, o.shipped_date, o.ship_via, o.freight, o.ship_name, o.ship_address, o.ship_city, o.ship_region, o.ship_postal_code, o.ship_country);
      totalRows++;
    }

    // Insert order details
    const insertOrderDetail = db.prepare(`INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount) VALUES (?, ?, ?, ?, ?)`);
    for (const od of data.orderDetails) {
      insertOrderDetail.run(od.order_id, od.product_id, od.unit_price, od.quantity, od.discount);
      totalRows++;
    }
  });

  loadTx();
  return totalRows;
}

function loadOnetIntoSQLite(db: BetterSqlite3.Database, data: OnetDataset): number {
  let totalRows = 0;

  const loadTx = db.transaction(() => {
    // Insert occupations
    const insertOccupation = db.prepare(`INSERT INTO occupations (onetsoc_code, title, description, major_group, minor_group, broad_occupation, job_zone, education_level, experience_months, training_level) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    for (const o of data.occupations) {
      insertOccupation.run(o.onetsoc_code, o.title, o.description, o.major_group, o.minor_group, o.broad_occupation, o.job_zone, o.education_level, o.experience_months, o.training_level);
      totalRows++;
    }

    // Insert skills
    const insertSkill = db.prepare(`INSERT INTO skills (skill_id, name, description, category) VALUES (?, ?, ?, ?)`);
    for (const s of data.skills) {
      insertSkill.run(s.skill_id, s.name, s.description, s.category);
      totalRows++;
    }

    // Insert occupation_skills
    const insertOccSkill = db.prepare(`INSERT INTO occupation_skills (onetsoc_code, skill_id, importance, level, data_source) VALUES (?, ?, ?, ?, ?)`);
    for (const os of data.occupationSkills) {
      insertOccSkill.run(os.onetsoc_code, os.skill_id, os.importance, os.level, os.data_source);
      totalRows++;
    }

    // Insert abilities
    const insertAbility = db.prepare(`INSERT INTO abilities (ability_id, name, description, category) VALUES (?, ?, ?, ?)`);
    for (const a of data.abilities) {
      insertAbility.run(a.ability_id, a.name, a.description, a.category);
      totalRows++;
    }

    // Insert occupation_abilities
    const insertOccAbility = db.prepare(`INSERT INTO occupation_abilities (onetsoc_code, ability_id, importance, level) VALUES (?, ?, ?, ?)`);
    for (const oa of data.occupationAbilities) {
      insertOccAbility.run(oa.onetsoc_code, oa.ability_id, oa.importance, oa.level);
      totalRows++;
    }

    // Insert knowledge
    const insertKnowledge = db.prepare(`INSERT INTO knowledge (knowledge_id, name, description, category) VALUES (?, ?, ?, ?)`);
    for (const k of data.knowledge) {
      insertKnowledge.run(k.knowledge_id, k.name, k.description, k.category);
      totalRows++;
    }

    // Insert occupation_knowledge
    const insertOccKnowledge = db.prepare(`INSERT INTO occupation_knowledge (onetsoc_code, knowledge_id, importance, level) VALUES (?, ?, ?, ?)`);
    for (const ok of data.occupationKnowledge) {
      insertOccKnowledge.run(ok.onetsoc_code, ok.knowledge_id, ok.importance, ok.level);
      totalRows++;
    }

    // Insert work_activities
    const insertActivity = db.prepare(`INSERT INTO work_activities (activity_id, name, description, category) VALUES (?, ?, ?, ?)`);
    for (const wa of data.workActivities) {
      insertActivity.run(wa.activity_id, wa.name, wa.description, wa.category);
      totalRows++;
    }

    // Insert occupation_activities
    const insertOccActivity = db.prepare(`INSERT INTO occupation_activities (onetsoc_code, activity_id, importance, level) VALUES (?, ?, ?, ?)`);
    for (const oa of data.occupationActivities) {
      insertOccActivity.run(oa.onetsoc_code, oa.activity_id, oa.importance, oa.level);
      totalRows++;
    }

    // Insert work_contexts
    const insertContext = db.prepare(`INSERT INTO work_contexts (context_id, name, description, category) VALUES (?, ?, ?, ?)`);
    for (const wc of data.workContexts) {
      insertContext.run(wc.context_id, wc.name, wc.description, wc.category);
      totalRows++;
    }

    // Insert occupation_contexts
    const insertOccContext = db.prepare(`INSERT INTO occupation_contexts (onetsoc_code, context_id, rating, category_description) VALUES (?, ?, ?, ?)`);
    for (const oc of data.occupationContexts) {
      insertOccContext.run(oc.onetsoc_code, oc.context_id, oc.rating, oc.category_description);
      totalRows++;
    }
  });

  loadTx();
  return totalRows;
}

function loadImdbIntoSQLite(db: BetterSqlite3.Database, data: ImdbDataset): number {
  let totalRows = 0;

  const loadTx = db.transaction(() => {
    // Insert genres
    const insertGenre = db.prepare(`INSERT INTO genres (genre_id, name, description) VALUES (?, ?, ?)`);
    for (const g of data.genres) {
      insertGenre.run(g.genre_id, g.name, g.description);
      totalRows++;
    }

    // Insert movies
    const insertMovie = db.prepare(`INSERT INTO movies (movie_id, title, original_title, release_year, runtime_minutes, genre, genre_secondary, budget, revenue, tagline, overview, production_country, original_language, status, sequel_to) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    for (const m of data.movies) {
      insertMovie.run(m.movie_id, m.title, m.original_title, m.release_year, m.runtime_minutes, m.genre, m.genre_secondary, m.budget, m.revenue, m.tagline, m.overview, m.production_country, m.original_language, m.status, m.sequel_to);
      totalRows++;
    }

    // Insert movie_genres
    const insertMovieGenre = db.prepare(`INSERT INTO movie_genres (movie_id, genre_id, is_primary) VALUES (?, ?, ?)`);
    for (const mg of data.movieGenres) {
      insertMovieGenre.run(mg.movie_id, mg.genre_id, mg.is_primary);
      totalRows++;
    }

    // Insert persons
    const insertPerson = db.prepare(`INSERT INTO persons (person_id, name, birth_date, death_date, birth_place, biography, primary_profession, gender, popularity) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    for (const p of data.persons) {
      insertPerson.run(p.person_id, p.name, p.birth_date, p.death_date, p.birth_place, p.biography, p.primary_profession, p.gender, p.popularity);
      totalRows++;
    }

    // Insert roles
    const insertRole = db.prepare(`INSERT INTO roles (movie_id, person_id, character_name, billing_order, role_type) VALUES (?, ?, ?, ?, ?)`);
    for (const r of data.roles) {
      insertRole.run(r.movie_id, r.person_id, r.character_name, r.billing_order, r.role_type);
      totalRows++;
    }

    // Insert directors
    const insertDirector = db.prepare(`INSERT INTO directors (movie_id, person_id, is_co_director) VALUES (?, ?, ?)`);
    for (const d of data.directors) {
      insertDirector.run(d.movie_id, d.person_id, d.is_co_director);
      totalRows++;
    }

    // Insert ratings
    const insertRating = db.prepare(`INSERT INTO ratings (movie_id, rating, vote_count, critic_score, audience_score, updated_at) VALUES (?, ?, ?, ?, ?, ?)`);
    for (const r of data.ratings) {
      insertRating.run(r.movie_id, r.rating, r.vote_count, r.critic_score, r.audience_score, r.updated_at);
      totalRows++;
    }

    // Insert awards
    const insertAward = db.prepare(`INSERT INTO awards (award_id, movie_id, person_id, ceremony, category, year, won) VALUES (?, ?, ?, ?, ?, ?, ?)`);
    for (const a of data.awards) {
      insertAward.run(a.award_id, a.movie_id, a.person_id, a.ceremony, a.category, a.year, a.won);
      totalRows++;
    }
  });

  loadTx();
  return totalRows;
}

// =============================================================================
// Benchmark Execution
// =============================================================================

async function runQueryBenchmark(
  dataset: string,
  query: { id: string; name: string; category: string; sql: string },
  dosqlDb: Database,
  sqliteDb: BetterSqlite3.Database,
  iterations: number = 5
): Promise<BenchmarkResult> {
  const dosqlTimes: number[] = [];
  const sqliteTimes: number[] = [];
  let dosqlRows = 0;
  let sqliteRows = 0;
  let error: string | undefined;

  // Warmup
  try {
    dosqlDb.prepare(query.sql).all();
    sqliteDb.prepare(query.sql).all();
  } catch (e) {
    error = e instanceof Error ? e.message : String(e);
  }

  // Run multiple iterations
  for (let i = 0; i < iterations; i++) {
    // DoSQL
    try {
      const dosqlStart = performance.now();
      const dosqlResult = dosqlDb.prepare(query.sql).all();
      const dosqlEnd = performance.now();
      dosqlTimes.push(dosqlEnd - dosqlStart);
      dosqlRows = (dosqlResult as unknown[]).length;
    } catch (e) {
      error = `DoSQL: ${e instanceof Error ? e.message : String(e)}`;
      dosqlTimes.push(Infinity);
    }

    // SQLite
    try {
      const sqliteStart = performance.now();
      const sqliteResult = sqliteDb.prepare(query.sql).all();
      const sqliteEnd = performance.now();
      sqliteTimes.push(sqliteEnd - sqliteStart);
      sqliteRows = sqliteResult.length;
    } catch (e) {
      error = `SQLite: ${e instanceof Error ? e.message : String(e)}`;
      sqliteTimes.push(Infinity);
    }
  }

  // Calculate medians
  const sortedDosql = [...dosqlTimes].sort((a, b) => a - b);
  const sortedSqlite = [...sqliteTimes].sort((a, b) => a - b);
  const dosqlMs = sortedDosql[Math.floor(sortedDosql.length / 2)];
  const sqliteMs = sortedSqlite[Math.floor(sortedSqlite.length / 2)];

  const ratio = sqliteMs > 0 ? dosqlMs / sqliteMs : Infinity;

  return {
    dataset,
    queryId: query.id,
    queryName: query.name,
    category: query.category,
    dosqlMs,
    sqliteMs,
    ratio,
    dosqlRows,
    sqliteRows,
    success: !error && dosqlMs !== Infinity && sqliteMs !== Infinity,
    error,
  };
}

// =============================================================================
// Main
// =============================================================================

async function main() {
  console.log('='.repeat(80));
  console.log('DoSQL vs better-sqlite3 Real Dataset Benchmark');
  console.log('='.repeat(80));
  console.log();
  console.log('NOTE: DoSQL currently has limited JOIN support. Queries that fail');
  console.log('      due to JOIN limitations are excluded from the comparison.');
  console.log();

  // Dynamic import for better-sqlite3
  const BetterSqlite3Module = await import('better-sqlite3');
  const BetterSqlite3 = BetterSqlite3Module.default;

  const results: BenchmarkResult[] = [];
  const datasetStats: DatasetStats[] = [];

  // ==========================================================================
  // Northwind Dataset
  // ==========================================================================
  console.log('Loading Northwind dataset...');
  const northwindData = generateNorthwindData();

  // Create DoSQL database
  const dosqlNorthwind = new Database(':memory:');
  const northwindSchema = getNorthwindSchemaStatements();
  for (const stmt of northwindSchema) {
    dosqlNorthwind.exec(stmt.trim());
  }

  // Create SQLite database
  const sqliteNorthwind = new BetterSqlite3(':memory:');
  sqliteNorthwind.pragma('journal_mode = WAL');
  sqliteNorthwind.pragma('synchronous = OFF');
  for (const stmt of northwindSchema) {
    sqliteNorthwind.exec(stmt.trim());
  }

  // Load data
  const { ms: dosqlNorthwindLoadTime } = runTimed(() => loadNorthwindIntoDoSQL(dosqlNorthwind, northwindData));
  const { ms: sqliteNorthwindLoadTime, result: northwindRows } = runTimed(() => loadNorthwindIntoSQLite(sqliteNorthwind, northwindData));

  datasetStats.push({
    name: 'Northwind',
    totalRows: northwindRows,
    tables: [
      { name: 'categories', rows: northwindData.categories.length },
      { name: 'customers', rows: northwindData.customers.length },
      { name: 'employees', rows: northwindData.employees.length },
      { name: 'orders', rows: northwindData.orders.length },
      { name: 'order_details', rows: northwindData.orderDetails.length },
      { name: 'products', rows: northwindData.products.length },
      { name: 'shippers', rows: northwindData.shippers.length },
      { name: 'suppliers', rows: northwindData.suppliers.length },
    ],
    loadTimeDoSQL: dosqlNorthwindLoadTime,
    loadTimeSQLite: sqliteNorthwindLoadTime,
  });

  console.log(`  Loaded ${northwindRows.toLocaleString()} rows in ${formatMs(sqliteNorthwindLoadTime)}ms (SQLite) / ${formatMs(dosqlNorthwindLoadTime)}ms (DoSQL)`);

  // Run Northwind queries
  console.log(`  Running ${NORTHWIND_BENCHMARK_QUERIES.length} benchmark queries...`);
  for (const query of NORTHWIND_BENCHMARK_QUERIES) {
    const result = await runQueryBenchmark('Northwind', query, dosqlNorthwind, sqliteNorthwind);
    results.push(result);
  }

  dosqlNorthwind.close();
  sqliteNorthwind.close();

  // ==========================================================================
  // ONET Dataset (using smaller config for faster benchmarks)
  // ==========================================================================
  console.log('\nLoading O*NET dataset...');
  const onetData = generateOnetData({ occupationsPerGroup: 20 }); // Reduced for speed

  // Create DoSQL database
  const dosqlOnet = new Database(':memory:');
  const onetSchema = getOnetSchemaStatements();
  for (const stmt of onetSchema) {
    dosqlOnet.exec(stmt.trim());
  }

  // Create SQLite database
  const sqliteOnet = new BetterSqlite3(':memory:');
  sqliteOnet.pragma('journal_mode = WAL');
  sqliteOnet.pragma('synchronous = OFF');
  for (const stmt of onetSchema) {
    sqliteOnet.exec(stmt.trim());
  }

  // Load data
  const { ms: dosqlOnetLoadTime } = runTimed(() => loadOnetIntoDoSQL(dosqlOnet, onetData));
  const { ms: sqliteOnetLoadTime, result: onetRows } = runTimed(() => loadOnetIntoSQLite(sqliteOnet, onetData));

  datasetStats.push({
    name: 'O*NET',
    totalRows: onetRows,
    tables: [
      { name: 'occupations', rows: onetData.occupations.length },
      { name: 'skills', rows: onetData.skills.length },
      { name: 'occupation_skills', rows: onetData.occupationSkills.length },
      { name: 'abilities', rows: onetData.abilities.length },
      { name: 'occupation_abilities', rows: onetData.occupationAbilities.length },
      { name: 'knowledge', rows: onetData.knowledge.length },
      { name: 'occupation_knowledge', rows: onetData.occupationKnowledge.length },
      { name: 'work_activities', rows: onetData.workActivities.length },
      { name: 'occupation_activities', rows: onetData.occupationActivities.length },
      { name: 'work_contexts', rows: onetData.workContexts.length },
      { name: 'occupation_contexts', rows: onetData.occupationContexts.length },
    ],
    loadTimeDoSQL: dosqlOnetLoadTime,
    loadTimeSQLite: sqliteOnetLoadTime,
  });

  console.log(`  Loaded ${onetRows.toLocaleString()} rows in ${formatMs(sqliteOnetLoadTime)}ms (SQLite) / ${formatMs(dosqlOnetLoadTime)}ms (DoSQL)`);

  // Run ONET queries
  console.log(`  Running ${ONET_BENCHMARK_QUERIES.length} benchmark queries...`);
  for (const query of ONET_BENCHMARK_QUERIES) {
    const result = await runQueryBenchmark('O*NET', query, dosqlOnet, sqliteOnet);
    results.push(result);
  }

  dosqlOnet.close();
  sqliteOnet.close();

  // ==========================================================================
  // IMDB Dataset (using smaller config for faster benchmarks)
  // ==========================================================================
  console.log('\nLoading IMDB dataset...');
  const imdbData = generateImdbData({ movieCount: 2000, actorCount: 3000 }); // Reduced for speed

  // Create DoSQL database
  const dosqlImdb = new Database(':memory:');
  const imdbSchema = getImdbSchemaStatements();
  for (const stmt of imdbSchema) {
    dosqlImdb.exec(stmt.trim());
  }

  // Create SQLite database
  const sqliteImdb = new BetterSqlite3(':memory:');
  sqliteImdb.pragma('journal_mode = WAL');
  sqliteImdb.pragma('synchronous = OFF');
  for (const stmt of imdbSchema) {
    sqliteImdb.exec(stmt.trim());
  }

  // Load data
  const { ms: dosqlImdbLoadTime } = runTimed(() => loadImdbIntoDoSQL(dosqlImdb, imdbData));
  const { ms: sqliteImdbLoadTime, result: imdbRows } = runTimed(() => loadImdbIntoSQLite(sqliteImdb, imdbData));

  datasetStats.push({
    name: 'IMDB',
    totalRows: imdbRows,
    tables: [
      { name: 'genres', rows: imdbData.genres.length },
      { name: 'movies', rows: imdbData.movies.length },
      { name: 'movie_genres', rows: imdbData.movieGenres.length },
      { name: 'persons', rows: imdbData.persons.length },
      { name: 'roles', rows: imdbData.roles.length },
      { name: 'directors', rows: imdbData.directors.length },
      { name: 'ratings', rows: imdbData.ratings.length },
      { name: 'awards', rows: imdbData.awards.length },
    ],
    loadTimeDoSQL: dosqlImdbLoadTime,
    loadTimeSQLite: sqliteImdbLoadTime,
  });

  console.log(`  Loaded ${imdbRows.toLocaleString()} rows in ${formatMs(sqliteImdbLoadTime)}ms (SQLite) / ${formatMs(dosqlImdbLoadTime)}ms (DoSQL)`);

  // Run IMDB queries
  console.log(`  Running ${IMDB_BENCHMARK_QUERIES.length} benchmark queries...`);
  for (const query of IMDB_BENCHMARK_QUERIES) {
    const result = await runQueryBenchmark('IMDB', query, dosqlImdb, sqliteImdb);
    results.push(result);
  }

  dosqlImdb.close();
  sqliteImdb.close();

  // ==========================================================================
  // Output Results
  // ==========================================================================
  console.log('\n' + '='.repeat(80));
  console.log('DATASET STATISTICS');
  console.log('='.repeat(80));

  for (const stats of datasetStats) {
    console.log(`\n${stats.name}:`);
    console.log(`  Total Rows: ${stats.totalRows.toLocaleString()}`);
    console.log(`  Load Time: ${formatMs(stats.loadTimeSQLite)}ms (SQLite) / ${formatMs(stats.loadTimeDoSQL)}ms (DoSQL)`);
    console.log(`  Tables:`);
    for (const table of stats.tables) {
      console.log(`    - ${table.name}: ${table.rows.toLocaleString()} rows`);
    }
  }

  // ==========================================================================
  // Benchmark Results Table
  // ==========================================================================
  console.log('\n' + '='.repeat(80));
  console.log('BENCHMARK RESULTS');
  console.log('='.repeat(80));

  // Group by category for summary
  const categories = new Map<string, BenchmarkResult[]>();
  for (const result of results) {
    const key = `${result.dataset}:${result.category}`;
    if (!categories.has(key)) {
      categories.set(key, []);
    }
    categories.get(key)!.push(result);
  }

  // Print summary table
  console.log('\n' + '-'.repeat(100));
  console.log(
    'Dataset'.padEnd(12) +
    'Category'.padEnd(18) +
    'DoSQL (ms)'.padStart(12) +
    'SQLite (ms)'.padStart(14) +
    'Ratio'.padStart(15) +
    'Queries'.padStart(10)
  );
  console.log('-'.repeat(100));

  const categoryResults: { dataset: string; category: string; dosqlAvg: number; sqliteAvg: number; count: number }[] = [];

  for (const [key, categoryQueries] of categories) {
    const [dataset, category] = key.split(':');
    const successful = categoryQueries.filter(r => r.success);
    if (successful.length === 0) continue;

    const dosqlAvg = successful.reduce((sum, r) => sum + r.dosqlMs, 0) / successful.length;
    const sqliteAvg = successful.reduce((sum, r) => sum + r.sqliteMs, 0) / successful.length;
    const ratio = sqliteAvg > 0 ? dosqlAvg / sqliteAvg : Infinity;

    categoryResults.push({ dataset, category, dosqlAvg, sqliteAvg, count: successful.length });

    console.log(
      dataset.padEnd(12) +
      category.padEnd(18) +
      formatMs(dosqlAvg).padStart(12) +
      formatMs(sqliteAvg).padStart(14) +
      formatRatio(ratio).padStart(15) +
      String(successful.length).padStart(10)
    );
  }

  console.log('-'.repeat(100));

  // Overall summary
  const totalDosql = categoryResults.reduce((sum, r) => sum + r.dosqlAvg * r.count, 0);
  const totalSqlite = categoryResults.reduce((sum, r) => sum + r.sqliteAvg * r.count, 0);
  const totalQueries = categoryResults.reduce((sum, r) => sum + r.count, 0);
  const overallRatio = totalSqlite > 0 ? totalDosql / totalSqlite : Infinity;

  console.log(
    'OVERALL'.padEnd(30) +
    formatMs(totalDosql / totalQueries).padStart(12) +
    formatMs(totalSqlite / totalQueries).padStart(14) +
    formatRatio(overallRatio).padStart(15) +
    String(totalQueries).padStart(10)
  );

  // ==========================================================================
  // Detailed Results by Query Type
  // ==========================================================================
  console.log('\n' + '='.repeat(80));
  console.log('DETAILED RESULTS BY QUERY TYPE');
  console.log('='.repeat(80));

  const queryTypes = ['point_lookup', 'range', 'join', 'aggregate', 'text_search', 'subquery', 'hierarchical', 'top_n', 'many_to_many', 'self_join', 'date_filter'];

  for (const queryType of queryTypes) {
    const typeResults = results.filter(r => r.category === queryType && r.success);
    if (typeResults.length === 0) continue;

    console.log(`\n${queryType.toUpperCase().replace('_', ' ')}:`);
    console.log('-'.repeat(90));

    for (const result of typeResults) {
      const ratioStr = formatRatio(result.ratio);
      console.log(
        `  ${result.dataset.padEnd(10)} ${result.queryName.substring(0, 35).padEnd(37)} ` +
        `${formatMs(result.dosqlMs).padStart(8)}ms / ${formatMs(result.sqliteMs).padStart(8)}ms  ${ratioStr.padStart(12)}`
      );
    }
  }

  // ==========================================================================
  // Failed Queries
  // ==========================================================================
  const failed = results.filter(r => !r.success);
  if (failed.length > 0) {
    console.log('\n' + '='.repeat(80));
    console.log('FAILED QUERIES');
    console.log('='.repeat(80));
    for (const result of failed) {
      console.log(`  ${result.dataset} - ${result.queryName}: ${result.error}`);
    }
  }

  // ==========================================================================
  // Summary
  // ==========================================================================
  console.log('\n' + '='.repeat(80));
  console.log('SUMMARY');
  console.log('='.repeat(80));
  console.log(`Total Datasets: ${datasetStats.length}`);
  console.log(`Total Queries Attempted: ${results.length}`);
  console.log(`Successful: ${results.filter(r => r.success).length}`);
  console.log(`Failed (mostly JOIN-related): ${failed.length}`);
  console.log(`\nOverall DoSQL/SQLite Ratio (successful queries): ${formatRatio(overallRatio)}`);

  if (overallRatio > 1) {
    console.log(`\nDoSQL is approximately ${overallRatio.toFixed(1)}x slower than native SQLite.`);
    console.log('This is expected for a pure TypeScript implementation vs native C.');
  } else {
    console.log(`\nDoSQL is approximately ${(1/overallRatio).toFixed(1)}x faster than SQLite.`);
  }

  // ==========================================================================
  // Performance Analysis
  // ==========================================================================
  console.log('\n' + '='.repeat(80));
  console.log('PERFORMANCE ANALYSIS');
  console.log('='.repeat(80));

  // Categorize results by performance tier
  const successfulResults = results.filter(r => r.success);
  const fast = successfulResults.filter(r => r.ratio <= 10);
  const medium = successfulResults.filter(r => r.ratio > 10 && r.ratio <= 50);
  const slow = successfulResults.filter(r => r.ratio > 50 && r.ratio <= 100);
  const verySlow = successfulResults.filter(r => r.ratio > 100);

  console.log('\nPerformance Tiers:');
  console.log(`  <= 10x slower (acceptable):   ${fast.length} queries (${(fast.length/successfulResults.length*100).toFixed(0)}%)`);
  console.log(`  10-50x slower (moderate):     ${medium.length} queries (${(medium.length/successfulResults.length*100).toFixed(0)}%)`);
  console.log(`  50-100x slower (slow):        ${slow.length} queries (${(slow.length/successfulResults.length*100).toFixed(0)}%)`);
  console.log(`  > 100x slower (very slow):    ${verySlow.length} queries (${(verySlow.length/successfulResults.length*100).toFixed(0)}%)`);

  if (verySlow.length > 0) {
    console.log('\nVery slow queries (> 100x):');
    for (const r of verySlow) {
      console.log(`  - ${r.dataset}/${r.queryName}: ${formatRatio(r.ratio)}`);
    }
  }

  console.log('\n' + '='.repeat(80));
}

main().catch(console.error);
