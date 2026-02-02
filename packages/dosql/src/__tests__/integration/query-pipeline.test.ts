/**
 * DoSQL Integration Tests - Full Query Execution Pipeline
 *
 * Comprehensive tests for the complete SQL execution pipeline:
 * SQL String -> Parser -> Planner -> Executor -> Storage
 *
 * Covers:
 * - Simple SELECT, INSERT, UPDATE, DELETE
 * - JOINs (INNER, LEFT, RIGHT, FULL)
 * - Subqueries (scalar, IN, EXISTS, correlated)
 * - CTEs (simple and recursive)
 * - Window functions
 * - Aggregations with GROUP BY, HAVING
 * - Set operations (UNION, INTERSECT, EXCEPT)
 * - Transactions
 *
 * Following the project's TDD with NO MOCKS philosophy.
 * All tests run in actual Cloudflare Workers environment via @cloudflare/vitest-pool-workers.
 */

import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { Database } from '../../database.js';

// =============================================================================
// TEST SETUP
// =============================================================================

describe('Query Execution Pipeline Integration', () => {
  let db: Database;

  beforeEach(() => {
    db = new Database(':memory:');
  });

  afterEach(() => {
    db.close();
  });

  // ===========================================================================
  // SIMPLE CRUD OPERATIONS
  // ===========================================================================

  describe('Simple CRUD Operations', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE users (
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          email TEXT UNIQUE,
          age INTEGER,
          active INTEGER DEFAULT 1
        )
      `);
    });

    describe('SELECT', () => {
      it('should execute SELECT * from empty table', () => {
        const result = db.prepare('SELECT * FROM users').all();
        expect(result).toEqual([]);
      });

      it('should execute SELECT with specific columns', () => {
        db.exec(`INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)`);

        const result = db.prepare('SELECT name, age FROM users').all();

        expect(result).toHaveLength(1);
        expect(result[0]).toEqual({ name: 'Alice', age: 30 });
      });

      it('should execute SELECT with WHERE clause', () => {
        db.exec(`INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)`);
        db.exec(`INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)`);
        db.exec(`INSERT INTO users (name, email, age) VALUES ('Carol', 'carol@example.com', 35)`);

        const result = db.prepare('SELECT name FROM users WHERE age > 28').all();

        expect(result).toHaveLength(2);
        expect(result.map(r => (r as { name: string }).name).sort()).toEqual(['Alice', 'Carol']);
      });

      it('should execute SELECT with ORDER BY', () => {
        db.exec(`INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)`);
        db.exec(`INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)`);

        const result = db.prepare('SELECT name FROM users ORDER BY name ASC').all();

        expect(result).toEqual([{ name: 'Alice' }, { name: 'Bob' }]);
      });

      it('should execute SELECT with LIMIT and OFFSET', () => {
        db.exec(`INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)`);
        db.exec(`INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)`);
        db.exec(`INSERT INTO users (name, email, age) VALUES ('Carol', 'carol@example.com', 35)`);

        const result = db.prepare('SELECT name FROM users ORDER BY name ASC LIMIT 2 OFFSET 1').all();

        expect(result).toEqual([{ name: 'Bob' }, { name: 'Carol' }]);
      });

      it('should execute SELECT with column aliases', () => {
        db.exec(`INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)`);

        const result = db.prepare('SELECT name AS user_name, age AS user_age FROM users').all();

        expect(result[0]).toEqual({ user_name: 'Alice', user_age: 30 });
      });
    });

    describe('INSERT', () => {
      it('should execute INSERT with explicit columns', () => {
        const result = db.prepare(
          `INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)`
        ).run();

        expect(result.changes).toBe(1);

        const users = db.prepare('SELECT * FROM users').all();
        expect(users).toHaveLength(1);
      });

      it('should execute INSERT with default values', () => {
        db.prepare(`INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')`).run();

        const result = db.prepare('SELECT active FROM users WHERE name = ?').get('Bob');

        expect((result as { active: number }).active).toBe(1);
      });

      it('should execute INSERT with parameters', () => {
        const stmt = db.prepare('INSERT INTO users (name, email, age) VALUES (?, ?, ?)');
        stmt.run('Alice', 'alice@example.com', 30);

        const user = db.prepare('SELECT * FROM users WHERE name = ?').get('Alice');

        expect(user).toBeDefined();
        expect((user as { email: string }).email).toBe('alice@example.com');
      });
    });

    describe('UPDATE', () => {
      beforeEach(() => {
        db.exec(`INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)`);
        db.exec(`INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)`);
      });

      it('should execute UPDATE with WHERE clause', () => {
        const result = db.prepare(
          `UPDATE users SET age = 31 WHERE name = 'Alice'`
        ).run();

        expect(result.changes).toBe(1);

        const user = db.prepare('SELECT age FROM users WHERE name = ?').get('Alice');
        expect((user as { age: number }).age).toBe(31);
      });

      it('should execute UPDATE affecting multiple rows', () => {
        const result = db.prepare(`UPDATE users SET active = 0`).run();

        expect(result.changes).toBe(2);
      });

      it('should execute UPDATE with expression', () => {
        db.prepare(`UPDATE users SET age = age + 1 WHERE name = 'Alice'`).run();

        const user = db.prepare('SELECT age FROM users WHERE name = ?').get('Alice');
        expect((user as { age: number }).age).toBe(31);
      });
    });

    describe('DELETE', () => {
      beforeEach(() => {
        db.exec(`INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)`);
        db.exec(`INSERT INTO users (name, email, age) VALUES ('Bob', 'bob@example.com', 25)`);
        db.exec(`INSERT INTO users (name, email, age) VALUES ('Carol', 'carol@example.com', 35)`);
      });

      it('should execute DELETE with WHERE clause', () => {
        const result = db.prepare(`DELETE FROM users WHERE name = 'Alice'`).run();

        expect(result.changes).toBe(1);

        const users = db.prepare('SELECT * FROM users').all();
        expect(users).toHaveLength(2);
      });

      it('should execute DELETE affecting all rows', () => {
        const result = db.prepare('DELETE FROM users').run();

        expect(result.changes).toBe(3);
      });

      it('should execute DELETE with complex WHERE', () => {
        const result = db.prepare('DELETE FROM users WHERE age > 28 AND active = 1').run();

        expect(result.changes).toBe(2); // Alice and Carol
      });
    });
  });

  // ===========================================================================
  // JOIN OPERATIONS
  // ===========================================================================

  describe('JOIN Operations', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE departments (
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL
        )
      `);
      db.exec(`
        CREATE TABLE employees (
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          dept_id INTEGER,
          salary INTEGER
        )
      `);

      // Insert test data
      db.exec(`INSERT INTO departments (id, name) VALUES (1, 'Engineering')`);
      db.exec(`INSERT INTO departments (id, name) VALUES (2, 'Sales')`);
      db.exec(`INSERT INTO departments (id, name) VALUES (3, 'HR')`);

      db.exec(`INSERT INTO employees (id, name, dept_id, salary) VALUES (1, 'Alice', 1, 100000)`);
      db.exec(`INSERT INTO employees (id, name, dept_id, salary) VALUES (2, 'Bob', 1, 90000)`);
      db.exec(`INSERT INTO employees (id, name, dept_id, salary) VALUES (3, 'Carol', 2, 80000)`);
      db.exec(`INSERT INTO employees (id, name, dept_id, salary) VALUES (4, 'Dave', NULL, 70000)`);
    });

    describe('INNER JOIN', () => {
      it('should execute INNER JOIN', () => {
        const result = db.prepare(`
          SELECT e.name AS employee, d.name AS department
          FROM employees e
          INNER JOIN departments d ON e.dept_id = d.id
        `).all();

        expect(result).toHaveLength(3); // Alice, Bob, Carol (Dave has no dept)
        expect(result.map(r => (r as { employee: string }).employee).sort())
          .toEqual(['Alice', 'Bob', 'Carol']);
      });

      it('should execute JOIN (implicit INNER)', () => {
        const result = db.prepare(`
          SELECT e.name, d.name AS dept
          FROM employees e
          JOIN departments d ON e.dept_id = d.id
        `).all();

        expect(result).toHaveLength(3);
      });

      it('should execute INNER JOIN with additional WHERE', () => {
        const result = db.prepare(`
          SELECT e.name
          FROM employees e
          INNER JOIN departments d ON e.dept_id = d.id
          WHERE d.name = 'Engineering'
        `).all();

        expect(result).toHaveLength(2); // Alice and Bob
      });
    });

    describe('LEFT JOIN', () => {
      it('should execute LEFT JOIN', () => {
        const result = db.prepare(`
          SELECT e.name AS employee, d.name AS department
          FROM employees e
          LEFT JOIN departments d ON e.dept_id = d.id
        `).all();

        expect(result).toHaveLength(4); // All employees including Dave

        const dave = result.find(r => (r as { employee: string }).employee === 'Dave');
        expect((dave as { department: string | null }).department).toBeNull();
      });

      it('should execute LEFT OUTER JOIN', () => {
        const result = db.prepare(`
          SELECT e.name, d.name AS dept
          FROM employees e
          LEFT OUTER JOIN departments d ON e.dept_id = d.id
        `).all();

        expect(result).toHaveLength(4);
      });

      it('should find rows without matches using LEFT JOIN', () => {
        const result = db.prepare(`
          SELECT e.name
          FROM employees e
          LEFT JOIN departments d ON e.dept_id = d.id
          WHERE d.id IS NULL
        `).all();

        expect(result).toHaveLength(1);
        expect((result[0] as { name: string }).name).toBe('Dave');
      });
    });

    describe('RIGHT JOIN', () => {
      it('should execute RIGHT JOIN', () => {
        const result = db.prepare(`
          SELECT e.name AS employee, d.name AS department
          FROM employees e
          RIGHT JOIN departments d ON e.dept_id = d.id
        `).all();

        // Engineering: Alice, Bob; Sales: Carol; HR: no one
        expect(result).toHaveLength(4);

        const hrRow = result.find(r => (r as { department: string }).department === 'HR');
        expect((hrRow as { employee: string | null }).employee).toBeNull();
      });
    });

    describe('FULL OUTER JOIN', () => {
      it('should execute FULL OUTER JOIN', () => {
        const result = db.prepare(`
          SELECT e.name AS employee, d.name AS department
          FROM employees e
          FULL OUTER JOIN departments d ON e.dept_id = d.id
        `).all();

        // All employees (including Dave) and all departments (including HR)
        expect(result.length).toBeGreaterThanOrEqual(5);
      });
    });

    describe('Multi-table JOINs', () => {
      beforeEach(() => {
        db.exec(`
          CREATE TABLE projects (
            id INTEGER PRIMARY KEY,
            name TEXT,
            dept_id INTEGER
          )
        `);
        db.exec(`INSERT INTO projects (id, name, dept_id) VALUES (1, 'Project A', 1)`);
        db.exec(`INSERT INTO projects (id, name, dept_id) VALUES (2, 'Project B', 2)`);
      });

      it('should execute three-table JOIN', () => {
        const result = db.prepare(`
          SELECT e.name AS employee, d.name AS department, p.name AS project
          FROM employees e
          INNER JOIN departments d ON e.dept_id = d.id
          INNER JOIN projects p ON p.dept_id = d.id
        `).all();

        expect(result.length).toBeGreaterThan(0);
      });
    });
  });

  // ===========================================================================
  // SUBQUERY OPERATIONS
  // ===========================================================================

  describe('Subquery Operations', () => {
    beforeEach(() => {
      db.exec(`CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount INTEGER)`);
      db.exec(`CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, tier TEXT)`);

      db.exec(`INSERT INTO customers (id, name, tier) VALUES (1, 'Alice', 'gold')`);
      db.exec(`INSERT INTO customers (id, name, tier) VALUES (2, 'Bob', 'silver')`);
      db.exec(`INSERT INTO customers (id, name, tier) VALUES (3, 'Carol', 'gold')`);

      db.exec(`INSERT INTO orders (id, customer_id, amount) VALUES (1, 1, 100)`);
      db.exec(`INSERT INTO orders (id, customer_id, amount) VALUES (2, 1, 200)`);
      db.exec(`INSERT INTO orders (id, customer_id, amount) VALUES (3, 2, 150)`);
    });

    describe('Scalar Subqueries', () => {
      it('should execute scalar subquery in SELECT', () => {
        const result = db.prepare(`
          SELECT name, (SELECT MAX(amount) FROM orders) AS max_order
          FROM customers
        `).all();

        expect(result).toHaveLength(3);
        expect((result[0] as { max_order: number }).max_order).toBe(200);
      });

      it('should execute scalar subquery in WHERE', () => {
        const result = db.prepare(`
          SELECT * FROM orders
          WHERE amount > (SELECT AVG(amount) FROM orders)
        `).all();

        // AVG is 150, so amounts > 150 are: 200
        expect(result).toHaveLength(1);
        expect((result[0] as { amount: number }).amount).toBe(200);
      });
    });

    describe('IN Subqueries', () => {
      it('should execute IN subquery', () => {
        const result = db.prepare(`
          SELECT name FROM customers
          WHERE id IN (SELECT customer_id FROM orders)
        `).all();

        expect(result).toHaveLength(2); // Alice and Bob have orders
      });

      it('should execute NOT IN subquery', () => {
        const result = db.prepare(`
          SELECT name FROM customers
          WHERE id NOT IN (SELECT customer_id FROM orders)
        `).all();

        expect(result).toHaveLength(1); // Only Carol has no orders
        expect((result[0] as { name: string }).name).toBe('Carol');
      });

      it('should execute IN subquery with condition', () => {
        const result = db.prepare(`
          SELECT name FROM customers
          WHERE id IN (SELECT customer_id FROM orders WHERE amount >= 150)
        `).all();

        expect(result).toHaveLength(2); // Alice (200) and Bob (150)
      });
    });

    describe('EXISTS Subqueries', () => {
      it('should execute EXISTS subquery', () => {
        const result = db.prepare(`
          SELECT name FROM customers c
          WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)
        `).all();

        expect(result).toHaveLength(2); // Alice and Bob
      });

      it('should execute NOT EXISTS subquery', () => {
        const result = db.prepare(`
          SELECT name FROM customers c
          WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)
        `).all();

        expect(result).toHaveLength(1); // Carol
        expect((result[0] as { name: string }).name).toBe('Carol');
      });

      it('should execute EXISTS with additional condition', () => {
        const result = db.prepare(`
          SELECT name FROM customers c
          WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.amount > 100)
        `).all();

        expect(result).toHaveLength(2); // Alice (200) and Bob (150)
      });
    });

    describe('Correlated Subqueries', () => {
      it('should execute correlated subquery in SELECT', () => {
        const result = db.prepare(`
          SELECT name,
                 (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.id) AS order_count
          FROM customers c
        `).all();

        expect(result).toHaveLength(3);

        const alice = result.find(r => (r as { name: string }).name === 'Alice');
        expect((alice as { order_count: number }).order_count).toBe(2);

        const carol = result.find(r => (r as { name: string }).name === 'Carol');
        expect((carol as { order_count: number }).order_count).toBe(0);
      });

      it('should execute correlated subquery in WHERE', () => {
        const result = db.prepare(`
          SELECT name FROM customers c
          WHERE (SELECT SUM(amount) FROM orders o WHERE o.customer_id = c.id) > 200
        `).all();

        // Alice has total 300, Bob has 150, Carol has 0
        expect(result).toHaveLength(1);
        expect((result[0] as { name: string }).name).toBe('Alice');
      });
    });
  });

  // ===========================================================================
  // CTE (Common Table Expressions)
  // ===========================================================================

  describe('CTE Operations', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE employees (
          id INTEGER PRIMARY KEY,
          name TEXT,
          manager_id INTEGER,
          salary INTEGER
        )
      `);

      db.exec(`INSERT INTO employees (id, name, manager_id, salary) VALUES (1, 'CEO', NULL, 200000)`);
      db.exec(`INSERT INTO employees (id, name, manager_id, salary) VALUES (2, 'VP Sales', 1, 150000)`);
      db.exec(`INSERT INTO employees (id, name, manager_id, salary) VALUES (3, 'VP Eng', 1, 160000)`);
      db.exec(`INSERT INTO employees (id, name, manager_id, salary) VALUES (4, 'Manager', 2, 100000)`);
      db.exec(`INSERT INTO employees (id, name, manager_id, salary) VALUES (5, 'Dev', 3, 90000)`);
    });

    describe('Simple CTEs', () => {
      it('should execute simple CTE', () => {
        const result = db.prepare(`
          WITH high_earners AS (
            SELECT name, salary FROM employees WHERE salary > 100000
          )
          SELECT * FROM high_earners
        `).all();

        expect(result).toHaveLength(3); // CEO, VP Sales, VP Eng
      });

      it('should execute CTE with column aliases', () => {
        const result = db.prepare(`
          WITH emp_data(employee_name, employee_salary) AS (
            SELECT name, salary FROM employees
          )
          SELECT employee_name FROM emp_data WHERE employee_salary > 150000
        `).all();

        expect(result).toHaveLength(2); // CEO and VP Eng
      });

      it('should execute multiple CTEs', () => {
        const result = db.prepare(`
          WITH
            high_salary AS (SELECT * FROM employees WHERE salary > 100000),
            executives AS (SELECT * FROM employees WHERE manager_id IS NULL)
          SELECT h.name FROM high_salary h
          INNER JOIN executives e ON h.id = e.id
        `).all();

        expect(result).toHaveLength(1); // Only CEO is both high salary and executive
      });
    });

    describe('Recursive CTEs', () => {
      it('should execute recursive CTE for number sequence', () => {
        const result = db.prepare(`
          WITH RECURSIVE cnt(x) AS (
            SELECT 1
            UNION ALL
            SELECT x + 1 FROM cnt WHERE x < 5
          )
          SELECT x FROM cnt
        `).all();

        expect(result).toHaveLength(5);
        const values = result.map(r => (r as { x: number }).x).sort((a, b) => a - b);
        expect(values).toEqual([1, 2, 3, 4, 5]);
      });

      it('should execute recursive CTE for org chart traversal', () => {
        const result = db.prepare(`
          WITH RECURSIVE org_chart AS (
            SELECT id, name, manager_id, 0 AS level
            FROM employees
            WHERE manager_id IS NULL
            UNION ALL
            SELECT e.id, e.name, e.manager_id, oc.level + 1
            FROM employees e
            INNER JOIN org_chart oc ON e.manager_id = oc.id
          )
          SELECT name, level FROM org_chart ORDER BY level
        `).all();

        expect(result.length).toBeGreaterThanOrEqual(5);

        const ceo = result.find(r => (r as { name: string }).name === 'CEO');
        expect((ceo as { level: number }).level).toBe(0);
      });
    });
  });

  // ===========================================================================
  // WINDOW FUNCTIONS
  // ===========================================================================

  describe('Window Functions', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE sales (
          id INTEGER PRIMARY KEY,
          region TEXT,
          salesperson TEXT,
          amount INTEGER,
          sale_date TEXT
        )
      `);

      db.exec(`INSERT INTO sales (region, salesperson, amount, sale_date) VALUES ('East', 'Alice', 100, '2024-01-01')`);
      db.exec(`INSERT INTO sales (region, salesperson, amount, sale_date) VALUES ('East', 'Alice', 150, '2024-01-02')`);
      db.exec(`INSERT INTO sales (region, salesperson, amount, sale_date) VALUES ('East', 'Bob', 200, '2024-01-01')`);
      db.exec(`INSERT INTO sales (region, salesperson, amount, sale_date) VALUES ('West', 'Carol', 175, '2024-01-01')`);
      db.exec(`INSERT INTO sales (region, salesperson, amount, sale_date) VALUES ('West', 'Carol', 225, '2024-01-02')`);
    });

    describe('Ranking Functions', () => {
      it('should execute ROW_NUMBER()', () => {
        const result = db.prepare(`
          SELECT salesperson, amount,
                 ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn
          FROM sales
        `).all();

        expect(result).toHaveLength(5);

        // Highest amount (225) should have rn = 1
        const first = result.find(r => (r as { rn: number }).rn === 1);
        expect((first as { amount: number }).amount).toBe(225);
      });

      it('should execute ROW_NUMBER() with PARTITION BY', () => {
        const result = db.prepare(`
          SELECT region, salesperson, amount,
                 ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn
          FROM sales
        `).all();

        expect(result).toHaveLength(5);

        // Each region should have its own row numbers starting from 1
        const eastRows = result.filter(r => (r as { region: string }).region === 'East');
        const eastRns = eastRows.map(r => (r as { rn: number }).rn);
        expect(eastRns.sort()).toEqual([1, 2, 3]);
      });

      it('should execute RANK()', () => {
        db.exec(`INSERT INTO sales (region, salesperson, amount, sale_date) VALUES ('East', 'Dave', 200, '2024-01-03')`);

        const result = db.prepare(`
          SELECT salesperson, amount,
                 RANK() OVER (ORDER BY amount DESC) AS rnk
          FROM sales
        `).all();

        // Two rows with amount 200 should both have rank 2
        const amount200 = result.filter(r => (r as { amount: number }).amount === 200);
        expect(amount200.length).toBe(2);
        expect(amount200.every(r => (r as { rnk: number }).rnk === 2)).toBe(true);
      });

      it('should execute DENSE_RANK()', () => {
        db.exec(`INSERT INTO sales (region, salesperson, amount, sale_date) VALUES ('East', 'Dave', 200, '2024-01-03')`);

        const result = db.prepare(`
          SELECT salesperson, amount,
                 DENSE_RANK() OVER (ORDER BY amount DESC) AS drnk
          FROM sales
        `).all();

        // After ties at rank 2, next rank should be 3 (not 4)
        const ranks = Array.from(new Set(result.map(r => (r as { drnk: number }).drnk))).sort((a, b) => a - b);
        // Should be consecutive: 1, 2, 3, 4, 5 (not 1, 2, 4, 5, 6)
        for (let i = 1; i < ranks.length; i++) {
          expect(ranks[i] - ranks[i - 1]).toBe(1);
        }
      });

      it('should execute NTILE()', () => {
        const result = db.prepare(`
          SELECT salesperson, amount,
                 NTILE(2) OVER (ORDER BY amount) AS bucket
          FROM sales
        `).all();

        expect(result).toHaveLength(5);

        const buckets = result.map(r => (r as { bucket: number }).bucket);
        expect(buckets.filter(b => b === 1).length).toBe(3);
        expect(buckets.filter(b => b === 2).length).toBe(2);
      });
    });

    describe('Value Functions', () => {
      it('should execute LAG()', () => {
        const result = db.prepare(`
          SELECT salesperson, amount,
                 LAG(amount) OVER (ORDER BY amount) AS prev_amount
          FROM sales
        `).all();

        const sorted = (result as { amount: number; prev_amount: number | null }[])
          .sort((a, b) => a.amount - b.amount);

        expect(sorted[0].prev_amount).toBeNull(); // First row has no previous
        expect(sorted[1].prev_amount).toBe(sorted[0].amount);
      });

      it('should execute LEAD()', () => {
        const result = db.prepare(`
          SELECT salesperson, amount,
                 LEAD(amount) OVER (ORDER BY amount) AS next_amount
          FROM sales
        `).all();

        const sorted = (result as { amount: number; next_amount: number | null }[])
          .sort((a, b) => a.amount - b.amount);

        expect(sorted[sorted.length - 1].next_amount).toBeNull(); // Last row has no next
      });

      it('should execute FIRST_VALUE()', () => {
        const result = db.prepare(`
          SELECT salesperson, amount,
                 FIRST_VALUE(salesperson) OVER (PARTITION BY region ORDER BY amount DESC) AS top_seller
          FROM sales
        `).all();

        expect(result).toHaveLength(5);
      });
    });

    describe('Aggregate Window Functions', () => {
      it('should execute SUM() OVER', () => {
        const result = db.prepare(`
          SELECT salesperson, amount,
                 SUM(amount) OVER (ORDER BY amount) AS running_total
          FROM sales
        `).all();

        expect(result).toHaveLength(5);
      });

      it('should execute AVG() OVER with PARTITION BY', () => {
        const result = db.prepare(`
          SELECT region, salesperson, amount,
                 AVG(amount) OVER (PARTITION BY region) AS region_avg
          FROM sales
        `).all();

        const eastRows = result.filter(r => (r as { region: string }).region === 'East');
        const eastAvg = (eastRows[0] as { region_avg: number }).region_avg;

        // East: 100 + 150 + 200 = 450 / 3 = 150
        expect(eastAvg).toBe(150);
      });

      it('should execute COUNT() OVER', () => {
        const result = db.prepare(`
          SELECT salesperson, amount,
                 COUNT(*) OVER (PARTITION BY salesperson) AS sale_count
          FROM sales
        `).all();

        const aliceRows = result.filter(r => (r as { salesperson: string }).salesperson === 'Alice');
        expect((aliceRows[0] as { sale_count: number }).sale_count).toBe(2);
      });
    });
  });

  // ===========================================================================
  // AGGREGATIONS
  // ===========================================================================

  describe('Aggregation Operations', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE products (
          id INTEGER PRIMARY KEY,
          category TEXT,
          name TEXT,
          price INTEGER,
          stock INTEGER
        )
      `);

      db.exec(`INSERT INTO products (category, name, price, stock) VALUES ('Electronics', 'Laptop', 1000, 10)`);
      db.exec(`INSERT INTO products (category, name, price, stock) VALUES ('Electronics', 'Phone', 500, 20)`);
      db.exec(`INSERT INTO products (category, name, price, stock) VALUES ('Electronics', 'Tablet', 600, 15)`);
      db.exec(`INSERT INTO products (category, name, price, stock) VALUES ('Clothing', 'Shirt', 50, 100)`);
      db.exec(`INSERT INTO products (category, name, price, stock) VALUES ('Clothing', 'Pants', 80, 75)`);
    });

    describe('Basic Aggregates', () => {
      it('should execute COUNT(*)', () => {
        const result = db.prepare('SELECT COUNT(*) AS total FROM products').get();
        expect((result as { total: number }).total).toBe(5);
      });

      it('should execute COUNT(column)', () => {
        const result = db.prepare('SELECT COUNT(category) AS cnt FROM products').get();
        expect((result as { cnt: number }).cnt).toBe(5);
      });

      it('should execute SUM()', () => {
        const result = db.prepare('SELECT SUM(price) AS total_price FROM products').get();
        expect((result as { total_price: number }).total_price).toBe(2230);
      });

      it('should execute AVG()', () => {
        const result = db.prepare('SELECT AVG(price) AS avg_price FROM products').get();
        expect((result as { avg_price: number }).avg_price).toBe(446);
      });

      it('should execute MIN() and MAX()', () => {
        const result = db.prepare('SELECT MIN(price) AS min_price, MAX(price) AS max_price FROM products').get();
        expect((result as { min_price: number }).min_price).toBe(50);
        expect((result as { max_price: number }).max_price).toBe(1000);
      });
    });

    describe('GROUP BY', () => {
      it('should execute GROUP BY with COUNT', () => {
        const result = db.prepare(`
          SELECT category, COUNT(*) AS product_count
          FROM products
          GROUP BY category
        `).all();

        expect(result).toHaveLength(2);

        const electronics = result.find(r => (r as { category: string }).category === 'Electronics');
        expect((electronics as { product_count: number }).product_count).toBe(3);
      });

      it('should execute GROUP BY with multiple aggregates', () => {
        const result = db.prepare(`
          SELECT category,
                 COUNT(*) AS cnt,
                 SUM(price) AS total_price,
                 AVG(price) AS avg_price
          FROM products
          GROUP BY category
        `).all();

        expect(result).toHaveLength(2);
      });

      it('should execute GROUP BY with ORDER BY', () => {
        const result = db.prepare(`
          SELECT category, SUM(price) AS total
          FROM products
          GROUP BY category
          ORDER BY total DESC
        `).all();

        expect((result[0] as { category: string }).category).toBe('Electronics');
      });
    });

    describe('HAVING', () => {
      it('should execute HAVING clause', () => {
        const result = db.prepare(`
          SELECT category, COUNT(*) AS cnt
          FROM products
          GROUP BY category
          HAVING COUNT(*) > 2
        `).all();

        expect(result).toHaveLength(1);
        expect((result[0] as { category: string }).category).toBe('Electronics');
      });

      it('should execute HAVING with aggregate comparison', () => {
        const result = db.prepare(`
          SELECT category, AVG(price) AS avg_price
          FROM products
          GROUP BY category
          HAVING AVG(price) > 100
        `).all();

        expect(result).toHaveLength(1);
        expect((result[0] as { category: string }).category).toBe('Electronics');
      });

      it('should execute GROUP BY with WHERE and HAVING', () => {
        const result = db.prepare(`
          SELECT category, SUM(stock) AS total_stock
          FROM products
          WHERE price > 40
          GROUP BY category
          HAVING SUM(stock) > 50
        `).all();

        expect(result.length).toBeGreaterThanOrEqual(1);
      });
    });
  });

  // ===========================================================================
  // SET OPERATIONS
  // ===========================================================================

  describe('Set Operations', () => {
    beforeEach(() => {
      db.exec(`CREATE TABLE table_a (id INTEGER, name TEXT)`);
      db.exec(`CREATE TABLE table_b (id INTEGER, name TEXT)`);

      db.exec(`INSERT INTO table_a (id, name) VALUES (1, 'Alice')`);
      db.exec(`INSERT INTO table_a (id, name) VALUES (2, 'Bob')`);
      db.exec(`INSERT INTO table_a (id, name) VALUES (3, 'Carol')`);

      db.exec(`INSERT INTO table_b (id, name) VALUES (2, 'Bob')`);
      db.exec(`INSERT INTO table_b (id, name) VALUES (3, 'Carol')`);
      db.exec(`INSERT INTO table_b (id, name) VALUES (4, 'Dave')`);
    });

    describe('UNION', () => {
      it('should execute UNION (removes duplicates)', () => {
        const result = db.prepare(`
          SELECT id, name FROM table_a
          UNION
          SELECT id, name FROM table_b
        `).all();

        // 1-Alice, 2-Bob, 3-Carol, 4-Dave = 4 unique rows
        expect(result).toHaveLength(4);
      });

      it('should execute UNION ALL (keeps duplicates)', () => {
        const result = db.prepare(`
          SELECT id, name FROM table_a
          UNION ALL
          SELECT id, name FROM table_b
        `).all();

        // 3 from A + 3 from B = 6 rows
        expect(result).toHaveLength(6);
      });

      it('should execute UNION with ORDER BY', () => {
        const result = db.prepare(`
          SELECT id, name FROM table_a
          UNION
          SELECT id, name FROM table_b
          ORDER BY name ASC
        `).all();

        expect((result[0] as { name: string }).name).toBe('Alice');
        expect((result[3] as { name: string }).name).toBe('Dave');
      });
    });

    describe('INTERSECT', () => {
      it('should execute INTERSECT', () => {
        const result = db.prepare(`
          SELECT id, name FROM table_a
          INTERSECT
          SELECT id, name FROM table_b
        `).all();

        // Common: 2-Bob, 3-Carol
        expect(result).toHaveLength(2);
      });
    });

    describe('EXCEPT', () => {
      it('should execute EXCEPT', () => {
        const result = db.prepare(`
          SELECT id, name FROM table_a
          EXCEPT
          SELECT id, name FROM table_b
        `).all();

        // In A but not B: 1-Alice
        expect(result).toHaveLength(1);
        expect((result[0] as { name: string }).name).toBe('Alice');
      });
    });

    describe('Compound Set Operations', () => {
      it('should execute multiple set operations', () => {
        db.exec(`CREATE TABLE table_c (id INTEGER, name TEXT)`);
        db.exec(`INSERT INTO table_c (id, name) VALUES (3, 'Carol')`);
        db.exec(`INSERT INTO table_c (id, name) VALUES (5, 'Eve')`);

        const result = db.prepare(`
          SELECT id, name FROM table_a
          UNION
          SELECT id, name FROM table_b
          UNION
          SELECT id, name FROM table_c
        `).all();

        // 1-Alice, 2-Bob, 3-Carol, 4-Dave, 5-Eve = 5 unique
        expect(result).toHaveLength(5);
      });
    });
  });

  // ===========================================================================
  // TRANSACTIONS
  // ===========================================================================

  describe('Transaction Operations', () => {
    beforeEach(() => {
      db.exec(`CREATE TABLE accounts (id INTEGER PRIMARY KEY, name TEXT, balance INTEGER)`);
      db.exec(`INSERT INTO accounts (name, balance) VALUES ('Alice', 1000)`);
      db.exec(`INSERT INTO accounts (name, balance) VALUES ('Bob', 500)`);
    });

    describe('Basic Transactions', () => {
      it('should execute transaction wrapper', () => {
        const transfer = db.transaction(() => {
          db.prepare(`UPDATE accounts SET balance = balance - 100 WHERE name = 'Alice'`).run();
          db.prepare(`UPDATE accounts SET balance = balance + 100 WHERE name = 'Bob'`).run();
        });

        transfer();

        const alice = db.prepare(`SELECT balance FROM accounts WHERE name = 'Alice'`).get();
        const bob = db.prepare(`SELECT balance FROM accounts WHERE name = 'Bob'`).get();

        expect((alice as { balance: number }).balance).toBe(900);
        expect((bob as { balance: number }).balance).toBe(600);
      });

      it('should handle transaction with parameters', () => {
        const transfer = db.transaction((from: string, to: string, amount: number) => {
          db.prepare(`UPDATE accounts SET balance = balance - ? WHERE name = ?`).run(amount, from);
          db.prepare(`UPDATE accounts SET balance = balance + ? WHERE name = ?`).run(amount, to);
          return { from, to, amount };
        });

        const result = transfer('Alice', 'Bob', 200);

        expect(result.amount).toBe(200);
      });
    });

    describe('Transaction Modes', () => {
      it('should execute deferred transaction', () => {
        const fn = db.transaction(() => {
          return db.prepare('SELECT COUNT(*) AS cnt FROM accounts').get();
        });

        const result = fn.deferred();
        expect((result as { cnt: number }).cnt).toBe(2);
      });

      it('should execute immediate transaction', () => {
        const fn = db.transaction(() => {
          return db.prepare('SELECT COUNT(*) AS cnt FROM accounts').get();
        });

        const result = fn.immediate();
        expect((result as { cnt: number }).cnt).toBe(2);
      });

      it('should execute exclusive transaction', () => {
        const fn = db.transaction(() => {
          return db.prepare('SELECT COUNT(*) AS cnt FROM accounts').get();
        });

        const result = fn.exclusive();
        expect((result as { cnt: number }).cnt).toBe(2);
      });
    });

    describe('Savepoints', () => {
      it('should create and release savepoint', () => {
        db.savepoint('sp1');
        db.prepare(`UPDATE accounts SET balance = 0 WHERE name = 'Alice'`).run();
        db.release('sp1');

        const alice = db.prepare(`SELECT balance FROM accounts WHERE name = 'Alice'`).get();
        expect((alice as { balance: number }).balance).toBe(0);
      });

      it('should rollback to savepoint', () => {
        const original = (db.prepare(`SELECT balance FROM accounts WHERE name = 'Alice'`).get() as { balance: number }).balance;

        db.savepoint('sp1');
        db.prepare(`UPDATE accounts SET balance = 0 WHERE name = 'Alice'`).run();
        db.rollback('sp1');

        const alice = db.prepare(`SELECT balance FROM accounts WHERE name = 'Alice'`).get();
        expect((alice as { balance: number }).balance).toBe(original);
      });
    });
  });

  // ===========================================================================
  // COMPLEX QUERIES
  // ===========================================================================

  describe('Complex Query Scenarios', () => {
    beforeEach(() => {
      db.exec(`
        CREATE TABLE customers (
          id INTEGER PRIMARY KEY,
          name TEXT,
          region TEXT,
          tier TEXT
        )
      `);
      db.exec(`
        CREATE TABLE orders (
          id INTEGER PRIMARY KEY,
          customer_id INTEGER,
          amount INTEGER,
          order_date TEXT
        )
      `);
      db.exec(`
        CREATE TABLE order_items (
          id INTEGER PRIMARY KEY,
          order_id INTEGER,
          product TEXT,
          quantity INTEGER,
          unit_price INTEGER
        )
      `);

      // Customers
      db.exec(`INSERT INTO customers (id, name, region, tier) VALUES (1, 'Acme Corp', 'East', 'gold')`);
      db.exec(`INSERT INTO customers (id, name, region, tier) VALUES (2, 'TechStart', 'West', 'silver')`);
      db.exec(`INSERT INTO customers (id, name, region, tier) VALUES (3, 'BigCo', 'East', 'gold')`);

      // Orders
      db.exec(`INSERT INTO orders (id, customer_id, amount, order_date) VALUES (1, 1, 1000, '2024-01-15')`);
      db.exec(`INSERT INTO orders (id, customer_id, amount, order_date) VALUES (2, 1, 1500, '2024-02-20')`);
      db.exec(`INSERT INTO orders (id, customer_id, amount, order_date) VALUES (3, 2, 500, '2024-01-10')`);
      db.exec(`INSERT INTO orders (id, customer_id, amount, order_date) VALUES (4, 3, 2000, '2024-03-01')`);

      // Order items
      db.exec(`INSERT INTO order_items (order_id, product, quantity, unit_price) VALUES (1, 'Widget', 10, 50)`);
      db.exec(`INSERT INTO order_items (order_id, product, quantity, unit_price) VALUES (1, 'Gadget', 5, 100)`);
      db.exec(`INSERT INTO order_items (order_id, product, quantity, unit_price) VALUES (2, 'Widget', 20, 50)`);
      db.exec(`INSERT INTO order_items (order_id, product, quantity, unit_price) VALUES (3, 'Gadget', 5, 100)`);
      db.exec(`INSERT INTO order_items (order_id, product, quantity, unit_price) VALUES (4, 'Widget', 30, 50)`);
    });

    it('should execute complex multi-join with aggregation', () => {
      const result = db.prepare(`
        SELECT c.name AS customer,
               c.region,
               COUNT(DISTINCT o.id) AS order_count,
               SUM(o.amount) AS total_amount
        FROM customers c
        LEFT JOIN orders o ON c.id = o.customer_id
        GROUP BY c.id, c.name, c.region
        ORDER BY total_amount DESC
      `).all();

      expect(result).toHaveLength(3);
      expect((result[0] as { customer: string }).customer).toBe('Acme Corp'); // 2500 total
    });

    it('should execute query with CTE and window function', () => {
      const result = db.prepare(`
        WITH customer_totals AS (
          SELECT customer_id, SUM(amount) AS total
          FROM orders
          GROUP BY customer_id
        )
        SELECT c.name,
               ct.total,
               RANK() OVER (ORDER BY ct.total DESC) AS spending_rank
        FROM customers c
        INNER JOIN customer_totals ct ON c.id = ct.customer_id
      `).all();

      expect(result.length).toBeGreaterThan(0);
    });

    it('should execute query with subquery and CASE expression', () => {
      const result = db.prepare(`
        SELECT c.name,
               c.tier,
               CASE
                 WHEN (SELECT SUM(amount) FROM orders WHERE customer_id = c.id) > 2000 THEN 'VIP'
                 WHEN (SELECT SUM(amount) FROM orders WHERE customer_id = c.id) > 1000 THEN 'Premium'
                 ELSE 'Standard'
               END AS classification
        FROM customers c
      `).all();

      expect(result).toHaveLength(3);

      const acme = result.find(r => (r as { name: string }).name === 'Acme Corp');
      expect((acme as { classification: string }).classification).toBe('VIP'); // 2500 total
    });

    it('should execute nested correlated subquery', () => {
      const result = db.prepare(`
        SELECT c.name,
               (SELECT COUNT(*)
                FROM orders o
                WHERE o.customer_id = c.id
                  AND o.amount > (SELECT AVG(amount) FROM orders)) AS above_avg_orders
        FROM customers c
      `).all();

      expect(result).toHaveLength(3);
    });

    it('should execute query with multiple set operations', () => {
      const result = db.prepare(`
        SELECT 'Gold Customers' AS type, name FROM customers WHERE tier = 'gold'
        UNION ALL
        SELECT 'High Spenders' AS type, c.name
        FROM customers c
        WHERE c.id IN (SELECT customer_id FROM orders WHERE amount > 1000)
      `).all();

      expect(result.length).toBeGreaterThan(0);
    });
  });
});
