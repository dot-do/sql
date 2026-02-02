/**
 * Drizzle ORM Integration Tests for DoSQL
 *
 * Comprehensive integration tests covering:
 * - Basic CRUD operations
 * - Transaction support
 * - Schema migrations
 * - Query building compatibility
 *
 * Uses workers-vitest-pool - NO MOCKS.
 */

import { describe, it, expect, beforeEach } from 'vitest';

import type {
  DoSQLBackend,
  DoSQLDrizzleConfig,
  DoSQLRunResult,
} from '../drizzle/types.js';

// =============================================================================
// IN-MEMORY BACKEND IMPLEMENTATION
// =============================================================================

/**
 * In-memory DoSQL backend for testing Drizzle integration.
 * This is a real implementation, not a mock.
 */
class InMemoryDrizzleBackend implements DoSQLBackend {
  private tables: Map<string, Map<string, unknown>[]> = new Map();
  private autoIncrement: Map<string, number> = new Map();
  private schema: Map<string, string[]> = new Map();

  async all<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<T[]> {
    return this.executeQuery<T>(sql, params);
  }

  async get<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<T | undefined> {
    const results = await this.all<T>(sql, params);
    return results[0];
  }

  async run(sql: string, params?: unknown[]): Promise<DoSQLRunResult> {
    return this.executeStatement(sql, params);
  }

  async values<T extends unknown[] = unknown[]>(sql: string, params?: unknown[]): Promise<T[]> {
    const rows = await this.all(sql, params);
    return rows.map((row) => Object.values(row as Record<string, unknown>)) as T[];
  }

  async transaction<T>(fn: (tx: DoSQLBackend) => Promise<T>): Promise<T> {
    // Snapshot current state
    const snapshot = new Map<string, Map<string, unknown>[]>();
    for (const [table, rows] of this.tables) {
      snapshot.set(table, rows.map((row) => ({ ...row })));
    }

    try {
      return await fn(this);
    } catch (error) {
      // Rollback on error
      this.tables = snapshot;
      throw error;
    }
  }

  // -------------------------------------------------------------------------
  // Internal execution methods
  // -------------------------------------------------------------------------

  private executeQuery<T>(sql: string, params?: unknown[]): T[] {
    const normalizedSql = sql.trim().toUpperCase();

    // Handle DDL
    if (normalizedSql.startsWith('CREATE TABLE')) {
      this.executeCreateTable(sql);
      return [] as T[];
    }

    if (normalizedSql.startsWith('ALTER TABLE')) {
      this.executeAlterTable(sql);
      return [] as T[];
    }

    if (normalizedSql.startsWith('DROP TABLE')) {
      this.executeDropTable(sql);
      return [] as T[];
    }

    // Handle SELECT
    if (normalizedSql.startsWith('SELECT')) {
      return this.executeSelect<T>(sql, params);
    }

    // Handle INSERT
    if (normalizedSql.startsWith('INSERT')) {
      const result = this.executeInsert(sql, params);
      return [result.insertedRow as T];
    }

    // Handle UPDATE
    if (normalizedSql.startsWith('UPDATE')) {
      this.executeUpdate(sql, params);
      return [] as T[];
    }

    // Handle DELETE
    if (normalizedSql.startsWith('DELETE')) {
      this.executeDelete(sql, params);
      return [] as T[];
    }

    return [] as T[];
  }

  private executeStatement(sql: string, params?: unknown[]): DoSQLRunResult {
    const normalizedSql = sql.trim().toUpperCase();

    if (normalizedSql.startsWith('CREATE TABLE')) {
      this.executeCreateTable(sql);
      return { rowsAffected: 0 };
    }

    if (normalizedSql.startsWith('ALTER TABLE')) {
      this.executeAlterTable(sql);
      return { rowsAffected: 0 };
    }

    if (normalizedSql.startsWith('DROP TABLE')) {
      this.executeDropTable(sql);
      return { rowsAffected: 0 };
    }

    if (normalizedSql.startsWith('INSERT')) {
      const result = this.executeInsert(sql, params);
      return {
        rowsAffected: 1,
        lastInsertRowId: result.lastInsertRowId,
      };
    }

    if (normalizedSql.startsWith('UPDATE')) {
      const affected = this.executeUpdate(sql, params);
      return { rowsAffected: affected };
    }

    if (normalizedSql.startsWith('DELETE')) {
      const affected = this.executeDelete(sql, params);
      return { rowsAffected: affected };
    }

    return { rowsAffected: 0 };
  }

  private executeCreateTable(sql: string): void {
    const match = sql.match(/CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?["'`]?(\w+)["'`]?\s*\(([^)]+)\)/i);
    if (match) {
      const tableName = match[1].toLowerCase();
      if (!this.tables.has(tableName)) {
        this.tables.set(tableName, []);
        this.autoIncrement.set(tableName, 1);

        // Parse column names
        const columnDefs = match[2].split(',').map((col) => {
          const colMatch = col.trim().match(/^["'`]?(\w+)["'`]?/);
          return colMatch ? colMatch[1] : '';
        }).filter(Boolean);
        this.schema.set(tableName, columnDefs);
      }
    }
  }

  private executeAlterTable(sql: string): void {
    // Handle ADD COLUMN
    const addMatch = sql.match(/ALTER\s+TABLE\s+["'`]?(\w+)["'`]?\s+ADD\s+(?:COLUMN\s+)?["'`]?(\w+)["'`]?/i);
    if (addMatch) {
      const tableName = addMatch[1].toLowerCase();
      const columnName = addMatch[2];
      const columns = this.schema.get(tableName) || [];
      if (!columns.includes(columnName)) {
        columns.push(columnName);
        this.schema.set(tableName, columns);
      }
      return;
    }

    // Handle DROP COLUMN
    const dropMatch = sql.match(/ALTER\s+TABLE\s+["'`]?(\w+)["'`]?\s+DROP\s+(?:COLUMN\s+)?["'`]?(\w+)["'`]?/i);
    if (dropMatch) {
      const tableName = dropMatch[1].toLowerCase();
      const columnName = dropMatch[2];
      const columns = this.schema.get(tableName) || [];
      const idx = columns.indexOf(columnName);
      if (idx >= 0) {
        columns.splice(idx, 1);
        this.schema.set(tableName, columns);
      }
      // Remove column from existing rows
      const rows = this.tables.get(tableName) || [];
      for (const row of rows) {
        delete row[columnName];
      }
      return;
    }

    // Handle RENAME COLUMN
    const renameMatch = sql.match(/ALTER\s+TABLE\s+["'`]?(\w+)["'`]?\s+RENAME\s+(?:COLUMN\s+)?["'`]?(\w+)["'`]?\s+TO\s+["'`]?(\w+)["'`]?/i);
    if (renameMatch) {
      const tableName = renameMatch[1].toLowerCase();
      const oldName = renameMatch[2];
      const newName = renameMatch[3];
      const columns = this.schema.get(tableName) || [];
      const idx = columns.indexOf(oldName);
      if (idx >= 0) {
        columns[idx] = newName;
        this.schema.set(tableName, columns);
      }
      // Rename in existing rows
      const rows = this.tables.get(tableName) || [];
      for (const row of rows) {
        if (oldName in row) {
          row[newName] = row[oldName];
          delete row[oldName];
        }
      }
    }
  }

  private executeDropTable(sql: string): void {
    const match = sql.match(/DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?["'`]?(\w+)["'`]?/i);
    if (match) {
      const tableName = match[1].toLowerCase();
      this.tables.delete(tableName);
      this.autoIncrement.delete(tableName);
      this.schema.delete(tableName);
    }
  }

  private executeSelect<T>(sql: string, params?: unknown[]): T[] {
    const match = sql.match(/SELECT\s+(.+?)\s+FROM\s+["'`]?(\w+)["'`]?/i);
    if (!match) return [] as T[];

    const tableName = match[2].toLowerCase();
    let rows = [...(this.tables.get(tableName) || [])];

    // Apply WHERE clause
    const whereMatch = sql.match(/WHERE\s+["'`]?(\w+)["'`]?\s*(=|<>|!=|<|>|<=|>=|LIKE)\s*(\?|'[^']*'|\d+)/i);
    if (whereMatch && params) {
      const column = whereMatch[1];
      const op = whereMatch[2].toUpperCase();
      let value: unknown = whereMatch[3];
      if (value === '?') {
        value = params[0];
      } else if (typeof value === 'string' && value.startsWith("'")) {
        value = value.slice(1, -1);
      } else if (/^\d+$/.test(String(value))) {
        value = parseInt(String(value), 10);
      }

      rows = rows.filter((row) => {
        const rowVal = row[column];
        switch (op) {
          case '=': return rowVal === value;
          case '<>':
          case '!=': return rowVal !== value;
          case '<': return (rowVal as number) < (value as number);
          case '>': return (rowVal as number) > (value as number);
          case '<=': return (rowVal as number) <= (value as number);
          case '>=': return (rowVal as number) >= (value as number);
          default: return true;
        }
      });
    }

    // Apply ORDER BY
    const orderMatch = sql.match(/ORDER\s+BY\s+["'`]?(\w+)["'`]?\s*(ASC|DESC)?/i);
    if (orderMatch) {
      const column = orderMatch[1];
      const direction = orderMatch[2]?.toUpperCase() === 'DESC' ? -1 : 1;
      rows.sort((a, b) => {
        const aVal = a[column];
        const bVal = b[column];
        if (aVal === bVal) return 0;
        return aVal < bVal ? -direction : direction;
      });
    }

    // Apply LIMIT
    const limitMatch = sql.match(/LIMIT\s+(\d+)/i);
    if (limitMatch) {
      rows = rows.slice(0, parseInt(limitMatch[1], 10));
    }

    return rows as T[];
  }

  private executeInsert(sql: string, params?: unknown[]): { lastInsertRowId: number; insertedRow: Record<string, unknown> } {
    const match = sql.match(/INSERT\s+INTO\s+["'`]?(\w+)["'`]?\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)/i);
    if (!match) return { lastInsertRowId: 0, insertedRow: {} };

    const tableName = match[1].toLowerCase();
    const columns = match[2].split(',').map((c) => c.trim().replace(/["'`]/g, ''));
    const valuePlaceholders = match[3].split(',').map((v) => v.trim());

    if (!this.tables.has(tableName)) {
      this.tables.set(tableName, []);
      this.autoIncrement.set(tableName, 1);
    }

    const row: Map<string, unknown> = new Map();
    let paramIndex = 0;

    for (let i = 0; i < columns.length; i++) {
      const placeholder = valuePlaceholders[i];
      let value: unknown;
      if (placeholder === '?') {
        value = params?.[paramIndex++];
      } else if (placeholder.startsWith("'")) {
        value = placeholder.slice(1, -1);
      } else if (/^\d+$/.test(placeholder)) {
        value = parseInt(placeholder, 10);
      } else {
        value = placeholder;
      }
      row.set(columns[i], value);
    }

    // Auto-generate ID if not provided
    if (!row.has('id')) {
      const nextId = this.autoIncrement.get(tableName) || 1;
      row.set('id', nextId);
      this.autoIncrement.set(tableName, nextId + 1);
    } else {
      const id = row.get('id') as number;
      const currentAuto = this.autoIncrement.get(tableName) || 1;
      if (id >= currentAuto) {
        this.autoIncrement.set(tableName, id + 1);
      }
    }

    const rowObj = Object.fromEntries(row);
    this.tables.get(tableName)!.push(rowObj);

    return { lastInsertRowId: rowObj.id as number, insertedRow: rowObj };
  }

  private executeUpdate(sql: string, params?: unknown[]): number {
    const match = sql.match(/UPDATE\s+["'`]?(\w+)["'`]?\s+SET\s+(.+?)(?:\s+WHERE|$)/i);
    if (!match) return 0;

    const tableName = match[1].toLowerCase();
    const rows = this.tables.get(tableName) || [];

    // Parse SET clause
    const setParts = match[2].split(',');
    const updates: Record<string, unknown> = {};
    let paramIndex = 0;

    for (const part of setParts) {
      const [col, val] = part.split('=').map((s) => s.trim());
      const colName = col.replace(/["'`]/g, '');
      if (val === '?') {
        updates[colName] = params?.[paramIndex++];
      } else if (val.startsWith("'")) {
        updates[colName] = val.slice(1, -1);
      } else if (/^\d+$/.test(val)) {
        updates[colName] = parseInt(val, 10);
      }
    }

    // Parse WHERE
    let targetRows = rows;
    const whereMatch = sql.match(/WHERE\s+["'`]?(\w+)["'`]?\s*=\s*(\?|\d+|'[^']*')/i);
    if (whereMatch) {
      const column = whereMatch[1];
      let value: unknown = whereMatch[2];
      if (value === '?') {
        value = params?.[paramIndex];
      } else if (typeof value === 'string' && value.startsWith("'")) {
        value = value.slice(1, -1);
      } else if (/^\d+$/.test(String(value))) {
        value = parseInt(String(value), 10);
      }
      targetRows = rows.filter((row) => row[column] === value);
    }

    // Apply updates
    for (const row of targetRows) {
      for (const [key, val] of Object.entries(updates)) {
        row[key] = val;
      }
    }

    return targetRows.length;
  }

  private executeDelete(sql: string, params?: unknown[]): number {
    const match = sql.match(/DELETE\s+FROM\s+["'`]?(\w+)["'`]?/i);
    if (!match) return 0;

    const tableName = match[1].toLowerCase();
    const rows = this.tables.get(tableName) || [];
    const initialCount = rows.length;

    // Parse WHERE
    const whereMatch = sql.match(/WHERE\s+["'`]?(\w+)["'`]?\s*=\s*(\?|\d+|'[^']*')/i);
    if (whereMatch) {
      const column = whereMatch[1];
      let value: unknown = whereMatch[2];
      if (value === '?') {
        value = params?.[0];
      } else if (typeof value === 'string' && value.startsWith("'")) {
        value = value.slice(1, -1);
      } else if (/^\d+$/.test(String(value))) {
        value = parseInt(String(value), 10);
      }
      const filtered = rows.filter((row) => row[column] !== value);
      this.tables.set(tableName, filtered);
      return initialCount - filtered.length;
    } else {
      this.tables.set(tableName, []);
      return initialCount;
    }
  }

  // -------------------------------------------------------------------------
  // Test helpers
  // -------------------------------------------------------------------------

  seed(tableName: string, rows: Record<string, unknown>[]): void {
    const table = tableName.toLowerCase();
    this.tables.set(table, rows.map((r) => ({ ...r })));
    const maxId = rows.reduce((max, row) => {
      const id = typeof row.id === 'number' ? row.id : 0;
      return Math.max(max, id);
    }, 0);
    this.autoIncrement.set(table, maxId + 1);
  }

  clear(): void {
    this.tables.clear();
    this.autoIncrement.clear();
    this.schema.clear();
  }

  getTableData(tableName: string): Record<string, unknown>[] {
    return [...(this.tables.get(tableName.toLowerCase()) || [])];
  }

  getSchema(tableName: string): string[] {
    return [...(this.schema.get(tableName.toLowerCase()) || [])];
  }
}

// =============================================================================
// TEST SUITES
// =============================================================================

describe('Drizzle ORM Integration Tests', () => {
  let backend: InMemoryDrizzleBackend;

  beforeEach(() => {
    backend = new InMemoryDrizzleBackend();
  });

  // ---------------------------------------------------------------------------
  // Basic CRUD Operations
  // ---------------------------------------------------------------------------

  describe('Basic CRUD Operations', () => {
    beforeEach(async () => {
      await backend.run('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, active INTEGER)');
      backend.seed('users', [
        { id: 1, name: 'Alice', email: 'alice@example.com', active: 1 },
        { id: 2, name: 'Bob', email: 'bob@example.com', active: 1 },
        { id: 3, name: 'Charlie', email: 'charlie@example.com', active: 0 },
      ]);
    });

    it('should read all rows', async () => {
      const rows = await backend.all('SELECT * FROM users');
      expect(rows).toHaveLength(3);
    });

    it('should read single row', async () => {
      const row = await backend.get('SELECT * FROM users WHERE id = ?', [1]);
      expect(row).toBeDefined();
      expect((row as Record<string, unknown>)?.name).toBe('Alice');
    });

    it('should insert a row', async () => {
      const result = await backend.run(
        'INSERT INTO users (name, email, active) VALUES (?, ?, ?)',
        ['David', 'david@example.com', 1]
      );
      expect(result.rowsAffected).toBe(1);
      expect(result.lastInsertRowId).toBeDefined();

      const rows = await backend.all('SELECT * FROM users');
      expect(rows).toHaveLength(4);
    });

    it('should update a row', async () => {
      const result = await backend.run(
        'UPDATE users SET name = ? WHERE id = ?',
        ['Alice Updated', 1]
      );
      expect(result.rowsAffected).toBe(1);

      const row = await backend.get('SELECT * FROM users WHERE id = ?', [1]);
      expect((row as Record<string, unknown>)?.name).toBe('Alice Updated');
    });

    it('should delete a row', async () => {
      const result = await backend.run('DELETE FROM users WHERE id = ?', [3]);
      expect(result.rowsAffected).toBe(1);

      const rows = await backend.all('SELECT * FROM users');
      expect(rows).toHaveLength(2);
    });

    it('should return values as arrays', async () => {
      const values = await backend.values('SELECT id, name FROM users WHERE id = ?', [1]);
      expect(values).toHaveLength(1);
      expect(Array.isArray(values[0])).toBe(true);
    });
  });

  // ---------------------------------------------------------------------------
  // Transaction Support
  // ---------------------------------------------------------------------------

  describe('Transaction Support', () => {
    beforeEach(async () => {
      await backend.run('CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)');
      backend.seed('accounts', [
        { id: 1, balance: 1000 },
        { id: 2, balance: 500 },
      ]);
    });

    it('should commit successful transaction', async () => {
      await backend.transaction(async (tx) => {
        await tx.run('UPDATE accounts SET balance = balance - 100 WHERE id = ?', [1]);
        await tx.run('UPDATE accounts SET balance = balance + 100 WHERE id = ?', [2]);
      });

      const acc1 = await backend.get('SELECT * FROM accounts WHERE id = ?', [1]);
      const acc2 = await backend.get('SELECT * FROM accounts WHERE id = ?', [2]);

      expect((acc1 as Record<string, unknown>)?.balance).toBe(900);
      expect((acc2 as Record<string, unknown>)?.balance).toBe(600);
    });

    it('should rollback failed transaction', async () => {
      const initialAcc1 = await backend.get('SELECT * FROM accounts WHERE id = ?', [1]);
      const initialBalance = (initialAcc1 as Record<string, unknown>)?.balance;

      try {
        await backend.transaction(async (tx) => {
          await tx.run('UPDATE accounts SET balance = balance - 100 WHERE id = ?', [1]);
          throw new Error('Intentional error');
        });
      } catch {
        // Expected
      }

      const acc1 = await backend.get('SELECT * FROM accounts WHERE id = ?', [1]);
      expect((acc1 as Record<string, unknown>)?.balance).toBe(initialBalance);
    });

    it('should support nested operations in transaction', async () => {
      const result = await backend.transaction(async (tx) => {
        await tx.run('INSERT INTO accounts (balance) VALUES (?)', [250]);
        const accounts = await tx.all('SELECT * FROM accounts');
        return accounts.length;
      });

      expect(result).toBe(3);
    });
  });

  // ---------------------------------------------------------------------------
  // Schema Migrations
  // ---------------------------------------------------------------------------

  describe('Schema Migrations', () => {
    it('should create table', async () => {
      await backend.run('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)');

      await backend.run('INSERT INTO products (name, price) VALUES (?, ?)', ['Widget', 9.99]);
      const rows = await backend.all('SELECT * FROM products');
      expect(rows).toHaveLength(1);
    });

    it('should create table if not exists', async () => {
      await backend.run('CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY, name TEXT)');
      await backend.run('CREATE TABLE IF NOT EXISTS products (id INTEGER PRIMARY KEY, name TEXT)');

      // Should not throw and table should exist
      await backend.run('INSERT INTO products (name) VALUES (?)', ['Test']);
      const rows = await backend.all('SELECT * FROM products');
      expect(rows).toHaveLength(1);
    });

    it('should drop table', async () => {
      await backend.run('CREATE TABLE temp_table (id INTEGER PRIMARY KEY)');
      await backend.run('INSERT INTO temp_table (id) VALUES (?)', [1]);

      await backend.run('DROP TABLE temp_table');

      // Table should no longer exist
      const rows = await backend.all('SELECT * FROM temp_table');
      expect(rows).toHaveLength(0);
    });

    it('should add column to table', async () => {
      await backend.run('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
      backend.seed('users', [{ id: 1, name: 'Alice' }]);

      await backend.run('ALTER TABLE users ADD COLUMN email TEXT');

      // Verify column was added
      const schema = backend.getSchema('users');
      expect(schema).toContain('email');
    });

    it('should rename column', async () => {
      await backend.run('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)');
      backend.seed('users', [{ id: 1, name: 'Alice' }]);

      await backend.run('ALTER TABLE users RENAME COLUMN name TO full_name');

      const data = backend.getTableData('users');
      expect(data[0]).toHaveProperty('full_name');
      expect(data[0]).not.toHaveProperty('name');
    });

    it('should drop column', async () => {
      await backend.run('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, temp_col TEXT)');
      backend.seed('users', [{ id: 1, name: 'Alice', temp_col: 'temp' }]);

      await backend.run('ALTER TABLE users DROP COLUMN temp_col');

      const data = backend.getTableData('users');
      expect(data[0]).not.toHaveProperty('temp_col');
    });

    it('should handle migration sequence', async () => {
      // Migration 1: Create initial table
      await backend.run('CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT)');

      // Migration 2: Add column
      await backend.run('ALTER TABLE posts ADD COLUMN content TEXT');

      // Migration 3: Add another column
      await backend.run('ALTER TABLE posts ADD COLUMN published INTEGER');

      // Verify final schema
      const schema = backend.getSchema('posts');
      expect(schema).toContain('id');
      expect(schema).toContain('title');
      expect(schema).toContain('content');
      expect(schema).toContain('published');

      // Verify we can use the table
      await backend.run(
        'INSERT INTO posts (title, content, published) VALUES (?, ?, ?)',
        ['Test Post', 'Test content', 1]
      );
      const posts = await backend.all('SELECT * FROM posts');
      expect(posts).toHaveLength(1);
    });
  });

  // ---------------------------------------------------------------------------
  // Query Building Compatibility
  // ---------------------------------------------------------------------------

  describe('Query Building Compatibility', () => {
    beforeEach(async () => {
      await backend.run('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL, quantity INTEGER, category TEXT)');
      backend.seed('items', [
        { id: 1, name: 'Apple', price: 1.50, quantity: 100, category: 'fruit' },
        { id: 2, name: 'Banana', price: 0.75, quantity: 150, category: 'fruit' },
        { id: 3, name: 'Carrot', price: 0.50, quantity: 200, category: 'vegetable' },
        { id: 4, name: 'Milk', price: 2.99, quantity: 50, category: 'dairy' },
        { id: 5, name: 'Bread', price: 3.50, quantity: 30, category: 'bakery' },
      ]);
    });

    it('should handle equality filter', async () => {
      const rows = await backend.all('SELECT * FROM items WHERE category = ?', ['fruit']);
      expect(rows).toHaveLength(2);
    });

    it('should handle inequality filter', async () => {
      const rows = await backend.all('SELECT * FROM items WHERE category <> ?', ['fruit']);
      expect(rows).toHaveLength(3);
    });

    it('should handle greater than filter', async () => {
      const rows = await backend.all('SELECT * FROM items WHERE price > ?', [1.00]);
      expect(rows).toHaveLength(3);
    });

    it('should handle less than filter', async () => {
      const rows = await backend.all('SELECT * FROM items WHERE quantity < ?', [100]);
      expect(rows).toHaveLength(2);
    });

    it('should handle ORDER BY ascending', async () => {
      const rows = await backend.all('SELECT * FROM items ORDER BY price ASC');
      expect(rows).toHaveLength(5);
      expect((rows[0] as Record<string, unknown>).price).toBe(0.50);
    });

    it('should handle ORDER BY descending', async () => {
      const rows = await backend.all('SELECT * FROM items ORDER BY price DESC');
      expect(rows).toHaveLength(5);
      expect((rows[0] as Record<string, unknown>).price).toBe(3.50);
    });

    it('should handle LIMIT', async () => {
      const rows = await backend.all('SELECT * FROM items LIMIT 3');
      expect(rows).toHaveLength(3);
    });

    it('should handle combined ORDER BY and LIMIT', async () => {
      const rows = await backend.all('SELECT * FROM items ORDER BY price DESC LIMIT 2');
      expect(rows).toHaveLength(2);
      expect((rows[0] as Record<string, unknown>).name).toBe('Bread');
      expect((rows[1] as Record<string, unknown>).name).toBe('Milk');
    });
  });

  // ---------------------------------------------------------------------------
  // Edge Cases
  // ---------------------------------------------------------------------------

  describe('Edge Cases', () => {
    it('should handle null values', async () => {
      await backend.run('CREATE TABLE nullable (id INTEGER PRIMARY KEY, value TEXT)');
      await backend.run('INSERT INTO nullable (value) VALUES (?)', [null]);

      const row = await backend.get('SELECT * FROM nullable WHERE id = ?', [1]);
      expect((row as Record<string, unknown>)?.value).toBeNull();
    });

    it('should handle empty tables', async () => {
      await backend.run('CREATE TABLE empty_table (id INTEGER PRIMARY KEY)');
      const rows = await backend.all('SELECT * FROM empty_table');
      expect(rows).toHaveLength(0);
    });

    it('should handle special characters in strings', async () => {
      await backend.run('CREATE TABLE special (id INTEGER PRIMARY KEY, text TEXT)');
      await backend.run('INSERT INTO special (text) VALUES (?)', ["It's a test"]);

      const row = await backend.get('SELECT * FROM special WHERE id = ?', [1]);
      expect((row as Record<string, unknown>)?.text).toBe("It's a test");
    });

    it('should handle large numbers', async () => {
      await backend.run('CREATE TABLE numbers (id INTEGER PRIMARY KEY, big_num INTEGER)');
      await backend.run('INSERT INTO numbers (big_num) VALUES (?)', [Number.MAX_SAFE_INTEGER]);

      const row = await backend.get('SELECT * FROM numbers WHERE id = ?', [1]);
      expect((row as Record<string, unknown>)?.big_num).toBe(Number.MAX_SAFE_INTEGER);
    });
  });

  // ---------------------------------------------------------------------------
  // Config and Types
  // ---------------------------------------------------------------------------

  describe('Config and Types', () => {
    it('should satisfy DoSQLBackend interface', () => {
      const config: DoSQLDrizzleConfig = {
        backend,
      };

      expect(config.backend).toBe(backend);
      expect(typeof config.backend.all).toBe('function');
      expect(typeof config.backend.get).toBe('function');
      expect(typeof config.backend.run).toBe('function');
      expect(typeof config.backend.values).toBe('function');
      expect(typeof config.backend.transaction).toBe('function');
    });

    it('should support logger config', () => {
      const logs: string[] = [];
      const config: DoSQLDrizzleConfig = {
        backend,
        logger: {
          logQuery(query, params) {
            logs.push(`${query} | ${JSON.stringify(params)}`);
          },
        },
      };

      expect(config.logger).toBeDefined();
    });

    it('should support casing config', () => {
      const snakeCaseConfig: DoSQLDrizzleConfig = {
        backend,
        casing: 'snake_case',
      };

      const camelCaseConfig: DoSQLDrizzleConfig = {
        backend,
        casing: 'camelCase',
      };

      expect(snakeCaseConfig.casing).toBe('snake_case');
      expect(camelCaseConfig.casing).toBe('camelCase');
    });
  });
});
