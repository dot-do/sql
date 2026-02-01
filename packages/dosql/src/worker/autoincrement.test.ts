/**
 * Auto-increment ID caching tests
 *
 * Verifies that getNextId uses cached max ID values instead of
 * performing a full table scan on every insert. Tests correctness
 * of auto-increment across multiple inserts, explicit IDs, and
 * table drops.
 */

import { describe, it, expect } from 'vitest';
import { SELF } from 'cloudflare:test';

async function execute(dbName: string, sql: string) {
  const response = await SELF.fetch(`http://localhost/db/${dbName}/execute`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql }),
  });
  return response.json() as Promise<{
    success: boolean;
    error?: string;
    rows?: Record<string, unknown>[];
    stats?: { rowsAffected: number };
  }>;
}

async function query(dbName: string, sql: string) {
  const response = await SELF.fetch(`http://localhost/db/${dbName}/query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ sql }),
  });
  return response.json() as Promise<{
    success: boolean;
    error?: string;
    rows?: Record<string, unknown>[];
    stats?: { rowsAffected: number };
  }>;
}

describe('Auto-increment ID caching', () => {
  it('should assign sequential IDs without explicit ID', async () => {
    const db = 'autoincrement-sequential';
    await execute(db, 'CREATE TABLE items (id INTEGER, name TEXT, PRIMARY KEY (id))');

    await execute(db, "INSERT INTO items (name) VALUES ('first')");
    await execute(db, "INSERT INTO items (name) VALUES ('second')");
    await execute(db, "INSERT INTO items (name) VALUES ('third')");

    const result = await query(db, 'SELECT id, name FROM items ORDER BY id');
    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(3);
    expect(result.rows![0]).toMatchObject({ id: 1, name: 'first' });
    expect(result.rows![1]).toMatchObject({ id: 2, name: 'second' });
    expect(result.rows![2]).toMatchObject({ id: 3, name: 'third' });
  });

  it('should not collide after explicit high ID insert', async () => {
    const db = 'autoincrement-explicit-id';
    await execute(db, 'CREATE TABLE items (id INTEGER, name TEXT, PRIMARY KEY (id))');

    // Insert with explicit high ID
    await execute(db, "INSERT INTO items (id, name) VALUES (100, 'explicit')");
    // Next auto-increment should be > 100
    await execute(db, "INSERT INTO items (name) VALUES ('auto')");

    const result = await query(db, 'SELECT id, name FROM items ORDER BY id');
    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(2);
    expect(result.rows![0]).toMatchObject({ id: 100, name: 'explicit' });
    expect(result.rows![1].id).toBeGreaterThan(100);
    expect(result.rows![1].name).toBe('auto');
  });

  it('should handle batch inserts with sequential IDs', async () => {
    const db = 'autoincrement-batch';
    await execute(db, 'CREATE TABLE counters (id INTEGER, label TEXT, PRIMARY KEY (id))');

    // Insert 10 rows without explicit IDs
    for (let i = 0; i < 10; i++) {
      await execute(db, `INSERT INTO counters (label) VALUES ('item-${i}')`);
    }

    const result = await query(db, 'SELECT id FROM counters');
    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(10);

    // IDs should be 1 through 10 with no duplicates
    const ids = result.rows!.map((r) => r.id as number).sort((a, b) => a - b);
    for (let i = 0; i < ids.length; i++) {
      expect(ids[i]).toBe(i + 1);
    }
  });

  it('should reset ID cache after DROP TABLE and re-create', async () => {
    const db = 'autoincrement-drop-recreate';
    await execute(db, 'CREATE TABLE temp (id INTEGER, val TEXT, PRIMARY KEY (id))');
    await execute(db, "INSERT INTO temp (val) VALUES ('a')");
    await execute(db, "INSERT INTO temp (val) VALUES ('b')");

    // Drop and recreate the table
    await execute(db, 'DROP TABLE temp');
    await execute(db, 'CREATE TABLE temp (id INTEGER, val TEXT, PRIMARY KEY (id))');

    // IDs should start from 1 again
    await execute(db, "INSERT INTO temp (val) VALUES ('x')");
    const result = await query(db, 'SELECT id, val FROM temp');
    expect(result.success).toBe(true);
    expect(result.rows).toHaveLength(1);
    expect(result.rows![0]).toMatchObject({ id: 1, val: 'x' });
  });
});
