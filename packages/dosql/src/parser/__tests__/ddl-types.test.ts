/**
 * Tests for DDL type guards and parse result helpers
 *
 * Verifies the type guard functions exported from ddl-types.ts:
 * - Statement type guards (isCreateTableStatement, isDropIndexStatement, etc.)
 * - Parse result type guards (isParseSuccess, isParseError)
 * - All DDL statement types are correctly identified
 */

import { describe, it, expect } from 'vitest';
import {
  isCreateTableStatement,
  isCreateIndexStatement,
  isAlterTableStatement,
  isDropTableStatement,
  isDropIndexStatement,
  isCreateViewStatement,
  isDropViewStatement,
  isCreateTriggerStatement,
  isDropTriggerStatement,
  isParseSuccess,
  isParseError,
  type DDLStatement,
  type CreateTableStatement,
  type CreateIndexStatement,
  type AlterTableStatement,
  type DropTableStatement,
  type DropIndexStatement,
  type CreateViewStatement,
  type DropViewStatement,
  type CreateTriggerStatement,
  type DropTriggerStatement,
  type ParseResult,
  type ParseSuccess,
  type ParseError,
} from '../ddl-types.js';

// =============================================================================
// Fixture Helpers
// =============================================================================

function makeCreateTable(): CreateTableStatement {
  return {
    type: 'CREATE TABLE',
    table: 'users',
    columns: [
      { name: 'id', type: 'INTEGER', constraints: [] },
      { name: 'name', type: 'TEXT', constraints: [] },
    ],
    constraints: [],
    ifNotExists: false,
    temporary: false,
    strict: false,
    withoutRowid: false,
  };
}

function makeCreateIndex(): CreateIndexStatement {
  return {
    type: 'CREATE INDEX',
    name: 'idx_users_name',
    table: 'users',
    columns: [{ column: 'name' }],
    unique: false,
    ifNotExists: false,
  };
}

function makeAlterTable(): AlterTableStatement {
  return {
    type: 'ALTER TABLE',
    table: 'users',
    operation: { type: 'ADD COLUMN', column: { name: 'email', type: 'TEXT', constraints: [] } },
  };
}

function makeDropTable(): DropTableStatement {
  return {
    type: 'DROP TABLE',
    table: 'users',
    ifExists: false,
  };
}

function makeDropIndex(): DropIndexStatement {
  return {
    type: 'DROP INDEX',
    name: 'idx_users_name',
    ifExists: false,
  };
}

function makeCreateView(): CreateViewStatement {
  return {
    type: 'CREATE VIEW',
    name: 'active_users',
    query: 'SELECT * FROM users WHERE active = 1',
    ifNotExists: false,
    temporary: false,
  };
}

function makeDropView(): DropViewStatement {
  return {
    type: 'DROP VIEW',
    name: 'active_users',
    ifExists: false,
  };
}

function makeCreateTrigger(): CreateTriggerStatement {
  return {
    type: 'CREATE TRIGGER',
    name: 'trg_users_insert',
    table: 'users',
    timing: 'BEFORE',
    event: 'INSERT',
    body: 'BEGIN SELECT 1; END',
    ifNotExists: false,
    temporary: false,
    forEachRow: true,
  };
}

function makeDropTrigger(): DropTriggerStatement {
  return {
    type: 'DROP TRIGGER',
    name: 'trg_users_insert',
    ifExists: false,
  };
}

// =============================================================================
// Statement Type Guards
// =============================================================================

describe('DDL Types - Statement Type Guards', () => {
  const allStatements: DDLStatement[] = [
    makeCreateTable(),
    makeCreateIndex(),
    makeAlterTable(),
    makeDropTable(),
    makeDropIndex(),
    makeCreateView(),
    makeDropView(),
    makeCreateTrigger(),
    makeDropTrigger(),
  ];

  it('should identify CREATE TABLE statement', () => {
    expect(isCreateTableStatement(makeCreateTable())).toBe(true);
    for (const stmt of allStatements) {
      if (stmt.type !== 'CREATE TABLE') {
        expect(isCreateTableStatement(stmt)).toBe(false);
      }
    }
  });

  it('should identify CREATE INDEX statement', () => {
    expect(isCreateIndexStatement(makeCreateIndex())).toBe(true);
    for (const stmt of allStatements) {
      if (stmt.type !== 'CREATE INDEX') {
        expect(isCreateIndexStatement(stmt)).toBe(false);
      }
    }
  });

  it('should identify ALTER TABLE statement', () => {
    expect(isAlterTableStatement(makeAlterTable())).toBe(true);
    for (const stmt of allStatements) {
      if (stmt.type !== 'ALTER TABLE') {
        expect(isAlterTableStatement(stmt)).toBe(false);
      }
    }
  });

  it('should identify DROP TABLE statement', () => {
    expect(isDropTableStatement(makeDropTable())).toBe(true);
    for (const stmt of allStatements) {
      if (stmt.type !== 'DROP TABLE') {
        expect(isDropTableStatement(stmt)).toBe(false);
      }
    }
  });

  it('should identify DROP INDEX statement', () => {
    expect(isDropIndexStatement(makeDropIndex())).toBe(true);
    for (const stmt of allStatements) {
      if (stmt.type !== 'DROP INDEX') {
        expect(isDropIndexStatement(stmt)).toBe(false);
      }
    }
  });

  it('should identify CREATE VIEW statement', () => {
    expect(isCreateViewStatement(makeCreateView())).toBe(true);
    for (const stmt of allStatements) {
      if (stmt.type !== 'CREATE VIEW') {
        expect(isCreateViewStatement(stmt)).toBe(false);
      }
    }
  });

  it('should identify DROP VIEW statement', () => {
    expect(isDropViewStatement(makeDropView())).toBe(true);
    for (const stmt of allStatements) {
      if (stmt.type !== 'DROP VIEW') {
        expect(isDropViewStatement(stmt)).toBe(false);
      }
    }
  });

  it('should identify CREATE TRIGGER statement', () => {
    expect(isCreateTriggerStatement(makeCreateTrigger())).toBe(true);
    for (const stmt of allStatements) {
      if (stmt.type !== 'CREATE TRIGGER') {
        expect(isCreateTriggerStatement(stmt)).toBe(false);
      }
    }
  });

  it('should identify DROP TRIGGER statement', () => {
    expect(isDropTriggerStatement(makeDropTrigger())).toBe(true);
    for (const stmt of allStatements) {
      if (stmt.type !== 'DROP TRIGGER') {
        expect(isDropTriggerStatement(stmt)).toBe(false);
      }
    }
  });
});

// =============================================================================
// Parse Result Type Guards
// =============================================================================

describe('DDL Types - Parse Result Type Guards', () => {
  it('should identify successful parse result', () => {
    const success: ParseSuccess<CreateTableStatement> = {
      success: true,
      statement: makeCreateTable(),
    };

    expect(isParseSuccess(success)).toBe(true);
    expect(isParseError(success)).toBe(false);
  });

  it('should identify failed parse result', () => {
    const error: ParseError = {
      success: false,
      error: 'Unexpected token',
      position: 10,
    };

    expect(isParseError(error)).toBe(true);
    expect(isParseSuccess(error)).toBe(false);
  });

  it('should work with generic ParseResult type', () => {
    const result: ParseResult<CreateTableStatement> = {
      success: true,
      statement: makeCreateTable(),
    };

    if (isParseSuccess(result)) {
      expect(result.statement!.type).toBe('CREATE TABLE');
    }
  });

  it('should narrow error type correctly', () => {
    const result: ParseResult = {
      success: false,
      error: 'Missing table name',
      position: 13,
    };

    if (isParseError(result)) {
      expect(result.error).toBe('Missing table name');
      expect(result.position).toBe(13);
    }
  });
});
