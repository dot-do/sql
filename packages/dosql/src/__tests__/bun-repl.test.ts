/**
 * Bun CLI REPL Tests - RED Phase TDD
 *
 * These tests document the expected behavior for a Bun-based CLI REPL
 * for DoSQL that can run locally with bun:sqlite or connect remotely
 * via HTTP/WebSocket.
 *
 * All tests use `it.fails()` to mark them as RED phase tests that
 * document expected behavior but are not yet implemented.
 *
 * Issue: sql-mc40.1
 *
 * Expected features:
 * - REPL command parsing (.help, .quit, .tables, etc)
 * - SQL execution and result formatting
 * - Multi-line statement detection
 * - History management
 * - Remote connection modes (HTTP/WebSocket)
 *
 * @see src/cli/repl.ts - Future implementation location
 * @packageDocumentation
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// =============================================================================
// MOCK TYPES - Define expected interfaces before implementation exists
// =============================================================================

/**
 * REPL configuration options
 */
interface REPLConfig {
  mode: 'local' | 'http' | 'websocket';
  database?: string;
  url?: string;
  historyFile?: string;
  maxHistorySize?: number;
  multilineEnabled?: boolean;
  prompt?: string;
  format?: 'table' | 'json' | 'csv' | 'vertical';
}

/**
 * REPL command handler result
 */
interface CommandResult {
  handled: boolean;
  output?: string;
  exit?: boolean;
  error?: Error;
}

/**
 * SQL execution result
 */
interface ExecutionResult {
  rows: Record<string, unknown>[];
  columns: string[];
  rowCount: number;
  changes?: number;
  lastInsertRowid?: number | bigint;
  duration: number;
}

/**
 * History entry
 */
interface HistoryEntry {
  input: string;
  timestamp: Date;
  successful: boolean;
}

// =============================================================================
// STUB IMPORTS - These will fail until implementation exists
// =============================================================================

// These imports will fail until the REPL module is implemented
// The tests document what SHOULD be exported from the module

// Uncomment when implementing:
// import {
//   BunREPL,
//   parseREPLCommand,
//   isMultilineStatement,
//   formatResults,
//   HistoryManager,
//   createLocalConnection,
//   createHTTPConnection,
//   createWebSocketConnection,
// } from '../cli/repl.js';

// =============================================================================
// 1. REPL COMMAND PARSING
// =============================================================================

describe('REPL Command Parsing', () => {
  describe('Built-in Commands', () => {
    /**
     * RED: .help command should display available commands
     * Expected: parseREPLCommand('.help') returns help text
     */
    it.fails('should parse .help command', () => {
      // EXPECTED: parseREPLCommand exists and handles .help
      // ACTUAL: Module not yet implemented
      const { parseREPLCommand } = require('../cli/repl.js');

      const result = parseREPLCommand('.help');

      expect(result.handled).toBe(true);
      expect(result.output).toContain('.help');
      expect(result.output).toContain('.quit');
      expect(result.output).toContain('.tables');
    });

    /**
     * RED: .quit and .exit commands should signal REPL termination
     */
    it.fails('should parse .quit command', () => {
      const { parseREPLCommand } = require('../cli/repl.js');

      const result = parseREPLCommand('.quit');

      expect(result.handled).toBe(true);
      expect(result.exit).toBe(true);
    });

    /**
     * RED: .exit is an alias for .quit
     */
    it.fails('should parse .exit command as alias for .quit', () => {
      const { parseREPLCommand } = require('../cli/repl.js');

      const result = parseREPLCommand('.exit');

      expect(result.handled).toBe(true);
      expect(result.exit).toBe(true);
    });

    /**
     * RED: .tables should list all tables in the database
     */
    it.fails('should parse .tables command', () => {
      const { parseREPLCommand } = require('../cli/repl.js');

      const result = parseREPLCommand('.tables');

      expect(result.handled).toBe(true);
      // Output will contain table names when executed
    });

    /**
     * RED: .schema should show table schema
     */
    it.fails('should parse .schema command with optional table name', () => {
      const { parseREPLCommand } = require('../cli/repl.js');

      // Without argument - shows all schemas
      const result1 = parseREPLCommand('.schema');
      expect(result1.handled).toBe(true);

      // With table name - shows specific schema
      const result2 = parseREPLCommand('.schema users');
      expect(result2.handled).toBe(true);
    });

    /**
     * RED: .mode should change output format
     */
    it.fails('should parse .mode command', () => {
      const { parseREPLCommand } = require('../cli/repl.js');

      const result = parseREPLCommand('.mode json');

      expect(result.handled).toBe(true);
      // Mode should be changed to 'json'
    });

    /**
     * RED: .mode should support table, json, csv, vertical formats
     */
    it.fails('should support multiple output formats', () => {
      const { parseREPLCommand } = require('../cli/repl.js');

      const formats = ['table', 'json', 'csv', 'vertical'];

      for (const format of formats) {
        const result = parseREPLCommand(`.mode ${format}`);
        expect(result.handled).toBe(true);
        expect(result.error).toBeUndefined();
      }
    });

    /**
     * RED: .mode with invalid format should error
     */
    it.fails('should reject invalid output format', () => {
      const { parseREPLCommand } = require('../cli/repl.js');

      const result = parseREPLCommand('.mode invalid');

      expect(result.handled).toBe(true);
      expect(result.error).toBeDefined();
      expect(result.error?.message).toMatch(/invalid.*format|unknown.*mode/i);
    });

    /**
     * RED: .headers command to toggle column headers
     */
    it.fails('should parse .headers command', () => {
      const { parseREPLCommand } = require('../cli/repl.js');

      const onResult = parseREPLCommand('.headers on');
      expect(onResult.handled).toBe(true);

      const offResult = parseREPLCommand('.headers off');
      expect(offResult.handled).toBe(true);
    });

    /**
     * RED: .databases should list attached databases
     */
    it.fails('should parse .databases command', () => {
      const { parseREPLCommand } = require('../cli/repl.js');

      const result = parseREPLCommand('.databases');

      expect(result.handled).toBe(true);
    });

    /**
     * RED: .open should open a new database
     */
    it.fails('should parse .open command', () => {
      const { parseREPLCommand } = require('../cli/repl.js');

      const result = parseREPLCommand('.open /path/to/database.db');

      expect(result.handled).toBe(true);
    });

    /**
     * RED: .read should execute SQL from a file
     */
    it.fails('should parse .read command', () => {
      const { parseREPLCommand } = require('../cli/repl.js');

      const result = parseREPLCommand('.read /path/to/script.sql');

      expect(result.handled).toBe(true);
    });

    /**
     * RED: .timer should toggle query timing display
     */
    it.fails('should parse .timer command', () => {
      const { parseREPLCommand } = require('../cli/repl.js');

      const onResult = parseREPLCommand('.timer on');
      expect(onResult.handled).toBe(true);

      const offResult = parseREPLCommand('.timer off');
      expect(offResult.handled).toBe(true);
    });

    /**
     * RED: Unknown dot commands should error gracefully
     */
    it.fails('should handle unknown dot commands', () => {
      const { parseREPLCommand } = require('../cli/repl.js');

      const result = parseREPLCommand('.unknowncommand');

      expect(result.handled).toBe(true);
      expect(result.error).toBeDefined();
      expect(result.error?.message).toMatch(/unknown.*command/i);
    });

    /**
     * RED: Non-dot commands should not be handled (pass to SQL executor)
     */
    it.fails('should not handle SQL statements', () => {
      const { parseREPLCommand } = require('../cli/repl.js');

      const result = parseREPLCommand('SELECT * FROM users');

      expect(result.handled).toBe(false);
    });
  });
});

// =============================================================================
// 2. SQL EXECUTION AND RESULT FORMATTING
// =============================================================================

describe('SQL Execution and Result Formatting', () => {
  describe('Result Formatting', () => {
    /**
     * RED: Table format should render ASCII table
     */
    it.fails('should format results as ASCII table', () => {
      const { formatResults } = require('../cli/repl.js');

      const result: ExecutionResult = {
        rows: [
          { id: 1, name: 'Alice', email: 'alice@example.com' },
          { id: 2, name: 'Bob', email: 'bob@example.com' },
        ],
        columns: ['id', 'name', 'email'],
        rowCount: 2,
        duration: 5,
      };

      const output = formatResults(result, 'table');

      // Should have header row with column names
      expect(output).toContain('id');
      expect(output).toContain('name');
      expect(output).toContain('email');

      // Should have data rows
      expect(output).toContain('Alice');
      expect(output).toContain('Bob');

      // Should have table borders
      expect(output).toMatch(/[+\-|]/);
    });

    /**
     * RED: JSON format should output valid JSON
     */
    it.fails('should format results as JSON', () => {
      const { formatResults } = require('../cli/repl.js');

      const result: ExecutionResult = {
        rows: [
          { id: 1, name: 'Alice' },
          { id: 2, name: 'Bob' },
        ],
        columns: ['id', 'name'],
        rowCount: 2,
        duration: 5,
      };

      const output = formatResults(result, 'json');

      // Should be valid JSON
      const parsed = JSON.parse(output);
      expect(parsed).toHaveLength(2);
      expect(parsed[0].name).toBe('Alice');
    });

    /**
     * RED: CSV format should output valid CSV
     */
    it.fails('should format results as CSV', () => {
      const { formatResults } = require('../cli/repl.js');

      const result: ExecutionResult = {
        rows: [
          { id: 1, name: 'Alice', note: 'Has "quotes"' },
          { id: 2, name: 'Bob', note: 'Has, comma' },
        ],
        columns: ['id', 'name', 'note'],
        rowCount: 2,
        duration: 5,
      };

      const output = formatResults(result, 'csv');

      // Should have header line
      expect(output.split('\n')[0]).toBe('id,name,note');

      // Should properly escape quotes and commas
      expect(output).toContain('"Has ""quotes"""');
      expect(output).toContain('"Has, comma"');
    });

    /**
     * RED: Vertical format should show one column per line
     */
    it.fails('should format results as vertical', () => {
      const { formatResults } = require('../cli/repl.js');

      const result: ExecutionResult = {
        rows: [{ id: 1, name: 'Alice', email: 'alice@example.com' }],
        columns: ['id', 'name', 'email'],
        rowCount: 1,
        duration: 5,
      };

      const output = formatResults(result, 'vertical');

      // Each column on separate line
      expect(output).toMatch(/id:\s*1/);
      expect(output).toMatch(/name:\s*Alice/);
      expect(output).toMatch(/email:\s*alice@example.com/);
    });

    /**
     * RED: Empty results should show appropriate message
     */
    it.fails('should handle empty result sets', () => {
      const { formatResults } = require('../cli/repl.js');

      const result: ExecutionResult = {
        rows: [],
        columns: ['id', 'name'],
        rowCount: 0,
        duration: 1,
      };

      const output = formatResults(result, 'table');

      expect(output).toMatch(/no rows|empty|0 rows/i);
    });

    /**
     * RED: DML statements should show affected rows
     */
    it.fails('should format DML results showing changes', () => {
      const { formatResults } = require('../cli/repl.js');

      const result: ExecutionResult = {
        rows: [],
        columns: [],
        rowCount: 0,
        changes: 5,
        duration: 2,
      };

      const output = formatResults(result, 'table');

      expect(output).toMatch(/5.*rows?.*changed|changed.*5/i);
    });

    /**
     * RED: INSERT should show lastInsertRowid
     */
    it.fails('should show lastInsertRowid for INSERT', () => {
      const { formatResults } = require('../cli/repl.js');

      const result: ExecutionResult = {
        rows: [],
        columns: [],
        rowCount: 0,
        changes: 1,
        lastInsertRowid: 42n,
        duration: 1,
      };

      const output = formatResults(result, 'table');

      expect(output).toMatch(/last.*id.*42|rowid.*42/i);
    });

    /**
     * RED: NULL values should be displayed distinctly
     */
    it.fails('should display NULL values distinctly', () => {
      const { formatResults } = require('../cli/repl.js');

      const result: ExecutionResult = {
        rows: [{ id: 1, name: null, value: 'test' }],
        columns: ['id', 'name', 'value'],
        rowCount: 1,
        duration: 1,
      };

      const output = formatResults(result, 'table');

      // NULL should be visually distinct (not empty string)
      expect(output).toMatch(/NULL|<null>|\(null\)/i);
    });

    /**
     * RED: BLOB values should be displayed as hex or truncated
     */
    it.fails('should display BLOB values appropriately', () => {
      const { formatResults } = require('../cli/repl.js');

      const result: ExecutionResult = {
        rows: [{ id: 1, data: new Uint8Array([0xDE, 0xAD, 0xBE, 0xEF]) }],
        columns: ['id', 'data'],
        rowCount: 1,
        duration: 1,
      };

      const output = formatResults(result, 'table');

      // Should show hex or indicate it's a blob
      expect(output).toMatch(/DEADBEEF|<blob>|\[blob\]/i);
    });
  });

  describe('Query Timing', () => {
    /**
     * RED: Should display query duration when timer is on
     */
    it.fails('should include timing information', () => {
      const { formatResults } = require('../cli/repl.js');

      const result: ExecutionResult = {
        rows: [{ id: 1 }],
        columns: ['id'],
        rowCount: 1,
        duration: 15.5,
      };

      const output = formatResults(result, 'table', { showTiming: true });

      expect(output).toMatch(/15\.5\s*ms|time.*15\.5/i);
    });
  });
});

// =============================================================================
// 3. MULTI-LINE STATEMENT DETECTION
// =============================================================================

describe('Multi-line Statement Detection', () => {
  /**
   * RED: Complete statements end with semicolon
   */
  it.fails('should detect complete single-line statement', () => {
    const { isMultilineStatement } = require('../cli/repl.js');

    expect(isMultilineStatement('SELECT * FROM users;')).toBe(false);
  });

  /**
   * RED: Incomplete statements don't end with semicolon
   */
  it.fails('should detect incomplete statement without semicolon', () => {
    const { isMultilineStatement } = require('../cli/repl.js');

    expect(isMultilineStatement('SELECT * FROM users')).toBe(true);
  });

  /**
   * RED: Semicolons inside strings should not count
   */
  it.fails('should ignore semicolons inside string literals', () => {
    const { isMultilineStatement } = require('../cli/repl.js');

    // Semicolon is inside string, statement is incomplete
    expect(isMultilineStatement("SELECT 'hello; world'")).toBe(true);

    // Statement properly terminated
    expect(isMultilineStatement("SELECT 'hello; world';")).toBe(false);
  });

  /**
   * RED: Should handle escaped quotes in strings
   */
  it.fails('should handle escaped quotes in strings', () => {
    const { isMultilineStatement } = require('../cli/repl.js');

    // Escaped quote, semicolon still inside string
    expect(isMultilineStatement("SELECT 'it''s; fine'")).toBe(true);
    expect(isMultilineStatement("SELECT 'it''s; fine';")).toBe(false);
  });

  /**
   * RED: Should handle double-quoted identifiers
   */
  it.fails('should handle double-quoted identifiers', () => {
    const { isMultilineStatement } = require('../cli/repl.js');

    expect(isMultilineStatement('SELECT "column;name" FROM t')).toBe(true);
    expect(isMultilineStatement('SELECT "column;name" FROM t;')).toBe(false);
  });

  /**
   * RED: Should handle multi-line accumulated input
   */
  it.fails('should accumulate multi-line input', () => {
    const { isMultilineStatement } = require('../cli/repl.js');

    const lines = [
      'SELECT',
      '  id,',
      '  name',
      'FROM users',
      'WHERE id = 1;',
    ];

    // Accumulating lines
    expect(isMultilineStatement(lines[0])).toBe(true);
    expect(isMultilineStatement(lines.slice(0, 2).join('\n'))).toBe(true);
    expect(isMultilineStatement(lines.slice(0, 3).join('\n'))).toBe(true);
    expect(isMultilineStatement(lines.slice(0, 4).join('\n'))).toBe(true);

    // Complete statement
    expect(isMultilineStatement(lines.join('\n'))).toBe(false);
  });

  /**
   * RED: Should handle comments
   */
  it.fails('should handle SQL comments', () => {
    const { isMultilineStatement } = require('../cli/repl.js');

    // Single-line comment with semicolon
    expect(isMultilineStatement('SELECT 1 -- comment;')).toBe(true);
    expect(isMultilineStatement('SELECT 1; -- comment')).toBe(false);

    // Block comment
    expect(isMultilineStatement('SELECT /* ; */ 1')).toBe(true);
    expect(isMultilineStatement('SELECT /* ; */ 1;')).toBe(false);
  });

  /**
   * RED: Should handle BEGIN/END blocks
   */
  it.fails('should handle BEGIN/END transaction blocks', () => {
    const { isMultilineStatement } = require('../cli/repl.js');

    // BEGIN without matching COMMIT/ROLLBACK
    expect(isMultilineStatement('BEGIN')).toBe(true);
    expect(isMultilineStatement('BEGIN;')).toBe(false); // Single statement

    // Full transaction block
    const txBlock = `
      BEGIN;
      INSERT INTO t VALUES (1);
      COMMIT;
    `;
    expect(isMultilineStatement(txBlock)).toBe(false);
  });

  /**
   * RED: Should handle CREATE TRIGGER with compound statements
   */
  it.fails('should handle CREATE TRIGGER blocks', () => {
    const { isMultilineStatement } = require('../cli/repl.js');

    const trigger = `
      CREATE TRIGGER update_timestamp
      AFTER UPDATE ON users
      BEGIN
        UPDATE users SET updated_at = datetime('now')
        WHERE id = NEW.id
    `;
    expect(isMultilineStatement(trigger)).toBe(true);

    const completeTrigger = trigger + ';\nEND;';
    expect(isMultilineStatement(completeTrigger)).toBe(false);
  });

  /**
   * RED: Empty input should not be multi-line
   */
  it.fails('should handle empty input', () => {
    const { isMultilineStatement } = require('../cli/repl.js');

    expect(isMultilineStatement('')).toBe(false);
    expect(isMultilineStatement('   ')).toBe(false);
    expect(isMultilineStatement('\n\n')).toBe(false);
  });

  /**
   * RED: Dot commands are complete regardless of semicolon
   */
  it.fails('should treat dot commands as complete', () => {
    const { isMultilineStatement } = require('../cli/repl.js');

    expect(isMultilineStatement('.help')).toBe(false);
    expect(isMultilineStatement('.tables')).toBe(false);
    expect(isMultilineStatement('.mode json')).toBe(false);
  });
});

// =============================================================================
// 4. HISTORY MANAGEMENT
// =============================================================================

describe('History Management', () => {
  describe('HistoryManager', () => {
    /**
     * RED: Should add entries to history
     */
    it.fails('should add entries to history', () => {
      const { HistoryManager } = require('../cli/repl.js');

      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT 1;');
      history.add('SELECT 2;');

      expect(history.size()).toBe(2);
    });

    /**
     * RED: Should not add duplicate consecutive entries
     */
    it.fails('should not add duplicate consecutive entries', () => {
      const { HistoryManager } = require('../cli/repl.js');

      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT 1;');
      history.add('SELECT 1;');
      history.add('SELECT 1;');

      expect(history.size()).toBe(1);
    });

    /**
     * RED: Should respect max size limit
     */
    it.fails('should respect max history size', () => {
      const { HistoryManager } = require('../cli/repl.js');

      const history = new HistoryManager({ maxSize: 3 });

      history.add('SELECT 1;');
      history.add('SELECT 2;');
      history.add('SELECT 3;');
      history.add('SELECT 4;');

      expect(history.size()).toBe(3);
      // Oldest entry should be removed
      expect(history.getAll().map(e => e.input)).not.toContain('SELECT 1;');
    });

    /**
     * RED: Should navigate history with up/down
     */
    it.fails('should navigate history', () => {
      const { HistoryManager } = require('../cli/repl.js');

      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT 1;');
      history.add('SELECT 2;');
      history.add('SELECT 3;');

      // Navigate up (backward in time)
      expect(history.previous()).toBe('SELECT 3;');
      expect(history.previous()).toBe('SELECT 2;');
      expect(history.previous()).toBe('SELECT 1;');
      expect(history.previous()).toBe('SELECT 1;'); // Stay at oldest

      // Navigate down (forward in time)
      expect(history.next()).toBe('SELECT 2;');
      expect(history.next()).toBe('SELECT 3;');
      expect(history.next()).toBe(''); // Back to empty prompt
    });

    /**
     * RED: Should reset navigation position after new input
     */
    it.fails('should reset navigation after new input', () => {
      const { HistoryManager } = require('../cli/repl.js');

      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT 1;');
      history.add('SELECT 2;');

      history.previous(); // Navigate to SELECT 2
      history.add('SELECT 3;'); // New input

      // Should start from the newest entry again
      expect(history.previous()).toBe('SELECT 3;');
    });

    /**
     * RED: Should not add empty or whitespace-only entries
     */
    it.fails('should not add empty entries', () => {
      const { HistoryManager } = require('../cli/repl.js');

      const history = new HistoryManager({ maxSize: 100 });

      history.add('');
      history.add('   ');
      history.add('\n\t');

      expect(history.size()).toBe(0);
    });

    /**
     * RED: Should not add dot commands to history by default
     */
    it.fails('should optionally exclude dot commands', () => {
      const { HistoryManager } = require('../cli/repl.js');

      const history = new HistoryManager({
        maxSize: 100,
        excludeDotCommands: true,
      });

      history.add('.help');
      history.add('SELECT 1;');
      history.add('.tables');

      expect(history.size()).toBe(1);
      expect(history.getAll()[0].input).toBe('SELECT 1;');
    });

    /**
     * RED: Should search history
     */
    it.fails('should search history with pattern', () => {
      const { HistoryManager } = require('../cli/repl.js');

      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT * FROM users;');
      history.add('SELECT * FROM orders;');
      history.add('INSERT INTO users VALUES (1);');
      history.add('SELECT id FROM users;');

      const results = history.search('users');

      expect(results).toHaveLength(3);
      expect(results.every(r => r.input.includes('users'))).toBe(true);
    });

    /**
     * RED: Should persist history to file
     */
    it.fails('should save history to file', async () => {
      const { HistoryManager } = require('../cli/repl.js');

      const mockWriteFile = vi.fn();
      const history = new HistoryManager({
        maxSize: 100,
        historyFile: '~/.dosql_history',
        writeFile: mockWriteFile,
      });

      history.add('SELECT 1;');
      history.add('SELECT 2;');

      await history.save();

      expect(mockWriteFile).toHaveBeenCalledWith(
        expect.stringContaining('.dosql_history'),
        expect.stringContaining('SELECT 1;')
      );
    });

    /**
     * RED: Should load history from file
     */
    it.fails('should load history from file', async () => {
      const { HistoryManager } = require('../cli/repl.js');

      const mockReadFile = vi.fn().mockResolvedValue(
        'SELECT 1;\nSELECT 2;\nSELECT 3;'
      );

      const history = new HistoryManager({
        maxSize: 100,
        historyFile: '~/.dosql_history',
        readFile: mockReadFile,
      });

      await history.load();

      expect(history.size()).toBe(3);
    });

    /**
     * RED: Should handle missing history file gracefully
     */
    it.fails('should handle missing history file', async () => {
      const { HistoryManager } = require('../cli/repl.js');

      const mockReadFile = vi.fn().mockRejectedValue(
        new Error('ENOENT: file not found')
      );

      const history = new HistoryManager({
        maxSize: 100,
        historyFile: '~/.dosql_history',
        readFile: mockReadFile,
      });

      // Should not throw
      await expect(history.load()).resolves.not.toThrow();
      expect(history.size()).toBe(0);
    });

    /**
     * RED: Should clear history
     */
    it.fails('should clear history', () => {
      const { HistoryManager } = require('../cli/repl.js');

      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT 1;');
      history.add('SELECT 2;');
      history.clear();

      expect(history.size()).toBe(0);
    });
  });
});

// =============================================================================
// 5. REMOTE CONNECTION MODES
// =============================================================================

describe('Remote Connection Modes', () => {
  describe('Local bun:sqlite Connection', () => {
    /**
     * RED: Should create local SQLite connection
     */
    it.fails('should create local SQLite connection', async () => {
      const { createLocalConnection } = require('../cli/repl.js');

      const conn = await createLocalConnection({ database: ':memory:' });

      expect(conn).toBeDefined();
      expect(conn.execute).toBeInstanceOf(Function);
      expect(conn.close).toBeInstanceOf(Function);
    });

    /**
     * RED: Should execute queries on local connection
     */
    it.fails('should execute queries on local connection', async () => {
      const { createLocalConnection } = require('../cli/repl.js');

      const conn = await createLocalConnection({ database: ':memory:' });

      await conn.execute('CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)');
      await conn.execute("INSERT INTO test (name) VALUES ('Alice')");

      const result = await conn.execute('SELECT * FROM test');

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].name).toBe('Alice');

      await conn.close();
    });

    /**
     * RED: Should handle connection errors
     */
    it.fails('should handle invalid database path', async () => {
      const { createLocalConnection } = require('../cli/repl.js');

      await expect(
        createLocalConnection({ database: '/nonexistent/path/db.sqlite' })
      ).rejects.toThrow(/cannot open|not found|ENOENT/i);
    });
  });

  describe('HTTP Connection', () => {
    /**
     * RED: Should create HTTP connection to DoSQL endpoint
     */
    it.fails('should create HTTP connection', async () => {
      const { createHTTPConnection } = require('../cli/repl.js');

      const conn = await createHTTPConnection({
        url: 'https://sql.example.com',
        apiKey: 'test-key',
      });

      expect(conn).toBeDefined();
      expect(conn.execute).toBeInstanceOf(Function);
    });

    /**
     * RED: Should send queries over HTTP
     */
    it.fails('should execute queries over HTTP', async () => {
      const { createHTTPConnection } = require('../cli/repl.js');

      const mockFetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({
          rows: [{ id: 1, name: 'Alice' }],
          columns: ['id', 'name'],
        }),
      });

      const conn = await createHTTPConnection({
        url: 'https://sql.example.com',
        apiKey: 'test-key',
        fetch: mockFetch,
      });

      const result = await conn.execute('SELECT * FROM users');

      expect(mockFetch).toHaveBeenCalledWith(
        'https://sql.example.com/query',
        expect.objectContaining({
          method: 'POST',
          headers: expect.objectContaining({
            'Authorization': 'Bearer test-key',
            'Content-Type': 'application/json',
          }),
          body: expect.stringContaining('SELECT * FROM users'),
        })
      );

      expect(result.rows).toHaveLength(1);
    });

    /**
     * RED: Should handle HTTP errors
     */
    it.fails('should handle HTTP errors', async () => {
      const { createHTTPConnection } = require('../cli/repl.js');

      const mockFetch = vi.fn().mockResolvedValue({
        ok: false,
        status: 401,
        statusText: 'Unauthorized',
      });

      const conn = await createHTTPConnection({
        url: 'https://sql.example.com',
        apiKey: 'invalid-key',
        fetch: mockFetch,
      });

      await expect(conn.execute('SELECT 1')).rejects.toThrow(/401|unauthorized/i);
    });

    /**
     * RED: Should handle network errors
     */
    it.fails('should handle network errors', async () => {
      const { createHTTPConnection } = require('../cli/repl.js');

      const mockFetch = vi.fn().mockRejectedValue(new Error('Network error'));

      const conn = await createHTTPConnection({
        url: 'https://sql.example.com',
        apiKey: 'test-key',
        fetch: mockFetch,
      });

      await expect(conn.execute('SELECT 1')).rejects.toThrow(/network|connection/i);
    });

    /**
     * RED: Should support query timeout
     */
    it.fails('should support query timeout', async () => {
      const { createHTTPConnection } = require('../cli/repl.js');

      const mockFetch = vi.fn().mockImplementation(
        () => new Promise(resolve => setTimeout(resolve, 10000))
      );

      const conn = await createHTTPConnection({
        url: 'https://sql.example.com',
        apiKey: 'test-key',
        fetch: mockFetch,
        timeout: 1000,
      });

      await expect(conn.execute('SELECT 1')).rejects.toThrow(/timeout/i);
    });
  });

  describe('WebSocket Connection', () => {
    /**
     * RED: Should create WebSocket connection to DoSQL endpoint
     */
    it.fails('should create WebSocket connection', async () => {
      const { createWebSocketConnection } = require('../cli/repl.js');

      const mockWs = {
        readyState: 1, // OPEN
        send: vi.fn(),
        close: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
      };

      const mockConnect = vi.fn().mockResolvedValue(mockWs);

      const conn = await createWebSocketConnection({
        url: 'wss://sql.example.com/ws',
        apiKey: 'test-key',
        connect: mockConnect,
      });

      expect(conn).toBeDefined();
      expect(conn.execute).toBeInstanceOf(Function);
    });

    /**
     * RED: Should send queries over WebSocket with JSON-RPC
     */
    it.fails('should execute queries over WebSocket', async () => {
      const { createWebSocketConnection } = require('../cli/repl.js');

      const messageHandlers: ((event: { data: string }) => void)[] = [];
      const mockWs = {
        readyState: 1,
        send: vi.fn().mockImplementation((data: string) => {
          const request = JSON.parse(data);
          // Simulate response
          setTimeout(() => {
            messageHandlers.forEach(handler =>
              handler({
                data: JSON.stringify({
                  jsonrpc: '2.0',
                  id: request.id,
                  result: {
                    rows: [{ id: 1 }],
                    columns: ['id'],
                  },
                }),
              })
            );
          }, 10);
        }),
        close: vi.fn(),
        addEventListener: vi.fn().mockImplementation((event, handler) => {
          if (event === 'message') messageHandlers.push(handler);
        }),
        removeEventListener: vi.fn(),
      };

      const mockConnect = vi.fn().mockResolvedValue(mockWs);

      const conn = await createWebSocketConnection({
        url: 'wss://sql.example.com/ws',
        apiKey: 'test-key',
        connect: mockConnect,
      });

      const result = await conn.execute('SELECT * FROM users');

      expect(mockWs.send).toHaveBeenCalledWith(
        expect.stringContaining('SELECT * FROM users')
      );
      expect(result.rows).toHaveLength(1);
    });

    /**
     * RED: Should handle WebSocket close
     */
    it.fails('should handle WebSocket close', async () => {
      const { createWebSocketConnection } = require('../cli/repl.js');

      let closeHandler: () => void = () => {};
      const mockWs = {
        readyState: 1,
        send: vi.fn(),
        close: vi.fn(),
        addEventListener: vi.fn().mockImplementation((event, handler) => {
          if (event === 'close') closeHandler = handler;
        }),
        removeEventListener: vi.fn(),
      };

      const mockConnect = vi.fn().mockResolvedValue(mockWs);

      const conn = await createWebSocketConnection({
        url: 'wss://sql.example.com/ws',
        apiKey: 'test-key',
        connect: mockConnect,
      });

      // Simulate close
      mockWs.readyState = 3; // CLOSED
      closeHandler();

      await expect(conn.execute('SELECT 1')).rejects.toThrow(/closed|disconnected/i);
    });

    /**
     * RED: Should support automatic reconnection
     */
    it.fails('should support automatic reconnection', async () => {
      const { createWebSocketConnection } = require('../cli/repl.js');

      let connectCount = 0;
      const mockConnect = vi.fn().mockImplementation(() => {
        connectCount++;
        return Promise.resolve({
          readyState: 1,
          send: vi.fn(),
          close: vi.fn(),
          addEventListener: vi.fn(),
          removeEventListener: vi.fn(),
        });
      });

      const conn = await createWebSocketConnection({
        url: 'wss://sql.example.com/ws',
        apiKey: 'test-key',
        connect: mockConnect,
        autoReconnect: true,
      });

      expect(connectCount).toBe(1);

      // Simulate disconnect and reconnect
      await conn.reconnect();

      expect(connectCount).toBe(2);
    });

    /**
     * RED: Should queue messages during reconnection
     */
    it.fails('should queue messages during reconnection', async () => {
      const { createWebSocketConnection } = require('../cli/repl.js');

      const sentMessages: string[] = [];
      let messageHandlers: ((event: { data: string }) => void)[] = [];

      const createMockWs = () => ({
        readyState: 1,
        send: vi.fn().mockImplementation((data: string) => {
          sentMessages.push(data);
          const request = JSON.parse(data);
          setTimeout(() => {
            messageHandlers.forEach(handler =>
              handler({
                data: JSON.stringify({
                  jsonrpc: '2.0',
                  id: request.id,
                  result: { rows: [], columns: [] },
                }),
              })
            );
          }, 10);
        }),
        close: vi.fn(),
        addEventListener: vi.fn().mockImplementation((event, handler) => {
          if (event === 'message') messageHandlers.push(handler);
        }),
        removeEventListener: vi.fn(),
      });

      const mockConnect = vi.fn().mockImplementation(() =>
        Promise.resolve(createMockWs())
      );

      const conn = await createWebSocketConnection({
        url: 'wss://sql.example.com/ws',
        apiKey: 'test-key',
        connect: mockConnect,
        autoReconnect: true,
      });

      // Start a query
      const query1 = conn.execute('SELECT 1');

      // Start reconnection while query is pending
      const reconnectPromise = conn.reconnect();

      // Start another query during reconnection
      const query2 = conn.execute('SELECT 2');

      await reconnectPromise;
      await Promise.all([query1, query2]);

      // Both queries should have been sent
      expect(sentMessages.some(m => m.includes('SELECT 1'))).toBe(true);
      expect(sentMessages.some(m => m.includes('SELECT 2'))).toBe(true);
    });
  });

  describe('Connection Selection', () => {
    /**
     * RED: BunREPL should select connection based on config
     */
    it.fails('should select local connection by default', async () => {
      const { BunREPL } = require('../cli/repl.js');

      const repl = new BunREPL({ mode: 'local', database: ':memory:' });

      expect(repl.connectionMode).toBe('local');
    });

    /**
     * RED: Should use HTTP when url is http/https
     */
    it.fails('should use HTTP connection for http URL', async () => {
      const { BunREPL } = require('../cli/repl.js');

      const repl = new BunREPL({
        mode: 'http',
        url: 'https://sql.example.com',
      });

      expect(repl.connectionMode).toBe('http');
    });

    /**
     * RED: Should use WebSocket when url is ws/wss
     */
    it.fails('should use WebSocket connection for ws URL', async () => {
      const { BunREPL } = require('../cli/repl.js');

      const repl = new BunREPL({
        mode: 'websocket',
        url: 'wss://sql.example.com/ws',
      });

      expect(repl.connectionMode).toBe('websocket');
    });

    /**
     * RED: .connect command should switch connections
     */
    it.fails('should switch connections with .connect command', async () => {
      const { BunREPL, parseREPLCommand } = require('../cli/repl.js');

      const result = parseREPLCommand('.connect wss://sql.example.com/ws');

      expect(result.handled).toBe(true);
      // Connection mode should change when command is executed
    });

    /**
     * RED: .disconnect should close current connection
     */
    it.fails('should disconnect with .disconnect command', async () => {
      const { parseREPLCommand } = require('../cli/repl.js');

      const result = parseREPLCommand('.disconnect');

      expect(result.handled).toBe(true);
    });

    /**
     * RED: .status should show connection info
     */
    it.fails('should show connection status', async () => {
      const { parseREPLCommand } = require('../cli/repl.js');

      const result = parseREPLCommand('.status');

      expect(result.handled).toBe(true);
      // Output should contain connection mode, URL, etc.
    });
  });
});

// =============================================================================
// 6. REPL LIFECYCLE
// =============================================================================

describe('REPL Lifecycle', () => {
  /**
   * RED: Should initialize REPL with config
   */
  it.fails('should initialize BunREPL', async () => {
    const { BunREPL } = require('../cli/repl.js');

    const repl = new BunREPL({
      mode: 'local',
      database: ':memory:',
      prompt: 'dosql> ',
      format: 'table',
    });

    expect(repl.prompt).toBe('dosql> ');
    expect(repl.format).toBe('table');
  });

  /**
   * RED: Should process input line by line
   */
  it.fails('should process input', async () => {
    const { BunREPL } = require('../cli/repl.js');

    const repl = new BunREPL({ mode: 'local', database: ':memory:' });

    const output = await repl.processInput('SELECT 1 AS value;');

    expect(output).toContain('value');
    expect(output).toContain('1');
  });

  /**
   * RED: Should handle CTRL+C gracefully
   */
  it.fails('should handle interrupt signal', async () => {
    const { BunREPL } = require('../cli/repl.js');

    const repl = new BunREPL({ mode: 'local', database: ':memory:' });

    // Start a long-running query
    const queryPromise = repl.processInput('SELECT * FROM generate_series(1, 1000000);');

    // Interrupt
    repl.interrupt();

    // Should be cancelled
    await expect(queryPromise).rejects.toThrow(/cancel|interrupt/i);
  });

  /**
   * RED: Should cleanup on exit
   */
  it.fails('should cleanup on exit', async () => {
    const { BunREPL } = require('../cli/repl.js');

    const mockClose = vi.fn();
    const repl = new BunREPL({
      mode: 'local',
      database: ':memory:',
      onClose: mockClose,
    });

    await repl.close();

    expect(mockClose).toHaveBeenCalled();
  });

  /**
   * RED: Should display welcome message on start
   */
  it.fails('should display welcome message', async () => {
    const { BunREPL } = require('../cli/repl.js');

    const output: string[] = [];
    const repl = new BunREPL({
      mode: 'local',
      database: ':memory:',
      output: (msg: string) => output.push(msg),
    });

    await repl.start();

    expect(output.some(line => line.includes('DoSQL'))).toBe(true);
    expect(output.some(line => line.includes('.help'))).toBe(true);
  });

  /**
   * RED: Should change prompt for multi-line input
   */
  it.fails('should change prompt for multi-line input', async () => {
    const { BunREPL } = require('../cli/repl.js');

    const repl = new BunREPL({
      mode: 'local',
      database: ':memory:',
      prompt: 'dosql> ',
      multilinePrompt: '   ...> ',
    });

    // Input incomplete statement
    await repl.processInput('SELECT');

    expect(repl.currentPrompt).toBe('   ...> ');

    // Complete the statement
    await repl.processInput('1;');

    expect(repl.currentPrompt).toBe('dosql> ');
  });
});

// =============================================================================
// 7. ERROR HANDLING
// =============================================================================

describe('Error Handling', () => {
  /**
   * RED: Should display SQL errors clearly
   */
  it.fails('should display SQL syntax errors', async () => {
    const { BunREPL } = require('../cli/repl.js');

    const repl = new BunREPL({ mode: 'local', database: ':memory:' });

    const output = await repl.processInput('SELEC * FROM users;');

    expect(output).toMatch(/syntax|error|near/i);
  });

  /**
   * RED: Should display table not found errors
   */
  it.fails('should display table not found errors', async () => {
    const { BunREPL } = require('../cli/repl.js');

    const repl = new BunREPL({ mode: 'local', database: ':memory:' });

    const output = await repl.processInput('SELECT * FROM nonexistent;');

    expect(output).toMatch(/no such table|not found|nonexistent/i);
  });

  /**
   * RED: Should continue after errors
   */
  it.fails('should continue after errors', async () => {
    const { BunREPL } = require('../cli/repl.js');

    const repl = new BunREPL({ mode: 'local', database: ':memory:' });

    // Error
    await repl.processInput('INVALID SQL;');

    // Should still work
    const output = await repl.processInput('SELECT 1;');
    expect(output).toContain('1');
  });

  /**
   * RED: Should handle connection errors for remote modes
   */
  it.fails('should handle connection errors', async () => {
    const { BunREPL } = require('../cli/repl.js');

    const repl = new BunREPL({
      mode: 'http',
      url: 'https://invalid.example.com',
    });

    const output = await repl.processInput('SELECT 1;');

    expect(output).toMatch(/connection|error|failed/i);
  });
});
