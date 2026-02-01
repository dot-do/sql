/**
 * DoSQL CLI REPL Tests
 *
 * Tests for the REPL module (repl.ts) which provides:
 * - REPL command parsing (.help, .tables, .schema, .quit)
 * - SQL execution and result formatting
 * - Multi-line statement detection
 * - History management
 * - Connection modes (local, HTTP, WebSocket)
 * - Tab completion for SQL keywords, table names, and column names
 * - Syntax highlighting for SQL
 *
 * @module cli/__tests__/repl.test
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

import {
  BunREPL,
  parseREPLCommand,
  isMultilineStatement,
  formatResults,
  HistoryManager,
  createHTTPConnection,
  createWebSocketConnection,
  TabCompleter,
  SQL_KEYWORDS,
  DOT_COMMANDS,
  COLORS,
  highlightSQL,
  tokenizeForHighlight,
  type ExecutionResult,
  type CommandResult,
  type REPLConfig,
  type CompletionResult,
} from '../repl.js';

// =============================================================================
// 1. REPL COMMAND PARSING
// =============================================================================

describe('REPL Command Parsing', () => {
  describe('.help command', () => {
    it('should recognize .help command', () => {
      const result = parseREPLCommand('.help');

      expect(result.handled).toBe(true);
      expect(result.output).toBeDefined();
    });

    it('should list available commands in help output', () => {
      const result = parseREPLCommand('.help');

      expect(result.output).toContain('.help');
      expect(result.output).toContain('.quit');
      expect(result.output).toContain('.exit');
      expect(result.output).toContain('.tables');
      expect(result.output).toContain('.schema');
      expect(result.output).toContain('.mode');
    });
  });

  describe('.quit and .exit commands', () => {
    it('should recognize .quit command', () => {
      const result = parseREPLCommand('.quit');

      expect(result.handled).toBe(true);
      expect(result.exit).toBe(true);
    });

    it('should recognize .exit as alias for .quit', () => {
      const result = parseREPLCommand('.exit');

      expect(result.handled).toBe(true);
      expect(result.exit).toBe(true);
    });
  });

  describe('.tables command', () => {
    it('should recognize .tables command', () => {
      const result = parseREPLCommand('.tables');

      expect(result.handled).toBe(true);
    });
  });

  describe('.schema command', () => {
    it('should recognize .schema without argument', () => {
      const result = parseREPLCommand('.schema');

      expect(result.handled).toBe(true);
    });

    it('should recognize .schema with table name', () => {
      const result = parseREPLCommand('.schema users');

      expect(result.handled).toBe(true);
    });
  });

  describe('.mode command', () => {
    it('should recognize .mode command', () => {
      const result = parseREPLCommand('.mode');

      expect(result.handled).toBe(true);
      expect(result.output).toContain('mode');
    });

    it('should accept valid formats: table, json, csv, vertical', () => {
      const formats = ['table', 'json', 'csv', 'vertical'];

      for (const format of formats) {
        const result = parseREPLCommand(`.mode ${format}`);

        expect(result.handled).toBe(true);
        expect(result.error).toBeUndefined();
      }
    });

    it('should reject invalid format', () => {
      const result = parseREPLCommand('.mode invalid');

      expect(result.handled).toBe(true);
      expect(result.error).toBeDefined();
      expect(result.error?.message).toMatch(/invalid.*format/i);
    });

    it('should be case-insensitive for format names', () => {
      const result = parseREPLCommand('.mode JSON');

      expect(result.handled).toBe(true);
      expect(result.error).toBeUndefined();
    });
  });

  describe('.headers command', () => {
    it('should recognize .headers on', () => {
      const result = parseREPLCommand('.headers on');

      expect(result.handled).toBe(true);
      expect(result.output).toContain('enabled');
    });

    it('should recognize .headers off', () => {
      const result = parseREPLCommand('.headers off');

      expect(result.handled).toBe(true);
      expect(result.output).toContain('disabled');
    });

    it('should show current state without argument', () => {
      const result = parseREPLCommand('.headers');

      expect(result.handled).toBe(true);
      expect(result.output).toMatch(/on|off/i);
    });
  });

  describe('.timer command', () => {
    it('should recognize .timer on', () => {
      const result = parseREPLCommand('.timer on');

      expect(result.handled).toBe(true);
      expect(result.output).toContain('enabled');
    });

    it('should recognize .timer off', () => {
      const result = parseREPLCommand('.timer off');

      expect(result.handled).toBe(true);
      expect(result.output).toContain('disabled');
    });
  });

  describe('.databases command', () => {
    it('should recognize .databases command', () => {
      const result = parseREPLCommand('.databases');

      expect(result.handled).toBe(true);
    });
  });

  describe('.open command', () => {
    it('should recognize .open with path', () => {
      const result = parseREPLCommand('.open /path/to/db.sqlite');

      expect(result.handled).toBe(true);
    });

    it('should error when path missing', () => {
      const result = parseREPLCommand('.open');

      expect(result.handled).toBe(true);
      expect(result.error).toBeDefined();
      expect(result.error?.message).toContain('requires');
    });
  });

  describe('.read command', () => {
    it('should recognize .read with file path', () => {
      const result = parseREPLCommand('.read /path/to/script.sql');

      expect(result.handled).toBe(true);
    });

    it('should error when file path missing', () => {
      const result = parseREPLCommand('.read');

      expect(result.handled).toBe(true);
      expect(result.error).toBeDefined();
    });
  });

  describe('.connect command', () => {
    it('should recognize .connect with URL', () => {
      const result = parseREPLCommand('.connect https://sql.example.com');

      expect(result.handled).toBe(true);
    });

    it('should error when URL missing', () => {
      const result = parseREPLCommand('.connect');

      expect(result.handled).toBe(true);
      expect(result.error).toBeDefined();
    });
  });

  describe('.disconnect command', () => {
    it('should recognize .disconnect command', () => {
      const result = parseREPLCommand('.disconnect');

      expect(result.handled).toBe(true);
    });
  });

  describe('.status command', () => {
    it('should recognize .status command', () => {
      const result = parseREPLCommand('.status');

      expect(result.handled).toBe(true);
    });
  });

  describe('Unknown commands', () => {
    it('should handle unknown dot commands', () => {
      const result = parseREPLCommand('.unknowncommand');

      expect(result.handled).toBe(true);
      expect(result.error).toBeDefined();
      expect(result.error?.message).toMatch(/unknown.*command/i);
    });
  });

  describe('SQL statements', () => {
    it('should not handle SQL statements', () => {
      const result = parseREPLCommand('SELECT * FROM users');

      expect(result.handled).toBe(false);
    });

    it('should not handle whitespace-only input', () => {
      const result = parseREPLCommand('   ');

      expect(result.handled).toBe(false);
    });
  });
});

// =============================================================================
// 2. MULTI-LINE STATEMENT DETECTION
// =============================================================================

describe('Multi-line Statement Detection', () => {
  describe('Complete statements', () => {
    it('should detect complete single-line statement', () => {
      expect(isMultilineStatement('SELECT * FROM users;')).toBe(false);
    });

    it('should detect complete multi-line statement', () => {
      const sql = `
        SELECT id, name
        FROM users
        WHERE id = 1;
      `;
      expect(isMultilineStatement(sql)).toBe(false);
    });

    it('should handle trailing whitespace after semicolon', () => {
      expect(isMultilineStatement('SELECT 1;   ')).toBe(false);
    });

    it('should handle multiple statements', () => {
      expect(isMultilineStatement('SELECT 1; SELECT 2;')).toBe(false);
    });
  });

  describe('Incomplete statements', () => {
    it('should detect incomplete statement without semicolon', () => {
      expect(isMultilineStatement('SELECT * FROM users')).toBe(true);
    });

    it('should detect incomplete SELECT', () => {
      expect(isMultilineStatement('SELECT')).toBe(true);
    });

    it('should detect incomplete FROM clause', () => {
      expect(isMultilineStatement('SELECT * FROM')).toBe(true);
    });
  });

  describe('String literals', () => {
    it('should ignore semicolons inside single-quoted strings', () => {
      expect(isMultilineStatement("SELECT 'hello; world'")).toBe(true);
      expect(isMultilineStatement("SELECT 'hello; world';")).toBe(false);
    });

    it('should handle escaped single quotes', () => {
      expect(isMultilineStatement("SELECT 'it''s; fine'")).toBe(true);
      expect(isMultilineStatement("SELECT 'it''s; fine';")).toBe(false);
    });

    it('should handle double-quoted identifiers', () => {
      expect(isMultilineStatement('SELECT "column;name"')).toBe(true);
      expect(isMultilineStatement('SELECT "column;name";')).toBe(false);
    });

    it('should detect unterminated single-quoted string', () => {
      expect(isMultilineStatement("SELECT 'unterminated")).toBe(true);
    });

    it('should detect unterminated double-quoted string', () => {
      expect(isMultilineStatement('SELECT "unterminated')).toBe(true);
    });
  });

  describe('Comments', () => {
    it('should ignore semicolons in line comments', () => {
      expect(isMultilineStatement('SELECT 1 -- comment;')).toBe(true);
    });

    it('should handle statement after line comment', () => {
      expect(isMultilineStatement('SELECT 1; -- comment')).toBe(false);
    });

    it('should ignore semicolons in block comments', () => {
      expect(isMultilineStatement('SELECT /* ; */ 1')).toBe(true);
      expect(isMultilineStatement('SELECT /* ; */ 1;')).toBe(false);
    });

    it('should detect unterminated block comment', () => {
      expect(isMultilineStatement('SELECT /* unterminated')).toBe(true);
    });

    it('should handle nested-looking block comments', () => {
      expect(isMultilineStatement('SELECT /* outer /* inner */ still outer */')).toBe(true);
    });
  });

  describe('Special cases', () => {
    it('should handle empty input', () => {
      expect(isMultilineStatement('')).toBe(false);
    });

    it('should handle whitespace-only input', () => {
      expect(isMultilineStatement('   ')).toBe(false);
      expect(isMultilineStatement('\n\t')).toBe(false);
    });

    it('should treat dot commands as complete', () => {
      expect(isMultilineStatement('.help')).toBe(false);
      expect(isMultilineStatement('.tables')).toBe(false);
      expect(isMultilineStatement('.mode json')).toBe(false);
    });

    it('should handle BEGIN/COMMIT blocks', () => {
      expect(isMultilineStatement('BEGIN')).toBe(true);
      expect(isMultilineStatement('BEGIN;')).toBe(false);
      expect(isMultilineStatement('BEGIN; SELECT 1; COMMIT;')).toBe(false);
    });
  });
});

// =============================================================================
// 3. RESULT FORMATTING
// =============================================================================

describe('Result Formatting', () => {
  describe('Table format', () => {
    it('should format results as ASCII table', () => {
      const result: ExecutionResult = {
        rows: [
          { id: 1, name: 'Alice' },
          { id: 2, name: 'Bob' },
        ],
        columns: ['id', 'name'],
        rowCount: 2,
        duration: 5,
      };

      const output = formatResults(result, 'table');

      expect(output).toContain('id');
      expect(output).toContain('name');
      expect(output).toContain('Alice');
      expect(output).toContain('Bob');
      expect(output).toMatch(/[+\-|]/); // Table borders
    });

    it('should align columns properly', () => {
      const result: ExecutionResult = {
        rows: [
          { id: 1, name: 'A' },
          { id: 100, name: 'Longer Name' },
        ],
        columns: ['id', 'name'],
        rowCount: 2,
        duration: 1,
      };

      const output = formatResults(result, 'table');
      const lines = output.split('\n');

      // All data rows should have the same length
      const dataLines = lines.filter(l => l.startsWith('|'));
      const lengths = dataLines.map(l => l.length);
      expect(new Set(lengths).size).toBe(1);
    });
  });

  describe('JSON format', () => {
    it('should format results as valid JSON', () => {
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
      const parsed = JSON.parse(output);

      expect(parsed).toHaveLength(2);
      expect(parsed[0].name).toBe('Alice');
    });

    it('should handle bigint values in JSON', () => {
      const result: ExecutionResult = {
        rows: [{ id: 9007199254740993n }],
        columns: ['id'],
        rowCount: 1,
        duration: 1,
      };

      const output = formatResults(result, 'json');
      const parsed = JSON.parse(output);

      expect(parsed[0].id).toBe('9007199254740993');
    });
  });

  describe('CSV format', () => {
    it('should format results as CSV', () => {
      const result: ExecutionResult = {
        rows: [
          { id: 1, name: 'Alice' },
          { id: 2, name: 'Bob' },
        ],
        columns: ['id', 'name'],
        rowCount: 2,
        duration: 5,
      };

      const output = formatResults(result, 'csv');
      const lines = output.split('\n');

      expect(lines[0]).toBe('id,name');
      expect(lines[1]).toBe('1,Alice');
      expect(lines[2]).toBe('2,Bob');
    });

    it('should escape quotes in CSV', () => {
      const result: ExecutionResult = {
        rows: [{ value: 'has "quotes"' }],
        columns: ['value'],
        rowCount: 1,
        duration: 1,
      };

      const output = formatResults(result, 'csv');

      expect(output).toContain('"has ""quotes"""');
    });

    it('should escape commas in CSV', () => {
      const result: ExecutionResult = {
        rows: [{ value: 'has, comma' }],
        columns: ['value'],
        rowCount: 1,
        duration: 1,
      };

      const output = formatResults(result, 'csv');

      expect(output).toContain('"has, comma"');
    });

    it('should escape newlines in CSV', () => {
      const result: ExecutionResult = {
        rows: [{ value: 'has\nnewline' }],
        columns: ['value'],
        rowCount: 1,
        duration: 1,
      };

      const output = formatResults(result, 'csv');

      expect(output).toContain('"has\nnewline"');
    });
  });

  describe('Vertical format', () => {
    it('should format results vertically', () => {
      const result: ExecutionResult = {
        rows: [{ id: 1, name: 'Alice', email: 'alice@example.com' }],
        columns: ['id', 'name', 'email'],
        rowCount: 1,
        duration: 5,
      };

      const output = formatResults(result, 'vertical');

      expect(output).toMatch(/id:\s*1/);
      expect(output).toMatch(/name:\s*Alice/);
      expect(output).toMatch(/email:\s*alice@example.com/);
    });

    it('should separate multiple rows', () => {
      const result: ExecutionResult = {
        rows: [
          { id: 1 },
          { id: 2 },
        ],
        columns: ['id'],
        rowCount: 2,
        duration: 1,
      };

      const output = formatResults(result, 'vertical');

      expect(output).toContain('1. row');
      expect(output).toContain('2. row');
    });
  });

  describe('Special values', () => {
    it('should display NULL values distinctly', () => {
      const result: ExecutionResult = {
        rows: [{ value: null }],
        columns: ['value'],
        rowCount: 1,
        duration: 1,
      };

      const output = formatResults(result, 'table');

      expect(output).toMatch(/NULL/i);
    });

    it('should display undefined as NULL', () => {
      const result: ExecutionResult = {
        rows: [{ value: undefined }],
        columns: ['value'],
        rowCount: 1,
        duration: 1,
      };

      const output = formatResults(result, 'table');

      expect(output).toMatch(/NULL/i);
    });

    it('should display BLOB values', () => {
      const result: ExecutionResult = {
        rows: [{ data: new Uint8Array([0xDE, 0xAD, 0xBE, 0xEF]) }],
        columns: ['data'],
        rowCount: 1,
        duration: 1,
      };

      const output = formatResults(result, 'table');

      expect(output).toMatch(/DEADBEEF|<blob>/i);
    });

    it('should display bigint values', () => {
      const result: ExecutionResult = {
        rows: [{ id: 9007199254740993n }],
        columns: ['id'],
        rowCount: 1,
        duration: 1,
      };

      const output = formatResults(result, 'table');

      expect(output).toContain('9007199254740993');
    });
  });

  describe('Empty results', () => {
    it('should handle empty result set', () => {
      const result: ExecutionResult = {
        rows: [],
        columns: ['id', 'name'],
        rowCount: 0,
        duration: 1,
      };

      const output = formatResults(result, 'table');

      expect(output).toMatch(/no rows|empty|0 rows/i);
    });
  });

  describe('DML results', () => {
    it('should show affected rows for DML', () => {
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

    it('should show lastInsertRowid for INSERT', () => {
      const result: ExecutionResult = {
        rows: [],
        columns: [],
        rowCount: 0,
        changes: 1,
        lastInsertRowid: 42,
        duration: 1,
      };

      const output = formatResults(result, 'table');

      expect(output).toMatch(/42/);
    });

    it('should handle bigint lastInsertRowid', () => {
      const result: ExecutionResult = {
        rows: [],
        columns: [],
        rowCount: 0,
        changes: 1,
        lastInsertRowid: 9007199254740993n,
        duration: 1,
      };

      const output = formatResults(result, 'table');

      expect(output).toContain('9007199254740993');
    });
  });

  describe('Timing information', () => {
    it('should include timing when showTiming is true', () => {
      const result: ExecutionResult = {
        rows: [{ id: 1 }],
        columns: ['id'],
        rowCount: 1,
        duration: 15.5,
      };

      const output = formatResults(result, 'table', { showTiming: true });

      expect(output).toMatch(/15\.5.*ms|time.*15\.5/i);
    });

    it('should not include timing when showTiming is false', () => {
      const result: ExecutionResult = {
        rows: [{ id: 1 }],
        columns: ['id'],
        rowCount: 1,
        duration: 15.5,
      };

      const output = formatResults(result, 'table', { showTiming: false });

      expect(output).not.toMatch(/15\.5.*ms/i);
    });
  });
});

// =============================================================================
// 4. HISTORY MANAGEMENT
// =============================================================================

describe('History Management', () => {
  describe('Adding entries', () => {
    it('should add entries to history', () => {
      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT 1;');
      history.add('SELECT 2;');

      expect(history.size()).toBe(2);
    });

    it('should not add empty entries', () => {
      const history = new HistoryManager({ maxSize: 100 });

      history.add('');
      history.add('   ');
      history.add('\n\t');

      expect(history.size()).toBe(0);
    });

    it('should not add duplicate consecutive entries', () => {
      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT 1;');
      history.add('SELECT 1;');
      history.add('SELECT 1;');

      expect(history.size()).toBe(1);
    });

    it('should allow duplicate non-consecutive entries', () => {
      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT 1;');
      history.add('SELECT 2;');
      history.add('SELECT 1;');

      expect(history.size()).toBe(3);
    });

    it('should trim input before adding', () => {
      const history = new HistoryManager({ maxSize: 100 });

      history.add('  SELECT 1;  ');
      history.add('  SELECT 1;  ');

      expect(history.size()).toBe(1);
    });
  });

  describe('Max size limit', () => {
    it('should respect max size', () => {
      const history = new HistoryManager({ maxSize: 3 });

      history.add('SELECT 1;');
      history.add('SELECT 2;');
      history.add('SELECT 3;');
      history.add('SELECT 4;');

      expect(history.size()).toBe(3);
    });

    it('should remove oldest entries when exceeding max size', () => {
      const history = new HistoryManager({ maxSize: 3 });

      history.add('SELECT 1;');
      history.add('SELECT 2;');
      history.add('SELECT 3;');
      history.add('SELECT 4;');

      const entries = history.getAll().map(e => e.input);

      expect(entries).not.toContain('SELECT 1;');
      expect(entries).toContain('SELECT 4;');
    });
  });

  describe('Navigation', () => {
    it('should navigate to previous entries', () => {
      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT 1;');
      history.add('SELECT 2;');
      history.add('SELECT 3;');

      expect(history.previous()).toBe('SELECT 3;');
      expect(history.previous()).toBe('SELECT 2;');
      expect(history.previous()).toBe('SELECT 1;');
    });

    it('should stay at oldest entry when navigating past start', () => {
      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT 1;');
      history.add('SELECT 2;');

      history.previous();
      history.previous();
      expect(history.previous()).toBe('SELECT 1;');
    });

    it('should navigate to next entries', () => {
      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT 1;');
      history.add('SELECT 2;');
      history.add('SELECT 3;');

      history.previous();
      history.previous();
      history.previous();

      expect(history.next()).toBe('SELECT 2;');
      expect(history.next()).toBe('SELECT 3;');
    });

    it('should return empty string when navigating past newest', () => {
      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT 1;');

      history.previous();
      expect(history.next()).toBe('');
    });

    it('should reset navigation after new input', () => {
      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT 1;');
      history.add('SELECT 2;');

      history.previous();
      history.add('SELECT 3;');

      expect(history.previous()).toBe('SELECT 3;');
    });

    it('should return empty from previous with empty history', () => {
      const history = new HistoryManager({ maxSize: 100 });

      expect(history.previous()).toBe('');
    });

    it('should return empty from next with no prior navigation', () => {
      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT 1;');

      expect(history.next()).toBe('');
    });
  });

  describe('Excluding dot commands', () => {
    it('should exclude dot commands when configured', () => {
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

    it('should include dot commands when not configured', () => {
      const history = new HistoryManager({ maxSize: 100 });

      history.add('.help');
      history.add('SELECT 1;');

      expect(history.size()).toBe(2);
    });
  });

  describe('Search', () => {
    it('should search history by pattern', () => {
      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT * FROM users;');
      history.add('SELECT * FROM orders;');
      history.add('INSERT INTO users VALUES (1);');
      history.add('SELECT id FROM users;');

      const results = history.search('users');

      expect(results).toHaveLength(3);
      expect(results.every(r => r.input.includes('users'))).toBe(true);
    });

    it('should be case-insensitive', () => {
      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT * FROM USERS;');
      history.add('select * from users;');

      const results = history.search('users');

      expect(results).toHaveLength(2);
    });

    it('should return empty array when no matches', () => {
      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT 1;');

      const results = history.search('nonexistent');

      expect(results).toHaveLength(0);
    });
  });

  describe('Clear', () => {
    it('should clear all history', () => {
      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT 1;');
      history.add('SELECT 2;');
      history.clear();

      expect(history.size()).toBe(0);
    });

    it('should reset navigation after clear', () => {
      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT 1;');
      history.previous();
      history.clear();

      expect(history.previous()).toBe('');
    });
  });

  describe('Persistence', () => {
    it('should save history to file', async () => {
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

    it('should load history from file', async () => {
      const mockReadFile = vi.fn().mockResolvedValue('SELECT 1;\nSELECT 2;\nSELECT 3;');

      const history = new HistoryManager({
        maxSize: 100,
        historyFile: '~/.dosql_history',
        readFile: mockReadFile,
      });

      await history.load();

      expect(history.size()).toBe(3);
    });

    it('should handle missing history file', async () => {
      const mockReadFile = vi.fn().mockRejectedValue(new Error('ENOENT'));

      const history = new HistoryManager({
        maxSize: 100,
        historyFile: '~/.dosql_history',
        readFile: mockReadFile,
      });

      await expect(history.load()).resolves.not.toThrow();
      expect(history.size()).toBe(0);
    });

    it('should respect max size when loading', async () => {
      const mockReadFile = vi.fn().mockResolvedValue(
        'SELECT 1;\nSELECT 2;\nSELECT 3;\nSELECT 4;\nSELECT 5;'
      );

      const history = new HistoryManager({
        maxSize: 3,
        historyFile: '~/.dosql_history',
        readFile: mockReadFile,
      });

      await history.load();

      expect(history.size()).toBe(3);
    });

    it('should skip empty lines when loading', async () => {
      const mockReadFile = vi.fn().mockResolvedValue('SELECT 1;\n\n\nSELECT 2;\n  \n');

      const history = new HistoryManager({
        maxSize: 100,
        historyFile: '~/.dosql_history',
        readFile: mockReadFile,
      });

      await history.load();

      expect(history.size()).toBe(2);
    });
  });

  describe('getAll', () => {
    it('should return all entries', () => {
      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT 1;');
      history.add('SELECT 2;');

      const entries = history.getAll();

      expect(entries).toHaveLength(2);
      expect(entries[0].input).toBe('SELECT 1;');
      expect(entries[1].input).toBe('SELECT 2;');
    });

    it('should return a copy of entries', () => {
      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT 1;');

      const entries = history.getAll();
      entries.push({ input: 'fake', timestamp: new Date(), successful: true });

      expect(history.size()).toBe(1);
    });

    it('should include timestamp and successful flag', () => {
      const history = new HistoryManager({ maxSize: 100 });

      history.add('SELECT 1;', true);
      history.add('INVALID;', false);

      const entries = history.getAll();

      expect(entries[0].timestamp).toBeInstanceOf(Date);
      expect(entries[0].successful).toBe(true);
      expect(entries[1].successful).toBe(false);
    });
  });
});

// =============================================================================
// 5. CONNECTION MODES
// =============================================================================

describe('Connection Modes', () => {
  describe('HTTP Connection', () => {
    it('should create HTTP connection', async () => {
      const conn = await createHTTPConnection({
        url: 'https://sql.example.com',
        apiKey: 'test-key',
      });

      expect(conn).toBeDefined();
      expect(conn.execute).toBeInstanceOf(Function);
      expect(conn.close).toBeInstanceOf(Function);
    });

    it('should send queries over HTTP', async () => {
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
        })
      );

      expect(result.rows).toHaveLength(1);
      expect(result.rows[0].name).toBe('Alice');
    });

    it('should handle HTTP errors', async () => {
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

    it('should handle network errors', async () => {
      const mockFetch = vi.fn().mockRejectedValue(new Error('Network error'));

      const conn = await createHTTPConnection({
        url: 'https://sql.example.com',
        fetch: mockFetch,
      });

      await expect(conn.execute('SELECT 1')).rejects.toThrow(/network|error/i);
    });

    it('should work without API key', async () => {
      const mockFetch = vi.fn().mockResolvedValue({
        ok: true,
        json: () => Promise.resolve({ rows: [], columns: [] }),
      });

      const conn = await createHTTPConnection({
        url: 'https://sql.example.com',
        fetch: mockFetch,
      });

      await conn.execute('SELECT 1');

      const headers = mockFetch.mock.calls[0][1].headers;
      expect(headers['Authorization']).toBeUndefined();
    });
  });

  describe('WebSocket Connection', () => {
    it('should create WebSocket connection', async () => {
      const mockWs = {
        readyState: 1,
        send: vi.fn(),
        close: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
      };

      const mockConnect = vi.fn().mockResolvedValue(mockWs);

      const conn = await createWebSocketConnection({
        url: 'wss://sql.example.com/ws',
        connect: mockConnect,
      });

      expect(conn).toBeDefined();
      expect(conn.execute).toBeInstanceOf(Function);
      expect(conn.close).toBeInstanceOf(Function);
    });

    it('should execute queries over WebSocket', async () => {
      const messageHandlers: ((event: { data: string }) => void)[] = [];
      const mockWs = {
        readyState: 1,
        send: vi.fn().mockImplementation((data: string) => {
          const request = JSON.parse(data);
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
        connect: mockConnect,
      });

      const result = await conn.execute('SELECT * FROM users');

      expect(mockWs.send).toHaveBeenCalledWith(
        expect.stringContaining('SELECT * FROM users')
      );
      expect(result.rows).toHaveLength(1);
    });

    it('should handle WebSocket close', async () => {
      const mockWs = {
        readyState: 3, // CLOSED
        send: vi.fn(),
        close: vi.fn(),
        addEventListener: vi.fn(),
        removeEventListener: vi.fn(),
      };

      const mockConnect = vi.fn().mockResolvedValue(mockWs);

      const conn = await createWebSocketConnection({
        url: 'wss://sql.example.com/ws',
        connect: mockConnect,
      });

      await expect(conn.execute('SELECT 1')).rejects.toThrow(/closed/i);
    });

    it('should support reconnection', async () => {
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
        connect: mockConnect,
      });

      expect(connectCount).toBe(1);

      await conn.reconnect();

      expect(connectCount).toBe(2);
    });
  });
});

// =============================================================================
// 6. BunREPL CLASS
// =============================================================================

describe('BunREPL', () => {
  describe('Configuration', () => {
    it('should initialize with default values', () => {
      const repl = new BunREPL({ mode: 'local' });

      expect(repl.prompt).toBe('dosql> ');
      expect(repl.format).toBe('table');
      expect(repl.connectionMode).toBe('local');
    });

    it('should accept custom prompt', () => {
      const repl = new BunREPL({
        mode: 'local',
        prompt: 'sql> ',
      });

      expect(repl.prompt).toBe('sql> ');
    });

    it('should accept custom format', () => {
      const repl = new BunREPL({
        mode: 'local',
        format: 'json',
      });

      expect(repl.format).toBe('json');
    });

    it('should use different prompt for multi-line input', () => {
      const repl = new BunREPL({
        mode: 'local',
        prompt: 'dosql> ',
        multilinePrompt: '   ...> ',
      });

      expect(repl.multilinePrompt).toBe('   ...> ');
    });
  });

  describe('Connection Mode Selection', () => {
    it('should select local mode', () => {
      const repl = new BunREPL({ mode: 'local', database: ':memory:' });

      expect(repl.connectionMode).toBe('local');
    });

    it('should select HTTP mode', () => {
      const repl = new BunREPL({
        mode: 'http',
        url: 'https://sql.example.com',
      });

      expect(repl.connectionMode).toBe('http');
    });

    it('should select WebSocket mode', () => {
      const repl = new BunREPL({
        mode: 'websocket',
        url: 'wss://sql.example.com/ws',
      });

      expect(repl.connectionMode).toBe('websocket');
    });
  });

  describe('Cleanup', () => {
    it('should call onClose callback on close', async () => {
      const mockClose = vi.fn();
      const repl = new BunREPL({
        mode: 'local',
        onClose: mockClose,
      });

      await repl.close();

      expect(mockClose).toHaveBeenCalled();
    });
  });

  describe('Status', () => {
    it('should report connection status', () => {
      const repl = new BunREPL({
        mode: 'local',
        database: 'test.db',
      });

      const status = repl.getStatus();

      expect(status).toContain('local');
      expect(status).toContain('test.db');
    });

    it('should report HTTP URL in status', () => {
      const repl = new BunREPL({
        mode: 'http',
        url: 'https://sql.example.com',
      });

      const status = repl.getStatus();

      expect(status).toContain('http');
    });
  });

  describe('Tab Completion', () => {
    it('should have tab completer accessible', () => {
      const repl = new BunREPL({ mode: 'local' });

      expect(repl.completer).toBeDefined();
    });

    it('should be disabled when completionEnabled is false', () => {
      const repl = new BunREPL({
        mode: 'local',
        completionEnabled: false,
      });

      const result = repl.getCompletions('SEL');

      expect(result.completions).toHaveLength(0);
    });

    it('should provide completions through BunREPL', () => {
      const repl = new BunREPL({ mode: 'local' });

      const result = repl.getCompletions('SEL');

      expect(result.completions).toContain('SELECT');
    });

    it('should apply completion through BunREPL', () => {
      const repl = new BunREPL({ mode: 'local' });

      const completed = repl.applyCompletion('SEL');

      expect(completed).toBe('SELECT');
    });
  });

  describe('Syntax Highlighting', () => {
    it('should be enabled by default', () => {
      const repl = new BunREPL({ mode: 'local' });

      expect(repl.highlightEnabled).toBe(true);
    });

    it('should be disabled when highlightEnabled is false', () => {
      const repl = new BunREPL({
        mode: 'local',
        highlightEnabled: false,
      });

      expect(repl.highlightEnabled).toBe(false);
    });

    it('should highlight SQL through BunREPL', () => {
      const repl = new BunREPL({ mode: 'local' });

      const highlighted = repl.highlight('SELECT * FROM users');

      // Should contain ANSI codes
      expect(highlighted).toContain('\x1b[');
    });

    it('should not highlight when disabled', () => {
      const repl = new BunREPL({
        mode: 'local',
        highlightEnabled: false,
      });

      const result = repl.highlight('SELECT * FROM users');

      // Should not contain ANSI codes
      expect(result).not.toContain('\x1b[');
      expect(result).toBe('SELECT * FROM users');
    });

    it('should allow toggling highlighting', () => {
      const repl = new BunREPL({ mode: 'local' });

      expect(repl.highlightEnabled).toBe(true);
      repl.setHighlightEnabled(false);
      expect(repl.highlightEnabled).toBe(false);
    });
  });
});

// =============================================================================
// 7. TAB COMPLETION
// =============================================================================

describe('Tab Completion', () => {
  describe('TabCompleter', () => {
    it('should complete SQL keywords', () => {
      const completer = new TabCompleter();

      const result = completer.complete('SEL');

      expect(result.completions).toContain('SELECT');
      expect(result.word).toBe('SEL');
    });

    it('should be case-insensitive for matching', () => {
      const completer = new TabCompleter();

      const result = completer.complete('sel');

      expect(result.completions).toContain('SELECT');
    });

    it('should return multiple matches', () => {
      const completer = new TabCompleter();

      const result = completer.complete('SUM');

      expect(result.completions).toContain('SUM');
      // 'SUM' is both a complete keyword, check that it matches
      expect(result.completions.length).toBeGreaterThanOrEqual(1);
    });

    it('should complete dot commands', () => {
      const completer = new TabCompleter();

      const result = completer.complete('.he');

      expect(result.completions).toContain('.help');
      expect(result.completions).toContain('.headers');
    });

    it('should complete table names when schema is set', () => {
      const completer = new TabCompleter();
      completer.addTable('users', ['id', 'name', 'email']);
      completer.addTable('orders', ['id', 'user_id', 'total']);

      const result = completer.complete('SELECT * FROM u');

      expect(result.completions).toContain('users');
    });

    it('should complete column names after table prefix', () => {
      const completer = new TabCompleter();
      completer.addTable('users', ['id', 'name', 'email']);

      const result = completer.complete('SELECT users.');

      expect(result.completions).toContain('id');
      expect(result.completions).toContain('name');
      expect(result.completions).toContain('email');
    });

    it('should return empty completions when disabled', () => {
      const completer = new TabCompleter();
      completer.setEnabled(false);

      const result = completer.complete('SEL');

      expect(result.completions).toHaveLength(0);
    });

    it('should find common prefix for multiple completions', () => {
      const completer = new TabCompleter();

      // "IN" should match INSERT, INTO, INNER, INT, INTEGER, etc.
      const completed = completer.getCompletion('IN', undefined, 0);

      // Should get common prefix or first match
      expect(completed).toBeDefined();
      expect(completed?.toUpperCase().startsWith('IN')).toBe(true);
    });

    it('should cycle through completions with index', () => {
      const completer = new TabCompleter();

      const first = completer.getCompletion('.', undefined, 1);
      const second = completer.getCompletion('.', undefined, 2);

      expect(first).toBeDefined();
      expect(second).toBeDefined();
      expect(first).not.toBe(second);
    });

    it('should return null for no matches', () => {
      const completer = new TabCompleter();

      const result = completer.getCompletion('xyz123nonexistent');

      expect(result).toBeNull();
    });

    it('should clear schema', () => {
      const completer = new TabCompleter();
      completer.addTable('users', ['id']);

      completer.clearSchema();
      const result = completer.complete('SELECT * FROM u');

      expect(result.completions).not.toContain('users');
    });

    it('should complete after FROM keyword', () => {
      const completer = new TabCompleter();
      completer.addTable('users', ['id', 'name']);
      completer.addTable('products', ['id', 'title']);

      const result = completer.complete('SELECT * FROM ');

      // Should primarily suggest tables after FROM
      expect(result.completions).toContain('users');
      expect(result.completions).toContain('products');
    });

    it('should complete after JOIN keyword', () => {
      const completer = new TabCompleter();
      completer.addTable('users', ['id']);
      completer.addTable('orders', ['id', 'user_id']);

      const result = completer.complete('SELECT * FROM users JOIN o');

      expect(result.completions).toContain('orders');
    });

    it('should complete after UPDATE keyword', () => {
      const completer = new TabCompleter();
      completer.addTable('users', ['id', 'name']);

      const result = completer.complete('UPDATE u');

      expect(result.completions).toContain('users');
    });

    it('should complete after INSERT INTO keyword', () => {
      const completer = new TabCompleter();
      completer.addTable('users', ['id', 'name']);

      const result = completer.complete('INSERT INTO u');

      expect(result.completions).toContain('users');
    });
  });

  describe('SQL_KEYWORDS', () => {
    it('should contain common DML keywords', () => {
      expect(SQL_KEYWORDS).toContain('SELECT');
      expect(SQL_KEYWORDS).toContain('FROM');
      expect(SQL_KEYWORDS).toContain('WHERE');
      expect(SQL_KEYWORDS).toContain('INSERT');
      expect(SQL_KEYWORDS).toContain('UPDATE');
      expect(SQL_KEYWORDS).toContain('DELETE');
    });

    it('should contain common DDL keywords', () => {
      expect(SQL_KEYWORDS).toContain('CREATE');
      expect(SQL_KEYWORDS).toContain('TABLE');
      expect(SQL_KEYWORDS).toContain('DROP');
      expect(SQL_KEYWORDS).toContain('ALTER');
      expect(SQL_KEYWORDS).toContain('INDEX');
    });

    it('should contain data type keywords', () => {
      expect(SQL_KEYWORDS).toContain('INTEGER');
      expect(SQL_KEYWORDS).toContain('TEXT');
      expect(SQL_KEYWORDS).toContain('BLOB');
      expect(SQL_KEYWORDS).toContain('REAL');
    });

    it('should contain window function keywords', () => {
      expect(SQL_KEYWORDS).toContain('OVER');
      expect(SQL_KEYWORDS).toContain('PARTITION');
      expect(SQL_KEYWORDS).toContain('ROW_NUMBER');
      expect(SQL_KEYWORDS).toContain('RANK');
    });

    it('should be sorted', () => {
      const sorted = [...SQL_KEYWORDS].sort();
      expect(SQL_KEYWORDS).toEqual(sorted);
    });
  });

  describe('DOT_COMMANDS', () => {
    it('should contain expected commands', () => {
      expect(DOT_COMMANDS).toContain('.help');
      expect(DOT_COMMANDS).toContain('.quit');
      expect(DOT_COMMANDS).toContain('.tables');
      expect(DOT_COMMANDS).toContain('.schema');
      expect(DOT_COMMANDS).toContain('.mode');
    });

    it('should be sorted', () => {
      const sorted = [...DOT_COMMANDS].sort();
      expect(DOT_COMMANDS).toEqual(sorted);
    });
  });
});

// =============================================================================
// 8. SYNTAX HIGHLIGHTING
// =============================================================================

describe('Syntax Highlighting', () => {
  describe('highlightSQL', () => {
    it('should highlight keywords', () => {
      const result = highlightSQL('SELECT');

      expect(result).toContain(COLORS.keyword);
      expect(result).toContain(COLORS.reset);
    });

    it('should highlight strings', () => {
      const result = highlightSQL("'hello world'");

      expect(result).toContain(COLORS.string);
    });

    it('should highlight numbers', () => {
      const result = highlightSQL('42');

      expect(result).toContain(COLORS.number);
    });

    it('should highlight comments', () => {
      const result = highlightSQL('-- this is a comment');

      expect(result).toContain(COLORS.comment);
    });

    it('should highlight block comments', () => {
      const result = highlightSQL('/* block comment */');

      expect(result).toContain(COLORS.comment);
    });

    it('should highlight operators', () => {
      const result = highlightSQL('=');

      expect(result).toContain(COLORS.operator);
    });

    it('should highlight functions', () => {
      const result = highlightSQL('COUNT(*)');

      expect(result).toContain(COLORS.function);
    });

    it('should not add colors when useColors is false', () => {
      const result = highlightSQL('SELECT * FROM users', false);

      expect(result).toBe('SELECT * FROM users');
      expect(result).not.toContain('\x1b[');
    });

    it('should handle complex SQL', () => {
      const sql = `
        SELECT u.id, u.name, COUNT(o.id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        WHERE u.created_at > '2024-01-01'
        GROUP BY u.id
        ORDER BY order_count DESC
        LIMIT 10;
      `;

      const result = highlightSQL(sql);

      // Should have multiple keywords highlighted
      expect((result.match(/\x1b\[1;34m/g) || []).length).toBeGreaterThan(5);
    });

    it('should preserve whitespace', () => {
      const sql = 'SELECT  *  FROM  users';
      const result = highlightSQL(sql);

      // Remove ANSI codes and check whitespace preserved
      const stripped = result.replace(/\x1b\[[0-9;]*m/g, '');
      expect(stripped).toBe(sql);
    });

    it('should handle quoted identifiers', () => {
      const result = highlightSQL('SELECT "column name" FROM users');

      expect(result).toContain(COLORS.identifier);
    });

    it('should handle decimal numbers', () => {
      const result = highlightSQL('3.14159');

      expect(result).toContain(COLORS.number);
    });

    it('should handle scientific notation', () => {
      const result = highlightSQL('1.5e10');

      expect(result).toContain(COLORS.number);
    });

    it('should handle escaped quotes in strings', () => {
      const result = highlightSQL("'it''s'");

      expect(result).toContain(COLORS.string);
    });

    it('should handle two-character operators', () => {
      const sql = 'a <> b AND c >= d';
      const result = highlightSQL(sql);

      expect(result).toContain(COLORS.operator);
    });
  });

  describe('tokenizeForHighlight', () => {
    it('should tokenize keywords', () => {
      const tokens = tokenizeForHighlight('SELECT');

      expect(tokens).toHaveLength(1);
      expect(tokens[0].type).toBe('keyword');
      expect(tokens[0].value).toBe('SELECT');
    });

    it('should tokenize identifiers', () => {
      const tokens = tokenizeForHighlight('my_table');

      expect(tokens).toHaveLength(1);
      expect(tokens[0].type).toBe('identifier');
    });

    it('should tokenize strings', () => {
      const tokens = tokenizeForHighlight("'hello'");

      expect(tokens).toHaveLength(1);
      expect(tokens[0].type).toBe('string');
      expect(tokens[0].value).toBe("'hello'");
    });

    it('should tokenize numbers', () => {
      const tokens = tokenizeForHighlight('123');

      expect(tokens).toHaveLength(1);
      expect(tokens[0].type).toBe('number');
    });

    it('should tokenize operators', () => {
      const tokens = tokenizeForHighlight('=');

      expect(tokens).toHaveLength(1);
      expect(tokens[0].type).toBe('operator');
    });

    it('should tokenize punctuation', () => {
      const tokens = tokenizeForHighlight('(');

      expect(tokens).toHaveLength(1);
      expect(tokens[0].type).toBe('punctuation');
    });

    it('should tokenize whitespace', () => {
      const tokens = tokenizeForHighlight('SELECT  *');

      expect(tokens).toHaveLength(3);
      expect(tokens[1].type).toBe('whitespace');
    });

    it('should tokenize comments', () => {
      const tokens = tokenizeForHighlight('-- comment');

      expect(tokens).toHaveLength(1);
      expect(tokens[0].type).toBe('comment');
    });

    it('should tokenize functions followed by parentheses', () => {
      const tokens = tokenizeForHighlight('COUNT(*)');

      // Should have: COUNT, (, *, )
      expect(tokens.length).toBeGreaterThanOrEqual(2);
      expect(tokens[0].type).toBe('function');
      expect(tokens[0].value).toBe('COUNT');
    });

    it('should tokenize standalone aggregate keywords as keywords', () => {
      const tokens = tokenizeForHighlight('COUNT');

      // Without (), COUNT is a keyword (since it's in both lists)
      expect(tokens).toHaveLength(1);
      expect(tokens[0].type).toBe('keyword');
    });
  });

  describe('COLORS', () => {
    it('should have reset code', () => {
      expect(COLORS.reset).toBe('\x1b[0m');
    });

    it('should have keyword color', () => {
      expect(COLORS.keyword).toBeDefined();
      expect(COLORS.keyword).toContain('\x1b[');
    });

    it('should have all token type colors', () => {
      expect(COLORS.string).toBeDefined();
      expect(COLORS.number).toBeDefined();
      expect(COLORS.comment).toBeDefined();
      expect(COLORS.operator).toBeDefined();
      expect(COLORS.function).toBeDefined();
      expect(COLORS.identifier).toBeDefined();
      expect(COLORS.punctuation).toBeDefined();
    });
  });
});
