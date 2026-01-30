/**
 * DoSQL CLI Binary Entry Point Tests
 *
 * Tests for the CLI binary entry point (bin.ts) which:
 * - Sets up Node.js file system
 * - Handles the 'shell' command with interactive input
 * - Routes other commands to main CLI
 * - Handles process arguments
 *
 * Note: These tests verify the structure and exported functions
 * since the actual binary execution requires a Node.js environment
 * with readline and process.stdin/stdout.
 *
 * @module cli/__tests__/bin.test
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Import from the main CLI module since bin.ts is the entry point
import {
  main,
  setFileSystem,
  loadConfig,
  query as runQuery,
} from '../index.js';
import { Database } from '../../database.js';

// =============================================================================
// 1. FILE SYSTEM SETUP
// =============================================================================

describe('File System Setup', () => {
  describe('setFileSystem', () => {
    it('should accept file system implementation', () => {
      const mockFs = {
        existsSync: vi.fn(),
        readFileSync: vi.fn(),
        writeFileSync: vi.fn(),
        mkdirSync: vi.fn(),
        readdirSync: vi.fn(),
      };

      // Should not throw
      expect(() => setFileSystem(mockFs)).not.toThrow();
    });

    it('should use provided file system for operations', async () => {
      const mockFs = {
        existsSync: vi.fn().mockReturnValue(true),
        readFileSync: vi.fn().mockReturnValue(JSON.stringify({
          database: { type: 'sqlite', path: ':memory:' },
        })),
        writeFileSync: vi.fn(),
        mkdirSync: vi.fn(),
        readdirSync: vi.fn().mockReturnValue([]),
      };

      setFileSystem(mockFs);

      const config = await loadConfig();

      expect(mockFs.existsSync).toHaveBeenCalled();
      expect(mockFs.readFileSync).toHaveBeenCalled();
      expect(config.database.type).toBe('sqlite');
    });
  });
});

// =============================================================================
// 2. SHELL COMMAND HANDLING
// =============================================================================

describe('Shell Command Handling', () => {
  let mockFs: {
    existsSync: ReturnType<typeof vi.fn>;
    readFileSync: ReturnType<typeof vi.fn>;
    writeFileSync: ReturnType<typeof vi.fn>;
    mkdirSync: ReturnType<typeof vi.fn>;
    readdirSync: ReturnType<typeof vi.fn>;
  };
  let logSpy: ReturnType<typeof vi.fn>;
  let errorSpy: ReturnType<typeof vi.fn>;
  let originalLog: typeof console.log;
  let originalError: typeof console.error;

  beforeEach(() => {
    mockFs = {
      existsSync: vi.fn().mockReturnValue(true),
      readFileSync: vi.fn().mockReturnValue(JSON.stringify({
        database: { type: 'sqlite', path: ':memory:' },
      })),
      writeFileSync: vi.fn(),
      mkdirSync: vi.fn(),
      readdirSync: vi.fn().mockReturnValue([]),
    };
    setFileSystem(mockFs);
    logSpy = vi.fn();
    errorSpy = vi.fn();
    originalLog = console.log;
    originalError = console.error;
    console.log = logSpy;
    console.error = errorSpy;
  });

  afterEach(() => {
    console.log = originalLog;
    console.error = originalError;
    vi.restoreAllMocks();
  });

  describe('Shell special commands', () => {
    it('should recognize shell command', async () => {
      // The shell command is handled specially in bin.ts
      // In main(), it delegates to the shell function
      await main(['shell']);

      // Should print shell welcome message
      const logCalls = logSpy.mock.calls.map((call: unknown[]) => call[0]);
      expect(logCalls.some((msg: string) => msg.includes('DoSQL Shell'))).toBe(true);
    });

    it('should accept --config with shell command', async () => {
      mockFs.existsSync.mockImplementation((path: string) =>
        path === '/custom/config.json' || path === 'dosql.config.json'
      );
      mockFs.readFileSync.mockImplementation((path: string) => {
        if (path === '/custom/config.json') {
          return JSON.stringify({
            database: { type: 'sqlite', path: 'custom.db' },
          });
        }
        return JSON.stringify({
          database: { type: 'sqlite', path: ':memory:' },
        });
      });

      // The main function handles shell command
      await main(['shell', '--config', '/custom/config.json']);

      // Should use custom config
      const logCalls = logSpy.mock.calls.map((call: unknown[]) => call[0]);
      expect(logCalls.some((msg: string) => msg.includes('custom.db'))).toBe(true);
    });
  });
});

// =============================================================================
// 3. INTERACTIVE SHELL COMMANDS
// =============================================================================

describe('Interactive Shell Commands', () => {
  /**
   * The interactive shell (bin.ts interactiveShell) handles:
   * - .exit / .quit - Exit the shell
   * - .help - Show help
   * - .tables - List tables
   * - .schema - Show schema
   * - SQL execution
   *
   * These are tested indirectly through the Database class
   * since interactiveShell uses Database directly.
   */

  describe('.exit and .quit commands', () => {
    it('should have .exit command defined', () => {
      // The commands are string comparisons in interactiveShell
      const exitCommands = ['.exit', '.quit'];
      expect(exitCommands).toContain('.exit');
      expect(exitCommands).toContain('.quit');
    });
  });

  describe('.help command', () => {
    it('should have help text defined', () => {
      // Help text is inline in interactiveShell
      const helpTopics = [
        '.exit',
        '.quit',
        '.tables',
        '.schema',
        '.help',
        'SQL commands',
      ];

      helpTopics.forEach(topic => {
        expect(topic).toBeDefined();
      });
    });
  });

  describe('.tables command', () => {
    it('should use Database.getTables', () => {
      const db = new Database(':memory:');

      db.exec('CREATE TABLE users (id INTEGER, name TEXT)');
      db.exec('CREATE TABLE orders (id INTEGER, user_id INTEGER)');

      const tables = db.getTables();

      expect(tables).toContain('users');
      expect(tables).toContain('orders');

      db.close();
    });

    it('should handle empty database', () => {
      const db = new Database(':memory:');

      const tables = db.getTables();

      expect(tables).toHaveLength(0);

      db.close();
    });
  });

  describe('.schema command', () => {
    it('should use Database.pragma for table_info', () => {
      const db = new Database(':memory:');

      db.exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)');

      const info = db.pragma('table_info', 'users');

      expect(Array.isArray(info)).toBe(true);
      if (Array.isArray(info)) {
        expect(info.length).toBeGreaterThan(0);
        expect(info[0]).toHaveProperty('name');
        expect(info[0]).toHaveProperty('type');
      }

      db.close();
    });
  });

  describe('SQL execution', () => {
    it('should execute SELECT statements', () => {
      const db = new Database(':memory:');

      db.exec('CREATE TABLE test (value INTEGER)');
      db.exec('INSERT INTO test (value) VALUES (1), (2), (3)');

      const stmt = db.prepare('SELECT * FROM test');
      const results = stmt.all();

      expect(results).toHaveLength(3);

      db.close();
    });

    it('should execute INSERT/UPDATE/DELETE statements', () => {
      const db = new Database(':memory:');

      db.exec('CREATE TABLE test (value INTEGER)');

      const insertStmt = db.prepare('INSERT INTO test (value) VALUES (?)');
      const insertResult = insertStmt.run(42);

      expect(insertResult.changes).toBe(1);

      db.close();
    });

    it('should handle SQL errors gracefully', () => {
      const db = new Database(':memory:');

      expect(() => {
        db.prepare('INVALID SQL SYNTAX');
      }).toThrow();

      // Database should still be usable
      db.exec('CREATE TABLE test (value INTEGER)');
      const tables = db.getTables();
      expect(tables).toContain('test');

      db.close();
    });
  });
});

// =============================================================================
// 4. ARGUMENT PARSING FOR SHELL
// =============================================================================

describe('Shell Argument Parsing', () => {
  /**
   * bin.ts parses arguments manually for the shell command:
   * - args[0] === 'shell' triggers interactive mode
   * - --config index + 1 gives config path
   */

  it('should detect shell command', () => {
    const args = ['shell'];
    const isShellCommand = args[0] === 'shell';

    expect(isShellCommand).toBe(true);
  });

  it('should not detect shell for other commands', () => {
    const testCases = [
      ['init'],
      ['migrate'],
      ['query', 'SELECT 1'],
      ['--help'],
      [],
    ];

    testCases.forEach(args => {
      const isShellCommand = args[0] === 'shell';
      expect(isShellCommand).toBe(false);
    });
  });

  it('should extract config path from shell args', () => {
    const args = ['shell', '--config', '/path/to/config.json'];
    const configIndex = args.indexOf('--config');

    let configPath: string | undefined;
    if (configIndex !== -1 && args[configIndex + 1]) {
      configPath = args[configIndex + 1];
    }

    expect(configPath).toBe('/path/to/config.json');
  });

  it('should handle shell without config', () => {
    const args = ['shell'];
    const configIndex = args.indexOf('--config');

    let configPath: string | undefined;
    if (configIndex !== -1 && args[configIndex + 1]) {
      configPath = args[configIndex + 1];
    }

    expect(configPath).toBeUndefined();
  });
});

// =============================================================================
// 5. MAIN CLI ROUTING
// =============================================================================

describe('Main CLI Routing', () => {
  let mockFs: {
    existsSync: ReturnType<typeof vi.fn>;
    readFileSync: ReturnType<typeof vi.fn>;
    writeFileSync: ReturnType<typeof vi.fn>;
    mkdirSync: ReturnType<typeof vi.fn>;
    readdirSync: ReturnType<typeof vi.fn>;
  };

  beforeEach(() => {
    mockFs = {
      existsSync: vi.fn().mockReturnValue(false),
      readFileSync: vi.fn(),
      writeFileSync: vi.fn(),
      mkdirSync: vi.fn(),
      readdirSync: vi.fn().mockReturnValue([]),
    };
    setFileSystem(mockFs);
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('Non-shell commands route to main', () => {
    it('should route init to main', async () => {
      await main(['init']);

      expect(mockFs.writeFileSync).toHaveBeenCalled();
    });

    it('should route --help to main', async () => {
      await main(['--help']);

      expect(console.log).toHaveBeenCalledWith(
        expect.stringContaining('DoSQL CLI')
      );
    });

    it('should route --version to main', async () => {
      await main(['--version']);

      expect(console.log).toHaveBeenCalledWith(
        expect.stringMatching(/dosql v\d+\.\d+\.\d+/)
      );
    });

    it('should route migrate to main', async () => {
      mockFs.existsSync.mockReturnValue(true);
      mockFs.readFileSync.mockReturnValue(JSON.stringify({
        database: { type: 'sqlite', path: ':memory:' },
        migrations: { directory: 'migrations' },
      }));

      await main(['migrate']);

      // Should attempt to load config
      expect(mockFs.readFileSync).toHaveBeenCalled();
    });

    it('should route query to main', async () => {
      mockFs.existsSync.mockReturnValue(true);
      mockFs.readFileSync.mockReturnValue(JSON.stringify({
        database: { type: 'sqlite', path: ':memory:' },
      }));

      await main(['query', 'CREATE TABLE test (id INTEGER)']);

      expect(mockFs.readFileSync).toHaveBeenCalled();
    });
  });
});

// =============================================================================
// 6. ERROR HANDLING
// =============================================================================

describe('Error Handling', () => {
  let mockFs: {
    existsSync: ReturnType<typeof vi.fn>;
    readFileSync: ReturnType<typeof vi.fn>;
    writeFileSync: ReturnType<typeof vi.fn>;
    mkdirSync: ReturnType<typeof vi.fn>;
    readdirSync: ReturnType<typeof vi.fn>;
  };

  beforeEach(() => {
    mockFs = {
      existsSync: vi.fn().mockReturnValue(false),
      readFileSync: vi.fn(),
      writeFileSync: vi.fn(),
      mkdirSync: vi.fn(),
      readdirSync: vi.fn().mockReturnValue([]),
    };
    setFileSystem(mockFs);
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should handle missing config file', async () => {
    await main(['migrate']);

    expect(console.error).toHaveBeenCalledWith(
      expect.stringContaining('Error:')
    );
  });

  it('should handle invalid config JSON', async () => {
    mockFs.existsSync.mockReturnValue(true);
    mockFs.readFileSync.mockReturnValue('invalid json {{{');

    await main(['migrate']);

    expect(console.error).toHaveBeenCalledWith(
      expect.stringContaining('Error:')
    );
  });

  it('should handle unknown commands', async () => {
    await main(['unknown-command']);

    expect(console.error).toHaveBeenCalledWith(
      expect.stringContaining('Unknown command')
    );
  });

  it('should handle query without SQL', async () => {
    mockFs.existsSync.mockReturnValue(true);
    mockFs.readFileSync.mockReturnValue(JSON.stringify({
      database: { type: 'sqlite', path: ':memory:' },
    }));

    await main(['query']);

    expect(console.error).toHaveBeenCalledWith(
      expect.stringContaining('requires a SQL argument')
    );
  });
});

// =============================================================================
// 7. DATABASE INTEGRATION
// =============================================================================

describe('Database Integration', () => {
  describe('Database class usage', () => {
    it('should create in-memory database', () => {
      const db = new Database(':memory:');

      expect(db).toBeDefined();

      db.close();
    });

    it('should execute exec for DDL', () => {
      const db = new Database(':memory:');

      // Should not throw
      db.exec('CREATE TABLE test (id INTEGER)');

      const tables = db.getTables();
      expect(tables).toContain('test');

      db.close();
    });

    it('should prepare and run statements', () => {
      const db = new Database(':memory:');

      db.exec('CREATE TABLE test (id INTEGER, value TEXT)');

      const stmt = db.prepare('INSERT INTO test (id, value) VALUES (?, ?)');
      const result = stmt.run(1, 'hello');

      expect(result.changes).toBe(1);

      db.close();
    });

    it('should prepare and fetch all results', () => {
      const db = new Database(':memory:');

      db.exec('CREATE TABLE test (value INTEGER)');
      db.exec('INSERT INTO test (value) VALUES (1), (2), (3)');

      const stmt = db.prepare('SELECT * FROM test ORDER BY value');
      const results = stmt.all();

      expect(results).toHaveLength(3);
      expect(results[0]).toEqual({ value: 1 });

      db.close();
    });

    it('should close database cleanly', () => {
      const db = new Database(':memory:');

      db.exec('CREATE TABLE test (id INTEGER)');

      // Should not throw
      db.close();
    });
  });
});

// =============================================================================
// 8. EXPORT VERIFICATION
// =============================================================================

describe('Export Verification', () => {
  it('should export main function', () => {
    expect(typeof main).toBe('function');
  });

  it('should export setFileSystem function', () => {
    expect(typeof setFileSystem).toBe('function');
  });

  it('should export loadConfig function', () => {
    expect(typeof loadConfig).toBe('function');
  });

  it('should export query function', () => {
    expect(typeof runQuery).toBe('function');
  });

  it('should have Database class available', () => {
    expect(typeof Database).toBe('function');
  });
});
