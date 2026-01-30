/**
 * DoSQL CLI Index Tests
 *
 * Tests for the main CLI module (index.ts) which provides:
 * - CLI argument parsing (--help, --version, --config, etc.)
 * - Config file loading and validation
 * - Command execution (init, migrate, shell, query)
 * - Error handling and display
 *
 * @module cli/__tests__/index.test
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

import {
  init,
  migrate,
  shell,
  query,
  loadConfig,
  parseArgs,
  setFileSystem,
  main,
  VERSION,
  HELP_TEXT,
  DEFAULT_CONFIG_FILE,
  DEFAULT_MIGRATIONS_DIR,
  type CLIConfig,
  type ParsedArgs,
} from '../index.js';

// =============================================================================
// TEST UTILITIES
// =============================================================================

/**
 * Create a mock file system for testing
 */
function createMockFs() {
  const files = new Map<string, string>();
  const directories = new Set<string>();

  return {
    files,
    directories,
    existsSync: vi.fn((path: string) => files.has(path) || directories.has(path)),
    readFileSync: vi.fn((path: string, _encoding: 'utf-8') => {
      if (!files.has(path)) {
        throw new Error(`ENOENT: no such file or directory, open '${path}'`);
      }
      return files.get(path)!;
    }),
    writeFileSync: vi.fn((path: string, content: string) => {
      files.set(path, content);
    }),
    mkdirSync: vi.fn((path: string, _options?: { recursive?: boolean }) => {
      directories.add(path);
    }),
    readdirSync: vi.fn((path: string) => {
      const result: string[] = [];
      for (const [filePath] of files) {
        if (filePath.startsWith(path + '/')) {
          const relativePath = filePath.slice(path.length + 1);
          if (!relativePath.includes('/')) {
            result.push(relativePath);
          }
        }
      }
      return result;
    }),
    reset() {
      files.clear();
      directories.clear();
    },
  };
}

// =============================================================================
// 1. CLI ARGUMENT PARSING
// =============================================================================

describe('CLI Argument Parsing', () => {
  describe('parseArgs', () => {
    describe('Global Flags', () => {
      it('should parse --help flag', () => {
        const result = parseArgs(['--help']);

        expect(result.options.help).toBe(true);
        expect(result.command).toBe('');
      });

      it('should parse -h flag as alias for --help', () => {
        const result = parseArgs(['-h']);

        expect(result.options.help).toBe(true);
      });

      it('should parse --version flag', () => {
        const result = parseArgs(['--version']);

        expect(result.options.version).toBe(true);
        expect(result.command).toBe('');
      });

      it('should parse -v flag as alias for --version', () => {
        const result = parseArgs(['-v']);

        expect(result.options.version).toBe(true);
      });

      it('should parse --config flag with path', () => {
        const result = parseArgs(['--config', '/path/to/config.json', 'migrate']);

        expect(result.options.configPath).toBe('/path/to/config.json');
        expect(result.command).toBe('migrate');
      });

      it('should throw error when --config missing argument', () => {
        expect(() => parseArgs(['--config'])).toThrow('--config requires a path argument');
      });
    });

    describe('Commands', () => {
      it('should parse init command', () => {
        const result = parseArgs(['init']);

        expect(result.command).toBe('init');
      });

      it('should parse migrate command', () => {
        const result = parseArgs(['migrate']);

        expect(result.command).toBe('migrate');
      });

      it('should parse shell command', () => {
        const result = parseArgs(['shell']);

        expect(result.command).toBe('shell');
      });

      it('should parse query command', () => {
        const result = parseArgs(['query', 'SELECT 1']);

        expect(result.command).toBe('query');
        expect(result.options.sql).toBe('SELECT 1');
      });

      it('should handle empty args', () => {
        const result = parseArgs([]);

        expect(result.command).toBe('');
        expect(result.options).toEqual({});
      });
    });

    describe('Command-specific Options', () => {
      it('should parse migrate --dry-run', () => {
        const result = parseArgs(['migrate', '--dry-run']);

        expect(result.command).toBe('migrate');
        expect(result.options.dryRun).toBe(true);
      });

      it('should parse query --format json', () => {
        const result = parseArgs(['query', 'SELECT 1', '--format', 'json']);

        expect(result.command).toBe('query');
        expect(result.options.format).toBe('json');
      });

      it('should parse query --format table', () => {
        const result = parseArgs(['query', 'SELECT 1', '--format', 'table']);

        expect(result.options.format).toBe('table');
      });

      it('should parse query --format csv', () => {
        const result = parseArgs(['query', 'SELECT 1', '--format', 'csv']);

        expect(result.options.format).toBe('csv');
      });

      it('should throw error when --format missing argument', () => {
        expect(() => parseArgs(['query', 'SELECT 1', '--format'])).toThrow(
          '--format requires a format argument'
        );
      });

      it('should parse --config after command', () => {
        const result = parseArgs(['migrate', '--config', '/path/config.json']);

        expect(result.command).toBe('migrate');
        expect(result.options.configPath).toBe('/path/config.json');
      });

      it('should handle --help after command', () => {
        const result = parseArgs(['migrate', '--help']);

        expect(result.command).toBe('migrate');
        expect(result.options.help).toBe(true);
      });
    });

    describe('Complex Argument Combinations', () => {
      it('should parse multiple flags together', () => {
        const result = parseArgs([
          '--config',
          '/path/config.json',
          'migrate',
          '--dry-run',
        ]);

        expect(result.command).toBe('migrate');
        expect(result.options.configPath).toBe('/path/config.json');
        expect(result.options.dryRun).toBe(true);
      });

      it('should parse query with SQL containing spaces', () => {
        const result = parseArgs(['query', 'SELECT * FROM users WHERE id = 1']);

        expect(result.options.sql).toBe('SELECT * FROM users WHERE id = 1');
      });

      it('should ignore unknown flags', () => {
        const result = parseArgs(['query', 'SELECT 1', '--unknown-flag']);

        expect(result.command).toBe('query');
        expect(result.options.sql).toBe('SELECT 1');
      });
    });
  });
});

// =============================================================================
// 2. CONFIG LOADING
// =============================================================================

describe('Config Loading', () => {
  let mockFs: ReturnType<typeof createMockFs>;

  beforeEach(() => {
    mockFs = createMockFs();
    setFileSystem(mockFs);
  });

  describe('loadConfig', () => {
    it('should load config from default path', async () => {
      mockFs.files.set('dosql.config.json', JSON.stringify({
        database: { type: 'sqlite', path: ':memory:' },
      }));

      const config = await loadConfig();

      expect(config.database.type).toBe('sqlite');
      expect(config.database.path).toBe(':memory:');
    });

    it('should load config from custom path', async () => {
      mockFs.files.set('/custom/path/config.json', JSON.stringify({
        database: { type: 'sqlite', path: 'test.db' },
      }));

      const config = await loadConfig('/custom/path/config.json');

      expect(config.database.path).toBe('test.db');
    });

    it('should throw error when config file not found', async () => {
      await expect(loadConfig()).rejects.toThrow('Config file not found');
    });

    it('should throw error when config file not found at custom path', async () => {
      await expect(loadConfig('/nonexistent/config.json')).rejects.toThrow(
        'Config file not found: /nonexistent/config.json'
      );
    });

    it('should throw error for invalid JSON', async () => {
      mockFs.files.set('dosql.config.json', 'invalid json {{{');

      await expect(loadConfig()).rejects.toThrow('Invalid JSON in config file');
    });

    it('should throw error when missing database field', async () => {
      mockFs.files.set('dosql.config.json', JSON.stringify({}));

      await expect(loadConfig()).rejects.toThrow('Config missing required "database" field');
    });

    it('should throw error when missing database.type field', async () => {
      mockFs.files.set('dosql.config.json', JSON.stringify({ database: {} }));

      await expect(loadConfig()).rejects.toThrow('Config missing required "database.type" field');
    });

    it('should accept valid config with all fields', async () => {
      mockFs.files.set('dosql.config.json', JSON.stringify({
        database: {
          type: 'sqlite',
          path: 'mydb.sqlite',
        },
        migrations: {
          directory: 'db/migrations',
        },
      }));

      const config = await loadConfig();

      expect(config.database.type).toBe('sqlite');
      expect(config.database.path).toBe('mydb.sqlite');
      expect(config.migrations?.directory).toBe('db/migrations');
    });

    it('should accept libsql type', async () => {
      mockFs.files.set('dosql.config.json', JSON.stringify({
        database: {
          type: 'libsql',
          url: 'libsql://example.turso.io',
        },
      }));

      const config = await loadConfig();

      expect(config.database.type).toBe('libsql');
      expect(config.database.url).toBe('libsql://example.turso.io');
    });
  });
});

// =============================================================================
// 3. INIT COMMAND
// =============================================================================

describe('Init Command', () => {
  let mockFs: ReturnType<typeof createMockFs>;
  let logSpy: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    mockFs = createMockFs();
    setFileSystem(mockFs);
    logSpy = vi.fn();
    // Replace console.log with our spy
    const originalLog = console.log;
    console.log = logSpy;
    return () => {
      console.log = originalLog;
    };
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('init', () => {
    it('should create default config file', async () => {
      await init();

      expect(mockFs.writeFileSync).toHaveBeenCalledWith(
        'dosql.config.json',
        expect.any(String)
      );

      const writtenContent = mockFs.writeFileSync.mock.calls[0][1];
      const config = JSON.parse(writtenContent as string);

      expect(config.database.type).toBe('sqlite');
      expect(config.database.path).toBe('dosql.db');
      expect(config.migrations.directory).toBe('migrations');
    });

    it('should create migrations directory', async () => {
      await init();

      expect(mockFs.mkdirSync).toHaveBeenCalledWith(
        'migrations',
        { recursive: true }
      );
    });

    it('should use custom config path when specified', async () => {
      await init({ configPath: '/custom/dosql.config.json' });

      expect(mockFs.writeFileSync).toHaveBeenCalledWith(
        '/custom/dosql.config.json',
        expect.any(String)
      );
    });

    it('should not overwrite existing config file', async () => {
      mockFs.files.set('dosql.config.json', '{}');

      await expect(init()).rejects.toThrow('Config file already exists');
    });

    it('should not create migrations directory if it already exists', async () => {
      mockFs.directories.add('migrations');

      await init();

      // mkdirSync should still be called but with recursive option
      expect(mockFs.mkdirSync).not.toHaveBeenCalled();
    });

    it('should log creation messages', async () => {
      await init();

      // Verify that log was called with the expected messages
      const logCalls = logSpy.mock.calls.map((call: unknown[]) => call[0]);
      expect(logCalls.some((msg: string) => msg.includes('Created dosql.config.json'))).toBe(true);
      expect(logCalls.some((msg: string) => msg.includes('Created migrations/'))).toBe(true);
    });
  });
});

// =============================================================================
// 4. MIGRATE COMMAND
// =============================================================================

describe('Migrate Command', () => {
  let mockFs: ReturnType<typeof createMockFs>;

  beforeEach(() => {
    mockFs = createMockFs();
    setFileSystem(mockFs);
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'warn').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('migrate', () => {
    it('should return empty result when no migrations exist', async () => {
      mockFs.files.set('dosql.config.json', JSON.stringify({
        database: { type: 'sqlite', path: ':memory:' },
        migrations: { directory: 'migrations' },
      }));

      const result = await migrate();

      expect(result.applied).toHaveLength(0);
      expect(result.pending).toHaveLength(0);
    });

    it('should apply pending migrations', async () => {
      mockFs.files.set('dosql.config.json', JSON.stringify({
        database: { type: 'sqlite', path: ':memory:' },
        migrations: { directory: 'migrations' },
      }));
      mockFs.directories.add('migrations');
      mockFs.files.set('migrations/001_init.sql', 'CREATE TABLE users (id INTEGER, name TEXT)');

      const result = await migrate();

      expect(result.applied).toContain('001_init');
      expect(result.pending).toHaveLength(0);
    });

    it('should apply migrations in alphabetical order', async () => {
      mockFs.files.set('dosql.config.json', JSON.stringify({
        database: { type: 'sqlite', path: ':memory:' },
        migrations: { directory: 'migrations' },
      }));
      mockFs.directories.add('migrations');
      mockFs.files.set('migrations/001_first.sql', 'CREATE TABLE first (id INTEGER)');
      mockFs.files.set('migrations/002_second.sql', 'CREATE TABLE second (id INTEGER)');

      const result = await migrate();

      expect(result.applied.indexOf('001_first')).toBeLessThan(result.applied.indexOf('002_second'));
    });

    it('should respect --dry-run option', async () => {
      mockFs.files.set('dosql.config.json', JSON.stringify({
        database: { type: 'sqlite', path: ':memory:' },
        migrations: { directory: 'migrations' },
      }));
      mockFs.directories.add('migrations');
      mockFs.files.set('migrations/001_init.sql', 'CREATE TABLE users (id INTEGER)');

      const result = await migrate({ dryRun: true });

      expect(result.applied).toHaveLength(0);
      expect(result.pending).toContain('001_init');
    });

    it('should use custom config path', async () => {
      mockFs.files.set('/custom/config.json', JSON.stringify({
        database: { type: 'sqlite', path: ':memory:' },
        migrations: { directory: '/custom/migrations' },
      }));
      mockFs.directories.add('/custom/migrations');
      mockFs.files.set('/custom/migrations/001_init.sql', 'CREATE TABLE test (id INTEGER)');

      const result = await migrate({ configPath: '/custom/config.json' });

      expect(result.applied).toContain('001_init');
    });
  });
});

// =============================================================================
// 5. SHELL COMMAND
// =============================================================================

describe('Shell Command', () => {
  let mockFs: ReturnType<typeof createMockFs>;
  let logSpy: ReturnType<typeof vi.fn>;
  let originalLog: typeof console.log;

  beforeEach(() => {
    mockFs = createMockFs();
    setFileSystem(mockFs);
    logSpy = vi.fn();
    originalLog = console.log;
    console.log = logSpy;
  });

  afterEach(() => {
    console.log = originalLog;
    vi.restoreAllMocks();
  });

  describe('shell', () => {
    it('should be a function', () => {
      expect(typeof shell).toBe('function');
    });

    it('should accept config path option', async () => {
      mockFs.files.set('/custom/config.json', JSON.stringify({
        database: { type: 'sqlite', path: ':memory:' },
      }));

      // Shell should initialize and close immediately in test environment
      await shell({ configPath: '/custom/config.json' });

      const logCalls = logSpy.mock.calls.map((call: unknown[]) => call[0]);
      expect(logCalls.some((msg: string) => msg.includes('DoSQL Shell'))).toBe(true);
    });

    it('should display connection info', async () => {
      mockFs.files.set('dosql.config.json', JSON.stringify({
        database: { type: 'sqlite', path: 'test.db' },
      }));

      await shell();

      const logCalls = logSpy.mock.calls.map((call: unknown[]) => call[0]);
      expect(logCalls.some((msg: string) => msg.includes('Connected to'))).toBe(true);
    });
  });
});

// =============================================================================
// 6. QUERY COMMAND
// =============================================================================

describe('Query Command', () => {
  let mockFs: ReturnType<typeof createMockFs>;

  beforeEach(() => {
    mockFs = createMockFs();
    setFileSystem(mockFs);
    vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  describe('query', () => {
    it('should execute SQL and return results', async () => {
      mockFs.files.set('dosql.config.json', JSON.stringify({
        database: { type: 'sqlite', path: ':memory:' },
      }));

      const result = await query('CREATE TABLE test (id INTEGER)');

      expect(Array.isArray(result)).toBe(true);
    });

    it('should use custom config path', async () => {
      mockFs.files.set('/custom/config.json', JSON.stringify({
        database: { type: 'sqlite', path: ':memory:' },
      }));

      const result = await query('CREATE TABLE test (id INTEGER)', {
        configPath: '/custom/config.json',
      });

      expect(Array.isArray(result)).toBe(true);
    });

    it('should format as table when requested', async () => {
      mockFs.files.set('dosql.config.json', JSON.stringify({
        database: { type: 'sqlite', path: ':memory:' },
      }));

      const result = await query('CREATE TABLE test (id INTEGER)', { format: 'table' });

      expect(Array.isArray(result)).toBe(true);
    });

    it('should format as CSV when requested', async () => {
      mockFs.files.set('dosql.config.json', JSON.stringify({
        database: { type: 'sqlite', path: ':memory:' },
      }));

      const result = await query('CREATE TABLE test (id INTEGER)', { format: 'csv' });

      expect(Array.isArray(result)).toBe(true);
    });

    it('should throw error for DO database type', async () => {
      mockFs.files.set('dosql.config.json', JSON.stringify({
        database: { type: 'do' },
      }));

      await expect(query('SELECT 1')).rejects.toThrow('Durable Object connections not available');
    });

    it('should throw error for invalid SQL', async () => {
      mockFs.files.set('dosql.config.json', JSON.stringify({
        database: { type: 'sqlite', path: ':memory:' },
      }));

      await expect(query('INVALID SQL SYNTAX')).rejects.toThrow(/syntax|parse|error/i);
    });

    it('should throw error for missing table', async () => {
      mockFs.files.set('dosql.config.json', JSON.stringify({
        database: { type: 'sqlite', path: ':memory:' },
      }));

      await expect(query('SELECT * FROM nonexistent_table')).rejects.toThrow(
        /table.*not found|no such table/i
      );
    });
  });
});

// =============================================================================
// 7. MAIN ENTRY POINT
// =============================================================================

describe('Main Entry Point', () => {
  let mockFs: ReturnType<typeof createMockFs>;
  let logSpy: ReturnType<typeof vi.fn>;
  let errorSpy: ReturnType<typeof vi.fn>;
  let originalLog: typeof console.log;
  let originalError: typeof console.error;

  beforeEach(() => {
    mockFs = createMockFs();
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

  describe('main', () => {
    it('should display help text for --help', async () => {
      await main(['--help']);

      expect(logSpy).toHaveBeenCalledWith(HELP_TEXT);
    });

    it('should display version for --version', async () => {
      await main(['--version']);

      expect(logSpy).toHaveBeenCalledWith(`dosql v${VERSION}`);
    });

    it('should display help for empty args', async () => {
      await main([]);

      expect(logSpy).toHaveBeenCalledWith(HELP_TEXT);
    });

    it('should handle unknown command', async () => {
      await main(['unknown-command']);

      const errorCalls = errorSpy.mock.calls.map((call: unknown[]) => call[0]);
      expect(errorCalls.some((msg: string) => msg.includes('Unknown command'))).toBe(true);
    });

    it('should handle init command', async () => {
      await main(['init']);

      expect(mockFs.writeFileSync).toHaveBeenCalled();
    });

    it('should handle query command without SQL', async () => {
      mockFs.files.set('dosql.config.json', JSON.stringify({
        database: { type: 'sqlite', path: ':memory:' },
      }));

      await main(['query']);

      const errorCalls = errorSpy.mock.calls.map((call: unknown[]) => call[0]);
      expect(errorCalls.some((msg: string) => msg.includes('requires a SQL argument'))).toBe(true);
    });

    it('should handle errors gracefully', async () => {
      await main(['migrate']); // Will fail - no config

      const errorCalls = errorSpy.mock.calls.map((call: unknown[]) => call[0]);
      expect(errorCalls.some((msg: string) => msg.includes('Error:'))).toBe(true);
    });
  });
});

// =============================================================================
// 8. EXPORTS AND CONSTANTS
// =============================================================================

describe('Exports and Constants', () => {
  it('should export VERSION constant', () => {
    expect(VERSION).toBeDefined();
    expect(typeof VERSION).toBe('string');
    expect(VERSION).toMatch(/^\d+\.\d+\.\d+$/);
  });

  it('should export HELP_TEXT constant', () => {
    expect(HELP_TEXT).toBeDefined();
    expect(typeof HELP_TEXT).toBe('string');
    expect(HELP_TEXT).toContain('DoSQL CLI');
    expect(HELP_TEXT).toContain('init');
    expect(HELP_TEXT).toContain('migrate');
    expect(HELP_TEXT).toContain('shell');
    expect(HELP_TEXT).toContain('query');
  });

  it('should export DEFAULT_CONFIG_FILE constant', () => {
    expect(DEFAULT_CONFIG_FILE).toBe('dosql.config.json');
  });

  it('should export DEFAULT_MIGRATIONS_DIR constant', () => {
    expect(DEFAULT_MIGRATIONS_DIR).toBe('migrations');
  });

  it('should export all command functions', () => {
    expect(typeof init).toBe('function');
    expect(typeof migrate).toBe('function');
    expect(typeof shell).toBe('function');
    expect(typeof query).toBe('function');
    expect(typeof main).toBe('function');
  });

  it('should export utility functions', () => {
    expect(typeof loadConfig).toBe('function');
    expect(typeof parseArgs).toBe('function');
    expect(typeof setFileSystem).toBe('function');
  });
});
