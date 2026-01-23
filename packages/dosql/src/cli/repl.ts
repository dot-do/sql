/**
 * Bun CLI REPL - Interactive SQL Shell
 *
 * Provides a Bun-based CLI REPL for DoSQL that supports:
 * - Local mode with bun:sqlite
 * - Remote mode with HTTP/WebSocket connections
 * - Command parsing (.help, .quit, .tables, etc)
 * - SQL execution and result formatting
 * - Multi-line statement detection
 * - History management
 *
 * @module cli/repl
 */

// =============================================================================
// TYPES
// =============================================================================

/**
 * REPL configuration options
 */
export interface REPLConfig {
  mode: 'local' | 'http' | 'websocket';
  database?: string;
  url?: string;
  historyFile?: string;
  maxHistorySize?: number;
  multilineEnabled?: boolean;
  prompt?: string;
  multilinePrompt?: string;
  format?: 'table' | 'json' | 'csv' | 'vertical';
  onClose?: () => void;
  output?: (msg: string) => void;
}

/**
 * REPL command handler result
 */
export interface CommandResult {
  handled: boolean;
  output?: string;
  exit?: boolean;
  error?: Error;
}

/**
 * SQL execution result
 */
export interface ExecutionResult {
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
export interface HistoryEntry {
  input: string;
  timestamp: Date;
  successful: boolean;
}

/**
 * Format options for result formatting
 */
export interface FormatOptions {
  showTiming?: boolean;
  showHeaders?: boolean;
}

/**
 * Connection interface for database operations
 */
export interface Connection {
  execute(sql: string): Promise<ExecutionResult>;
  close(): Promise<void>;
  reconnect?(): Promise<void>;
}

/**
 * History manager configuration
 */
export interface HistoryConfig {
  maxSize: number;
  historyFile?: string;
  excludeDotCommands?: boolean;
  readFile?: (path: string) => Promise<string>;
  writeFile?: (path: string, content: string) => Promise<void>;
}

/**
 * Local connection config
 */
export interface LocalConnectionConfig {
  database: string;
}

/**
 * HTTP connection config
 */
export interface HTTPConnectionConfig {
  url: string;
  apiKey?: string;
  fetch?: typeof fetch;
  timeout?: number;
}

/**
 * WebSocket connection config
 */
export interface WebSocketConnectionConfig {
  url: string;
  apiKey?: string;
  connect?: (url: string) => Promise<WebSocketLike>;
  autoReconnect?: boolean;
}

/**
 * WebSocket-like interface for mocking
 */
interface WebSocketLike {
  readyState: number;
  send(data: string): void;
  close(): void;
  addEventListener(event: string, handler: (ev: unknown) => void): void;
  removeEventListener(event: string, handler: (ev: unknown) => void): void;
}

// =============================================================================
// REPL STATE
// =============================================================================

/**
 * REPL state for tracking settings
 */
interface REPLState {
  format: 'table' | 'json' | 'csv' | 'vertical';
  showHeaders: boolean;
  showTimer: boolean;
}

// Global state for command handlers (mutable by commands)
let replState: REPLState = {
  format: 'table',
  showHeaders: true,
  showTimer: false,
};

// =============================================================================
// COMMAND PARSING
// =============================================================================

/**
 * Built-in REPL commands
 */
const COMMANDS: Record<string, (args: string, state: REPLState) => CommandResult> = {
  '.help': () => ({
    handled: true,
    output: `
Available commands:
  .help              Show this help message
  .quit              Exit the REPL
  .exit              Exit the REPL (alias for .quit)
  .tables            List all tables
  .schema [table]    Show schema for all tables or a specific table
  .mode <format>     Set output format (table, json, csv, vertical)
  .headers <on|off>  Toggle column headers display
  .timer <on|off>    Toggle query timing display
  .databases         List attached databases
  .open <path>       Open a new database file
  .read <file>       Execute SQL from a file
  .connect <url>     Connect to a remote DoSQL endpoint
  .disconnect        Close the current connection
  .status            Show connection status
`.trim(),
  }),

  '.quit': () => ({
    handled: true,
    exit: true,
  }),

  '.exit': () => ({
    handled: true,
    exit: true,
  }),

  '.tables': () => ({
    handled: true,
    // Output will be provided when executed with a connection
  }),

  '.schema': (args: string) => ({
    handled: true,
    // args contains the optional table name
  }),

  '.mode': (args: string, state: REPLState) => {
    const format = args.trim().toLowerCase();
    const validFormats = ['table', 'json', 'csv', 'vertical'];

    if (!format) {
      return {
        handled: true,
        output: `Current mode: ${state.format}\nAvailable modes: ${validFormats.join(', ')}`,
      };
    }

    if (!validFormats.includes(format)) {
      return {
        handled: true,
        error: new Error(`Invalid format: ${format}. Valid formats are: ${validFormats.join(', ')}`),
      };
    }

    state.format = format as typeof state.format;
    return {
      handled: true,
      output: `Output mode set to: ${format}`,
    };
  },

  '.headers': (args: string, state: REPLState) => {
    const value = args.trim().toLowerCase();

    if (value === 'on') {
      state.showHeaders = true;
      return { handled: true, output: 'Headers enabled' };
    } else if (value === 'off') {
      state.showHeaders = false;
      return { handled: true, output: 'Headers disabled' };
    }

    return {
      handled: true,
      output: `Headers: ${state.showHeaders ? 'on' : 'off'}`,
    };
  },

  '.timer': (args: string, state: REPLState) => {
    const value = args.trim().toLowerCase();

    if (value === 'on') {
      state.showTimer = true;
      return { handled: true, output: 'Timer enabled' };
    } else if (value === 'off') {
      state.showTimer = false;
      return { handled: true, output: 'Timer disabled' };
    }

    return {
      handled: true,
      output: `Timer: ${state.showTimer ? 'on' : 'off'}`,
    };
  },

  '.databases': () => ({
    handled: true,
    // Output will be provided when executed with a connection
  }),

  '.open': (args: string) => {
    if (!args.trim()) {
      return {
        handled: true,
        error: new Error('.open requires a database path'),
      };
    }
    return {
      handled: true,
      // Will be handled by REPL to open new database
    };
  },

  '.read': (args: string) => {
    if (!args.trim()) {
      return {
        handled: true,
        error: new Error('.read requires a file path'),
      };
    }
    return {
      handled: true,
      // Will be handled by REPL to read and execute file
    };
  },

  '.connect': (args: string) => {
    if (!args.trim()) {
      return {
        handled: true,
        error: new Error('.connect requires a URL'),
      };
    }
    return {
      handled: true,
      // Will be handled by REPL to establish connection
    };
  },

  '.disconnect': () => ({
    handled: true,
    // Will be handled by REPL to close connection
  }),

  '.status': () => ({
    handled: true,
    // Will be handled by REPL to show connection status
  }),
};

/**
 * Parse a REPL command (dot commands)
 *
 * @param input - User input to parse
 * @returns Command result indicating if it was handled
 */
export function parseREPLCommand(input: string): CommandResult {
  const trimmed = input.trim();

  // Not a dot command - pass to SQL executor
  if (!trimmed.startsWith('.')) {
    return { handled: false };
  }

  // Extract command and arguments
  const spaceIndex = trimmed.indexOf(' ');
  const command = spaceIndex === -1 ? trimmed : trimmed.substring(0, spaceIndex);
  const args = spaceIndex === -1 ? '' : trimmed.substring(spaceIndex + 1);

  // Look up command handler
  const handler = COMMANDS[command.toLowerCase()];

  if (!handler) {
    return {
      handled: true,
      error: new Error(`Unknown command: ${command}. Type .help for available commands.`),
    };
  }

  return handler(args, replState);
}

// =============================================================================
// MULTI-LINE STATEMENT DETECTION
// =============================================================================

/**
 * Determine if input is an incomplete multi-line statement
 *
 * @param input - SQL input to check
 * @returns true if statement is incomplete and needs more input
 */
export function isMultilineStatement(input: string): boolean {
  const trimmed = input.trim();

  // Empty or whitespace-only input is complete
  if (!trimmed) {
    return false;
  }

  // Dot commands are always complete
  if (trimmed.startsWith('.')) {
    return false;
  }

  // Check for unterminated string literals and ignore semicolons inside them
  let inSingleQuote = false;
  let inDoubleQuote = false;
  let inLineComment = false;
  let inBlockComment = false;
  let lastChar = '';
  let lastSemicolonIndex = -1;

  for (let i = 0; i < trimmed.length; i++) {
    const char = trimmed[i];
    const nextChar = trimmed[i + 1] || '';

    // Handle line comments
    if (!inSingleQuote && !inDoubleQuote && !inBlockComment && char === '-' && nextChar === '-') {
      inLineComment = true;
      i++; // Skip next char
      continue;
    }

    // End of line comment
    if (inLineComment && char === '\n') {
      inLineComment = false;
      continue;
    }

    // Skip characters in line comments
    if (inLineComment) {
      continue;
    }

    // Handle block comments
    if (!inSingleQuote && !inDoubleQuote && !inBlockComment && char === '/' && nextChar === '*') {
      inBlockComment = true;
      i++; // Skip next char
      continue;
    }

    // End of block comment
    if (inBlockComment && char === '*' && nextChar === '/') {
      inBlockComment = false;
      i++; // Skip next char
      continue;
    }

    // Skip characters in block comments
    if (inBlockComment) {
      continue;
    }

    // Handle single quotes (with escaped quotes '')
    if (!inDoubleQuote && char === "'") {
      if (inSingleQuote && nextChar === "'") {
        // Escaped quote
        i++; // Skip next char
        continue;
      }
      inSingleQuote = !inSingleQuote;
      continue;
    }

    // Handle double quotes (for identifiers)
    if (!inSingleQuote && char === '"') {
      if (inDoubleQuote && nextChar === '"') {
        // Escaped quote
        i++;
        continue;
      }
      inDoubleQuote = !inDoubleQuote;
      continue;
    }

    // Track semicolons outside of strings/comments
    if (!inSingleQuote && !inDoubleQuote && char === ';') {
      lastSemicolonIndex = i;
    }

    lastChar = char;
  }

  // If we're still inside a string or comment, statement is incomplete
  if (inSingleQuote || inDoubleQuote || inBlockComment) {
    return true;
  }

  // Check if statement ends with semicolon (ignoring trailing whitespace)
  const afterLastSemicolon = lastSemicolonIndex >= 0
    ? trimmed.substring(lastSemicolonIndex + 1).trim()
    : trimmed;

  // If there's no semicolon or there's non-whitespace after the last one
  // (except for comments), it's incomplete
  if (lastSemicolonIndex === -1) {
    return true;
  }

  // Check what's after the last semicolon
  const remainder = trimmed.substring(lastSemicolonIndex + 1).trim();

  // If remainder is empty or only comments, statement is complete
  if (!remainder) {
    return false;
  }

  // Check if remainder is only a comment
  if (remainder.startsWith('--')) {
    return false;
  }

  // There's content after the semicolon - could be another statement
  // For REPL purposes, we consider it complete
  return false;
}

// =============================================================================
// RESULT FORMATTING
// =============================================================================

/**
 * Format a value for display
 */
function formatValue(value: unknown): string {
  if (value === null) {
    return 'NULL';
  }
  if (value === undefined) {
    return 'NULL';
  }
  if (value instanceof Uint8Array || ArrayBuffer.isView(value)) {
    // Format as hex for blobs
    const bytes = value instanceof Uint8Array ? value : new Uint8Array(value.buffer);
    const hex = Array.from(bytes).map(b => b.toString(16).padStart(2, '0').toUpperCase()).join('');
    return hex || '<blob>';
  }
  if (typeof value === 'bigint') {
    return value.toString();
  }
  return String(value);
}

/**
 * Format execution results for display
 *
 * @param result - Execution result to format
 * @param format - Output format (table, json, csv, vertical)
 * @param options - Additional formatting options
 * @returns Formatted string output
 */
export function formatResults(
  result: ExecutionResult,
  format: 'table' | 'json' | 'csv' | 'vertical',
  options?: FormatOptions
): string {
  const { rows, columns, rowCount, changes, lastInsertRowid, duration } = result;
  const lines: string[] = [];

  // Handle DML results (INSERT/UPDATE/DELETE)
  if (changes !== undefined && rows.length === 0) {
    if (changes > 0) {
      lines.push(`${changes} row(s) changed`);
    } else {
      lines.push('0 rows changed');
    }
    if (lastInsertRowid !== undefined) {
      lines.push(`Last insert rowid: ${lastInsertRowid}`);
    }
    if (options?.showTiming) {
      lines.push(`Time: ${duration}ms`);
    }
    return lines.join('\n');
  }

  // Handle empty results
  if (rows.length === 0) {
    if (options?.showTiming) {
      return `No rows returned\nTime: ${duration}ms`;
    }
    return 'No rows returned';
  }

  // Format based on output mode
  let output: string;
  switch (format) {
    case 'json':
      output = JSON.stringify(rows, (_, v) => (typeof v === 'bigint' ? v.toString() : v), 2);
      break;

    case 'csv':
      output = formatCSV(rows, columns);
      break;

    case 'vertical':
      output = formatVertical(rows, columns, options);
      break;

    case 'table':
    default:
      output = formatTable(rows, columns, options);
      break;
  }

  // Add timing info if requested
  if (options?.showTiming) {
    output += `\n${rows.length} row(s) in set (${duration}ms)`;
  }

  return output;
}

/**
 * Format results as CSV
 */
function formatCSV(rows: Record<string, unknown>[], columns: string[]): string {
  const lines: string[] = [];

  // Header row
  lines.push(columns.join(','));

  // Data rows
  for (const row of rows) {
    const values = columns.map(col => {
      const value = row[col];
      if (value === null || value === undefined) {
        return '';
      }
      const str = formatValue(value);
      // Escape if contains comma, quote, or newline
      if (str.includes(',') || str.includes('"') || str.includes('\n')) {
        return `"${str.replace(/"/g, '""')}"`;
      }
      return str;
    });
    lines.push(values.join(','));
  }

  return lines.join('\n');
}

/**
 * Format results as vertical (one column per line)
 */
function formatVertical(
  rows: Record<string, unknown>[],
  columns: string[],
  options?: FormatOptions
): string {
  const lines: string[] = [];
  const maxColWidth = Math.max(...columns.map(c => c.length));

  for (let i = 0; i < rows.length; i++) {
    if (i > 0) {
      lines.push(''); // Blank line between rows
    }
    lines.push(`*************************** ${i + 1}. row ***************************`);

    const row = rows[i];
    for (const col of columns) {
      const paddedCol = col.padStart(maxColWidth);
      const value = formatValue(row[col]);
      lines.push(`${paddedCol}: ${value}`);
    }
  }

  if (options?.showTiming) {
    lines.push('');
    lines.push(`${rows.length} row(s) in set`);
  }

  return lines.join('\n');
}

/**
 * Format results as ASCII table
 */
function formatTable(
  rows: Record<string, unknown>[],
  columns: string[],
  options?: FormatOptions
): string {
  // Calculate column widths
  const widths: number[] = columns.map(col => col.length);

  for (const row of rows) {
    columns.forEach((col, i) => {
      const value = formatValue(row[col]);
      widths[i] = Math.max(widths[i], value.length);
    });
  }

  // Build table
  const lines: string[] = [];

  // Top border
  const borderLine = '+' + widths.map(w => '-'.repeat(w + 2)).join('+') + '+';
  lines.push(borderLine);

  // Header row
  const headerRow = '|' + columns.map((col, i) => ` ${col.padEnd(widths[i])} `).join('|') + '|';
  lines.push(headerRow);
  lines.push(borderLine);

  // Data rows
  for (const row of rows) {
    const dataRow = '|' + columns.map((col, i) => {
      const value = formatValue(row[col]);
      return ` ${value.padEnd(widths[i])} `;
    }).join('|') + '|';
    lines.push(dataRow);
  }

  // Bottom border
  lines.push(borderLine);

  // Row count and timing - note: timing info not available in formatTable, use formatResults wrapper
  // The timing is shown at the formatResults level, not here

  return lines.join('\n');
}

// =============================================================================
// HISTORY MANAGEMENT
// =============================================================================

/**
 * History manager for tracking command history
 */
export class HistoryManager {
  private entries: HistoryEntry[] = [];
  private maxSize: number;
  private position: number = -1;
  private historyFile?: string;
  private excludeDotCommands: boolean;
  private readFileFn?: (path: string) => Promise<string>;
  private writeFileFn?: (path: string, content: string) => Promise<void>;

  constructor(config: HistoryConfig) {
    this.maxSize = config.maxSize;
    this.historyFile = config.historyFile;
    this.excludeDotCommands = config.excludeDotCommands ?? false;
    this.readFileFn = config.readFile;
    this.writeFileFn = config.writeFile;
  }

  /**
   * Add an entry to history
   */
  add(input: string, successful: boolean = true): void {
    const trimmed = input.trim();

    // Don't add empty entries
    if (!trimmed) {
      return;
    }

    // Optionally exclude dot commands
    if (this.excludeDotCommands && trimmed.startsWith('.')) {
      return;
    }

    // Don't add duplicate consecutive entries
    if (this.entries.length > 0 && this.entries[this.entries.length - 1].input === trimmed) {
      return;
    }

    // Add entry
    this.entries.push({
      input: trimmed,
      timestamp: new Date(),
      successful,
    });

    // Enforce max size
    while (this.entries.length > this.maxSize) {
      this.entries.shift();
    }

    // Reset navigation position
    this.position = -1;
  }

  /**
   * Get the number of entries
   */
  size(): number {
    return this.entries.length;
  }

  /**
   * Get all entries
   */
  getAll(): HistoryEntry[] {
    return [...this.entries];
  }

  /**
   * Navigate to previous entry (up arrow)
   */
  previous(): string {
    if (this.entries.length === 0) {
      return '';
    }

    if (this.position === -1) {
      // Start from most recent
      this.position = this.entries.length - 1;
    } else if (this.position > 0) {
      this.position--;
    }
    // Stay at oldest if already there

    return this.entries[this.position].input;
  }

  /**
   * Navigate to next entry (down arrow)
   */
  next(): string {
    if (this.position === -1) {
      return '';
    }

    if (this.position < this.entries.length - 1) {
      this.position++;
      return this.entries[this.position].input;
    }

    // Back to empty prompt
    this.position = -1;
    return '';
  }

  /**
   * Search history for entries matching pattern
   */
  search(pattern: string): HistoryEntry[] {
    const lower = pattern.toLowerCase();
    return this.entries.filter(e => e.input.toLowerCase().includes(lower));
  }

  /**
   * Clear all history
   */
  clear(): void {
    this.entries = [];
    this.position = -1;
  }

  /**
   * Save history to file
   */
  async save(): Promise<void> {
    if (!this.historyFile || !this.writeFileFn) {
      return;
    }

    const content = this.entries.map(e => e.input).join('\n');
    const path = this.historyFile.replace(/^~/, process.env.HOME || '');
    await this.writeFileFn(path, content);
  }

  /**
   * Load history from file
   */
  async load(): Promise<void> {
    if (!this.historyFile || !this.readFileFn) {
      return;
    }

    try {
      const path = this.historyFile.replace(/^~/, process.env.HOME || '');
      const content = await this.readFileFn(path);
      const lines = content.split('\n').filter(line => line.trim());

      for (const line of lines) {
        this.entries.push({
          input: line,
          timestamp: new Date(),
          successful: true,
        });
      }

      // Enforce max size
      while (this.entries.length > this.maxSize) {
        this.entries.shift();
      }
    } catch {
      // Ignore errors (file may not exist)
    }
  }
}

// =============================================================================
// LOCAL CONNECTION (bun:sqlite)
// =============================================================================

/**
 * Create a local SQLite connection using bun:sqlite
 *
 * @param config - Connection configuration
 * @returns Connection interface
 */
export async function createLocalConnection(config: LocalConnectionConfig): Promise<Connection> {
  // Dynamic import for bun:sqlite
  // In Bun, this is a built-in module
  let Database: new (path: string) => BunSQLiteDatabase;

  try {
    // Try to import bun:sqlite
    const bunSqlite = await import('bun:sqlite');
    Database = bunSqlite.Database;
  } catch {
    // Fallback to better-sqlite3 for Node.js compatibility
    const betterSqlite3 = await import('better-sqlite3');
    Database = betterSqlite3.default as unknown as new (path: string) => BunSQLiteDatabase;
  }

  interface BunSQLiteDatabase {
    query(sql: string): { all(): unknown[]; run(): { changes: number; lastInsertRowid: number | bigint } };
    close(): void;
    exec?(sql: string): void;
  }

  let db: BunSQLiteDatabase;

  try {
    db = new Database(config.database);
  } catch (error) {
    throw new Error(`Cannot open database: ${config.database}. ${error instanceof Error ? error.message : 'Unknown error'}`);
  }

  return {
    async execute(sql: string): Promise<ExecutionResult> {
      const start = performance.now();

      try {
        const query = db.query(sql);

        // Determine if it's a SELECT or DML statement
        const trimmed = sql.trim().toUpperCase();
        const isSelect = trimmed.startsWith('SELECT') || trimmed.startsWith('PRAGMA') || trimmed.startsWith('EXPLAIN');

        if (isSelect) {
          const rows = query.all() as Record<string, unknown>[];
          const columns = rows.length > 0 ? Object.keys(rows[0]) : [];

          return {
            rows,
            columns,
            rowCount: rows.length,
            duration: performance.now() - start,
          };
        } else {
          const result = query.run();
          return {
            rows: [],
            columns: [],
            rowCount: 0,
            changes: result.changes,
            lastInsertRowid: result.lastInsertRowid,
            duration: performance.now() - start,
          };
        }
      } catch (error) {
        throw error;
      }
    },

    async close(): Promise<void> {
      db.close();
    },
  };
}

// =============================================================================
// HTTP CONNECTION
// =============================================================================

/**
 * Create an HTTP connection to a DoSQL endpoint
 *
 * @param config - Connection configuration
 * @returns Connection interface
 */
export async function createHTTPConnection(config: HTTPConnectionConfig): Promise<Connection> {
  const fetchFn = config.fetch ?? fetch;
  const timeout = config.timeout ?? 30000;

  return {
    async execute(sql: string): Promise<ExecutionResult> {
      const start = performance.now();

      try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), timeout);

        const response = await fetchFn(`${config.url}/query`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            ...(config.apiKey ? { Authorization: `Bearer ${config.apiKey}` } : {}),
          },
          body: JSON.stringify({ sql }),
          signal: controller.signal,
        });

        clearTimeout(timeoutId);

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const data = await response.json() as {
          rows: Record<string, unknown>[];
          columns: string[];
          changes?: number;
          lastInsertRowid?: number | bigint;
        };

        return {
          rows: data.rows || [],
          columns: data.columns || [],
          rowCount: data.rows?.length || 0,
          changes: data.changes,
          lastInsertRowid: data.lastInsertRowid,
          duration: performance.now() - start,
        };
      } catch (error) {
        if (error instanceof Error) {
          if (error.name === 'AbortError') {
            throw new Error('Request timeout');
          }
          if (error.message.includes('fetch') || error.message.includes('network') || error.message.includes('Network')) {
            throw new Error(`Network error: ${error.message}`);
          }
        }
        throw error;
      }
    },

    async close(): Promise<void> {
      // HTTP connections are stateless, nothing to close
    },
  };
}

// =============================================================================
// WEBSOCKET CONNECTION
// =============================================================================

/**
 * Create a WebSocket connection to a DoSQL endpoint
 *
 * @param config - Connection configuration
 * @returns Connection interface
 */
export async function createWebSocketConnection(config: WebSocketConnectionConfig): Promise<Connection & { reconnect(): Promise<void> }> {
  let ws: WebSocketLike | null = null;
  let requestId = 0;
  const pendingRequests = new Map<number, {
    resolve: (value: ExecutionResult) => void;
    reject: (error: Error) => void;
    start: number;
  }>();
  const messageQueue: string[] = [];
  let isReconnecting = false;

  const connect = async (): Promise<void> => {
    if (config.connect) {
      ws = await config.connect(config.url);
    } else {
      ws = new WebSocket(config.url) as unknown as WebSocketLike;
    }

    ws.addEventListener('message', (event: { data: string }) => {
      try {
        const response = JSON.parse(event.data) as {
          jsonrpc: string;
          id: number;
          result?: { rows: Record<string, unknown>[]; columns: string[] };
          error?: { message: string };
        };

        const pending = pendingRequests.get(response.id);
        if (pending) {
          pendingRequests.delete(response.id);

          if (response.error) {
            pending.reject(new Error(response.error.message));
          } else {
            const result = response.result || { rows: [], columns: [] };
            pending.resolve({
              rows: result.rows || [],
              columns: result.columns || [],
              rowCount: result.rows?.length || 0,
              duration: performance.now() - pending.start,
            });
          }
        }
      } catch {
        // Ignore parse errors
      }
    });

    ws.addEventListener('close', () => {
      // Reject all pending requests
      for (const [id, pending] of pendingRequests) {
        pending.reject(new Error('WebSocket connection closed'));
        pendingRequests.delete(id);
      }
    });

    // Send queued messages
    while (messageQueue.length > 0) {
      const msg = messageQueue.shift()!;
      ws.send(msg);
    }
  };

  await connect();

  return {
    async execute(sql: string): Promise<ExecutionResult> {
      if (!ws || ws.readyState !== 1) {
        throw new Error('WebSocket connection closed');
      }

      const id = ++requestId;
      const start = performance.now();

      const request = JSON.stringify({
        jsonrpc: '2.0',
        id,
        method: 'query',
        params: { sql },
      });

      return new Promise((resolve, reject) => {
        pendingRequests.set(id, { resolve, reject, start });

        if (isReconnecting) {
          messageQueue.push(request);
        } else {
          ws!.send(request);
        }
      });
    },

    async close(): Promise<void> {
      if (ws) {
        ws.close();
        ws = null;
      }
    },

    async reconnect(): Promise<void> {
      isReconnecting = true;

      if (ws) {
        ws.close();
        ws = null;
      }

      await connect();
      isReconnecting = false;
    },
  };
}

// =============================================================================
// BUN REPL CLASS
// =============================================================================

/**
 * Bun-based CLI REPL for DoSQL
 */
export class BunREPL {
  private config: REPLConfig;
  private connection: Connection | null = null;
  private history: HistoryManager;
  private multilineBuffer: string = '';
  private outputFn: (msg: string) => void;
  private _interrupted: boolean = false;
  private _currentPromise: Promise<string> | null = null;

  /** Current connection mode */
  readonly connectionMode: 'local' | 'http' | 'websocket';

  /** Current prompt string */
  prompt: string;

  /** Multi-line prompt string */
  multilinePrompt: string;

  /** Current output format */
  format: 'table' | 'json' | 'csv' | 'vertical';

  /** Get the current prompt (changes for multi-line input) */
  get currentPrompt(): string {
    return this.multilineBuffer ? this.multilinePrompt : this.prompt;
  }

  constructor(config: REPLConfig) {
    this.config = config;
    this.connectionMode = config.mode;
    this.prompt = config.prompt ?? 'dosql> ';
    this.multilinePrompt = config.multilinePrompt ?? '   ...> ';
    this.format = config.format ?? 'table';
    this.outputFn = config.output ?? console.log;

    // Initialize history
    this.history = new HistoryManager({
      maxSize: config.maxHistorySize ?? 1000,
      historyFile: config.historyFile,
    });

    // Update global state
    replState.format = this.format;
  }

  /**
   * Start the REPL (shows welcome message)
   */
  async start(): Promise<void> {
    // Connect to database
    await this.connect();

    // Show welcome message
    this.outputFn('DoSQL CLI REPL');
    this.outputFn(`Mode: ${this.connectionMode}`);
    if (this.config.database) {
      this.outputFn(`Database: ${this.config.database}`);
    }
    if (this.config.url) {
      this.outputFn(`URL: ${this.config.url}`);
    }
    this.outputFn('Type .help for available commands');
    this.outputFn('');
  }

  /**
   * Connect to the database based on mode
   */
  private async connect(): Promise<void> {
    switch (this.connectionMode) {
      case 'local':
        this.connection = await createLocalConnection({
          database: this.config.database ?? ':memory:',
        });
        break;

      case 'http':
        if (!this.config.url) {
          throw new Error('HTTP mode requires a URL');
        }
        this.connection = await createHTTPConnection({
          url: this.config.url,
        });
        break;

      case 'websocket':
        if (!this.config.url) {
          throw new Error('WebSocket mode requires a URL');
        }
        this.connection = await createWebSocketConnection({
          url: this.config.url,
        });
        break;
    }
  }

  /**
   * Process user input
   *
   * @param input - User input line
   * @returns Output to display
   */
  async processInput(input: string): Promise<string> {
    this._interrupted = false;

    // Add to multi-line buffer if we're accumulating
    const fullInput = this.multilineBuffer ? this.multilineBuffer + '\n' + input : input;

    // Check if it's a dot command (only process if not in multi-line mode)
    if (!this.multilineBuffer && input.trim().startsWith('.')) {
      const result = parseREPLCommand(input);

      if (result.handled) {
        if (result.exit) {
          return 'Goodbye!';
        }
        if (result.error) {
          return `Error: ${result.error.message}`;
        }

        // Handle special commands that need connection
        const cmd = input.trim().split(' ')[0].toLowerCase();

        if (cmd === '.tables' && this.connection) {
          try {
            const tablesResult = await this.connection.execute(
              "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            );
            return tablesResult.rows.map(r => r.name as string).join('\n') || 'No tables found';
          } catch (err) {
            return `Error: ${err instanceof Error ? err.message : 'Unknown error'}`;
          }
        }

        if (cmd === '.schema' && this.connection) {
          const tableName = input.trim().split(' ')[1];
          try {
            let query = "SELECT sql FROM sqlite_master WHERE type='table'";
            if (tableName) {
              query += ` AND name='${tableName}'`;
            }
            query += ' ORDER BY name';
            const schemaResult = await this.connection.execute(query);
            return schemaResult.rows.map(r => r.sql as string).join('\n\n') || 'No schema found';
          } catch (err) {
            return `Error: ${err instanceof Error ? err.message : 'Unknown error'}`;
          }
        }

        if (cmd === '.databases' && this.connection) {
          try {
            const dbResult = await this.connection.execute('PRAGMA database_list');
            return formatResults(dbResult, 'table');
          } catch (err) {
            return `Error: ${err instanceof Error ? err.message : 'Unknown error'}`;
          }
        }

        // Update format if changed
        if (cmd === '.mode') {
          this.format = replState.format;
        }

        return result.output ?? '';
      }
    }

    // Check if statement is complete
    if (isMultilineStatement(fullInput)) {
      this.multilineBuffer = fullInput;
      return ''; // Wait for more input
    }

    // Statement is complete - execute it
    this.multilineBuffer = '';
    const sql = fullInput.trim();

    if (!sql) {
      return '';
    }

    // Add to history
    this.history.add(sql);

    // Execute SQL
    if (!this.connection) {
      return 'Error: Not connected to database';
    }

    try {
      const promise = this.connection.execute(sql);
      this._currentPromise = promise.then(r => formatResults(r, this.format, { showTiming: replState.showTimer }));

      const result = await promise;

      if (this._interrupted) {
        throw new Error('Query cancelled');
      }

      return formatResults(result, this.format, { showTiming: replState.showTimer });
    } catch (err) {
      return `Error: ${err instanceof Error ? err.message : 'Unknown error'}`;
    }
  }

  /**
   * Interrupt the current operation
   */
  interrupt(): void {
    this._interrupted = true;
  }

  /**
   * Close the REPL and cleanup
   */
  async close(): Promise<void> {
    if (this.connection) {
      await this.connection.close();
      this.connection = null;
    }

    if (this.config.onClose) {
      this.config.onClose();
    }
  }
}

// =============================================================================
// EXPORTS
// =============================================================================

export type {
  WebSocketLike,
};
