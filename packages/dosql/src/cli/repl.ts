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
 * - Tab completion for SQL keywords, table names, and column names
 * - Syntax highlighting for SQL
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
  /** Inject a connection (useful for testing without bun:sqlite) */
  connection?: Connection;
  /** Enable syntax highlighting (default: true if terminal supports colors) */
  highlightEnabled?: boolean;
  /** Enable tab completion (default: true) */
  completionEnabled?: boolean;
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
  changes?: number | undefined;
  lastInsertRowid?: number | bigint | undefined;
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
 * WebSocket event map for type-safe event handling
 */
interface WebSocketEventMap {
  open: Event;
  close: CloseEvent;
  error: Event;
  message: MessageEvent;
}

/**
 * WebSocket-like interface for mocking with type-safe event handlers
 */
interface WebSocketLike {
  readyState: number;
  send(data: string): void;
  close(): void;
  addEventListener<K extends keyof WebSocketEventMap>(
    event: K,
    handler: (ev: WebSocketEventMap[K]) => void
  ): void;
  removeEventListener<K extends keyof WebSocketEventMap>(
    event: K,
    handler: (ev: WebSocketEventMap[K]) => void
  ): void;
}

// =============================================================================
// SQL KEYWORDS FOR COMPLETION
// =============================================================================

/**
 * SQL keywords recognized for tab completion (sorted alphabetically for efficient searching)
 */
export const SQL_KEYWORDS: string[] = [
  // DML
  'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'BETWEEN', 'LIKE', 'ILIKE',
  'IS', 'NULL', 'TRUE', 'FALSE', 'AS', 'ON', 'JOIN', 'INNER', 'LEFT', 'RIGHT',
  'FULL', 'OUTER', 'CROSS', 'GROUP', 'BY', 'HAVING', 'ORDER', 'ASC', 'DESC',
  'LIMIT', 'OFFSET', 'DISTINCT', 'ALL', 'UNION', 'INTERSECT', 'EXCEPT',
  'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'NULLS', 'FIRST', 'LAST',
  'EXISTS', 'ANY', 'SOME', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
  'WITH', 'RECURSIVE', 'COALESCE', 'NULLIF', 'IIF', 'IF',
  'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE', 'REPLACE',
  'RETURNING', 'CONFLICT', 'DO', 'NOTHING',

  // DDL
  'CREATE', 'TABLE', 'INDEX', 'VIEW', 'DROP', 'ALTER', 'ADD', 'COLUMN', 'RENAME',
  'TO', 'TEMPORARY', 'TEMP', 'UNIQUE', 'PRIMARY', 'KEY', 'FOREIGN',
  'REFERENCES', 'CASCADE', 'RESTRICT', 'DEFAULT', 'CHECK', 'CONSTRAINT',
  'AUTOINCREMENT', 'COLLATE', 'WITHOUT', 'ROWID', 'STRICT', 'GENERATED',
  'ALWAYS', 'STORED', 'VIRTUAL', 'NO', 'ACTION', 'ABORT', 'FAIL', 'IGNORE',
  'ROLLBACK', 'MATCH', 'SIMPLE', 'PARTIAL', 'DEFERRABLE', 'INITIALLY',
  'DEFERRED', 'IMMEDIATE',

  // Data types
  'INTEGER', 'INT', 'SMALLINT', 'MEDIUMINT', 'BIGINT', 'TINYINT',
  'REAL', 'DOUBLE', 'PRECISION', 'FLOAT', 'NUMERIC', 'DECIMAL',
  'TEXT', 'VARCHAR', 'CHAR', 'NCHAR', 'NVARCHAR', 'CLOB',
  'BLOB', 'NONE', 'DATE', 'DATETIME', 'TIMESTAMP', 'TIME',
  'BOOLEAN', 'BOOL', 'JSON', 'JSONB', 'UUID',

  // Window functions
  'OVER', 'PARTITION', 'ROWS', 'RANGE', 'GROUPS', 'UNBOUNDED', 'PRECEDING',
  'FOLLOWING', 'CURRENT', 'ROW', 'EXCLUDE', 'TIES', 'OTHERS', 'WINDOW',
  'FILTER', 'WITHIN', 'RESPECT', 'NTILE', 'LAG', 'LEAD', 'FIRST_VALUE',
  'LAST_VALUE', 'NTH_VALUE', 'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'PERCENT_RANK',
  'CUME_DIST',

  // Transaction
  'BEGIN', 'COMMIT', 'TRANSACTION', 'SAVEPOINT', 'RELEASE',

  // Other
  'PRAGMA', 'EXPLAIN', 'QUERY', 'PLAN', 'ANALYZE', 'ATTACH', 'DETACH', 'VACUUM',
  'REINDEX', 'GLOB', 'CAST', 'TYPEOF', 'LENGTH', 'SUBSTR', 'UPPER', 'LOWER',
  'TRIM', 'LTRIM', 'RTRIM', 'REPLACE', 'INSTR', 'PRINTF', 'HEX', 'UNHEX',
  'ABS', 'ROUND', 'RANDOM', 'RANDOMBLOB', 'ZEROBLOB', 'TOTAL', 'GROUP_CONCAT',
].sort();

/**
 * SQL keyword set for fast lookup (lowercase)
 */
const SQL_KEYWORD_SET = new Set(SQL_KEYWORDS.map(k => k.toLowerCase()));

/**
 * REPL dot commands for completion
 */
export const DOT_COMMANDS: string[] = [
  '.help', '.quit', '.exit', '.tables', '.schema', '.mode', '.headers',
  '.timer', '.databases', '.open', '.read', '.connect', '.disconnect', '.status',
].sort();

// =============================================================================
// SYNTAX HIGHLIGHTING
// =============================================================================

/**
 * ANSI color codes for syntax highlighting
 */
export const COLORS = {
  reset: '\x1b[0m',
  // Keywords
  keyword: '\x1b[1;34m',       // Bold blue
  // Strings
  string: '\x1b[32m',          // Green
  // Numbers
  number: '\x1b[33m',          // Yellow
  // Comments
  comment: '\x1b[2;37m',       // Dim white
  // Operators
  operator: '\x1b[36m',        // Cyan
  // Functions
  function: '\x1b[35m',        // Magenta
  // Identifiers
  identifier: '\x1b[37m',      // White
  // Punctuation
  punctuation: '\x1b[37m',     // White
  // Error
  error: '\x1b[1;31m',         // Bold red
  // Table/column names (when known)
  table: '\x1b[1;33m',         // Bold yellow
  column: '\x1b[33m',          // Yellow
} as const;

/**
 * SQL function names for highlighting
 */
const SQL_FUNCTIONS = new Set([
  'count', 'sum', 'avg', 'min', 'max', 'total', 'group_concat',
  'abs', 'round', 'random', 'randomblob', 'zeroblob',
  'length', 'substr', 'upper', 'lower', 'trim', 'ltrim', 'rtrim',
  'replace', 'instr', 'printf', 'hex', 'unhex',
  'coalesce', 'nullif', 'iif', 'ifnull', 'typeof', 'cast',
  'date', 'time', 'datetime', 'julianday', 'strftime',
  'row_number', 'rank', 'dense_rank', 'percent_rank', 'cume_dist',
  'ntile', 'lag', 'lead', 'first_value', 'last_value', 'nth_value',
  'json', 'json_array', 'json_object', 'json_extract', 'json_type',
  'json_valid', 'json_quote', 'json_group_array', 'json_group_object',
]);

/**
 * Token types for syntax highlighting
 */
type HighlightTokenType = 'keyword' | 'string' | 'number' | 'comment' | 'operator' | 'function' | 'identifier' | 'punctuation' | 'whitespace';

/**
 * Token for syntax highlighting
 */
interface HighlightToken {
  type: HighlightTokenType;
  value: string;
}

/**
 * Tokenize SQL for syntax highlighting
 *
 * @param sql - SQL string to tokenize
 * @returns Array of tokens with types
 */
export function tokenizeForHighlight(sql: string): HighlightToken[] {
  const tokens: HighlightToken[] = [];
  let pos = 0;

  while (pos < sql.length) {
    const char = sql[pos];
    const remaining = sql.slice(pos);

    // Whitespace
    if (/\s/.test(char)) {
      let value = '';
      while (pos < sql.length && /\s/.test(sql[pos])) {
        value += sql[pos++];
      }
      tokens.push({ type: 'whitespace', value });
      continue;
    }

    // Single-line comment
    if (char === '-' && sql[pos + 1] === '-') {
      let value = '--';
      pos += 2;
      while (pos < sql.length && sql[pos] !== '\n') {
        value += sql[pos++];
      }
      tokens.push({ type: 'comment', value });
      continue;
    }

    // Multi-line comment
    if (char === '/' && sql[pos + 1] === '*') {
      let value = '/*';
      pos += 2;
      while (pos < sql.length - 1 && !(sql[pos] === '*' && sql[pos + 1] === '/')) {
        value += sql[pos++];
      }
      if (pos < sql.length - 1) {
        value += '*/';
        pos += 2;
      }
      tokens.push({ type: 'comment', value });
      continue;
    }

    // String literal (single quotes)
    if (char === "'") {
      let value = "'";
      pos++;
      while (pos < sql.length) {
        if (sql[pos] === "'") {
          value += "'";
          pos++;
          if (sql[pos] === "'") {
            // Escaped quote
            value += "'";
            pos++;
          } else {
            break;
          }
        } else {
          value += sql[pos++];
        }
      }
      tokens.push({ type: 'string', value });
      continue;
    }

    // Number
    if (/[0-9]/.test(char) || (char === '.' && /[0-9]/.test(sql[pos + 1] || ''))) {
      let value = '';
      // Integer part
      while (pos < sql.length && /[0-9]/.test(sql[pos])) {
        value += sql[pos++];
      }
      // Decimal part
      if (sql[pos] === '.' && /[0-9]/.test(sql[pos + 1] || '')) {
        value += sql[pos++];
        while (pos < sql.length && /[0-9]/.test(sql[pos])) {
          value += sql[pos++];
        }
      }
      // Scientific notation
      if ((sql[pos] || '').toLowerCase() === 'e') {
        value += sql[pos++];
        if (sql[pos] === '+' || sql[pos] === '-') {
          value += sql[pos++];
        }
        while (pos < sql.length && /[0-9]/.test(sql[pos])) {
          value += sql[pos++];
        }
      }
      tokens.push({ type: 'number', value });
      continue;
    }

    // Identifier or keyword
    if (/[a-zA-Z_]/.test(char)) {
      let value = '';
      while (pos < sql.length && /[a-zA-Z0-9_]/.test(sql[pos])) {
        value += sql[pos++];
      }
      const lower = value.toLowerCase();

      // Check if followed by '(' to determine if it's a function call
      // Skip whitespace to find the next non-whitespace character
      let lookAhead = pos;
      while (lookAhead < sql.length && /\s/.test(sql[lookAhead])) {
        lookAhead++;
      }
      const isFollowedByParen = sql[lookAhead] === '(';

      // If followed by '(' and is a known function, treat as function
      if (isFollowedByParen && SQL_FUNCTIONS.has(lower)) {
        tokens.push({ type: 'function', value });
      } else if (SQL_KEYWORD_SET.has(lower)) {
        tokens.push({ type: 'keyword', value });
      } else if (SQL_FUNCTIONS.has(lower)) {
        // Function name not followed by ( - still highlight as function
        tokens.push({ type: 'function', value });
      } else {
        tokens.push({ type: 'identifier', value });
      }
      continue;
    }

    // Quoted identifier (double quotes or backticks)
    if (char === '"' || char === '`') {
      const quote = char;
      let value = quote;
      pos++;
      while (pos < sql.length && sql[pos] !== quote) {
        value += sql[pos++];
      }
      if (pos < sql.length) {
        value += sql[pos++];
      }
      tokens.push({ type: 'identifier', value });
      continue;
    }

    // Square bracket quoted identifier
    if (char === '[') {
      let value = '[';
      pos++;
      while (pos < sql.length && sql[pos] !== ']') {
        value += sql[pos++];
      }
      if (pos < sql.length) {
        value += sql[pos++];
      }
      tokens.push({ type: 'identifier', value });
      continue;
    }

    // Two-character operators
    const twoChar = char + (sql[pos + 1] || '');
    if (['<=', '>=', '<>', '!=', '||', '<<', '>>'].includes(twoChar)) {
      tokens.push({ type: 'operator', value: twoChar });
      pos += 2;
      continue;
    }

    // Single-character operators
    if ('=<>+-*/%&|~'.includes(char)) {
      tokens.push({ type: 'operator', value: char });
      pos++;
      continue;
    }

    // Punctuation
    if ('(),;.?:$'.includes(char)) {
      tokens.push({ type: 'punctuation', value: char });
      pos++;
      continue;
    }

    // Unknown - treat as identifier
    tokens.push({ type: 'identifier', value: char });
    pos++;
  }

  return tokens;
}

/**
 * Apply syntax highlighting to SQL
 *
 * @param sql - SQL string to highlight
 * @param useColors - Whether to apply ANSI colors (default: true)
 * @returns Highlighted SQL string with ANSI codes
 */
export function highlightSQL(sql: string, useColors: boolean = true): string {
  if (!useColors) {
    return sql;
  }

  const tokens = tokenizeForHighlight(sql);
  let result = '';

  for (const token of tokens) {
    switch (token.type) {
      case 'keyword':
        result += COLORS.keyword + token.value + COLORS.reset;
        break;
      case 'string':
        result += COLORS.string + token.value + COLORS.reset;
        break;
      case 'number':
        result += COLORS.number + token.value + COLORS.reset;
        break;
      case 'comment':
        result += COLORS.comment + token.value + COLORS.reset;
        break;
      case 'operator':
        result += COLORS.operator + token.value + COLORS.reset;
        break;
      case 'function':
        result += COLORS.function + token.value + COLORS.reset;
        break;
      case 'identifier':
        result += COLORS.identifier + token.value + COLORS.reset;
        break;
      case 'punctuation':
        result += COLORS.punctuation + token.value + COLORS.reset;
        break;
      case 'whitespace':
        result += token.value;
        break;
    }
  }

  return result;
}

// =============================================================================
// TAB COMPLETION
// =============================================================================

/**
 * Completion result
 */
export interface CompletionResult {
  /** Completions that match the current input */
  completions: string[];
  /** The word being completed (for replacement) */
  word: string;
  /** Start position of the word in the input */
  start: number;
  /** End position of the word in the input */
  end: number;
}

/**
 * Schema information for context-aware completion
 */
export interface SchemaInfo {
  tables: string[];
  columns: Map<string, string[]>; // table -> columns
}

/**
 * Tab completer for SQL input
 */
export class TabCompleter {
  private schemaInfo: SchemaInfo = { tables: [], columns: new Map() };
  private enabled: boolean = true;

  /**
   * Enable or disable completion
   */
  setEnabled(enabled: boolean): void {
    this.enabled = enabled;
  }

  /**
   * Update schema information for context-aware completion
   */
  updateSchema(info: SchemaInfo): void {
    this.schemaInfo = info;
  }

  /**
   * Add a table to schema
   */
  addTable(name: string, columns: string[] = []): void {
    if (!this.schemaInfo.tables.includes(name)) {
      this.schemaInfo.tables.push(name);
      this.schemaInfo.tables.sort();
    }
    if (columns.length > 0) {
      this.schemaInfo.columns.set(name, columns.sort());
    }
  }

  /**
   * Clear schema information
   */
  clearSchema(): void {
    this.schemaInfo = { tables: [], columns: new Map() };
  }

  /**
   * Get completions for the given input at cursor position
   *
   * @param input - Current input line
   * @param cursorPos - Cursor position in input (defaults to end)
   * @returns Completion result with matching suggestions
   */
  complete(input: string, cursorPos: number = input.length): CompletionResult {
    if (!this.enabled) {
      return { completions: [], word: '', start: cursorPos, end: cursorPos };
    }

    // Find the word being typed at cursor position
    const beforeCursor = input.slice(0, cursorPos);

    // Check for dot command first (must be at start of line, ignoring whitespace)
    const dotMatch = beforeCursor.match(/^\s*(\.[a-zA-Z]*)$/);
    if (dotMatch) {
      const prefix = dotMatch[1].toLowerCase();
      const completions = DOT_COMMANDS.filter(cmd =>
        cmd.toLowerCase().startsWith(prefix)
      );
      return {
        completions,
        word: dotMatch[1],
        start: beforeCursor.indexOf(dotMatch[1]),
        end: cursorPos,
      };
    }

    // Check for table.column completion (ends with "table.")
    const tableColMatch = beforeCursor.match(/(\w+)\.\s*$/);
    if (tableColMatch) {
      const tableName = tableColMatch[1].toLowerCase();
      let completions: string[] = [];

      // Find table (case-insensitive)
      for (const [table, cols] of this.schemaInfo.columns.entries()) {
        if (table.toLowerCase() === tableName) {
          completions = [...cols]; // All columns match (no prefix after .)
          break;
        }
      }

      // Remove duplicates and sort
      completions = [...new Set(completions)].sort((a, b) =>
        a.toLowerCase().localeCompare(b.toLowerCase())
      );

      return {
        completions,
        word: '',
        start: cursorPos,
        end: cursorPos,
      };
    }

    const wordMatch = beforeCursor.match(/([a-zA-Z_][a-zA-Z0-9_]*)$/);

    // Even if there's no word typed, we might offer contextual completions
    const word = wordMatch ? wordMatch[1] : '';
    const start = cursorPos - word.length;
    const prefix = word.toLowerCase();

    // Determine context for smart completion
    const context = this.getCompletionContext(beforeCursor);

    let completions: string[] = [];

    // Context-specific completions
    switch (context) {
      case 'from':
      case 'join':
      case 'into':
      case 'update':
        // After FROM, JOIN, INTO, UPDATE - suggest tables
        completions = this.schemaInfo.tables.filter(t =>
          t.toLowerCase().startsWith(prefix)
        );
        break;

      case 'column':
        // In SELECT, WHERE, etc. - suggest columns and keywords
        completions = this.getAllColumns().filter(c =>
          c.toLowerCase().startsWith(prefix)
        );
        // Also add keywords
        completions = completions.concat(
          SQL_KEYWORDS.filter(k => k.toLowerCase().startsWith(prefix))
        );
        break;

      case 'table_column':
        // After table alias (e.g., "users.col") - suggest columns for that table
        const tableMatch = beforeCursor.match(/(\w+)\.(\w*)$/);
        if (tableMatch) {
          const tableName = tableMatch[1].toLowerCase();
          const colPrefix = tableMatch[2].toLowerCase();
          // Find table (case-insensitive)
          for (const [table, cols] of this.schemaInfo.columns.entries()) {
            if (table.toLowerCase() === tableName) {
              completions = cols.filter(c =>
                c.toLowerCase().startsWith(colPrefix)
              );
              break;
            }
          }
        }
        break;

      default:
        // General context - suggest keywords and tables
        if (prefix) {
          completions = SQL_KEYWORDS.filter(k =>
            k.toLowerCase().startsWith(prefix)
          );
          completions = completions.concat(
            this.schemaInfo.tables.filter(t =>
              t.toLowerCase().startsWith(prefix)
            )
          );
        }
    }

    // Remove duplicates and sort
    completions = [...new Set(completions)].sort((a, b) =>
      a.toLowerCase().localeCompare(b.toLowerCase())
    );

    return { completions, word, start, end: cursorPos };
  }

  /**
   * Determine the completion context from the input
   */
  private getCompletionContext(input: string): 'from' | 'join' | 'into' | 'update' | 'column' | 'table_column' | 'general' {
    const lower = input.toLowerCase();
    const trimmed = lower.trim();

    // Check for table.column context
    if (/\w+\.\s*$/.test(input)) {
      return 'table_column';
    }

    // Check for FROM clause
    if (/\bfrom\s+\w*$/.test(trimmed) || /\bfrom\s*$/.test(trimmed)) {
      return 'from';
    }

    // Check for JOIN clause
    if (/\bjoin\s+\w*$/.test(trimmed) || /\bjoin\s*$/.test(trimmed)) {
      return 'join';
    }

    // Check for INTO clause
    if (/\binto\s+\w*$/.test(trimmed) || /\binto\s*$/.test(trimmed)) {
      return 'into';
    }

    // Check for UPDATE statement
    if (/\bupdate\s+\w*$/.test(trimmed) || /\bupdate\s*$/.test(trimmed)) {
      return 'update';
    }

    // Check for SELECT column context
    if (/\bselect\s+[\w\s,*]*$/.test(trimmed) && !/\bfrom\b/.test(trimmed)) {
      return 'column';
    }

    // Check for WHERE clause
    if (/\bwhere\s+[\w\s]*$/.test(trimmed)) {
      return 'column';
    }

    return 'general';
  }

  /**
   * Get all columns from all tables
   */
  private getAllColumns(): string[] {
    const allColumns = new Set<string>();
    for (const columns of this.schemaInfo.columns.values()) {
      columns.forEach(c => allColumns.add(c));
    }
    return [...allColumns].sort();
  }

  /**
   * Get completion for a single word (returns common prefix or cycles through options)
   *
   * @param input - Current input
   * @param cursorPos - Cursor position
   * @param index - Index for cycling through completions (0 = common prefix or first match)
   * @returns The completed string, or null if no completions
   */
  getCompletion(input: string, cursorPos: number = input.length, index: number = 0): string | null {
    const result = this.complete(input, cursorPos);

    if (result.completions.length === 0) {
      return null;
    }

    // If only one completion, use it
    if (result.completions.length === 1) {
      return input.slice(0, result.start) + result.completions[0] + input.slice(result.end);
    }

    // Multiple completions - if index is 0, find common prefix
    if (index === 0) {
      const commonPrefix = this.findCommonPrefix(result.completions);
      if (commonPrefix.length > result.word.length) {
        return input.slice(0, result.start) + commonPrefix + input.slice(result.end);
      }
    }

    // Cycle through completions
    const completion = result.completions[index % result.completions.length];
    return input.slice(0, result.start) + completion + input.slice(result.end);
  }

  /**
   * Find common prefix among strings
   */
  private findCommonPrefix(strings: string[]): string {
    if (strings.length === 0) return '';
    if (strings.length === 1) return strings[0];

    let prefix = strings[0];
    for (let i = 1; i < strings.length; i++) {
      while (!strings[i].toLowerCase().startsWith(prefix.toLowerCase())) {
        prefix = prefix.slice(0, -1);
        if (prefix.length === 0) return '';
      }
      // Preserve case from first match
      prefix = strings[0].slice(0, prefix.length);
    }
    return prefix;
  }
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
  private _remoteUrl: string | null = null;

  /** Current connection mode */
  private _connectionMode: 'local' | 'http' | 'websocket';

  /** Tab completer instance */
  private _completer: TabCompleter;

  /** Whether syntax highlighting is enabled */
  private _highlightEnabled: boolean;

  /** Get current connection mode */
  get connectionMode(): 'local' | 'http' | 'websocket' {
    return this._connectionMode;
  }

  /** Get the tab completer */
  get completer(): TabCompleter {
    return this._completer;
  }

  /** Get whether highlighting is enabled */
  get highlightEnabled(): boolean {
    return this._highlightEnabled;
  }

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
    this._connectionMode = config.mode;
    this.prompt = config.prompt ?? 'dosql> ';
    this.multilinePrompt = config.multilinePrompt ?? '   ...> ';
    this.format = config.format ?? 'table';
    this.outputFn = config.output ?? console.log;

    // Initialize history
    this.history = new HistoryManager({
      maxSize: config.maxHistorySize ?? 1000,
      historyFile: config.historyFile,
    });

    // Initialize tab completer
    this._completer = new TabCompleter();
    if (config.completionEnabled === false) {
      this._completer.setEnabled(false);
    }

    // Initialize highlighting (default to true if not specified)
    this._highlightEnabled = config.highlightEnabled ?? true;

    // Update global state
    replState.format = this.format;
  }

  /**
   * Start the REPL (shows welcome message)
   */
  async start(): Promise<void> {
    // Connect to database
    await this.connect();

    // Refresh schema for tab completion
    await this.refreshSchema();

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
   * Refresh schema information for tab completion
   */
  async refreshSchema(): Promise<void> {
    if (!this.connection) {
      return;
    }

    try {
      // Get table names
      const tablesResult = await this.connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
      );

      this._completer.clearSchema();

      for (const row of tablesResult.rows) {
        const tableName = row.name as string;

        // Get columns for this table
        try {
          const columnsResult = await this.connection.execute(
            `PRAGMA table_info("${tableName}")`
          );
          const columns = columnsResult.rows.map(r => r.name as string);
          this._completer.addTable(tableName, columns);
        } catch {
          // If PRAGMA fails, just add the table without columns
          this._completer.addTable(tableName);
        }
      }
    } catch {
      // Silently fail - schema completion is a convenience feature
    }
  }

  /**
   * Enable or disable syntax highlighting
   */
  setHighlightEnabled(enabled: boolean): void {
    this._highlightEnabled = enabled;
  }

  /**
   * Get syntax-highlighted version of input
   *
   * @param input - SQL input to highlight
   * @returns Highlighted string with ANSI codes, or original if highlighting disabled
   */
  highlight(input: string): string {
    if (!this._highlightEnabled) {
      return input;
    }
    return highlightSQL(input, true);
  }

  /**
   * Get tab completion for current input
   *
   * @param input - Current input line
   * @param cursorPos - Cursor position (defaults to end)
   * @returns Completion result
   */
  getCompletions(input: string, cursorPos?: number): CompletionResult {
    return this._completer.complete(input, cursorPos);
  }

  /**
   * Apply tab completion to input
   *
   * @param input - Current input line
   * @param cursorPos - Cursor position (defaults to end)
   * @param index - Completion index for cycling (0 = common prefix or first)
   * @returns Completed string or null if no completions
   */
  applyCompletion(input: string, cursorPos?: number, index: number = 0): string | null {
    return this._completer.getCompletion(input, cursorPos, index);
  }

  /**
   * Connect to the database based on mode
   */
  private async connect(): Promise<void> {
    // Use injected connection if provided
    if (this.config.connection) {
      this.connection = this.config.connection;
      return;
    }

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

        // Handle .connect command
        if (cmd === '.connect') {
          const url = input.trim().split(/\s+/)[1];
          if (!url) {
            return 'Error: .connect requires a URL';
          }
          try {
            return await this.connectRemote(url);
          } catch (err) {
            return `Error: ${err instanceof Error ? err.message : 'Unknown error'}`;
          }
        }

        // Handle .disconnect command
        if (cmd === '.disconnect') {
          return await this.disconnect();
        }

        // Handle .status command
        if (cmd === '.status') {
          return this.getStatus();
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

      // Refresh schema after DDL statements for tab completion
      const upperSql = sql.toUpperCase().trim();
      if (upperSql.startsWith('CREATE') || upperSql.startsWith('DROP') || upperSql.startsWith('ALTER')) {
        // Fire and forget - don't wait for schema refresh
        this.refreshSchema().catch(() => {});
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
   * Connect to a remote DoSQL endpoint
   *
   * @param url - The URL to connect to (http/https or ws/wss)
   * @returns Status message
   */
  async connectRemote(url: string): Promise<string> {
    // Close existing connection if any
    if (this.connection) {
      await this.connection.close();
      this.connection = null;
    }

    // Determine connection type from URL
    const isWebSocket = url.startsWith('ws://') || url.startsWith('wss://');
    const isHTTP = url.startsWith('http://') || url.startsWith('https://');

    if (!isWebSocket && !isHTTP) {
      throw new Error('URL must start with http://, https://, ws://, or wss://');
    }

    try {
      if (isWebSocket) {
        this._connectionMode = 'websocket';
        this.connection = await createWebSocketConnection({
          url,
          autoReconnect: true,
        });
      } else {
        this._connectionMode = 'http';
        this.connection = await createHTTPConnection({
          url,
        });
      }

      this._remoteUrl = url;

      // Refresh schema for tab completion
      await this.refreshSchema();

      return `Connected to ${url} (${this._connectionMode} mode)`;
    } catch (error) {
      this._connectionMode = 'local';
      this._remoteUrl = null;
      throw error;
    }
  }

  /**
   * Disconnect from remote endpoint and optionally reconnect to local
   *
   * @returns Status message
   */
  async disconnect(): Promise<string> {
    if (!this.connection) {
      return 'Not connected';
    }

    const previousUrl = this._remoteUrl;
    const previousMode = this._connectionMode;

    await this.connection.close();
    this.connection = null;
    this._remoteUrl = null;

    // Reconnect to local if we were on a remote connection
    if (previousMode !== 'local' && this.config.database) {
      this._connectionMode = 'local';
      this.connection = await createLocalConnection({
        database: this.config.database,
      });

      // Refresh schema for tab completion
      await this.refreshSchema();

      return `Disconnected from ${previousUrl}. Reconnected to local database.`;
    }

    this._connectionMode = 'local';
    return previousUrl
      ? `Disconnected from ${previousUrl}`
      : 'Disconnected';
  }

  /**
   * Get connection status information
   *
   * @returns Status string
   */
  getStatus(): string {
    const lines: string[] = [];

    lines.push(`Mode: ${this._connectionMode}`);

    if (this._connectionMode === 'local') {
      lines.push(`Database: ${this.config.database ?? ':memory:'}`);
    } else if (this._remoteUrl) {
      lines.push(`URL: ${this._remoteUrl}`);
    }

    lines.push(`Connected: ${this.connection ? 'yes' : 'no'}`);

    if (this.connection && 'reconnect' in this.connection) {
      lines.push('Auto-reconnect: enabled');
    }

    return lines.join('\n');
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

