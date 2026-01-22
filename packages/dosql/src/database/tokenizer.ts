/**
 * SQL Tokenizer - State-aware SQL statement splitter
 *
 * This tokenizer properly handles:
 * - Single-quoted string literals with escaped quotes ('')
 * - Double-quoted identifiers (SQLite/PostgreSQL style)
 * - Backtick-quoted identifiers (MySQL style)
 * - Block comments (/* ... *\/)
 * - Line comments (-- ...)
 *
 * Only splits on semicolons in NORMAL state, ensuring that semicolons
 * inside strings, identifiers, and comments are preserved.
 *
 * @packageDocumentation
 */

/**
 * Tokenizer state machine states
 */
type TokenizerState =
  | 'normal'
  | 'single_quote'
  | 'double_quote'
  | 'backtick'
  | 'block_comment'
  | 'line_comment';

/**
 * Tokenize a SQL string into individual statements.
 *
 * This function implements a state-aware lexer that properly handles:
 * - String literals with escaped quotes
 * - Double-quoted and backtick-quoted identifiers
 * - Block comments (/* ... *\/)
 * - Line comments (-- ...)
 *
 * @param sql - The SQL string containing one or more statements
 * @returns An array of individual SQL statements (trimmed, non-empty)
 *
 * @example
 * ```typescript
 * // Simple case
 * tokenizeSQL('SELECT 1; SELECT 2')
 * // => ['SELECT 1', 'SELECT 2']
 *
 * // Semicolon in string (should NOT split)
 * tokenizeSQL("SELECT 'a;b'")
 * // => ["SELECT 'a;b'"]
 *
 * // Semicolon in comment (should NOT split)
 * // tokenizeSQL('SELECT * /\* ; *\/ FROM t')
 * // => ['SELECT * /\* ; *\/ FROM t']
 * ```
 */
export function tokenizeSQL(sql: string): string[] {
  const statements: string[] = [];
  let current = '';
  let state: TokenizerState = 'normal';
  let i = 0;

  while (i < sql.length) {
    const char = sql[i];
    const next = sql[i + 1];

    switch (state) {
      case 'normal':
        // Check for start of single-quoted string
        if (char === "'") {
          state = 'single_quote';
          current += char;
        }
        // Check for start of double-quoted identifier
        else if (char === '"') {
          state = 'double_quote';
          current += char;
        }
        // Check for start of backtick-quoted identifier
        else if (char === '`') {
          state = 'backtick';
          current += char;
        }
        // Check for start of block comment
        else if (char === '/' && next === '*') {
          state = 'block_comment';
          current += '/*';
          i++; // Skip the '*'
        }
        // Check for start of line comment
        else if (char === '-' && next === '-') {
          state = 'line_comment';
          current += '--';
          i++; // Skip the second '-'
        }
        // Semicolon - statement separator
        else if (char === ';') {
          const trimmed = current.trim();
          if (trimmed.length > 0) {
            statements.push(trimmed);
          }
          current = '';
        }
        // Regular character
        else {
          current += char;
        }
        break;

      case 'single_quote':
        // Check for escaped single quote ('')
        if (char === "'" && next === "'") {
          current += "''";
          i++; // Skip the second quote
        }
        // End of single-quoted string
        else if (char === "'") {
          current += char;
          state = 'normal';
        }
        // Regular character inside string
        else {
          current += char;
        }
        break;

      case 'double_quote':
        // Check for escaped double quote ("")
        if (char === '"' && next === '"') {
          current += '""';
          i++; // Skip the second quote
        }
        // End of double-quoted identifier
        else if (char === '"') {
          current += char;
          state = 'normal';
        }
        // Regular character inside identifier
        else {
          current += char;
        }
        break;

      case 'backtick':
        // Check for escaped backtick (``)
        if (char === '`' && next === '`') {
          current += '``';
          i++; // Skip the second backtick
        }
        // End of backtick-quoted identifier
        else if (char === '`') {
          current += char;
          state = 'normal';
        }
        // Regular character inside identifier
        else {
          current += char;
        }
        break;

      case 'block_comment':
        // Check for end of block comment
        if (char === '*' && next === '/') {
          current += '*/';
          i++; // Skip the '/'
          state = 'normal';
        }
        // Regular character inside comment
        else {
          current += char;
        }
        break;

      case 'line_comment':
        // End of line comment (newline)
        if (char === '\n' || char === '\r') {
          current += char;
          state = 'normal';
        }
        // Regular character inside comment
        else {
          current += char;
        }
        break;
    }

    i++;
  }

  // Add any remaining content as the final statement
  const trimmed = current.trim();
  if (trimmed.length > 0) {
    statements.push(trimmed);
  }

  return statements;
}

/**
 * Check if the SQL tokenizer is in a balanced state.
 *
 * This can be used to detect potentially malformed SQL with unclosed
 * strings or comments.
 *
 * @param sql - The SQL string to check
 * @returns true if all strings and comments are properly closed
 */
export function isBalanced(sql: string): boolean {
  let state: TokenizerState = 'normal';
  let i = 0;

  while (i < sql.length) {
    const char = sql[i];
    const next = sql[i + 1];

    switch (state) {
      case 'normal':
        if (char === "'") {
          state = 'single_quote';
        } else if (char === '"') {
          state = 'double_quote';
        } else if (char === '`') {
          state = 'backtick';
        } else if (char === '/' && next === '*') {
          state = 'block_comment';
          i++;
        } else if (char === '-' && next === '-') {
          state = 'line_comment';
          i++;
        }
        break;

      case 'single_quote':
        if (char === "'" && next === "'") {
          i++; // Skip escaped quote
        } else if (char === "'") {
          state = 'normal';
        }
        break;

      case 'double_quote':
        if (char === '"' && next === '"') {
          i++;
        } else if (char === '"') {
          state = 'normal';
        }
        break;

      case 'backtick':
        if (char === '`' && next === '`') {
          i++;
        } else if (char === '`') {
          state = 'normal';
        }
        break;

      case 'block_comment':
        if (char === '*' && next === '/') {
          i++;
          state = 'normal';
        }
        break;

      case 'line_comment':
        if (char === '\n' || char === '\r') {
          state = 'normal';
        }
        break;
    }

    i++;
  }

  // Line comments at end of input are considered closed (implicitly ended by EOF)
  return state === 'normal' || state === 'line_comment';
}
