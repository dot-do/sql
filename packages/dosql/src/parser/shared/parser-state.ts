/**
 * Shared Parser State Utilities
 *
 * Functional-style parser state management used by DML and window parsers.
 * Provides an alternative to class-based parsing for simpler grammars.
 *
 * @packageDocumentation
 */

import type { SourceLocation } from './types.js';

// =============================================================================
// PARSER STATE
// =============================================================================

/**
 * Immutable parser state
 */
export interface ParserState {
  /** Original input string */
  readonly input: string;
  /** Current position in input */
  readonly position: number;
  /** Current line number (1-based) */
  readonly line: number;
  /** Current column number (1-based) */
  readonly column: number;
}

/**
 * Create initial parser state from input string
 */
export function createParserState(input: string): ParserState {
  return {
    input: input.trim(),
    position: 0,
    line: 1,
    column: 1,
  };
}

/**
 * Get remaining input from current position
 */
export function remaining(state: ParserState): string {
  return state.input.slice(state.position);
}

/**
 * Check if at end of input
 */
export function isEOF(state: ParserState): boolean {
  return state.position >= state.input.length;
}

/**
 * Get current source location
 */
export function getLocation(state: ParserState): SourceLocation {
  return {
    line: state.line,
    column: state.column,
    offset: state.position,
  };
}

/**
 * Advance position by count characters
 */
export function advance(state: ParserState, count: number): ParserState {
  let { line, column } = state;

  // Track line/column changes
  for (let i = 0; i < count && state.position + i < state.input.length; i++) {
    if (state.input[state.position + i] === '\n') {
      line++;
      column = 1;
    } else {
      column++;
    }
  }

  return {
    ...state,
    position: state.position + count,
    line,
    column,
  };
}

/**
 * Peek at character at offset from current position
 */
export function peek(state: ParserState, offset = 0): string {
  return state.input[state.position + offset] || '';
}

// =============================================================================
// WHITESPACE HANDLING
// =============================================================================

/**
 * Skip whitespace and return new state
 */
export function skipWhitespace(state: ParserState): ParserState {
  const rest = remaining(state);
  const match = rest.match(/^\s+/);
  if (match) {
    return advance(state, match[0].length);
  }
  return state;
}

/**
 * Skip whitespace and comments
 */
export function skipWhitespaceAndComments(state: ParserState): ParserState {
  while (state.position < state.input.length) {
    state = skipWhitespace(state);
    const rest = remaining(state);

    // Single-line comment
    if (rest.startsWith('--')) {
      const end = rest.indexOf('\n');
      if (end === -1) {
        return advance(state, rest.length);
      }
      state = advance(state, end + 1);
      continue;
    }

    // Multi-line comment
    if (rest.startsWith('/*')) {
      const end = rest.indexOf('*/', 2);
      if (end === -1) {
        return advance(state, rest.length);
      }
      state = advance(state, end + 2);
      continue;
    }

    break;
  }
  return state;
}

// =============================================================================
// MATCHING UTILITIES
// =============================================================================

/**
 * Try to match a keyword (case-insensitive)
 * @returns New state if matched, null otherwise
 */
export function matchKeyword(state: ParserState, keyword: string): ParserState | null {
  const rest = remaining(state);
  // Match keyword followed by word boundary or end
  const regex = new RegExp(`^${keyword}(?=[\\s;,()\\[\\]|]|$)`, 'i');
  if (regex.test(rest)) {
    return advance(state, keyword.length);
  }
  // Also match if exactly at end of input
  if (rest.toUpperCase() === keyword.toUpperCase()) {
    return advance(state, keyword.length);
  }
  return null;
}

/**
 * Try to match exact string
 * @returns New state if matched, null otherwise
 */
export function matchExact(state: ParserState, str: string): ParserState | null {
  if (remaining(state).startsWith(str)) {
    return advance(state, str.length);
  }
  return null;
}

/**
 * Try to match a regex pattern
 * @returns Match result with new state, or null
 */
export function matchRegex(
  state: ParserState,
  pattern: RegExp
): { state: ParserState; match: RegExpMatchArray } | null {
  const rest = remaining(state);
  const match = rest.match(pattern);
  if (match && match.index === 0) {
    return {
      state: advance(state, match[0].length),
      match,
    };
  }
  return null;
}

// =============================================================================
// IDENTIFIER PARSING
// =============================================================================

/**
 * Parse an identifier (table name, column name, etc.)
 */
export function parseIdentifier(
  state: ParserState
): { state: ParserState; value: string } | null {
  state = skipWhitespace(state);
  const rest = remaining(state);

  // Quoted identifier (double quotes or backticks)
  if (rest.startsWith('"') || rest.startsWith('`') || rest.startsWith('[')) {
    const quote = rest[0];
    const endQuote = quote === '[' ? ']' : quote;
    const endIndex = rest.indexOf(endQuote, 1);
    if (endIndex === -1) return null;
    const value = rest.slice(1, endIndex);
    return { state: advance(state, endIndex + 1), value };
  }

  // Unquoted identifier
  const match = rest.match(/^[a-zA-Z_][a-zA-Z0-9_]*/);
  if (match) {
    return { state: advance(state, match[0].length), value: match[0] };
  }
  return null;
}

/**
 * Parse a comma-separated list of identifiers
 */
export function parseIdentifierList(
  state: ParserState
): { state: ParserState; values: string[] } | null {
  const values: string[] = [];

  // Parse first identifier
  const first = parseIdentifier(state);
  if (!first) return null;
  values.push(first.value);
  state = first.state;

  // Parse additional identifiers
  while (true) {
    state = skipWhitespace(state);
    if (!remaining(state).startsWith(',')) break;
    state = advance(state, 1);
    state = skipWhitespace(state);

    const next = parseIdentifier(state);
    if (!next) return null;
    values.push(next.value);
    state = next.state;
  }

  return { state, values };
}

// =============================================================================
// LITERAL PARSING
// =============================================================================

/**
 * Parse a string literal
 */
export function parseStringLiteral(
  state: ParserState
): { state: ParserState; value: string; raw: string } | null {
  state = skipWhitespace(state);
  const rest = remaining(state);
  const quote = rest[0];

  if (quote !== "'" && quote !== '"') return null;

  let i = 1;
  let value = '';
  while (i < rest.length) {
    if (rest[i] === quote) {
      // Escaped quote (doubled)
      if (rest[i + 1] === quote) {
        value += quote;
        i += 2;
      } else {
        // End of string
        return {
          state: advance(state, i + 1),
          value,
          raw: rest.slice(0, i + 1),
        };
      }
    } else if (rest[i] === '\\' && rest[i + 1]) {
      // Escape sequence
      const escaped = rest[i + 1];
      switch (escaped) {
        case 'n': value += '\n'; break;
        case 't': value += '\t'; break;
        case 'r': value += '\r'; break;
        default: value += escaped;
      }
      i += 2;
    } else {
      value += rest[i];
      i++;
    }
  }
  return null; // Unterminated string
}

/**
 * Parse a numeric literal
 */
export function parseNumericLiteral(
  state: ParserState
): { state: ParserState; value: number; raw: string } | null {
  state = skipWhitespace(state);
  const rest = remaining(state);
  const match = rest.match(/^-?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?/);
  if (match) {
    const raw = match[0];
    const value = raw.includes('.') || raw.toLowerCase().includes('e')
      ? parseFloat(raw)
      : parseInt(raw, 10);
    return { state: advance(state, raw.length), value, raw };
  }
  return null;
}

// =============================================================================
// PARENTHESES PARSING
// =============================================================================

/**
 * Parse content within balanced parentheses
 * @returns The content between parens (not including the parens)
 */
export function parseParenthesized(
  state: ParserState
): { state: ParserState; content: string } | null {
  state = skipWhitespace(state);
  if (!remaining(state).startsWith('(')) return null;

  let depth = 1;
  let i = 1;
  const rest = remaining(state);

  while (i < rest.length && depth > 0) {
    if (rest[i] === '(') depth++;
    else if (rest[i] === ')') depth--;
    i++;
  }

  if (depth !== 0) return null;

  const content = rest.slice(1, i - 1);
  return { state: advance(state, i), content };
}

/**
 * Find matching closing parenthesis
 * @returns Position of closing paren, or -1 if not found
 */
export function findMatchingParen(str: string, start = 0): number {
  let depth = 0;
  for (let i = start; i < str.length; i++) {
    if (str[i] === '(') depth++;
    else if (str[i] === ')') {
      depth--;
      if (depth === 0) return i;
    }
  }
  return -1;
}
