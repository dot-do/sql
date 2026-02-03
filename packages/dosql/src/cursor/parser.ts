/**
 * Cursor SQL Parser for DoSQL
 *
 * Parses cursor-related SQL commands:
 * - DECLARE cursor_name [SCROLL] CURSOR FOR select_query
 * - FETCH [NEXT|PRIOR|FIRST|LAST|ABSOLUTE n|RELATIVE n] n FROM cursor_name
 * - CLOSE cursor_name
 *
 * @packageDocumentation
 */

import type {
  CursorCommand,
  DeclareCursorCommand,
  FetchCommand,
  CloseCursorCommand,
  FetchDirection,
} from './types.js';

// =============================================================================
// Parser
// =============================================================================

/**
 * Check if a SQL statement is a cursor command
 */
export function isCursorCommand(sql: string): boolean {
  const normalized = sql.trim().toUpperCase();
  return (
    normalized.startsWith('DECLARE ') ||
    normalized.startsWith('FETCH ') ||
    normalized.startsWith('CLOSE ')
  );
}

/**
 * Parse a cursor command from SQL
 *
 * @param sql - SQL statement to parse
 * @returns Parsed cursor command or null if not a cursor command
 */
export function parseCursorCommand(sql: string): CursorCommand | null {
  const trimmed = sql.trim();
  const normalized = trimmed.toUpperCase();

  if (normalized.startsWith('DECLARE ')) {
    return parseDeclareCursor(trimmed);
  }

  if (normalized.startsWith('FETCH ')) {
    return parseFetchCommand(trimmed);
  }

  if (normalized.startsWith('CLOSE ')) {
    return parseCloseCommand(trimmed);
  }

  return null;
}

// =============================================================================
// DECLARE CURSOR Parser
// =============================================================================

/**
 * Parse DECLARE CURSOR statement
 *
 * Syntax variations supported:
 * - DECLARE cursor_name CURSOR FOR select_query
 * - DECLARE cursor_name SCROLL CURSOR FOR select_query
 * - DECLARE cursor_name CURSOR WITH HOLD FOR select_query
 * - DECLARE cursor_name SCROLL CURSOR WITH HOLD FOR select_query
 */
function parseDeclareCursor(sql: string): DeclareCursorCommand | null {
  // Match: DECLARE name [SCROLL] [INSENSITIVE] CURSOR [WITH HOLD] FOR query
  const pattern = /^DECLARE\s+(\w+)\s+((?:SCROLL\s+)?(?:INSENSITIVE\s+)?CURSOR(?:\s+WITH\s+HOLD)?)\s+FOR\s+(.+)$/is;
  const match = sql.match(pattern);

  if (!match) {
    return null;
  }

  const cursorName = match[1]!;
  const cursorOptions = match[2]!.toUpperCase();
  const query = match[3]!.trim();

  // Validate that query is a SELECT
  if (!query.toUpperCase().startsWith('SELECT')) {
    throw new Error('DECLARE CURSOR query must be a SELECT statement');
  }

  return {
    type: 'DECLARE_CURSOR',
    cursorName,
    scrollable: cursorOptions.includes('SCROLL'),
    holdable: cursorOptions.includes('WITH HOLD'),
    query,
  };
}

// =============================================================================
// FETCH Parser
// =============================================================================

/**
 * Parse FETCH statement
 *
 * Syntax variations supported:
 * - FETCH NEXT n FROM cursor_name
 * - FETCH n FROM cursor_name (shorthand for FETCH NEXT n)
 * - FETCH PRIOR n FROM cursor_name
 * - FETCH FIRST FROM cursor_name
 * - FETCH LAST FROM cursor_name
 * - FETCH ABSOLUTE n FROM cursor_name
 * - FETCH RELATIVE n FROM cursor_name
 * - FETCH FROM cursor_name (shorthand for FETCH NEXT 1)
 * - FETCH NEXT FROM cursor_name (shorthand for FETCH NEXT 1)
 *
 * PostgreSQL-style:
 * - FETCH NEXT n ROWS FROM cursor_name
 * - FETCH FORWARD n FROM cursor_name
 */
function parseFetchCommand(sql: string): FetchCommand | null {
  const trimmed = sql.trim();
  const upper = trimmed.toUpperCase();

  // Try different patterns

  // Pattern 1: FETCH [direction] count [ROWS] FROM cursor
  const withCountPattern = /^FETCH\s+(?:(NEXT|PRIOR|FORWARD|BACKWARD)\s+)?(\d+)\s*(?:ROWS?\s+)?FROM\s+(\w+)$/i;
  let match = trimmed.match(withCountPattern);
  if (match) {
    const rawDirection = match[1]?.toUpperCase() ?? 'NEXT';
    // Normalize FORWARD/BACKWARD to NEXT/PRIOR
    let direction: FetchDirection;
    if (rawDirection === 'FORWARD' || rawDirection === 'NEXT') {
      direction = 'NEXT';
    } else if (rawDirection === 'BACKWARD' || rawDirection === 'PRIOR') {
      direction = 'PRIOR';
    } else {
      direction = rawDirection as FetchDirection;
    }

    return {
      type: 'FETCH',
      direction,
      count: parseInt(match[2]!, 10),
      cursorName: match[3]!,
    };
  }

  // Pattern 2: FETCH direction FROM cursor (count = 1)
  const directionOnlyPattern = /^FETCH\s+(NEXT|PRIOR|FIRST|LAST)\s+FROM\s+(\w+)$/i;
  match = trimmed.match(directionOnlyPattern);
  if (match) {
    return {
      type: 'FETCH',
      direction: match[1]!.toUpperCase() as FetchDirection,
      count: 1,
      cursorName: match[2]!,
    };
  }

  // Pattern 3: FETCH FROM cursor (default NEXT 1)
  const simplePattern = /^FETCH\s+FROM\s+(\w+)$/i;
  match = trimmed.match(simplePattern);
  if (match) {
    return {
      type: 'FETCH',
      direction: 'NEXT',
      count: 1,
      cursorName: match[1]!,
    };
  }

  // Pattern 4: FETCH ABSOLUTE n FROM cursor
  const absolutePattern = /^FETCH\s+ABSOLUTE\s+(-?\d+)\s+FROM\s+(\w+)$/i;
  match = trimmed.match(absolutePattern);
  if (match) {
    return {
      type: 'FETCH',
      direction: 'ABSOLUTE',
      count: parseInt(match[1]!, 10),
      cursorName: match[2]!,
    };
  }

  // Pattern 5: FETCH RELATIVE n FROM cursor
  const relativePattern = /^FETCH\s+RELATIVE\s+(-?\d+)\s+FROM\s+(\w+)$/i;
  match = trimmed.match(relativePattern);
  if (match) {
    return {
      type: 'FETCH',
      direction: 'RELATIVE',
      count: parseInt(match[1]!, 10),
      cursorName: match[2]!,
    };
  }

  // Pattern 6: FETCH ALL FROM cursor (fetch all remaining rows)
  const allPattern = /^FETCH\s+ALL\s+FROM\s+(\w+)$/i;
  match = trimmed.match(allPattern);
  if (match) {
    return {
      type: 'FETCH',
      direction: 'NEXT',
      count: Number.MAX_SAFE_INTEGER, // Will be clamped by maxFetchSize
      cursorName: match[1]!,
    };
  }

  return null;
}

// =============================================================================
// CLOSE Parser
// =============================================================================

/**
 * Parse CLOSE statement
 *
 * Syntax:
 * - CLOSE cursor_name
 * - CLOSE ALL (closes all cursors)
 */
function parseCloseCommand(sql: string): CloseCursorCommand | null {
  const trimmed = sql.trim();

  const pattern = /^CLOSE\s+(\w+|ALL)$/i;
  const match = trimmed.match(pattern);

  if (!match) {
    return null;
  }

  return {
    type: 'CLOSE_CURSOR',
    cursorName: match[1]!,
  };
}

// =============================================================================
// Validation Helpers
// =============================================================================

/**
 * Validate a cursor name
 */
export function isValidCursorName(name: string): boolean {
  // SQL identifier rules: starts with letter or underscore, contains alphanumeric/underscore
  return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name);
}

/**
 * Check if a SQL query is suitable for a cursor (must be SELECT)
 */
export function isSelectQuery(sql: string): boolean {
  return sql.trim().toUpperCase().startsWith('SELECT');
}
