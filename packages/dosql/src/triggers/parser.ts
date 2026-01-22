/**
 * SQL Trigger Parser for DoSQL
 *
 * Parses SQL CREATE TRIGGER and DROP TRIGGER statements following SQLite syntax.
 * Supports:
 * - BEFORE/AFTER/INSTEAD OF triggers
 * - INSERT/UPDATE/DELETE events
 * - UPDATE OF column-specific triggers
 * - WHEN clause conditions
 * - FOR EACH ROW
 * - IF NOT EXISTS
 * - TEMP/TEMPORARY triggers
 * - Schema-qualified names
 */

import type {
  SQLTriggerDefinition,
  SQLTriggerTiming,
  SQLTriggerEvent,
  DropTriggerStatement,
  ParsedSQLTrigger,
} from './types.js';

// =============================================================================
// REGEX PATTERNS
// =============================================================================

// Main CREATE TRIGGER pattern (case-insensitive, multiline)
// Updated to handle quoted identifiers with dots inside them
const CREATE_TRIGGER_REGEX = /^\s*CREATE\s+(TEMP(?:ORARY)?\s+)?TRIGGER\s+(IF\s+NOT\s+EXISTS\s+)?("[^"]+"|`[^`]+`|(?:[\w]+\.)?[\w]+)\s+(BEFORE|AFTER|INSTEAD\s+OF)\s+(INSERT|UPDATE|DELETE)(\s+OF\s+[\w\s,]+)?\s+ON\s+("[^"]+"|`[^`]+`|(?:[\w]+\.)?[\w]+)\s*(?:FOR\s+EACH\s+ROW\s*)?(WHEN\s+.+?)?\s*BEGIN\s+(.+?)\s*END\s*;?\s*$/is;

// DROP TRIGGER pattern
const DROP_TRIGGER_REGEX = /^\s*DROP\s+TRIGGER\s+(IF\s+EXISTS\s+)?((?:["`]?[\w.-]+["`]?\.)?["`]?[\w.-]+["`]?)\s*;?\s*$/i;

// Pattern to detect CREATE TRIGGER statements
const IS_CREATE_TRIGGER_REGEX = /^\s*CREATE\s+(TEMP(?:ORARY)?\s+)?TRIGGER/i;

// Pattern to detect DROP TRIGGER statements
const IS_DROP_TRIGGER_REGEX = /^\s*DROP\s+TRIGGER/i;

// Pattern to extract columns from UPDATE OF clause
const UPDATE_OF_COLUMNS_REGEX = /OF\s+([\w\s,]+)/i;

// Pattern to detect NEW. references
const NEW_REFERENCE_REGEX = /\bNEW\./i;

// Pattern to detect OLD. references
const OLD_REFERENCE_REGEX = /\bOLD\./i;

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Remove quotes from an identifier
 */
function unquoteIdentifier(identifier: string): string {
  if (!identifier) return identifier;
  const trimmed = identifier.trim();

  // Handle double-quoted identifiers (preserve content including dots)
  if (trimmed.startsWith('"') && trimmed.endsWith('"')) {
    return trimmed.slice(1, -1);
  }

  // Handle backtick-quoted identifiers (preserve content including dots)
  if (trimmed.startsWith('`') && trimmed.endsWith('`')) {
    return trimmed.slice(1, -1);
  }

  // Handle square-bracket quoted identifiers (SQL Server style)
  if (trimmed.startsWith('[') && trimmed.endsWith(']')) {
    return trimmed.slice(1, -1);
  }

  return trimmed;
}

/**
 * Parse a schema-qualified name into schema and name parts
 */
function parseQualifiedName(qualifiedName: string): { schema?: string; name: string } {
  const trimmed = qualifiedName.trim();

  // If the whole thing is quoted, the dot is part of the name, not a schema separator
  if ((trimmed.startsWith('"') && trimmed.endsWith('"')) ||
      (trimmed.startsWith('`') && trimmed.endsWith('`')) ||
      (trimmed.startsWith('[') && trimmed.endsWith(']'))) {
    return { name: unquoteIdentifier(trimmed) };
  }

  // Check for unquoted schema.name format (e.g., main.users)
  // The dot must be outside of quotes
  let dotIndex = -1;
  let inQuote = false;
  let quoteChar = '';

  for (let i = 0; i < trimmed.length; i++) {
    const char = trimmed[i];

    if (!inQuote && (char === '"' || char === '`' || char === '[')) {
      inQuote = true;
      quoteChar = char === '[' ? ']' : char;
    } else if (inQuote && char === quoteChar) {
      inQuote = false;
      quoteChar = '';
    } else if (!inQuote && char === '.') {
      dotIndex = i;
      break;
    }
  }

  if (dotIndex > 0) {
    return {
      schema: unquoteIdentifier(trimmed.slice(0, dotIndex)),
      name: unquoteIdentifier(trimmed.slice(dotIndex + 1)),
    };
  }

  return { name: unquoteIdentifier(trimmed) };
}

/**
 * Parse columns from UPDATE OF clause
 */
function parseColumns(columnsString: string | undefined): string[] | undefined {
  if (!columnsString) return undefined;

  const match = columnsString.match(UPDATE_OF_COLUMNS_REGEX);
  if (!match) return undefined;

  return match[1]
    .split(',')
    .map(col => col.trim())
    .filter(col => col.length > 0);
}

/**
 * Normalize timing string to enum value
 */
function normalizeTiming(timing: string): SQLTriggerTiming {
  const normalized = timing.toUpperCase().replace(/\s+/g, ' ');
  switch (normalized) {
    case 'BEFORE':
      return 'BEFORE';
    case 'AFTER':
      return 'AFTER';
    case 'INSTEAD OF':
      return 'INSTEAD OF';
    default:
      throw new Error(`Invalid trigger timing: ${timing}`);
  }
}

/**
 * Normalize event string to enum value
 */
function normalizeEvent(event: string): SQLTriggerEvent {
  const normalized = event.toUpperCase();
  switch (normalized) {
    case 'INSERT':
      return 'INSERT';
    case 'UPDATE':
      return 'UPDATE';
    case 'DELETE':
      return 'DELETE';
    default:
      throw new Error(`Invalid trigger event: ${event}`);
  }
}

/**
 * Count statements in trigger body (separated by semicolons)
 */
function countStatements(body: string): number {
  // Simple heuristic: count semicolons not inside strings
  let count = 0;
  let inString = false;
  let stringChar = '';

  for (let i = 0; i < body.length; i++) {
    const char = body[i];

    if (inString) {
      if (char === stringChar && body[i - 1] !== '\\') {
        inString = false;
      }
    } else {
      if (char === "'" || char === '"') {
        inString = true;
        stringChar = char;
      } else if (char === ';') {
        count++;
      }
    }
  }

  // If no semicolons found, there's at least one statement
  return count || 1;
}

/**
 * Extract WHEN clause from the matched group
 */
function extractWhenClause(whenMatch: string | undefined): string | undefined {
  if (!whenMatch) return undefined;

  // Remove the WHEN keyword and trim
  return whenMatch.replace(/^WHEN\s+/i, '').trim();
}

// =============================================================================
// MAIN PARSER FUNCTIONS
// =============================================================================

/**
 * Parse a CREATE TRIGGER SQL statement
 *
 * @param sql - The SQL statement to parse
 * @returns Parsed trigger definition
 * @throws Error if the SQL is not a valid CREATE TRIGGER statement
 */
export function parseTrigger(sql: string): ParsedSQLTrigger {
  const normalizedSql = sql.replace(/\r\n/g, '\n').trim();

  const match = normalizedSql.match(CREATE_TRIGGER_REGEX);

  if (!match) {
    throw new Error(`Invalid CREATE TRIGGER syntax: ${sql.slice(0, 100)}...`);
  }

  const [
    , // full match
    tempKeyword,
    ifNotExists,
    triggerName,
    timing,
    event,
    updateOfColumns,
    tableName,
    whenClause,
    body,
  ] = match;

  // Parse schema-qualified names
  const triggerParts = parseQualifiedName(triggerName);
  const tableParts = parseQualifiedName(tableName);

  const parsedTiming = normalizeTiming(timing);
  const parsedEvent = normalizeEvent(event);
  const parsedColumns = parseColumns(updateOfColumns);
  const parsedWhenClause = extractWhenClause(whenClause);

  // Detect NEW and OLD references
  const referencesNew = NEW_REFERENCE_REGEX.test(body) ||
    (parsedWhenClause ? NEW_REFERENCE_REGEX.test(parsedWhenClause) : false);
  const referencesOld = OLD_REFERENCE_REGEX.test(body) ||
    (parsedWhenClause ? OLD_REFERENCE_REGEX.test(parsedWhenClause) : false);

  return {
    name: triggerParts.name,
    schema: triggerParts.schema,
    table: tableParts.name,
    tableSchema: tableParts.schema,
    timing: parsedTiming,
    event: parsedEvent,
    events: [parsedEvent], // For compatibility with existing types
    columns: parsedColumns,
    whenClause: parsedWhenClause,
    body: body.trim(),
    forEachRow: true, // SQLite only supports FOR EACH ROW
    ifNotExists: !!ifNotExists,
    temporary: !!tempKeyword,
    referencesNew,
    referencesOld,
    statementCount: countStatements(body),
    rawSql: sql,
  };
}

/**
 * Try to parse a CREATE TRIGGER statement, returning a result object
 */
export function tryParseTrigger(sql: string): {
  success: true;
  trigger: ParsedSQLTrigger;
} | {
  success: false;
  error: { message: string; position?: number };
} {
  try {
    const trigger = parseTrigger(sql);
    return { success: true, trigger };
  } catch (error) {
    return {
      success: false,
      error: {
        message: error instanceof Error ? error.message : String(error),
      },
    };
  }
}

/**
 * Check if a SQL statement is a CREATE TRIGGER statement
 */
export function isCreateTrigger(sql: string): boolean {
  return IS_CREATE_TRIGGER_REGEX.test(sql.trim());
}

/**
 * Check if a SQL statement is a DROP TRIGGER statement
 */
export function isDropTrigger(sql: string): boolean {
  return IS_DROP_TRIGGER_REGEX.test(sql.trim());
}

/**
 * Parse a DROP TRIGGER SQL statement
 */
export function parseDropTrigger(sql: string): DropTriggerStatement {
  const match = sql.match(DROP_TRIGGER_REGEX);

  if (!match) {
    throw new Error(`Invalid DROP TRIGGER syntax: ${sql}`);
  }

  const [, ifExists, triggerName] = match;
  const nameParts = parseQualifiedName(triggerName);

  return {
    name: nameParts.name,
    schema: nameParts.schema,
    ifExists: !!ifExists,
  };
}

/**
 * Try to parse a DROP TRIGGER statement
 */
export function tryParseDropTrigger(sql: string): {
  success: true;
  statement: DropTriggerStatement;
} | {
  success: false;
  error: { message: string };
} {
  try {
    const statement = parseDropTrigger(sql);
    return { success: true, statement };
  } catch (error) {
    return {
      success: false,
      error: {
        message: error instanceof Error ? error.message : String(error),
      },
    };
  }
}

// =============================================================================
// VALIDATION FUNCTIONS
// =============================================================================

/**
 * Validate a trigger definition
 */
export function validateTriggerDefinition(trigger: ParsedSQLTrigger): {
  valid: boolean;
  errors: string[];
} {
  const errors: string[] = [];

  // Name validation
  if (!trigger.name || trigger.name.trim().length === 0) {
    errors.push('Trigger name is required');
  }

  // Table validation
  if (!trigger.table || trigger.table.trim().length === 0) {
    errors.push('Table name is required');
  }

  // Timing validation
  if (!['BEFORE', 'AFTER', 'INSTEAD OF'].includes(trigger.timing)) {
    errors.push(`Invalid timing: ${trigger.timing}`);
  }

  // Event validation
  if (!['INSERT', 'UPDATE', 'DELETE'].includes(trigger.event)) {
    errors.push(`Invalid event: ${trigger.event}`);
  }

  // INSTEAD OF can only be used with views
  // (This is a semantic check that would need runtime validation)

  // UPDATE OF columns only valid for UPDATE event
  if (trigger.columns && trigger.columns.length > 0 && trigger.event !== 'UPDATE') {
    errors.push('UPDATE OF columns can only be specified for UPDATE triggers');
  }

  // Body validation
  if (!trigger.body || trigger.body.trim().length === 0) {
    errors.push('Trigger body is required');
  }

  // NEW reference validation for DELETE triggers
  if (trigger.event === 'DELETE' && trigger.referencesNew) {
    errors.push('NEW cannot be referenced in DELETE triggers');
  }

  // OLD reference validation for INSERT triggers
  if (trigger.event === 'INSERT' && trigger.referencesOld) {
    errors.push('OLD cannot be referenced in INSERT triggers');
  }

  return {
    valid: errors.length === 0,
    errors,
  };
}

/**
 * Check if two triggers would conflict (same name, table, timing, and event)
 */
export function triggersConflict(a: ParsedSQLTrigger, b: ParsedSQLTrigger): boolean {
  return (
    a.name === b.name &&
    a.table === b.table &&
    a.timing === b.timing &&
    a.event === b.event
  );
}

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

/**
 * Generate CREATE TRIGGER SQL from a parsed trigger
 */
export function generateTriggerSql(trigger: ParsedSQLTrigger): string {
  const parts: string[] = ['CREATE'];

  if (trigger.temporary) {
    parts.push('TEMP');
  }

  parts.push('TRIGGER');

  if (trigger.ifNotExists) {
    parts.push('IF NOT EXISTS');
  }

  // Trigger name with optional schema
  if (trigger.schema) {
    parts.push(`${trigger.schema}.${trigger.name}`);
  } else {
    parts.push(trigger.name);
  }

  parts.push(trigger.timing);
  parts.push(trigger.event);

  if (trigger.columns && trigger.columns.length > 0) {
    parts.push(`OF ${trigger.columns.join(', ')}`);
  }

  parts.push('ON');

  // Table name with optional schema
  if (trigger.tableSchema) {
    parts.push(`${trigger.tableSchema}.${trigger.table}`);
  } else {
    parts.push(trigger.table);
  }

  if (trigger.forEachRow) {
    parts.push('FOR EACH ROW');
  }

  if (trigger.whenClause) {
    parts.push(`WHEN ${trigger.whenClause}`);
  }

  parts.push('BEGIN');
  parts.push(trigger.body);
  parts.push('END;');

  return parts.join(' ');
}

/**
 * Extract table names referenced in trigger body
 */
export function extractReferencedTables(body: string): string[] {
  const tables: Set<string> = new Set();

  // Match INSERT INTO, UPDATE, DELETE FROM, SELECT FROM patterns
  const patterns = [
    /INSERT\s+INTO\s+(["`]?[\w.-]+["`]?)/gi,
    /UPDATE\s+(["`]?[\w.-]+["`]?)/gi,
    /DELETE\s+FROM\s+(["`]?[\w.-]+["`]?)/gi,
    /FROM\s+(["`]?[\w.-]+["`]?)/gi,
    /JOIN\s+(["`]?[\w.-]+["`]?)/gi,
  ];

  for (const pattern of patterns) {
    let match;
    while ((match = pattern.exec(body)) !== null) {
      const tableName = unquoteIdentifier(match[1]);
      tables.add(tableName);
    }
  }

  return Array.from(tables);
}

/**
 * Check if a trigger body contains potentially dangerous operations
 */
export function analyzeTriggerRisks(trigger: ParsedSQLTrigger): {
  hasRecursiveRisk: boolean;
  modifiesTriggerTable: boolean;
  referencedTables: string[];
} {
  const referencedTables = extractReferencedTables(trigger.body);
  const modifiesTriggerTable = referencedTables.includes(trigger.table);

  // Check for recursive risk (AFTER trigger that modifies its own table)
  const hasRecursiveRisk =
    trigger.timing === 'AFTER' && modifiesTriggerTable;

  return {
    hasRecursiveRisk,
    modifiesTriggerTable,
    referencedTables,
  };
}
