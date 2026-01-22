/**
 * Collation Registry for DoSQL
 *
 * Manages registered collations including:
 * - Built-in collations (BINARY, NOCASE, RTRIM)
 * - Custom user-defined collations
 * - CREATE COLLATION / DROP COLLATION support
 * - Collation lookup and validation
 */

import type {
  CollationFunction,
  CollationDefinition,
  CreateCollationOptions,
  CollateClause,
  ColumnCollation,
  IndexCollation,
  CreateCollationStatement,
  DropCollationStatement,
  UnicodeCollationOptions,
} from './types.js';
import {
  CollationNotFoundError,
  CollationExistsError,
  BuiltinCollationError,
} from './types.js';
import {
  BUILTIN_COLLATIONS,
  isBuiltinCollation,
  createUnicodeCollation,
} from './builtin.js';

/**
 * Collation registry interface
 */
export interface CollationRegistry {
  /** Get a collation by name (case-insensitive) */
  get(name: string): CollationDefinition | undefined;

  /** Check if a collation exists */
  has(name: string): boolean;

  /** Register a custom collation */
  register(options: CreateCollationOptions): CollationDefinition;

  /** Remove a custom collation */
  remove(name: string): boolean;

  /** List all registered collations */
  list(): CollationDefinition[];

  /** List only custom (non-built-in) collations */
  listCustom(): CollationDefinition[];

  /** Compare two strings using a named collation */
  compare(name: string, a: string, b: string): number;

  /** Get the default collation (BINARY) */
  getDefault(): CollationDefinition;

  /** Clear all custom collations */
  clearCustom(): void;
}

/**
 * Creates a new collation registry with built-in collations pre-registered.
 *
 * @returns A new CollationRegistry instance
 */
export function createCollationRegistry(): CollationRegistry {
  // Custom collations storage (case-insensitive keys)
  const customCollations = new Map<string, CollationDefinition>();

  /**
   * Normalizes collation name for lookup (uppercase)
   */
  function normalizeName(name: string): string {
    return name.toUpperCase().trim();
  }

  const registry: CollationRegistry = {
    get(name: string): CollationDefinition | undefined {
      const normalized = normalizeName(name);

      // Check built-ins first
      const builtin = BUILTIN_COLLATIONS.get(normalized);
      if (builtin) {
        return builtin;
      }

      // Then check custom
      return customCollations.get(normalized);
    },

    has(name: string): boolean {
      const normalized = normalizeName(name);
      return BUILTIN_COLLATIONS.has(normalized) || customCollations.has(normalized);
    },

    register(options: CreateCollationOptions): CollationDefinition {
      const normalized = normalizeName(options.name);

      // Cannot override built-in collations
      if (isBuiltinCollation(options.name)) {
        throw new BuiltinCollationError(options.name);
      }

      // Check for existing custom collation
      if (customCollations.has(normalized) && !options.replace) {
        throw new CollationExistsError(options.name);
      }

      const definition: CollationDefinition = {
        name: normalized,
        compare: options.compare,
        builtin: false,
        description: options.description,
        // Assume custom collations are case-sensitive and respect trailing spaces
        // unless the description suggests otherwise
        caseSensitive: true,
        respectsTrailingSpaces: true,
      };

      customCollations.set(normalized, definition);
      return definition;
    },

    remove(name: string): boolean {
      const normalized = normalizeName(name);

      // Cannot remove built-in collations
      if (isBuiltinCollation(name)) {
        throw new BuiltinCollationError(name);
      }

      return customCollations.delete(normalized);
    },

    list(): CollationDefinition[] {
      return [
        ...Array.from(BUILTIN_COLLATIONS.values()),
        ...Array.from(customCollations.values()),
      ];
    },

    listCustom(): CollationDefinition[] {
      return Array.from(customCollations.values());
    },

    compare(name: string, a: string, b: string): number {
      const collation = registry.get(name);
      if (!collation) {
        throw new CollationNotFoundError(name);
      }
      return collation.compare(a, b);
    },

    getDefault(): CollationDefinition {
      return BUILTIN_COLLATIONS.get('BINARY')!;
    },

    clearCustom(): void {
      customCollations.clear();
    },
  };

  return registry;
}

// =============================================================================
// SQL PARSING UTILITIES
// =============================================================================

/**
 * Parses a COLLATE clause from a SQL expression.
 *
 * Examples:
 * - "COLLATE NOCASE"
 * - "COLLATE BINARY"
 * - "COLLATE my_custom_collation"
 *
 * @param sql The SQL text containing a COLLATE clause
 * @returns The parsed CollateClause or null if not found
 */
export function parseCollateClause(sql: string): CollateClause | null {
  const regex = /\bCOLLATE\s+([a-zA-Z_][a-zA-Z0-9_]*|"[^"]+"|'[^']+')/i;
  const match = sql.match(regex);

  if (!match) {
    return null;
  }

  let collationName = match[1];

  // Remove quotes if present
  if ((collationName.startsWith('"') && collationName.endsWith('"')) ||
      (collationName.startsWith("'") && collationName.endsWith("'"))) {
    collationName = collationName.slice(1, -1);
  }

  return {
    collationName,
    fromColumnDef: false,
    fromExpression: true,
  };
}

/**
 * Parses a column definition to extract collation.
 *
 * Example: "name TEXT COLLATE NOCASE"
 *
 * @param columnDef The column definition SQL
 * @returns The ColumnCollation or null if no collation specified
 */
export function parseColumnCollation(columnDef: string): ColumnCollation | null {
  // Match column name and optional COLLATE clause
  const regex = /^([a-zA-Z_][a-zA-Z0-9_]*|"[^"]+"|`[^`]+`)\s+\w+.*?\bCOLLATE\s+([a-zA-Z_][a-zA-Z0-9_]*|"[^"]+"|'[^']+')/i;
  const match = columnDef.match(regex);

  if (!match) {
    // Try to at least extract the column name
    const nameRegex = /^([a-zA-Z_][a-zA-Z0-9_]*|"[^"]+"|`[^`]+`)/;
    const nameMatch = columnDef.match(nameRegex);
    if (nameMatch) {
      return {
        column: stripQuotes(nameMatch[1]),
        collation: null,
      };
    }
    return null;
  }

  return {
    column: stripQuotes(match[1]),
    collation: stripQuotes(match[2]),
  };
}

/**
 * Checks if a SQL statement is a CREATE COLLATION statement.
 *
 * @param sql The SQL statement
 * @returns True if it's a CREATE COLLATION statement
 */
export function isCreateCollation(sql: string): boolean {
  return /^\s*CREATE\s+COLLATION\b/i.test(sql);
}

/**
 * Parses a CREATE COLLATION statement.
 *
 * Note: SQLite doesn't have CREATE COLLATION syntax; this is a DoSQL extension.
 * Example: CREATE COLLATION my_collation AS UNICODE('en_US', STRENGTH='secondary')
 *
 * @param sql The SQL statement
 * @returns The parsed statement
 */
export function parseCreateCollation(sql: string): CreateCollationStatement {
  // Basic pattern: CREATE COLLATION name ...
  const nameMatch = sql.match(/^\s*CREATE\s+COLLATION\s+([a-zA-Z_][a-zA-Z0-9_]*|"[^"]+"|'[^']+')/i);

  if (!nameMatch) {
    throw new Error('Invalid CREATE COLLATION syntax');
  }

  const name = stripQuotes(nameMatch[1]);

  // Check for UNICODE(...) clause
  const unicodeMatch = sql.match(/\bUNICODE\s*\(\s*'([^']+)'/i);

  const result: CreateCollationStatement = {
    type: 'CREATE_COLLATION',
    name,
  };

  if (unicodeMatch) {
    const options: UnicodeCollationOptions = {
      locale: unicodeMatch[1],
    };

    // Parse additional options
    const strengthMatch = sql.match(/\bSTRENGTH\s*=\s*'?(primary|secondary|tertiary|identical)'?/i);
    if (strengthMatch) {
      options.strength = strengthMatch[1].toLowerCase() as UnicodeCollationOptions['strength'];
    }

    const numericMatch = sql.match(/\bNUMERIC\s*=\s*(true|false|1|0)/i);
    if (numericMatch) {
      options.numeric = numericMatch[1].toLowerCase() === 'true' || numericMatch[1] === '1';
    }

    const caseFirstMatch = sql.match(/\bCASE_FIRST\s*=\s*'?(upper|lower|off)'?/i);
    if (caseFirstMatch) {
      options.caseFirst = caseFirstMatch[1].toLowerCase() as UnicodeCollationOptions['caseFirst'];
    }

    result.options = options;
  }

  // Check for base collation
  const asMatch = sql.match(/\bAS\s+([a-zA-Z_][a-zA-Z0-9_]*)/i);
  if (asMatch && !unicodeMatch) {
    result.baseCollation = asMatch[1].toUpperCase();
  }

  return result;
}

/**
 * Checks if a SQL statement is a DROP COLLATION statement.
 *
 * @param sql The SQL statement
 * @returns True if it's a DROP COLLATION statement
 */
export function isDropCollation(sql: string): boolean {
  return /^\s*DROP\s+COLLATION\b/i.test(sql);
}

/**
 * Parses a DROP COLLATION statement.
 *
 * Example: DROP COLLATION IF EXISTS my_collation
 *
 * @param sql The SQL statement
 * @returns The parsed statement
 */
export function parseDropCollation(sql: string): DropCollationStatement {
  const ifExistsMatch = sql.match(/\bIF\s+EXISTS\b/i);
  const nameMatch = sql.match(/DROP\s+COLLATION\s+(?:IF\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*|"[^"]+"|'[^']+')/i);

  if (!nameMatch) {
    throw new Error('Invalid DROP COLLATION syntax');
  }

  return {
    type: 'DROP_COLLATION',
    name: stripQuotes(nameMatch[1]),
    ifExists: !!ifExistsMatch,
  };
}

/**
 * Parses an INDEX definition to extract column collations.
 *
 * Example: CREATE INDEX idx ON table (name COLLATE NOCASE, email COLLATE NOCASE DESC)
 *
 * @param sql The CREATE INDEX statement
 * @returns The parsed IndexCollation or null
 */
export function parseIndexCollation(sql: string): IndexCollation | null {
  // Match CREATE INDEX ... ON table (columns)
  const indexMatch = sql.match(/CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z_][a-zA-Z0-9_]*|"[^"]+"|`[^`]+`)\s+ON\s+([a-zA-Z_][a-zA-Z0-9_]*|"[^"]+"|`[^`]+`)\s*\(([^)]+)\)/i);

  if (!indexMatch) {
    return null;
  }

  const indexName = stripQuotes(indexMatch[1]);
  const tableName = stripQuotes(indexMatch[2]);
  const columnsStr = indexMatch[3];

  // Parse individual columns
  const columns: IndexCollation['columns'] = [];
  const columnParts = splitColumnList(columnsStr);

  for (const part of columnParts) {
    const colMatch = part.match(/([a-zA-Z_][a-zA-Z0-9_]*|"[^"]+"|`[^`]+`)(?:\s+COLLATE\s+([a-zA-Z_][a-zA-Z0-9_]*|"[^"]+"?))?(?:\s+(ASC|DESC))?/i);

    if (colMatch) {
      columns.push({
        name: stripQuotes(colMatch[1]),
        collation: colMatch[2] ? stripQuotes(colMatch[2]) : null,
        sortOrder: (colMatch[3]?.toUpperCase() as 'ASC' | 'DESC') || 'ASC',
      });
    }
  }

  return {
    indexName,
    tableName,
    columns,
  };
}

/**
 * Strips quotes from an identifier.
 */
function stripQuotes(identifier: string): string {
  if ((identifier.startsWith('"') && identifier.endsWith('"')) ||
      (identifier.startsWith("'") && identifier.endsWith("'")) ||
      (identifier.startsWith('`') && identifier.endsWith('`'))) {
    return identifier.slice(1, -1);
  }
  return identifier;
}

/**
 * Splits a column list, respecting nested parentheses.
 */
function splitColumnList(columnsStr: string): string[] {
  const columns: string[] = [];
  let current = '';
  let depth = 0;

  for (const char of columnsStr) {
    if (char === '(') {
      depth++;
      current += char;
    } else if (char === ')') {
      depth--;
      current += char;
    } else if (char === ',' && depth === 0) {
      columns.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }

  if (current.trim()) {
    columns.push(current.trim());
  }

  return columns;
}

// =============================================================================
// COLLATION APPLICATION UTILITIES
// =============================================================================

/**
 * Applies a collation to sort an array of strings.
 *
 * @param items The array of strings to sort
 * @param collationName The collation to use
 * @param registry The collation registry
 * @param descending Whether to sort in descending order
 * @returns A new sorted array
 */
export function sortWithCollation(
  items: string[],
  collationName: string,
  registry: CollationRegistry,
  descending: boolean = false
): string[] {
  const collation = registry.get(collationName);
  if (!collation) {
    throw new CollationNotFoundError(collationName);
  }

  const sorted = [...items].sort((a, b) => collation.compare(a, b));
  return descending ? sorted.reverse() : sorted;
}

/**
 * Checks equality of two strings using a collation.
 *
 * @param a First string
 * @param b Second string
 * @param collationName The collation to use
 * @param registry The collation registry
 * @returns True if strings are equal according to the collation
 */
export function equalsWithCollation(
  a: string,
  b: string,
  collationName: string,
  registry: CollationRegistry
): boolean {
  return registry.compare(collationName, a, b) === 0;
}

/**
 * Creates a collation-aware string comparator for use with Array.sort().
 *
 * @param collationName The collation to use
 * @param registry The collation registry
 * @param descending Whether to sort in descending order
 * @returns A comparator function
 */
export function createCollationComparator(
  collationName: string,
  registry: CollationRegistry,
  descending: boolean = false
): (a: string, b: string) => number {
  const collation = registry.get(collationName);
  if (!collation) {
    throw new CollationNotFoundError(collationName);
  }

  return (a: string, b: string): number => {
    const result = collation.compare(a, b);
    return descending ? -result : result;
  };
}

/**
 * Registers a Unicode collation in the registry.
 *
 * @param registry The collation registry
 * @param name The collation name
 * @param options Unicode collation options
 * @returns The registered collation definition
 */
export function registerUnicodeCollation(
  registry: CollationRegistry,
  name: string,
  options: UnicodeCollationOptions
): CollationDefinition {
  return registry.register({
    name,
    compare: createUnicodeCollation(options),
    description: `Unicode collation for ${options.locale} (strength: ${options.strength || 'default'})`,
  });
}
