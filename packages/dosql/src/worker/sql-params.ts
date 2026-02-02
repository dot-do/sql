/**
 * SQL Parameter Substitution (SQL Injection Prevention)
 *
 * Extracted from database.ts to reduce the size of the main DoSQLDatabase class.
 * Handles safe parameter binding and SQL value parsing.
 */

/**
 * Safely escape a value for embedding in a SQL string.
 * Strings are wrapped in single quotes with internal single quotes doubled.
 */
export function escapeSqlValue(value: unknown): string {
  if (value === null || value === undefined) {
    return 'NULL';
  }
  if (typeof value === 'number') {
    if (!isFinite(value)) {
      return 'NULL';
    }
    return String(value);
  }
  if (typeof value === 'boolean') {
    return value ? '1' : '0';
  }
  // String values: wrap in single quotes, escape internal single quotes by doubling
  const str = String(value);
  return "'" + str.replace(/'/g, "''") + "'";
}

/**
 * Substitute named parameters (:name) in a SQL string with safely escaped values.
 * Only substitutes outside of string literals and quoted identifiers to avoid
 * corrupting literal content or identifier names.
 *
 * Handles all common SQL quoting styles:
 * - Single quotes ('value') - string literals
 * - Double quotes ("identifier") - ANSI SQL identifiers
 * - Backticks (`identifier`) - MySQL/MariaDB identifiers
 * - Square brackets ([identifier]) - SQL Server identifiers
 */
export function substituteParams(sql: string, params: Record<string, unknown>): string {
  let result = '';
  let i = 0;

  while (i < sql.length) {
    // Handle string literals (single-quoted) - skip over them
    if (sql[i] === "'") {
      result += "'";
      i++;
      while (i < sql.length) {
        if (sql[i] === "'" && sql[i + 1] === "'") {
          result += "''";
          i += 2;
        } else if (sql[i] === "'") {
          result += "'";
          i++;
          break;
        } else {
          result += sql[i];
          i++;
        }
      }
      continue;
    }

    // Handle double-quoted identifiers (ANSI SQL) - skip over them
    if (sql[i] === '"') {
      result += '"';
      i++;
      while (i < sql.length) {
        if (sql[i] === '"' && sql[i + 1] === '"') {
          // Escaped double quote inside identifier
          result += '""';
          i += 2;
        } else if (sql[i] === '"') {
          result += '"';
          i++;
          break;
        } else {
          result += sql[i];
          i++;
        }
      }
      continue;
    }

    // Handle backtick-quoted identifiers (MySQL/MariaDB) - skip over them
    if (sql[i] === '`') {
      result += '`';
      i++;
      while (i < sql.length) {
        if (sql[i] === '`' && sql[i + 1] === '`') {
          // Escaped backtick inside identifier
          result += '``';
          i += 2;
        } else if (sql[i] === '`') {
          result += '`';
          i++;
          break;
        } else {
          result += sql[i];
          i++;
        }
      }
      continue;
    }

    // Handle bracket-quoted identifiers (SQL Server) - skip over them
    if (sql[i] === '[') {
      result += '[';
      i++;
      while (i < sql.length) {
        if (sql[i] === ']' && sql[i + 1] === ']') {
          // Escaped bracket inside identifier
          result += ']]';
          i += 2;
        } else if (sql[i] === ']') {
          result += ']';
          i++;
          break;
        } else {
          result += sql[i];
          i++;
        }
      }
      continue;
    }

    // Handle named parameters outside of string literals and quoted identifiers
    if (sql[i] === ':') {
      const paramMatch = sql.substring(i).match(/^:(\w+)/);
      if (paramMatch) {
        const paramName = paramMatch[1];
        if (paramName in params) {
          result += escapeSqlValue(params[paramName]);
          i += paramMatch[0].length;
          continue;
        }
      }
    }

    result += sql[i];
    i++;
  }

  return result;
}

/**
 * Parse a SQL value token, handling escaped single quotes ('').
 * For string literals, unescapes doubled quotes to single quotes.
 */
export function parseSqlValue(trimmed: string): unknown {
  // Parse string literals with escaped quote support
  if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
    return trimmed.slice(1, -1).replace(/''/g, "'");
  }
  // Parse numbers
  const num = Number(trimmed);
  if (!isNaN(num) && trimmed !== '') return num;
  // Parse null
  if (trimmed.toUpperCase() === 'NULL') return null;
  return trimmed;
}
