/**
 * SQL Expression Evaluation Utilities
 *
 * Provides utilities for evaluating SQL expressions in JavaScript,
 * particularly for WHEN clauses in triggers and other conditional logic.
 */

// =============================================================================
// WHEN CLAUSE EVALUATION
// =============================================================================

/**
 * Evaluate a SQL WHEN clause condition.
 *
 * This function converts SQL-style expressions to JavaScript and evaluates them.
 * It supports:
 * - NEW.column and OLD.column references
 * - LIKE and NOT LIKE patterns
 * - IS NULL and IS NOT NULL checks
 * - AND, OR, NOT operators
 * - Comparison operators (=, !=, <>, <, >, <=, >=)
 *
 * @param whenClause - The SQL WHEN clause expression
 * @param oldRow - The OLD row values (for UPDATE/DELETE triggers)
 * @param newRow - The NEW row values (for INSERT/UPDATE triggers)
 * @returns true if the condition is met, false otherwise
 *
 * @example
 * ```ts
 * // Check if email changed
 * const result = evaluateWhenClause(
 *   "NEW.email != OLD.email",
 *   { email: "old@example.com" },
 *   { email: "new@example.com" }
 * );
 * // result: true
 *
 * // Check with LIKE pattern
 * const valid = evaluateWhenClause(
 *   "NEW.email LIKE '%@%'",
 *   undefined,
 *   { email: "test@example.com" }
 * );
 * // valid: true
 * ```
 */
export function evaluateWhenClause<T extends Record<string, unknown>>(
  whenClause: string,
  oldRow: T | undefined,
  newRow: T | undefined
): boolean {
  if (!whenClause) return true;

  try {
    // Create a simple expression evaluator for WHEN clauses
    // Replace NEW.col and OLD.col with actual values
    let expr = whenClause;

    // Replace NEW references
    if (newRow) {
      for (const [key, value] of Object.entries(newRow)) {
        const pattern = new RegExp(`\\bNEW\\.${key}\\b`, 'gi');
        const replacement = formatSqlValue(value);
        expr = expr.replace(pattern, replacement);
      }
    }

    // Replace OLD references
    if (oldRow) {
      for (const [key, value] of Object.entries(oldRow)) {
        const pattern = new RegExp(`\\bOLD\\.${key}\\b`, 'gi');
        const replacement = formatSqlValue(value);
        expr = expr.replace(pattern, replacement);
      }
    }

    // Handle NOT LIKE pattern (convert to regex test)
    // e.g., 'invalid' NOT LIKE '%@%' => !/.*@.*/.test('invalid')
    expr = expr.replace(
      /(['"]\w+['"]|\w+)\s+NOT\s+LIKE\s+['"](%?)([^%'"]+)(%?)['"]/gi,
      (_, val, prefix, pattern, suffix) => {
        const regexPattern = (prefix ? '.*' : '') + escapeRegExp(pattern) + (suffix ? '.*' : '');
        return `!/${regexPattern}/.test(${val})`;
      }
    );

    // Handle LIKE pattern (convert to regex test)
    // e.g., 'test@example.com' LIKE '%@%' => /.*@.*/.test('test@example.com')
    expr = expr.replace(
      /(['"]\w+['"]|\w+)\s+LIKE\s+['"](%?)([^%'"]+)(%?)['"]/gi,
      (_, val, prefix, pattern, suffix) => {
        const regexPattern = (prefix ? '.*' : '') + escapeRegExp(pattern) + (suffix ? '.*' : '');
        return `/${regexPattern}/.test(${val})`;
      }
    );

    // Handle IS NULL
    expr = expr.replace(/\bIS\s+NULL\b/gi, '=== null');
    expr = expr.replace(/\bIS\s+NOT\s+NULL\b/gi, '!== null');

    // Handle AND/OR
    expr = expr.replace(/\bAND\b/gi, '&&');
    expr = expr.replace(/\bOR\b/gi, '||');

    // Handle NOT (but not NOT IN or NOT LIKE which are handled above)
    expr = expr.replace(/\bNOT\s+(?!IN\b)/gi, '!');

    // Handle SQL equality operators
    expr = expr.replace(/!=/g, '!==');
    expr = expr.replace(/(?<![!<>=])=(?!=)/g, '===');
    expr = expr.replace(/<>/g, '!==');

    // Evaluate the expression (simplified - production would need proper parser)
    // eslint-disable-next-line no-new-func
    const result = new Function(`return ${expr}`)();
    return Boolean(result);
  } catch {
    // If evaluation fails, default to true (fire the trigger)
    return true;
  }
}

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

/**
 * Format a value for use in SQL-to-JS expression conversion.
 */
function formatSqlValue(value: unknown): string {
  if (value === null || value === undefined) {
    return 'null';
  }
  if (typeof value === 'string') {
    return `'${value}'`;
  }
  return String(value);
}

/**
 * Escape special regex characters in a string.
 */
function escapeRegExp(str: string): string {
  return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

// =============================================================================
// ADDITIONAL SQL EVALUATION UTILITIES
// =============================================================================

/**
 * Check if two SQL values are equal using SQL semantics.
 *
 * In SQL, NULL = NULL is false (NULL is unknown), but this function
 * treats NULL = NULL as true for practical comparison purposes.
 * Use evaluateWhenClause for proper SQL NULL handling.
 *
 * @param a - First value
 * @param b - Second value
 * @returns true if values are equal
 */
export function sqlEquals(a: unknown, b: unknown): boolean {
  // Handle null comparisons
  if (a === null && b === null) return true;
  if (a === null || b === null) return false;

  // Handle different types
  if (typeof a !== typeof b) {
    // Try type coercion for numeric comparisons
    if (typeof a === 'number' && typeof b === 'string') {
      return a === Number(b);
    }
    if (typeof a === 'string' && typeof b === 'number') {
      return Number(a) === b;
    }
    return false;
  }

  return a === b;
}

/**
 * Evaluate a LIKE pattern match.
 *
 * @param value - The value to test
 * @param pattern - The SQL LIKE pattern (using % and _ wildcards)
 * @returns true if the value matches the pattern
 */
export function sqlLike(value: string, pattern: string): boolean {
  // Convert SQL LIKE pattern to regex
  const regexPattern = pattern
    .replace(/[.*+?^${}()|[\]\\]/g, '\\$&') // Escape regex chars
    .replace(/%/g, '.*') // % matches any sequence
    .replace(/_/g, '.'); // _ matches any single char

  const regex = new RegExp(`^${regexPattern}$`, 'i');
  return regex.test(value);
}

/**
 * Check if a value is truthy in SQL semantics.
 *
 * In SQL:
 * - 0 is false
 * - Empty string is typically truthy (varies by DB)
 * - NULL is neither true nor false
 * - Non-zero numbers are true
 *
 * @param value - The value to check
 * @returns true if the value is truthy in SQL semantics
 */
export function sqlTruthy(value: unknown): boolean {
  if (value === null || value === undefined) return false;
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  if (typeof value === 'string') return value.length > 0;
  return true;
}
