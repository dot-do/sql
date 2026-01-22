/**
 * CREATE PROCEDURE SQL Parser
 *
 * Parses CREATE PROCEDURE statements that contain ESM module code.
 *
 * Syntax:
 *   CREATE [OR REPLACE] PROCEDURE name [(params)]
 *   [RETURNS type]
 *   AS MODULE $$
 *     // ESM code here
 *     export default async (ctx, ...args) => { ... }
 *   $$;
 */

import type { ParsedProcedure, ParseError, Schema } from './types.js';

// =============================================================================
// TOKEN TYPES
// =============================================================================

interface Token {
  type: 'KEYWORD' | 'IDENTIFIER' | 'STRING' | 'NUMBER' | 'SYMBOL' | 'MODULE_CODE' | 'EOF';
  value: string;
  position: number;
  line: number;
  column: number;
}

// =============================================================================
// LEXER
// =============================================================================

const KEYWORDS = new Set([
  'CREATE', 'OR', 'REPLACE', 'PROCEDURE', 'FUNCTION', 'AS', 'MODULE',
  'RETURNS', 'IN', 'OUT', 'INOUT', 'DEFAULT', 'LANGUAGE', 'BEGIN', 'END',
]);

/**
 * Tokenize a CREATE PROCEDURE statement
 */
function tokenize(input: string): Token[] {
  const tokens: Token[] = [];
  let pos = 0;
  let line = 1;
  let column = 1;

  function advance(count = 1): void {
    for (let i = 0; i < count; i++) {
      if (input[pos] === '\n') {
        line++;
        column = 1;
      } else {
        column++;
      }
      pos++;
    }
  }

  function peek(offset = 0): string {
    return input[pos + offset] ?? '';
  }

  function skipWhitespace(): void {
    while (pos < input.length && /\s/.test(peek())) {
      advance();
    }
  }

  function skipLineComment(): void {
    // Skip --
    advance(2);
    while (pos < input.length && peek() !== '\n') {
      advance();
    }
  }

  function skipBlockComment(): void {
    // Skip /*
    advance(2);
    while (pos < input.length) {
      if (peek() === '*' && peek(1) === '/') {
        advance(2);
        return;
      }
      advance();
    }
  }

  while (pos < input.length) {
    skipWhitespace();

    if (pos >= input.length) break;

    const startPos = pos;
    const startLine = line;
    const startColumn = column;
    const char = peek();

    // Skip comments
    if (char === '-' && peek(1) === '-') {
      skipLineComment();
      continue;
    }

    if (char === '/' && peek(1) === '*') {
      skipBlockComment();
      continue;
    }

    // Module code delimiter: $$
    if (char === '$' && peek(1) === '$') {
      advance(2); // Skip opening $$

      // Find the closing $$
      let code = '';
      const codeStartPos = pos;
      while (pos < input.length) {
        if (peek() === '$' && peek(1) === '$') {
          break;
        }
        code += peek();
        advance();
      }

      if (pos >= input.length) {
        throw new Error(`Unterminated module code at position ${codeStartPos}`);
      }

      advance(2); // Skip closing $$

      tokens.push({
        type: 'MODULE_CODE',
        value: code.trim(),
        position: startPos,
        line: startLine,
        column: startColumn,
      });
      continue;
    }

    // String literal (single quotes)
    if (char === "'") {
      advance(); // Skip opening quote
      let value = '';
      while (pos < input.length && peek() !== "'") {
        if (peek() === '\\') {
          advance();
          value += peek();
        } else {
          value += peek();
        }
        advance();
      }
      advance(); // Skip closing quote

      tokens.push({
        type: 'STRING',
        value,
        position: startPos,
        line: startLine,
        column: startColumn,
      });
      continue;
    }

    // Identifier or keyword
    if (/[a-zA-Z_]/.test(char)) {
      let value = '';
      while (pos < input.length && /[a-zA-Z0-9_]/.test(peek())) {
        value += peek();
        advance();
      }

      const upper = value.toUpperCase();
      tokens.push({
        type: KEYWORDS.has(upper) ? 'KEYWORD' : 'IDENTIFIER',
        value: KEYWORDS.has(upper) ? upper : value,
        position: startPos,
        line: startLine,
        column: startColumn,
      });
      continue;
    }

    // Number
    if (/[0-9]/.test(char)) {
      let value = '';
      while (pos < input.length && /[0-9.]/.test(peek())) {
        value += peek();
        advance();
      }

      tokens.push({
        type: 'NUMBER',
        value,
        position: startPos,
        line: startLine,
        column: startColumn,
      });
      continue;
    }

    // Symbols
    const symbols = ['(', ')', ',', ';', '=', '::', ':'];
    let matched = false;
    for (const sym of symbols) {
      if (input.slice(pos, pos + sym.length) === sym) {
        tokens.push({
          type: 'SYMBOL',
          value: sym,
          position: startPos,
          line: startLine,
          column: startColumn,
        });
        advance(sym.length);
        matched = true;
        break;
      }
    }

    if (!matched) {
      // Skip unknown characters
      advance();
    }
  }

  tokens.push({
    type: 'EOF',
    value: '',
    position: pos,
    line,
    column,
  });

  return tokens;
}

// =============================================================================
// PARSER
// =============================================================================

interface ParseContext {
  tokens: Token[];
  pos: number;
  input: string;
}

function currentToken(ctx: ParseContext): Token {
  return ctx.tokens[ctx.pos] ?? { type: 'EOF', value: '', position: 0, line: 0, column: 0 };
}

function advance(ctx: ParseContext): Token {
  const token = currentToken(ctx);
  ctx.pos++;
  return token;
}

function expect(ctx: ParseContext, type: Token['type'], value?: string): Token {
  const token = currentToken(ctx);
  if (token.type !== type || (value !== undefined && token.value.toUpperCase() !== value.toUpperCase())) {
    throw createParseError(
      `Expected ${value ?? type} but got ${token.value}`,
      token
    );
  }
  return advance(ctx);
}

function match(ctx: ParseContext, type: Token['type'], value?: string): boolean {
  const token = currentToken(ctx);
  return token.type === type && (value === undefined || token.value.toUpperCase() === value.toUpperCase());
}

function createParseError(message: string, token: Token): ParseError {
  return {
    message,
    position: token.position,
    line: token.line,
    column: token.column,
  };
}

/**
 * Parse parameter list: (param1 type, param2 type DEFAULT value, ...)
 */
function parseParameters(ctx: ParseContext): ParsedProcedure['parameters'] {
  if (!match(ctx, 'SYMBOL', '(')) {
    return undefined;
  }

  advance(ctx); // Skip (
  const params: NonNullable<ParsedProcedure['parameters']> = [];

  while (!match(ctx, 'SYMBOL', ')') && !match(ctx, 'EOF')) {
    // Skip IN/OUT/INOUT
    if (match(ctx, 'KEYWORD', 'IN') || match(ctx, 'KEYWORD', 'OUT') || match(ctx, 'KEYWORD', 'INOUT')) {
      advance(ctx);
    }

    // Parameter name
    const nameToken = expect(ctx, 'IDENTIFIER');
    const param: { name: string; type: string; defaultValue?: string } = {
      name: nameToken.value,
      type: 'any',
    };

    // Optional type
    if (match(ctx, 'IDENTIFIER') || match(ctx, 'KEYWORD')) {
      const typeToken = advance(ctx);
      param.type = typeToken.value;

      // Handle array types like TEXT[]
      if (match(ctx, 'SYMBOL', '[')) {
        advance(ctx);
        if (match(ctx, 'SYMBOL', ']')) {
          advance(ctx);
          param.type += '[]';
        }
      }
    }

    // Optional default value
    if (match(ctx, 'KEYWORD', 'DEFAULT') || match(ctx, 'SYMBOL', '=')) {
      advance(ctx);
      const valueToken = advance(ctx);
      param.defaultValue = valueToken.value;
    }

    params.push(param);

    // Skip comma
    if (match(ctx, 'SYMBOL', ',')) {
      advance(ctx);
    }
  }

  expect(ctx, 'SYMBOL', ')');
  return params;
}

/**
 * Parse return type: RETURNS type
 */
function parseReturnType(ctx: ParseContext): string | undefined {
  if (!match(ctx, 'KEYWORD', 'RETURNS')) {
    return undefined;
  }

  advance(ctx); // Skip RETURNS

  if (match(ctx, 'IDENTIFIER') || match(ctx, 'KEYWORD')) {
    const typeToken = advance(ctx);
    let returnType = typeToken.value;

    // Handle array types
    if (match(ctx, 'SYMBOL', '[')) {
      advance(ctx);
      if (match(ctx, 'SYMBOL', ']')) {
        advance(ctx);
        returnType += '[]';
      }
    }

    // Handle TABLE(...) return type
    if (returnType.toUpperCase() === 'TABLE') {
      if (match(ctx, 'SYMBOL', '(')) {
        advance(ctx);
        // Skip the table definition for now
        let depth = 1;
        while (depth > 0 && !match(ctx, 'EOF')) {
          if (match(ctx, 'SYMBOL', '(')) depth++;
          if (match(ctx, 'SYMBOL', ')')) depth--;
          advance(ctx);
        }
        returnType = 'TABLE';
      }
    }

    return returnType;
  }

  return undefined;
}

/**
 * Parse the main CREATE PROCEDURE statement
 */
function parseCreateProcedure(ctx: ParseContext): ParsedProcedure {
  // CREATE
  expect(ctx, 'KEYWORD', 'CREATE');

  // OR REPLACE (optional)
  if (match(ctx, 'KEYWORD', 'OR')) {
    advance(ctx);
    expect(ctx, 'KEYWORD', 'REPLACE');
  }

  // PROCEDURE or FUNCTION
  if (match(ctx, 'KEYWORD', 'PROCEDURE')) {
    advance(ctx);
  } else if (match(ctx, 'KEYWORD', 'FUNCTION')) {
    advance(ctx);
  } else {
    throw createParseError(
      'Expected PROCEDURE or FUNCTION',
      currentToken(ctx)
    );
  }

  // Procedure name
  const nameToken = expect(ctx, 'IDENTIFIER');
  const name = nameToken.value;

  // Optional parameters
  const parameters = parseParameters(ctx);

  // Optional return type
  const returnType = parseReturnType(ctx);

  // AS MODULE
  expect(ctx, 'KEYWORD', 'AS');
  expect(ctx, 'KEYWORD', 'MODULE');

  // Module code
  const codeToken = expect(ctx, 'MODULE_CODE');
  const code = codeToken.value;

  // Optional semicolon
  if (match(ctx, 'SYMBOL', ';')) {
    advance(ctx);
  }

  return {
    name,
    code,
    parameters,
    returnType,
    rawSql: ctx.input,
  };
}

// =============================================================================
// PUBLIC API
// =============================================================================

/**
 * Parse a CREATE PROCEDURE SQL statement
 *
 * @param sql - The SQL statement to parse
 * @returns The parsed procedure definition
 * @throws ParseError if the SQL is invalid
 *
 * @example
 * ```typescript
 * const result = parseProcedure(`
 *   CREATE PROCEDURE calculate_score AS MODULE $$
 *     export default async ({ db }, userId) => {
 *       const user = await db.users.get(userId);
 *       const orders = await db.orders.where({ userId });
 *       return orders.reduce((sum, o) => sum + o.total, 0);
 *     }
 *   $$;
 * `);
 * ```
 */
export function parseProcedure(sql: string): ParsedProcedure {
  const tokens = tokenize(sql);
  const ctx: ParseContext = { tokens, pos: 0, input: sql };
  return parseCreateProcedure(ctx);
}

/**
 * Try to parse a CREATE PROCEDURE statement, returning an error instead of throwing
 */
export function tryParseProcedure(sql: string): { success: true; procedure: ParsedProcedure } | { success: false; error: ParseError } {
  try {
    const procedure = parseProcedure(sql);
    return { success: true, procedure };
  } catch (e) {
    if (typeof e === 'object' && e !== null && 'message' in e) {
      const error = e as ParseError;
      return {
        success: false,
        error: {
          message: error.message,
          position: error.position,
          line: error.line,
          column: error.column,
        },
      };
    }
    return {
      success: false,
      error: { message: String(e) },
    };
  }
}

/**
 * Check if a SQL string is a CREATE PROCEDURE statement
 */
export function isCreateProcedure(sql: string): boolean {
  const trimmed = sql.trim().toUpperCase();
  return (
    trimmed.startsWith('CREATE PROCEDURE') ||
    trimmed.startsWith('CREATE OR REPLACE PROCEDURE') ||
    trimmed.startsWith('CREATE FUNCTION') ||
    trimmed.startsWith('CREATE OR REPLACE FUNCTION')
  );
}

/**
 * Extract module code from a parsed procedure and validate ESM syntax
 */
export function validateModuleCode(code: string): { valid: boolean; error?: string } {
  // Check for default export
  const hasDefaultExport =
    code.includes('export default') ||
    code.includes('exports.default') ||
    code.includes('module.exports');

  if (!hasDefaultExport) {
    return {
      valid: false,
      error: 'Module must have a default export',
    };
  }

  // Check for obvious syntax errors (basic validation)
  const balanced = checkBracketBalance(code);
  if (!balanced.valid) {
    return {
      valid: false,
      error: `Unbalanced brackets: ${balanced.error}`,
    };
  }

  return { valid: true };
}

function checkBracketBalance(code: string): { valid: boolean; error?: string } {
  const stack: string[] = [];
  const pairs: Record<string, string> = { '(': ')', '[': ']', '{': '}' };
  const closers = new Set(Object.values(pairs));
  let inString = false;
  let stringChar = '';
  let inTemplateString = false;

  for (let i = 0; i < code.length; i++) {
    const char = code[i];
    const prev = i > 0 ? code[i - 1] : '';

    // Handle string literals
    if ((char === '"' || char === "'") && prev !== '\\') {
      if (!inString && !inTemplateString) {
        inString = true;
        stringChar = char;
      } else if (inString && char === stringChar) {
        inString = false;
        stringChar = '';
      }
      continue;
    }

    // Handle template literals
    if (char === '`' && prev !== '\\') {
      inTemplateString = !inTemplateString;
      continue;
    }

    // Skip if we're inside a string
    if (inString || inTemplateString) continue;

    // Handle brackets
    if (pairs[char]) {
      stack.push(pairs[char]);
    } else if (closers.has(char)) {
      if (stack.length === 0) {
        return { valid: false, error: `Unexpected '${char}' at position ${i}` };
      }
      const expected = stack.pop();
      if (expected !== char) {
        return { valid: false, error: `Expected '${expected}' but got '${char}' at position ${i}` };
      }
    }
  }

  if (stack.length > 0) {
    return { valid: false, error: `Missing closing bracket(s): ${stack.join(', ')}` };
  }

  return { valid: true };
}

// =============================================================================
// SCHEMA INFERENCE (Type-level)
// =============================================================================

/**
 * Map SQL types to Schema types
 */
export function sqlTypeToSchema(sqlType: string): Schema {
  const normalized = sqlType.toUpperCase().replace(/\[\]$/, '');
  const isArray = sqlType.toUpperCase().endsWith('[]');

  let baseSchema: Schema;

  switch (normalized) {
    case 'INTEGER':
    case 'INT':
    case 'BIGINT':
    case 'SMALLINT':
    case 'REAL':
    case 'DOUBLE':
    case 'DOUBLE PRECISION':
    case 'DECIMAL':
    case 'NUMERIC':
    case 'FLOAT':
    case 'NUMBER':
      baseSchema = { type: 'number' };
      break;
    case 'TEXT':
    case 'VARCHAR':
    case 'CHAR':
    case 'STRING':
    case 'UUID':
      baseSchema = { type: 'string' };
      break;
    case 'BOOLEAN':
    case 'BOOL':
      baseSchema = { type: 'boolean' };
      break;
    case 'JSON':
    case 'JSONB':
    case 'OBJECT':
      baseSchema = { type: 'object' };
      break;
    default:
      baseSchema = { type: 'any' };
  }

  if (isArray) {
    return { type: 'array', items: baseSchema };
  }

  return baseSchema;
}

/**
 * Build input schema from parsed parameters
 */
export function buildInputSchema(parameters: ParsedProcedure['parameters']): Schema | undefined {
  if (!parameters || parameters.length === 0) {
    return undefined;
  }

  const properties: Record<string, Schema> = {};
  const required: string[] = [];

  for (const param of parameters) {
    properties[param.name] = sqlTypeToSchema(param.type);
    if (param.defaultValue === undefined) {
      required.push(param.name);
    }
  }

  return {
    type: 'object',
    properties,
    required: required.length > 0 ? required : undefined,
  };
}

/**
 * Build output schema from return type
 */
export function buildOutputSchema(returnType: string | undefined): Schema | undefined {
  if (!returnType) {
    return undefined;
  }

  return sqlTypeToSchema(returnType);
}
