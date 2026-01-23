/**
 * DoSQL AST-based Schema Parser
 *
 * Uses TypeScript Compiler API for accurate parsing of schema definition files.
 * This provides:
 * - More accurate parsing than regex
 * - Better error messages with position information
 * - Support for complex type expressions
 * - Proper handling of imports and type references
 *
 * Handles TypeScript syntax that regex-based parsing cannot:
 * - Type assertions (e.g., `'integer' as ColumnType`)
 * - Satisfies operator (e.g., `{ ... } satisfies Column<'integer'>`)
 * - Multi-line column definitions
 * - Computed property names (e.g., `[ID_COLUMN]: { ... }`)
 * - Property shorthand (e.g., `{ tableName, columns }`)
 * - External const references
 * - Spread operators (e.g., `{ ...baseColumns }`)
 * - JSDoc comments preservation
 * - Double/single/backtick string literals
 * - Multiple schemas per file
 */

import * as ts from 'typescript';
import type {
  SchemaDefinition,
  TableDefinition,
  ParsedField,
  FieldModifiers,
  RelationInfo,
} from './types.js';

// =============================================================================
// TYPES
// =============================================================================

/**
 * Position information for error messages
 */
export interface SourcePosition {
  line: number;
  column: number;
  offset: number;
}

/**
 * Parse error with position information
 */
export interface ParseError {
  message: string;
  position?: SourcePosition;
  nodePath?: string[];
}

/**
 * Parse result with errors
 */
export interface ParseResult<T> {
  value: T | null;
  errors: ParseError[];
  warnings: ParseError[];
}

/**
 * Parsed schema from TypeScript source
 */
export interface ParsedSchemaSource {
  /** Schema name (variable name) */
  name: string;
  /** Parsed schema definition */
  schema: SchemaDefinition;
  /** JSDoc comment if present */
  jsDoc?: string;
  /** Source position */
  position: SourcePosition;
}

/**
 * Parse options
 */
export interface AstParseOptions {
  /** File name for error messages */
  fileName?: string;
  /** Whether to include JSDoc comments */
  includeJsDoc?: boolean;
  /** Whether to resolve external const references */
  resolveReferences?: boolean;
}

// =============================================================================
// AST UTILITIES
// =============================================================================

/**
 * Get position information from a node
 */
function getPosition(node: ts.Node, sourceFile: ts.SourceFile): SourcePosition {
  const pos = sourceFile.getLineAndCharacterOfPosition(node.getStart(sourceFile));
  return {
    line: pos.line + 1, // 1-indexed
    column: pos.character + 1, // 1-indexed
    offset: node.getStart(sourceFile),
  };
}

/**
 * Unwrap type assertions, satisfies, and parenthesized expressions
 */
function unwrapExpression(node: ts.Expression): ts.Expression {
  if (ts.isAsExpression(node)) {
    return unwrapExpression(node.expression);
  }
  if (ts.isSatisfiesExpression(node)) {
    return unwrapExpression(node.expression);
  }
  if (ts.isParenthesizedExpression(node)) {
    return unwrapExpression(node.expression);
  }
  return node;
}

/**
 * Get JSDoc comment for a node
 */
function getJsDocComment(node: ts.Node, sourceFile: ts.SourceFile): string | undefined {
  const jsDocNodes = ts.getJSDocCommentsAndTags(node);
  if (jsDocNodes.length === 0) {
    return undefined;
  }

  const comments: string[] = [];
  for (const jsDoc of jsDocNodes) {
    if (ts.isJSDoc(jsDoc)) {
      const text = jsDoc.comment;
      if (text) {
        if (typeof text === 'string') {
          comments.push(text);
        } else {
          // Handle JSDocComment array
          comments.push(text.map(c => c.getText(sourceFile)).join(''));
        }
      }
    }
  }

  return comments.length > 0 ? comments.join('\n') : undefined;
}

// =============================================================================
// EXPRESSION EVALUATOR
// =============================================================================

/**
 * Evaluate a simple expression to a constant value
 */
function evaluateExpression(
  node: ts.Expression,
  constants: Map<string, unknown>,
  sourceFile: ts.SourceFile
): unknown {
  const expr = unwrapExpression(node);

  // String literal
  if (ts.isStringLiteral(expr)) {
    return expr.text;
  }

  // Template literal (simple, no expressions)
  if (ts.isNoSubstitutionTemplateLiteral(expr)) {
    return expr.text;
  }

  // Template literal with expressions
  if (ts.isTemplateExpression(expr)) {
    let result = expr.head.text;
    for (const span of expr.templateSpans) {
      const spanValue = evaluateExpression(span.expression, constants, sourceFile);
      if (typeof spanValue === 'string' || typeof spanValue === 'number') {
        result += String(spanValue);
      }
      result += span.literal.text;
    }
    return result;
  }

  // Number literal
  if (ts.isNumericLiteral(expr)) {
    return Number(expr.text);
  }

  // Boolean literals
  if (expr.kind === ts.SyntaxKind.TrueKeyword) {
    return true;
  }
  if (expr.kind === ts.SyntaxKind.FalseKeyword) {
    return false;
  }

  // Null/undefined
  if (expr.kind === ts.SyntaxKind.NullKeyword) {
    return null;
  }
  if (expr.kind === ts.SyntaxKind.UndefinedKeyword) {
    return undefined;
  }

  // Identifier - resolve from constants
  if (ts.isIdentifier(expr)) {
    return constants.get(expr.text);
  }

  // Property access: obj.prop
  if (ts.isPropertyAccessExpression(expr)) {
    const obj = evaluateExpression(expr.expression, constants, sourceFile);
    if (obj && typeof obj === 'object') {
      return (obj as Record<string, unknown>)[expr.name.text];
    }
    return undefined;
  }

  // Object literal
  if (ts.isObjectLiteralExpression(expr)) {
    const result: Record<string, unknown> = {};

    for (const prop of expr.properties) {
      if (ts.isSpreadAssignment(prop)) {
        const spreadValue = evaluateExpression(prop.expression, constants, sourceFile);
        if (spreadValue && typeof spreadValue === 'object') {
          Object.assign(result, spreadValue);
        }
        continue;
      }

      if (ts.isShorthandPropertyAssignment(prop)) {
        const value = constants.get(prop.name.text);
        if (value !== undefined) {
          result[prop.name.text] = value;
        }
        continue;
      }

      if (ts.isPropertyAssignment(prop)) {
        const propName = getPropertyName(prop, constants, sourceFile);
        if (propName) {
          result[propName] = evaluateExpression(prop.initializer, constants, sourceFile);
        }
      }
    }

    return result;
  }

  // Array literal
  if (ts.isArrayLiteralExpression(expr)) {
    return expr.elements.map(el => evaluateExpression(el, constants, sourceFile));
  }

  return undefined;
}

/**
 * Get the name of a property (handles computed property names)
 */
function getPropertyName(
  prop: ts.PropertyAssignment | ts.ShorthandPropertyAssignment,
  constants: Map<string, unknown>,
  sourceFile: ts.SourceFile
): string | undefined {
  if (ts.isShorthandPropertyAssignment(prop)) {
    return prop.name.text;
  }

  const name = prop.name;

  if (ts.isIdentifier(name)) {
    return name.text;
  }

  if (ts.isStringLiteral(name)) {
    return name.text;
  }

  if (ts.isComputedPropertyName(name)) {
    // Handle computed property names like [ID_COLUMN]
    const value = evaluateExpression(name.expression, constants, sourceFile);
    if (typeof value === 'string') {
      return value;
    }
  }

  return undefined;
}

// =============================================================================
// DSL FIELD PARSER (AST-based)
// =============================================================================

/**
 * Parse a field definition string using a simple state machine
 * This replaces the regex-based parser with a more robust approach
 */
export function parseFieldDefinition(fieldDef: string): ParsedField {
  const modifiers: FieldModifiers = {
    required: true,
    primaryKey: false,
    indexed: false,
    nullable: false,
    isArray: false,
  };
  let relation: RelationInfo | undefined;
  let baseType = '';

  const input = fieldDef.trim();
  let index = 0;

  // Helper to consume whitespace
  const skipWhitespace = () => {
    while (index < input.length && /\s/.test(input[index]!)) {
      index++;
    }
  };

  // Helper to peek at current character
  const peek = (offset = 0): string | undefined => input[index + offset];

  // Helper to consume a character
  const consume = (): string => input[index++]!;

  // Helper to check if we're at end
  const atEnd = () => index >= input.length;

  const isIdentifierChar = (ch: string | undefined): boolean =>
    ch !== undefined && /[a-zA-Z0-9_]/.test(ch);

  skipWhitespace();

  // Check for forward relation (->)
  if (peek() === '-' && peek(1) === '>') {
    index += 2;
    skipWhitespace();

    // Parse target table name
    let target = '';
    while (!atEnd() && isIdentifierChar(peek())) {
      target += consume();
    }
    skipWhitespace();

    // Check for array notation
    let isMany = false;
    if (peek() === '[' && peek(1) === ']') {
      index += 2;
      isMany = true;
      modifiers.isArray = true;
    }

    return {
      raw: fieldDef,
      baseType: target,
      modifiers,
      relation: {
        type: 'forward',
        target,
        isMany,
      },
    };
  }

  // Check for backward relation (<-)
  if (peek() === '<' && peek(1) === '-') {
    index += 2;
    skipWhitespace();

    // Parse target table name
    let target = '';
    while (!atEnd() && isIdentifierChar(peek())) {
      target += consume();
    }
    skipWhitespace();

    // Check for array notation
    let isMany = false;
    if (peek() === '[' && peek(1) === ']') {
      index += 2;
      isMany = true;
      modifiers.isArray = true;
    }

    return {
      raw: fieldDef,
      baseType: target,
      modifiers,
      relation: {
        type: 'backward',
        target,
        isMany,
      },
    };
  }

  // Handle decimal with precision: decimal(10,2)
  if (input.slice(index).toLowerCase().startsWith('decimal(')) {
    let depth = 0;
    while (!atEnd()) {
      const ch = consume();
      baseType += ch;
      if (ch === '(') depth++;
      if (ch === ')') {
        depth--;
        if (depth === 0) break;
      }
    }
  } else {
    // Regular type name
    while (!atEnd() && isIdentifierChar(peek())) {
      baseType += consume();
    }
  }

  // Parse modifiers and array notation
  while (!atEnd()) {
    skipWhitespace();
    const ch = peek();

    if (ch === '!') {
      consume();
      modifiers.primaryKey = true;
      modifiers.required = true;
      modifiers.nullable = false;
    } else if (ch === '#') {
      consume();
      modifiers.indexed = true;
    } else if (ch === '?') {
      consume();
      modifiers.nullable = true;
      modifiers.required = false;
    } else if (ch === '[' && peek(1) === ']') {
      index += 2;
      modifiers.isArray = true;
    } else if (ch === '=') {
      // Default value
      consume();
      skipWhitespace();

      let defaultValue = '';
      if (peek() === '"') {
        // Quoted string
        consume(); // Opening quote
        while (!atEnd() && peek() !== '"') {
          if (peek() === '\\' && peek(1) === '"') {
            consume(); // Escape
            defaultValue += consume();
          } else {
            defaultValue += consume();
          }
        }
        if (peek() === '"') consume(); // Closing quote
      } else {
        // Unquoted value (function call or identifier)
        while (!atEnd() && peek() !== undefined && /[a-zA-Z0-9_().]/.test(peek()!)) {
          defaultValue += consume();
        }
      }
      modifiers.defaultValue = defaultValue;
    } else {
      break;
    }
  }

  return {
    raw: fieldDef,
    baseType: baseType.toLowerCase(),
    modifiers,
    relation,
  };
}

// =============================================================================
// SCHEMA SOURCE PARSER
// =============================================================================

/**
 * Parse a TypeScript source file containing schema definitions
 */
export function parseSchemaSource(
  content: string,
  options: AstParseOptions = {}
): ParseResult<ParsedSchemaSource[]> {
  const { fileName = 'schema.ts', includeJsDoc = true, resolveReferences = true } = options;

  const errors: ParseError[] = [];
  const warnings: ParseError[] = [];
  const schemas: ParsedSchemaSource[] = [];

  // Parse the source file
  const sourceFile = ts.createSourceFile(
    fileName,
    content,
    ts.ScriptTarget.Latest,
    true, // setParentNodes - needed for JSDoc
    ts.ScriptKind.TS
  );

  // First pass: collect all const declarations for reference resolution
  const constants = new Map<string, unknown>();

  if (resolveReferences) {
    ts.forEachChild(sourceFile, (node) => {
      if (ts.isVariableStatement(node)) {
        for (const decl of node.declarationList.declarations) {
          if (ts.isIdentifier(decl.name) && decl.initializer) {
            const name = decl.name.text;
            const value = evaluateExpression(decl.initializer, constants, sourceFile);
            if (value !== undefined) {
              constants.set(name, value);
            }
          }
        }
      }
    });
  }

  // Second pass: find exported schema definitions
  ts.forEachChild(sourceFile, (node) => {
    if (ts.isVariableStatement(node)) {
      const isExported = node.modifiers?.some(
        (mod) => mod.kind === ts.SyntaxKind.ExportKeyword
      );

      for (const decl of node.declarationList.declarations) {
        if (ts.isIdentifier(decl.name) && decl.initializer) {
          const schemaResult = extractSchema(
            decl.name.text,
            decl.initializer,
            constants,
            sourceFile,
            includeJsDoc
          );

          if (schemaResult) {
            // Extract JSDoc for the schema
            if (includeJsDoc) {
              const jsDoc = getJsDocComment(node, sourceFile);
              if (jsDoc) {
                schemaResult.jsDoc = jsDoc;
              }
            }

            schemas.push(schemaResult);
          }
        }
      }
    }
  });

  return {
    value: schemas.length > 0 ? schemas : null,
    errors,
    warnings,
  };
}

/**
 * Extract a schema from an object literal expression
 */
function extractSchema(
  name: string,
  node: ts.Expression,
  constants: Map<string, unknown>,
  sourceFile: ts.SourceFile,
  includeJsDoc: boolean
): ParsedSchemaSource | null {
  const objectExpr = unwrapExpression(node);

  if (!ts.isObjectLiteralExpression(objectExpr)) {
    return null;
  }

  const schema: SchemaDefinition = {};
  const position = getPosition(node, sourceFile);

  for (const prop of objectExpr.properties) {
    if (ts.isSpreadAssignment(prop)) {
      // Handle spread: { ...baseSchema }
      const spreadValue = evaluateExpression(prop.expression, constants, sourceFile);
      if (spreadValue && typeof spreadValue === 'object') {
        Object.assign(schema, spreadValue);
      }
      continue;
    }

    if (!ts.isPropertyAssignment(prop) && !ts.isShorthandPropertyAssignment(prop)) {
      continue;
    }

    const propName = getPropertyName(prop, constants, sourceFile);
    if (!propName) continue;

    // Skip schema directives
    if (propName.startsWith('@')) {
      continue;
    }

    const tableValue = ts.isShorthandPropertyAssignment(prop)
      ? constants.get(propName)
      : evaluateExpression(prop.initializer, constants, sourceFile);

    if (tableValue && typeof tableValue === 'object' && tableValue !== null) {
      schema[propName] = tableValue as TableDefinition;
    }
  }

  // Check if this looks like a schema (has table definitions)
  const hasTableDefs = Object.keys(schema).some(key => {
    const value = schema[key];
    return value && typeof value === 'object' && !key.startsWith('@');
  });

  if (!hasTableDefs) {
    return null;
  }

  return {
    name,
    schema,
    position,
  };
}

// =============================================================================
// PARSE TABLE DEFINITION FROM AST
// =============================================================================

/**
 * Parse a table definition from an object literal, converting to DSL format
 * This handles the case where schemas are defined with object syntax:
 * { id: { type: 'integer', primaryKey: true } }
 * and converts to the DSL format: { id: 'integer!' }
 */
export function parseObjectTableDefinition(
  node: ts.Expression,
  constants: Map<string, unknown>,
  sourceFile: ts.SourceFile
): TableDefinition | null {
  const objectExpr = unwrapExpression(node);

  if (!ts.isObjectLiteralExpression(objectExpr)) {
    return null;
  }

  const table: TableDefinition = {};

  for (const prop of objectExpr.properties) {
    if (ts.isSpreadAssignment(prop)) {
      const spreadValue = evaluateExpression(prop.expression, constants, sourceFile);
      if (spreadValue && typeof spreadValue === 'object') {
        Object.assign(table, spreadValue);
      }
      continue;
    }

    if (!ts.isPropertyAssignment(prop) && !ts.isShorthandPropertyAssignment(prop)) {
      continue;
    }

    const colName = getPropertyName(prop, constants, sourceFile);
    if (!colName) continue;

    if (ts.isShorthandPropertyAssignment(prop)) {
      const value = constants.get(colName);
      if (typeof value === 'string') {
        table[colName] = value;
      } else if (value && typeof value === 'object') {
        const dsl = objectColumnToDsl(value as Record<string, unknown>);
        if (dsl) table[colName] = dsl;
      }
      continue;
    }

    const colExpr = unwrapExpression(prop.initializer);

    // If it's a string literal, it's already in DSL format
    if (ts.isStringLiteral(colExpr) || ts.isNoSubstitutionTemplateLiteral(colExpr)) {
      table[colName] = colExpr.text;
      continue;
    }

    // If it's an identifier, resolve it
    if (ts.isIdentifier(colExpr)) {
      const value = constants.get(colExpr.text);
      if (typeof value === 'string') {
        table[colName] = value;
      } else if (value && typeof value === 'object') {
        const dsl = objectColumnToDsl(value as Record<string, unknown>);
        if (dsl) table[colName] = dsl;
      }
      continue;
    }

    // If it's an object literal, convert to DSL
    if (ts.isObjectLiteralExpression(colExpr)) {
      const colObj = evaluateExpression(colExpr, constants, sourceFile);
      if (colObj && typeof colObj === 'object') {
        const dsl = objectColumnToDsl(colObj as Record<string, unknown>);
        if (dsl) table[colName] = dsl;
      }
    }
  }

  return Object.keys(table).length > 0 ? table : null;
}

/**
 * Convert an object column definition to DSL string
 * { type: 'integer', primaryKey: true, nullable: false } -> 'integer!'
 */
function objectColumnToDsl(col: Record<string, unknown>): string | null {
  const type = col.type;
  if (typeof type !== 'string') {
    return null;
  }

  let dsl = type;

  // Add modifiers
  if (col.primaryKey === true) {
    dsl += '!';
  }
  if (col.indexed === true) {
    dsl += '#';
  }
  if (col.nullable === true) {
    dsl += '?';
  }
  if (col.array === true || col.isArray === true) {
    dsl += '[]';
  }
  if (col.default !== undefined || col.defaultValue !== undefined) {
    const defaultVal = col.default ?? col.defaultValue;
    if (typeof defaultVal === 'string') {
      dsl += ` = "${defaultVal}"`;
    } else {
      dsl += ` = ${defaultVal}`;
    }
  }

  // Handle relations
  if (col.relation === 'forward' || col.references) {
    const target = col.target ?? col.references;
    if (typeof target === 'string') {
      dsl = `-> ${target}${col.many || col.isArray ? '[]' : ''}`;
    }
  }
  if (col.relation === 'backward' || col.belongsTo) {
    const target = col.target ?? col.belongsTo;
    if (typeof target === 'string') {
      dsl = `<- ${target}${col.many || col.isArray ? '[]' : ''}`;
    }
  }

  return dsl;
}

// =============================================================================
// EXPORTS
// =============================================================================

export {
  getPosition,
  unwrapExpression,
  getJsDocComment,
  evaluateExpression,
  getPropertyName,
};
