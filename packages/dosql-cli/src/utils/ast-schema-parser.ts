/**
 * AST-based schema parser using TypeScript Compiler API.
 *
 * This parser handles complex TypeScript syntax that regex-based parsing cannot:
 * - Type assertions (e.g., `'integer' as ColumnType`)
 * - Satisfies operator (e.g., `{ ... } satisfies Column<'integer'>`)
 * - Multi-line column definitions
 * - Computed property names (e.g., `[ID_COLUMN]: { ... }`)
 * - Property shorthand (e.g., `{ tableName, columns }`)
 * - External const references
 * - Spread operators (e.g., `{ ...baseColumns }`)
 * - JSDoc comments preservation
 * - Double/single/backtick string literals
 * - Multiple tables per file
 */

import * as ts from 'typescript';

export interface ColumnDefinition {
  type: 'integer' | 'text' | 'real' | 'blob' | 'boolean';
  primaryKey?: boolean;
  nullable?: boolean;
  jsDoc?: string;
}

export interface TableSchema {
  tableName: string;
  columns: Record<string, ColumnDefinition>;
  jsDoc?: string;
}

/**
 * Parse schema file content using TypeScript AST.
 */
export function parseSchemaSource(content: string, fileName = 'schema.ts'): TableSchema[] {
  const sourceFile = ts.createSourceFile(
    fileName,
    content,
    ts.ScriptTarget.Latest,
    true, // setParentNodes - needed for JSDoc
    ts.ScriptKind.TS
  );

  const schemas: TableSchema[] = [];
  const constants = new Map<string, unknown>();

  // First pass: collect all const declarations for reference resolution
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

  // Second pass: find exported table schema definitions
  ts.forEachChild(sourceFile, (node) => {
    if (ts.isVariableStatement(node)) {
      const isExported = node.modifiers?.some(
        (mod) => mod.kind === ts.SyntaxKind.ExportKeyword
      );

      if (isExported) {
        for (const decl of node.declarationList.declarations) {
          if (decl.initializer) {
            const tableSchema = extractTableSchema(decl.initializer, constants, sourceFile);
            if (tableSchema) {
              // Extract JSDoc for the table
              const jsDoc = getJSDocComment(node, sourceFile);
              if (jsDoc) {
                tableSchema.jsDoc = jsDoc;
              }
              schemas.push(tableSchema);
            }
          }
        }
      }
    }
  });

  return schemas;
}

/**
 * Extract table schema from an object literal expression.
 */
function extractTableSchema(
  node: ts.Expression,
  constants: Map<string, unknown>,
  sourceFile: ts.SourceFile
): TableSchema | null {
  // Handle 'as const' and 'satisfies' expressions
  const objectExpr = unwrapExpression(node);

  if (!ts.isObjectLiteralExpression(objectExpr)) {
    return null;
  }

  let tableName: string | undefined;
  let columns: Record<string, ColumnDefinition> = {};

  for (const prop of objectExpr.properties) {
    if (ts.isSpreadAssignment(prop)) {
      // Handle spread: { ...baseSchema }
      const spreadValue = evaluateExpression(prop.expression, constants, sourceFile);
      if (spreadValue && typeof spreadValue === 'object') {
        const spreadObj = spreadValue as Record<string, unknown>;
        if (typeof spreadObj.tableName === 'string') {
          tableName = spreadObj.tableName;
        }
        if (spreadObj.columns && typeof spreadObj.columns === 'object') {
          columns = { ...columns, ...(spreadObj.columns as Record<string, ColumnDefinition>) };
        }
      }
      continue;
    }

    if (!ts.isPropertyAssignment(prop) && !ts.isShorthandPropertyAssignment(prop)) {
      continue;
    }

    const propName = getPropertyName(prop, constants, sourceFile);
    if (!propName) continue;

    if (propName === 'tableName') {
      tableName = getTableNameValue(prop, constants, sourceFile);
    } else if (propName === 'columns') {
      columns = { ...columns, ...extractColumns(prop, constants, sourceFile) };
    }
  }

  if (!tableName) {
    return null;
  }

  return { tableName, columns };
}

/**
 * Unwrap 'as const', 'satisfies', and parenthesized expressions.
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
 * Get the name of a property (handles computed property names).
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

/**
 * Get the table name value from a property assignment.
 */
function getTableNameValue(
  prop: ts.PropertyAssignment | ts.ShorthandPropertyAssignment,
  constants: Map<string, unknown>,
  sourceFile: ts.SourceFile
): string | undefined {
  if (ts.isShorthandPropertyAssignment(prop)) {
    // Handle shorthand: { tableName }
    const value = constants.get(prop.name.text);
    if (typeof value === 'string') {
      return value;
    }
    return undefined;
  }

  const value = evaluateExpression(prop.initializer, constants, sourceFile);
  if (typeof value === 'string') {
    return value;
  }

  return undefined;
}

/**
 * Extract columns from a columns property.
 */
function extractColumns(
  prop: ts.PropertyAssignment | ts.ShorthandPropertyAssignment,
  constants: Map<string, unknown>,
  sourceFile: ts.SourceFile
): Record<string, ColumnDefinition> {
  const columns: Record<string, ColumnDefinition> = {};

  let columnsExpr: ts.Expression | undefined;

  if (ts.isShorthandPropertyAssignment(prop)) {
    // Handle shorthand: { columns }
    const value = constants.get(prop.name.text);
    if (value && typeof value === 'object') {
      return value as Record<string, ColumnDefinition>;
    }
    return columns;
  }

  columnsExpr = unwrapExpression(prop.initializer);

  if (!ts.isObjectLiteralExpression(columnsExpr)) {
    // Try to resolve from constants
    if (ts.isIdentifier(columnsExpr)) {
      const value = constants.get(columnsExpr.text);
      if (value && typeof value === 'object') {
        return value as Record<string, ColumnDefinition>;
      }
    }
    return columns;
  }

  for (const colProp of columnsExpr.properties) {
    if (ts.isSpreadAssignment(colProp)) {
      // Handle spread in columns: { ...baseColumns }
      const spreadValue = evaluateExpression(colProp.expression, constants, sourceFile);
      if (spreadValue && typeof spreadValue === 'object') {
        Object.assign(columns, spreadValue);
      }
      continue;
    }

    if (!ts.isPropertyAssignment(colProp)) continue;

    const colName = getPropertyName(colProp, constants, sourceFile);
    if (!colName) continue;

    const colDef = extractColumnDefinition(colProp.initializer, constants, sourceFile);
    if (colDef) {
      // Extract JSDoc for the column
      const jsDoc = getJSDocComment(colProp, sourceFile);
      if (jsDoc) {
        colDef.jsDoc = jsDoc;
      }
      columns[colName] = colDef;
    }
  }

  return columns;
}

/**
 * Extract column definition from a column property value.
 */
function extractColumnDefinition(
  node: ts.Expression,
  constants: Map<string, unknown>,
  sourceFile: ts.SourceFile
): ColumnDefinition | null {
  const expr = unwrapExpression(node);

  // Handle reference to a const: id: idColumn
  if (ts.isIdentifier(expr)) {
    const value = constants.get(expr.text);
    if (value && typeof value === 'object') {
      return value as ColumnDefinition;
    }
    return null;
  }

  if (!ts.isObjectLiteralExpression(expr)) {
    return null;
  }

  let type: ColumnDefinition['type'] | undefined;
  let primaryKey: boolean | undefined;
  let nullable: boolean | undefined;

  for (const prop of expr.properties) {
    if (!ts.isPropertyAssignment(prop)) continue;

    const propName = getPropertyName(prop, constants, sourceFile);
    if (!propName) continue;

    const value = evaluateExpression(prop.initializer, constants, sourceFile);

    switch (propName) {
      case 'type':
        if (typeof value === 'string' && isValidColumnType(value)) {
          type = value;
        }
        break;
      case 'primaryKey':
        if (typeof value === 'boolean') {
          primaryKey = value;
        }
        break;
      case 'nullable':
        if (typeof value === 'boolean') {
          nullable = value;
        }
        break;
    }
  }

  if (!type) {
    return null;
  }

  return {
    type,
    ...(primaryKey !== undefined && { primaryKey }),
    ...(nullable !== undefined && { nullable }),
  };
}

/**
 * Check if a string is a valid column type.
 */
function isValidColumnType(value: string): value is ColumnDefinition['type'] {
  return ['integer', 'text', 'real', 'blob', 'boolean'].includes(value);
}

/**
 * Evaluate a simple expression to a constant value.
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

  // Boolean literal
  if (expr.kind === ts.SyntaxKind.TrueKeyword) {
    return true;
  }
  if (expr.kind === ts.SyntaxKind.FalseKeyword) {
    return false;
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

  return undefined;
}

/**
 * Get JSDoc comment for a node.
 */
function getJSDocComment(
  node: ts.Node,
  sourceFile: ts.SourceFile
): string | undefined {
  const jsDocNodes = ts.getJSDocCommentsAndTags(node);

  if (jsDocNodes.length === 0) {
    return undefined;
  }

  const comments: string[] = [];

  for (const jsDoc of jsDocNodes) {
    if (ts.isJSDoc(jsDoc)) {
      // Get the full text of the JSDoc comment
      const text = jsDoc.getFullText(sourceFile).trim();
      comments.push(text);
    }
  }

  return comments.length > 0 ? comments.join('\n') : undefined;
}
