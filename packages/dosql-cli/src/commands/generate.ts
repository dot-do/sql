import { promises as fs } from 'node:fs';
import { join } from 'node:path';
import ts from 'typescript';

export interface GenerateOptions {
  schemaDir: string;
  outputDir: string;
}

export interface GenerateResult {
  success: boolean;
  generatedFiles: string[];
  tablesProcessed: string[];
}

/**
 * Context information for schema parsing errors.
 */
export interface SchemaParseContext {
  filePath?: string | undefined;
  variableName?: string | undefined;
  tableName?: string | undefined;
}

/**
 * Custom error class for schema parsing errors.
 * Provides user-friendly error messages with context about where the error occurred.
 */
export class SchemaParseError extends Error {
  constructor(
    message: string,
    public readonly context?: SchemaParseContext
  ) {
    const contextParts: string[] = [];
    if (context?.filePath) contextParts.push(`file: ${context.filePath}`);
    if (context?.variableName) contextParts.push(`variable: ${context.variableName}`);
    if (context?.tableName) contextParts.push(`table: ${context.tableName}`);

    const contextStr = contextParts.length > 0 ? ` (${contextParts.join(', ')})` : '';
    super(`${message}${contextStr}`);
    this.name = 'SchemaParseError';
  }
}

interface ColumnDefinition {
  type: 'integer' | 'text' | 'real' | 'blob' | 'boolean';
  primaryKey?: boolean;
  nullable?: boolean;
}

interface TableSchema {
  tableName: string;
  columns: Record<string, ColumnDefinition>;
}

function mapSqlTypeToTs(type: string, nullable: boolean): string {
  const baseType = (() => {
    switch (type) {
      case 'integer':
        return 'number';
      case 'real':
        return 'number';
      case 'boolean':
        return 'boolean';
      case 'blob':
        return 'Uint8Array';
      case 'text':
      default:
        return 'string';
    }
  })();

  return nullable ? `${baseType} | null` : baseType;
}

// Common irregular plurals mapping (lowercase)
const IRREGULAR_PLURALS: Record<string, string> = {
  people: 'person',
  children: 'child',
  men: 'man',
  women: 'woman',
  feet: 'foot',
  teeth: 'tooth',
  geese: 'goose',
  mice: 'mouse',
  indices: 'index',
  vertices: 'vertex',
  matrices: 'matrix',
  analyses: 'analysis',
  crises: 'crisis',
  theses: 'thesis',
  phenomena: 'phenomenon',
  criteria: 'criterion',
  data: 'datum',
  media: 'medium',
};

// Words that end in 's' but are not plural (should not be singularized)
const NON_PLURAL_S_WORDS = new Set([
  'status',
  'bus',
  'campus',
  'virus',
  'genus',
  'radius',
  'focus',
  'corpus',
  'census',
  'consensus',
  'axis',
  'basis',
  'series',
  'species',
  'news',
  'atlas',
  'alias',
  'canvas',
  'gas',
  'class',
  'glass',
  'grass',
  'mass',
  'pass',
  'process',
  'address',
  'success',
  'access',
  'progress',
  'congress',
  'express',
  'business',
  'witness',
  'chess',
  'dress',
  'stress',
  'boss',
  'loss',
  'cross',
  'moss',
]);

/**
 * Singularizes a word using common English rules.
 * Handles irregular plurals, words ending in -ies, -es, and regular -s plurals.
 * Also avoids incorrectly singularizing words that end in 's' but aren't plural.
 */
function singularize(word: string): string {
  const lower = word.toLowerCase();

  // Check irregular plurals first
  if (IRREGULAR_PLURALS[lower]) {
    // Preserve original casing (PascalCase)
    const singular = IRREGULAR_PLURALS[lower];
    return singular.charAt(0).toUpperCase() + singular.slice(1);
  }

  // Check if word ends in 's' but is not a plural
  if (NON_PLURAL_S_WORDS.has(lower)) {
    return word;
  }

  // Handle -ies -> -y (e.g., "Categories" -> "Category")
  if (word.endsWith('ies') && word.length > 3) {
    return word.slice(0, -3) + 'y';
  }

  // Handle -ses -> -s (e.g., "Buses" -> "Bus", "Statuses" -> "Status")
  if (word.endsWith('ses') && word.length > 3) {
    return word.slice(0, -2);
  }

  // Handle -xes -> -x (e.g., "Boxes" -> "Box")
  if (word.endsWith('xes') && word.length > 3) {
    return word.slice(0, -2);
  }

  // Handle -ches -> -ch (e.g., "Batches" -> "Batch")
  if (word.endsWith('ches') && word.length > 4) {
    return word.slice(0, -2);
  }

  // Handle -shes -> -sh (e.g., "Dishes" -> "Dish")
  if (word.endsWith('shes') && word.length > 4) {
    return word.slice(0, -2);
  }

  // Handle -oes -> -o (e.g., "Heroes" -> "Hero", "Potatoes" -> "Potato")
  if (word.endsWith('oes') && word.length > 3) {
    return word.slice(0, -2);
  }

  // Handle -ves -> -f or -fe (e.g., "Leaves" -> "Leaf", "Knives" -> "Knife")
  if (word.endsWith('ves') && word.length > 3) {
    const stem = word.slice(0, -3);
    // Common -ife words: knife, wife, life
    if (lower.endsWith('ives') && ['kn', 'w', 'l'].some(prefix => lower.slice(0, -4).endsWith(prefix))) {
      return stem + 'ife';
    }
    return stem + 'f';
  }

  // Handle regular -s plurals (e.g., "Users" -> "User")
  if (word.endsWith('s') && word.length > 1) {
    return word.slice(0, -1);
  }

  return word;
}

function tableNameToInterfaceName(tableName: string): string {
  // Convert snake_case to PascalCase: split on underscores and capitalize each part
  // Example: "user_profiles" -> "UserProfiles"
  const pascal = tableName
    .split('_')
    .map(part => part.charAt(0).toUpperCase() + part.slice(1).toLowerCase())
    .join('');

  // Apply singularization with improved rules
  return singularize(pascal);
}

/**
 * Storage for resolved variable values during AST traversal.
 * Used to resolve shorthand properties and external const references.
 */
interface VariableStore {
  [key: string]: {
    value: unknown;
    node?: ts.Node;
  };
}

/**
 * Extracts a string value from a TypeScript AST node.
 * Handles string literals, template literals, and identifiers (for shorthand).
 */
function extractStringValue(
  node: ts.Node | undefined,
  sourceFile: ts.SourceFile,
  variables: VariableStore
): string | undefined {
  if (!node) return undefined;

  // String literal: 'value' or "value"
  if (ts.isStringLiteral(node)) {
    return node.text;
  }

  // Template literal: `value` (no interpolation)
  if (ts.isNoSubstitutionTemplateLiteral(node)) {
    return node.text;
  }

  // Template expression: `${prefix}value`
  if (ts.isTemplateExpression(node)) {
    let result = node.head.text;
    for (const span of node.templateSpans) {
      // Try to resolve the expression
      const exprValue = extractStringValue(span.expression, sourceFile, variables);
      if (exprValue !== undefined) {
        result += exprValue;
      }
      result += span.literal.text;
    }
    return result;
  }

  // Identifier: reference to a variable
  if (ts.isIdentifier(node)) {
    const varName = node.text;
    const varInfo = variables[varName];
    if (varInfo && typeof varInfo.value === 'string') {
      return varInfo.value;
    }
  }

  // Type assertion: 'value' as Type
  if (ts.isAsExpression(node)) {
    return extractStringValue(node.expression, sourceFile, variables);
  }

  return undefined;
}

/**
 * Extracts a boolean value from a TypeScript AST node.
 */
function extractBooleanValue(node: ts.Node | undefined): boolean | undefined {
  if (!node) return undefined;

  // Handle type assertions: true as const, false as const
  if (ts.isAsExpression(node)) {
    return extractBooleanValue(node.expression);
  }

  if (node.kind === ts.SyntaxKind.TrueKeyword) return true;
  if (node.kind === ts.SyntaxKind.FalseKeyword) return false;

  return undefined;
}

/**
 * Parses a column definition from an object literal expression.
 */
function parseColumnFromObjectLiteral(
  obj: ts.ObjectLiteralExpression,
  sourceFile: ts.SourceFile,
  variables: VariableStore
): ColumnDefinition | undefined {
  let type: string | undefined;
  let primaryKey: boolean | undefined;
  let nullable: boolean | undefined;

  for (const prop of obj.properties) {
    // Handle property assignment: type: 'integer'
    if (ts.isPropertyAssignment(prop)) {
      const propName = prop.name.getText(sourceFile);

      if (propName === 'type') {
        type = extractStringValue(prop.initializer, sourceFile, variables);
      } else if (propName === 'primaryKey') {
        primaryKey = extractBooleanValue(prop.initializer);
      } else if (propName === 'nullable') {
        nullable = extractBooleanValue(prop.initializer);
      }
    }
  }

  // Valid SQL types
  const validTypes = ['integer', 'text', 'real', 'blob', 'boolean'];
  if (type && validTypes.includes(type)) {
    return {
      type: type as ColumnDefinition['type'],
      primaryKey: primaryKey ?? false,
      nullable: nullable ?? false,
    };
  }

  return undefined;
}

/**
 * Extracts the property name from a property assignment node.
 * Handles regular identifiers, string literals, and computed property names.
 */
function extractPropertyName(
  propName: ts.PropertyName,
  sourceFile: ts.SourceFile,
  variables: VariableStore
): string | undefined {
  // Regular identifier: id, name, etc.
  if (ts.isIdentifier(propName)) {
    return propName.text;
  }

  // String literal: 'my-column' or "my-column"
  if (ts.isStringLiteral(propName)) {
    return propName.text;
  }

  // Computed property name: [CONSTANT] or [`${prefix}key`]
  if (ts.isComputedPropertyName(propName)) {
    return extractStringValue(propName.expression, sourceFile, variables);
  }

  return undefined;
}

/**
 * Parses columns from an object literal expression representing the columns block.
 */
function parseColumnsFromObjectLiteral(
  obj: ts.ObjectLiteralExpression,
  sourceFile: ts.SourceFile,
  variables: VariableStore
): Record<string, ColumnDefinition> {
  const columns: Record<string, ColumnDefinition> = {};

  for (const prop of obj.properties) {
    // Property assignment: columnName: { type: '...' }
    if (ts.isPropertyAssignment(prop)) {
      const colName = extractPropertyName(prop.name, sourceFile, variables);
      if (!colName) continue;

      // Handle type assertions and satisfies on the column value
      let valueNode = prop.initializer;

      // Strip type assertion: { ... } as const
      if (ts.isAsExpression(valueNode)) {
        valueNode = valueNode.expression;
      }

      // Strip satisfies: { ... } satisfies Column<'integer'>
      if (ts.isSatisfiesExpression(valueNode)) {
        valueNode = valueNode.expression;
      }

      if (ts.isObjectLiteralExpression(valueNode)) {
        const colDef = parseColumnFromObjectLiteral(valueNode, sourceFile, variables);
        if (colDef) {
          columns[colName] = colDef;
        }
      }
    }

    // Spread element: ...baseColumns
    if (ts.isSpreadAssignment(prop)) {
      // Try to resolve the spread expression
      let spreadExpr = prop.expression;

      // Handle property access: baseUser.columns
      if (ts.isPropertyAccessExpression(spreadExpr)) {
        const objName = spreadExpr.expression.getText(sourceFile);
        const propName = spreadExpr.name.text;

        const varInfo = variables[objName];
        if (varInfo && typeof varInfo.value === 'object' && varInfo.value !== null) {
          const objValue = varInfo.value as Record<string, unknown>;
          const nestedValue = objValue[propName];
          if (typeof nestedValue === 'object' && nestedValue !== null) {
            Object.assign(columns, nestedValue);
          }
        }
      }
      // Handle identifier: ...columns
      else if (ts.isIdentifier(spreadExpr)) {
        const varName = spreadExpr.text;
        const varInfo = variables[varName];
        if (varInfo && typeof varInfo.value === 'object' && varInfo.value !== null) {
          Object.assign(columns, varInfo.value);
        }
      }
    }
  }

  return columns;
}

/**
 * Evaluates a columns object from an AST node, handling various patterns.
 */
function evaluateColumnsValue(
  node: ts.Node,
  sourceFile: ts.SourceFile,
  variables: VariableStore
): Record<string, ColumnDefinition> | undefined {
  // Strip type assertions
  if (ts.isAsExpression(node)) {
    return evaluateColumnsValue(node.expression, sourceFile, variables);
  }

  // Object literal: { id: { type: 'integer' }, ... }
  if (ts.isObjectLiteralExpression(node)) {
    return parseColumnsFromObjectLiteral(node, sourceFile, variables);
  }

  // Identifier: reference to external const
  if (ts.isIdentifier(node)) {
    const varName = node.text;
    const varInfo = variables[varName];
    if (varInfo && typeof varInfo.value === 'object' && varInfo.value !== null) {
      return varInfo.value as Record<string, ColumnDefinition>;
    }
  }

  return undefined;
}

/**
 * Collects all variable declarations from a source file for reference resolution.
 */
function collectVariables(sourceFile: ts.SourceFile): VariableStore {
  const variables: VariableStore = {};

  function visit(node: ts.Node) {
    if (ts.isVariableDeclaration(node) && node.initializer) {
      const name = node.name.getText(sourceFile);

      // Handle string literals
      if (ts.isStringLiteral(node.initializer)) {
        variables[name] = { value: node.initializer.text, node: node.initializer };
      }
      // Handle object literals (for column definitions and schemas)
      else if (ts.isObjectLiteralExpression(node.initializer)) {
        // First pass: just store the node for later resolution
        variables[name] = { value: {}, node: node.initializer };
      }
      // Handle 'as const' expressions
      else if (ts.isAsExpression(node.initializer)) {
        const inner = node.initializer.expression;
        if (ts.isStringLiteral(inner)) {
          variables[name] = { value: inner.text, node: inner };
        } else if (ts.isObjectLiteralExpression(inner)) {
          variables[name] = { value: {}, node: inner };
        }
      }
    }

    ts.forEachChild(node, visit);
  }

  visit(sourceFile);

  // Second pass: resolve object literal values
  for (const [name, info] of Object.entries(variables)) {
    if (info.node && ts.isObjectLiteralExpression(info.node)) {
      const columns = parseColumnsFromObjectLiteral(info.node, sourceFile, variables);
      if (Object.keys(columns).length > 0) {
        variables[name] = { value: columns, node: info.node };
      }
    }
  }

  return variables;
}

/**
 * Interface for JSDoc comment info attached to columns.
 */
interface ColumnWithJsDoc extends ColumnDefinition {
  jsDoc?: string;
}

/**
 * Parses columns from an object literal expression, including JSDoc comments.
 */
function parseColumnsWithJsDoc(
  obj: ts.ObjectLiteralExpression,
  sourceFile: ts.SourceFile,
  variables: VariableStore
): Record<string, ColumnWithJsDoc> {
  const columns: Record<string, ColumnWithJsDoc> = {};

  for (const prop of obj.properties) {
    if (ts.isPropertyAssignment(prop)) {
      const colName = extractPropertyName(prop.name, sourceFile, variables);
      if (!colName) continue;

      let valueNode = prop.initializer;

      // Strip type assertion
      if (ts.isAsExpression(valueNode)) {
        valueNode = valueNode.expression;
      }

      // Strip satisfies
      if (ts.isSatisfiesExpression(valueNode)) {
        valueNode = valueNode.expression;
      }

      if (ts.isObjectLiteralExpression(valueNode)) {
        const colDef = parseColumnFromObjectLiteral(valueNode, sourceFile, variables);
        if (colDef) {
          // Extract JSDoc comment
          const jsDocTags = ts.getJSDocTags(prop);
          const leadingComments = ts.getLeadingCommentRanges(sourceFile.text, prop.pos);

          let jsDoc: string | undefined;
          if (leadingComments) {
            for (const comment of leadingComments) {
              const commentText = sourceFile.text.slice(comment.pos, comment.end);
              if (commentText.startsWith('/**')) {
                jsDoc = commentText;
                break;
              }
            }
          }

          // Only include jsDoc property if it's defined (exactOptionalPropertyTypes)
          columns[colName] = jsDoc !== undefined ? { ...colDef, jsDoc } : { ...colDef };
        }
      }
    }

    // Handle spread
    if (ts.isSpreadAssignment(prop)) {
      let spreadExpr = prop.expression;

      if (ts.isPropertyAccessExpression(spreadExpr)) {
        const objName = spreadExpr.expression.getText(sourceFile);
        const propName = spreadExpr.name.text;

        const varInfo = variables[objName];
        if (varInfo && typeof varInfo.value === 'object' && varInfo.value !== null) {
          const objValue = varInfo.value as Record<string, unknown>;
          const nestedValue = objValue[propName];
          if (typeof nestedValue === 'object' && nestedValue !== null) {
            Object.assign(columns, nestedValue);
          }
        }
      } else if (ts.isIdentifier(spreadExpr)) {
        const varName = spreadExpr.text;
        const varInfo = variables[varName];
        if (varInfo && typeof varInfo.value === 'object' && varInfo.value !== null) {
          Object.assign(columns, varInfo.value);
        }
      }
    }
  }

  return columns;
}

/**
 * Interface for table schema with JSDoc comments on columns.
 */
interface TableSchemaWithJsDoc extends TableSchema {
  columnsWithJsDoc?: Record<string, ColumnWithJsDoc>;
}

/**
 * Extracts table schemas from a source file using TypeScript AST.
 */
function extractTableSchemasFromAst(
  sourceFile: ts.SourceFile,
  filePath?: string
): TableSchemaWithJsDoc[] {
  const schemas: TableSchemaWithJsDoc[] = [];
  const variables = collectVariables(sourceFile);

  function visit(node: ts.Node) {
    // Look for exported variable declarations
    if (ts.isVariableStatement(node)) {
      const hasExport = node.modifiers?.some(m => m.kind === ts.SyntaxKind.ExportKeyword);
      if (!hasExport) {
        ts.forEachChild(node, visit);
        return;
      }

      for (const decl of node.declarationList.declarations) {
        if (!decl.initializer) continue;

        let initNode = decl.initializer;

        // Strip 'as const' and 'satisfies Type'
        if (ts.isAsExpression(initNode)) {
          initNode = initNode.expression;
        }
        if (ts.isSatisfiesExpression(initNode)) {
          initNode = initNode.expression;
        }
        if (ts.isAsExpression(initNode)) {
          initNode = initNode.expression;
        }

        if (!ts.isObjectLiteralExpression(initNode)) continue;

        let tableName: string | undefined;
        let columnsObj: ts.ObjectLiteralExpression | undefined;
        let columnsFromVar: Record<string, ColumnDefinition> | undefined;

        for (const prop of initNode.properties) {
          // Handle spread: ...baseUser
          if (ts.isSpreadAssignment(prop)) {
            const spreadExpr = prop.expression;
            if (ts.isIdentifier(spreadExpr)) {
              const varName = spreadExpr.text;
              const varInfo = variables[varName];
              if (varInfo && varInfo.node && ts.isObjectLiteralExpression(varInfo.node)) {
                // Extract tableName and columns from spread object if not already set
                for (const spreadProp of varInfo.node.properties) {
                  if (ts.isPropertyAssignment(spreadProp)) {
                    const propName = spreadProp.name.getText(sourceFile);
                    if (propName === 'tableName' && !tableName) {
                      tableName = extractStringValue(spreadProp.initializer, sourceFile, variables);
                    }
                    if (propName === 'columns' && !columnsObj && !columnsFromVar) {
                      let colNode = spreadProp.initializer;
                      if (ts.isAsExpression(colNode)) colNode = colNode.expression;
                      if (ts.isObjectLiteralExpression(colNode)) {
                        columnsObj = colNode;
                      }
                    }
                  }
                }
              }
            }
          }

          // Handle property assignment
          if (ts.isPropertyAssignment(prop)) {
            const propName = prop.name.getText(sourceFile);

            if (propName === 'tableName') {
              tableName = extractStringValue(prop.initializer, sourceFile, variables);
            }

            if (propName === 'columns') {
              let colNode = prop.initializer;

              // Handle identifier (shorthand or reference): columns
              if (ts.isIdentifier(colNode)) {
                const varName = colNode.text;
                const varInfo = variables[varName];
                if (varInfo) {
                  if (typeof varInfo.value === 'object' && varInfo.value !== null) {
                    columnsFromVar = varInfo.value as Record<string, ColumnDefinition>;
                  }
                  if (varInfo.node && ts.isObjectLiteralExpression(varInfo.node)) {
                    columnsObj = varInfo.node;
                  }
                }
              } else {
                // Strip type assertions
                if (ts.isAsExpression(colNode)) colNode = colNode.expression;
                if (ts.isSatisfiesExpression(colNode)) colNode = colNode.expression;

                if (ts.isObjectLiteralExpression(colNode)) {
                  columnsObj = colNode;
                }
              }
            }
          }

          // Handle shorthand property: { tableName, columns }
          if (ts.isShorthandPropertyAssignment(prop)) {
            const propName = prop.name.text;

            if (propName === 'tableName') {
              const varInfo = variables[propName];
              if (varInfo && typeof varInfo.value === 'string') {
                tableName = varInfo.value;
              }
            }

            if (propName === 'columns') {
              const varInfo = variables[propName];
              if (varInfo) {
                if (typeof varInfo.value === 'object' && varInfo.value !== null) {
                  columnsFromVar = varInfo.value as Record<string, ColumnDefinition>;
                }
                if (varInfo.node && ts.isObjectLiteralExpression(varInfo.node)) {
                  columnsObj = varInfo.node;
                }
              }
            }
          }
        }

        if (tableName) {
          let columns: Record<string, ColumnDefinition>;
          let columnsWithJsDoc: Record<string, ColumnWithJsDoc> | undefined;

          if (columnsObj) {
            columnsWithJsDoc = parseColumnsWithJsDoc(columnsObj, sourceFile, variables);
            columns = {};
            for (const [name, col] of Object.entries(columnsWithJsDoc)) {
              const { jsDoc, ...colDef } = col;
              columns[name] = colDef;
            }
          } else if (columnsFromVar) {
            columns = columnsFromVar;
          } else {
            continue;
          }

          if (Object.keys(columns).length > 0) {
            // Only include columnsWithJsDoc if defined (exactOptionalPropertyTypes)
            const schema: TableSchemaWithJsDoc = { tableName, columns };
            if (columnsWithJsDoc !== undefined) {
              schema.columnsWithJsDoc = columnsWithJsDoc;
            }
            schemas.push(schema);
          }
        }
      }
    }

    ts.forEachChild(node, visit);
  }

  visit(sourceFile);
  return schemas;
}

/**
 * Extracts all table schema definitions from file content using TypeScript AST.
 * @param content - The raw file content to parse
 * @param filePath - Optional file path for error context
 * @returns An array of TableSchema objects found in the content
 */
function extractTableSchemas(content: string, filePath?: string): TableSchemaWithJsDoc[] {
  const sourceFile = ts.createSourceFile(
    filePath || 'schema.ts',
    content,
    ts.ScriptTarget.Latest,
    true,
    ts.ScriptKind.TS
  );

  return extractTableSchemasFromAst(sourceFile, filePath);
}

/**
 * Reads and parses a schema file to extract table definitions.
 * @param filePath - The path to the schema file
 * @returns An array of TableSchema objects found in the file
 * @throws SchemaParseError if any schema definition is invalid
 */
async function parseSchemaFile(filePath: string): Promise<TableSchemaWithJsDoc[]> {
  const content = await fs.readFile(filePath, 'utf-8');
  return extractTableSchemas(content, filePath);
}

function generateInterface(schema: TableSchemaWithJsDoc): string {
  const interfaceName = tableNameToInterfaceName(schema.tableName);
  const lines: string[] = [`export interface ${interfaceName} {`];

  const columnsWithJsDoc = schema.columnsWithJsDoc || {};

  for (const [colName, colDef] of Object.entries(schema.columns)) {
    const tsType = mapSqlTypeToTs(colDef.type, colDef.nullable ?? false);
    const colWithJsDoc = columnsWithJsDoc[colName];

    // Add JSDoc comment if present
    if (colWithJsDoc?.jsDoc) {
      lines.push(`  ${colWithJsDoc.jsDoc}`);
    }

    lines.push(`  ${colName}: ${tsType};`);
  }

  lines.push('}');
  return lines.join('\n');
}

export async function generateTypes(options: GenerateOptions): Promise<GenerateResult> {
  const { schemaDir, outputDir } = options;

  // Check if schema directory exists
  const exists = await fs.access(schemaDir).then(() => true).catch(() => false);
  if (!exists) {
    throw new Error(
      `Schema directory not found: ${schemaDir}\n\n` +
      `To fix this, either:\n` +
      `  1. Run 'dosql init' to create the default project structure with schema files, or\n` +
      `  2. Create the schema directory manually: mkdir -p ${schemaDir}\n` +
      `  3. Use --schema to specify a different schema directory path\n\n` +
      `Schema files should be TypeScript files (.ts) containing table definitions.\n` +
      `See 'dosql init' output for an example schema file format.`
    );
  }

  // Ensure output directory exists
  await fs.mkdir(outputDir, { recursive: true });

  // Find all schema files
  const entries = await fs.readdir(schemaDir, { withFileTypes: true });
  const schemaFiles = entries
    .filter(e => e.isFile() && e.name.endsWith('.ts'))
    .map(e => join(schemaDir, e.name));

  // Parse all schemas
  const allSchemas: TableSchemaWithJsDoc[] = [];
  for (const file of schemaFiles) {
    const schemas = await parseSchemaFile(file);
    allSchemas.push(...schemas);
  }

  // Generate TypeScript interfaces
  const interfaces = allSchemas.map(generateInterface);
  const header = `// This file is auto-generated by dosql generate
// Do not edit manually

`;
  const output = header + interfaces.join('\n\n') + '\n';

  // Write output file
  const outputPath = join(outputDir, 'types.ts');
  await fs.writeFile(outputPath, output);

  return {
    success: true,
    generatedFiles: ['types.ts'],
    tablesProcessed: allSchemas.map(s => s.tableName),
  };
}
