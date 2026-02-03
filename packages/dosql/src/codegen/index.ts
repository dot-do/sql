/**
 * DoSQL Code Generation Module
 *
 * Utilities for generating TypeScript types from SQL schemas.
 *
 * @example
 * ```typescript
 * import { inferSchemaFromDDL, ddlToTypeScript } from 'dosql/codegen';
 *
 * const sql = `
 *   CREATE TABLE users (
 *     id INTEGER PRIMARY KEY,
 *     name TEXT NOT NULL,
 *     email TEXT
 *   );
 * `;
 *
 * // Quick generation
 * const code = ddlToTypeScript(sql);
 *
 * // Or with full control
 * const result = inferSchemaFromDDL(sql, {
 *   useBrandedTypes: true,
 *   generateHelperTypes: true,
 *   generateValidation: true,
 * });
 * ```
 */

// Main schema inference API
export {
  // Core functions
  inferSchemaFromDDL,
  inferTableType,
  ddlToTypeScript,
  ddlToTypeScriptWithHelpers,
  // Utility functions
  toPascalCase,
  toCamelCase,
} from './schema-inference.js';

// Types
export type {
  // Options and configuration
  SchemaInferenceOptions,
  TypeMapping,
  // Generated output types
  GeneratedField,
  GeneratedInterface,
  GeneratedHelperTypes,
  SchemaInferenceResult,
} from './schema-inference.js';
