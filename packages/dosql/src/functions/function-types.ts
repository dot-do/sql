/**
 * Function Type Definitions
 *
 * Extracted from registry.ts to avoid circular dependencies:
 * registry.ts -> {aggregate,date,json,math,string,vector}.ts -> registry.ts
 *
 * @module functions/function-types
 */

import type { SqlValue } from '../engine/types.js';

// =============================================================================
// TYPES
// =============================================================================

/**
 * SQL function implementation
 */
export interface SqlFunction {
  /** The function implementation */
  fn: (...args: SqlValue[]) => SqlValue;
  /** Minimum number of arguments */
  minArgs: number;
  /** Maximum number of arguments (Infinity for variadic) */
  maxArgs: number;
  /** Whether this is deterministic (same inputs = same output) */
  deterministic?: boolean;
}

/**
 * Parameter definition for function signature
 */
export interface FunctionParam {
  /** Parameter name */
  name: string;
  /** Parameter type */
  type: 'any' | 'string' | 'number' | 'boolean' | 'bytes' | 'date';
  /** Is this parameter optional? */
  optional?: boolean;
  /** Is this a variadic parameter (can repeat)? */
  variadic?: boolean;
}

/**
 * Function signature for documentation and type checking
 */
export interface FunctionSignature {
  /** Function name */
  name: string;
  /** Parameters */
  params: FunctionParam[];
  /** Return type */
  returnType: 'any' | 'string' | 'number' | 'boolean' | 'bytes' | 'date' | 'null';
  /** Description */
  description: string;
  /** Is this an aggregate function? */
  isAggregate?: boolean;
}

/**
 * User-defined function definition
 */
export interface UserDefinedFunction {
  /** Function name */
  name: string;
  /** Implementation */
  fn: SqlFunction;
  /** Optional signature for documentation */
  signature?: FunctionSignature;
}
