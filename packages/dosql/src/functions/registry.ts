/**
 * Function Registry
 *
 * Central registry for all SQLite built-in functions and user-defined functions.
 * Provides:
 * - Registration of built-in functions
 * - Type signatures for each function
 * - Extensibility for user-defined functions (UDFs)
 * - Function lookup and invocation
 */

import type { SqlValue } from '../engine/types.js';
import { stringFunctions, stringSignatures } from './string.js';
import { mathFunctions, mathSignatures } from './math.js';
import { dateFunctions, dateSignatures } from './date.js';
import { jsonFunctions, jsonSignatures } from './json.js';
import {
  aggregateFactories,
  aggregateSignatures,
  isAggregateFunction,
  type AggregateAccumulator,
  type AggregateFactory,
} from './aggregate.js';
import { vectorFunctions, vectorSignatures } from './vector.js';

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

// =============================================================================
// FUNCTION REGISTRY
// =============================================================================

/**
 * Registry of all available functions
 */
export class FunctionRegistry {
  private functions = new Map<string, SqlFunction>();
  private signatures = new Map<string, FunctionSignature>();
  private aggregateFactories = new Map<string, AggregateFactory>();

  constructor() {
    this.registerBuiltins();
  }

  /**
   * Register all built-in functions
   */
  private registerBuiltins(): void {
    // String functions
    for (const [name, fn] of Object.entries(stringFunctions)) {
      this.functions.set(name.toLowerCase(), fn);
    }
    for (const [name, sig] of Object.entries(stringSignatures)) {
      this.signatures.set(name.toLowerCase(), sig);
    }

    // Math functions
    for (const [name, fn] of Object.entries(mathFunctions)) {
      this.functions.set(name.toLowerCase(), fn);
    }
    for (const [name, sig] of Object.entries(mathSignatures)) {
      this.signatures.set(name.toLowerCase(), sig);
    }

    // Date functions
    for (const [name, fn] of Object.entries(dateFunctions)) {
      this.functions.set(name.toLowerCase(), fn);
    }
    for (const [name, sig] of Object.entries(dateSignatures)) {
      this.signatures.set(name.toLowerCase(), sig);
    }

    // JSON functions
    for (const [name, fn] of Object.entries(jsonFunctions)) {
      this.functions.set(name.toLowerCase(), fn);
    }
    for (const [name, sig] of Object.entries(jsonSignatures)) {
      this.signatures.set(name.toLowerCase(), sig);
    }

    // Aggregate functions (signatures only - actual execution is different)
    for (const [name, sig] of Object.entries(aggregateSignatures)) {
      this.signatures.set(name.toLowerCase(), sig);
    }

    // Aggregate factories
    for (const [name, factory] of Object.entries(aggregateFactories)) {
      this.aggregateFactories.set(name.toLowerCase(), factory);
    }

    // Vector functions
    for (const [name, fn] of Object.entries(vectorFunctions)) {
      this.functions.set(name.toLowerCase(), fn);
    }
    for (const [name, sig] of Object.entries(vectorSignatures)) {
      this.signatures.set(name.toLowerCase(), sig);
    }
  }

  /**
   * Register a user-defined function
   */
  register(name: string, fn: SqlFunction, signature?: FunctionSignature): void {
    const lowerName = name.toLowerCase();
    this.functions.set(lowerName, fn);
    if (signature) {
      this.signatures.set(lowerName, signature);
    }
  }

  /**
   * Register a user-defined aggregate function
   */
  registerAggregate(name: string, factory: AggregateFactory, signature?: FunctionSignature): void {
    const lowerName = name.toLowerCase();
    this.aggregateFactories.set(lowerName, factory);
    if (signature) {
      this.signatures.set(lowerName, { ...signature, isAggregate: true });
    }
  }

  /**
   * Unregister a function
   */
  unregister(name: string): boolean {
    const lowerName = name.toLowerCase();
    const existed = this.functions.has(lowerName) || this.aggregateFactories.has(lowerName);
    this.functions.delete(lowerName);
    this.signatures.delete(lowerName);
    this.aggregateFactories.delete(lowerName);
    return existed;
  }

  /**
   * Check if a function exists
   */
  has(name: string): boolean {
    const lowerName = name.toLowerCase();
    return this.functions.has(lowerName) || this.aggregateFactories.has(lowerName);
  }

  /**
   * Check if a function is an aggregate
   */
  isAggregate(name: string): boolean {
    return isAggregateFunction(name) || this.aggregateFactories.has(name.toLowerCase());
  }

  /**
   * Get function signature
   */
  getSignature(name: string): FunctionSignature | undefined {
    return this.signatures.get(name.toLowerCase());
  }

  /**
   * Get all registered function names
   */
  getFunctionNames(): string[] {
    const names = new Set<string>();
    for (const name of this.functions.keys()) {
      names.add(name);
    }
    for (const name of this.aggregateFactories.keys()) {
      names.add(name);
    }
    return Array.from(names).sort();
  }

  /**
   * Get all function signatures
   */
  getAllSignatures(): FunctionSignature[] {
    return Array.from(this.signatures.values());
  }

  /**
   * Invoke a scalar function
   */
  invoke(name: string, args: SqlValue[]): SqlValue {
    const lowerName = name.toLowerCase();
    const fn = this.functions.get(lowerName);

    if (!fn) {
      throw new Error(`Unknown function: ${name}`);
    }

    // Validate argument count
    if (args.length < fn.minArgs) {
      throw new Error(`${name}() requires at least ${fn.minArgs} argument(s), got ${args.length}`);
    }
    if (args.length > fn.maxArgs) {
      throw new Error(`${name}() accepts at most ${fn.maxArgs} argument(s), got ${args.length}`);
    }

    return fn.fn(...args);
  }

  /**
   * Create an aggregate accumulator
   */
  createAccumulator(name: string): AggregateAccumulator {
    const lowerName = name.toLowerCase();
    const factory = this.aggregateFactories.get(lowerName);

    if (!factory) {
      throw new Error(`Unknown aggregate function: ${name}`);
    }

    return factory();
  }
}

// =============================================================================
// DEFAULT REGISTRY INSTANCE
// =============================================================================

/**
 * Default function registry with all built-in functions
 */
export const defaultRegistry = new FunctionRegistry();

/**
 * Invoke a function using the default registry
 */
export function invokeFunction(name: string, args: SqlValue[]): SqlValue {
  return defaultRegistry.invoke(name, args);
}

/**
 * Check if a function exists in the default registry
 */
export function hasFunction(name: string): boolean {
  return defaultRegistry.has(name);
}

/**
 * Check if a function is an aggregate in the default registry
 */
export function isAggregate(name: string): boolean {
  return defaultRegistry.isAggregate(name);
}

// =============================================================================
// HELPER FUNCTIONS FOR EXPRESSION EVALUATION
// =============================================================================

/**
 * Evaluate a function call expression
 * This is used by the filter operator
 */
export function evaluateFunction(name: string, args: SqlValue[]): SqlValue {
  return defaultRegistry.invoke(name, args);
}

// =============================================================================
// TYPE EXPORTS
// =============================================================================

export type {
  AggregateAccumulator,
  AggregateFactory,
};

// Re-export individual function modules for direct access
export { stringFunctions, stringSignatures } from './string.js';
export { mathFunctions, mathSignatures } from './math.js';
export { dateFunctions, dateSignatures } from './date.js';
export { jsonFunctions, jsonSignatures } from './json.js';
export { aggregateFactories, aggregateSignatures, executeAggregate } from './aggregate.js';
export { vectorFunctions, vectorSignatures } from './vector.js';
