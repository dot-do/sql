/**
 * Type Guards Tests
 *
 * Tests for the type narrowing utilities.
 */

import { describe, it, expect } from 'vitest';
import {
  isObject,
  isObjectOrArray,
  isArray,
  isTypedArray,
  hasProperty,
  hasPropertyOfType,
  hasStringProperty,
  hasNumberProperty,
  hasBooleanProperty,
  hasArrayProperty,
  hasTypedArrayProperty,
  hasObjectProperty,
  hasFunctionProperty,
  hasStringProperties,
  hasProperties,
  getStringProperty,
  getNumberProperty,
  getBooleanProperty,
  getArrayProperty,
  getObjectProperty,
  getTypedProperty,
  hasErrorCode,
  hasTypeProperty,
  isErrorLike,
  hasTypeDiscriminator,
  iterateRecord,
  getKeys,
  getValues,
  getEntries,
  isString,
  isNumber,
  isBoolean,
  isBigInt,
  isFunction,
  isNullish,
  isDefined,
  asRecord,
  asRecordOrThrow,
  asArray,
  asTypedArray,
  isSqlValue,
  isRow,
} from '../type-guards.js';

describe('type-guards', () => {
  // ============================================================================
  // Basic Type Guards
  // ============================================================================

  describe('isObject', () => {
    it('should return true for plain objects', () => {
      expect(isObject({})).toBe(true);
      expect(isObject({ a: 1 })).toBe(true);
    });

    it('should return false for arrays', () => {
      expect(isObject([])).toBe(false);
      expect(isObject([1, 2, 3])).toBe(false);
    });

    it('should return false for null and undefined', () => {
      expect(isObject(null)).toBe(false);
      expect(isObject(undefined)).toBe(false);
    });

    it('should return false for primitives', () => {
      expect(isObject('string')).toBe(false);
      expect(isObject(123)).toBe(false);
      expect(isObject(true)).toBe(false);
    });
  });

  describe('isObjectOrArray', () => {
    it('should return true for objects and arrays', () => {
      expect(isObjectOrArray({})).toBe(true);
      expect(isObjectOrArray([])).toBe(true);
      expect(isObjectOrArray([1, 2])).toBe(true);
    });

    it('should return false for null', () => {
      expect(isObjectOrArray(null)).toBe(false);
    });
  });

  describe('isArray', () => {
    it('should return true for arrays', () => {
      expect(isArray([])).toBe(true);
      expect(isArray([1, 2, 3])).toBe(true);
    });

    it('should return false for objects', () => {
      expect(isArray({})).toBe(false);
    });
  });

  describe('isTypedArray', () => {
    it('should validate array items with guard', () => {
      const isStringArray = (v: unknown): v is string[] => isTypedArray(v, isString);
      expect(isStringArray(['a', 'b'])).toBe(true);
      expect(isStringArray(['a', 1])).toBe(false);
    });
  });

  // ============================================================================
  // Property Type Guards
  // ============================================================================

  describe('hasProperty', () => {
    it('should check for property existence', () => {
      expect(hasProperty({ name: 'test' }, 'name')).toBe(true);
      expect(hasProperty({}, 'name')).toBe(false);
    });

    it('should return false for non-objects', () => {
      expect(hasProperty(null, 'name')).toBe(false);
      expect(hasProperty('string', 'length')).toBe(false);
    });
  });

  describe('hasPropertyOfType', () => {
    it('should check property type', () => {
      expect(hasPropertyOfType({ age: 25 }, 'age', isNumber)).toBe(true);
      expect(hasPropertyOfType({ age: '25' }, 'age', isNumber)).toBe(false);
    });
  });

  describe('hasStringProperty', () => {
    it('should check for string property', () => {
      expect(hasStringProperty({ name: 'test' }, 'name')).toBe(true);
      expect(hasStringProperty({ name: 123 }, 'name')).toBe(false);
    });
  });

  describe('hasNumberProperty', () => {
    it('should check for number property', () => {
      expect(hasNumberProperty({ count: 5 }, 'count')).toBe(true);
      expect(hasNumberProperty({ count: '5' }, 'count')).toBe(false);
    });
  });

  describe('hasBooleanProperty', () => {
    it('should check for boolean property', () => {
      expect(hasBooleanProperty({ active: true }, 'active')).toBe(true);
      expect(hasBooleanProperty({ active: 'true' }, 'active')).toBe(false);
    });
  });

  describe('hasArrayProperty', () => {
    it('should check for array property', () => {
      expect(hasArrayProperty({ items: [1, 2] }, 'items')).toBe(true);
      expect(hasArrayProperty({ items: {} }, 'items')).toBe(false);
    });
  });

  describe('hasTypedArrayProperty', () => {
    it('should check for typed array property', () => {
      expect(hasTypedArrayProperty({ names: ['a', 'b'] }, 'names', isString)).toBe(true);
      expect(hasTypedArrayProperty({ names: ['a', 1] }, 'names', isString)).toBe(false);
    });
  });

  describe('hasObjectProperty', () => {
    it('should check for object property', () => {
      expect(hasObjectProperty({ config: { a: 1 } }, 'config')).toBe(true);
      expect(hasObjectProperty({ config: [] }, 'config')).toBe(false);
    });
  });

  describe('hasFunctionProperty', () => {
    it('should check for function property', () => {
      expect(hasFunctionProperty({ handler: () => {} }, 'handler')).toBe(true);
      expect(hasFunctionProperty({ handler: 'fn' }, 'handler')).toBe(false);
    });
  });

  describe('hasStringProperties', () => {
    it('should check for multiple string properties', () => {
      expect(hasStringProperties({ a: 'x', b: 'y' }, ['a', 'b'])).toBe(true);
      expect(hasStringProperties({ a: 'x', b: 1 }, ['a', 'b'])).toBe(false);
    });
  });

  describe('hasProperties', () => {
    it('should check for multiple properties', () => {
      expect(hasProperties({ a: 1, b: 'x' }, ['a', 'b'])).toBe(true);
      expect(hasProperties({ a: 1 }, ['a', 'b'])).toBe(false);
    });
  });

  // ============================================================================
  // Property Accessors
  // ============================================================================

  describe('getStringProperty', () => {
    it('should get string property or undefined', () => {
      expect(getStringProperty({ name: 'test' }, 'name')).toBe('test');
      expect(getStringProperty({ name: 123 }, 'name')).toBeUndefined();
      expect(getStringProperty({}, 'name')).toBeUndefined();
    });
  });

  describe('getNumberProperty', () => {
    it('should get number property or undefined', () => {
      expect(getNumberProperty({ age: 25 }, 'age')).toBe(25);
      expect(getNumberProperty({ age: '25' }, 'age')).toBeUndefined();
    });
  });

  describe('getBooleanProperty', () => {
    it('should get boolean property or undefined', () => {
      expect(getBooleanProperty({ active: true }, 'active')).toBe(true);
      expect(getBooleanProperty({ active: 1 }, 'active')).toBeUndefined();
    });
  });

  describe('getArrayProperty', () => {
    it('should get array property or undefined', () => {
      expect(getArrayProperty({ items: [1, 2] }, 'items')).toEqual([1, 2]);
      expect(getArrayProperty({ items: 'not array' }, 'items')).toBeUndefined();
    });
  });

  describe('getObjectProperty', () => {
    it('should get object property or undefined', () => {
      expect(getObjectProperty({ config: { a: 1 } }, 'config')).toEqual({ a: 1 });
      expect(getObjectProperty({ config: [] }, 'config')).toBeUndefined();
    });
  });

  describe('getTypedProperty', () => {
    it('should get typed property or undefined', () => {
      expect(getTypedProperty({ count: 5 }, 'count', isNumber)).toBe(5);
      expect(getTypedProperty({ count: '5' }, 'count', isNumber)).toBeUndefined();
    });
  });

  // ============================================================================
  // Common Object Shape Guards
  // ============================================================================

  describe('hasErrorCode', () => {
    it('should check for error code property', () => {
      expect(hasErrorCode({ code: 'ERR001' })).toBe(true);
      expect(hasErrorCode({ code: 123 })).toBe(false);
      expect(hasErrorCode({})).toBe(false);
    });
  });

  describe('hasTypeProperty', () => {
    it('should check for type property', () => {
      expect(hasTypeProperty({ type: 'user' })).toBe(true);
      expect(hasTypeProperty({ kind: 'user' })).toBe(false);
    });
  });

  describe('isErrorLike', () => {
    it('should check for error-like objects', () => {
      expect(isErrorLike({ name: 'Error', message: 'test' })).toBe(true);
      expect(isErrorLike(new Error('test'))).toBe(true);
      expect(isErrorLike({ message: 'test' })).toBe(false);
    });
  });

  describe('hasTypeDiscriminator', () => {
    it('should check for specific type value', () => {
      expect(hasTypeDiscriminator({ type: 'insert' }, 'insert')).toBe(true);
      expect(hasTypeDiscriminator({ type: 'update' }, 'insert')).toBe(false);
    });
  });

  // ============================================================================
  // Record Iteration Helpers
  // ============================================================================

  describe('iterateRecord', () => {
    it('should iterate over object entries', () => {
      const entries = [...iterateRecord({ a: 1, b: 2 })];
      expect(entries).toEqual([['a', 1], ['b', 2]]);
    });

    it('should return empty for non-objects', () => {
      expect([...iterateRecord(null)]).toEqual([]);
      expect([...iterateRecord([1, 2])]).toEqual([]);
    });
  });

  describe('getKeys', () => {
    it('should get object keys', () => {
      expect(getKeys({ a: 1, b: 2 })).toEqual(['a', 'b']);
    });

    it('should return empty for non-objects', () => {
      expect(getKeys(null)).toEqual([]);
    });
  });

  describe('getValues', () => {
    it('should get object values', () => {
      expect(getValues({ a: 1, b: 2 })).toEqual([1, 2]);
    });
  });

  describe('getEntries', () => {
    it('should get object entries', () => {
      expect(getEntries({ a: 1 })).toEqual([['a', 1]]);
    });
  });

  // ============================================================================
  // Primitive Type Guards
  // ============================================================================

  describe('isString', () => {
    it('should check for strings', () => {
      expect(isString('test')).toBe(true);
      expect(isString('')).toBe(true);
      expect(isString(123)).toBe(false);
    });
  });

  describe('isNumber', () => {
    it('should check for numbers', () => {
      expect(isNumber(123)).toBe(true);
      expect(isNumber(0)).toBe(true);
      expect(isNumber('123')).toBe(false);
    });
  });

  describe('isBoolean', () => {
    it('should check for booleans', () => {
      expect(isBoolean(true)).toBe(true);
      expect(isBoolean(false)).toBe(true);
      expect(isBoolean(1)).toBe(false);
    });
  });

  describe('isBigInt', () => {
    it('should check for bigints', () => {
      expect(isBigInt(BigInt(123))).toBe(true);
      expect(isBigInt(123)).toBe(false);
    });
  });

  describe('isFunction', () => {
    it('should check for functions', () => {
      expect(isFunction(() => {})).toBe(true);
      expect(isFunction(function() {})).toBe(true);
      expect(isFunction({})).toBe(false);
    });
  });

  describe('isNullish', () => {
    it('should check for null or undefined', () => {
      expect(isNullish(null)).toBe(true);
      expect(isNullish(undefined)).toBe(true);
      expect(isNullish(0)).toBe(false);
      expect(isNullish('')).toBe(false);
    });
  });

  describe('isDefined', () => {
    it('should check for non-null/undefined values', () => {
      expect(isDefined(0)).toBe(true);
      expect(isDefined('')).toBe(true);
      expect(isDefined(null)).toBe(false);
      expect(isDefined(undefined)).toBe(false);
    });
  });

  // ============================================================================
  // Casting Helpers
  // ============================================================================

  describe('asRecord', () => {
    it('should cast to record or return undefined', () => {
      expect(asRecord({ a: 1 })).toEqual({ a: 1 });
      expect(asRecord([])).toBeUndefined();
      expect(asRecord(null)).toBeUndefined();
    });
  });

  describe('asRecordOrThrow', () => {
    it('should cast to record or throw', () => {
      expect(asRecordOrThrow({ a: 1 })).toEqual({ a: 1 });
      expect(() => asRecordOrThrow([])).toThrow(TypeError);
      expect(() => asRecordOrThrow(null, 'Custom message')).toThrow('Custom message');
    });
  });

  describe('asArray', () => {
    it('should cast to array or return undefined', () => {
      expect(asArray([1, 2])).toEqual([1, 2]);
      expect(asArray({})).toBeUndefined();
    });
  });

  describe('asTypedArray', () => {
    it('should cast to typed array or return undefined', () => {
      expect(asTypedArray(['a', 'b'], isString)).toEqual(['a', 'b']);
      expect(asTypedArray(['a', 1], isString)).toBeUndefined();
      expect(asTypedArray({}, isString)).toBeUndefined();
    });
  });

  // ============================================================================
  // SQL Value Guards
  // ============================================================================

  describe('isSqlValue', () => {
    it('should validate SQL-compatible values', () => {
      expect(isSqlValue('text')).toBe(true);
      expect(isSqlValue(123)).toBe(true);
      expect(isSqlValue(true)).toBe(true);
      expect(isSqlValue(null)).toBe(true);
      expect(isSqlValue(BigInt(123))).toBe(true);
      expect(isSqlValue(new Uint8Array([1, 2]))).toBe(true);
      expect(isSqlValue({})).toBe(false);
      expect(isSqlValue([])).toBe(false);
    });
  });

  describe('isRow', () => {
    it('should validate row objects', () => {
      expect(isRow({ id: 1, name: 'test', active: true })).toBe(true);
      expect(isRow({ id: 1, data: {} })).toBe(false);
    });

    it('should return false for non-objects', () => {
      expect(isRow(null)).toBe(false);
      expect(isRow([])).toBe(false);
    });
  });
});
