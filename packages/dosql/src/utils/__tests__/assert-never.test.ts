import { describe, it, expect } from 'vitest';
import { assertNever } from '../assert-never.js';

describe('assertNever', () => {
  it('throws with default message including the value', () => {
    const value = 'unexpected' as never;
    expect(() => assertNever(value)).toThrow('Unexpected value: "unexpected"');
  });

  it('throws with custom message when provided', () => {
    const value = 42 as never;
    expect(() => assertNever(value, 'Custom error')).toThrow('Custom error');
  });

  it('handles object values in default message', () => {
    const value = { type: 'unknown' } as never;
    expect(() => assertNever(value)).toThrow('Unexpected value: {"type":"unknown"}');
  });

  it('handles null value in default message', () => {
    const value = null as never;
    expect(() => assertNever(value)).toThrow('Unexpected value: null');
  });

  it('provides exhaustiveness checking for discriminated unions', () => {
    type Action =
      | { type: 'add'; value: number }
      | { type: 'remove'; id: string };

    function handleAction(action: Action): string {
      switch (action.type) {
        case 'add':
          return `added ${action.value}`;
        case 'remove':
          return `removed ${action.id}`;
        default:
          return assertNever(action);
      }
    }

    expect(handleAction({ type: 'add', value: 1 })).toBe('added 1');
    expect(handleAction({ type: 'remove', id: 'x' })).toBe('removed x');
  });
});
