/**
 * Tests for window clause parser module
 *
 * Verifies parsing of SQL window specifications:
 * - Window function calls (ROW_NUMBER, RANK, SUM, etc.)
 * - OVER clause parsing (inline and named)
 * - PARTITION BY clause
 * - ORDER BY clause with direction and NULLS position
 * - Frame specifications (ROWS, RANGE, GROUPS)
 * - Frame boundaries (UNBOUNDED, CURRENT ROW, n PRECEDING/FOLLOWING)
 * - Frame exclusion (EXCLUDE CURRENT ROW, GROUP, TIES, NO OTHERS)
 * - WINDOW clause for named window definitions
 * - Window spec resolution against named windows
 * - Window spec validation
 */

import { describe, it, expect } from 'vitest';
import {
  parseWindowFunctionCall,
  parseOverClause,
  parseWindowClause,
  resolveWindowSpec,
  validateWindowSpec,
  type WindowSpec,
  type FrameSpec,
} from '../window.js';

// =============================================================================
// Window Function Call Parsing
// =============================================================================

describe('Window Parser - Function Call Parsing', () => {
  it('should parse ROW_NUMBER() OVER ()', () => {
    const result = parseWindowFunctionCall('ROW_NUMBER() OVER ()');
    expect(result).not.toBeNull();
    expect(result!.function.name).toBe('ROW_NUMBER');
    expect(result!.function.over).toBeDefined();
  });

  it('should parse COUNT(*) with OVER clause', () => {
    const result = parseWindowFunctionCall('COUNT(*) OVER (PARTITION BY dept)');
    expect(result).not.toBeNull();
    expect(result!.function.name).toBe('COUNT');
    expect(result!.function.args).toHaveLength(1);
    expect(result!.function.args[0]).toEqual({ type: 'column', column: '*' });
  });

  it('should parse SUM with argument', () => {
    const result = parseWindowFunctionCall('SUM(salary) OVER (PARTITION BY dept)');
    expect(result).not.toBeNull();
    expect(result!.function.name).toBe('SUM');
  });

  it('should parse function with DISTINCT', () => {
    const result = parseWindowFunctionCall('COUNT(DISTINCT category) OVER ()');
    expect(result).not.toBeNull();
    expect(result!.function.distinct).toBe(true);
  });

  it('should parse function without OVER (non-window)', () => {
    const result = parseWindowFunctionCall('COUNT(*)');
    expect(result).not.toBeNull();
    expect(result!.function.name).toBe('COUNT');
    expect(result!.function.over).toBeUndefined();
  });

  it('should parse function with named window reference', () => {
    const result = parseWindowFunctionCall('SUM(amount) OVER w');
    expect(result).not.toBeNull();
    expect(result!.function.over).toBe('w');
  });

  it('should return null for non-function input', () => {
    const result = parseWindowFunctionCall('42');
    expect(result).toBeNull();
  });

  it('should parse function with multiple arguments', () => {
    const result = parseWindowFunctionCall('LEAD(salary, 1) OVER ()');
    expect(result).not.toBeNull();
    expect(result!.function.name).toBe('LEAD');
    expect(result!.function.args).toHaveLength(2);
  });
});

// =============================================================================
// OVER Clause Parsing
// =============================================================================

describe('Window Parser - OVER Clause', () => {
  it('should parse empty OVER clause', () => {
    const result = parseOverClause('OVER ()');
    expect(result).not.toBeNull();
    expect(typeof result).toBe('object');
  });

  it('should parse OVER with PARTITION BY', () => {
    const result = parseOverClause('OVER (PARTITION BY dept)') as WindowSpec;
    expect(result).not.toBeNull();
    expect(result.partitionBy).toBeDefined();
    expect(result.partitionBy).toHaveLength(1);
  });

  it('should parse OVER with ORDER BY', () => {
    const result = parseOverClause('OVER (ORDER BY salary DESC)') as WindowSpec;
    expect(result).not.toBeNull();
    expect(result.orderBy).toBeDefined();
    expect(result.orderBy).toHaveLength(1);
    expect(result.orderBy![0].direction).toBe('desc');
  });

  it('should parse OVER with ORDER BY ASC', () => {
    const result = parseOverClause('OVER (ORDER BY name ASC)') as WindowSpec;
    expect(result).not.toBeNull();
    expect(result.orderBy![0].direction).toBe('asc');
  });

  it('should parse OVER with ORDER BY NULLS FIRST', () => {
    const result = parseOverClause('OVER (ORDER BY name NULLS FIRST)') as WindowSpec;
    expect(result).not.toBeNull();
    expect(result.orderBy![0].nulls).toBe('first');
  });

  it('should parse OVER with ORDER BY NULLS LAST', () => {
    const result = parseOverClause('OVER (ORDER BY name NULLS LAST)') as WindowSpec;
    expect(result).not.toBeNull();
    expect(result.orderBy![0].nulls).toBe('last');
  });

  it('should parse OVER with both PARTITION BY and ORDER BY', () => {
    const result = parseOverClause('OVER (PARTITION BY dept ORDER BY salary)') as WindowSpec;
    expect(result).not.toBeNull();
    expect(result.partitionBy).toHaveLength(1);
    expect(result.orderBy).toHaveLength(1);
  });

  it('should parse OVER with named window reference', () => {
    const result = parseOverClause('OVER w');
    expect(result).toBe('w');
  });

  it('should return null for non-OVER input', () => {
    const result = parseOverClause('SELECT');
    expect(result).toBeNull();
  });

  it('should parse OVER with multiple PARTITION BY columns', () => {
    const result = parseOverClause('OVER (PARTITION BY dept, region)') as WindowSpec;
    expect(result).not.toBeNull();
    expect(result.partitionBy).toHaveLength(2);
  });

  it('should parse OVER with multiple ORDER BY items', () => {
    const result = parseOverClause('OVER (ORDER BY dept ASC, salary DESC)') as WindowSpec;
    expect(result).not.toBeNull();
    expect(result.orderBy).toHaveLength(2);
    expect(result.orderBy![0].direction).toBe('asc');
    expect(result.orderBy![1].direction).toBe('desc');
  });
});

// =============================================================================
// Frame Specification Parsing
// =============================================================================

describe('Window Parser - Frame Specifications', () => {
  it('should parse ROWS frame with BETWEEN', () => {
    const result = parseOverClause(
      'OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)',
    ) as WindowSpec;
    expect(result).not.toBeNull();
    expect(result.frame).toBeDefined();
    expect(result.frame!.mode).toBe('rows');
    expect(result.frame!.start.type).toBe('unboundedPreceding');
    expect(result.frame!.end.type).toBe('currentRow');
  });

  it('should parse RANGE frame', () => {
    const result = parseOverClause(
      'OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)',
    ) as WindowSpec;
    expect(result).not.toBeNull();
    expect(result.frame!.mode).toBe('range');
  });

  it('should parse GROUPS frame', () => {
    const result = parseOverClause(
      'OVER (ORDER BY id GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)',
    ) as WindowSpec;
    expect(result).not.toBeNull();
    expect(result.frame!.mode).toBe('groups');
  });

  it('should parse n PRECEDING', () => {
    const result = parseOverClause(
      'OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)',
    ) as WindowSpec;
    expect(result).not.toBeNull();
    expect(result.frame!.start.type).toBe('preceding');
    expect(result.frame!.start.offset).toBe(3);
  });

  it('should parse n FOLLOWING', () => {
    const result = parseOverClause(
      'OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 5 FOLLOWING)',
    ) as WindowSpec;
    expect(result).not.toBeNull();
    expect(result.frame!.end.type).toBe('following');
    expect(result.frame!.end.offset).toBe(5);
  });

  it('should parse UNBOUNDED FOLLOWING', () => {
    const result = parseOverClause(
      'OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)',
    ) as WindowSpec;
    expect(result).not.toBeNull();
    expect(result.frame!.end.type).toBe('unboundedFollowing');
  });

  it('should parse single boundary (defaults end to CURRENT ROW)', () => {
    const result = parseOverClause(
      'OVER (ORDER BY id ROWS UNBOUNDED PRECEDING)',
    ) as WindowSpec;
    expect(result).not.toBeNull();
    expect(result.frame!.start.type).toBe('unboundedPreceding');
    expect(result.frame!.end.type).toBe('currentRow');
  });

  it('should parse frame with EXCLUDE CURRENT ROW', () => {
    const result = parseOverClause(
      'OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW)',
    ) as WindowSpec;
    expect(result).not.toBeNull();
    expect(result.frame!.exclusion).toBe('currentRow');
  });

  it('should parse frame with EXCLUDE GROUP', () => {
    const result = parseOverClause(
      'OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP)',
    ) as WindowSpec;
    expect(result).not.toBeNull();
    expect(result.frame!.exclusion).toBe('group');
  });

  it('should parse frame with EXCLUDE TIES', () => {
    const result = parseOverClause(
      'OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES)',
    ) as WindowSpec;
    expect(result).not.toBeNull();
    expect(result.frame!.exclusion).toBe('ties');
  });

  it('should parse frame with EXCLUDE NO OTHERS', () => {
    const result = parseOverClause(
      'OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE NO OTHERS)',
    ) as WindowSpec;
    expect(result).not.toBeNull();
    expect(result.frame!.exclusion).toBe('noOthers');
  });
});

// =============================================================================
// WINDOW Clause (Named Windows)
// =============================================================================

describe('Window Parser - Named Window Definitions', () => {
  it('should return null for non-WINDOW input', () => {
    const state = { input: 'SELECT', position: 0 };
    const result = parseWindowClause(state);
    expect(result).toBeNull();
  });
});

// =============================================================================
// Window Spec Resolution
// =============================================================================

describe('Window Parser - Spec Resolution', () => {
  it('should resolve named window reference', () => {
    const namedWindows = new Map<string, WindowSpec>();
    namedWindows.set('w', {
      partitionBy: [{ type: 'column', column: 'dept' }],
      orderBy: [{ expression: { type: 'column', column: 'salary' }, direction: 'desc' }],
    });

    const resolved = resolveWindowSpec('w', namedWindows);
    expect(resolved.partitionBy).toHaveLength(1);
    expect(resolved.orderBy).toHaveLength(1);
  });

  it('should throw for unknown window name', () => {
    const namedWindows = new Map<string, WindowSpec>();
    expect(() => resolveWindowSpec('nonexistent', namedWindows)).toThrow('Unknown window name');
  });

  it('should merge spec with base window', () => {
    const namedWindows = new Map<string, WindowSpec>();
    namedWindows.set('base', {
      partitionBy: [{ type: 'column', column: 'dept' }],
    });

    const spec: WindowSpec = {
      baseName: 'base',
      orderBy: [{ expression: { type: 'column', column: 'salary' }, direction: 'asc' }],
    };

    const resolved = resolveWindowSpec(spec, namedWindows);
    expect(resolved.partitionBy).toHaveLength(1);
    expect(resolved.orderBy).toHaveLength(1);
  });

  it('should override base window partitionBy when specified', () => {
    const namedWindows = new Map<string, WindowSpec>();
    namedWindows.set('base', {
      partitionBy: [{ type: 'column', column: 'dept' }],
    });

    const spec: WindowSpec = {
      baseName: 'base',
      partitionBy: [{ type: 'column', column: 'region' }],
    };

    const resolved = resolveWindowSpec(spec, namedWindows);
    expect(resolved.partitionBy).toHaveLength(1);
    expect((resolved.partitionBy![0] as { column: string }).column).toBe('region');
  });

  it('should throw for unknown baseName', () => {
    const namedWindows = new Map<string, WindowSpec>();
    const spec: WindowSpec = { baseName: 'nonexistent' };
    expect(() => resolveWindowSpec(spec, namedWindows)).toThrow('Unknown window name');
  });

  it('should return spec as-is when no baseName', () => {
    const namedWindows = new Map<string, WindowSpec>();
    const spec: WindowSpec = {
      partitionBy: [{ type: 'column', column: 'dept' }],
    };
    const resolved = resolveWindowSpec(spec, namedWindows);
    expect(resolved).toEqual(spec);
  });
});

// =============================================================================
// Window Spec Validation
// =============================================================================

describe('Window Parser - Spec Validation', () => {
  it('should validate valid window spec without frame', () => {
    expect(() =>
      validateWindowSpec({
        partitionBy: [{ type: 'column', column: 'dept' }],
        orderBy: [{ expression: { type: 'column', column: 'salary' }, direction: 'asc' }],
      }),
    ).not.toThrow();
  });

  it('should validate valid frame spec', () => {
    expect(() =>
      validateWindowSpec({
        frame: {
          mode: 'rows',
          start: { type: 'unboundedPreceding' },
          end: { type: 'currentRow' },
        },
      }),
    ).not.toThrow();
  });

  it('should throw when UNBOUNDED FOLLOWING is frame start', () => {
    expect(() =>
      validateWindowSpec({
        frame: {
          mode: 'rows',
          start: { type: 'unboundedFollowing' },
          end: { type: 'currentRow' },
        },
      }),
    ).toThrow('UNBOUNDED FOLLOWING cannot be used as frame start');
  });

  it('should throw when UNBOUNDED PRECEDING is frame end', () => {
    expect(() =>
      validateWindowSpec({
        frame: {
          mode: 'rows',
          start: { type: 'unboundedPreceding' },
          end: { type: 'unboundedPreceding' },
        },
      }),
    ).toThrow('UNBOUNDED PRECEDING cannot be used as frame end');
  });

  it('should throw when start FOLLOWING and end is PRECEDING', () => {
    expect(() =>
      validateWindowSpec({
        frame: {
          mode: 'rows',
          start: { type: 'following', offset: 1 },
          end: { type: 'preceding', offset: 1 },
        },
      }),
    ).toThrow('Frame start cannot be after frame end');
  });

  it('should throw when start FOLLOWING and end is CURRENT ROW', () => {
    expect(() =>
      validateWindowSpec({
        frame: {
          mode: 'rows',
          start: { type: 'following', offset: 1 },
          end: { type: 'currentRow' },
        },
      }),
    ).toThrow('Frame start cannot be after frame end');
  });

  it('should throw when start FOLLOWING > end FOLLOWING', () => {
    expect(() =>
      validateWindowSpec({
        frame: {
          mode: 'rows',
          start: { type: 'following', offset: 5 },
          end: { type: 'following', offset: 2 },
        },
      }),
    ).toThrow('Frame start cannot be after frame end');
  });

  it('should throw when start PRECEDING < end PRECEDING', () => {
    expect(() =>
      validateWindowSpec({
        frame: {
          mode: 'rows',
          start: { type: 'preceding', offset: 2 },
          end: { type: 'preceding', offset: 5 },
        },
      }),
    ).toThrow('Frame start cannot be after frame end');
  });

  it('should allow start FOLLOWING <= end FOLLOWING', () => {
    expect(() =>
      validateWindowSpec({
        frame: {
          mode: 'rows',
          start: { type: 'following', offset: 2 },
          end: { type: 'following', offset: 5 },
        },
      }),
    ).not.toThrow();
  });

  it('should allow start PRECEDING >= end PRECEDING', () => {
    expect(() =>
      validateWindowSpec({
        frame: {
          mode: 'rows',
          start: { type: 'preceding', offset: 5 },
          end: { type: 'preceding', offset: 2 },
        },
      }),
    ).not.toThrow();
  });
});
