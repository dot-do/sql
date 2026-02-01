/**
 * Exhaustiveness checking utility for discriminated unions.
 *
 * Use in the `default` case of switch statements on discriminated unions
 * to get compile-time errors when new variants are added but not handled.
 *
 * @example
 * ```typescript
 * type Shape = { type: 'circle'; radius: number } | { type: 'square'; side: number };
 *
 * function area(shape: Shape): number {
 *   switch (shape.type) {
 *     case 'circle': return Math.PI * shape.radius ** 2;
 *     case 'square': return shape.side ** 2;
 *     default: return assertNever(shape);
 *   }
 * }
 * ```
 *
 * If a new variant (e.g., `{ type: 'triangle' }`) is added to the union,
 * TypeScript will report a compile-time error because `triangle` is not
 * assignable to `never`.
 *
 * @param value - The value that should be unreachable (typed as `never`)
 * @param message - Optional custom error message
 * @returns never - Always throws
 */
export function assertNever(value: never, message?: string): never {
  throw new Error(message || `Unexpected value: ${JSON.stringify(value)}`);
}
