/**
 * In-Memory Storage Implementation
 *
 * Simple in-memory key-value storage for testing and embedding.
 */

import type { KVStorage } from './types.js';

/**
 * In-memory key-value storage backed by a Map.
 * Useful for testing, embedding, and scenarios where persistence is not needed.
 */
export class MemoryStorage implements KVStorage {
  private data = new Map<string, Record<string, unknown>>();

  async get(key: string): Promise<Record<string, unknown> | undefined> {
    const value = this.data.get(key);
    // Return a copy to prevent mutation
    return value ? { ...value } : undefined;
  }

  async set(key: string, value: Record<string, unknown>): Promise<void> {
    // Store a copy to prevent external mutation
    this.data.set(key, { ...value });
  }

  async delete(key: string): Promise<boolean> {
    return this.data.delete(key);
  }

  async *range(
    start: string,
    end: string
  ): AsyncIterable<[string, Record<string, unknown>]> {
    // Get all keys, sort them, and filter to range
    const keys = [...this.data.keys()].sort();

    for (const key of keys) {
      if (key >= start && key < end) {
        const value = this.data.get(key);
        if (value) {
          yield [key, { ...value }];
        }
      }
    }
  }

  /**
   * Clear all data (useful for test cleanup)
   */
  clear(): void {
    this.data.clear();
  }

  /**
   * Get the number of stored entries
   */
  get size(): number {
    return this.data.size;
  }

  /**
   * Get all keys (for debugging)
   */
  keys(): string[] {
    return [...this.data.keys()];
  }
}

/**
 * Create a new in-memory storage instance
 */
export function createMemoryStorage(): MemoryStorage {
  return new MemoryStorage();
}
