/**
 * Type declarations for cloudflare-worker-sqlite-wasm
 *
 * This module provides SQLite compiled to WebAssembly, optimized for
 * Cloudflare Workers runtime environment.
 *
 * @module cloudflare-worker-sqlite-wasm
 */

declare module 'cloudflare-worker-sqlite-wasm/dist/sql-cf-wasm.wasm' {
  /**
   * Pre-compiled WebAssembly module for SQLite.
   * This is the WASM binary that can be instantiated directly.
   */
  const wasmModule: WebAssembly.Module;
  export default wasmModule;
}

declare module 'cloudflare-worker-sqlite-wasm/dist/sql-cf-wasm.js' {
  /**
   * sql.js Database interface (Cloudflare Workers fork)
   */
  interface SqlJsDatabase {
    run(sql: string, params?: unknown[]): SqlJsDatabase;
    exec(sql: string): Array<{ columns: string[]; values: unknown[][] }>;
    prepare(sql: string): SqlJsStatement;
    close(): void;
    getRowsModified(): number;
  }

  /**
   * sql.js Statement interface
   */
  interface SqlJsStatement {
    bind(params?: unknown[] | Record<string, unknown>): boolean;
    step(): boolean;
    getAsObject(params?: Record<string, unknown>): Record<string, unknown>;
    get(params?: Record<string, unknown>): unknown[];
    free(): boolean;
    reset(): void;
    run(params?: unknown[]): void;
  }

  /**
   * sql.js Module interface
   */
  interface SqlJsModule {
    Database: new (data?: ArrayLike<number>) => SqlJsDatabase;
  }

  /**
   * sql.js initialization options
   */
  interface SqlJsInitOptions {
    /** Custom WASM file URL */
    locateFile?: (file: string) => string;
    /** Custom WASM instantiation (for Workers) */
    instantiateWasm?: (
      info: WebAssembly.Imports,
      receive: (instance: WebAssembly.Instance) => void
    ) => WebAssembly.Exports;
  }

  /**
   * Initialize sql.js and return the SQL module.
   *
   * @param options - Initialization options
   * @returns Promise resolving to the sql.js module
   */
  function initSqlJs(options?: SqlJsInitOptions): Promise<SqlJsModule>;

  export default initSqlJs;
  export type {
    SqlJsDatabase,
    SqlJsStatement,
    SqlJsModule,
    SqlJsInitOptions,
  };
}
