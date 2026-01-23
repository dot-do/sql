/**
 * Sandbox Module Utilities
 *
 * Shared utilities for building sandboxed module wrappers used by
 * stored procedures and triggers. Provides consistent table accessor
 * generation and RPC handler patterns.
 */

import type { EvaluateOptions } from 'ai-evaluate';

// =============================================================================
// TYPES
// =============================================================================

/**
 * Database context interface for sandbox execution
 */
export interface SandboxDatabaseContext {
  tables: Record<string, {
    get(key: string | number): Promise<unknown>;
    where(predicate: unknown, options?: unknown): Promise<unknown[]>;
    all(options?: unknown): Promise<unknown[]>;
    count(predicate?: unknown): Promise<number>;
    insert(record: unknown): Promise<unknown>;
    update(predicate: unknown, changes: unknown): Promise<number>;
    delete(predicate: unknown): Promise<number>;
  }>;
  sql?: (strings: TemplateStringsArray, ...values: unknown[]) => Promise<unknown[]>;
  transaction?: <T>(callback: (tx: unknown) => Promise<T>) => Promise<T>;
}

/**
 * Options for building sandbox module wrapper
 */
export interface SandboxModuleOptions {
  /** Table names to create accessors for */
  tableNames: string[];
  /** Handler code to include */
  handlerCode: string;
  /** Whether to include SQL template literal support */
  includeSql?: boolean;
  /** Whether to include transaction support */
  includeTransaction?: boolean;
}

/**
 * Options for sandbox execution
 */
export interface SandboxExecutionOptions {
  /** Timeout in milliseconds */
  timeout: number;
  /** Environment variables */
  env: Record<string, string>;
  /** Database context for RPC handling */
  db: SandboxDatabaseContext;
}

// =============================================================================
// TABLE ACCESSOR GENERATION
// =============================================================================

/**
 * Build table accessor code for a single table
 */
export function buildTableAccessorCode(tableName: string): string {
  return `
    ${tableName}: {
      async get(key) { return __dbCall('${tableName}', 'get', [key]); },
      async where(predicate, options) {
        if (typeof predicate === 'function') {
          const all = await __dbCall('${tableName}', 'all', []);
          return all.filter(predicate);
        }
        return __dbCall('${tableName}', 'where', [predicate, options]);
      },
      async all(options) { return __dbCall('${tableName}', 'all', [options]); },
      async count(predicate) {
        if (typeof predicate === 'function') {
          const all = await __dbCall('${tableName}', 'all', []);
          return all.filter(predicate).length;
        }
        return __dbCall('${tableName}', 'count', [predicate]);
      },
      async insert(record) { return __dbCall('${tableName}', 'insert', [record]); },
      async update(predicate, changes) {
        if (typeof predicate === 'function') {
          throw new Error('Function predicates not supported for update');
        }
        return __dbCall('${tableName}', 'update', [predicate, changes]);
      },
      async delete(predicate) {
        if (typeof predicate === 'function') {
          throw new Error('Function predicates not supported for delete');
        }
        return __dbCall('${tableName}', 'delete', [predicate]);
      },
    }`;
}

/**
 * Build the __dbCall function code
 */
export function buildDbCallCode(): string {
  return `
async function __dbCall(table, method, args) {
  const response = await fetch('db://internal/call', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ table, method, args }),
  });
  if (!response.ok) {
    const error = await response.text();
    throw new Error(\`Database error: \${error}\`);
  }
  return response.json();
}`;
}

/**
 * Build SQL template literal function code
 */
export function buildSqlCode(): string {
  return `
async function sql(strings, ...values) {
  const response = await fetch('db://internal/sql', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ strings: Array.from(strings), values }),
  });
  if (!response.ok) {
    const error = await response.text();
    throw new Error(\`SQL error: \${error}\`);
  }
  return response.json();
}`;
}

/**
 * Build transaction wrapper code
 */
export function buildTransactionCode(): string {
  return `
async function transaction(callback) {
  const startRes = await fetch('db://internal/tx/begin', { method: 'POST' });
  if (!startRes.ok) throw new Error('Failed to start transaction');
  const { txId } = await startRes.json();

  try {
    const txCtx = {
      tables: db.tables,
      sql,
      commit: async () => {
        await fetch('db://internal/tx/commit', {
          method: 'POST',
          body: JSON.stringify({ txId }),
        });
      },
      rollback: async () => {
        await fetch('db://internal/tx/rollback', {
          method: 'POST',
          body: JSON.stringify({ txId }),
        });
      },
    };

    const result = await callback(txCtx);
    await txCtx.commit();
    return result;
  } catch (error) {
    await fetch('db://internal/tx/rollback', {
      method: 'POST',
      body: JSON.stringify({ txId }),
    });
    throw error;
  }
}`;
}

// =============================================================================
// MODULE WRAPPER BUILDERS
// =============================================================================

/**
 * Build a sandbox module wrapper for trigger execution
 *
 * This is the simpler version used by triggers that only needs
 * table accessors and the handler.
 */
export function buildSandboxModule(
  handlerCode: string,
  tableNames: string[]
): string {
  const tableAccessors = tableNames.map(buildTableAccessorCode).join(',\n');

  return `${buildDbCallCode()}

const db = {
  tables: { ${tableAccessors} },
};

${tableNames.map(name => `db.${name} = db.tables.${name};`).join('\n')}

const handler = ${handlerCode};

export default handler;
`;
}

/**
 * Build a sandbox module wrapper for procedure execution
 *
 * This is the extended version used by procedures that includes
 * SQL template literal support and transactions.
 */
export function buildProcedureModule(
  procedureCode: string,
  tableNames: string[]
): string {
  const tableAccessors = tableNames.map(buildTableAccessorCode).join(',\n');

  return `${buildDbCallCode()}

${buildSqlCode()}

${buildTransactionCode()}

const db = {
  tables: { ${tableAccessors} },
  sql,
  transaction,
};

${tableNames.map(name => `db.${name} = db.tables.${name};`).join('\n')}

${procedureCode}
`;
}

/**
 * Build a custom sandbox module with configurable features
 */
export function buildCustomSandboxModule(options: SandboxModuleOptions): string {
  const { tableNames, handlerCode, includeSql, includeTransaction } = options;
  const tableAccessors = tableNames.map(buildTableAccessorCode).join(',\n');

  let code = buildDbCallCode() + '\n';

  if (includeSql) {
    code += buildSqlCode() + '\n';
  }

  if (includeTransaction) {
    code += buildTransactionCode() + '\n';
  }

  code += `
const db = {
  tables: { ${tableAccessors} },${includeSql ? '\n  sql,' : ''}${includeTransaction ? '\n  transaction,' : ''}
};

${tableNames.map(name => `db.${name} = db.tables.${name};`).join('\n')}

${handlerCode}
`;

  return code;
}

// =============================================================================
// OUTBOUND RPC HANDLER
// =============================================================================

/**
 * Create an outbound RPC handler for database operations
 *
 * This handles `db://internal/*` URLs and proxies calls to the actual
 * database context.
 */
export function createOutboundRpcHandler(
  db: SandboxDatabaseContext
): (url: string, request: Request) => Response | Promise<Response> | null {
  return (url: string, request: Request): Response | Promise<Response> | null => {
    if (!url.startsWith('db://internal/')) {
      return null;
    }

    const path = url.replace('db://internal/', '');

    return (async (): Promise<Response> => {
      try {
        if (path === 'call') {
          const body = await request.json() as { table: string; method: string; args: unknown[] };
          const { table, method, args } = body;

          const tableAccessor = db.tables[table];
          if (!tableAccessor) {
            return new Response(`Table '${table}' not found`, { status: 404 });
          }

          const fn = (tableAccessor as unknown as Record<string, (...args: unknown[]) => Promise<unknown>>)[method];
          if (typeof fn !== 'function') {
            return new Response(`Method '${method}' not found`, { status: 404 });
          }

          const result = await fn.apply(tableAccessor, args);
          return new Response(JSON.stringify(result), {
            headers: { 'Content-Type': 'application/json' },
          });
        }

        if (path === 'sql' && db.sql) {
          const body = await request.json() as { strings: string[]; values: unknown[] };
          const { strings, values } = body;
          const templateStrings = Object.assign(strings, { raw: strings }) as TemplateStringsArray;
          const result = await db.sql(templateStrings, ...values);
          return new Response(JSON.stringify(result), {
            headers: { 'Content-Type': 'application/json' },
          });
        }

        if (path === 'tx/begin') {
          return new Response(JSON.stringify({ txId: `tx_${Date.now()}` }), {
            headers: { 'Content-Type': 'application/json' },
          });
        }

        if (path === 'tx/commit' || path === 'tx/rollback') {
          return new Response(JSON.stringify({ success: true }), {
            headers: { 'Content-Type': 'application/json' },
          });
        }

        return new Response(`Unknown path: ${path}`, { status: 404 });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        return new Response(message, { status: 500 });
      }
    })();
  };
}

/**
 * Create an outbound RPC handler with custom path handlers
 */
export function createExtendedOutboundRpcHandler(
  db: SandboxDatabaseContext,
  customHandlers?: Record<string, (request: Request) => Promise<Response>>
): (url: string, request: Request) => Response | Promise<Response> | null {
  const baseHandler = createOutboundRpcHandler(db);

  return (url: string, request: Request): Response | Promise<Response> | null => {
    if (!url.startsWith('db://internal/')) {
      return null;
    }

    const path = url.replace('db://internal/', '');

    // Check custom handlers first
    if (customHandlers && path in customHandlers) {
      return customHandlers[path](request);
    }

    // Fall back to base handler
    return baseHandler(url, request);
  };
}
