/**
 * RED Phase TDD Tests: Type Duplication Resolution
 *
 * These tests document type duplication issues between client and server packages:
 * - @dotdo/sql.do (client) vs @dotdo/dosql (server)
 * - @dotdo/lake.do (client) vs @dotdo/dolake (server)
 *
 * Issue Reference: CODE_REVIEW.md Issue #3
 *
 * The problems documented here:
 * 1. QueryRequest has different fields in client vs server (namedParams, branch, etc.)
 * 2. QueryResponse has different structures (rows: T[] vs rows: unknown[][])
 * 3. CDCEvent has incompatible definitions across packages
 * 4. ClientCapabilities exists only in server package
 * 5. ColumnType has different values in client vs server
 *
 * Solution: Create a shared @dotdo/shared-types package
 */

import { describe, it, expect } from 'vitest';

// =============================================================================
// Type Imports for Comparison
// =============================================================================

// Client types (sql.do)
import type {
  QueryResult as ClientQueryResult,
  SQLValue as ClientSQLValue,
  CDCEvent as ClientCDCEvent,
  CDCOperation as ClientCDCOperation,
  ColumnType as ClientColumnType,
  TransactionId as ClientTransactionId,
  LSN as ClientLSN,
  RPCRequest as ClientRPCRequest,
  RPCResponse as ClientRPCResponse,
  RPCError as ClientRPCError,
  IsolationLevel as ClientIsolationLevel,
} from '@dotdo/sql.do';

// Server types (dosql/rpc)
import type {
  QueryRequest as ServerQueryRequest,
  QueryResponse as ServerQueryResponse,
  CDCEvent as ServerCDCEvent,
  CDCOperation as ServerCDCOperation,
  ColumnType as ServerColumnType,
  RPCError as ServerRPCError,
  RPCErrorCode,
  ConnectionOptions as ServerConnectionOptions,
} from '../../rpc/types.js';

// DoLake types (dolake)
import type {
  CDCEvent as DoLakeCDCEvent,
  CDCOperation as DoLakeCDCOperation,
  ClientCapabilities as DoLakeClientCapabilities,
} from '@dotdo/dolake';

// Lake.do types (lake.do client)
import type {
  CDCEvent as LakeClientCDCEvent,
} from '@dotdo/lake.do';

// CDC types from dosql/cdc
import type {
  CDCEvent as DoSQLCDCEvent,
  ChangeEvent,
  TransactionEvent,
} from '../../cdc/types.js';

// =============================================================================
// QueryRequest Compatibility Tests
// =============================================================================

describe('Type Duplication - QueryRequest Compatibility', () => {
  describe('Field differences between client and server', () => {
    it.fails('should have namedParams field in client QueryRequest', () => {
      // SERVER has namedParams?: Record<string, unknown>
      // CLIENT QueryOptions does NOT have namedParams
      //
      // This documents that the client cannot send named parameters
      // because the type doesn't support it, even though the server does.

      type ClientQueryOptions = {
        transactionId?: ClientTransactionId;
        asOf?: Date | ClientLSN;
        timeout?: number;
      };

      // This should fail because namedParams doesn't exist on client type
      type HasNamedParams = ClientQueryOptions extends { namedParams?: Record<string, unknown> }
        ? true
        : false;

      const result: HasNamedParams = true;
      expect(result).toBe(true);
    });

    it.fails('should have branch field in client QueryOptions', () => {
      // SERVER QueryRequest has: branch?: string
      // CLIENT QueryOptions does NOT have branch
      //
      // Multi-tenancy via branch is server-only concept not exposed to client

      type ClientQueryOptions = {
        transactionId?: ClientTransactionId;
        asOf?: Date | ClientLSN;
        timeout?: number;
      };

      type HasBranch = ClientQueryOptions extends { branch?: string } ? true : false;

      const result: HasBranch = true;
      expect(result).toBe(true);
    });

    it.fails('should have streaming field in client QueryOptions', () => {
      // SERVER QueryRequest has: streaming?: boolean
      // CLIENT QueryOptions does NOT have streaming

      type ClientQueryOptions = {
        transactionId?: ClientTransactionId;
        asOf?: Date | ClientLSN;
        timeout?: number;
      };

      type HasStreaming = ClientQueryOptions extends { streaming?: boolean } ? true : false;

      const result: HasStreaming = true;
      expect(result).toBe(true);
    });

    it.fails('should have limit/offset fields in client QueryOptions', () => {
      // SERVER QueryRequest has: limit?: number, offset?: number
      // CLIENT QueryOptions does NOT have pagination fields

      type ClientQueryOptions = {
        transactionId?: ClientTransactionId;
        asOf?: Date | ClientLSN;
        timeout?: number;
      };

      type HasPagination = ClientQueryOptions extends { limit?: number; offset?: number }
        ? true
        : false;

      const result: HasPagination = true;
      expect(result).toBe(true);
    });
  });

  describe('Runtime shape compatibility', () => {
    it.fails('client request object should be assignable to server request type', () => {
      // A request created with client types should be valid for server

      const clientRequest = {
        sql: 'SELECT * FROM users WHERE id = ?',
        params: [1],
        // Client doesn't know about these server fields:
        // namedParams, branch, streaming, limit, offset
      };

      // This shape check simulates runtime compatibility
      // The server expects certain optional fields that client doesn't provide
      const serverFields = ['sql', 'params', 'namedParams', 'branch', 'asOf', 'timeoutMs', 'streaming', 'limit', 'offset'];
      const clientFields = Object.keys(clientRequest);

      // Client should provide all fields server expects (at least optionally)
      // This fails because client doesn't know about server-specific fields
      const allFieldsKnown = serverFields.every(
        (f) => clientFields.includes(f) || f === 'sql' || f === 'params'
      );

      expect(allFieldsKnown).toBe(true);
    });
  });
});

// =============================================================================
// QueryResponse Compatibility Tests
// =============================================================================

describe('Type Duplication - QueryResponse Compatibility', () => {
  describe('Row format differences', () => {
    it.fails('should have identical row format between client and server', () => {
      // CLIENT QueryResult: rows: T[] (array of objects)
      // SERVER QueryResponse: rows: unknown[][] (columnar format)
      //
      // These are fundamentally incompatible representations!

      type ClientRowFormat = Record<string, ClientSQLValue>[];
      type ServerRowFormat = unknown[][];

      // This type check shows the formats are different
      type FormatsMatch = ClientRowFormat extends ServerRowFormat ? true : false;

      const result: FormatsMatch = true;
      expect(result).toBe(true);
    });

    it.fails('should have columnTypes field in client QueryResult', () => {
      // SERVER QueryResponse has: columnTypes: ColumnType[]
      // CLIENT QueryResult does NOT have columnTypes
      //
      // Client cannot reconstruct proper types without columnTypes

      type ClientQueryResult = {
        rows: Record<string, ClientSQLValue>[];
        columns: string[];
        rowsAffected: number;
        lastInsertRowid?: bigint;
        duration: number;
      };

      type HasColumnTypes = ClientQueryResult extends { columnTypes: unknown[] } ? true : false;

      const result: HasColumnTypes = true;
      expect(result).toBe(true);
    });

    it.fails('should have lsn field in client QueryResult', () => {
      // SERVER QueryResponse has: lsn: bigint
      // CLIENT QueryResult does NOT have lsn
      //
      // Client cannot track position for CDC without lsn

      type ClientQueryResult = {
        rows: Record<string, ClientSQLValue>[];
        columns: string[];
        rowsAffected: number;
        lastInsertRowid?: bigint;
        duration: number;
      };

      type HasLSN = ClientQueryResult extends { lsn: bigint } ? true : false;

      const result: HasLSN = true;
      expect(result).toBe(true);
    });

    it.fails('should have pagination fields (hasMore, cursor) in client QueryResult', () => {
      // SERVER QueryResponse has: hasMore?: boolean, cursor?: string
      // CLIENT QueryResult does NOT have pagination fields

      type ClientQueryResult = {
        rows: Record<string, ClientSQLValue>[];
        columns: string[];
        rowsAffected: number;
        lastInsertRowid?: bigint;
        duration: number;
      };

      type HasPagination = ClientQueryResult extends { hasMore?: boolean; cursor?: string }
        ? true
        : false;

      const result: HasPagination = true;
      expect(result).toBe(true);
    });
  });

  describe('Field naming differences', () => {
    it.fails('should use consistent naming for row count field', () => {
      // CLIENT: rowsAffected: number
      // SERVER: rowCount: number
      //
      // Different names for conceptually similar data

      type ClientResult = { rowsAffected: number };
      type ServerResult = { rowCount: number };

      type NamesMatch = keyof ClientResult extends keyof ServerResult ? true : false;

      const result: NamesMatch = true;
      expect(result).toBe(true);
    });

    it.fails('should use consistent naming for timing field', () => {
      // CLIENT: duration: number
      // SERVER: executionTimeMs: number
      //
      // Different names and potentially different units

      type ClientResult = { duration: number };
      type ServerResult = { executionTimeMs: number };

      type NamesMatch = keyof ClientResult extends keyof ServerResult ? true : false;

      const result: NamesMatch = true;
      expect(result).toBe(true);
    });
  });
});

// =============================================================================
// CDCEvent Compatibility Tests
// =============================================================================

describe('Type Duplication - CDCEvent Compatibility', () => {
  describe('CDCEvent definition conflicts across packages', () => {
    it.fails('should have identical CDCEvent structure in sql.do and dosql/rpc', () => {
      // sql.do CDCEvent:
      //   lsn: LSN (branded bigint)
      //   timestamp: Date
      //   table: string
      //   operation: CDCOperation
      //   primaryKey: Record<string, SQLValue>
      //   before?: Record<string, SQLValue>
      //   after?: Record<string, SQLValue>
      //   transactionId: TransactionId

      // dosql/rpc CDCEvent:
      //   lsn: bigint (not branded)
      //   table: string
      //   operation: CDCOperation (includes TRUNCATE!)
      //   timestamp: number (not Date!)
      //   txId: string (different name!)
      //   oldRow?: Record<string, unknown>
      //   newRow?: Record<string, unknown>
      //   primaryKey?: Record<string, unknown>

      // Key differences:
      // 1. timestamp: Date vs number
      // 2. transactionId vs txId
      // 3. before/after vs oldRow/newRow
      // 4. LSN branded vs plain bigint
      // 5. primaryKey required vs optional

      type ClientCDCEventShape = {
        lsn: ClientLSN;
        timestamp: Date;
        transactionId: ClientTransactionId;
        before?: Record<string, ClientSQLValue>;
        after?: Record<string, ClientSQLValue>;
        primaryKey: Record<string, ClientSQLValue>;
      };

      type ServerCDCEventShape = {
        lsn: bigint;
        timestamp: number;
        txId: string;
        oldRow?: Record<string, unknown>;
        newRow?: Record<string, unknown>;
        primaryKey?: Record<string, unknown>;
      };

      // These types are NOT compatible
      type AreCompatible = ClientCDCEventShape extends ServerCDCEventShape ? true : false;

      const result: AreCompatible = true;
      expect(result).toBe(true);
    });

    it.fails('should have identical CDCEvent structure in dosql/rpc and dolake', () => {
      // dosql/rpc CDCEvent has txId: string
      // dolake CDCEvent has NO txId, uses different structure entirely:
      //   sequence: number
      //   timestamp: number
      //   operation: CDCOperation
      //   table: string
      //   rowId: string
      //   before?: T
      //   after?: T
      //   metadata?: Record<string, unknown>

      // dolake doesn't even have lsn or txId!

      type DoSQLRPCCDCEvent = {
        lsn: bigint;
        txId: string;
        timestamp: number;
      };

      type DoLakeCDCEventShape = {
        sequence: number;
        rowId: string;
        timestamp: number;
        // No lsn, no txId!
      };

      type AreCompatible = DoSQLRPCCDCEvent extends DoLakeCDCEventShape ? true : false;

      const result: AreCompatible = true;
      expect(result).toBe(true);
    });

    it.fails('should have identical CDCEvent in dosql/cdc and dosql/rpc', () => {
      // Even within the SAME package (dosql), CDCEvent is defined differently!
      //
      // dosql/rpc/types.ts CDCEvent:
      //   lsn: bigint, table, operation, timestamp: number, txId, oldRow, newRow, primaryKey
      //
      // dosql/cdc/types.ts CDCEvent:
      //   type CDCEvent<T> = ChangeEvent<T> | TransactionEvent
      //   It's a UNION TYPE, not a single interface!

      // This is a major inconsistency within the same package
      type RPCCDCEvent = ServerCDCEvent;
      type CDCModuleCDCEvent = DoSQLCDCEvent;

      // These are fundamentally different: one is a union, one is an interface
      type AreIdentical = RPCCDCEvent extends CDCModuleCDCEvent ? true : false;

      const result: AreIdentical = true;
      expect(result).toBe(true);
    });
  });

  describe('CDCOperation differences', () => {
    it.fails('should have identical CDCOperation values across packages', () => {
      // sql.do CDCOperation: 'INSERT' | 'UPDATE' | 'DELETE'
      // dosql/rpc CDCOperation: 'INSERT' | 'UPDATE' | 'DELETE' | 'TRUNCATE'
      // dolake CDCOperation: 'INSERT' | 'UPDATE' | 'DELETE'

      // Server supports TRUNCATE but client doesn't know about it!

      type ClientOps = 'INSERT' | 'UPDATE' | 'DELETE';
      type ServerOps = 'INSERT' | 'UPDATE' | 'DELETE' | 'TRUNCATE';

      type OpsMatch = ClientOps extends ServerOps ? ServerOps extends ClientOps ? true : false : false;

      const result: OpsMatch = true;
      expect(result).toBe(true);
    });
  });
});

// =============================================================================
// ColumnType Compatibility Tests
// =============================================================================

describe('Type Duplication - ColumnType Compatibility', () => {
  it.fails('should have identical ColumnType values in client and server', () => {
    // CLIENT ColumnType (sql.do):
    //   'INTEGER' | 'REAL' | 'TEXT' | 'BLOB' | 'NULL' | 'BOOLEAN' | 'DATETIME' | 'JSON'
    //
    // SERVER ColumnType (dosql/rpc):
    //   'string' | 'number' | 'bigint' | 'boolean' | 'date' | 'timestamp' | 'json' | 'blob' | 'null' | 'unknown'

    // These are COMPLETELY DIFFERENT sets of values!
    // Client uses SQL-style types, server uses JS-style types

    type ClientTypes = 'INTEGER' | 'REAL' | 'TEXT' | 'BLOB' | 'NULL' | 'BOOLEAN' | 'DATETIME' | 'JSON';
    type ServerTypes = 'string' | 'number' | 'bigint' | 'boolean' | 'date' | 'timestamp' | 'json' | 'blob' | 'null' | 'unknown';

    type TypesMatch = ClientTypes extends ServerTypes ? true : false;

    const result: TypesMatch = true;
    expect(result).toBe(true);
  });

  it.fails('should have mapping between client and server column types', () => {
    // There's no defined mapping between the two type systems
    // INTEGER (client) should map to 'number' or 'bigint' (server)?
    // REAL (client) should map to 'number' (server)?
    // TEXT (client) should map to 'string' (server)?

    const typeMapping: Record<ClientColumnType, ServerColumnType> = {
      INTEGER: 'number', // or bigint?
      REAL: 'number',
      TEXT: 'string',
      BLOB: 'blob',
      NULL: 'null',
      BOOLEAN: 'boolean',
      DATETIME: 'timestamp', // or date?
      JSON: 'json',
    };

    // This mapping is ambiguous and not formalized
    expect(typeMapping.INTEGER).toBe('bigint'); // Should it be bigint or number?
  });
});

// =============================================================================
// ClientCapabilities Tests
// =============================================================================

describe('Type Duplication - ClientCapabilities', () => {
  it.fails('should have ClientCapabilities defined in client package', () => {
    // ClientCapabilities is ONLY defined in dolake (server)
    // The client package has no way to declare its capabilities

    // dolake ClientCapabilities:
    //   binaryProtocol: boolean
    //   compression: boolean
    //   batching: boolean
    //   maxBatchSize: number
    //   maxMessageSize: number

    // Client package should export this type for connection negotiation

    type ClientPackageExports = typeof import('@dotdo/sql.do');

    type HasClientCapabilities = 'ClientCapabilities' extends keyof ClientPackageExports
      ? true
      : false;

    const result: HasClientCapabilities = true;
    expect(result).toBe(true);
  });
});

// =============================================================================
// RPCError Compatibility Tests
// =============================================================================

describe('Type Duplication - RPCError Compatibility', () => {
  it.fails('should have identical RPCError structure', () => {
    // CLIENT RPCError (sql.do):
    //   code: string
    //   message: string
    //   details?: unknown

    // SERVER RPCError (dosql/rpc):
    //   code: RPCErrorCode (enum, not string!)
    //   message: string
    //   details?: Record<string, unknown>
    //   stack?: string

    // Key differences:
    // 1. code: string vs RPCErrorCode enum
    // 2. details: unknown vs Record<string, unknown>
    // 3. Server has stack field

    type ClientError = {
      code: string;
      message: string;
      details?: unknown;
    };

    type ServerError = {
      code: RPCErrorCode;
      message: string;
      details?: Record<string, unknown>;
      stack?: string;
    };

    type AreCompatible = ClientError extends ServerError ? true : false;

    const result: AreCompatible = true;
    expect(result).toBe(true);
  });
});

// =============================================================================
// IsolationLevel Compatibility Tests
// =============================================================================

describe('Type Duplication - IsolationLevel Compatibility', () => {
  it.fails('should have identical IsolationLevel values', () => {
    // CLIENT IsolationLevel (sql.do):
    //   'READ_UNCOMMITTED' | 'READ_COMMITTED' | 'REPEATABLE_READ' | 'SERIALIZABLE' | 'SNAPSHOT'

    // SERVER isolation (dosql/rpc BeginTransactionRequest):
    //   'READ_COMMITTED' | 'REPEATABLE_READ' | 'SERIALIZABLE'

    // Server doesn't support READ_UNCOMMITTED or SNAPSHOT!

    type ClientIsolation = 'READ_UNCOMMITTED' | 'READ_COMMITTED' | 'REPEATABLE_READ' | 'SERIALIZABLE' | 'SNAPSHOT';
    type ServerIsolation = 'READ_COMMITTED' | 'REPEATABLE_READ' | 'SERIALIZABLE';

    type LevelsMatch = ClientIsolation extends ServerIsolation ? true : false;

    const result: LevelsMatch = true;
    expect(result).toBe(true);
  });
});

// =============================================================================
// Import Path Verification Tests
// =============================================================================

describe('Type Duplication - Import Path Verification', () => {
  it.fails('should be able to import shared types from a single source', () => {
    // Currently types must be imported from multiple packages:
    //   import { CDCEvent } from '@dotdo/sql.do';
    //   import { CDCEvent } from '@dotdo/dosql/rpc';
    //   import { CDCEvent } from '@dotdo/dolake';
    //
    // This leads to confusion about which definition to use

    // A shared types package would provide:
    //   import { CDCEvent, QueryRequest, QueryResponse } from '@dotdo/shared-types';

    const sharedTypesExists = false; // @dotdo/shared-types doesn't exist

    expect(sharedTypesExists).toBe(true);
  });

  it.fails('should not have duplicate type exports across packages', () => {
    // List of types that are duplicated:
    const duplicatedTypes = [
      'CDCEvent',
      'CDCOperation',
      'ColumnType',
      'QueryRequest', // Conceptually duplicated with QueryOptions
      'QueryResponse', // Conceptually duplicated with QueryResult
      'RPCError',
      'IsolationLevel', // vs isolation in BeginTransactionRequest
    ];

    // Each type should only be defined once
    const typesWithSingleDefinition = 0;

    expect(typesWithSingleDefinition).toBe(duplicatedTypes.length);
  });
});

// =============================================================================
// Lake.do vs DoLake Compatibility Tests
// =============================================================================

describe('Type Duplication - Lake.do vs DoLake Compatibility', () => {
  it.fails('should have identical types between lake.do client and dolake server', () => {
    // lake.do re-exports CDCEvent from sql.do
    // dolake defines its own CDCEvent with different structure

    // lake.do CDCEvent (from sql.do):
    //   lsn: LSN, timestamp: Date, table, operation, primaryKey, before, after, transactionId

    // dolake CDCEvent:
    //   sequence: number, timestamp: number, operation, table, rowId, before, after, metadata

    // These are completely incompatible!

    type LakeClientEvent = LakeClientCDCEvent;
    type DoLakeServerEvent = DoLakeCDCEvent;

    // The types don't share a common structure
    type AreCompatible = LakeClientEvent extends DoLakeServerEvent ? true : false;

    const result: AreCompatible = true;
    expect(result).toBe(true);
  });
});

// =============================================================================
// Solution Verification Tests (for GREEN phase)
// =============================================================================

describe('Solution: Shared Types Package', () => {
  it.fails('should provide unified CDCEvent type', () => {
    // A proper shared CDCEvent should include all fields needed by all consumers:
    interface UnifiedCDCEvent<T = unknown> {
      // Identification
      lsn: bigint;
      sequence?: number;

      // Metadata
      table: string;
      operation: 'INSERT' | 'UPDATE' | 'DELETE' | 'TRUNCATE';
      timestamp: Date;

      // Transaction context
      transactionId: string;

      // Data
      primaryKey?: Record<string, unknown>;
      before?: T;
      after?: T;

      // Extension
      metadata?: Record<string, unknown>;
    }

    // This unified type doesn't exist yet
    const unifiedTypeExists = false;
    expect(unifiedTypeExists).toBe(true);
  });

  it.fails('should provide unified QueryRequest type', () => {
    // A proper shared QueryRequest should support all use cases:
    interface UnifiedQueryRequest {
      sql: string;
      params?: unknown[];
      namedParams?: Record<string, unknown>;
      branch?: string;
      asOf?: bigint | Date;
      timeout?: number;
      streaming?: boolean;
      limit?: number;
      offset?: number;
      transactionId?: string;
      shardId?: string;
    }

    // This unified type doesn't exist yet
    const unifiedTypeExists = false;
    expect(unifiedTypeExists).toBe(true);
  });

  it.fails('should provide unified QueryResponse type', () => {
    // A proper shared QueryResponse should support both row formats:
    interface UnifiedQueryResponse<T = Record<string, unknown>> {
      // Column metadata
      columns: string[];
      columnTypes: string[];

      // Row data (support both formats)
      rows: T[] | unknown[][];
      rowFormat: 'objects' | 'arrays';

      // Counts
      rowCount: number;
      rowsAffected?: number;

      // Position tracking
      lsn: bigint;
      lastInsertRowid?: bigint;

      // Timing
      executionTimeMs: number;

      // Pagination
      hasMore?: boolean;
      cursor?: string;
    }

    // This unified type doesn't exist yet
    const unifiedTypeExists = false;
    expect(unifiedTypeExists).toBe(true);
  });

  it.fails('should provide unified ColumnType enum', () => {
    // A proper shared ColumnType should map SQL to JS types:
    const UnifiedColumnType = {
      // SQL types
      INTEGER: 'INTEGER',
      REAL: 'REAL',
      TEXT: 'TEXT',
      BLOB: 'BLOB',
      BOOLEAN: 'BOOLEAN',
      DATETIME: 'DATETIME',
      JSON: 'JSON',
      NULL: 'NULL',
    } as const;

    // With a mapping to JS types
    type ColumnTypeMapping = {
      [K in keyof typeof UnifiedColumnType]: 'string' | 'number' | 'bigint' | 'boolean' | 'object' | 'null';
    };

    // This unified type doesn't exist yet
    const unifiedTypeExists = false;
    expect(unifiedTypeExists).toBe(true);
  });
});

// =============================================================================
// Runtime Type Guard Tests
// =============================================================================

describe('Runtime Compatibility - Type Guards', () => {
  it.fails('should have type guards to convert between client and server types', () => {
    // There are no type guards to safely convert between the different type shapes

    function isServerCDCEvent(event: unknown): event is ServerCDCEvent {
      return (
        typeof event === 'object' &&
        event !== null &&
        'lsn' in event &&
        'txId' in event &&
        typeof (event as ServerCDCEvent).timestamp === 'number'
      );
    }

    function isClientCDCEvent(event: unknown): event is ClientCDCEvent {
      return (
        typeof event === 'object' &&
        event !== null &&
        'lsn' in event &&
        'transactionId' in event &&
        (event as ClientCDCEvent).timestamp instanceof Date
      );
    }

    // These guards exist in test but not in production code
    const guardsExistInProduction = false;
    expect(guardsExistInProduction).toBe(true);
  });

  it.fails('should have converters between client and server types', () => {
    // There are no converter functions to transform between type shapes

    function serverToClientCDCEvent(server: ServerCDCEvent): ClientCDCEvent {
      return {
        lsn: server.lsn as ClientLSN,
        timestamp: new Date(server.timestamp),
        table: server.table,
        operation: server.operation as ClientCDCOperation,
        primaryKey: (server.primaryKey ?? {}) as Record<string, ClientSQLValue>,
        before: server.oldRow as Record<string, ClientSQLValue> | undefined,
        after: server.newRow as Record<string, ClientSQLValue> | undefined,
        transactionId: server.txId as ClientTransactionId,
      };
    }

    // This converter exists in test but not in production code
    const convertersExistInProduction = false;
    expect(convertersExistInProduction).toBe(true);
  });
});
