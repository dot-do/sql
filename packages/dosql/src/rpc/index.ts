/**
 * DoSQL RPC Module
 *
 * CapnWeb-based RPC for efficient DO-to-DO communication and client queries.
 *
 * Features:
 * - Type-safe RPC over WebSocket and HTTP batch modes
 * - Streaming query results for large datasets
 * - CDC (Change Data Capture) subscriptions
 * - Transaction support
 * - Promise pipelining for reduced round trips
 *
 * @packageDocumentation
 */

// =============================================================================
// Type Exports
// =============================================================================

export type {
  // Core Query Types
  QueryRequest,
  QueryResponse,
  ColumnType,

  // Streaming Types
  StreamRequest,
  StreamChunk,
  StreamComplete,

  // CDC Types
  CDCRequest,
  CDCEvent,
  CDCAck,
  CDCOperation,

  // Transaction Types
  BeginTransactionRequest,
  TransactionHandle,
  TransactionQueryRequest,
  CommitRequest,
  RollbackRequest,
  TransactionResult,

  // Batch Types
  BatchRequest,
  BatchResponse,
  BatchError,

  // Schema Types
  SchemaRequest,
  SchemaResponse,
  TableSchema,
  ColumnSchema,
  IndexSchema,
  ForeignKeySchema,

  // Error Types
  RPCError,

  // Connection Types
  ConnectionOptions,
  ConnectionStats,

  // API Interface
  DoSQLAPI,
} from './types.js';

export { RPCErrorCode } from './types.js';

// =============================================================================
// Client Exports
// =============================================================================

export {
  // Client factories
  createWebSocketClient,
  createHttpClient,

  // Transaction helper
  withTransaction,

  // Error utilities
  isRPCError,
  createRPCError,

  // Client types
  type DoSQLClient,
  type TransactionContext,
} from './client.js';

// =============================================================================
// Server Exports
// =============================================================================

export {
  // RPC Target
  DoSQLTarget,

  // Request handler
  handleDoSQLRequest,
  isDoSQLRequest,

  // Executor interface
  type QueryExecutor,
  type ExecuteOptions,
  type ExecuteResult,
  type TransactionOptions,

  // CDC interface
  type CDCManager,
  type CDCSubscribeOptions,
  type CDCSubscription,
} from './server.js';

// =============================================================================
// Convenience Re-exports from CapnWeb
// =============================================================================

export {
  RpcTarget,
  newWebSocketRpcSession,
  newHttpBatchRpcSession,
  newWorkersRpcResponse,
  newHttpBatchRpcResponse,
  newWorkersWebSocketRpcResponse,
  serialize,
  deserialize,
  type RpcSessionOptions,
  type RpcStub,
} from 'capnweb';
