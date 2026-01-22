/**
 * DoSQL Distributed Transactions with Two-Phase Commit (2PC)
 *
 * Implements distributed transactions across multiple shards with:
 * - Two-Phase Commit (2PC) protocol
 * - Coordinator and participant roles
 * - Prepare/commit/rollback phases
 * - Timeout handling
 * - Coordinator failure recovery
 * - Participant failure handling
 * - Read-your-writes consistency
 * - Serializable isolation across shards
 *
 * This file re-exports from the modular distributed-tx directory
 * for backwards compatibility.
 *
 * @packageDocumentation
 */

// Re-export everything from the modular distributed-tx directory
export * from './distributed-tx/index.js';
