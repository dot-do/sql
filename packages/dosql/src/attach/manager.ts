/**
 * DoSQL Database Attachment Manager
 *
 * Manages multiple attached databases following SQLite semantics:
 * - Maintains 'main' and 'temp' databases by default
 * - Supports up to MAX_ATTACHED_DATABASES additional databases
 * - Handles cross-database transaction coordination
 */

import type {
  AttachedDatabase,
  AttachOptions,
  AttachResult,
  DetachResult,
  DatabaseConnectionOptions,
  DatabaseConnectionType,
  DatabaseListEntry,
  DatabaseManager,
  AttachError,
  AttachErrorCode,
  MultiDatabaseTransaction,
  CrossDatabaseTransactionOptions,
  RESERVED_DATABASE_ALIASES,
  MAX_ATTACHED_DATABASES,
} from './types.js';
import type { Schema, TableSchema } from '../engine/types.js';

// Re-export constants
export { RESERVED_DATABASE_ALIASES, MAX_ATTACHED_DATABASES } from './types.js';

// =============================================================================
// DATABASE CONNECTION FACTORY
// =============================================================================

/**
 * Factory for creating database connections
 */
export interface DatabaseConnectionFactory {
  /**
   * Create a connection to a database
   */
  connect(options: DatabaseConnectionOptions): Promise<DatabaseConnection>;
}

/**
 * Database connection interface
 */
export interface DatabaseConnection {
  /** Get the schema of the connected database */
  getSchema(): Promise<Schema>;

  /** Check if the connection is alive */
  ping(): Promise<boolean>;

  /** Close the connection */
  close(): Promise<void>;

  /** Get the connection type */
  type: DatabaseConnectionType;

  /** Whether the connection is read-only */
  readonly: boolean;
}

// =============================================================================
// IN-MEMORY DATABASE CONNECTION
// =============================================================================

/**
 * In-memory database connection for testing
 */
export class InMemoryDatabaseConnection implements DatabaseConnection {
  readonly type: DatabaseConnectionType = 'memory';
  readonly readonly: boolean;

  private _schema: Schema;
  private _connected: boolean = true;

  constructor(schema?: Schema, readonly_?: boolean) {
    this._schema = schema || { tables: new Map() };
    this.readonly = readonly_ || false;
  }

  async getSchema(): Promise<Schema> {
    return this._schema;
  }

  async ping(): Promise<boolean> {
    return this._connected;
  }

  async close(): Promise<void> {
    this._connected = false;
  }

  /**
   * Add a table to the in-memory schema
   */
  addTable(tableDef: TableSchema): void {
    this._schema.tables.set(tableDef.name, tableDef);
  }

  /**
   * Remove a table from the in-memory schema
   */
  removeTable(tableName: string): boolean {
    return this._schema.tables.delete(tableName);
  }
}

// =============================================================================
// DEFAULT CONNECTION FACTORY
// =============================================================================

/**
 * Default connection factory that creates in-memory connections
 */
export class DefaultConnectionFactory implements DatabaseConnectionFactory {
  async connect(options: DatabaseConnectionOptions): Promise<DatabaseConnection> {
    switch (options.type) {
      case 'memory':
        return new InMemoryDatabaseConnection(undefined, options.readonly);

      case 'file':
      case 'r2':
      case 'durable':
      case 'kv':
      case 'd1':
      case 'remote':
        // For now, return in-memory connection as placeholder
        // Real implementations would connect to actual backends
        return new InMemoryDatabaseConnection(undefined, options.readonly);

      default:
        throw new Error(`Unsupported connection type: ${options.type}`);
    }
  }
}

// =============================================================================
// DATABASE ATTACHMENT MANAGER
// =============================================================================

/**
 * Implementation of the database attachment manager
 */
export class AttachmentManager implements DatabaseManager {
  private _databases: Map<string, AttachedDatabase> = new Map();
  private _sequenceNumber: number = 0;
  private _connectionFactory: DatabaseConnectionFactory;
  private _connections: Map<string, DatabaseConnection> = new Map();
  private _activeTransactions: Map<string, MultiDatabaseTransaction> = new Map();

  constructor(connectionFactory?: DatabaseConnectionFactory) {
    this._connectionFactory = connectionFactory || new DefaultConnectionFactory();
    this._initializeDefaultDatabases();
  }

  /**
   * Initialize main and temp databases
   */
  private _initializeDefaultDatabases(): void {
    // Create main database
    const mainDb: AttachedDatabase = {
      alias: 'main',
      path: ':memory:',
      options: {
        type: 'memory',
        path: ':memory:',
        readonly: false,
      },
      schema: { tables: new Map() },
      isMain: true,
      isTemp: false,
      seq: this._sequenceNumber++,
      attachedAt: new Date(),
      isConnected: true,
      isReadOnly: false,
    };
    this._databases.set('main', mainDb);

    // Create temp database
    const tempDb: AttachedDatabase = {
      alias: 'temp',
      path: '',
      options: {
        type: 'memory',
        path: '',
        readonly: false,
      },
      schema: { tables: new Map() },
      isMain: false,
      isTemp: true,
      seq: this._sequenceNumber++,
      attachedAt: new Date(),
      isConnected: true,
      isReadOnly: false,
    };
    this._databases.set('temp', tempDb);
  }

  /**
   * Parse path to determine connection type
   */
  private _parseConnectionType(path: string): DatabaseConnectionType {
    if (path === ':memory:' || path === '') {
      return 'memory';
    }

    // Check for URL schemes
    if (path.startsWith('r2://')) {
      return 'r2';
    }
    if (path.startsWith('kv://')) {
      return 'kv';
    }
    if (path.startsWith('d1://')) {
      return 'd1';
    }
    if (path.startsWith('do://') || path.startsWith('durable://')) {
      return 'durable';
    }
    if (path.startsWith('http://') || path.startsWith('https://')) {
      return 'remote';
    }

    // Default to file
    return 'file';
  }

  /**
   * Validate alias name
   */
  private _validateAlias(alias: string): AttachError | null {
    // Check if empty
    if (!alias || alias.trim() === '') {
      return {
        code: 'INVALID_ALIAS',
        message: 'Database alias cannot be empty',
      };
    }

    // Check reserved names
    if (alias.toLowerCase() === 'main' || alias.toLowerCase() === 'temp') {
      return {
        code: 'INVALID_ALIAS',
        message: `Cannot use reserved alias '${alias}'`,
        details: { alias },
      };
    }

    // Check if already exists
    if (this._databases.has(alias)) {
      return {
        code: 'ALIAS_EXISTS',
        message: `Database alias '${alias}' is already in use`,
        details: { alias },
      };
    }

    // Validate alias format (alphanumeric and underscore only)
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(alias)) {
      return {
        code: 'INVALID_ALIAS',
        message: `Invalid database alias '${alias}'. Must start with letter or underscore and contain only alphanumeric characters and underscores`,
        details: { alias },
      };
    }

    return null;
  }

  /**
   * Attach a database
   */
  async attach(path: string, options?: AttachOptions): Promise<AttachResult> {
    // Extract alias from options
    const alias = options?.alias;

    if (!alias) {
      return {
        success: false,
        error: 'Database alias is required',
      };
    }

    // Validate alias
    const aliasError = this._validateAlias(alias);
    if (aliasError) {
      return {
        success: false,
        error: aliasError.message,
      };
    }

    // Check max attached limit
    const userDatabaseCount = this.count() - 2; // Exclude main and temp
    if (userDatabaseCount >= 10) { // MAX_ATTACHED_DATABASES
      return {
        success: false,
        error: `Maximum number of attached databases (10) exceeded`,
      };
    }

    // Determine connection type
    const connectionType = options?.type || this._parseConnectionType(path);

    // Build connection options
    const connectionOptions: DatabaseConnectionOptions = {
      type: connectionType,
      path,
      readonly: options?.readonly ?? false,
      ...options?.connectionOptions,
    };

    try {
      // Create connection
      const connection = await this._connectionFactory.connect(connectionOptions);
      this._connections.set(alias, connection);

      // Get schema from connection
      const schema = await connection.getSchema();

      // Create attached database entry
      const attachedDb: AttachedDatabase = {
        alias,
        path,
        options: connectionOptions,
        schema,
        isMain: false,
        isTemp: false,
        seq: this._sequenceNumber++,
        attachedAt: new Date(),
        isConnected: true,
        isReadOnly: options?.readonly ?? false,
      };

      this._databases.set(alias, attachedDb);

      return {
        success: true,
        database: attachedDb,
      };
    } catch (err) {
      return {
        success: false,
        error: `Failed to attach database: ${err instanceof Error ? err.message : String(err)}`,
      };
    }
  }

  /**
   * Detach a database
   */
  async detach(alias: string): Promise<DetachResult> {
    // Cannot detach main
    if (alias.toLowerCase() === 'main') {
      return {
        success: false,
        error: 'Cannot detach the main database',
      };
    }

    // Cannot detach temp
    if (alias.toLowerCase() === 'temp') {
      return {
        success: false,
        error: 'Cannot detach the temp database',
      };
    }

    // Check if exists
    if (!this._databases.has(alias)) {
      return {
        success: false,
        error: `No such database: ${alias}`,
      };
    }

    // Check for active transactions
    const hasActiveTxn = this._hasActiveTransactions(alias);
    if (hasActiveTxn) {
      return {
        success: false,
        error: `Cannot detach database '${alias}' while it has active transactions`,
        hadActiveTransactions: true,
      };
    }

    // Close connection if exists
    const connection = this._connections.get(alias);
    if (connection) {
      await connection.close();
      this._connections.delete(alias);
    }

    // Remove database
    this._databases.delete(alias);

    return {
      success: true,
    };
  }

  /**
   * Check if alias has active transactions
   */
  private _hasActiveTransactions(alias: string): boolean {
    for (const txn of this._activeTransactions.values()) {
      if (txn.databases.includes(alias) && txn.state === 'active') {
        return true;
      }
    }
    return false;
  }

  /**
   * Get attached database by alias
   */
  get(alias: string): AttachedDatabase | undefined {
    return this._databases.get(alias);
  }

  /**
   * List all attached databases
   */
  list(): DatabaseListEntry[] {
    const entries: DatabaseListEntry[] = [];

    for (const db of this._databases.values()) {
      entries.push({
        seq: db.seq,
        name: db.alias,
        file: db.path,
      });
    }

    // Sort by sequence number
    return entries.sort((a, b) => a.seq - b.seq);
  }

  /**
   * Get the main database
   */
  getMain(): AttachedDatabase {
    return this._databases.get('main')!;
  }

  /**
   * Get the temp database
   */
  getTemp(): AttachedDatabase {
    return this._databases.get('temp')!;
  }

  /**
   * Check if database alias exists
   */
  has(alias: string): boolean {
    return this._databases.has(alias);
  }

  /**
   * Get count of attached databases
   */
  count(): number {
    return this._databases.size;
  }

  /**
   * Get all database aliases
   */
  aliases(): string[] {
    return Array.from(this._databases.keys());
  }

  /**
   * Get schema for a database
   */
  getSchema(alias: string): Schema | undefined {
    return this._databases.get(alias)?.schema;
  }

  /**
   * Merge schemas from all databases
   */
  getMergedSchema(): Schema {
    const merged: Schema = { tables: new Map() };

    for (const [alias, db] of this._databases) {
      for (const [tableName, tableDef] of db.schema.tables) {
        // Use qualified name: db.table
        const qualifiedName = `${alias}.${tableName}`;
        merged.tables.set(qualifiedName, tableDef);

        // Also add unqualified if from main
        if (alias === 'main' && !merged.tables.has(tableName)) {
          merged.tables.set(tableName, tableDef);
        }
      }
    }

    return merged;
  }

  // ===========================================================================
  // TRANSACTION MANAGEMENT
  // ===========================================================================

  /**
   * Begin a cross-database transaction
   */
  beginTransaction(options?: CrossDatabaseTransactionOptions): MultiDatabaseTransaction {
    const txnId = `txn_${Date.now()}_${Math.random().toString(36).slice(2)}`;

    const txn: MultiDatabaseTransaction = {
      id: txnId,
      databases: options?.databases || this.aliases(),
      state: 'active',
      startedAt: new Date(),
      isCrossDatabase: (options?.databases?.length || this.count()) > 1,
      savepoints: [],
    };

    this._activeTransactions.set(txnId, txn);
    return txn;
  }

  /**
   * Commit a transaction
   *
   * For cross-database transactions, this implements a simplified two-phase commit:
   * 1. Prepare phase: Verify all databases can commit
   * 2. Commit phase: Commit on each database
   *
   * Note: Full durability requires database connections that support prepare/commit.
   * Current implementation commits sequentially with best-effort rollback on failure.
   */
  async commitTransaction(txnId: string): Promise<boolean> {
    const txn = this._activeTransactions.get(txnId);
    if (!txn) {
      return false;
    }

    if (txn.state !== 'active') {
      return false;
    }

    txn.state = 'committing';

    try {
      // Phase 1: Prepare - verify all connections are healthy
      const prepareResults: Array<{ db: string; ready: boolean }> = [];

      for (const dbAlias of txn.databases) {
        const connection = this._connections.get(dbAlias);
        const database = this._databases.get(dbAlias);

        if (!connection || !database) {
          prepareResults.push({ db: dbAlias, ready: false });
          continue;
        }

        // Check connection health
        const isHealthy = await connection.ping().catch(() => false);
        prepareResults.push({ db: dbAlias, ready: isHealthy && database.isConnected });
      }

      // Check if all databases are ready
      const allReady = prepareResults.every(r => r.ready);
      if (!allReady) {
        // Abort - not all databases ready for commit
        const failedDbs = prepareResults.filter(r => !r.ready).map(r => r.db);
        txn.state = 'active'; // Allow retry or explicit rollback
        throw new Error(`Commit failed: databases not ready: ${failedDbs.join(', ')}`);
      }

      // Phase 2: Commit - actual commit on each database
      // Note: Real implementation would call connection.commit() for each database
      // For now, we mark the transaction as committed since in-memory connections
      // auto-commit and don't support explicit transaction boundaries

      txn.state = 'committed';
      this._activeTransactions.delete(txnId);
      return true;
    } catch (error) {
      // On error, attempt rollback for safety
      txn.state = 'active';
      await this.rollbackTransaction(txnId).catch(() => {
        // Ignore rollback errors - transaction may be in inconsistent state
      });
      return false;
    }
  }

  /**
   * Rollback a transaction
   *
   * Attempts to rollback changes on all databases involved in the transaction.
   * For cross-database transactions, rollback is best-effort: if one database
   * fails to rollback, the others will still be attempted.
   *
   * Note: Full atomicity requires database connections that support rollback.
   * Current implementation marks the transaction as rolled back and relies on
   * database-level isolation for actual rollback behavior.
   */
  async rollbackTransaction(txnId: string): Promise<boolean> {
    const txn = this._activeTransactions.get(txnId);
    if (!txn) {
      return false;
    }

    if (txn.state !== 'active' && txn.state !== 'committing') {
      return false;
    }

    txn.state = 'rolling_back';

    const rollbackErrors: Array<{ db: string; error: string }> = [];

    // Attempt rollback on each database
    for (const dbAlias of txn.databases) {
      const connection = this._connections.get(dbAlias);
      const database = this._databases.get(dbAlias);

      if (!connection || !database) {
        rollbackErrors.push({
          db: dbAlias,
          error: 'Database not found or not connected',
        });
        continue;
      }

      try {
        // Check if connection is healthy before attempting rollback
        const isHealthy = await connection.ping().catch(() => false);
        if (!isHealthy) {
          rollbackErrors.push({
            db: dbAlias,
            error: 'Connection unhealthy - rollback may not be complete',
          });
        }
        // Note: Real implementation would call connection.rollback() here
        // In-memory connections don't need explicit rollback as they auto-commit
      } catch (error) {
        rollbackErrors.push({
          db: dbAlias,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }

    // Mark transaction as rolled back even if some databases had errors
    // This prevents the transaction from being used further
    txn.state = 'rolled_back';
    this._activeTransactions.delete(txnId);

    // Return true even with errors - the transaction is no longer active
    // Callers should check database state if consistency is critical
    return true;
  }

  /**
   * Create a savepoint
   */
  savepoint(txnId: string, name: string): boolean {
    const txn = this._activeTransactions.get(txnId);
    if (!txn || txn.state !== 'active') {
      return false;
    }

    txn.savepoints.push(name);
    return true;
  }

  /**
   * Release a savepoint
   */
  releaseSavepoint(txnId: string, name: string): boolean {
    const txn = this._activeTransactions.get(txnId);
    if (!txn || txn.state !== 'active') {
      return false;
    }

    const idx = txn.savepoints.lastIndexOf(name);
    if (idx === -1) {
      return false;
    }

    txn.savepoints.splice(idx, 1);
    return true;
  }

  /**
   * Rollback to a savepoint
   */
  rollbackToSavepoint(txnId: string, name: string): boolean {
    const txn = this._activeTransactions.get(txnId);
    if (!txn || txn.state !== 'active') {
      return false;
    }

    const idx = txn.savepoints.lastIndexOf(name);
    if (idx === -1) {
      return false;
    }

    // Remove all savepoints after this one
    txn.savepoints = txn.savepoints.slice(0, idx + 1);
    return true;
  }

  /**
   * Get active transaction by ID
   */
  getTransaction(txnId: string): MultiDatabaseTransaction | undefined {
    return this._activeTransactions.get(txnId);
  }

  /**
   * Get all active transactions
   */
  getActiveTransactions(): MultiDatabaseTransaction[] {
    return Array.from(this._activeTransactions.values())
      .filter(txn => txn.state === 'active');
  }

  // ===========================================================================
  // SCHEMA MODIFICATION
  // ===========================================================================

  /**
   * Add a table to a database's schema
   */
  addTableToSchema(alias: string, tableDef: TableSchema): boolean {
    const db = this._databases.get(alias);
    if (!db) {
      return false;
    }

    db.schema.tables.set(tableDef.name, tableDef);
    return true;
  }

  /**
   * Remove a table from a database's schema
   */
  removeTableFromSchema(alias: string, tableName: string): boolean {
    const db = this._databases.get(alias);
    if (!db) {
      return false;
    }

    return db.schema.tables.delete(tableName);
  }

  /**
   * Get connection for a database
   */
  getConnection(alias: string): DatabaseConnection | undefined {
    return this._connections.get(alias);
  }
}

// =============================================================================
// FACTORY FUNCTION
// =============================================================================

/**
 * Create a new database attachment manager
 */
export function createDatabaseManager(
  connectionFactory?: DatabaseConnectionFactory
): AttachmentManager {
  return new AttachmentManager(connectionFactory);
}
