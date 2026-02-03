/**
 * DoSQL Sharding Configuration Migration
 *
 * Provides configuration versioning, migration support, and safe resharding:
 * - Config version tracking
 * - Automatic migration on version mismatch
 * - Rollback support
 * - Validation before applying
 *
 * @packageDocumentation
 */

import type {
  VSchema,
  VSchemaSettings,
  ShardConfig,
  TableShardingConfig,
  VindexConfig,
  HashVindexConfig,
  ConsistentHashVindexConfig,
  RangeVindexConfig,
} from './types.js';

// =============================================================================
// VERSION TYPES
// =============================================================================

/**
 * Configuration version identifier
 */
export type ConfigVersion = `${number}.${number}.${number}`;

/**
 * Migration status
 */
export type MigrationStatus = 'pending' | 'in_progress' | 'completed' | 'rolled_back' | 'failed';

/**
 * Migration direction
 */
export type MigrationDirection = 'up' | 'down';

// =============================================================================
// VERSIONED CONFIGURATION
// =============================================================================

/**
 * Extended VSchema with versioning support
 */
export interface VersionedVSchema<
  Tables extends Record<string, TableShardingConfig> = Record<string, TableShardingConfig>
> extends VSchema<Tables> {
  /** Configuration version (semver format) */
  version: ConfigVersion;
  /** Timestamp when this version was created */
  createdAt: number;
  /** Optional description of this configuration version */
  description?: string;
  /** Checksum for integrity verification */
  checksum?: string;
}

/**
 * ShardingConfig with versioning support
 */
export interface VersionedShardingConfig {
  /** VSchema with version tracking */
  vschema: VersionedVSchema;
  /** Optional previous version for rollback */
  previousVersion?: ConfigVersion;
  /** Migration history */
  migrationHistory?: MigrationRecord[];
}

// =============================================================================
// MIGRATION TYPES
// =============================================================================

/**
 * Record of a migration that was applied
 */
export interface MigrationRecord {
  /** Migration identifier */
  id: string;
  /** Source version */
  fromVersion: ConfigVersion;
  /** Target version */
  toVersion: ConfigVersion;
  /** When the migration started */
  startedAt: number;
  /** When the migration completed (undefined if still in progress or failed) */
  completedAt?: number;
  /** Migration status */
  status: MigrationStatus;
  /** Error message if failed */
  error?: string;
  /** Direction of migration */
  direction: MigrationDirection;
  /** Affected tables */
  affectedTables: string[];
  /** Affected shards */
  affectedShards: string[];
}

/**
 * Migration step definition
 */
export interface MigrationStep {
  /** Step identifier */
  id: string;
  /** Human-readable description */
  description: string;
  /** Tables affected by this step */
  tables: string[];
  /** Shards affected by this step */
  shards: string[];
  /** Whether this step can be rolled back */
  reversible: boolean;
  /** The migration function to execute */
  execute: (context: MigrationContext) => Promise<void>;
  /** The rollback function (only if reversible) */
  rollback?: (context: MigrationContext) => Promise<void>;
}

/**
 * Migration definition
 */
export interface Migration {
  /** Migration identifier (unique) */
  id: string;
  /** Source version */
  fromVersion: ConfigVersion;
  /** Target version */
  toVersion: ConfigVersion;
  /** Human-readable description */
  description: string;
  /** Migration steps in order */
  steps: MigrationStep[];
  /** Whether the entire migration can be rolled back */
  reversible: boolean;
  /** Estimated downtime (in milliseconds, 0 for zero-downtime) */
  estimatedDowntimeMs: number;
}

/**
 * Context provided to migration functions
 */
export interface MigrationContext {
  /** Current configuration */
  currentConfig: VersionedVSchema;
  /** Target configuration */
  targetConfig: VersionedVSchema;
  /** Migration being executed */
  migration: Migration;
  /** Current step index */
  stepIndex: number;
  /** Logging function */
  log: (message: string) => void;
  /** Progress callback */
  onProgress?: (percent: number, message: string) => void;
}

// =============================================================================
// VALIDATION TYPES
// =============================================================================

/**
 * Validation error
 */
export interface ValidationError {
  /** Error code */
  code: string;
  /** Human-readable message */
  message: string;
  /** Path to the invalid field (e.g., "tables.users.vindex") */
  path: string;
  /** Severity */
  severity: 'error' | 'warning';
}

/**
 * Validation result
 */
export interface ValidationResult {
  /** Whether the configuration is valid */
  valid: boolean;
  /** List of errors/warnings */
  errors: ValidationError[];
}

// =============================================================================
// CONFIGURATION MANAGER
// =============================================================================

/**
 * Configuration manager for versioned sharding configurations
 */
export class ShardingConfigManager {
  private migrations: Map<string, Migration> = new Map();
  private migrationHistory: MigrationRecord[] = [];

  constructor(
    private currentConfig: VersionedVSchema,
    private options: {
      onLog?: (message: string) => void;
      onProgress?: (percent: number, message: string) => void;
    } = {}
  ) {}

  /**
   * Get the current configuration
   */
  getCurrentConfig(): VersionedVSchema {
    return this.currentConfig;
  }

  /**
   * Get the current version
   */
  getCurrentVersion(): ConfigVersion {
    return this.currentConfig.version;
  }

  /**
   * Register a migration
   */
  registerMigration(migration: Migration): void {
    const key = `${migration.fromVersion}->${migration.toVersion}`;
    this.migrations.set(key, migration);
  }

  /**
   * Get all registered migrations
   */
  getMigrations(): Migration[] {
    return Array.from(this.migrations.values());
  }

  /**
   * Find migration path between two versions
   */
  findMigrationPath(fromVersion: ConfigVersion, toVersion: ConfigVersion): Migration[] | null {
    const direction = this.compareVersions(fromVersion, toVersion);

    if (direction === 0) {
      return []; // Same version, no migration needed
    }

    // Direct migration
    const directKey = `${fromVersion}->${toVersion}`;
    if (this.migrations.has(directKey)) {
      return [this.migrations.get(directKey)!];
    }

    // Try to find a path through intermediate versions
    // If direction < 0, we're going UP (from < to)
    // If direction > 0, we're going DOWN (from > to)
    const path = this.findPath(fromVersion, toVersion, direction < 0 ? 'up' : 'down');
    return path;
  }

  private findPath(
    from: ConfigVersion,
    to: ConfigVersion,
    direction: MigrationDirection,
    visited: Set<ConfigVersion> = new Set()
  ): Migration[] | null {
    if (from === to) {
      return [];
    }

    if (visited.has(from)) {
      return null; // Cycle detected
    }
    visited.add(from);

    // Find all migrations from current version
    const candidates = Array.from(this.migrations.values()).filter(m => {
      if (direction === 'up') {
        return m.fromVersion === from && this.compareVersions(m.toVersion, from) > 0;
      } else {
        return m.toVersion === from && this.compareVersions(m.fromVersion, from) < 0;
      }
    });

    // Sort by version proximity to target
    candidates.sort((a, b) => {
      const aTarget = direction === 'up' ? a.toVersion : a.fromVersion;
      const bTarget = direction === 'up' ? b.toVersion : b.fromVersion;
      const aDist = Math.abs(this.compareVersions(aTarget, to));
      const bDist = Math.abs(this.compareVersions(bTarget, to));
      return aDist - bDist;
    });

    for (const migration of candidates) {
      const nextVersion = direction === 'up' ? migration.toVersion : migration.fromVersion;
      const remainingPath = this.findPath(nextVersion, to, direction, new Set(visited));

      if (remainingPath !== null) {
        return [migration, ...remainingPath];
      }
    }

    return null;
  }

  /**
   * Compare two versions
   * Returns: negative if a < b, 0 if a === b, positive if a > b
   */
  compareVersions(a: ConfigVersion, b: ConfigVersion): number {
    const [aMajor, aMinor, aPatch] = a.split('.').map(Number);
    const [bMajor, bMinor, bPatch] = b.split('.').map(Number);

    if (aMajor !== bMajor) return aMajor - bMajor;
    if (aMinor !== bMinor) return aMinor - bMinor;
    return aPatch - bPatch;
  }

  /**
   * Validate a configuration
   */
  validate(config: VersionedVSchema): ValidationResult {
    const errors: ValidationError[] = [];

    // Validate version format
    if (!this.isValidVersion(config.version)) {
      errors.push({
        code: 'INVALID_VERSION',
        message: `Invalid version format: ${config.version}. Expected semver format (e.g., "1.0.0")`,
        path: 'version',
        severity: 'error',
      });
    }

    // Validate shards
    if (!config.shards || config.shards.length === 0) {
      errors.push({
        code: 'NO_SHARDS',
        message: 'At least one shard is required',
        path: 'shards',
        severity: 'error',
      });
    } else {
      const shardIds = new Set<string>();
      for (let i = 0; i < config.shards.length; i++) {
        const shard = config.shards[i];

        // Check for duplicate shard IDs
        if (shardIds.has(shard.id)) {
          errors.push({
            code: 'DUPLICATE_SHARD_ID',
            message: `Duplicate shard ID: ${shard.id}`,
            path: `shards[${i}].id`,
            severity: 'error',
          });
        }
        shardIds.add(shard.id);

        // Validate shard namespace
        if (!shard.doNamespace) {
          errors.push({
            code: 'MISSING_DO_NAMESPACE',
            message: `Shard ${shard.id} is missing doNamespace`,
            path: `shards[${i}].doNamespace`,
            severity: 'error',
          });
        }
      }
    }

    // Validate tables
    for (const [tableName, tableConfig] of Object.entries(config.tables)) {
      // Validate sharded table configuration
      if (tableConfig.type === 'sharded') {
        if (!tableConfig.shardKey) {
          errors.push({
            code: 'MISSING_SHARD_KEY',
            message: `Sharded table ${tableName} is missing shard key`,
            path: `tables.${tableName}.shardKey`,
            severity: 'error',
          });
        }

        if (!tableConfig.vindex) {
          errors.push({
            code: 'MISSING_VINDEX',
            message: `Sharded table ${tableName} is missing vindex configuration`,
            path: `tables.${tableName}.vindex`,
            severity: 'error',
          });
        } else {
          // Validate vindex configuration
          const vindexErrors = this.validateVindex(tableConfig.vindex, `tables.${tableName}.vindex`, config.shards);
          errors.push(...vindexErrors);
        }
      }

      // Validate unsharded table configuration
      if (tableConfig.type === 'unsharded' && tableConfig.shard) {
        const shardExists = config.shards.some(s => s.id === tableConfig.shard);
        if (!shardExists) {
          errors.push({
            code: 'INVALID_SHARD_REFERENCE',
            message: `Unsharded table ${tableName} references non-existent shard: ${tableConfig.shard}`,
            path: `tables.${tableName}.shard`,
            severity: 'error',
          });
        }
      }
    }

    // Validate default shard
    if (config.defaultShard) {
      const defaultShardExists = config.shards.some(s => s.id === config.defaultShard);
      if (!defaultShardExists) {
        errors.push({
          code: 'INVALID_DEFAULT_SHARD',
          message: `Default shard references non-existent shard: ${config.defaultShard}`,
          path: 'defaultShard',
          severity: 'error',
        });
      }
    }

    return {
      valid: errors.filter(e => e.severity === 'error').length === 0,
      errors,
    };
  }

  private validateVindex(vindex: VindexConfig, path: string, shards: ShardConfig[]): ValidationError[] {
    const errors: ValidationError[] = [];

    switch (vindex.type) {
      case 'hash':
        if (vindex.algorithm && !['fnv1a', 'xxhash'].includes(vindex.algorithm)) {
          errors.push({
            code: 'INVALID_HASH_ALGORITHM',
            message: `Invalid hash algorithm: ${vindex.algorithm}. Expected 'fnv1a' or 'xxhash'`,
            path: `${path}.algorithm`,
            severity: 'error',
          });
        }
        break;

      case 'consistent-hash':
        if (vindex.virtualNodes !== undefined && (vindex.virtualNodes < 1 || vindex.virtualNodes > 10000)) {
          errors.push({
            code: 'INVALID_VIRTUAL_NODES',
            message: `Invalid virtualNodes: ${vindex.virtualNodes}. Expected 1-10000`,
            path: `${path}.virtualNodes`,
            severity: 'warning',
          });
        }
        break;

      case 'range':
        if (!vindex.boundaries || vindex.boundaries.length === 0) {
          errors.push({
            code: 'MISSING_BOUNDARIES',
            message: 'Range vindex requires at least one boundary',
            path: `${path}.boundaries`,
            severity: 'error',
          });
        } else {
          for (let i = 0; i < vindex.boundaries.length; i++) {
            const boundary = vindex.boundaries[i];
            const shardExists = shards.some(s => s.id === boundary.shard);
            if (!shardExists) {
              errors.push({
                code: 'INVALID_BOUNDARY_SHARD',
                message: `Boundary ${i} references non-existent shard: ${boundary.shard}`,
                path: `${path}.boundaries[${i}].shard`,
                severity: 'error',
              });
            }
          }
        }
        break;
    }

    return errors;
  }

  private isValidVersion(version: string): version is ConfigVersion {
    return /^\d+\.\d+\.\d+$/.test(version);
  }

  /**
   * Execute a migration to target version
   */
  async migrate(targetVersion: ConfigVersion): Promise<MigrationRecord> {
    const currentVersion = this.currentConfig.version;

    // Find migration path
    const path = this.findMigrationPath(currentVersion, targetVersion);
    if (!path) {
      throw new Error(`No migration path found from ${currentVersion} to ${targetVersion}`);
    }

    if (path.length === 0) {
      throw new Error(`Already at version ${targetVersion}`);
    }

    // Create migration record
    const record: MigrationRecord = {
      id: `migration-${Date.now()}-${Math.random().toString(36).slice(2)}`,
      fromVersion: currentVersion,
      toVersion: targetVersion,
      startedAt: Date.now(),
      status: 'pending',
      direction: this.compareVersions(targetVersion, currentVersion) > 0 ? 'up' : 'down',
      affectedTables: [],
      affectedShards: [],
    };

    // Collect affected tables and shards
    for (const migration of path) {
      for (const step of migration.steps) {
        record.affectedTables.push(...step.tables.filter(t => !record.affectedTables.includes(t)));
        record.affectedShards.push(...step.shards.filter(s => !record.affectedShards.includes(s)));
      }
    }

    this.migrationHistory.push(record);

    try {
      record.status = 'in_progress';
      this.log(`Starting migration from ${currentVersion} to ${targetVersion}`);

      // Execute migrations in sequence
      for (const migration of path) {
        await this.executeMigration(migration, record.direction);
      }

      record.status = 'completed';
      record.completedAt = Date.now();
      this.log(`Migration completed successfully in ${record.completedAt - record.startedAt}ms`);

      return record;
    } catch (error) {
      record.status = 'failed';
      record.completedAt = Date.now();
      record.error = error instanceof Error ? error.message : String(error);
      this.log(`Migration failed: ${record.error}`);
      throw error;
    }
  }

  private async executeMigration(migration: Migration, direction: MigrationDirection): Promise<void> {
    this.log(`Executing migration: ${migration.description}`);

    const steps = direction === 'up' ? migration.steps : [...migration.steps].reverse();

    for (let i = 0; i < steps.length; i++) {
      const step = steps[i];
      const context: MigrationContext = {
        currentConfig: this.currentConfig,
        targetConfig: this.currentConfig, // Will be updated
        migration,
        stepIndex: i,
        log: this.log.bind(this),
        onProgress: this.options.onProgress,
      };

      this.log(`  Step ${i + 1}/${steps.length}: ${step.description}`);

      try {
        if (direction === 'up') {
          await step.execute(context);
        } else if (step.rollback) {
          await step.rollback(context);
        } else {
          throw new Error(`Step ${step.id} is not reversible`);
        }
      } catch (error) {
        throw new Error(`Failed at step ${step.id}: ${error instanceof Error ? error.message : String(error)}`);
      }
    }
  }

  /**
   * Rollback to a previous version
   */
  async rollback(targetVersion?: ConfigVersion): Promise<MigrationRecord> {
    // If no target specified, rollback to previous version
    if (!targetVersion) {
      const lastMigration = this.migrationHistory.filter(m => m.status === 'completed').pop();
      if (!lastMigration) {
        throw new Error('No completed migrations to rollback');
      }
      targetVersion = lastMigration.fromVersion;
    }

    // Execute migration in reverse direction
    return this.migrate(targetVersion);
  }

  /**
   * Get migration history
   */
  getMigrationHistory(): MigrationRecord[] {
    return [...this.migrationHistory];
  }

  /**
   * Check if a migration is safe (zero downtime)
   */
  isSafeMigration(fromVersion: ConfigVersion, toVersion: ConfigVersion): boolean {
    const path = this.findMigrationPath(fromVersion, toVersion);
    if (!path) return false;

    return path.every(m => m.estimatedDowntimeMs === 0);
  }

  /**
   * Estimate total migration downtime
   */
  estimateDowntime(fromVersion: ConfigVersion, toVersion: ConfigVersion): number {
    const path = this.findMigrationPath(fromVersion, toVersion);
    if (!path) return -1;

    return path.reduce((total, m) => total + m.estimatedDowntimeMs, 0);
  }

  /**
   * Update the current configuration
   */
  updateConfig(newConfig: VersionedVSchema): void {
    const validation = this.validate(newConfig);
    if (!validation.valid) {
      const errorMessages = validation.errors
        .filter(e => e.severity === 'error')
        .map(e => `${e.path}: ${e.message}`)
        .join('\n');
      throw new Error(`Invalid configuration:\n${errorMessages}`);
    }
    this.currentConfig = newConfig;
  }

  private log(message: string): void {
    this.options.onLog?.(`[ShardingConfigManager] ${message}`);
  }
}

// =============================================================================
// FACTORY FUNCTIONS
// =============================================================================

/**
 * Create a versioned VSchema from a regular VSchema
 */
export function createVersionedVSchema<Tables extends Record<string, TableShardingConfig>>(
  vschema: VSchema<Tables>,
  version: ConfigVersion,
  options?: {
    description?: string;
    checksum?: string;
  }
): VersionedVSchema<Tables> {
  return {
    ...vschema,
    version,
    createdAt: Date.now(),
    description: options?.description,
    checksum: options?.checksum,
  };
}

/**
 * Create a configuration manager
 */
export function createConfigManager(
  config: VersionedVSchema,
  options?: {
    onLog?: (message: string) => void;
    onProgress?: (percent: number, message: string) => void;
  }
): ShardingConfigManager {
  return new ShardingConfigManager(config, options);
}

/**
 * Create a migration
 */
export function createMigration(options: {
  id: string;
  fromVersion: ConfigVersion;
  toVersion: ConfigVersion;
  description: string;
  steps: MigrationStep[];
  estimatedDowntimeMs?: number;
}): Migration {
  const reversible = options.steps.every(s => s.reversible);

  return {
    id: options.id,
    fromVersion: options.fromVersion,
    toVersion: options.toVersion,
    description: options.description,
    steps: options.steps,
    reversible,
    estimatedDowntimeMs: options.estimatedDowntimeMs ?? 0,
  };
}

/**
 * Create a migration step
 */
export function createMigrationStep(options: {
  id: string;
  description: string;
  tables?: string[];
  shards?: string[];
  reversible?: boolean;
  execute: (context: MigrationContext) => Promise<void>;
  rollback?: (context: MigrationContext) => Promise<void>;
}): MigrationStep {
  return {
    id: options.id,
    description: options.description,
    tables: options.tables ?? [],
    shards: options.shards ?? [],
    reversible: options.reversible ?? !!options.rollback,
    execute: options.execute,
    rollback: options.rollback,
  };
}

// =============================================================================
// COMMON MIGRATION HELPERS
// =============================================================================

/**
 * Create a migration step for adding a new shard
 */
export function addShardStep(shard: ShardConfig): MigrationStep {
  return createMigrationStep({
    id: `add-shard-${shard.id}`,
    description: `Add shard ${shard.id}`,
    shards: [shard.id],
    reversible: true,
    execute: async (context) => {
      // In a real implementation, this would:
      // 1. Create the new DO namespace binding
      // 2. Initialize the shard
      // 3. Update the vschema
      context.log(`Adding shard ${shard.id}`);
    },
    rollback: async (context) => {
      context.log(`Removing shard ${shard.id}`);
    },
  });
}

/**
 * Create a migration step for removing a shard
 */
export function removeShardStep(shardId: string): MigrationStep {
  return createMigrationStep({
    id: `remove-shard-${shardId}`,
    description: `Remove shard ${shardId}`,
    shards: [shardId],
    reversible: false, // Data loss - not reversible
    execute: async (context) => {
      context.log(`Removing shard ${shardId}`);
    },
  });
}

/**
 * Create a migration step for changing vindex type
 */
export function changeVindexStep(
  tableName: string,
  newVindex: VindexConfig,
  affectedShards: string[]
): MigrationStep {
  return createMigrationStep({
    id: `change-vindex-${tableName}`,
    description: `Change vindex for table ${tableName}`,
    tables: [tableName],
    shards: affectedShards,
    reversible: false, // Would need data migration
    execute: async (context) => {
      context.log(`Changing vindex for ${tableName} to ${newVindex.type}`);
      // In a real implementation, this would need to:
      // 1. Create new shard mappings
      // 2. Copy data to correct shards
      // 3. Update routing configuration
    },
  });
}

/**
 * Create a migration step for adding a table
 */
export function addTableStep(
  tableName: string,
  config: TableShardingConfig,
  affectedShards: string[]
): MigrationStep {
  return createMigrationStep({
    id: `add-table-${tableName}`,
    description: `Add table ${tableName}`,
    tables: [tableName],
    shards: affectedShards,
    reversible: true,
    execute: async (context) => {
      context.log(`Adding table ${tableName}`);
    },
    rollback: async (context) => {
      context.log(`Removing table ${tableName}`);
    },
  });
}

/**
 * Create a migration step for removing a table
 */
export function removeTableStep(tableName: string, affectedShards: string[]): MigrationStep {
  return createMigrationStep({
    id: `remove-table-${tableName}`,
    description: `Remove table ${tableName}`,
    tables: [tableName],
    shards: affectedShards,
    reversible: false, // Data loss - not reversible
    execute: async (context) => {
      context.log(`Removing table ${tableName}`);
    },
  });
}

// =============================================================================
// CHECKSUM UTILITIES
// =============================================================================

/**
 * Calculate a checksum for a VSchema configuration
 * Uses a simple hash of the JSON representation
 */
export function calculateConfigChecksum(config: VSchema): string {
  const normalized = JSON.stringify({
    tables: config.tables,
    shards: config.shards.map(s => ({
      id: s.id,
      doNamespace: s.doNamespace,
      doId: s.doId,
      replicas: s.replicas,
      readOnly: s.readOnly,
    })),
    defaultShard: config.defaultShard,
    settings: config.settings,
  });

  // Simple FNV-1a hash for checksum
  let hash = 2166136261;
  for (let i = 0; i < normalized.length; i++) {
    hash ^= normalized.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }

  return (hash >>> 0).toString(16).padStart(8, '0');
}

/**
 * Verify a configuration checksum
 */
export function verifyConfigChecksum(config: VersionedVSchema): boolean {
  if (!config.checksum) {
    return true; // No checksum to verify
  }

  const calculated = calculateConfigChecksum(config);
  return calculated === config.checksum;
}

// =============================================================================
// VERSION UTILITIES
// =============================================================================

/**
 * Increment a version
 */
export function incrementVersion(
  version: ConfigVersion,
  type: 'major' | 'minor' | 'patch'
): ConfigVersion {
  const [major, minor, patch] = version.split('.').map(Number);

  switch (type) {
    case 'major':
      return `${major + 1}.0.0`;
    case 'minor':
      return `${major}.${minor + 1}.0`;
    case 'patch':
      return `${major}.${minor}.${patch + 1}`;
  }
}

/**
 * Parse a version string
 */
export function parseVersion(version: ConfigVersion): { major: number; minor: number; patch: number } {
  const [major, minor, patch] = version.split('.').map(Number);
  return { major, minor, patch };
}

/**
 * Check if version a is compatible with version b
 * Compatible means same major version
 */
export function isCompatibleVersion(a: ConfigVersion, b: ConfigVersion): boolean {
  const parsedA = parseVersion(a);
  const parsedB = parseVersion(b);
  return parsedA.major === parsedB.major;
}
