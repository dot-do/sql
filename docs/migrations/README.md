# Migration Guides

This directory contains comprehensive migration guides for transitioning from other database solutions to DoSQL. Each guide follows a unified structure to ensure consistency and ease of navigation.

## Available Migration Guides

| Source Database | Guide | Difficulty | Time Estimate |
|-----------------|-------|------------|---------------|
| [Cloudflare D1](./from-d1.md) | D1 to DoSQL | Low | 1-4 hours |
| [Turso (libSQL)](./from-turso.md) | Turso to DoSQL | Medium | 2-8 hours |

## Quick Links

- [Migration Checklist Template](./CHECKLIST.md) - Universal pre-migration checklist
- [Testing Your Migration](./testing.md) - How to verify migration success
- [Rollback Procedures](./rollback.md) - Emergency rollback steps

## Migration Overview

### Why Migrate to DoSQL?

DoSQL provides several advanced features not available in traditional edge databases:

| Feature | D1 | Turso | DoSQL |
|---------|-----|-------|-------|
| **Bundle Size** | N/A (binding) | ~500KB (WASM) | **7KB** (native TS) |
| **Change Data Capture (CDC)** | No | No | Yes |
| **Time Travel Queries** | No | Limited (PITR) | Yes |
| **Database Branching** | No | No | Yes |
| **Vector Search** | No | Yes | Yes |
| **Multi-Tenant Isolation** | Limited | Per-database | Per-DO |
| **Real-time Subscriptions** | No | No | Yes |
| **Tiered Storage** | No | No | Yes (Hot/Cold) |
| **Compile-time Type Safety** | No | No | Yes |
| **Custom Stored Procedures** | No | No | Yes (ESM) |

### Migration Process Overview

All migrations follow a similar four-phase process:

```
Phase 1: Assessment & Planning
+---------------------------------------------------------------+
|  - Evaluate current database usage patterns                    |
|  - Identify incompatible features                              |
|  - Create migration timeline                                   |
|  - Set up DoSQL infrastructure                                 |
+---------------------------------------------------------------+
                              |
                              v
Phase 2: Schema & Data Migration
+---------------------------------------------------------------+
|  - Export schema from source database                          |
|  - Transform schema for DoSQL compatibility                    |
|  - Migrate data (full export or incremental sync)              |
|  - Verify data integrity                                       |
+---------------------------------------------------------------+
                              |
                              v
Phase 3: Application Code Migration
+---------------------------------------------------------------+
|  - Update client libraries                                     |
|  - Modify query syntax where needed                            |
|  - Update transaction handling                                 |
|  - Implement error handling changes                            |
+---------------------------------------------------------------+
                              |
                              v
Phase 4: Testing & Cutover
+---------------------------------------------------------------+
|  - Run parallel databases for verification                     |
|  - Perform performance benchmarking                            |
|  - Execute staged cutover                                      |
|  - Monitor and maintain rollback capability                    |
+---------------------------------------------------------------+
```

## Unified Guide Structure

Each migration guide follows this structure for consistency:

1. **Overview**
   - Why Migrate
   - Architecture Differences
   - When to Migrate vs Stay

2. **Compatibility Matrix**
   - SQL Dialect Differences
   - Feature Comparison
   - API Mapping Table

3. **Step-by-Step Migration**
   - Schema Export
   - Schema Import to DoSQL
   - Data Migration Strategies
   - Zero-Downtime Migration

4. **Code Changes Required**
   - Client Library Replacement
   - Query Syntax Adjustments
   - Transaction Handling
   - Error Handling Changes

5. **Testing the Migration**
   - Parallel Running Both Databases
   - Data Consistency Verification
   - Performance Comparison

6. **Rollback Strategy**
   - How to Roll Back
   - Data Sync Considerations

## Choosing a Migration Strategy

### Strategy Comparison

| Strategy | Downtime | Complexity | Data Volume | Risk Level |
|----------|----------|------------|-------------|------------|
| **Full Export/Import** | Yes (minutes to hours) | Low | Small (<1GB) | Low |
| **Incremental Sync** | Minimal | Medium | Medium (1-100GB) | Medium |
| **Dual-Write** | Zero | High | Any | Low |

### Recommendation Matrix

| Scenario | Recommended Strategy |
|----------|---------------------|
| Development/staging environment | Full Export/Import |
| Small production database (<1GB) | Full Export/Import with maintenance window |
| Medium production database (1-100GB) | Incremental Sync |
| Large production database (>100GB) | Dual-Write with staged cutover |
| High-availability requirement | Dual-Write |
| Simple CRUD application | Full Export/Import |
| Complex transaction patterns | Dual-Write with thorough testing |

## Common Migration Patterns

### Pattern 1: Tenant-by-Tenant Migration

For multi-tenant applications, migrate one tenant at a time:

```typescript
async function migrateTenant(tenantId: string): Promise<void> {
  // 1. Create tenant's DoSQL Durable Object
  const dosql = await getDOSQLForTenant(tenantId);

  // 2. Export tenant's data from source
  const data = await exportTenantData(tenantId);

  // 3. Import to DoSQL
  await importToDoSQL(dosql, data);

  // 4. Verify data integrity
  await verifyTenantData(tenantId);

  // 5. Switch tenant to DoSQL
  await switchTenantToDoSQL(tenantId);
}

// Migrate tenants in batches
for (const batch of tenantBatches) {
  await Promise.all(batch.map(migrateTenant));
  await sleep(5000); // Cool-down between batches
}
```

### Pattern 2: Read-Only Migration First

Start with read-only operations before migrating writes:

```typescript
class GradualMigrationDatabase {
  constructor(
    private source: SourceDatabase,
    private dosql: Database,
    private readFromDoSQL: boolean = false,
    private writeToDoSQL: boolean = false
  ) {}

  // Phase 1: Read from source, write to source only
  // Phase 2: Read from DoSQL, write to source + DoSQL
  // Phase 3: Read from DoSQL, write to DoSQL only

  async query<T>(sql: string, params?: unknown[]): Promise<T[]> {
    return this.readFromDoSQL
      ? this.dosql.query<T>(sql, params)
      : this.source.query<T>(sql, params);
  }

  async run(sql: string, params?: unknown[]): Promise<void> {
    if (this.writeToDoSQL) {
      await this.dosql.run(sql, params);
    }
    if (!this.writeToDoSQL || this.isInDualWritePhase()) {
      await this.source.run(sql, params);
    }
  }
}
```

### Pattern 3: Feature-by-Feature Migration

Migrate specific features before full migration:

```typescript
// Start by migrating CDC-dependent features
const cdcFeatures = ['audit_logs', 'notifications', 'analytics'];

for (const feature of cdcFeatures) {
  // Set up CDC subscription in DoSQL
  const cdc = createCDC(dosql);

  // Route feature-specific tables to DoSQL
  await routeFeatureToDoSQL(feature);

  // Keep other features on source database
}
```

## Pre-Migration Checklist

Before starting any migration, ensure you have:

- [ ] **Backed up your source database** - Full backup with tested restore procedure
- [ ] **Documented current schema** - Export and review all tables, indexes, and constraints
- [ ] **Identified incompatible features** - Review the compatibility matrix for your source database
- [ ] **Estimated data volume** - Calculate total data size and row counts
- [ ] **Planned maintenance window** (if applicable) - Coordinate with stakeholders
- [ ] **Set up monitoring** - Prepare dashboards for both source and DoSQL
- [ ] **Prepared rollback procedure** - Document and test rollback steps
- [ ] **Tested in staging** - Complete full migration in non-production environment

## Getting Help

If you encounter issues during migration:

1. Check the troubleshooting section in your specific migration guide
2. Review [DoSQL Error Codes](../ERROR_CODES.md) for error-specific guidance
3. Consult [DoSQL Operations Guide](../OPERATIONS.md) for operational issues
4. See [DoSQL Performance Tuning](../PERFORMANCE_TUNING.md) for performance problems

## Related Documentation

- [DoSQL Architecture](../../packages/dosql/docs/ARCHITECTURE_REVIEW.md)
- [DoSQL API Reference](../../packages/dosql/docs/api-reference.md)
- [DoSQL Getting Started](../../packages/dosql/docs/getting-started.md)
- [DoSQL Examples](../EXAMPLES.md)
