# API Stability Policy

This document describes the stability policy for the sql.do and lake.do client packages.

## Overview

All public exports are marked with stability annotations using JSDoc tags:

- `@stability stable` - Safe for production use
- `@stability experimental` - May change in any version
- `@internal` - Not part of the public API

## Stability Levels

### Stable

Exports marked as **stable** follow semantic versioning guarantees:

- **No breaking changes** in minor versions (0.x.y to 0.x.z)
- Breaking changes only in major versions (0.x to 1.0, 1.x to 2.0)
- Deprecation warnings before removal
- Bug fixes may change behavior to match documented expectations

**Stable exports include:**

#### sql.do

- `createSQLClient` - Factory function
- `DoSQLClient` - Client class
- `SQLError` - Error class
- `TransactionContext` - Transaction helper
- `SQLClientConfig`, `RetryConfig` - Configuration types
- `SQLClient` interface and core types
- Query types: `QueryResult`, `QueryOptions`, `PreparedStatement`
- Transaction types: `TransactionState`, `TransactionOptions`
- Schema types: `TableSchema`, `ColumnDefinition`
- Brand constructors: `createTransactionId`, `createLSN`, etc.
- Type utilities: `sqlToJsType`, `jsToSqlType`

#### lake.do

- `createLakeClient` - Factory function
- `DoLakeClient` - Client class
- `LakeError` - Error class
- `LakeClientConfig`, `RetryConfig` - Configuration types
- `LakeClient` interface
- Query types: `LakeQueryResult`, `LakeQueryOptions`
- Partition types: `PartitionInfo`, `PartitionConfig`
- Snapshot types: `Snapshot`, `TimeTravelOptions`
- Schema types: `LakeSchema`, `LakeColumn`, `TableMetadata`
- Branded types and constructors

### Experimental

Exports marked as **experimental** may change in any release:

- API may change without notice
- May be removed entirely
- May have bugs or incomplete implementations
- Not recommended for production without caution

**Experimental exports include:**

#### sql.do

- `generateIdempotencyKey` - Idempotency key generation
- `isMutationQuery` - SQL mutation detection
- CDC types and utilities
- Sharding types: `ShardConfig`, `ShardInfo`
- Client capabilities: `ClientCapabilities`, `DEFAULT_CLIENT_CAPABILITIES`
- Connection types: `ConnectionOptions`, `ConnectionStats`
- Response/Result converters

#### lake.do

- CDC streaming: `CDCStreamOptions`, `CDCBatch`, `CDCStreamState`
- Compaction: `CompactionConfig`, `CompactionJob`
- Metrics: `LakeMetrics`
- CDC utilities from sql.do re-exports

### Internal

Exports marked as **@internal** are not part of the public API:

- May change or be removed without notice
- Not covered by semantic versioning
- Use at your own risk

**Internal exports include:**

- RPC types: `RPCRequest`, `RPCResponse`, `LakeRPCMethod`
- Helper functions and utilities
- Implementation details

## Importing Experimental Features

Experimental features can be imported from the `/experimental` subpath:

```typescript
// Explicit import of experimental features
import { generateIdempotencyKey } from '@dotdo/sql.do/experimental';
import { CDCStreamOptions, CompactionConfig } from '@dotdo/lake.do/experimental';
```

Alternatively, they are also available from the main entry point with JSDoc warnings:

```typescript
// Also available from main export (with stability annotations in docs)
import { generateIdempotencyKey, CDCOperationCode } from '@dotdo/sql.do';
```

## Migration Path

When experimental features become stable:

1. The feature will be announced as stable in release notes
2. The `@stability` annotation will change to `stable`
3. No code changes required - imports remain the same

When experimental features are modified:

1. Breaking changes documented in CHANGELOG
2. Migration guide provided if significant changes
3. Old API deprecated before removal when possible

When experimental features are removed:

1. Removal announced in release notes
2. TypeScript errors will guide migration
3. Consider using the replacement API documented in CHANGELOG

## Reporting Issues

### For Stable Features

- Bug reports with reproduction steps
- Documentation improvements
- Performance issues

### For Experimental Features

When reporting issues with experimental features:

1. Include "experimental" in the issue title
2. Describe your use case
3. Provide feedback on API design
4. Suggest improvements

GitHub Issues: https://github.com/dot-do/sql/issues

## Version History

### 0.1.0

Initial release with stability annotations:

**Stable:**
- Core SQL client functionality
- Core Lake client functionality
- Query execution and results
- Transaction management
- Schema inspection
- Partition management
- Time travel queries

**Experimental:**
- Idempotency key generation
- CDC streaming
- File compaction
- Sharding
- Metrics

## Best Practices

### For Production Code

1. Prefer stable APIs when available
2. Pin exact package versions
3. Test thoroughly when upgrading
4. Read CHANGELOG before upgrading

### For Experimental Features

1. Wrap in feature flags
2. Have fallback strategies
3. Monitor for deprecation notices
4. Provide feedback to help stabilize APIs

### Example: Safe Usage of Experimental Features

```typescript
import { createSQLClient } from '@dotdo/sql.do';

// Use stable features directly
const client = createSQLClient({ url: 'https://sql.example.com' });
const result = await client.query('SELECT * FROM users');

// Wrap experimental features
let idempotencyKey: string | undefined;
try {
  // Dynamic import for experimental features
  const { generateIdempotencyKey } = await import('@dotdo/sql.do/experimental');
  idempotencyKey = await generateIdempotencyKey('INSERT INTO logs (msg) VALUES (?)', ['test']);
} catch {
  // Fallback if experimental API changes
  idempotencyKey = `fallback-${Date.now()}`;
}
```

## FAQ

### Why are some features experimental?

Features are marked experimental when:
- The API design is still evolving
- Implementation is incomplete
- More user feedback is needed
- Dependencies are unstable

### When will experimental features become stable?

Features graduate to stable when:
- API design is finalized
- Implementation is complete and tested
- Sufficient production usage
- Documentation is comprehensive

### Can I use experimental features in production?

Yes, with caution:
- Understand the risks
- Have fallback strategies
- Pin exact package versions
- Monitor changelogs closely

### How do I know if something is experimental?

1. Check JSDoc `@stability` tags in IDE
2. Check this STABILITY.md document
3. Import from `/experimental` subpath for explicit marking
4. Check package documentation
