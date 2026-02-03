# sql.do Client SDK Test Suite

Test organization for the `sql.do` package (DoSQL client SDK).

## Test Files

- `client.test.ts` - DoSQLClient constructor, connection, query execution, transactions
- `connection-manager.test.ts` - ConnectionManager WebSocket management and pooling
- `connection-lifecycle.test.ts` - Connect, disconnect, reconnect flows
- `connection-pool.test.ts` - Connection pool initialization, exhaustion, health checks
- `cache-cleanup.test.ts` - Cache cleanup and eviction
- `storage-config.test.ts` - Storage configuration types and validation
- `streaming-cdc.test.ts` - CDC event streaming and subscriptions

## Shared Utilities

- `test-utils.ts` - MockWebSocket, test client factories, assertion helpers

## Running Tests

```bash
pnpm --filter sql.do test
```

## Test Conventions

- `describe()` blocks use `Module - Feature` naming (dash separator)
- `it()` blocks start with "should"
- MockWebSocket provides a controllable WebSocket implementation for testing
