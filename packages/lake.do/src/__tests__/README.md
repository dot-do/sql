# lake.do Client SDK Test Suite

Test organization for the `lake.do` package (DoLake client SDK).

## Test Files

- `client.test.ts` - Lake client connection, query, and streaming
- `cdc-backpressure.test.ts` - CDC backpressure handling

## End-to-End Tests (`e2e/__tests__/`)

- `lake-e2e.test.ts` - End-to-end lake operations

## Running Tests

```bash
pnpm --filter lake.do test
```

## Test Conventions

- `describe()` blocks use `Module - Feature` naming (dash separator)
- `it()` blocks start with "should"
- NO MOCKS philosophy applies
