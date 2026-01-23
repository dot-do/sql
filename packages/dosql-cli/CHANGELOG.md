# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Nothing yet

## [0.1.0] - 2026-01-22

### Added
- Initial release of `@dotdo/dosql-cli`
- `dosql init` command for project scaffolding
  - Creates `dosql.config.ts` configuration file
  - Creates `migrations/` directory for SQL migrations
  - Creates `schema/schema.ts` with example table definition
  - Supports `--directory`, `--name`, and `--force` options
- `dosql migrate` command for database migrations
  - Finds SQL migration files in migrations directory
  - Supports WebSocket connection to DoSQL databases
  - Tracks applied migrations in `_migrations` table
  - Computes SHA-256 checksums for migration integrity
  - Supports `--dry-run` mode to preview pending migrations
  - Environment variable support (`DOSQL_URL`, `DOSQL_TOKEN`)
  - Configurable connection timeout
- `dosql generate` command for TypeScript type generation
  - Parses schema files using TypeScript AST
  - Generates TypeScript interfaces from table definitions
  - Preserves JSDoc comments from schema to generated types
  - Supports complex schema syntax (type assertions, satisfies, spread operators)
  - Maps SQL types to TypeScript types:
    - `integer` -> `number`
    - `real` -> `number`
    - `text` -> `string`
    - `boolean` -> `boolean`
    - `blob` -> `Uint8Array`
- Programmatic API for all commands
  - `createCLI()` for custom CLI implementations
  - `initProject()`, `generateTypes()`, `findMigrations()`, `runMigrations()`
  - `createMigrationConnection()` for database connectivity
  - `runMigrationsWithConnection()` for integrated migration workflow
- Path validation utilities to prevent directory traversal attacks
  - `validatePath()` and `validateCliPath()` for secure path handling
  - `PathValidationError` for validation failures
- Logging utilities for consistent CLI output
  - `Logger` class with configurable log levels
  - Convenience functions: `logInfo()`, `logError()`, `logSuccess()`, etc.
- Error handling utilities
  - `toError()` for coercing unknown errors
  - `getErrorMessage()` for extracting error messages
- File system utilities
  - `fileExists()` and `fileExistsSync()` for path existence checks

### Security
- Path validation prevents directory traversal attacks
- Maximum path length enforcement (4096 characters)
- URL-encoded traversal pattern detection
- Symlink target validation

[Unreleased]: https://github.com/dotdo/dosql-cli/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/dotdo/dosql-cli/releases/tag/v0.1.0
