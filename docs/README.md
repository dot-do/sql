<!--
Documentation Link Structure
=============================

Root Level (docs/):
  - ARCHITECTURE_REVIEW.md
  - CODE_REVIEW.md
  - ERROR_CODES.md
  - EXAMPLES.md
  - MIGRATION_FROM_D1.md
  - MIGRATION_FROM_TURSO.md
  - OPERATIONS.md
  - PERFORMANCE_TUNING.md
  - PRODUCT_REVIEW.md
  - SECURITY.md
  - STABILITY.md
  - TESTING_REVIEW.md
  - TYPESCRIPT_REVIEW.md
  - integrations/ (empty - see packages/dosql/docs/integrations/)

Package Documentation (packages/dosql/docs/):
  - README.md (index)
  - getting-started.md
  - api-reference.md
  - advanced.md
  - architecture.md
  - TROUBLESHOOTING.md
  - PERSONAS.md
  - USE_CASES.md
  - integrations/
    - NEXTJS.md
    - REMIX.md
  - (various review/analysis docs)

Package Documentation (packages/dolake/docs/):
  - API.md
  - ARCHITECTURE.md
  - INTEGRATION.md

Cross-Package Links:
  - Root docs link to packages via ../packages/dosql/docs/
  - Package docs link to root via ../../docs/
  - Integration docs link to parent via ../

Anchor Links:
  - All anchors are auto-generated from headings (lowercase, spaces to hyphens)
  - Example: "## Bundle Size" -> #bundle-size
-->

# DoSQL/DoLake Documentation

This directory contains documentation for the DoSQL and DoLake project.

## Quick Links

| Guide | Description |
|-------|-------------|
| [Performance Tuning](./PERFORMANCE_TUNING.md) | Query optimization, DO tuning, WebSocket performance |
| [Migration from D1](./MIGRATION_FROM_D1.md) | Step-by-step guide for Cloudflare D1 users |
| [Migration from Turso](./MIGRATION_FROM_TURSO.md) | Guide for libSQL/Turso users |
| [Error Codes](./ERROR_CODES.md) | Complete error code documentation |
| [Security](./SECURITY.md) | Authentication, authorization, and security best practices |
| [Operations](./OPERATIONS.md) | Deployment, monitoring, and maintenance |
| [Examples](./EXAMPLES.md) | Code examples and patterns |

## Package Documentation

- [DoSQL Documentation](../packages/dosql/docs/README.md) - Full DoSQL documentation
- [DoLake Documentation](../packages/dolake/README.md) - DoLake lakehouse documentation

## Review Documents

Internal review documents for development:

| Review | Description |
|--------|-------------|
| [Architecture Review](./ARCHITECTURE_REVIEW.md) | System architecture analysis |
| [Code Review](./CODE_REVIEW.md) | Code quality assessment |
| [Product Review](./PRODUCT_REVIEW.md) | Product requirements review |
| [Testing Review](./TESTING_REVIEW.md) | Test coverage analysis |
| [TypeScript Review](./TYPESCRIPT_REVIEW.md) | Type safety assessment |
| [Stability](./STABILITY.md) | API stability status |
