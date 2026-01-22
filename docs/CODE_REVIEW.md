# Code Review: @dotdo/sql Monorepo

**Review Date:** 2026-01-22
**Repository:** /Users/nathanclevenger/projects/sql
**Reviewer:** Claude Code

## Executive Summary

The `@dotdo/sql` monorepo is a newly initialized project intended to contain DoSQL (SQL database engine) and DoLake (lakehouse for CDC/analytics) packages for Cloudflare Workers. Currently, the repository contains only skeleton configuration files with no actual package implementations.

**Overall Status:** Project Initialization Phase - No source code to review

---

## Repository Structure

### Current State

```
/Users/nathanclevenger/projects/sql/
├── .beads/                    # Issue tracking (Beads)
│   ├── metadata.json
│   ├── README.md
│   ├── config.yaml
│   └── beads.db               # SQLite database
├── .git/                      # Git repository
├── .gitattributes             # Git LFS/merge config
├── .gitignore                 # Ignore patterns
├── .nvmrc                     # Node version (20)
├── AGENTS.md                  # Agent workflow instructions
├── CLAUDE.md                  # Detailed project instructions
├── README.md                  # Project overview
├── package.json               # Root package manifest
├── packages/                  # Empty - no packages yet
└── pnpm-workspace.yaml        # Workspace configuration
```

### Planned Structure (from CLAUDE.md)

```
packages/
├── dosql/          # @dotdo/dosql - SQL database engine
│   ├── src/
│   │   ├── parser/     # SQL parser
│   │   ├── planner/    # Query planner and optimizer
│   │   ├── engine/     # Execution engine
│   │   ├── sharding/   # Horizontal scaling
│   │   ├── btree/      # B-tree index implementation
│   │   ├── fsx/        # File system abstraction
│   │   ├── wal/        # Write-ahead log
│   │   ├── transaction/# Transaction management
│   │   ├── database/   # Database operations
│   │   └── worker/     # Durable Object entry point
│   └── wrangler.jsonc
│
└── dolake/         # @dotdo/dolake - Lakehouse
    ├── src/
    │   ├── dolake.ts       # Main Durable Object
    │   ├── compaction.ts   # Parquet file compaction
    │   ├── partitioning.ts # Table partitioning
    │   ├── query-engine.ts # Analytical queries
    │   ├── rate-limiter.ts # WebSocket rate limiting
    │   └── schemas.ts      # Zod validation schemas
    └── wrangler.jsonc
```

---

## Review of Existing Files

### Configuration Files

#### package.json

| Aspect | Assessment |
|--------|------------|
| Name | Correct: `@dotdo/sql` |
| Version | `0.1.0` - appropriate for initial development |
| Type | `module` - correct for ESM |
| Package Manager | `pnpm@9.15.0` - explicitly pinned, good |
| Engines | Node >=20.0.0, pnpm >=9.0.0 - appropriate |
| Scripts | Standard workspace scripts defined |

**Issues Found:**

| Severity | Issue | Recommendation |
|----------|-------|----------------|
| **Low** | Missing `devDependencies` | Add TypeScript, vitest, eslint, prettier when creating packages |
| **Low** | Missing `tsconfig.json` | Create root TypeScript configuration |
| **Low** | Missing `vitest.config.ts` | Create root Vitest configuration |
| **Low** | Missing `.prettierrc` | Create consistent code formatting config |
| **Low** | Missing `eslint.config.js` | Create linting configuration |

#### pnpm-workspace.yaml

```yaml
packages:
  - 'packages/*'
```

**Assessment:** Minimal but correct. No issues.

#### .gitignore

**Assessment:** Comprehensive and appropriate for the project:
- Node modules
- Build outputs
- Cloudflare-specific files
- IDE files
- Environment files
- Beads local files

**Issues Found:**

| Severity | Issue | Recommendation |
|----------|-------|----------------|
| **Low** | Missing `*.d.ts.map` pattern | Add to prevent committing source maps if not wanted |

#### .nvmrc

Contains `20` - correct for Node.js 20 LTS.

#### .gitattributes

```
.beads/issues.jsonl merge=beads
```

**Assessment:** Correct configuration for Beads merge driver.

---

### Documentation Files

#### README.md

**Assessment:** Well-structured with:
- Clear project description
- Feature list
- Architecture diagram
- Getting started instructions

**Issues Found:**

| Severity | Issue | Recommendation |
|----------|-------|----------------|
| **Low** | Architecture diagram references non-existent components | Update once packages are created |
| **Low** | No contribution guidelines | Add CONTRIBUTING.md when project matures |
| **Low** | No license file | Add LICENSE file (MIT mentioned in package.json) |

#### CLAUDE.md

**Assessment:** Excellent project instructions including:
- Project structure
- Development commands
- Testing philosophy (TDD with NO MOCKS)
- Code style guidelines
- Beads workflow documentation

**Issues Found:** None - well-documented

#### AGENTS.md

**Assessment:** Good quick reference for agent workflows. Properly duplicates essential information from CLAUDE.md.

---

## Critical Gaps: Missing Components

### High Priority

| Missing Component | Description | Recommended Action |
|-------------------|-------------|-------------------|
| `packages/dosql/` | Main SQL database engine | Create package skeleton |
| `packages/dolake/` | Lakehouse for CDC/analytics | Create package skeleton |
| `tsconfig.json` | Root TypeScript configuration | Create with strict mode |
| `vitest.config.ts` | Root test configuration | Create with workers-vitest-pool |
| `wrangler.jsonc` | Cloudflare Workers config | Create per package |

### Medium Priority

| Missing Component | Description | Recommended Action |
|-------------------|-------------|-------------------|
| `eslint.config.js` | Linting configuration | Create ESLint flat config |
| `.prettierrc` | Code formatting | Create Prettier config |
| `LICENSE` | MIT license file | Create license file |
| `CONTRIBUTING.md` | Contribution guidelines | Create when ready for contributors |
| `.github/` | CI/CD workflows | Create GitHub Actions |

### Low Priority

| Missing Component | Description | Recommended Action |
|-------------------|-------------|-------------------|
| `CHANGELOG.md` | Version history | Create when releasing |
| `docs/` | Additional documentation | Expand as needed |

---

## Recommended Initial Package Structure

### packages/dosql/package.json

```json
{
  "name": "@dotdo/dosql",
  "version": "0.1.0",
  "description": "SQL database engine for Cloudflare Workers",
  "type": "module",
  "main": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "exports": {
    ".": {
      "types": "./dist/index.d.ts",
      "import": "./dist/index.js"
    }
  },
  "scripts": {
    "build": "tsc",
    "test": "vitest run",
    "typecheck": "tsc --noEmit",
    "lint": "eslint src",
    "clean": "rm -rf dist"
  },
  "peerDependencies": {
    "@cloudflare/workers-types": "^4.0.0"
  },
  "devDependencies": {
    "typescript": "^5.4.0",
    "vitest": "^2.0.0",
    "@cloudflare/vitest-pool-workers": "^0.5.0"
  }
}
```

### packages/dolake/package.json

```json
{
  "name": "@dotdo/dolake",
  "version": "0.1.0",
  "description": "Lakehouse for CDC streaming and analytics on Cloudflare Workers",
  "type": "module",
  "main": "./dist/index.js",
  "types": "./dist/index.d.ts",
  "exports": {
    ".": {
      "types": "./dist/index.d.ts",
      "import": "./dist/index.js"
    }
  },
  "scripts": {
    "build": "tsc",
    "test": "vitest run",
    "typecheck": "tsc --noEmit",
    "lint": "eslint src",
    "clean": "rm -rf dist"
  },
  "peerDependencies": {
    "@cloudflare/workers-types": "^4.0.0"
  },
  "dependencies": {
    "zod": "^3.23.0"
  },
  "devDependencies": {
    "typescript": "^5.4.0",
    "vitest": "^2.0.0",
    "@cloudflare/vitest-pool-workers": "^0.5.0"
  }
}
```

### Root tsconfig.json

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "lib": ["ES2022"],
    "strict": true,
    "noUncheckedIndexedAccess": true,
    "noEmit": true,
    "skipLibCheck": true,
    "esModuleInterop": true,
    "resolveJsonModule": true,
    "isolatedModules": true,
    "verbatimModuleSyntax": true,
    "types": ["@cloudflare/workers-types"]
  },
  "exclude": ["node_modules", "dist"]
}
```

---

## Security Considerations

| Category | Current State | Recommendation |
|----------|---------------|----------------|
| **Environment Variables** | `.env*` properly gitignored | Maintain this pattern |
| **Secrets** | No hardcoded secrets found | Continue to use environment bindings |
| **Dependencies** | None yet | Review dependencies before adding |
| **SQL Injection** | N/A (no code) | Use parameterized queries in DoSQL |
| **Input Validation** | N/A (no code) | Use Zod schemas as documented for DoLake |

---

## Testing Strategy Validation

The CLAUDE.md specifies an excellent testing philosophy:

> - Tests run in actual Cloudflare Workers environment via `workers-vitest-pool`
> - Use real Durable Objects, not mocks
> - Use real SQLite, not mocks
> - Integration tests over unit tests

**Assessment:** This approach is correct for Cloudflare Workers. The `@cloudflare/vitest-pool-workers` package provides a real Workers runtime for testing.

---

## Issue Summary by Severity

### Critical Issues
None - the repository is correctly initialized but empty.

### High Priority Issues
| # | Issue | Location | Action Required |
|---|-------|----------|-----------------|
| 1 | Missing DoSQL package | `packages/dosql/` | Create package skeleton |
| 2 | Missing DoLake package | `packages/dolake/` | Create package skeleton |
| 3 | No TypeScript configuration | Root | Create `tsconfig.json` |
| 4 | No test configuration | Root | Create `vitest.config.ts` |

### Medium Priority Issues
| # | Issue | Location | Action Required |
|---|-------|----------|-----------------|
| 5 | No linting configuration | Root | Create `eslint.config.js` |
| 6 | No formatting configuration | Root | Create `.prettierrc` |
| 7 | Missing LICENSE file | Root | Create MIT license |
| 8 | No CI/CD configuration | `.github/` | Create GitHub Actions workflows |

### Low Priority Issues
| # | Issue | Location | Action Required |
|---|-------|----------|-----------------|
| 9 | Missing dev dependencies in root | `package.json` | Add shared dev dependencies |
| 10 | No CHANGELOG.md | Root | Create when releasing |
| 11 | Architecture diagram references non-existent code | `README.md` | Update after implementation |

---

## Recommendations

### Immediate Actions

1. **Create package skeletons** - Initialize `packages/dosql` and `packages/dolake` with basic structure
2. **Add TypeScript configuration** - Create root and per-package `tsconfig.json`
3. **Add test configuration** - Create `vitest.config.ts` with workers pool
4. **Create LICENSE file** - Add MIT license as specified in package.json

### Short-term Actions

5. **Set up linting** - Add ESLint with TypeScript support
6. **Set up formatting** - Add Prettier configuration
7. **Create CI/CD** - Add GitHub Actions for build, test, lint

### Development Best Practices

Based on CLAUDE.md guidelines, ensure the following as code is written:

- Use TypeScript strict mode
- Use branded types for IDs (TransactionId, LSN)
- Prefer `unknown` over `any`
- Write integration tests using workers-vitest-pool
- No unnecessary abstractions

---

## Conclusion

The `@dotdo/sql` monorepo is properly initialized with good documentation and configuration groundwork. The project structure is well-planned in CLAUDE.md, but no actual implementation exists yet.

**Next Steps:**
1. Create the DoSQL and DoLake package skeletons
2. Add TypeScript and testing configuration
3. Begin implementing core functionality according to the planned structure

The documentation quality is high, indicating thoughtful planning. The testing philosophy (TDD with real Workers runtime, no mocks) is correct for this type of project.

---

*Generated by Claude Code on 2026-01-22*
