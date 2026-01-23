/**
 * GREEN Phase: Coverage Requirements Verification
 *
 * These tests verify that coverage configuration is properly set up
 * across the project.
 *
 * Previously these were RED phase tests using `it.fails()`.
 * Now that coverage is configured, they verify the implementation.
 */

import { describe, it, expect } from 'vitest';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

describe('Coverage Configuration Requirements', () => {
  // Go from packages/shared-types/src/__tests__ to packages/
  const packagesDir = path.resolve(__dirname, '../../../');

  describe('Coverage thresholds are enforced', () => {
    it('should have coverage thresholds configured in vitest.config.ts', () => {
      const vitestConfig = fs.readFileSync(
        path.join(packagesDir, 'shared-types/vitest.config.ts'),
        'utf-8'
      );

      // Check for coverage configuration
      expect(vitestConfig).toContain('coverage:');
      expect(vitestConfig).toContain('thresholds:');
    });

    it('should enforce 80% statement coverage threshold', () => {
      const vitestConfig = fs.readFileSync(
        path.join(packagesDir, 'shared-types/vitest.config.ts'),
        'utf-8'
      );

      // Check for specific thresholds
      expect(vitestConfig).toContain('statements: 80');
    });

    it('should enforce 80% branch coverage threshold', () => {
      const vitestConfig = fs.readFileSync(
        path.join(packagesDir, 'shared-types/vitest.config.ts'),
        'utf-8'
      );

      expect(vitestConfig).toContain('branches: 80');
    });

    it('should enforce 80% function coverage threshold', () => {
      const vitestConfig = fs.readFileSync(
        path.join(packagesDir, 'shared-types/vitest.config.ts'),
        'utf-8'
      );

      expect(vitestConfig).toContain('functions: 80');
    });

    it('should enforce 80% line coverage threshold', () => {
      const vitestConfig = fs.readFileSync(
        path.join(packagesDir, 'shared-types/vitest.config.ts'),
        'utf-8'
      );

      expect(vitestConfig).toContain('lines: 80');
    });
  });

  describe('Coverage is collected for all packages', () => {
    const packages = ['shared-types', 'sql.do', 'lake.do', 'dosql', 'dolake', 'dosql-cli'];

    packages.forEach((pkg) => {
      it(`should have coverage configured for ${pkg}`, () => {
        const configPath = path.join(packagesDir, pkg, 'vitest.config.ts');

        // Check that vitest config exists
        expect(fs.existsSync(configPath)).toBe(true);

        const vitestConfig = fs.readFileSync(configPath, 'utf-8');

        // Check for coverage configuration
        expect(vitestConfig).toContain('coverage:');
        expect(vitestConfig).toContain('provider:');
      });
    });
  });

  describe('Coverage report is generated in CI', () => {
    // Root of the project (packages/../)
    const rootDir = path.resolve(packagesDir, '..');

    it('should have coverage collection in GitHub Actions workflow', () => {
      const workflowPath = path.join(rootDir, '.github/workflows/test.yml');
      const workflow = fs.readFileSync(workflowPath, 'utf-8');

      // Check for coverage-related steps
      expect(workflow).toContain('coverage');
      expect(workflow).toContain('vitest run --coverage');
    });

    it('should upload coverage reports to Codecov', () => {
      const workflowPath = path.join(rootDir, '.github/workflows/test.yml');
      const workflow = fs.readFileSync(workflowPath, 'utf-8');

      // Check for Codecov integration
      expect(workflow).toContain('codecov/codecov-action');
    });

    it('should merge coverage reports from all packages', () => {
      const workflowPath = path.join(rootDir, '.github/workflows/test.yml');
      const workflow = fs.readFileSync(workflowPath, 'utf-8');

      // Check for coverage report merging
      expect(workflow).toContain('coverage-final.json');
    });
  });

  describe('Coverage badge is displayed in README', () => {
    const rootDir = path.resolve(packagesDir, '..');

    it('should have coverage badge in root README.md', () => {
      const readmePath = path.join(rootDir, 'README.md');
      const readme = fs.readFileSync(readmePath, 'utf-8');

      // Check for Codecov badge
      expect(readme).toContain('codecov.io');
      expect(readme.toLowerCase()).toContain('coverage');
    });

    it('should have coverage badge URL format', () => {
      const readmePath = path.join(rootDir, 'README.md');
      const readme = fs.readFileSync(readmePath, 'utf-8');

      // Check for proper badge format
      expect(readme).toMatch(/\[!\[.*[Cc]overage.*\]\(.*codecov\.io.*\)/);
    });
  });
});
