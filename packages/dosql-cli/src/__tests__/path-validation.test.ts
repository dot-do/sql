import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { promises as fs } from 'node:fs';
import { join, resolve } from 'node:path';
import { tmpdir } from 'node:os';
import {
  validatePath,
  isPathWithinBase,
  PathValidationError,
} from '../utils/path-validation.js';

describe('Path validation - directory traversal prevention', () => {
  let testDir: string;
  let projectDir: string;

  beforeEach(async () => {
    // Create a unique temp directory for each test
    testDir = join(tmpdir(), `dosql-path-test-${Date.now()}-${Math.random().toString(36).slice(2)}`);
    projectDir = join(testDir, 'project');
    await fs.mkdir(projectDir, { recursive: true });
  });

  afterEach(async () => {
    // Clean up test directory
    await fs.rm(testDir, { recursive: true, force: true });
  });

  describe('Directory traversal with ../', () => {
    it('should reject paths containing ../ sequences', async () => {
      await expect(validatePath('../../../etc/passwd', projectDir))
        .rejects.toThrow(PathValidationError);
    });

    it('should reject paths with encoded directory traversal', async () => {
      // URL encoded ../ = %2e%2e%2f
      await expect(validatePath('%2e%2e%2f%2e%2e%2fetc/passwd', projectDir))
        .rejects.toThrow(PathValidationError);
    });

    it('should reject paths with mixed traversal patterns', async () => {
      await expect(validatePath('subdir/../../../etc/passwd', projectDir))
        .rejects.toThrow(PathValidationError);
    });

    it('should reject paths that resolve outside project root', async () => {
      // Even if normalized, the resolved path is outside the project
      const outsidePath = join(projectDir, '..', '..', 'etc', 'passwd');
      await expect(validatePath(outsidePath, projectDir))
        .rejects.toThrow(PathValidationError);
    });
  });

  describe('Absolute path validation', () => {
    it('should reject absolute paths outside project directory', async () => {
      await expect(validatePath('/etc/passwd', projectDir))
        .rejects.toThrow(PathValidationError);
    });

    it('should reject absolute paths to sensitive system locations', async () => {
      await expect(validatePath('/etc/shadow', projectDir))
        .rejects.toThrow(PathValidationError);
    });

    it('should reject home directory references outside project', async () => {
      const homeDir = process.env.HOME || '/Users/test';
      const pathOutsideProject = join(homeDir, '.ssh', 'id_rsa');
      await expect(validatePath(pathOutsideProject, projectDir))
        .rejects.toThrow(PathValidationError);
    });
  });

  describe('Valid paths should be accepted', () => {
    it('should accept relative paths within project', async () => {
      // Create the directory first
      await fs.mkdir(join(projectDir, 'migrations'), { recursive: true });
      const result = await validatePath('./migrations', projectDir);
      expect(result).toBe(resolve(projectDir, 'migrations'));
    });

    it('should accept absolute paths within project', async () => {
      const validPath = join(projectDir, 'schema');
      await fs.mkdir(validPath, { recursive: true });
      const result = await validatePath(validPath, projectDir);
      expect(result).toBe(validPath);
    });

    it('should accept paths that do not yet exist (for creation)', async () => {
      // Path validation should work even for non-existent paths
      const newPath = join(projectDir, 'new-folder');
      const result = await validatePath('./new-folder', projectDir);
      expect(result).toBe(newPath);
    });

    it('should accept nested paths within project', async () => {
      await fs.mkdir(join(projectDir, 'deep', 'nested', 'dir'), { recursive: true });
      const result = await validatePath('./deep/nested/dir', projectDir);
      expect(result).toBe(resolve(projectDir, 'deep', 'nested', 'dir'));
    });
  });

  describe('Error messages', () => {
    it('should provide clear error message for traversal attempts', async () => {
      try {
        await validatePath('../../../etc', projectDir);
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(PathValidationError);
        expect((error as PathValidationError).message).toContain('directory traversal');
      }
    });

    it('should provide clear error message for paths outside project', async () => {
      try {
        await validatePath('/etc/passwd', projectDir);
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(PathValidationError);
        expect((error as PathValidationError).message).toContain('outside');
      }
    });

    it('should include the attempted path in error', async () => {
      const maliciousPath = '../../../etc/passwd';
      try {
        await validatePath(maliciousPath, projectDir);
        expect.fail('Should have thrown');
      } catch (error) {
        expect(error).toBeInstanceOf(PathValidationError);
        expect((error as PathValidationError).attemptedPath).toBe(maliciousPath);
      }
    });
  });

  describe('Symbolic link handling', () => {
    it('should reject symlinks that point outside project', async () => {
      // Create a symlink pointing outside the project
      const symlinkPath = join(projectDir, 'evil-link');
      try {
        await fs.symlink('/etc', symlinkPath);
        await expect(validatePath(symlinkPath, projectDir))
          .rejects.toThrow(PathValidationError);
      } catch (err) {
        // Skip test if symlinks not supported (e.g., no permissions)
        if ((err as NodeJS.ErrnoException).code === 'EPERM') {
          return;
        }
        throw err;
      }
    });

    it('should accept symlinks that point within project', async () => {
      // Create a valid internal symlink
      const targetDir = join(projectDir, 'real-dir');
      await fs.mkdir(targetDir, { recursive: true });
      const symlinkPath = join(projectDir, 'linked-dir');
      try {
        await fs.symlink(targetDir, symlinkPath);
        const result = await validatePath(symlinkPath, projectDir);
        // Result is the real path (may have /private prefix on macOS)
        const { realpath } = await import('node:fs/promises');
        const expectedRealPath = await realpath(targetDir);
        expect(result).toBe(expectedRealPath);
      } catch (err) {
        // Skip test if symlinks not supported (e.g., no permissions)
        if ((err as NodeJS.ErrnoException).code === 'EPERM') {
          return;
        }
        throw err;
      }
    });

    it('should handle nested symlinks safely', async () => {
      // Create a chain of symlinks
      const realDir = join(projectDir, 'real');
      await fs.mkdir(realDir, { recursive: true });
      const link1 = join(projectDir, 'link1');
      const link2 = join(projectDir, 'link2');
      try {
        await fs.symlink(realDir, link1);
        await fs.symlink(link1, link2);
        const result = await validatePath(link2, projectDir);
        // Result is the real path (may have /private prefix on macOS)
        const { realpath } = await import('node:fs/promises');
        const expectedRealPath = await realpath(realDir);
        expect(result).toBe(expectedRealPath);
      } catch (err) {
        // Skip test if symlinks not supported (e.g., no permissions)
        if ((err as NodeJS.ErrnoException).code === 'EPERM') {
          return;
        }
        throw err;
      }
    });
  });

  describe('isPathWithinBase utility', () => {
    it('should return true for paths within base', () => {
      expect(isPathWithinBase('/project/foo', '/project')).toBe(true);
    });

    it('should return false for paths outside base', () => {
      expect(isPathWithinBase('/other/foo', '/project')).toBe(false);
    });

    it('should handle edge case of exact match', () => {
      expect(isPathWithinBase('/project', '/project')).toBe(true);
    });

    it('should handle prefix attacks', () => {
      // /project-evil should not be considered within /project
      expect(isPathWithinBase('/project-evil/foo', '/project')).toBe(false);
    });

    it('should handle nested paths', () => {
      expect(isPathWithinBase('/project/deep/nested/path', '/project')).toBe(true);
    });

    it('should handle paths with trailing slashes', () => {
      expect(isPathWithinBase('/project/foo/', '/project/')).toBe(true);
    });
  });

  describe('Edge cases', () => {
    it('should handle null bytes in path', async () => {
      await expect(validatePath('migrations\x00.sql', projectDir))
        .rejects.toThrow(PathValidationError);
    });

    it('should handle Windows-style path separators', async () => {
      await expect(validatePath('..\\..\\etc\\passwd', projectDir))
        .rejects.toThrow(PathValidationError);
    });

    it('should handle extremely long paths', async () => {
      const longPath = 'a/'.repeat(2500) + 'file.sql';
      await expect(validatePath(longPath, projectDir))
        .rejects.toThrow(PathValidationError);
    });

    it('should handle empty path', async () => {
      await expect(validatePath('', projectDir))
        .rejects.toThrow(PathValidationError);
    });

    it('should handle path with only whitespace', async () => {
      await expect(validatePath('   ', projectDir))
        .rejects.toThrow(PathValidationError);
    });

    it('should handle path with only dots (not traversal)', async () => {
      // "..." is a valid filename, not a traversal attempt
      const result = await validatePath('...', projectDir);
      expect(result).toBe(resolve(projectDir, '...'));
    });

    it('should handle path with special characters', async () => {
      // Create directory with spaces
      const dirWithSpaces = join(projectDir, 'my migrations');
      await fs.mkdir(dirWithSpaces, { recursive: true });
      const result = await validatePath('./my migrations', projectDir);
      expect(result).toBe(dirWithSpaces);
    });

    it('should handle unicode paths', async () => {
      const unicodePath = join(projectDir, 'migrations-\u00e9');
      await fs.mkdir(unicodePath, { recursive: true });
      const result = await validatePath('./migrations-\u00e9', projectDir);
      expect(result).toBe(unicodePath);
    });
  });
});
