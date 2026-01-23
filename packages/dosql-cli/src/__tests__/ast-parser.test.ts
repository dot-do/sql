import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { promises as fs } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { generateTypes } from '../commands/generate.js';

const __dirname = dirname(fileURLToPath(import.meta.url));

/**
 * GREEN PHASE TESTS: AST-based schema parsing
 *
 * These tests validate the AST-based schema parser using TypeScript Compiler API.
 * The parser correctly handles complex TypeScript syntax that regex-based parsing cannot:
 *
 * - Type assertions (e.g., `'integer' as ColumnType`)
 * - Satisfies operator (e.g., `{ ... } satisfies Column<'integer'>`)
 * - Multi-line column definitions
 * - Computed property names (e.g., `[ID_COLUMN]: { ... }`)
 * - Property shorthand (e.g., `{ tableName, columns }`)
 * - External const references
 * - Spread operators (e.g., `{ ...baseColumns }`)
 * - JSDoc comments preservation
 * - Double/single/backtick string literals
 * - Multiple tables per file
 */
describe('AST-based schema parsing', () => {
  let testDir: string;
  let schemaDir: string;
  let outputDir: string;

  beforeEach(async () => {
    // Use a temp directory within the package to satisfy path validation
    testDir = join(__dirname, `../.test-tmp-${Date.now()}-${Math.random().toString(36).slice(2)}`);
    schemaDir = join(testDir, 'schema');
    outputDir = join(testDir, 'generated');
    await fs.mkdir(schemaDir, { recursive: true });
  });

  afterEach(async () => {
    await fs.rm(testDir, { recursive: true, force: true });
  });

  describe('type assertions on values', () => {
    it('should parse columns with type assertions like `as ColumnType`', async () => {
      // Regex column pattern: (\w+):\s*\{\s*type:\s*['"](\w+)['"]
      // The `as ColumnType` after the type value breaks the regex
      const schemaContent = `
type ColumnType = 'integer' | 'text' | 'real' | 'blob' | 'boolean';

export const users = {
  tableName: 'users',
  columns: {
    id: { type: 'integer' as ColumnType, primaryKey: true },
    name: { type: 'text' as ColumnType, nullable: false },
  },
} as const;
`;
      await fs.writeFile(join(schemaDir, 'users.ts'), schemaContent);

      const result = await generateTypes({ schemaDir, outputDir });
      expect(result.tablesProcessed).toContain('users');

      const content = await fs.readFile(join(outputDir, 'types.ts'), 'utf-8');
      expect(content).toContain('interface User');
      expect(content).toContain('id: number');
      expect(content).toContain('name: string');
    });

    it('should parse columns with satisfies on column definitions', async () => {
      // Regex cannot handle satisfies operator after column object
      const schemaContent = `
type Column<T> = { type: T; primaryKey?: boolean; nullable?: boolean };

export const posts = {
  tableName: 'posts',
  columns: {
    id: { type: 'integer', primaryKey: true } satisfies Column<'integer'>,
    title: { type: 'text', nullable: false } satisfies Column<'text'>,
  },
} as const;
`;
      await fs.writeFile(join(schemaDir, 'posts.ts'), schemaContent);

      const result = await generateTypes({ schemaDir, outputDir });
      expect(result.tablesProcessed).toContain('posts');

      const content = await fs.readFile(join(outputDir, 'types.ts'), 'utf-8');
      expect(content).toContain('interface Post');
      expect(content).toContain('id: number');
      expect(content).toContain('title: string');
    });
  });

  describe('multi-line column definitions', () => {
    it('should parse columns with expanded multi-line formatting', async () => {
      // Regex column pattern expects type on same line or specific format
      const schemaContent = `
export const orders = {
  tableName: 'orders',
  columns: {
    id: {
      type: 'integer',
      primaryKey: true,
    },
    customer_id: {
      type: 'integer',
      nullable: false,
    },
    total: {
      type: 'real',
      nullable: true,
    },
  },
} as const;
`;
      await fs.writeFile(join(schemaDir, 'orders.ts'), schemaContent);

      const result = await generateTypes({ schemaDir, outputDir });
      expect(result.tablesProcessed).toContain('orders');

      const content = await fs.readFile(join(outputDir, 'types.ts'), 'utf-8');
      expect(content).toContain('interface Order');
      expect(content).toContain('id: number');
      expect(content).toContain('customer_id: number');
      expect(content).toContain('total: number | null');
    });

    it('should handle trailing comments in column definitions', async () => {
      // Comments break the regex pattern
      const schemaContent = `
export const products = {
  tableName: 'products',
  columns: {
    id: { type: 'integer', primaryKey: true }, // Primary key
    name: { type: 'text', nullable: false }, // Required field
    price: { type: 'real', nullable: true }, // Optional
  },
} as const;
`;
      await fs.writeFile(join(schemaDir, 'products.ts'), schemaContent);

      const result = await generateTypes({ schemaDir, outputDir });
      expect(result.tablesProcessed).toContain('products');

      const content = await fs.readFile(join(outputDir, 'types.ts'), 'utf-8');
      expect(content).toContain('interface Product');
      expect(content).toContain('id: number');
      expect(content).toContain('name: string');
      expect(content).toContain('price: number | null');
    });
  });

  describe('computed property names', () => {
    it('should resolve computed property names to actual column names', async () => {
      // Regex cannot handle computed property names [CONSTANT]
      const schemaContent = `
const ID_COLUMN = 'id';
const NAME_COLUMN = 'name';

export const categories = {
  tableName: 'categories',
  columns: {
    [ID_COLUMN]: { type: 'integer', primaryKey: true },
    [NAME_COLUMN]: { type: 'text', nullable: false },
  },
} as const;
`;
      await fs.writeFile(join(schemaDir, 'categories.ts'), schemaContent);

      await generateTypes({ schemaDir, outputDir });

      const content = await fs.readFile(join(outputDir, 'types.ts'), 'utf-8');
      // Note: Simple singularization "categories" -> "Categorie" (not "Category")
      // This is a known limitation of the simple singularization logic
      expect(content).toContain('interface Categorie');
      // Should resolve [ID_COLUMN] to 'id'
      expect(content).toContain('id: number');
      // Should resolve [NAME_COLUMN] to 'name'
      expect(content).toContain('name: string');
    });

    it('should handle template literal property names', async () => {
      // Regex cannot resolve template literals
      const schemaContent = `
const prefix = 'meta_';

export const metadata = {
  tableName: 'metadata',
  columns: {
    id: { type: 'integer', primaryKey: true },
    [\`\${prefix}key\`]: { type: 'text', nullable: false },
    [\`\${prefix}value\`]: { type: 'text', nullable: true },
  },
} as const;
`;
      await fs.writeFile(join(schemaDir, 'metadata.ts'), schemaContent);

      const result = await generateTypes({ schemaDir, outputDir });
      expect(result.tablesProcessed).toContain('metadata');

      const content = await fs.readFile(join(outputDir, 'types.ts'), 'utf-8');
      expect(content).toContain('interface Metadata');
      expect(content).toContain('id: number');
      expect(content).toContain('meta_key: string');
      expect(content).toContain('meta_value: string | null');
    });
  });

  describe('property shorthand and external references', () => {
    it('should resolve property shorthand for tableName', async () => {
      // Regex expects tableName: 'value', not shorthand
      const schemaContent = `
const tableName = 'configs';
const columns = {
  id: { type: 'integer', primaryKey: true },
  key: { type: 'text', nullable: false },
};

export const configs = {
  tableName,
  columns,
} as const;
`;
      await fs.writeFile(join(schemaDir, 'configs.ts'), schemaContent);

      const result = await generateTypes({ schemaDir, outputDir });
      expect(result.tablesProcessed).toContain('configs');

      const content = await fs.readFile(join(outputDir, 'types.ts'), 'utf-8');
      expect(content).toContain('interface Config');
      expect(content).toContain('id: number');
      expect(content).toContain('key: string');
    });

    it('should handle columns defined in external const', async () => {
      // Regex cannot resolve columns when shorthand: `columns`
      const schemaContent = `
const columns = {
  id: { type: 'integer', primaryKey: true },
  name: { type: 'text', nullable: false },
} as const;

export const items = {
  tableName: 'items',
  columns,
} as const;
`;
      await fs.writeFile(join(schemaDir, 'items.ts'), schemaContent);

      const result = await generateTypes({ schemaDir, outputDir });
      expect(result.tablesProcessed).toContain('items');

      const content = await fs.readFile(join(outputDir, 'types.ts'), 'utf-8');
      expect(content).toContain('interface Item');
      expect(content).toContain('id: number');
      expect(content).toContain('name: string');
    });

    it('should handle spread operator in schema definitions', async () => {
      // Regex cannot handle spread operator
      const baseContent = `
export const baseUser = {
  tableName: 'base_users',
  columns: {
    id: { type: 'integer', primaryKey: true },
  },
} as const;
`;
      await fs.writeFile(join(schemaDir, 'base.ts'), baseContent);

      const extendedContent = `
import { baseUser } from './base.js';

export const users = {
  ...baseUser,
  tableName: 'users',
  columns: {
    ...baseUser.columns,
    email: { type: 'text', nullable: false },
  },
} as const;
`;
      await fs.writeFile(join(schemaDir, 'users.ts'), extendedContent);

      const result = await generateTypes({ schemaDir, outputDir });
      expect(result.tablesProcessed).toContain('users');

      const content = await fs.readFile(join(outputDir, 'types.ts'), 'utf-8');
      expect(content).toContain('interface User');
      expect(content).toContain('id: number');
      expect(content).toContain('email: string');
    });
  });

  describe('JSDoc comments for documentation', () => {
    it('should preserve JSDoc comments in generated output', async () => {
      // Regex cannot extract and preserve JSDoc comments
      const schemaContent = `
export const users = {
  tableName: 'users',
  columns: {
    /** Primary key identifier */
    id: { type: 'integer', primaryKey: true },
    /** User's display name */
    name: { type: 'text', nullable: false },
  },
} as const;
`;
      await fs.writeFile(join(schemaDir, 'users.ts'), schemaContent);

      await generateTypes({ schemaDir, outputDir });

      const content = await fs.readFile(join(outputDir, 'types.ts'), 'utf-8');
      expect(content).toContain('interface User');
      expect(content).toContain('/** Primary key identifier */');
      expect(content).toContain("/** User's display name */");
    });
  });

  describe('satisfies operator', () => {
    it('should handle satisfies with as const at table level', async () => {
      // Regex expects `} as const;` not `} as const satisfies TableDef;`
      const schemaContent = `
interface TableDef {
  tableName: string;
  columns: Record<string, { type: string; primaryKey?: boolean; nullable?: boolean }>;
}

export const tags = {
  tableName: 'tags',
  columns: {
    id: { type: 'integer', primaryKey: true },
    label: { type: 'text', nullable: false },
  },
} as const satisfies TableDef;
`;
      await fs.writeFile(join(schemaDir, 'tags.ts'), schemaContent);

      const result = await generateTypes({ schemaDir, outputDir });
      expect(result.tablesProcessed).toContain('tags');

      const content = await fs.readFile(join(outputDir, 'types.ts'), 'utf-8');
      expect(content).toContain('interface Tag');
      expect(content).toContain('id: number');
      expect(content).toContain('label: string');
    });
  });

  describe('string literal variations', () => {
    it('should handle string literal with escaped quotes', async () => {
      // Regex pattern ['"]([^'"]+)['"] doesn't handle escaped quotes
      const schemaContent = `
export const quotes = {
  tableName: 'user\\'s_quotes',
  columns: {
    id: { type: 'integer', primaryKey: true },
    text: { type: 'text', nullable: false },
  },
} as const;
`;
      await fs.writeFile(join(schemaDir, 'quotes.ts'), schemaContent);

      const result = await generateTypes({ schemaDir, outputDir });
      expect(result.tablesProcessed).toContain("user's_quotes");

      const content = await fs.readFile(join(outputDir, 'types.ts'), 'utf-8');
      expect(content).toContain('id: number');
    });

    it('should handle double-quoted strings', async () => {
      // Regex uses ['"] but the column regex may have issues
      const schemaContent = `
export const items = {
  tableName: "items",
  columns: {
    id: { type: "integer", primaryKey: true },
    name: { type: "text", nullable: false },
  },
} as const;
`;
      await fs.writeFile(join(schemaDir, 'items.ts'), schemaContent);

      const result = await generateTypes({ schemaDir, outputDir });
      expect(result.tablesProcessed).toContain('items');

      const content = await fs.readFile(join(outputDir, 'types.ts'), 'utf-8');
      expect(content).toContain('interface Item');
      expect(content).toContain('id: number');
      expect(content).toContain('name: string');
    });

    it('should handle backtick template strings for table name', async () => {
      // Regex only handles single and double quotes for tableName
      const schemaContent = `
export const items = {
  tableName: \`items\`,
  columns: {
    id: { type: 'integer', primaryKey: true },
  },
} as const;
`;
      await fs.writeFile(join(schemaDir, 'items.ts'), schemaContent);

      const result = await generateTypes({ schemaDir, outputDir });
      expect(result.tablesProcessed).toContain('items');
    });
  });

  describe('multiple tables in one file', () => {
    it('should handle multiple tables in one file', async () => {
      // Regex global match has issues with multiple tables and column matching
      const schemaContent = `
export const authors = {
  tableName: 'authors',
  columns: {
    id: { type: 'integer', primaryKey: true },
    name: { type: 'text', nullable: false },
  },
} as const;

export const books = {
  tableName: 'books',
  columns: {
    id: { type: 'integer', primaryKey: true },
    title: { type: 'text', nullable: false },
    author_id: { type: 'integer', nullable: false },
  },
} as const;

export const reviews = {
  tableName: 'reviews',
  columns: {
    id: { type: 'integer', primaryKey: true },
    book_id: { type: 'integer', nullable: false },
    rating: { type: 'integer', nullable: false },
    comment: { type: 'text', nullable: true },
  },
} as const;
`;
      await fs.writeFile(join(schemaDir, 'library.ts'), schemaContent);

      const result = await generateTypes({ schemaDir, outputDir });
      expect(result.tablesProcessed).toContain('authors');
      expect(result.tablesProcessed).toContain('books');
      expect(result.tablesProcessed).toContain('reviews');

      const content = await fs.readFile(join(outputDir, 'types.ts'), 'utf-8');
      expect(content).toContain('interface Author');
      expect(content).toContain('interface Book');
      expect(content).toContain('interface Review');
      expect(content).toContain('author_id: number');
      expect(content).toContain('book_id: number');
      expect(content).toContain('comment: string | null');
    });
  });

  describe('column property order variations', () => {
    it('should handle properties in non-standard order', async () => {
      // Regex expects type first, then primaryKey, then nullable
      const schemaContent = `
export const users = {
  tableName: 'users',
  columns: {
    id: { primaryKey: true, type: 'integer' },
    name: { nullable: false, type: 'text' },
    email: { nullable: true, type: 'text' },
  },
} as const;
`;
      await fs.writeFile(join(schemaDir, 'users.ts'), schemaContent);

      const result = await generateTypes({ schemaDir, outputDir });
      expect(result.tablesProcessed).toContain('users');

      const content = await fs.readFile(join(outputDir, 'types.ts'), 'utf-8');
      expect(content).toContain('interface User');
      expect(content).toContain('id: number');
      expect(content).toContain('name: string');
      expect(content).toContain('email: string | null');
    });

    it('should handle all properties in any order', async () => {
      const schemaContent = `
export const data = {
  tableName: 'data',
  columns: {
    id: { nullable: false, primaryKey: true, type: 'integer' },
    count: { type: 'integer', nullable: true, primaryKey: false },
  },
} as const;
`;
      await fs.writeFile(join(schemaDir, 'data.ts'), schemaContent);

      const result = await generateTypes({ schemaDir, outputDir });
      expect(result.tablesProcessed).toContain('data');

      const content = await fs.readFile(join(outputDir, 'types.ts'), 'utf-8');
      expect(content).toContain('id: number');
      expect(content).toContain('count: number | null');
    });
  });
});
