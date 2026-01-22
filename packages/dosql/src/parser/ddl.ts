/**
 * DoSQL DDL (Data Definition Language) Parser
 *
 * Parses DDL statements into an Abstract Syntax Tree.
 * Supports SQLite-compatible DDL statements including:
 * - CREATE TABLE with column definitions and constraints
 * - CREATE INDEX / CREATE UNIQUE INDEX
 * - ALTER TABLE ADD COLUMN / DROP COLUMN / RENAME
 * - DROP TABLE / DROP INDEX / DROP VIEW
 * - CREATE VIEW AS SELECT
 */

import type {
  DDLStatement,
  CreateTableStatement,
  CreateIndexStatement,
  AlterTableStatement,
  DropTableStatement,
  DropIndexStatement,
  CreateViewStatement,
  DropViewStatement,
  ColumnDefinition,
  ColumnDataType,
  ColumnConstraint,
  TableConstraint,
  ConflictClause,
  ReferenceAction,
  DeferrableClause,
  IndexColumn,
  AlterTableOperation,
  ParseResult,
  ParseError,
} from './ddl-types.js';

// =============================================================================
// TOKENIZER
// =============================================================================

/**
 * Token types for the DDL lexer
 */
type TokenType =
  | 'KEYWORD'
  | 'IDENTIFIER'
  | 'STRING'
  | 'NUMBER'
  | 'OPERATOR'
  | 'PUNCTUATION'
  | 'WHITESPACE'
  | 'EOF';

interface Token {
  type: TokenType;
  value: string;
  position: number;
}

/**
 * SQL keywords for DDL parsing
 */
const KEYWORDS = new Set([
  // DDL keywords
  'CREATE',
  'TABLE',
  'INDEX',
  'VIEW',
  'DROP',
  'ALTER',
  'ADD',
  'COLUMN',
  'RENAME',
  'TO',
  'IF',
  'NOT',
  'EXISTS',
  'TEMPORARY',
  'TEMP',
  'UNIQUE',
  'PRIMARY',
  'KEY',
  'FOREIGN',
  'REFERENCES',
  'ON',
  'DELETE',
  'UPDATE',
  'CASCADE',
  'RESTRICT',
  'SET',
  'NULL',
  'DEFAULT',
  'CHECK',
  'CONSTRAINT',
  'AUTOINCREMENT',
  'ASC',
  'DESC',
  'COLLATE',
  'WITHOUT',
  'ROWID',
  'STRICT',
  'AS',
  'SELECT',
  'FROM',
  'WHERE',
  'AND',
  'OR',
  'IN',
  'LIKE',
  'BETWEEN',
  'MATCH',
  'SIMPLE',
  'PARTIAL',
  'FULL',
  'DEFERRABLE',
  'INITIALLY',
  'DEFERRED',
  'IMMEDIATE',
  'GENERATED',
  'ALWAYS',
  'STORED',
  'VIRTUAL',
  'NO',
  'ACTION',
  'REPLACE',
  'ABORT',
  'FAIL',
  'IGNORE',
  'ROLLBACK',
  // Data types
  'INTEGER',
  'INT',
  'SMALLINT',
  'MEDIUMINT',
  'BIGINT',
  'TINYINT',
  'REAL',
  'DOUBLE',
  'PRECISION',
  'FLOAT',
  'NUMERIC',
  'DECIMAL',
  'TEXT',
  'VARCHAR',
  'CHAR',
  'NCHAR',
  'NVARCHAR',
  'CLOB',
  'BLOB',
  'NONE',
  'DATE',
  'DATETIME',
  'TIMESTAMP',
  'TIME',
  'BOOLEAN',
  'BOOL',
  'JSON',
  'JSONB',
  'UUID',
]);

/**
 * Tokenize a DDL statement
 */
function tokenize(sql: string): Token[] {
  const tokens: Token[] = [];
  let pos = 0;

  while (pos < sql.length) {
    const char = sql[pos];

    // Whitespace
    if (/\s/.test(char)) {
      const start = pos;
      while (pos < sql.length && /\s/.test(sql[pos])) {
        pos++;
      }
      tokens.push({ type: 'WHITESPACE', value: sql.slice(start, pos), position: start });
      continue;
    }

    // Single-line comment
    if (char === '-' && sql[pos + 1] === '-') {
      while (pos < sql.length && sql[pos] !== '\n') {
        pos++;
      }
      continue;
    }

    // Multi-line comment
    if (char === '/' && sql[pos + 1] === '*') {
      pos += 2;
      while (pos < sql.length - 1 && !(sql[pos] === '*' && sql[pos + 1] === '/')) {
        pos++;
      }
      pos += 2;
      continue;
    }

    // String literal (single quotes)
    if (char === "'") {
      const start = pos;
      pos++;
      while (pos < sql.length) {
        if (sql[pos] === "'" && sql[pos + 1] === "'") {
          pos += 2; // Escaped quote
        } else if (sql[pos] === "'") {
          pos++;
          break;
        } else {
          pos++;
        }
      }
      tokens.push({ type: 'STRING', value: sql.slice(start, pos), position: start });
      continue;
    }

    // Quoted identifier (double quotes or backticks)
    if (char === '"' || char === '`') {
      const quote = char;
      const start = pos;
      pos++;
      while (pos < sql.length && sql[pos] !== quote) {
        pos++;
      }
      pos++;
      tokens.push({ type: 'IDENTIFIER', value: sql.slice(start, pos), position: start });
      continue;
    }

    // Square bracket quoted identifier (SQL Server style)
    if (char === '[') {
      const start = pos;
      pos++;
      while (pos < sql.length && sql[pos] !== ']') {
        pos++;
      }
      pos++;
      tokens.push({ type: 'IDENTIFIER', value: sql.slice(start, pos), position: start });
      continue;
    }

    // Number
    if (/[0-9]/.test(char) || (char === '.' && /[0-9]/.test(sql[pos + 1] || ''))) {
      const start = pos;
      while (pos < sql.length && /[0-9.]/.test(sql[pos])) {
        pos++;
      }
      // Handle scientific notation
      if ((sql[pos] === 'e' || sql[pos] === 'E') && /[0-9+-]/.test(sql[pos + 1] || '')) {
        pos++;
        if (sql[pos] === '+' || sql[pos] === '-') pos++;
        while (pos < sql.length && /[0-9]/.test(sql[pos])) {
          pos++;
        }
      }
      tokens.push({ type: 'NUMBER', value: sql.slice(start, pos), position: start });
      continue;
    }

    // Identifier or keyword
    if (/[a-zA-Z_]/.test(char)) {
      const start = pos;
      while (pos < sql.length && /[a-zA-Z0-9_]/.test(sql[pos])) {
        pos++;
      }
      const value = sql.slice(start, pos);
      const upperValue = value.toUpperCase();
      const type: TokenType = KEYWORDS.has(upperValue) ? 'KEYWORD' : 'IDENTIFIER';
      tokens.push({ type, value, position: start });
      continue;
    }

    // Punctuation and operators
    if ('(),;.'.includes(char)) {
      tokens.push({ type: 'PUNCTUATION', value: char, position: pos });
      pos++;
      continue;
    }

    // Operators
    if ('+-*/<>=!'.includes(char)) {
      const start = pos;
      pos++;
      // Check for two-character operators
      if (pos < sql.length && '=<>'.includes(sql[pos])) {
        pos++;
      }
      tokens.push({ type: 'OPERATOR', value: sql.slice(start, pos), position: start });
      continue;
    }

    // Unknown character - skip it
    pos++;
  }

  tokens.push({ type: 'EOF', value: '', position: pos });
  return tokens;
}

// =============================================================================
// PARSER
// =============================================================================

/**
 * Parser state
 */
class Parser {
  private tokens: Token[];
  private pos: number = 0;

  constructor(sql: string) {
    this.tokens = tokenize(sql).filter((t) => t.type !== 'WHITESPACE');
  }

  /**
   * Get current token
   */
  private current(): Token {
    return this.tokens[this.pos] || { type: 'EOF', value: '', position: -1 };
  }

  /**
   * Peek at a token ahead
   */
  private peek(offset: number = 1): Token {
    return this.tokens[this.pos + offset] || { type: 'EOF', value: '', position: -1 };
  }

  /**
   * Advance to next token
   */
  private advance(): Token {
    const token = this.current();
    this.pos++;
    return token;
  }

  /**
   * Check if current token matches
   */
  private match(type: TokenType, value?: string): boolean {
    const token = this.current();
    if (token.type !== type) return false;
    if (value !== undefined && token.value.toUpperCase() !== value.toUpperCase()) return false;
    return true;
  }

  /**
   * Expect a token or throw error
   */
  private expect(type: TokenType, value?: string): Token {
    if (!this.match(type, value)) {
      const token = this.current();
      throw new Error(
        `Expected ${value || type} but got ${token.value || token.type} at position ${token.position}`
      );
    }
    return this.advance();
  }

  /**
   * Consume a keyword if it matches
   */
  private consumeKeyword(keyword: string): boolean {
    if (this.match('KEYWORD', keyword)) {
      this.advance();
      return true;
    }
    return false;
  }

  /**
   * Parse an identifier (column name, table name, etc.)
   */
  private parseIdentifier(): string {
    const token = this.current();
    if (token.type === 'IDENTIFIER' || token.type === 'KEYWORD') {
      this.advance();
      // Remove quotes if present
      let name = token.value;
      if ((name.startsWith('"') && name.endsWith('"')) ||
          (name.startsWith('`') && name.endsWith('`')) ||
          (name.startsWith('[') && name.endsWith(']'))) {
        name = name.slice(1, -1);
      }
      return name;
    }
    throw new Error(`Expected identifier but got ${token.value || token.type} at position ${token.position}`);
  }

  /**
   * Parse optional schema-qualified name (schema.name or just name)
   */
  private parseQualifiedName(): { schema?: string; name: string } {
    const first = this.parseIdentifier();
    if (this.match('PUNCTUATION', '.')) {
      this.advance();
      const second = this.parseIdentifier();
      return { schema: first, name: second };
    }
    return { name: first };
  }

  /**
   * Parse a data type
   */
  private parseDataType(): ColumnDataType {
    const token = this.current();
    let typeName = '';

    // Handle DOUBLE PRECISION
    if (this.match('KEYWORD', 'DOUBLE')) {
      this.advance();
      if (this.consumeKeyword('PRECISION')) {
        typeName = 'DOUBLE PRECISION';
      } else {
        typeName = 'DOUBLE';
      }
    } else if (token.type === 'KEYWORD' || token.type === 'IDENTIFIER') {
      typeName = this.advance().value.toUpperCase();
    } else {
      throw new Error(`Expected data type but got ${token.value || token.type}`);
    }

    // Check for precision/scale
    let precision: number | undefined;
    let scale: number | undefined;

    if (this.match('PUNCTUATION', '(')) {
      this.advance();
      const precToken = this.expect('NUMBER');
      precision = parseInt(precToken.value, 10);

      if (this.match('PUNCTUATION', ',')) {
        this.advance();
        const scaleToken = this.expect('NUMBER');
        scale = parseInt(scaleToken.value, 10);
      }

      this.expect('PUNCTUATION', ')');
    }

    return { name: typeName, precision, scale };
  }

  /**
   * Parse a conflict clause
   */
  private parseConflictClause(): ConflictClause | undefined {
    if (!this.consumeKeyword('ON')) return undefined;
    this.expect('KEYWORD', 'CONFLICT');

    const token = this.current();
    if (this.consumeKeyword('ROLLBACK')) return 'ROLLBACK';
    if (this.consumeKeyword('ABORT')) return 'ABORT';
    if (this.consumeKeyword('FAIL')) return 'FAIL';
    if (this.consumeKeyword('IGNORE')) return 'IGNORE';
    if (this.consumeKeyword('REPLACE')) return 'REPLACE';

    throw new Error(`Expected conflict action but got ${token.value}`);
  }

  /**
   * Parse a reference action (ON DELETE/UPDATE ...)
   */
  private parseReferenceAction(): ReferenceAction {
    if (this.consumeKeyword('NO')) {
      this.expect('KEYWORD', 'ACTION');
      return 'NO ACTION';
    }
    if (this.consumeKeyword('RESTRICT')) return 'RESTRICT';
    if (this.consumeKeyword('CASCADE')) return 'CASCADE';
    if (this.consumeKeyword('SET')) {
      if (this.consumeKeyword('NULL')) return 'SET NULL';
      if (this.consumeKeyword('DEFAULT')) return 'SET DEFAULT';
      throw new Error('Expected NULL or DEFAULT after SET');
    }
    throw new Error('Expected reference action');
  }

  /**
   * Parse deferrable clause
   */
  private parseDeferrableClause(): DeferrableClause | undefined {
    if (this.consumeKeyword('NOT')) {
      this.expect('KEYWORD', 'DEFERRABLE');
      return 'NOT DEFERRABLE';
    }
    if (this.consumeKeyword('DEFERRABLE')) {
      if (this.consumeKeyword('INITIALLY')) {
        if (this.consumeKeyword('DEFERRED')) return 'DEFERRABLE INITIALLY DEFERRED';
        if (this.consumeKeyword('IMMEDIATE')) return 'DEFERRABLE INITIALLY IMMEDIATE';
        throw new Error('Expected DEFERRED or IMMEDIATE');
      }
      return 'DEFERRABLE';
    }
    return undefined;
  }

  /**
   * Parse a parenthesized expression (for CHECK, DEFAULT, etc.)
   */
  private parseParenthesizedExpression(): string {
    this.expect('PUNCTUATION', '(');
    let depth = 1;
    const parts: string[] = [];

    while (depth > 0 && this.current().type !== 'EOF') {
      const token = this.current();
      if (token.value === '(') depth++;
      if (token.value === ')') depth--;

      if (depth > 0) {
        parts.push(token.value);
        this.advance();
      }
    }

    this.expect('PUNCTUATION', ')');
    return parts.join(' ').trim();
  }

  /**
   * Parse column constraints
   */
  private parseColumnConstraints(): ColumnConstraint[] {
    const constraints: ColumnConstraint[] = [];

    while (true) {
      let constraintName: string | undefined;

      // Optional CONSTRAINT name
      if (this.consumeKeyword('CONSTRAINT')) {
        constraintName = this.parseIdentifier();
      }

      // PRIMARY KEY
      if (this.consumeKeyword('PRIMARY')) {
        this.expect('KEYWORD', 'KEY');
        let order: 'ASC' | 'DESC' | undefined;
        if (this.consumeKeyword('ASC')) order = 'ASC';
        else if (this.consumeKeyword('DESC')) order = 'DESC';

        const onConflict = this.parseConflictClause();
        const autoincrement = this.consumeKeyword('AUTOINCREMENT');

        constraints.push({
          type: 'PRIMARY KEY',
          name: constraintName,
          order,
          onConflict,
          autoincrement,
        });
        continue;
      }

      // NOT NULL
      if (this.consumeKeyword('NOT')) {
        this.expect('KEYWORD', 'NULL');
        const onConflict = this.parseConflictClause();
        constraints.push({
          type: 'NOT NULL',
          name: constraintName,
          onConflict,
        });
        continue;
      }

      // UNIQUE
      if (this.consumeKeyword('UNIQUE')) {
        const onConflict = this.parseConflictClause();
        constraints.push({
          type: 'UNIQUE',
          name: constraintName,
          onConflict,
        });
        continue;
      }

      // CHECK
      if (this.consumeKeyword('CHECK')) {
        const expression = this.parseParenthesizedExpression();
        constraints.push({
          type: 'CHECK',
          name: constraintName,
          expression,
        });
        continue;
      }

      // DEFAULT
      if (this.consumeKeyword('DEFAULT')) {
        const token = this.current();
        let value: string | number | boolean | null;
        let isExpression = false;

        if (token.type === 'STRING') {
          value = token.value.slice(1, -1).replace(/''/g, "'");
          this.advance();
        } else if (token.type === 'NUMBER') {
          value = parseFloat(token.value);
          this.advance();
        } else if (this.consumeKeyword('NULL')) {
          value = null;
        } else if (this.consumeKeyword('TRUE')) {
          value = true;
        } else if (this.consumeKeyword('FALSE')) {
          value = false;
        } else if (this.match('PUNCTUATION', '(')) {
          value = this.parseParenthesizedExpression();
          isExpression = true;
        } else if (token.type === 'KEYWORD' || token.type === 'IDENTIFIER') {
          // Could be CURRENT_TIMESTAMP, CURRENT_DATE, etc.
          value = this.advance().value;
          isExpression = true;
        } else {
          throw new Error(`Unexpected default value: ${token.value}`);
        }

        constraints.push({
          type: 'DEFAULT',
          value,
          isExpression,
        });
        continue;
      }

      // COLLATE
      if (this.consumeKeyword('COLLATE')) {
        const collation = this.parseIdentifier();
        constraints.push({
          type: 'COLLATE',
          collation,
        });
        continue;
      }

      // REFERENCES
      if (this.consumeKeyword('REFERENCES')) {
        const refTable = this.parseIdentifier();
        let refColumns: string[] | undefined;

        if (this.match('PUNCTUATION', '(')) {
          this.advance();
          refColumns = [this.parseIdentifier()];
          while (this.match('PUNCTUATION', ',')) {
            this.advance();
            refColumns.push(this.parseIdentifier());
          }
          this.expect('PUNCTUATION', ')');
        }

        let onDelete: ReferenceAction | undefined;
        let onUpdate: ReferenceAction | undefined;
        let match: 'SIMPLE' | 'PARTIAL' | 'FULL' | undefined;
        let deferrable: DeferrableClause | undefined;

        while (true) {
          if (this.consumeKeyword('ON')) {
            if (this.consumeKeyword('DELETE')) {
              onDelete = this.parseReferenceAction();
            } else if (this.consumeKeyword('UPDATE')) {
              onUpdate = this.parseReferenceAction();
            } else {
              throw new Error('Expected DELETE or UPDATE after ON');
            }
          } else if (this.consumeKeyword('MATCH')) {
            if (this.consumeKeyword('SIMPLE')) match = 'SIMPLE';
            else if (this.consumeKeyword('PARTIAL')) match = 'PARTIAL';
            else if (this.consumeKeyword('FULL')) match = 'FULL';
            else throw new Error('Expected SIMPLE, PARTIAL, or FULL after MATCH');
          } else {
            const defer = this.parseDeferrableClause();
            if (defer) {
              deferrable = defer;
            } else {
              break;
            }
          }
        }

        constraints.push({
          type: 'REFERENCES',
          name: constraintName,
          table: refTable,
          columns: refColumns,
          onDelete,
          onUpdate,
          match,
          deferrable,
        });
        continue;
      }

      // GENERATED ALWAYS AS
      if (this.consumeKeyword('GENERATED')) {
        this.expect('KEYWORD', 'ALWAYS');
        this.expect('KEYWORD', 'AS');
        const expression = this.parseParenthesizedExpression();
        let storage: 'STORED' | 'VIRTUAL' = 'VIRTUAL';
        if (this.consumeKeyword('STORED')) storage = 'STORED';
        else if (this.consumeKeyword('VIRTUAL')) storage = 'VIRTUAL';

        constraints.push({
          type: 'GENERATED',
          expression,
          storage,
        });
        continue;
      }

      // No more constraints
      break;
    }

    return constraints;
  }

  /**
   * Parse a column definition
   */
  private parseColumnDefinition(): ColumnDefinition {
    const name = this.parseIdentifier();
    const dataType = this.parseDataType();
    const constraints = this.parseColumnConstraints();

    return { name, dataType, constraints };
  }

  /**
   * Parse table constraints
   */
  private parseTableConstraints(): TableConstraint[] {
    const constraints: TableConstraint[] = [];

    while (true) {
      let constraintName: string | undefined;

      // Check for CONSTRAINT keyword
      if (this.consumeKeyword('CONSTRAINT')) {
        constraintName = this.parseIdentifier();
      }

      // PRIMARY KEY
      if (this.consumeKeyword('PRIMARY')) {
        this.expect('KEYWORD', 'KEY');
        this.expect('PUNCTUATION', '(');

        const columns: Array<{ name: string; order?: 'ASC' | 'DESC'; collation?: string }> = [];
        do {
          if (this.match('PUNCTUATION', ',')) this.advance();
          const colName = this.parseIdentifier();
          let collation: string | undefined;
          let order: 'ASC' | 'DESC' | undefined;

          if (this.consumeKeyword('COLLATE')) {
            collation = this.parseIdentifier();
          }
          if (this.consumeKeyword('ASC')) order = 'ASC';
          else if (this.consumeKeyword('DESC')) order = 'DESC';

          columns.push({ name: colName, order, collation });
        } while (this.match('PUNCTUATION', ','));

        this.expect('PUNCTUATION', ')');
        const onConflict = this.parseConflictClause();

        constraints.push({
          type: 'PRIMARY KEY',
          name: constraintName,
          columns,
          onConflict,
        });
        continue;
      }

      // UNIQUE
      if (this.consumeKeyword('UNIQUE')) {
        this.expect('PUNCTUATION', '(');

        const columns: Array<{ name: string; order?: 'ASC' | 'DESC'; collation?: string }> = [];
        do {
          if (this.match('PUNCTUATION', ',')) this.advance();
          const colName = this.parseIdentifier();
          let collation: string | undefined;
          let order: 'ASC' | 'DESC' | undefined;

          if (this.consumeKeyword('COLLATE')) {
            collation = this.parseIdentifier();
          }
          if (this.consumeKeyword('ASC')) order = 'ASC';
          else if (this.consumeKeyword('DESC')) order = 'DESC';

          columns.push({ name: colName, order, collation });
        } while (this.match('PUNCTUATION', ','));

        this.expect('PUNCTUATION', ')');
        const onConflict = this.parseConflictClause();

        constraints.push({
          type: 'UNIQUE',
          name: constraintName,
          columns,
          onConflict,
        });
        continue;
      }

      // FOREIGN KEY
      if (this.consumeKeyword('FOREIGN')) {
        this.expect('KEYWORD', 'KEY');
        this.expect('PUNCTUATION', '(');

        const localColumns: string[] = [];
        do {
          if (this.match('PUNCTUATION', ',')) this.advance();
          localColumns.push(this.parseIdentifier());
        } while (this.match('PUNCTUATION', ','));

        this.expect('PUNCTUATION', ')');
        this.expect('KEYWORD', 'REFERENCES');

        const refTable = this.parseIdentifier();
        this.expect('PUNCTUATION', '(');

        const refColumns: string[] = [];
        do {
          if (this.match('PUNCTUATION', ',')) this.advance();
          refColumns.push(this.parseIdentifier());
        } while (this.match('PUNCTUATION', ','));

        this.expect('PUNCTUATION', ')');

        let onDelete: ReferenceAction | undefined;
        let onUpdate: ReferenceAction | undefined;
        let match: 'SIMPLE' | 'PARTIAL' | 'FULL' | undefined;
        let deferrable: DeferrableClause | undefined;

        while (true) {
          if (this.consumeKeyword('ON')) {
            if (this.consumeKeyword('DELETE')) {
              onDelete = this.parseReferenceAction();
            } else if (this.consumeKeyword('UPDATE')) {
              onUpdate = this.parseReferenceAction();
            } else {
              throw new Error('Expected DELETE or UPDATE after ON');
            }
          } else if (this.consumeKeyword('MATCH')) {
            if (this.consumeKeyword('SIMPLE')) match = 'SIMPLE';
            else if (this.consumeKeyword('PARTIAL')) match = 'PARTIAL';
            else if (this.consumeKeyword('FULL')) match = 'FULL';
            else throw new Error('Expected SIMPLE, PARTIAL, or FULL after MATCH');
          } else {
            const defer = this.parseDeferrableClause();
            if (defer) {
              deferrable = defer;
            } else {
              break;
            }
          }
        }

        constraints.push({
          type: 'FOREIGN KEY',
          name: constraintName,
          columns: localColumns,
          references: { table: refTable, columns: refColumns },
          onDelete,
          onUpdate,
          match,
          deferrable,
        });
        continue;
      }

      // CHECK
      if (this.consumeKeyword('CHECK')) {
        const expression = this.parseParenthesizedExpression();
        constraints.push({
          type: 'CHECK',
          name: constraintName,
          expression,
        });
        continue;
      }

      // No more table constraints
      break;
    }

    return constraints;
  }

  /**
   * Parse CREATE TABLE statement
   */
  parseCreateTable(): CreateTableStatement {
    let temporary = false;
    let ifNotExists = false;

    // TEMP/TEMPORARY
    if (this.consumeKeyword('TEMPORARY') || this.consumeKeyword('TEMP')) {
      temporary = true;
    }

    this.expect('KEYWORD', 'TABLE');

    // IF NOT EXISTS
    if (this.consumeKeyword('IF')) {
      this.expect('KEYWORD', 'NOT');
      this.expect('KEYWORD', 'EXISTS');
      ifNotExists = true;
    }

    const { schema, name } = this.parseQualifiedName();

    // Check for AS SELECT (CREATE TABLE ... AS SELECT)
    if (this.consumeKeyword('AS')) {
      const selectStart = this.current().position;
      // Consume the rest as the SELECT statement
      const parts: string[] = [];
      while (this.current().type !== 'EOF' && this.current().value !== ';') {
        parts.push(this.advance().value);
      }
      const asSelect = parts.join(' ');

      return {
        type: 'CREATE TABLE',
        name,
        schema,
        temporary,
        ifNotExists,
        columns: [],
        constraints: [],
        asSelect,
      };
    }

    this.expect('PUNCTUATION', '(');

    const columns: ColumnDefinition[] = [];
    const tableConstraints: TableConstraint[] = [];

    // Parse columns and constraints
    while (!this.match('PUNCTUATION', ')')) {
      // Check if this is a table constraint
      const token = this.current();
      const nextToken = this.peek();

      if (token.type === 'KEYWORD' &&
          (token.value.toUpperCase() === 'PRIMARY' ||
           token.value.toUpperCase() === 'UNIQUE' ||
           token.value.toUpperCase() === 'FOREIGN' ||
           token.value.toUpperCase() === 'CHECK' ||
           token.value.toUpperCase() === 'CONSTRAINT')) {
        tableConstraints.push(...this.parseTableConstraints());
      } else {
        // Parse column definition
        columns.push(this.parseColumnDefinition());
      }

      // Comma or end of list
      if (this.match('PUNCTUATION', ',')) {
        this.advance();
      } else {
        break;
      }
    }

    this.expect('PUNCTUATION', ')');

    // Table options
    let withoutRowId = false;
    let strict = false;

    while (true) {
      if (this.consumeKeyword('WITHOUT')) {
        this.expect('KEYWORD', 'ROWID');
        withoutRowId = true;
      } else if (this.consumeKeyword('STRICT')) {
        strict = true;
      } else {
        break;
      }
    }

    return {
      type: 'CREATE TABLE',
      name,
      schema,
      temporary,
      ifNotExists,
      columns,
      constraints: tableConstraints,
      withoutRowId,
      strict,
    };
  }

  /**
   * Parse CREATE INDEX statement
   */
  parseCreateIndex(): CreateIndexStatement {
    let unique = false;

    if (this.consumeKeyword('UNIQUE')) {
      unique = true;
    }

    this.expect('KEYWORD', 'INDEX');

    let ifNotExists = false;
    if (this.consumeKeyword('IF')) {
      this.expect('KEYWORD', 'NOT');
      this.expect('KEYWORD', 'EXISTS');
      ifNotExists = true;
    }

    const { schema, name } = this.parseQualifiedName();

    this.expect('KEYWORD', 'ON');
    const table = this.parseIdentifier();

    this.expect('PUNCTUATION', '(');

    const columns: IndexColumn[] = [];
    do {
      if (this.match('PUNCTUATION', ',')) this.advance();

      let colName: string;
      let isExpression = false;

      // Check for expression
      if (this.match('PUNCTUATION', '(')) {
        colName = this.parseParenthesizedExpression();
        isExpression = true;
      } else {
        colName = this.parseIdentifier();
      }

      let collation: string | undefined;
      let order: 'ASC' | 'DESC' | undefined;

      if (this.consumeKeyword('COLLATE')) {
        collation = this.parseIdentifier();
      }
      if (this.consumeKeyword('ASC')) order = 'ASC';
      else if (this.consumeKeyword('DESC')) order = 'DESC';

      columns.push({ name: colName, order, collation, isExpression });
    } while (this.match('PUNCTUATION', ','));

    this.expect('PUNCTUATION', ')');

    // WHERE clause for partial index
    let where: string | undefined;
    if (this.consumeKeyword('WHERE')) {
      const parts: string[] = [];
      while (this.current().type !== 'EOF' && this.current().value !== ';') {
        parts.push(this.advance().value);
      }
      where = parts.join(' ');
    }

    return {
      type: 'CREATE INDEX',
      name,
      schema,
      ifNotExists,
      unique,
      table,
      columns,
      where,
    };
  }

  /**
   * Parse CREATE VIEW statement
   */
  parseCreateView(): CreateViewStatement {
    let temporary = false;

    if (this.consumeKeyword('TEMPORARY') || this.consumeKeyword('TEMP')) {
      temporary = true;
    }

    this.expect('KEYWORD', 'VIEW');

    let ifNotExists = false;
    if (this.consumeKeyword('IF')) {
      this.expect('KEYWORD', 'NOT');
      this.expect('KEYWORD', 'EXISTS');
      ifNotExists = true;
    }

    const { schema, name } = this.parseQualifiedName();

    // Optional column list
    let columns: string[] | undefined;
    if (this.match('PUNCTUATION', '(')) {
      this.advance();
      columns = [];
      do {
        if (this.match('PUNCTUATION', ',')) this.advance();
        columns.push(this.parseIdentifier());
      } while (this.match('PUNCTUATION', ','));
      this.expect('PUNCTUATION', ')');
    }

    this.expect('KEYWORD', 'AS');

    // Capture the SELECT statement
    const parts: string[] = [];
    while (this.current().type !== 'EOF' && this.current().value !== ';') {
      parts.push(this.advance().value);
    }
    const select = parts.join(' ');

    return {
      type: 'CREATE VIEW',
      name,
      schema,
      temporary,
      ifNotExists,
      columns,
      select,
    };
  }

  /**
   * Parse ALTER TABLE statement
   */
  parseAlterTable(): AlterTableStatement {
    this.expect('KEYWORD', 'TABLE');
    const { schema, name } = this.parseQualifiedName();

    const operations: AlterTableOperation[] = [];

    // Parse operations
    do {
      if (this.consumeKeyword('ADD')) {
        // ADD COLUMN is optional in SQLite
        this.consumeKeyword('COLUMN');
        const column = this.parseColumnDefinition();
        operations.push({ operation: 'ADD COLUMN', column });
      } else if (this.consumeKeyword('DROP')) {
        this.consumeKeyword('COLUMN');
        const colName = this.parseIdentifier();
        operations.push({ operation: 'DROP COLUMN', column: colName });
      } else if (this.consumeKeyword('RENAME')) {
        if (this.consumeKeyword('TO')) {
          const newName = this.parseIdentifier();
          operations.push({ operation: 'RENAME TO', newName });
        } else if (this.consumeKeyword('COLUMN')) {
          const oldName = this.parseIdentifier();
          this.expect('KEYWORD', 'TO');
          const newName = this.parseIdentifier();
          operations.push({ operation: 'RENAME COLUMN', oldName, newName });
        } else {
          // RENAME old_name TO new_name (column rename without COLUMN keyword)
          const oldName = this.parseIdentifier();
          this.expect('KEYWORD', 'TO');
          const newName = this.parseIdentifier();
          operations.push({ operation: 'RENAME COLUMN', oldName, newName });
        }
      } else {
        break;
      }
    } while (this.match('PUNCTUATION', ',') && this.advance());

    return {
      type: 'ALTER TABLE',
      name,
      schema,
      operations,
    };
  }

  /**
   * Parse DROP TABLE statement
   */
  parseDropTable(): DropTableStatement {
    this.expect('KEYWORD', 'TABLE');

    let ifExists = false;
    if (this.consumeKeyword('IF')) {
      this.expect('KEYWORD', 'EXISTS');
      ifExists = true;
    }

    const { schema, name } = this.parseQualifiedName();

    return {
      type: 'DROP TABLE',
      name,
      schema,
      ifExists,
    };
  }

  /**
   * Parse DROP INDEX statement
   */
  parseDropIndex(): DropIndexStatement {
    this.expect('KEYWORD', 'INDEX');

    let ifExists = false;
    if (this.consumeKeyword('IF')) {
      this.expect('KEYWORD', 'EXISTS');
      ifExists = true;
    }

    const { schema, name } = this.parseQualifiedName();

    return {
      type: 'DROP INDEX',
      name,
      schema,
      ifExists,
    };
  }

  /**
   * Parse DROP VIEW statement
   */
  parseDropView(): DropViewStatement {
    this.expect('KEYWORD', 'VIEW');

    let ifExists = false;
    if (this.consumeKeyword('IF')) {
      this.expect('KEYWORD', 'EXISTS');
      ifExists = true;
    }

    const { schema, name } = this.parseQualifiedName();

    return {
      type: 'DROP VIEW',
      name,
      schema,
      ifExists,
    };
  }

  /**
   * Parse any DDL statement
   */
  parse(): DDLStatement {
    const token = this.current();

    if (this.consumeKeyword('CREATE')) {
      // Could be CREATE TABLE, CREATE INDEX, CREATE VIEW
      const nextToken = this.current();
      const nextUpper = nextToken.value.toUpperCase();

      // Handle TEMPORARY/TEMP - need to look ahead to determine if it's TABLE or VIEW
      if (nextUpper === 'TEMPORARY' || nextUpper === 'TEMP') {
        const afterTemp = this.peek();
        const afterTempUpper = afterTemp.value.toUpperCase();
        if (afterTempUpper === 'VIEW') {
          return this.parseCreateView();
        }
        // Default to TABLE for TEMPORARY without VIEW
        return this.parseCreateTable();
      }

      if (nextUpper === 'TABLE') {
        return this.parseCreateTable();
      }

      if (nextUpper === 'INDEX' || nextUpper === 'UNIQUE') {
        return this.parseCreateIndex();
      }

      if (nextUpper === 'VIEW') {
        return this.parseCreateView();
      }

      throw new Error(`Unexpected token after CREATE: ${nextToken.value}`);
    }

    if (this.consumeKeyword('DROP')) {
      const nextToken = this.current();

      if (nextToken.value.toUpperCase() === 'TABLE') {
        return this.parseDropTable();
      }

      if (nextToken.value.toUpperCase() === 'INDEX') {
        return this.parseDropIndex();
      }

      if (nextToken.value.toUpperCase() === 'VIEW') {
        return this.parseDropView();
      }

      throw new Error(`Unexpected token after DROP: ${nextToken.value}`);
    }

    if (this.consumeKeyword('ALTER')) {
      return this.parseAlterTable();
    }

    throw new Error(`Unknown DDL statement starting with: ${token.value}`);
  }
}

// =============================================================================
// PUBLIC API
// =============================================================================

/**
 * Parse a DDL statement
 *
 * @param sql - The DDL SQL string to parse
 * @returns ParseResult with the parsed statement or an error
 */
export function parseDDL(sql: string): ParseResult {
  try {
    const parser = new Parser(sql);
    const statement = parser.parse();
    return { success: true, statement };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return { success: false, error: message };
  }
}

/**
 * Parse a CREATE TABLE statement
 *
 * @param sql - The CREATE TABLE SQL string
 * @returns ParseResult with the parsed CreateTableStatement or an error
 */
export function parseCreateTable(sql: string): ParseResult<CreateTableStatement> {
  const result = parseDDL(sql);
  if (!result.success) return result;
  if (result.statement.type !== 'CREATE TABLE') {
    return { success: false, error: 'Expected CREATE TABLE statement' };
  }
  return { success: true, statement: result.statement };
}

/**
 * Parse a CREATE INDEX statement
 *
 * @param sql - The CREATE INDEX SQL string
 * @returns ParseResult with the parsed CreateIndexStatement or an error
 */
export function parseCreateIndex(sql: string): ParseResult<CreateIndexStatement> {
  const result = parseDDL(sql);
  if (!result.success) return result;
  if (result.statement.type !== 'CREATE INDEX') {
    return { success: false, error: 'Expected CREATE INDEX statement' };
  }
  return { success: true, statement: result.statement };
}

/**
 * Parse an ALTER TABLE statement
 *
 * @param sql - The ALTER TABLE SQL string
 * @returns ParseResult with the parsed AlterTableStatement or an error
 */
export function parseAlterTable(sql: string): ParseResult<AlterTableStatement> {
  const result = parseDDL(sql);
  if (!result.success) return result;
  if (result.statement.type !== 'ALTER TABLE') {
    return { success: false, error: 'Expected ALTER TABLE statement' };
  }
  return { success: true, statement: result.statement };
}

/**
 * Parse a DROP TABLE statement
 *
 * @param sql - The DROP TABLE SQL string
 * @returns ParseResult with the parsed DropTableStatement or an error
 */
export function parseDropTable(sql: string): ParseResult<DropTableStatement> {
  const result = parseDDL(sql);
  if (!result.success) return result;
  if (result.statement.type !== 'DROP TABLE') {
    return { success: false, error: 'Expected DROP TABLE statement' };
  }
  return { success: true, statement: result.statement };
}

/**
 * Parse a DROP INDEX statement
 *
 * @param sql - The DROP INDEX SQL string
 * @returns ParseResult with the parsed DropIndexStatement or an error
 */
export function parseDropIndex(sql: string): ParseResult<DropIndexStatement> {
  const result = parseDDL(sql);
  if (!result.success) return result;
  if (result.statement.type !== 'DROP INDEX') {
    return { success: false, error: 'Expected DROP INDEX statement' };
  }
  return { success: true, statement: result.statement };
}

/**
 * Parse a CREATE VIEW statement
 *
 * @param sql - The CREATE VIEW SQL string
 * @returns ParseResult with the parsed CreateViewStatement or an error
 */
export function parseCreateView(sql: string): ParseResult<CreateViewStatement> {
  const result = parseDDL(sql);
  if (!result.success) return result;
  if (result.statement.type !== 'CREATE VIEW') {
    return { success: false, error: 'Expected CREATE VIEW statement' };
  }
  return { success: true, statement: result.statement };
}

/**
 * Parse a DROP VIEW statement
 *
 * @param sql - The DROP VIEW SQL string
 * @returns ParseResult with the parsed DropViewStatement or an error
 */
export function parseDropView(sql: string): ParseResult<DropViewStatement> {
  const result = parseDDL(sql);
  if (!result.success) return result;
  if (result.statement.type !== 'DROP VIEW') {
    return { success: false, error: 'Expected DROP VIEW statement' };
  }
  return { success: true, statement: result.statement };
}

// Re-export types for convenience
export * from './ddl-types.js';
