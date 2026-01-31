/**
 * DoSQL CASE Expression Parser
 *
 * Extends the SQL parser to support comprehensive CASE expressions:
 * - Simple CASE: CASE x WHEN y THEN z END
 * - Searched CASE: CASE WHEN condition THEN value END
 * - COALESCE, NULLIF, IIF shorthands
 *
 * Follows SQL standard CASE expression semantics.
 */

// =============================================================================
// AST TYPES
// =============================================================================

/**
 * Location information for error reporting
 */
export interface SourceLocation {
  line: number;
  column: number;
  offset: number;
}

/**
 * Base parsed expression types
 */
export type ParsedExpr =
  | ColumnExpr
  | LiteralExpr
  | BinaryExpr
  | UnaryExpr
  | FunctionExpr
  | AggregateExpr
  | BetweenExpr
  | InExpr
  | IsNullExpr
  | CaseExpression
  | SubqueryExpr
  | ExistsExpr
  | StarExpr;

export interface ColumnExpr {
  type: 'column';
  name: string;
  table?: string;
  location?: SourceLocation;
}

export interface LiteralExpr {
  type: 'literal';
  value: string | number | boolean | null;
  location?: SourceLocation;
}

export interface BinaryExpr {
  type: 'binary';
  op: string;
  left: ParsedExpr;
  right: ParsedExpr;
  location?: SourceLocation;
}

export interface UnaryExpr {
  type: 'unary';
  op: string;
  operand: ParsedExpr;
  location?: SourceLocation;
}

export interface FunctionExpr {
  type: 'function';
  name: string;
  args: ParsedExpr[];
  location?: SourceLocation;
}

export interface AggregateExpr {
  type: 'aggregate';
  name: string;
  arg: ParsedExpr | '*';
  distinct?: boolean;
  location?: SourceLocation;
}

export interface BetweenExpr {
  type: 'between';
  expr: ParsedExpr;
  low: ParsedExpr;
  high: ParsedExpr;
  location?: SourceLocation;
}

export interface InExpr {
  type: 'in';
  expr: ParsedExpr;
  values: ParsedExpr[] | SubqueryExpr;
  not?: boolean;
  location?: SourceLocation;
}

export interface IsNullExpr {
  type: 'isNull';
  expr: ParsedExpr;
  isNot: boolean;
  location?: SourceLocation;
}

export interface SubqueryExpr {
  type: 'subquery';
  subqueryType: 'scalar' | 'in' | 'exists' | 'any' | 'all';
  query: ParsedSelect;
  location?: SourceLocation;
}

export interface ExistsExpr {
  type: 'exists';
  query: ParsedSelect;
  location?: SourceLocation;
}

export interface StarExpr {
  type: 'star';
  table?: string;
  location?: SourceLocation;
}

// =============================================================================
// CASE EXPRESSION TYPES
// =============================================================================

/**
 * Base CASE expression interface
 */
export interface BaseCaseExpression {
  type: 'case';
  whenClauses: WhenClause[];
  elseClause?: ParsedExpr;
  location?: SourceLocation;
  /** Inferred result type */
  inferredType?: 'string' | 'number' | 'boolean' | 'date' | 'unknown';
  /** Whether result can be NULL */
  nullable?: boolean;
}

/**
 * Simple CASE expression: CASE x WHEN y THEN z END
 */
export interface SimpleCaseExpression extends BaseCaseExpression {
  caseType: 'simple';
  /** The operand being compared */
  operand: ParsedExpr;
  whenClauses: SimpleWhenClause[];
}

/**
 * Searched CASE expression: CASE WHEN condition THEN value END
 */
export interface SearchedCaseExpression extends BaseCaseExpression {
  caseType: 'searched';
  operand?: undefined;
  whenClauses: SearchedWhenClause[];
}

/**
 * Union of CASE expression types
 */
export type CaseExpression = SimpleCaseExpression | SearchedCaseExpression;

/**
 * Base WHEN clause
 */
export interface WhenClause {
  result: ParsedExpr;
}

/**
 * Simple WHEN clause: WHEN value THEN result
 */
export interface SimpleWhenClause extends WhenClause {
  value: ParsedExpr;
}

/**
 * Searched WHEN clause: WHEN condition THEN result
 */
export interface SearchedWhenClause extends WhenClause {
  condition: ParsedExpr;
}

// =============================================================================
// PARSED SELECT TYPES
// =============================================================================

export interface ParsedColumn {
  expr: ParsedExpr;
  alias?: string;
  location?: SourceLocation;
}

export interface ParsedFrom {
  type: 'table' | 'derived';
  table?: string;
  alias?: string;
  query?: ParsedSelect;
  location?: SourceLocation;
}

export interface ParsedJoin {
  type: 'inner' | 'left' | 'right' | 'full' | 'cross';
  table: ParsedFrom;
  alias?: string;
  on?: ParsedExpr;
  location?: SourceLocation;
}

export interface ParsedOrderBy {
  expr: ParsedExpr;
  direction: 'asc' | 'desc';
  nullsFirst?: boolean;
  location?: SourceLocation;
}

export interface ParsedSelect {
  type: 'select';
  columns: ParsedColumn[];
  from: ParsedFrom;
  joins?: ParsedJoin[];
  where?: ParsedExpr;
  groupBy?: ParsedExpr[];
  having?: ParsedExpr;
  orderBy?: ParsedOrderBy[];
  limit?: number;
  offset?: number;
  distinct?: boolean;
  location?: SourceLocation;
}

// =============================================================================
// TOKENIZER
// =============================================================================

type TokenType =
  | 'keyword'
  | 'identifier'
  | 'number'
  | 'string'
  | 'operator'
  | 'punctuation'
  | 'parameter'
  | 'eof';

interface Token {
  type: TokenType;
  value: string;
  location: SourceLocation;
}

const KEYWORDS = new Set([
  'select', 'from', 'where', 'and', 'or', 'not', 'in', 'between', 'like', 'ilike',
  'is', 'null', 'true', 'false', 'as', 'on', 'join', 'inner', 'left', 'right',
  'full', 'outer', 'cross', 'group', 'by', 'having', 'order', 'asc', 'desc',
  'limit', 'offset', 'distinct', 'all', 'union', 'intersect', 'except',
  'count', 'sum', 'avg', 'min', 'max', 'nulls', 'first', 'last',
  'exists', 'any', 'some', 'case', 'when', 'then', 'else', 'end',
  'with', 'recursive', 'coalesce', 'nullif', 'iif', 'if',
]);

class Tokenizer {
  private sql: string;
  private pos = 0;
  private line = 1;
  private column = 1;
  private tokens: Token[] = [];

  constructor(sql: string) {
    this.sql = sql;
  }

  tokenize(): Token[] {
    while (this.pos < this.sql.length) {
      this.skipWhitespaceAndComments();
      if (this.pos >= this.sql.length) break;
      this.readToken();
    }

    this.tokens.push({
      type: 'eof',
      value: '',
      location: { line: this.line, column: this.column, offset: this.pos },
    });

    return this.tokens;
  }

  private location(): SourceLocation {
    return { line: this.line, column: this.column, offset: this.pos };
  }

  private advance(): string {
    const char = this.sql[this.pos];
    this.pos++;
    if (char === '\n') {
      this.line++;
      this.column = 1;
    } else {
      this.column++;
    }
    return char;
  }

  private peek(offset = 0): string {
    return this.sql[this.pos + offset] || '';
  }

  private skipWhitespaceAndComments(): void {
    while (this.pos < this.sql.length) {
      const char = this.peek();

      if (/\s/.test(char)) {
        this.advance();
        continue;
      }

      if (char === '-' && this.peek(1) === '-') {
        while (this.pos < this.sql.length && this.peek() !== '\n') {
          this.advance();
        }
        continue;
      }

      if (char === '/' && this.peek(1) === '*') {
        this.advance();
        this.advance();
        while (this.pos < this.sql.length - 1) {
          if (this.peek() === '*' && this.peek(1) === '/') {
            this.advance();
            this.advance();
            break;
          }
          this.advance();
        }
        continue;
      }

      break;
    }
  }

  private readToken(): void {
    const loc = this.location();
    const char = this.peek();

    if (char === "'" || char === '"') {
      this.readString(loc);
      return;
    }

    if (/[0-9]/.test(char) || (char === '.' && /[0-9]/.test(this.peek(1)))) {
      this.readNumber(loc);
      return;
    }

    if (char === '$' || char === '?') {
      this.readParameter(loc);
      return;
    }

    if (/[a-zA-Z_]/.test(char)) {
      this.readIdentifier(loc);
      return;
    }

    const twoChar = this.peek() + this.peek(1);
    if (['<=', '>=', '<>', '!=', '||'].includes(twoChar)) {
      this.advance();
      this.advance();
      this.tokens.push({ type: 'operator', value: twoChar, location: loc });
      return;
    }

    if ('=<>+-*/%'.includes(char)) {
      this.tokens.push({ type: 'operator', value: this.advance(), location: loc });
      return;
    }

    if ('(),;.'.includes(char)) {
      this.tokens.push({ type: 'punctuation', value: this.advance(), location: loc });
      return;
    }

    throw new SyntaxError(`Unexpected character '${char}'`, loc);
  }

  private readString(loc: SourceLocation): void {
    const quote = this.advance();
    let value = '';

    while (this.pos < this.sql.length) {
      const char = this.peek();

      if (char === quote) {
        if (this.peek(1) === quote) {
          this.advance();
          value += this.advance();
        } else {
          this.advance();
          break;
        }
      } else if (char === '\\') {
        this.advance();
        const escaped = this.advance();
        switch (escaped) {
          case 'n': value += '\n'; break;
          case 't': value += '\t'; break;
          case 'r': value += '\r'; break;
          default: value += escaped;
        }
      } else {
        value += this.advance();
      }
    }

    this.tokens.push({ type: 'string', value, location: loc });
  }

  private readNumber(loc: SourceLocation): void {
    let value = '';

    while (/[0-9.]/.test(this.peek())) {
      value += this.advance();
    }

    if (this.peek().toLowerCase() === 'e') {
      value += this.advance();
      if (this.peek() === '+' || this.peek() === '-') {
        value += this.advance();
      }
      while (/[0-9]/.test(this.peek())) {
        value += this.advance();
      }
    }

    this.tokens.push({ type: 'number', value, location: loc });
  }

  private readParameter(loc: SourceLocation): void {
    let value = this.advance();

    while (/[0-9]/.test(this.peek())) {
      value += this.advance();
    }

    this.tokens.push({ type: 'parameter', value, location: loc });
  }

  private readIdentifier(loc: SourceLocation): void {
    let value = '';

    while (/[a-zA-Z0-9_]/.test(this.peek())) {
      value += this.advance();
    }

    const lower = value.toLowerCase();
    if (KEYWORDS.has(lower)) {
      this.tokens.push({ type: 'keyword', value: lower, location: loc });
    } else {
      this.tokens.push({ type: 'identifier', value, location: loc });
    }
  }
}

// =============================================================================
// SYNTAX ERROR
// =============================================================================

export class SyntaxError extends Error {
  location?: SourceLocation;

  constructor(message: string, location?: SourceLocation) {
    const locStr = location ? ` at line ${location.line}, column ${location.column}` : '';
    super(message + locStr);
    this.name = 'SyntaxError';
    this.location = location;
  }
}

// =============================================================================
// CASE EXPRESSION PARSER
// =============================================================================

export class CaseExpressionParser {
  private tokens: Token[] = [];
  private pos = 0;

  /**
   * Parse SQL string into AST with CASE expression support
   */
  parse(sql: string): ParsedSelect {
    const tokenizer = new Tokenizer(sql);
    this.tokens = tokenizer.tokenize();
    this.pos = 0;

    const result = this.parseSelect();

    if (!this.isAtEnd()) {
      throw new SyntaxError(`Unexpected token '${this.current().value}'`, this.current().location);
    }

    return result;
  }

  // ===========================================================================
  // TOKEN UTILITIES
  // ===========================================================================

  private current(): Token {
    return this.tokens[this.pos] || { type: 'eof', value: '', location: { line: 0, column: 0, offset: 0 } };
  }

  private peek(offset = 0): Token {
    return this.tokens[this.pos + offset] || { type: 'eof', value: '', location: { line: 0, column: 0, offset: 0 } };
  }

  private advance(): Token {
    return this.tokens[this.pos++] || { type: 'eof', value: '', location: { line: 0, column: 0, offset: 0 } };
  }

  private isAtEnd(): boolean {
    return this.current().type === 'eof';
  }

  private match(type: TokenType, value?: string): boolean {
    const token = this.current();
    return token.type === type && (value === undefined || token.value.toLowerCase() === value.toLowerCase());
  }

  private matchKeyword(...keywords: string[]): boolean {
    const token = this.current();
    return token.type === 'keyword' && keywords.includes(token.value.toLowerCase());
  }

  private expect(type: TokenType, value?: string): Token {
    const token = this.current();
    if (token.type !== type || (value !== undefined && token.value.toLowerCase() !== value.toLowerCase())) {
      throw new SyntaxError(
        `Expected ${type}${value ? ` '${value}'` : ''}, got ${token.type} '${token.value}'`,
        token.location
      );
    }
    return this.advance();
  }

  // ===========================================================================
  // SELECT PARSING
  // ===========================================================================

  private parseSelect(): ParsedSelect {
    const loc = this.current().location;
    this.expect('keyword', 'select');

    const distinct = this.matchKeyword('distinct');
    if (distinct) this.advance();

    const columns = this.parseSelectList();

    this.expect('keyword', 'from');
    const from = this.parseFromClause();

    const joins = this.parseJoins();

    let where: ParsedExpr | undefined;
    if (this.matchKeyword('where')) {
      this.advance();
      where = this.parseExpression();
    }

    let groupBy: ParsedExpr[] | undefined;
    if (this.matchKeyword('group')) {
      this.advance();
      this.expect('keyword', 'by');
      groupBy = [this.parseExpression()];
      while (this.match('punctuation', ',')) {
        this.advance();
        groupBy.push(this.parseExpression());
      }
    }

    let having: ParsedExpr | undefined;
    if (this.matchKeyword('having')) {
      this.advance();
      having = this.parseExpression();
    }

    let orderBy: ParsedOrderBy[] | undefined;
    if (this.matchKeyword('order')) {
      this.advance();
      this.expect('keyword', 'by');
      orderBy = [this.parseOrderByItem()];
      while (this.match('punctuation', ',')) {
        this.advance();
        orderBy.push(this.parseOrderByItem());
      }
    }

    let limit: number | undefined;
    if (this.matchKeyword('limit')) {
      this.advance();
      limit = parseInt(this.expect('number').value, 10);
    }

    let offset: number | undefined;
    if (this.matchKeyword('offset')) {
      this.advance();
      offset = parseInt(this.expect('number').value, 10);
    }

    return {
      type: 'select',
      columns,
      from,
      joins: joins.length > 0 ? joins : undefined,
      where,
      groupBy,
      having,
      orderBy,
      limit,
      offset,
      distinct,
      location: loc,
    };
  }

  // ===========================================================================
  // SELECT LIST PARSING
  // ===========================================================================

  private parseSelectList(): ParsedColumn[] {
    const columns: ParsedColumn[] = [];

    do {
      columns.push(this.parseSelectColumn());
    } while (this.match('punctuation', ',') && this.advance());

    return columns;
  }

  private parseSelectColumn(): ParsedColumn {
    const loc = this.current().location;

    if (this.match('operator', '*')) {
      this.advance();
      return { expr: { type: 'star', location: loc }, location: loc };
    }

    const expr = this.parseExpression();

    let alias: string | undefined;
    if (this.matchKeyword('as')) {
      this.advance();
      alias = this.expect('identifier').value;
    } else if (this.match('identifier') && !this.isClauseKeyword()) {
      alias = this.advance().value;
    }

    return { expr, alias, location: loc };
  }

  private isClauseKeyword(): boolean {
    return this.matchKeyword(
      'from', 'where', 'group', 'having', 'order', 'limit', 'offset',
      'union', 'intersect', 'except', 'join', 'inner', 'left', 'right', 'full', 'cross'
    );
  }

  // ===========================================================================
  // FROM CLAUSE PARSING
  // ===========================================================================

  private parseFromClause(): ParsedFrom {
    const loc = this.current().location;

    if (this.match('punctuation', '(')) {
      return this.parseDerivedTable(loc);
    }

    const table = this.expect('identifier').value;
    let alias: string | undefined;

    if (this.matchKeyword('as')) {
      this.advance();
      alias = this.expect('identifier').value;
    } else if (this.match('identifier') && !this.isClauseKeyword() && !this.isJoinKeyword()) {
      alias = this.advance().value;
    }

    return { type: 'table', table, alias, location: loc };
  }

  private parseDerivedTable(loc: SourceLocation): ParsedFrom {
    this.expect('punctuation', '(');

    if (!this.matchKeyword('select')) {
      throw new SyntaxError('Expected SELECT in derived table', this.current().location);
    }

    const query = this.parseSelect();
    this.expect('punctuation', ')');

    let alias: string;
    if (this.matchKeyword('as')) {
      this.advance();
      alias = this.expect('identifier').value;
    } else if (this.match('identifier')) {
      alias = this.advance().value;
    } else {
      throw new SyntaxError('Derived table requires an alias', this.current().location);
    }

    return { type: 'derived', query, alias, location: loc };
  }

  private isJoinKeyword(): boolean {
    return this.matchKeyword('join', 'inner', 'left', 'right', 'full', 'cross');
  }

  // ===========================================================================
  // JOIN PARSING
  // ===========================================================================

  private parseJoins(): ParsedJoin[] {
    const joins: ParsedJoin[] = [];

    while (this.isJoinKeyword()) {
      joins.push(this.parseJoin());
    }

    return joins;
  }

  private parseJoin(): ParsedJoin {
    const loc = this.current().location;
    let type: ParsedJoin['type'] = 'inner';

    if (this.matchKeyword('cross')) {
      type = 'cross';
      this.advance();
    } else if (this.matchKeyword('left')) {
      type = 'left';
      this.advance();
      this.matchKeyword('outer') && this.advance();
    } else if (this.matchKeyword('right')) {
      type = 'right';
      this.advance();
      this.matchKeyword('outer') && this.advance();
    } else if (this.matchKeyword('full')) {
      type = 'full';
      this.advance();
      this.matchKeyword('outer') && this.advance();
    } else if (this.matchKeyword('inner')) {
      this.advance();
    }

    this.expect('keyword', 'join');

    let table: ParsedFrom;
    if (this.match('punctuation', '(')) {
      table = this.parseDerivedTable(loc);
    } else {
      const tableName = this.expect('identifier').value;
      let alias: string | undefined;

      if (this.matchKeyword('as')) {
        this.advance();
        alias = this.expect('identifier').value;
      } else if (this.match('identifier') && !this.matchKeyword('on') && !this.isClauseKeyword()) {
        alias = this.advance().value;
      }

      table = { type: 'table', table: tableName, alias, location: loc };
    }

    let on: ParsedExpr | undefined;
    if (this.matchKeyword('on')) {
      this.advance();
      on = this.parseExpression();
    }

    return { type, table, on, location: loc };
  }

  // ===========================================================================
  // ORDER BY PARSING
  // ===========================================================================

  private parseOrderByItem(): ParsedOrderBy {
    const loc = this.current().location;
    const expr = this.parseExpression();

    let direction: 'asc' | 'desc' = 'asc';
    if (this.matchKeyword('asc')) {
      this.advance();
    } else if (this.matchKeyword('desc')) {
      direction = 'desc';
      this.advance();
    }

    let nullsFirst: boolean | undefined;
    if (this.matchKeyword('nulls')) {
      this.advance();
      if (this.matchKeyword('first')) {
        nullsFirst = true;
        this.advance();
      } else if (this.matchKeyword('last')) {
        nullsFirst = false;
        this.advance();
      }
    }

    return { expr, direction, nullsFirst, location: loc };
  }

  // ===========================================================================
  // EXPRESSION PARSING
  // ===========================================================================

  private parseExpression(): ParsedExpr {
    return this.parseOr();
  }

  private parseOr(): ParsedExpr {
    let left = this.parseAnd();

    while (this.matchKeyword('or')) {
      const loc = this.current().location;
      this.advance();
      const right = this.parseAnd();
      left = { type: 'binary', op: 'or', left, right, location: loc };
    }

    return left;
  }

  private parseAnd(): ParsedExpr {
    let left = this.parseNot();

    while (this.matchKeyword('and')) {
      const loc = this.current().location;
      this.advance();
      const right = this.parseNot();
      left = { type: 'binary', op: 'and', left, right, location: loc };
    }

    return left;
  }

  private parseNot(): ParsedExpr {
    if (this.matchKeyword('not')) {
      const loc = this.current().location;
      this.advance();
      return { type: 'unary', op: 'not', operand: this.parseNot(), location: loc };
    }
    return this.parseComparison();
  }

  private parseComparison(): ParsedExpr {
    const loc = this.current().location;
    let left = this.parseAddSub();

    // IS NULL / IS NOT NULL
    if (this.matchKeyword('is')) {
      this.advance();
      const isNot = this.matchKeyword('not');
      if (isNot) this.advance();
      this.expect('keyword', 'null');
      return { type: 'isNull', expr: left, isNot, location: loc };
    }

    // BETWEEN
    if (this.matchKeyword('between')) {
      this.advance();
      const low = this.parseAddSub();
      this.expect('keyword', 'and');
      const high = this.parseAddSub();
      return { type: 'between', expr: left, low, high, location: loc };
    }

    // IN / NOT IN
    if (this.matchKeyword('in') || (this.matchKeyword('not') && this.peek(1).value === 'in')) {
      const not = this.matchKeyword('not');
      if (not) this.advance();
      this.advance(); // IN
      return this.parseInExpression(left, not, loc);
    }

    // NOT BETWEEN
    if (this.matchKeyword('not') && this.peek(1).value === 'between') {
      this.advance();
      this.advance();
      const low = this.parseAddSub();
      this.expect('keyword', 'and');
      const high = this.parseAddSub();
      return { type: 'unary', op: 'not', operand: { type: 'between', expr: left, low, high, location: loc }, location: loc };
    }

    // Comparison operators
    const compOps: Record<string, string> = {
      '=': 'eq', '<>': 'ne', '!=': 'ne', '<': 'lt', '<=': 'le', '>': 'gt', '>=': 'ge',
    };

    if (this.match('operator') && compOps[this.current().value]) {
      const op = compOps[this.advance().value];
      const right = this.parseAddSub();
      return { type: 'binary', op, left, right, location: loc };
    }

    // LIKE / ILIKE
    if (this.matchKeyword('like', 'ilike')) {
      const op = this.advance().value;
      const right = this.parseAddSub();
      return { type: 'binary', op, left, right, location: loc };
    }

    return left;
  }

  private parseInExpression(left: ParsedExpr, not: boolean, loc: SourceLocation): ParsedExpr {
    this.expect('punctuation', '(');

    // Check if it's a subquery
    if (this.matchKeyword('select')) {
      const query = this.parseSelect();
      this.expect('punctuation', ')');

      const subquery: SubqueryExpr = { type: 'subquery', subqueryType: 'in', query, location: loc };
      const inExpr: InExpr = { type: 'in', expr: left, values: subquery, location: loc };
      return not
        ? { type: 'unary', op: 'not', operand: inExpr, location: loc }
        : inExpr;
    }

    // It's a value list
    if (this.match('punctuation', ')')) {
      throw new SyntaxError('Expected at least one value in IN list', this.current().location);
    }

    const values: ParsedExpr[] = [];
    values.push(this.parseExpression());
    while (this.match('punctuation', ',')) {
      this.advance();
      values.push(this.parseExpression());
    }

    this.expect('punctuation', ')');

    const inExpr: InExpr = { type: 'in', expr: left, values, location: loc };
    return not
      ? { type: 'unary', op: 'not', operand: inExpr, location: loc }
      : inExpr;
  }

  private parseAddSub(): ParsedExpr {
    let left = this.parseMulDiv();

    while (this.match('operator', '+') || this.match('operator', '-')) {
      const loc = this.current().location;
      const op = this.advance().value === '+' ? 'add' : 'sub';
      const right = this.parseMulDiv();
      left = { type: 'binary', op, left, right, location: loc };
    }

    return left;
  }

  private parseMulDiv(): ParsedExpr {
    let left = this.parseUnary();

    while (this.match('operator', '*') || this.match('operator', '/') || this.match('operator', '%')) {
      const loc = this.current().location;
      const opChar = this.advance().value;
      const op = opChar === '*' ? 'mul' : opChar === '/' ? 'div' : 'mod';
      const right = this.parseUnary();
      left = { type: 'binary', op, left, right, location: loc };
    }

    return left;
  }

  private parseUnary(): ParsedExpr {
    if (this.match('operator', '-')) {
      const loc = this.current().location;
      this.advance();
      return { type: 'unary', op: 'neg', operand: this.parseUnary(), location: loc };
    }
    return this.parsePrimary();
  }

  private parsePrimary(): ParsedExpr {
    const loc = this.current().location;

    // Parenthesized expression or subquery
    if (this.match('punctuation', '(')) {
      this.advance();

      if (this.matchKeyword('select')) {
        const query = this.parseSelect();
        this.expect('punctuation', ')');
        return { type: 'subquery', subqueryType: 'scalar', query, location: loc };
      }

      const expr = this.parseExpression();
      this.expect('punctuation', ')');
      return expr;
    }

    // EXISTS
    if (this.matchKeyword('exists')) {
      this.advance();
      this.expect('punctuation', '(');
      const query = this.parseSelect();
      this.expect('punctuation', ')');
      return { type: 'exists', query, location: loc };
    }

    // CASE expression
    if (this.matchKeyword('case')) {
      return this.parseCaseExpression(loc);
    }

    // Aggregate functions
    if (this.matchKeyword('count', 'sum', 'avg', 'min', 'max')) {
      return this.parseAggregate(loc);
    }

    // COALESCE, NULLIF, IIF - special functions
    if (this.matchKeyword('coalesce', 'nullif', 'iif', 'if')) {
      return this.parseSpecialFunction(loc);
    }

    // Function call or column
    if (this.match('identifier')) {
      const name = this.advance().value;

      // Function call
      if (this.match('punctuation', '(')) {
        this.advance();
        const args: ParsedExpr[] = [];
        if (!this.match('punctuation', ')')) {
          args.push(this.parseExpression());
          while (this.match('punctuation', ',')) {
            this.advance();
            args.push(this.parseExpression());
          }
        }
        this.expect('punctuation', ')');
        return { type: 'function', name, args, location: loc };
      }

      // Table.column
      if (this.match('punctuation', '.')) {
        this.advance();

        if (this.match('operator', '*')) {
          this.advance();
          return { type: 'star', table: name, location: loc };
        }

        const column = this.expect('identifier').value;
        return { type: 'column', name: column, table: name, location: loc };
      }

      return { type: 'column', name, location: loc };
    }

    // Number
    if (this.match('number')) {
      const value = this.advance().value;
      const numValue = value.includes('.') ? parseFloat(value) : parseInt(value, 10);
      return { type: 'literal', value: numValue, location: loc };
    }

    // String
    if (this.match('string')) {
      return { type: 'literal', value: this.advance().value, location: loc };
    }

    // Boolean literals
    if (this.matchKeyword('true')) {
      this.advance();
      return { type: 'literal', value: true, location: loc };
    }
    if (this.matchKeyword('false')) {
      this.advance();
      return { type: 'literal', value: false, location: loc };
    }

    // NULL
    if (this.matchKeyword('null')) {
      this.advance();
      return { type: 'literal', value: null, location: loc };
    }

    // Parameter
    if (this.match('parameter')) {
      const param = this.advance().value;
      return { type: 'column', name: param, location: loc };
    }

    // Star
    if (this.match('operator', '*')) {
      this.advance();
      return { type: 'star', location: loc };
    }

    throw new SyntaxError(`Unexpected token '${this.current().value}'`, this.current().location);
  }

  // ===========================================================================
  // CASE EXPRESSION PARSING
  // ===========================================================================

  private parseCaseExpression(loc: SourceLocation): CaseExpression {
    this.advance(); // CASE

    // Check if this is simple or searched CASE
    if (this.matchKeyword('when')) {
      // Searched CASE: CASE WHEN ... THEN ... END
      return this.parseSearchedCase(loc);
    } else if (this.matchKeyword('else')) {
      // CASE with only ELSE is invalid
      throw new SyntaxError('Expected WHEN after CASE', this.current().location);
    } else {
      // Simple CASE: CASE operand WHEN ... THEN ... END
      return this.parseSimpleCase(loc);
    }
  }

  private parseSimpleCase(loc: SourceLocation): SimpleCaseExpression {
    const operand = this.parseExpression();

    // Must have at least one WHEN clause
    if (!this.matchKeyword('when')) {
      throw new SyntaxError('Expected WHEN after CASE operand', this.current().location);
    }

    const whenClauses: SimpleWhenClause[] = [];
    while (this.matchKeyword('when')) {
      this.advance(); // WHEN
      const value = this.parseExpression();

      if (!this.matchKeyword('then')) {
        throw new SyntaxError('Expected THEN after WHEN value', this.current().location);
      }
      this.advance(); // THEN

      const result = this.parseExpression();
      whenClauses.push({ value, result });
    }

    let elseClause: ParsedExpr | undefined;
    if (this.matchKeyword('else')) {
      this.advance();
      elseClause = this.parseExpression();
    }

    if (!this.matchKeyword('end')) {
      throw new SyntaxError('Expected END to close CASE expression', this.current().location);
    }
    this.advance(); // END

    const caseExpr: SimpleCaseExpression = {
      type: 'case',
      caseType: 'simple',
      operand,
      whenClauses,
      elseClause,
      location: loc,
      nullable: !elseClause,
    };

    // Infer type from results
    this.inferCaseType(caseExpr);

    return caseExpr;
  }

  private parseSearchedCase(loc: SourceLocation): SearchedCaseExpression {
    const whenClauses: SearchedWhenClause[] = [];

    while (this.matchKeyword('when')) {
      this.advance(); // WHEN
      const condition = this.parseExpression();

      if (!this.matchKeyword('then')) {
        throw new SyntaxError('Expected THEN after WHEN condition', this.current().location);
      }
      this.advance(); // THEN

      const result = this.parseExpression();
      whenClauses.push({ condition, result });
    }

    if (whenClauses.length === 0) {
      throw new SyntaxError('Expected at least one WHEN clause in CASE', this.current().location);
    }

    let elseClause: ParsedExpr | undefined;
    if (this.matchKeyword('else')) {
      this.advance();
      elseClause = this.parseExpression();
    }

    if (!this.matchKeyword('end')) {
      throw new SyntaxError('Expected END to close CASE expression', this.current().location);
    }
    this.advance(); // END

    const caseExpr: SearchedCaseExpression = {
      type: 'case',
      caseType: 'searched',
      whenClauses,
      elseClause,
      location: loc,
      nullable: !elseClause || (elseClause.type === 'literal' && elseClause.value === null),
    };

    // Infer type from results
    this.inferCaseType(caseExpr);

    return caseExpr;
  }

  private inferCaseType(caseExpr: CaseExpression): void {
    const results: ParsedExpr[] = caseExpr.whenClauses.map(w => w.result);
    if (caseExpr.elseClause) {
      results.push(caseExpr.elseClause);
    }

    // Check if all results are literals of the same type
    const literalTypes = new Set<string>();
    for (const result of results) {
      if (result.type === 'literal') {
        if (result.value === null) {
          caseExpr.nullable = true;
        } else {
          literalTypes.add(typeof result.value);
        }
      }
    }

    if (literalTypes.size === 1) {
      const type = Array.from(literalTypes)[0];
      caseExpr.inferredType = type as any;
    }
  }

  // ===========================================================================
  // SPECIAL FUNCTIONS (COALESCE, NULLIF, IIF)
  // ===========================================================================

  private parseSpecialFunction(loc: SourceLocation): FunctionExpr {
    const name = this.advance().value.toLowerCase();
    this.expect('punctuation', '(');

    const args: ParsedExpr[] = [];
    if (!this.match('punctuation', ')')) {
      args.push(this.parseExpression());
      while (this.match('punctuation', ',')) {
        this.advance();
        args.push(this.parseExpression());
      }
    }

    this.expect('punctuation', ')');

    // Validate argument count for special functions
    if (name === 'nullif' && args.length !== 2) {
      throw new SyntaxError('NULLIF requires exactly 2 arguments', loc);
    }
    if ((name === 'iif' || name === 'if') && args.length !== 3) {
      throw new SyntaxError('IIF requires exactly 3 arguments', loc);
    }
    if (name === 'coalesce' && args.length < 1) {
      throw new SyntaxError('COALESCE requires at least 1 argument', loc);
    }

    return { type: 'function', name, args, location: loc };
  }

  // ===========================================================================
  // AGGREGATE PARSING
  // ===========================================================================

  private parseAggregate(loc: SourceLocation): AggregateExpr {
    const name = this.advance().value;
    this.expect('punctuation', '(');

    let distinct = false;
    if (this.matchKeyword('distinct')) {
      distinct = true;
      this.advance();
    }

    let arg: ParsedExpr | '*';
    if (this.match('operator', '*')) {
      this.advance();
      arg = '*';
    } else {
      arg = this.parseExpression();
    }

    this.expect('punctuation', ')');

    return { type: 'aggregate', name, arg, distinct, location: loc };
  }
}

// =============================================================================
// CASE EXPRESSION EVALUATION
// =============================================================================

type SqlValue = string | number | bigint | boolean | Date | null | Uint8Array;
type Row = Record<string, SqlValue>;

/**
 * Evaluate a CASE expression against a row
 */
export function evaluateCaseExpression(caseExpr: CaseExpression, row: Row): SqlValue {
  if (caseExpr.caseType === 'simple') {
    return evaluateSimpleCase(caseExpr, row);
  } else {
    return evaluateSearchedCase(caseExpr, row);
  }
}

function evaluateSimpleCase(caseExpr: SimpleCaseExpression, row: Row): SqlValue {
  const operandValue = evaluateExpr(caseExpr.operand, row);

  for (const when of caseExpr.whenClauses) {
    const whenValue = evaluateExpr(when.value, row);
    if (operandValue === whenValue) {
      return evaluateExpr(when.result, row);
    }
  }

  if (caseExpr.elseClause) {
    return evaluateExpr(caseExpr.elseClause, row);
  }

  return null;
}

function evaluateSearchedCase(caseExpr: SearchedCaseExpression, row: Row): SqlValue {
  for (const when of caseExpr.whenClauses) {
    const conditionValue = evaluateExpr(when.condition, row);
    if (isTruthy(conditionValue)) {
      return evaluateExpr(when.result, row);
    }
  }

  if (caseExpr.elseClause) {
    return evaluateExpr(caseExpr.elseClause, row);
  }

  return null;
}

function evaluateExpr(expr: ParsedExpr, row: Row): SqlValue {
  switch (expr.type) {
    case 'column':
      if (expr.table) {
        const fullKey = `${expr.table}.${expr.name}`;
        if (fullKey in row) return row[fullKey];
      }
      return row[expr.name] ?? null;

    case 'literal':
      return expr.value;

    case 'binary': {
      const left = evaluateExpr(expr.left, row);
      const right = evaluateExpr(expr.right, row);
      return evaluateBinaryOp(expr.op, left, right);
    }

    case 'unary': {
      const operand = evaluateExpr(expr.operand, row);
      return evaluateUnaryOp(expr.op, operand);
    }

    case 'function':
      return evaluateFunction(expr.name, expr.args.map(a => evaluateExpr(a, row)));

    case 'case':
      return evaluateCaseExpression(expr, row);

    case 'isNull':
      const val = evaluateExpr(expr.expr, row);
      return expr.isNot ? val !== null : val === null;

    case 'between': {
      const value = evaluateExpr(expr.expr, row);
      const low = evaluateExpr(expr.low, row);
      const high = evaluateExpr(expr.high, row);
      if (value === null || low === null || high === null) return false;
      return value >= low && value <= high;
    }

    case 'in': {
      const value = evaluateExpr(expr.expr, row);
      if (Array.isArray(expr.values)) {
        // Per SQL standard: When the right operand is an empty set, IN returns false
        // regardless of the left operand (even if NULL)
        if (expr.values.length === 0) {
          return false;
        }
        if (value === null) return false;
        for (const v of expr.values) {
          const evalV = evaluateExpr(v, row);
          if (evalV === value) return true;
        }
        return false;
      }
      return false; // Subquery - would need context
    }

    default:
      return null;
  }
}

function evaluateBinaryOp(op: string, left: SqlValue, right: SqlValue): SqlValue {
  switch (op) {
    case 'add':
      if (typeof left === 'number' && typeof right === 'number') return left + right;
      return null;
    case 'sub':
      if (typeof left === 'number' && typeof right === 'number') return left - right;
      return null;
    case 'mul':
      if (typeof left === 'number' && typeof right === 'number') return left * right;
      return null;
    case 'div':
      if (typeof left === 'number' && typeof right === 'number' && right !== 0) return left / right;
      return null;
    case 'mod':
      if (typeof left === 'number' && typeof right === 'number' && right !== 0) return left % right;
      return null;
    case 'eq': return left === right;
    case 'ne': return left !== right;
    case 'lt': return left !== null && right !== null && left < right;
    case 'le': return left !== null && right !== null && left <= right;
    case 'gt': return left !== null && right !== null && left > right;
    case 'ge': return left !== null && right !== null && left >= right;
    case 'and': return isTruthy(left) && isTruthy(right);
    case 'or': return isTruthy(left) || isTruthy(right);
    case 'like':
      if (typeof left === 'string' && typeof right === 'string') {
        const pattern = right
          .replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
          .replace(/%/g, '.*')
          .replace(/_/g, '.');
        return new RegExp(`^${pattern}$`, 'i').test(left);
      }
      return false;
    default:
      return null;
  }
}

function evaluateUnaryOp(op: string, operand: SqlValue): SqlValue {
  switch (op) {
    case 'not': return !isTruthy(operand);
    case 'neg':
      if (typeof operand === 'number') return -operand;
      if (typeof operand === 'bigint') return -operand;
      return null;
    default:
      return null;
  }
}

function evaluateFunction(name: string, args: SqlValue[]): SqlValue {
  const lowerName = name.toLowerCase();

  switch (lowerName) {
    case 'coalesce':
      for (const arg of args) {
        if (arg !== null) return arg;
      }
      return null;

    case 'nullif':
      return args[0] === args[1] ? null : args[0];

    case 'iif':
    case 'if':
      return isTruthy(args[0]) ? args[1] : args[2];

    case 'upper':
      return typeof args[0] === 'string' ? args[0].toUpperCase() : null;

    case 'lower':
      return typeof args[0] === 'string' ? args[0].toLowerCase() : null;

    case 'concat':
      return args.map(a => String(a ?? '')).join('');

    case 'length':
    case 'len':
      return typeof args[0] === 'string' ? args[0].length : null;

    case 'abs':
      if (typeof args[0] === 'number') return Math.abs(args[0]);
      return null;

    case 'round':
      if (typeof args[0] === 'number') {
        const precision = typeof args[1] === 'number' ? args[1] : 0;
        const factor = Math.pow(10, precision);
        return Math.round(args[0] * factor) / factor;
      }
      return null;

    default:
      throw new Error(`Unknown function: ${name}`);
  }
}

function isTruthy(value: SqlValue): boolean {
  if (value === null) return false;
  if (value === false) return false;
  if (value === 0) return false;
  if (value === '') return false;
  return true;
}

// =============================================================================
// CONVENIENCE FUNCTION
// =============================================================================

/**
 * Parse SQL string with CASE expression support
 */
export function parseCaseExpression(sql: string): ParsedSelect {
  return new CaseExpressionParser().parse(sql);
}
