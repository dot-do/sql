/**
 * Tests for shared parser utilities module
 *
 * Verifies the consolidated tokenizer, parser state, and error handling
 * utilities work correctly.
 *
 * Issue: pocs-ftmh - DoSQL parser consolidation
 */

import { describe, it, expect } from 'vitest';
import {
  // Tokenizer
  Tokenizer,
  tokenize,
  createTokenStream,
  isKeyword,
  SQL_KEYWORDS,
  // Errors
  SQLSyntaxError,
  UnexpectedTokenError,
  UnexpectedEOFError,
  createErrorFromException,
  // Parser state
  createParserState,
  parserRemaining,
  parserIsEOF,
  parserGetLocation,
  parserAdvance,
  parserSkipWhitespace,
  parserMatchKeyword,
  parserMatchExact,
  parserParseIdentifier,
  parserParseIdentifierList,
  parserParseStringLiteral,
  parserParseNumericLiteral,
  parserParseParenthesized,
  findMatchingParen,
  // Types
  OPERATOR_PRECEDENCE,
} from '../index.js';

describe('Shared Parser Utilities', () => {
  describe('Tokenizer', () => {
    it('should tokenize simple SELECT statement', () => {
      const tokens = tokenize('SELECT id FROM users');
      expect(tokens.length).toBeGreaterThan(0);
      expect(tokens[0].type).toBe('keyword');
      expect(tokens[0].value).toBe('select');
      expect(tokens[1].type).toBe('identifier');
      expect(tokens[1].value).toBe('id');
    });

    it('should recognize SQL keywords', () => {
      expect(isKeyword('SELECT')).toBe(true);
      expect(isKeyword('from')).toBe(true);
      expect(isKeyword('WHERE')).toBe(true);
      expect(isKeyword('myColumn')).toBe(false);
      expect(isKeyword('foo')).toBe(false);
    });

    it('should tokenize string literals', () => {
      const tokens = tokenize("SELECT 'hello world'");
      const stringToken = tokens.find(t => t.type === 'string');
      expect(stringToken).toBeDefined();
      expect(stringToken?.value).toBe('hello world');
    });

    it('should tokenize numeric literals', () => {
      const tokens = tokenize('SELECT 123, 45.67, -89');
      const numTokens = tokens.filter(t => t.type === 'number');
      expect(numTokens.length).toBe(3); // -89 is operator + number separately
      expect(numTokens[0].value).toBe('123');
      expect(numTokens[1].value).toBe('45.67');
      expect(numTokens[2].value).toBe('89');
    });

    it('should tokenize quoted identifiers', () => {
      const tokens = tokenize('SELECT "my column" FROM `my table`');
      const identifiers = tokens.filter(t => t.type === 'identifier');
      expect(identifiers.length).toBe(2);
      expect(identifiers[0].value).toBe('my column');
      expect(identifiers[1].value).toBe('my table');
    });

    it('should skip comments by default', () => {
      const tokens = tokenize('SELECT -- comment\nid FROM users');
      const commentTokens = tokens.filter(t => t.type === 'comment');
      expect(commentTokens.length).toBe(0);
    });

    it('should include comments when requested', () => {
      const tokens = tokenize('SELECT -- comment\nid', { includeComments: true });
      const commentTokens = tokens.filter(t => t.type === 'comment');
      expect(commentTokens.length).toBe(1);
    });

    it('should track source locations', () => {
      const tokens = tokenize('SELECT\nid');
      expect(tokens[0].location.line).toBe(1);
      expect(tokens[1].location.line).toBe(2);
    });

    it('should tokenize operators', () => {
      const tokens = tokenize('a = b AND c <> d OR e >= f');
      const operators = tokens.filter(t => t.type === 'operator');
      expect(operators.length).toBe(3);
      expect(operators.map(o => o.value)).toContain('=');
      expect(operators.map(o => o.value)).toContain('<>');
      expect(operators.map(o => o.value)).toContain('>=');
    });

    it('should tokenize parameters', () => {
      const tokens = tokenize('SELECT * FROM users WHERE id = ? AND name = :name AND age = $1');
      const params = tokens.filter(t => t.type === 'parameter');
      expect(params.length).toBe(3);
      expect(params[0].value).toBe('?');
      expect(params[1].value).toBe(':name');
      expect(params[2].value).toBe('$1');
    });

    it('should use Tokenizer class directly', () => {
      const tokenizer = new Tokenizer('SELECT * FROM users');
      const tokens = tokenizer.tokenize();
      expect(tokens.length).toBeGreaterThan(0);
      expect(tokens[tokens.length - 1].type).toBe('eof');
    });
  });

  describe('SQL Keywords', () => {
    it('should have comprehensive keyword set', () => {
      expect(SQL_KEYWORDS.has('select')).toBe(true);
      expect(SQL_KEYWORDS.has('insert')).toBe(true);
      expect(SQL_KEYWORDS.has('update')).toBe(true);
      expect(SQL_KEYWORDS.has('delete')).toBe(true);
      expect(SQL_KEYWORDS.has('create')).toBe(true);
      expect(SQL_KEYWORDS.has('table')).toBe(true);
      expect(SQL_KEYWORDS.has('index')).toBe(true);
      expect(SQL_KEYWORDS.has('view')).toBe(true);
      expect(SQL_KEYWORDS.has('case')).toBe(true);
      expect(SQL_KEYWORDS.has('when')).toBe(true);
      expect(SQL_KEYWORDS.has('then')).toBe(true);
      expect(SQL_KEYWORDS.has('else')).toBe(true);
      expect(SQL_KEYWORDS.has('end')).toBe(true);
      expect(SQL_KEYWORDS.has('over')).toBe(true);
      expect(SQL_KEYWORDS.has('partition')).toBe(true);
      expect(SQL_KEYWORDS.has('window')).toBe(true);
    });
  });

  describe('Error Classes', () => {
    it('should create SQLSyntaxError with location', () => {
      const error = new SQLSyntaxError('Unexpected token', { line: 1, column: 5, offset: 4 });
      expect(error.message).toContain('line 1');
      expect(error.message).toContain('column 5');
      expect(error.location?.line).toBe(1);
    });

    it('should create UnexpectedTokenError', () => {
      const error = new UnexpectedTokenError('foo', 'SELECT');
      expect(error.message).toContain("'foo'");
      expect(error.message).toContain('SELECT');
      expect(error.token).toBe('foo');
      expect(error.expected).toBe('SELECT');
    });

    it('should create UnexpectedEOFError', () => {
      const error = new UnexpectedEOFError('closing parenthesis');
      expect(error.message).toContain('end of input');
      expect(error.message).toContain('closing parenthesis');
    });

    it('should format error with context', () => {
      const error = new SQLSyntaxError(
        'Unexpected token',
        { line: 1, column: 8, offset: 7 },
        'SELECT FROM users'
      );
      const formatted = error.format();
      expect(formatted).toContain('SELECT FROM users');
      expect(formatted).toContain('^');
    });

    it('should convert exceptions to SQLSyntaxError', () => {
      const error = createErrorFromException(new Error('test error'), 'SELECT');
      expect(error).toBeInstanceOf(SQLSyntaxError);
      expect(error.message).toContain('test error');
    });
  });

  describe('Parser State', () => {
    it('should create parser state', () => {
      const state = createParserState('SELECT * FROM users');
      expect(parserRemaining(state)).toBe('SELECT * FROM users');
      expect(parserIsEOF(state)).toBe(false);
    });

    it('should advance position', () => {
      const state = createParserState('SELECT');
      const newState = parserAdvance(state, 3);
      expect(parserRemaining(newState)).toBe('ECT');
    });

    it('should skip whitespace', () => {
      const state = createParserState('  SELECT');
      const newState = parserSkipWhitespace(state);
      expect(parserRemaining(newState)).toBe('SELECT');
    });

    it('should get source location', () => {
      const state = createParserState('SELECT');
      const loc = parserGetLocation(state);
      expect(loc.line).toBe(1);
      expect(loc.column).toBe(1);
      expect(loc.offset).toBe(0);
    });

    it('should match keyword', () => {
      const state = createParserState('SELECT * FROM');
      const newState = parserMatchKeyword(state, 'SELECT');
      expect(newState).not.toBeNull();
      expect(parserRemaining(newState!)).toBe(' * FROM');
    });

    it('should not match wrong keyword', () => {
      const state = createParserState('SELECT');
      const newState = parserMatchKeyword(state, 'INSERT');
      expect(newState).toBeNull();
    });

    it('should match exact string', () => {
      const state = createParserState('SELECT');
      const newState = parserMatchExact(state, 'SEL');
      expect(newState).not.toBeNull();
      expect(parserRemaining(newState!)).toBe('ECT');
    });

    it('should parse identifier', () => {
      const state = createParserState('users');
      const result = parserParseIdentifier(state);
      expect(result).not.toBeNull();
      expect(result?.value).toBe('users');
    });

    it('should parse quoted identifier', () => {
      const state = createParserState('"my table"');
      const result = parserParseIdentifier(state);
      expect(result).not.toBeNull();
      expect(result?.value).toBe('my table');
    });

    it('should parse identifier list', () => {
      const state = createParserState('id, name, email');
      const result = parserParseIdentifierList(state);
      expect(result).not.toBeNull();
      expect(result?.values).toEqual(['id', 'name', 'email']);
    });

    it('should parse string literal', () => {
      const state = createParserState("'hello world'");
      const result = parserParseStringLiteral(state);
      expect(result).not.toBeNull();
      expect(result?.value).toBe('hello world');
    });

    it('should parse numeric literal', () => {
      const state = createParserState('42.5');
      const result = parserParseNumericLiteral(state);
      expect(result).not.toBeNull();
      expect(result?.value).toBe(42.5);
    });

    it('should parse parenthesized content', () => {
      const state = createParserState('(a, b, c)');
      const result = parserParseParenthesized(state);
      expect(result).not.toBeNull();
      expect(result?.content).toBe('a, b, c');
    });

    it('should find matching parenthesis', () => {
      const str = '(a + (b * c))';
      expect(findMatchingParen(str, 0)).toBe(12);
      expect(findMatchingParen(str, 5)).toBe(11);
    });
  });

  describe('Operator Precedence', () => {
    it('should have correct precedence values', () => {
      expect(OPERATOR_PRECEDENCE['*']).toBeGreaterThan(OPERATOR_PRECEDENCE['+']);
      expect(OPERATOR_PRECEDENCE['+']).toBeGreaterThan(OPERATOR_PRECEDENCE['AND']);
      expect(OPERATOR_PRECEDENCE['AND']).toBeGreaterThan(OPERATOR_PRECEDENCE['OR']);
      expect(OPERATOR_PRECEDENCE['||']).toBeGreaterThan(OPERATOR_PRECEDENCE['*']);
    });
  });

  describe('Token Stream Helper', () => {
    it('should create filtered token stream', () => {
      const tokens = createTokenStream('SELECT  *  FROM  users');
      // Should not have whitespace tokens
      const whitespaceTokens = tokens.filter(t => t.type === 'whitespace');
      expect(whitespaceTokens.length).toBe(0);
    });
  });
});
