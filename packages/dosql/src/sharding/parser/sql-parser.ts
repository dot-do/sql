/**
 * SQL Parser - State-aware SQL parsing for query routing
 *
 * This parser uses tokenization to properly handle:
 * - SQL comments (block and line)
 * - String literals with keywords inside
 * - Escaped quotes
 * - Unicode characters
 * - Table aliases
 *
 * @packageDocumentation
 */

import type { AggregateOp, QueryOperation } from '../types.js';
import type {
  ParsedQuery,
  TableReference,
  ColumnReference,
  WhereCondition,
  WhereClause,
} from './types.js';
import {
  tokenizeSQL,
  stripComments,
  findKeywordIndex,
  getTokensBetweenKeywords,
  extractConditionsFromTokens,
  type SQLToken,
} from './tokenizer.js';

// =============================================================================
// SQL PARSER CLASS
// =============================================================================

/**
 * SQL parser using state-aware tokenization
 *
 * This parser uses the tokenizer to properly handle:
 * - SQL comments (block and line)
 * - String literals with keywords inside
 * - Escaped quotes
 * - Unicode characters
 * - Table aliases
 */
export class SQLParser {
  /**
   * Parse a SQL query
   */
  parse(sql: string): ParsedQuery {
    // Tokenize and strip comments
    const tokens = stripComments(tokenizeSQL(sql));

    if (tokens.length === 0) {
      throw new Error('Empty SQL query');
    }

    const operation = this.detectOperation(tokens);

    switch (operation) {
      case 'SELECT':
        return this.parseSelect(tokens, sql);
      case 'INSERT':
        return this.parseInsert(tokens);
      case 'UPDATE':
        return this.parseUpdate(tokens);
      case 'DELETE':
        return this.parseDelete(tokens);
      default:
        throw new Error(`Unsupported operation: ${operation}`);
    }
  }

  private detectOperation(tokens: SQLToken[]): QueryOperation {
    const firstKeyword = tokens.find(t => t.type === 'keyword');
    if (!firstKeyword) {
      throw new Error('Unknown SQL operation');
    }

    switch (firstKeyword.value) {
      case 'SELECT':
        return 'SELECT';
      case 'INSERT':
        return 'INSERT';
      case 'UPDATE':
        return 'UPDATE';
      case 'DELETE':
        return 'DELETE';
      default:
        throw new Error('Unknown SQL operation');
    }
  }

  private parseSelect(tokens: SQLToken[], _originalSql: string): ParsedQuery {
    const parsed: ParsedQuery = {
      operation: 'SELECT',
      tables: [],
    };

    // Check for DISTINCT
    const selectIndex = findKeywordIndex(tokens, 'SELECT');
    if (selectIndex !== -1 && selectIndex + 1 < tokens.length) {
      parsed.distinct = tokens[selectIndex + 1].type === 'keyword' && tokens[selectIndex + 1].value === 'DISTINCT';
    }

    // Extract columns (between SELECT and FROM)
    const columnsTokens = getTokensBetweenKeywords(tokens, 'SELECT', ['FROM']);
    if (columnsTokens.length > 0) {
      // Skip DISTINCT if present
      const startIdx = columnsTokens[0]?.value === 'DISTINCT' ? 1 : 0;
      const columnTokens = columnsTokens.slice(startIdx);
      parsed.columns = this.parseColumnList(columnTokens);
      parsed.aggregates = this.extractAggregates(columnTokens);
    }

    // Extract tables (after FROM)
    const fromIndex = findKeywordIndex(tokens, 'FROM');
    if (fromIndex !== -1 && fromIndex + 1 < tokens.length) {
      const tableResult = this.parseTableReference(tokens, fromIndex + 1);
      if (tableResult) {
        parsed.tables.push(tableResult);
      }
    }

    // Extract WHERE clause using tokenizer
    const { conditions, operator } = extractConditionsFromTokens(tokens);
    if (conditions.length > 0) {
      parsed.where = {
        conditions: conditions.map(c => ({
          column: c.column,
          operator: c.operator as WhereCondition['operator'],
          value: c.value,
          values: c.values,
          minValue: c.minValue,
          maxValue: c.maxValue,
        })),
        operator,
      };
    }

    // Extract GROUP BY
    const groupByTokens = getTokensBetweenKeywords(tokens, 'BY', ['HAVING', 'ORDER', 'LIMIT', 'OFFSET', 'UNION', 'INTERSECT', 'EXCEPT']);
    // Only use if preceded by GROUP
    const groupIndex = findKeywordIndex(tokens, 'GROUP');
    if (groupIndex !== -1) {
      const byIndex = findKeywordIndex(tokens, 'BY');
      if (byIndex === groupIndex + 1 && groupByTokens.length > 0) {
        parsed.groupBy = this.parseGroupByColumns(groupByTokens);
      }
    }

    // Extract ORDER BY
    const orderIndex = findKeywordIndex(tokens, 'ORDER');
    if (orderIndex !== -1) {
      const orderByTokens = getTokensBetweenKeywords(tokens, 'BY', ['LIMIT', 'OFFSET', 'UNION', 'INTERSECT', 'EXCEPT']);
      // Find the BY that follows ORDER
      let orderByIndex = -1;
      for (let i = orderIndex + 1; i < tokens.length; i++) {
        if (tokens[i].type === 'keyword' && tokens[i].value === 'BY') {
          orderByIndex = i;
          break;
        }
      }
      if (orderByIndex !== -1) {
        const orderTokens = this.getTokensUntilKeywords(tokens, orderByIndex + 1, ['LIMIT', 'OFFSET', 'UNION', 'INTERSECT', 'EXCEPT']);
        parsed.orderBy = this.parseOrderBy(orderTokens);
      }
    }

    // Extract LIMIT
    const limitIndex = findKeywordIndex(tokens, 'LIMIT');
    if (limitIndex !== -1 && limitIndex + 1 < tokens.length) {
      const limitToken = tokens[limitIndex + 1];
      if (limitToken.type === 'number') {
        parsed.limit = parseInt(limitToken.value, 10);
      }
    }

    // Extract OFFSET
    const offsetIndex = findKeywordIndex(tokens, 'OFFSET');
    if (offsetIndex !== -1 && offsetIndex + 1 < tokens.length) {
      const offsetToken = tokens[offsetIndex + 1];
      if (offsetToken.type === 'number') {
        parsed.offset = parseInt(offsetToken.value, 10);
      }
    }

    return parsed;
  }

  private parseTableReference(tokens: SQLToken[], startIndex: number): TableReference | null {
    if (startIndex >= tokens.length) return null;

    const nameToken = tokens[startIndex];
    if (nameToken.type !== 'identifier' && nameToken.type !== 'keyword') {
      // Handle quoted identifier
      if (nameToken.type === 'string' || nameToken.original?.startsWith('"')) {
        return { name: nameToken.value };
      }
      return null;
    }

    const table: TableReference = { name: nameToken.value };

    // Check for alias
    if (startIndex + 1 < tokens.length) {
      const nextToken = tokens[startIndex + 1];
      if (nextToken.type === 'keyword' && nextToken.value === 'AS') {
        if (startIndex + 2 < tokens.length && tokens[startIndex + 2].type === 'identifier') {
          table.alias = tokens[startIndex + 2].value;
        }
      } else if (nextToken.type === 'identifier') {
        // Implicit alias (no AS keyword)
        table.alias = nextToken.value;
      }
    }

    return table;
  }

  private parseColumnList(tokens: SQLToken[]): ColumnReference[] {
    // Check for *
    if (tokens.length === 1 && tokens[0].value === '*') {
      return [{ name: '*' }];
    }

    const result: ColumnReference[] = [];
    const columns = this.splitTokensByComma(tokens);

    for (const colTokens of columns) {
      const colRef = this.parseColumnRef(colTokens);
      if (colRef) {
        result.push(colRef);
      }
    }

    return result;
  }

  private parseColumnRef(tokens: SQLToken[]): ColumnReference | null {
    if (tokens.length === 0) return null;

    // Check for aggregate function
    if (tokens[0].type === 'keyword' && ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'].includes(tokens[0].value)) {
      const aggName = tokens[0].value;
      // Find the column inside parentheses
      let parenDepth = 0;
      let columnName = '';
      let alias: string | undefined;

      for (let i = 1; i < tokens.length; i++) {
        const t = tokens[i];
        if (t.value === '(') {
          parenDepth++;
        } else if (t.value === ')') {
          parenDepth--;
        } else if (parenDepth === 1 && (t.type === 'identifier' || t.value === '*')) {
          columnName = t.value;
        } else if (parenDepth === 0 && t.type === 'keyword' && t.value === 'AS') {
          if (i + 1 < tokens.length && tokens[i + 1].type === 'identifier') {
            alias = tokens[i + 1].value;
          }
        }
      }

      return {
        name: columnName || '*',
        alias,
        aggregate: aggName,
      };
    }

    // Check for table.column
    if (tokens.length >= 3 && tokens[1].value === '.') {
      const table = tokens[0].value;
      const name = tokens[2].value;
      let alias: string | undefined;

      // Check for AS alias
      for (let i = 3; i < tokens.length; i++) {
        if (tokens[i].type === 'keyword' && tokens[i].value === 'AS' && i + 1 < tokens.length) {
          alias = tokens[i + 1].value;
          break;
        }
      }

      return { name, table, alias };
    }

    // Check for column AS alias
    if (tokens.length >= 3) {
      for (let i = 0; i < tokens.length - 1; i++) {
        if (tokens[i].type === 'keyword' && tokens[i].value === 'AS') {
          const name = tokens.slice(0, i).map(t => t.value).join('');
          const alias = tokens[i + 1]?.value;
          return { name, alias };
        }
      }
    }

    // Simple column name
    if (tokens.length === 1 && (tokens[0].type === 'identifier' || tokens[0].value === '*')) {
      return { name: tokens[0].value };
    }

    // Fallback: join all tokens as name
    return { name: tokens.map(t => t.value).join('') };
  }

  private extractAggregates(tokens: SQLToken[]): AggregateOp[] {
    const aggregates: AggregateOp[] = [];
    const aggKeywords = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'];

    for (let i = 0; i < tokens.length; i++) {
      const t = tokens[i];
      if (t.type === 'keyword' && aggKeywords.includes(t.value)) {
        // Find the column inside parentheses
        let parenDepth = 0;
        let columnName = '';
        let alias: string | undefined;

        for (let j = i + 1; j < tokens.length; j++) {
          const next = tokens[j];
          if (next.value === '(') {
            parenDepth++;
          } else if (next.value === ')') {
            parenDepth--;
            if (parenDepth === 0) {
              // Check for AS alias after closing paren
              if (j + 2 < tokens.length && tokens[j + 1].value === 'AS') {
                alias = tokens[j + 2].value;
              }
              break;
            }
          } else if (parenDepth === 1 && (next.type === 'identifier' || next.value === '*')) {
            columnName = next.value;
          }
        }

        aggregates.push({
          function: t.value as AggregateOp['function'],
          column: columnName || '*',
          alias: alias || t.value.toLowerCase(),
          isPartial: t.value === 'AVG',
        });
      }
    }

    return aggregates;
  }

  private parseGroupByColumns(tokens: SQLToken[]): string[] {
    const columns: string[] = [];
    const groups = this.splitTokensByComma(tokens);

    for (const group of groups) {
      if (group.length > 0) {
        // Handle table.column
        if (group.length >= 3 && group[1].value === '.') {
          columns.push(`${group[0].value}.${group[2].value}`);
        } else if (group[0].type === 'identifier') {
          columns.push(group[0].value);
        }
      }
    }

    return columns;
  }

  private parseOrderBy(tokens: SQLToken[]): ParsedQuery['orderBy'] {
    const columns: NonNullable<ParsedQuery['orderBy']> = [];
    const orders = this.splitTokensByComma(tokens);

    for (const order of orders) {
      if (order.length === 0) continue;

      let column = '';
      let direction: 'ASC' | 'DESC' = 'ASC';
      let nulls: 'FIRST' | 'LAST' | undefined;

      // Handle table.column
      if (order.length >= 3 && order[1].value === '.') {
        column = `${order[0].value}.${order[2].value}`;
      } else if (order[0].type === 'identifier') {
        column = order[0].value;
      }

      // Find direction
      for (const t of order) {
        if (t.type === 'keyword') {
          if (t.value === 'ASC') direction = 'ASC';
          else if (t.value === 'DESC') direction = 'DESC';
          else if (t.value === 'FIRST') nulls = 'FIRST';
          else if (t.value === 'LAST') nulls = 'LAST';
        }
      }

      if (column) {
        columns.push({ column, direction, nulls });
      }
    }

    return columns;
  }

  private parseInsert(tokens: SQLToken[]): ParsedQuery {
    const intoIndex = findKeywordIndex(tokens, 'INTO');
    if (intoIndex === -1 || intoIndex + 1 >= tokens.length) {
      throw new Error('Invalid INSERT statement');
    }

    const tableToken = tokens[intoIndex + 1];
    if (tableToken.type !== 'identifier') {
      throw new Error('Invalid INSERT statement');
    }

    const parsed: ParsedQuery = {
      operation: 'INSERT',
      tables: [{ name: tableToken.value }],
    };

    // Extract column names if present (between table name and VALUES)
    const valuesIndex = findKeywordIndex(tokens, 'VALUES');
    if (valuesIndex !== -1 && intoIndex + 2 < valuesIndex) {
      // Look for opening parenthesis after table name
      if (tokens[intoIndex + 2]?.value === '(') {
        const columnTokens: SQLToken[] = [];
        let depth = 1;
        for (let i = intoIndex + 3; i < valuesIndex && depth > 0; i++) {
          if (tokens[i].value === '(') depth++;
          else if (tokens[i].value === ')') depth--;
          else if (depth === 1 && tokens[i].type === 'identifier') {
            columnTokens.push(tokens[i]);
          }
        }
        parsed.columns = columnTokens.map(t => ({ name: t.value }));
      }
    }

    return parsed;
  }

  private parseUpdate(tokens: SQLToken[]): ParsedQuery {
    const updateIndex = findKeywordIndex(tokens, 'UPDATE');
    if (updateIndex === -1 || updateIndex + 1 >= tokens.length) {
      throw new Error('Invalid UPDATE statement');
    }

    const tableToken = tokens[updateIndex + 1];
    if (tableToken.type !== 'identifier') {
      throw new Error('Invalid UPDATE statement');
    }

    const parsed: ParsedQuery = {
      operation: 'UPDATE',
      tables: [{ name: tableToken.value }],
    };

    // Extract WHERE clause
    const { conditions, operator } = extractConditionsFromTokens(tokens);
    if (conditions.length > 0) {
      parsed.where = {
        conditions: conditions.map(c => ({
          column: c.column,
          operator: c.operator as WhereCondition['operator'],
          value: c.value,
          values: c.values,
          minValue: c.minValue,
          maxValue: c.maxValue,
        })),
        operator,
      };
    }

    return parsed;
  }

  private parseDelete(tokens: SQLToken[]): ParsedQuery {
    const fromIndex = findKeywordIndex(tokens, 'FROM');
    if (fromIndex === -1 || fromIndex + 1 >= tokens.length) {
      throw new Error('Invalid DELETE statement');
    }

    const tableToken = tokens[fromIndex + 1];
    if (tableToken.type !== 'identifier') {
      throw new Error('Invalid DELETE statement');
    }

    const parsed: ParsedQuery = {
      operation: 'DELETE',
      tables: [{ name: tableToken.value }],
    };

    // Extract WHERE clause
    const { conditions, operator } = extractConditionsFromTokens(tokens);
    if (conditions.length > 0) {
      parsed.where = {
        conditions: conditions.map(c => ({
          column: c.column,
          operator: c.operator as WhereCondition['operator'],
          value: c.value,
          values: c.values,
          minValue: c.minValue,
          maxValue: c.maxValue,
        })),
        operator,
      };
    }

    return parsed;
  }

  private splitTokensByComma(tokens: SQLToken[]): SQLToken[][] {
    const result: SQLToken[][] = [];
    let current: SQLToken[] = [];
    let depth = 0;

    for (const token of tokens) {
      if (token.value === '(') {
        depth++;
        current.push(token);
      } else if (token.value === ')') {
        depth--;
        current.push(token);
      } else if (token.value === ',' && depth === 0) {
        if (current.length > 0) {
          result.push(current);
          current = [];
        }
      } else {
        current.push(token);
      }
    }

    if (current.length > 0) {
      result.push(current);
    }

    return result;
  }

  private getTokensUntilKeywords(tokens: SQLToken[], startIndex: number, endKeywords: string[]): SQLToken[] {
    const result: SQLToken[] = [];
    const endKeywordSet = new Set(endKeywords);

    for (let i = startIndex; i < tokens.length; i++) {
      if (tokens[i].type === 'keyword' && endKeywordSet.has(tokens[i].value)) {
        break;
      }
      result.push(tokens[i]);
    }

    return result;
  }
}
