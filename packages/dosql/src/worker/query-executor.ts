/**
 * Query Executor
 *
 * Extracted from database.ts to reduce the size of the main DoSQLDatabase class.
 * Handles SQL routing and execution of SELECT, INSERT, UPDATE, DELETE, and REPLACE.
 */

import type { BTree } from '../btree/index.js';
import type { WALWriter } from '../wal/index.js';
import type { SchemaManager } from './schema-manager.js';
import { substituteParams, parseSqlValue } from './sql-params.js';
import {
  parseReturningClause,
  stripReturningClause,
  applyReturning,
  evaluateExpression,
  type ParsedReturning,
} from './returning.js';
import {
  StatementError,
  StatementErrorCode,
  createTableNotFoundError,
  createUnsupportedSqlError,
  DatabaseError,
  DatabaseErrorCode,
} from '../errors/index.js';

// =============================================================================
// Query Result Type
// =============================================================================

export interface QueryResult {
  rows: Record<string, unknown>[];
  rowsAffected: number;
}

// =============================================================================
// Query Executor
// =============================================================================

export class QueryExecutor {
  constructor(
    private schemaManager: SchemaManager,
    private getBTree: () => BTree<string, Record<string, unknown>>,
    private getWAL: () => WALWriter | null,
  ) {}

  private get btree(): BTree<string, Record<string, unknown>> {
    return this.getBTree();
  }

  private get wal(): WALWriter | null {
    return this.getWAL();
  }

  /**
   * Route SQL to the appropriate executor
   */
  async executeSQL(
    sql: string,
    params?: Record<string, unknown>
  ): Promise<QueryResult> {
    // Substitute named parameters before parsing to prevent SQL injection
    if (params) {
      sql = substituteParams(sql, params);
    }

    const normalized = sql.trim().toUpperCase();

    // CREATE TABLE
    if (normalized.startsWith('CREATE TABLE')) {
      return this.schemaManager.executeCreateTable(sql);
    }

    // INSERT
    if (normalized.startsWith('INSERT')) {
      return this.executeInsert(sql);
    }

    // SELECT
    if (normalized.startsWith('SELECT')) {
      return this.executeSelect(sql);
    }

    // UPDATE
    if (normalized.startsWith('UPDATE')) {
      return this.executeUpdate(sql);
    }

    // DELETE
    if (normalized.startsWith('DELETE')) {
      return this.executeDelete(sql);
    }

    // REPLACE
    if (normalized.startsWith('REPLACE')) {
      return this.executeReplace(sql);
    }

    // DROP TABLE
    if (normalized.startsWith('DROP TABLE')) {
      return this.schemaManager.executeDropTable(sql, this.btree, this.wal);
    }

    throw createUnsupportedSqlError(sql);
  }

  /**
   * INSERT implementation with RETURNING support
   */
  private async executeInsert(sql: string): Promise<QueryResult> {
    // Parse RETURNING clause first
    const returning = parseReturningClause(sql);
    const sqlWithoutReturning = returning ? stripReturningClause(sql) : sql;

    // Check if this is INSERT ... SELECT
    const insertSelectMatch = sqlWithoutReturning.match(
      /INSERT\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*SELECT\s+(.+)/i
    );
    if (insertSelectMatch) {
      return this.executeInsertSelect(sql, returning, insertSelectMatch);
    }

    // Check if ON CONFLICT is present
    const hasOnConflict = /\sON\s+CONFLICT\s/i.test(sqlWithoutReturning);

    // Simple parser: INSERT INTO table (cols) VALUES (vals), (vals2), ...
    const multiValueMatch = sqlWithoutReturning.match(
      /INSERT\s+INTO\s+(\w+)\s*(?:AS\s+\w+\s*)?\(([^)]+)\)\s*VALUES\s*(.+)$/i
    );

    if (!multiValueMatch) {
      throw new StatementError(
        StatementErrorCode.INVALID_SQL,
        'Invalid INSERT syntax',
        sql
      );
    }

    // Non-null assertions: multiValueMatch groups are validated by regex pattern
    const tableName = multiValueMatch[1]!;
    const columnsStr = multiValueMatch[2]!;
    const columns = columnsStr.split(',').map((c) => c.trim());

    const schema = this.schemaManager.getSchema(tableName);
    if (!schema) {
      throw createTableNotFoundError(tableName, sql);
    }

    // Get schema columns for RETURNING *
    const schemaColumns = schema.columns.map((c) => c.name);

    // Parse all value rows
    const insertedRows: Record<string, unknown>[] = [];
    let valuesStr = multiValueMatch[3]!;

    // Remove ON CONFLICT clause from values string if present
    const onConflictIdx = valuesStr.toUpperCase().indexOf(' ON CONFLICT');
    if (onConflictIdx !== -1) {
      valuesStr = valuesStr.substring(0, onConflictIdx);
    }

    // Parse multiple value tuples: (val1, val2), (val3, val4), ...
    const valueMatches = valuesStr.matchAll(/\(([^)]+)\)/g);

    for (const valueMatch of valueMatches) {
      // Non-null assertion: valueMatch[1] is the captured group from regex
      const values = valueMatch[1]!.split(',').map((v) => parseSqlValue(v.trim()));

      // Build row object
      const row: Record<string, unknown> = {};
      columns.forEach((col, i) => {
        row[col] = values[i];
      });

      // Apply DEFAULT values for columns not explicitly provided
      for (const col of schema.columns) {
        if (!(col.name in row) && col.defaultValue) {
          row[col.name] = this.schemaManager.evaluateDefaultValue(col.defaultValue);
        }
      }

      // Auto-generate ID if not provided and schema has an INTEGER PRIMARY KEY
      if (row[schema.primaryKey] === undefined || row[schema.primaryKey] === null) {
        // Generate next ID
        const nextId = await this.schemaManager.getNextId(tableName, this.btree);
        row[schema.primaryKey] = nextId;
      }

      // Handle ON CONFLICT if present
      if (hasOnConflict) {
        const doNothingMatch = sqlWithoutReturning.match(/ON\s+CONFLICT\s+DO\s+NOTHING/i);
        const doUpdateMatch = sqlWithoutReturning.match(/ON\s+CONFLICT\s*(?:\([^)]+\))?\s*DO\s+UPDATE\s+SET\s+(.+?)(?:WHERE|$)/i);

        const key = `${tableName}:${String(row[schema.primaryKey])}`;
        const existingRow = await this.btree.get(key);

        if (existingRow) {
          if (doNothingMatch) {
            // DO NOTHING - skip this row, don't add to insertedRows
            continue;
          } else if (doUpdateMatch) {
            // DO UPDATE - update the existing row
            // Non-null assertion: doUpdateMatch[1] is the captured group from regex
            const setClauseStr = doUpdateMatch[1]!;
            const setMatches = setClauseStr.matchAll(/(\w+)\s*=\s*(\w+(?:\s*[+\-*/]\s*\w+)?)/g);
            const updatedRow = { ...existingRow };

            for (const setMatch of setMatches) {
              // Non-null assertions: setMatch groups are validated by regex pattern
              const setCol = setMatch[1]!;
              const setExpr = setMatch[2]!;
              updatedRow[setCol] = evaluateExpression(setExpr, existingRow, schemaColumns);
            }

            await this.btree.set(key, updatedRow);
            insertedRows.push(updatedRow);
            continue;
          }
        }
      }

      // Get primary key value
      const pkValue = row[schema.primaryKey];
      if (pkValue === undefined) {
        throw new StatementError(
          StatementErrorCode.CONSTRAINT_VIOLATION,
          `Primary key ${schema.primaryKey} is required`,
          sql
        );
      }

      // Write to B-tree
      const key = `${tableName}:${String(pkValue)}`;
      await this.btree.set(key, row);
      this.schemaManager.updateMaxIdCache(tableName, pkValue);

      // Write WAL entry (sync:true ensures flush before acknowledging)
      if (this.wal) {
        await this.wal.append({
          timestamp: Date.now(),
          txnId: `txn_${Date.now()}`,
          op: 'INSERT',
          table: tableName,
          after: new TextEncoder().encode(JSON.stringify(row)),
        }, { sync: true });
      }

      insertedRows.push(row);
    }

    // Apply RETURNING clause if present
    const resultRows = returning
      ? applyReturning(returning, insertedRows, schemaColumns)
      : [];

    return { rows: resultRows, rowsAffected: insertedRows.length };
  }

  /**
   * INSERT ... SELECT implementation with RETURNING support
   */
  private async executeInsertSelect(
    sql: string,
    returning: ParsedReturning | null,
    match: RegExpMatchArray
  ): Promise<QueryResult> {
    // Non-null assertions: match groups are validated by the regex pattern
    const tableName = match[1]!;
    const columnsStr = match[2]!;
    const selectPart = match[3]!;

    const columns = columnsStr.split(',').map((c) => c.trim());

    const schema = this.schemaManager.getSchema(tableName);
    if (!schema) {
      throw createTableNotFoundError(tableName, sql);
    }

    // Get schema columns for RETURNING *
    const schemaColumns = schema.columns.map((c) => c.name);

    // Execute the SELECT to get source rows
    const selectSql = `SELECT ${selectPart}`;
    const selectResult = await this.executeSelect(selectSql);

    // Insert each selected row
    const insertedRows: Record<string, unknown>[] = [];

    for (const sourceRow of selectResult.rows) {
      // Build row object from selected columns
      const row: Record<string, unknown> = {};
      columns.forEach((col, i) => {
        // Map selected columns to insert columns
        const sourceKeys = Object.keys(sourceRow);
        if (i < sourceKeys.length) {
          // Non-null assertion: we just checked i < sourceKeys.length
          row[col] = sourceRow[sourceKeys[i]!];
        }
      });

      // Apply DEFAULT values for columns not explicitly provided
      for (const col of schema.columns) {
        if (!(col.name in row) && col.defaultValue) {
          row[col.name] = this.schemaManager.evaluateDefaultValue(col.defaultValue);
        }
      }

      // Auto-generate ID if not provided
      if (row[schema.primaryKey] === undefined || row[schema.primaryKey] === null) {
        const nextId = await this.schemaManager.getNextId(tableName, this.btree);
        row[schema.primaryKey] = nextId;
      }

      // Get primary key value
      const pkValue = row[schema.primaryKey];
      const key = `${tableName}:${String(pkValue)}`;

      // Write to B-tree
      await this.btree.set(key, row);
      this.schemaManager.updateMaxIdCache(tableName, pkValue);

      // Write WAL entry (sync:true ensures flush before acknowledging)
      if (this.wal) {
        await this.wal.append({
          timestamp: Date.now(),
          txnId: `txn_${Date.now()}`,
          op: 'INSERT',
          table: tableName,
          after: new TextEncoder().encode(JSON.stringify(row)),
        }, { sync: true });
      }

      insertedRows.push(row);
    }

    // Apply RETURNING clause if present
    const resultRows = returning
      ? applyReturning(returning, insertedRows, schemaColumns)
      : [];

    return { rows: resultRows, rowsAffected: insertedRows.length };
  }

  /**
   * SELECT implementation
   */
  async executeSelect(sql: string): Promise<QueryResult> {
    // Simple parser: SELECT * FROM table [WHERE col = value]
    const match = sql.match(/SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?/i);
    if (!match) {
      throw new StatementError(
        StatementErrorCode.INVALID_SQL,
        'Invalid SELECT syntax',
        sql
      );
    }

    // Non-null assertions: match groups are validated by the regex pattern
    const _selectCols = match[1]!;
    const tableName = match[2]!;
    const whereClause = match[3];

    const schema = this.schemaManager.getSchema(tableName);
    if (!schema) {
      throw createTableNotFoundError(tableName, sql);
    }

    const rows: Record<string, unknown>[] = [];
    const prefix = `${tableName}:`;

    // Scan B-tree for matching rows
    for await (const [_key, value] of this.btree.range(prefix, prefix + '\uffff')) {
      // Apply WHERE filter if present
      if (whereClause) {
        const filterMatch = whereClause.match(/(\w+)\s*=\s*('(?:[^']|'')*'|[^'\s]+)/);
        if (filterMatch) {
          // Non-null assertions: filterMatch groups are validated by regex pattern
          const filterCol = filterMatch[1]!;
          const filterVal = parseSqlValue(filterMatch[2]!) as string;
          if (String(value[filterCol]) !== String(filterVal)) {
            continue;
          }
        }
      }
      rows.push(value);
    }

    return { rows, rowsAffected: 0 };
  }

  /**
   * UPDATE implementation with RETURNING support
   */
  private async executeUpdate(sql: string): Promise<QueryResult> {
    // Parse RETURNING clause first
    const returning = parseReturningClause(sql);
    const sqlWithoutReturning = returning ? stripReturningClause(sql) : sql;

    // Parse ORDER BY and LIMIT if present (before WHERE clause parsing)
    const orderByMatch = sqlWithoutReturning.match(/ORDER\s+BY\s+(\w+)\s+(ASC|DESC)?/i);
    const limitMatch = sqlWithoutReturning.match(/LIMIT\s+(\d+)/i);

    // Simple parser: UPDATE table SET col = value [WHERE col = value]
    // Also supports UPDATE table AS alias SET col = value WHERE col = value
    const match = sqlWithoutReturning.match(
      /UPDATE\s+(\w+)(?:\s+AS\s+\w+)?\s+SET\s+(.+?)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY|\s+LIMIT|$)/i
    );
    if (!match) {
      throw new StatementError(
        StatementErrorCode.INVALID_SQL,
        'Invalid UPDATE syntax',
        sql
      );
    }

    // Non-null assertions: match groups are validated by the regex pattern
    const tableName = match[1]!;
    let setClauseStr = match[2]!;
    const whereClause = match[3];

    const schema = this.schemaManager.getSchema(tableName);
    if (!schema) {
      throw createTableNotFoundError(tableName, sql);
    }

    // Get schema columns for RETURNING *
    const schemaColumns = schema.columns.map((c) => c.name);

    // Parse multiple SET clauses (col1 = val1, col2 = val2)
    const setUpdates: Array<{ col: string; expr: string }> = [];

    // Remove any trailing WHERE/ORDER BY/LIMIT from SET clause
    setClauseStr = setClauseStr.replace(/\s+(WHERE|ORDER|LIMIT).*/i, '');

    const setClauses = setClauseStr.split(',');
    for (const clause of setClauses) {
      const setMatch = clause.trim().match(/^([\w.]+)\s*=\s*(.+)$/);
      if (setMatch) {
        // Non-null assertions: setMatch groups are validated by regex pattern
        const setMatchCol = setMatch[1]!;
        const colName = setMatchCol.includes('.') ? setMatchCol.split('.').pop()! : setMatchCol;
        setUpdates.push({
          col: colName,
          expr: setMatch[2]!.trim(),
        });
      }
    }

    if (setUpdates.length === 0) {
      throw new StatementError(
        StatementErrorCode.INVALID_SQL,
        'Invalid SET clause',
        sql
      );
    }

    // Collect matching rows
    const prefix = `${tableName}:`;
    const matchingRows: Array<{ key: string; value: Record<string, unknown> }> = [];

    for await (const [key, value] of this.btree.range(prefix, prefix + '\uffff')) {
      // Apply WHERE filter if present
      if (whereClause) {
        if (!this.evaluateWhereClause(whereClause, value, schemaColumns)) {
          continue;
        }
      }
      matchingRows.push({ key, value: { ...value } });
    }

    // Apply ORDER BY if present
    if (orderByMatch) {
      const orderCol = orderByMatch[1];
      const orderDir = (orderByMatch[2] || 'ASC').toUpperCase();
      matchingRows.sort((a, b) => {
        const aVal = a.value[orderCol];
        const bVal = b.value[orderCol];
        if (aVal === bVal) return 0;
        if (aVal === null) return orderDir === 'ASC' ? 1 : -1;
        if (bVal === null) return orderDir === 'ASC' ? -1 : 1;
        const cmp = aVal < bVal ? -1 : 1;
        return orderDir === 'DESC' ? -cmp : cmp;
      });
    }

    // Apply LIMIT if present
    if (limitMatch) {
      const limit = parseInt(limitMatch[1], 10);
      matchingRows.splice(limit);
    }

    // Update matching rows
    const updatedRows: Record<string, unknown>[] = [];

    for (const { key, value } of matchingRows) {
      const before = { ...value };

      // Apply SET updates
      for (const update of setUpdates) {
        const newValue = evaluateExpression(update.expr, value, schemaColumns);
        value[update.col] = newValue;
      }

      await this.btree.set(key, value);
      updatedRows.push(value);

      // Write WAL entry (sync:true ensures flush before acknowledging)
      if (this.wal) {
        await this.wal.append({
          timestamp: Date.now(),
          txnId: `txn_${Date.now()}`,
          op: 'UPDATE',
          table: tableName,
          before: new TextEncoder().encode(JSON.stringify(before)),
          after: new TextEncoder().encode(JSON.stringify(value)),
        }, { sync: true });
      }
    }

    // Apply RETURNING clause if present
    const resultRows = returning
      ? applyReturning(returning, updatedRows, schemaColumns)
      : [];

    return { rows: resultRows, rowsAffected: updatedRows.length };
  }

  /**
   * DELETE implementation with RETURNING support
   */
  private async executeDelete(sql: string): Promise<QueryResult> {
    // Parse RETURNING clause first
    const returning = parseReturningClause(sql);
    const sqlWithoutReturning = returning ? stripReturningClause(sql) : sql;

    // Parse ORDER BY and LIMIT if present
    const orderByMatch = sqlWithoutReturning.match(/ORDER\s+BY\s+(\w+)\s+(ASC|DESC)?/i);
    const limitMatch = sqlWithoutReturning.match(/LIMIT\s+(\d+)/i);

    // Simple parser: DELETE FROM table [AS alias] [WHERE col = value]
    const match = sqlWithoutReturning.match(
      /DELETE\s+FROM\s+(\w+)(?:\s+AS\s+\w+)?(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY|\s+LIMIT|$)/i
    );
    if (!match) {
      throw new StatementError(
        StatementErrorCode.INVALID_SQL,
        'Invalid DELETE syntax',
        sql
      );
    }

    const tableName = match[1];
    const whereClause = match[2];

    const schema = this.schemaManager.getSchema(tableName);
    if (!schema) {
      throw createTableNotFoundError(tableName, sql);
    }

    // Get schema columns for RETURNING *
    const schemaColumns = schema.columns.map((c) => c.name);

    // Collect matching rows
    const prefix = `${tableName}:`;
    const matchingRows: Array<{ key: string; value: Record<string, unknown> }> = [];

    for await (const [key, value] of this.btree.range(prefix, prefix + '\uffff')) {
      // Apply WHERE filter if present
      if (whereClause) {
        if (!this.evaluateWhereClause(whereClause, value, schemaColumns)) {
          continue;
        }
      }
      matchingRows.push({ key, value: { ...value } });
    }

    // Apply ORDER BY if present
    if (orderByMatch) {
      const orderCol = orderByMatch[1];
      const orderDir = (orderByMatch[2] || 'ASC').toUpperCase();
      matchingRows.sort((a, b) => {
        const aVal = a.value[orderCol];
        const bVal = b.value[orderCol];
        if (aVal === bVal) return 0;
        if (aVal === null) return orderDir === 'ASC' ? 1 : -1;
        if (bVal === null) return orderDir === 'ASC' ? -1 : 1;
        const cmp = aVal < bVal ? -1 : 1;
        return orderDir === 'DESC' ? -cmp : cmp;
      });
    }

    // Apply LIMIT if present
    if (limitMatch) {
      const limit = parseInt(limitMatch[1], 10);
      matchingRows.splice(limit);
    }

    // Capture rows for RETURNING before deletion
    const deletedRows = matchingRows.map((r) => r.value);

    // Delete the rows
    for (const { key, value } of matchingRows) {
      await this.btree.delete(key);

      // Write WAL entry (sync:true ensures flush before acknowledging)
      if (this.wal) {
        await this.wal.append({
          timestamp: Date.now(),
          txnId: `txn_${Date.now()}`,
          op: 'DELETE',
          table: tableName,
          before: new TextEncoder().encode(JSON.stringify(value)),
        }, { sync: true });
      }
    }

    // Apply RETURNING clause if present
    const resultRows = returning
      ? applyReturning(returning, deletedRows, schemaColumns)
      : [];

    return { rows: resultRows, rowsAffected: deletedRows.length };
  }

  /**
   * REPLACE implementation with RETURNING support
   * REPLACE is INSERT OR REPLACE - it deletes conflicting rows and inserts new ones
   */
  private async executeReplace(sql: string): Promise<QueryResult> {
    // Parse RETURNING clause first
    const returning = parseReturningClause(sql);
    const sqlWithoutReturning = returning ? stripReturningClause(sql) : sql;

    // Simple parser: REPLACE INTO table (cols) VALUES (vals)
    const match = sqlWithoutReturning.match(
      /REPLACE\s+INTO\s+(\w+)\s*\(([^)]+)\)\s*VALUES\s*(.+)$/i
    );
    if (!match) {
      throw new StatementError(
        StatementErrorCode.INVALID_SQL,
        'Invalid REPLACE syntax',
        sql
      );
    }

    const tableName = match[1];
    const columnsStr = match[2];
    const columns = columnsStr.split(',').map((c) => c.trim());

    const schema = this.schemaManager.getSchema(tableName);
    if (!schema) {
      throw createTableNotFoundError(tableName, sql);
    }

    // Get schema columns for RETURNING *
    const schemaColumns = schema.columns.map((c) => c.name);

    // Parse all value rows
    const insertedRows: Record<string, unknown>[] = [];
    const valuesStr = match[3];

    // Parse multiple value tuples: (val1, val2), (val3, val4), ...
    const valueMatches = valuesStr.matchAll(/\(([^)]+)\)/g);

    for (const valueMatch of valueMatches) {
      const values = valueMatch[1].split(',').map((v) => parseSqlValue(v.trim()));

      // Build row object
      const row: Record<string, unknown> = {};
      columns.forEach((col, i) => {
        row[col] = values[i];
      });

      // Auto-generate ID if not provided
      if (row[schema.primaryKey] === undefined || row[schema.primaryKey] === null) {
        const nextId = await this.schemaManager.getNextId(tableName, this.btree);
        row[schema.primaryKey] = nextId;
      }

      const pkValue = row[schema.primaryKey];
      const key = `${tableName}:${String(pkValue)}`;

      // Check if row exists and delete it first (REPLACE behavior)
      const existingRow = await this.btree.get(key);
      if (existingRow) {
        // Write WAL entry for delete (sync:true ensures flush before acknowledging)
        if (this.wal) {
          await this.wal.append({
            timestamp: Date.now(),
            txnId: `txn_${Date.now()}`,
            op: 'DELETE',
            table: tableName,
            before: new TextEncoder().encode(JSON.stringify(existingRow)),
          }, { sync: true });
        }
      }

      // Write to B-tree
      await this.btree.set(key, row);
      this.schemaManager.updateMaxIdCache(tableName, pkValue);

      // Write WAL entry for insert (sync:true ensures flush before acknowledging)
      if (this.wal) {
        await this.wal.append({
          timestamp: Date.now(),
          txnId: `txn_${Date.now()}`,
          op: 'INSERT',
          table: tableName,
          after: new TextEncoder().encode(JSON.stringify(row)),
        }, { sync: true });
      }

      insertedRows.push(row);
    }

    // Apply RETURNING clause if present
    const resultRows = returning
      ? applyReturning(returning, insertedRows, schemaColumns)
      : [];

    return { rows: resultRows, rowsAffected: insertedRows.length };
  }

  // =============================================================================
  // WHERE Clause Evaluation
  // =============================================================================

  /**
   * Evaluate a WHERE clause against a row
   */
  private evaluateWhereClause(
    whereClause: string,
    row: Record<string, unknown>,
    schemaColumns: string[]
  ): boolean {
    const trimmed = whereClause.trim();

    // Handle parenthesized expressions first
    if (trimmed.startsWith('(') && trimmed.endsWith(')')) {
      // Check if it's a balanced outer parentheses
      let depth = 0;
      let isOuter = true;
      for (let i = 0; i < trimmed.length; i++) {
        if (trimmed[i] === '(') depth++;
        else if (trimmed[i] === ')') depth--;
        if (depth === 0 && i < trimmed.length - 1) {
          isOuter = false;
          break;
        }
      }
      if (isOuter) {
        return this.evaluateWhereClause(trimmed.slice(1, -1), row, schemaColumns);
      }
    }

    // Split by OR (lowest precedence), respecting parentheses
    const orParts = this.splitByKeyword(trimmed, 'OR');
    if (orParts.length > 1) {
      return orParts.some((part) => this.evaluateWhereClause(part, row, schemaColumns));
    }

    // Split by AND (higher precedence than OR), respecting parentheses
    const andParts = this.splitByKeyword(trimmed, 'AND');
    if (andParts.length > 1) {
      return andParts.every((part) => this.evaluateWhereClause(part, row, schemaColumns));
    }

    // Handle IN clause
    const inMatch = trimmed.match(/(\w+)\s+IN\s*\(([^)]+)\)/i);
    if (inMatch) {
      const col = inMatch[1];
      const values = inMatch[2].split(',').map((v) => {
        const val = v.trim();
        if (val.startsWith("'") && val.endsWith("'")) {
          return val.slice(1, -1);
        }
        const num = Number(val);
        return !isNaN(num) ? num : val;
      });
      const rowVal = row[col];
      return values.some((v) => v == rowVal);
    }

    // Handle comparison operators
    const compMatch = trimmed.match(/(\w+)\s*(=|!=|<>|<|<=|>|>=)\s*('?[^']*'?)/);
    if (compMatch) {
      const col = compMatch[1];
      const op = compMatch[2];
      let val: unknown = compMatch[3].trim();

      // Parse value using parseSqlValue for proper escaped quote handling
      if (typeof val === 'string') {
        val = parseSqlValue(val);
      }

      const rowVal = row[col];

      switch (op) {
        case '=':
          return rowVal == val;
        case '!=':
        case '<>':
          return rowVal != val;
        case '<':
          return Number(rowVal) < Number(val);
        case '<=':
          return Number(rowVal) <= Number(val);
        case '>':
          return Number(rowVal) > Number(val);
        case '>=':
          return Number(rowVal) >= Number(val);
      }
    }

    // Simple equality check (fallback)
    const simpleMatch = trimmed.match(/(\w+)\s*=\s*'?([^']+)'?/);
    if (simpleMatch) {
      const filterCol = simpleMatch[1];
      const filterVal = simpleMatch[2];
      return String(row[filterCol]) === filterVal;
    }

    return true;
  }

  /**
   * Split a string by keyword, respecting parentheses
   */
  private splitByKeyword(str: string, keyword: string): string[] {
    const result: string[] = [];
    let depth = 0;
    const keywordRegex = new RegExp(`\\s+${keyword}\\s+`, 'gi');
    let lastIndex = 0;

    // Find all keyword positions
    const positions: number[] = [];
    let match;
    while ((match = keywordRegex.exec(str)) !== null) {
      positions.push(match.index);
    }

    if (positions.length === 0) {
      return [str.trim()];
    }

    // Check depth at each keyword position
    for (let i = 0; i < str.length; i++) {
      if (str[i] === '(') depth++;
      else if (str[i] === ')') depth--;

      if (depth === 0 && positions.includes(i)) {
        result.push(str.substring(lastIndex, i).trim());
        lastIndex = i + keyword.length + 2; // Skip keyword and surrounding spaces
      }
    }

    // Add remaining part
    if (lastIndex < str.length) {
      result.push(str.substring(lastIndex).trim());
    }

    return result.length > 0 ? result : [str.trim()];
  }
}
