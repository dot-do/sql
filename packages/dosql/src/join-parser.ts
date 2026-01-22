/**
 * JOIN Parser Types for DoSQL
 *
 * Extends the type-level SQL parser to handle JOINs with proper type inference.
 * Supports: INNER JOIN, LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN, CROSS JOIN
 *
 * Features:
 * - Parse table aliases (FROM users u, FROM users AS u)
 * - Resolve alias.column references to correct types
 * - Merge types from multiple tables
 * - Handle nullable columns for outer joins
 */

// =============================================================================
// UTILITY TYPES
// =============================================================================

type Whitespace = ' ' | '\n' | '\t' | '\r';

type Trim<S extends string> =
  S extends `${Whitespace}${infer Rest}` ? Trim<Rest> :
  S extends `${infer Rest}${Whitespace}` ? Trim<Rest> :
  S;

type TrimStart<S extends string> =
  S extends `${Whitespace}${infer Rest}` ? TrimStart<Rest> : S;

type Upper<S extends string> = Uppercase<S>;
type Lower<S extends string> = Lowercase<S>;

// Split string by delimiter (improved to handle edge cases)
type Split<S extends string, D extends string> =
  S extends `${infer Head}${D}${infer Tail}`
    ? [Trim<Head>, ...Split<Tail, D>]
    : S extends '' ? [] : [Trim<S>];

// =============================================================================
// SCHEMA TYPES
// =============================================================================

export type ColumnType =
  | 'string'
  | 'number'
  | 'boolean'
  | 'Date'
  | 'null'
  | 'unknown';

export type TableSchema = Record<string, ColumnType>;
export type DatabaseSchema = Record<string, TableSchema>;

// Convert our column types to TypeScript types
type TSType<T extends ColumnType> =
  T extends 'string' ? string :
  T extends 'number' ? number :
  T extends 'boolean' ? boolean :
  T extends 'Date' ? Date :
  T extends 'null' ? null :
  unknown;

// Make type nullable (for LEFT/RIGHT/FULL OUTER JOINs)
type Nullable<T> = T | null;

// =============================================================================
// JOIN TYPES
// =============================================================================

export type JoinType =
  | 'INNER'
  | 'LEFT'
  | 'RIGHT'
  | 'FULL'
  | 'CROSS';

export interface TableRef {
  table: string;
  alias: string;
}

export interface JoinClause {
  type: JoinType;
  table: string;
  alias: string;
}

export interface ParsedQuery {
  columns: string[];
  from: TableRef;
  joins: JoinClause[];
}

// =============================================================================
// PARSE TABLE REFERENCE
// =============================================================================

/**
 * Parse table reference with optional alias
 * Formats: "table", "table alias", "table AS alias"
 */
type ParseTableRef<S extends string> = ParseTableRefTrimmed<Trim<S>>;

/**
 * Internal: Parse table reference after trimming whitespace
 */
type ParseTableRefTrimmed<S extends string> =
  // Handle "table AS alias" (case insensitive)
  Upper<S> extends `${infer Table} AS ${infer Alias}`
    ? { table: Lower<Trim<Table>>; alias: Lower<Trim<Alias>> }
  // Handle "table alias" (without AS keyword)
  : S extends `${infer Table} ${infer Rest}`
    ? Trim<Rest> extends ''
      ? { table: Lower<S>; alias: Lower<S> }
      // Check if Rest starts with a keyword (JOIN, WHERE, etc.)
      : IsKeyword<Trim<Rest>> extends true
        ? { table: Lower<Table>; alias: Lower<Table> }
        : { table: Lower<Table>; alias: Lower<Trim<Rest>> }
  // Just table name
  : { table: Lower<S>; alias: Lower<S> };

// Check if string starts with SQL keyword
type IsKeyword<S extends string> =
  Upper<S> extends `JOIN${string}` ? true :
  Upper<S> extends `LEFT${string}` ? true :
  Upper<S> extends `RIGHT${string}` ? true :
  Upper<S> extends `INNER${string}` ? true :
  Upper<S> extends `OUTER${string}` ? true :
  Upper<S> extends `FULL${string}` ? true :
  Upper<S> extends `CROSS${string}` ? true :
  Upper<S> extends `WHERE${string}` ? true :
  Upper<S> extends `GROUP${string}` ? true :
  Upper<S> extends `ORDER${string}` ? true :
  Upper<S> extends `HAVING${string}` ? true :
  Upper<S> extends `LIMIT${string}` ? true :
  Upper<S> extends `ON${string}` ? true :
  false;

// =============================================================================
// PARSE JOIN TYPE
// =============================================================================

/**
 * Extract the join type from a JOIN clause prefix
 */
type ParseJoinType<S extends string> =
  Upper<S> extends `LEFT OUTER JOIN${string}` ? 'LEFT' :
  Upper<S> extends `RIGHT OUTER JOIN${string}` ? 'RIGHT' :
  Upper<S> extends `FULL OUTER JOIN${string}` ? 'FULL' :
  Upper<S> extends `LEFT JOIN${string}` ? 'LEFT' :
  Upper<S> extends `RIGHT JOIN${string}` ? 'RIGHT' :
  Upper<S> extends `INNER JOIN${string}` ? 'INNER' :
  Upper<S> extends `CROSS JOIN${string}` ? 'CROSS' :
  Upper<S> extends `JOIN${string}` ? 'INNER' :  // Default to INNER
  never;

// =============================================================================
// PARSE SINGLE JOIN
// =============================================================================

/**
 * Parse a single JOIN clause and extract table reference and join type
 */
type ParseSingleJoin<S extends string> =
  // LEFT OUTER JOIN table ON condition
  Upper<S> extends `LEFT OUTER JOIN ${infer Rest}`
    ? ExtractJoinTableRef<Rest, 'LEFT'>
  // RIGHT OUTER JOIN table ON condition
  : Upper<S> extends `RIGHT OUTER JOIN ${infer Rest}`
    ? ExtractJoinTableRef<Rest, 'RIGHT'>
  // FULL OUTER JOIN table ON condition
  : Upper<S> extends `FULL OUTER JOIN ${infer Rest}`
    ? ExtractJoinTableRef<Rest, 'FULL'>
  // LEFT JOIN table ON condition
  : Upper<S> extends `LEFT JOIN ${infer Rest}`
    ? ExtractJoinTableRef<Rest, 'LEFT'>
  // RIGHT JOIN table ON condition
  : Upper<S> extends `RIGHT JOIN ${infer Rest}`
    ? ExtractJoinTableRef<Rest, 'RIGHT'>
  // INNER JOIN table ON condition
  : Upper<S> extends `INNER JOIN ${infer Rest}`
    ? ExtractJoinTableRef<Rest, 'INNER'>
  // CROSS JOIN table (no ON clause)
  : Upper<S> extends `CROSS JOIN ${infer Rest}`
    ? ExtractJoinTableRefCross<Rest>
  // Plain JOIN table ON condition (defaults to INNER)
  : Upper<S> extends `JOIN ${infer Rest}`
    ? ExtractJoinTableRef<Rest, 'INNER'>
  : never;

/**
 * Extract table reference from JOIN ... ON ... portion
 * Lowercases input for case-insensitive matching
 */
type ExtractJoinTableRef<S extends string, JT extends JoinType> = ExtractJoinTableRefLower<Lower<S>, JT>;

type ExtractJoinTableRefLower<S extends string, JT extends JoinType> =
  // Handle "table ON condition" (lowercase)
  S extends `${infer TablePart} on ${infer _Condition}`
    ? ParseTableRef<Trim<TablePart>> extends { table: infer T extends string; alias: infer A extends string }
      ? { type: JT; table: T; alias: A }
      : never
  : never;

/**
 * Extract table reference from CROSS JOIN (no ON clause)
 */
type ExtractJoinTableRefCross<S extends string> =
  // CROSS JOIN doesn't have ON clause, just table reference
  ParseTableRef<Trim<S>> extends { table: infer T extends string; alias: infer A extends string }
    ? { type: 'CROSS'; table: T; alias: A }
    : never;

// =============================================================================
// EXTRACT ALL JOINS FROM QUERY
// =============================================================================

/**
 * Recursively extract all JOIN clauses from the query string
 * Lowercases input for case-insensitive matching
 *
 * Strategy: Split by " on " to process each segment. Each " on " marker
 * indicates the end of a table reference. The segment before " on " contains
 * the join type and table ref, the segment after might contain another join.
 */
type ExtractAllJoins<S extends string, Acc extends JoinClause[] = []> = ExtractAllJoinsLower<Lower<S>, Acc>;

/**
 * Process string segment by segment, looking for JOINs
 * We split on " on " boundaries since that's where each join's table ref ends
 */
type ExtractAllJoinsLower<
  S extends string,
  Acc extends JoinClause[] = []
> =
  // Look for "X join TABLE on CONDITION" pattern
  // Split at first " on " to get table ref part, then recurse on rest
  S extends `${infer BeforeOn} on ${infer AfterOn}`
    ? ExtractJoinFromSegment<BeforeOn> extends infer J extends JoinClause
      ? ExtractAllJoinsLower<AfterOn, [...Acc, J]>
      : ExtractAllJoinsLower<AfterOn, Acc>
    : Acc;

/**
 * Extract join info from a segment that ends at " on "
 * The segment might look like "users u join orders o" or "u.id = o.id left join items i"
 */
type ExtractJoinFromSegment<S extends string> =
  // Try to match different join types at the END of the segment (before " on ")
  // These patterns match "... join TABLE" where TABLE is the last table ref
  S extends `${infer _} left outer join ${infer TablePart}`
    ? { type: 'LEFT'; table: ParseTableRef<Trim<TablePart>>['table']; alias: ParseTableRef<Trim<TablePart>>['alias'] }
  : S extends `${infer _} right outer join ${infer TablePart}`
    ? { type: 'RIGHT'; table: ParseTableRef<Trim<TablePart>>['table']; alias: ParseTableRef<Trim<TablePart>>['alias'] }
  : S extends `${infer _} full outer join ${infer TablePart}`
    ? { type: 'FULL'; table: ParseTableRef<Trim<TablePart>>['table']; alias: ParseTableRef<Trim<TablePart>>['alias'] }
  : S extends `${infer _} left join ${infer TablePart}`
    ? { type: 'LEFT'; table: ParseTableRef<Trim<TablePart>>['table']; alias: ParseTableRef<Trim<TablePart>>['alias'] }
  : S extends `${infer _} right join ${infer TablePart}`
    ? { type: 'RIGHT'; table: ParseTableRef<Trim<TablePart>>['table']; alias: ParseTableRef<Trim<TablePart>>['alias'] }
  : S extends `${infer _} inner join ${infer TablePart}`
    ? { type: 'INNER'; table: ParseTableRef<Trim<TablePart>>['table']; alias: ParseTableRef<Trim<TablePart>>['alias'] }
  : S extends `${infer _} cross join ${infer TablePart}`
    ? { type: 'CROSS'; table: ParseTableRef<Trim<TablePart>>['table']; alias: ParseTableRef<Trim<TablePart>>['alias'] }
  : S extends `${infer _} join ${infer TablePart}`
    ? { type: 'INNER'; table: ParseTableRef<Trim<TablePart>>['table']; alias: ParseTableRef<Trim<TablePart>>['alias'] }
  : never;

// =============================================================================
// PARSE COLUMN EXPRESSIONS
// =============================================================================

/**
 * Parse column expression with optional alias
 * Formats: "col", "col AS alias", "table.col", "table.col AS alias"
 */
type ParseColumnExpr<S extends string> =
  // Handle "expr AS alias"
  Upper<S> extends `${infer Expr} AS ${infer Alias}`
    ? { expr: Lower<Trim<Expr>>; alias: Lower<Trim<Alias>> }
  // Handle simple expression (may contain spaces in functions, but simplified here)
  : { expr: Lower<Trim<S>>; alias: null };

/**
 * Get the output column name from a parsed column expression
 */
type GetOutputName<C extends { expr: string; alias: string | null }> =
  C['alias'] extends string ? C['alias'] :
  C['expr'] extends `${string}.${infer Col}` ? Col :
  C['expr'];

// =============================================================================
// AGGREGATE FUNCTIONS
// =============================================================================

type AggregateFunction = 'COUNT' | 'SUM' | 'AVG' | 'MIN' | 'MAX';

type IsAggregate<S extends string> =
  Upper<S> extends `${AggregateFunction}(${string})` ? true : false;

type ParseAggregate<S extends string> =
  Upper<S> extends `COUNT(${infer _})` ? { fn: 'COUNT'; type: 'number' } :
  Upper<S> extends `SUM(${infer _})` ? { fn: 'SUM'; type: 'number' } :
  Upper<S> extends `AVG(${infer _})` ? { fn: 'AVG'; type: 'number' } :
  Upper<S> extends `MIN(${infer _})` ? { fn: 'MIN'; type: 'number' } :
  Upper<S> extends `MAX(${infer _})` ? { fn: 'MAX'; type: 'number' } :
  never;

// =============================================================================
// PARSE FROM CLAUSE
// =============================================================================

/**
 * Extract the main FROM table, stopping at JOIN or WHERE keywords
 * Lowercases input for case-insensitive matching
 */
type ParseFromTable<S extends string> = ParseFromTableLower<Lower<S>>;

type ParseFromTableLower<S extends string> =
  // Check plain " join " FIRST to find the earliest JOIN keyword in the string
  // This catches any join type since all contain " join " as substring
  S extends `${infer Table} join ${infer _}` ? ExtractTableBeforeJoin<Table> :
  // If no join found, check other clauses
  S extends `${infer Table} where ${infer _}` ? ParseTableRef<Trim<Table>> :
  S extends `${infer Table} group ${infer _}` ? ParseTableRef<Trim<Table>> :
  S extends `${infer Table} order ${infer _}` ? ParseTableRef<Trim<Table>> :
  S extends `${infer Table} having ${infer _}` ? ParseTableRef<Trim<Table>> :
  S extends `${infer Table} limit ${infer _}` ? ParseTableRef<Trim<Table>> :
  ParseTableRef<Trim<S>>;

/**
 * Extract the table reference from text that may contain a join type prefix
 * e.g., "users u" or "users u left" -> should return {table: "users", alias: "u"}
 */
type ExtractTableBeforeJoin<S extends string> =
  // Check if Table ends with a join type keyword (case: "users u left" from "users u left join")
  S extends `${infer Actual} left outer` ? ParseTableRef<Trim<Actual>> :
  S extends `${infer Actual} right outer` ? ParseTableRef<Trim<Actual>> :
  S extends `${infer Actual} full outer` ? ParseTableRef<Trim<Actual>> :
  S extends `${infer Actual} left` ? ParseTableRef<Trim<Actual>> :
  S extends `${infer Actual} right` ? ParseTableRef<Trim<Actual>> :
  S extends `${infer Actual} inner` ? ParseTableRef<Trim<Actual>> :
  S extends `${infer Actual} cross` ? ParseTableRef<Trim<Actual>> :
  S extends `${infer Actual} full` ? ParseTableRef<Trim<Actual>> :
  // No prefix, just the table reference
  ParseTableRef<Trim<S>>;

// =============================================================================
// FULL SELECT STATEMENT PARSER
// =============================================================================

/**
 * Parse a complete SELECT statement
 * We lowercase the entire query first to normalize, then parse
 */
type ParseSelectStatement<S extends string> = ParseSelectLower<Lower<S>>;

type ParseSelectLower<S extends string> =
  S extends `select ${infer ColPart} from ${infer FromRest}`
    ? {
        columns: Split<Trim<ColPart>, ','>;
        from: ParseFromTable<Trim<FromRest>>;
        joins: ExtractAllJoins<FromRest>;
      }
    : { error: 'Invalid SELECT statement' };

// =============================================================================
// BUILD ALIAS MAP
// =============================================================================

/**
 * Build a mapping from alias -> table name for all tables in scope
 */
type BuildAliasMap<
  From extends TableRef,
  Joins extends JoinClause[]
> = {
  [K in From['alias']]: From['table']
} & UnionToIntersection<{
  [I in keyof Joins]: Joins[I] extends JoinClause
    ? { [K in Joins[I]['alias']]: Joins[I]['table'] }
    : never
}[number]>;

/**
 * Build a mapping from alias -> join type for handling nullability
 */
type BuildJoinTypeMap<
  From extends TableRef,
  Joins extends JoinClause[]
> = {
  [K in From['alias']]: 'FROM'
} & UnionToIntersection<{
  [I in keyof Joins]: Joins[I] extends JoinClause
    ? { [K in Joins[I]['alias']]: Joins[I]['type'] }
    : never
}[number]>;

// Convert union to intersection helper
type UnionToIntersection<U> =
  (U extends any ? (k: U) => void : never) extends ((k: infer I) => void) ? I : never;

// =============================================================================
// RESOLVE COLUMN REFERENCES
// =============================================================================

/**
 * Check if a table's columns should be nullable based on join type
 * - LEFT JOIN: right table columns are nullable
 * - RIGHT JOIN: left table (FROM) columns are nullable
 * - FULL OUTER JOIN: both sides are nullable
 */
type ShouldBeNullable<
  Alias extends string,
  JoinTypeMap extends Record<string, JoinType | 'FROM'>,
  Joins extends JoinClause[]
> =
  Alias extends keyof JoinTypeMap
    ? JoinTypeMap[Alias] extends 'LEFT' ? true :
      JoinTypeMap[Alias] extends 'FULL' ? true :
      // For FROM table, check if any join is RIGHT or FULL
      JoinTypeMap[Alias] extends 'FROM'
        ? HasRightOrFullJoin<Joins>
        : false
    : false;

type HasRightOrFullJoin<Joins extends JoinClause[]> =
  Joins extends [infer First, ...infer Rest extends JoinClause[]]
    ? First extends JoinClause
      ? First['type'] extends 'RIGHT' | 'FULL' ? true : HasRightOrFullJoin<Rest>
      : HasRightOrFullJoin<Rest>
    : false;

/**
 * Resolve a column reference (alias.column or bare column) to its type
 */
type ResolveColumnRef<
  Ref extends string,
  AliasMap extends Record<string, string>,
  JoinTypeMap extends Record<string, JoinType | 'FROM'>,
  Joins extends JoinClause[],
  DB extends DatabaseSchema
> =
  // Handle "alias.column"
  Lower<Ref> extends `${infer Alias}.${infer Col}`
    ? Alias extends keyof AliasMap
      ? AliasMap[Alias] extends keyof DB
        ? Col extends keyof DB[AliasMap[Alias]]
          ? ShouldBeNullable<Alias, JoinTypeMap, Joins> extends true
            ? DB[AliasMap[Alias]][Col] | 'null'
            : DB[AliasMap[Alias]][Col]
          : 'unknown'
        : 'unknown'
      : 'unknown'
  // Handle bare column name - search all tables
  : SearchAllTables<Lower<Ref>, AliasMap, DB>;

/**
 * Search all tables in scope for a column
 */
type SearchAllTables<
  Col extends string,
  AliasMap extends Record<string, string>,
  DB extends DatabaseSchema
> = {
  [K in keyof AliasMap]: AliasMap[K] extends keyof DB
    ? Col extends keyof DB[AliasMap[K]]
      ? DB[AliasMap[K]][Col]
      : never
    : never
}[keyof AliasMap] extends never ? 'unknown' : {
  [K in keyof AliasMap]: AliasMap[K] extends keyof DB
    ? Col extends keyof DB[AliasMap[K]]
      ? DB[AliasMap[K]][Col]
      : never
    : never
}[keyof AliasMap];

/**
 * Resolve a column expression (may be aggregate, etc.)
 */
type ResolveColumnExpr<
  Expr extends string,
  AliasMap extends Record<string, string>,
  JoinTypeMap extends Record<string, JoinType | 'FROM'>,
  Joins extends JoinClause[],
  DB extends DatabaseSchema
> =
  // Aggregate function
  IsAggregate<Expr> extends true
    ? ParseAggregate<Expr>['type']
  // Column reference
  : ResolveColumnRef<Expr, AliasMap, JoinTypeMap, Joins, DB>;

// =============================================================================
// BUILD RESULT TYPE
// =============================================================================

/**
 * Handle SELECT * - merge all columns from all tables
 */
type SelectStarResult<
  AliasMap extends Record<string, string>,
  JoinTypeMap extends Record<string, JoinType | 'FROM'>,
  Joins extends JoinClause[],
  DB extends DatabaseSchema
> = UnionToIntersection<{
  [K in keyof AliasMap]: AliasMap[K] extends keyof DB
    ? ShouldBeNullable<K & string, JoinTypeMap, Joins> extends true
      ? { [C in keyof DB[AliasMap[K]]]: TSType<DB[AliasMap[K]][C]> | null }
      : { [C in keyof DB[AliasMap[K]]]: TSType<DB[AliasMap[K]][C]> }
    : {}
}[keyof AliasMap]>;

/**
 * Process a single column into the result shape
 */
type ProcessColumn<
  Col extends string,
  AliasMap extends Record<string, string>,
  JoinTypeMap extends Record<string, JoinType | 'FROM'>,
  Joins extends JoinClause[],
  DB extends DatabaseSchema
> = ParseColumnExpr<Col> extends { expr: infer E extends string; alias: infer A }
  ? A extends string
    // Has explicit alias
    ? { [K in A]: TSType<ResolveColumnExpr<E, AliasMap, JoinTypeMap, Joins, DB> & ColumnType> }
    // No alias - use column name (strip table prefix)
    : E extends `${string}.${infer ColName}`
      ? { [K in ColName]: TSType<ResolveColumnExpr<E, AliasMap, JoinTypeMap, Joins, DB> & ColumnType> }
      : { [K in E]: TSType<ResolveColumnExpr<E, AliasMap, JoinTypeMap, Joins, DB> & ColumnType> }
  : {};

/**
 * Process all columns into the result type
 */
type ProcessColumns<
  Cols extends string[],
  AliasMap extends Record<string, string>,
  JoinTypeMap extends Record<string, JoinType | 'FROM'>,
  Joins extends JoinClause[],
  DB extends DatabaseSchema
> = Cols extends [infer First extends string, ...infer Rest extends string[]]
  ? Trim<First> extends '*'
    ? SelectStarResult<AliasMap, JoinTypeMap, Joins, DB>
    : ProcessColumn<First, AliasMap, JoinTypeMap, Joins, DB> &
      ProcessColumns<Rest, AliasMap, JoinTypeMap, Joins, DB>
  : {};

// =============================================================================
// MAIN QUERY TYPE RESOLVER
// =============================================================================

/**
 * Main type that resolves a SQL query string to its result type
 */
export type JoinQueryResult<
  SQL extends string,
  DB extends DatabaseSchema
> = ParseSelectStatement<SQL> extends {
  columns: infer Cols extends string[];
  from: infer From extends TableRef;
  joins: infer Joins extends JoinClause[];
}
  ? ProcessColumns<
      Cols,
      BuildAliasMap<From, Joins>,
      BuildJoinTypeMap<From, Joins>,
      Joins,
      DB
    >[]
  : { error: 'Failed to parse query'; sql: SQL };

// =============================================================================
// TYPE-SAFE DATABASE INTERFACE
// =============================================================================

export interface TypedDatabase<DB extends DatabaseSchema> {
  sql<SQL extends string>(
    strings: TemplateStringsArray,
    ...values: unknown[]
  ): Promise<JoinQueryResult<SQL, DB>>;

  // Alternative: direct string query
  query<SQL extends string>(sql: SQL): Promise<JoinQueryResult<SQL, DB>>;
}

/**
 * Create a typed database instance
 */
export function createTypedDatabase<DB extends DatabaseSchema>(): TypedDatabase<DB> {
  return {
    async sql(strings, ...values) {
      const sql = strings.reduce((acc, str, i) =>
        acc + str + (values[i] !== undefined ? `$${i + 1}` : ''), ''
      );
      console.log('Executing:', sql);
      return [] as any;
    },
    async query(sql) {
      console.log('Executing:', sql);
      return [] as any;
    }
  };
}

// =============================================================================
// EXPORTED TEST HELPERS (for type-level testing)
// =============================================================================

// Export internal types for testing (JoinType, TableRef, JoinClause already exported above)
export type {
  ParseSelectStatement,
  ParseTableRef,
  ParseFromTable,
  ExtractAllJoins,
  BuildAliasMap,
  BuildJoinTypeMap,
  ResolveColumnRef,
  ProcessColumns,
  SelectStarResult,
  ParseJoinType,
  ParseSingleJoin,
};
