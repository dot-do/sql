/**
 * Type-Level SQL Parser
 *
 * Parses SQL strings at compile time and infers result types.
 * Case-sensitive for identifiers (like ClickHouse, not SQLite).
 */

// =============================================================================
// UTILITY TYPES
// =============================================================================

type Whitespace = ' ' | '\n' | '\t' | '\r';

/** Trim whitespace from both ends */
type Trim<S extends string> =
  S extends `${Whitespace}${infer Rest}` ? Trim<Rest> :
  S extends `${infer Rest}${Whitespace}` ? Trim<Rest> :
  S;

/** Trim leading whitespace only */
type TrimStart<S extends string> =
  S extends `${Whitespace}${infer Rest}` ? TrimStart<Rest> : S;

/** Split string by delimiter */
type Split<S extends string, D extends string> =
  S extends `${infer Head}${D}${infer Tail}`
    ? [Trim<Head>, ...Split<Tail, D>]
    : S extends '' ? [] : [Trim<S>];

// =============================================================================
// SCHEMA TYPES
// =============================================================================

/** Supported column types in schema definitions */
export type ColumnType =
  | 'string'
  | 'number'
  | 'boolean'
  | 'Date'
  | 'null'
  | 'unknown';

/** Schema for a single table: column name -> column type */
export type TableSchema = Record<string, ColumnType>;

/** Database schema: table name -> table schema */
export type DatabaseSchema = Record<string, TableSchema>;

/** Convert our column type strings to actual TypeScript types */
type TSType<T extends ColumnType> =
  T extends 'string' ? string :
  T extends 'number' ? number :
  T extends 'boolean' ? boolean :
  T extends 'Date' ? Date :
  T extends 'null' ? null :
  unknown;

// =============================================================================
// SQL KEYWORD DETECTION (case-insensitive)
// =============================================================================

/** Check if string starts with SELECT (case-insensitive) */
type StartsWithSelect<S extends string> =
  Uppercase<S> extends `SELECT${infer _}` ? true : false;

/** Check if string contains FROM (case-insensitive) */
type ContainsFrom<S extends string> =
  Uppercase<S> extends `${infer _}FROM${infer __}` ? true : false;

// =============================================================================
// PARSE TABLE REFERENCE (case-sensitive for table names!)
// =============================================================================

/**
 * Parse table reference: "table", "table alias", or "table AS alias"
 * Table names are CASE-SENSITIVE
 */
type ParseTableRef<S extends string> =
  // Handle "table AS alias" (AS is case-insensitive)
  Uppercase<S> extends `${infer _Pre} AS ${infer _Post}`
    ? S extends `${infer Table} ${infer Rest}`
      ? Rest extends `${infer _AS} ${infer Alias}`
        ? { table: Trim<Table>; alias: Trim<Alias> }
        : { table: Trim<Table>; alias: Trim<Table> }
      : { table: Trim<S>; alias: Trim<S> }
  // Handle "table alias" (no AS keyword)
  : S extends `${infer Table} ${infer Alias}`
    ? Trim<Alias> extends ''
      ? { table: Trim<S>; alias: Trim<S> }
      // Check if alias looks like a keyword (WHERE, JOIN, etc.)
      : Uppercase<Trim<Alias>> extends `WHERE${string}` | `JOIN${string}` | `LEFT${string}` | `RIGHT${string}` | `INNER${string}` | `OUTER${string}` | `GROUP${string}` | `ORDER${string}` | `LIMIT${string}` | `HAVING${string}`
        ? { table: Trim<Table>; alias: Trim<Table> }
        : { table: Trim<Table>; alias: Trim<Alias> }
  // Just table name
  : { table: Trim<S>; alias: Trim<S> };

// =============================================================================
// EXTRACT FROM CLAUSE
// =============================================================================

/** Extract the table part after FROM, before any WHERE/JOIN/etc */
type ExtractFromTable<S extends string> =
  // Stop at WHERE
  Uppercase<S> extends `${infer TablePart} WHERE ${infer _}`
    ? Trim<ExtractCaseSensitivePart<S, 'WHERE'>>
  // Stop at JOIN
  : Uppercase<S> extends `${infer _TablePart} JOIN ${infer __}`
    ? Trim<ExtractCaseSensitivePart<S, 'JOIN'>>
  // Stop at LEFT JOIN
  : Uppercase<S> extends `${infer _TablePart} LEFT ${infer __}`
    ? Trim<ExtractCaseSensitivePart<S, 'LEFT'>>
  // Stop at RIGHT JOIN
  : Uppercase<S> extends `${infer _TablePart} RIGHT ${infer __}`
    ? Trim<ExtractCaseSensitivePart<S, 'RIGHT'>>
  // Stop at INNER JOIN
  : Uppercase<S> extends `${infer _TablePart} INNER ${infer __}`
    ? Trim<ExtractCaseSensitivePart<S, 'INNER'>>
  // Stop at GROUP BY
  : Uppercase<S> extends `${infer _TablePart} GROUP ${infer __}`
    ? Trim<ExtractCaseSensitivePart<S, 'GROUP'>>
  // Stop at ORDER BY
  : Uppercase<S> extends `${infer _TablePart} ORDER ${infer __}`
    ? Trim<ExtractCaseSensitivePart<S, 'ORDER'>>
  // Stop at LIMIT
  : Uppercase<S> extends `${infer _TablePart} LIMIT ${infer __}`
    ? Trim<ExtractCaseSensitivePart<S, 'LIMIT'>>
  // Stop at HAVING
  : Uppercase<S> extends `${infer _TablePart} HAVING ${infer __}`
    ? Trim<ExtractCaseSensitivePart<S, 'HAVING'>>
  // Otherwise, entire string is the table reference
  : Trim<S>;

/** Helper to extract case-sensitive part before a keyword */
type ExtractCaseSensitivePart<S extends string, Keyword extends string> =
  S extends `${infer Before} ${Keyword}${infer _}` ? Before :
  S extends `${infer Before} ${Lowercase<Keyword>}${infer _}` ? Before :
  S extends `${infer Before} ${Capitalize<Lowercase<Keyword>>}${infer _}` ? Before :
  // Fallback: find the space-separated version
  ExtractBeforeKeyword<S, Keyword>;

type ExtractBeforeKeyword<S extends string, Keyword extends string> =
  Uppercase<S> extends `${infer Upper} ${Uppercase<Keyword>}${infer _}`
    ? TakeChars<S, StringLength<Upper>>
    : S;

type StringLength<S extends string, Acc extends unknown[] = []> =
  S extends `${infer _}${infer Rest}` ? StringLength<Rest, [...Acc, unknown]> : Acc['length'];

type TakeChars<S extends string, N extends number, Acc extends string = '', Count extends unknown[] = []> =
  Count['length'] extends N ? Acc :
  S extends `${infer C}${infer Rest}` ? TakeChars<Rest, N, `${Acc}${C}`, [...Count, unknown]> : Acc;

// =============================================================================
// EXTRACT SELECT COLUMNS
// =============================================================================

/** Extract column list between SELECT and FROM (case-sensitive for column names!) */
type ExtractSelectColumns<S extends string> =
  Uppercase<S> extends `SELECT ${infer _} FROM ${infer __}`
    ? ExtractBetweenSelectAndFrom<S>
    : never;

/**
 * Extract the actual case-sensitive string between SELECT and FROM
 * This preserves the original case of column names
 */
type ExtractBetweenSelectAndFrom<S extends string> =
  S extends `${infer _Select}${SelectKeyword}${infer After}`
    ? After extends `${infer Cols}${FromKeyword}${infer _Rest}`
      ? Trim<Cols>
      : After extends `${infer Cols}${FromKeywordLower}${infer _Rest}`
        ? Trim<Cols>
        : never
    : never;

type SelectKeyword = 'SELECT ' | 'select ' | 'Select ';
type FromKeyword = ' FROM ' | ' from ' | ' From ';
type FromKeywordLower = ' FROM' | ' from' | ' From';

// Better approach: use pattern matching on uppercase but preserve original
type ExtractColumnsPart<S extends string> =
  // Match common patterns and extract with original case
  S extends `SELECT ${infer Cols} FROM ${infer _}` ? Trim<Cols> :
  S extends `select ${infer Cols} from ${infer _}` ? Trim<Cols> :
  S extends `Select ${infer Cols} From ${infer _}` ? Trim<Cols> :
  S extends `SELECT ${infer Cols} from ${infer _}` ? Trim<Cols> :
  S extends `select ${infer Cols} FROM ${infer _}` ? Trim<Cols> :
  // Fallback for other case combinations
  ExtractColumnsFallback<S>;

type ExtractColumnsFallback<S extends string> =
  Uppercase<S> extends `SELECT ${infer _ColsUpper} FROM ${infer __}`
    ? ExtractByPosition<S, 7> // Skip "SELECT "
    : never;

type ExtractByPosition<S extends string, Skip extends number, Count extends unknown[] = []> =
  Count['length'] extends Skip
    ? ExtractUntilFrom<S>
    : S extends `${infer _}${infer Rest}`
      ? ExtractByPosition<Rest, Skip, [...Count, unknown]>
      : never;

type ExtractUntilFrom<S extends string> =
  Uppercase<S> extends `${infer Before} FROM ${infer _}`
    ? TakeBeforeFrom<S>
    : S;

type TakeBeforeFrom<S extends string, Acc extends string = ''> =
  S extends `${infer C}${infer Rest}`
    ? Uppercase<`${Acc}${C}`> extends `${infer _} FROM`
      ? Trim<TrimEnd<Acc, ' FROM'>>
      : Uppercase<S> extends ` FROM${infer _}`
        ? Trim<Acc>
        : TakeBeforeFrom<Rest, `${Acc}${C}`>
    : Trim<Acc>;

type TrimEnd<S extends string, End extends string> =
  S extends `${infer Before}${End}` ? Before : S;

// =============================================================================
// EXTRACT FROM PART
// =============================================================================

/** Extract what comes after FROM */
type ExtractFromPart<S extends string> =
  S extends `${infer _}FROM ${infer Rest}` ? Trim<Rest> :
  S extends `${infer _}from ${infer Rest}` ? Trim<Rest> :
  S extends `${infer _}From ${infer Rest}` ? Trim<Rest> :
  ExtractFromFallback<S>;

type ExtractFromFallback<S extends string> =
  Uppercase<S> extends `${infer _} FROM ${infer Rest}`
    ? ExtractAfterFrom<S>
    : never;

type ExtractAfterFrom<S extends string, FoundFrom extends boolean = false> =
  FoundFrom extends true
    ? Trim<S>
    : S extends `${infer C}${infer Rest}`
      ? Uppercase<`${C}${FirstChars<Rest, 5>}`> extends `FROM ${infer _}`
        ? Trim<DropChars<Rest, 4>>
        : Uppercase<`${C}${FirstChars<Rest, 4>}`> extends ` FROM`
          ? ExtractAfterFrom<Rest, true>
          : ExtractAfterFrom<Rest, false>
      : never;

type FirstChars<S extends string, N extends number, Acc extends string = '', Count extends unknown[] = []> =
  Count['length'] extends N ? Acc :
  S extends `${infer C}${infer Rest}` ? FirstChars<Rest, N, `${Acc}${C}`, [...Count, unknown]> : Acc;

type DropChars<S extends string, N extends number, Count extends unknown[] = []> =
  Count['length'] extends N ? S :
  S extends `${infer _}${infer Rest}` ? DropChars<Rest, N, [...Count, unknown]> : S;

// =============================================================================
// PARSE SELECT STATEMENT - SIMPLIFIED APPROACH
// =============================================================================

/** Parse the complete SELECT statement */
type ParseSelect<S extends string, DB extends DatabaseSchema> =
  ParseSelectInternal<Trim<S>, DB>;

type ParseSelectInternal<S extends string, DB extends DatabaseSchema> =
  // Use a simpler, more reliable parsing approach
  S extends `SELECT ${infer Cols} FROM ${infer Rest}` ? BuildResult<Cols, Rest, DB> :
  S extends `select ${infer Cols} from ${infer Rest}` ? BuildResult<Cols, Rest, DB> :
  S extends `Select ${infer Cols} From ${infer Rest}` ? BuildResult<Cols, Rest, DB> :
  S extends `SELECT ${infer Cols} from ${infer Rest}` ? BuildResult<Cols, Rest, DB> :
  S extends `select ${infer Cols} FROM ${infer Rest}` ? BuildResult<Cols, Rest, DB> :
  // Try case-insensitive matching
  Uppercase<S> extends `SELECT ${infer _Cols} FROM ${infer _Rest}`
    ? ParseWithUppercaseMatch<S, DB>
    : { error: 'Invalid SELECT statement'; sql: S };

type ParseWithUppercaseMatch<S extends string, DB extends DatabaseSchema> =
  ExtractParts<S> extends { cols: infer Cols extends string; rest: infer Rest extends string }
    ? BuildResult<Cols, Rest, DB>
    : { error: 'Failed to extract parts'; sql: S };

type ExtractParts<S extends string> =
  S extends `${infer Pre}SELECT${infer _}` ? never :
  S extends `SELECT ${infer Cols} FROM ${infer Rest}` ? { cols: Cols; rest: Rest } :
  S extends `select ${infer Cols} from ${infer Rest}` ? { cols: Cols; rest: Rest } :
  never;

/** Build the result type from parsed columns and table */
type BuildResult<Cols extends string, Rest extends string, DB extends DatabaseSchema> =
  ParseTableRef<ExtractFromTable<Trim<Rest>>> extends { table: infer T extends string; alias: infer A extends string }
    ? T extends keyof DB
      ? ResolveColumns<Trim<Cols>, T, A, DB>
      : { error: `Table '${T}' not found in schema` }
    : { error: 'Failed to parse table reference' };

// =============================================================================
// RESOLVE COLUMNS
// =============================================================================

/** Resolve column list to result type */
type ResolveColumns<
  Cols extends string,
  Table extends keyof DB & string,
  Alias extends string,
  DB extends DatabaseSchema
> =
  Trim<Cols> extends '*'
    ? SelectStar<Table, DB>
    : SelectSpecificColumns<Cols, Table, Alias, DB>;

/** Handle SELECT * - return all columns from table */
type SelectStar<Table extends keyof DB & string, DB extends DatabaseSchema> = {
  [K in keyof DB[Table]]: TSType<DB[Table][K]>
};

/** Handle specific column selection using mapped type approach */
type SelectSpecificColumns<
  Cols extends string,
  Table extends keyof DB & string,
  _Alias extends string,
  DB extends DatabaseSchema
> = ColumnsToResult<Split<Cols, ','>, Table, DB>;

/** Convert array of column names to result object type */
type ColumnsToResult<
  Cols extends string[],
  Table extends keyof DB & string,
  DB extends DatabaseSchema
> = {
  [K in Cols[number] as GetOutputName<K, Table, DB>]: GetColumnType<K, Table, DB>
};

/** Get the output name for a column (handles aliases) */
type GetOutputName<
  Col extends string,
  Table extends keyof DB & string,
  DB extends DatabaseSchema
> =
  // Handle "column AS alias" (case-insensitive AS)
  Uppercase<Col> extends `${infer _} AS ${infer __}`
    ? Col extends `${infer _Expr} ${infer Rest}`
      ? Rest extends `${infer _AS} ${infer Alias}`
        ? Trim<Alias>
        : Col
      : Col
  // Handle "alias.column"
  : Col extends `${infer _Prefix}.${infer Column}`
    ? Trim<Column>
  // Just column name
  : Col;

/** Get the TypeScript type for a column */
type GetColumnType<
  Col extends string,
  Table extends keyof DB & string,
  DB extends DatabaseSchema
> =
  // Handle "column AS alias" (case-insensitive AS)
  Uppercase<Col> extends `${infer _} AS ${infer __}`
    ? Col extends `${infer Expr} ${infer _Rest}`
      ? Trim<Expr> extends keyof DB[Table]
        ? TSType<DB[Table][Trim<Expr>]>
        : unknown
      : unknown
  // Handle "alias.column"
  : Col extends `${infer _Prefix}.${infer Column}`
    ? Trim<Column> extends keyof DB[Table]
      ? TSType<DB[Table][Trim<Column>]>
      : unknown
  // Just column name
  : Col extends keyof DB[Table]
    ? TSType<DB[Table][Col]>
    : unknown;


// =============================================================================
// UTILITY: Union to Intersection
// =============================================================================

type UnionToIntersection<U> =
  (U extends unknown ? (k: U) => void : never) extends ((k: infer I) => void) ? I : never;

// =============================================================================
// MAIN QUERY TYPE RESOLVER
// =============================================================================

/**
 * Resolve SQL query string to result type
 *
 * @template SQL - The SQL query string (as a string literal type)
 * @template DB - The database schema
 */
export type QueryResult<
  SQL extends string,
  DB extends DatabaseSchema
> = ParseSelect<SQL, DB> extends infer R
  ? R extends { error: string }
    ? R
    : R[]
  : never;

// =============================================================================
// TYPED SQL TEMPLATE TAG
// =============================================================================

/**
 * A typed database interface that provides compile-time SQL validation
 */
export interface TypedDatabase<DB extends DatabaseSchema> {
  /**
   * Execute a SQL query with type inference
   *
   * @example
   * ```typescript
   * const result = await db.sql`SELECT id, name FROM users`;
   * // result is typed as { id: number; name: string }[]
   * ```
   */
  sql<SQL extends string>(
    strings: TemplateStringsArray & { raw: readonly [SQL] },
    ...values: never[]
  ): Promise<QueryResult<SQL, DB>>;
}

/**
 * Create a typed database instance
 *
 * @template DB - The database schema type
 * @returns A typed database instance
 *
 * @example
 * ```typescript
 * interface MyDB {
 *   users: { id: 'number'; name: 'string'; email: 'string' };
 * }
 *
 * const db = createDatabase<MyDB>();
 * const users = await db.sql`SELECT * FROM users`;
 * // users is typed as { id: number; name: string; email: string }[]
 * ```
 */
export function createDatabase<DB extends DatabaseSchema>(): TypedDatabase<DB> {
  return {
    async sql(_strings, ..._values) {
      // Runtime implementation would execute the query against actual database
      // For now, this is just the type-level implementation
      throw new Error('Not implemented: runtime SQL execution');
    }
  };
}

// =============================================================================
// ALTERNATIVE: Direct query function
// =============================================================================

/**
 * Type helper to infer query result type
 * Use this for type testing without runtime execution
 */
export type SQL<Query extends string, DB extends DatabaseSchema> = QueryResult<Query, DB>;

/**
 * Helper to create a typed query function
 */
export function createQuery<DB extends DatabaseSchema>() {
  return function query<Q extends string>(_sql: Q): QueryResult<Q, DB> {
    return undefined as unknown as QueryResult<Q, DB>;
  };
}
