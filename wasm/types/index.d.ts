/**
 * @blaze-sql/wasm - TypeScript type definitions
 */

/**
 * Column information for query results
 */
export interface ColumnInfo {
  /** Column name */
  name: string;
  /** Data type (e.g., "Int64", "Utf8", "Float64") */
  type: string;
  /** Whether the column allows null values */
  nullable: boolean;
}

/**
 * Query result from the database
 */
export interface QueryResult {
  /** Array of row objects */
  rows: Record<string, unknown>[];
  /** Column metadata */
  columns: ColumnInfo[];
  /** Number of rows returned */
  rowCount: number;
  /** Execution time in milliseconds */
  executionTimeMs: number;
}

/**
 * Query options for customizing result format
 */
export interface QueryOptions {
  /** Maximum number of rows to return */
  maxRows?: number;
  /** Output format: 'json' | 'csv' | 'arrow' */
  format?: 'json' | 'csv' | 'arrow';
  /** Include schema information in results */
  includeSchema?: boolean;
  /** Pretty print output (JSON only) */
  pretty?: boolean;
}

/**
 * Database configuration options
 */
export interface DBConfig {
  /** Maximum memory limit in bytes (default: 256MB) */
  maxMemory?: number;
  /** Maximum result size in bytes (default: 64MB) */
  maxResultSize?: number;
  /** Enable query logging */
  enableLogging?: boolean;
}

/**
 * Database statistics
 */
export interface DBStats {
  /** Total number of queries executed */
  queriesExecuted: number;
  /** Total query execution time in milliseconds */
  totalQueryTimeMs: number;
  /** Current memory usage in bytes */
  memoryUsed: number;
  /** Peak memory usage in bytes */
  peakMemory: number;
  /** Number of registered tables */
  tableCount: number;
  /** Total rows across all tables */
  totalRows: number;
}

/**
 * Table schema information
 */
export interface TableSchema {
  /** Array of column definitions */
  columns: ColumnInfo[];
}

/**
 * Initialize the Blaze WASM module.
 * Must be called before creating any BlazeDB instances.
 *
 * @example
 * ```typescript
 * await initBlaze();
 * const db = new BlazeDB();
 * ```
 */
export function initBlaze(): Promise<void>;

/**
 * Check if the WASM module has been initialized.
 * @returns true if initialized, false otherwise
 */
export function isInitialized(): boolean;

/**
 * BlazeDB - Main database connection class
 *
 * Provides a JavaScript-friendly interface to the Blaze SQL query engine
 * running in WebAssembly.
 */
export class BlazeDB {
  /**
   * Create a new database connection
   * @param config Optional configuration options
   * @throws Error if WASM module is not initialized
   */
  constructor(config?: DBConfig);

  /**
   * Execute a SQL statement (DDL like CREATE TABLE, DROP TABLE)
   * @param sql SQL statement to execute
   * @returns Number of rows affected
   * @throws Error on SQL syntax error or execution failure
   */
  execute(sql: string): number;

  /**
   * Execute a SQL query and return results
   * @param sql SQL query to execute
   * @param options Optional query options
   * @returns Query result with rows and metadata
   * @throws Error on SQL syntax error or execution failure
   */
  query(sql: string, options?: QueryOptions): QueryResult;

  /**
   * Execute a query and return results as an Arrow IPC buffer
   * @param sql SQL query to execute
   * @returns Uint8Array containing Arrow IPC data
   * @throws Error on SQL syntax error or execution failure
   */
  queryArrow(sql: string): Uint8Array;

  /**
   * Register JSON data as a table
   * @param tableName Name for the new table
   * @param data Array of objects or JSON string
   * @returns Number of rows registered
   * @throws Error on invalid JSON or registration failure
   */
  registerJson(tableName: string, data: Record<string, unknown>[] | string): number;

  /**
   * Register CSV data as a table
   * @param tableName Name for the new table
   * @param csvData CSV string with headers in first row
   * @returns Number of rows registered
   * @throws Error on invalid CSV or registration failure
   */
  registerCsv(tableName: string, csvData: string): number;

  /**
   * Drop a table from the database
   * @param tableName Name of the table to drop
   * @throws Error if table doesn't exist
   */
  dropTable(tableName: string): void;

  /**
   * List all tables in the database
   * @returns Array of table names
   */
  listTables(): string[];

  /**
   * Get the schema of a table
   * @param tableName Name of the table
   * @returns Table schema or null if table doesn't exist
   */
  getTableSchema(tableName: string): TableSchema | null;

  /**
   * Get database statistics
   * @returns Current database statistics
   */
  getStats(): DBStats;

  /**
   * Clear all tables and reset the database
   */
  clear(): void;

  /**
   * Close the database connection and free resources
   * After calling close(), the connection cannot be used.
   */
  close(): void;
}

export default BlazeDB;
