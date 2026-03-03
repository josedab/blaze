/**
 * TypeScript type definitions for @blaze-sql/node
 *
 * These types describe the public API surface of the Node.js bindings
 * for the Blaze SQL query engine.
 */

export { ColumnInfo, QueryResult, QueryOptions, DBConfig, DBStats, TableSchema } from '@blaze-sql/wasm';

import { QueryOptions, QueryResult, DBConfig, DBStats, TableSchema } from '@blaze-sql/wasm';

/** Node.js-specific configuration options */
export interface NodeDBConfig extends DBConfig {
  /** Base directory for relative file paths */
  baseDir?: string;
  /** Enable file watching for auto-reload */
  watchFiles?: boolean;
}

/** File format options for reading data files */
export interface FileOptions {
  /** File encoding (default: 'utf-8') */
  encoding?: BufferEncoding;
  /** CSV delimiter (default: ',') */
  delimiter?: string;
  /** CSV has header row (default: true) */
  header?: boolean;
}

/**
 * BlazeDB - Node.js database connection class
 *
 * Extends the WASM BlazeDB with filesystem integration for Node.js.
 * Supports reading Parquet, CSV, and JSON files directly.
 */
export declare class BlazeDB {
  private constructor(db: unknown, config?: NodeDBConfig);

  /** Create a new database connection */
  static create(config?: NodeDBConfig): Promise<BlazeDB>;

  /** Execute a SQL statement (DDL like CREATE TABLE, DROP TABLE) */
  execute(sql: string): number;

  /** Execute a SQL query and return results */
  query(sql: string, options?: QueryOptions): QueryResult;

  /** Execute a query and return results as an Arrow IPC buffer */
  queryArrow(sql: string): Buffer;

  /** Register JSON data as a table */
  registerJson(
    tableName: string,
    data: Record<string, unknown>[] | string
  ): Promise<number>;

  /** Register a CSV file as a table */
  registerCsv(
    tableName: string,
    filePath: string,
    options?: FileOptions
  ): Promise<number>;

  /** Register a Parquet file as a table */
  registerParquet(tableName: string, filePath: string): Promise<number>;

  /** Register a JSON file as a table */
  registerJsonFile(tableName: string, filePath: string): Promise<number>;

  /** Drop a table from the database */
  dropTable(tableName: string): void;

  /** List all tables in the database */
  listTables(): string[];

  /** Get the schema of a table */
  getTableSchema(tableName: string): TableSchema | null;

  /** Get database statistics */
  getStats(): DBStats;

  /** Clear all tables and reset the database */
  clear(): void;

  /** Close the database connection and free resources */
  close(): void;
}

export default BlazeDB;
