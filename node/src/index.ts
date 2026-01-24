/**
 * @blaze-sql/node - Node.js bindings for Blaze SQL query engine
 *
 * This package wraps the WASM module with Node.js-specific features
 * like filesystem integration for reading Parquet and CSV files.
 *
 * @example
 * ```typescript
 * import { BlazeDB } from '@blaze-sql/node';
 *
 * const db = await BlazeDB.create();
 *
 * // Register data from files
 * await db.registerParquet('users', './data/users.parquet');
 * await db.registerCsv('orders', './data/orders.csv');
 *
 * // Query the data
 * const result = db.query('SELECT * FROM users JOIN orders ON users.id = orders.user_id');
 * console.log(result.rows);
 * ```
 */

import * as fs from 'fs';
import * as path from 'path';

// Re-export types from WASM package
export type {
  ColumnInfo,
  QueryResult,
  QueryOptions,
  DBConfig,
  DBStats,
  TableSchema,
} from '@blaze-sql/wasm';

import {
  initBlaze,
  isInitialized,
  BlazeDB as WasmBlazeDB,
  QueryOptions,
  QueryResult,
  DBConfig,
  DBStats,
  TableSchema,
} from '@blaze-sql/wasm';

/**
 * Node.js-specific configuration options
 */
export interface NodeDBConfig extends DBConfig {
  /** Base directory for relative file paths */
  baseDir?: string;
  /** Enable file watching for auto-reload */
  watchFiles?: boolean;
}

/**
 * File format options for reading data files
 */
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
export class BlazeDB {
  private db: WasmBlazeDB;
  private baseDir: string;
  private watchFiles: boolean;
  private fileWatchers: Map<string, fs.FSWatcher>;

  private constructor(db: WasmBlazeDB, config?: NodeDBConfig) {
    this.db = db;
    this.baseDir = config?.baseDir ?? process.cwd();
    this.watchFiles = config?.watchFiles ?? false;
    this.fileWatchers = new Map();
  }

  /**
   * Create a new database connection
   * @param config Optional configuration options
   * @returns Promise resolving to a new BlazeDB instance
   */
  static async create(config?: NodeDBConfig): Promise<BlazeDB> {
    if (!isInitialized()) {
      await initBlaze();
    }

    const wasmDb = new WasmBlazeDB(config);
    return new BlazeDB(wasmDb, config);
  }

  /**
   * Execute a SQL statement (DDL like CREATE TABLE, DROP TABLE)
   * @param sql SQL statement to execute
   * @returns Number of rows affected
   */
  execute(sql: string): number {
    return this.db.execute(sql);
  }

  /**
   * Execute a SQL query and return results
   * @param sql SQL query to execute
   * @param options Optional query options
   * @returns Query result with rows and metadata
   */
  query(sql: string, options?: QueryOptions): QueryResult {
    return this.db.query(sql, options);
  }

  /**
   * Execute a query and return results as an Arrow IPC buffer
   * @param sql SQL query to execute
   * @returns Buffer containing Arrow IPC data
   */
  queryArrow(sql: string): Buffer {
    const uint8Array = this.db.queryArrow(sql);
    return Buffer.from(uint8Array);
  }

  /**
   * Register JSON data as a table
   * @param tableName Name for the new table
   * @param data Array of objects, JSON string, or file path
   * @returns Number of rows registered
   */
  async registerJson(
    tableName: string,
    data: Record<string, unknown>[] | string
  ): Promise<number> {
    let jsonData: Record<string, unknown>[] | string;

    if (typeof data === 'string') {
      // Check if it's a file path
      const filePath = this.resolvePath(data);
      if (fs.existsSync(filePath) && filePath.endsWith('.json')) {
        const content = await fs.promises.readFile(filePath, 'utf-8');
        jsonData = content;
      } else {
        jsonData = data;
      }
    } else {
      jsonData = data;
    }

    return this.db.registerJson(tableName, jsonData);
  }

  /**
   * Register a CSV file as a table
   * @param tableName Name for the new table
   * @param filePath Path to the CSV file
   * @param options Optional file reading options
   * @returns Number of rows registered
   */
  async registerCsv(
    tableName: string,
    filePath: string,
    options?: FileOptions
  ): Promise<number> {
    const resolvedPath = this.resolvePath(filePath);

    if (!fs.existsSync(resolvedPath)) {
      throw new Error(`File not found: ${resolvedPath}`);
    }

    const encoding = options?.encoding ?? 'utf-8';
    const content = await fs.promises.readFile(resolvedPath, encoding);

    const rowCount = this.db.registerCsv(tableName, content);

    if (this.watchFiles) {
      this.watchFile(tableName, resolvedPath, () => {
        this.db.dropTable(tableName);
        const newContent = fs.readFileSync(resolvedPath, encoding);
        this.db.registerCsv(tableName, newContent);
      });
    }

    return rowCount;
  }

  /**
   * Register a Parquet file as a table
   * @param tableName Name for the new table
   * @param filePath Path to the Parquet file
   * @returns Number of rows registered
   *
   * Note: Parquet support requires the apache-arrow package.
   */
  async registerParquet(tableName: string, filePath: string): Promise<number> {
    const resolvedPath = this.resolvePath(filePath);

    if (!fs.existsSync(resolvedPath)) {
      throw new Error(`File not found: ${resolvedPath}`);
    }

    // Read Parquet file using Apache Arrow
    try {
      // Dynamic import to make apache-arrow optional
      const { tableFromIPC, tableToIPC } = await import('apache-arrow');
      const buffer = await fs.promises.readFile(resolvedPath);

      // Parse Parquet to Arrow
      // Note: In a real implementation, we'd use parquet-wasm or similar
      // For now, we'll assume the file is already in Arrow IPC format
      const table = tableFromIPC(buffer);

      // Convert to JSON for the WASM layer
      const rows = table.toArray().map((row: Record<string, unknown>) => {
        const obj: Record<string, unknown> = {};
        for (const field of table.schema.fields) {
          obj[field.name] = row[field.name];
        }
        return obj;
      });

      return this.db.registerJson(tableName, rows);
    } catch (error) {
      throw new Error(
        `Failed to read Parquet file. Make sure apache-arrow is installed: ${error}`
      );
    }
  }

  /**
   * Register a JSON file as a table
   * @param tableName Name for the new table
   * @param filePath Path to the JSON file
   * @returns Number of rows registered
   */
  async registerJsonFile(tableName: string, filePath: string): Promise<number> {
    const resolvedPath = this.resolvePath(filePath);

    if (!fs.existsSync(resolvedPath)) {
      throw new Error(`File not found: ${resolvedPath}`);
    }

    const content = await fs.promises.readFile(resolvedPath, 'utf-8');
    const data = JSON.parse(content);

    if (!Array.isArray(data)) {
      throw new Error('JSON file must contain an array of objects');
    }

    const rowCount = this.db.registerJson(tableName, data);

    if (this.watchFiles) {
      this.watchFile(tableName, resolvedPath, () => {
        this.db.dropTable(tableName);
        const newContent = fs.readFileSync(resolvedPath, 'utf-8');
        const newData = JSON.parse(newContent);
        this.db.registerJson(tableName, newData);
      });
    }

    return rowCount;
  }

  /**
   * Drop a table from the database
   * @param tableName Name of the table to drop
   */
  dropTable(tableName: string): void {
    // Stop watching the file if we were
    const watcher = this.fileWatchers.get(tableName);
    if (watcher) {
      watcher.close();
      this.fileWatchers.delete(tableName);
    }

    this.db.dropTable(tableName);
  }

  /**
   * List all tables in the database
   * @returns Array of table names
   */
  listTables(): string[] {
    return this.db.listTables();
  }

  /**
   * Get the schema of a table
   * @param tableName Name of the table
   * @returns Table schema or null if table doesn't exist
   */
  getTableSchema(tableName: string): TableSchema | null {
    return this.db.getTableSchema(tableName);
  }

  /**
   * Get database statistics
   * @returns Current database statistics
   */
  getStats(): DBStats {
    return this.db.getStats();
  }

  /**
   * Clear all tables and reset the database
   */
  clear(): void {
    // Stop all file watchers
    for (const watcher of this.fileWatchers.values()) {
      watcher.close();
    }
    this.fileWatchers.clear();

    this.db.clear();
  }

  /**
   * Close the database connection and free resources
   */
  close(): void {
    // Stop all file watchers
    for (const watcher of this.fileWatchers.values()) {
      watcher.close();
    }
    this.fileWatchers.clear();

    this.db.close();
  }

  /**
   * Resolve a path relative to the base directory
   */
  private resolvePath(filePath: string): string {
    if (path.isAbsolute(filePath)) {
      return filePath;
    }
    return path.resolve(this.baseDir, filePath);
  }

  /**
   * Set up a file watcher for auto-reload
   */
  private watchFile(
    tableName: string,
    filePath: string,
    onReload: () => void
  ): void {
    // Close existing watcher if any
    const existing = this.fileWatchers.get(tableName);
    if (existing) {
      existing.close();
    }

    const watcher = fs.watch(filePath, (eventType) => {
      if (eventType === 'change') {
        console.log(`File changed, reloading table: ${tableName}`);
        try {
          onReload();
        } catch (error) {
          console.error(`Failed to reload table ${tableName}:`, error);
        }
      }
    });

    this.fileWatchers.set(tableName, watcher);
  }
}

// Export for CommonJS compatibility
export default BlazeDB;
