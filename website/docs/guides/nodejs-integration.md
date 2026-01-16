---
sidebar_position: 8
---

# Node.js Integration

The `@blaze-sql/node` package provides a Node.js wrapper around Blaze with native filesystem support.

## Installation

```bash
npm install @blaze-sql/node
```

## Quick Start

```typescript
import { BlazeDB } from '@blaze-sql/node';

async function main() {
    // Create database
    const db = await BlazeDB.create();

    // Register files
    await db.registerCsv('users', './data/users.csv');
    await db.registerParquet('orders', './data/orders.parquet');

    // Execute queries
    const result = db.query(`
        SELECT u.name, COUNT(o.id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.id = o.user_id
        GROUP BY u.name
    `);

    console.log(result.rows);
}

main();
```

## API Reference

### BlazeDB

```typescript
class BlazeDB {
    // Create a new database instance
    static async create(): Promise<BlazeDB>

    // File registration
    registerCsv(name: string, path: string): Promise<void>
    registerParquet(name: string, path: string): Promise<void>
    registerJson(name: string, data: object[]): void
    registerArrow(name: string, buffer: Buffer): void

    // Query execution
    query(sql: string): QueryResult
    queryWithParams(sql: string, params: any[]): QueryResult
    execute(sql: string): void

    // Table management
    listTables(): string[]
    getSchema(tableName: string): ColumnInfo[]
    dropTable(tableName: string): void

    // Export
    exportToParquet(query: string, path: string): Promise<void>
    exportToCsv(query: string, path: string): Promise<void>
    exportToJson(query: string, path: string): Promise<void>

    // Cleanup
    close(): void
}
```

### QueryResult

```typescript
interface QueryResult {
    rows: Record<string, unknown>[]
    columns: ColumnInfo[]
    rowCount: number
    toArrow(): Buffer
    toJson(): string
}
```

## File Operations

### CSV Files

```typescript
// Load CSV file
await db.registerCsv('users', './users.csv');

// With options
await db.registerCsv('data', './data.csv', {
    delimiter: ';',
    hasHeader: true,
    nullValue: 'NA'
});
```

### Parquet Files

```typescript
// Load Parquet file
await db.registerParquet('events', './events.parquet');

// Load multiple Parquet files (partitioned data)
await db.registerParquet('logs', './logs/*.parquet');
```

### JSON Data

```typescript
// From array
db.registerJson('config', [
    { key: 'timeout', value: 30 },
    { key: 'retries', value: 3 }
]);

// From file
import { readFileSync } from 'fs';
const data = JSON.parse(readFileSync('./data.json', 'utf-8'));
db.registerJson('imported', data);
```

## Export Operations

### Export to Parquet

```typescript
await db.exportToParquet(
    'SELECT * FROM users WHERE active = true',
    './output/active_users.parquet'
);
```

### Export to CSV

```typescript
await db.exportToCsv(
    'SELECT id, name, email FROM users',
    './output/users.csv'
);
```

### Export to JSON

```typescript
await db.exportToJson(
    'SELECT * FROM analytics',
    './output/analytics.json'
);
```

## CLI Usage

The package includes a CLI for quick queries:

```bash
# Interactive REPL
npx blaze-node

# Execute query
npx blaze-node -c "SELECT 1 + 1"

# Query files
npx blaze-node -c "SELECT * FROM './data.csv' LIMIT 10"

# Output to file
npx blaze-node -c "SELECT * FROM users" -o results.json
```

## Integration Examples

### Express API

```typescript
import express from 'express';
import { BlazeDB } from '@blaze-sql/node';

const app = express();
let db: BlazeDB;

async function init() {
    db = await BlazeDB.create();
    await db.registerCsv('products', './data/products.csv');
}

app.get('/api/products', (req, res) => {
    const { category, minPrice } = req.query;
    const result = db.queryWithParams(
        `SELECT * FROM products
         WHERE ($1 IS NULL OR category = $1)
         AND ($2 IS NULL OR price >= $2)`,
        [category || null, minPrice ? Number(minPrice) : null]
    );
    res.json(result.rows);
});

app.get('/api/products/:id', (req, res) => {
    const result = db.queryWithParams(
        'SELECT * FROM products WHERE id = $1',
        [Number(req.params.id)]
    );
    if (result.rows.length === 0) {
        res.status(404).json({ error: 'Not found' });
    } else {
        res.json(result.rows[0]);
    }
});

init().then(() => {
    app.listen(3000, () => console.log('Server running on port 3000'));
});
```

### Data Pipeline

```typescript
import { BlazeDB } from '@blaze-sql/node';
import { glob } from 'glob';

async function processLogs() {
    const db = await BlazeDB.create();

    // Load all log files
    const logFiles = await glob('./logs/**/*.parquet');
    for (const file of logFiles) {
        await db.registerParquet('logs', file);
    }

    // Aggregate daily stats
    const dailyStats = db.query(`
        SELECT
            DATE_TRUNC('day', timestamp) as date,
            level,
            COUNT(*) as count,
            COUNT(DISTINCT user_id) as unique_users
        FROM logs
        WHERE timestamp >= CURRENT_DATE - 7
        GROUP BY DATE_TRUNC('day', timestamp), level
        ORDER BY date, level
    `);

    // Export results
    await db.exportToParquet(
        dailyStats.toArrow(),
        './output/daily_stats.parquet'
    );

    console.log('Processed', dailyStats.rowCount, 'summary rows');
    db.close();
}
```

### Batch Processing

```typescript
import { BlazeDB } from '@blaze-sql/node';

async function batchProcess(inputFiles: string[], outputDir: string) {
    const db = await BlazeDB.create();

    for (const file of inputFiles) {
        const tableName = path.basename(file, '.csv');
        await db.registerCsv(tableName, file);

        // Process
        const result = db.query(`
            SELECT
                *,
                ROW_NUMBER() OVER (ORDER BY id) as row_num
            FROM ${tableName}
            WHERE status = 'active'
        `);

        // Export
        await db.exportToParquet(
            `SELECT * FROM ${tableName} WHERE status = 'active'`,
            path.join(outputDir, `${tableName}.parquet`)
        );

        db.dropTable(tableName);
    }

    db.close();
}
```

### Streaming Large Results

```typescript
import { BlazeDB } from '@blaze-sql/node';
import { createWriteStream } from 'fs';

async function streamResults() {
    const db = await BlazeDB.create();
    await db.registerParquet('large_table', './large_data.parquet');

    // For very large results, export directly to file
    await db.exportToCsv(
        'SELECT * FROM large_table',
        './output/large_output.csv'
    );

    db.close();
}
```

## TypeScript Support

The package includes TypeScript definitions:

```typescript
import { BlazeDB, QueryResult, ColumnInfo } from '@blaze-sql/node';

interface User {
    id: number;
    name: string;
    email: string;
}

async function getUsers(): Promise<User[]> {
    const db = await BlazeDB.create();
    await db.registerCsv('users', './users.csv');

    const result: QueryResult = db.query('SELECT * FROM users');
    return result.rows as User[];
}
```

## Error Handling

```typescript
import { BlazeDB, BlazeError } from '@blaze-sql/node';

try {
    const db = await BlazeDB.create();
    const result = db.query('SELECT * FROM nonexistent');
} catch (error) {
    if (error instanceof BlazeError) {
        console.error('SQL Error:', error.message);
        console.error('Error Code:', error.code);
    } else {
        throw error;
    }
}
```

## Configuration

```typescript
const db = await BlazeDB.create({
    batchSize: 8192,      // Vectorized batch size
    memoryLimit: 1e9,     // 1 GB memory limit
    numThreads: 4         // Worker threads
});
```

## Performance Tips

1. **Use Parquet for large files**: 10-100x faster than CSV
2. **Register once, query many times**: Avoid re-registering unchanged data
3. **Use prepared statements**: For repeated queries with different parameters
4. **Project only needed columns**: `SELECT a, b` is faster than `SELECT *`
5. **Export directly**: Use `exportToParquet()` instead of reading into memory

## Comparison with WASM

| Feature | @blaze-sql/node | @blaze-sql/wasm |
|---------|----------------|-----------------|
| Filesystem access | ✅ Native | ❌ None |
| Large file support | ✅ Streaming | ⚠️ Memory limited |
| COPY TO | ✅ Yes | ❌ Use toArrow() |
| Performance | ✅ Native | ⚠️ WASM overhead |
| Browser support | ❌ No | ✅ Yes |
