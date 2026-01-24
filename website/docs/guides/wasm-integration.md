---
sidebar_position: 7
---

# WASM Integration

Blaze can run in web browsers and Node.js via WebAssembly, enabling SQL queries on client-side data.

## Installation

```bash
npm install @blaze-sql/wasm
```

## Quick Start

### Browser

```typescript
import { initBlaze, BlazeDB } from '@blaze-sql/wasm';

// Initialize the WASM module (required once)
await initBlaze();

// Create a database connection
const db = new BlazeDB();

// Register data from JSON
db.registerJson('users', [
    { id: 1, name: 'Alice', age: 30 },
    { id: 2, name: 'Bob', age: 25 },
    { id: 3, name: 'Carol', age: 35 }
]);

// Execute queries
const result = db.query('SELECT * FROM users WHERE age > 25');
console.log(result.rows);
// [{ id: 1, name: 'Alice', age: 30 }, { id: 3, name: 'Carol', age: 35 }]
```

### With Bundlers (Vite, Webpack)

```typescript
// vite.config.ts
import { defineConfig } from 'vite';

export default defineConfig({
    optimizeDeps: {
        exclude: ['@blaze-sql/wasm']
    }
});
```

```typescript
// app.ts
import { initBlaze, BlazeDB } from '@blaze-sql/wasm';

async function main() {
    await initBlaze();
    const db = new BlazeDB();
    // ... use db
}
```

## API Reference

### Initialization

```typescript
// Initialize WASM module
await initBlaze();

// With custom WASM path (for CDN deployment)
await initBlaze('/assets/blaze_bg.wasm');
```

### BlazeDB

```typescript
class BlazeDB {
    // Create a new database
    constructor()

    // Register data as a table
    registerJson(name: string, data: object[]): void
    registerCsv(name: string, csvContent: string): void
    registerArrow(name: string, arrowData: Uint8Array): void

    // Query execution
    query(sql: string): QueryResult
    queryWithParams(sql: string, params: any[]): QueryResult

    // Table management
    listTables(): string[]
    getSchema(tableName: string): ColumnInfo[]
    dropTable(tableName: string): void

    // Resource cleanup
    close(): void
}
```

### QueryResult

```typescript
interface QueryResult {
    // Row data as JavaScript objects
    rows: Record<string, unknown>[]

    // Column metadata
    columns: ColumnInfo[]

    // Row count
    rowCount: number

    // Export to Arrow format
    toArrow(): Uint8Array

    // Export to JSON string
    toJson(): string
}

interface ColumnInfo {
    name: string
    dataType: string
    nullable: boolean
}
```

## Data Loading

### From JSON

```typescript
// Array of objects
db.registerJson('users', [
    { id: 1, name: 'Alice' },
    { id: 2, name: 'Bob' }
]);

// Nested data is flattened
db.registerJson('events', [
    { id: 1, user: { name: 'Alice' }, timestamp: '2024-01-15' }
]);
```

### From CSV String

```typescript
const csvData = `id,name,age
1,Alice,30
2,Bob,25
3,Carol,35`;

db.registerCsv('users', csvData);
```

### From Fetch

```typescript
// Load CSV from URL
const response = await fetch('/data/users.csv');
const csvText = await response.text();
db.registerCsv('users', csvText);

// Load Arrow from URL
const arrowResponse = await fetch('/data/users.arrow');
const arrowData = new Uint8Array(await arrowResponse.arrayBuffer());
db.registerArrow('users', arrowData);
```

### From File Input

```typescript
const input = document.querySelector('input[type="file"]');
input.addEventListener('change', async (e) => {
    const file = e.target.files[0];
    const text = await file.text();

    if (file.name.endsWith('.csv')) {
        db.registerCsv('uploaded', text);
    } else {
        const data = JSON.parse(text);
        db.registerJson('uploaded', data);
    }
});
```

## Query Examples

### Basic Queries

```typescript
// Simple SELECT
const users = db.query('SELECT * FROM users');

// With filtering
const adults = db.query('SELECT * FROM users WHERE age >= 18');

// With projection
const names = db.query('SELECT name FROM users');
```

### Aggregations

```typescript
const stats = db.query(`
    SELECT
        department,
        COUNT(*) as count,
        AVG(salary) as avg_salary,
        MAX(salary) as max_salary
    FROM employees
    GROUP BY department
    ORDER BY avg_salary DESC
`);
```

### Joins

```typescript
db.registerJson('orders', [
    { id: 1, user_id: 1, amount: 100 },
    { id: 2, user_id: 2, amount: 200 }
]);

const result = db.query(`
    SELECT u.name, SUM(o.amount) as total
    FROM users u
    LEFT JOIN orders o ON u.id = o.user_id
    GROUP BY u.name
`);
```

### Window Functions

```typescript
const ranked = db.query(`
    SELECT
        name,
        score,
        RANK() OVER (ORDER BY score DESC) as rank,
        PERCENT_RANK() OVER (ORDER BY score DESC) as percentile
    FROM players
`);
```

### Parameterized Queries

```typescript
const userId = 42;
const result = db.queryWithParams(
    'SELECT * FROM users WHERE id = $1',
    [userId]
);
```

## React Integration

```tsx
import { useState, useEffect } from 'react';
import { initBlaze, BlazeDB } from '@blaze-sql/wasm';

function SqlQueryComponent() {
    const [db, setDb] = useState<BlazeDB | null>(null);
    const [query, setQuery] = useState('SELECT * FROM users');
    const [results, setResults] = useState<any[]>([]);
    const [error, setError] = useState('');

    useEffect(() => {
        async function init() {
            await initBlaze();
            const database = new BlazeDB();
            database.registerJson('users', [
                { id: 1, name: 'Alice' },
                { id: 2, name: 'Bob' }
            ]);
            setDb(database);
        }
        init();
    }, []);

    const runQuery = () => {
        if (!db) return;
        try {
            const result = db.query(query);
            setResults(result.rows);
            setError('');
        } catch (e) {
            setError(e.message);
            setResults([]);
        }
    };

    return (
        <div>
            <textarea
                value={query}
                onChange={e => setQuery(e.target.value)}
            />
            <button onClick={runQuery}>Run</button>
            {error && <div className="error">{error}</div>}
            <table>
                {results.length > 0 && (
                    <>
                        <thead>
                            <tr>
                                {Object.keys(results[0]).map(col => (
                                    <th key={col}>{col}</th>
                                ))}
                            </tr>
                        </thead>
                        <tbody>
                            {results.map((row, i) => (
                                <tr key={i}>
                                    {Object.values(row).map((val, j) => (
                                        <td key={j}>{String(val)}</td>
                                    ))}
                                </tr>
                            ))}
                        </tbody>
                    </>
                )}
            </table>
        </div>
    );
}
```

## Vue Integration

```vue
<template>
    <div>
        <textarea v-model="query"></textarea>
        <button @click="runQuery">Run</button>
        <table v-if="results.length">
            <thead>
                <tr>
                    <th v-for="col in columns" :key="col">{{ col }}</th>
                </tr>
            </thead>
            <tbody>
                <tr v-for="(row, i) in results" :key="i">
                    <td v-for="col in columns" :key="col">{{ row[col] }}</td>
                </tr>
            </tbody>
        </table>
    </div>
</template>

<script setup>
import { ref, onMounted, computed } from 'vue';
import { initBlaze, BlazeDB } from '@blaze-sql/wasm';

const db = ref(null);
const query = ref('SELECT * FROM users');
const results = ref([]);

const columns = computed(() =>
    results.value.length ? Object.keys(results.value[0]) : []
);

onMounted(async () => {
    await initBlaze();
    db.value = new BlazeDB();
    db.value.registerJson('users', [
        { id: 1, name: 'Alice' },
        { id: 2, name: 'Bob' }
    ]);
});

function runQuery() {
    try {
        results.value = db.value.query(query.value).rows;
    } catch (e) {
        console.error(e);
    }
}
</script>
```

## Performance Tips

1. **Reuse BlazeDB instances**: Creating a new instance loads the schema
2. **Use Arrow format for large data**: More efficient than JSON
3. **Limit result sets**: Use `LIMIT` for large tables during exploration
4. **Register data once**: Avoid re-registering unchanged data

## Browser Compatibility

| Browser | Support |
|---------|---------|
| Chrome 89+ | ✅ |
| Firefox 89+ | ✅ |
| Safari 15+ | ✅ |
| Edge 89+ | ✅ |

## Bundle Size

- **WASM binary**: ~2.5 MB (gzipped: ~800 KB)
- **JavaScript glue**: ~50 KB

## Limitations

- No filesystem access (use `registerCsv`/`registerJson` instead)
- No COPY TO (use `toArrow()`/`toJson()` instead)
- Maximum data size limited by browser memory
- No persistent storage (data is in-memory only)
