# @blaze-sql/wasm

WebAssembly bindings for the Blaze SQL query engine. Run SQL queries directly in your browser with full support for Arrow/Parquet data formats.

## Features

- **Full SQL:2016 Support**: SELECT, JOIN, GROUP BY, window functions, CTEs, and more
- **Arrow/Parquet Integration**: Native Apache Arrow support for efficient data handling
- **Zero Server Dependency**: All query processing happens in the browser
- **TypeScript Support**: Full type definitions included

## Installation

```bash
npm install @blaze-sql/wasm
# or
yarn add @blaze-sql/wasm
# or
pnpm add @blaze-sql/wasm
```

## Quick Start

```typescript
import { initBlaze, BlazeDB } from '@blaze-sql/wasm';

// Initialize the WASM module (required before creating connections)
await initBlaze();

// Create a database connection
const db = new BlazeDB();

// Register some data
db.registerJson('users', [
  { id: 1, name: 'Alice', age: 30 },
  { id: 2, name: 'Bob', age: 25 },
  { id: 3, name: 'Charlie', age: 35 },
]);

// Run a query
const result = db.query('SELECT * FROM users WHERE age > 25 ORDER BY age');
console.log(result.rows);
// [
//   { id: 1, name: 'Alice', age: 30 },
//   { id: 3, name: 'Charlie', age: 35 }
// ]
```

## API Reference

### Initialization

```typescript
// Initialize the WASM module
await initBlaze();

// Check if initialized
if (isInitialized()) {
  // Ready to use
}
```

### Creating a Connection

```typescript
// Default configuration
const db = new BlazeDB();

// With custom configuration
const db = new BlazeDB({
  maxMemory: 256 * 1024 * 1024,    // 256 MB
  maxResultSize: 64 * 1024 * 1024, // 64 MB
  enableLogging: true,
});
```

### Registering Data

```typescript
// Register JSON data
db.registerJson('tableName', [
  { col1: 'value1', col2: 123 },
  { col1: 'value2', col2: 456 },
]);

// Register CSV data
db.registerCsv('tableName', `
id,name,value
1,Alice,100
2,Bob,200
`);
```

### Executing Queries

```typescript
// Execute DDL (CREATE TABLE, DROP TABLE, etc.)
db.execute('CREATE TABLE test (id INT, name VARCHAR)');

// Execute queries
const result = db.query('SELECT * FROM users');

// Query with options
const result = db.query('SELECT * FROM users', {
  maxRows: 100,
  format: 'json',
  includeSchema: true,
});

// Get results as Arrow IPC buffer
const arrowBuffer = db.queryArrow('SELECT * FROM users');
```

### Managing Tables

```typescript
// List all tables
const tables = db.listTables();
// ['users', 'orders', ...]

// Get table schema
const schema = db.getTableSchema('users');
// { columns: [{ name: 'id', type: 'Int64', nullable: false }, ...] }

// Drop a table
db.dropTable('users');

// Clear all tables
db.clear();
```

### Statistics

```typescript
const stats = db.getStats();
console.log(stats);
// {
//   queriesExecuted: 42,
//   totalQueryTimeMs: 1234,
//   memoryUsed: 12345678,
//   peakMemory: 23456789,
//   tableCount: 5,
//   totalRows: 10000,
// }
```

### Resource Management

```typescript
// Close the connection when done
db.close();
```

## Framework Integration

### React

```tsx
import { useEffect, useState } from 'react';
import { initBlaze, BlazeDB } from '@blaze-sql/wasm';

function useBlaze() {
  const [db, setDb] = useState<BlazeDB | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function init() {
      await initBlaze();
      setDb(new BlazeDB());
      setLoading(false);
    }
    init();

    return () => db?.close();
  }, []);

  return { db, loading };
}

function App() {
  const { db, loading } = useBlaze();

  if (loading) return <div>Loading...</div>;

  // Use db to execute queries
}
```

### Vue

```vue
<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue';
import { initBlaze, BlazeDB } from '@blaze-sql/wasm';

const db = ref<BlazeDB | null>(null);
const loading = ref(true);

onMounted(async () => {
  await initBlaze();
  db.value = new BlazeDB();
  loading.value = false;
});

onUnmounted(() => {
  db.value?.close();
});
</script>
```

## Bundler Configuration

### Vite

```typescript
// vite.config.ts
export default {
  optimizeDeps: {
    exclude: ['@blaze-sql/wasm'],
  },
  build: {
    target: 'esnext',
  },
};
```

### Webpack

```javascript
// webpack.config.js
module.exports = {
  experiments: {
    asyncWebAssembly: true,
  },
};
```

## Browser Compatibility

- Chrome 89+
- Firefox 89+
- Safari 15+
- Edge 89+

## Performance Tips

1. **Batch data registration**: Register all your data before running queries
2. **Use appropriate limits**: Set `maxRows` for large result sets
3. **Prefer Arrow format**: Use `queryArrow()` for large data transfers
4. **Close connections**: Call `close()` when done to free memory

## License

Apache-2.0
