/**
 * Blaze Node.js Quickstart
 *
 * Minimal example showing core API usage: create tables, query, load files.
 *
 * Run with: node quickstart.mjs
 */

import { BlazeDB } from '@blaze-sql/node';

async function main() {
  const db = await BlazeDB.create();

  // Create a table and insert data
  db.execute(`
    CREATE TABLE employees (
      id INT NOT NULL,
      name VARCHAR NOT NULL,
      department VARCHAR,
      salary INT
    )
  `);
  db.execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 120000)");
  db.execute("INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 110000)");
  db.execute("INSERT INTO employees VALUES (3, 'Carol', 'Marketing', 95000)");

  // Basic query
  console.log('=== All Employees ===');
  const all = db.query('SELECT * FROM employees ORDER BY id');
  console.table(all.rows);

  // Aggregation
  console.log('\n=== Avg Salary by Department ===');
  const agg = db.query(`
    SELECT department, COUNT(*) AS headcount, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department
    ORDER BY avg_salary DESC
  `);
  console.table(agg.rows);

  // Load JSON data
  db.registerJson('products', [
    { id: 1, name: 'Widget', price: 9.99 },
    { id: 2, name: 'Gadget', price: 24.99 },
  ]);

  const products = db.query('SELECT * FROM products ORDER BY price DESC');
  console.log('\n=== Products ===');
  console.table(products.rows);

  // Load CSV file (if exists)
  // await db.registerCsv('sales', './sales.csv');
  // const sales = db.query('SELECT * FROM sales LIMIT 5');

  console.log(`\nStats: ${db.getStats().totalQueries} queries executed`);
  db.close();
}

main().catch(console.error);
