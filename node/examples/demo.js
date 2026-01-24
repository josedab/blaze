/**
 * Blaze SQL Node.js Demo
 *
 * This example demonstrates how to use the Blaze SQL query engine
 * in a Node.js application.
 *
 * Run with: node demo.js
 */

const { BlazeDB } = require('@blaze-sql/node');
const fs = require('fs');
const path = require('path');

async function main() {
  console.log('='.repeat(50));
  console.log('Blaze SQL - Node.js Demo');
  console.log('='.repeat(50));
  console.log();

  // Create a database connection
  const db = await BlazeDB.create({
    baseDir: __dirname,
  });

  // Sample data
  const employees = [
    { id: 1, name: 'Alice', department: 'Engineering', salary: 85000 },
    { id: 2, name: 'Bob', department: 'Engineering', salary: 92000 },
    { id: 3, name: 'Charlie', department: 'Sales', salary: 65000 },
    { id: 4, name: 'Diana', department: 'Marketing', salary: 70000 },
    { id: 5, name: 'Eve', department: 'Engineering', salary: 95000 },
  ];

  const departments = [
    { id: 1, name: 'Engineering', budget: 500000 },
    { id: 2, name: 'Sales', budget: 300000 },
    { id: 3, name: 'Marketing', budget: 200000 },
  ];

  // Register data
  console.log('Registering data...');
  await db.registerJson('employees', employees);
  await db.registerJson('departments', departments);

  console.log('Tables:', db.listTables());
  console.log();

  // Example 1: Basic SELECT
  console.log('1. Basic SELECT Query');
  console.log('-'.repeat(40));
  const result1 = db.query(`
    SELECT name, department, salary
    FROM employees
    WHERE salary > 70000
    ORDER BY salary DESC
  `);
  console.log(`Rows: ${result1.rowCount}`);
  console.table(result1.rows);
  console.log();

  // Example 2: Aggregation
  console.log('2. Aggregation Query');
  console.log('-'.repeat(40));
  const result2 = db.query(`
    SELECT
      department,
      COUNT(*) as employee_count,
      AVG(salary) as avg_salary,
      MAX(salary) as max_salary
    FROM employees
    GROUP BY department
    ORDER BY avg_salary DESC
  `);
  console.log(`Rows: ${result2.rowCount}`);
  console.table(result2.rows);
  console.log();

  // Example 3: JOIN
  console.log('3. JOIN Query');
  console.log('-'.repeat(40));
  const result3 = db.query(`
    SELECT
      e.name,
      e.salary,
      d.name as department,
      d.budget
    FROM employees e
    JOIN departments d ON e.department = d.name
    ORDER BY d.budget DESC
  `);
  console.log(`Rows: ${result3.rowCount}`);
  console.table(result3.rows);
  console.log();

  // Example 4: Window Functions
  console.log('4. Window Functions');
  console.log('-'.repeat(40));
  const result4 = db.query(`
    SELECT
      name,
      department,
      salary,
      ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank
    FROM employees
  `);
  console.log(`Rows: ${result4.rowCount}`);
  console.table(result4.rows);
  console.log();

  // Example 5: Statistics
  console.log('5. Database Statistics');
  console.log('-'.repeat(40));
  const stats = db.getStats();
  console.log(`Queries Executed: ${stats.queriesExecuted}`);
  console.log(`Total Query Time: ${stats.totalQueryTimeMs}ms`);
  console.log(`Table Count: ${stats.tableCount}`);
  console.log(`Total Rows: ${stats.totalRows}`);
  console.log();

  // Cleanup
  db.close();
  console.log('Demo complete!');
}

// Run the demo
main().catch(console.error);
