---
sidebar_position: 12
title: SQL Playground
description: Interactive SQL playground powered by Blaze WASM
---

# SQL Playground

Try Blaze directly in your browser! The playground runs entirely client-side using WebAssembly — no server required.

## Embedded Playground

<iframe
  src="/playground.html"
  width="100%"
  height="600"
  style={{border: '1px solid #313244', borderRadius: '12px'}}
  title="Blaze SQL Playground"
/>

## Sample Queries

### Basic SELECT

```sql
SELECT name, department, salary
FROM employees
WHERE salary > 50000
ORDER BY salary DESC
```

### Aggregation

```sql
SELECT
  department,
  COUNT(*) as employee_count,
  AVG(salary) as avg_salary
FROM employees
GROUP BY department
ORDER BY avg_salary DESC
```

### JOIN with Window Functions

```sql
SELECT
  e.name,
  e.department,
  e.salary,
  ROW_NUMBER() OVER (PARTITION BY e.department ORDER BY e.salary DESC) as rank
FROM employees e
JOIN departments d ON e.department = d.name
```

### CTE (Common Table Expressions)

```sql
WITH dept_stats AS (
  SELECT department, AVG(salary) as avg_salary
  FROM employees
  GROUP BY department
)
SELECT
  e.name,
  e.salary,
  ds.avg_salary,
  e.salary - ds.avg_salary as diff
FROM employees e
JOIN dept_stats ds ON e.department = ds.department
ORDER BY diff DESC
```

## Sharing Queries

Click the **Share** button in the playground to generate a URL with your query encoded. Share it with anyone — they'll see your exact query loaded when they open the link.

## Embedding in Your Docs

You can embed the Blaze playground in any webpage using an iframe:

```html
<iframe
  src="https://your-site.com/playground.html"
  width="100%"
  height="600"
  style="border: 1px solid #313244; border-radius: 12px;"
  title="Blaze SQL Playground"
></iframe>
```

You can also send queries programmatically:

```javascript
const iframe = document.querySelector('iframe');
iframe.contentWindow.postMessage({
  type: 'blaze-query',
  sql: 'SELECT * FROM employees LIMIT 5',
  autorun: true
}, '*');
```
