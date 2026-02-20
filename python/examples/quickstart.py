"""PyBlaze quickstart example.

Demonstrates basic usage of the Blaze query engine from Python:
creating tables, inserting data, and running analytical queries.

Usage:
    pip install pyblaze
    python quickstart.py
"""

import pyblaze


def main():
    # Create an in-memory connection
    conn = pyblaze.connect()

    # Create tables
    conn.execute("""
        CREATE TABLE employees (
            id INT NOT NULL,
            name VARCHAR NOT NULL,
            department VARCHAR,
            salary INT
        )
    """)

    conn.execute("""
        CREATE TABLE departments (
            name VARCHAR NOT NULL,
            budget INT
        )
    """)

    # Insert sample data
    conn.execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 120000)")
    conn.execute("INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 110000)")
    conn.execute("INSERT INTO employees VALUES (3, 'Carol', 'Marketing', 95000)")
    conn.execute("INSERT INTO employees VALUES (4, 'Dave', 'Marketing', 90000)")
    conn.execute("INSERT INTO employees VALUES (5, 'Eve', 'Engineering', 130000)")

    conn.execute("INSERT INTO departments VALUES ('Engineering', 500000)")
    conn.execute("INSERT INTO departments VALUES ('Marketing', 200000)")

    # Basic query
    print("=== All Employees ===")
    result = conn.query("SELECT * FROM employees ORDER BY id")
    for row in result.to_dicts():
        print(row)

    # Aggregation
    print("\n=== Average Salary by Department ===")
    result = conn.query("""
        SELECT department, COUNT(*) AS headcount, AVG(salary) AS avg_salary
        FROM employees
        GROUP BY department
        ORDER BY avg_salary DESC
    """)
    for row in result.to_dicts():
        print(row)

    # JOIN query
    print("\n=== Department Budget vs Payroll ===")
    result = conn.query("""
        SELECT
            d.name AS department,
            d.budget,
            SUM(e.salary) AS total_payroll
        FROM departments d
        JOIN employees e ON d.name = e.department
        GROUP BY d.name, d.budget
    """)
    for row in result.to_dicts():
        print(row)

    # Window function
    print("\n=== Salary Rank Within Department ===")
    result = conn.query("""
        SELECT
            name,
            department,
            salary,
            RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank
        FROM employees
    """)
    for row in result.to_dicts():
        print(row)

    # Load from CSV file (if available)
    # conn = pyblaze.read_csv("data/sales.csv")
    # result = conn.query("SELECT * FROM sales LIMIT 5")


if __name__ == "__main__":
    main()
