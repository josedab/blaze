# blaze-kernel

Jupyter kernel for the Blaze SQL Query Engine.

## Installation

```bash
pip install blaze-sql-kernel
python -m blaze_kernel.install
```

## Usage

After installation, select "Blaze SQL" from the Jupyter kernel dropdown.

```sql
-- Cells are executed as SQL
CREATE TABLE users (id INT, name VARCHAR);
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
SELECT * FROM users;
```

## Features

- SQL syntax execution
- Rich HTML table output
- Auto-completion for table/column names
- Error messages with suggestions
