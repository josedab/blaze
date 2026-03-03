# dbt-blaze

dbt adapter for the Blaze Query Engine.

## Installation

```bash
pip install dbt-blaze
```

## Configuration

Add to your `profiles.yml`:

```yaml
my_project:
  target: dev
  outputs:
    dev:
      type: blaze
      path: ':memory:'  # or path to database file
```

## Supported Features

- Table materializations (CREATE TABLE AS SELECT)
- View materializations (CREATE VIEW)
- Incremental models (INSERT INTO ... SELECT)
- Seeds (COPY FROM CSV)
- Schema tests
- Source freshness (via information_schema)

## Requirements

- Python 3.10+
- dbt-core >= 1.7
- pyblaze (Blaze Python bindings)
