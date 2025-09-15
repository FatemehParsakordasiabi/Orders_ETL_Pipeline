# 🍰 End-to-End ETL Project — Orders -> SQLite Data Warehouse

This repository is a **production-style, end-to-end ETL** example you can fork or use as a template.
It demonstrates a clean Python ETL stack with a CLI, logging, tests, CI, and Docker.

## What it does

- **Extract**: reads raw orders data (CSV) from `data/raw/orders.csv`.
- **Transform**: cleans and validates rows, normalizes dates, computes `total_amount`, and builds
  simple **dim_customers** and **fact_orders** models.
- **Load**: writes dimension + fact tables to a **SQLite** warehouse at `data/warehouse.db` via SQLAlchemy.
- **Run**: single-command CLI powered by Typer.
- **Test**: unit tests for core transform logic with pytest.
- **CI**: GitHub Actions workflow runs tests on every push.
- **Container**: Dockerfile to run the pipeline in a container.

> Tech: Python 3.11, pandas, SQLAlchemy, Typer, Pytest

---

## Quickstart

### 1) Create and activate a virtualenv
```bash
python -m venv .venv
# Windows PowerShell
. .venv/Scripts/Activate.ps1
# macOS/Linux
source .venv/bin/activate
```

### 2) Install dependencies
```bash
pip install -r requirements.txt
```

### 3) Run the whole pipeline (extract -> transform -> load)
```bash
python -m etl.cli run   --source data/raw/orders.csv   --db data/warehouse.db
```

You can also pass a HTTP(S) URL for `--source` and the pipeline will download it.

### 4) Inspect the loaded data
```bash
python -m etl.cli query --db data/warehouse.db --sql "SELECT * FROM fact_orders LIMIT 5;"
python -m etl.cli query --db data/warehouse.db --sql "SELECT * FROM dim_customers;"
```

### 5) Run tests
```bash
pytest -q
```

### 6) Build & run with Docker
```bash
# Build image
docker build -t orders-etl:latest .

# Run
docker run --rm -v $PWD/data:/app/data orders-etl:latest   python -m etl.cli run --source data/raw/orders.csv --db data/warehouse.db
```

> On Windows PowerShell, use `${PWD}/data` instead of `$PWD/data`.

---

## Project structure

```
.
├── data/
│   ├── raw/
│   │   └── orders.csv
│   └── processed/               # optional: intermediate outputs
├── src/
│   └── etl/
│       ├── __init__.py
│       ├── cli.py               # Typer CLI entrypoint
│       ├── extract.py
│       ├── transform.py
│       ├── load.py
│       └── logging_config.py
├── tests/
│   └── test_transform.py
├── .github/workflows/ci.yml     # run tests on push
├── Dockerfile
├── Makefile
├── requirements.txt
└── README.md
```

---

## Configuration & Parameters

Most knobs are exposed as CLI flags:
- `--source`: CSV path or URL to raw orders data
- `--db`: SQLite database path
- `--chunksize`: rows per chunk during extract (for large files)
- `--min_date`: optional ISO date (YYYY-MM-DD) to filter orders on/after this date

---

## Data model

### `dim_customers`
- `customer_id` (PK)
- `city`
- `country`
- `first_order_date`
- `last_order_date`
- `order_count`
- `lifetime_value`

### `fact_orders`
- `order_id` (PK)
- `customer_id` (FK to dim_customers)
- `order_date` (DATE)
- `item`
- `quantity`
- `price`
- `total_amount`

---

## Example analytics queries

```sql
-- Top 5 customers by lifetime value
SELECT customer_id, lifetime_value
FROM dim_customers
ORDER BY lifetime_value DESC
LIMIT 5;

-- Daily revenue
SELECT order_date, SUM(total_amount) AS revenue
FROM fact_orders
GROUP BY order_date
ORDER BY order_date;
```

---

## Extending the project

- Swap SQLite for Postgres by changing the SQLAlchemy engine URL.
- Schedule with Airflow/Prefect/Dagster by calling the CLI in a task.
- Validate inputs with Great Expectations (add a checkpoint before load).
- Add dbt for richer modeling downstream.

---

## License

MIT
