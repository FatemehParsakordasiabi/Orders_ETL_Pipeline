from __future__ import annotations
import typer
from typing import Optional
import pandas as pd

from .extract import extract_orders
from .transform import transform_orders
from .load import load_to_sqlite
from .logging_config import get_logger

app = typer.Typer(help="ETL pipeline: orders -> SQLite warehouse")


@app.command()
def run(
    source: str = typer.Option(..., help="CSV path or URL"),
    db: str = typer.Option("data/warehouse.db", help="SQLite database path"),
    chunksize: Optional[int] = typer.Option(None, help="Chunk size for large CSV"),
    min_date: Optional[str] = typer.Option(None, help="Filter orders on/after YYYY-MM-DD"),
):
    """Run the full pipeline: extract -> transform -> load."""
    log = get_logger()
    log.info("Extracting from %s", source)
    raw = extract_orders(source, chunksize=chunksize)
    log.info("Extracted %d rows", len(raw))

    log.info("Transforming...")
    fact, dim = transform_orders(raw, min_date=min_date)
    log.info("fact_orders=%d rows, dim_customers=%d rows", len(fact), len(dim))

    log.info("Loading to %s", db)
    load_to_sqlite(fact, dim, db)
    log.info("DONE ✅")


@app.command()
def preview(source: str):
    """Show the first 5 rows of the source CSV (debug helper)."""
    df = extract_orders(source)
    typer.echo(df.head().to_string())


@app.command()
def query(db: str, sql: str):
    """Run a quick SQL query against the SQLite warehouse."""
    import sqlite3
    con = sqlite3.connect(db)
    try:
        cur = con.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        # Print with simple formatting
        for r in rows:
            typer.echo(str(r))
    finally:
        con.close()


if __name__ == "__main__":
    app()
