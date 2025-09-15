from __future__ import annotations
import pandas as pd
from typing import Tuple, Optional
from datetime import datetime

REQUIRED_COLUMNS = [
    "order_id","customer_id","order_date","item","quantity","price","city","country"
]

def _coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["order_id"] = df["order_id"].astype(int)
    df["customer_id"] = df["customer_id"].astype(int)
    df["quantity"] = df["quantity"].astype(float)
    df["price"] = df["price"].astype(float)
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date
    return df

def _drop_bad_rows(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["order_id","customer_id","order_date","item","quantity","price"])
    df = df[df["quantity"] > 0]
    df = df[df["price"] >= 0]
    return df

def transform_orders(raw: pd.DataFrame, min_date: Optional[str] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Transform raw orders into fact and dimension tables.

    - Ensures required columns exist
    - Coerces types, drops invalid rows
    - Computes total_amount
    - Optional filter by min_date (YYYY-MM-DD)
    - Builds dim_customers and fact_orders
    """
    missing = [c for c in REQUIRED_COLUMNS if c not in raw.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")
    df = raw.copy()
    df = _coerce_types(df)
    if min_date:
        cutoff = pd.to_datetime(min_date).date()
        df = df[df["order_date"] >= cutoff]
    df = _drop_bad_rows(df)
    df["total_amount"] = df["quantity"] * df["price"]

    # fact_orders (one row per order)
    fact_orders = df[[
        "order_id","customer_id","order_date","item","quantity","price","total_amount",
    ]].drop_duplicates(subset=["order_id"]).sort_values("order_date")

    # dim_customers (simple rollup)
    grp = df.groupby("customer_id").agg(
        city=("city","first"),
        country=("country","first"),
        first_order_date=("order_date","min"),
        last_order_date=("order_date","max"),
        order_count=("order_id","nunique"),
        lifetime_value=("total_amount","sum")
    ).reset_index()

    dim_customers = grp[[
        "customer_id","city","country","first_order_date","last_order_date","order_count","lifetime_value"
    ]].sort_values("customer_id")

    return fact_orders, dim_customers
