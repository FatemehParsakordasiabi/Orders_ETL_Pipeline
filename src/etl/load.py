from __future__ import annotations
import pandas as pd
from sqlalchemy import create_engine

def load_to_sqlite(fact_orders: pd.DataFrame, dim_customers: pd.DataFrame, db_path: str) -> None:
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as conn:
        dim_customers.to_sql("dim_customers", con=conn, if_exists="replace", index=False)
        fact_orders.to_sql("fact_orders", con=conn, if_exists="replace", index=False)
