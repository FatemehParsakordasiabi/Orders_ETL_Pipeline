from __future__ import annotations
import io
import pandas as pd
import requests
from typing import Optional

def extract_orders(source: str, chunksize: Optional[int] = None) -> pd.DataFrame:
    """Extract orders data from a CSV *path* or *URL*.

    Args:
        source: local file path or http(s) URL to a CSV
        chunksize: optional chunk size for large files (rows per chunk)

    Returns:
        DataFrame with raw orders
    """
    if source.startswith("http://") or source.startswith("https://"):
        resp = requests.get(source, timeout=30)
        resp.raise_for_status()
        bio = io.BytesIO(resp.content)
        if chunksize:
            # concatenate chunks
            frames = [chunk for chunk in pd.read_csv(bio, chunksize=chunksize)]
            return pd.concat(frames, ignore_index=True)
        return pd.read_csv(bio)
    else:
        if chunksize:
            frames = [chunk for chunk in pd.read_csv(source, chunksize=chunksize)]
            return pd.concat(frames, ignore_index=True)
        return pd.read_csv(source)
