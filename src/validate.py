from datetime import datetime

import pandas as pd
from prefect import task

COLS = ["date_time", "id", "type", "subtype", "location", "last_updated", "description"]
REQUIRED = ["date_time", "id", "type", "location"]


@task
def validate_data(df):
    """Fail loudly on schema / datetime / null problems; non-numeric ids become NaN (dropped in transform)."""
    missing = set(REQUIRED) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    df = df.reindex(columns=COLS)  # optional columns (e.g. subtype) may be absent in a given pull
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    for c in ("date_time", "type", "location"):
        if df[c].isnull().any():
            raise ValueError(f"Nulls in required column '{c}'")
    df["date_time"].map(datetime.fromisoformat)  # raises on non-ISO values
    return df
