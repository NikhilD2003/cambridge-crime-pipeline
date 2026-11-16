import pandas as pd
from prefect import task


@task
def transform_data(df):
    """Drop invalid/duplicate ids, parse timestamps, add year/month/day/hour."""
    df = df.dropna(subset=["id"]).drop_duplicates(subset="id").copy()
    df["id"] = df["id"].astype(int)
    df["date_time"] = pd.to_datetime(df["date_time"])
    df["last_updated"] = pd.to_datetime(df["last_updated"], errors="coerce")
    d = df["date_time"].dt
    df["year"], df["month"], df["day"], df["hour"] = d.year, d.month, d.day, d.hour
    return df
