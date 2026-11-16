import os
from urllib.parse import quote
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from prefect import task
from psycopg2.extras import execute_values
from sqlalchemy import create_engine

load_dotenv()
SCHEMA = Path(__file__).with_name("schema.sql").read_text()


def make_engine(url, **kw):
    """Accepts a raw Supabase URI even if the password has @ : / # (split on the LAST '@')."""
    if url.startswith("sqlite"):
        return create_engine(url, **kw)
    creds, _, hostpart = url.split("://", 1)[1].rpartition("@")
    user, _, pw = creds.partition(":")
    return create_engine(f"postgresql+psycopg2://{quote(user, safe='.')}:{quote(pw, safe='')}@{hostpart}", **kw)


def get_engine():
    return make_engine(os.environ["DATABASE_URL"], pool_pre_ping=True)


@task
def load_into_postgres(df, chunk=2000):
    """Upsert into Supabase Postgres in batches (one round trip per `chunk` rows; safe to re-run daily)."""
    cols = list(df.columns)
    rows = df.astype(object).where(df.notna(), None)
    rows = [tuple(v.to_pydatetime() if isinstance(v, pd.Timestamp) else v for v in r) for r in rows.itertuples(index=False)]
    sets = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c != "id")
    sql = f"INSERT INTO cpd_incidents ({','.join(cols)}) VALUES %s ON CONFLICT (id) DO UPDATE SET {sets}"
    conn = get_engine().raw_connection()
    try:
        cur = conn.cursor()
        cur.execute(SCHEMA)
        for i in range(0, len(rows), chunk):
            execute_values(cur, sql, rows[i:i + chunk])
        conn.commit()
        cur.execute("SELECT count(*) FROM cpd_incidents WHERE id = ANY(%s)", ([int(i) for i in df["id"]],))
        n = cur.fetchone()[0]
        if n != len(rows):  # row-count gate: every row we sent must now be in the table
            raise RuntimeError(f"Row count mismatch: sent {len(rows)}, found {n} in cpd_incidents")
    finally:
        conn.close()
    return len(rows)
