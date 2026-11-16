"""Print column types of cpd_incidents; date_time and last_updated must be timestamps: python src/check_column_types.py"""
from sqlalchemy import text

from load import get_engine

with get_engine().connect() as c:
    for name, typ in c.execute(text("select column_name, data_type from information_schema.columns "
                                    "where table_name='cpd_incidents' order by ordinal_position")):
        print(f"{name}: {typ}")
