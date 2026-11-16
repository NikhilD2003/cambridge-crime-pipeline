"""Check DATABASE_URL works: python src/test_connection.py"""
from sqlalchemy import text

from load import get_engine

with get_engine().connect() as c:
    print("Connected to PostgreSQL successfully!")
    print(c.execute(text("select version()")).scalar())
