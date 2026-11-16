"""Vercel serverless function: GET /api/data?year=2025  ->  aggregated crime stats from Supabase."""
import json
import os
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, quote, urlparse

import psycopg2


def connect():
    # split on the LAST '@' so passwords containing '@' ':' '/' still work
    creds, _, host = os.environ["DATABASE_URL"].split("://", 1)[1].rpartition("@")
    user, _, pw = creds.partition(":")
    return psycopg2.connect(f"postgresql://{quote(user, safe='.')}:{quote(pw, safe='')}@{host}", connect_timeout=10)


def build(year=None):
    def rows(cur, sql):
        cur.execute(sql, {"y": year})
        return cur.fetchall()

    where = "WHERE (%(y)s::int IS NULL OR year = %(y)s::int)"
    with connect() as con, con.cursor() as cur:
        total, latest = rows(cur, f"SELECT count(*), max(date_time) FROM cpd_incidents {where}")[0]
        out = {
            "total": total,
            "latest": latest.isoformat(sep=" ", timespec="minutes") if latest else None,
            "years": [r[0] for r in rows(cur, "SELECT DISTINCT year FROM cpd_incidents ORDER BY 1")],
            "by_hour": rows(cur, f"SELECT hour, count(*) FROM cpd_incidents {where} GROUP BY 1 ORDER BY 1"),
            "by_month": [[d.isoformat(), n] for d, n in rows(
                cur, f"SELECT date_trunc('month', date_time)::date, count(*) FROM cpd_incidents {where} GROUP BY 1 ORDER BY 1")],
            "top_types": rows(cur, f"SELECT regexp_replace(subtype, '\s+C\d+\s+S.*$', '') t, count(*) c FROM cpd_incidents {where} "
                                   "AND subtype IS NOT NULL GROUP BY 1 ORDER BY c DESC LIMIT 8"),
            "top_locations": rows(cur, f"SELECT location, count(*) c FROM cpd_incidents {where} GROUP BY 1 ORDER BY c DESC LIMIT 10"),
        }
    return out


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            y = parse_qs(urlparse(self.path).query).get("year", [None])[0]
            body, code = json.dumps(build(int(y) if y else None)).encode(), 200
        except Exception as e:  # never leak connection details
            body, code = json.dumps({"error": type(e).__name__}).encode(), 500
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "s-maxage=3600, stale-while-revalidate")  # CDN-cache 1h
        self.end_headers()
        self.wfile.write(body)
