"""Local preview of the Vercel dashboard: python dashboard/dev.py  ->  http://localhost:8000"""
import sys
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

from dotenv import load_dotenv

here = Path(__file__).parent
load_dotenv(here.parent / ".env")
sys.path.insert(0, str(here / "api"))
import data  # noqa: E402


class H(SimpleHTTPRequestHandler):
    def __init__(self, *a, **k):
        super().__init__(*a, directory=str(here / "public"), **k)

    def do_GET(self):
        return data.handler.do_GET(self) if self.path.startswith("/api/data") else super().do_GET()


print("Dashboard on http://localhost:8000")
HTTPServer(("", 8000), H).serve_forever()
