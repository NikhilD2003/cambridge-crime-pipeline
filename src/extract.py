import os

import pandas as pd
import requests
from dotenv import load_dotenv
from prefect import task
from sodapy import Socrata

load_dotenv()


@task(retries=3, retry_delay_seconds=10)
def extract_data():
    """Fetch all Cambridge PD log rows (Socrata dataset 3gki-wyrb) as a DataFrame."""
    token = os.getenv("SOCRATA_APP_TOKEN") or None
    try:
        return pd.DataFrame.from_records(Socrata("data.cambridgema.gov", token, timeout=30).get_all("3gki-wyrb", order=":id"))
    except requests.HTTPError as e:
        if token and e.response is not None and e.response.status_code == 403:  # bad token: token is optional, go anonymous
            print("Socrata rejected SOCRATA_APP_TOKEN; retrying without it (throttling possible).")
            return pd.DataFrame.from_records(Socrata("data.cambridgema.gov", None, timeout=30).get_all("3gki-wyrb", order=":id"))
        raise
