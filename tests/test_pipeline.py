"""Offline checks of validate + transform (no network, no DB): python tests/test_pipeline.py"""
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
from transform import transform_data  # noqa: E402
from validate import validate_data  # noqa: E402

ROW = {"id": "1", "date_time": "2025-03-06T01:07:00", "type": "INCIDENT", "location": "MAIN ST", "last_updated": "2025-03-07T00:00:00"}


def test_good_rows_clean():
    out = transform_data.fn(validate_data.fn(pd.DataFrame([ROW, {**ROW}, {**ROW, "id": "x"}])))  # dup id + non-numeric id
    assert len(out) == 1 and out.loc[0, ["year", "month", "day", "hour"]].tolist() == [2025, 3, 6, 1]


def test_validate_fails_loudly():
    for bad in ({**ROW, "type": None}, {**ROW, "date_time": "06/03/2025"}):
        try:
            validate_data.fn(pd.DataFrame([bad]))
        except ValueError:
            continue
        raise AssertionError(f"accepted {bad}")
    try:
        validate_data.fn(pd.DataFrame([{"id": 1}]))
    except ValueError:
        return
    raise AssertionError("missing columns accepted")


if __name__ == "__main__":
    test_good_rows_clean(); test_validate_fails_loudly(); print("all checks passed")
