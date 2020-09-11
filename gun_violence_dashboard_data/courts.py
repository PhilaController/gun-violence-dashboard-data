import json

import pandas as pd

from . import DATA_DIR


def process_courts_data():
    """"""

    data = json.load((DATA_DIR / "raw" / "scraped_courts_data.json").open("r"))

    out = []
    for dc_key in data:
        out.append([dc_key, len(data[dc_key])])
    return pd.DataFrame(out, columns=["dc_key", "entries"])


def merge_courts_data(data):
    """Merge courts data."""

    courts = process_courts_data()

    data["dc_key"] = data["dc_key"].astype(str)
    data["has_court_case"] = False
    data.loc[data["dc_key"].isin(courts["dc_key"]), "has_court_case"] = True

    return data
