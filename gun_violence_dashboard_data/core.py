import datetime

import carto2gpd
import click
import numpy as np
import pandas as pd
import simplejson as json
from loguru import logger

from . import DATA_DIR

ENDPOINT = "https://phl.carto.com/api/v2/sql"
TABLE_NAME = "shootings"
CURRENT_YEAR = datetime.datetime.now().year
MONTHS = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]


def run_daily_update():
    """"""
    # Download full dataset
    logger.info("Downloading shootings database")
    data = download_shootings_data()

    # Loop over each year of data
    daily = []
    for year in sorted(data["year"].unique()):

        # Get data for this year
        data_yr = data.query(f"year == {year}")

        # Save geojson
        logger.info(f"Saving {year} shootings as a GeoJSON file")
        data_yr[
            [
                "geometry",
                "dc_key",
                "race",
                "sex",
                "age",
                "latino",
                "fatal",
                "date",
                "age_group",
            ]
        ].to_file(DATA_DIR / f"shootings_{year}.json", driver="GeoJSON")

        # Daily counts
        daily.append(calculate_daily_counts(data_yr, year))

    # Finish daily cumulative calculation
    daily = pd.concat(daily, axis=1).sort_index()

    # Figure max day in current year
    cut = daily.index[daily[str(CURRENT_YEAR)].isnull()].min()

    # Fillna and do the cum sum
    daily = daily.fillna(0).cumsum()

    # Current year back to NaNs
    daily.loc[cut:, str(CURRENT_YEAR)] = None

    # Re-index to account for leap years
    new_index = []
    for v in daily.index:
        fields = v.split()
        new_index.append(f"{MONTHS[int(fields[0])-1]} {fields[1]}")
    daily.index = new_index

    # Save daily
    logger.info(f"Saving cumulative daily shooting counts as a JSON file")
    out = {}
    for col in daily:
        out[col] = daily[col].tolist()
    out["date"] = daily.index.tolist()
    json.dump(
        out, open(DATA_DIR / f"shootings_cumulative_daily.json", "w"), ignore_nan=True
    )

    # Update meta data
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    meta_path = DATA_DIR / "meta.json"

    # save the download time
    meta = {"last_updated": now}
    json.dump(meta, meta_path.open(mode="w"))


def calculate_daily_counts(df, year):
    """Calculate daily shooting counts."""

    # Group by day
    N = df.set_index("date").groupby(pd.Grouper(freq="D")).size()

    # Reindex
    N = N.reindex(pd.date_range(f"{year}-01-01", f"{year}-12-31")).rename(str(year))
    N.index = N.index.strftime("%m %d")
    return N


def download_shootings_data():
    """Download and format shootings database from OpenDataPhilly.
    
    Source
    ------
    https://www.opendataphilly.org/dataset/shooting-victims
    """
    df = (
        carto2gpd.get(ENDPOINT, TABLE_NAME)
        .assign(
            time=lambda df: df.time.replace("<Null>", np.nan).fillna("00:00:00"),
            date=lambda df: pd.to_datetime(
                df.date_.str.slice(0, 10).str.cat(df.time, sep=" ")
            ),
            year=lambda df: df.date.dt.year,
            race=lambda df: df.race.fillna("Other/Unknown"),
            age=lambda df: df.age.astype(float),
            age_group=lambda df: np.select(
                [
                    df.age < 18,
                    (df.age >= 18) & (df.age <= 30),
                    (df.age > 30) & (df.age <= 45),
                    (df.age > 45),
                ],
                ["Under 18", "19 to 30", "31 to 45", "Greater than 45"],
                default="Unknown",
            ),
        )
        .assign(age=lambda df: df.age.fillna("Unknown"))
        .drop(labels=["point_x", "point_y", "date_", "time", "objectid"], axis=1)
        .sort_values("date", ascending=False)
        .reset_index(drop=True)
    )

    # Track latino
    df.loc[df["latino"] > 0, "race"] = "H"

    return df


@click.group()
@click.version_option()
def cli() -> None:
    """Gun Violence Dashboard Data"""


@cli.command()  # @cli, not @click!
def daily_update():
    """Run the daily update."""
    run_daily_update()


if __name__ == "__main__":
    cli(prog_name="gv_dashboard_data")
