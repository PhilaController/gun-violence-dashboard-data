import datetime
import json

import click

import carto2gpd
import numpy as np
import pandas as pd
from loguru import logger

from . import DATA_DIR

ENDPOINT = "https://phl.carto.com/api/v2/sql"
TABLE_NAME = "shootings"


def run_daily_update():
    """"""
    # Download full dataset
    logger.info("Downloading shootings database")
    data = download_shootings_data()

    # Loop over each year of data
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
        daily = calculate_daily_counts(data_yr)

        # Save daily
        logger.info(f"Saving {year} daily shooting counts as a JSON file")
        daily.to_json(DATA_DIR / f"shootings_{year}_daily.json", orient="records")

    # Update meta data
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    meta_path = DATA_DIR / "meta.json"

    # save the download time
    meta = {"last_updated": now}
    json.dump(meta, meta_path.open(mode="w"))


def calculate_daily_counts(df):
    """Calculate daily shooting counts."""

    # Group by day
    N = df.set_index("date").groupby(pd.Grouper(freq="D")).size()

    # Convert index to a string
    N.index = N.index.strftime("%Y-%m-%d")

    # Return as dataframe
    return N.reset_index(name="count")


def download_shootings_data():
    """Download and format shootings database from OpenDataPhilly.
    
    Source
    ------
    https://www.opendataphilly.org/dataset/shooting-victims
    """
    return (
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
        .drop(labels=["point_x", "point_y", "date_", "time", "objectid"], axis=1)
        .sort_values("date", ascending=False)
        .reset_index(drop=True)
    )


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
