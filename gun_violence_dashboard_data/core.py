import datetime

import click

import carto2gpd
import geopandas as gpd
import numpy as np
import pandas as pd
import simplejson as json
from loguru import logger
from shapely.geometry import MultiLineString

from . import DATA_DIR
from .streets import EPSG, load_streets_directory, match_to_streets
from .tools import replace_missing_geometries

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
    data.to_file(DATA_DIR / "raw" / "shootings.json", driver="GeoJSON")

    # Load streets
    streets = gpd.GeoDataFrame(
        (
            load_streets_directory()
            .dropna(subset=["street_name"])
            .groupby(["street_name", "block_number"])
            .agg({"geometry": lambda x: MultiLineString(x.tolist()), "length": "sum",})
            .reset_index()
            .reset_index()
            .rename(columns={"index": "segment_id"})
        ),
        crs="EPSG:2272",
        geometry="geometry",
    )

    logger.info("Calculating street hot spots")
    data = calculate_street_hotspots(streets, data)

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
                "segment_id",
                "block_number",
                "street_name",
                "length",
            ]
        ].to_file(DATA_DIR / "processed" / f"shootings_{year}.json", driver="GeoJSON")

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
        out,
        open(DATA_DIR / "processed" / f"shootings_cumulative_daily.json", "w"),
        ignore_nan=True,
    )

    # Save streets
    logger.info("Saving streets directory")
    streets = streets.to_crs(epsg=4326)
    streets["segment_id"] = streets["segment_id"].apply(lambda x: f"{x:.0f}")
    streets[
        ["geometry", "segment_id", "street_name", "block_number", "length"]
    ].to_file(DATA_DIR / "processed" / "streets.geojson", driver="GeoJSON")

    # Update meta data
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    meta_path = DATA_DIR / "meta.json"

    # save the download time
    meta = {"last_updated": now}
    json.dump(meta, meta_path.open(mode="w"))


def calculate_street_hotspots(streets, data):
    """Calculate hot spots."""

    # Drop missing
    data_geo = replace_missing_geometries(data).to_crs(epsg=EPSG).copy()
    data_geo = data_geo.loc[~data_geo.geometry.is_empty]

    # Match to
    df = match_to_streets(data_geo, streets.copy(), "cartodb_id", buffer=200)

    # Merge back
    merged = data.merge(
        df[["cartodb_id", "segment_id", "length", "street_name", "block_number"]],
        on="cartodb_id",
        how="left",
    )

    # long segments
    long_segments = merged["length"] > 5200
    merged.loc[
        long_segments, ["segment_id", "length", "street_name", "block_number"]
    ] = np.nan

    # store segment id as str
    merged["segment_id"] = (
        merged["segment_id"].fillna("").apply(lambda x: f"{x:.0f}" if x else "")
    )

    return merged

    # # Count
    # counts = df.groupby(["segment_id"]).size().reset_index(name="count")

    # # Merge
    # streets = load_streets_directory()
    # X = (
    #     streets.merge(counts, on="segment_id", how="left")
    #     .assign(count=lambda df: df["count"].fillna(0))
    #     .dropna(subset=["street_name"])
    #     .rename(columns={"street_name": "street"})
    # )

    # # calculate the index
    # CPL = np.log10(X["count"] / X["length"])
    # CPL[~np.isfinite(CPL)] = 0  # no shootings equals a 0

    # X = X.assign(rating=CPL).drop(labels=["length"], axis=1)
    # return X.query("count > 0").to_crs(epsg=4326)


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
                ["Younger than 18", "19 to 30", "31 to 45", "Older than 45"],
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
