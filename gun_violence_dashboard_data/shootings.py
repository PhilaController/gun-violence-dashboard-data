"""Module for downloading and analyzing the shooting victims database."""

import datetime
from dataclasses import dataclass

import carto2gpd
import geopandas as gpd
import numpy as np
import pandas as pd
import simplejson as json
from loguru import logger
from shapely.geometry import Point

from . import DATA_DIR, EPSG
from .geo import *

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


def add_geographic_info(df):
    """Add geographic info."""

    # Get a fresh copy
    df = df.copy().reset_index(drop=True)

    # The original length
    original_length = len(df)

    # Check city limits
    city_limits = get_city_limits().to_crs(df.crs)
    outside_limits = ~df.geometry.within(city_limits.squeeze().geometry)
    missing = outside_limits.sum()

    # Set missing geometry to null
    logger.info(f"{missing} shootings outside city limits")
    if missing > 0:
        df.loc[missing, "geometry"] = np.nan

    # Try to replace any missing geometries from criminal incidents
    dc_key_list = ", ".join(
        df.loc[df.geometry.isnull(), "dc_key"].apply(lambda x: f"'{x}'")
    )
    url = "https://phl.carto.com/api/v2/sql"
    incidents = carto2gpd.get(
        url, "incidents_part1_part2", where=f"dc_key IN ( {dc_key_list} )"
    )

    # Did we get any matches
    matches = len(incidents)
    logger.info(f"Found {matches} matches for {missing} missing geometries")

    # Merge
    if matches > 0:

        missing_sel = df.geometry.isnull()
        missing = df.loc[missing_sel]
        df2 = missing.drop(columns=["geometry"]).merge(
            incidents[["dc_key", "geometry"]].drop_duplicates(subset=["dc_key"]),
            on="dc_key",
            how="left",
        )
        df = pd.concat([df.loc[~missing_sel], df2]).reset_index(drop=True)

    def _add_geo_info(data, geo):
        out = gpd.sjoin(data, geo, how="left", predicate="within")

        # NOTE: sometimes this will match multiple geo boundaries
        # REMOVE THEM
        duplicated = out.index.duplicated()
        if duplicated.sum():
            out = out.loc[~duplicated]

        return out.drop(labels=["index_right"], axis=1)

    # Add geographic columns
    geo_funcs = [
        get_zip_codes,
        get_police_districts,
        get_council_districts,
        get_neighborhoods,
        get_school_catchments,
        get_pa_house_districts,
    ]
    for geo_func in geo_funcs:
        df = df.pipe(_add_geo_info, geo_func().to_crs(df.crs))

    # if geo columns are missing, geometry should be NaN
    df.loc[df["hood"].isnull(), "geometry"] = np.nan

    # Check the length
    if len(df) != original_length:
        raise ValueError("Length of data has changed; this shouldn't happen!")

    return df


@dataclass
class ShootingVictimsData:
    """Class for downloading and analyzing the shooting victims
    database from Open Data Philly."""

    debug: bool = False
    ignore_checks: bool = False

    ENDPOINT = "https://phl.carto.com/api/v2/sql"
    TABLE_NAME = "shootings"

    @property
    def path(self):
        return DATA_DIR / "raw" / "shootings.json"

    def get(self, fresh=False, update_local=True):
        """Get the shooting victims data, either loading
        the currently downloaded version or a fresh copy."""

        if fresh or not self.path.exists():

            if self.debug:
                logger.debug("Downloading shooting victims database")

            # Raw data
            df = carto2gpd.get(self.ENDPOINT, self.TABLE_NAME)

            # Verify DC key first
            missing_dc_keys = df["dc_key"].isnull()
            if missing_dc_keys.sum() and not self.ignore_checks:
                n = missing_dc_keys.sum()
                raise ValueError(f"Found {n} rows with missing DC keys")

            # Format
            df = (
                df.assign(
                    time=lambda df: df.time.replace("<Null>", np.nan).fillna(
                        "00:00:00"
                    ),
                    date=lambda df: pd.to_datetime(
                        df.date_.str.slice(0, 10).str.cat(df.time, sep=" ")
                    ),
                    dc_key=lambda df: df.dc_key.astype(float).astype(int).astype(str),
                    year=lambda df: df.date.dt.year,
                    race=lambda df: df.race.fillna("Other/Unknown"),
                    age=lambda df: df.age.astype(float),
                    age_group=lambda df: np.select(
                        [
                            df.age <= 17,
                            (df.age > 17) & (df.age <= 30),
                            (df.age > 30) & (df.age <= 45),
                            (df.age > 45),
                        ],
                        ["Younger than 18", "18 to 30", "31 to 45", "Older than 45"],
                        default="Unknown",
                    ),
                )
                .assign(
                    race=lambda df: df.race.where(df.latino != 1, other="H"),
                    age=lambda df: df.age.fillna("Unknown"),
                )
                .drop(
                    labels=["point_x", "point_y", "date_", "time", "objectid"], axis=1
                )
                .sort_values("date", ascending=False)
                .reset_index(drop=True)
            )

            # CHECKS
            if not self.ignore_checks:
                old_df = gpd.read_file(self.path)
                TOLERANCE = 100
                if len(df) - len(old_df) > TOLERANCE:
                    logger.info(f"Length of new data: {len(df)}")
                    logger.info(f"Length of old data: {len(old_df)}")
                    raise ValueError(
                        "New data seems to have too many rows...please manually confirm new data is correct."
                    )

                TOLERANCE = 10
                if len(old_df) - len(df) > TOLERANCE:
                    logger.info(f"Length of new data: {len(df)}")
                    logger.info(f"Length of old data: {len(old_df)}")
                    raise ValueError(
                        "New data seems to have too few rows...please manually confirm new data is correct."
                    )

            # Add geographic info
            df = add_geographic_info(df)

            # Save it
            if update_local:
                if self.debug:
                    logger.debug("Updating saved copy of shooting victims database")
                df.to_file(self.path, driver="GeoJSON")

        # Load from disk, fill missing geometries and convert CRS
        out = (
            gpd.read_file(self.path, dtype={"dc_key": str})
            .assign(
                geometry=lambda df: df.geometry.fillna(Point()),
                date=lambda df: pd.to_datetime(df.date),
            )
            .to_crs(epsg=EPSG)
        )

        # Check dc_key is properly formatted
        assert (
            out["dc_key"].str.contains(".0", regex=False).sum() == 0
        ), "dc_key not properly formatted"

        return out

    def save_cumulative_totals(self, data, update_local=True):
        """Calculate the cumulative daily total."""

        # Loop over each year of data
        daily = []
        for year in sorted(data["year"].unique()):

            # Group by day
            N = (
                data.query(f"year == {year}")
                .set_index("date")
                .groupby(pd.Grouper(freq="D"))
                .size()
            )

            # Reindex
            N = N.reindex(pd.date_range(f"{year}-01-01", f"{year}-12-31")).rename(
                str(year)
            )
            N.index = N.index.strftime("%m %d")

            daily.append(N)

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

        # Convert to a dict
        out = {}
        for col in daily:
            out[col] = daily[col].tolist()
        out["date"] = daily.index.tolist()

        # Save?
        if update_local:
            if self.debug:
                logger.debug(f"Saving cumulative daily shooting counts as a JSON file")
            json.dump(
                out,
                open(DATA_DIR / "processed" / f"shootings_cumulative_daily.json", "w"),
                ignore_nan=True,
            )

        return out

    def save(self, data):
        """Save annual, processed data files."""

        # years
        years = sorted(data["year"].astype(int).unique())
        years = [int(yr) for yr in reversed(years)]
        json.dump(years, (DATA_DIR / "processed" / "data_years.json").open("w"))

        # Save each year's data to separate file
        for year in years:

            if self.debug:
                logger.debug(f"Saving {year} shootings as a GeoJSON file")

            # Get data for this year
            # Save in EPSG = 4326
            data_yr = data.query(f"year == {year}").to_crs(epsg=4326)

            # Convert the date column
            data_yr["date"] = data_yr["date"].dt.strftime("%Y/%m/%d %H:%M:%S")

            # Extract columns and save
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
                    "has_court_case",
                    "zip",
                    "council",
                    "police",
                    "hood",
                    "school",
                    "house_district",
                ]
            ].to_file(
                DATA_DIR / "processed" / f"shootings_{year}.json", driver="GeoJSON"
            )
