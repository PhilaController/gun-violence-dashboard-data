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


@dataclass
class ShootingVictimsData:
    """Class for downloading and analyzing the shooting victims
    database from Open Data Philly."""

    debug: bool = False

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

            df = (
                carto2gpd.get(self.ENDPOINT, self.TABLE_NAME)
                .assign(
                    time=lambda df: df.time.replace("<Null>", np.nan).fillna(
                        "00:00:00"
                    ),
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

            def _add_geo_info(data, geo):
                return gpd.sjoin(data, geo, how="left", op="within").drop(
                    labels=["index_right"], axis=1
                )

            # Add geographic columns
            df = (
                df.pipe(_add_geo_info, get_zip_codes().to_crs(df.crs))
                .pipe(_add_geo_info, get_police_districts().to_crs(df.crs))
                .pipe(_add_geo_info, get_council_districts().to_crs(df.crs))
                .pipe(_add_geo_info, get_neighborhoods().to_crs(df.crs))
            )

            # Save it
            if update_local:
                if self.debug:
                    logger.debug("Updating saved copy of shooting victims database")
                df.to_file(self.path, driver="GeoJSON")

        # Load from disk, fill missing geometries and convert CRS
        return (
            gpd.read_file(self.path)
            .assign(
                geometry=lambda df: df.geometry.fillna(Point()),
                date=lambda df: pd.to_datetime(df.date),
            )
            .to_crs(epsg=EPSG)
        )

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

        # Save each year's data to separate file
        for year in sorted(data["year"].unique()):

            if self.debug:
                logger.debug(f"Saving {year} shootings as a GeoJSON file")

            # Get data for this year
            # Save in EPSG = 4326
            data_yr = data.query(f"year == {year}").to_crs(epsg=4326)

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
                    "hood"
                ]
            ].to_file(
                DATA_DIR / "processed" / f"shootings_{year}.json", driver="GeoJSON"
            )
