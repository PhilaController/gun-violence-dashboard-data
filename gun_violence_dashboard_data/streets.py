"""Module for calculating shooting hot spots by street block."""

from dataclasses import dataclass

import geopandas as gpd
import numpy as np
import pandas as pd
from cached_property import cached_property
from loguru import logger
from shapely.geometry import MultiLineString

from . import DATA_DIR, EPSG


def _as_string(x):
    return f"{x:.0f}" if x else ""


def _match_to_streets(data, streets, key, buffer):
    """
    Associate the input Point data set with the nearest street.

    Parameters
    ----------
    data : GeoDataFrame
        dataframe holding the Point data set, in this case, either
        the work orders or requests for street defects
    key : str
        the unique identifier for the input data
    buffer : int
        the buffer in feet to search for matches
    """
    # get only the unique values
    unique_data = data.drop_duplicates(subset=key)

    # save the original un-buffered streets data
    streets_geometry = streets.geometry.copy()

    # buffer the geometry and do the spatial join
    streets.geometry = streets.geometry.buffer(buffer)
    df = gpd.sjoin(unique_data, streets, op="within", how="left")

    # missing vs matched
    missing = df.loc[df["street_name"].isnull()].copy()
    matched = df.loc[~df["street_name"].isnull()].copy()

    # remove any missing that are in matched
    missing = missing.loc[missing.index.difference(matched.index)]
    missing = missing.loc[~missing.duplicated(subset=key)]

    def get_closest(x):
        match = streets.loc[streets_geometry.distance(x.geometry).idxmin()]
        match = match.drop("geometry")
        x.update(match)
        return x

    # add a distance column
    matched_streets = streets_geometry.loc[matched["index_right"]]
    D = matched.reset_index(drop=True).distance(matched_streets.reset_index(drop=True))
    matched["distance"] = D.values

    # drop duplicates, keeping the first
    matched = matched.sort_values(by="distance", ascending=True)
    matched = matched.loc[~matched.index.duplicated(keep="first")]
    matched = matched.drop(labels=["distance"], axis=1)

    # get matches for missing
    Y = missing.apply(get_closest, axis=1)

    # join missing and matched
    out = pd.concat([matched, Y], axis=0)

    # merge back in to original data frame
    columns = list(set(streets.columns) - {"geometry"})
    out = pd.merge(data, out[columns + [key]], on=key)

    return out


@dataclass
class StreetHotSpots:
    """"""

    debug: bool = False

    @cached_property
    def centerlines(self):
        """Street centerlines"""

        def round(x):
            return int(np.floor(x / 100.0)) * 100

        return (
            gpd.read_file(DATA_DIR / "raw" / "Street_Centerline")
            .assign(block_number=lambda df: df["L_F_ADD"].apply(round))
            .to_crs(epsg=EPSG)
        )

    @cached_property
    def streets_directory(self):
        """"""
        return (
            gpd.read_file(DATA_DIR / "raw" / "Street_Network_Types")
            .rename(columns={"SEG_ID": "segment_id", "LENGTH": "length"})
            .drop(labels=["OBJECTID", "NETWORK", "RESPONSIBI"], axis=1)
            .merge(
                self.centerlines.rename(
                    columns={"STNAME": "street_name", "SEG_ID": "segment_id"}
                )[["segment_id", "street_name", "block_number"]],
                how="left",
                on="segment_id",
            )
            .to_crs(epsg=EPSG)
        )

    @cached_property
    def block_level_streets(self):
        """Load streets, aggregated by block"""

        # Load streets
        return gpd.GeoDataFrame(
            (
                self.streets_directory.dropna(subset=["street_name"])
                .groupby(["street_name", "block_number"])
                .agg(
                    {
                        "geometry": lambda x: MultiLineString(x.tolist()),
                        "length": "sum",
                    }
                )
                .reset_index()
                .reset_index()
                .rename(columns={"index": "segment_id"})
            ),
            crs=f"EPSG:{EPSG}",
            geometry="geometry",
        )

    def merge(self, data):
        """Calculate hot spots and merge data into input dataframe."""

        # Drop empty sgeometries
        data_geo = data.loc[~data.geometry.is_empty].copy()

        # Match to streets, using radius of 200 ft
        if self.debug:
            logger.debug("Calculating street hot spots")
        df = _match_to_streets(
            data_geo, self.block_level_streets.copy(), "cartodb_id", buffer=200
        )

        # Drop long segments for visual aesthetics
        df = df.query("length < 5200")

        # The new fields we want to merge
        new_info = df[["cartodb_id", "segment_id", "street_name", "block_number"]]

        # Merge new hot spot segment id back into the original data frame
        return data.merge(
            new_info,
            on="cartodb_id",
            how="left",
        ).assign(segment_id=lambda df: df.segment_id.fillna("").apply(_as_string))

    def save(self):
        """"""
        if self.debug:
            logger.debug("Saving hot spot streets layer as GeoJSON")

        # Important: output in 4326
        (
            self.block_level_streets.to_crs(epsg=4326)
            .assign(segment_id=lambda df: df.segment_id.apply(_as_string))[
                ["geometry", "segment_id", "street_name", "block_number"]
            ]
            .to_file(
                DATA_DIR / "processed" / "geo" / "streets.geojson", driver="GeoJSON"
            )
        )
