import geopandas as gpd
import numpy as np
import pandas as pd

from . import DATA_DIR

EPSG = 2272


def load_centerlines():
    """
    Load the street center lines.

    .. note::
        Returned coordinates use EPSG 2272
    """
    df = gpd.read_file(DATA_DIR / "Street_Centerline")

    # add a block
    def round(x):
        return int(np.floor(x / 100.0)) * 100

    df["block_number"] = df["L_F_ADD"].apply(round)

    return df.to_crs(epsg=EPSG)


def load_streets_directory():
    """
    Load the directory of streets, giving data on the network type, 
    length, and agency responsible.

    .. note::
        Returned coordinates use EPSG 2272
    """
    df = gpd.read_file(DATA_DIR / "Street_Network_Types")

    rename = {}
    rename["NETWORK"] = "network"
    rename["SEG_ID"] = "segment_id"
    rename["LENGTH"] = "length"
    rename["RESPONSIBI"] = "responsible"

    # rename and remove
    drop = ["OBJECTID"]
    df = df.rename(columns=rename).drop(drop, axis=1)

    centers = load_centerlines().rename(
        columns={"STNAME": "street_name", "SEG_ID": "segment_id"}
    )
    df = pd.merge(
        df,
        centers[["segment_id", "street_name", "block_number"]],
        how="left",
        on="segment_id",
    )

    return df.to_crs(epsg=EPSG)


def match_to_streets(data, streets, key, buffer):
    """
    Associate the input Point data set with the nearest street.

    Parameters
    ----------
    data : GeoDataFrame
        dataframe holding the Point data set, in this case, either 
        the work orders or requests for street defects
    streets : GeoDataFrame
        the dataframe holding the streets directory
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
    missing = df.loc[df["street_name"].isnull()]
    matched = df.loc[~df["street_name"].isnull()]

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
